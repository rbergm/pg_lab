
#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "access/relation.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/read_stream.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#ifdef _POSIX_C_SOURCE
#include <fcntl.h>
#endif


/*
 * PG API stuff that we need
 */

extern PGDLLEXPORT planner_hook_type planner_hook;

#if PG_VERSION_NUM < 160000
    #define DropRelFileNodesAllBuffers(rel, n) DropRelFileNodesAllBuffers(rel, n)
#else
    #define DropRelFileNodesAllBuffers(rel, n) DropRelationsAllBuffers(rel, n)
#endif

static planner_hook_type prev_planner_hook = NULL;


/*
 * Copied from md.c because this struct is not available in any header file
 * and we need it to find the FD to call posix_fadvise().
 * This is really ugly, but still the best we can do for now.
 */
struct MdfdVecData
{
    File mdfd_vfd;
    BlockNumber mdfd_segno;
};

/*
 * Copied from fd.h because this struct is not available in any header file and
 * we need it to find the FD to call posix_fadvise().
 * This is really ugly, but still the best we can do for now.
 */
struct vfd
{
    int fd;

    /*
     * Actually, there are a lot of additional fields in vfd, but we only care about the fd.
     * Omitting the rest here means that we need to be a bit careful with casts to/from vfd
     * and sizeof() calls.
     */
};


/*
 * Extension-specific (GUC) variables, definitions and setup
 */

extern PGDLLEXPORT void _PG_init(void);
extern PGDLLEXPORT void _PG_fini(void);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_cooldown);

typedef enum ExperimentMode
{
    EMODE_OFF = 0,
    EMODE_COLD = 1,
    EMODE_HOT = 2
} ExperimentMode;

static const struct config_enum_entry experiment_mode_options[] = {
    {"off", EMODE_OFF, false},
    {"cold", EMODE_COLD, false},
    {"hot", EMODE_HOT, false},
    {NULL, 0, false}
};

static int experiment_mode = EMODE_OFF;


PlannedStmt* pg_temperature_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);


/*
 * private function prototypes
 */

typedef void (*oid_handler)(Oid oid);
static void cooldown_oid(Oid oid);
static void warmup_oid(Oid oid);

static List* CollectScanOids(PlannedStmt *query_plan, Plan *root);
static List* CollectChildOids(PlannedStmt *query_plan, Plan *root);
static void  ProcessOids(List *oids, oid_handler handler);

/*
 * actual function implementations
 */

static void
cooldown_oid(Oid oid)
{
    Relation        rel;
    AclResult       aclres;
    SMgrRelation    smgr;

    rel = relation_open(oid, AccessExclusiveLock);
    aclres = pg_class_aclcheck(oid, GetUserId(), ACL_SELECT);
    if (aclres != ACLCHECK_OK)
    {
        aclcheck_error(aclres, get_relkind_objtype(oid), get_rel_name(oid));
        return;
    }

    smgr = RelationGetSmgr(rel);

    #ifdef _POSIX_C_SOURCE
    for (int forknum = 0; forknum < MAX_FORKNUM; ++forknum)
    {
        struct MdfdVecData  *mdfd;
        struct vfd          *vfd;
        int                  fd;

        mdfd = (struct MdfdVecData *) smgr->md_seg_fds[forknum];
        if (mdfd == NULL)
            continue;

        vfd = GetVfdByFile(mdfd->mdfd_vfd);
        if (vfd == NULL)
            continue;

        fd = vfd->fd;

        /*
         * TODO: the documentation of fadvise() says that
         *   One can obtain a snapshot of which pages of a file are resident in the buffer cache by opening a file, mapping
         *   it with mmap(), and the applying mincore() to the mapping.
         * (https://man7.org/linux/man-pages/man2/posix_fadvise.2.html)
         * Maybe we can use this to add a better reporting to our extension?
         */
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    }
    #else
    ereport(WARNING,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("Can only remove OS cached data on POSIX systems")));
    #endif

    DropRelFileNodesAllBuffers(&smgr, 1);
    relation_close(rel, AccessExclusiveLock);
}

/*
 * Our hot-start code is basically a copy of the pg_prewarm logic
 */

 struct read_stream_private
 {
    BlockNumber blocknum;
    BlockNumber last_block;
 };

static BlockNumber
next_block(ReadStream *stream, void *callback_private_data, void *per_buffer_data)
{
    struct read_stream_private *priv = callback_private_data;

    if (priv->blocknum <= priv->last_block)
        return priv->blocknum++;

    return InvalidBlockNumber;
}

static void
warmup_oid(Oid oid)
{
    Relation        rel;
    AclResult       aclres;
    BlockNumber     nblocks;

    struct read_stream_private   stream_private;
    ReadStream                  *stream;

    rel = relation_open(oid, AccessExclusiveLock);
    aclres = pg_class_aclcheck(oid, GetUserId(), ACL_SELECT);
    if (aclres != ACLCHECK_OK)
    {
        aclcheck_error(aclres, get_relkind_objtype(oid), get_rel_name(oid));
        return;
    }

    nblocks = RelationGetNumberOfBlocks(rel);
    stream_private.blocknum = 0;
    stream_private.last_block = nblocks - 1;

    stream = read_stream_begin_relation(READ_STREAM_FULL,
                                        NULL,
                                        rel,
                                        MAIN_FORKNUM,
                                        next_block,
                                        &stream_private,
                                        0);

    for (BlockNumber current_block = 0; current_block < nblocks; ++current_block)
    {
        Buffer buf;
        CHECK_FOR_INTERRUPTS();
        buf = read_stream_next_buffer(stream, NULL);
        ReleaseBuffer(buf);
    }

    read_stream_end(stream);
    relation_close(rel, AccessExclusiveLock);
}

static List *
CollectChildOids(PlannedStmt *query_plan, Plan *root)
{
    List        *oids_list = NIL;
    ListCell    *lc;
    Plan        *child;

    if (root->lefttree)
        oids_list = list_concat(oids_list, CollectScanOids(query_plan, root->lefttree));
    if (root->righttree)
        oids_list = list_concat(oids_list, CollectScanOids(query_plan, root->righttree));

    foreach(lc, root->initPlan)
    {
        Plan *init_plan = (Plan*) lfirst(lc);
        oids_list = list_concat(oids_list, CollectScanOids(query_plan, init_plan));
    }

    switch (root->type)
    {
        case T_BitmapAnd:
        {
            BitmapAnd *bm_and = (BitmapAnd*) root;
            foreach (lc, bm_and->bitmapplans)
            {
                child = (Plan*) lfirst(lc);
                oids_list = list_concat(oids_list, CollectScanOids(query_plan, child));
            }
            break;
        }

        case T_BitmapOr:
        {
            BitmapOr *bm_or = (BitmapOr*) root;
            foreach (lc, bm_or->bitmapplans)
            {
                child = (Plan*) lfirst(lc);
                oids_list = list_concat(oids_list, CollectScanOids(query_plan, child));
            }
            break;
        }

        case T_Append:
        {
            Append *append = (Append*) root;
            foreach (lc, append->appendplans)
            {
                child = (Plan*) lfirst(lc);
                oids_list = list_concat(oids_list, CollectScanOids(query_plan, child));
            }
            break;
        }

        case T_MergeAppend:
        {
            MergeAppend *merge = (MergeAppend*) root;
            foreach (lc, merge->mergeplans)
            {
                child = (Plan*) lfirst(lc);
                oids_list = list_concat(oids_list, CollectScanOids(query_plan, child));
            }
            break;
        }

        case T_SubqueryScan:
        {
            SubqueryScan *subquery = (SubqueryScan*) root;
            oids_list = list_concat(oids_list, CollectScanOids(query_plan, subquery->subplan));
            break;
        }

        default:
            /* Silence compiler warnings. We don't need to handle the remaining nodes in any special way. */
            break;
    }

    return oids_list;
}

static List *
CollectScanOids(PlannedStmt *query_plan, Plan *root)
{
    RangeTblEntry *scan_rte = NULL;

    if (!(IsA(root, SeqScan) || IsA(root, IndexScan) ||
        IsA(root, IndexOnlyScan) || IsA(root, BitmapHeapScan) ||
        IsA(root, BitmapIndexScan)))
        return CollectChildOids(query_plan, root);

    switch (root->type)
    {
        case T_SeqScan:
        {
            SeqScan *seq_scan = (SeqScan*) root;
            scan_rte = rt_fetch(seq_scan->scan.scanrelid, query_plan->rtable);
            return scan_rte->rtekind == RTE_RELATION ? list_make1_oid(scan_rte->relid) : NIL;
        }

        case T_IndexScan:
        {
            IndexScan *idx_scan = (IndexScan*) root;
            scan_rte = rt_fetch(idx_scan->scan.scanrelid, query_plan->rtable);
            return list_make2_oid(idx_scan->indexid, scan_rte->relid);
        }

        case T_IndexOnlyScan:
        {
            IndexOnlyScan *idxo_scan = (IndexOnlyScan*) root;
            return list_make1_oid(idxo_scan->indexid);
        }


        case T_BitmapHeapScan:
        {
            BitmapHeapScan *bm_heap_scan = (BitmapHeapScan*) root;
            scan_rte = rt_fetch(bm_heap_scan->scan.scanrelid, query_plan->rtable);
            return list_concat(list_make1_oid(scan_rte->relid), CollectChildOids(query_plan, root));
        }

        case T_BitmapIndexScan:
        {
            BitmapIndexScan *bm_idx_scan = (BitmapIndexScan*) root;
            return list_make1_oid(bm_idx_scan->indexid);
        }

        default:
            Assert(false);
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("Unsupported scan type: %d", root->type)));
            return NIL;
    }
}

static List *
RemoveOidDuplicates(List *oids)
{
    ListCell    *lc             = NULL;
    int          i              = -1;
    List        *unique_oids    = NIL;
    Bitmapset   *oid_set        = NULL;

    foreach (lc, oids)
    {
        Oid oid = lfirst_oid(lc);
        oid_set = bms_add_member(oid_set, oid);
    }

    while ((i = bms_next_member(oid_set, i)) >= 0)
        unique_oids = lappend_oid(unique_oids, i);

    bms_free(oid_set);
    return unique_oids;
}

static void
ProcessOids(List *oids, oid_handler handler)
{
    ListCell *lc;

    foreach (lc, oids)
    {
        Oid oid = lfirst_oid(lc);
        handler(oid);
    }
}


PlannedStmt *
pg_temperature_planner(Query *parse,
                      const char *query_string,
                      int cursorOptions,
                      ParamListInfo boundParams)
{
    PlannedStmt *result;
    List        *scanned_oids;

    if (prev_planner_hook)
        result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    else
        result = standard_planner(parse, query_string, cursorOptions, boundParams);

    if (experiment_mode == EMODE_OFF)
        return result;

    scanned_oids = CollectScanOids(result, (Plan*) result->planTree);
    scanned_oids = RemoveOidDuplicates(scanned_oids);

    switch (experiment_mode)
    {
        case EMODE_HOT:
            ProcessOids(scanned_oids, warmup_oid);
            break;

        case EMODE_COLD:
            ProcessOids(scanned_oids, cooldown_oid);
            break;

        default:
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("Unsupported experiment mode: %d", experiment_mode)));
            break;
    }

    return result;
}


Datum
pg_cooldown(PG_FUNCTION_ARGS)
{
    Oid oid;

    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("Relation name expected")));

    oid = PG_GETARG_OID(0);
    cooldown_oid(oid);

    PG_RETURN_VOID();
}


void
_PG_init(void)
{
    prev_planner_hook = planner_hook;
    planner_hook = pg_temperature_planner;

    DefineCustomEnumVariable("pg_temperature.experiment_mode",
                             "Experiment mode for pg_temperature",
                             NULL,
                             &experiment_mode,
                             EMODE_OFF,
                             experiment_mode_options,
                             PGC_USERSET,
                             0,
                             NULL, NULL, NULL);
}


void
_PG_fini(void)
{
    planner_hook = prev_planner_hook;
    experiment_mode = EMODE_OFF;
}
