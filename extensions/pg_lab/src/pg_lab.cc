
#ifdef __cplusplus
extern "C" {
#endif

#include <limits.h>
#include <math.h>

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "access/parallel.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

#include "hints.h"

char* JOIN_ORDER_TYPE_FORCED  = (char*) "Forced";

#if PG_VERSION_NUM < 170000
void destroyStringInfo(StringInfo);
void destroyStringInfo(StringInfo str) {
  pfree(str->data);
  pfree(str);
}
#endif

PG_MODULE_MAGIC;

/* GeQO GUC variables */
extern bool enable_geqo;
extern int geqo_threshold;

extern bool enable_material;

/* Existing optimizer hooks */
extern planner_hook_type planner_hook;
static planner_hook_type prev_planner_hook = NULL;
extern ExecutorEnd_hook_type ExecutorEnd_hook;
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;

/* pg_lab hook additions */
extern prepare_make_one_rel_callback_type prepare_make_one_rel_callback;
static prepare_make_one_rel_callback_type prev_prepare_make_one_rel_hook = NULL;

extern final_path_callback_type final_path_callback;
static final_path_callback_type prev_final_path_callback = NULL;

extern join_search_hook_type join_search_hook;
static join_search_hook_type prev_join_search_hook = NULL;

extern add_path_hook_type add_path_hook;
static add_path_hook_type prev_add_path_hook = NULL;

extern add_partial_path_hook_type add_partial_path_hook;
static add_partial_path_hook_type prev_add_partial_path_hook = NULL;

extern add_path_precheck_hook_type add_path_precheck_hook;
static add_path_precheck_hook_type prev_add_path_precheck_hook = NULL;

extern add_partial_path_precheck_hook_type add_partial_path_precheck_hook;
static add_partial_path_precheck_hook_type prev_add_partial_path_precheck_hook = NULL;

extern set_rel_pathlist_hook_type set_rel_pathlist_hook;
static set_rel_pathlist_hook_type prev_rel_pathlist_hook = NULL;

extern set_join_pathlist_hook_type set_join_pathlist_hook;
static set_join_pathlist_hook_type prev_join_pathlist_hook = NULL;

extern set_baserel_size_estimates_hook_type set_baserel_size_estimates_hook;
static set_baserel_size_estimates_hook_type prev_baserel_size_estimates_hook = NULL;

extern set_joinrel_size_estimates_hook_type set_joinrel_size_estimates_hook;
static set_joinrel_size_estimates_hook_type prev_joinrel_size_estimates_hook = NULL;

extern compute_parallel_worker_hook_type compute_parallel_worker_hook;
static compute_parallel_worker_hook_type prev_compute_parallel_workers_hook = NULL;

extern cost_seqscan_hook_type cost_seqscan_hook;
static cost_seqscan_hook_type prev_cost_seqscan_hook = NULL;

extern cost_index_hook_type cost_index_hook;
static cost_index_hook_type prev_cost_index_hook = NULL;

extern cost_bitmap_heap_scan_hook_type cost_bitmap_heap_scan_hook;
static cost_bitmap_heap_scan_hook_type prev_cost_bitmap_heap_scan_hook = NULL;

extern initial_cost_nestloop_hook_type initial_cost_nestloop_hook;
static initial_cost_nestloop_hook_type prev_initial_cost_nestloop_hook = NULL;
extern final_cost_nestloop_hook_type final_cost_nestloop_hook;
static final_cost_nestloop_hook_type prev_final_cost_nestloop_hook = NULL;

extern initial_cost_hashjoin_hook_type initial_cost_hashjoin_hook;
static initial_cost_hashjoin_hook_type prev_initial_cost_hashjoin_hook = NULL;
extern final_cost_hashjoin_hook_type final_cost_hashjoin_hook;
static final_cost_hashjoin_hook_type prev_final_cost_hashjoin_hook = NULL;

extern initial_cost_mergejoin_hook_type initial_cost_mergejoin_hook;
static initial_cost_mergejoin_hook_type prev_initial_cost_mergejoin_hook = NULL;
extern final_cost_mergejoin_hook_type final_cost_mergejoin_hook;
static final_cost_mergejoin_hook_type prev_final_cost_mergejoin_hook = NULL;

/* extension boilerplate */

extern char **current_planner_type;
extern char **current_join_ordering_type;

extern PGDLLEXPORT void _PG_init(void);
extern PGDLLEXPORT void _PG_fini(void);

extern PGDLLEXPORT PlannedStmt *hint_aware_planner(Query*, const char*, int, ParamListInfo);
extern PGDLLEXPORT void hint_aware_make_one_rel_prep(PlannerInfo*, List*);

extern PGDLLEXPORT RelOptInfo *hint_aware_join_search(PlannerInfo*, int, List*);

extern PGDLLEXPORT void hint_aware_add_path(RelOptInfo*, Path*);

extern PGDLLEXPORT double hint_aware_baserel_size_estimates(PlannerInfo*, RelOptInfo*);
extern PGDLLEXPORT double hint_aware_joinrel_size_estimates(PlannerInfo*, RelOptInfo*,
                                                            RelOptInfo*, RelOptInfo*,
                                                            SpecialJoinInfo*, List*);

extern PGDLLEXPORT void hint_aware_initial_cost_nestloop(PlannerInfo *root,
                                                            JoinCostWorkspace *workspace,
                                                            JoinType jointype,
                                                            Path *outer_path, Path *inner_path,
                                                            JoinPathExtraData *extra);
extern PGDLLEXPORT void hint_aware_final_cost_nestloop(PlannerInfo *root, NestPath *path,
                                                        JoinCostWorkspace *workspace,
                                                        JoinPathExtraData *extra);

extern PGDLLEXPORT void hint_aware_ExecutorEnd(QueryDesc *queryDesc);

extern TempGUC **guc_cleanup_actions;
extern int n_cleanup_actions;


static bool enable_pglab = true;
static bool pglab_check_final_path = true;
static bool trace_pruning = false;

/* Stores the raw query that is currently being optimized in this backend. */
char *current_query_string = NULL;

/*
* We explicitly store the PlannerInfo as a static variable because some low-level routines in the planner do not
* receive it as an argument. But, some of our hint-aware variants of these routines need it.
*/
static PlannerInfo  *current_planner_root = NULL;

/* The hints that are available for the current query. */
static PlannerHints *current_hints = NULL;

#define IS_HINTED() (current_hints != NULL && current_hints->contains_hint)

#define PathRelids(pathptr) (IS_UPPER_REL(pathptr->parent) \
                             ? current_planner_root->all_baserels \
                             : pathptr->parent->relids)
#define FreePath(pathptr) if (!IsA(pathptr, IndexScan)) { pfree(pathptr); }

#define pglab_trace(...) \
    if (trace_pruning) \
    { \
        elog(INFO, __VA_ARGS__); \
    }

static char *
relopt_to_string(RelOptInfo *rel)
{
    int i = -1;
    StringInfo buf = makeStringInfo();
    bool first = true;

    while ((i = bms_next_member(rel->relids, i)) >= 0)
    {
        RangeTblEntry *rte = rt_fetch(i, current_planner_root->parse->rtable);
        if (first)
            first = false;
        else
            appendStringInfo(buf, ", ");
        appendStringInfo(buf, "%s", rte->eref->aliasname);
    }

    return buf->data;
}

static char *
pathtype_to_string(Path *path)
{
    if (PathIsA(path, SeqScan))
        return "SeqScan";
    else if (PathIsA(path, IndexScan))
        return "IndexScan";
    else if (PathIsA(path, IndexOnlyScan))
        return "IndexOnlyScan";
    else if (PathIsA(path, BitmapHeapScan))
        return "BitmapHeapScan";
    else if (PathIsA(path, TidScan))
        return "TidScan";
    else if (PathIsA(path, SubqueryScan))
        return "SubqueryScan";
    else if (PathIsA(path, FunctionScan))
        return "FunctionScan";
    else if (PathIsA(path, ValuesScan))
        return "ValuesScan";
    else if (PathIsA(path, CteScan))
        return "CteScan";
    else if (PathIsA(path, ForeignScan))
        return "ForeignScan";
    else if (PathIsA(path, NestLoop))
        return "NestLoop";
    else if (PathIsA(path, MergeJoin))
        return "MergeJoin";
    else if (PathIsA(path, HashJoin))
        return "HashJoin";
    else if (PathIsA(path, Material))
        return "Material";
    else if (PathIsA(path, Memoize))
        return "Memoize";
    else if (PathIsA(path, Agg))
        return "Aggregate";
    else if (PathIsA(path, Gather))
        return "Gather";
    else if (PathIsA(path, GatherMerge))
        return "GatherMerge";
    else if (PathIsA(path, Sort))
        return "Sort";
    else if (PathIsA(path, Limit))
        return "Limit";
    else if (PathIsA(path, Result))
    {
        switch (path->type)
        {
            case T_ProjectionPath:
                return "Projection";
            case T_MinMaxAggPath:
                return "MinMaxAgg";
            case T_GroupResultPath:
                return "GroupResult";
            case T_Path:
                return "ResultScan";
            default:
                return "<Unknown Result Type>";

        }
    }
    else if (PathIsA(path, Append))
        return "Append";
    else if (PathIsA(path, MergeAppend))
        return "MergeAppend";
    else if (PathIsA(path, Unique))
        return "Unique";
    else if (PathIsA(path, ProjectSet))
        return "ProjectSet";
    else if (PathIsA(path, IncrementalSort))
        return "IncrementalSort";
    else if (PathIsA(path, Group))
        return "Group";
    else if (PathIsA(path, GroupingSet))
        return "GroupingSets";
    else if (PathIsA(path, WindowAgg))
        return "WindowAgg";
    else if (PathIsA(path, SetOp))
        return "SetOp";
    else
        return "<Unknown>";
}

static char *
path_to_string(Path *path)
{
    StringInfo buf = makeStringInfo();
    appendStringInfoString(buf, pathtype_to_string(path));

    switch (path->pathtype)
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_SubqueryScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_ForeignScan:
            appendStringInfo(buf, "[%s]", relopt_to_string(path->parent));
            break;
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
        {
            JoinPath *jpath = (JoinPath *) path;
            appendStringInfo(buf, "(%s, %s)",
                                path_to_string(jpath->outerjoinpath),
                                path_to_string(jpath->innerjoinpath));
            break;
        }
        case T_Append:
        {
            AppendPath *apath;
            ListCell *lc;
            bool first;
            apath = (AppendPath *) path;
            appendStringInfoChar(buf, '(');
            first = true;
            foreach(lc, apath->subpaths)
            {
                Path *subpath = (Path *) lfirst(lc);
                char *substring;
                substring = path_to_string(subpath);
                if (first)
                {
                    appendStringInfoString(buf, substring);
                    first = false;
                }
                else
                {
                    appendStringInfo(buf, ", %s", substring);
                }
            }
            appendStringInfoChar(buf, ')');
            break;
        }
        case T_MergeAppend:
        {
            MergeAppendPath *mapath;
            ListCell *lc;
            bool first;
            mapath = (MergeAppendPath *) path;
            appendStringInfoChar(buf, '(');
            first = true;
            foreach(lc, mapath->subpaths)
            {
                Path *subpath = (Path *) lfirst(lc);
                char *substring;
                substring = path_to_string(subpath);
                if (first)
                {
                    appendStringInfoString(buf, substring);
                    first = false;
                }
                else
                {
                    appendStringInfo(buf, ", %s", substring);
                }
            }
            appendStringInfoChar(buf, ')');
            break;
        }
        case T_Material:
        {
            MaterialPath *mpath;
            mpath = (MaterialPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(mpath->subpath));
            break;
        }
        case T_Memoize:
        {
            MemoizePath *mpath;
            mpath = (MemoizePath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(mpath->subpath));
            break;
        }
        case T_Unique:
        {
            UniquePath *upath;
            upath = (UniquePath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(upath->subpath));
            break;
        }
        case T_ProjectSet:
        {
            ProjectSetPath *ppath;
            ppath = (ProjectSetPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(ppath->subpath));
            break;
        }
        case T_Sort:
        {
            SortPath *spath;
            spath = (SortPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(spath->subpath));
            break;
        }
        case T_IncrementalSort:
        {
            IncrementalSortPath *ispath;
            ispath = (IncrementalSortPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(ispath->spath.subpath));
            break;
        }
        case T_Group:
        {
            GroupPath *gpath;
            gpath = (GroupPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(gpath->subpath));
            break;
        }
        case T_Agg:
        {
            AggPath *apath;
            apath = (AggPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(apath->subpath));
            break;
        }
        case T_GroupingSet:
        {
            GroupingSetsPath *gspath;
            gspath = (GroupingSetsPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(gspath->subpath));
            break;
        }
        case T_WindowAgg:
        {
            WindowAggPath *wpath;
            wpath = (WindowAggPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(wpath->subpath));
            break;
        }
        case T_SetOp:
        {
            SetOpPath *spath;
            spath = (SetOpPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(spath->subpath));
            break;
        }
        case T_Limit:
        {
            LimitPath *lpath;
            lpath = (LimitPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(lpath->subpath));
            break;
        }
        case T_Gather:
        {
            GatherPath *gpath;
            gpath = (GatherPath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(gpath->subpath));
            break;
        }
        case T_GatherMerge:
        {
            GatherMergePath *gmpath;
            gmpath = (GatherMergePath *) path;
            appendStringInfo(buf, "(%s)", path_to_string(gmpath->subpath));
            break;
        }
        case T_Result:
        {
            switch (path->type)
            {
                case T_ProjectionPath:
                {
                    ProjectionPath *ppath;
                    ppath = (ProjectionPath *) path;
                    appendStringInfo(buf, "(%s)", path_to_string(ppath->subpath));
                    break;
                }
                case T_MinMaxAggPath:
                case T_GroupResultPath:
                case T_Path:  /* this is a ResultScan */
                    /* these paths have no further children */
                    break;
                default:
                    elog(WARNING, "pg_lab: path_to_string: unsupported Result path type %d", path->type);
                    break;
            }
            break;
        }
        default:
            elog(WARNING, "pg_lab: path_to_string: unsupported path type %d", path->pathtype);
            break;
    }

    return buf->data;
}

/*
 * Checks, whether the given path is compatible with the join order hint.
 *
 * This includes that
 *
 * a) the corresponding intermediate is computed by the join order, and
 * b) for joins, both input nodes are also part of the join order
 *
 * If no join order is given, this check is automatically passed.
 *
 * If the path path is found is found, `op_hint` will point to the operator
 * hint. This can be disabled by nulling `op_hint`.
 */
static bool
path_satisfies_joinorder(Path *path, JoinOrder *join_order, OperatorHint **op_hint)
{
    Relids       relids;
    JoinOrder   *current_node;
    bool         correct_outer, correct_inner;
    JoinPath    *jpath;

    if (!join_order)
        return true;

    /*
     * Especially for the final path check in final_path_callback() we might encouter an upper-rel path.
     * For such a path, we recurse into the child path until we encouter a scan/join path to check.
     */
    switch (path->pathtype)
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
            /* these are the supported path types */
            break;
        case T_Material:
        {
            MaterialPath *mpath;
            mpath = (MaterialPath *) path;
            return path_satisfies_joinorder(mpath->subpath, join_order, op_hint);
        }
        case T_Memoize:
        {
            MemoizePath *mpath;
            mpath = (MemoizePath *) path;
            return path_satisfies_joinorder(mpath->subpath, join_order, op_hint);
        }
        case T_Gather:
        {
            GatherPath *gpath;
            gpath = (GatherPath *) path;
            return path_satisfies_joinorder(gpath->subpath, join_order, op_hint);
        }
        case T_GatherMerge:
        {
            GatherMergePath *gmpath;
            gmpath = (GatherMergePath *) path;
            return path_satisfies_joinorder(gmpath->subpath, join_order, op_hint);
        }
        case T_Unique:
        {
            UniquePath *upath;
            upath = (UniquePath *) path;
            return path_satisfies_joinorder(upath->subpath, join_order, op_hint);
        }
        case T_ProjectSet:
        {
            ProjectSetPath *ppath;
            ppath = (ProjectSetPath *) path;
            return path_satisfies_joinorder(ppath->subpath, join_order, op_hint);
        }
        case T_Sort:
        {
            SortPath *spath;
            spath = (SortPath *) path;
            return path_satisfies_joinorder(spath->subpath, join_order, op_hint);
        }
        case T_IncrementalSort:
        {
            IncrementalSortPath *ispath;
            ispath = (IncrementalSortPath *) path;
            return path_satisfies_joinorder(ispath->spath.subpath, join_order, op_hint);
        }
        case T_Group:
        {
            GroupPath *gpath;
            gpath = (GroupPath *) path;
            return path_satisfies_joinorder(gpath->subpath, join_order, op_hint);
        }
        case T_Agg:
        {
            AggPath *apath;
            apath = (AggPath *) path;
            return path_satisfies_joinorder(apath->subpath, join_order, op_hint);
        }
        case T_GroupingSet:
        {
            GroupingSetsPath *gspath;
            gspath = (GroupingSetsPath *) path;
            return path_satisfies_joinorder(gspath->subpath, join_order, op_hint);
        }
        case T_WindowAgg:
        {
            WindowAggPath *wpath;
            wpath = (WindowAggPath *) path;
            return path_satisfies_joinorder(wpath->subpath, join_order, op_hint);
        }
        case T_SetOp:
        {
            SetOpPath *spath;
            spath = (SetOpPath *) path;
            return path_satisfies_joinorder(spath->subpath, join_order, op_hint);
        }
        case T_Limit:
        {
            LimitPath *lpath;
            lpath = (LimitPath *) path;
            return path_satisfies_joinorder(lpath->subpath, join_order, op_hint);
        }
        case T_Result:
        {
            /* Result paths are Postgres catch all paths with different concrete operators... */
            switch (path->type)
            {
                case T_ProjectionPath:
                {
                    ProjectionPath *ppath;
                    ppath = (ProjectionPath *) path;
                    return path_satisfies_joinorder(ppath->subpath, join_order, op_hint);
                }
                case T_MinMaxAggPath:  /* min-max-agg derives the result directly from an index. This is essentially a scan. */
                case T_GroupResultPath:
                case T_Path:
                    return true;
                default:
                    ereport(ERROR,
                            errmsg("In path_satisfies_operators: Unsupported Result path type."),
                            errdetail("Path: %s", pathtype_to_string(path)),
                            errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            }

        }
        default:
            ereport(ERROR,
                    errmsg("In path_satisfies_joinorder: Unsupported path type."),
                    errdetail("Path: %s", pathtype_to_string(path)),
                    errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            break;
    }

    relids = PathRelids(path);
    current_node = traverse_join_order(join_order, relids);
    if (!current_node)
    {
        /* the path belongs to an intermediate that is not part of the hinted join order */
        return false;
    }

    if (op_hint)
        *op_hint = current_node->physical_op;

    if (current_node->node_type == BASE_REL)
    {
        /* we are at a leave node, nothing more to check */
        return true;
    }

    /* for join nodes, we also need to make sure that they join their children in the correct order */
    Assert(IsAJoinPath(path));
    jpath = (JoinPath *) path;

    /* we cannot be in an upper rel, yet. Therefore it is safe to access relids directly. */
    correct_outer = bms_equal(jpath->outerjoinpath->parent->relids,
                              current_node->outer_child->relids);
    correct_inner = bms_equal(jpath->innerjoinpath->parent->relids,
                              current_node->inner_child->relids);
    return correct_outer && correct_inner;
}

/*
 * Checks, whether the given path is compatible with the operator hints.
 *
 * This actually includes two different checks:
 *
 * 1. that the path itself has a allowed operator
 * 2. for joins, that the inner input uses materialization/memoization correctly
 */
static bool
path_satisfies_operators(PlannerHints *hints, Path *path, OperatorHint *op_hint)
{
    Relids           relids;
    JoinPath        *jpath;
    MergePath       *merge_path;
    Path            *inner_child;
    OperatorHint    *inner_hint;
    bool             hint_found;
    bool             memo_required, material_required;
    bool             memo_allowed, material_allowed;
    bool             memo_used, material_used;

    if (IS_UPPER_REL(path->parent) || !hints->operator_hints)
    {
        /*
         * We currently only support hints for joins and scans. Everything else needs to be auto-accepted.
         */
        return true;
    }

    /*
     * Especially for the final path check in final_path_callback() we might encouter an upper-rel path.
     * For such a path, we recurse into the child path until we encouter a scan/join path to check.
     */
    switch (path->pathtype)
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
            /* These are the supported path types. We check them below. */
            break;
        case T_Material:
        {
            MaterialPath *mpath;
            mpath = (MaterialPath *) path;
            return path_satisfies_operators(hints, mpath->subpath, op_hint);
        }
        case T_Memoize:
        {
            MemoizePath *mpath;
            mpath = (MemoizePath *) path;
            return path_satisfies_operators(hints, mpath->subpath, op_hint);
        }
        case T_Gather:
        {
            GatherPath *gpath;
            gpath = (GatherPath *) path;
            return path_satisfies_operators(hints, gpath->subpath, op_hint);
        }
        case T_GatherMerge:
        {
            GatherMergePath *gmpath;
            gmpath = (GatherMergePath *) path;
            return path_satisfies_operators(hints, gmpath->subpath, op_hint);
        }
        case T_Unique:
        {
            UniquePath *upath;
            upath = (UniquePath *) path;
            return path_satisfies_operators(hints, upath->subpath, op_hint);
        }
        case T_ProjectSet:
        {
            ProjectSetPath *ppath;
            ppath = (ProjectSetPath *) path;
            return path_satisfies_operators(hints, ppath->subpath, op_hint);
        }
        case T_Sort:
        {
            SortPath *spath;
            spath = (SortPath *) path;
            return path_satisfies_operators(hints, spath->subpath, op_hint);
        }
        case T_IncrementalSort:
        {
            IncrementalSortPath *ispath;
            ispath = (IncrementalSortPath *) path;
            return path_satisfies_operators(hints, ispath->spath.subpath, op_hint);
        }
        case T_Group:
        {
            GroupPath *gpath;
            gpath = (GroupPath *) path;
            return path_satisfies_operators(hints, gpath->subpath, op_hint);
        }
        case T_Agg:
        {
            AggPath *apath;
            apath = (AggPath *) path;
            return path_satisfies_operators(hints, apath->subpath, op_hint);
        }
        case T_GroupingSet:
        {
            GroupingSetsPath *gspath;
            gspath = (GroupingSetsPath *) path;
            return path_satisfies_operators(hints, gspath->subpath, op_hint);
        }
        case T_WindowAgg:
        {
            WindowAggPath *wpath;
            wpath = (WindowAggPath *) path;
            return path_satisfies_operators(hints, wpath->subpath, op_hint);
        }
        case T_SetOp:
        {
            SetOpPath *spath;
            spath = (SetOpPath *) path;
            return path_satisfies_operators(hints, spath->subpath, op_hint);
        }
        case T_Limit:
        {
            LimitPath *lpath;
            lpath = (LimitPath *) path;
            return path_satisfies_operators(hints, lpath->subpath, op_hint);
        }
        case T_Result:
        {
            switch (path->type)
            {
                case T_ProjectionPath:
                {
                    ProjectionPath *ppath;
                    ppath = (ProjectionPath *) path;
                    return path_satisfies_operators(hints, ppath->subpath, op_hint);
                }
                case T_GroupResultPath:
                case T_MinMaxAggPath:
                case T_Path:
                    return true;
                default:
                    ereport(ERROR,
                            errmsg("In path_satisfies_operators: Unsupported Result path type."),
                            errdetail("Path: %s", pathtype_to_string(path)),
                            errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            }

        }
        default:
            ereport(ERROR,
                    errmsg("In path_satisfies_operators: Unsupported path type."),
                    errdetail("Path: %s", pathtype_to_string(path)),
                    errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            break;
    }

    /*
     * Now that we have the sanity checks out of the way, we need to check two things:
     *
     * 1. whether the path itself has the correct operator
     * 2. for joins, whether the inner child makes correct use of materialize/memoize operations
     *
     * We perform both checks in sequence. However, this means that we cannot stop if we do not
     * find a hint for the path's root - it might still be that we have a join with hints on the
     * input nodes. Even, if the join itself is not hinted.
     */

    relids = PathRelids(path);

    if (!op_hint)
    {
        op_hint = (OperatorHint *) hash_search(hints->operator_hints, &relids,
                                               HASH_FIND, &hint_found);
    }

    if (op_hint)
    {
        /* See above comment on why we need to guard this */
        PhysicalOperator requested_op;
        requested_op = op_hint->op;

        if (requested_op == OP_SEQSCAN && !PathIsA(path, SeqScan))
            return false;
        else if (requested_op == OP_IDXSCAN && !(PathIsA(path, IndexScan) || PathIsA(path, IndexOnlyScan)))
            return false;
        else if (requested_op == OP_BITMAPSCAN && !PathIsA(path, BitmapHeapScan))
            return false;
        else if (requested_op == OP_NESTLOOP && !PathIsA(path, NestLoop))
            return false;
        else if (requested_op == OP_HASHJOIN && !PathIsA(path, HashJoin))
            return false;
        else if (requested_op == OP_MERGEJOIN && !PathIsA(path, MergeJoin))
            return false;

        /* if we got to this point, the hinted operator is either OP_UNKNOWN, or the path has the correct operator */
    }

    /*
     * For scans we are done at this point. But for joins, we still need to make sure that the inner input uses
     * materialize/memoize correctly.
     *
     * These checks are a bit complicated, so let's discuss them first:
     *
     * The primary complicating matter is that we allow different hinting modes.
     * In _anchored_ mode, the optimizer is free to insert additional memoize/materialize nodes as it sees fit.
     * But if we have a memoize/materialize hint, we still must use the corresponding operator.
     * In _full_ mode, we must adhere precisely to the given hints. If we do not have a materialize/memoize hint,
     * we cannot use it.
     * Our control logic needs to handle both cases adequately.
     *
     * The second complicating matter is that Postgres uses two different ways to express materialization.
     * The straightforward way is via explicit Material operators. But in additiona, materialization for merge
     * joins is expressed via the special materialize_inner flag on the join itself.
     * Again, our control logic needs to handle both.
     *
     * In the end, we basically implement the following truth tables:
     * (colums are the actual operators in the path, rows are the operators requested by the hints)
     *
     * For _anchored_ mode
     * +=============+========+==========+========+
     * | Hint / Path |  Memo  | Material | Other  |
     * +=============+========+==========+========+
     * | Memo        | accept | reject   | reject |
     * +-------------+--------+----------+--------+
     * | Material    | reject | accept   | reject |
     * +-------------+--------+----------+--------+
     * | None        | accept | accept   | accept |
     * +-------------+--------+----------+--------+
     *
     * For _full_ mode
     * +=============+========+==========+========+
     * | Hint / Path |  Memo  | Material | Other  |
     * +=============+========+==========+========+
     * | Memo        | accept | reject   | reject |
     * +-------------+--------+----------+--------+
     * | Material    | reject | accept   | reject |
     * +-------------+--------+----------+--------+
     * | None        | reject | reject   | accept |
     * +-------------+--------+----------+--------+
     *
     */
    if (!IsAJoinPath(path))
        return true;

    merge_path = PathIsA(path, MergeJoin) ? (MergePath *) path : NULL; /* for materialize_inner */
    jpath = (JoinPath *) path;
    inner_child = jpath->innerjoinpath;

    /* we cannot be in an upper rel, yet. Therefore it is safe to access relids directly. */
    inner_hint = (OperatorHint *) hash_search(hints->operator_hints, &inner_child->parent->relids,
                                              HASH_FIND, &hint_found);

    if (!hint_found)
    {
        /* if there are no restrictions on the inner child, we are done. */
        return true;
    }

    /* first, determine if the hints themselves require a specific operator */
    memo_required = inner_hint->memoize_output;
    material_required = inner_hint->materialize_output;

    /* now, check whether we might use the operator even if our hints do not require it */
    memo_allowed = memo_required || hints->mode == HINTMODE_ANCHORED;
    material_allowed = material_required || hints->mode == HINTMODE_ANCHORED;

    if (PathIsA(inner_child, Memoize))
    {
        return memo_allowed && !material_required;
    }
    else if (PathIsA(inner_child, Material) || (merge_path && merge_path->materialize_inner))
    {
        /* see longer comment above: PG uses two different representations for materialization */
        return material_allowed && !memo_required;
    }
    else
    {
        return !memo_required && !material_required;
    }
}

/*
 * Extracts the root node of the parallel portion of a path.
 *
 * If the path is entirely sequential, NULL is returned. If a parallel portion is found
 * (i.e. the path contains a Gather or GatherMerge node), its immediate subpath is returned.
 *
 * Notice that this function does not work on partial paths, they must already be gathered.
 */
static Path*
find_parallel_subpath(Path *path)
{
    switch (path->pathtype)
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapIndexScan:
        case T_BitmapOr:
        case T_BitmapAnd:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_TidRangeScan:
        case T_SubqueryScan:
        case T_ForeignScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
            return NULL;
        case T_Append:
        {
            AppendPath *apath;
            ListCell   *lc;
            apath = (AppendPath *) path;
            if (apath->first_partial_path < list_length(apath->subpaths))
            {
                ereport(ERROR,
                        errmsg("In find_parallel_subpath: AppendPath with partial paths found"),
                        errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            }
            foreach(lc, apath->subpaths)
            {
                Path *subpath = (Path *) lfirst(lc);
                Path *par_subpath = find_parallel_subpath(subpath);
                if (par_subpath)
                    return par_subpath;
            }
            return NULL;
        }
        case T_MergeAppend:
        {
            MergeAppendPath *mpath;
            ListCell        *lc;
            mpath = (MergeAppendPath *) path;
            /* merge-append doesn't support parallel subpaths, yet */
            foreach(lc, mpath->subpaths)
            {
                Path *subpath = (Path *) lfirst(lc);
                Path *par_subpath = find_parallel_subpath(subpath);
                if (par_subpath)
                    return par_subpath;
            }
            return NULL;
        }
        case T_NestLoop:
        case T_HashJoin:
        case T_MergeJoin:
        {
            JoinPath *jpath;
            Path     *outer, *inner;
            jpath = (JoinPath *) path;
            outer = find_parallel_subpath(jpath->outerjoinpath);
            if (outer)
                return outer;
            inner = find_parallel_subpath(jpath->innerjoinpath);
            return inner;
        }
        case T_Material:
        {
            MaterialPath *mpath;
            mpath = (MaterialPath *) path;
            return find_parallel_subpath(mpath->subpath);
        }
        case T_Memoize:
        {
            MemoizePath *mpath;
            mpath = (MemoizePath *) path;
            return find_parallel_subpath(mpath->subpath);
        }
        case T_Unique:
        {
            UniquePath *upath;
            upath = (UniquePath *) path;
            return find_parallel_subpath(upath->subpath);
        }
        case T_ProjectSet:
        {
            ProjectSetPath *ppath;
            ppath = (ProjectSetPath *) path;
            return find_parallel_subpath(ppath->subpath);
        }
        case T_Sort:
        {
            SortPath *spath;
            spath = (SortPath *) path;
            return find_parallel_subpath(spath->subpath);
        }
        case T_IncrementalSort:
        {
            IncrementalSortPath *ispath;
            ispath = (IncrementalSortPath *) path;
            return find_parallel_subpath(ispath->spath.subpath);
        }
        case T_Group:
        {
            GroupPath *gpath;
            gpath = (GroupPath *) path;
            return find_parallel_subpath(gpath->subpath);
        }
        case T_Agg:
        {
            AggPath *apath;
            apath = (AggPath *) path;
            return find_parallel_subpath(apath->subpath);
        }
        case T_GroupingSet:
        {
            GroupingSetsPath *gspath;
            gspath = (GroupingSetsPath *) path;
            return find_parallel_subpath(gspath->subpath);
        }
        case T_WindowAgg:
        {
            WindowAggPath *wpath;
            wpath = (WindowAggPath *) path;
            return find_parallel_subpath(wpath->subpath);
        }
        case T_SetOp:
        {
            SetOpPath *spath;
            spath = (SetOpPath *) path;
            return find_parallel_subpath(spath->subpath);
        }
        case T_Limit:
        {
            LimitPath *lpath;
            lpath = (LimitPath *) path;
            return find_parallel_subpath(lpath->subpath);
        }
        case T_Result:
        {
            switch (path->type)
            {
                case T_ProjectionPath:
                {
                    ProjectionPath *ppath;
                    ppath = (ProjectionPath *) path;
                    return find_parallel_subpath(ppath->subpath);
                }
                case T_MinMaxAggPath:
                case T_GroupResultPath:
                case T_Path:
                    return NULL;
                default:
                    ereport(ERROR,
                        errmsg("Unsupported Result path type %s in find_parallel_subpath", pathtype_to_string(path)),
                        errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
                    return NULL;
            }
            return NULL;
        }
        case T_Gather:
        {
            GatherPath *gpath;
            gpath = (GatherPath *) path;
            return gpath->subpath;
        }
        case T_GatherMerge:
        {
            GatherMergePath *gmpath;
            gmpath = (GatherMergePath *) path;
            return gmpath->subpath;
        }
        default:
            ereport(ERROR,
                errmsg("Unsupported path type %s in find_parallel_subpath", pathtype_to_string(path)),
                errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            return NULL;
    }

}

/*
 * Checks, if a path from the *pathlist* is compatible with the parallelization hints.
 *
 * NB: This function really only works for paths from the pathlist, i.e. those paths that are
 * either fully sequential, or paths that have already been gathered. Calling this function
 * on partial paths will lead to incorrect results.
 */
static bool
path_satisfies_parallelization(PlannerHints *hints, Path *path)
{
    Path            *par_subpath;
    JoinOrder       *parallel_root;
    BMS_Comparison   bms_comp;

    if (hints->parallel_mode == PARMODE_DEFAULT)
    {
        /* No restrictions on parallelization */
        return true;
    }

    par_subpath = find_parallel_subpath(path);
    if (hints->parallel_mode == PARMODE_SEQUENTIAL)
    {
        /* We should only produce sequential paths. This is easy to enforce.*/
        return par_subpath == NULL;
    }

    /*
     * At this point we know that we should indeed produce a parallel path.
     * This leaves us with two cases:
     *
     * 1. Either the path contains a parallel portion (par_subpath != NULL). In this case, we need to make sure that the
     *    correct part of the path is parallelized.
     * 2. Or, the path is fully sequential (par_subpath == NULL). Even in this case, we still cannot immediately reject the
     *    path. The reason is that the planner might still use this path as the inner relation of a parallel join. Therefore,
     *    we need to check whether this path can actually become such an inner relation.
     */
    Assert(hints->parallel_mode == PARMODE_PARALLEL);

    if (par_subpath)
    {
        /* Case 1: make sure that we did parallelize the correct subpath */

        if (hints->parallelize_entire_plan)
        {
            return IS_UPPER_REL(par_subpath->parent);
        }
        else
        {
            Assert(hints->parallel_rels != NULL);
            return !IS_UPPER_REL(par_subpath->parent)  /* this proofs that parent->relids != NULL */
                && bms_equal(par_subpath->parent->relids, hints->parallel_rels);
        }
    }

    /*
     * We are in case 2: either our path could still be usefull as an inner relation of a parallel join,
     * or we have to reject it (since parallel_mode == PARALLEL)
     */
    if (hints->parallelize_entire_plan || hints->parallel_rels == NULL)
    {
        /*
         * For parallelize_entire_plan, we need to parallelize all joins, so make sure that this is not the last join.
         *
         * The same reasoning applies if parallel_rels == NULL: this indicates that we can parallelize any portion of the plan.
         * Therefore, we just need to make sure that there is still a chance that we can parallelize later on
         * (i.e. in a subsequent join, which in turn implies that this is not allowed to be the final join).
         *
         * NB: we cannot use bms_is_subset here because this check would pass for parent->relids == all_baserels
         *     as well.
         */
        return !IS_UPPER_REL(path->parent)
            && !bms_equal(path->parent->relids, current_planner_root->all_baserels);
    }

    /*
     * This is the final case: we should compute a specific intermediate in parallel. So make sure that our current path can
     * become an inner relation of the parallel join of the intermediate.
     */
    Assert(hints->parallel_rels != NULL);
    if (IS_UPPER_REL(path->parent))
    {
        /* We are already at an upper rel. There is no way to introduce parallelization at this point. Reject. */
        return false;
    }

    /* From this point onwards we can directly access parent->relids since we just proofed that we are not in an upper rel. */
    bms_comp = bms_subset_compare(path->parent->relids, hints->parallel_rels);
    if (bms_comp == BMS_EQUAL || bms_comp == BMS_SUBSET2)
    {
        /* Our path is too far up in the plan. We should have already parallelized. Reject. */
        return false;
    }
    else if (bms_comp == BMS_DIFFERENT)
    {
        /* This path does not belong to the parallel join at all. We can keep it. */
        return true;
    }

    /*
     * Now we did also proof that our path computes a relation that will become part of the parallel join.
     * But can it become the inner relation? We can only really answer this question if we know the final join order.
     * Otherwise, later add_path() calls need to evict the bad paths again.
     */
    if (!hints->join_order_hint)
        return true;

    parallel_root = traverse_join_order(hints->join_order_hint, hints->parallel_rels);
    while (parallel_root)
    {
        if (parallel_root->node_type == BASE_REL)
        {
            /* We reached a base rel without finding the inner child. Reject. */
            return false;
        }
        else if (bms_is_subset(path->parent->relids, parallel_root->inner_child->relids))
        {
            /* Bingo! Our path is on its way to become an inner child! */
            return true;
        }
        else if (bms_is_subset(path->parent->relids, parallel_root->outer_child->relids))
        {
            /* The path could still become an inner relation further down in the plan. Keep checking. */
            parallel_root = parallel_root->outer_child;
        }
        else
        {
            /*
             * Some of the relations from our path belong to the outer child while others belong to the inner one.
             * This will be rejected by the join order check anyhow, but we can also reject the path here since it cannot
             * be part of an inner relation anymore.
             */
            return false;
        }
    }

    ereport(ERROR,
            errmsg("In path_satisfies_parallelization: join order traversal failed unexpectedly."),
            errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
    return false;
}

/*
 * Checks, if a partial path is compatible with the parallielization hints.
 */
static bool
partial_path_satisfies_parallelization(PlannerHints *hints, Path *path)
{
    if (hints->parallel_mode == PARMODE_DEFAULT)
    {
        /* no restrictions on parallelization */
        return true;
    }
    else if (hints->parallel_mode == PARMODE_SEQUENTIAL)
    {
        /* if we should only produce sequential paths, we can stop here */
        return false;
    }

    Assert(hints->parallel_mode == PARMODE_PARALLEL);

    if (hints->parallelize_entire_plan)
    {
        /*
         * We should parallelize the entire plan, so we need to let all
         * parallel "building blocks" pass.
         */
        return true;
    }

    return !IS_UPPER_REL(path->parent)
        &&  bms_is_subset(path->parent->relids, hints->parallel_rels);
}

/*
 * Checks, whether the given path is on any of the join prefixes
 * (i.e. the path's join order is equal or part of one of the prefixes).
 *
 * Notice that this check suceeds in two cases:
 *
 * 1. by default, if one of the join prefixes is found on the path's join order.
 * 2. the path uses a different set of relations than the all of the prefixes
 *
 * In the second case, the current path describes a portion of the query plan
 * that is not restricted by the join prefixes.
 *
 * For example, consider a query
 *
 * SELECT * FROM R, S, T, U
 * WHERE  R.a = S.b AND R.a = T.c AND R.a = U.d
 *  AND S.b = T.c AND S.b = U.d
 *  AND T.c = U.d;
 *
 * with join prefix (R S) - i.e. R should first be joined with S.
 * Now, any path that joins T and U will still be accepted, even though they are not
 * on the join prefix.
 */
static bool
path_satisfies_joinprefixes(Path *path, List *prefixes)
{
    ListCell *lc;
    bool matching_prefix;
    bool all_disjunct;

    if (!prefixes || bms_num_members(path->parent->relids) <= 1)
    {
        /*
         * We don't need to check anything if
         * - there are no prefixes at all, or
         * - if the current path is a base relation (relids == 1)
         * - if the current path is part of the upper rel (relids == 0)
         */
        return true;
    }

    foreach(lc, prefixes)
    {
        JoinOrder *prefix;
        JoinOrder_Comparison cmp;

        prefix = (JoinOrder *) lfirst(lc);
        cmp = join_order_compare(prefix, path, current_planner_root->all_baserels);

        if (cmp == JO_EQUAL)
        {
            /* found a match, we are done */
            matching_prefix = true;
            all_disjunct = false;
            break;
        }
        else if (cmp == JO_DIFFERENT)
        {
            /*
             * There is a prefix for the same portion of the query as our path, but
             * our path is not compatible with the prefix.
             * Continue searching, maybe another prefix matches.
             */
            all_disjunct = false;
        }
        #ifdef  USE_ASSERT_CHECKING
        else
        {
            /*
             * Our path is for a portion of the query that is not included in the prefix.
             */
            Assert(cmp == JO_DISJOINT);
        }
        #endif
    }

    return matching_prefix || all_disjunct;
}


static bool
check_path_recursive(PlannerHints *hints, Path *path, bool is_partial)
{
    OperatorHint *op_hint;

    /*
     * TODO: the current implementation is incredibly inefficient.
     * We basically traverse the entire path from scratch for each check.
     */

    check_stack_depth();

    if (!path_satisfies_joinprefixes(path, hints->join_prefixes))
    {
        pglab_trace("Check failed for path %s - does not satisfy join prefixes", path_to_string(path));
        return false;
    }

    op_hint = NULL;
    if (!path_satisfies_joinorder(path, hints->join_order_hint, &op_hint))
    {
        pglab_trace("Check failed for path %s - does not satisfy join order", path_to_string(path));
        return false;
    }

    if (!path_satisfies_operators(hints, path, op_hint))
    {
        pglab_trace("Check failed for path %s - does not satisfy operator hints", path_to_string(path));
        return false;
    }

    if (is_partial)
    {
        if (!partial_path_satisfies_parallelization(hints, path))
        {
            pglab_trace("Check failed for path %s - does not satisfy parallelization hints", path_to_string(path));
            return false;
        }
    }
    else
    {
        if (!path_satisfies_parallelization(hints, path))
        {
            pglab_trace("Check failed for path %s - does not satisfy parallelization hints", path_to_string(path));
            return false;
        }
    }

    switch (path->pathtype)
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapIndexScan:
        case T_BitmapOr:
        case T_BitmapAnd:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_TidRangeScan:
        case T_SubqueryScan:
        case T_ForeignScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
            /*
             * We are at a scan node and all checks have passed.
             * This path is good to go.
             */
            return true;
        case T_Append:
        {
            AppendPath  *apath;
            ListCell    *lc;
            apath = (AppendPath *) path;
            foreach(lc, apath->subpaths)
            {
                Path *subpath = (Path *) lfirst(lc);
                if (!check_path_recursive(hints, subpath, is_partial))
                    return false;
            }
            return true;
        }
        case T_MergeAppend:
        {
            MergeAppendPath *mpath;
            ListCell        *lc;
            mpath = (MergeAppendPath *) path;
            foreach(lc, mpath->subpaths)
            {
                Path *subpath = (Path *) lfirst(lc);
                if (!check_path_recursive(hints, subpath, is_partial))
                    return false;
            }
            return true;
        }
        case T_NestLoop:
        case T_HashJoin:
        case T_MergeJoin:
        {
            JoinPath *jpath;
            jpath = (JoinPath *) path;
            if (!check_path_recursive(hints, jpath->outerjoinpath, is_partial))
                return false;

            /*
             * For parallel plans, the PG optimizer uses a partial path as the outer path but
             * selects the inner path from the plain pathlist. Naively, one would assume that
             * we can pass is_partial = false to the inner path check. However, this would break
             * the recursive parallelization checks since we require a partial parent path but
             * supplied a sequential inner path. The check has no knowledge that the inner path
             * will actually become part of a partial join later one. To circumvent this issue,
             * we simply pass the same is_partial flag to both sides. Now, the check for the inner
             * path (errorneously) assumes that it is part of a partial path, too.
             */
            if (!check_path_recursive(hints, jpath->innerjoinpath, is_partial))
                return false;
            return true;
        }
        case T_Material:
        {
            MaterialPath *mpath;
            mpath = (MaterialPath *) path;
            return check_path_recursive(hints, mpath->subpath, is_partial);
        }
        case T_Memoize:
        {
            MemoizePath *mpath;
            mpath = (MemoizePath *) path;
            return check_path_recursive(hints, mpath->subpath, is_partial);
        }
        case T_Unique:
        {
            UniquePath *upath;
            upath = (UniquePath *) path;
            return check_path_recursive(hints, upath->subpath, is_partial);
        }
        case T_ProjectSet:
        {
            ProjectSetPath *ppath;
            ppath = (ProjectSetPath *) path;
            return check_path_recursive(hints, ppath->subpath, is_partial);
        }
        case T_Sort:
        {
            SortPath *spath;
            spath = (SortPath *) path;
            return check_path_recursive(hints, spath->subpath, is_partial);
        }
        case T_IncrementalSort:
        {
            IncrementalSortPath *ispath;
            ispath = (IncrementalSortPath *) path;
            return check_path_recursive(hints, ispath->spath.subpath, is_partial);
        }
        case T_Group:
        {
            GroupPath *gpath;
            gpath = (GroupPath *) path;
            return check_path_recursive(hints, gpath->subpath, is_partial);
        }
        case T_Agg:
        {
            AggPath *apath;
            apath = (AggPath *) path;
            return check_path_recursive(hints, apath->subpath, is_partial);
        }
        case T_GroupingSet:
        {
            GroupingSetsPath *gspath;
            gspath = (GroupingSetsPath *) path;
            return check_path_recursive(hints, gspath->subpath, is_partial);
        }
        case T_WindowAgg:
        {
            WindowAggPath *wpath;
            wpath = (WindowAggPath *) path;
            return check_path_recursive(hints, wpath->subpath, is_partial);
        }
        case T_SetOp:
        {
            SetOpPath *spath;
            spath = (SetOpPath *) path;
            return check_path_recursive(hints, spath->subpath, is_partial);
        }
        case T_Limit:
        {
            LimitPath *lpath;
            lpath = (LimitPath *) path;
            return check_path_recursive(hints, lpath->subpath, is_partial);
        }
        case T_Gather:
        {
            GatherPath *gpath;
            gpath = (GatherPath *) path;
            return check_path_recursive(hints, gpath->subpath, true);
        }
        case T_GatherMerge:
        {
            GatherMergePath *gmpath;
            gmpath = (GatherMergePath *) path;
            return check_path_recursive(hints, gmpath->subpath, true);
        }
        case T_Result:
        {
            switch (path->type)
            {
                case T_ProjectionPath:
                {
                    ProjectionPath *ppath;
                    ppath = (ProjectionPath *) path;
                    return check_path_recursive(hints, ppath->subpath, is_partial);
                }
                case T_GroupResultPath:
                case T_MinMaxAggPath:
                case T_Path:
                    return true;
                default:
                    ereport(ERROR,
                            errmsg("In check_path: Unsupported Result path type."),
                            errdetail("Path: %s", path_to_string(path)),
                            errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            }
        }
        default:
            ereport(ERROR,
                    errmsg("In check_path: Unsupported path type."),
                    errdetail("Path: %s", path_to_string(path)),
                    errhint("This is a programming error. Please report at https://github.com/rbergm/pg_lab/issues."));
            return false;
    }
}


/*
 * Our custom planner hook is required because this is the last point during the Postgres planning phase where the raw query
 * string is available. Everywhere down the line, only the parsed Query* node is available. However, the Query* does not
 * contain any comments and hence, no hints.
 *
 * Sadly, in the planner entry point the PlannerInfo is not yet available, so we can only export the raw query string and
 * need to delegate the actual parsing of the hints to the pg_lab-specific make_one_rel_prep() hook.
 */
PlannedStmt *
hint_aware_planner(Query* parse, const char* query_string, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *result;

    current_hints        = NULL;
    current_planner_root = NULL;
    current_query_string = (char*) query_string;

    if (prev_planner_hook)
    {
        current_planner_type = &PLANNER_TYPE_CUSTOM;
        result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    }
    else
    {
        current_planner_type = &PLANNER_TYPE_DEFAULT;
        result = standard_planner(parse, query_string, cursorOptions, boundParams);
    }

    /* we let the context-based memory manager of PG take care of properly freeing our stuff */
    current_hints        = NULL;
    current_planner_root = NULL;
    current_query_string = NULL;

    return result;
}

void
hint_aware_ExecutorEnd(QueryDesc *queryDesc)
{
    ListCell *lc;

    if (IsParallelWorker())
    {
        if (prev_executor_end_hook)
            prev_executor_end_hook(queryDesc);
        else
            standard_ExecutorEnd(queryDesc);
        return;
    }

    for (int i = 0; i < n_cleanup_actions; i++)
    {
        TempGUC *temp_guc = guc_cleanup_actions[i];
        SetConfigOption(temp_guc->guc_name, temp_guc->guc_value, PGC_USERSET, PGC_S_SESSION);
    }

    FreeGucCleanup();

    if (prev_executor_end_hook)
        prev_executor_end_hook(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

/*
 * The make_one_rel_prep() hook is called before the actual planning starts. We use it to extract the hints from our raw
 * query string.
 */
void
hint_aware_make_one_rel_prep(PlannerInfo *root, List *joinlist)
{
    PlannerHints *hints;
    ListCell *lc;

    if (!enable_pglab)
    {
        if (prev_prepare_make_one_rel_hook)
            prev_prepare_make_one_rel_hook(root, joinlist);
        return;
    }

    hints = init_hints(current_query_string);
    parse_hint_block(root, hints);
    post_process_hint_block(hints);

    foreach (lc, hints->temp_gucs)
    {
        TempGUC *temp_guc = (TempGUC *) lfirst(lc);
        SetConfigOption(temp_guc->guc_name, temp_guc->guc_value, PGC_USERSET, PGC_S_SESSION);
    }

    if (prev_prepare_make_one_rel_hook)
        prev_prepare_make_one_rel_hook(root, joinlist);

    current_planner_root = root;
    current_hints = hints;
}

Path *
hint_aware_final_path_callback(PlannerInfo *root, RelOptInfo *rel, Path *best_path)
{
    if (current_hints && current_hints->contains_hint)
    {
        if (pglab_check_final_path &&
            !check_path_recursive(current_hints, best_path, false))
            ereport(ERROR,
                    errmsg("pg_lab could not find a valid path that satisfies all hints."),
                    errdetail("Final path was %s", path_to_string(best_path)),
                    errhint("If you are certain that the hinted query should be valid, please open an issue at https://github.com/rbergm/pg_lab/issues."));
    }

    if (prev_final_path_callback)
        best_path = (*prev_final_path_callback)(root, rel, best_path);

    return best_path;
}

/*
 * We need a custom hook for the add_path_precheck(), because the original function is used to skip access paths if they are
 * definitely not useful (at least according to the native cost model). We need to overwrite this behavior to enforce our
 * current hints.
 */
bool
hint_aware_add_path_precheck(RelOptInfo *parent_rel, Cost startup_cost, Cost total_cost,
                             List *pathkeys, Relids required_outer)
{
    if (current_hints && (current_hints->join_order_hint || current_hints->join_prefixes || current_hints->operator_hints))
        /* XXX: we could be smarter and try to check, whether there actually is a hint concerning the current path */
        return true;

    if (prev_add_path_precheck_hook)
        return (*prev_add_path_precheck_hook)(parent_rel, startup_cost, total_cost, pathkeys, required_outer);
    else
        return standard_add_path_precheck(parent_rel, startup_cost, total_cost, pathkeys, required_outer);
}

/*
 * We need a custom hook for the add_partial_path_precheck(), because the original function is used to skip access paths if
 * they are definitely not useful (at least according to the native cost model). We need to overwrite this behavior to enforce
 * our current hints.
 */
bool
hint_aware_add_partial_path_precheck(RelOptInfo *parent_rel, Cost total_cost, List *pathkeys)
{
    if (current_hints && (current_hints->join_order_hint || current_hints->join_prefixes || current_hints->operator_hints))
        /* XXX: we could be smarter and try to check, whether there actually is a hint concerning the current path */
        return true;

    if (prev_add_partial_path_precheck_hook)
        return (*prev_add_partial_path_precheck_hook)(parent_rel, total_cost, pathkeys);
    else
        return standard_add_partial_path_precheck(parent_rel, total_cost, pathkeys);
}


#define PG_ADD_PATH(rel, path) \
    { \
        if (prev_add_path_hook) \
            (*prev_add_path_hook)(rel, path); \
        else \
            standard_add_path(rel, path); \
    }

#define PG_ADD_PARTIAL_PATH(rel, path) \
    { \
        if (prev_add_partial_path_hook) \
            (*prev_add_partial_path_hook)(rel, path); \
        else \
            standard_add_partial_path(rel, path); \
    }

static void
evict_path(Path *path, bool is_partial)
{
    if (PathIsA(path, IndexScan) || PathIsA(path, IndexOnlyScan))
    {
        return;
    }

    pfree(path);
}


void
hint_aware_add_path(RelOptInfo *parent_rel, Path *path)
{
    OperatorHint *op_hint;

    CHECK_FOR_INTERRUPTS();
    if (!current_hints || !current_hints->contains_hint)
    {
        PG_ADD_PATH(parent_rel, path);
        return;
    }

    if (parent_rel->pathlist == NIL)
    {
        pglab_trace("Accepting placeholder path %s", path_to_string(path));
        PG_ADD_PATH(parent_rel, path);
        return;
    }

    if (!path_satisfies_joinprefixes(path, current_hints->join_prefixes))
    {
        pglab_trace("Rejecting path %s - does not satisfy join prefixes", path_to_string(path));
        evict_path(path, false);
        return;
    }

    op_hint = NULL;
    if (!path_satisfies_joinorder(path, current_hints->join_order_hint, &op_hint))
    {
        pglab_trace("Rejecting path %s - does not satisfy join order", path_to_string(path));
        evict_path(path, false);
        return;
    }

    if (!path_satisfies_operators(current_hints, path, op_hint))
    {
        pglab_trace("Rejecting path %s - does not satisfy operator hints", path_to_string(path));
        evict_path(path, false);
        return;
    }

    if (!path_satisfies_parallelization(current_hints, path))
    {
        pglab_trace("Rejecting path %s - does not satisfy parallelization hints", path_to_string(path));
        evict_path(path, false);
        return;
    }

    /*
     * At this point we will definitely accept the new path. The only remaining question
     * is whether we need to evict one of the existing paths.
     * This happens, if we previously accepted a path not because it satisfied the hints, but because
     * it was the only known path for the relation so far (remember that set_cheapest() errors for empty
     * pathlists and we therefore always need at least one path).
     *
     * This situation may only occur, if there is exactly one path in the pathlist - if there are multiple
     * paths, these had to pass through hint_aware_add_path() already and survived all checks, including
     * this eviction logic.
     *
     * We basically check, if the existing path satisfies all hints. If it does not, we evict it.
     */

    if (list_length(parent_rel->pathlist) == 1)
    {
        Path *placeholder;
        placeholder = (Path *) linitial(parent_rel->pathlist);
        if (!check_path_recursive(current_hints, placeholder, false))
        {
            pglab_trace("Evicting placeholder path %s", path_to_string(placeholder));
            parent_rel->pathlist = list_delete_first(parent_rel->pathlist);
            evict_path(placeholder, false);
        }
    }

    pglab_trace("Accepting path %s", path_to_string(path));
    PG_ADD_PATH(parent_rel, path);
}

void
hint_aware_add_partial_path(RelOptInfo *parent_rel, Path *path)
{
    OperatorHint *op_hint;

    CHECK_FOR_INTERRUPTS();
    if (!current_hints || !current_hints->contains_hint)
    {
        PG_ADD_PARTIAL_PATH(parent_rel, path);
        return;
    }

    if (!path_satisfies_joinprefixes(path, current_hints->join_prefixes))
    {
        pglab_trace("Rejecting partial path %s - does not satisfy join prefixes", path_to_string(path));
        evict_path(path, true);
        return;
    }

    op_hint = NULL;
    if (!path_satisfies_joinorder(path, current_hints->join_order_hint, &op_hint))
    {
        pglab_trace("Rejecting partial path %s - does not satisfy join order", path_to_string(path));
        evict_path(path, true);
        return;
    }

    if (!path_satisfies_operators(current_hints, path, op_hint))
    {
        pglab_trace("Rejecting partial path %s - does not satisfy operator hints", path_to_string(path));
        evict_path(path, true);
        return;
    }

    if (!partial_path_satisfies_parallelization(current_hints, path))
    {
        pglab_trace("Rejecting partial path %s - does not satisfy parallelization hints", path_to_string(path));
        evict_path(path, true);
        return;
    }

    pglab_trace("Accepting partial path %s", path_to_string(path));
    PG_ADD_PARTIAL_PATH(parent_rel, path);
}

int
hint_aware_compute_parallel_workers(RelOptInfo *rel, double heap_pages,
                                    double index_pages, int max_workers)
{
    if (!current_hints || !current_hints->contains_hint)
    {
        if (prev_compute_parallel_workers_hook)
            return (*prev_compute_parallel_workers_hook)(rel, heap_pages, index_pages, max_workers);
        else
            return standard_compute_parallel_worker(rel, heap_pages, index_pages, max_workers);
    }

    if (current_hints->parallelize_entire_plan)
        return current_hints->parallel_workers;
    else if (current_hints->parallel_rels && bms_overlap(current_hints->parallel_rels, rel->relids))
        return current_hints->parallel_workers;

    if (prev_compute_parallel_workers_hook)
        return (*prev_compute_parallel_workers_hook)(rel, heap_pages, index_pages, max_workers);
    else
        return standard_compute_parallel_worker(rel, heap_pages, index_pages, max_workers);
}

static double
set_baserel_size_fallback(PlannerInfo *root, RelOptInfo *rel)
{
    if (prev_baserel_size_estimates_hook)
        return (*prev_baserel_size_estimates_hook)(root, rel);
    else
        return standard_set_baserel_size_estimates(root, rel);
}

double
hint_aware_baserel_size_estimates(PlannerInfo *root, RelOptInfo *rel)
{
    bool hint_found = false;
    CardinalityHint *hint_entry;

    if (!current_hints || !current_hints->cardinality_hints)
        return set_baserel_size_fallback(root, rel);

    hint_entry = (CardinalityHint*) hash_search(current_hints->cardinality_hints, &(rel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return set_baserel_size_fallback(root, rel);

    return hint_entry->card;
}


static double
set_joinrel_size_fallback(PlannerInfo *root, RelOptInfo *rel, RelOptInfo *outer_rel, RelOptInfo *inner_rel, SpecialJoinInfo *sjinfo, List *restrictlist)
{
    if (prev_joinrel_size_estimates_hook)
        return (*prev_joinrel_size_estimates_hook)(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);
    else
        return standard_set_joinrel_size_estimates(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);
}

double
hint_aware_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel, RelOptInfo *outer_rel,
                                 RelOptInfo *inner_rel, SpecialJoinInfo *sjinfo, List *restrictlist)
{
    bool hint_found = false;
    CardinalityHint *hint_entry;

    if (!current_hints || !current_hints->cardinality_hints)
        return set_joinrel_size_fallback(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);

    hint_entry = (CardinalityHint*) hash_search(current_hints->cardinality_hints, &(rel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return set_joinrel_size_fallback(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);

    return hint_entry->card;
}

static Index
locate_reloptinfo(List *candidate_rels, Relids target)
{
    RelOptInfo *current_relopt;
    ListCell *lc;
    foreach(lc, candidate_rels)
    {
        current_relopt = (RelOptInfo*) lfirst(lc);
        if (bms_equal(current_relopt->relids, target))
            return foreach_current_index(lc) + 1;
    }

    Assert(false);
    return 0;
}

static RelOptInfo *
forced_geqo(PlannerInfo *root, int levels_needed, List *initial_rels)
{
    int nrels;
    RelOptInfo *result;
    JoinOrder *current_node;
    JoinOrderIterator *join_order_it;
    int gene_idx;
    Gene *forced_tour;
    GeqoPrivateData priv;

    root->join_search_private = (void*) &priv;
    priv.initial_rels = initial_rels;

    nrels = list_length(initial_rels);
    forced_tour = (Gene*) palloc0(sizeof(Gene) * nrels);
    join_order_it = (JoinOrderIterator*) palloc0(sizeof(JoinOrderIterator));

    Assert(current_hints->join_order_hint);
    joinorder_it_init(join_order_it, current_hints->join_order_hint);

    gene_idx = 0;
    for (int lc_idx = nrels - 1; lc_idx >= 0; --lc_idx)
    {
        current_node = (JoinOrder*) list_nth(join_order_it->current_nodes, lc_idx);
        Assert(current_node->node_type == BASE_REL);
        forced_tour[gene_idx++] = locate_reloptinfo(initial_rels, current_node->relids);
    }

    result = gimme_tree(root, forced_tour, nrels);

    pfree(forced_tour);
    joinorder_it_free(join_order_it);
    root->join_search_private = NULL;

    return result;
}

static RelOptInfo *
join_search_fallback(PlannerInfo *root, int levels_needed, List *initial_rels)
{
    RelOptInfo *result;

    if (prev_join_search_hook)
    {
        current_join_ordering_type = &JOIN_ORDER_TYPE_CUSTOM;
        result = prev_join_search_hook(root, levels_needed, initial_rels);
    }
    else if (enable_geqo && levels_needed >= geqo_threshold)
    {
        current_join_ordering_type = &JOIN_ORDER_TYPE_GEQO;
        result = geqo(root, levels_needed, initial_rels);
    }
    else
    {
        current_join_ordering_type = &JOIN_ORDER_TYPE_STANDARD;
        result = standard_join_search(root, levels_needed, initial_rels);
    }

    return result;
}

/*
 * While we normally enforce the join order in the hint_aware_add_path function, there is one corner-case that is handled here:
 * If the user only supplied a join order and no operator hints, and if additionally the query would be optimized using GEQO,
 * we still want the operator selection to be handled by GEQO. Therefore, we use a special GEQO-style join "search" to generate
 * the final RelOptInfo. In all other cases, we simply fall back to the standard policies.
 */
RelOptInfo *
hint_aware_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
    RelOptInfo *result;
    bool can_geqo;

    if (current_hints && current_hints->join_order_hint)
    {
        current_join_ordering_type = &JOIN_ORDER_TYPE_FORCED;
        can_geqo = enable_geqo && levels_needed >= geqo_threshold && is_linear_join_order(current_hints->join_order_hint);
        if (can_geqo)
            result = forced_geqo(root, levels_needed, initial_rels);
        else
            result = standard_join_search(root, levels_needed, initial_rels);
    }
    else
        result = join_search_fallback(root, levels_needed, initial_rels);

    return result;
}

void
hint_aware_cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Cost startup_cost, total_cost;

    if (prev_cost_seqscan_hook)
        (*prev_cost_seqscan_hook)(path, root, baserel, param_info);
    else
        standard_cost_seqscan(path, root, baserel, param_info);

    if (!current_hints || !current_hints->cost_hints)
        return;

    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &(baserel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.scan_cost.seqscan_startup;
    total_cost = hint_entry->costs.scan_cost.seqscan_total;

    if (!isnan(startup_cost))
        path->startup_cost = startup_cost;
    if (!isnan(total_cost))
        path->total_cost = total_cost;
}

void
hint_aware_cost_idxscan(IndexPath *path, PlannerInfo *root, double loop_count, bool partial_path)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Cost startup_cost, total_cost;

    if (prev_cost_index_hook)
        (*prev_cost_index_hook)(path, root, loop_count, partial_path);
    else
        standard_cost_index(path, root, loop_count, partial_path);

    if (!current_hints || !current_hints->cost_hints)
        return;

    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &(path->path.parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.scan_cost.idxscan_startup;
    total_cost = hint_entry->costs.scan_cost.idxscan_total;

    if (!isnan(startup_cost))
        path->path.startup_cost = startup_cost;
    if (!isnan(total_cost))
        path->path.total_cost = total_cost;
}

void
hint_aware_cost_bitmapscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info,
                           Path *bitmapqual, double loop_count)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Cost startup_cost, total_cost;

    if (prev_cost_bitmap_heap_scan_hook)
        (*prev_cost_bitmap_heap_scan_hook)(path, root, baserel, param_info, bitmapqual, loop_count);
    else
        standard_cost_bitmap_heap_scan(path, root, baserel, param_info, bitmapqual, loop_count);

    if (!current_hints || !current_hints->cost_hints)
        return;

    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &(baserel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.scan_cost.bitmap_startup;
    total_cost = hint_entry->costs.scan_cost.bitmap_total;

    if (!isnan(startup_cost))
        path->startup_cost = startup_cost;
    if (!isnan(total_cost))
        path->total_cost = total_cost;
}

void
hint_aware_initial_cost_nestloop(PlannerInfo *root,
                                 JoinCostWorkspace *workspace,
								 JoinType jointype,
								 Path *outer_path, Path *inner_path,
								 JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Relids join_relids = EMPTY_BITMAP;
    Cost startup_cost, total_cost;

    if (prev_initial_cost_nestloop_hook)
        (*prev_initial_cost_nestloop_hook)(root, workspace, jointype, outer_path, inner_path, extra);
    else
        standard_initial_cost_nestloop(root, workspace, jointype, outer_path, inner_path, extra);

    if (!current_hints || !current_hints->cost_hints)
        return;

    join_relids = bms_add_members(join_relids, outer_path->parent->relids);
    join_relids = bms_add_members(join_relids, inner_path->parent->relids);
    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &join_relids, HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.join_cost.nestloop_startup;
    total_cost = hint_entry->costs.join_cost.nestloop_total;

    if (!isnan(startup_cost))
        workspace->startup_cost = startup_cost;
    if (!isnan(total_cost))
        workspace->total_cost = total_cost;
}

void
hint_aware_final_cost_nestloop(PlannerInfo *root, NestPath *path,
                               JoinCostWorkspace *workspace,
							   JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Path *raw_path;
    Cost startup_cost, total_cost;

    if (prev_final_cost_nestloop_hook)
        (*prev_final_cost_nestloop_hook)(root, path, workspace, extra);
    else
        standard_final_cost_nestloop(root, path, workspace, extra);

    if (!current_hints || !current_hints->cost_hints)
        return;

    raw_path = &(path->jpath.path);
    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &(raw_path->parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.join_cost.nestloop_startup;
    total_cost = hint_entry->costs.join_cost.nestloop_total;

    if (!isnan(startup_cost))
        raw_path->startup_cost = startup_cost;
    if (!isnan(total_cost))
        raw_path->total_cost = total_cost;
}


void
hint_aware_initial_cost_hashjoin(PlannerInfo *root,
                                 JoinCostWorkspace *workspace,
								 JoinType jointype,
								 List *hashclauses,
								 Path *outer_path, Path *inner_path,
								 JoinPathExtraData *extra,
								 bool parallel_hash)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Relids join_relids = EMPTY_BITMAP;
    Cost startup_cost, total_cost;

    if (prev_initial_cost_hashjoin_hook)
        (*prev_initial_cost_hashjoin_hook)(root, workspace, jointype, hashclauses, outer_path, inner_path, extra, parallel_hash);
    else
        standard_initial_cost_hashjoin(root, workspace, jointype, hashclauses, outer_path, inner_path, extra, parallel_hash);

    if (!current_hints || !current_hints->cost_hints)
        return;

    join_relids = bms_add_members(join_relids, outer_path->parent->relids);
    join_relids = bms_add_members(join_relids, inner_path->parent->relids);
    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &join_relids, HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.join_cost.hash_startup;
    total_cost = hint_entry->costs.join_cost.hash_total;

    if (!isnan(startup_cost))
        workspace->startup_cost = startup_cost;
    if (!isnan(total_cost))
        workspace->total_cost = total_cost;
}

void
hint_aware_final_cost_hashjoin(PlannerInfo *root, HashPath *path,
                               JoinCostWorkspace *workspace,
                               JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Path *raw_path;
    Cost startup_cost, total_cost;

    if (prev_final_cost_hashjoin_hook)
        (*prev_final_cost_hashjoin_hook)(root, path, workspace, extra);
    else
        standard_final_cost_hashjoin(root, path, workspace, extra);

    if (!current_hints || !current_hints->cost_hints)
        return;

    raw_path = &(path->jpath.path);
    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &(raw_path->parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.join_cost.hash_startup;
    total_cost = hint_entry->costs.join_cost.hash_total;

    if (!isnan(startup_cost))
        raw_path->startup_cost = startup_cost;
    if (!isnan(total_cost))
        raw_path->total_cost = total_cost;
}

void
hint_aware_intial_cost_mergejoin(PlannerInfo *root,
                                 JoinCostWorkspace *workspace,
                                 JoinType jointype,
                                 List *mergeclauses,
                                 Path *outer_path, Path *inner_path,
                                 List *outersortkeys, List *innersortkeys,
                                 JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Relids join_relids = EMPTY_BITMAP;
    Cost startup_cost, total_cost;

    if (prev_initial_cost_mergejoin_hook)
        (*prev_initial_cost_mergejoin_hook)(root, workspace, jointype, mergeclauses, outer_path, inner_path,
                                            outersortkeys, innersortkeys, extra);
    else
        standard_initial_cost_mergejoin(root, workspace, jointype, mergeclauses, outer_path, inner_path,
                                        outersortkeys, innersortkeys, extra);

    if (!current_hints || !current_hints->cost_hints)
        return;

    join_relids = bms_add_members(join_relids, outer_path->parent->relids);
    join_relids = bms_add_members(join_relids, inner_path->parent->relids);
    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &join_relids, HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.join_cost.merge_startup;
    total_cost = hint_entry->costs.join_cost.merge_total;

    if (!isnan(startup_cost))
        workspace->startup_cost = startup_cost;
    if (!isnan(total_cost))
        workspace->total_cost = total_cost;
}

void
hint_aware_final_cost_mergejoin(PlannerInfo *root, MergePath *path,
                                JoinCostWorkspace *workspace,
                                JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHint *hint_entry;
    Path *raw_path;
    Cost startup_cost, total_cost;

    if (prev_final_cost_mergejoin_hook)
        (*prev_final_cost_mergejoin_hook)(root, path, workspace, extra);
    else
        standard_final_cost_mergejoin(root, path, workspace, extra);

    if (!current_hints || !current_hints->cost_hints)
        return;

    raw_path = &(path->jpath.path);
    hint_entry = (CostHint*) hash_search(current_hints->cost_hints, &(raw_path->parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    startup_cost = hint_entry->costs.join_cost.merge_startup;
    total_cost = hint_entry->costs.join_cost.merge_total;

    if (!isnan(startup_cost))
        raw_path->startup_cost = startup_cost;
    if (!isnan(total_cost))
        raw_path->total_cost = total_cost;
}

static char *
debug_reloptinfo(RelOptInfo *rel)
{
    StringInfo buf;
    int i = -1;
    buf = makeStringInfo();

    while ((i = bms_next_member(rel->relids, i)) >= 0)
    {
        RangeTblEntry *rte;
        rte = current_planner_root->simple_rte_array[i];
        if (!rte)
            continue;

        appendStringInfo(buf, "%s ", rte->eref->aliasname);
    }

    return buf->data;
}

static Path *
debug_fetch_outer(Path *path)
{
    JoinPath *jpath;

    if (!IsAJoinPath(path))
        return NULL;

    jpath = (JoinPath *) path;
    return jpath->outerjoinpath;
}

static Path *
debug_fetch_inner(Path *path)
{
    JoinPath *jpath;

    if (!IsAJoinPath(path))
        return NULL;

    jpath = (JoinPath *) path;
    return jpath->innerjoinpath;
}


void
_PG_init(void)
{
    DefineCustomBoolVariable("enable_pglab",
                             "Enable plan hinting via pg_lab", NULL,
                             &enable_pglab, true,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    DefineCustomBoolVariable("pglab.check_final_path",
                             "If enabled, checks whether the final path satisfies all hints and errors if not.", NULL,
                             &pglab_check_final_path, true,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    DefineCustomBoolVariable("pglab.trace",
                             "Show detailed tracing information during path pruning.", NULL,
                             &trace_pruning, false,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    prev_planner_hook = planner_hook;
    planner_hook = hint_aware_planner;

    prev_prepare_make_one_rel_hook = prepare_make_one_rel_callback;
    prepare_make_one_rel_callback = hint_aware_make_one_rel_prep;

    prev_final_path_callback = final_path_callback;
    final_path_callback = hint_aware_final_path_callback;

    prev_join_search_hook = join_search_hook;
    join_search_hook = hint_aware_join_search;

    prev_add_path_hook = add_path_hook;
    add_path_hook = hint_aware_add_path;

    prev_add_partial_path_hook = add_partial_path_hook;
    add_partial_path_hook = hint_aware_add_partial_path;

    prev_add_path_precheck_hook = add_path_precheck_hook;
    add_path_precheck_hook = hint_aware_add_path_precheck;

    prev_add_partial_path_precheck_hook = add_partial_path_precheck_hook;
    add_partial_path_precheck_hook = hint_aware_add_partial_path_precheck;

    prev_baserel_size_estimates_hook = set_baserel_size_estimates_hook;
    set_baserel_size_estimates_hook = hint_aware_baserel_size_estimates;

    prev_joinrel_size_estimates_hook = set_joinrel_size_estimates_hook;
    set_joinrel_size_estimates_hook = hint_aware_joinrel_size_estimates;

    prev_compute_parallel_workers_hook = compute_parallel_worker_hook;
    compute_parallel_worker_hook = hint_aware_compute_parallel_workers;

    prev_cost_seqscan_hook = cost_seqscan_hook;
    cost_seqscan_hook = hint_aware_cost_seqscan;

    prev_cost_index_hook = cost_index_hook;
    cost_index_hook = hint_aware_cost_idxscan;

    prev_cost_bitmap_heap_scan_hook = cost_bitmap_heap_scan_hook;
    cost_bitmap_heap_scan_hook = hint_aware_cost_bitmapscan;

    prev_initial_cost_nestloop_hook = initial_cost_nestloop_hook;
    initial_cost_nestloop_hook = hint_aware_initial_cost_nestloop;
    prev_final_cost_nestloop_hook = final_cost_nestloop_hook;
    final_cost_nestloop_hook = hint_aware_final_cost_nestloop;

    prev_initial_cost_hashjoin_hook = initial_cost_hashjoin_hook;
    initial_cost_hashjoin_hook = hint_aware_initial_cost_hashjoin;
    prev_final_cost_hashjoin_hook = final_cost_hashjoin_hook;
    final_cost_hashjoin_hook = hint_aware_final_cost_hashjoin;

    prev_initial_cost_mergejoin_hook = initial_cost_mergejoin_hook;
    initial_cost_mergejoin_hook = hint_aware_intial_cost_mergejoin;
    prev_final_cost_mergejoin_hook = final_cost_mergejoin_hook;
    final_cost_mergejoin_hook = hint_aware_final_cost_mergejoin;

    prev_executor_end_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = hint_aware_ExecutorEnd;
}

void
_PG_fini(void)
{
    planner_hook = prev_planner_hook;
    prepare_make_one_rel_callback = prev_prepare_make_one_rel_hook;
    final_path_callback = prev_final_path_callback;
    join_search_hook = prev_join_search_hook;
    add_path_hook = prev_add_path_hook;
    add_partial_path_hook = prev_add_partial_path_hook;
    add_path_precheck_hook = prev_add_path_precheck_hook;
    add_partial_path_precheck_hook = prev_add_partial_path_precheck_hook;
    set_baserel_size_estimates_hook = prev_baserel_size_estimates_hook;
    set_joinrel_size_estimates_hook = prev_joinrel_size_estimates_hook;
    cost_seqscan_hook = prev_cost_seqscan_hook;
    cost_index_hook = prev_cost_index_hook;
    cost_bitmap_heap_scan_hook = prev_cost_bitmap_heap_scan_hook;
    initial_cost_nestloop_hook = prev_initial_cost_nestloop_hook;
    final_cost_nestloop_hook = prev_final_cost_nestloop_hook;
    initial_cost_hashjoin_hook = prev_initial_cost_hashjoin_hook;
    final_cost_hashjoin_hook = prev_final_cost_hashjoin_hook;
    initial_cost_mergejoin_hook = prev_initial_cost_mergejoin_hook;
    final_cost_mergejoin_hook = prev_final_cost_mergejoin_hook;
    compute_parallel_worker_hook = prev_compute_parallel_workers_hook;
    ExecutorEnd_hook = prev_executor_end_hook;
}


#ifdef __cplusplus
}  /* extern "C" */
#endif