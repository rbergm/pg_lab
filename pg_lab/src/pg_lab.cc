
extern "C" {
#include <limits.h>

#include "postgres.h"

#include "commands/explain.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "fmgr.h"
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
}

/* The parser is our C++ interface -- hence no extern "C" */
#include "hint_parser.h"

extern "C" {
    PG_MODULE_MAGIC;

    /* GeQO GUC variables */
    extern bool enable_geqo;
    extern int geqo_threshold;

    extern bool enable_material;

    /* Existing optimizer hooks */
    extern planner_hook_type planner_hook;
    static planner_hook_type prev_planner_hook = NULL;

    extern prepare_make_one_rel_hook_type prepare_make_one_rel_hook;
    static prepare_make_one_rel_hook_type prev_prepare_make_one_rel_hook = NULL;

    extern join_search_hook_type join_search_hook;
    static join_search_hook_type prev_join_search_hook = NULL;

    extern add_path_hook_type add_path_hook;
    static add_path_hook_type prev_add_path_hook = NULL;

    extern add_partial_path_hook_type add_partial_path_hook;
    static add_partial_path_hook_type prev_add_partial_path_hook = NULL;

    extern set_rel_pathlist_hook_type set_rel_pathlist_hook;
    static set_rel_pathlist_hook_type prev_rel_pathlist_hook = NULL;

    extern set_join_pathlist_hook_type set_join_pathlist_hook;
    static set_join_pathlist_hook_type prev_join_pathlist_hook = NULL;

    extern set_baserel_size_estimates_hook_type set_baserel_size_estimates_hook;
    static set_baserel_size_estimates_hook_type prev_baserel_size_estimates_hook = NULL;

    extern set_joinrel_size_estimates_hook_type set_joinrel_size_estimates_hook;
    static set_joinrel_size_estimates_hook_type prev_joinrel_size_estimates_hook = NULL;

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

}

/* Stores the raw query that is currently being optimized in this backend. */
char *current_query_string = NULL;

static PlannerInfo *current_planner_root = NULL;
static PlannerHints *current_hints = NULL;

static RelOptInfo *
fetch_reloptinfo(PlannerInfo *root, Relids relids)
{
    List        *candidate_relopts;
    ListCell    *lc;
    int          level;

    level = bms_num_members(relids);
    candidate_relopts = root->join_rel_level[level];

    foreach (lc, candidate_relopts)
    {
        RelOptInfo *rel = (RelOptInfo *) lfirst(lc);
        if (bms_equal(rel->relids, relids))
            return rel;
    }

    return NULL;
}

RelOptInfo *
force_join_order(PlannerInfo* root, int levels_needed, List* initial_rels)
{
    JoinOrder           *join_order;
    JoinOrderIterator   iterator;
    JoinOrder           *current_node;
    ListCell            *lc;
    RelOptInfo          *final_rel;

    /* Initialize important PlannerInfo data, same as standard_join_search() does */
    root->join_rel_level = (List **) palloc0((levels_needed + 1) * sizeof(List *));
    root->join_rel_level[1] = initial_rels;

    /* Initialize join order iterator */
    join_order = ((PlannerHints *) root->join_search_private)->join_order_hint;
    init_join_order_iterator(&iterator, join_order);
    join_order_iterator_next(&iterator); /* Iterator starts at base rels, skip these */

    /* Main "optimization" loop */
    while (!iterator.done)
    {
        foreach (lc, iterator.current_nodes)
        {
            RelOptInfo *outer_rel = NULL;
            RelOptInfo *inner_rel = NULL;
            RelOptInfo *join_rel = NULL;
            current_node = (JoinOrder*) lfirst(lc);
            Assert(current_node->node_type == JOIN_REL);

            outer_rel = fetch_reloptinfo(root, current_node->outer_child->relids);
            inner_rel = fetch_reloptinfo(root, current_node->inner_child->relids);
            Assert(outer_rel); Assert(inner_rel);

            /* XXX: this way we cannot force join directions. But: do we really need to? */
            root->join_cur_level = bms_num_members(current_node->relids);
            join_rel = make_join_rel(root, outer_rel, inner_rel);

            /* Post-process the join_rel same way as standard_join_search() does */
            generate_partitionwise_join_paths(root, join_rel);
            if (!bms_equal(join_rel->relids, root->all_query_rels))
				generate_useful_gather_paths(root, join_rel, false);
            set_cheapest(join_rel);
        }

        join_order_iterator_next(&iterator);
    }

    /* Post-process generated relations, same as standard_join_search() does */
    final_rel = (RelOptInfo *) linitial(root->join_rel_level[levels_needed]);
    root->join_rel_level = NULL;

    free_join_order_iterator(&iterator);
    return final_rel;
}


extern "C" PlannedStmt *
hint_aware_planner(Query* parse, const char* query_string, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *result = NULL;
    current_hints = NULL;
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

    current_hints = NULL;
    current_planner_root = NULL;
    current_query_string = NULL;
    return result;
}

typedef void (*add_path_handler_type) (RelOptInfo *parent_rel, Path *new_path);

/* utility macros inspired by nodeTag and IsA from nodes.h */
#define pathTag(pathptr) (((const Path*)pathptr)->pathtype)
#define PathIsA(nodeptr,_type_) (pathTag(nodeptr) == T_ ## _type_)
#define IsAScanPath(pathptr) (PathIsA(pathptr, SeqScan) || PathIsA(pathptr, IndexScan) || PathIsA(pathptr, BitmapHeapScan))
#define IsAJoinPath(pathptr) (PathIsA(pathptr, NestLoop) || PathIsA(pathptr, MergeJoin) || PathIsA(pathptr, HashJoin))

void add_path_handler(add_path_handler_type handler, RelOptInfo *parent_rel, Path *new_path)
{
    Relids parent_relids;
    bool hint_found;
    OperatorHashEntry *hint_entry;
    bool reject_path = false;


    if (!current_hints || !current_hints->contains_hint)
    {
        (*handler)(parent_rel, new_path);
        return;
    }

    if (current_hints->join_order_hint && parent_rel->reloptkind == RELOPT_JOINREL)
    {
        JoinOrder *jnode;
        JoinPath *jpath;

        jnode = traverse_join_order(current_hints->join_order_hint, parent_rel->relids);
        if (!jnode || !IsAJoinPath(new_path))
        {
            (*handler)(parent_rel, new_path);
            return;
        }

        jpath = (JoinPath*) new_path;
        if (!bms_equal(jnode->inner_child->relids, jpath->innerjoinpath->parent->relids))
            return;
        if (!bms_equal(jnode->outer_child->relids, jpath->outerjoinpath->parent->relids))
            return;
    }

    if (!IsAScanPath(new_path) && !IsAJoinPath(new_path))
    {
        (*handler)(parent_rel, new_path);
        return;
    }

    hint_entry = (OperatorHashEntry*) hash_search(current_hints->operator_hints, &(parent_rel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
    {
        (*handler)(parent_rel, new_path);
        return;
    }

    if (hint_entry->op == SEQSCAN && !PathIsA(new_path, SeqScan))
        reject_path = true;
    else if (hint_entry->op == IDXSCAN && !PathIsA(new_path, IndexScan) && !PathIsA(new_path, BitmapHeapScan))
        reject_path = true;
    else if (hint_entry->op == NESTLOOP && !PathIsA(new_path, NestLoop))
        reject_path = true;
    else if (hint_entry->op == MERGEJOIN && !PathIsA(new_path, MergeJoin))
        reject_path = true;
    else if (hint_entry->op == HASHJOIN && !PathIsA(new_path, HashJoin))
        reject_path = true;

    if (!reject_path)
        (*handler)(parent_rel, new_path);
}

extern "C" void
hint_aware_add_path(RelOptInfo *parent_rel, Path *new_path)
{
    add_path_handler(standard_add_path, parent_rel, new_path);
}

extern "C" void
hint_aware_add_partial_path(RelOptInfo *parent_rel, Path *new_path)
{
    add_path_handler(standard_add_partial_path, parent_rel, new_path);
}

static double
set_baserel_size_fallback(PlannerInfo *root, RelOptInfo *rel)
{
    if (prev_baserel_size_estimates_hook)
        return (*prev_baserel_size_estimates_hook)(root, rel);
    else
        return standard_set_baserel_size_estimates(root, rel);
}

extern "C" double
hint_aware_baserel_size_estimates(PlannerInfo *root, RelOptInfo *rel)
{
    bool hint_found = false;
    CardinalityHashEntry *hint_entry;

    if (prev_baserel_size_estimates_hook)
        return set_baserel_size_fallback(root, rel);
    if (!current_hints || !current_hints->contains_hint)
        return set_baserel_size_fallback(root, rel);

    hint_entry = (CardinalityHashEntry*) hash_search(current_hints->cardinality_hints, &(rel->relids), HASH_FIND, &hint_found);
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

extern "C" double
hint_aware_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel, RelOptInfo *outer_rel,
                                 RelOptInfo *inner_rel, SpecialJoinInfo *sjinfo, List *restrictlist)
{
    bool hint_found = false;
    CardinalityHashEntry *hint_entry;

    if (!current_hints || !current_hints->contains_hint)
        return set_joinrel_size_fallback(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);

    hint_entry = (CardinalityHashEntry*) hash_search(current_hints->cardinality_hints, &(rel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return set_joinrel_size_fallback(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);

    return hint_entry->card;
}

extern "C" void
hint_aware_make_one_rel_prep(PlannerInfo *root, List *joinlist)
{
    PlannerHints *hints = (PlannerHints*) palloc0(sizeof(PlannerHints));
    init_hints(root, hints);

    if (prev_prepare_make_one_rel_hook)
        prev_prepare_make_one_rel_hook(root, joinlist);

    current_planner_root = root;
    current_hints = hints;
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
    ListCell *lc;

    root->join_search_private = (void*) &priv;
    priv.initial_rels = initial_rels;

    nrels = list_length(initial_rels);
    forced_tour = (Gene*) palloc0(sizeof(Gene) * nrels);
    join_order_it = (JoinOrderIterator*) palloc0(sizeof(JoinOrderIterator));

    Assert(current_hints->join_order_hint);
    init_join_order_iterator(join_order_it, current_hints->join_order_hint);

    gene_idx = 0;
    for (int lc_idx = nrels - 1; lc_idx >= 0; --lc_idx)
    {
        current_node = (JoinOrder*) list_nth(join_order_it->current_nodes, lc_idx);
        Assert(current_node->node_type == BASE_REL);
        forced_tour[gene_idx++] = locate_reloptinfo(initial_rels, current_node->relids);
    }

    result = gimme_tree(root, forced_tour, nrels);

    pfree(forced_tour);
    free_join_order_iterator(join_order_it);
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

extern "C" RelOptInfo *
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

extern "C" void
hint_aware_cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;

    if (prev_cost_seqscan_hook)
        (*prev_cost_seqscan_hook)(path, root, baserel, param_info);
    else
        standard_cost_seqscan(path, root, baserel, param_info);

    if (!current_hints || !current_hints->contains_hint)
        return;

    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &(baserel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    path->startup_cost = hint_entry->costs.scan_cost.seqscan_startup;
    path->total_cost = hint_entry->costs.scan_cost.seqscan_total;
}

extern "C" void
hint_aware_cost_idxscan(IndexPath *path, PlannerInfo *root, double loop_count, bool partial_path)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;

    if (prev_cost_index_hook)
        (*prev_cost_index_hook)(path, root, loop_count, partial_path);
    else
        standard_cost_index(path, root, loop_count, partial_path);

    if (!current_hints || !current_hints->contains_hint)
        return;

    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &(path->path.parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    path->path.startup_cost = hint_entry->costs.scan_cost.idxscan_startup;
    path->path.total_cost = hint_entry->costs.scan_cost.idxscan_total;
}

extern "C" void
hint_aware_cost_bitmapscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info,
                           Path *bitmapqual, double loop_count)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;

    if (prev_cost_bitmap_heap_scan_hook)
        (*prev_cost_bitmap_heap_scan_hook)(path, root, baserel, param_info, bitmapqual, loop_count);
    else
        standard_cost_bitmap_heap_scan(path, root, baserel, param_info, bitmapqual, loop_count);

    if (!current_hints || !current_hints->contains_hint)
        return;

    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &(baserel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    path->startup_cost = hint_entry->costs.scan_cost.bitmap_startup;
    path->total_cost = hint_entry->costs.scan_cost.bitmap_total;
}

extern "C" void
hint_aware_initial_cost_nestloop(PlannerInfo *root,
                                 JoinCostWorkspace *workspace,
								 JoinType jointype,
								 Path *outer_path, Path *inner_path,
								 JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;
    Relids join_relids = EMPTY_BITMAP;

    if (prev_initial_cost_nestloop_hook)
        (*prev_initial_cost_nestloop_hook)(root, workspace, jointype, outer_path, inner_path, extra);
    else
        standard_initial_cost_nestloop(root, workspace, jointype, outer_path, inner_path, extra);

    if (!current_hints || !current_hints->contains_hint)
        return;

    join_relids = bms_add_members(join_relids, outer_path->parent->relids);
    join_relids = bms_add_members(join_relids, inner_path->parent->relids);
    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &join_relids, HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    workspace->startup_cost = hint_entry->costs.join_cost.nestloop_startup;
    workspace->total_cost = hint_entry->costs.join_cost.nestloop_total;
}

extern "C" void
hint_aware_final_cost_nestloop(PlannerInfo *root, NestPath *path,
                               JoinCostWorkspace *workspace,
							   JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;
    Path *raw_path;
    Relids relids;

    if (prev_final_cost_nestloop_hook)
        (*prev_final_cost_nestloop_hook)(root, path, workspace, extra);
    else
        standard_final_cost_nestloop(root, path, workspace, extra);

    if (!current_hints || !current_hints->contains_hint)
        return;

    raw_path = &(path->jpath.path);
    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &(raw_path->parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    raw_path->startup_cost = hint_entry->costs.join_cost.nestloop_startup;
    raw_path->total_cost = hint_entry->costs.join_cost.nestloop_total;
}


extern "C" void
hint_aware_initial_cost_hashjoin(PlannerInfo *root,
                                 JoinCostWorkspace *workspace,
								 JoinType jointype,
								 List *hashclauses,
								 Path *outer_path, Path *inner_path,
								 JoinPathExtraData *extra,
								 bool parallel_hash)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;
    Relids join_relids = EMPTY_BITMAP;

    if (prev_initial_cost_hashjoin_hook)
        (*prev_initial_cost_hashjoin_hook)(root, workspace, jointype, hashclauses, outer_path, inner_path, extra, parallel_hash);
    else
        standard_initial_cost_hashjoin(root, workspace, jointype, hashclauses, outer_path, inner_path, extra, parallel_hash);

    if (!current_hints || !current_hints->contains_hint)
        return;

    join_relids = bms_add_members(join_relids, outer_path->parent->relids);
    join_relids = bms_add_members(join_relids, inner_path->parent->relids);
    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &join_relids, HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    workspace->startup_cost = hint_entry->costs.join_cost.hash_startup;
    workspace->total_cost = hint_entry->costs.join_cost.hash_total;
}

extern "C" void
hint_aware_final_cost_hashjoin(PlannerInfo *root, HashPath *path,
                               JoinCostWorkspace *workspace,
                               JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;
    Path *raw_path;
    Relids relids;

    if (prev_final_cost_hashjoin_hook)
        (*prev_final_cost_hashjoin_hook)(root, path, workspace, extra);
    else
        standard_final_cost_hashjoin(root, path, workspace, extra);

    if (!current_hints || !current_hints->contains_hint)
        return;

    raw_path = &(path->jpath.path);
    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &(raw_path->parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    raw_path->startup_cost = hint_entry->costs.join_cost.hash_startup;
    raw_path->total_cost = hint_entry->costs.join_cost.hash_total;
}

extern "C" void
hint_aware_intial_cost_mergejoin(PlannerInfo *root,
                                 JoinCostWorkspace *workspace,
                                 JoinType jointype,
                                 List *mergeclauses,
                                 Path *outer_path, Path *inner_path,
                                 List *outersortkeys, List *innersortkeys,
                                 JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;
    Relids join_relids = EMPTY_BITMAP;

    if (prev_initial_cost_mergejoin_hook)
        (*prev_initial_cost_mergejoin_hook)(root, workspace, jointype, mergeclauses, outer_path, inner_path,
                                            outersortkeys, innersortkeys, extra);
    else
        standard_initial_cost_mergejoin(root, workspace, jointype, mergeclauses, outer_path, inner_path,
                                        outersortkeys, innersortkeys, extra);

    if (!current_hints || !current_hints->contains_hint)
        return;

    join_relids = bms_add_members(join_relids, outer_path->parent->relids);
    join_relids = bms_add_members(join_relids, inner_path->parent->relids);
    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &join_relids, HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    workspace->startup_cost = hint_entry->costs.join_cost.merge_startup;
    workspace->total_cost = hint_entry->costs.join_cost.merge_total;
}

extern "C" void
hint_aware_final_cost_mergejoin(PlannerInfo *root, MergePath *path,
                                JoinCostWorkspace *workspace,
                                JoinPathExtraData *extra)
{
    bool hint_found = false;
    CostHashEntry *hint_entry;
    Path *raw_path;
    Relids relids;

    if (prev_final_cost_mergejoin_hook)
        (*prev_final_cost_mergejoin_hook)(root, path, workspace, extra);
    else
        standard_final_cost_mergejoin(root, path, workspace, extra);

    if (!current_hints || !current_hints->contains_hint)
        return;

    raw_path = &(path->jpath.path);
    hint_entry = (CostHashEntry*) hash_search(current_hints->cost_hints, &(raw_path->parent->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return;

    raw_path->startup_cost = hint_entry->costs.join_cost.merge_startup;
    raw_path->total_cost = hint_entry->costs.join_cost.merge_total;
}

extern "C" void
_PG_init(void)
{
    prev_planner_hook = planner_hook;
    planner_hook = hint_aware_planner;

    prev_prepare_make_one_rel_hook = prepare_make_one_rel_hook;
    prepare_make_one_rel_hook = hint_aware_make_one_rel_prep;

    prev_join_search_hook = join_search_hook;
    join_search_hook = hint_aware_join_search;

    prev_add_path_hook = add_path_hook;
    add_path_hook = hint_aware_add_path;

    prev_add_partial_path_hook = add_partial_path_hook;
    add_partial_path_hook = hint_aware_add_partial_path;

    prev_baserel_size_estimates_hook = set_baserel_size_estimates_hook;
    set_baserel_size_estimates_hook = hint_aware_baserel_size_estimates;

    prev_joinrel_size_estimates_hook = set_joinrel_size_estimates_hook;
    set_joinrel_size_estimates_hook = hint_aware_joinrel_size_estimates;

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
}

extern "C" void
_PG_fini(void)
{
    planner_hook = prev_planner_hook;
    prepare_make_one_rel_hook = prev_prepare_make_one_rel_hook;
    join_search_hook = prev_join_search_hook;
    set_rel_pathlist_hook = prev_rel_pathlist_hook;
    set_join_pathlist_hook = prev_join_pathlist_hook;
    set_baserel_size_estimates_hook = prev_baserel_size_estimates_hook;
    set_joinrel_size_estimates_hook = prev_joinrel_size_estimates_hook;
}
