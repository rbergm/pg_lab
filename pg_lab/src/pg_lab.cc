
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
#include "path_gen.h"

const char* JOIN_ORDER_TYPE_FORCED  = "Forced";

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

    extern set_rel_pathlist_hook_type set_rel_pathlist_hook;
    static set_rel_pathlist_hook_type prev_rel_pathlist_hook = NULL;

    extern set_join_pathlist_hook_type set_join_pathlist_hook;
    static set_join_pathlist_hook_type prev_join_pathlist_hook = NULL;

    extern set_baserel_size_estimates_hook_type set_baserel_size_estimates_hook;
    static set_baserel_size_estimates_hook_type prev_baserel_size_estimates_hook = NULL;

    extern set_joinrel_size_estimates_hook_type set_joinrel_size_estimates_hook;
    static set_joinrel_size_estimates_hook_type prev_joinrel_size_estimates_hook = NULL;

    extern initial_cost_nestloop_hook_type initial_cost_nestloop_hook;
    static initial_cost_nestloop_hook_type prev_initial_cost_nestloop_hook = NULL;
    extern final_cost_nestloop_hook_type final_cost_nestloop_hook;
    static final_cost_nestloop_hook_type prev_final_cost_nestloop_hook = NULL;

    extern const char **current_planner_type;
    extern const char **current_join_ordering_type;

    extern PGDLLEXPORT void _PG_init(void);
    extern PGDLLEXPORT void _PG_fini(void);

    extern PGDLLEXPORT PlannedStmt *hint_aware_planner(Query*, const char*, int, ParamListInfo);
    extern PGDLLEXPORT void hint_aware_make_one_rel_prep(PlannerInfo*, List*);

    extern PGDLLEXPORT RelOptInfo *hint_aware_join_search(PlannerInfo*, int, List*);

    extern PGDLLEXPORT void hint_aware_set_rel_pathlist(PlannerInfo*, RelOptInfo*,
                                                        Index, RangeTblEntry*);
    extern PGDLLEXPORT void hint_aware_set_join_pathlist(PlannerInfo*, RelOptInfo*,
                                                         RelOptInfo*, RelOptInfo*,
                                                         JoinType, JoinPathExtraData*);

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
char *current_query_string;

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
    join_order_iterator_next(&iterator); /* Iterator starts at base rels, skip these*/

    /* Main "optimization" loop */
    while (!iterator.done)
    {
        foreach (lc, iterator.current_nodes)
        {
            RelOptInfo *left_rel = NULL;
            RelOptInfo *right_rel = NULL;
            RelOptInfo *join_rel = NULL;
            current_node = (JoinOrder*) lfirst(lc);
            Assert(current_node->node_type == JOIN_REL);

            left_rel = fetch_reloptinfo(root, current_node->left_child->relids);
            right_rel = fetch_reloptinfo(root, current_node->right_child->relids);
            Assert(left_rel);
            Assert(right_rel);
            root->join_cur_level = bms_num_members(current_node->relids);

            /* XXX: this way we cannot force join directions. But: do we really need to? */

            join_rel = make_join_rel(root, left_rel, right_rel);
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

    current_query_string = NULL;
    return result;
}

extern "C" void
hint_aware_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    PlannerHints *hints;
    bool hint_found = false;
    OperatorHashEntry *hint_entry;
    ListCell *lc;

    if (prev_rel_pathlist_hook)
        prev_rel_pathlist_hook(root, rel, rti, rte);
    if (!root->join_search_private)
        return;

    hints = (PlannerHints*) root->join_search_private;
    if (!hints->contains_hint)
        return;

    hint_entry = (OperatorHashEntry*) hash_search(hints->operator_hints, &(rel->relids), HASH_FIND, &hint_found);
    if (!hint_found || hint_entry->hint_type == COST_OP)
        return;

    /*
     * Delete any existing paths, only force our desired path.
     *
     * We really need to re-compute the paths, b/c add_path can already throw away seemingly inferior paths.
     */
    if (rel->pathlist != NIL)
    {
        list_free(rel->pathlist);
        rel->pathlist = NIL;
    }
    if (rel->partial_pathlist != NIL)
    {
        list_free(rel->partial_pathlist);
        rel->partial_pathlist = NIL;
    }

    switch (hint_entry->op)
    {
        case SEQSCAN:
            force_seqscan_paths(root, rel);
            break;
        case IDXSCAN:
            force_idxscan_paths(root, rel);
            break;
        default:
            Assert(false);
    }

    /* XXX: do we need a fallback in case no paths? */
}

extern "C" void
enforce_hinted_operator(List *pathlist, OperatorHashEntry *hint)
{
    ListCell *lc;
    foreach (lc, pathlist)
    {
        Path *current_path = (Path*) lfirst(lc);
        Assert(bms_equal(current_path->parent->relids, hint->relids));

        if (IsA(current_path, SeqScan) && hint->op != SEQSCAN)
            foreach_delete_current(pathlist, lc);
        else if (IsA(current_path, IndexPath) && hint->op != IDXSCAN)
            foreach_delete_current(pathlist, lc);
        else if (IsA(current_path, NestPath) && hint->op != NESTLOOP)
            foreach_delete_current(pathlist, lc);
        else if (IsA(current_path, MergePath) && hint->op != MERGEJOIN)
            foreach_delete_current(pathlist, lc);
        else if (IsA(current_path, HashPath) && hint->op != HASHJOIN)
            foreach_delete_current(pathlist, lc);
    }
}

extern "C" void
hint_aware_set_join_pathlist(PlannerInfo *root, RelOptInfo *joinrel,
                             RelOptInfo *outerrel, RelOptInfo *innerrel,
                             JoinType jointype, JoinPathExtraData *extra)
{
    PlannerHints *hints;
    bool hint_found = false;
    OperatorHashEntry *hint_entry;
    ListCell *lc;
    Path *current_path;


    if (prev_join_pathlist_hook)
        prev_join_pathlist_hook(root, joinrel, outerrel, innerrel, jointype, extra);
    if (!root->join_search_private)
        return;

    hints = (PlannerHints*) root->join_search_private;
    if (!hints->contains_hint)
        return;

    hint_entry = (OperatorHashEntry*) hash_search(hints->operator_hints, &(joinrel->relids), HASH_FIND, &hint_found);
    if (!hint_found || hint_entry->hint_type == COST_OP)
        return;

    /*
     * WTFFFFFFFFFFFFF
     *
     * set_join_pathlist is being called twice - once for each join direction (outerrel and innerrel) are swapped
     * During each call, the optimizer generates nestloop, mergejoin (based on presorted and unsorted inputs) and hashjoin
     * paths. While calling add_path, an automatica pruning step is included - if the new path is cheaper (or otherwise better)
     * than the old one, it is removed from the pathlist.
     *
     * When we simply copy the current pathlist into our hint structure, this still removes the paths from the hints.
     * Calling pfree on such Paths leaves them as T_Invalid once we re-read the pathlist in our hint.
     */


    /* Delete any existing paths, only force our desired path */
    if (joinrel->pathlist != NIL)
    {
        list_free(joinrel->pathlist);
        joinrel->pathlist = NIL;
    }
    if (joinrel->partial_pathlist != NIL)
    {
        list_free(joinrel->partial_pathlist);
        joinrel->partial_pathlist = NIL;
    }

    switch (hint_entry->op)
    {
        case NESTLOOP:
            force_nestloop_paths(root, joinrel, outerrel, innerrel, jointype, extra);
            force_nestloop_paths(root, joinrel, innerrel, outerrel, jointype, extra);
            break;
        case MERGEJOIN:
            force_mergejoin_paths(root, joinrel, outerrel, innerrel, jointype, extra);
            force_mergejoin_paths(root, joinrel, innerrel, outerrel, jointype, extra);
            break;
        case HASHJOIN:
            force_hashjoin_paths(root, joinrel, outerrel, innerrel, jointype, extra);
            force_hashjoin_paths(root, joinrel, innerrel, outerrel, jointype, extra);
            break;
        default:
            Assert(false);
    }

    /* XXX: do we need a fallback in case no paths? */
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
    PlannerHints *hints;
    bool hint_found = false;
    CardinalityHashEntry *hint_entry;

    if (prev_baserel_size_estimates_hook)
        return set_baserel_size_fallback(root, rel);
    if (!root->join_search_private)
        return set_baserel_size_fallback(root, rel);

    hints = (PlannerHints*) root->join_search_private;
    if (!hints->contains_hint)
        return set_baserel_size_fallback(root, rel);

    hint_entry = (CardinalityHashEntry*) hash_search(hints->cardinality_hints, &(rel->relids), HASH_FIND, &hint_found);
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
    PlannerHints *hints;
    bool hint_found = false;
    CardinalityHashEntry *hint_entry;

    if (!root->join_search_private)
        return set_joinrel_size_fallback(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);

    hints = (PlannerHints*) root->join_search_private;
    if (!hints->contains_hint)
        return set_joinrel_size_fallback(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);

    hint_entry = (CardinalityHashEntry*) hash_search(hints->cardinality_hints, &(rel->relids), HASH_FIND, &hint_found);
    if (!hint_found)
        return set_joinrel_size_fallback(root, rel, outer_rel, inner_rel, sjinfo, restrictlist);

    return hint_entry->card;
}

extern "C" void
hint_aware_make_one_rel_prep(PlannerInfo *root, List *joinlist)
{
    PlannerHints *hints = (PlannerHints*) palloc0(sizeof(PlannerHints));
    init_hints(root, hints);
    root->join_search_private = (void*) hints;

    if (prev_prepare_make_one_rel_hook)
        prev_prepare_make_one_rel_hook(root, joinlist);
}

extern "C" RelOptInfo *
hint_aware_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
    RelOptInfo *result;
    PlannerHints *hints;

    if (!root->join_search_private)
        goto default_join_search;

    hints = (PlannerHints*) root->join_search_private;
    if (hints->join_order_hint)
    {
        current_join_ordering_type = &JOIN_ORDER_TYPE_FORCED;
        result = force_join_order(root, levels_needed, initial_rels);
    }
    else
    {
        /* Use default join order optimization strategy */
        default_join_search:

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
    }

    if (root->join_search_private)
    {
        free_hints(hints);
        root->join_search_private = NULL;
    }

    return result;
}

extern "C" void
hint_aware_initial_cost_nestloop(PlannerInfo *root,
                                 JoinCostWorkspace *workspace,
								 JoinType jointype,
								 Path *outer_path, Path *inner_path,
								 JoinPathExtraData *extra)
{
    PlannerHints *hints;
    bool hint_found = false;
    OperatorHashEntry *hint_entry;
    Relids join_relids = EMPTY_BITMAP;

    if (prev_initial_cost_nestloop_hook)
        (*prev_initial_cost_nestloop_hook)(root, workspace, jointype, outer_path, inner_path, extra);
    else
        standard_initial_cost_nestloop(root, workspace, jointype, outer_path, inner_path, extra);
    if (!root->join_search_private)
        return;

    hints = (PlannerHints*) root->join_search_private;
    if (!hints->contains_hint)
        return;

    join_relids = bms_add_members(join_relids, outer_path->parent->relids);
    join_relids = bms_add_members(join_relids, inner_path->parent->relids);
    hint_entry = (OperatorHashEntry*) hash_search(hints->operator_hints, &join_relids, HASH_FIND, &hint_found);
    if (!hint_found || hint_entry->hint_type == FORCED_OP)
        return;

    workspace->startup_cost = hint_entry->startup_cost;
    workspace->total_cost = hint_entry->total_cost;
}

extern "C" void
hint_aware_final_cost_nestloop(PlannerInfo *root, NestPath *path,
                               JoinCostWorkspace *workspace,
							   JoinPathExtraData *extra)
{
    PlannerHints *hints;
    bool hint_found = false;
    OperatorHashEntry *hint_entry;
    Path *raw_path;
    Relids relids;

    if (prev_final_cost_nestloop_hook)
        (*prev_final_cost_nestloop_hook)(root, path, workspace, extra);
    else
        standard_final_cost_nestloop(root, path, workspace, extra);

    if (!root->join_search_private)
        return;

    hints = (PlannerHints*) root->join_search_private;
    if (!hints->contains_hint)
        return;

    raw_path = &(path->jpath.path);
    hint_entry = (OperatorHashEntry*) hash_search(hints->operator_hints, &(raw_path->parent->relids), HASH_FIND, &hint_found);
    if (!hint_found || hint_entry->hint_type == FORCED_OP)
        return;

    raw_path->startup_cost = hint_entry->startup_cost;
    raw_path->total_cost = hint_entry->total_cost;
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

    prev_rel_pathlist_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = hint_aware_set_rel_pathlist;

    prev_join_pathlist_hook = set_join_pathlist_hook;
    set_join_pathlist_hook = hint_aware_set_join_pathlist;

    prev_baserel_size_estimates_hook = set_baserel_size_estimates_hook;
    set_baserel_size_estimates_hook = hint_aware_baserel_size_estimates;

    prev_joinrel_size_estimates_hook = set_joinrel_size_estimates_hook;
    set_joinrel_size_estimates_hook = hint_aware_joinrel_size_estimates;

    prev_initial_cost_nestloop_hook = initial_cost_nestloop_hook;
    initial_cost_nestloop_hook = hint_aware_initial_cost_nestloop;
    prev_final_cost_nestloop_hook = final_cost_nestloop_hook;
    final_cost_nestloop_hook = hint_aware_final_cost_nestloop;
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
