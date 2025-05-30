
extern "C" {
#include <limits.h>
#include <math.h>

#include "postgres.h"
#include "miscadmin.h"

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
}

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

static bool enable_pglab = true;

/* Stores the raw query that is currently being optimized in this backend. */
char *current_query_string = NULL;

/*
 * We explicitly store the PlannerInfo as a static variable because some low-level routines in the planner do not receive it
 * as an argument. But, some of our hint-aware variants of these routines need it.
 */
static PlannerInfo  *current_planner_root = NULL;

/* The hints that are available for the current query. */
static PlannerHints *current_hints = NULL;


/*
 * Our custom planner hook is required because this is the last point during the Postgres planning phase where the raw query
 * string is available. Everywhere down the line, only the parsed Query* node is available. However, the Query* does not
 * contain any comments and hence, no hints.
 *
 * Sadly, in the planner entry point the PlannerInfo is not yet available, so we can only export the raw query string and
 * need to delegate the actual parsing of the hints to the pg_lab-specific make_one_rel_prep() hook.
 */
extern "C" PlannedStmt *
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

/*
 * The make_one_rel_prep() hook is called before the actual planning starts. We use it to extract the hints from our raw
 * query string.
 */
extern "C" void
hint_aware_make_one_rel_prep(PlannerInfo *root, List *joinlist)
{
    PlannerHints *hints;

    if (!enable_pglab)
    {
        if (prev_prepare_make_one_rel_hook)
            prev_prepare_make_one_rel_hook(root, joinlist);
        return;
    }

    hints = init_hints(current_query_string);
    parse_hint_block(root, hints);
    post_process_hint_block(hints);

    if (prev_prepare_make_one_rel_hook)
        prev_prepare_make_one_rel_hook(root, joinlist);

    current_planner_root = root;
    current_hints = hints;
}

/*
 * We need a custom hook for the add_path_precheck(), because the original function is used to skip access paths if they are
 * definitely not useful (at least according to the native cost model). We need to overwrite this behavior to enforce our
 * current hints.
 */
extern "C" bool
hint_aware_add_path_precheck(RelOptInfo *parent_rel, Cost startup_cost, Cost total_cost,
                             List *pathkeys, Relids required_outer)
{
    if (current_hints)
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
extern "C" bool
hint_aware_add_partial_path_precheck(RelOptInfo *parent_rel, Cost total_cost, List *pathkeys)
{
    if (current_hints)
        /* XXX: we could be smarter and try to check, whether there actually is a hint concerning the current path */
        return true;

    if (prev_add_partial_path_precheck_hook)
        return (*prev_add_partial_path_precheck_hook)(parent_rel, total_cost, pathkeys);
    else
        return standard_add_partial_path_precheck(parent_rel, total_cost, pathkeys);
}


/* utility macros inspired by nodeTag and IsA from nodes.h */
#define pathTag(pathptr) (((const Path*)pathptr)->pathtype)
#define PathIsA(nodeptr,_type_) (pathTag(nodeptr) == T_ ## _type_)
#define IsAScanPath(pathptr) (PathIsA(pathptr, SeqScan) || \
                              PathIsA(pathptr, IndexScan) || \
                              PathIsA(pathptr, IndexOnlyScan) || \
                              PathIsA(pathptr, BitmapHeapScan))
#define IsAJoinPath(pathptr) (PathIsA(pathptr, NestLoop) || PathIsA(pathptr, MergeJoin) || PathIsA(pathptr, HashJoin))
#define IsAIntermediatePath(pathptr) (PathIsA(pathptr, Memoize) || PathIsA(pathptr, Material))
#define IsAParPath(pathptr) (PathIsA(pathptr, Gather) || PathIsA(pathptr, GatherMerge))


/*
 * Checks, whether the given path has the operator that is required by the current hints.
 *
 * We pass the join_order, because it can serve as a shortcut to the OperatorHint. This is cheaper than performing a hash
 * lookup in the hint table.
 *
 * Notice that we don't recurse into subpaths for joins or intermediate ops (memoize/materialize) here, we just make sure that
 * the top-level operator is correct. However, we do recurse into upper-rel related paths (gather, limit, etc).
 */
static bool
path_satisfies_operator(Path *path, JoinOrder *join_order)
{
    Relids        relids;
    OperatorHint *op_hint;
    bool          hint_found;

    if (!IsAJoinPath(path) && !IsAScanPath(path) && !IsAIntermediatePath(path))
    {
        /*
         * Could be a gather/gather merge path or a path belonging to the upper rel.
         * We don't force these, so we leave the path be.
         */
        return true;
    }

    hint_found = false;
    if (join_order)
    {
        op_hint = join_order->physical_op;
        hint_found = op_hint != NULL;
    }

    if (!hint_found && current_hints->operator_hints)
    {
        /* We use the slightly bogus control flow to fall back to the hint table in case the join order did not do its job. */
        relids = path->parent->relids;
        op_hint = (OperatorHint*) hash_search(current_hints->operator_hints,
                                              &relids, HASH_FIND, &hint_found);
    }

    if (!hint_found)
        /* Nothing restricts the choice of operators, we are good to continue */
        return true;

    /*
     * Materialization and memoization is handled by flags attached to the main operator hint. Therefore, we need to handle
     * them separately.
     * Afterwards, we simply switch over the hinted operator and make sure that our path is of the correct type.
     */

    if (PathIsA(path, Memoize))
        return op_hint->memoize_output;
    else if (PathIsA(path, Material))
        return op_hint->materialize_output;

    switch (op_hint->op)
    {
        case OP_UNKNOWN:
            /* We can't check for OP_UNKNOWN earlier, to make sure that the above special cases are handled appropriately */
            return true;
        case OP_SEQSCAN:
            return PathIsA(path, SeqScan);
        case OP_IDXSCAN:
            return PathIsA(path, IndexScan) || PathIsA(path, IndexOnlyScan);
        case OP_BITMAPSCAN:
            return PathIsA(path, BitmapHeapScan);
        case OP_NESTLOOP:
            return PathIsA(path, NestLoop);
        case OP_HASHJOIN:
            return PathIsA(path, HashJoin);
        case OP_MERGEJOIN:
            return PathIsA(path, MergeJoin);
        default:
            /* We don't check for memoize/materialize here, because these are never root nodes of a path */
            return false;
    }
}


/*
 * Checks, whether the given path satisfies all our hints with regards to the assigned operators and the join order.
 *
 * The join order has to be rooted at our current path. If no join order is given, NULL can be passed. Otherwise, a NULL value
 * implies that the path does not satisfy the join order and hence fails the check.
 *
 * If we detect a parallel subpath, we store it in the par_subpath, because the caller might find this information useful to
 * decide whether the parallelization hints are satisfied. This portion of the hints check is explicitly not handled in this
 * function, because it requires more context (e.g. to distinguish between partial and normal paths).
 * If the caller does not care about the parallel subpath, they can pass a NULL pointer.
 */
static bool
path_satisfies_hints(Path *path, JoinOrder *join_node, Path **par_subpath)
{
    JoinPath *jpath;
    bool inner_valid, outer_valid;

    if (current_hints->join_order_hint && !join_node)
        /* We have a join order, but the path is not on it. This is easy, we can reject the path. */
        return false;

    /*
     * Now the easy checks are over and we need to do some actual work.
     * This involves recursion, so we should be as paranoid as the normal planner implementation.
     */
    check_stack_depth();

    /*
     * First up, we handle all intermediate operators
     */
    if (!IsAJoinPath(path) && !IsAScanPath(path))
    {
        switch (path->pathtype)
        {
            case T_Gather:
            {
                GatherPath *gpath = (GatherPath*) path;
                if (par_subpath != NULL)
                    *par_subpath = gpath->subpath;
                return path_satisfies_hints(gpath->subpath, join_node, par_subpath);
            }

            case T_GatherMerge:
            {
                GatherMergePath *gmpath = (GatherMergePath*) path;
                if (par_subpath != NULL)
                    *par_subpath = gmpath->subpath;
                return path_satisfies_hints(gmpath->subpath, join_node, par_subpath);
            }

            case T_Memoize:
            {
                MemoizePath *mempath = (MemoizePath*) path;
                return path_satisfies_operator(path, join_node) &&
                       path_satisfies_hints(mempath->subpath, join_node, par_subpath);
            }

            case T_Material:
            {
                MaterialPath *matpath = (MaterialPath*) path;
                return path_satisfies_operator(path, join_node) &&
                       path_satisfies_hints(matpath->subpath, join_node, par_subpath);
            }

            case T_Sort:
            {
                SortPath *spath = (SortPath*) path;
                return path_satisfies_hints(spath->subpath, join_node, par_subpath);
            }

            case T_IncrementalSort:
            {
                IncrementalSortPath *ispath = (IncrementalSortPath*) path;
                return path_satisfies_hints((ispath->spath).subpath, join_node, par_subpath);
            }

            case T_Group:
            {
                GroupPath *gpath = (GroupPath*) path;
                return path_satisfies_hints(gpath->subpath, join_node, par_subpath);
            }

            case T_Agg:
            {
                AggPath *apath = (AggPath*) path;
                return path_satisfies_hints(apath->subpath, join_node, par_subpath);
            }

            case T_Limit:
            {
                LimitPath *lpath = (LimitPath*) path;
                return path_satisfies_hints(lpath->subpath, join_node, par_subpath);
            }

            default:
                ereport(ERROR, errmsg("[pg_lab] Cannot hint query. Path type is not supported"));
                break;
        }
    }

    /*
     * At this point we know that our path is either a join path or a scan path.
     * To check its validity, we first check the associated operator (because this check is usually cheaper).
     * Then, we check if the path is on the hinted join order.
     * Finally, for join paths we also need to recurse into the child paths.
     */

    if  (!path_satisfies_operator(path, join_node))
        return false;

    if (!join_node)
    {
        /*
         * When we don't have a join order to enforce, we just need to recurse into the child nodes to make sure they use the
         * correct operators.
         */

        if (IsAJoinPath(path))
        {
            jpath = (JoinPath *) path;

            /*
             * We cannot use the shortcut logic here, because we still need to check for parallelism in both paths!
             *
             * Even though it might reasonable to assume that a parallel path is of no use if it violates the hinted operators
             * (and right now this is indeed the case), using the shortcut logic here might lead to very subtle bugs in the
             * future. For example, we might be interested in the parallel subpath regardless of the operator hint
             * compatibility at some point and for whatever reason.
             */

            inner_valid = path_satisfies_hints(jpath->innerjoinpath, NULL, par_subpath);
            outer_valid = path_satisfies_hints(jpath->outerjoinpath, NULL, par_subpath);
            return inner_valid && outer_valid;
        }
        else if (IsAScanPath(path))
            return true;
    }

    if (!bms_equal(path->parent->relids, join_node->relids))
        /* Make sure that we satisfy the join order */
        return false;

    if (IsAScanPath(path))
        /*
         * Scan paths can't recurse further, except for bitmap scans.
         * And we currently don't care about their specific structure.
         */
        return true;

    /*
     * At this point we know for certain that this must be a join path and we know that the intermediate is indeed on our
     * hinted join order. Now, we just need to make sure that this is also the case for our child paths.
     */
    Assert(IsAJoinPath(path));

    jpath = (JoinPath*) path;

    /* Once again we should not use the shortcut syntax here, see comment above. */
    inner_valid = path_satisfies_hints(jpath->innerjoinpath, join_node->inner_child, par_subpath);
    outer_valid = path_satisfies_hints(jpath->outerjoinpath, join_node->outer_child, par_subpath);
    return inner_valid && outer_valid;
}


/*
 * For parallel Result() hints, checks whether the given path satisfies is compatible with the parallelization hints.
 *
 * In order for this condition to be satisfied, the path must be part of the upper rel if the current query requires an upper
 * rel and it must perform upper rel-specific operations (e.g. sorting, grouping, etc).
 *
 * If the current query does not require an upper rel, the path can do whatever operations it wants.
 *
 * par_subpath is the part of the plan that immediately follows the gather node, i.e. the subpath that is executed in parallel.
 * It might not be NULL.
 */
static bool
upper_rel_satisfies_parallelization(RelOptInfo *parent_rel, Path *par_subpath)
{
    Query *query;
    bool   passed;
    bool   requires_post_join;

    query = current_planner_root->parse;
    requires_post_join = query->hasAggs ||
                         query->groupClause ||
                         query->groupingSets ||
                         current_planner_root->hasHavingQual ||
                         query->hasWindowFuncs ||
                         query->sortClause ||
                         query->distinctClause ||
                         query->setOperations;

    if (requires_post_join)
        passed = !IsAJoinPath(par_subpath) &&
                 !IsAScanPath(par_subpath) &&
                  bms_equal(current_hints->parallel_rels, parent_rel->relids);
    else
        passed = bms_equal(current_hints->parallel_rels, parent_rel->relids);

    return passed;
}

/*
 * The magic sauce that makes pg_lab's hinting work.
 *
 * Conceptually, this hook's job is quite straightforward: whenever Postgres generates a new path, we check whether it
 * satisfies the given hints. If it does, we let it through to the standard add_path() function. Otherwise, we exit.
 *
 * However, there are many subtleties that we need to take care of, especially with regards to parallel plans. These issues
 * along with our solutions are described directly in the code below.
 */
extern "C" void
hint_aware_add_path(RelOptInfo *parent_rel, Path *new_path)
{
    Path        *par_subpath;
    JoinOrder   *join_order;
    bool         contains_par_subplan;
    bool         keep_new = true;

    if (!current_hints || !current_hints->contains_hint)
    {
        if (prev_add_path_hook)
            (*prev_add_path_hook)(parent_rel, new_path);
        else
            standard_add_path(parent_rel, new_path);
        return;
    }

    /* We first check for structural compatibility with our hints. */

    join_order = current_hints->join_order_hint != NULL
                 ? traverse_join_order(current_hints->join_order_hint, parent_rel->relids)
                 : NULL;

    par_subpath = NULL;
    if (!path_satisfies_hints(new_path, join_order, &par_subpath))
        return;

    /*
     * At this point we know that the new path at least satisfies the hints regarding join, scan and intermediate operators.
     * It is also on the hinted join order.
     *
     * Now, we just need to make sure that the path is also compatible with our parallelization hints. However, this turns out
     * to be much more complicated than it seems at first glance.
     *
     * This is mostly due to the fact that there are a lot of different cases that we need to handle. add_path() is called for
     * both strictly sequential paths and parallel paths that have been gathered. Furthermore, it handles paths during the
     * join phase as well as for the upper rel. But the logical flow between these two phases is reversed: whereas gather paths
     * are only consider after their sequential counterparts during the join phase, the upper rel first considers gathering
     * the partial paths and afterwards sequential ones.
     *
     * This is bad, because at the end of each of the sub-phases, set_cheapest() is called. But set_cheapest() fails the entire
     * query if there are no paths. We encounter this case frequently when using parallel hints.
     * The solution is the spaghetti-ish control flow logic below.
     * We try our best to explain each of the branches, but it is still quite convoluted.
     *
     * Query optimization is just complicated, man.
     */

    contains_par_subplan = par_subpath != NULL;

    if (current_hints->parallel_mode == PARMODE_SEQUENTIAL && contains_par_subplan)
    {
        /*
         * We are hinted to only produce sequential paths, but we are currently dealing with a parallel path.
         * It is safe to reject it.
         *
         * Notice that this branch should never be reached, because we should have already rejected the partial path.
         * Therefore we issue a warning to investigate this further.
         *
         * TODO: test sequential mode with upper rel queries. This should not work right now?
         */
        ereport(WARNING,
                errmsg("[pg_lab] Reached presumably dead code path in add_path()"),
                errdetail("Parallel mode is set to sequential, but parallel subplan was detected."));
        return;
    }

    if (current_hints->parallel_mode == PARMODE_SEQUENTIAL || current_hints->parallel_mode == PARMODE_DEFAULT)
    {
        if (prev_add_path_hook)
            (*prev_add_path_hook)(parent_rel, new_path);
        else
            standard_add_path(parent_rel, new_path);
        return;
    }

    Assert(current_hints->parallel_mode == PARMODE_PARALLEL);

    if (IS_UPPER_REL(parent_rel) && parent_rel->pathlist == NIL)
    {
        /*
         * TODO
         */

        if (prev_add_path_hook)
            (*prev_add_path_hook)(parent_rel, new_path);
        else
            standard_add_path(parent_rel, new_path);

        return;
    }

    if (par_subpath)
    {
        /*
         * We are tasked with only producing parallel paths and we found a parallelized path.
         * Now, we just need to make sure that this is path is parallelized correctly.
         */

        if (current_hints->parallelize_entire_plan)
        {
            keep_new = upper_rel_satisfies_parallelization(parent_rel, par_subpath);
        }
        else if (current_hints->parallel_rels == NULL)
            /*
             * We should only produce parallel plans, but it does not matter where we start with the parallelization.
             * Since we already know that we are currently working with a partial path, we can just accept it.
             */
            keep_new = true;
        else
        {
            Assert(current_hints->parallel_rels != NULL);
            keep_new = bms_equal(current_hints->parallel_rels, par_subpath->parent->relids) &&
                       (IsAJoinPath(par_subpath) || IsAScanPath(par_subpath));
        }

        if (keep_new)
        {
            ListCell *lc;

            foreach (lc, parent_rel->pathlist)
            {
                Path *other_path = (Path *) lfirst(lc);
                Path *other_gather;
                bool  prune_other;

                prune_other = path_satisfies_hints(other_path, join_order, &other_gather) ||
                              other_gather == NULL ||
                              (current_hints->parallelize_entire_plan &&
                                !upper_rel_satisfies_parallelization(parent_rel, other_gather));

                if (prune_other)
                    parent_rel->pathlist = foreach_delete_current(parent_rel->pathlist, lc);
            }
        }
    }
    else if (list_length(parent_rel->pathlist) == 0)
    {
        /*
         * TODO
         */

        keep_new = true;
    }
    else
    {
        /*
         * TODO
         */

        keep_new = false;
    }

    if (keep_new)
    {
        if (prev_add_path_hook)
            (*prev_add_path_hook)(parent_rel, new_path);
        else
            standard_add_path(parent_rel, new_path);
    }
}

extern "C" void
hint_aware_add_partial_path(RelOptInfo *parent_rel, Path *new_path)
{
    JoinOrder *join_order;
    bool keep_new;

    if (!current_hints || !current_hints->contains_hint)
    {
        if (prev_add_path_hook)
            (*prev_add_path_hook)(parent_rel, new_path);
        else
            standard_add_partial_path(parent_rel, new_path);
        return;
    }

    if (current_hints->parallel_mode == PARMODE_SEQUENTIAL)
        /*
         * We are hinted to only produce sequential path, but are currently considering a parallel path.
         * Reject it.
         */
        return;

    join_order = current_hints->join_order_hint != NULL
                 ? traverse_join_order(current_hints->join_order_hint, parent_rel->relids)
                 : NULL;

    if (!path_satisfies_hints(new_path, join_order, NULL))
        return;

    /*
     * Whether we may or may not accept the new path depends on a lot of rather spaghetti-ish decision logic.
     * This is not particularly pretty, but it gets the job done (at least for now).
     *
     * The basic idea is as follows:
     * TODO
     */

    if (current_hints->parallel_mode == PARMODE_DEFAULT)
    {
        /*
         * In default mode we can do whatever, as long as join order and physical operators are satisfied
         * (which was already checked above).
         */
        if (prev_add_path_hook)
            (*prev_add_path_hook)(parent_rel, new_path);
        else
            standard_add_partial_path(parent_rel, new_path);
        return;
    }

    Assert(current_hints->parallel_mode == PARMODE_PARALLEL);

    if (current_hints->parallel_rels == NULL || current_hints->parallelize_entire_plan)
        /*
         * TODO
         */
        keep_new = true;
    else
        /*
         * TODO
         */
        keep_new = bms_is_subset(parent_rel->relids, current_hints->parallel_rels);

    if (keep_new)
    {
        if (prev_add_path_hook)
            (*prev_add_path_hook)(parent_rel, new_path);
        else
            standard_add_partial_path(parent_rel, new_path);
    }
}

extern "C" int
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

extern "C" double
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

extern "C" double
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

extern "C" void
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

extern "C" void
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

extern "C" void
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

extern "C" void
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

extern "C" void
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

extern "C" void
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

extern "C" void
_PG_init(void)
{
    DefineCustomBoolVariable("enable_pglab",
                             "Enable plan hinting via pg_lab", NULL,
                             &enable_pglab, true,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

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
}

extern "C" void
_PG_fini(void)
{
    /* XXX */
    planner_hook = prev_planner_hook;
    prepare_make_one_rel_hook = prev_prepare_make_one_rel_hook;
    join_search_hook = prev_join_search_hook;
    set_rel_pathlist_hook = prev_rel_pathlist_hook;
    set_join_pathlist_hook = prev_join_pathlist_hook;
    set_baserel_size_estimates_hook = prev_baserel_size_estimates_hook;
    set_joinrel_size_estimates_hook = prev_joinrel_size_estimates_hook;
    compute_parallel_worker_hook = prev_compute_parallel_workers_hook;
}
