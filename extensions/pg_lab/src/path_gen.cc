
#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "executor/executor.h"
#include "nodes/bitmapset.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"

#include "hints.h"
#include "path_gen.h"

/* extracted from joinpath.c */
#define PATH_PARAM_BY_PARENT(path, rel)	\
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path),	\
									   (rel)->top_parent_relids))
#define PATH_PARAM_BY_REL_SELF(path, rel)  \
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path), (rel)->relids))

#define PATH_PARAM_BY_REL(path, rel)	\
	(PATH_PARAM_BY_REL_SELF(path, rel) || PATH_PARAM_BY_PARENT(path, rel))

#define SUPPORTS_PARALLEL(joinrel, outerrel, jointype) \
    (   joinrel->consider_parallel && \
        jointype != JOIN_UNIQUE_OUTER && \
        jointype != JOIN_FULL && \
        jointype != JOIN_RIGHT && \
        jointype != JOIN_RIGHT_ANTI && \
        outerrel->partial_pathlist != NIL && \
        bms_is_empty(joinrel->lateral_relids) \
    )

void force_seqscan_paths(PlannerInfo *root, RelOptInfo *rel)
{
    /* adapted from allpaths.c */

    /* this part is adapted from set_plain_rel_pathlist */
    Relids required_outer;
    required_outer = rel->lateral_relids;

    add_path(rel, (Path*) create_seqscan_path(root, rel, required_outer, 0));
    if (rel->consider_parallel && required_outer == NULL)
    {
        /* this part is adapted from create_plain_partial_paths */
        int parallel_workers;
        parallel_workers = compute_parallel_worker(rel, rel->pages, -1, max_parallel_workers_per_gather);
        if (parallel_workers <= 0)
            return;
        add_partial_path(rel, (Path*) create_seqscan_path(root, rel, NULL, parallel_workers));
    }
}

void force_idxscan_paths(PlannerInfo *root, RelOptInfo *rel)
{
    /* XXX: this might also add bitmap paths. Is this a good thing? */
    create_index_paths(root, rel);
}

void force_nestloop_paths(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel, RelOptInfo *innerrel,
                          JoinType jointype, JoinPathExtraData *extra)
{
    /* Implementation adapted from match_unsorted_outer in joinpath.c */
    JoinType save_jointype = jointype;
    ListCell *outer_lc, *inner_lc;
    Path *best_inner = innerrel->cheapest_total_path;
    Path *materialize_path;

    if (jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI || jointype == JOIN_FULL)
    {
        /* Postgres cannot apply NLJ for these join types */
        elog(ERROR, "cannot apply NLJ for join type: %d",
				 (int) jointype);
    }

    if (jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER)
        jointype = JOIN_INNER;

    if (PATH_PARAM_BY_REL(best_inner, outerrel))
        best_inner = NULL;

    if (save_jointype == JOIN_UNIQUE_INNER)
	{
		if (best_inner == NULL)
			return;
		best_inner = (Path *) create_unique_path(root, innerrel, best_inner, extra->sjinfo);
		Assert(best_inner);
	}
    else if (enable_material && best_inner != NULL && !ExecMaterializesOutput(best_inner->pathtype))
    {
        /* Try materialized version of the inner path */
        materialize_path = (Path*) create_material_path(innerrel, best_inner);
    }

    foreach (outer_lc, outerrel->pathlist)
    {
        Path *outerpath = (Path*) lfirst(outer_lc);
        List *merge_pathkeys;

        if (PATH_PARAM_BY_REL(outerpath, innerrel))
            continue;

        if (save_jointype == JOIN_UNIQUE_OUTER)
		{
			if (outerpath != outerrel->cheapest_total_path)
				continue;
			outerpath = (Path *) create_unique_path(root, outerrel,
													outerpath, extra->sjinfo);
			Assert(outerpath);
		}

        merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, outerpath->pathkeys);

        if (save_jointype == JOIN_UNIQUE_INNER)
		{
			try_nestloop_path(root,
							  joinrel,
							  outerpath,
							  best_inner,
							  merge_pathkeys,
							  jointype,
							  extra);
            continue;
		}

        foreach (inner_lc, innerrel->cheapest_parameterized_paths)
        {
            Path *innerpath = (Path*) lfirst(inner_lc);
            Path *memoize_path;

            try_nestloop_path(root, joinrel, outerpath, innerpath, merge_pathkeys, jointype, extra);

            memoize_path = get_memoize_path(root, innerrel, outerrel, innerpath, outerpath, jointype, extra);
            if (!memoize_path)
                continue;
            try_nestloop_path(root, joinrel, outerpath, memoize_path, merge_pathkeys, jointype, extra);
        }

        if (materialize_path)
            try_nestloop_path(root, joinrel, outerpath, materialize_path, merge_pathkeys, jointype, extra);
    }

    if (SUPPORTS_PARALLEL(joinrel, outerrel, save_jointype))
        consider_parallel_nestloop(root, joinrel, outerrel, innerrel, save_jointype, extra);
}

void force_mergejoin_paths(PlannerInfo *root, RelOptInfo *joinrel,
                           RelOptInfo *outerrel, RelOptInfo *innerrel,
                           JoinType jointype, JoinPathExtraData *extra)
{
    /* Implementation adapted from match_unsorted_outer() in joinpath.c */
    JoinType save_jointype = jointype;
    ListCell *outer_lc, *inner_lc;
    Path *best_inner = innerrel->cheapest_total_path;
    bool useallclauses = false;

    /* First up, generate mergejoin paths based on unsorted inner and outer */
    sort_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, extra);

    /* Now, try to generate mergejoin paths with pre-sorted inputs. This is based on match_unsorted_outer */

    if (jointype == JOIN_FULL || jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI)
        useallclauses = true;
    if (jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER)
        jointype = JOIN_INNER;


    if (PATH_PARAM_BY_REL(best_inner, outerrel))
        best_inner = NULL;

    if (save_jointype == JOIN_UNIQUE_INNER)
	{
		/* No way to do this with an inner path parameterized by outer rel */
		if (best_inner == NULL)
			return;
		best_inner = (Path *) create_unique_path(root, innerrel, best_inner, extra->sjinfo);
		Assert(best_inner);
	}

    if (save_jointype != JOIN_UNIQUE_INNER && best_inner)
    {
        foreach (outer_lc, outerrel->pathlist)
        {
            Path *outerpath = (Path*) lfirst(outer_lc);
            List *merge_pathkeys;

            if (PATH_PARAM_BY_REL(outerpath, innerrel))
                continue;

            if (save_jointype == JOIN_UNIQUE_OUTER)
            {
                if (outerpath != outerrel->cheapest_total_path)
                    continue;
                outerpath = (Path *) create_unique_path(root, outerrel,
                                                        outerpath, extra->sjinfo);
                Assert(outerpath);
            }

            merge_pathkeys = build_join_pathkeys(root, joinrel, jointype, outerpath->pathkeys);

            generate_mergejoin_paths(root, joinrel,
                                     innerrel, outerpath,
                                     save_jointype, extra,
                                     useallclauses, best_inner,
                                     merge_pathkeys, false);
        }
    }


    if (SUPPORTS_PARALLEL(joinrel, outerrel, save_jointype))
    {
        if (best_inner == NULL || !best_inner->parallel_safe)
        {
            if (save_jointype == JOIN_UNIQUE_INNER)
                return;
            best_inner = get_cheapest_parallel_safe_total_inner(innerrel->pathlist);
        }

        if (best_inner)
            consider_parallel_mergejoin(root, joinrel, outerrel, innerrel, jointype, extra, best_inner);
    }
}


void force_hashjoin_paths(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel, RelOptInfo *innerrel,
                          JoinType jointype, JoinPathExtraData *extra)
{
    hash_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, extra);
}


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
    joinorder_it_init(&iterator, join_order);
    joinorder_it_next(&iterator); /* Iterator starts at base rels, skip these */

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

        joinorder_it_next(&iterator);
    }

    /* Post-process generated relations, same as standard_join_search() does */
    final_rel = (RelOptInfo *) linitial(root->join_rel_level[levels_needed]);
    root->join_rel_level = NULL;

    joinorder_it_free(&iterator);
    return final_rel;
}

#ifdef __cplusplus
}
#endif
