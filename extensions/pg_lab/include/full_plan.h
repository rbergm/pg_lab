#ifndef FULL_PLAN_H
#define FULL_PLAN_H

#include "postgres.h"
#include "nodes/pathnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"

#include "hints.h"


typedef struct PlanNode {
    PhysicalOperator op;
    Relids relids;
    int parallel_workers;

    Index base_relation;

    PlanNode *outer_child;
    PlanNode *inner_child;

    RelOptInfo *rel;
} PlanNode;


RelOptInfo*
full_plan_join_search()
{
    return NULL;
}

Path*
make_path(PlannerInfo *root, PlanNode *plan_node)
{
    switch (plan_node->op)
    {
        case OP_SEQSCAN:
            return make_seqscan(root, plan_node);
        default:
            /* XXX: error handling */
    }

    /* XXX: error handling*/
}

Path*
make_seqscan(PlannerInfo *root, PlanNode *plan_node)
{
    RelOptInfo *rel;
    Relids required_outers;

    rel = root->simple_rel_array[plan_node->base_relation];
    required_outers = rel->lateral_relids;

    return create_seqscan_path(root, rel, required_outers, plan_node->parallel_workers);
}

Path*
make_idxscan(PlannerInfo *root, PlanNode *plan_node)
{

}

Path*
make_bitmaptscan(PlannerInfo *root, PlanNode *plan_node)
{

}

Path*
make_materialize(PlannerInfo *root, PlanNode *plan_node)
{

}

Path*
make_memoize(PlannerInfo *root, PlanNode *plan_node)
{

}

Path*
make_sort(PlannerInfo *root, PlanNode *plan_node)
{

}

Path*
make_nestloop(PlannerInfo *root, PlanNode *plan_node)
{
    RelOptInfo *joinrel;
    Path *outer_path;
    Path *inner_path;
    NestPath *nest_path;
    List *merge_pathkeys;
    JoinCostWorkspace workspace;

    joinrel = make_joinrel(root, plan_node);
    outer_path = make_path(root, plan_node->outer_child);
    inner_path = make_path(root, plan_node->inner_child);

    nest_path = create_nestloop_path(root, joinrel, JOIN_INNER, NULL, NULL, outer_path, inner_path, NULL, NULL, NULL);

    return (Path*) nest_path;
}

Path*
make_mergejoin(PlannerInfo *root, PlanNode *plan_node)
{

}

Path*
make_hashjoin(PlannerInfo *root, PlanNode *plan_node)
{

}

RelOptInfo*
fetch_reloptinfo(PlannerInfo *root, Relids relids)
{
    List *candidate_relopts;
    int nrels;
    ListCell *lc;

    nrels = bms_num_members(relids);
    candidate_relopts = root->join_rel_level[nrels];

    foreach(lc, candidate_relopts)
    {
        RelOptInfo *rel = (RelOptInfo*) lfirst(lc);
        if (bms_equal(rel->relids, relids))
            return rel;
    }

    return NULL;
}

RelOptInfo*
make_joinrel(PlannerInfo *root, PlanNode *plan_node)
{
    RelOptInfo *outer_rel;
    RelOptInfo *inner_rel;
    RelOptInfo *joinrel;
    SpecialJoinInfo *sjinfo;
    SpecialJoinInfo sjinfo_data;
    Relids joinrelids;
    List *pushed_down_joins = NIL;
    List *restrictlist;
    int nrels;
    bool reversed;

    joinrelids = bms_copy(plan_node->relids);
    nrels = bms_num_members(plan_node->relids);
    outer_rel = fetch_reloptinfo(root, plan_node->outer_child->relids);
    inner_rel = fetch_reloptinfo(root, plan_node->inner_child->relids);

    /* We call join_is_legal just for its side effects */
    Assert(join_is_legal(root, outer_rel, inner_rel, joinrelids, &sjinfo, &reversed));

    /* Sanity check: the join should never ever be reversed, otherwise the input plan was already broken */
    Assert(!reversed);

    joinrelids = add_outer_joins_to_relids(root, joinrelids, sjinfo, &pushed_down_joins);

    if (sjinfo == NULL)
    {
        sjinfo = &sjinfo_data;
        init_dummy_sjinfo(sjinfo, outer_rel->relids, inner_rel->relids);
    }

    joinrel = build_join_rel(root, joinrelids, outer_rel, inner_rel, sjinfo, pushed_down_joins, &restrictlist);

    root->join_rel_level[nrels] = lappend(root->join_rel_level[nrels], joinrel);
    plan_node->rel = joinrel;
}

#endif // FULL_PLAN_H
