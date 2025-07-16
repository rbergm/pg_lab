#ifdef __cplusplus
extern "C" {
#endif

#include <limits.h>
#include <math.h>

#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"

#include "hints.h"

const char *
PhysicalOperatorToString(PhysicalOperator op)
{
    switch (op)
    {
        case OP_SEQSCAN:      return "SeqScan";
        case OP_IDXSCAN:      return "IdxScan";
        case OP_BITMAPSCAN:   return "BitmapScan";
        case OP_NESTLOOP:     return "NestLoop";
        case OP_HASHJOIN:     return "HashJoin";
        case OP_MERGEJOIN:    return "MergeJoin";
        case OP_MEMOIZE:      return "Memoize";
        case OP_MATERIALIZE:  return "Materialize";
        default:              return "Unknown";
    }
}

void free_join_order(JoinOrder *join_order)
{
    if (join_order->node_type == JOIN_REL)
    {
        free_join_order(join_order->outer_child);
        free_join_order(join_order->inner_child);
    }

    bms_free(join_order->relids);
    if (join_order->base_identifier)
        pfree(join_order->base_identifier);

    pfree(join_order);
}

JoinOrder *
traverse_join_order(JoinOrder *join_order, Relids node)
{
    JoinOrder *result;

    check_stack_depth();

    if (bms_equal(join_order->relids, node))
        result = join_order;
    else  if (join_order->node_type == BASE_REL)
        result = NULL;
    else
    {
        Assert(join_order->node_type == JOIN_REL);
        if (bms_is_subset(node, join_order->outer_child->relids))
            result = traverse_join_order(join_order->outer_child, node);
        else if (bms_is_subset(node, join_order->inner_child->relids))
            result = traverse_join_order(join_order->inner_child, node);
        else
            result = NULL;
    }

    return result;
}

JoinOrder_Comparison
join_order_compare(JoinOrder *prefix, Path *path, Relids all_rels)
{
    Relids path_relids;
    BMS_Comparison relid_cmp;
    Relids common_rels;

    check_stack_depth();

    path_relids = path->parent->relids;
    path_relids = path_relids == NULL ? all_rels : path_relids;  /* upper rels have no relids*/

    relid_cmp = bms_subset_compare(prefix->relids, path_relids);
    common_rels = bms_intersect(prefix->relids, path_relids);
    if (relid_cmp == BMS_DIFFERENT && common_rels)
        return JO_DIFFERENT;
    if (relid_cmp == BMS_DIFFERENT)
        return JO_DISJOINT;

    if (relid_cmp == BMS_SUBSET2)
    {
        /*
         * Join order is a superset of the path's relids. The path is apparently still being built, but we need to make sure
         * that it can eventually become part of the join prefix.
         */

        JoinOrder *sub_prefix;
        sub_prefix = traverse_join_order(prefix, path_relids);
        return sub_prefix == NULL ? JO_DIFFERENT : join_order_compare(sub_prefix, path, all_rels);
    }

    if (prefix->node_type == BASE_REL)
        return relid_cmp == BMS_EQUAL ? JO_EQUAL : JO_DIFFERENT;


    /*
     * At this point we might either have a larger path that does (or does not) contain our prefix, or our prefix is exactly
     * as large as the path. It doesn't really matter which is the case, we still need to traverse the path to check all of its
     * children.
     */

    if (IsAJoinPath(path) && relid_cmp == BMS_SUBSET1)
    {
        /* join prefix is contained in the join path (or not), we need to "fast forward" */
        JoinPath *jpath = (JoinPath *) path;

        if (bms_is_subset(prefix->relids, jpath->outerjoinpath->parent->relids))
            return join_order_compare(prefix, jpath->outerjoinpath, all_rels);
        else if (bms_is_subset(prefix->relids, jpath->innerjoinpath->parent->relids))
            return join_order_compare(prefix, jpath->innerjoinpath, all_rels);
        else
            /* The tables of our prefix are scattered across different branches in the path. This violates the prefix. */
            return JO_DIFFERENT;
    }
    else if (IsAJoinPath(path))
    {
        JoinPath *jpath = (JoinPath *) path;
        JoinOrder_Comparison outer_cmp, inner_cmp;
        Assert(relid_cmp == BMS_EQUAL);

        outer_cmp = join_order_compare(prefix, jpath->outerjoinpath, all_rels);
        if (outer_cmp != JO_EQUAL)
            return JO_DIFFERENT;

        inner_cmp = join_order_compare(prefix, jpath->innerjoinpath, all_rels);
        return inner_cmp == JO_EQUAL ? JO_EQUAL : JO_DIFFERENT;
    }

    /* We have some sort of intermediate path node, just skip through it and continue with the normal prefix validation */
    switch (path->pathtype)
    {
        case T_Gather:
        {
            GatherPath *gpath = (GatherPath*) path;
            return join_order_compare(prefix, gpath->subpath, all_rels);
        }

        case T_GatherMerge:
        {
            GatherMergePath *gpath = (GatherMergePath*) path;
            return join_order_compare(prefix, gpath->subpath, all_rels);
        }

        case T_Memoize:
        {
            MemoizePath *mempath = (MemoizePath*) path;
            return join_order_compare(prefix, mempath->subpath, all_rels);
        }

        case T_Material:
        {
            MaterialPath *matpath = (MaterialPath*) path;
            return join_order_compare(prefix, matpath->subpath, all_rels);
        }

        case T_Sort:
        {
            SortPath *spath = (SortPath*) path;
            return join_order_compare(prefix, spath->subpath, all_rels);
        }

        case T_IncrementalSort:
        {
            IncrementalSortPath *spath = (IncrementalSortPath*) path;
            return join_order_compare(prefix, spath->spath.subpath, all_rels);
        }

        case T_Group:
        {
            GroupPath *gpath = (GroupPath*) path;
            return join_order_compare(prefix, gpath->subpath, all_rels);
        }

        case T_Agg:
        {
            AggPath *apath = (AggPath*) path;
            return join_order_compare(prefix, apath->subpath, all_rels);
        }

        case T_Limit:
        {
            LimitPath *lpath = (LimitPath*) path;
            return join_order_compare(prefix, lpath->subpath, all_rels);
        }

        case T_Result:
        {
            if (IsA(path, ProjectionPath))
            {
                ProjectionPath *projpath = (ProjectionPath*) path;
                return join_order_compare(prefix, projpath->subpath, all_rels);
            }
            else if (IsA(path, ProjectSetPath))
            {
                ProjectSetPath *projpath = (ProjectSetPath*) path;
                return join_order_compare(prefix, projpath->subpath, all_rels);
            }
            else
                ereport(ERROR, errmsg("[pg_lab] Cannot check prefix query. Result path is not supported: %s",
                                        nodeToString(path)));
            break;
        }

        default:
            ereport(ERROR, errmsg("[pg_lab] Cannot hint query. Path type is not supported: %s",
                                    nodeToString(path)));
            break;
    }

}

bool
is_linear_join_order(JoinOrder* join_order)
{
    if (join_order->node_type == BASE_REL)
        return true;

    Assert(join_order->node_type == JOIN_REL);
    if (join_order->outer_child->node_type != BASE_REL &&
        join_order->inner_child->node_type != BASE_REL)
        return false;

    return is_linear_join_order(join_order->outer_child) &&
           is_linear_join_order(join_order->inner_child);
}

void
joinorder_it_init(JoinOrderIterator *iterator, JoinOrder *join_order)
{
    List        *leaf_nodes = NIL;
    JoinOrder   *current_node = NULL;
    List        *node_stack = list_make1(join_order);

    iterator->current_relids = EMPTY_BITMAP;

    while (node_stack)
    {
        current_node = (JoinOrder*) linitial(node_stack);
        if (current_node->node_type == BASE_REL)
        {
            leaf_nodes = lappend(leaf_nodes, current_node);
            bms_add_member(iterator->current_relids, current_node->rt_index);
        }
        else
        {
            node_stack = lappend(node_stack, current_node->outer_child);
            node_stack = lappend(node_stack, current_node->inner_child);
        }
        node_stack = list_delete_first(node_stack);
    }

    iterator->done = list_length(leaf_nodes) == 0;
    iterator->current_nodes = leaf_nodes;
}

void
joinorder_it_next(JoinOrderIterator *iterator)
{
    List        *new_nodes = NIL;
    int          min_new_level = INT_MAX;
    JoinOrder   *current_node = NULL;
    JoinOrder   *parent_node = NULL;
    ListCell    *lc = NULL;

    if (iterator->done)
        return;

    bms_free(iterator->current_relids);
    iterator->current_relids = EMPTY_BITMAP;

    foreach (lc, iterator->current_nodes)
    {
        current_node = (JoinOrder*) lfirst(lc);
        parent_node = current_node->parent_node;

        if (!parent_node || parent_node->level > min_new_level)
            continue;

        if (parent_node->level < min_new_level)
        {
            list_free(new_nodes);
            new_nodes = list_make1(parent_node);

            bms_free(iterator->current_relids);
            iterator->current_relids = bms_copy(parent_node->relids);

            min_new_level = parent_node->level;
        }
        else if (!bms_overlap(parent_node->relids, iterator->current_relids))
        {
            new_nodes = lappend(new_nodes, parent_node);
            bms_add_members(iterator->current_relids, parent_node->relids);
        }
    }

    list_free(iterator->current_nodes);
    iterator->done = list_length(new_nodes) == 0;
    iterator->current_nodes = new_nodes;
}

void
joinorder_it_free(JoinOrderIterator *iterator)
{
    list_free(iterator->current_nodes);
    bms_free(iterator->current_relids);
}

void
joinorder_to_string(JoinOrder *join_order, StringInfo buf)
{
    if (join_order->node_type == BASE_REL)
        appendStringInfoString(buf, join_order->base_identifier);
    else
    {
        appendStringInfoChar(buf, '(');
        joinorder_to_string(join_order->outer_child, buf);
        appendStringInfoString(buf, ", ");
        joinorder_to_string(join_order->inner_child, buf);
        appendStringInfoChar(buf, ')');
    }
}


static Index
FetchRTIndex(PlannerInfo *root, const char *relname)
{
    for (int i = 1; i < root->simple_rel_array_size; ++i)
    {
        RangeTblEntry *rte = root->simple_rte_array[i];
        if (rte && strcmp(rte->eref->aliasname, relname) == 0)
            return i;
    }

    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
             errmsg("relation \"%s\" does not exist", relname)));
    return InvalidIndex;
}

static Relids
FetchRelids(PlannerInfo *root, List *relnames)
{
    ListCell *lc;
    Relids relids = EMPTY_BITMAP;

    if (relnames == NIL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Must supply at least one relation")));
    }

    foreach (lc, relnames)
    {
        char *relname = (char *) lfirst(lc);
        Index rti = FetchRTIndex(root, relname);
        relids = bms_add_member(relids, rti);
    }

    return relids;
}

static char *
relnames_to_string(List *relnames)
{
    StringInfoData buf;
    ListCell *lc;
    char *result;

    initStringInfo(&buf);
    appendStringInfoChar(&buf, '(');

    foreach (lc, relnames)
    {
        char *relname = (char *) lfirst(lc);
        appendStringInfoString(&buf, relname);
        if (lnext(relnames, lc))
            appendStringInfoString(&buf, ", ");
    }

    appendStringInfoChar(&buf, ')');
    result = pstrdup(buf.data);

    return result;
}

PlannerHints *
init_hints(const char *raw_query)
{
    PlannerHints *hints;

    hints = (PlannerHints *) palloc0(sizeof(PlannerHints));
    hints->raw_query = pstrdup(raw_query);
    hints->contains_hint = false;
    hints->raw_hint = NULL;

    hints->mode = HINTMODE_ANCHORED;
    hints->parallel_mode = PARMODE_DEFAULT;

    hints->join_order_hint = NULL;
    hints->join_prefixes = NIL;

    hints->operator_hints = NULL;
    hints->cardinality_hints = NULL;
    hints->cost_hints = NULL;

    hints->parallel_rels = EMPTY_BITMAP;
    hints->parallel_workers = 0;
    hints->parallelize_entire_plan = false;

    hints->pre_opt_gucs = NIL;
    hints->post_opt_gucs = NIL;

    return hints;
}

void
free_hints(PlannerHints *hints)
{
    if (!hints)
        return;

    if (hints->join_order_hint)
        free_join_order(hints->join_order_hint);

    list_free_deep(hints->join_prefixes);

    hash_destroy(hints->operator_hints);
    hash_destroy(hints->cardinality_hints);
    hash_destroy(hints->cost_hints);
}

void
post_process_hint_block(PlannerHints *hints)
{
    JoinOrderIterator it;

    if (!hints ||
        !hints->contains_hint ||
        !hints->join_order_hint ||
        !hints->operator_hints ||
        hash_get_num_entries(hints->operator_hints) == 0)
        return;

    joinorder_it_init(&it, hints->join_order_hint);

    while (!it.done)
    {
        ListCell *lc;
        foreach (lc, it.current_nodes)
        {
            OperatorHint *op_hint;
            bool found;
            JoinOrder *current_node = (JoinOrder *) lfirst(lc);

            op_hint = (OperatorHint *) hash_search(hints->operator_hints, &(current_node->relids),
                                                   HASH_FIND, &found);
            if (found)
                current_node->physical_op = op_hint;
        }

        joinorder_it_next(&it);
    }

    joinorder_it_free(&it);
}

void
MakeOperatorHint(PlannerInfo *root, PlannerHints *hints, List *rels,
                 PhysicalOperator op, float par_workers)
{
    OperatorHint *op_hint;
    bool found;
    Relids relids;

    hints->contains_hint = true;

    if (!hints->operator_hints)
    {
        HASHCTL hctl;
        long nelems;

        hctl.keysize = sizeof(Relids);
        hctl.entrysize = sizeof(OperatorHint);
        hctl.hcxt = CurrentMemoryContext;
        hctl.hash = bitmap_hash;
        hctl.match = bitmap_match;

        nelems = 2 * list_length(root->parse->rtable) - 1;
        hints->operator_hints = hash_create("OperatorHintHashes", nelems, &hctl,
                                            HASH_ELEM | HASH_CONTEXT | HASH_COMPARE | HASH_FUNCTION);
    }

    relids = FetchRelids(root, rels);
    op_hint = (OperatorHint *) hash_search(hints->operator_hints, &relids, HASH_ENTER, &found);

    #ifdef PGLAB_TRACE

    ereport(INFO, (errmsg("Creating operator hint"),
                   errdetail("op=%s, rels=%s",  PhysicalOperatorToString(op), relnames_to_string(rels))));

    #endif

    if (found)
    {
        /* The hint could already exist if we parsed a Memoize/Materialize hint before */
        op_hint->op = op;

        if (!isnan(par_workers))
            op_hint->parallel_workers = par_workers;
    }
    else
    {
        op_hint->op = op;
        op_hint->materialize_output = false;
        op_hint->memoize_output = false;
        op_hint->parallel_workers = par_workers;
    }

    if (!isnan(par_workers))
    {
        hints->parallel_mode = PARMODE_PARALLEL;
        hints->parallel_rels = relids;
        hints->parallel_workers = par_workers;
    }
}

void
MakeIntermediateOpHint(PlannerInfo *root, PlannerHints *hints, List *rels,
                       bool materialize, bool memoize, float par_workers)
{
    OperatorHint *op_hint;
    bool found;
    Relids relids;

    hints->contains_hint = true;

    if (!hints->operator_hints)
    {
        HASHCTL hctl;
        long nelems;

        hctl.keysize = sizeof(Relids);
        hctl.entrysize = sizeof(OperatorHint);
        hctl.hcxt = CurrentMemoryContext;
        hctl.hash = bitmap_hash;
        hctl.match = bitmap_match;

        nelems = 2 * list_length(root->parse->rtable) - 1;
        hints->operator_hints = hash_create("OperatorHintHashes", nelems, &hctl,
                                            HASH_ELEM | HASH_CONTEXT | HASH_COMPARE | HASH_FUNCTION);
    }

    relids = FetchRelids(root, rels);
    op_hint = (OperatorHint *) hash_search(hints->operator_hints, &relids, HASH_ENTER, &found);

    if (found)
    {
        /* The hint could already exist if we parsed an operator hint before */
        op_hint->materialize_output = materialize;
        op_hint->memoize_output = memoize;

        if (!isnan(par_workers))
            op_hint->parallel_workers = par_workers;
    }
    else
    {
        op_hint->op = OP_UNKNOWN; // Set to unknown as we don't know the physical operator yet
        op_hint->materialize_output = materialize;
        op_hint->memoize_output = memoize;
        op_hint->parallel_workers = par_workers;
    }

    if (!isnan(par_workers))
    {
        hints->parallel_mode = PARMODE_PARALLEL;
        hints->parallel_rels = relids;
        hints->parallel_workers = par_workers;
    }
}

void
MakeCardHint(PlannerInfo *root, PlannerHints *hints, List *rels, Cardinality cardinality)
{
    CardinalityHint *card_hint;
    bool found;
    Relids relids;

    hints->contains_hint = true;

    if (!hints->cardinality_hints)
    {
        HASHCTL hctl;
        long nelems;

        hctl.keysize = sizeof(Relids);
        hctl.entrysize = sizeof(CardinalityHint);
        hctl.hcxt = CurrentMemoryContext;
        hctl.hash = bitmap_hash;
        hctl.match = bitmap_match;

        nelems = 2 * list_length(root->parse->rtable) - 1;
        hints->cardinality_hints = hash_create("CardinalityHintHashes", nelems, &hctl,
                                               HASH_ELEM | HASH_CONTEXT | HASH_COMPARE | HASH_FUNCTION);
    }

    relids = FetchRelids(root, rels);
    card_hint = (CardinalityHint *) hash_search(hints->cardinality_hints, &relids, HASH_ENTER, &found);

    if (found)
    {
        char *relnames;
        relnames = relnames_to_string(rels);
        ereport(WARNING,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("Duplicate cardinality hint"),
                 errdetail("Relations: %s", relnames)));
        pfree(relnames);
        card_hint->card = cardinality;
    }
    else
        card_hint->card = cardinality;
}

void
MakeCostHint(PlannerInfo *root, PlannerHints *hints, List *rels, PhysicalOperator op, Cost startup, Cost total)
{
    CostHint *cost_hint;
    bool found;
    Relids relids;
    bool baserel;

    hints->contains_hint = true;

    if (!hints->cost_hints)
    {
        HASHCTL hctl;
        long nelems;

        hctl.keysize = sizeof(Relids);
        hctl.entrysize = sizeof(CostHint);
        hctl.hcxt = CurrentMemoryContext;
        hctl.hash = bitmap_hash;
        hctl.match = bitmap_match;

        nelems = 6 * list_length(root->parse->rtable) - 1;
        hints->cost_hints = hash_create("CostHintHashes", nelems, &hctl,
                                        HASH_ELEM | HASH_CONTEXT | HASH_COMPARE | HASH_FUNCTION);
    }

    baserel = list_length(rels) == 1;
    relids = FetchRelids(root, rels);
    cost_hint = (CostHint *) hash_search(hints->cost_hints, &relids, HASH_ENTER, &found);

    if (!found && baserel)
    {
        ScanCost *costs;
        cost_hint->node_type = BASE_REL;
        costs = &(cost_hint->costs.scan_cost);

        costs->seqscan_startup = NAN;
        costs->seqscan_total = NAN;
        costs->idxscan_startup = NAN;
        costs->idxscan_total = NAN;
        costs->bitmap_startup = NAN;
        costs->bitmap_total = NAN;
    }
    else if (!found && !baserel)
    {
        JoinCost *costs;
        cost_hint->node_type = JOIN_REL;
        costs = &(cost_hint->costs.join_cost);

        costs->nestloop_startup = NAN;
        costs->nestloop_total = NAN;
        costs->hash_startup = NAN;
        costs->hash_total = NAN;
        costs->merge_startup = NAN;
        costs->merge_total = NAN;
    }

    switch (op)
    {
        case OP_SEQSCAN:
            cost_hint->costs.scan_cost.seqscan_startup = startup;
            cost_hint->costs.scan_cost.seqscan_total = total;
            break;
        case OP_IDXSCAN:
            cost_hint->costs.scan_cost.idxscan_startup = startup;
            cost_hint->costs.scan_cost.idxscan_total = total;
            break;
        case OP_BITMAPSCAN:
            cost_hint->costs.scan_cost.bitmap_startup = startup;
            cost_hint->costs.scan_cost.bitmap_total = total;
            break;
        case OP_NESTLOOP:
            cost_hint->costs.join_cost.nestloop_startup = startup;
            cost_hint->costs.join_cost.nestloop_total = total;
            break;
        case OP_HASHJOIN:
            cost_hint->costs.join_cost.hash_startup = startup;
            cost_hint->costs.join_cost.hash_total = total;
            break;
        case OP_MERGEJOIN:
            cost_hint->costs.join_cost.merge_startup = startup;
            cost_hint->costs.join_cost.merge_total = total;
            break;
        default:
            elog(ERROR, "Unknown scan operator: %d", op);
            break;
    }

}

JoinOrder *
MakeJoinOrderIntermediate(PlannerInfo *root, JoinOrder *outer_child, JoinOrder *inner_child)
{
    JoinOrder *join_order;
    int level;

    level = (outer_child->level > inner_child->level ? outer_child->level : inner_child->level) + 1;

    join_order = (JoinOrder *) palloc0(sizeof(JoinOrder));
    join_order->node_type = JOIN_REL;
    join_order->relids = bms_union(outer_child->relids, inner_child->relids);
    join_order->outer_child = outer_child;
    join_order->inner_child = inner_child;
    outer_child->parent_node = join_order;
    inner_child->parent_node = join_order;
    join_order->level = level;
    join_order->physical_op = NULL;
    join_order->parent_node = NULL;

    return join_order;
}

JoinOrder *
MakeJoinOrderBase(PlannerInfo *root, const char *relname)
{
    JoinOrder *join_order;
    Index rti;

    rti = FetchRTIndex(root, relname);

    join_order = (JoinOrder *) palloc0(sizeof(JoinOrder));
    join_order->node_type = BASE_REL;
    join_order->relids = bms_make_singleton(rti);
    join_order->rt_index = rti;
    join_order->base_identifier = pstrdup(relname);
    join_order->level = 1;
    join_order->physical_op = NULL;
    join_order->parent_node = NULL;

    return join_order;
}

void MakeGUCHint(PlannerHints *hints, const char *guc_name, const char *guc_value)
{
    TempGUC *set_guc, *reset_guc;
    const char *current_val;

    current_val = GetConfigOption(guc_name, true, true);
    if (current_val == NULL)
    {
        ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("GUC \"%s\" does not exist, cannot set hint", guc_name)));
        return;
    }

    set_guc = (TempGUC *) palloc0(sizeof(TempGUC));
    set_guc->guc_name = pstrdup(guc_name);
    set_guc->guc_value = pstrdup(guc_value);

    reset_guc = (TempGUC *) palloc0(sizeof(TempGUC));
    reset_guc->guc_name = pstrdup(guc_name);
    reset_guc->guc_value = pstrdup(current_val);

    hints->pre_opt_gucs = lappend(hints->pre_opt_gucs, set_guc);
    hints->post_opt_gucs = lappend(hints->post_opt_gucs, reset_guc);
    hints->contains_hint = true;
}

#ifdef __cplusplus
} // extern "C"
#endif
