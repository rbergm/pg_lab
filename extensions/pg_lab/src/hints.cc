
#include <limits.h>
#include <math.h>

#include "nodes/bitmapset.h"

#include "hints.h"

#ifdef __cplusplus
extern "C" {
#endif

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
    hints->mode = ANCHORED;
    hints->parallel_mode = PARMODE_DEFAULT;
    hints->join_order_hint = NULL;
    hints->operator_hints = NULL;
    hints->cardinality_hints = NULL;
    hints->cost_hints = NULL;
    hints->parallel_rels = EMPTY_BITMAP;
    hints->parallel_workers = 0;
    hints->parallelize_entire_plan = false;

    return hints;
}

void
free_hints(PlannerHints *hints)
{
    if (!hints)
        return;

    if (hints->join_order_hint)
        free_join_order(hints->join_order_hint);

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

#ifdef __cplusplus
} // extern "C"
#endif
