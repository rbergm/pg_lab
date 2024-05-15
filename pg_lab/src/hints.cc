
#include <limits.h>

#include "hints.h"

#ifdef __cplusplus
extern "C" {
#endif

void free_join_order(JoinOrder *join_order)
{
    if (join_order->node_type == JOIN_REL)
    {
        free_join_order(join_order->left_child);
        free_join_order(join_order->right_child);
    }

    bms_free(join_order->relids);
    if (join_order->base_identifier)
        pfree(join_order->base_identifier);

    pfree(join_order);
}

void
init_join_order_iterator(JoinOrderIterator *iterator, JoinOrder *join_order)
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
            node_stack = lappend(node_stack, current_node->left_child);
            node_stack = lappend(node_stack, current_node->right_child);
        }
        node_stack = list_delete_first(node_stack);
    }

    iterator->done = list_length(leaf_nodes) == 0;
    iterator->current_nodes = leaf_nodes;
}

void
join_order_iterator_next(JoinOrderIterator *iterator)
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
free_join_order_iterator(JoinOrderIterator *iterator)
{
    list_free(iterator->current_nodes);
    bms_free(iterator->current_relids);
}


#ifdef __cplusplus
} // extern "C"
#endif
