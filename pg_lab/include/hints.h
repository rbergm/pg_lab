
#ifndef HINTS_H
#define HINTS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "optimizer/geqo.h"
#include "utils/hsearch.h"


#define EMPTY_BITMAP NULL;

extern char *current_query_string;

typedef enum HintMode
{
    FULL,
    ANCHORED
} HintMode;


typedef enum HintTag
{
    BASE_REL,
    JOIN_REL
} HintTag;


typedef enum PhysicalOperator
{
    NOT_SPECIFIED,
    SEQSCAN,
    IDXSCAN,
    NESTLOOP,
    HASHJOIN,
    MERGEJOIN,
    MEMOIZE,
    MATERIALIZE
} PhysicalOperator;

typedef struct JoinOrder
{
    HintTag node_type;
    Relids relids;  /* The rangetable indexes of the relations combined by this node */

    /* Only set for base rels */
    Index rt_index;
    char* base_identifier;

    /* Only set for join rels */
    JoinOrder *outer_child;
    JoinOrder *inner_child;

    /* Set on all nodes */
    int level;
    JoinOrder *parent_node; /* NULL for root node */
} JoinOrder;

#define join_order_is_root(join_order) ((join_order)->parent_node == NULL)

extern JoinOrder* traverse_join_order(JoinOrder *join_order, Relids node);
extern bool is_linear_join_order(JoinOrder *join_order);
extern void free_join_order(JoinOrder *join_order);

typedef struct JoinOrderIterator
{
    bool done;
    List *current_nodes;
    Bitmapset *current_relids;
} JoinOrderIterator;

extern void init_join_order_iterator(JoinOrderIterator *iterator, JoinOrder *join_order);
extern void join_order_iterator_next(JoinOrderIterator *iterator);
extern void free_join_order_iterator(JoinOrderIterator *iterator);

typedef struct OperatorHashEntry
{
    Relids relids;
    PhysicalOperator op;

    bool materialize_output;
    bool memoize_output;
} OperatorHashEntry;

typedef struct CardinalityHashEntry
{
    Relids relids;
    Cardinality card;
} CardinalityHashEntry;

typedef struct ScanCost
{
    Cost seqscan_startup;
    Cost seqscan_total;
    Cost idxscan_startup;
    Cost idxscan_total;
    Cost bitmap_startup;
    Cost bitmap_total;
} ScanCost;

typedef struct JoinCost
{
    Cost nestloop_startup;
    Cost nestloop_total;
    Cost hash_startup;
    Cost hash_total;
    Cost merge_startup;
    Cost merge_total;
} JoinCost;

typedef struct CostHashEntry
{
    Relids relids;
    HintTag node_type;

    union
    {
        ScanCost scan_cost;
        JoinCost join_cost;
    } costs;
} CostHashEntry;

typedef struct PlannerHints {

    char *raw_query;

    bool contains_hint;

    char *raw_hint;

    HintMode mode;

    JoinOrder *join_order_hint;

    HTAB *operator_hints;

    HTAB *cardinality_hints;

    HTAB *cost_hints;

} PlannerHints;

#ifdef __cplusplus
} // extern "C"
#endif

#endif // HINTS_H
