
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

typedef enum JoinNodeTag
{
    BASE_REL,
    JOIN_REL
} JoinNodeTag;


typedef enum PhysicalOperator
{
    SEQSCAN,
    IDXSCAN,
    NESTLOOP,
    HASHJOIN,
    MERGEJOIN
} PhysicalOperator;

typedef struct JoinOrder
{
    JoinNodeTag node_type;
    Relids relids;  /* The rangetable indexes of the relations combined by this node */

    /* Only set for base rels */
    Index rt_index;
    char* base_identifier;

    /* Only set for join rels */
    JoinOrder *left_child;
    JoinOrder *right_child;

    /* Set on all nodes */
    int level;
    JoinOrder *parent_node; /* NULL for root node */
} JoinOrder;

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

typedef enum OperatorHintType
{
    FORCED_OP,
    COST_OP
} OperatorHintType;

typedef struct OperatorHashEntry
{
    Relids relids;
    OperatorHintType hint_type;
    PhysicalOperator op;
    Cost startup_cost;
    Cost total_cost;
} OperatorHashEntry;

typedef struct CardinalityHashEntry
{
    Relids relids;
    Cardinality card;
} CardinalityHashEntry;


typedef struct PlannerHints {

    /* FIXME: the current BMS-based hash tables do not work for queries with multiple references to the same physical relation */

    /*
     * We need to include the GeQO data as the first field here b/c there is no guarantee
     * we force the join order via a hint.
     * In this case, Postgres' default policies will be used. Specifically, GeQO might still be
     * invoked. If we store our planner info in the join search private data, this field would no
     * longer be usable by GeQO. Therefore, we store the GeQO data here. All casts from within the
     * GeQO code will still work.
     */
    GeqoPrivateData geqo_private;

    char *raw_query;

    bool contains_hint;

    char *raw_hint;

    JoinOrder *join_order_hint;

    HTAB *operator_hints;

    HTAB *cardinality_hints;

} PlannerHints;

#ifdef __cplusplus
} // extern "C"
#endif

#endif // HINTS_H
