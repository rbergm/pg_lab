
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


#define InvalidIndex -1
#define EMPTY_BITMAP NULL

/* utility macros inspired by nodeTag and IsA from nodes.h */
#define pathTag(pathptr) (((const Path*)pathptr)->pathtype)
#define PathIsA(nodeptr,_type_) (pathTag(nodeptr) == T_ ## _type_)
#define IsAScanPath(pathptr) (PathIsA(pathptr, SeqScan) || \
                              PathIsA(pathptr, IndexScan) || \
                              PathIsA(pathptr, IndexOnlyScan) || \
                              PathIsA(pathptr, BitmapHeapScan) || \
                              PathIsA(pathptr, TidScan) || \
                              PathIsA(pathptr, TidRangePath))
#define IsAJoinPath(pathptr) (PathIsA(pathptr, NestLoop) || PathIsA(pathptr, MergeJoin) || PathIsA(pathptr, HashJoin))
#define IsAIntermediatePath(pathptr) (PathIsA(pathptr, Memoize) || PathIsA(pathptr, Material))
#define IsAParPath(pathptr) (PathIsA(pathptr, Gather) || PathIsA(pathptr, GatherMerge))

typedef struct TempGUC
{
    char *guc_name;
    char *guc_value;
} TempGUC;


/* The query currently being optimized/executed. */
extern char *current_query_string;

extern TempGUC **guc_cleanup_actions;
extern int n_cleanup_actions;

typedef enum HintMode
{
    HINTMODE_FULL,
    HINTMODE_ANCHORED
} HintMode;

typedef enum ParallelMode
{
    PARMODE_DEFAULT,
    PARMODE_SEQUENTIAL,
    PARMODE_PARALLEL
} ParallelMode;


typedef enum HintTag
{
    BASE_REL,
    JOIN_REL
} HintTag;


typedef enum PhysicalOperator
{
    OP_UNKNOWN,
    OP_SEQSCAN,
    OP_IDXSCAN,
    OP_BITMAPSCAN,
    OP_NESTLOOP,
    OP_HASHJOIN,
    OP_MERGEJOIN,
    OP_MEMOIZE,
    OP_MATERIALIZE
} PhysicalOperator;

extern const char *PhysicalOperatorToString(PhysicalOperator op);

typedef struct OperatorHint
{
    Relids           relids;
    PhysicalOperator op;

    bool materialize_output;
    bool memoize_output;

    float parallel_workers;
} OperatorHint;

typedef struct JoinOrder
{
    HintTag node_type;
    Relids  relids;  /* The rangetable indexes of the relations combined by this node */

    /* Only set for base rels */
    Index  rt_index;
    char  *base_identifier;

    /* Only set for join rels */
    JoinOrder *outer_child;
    JoinOrder *inner_child;

    /* Set on all nodes */
    int           level;
    OperatorHint *physical_op;
    JoinOrder    *parent_node; /* NULL for root node */
} JoinOrder;

typedef enum JoinOrder_Comparison
{
    JO_DISJOINT,
    JO_EQUAL,
    JO_DIFFERENT
} JoinOrder_Comparison;

#define IsRootNode(join_order) ((join_order)->parent_node == NULL)

extern JoinOrder* traverse_join_order(JoinOrder *join_order, Relids node);
extern JoinOrder_Comparison join_order_compare(JoinOrder *prefix, Path *path, Relids all_rels);
extern bool is_linear_join_order(JoinOrder *join_order);
extern void free_join_order(JoinOrder *join_order);

typedef struct JoinOrderIterator
{
    bool       done;
    List      *current_nodes;
    Bitmapset *current_relids;
} JoinOrderIterator;

extern void joinorder_it_init(JoinOrderIterator *iterator, JoinOrder *join_order);
extern void joinorder_it_next(JoinOrderIterator *iterator);
extern void joinorder_it_free(JoinOrderIterator *iterator);

extern void joinorder_to_string(JoinOrder *join_order, StringInfo buf);

typedef struct CardinalityHint
{
    Relids relids;
    Cardinality card;
} CardinalityHint;

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

typedef struct CostHint
{
    Relids  relids;
    HintTag node_type;

    union
    {
        ScanCost scan_cost;
        JoinCost join_cost;
    } costs;
} CostHint;

typedef struct PlannerHints
{
    char *raw_query;

    bool contains_hint;

    char *raw_hint;

    HintMode mode;

    ParallelMode parallel_mode;

    JoinOrder *join_order_hint;

    List *join_prefixes;

    struct HTAB *operator_hints;

    struct HTAB *cardinality_hints;

    struct HTAB *cost_hints;

    Relids parallel_rels;

    int parallel_workers;

    bool parallelize_entire_plan;

    List *temp_gucs;

} PlannerHints;


extern PlannerHints* init_hints(const char *raw_query);
extern void free_hints(PlannerHints *hints);
extern void parse_hint_block(PlannerInfo *root, PlannerHints *hints);
extern void post_process_hint_block(PlannerHints *hints);

extern void MakeOperatorHint(PlannerInfo *root, PlannerHints *hints, List *rels,
                             PhysicalOperator op, float par_workers);
extern void MakeIntermediateOpHint(PlannerInfo *root, PlannerHints *hints, List *rels,
                                   bool materialize, bool memoize, float par_workers);

extern void MakeCardHint(PlannerInfo *root, PlannerHints *hints, List *rels, Cardinality card);

extern void MakeCostHint(PlannerInfo *root, PlannerHints *hints, List *rels,
                         PhysicalOperator op, Cost startup_cost, Cost total_cost);

extern JoinOrder* MakeJoinOrderIntermediate(PlannerInfo *root, JoinOrder *outer_child, JoinOrder *inner_child);
extern JoinOrder* MakeJoinOrderBase(PlannerInfo *root, const char *relname);

extern TempGUC* MakeGUCHint(PlannerHints *hints, const char *guc_name, const char *guc_value);

extern void InitGucCleanup(int n_actions);
extern void StoreGucCleanup(TempGUC *temp_guc);
extern void FreeGucCleanup();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // HINTS_H
