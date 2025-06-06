
# Postgres Extensions

pg_lab modifies some aspects of the PostgreSQL query optimizer to make the development of optimizer extensions easier.
These changes mostly add additional extension points to the optimization workflow.
In turn, the extension points can be used by users to implement their own custom optimizer behavior.


## Extension points

By default, vanilla Postgres provides three kinds of extension points for the optimizer:

- `planner_hook` (defined in _planner.h_) enables to overwrite the entire optimization process
- `join_search_hook` handles the construction of everything below the _upper relation_, i.e. the join order as well as the
  assignment of physical operators[^physical-ops]
- `set_rel_pathlist_hook`, `set_join_pathlist_hook` and `create_upper_paths_hook` control the available access paths/
  physical operators for a specific intermediate[^pathlist-hooks]

However, these hooks barely scratch the surface of common common research goals. For example, it is pretty much impossible
to implement a custom cardinality estimation strategy as an extension (i.e. without modifying the actual Postgres source
code). Therefore, pg_lab adds additional extension points to allow for such use cases.

[^physical-ops] Actually, *join_search_hook* does not handle the selection of physical operators by itself, but it collects
    which access paths are available on the intermediate (potentially including different join orders). Furthermore, it
    determines the cost of each of these paths. Essentially, this gives users complete control over the selected join
    order as well as the physical operators.

[^pathlist-hooks] The names of these hooks are a bit misleading because they explicitly allow the user to also remove
    existing paths from the pathlists. By combining this deletion step with a subsequent (re-)creation of the desired
    paths, users once again have complete control over the pathlist.


### Overview

pg_lab provides the following extension points:

| EP | Location | Description |
|----|----------|-------------|
| `set_baserel_size_estimates_hook` | _cost.h_ | Sets a custom cardinality estimate on a base relation |
| `set_joinrel_size_estimates_hook` | _cost.h_ | Sets a custom cardinality estimate for a join relation |
| `compute_parallel_worker_hook` | _paths.h_ | Sets the number of parallel workers that _would_ be used if the relation should be scanned in parallel |
| `cost_XXX_hook` | _cost.h_ | Sets the estimated cost for a specific operator, e.g. `cost_seqscan_hook` |
| `prepare_make_one_rel_hook` | _planmain.h_ | Callback for plugins to perform initializations before the main initialization starts.


### Cardinality estimation hooks

**Interface for base relations:**
```c
double set_baserel_size_estimates_hook(PlannerInfo *root, RelOptInfo *rel)
```
(parameters similar to `set_baserel_size_estimates()`)

**Interface for join relations:**
```c
double set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *join_rel,
                                  RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                                  SpecialJoinInfo *sjinfo, List *restrictlist)
```
(parameters similar to `set_joinrel_size_estimates()`)

Notice that both hook methods provide the estimated cardinality as the return value, rather than directly setting it on the
RelOptInfo.
The reason is that the vanilla methods perform additional post-processing such as clamping the estimation to the allowed
range.

The vanilla behavior can be invoked by calling `standard_set_baserel_size_estimates()` or
`standard_set_joinrel_size_estimates()` respectively.
This can be useful as a fallback if an extension encouters a case where it cannot (or does not want to) calculate a proper
estimate.


### Cost estimation hooks

Hooks for the cost model all follow the same pattern.
Based on the original `cost_XXX()` function, a `cost_XXX_hook` is introduced that has the same signature as the original
function.
Furthermore, a `standard_cost_XXX()` function is added which implements the vanilla behavior (and also has the same
signature).

In general, it is recommended to always call the standard function at the beginning of each hook, even if the hook provides
its own cost estimate.
The reason is that many cost functions also set parameters other than `cost` on the path, and these might not be handled by
the hook.
This way, the path is always completely initialized and no segfaults/NPEs occur (even if the values might not be
completely sound).
If the extension computes estimates for all required attributes, this is of course not necessary.
However, doing so probably requires a thorough reverse engineering of the original function.

Notice that for join operators, two hooks exist and must be set in order for the estimation to work as expected:
the `initial_cost_XXX()` function and the `final_cost_XXX()` function.
Both functions also provide their standard counterparts.
On a practical level, extension can of course execute the same logic in both code paths.

All currently supported cost hints are listed below.
Their parameters and return types are the same as for the original functions.
We did not implement hooks for every single operator yet (simple because there are too many that are rarely useful such as
`cost_tidscan()`).
If a cost hook that you need for your specific use case should be missing, feel free to
[open an issue](https://github.com/rbergm/pg_lab/issues/new/choose) or create a PR[^creating-prs] right away.

[^creating-prs] On a technical level, we maintain 
    [our own Postgres fork](https://github.com/rbergm/pg_lab/issues/new/choose) to keep track of all modifications that are
    required to keep pg_lab running (and to integrate upstream changes cleanly). Therefore, the PR has to be opened against
    this repo.

**Scan operators:**

```c
void cost_seqscan_hook(Path *path, PlannerInfo *root,
                       RelOptInfo *baserel, ParamPathInfo *param_info);
/* Remember that Postgres uses a plain Path* instance for sequential scans */

void cost_index_hook(IndexPath *path, PlannerInfo *root,
                     double loop_count, bool partial_path);

void cost_bitmap_heap_scan_hook(Path *path, PlannerInfo *root,
                                RelOptInfo *baserel, ParamPathInfo *param_info,
                                Path *bitmapqual, double loop_count);
/*
 * This is the final scan portion. The individual index scans are represented
 * as IndexPaths
 */

void cost_bitmap_and_node_hook(BitmapAndPath *path, PlannerInfo *root);

void cost_bitmap_or_node_hook(BitmapOrPath *path, PlannerInfo *root);
```

**Intermediate operators:**

```c
void cost_sort_hook(Path *path, PlannerInfo *root,
                    List *pathkeys, Cost input_cost, double tuples, int width,
                    Cost comparison_cost, int sort_mem,
                    double limit_tuples);

void cost_incremental_sort_hook(Path *path, PlannerInfo *root,
                                List *pathkeys, int presorted_keys,
                                Cost input_startup_cost, Cost input_total_cost,
                                double input_tuples, int width,
                                Cost comparison_cost, int sort_mem,
                                double limit_tuples);

void cost_rescan_hook(PlannerInfo *root, Path *path,
                      Cost *rescan_startup_cost, Cost *rescan_total_cost);
/*
 * Notice that rescan receives arbitrary paths as input and sets its results
 * in the pointers directly.
 */

void cost_memoize_rescan_hook(PlannerInfo *root, MemoizePath *mpath,
                              Cost *rescan_startup_cost, Cost *rescan_total_cost);
/* Similar to cost_rescan, the result is set directly on the pointers. */

void cost_material_hook(Path *path, Cost input_startup_cost, Cost input_total_cost,
                        double tuples, int width);
```

**Upper rel operators:**

```c
void cost_agg_hook(Path *path, PlannerInfo *root,
                   AggStrategy aggstrategy, const AggClauseCosts *aggcosts,
                   int numGroupCols, double numGroups,
                   List *quals,
                   Cost input_startup_cost, Cost input_total_cost,
                   double input_tuples, double input_width);

void cost_windowagg_hook(Path *path, PlanenrInfo *root,
                         List *windowFuncs, WindowClause *winclause,
                         Cost input_startup_cost, Cost input_total_cost,
                         double input_tuples);

void cost_group_hook(Path *path, PlannerInfo *root,
                     int numGroupCols, double numGroups,
                     List *quals,
                     Cost input_startup_cost, Cost input_total_cost,
                     double input_tuples);
```

**Join operators:**

```c
void initial_cost_nestloop_hook(PlannerInfo *root,
                                JoinCostWorkspace *workspace,
                                JoinType jointype,
                                Path *outer_path, Path *inner_path,
                                JoinPathExtraData *extra);
void final_cost_nestloop_hook(PlannerInfo *root, NestPath *path,
                              JoinCostWorkspace *workspace,
                              JoinPathExtraData *extra);

void intial_cost_mergejoin_hook(PlannerInfo *root,
                                JoinCostWorkspace *workspace,
                                JoinType jointype,
                                List *mergeclauses,
                                Path *outer_path, Path *inner_path,
                                List *outersortkeys, List *innersortkeys,
                                JoinPathExtraData *extra);
void final_cost_mergejoin_hook(PlannerInfo *root, MergePath *path,
                               JoinCostWorkspace *workspace,
                               JoinPathExtraData *extra);

void initial_cost_hashjoin_hook(PlannerInfo *root,
                                JoinCostWorkspace *workspace,
                                JoinType jointype,
                                List *hashclauses,
                                Path *outer_path, Path *inner_path,
                                JoinPathExtraData *extra,
                                bool parallel_hash);
void final_cost_hashjoin_hook(PlannerInfo *root, MergePath *path,
                              JoinCostWorkspace *workspace,
                              JoinPathExtraData *extra);
```

**Parallelization operators:**

```c
void cost_gather_hook(GatherPath *path, PlannerInfo *root,
                      RelOptInfo *rel, ParamPathInfo *param_info,
                      double *rows);

void cost_gather_merge_hook(GatherMergePath *path, PlannerInfo *root,
                            RelOptInfo *rel, ParamPathInfo *param_info,
                            Cost input_startup_cost, Cost input_total_cost,
                            double *rows);
```

Important missing hooks:

- `cost_subplan()`
- `cost_ctescan()`
- `cost_subqueryscan()`
- `cost_append()`
- `cost_merge_append()`


### Parallel worker hook

**Interface:**
```c
int compute_parallel_worker_hook(RelOptInfo *rel,
                                 double heap_pages, double index_pages,
                                 int max_workers);
```

This hook is used to set the number of parallel workers that _would_ be used _if_ the optimizer decided, that a parallel
execution of the plan is worthwhile.
If the optimizer prevers a sequential execution, this value does nothing.
Notice that the number of parallel workers influence the cost estimates for the partial (i.e. parallel) paths.

The Postgres parallelization model is currently very simple: the number of parallel workers of an intermediate operator
are the same as the number of workers of the (outer) input relation.
Therefore, this setting has a very large impact on the overall plan.

For each parallel plan, the optimizer decides whether it might be worth to finish parallelization at this node, or whether
some more upper nodes should be integrated in the parallel plan.
Afterwards, the entire subplan is executed in parallel.
Notice that the final number of parallel processes is the number of parallel workers plus one, because the main backend
process also operates on part of the input (in addition to gathering the results from the workers)[^parallel-processing].

[^parallel-processing] However, if fewer than estimated parallel workers are available at execution time, not all workers
  might be used.


### `make_one_rel` hook

**Interface:** 
```c
void make_one_rel_hook(RelOptInfo *root, List *joinlist)
```
(parameters similar to `make_one_rel()`)

This hook differs from the other extension points in that it does not provide customization of a Postgres procedure.
Instead, it enables completely new behavior.
Its main purpose is for extensions to perform its own initialization just before the actual optimization starts, but after
all vanilla Postgres data structures have been initialized.

More specifically, when `make_one_rel_hook` is called, the `PlannerInfo` is completely initialized, but the `RelOptInfo`s
for the base relations have not yet been computed.
This differs from the `join_search_hook` which is called with a list of readily available base rels.

After the hook terminates, the optimizer proceeds with `make_one_rel` to compute the query plan. 


## Additional modifications

To make the life of extension developers easier, pg_lab also adds two additional fields called `pglab_private` to the
`Path` and `RelOptInfo` structs.
Similar to `join_search_private` in `PlannerInfo`, these are simple `void *` pointers which allow developers to store
arbitrary data for their extensions.

For a nicer debugging experience, pg_lab also defines the global `current_planner_type` and `current_join_ordering_type`
variables. Both are pointers to strings that should be set by extensions that use the corresponding hooks.
The purpose of these variables is to get 
