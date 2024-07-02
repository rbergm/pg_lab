# pg_lab

<img src="assets/pg_lab_logo.png" width="256" alt="The Logo of pg_lab: a blue elephant wearing a chemist's coat is surrounded by various reagents." />

_pg\_lab_ is a research-focused fork of PostgreSQL. Its goal is to enable research into the design of query optimizers for
relational database systems.

The Postgres fork provides extension hooks for many important aspects of the query optimizer, such as the selection of a join
order or physical access paths, as well as the computation of cardinality and cost estimates. In addition, pg_lab ships a
hinting language inspired by [pg_hint_plan](https://github.com/ossc-db/pg_hint_plan) that allows to embed such decisions in
comment blocks of the actual SQL queries:

```sql
/*=pg_lab=
 Card(a #42000)

 JoinOrder(((a b) c))
 IdxScan(b)
 NestLoop(a b)

 HashJoin(a b c (COST start=50 total=500))
 MergeJoin(a b c (COST start=25 total=400))
 NestLoop(a b c (COST start=100 total=1000))
*/
SELECT count(*)
FROM a, b, c
WHERE a.id = b.id
  AND a.id = c.id
```

## Comparison to pg_hint_plan

While a lot of the behavior of pg_lab can also be found in pg_hint_plan, some
features are only present in pg_lab, but not in pg_hint_plan and vice-versa. See the table below for a comparison.
Furthermore, pg_lab uses a different design philosophy: instead of directly modifying the optimizer's logic, pg_lab utilizes
extension points to implement its custom behavior. The fundamental control-flow of the optimizer is thereby left intact.

| Feature | pg_hint_plan | pg_lab |
|---------|--------------|--------|
| Forcing the join order | ✅ `Leading` hint | ✅ `JoinOrder` hint |
| Forcing physical operators | ✅ Specific hints, e.g. `NestLoop(a b)` | ✅ Specific hints, e.g. `NestLoop(a b (FORCED))` |
| Custom cardinality estimates for joins | ✅ `Rows` hint | ✅ `Card` hint |
| Custom cardinality estimates for base tables | ❌ Not supported | ✅ `Card` hint |
| Parallel workers for joins | ✅ `Parallel` hint | ❌ Not supported
| Storing and automatically re-using hint sets | ✅ Specific hint table | ❌ Not supported
| Custom cost estimates for joins | ❌ Not supported | ✅ Specific hints, e.g. `NestLoop(a b (COST start=4.2 total=42.42))`
| Custom cost estimates for base tables | ❌ Not supported | ✅ Specific hints, e.g. `SeqScan(a (COST start=4.2 total=42.42))`


## Installation

The `postgres-setup.sh` acts as the main installation tool. It pulls and compiles a new local installation of Postgres. No
global paths, etc. are modified. See `./postgres-setup.sh --help` for additional details.
Make sure to have the appropriate build tools available on your system. The setup has been tested for Ubuntu 22.04, but should
also work for other Linux distributions.


## Usage

Since pg_lab is both a fork of Postgres, as well as an extension, it can be used for two different use-cases:

1. enforcing optimizer decisions through an external component by means of hints embedded into the SQL query
2. modifying the optimizer behavior through a Postgres extension

### Hint syntax

All hints must be embedded in a comment that is submitted to the Postgres server as part of the SQL query. The comment _must_
have the following syntax in order to be recognized:

```
/*=pg_lab=
 <hint list>
*/
```
The usage of newlines is optional. Notice that the hint block detection is currently rather primitive: the extension only
performs a (context-free) substring search. Therefore, certain queries might trigger the extension unintentionally.

Within the hint block, three types of hints are supported:

- `JoinOrder(<tables>)` enforces the specified join order. Use parantheses to denote subtrees and always wrap the entire join
  order into another set of parantheses. E.g. `JoinOrder(((A B) C))` enforces the join A ⋈ B to be executed first and the
  resulting intermediate to be joined to C. Notice that this join is different from `JoinOrder((C (A B)))`: in the first join
  order, C acted as the inner relation, whereas C is the outer relation in the second example.
- `Card(<tables> #<rows>)` overwrites the cardinality estimate of the (intermediate) table(s) and sets it to the given number
  of rows. The rows must be an integer. E.g. `Card(A #42)` sets the estimate for scanning A (including all filters) to 42,
  whereas `Card(A B C #42)` does the same for the intermediate consisting of the join between A, B and C.
- `<operator>(<tables>)` forces the (intermediate) result consisting of the specified table(s) to be computed using the given
  operator. Notice that this does not force the computation of the intermediate. The optimizer is free to use a different join
  order instead - unless the `JoinOrder` hint is used as well.

  Supported operators are `SeqScan` and `IdxScan` for base relations, e.g. `SeqScan(A)` or `IdxScan(B)`, and `NestLoop`,
  `MergeJoin` or `HashJoin` for joins, e.g. `MergeJoin(A B)` or `NestLoop(A B C)`. The cardinality _must_ be an integer.

  You can also add an optional `(FORCED)` clause after the tables to contrast this hint from a cost hint (see next bullet
  point): `HashJoin(A B C (FORCED))`.
- `<operator>(<tables> (COST start=<float> total=<float>))` overwrites the native cost estimate for the given operator. The
  same operator and tables syntax as for the operator-forcing hint is used. Additionally, `BitmapScan` can be used to cost
  bitmap scans, in which case the cost corresponds to the actual heap operator.
  For example, `NestLoop(A B (COST start=4.2 total=42.0))`.

Tables can be referred to using either their full names, or aliases.

### Custom Postgres optimizer extensions

TODO

## Supported Postgres versions

Currently, pg_lab only supports Postgres v16.


## Modifications

pg_lab uses three fundamental patterns to change the Postgres source code:

1. Moving static methods into the public API. This enables the hinting system to use such methods to create appropriate access
   paths. For example, the function to add nested-loop join paths `try_nestloop_path` is no longer static and prototype
2. Introducing new extension hooks to control existing functionality. For example, the cost model has been refactored to
   support new estimation functions. These refactorings all follow the same general pattern:

   For an existing function, a new hook type with the same signature (return type and parameters) is introduced. For example,
   the function `void cost_seqscan(Path*, PlannerInfo*, RelOptInfo*, ParamPathInfo*)` received a `cost_seqscan_hook_type` as
   `void (*cost_seqscan_hook_type) (Path*, PlanenrInfo*, RelOptInfo, ParamPathInfo*)`. An instance of this hook is added,
   defaulting to a `NULL` pointer: `cost_seqscan_hook_type cost_seqscan_hook = NULL`. The default implementation of
   `cost_seqscan` is moved into a new function `standard_cost_seqscan`, whose prototype is publicly available. The new
   implementation of `cost_seqscan` now calls `cost_seqscan_hook` if it has been set, and delegates to `standard_cost_seqscan`
   otherwise.
3. Introducing new extension points without a corresponding standard implementation. This is only required for the hinting part
   of pg_lab in order to generate and attach the hint block data at the appropriate time. More specifically, a new hook
   `prepare_make_one_rel` has been added. This hook is called right before `make_one_rel` is executed and receives the current
   `PlannerInfo` and `joinlist` as input.


## Detailed API description

TODO


## Limitations

TODO
