
# pg_lab Hinting

pg_lab ships with a hinting extension that is heavily inspired by [pg_hint_plan](https://github.com/ossc-db/pg_hint_plan).
Hints are embedded in a SQL comment and encode special optimizer behavior like so:

```sql
/*=pg_lab=
    HashJoin(t mi)
 */
SELECT *
FROM title t
JOIN movie_info mi ON t.id = mi.movie_id
WHERE t.production_year >= 2005
```

All contents betwen `/*=pg_lab=` (the hint prefix) and `*/` (the hint suffix) are parsed as potential hint blocks.
The hint block can be placed at an arbitary point in the query (e.g. after the `SELECT`).
Please note, that the hint detection is currently rather primitive and pg_lab only performs a (context-free) substring
search for `/*=pg_lab=` and `*/` to find the hint block.
This is because comments are stripped from the query AST and we simply did not have the resources nor the knowledge to
modify the Postgres parser. If this primitive strategy does not work for you, we welcome any PRs to improve the detection
logic.

> [!WARNING]
> You cannot use pg_lab and pg_hint_plan side-by-side. This is because pg_lab modifies some parts of the Postgres source
> code and pg_hint_plan works based on a diff from the upstream source.

When you install pg_lab through the `postgres-setup.sh` script, the hinting extension is automatically build, enabled and
added to the _preload libraries_.
If you decide to build pg_lab on your own, make sure to also setup the hinting extension (see `extensions/pg_lab` for
details).

When pg_lab detects a hint block, it will check whether the final execution plan is compatible with all hints.
If this should not be the case (due to the [Limitations](#hint-enforcement)), an error will be raised.
This behavior can be disabled by setting `pg_lab.check_final_path` to _off_.


## Hint List

| Hint | Description | Example |
|------|-------------|---------|
| `Config` | Sets options that confiure the entire optimization process | `Config(plan_mode=full)` |
| `Card` | Overwrites the native cardinality estimate for a specific intermediate. | `Card(t mi ci #42000)` |
| Physical operators, e.g., `HashJoin` | Control the access paths for specific intermediates | `IdxScan(t)`, `MergeJoin(t mi)` |
| `JoinOrder` | Sets the join tree for the query | `JoinOrder(((t mi) ci))` |

Hints are case-insensitive and you can even change casing within a hint (e.g., `CONFIG(plan_mode=FULL)`). Whether you
should do this is of course another question.

To identify tables in the hints, they have to be referenced as specifically as possible, i.e. if the tables have an alias
in the SQL query, the alias has to be used. Otherwise, the full name is required:

```sql
/*=pg_lab= SeqScan(t) IdxScan(movie_info) NestLoop(t movie_info) */
SELECT min(t.production_year)
FROM title t
JOIN movie_info
ON t.id = movie_id
```

Here, the _title_ table is referenced via its alias, while *movie_info* requires the full table name.

To identify a specific intermediate, simply list the tables that form the intermediate:
`Card(t mi ci #42000)` tells the optimizer that the relation that is constructed by joining `title t` with `movie_info mi`
and `cast_info ci` has a cardinality of 42000. Notice that for the join order, you need
[additional parentheses](#join-order).

### Configuration hint

The `Config` hint can be used to change high-level details about the way hints are used and about the kind of plans that
are generated.

The syntax of this hint is:

```
Config(<setting>+)

  setting ::= <plan_mode> | <exec_mode>
plan_mode ::= plan_mode = anchored | full
exec_mode ::= exec_mode = default | sequential | parallel
```

Each setting follows the structure of `setting name = setting value` and multiple settings are separated by semicolons.

The **planner mode** changes how intermediate hints (and their absence) should impact the selected plans.
Currently, this affects materialization and memoization operators in two different modes:

- `anchored`: In _anchored_ mode, pg_lab only enforces hints that it sees, but the optimizer is free to "fill the gaps"
  as it sees fit. For example, the optimizer can insert additional memoization operators before a nested-loop join.
  Notice, that this does not influence the behaviour of explicit `Memoize` or `Material` hints. If those are present,
  pg_lab will always enforce them (but see [Limitations](#hint-enforcement)).
- `full`: In _full_ mode, the absence of an intermediate hint is treated as disabling the hint.
  E.g., if the user did not explicitly specify materialization for an intermediate, this intermediate will not be
  materialized in the selected plan.

Compare the following queries:

```
imdb=# EXPLAIN /*=pg_lab= Config(plan_mode=anchored) NestLoop(t mi) SeqScan(mi) */
imdb-# SELECT * FROM title t JOIN movie_info mi ON t.id = mi.movie_id WHERE t.production_year < 1930;

                                          QUERY PLAN
-----------------------------------------------------------------------------------------------
 Gather  (cost=1000.44..5234008.65 rows=703856 width=168)
   Workers Planned: 2
   ->  Nested Loop  (cost=0.44..5162623.05 rows=293273 width=168)
         ->  Parallel Seq Scan on movie_info mi  (cost=0.00..379762.06 rows=10420006 width=74)
         ->  Memoize  (cost=0.44..0.48 rows=1 width=94)
               Cache Key: mi.movie_id
               Cache Mode: logical
               ->  Index Scan using title_pkey on title t  (cost=0.43..0.47 rows=1 width=94)
                     Index Cond: (id = mi.movie_id)
                     Filter: (production_year < 1930)
```

Here, the optimizer decided to insert an additional memoize-operator after scanning *movie_info*.
In contrast,

```
imdb=# EXPLAIN /*=pg_lab= Config(plan_mode=full) NestLoop(t mi) SeqScan(mi) */
imdb-# SELECT * FROM title t JOIN movie_info mi ON t.id = mi.movie_info WHERE t.production_year < 1930;

                                          QUERY PLAN
-----------------------------------------------------------------------------------------------
 Gather  (cost=1000.43..5308186.98 rows=703856 width=168)
   Workers Planned: 2
   ->  Nested Loop  (cost=0.43..5236801.38 rows=293273 width=168)
         ->  Parallel Seq Scan on movie_info mi  (cost=0.00..379762.06 rows=10420006 width=74)
         ->  Index Scan using title_pkey on title t  (cost=0.43..0.47 rows=1 width=94)
               Index Cond: (id = mi.movie_id)
               Filter: (production_year < 1930)
```

does not use the memo node. This is because the absence of the `Memo` hint told the optimizer to not use the operator.

The *exec_mode* setting controls the creation of parallel plans:

- `default` leaves this decision to the PG optimizer. It can generate a parallel or a sequential plan
- `sequential` disables parallel plans
- `parallel` forces the creation of parallel plans

For example, consider

```
imdb=# EXPLAIN /*=pg_lab= Config(exec_mode=sequential) */
imdb-# SELECT count(*) FROM title t JOIN movie_info mi ON t.id = mi.movie_id;

                                                  QUERY PLAN
--------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=1031737.16..1031737.17 rows=1 width=8)
   ->  Merge Join  (cost=21.67..969217.12 rows=25008014 width=0)
         Merge Cond: (t.id = mi.movie_id)
         ->  Index Only Scan using title_pkey on title t  (cost=0.43..140304.06 rows=4737042 width=4)
         ->  Index Only Scan using mi_movie_id on movie_info mi  (cost=0.44..504660.65 rows=25008014 width=4)
```

compared to

```
imdb=# EXPLAIN /*=pg_lab= Config(exec_mode=parallel) */
imdb-# SELECT count(*) FROM title t JOIN movie_info mi ON t.id = mi.movie_id;

                                                            QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=622303.11..622303.12 rows=1 width=8)
   ->  Gather  (cost=622302.89..622303.10 rows=2 width=8)
         Workers Planned: 2
         ->  Partial Aggregate  (cost=621302.89..621302.90 rows=1 width=8)
               ->  Parallel Hash Join  (cost=120001.21..595252.88 rows=10420006 width=0)
                     Hash Cond: (mi.movie_id = t.id)
                     ->  Parallel Index Only Scan using mi_movie_id on movie_info mi  (cost=0.44..358780.57 rows=10420006 width=4)
                     ->  Parallel Hash  (cost=87617.68..87617.68 rows=1973768 width=4)
                           ->  Parallel Seq Scan on title t  (cost=0.00..87617.68 rows=1973768 width=4)
```

In the first example, the best sequential plan is selected, whereas the second example uses the cheapest parallel plan
(which in this case is estimated to be much better than the sequential one).

> [!NOTE]
> `parallel` does not control which part of the query plan is being parallelized.
> This has to be customized via [operator-level hints](#operator-level-hints).
> Using operator-level hints implies parallel execution and overwrites the *exec_mode*.

### Cardinality

The `Card` hint can be used to overwrite the PG native cardinality estimator and to use custom values instead.

The syntax of this hint is:

```
Card(<intermediate> #<rows>)

intermediate ::= <intermediate> <intermediate>
               | <base table>
        rows ::= integer
```

In contrast to [pg_hint_plan](https://github.com/ossc-db/pg_hint_plan), pg_lab supports cardinality hints on both join
relations as well as base tables. Therefore, `Card(t #42)` is completely valid hint in pg_lab.

The cardinality is interpreted as the size of the specific intermediate after all applicable filters have been applied.

```
imdb=# EXPLAIN /*=pg_lab= Card(t #4200000) */ SELECT * FROM title t WHERE t.production_year > 2010;

                                   QUERY PLAN
---------------------------------------------------------------------------------
 Seq Scan on title t  (cost=0.00..127093.02 rows=4200000 width=94)
   Filter: (production_year > 2010)

imdb=# EXPLAIN /*=pg_lab= Card(t #42) */ SELECT * FROM title t WHERE t.production_year > 2010;

                                 QUERY PLAN
----------------------------------------------------------------------------
 Gather  (cost=1000.00..93556.29 rows=42 width=94)
   Workers Planned: 2
   ->  Parallel Seq Scan on title t  (cost=0.00..92552.09 rows=18 width=94)
         Filter: (production_year > 2010)
```

### Operator-level hints

Operator hints influence the selection of physical operators, mostly for scans and joins.
For each supported operator, a different hint exists, similar to pg_hint_plan.
They all follow the same basic syntax:

```
<operator name> ( <intermediate> <options>* )

intermediate ::= <intermediate> <intermediate>
               | <base table>

options ::= <forced> | <costs> | <parallelization>

forced ::= (forced)

costs ::= (cost start=<cost> total=<cost>)
 cost ::= float | int

parallelization ::= (workers=int)
```

#### Description

Currently, pg_lab supports the following operators:

| Operator | Description |
|----------|-------------|
| `SeqScan` | Sequential scan |
| `IdxScan` | Index scan. The optimizer determines whether an index-only is possible. If it is, there really is no reason why a plain index scan should be used. |
| `BitmapScan` | A bitmap scan first performs (one or multiple) index lookups, but does not fetch matching tuples right away. Instead, all matches are collected in a bitmap. This bitmap is then used to scan the relation sequentially. The optimizer can decide which indexes to use in the lookup phase and how they should be combined. |
| `NestLoop` | Nested-loop join. Unless the [join order](#join-order) is also forced, the optimizer is free to decide which relation should be on the outer loop. |
| `MergeJoin` |  (Sort-) Merge join. The optimizer determines whether any of the input relations require an explicit sort operation (and what the appropriate sort key is). Unless the [join order](#join-order) is also forced, the optimizer is free to decide which relation should be on the outer loop. |
| `HashJoin` |  Hash join. Unless the [join order](#join-order) is also forced, the optimizer is free to decide which relation should be on the outer loop. This is especially important for hash joins, because the inner relation becomes the build side. The outer relation is the probe side. |
| `Memo` | Insert a memoize operator on top of the specified relation. For example, `SeqScan(t) Memo(t)` indicates that _t_ should be scanned sequentially and its result should be memoized. Memoization essentially uses a cache to prevent repeated lookups of the same key values in a join. The optimizer is free to decide which key to use for the lookup (but see [Caveats](#hint-enforcement) and the interaction with the [planner mode](#configuration)). |
| `Material` | Insert a materialization operator on top of the specified relation. For example, `IdxScan(mi) Material(mi)` indicates that all matching tuples from _mi_ should be collected first and stored in a materialized relation. When using this hint, see [Caveats](#hint-enforcement) and the interaction with the [planner mode](#configuration). |
| `Result` | Catch all "operator" that applies to all operators after the final join, such as aggregation or sorting. See [below](#result-operator) for its usage. |

The most basic usage of these hints is to force the optimizer to use a specific operator when computing an intermediate.
For example, `NestLoop(t mi)` tells the optimizer that the join between _t_ and _mi_ has to executed using a nested-loop
join.
However, this behavior can be modified epending on the given options:

`forced` enforces the usage of the operator when computing the specified intermediate (but see
[Limitations](#hint-enforcement) for situations when this might not work).
_forced_ is the default assumed option. Therefore, the following two hints are equivalent: `NestLoop(t mi)` and
`NestLoop(t mi (FORCED))`

`workers` enforces that when computing this operator, the specified number of parallel workers has to be used.
This requires Postgres to actually support a parallel computation of the operator.
Otherwise, no valid plan will be found.
Notice that the backend process also takes part in the result computation, the workers are additional processes.
For example, `HashJoin(t mi (workers=5))` would compute the hash join with 6 processes in total.
_workers_ can also be specified on scans: `IdxScan(t (workers=1))` would use a parallel index scan with two processes.

`cost` does not enforce a specific operator. Rather, it overwrites the cost estimates for the operator.
The optimizer is still free to select a different operator if it appears cheaper.
Notice that the cost hints cannot be conditioned by specific access paths.
For example, it is currently not possible to enforce different costs depending on whether a merge join receives pre-sorted
input or not.
Therefore, one should fix the plan as far as possible by specifying the [join order](#join-order) as well as operators for
the input nodes.
The cost hint cannot currently be combined with the `workers` hint.
Notice that Postgres differentiates between startup costs (i.e. costs that have to be paid before the first tuple can be
produced, such as building a hash table) and total costs (i.e. the cost to compute the entire result), e.g.,
`MergeJoin(t ci (cost start=42 total=4224))`

#### Result operator

The `Result` pseudo-operator only supports the _workers_ option. Its main use is to be able to indicate that the entire
plan should be parallelized, e.g., including any aggregations, rather than the final join.

To make this concept more clear, consider the following query based on the IMDB schema:
*SELECT count(\*) FROM title t JOIN movie_info mi ON t.id = mi.movie_id*
Without the result hint, it would not be possible to enforce the parallel execution of the entire plan. This is because
any operator hint like `MergeJoin(t mi (workers=12))` would indicate that the merge join should be executed in parallel:

```
imdb=# EXPLAIN /*=pg_lab= MergeJoin(t mi (workers=12)) */
imdb-# SELECT count(*) FROM title t JOIN movie_info mi ON t.id = mi.movie_id

                                                         QUERY PLAN
----------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=3659465.64..3659465.65 rows=1 width=8)
   ->  Gather  (cost=771028.77..3596945.60 rows=25008014 width=0)
         Workers Planned: 12
         ->  Merge Join  (cost=770028.77..1095144.20 rows=2084001 width=0)
               Merge Cond: (mi.movie_id = t.id)
               ->  Parallel Index Only Scan using mi_movie_id on movie_info mi  (cost=0.44..275420.52 rows=2084001 width=4)
               ->  Sort  (cost=770018.10..781860.70 rows=4737042 width=4)
                     Sort Key: t.id
                     ->  Seq Scan on title t  (cost=0.00..115250.42 rows=4737042 width=4)
```

This forces Postgres to introduce a gather node after the join to collect the results from all workers.
Afterwards, the aggregation is executed sequentially.

In contrast, `Result` applies to any operations that happen _after_ the last join has been computed (remember that Postgres
uses a rather simple execution model of "all joins first, then all grouping, aggregation, etc."). This way, the
aggregation can be included in the parallel portion:

```
imdb=# EXPLAIN /*=pg_lab= Result(workers=12) MergeJoin(t mi) */
imdb-# SELECT count(*) FROM title t JOIN movie_info mi ON t.id = mi.movie_id

                                                            QUERY PLAN
----------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=1101355.45..1101355.46 rows=1 width=8)
   ->  Gather  (cost=1101354.21..1101355.42 rows=12 width=8)
         Workers Planned: 12
         ->  Partial Aggregate  (cost=1100354.21..1100354.22 rows=1 width=8)
               ->  Merge Join  (cost=770028.77..1095144.20 rows=2084001 width=0)
                     Merge Cond: (mi.movie_id = t.id)
                     ->  Parallel Index Only Scan using mi_movie_id on movie_info mi  (cost=0.44..275420.52 rows=2084001 width=4)
                     ->  Sort  (cost=770018.10..781860.70 rows=4737042 width=4)
                           Sort Key: t.id
                           ->  Seq Scan on title t  (cost=0.00..115250.42 rows=4737042 width=4)
```

Notice that it is currently not possible to specify which part of the join post-processing is parallelized.
For example, if Postgres needs to perform both an aggregation as well as a duplicate elimination, it is free to parallelize
any portion of this plan.
Fixing this shortcoming would basically require the possibility to hint entire query plans.
This is something that we are working on, but it is way more complicated than it might seem.


### Join order

The join tree of a query can be set using the `JoinOrder` hint:

```
JoinOrder( <intermediate> | <base table> )

intermediate ::= ( <intermediate> <intermediate> )
               | <base table>
```

This enforces the intermediates that should be computed to answer the query, as well as the sequence in which this
computation should take place (and even [a little more](#join-order-shenanigans)).
The latter is due to the fact that Postgres uses a left-deep execution model.
However, the hint does not restrict the operators that should be used to compute the intermediates.
This is done using [operator-level hints](#operator-level-hints).

> [!NOTE]
> In contrast to the operator-level hints, intermediates for the join order hint always require braces.
> This is necessary to encode the correct join hierarchy.

Using appropriate nesting, the join order hint can be used to enforce bushy plans.
Compare

```
imdb=# EXPLAIN /*=pg_lab= JoinOrder((ci (t mi))) */  -- bushy plan
imdb-# SELECT count(*)
imdb-# FROM title t JOIN movie_info mi ON t.id = mi.movie_id JOIN cast_info ci ON t.id = ci.movie_id
imdb-# WHERE t.production_year > 2000;

                                                              QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=2772595.00..2772595.01 rows=1 width=8)
   ->  Gather  (cost=2772594.79..2772595.00 rows=2 width=8)
         Workers Planned: 2
         ->  Partial Aggregate  (cost=2771594.79..2771594.80 rows=1 width=8)
               ->  Parallel Hash Join  (cost=937091.30..2548659.56 rows=89174092 width=0)
                     Hash Cond: (ci.movie_id = t.id)
                     ->  Parallel Seq Scan on cast_info ci  (cost=0.00..709675.83 rows=26457483 width=4)
                     ->  Parallel Hash  (cost=827947.80..827947.80 rows=6652520 width=8)
                           ->  Merge Join  (cost=21.67..827947.80 rows=6652520 width=8)
                                 Merge Cond: (t.id = mi.movie_id)
                                 ->  Parallel Index Scan using title_pkey on title t  (cost=0.43..191334.98 rows=1260127 width=4)
                                       Filter: (production_year > 2000)
                                 ->  Index Only Scan using mi_movie_id on movie_info mi  (cost=0.44..504660.65 rows=25008014 width=4)
```

to

```
imdb=# EXPLAIN /*=pg_lab= JoinOrder(((t mi) ci)) */ -- normal left-deep plan
imdb-# SELECT count(*)
imdb-# FROM title t JOIN movie_info mi ON t.id = mi.movie_id JOIN cast_info ci ON t.id = ci.movie_id
imdb-# WHERE t.production_year > 2000;

                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=2955348.55..2955348.56 rows=1 width=8)
   ->  Gather  (cost=2955348.34..2955348.55 rows=2 width=8)
         Workers Planned: 2
         ->  Partial Aggregate  (cost=2954348.34..2954348.35 rows=1 width=8)
               ->  Parallel Hash Join  (cost=1143766.04..2731413.11 rows=89174092 width=0)
                     Hash Cond: (t.id = ci.movie_id)
                     ->  Merge Join  (cost=21.67..827947.80 rows=6652520 width=8)
                           Merge Cond: (t.id = mi.movie_id)
                           ->  Parallel Index Scan using title_pkey on title t  (cost=0.43..191334.98 rows=1260127 width=4)
                                 Filter: (production_year > 2000)
                           ->  Index Only Scan using mi_movie_id on movie_info mi  (cost=0.44..504660.65 rows=25008014 width=4)
                     ->  Parallel Hash  (cost=709675.83..709675.83 rows=26457483 width=4)
                           ->  Parallel Seq Scan on cast_info ci  (cost=0.00..709675.83 rows=26457483 width=4)
```

## Parallel plans

pg_lab provides two main ways to control the creation of parallel plans: Parallelization can be set "globally" via the
`Config` variable.
This only allows or disables the generation of _any_ parallel plan, but does not control which portions of the plan would
be parallelized.

Alternatively, hints for physical operators also support a `workers` setting that specifies that the specific operator must
be computed in parallel using the designated number of workers (in addition to the main backend process).
See [Limitations](#hint-enforcement) on how to enforce this.

This strategy has one major drawback: parallelization is always attached to specific join or scan operators.
In reality, aggregations should also be parallelized, but we current don't hint anything that happens after all joins are computed.
To mitigate this situation (at least to some extend), the pseudo-operator `Result` exists.
It applies to all nodes that consume the final join result (such as aggregations).
By setting the parallel workers on `Result`, Postgres is forced to execute those nodes in parallel.
Notice however, that it is currently not possible, to indicate which part of the post-processing is parallelized.
For example, Postgres could parallelize the final aggregation as well as the final sorting, if both are requested.


## Limitations

While using a Postgres fork allows us to achieve many things that would otherwise be impossible, the overall Postgres
architecture or our implementation decisions impose some limits on what we can do.
These limitations are outlined below.

### Supported planner features

pg_lab is currently only tested on SPJ-ish queries that do not contain subqueries, CTEs or other features that generate
relations from non-base table sources (e.g. views or table-returning UDFs).
Some queries with such features might work by accident, but it might just as well crash and burn (and probably will).

### Hint enforcement

The current strategy to enforce hints works by intercepting Postgres' `add_path()` function.
This function is responsible (among other things) to store promising access paths for optimizer.
When the Postgres optimizer calls this function, pg_lab checks, whether the current path matches the constraints that are
specified in the hint block.
If the constraints are satisfied, Postgres continues with the normal path adding logic.
If any constraint is violated, the path is discarded.

While we think that this is a very elegant solution that allows for a clear implementation, there is one central downside:
if the hint block demands the usage of a specific physical operator, but Postgres never considers the correponding
intermediate, we cannot enforce the operator.
On the other, intercepting `add_path()` allows to retain the normal flow of the optimizer. So for now, we are fine with
this decision.

> [!NOTE]
> Other solutions, such as directly constructing compatible paths, are sadly incredibly challenging from a technical point
> of view and make the entire extension much more fragile.
> This becomes especially apparent when considering partial hint blocks, where the optimizer is only constrained for part
> of the query plan and should operate as usual for the remainder.

The problem with intercepting `add_path()` mostly appears in conjunction with hints for materialization or memoization, as
in the following example:

```
imdb=# EXPLAIN
imdb-# /*=pg_lab=
         JoinOrder((t mi))
         NestLoop(t mi)
         SeqScan(t)
         Memoize(mi)
         IdxScan(mi)
        */
imdb-# SELECT * FROM title t JOIN movie_info mi ON t.id = mi.movie_id;

                                          QUERY PLAN
-----------------------------------------------------------------------------------------------
 Gather  (cost=1000.44..6788675.95 rows=25008014 width=168)
   Workers Planned: 2
   ->  Nested Loop  (cost=0.44..4286874.55 rows=10420006 width=168)
         ->  Parallel Seq Scan on title t  (cost=0.00..87617.68 rows=1973768 width=94)
         ->  Index Scan using mi_movie_id on movie_info mi  (cost=0.44..1.61 rows=52 width=74)
               Index Cond: (movie_id = t.id)
```

Postgres does not implement the dynamic programming-based plan search in the puristic textbook sense.
This means, that it will not try to generate a memoize path on top of each intermediate.
Instead, the operator is only considered if Postgres finds a suitable parameterization for the index scan on *movie_info*
(among other conditions, as encoded in `get_memoize_path()`).
If this is not the case, Postgres will never try to add a memoize path and pg_lab's custom `add_path()` logic is never
called.
Therefore, the resulting plan will not match the hints.

> [!TIP]
> The [Hinting Mode](#configuration) setting can mitigate these issues somewhat, but not completely.

To a lesser extent, the same issue appears when using the other operator hints: if Postgres selects a different join order
and the intermediate is not used, the operator cannot be enforced:

```
imdb=# EXPLAIN /*=pg_lab= JoinOrder(((mi ci) t)) NestLoop(mi t) */
imdb-# SELECT * FROM title t
imdb-# JOIN movie_info mi ON t.id = mi.movie_id
imdb-# JOIN cast_info ci ON t.id = ci.movie_id;

                                            QUERY PLAN
--------------------------------------------------------------------------------------------------
 Merge Join  (cost=22102162.86..45638824.47 rows=335221405 width=210)
   Merge Cond: (mi.movie_id = t.id)
   ->  Merge Join  (cost=22101614.92..34946020.83 rows=837376828 width=116)
         Merge Cond: (mi.movie_id = ci.movie_id)
         ->  Sort  (cost=6932205.29..6994725.33 rows=25008014 width=74)
               Sort Key: mi.movie_id
               ->  Seq Scan on movie_info mi  (cost=0.00..525642.14 rows=25008014 width=74)
         ->  Materialize  (cost=15169405.91..15486895.71 rows=63497960 width=42)
               ->  Sort  (cost=15169405.91..15328150.81 rows=63497960 width=42)
                     Sort Key: ci.movie_id
                     ->  Seq Scan on cast_info ci  (cost=0.00..1080080.60 rows=63497960 width=42)
   ->  Index Scan using title_pkey on title t  (cost=0.43..214033.31 rows=4737042 width=94)
```

In this example, the join between _mi_ and _t_ is never performed. Hence, the nested-loop hint is ignored.

Likewise, the following does not work because Postgres never considers index paths for the following query:

```
imdb=# EXPLAIN /*=pg_lab= IdxScan(t) */ SELECT * FROM title;

                            QUERY PLAN
-------------------------------------------------------------------
 Seq Scan on title t  (cost=0.00..115250.42 rows=4737042 width=94)
```

### Join Order shenanigans

When using the `JoinOrder` hint, this hint not only enforces the join order, but also the join direction: a hint
`JoinOrder((R S))` enforces that while joining _R_ and _S_, _R_ will act as the outer relation and _S_ will be the inner
relation.
As a practical consequence, _R_ will become the probe side in a hash join whereas _S_ will be materialized in a hash table.

It is currently not possible to only specify the join order, but let Postgres assign its own join direction.
To make this a bit more obvious, we always require an additional set of paratheses around the outermost join, i.e.
`JoinOrder((R S))` instead of `JoinOrder(R S)`.

### Plan equivalence

Optimizer hints are a fairly abstract technique to influence the query plan. As a consequence, using hints to exactly
reproduce a plan might not always work. This is because the optimizer still retains its freedom regarding lower-level details
of the query plan. For example, consider query _q-10_ from the Stats benchmark. Natively, Postgres might produce a plan like
the following:

```
stats=# EXPLAIN
SELECT COUNT(*)
FROM comments AS c, posts AS p, users AS u
WHERE c.UserId = u.Id
  AND u.Id = p.OwnerUserId
  AND c.CreationDate >= CAST('2010-08-05 00:36:02' AS timestamp)
  AND c.CreationDate <= CAST('2014-09-08 16:50:49' AS timestamp)
  AND p.ViewCount >= 0
  AND p.ViewCount <= 2897
  AND p.CommentCount >= 0
  AND p.CommentCount <= 16
  AND p.FavoriteCount >= 0
  AND p.FavoriteCount <= 10;
                                                                                       QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=20417.33..20417.34 rows=1 width=8)
   ->  Gather  (cost=20417.12..20417.33 rows=2 width=8)
         Workers Planned: 2
         ->  Partial Aggregate  (cost=19417.12..19417.13 rows=1 width=8)
               ->  Nested Loop  (cost=1196.96..15910.81 rows=1402522 width=0)
                     ->  Parallel Hash Join  (cost=1196.66..8825.79 rows=71505 width=8)
                           Hash Cond: (c.userid = u.id)
                           ->  Parallel Seq Scan on comments c  (cost=0.00..7441.41 rows=71505 width=4)
                                 Filter: ((creationdate >= '2010-08-05 00:36:02'::timestamp without time zone) AND (creationdate <= '2014-09-08 16:50:49'::timestamp without time zone))
                           ->  Parallel Hash  (cost=900.15..900.15 rows=23721 width=4)
                                 ->  Parallel Index Only Scan using users_pkey on users u  (cost=0.29..900.15 rows=23721 width=4)
                     ->  Memoize  (cost=0.30..0.80 rows=1 width=4)
                           Cache Key: c.userid
                           Cache Mode: logical
                           ->  Index Scan using posts_owneruserid_fkey on posts p  (cost=0.29..0.79 rows=1 width=4)
                                 Index Cond: (owneruserid = c.userid)
                                 Filter: ((viewcount >= 0) AND (viewcount <= 2897) AND (commentcount >= 0) AND (commentcount <= 16) AND (favoritecount >= 0) AND (favoritecount <= 10))
 Optimizer: planner=Custom Hook joinorder=Dynamic Programming
(18 rows)
```

Using hints for all of the major optimizer decisions results in a plan like the following:

```
stats=# EXPLAIN
/*=pg_lab= Result(workers=2) NestLoop(c p u) HashJoin(c u) SeqScan(c) IdxScan(u) IdxScan(p) Memo(p) */
SELECT COUNT(*)
FROM comments AS c, posts AS p, users AS u
WHERE c.UserId = u.Id
  AND u.Id = p.OwnerUserId
  AND c.CreationDate >= CAST('2010-08-05 00:36:02' AS timestamp)
  AND c.CreationDate <= CAST('2014-09-08 16:50:49' AS timestamp)
  AND p.ViewCount >= 0
  AND p.ViewCount <= 2897
  AND p.CommentCount >= 0
  AND p.CommentCount <= 16
  AND p.FavoriteCount >= 0
  AND p.FavoriteCount <= 10;
                                                                                       QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=20261.66..20261.67 rows=1 width=8)
   ->  Gather  (cost=20261.45..20261.66 rows=2 width=8)
         Workers Planned: 2
         ->  Partial Aggregate  (cost=19261.45..19261.46 rows=1 width=8)
               ->  Nested Loop  (cost=1041.29..15755.14 rows=1402522 width=0)
                     ->  Parallel Hash Join  (cost=1040.99..8670.11 rows=71505 width=8)
                           Hash Cond: (c.userid = u.id)
                           ->  Parallel Seq Scan on comments c  (cost=0.00..7441.41 rows=71505 width=4)
                                 Filter: ((creationdate >= '2010-08-05 00:36:02'::timestamp without time zone) AND (creationdate <= '2014-09-08 16:50:49'::timestamp without time zone))
                           ->  Parallel Hash  (cost=830.96..830.96 rows=16802 width=4)
                                 ->  Parallel Index Only Scan using users_pkey on users u  (cost=0.29..830.96 rows=16802 width=4)
                     ->  Memoize  (cost=0.30..0.80 rows=1 width=4)
                           Cache Key: c.userid
                           Cache Mode: logical
                           ->  Index Scan using posts_owneruserid_fkey on posts p  (cost=0.29..0.79 rows=1 width=4)
                                 Index Cond: (owneruserid = c.userid)
                                 Filter: ((viewcount >= 0) AND (viewcount <= 2897) AND (commentcount >= 0) AND (commentcount <= 16) AND (favoritecount >= 0) AND (favoritecount <= 10))
 Optimizer: planner=Custom Hook joinorder=Dynamic Programming
(18 rows)
```

While the plans are exactly the same, their cost estimates differ slightly. This is because the cardinality estimate of the
index only scan on _users_ is lower for the hinted plan. Likewise, the planner could decide to perform lookups on a different
index of the same relation or to use a different column as cache in a memoize node. The underlying cause of these issues is
(most likely) that Postgres does not use costs as the only criteria to select access paths, which causes some of the
optimization process to be order-dependent, i.e. if some candidate path already exists when adding a new path, the behavior
might be different than if the first path was not stored. We do not intend to fix these issues since they would require a much
more low-level hinting interface, defeating the motivation behind hints as a high-level tool.

### Hint parsing

The parser is currently implemented as an ANTLR grammar, but there is no error listener yet.
This means that unknown hints or hints that do not match the expected syntax are silently ignored.

For example, the following hint block will simply not do anything (the correct hint would have been `SeqScan`):

```
imdb=# EXPLAIN /*=pg_lab= SequentialScan(t) */ SELECT * FROM title t WHERE t.id < 42;

                                 QUERY PLAN
----------------------------------------------------------------------------
 Index Scan using title_pkey on title t  (cost=0.43..9.29 rows=41 width=94)
   Index Cond: (id < 42)
```

We plan to improve the error behavior at some point, but for now the user is responsible for checking whether all features
are used as intended.
