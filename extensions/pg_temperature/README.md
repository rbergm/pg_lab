# pg_temperature

This extension provides mechanisms to automatically simulate hot and cold shared buffers when executing a query workload.

> [!INFO]
> pg_temperature is not compatible with vanilla Postgres and requires a pg_lab-based fork. This is because pg_temperature
> needs to access some very low-level details of the file (handle) abstractions used by Postgres. Specifically, we need to
> determine the file descriptors that open the raw relation files. This knowledge is hidden behind the virtual file descriptors
> which are only accessible to the internals of Postgres' fd module.

> [!INFO]
> The cold state is achieved by issuing `fadvise` calls which are only available on POSIX systems. Hence, this mode does
> not work meaningfully on Windows systems.

## Installation

Install the extension using the normal `CREATE EXTENSION` call and consider adding it to the _preload\_libraries_.

## Usage

pg_temperature provides a `pg_cooldown` UDF that works similar to the
[pg_prewarm](https://www.postgresql.org/docs/current/pgprewarm.html) extension, just for removing specific relations from the
shared buffer and the OS page cache.

In addition, you can set the `pg_temperature.experiment_mode` GUC parameter to automatically simulate a hot or cold start when
executing a query. The allowed values are:

. `off` (default) - This disabled pg_temperature
. `hot` - This provides a perfectly warmed-up cache for the query. This means that all relations that need to be accessed by
  the query are already contained in the shared buffer.
. `cold` - This runs the query on a completely empty shared buffer with respect to the required relations. In addition, the OS
  page cache will also not contain any of the relation's raw data.

> [!WARNING]
> The hot mode assumes that the shared buffer is actually large enough to accomodate all data of the required relations. If
> this is not the case, some data will be thrown out again. Which data is affected is an implementation detail.

> [!WARNING]
> If you use the experiment modes, all required setup is performed at the end of the query optimization.
> This means, that you need to make sure to only measure the actual execution time (e.g. as reported by `EXPLAIN ANALYZE`) of
> your query. This is especially important for the hot start, which might perform a lot of expensive I/O operations to load
> the relation data.

## Limitations

Support for "advanced" SQL features like CTEs, subqueries or set operations is not thoroughly tested but should work in
principle.
