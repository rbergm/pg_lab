# $C_{out}^*$ cost model

This extension provides an alternative cost model for Postgres. It is adapted and extended from the $C_{out}$ and $C_{mm}$ cost models. $C_{mm}$ was published by Leis et al. in their paper "How Good Are Query Optimizers, Really?" (VLDB'2015).

We expand this cost model to support a much broader range of operators in this extension. These expansions also required
some changes to the way the join operators are evaluated, to ensure that other join operators have an actual chance of
being selected as well.

## Usage

The cost model can be used by simply issuing a `LOAD 'coutstar';` statement once the extension has been build and installed
using the usual `make && make install` pipeline.

```sql
-- Enable the simplified cost model. Disabling returns to the PG vanilla cost functions
SET enable_cout TO on;

-- There are 3 configuration parameters available. We demonstrate them here along with their default values
SET cout_scan_cost TO 1.0;  -- this quantifies how expensive sequential I/O is (as used by Leis et al.)
SET cout_ind_cost  TO 2.0;  -- this quantifies how expensive random I/O is (as used by Leis et al.)
SET cout_proc_cost TO 1.2;  -- this quantifies how expensive arbitrary CPU operations are (e.g. hash functions)
```

## Detailed description

| Operator | Cost function | Comment |
|----------|---------------|---------|
|Sequential Scan | $C = \tau * \|R\|$ | $\tau$ can be configured via `cout_scan_cost` |
|Index Scan | $C = \lambda * log \|R\| + sel_p * \|R\|$ | $\lambda$ can be configured via `cout_ind_cost`. $sel_p$
| ... | TODO | |
