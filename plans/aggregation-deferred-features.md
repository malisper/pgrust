# Aggregation — Deferred Features

This note records what is intentionally missing from the current aggregation
implementation.

The current code is enough to exercise:

- `COUNT(*)`, `COUNT(expr)`, `SUM(expr)`, `AVG(expr)`, `MIN(expr)`, `MAX(expr)`
- `GROUP BY` with one or more key expressions
- `HAVING` with aggregate predicates
- Correct NULL handling (COUNT skips NULLs, SUM/AVG/MIN/MAX return NULL for
  all-NULL inputs, COUNT(*) on empty table returns 0)
- Grouping validation (non-aggregated columns must appear in GROUP BY)
- `ORDER BY` and `LIMIT`/`OFFSET` work above aggregate results

It is not a realistic implementation of PostgreSQL aggregation yet.

## DISTINCT inside aggregates

`COUNT(DISTINCT col)`, `SUM(DISTINCT col)`, etc. are not supported. The grammar
does not accept `DISTINCT` inside aggregate function calls. Adding it would
require tracking a set of seen values per accumulator.

## FILTER clause

`COUNT(*) FILTER (WHERE condition)` is not supported. This would require per-row
predicate evaluation inside the accumulator loop.

## Window functions / OVER clauses

`ROW_NUMBER() OVER (...)`, `SUM(col) OVER (PARTITION BY ...)`, etc. are entirely
missing. Window functions are a separate execution model from grouped aggregation
and would need their own plan node.

## GROUPING SETS / ROLLUP / CUBE

Only simple `GROUP BY col, col, ...` is supported. `GROUPING SETS`, `ROLLUP`,
and `CUBE` would require generating multiple grouping passes or a single pass
with multiple accumulator sets.

## Non-numeric aggregates

`STRING_AGG`, `ARRAY_AGG`, `JSON_AGG`, `BOOL_AND`, `BOOL_OR`, and other
non-numeric aggregate functions are not implemented. The current accumulator
model is built around integer arithmetic and min/max comparison.

## User-defined aggregate functions

There is no extensibility mechanism for user-defined aggregates. All five
aggregate functions are hard-coded in the grammar, parser, and executor.

## Hash-based vs sort-based grouping strategy

The current implementation uses a simple linear scan to find matching groups
(`Vec::position`). PostgreSQL chooses between hash aggregation and sort-based
grouping based on cost estimates. The current approach is O(n*g) where g is
the number of groups, which is fine for small datasets but would need a hash
map for production use.

## Parallel aggregation

There is no support for partial aggregation, parallel workers, or finalize
aggregation. The entire aggregation runs single-threaded.

## Aggregate pushdown

Aggregates are always evaluated in a separate Aggregate plan node above the
scan. There is no pushdown of aggregation into scan nodes or storage.

## ORDER BY within aggregate arguments

`STRING_AGG(col, ',' ORDER BY col)` and similar ordered-set aggregates are
not supported. The grammar does not accept ORDER BY inside aggregate function
calls.

## Nested aggregate calls

`SUM(COUNT(*))` and other nested aggregate calls are not supported. PostgreSQL
rejects these at parse time. The current implementation will produce an error
when trying to bind the inner aggregate's argument expression.

## AVG returns integer

`AVG` currently performs integer division and returns `Int32`. PostgreSQL
returns a numeric/float type for `AVG(integer)`. Supporting this would require
adding a float or numeric scalar type.

## Multiple data types

`SUM` and `AVG` only work on `Int32` values. `MIN` and `MAX` work on any
comparable type via the existing `compare_order_values` function, but `SUM`
and `AVG` on text or boolean columns silently skip non-integer values rather
than producing a type error.
