# `bit.sql` `\d` Follow-Up

`bit.sql` exercises `\d bit_defaults`, which drives a chain of hidden psql
catalog queries. We intentionally did not broaden SQL/catalog support just to
make that one describe path run natively.

## Deferred Instead Of Implemented

- `LEFT JOIN` support in the planner/executor just for psql describe queries
- `pg_attrdef` as a real catalog-backed default-expression source
- `format_type(...)` support just to render `\d` column type names
- regex operators used by psql pattern matching, especially `~`
- `COLLATE` syntax/semantics for the describe lookup queries

## Current Tradeoff

For the `bit.sql` work, the practical path is narrow compatibility handling for
the specific psql describe queries that `\d bit_defaults` emits, instead of
using this file to force:

- broader join support
- more catalog tables/functions
- regex operator semantics
- collation-aware expression parsing/execution

## Preferred Future Direction

Replace the narrow compatibility path by making the describe queries work
natively through normal SQL support:

1. add real `LEFT JOIN`
2. add PostgreSQL-shaped default metadata (`pg_attrdef`) instead of the narrow
   sidecar default store
3. add `format_type(...)`
4. add regex operators used by psql
5. add `COLLATE` parsing and no-op-or-real collation semantics as appropriate

At that point `\d` should stop needing any bit-specific compatibility logic.
