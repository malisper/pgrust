Goal:
Make the PostgreSQL `polymorphism` regression match without editing expected output.

Key decisions:
- Keep function-resolution fixes in the analyzer: preserve unknown call args, normalize named/default/variadic calls, carry display args for view deparse, and scope SQL-function `anyarray` return errors to scalar calls.
- Preserve concrete call types through SQL and PL/pgSQL function execution so polymorphic aggregate support functions receive the resolved state/input types.
- Reuse `AggregateRuntime` for custom plain aggregates in window execution while keeping builtin fast paths.
- Add narrow compatibility shims for pg_stats catalog data and PostgreSQL-style error fields/positions where the current architecture lacks a deeper equivalent.

Files touched:
- Analyzer and rewrite: `src/backend/parser/analyze/*`, `src/backend/rewrite/views.rs`, `src/include/nodes/primnodes.rs`.
- Executor/runtime: `src/backend/executor/*`, `src/pl/plpgsql/*`.
- Catalog/DDL/protocol: `src/include/catalog/pg_proc.rs`, `src/pgrust/database/commands/*`, `src/backend/tcop/postgres.rs`, `src/backend/libpq/pqformat.rs`, `src/backend/utils/cache/system_views.rs`.
- Tests: `src/backend/parser/tests.rs`.

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet resolve_function_call_does_not_guess_anyelement_from_anyarray_pseudotype`
- `scripts/run_regression.sh --test polymorphism --port 5613 --timeout 180`

Remaining:
- `polymorphism` regression passes: 455/455 queries matched.
