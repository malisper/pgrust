Goal:
Bring the PostgreSQL `stats_ext` regression file to a clean pass.

Key decisions:
- Kept PostgreSQL behavior as the reference for scalar-array selectivity,
  functional dependencies, MCV combination, catalog visibility, DDL support,
  psql display, RLS/operator binding, grouping uniqueness, and EXPLAIN output.
- Restored PostgreSQL's MCV residual formula and added a private uniform-list
  correction for pgrust's truncated JSON MCV payloads.
- Matched PostgreSQL's inclusive inequality estimator shape and float4
  precision for `pg_statistic` number-slot selectivity reads.

Files touched:
- SQL grammar/parser/analyzer, DDL/catalog privilege helpers, stats build and
  system views, optimizer selectivity/costing, EXPLAIN formatting, executor
  boolean/RLS behavior, and focused tests.

Tests run:
- `cargo fmt`
- `CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check`
- `PGRUST_STATEMENT_TIMEOUT=60 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 420 --port 62751`

Remaining:
- `stats_ext` is clean: final run passed `866/866` queries with no timeout.
