Goal:
Run the PostgreSQL `foreign_key` regression and explain why server startup reports a huge allocation.

Key decisions:
The regression did not reach SQL execution. Startup aborts while bootstrapping durable shared catalog data. Temporary probes narrowed the path to `CatalogStore::load_shared` -> `sync_physical_catalogs_scoped` -> `CatCache::from_catalog` -> `bootstrap_pg_amop_rows` -> `bootstrap_pg_operator_rows`.

The reported hundreds-of-TB allocation is not an intentional buffer-pool allocation. Further narrowed with a temporary `catalog_alloc_repro` bin: a normal debug binary that only calls `bootstrap_pg_operator_rows()` is enough to fail. Release mode prints `operator rows: 603` and `amop rows: 522`.

Root cause: dev builds were using `rustc_codegen_cranelift` via `.cargo/config.toml`, while test and release profiles use LLVM. A normal debug binary that only calls `bootstrap_pg_operator_rows()` corrupts generated `PgOperatorRow` Vec metadata under Cranelift. LLVM release builds print the expected `operator rows: 603` and `amop rows: 522`, and lib tests pass because profile.test is already pinned to LLVM.

Files touched:
`.cargo/config.toml`: switched profile.dev from Cranelift to LLVM because
Cranelift corrupts generated catalog bootstrap row Vec metadata on aarch64
macOS normal debug builds.

Tests run:
`scripts/run_regression.sh --test foreign_key --jobs 1 --results-dir /tmp/diffs 2>&1 | tee /tmp/diffs/run.log`
`RUST_BACKTRACE=1 scripts/run_regression.sh --test foreign_key --jobs 1 --results-dir /tmp/diffs 2>&1 | tee /tmp/diffs/run-backtrace.log`
Focused catalog tests:
`scripts/cargo_isolated.sh test --lib --quiet bootstrap_rows_include_macaddr_operators`
`scripts/cargo_isolated.sh test --lib --quiet spgist_box_ordering_row_matches_postgres_shape`
Temporary repros:
debug normal bin calling `bootstrap_pg_operator_rows()` failed;
release normal bin calling `bootstrap_pg_operator_rows()` and `bootstrap_pg_amop_rows()` passed.

Remaining:
The foreign_key regression now reaches SQL execution and writes diffs under
`/tmp/diffs`, but the full file still times out. Latest run summary: planned 1,
timed out 1, queries total 1252, matched 778, mismatched 474.
