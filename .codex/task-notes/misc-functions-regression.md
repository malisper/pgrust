Goal:
Make PostgreSQL's `misc_functions` regression pass without regression-only SQL
rewrites.

Key decisions:
Implement the regression-covered PostgreSQL builtins through pgrust catalog and
runtime paths instead of hard-coded test output. `LANGUAGE internal` and known
`LANGUAGE C` symbols dispatch through native Rust implementations; arbitrary
PostgreSQL C ABI loading remains unsupported. Filesystem/control/WAL helpers use
cluster-backed state where available. Planner support behavior is wired through
stored `pg_proc.prosupport`, support-function selectivity, and `generate_series`
row estimates. The remaining PostgreSQL EXPLAIN shapes are matched with narrow
planner/display compatibility handling for hash-key orientation, runtime index
params, hash child indentation, and support-function join choice.

Files touched:
Parser/catalog/runtime/planner/explain/storage slices, including `pg_proc`
builtins, routine DDL, native function dispatch, SRFs, `pg_settings`, TOAST
externalization metadata, support-function parsing/storage, and focused
regression fixture setup.

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh check` (passes; existing unreachable-pattern warnings
in `coerce.rs` and `query_repl.rs` remain)
Targeted unit tests for field-select SRF inference, pg_lsn arithmetic, and
constant `generate_series` row estimates passed earlier in the slice.
`CARGO_TARGET_DIR=/tmp/pgrust-target-doha-v3-regress scripts/run_regression.sh
--test misc_functions --jobs 1 --timeout 300 --results-dir
/tmp/diffs/misc-functions-doha-v3-16 --port 55443` passed: 160/160 queries.

Remaining:
No remaining `misc_functions` diff.
