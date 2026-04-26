Goal:
Move type_sanity toward PostgreSQL-compatible catalog/function behavior and route
the regression helper through a Rust LANGUAGE internal function.

Key decisions:
PostgreSQL treats pg_proc as the executable source of truth, so scalar function
plans now carry funcid and executor dispatch goes through an fmgr-style layer.
pg_rust_is_catalog_text_unique_index_oid(oid) is a normal pg_proc-backed
internal function, and the type_sanity fixture rewrites the upstream C helper to
LANGUAGE internal.

type_sanity also needed broader catalog compatibility, not just the C helper:
pg_type now exposes the columns type_sanity asks for, builtin/dynamic type rows
carry fuller metadata, pg_range is queryable, pg_attribute rows derive storage
metadata from pg_type, and catalog-only support/type-I/O rows exist for the
sanity checks. Catalog-only shims have nearby :HACK: comments.

regtype casts must resolve pg_type catalog rows, not only runtime text-input
targets. This matters for catalog-only types such as gtsvector.

arrayrange/varbitrange are pgrust compatibility shims for PostgreSQL regression
tests, not PostgreSQL in-core bootstrap types. Their OIDs were moved out of the
oid < 16384 bootstrap range so type_sanity does not treat them as core coverage.
VisibleCatalog now snapshots dynamic pg_range rows alongside dynamic pg_type rows
so fmgr can resolve synthetic range procs by OID at execution time.

Files touched:
.codex/task-notes/type-sanity-diff.md
scripts/run_regression.sh
scripts/run_regression_one_by_one.sh
src/backend/catalog/pg_class.rs
src/backend/catalog/rowcodec.rs
src/backend/catalog/rows.rs
src/backend/catalog/store/heap.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_ops.rs
src/backend/executor/expr_string.rs
src/backend/executor/fmgr.rs
src/backend/executor/mod.rs
src/backend/executor/tests.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/system_views.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/utils/cache/catcache.rs
src/backend/utils/cache/lsyscache.rs
src/backend/utils/cache/system_view_registry.rs
src/backend/utils/cache/visible_catalog.rs
src/backend/utils/time/date.rs
src/include/catalog/bootstrap.rs
src/include/catalog/builtin_ranges.rs
src/include/catalog/pg_attribute.rs
src/include/catalog/pg_cast.rs
src/include/catalog/pg_class.rs
src/include/catalog/pg_namespace.rs
src/include/catalog/pg_opclass.rs
src/include/catalog/pg_proc.rs
src/include/catalog/pg_range.rs
src/include/catalog/pg_type.rs
src/include/nodes/primnodes.rs
src/pgrust/database.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet regtype_cast_resolves_catalog_only_types
scripts/cargo_isolated.sh test --lib --quiet regtype_input_distinguishes_soft_and_hard_errors
scripts/cargo_isolated.sh test --lib --quiet bootstrap_rows_have_unique_oids
scripts/cargo_isolated.sh test --lib --quiet create_ranges_over_array_and_varbit_subtypes
scripts/cargo_isolated.sh test --lib --quiet multiranges_support_array_varbit_and_composite_subtypes
CARGO_TARGET_DIR=/tmp/pgrust-target-kyiv-type-sanity PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh --test type_sanity --jobs 1 --timeout 240 --port 55449
CARGO_TARGET_DIR=/tmp/pgrust-target-kyiv-type-sanity PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh --skip-build --test strings --jobs 1 --timeout 240 --port 55451
CARGO_TARGET_DIR=/tmp/pgrust-target-kyiv-type-sanity PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh --skip-build --test create_type --jobs 1 --timeout 240 --port 55450

Results:
type_sanity PASS: 63/63 queries.
strings PASS: 508/508 queries.
create_type FAIL: 43/86 queries matched. The visible failures are broader
DDL/comment/dependency gaps, not the type_sanity helper or fmgr dispatch path.
Diff saved to /tmp/diffs/create_type_kyiv_v2_after_fmgr.diff.

Remaining:
create_type still needs separate work for COMMENT ON TYPE/COLUMN, dependency
error detail/cascade behavior, and exact notice cursor output. The local check
still emits the pre-existing query_repl.rs unreachable-pattern warning.
