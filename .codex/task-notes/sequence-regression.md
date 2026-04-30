Goal:
Make PostgreSQL's sequence regression pass and copy final artifacts to /tmp/diffs.

Key decisions:
Implemented PostgreSQL-compatible sequence syntax, options, runtime state,
privileges, read-only transaction checks, catalog functions/views, comments,
dependencies, and INSERT/default ordering.
Fixed temp serial sequence ownership so DROP TABLE removes owned temp sequences
and clears cascaded temp namespace entries.
Kept integer-to-smallint assignment coercion on the numeric cast path so
sequence regression gets PostgreSQL's smallint out-of-range error.
Used ParseError::UnsupportedType for unknown sequence AS types so the protocol
can attach the expected source position.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/catalog/store/heap.rs
src/backend/executor/exec_expr.rs
src/backend/executor/srf.rs
src/backend/executor/value_io.rs
src/backend/parser/gram.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/system_views.rs
src/backend/utils/cache/system_view_registry.rs
src/include/catalog/pg_proc.rs
src/include/nodes/parsenodes.rs
src/include/nodes/primnodes.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/privilege.rs
src/pgrust/database/commands/rename.rs
src/pgrust/database/commands/sequence.rs
src/pgrust/database/sequences.rs
src/pgrust/database/temp.rs
src/pgrust/session.rs
plus exhaustive-match plumbing files for new sequence SRFs/builtins.

Tests run:
cargo fmt
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=42 scripts/cargo_isolated.sh check
env -u CARGO_TARGET_DIR scripts/run_regression.sh --test sequence --results-dir /tmp/pgrust_sequence_regress --timeout 120

Remaining:
sequence passes: 261/261 queries matched. /tmp/diffs/sequence.diff is empty and
/tmp/diffs/sequence.out contains the passing output.
