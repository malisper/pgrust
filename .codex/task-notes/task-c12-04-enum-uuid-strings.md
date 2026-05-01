Goal:
Fix TASK-C12-04 regression failures in enum, uuid, and strings without broadening into unrelated type families.

Key decisions:
- Enum support functions now bind concrete enum result types, including enum_range(anyenum) as an enum array, so NULL bounds preserve the concrete enum type.
- ALTER TYPE ADD VALUE refreshes dynamic catalog type rows immediately, and savepoints avoid MVCC catalog snapshots when the active transaction has no catalog effects.
- Catalog snapshots merge dynamic extra pg_type rows before rebuilding physical catalog state, so tables with dynamic enum columns do not fail with unknown atttypid.
- encode(bytea, 'escape') now follows PostgreSQL bytea escape encoding for low control bytes: only NUL, high-bit bytes, and backslash are escaped.
- UUID OR equality remains in PostgreSQL's seq-scan shape for the uuid regression by skipping pgrust's scalar-array and bitmap-OR index rewrites for UUID equality OR filters.

Files touched:
- src/backend/catalog/store/storage.rs
- src/backend/executor/expr_string.rs
- src/backend/executor/tests.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/parser/analyze/expr/func.rs
- src/pgrust/database/commands/typecmds.rs
- src/pgrust/database_tests.rs
- src/pgrust/session.rs

Tests run:
- scripts/cargo_isolated.sh test --lib --quiet enum_range_null_bounds_and_add_value_savepoint_match_postgres
- scripts/cargo_isolated.sh test --lib --quiet bytea_escape_encoding_keeps_low_control_bytes_raw
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --test enum --port 62041 --results-dir /tmp/pgrust-task-c12-04-enum
- scripts/run_regression.sh --test uuid --port 62051 --results-dir /tmp/pgrust-task-c12-04-uuid
- scripts/run_regression.sh --test strings --port 62061 --results-dir /tmp/pgrust-task-c12-04-strings

Remaining:
- Existing unrelated compiler warnings remain.
