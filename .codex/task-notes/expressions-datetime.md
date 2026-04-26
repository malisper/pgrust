Goal:
Diagnose and fix expressions.diff mismatches for now(), current_time, localtime, current_timestamp, and localtimestamp.

Key decisions:
Autocommit execution now stamps DateTimeConfig once per statement so SQL value functions and now()/transaction_timestamp() share the same timestamp source.
timestamptz casts and current_date now use the named timezone offset at the relevant UTC timestamp instead of fixed-offset-only timezone_offset_seconds().

Files touched:
src/pgrust/database/commands/execute.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_datetime.rs

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-west-monroe-v1-target cargo check
CARGO_TARGET_DIR=/tmp/pgrust-west-monroe-v1-target scripts/run_regression.sh --test expressions --timeout 20 --jobs 1

Remaining:
None for expressions.diff.
