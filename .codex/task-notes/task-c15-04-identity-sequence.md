Goal:
Fix TASK-C15-04 identity/sequence regression failures around identity sequence metadata rewrites and table-drop sequence ownership.

Key decisions:
- Treat `pg_depend` owned-by rows as the source of truth for table-drop sequence cascading; plain `nextval(...)` defaults keep their referenced sequence after the table is dropped.
- During partitioned identity column type changes, update each identity sequence at most once and skip `pg_sequence` rewrites when the computed sequence data is unchanged.

Files touched:
- src/pgrust/database/commands/alter_column_type.rs
- src/pgrust/database/commands/drop.rs

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-c15-04-identity scripts/run_regression.sh --test identity --port 65421 --results-dir /tmp/pgrust-task-c15-04-identity
- CARGO_TARGET_DIR=/tmp/pgrust-target-c15-04-identity scripts/run_regression.sh --test sequence --port 65423 --results-dir /tmp/pgrust-task-c15-04-sequence
- CARGO_TARGET_DIR=/tmp/pgrust-target-c15-04-identity PGRUST_TARGET_POOL_SIZE=64 PGRUST_TARGET_SLOT=43 scripts/cargo_isolated.sh check

Remaining:
- Both targeted regression files pass. `cargo_isolated.sh check` passes with pre-existing unreachable-pattern warnings outside this slice.
