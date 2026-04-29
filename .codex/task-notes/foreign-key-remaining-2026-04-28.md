Goal:
Finish the remaining FK-specific foreign_key regression mismatches after the committed baseline.

Key decisions:
- Kept partition-aware referenced validation and FK action recheck fixes.
- Prioritized exact referenced-partition FK clone rows ahead of ancestor fallback rows for inbound checks.
- Added PostgreSQL-style inherited FK ALTER CONSTRAINT detail/hint.
- Fixed psql FK display to respect the configured search path for referenced relation names.
- Tried but reverted a direct non-root referenced-ancestor partition move guard because it caused broad timeout/regression fallout.

Files touched:
- src/backend/commands/tablecmds.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/foreign_keys.rs
- src/backend/executor/mod.rs
- src/backend/parser/analyze/constraints.rs
- src/backend/tcop/postgres.rs
- src/pgrust/database/commands/constraint.rs
- src/pgrust/database/commands/partition.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- git diff --check
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet partitioned_foreign_key
- scripts/cargo_isolated.sh test --lib --quiet referenced_partition_foreign_key -- --nocapture
- scripts/cargo_isolated.sh test --lib --quiet foreign_keys_apply_referential_actions
- PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh --test foreign_key --timeout 300 --jobs 1 --port 55433

Remaining:
- foreign_key regression is at 1205/1252 matched with 47 mismatches using a 30s statement timeout.
- The default 5s statement timeout can still time out in the large fkpart11 CREATE SCHEMA statement on this machine.
- Remaining buckets are clone-name/dependency display, fkpart10/fkpart11 partition action routing/display, CONSTRAINT TRIGGER unsupported, data-modifying CTE unsupported, and drop-schema notice ordering.
