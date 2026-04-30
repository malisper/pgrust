Goal:
Implement PostgreSQL-compatible fixes for privileges regression failures.

Key decisions:
`/tmp/diff` did not exist, so reproduced focused `privileges` regression into `/tmp/diffs/privileges-investigate`.
Longer baseline completed without timeout: `/tmp/diffs/privileges-investigate-120` had 973/1295 matched queries.
Primary first slice is PostgreSQL-style relation privilege tracking: mark columns reached by Vars, not every visible column.
`GRANTED BY` object grantor selection is only minimally handled for current/non-current grantor errors; full grant-option dependency semantics remain.

Files touched:
`src/backend/parser/analyze/query.rs`
`src/backend/rewrite/mod.rs`
`src/backend/parser/analyze/modify.rs`
`src/backend/parser/gram.rs`
`src/backend/commands/tablecmds.rs`
`src/pgrust/database/commands/privilege.rs`
`src/pgrust/database/commands/role.rs`
`.codex/task-notes/privileges-regression.md`

Tests run:
`scripts/run_regression.sh --test privileges --results-dir /tmp/diffs/privileges-investigate --timeout 30 --jobs 1`
`scripts/run_regression.sh --test privileges --results-dir /tmp/diffs/privileges-investigate-120 --timeout 120 --jobs 1 --port 55433`
`CARGO_TARGET_DIR=/tmp/pgrust-target-privileges-check cargo check`
`scripts/run_regression.sh --test privileges --results-dir /tmp/diffs/privileges-after-next --timeout 120 --jobs 1 --port 55435`

Remaining:
Last completed focused regression after the first slices was 1002/1295 matched, 293 mismatches, no timeout.
Need rerun after ON CONFLICT/DELETE/function-parser/password-detail changes once local Cargo build contention clears.
Large remaining surfaces: default privileges, large-object ACLs/builtins, function/type privilege enforcement, `ALL ... IN SCHEMA`, catalog/info-schema views, MAINTAIN/pg_maintain, security-restricted operation contexts, and grant-option dependency cascade/restrict.
