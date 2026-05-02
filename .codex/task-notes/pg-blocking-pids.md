Goal:
Add PostgreSQL-compatible pg_blocking_pids() and pg_isolation_test_session_is_blocked() support for isolationtester lock waits.

Key decisions:
Blockers are computed from live lock-manager state for advisory, relation, tuple, and transactionid waits.
Results are sorted and deduplicated at the Database layer for deterministic pgrust behavior.
pg_isolation_test_session_is_blocked() intentionally checks modeled lock waits only; safe-snapshot and isolationtester injection-point waits are not modeled yet.
Isolation runner follow-up resolves Cargo's configured target directory, supports application_name for isolationtester setup, and accepts the text array literal form isolationtester prepares.

Files touched:
docs/isolation-tests.md
scripts/run_isolation.sh
src/backend/executor/exec_expr.rs
src/backend/executor/mod.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/tests.rs
src/backend/storage/lmgr/advisory.rs
src/backend/storage/lmgr/lock.rs
src/backend/storage/lmgr/proc.rs
src/backend/storage/lmgr/row.rs
src/include/catalog/pg_proc.rs
src/include/nodes/primnodes.rs
src/pgrust/database.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet pg_blocking_pids
scripts/cargo_isolated.sh test --lib --quiet pg_isolation_test_session_is_blocked
scripts/cargo_isolated.sh test --lib --quiet pg_locks
scripts/cargo_isolated.sh test --lib --quiet application_name_default_and_set_config_match_isolationtester
scripts/run_isolation.sh --test insert-conflict-do-nothing

Remaining:
None for insert-conflict-do-nothing; the upstream isolationtester binary builds and that spec passes locally.
