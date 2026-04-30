Goal:
Make `scripts/run_regression.sh --test transactions` pass against PostgreSQL expected output and copy current regression artifacts to `/tmp/diffs`.

Key decisions:
- Renamed the branch to malisper/transactions-regression.
- Added transaction read-only/deferrable/default GUC state and PostgreSQL-compatible SET/SHOW/RESET behavior, including read-only write guards that still allow temp relations.
- Added COMMIT/ROLLBACK AND [NO] CHAIN parsing and session behavior that preserves transaction characteristics across chained transactions.
- Modeled savepoint subxids in snapshots/visibility and portal state so rollback/release behavior matches the transactions regression without physically deleting heap tuples.
- Added readable `xmin` system column support backed by tuple header xmin.
- Fixed the PL/pgSQL/SPI shapes exercised by the regression, including aggregate RETURN FROM, ANALYZE/temp-table statements, volatile function snapshot behavior, and return-expression context.
- Allowed bare VACUUM parsing so escaped semicolon tests reach the transaction-block error path.

Files touched:
- crates/pgrust_sql_grammar/src/gram.pest
- src/backend/access/heap/heapam_visibility.rs
- src/backend/access/transam/xact.rs
- src/backend/executor/driver.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/nodes.rs
- src/backend/executor/sqlfunc.rs
- src/backend/parser/analyze/scope.rs
- src/backend/parser/gram.rs
- src/backend/parser/tests.rs
- src/backend/tcop/postgres.rs
- src/backend/utils/misc/guc.rs
- src/backend/utils/time/snapmgr.rs
- src/bin/query_repl.rs
- src/include/nodes/execnodes.rs
- src/include/nodes/parsenodes.rs
- src/include/nodes/primnodes.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/database_tests.rs
- src/pgrust/portal.rs
- src/pgrust/session.rs
- src/pl/plpgsql/compile.rs
- src/pl/plpgsql/exec.rs
- .codex/task-notes/transactions-regression.md

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet parse_set_transaction_snapshot
- scripts/cargo_isolated.sh test --lib --quiet transaction_characteristic_gucs_use_session_state
- scripts/cargo_isolated.sh test --lib --quiet parse_transaction_characteristic_gucs
- scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh build --bin pgrust_server
- scripts/run_regression.sh --test transactions --results-dir /tmp/pgrust_regress_transactions --timeout 60 --port 55433 --skip-build
- Copied `/tmp/pgrust_regress_transactions/output/transactions.out` and `summary.json` to `/tmp/diffs`; no `transactions.diff` exists because the regression passed.

Remaining:
- None for the transactions regression: 439/439 queries matched.
