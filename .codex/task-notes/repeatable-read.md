Goal:
Implement repeatable-read transaction isolation as the first isolation-level step.

Key decisions:
- Parse BEGIN/SET TRANSACTION/SET SESSION CHARACTERISTICS isolation options into parsenodes.
- Store an active transaction isolation level and reuse the first snapshot for repeatable read and serializable.
- Reuse the transaction snapshot for catalog lookups, normal execution, and streaming SELECT.
- Raise SQLSTATE 40001 on concurrent row update/delete conflicts when using a transaction snapshot.
- Parse READ ONLY and DEFERRABLE options but leave enforcement as a documented compatibility HACK.

Files touched:
- src/include/nodes/parsenodes.rs
- src/backend/parser/gram.pest
- src/backend/parser/gram.rs
- src/backend/parser/tests.rs
- src/pgrust/session.rs
- src/pgrust/database/commands/execute.rs
- src/backend/executor/mod.rs
- src/backend/executor/driver.rs
- src/backend/utils/cache/syscache.rs
- src/backend/utils/time/snapmgr.rs
- src/backend/commands/tablecmds.rs
- src/bin/query_repl.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet parse_set_transaction
- scripts/cargo_isolated.sh test --lib --quiet parse_transaction
- scripts/cargo_isolated.sh test --lib --quiet parse_set_session_transaction_characteristics
- scripts/cargo_isolated.sh test --lib --quiet parse_show_transaction_isolation_level
- scripts/cargo_isolated.sh test --lib --quiet repeatable_read
- scripts/cargo_isolated.sh test --lib --quiet read_committed_uses_fresh_statement_snapshots
- scripts/cargo_isolated.sh test --lib --quiet set_transaction_isolation_after_query_errors
- scripts/cargo_isolated.sh test --lib --quiet set_session_characteristics_sets_default_transaction_isolation
- scripts/cargo_isolated.sh check
- git diff --check

Remaining:
- Serializable currently uses repeatable-read snapshots without SSI/predicate locking.
- READ ONLY and DEFERRABLE transaction modes parse but are not enforced yet.
