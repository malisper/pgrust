Goal:
- Fix TASK-C13-02 for PostgreSQL regression file `truncate`: TRUNCATE options, identity restart, partition expansion, cascade/restrict FK behavior, and statement-level TRUNCATE triggers.

Key decisions:
- Extended the TRUNCATE raw parse node to carry target relations, descendant inclusion, identity mode, and cascade/restrict behavior while keeping the old table-name field for compatibility.
- Resolved TRUNCATE targets centrally in table commands so parser/session/database paths share partition/inheritance expansion and FK restrict/cascade checks.
- Routed transactional TRUNCATE through catalog-storage rewrite with owned sequence restart effects, and advanced the executor command id after rewrite so after-TRUNCATE trigger dynamic SQL sees the rewritten relation.
- Added statement-level TRUNCATE trigger firing only; row-level TRUNCATE triggers and transition tables remain unsupported.
- Switched missing text sequence lookup in sequence functions from "table does not exist" to PostgreSQL's "relation does not exist" wording.

Files touched:
- crates/pgrust_sql_grammar/src/gram.pest
- src/backend/parser/gram.rs
- src/backend/parser/tests.rs
- src/include/nodes/parsenodes.rs
- src/backend/commands/tablecmds.rs
- src/backend/commands/trigger.rs
- src/backend/utils/trigger.rs
- src/backend/executor/exec_expr.rs
- src/pgrust/database.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/database/commands/trigger.rs
- src/pgrust/session.rs
- src/pl/plpgsql/exec.rs

Tests run:
- cargo fmt
- env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_SIZE=32 PGRUST_TARGET_SLOT=17 scripts/cargo_isolated.sh check
- env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_SIZE=32 PGRUST_TARGET_SLOT=17 scripts/run_regression.sh --test truncate --port 55433 --results-dir /tmp/pgrust-task-c13-02-truncate
- git diff --check

Remaining:
- None for the owned `truncate` regression file. The final regression run passed 201/201 queries.
- Note: target slot 7 produced a stale-binary regression result during validation; slot 17 was rebuilt from this checkout and used for final validation.
