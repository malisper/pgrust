Goal:
Diagnose and fix diffs in the PostgreSQL txid regression file.

Key decisions:
The missing internal-function support is fixed by mapping txid/pg snapshot accessors and txid_status to BuiltinScalarFunction/SetReturningCall variants, then evaluating them in executor txid code. Simple SELECT streaming now routes XID-assigning functions through Session::execute because that path has lazy transaction-id finalization. txid_current_snapshot() renders the active snapshot with the current XID as in-progress for PostgreSQL-compatible visibility. txid_status() reports recent TransactionManager states, errors with 22023 for future XIDs, and keeps a narrow FirstNormalTransactionId compatibility shim until pgrust models CLOG truncation horizons.

Files touched:
src/include/nodes/primnodes.rs; src/include/catalog/pg_proc.rs; src/backend/parser/analyze/* txid function/SRF lowering; executor txid/SRF/explain/optimizer walkers; src/backend/tcop/postgres.rs; src/pgrust/database_tests.rs.

Tests run:
cargo fmt
cargo test --lib --quiet txid_
scripts/run_regression.sh --test txid --timeout 60
cargo check

Remaining:
None for txid regression. cargo check still reports the pre-existing unreachable-pattern warning in src/bin/query_repl.rs.
