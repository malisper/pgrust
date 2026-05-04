Goal:
Fix regproc regression where missing regoperator errors printed `integer` instead of preserving input `int4`.
Key decisions:
PostgreSQL `regoperatorin` parses argument types for lookup but reports lookup misses with the original input string. Match that in the analyzer regoperator literal path and stop the privileges routine-signature shim from normalizing operator errors.
Files touched:
crates/pgrust_analyze/src/expr.rs
src/backend/tcop/postgres.rs
src/pgrust/database_tests.rs
Tests run:
scripts/cargo_isolated.sh test --lib --quiet regoperator -- --nocapture
scripts/run_regression.sh --test regproc --jobs 1 --timeout 120 --port 55463 --results-dir /tmp/pgrust-regproc-shanghai-v5-final
Remaining:
None.
