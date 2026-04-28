Goal:
Fix hash_part regression failures caused by missing satisfies_hash_partition support.

Key decisions:
Implemented satisfies_hash_partition as a SQL-visible builtin with PostgreSQL-compatible validation, typed-null preservation, normal arguments, and explicit VARIADIC array handling.

Files touched:
src/include/nodes/primnodes.rs
src/include/catalog/pg_proc.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/expr/func.rs
src/backend/optimizer/constfold.rs
src/backend/executor/mod.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_partition.rs
src/backend/rewrite/views.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet satisfies_hash_partition_matches_postgres_validation_and_hashing
scripts/run_regression.sh --test hash_part --results-dir /tmp/pgrust_hash_part_fix --timeout 60
scripts/cargo_isolated.sh check

Remaining:
None for hash_part; regression passed 28/28 queries.
