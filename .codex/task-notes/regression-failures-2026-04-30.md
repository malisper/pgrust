Goal:
Fix GitHub regression failures for btree_index, matview, prepare.
Key decisions:
Downloaded latest regression-history output from run 25177016167 into /tmp/pgrust-regression-latest.
Prepared statements now preserve source SQL, validate declared/derived parameter types at PREPARE/EXECUTE time, and expose nullable result metadata like PostgreSQL.
Materialized view refresh/deparse now tolerates renamed output columns while preserving stored relation column names; default ACL cleanup removes restored self-grants.
Btree fixes align opclass option errors, temp relation display, disabled scan EXPLAIN output, scalar-array/range costing, row-prefix index quals, and pg_proc name/text pattern matching.
Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/catalog/object_address.rs
src/backend/commands/explain.rs
src/backend/executor/srf.rs
src/backend/executor/value_io/array.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/setrefs.rs
src/backend/parser/gram.rs
src/backend/rewrite/views.rs
src/backend/tcop/postgres.rs
src/include/nodes/parsenodes.rs
src/pgrust/database.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/index.rs
src/pgrust/session.rs
Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-irvine-v3-fix PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/irvine-v3-fix PGRUST_TARGET_POOL_SIZE=1 PGRUST_TARGET_SLOT=0 scripts/cargo_isolated.sh check
scripts/run_regression.sh --test prepare --port 6543 --results-dir /tmp/pgrust-regression-fix-prepare
scripts/run_regression.sh --test matview --port 6553 --results-dir /tmp/pgrust-regression-fix-matview
CARGO_TARGET_DIR=/tmp/pgrust-target-irvine-v3-fix scripts/run_regression.sh --test btree_index --port 6585 --results-dir /tmp/pgrust-regression-fix-btree
Remaining:
Focused validations pass. Cargo check still reports pre-existing unreachable-pattern warnings.
