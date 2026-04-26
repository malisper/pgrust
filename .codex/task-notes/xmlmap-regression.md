Goal:
Fix xmlmap regression failures against PostgreSQL's no-libxml expected output.

Key decisions:
Seed the missing XML mapping pg_proc rows and route those calls to PostgreSQL's
standard unsupported XML feature error. Treat XMLFOREST as libxml-gated for the
current regression profile. Allow DECLARE CURSOR WITH HOLD outside an explicit
transaction by materializing it through an implicit transaction, matching the
xmlmap cursor setup. Domain casts to date now validate against the domain base
type for explicit text input casts. The protocol error-position helper suppresses
cursor locations for the no-libxml unsupported XML feature error, matching
PostgreSQL's expected output.

Files touched:
src/include/catalog/pg_proc.rs
src/include/nodes/primnodes.rs
src/backend/parser/analyze/functions.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_xml.rs
src/backend/executor/fmgr.rs
src/backend/executor/tests.rs
src/backend/parser/analyze/expr.rs
src/backend/tcop/postgres.rs
src/pgrust/session.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet backend::executor::tests::xml
scripts/cargo_isolated.sh test --lib --quiet holdable_cursor
CARGO_TARGET_DIR=/tmp/pgrust-target-kingston cargo test --lib --quiet exec_error_position_omits_unsupported_xml_feature
CARGO_TARGET_DIR=/tmp/pgrust-target-kingston cargo test --lib --quiet unsupported_xml_feature
CARGO_TARGET_DIR=/tmp/pgrust-target-kingston cargo test --lib --quiet holdable_cursor_can_be_declared_outside_explicit_transaction
CARGO_TARGET_DIR=/tmp/pgrust-target-kingston cargo test --lib --quiet text_literal_cast_to_date_domain_uses_base_type_input
CARGO_TARGET_DIR=/tmp/pgrust-target-kingston scripts/run_regression.sh --test xmlmap --schedule .context/xmlmap.schedule --port 55450 --timeout 180

Remaining:
xmlmap passes with a custom one-test schedule to avoid unrelated create_index
base staging failures. Shared /tmp/pgrust-target was stale from another
workspace; the passing run used /tmp/pgrust-target-kingston.
