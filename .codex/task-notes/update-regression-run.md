Goal:
Fix the PostgreSQL `update` regression mismatches and rerun the focused test.

Key decisions:
Preserved the existing parser/analyzer/executor split while adding compatibility
behavior for PostgreSQL UPDATE edge cases. Used the PostgreSQL expected output as
the comparison target and reran only the focused `update` regression.

Files touched:
src/backend/parser/gram.rs
src/backend/parser/analyze/modify.rs
src/backend/parser/analyze/query.rs
src/backend/parser/analyze/scope.rs
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/backend/executor/exec_expr.rs
src/backend/executor/nodes.rs
src/backend/rewrite/view_dml.rs
src/backend/tcop/postgres.rs
src/include/catalog/pg_operator.rs
src/include/nodes/execnodes.rs
src/include/nodes/primnodes.rs

Tests run:
CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-yangon-update' cargo check
CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-target-yangon-update' scripts/run_regression.sh --test update --timeout 300 --port 15433 --results-dir '/Volumes/OSCOO PSSD/pgrust/tmp/pgrust-update-regress-after11'

Remaining:
Focused `update` regression passes: 300/300 query blocks matched.
`cargo check` passes with the pre-existing unreachable-pattern warnings.
