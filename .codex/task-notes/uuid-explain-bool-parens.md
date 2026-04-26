Goal:
Fix uuid regression EXPLAIN filter output so OR clauses keep PostgreSQL-style per-clause parentheses.
Key decisions:
Changed generic EXPLAIN boolean rendering instead of adding UUID-specific behavior. Boolean AND/OR rendering now relies on the outer expression wrapper and parenthesizes non-bare clause arguments.
Files touched:
src/backend/executor/nodes.rs
src/backend/executor/tests.rs
Tests run:
cargo fmt
cargo test --lib --quiet explain_expr_ attempted; isolated target retry failed because /tmp had no free space.
git diff --check
CARGO_INCREMENTAL=0 cargo test --lib --quiet explain_expr_parenthesizes_boolean_clause_args failed before pgrust compiled because shared /tmp/pgrust-target dependency artifacts were missing/corrupt during serde_derive linking.
Remaining:
Run targeted unit tests and scripts/run_regression.sh --test uuid once local /tmp space/build contention is cleared.
