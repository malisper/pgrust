Goal:
Fix the requested /tmp/diffs/join.diff buckets: LATERAL/correlated binding failures, unsupported complex SELECT join forms, and DELETE ... USING forms.

Key decisions:
Added DELETE USING to the parser/AST/analyzer and execute it by planning the joined input, preserving target CTIDs for deletion.
Allowed joined trees on the RHS of joins and threaded outer scopes through derived/lateral subqueries so correlated references bind in PostgreSQL-like places.
Kept RIGHT/FULL JOIN LATERAL references to the left side invalid at bind time.
Taught join costing/setrefs about nested lateral/subquery dependencies so correlated SubLinks and LATERAL VALUES keep the required nested-loop orientation.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/modify.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/commands/rules.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/setrefs.rs
src/backend/parser/tests.rs
src/backend/executor/tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet lateral
scripts/cargo_isolated.sh test --lib --quiet delete_using
scripts/cargo_isolated.sh test --lib --quiet parse_delete_using_clause
scripts/cargo_isolated.sh test --lib --quiet parse_join_rhs_can_be_join_tree
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh build --bin pgrust_server
scripts/run_regression.sh --test join --skip-build --jobs 1 --port 55490 --timeout 240

Remaining:
The single-test join harness now runs to completion and /tmp/diffs/join.after.diff has no unsupported SELECT-form or DELETE-form errors. It still has unrelated join output diffs, a PREPARE/EXECUTE unsupported block, single-test LATERAL VALUES timeouts because the harness does not set up create_index for join, and one LATERAL placeholder-var runtime corner case that changed from a binding error to a setrefs error.
