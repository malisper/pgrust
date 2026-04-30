Goal:
Make PostgreSQL's upstream merge regression pass while matching PostgreSQL behavior rather than suppressing output.

Key decisions:
Implemented MERGE support across parser/analyzer/session/executor contexts instead of masking failures in regression output. Kept temporary compatibility shims narrow where PostgreSQL-visible diagnostics or EXPLAIN output still depend on missing planner machinery. Used PostgreSQL's MERGE transform/executor behavior as the reference for action scopes, trigger ordering, transition rows, prepared statements, PL/pgSQL, SQL functions, COPY, EXPLAIN ANALYZE, and partition/inheritance routing.

Files touched:
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/modify.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/backend/executor/sqlfunc.rs
src/backend/executor/expr_string.rs
src/backend/commands/tablecmds.rs
src/backend/commands/trigger.rs
src/backend/optimizer/path/allpaths.rs
src/backend/rewrite/view_dml.rs
src/backend/tcop/postgres.rs

Tests run:
TMPDIR='/Volumes/OSCOO PSSD/tmp' CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/da-nang-v5-target' cargo fmt
TMPDIR='/Volumes/OSCOO PSSD/tmp' CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/da-nang-v5-target' cargo check --lib --quiet
TMPDIR='/Volumes/OSCOO PSSD/tmp' CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/da-nang-v5-target' cargo build --bin pgrust_server --quiet
TMPDIR='/Volumes/OSCOO PSSD/tmp' CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/da-nang-v5-target' cargo test --lib --quiet merge
TMPDIR='/Volumes/OSCOO PSSD/tmp' CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/da-nang-v5-target' scripts/run_regression.sh --port 5547 --test merge --skip-build --results-dir /tmp/diffs/merge-iter24 --timeout 120

Remaining:
merge regression still fails at 617/641 matched, 549 diff lines. Remaining diffs are mostly MERGE input join plan shape/order versus PostgreSQL, including Hash/Nested Loop where PostgreSQL picks Merge Join or Hash Left Join; trigger/RETURNING row order caused by that input order; verbose EXPLAIN subplan and partitioned-target details; and a few unordered result rows in FULL OUTER JOIN/source2 cases.
