Goal:
Fix writable CTE RETURNING behavior for the with regression, excluding privileges column-privilege diffs.

Key decisions:
Use a bound statement-local writable CTE path instead of pre-bind row materialization.
Bind writable CTE bodies during normal CTE binding, derive CTE columns from RETURNING, execute producers once with the outer statement xid/cid/snapshot, and pin RETURNING rows by cte_id for CteScan.
Keep PostgreSQL-compatible errors for referenced writable CTEs without RETURNING, recursive writable CTEs, and non-top-level writable CTEs.
Run unconditional DO INSTEAD rule actions that do not reference OLD/NEW once at statement level for writable CTE DELETE RETURNING.
Preserve CTE projection boundaries when a simple CTE wrapper drops columns from its source CTE; PostgreSQL does not let a pass-through optimization change the visible CTE rowtype/attnos.

Files touched:
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/mod.rs
src/backend/executor/nodes.rs
src/backend/commands/tablecmds.rs
src/pgrust/session.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/rules.rs
src/backend/optimizer/tests.rs

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/daegu-v6/returning scripts/cargo_isolated.sh test --lib --quiet writable_cte
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/daegu-v6/returning scripts/cargo_isolated.sh test --lib --quiet rule_view_dml_returning
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/daegu-v6/returning scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/daegu-v6/returning scripts/cargo_isolated.sh test --lib --quiet planner_handles_recursive_cte_non_output_filter_column -- --nocapture
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/daegu-v6/regression scripts/run_regression.sh --test with --port 55433 --results-dir /tmp/diffs/returning-with-fix

Remaining:
The recursive CTE setrefs panic is fixed; the with regression now completes instead of erroring.
The with regression still fails 99 queries (213/312 matched). Remaining diffs include recursive CTE execution semantics in the non-initialization-order section, writable CTE row/explain differences, and known unrelated planner/viewdef formatting gaps.
