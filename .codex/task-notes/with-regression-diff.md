Goal:
Make the PostgreSQL `with` regression greener against upstream behavior.

Key decisions:
Current implemented fixes:
- Preserve outer `WITH RECURSIVE` when visible select CTEs are prepended into
  DML CTE bodies.
- Let subquery target pruning skip unused `SubLink`/`SubPlan` targets.
- Preserve `DELETE ... USING` input-plan subplans during delete finalization.
- Add a setrefs fallback for recursive CTE join paths whose semantic `Var`
  carries a child RTE varno with a joined-path logical attno.
- Select the `OuterLevelAggregateNestedCte` error for outer-owned aggregates
  that reference a nested CTE.
- Execute unconditional row-independent `DO INSTEAD` delete rule actions once
  per statement, and return rows from `DO INSTEAD SELECT` insert rule actions.

Latest regression:
- `/tmp/diffs/with-after6/status/with.status`
- `fail with 219 93 312 1450`

Files touched:
- `src/backend/commands/tablecmds.rs`
- `src/backend/optimizer/path/subquery_prune.rs`
- `src/backend/optimizer/setrefs.rs`
- `src/backend/parser/analyze/agg_output.rs`
- `src/backend/parser/analyze/agg_scope.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/parser/tests.rs`
- `src/pgrust/database/commands/execute.rs`
- `src/pgrust/database/commands/rules.rs`
- `src/pgrust/session.rs`

Tests run:
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-spokane-v4-with' scripts/cargo_isolated.sh test --lib --quiet writable_cte`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-spokane-v4-with' scripts/cargo_isolated.sh test --lib --quiet sublink`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-spokane-v4-with' scripts/cargo_isolated.sh test --lib --quiet recursive`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-spokane-v4-with' scripts/cargo_isolated.sh test --lib --quiet outer_aggregate_rejects_nested_subquery_reference_to_local_cte`
- `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/rust/cargo-target-spokane-v4-with' scripts/run_regression.sh --test with --results-dir /tmp/diffs/with-after6 --timeout 120 --jobs 1 --port 25436`
- `git diff --check`

Remaining:
- Recursive CTE non-initialization-order query still times out.
- `ON CONFLICT` still treats a row changed in a writable CTE and then touched by
  the outer statement as same-command cardinality violation; likely command-id
  boundary/visibility work.
- SEARCH/CYCLE traversal and record-array formatting diffs remain.
- Many EXPLAIN/viewdef/error-caret diffs remain display-only.
- Column label propagation remains wrong for nested WITH scalar/VALUES outputs.
