Goal:
Fix `with.sql` recursive CTE gaps around SEARCH/CYCLE syntax, recursive
binding diagnostics, and planner behavior.

Key decisions:
- Preserve SEARCH/CYCLE metadata in the raw AST and lower generated search,
  cycle mark, and cycle path columns during recursive CTE binding.
- Preserve left-nested recursive UNION shape so error selection matches
  PostgreSQL more closely.
- Allow recursive UNION DISTINCT hashing for generated record and record-array
  columns because `Value` already supports record equality and hashing.
- Treat qualified column references as table/CTE references for dependency
  ordering and rule-action OLD/NEW checks.

Files touched:
- `crates/pgrust_sql_grammar/src/gram.pest`
- `src/backend/parser/gram.rs`
- `src/include/nodes/parsenodes.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/parser/analyze/expr.rs`
- `src/backend/parser/analyze/rules.rs`
- `src/backend/parser/analyze/scope.rs`
- `src/backend/executor/startup.rs`
- `src/pgrust/database/commands/rules.rs`
- `src/backend/parser/tests.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `scripts/cargo_isolated.sh check`
- Focused parser/executor tests for recursive CTE SEARCH/CYCLE and MERGE
  parsing.
- `scripts/run_regression.sh --test with --jobs 1 --port 55450 --results-dir /tmp/diffs/with_full_fix10`
  completed without harness timeout: 206/312 queries matched.

Remaining:
- See `.codex/task-notes/with-full-fix.md` for the current remaining
  `with.sql` failure categories after the broader writable CTE/rules/MERGE
  work.
