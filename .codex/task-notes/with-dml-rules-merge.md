Goal:
Implement PostgreSQL parity for data-modifying CTEs, rule interactions, and MERGE coverage in the `with` regression.

Key decisions:
- Added DELETE and MERGE as CTE bodies in grammar, AST, parser, PL/pgSQL normalization, relation-reference walking, and prepared-statement substitution.
- Materialize modifying CTE producers before the outer SELECT/VALUES/DML statement in session and database autocommit paths.
- Execute INSERT/UPDATE/DELETE/MERGE CTE bodies once and expose RETURNING rows as bound materialized CTEs.
- Added dependency ordering for non-self-referencing modifying CTEs under WITH RECURSIVE, preserving text order where there is no dependency.
- Added CREATE OR REPLACE RULE parsing/execution and PostgreSQL-style rule restriction checks for modifying CTE producers.

Files touched:
- `crates/pgrust_sql_grammar/src/gram.pest`
- `src/include/nodes/parsenodes.rs`
- `src/backend/parser/gram.rs`
- `src/backend/parser/tests.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/parser/analyze/modify.rs`
- `src/backend/parser/analyze/rules.rs`
- `src/backend/tcop/postgres.rs`
- `src/pgrust/session.rs`
- `src/pgrust/database/commands/execute.rs`
- `src/pgrust/database/commands/rules.rs`
- `src/pgrust/database/relation_refs.rs`
- `src/pl/plpgsql/compile.rs`

Tests run:
- `cargo fmt`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-ottawa-v2-dml-cte scripts/cargo_isolated.sh check`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-ottawa-v2-dml-cte scripts/cargo_isolated.sh test --lib --quiet parse_select_with_writable`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-ottawa-v2-dml-cte scripts/cargo_isolated.sh test --lib --quiet parse_create_or_replace_rule`
- `scripts/run_regression.sh --test with --jobs 1 --port 55443 --results-dir /tmp/diffs/with_dml_rules_merge`

Remaining:
- Latest focused `with` regression: 169/312 matched, 143 mismatched, 0 timed out.
- Remaining data-modifying CTE gaps: EXPLAIN still binds writable CTEs through the read-only path; nested writable CTE errors still surface as the materialization guard in at least one subquery case.
- Remaining rule gaps: rule actions containing writable CTEs are rejected at rule creation; statement-level rewrite semantics are still approximated by row-level rule execution for some unconditional DO INSTEAD cases.
- Remaining MERGE gaps: MERGE source/action expressions with non-recursive CTEs still fail in source subqueries and row expansion casts; EXPLAIN output does not show PostgreSQL-style CTE producers/scans.
- Other unrelated `with` failures remain around SEARCH/CYCLE, recursive CTE validation text, recursive set-operation scopes, aggregate/CTE semantic-level errors, and output formatting/caret differences.
