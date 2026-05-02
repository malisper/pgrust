Goal:
Fix DML RETURNING OLD/NEW aliases, pseudo-row system columns, rule-rewritten RETURNING projection, and partition row movement metadata.

Key decisions:
- Preserve RETURNING WITH aliases in raw DML parse nodes instead of rewriting aliases during parsing.
- Bind RETURNING OLD/NEW as qualified pseudo relations backed by OUTER_VAR/INNER_VAR.
- Use explicit old/new tuple metadata during RETURNING projection so tableoid/ctid can differ across row movement.

Files touched:
- src/include/nodes/parsenodes.rs
- src/backend/parser/gram.rs
- src/backend/parser/analyze/modify.rs
- src/backend/parser/analyze/scope.rs
- src/backend/parser/analyze/expr/targets.rs
- src/backend/commands/tablecmds.rs
- src/backend/commands/upsert.rs
- src/pgrust/database/commands/rules.rs
- src/backend/rewrite/rules.rs
- src/backend/tcop/postgres.rs
- src/backend/parser/tests.rs
- src/pgrust/database_tests.rs

Tests run:
- CARGO_TARGET_DIR=/tmp/pgrust-target-kampala-v5-check cargo check --message-format=short
- CARGO_TARGET_DIR=/tmp/pgrust-target-kampala-v5-check cargo test --lib --quiet returning_with
- CARGO_TARGET_DIR=/tmp/pgrust-target-kampala-v5-check cargo test --lib --quiet returning_old_new
- CARGO_TARGET_DIR=/tmp/pgrust-target-kampala-v5-check cargo test --lib --quiet bind_insert_returning_alias_hides_base_table_name_for_star
- CARGO_TARGET_DIR=/tmp/pgrust-target-kampala-v5-check cargo test --lib --quiet view_rule_returning_projects_statement_old_new
- CARGO_TARGET_DIR=/tmp/pgrust-target-kampala-v5-check cargo test --lib --quiet partition_update_returning_old_new_system_columns
- PGRUST_STATEMENT_TIMEOUT=30 CARGO_TARGET_DIR=/tmp/pgrust-target-kampala-v5-check scripts/run_regression.sh --test returning --skip-build --timeout 240 --jobs 1 --port 56522 --results-dir /tmp/pgrust-returning-kampala-v5-rerun

Remaining:
- returning regression still fails: 114/150 queries matched, 593 diff lines.
- Copied current regression diff to /tmp/diffs/returning-kampala-v5.diff.
- Remaining gaps include joinview rule cardinality, EXPLAIN formatting, zero-column INSERT SELECT RETURNING syntax, schema-qualified target references in one UPDATE case, SQL function ruleutils star expansion, and routed INSERT ctid metadata.
