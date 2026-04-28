Goal:
Fix remaining PostgreSQL domain regression mismatches, continuing from the partially implemented domain DDL/drop/type-identity patch.

Key decisions:
- Kept the existing CREATE/ALTER DOMAIN and dependency work in scope.
- Fixed display issues in the real rewrite/EXPLAIN formatting paths instead of adding tcop query rewrites.
- Preserved PostgreSQL error wording for composite-domain CHECK revalidation when ALTER TYPE changes a referenced field type.
- Left row-order differences alone for now because they come from pgrust UPDATE storage behavior versus PostgreSQL heap tuple append order.

Files touched:
- src/backend/rewrite/rules.rs
- src/backend/commands/tablecmds.rs
- src/backend/commands/explain.rs
- src/backend/executor/nodes.rs
- src/backend/executor/exec_expr.rs
- src/pgrust/database/commands/typecmds.rs
- src/pgrust/database_tests.rs
- Plus earlier domain/drop/array/catalog/parser files already in the worktree.

Tests run:
- cargo fmt
- PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh check
- PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet pg_get_ruledef_formats_update_rule_actions_with_composite_fields
- PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet explain_verbose_update_composite_fields_uses_scan_projection
- PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet explain_verbose_update_where_false_is_accepted
- PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet alter_domain_rejects_unsupported_derived_type_columns
- scripts/run_regression.sh --test domain --jobs 1 --timeout 180 --port 55627 --results-dir /tmp/diffs/domain-after-alter-error

Remaining:
- Latest domain regression: /tmp/diffs/domain-after-alter-error, 499/507 queries matched, 8 mismatches, 98 diff lines.
- 4 mismatches are unordered SELECT output after UPDATE on composite-domain tables; PostgreSQL heap order places updated tuples later, pgrust currently behaves like in-place update.
- 4 mismatches are dposinta[]: array-of-domain-over-array prints as multidimensional arrays, pg_typeof((f1[1])[1]) stays dposinta instead of posint, and the parenthesized assignment syntax error caret differs.
