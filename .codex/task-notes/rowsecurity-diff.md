Goal:
Diagnose the pasted rowsecurity regression diff.

Key decisions:
The first mismatch was `CREATE POLICY ... AS UGLY`. PostgreSQL parses `AS IDENT`
and emits a custom "unrecognized row security option" error with a hint. pgrust
now matches this in the hand parser by returning a positioned detailed parse
error for unknown identifier policy options.

Other major diff buckets are compatibility gaps, not one cascade: `\dp` uses
`pg_catalog.pg_roles` and unqualified `polroles`, which the current psql shim
did not handle. The `\dp` permissions-list query is now handled directly in
`src/backend/tcop/postgres.rs`, including relation ACLs, column ACLs, and a
policies column assembled from `pg_policy` rows and role names. `\d` still omits
policy display; `pg_policies` exposes raw stored SQL instead of
PostgreSQL-style `pg_get_expr` deparse; TABLESAMPLE, partitioned-table parent
RLS, PREPARE/EXECUTE, COPY options/RLS, CTE materialization, MERGE, and some
view/security-invoker permission semantics are incomplete or unsupported.

Follow-up formatting pass:
EXPLAIN expression text now matches PostgreSQL more closely for top-level
function filters (`f_leak(b)` instead of `(f_leak(b))`), infix operator
nesting (`((a % 2) = 0)`), LIKE operator rendering (`~~`), and join condition
relation qualifiers. This does not address real plan-shape differences such as
extra Projection nodes or different join/index choices.

Error formatting now matches the rowsecurity FK/unique/security DDL cases:
`DROP POLICY` non-owner failures say "relation" while ALTER still says
"table"; FK and unique violations redact key values when RLS or missing
table-level SELECT would make those values unsafe to reveal.

Files touched:
`.codex/task-notes/rowsecurity-diff.md`
`src/backend/parser/gram.rs`
`src/backend/parser/tests.rs`
`src/backend/tcop/postgres.rs`
`src/backend/commands/explain.rs`
`src/backend/commands/tablecmds.rs`
`src/backend/executor/foreign_keys.rs`
`src/backend/executor/mod.rs`
`src/backend/executor/nodes.rs`
`src/backend/executor/permissions.rs`
`src/backend/executor/tests.rs`
`src/backend/parser/analyze/constraints.rs`
`src/pgrust/database/commands/policy.rs`
`src/pgrust/database/foreign_keys.rs`
`src/pgrust/database_tests.rs`

Tests run:
`scripts/cargo_isolated.sh test --lib --quiet parse_policy_statements`
`scripts/cargo_isolated.sh test --lib --quiet psql_permissions_query_handles_unqualified_polroles`
`scripts/cargo_isolated.sh test --lib --quiet explain_expr_matches_postgres_filter_formatting`
`scripts/cargo_isolated.sh test --lib --quiet explain_expr_renders_user_function_current_user_and_initplan`
`scripts/cargo_isolated.sh test --lib --quiet explain_expr_parenthesizes_boolean_clause_args`
`scripts/cargo_isolated.sh test --lib --quiet explain_expr_renders_scalar_array_op_with_typed_array_literal`
`scripts/cargo_isolated.sh test --lib --quiet drop_policy_non_owner_uses_relation_wording`
`scripts/cargo_isolated.sh test --lib --quiet rls_key_errors_hide_values_when_relation_rows_are_not_visible`
`scripts/cargo_isolated.sh test --lib --quiet unique_index_insert_rejects_duplicate_key`
`scripts/cargo_isolated.sh test --lib --quiet unique_include_constraint_uses_only_key_columns_for_enforcement_and_catalogs`
`scripts/cargo_isolated.sh test --lib --quiet foreign_keys_restrict_parent_updates_and_deletes`
`scripts/cargo_isolated.sh test --lib --quiet create_table_foreign_keys_are_enforced_and_persisted`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --test rowsecurity --timeout 120 --jobs 1` was
attempted, but stopped after it remained blocked on Cargo's shared artifact
directory lock behind other concurrent regression jobs. It was attempted again
after the formatting pass and stopped for the same lock reason.

Remaining:
The invalid `AS UGLY` first mismatch is fixed and committed. The `\dp`
`pg_roles`/unqualified-`polroles` hard error is fixed and committed. Other
candidates from this rowsecurity diff remain: policy display in `\d`,
pg_policies expression deparsing in `src/backend/utils/cache/system_views.rs`,
real planner-shape differences (Projection nodes, join/index choices, inherited
scan aliases), and broader unsupported-feature gaps listed above.
