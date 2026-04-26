Goal:
Implement the remaining create_view/rules deparser plan: stable RTE metadata, canonical view SQL, pg_get_ruledef, SRF rowtype, restrict-view GUC, and cascade notice behavior.

Key decisions:
- Added RangeTblEref plus alias_preserves_source_names to RangeTblEntry.
- CREATE VIEW now stores rendered analyzed Query SQL instead of raw query_sql.
- View deparser now has set-operation rendering, function/VALUES alias rendering, join alias/parentheses handling, and deparse-time alias column collision names for user-aliased duplicate relations.
- Stored view SQL is parser-friendly canonical SQL: display semicolons are omitted, deparser keywords are lowercased outside quoted strings/identifiers, and WITH-clause views temporarily keep their original query text because analyzed Query does not retain CTE structure yet.
- View display deparse now renders simple base-table columns unqualified when unambiguous, system columns like ctid/tableoid by name, non-public relations schema-qualified, and builtin upper/lower with their real function names.
- pg_get_ruledef is wired through pg_proc, builtin lookup, executor dispatch, and CatalogLookup::rewrite_rows.
- restrict_nonsystem_relation_kind is accepted and enforced in the Database autocommit execution path for SELECT/INSERT.

Files touched:
- src/include/nodes/parsenodes.rs
- src/backend/parser/analyze/query.rs
- src/backend/parser/analyze/scope.rs
- src/backend/rewrite/views.rs
- src/pgrust/database/commands/create.rs
- src/backend/executor/exec_expr.rs
- src/include/catalog/pg_proc.rs
- src/backend/parser/analyze/functions.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/session.rs
- optimizer/cache support files

Tests run:
- scripts/cargo_isolated.sh check
- cargo test --lib --quiet bootstrap_rows_have_unique_oids -- --nocapture
- scripts/run_regression.sh --jobs 1 --timeout 300 --test create_view --port 55435
- scripts/run_regression.sh --jobs 1 --timeout 300 --test create_view --port 55436
- CI-focused cargo tests from attached logs:
  psql_get_viewdef_query_returns_return_rule_sql,
  psql_get_viewdef_query_accepts_regclass_literal,
  create_view_for_update_of_renders_view_definition,
  create_view_selects_and_persists_rewrite_rule,
  auto_view_errors_preserve_postgres_distinct_with_and_hint_text,
  auto_view_errors_preserve_postgres_column_specific_text,
  create_view_supports_check_option_and_or_replace,
  information_schema_view_metadata_tracks_updatable_views,
  nested_views_and_pg_views_work,
  dependent_views_track_relation_rename_and_set_schema,
  set_operation_inputs_expand_views,
  pg_get_viewdef_returns_canonical_view_query,
  build_plan_partial_derived_table_column_aliases_preserve_suffix
- scripts/cargo_isolated.sh check

Remaining:
- Rebuild/rerun create_view after the final alias collision patch to verify vv2/vv3/vv4 ambiguity is fixed.
- restrict_nonsystem_relation_kind still needs enforcement in the explicit transaction/session read-only path.
- SRF rowtype dropped-column execution errors and dependency tracking are not complete.
- Whole-row/composite equality deparse still falls back to debug output in some cases.
- Rule pretty formatting is functional but not PostgreSQL-shaped yet.
- Cascade notice aggregation/order is not yet normalized.
