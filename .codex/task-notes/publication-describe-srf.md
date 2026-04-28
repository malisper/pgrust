Goal:
Fix publication `\dRp+` / table `\d+` describe failures caused by qualified catalog SRFs.

Key decisions:
Normalize `pg_catalog.` prefixes when choosing native builtin table-function paths, so `pg_catalog.generate_series` uses the same SRF plan as `generate_series`.

Files touched:
`src/backend/parser/analyze/functions.rs`, `src/backend/parser/analyze/expr/targets.rs`, `src/backend/parser/analyze/scope.rs`, `src/backend/executor/tests.rs`, `src/backend/tcop/postgres.rs`.

Tests run:
`scripts/cargo_isolated.sh test --lib --quiet generate_series_accepts_pg_catalog_qualification`
`scripts/cargo_isolated.sh test --lib --quiet psql_publication_detail_footer_queries_run_via_native_sql`
`scripts/cargo_isolated.sh test --lib --quiet psql_publication_detail_tables_query_reports_column_lists`
`scripts/cargo_isolated.sh check`
Live psql reproduction for `\dRp+`, `\d+`, and `select * from pg_catalog.generate_series(1, 2)`.

Remaining:
Full `publication` regression was not rerun; unrelated publication diffs remain.
