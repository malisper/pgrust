Goal:
Investigate why the window regression's initial CREATE TEMPORARY TABLE empsalary failed with "catalog error".

Key decisions:
The initial symptom is stale pg_temp_1 catalog rows in the post_create_index regression base, not window semantics or the empsalary column types. Comparing with PostgreSQL showed two relevant differences:
- PostgreSQL initializes temp namespaces before temp relation creation and removes temp-namespace contents on backend cleanup by deleting objects dependent on the temp namespace, while keeping the namespace rows.
- PostgreSQL rejects REINDEX TABLE/INDEX CONCURRENTLY when the target is a system catalog heap or an index on a system catalog heap. pgrust was letting REINDEX TABLE CONCURRENTLY pg_class rewrite pg_class indexes, leaving system catalog btree metapages zeroed. That made later temp cleanup fail before it could remove stale rows.

Implemented PG-aligned behavior: early temp namespace initialization, namespace-scoped stale temp cleanup, backend/DISCARD temp cleanup error propagation, temp CREATE INDEX CONCURRENTLY treated as non-concurrent, and concurrent system-catalog REINDEX rejection/filtering.

Files touched:
src/backend/tcop/postgres.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/index.rs
src/pgrust/database/temp.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs
.codex/task-notes/window-temp-table-catalog-error.md

Tests run:
scripts/run_regression.sh --timeout 30 --test window
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet reindex_catalog_table_and_index_concurrently_reject_without_corrupting_catalogs
scripts/cargo_isolated.sh test --lib --quiet reindex_system_concurrently_and_other_database_reject_before_work
scripts/cargo_isolated.sh test --lib --quiet temp_namespace_reuse_cleans_stale_relations_before_create_table_lowering
scripts/cargo_isolated.sh test --lib --quiet create_index_concurrently_on_temp_table_uses_nonconcurrent_catalog_cleanup
scripts/cargo_isolated.sh test --lib --quiet terminate_message_removes_backend_temp_relations
scripts/cargo_isolated.sh test --lib --quiet discard_temp_and_all_reset_session_owned_state
scripts/cargo_isolated.sh test --lib --quiet temp_tables_are_removed_on_client_cleanup

Remaining:
window regression no longer fails at CREATE TEMPORARY TABLE empsalary. It still times out later around the row_number subquery run-condition section, which is a separate executor/performance issue.
