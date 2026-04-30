Goal:
Fix regressions reported for create_procedure, delete, drop_operator, index_including_gist, numa, select.
Key decisions:
- Added pg_numa_available plus pg_shmem_allocations_numa synthetic view/SRF behavior for NUMA skip path.
- Enabled DELETE target aliases and PostgreSQL-style alias reference error/hint.
- Fixed CREATE/DROP OPERATOR partner catalog maintenance for shell replacement, duplicate partner updates, and dangling references.
- Suppressed no-op COMMIT/ROLLBACK warnings in SQL procedure body execution when no transaction is active.
- Kept SQL-standard CREATE PROCEDURE bodies together in the simple-query splitter.
- Matched pg_get_functiondef procedure output for quoted bodies, SQL-standard INSERT bodies, and empty BEGIN ATOMIC bodies.
- Allowed SELECT planning to use GiST exclusion indexes for index scans.
- Replayed create_index before single-file select regression runs.
- Prevented unsafe full scans of unrelated partial indexes when seqscan is disabled.
- Avoided unsafe parameterized index-only scans when root target attrs were not proven covered.
- Matched PostgreSQL bitmap OR recheck/filter shape for select's partial-index case.
Files touched:
- scripts/run_regression.sh
- src/backend/executor/exec_expr.rs
- src/backend/executor/srf.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/parser/analyze/functions.rs
- src/backend/parser/analyze/modify.rs
- src/backend/parser/analyze/system_views.rs
- src/backend/parser/tests.rs
- src/backend/tcop/postgres.rs
- src/backend/utils/cache/system_view_registry.rs
- src/include/catalog/pg_proc.rs
- src/include/nodes/primnodes.rs
- src/pgrust/database/commands/create.rs
- src/pgrust/database/commands/operator.rs
- src/pgrust/database/commands/routine.rs
- src/pgrust/session.rs
Tests run:
- cargo check --lib (direct target dir, RUSTC_WRAPPER=) passes with existing unreachable-pattern warnings.
- cargo test --lib --quiet parse_delete -- --nocapture passes.
- cargo test --lib --quiet split_simple_query_statements_keeps_sql_standard_procedure_body_together -- --nocapture passes.
- scripts/run_regression.sh --test create_procedure passes.
- scripts/run_regression.sh --test numa passes.
- scripts/run_regression.sh --test delete passes.
- scripts/run_regression.sh --test drop_operator passes.
- scripts/run_regression.sh --test index_including_gist passes.
- scripts/run_regression.sh --test select passes.
Remaining:
- None for the six requested regression files in focused reruns.
