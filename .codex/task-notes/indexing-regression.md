Goal:
Reduce `/tmp/diffs/indexing.diff` and `/tmp/diffs/create_index.diff` toward PostgreSQL-compatible output, without updating expected files except intentional skips.

Key decisions:
- Use `../postgres` regression expected files as the behavioral reference when the implementation plan and expected output disagree.
- Keep `tenk1_unique1` out of `scripts/test_setup_pgrust.sql`; isolated `brin` now depends on `create_index`.
- Implement contained SQL-visible fixes first: geometry subscripts/operators, partitioned index metadata/deparse, ALTER COLUMN TYPE on partition roots, attach row remapping, drop-column detach metadata, and error text.
- Render built-in collation/opclass names from bootstrap catalog rows inside `pg_get_indexdef` to avoid recursive catalog scans during catalog queries.

Files touched:
- `scripts/run_regression.sh`
- `scripts/test_setup_pgrust.sql`
- Geometry/binder/executor/planner files under `src/backend/parser/analyze`, `src/backend/executor`, `src/backend/optimizer`, `src/include`
- Catalog/storage files under `src/backend/catalog`
- DDL command files under `src/pgrust/database/commands`
- `src/backend/tcop/postgres.rs`
- `src/backend/utils/sql_deparse.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh check`
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet create_index_on_partitioned_table_`
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet create_index_on_partitioned_table_builds_index_tree`
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet partitioned_key_coverage_checks_fire_for_root_partition_of_and_attach_partition`
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet comment_on_missing_index_reports_relation_does_not_exist`
- Earlier focused tests for geometry subscripts, point-in-circle, EXPLAIN geometry subscript rendering, ALTER COLUMN TYPE, attach dropped-column remapping, and detached partition column locality.
- `scripts/run_regression.sh --test indexing --port 56653 --results-dir /tmp/pgrust_indexing_fix4`
- `bash -n scripts/run_regression.sh`
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh build --bin pgrust_server`
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh check`
- `PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh test --lib --quiet partitioned_key_coverage_checks_fire_for_root_partition_of_and_attach_partition`
- `PGRUST_TARGET_SLOT=1 scripts/run_regression.sh --test indexing --port 56764 --results-dir /tmp/pgrust_indexing_no_timeout_probe2`
- `PGRUST_TARGET_SLOT=1 scripts/run_regression.sh --skip-build --test create_index --port 56766 --results-dir /tmp/pgrust_create_index_no_timeout_probe`

Remaining:
- `indexing` no longer times out with the long-file regression budget and catalog pruning/cache fixes: latest run matched 532/570 with 320 diff lines.
- `create_index` no longer times out: latest run matched 560/687 with 1734 diff lines.
- Earlier non-timeout diff still shows missing child indexes before `ALTER TABLE ... ATTACH PARTITION` in one `CREATE INDEX ONLY` scenario.
- Large remaining groups include partitioned opclass/drop dependency traversal, dropped-column index mapping, partitioned PK/UNIQUE/EXCLUDE inheritance and enforcement, replica identity/partition detach behavior, and REINDEX/concurrent index behavior.
- `create_index` still needs the larger planner/access-method work for GiST/GIN/BRIN/SP-GiST path generation and remaining geometry operator/index semantics.
