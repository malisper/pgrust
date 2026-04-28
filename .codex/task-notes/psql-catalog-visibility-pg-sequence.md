Goal:
Fix the psql regression buckets for missing catalog visibility functions and missing pg_catalog.pg_sequence.

Key decisions:
Implemented pg_sequence as a real bootstrap catalog relation with OID 2224 and MVCC-backed rows sourced from SequenceData.
Added PostgreSQL-compatible pg_*_is_visible builtins with catalog/search-path visibility checks and NULL-on-NULL/missing-OID behavior.
Kept the scope to the requested psql buckets; remaining psql diffs are separate protocol, access method, function, and catalog gaps.

Files touched:
src/include/catalog/pg_sequence.rs
src/include/catalog/{mod.rs,bootstrap.rs,pg_class.rs,pg_proc.rs}
src/backend/catalog/{bootstrap.rs,loader.rs,persistence.rs,rowcodec.rs,rows.rs}
src/backend/catalog/store/{heap.rs,storage.rs}
src/backend/utils/cache/{catcache.rs,visible_catalog.rs,lsyscache.rs,syscache.rs,relcache.rs}
src/backend/parser/analyze/{mod.rs,functions.rs}
src/backend/parser/tests.rs
src/backend/executor/exec_expr.rs
src/include/nodes/primnodes.rs
src/pgrust/database/{sequences.rs,commands/create.rs,commands/sequence.rs,commands/alter_column_identity.rs}
src/pgrust/session.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet pg_sequence_catalog_tracks_sequence_metadata
scripts/cargo_isolated.sh test --lib --quiet catalog_visibility_functions_cover_psql_describe_helpers
scripts/cargo_isolated.sh test --lib --quiet visible
scripts/cargo_isolated.sh test --lib --quiet pg_sequence
scripts/run_regression.sh --test psql --results-dir /tmp/pgrust_psql_regress_catalog_fix --timeout 120 --port 64903
scripts/cargo_isolated.sh test --lib --quiet pg_table_size_and_tablespace_location_helpers_work
scripts/run_regression.sh --test psql --results-dir /tmp/pgrust_psql_regress_helpers_fix --timeout 120 --port 56591

Remaining:
The targeted psql buckets are gone: catalog visibility function errors 0, missing pg_sequence errors 0.
The focused psql rerun timed out after 374/464 matched queries; current top remaining error buckets include pg_table_size, portal/cursor behavior, unsupported CREATE ACCESS METHOD / CREATE TABLE USING syntax, information_schema._pg_index_position, pg_parameter_acl, and pg_tablespace_location.
Follow-up commit ae6458bbd added pg_table_size(regclass), pg_tablespace_location(oid), and database data_dir propagation for executor contexts. The fresh psql rerun completed at 396/464 matched with pg_table_size and pg_tablespace_location errors both at 0.
