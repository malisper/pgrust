Goal:
Investigate and fix statement timeouts reported in join.sql, memoize.sql,
subselect.sql, and tidscan.sql.

Key decisions:
Use PostgreSQL planner behavior as the reference: pull up IN sublinks before
planning derived queries, use merge/hash-joinable equality where PostgreSQL
does, and prefer index/hash shapes over repeated unique parameter probes.
Use sample on a live regression run when join.sql still timed out after the
planner fixes. The active profile showed the remaining time was not executor
join work: pgrust was eagerly formatting every pg_proc signature while preparing
row output for ordinary SELECT responses. PostgreSQL does not do that per row
description, so pgrust now builds wire catalog name/signature maps only when the
result column types can need them.

Files touched:
scripts/run_regression.sh
src/backend/optimizer/bestpath.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/path/mod.rs
src/backend/optimizer/setrefs.rs
src/backend/tcop/postgres.rs
src/pgrust/session.rs

Tests run:
scripts/cargo_isolated.sh build --bin pgrust_server
scripts/run_regression.sh --test join --timeout 240 --jobs 1 --port 55532 --results-dir /tmp/pgrust-timeout-join-samplefix --data-dir /tmp/pgrust-timeout-join-samplefix-data --skip-build
scripts/run_regression.sh --test subselect --timeout 240 --jobs 1 --port 55533 --results-dir /tmp/pgrust-timeout-subselect-samplefix --data-dir /tmp/pgrust-timeout-subselect-samplefix-data --skip-build
scripts/run_regression.sh --test tidscan --timeout 240 --jobs 1 --port 55534 --results-dir /tmp/pgrust-timeout-tidscan-samplefix --data-dir /tmp/pgrust-timeout-tidscan-samplefix-data --skip-build
scripts/run_regression.sh --test memoize --timeout 240 --jobs 1 --port 55535 --results-dir /tmp/pgrust-timeout-memoize-samplefix --data-dir /tmp/pgrust-timeout-memoize-samplefix-data --skip-build
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet typed_data_row_renders_regprocedure_with_proc_name

Remaining:
Focused regression files still have output mismatches unrelated to statement
timeouts. subselect.sql reaches the existing executor panic at
src/backend/executor/nodes.rs:9332 after the previously timed-out IN queries
complete.

Crash follow-up:
MERGE regression later exposed two separate crash paths. The source-side setrefs
panic came from binding MERGE source action Vars to the raw joined source RTEs
even though pgrust executes MERGE through a projected source subquery.
PostgreSQL's setrefs.c rewrites MERGE source Vars against the MERGE subplan
targetlist, so pgrust now retargets the source scope to the source projection
before joining it to the target.

The later pg_class heap tuple panic was caused by pgrust encoding aclitem[]
catalog columns with generic 4-byte array alignment while relcache decoded
pg_class.relacl with PostgreSQL's double alignment for arrays of double-aligned
elements. pgrust now follows that PostgreSQL array alignment rule for column
descriptors. heap_page_replace_tuple also now preserves on-page tuple layout
header bits while updating MVCC header fields.

Crash tests run:
scripts/cargo_isolated.sh test --lib --quiet array_column_alignment_follows_postgres_element_alignment
scripts/cargo_isolated.sh test --lib --quiet replace_tuple_preserves_tuple_layout_bits
scripts/cargo_isolated.sh test --lib --quiet pg_class_long_relacl_catalog_tuple_roundtrips
scripts/cargo_isolated.sh test --lib --quiet duplicate_table_grant_keeps_pg_class_tuple_layout
scripts/cargo_isolated.sh test --lib --quiet merge_updates_pg_class_through_auto_updatable_view
scripts/cargo_isolated.sh test --lib --quiet merge_inserts_from_unaliased_joined_source
scripts/cargo_isolated.sh test --lib --quiet merge_accepts_joined_source
scripts/run_regression.sh --test merge --timeout 300 --jobs 1 --port 59545 --results-dir /tmp/pgrust-merge-current-madrid-v4

Crash follow-up remaining:
merge.sql no longer crashes or times out locally; it completes with existing
output diffs: 542/641 queries matched, 99 mismatched.
