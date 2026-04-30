Goal:
Investigate high memory use in the psql regression test.

Key decisions:
The regression harness memory sampler only read /proc, so macOS runs wrote
peak_rss_kb=0. Added a ps fallback.

The high memory is server-side, not psql output buffering. A focused run of
scripts/run_regression.sh --skip-build --test psql on port 55631 timed out at
\df has_database_privilege oid text - and /usr/bin/time -l reported about
1.97 GB maximum resident set size. The psql client stayed around 10 MB and
output/psql.out was about 130 KB while the server RSS reached about 1.6 GB.

The hidden SQL behind the psql \df command joins pg_proc to pg_namespace and
three pg_type aliases through p.proargtypes[0..2]. Running just that hidden
query with psql -E succeeded but left the server near 1.9 GB RSS.

Smaller repros against the same regression data:
- pg_proc proname regex only: server grew from about 194 MB to 223 MB.
- pg_proc plus three pg_type left joins and t2.typname is null: about 555 MB.
- full pg_namespace/pg_type left-join shape: about 1.75 GB and slow.

Files touched:
scripts/run_regression.sh

Tests run:
bash -n scripts/run_regression.sh

Remaining:
Likely optimizer/executor issue around psql catalog describe queries: pgrust
appears to materialize too much of the outer-join catalog shape before using
the selective pg_proc.proname and argument filters. Investigate predicate
placement/pushdown for left joins and hash/nested-loop join planning around
pg_proc.proargtypes subscripts.

Implemented fix:
Runtime btree index argument checks now recurse through ArraySubscript, so
p.proargtypes[n] can be parameterized and used as an IndexScan/IndexOnlyScan
runtime key. The pg_type table name now resolves to the physical pg_type
catalog relation, including the qualified pg_catalog.pg_type name, before the
synthetic values-view fallback. This lets psql catalog-describe queries use
pg_type_oid_index and Memoize instead of building large pg_type VALUES joins.

Files touched:
scripts/run_regression.sh
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/tests.rs
src/backend/parser/analyze/scope.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet planner_uses_runtime_index_key_for_array_subscript_join
cargo build --bin pgrust_server
manual hidden psql \df query with statement_timeout=5s against updated server:
  returned the expected two rows in 3.6s
scripts/run_regression.sh --test psql --timeout 60 --port 55660 --results-dir /tmp/pgrust-psql-memory-results-6 --data-dir /tmp/pgrust-psql-memory-data-6

Remaining:
The psql regression still times out later around the multiple COPY TO STDOUT
with output file section,
which is unrelated to the \df has_database_privilege memory spike. The latest
psql regression memory sampler recorded peak_rss_kb=277808, and both
\df has_database_privilege oid text variants completed with expected rows.

CI follow-up:
Unqualified pg_type stays on the synthetic dynamic system view, because enum
and domain DDL still expose fresh type rows there. Qualified pg_catalog.pg_type
uses the physical catalog relation so the psql describe query keeps the indexed
planning path.
