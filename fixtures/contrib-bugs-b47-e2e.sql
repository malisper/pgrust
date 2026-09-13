-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-47-misc-contrib (C 18.6 oracle vs pgrust). Each leg is one row of the
-- batch.
\set VERBOSITY verbose
CREATE DATABASE b47e2e TEMPLATE template0 ENCODING 'UTF8';
\c b47e2e
\set VERBOSITY verbose
-- fp-contrib-pgstattuple-pgstattuple#1 / -b1#1 / -pgstatindex#1: a RECORD
-- wrapper with a column definition list resolves through expectedDesc
CREATE EXTENSION pgstattuple;
CREATE TABLE b47_t(i int);
INSERT INTO b47_t VALUES (1);
CREATE FUNCTION b47_stat_record(regclass) RETURNS record AS '$libdir/pgstattuple', 'pgstattuplebyid_v1_5' LANGUAGE C STRICT;
SELECT * FROM b47_stat_record('b47_t') AS s(table_len bigint, tuple_count bigint, tuple_len bigint, tuple_percent float8, dead_tuple_count bigint, dead_tuple_len bigint, dead_tuple_percent float8, free_space bigint, free_percent float8);
SELECT * FROM b47_stat_record('b47_t') AS s(table_len text, tuple_count bigint);
CREATE FUNCTION b47_approx_record(regclass) RETURNS record AS '$libdir/pgstattuple', 'pgstattuple_approx_v1_5' LANGUAGE C STRICT;
SELECT table_len, approx_tuple_count FROM b47_approx_record('b47_t') AS x(table_len bigint, scanned_percent float8, approx_tuple_count bigint, approx_tuple_len bigint, approx_tuple_percent float8, dead_tuple_count bigint, dead_tuple_len bigint, dead_tuple_percent float8, approx_free_space bigint, approx_free_percent float8);
CREATE TABLE b47_g(a int[]);
CREATE INDEX b47_gi ON b47_g USING gin(a);
CREATE FUNCTION b47_gin_record(regclass) RETURNS record AS '$libdir/pgstattuple', 'pgstatginindex_v1_5' LANGUAGE C STRICT;
SELECT * FROM b47_gin_record('b47_gi') AS s(version integer, pending_pages integer, pending_tuples bigint);
CREATE INDEX b47_ti ON b47_t(i);
CREATE FUNCTION b47_bt_record(regclass) RETURNS record AS '$libdir/pgstattuple', 'pgstatindexbyid_v1_5' LANGUAGE C STRICT;
SELECT * FROM b47_bt_record('b47_ti') AS s(version integer, tree_level integer, index_size bigint);
-- fp-contrib-pgstattuple-pgstattuple#2: dropped attributes of a named
-- composite result are NULL, not looked up
CREATE TYPE b47_stat_pair AS (table_len bigint, discarded bigint);
ALTER TYPE b47_stat_pair DROP ATTRIBUTE discarded;
CREATE FUNCTION b47_stat_named(regclass) RETURNS b47_stat_pair AS '$libdir/pgstattuple', 'pgstattuplebyid_v1_5' LANGUAGE C STRICT;
SELECT * FROM b47_stat_named('b47_t');
-- fp-contrib-pg_logicalinspect-b1#1: same expectedDesc resolution (the
-- filename error proves the result type was accepted first)
CREATE EXTENSION pg_logicalinspect;
CREATE FUNCTION b47_snap_meta(text) RETURNS record AS '$libdir/pg_logicalinspect', 'pg_get_logical_snapshot_meta' LANGUAGE C STRICT;
CREATE FUNCTION b47_snap_info(text) RETURNS record AS '$libdir/pg_logicalinspect', 'pg_get_logical_snapshot_info' LANGUAGE C STRICT;
SELECT * FROM b47_snap_meta('bogus') AS t(magic int4, checksum int8, version int4);
SELECT * FROM b47_snap_meta('0-1.snap') AS t(magic int4, checksum int8, version int4);
SELECT * FROM b47_snap_info('bogus') AS t(state text);
-- fp-contrib-pgrowlocks-b1#1: rows are built through the declared columns'
-- input functions (BuildTupleFromCStrings)
CREATE EXTENSION pgrowlocks;
CREATE TABLE b47_rl(i int);
INSERT INTO b47_rl VALUES (1);
CREATE FUNCTION b47_locks_int(text) RETURNS TABLE(locked_row tid, locker xid, multi integer, xids xid[], modes text[], pids integer[]) AS '$libdir/pgrowlocks', 'pgrowlocks' LANGUAGE C STRICT;
CREATE FUNCTION b47_locks_txt(text) RETURNS TABLE(locked_row text, locker text, multi text, xids text, modes text, pids text) AS '$libdir/pgrowlocks', 'pgrowlocks' LANGUAGE C STRICT;
BEGIN;
SELECT * FROM b47_rl FOR UPDATE;
SELECT locked_row, multi, modes FROM pgrowlocks('b47_rl');
SELECT locked_row, locker ~ '^[0-9]+$' AS locker_ok, multi, xids ~ '^\{[0-9]+\}$' AS xids_ok, modes, pids ~ '^\{[0-9]+\}$' AS pids_ok FROM b47_locks_txt('b47_rl');
SELECT * FROM b47_rl FOR SHARE;
SELECT locked_row, multi, modes FROM pgrowlocks('b47_rl');
ROLLBACK;
BEGIN;
SELECT * FROM b47_rl FOR UPDATE;
SELECT locked_row, multi, modes FROM b47_locks_int('b47_rl');
ROLLBACK;
-- fp-contrib-pg_trgm-trgm_gist#1: gtrgm_distance extracts trigrams for any
-- strategy, then rejects the unsupported one
CREATE EXTENSION pg_trgm;
CREATE OPERATOR CLASS b47_gist_trgm FOR TYPE text USING gist AS
  OPERATOR 5 <-> (text, text) FOR ORDER BY pg_catalog.float_ops,
  FUNCTION 1 gtrgm_consistent(internal,text,smallint,oid,internal),
  FUNCTION 2 gtrgm_union(internal,internal),
  FUNCTION 3 gtrgm_compress(internal),
  FUNCTION 4 gtrgm_decompress(internal),
  FUNCTION 5 gtrgm_penalty(internal,internal,internal),
  FUNCTION 6 gtrgm_picksplit(internal,internal),
  FUNCTION 7 gtrgm_same(gtrgm,gtrgm,internal),
  FUNCTION 8 (text,text) gtrgm_distance(internal,text,smallint,oid,internal),
  FUNCTION 10 gtrgm_options(internal),
  STORAGE gtrgm;
CREATE TABLE b47_ta(t text);
INSERT INTO b47_ta VALUES ('abc'), ('xyz');
CREATE INDEX b47_ta_idx ON b47_ta USING gist(t b47_gist_trgm);
SET enable_seqscan = off;
SELECT t FROM b47_ta ORDER BY t <-> '.' LIMIT 1;
SELECT t FROM b47_ta ORDER BY t <-> '[' LIMIT 1;
SELECT t FROM b47_ta ORDER BY t <-> 'abc' LIMIT 1;
RESET enable_seqscan;
-- fp-contrib-pg_trgm-trgm_gist#2: signature bits hash CPTRGM's native int
CREATE EXTENSION pageinspect;
CREATE TABLE b47_trgm(t text);
INSERT INTO b47_trgm SELECT 'abc' FROM generate_series(1, 10000);
CREATE INDEX b47_trgm_idx ON b47_trgm USING gist(t gist_trgm_ops);
SELECT itemoffset, encode(key_data, 'hex') FROM gist_page_items_bytea(get_raw_page('b47_trgm_idx', 0)) ORDER BY itemoffset LIMIT 2;
SET enable_seqscan = off;
SELECT count(*) FROM b47_trgm WHERE t % 'abc';
SELECT count(*) FROM b47_trgm WHERE t LIKE '%abc%';
RESET enable_seqscan;
-- fp-contrib-pg_trgm-trgm_op#1 / -b1#2: the trigram array size guard is
-- 54000 without palloc's DETAIL
SELECT similarity(repeat('!', 357913941), 'a');
-- fp-contrib-pg_trgm-trgm_op#2: the positional trigram array is bounded by
-- MaxAllocSize
SELECT word_similarity(repeat('a', 134217727), '');
-- fp-contrib-pg_prewarm-b1#1: 'buffer' rejects another session's temp table
-- before reading any block, so an empty relation still errors
CREATE EXTENSION pg_prewarm;
CREATE EXTENSION dblink;
SELECT dblink_connect('b47c', 'dbname=' || current_database() || ' port=' || current_setting('port') || ' host=' || current_setting('unix_socket_directories') || ' user=' || current_user);
SELECT dblink_exec('b47c', 'CREATE TEMP TABLE b47_pw_empty(i int); CREATE TEMP TABLE b47_pw_full(i int); INSERT INTO b47_pw_full SELECT generate_series(1, 10)');
SELECT e, f FROM dblink('b47c', 'SELECT ''b47_pw_empty''::regclass::oid, ''b47_pw_full''::regclass::oid') AS t(e oid, f oid) \gset
SELECT pg_prewarm(:e::regclass, 'buffer');
SELECT pg_prewarm(:e::regclass, 'read');
SELECT pg_prewarm(:e::regclass, 'prefetch');
SELECT pg_prewarm(:f::regclass, 'buffer');
SELECT pg_prewarm(:f::regclass, 'read');
SELECT pg_prewarm(:f::regclass, 'prefetch');
SELECT dblink_disconnect('b47c');
