-- fp-cache-relcache-p1#1: a non-mapped catalog rewritten between retained
-- changes; decoding rebuilds its relcache entry under the historic snapshot
-- and must address the current relfilenode. Expected captured from C 18.6.
\set VERBOSITY verbose
CREATE TABLE b28_dec(a int);
SELECT slot_name FROM pg_create_logical_replication_slot('b28_slot', 'test_decoding');
INSERT INTO b28_dec VALUES (1);
VACUUM FULL pg_catalog.pg_namespace;
REINDEX TABLE pg_catalog.pg_namespace;
CHECKPOINT;
CREATE SCHEMA b28_s;
CREATE TABLE b28_s.t(b int);
INSERT INTO b28_s.t VALUES (2);
\c
SELECT data FROM pg_logical_slot_get_changes('b28_slot', NULL, NULL, 'include-xids', '0');
SELECT pg_drop_replication_slot('b28_slot');
DROP TABLE b28_dec;
DROP SCHEMA b28_s CASCADE;
