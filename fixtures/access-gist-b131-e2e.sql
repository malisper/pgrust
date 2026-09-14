-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-131-backend-access-gist (C 18.6 oracle vs pgrust). Each leg is one
-- row of the batch (scripts/access-gist-b131-e2e.sh).
\set VERBOSITY verbose
CREATE DATABASE b131e2e TEMPLATE template0 ENCODING 'UTF8';
\c b131e2e
\set VERBOSITY verbose
-- fp-gist-gistget#1: an index-only scan hands back the fetched value in its
-- original (uncompressed) form, as C's gistFetchTuple heap_form_tuple does.
CREATE EXTENSION btree_gist;
CREATE TABLE b131_t(x text);
INSERT INTO b131_t SELECT repeat('a', 1500) || i FROM generate_series(1,50) i;
CREATE INDEX b131_t_i ON b131_t USING gist (x);
CREATE TABLE b131_t2(i int, x text);
INSERT INTO b131_t2 SELECT i, repeat('b', 1500) || i FROM generate_series(1,50) i;
CREATE INDEX b131_t2_i ON b131_t2 USING gist (i, x);
VACUUM ANALYZE b131_t;
VACUUM ANALYZE b131_t2;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT pg_column_size(x), pg_column_compression(x), length(x) FROM b131_t WHERE x = repeat('a',1500)||'7';
SELECT pg_column_size(x), pg_column_compression(x), length(x) FROM b131_t WHERE x = repeat('a',1500)||'7';
EXPLAIN (COSTS OFF) SELECT i, pg_column_size(x), pg_column_compression(x) FROM b131_t2 ORDER BY i <-> 7 LIMIT 2;
SELECT i, pg_column_size(x), pg_column_compression(x) FROM b131_t2 ORDER BY i <-> 7 LIMIT 2;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE b131_t;
DROP TABLE b131_t2;
-- fp-gist-b1#1: an open GiST scan owns a "GiST scan context" under
-- ExecutorState with the temporary context, the page data context (index-only
-- scans) and, from the second rescan on, the queue context as children.
CREATE TABLE b131_p(p point, v int);
INSERT INTO b131_p SELECT point(i,i), i FROM generate_series(1,100) i;
CREATE INDEX b131_p_i ON b131_p USING gist (p);
VACUUM ANALYZE b131_p;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_material = off;
CREATE VIEW b131_ctx AS
  WITH m AS MATERIALIZED (SELECT name, level, path FROM pg_backend_memory_contexts)
  SELECT c.name, p.name AS parent FROM m c JOIN m p ON p.level = c.level - 1 AND p.path = c.path[1:c.level-1]
  WHERE c.name LIKE 'GiST%';
BEGIN;
DECLARE c CURSOR FOR SELECT p FROM b131_p WHERE p <@ box '((0,0),(10,10))';
FETCH 1 FROM c;
SELECT * FROM b131_ctx ORDER BY 1, 2;
DECLARE d CURSOR FOR SELECT v FROM b131_p WHERE p <@ box '((0,0),(10,10))';
FETCH 1 FROM d;
SELECT * FROM b131_ctx ORDER BY 1, 2;
CLOSE c;
CLOSE d;
DECLARE e CURSOR FOR SELECT g, v FROM generate_series(1,3) g, LATERAL (SELECT v FROM b131_p WHERE p <@ box(point(0,0), point(g*3,g*3))) s;
FETCH 2 FROM e;
SELECT * FROM b131_ctx ORDER BY 1, 2;
FETCH 4 FROM e;
SELECT * FROM b131_ctx ORDER BY 1, 2;
ROLLBACK;
SELECT count(*) FROM b131_ctx;
DROP VIEW b131_ctx;
DROP TABLE b131_p;
