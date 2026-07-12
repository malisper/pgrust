CREATE EXTENSION IF NOT EXISTS vector;
SET enable_seqscan = off;

-- L2

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l2_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <-> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <-> (SELECT NULL::vector)) t2;
SELECT COUNT(*) FROM t;

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

-- inner product

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_ip_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <#> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <#> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- cosine

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_cosine_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <=> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> '[0,0,0]') t2;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- L1

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l1_ops);

INSERT INTO t (val) VALUES ('[1,2,4]');

SELECT * FROM t ORDER BY val <+> '[3,3,3]';
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <+> (SELECT NULL::vector)) t2;

DROP TABLE t;

-- iterative

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l2_ops);

SET hnsw.iterative_scan = strict_order;
SET hnsw.ef_search = 1;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

SET hnsw.iterative_scan = relaxed_order;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

TRUNCATE t;
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

RESET hnsw.iterative_scan;
RESET hnsw.ef_search;
DROP TABLE t;

-- duplicates: the in-memory build merges exact duplicates into one element
-- (up to 10 heap tids); duplicates must not be flushed as orphan tuples.
-- 1000-dim rows make each element+neighbor pair fill its own page, so the
-- index size pins the element count independent of random levels.

CREATE TABLE t (id int, val vector(1000));
INSERT INTO t SELECT i, ('[' || repeat('7,', 999) || '7]')::vector FROM generate_series(1, 10) i;
CREATE INDEX dup_idx ON t USING hnsw (val vector_l2_ops);
SELECT pg_relation_size('dup_idx') / current_setting('block_size')::int AS pages;
SELECT COUNT(*) FROM (SELECT id FROM t ORDER BY val <-> ('[' || repeat('7,', 999) || '7]')::vector LIMIT 20) s;
DROP TABLE t;

-- duplicates mixed with distinct values

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[1,1,1]'), ('[1,1,1]'), ('[2,2,2]'), ('[1,1,1]'), ('[3,3,3]');
CREATE INDEX ON t USING hnsw (val vector_l2_ops);
SELECT val FROM t ORDER BY val <-> '[1,1,1]';
SELECT COUNT(*) FROM (SELECT val FROM t ORDER BY val <-> '[0,0,0]') s;
DROP TABLE t;

-- rescan: nested-loop inner index scans re-enter hnswrescan

CREATE TABLE q (qv vector(3));
INSERT INTO q VALUES ('[0,0,0]'), ('[1,2,3]'), ('[2,2,2]');
CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]');
CREATE INDEX ON t USING hnsw (val vector_l2_ops);
SELECT q.qv, s.val FROM q, LATERAL (SELECT val FROM t ORDER BY val <-> q.qv LIMIT 2) s;
SET hnsw.iterative_scan = relaxed_order;
SELECT q.qv, s.val FROM q, LATERAL (SELECT val FROM t ORDER BY val <-> q.qv LIMIT 2) s;
RESET hnsw.iterative_scan;
DROP TABLE q;
DROP TABLE t;

-- unlogged

CREATE UNLOGGED TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);
CREATE INDEX ON t USING hnsw (val vector_l2_ops);

SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DROP TABLE t;

-- options

CREATE TABLE t (val vector(3));
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 1);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 101);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 3);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (ef_construction = 1001);
CREATE INDEX ON t USING hnsw (val vector_l2_ops) WITH (m = 16, ef_construction = 31);
DROP TABLE t;

SHOW hnsw.ef_search;
SET hnsw.ef_search = 0;
SET hnsw.ef_search = 1001;

SHOW hnsw.iterative_scan;
SET hnsw.iterative_scan = on;

SHOW hnsw.max_scan_tuples;
SET hnsw.max_scan_tuples = 0;

SHOW hnsw.scan_mem_multiplier;
SET hnsw.scan_mem_multiplier = 0;
SET hnsw.scan_mem_multiplier = 1001;

-- dimensions

CREATE TABLE t (val vector(2000));
CREATE INDEX ON t USING hnsw (val vector_l2_ops);
DROP TABLE t;

CREATE TABLE t (val vector(2001));
CREATE INDEX ON t USING hnsw (val vector_l2_ops);
DROP TABLE t;
