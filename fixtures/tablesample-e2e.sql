-- TABLESAMPLE end-to-end: BERNOULLI/SYSTEM with REPEATABLE seeds are
-- deterministic (TID/block hashing against the seed), so results byte-diff
-- vs C 18.3. Non-REPEATABLE forms only appear under aggregates whose result
-- is seed-independent at the 0/100 limits.
CREATE TABLE ts_t (id int, val text);
INSERT INTO ts_t SELECT g, 'val_' || g FROM generate_series(1, 10000) g;
CREATE TABLE ts_small (id int);
INSERT INTO ts_small SELECT g FROM generate_series(1, 50) g;
CREATE TABLE ts_empty (id int);
ANALYZE ts_t;
ANALYZE ts_small;
ANALYZE ts_empty;

SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (100);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (0);
SELECT count(*) FROM ts_t TABLESAMPLE SYSTEM (100);
SELECT count(*) FROM ts_t TABLESAMPLE SYSTEM (0);
SELECT count(*) FROM ts_empty TABLESAMPLE BERNOULLI (50) REPEATABLE (1);
SELECT count(*) FROM ts_empty TABLESAMPLE SYSTEM (50) REPEATABLE (1);

SELECT * FROM ts_small TABLESAMPLE BERNOULLI (50) REPEATABLE (0);
SELECT * FROM ts_small TABLESAMPLE BERNOULLI (50) REPEATABLE (2);
SELECT * FROM ts_small TABLESAMPLE SYSTEM (100) REPEATABLE (0);
SELECT id FROM ts_t TABLESAMPLE BERNOULLI (1) REPEATABLE (0) ORDER BY id;
SELECT id FROM ts_t TABLESAMPLE BERNOULLI (1.5) REPEATABLE (42) ORDER BY id;
SELECT count(*), sum(id) FROM ts_t TABLESAMPLE SYSTEM (30) REPEATABLE (0);
SELECT count(*), sum(id) FROM ts_t TABLESAMPLE SYSTEM (30) REPEATABLE (7.5);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (50) REPEATABLE (-1);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (50) REPEATABLE (1 + 2);

-- alias, qual, projection over the sampled rel
SELECT x.id, x.val FROM ts_t AS x TABLESAMPLE BERNOULLI (2) REPEATABLE (0) WHERE x.id % 2 = 0 ORDER BY x.id;
SELECT count(val) FROM ts_t TABLESAMPLE SYSTEM (25) REPEATABLE (3) WHERE id > 5000;

-- joins with a sampled rel
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (10) REPEATABLE (0) t JOIN ts_small s ON t.id = s.id;
SELECT count(*) FROM ts_small s JOIN ts_t TABLESAMPLE SYSTEM (50) REPEATABLE (1) t ON t.id = s.id;

-- rescan determinism: sampled rel on the inner side of a nestloop
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SELECT count(*) FROM ts_small s, ts_t TABLESAMPLE BERNOULLI (10) REPEATABLE (5) t WHERE t.id = s.id;
RESET enable_hashjoin;
RESET enable_mergejoin;

-- subquery + group by over samples
SELECT count(*) FROM (SELECT id FROM ts_t TABLESAMPLE BERNOULLI (20) REPEATABLE (9)) sub;
SELECT id % 10 AS bucket, count(*) FROM ts_t TABLESAMPLE SYSTEM (40) REPEATABLE (2) GROUP BY 1 ORDER BY 1;

-- EXPLAIN shapes
EXPLAIN (COSTS OFF) SELECT * FROM ts_t TABLESAMPLE BERNOULLI (10);
EXPLAIN (COSTS OFF) SELECT * FROM ts_t TABLESAMPLE SYSTEM (10) REPEATABLE (0);
EXPLAIN (COSTS OFF) SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (50) REPEATABLE (1 + 2) WHERE id > 100;
EXPLAIN SELECT * FROM ts_t TABLESAMPLE BERNOULLI (10) REPEATABLE (0);
EXPLAIN SELECT * FROM ts_t TABLESAMPLE SYSTEM (10) REPEATABLE (0);
EXPLAIN (VERBOSE, COSTS OFF) SELECT id FROM ts_t TABLESAMPLE SYSTEM (10) REPEATABLE (0) WHERE id < 3;

-- negative cases
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (-1);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (101);
SELECT count(*) FROM ts_t TABLESAMPLE SYSTEM (102.5);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (NULL);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (50) REPEATABLE (NULL);
SELECT count(*) FROM ts_t TABLESAMPLE nosuchmethod (50);
SELECT count(*) FROM ts_t TABLESAMPLE abs (50);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI (50, 60);
SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI ();
CREATE VIEW ts_v AS SELECT * FROM ts_small;
SELECT count(*) FROM ts_v TABLESAMPLE BERNOULLI (50);
SELECT count(*) FROM (VALUES (1)) v(x) TABLESAMPLE BERNOULLI (50);

-- view over TABLESAMPLE round-trips through ruleutils
CREATE VIEW ts_sampled_v AS
  SELECT id FROM ts_t TABLESAMPLE BERNOULLI (5.5) REPEATABLE (1) WHERE id < 100;
SELECT pg_get_viewdef('ts_sampled_v'::regclass);
SELECT pg_get_viewdef('ts_sampled_v'::regclass, true);
SELECT * FROM ts_sampled_v ORDER BY id;

-- matview sampling
CREATE MATERIALIZED VIEW ts_mv AS SELECT * FROM ts_small;
SELECT count(*) FROM ts_mv TABLESAMPLE BERNOULLI (100);
SELECT * FROM ts_mv TABLESAMPLE BERNOULLI (60) REPEATABLE (4) ORDER BY id;

DROP MATERIALIZED VIEW ts_mv;
DROP VIEW ts_sampled_v;
DROP VIEW ts_v;
DROP TABLE ts_t, ts_small, ts_empty;
