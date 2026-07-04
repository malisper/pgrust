-- DISTINCT ON matrix: parser (transformDistinctOnClause), planner
-- (create_final_distinct_paths hasDistinctOn legs), and the
-- examine_simple_variable subquery/CTE drill. EXPLAIN and results must be
-- byte-identical vs C 18.3.
-- Envelope: int keys only (no text hash keys); rows < 30000 keeps ANALYZE
-- deterministic; CTEs referenced twice so C materializes like we do.
SET compute_query_id = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;

CREATE TABLE don_t(a int, b int, c float8, d int);
INSERT INTO don_t SELECT g % 40, g % 7, (g % 13)::float8 / 7, g
  FROM generate_series(1, 10000) g;
CREATE TABLE don_u(k int, v int);
INSERT INTO don_u SELECT g, g % 40 FROM generate_series(1, 300) g;
ANALYZE don_t;
ANALYZE don_u;

-- parser + plain sorted-unique shapes
SELECT DISTINCT ON (a) a, b FROM don_t ORDER BY a, b, d LIMIT 5;
SELECT DISTINCT ON (a) a, b, d FROM don_t ORDER BY a, b DESC, d LIMIT 5;
SELECT DISTINCT ON (a, b) a, b, d FROM don_t ORDER BY a, b, d DESC LIMIT 7;
SELECT DISTINCT ON (b) b, a FROM don_t ORDER BY b DESC, a DESC LIMIT 4;
SELECT DISTINCT ON (a % 3) a % 3, d FROM don_t ORDER BY a % 3, d LIMIT 4;
-- no ORDER BY at all (row choice unspecified; count only)
SELECT count(*) FROM (SELECT DISTINCT ON (a) a, b FROM don_t) s;
-- ORDER BY longer than DISTINCT ON: sort by the more rigorous list
-- (int keys only: float8 sort keys hit the unported sortsupport loud)
SELECT DISTINCT ON (a) a, b, d FROM don_t ORDER BY a, d DESC, b LIMIT 5;

EXPLAIN SELECT DISTINCT ON (a) a, b FROM don_t ORDER BY a, b;
EXPLAIN SELECT DISTINCT ON (a) a, b, d FROM don_t ORDER BY a, b DESC, d;
EXPLAIN SELECT DISTINCT ON (a, b) a, b, d FROM don_t ORDER BY a, b, d DESC;
EXPLAIN SELECT DISTINCT ON (b) b, a FROM don_t ORDER BY b DESC, a DESC;
EXPLAIN SELECT DISTINCT ON (a % 3) a % 3, d FROM don_t ORDER BY a % 3, d;
EXPLAIN SELECT DISTINCT ON (a) a, b FROM don_t;
EXPLAIN SELECT DISTINCT ON (a) a, b FROM don_t ORDER BY a, b LIMIT 3;
EXPLAIN SELECT DISTINCT ON (a) * FROM don_t ORDER BY a, c;

-- errors: DISTINCT ON must be a prefix of ORDER BY
SELECT DISTINCT ON (b) a, b FROM don_t ORDER BY a, b;
SELECT DISTINCT ON (a, c) a, b, c FROM don_t ORDER BY a, b, c;

-- DISTINCT ON under GROUP BY / with aggregates
SELECT DISTINCT ON (b) b, count(*) FROM don_t GROUP BY a, b ORDER BY b, count(*) DESC LIMIT 5;
EXPLAIN SELECT DISTINCT ON (b) b, count(*) FROM don_t GROUP BY a, b ORDER BY b, count(*) DESC;

-- subquery drill: eqjoinsel sees the sub-select output column
EXPLAIN SELECT * FROM don_u JOIN (SELECT DISTINCT ON (a) a, b FROM don_t ORDER BY a, b) s
  ON don_u.v = s.a;
EXPLAIN SELECT * FROM don_u JOIN (SELECT a, min(d) md FROM don_t GROUP BY a) s
  ON don_u.v = s.a;
EXPLAIN SELECT * FROM don_u JOIN (SELECT DISTINCT a FROM don_t) s ON don_u.v = s.a;
SELECT count(*) FROM don_u JOIN (SELECT DISTINCT ON (a) a, b FROM don_t ORDER BY a, b) s
  ON don_u.v = s.a;

-- CTE drill (two same-level references keep C on the materialized plan;
-- sub-level CTE refs are the ctelevelsup loud; independent join keys keep
-- EC-derived implied equalities out — equivclass lane)
EXPLAIN WITH g AS (SELECT a, count(*) n FROM don_t GROUP BY a)
  SELECT * FROM don_u JOIN g ON don_u.v = g.a JOIN g g2 ON g2.a = don_u.k;
WITH g AS (SELECT a, count(*) n FROM don_t GROUP BY a)
  SELECT count(*) FROM don_u JOIN g ON don_u.v = g.a JOIN g g2 ON g2.a = don_u.k;

DROP TABLE don_t, don_u;
