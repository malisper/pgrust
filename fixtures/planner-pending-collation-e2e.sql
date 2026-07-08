-- Differential fixture for the 18.5-pending planner/LIKE upstream fixes
-- (fe5d629 qual pushdown past grouping, a99bd8d+51652c4 nondeterministic
-- LIKE backslashes, d0bb49e exact-match indexqual). The in-pod C 18.4
-- oracle predates these fixes, so this fixture runs one-binary against a
-- frozen expected extracted from the upstream regression outputs.
CREATE COLLATION case_sensitive (provider = icu, locale = '');
CREATE COLLATION case_insensitive (provider = icu, locale = '@colStrength=secondary', deterministic = false);
CREATE COLLATION ignore_accents (provider = icu, locale = '@colStrength=primary;colCaseLevel=yes', deterministic = false);

-- check that an upper-level qual is not pushed down if its operator is from a
-- different btree opfamily than the subquery's grouping eqop
--
BEGIN;

CREATE TYPE t_rec AS (x numeric);
CREATE TEMP TABLE pdt (id int, a t_rec);
INSERT INTO pdt VALUES
  (1, ROW(1.00)::t_rec),
  (2, ROW(1.0)::t_rec),
  (3, ROW(2)::t_rec);

-- DISTINCT ON: conflict, qual stays in outer query
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT DISTINCT ON (a) id, a FROM pdt ORDER BY a, id) s
WHERE a *= ROW(1.0)::t_rec;

SELECT * FROM (SELECT DISTINCT ON (a) id, a FROM pdt ORDER BY a, id) s
WHERE a *= ROW(1.0)::t_rec;

-- Window function PARTITION BY: conflict, qual stays outside the WindowAgg
EXPLAIN (COSTS OFF)
SELECT * FROM (
  SELECT id, a, count(*) OVER (PARTITION BY a) AS cnt FROM pdt
) s
WHERE a *= ROW(1.0)::t_rec;

SELECT * FROM (
  SELECT id, a, count(*) OVER (PARTITION BY a) AS cnt FROM pdt
) s
WHERE a *= ROW(1.0)::t_rec;

-- Plain DISTINCT: conflict, qual stays in outer query
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT DISTINCT a FROM pdt) s WHERE a *= ROW(1.0)::t_rec;

-- (execution of the record *= filter above a grouping node is trimmed:
-- pre-existing executor fmgr result-mcx gap, tracked separately)


-- Positive: compatible opfamily, safe to push past the grouping
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT DISTINCT ON (a) id, a FROM pdt ORDER BY a, id) s
WHERE a = ROW(1.0)::t_rec;

SELECT * FROM (SELECT DISTINCT ON (a) id, a FROM pdt ORDER BY a, id) s
WHERE a = ROW(1.0)::t_rec;

-- Set operations: any operation other than UNION ALL groups rows by equality,
-- so the same opfamily-mismatch rules apply.
CREATE TEMP TABLE u1 (a t_rec);
CREATE TEMP TABLE u2 (a t_rec);
INSERT INTO u1 VALUES (ROW(1.00)::t_rec), (ROW(1.0)::t_rec);
INSERT INTO u2 VALUES (ROW(1.0)::t_rec);

-- UNION: conflict, qual stays in outer query
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a FROM u1 UNION SELECT a FROM u2) s
WHERE a *= ROW(1.0)::t_rec;

-- (execution of the record *= filter above a grouping node is trimmed:
-- pre-existing executor fmgr result-mcx gap, tracked separately)


-- INTERSECT: same
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a FROM u1 INTERSECT SELECT a FROM u2) s
WHERE a *= ROW(1.0)::t_rec;

-- (execution of the record *= filter above a grouping node is trimmed:
-- pre-existing executor fmgr result-mcx gap, tracked separately)


-- INTERSECT ALL: still groups
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a FROM u1 INTERSECT ALL SELECT a FROM u2) s
WHERE a *= ROW(1.0)::t_rec;

-- (execution of the record *= filter above a grouping node is trimmed:
-- pre-existing executor fmgr result-mcx gap, tracked separately)


-- UNION ALL of (UNION ...): an inner grouping node still exposes the
-- conflict to a qual pushed down through the outer UNION ALL.
EXPLAIN (COSTS OFF)
SELECT * FROM (
  (SELECT a FROM u1 UNION SELECT a FROM u2)
  UNION ALL
  SELECT a FROM u2
) s
WHERE a *= ROW(1.0)::t_rec;

-- (execution of the record *= filter above a grouping node is trimmed:
-- pre-existing executor fmgr result-mcx gap, tracked separately)


-- UNION ALL only: no grouping anywhere, pushdown remains allowed.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a FROM u1 UNION ALL SELECT a FROM u2) s
WHERE a *= ROW(1.0)::t_rec;

SELECT * FROM (SELECT a FROM u1 UNION ALL SELECT a FROM u2) s
WHERE a *= ROW(1.0)::t_rec;

ROLLBACK;

create type avg_rec as (x numeric);
-- A HAVING clause that uses an equality operator from a different opfamily
-- than the GROUP BY's eqop must NOT be pushed down to WHERE.
create temp table t_having (id int, a avg_rec);
insert into t_having values
  (1, row(1.0)::avg_rec),
  (2, row(1.00)::avg_rec),
  (3, row(2)::avg_rec);

-- the clause must stay in HAVING
explain (costs off)
select a, count(*) from t_having group by a having a *= row(1.0)::avg_rec;
-- (execution of the record *= filter above a grouping node is trimmed:
-- pre-existing executor fmgr result-mcx gap, tracked separately)


-- the clause must stay in HAVING
explain (costs off)
select a, count(*) from t_having group by a having a *= any (array[row(1.0)::avg_rec]);
-- (execution of the record *= filter above a grouping node is trimmed:
-- pre-existing executor fmgr result-mcx gap, tracked separately)


-- the clause can be pushed down to WHERE
explain (costs off)
select a, count(*) from t_having group by a having a = row(1.0)::avg_rec;
select a, count(*) from t_having group by a having a = row(1.0)::avg_rec;

drop table t_having;
drop type avg_rec;

-- Test WHERE-pushdown past a grouping layer (DISTINCT, DISTINCT ON, window
-- PARTITION BY) when the qual applies a different collation than the
-- grouping column's nondeterministic collation.  The qual would distinguish
-- rows the grouping considers equal, so it must NOT be pushed inside the
-- subquery.
CREATE TABLE pushdown_ci (id int, x text COLLATE case_insensitive);
INSERT INTO pushdown_ci VALUES (1, 'ABC'), (2, 'abc'), (3, 'def');

-- DISTINCT ON: conflict, qual stays in outer query
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT DISTINCT ON (x) id, x FROM pushdown_ci ORDER BY x, id) s
WHERE x = 'abc' COLLATE case_sensitive;

SELECT * FROM (SELECT DISTINCT ON (x) id, x FROM pushdown_ci ORDER BY x, id) s
WHERE x = 'abc' COLLATE case_sensitive;

-- Window function PARTITION BY: conflict, qual stays outside the WindowAgg
EXPLAIN (COSTS OFF)
SELECT * FROM (
  SELECT id, x, count(*) OVER (PARTITION BY x) AS cnt FROM pushdown_ci
) s
WHERE x = 'abc' COLLATE case_sensitive;

SELECT * FROM (
  SELECT id, x, count(*) OVER (PARTITION BY x) AS cnt FROM pushdown_ci
) s
WHERE x = 'abc' COLLATE case_sensitive;

-- Plain DISTINCT: conflict, qual stays in outer query
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT DISTINCT x FROM pushdown_ci) s
WHERE x = 'abc' COLLATE case_sensitive;

SELECT * FROM (SELECT DISTINCT x FROM pushdown_ci) s
WHERE x = 'abc' COLLATE case_sensitive;

-- Positive: matching collation, safe to push past the grouping
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT DISTINCT ON (x) id, x FROM pushdown_ci ORDER BY x, id) s
WHERE x = 'abc' COLLATE case_insensitive;

SELECT * FROM (SELECT DISTINCT ON (x) id, x FROM pushdown_ci ORDER BY x, id) s
WHERE x = 'abc' COLLATE case_insensitive;

-- Set operations: any operation other than UNION ALL groups rows by equality,
-- so the same collation-mismatch rules apply.
CREATE TABLE pushdown_ci2 (x text COLLATE case_insensitive);
INSERT INTO pushdown_ci2 VALUES ('abc');

-- UNION: conflict, qual stays in outer query
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT x FROM pushdown_ci UNION SELECT x FROM pushdown_ci2) s
WHERE x = 'abc' COLLATE case_sensitive;

SELECT * FROM (SELECT x FROM pushdown_ci UNION SELECT x FROM pushdown_ci2) s
WHERE x = 'abc' COLLATE case_sensitive;

-- INTERSECT: same
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT x FROM pushdown_ci INTERSECT SELECT x FROM pushdown_ci2) s
WHERE x = 'abc' COLLATE case_sensitive;

SELECT * FROM (SELECT x FROM pushdown_ci INTERSECT SELECT x FROM pushdown_ci2) s
WHERE x = 'abc' COLLATE case_sensitive;

-- INTERSECT ALL: still groups
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT x FROM pushdown_ci INTERSECT ALL SELECT x FROM pushdown_ci2) s
WHERE x = 'abc' COLLATE case_sensitive;

SELECT * FROM (SELECT x FROM pushdown_ci INTERSECT ALL SELECT x FROM pushdown_ci2) s
WHERE x = 'abc' COLLATE case_sensitive;

-- Negative: a function over a grouping column with a nondeterministic
-- collation, whose result is compared under no collation (an integer
-- comparison), can distinguish values the grouping considers equal.
-- PARTITION BY
EXPLAIN (COSTS OFF)
SELECT * FROM (
  SELECT id, x, count(*) OVER (PARTITION BY x) AS cnt FROM pushdown_ci
) s
WHERE ascii(x) = 97;

SELECT * FROM (
  SELECT id, x, count(*) OVER (PARTITION BY x) AS cnt FROM pushdown_ci
) s
WHERE ascii(x) = 97;

-- Same with DISTINCT
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT DISTINCT x FROM pushdown_ci) s WHERE ascii(x) = 97;

SELECT * FROM (SELECT DISTINCT x FROM pushdown_ci) s WHERE ascii(x) = 97;

-- Same with Set operations
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT x FROM pushdown_ci UNION SELECT x FROM pushdown_ci2) s
WHERE ascii(x) = 97;

SELECT * FROM (SELECT x FROM pushdown_ci UNION SELECT x FROM pushdown_ci2) s
WHERE ascii(x) = 97;

DROP TABLE pushdown_ci2;
DROP TABLE pushdown_ci;

SELECT 'AB' LIKE 'ab' COLLATE case_insensitive AS t;
SELECT 'AB' LIKE 'a\b' COLLATE case_insensitive AS t;
SELECT 'AB' LIKE '\ab' COLLATE case_insensitive AS t;
SELECT 'AB' LIKE '\a%' COLLATE case_insensitive AS t;
SELECT 'AB' LIKE '\a\%' COLLATE case_insensitive AS f;

-- literal backslash with nondeterministic collation (bug #19474)
SELECT 'back\slash' COLLATE ignore_accents LIKE 'back\slash%' ESCAPE '#';
SELECT 'aäb' COLLATE ignore_accents LIKE 'a#äb' ESCAPE '#' AS multibyte_escape;
SELECT 'a\äb' COLLATE ignore_accents LIKE 'a\äb%' ESCAPE '#' AS backslash_multibyte;
SELECT 'a\b%c' COLLATE ignore_accents LIKE 'a#\b#%%c' ESCAPE '#' AS mixed_escapes;
SELECT 'backslash' COLLATE ignore_accents LIKE 'back\\slash%';

CREATE TABLE test1ci (x text COLLATE case_insensitive);
INSERT INTO test1ci VALUES ('abc'), ('def'), ('ghi');
CREATE UNIQUE INDEX ON test1ci (x);
-- These queries should be able to use the index on test1ci.x:
SET enable_seqscan = off;
SET enable_indexonlyscan = off;
EXPLAIN (COSTS OFF)
SELECT * FROM test1ci WHERE x ~ '^abc$' COLLATE "C";
EXPLAIN (COSTS OFF)
SELECT * FROM test1ci WHERE x LIKE 'abc' COLLATE case_insensitive;
RESET enable_seqscan;
RESET enable_indexonlyscan;

-- Regex exact-match optimization should use an index even when the expression
-- and index have different collations, so long as the expression's collation
-- is deterministic.  This example tests what we want because the optimizer
-- does not perceive "C" collation (used by the system catalogs) as identical
-- to "POSIX" collation.
EXPLAIN (COSTS OFF)
SELECT * FROM pg_class WHERE relname ~ '^pg_class$' COLLATE "POSIX";
EXPLAIN (COSTS OFF)
SELECT * FROM pg_class WHERE relname LIKE 'pg\_class' COLLATE "POSIX";

DROP TABLE test1ci;
DROP COLLATION ignore_accents;
DROP COLLATION case_insensitive;
DROP COLLATION case_sensitive;
