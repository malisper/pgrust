-- Differential battery for the planner collation/uniqueness upstream fixes
-- that shipped in PostgreSQL 18.4 (b62f514, bed3ffb, e8fd5e5, 1132af2,
-- 5c214b5). Ported from the upstream regression tests
-- (collate.icu.utf8.sql / aggregates.sql); run through regress-diff.sh
-- against a C 18.4+ oracle.

CREATE COLLATION case_sensitive (provider = icu, locale = '');
CREATE COLLATION case_insensitive (provider = icu, locale = '@colStrength=secondary', deterministic = false);

CREATE TABLE test1cs (x text COLLATE case_sensitive);
CREATE TABLE test3cs (x text COLLATE case_sensitive);
INSERT INTO test1cs VALUES ('abc'), ('ABC'), ('def'), ('ghi');
INSERT INTO test3cs VALUES ('abc'), ('ABC'), ('def'), ('ghi');
CREATE UNIQUE INDEX ON test3cs (x);

--
-- b62f514: a unique index under one collation does not prove uniqueness
-- under another.
--

-- Ensure that we do not use inner-unique join execution
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM test1cs t1, test3cs t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

SELECT * FROM test1cs t1, test3cs t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

-- Ensure that left-join is not removed
EXPLAIN (COSTS OFF)
SELECT t1.* FROM test3cs t1
       LEFT JOIN test3cs t2 ON t1.x = t2.x COLLATE case_insensitive
ORDER BY 1;

SELECT t1.* FROM test3cs t1
       LEFT JOIN test3cs t2 ON t1.x = t2.x COLLATE case_insensitive
ORDER BY 1;

-- Ensure that self-join is not removed
EXPLAIN (COSTS OFF)
SELECT * FROM test3cs t1, test3cs t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

SELECT * FROM test3cs t1, test3cs t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

-- Ensure that semijoin is not reduced to innerjoin
EXPLAIN (COSTS OFF)
SELECT * FROM test3cs t1
  WHERE EXISTS (SELECT 1 FROM test3cs t2 WHERE t1.x = t2.x COLLATE case_insensitive)
ORDER BY 1;

SELECT * FROM test3cs t1
  WHERE EXISTS (SELECT 1 FROM test3cs t2 WHERE t1.x = t2.x COLLATE case_insensitive)
ORDER BY 1;

--
-- bed3ffb: a DISTINCT / GROUP BY / set-op on a subquery does not prove
-- uniqueness under a different collation.
--

-- Ensure that we do not use inner-unique join execution
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM test1cs t1, (SELECT DISTINCT x FROM test3cs) t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

SELECT * FROM test1cs t1, (SELECT DISTINCT x FROM test3cs) t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

-- Same with GROUP BY
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM test1cs t1, (SELECT x FROM test3cs GROUP BY x) t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

SELECT * FROM test1cs t1, (SELECT x FROM test3cs GROUP BY x) t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

-- Same with set-op
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM test1cs t1, (SELECT x FROM test3cs UNION SELECT x FROM test3cs) t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

SELECT * FROM test1cs t1, (SELECT x FROM test3cs UNION SELECT x FROM test3cs) t2
WHERE t1.x = t2.x COLLATE case_insensitive
ORDER BY 1, 2;

-- Ensure that left-join is not removed
EXPLAIN (COSTS OFF)
SELECT t1.* FROM test3cs t1
       LEFT JOIN (SELECT DISTINCT x FROM test3cs) t2 ON t1.x = t2.x COLLATE case_insensitive
ORDER BY 1;

SELECT t1.* FROM test3cs t1
       LEFT JOIN (SELECT DISTINCT x FROM test3cs) t2 ON t1.x = t2.x COLLATE case_insensitive
ORDER BY 1;

-- Ensure that semijoin is not reduced to innerjoin
EXPLAIN (COSTS OFF)
SELECT * FROM test3cs t1
  WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT x FROM test3cs) t2
                WHERE t1.x = t2.x COLLATE case_insensitive)
ORDER BY 1;

SELECT * FROM test3cs t1
  WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT x FROM test3cs) t2
                WHERE t1.x = t2.x COLLATE case_insensitive)
ORDER BY 1;

--
-- e8fd5e5 + 1132af2: HAVING-to-WHERE pushdown with nondeterministic
-- collations (including the simple-CASE form).
--

CREATE TABLE test3ci (x text COLLATE case_insensitive);
INSERT INTO test3ci VALUES ('abc'), ('ABC'), ('def'), ('ghi');

-- Negative: collation conflict, HAVING must not be pushed to WHERE
EXPLAIN (COSTS OFF)
SELECT x, count(*) FROM test3ci GROUP BY x HAVING x = 'abc' COLLATE case_sensitive;
SELECT x, count(*) FROM test3ci GROUP BY x HAVING x = 'abc' COLLATE case_sensitive;

-- Positive: same collation, safe to push HAVING to WHERE
EXPLAIN (COSTS OFF)
SELECT x, count(*) FROM test3ci GROUP BY x HAVING x = 'abc' COLLATE case_insensitive;
SELECT x, count(*) FROM test3ci GROUP BY x HAVING x = 'abc' COLLATE case_insensitive;

-- Negative: ROW comparison with conflicting collation
EXPLAIN (COSTS OFF)
SELECT x, count(*) FROM test3ci GROUP BY x HAVING ROW(x, 1) < ROW('ABC' COLLATE case_sensitive, 1) ORDER BY 1;
SELECT x, count(*) FROM test3ci GROUP BY x HAVING ROW(x, 1) < ROW('ABC' COLLATE case_sensitive, 1) ORDER BY 1;

-- Negative: simple-CASE form with conflicting WHEN comparison collation
EXPLAIN (COSTS OFF)
SELECT x, count(*) FROM test3ci GROUP BY x HAVING (CASE x WHEN 'abc' COLLATE case_sensitive THEN true ELSE false END);
SELECT x, count(*) FROM test3ci GROUP BY x HAVING (CASE x WHEN 'abc' COLLATE case_sensitive THEN true ELSE false END);

-- Positive: simple-CASE form with matching collation, safe to push
EXPLAIN (COSTS OFF)
SELECT x, count(*) FROM test3ci GROUP BY x HAVING (CASE x WHEN 'abc' COLLATE case_insensitive THEN true ELSE false END);
SELECT x, count(*) FROM test3ci GROUP BY x HAVING (CASE x WHEN 'abc' COLLATE case_insensitive THEN true ELSE false END);

-- Negative: nested CASE with collation conflict
EXPLAIN (COSTS OFF)
SELECT x, count(*) FROM test3ci GROUP BY x HAVING (CASE WHEN (CASE x WHEN 'abc' COLLATE case_sensitive THEN 1 ELSE 0 END) = 1 THEN true ELSE false END);
SELECT x, count(*) FROM test3ci GROUP BY x HAVING (CASE WHEN (CASE x WHEN 'abc' COLLATE case_sensitive THEN 1 ELSE 0 END) = 1 THEN true ELSE false END);

-- Positive: conflicting collation but no grouping expression reference
EXPLAIN (COSTS OFF)
SELECT x, count(*) FROM test3ci GROUP BY x HAVING current_setting('server_version') = 'abc' COLLATE case_sensitive;

--
-- 5c214b5: opfamily and collation must agree before a unique index can
-- prove functional dependency for redundant-GROUP-BY-column removal.
--

CREATE TABLE groupby_collation_t (a text COLLATE case_insensitive NOT NULL, b text);
INSERT INTO groupby_collation_t VALUES ('foo', 'X'), ('FOO', 'Y');
CREATE UNIQUE INDEX ON groupby_collation_t (a COLLATE "C");

-- Column b must NOT be dropped: under case_insensitive on a, 'foo' and
-- 'FOO' would merge, but they have distinct b values.
EXPLAIN (COSTS OFF)
SELECT a, b FROM groupby_collation_t GROUP BY a, b ORDER BY a, b;
SELECT a, b FROM groupby_collation_t GROUP BY a, b ORDER BY a, b;

DROP TABLE groupby_collation_t;

create type t_rec as (x numeric);
create table t_opf (a t_rec not null, b text);
create unique index on t_opf (a record_image_ops);
-- (1.0) and (1.00) are bytewise distinct but logically equal as records;
-- the index admits both, but GROUP BY a (default record_ops) would merge
-- them, so b must be retained as a grouping key.
insert into t_opf values (row(1.0)::t_rec, 'X'), (row(1.00)::t_rec, 'Y');
explain (costs off)
select a, b from t_opf group by a, b order by b;
select a, b from t_opf group by a, b order by b;
drop table t_opf;
drop type t_rec;

DROP TABLE test3ci;
DROP TABLE test3cs;
DROP TABLE test1cs;
