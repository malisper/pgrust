-- JOIN USING / NATURAL JOIN differential battery: merged columns, USING
-- aliases (PG14+), NATURAL over mixed outer joins, whole-row refs of join
-- RTEs, output-column visibility, ambiguity errors, view round-trips.
\set VERBOSITY verbose
CREATE TABLE ju_a (id int, name text, x int);
CREATE TABLE ju_b (id int, name text, y int);
CREATE TABLE ju_c (id int, z int);
INSERT INTO ju_a VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30), (NULL, 'anull', 40);
INSERT INTO ju_b VALUES (1, 'one', 100), (3, 'trois', 300), (4, 'four', 400), (NULL, 'bnull', 500);
INSERT INTO ju_c VALUES (1, 1000), (4, 4000), (5, 5000);
-- single-column USING, all join types
SELECT * FROM ju_a JOIN ju_b USING (id) ORDER BY id;
SELECT * FROM ju_a LEFT JOIN ju_b USING (id) ORDER BY id;
SELECT * FROM ju_a RIGHT JOIN ju_b USING (id) ORDER BY id;
SELECT * FROM ju_a FULL JOIN ju_b USING (id) ORDER BY id;
-- the merged column is non-null on the outer side (COALESCE semantics)
SELECT id, ju_a.id, ju_b.id FROM ju_a FULL JOIN ju_b USING (id) ORDER BY id;
-- multi-column USING
SELECT * FROM ju_a JOIN ju_b USING (id, name) ORDER BY id;
SELECT id, name, x, y FROM ju_a LEFT JOIN ju_b USING (id, name) ORDER BY id, name;
SELECT * FROM ju_a FULL JOIN ju_b USING (id, name) ORDER BY id, name;
-- qualified refs to the input columns still work without an alias
SELECT ju_a.id, ju_b.id FROM ju_a JOIN ju_b USING (id) ORDER BY 1;
-- merged column is unambiguous unqualified
SELECT id FROM ju_a JOIN ju_b USING (id) ORDER BY id;
SELECT id FROM ju_a JOIN ju_b USING (id) WHERE id > 1 ORDER BY id;
-- non-USING same-named column stays ambiguous
SELECT name FROM ju_a JOIN ju_b USING (id) ORDER BY 1;
-- USING alias (PG14+): merged columns via the alias
SELECT j.id FROM ju_a JOIN ju_b USING (id) AS j ORDER BY j.id;
SELECT j.id, ju_a.name, ju_b.name FROM ju_a JOIN ju_b USING (id) AS j ORDER BY 1;
SELECT j.* FROM ju_a JOIN ju_b USING (id) AS j ORDER BY 1;
SELECT j.id FROM ju_a LEFT JOIN ju_b USING (id) AS j WHERE j.id IS NOT NULL ORDER BY 1;
SELECT j.id, j.name FROM ju_a FULL JOIN ju_b USING (id, name) AS j ORDER BY 1, 2;
-- USING alias only exposes the merged columns
SELECT j.x FROM ju_a JOIN ju_b USING (id) AS j;
SELECT j.y FROM ju_a JOIN ju_b USING (id) AS j;
-- whole-row of the USING alias is a RowExpr over the merged columns
SELECT j FROM ju_a JOIN ju_b USING (id) AS j ORDER BY j.id;
SELECT j FROM ju_a LEFT JOIN ju_b USING (id, name) AS j ORDER BY j.id, j.name;
-- NATURAL joins
SELECT * FROM ju_a NATURAL JOIN ju_b ORDER BY id;
SELECT * FROM ju_a NATURAL LEFT JOIN ju_b ORDER BY id;
SELECT * FROM ju_a NATURAL FULL JOIN ju_b ORDER BY id, name;
SELECT * FROM ju_a NATURAL JOIN ju_c ORDER BY id;
SELECT * FROM ju_a NATURAL JOIN ju_b NATURAL JOIN ju_c ORDER BY id;
-- NATURAL with no common columns degenerates to cross join
CREATE TABLE ju_d (q int);
INSERT INTO ju_d VALUES (7);
SELECT * FROM ju_c NATURAL JOIN ju_d ORDER BY id, q;
-- join alias hides inputs
SELECT * FROM (ju_a JOIN ju_b USING (id)) t ORDER BY id;
SELECT t.id, t.x, t.y FROM (ju_a JOIN ju_b USING (id)) t ORDER BY 1;
SELECT ju_a.id FROM (ju_a JOIN ju_b USING (id)) t;
SELECT * FROM (ju_a JOIN ju_b USING (id)) t(a1, a2, a3) ORDER BY 1;
-- whole-row refs of join RTEs
SELECT t FROM (ju_a JOIN ju_b USING (id)) t ORDER BY t.id;
SELECT row_to_json(t) FROM (ju_a JOIN ju_b USING (id)) t ORDER BY t.id;
SELECT count(t) FROM (ju_a JOIN ju_b USING (id)) t;
-- USING columns of different but coercible types
CREATE TABLE ju_e (id bigint, w int);
INSERT INTO ju_e VALUES (1, 11), (3, 33);
SELECT * FROM ju_a JOIN ju_e USING (id) ORDER BY id;
SELECT pg_typeof(id) FROM ju_a JOIN ju_e USING (id) LIMIT 1;
SELECT pg_typeof(id) FROM ju_a FULL JOIN ju_e USING (id) LIMIT 1;
-- varchar/text typmod mix
CREATE TABLE ju_f (name varchar(10), v int);
INSERT INTO ju_f VALUES ('one', 111), ('four', 444);
SELECT * FROM ju_a JOIN ju_f USING (name) ORDER BY name;
SELECT pg_typeof(name) FROM ju_a JOIN ju_f USING (name) LIMIT 1;
-- errors: USING duplicates / missing / ambiguous
SELECT * FROM ju_a JOIN ju_b USING (id, id);
SELECT * FROM ju_a JOIN ju_b USING (nosuch);
SELECT * FROM ju_a JOIN ju_b USING (x);
SELECT * FROM (SELECT x AS d, y AS d FROM ju_a, ju_b) s JOIN ju_c ON true JOIN (SELECT 1 AS d) s2 USING (d);
-- error: NATURAL and mixed USING/ON are grammar-level, skip; alias conflicts
SELECT * FROM ju_a JOIN ju_b USING (id) AS ju_a;
SELECT * FROM ju_a a1 JOIN ju_b USING (id) AS a1;
-- USING alias in target/expressions and group by
SELECT j.id, count(*) FROM ju_a LEFT JOIN ju_b USING (id) AS j GROUP BY j.id ORDER BY 1;
-- EXPLAIN over USING joins
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ju_a JOIN ju_b USING (id);
EXPLAIN (VERBOSE, COSTS OFF) SELECT id FROM ju_a FULL JOIN ju_b USING (id);
EXPLAIN (VERBOSE, COSTS OFF) SELECT j.id FROM ju_a JOIN ju_b USING (id) AS j;
-- view round-trips (pg_get_viewdef byte parity)
CREATE VIEW ju_v1 AS SELECT * FROM ju_a JOIN ju_b USING (id, name);
-- duplicate output column names cannot be stored in a view
CREATE VIEW ju_vdup AS SELECT * FROM ju_a JOIN ju_b USING (id);
CREATE VIEW ju_v2 AS SELECT id, x, y FROM ju_a LEFT JOIN ju_b USING (id, name);
CREATE VIEW ju_v3 AS SELECT j.id FROM ju_a FULL JOIN ju_b USING (id) AS j WHERE j.id > 0;
CREATE VIEW ju_v4 AS SELECT * FROM ju_a NATURAL JOIN ju_b;
CREATE VIEW ju_v5 AS SELECT t.id, t.x FROM (ju_a JOIN ju_b USING (id)) t;
CREATE VIEW ju_v6 AS SELECT * FROM ju_a NATURAL LEFT JOIN ju_c NATURAL JOIN ju_d;
SELECT pg_get_viewdef('ju_v1'::regclass);
SELECT pg_get_viewdef('ju_v2'::regclass);
SELECT pg_get_viewdef('ju_v3'::regclass);
SELECT pg_get_viewdef('ju_v4'::regclass);
SELECT pg_get_viewdef('ju_v5'::regclass);
SELECT pg_get_viewdef('ju_v6'::regclass);
SELECT * FROM ju_v1 ORDER BY id;
SELECT * FROM ju_v2 ORDER BY id;
SELECT * FROM ju_v3 ORDER BY id;
SELECT * FROM ju_v4 ORDER BY id;
SELECT * FROM ju_v5 ORDER BY id;
SELECT * FROM ju_v6 ORDER BY id;
-- USING columns in WHERE/ORDER BY/aggregates
SELECT id, sum(x + y) FROM ju_a JOIN ju_b USING (id) GROUP BY id HAVING id < 4 ORDER BY id;
-- nested USING over a join output column
SELECT * FROM (ju_a JOIN ju_b USING (id)) JOIN ju_c USING (id) ORDER BY id;
SELECT * FROM ju_a JOIN ju_b USING (id) JOIN ju_c USING (id) ORDER BY id;
-- lateral-ish ordering sanity: USING var in subquery output
SELECT s.id FROM (SELECT id FROM ju_a JOIN ju_b USING (id)) s ORDER BY 1;
DROP VIEW ju_v1, ju_v2, ju_v3, ju_v4, ju_v5, ju_v6;
DROP TABLE ju_a, ju_b, ju_c, ju_d, ju_e, ju_f;
