-- arrays tier 2 corpus: ARRAY[], subscripting, slices, = ANY/ALL, IN,
-- array_agg, unnest (FROM + tlist), int[] typenames, UPDATE/INSERT element
-- assignment. Byte-diffed pgrust vs C 18.3.
SELECT ARRAY[1,2,3];
SELECT ARRAY[1,2,3][2];
SELECT (ARRAY[1,2,3])[0];
SELECT (ARRAY[1,2,3])[4];
SELECT ARRAY['a','b','c'];
SELECT ARRAY[1.5, 2.5];
SELECT ARRAY[1, NULL, 3];
SELECT ARRAY[]::int[];
SELECT ARRAY[[1,2],[3,4]];
SELECT ARRAY[[1,2],[3,4]][2][1];
SELECT ARRAY[ARRAY[1,2], ARRAY[3,4]];
SELECT ARRAY[[1,2],[3]];
SELECT (ARRAY[1,2,3,4,5])[2:4];
SELECT (ARRAY[1,2,3,4,5])[:3];
SELECT (ARRAY[1,2,3,4,5])[3:];
SELECT (ARRAY[1,2,3])[NULL:2];
SELECT 2 = ANY (ARRAY[1,2,3]);
SELECT 5 = ANY (ARRAY[1,2,3]);
SELECT 5 <> ALL (ARRAY[1,2,3]);
SELECT 1 < SOME (ARRAY[1,2,3]);
SELECT NULL = ANY (ARRAY[1,2,3]);
SELECT 1 = ANY (ARRAY[NULL,1]::int[]);
SELECT 7 = ANY (ARRAY[NULL,1]::int[]);
SELECT 7 = ALL (ARRAY[]::int[]);
SELECT 7 = ANY (NULL::int[]);
SELECT 2 IN (1,2,3);
SELECT 9 IN (1,2,3);
SELECT unnest(ARRAY[1,2,3]);
SELECT unnest(ARRAY['x','y']), 1;
SELECT u FROM unnest(ARRAY[10,20,30]) u;
SELECT u FROM unnest(ARRAY[[1,2],[3,4]]) u;
CREATE TABLE arr_t (id int, xs int[], ys text[]);
INSERT INTO arr_t VALUES (1, ARRAY[1,2,3], ARRAY['a','b']);
INSERT INTO arr_t VALUES (2, '{4,5,6}', '{"c","d"}');
INSERT INTO arr_t VALUES (3, NULL, NULL);
SELECT id, xs[1], xs[2:3], ys[2] FROM arr_t ORDER BY id;
-- WHERE <const> = ANY(<array column>) rides scalararraysel_containment
-- (array_selfuncs.c) — loud on main (M2 selectivity lane); tlist form
-- exercises the executor leg.
SELECT id, 2 = ANY (xs) FROM arr_t ORDER BY id;
SELECT id FROM arr_t WHERE id = ANY (ARRAY[1,3]) ORDER BY id;
SELECT array_agg(id) FROM arr_t;
SELECT array_agg(xs[1]) FROM arr_t WHERE xs IS NOT NULL;
EXPLAIN (COSTS OFF) SELECT id FROM arr_t WHERE id = ANY (ARRAY[1,3]);
EXPLAIN (COSTS OFF) SELECT xs[1] FROM arr_t;
UPDATE arr_t SET xs[1] = 100 WHERE id = 1;
UPDATE arr_t SET xs[5] = 50 WHERE id = 2;
SELECT id, xs FROM arr_t ORDER BY id;
INSERT INTO arr_t (id, xs) VALUES (4, '{7,8}');
UPDATE arr_t SET xs[2] = xs[1] + 1 WHERE id = 4;
SELECT id, xs FROM arr_t ORDER BY id;
CREATE TABLE arr_b (v int ARRAY);
INSERT INTO arr_b VALUES (ARRAY[9]);
SELECT v[1] FROM arr_b;
SELECT '{1,2,3}'::int[];
SELECT '{{1,2},{3,4}}'::int[][2];
SELECT 3 = ANY ('{1,2,3}'::int[]);
DROP TABLE arr_t;
DROP TABLE arr_b;
