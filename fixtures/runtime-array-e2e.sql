CREATE TABLE rak_t (id int primary key, v text);
INSERT INTO rak_t SELECT g, 'v'||g FROM generate_series(1,100) g;
CREATE INDEX rak_t_v_idx ON rak_t (v);
CREATE TABLE rak_src (k int, arr int[]);
INSERT INTO rak_src VALUES (1, ARRAY[2,4,6]), (2, ARRAY[1,99]), (3, ARRAY[50]);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
SELECT id, v FROM rak_t WHERE id = ANY ((SELECT array_agg(x) FROM generate_series(3,5) x)) ORDER BY id;
SELECT id, v FROM rak_t WHERE id = ANY ((SELECT array_agg(x) FROM generate_series(3,5) x)) ORDER BY id;
SELECT id, v FROM rak_t WHERE id = ANY ((SELECT array_agg(x) FROM generate_series(1,0) x)) ORDER BY id;
EXPLAIN (COSTS OFF)
SELECT id, v FROM rak_t WHERE v = ANY ((SELECT array_agg('v'||x::text) FROM generate_series(7,9) x)) ORDER BY id;
SELECT id, v FROM rak_t WHERE v = ANY ((SELECT array_agg('v'||x::text) FROM generate_series(7,9) x)) ORDER BY id;
EXPLAIN (COSTS OFF)
SELECT s.k, t.id, t.v FROM rak_src s JOIN rak_t t ON t.id = ANY (s.arr) ORDER BY s.k, t.id;
SELECT s.k, t.id, t.v FROM rak_src s JOIN rak_t t ON t.id = ANY (s.arr) ORDER BY s.k, t.id;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE rak_src;
DROP TABLE rak_t;
