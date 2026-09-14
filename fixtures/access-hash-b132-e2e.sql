-- batch-132: hash AM fixes checked against C 18.6.
-- fp-hash-b1#2: an errored spool build leaves no tuplesort "ended" trace line.
SET client_min_messages = log;
SET temp_buffers = 100;
CREATE TEMP TABLE hs_b132 (x int);
INSERT INTO hs_b132 SELECT g FROM generate_series(1, 40000) g;
INSERT INTO hs_b132 VALUES (0);
ANALYZE hs_b132;
SET trace_sort = on;
CREATE INDEX hs_b132_bad ON hs_b132 USING hash ((100 / x));
RESET trace_sort;
RESET client_min_messages;
-- fp-hash-hash#1: hashinsert frees its tuple image per row (no per-row growth).
CREATE TABLE hi_b132 (x int);
CREATE INDEX hi_b132_h ON hi_b132 USING hash (x);
INSERT INTO hi_b132 SELECT g FROM generate_series(1, 5000) g;
SET enable_seqscan = off;
SELECT count(*) FROM hi_b132 WHERE x = 4321;
RESET enable_seqscan;
DROP TABLE hi_b132, hs_b132;
