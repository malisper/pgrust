-- Panic-audit A6 (notes/audits/unported-panic-inventory-2026-08-08.md):
-- GENERATED ... STORED columns on an attno-remapped partition reached via
-- tuple routing. Expected output captured from real PostgreSQL 18.
-- C computes stored generated columns against the routed leaf's own
-- ResultRelInfo (leaf pg_attrdef expressions, leaf attnos) after the
-- tuple-conversion map produced the leaf-layout slot; both remap flavors
-- (reordered columns, dropped-column skew) must insert and generate.
CREATE TABLE gsp_parent (a int, b int, g int GENERATED ALWAYS AS (a * 10 + b) STORED)
  PARTITION BY RANGE (a);
-- reordered-columns partition
CREATE TABLE gsp_p1 (g int GENERATED ALWAYS AS (a * 10 + b) STORED, b int, a int);
ALTER TABLE gsp_parent ATTACH PARTITION gsp_p1 FOR VALUES FROM (0) TO (100);
-- dropped-column-skew partition
CREATE TABLE gsp_p2 (x int, a int, b int, g int GENERATED ALWAYS AS (a * 10 + b) STORED);
ALTER TABLE gsp_p2 DROP COLUMN x;
ALTER TABLE gsp_parent ATTACH PARTITION gsp_p2 FOR VALUES FROM (100) TO (200);
INSERT INTO gsp_parent (a, b) VALUES (1, 2), (5, 7), (150, 9);
INSERT INTO gsp_parent (a, b) VALUES (42, 8) RETURNING a, b, g;
SELECT tableoid::regclass, a, b, g FROM gsp_parent ORDER BY a;
-- the leaf's own generated machinery agrees when addressed directly
INSERT INTO gsp_p1 (a, b) VALUES (60, 1);
SELECT a, b, g FROM gsp_p1 WHERE a = 60;
DROP TABLE gsp_parent;
