-- estimate_rel_size (plancat.c) parity: unanalyzed tables of varied widths and
-- row counts, EXPLAIN (COSTS ON) byte-identical vs C 18.3, BEFORE and AFTER
-- ANALYZE. Exercises the never-vacuumed density arm (reltuples<0, 10-page
-- floor, tuple-width-from-datatypes fallback), the reltuples/relpages arm
-- (post-ANALYZE), the index branch (rel->rd_rel->reltuples interplay, metapage
-- discount), the curpages==0 quick-exit, and the relhassubclass
-- inheritance-parent suppression of the 10-page floor.
-- Envelope: no varchar(n) support fn (prosupport lane), no name-column IN
-- lists (hashname fn 455 unported), no inheritance-tree ANALYZE lane; small
-- deterministic row counts.
SET compute_query_id = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;

-- Varied widths: narrow (single int4), int4+text, wide fixed, bpchar(n).
-- Varied row counts spanning the <10-page floor and above it.
CREATE TABLE rs_narrow(a int);
INSERT INTO rs_narrow SELECT g FROM generate_series(1, 5) g;
CREATE TABLE rs_it(a int, t text);
INSERT INTO rs_it SELECT g, 'row' || (g % 7)::text FROM generate_series(1, 5) g;
CREATE TABLE rs_wide(a int, b int, c int, d int, e int, f int, g int, h int);
INSERT INTO rs_wide SELECT i,i,i,i,i,i,i,i FROM generate_series(1, 12000) i;
CREATE TABLE rs_bpchar(a int, c char(40));
INSERT INTO rs_bpchar SELECT g, 'x' FROM generate_series(1, 3000) g;
-- Empty (curpages == 0 quick-exit) and a truly single-page table.
CREATE TABLE rs_empty(a int, b int);
CREATE TABLE rs_one(a int);
INSERT INTO rs_one VALUES (1);

-- Inheritance parent (relhassubclass true) suppresses the 10-page floor.
CREATE TABLE rs_parent(a int);
CREATE TABLE rs_child(a int) INHERITS (rs_parent);
INSERT INTO rs_child SELECT g FROM generate_series(1, 4) g;

-- === BEFORE ANALYZE: never-vacuumed density arm ===
EXPLAIN SELECT * FROM rs_narrow;
EXPLAIN SELECT * FROM rs_it;
EXPLAIN SELECT * FROM rs_wide;
EXPLAIN SELECT * FROM rs_bpchar;
EXPLAIN SELECT * FROM rs_empty;
EXPLAIN SELECT * FROM rs_one;
EXPLAIN SELECT * FROM rs_parent;
EXPLAIN SELECT * FROM ONLY rs_parent;
EXPLAIN SELECT * FROM rs_narrow WHERE a = 3;
EXPLAIN SELECT * FROM rs_wide WHERE a < 100;
EXPLAIN SELECT count(*) FROM rs_wide;

-- Index branch before ANALYZE (index reltuples interplay, metapage discount).
CREATE INDEX rs_wide_a ON rs_wide(a);
CREATE INDEX rs_it_a ON rs_it(a);
EXPLAIN SELECT a FROM rs_wide WHERE a = 42;
EXPLAIN SELECT a FROM rs_wide WHERE a BETWEEN 100 AND 200;
EXPLAIN SELECT * FROM rs_it WHERE a = 3;

-- === AFTER ANALYZE: reltuples/relpages density arm ===
ANALYZE rs_narrow;
ANALYZE rs_it;
ANALYZE rs_wide;
ANALYZE rs_bpchar;
ANALYZE rs_one;

EXPLAIN SELECT * FROM rs_narrow;
EXPLAIN SELECT * FROM rs_it;
EXPLAIN SELECT * FROM rs_wide;
EXPLAIN SELECT * FROM rs_bpchar;
EXPLAIN SELECT * FROM rs_one;
EXPLAIN SELECT a FROM rs_wide WHERE a = 42;
EXPLAIN SELECT a FROM rs_wide WHERE a BETWEEN 100 AND 200;
EXPLAIN SELECT count(*) FROM rs_wide;

DROP TABLE rs_narrow, rs_it, rs_wide, rs_bpchar, rs_empty, rs_one;
DROP TABLE rs_parent CASCADE;
