-- Cursor e2e corpus: DECLARE/FETCH/MOVE/CLOSE vs C PostgreSQL 18.3.
-- Run via scripts/regress-diff.sh --sql fixtures/regress-cursors.sql C_BIN RUST_BIN
-- (FROM generate_series is a pre-existing server gap - pg_proc_result_arrays
-- seam unwired - so the corpus runs over real tables.)
\set VERBOSITY verbose
CREATE TABLE cursor_t (a int, b text);
INSERT INTO cursor_t VALUES (1,'r1'),(2,'r2'),(3,'r3'),(4,'r4'),(5,'r5'),(6,'r6'),(7,'r7'),(8,'r8'),(9,'r9'),(10,'r10');
-- ===== sorted cursor (Sort supports backward; auto-SCROLL heuristic) =====
BEGIN;
DECLARE c CURSOR FOR SELECT a FROM cursor_t ORDER BY a;
FETCH 3 c;
FETCH NEXT c;
FETCH ALL c;
FETCH BACKWARD 2 c;
MOVE 5 c;
FETCH 1 c;
FETCH FIRST c;
FETCH LAST c;
FETCH ABSOLUTE 4 FROM c;
FETCH RELATIVE 3 c;
FETCH RELATIVE -2 c;
FETCH PRIOR c;
FETCH FORWARD 2 c;
FETCH FORWARD ALL c;
FETCH BACKWARD ALL c;
MOVE ABSOLUTE 6 c;
FETCH 0 c;
MOVE BACKWARD ALL c;
MOVE FORWARD ALL c;
FETCH BACKWARD 3 IN c;
CLOSE c;
COMMIT;
-- ===== plain seqscan cursor, explicit SCROLL =====
BEGIN;
DECLARE tc SCROLL CURSOR FOR SELECT a, b FROM cursor_t;
FETCH 4 tc;
FETCH BACKWARD 2 tc;
MOVE FORWARD ALL tc;
FETCH BACKWARD ALL tc;
CLOSE tc;
COMMIT;
-- ===== seqscan cursor without SCROLL (heuristic allows backward) =====
BEGIN;
DECLARE hc CURSOR FOR SELECT a FROM cursor_t;
FETCH 3 hc;
FETCH BACKWARD 2 hc;
MOVE 2 hc;
FETCH ALL hc;
CLOSE hc;
COMMIT;
-- ===== explicit NO SCROLL refuses backward (55000) =====
BEGIN;
DECLARE ns NO SCROLL CURSOR FOR SELECT a FROM cursor_t;
FETCH 2 ns;
FETCH BACKWARD 1 ns;
ROLLBACK;
-- ===== error surface =====
FETCH 1 FROM nope;
CLOSE nope;
BEGIN;
DECLARE dup CURSOR FOR SELECT 1;
DECLARE dup CURSOR FOR SELECT 1;
ROLLBACK;
DECLARE toplevel CURSOR FOR SELECT 1;
BEGIN;
DECLARE both SCROLL NO SCROLL CURSOR FOR SELECT 1;
ROLLBACK;
BEGIN;
DECLARE c2 CURSOR FOR SELECT 1;
FETCH 1 c2;
FETCH 1 c2;
CLOSE c2;
CLOSE c2;
ROLLBACK;
-- ===== CLOSE ALL =====
BEGIN;
DECLARE a1 CURSOR FOR SELECT 1;
DECLARE a2 CURSOR FOR SELECT 2;
CLOSE ALL;
FETCH 1 a1;
ROLLBACK;
DROP TABLE cursor_t;
