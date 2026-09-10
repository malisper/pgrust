-- pg_stat_user_tables scan accounting vs C (sitediff 2026-09-10 N-1 / N-2).
-- N-1: seq_scan / seq_tup_read / idx_scan / idx_tup_fetch of a statement
--      that ERRORS are still counted (C bumps rel->pgstat_info per call and
--      reports at AbortTransaction).
-- N-2: seq_tup_read is per tuple RETURNED, not per page staged: an early-
--      terminated scan (LIMIT 1, EXISTS, an RI RESTRICT check's tcount=1)
--      credits only the rows the executor pulled.
-- Stats flush lazily: every probe is <statement>; force_next_flush; read.
CREATE TABLE rp (id int4 PRIMARY KEY, tag text);
CREATE TABLE rc (cid int4 PRIMARY KEY, pid int4 REFERENCES rp(id) ON DELETE RESTRICT, note text);
INSERT INTO rp VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');
INSERT INTO rc SELECT i, CASE WHEN i <= 1000 THEN 1 WHEN i <= 1500 THEN 2 ELSE 3 END, 'n'
  FROM generate_series(1,2000) i;
ANALYZE rp;
ANALYZE rc;
CREATE FUNCTION st() RETURNS TABLE(relname name, seq_scan bigint, seq_tup_read bigint,
                                   idx_scan bigint, idx_tup_fetch bigint)
LANGUAGE sql AS $$
  SELECT relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch
    FROM pg_stat_user_tables WHERE relname IN ('rp', 'rc') ORDER BY relname
$$;
-- Land the load's pending counts (the INSERT's per-row RI index probes on
-- rp) before the reset so the baseline is zero on both sides.
SELECT pg_stat_force_next_flush();
SELECT 1;
SELECT pg_stat_reset_single_table_counters('rp'::regclass),
       pg_stat_reset_single_table_counters('rc'::regclass);
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- Plan shapes the counters depend on.
EXPLAIN (COSTS OFF) DELETE FROM rp WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT cid FROM rc WHERE pid = 2 LIMIT 1;
EXPLAIN (COSTS OFF) SELECT note FROM rc WHERE cid = 7;

-- (1) full seq scan: +2000
SELECT count(*) FROM rc;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (2) LIMIT 1, no qual: +1
SELECT cid FROM rc LIMIT 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (3) LIMIT 1 with a qual first satisfied on page 6: +1001
SELECT cid FROM rc WHERE pid = 2 LIMIT 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (4) EXISTS: +1501
SELECT EXISTS (SELECT 1 FROM rc WHERE pid = 3);
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (5) RI RESTRICT check stops at the first match, statement ERRORS:
--     rp +1 scan / +4 read; rc +1 scan / +1 read
DELETE FROM rp WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (6) same, first match on page 6 (pid = 2 starts at cid 1001): rc +1001
DELETE FROM rp WHERE id = 2;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (7) RI check finds no child (full scan), statement succeeds: rc +2000
DELETE FROM rp WHERE id = 4;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (8) seq scan aborted mid-page by the projection: +100
SELECT 1 / (cid - 100) FROM rc;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (9) index scan, success: rc idx +1 / +1
SELECT note FROM rc WHERE cid = 7;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (10) index scan, statement errors: rc idx +1 / +1
SELECT 1 / (cid - 5), note FROM rc WHERE cid = 5;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (11) UPDATE that fails its FK check: rc idx +1 / +1, rp (RI lookup) counted
UPDATE rc SET pid = 9 WHERE cid = 3;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (12) errors inside a subtransaction, transaction commits
BEGIN;
SAVEPOINT a;
DELETE FROM rp WHERE id = 1;
ROLLBACK TO a;
SELECT count(*) FROM rp;
COMMIT;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (13) bitmap heap scan: heap tuples_fetched (idx_tup_fetch) +49, index +1
SET enable_seqscan = off;
SET enable_indexscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM rc WHERE cid < 50;
SELECT count(*) FROM rc WHERE cid < 50;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (14) bitmap heap scan aborted mid-page: +10
SELECT 1 / (cid - 10) FROM rc WHERE cid < 50;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
RESET enable_seqscan;
RESET enable_indexscan;
-- (15) TID scan: heap_fetch counts idx_tup_fetch +1
EXPLAIN (COSTS OFF) SELECT cid FROM rc WHERE ctid = '(0,1)';
SELECT cid FROM rc WHERE ctid = '(0,1)';
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
-- (16) cursor closed early mid-page: +3
BEGIN;
DECLARE c CURSOR FOR SELECT cid FROM rc;
FETCH 3 FROM c;
CLOSE c;
COMMIT;
SELECT pg_stat_force_next_flush();
SELECT * FROM st();
DROP FUNCTION st();
DROP TABLE rc;
DROP TABLE rp;
