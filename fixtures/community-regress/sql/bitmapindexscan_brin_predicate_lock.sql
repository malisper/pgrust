-- nodeBitmapIndexscan.c:323: ExecInitBitmapIndexScan begins the index scan
-- at init, and index_beginscan_bitmap takes the relation predicate lock for
-- an AM without ampredlocks (BRIN) even when the scan never runs (LIMIT 0).
CREATE TABLE brin_plock(a integer);
INSERT INTO brin_plock SELECT g FROM generate_series(1, 10000) g;
CREATE INDEX brin_plock_idx ON brin_plock USING brin(a);
ANALYZE brin_plock;
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_indexscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM brin_plock WHERE a BETWEEN 100 AND 200 LIMIT 0;
SELECT * FROM brin_plock WHERE a BETWEEN 100 AND 200 LIMIT 0;
SELECT locktype, mode FROM pg_locks
  WHERE pid = pg_backend_pid() AND relation = 'brin_plock_idx'::regclass
  ORDER BY 1, 2;
COMMIT;
DROP TABLE brin_plock;
