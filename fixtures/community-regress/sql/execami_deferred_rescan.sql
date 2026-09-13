-- ExecReScan defers a child's rescan while its chgParam is pending
-- (execAmi.c, nodeResult.c, nodeAppend.c, nodeSubplan.c): a false
-- one-time filter, an untaken CASE arm, or a pruned Append child must
-- never evaluate the expressions the deferred rescan would have run.
SELECT o.i, (SELECT s.x FROM (SELECT 1 AS x LIMIT -o.i) AS s WHERE o.i < 0)
FROM (VALUES (1), (2)) AS o(i);
SELECT (SELECT CASE WHEN g.i < 0 THEN (SELECT 1 LIMIT g.i - 2) ELSE 0 END)
FROM generate_series(1,2) AS g(i);
CREATE TABLE dr_p(k int, x int) PARTITION BY LIST(k);
CREATE TABLE dr_p1 PARTITION OF dr_p FOR VALUES IN (1);
CREATE TABLE dr_p2 PARTITION OF dr_p FOR VALUES IN (2);
CREATE INDEX ON dr_p(x);
INSERT INTO dr_p VALUES (1,100),(2,50);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_memoize = off;
EXPLAIN (COSTS OFF)
SELECT v.k, s.x FROM (VALUES (0),(1),(2)) v(k)
CROSS JOIN LATERAL (SELECT x FROM dr_p WHERE dr_p.k = v.k AND dr_p.x = 100/v.k OFFSET 0) s;
SELECT v.k, s.x FROM (VALUES (0),(1),(2)) v(k)
CROSS JOIN LATERAL (SELECT x FROM dr_p WHERE dr_p.k = v.k AND dr_p.x = 100/v.k OFFSET 0) s;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_memoize;
DROP TABLE dr_p;
