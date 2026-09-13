-- RelationGetIndexPredicate (relcache.c) const-folds, canonicalize_quals and
-- then splits the predicate. Canonicalization drops the redundant OR branch,
-- so the division by zero it contains is never evaluated for an inserted or
-- updated row.
CREATE TABLE idxpred_canon(a int, b int);
CREATE INDEX idxpred_canon_i ON idxpred_canon(a)
    WHERE ((a = 1 AND 1 / (a - 1) > 0) OR a = 1);
INSERT INTO idxpred_canon VALUES (1, 1);
INSERT INTO idxpred_canon VALUES (2, 2);
INSERT INTO idxpred_canon SELECT 1, g FROM generate_series(3, 5) g;
UPDATE idxpred_canon SET b = b + 10 WHERE a = 1;
SELECT * FROM idxpred_canon ORDER BY a, b;
SET enable_seqscan = off;
SELECT * FROM idxpred_canon WHERE a = 1 ORDER BY b;
RESET enable_seqscan;
DROP TABLE idxpred_canon;
