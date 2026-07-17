# WS-U wave-5 (se/wave5-epq-inc1): MERGE interplay beyond the baseline
# merge-match-recheck battery (wave-5 contract §6.4b).
#
# Three races against one MERGE with tiered MATCHED actions and a
# NOT MATCHED insert:
#   chain — a two-hop value storm; the EPQ recheck lands on the final
#           version and the MATCHED tier re-dispatches on it
#           (matched-high, not matched-low).
#   move  — the storm moves the JOIN KEY out from under the match; the
#           recheck fails the join qual and MERGE falls to the
#           NOT MATCHED arm (concurrent-key-move insert).
#   gone  — the storm deletes the matched row; same NOT MATCHED landing.
# Expected output is the C-oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE mt (k int PRIMARY KEY, v int, s text);
 CREATE TABLE ms (k int, delta int);
 INSERT INTO mt VALUES (1, 10, 'a');
 INSERT INTO ms VALUES (1, 5);
}

teardown
{
 DROP TABLE mt;
 DROP TABLE ms;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u1	{ UPDATE mt SET v = v + 100 WHERE k = 1; }
step s1u2	{ UPDATE mt SET v = v + 1000 WHERE k = 1; }
step s1move	{ UPDATE mt SET k = 9 WHERE k = 1; }
step s1del	{ DELETE FROM mt WHERE k = 1; }
step s1c	{ COMMIT; }

session s2
step merge
{
 MERGE INTO mt t USING ms s ON t.k = s.k
 WHEN MATCHED AND t.v < 500 THEN UPDATE SET v = t.v + s.delta, s = 'matched-low'
 WHEN MATCHED THEN UPDATE SET s = 'matched-high'
 WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.delta, 'inserted');
}

session s3
step s3sel	{ SELECT k, v, s FROM mt ORDER BY k; }

permutation s1u1 s1u2 merge s1c s3sel
permutation s1move merge s1c s3sel
permutation s1del merge s1c s3sel
