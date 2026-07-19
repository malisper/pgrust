# WS-U wave-5 (se/wave5-epq-inc1): ON CONFLICT DO UPDATE interplay beyond
# the baseline insert-conflict battery (wave-5 contract §6.4b).
#
# Three races against the same DO UPDATE ... WHERE statement:
#   pass  — concurrent value bump keeps the auxiliary WHERE true; the
#           requalified row is updated on the committed version.
#   fail  — concurrent bump flips the auxiliary WHERE false on the new
#           version; the conflict arm requalifies to a no-op (INSERT 0 0).
#   gone  — concurrent DELETE removes the conflicting row; the
#           speculative-insertion loop retries from the top and the
#           statement lands as a plain INSERT.
# Expected output is the C-oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE oc (k int PRIMARY KEY, v int, note text);
 INSERT INTO oc VALUES (1, 10, 'base');
}

teardown
{
 DROP TABLE oc;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u	{ UPDATE oc SET v = v + 100 WHERE k = 1; }
step s1big	{ UPDATE oc SET v = 1000 WHERE k = 1; }
step s1del	{ DELETE FROM oc WHERE k = 1; }
step s1c	{ COMMIT; }

session s2
step doup	{ INSERT INTO oc VALUES (1, 999, 'ins') ON CONFLICT (k) DO UPDATE SET v = oc.v + 1, note = 'upd' WHERE oc.v < 500 RETURNING k, v, note; }

session s3
step s3sel	{ SELECT k, v, note FROM oc ORDER BY k; }

permutation s1u doup s1c s3sel
permutation s1big doup s1c s3sel
permutation s1del doup s1c s3sel
