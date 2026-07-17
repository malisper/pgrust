# WS-U wave-5 (se/wave5-epq-inc1): LockRows-initiated EPQ with non-locked
# rels re-fetched through relsubs_rowmark (wave-5 contract §6.4b — the
# EvalPlanQualFetchRowMark surface, both mark kinds).
#
# s2 locks only rm_t (FOR UPDATE OF rm_t); the joined rel is NOT locked,
# so inside the recheck it is answered by its aux rowmark, not by a scan:
# a plain table gets ROW_MARK_REFERENCE (junk-ctid refetch under
# SnapshotAny), a VALUES list gets ROW_MARK_COPY (wholerow junk datum).
# The two-hop storm forces the recheck; both fetch paths must reproduce
# the joined row exactly. Expected output is the C-oracle's
# (PostgreSQL 18.3).

setup
{
 CREATE TABLE rm_t (k int PRIMARY KEY, v int);
 CREATE TABLE rm_s (k int PRIMARY KEY, tag text);
 INSERT INTO rm_t VALUES (1, 10), (2, 20);
 INSERT INTO rm_s VALUES (1, 'one'), (2, 'two');
}

teardown
{
 DROP TABLE rm_t;
 DROP TABLE rm_s;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u1	{ UPDATE rm_t SET v = v + 100 WHERE k = 1; }
step s1u2	{ UPDATE rm_t SET v = v + 1000 WHERE k = 1; }
step s1c	{ COMMIT; }

session s2
step ref	{ SELECT rm_t.k, rm_t.v, rm_s.tag FROM rm_t JOIN rm_s ON rm_s.k = rm_t.k WHERE rm_t.k = 1 FOR UPDATE OF rm_t; }
step copy	{ SELECT rm_t.k, rm_t.v, vs.tag FROM rm_t JOIN (VALUES (1, 'v-one'), (2, 'v-two')) AS vs(k, tag) ON vs.k = rm_t.k WHERE rm_t.k = 1 FOR UPDATE OF rm_t; }

permutation s1u1 s1u2 ref s1c
permutation s1u1 s1u2 copy s1c
