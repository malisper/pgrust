# Whitelist-completion wave (fix/epq-whitelist-completion): a WITH
# RECURSIVE cte joined under FOR UPDATE. NOT an admission — this pins the
# RecursiveUnion / WorkTableScan class-A verdict: recursive CTEs are
# never inlined, so RecursiveUnion (and the WorkTableScan inside its
# recursive term) lives in a PlannedStmt SUBPLAN read through an admitted
# CteScan, and check_epq_plan's walk never enters subplans — the recheck
# tree contains only the CteScan (the EXPLAIN step pins exactly that
# shape: CTE r hangs off LockRows, the join reads CTE Scan on r).
# Divergence note vs C (documented in epq.rs + lane-epq.md section 9): C's
# EvalPlanQualStart re-inits es_subplanstates in its child estate (fresh
# CTE tuplestore per recheck); pgrust's shared parent estate reuses the
# already-populated CTE state — equal results at the recheck's snapshot
# for stable CTE bodies, which this spec's C-oracle expected output
# (PostgreSQL 18.3) witnesses.

setup
{
 CREATE TABLE docs (id int PRIMARY KEY, status text);
 INSERT INTO docs SELECT g, 'pending' FROM generate_series(1, 5) g;
 ANALYZE docs;
}

teardown
{
 DROP TABLE docs;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u	{ UPDATE docs SET status = 'revoked' WHERE id = 3; }
step s1k	{ UPDATE docs SET id = 99 WHERE id = 3; }
step s1c	{ COMMIT; }

session s2
step expl	{ EXPLAIN (COSTS OFF) WITH RECURSIVE r AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM r WHERE id < 5) SELECT d.* FROM docs d JOIN r ON r.id = d.id FOR UPDATE OF d; }
step lock	{ WITH RECURSIVE r AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM r WHERE id < 5) SELECT d.* FROM docs d JOIN r ON r.id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

# Pass arm: locked row re-returned with its NEW status; the CTE feeds the
# recheck join from the shared-estate CTE state.
permutation expl s1u lock s1c sel
# Skip arm: the locked row's join key moves away; row skipped.
permutation expl s1k lock s1c sel
