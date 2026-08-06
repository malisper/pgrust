# Whitelist-completion wave (fix/epq-whitelist-completion): ForeignScan
# INSIDE the EPQ recheck plan — a file_fdw foreign table as a join source
# under FOR UPDATE (LockRows -> NestLoop -> ForeignScan + SeqScan on the
# locked rel). pgrust HAS in-tree FDW providers (file_fdw/postgres_fdw),
# so the "no FDW surface" assumption behind the old refusal was stale:
# this is a plain scanrelid != 0 foreign scan — the foreign rel gets a
# ROW_MARK_COPY aux rowmark, and the recheck answers it from the wholerow
# copy; the ForeignScan node itself still inits inside the recheck tree
# (the tag the loud list previously refused). scanrelid == 0 pushed-down
# joins stay loudly refused on their own arm (lane-epq.md section 2). The
# setup writes the CSV server-side via COPY TO so the spec is
# self-contained. The EXPLAIN step pins the plan shape into the expected
# file. Expected output is the C oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE docs (id int PRIMARY KEY, status text);
 INSERT INTO docs SELECT g, 'pending' FROM generate_series(1, 5) g;
 COPY (SELECT g FROM generate_series(1, 5) g) TO '/tmp/pgrust-epq-subq-foreignscan.csv' WITH (FORMAT csv);
 CREATE EXTENSION file_fdw;
 CREATE SERVER epq_fdw_files FOREIGN DATA WRAPPER file_fdw;
 CREATE FOREIGN TABLE approvals_file (doc_id int) SERVER epq_fdw_files
   OPTIONS (filename '/tmp/pgrust-epq-subq-foreignscan.csv', format 'csv');
 ANALYZE docs;
}

teardown
{
 DROP TABLE docs;
 DROP EXTENSION file_fdw CASCADE;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u	{ UPDATE docs SET status = 'revoked' WHERE id = 3; }
step s1k	{ UPDATE docs SET id = 99 WHERE id = 3; }
step s1c	{ COMMIT; }

session s2
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN approvals_file a ON a.doc_id = d.id FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN approvals_file a ON a.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

# Pass arm: locked row re-returned with its NEW status.
permutation expl s1u lock s1c sel
# Skip arm: the locked row's join key moves away; row skipped.
permutation expl s1k lock s1c sel
