# Whitelist-completion wave (fix/epq-whitelist-completion): TableFuncScan
# INSIDE the EPQ recheck plan — JSON_TABLE under a locking join
# (LockRows -> HashJoin -> TableFuncScan + SeqScan on the locked rel).
# JSON_TABLE rather than XMLTABLE so the C-oracle expected file needs no
# libxml build; both produce the same TableFuncScan executor node. The
# table function is a non-relation RTE, so the recheck answers it through
# its ROW_MARK_COPY wholerow; the TableFuncScan node itself still inits
# inside the recheck tree (the tag the loud list previously refused). The
# EXPLAIN step pins the plan shape into the expected file. Expected output
# is the C oracle's (PostgreSQL 18.3).

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
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN JSON_TABLE('[1,3]', '$[*]' COLUMNS (doc_id int PATH '$')) x ON x.doc_id = d.id FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN JSON_TABLE('[1,3]', '$[*]' COLUMNS (doc_id int PATH '$')) x ON x.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

# Pass arm: locked row re-returned with its NEW status.
permutation expl s1u lock s1c sel
# Skip arm: the locked row's join key moves away; row skipped.
permutation expl s1k lock s1c sel
