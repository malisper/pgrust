-- DENYLIST (do not train on these). Statement templates issued by the published
-- OLTP measurement rigs. Parameter placeholders are normalized away by the lint
-- (?, $1, :name and numeric/string literals all collapse to a single token), so
-- these entries deny the whole parameterized family, not one binding.
--
-- == read-only / read-write transactional rig (upstream oltp_common.lua 1.0.20,
--    tables sbtest1..sbtest10) ==
SELECT c FROM sbtest1 WHERE id=?;
SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?;
SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?;
SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c;
SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c;
UPDATE sbtest1 SET k=k+1 WHERE id=?;
UPDATE sbtest1 SET c=? WHERE id=?;
DELETE FROM sbtest1 WHERE id=?;
INSERT INTO sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?);
CREATE TABLE sbtest1(id SERIAL, k INTEGER DEFAULT '0' NOT NULL, c CHAR(120) DEFAULT '' NOT NULL, pad CHAR(60) DEFAULT '' NOT NULL, PRIMARY KEY (id));
CREATE INDEX k_1 ON sbtest1(k);
-- == the in-repo per-statement-type survey rig (same schema, pgbench-driven) ==
SELECT c FROM sbtest1 WHERE id = :id;
SELECT c FROM sbtest1 WHERE id BETWEEN :id AND :id+99;
SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN :id AND :id+99;
SELECT c FROM sbtest1 WHERE id BETWEEN :id AND :id+99 ORDER BY c;
SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN :id AND :id+99 ORDER BY c;
UPDATE sbtest1 SET k=k+1 WHERE id = :id;
UPDATE sbtest1 SET c = substr(md5(:rnd::text) || md5((:rnd+1)::text) || md5((:rnd+2)::text) || md5((:rnd+3)::text), 1, 120) WHERE id = :id;
INSERT INTO sbtest1 (id,k,c,pad) VALUES (:id, :k, 'x', 'y');
CREATE TABLE sbtest1 (id SERIAL PRIMARY KEY, k INTEGER NOT NULL DEFAULT 0, c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '');
VACUUM ANALYZE sbtest1;
-- == read-write transactional rig (builtin tpcb-like, tables pgbench_*) ==
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
SELECT sum(abalance) FROM pgbench_accounts WHERE bid = :bid;
