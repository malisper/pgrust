-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-137-backend-access-transam (C 18.6 oracle vs pgrust). Each leg is
-- one row of the batch (access/transam parallel / commit_ts). Boots with
-- REGRESS_DIFF_SERVER_OPTS="-c track_commit_timestamp=on"
-- (scripts/access-transam-b137-e2e.sh).
\set VERBOSITY verbose
CREATE DATABASE b137e2e TEMPLATE template0 ENCODING 'UTF8';
\c b137e2e
\set VERBOSITY verbose
-- fp-transam-parallel#2: a parallel worker names the leader's temp relations
-- by the leader's proc number (ParallelLeaderProcNumber)
CREATE TEMP TABLE tt(a int);
CREATE TABLE leader_path AS SELECT pg_relation_filepath('tt') AS p;
SET debug_parallel_query = on;
SELECT pg_relation_filepath('tt') = p AS worker_matches_leader FROM leader_path;
-- fp-misc-guc-p3#1: a retained worker reloading the leader's libraries does
-- not redefine their custom GUCs
LOAD 'postgres_fdw';
SELECT count(*) FROM leader_path;
SELECT count(*) FROM leader_path;
SELECT count(*) FROM leader_path;
RESET debug_parallel_query;
-- fp-transam-commit_ts#1: RETURNS record aliases of the commit_ts builtins
-- take their row type from the FROM-clause column definition list
CREATE TABLE t18(a int);
INSERT INTO t18 VALUES (1);
CREATE FUNCTION my_last() RETURNS record LANGUAGE internal STRICT AS 'pg_last_committed_xact';
CREATE FUNCTION my_origin(xid) RETURNS record LANGUAGE internal STRICT AS 'pg_xact_commit_timestamp_origin';
SELECT xid IS NOT NULL AS has_xid, roident FROM my_last() AS (xid xid, timestamp timestamptz, roident oid);
SELECT origin FROM my_origin((SELECT xmin FROM t18)) AS (ts timestamptz, origin oid);
\c postgres
DROP DATABASE b137e2e;
