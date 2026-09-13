-- postgres_fdw phase-2 CONNECTED suite: loopback self-connect over the
-- server's own unix socket (engine-agnostic, dblink_conn precedent).
-- Everything here is read-only foreign-scan execution; every row-returning
-- query carries ORDER BY because plan shapes (and so row order) legitimately
-- differ between engines (C pushes joins/aggregates down; phase 3 here).
-- DML / ANALYZE / IMPORT / async remain phase 3 (manifest in the worklog).
CREATE EXTENSION postgres_fdw;
\set SHOW_CONTEXT always
SET timezone = 'UTC';

-- user= is explicit: the harness initdb's the superuser as 'postgres', so
-- the client's OS-username default would pick a role that does not exist
-- (the dblink_conn precedent).
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '$$||current_setting('unix_socket_directories')||$$',
                     port '$$||current_setting('port')||$$',
                     dbname '$$||current_database()||$$'
            )$$;
        EXECUTE $$CREATE USER MAPPING FOR CURRENT_USER SERVER loopback
            OPTIONS (user '$$||current_user||$$')$$;
    END;
$d$;

CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
CREATE SCHEMA "S 1";
CREATE TABLE "S 1"."T 1" (
	"C 1" int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 user_enum,
	CONSTRAINT t1_pkey PRIMARY KEY ("C 1")
);
INSERT INTO "S 1"."T 1"
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'::user_enum
	FROM generate_series(1, 100) id;
ANALYZE "S 1"."T 1";

CREATE FOREIGN TABLE ft1 (
	c0 int,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 user_enum
) SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 1');
ALTER FOREIGN TABLE ft1 DROP COLUMN c0;
ALTER FOREIGN TABLE ft1 ALTER COLUMN c1 OPTIONS (column_name 'C 1');

-- ===================================================================
-- basic remote execution
-- ===================================================================
SELECT * FROM ft1 WHERE c1 < 5 ORDER BY c1;
SELECT c1, c3 FROM ft1 WHERE c1 = 47 ORDER BY c1;
SELECT COUNT(*) FROM ft1;
-- whole-row reference
SELECT t1 FROM ft1 t1 WHERE c1 = 3 ORDER BY c1;
-- empty result
SELECT * FROM ft1 WHERE false;
-- fixed values
SELECT 'fixed', NULL FROM ft1 WHERE c1 = 1;
-- remote condition + local condition mix (c8 is a non-shippable enum)
SELECT c1, c2 FROM ft1 WHERE c1 < 10 AND c8 = 'foo' ORDER BY c1;
-- text/timestamp round-trips (remote sends ISO/GMT, local converts)
SELECT c3, c4, c5 FROM ft1 WHERE c1 IN (1, 47, 100) ORDER BY c1;
SELECT c6, c7 FROM ft1 WHERE c6 = '1' ORDER BY c1 LIMIT 3;
-- NULL handling
SELECT c1, c3 IS NULL FROM ft1 WHERE c1 <= 3 ORDER BY c1;

-- ===================================================================
-- cursor batching: small fetch_size forces many FETCH round trips
-- ===================================================================
ALTER SERVER loopback OPTIONS (ADD fetch_size '3');
SELECT count(*) FROM (SELECT * FROM ft1) s;
SELECT c1 FROM ft1 WHERE c2 = 0 ORDER BY c1;
ALTER SERVER loopback OPTIONS (SET fetch_size '100');

-- ===================================================================
-- two cursors interleaved on one connection (local join of two scans)
-- ===================================================================
SELECT a.c1, b.c2 FROM ft1 a, ft1 b WHERE a.c1 = b.c1 AND a.c1 < 5 ORDER BY a.c1;

-- ===================================================================
-- remote parameters ($1::type): generic plan + correlated rescans
-- ===================================================================
SET plan_cache_mode = force_generic_plan;
PREPARE st1(int, text) AS SELECT c1, c3 FROM ft1 WHERE c1 = $1 AND c3 = $2;
EXECUTE st1(1, '00001');
EXECUTE st1(2, 'no such row');
DEALLOCATE st1;
RESET plan_cache_mode;
-- correlated scalar subquery: parameterized rescan per outer row
SELECT v.id, (SELECT c3 FROM ft1 WHERE c1 = v.id) AS c3
FROM (VALUES (1), (2), (3)) v(id) ORDER BY v.id;
-- unparameterized rescans
SET enable_material TO off;
SELECT count(*) FROM (VALUES (1), (2), (3)) v(x) CROSS JOIN ft1;
RESET enable_material;

-- ===================================================================
-- transactions: remote xact tracking, savepoints, abort cleanup
-- ===================================================================
BEGIN;
SELECT c1 FROM ft1 WHERE c1 = 1 ORDER BY c1;
SAVEPOINT s1;
SELECT c1 FROM ft1 WHERE c1 = 2 ORDER BY c1;
SAVEPOINT s2;
SELECT c1 FROM ft1 WHERE c1 = 3 ORDER BY c1;
ROLLBACK TO SAVEPOINT s2;
SELECT c1 FROM ft1 WHERE c1 = 4 ORDER BY c1;
RELEASE SAVEPOINT s1;
COMMIT;
SELECT c1 FROM ft1 WHERE c1 = 5 ORDER BY c1;
BEGIN;
SELECT c1 FROM ft1 WHERE c1 = 6 ORDER BY c1;
ROLLBACK;
SELECT c1 FROM ft1 WHERE c1 = 7 ORDER BY c1;
-- an error inside a subtransaction; connection stays usable
BEGIN;
SELECT c1 FROM ft1 WHERE c1 = 8 ORDER BY c1;
SAVEPOINT s1;
SELECT 1 / 0;
ROLLBACK TO SAVEPOINT s1;
SELECT c1 FROM ft1 WHERE c1 = 9 ORDER BY c1;
COMMIT;

-- ===================================================================
-- remote-error propagation (SQLSTATE + remote SQL command context)
-- ===================================================================
CREATE FOREIGN TABLE ft_missing (c1 int)
  SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 0');
SELECT * FROM ft_missing;
-- remote division by zero (pushed-down qual)
SELECT c1 FROM ft1 WHERE c2 = 1 / (c1 - 1) ORDER BY c1;

-- ===================================================================
-- reconnect after option changes (C corpus block; terse hides the
-- engine-specific connection DETAIL lines)
-- ===================================================================
\set VERBOSITY terse
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
ALTER SERVER loopback OPTIONS (SET dbname 'no such database');
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER loopback
            OPTIONS (SET dbname '$$||current_database()||$$')$$;
    END;
$d$;
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (SET user 'no such user');
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
DO $d$
    BEGIN
        EXECUTE $$ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
            OPTIONS (SET user '$$||current_user||$$')$$;
    END;
$d$;
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
\set VERBOSITY default

-- ===================================================================
-- PREPARE TRANSACTION is refused once the fdw touched the transaction
-- ===================================================================
BEGIN;
SELECT c1 FROM ft1 WHERE c1 = 1 ORDER BY c1;
PREPARE TRANSACTION 'fdw_should_fail';
ROLLBACK;

-- ===================================================================
-- system columns on a base foreign scan: xmin/xmax/cmin/cmax read as
-- zero (make_tuple_from_result_row stomps them), ctid/tableoid as usual
-- ===================================================================
SELECT xmin, xmax, cmin, cmax, c1 FROM ft1 WHERE c1 < 3 ORDER BY c1;
SELECT ctid, tableoid::regclass, xmin, c1 FROM ft1 WHERE c1 < 3 ORDER BY c1;

-- ===================================================================
-- pushed-down join: tableoid of the nullable side is the local relation
-- OID (TableOidAttributeNumber = -6); whole-row row identity of an
-- UPDATE target under a pushed-down join converts as the table's type
-- ===================================================================
CREATE TABLE "S 1"."T 3" (id int primary key, a int);
INSERT INTO "S 1"."T 3" VALUES (1, 10), (2, 20), (4, 40);
CREATE TABLE "S 1"."T 4" (id int primary key, v text);
INSERT INTO "S 1"."T 4" VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE FOREIGN TABLE ft3 (id int, a int)
  SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 3');
CREATE FOREIGN TABLE ft4 (id int, v text)
  SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 4');
ANALYZE ft3;
ANALYZE ft4;
ALTER SERVER loopback OPTIONS (ADD use_remote_estimate 'true');
SELECT ft4.id, ft3.tableoid::regclass FROM ft4 LEFT JOIN ft3 ON ft4.id = ft3.id ORDER BY ft4.id;
WITH u AS (
  UPDATE ft3 SET a = ft4.id FROM ft4 WHERE ft4.id = ft3.id RETURNING ft3.id, ft3.a, ft4.v
) SELECT * FROM u ORDER BY id;
SELECT * FROM ft3 ORDER BY id;
ALTER SERVER loopback OPTIONS (DROP use_remote_estimate);

-- ===================================================================
-- rescan with unchanged parameters reuses the fetched batch (chgParam
-- is NULL): the remote nextval() is not re-executed per outer row
-- ===================================================================
CREATE SEQUENCE "S 1".seq1;
CREATE VIEW "S 1".vseq AS
  SELECT g AS id, nextval('"S 1".seq1') AS n FROM generate_series(1, 50) g;
CREATE FOREIGN TABLE ftseq (id int, n bigint)
  SERVER loopback OPTIONS (schema_name 'S 1', table_name 'vseq');
ANALYZE ftseq;
CREATE TABLE loc_big AS SELECT g AS x FROM generate_series(1, 20000) g;
ANALYZE loc_big;
SET enable_material = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET plan_cache_mode = force_generic_plan;
PREPARE stseq(int) AS
  SELECT count(DISTINCT ftseq.n) AS distinct_n, count(*) AS rows
  FROM loc_big, ftseq WHERE ftseq.id > $1 AND loc_big.x <= 2;
EXECUTE stseq(0);
DEALLOCATE stseq;
RESET plan_cache_mode;
RESET enable_material;
RESET enable_hashjoin;
RESET enable_mergejoin;

-- ===================================================================
-- aggregate pushdown is refused when the aggregates of a non-shippable
-- target carry conflicting non-default collations (aggvars as a list)
-- ===================================================================
CREATE TABLE "S 1"."T 5" (id int, c1 text COLLATE "C", c2 text COLLATE "POSIX");
INSERT INTO "S 1"."T 5" VALUES (1, 'a', 'b'), (2, 'c', 'd');
CREATE FOREIGN TABLE ft5 (id int, c1 text COLLATE "C", c2 text COLLATE "POSIX")
  SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 5');
CREATE FUNCTION local_wrap(text) RETURNS text IMMUTABLE LANGUAGE plpgsql
  AS $$ BEGIN RETURN $1; END $$;
EXPLAIN (VERBOSE, COSTS OFF) SELECT local_wrap(max(c1) || max(c2)) FROM ft5;
SELECT local_wrap(max(c1) || max(c2)) FROM ft5;
EXPLAIN (VERBOSE, COSTS OFF) SELECT local_wrap(sum(id)::text || count(*)::text) FROM ft5;
SELECT local_wrap(sum(id)::text || count(*)::text) FROM ft5;

-- ===================================================================
-- async: a subtransaction abort while a FETCH is in flight on another
-- connection leaves both connections usable (pgfdw_abort_cleanup)
-- ===================================================================
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback2 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '$$||current_setting('unix_socket_directories')||$$',
                     port '$$||current_setting('port')||$$',
                     dbname '$$||current_database()||$$',
                     async_capable 'true'
            )$$;
        EXECUTE $$CREATE USER MAPPING FOR CURRENT_USER SERVER loopback2
            OPTIONS (user '$$||current_user||$$')$$;
    END;
$d$;
ALTER SERVER loopback OPTIONS (ADD async_capable 'true');
CREATE FOREIGN TABLE fta1 (c1 int OPTIONS (column_name 'C 1'), c2 int)
  SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 1', fetch_size '1');
CREATE FOREIGN TABLE fta2 (c1 int OPTIONS (column_name 'C 1'), c2 int)
  SERVER loopback2 OPTIONS (schema_name 'S 1', table_name 'T 1', fetch_size '1');
BEGIN;
SAVEPOINT s1;
SELECT CASE WHEN tag = 1 AND c1 = 2 THEN c1 / (c1 - 2) ELSE c1 END
  FROM (SELECT 1 AS tag, c1 FROM fta1 UNION ALL SELECT 2 AS tag, c1 FROM fta2) x;
ROLLBACK TO SAVEPOINT s1;
SELECT count(*) FROM fta2;
SELECT count(*) FROM fta1;
SELECT count(*) FROM (SELECT * FROM fta1 UNION ALL SELECT * FROM fta2) x;
COMMIT;
ALTER SERVER loopback OPTIONS (DROP async_capable);
