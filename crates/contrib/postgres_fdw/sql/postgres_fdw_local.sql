-- postgres_fdw phase-1.5 LOCAL suite: everything connection-free.
-- Option-validator battery + the deparse byte-oracle (EXPLAIN VERBOSE
-- "Remote SQL" on base foreign tables; planning never dials the remote when
-- use_remote_estimate is off, in C and here alike).
--
-- Deliberately EXCLUDED (phase-3 planner arms; C would push these down and
-- the plans diverge by design, see notes/contrib-pgfdw-p2.md manifest):
--   joins between foreign tables, aggregates/upper pushdown, ORDER BY
--   pathkeys, LIMIT pushdown, FOR UPDATE/SHARE locking clauses,
--   parameterized join paths, use_remote_estimate.
CREATE EXTENSION postgres_fdw;

CREATE SERVER testserver1 FOREIGN DATA WRAPPER postgres_fdw;
-- Points nowhere; EXPLAIN must never connect.
CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '/nonexistent-postgres-fdw-local', port '1', dbname 'no_such_db');

CREATE USER MAPPING FOR public SERVER testserver1
    OPTIONS (user 'value', password 'value');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

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
) SERVER loopback;
ALTER FOREIGN TABLE ft1 DROP COLUMN c0;
ALTER FOREIGN TABLE ft1 OPTIONS (schema_name 'S 1', table_name 'T 1');
ALTER FOREIGN TABLE ft1 ALTER COLUMN c1 OPTIONS (column_name 'C 1');

-- ===================================================================
-- validator battery (C sql/postgres_fdw.sql "tests for validator")
-- ===================================================================
ALTER SERVER testserver1 OPTIONS (
	use_remote_estimate 'false',
	updatable 'true',
	fdw_startup_cost '123.456',
	fdw_tuple_cost '0.123',
	service 'value',
	connect_timeout 'value',
	dbname 'value',
	host 'value',
	hostaddr 'value',
	port 'value',
	--client_encoding 'value',
	application_name 'value',
	--fallback_application_name 'value',
	keepalives 'value',
	keepalives_idle 'value',
	keepalives_interval 'value',
	tcp_user_timeout 'value',
	-- requiressl 'value',
	sslcompression 'value',
	sslmode 'value',
	sslcert 'value',
	sslkey 'value',
	sslrootcert 'value',
	sslcrl 'value',
	--requirepeer 'value',
	krbsrvname 'value',
	gsslib 'value',
	gssdelegation 'value'
	--replication 'value'
);

-- Error, invalid list syntax
ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- OK but gets a warning
ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
ALTER SERVER testserver1 OPTIONS (DROP extensions);

ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (DROP user, DROP password);

-- Attempt to add a valid option that's not allowed in a user mapping
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD sslmode 'require');

-- But we can add valid ones fine
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD sslpassword 'dummy');

-- Ensure valid options we haven't used in a user mapping yet are
-- permitted to check validation.
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD sslkey 'value', ADD sslcert 'value');

-- OAuth options are not allowed in either context
ALTER SERVER testserver1 OPTIONS (ADD oauth_issuer 'https://example.com');
ALTER SERVER testserver1 OPTIONS (ADD oauth_client_id 'myID');
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD oauth_issuer 'https://example.com');
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD oauth_client_id 'myID');

-- misspelled option gets the closest-match hint
ALTER SERVER testserver1 OPTIONS (ADD use_remote_estimates 'false');
ALTER FOREIGN TABLE ft1 OPTIONS (ADD table_nam 'x');

-- per-type value checks (loopback carries none of these options yet, so
-- these reach the validator, not the duplicate-option core check)
ALTER SERVER loopback OPTIONS (ADD fdw_startup_cost '-1');
ALTER SERVER loopback OPTIONS (ADD fetch_size '0');
ALTER SERVER loopback OPTIONS (ADD fetch_size 'nonint');
ALTER SERVER loopback OPTIONS (ADD use_remote_estimate 'not_a_bool');

\det+

-- ===================================================================
-- deparse byte-oracle: EXPLAIN (VERBOSE, COSTS OFF), base relation only
-- ===================================================================
-- target lists
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT c1 FROM ft1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT c3, c4 FROM ft1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT 'fixed', NULL FROM ft1;
-- whole-row reference
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1;
-- system column
EXPLAIN (VERBOSE, COSTS OFF) SELECT ctid, c1 FROM ft1;
-- plain EXPLAIN shows no Remote SQL (C gates it on VERBOSE)
EXPLAIN (COSTS OFF) SELECT c1 FROM ft1;

-- WHERE with remotely-executable conditions (C corpus block, single-table)
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c3 IS NULL;        -- NullTest
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c3 IS NOT NULL;    -- NullTest
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote

-- more shippability / const-formatting shapes
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 BETWEEN 5 AND 10;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 IN (1, 2, 3);
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 NOT IN (1, 2, 3);
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c2 = 1.0::numeric;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 = -42;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c3 LIKE 'a%';
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c3 = 'don''t';
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c6 = 'foo'::varchar(10);
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c4 = '1970-01-17 00:00:00+00'::timestamptz;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c5 = '1970-01-17'::timestamp;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE CASE WHEN c1 > 0 THEN c2 ELSE 0 END = 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE CASE c2 WHEN 1 THEN c1 ELSE 0 END > 0;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE NOT (c1 = 1);
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 = 1 OR c2 = 2;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c3 IS DISTINCT FROM c6;
-- unsafe collation introduction -> stays local
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c3 = 'foo' COLLATE "C";
-- mixed remote/local conds
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 = 1 AND c8 = 'foo';

-- user-defined operator/function: not shipped by default, shipped once
-- the extension is whitelisted via the extensions option
CREATE FUNCTION postgres_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT c3 FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT c3 FROM ft1 t1 WHERE t1.c1 = t1.c2;
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT c3 FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT c3 FROM ft1 t1 WHERE t1.c1 === t1.c2;
ALTER EXTENSION postgres_fdw ADD FUNCTION postgres_fdw_abs(int);
ALTER EXTENSION postgres_fdw ADD OPERATOR === (int, int);
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT c3 FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT c3 FROM ft1 t1 WHERE t1.c1 === t1.c2;
ALTER SERVER loopback OPTIONS (DROP extensions);

-- remote parameters via a generic plan (PARAM_EXTERN -> $1::type)
SET plan_cache_mode = force_generic_plan;
PREPARE st1(int, text) AS SELECT c1 FROM ft1 WHERE c1 = $1 AND c3 = $2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(101, 'foo');
DEALLOCATE st1;
RESET plan_cache_mode;
