-- bugs/batch-63-backend-optimizer: planner divergences vs C 18.6.
-- Expected captured from C: scripts/regress-diff.sh --capture fixtures/bugs-batch63-e2e.expected \
--   --sql fixtures/bugs-batch63-e2e.sql "$PGINSTALL/postgres"
\set VERBOSITY verbose

-- fp-util-clauses-p2#2: eval_const_expressions folds a SubscriptingRef whose
-- arguments are all Consts even when it carries an assignment, so a NULL
-- subscript in an INSERT target list is a plan-time 22004.
CREATE TABLE b63_t5 (a integer[]);
EXPLAIN INSERT INTO b63_t5 (a[NULL]) VALUES (1);
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO b63_t5 (a[2]) VALUES (7);
INSERT INTO b63_t5 (a[2]) VALUES (7);
SELECT * FROM b63_t5;

-- bug_b2363610: a GROUP BY with no local implementation (one hash-only and
-- one sort-only key type) fails at planning time, before the FDW is asked
-- for a pushed-down grouping path.
CREATE EXTENSION postgres_fdw;
DO $$ BEGIN
  EXECUTE format('CREATE SERVER b63_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L)',
                 current_setting('unix_socket_directories'), current_setting('port'), current_database());
  EXECUTE format('CREATE USER MAPPING FOR CURRENT_USER SERVER b63_srv OPTIONS (user %L)', current_user);
END $$;
CREATE TABLE b63_g(x xid, v tsvector);
INSERT INTO b63_g VALUES ('1'::xid, 'a'::tsvector), ('1'::xid, 'a'::tsvector), ('2'::xid, 'b'::tsvector);
CREATE FOREIGN TABLE b63_fg(x xid, v tsvector) SERVER b63_srv OPTIONS (table_name 'b63_g');
EXPLAIN (COSTS OFF) SELECT x, v FROM b63_fg GROUP BY x, v;
SELECT x, v FROM b63_fg GROUP BY x, v;
EXPLAIN (COSTS OFF) SELECT x, count(*) FROM b63_fg GROUP BY x;
SELECT x, count(*) FROM b63_fg GROUP BY x ORDER BY x::text;
