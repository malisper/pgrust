-- bugs/batch-55-backend-commands-misc: commands crates, single-session legs.
-- Expected captured from C 18.6 (scripts/commands-misc-b55-e2e.sh header).
\set VERBOSITY verbose

-- fp-catalog-namespace-p2#3 / #4: statistics-object and text-search lookups
-- must walk a search_path longer than 1024 entries.
BEGIN;
DO $$ BEGIN FOR i IN 1..1030 LOOP EXECUTE format('CREATE SCHEMA b55s%s', i); END LOOP; END $$;
CREATE TABLE b55s1030.t (a int, b int);
CREATE STATISTICS b55s1030.st ON a, b FROM b55s1030.t;
CREATE TEXT SEARCH CONFIGURATION b55s1030.cfg (COPY = pg_catalog.simple);
SELECT set_config('search_path', string_agg('b55s' || i, ',' ORDER BY i), false) IS NOT NULL FROM generate_series(1, 1030) i;
ALTER STATISTICS st SET STATISTICS 100;
SELECT stxstattarget FROM pg_statistic_ext WHERE stxname = 'st';
CREATE TEXT SEARCH CONFIGURATION b55s1.copied (COPY = cfg);
SELECT cfgname FROM pg_ts_config WHERE cfgname = 'copied';
ROLLBACK;

-- fp-commands-alter#2: ALTER TABLESPACE ... OWNER TO takes the shared-object
-- AccessExclusiveLock (objectaddress.c:1176) for the transaction.
BEGIN;
ALTER TABLESPACE pg_default OWNER TO CURRENT_USER;
SELECT mode FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'object'
  AND classid = 'pg_tablespace'::regclass
  AND objid = (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default');
ROLLBACK;

-- fp-commands-cluster#1: CLUSTER marks the surviving relfilenumber new in
-- the subtransaction, so COPY FREEZE in the same transaction is allowed.
CREATE TABLE b55_t6 (a integer);
CREATE INDEX b55_t6i ON b55_t6 (a);
BEGIN;
CLUSTER b55_t6 USING b55_t6i;
COPY b55_t6 FROM STDIN WITH (FREEZE);
1
2
\.
COMMIT;
SELECT a FROM b55_t6 ORDER BY a;
DROP TABLE b55_t6;

-- fp-commands-policy#2: both policy expressions are transformed before
-- either has collations assigned (policy.c:652-659).
CREATE TABLE b55_t19 (i int);
CREATE POLICY p ON b55_t19 USING (('a' COLLATE "C") = ('b' COLLATE "POSIX")) WITH CHECK (missing_column > 0);
CREATE POLICY p ON b55_t19 USING (('a' COLLATE "C") = ('b' COLLATE "POSIX"));
DROP TABLE b55_t19;

-- fp-commands-sequence#1: sequence statements queued by ALTER TABLE carry the
-- utility ParseState, so conflicting options report the cursor position.
CREATE TABLE b55_t23 (id bigint NOT NULL);
ALTER TABLE b55_t23 ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (INCREMENT 1 INCREMENT 2);
ALTER TABLE b55_t23 ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;
ALTER TABLE b55_t23 ALTER COLUMN id SET INCREMENT 1 SET INCREMENT 2;
ALTER TABLE b55_t23 ADD COLUMN s serial, ALTER COLUMN id SET START 5 SET START 6;
DROP TABLE b55_t23;
