-- FDW catalog-surface matrix: CREATE/ALTER/DROP FOREIGN DATA WRAPPER / SERVER
-- / USER MAPPING / FOREIGN TABLE, options round-trips, validator + dependency
-- error surfaces. Single superuser role (role DDL unported).
CREATE FOREIGN DATA WRAPPER dummy;
CREATE FOREIGN DATA WRAPPER dummy;
CREATE FOREIGN DATA WRAPPER postgresql VALIDATOR postgresql_fdw_validator;
CREATE FOREIGN DATA WRAPPER fdw_opt OPTIONS (a '1', b '2');
CREATE FOREIGN DATA WRAPPER bad_opt OPTIONS ("a=b" 'x');
CREATE FOREIGN DATA WRAPPER dup_opt OPTIONS (a '1', a '2');
CREATE FOREIGN DATA WRAPPER no_such HANDLER nosuchfunction;
CREATE FOREIGN DATA WRAPPER no_such VALIDATOR nosuchfunction;
CREATE FUNCTION not_a_handler() RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE FOREIGN DATA WRAPPER no_such HANDLER not_a_handler;

ALTER FOREIGN DATA WRAPPER nonexistent OPTIONS (a '1');
ALTER FOREIGN DATA WRAPPER fdw_opt OPTIONS (SET a 'one', DROP b, ADD c '3');
ALTER FOREIGN DATA WRAPPER fdw_opt OPTIONS (SET nothere 'x');
ALTER FOREIGN DATA WRAPPER fdw_opt OPTIONS (DROP nothere);
ALTER FOREIGN DATA WRAPPER fdw_opt OPTIONS (ADD a 'again');
ALTER FOREIGN DATA WRAPPER dummy VALIDATOR postgresql_fdw_validator;
ALTER FOREIGN DATA WRAPPER dummy NO VALIDATOR;
ALTER FOREIGN DATA WRAPPER postgresql OPTIONS (nonexistent 'fdw');

SELECT fdwname, fdwhandler, fdwvalidator::regproc, fdwacl, fdwoptions
FROM pg_foreign_data_wrapper ORDER BY fdwname;

CREATE SERVER s1 FOREIGN DATA WRAPPER dummy;
CREATE SERVER s1 FOREIGN DATA WRAPPER dummy;
CREATE SERVER IF NOT EXISTS s1 FOREIGN DATA WRAPPER dummy;
CREATE SERVER s2 TYPE 'oracle' VERSION '1.0' FOREIGN DATA WRAPPER fdw_opt OPTIONS (host 'h', dbname 'db');
CREATE SERVER s3 FOREIGN DATA WRAPPER nonexistent;
CREATE SERVER s4 FOREIGN DATA WRAPPER postgresql OPTIONS (host 'h', hosta 'x');
CREATE SERVER s4 FOREIGN DATA WRAPPER postgresql OPTIONS (host 'h', "user" 'u');
CREATE SERVER s4 FOREIGN DATA WRAPPER postgresql OPTIONS (host 'h', dbname 'db', sslmode 'require');

ALTER SERVER nonexistent VERSION '2.0';
ALTER SERVER s2 VERSION '2.0';
ALTER SERVER s2 VERSION NULL;
ALTER SERVER s2 OPTIONS (SET host 'h2', DROP dbname, ADD port '5433');
ALTER SERVER s2 OPTIONS (SET nothere 'x');
ALTER SERVER s4 OPTIONS (ADD "user" 'u');

SELECT s.srvname, w.fdwname, s.srvtype, s.srvversion, s.srvacl, s.srvoptions
FROM pg_foreign_server s JOIN pg_foreign_data_wrapper w ON s.srvfdw = w.oid
ORDER BY s.srvname;

CREATE USER MAPPING FOR public SERVER s1;
CREATE USER MAPPING FOR public SERVER s1;
CREATE USER MAPPING IF NOT EXISTS FOR public SERVER s1;
CREATE USER MAPPING FOR current_user SERVER s4 OPTIONS ("user" 'guest', password 'secret');
CREATE USER MAPPING FOR USER SERVER s4;
CREATE USER MAPPING FOR public SERVER nonexistent;
CREATE USER MAPPING FOR public SERVER s4 OPTIONS (host 'nope');

ALTER USER MAPPING FOR public SERVER s1 OPTIONS (ADD x '1');
ALTER USER MAPPING FOR public SERVER s2 OPTIONS (ADD x '1');
ALTER USER MAPPING FOR current_user SERVER s4 OPTIONS (SET "user" 'guest2', DROP password);
ALTER USER MAPPING FOR nonexistent_role SERVER s1 OPTIONS (ADD x '1');

SELECT CASE um.umuser WHEN 0 THEN 'public' ELSE 'session_user' END AS mapped,
       s.srvname, um.umoptions
FROM pg_user_mapping um JOIN pg_foreign_server s ON um.umserver = s.oid
ORDER BY 1, 2;

DROP USER MAPPING FOR public SERVER s2;
DROP USER MAPPING FOR public SERVER s2;
DROP USER MAPPING IF EXISTS FOR public SERVER s2;
DROP USER MAPPING IF EXISTS FOR public SERVER nonexistent;
DROP USER MAPPING IF EXISTS FOR nonexistent_role SERVER s1;

CREATE FOREIGN TABLE ft1 (c1 int NOT NULL, c2 text CHECK (c2 <> ''), c3 date) SERVER s1 OPTIONS (delimiter ',', quote '"');
CREATE FOREIGN TABLE ft2 (c1 int PRIMARY KEY) SERVER s1;
CREATE FOREIGN TABLE ft2 (c1 int UNIQUE) SERVER s1;
CREATE FOREIGN TABLE ft2 (c1 int REFERENCES ft1 (c1)) SERVER s1;
CREATE FOREIGN TABLE ft2 (c1 int, PRIMARY KEY (c1)) SERVER s1;
CREATE FOREIGN TABLE ft2 (c1 int) SERVER nonexistent;
CREATE FOREIGN TABLE ft2 (c1 int) SERVER s4 OPTIONS ("user" 'nope');

SELECT c.relname, c.relkind, s.srvname, ft.ftoptions
FROM pg_foreign_table ft
JOIN pg_class c ON c.oid = ft.ftrelid
JOIN pg_foreign_server s ON s.oid = ft.ftserver
ORDER BY c.relname;
SELECT a.attname, a.atttypid::regtype, a.attnotnull
FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid
WHERE c.relname = 'ft1' AND a.attnum > 0 ORDER BY a.attnum;

SELECT * FROM ft1;
EXPLAIN SELECT * FROM ft1;

IMPORT FOREIGN SCHEMA remote_s FROM SERVER s1 INTO public;
IMPORT FOREIGN SCHEMA remote_s LIMIT TO (t1) FROM SERVER nonexistent INTO public;

DROP TABLE ft1;
DROP FOREIGN TABLE nonexistent_ft;
DROP FOREIGN TABLE IF EXISTS nonexistent_ft;
DROP FOREIGN TABLE ft1;
CREATE TABLE plain_t (a int);
DROP FOREIGN TABLE plain_t;
DROP TABLE plain_t;

CREATE FOREIGN TABLE ft3 (c1 int) SERVER s1;
DROP SERVER s1;
DROP SERVER s1 CASCADE;
DROP SERVER IF EXISTS s1;
DROP SERVER nonexistent;
DROP SERVER IF EXISTS nonexistent;

DROP FOREIGN DATA WRAPPER nonexistent;
DROP FOREIGN DATA WRAPPER IF EXISTS nonexistent;
CREATE SERVER s5 FOREIGN DATA WRAPPER fdw_opt;
DROP FOREIGN DATA WRAPPER fdw_opt;
DROP FOREIGN DATA WRAPPER fdw_opt CASCADE;
DROP FOREIGN DATA WRAPPER dummy, postgresql;

SELECT count(*) FROM pg_foreign_data_wrapper;
SELECT count(*) FROM pg_foreign_server;
SELECT count(*) FROM pg_user_mapping;
SELECT count(*) FROM pg_foreign_table;

-- Audit pins (contrib-fdw fix lane).
-- M5: handlerless wrapper resolves the routine before the truncate 0A000.
CREATE FOREIGN DATA WRAPPER pin_fdw;
CREATE SERVER pin_s FOREIGN DATA WRAPPER pin_fdw;
CREATE FOREIGN TABLE pin_ft (a int) SERVER pin_s;
TRUNCATE pin_ft;
-- M1: restrict_nonsystem_relation_kind refuses planning foreign-table access
-- (before the no-handler surface: plancat's restricted error wins).
SET restrict_nonsystem_relation_kind = 'foreign-table';
SELECT * FROM pin_ft;
EXPLAIN SELECT * FROM pin_ft;
RESET restrict_nonsystem_relation_kind;
SELECT * FROM pin_ft;

-- file_fdw-backed pins (handler present).
CREATE EXTENSION file_fdw;
CREATE SERVER pin_file_s FOREIGN DATA WRAPPER file_fdw;
COPY (SELECT g, repeat('x', 10) FROM generate_series(1, 100) g)
  TO '/tmp/pgrust_fdw_e2e_pin.csv' (FORMAT csv);
CREATE FOREIGN TABLE pin_file (a int, b text) SERVER pin_file_s
  OPTIONS (filename '/tmp/pgrust_fdw_e2e_pin.csv', format 'csv');
-- M1: the restricted error also beats the handler-present scan.
SET restrict_nonsystem_relation_kind = 'foreign-table';
SELECT count(*) FROM pin_file;
RESET restrict_nonsystem_relation_kind;
-- M5 fall-through: handler present but no ExecForeignTruncate.
TRUNCATE pin_file;
-- M2: Foreign File shows through a gating Result (pseudoconstant qual).
EXPLAIN (COSTS OFF) SELECT a FROM pin_file WHERE now() > 'epoch'::timestamptz;
-- M3: Foreign File shows inside an InitPlan.
EXPLAIN (COSTS OFF) SELECT (SELECT count(*) FROM pin_file);
-- M4: self-join keeps per-instance size estimates (widths differ, so the
-- no-stats ntuples fallback differs per instance; costs must match C).
EXPLAIN SELECT x.a FROM pin_file x, pin_file y WHERE x.a = y.a AND y.b <> '';

DROP FOREIGN TABLE pin_file;
DROP SERVER pin_file_s;
DROP EXTENSION file_fdw;
DROP FOREIGN TABLE pin_ft;
DROP SERVER pin_s;
DROP FOREIGN DATA WRAPPER pin_fdw;
