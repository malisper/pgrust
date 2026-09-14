-- bugs/batch-70-backend-parser: parser fixes, expected captured from C 18.6.
\set VERBOSITY verbose
-- fp-parser-parse_utilcmd-p1#3 / fp-catalog-namespace-p1#4: parse_utilcmd.c:190
-- checks the TEMP creation namespace (namespace.c:4412) before the columns.
-- Runs first: the session's temp namespace must not exist yet.
CREATE ROLE b70_notemp LOGIN;
SELECT format('REVOKE TEMP ON DATABASE %I FROM PUBLIC', current_database()) \gexec
SET ROLE b70_notemp;
CREATE TEMP TABLE b70_tmp (a nonexistent_type);
CREATE TEMP TABLE b70_tmp (a int);
RESET ROLE;
SELECT format('GRANT TEMP ON DATABASE %I TO PUBLIC', current_database()) \gexec
DROP ROLE b70_notemp;
-- fp-commands-tablecmds-p3#4: transformAlterTableStmt derives isforeign from
-- the relation (parse_utilcmd.c:3580-3585), so column constraints are refused at
-- transformColumnDefinition (parse_utilcmd.c:912-943).
CREATE FOREIGN DATA WRAPPER b70_fdw;
CREATE SERVER b70_srv FOREIGN DATA WRAPPER b70_fdw;
CREATE FOREIGN TABLE b70_ft (a int) SERVER b70_srv;
ALTER FOREIGN TABLE b70_ft ADD COLUMN b int UNIQUE;
ALTER FOREIGN TABLE b70_ft ADD COLUMN b int PRIMARY KEY;
ALTER FOREIGN TABLE b70_ft ADD COLUMN b int REFERENCES b70_ft (a);
ALTER FOREIGN TABLE b70_ft ADD COLUMN b int;
SELECT attname FROM pg_attribute WHERE attrelid = 'b70_ft'::regclass AND attnum > 0 ORDER BY attnum;
DROP FOREIGN TABLE b70_ft;
DROP SERVER b70_srv;
DROP FOREIGN DATA WRAPPER b70_fdw;
-- fp-nodes-nodeFuncs-p2#2 / fp-parser-parse_agg#3: nodeFuncs.c:3171 mutates
-- SubLink.testexpr before the subselect.
CREATE TABLE b70_t (a int, b int, c int);
SELECT a IN (SELECT b) FROM b70_t GROUP BY c;
SELECT t.b IN (SELECT t.c) FROM (VALUES (1,2,3)) AS t(a,b,c) GROUP BY t.a;
SELECT c IN (SELECT b) FROM b70_t GROUP BY c;
-- fp-parser-parse_agg#2: parse_agg.c:1306-1318 checks the target list before
-- HAVING's GROUPING expressions are finalized.
SELECT t.b FROM (VALUES (1,2,3)) AS t(a,b,c) GROUP BY t.a HAVING GROUPING(t.c) = 0;
SELECT t.a FROM (VALUES (1,2,3)) AS t(a,b,c) GROUP BY t.a HAVING GROUPING(t.c) = 0;
SELECT t.a FROM (VALUES (1,2,3)) AS t(a,b,c) GROUP BY t.a HAVING GROUPING(t.a) = 0;
-- fp-parser-gram-p5#1 / fp-parser-gram-p8#1: gram.y:19044 positions the
-- duplicate ORDER BY through exprLocation (GroupingFunc, XmlExpr arms).
(SELECT 1 ORDER BY 1) ORDER BY GROUPING(1);
(SELECT 1 ORDER BY 1) ORDER BY XMLPARSE(DOCUMENT '<a/>');
-- fp-parser-parse_coerce-p1#1: parse_coerce.c:1818 keeps the scalar
-- anycompatible argument's domain when a range over that domain is present.
CREATE DOMAIN b70_d AS integer;
CREATE TYPE b70_dr AS RANGE (subtype = b70_d, subtype_opclass = pg_catalog.int4_ops);
CREATE FUNCTION b70_domain_range_ok(anycompatiblerange, anycompatible) RETURNS boolean LANGUAGE SQL AS 'SELECT true';
SELECT b70_domain_range_ok(NULL::b70_dr, 1::b70_d);
SELECT b70_domain_range_ok(NULL::b70_dr, 1);
SELECT b70_domain_range_ok(NULL::int4range, 1::b70_d);
DROP FUNCTION b70_domain_range_ok(anycompatiblerange, anycompatible);
DROP TYPE b70_dr;
DROP DOMAIN b70_d;
-- fp-parser-parse_collate#2: nodeFuncs.c:2371-2376 walks JsonExpr
-- passing_values before on_empty/on_error.
SELECT JSON_VALUE('{}'::jsonb, '$.missing' PASSING (('a' COLLATE "C") || ('b' COLLATE "POSIX")) AS x RETURNING text DEFAULT (('c' COLLATE "C") || ('d' COLLATE "POSIX")) ON EMPTY);
SELECT JSON_VALUE('{}'::jsonb, '$.missing' PASSING 'a' AS x RETURNING text DEFAULT (('c' COLLATE "C") || ('d' COLLATE "POSIX")) ON EMPTY);
-- fp-parser-parse_target#2: parse_target.c:1177 runs the plpgsql
-- pre-columnref hook (use_variable) before the range-table lookup.
CREATE SCHEMA b70_s1;
CREATE SCHEMA b70_s2;
CREATE TABLE b70_s1.t (a integer);
CREATE TABLE b70_s2.t (a integer);
DO $$
#variable_conflict use_variable
DECLARE t b70_s1.t;
BEGIN
  PERFORM t.* FROM b70_s1.t, b70_s2.t;
END
$$;
DO $$ DECLARE t record; BEGIN SELECT 1 AS a INTO t; PERFORM t.* FROM (VALUES (1)) AS t(a); END $$;
DROP SCHEMA b70_s1 CASCADE;
DROP SCHEMA b70_s2 CASCADE;
-- fp-parser-parse_utilcmd-p1#2: parse_utilcmd.c:2753-2769 rejects a column
-- named twice in an ADD CONSTRAINT ... USING INDEX.
CREATE TABLE b70_u (a int, b int);
CREATE UNIQUE INDEX b70_u_idx ON b70_u (a, a);
ALTER TABLE b70_u ADD CONSTRAINT b70_u_uniq UNIQUE USING INDEX b70_u_idx;
ALTER TABLE b70_u ADD CONSTRAINT b70_u_pk PRIMARY KEY USING INDEX b70_u_idx;
CREATE UNIQUE INDEX b70_u_idx2 ON b70_u (a, b);
ALTER TABLE b70_u ADD CONSTRAINT b70_u_uniq2 UNIQUE USING INDEX b70_u_idx2;
SELECT conname, conkey FROM pg_constraint WHERE conrelid = 'b70_u'::regclass ORDER BY conname;
DROP TABLE b70_u;
DROP TABLE b70_t;
