-- bugs/batch-24-backend-parser: parser/analyzer fixes, expected captured from C 18.6.
\set VERBOSITY verbose
-- fp-commands-tablecmds-p3#1: ATParseTransformCmd qualifies the RangeVar with
-- the relation's schema; the ADD COLUMN ... OPTIONS after-statement targets
-- the right foreign table when search_path resolves the bare name elsewhere.
CREATE FOREIGN DATA WRAPPER b24_w;
CREATE SERVER b24_srv FOREIGN DATA WRAPPER b24_w;
CREATE SCHEMA b24_s;
CREATE FOREIGN TABLE b24_s.ft(a int) SERVER b24_srv;
CREATE FOREIGN TABLE public.ft(b int) SERVER b24_srv;
SET search_path = public;
ALTER FOREIGN TABLE b24_s.ft ADD COLUMN b int OPTIONS (foo 'bar');
SELECT attrelid::regclass, attfdwoptions FROM pg_attribute
  WHERE attrelid IN ('b24_s.ft'::regclass, 'public.ft'::regclass) AND attname = 'b' ORDER BY 1;
-- fp-parser-parse_expr-p1#1: the repeated BETWEEN operand is copied before
-- transformation (a nested WITH analyzed twice tripped the CTE analysis).
SELECT ROW((WITH x AS (SELECT 1 AS n) SELECT n FROM x)) BETWEEN (SELECT 0) AND (SELECT 2);
SELECT ROW((WITH x AS (SELECT 1 AS n) SELECT n FROM x)) NOT BETWEEN (SELECT 0) AND (SELECT 2);
SELECT ROW((WITH x AS (SELECT 1 AS n) SELECT n FROM x)) BETWEEN SYMMETRIC (WITH y AS (SELECT 2 AS m) SELECT m FROM y) AND (SELECT 0);
SELECT ROW((WITH x AS (SELECT 1 AS n) SELECT n FROM x)) NOT BETWEEN SYMMETRIC (SELECT 2) AND (WITH y AS (SELECT 0 AS m) SELECT m FROM y);
-- fp-parser-parse_target#1: expandRecordVariable uses GetCTETargetList, so a
-- data-modifying CTE's RETURNING record column expands.
CREATE TABLE b24_t6 (a integer);
INSERT INTO b24_t6 VALUES (1), (2);
WITH c AS (DELETE FROM b24_t6 WHERE a = 1 RETURNING ROW(a, a + 1) AS r) SELECT (r).* FROM c;
WITH c AS (UPDATE b24_t6 SET a = a + 10 RETURNING ROW(a, a * 2) AS r) SELECT (c.r).f2 FROM c;
WITH c AS (INSERT INTO b24_t6 VALUES (5) RETURNING b24_t6) SELECT (c.b24_t6).* FROM c;
-- fp-parser-parse_utilcmd-p1#1: ALTER TABLE ... ADD EXCLUDE ... INCLUDE keeps
-- the included columns and validates them.
CREATE TABLE b24_t7 (r int4range, payload int, extra text);
ALTER TABLE b24_t7 ADD CONSTRAINT b24_t7_ex EXCLUDE USING gist (r WITH &&) INCLUDE (payload, extra);
SELECT indnatts, indnkeyatts FROM pg_index WHERE indexrelid = 'b24_t7_ex'::regclass;
ALTER TABLE b24_t7 ADD CONSTRAINT b24_t7_ex2 EXCLUDE USING gist (r WITH &&) INCLUDE (nosuch);
-- fp-cache-lsyscache-p2#2: the subscripting handler is identified by its
-- link symbol, so renaming the SQL function keeps hstore subscripting.
CREATE EXTENSION hstore;
ALTER FUNCTION hstore_subscript_handler(internal) RENAME TO b24_renamed_handler;
SELECT ('a=>b'::hstore)['a'];
SELECT h['k'] FROM (SELECT 'k=>v'::hstore AS h) s;
UPDATE b24_t6 SET a = 0 WHERE false;
DO $$ DECLARE h hstore := 'x=>1'; BEGIN h['y'] := '2'; RAISE NOTICE '%', h; END $$;
-- fp-pl-plpgsql-pl_comp#1: $n resolves through the namespace ($0 and a local
-- "$n" shadowing the argument), not only through the argument slots.
CREATE FUNCTION b24_param_shadow(integer) RETURNS integer LANGUAGE plpgsql AS $$ DECLARE "$1" integer := 7; BEGIN RETURN $1 + 0; END $$;
SELECT b24_param_shadow(3);
CREATE FUNCTION b24_param_named(x integer) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RETURN $1 + x; END $$;
SELECT b24_param_named(3);
CREATE FUNCTION b24_param_named_nested(x integer) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN DECLARE "$1" integer := 7; BEGIN RETURN $1 + x; END; END $$;
SELECT b24_param_named_nested(3);
CREATE FUNCTION b24_param_zero(anyelement) RETURNS anyelement LANGUAGE plpgsql AS $$ BEGIN RETURN coalesce($0, $1); END $$;
SELECT b24_param_zero(21), b24_param_zero('t'::text);
CREATE FUNCTION b24_param_two(integer) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RETURN $2; END $$;
SELECT b24_param_two(1);
-- fp-pl-plpgsql-pl_comp#2: a quoted identifier containing a dot is not a
-- label-qualified name.
CREATE FUNCTION b24_dotted_name() RETURNS integer LANGUAGE plpgsql AS $$ <<a>> DECLARE b integer := 1; "a.b" integer := 2; BEGIN RETURN "a.b" * 10 + a.b; END $$;
SELECT b24_dotted_name();
CREATE FUNCTION b24_dotted_rec() RETURNS integer LANGUAGE plpgsql AS $$ <<a>> DECLARE r record; "a.r" integer := 5; BEGIN SELECT 1 AS f INTO r; RETURN a.r.f * 10 + "a.r"; END $$;
SELECT b24_dotted_rec();
-- fp-pl-plpgsql-pl_comp#5: a field the record lacks is 42703 even when core
-- SQL resolution found a same-named table column (variable_conflict=error).
CREATE FUNCTION b24_missing_field() RETURNS integer LANGUAGE plpgsql AS $$ DECLARE r record; BEGIN SELECT 1 AS present INTO r; RETURN (SELECT r.missing FROM (SELECT 42 AS missing) AS r); END $$;
SELECT b24_missing_field();
CREATE FUNCTION b24_missing_field_col() RETURNS integer LANGUAGE plpgsql AS $$ #variable_conflict use_column
DECLARE r record; BEGIN SELECT 1 AS present INTO r; RETURN (SELECT r.missing FROM (SELECT 42 AS missing) AS r); END $$;
SELECT b24_missing_field_col();
CREATE FUNCTION b24_present_field() RETURNS integer LANGUAGE plpgsql AS $$ DECLARE r record; BEGIN SELECT 1 AS present INTO r; RETURN (SELECT r.present FROM (SELECT 42 AS present) AS r); END $$;
SELECT b24_present_field();
CREATE FUNCTION b24_wholerow_ref() RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE r record; BEGIN SELECT 1 AS x INTO r; RETURN (SELECT count(*) FROM (SELECT 42 AS q) AS s WHERE (r.*) IS NOT NULL); END $$;
SELECT b24_wholerow_ref();
CREATE FUNCTION b24_unassigned_field() RETURNS integer LANGUAGE plpgsql AS $$ DECLARE r record; BEGIN RETURN (SELECT r.missing FROM (SELECT 42 AS missing) AS r); END $$;
SELECT b24_unassigned_field();
