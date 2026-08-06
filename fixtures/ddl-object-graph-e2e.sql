-- ddl-object-graph lane corpus (unported-census 2026-08-05 lane 3):
-- DROP ... CASCADE across the doDeletion object-class matrix
-- (dependency.c findDependentObjects -> reportDependentObjects ->
-- deleteObjectsInList), dependency-blocked RESTRICT drops, and the
-- wrong-relkind trigger DDL errors (trigger.c CreateTriggerFiringOn /
-- RemoveTriggerById / renametrig over errdetail_relkind_not_supported).
-- Byte-diffed pgrust vs C 18.

-- === object introspection over arbitrary class OIDs ======================
-- C: elog(ERROR, "unsupported object class: %u") — clean error, never panic
SELECT pg_describe_object(16384, 1, 0);
SELECT pg_describe_object(0, 0, 0);
SELECT pg_identify_object(16384, 1, 0);
SELECT pg_identify_object(0, 0, 0);
SELECT pg_identify_object_as_address(16384, 1, 0);
SELECT pg_identify_object('pg_class'::regclass, 0, 0);
SELECT pg_describe_object('pg_class'::regclass, 0, 0);

-- === wrong-relkind trigger DDL: C raises clean 42809 =====================
CREATE TABLE trg_tbl (a int);
CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS
  $$ BEGIN RETURN NEW; END $$;
CREATE SEQUENCE trg_seq;
CREATE MATERIALIZED VIEW trg_mv AS SELECT 1 AS x;
CREATE INDEX trg_idx ON trg_tbl (a);
CREATE TYPE trg_comp AS (a int);
-- each of these must be SQLSTATE 42809 with C's errdetail
CREATE TRIGGER t1 BEFORE INSERT ON trg_seq FOR EACH ROW EXECUTE FUNCTION trg_fn();
CREATE TRIGGER t1 BEFORE INSERT ON trg_mv FOR EACH ROW EXECUTE FUNCTION trg_fn();
CREATE TRIGGER t1 BEFORE INSERT ON trg_idx FOR EACH ROW EXECUTE FUNCTION trg_fn();
CREATE TRIGGER t1 BEFORE INSERT ON trg_comp FOR EACH ROW EXECUTE FUNCTION trg_fn();
-- INSTEAD OF / timing mismatches (42809 texts from CreateTriggerFiringOn)
CREATE TRIGGER t1 INSTEAD OF INSERT ON trg_tbl FOR EACH ROW EXECUTE FUNCTION trg_fn();
CREATE VIEW trg_v AS SELECT * FROM trg_tbl;
CREATE TRIGGER t1 BEFORE INSERT ON trg_v FOR EACH ROW EXECUTE FUNCTION trg_fn();
CREATE TRIGGER t1 BEFORE TRUNCATE ON trg_v FOR EACH STATEMENT EXECUTE FUNCTION trg_fn();
-- ALTER TRIGGER RENAME on a relation without triggers
ALTER TRIGGER nosuch ON trg_seq RENAME TO other;
DROP VIEW trg_v;
DROP MATERIALIZED VIEW trg_mv;
DROP TYPE trg_comp;
DROP SEQUENCE trg_seq;
DROP TABLE trg_tbl;
DROP FUNCTION trg_fn();

-- === DROP ... CASCADE across object classes ==============================
-- table graph: view + trigger + constraint + owned sequence + index + rule
CREATE TABLE base_t (id int PRIMARY KEY, v text);
CREATE SEQUENCE base_seq OWNED BY base_t.id;
CREATE VIEW base_v AS SELECT id FROM base_t;
CREATE VIEW base_vv AS SELECT * FROM base_v;
CREATE FUNCTION base_trgfn() RETURNS trigger LANGUAGE plpgsql AS
  $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER base_trg BEFORE INSERT ON base_t FOR EACH ROW EXECUTE FUNCTION base_trgfn();
CREATE RULE base_rule AS ON DELETE TO base_t DO ALSO NOTHING;
COMMENT ON TABLE base_t IS 'doomed';
-- RESTRICT first: must fail listing dependents (2BP01)
DROP TABLE base_t;
DROP TABLE base_t CASCADE;
DROP FUNCTION base_trgfn();

-- type graph: composite/domain columns force cascades
CREATE TYPE dom_base AS (a int, b text);
CREATE DOMAIN posint AS int CHECK (VALUE > 0);
CREATE TABLE uses_types (c dom_base, d posint);
DROP TYPE dom_base;
DROP TYPE dom_base CASCADE;
DROP DOMAIN posint;
DROP DOMAIN posint CASCADE;
DROP TABLE uses_types;

-- function graph: view over function; default depending on function
CREATE FUNCTION f_one() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE VIEW v_fn AS SELECT f_one() AS x;
CREATE TABLE t_def (a int DEFAULT f_one());
DROP FUNCTION f_one();
DROP FUNCTION f_one() CASCADE;
SELECT count(*) FROM t_def;
INSERT INTO t_def DEFAULT VALUES RETURNING a;
DROP TABLE t_def;

-- operator / opclass-family graph via schema cascade
CREATE SCHEMA opstuff;
CREATE FUNCTION opstuff.int_noteq(int, int) RETURNS bool LANGUAGE sql
  AS $$ SELECT $1 <> $2 $$;
CREATE OPERATOR opstuff.<!> (LEFTARG = int, RIGHTARG = int, FUNCTION = opstuff.int_noteq);
CREATE FUNCTION opstuff.dummy_cmp(int, int) RETURNS int LANGUAGE sql
  AS $$ SELECT sign($1 - $2)::int $$;
CREATE OPERATOR FAMILY opstuff.fam USING btree;
CREATE OPERATOR CLASS opstuff.oc FOR TYPE int USING btree FAMILY opstuff.fam AS
  OPERATOR 3 =, FUNCTION 1 btint4cmp(int, int);
DROP SCHEMA opstuff;
DROP SCHEMA opstuff CASCADE;

-- text search graph: parser/dict/template/config in one schema
CREATE SCHEMA tsobjs;
CREATE TEXT SEARCH TEMPLATE tsobjs.tmpl (LEXIZE = dsimple_lexize);
CREATE TEXT SEARCH DICTIONARY tsobjs.dict (TEMPLATE = simple);
CREATE TEXT SEARCH CONFIGURATION tsobjs.cfg (COPY = simple);
CREATE TEXT SEARCH PARSER tsobjs.prs (
  START = prsd_start, GETTOKEN = prsd_nexttoken,
  END = prsd_end, LEXTYPES = prsd_lextype);
DROP SCHEMA tsobjs CASCADE;

-- collation / conversion / cast / aggregate in a schema cascade
CREATE SCHEMA miscobjs;
CREATE COLLATION miscobjs.c_from (FROM = "C");
CREATE CONVERSION miscobjs.conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;
CREATE TYPE miscobjs.wrap AS (x int);
CREATE FUNCTION miscobjs.to_wrap(int) RETURNS miscobjs.wrap LANGUAGE sql
  AS $$ SELECT ROW($1)::miscobjs.wrap $$;
CREATE CAST (int AS miscobjs.wrap) WITH FUNCTION miscobjs.to_wrap(int);
CREATE AGGREGATE miscobjs.mysum(int) (SFUNC = int4pl, STYPE = int, INITCOND = '0');
DROP SCHEMA miscobjs CASCADE;

-- statistics object rides its table
CREATE TABLE stat_t (a int, b int);
CREATE STATISTICS stat_s (dependencies) ON a, b FROM stat_t;
DROP TABLE stat_t CASCADE;

-- policy rides its table
CREATE TABLE pol_t (a int);
CREATE POLICY pol_p ON pol_t USING (a > 0);
DROP TABLE pol_t;

-- foreign objects: wrapper -> server -> user mapping -> foreign table
CREATE FOREIGN DATA WRAPPER dummy_fdw;
CREATE SERVER dummy_srv FOREIGN DATA WRAPPER dummy_fdw;
CREATE USER MAPPING FOR CURRENT_USER SERVER dummy_srv;
CREATE FOREIGN TABLE ftab (a int) SERVER dummy_srv;
DROP FOREIGN DATA WRAPPER dummy_fdw;
DROP FOREIGN DATA WRAPPER dummy_fdw CASCADE;

-- partitioned table graph: partitions + partitioned index
CREATE TABLE part_t (a int) PARTITION BY RANGE (a);
CREATE TABLE part_t1 PARTITION OF part_t FOR VALUES FROM (0) TO (10);
CREATE INDEX part_idx ON part_t (a);
CREATE VIEW part_v AS SELECT * FROM part_t;
DROP TABLE part_t;
DROP TABLE part_t CASCADE;

-- cannot-drop-required (internal dependency): index behind a constraint,
-- partition index member, toast internals
CREATE TABLE pk_t (a int CONSTRAINT pk_t_pkey PRIMARY KEY);
DROP INDEX pk_t_pkey;
DROP TABLE pk_t;
-- column drop cascades to dependent objects of the column
CREATE TABLE col_t (a int, b int);
CREATE INDEX col_idx ON col_t (b);
CREATE VIEW col_v AS SELECT b FROM col_t;
ALTER TABLE col_t DROP COLUMN b;
ALTER TABLE col_t DROP COLUMN b CASCADE;
DROP TABLE col_t;

-- sequence owned-by: dropping column takes the sequence
CREATE TABLE ser_t (id serial, v text);
DROP TABLE ser_t;
SELECT count(*) FROM pg_class WHERE relname = 'ser_t_id_seq';

-- schema mega-cascade touching many classes at once
CREATE SCHEMA mega;
CREATE TABLE mega.t (a int PRIMARY KEY, b text);
CREATE VIEW mega.v AS SELECT * FROM mega.t;
CREATE SEQUENCE mega.s;
CREATE FUNCTION mega.f() RETURNS int LANGUAGE sql AS $$ SELECT 42 $$;
CREATE TYPE mega.e AS ENUM ('x', 'y');
CREATE TYPE mega.r AS (p int, q text);
CREATE DOMAIN mega.d AS text CHECK (VALUE <> '');
CREATE TABLE mega.uses (ev mega.e, rv mega.r, dv mega.d);
CREATE MATERIALIZED VIEW mega.mv AS SELECT a FROM mega.t;
DROP SCHEMA mega;
DROP SCHEMA mega CASCADE;

-- event trigger drop (pg_event_trigger class through doDeletion)
CREATE FUNCTION evt_fn() RETURNS event_trigger LANGUAGE plpgsql AS
  $$ BEGIN NULL; END $$;
CREATE EVENT TRIGGER evt ON ddl_command_start EXECUTE FUNCTION evt_fn();
DROP FUNCTION evt_fn();
DROP FUNCTION evt_fn() CASCADE;

-- default ACL row through doDeletion (DROP OWNED not needed: ALTER resets)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM PUBLIC;

-- === ALTER ... RENAME across the ExecRenameStmt arms =====================
CREATE FUNCTION rn_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
ALTER FUNCTION rn_f() RENAME TO rn_f2;
CREATE AGGREGATE rn_agg(int) (SFUNC = int4pl, STYPE = int);
ALTER AGGREGATE rn_agg(int) RENAME TO rn_agg2;
CREATE COLLATION rn_coll (FROM = "C");
ALTER COLLATION rn_coll RENAME TO rn_coll2;
CREATE TYPE rn_ty AS (x int);
ALTER TYPE rn_ty RENAME TO rn_ty2;
CREATE DOMAIN rn_dom AS int CHECK (VALUE > 0);
ALTER DOMAIN rn_dom RENAME CONSTRAINT rn_dom_check TO rn_dom_chk;
DROP DOMAIN rn_dom;
DROP TYPE rn_ty2;
DROP COLLATION rn_coll2;
DROP AGGREGATE rn_agg2(int);
DROP FUNCTION rn_f2();

-- === EXCLUDE via ALTER (transformTableConstraint) + constraint deparse ===
CREATE TABLE ex_t (a int);
ALTER TABLE ex_t ADD CONSTRAINT ex_c EXCLUDE (a WITH =);
SELECT pg_get_constraintdef(oid) FROM pg_constraint
  WHERE conrelid = 'ex_t'::regclass ORDER BY conname;
DROP TABLE ex_t;

-- extension-style dependency error is exercised elsewhere; plain re-drop
-- of already-dropped objects for error parity
DROP TABLE base_t;
DROP VIEW base_v;
DROP SCHEMA mega;
