-- deck-w4err-earm3: ERROR-ARM error-fuel wave 3 (lane W4-ERR).
-- Residue of LD6/LD8 across the four ERROR-ARM chunks of
-- docs/fuzzing/line-drain-queue.md (ddl-cmds-residue, parser-arms,
-- catalog-residue, ddl-tablecmds). Every probe is per-statement isolated;
-- fixtures are created and dropped inside their own section; erroring
-- probes cannot create state. Hand-verified against BOTH engines before
-- encoding into crates/bin/fuzzgen/src/earm3.rs (verbatim emission).
--
-- Section markers (-- @sec NAME) delimit the future earm3:* shapes.

-- ============================================================ @sec pfk
-- FK-bearing partition trees through attach/detach/alter cycles:
-- CloneFkReferencing/CloneFkReferenced/addFkRecurseReferenced/
-- DetachPartitionFinalize/CloneRowTriggersToPartition (tablecmds.c).
CREATE TABLE ea3_ppk (id int PRIMARY KEY, grp int NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE ea3_ppk1 PARTITION OF ea3_ppk FOR VALUES FROM (0) TO (100);
CREATE TABLE ea3_ppk2 PARTITION OF ea3_ppk FOR VALUES FROM (100) TO (200);
INSERT INTO ea3_ppk VALUES (1, 1), (150, 2);
CREATE TABLE ea3_pfk (fid int REFERENCES ea3_ppk, tag text) PARTITION BY LIST (tag);
CREATE TABLE ea3_pfk_a PARTITION OF ea3_pfk FOR VALUES IN ('a');
INSERT INTO ea3_pfk VALUES (1, 'a');
INSERT INTO ea3_pfk VALUES (77, 'a');
CREATE TABLE ea3_pfk_b (fid int, tag text);
INSERT INTO ea3_pfk_b VALUES (99, 'b');
ALTER TABLE ea3_pfk ATTACH PARTITION ea3_pfk_b FOR VALUES IN ('b');
DELETE FROM ea3_pfk_b;
ALTER TABLE ea3_pfk ATTACH PARTITION ea3_pfk_b FOR VALUES IN ('b');
ALTER TABLE ea3_pfk DETACH PARTITION ea3_pfk_b;
ALTER TABLE ea3_pfk DETACH PARTITION ea3_pfk_b;
ALTER TABLE ea3_pfk ATTACH PARTITION ea3_pfk_b FOR VALUES IN ('b');
DELETE FROM ea3_ppk WHERE id = 1;
UPDATE ea3_ppk SET id = 2 WHERE id = 1;
-- detach a partition of the REFERENCED side (CloneFkReferenced on re-attach)
ALTER TABLE ea3_ppk DETACH PARTITION ea3_ppk2;
ALTER TABLE ea3_ppk ATTACH PARTITION ea3_ppk2 FOR VALUES FROM (100) TO (200);
-- FK NOT VALID on partitioned referencing table
ALTER TABLE ea3_pfk ADD CONSTRAINT ea3_nv FOREIGN KEY (fid) REFERENCES ea3_ppk NOT VALID;
-- self-referential FK on partitioned
ALTER TABLE ea3_ppk ADD CONSTRAINT ea3_selffk FOREIGN KEY (grp) REFERENCES ea3_ppk;
DELETE FROM ea3_ppk;
ALTER TABLE ea3_ppk DROP CONSTRAINT ea3_selffk;
-- row trigger on partitioned parent cloned to a later-attached partition
CREATE FUNCTION ea3_tgf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER ea3_rowtg BEFORE INSERT ON ea3_pfk FOR EACH ROW EXECUTE FUNCTION ea3_tgf();
CREATE TABLE ea3_pfk_c (fid int, tag text);
ALTER TABLE ea3_pfk ATTACH PARTITION ea3_pfk_c FOR VALUES IN ('c');
INSERT INTO ea3_pfk VALUES (150, 'c');
ALTER TABLE ea3_pfk DETACH PARTITION ea3_pfk_c;
DROP TRIGGER ea3_rowtg ON ea3_pfk_c;
ALTER TABLE ea3_pfk ATTACH PARTITION ea3_pfk_c FOR VALUES IN ('c');
-- DETACH FINALIZE when no detach is pending
ALTER TABLE ea3_pfk DETACH PARTITION ea3_pfk_c FINALIZE;
ALTER TABLE ea3_pfk DETACH PARTITION ea3_pfk_c;
ALTER TABLE ea3_pfk DETACH PARTITION nosuch_part;
-- attach with FK rows violating the constraint
CREATE TABLE ea3_pfk_d (fid int, tag text);
INSERT INTO ea3_pfk_d VALUES (9999, 'd');
ALTER TABLE ea3_pfk ATTACH PARTITION ea3_pfk_d FOR VALUES IN ('d');
DROP TABLE ea3_pfk_d;
DROP TABLE ea3_pfk_c;
DROP TABLE ea3_pfk;
DROP TABLE ea3_ppk;
DROP FUNCTION ea3_tgf();

-- ========================================================= @sec idxddl
-- DefineIndex residue (indexcmds.c): partitioned-index arms, capability
-- errors, ALTER INDEX ATTACH PARTITION.
CREATE TABLE ea3_ip (a int NOT NULL, b text, c int) PARTITION BY RANGE (a);
CREATE TABLE ea3_ip1 PARTITION OF ea3_ip FOR VALUES FROM (0) TO (100);
CREATE TABLE ea3_ip2 PARTITION OF ea3_ip FOR VALUES FROM (100) TO (200) PARTITION BY LIST (b);
CREATE UNIQUE INDEX ea3_bad_u ON ea3_ip (b);
CREATE UNIQUE INDEX ea3_ok_u ON ea3_ip (a);
CREATE INDEX CONCURRENTLY ea3_cic ON ea3_ip (c);
CREATE INDEX ea3_only ON ONLY ea3_ip (c);
CREATE INDEX ea3_leaf1 ON ea3_ip1 (c);
ALTER INDEX ea3_only ATTACH PARTITION ea3_leaf1;
ALTER INDEX ea3_only ATTACH PARTITION ea3_leaf1;
ALTER INDEX ea3_leaf1 ATTACH PARTITION ea3_only;
CREATE INDEX ea3_leafx ON ea3_ip1 (b);
ALTER INDEX ea3_only ATTACH PARTITION ea3_leafx;
CREATE TABLE ea3_plain (x int, y int);
CREATE INDEX ea3_pidx ON ea3_plain (x);
ALTER INDEX ea3_only ATTACH PARTITION ea3_pidx;
CREATE UNIQUE INDEX ea3_hash_u ON ea3_plain USING hash (x);
CREATE INDEX ea3_desc_h ON ea3_plain USING hash (x DESC);
CREATE INDEX ea3_inc_h ON ea3_plain USING hash (x) INCLUDE (y);
CREATE UNIQUE INDEX ea3_gin_u ON ea3_plain USING gin (x);
CREATE INDEX ea3_expr_h ON ea3_ip ((a + 1));
ALTER TABLE ea3_ip ADD CONSTRAINT ea3_excl EXCLUDE USING gist (a WITH =);
ALTER TABLE ea3_plain ADD CONSTRAINT ea3_pex EXCLUDE (x WITH =) WHERE (y > 0);
ALTER TABLE ea3_plain DROP CONSTRAINT ea3_pex;
REINDEX INDEX ea3_ok_u;
REINDEX TABLE ea3_ip;
REINDEX INDEX CONCURRENTLY ea3_ok_u;
REINDEX SYSTEM;
REINDEX (CONCURRENTLY) SYSTEM;
REINDEX (TABLESPACE pg_global) TABLE ea3_plain;
REINDEX (TABLESPACE nosuch_ts) TABLE ea3_plain;
DROP TABLE ea3_plain;
DROP TABLE ea3_ip;

-- ========================================================== @sec likei
-- MergeAttributes / expandTableLikeClause / transformTableLikeClause:
-- LIKE INCLUDING variants + inheritance merge conflicts.
CREATE TABLE ea3_src (a int PRIMARY KEY, b text DEFAULT 'x' NOT NULL, c numeric(8,2), d int GENERATED ALWAYS AS (a * 2) STORED, e int GENERATED BY DEFAULT AS IDENTITY, CONSTRAINT ea3_src_chk CHECK (a > 0));
COMMENT ON COLUMN ea3_src.b IS 'bcol';
CREATE INDEX ea3_src_ci ON ea3_src (c) WHERE c > 1;
CREATE STATISTICS ea3_src_st ON a, c FROM ea3_src;
CREATE TABLE ea3_l1 (LIKE ea3_src INCLUDING ALL);
DROP TABLE ea3_l1;
CREATE TABLE ea3_l2 (LIKE ea3_src INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING IDENTITY);
DROP TABLE ea3_l2;
CREATE TABLE ea3_l3 (LIKE ea3_src INCLUDING INDEXES INCLUDING STATISTICS INCLUDING COMMENTS INCLUDING COMPRESSION INCLUDING STORAGE);
DROP TABLE ea3_l3;
CREATE TABLE ea3_l4 (LIKE ea3_src EXCLUDING ALL);
DROP TABLE ea3_l4;
CREATE VIEW ea3_srcv AS SELECT 1 AS q;
CREATE TABLE ea3_l5 (LIKE ea3_srcv);
CREATE SEQUENCE ea3_seq;
CREATE TABLE ea3_l6 (LIKE ea3_seq);
CREATE TYPE ea3_comp AS (f1 int, f2 text);
CREATE TABLE ea3_l7 (LIKE ea3_comp);
DROP TABLE IF EXISTS ea3_l7;
CREATE TABLE ea3_l8 (LIKE nosuch_table);
CREATE TABLE ea3_pl (a int) PARTITION BY RANGE (a);
CREATE TABLE ea3_l9 (LIKE ea3_pl);
DROP TABLE ea3_l9;
DROP TABLE ea3_pl;
-- inheritance merge conflicts
CREATE TABLE ea3_par1 (a int NOT NULL, b text);
CREATE TABLE ea3_par2 (a bigint, c int);
CREATE TABLE ea3_kid () INHERITS (ea3_par1, ea3_par2);
CREATE TABLE ea3_kid (a text) INHERITS (ea3_par1);
CREATE TABLE ea3_par3 (b text DEFAULT 'p3');
CREATE TABLE ea3_par4 (b text DEFAULT 'p4');
CREATE TABLE ea3_kid2 () INHERITS (ea3_par3, ea3_par4);
CREATE TABLE ea3_kid2 (b text DEFAULT 'kid') INHERITS (ea3_par3, ea3_par4);
DROP TABLE ea3_kid2;
CREATE TABLE ea3_gpar (g int GENERATED ALWAYS AS (1) STORED);
CREATE TABLE ea3_gkid (g int) INHERITS (ea3_gpar);
CREATE TABLE ea3_ipar (i int GENERATED ALWAYS AS IDENTITY);
CREATE TABLE ea3_ikid () INHERITS (ea3_ipar);
DROP TABLE IF EXISTS ea3_ikid;
DROP TABLE ea3_ipar;
DROP TABLE ea3_gpar;
DROP TABLE ea3_par4;
DROP TABLE ea3_par3;
DROP TABLE ea3_par2;
DROP TABLE ea3_par1;
DROP TYPE ea3_comp;
DROP SEQUENCE ea3_seq;
DROP VIEW ea3_srcv;
DROP TABLE ea3_src;

-- ========================================================= @sec idxcon
-- transformIndexConstraint residue: PK/UNIQUE USING INDEX arms.
CREATE TABLE ea3_ui (a int NOT NULL, b int, c tsrange);
CREATE UNIQUE INDEX ea3_ui_ab ON ea3_ui (a, b);
CREATE UNIQUE INDEX ea3_ui_expr ON ea3_ui ((a + 1));
CREATE UNIQUE INDEX ea3_ui_part ON ea3_ui (a) WHERE b > 0;
CREATE INDEX ea3_ui_plain ON ea3_ui (a);
CREATE UNIQUE INDEX ea3_ui_desc ON ea3_ui (a DESC NULLS LAST);
ALTER TABLE ea3_ui ADD CONSTRAINT ea3_pk PRIMARY KEY USING INDEX ea3_ui_expr;
ALTER TABLE ea3_ui ADD CONSTRAINT ea3_pk PRIMARY KEY USING INDEX ea3_ui_part;
ALTER TABLE ea3_ui ADD CONSTRAINT ea3_pk PRIMARY KEY USING INDEX ea3_ui_plain;
ALTER TABLE ea3_ui ADD CONSTRAINT ea3_pk PRIMARY KEY USING INDEX nosuch_idx;
ALTER TABLE ea3_ui ADD CONSTRAINT ea3_pk PRIMARY KEY USING INDEX ea3_ui_desc;
ALTER TABLE ea3_ui ADD CONSTRAINT ea3_pk PRIMARY KEY USING INDEX ea3_ui_ab;
ALTER TABLE ea3_ui ADD CONSTRAINT ea3_pk2 UNIQUE USING INDEX ea3_ui_ab;
ALTER TABLE ea3_ui DROP CONSTRAINT ea3_pk;
-- WITHOUT OVERLAPS arms
CREATE TABLE ea3_wo (id int, valid tsrange, PRIMARY KEY (id, valid WITHOUT OVERLAPS));
DROP TABLE ea3_wo;
CREATE TABLE ea3_wo2 (id int, valid tsrange, PRIMARY KEY (valid WITHOUT OVERLAPS));
CREATE TABLE ea3_wo3 (id int, valid text, PRIMARY KEY (id, valid WITHOUT OVERLAPS));
CREATE TABLE ea3_wo4 (id int, valid tsrange, UNIQUE (id, valid WITHOUT OVERLAPS), FOREIGN KEY (id, PERIOD valid) REFERENCES ea3_wo4);
DROP TABLE IF EXISTS ea3_wo4;
DROP TABLE ea3_ui;

-- ======================================================== @sec gentype
-- enforce_generic_type_consistency residue (parse_coerce.c):
-- anycompatible / anyrange / anymultirange mixed-argument failures.
CREATE FUNCTION ea3_ac2(x anycompatible, y anycompatiblearray) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
SELECT ea3_ac2(1, ARRAY['a','b']);
SELECT ea3_ac2(1, 2);
SELECT ea3_ac2(NULL, NULL);
SELECT ea3_ac2('x', ARRAY[1]);
DROP FUNCTION ea3_ac2(anycompatible, anycompatiblearray);
CREATE FUNCTION ea3_acn(x anycompatiblenonarray) RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;
SELECT ea3_acn(ARRAY[1,2]);
SELECT ea3_acn(1);
DROP FUNCTION ea3_acn(anycompatiblenonarray);
CREATE FUNCTION ea3_acr(r anycompatiblerange, e anycompatible) RETURNS int LANGUAGE sql AS $$ SELECT 3 $$;
SELECT ea3_acr(int4range(1,5), 'abc');
SELECT ea3_acr(int4range(1,5), 2.5);
SELECT ea3_acr(NULL, 1);
DROP FUNCTION ea3_acr(anycompatiblerange, anycompatible);
CREATE FUNCTION ea3_mr(m anymultirange, r anyrange) RETURNS int LANGUAGE sql AS $$ SELECT 4 $$;
SELECT ea3_mr(int4multirange(), numrange(1,2));
SELECT ea3_mr(NULL, NULL);
SELECT ea3_mr(int4multirange(), int4range(1,2));
DROP FUNCTION ea3_mr(anymultirange, anyrange);
CREATE FUNCTION ea3_ae(a anyelement, b anyarray) RETURNS anyelement LANGUAGE sql AS $$ SELECT a $$;
SELECT ea3_ae(1, ARRAY['x']);
SELECT ea3_ae(NULL::int, NULL);
DROP FUNCTION ea3_ae(anyelement, anyarray);
CREATE FUNCTION ea3_enum(e anyenum) RETURNS int LANGUAGE sql AS $$ SELECT 5 $$;
SELECT ea3_enum(1);
SELECT ea3_enum(NULL);
DROP FUNCTION ea3_enum(anyenum);
CREATE FUNCTION ea3_badret(x anycompatible) RETURNS anycompatiblerange LANGUAGE sql AS $$ SELECT int4range(1,2) $$;
CREATE FUNCTION ea3_badret2() RETURNS anyelement LANGUAGE sql AS $$ SELECT 1 $$;

-- ========================================================= @sec aclres
-- objectsInSchemaToOids / RemoveRoleFromObjectACL / aclcheck NO_PRIV
-- residue (aclchk.c).
CREATE ROLE ea3_u1;
CREATE ROLE ea3_u2;
CREATE SCHEMA ea3_s;
CREATE TABLE ea3_s.t1 (a int);
CREATE SEQUENCE ea3_s.sq1;
CREATE FUNCTION ea3_s.f1() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE PROCEDURE ea3_s.p1() LANGUAGE sql AS $$ SELECT 1 $$;
GRANT ALL ON ALL TABLES IN SCHEMA ea3_s TO ea3_u1;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ea3_s TO ea3_u1;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ea3_s TO ea3_u1;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA ea3_s TO ea3_u1;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA ea3_s TO ea3_u1;
GRANT ALL ON ALL TABLES IN SCHEMA nosuch_schema TO ea3_u1;
REVOKE ALL ON ALL TABLES IN SCHEMA ea3_s FROM ea3_u1;
REVOKE ALL ON ALL ROUTINES IN SCHEMA ea3_s FROM ea3_u1;
ALTER DEFAULT PRIVILEGES IN SCHEMA ea3_s GRANT SELECT ON TABLES TO ea3_u2;
ALTER DEFAULT PRIVILEGES IN SCHEMA ea3_s GRANT USAGE ON SEQUENCES TO ea3_u2;
DROP ROLE ea3_u2;
ALTER DEFAULT PRIVILEGES IN SCHEMA ea3_s REVOKE SELECT ON TABLES FROM ea3_u2;
DROP OWNED BY ea3_u2;
ALTER DEFAULT PRIVILEGES IN SCHEMA ea3_s REVOKE ALL ON TABLES FROM ea3_u2;
ALTER DEFAULT PRIVILEGES IN SCHEMA ea3_s REVOKE ALL ON SEQUENCES FROM ea3_u2;
DROP ROLE ea3_u2;
-- NO_PRIV arms under an unprivileged role
GRANT USAGE ON SCHEMA ea3_s TO ea3_u1;
SET ROLE ea3_u1;
SELECT * FROM ea3_s.t1;
INSERT INTO ea3_s.t1 VALUES (1);
SELECT nextval('ea3_s.sq1');
SELECT setval('ea3_s.sq1', 10);
SELECT ea3_s.f1();
CALL ea3_s.p1();
CREATE TABLE ea3_s.hack (h int);
LOCK TABLE ea3_s.t1;
TRUNCATE ea3_s.t1;
COMMENT ON TABLE ea3_s.t1 IS 'nope';
ANALYZE ea3_s.t1;
VACUUM ea3_s.t1;
RESET ROLE;
REVOKE USAGE ON SCHEMA ea3_s FROM ea3_u1;
SET ROLE ea3_u1;
SELECT ea3_s.f1();
RESET ROLE;
DROP SCHEMA ea3_s CASCADE;
DROP ROLE ea3_u1;

-- ========================================================== @sec roles
-- CreateRole/AlterRole grantor arms (user.c): CREATEROLE without
-- SUPERUSER attempting elevated attributes.
CREATE ROLE ea3_cr LOGIN CREATEROLE PASSWORD 'x';
SET ROLE ea3_cr;
CREATE ROLE ea3_sub;
CREATE ROLE ea3_bad SUPERUSER;
CREATE ROLE ea3_bad REPLICATION;
CREATE ROLE ea3_bad BYPASSRLS;
ALTER ROLE ea3_sub SUPERUSER;
ALTER ROLE ea3_sub REPLICATION;
ALTER ROLE ea3_sub BYPASSRLS;
ALTER ROLE ea3_cr CREATEDB;
DROP ROLE ea3_sub;
RESET ROLE;
CREATE ROLE ea3_m1;
CREATE ROLE ea3_m2 IN ROLE ea3_m1;
GRANT ea3_m1 TO ea3_m2;
GRANT ea3_m2 TO ea3_m1;
GRANT ea3_m1 TO ea3_m1;
GRANT nosuch_role TO ea3_m1;
GRANT ea3_m1 TO nosuch_role;
GRANT ea3_m1 TO ea3_m2 WITH ADMIN OPTION;
REVOKE ADMIN OPTION FOR ea3_m1 FROM ea3_m2;
REVOKE ea3_m1 FROM ea3_m2;
ALTER ROLE ea3_m1 SET work_mem = '7MB';
ALTER ROLE ea3_m1 IN DATABASE fuzz SET work_mem = '8MB';
ALTER ROLE ea3_m1 IN DATABASE nosuch_db SET work_mem = '8MB';
ALTER ROLE ea3_m1 RESET ALL;
ALTER ROLE ALL SET statement_timeout = 0;
ALTER ROLE ALL RESET statement_timeout;
CREATE ROLE ea3_m1;
CREATE ROLE pg_bogus;
ALTER ROLE nosuch_role LOGIN;
DROP ROLE ea3_m2;
DROP ROLE ea3_m1;
DROP ROLE ea3_cr;

-- ========================================================== @sec trunc
-- ExecuteTruncateGuts (tablecmds.c): FK-blocked truncate, CASCADE,
-- RESTART IDENTITY, partitioned trees, ONLY.
CREATE TABLE ea3_tp (id int PRIMARY KEY);
CREATE TABLE ea3_tc (fid int REFERENCES ea3_tp);
INSERT INTO ea3_tp VALUES (1);
INSERT INTO ea3_tc VALUES (1);
TRUNCATE ea3_tp;
TRUNCATE ea3_tp, ea3_tc;
TRUNCATE ea3_tp CASCADE;
CREATE TABLE ea3_ti (id int GENERATED ALWAYS AS IDENTITY, v int);
INSERT INTO ea3_ti (v) VALUES (10), (20);
TRUNCATE ea3_ti RESTART IDENTITY;
TRUNCATE ea3_ti CONTINUE IDENTITY;
CREATE TABLE ea3_tpart (a int) PARTITION BY LIST (a);
CREATE TABLE ea3_tpart1 PARTITION OF ea3_tpart FOR VALUES IN (1);
INSERT INTO ea3_tpart VALUES (1);
TRUNCATE ONLY ea3_tpart;
TRUNCATE ea3_tpart;
TRUNCATE ea3_tpart1, ea3_tpart;
CREATE VIEW ea3_tv AS SELECT 1 AS x;
TRUNCATE ea3_tv;
TRUNCATE pg_class;
CREATE TABLE ea3_inh_p (a int);
CREATE TABLE ea3_inh_c () INHERITS (ea3_inh_p);
INSERT INTO ea3_inh_c VALUES (5);
TRUNCATE ea3_inh_p;
TRUNCATE ONLY ea3_inh_p;
DROP TABLE ea3_inh_c;
DROP TABLE ea3_inh_p;
DROP VIEW ea3_tv;
DROP TABLE ea3_tpart;
DROP TABLE ea3_ti;
DROP TABLE ea3_tc;
DROP TABLE ea3_tp;

-- ======================================================== @sec dropcon
-- dropconstraint_internal + rename_constraint_internal: inherited
-- constraint arms.
CREATE TABLE ea3_dp (a int, CONSTRAINT ea3_chk CHECK (a > 0));
CREATE TABLE ea3_dc () INHERITS (ea3_dp);
ALTER TABLE ea3_dc DROP CONSTRAINT ea3_chk;
ALTER TABLE ONLY ea3_dp DROP CONSTRAINT ea3_chk;
ALTER TABLE ea3_dp DROP CONSTRAINT ea3_chk;
ALTER TABLE ea3_dp DROP CONSTRAINT nosuch_con;
ALTER TABLE ea3_dp DROP CONSTRAINT IF EXISTS nosuch_con;
CREATE TABLE ea3_rp (a int, CONSTRAINT ea3_rchk CHECK (a > 0));
CREATE TABLE ea3_rc () INHERITS (ea3_rp);
ALTER TABLE ea3_rc RENAME CONSTRAINT ea3_rchk TO ea3_rchk2;
ALTER TABLE ONLY ea3_rp RENAME CONSTRAINT ea3_rchk TO ea3_rchk2;
ALTER TABLE ea3_rp RENAME CONSTRAINT ea3_rchk TO ea3_rchk2;
ALTER TABLE ea3_rp RENAME CONSTRAINT nosuch_con TO x;
-- NOT NULL drop when PK depends
CREATE TABLE ea3_nn (a int PRIMARY KEY, b int NOT NULL);
ALTER TABLE ea3_nn ALTER COLUMN a DROP NOT NULL;
ALTER TABLE ea3_nn ALTER COLUMN b DROP NOT NULL;
ALTER TABLE ea3_nn ALTER COLUMN nosuch DROP NOT NULL;
CREATE TABLE ea3_pnn (a int NOT NULL) PARTITION BY RANGE (a);
ALTER TABLE ea3_pnn ALTER COLUMN a DROP NOT NULL;
DROP TABLE ea3_pnn;
DROP TABLE ea3_nn;
DROP TABLE ea3_rc;
DROP TABLE ea3_rp;
DROP TABLE ea3_dc;
DROP TABLE ea3_dp;

-- ========================================================= @sec altcol
-- ATPrep/ATExecAlterColumnType + ATPostAlterTypeParse/Cleanup residue:
-- dependent-object re-derivation and refusal arms.
CREATE TABLE ea3_ac (a int PRIMARY KEY, b int, c int, t text);
CREATE INDEX ea3_ac_ei ON ea3_ac ((b * 2));
CREATE TABLE ea3_acr2 (r int REFERENCES ea3_ac);
CREATE VIEW ea3_acv AS SELECT b FROM ea3_ac;
CREATE FUNCTION ea3_wf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER ea3_wtg BEFORE UPDATE ON ea3_ac FOR EACH ROW WHEN (OLD.c IS DISTINCT FROM NEW.c) EXECUTE FUNCTION ea3_wf();
ALTER TABLE ea3_ac ALTER COLUMN b TYPE bigint;
ALTER TABLE ea3_ac ALTER COLUMN c TYPE bigint;
ALTER TABLE ea3_ac ALTER COLUMN a TYPE bigint;
ALTER TABLE ea3_ac ALTER COLUMN t TYPE varchar(2);
INSERT INTO ea3_ac VALUES (1, 2, 3, 'abc');
ALTER TABLE ea3_ac ALTER COLUMN t TYPE varchar(2);
ALTER TABLE ea3_ac ALTER COLUMN t TYPE varchar(8);
-- generated column depending on altered column
CREATE TABLE ea3_gd (base int, dbl int GENERATED ALWAYS AS (base * 2) STORED);
ALTER TABLE ea3_gd ALTER COLUMN base TYPE numeric;
ALTER TABLE ea3_gd ALTER COLUMN base TYPE bigint;
DROP TABLE ea3_gd;
-- partition key column
CREATE TABLE ea3_pk2 (k int, v int) PARTITION BY HASH (k);
ALTER TABLE ea3_pk2 ALTER COLUMN k TYPE bigint;
ALTER TABLE ea3_pk2 ALTER COLUMN v TYPE bigint;
DROP TABLE ea3_pk2;
-- typed table / composite dependency
CREATE TYPE ea3_ct AS (x int, y int);
CREATE TABLE ea3_tt OF ea3_ct;
ALTER TABLE ea3_tt ALTER COLUMN x TYPE bigint;
ALTER TYPE ea3_ct ALTER ATTRIBUTE x TYPE bigint;
ALTER TYPE ea3_ct ALTER ATTRIBUTE x TYPE bigint CASCADE;
DROP TABLE ea3_tt;
DROP TYPE ea3_ct;
DROP VIEW ea3_acv;
DROP TABLE ea3_acr2;
DROP TABLE ea3_ac;
DROP FUNCTION ea3_wf();

-- ======================================================== @sec wrongk
-- alter_table_type_to_string / ATWrongRelkindError + ATExecChangeOwner:
-- ALTER TABLE subcommands applied to the wrong relation kind.
CREATE VIEW ea3_wv AS SELECT 1 AS a;
CREATE SEQUENCE ea3_ws;
CREATE MATERIALIZED VIEW ea3_wm AS SELECT 1 AS a;
CREATE TABLE ea3_wt (a int);
CREATE INDEX ea3_wi ON ea3_wt (a);
ALTER TABLE ea3_wv ADD COLUMN z int;
ALTER TABLE ea3_wv ADD CONSTRAINT c CHECK (a > 0);
ALTER TABLE ea3_ws ADD COLUMN z int;
ALTER TABLE ea3_ws ALTER COLUMN last_value TYPE bigint;
ALTER TABLE ea3_wm ADD CONSTRAINT c CHECK (a > 0);
ALTER TABLE ea3_wi ADD COLUMN z int;
ALTER TABLE ea3_wv SET LOGGED;
ALTER TABLE ea3_wm SET UNLOGGED;
ALTER TABLE ea3_ws ENABLE ROW LEVEL SECURITY;
ALTER TABLE ea3_wv CLUSTER ON nosuch;
ALTER TABLE ea3_wv SET WITHOUT CLUSTER;
ALTER TABLE ea3_wv INHERIT ea3_wt;
ALTER TABLE ea3_wv OF ea3_ct2;
ALTER VIEW ea3_wt RESET (security_barrier);
ALTER SEQUENCE ea3_wt RESTART;
ALTER MATERIALIZED VIEW ea3_wt SET (fillfactor = 70);
ALTER INDEX ea3_wt SET (fillfactor = 70);
-- owner arms
ALTER TABLE ea3_wi OWNER TO postgres;
ALTER SEQUENCE ea3_ws OWNER TO nosuch_role;
CREATE ROLE ea3_wown;
SET ROLE ea3_wown;
ALTER TABLE ea3_wt OWNER TO ea3_wown;
ALTER VIEW ea3_wv OWNER TO ea3_wown;
RESET ROLE;
ALTER TABLE ea3_wt OWNER TO ea3_wown;
ALTER TABLE ea3_wt OWNER TO postgres;
DROP ROLE ea3_wown;
-- ALTER TABLE ALL IN TABLESPACE early error arms
ALTER TABLE ALL IN TABLESPACE nosuch_ts SET TABLESPACE pg_default;
ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE pg_default;
ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE nosuch_ts;
ALTER INDEX ALL IN TABLESPACE nosuch_ts SET TABLESPACE pg_default;
ALTER MATERIALIZED VIEW ALL IN TABLESPACE nosuch_ts SET TABLESPACE pg_default;
DROP INDEX ea3_wi;
DROP TABLE ea3_wt;
DROP MATERIALIZED VIEW ea3_wm;
DROP SEQUENCE ea3_ws;
DROP VIEW ea3_wv;

-- ======================================================== @sec addcol
-- ATExecAddColumn / ATExecDropColumn recursion + conflict arms.
CREATE TABLE ea3_ap (a int, b text);
CREATE TABLE ea3_ak (c int) INHERITS (ea3_ap);
ALTER TABLE ea3_ap ADD COLUMN c int;
ALTER TABLE ea3_ap ADD COLUMN c text;
ALTER TABLE ea3_ap ADD COLUMN IF NOT EXISTS c int;
ALTER TABLE ea3_ak ADD COLUMN a int;
ALTER TABLE ONLY ea3_ap ADD COLUMN d int;
ALTER TABLE ea3_ak DROP COLUMN a;
ALTER TABLE ONLY ea3_ap DROP COLUMN b;
ALTER TABLE ea3_ak DROP COLUMN b;
ALTER TABLE ea3_ap DROP COLUMN nosuch;
ALTER TABLE ea3_ap DROP COLUMN IF EXISTS nosuch;
ALTER TABLE ea3_ap DROP COLUMN xmin;
ALTER TABLE ea3_ap ADD COLUMN xmax int;
CREATE DOMAIN ea3_dompos AS int CHECK (VALUE > 0);
CREATE TABLE ea3_dtab (v int);
INSERT INTO ea3_dtab VALUES (-1);
ALTER TABLE ea3_dtab ADD COLUMN dd ea3_dompos DEFAULT -5;
ALTER TABLE ea3_dtab ADD COLUMN dd ea3_dompos DEFAULT 5;
ALTER TABLE ea3_dtab ADD COLUMN nn int NOT NULL;
ALTER TABLE ea3_dtab ADD COLUMN nn int NOT NULL DEFAULT 0;
-- identity column on partitioned table
CREATE TABLE ea3_pid (a int NOT NULL) PARTITION BY RANGE (a);
ALTER TABLE ea3_pid ADD COLUMN gi int GENERATED ALWAYS AS IDENTITY;
DROP TABLE ea3_pid;
DROP TABLE ea3_dtab;
DROP DOMAIN ea3_dompos;
DROP TABLE ea3_ak;
DROP TABLE ea3_ap;

-- ========================================================= @sec castop
-- CreateCast (functioncmds.c) + AlterOperator (operatorcmds.c) +
-- assignProcTypes (opclasscmds.c).
CREATE FUNCTION ea3_c1(int) RETURNS int LANGUAGE sql AS $$ SELECT $1 $$;
CREATE FUNCTION ea3_c2(text) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION ea3_c3(int, int, boolean) RETURNS text LANGUAGE sql AS $$ SELECT 'x' $$;
CREATE CAST (int AS int) WITH FUNCTION ea3_c1(int);
CREATE CAST (text AS int) WITH FUNCTION ea3_c1(int);
CREATE CAST (int AS text) WITH FUNCTION ea3_c3(int, int, boolean);
CREATE CAST (int AS point) WITHOUT FUNCTION;
CREATE CAST (varchar AS text) WITHOUT FUNCTION;
CREATE CAST (int AS money) WITH INOUT;
DROP CAST (int AS money);
DROP CAST (int AS money);
DROP CAST IF EXISTS (int AS money);
CREATE CAST (bigint AS int4) WITH FUNCTION int4(bigint) AS IMPLICIT;
DROP CAST (bigint AS int4);
-- AlterOperator
CREATE FUNCTION ea3_opf(int, int) RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;
CREATE OPERATOR ===^ (leftarg = int, rightarg = int, function = ea3_opf);
ALTER OPERATOR ===^ (int, int) SET (restrict = scalarltsel, join = scalarltjoinsel);
ALTER OPERATOR ===^ (int, int) SET (restrict = nosuch_fn);
ALTER OPERATOR ===^ (int, int) SET (join = nosuch_fn);
ALTER OPERATOR ===^ (int, int) SET (negator = ===^);
ALTER OPERATOR ===^ (int, int) SET (commutator = ===^);
ALTER OPERATOR ===^ (int, int) SET (hashes);
ALTER OPERATOR ===^ (int, int) SET (restrict = NONE, join = NONE);
ALTER OPERATOR nosuch_op (int, int) SET (restrict = scalarltsel);
DROP OPERATOR ===^ (int, int);
DROP FUNCTION ea3_opf(int, int);
-- opclass proc arms
CREATE FUNCTION ea3_cmp(int, int) RETURNS int LANGUAGE sql AS $$ SELECT 0 $$;
CREATE FUNCTION ea3_cmp_wrong(text, text) RETURNS int LANGUAGE sql AS $$ SELECT 0 $$;
CREATE OPERATOR CLASS ea3_oc FOR TYPE int USING btree AS OPERATOR 1 <, FUNCTION 1 ea3_cmp(int, int);
DROP OPERATOR CLASS ea3_oc USING btree;
CREATE OPERATOR CLASS ea3_oc2 FOR TYPE int USING btree AS FUNCTION 9 ea3_cmp(int, int);
CREATE OPERATOR CLASS ea3_oc3 FOR TYPE int USING btree AS OPERATOR 1 <, FUNCTION 1 ea3_cmp_wrong(text, text);
CREATE OPERATOR CLASS ea3_oc4 FOR TYPE int USING btree AS OPERATOR 7 <;
CREATE OPERATOR CLASS ea3_oc5 FOR TYPE int USING hash AS OPERATOR 1 =, FUNCTION 1 ea3_cmp(int, int);
CREATE OPERATOR CLASS ea3_oc6 FOR TYPE int USING nosuch_am AS OPERATOR 1 <;
DROP FUNCTION ea3_cmp_wrong(text, text);
DROP FUNCTION ea3_cmp(int, int);
DROP FUNCTION ea3_opf(int, int);
DROP FUNCTION ea3_c3(int, int, boolean);
DROP FUNCTION ea3_c2(text);
DROP FUNCTION ea3_c1(int);

-- =========================================================== @sec rcte
-- checkWellFormedRecursionWalker (parse_cte.c): recursive-CTE misuse.
CREATE TABLE ea3_r (n int);
INSERT INTO ea3_r VALUES (1);
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT sum(n) FROM t;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t, t AS t2 WHERE n < 3) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n FROM ea3_r LEFT JOIN t ON true) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n FROM t RIGHT JOIN ea3_r ON true) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n FROM t FULL JOIN ea3_r ON true) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT (SELECT max(n) FROM t)) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n IN (SELECT n FROM t)) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t ORDER BY 1 LIMIT 1) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t FOR UPDATE) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t GROUP BY n) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM t) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION SELECT n + 1 FROM t WHERE n < 3) SELECT sum(n) FROM t;
WITH RECURSIVE t(n) AS (SELECT 1 INTERSECT SELECT n + 1 FROM t) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 EXCEPT SELECT n + 1 FROM t) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT n FROM t) SELECT 1;
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3), u(m) AS (SELECT n FROM t UNION ALL SELECT m + 1 FROM u, t WHERE m < 2) SELECT count(*) FROM u;
WITH t AS (SELECT 1 AS n), t AS (SELECT 2 AS n) SELECT 1;
WITH RECURSIVE t(n, n) AS (SELECT 1, 2) SELECT 1;
DROP TABLE ea3_r;

-- ======================================================== @sec colname
-- FigureColnameInternal (parse_target.c): default output column names
-- for expression forms (functional coverage; all succeed, results
-- deterministic single-row).
SELECT CASE WHEN true THEN 1 END;
SELECT CASE 1 WHEN 1 THEN 'a' ELSE 'b' END;
SELECT ARRAY[1, 2];
SELECT ROW(1, 'x');
SELECT COALESCE(NULL, 3);
SELECT GREATEST(1, 2);
SELECT LEAST(1, 2);
SELECT NULLIF(1, 2);
SELECT EXISTS (SELECT 1);
SELECT (SELECT 42);
SELECT CURRENT_DATE > '2000-01-01';
SELECT CURRENT_TIMESTAMP > '2000-01-01';
SELECT LOCALTIMESTAMP > '2000-01-01';
SELECT CURRENT_ROLE = CURRENT_USER;
SELECT SESSION_USER = USER;
SELECT CURRENT_CATALOG;
SELECT CURRENT_SCHEMA;
SELECT CAST(1 AS bigint);
SELECT 1::int8;
SELECT (ROW(1, 2)).f1;
SELECT ('{"a": 1}'::json) -> 'a';
-- (XML colname forms omitted: the in-lane A side is built --without-libxml
-- and raises 0A000 where pgrust's native XML succeeds — rig-config
-- divergence, not a conformance surface; see findings-w4err.md.)
SELECT TREAT(1 AS int);
SELECT position('b' in 'abc');
SELECT substring('abc' from 2 for 1);
SELECT trim(both 'x' from 'xax');
SELECT overlay('abc' placing 'z' from 2);
SELECT collation for ('x'::text);
SELECT normalize('a');
SELECT json_scalar(1);
SELECT json_serialize('{"a":1}');
SELECT json_query('{"a":1}', '$.a');
SELECT json_value('{"a":1}', '$.a');
SELECT json_exists('{"a":1}', '$.a');
SELECT JSON_OBJECT('a': 1);
SELECT JSON_ARRAY(1, 2);
SELECT merge_action();

-- ========================================================= @sec colref
-- transformColumnRef / ParseFuncOrColumn / func_get_detail / expandRTE
-- residue: whole-row, missing-FROM, ambiguity, composite notation.
CREATE TABLE ea3_x (a int, b text);
CREATE TABLE ea3_y (a int, c text);
INSERT INTO ea3_x VALUES (1, 'x1');
INSERT INTO ea3_y VALUES (1, 'y1');
SELECT a FROM ea3_x, ea3_y;
SELECT ea3_x.a FROM ea3_x, ea3_y;
SELECT nosuch.a FROM ea3_x;
SELECT ea3_x.nosuch FROM ea3_x;
SELECT nosuchschema.nosuchtab.a FROM ea3_x;
SELECT fuzz.public.ea3_x.a FROM ea3_x;
SELECT nosuchdb.public.ea3_x.a FROM ea3_x;
SELECT toomany.levels.of.qualification.here FROM ea3_x;
SELECT b FROM ea3_y;
SELECT ea3_x FROM ea3_x;
SELECT (ea3_x).a FROM ea3_x;
SELECT (ea3_x).nosuch FROM ea3_x;
SELECT (ea3_x.*).a FROM ea3_x;
SELECT ea3_x.* FROM ea3_x;
SELECT ea3_x.*;
SELECT *;
-- column notation on function results / function notation on columns
CREATE FUNCTION ea3_getb(ea3_x) RETURNS text LANGUAGE sql AS $$ SELECT $1.b $$;
SELECT ea3_getb(ea3_x) FROM ea3_x;
SELECT (ea3_x).ea3_getb FROM ea3_x;
SELECT ea3_x.ea3_getb FROM ea3_x;
SELECT b(ea3_x) FROM ea3_x;
SELECT nosuchfn(ea3_x) FROM ea3_x;
SELECT ea3_getb(ea3_y) FROM ea3_y;
-- ambiguous / wrong-arg function calls (func_get_detail candidates)
CREATE FUNCTION ea3_amb(int) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION ea3_amb(smallint) RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;
SELECT ea3_amb('1');
SELECT ea3_amb(1.5);
SELECT ea3_amb();
SELECT ea3_amb(1, 2);
SELECT ea3_amb(x => 1);
DROP FUNCTION ea3_amb(int);
DROP FUNCTION ea3_amb(smallint);
DROP FUNCTION ea3_getb(ea3_x);
-- aggregate/procedure called wrongly (ParseFuncOrColumn arms)
SELECT count(*) FILTER (WHERE true) OVER ();
CREATE PROCEDURE ea3_proc(int) LANGUAGE sql AS $$ SELECT 1 $$;
SELECT ea3_proc(1);
CALL ea3_proc(1);
CALL abs(1);
CALL count(*);
DROP PROCEDURE ea3_proc(int);
SELECT count(DISTINCT a ORDER BY a) FROM ea3_x;
SELECT sum(a) WITHIN GROUP (ORDER BY a) FROM ea3_x;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a) FROM ea3_x;
SELECT percentile_cont(0.5) FROM ea3_x;
SELECT rank(1) WITHIN GROUP (ORDER BY a) OVER () FROM ea3_x;
DROP TABLE ea3_y;
DROP TABLE ea3_x;

-- ========================================================= @sec shdep2
-- checkSharedDependencies / shdepDropOwned / findDependentObjects /
-- find_expr_references_walker: dependency-rich objects, blocked drops,
-- REASSIGN OWNED arms.
CREATE ROLE ea3_own1;
CREATE ROLE ea3_own2;
CREATE SCHEMA ea3_ds AUTHORIZATION ea3_own1;
GRANT CREATE ON SCHEMA ea3_ds TO ea3_own1;
SET ROLE ea3_own1;
CREATE TABLE ea3_ds.owned (a int DEFAULT abs(-3), b text DEFAULT lower('X'));
CREATE VIEW ea3_ds.ov AS SELECT a + 1 AS a1, b || 'suffix' AS b1 FROM ea3_ds.owned WHERE a IN (SELECT abs(a) FROM ea3_ds.owned);
CREATE FUNCTION ea3_ds.of(x int) RETURNS int LANGUAGE sql AS $$ SELECT x + (SELECT count(*)::int FROM ea3_ds.owned) $$;
RESET ROLE;
DROP ROLE ea3_own1;
REASSIGN OWNED BY ea3_own1 TO ea3_own1;
REASSIGN OWNED BY nosuch_role TO ea3_own2;
REASSIGN OWNED BY ea3_own1 TO nosuch_role;
REASSIGN OWNED BY postgres TO ea3_own2;
REASSIGN OWNED BY ea3_own1 TO ea3_own2;
DROP ROLE ea3_own1;
DROP OWNED BY ea3_own1;
DROP ROLE ea3_own1;
-- dependency-walker fuel + blocked drops
CREATE TABLE ea3_dep (a int);
CREATE VIEW ea3_depv AS SELECT a FROM ea3_dep;
DROP TABLE ea3_dep;
DROP TABLE ea3_dep CASCADE;
CREATE DOMAIN ea3_dd AS int;
CREATE TABLE ea3_ddt (v ea3_dd);
DROP DOMAIN ea3_dd;
DROP DOMAIN ea3_dd CASCADE;
DROP TABLE ea3_ddt;
CREATE FUNCTION ea3_df() RETURNS int LANGUAGE sql AS $$ SELECT 7 $$;
CREATE TABLE ea3_dft (v int DEFAULT ea3_df());
DROP FUNCTION ea3_df();
DROP FUNCTION ea3_df() CASCADE;
DROP TABLE ea3_dft;
CREATE TYPE ea3_de AS ENUM ('a', 'b');
CREATE TABLE ea3_det (v ea3_de DEFAULT 'a');
DROP TYPE ea3_de;
DROP TYPE ea3_de CASCADE;
DROP TABLE ea3_det;
CREATE SEQUENCE ea3_dsq;
CREATE TABLE ea3_dst (v int DEFAULT nextval('ea3_dsq'));
DROP SEQUENCE ea3_dsq;
ALTER SEQUENCE ea3_dsq OWNED BY ea3_dst.v;
DROP TABLE ea3_dst;
DROP SEQUENCE ea3_dsq;
DROP SCHEMA ea3_ds CASCADE;
DROP OWNED BY ea3_own2;
DROP ROLE ea3_own2;

-- ========================================================== @sec tsdes
-- deserialize_deflist (tsearchcmds.c) + dictionary option validation.
CREATE TEXT SEARCH DICTIONARY ea3_d1 (template = simple, stopwords = english);
ALTER TEXT SEARCH DICTIONARY ea3_d1 (stopwords = english);
ALTER TEXT SEARCH DICTIONARY ea3_d1 (nosuchopt = 1);
ALTER TEXT SEARCH DICTIONARY ea3_d1 (accept = false);
ALTER TEXT SEARCH DICTIONARY ea3_d1 (accept = maybe);
ALTER TEXT SEARCH DICTIONARY ea3_d1 (accept);
SELECT ts_lexize('ea3_d1', 'word');
ALTER TEXT SEARCH DICTIONARY ea3_d1 (stopwords);
CREATE TEXT SEARCH DICTIONARY ea3_d2 (template = snowball, language = english);
ALTER TEXT SEARCH DICTIONARY ea3_d2 (language = nosuch_lang);
CREATE TEXT SEARCH DICTIONARY ea3_d3 (template = synonym, synonyms = nosuch_file);
CREATE TEXT SEARCH DICTIONARY ea3_d4 (template = ispell, dictfile = nosuch, afffile = nosuch);
CREATE TEXT SEARCH DICTIONARY ea3_d5 (template = thesaurus, dictfile = nosuch, dictionary = english_stem);
CREATE TEXT SEARCH DICTIONARY ea3_d6 (template = nosuch_template);
CREATE TEXT SEARCH DICTIONARY ea3_d7 (language = english);
SELECT ts_lexize('ea3_d2', 'jumping');
DROP TEXT SEARCH DICTIONARY ea3_d2;
DROP TEXT SEARCH DICTIONARY ea3_d1;
DROP TEXT SEARCH DICTIONARY IF EXISTS ea3_d3;
-- config mapping arms
CREATE TEXT SEARCH CONFIGURATION ea3_cfg (copy = english);
ALTER TEXT SEARCH CONFIGURATION ea3_cfg DROP MAPPING FOR nosuch_toktype;
ALTER TEXT SEARCH CONFIGURATION ea3_cfg DROP MAPPING IF EXISTS FOR word;
ALTER TEXT SEARCH CONFIGURATION ea3_cfg ALTER MAPPING FOR asciiword WITH nosuch_dict;
ALTER TEXT SEARCH CONFIGURATION ea3_cfg ALTER MAPPING REPLACE english_stem WITH simple;
ALTER TEXT SEARCH CONFIGURATION ea3_cfg ALTER MAPPING REPLACE nosuch_dict WITH simple;
DROP TEXT SEARCH CONFIGURATION ea3_cfg;
CREATE TEXT SEARCH PARSER ea3_prs (start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
CREATE TEXT SEARCH PARSER ea3_prs2 (start = prsd_start);
CREATE TEXT SEARCH PARSER ea3_prs3 (start = nosuch_fn, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
DROP TEXT SEARCH PARSER ea3_prs;
