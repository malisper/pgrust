-- deck-err3-earm4.sql
-- ERR3 lane ERROR-ARM round 4 deck (source of truth: crates/bin/fuzzgen/src/earm4.rs).
-- Catalog/DDL error-arm residue the earm/earm2/earm3 passes left: domain +
-- constraint validation, ALTER TABLE misc, tablespace/AM, ownership +
-- dependency, RLS policy, rules, publication/subscription DDL, COMMENT +
-- SECURITY LABEL, sequence/identity, remaining parser grammar productions.
--
-- Charter: each section is applied per-statement to the pinned A/B pair; the
-- differential bar is ERROR IDENTITY (SQLSTATE + message). Every probe errors
-- deliberately; setup/teardown statements keep fixtures self-contained under
-- ea4_ names, so a poisoned probe aborts only itself under per-statement
-- isolation. No BEGIN spans, no SET search_path; every CREATE ROLE has its
-- DROP ROLE in the same section.
--
-- Smoke provenance: every section was executed statement-by-statement against
-- a throwaway stock PostgreSQL 18.4 (Homebrew) cluster (2026-08-12): 113 probe
-- statements each raised a clean SQLSTATE, zero catalog/prepared-statement
-- residue left behind. The authoritative A(REL_18_3 @62d6c7d)/B(pgrust
-- origin/main) transcript differential + before->after line-drain measurement
-- is a deferred CI cluster coverage leg (local disk guard: laptop is not a compute
-- node). VERSION-SKEW note: the smoke oracle is 18.4, so any 18.4-added check
-- must be reconfirmed against 18.3 on the CI cluster leg before banking.


-- ==== earm4:domcon ====
CREATE DOMAIN ea4_bt AS nosuch_base;
CREATE DOMAIN ea4_dsub AS int CHECK (VALUE > (SELECT 1));
CREATE DOMAIN ea4_ddef AS int DEFAULT 'notanint';
CREATE DOMAIN ea4_dd AS int;
CREATE TABLE ea4_ddt (a ea4_dd);
INSERT INTO ea4_ddt VALUES (NULL), (-1);
ALTER DOMAIN ea4_dd SET NOT NULL;
ALTER DOMAIN ea4_dd ADD CONSTRAINT ea4_dpos CHECK (VALUE > 0);
ALTER DOMAIN ea4_dd ADD CONSTRAINT ea4_dnv CHECK (VALUE > 0) NOT VALID;
ALTER DOMAIN ea4_dd VALIDATE CONSTRAINT ea4_dnv;
ALTER DOMAIN ea4_dd DROP CONSTRAINT ea4_missing;
ALTER DOMAIN ea4_dd RENAME CONSTRAINT ea4_missing TO ea4_x;
ALTER DOMAIN ea4_dd SET DEFAULT 'notanint';
ALTER DOMAIN ea4_dd ADD CONSTRAINT ea4_dnn NOT NULL;
DROP DOMAIN ea4_dd;
DROP DOMAIN ea4_dd RESTRICT;
DROP TABLE ea4_ddt;
DROP DOMAIN ea4_dd;

-- ==== earm4:chkval ====
CREATE TABLE ea4_ck (a int, b int);
INSERT INTO ea4_ck VALUES (1, 1), (NULL, 2), (-1, 3);
ALTER TABLE ea4_ck ADD CONSTRAINT ea4_ckc CHECK (a > 0);
ALTER TABLE ea4_ck ALTER COLUMN a SET NOT NULL;
ALTER TABLE ea4_ck ADD PRIMARY KEY (a);
ALTER TABLE ea4_ck ADD CONSTRAINT ea4_cknv CHECK (a > 0) NOT VALID;
ALTER TABLE ea4_ck VALIDATE CONSTRAINT ea4_cknv;
ALTER TABLE ea4_ck ADD CONSTRAINT ea4_ckfk FOREIGN KEY (a) REFERENCES ea4_ck (b);
ALTER TABLE ea4_ck ALTER CONSTRAINT ea4_cknv DEFERRABLE;
ALTER TABLE ea4_ck VALIDATE CONSTRAINT ea4_missing;
ALTER TABLE ea4_ck DROP CONSTRAINT ea4_missing;
DROP TABLE ea4_ck;

-- ==== earm4:atmisc ====
CREATE TABLE ea4_am (a int);
ALTER TABLE ea4_am SET ACCESS METHOD ea4_nosuch_am;
ALTER TABLE ea4_am ALTER COLUMN a SET STORAGE EXTERNAL;
ALTER TABLE ea4_am ALTER COLUMN a SET STATISTICS -5;
ALTER TABLE ea4_am CLUSTER ON ea4_nosuch_idx;
ALTER TABLE ea4_am SET (fillfactor = 5);
ALTER TABLE ea4_am SET (fillfactor = 200);
ALTER TABLE ea4_am SET (autovacuum_enabled = maybe);
ALTER TABLE ea4_am DISABLE TRIGGER ea4_nosuch_trg;
ALTER TABLE ea4_am DISABLE RULE ea4_nosuch_rule;
ALTER TABLE ea4_am REPLICA IDENTITY USING INDEX ea4_nosuch_idx;
ALTER TABLE ea4_am OF ea4_nosuch_type;
ALTER TABLE ea4_am INHERIT ea4_nosuch_parent;
ALTER TABLE ea4_am ALTER COLUMN a SET (n_distinct = 'x');
DROP TABLE ea4_am;

-- ==== earm4:tblspc ====
CREATE TABLE ea4_ts (a int);
ALTER TABLE ea4_ts SET TABLESPACE ea4_nosuch_ts;
ALTER TABLE ea4_ts SET TABLESPACE pg_global;
CREATE TABLE ea4_ts2 (a int) TABLESPACE ea4_nosuch_ts;
CREATE INDEX ea4_tsi ON ea4_ts (a) TABLESPACE ea4_nosuch_ts;
ALTER TABLE ALL IN TABLESPACE ea4_nosuch_ts SET TABLESPACE pg_default;
ALTER INDEX ALL IN TABLESPACE ea4_nosuch_ts SET TABLESPACE pg_default;
DROP TABLE ea4_ts;

-- ==== earm4:owndep ====
CREATE ROLE ea4_o1 NOSUPERUSER;
CREATE ROLE ea4_o2 NOSUPERUSER;
CREATE TABLE ea4_ot (a int);
ALTER TABLE ea4_ot OWNER TO ea4_nosuch_role;
SET ROLE ea4_o1;
ALTER TABLE ea4_ot OWNER TO ea4_o2;
DROP TABLE ea4_ot;
RESET ROLE;
REASSIGN OWNED BY ea4_nosuch_role TO postgres;
DROP OWNED BY ea4_nosuch_role;
CREATE TABLE ea4_dp (a int);
CREATE VIEW ea4_dv AS SELECT a FROM ea4_dp;
DROP TABLE ea4_dp;
DROP TABLE ea4_dp RESTRICT;
ALTER TABLE ea4_dp OWNER TO ea4_o1;
DROP VIEW ea4_dv;
DROP TABLE ea4_dp;
DROP TABLE ea4_ot;
DROP ROLE ea4_o1;
DROP ROLE ea4_o2;

-- ==== earm4:policy ====
CREATE TABLE ea4_pt (a int, b int);
CREATE VIEW ea4_pv AS SELECT 1 AS a;
CREATE POLICY ea4_pol ON ea4_pt USING (a > 0);
CREATE POLICY ea4_pol ON ea4_pt USING (a > 0);
CREATE POLICY ea4_psel ON ea4_pt FOR SELECT WITH CHECK (a > 0);
CREATE POLICY ea4_pdel ON ea4_pt FOR DELETE WITH CHECK (a > 0);
CREATE POLICY ea4_pmiss ON ea4_nosuch_tab USING (true);
CREATE POLICY ea4_pcol ON ea4_pt USING (nosuchcol > 0);
CREATE POLICY ea4_pview ON ea4_pv USING (true);
ALTER POLICY ea4_nosuch_pol ON ea4_pt USING (true);
DROP POLICY ea4_nosuch_pol ON ea4_pt;
ALTER TABLE ea4_pv ENABLE ROW LEVEL SECURITY;
ALTER TABLE ea4_pv FORCE ROW LEVEL SECURITY;
DROP VIEW ea4_pv;
DROP TABLE ea4_pt;

-- ==== earm4:rules ====
CREATE TABLE ea4_rt (a int);
CREATE RULE ea4_rins AS ON INSERT TO ea4_rt DO INSTEAD NOTHING;
CREATE RULE ea4_rins AS ON INSERT TO ea4_rt DO INSTEAD NOTHING;
CREATE RULE ea4_rsel AS ON SELECT TO ea4_rt DO INSTEAD SELECT a FROM ea4_rt;
CREATE RULE ea4_rmiss AS ON INSERT TO ea4_nosuch_tab DO NOTHING;
DROP RULE ea4_nosuch_rule ON ea4_rt;
DROP RULE ea4_rins ON ea4_nosuch_tab;
DROP TABLE ea4_rt;

-- ==== earm4:pubval ====
CREATE TABLE ea4_put (a int PRIMARY KEY, b int);
CREATE PUBLICATION ea4_pub FOR TABLE ea4_put;
CREATE PUBLICATION ea4_pub FOR TABLE ea4_put;
CREATE PUBLICATION ea4_pub2 FOR ALL TABLES, TABLE ea4_put;
CREATE PUBLICATION ea4_pub3 FOR TABLE ea4_put WHERE (b > random());
CREATE PUBLICATION ea4_pub4 WITH (publish = 'bogus');
CREATE PUBLICATION ea4_pub5 FOR TABLE ea4_nosuch_tab;
ALTER PUBLICATION ea4_pub ADD TABLE ea4_nosuch_tab;
ALTER PUBLICATION ea4_pub SET (publish = 'bogus');
ALTER PUBLICATION ea4_nosuch_pub ADD TABLE ea4_put;
DROP PUBLICATION ea4_pub;
DROP TABLE ea4_put;

-- ==== earm4:subval ====
CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (connect = false, copy_data = true);
CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (connect = false, create_slot = true);
CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (connect = false, enabled = true);
CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (slot_name = NONE, enabled = true);
CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (streaming = 'bogus');
CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (synchronous_commit = 'bogus');
ALTER SUBSCRIPTION ea4_nosuch_sub SET (streaming = true);
ALTER SUBSCRIPTION ea4_nosuch_sub OWNER TO postgres;
DROP SUBSCRIPTION ea4_nosuch_sub;

-- ==== earm4:commlbl ====
CREATE TABLE ea4_ct (a int);
COMMENT ON TABLE ea4_nosuch_tab IS 'x';
COMMENT ON COLUMN ea4_ct.nosuchcol IS 'x';
COMMENT ON FUNCTION ea4_nosuch_fn() IS 'x';
COMMENT ON SCHEMA ea4_nosuch_schema IS 'x';
COMMENT ON ROLE ea4_nosuch_role IS 'x';
COMMENT ON CONSTRAINT ea4_nosuch_con ON ea4_ct IS 'x';
COMMENT ON DOMAIN ea4_nosuch_dom IS 'x';
SECURITY LABEL ON TABLE ea4_ct IS 'x';
SECURITY LABEL FOR ea4_nosuch_provider ON TABLE ea4_ct IS 'x';
SECURITY LABEL FOR ea4_nosuch_provider ON ROLE postgres IS 'x';
DROP TABLE ea4_ct;

-- ==== earm4:seqid2 ====
CREATE SEQUENCE ea4_s1;
ALTER SEQUENCE ea4_s1 INCREMENT BY 0;
ALTER SEQUENCE ea4_s1 MINVALUE 100 MAXVALUE 10;
ALTER SEQUENCE ea4_s1 START WITH 5 MINVALUE 10;
ALTER SEQUENCE ea4_s1 AS smallint MAXVALUE 100000;
ALTER SEQUENCE ea4_s1 OWNED BY ea4_nosuch_tab.col;
CREATE SEQUENCE ea4_s2 AS boolean;
CREATE TABLE ea4_it (a int GENERATED ALWAYS AS IDENTITY, b text);
ALTER TABLE ea4_it ALTER COLUMN b ADD GENERATED ALWAYS AS IDENTITY;
ALTER TABLE ea4_it ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;
ALTER TABLE ea4_it ALTER COLUMN b DROP IDENTITY;
ALTER TABLE ea4_it ALTER COLUMN b SET GENERATED BY DEFAULT;
CREATE TABLE ea4_it2 (a int GENERATED ALWAYS AS IDENTITY DEFAULT 5);
INSERT INTO ea4_it (a) VALUES (1);
DROP TABLE ea4_it;
DROP SEQUENCE ea4_s1;

-- ==== earm4:pgram2 ====
CREATE TABLE ea4_g (a int, b int);
SELECT a FROM ea4_g UNION SELECT a, b FROM ea4_g;
INSERT INTO ea4_g VALUES (1, 2), (3);
LOCK TABLE ea4_g IN ea4_bogus MODE;
EXPLAIN (FORMAT ea4_bogus) SELECT 1;
EXPLAIN (ea4_bogus) SELECT 1;
VACUUM (ea4_bogus) ea4_g;
REINDEX (ea4_bogus) TABLE ea4_g;
SELECT * FROM ea4_g LIMIT 1, 2;
SELECT sum(a) OVER (ORDER BY a ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM ea4_g;
SELECT $1;
EXECUTE ea4_nosuch_ps;
DEALLOCATE ea4_nosuch_ps;
SELECT a FROM ea4_g GROUP BY ROLLUP;
DROP TABLE ea4_g;
