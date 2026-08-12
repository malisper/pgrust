//! Object-identity + deparse drain module (LD1, line-drain-queue chunks
//! `objaddr-identity` + `ruleutils-deparse`): hand-verified deck batteries
//! that instantiate (nearly) every OBJECT_* class and expression node kind,
//! then drive the identity/description/address and deparse surfaces:
//!
//!   - `objid:address` — one object of every addressable class (schema,
//!     fdw/server/user mapping, text-search quartet, table/partitioned
//!     table/view/matview/foreign table, composite/enum/domain + domain
//!     constraint, aggregate/procedure/trigger/policy, default acls,
//!     transform, publications (incl. FOR TABLES IN SCHEMA), subscription
//!     (connect=false), statistics object, event trigger, parameter ACL,
//!     role membership, large object with a pinned OID), then the
//!     pg_get_object_address -> pg_identify_object /
//!     pg_identify_object_as_address roundtrip + pg_describe_object over
//!     all of them, the get_object_address error battery, and the
//!     missing_ok=true invalid-object battery over all 42 catalog classes
//!     (getObjectIdentityParts / getObjectDescription /
//!     getObjectTypeDescription / get_object_address arm-per-class).
//!   - `objid:deparse` — views storing every deparsable expression node
//!     (bool/null/case tests, arrays + slices, row/rowcompare, all
//!     sublink kinds, field select/store, coercion forms, collate,
//!     SQLValueFunction + special-syntax function battery (EXTRACT,
//!     OVERLAY, POSITION, SUBSTRING, TRIM, AT TIME ZONE / AT LOCAL,
//!     OVERLAPS, NORMALIZE, XMLEXISTS, SYSTEM_USER — get_func_sql_syntax),
//!     XML ops, SQL/JSON constructors + query functions + IS JSON,
//!     grouping sets/GROUPING, window frames, WITH incl. MATERIALIZED and
//!     SEARCH/CYCLE, join alias/tablesample/ordinality/ROWS FROM from-items,
//!     XMLTABLE + JSON_TABLE), rules (INSERT..ON CONFLICT, MULTIEXPR
//!     UPDATE, FieldStore/SubscriptingRef assignment, WITH + RETURNING
//!     OLD/NEW, NOTIFY), the constraint/index/trigger/statistics/partition
//!     battery, function-def shapes (VARIADIC/defaults/OUT/TABLE/SET,
//!     BEGIN ATOMIC) — and the pg_get_viewdef / pg_get_ruledef /
//!     pg_get_constraintdef / pg_get_indexdef / pg_get_triggerdef /
//!     pg_get_statisticsobjdef / pg_get_partkeydef / pg_get_expr /
//!     pg_get_functiondef sweeps over all of it.
//!   - `objid:explain` — EXPLAIN (VERBOSE, COSTS OFF) arms whose deparse
//!     paths only exist plan-side: SubPlan/InitPlan labels, InferenceElem
//!     arbiters (ON CONFLICT), NextValueExpr (identity insert), MULTIEXPR
//!     SubLink numbering, CurrentOfExpr (WHERE CURRENT OF under a FOR
//!     UPDATE cursor bracket).
//!
//! Rules of the module (mirrors `nodes`):
//!   - every group is self-contained: fixed ldo_/ldp_-prefixed names,
//!     created and dropped within the group; no cross-group state;
//!   - every statement was hand-verified byte-identical on C REL_18_3
//!     (cpg-ref) and pgrust origin/main before embedding — see
//!     docs/fuzzing/deck-ld1-objid.sql / deck-ld1-deparse.sql (the decks
//!     these arrays were generated from) and findings-ld1-objid.md;
//!   - the ONE known-divergent statement (pg_get_viewdef of a SEARCH/
//!     CYCLE view — pgrust assertion, finding LD1-F1) is deliberately
//!     NOT embedded: the ldp_v28 view is still created (its CREATE and
//!     readfuncs surface are parity-clean) but the viewdef sweeps
//!     exclude it; the deck keeps the divergent probe for the C-side
//!     coverage arm and the finding repro;
//!   - XMLPARSE / XMLELEMENT / XMLFOREST / XMLPI are absent: the rig's
//!     no-libxml C reference rejects them at CREATE VIEW time
//!     ("unsupported XML feature") while pgrust accepts them — an A-side
//!     build-config gap, not a pgrust bug (banked as LD1-N1); their
//!     get_rule_expr arms are ruled CONFIG-GATED in the line-drain queue.

use crate::stmt::{Gen, StmtKind};

const ADDRESS_DECK: &[&str] = &[
    r#"CREATE ROLE ldo_user;"#,
    r#"CREATE ROLE ldo_user2;"#,
    r#"GRANT ldo_user2 TO ldo_user WITH ADMIN OPTION;"#,
    r#"CREATE SCHEMA ldo_nsp;"#,
    r#"CREATE FOREIGN DATA WRAPPER ldo_fdw;"#,
    r#"CREATE SERVER ldo_fserv FOREIGN DATA WRAPPER ldo_fdw;"#,
    r#"CREATE TEXT SEARCH DICTIONARY ldo_ts_dict (template=simple);"#,
    r#"CREATE TEXT SEARCH CONFIGURATION ldo_ts_conf (copy=english);"#,
    r#"CREATE TEXT SEARCH TEMPLATE ldo_ts_temp (lexize=dsimple_lexize);"#,
    r#"CREATE TEXT SEARCH PARSER ldo_ts_prs (start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);"#,
    r#"CREATE TABLE ldo_nsp.gentable (a serial primary key CONSTRAINT ldo_a_chk CHECK (a > 0), b text DEFAULT 'hello');"#,
    r#"CREATE TABLE ldo_nsp.parttable (a int PRIMARY KEY) PARTITION BY RANGE (a);"#,
    r#"CREATE VIEW ldo_nsp.genview AS SELECT * from ldo_nsp.gentable;"#,
    r#"CREATE MATERIALIZED VIEW ldo_nsp.genmatview AS SELECT * FROM ldo_nsp.gentable;"#,
    r#"CREATE TYPE ldo_nsp.gencomptype AS (a int);"#,
    r#"CREATE TYPE ldo_nsp.genenum AS ENUM ('one', 'two');"#,
    r#"CREATE FOREIGN TABLE ldo_nsp.genftable (a int) SERVER ldo_fserv;"#,
    r#"CREATE AGGREGATE ldo_nsp.genaggr(int4) (sfunc = int4pl, stype = int4);"#,
    r#"CREATE DOMAIN ldo_nsp.gendomain AS int4 CONSTRAINT ldo_domconstr CHECK (value > 0);"#,
    r#"CREATE FUNCTION ldo_nsp.trig() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN END; $$;"#,
    r#"CREATE TRIGGER ldo_t BEFORE INSERT ON ldo_nsp.gentable FOR EACH ROW EXECUTE PROCEDURE ldo_nsp.trig();"#,
    r#"CREATE POLICY ldo_genpol ON ldo_nsp.gentable;"#,
    r#"CREATE PROCEDURE ldo_nsp.proc(int4) LANGUAGE SQL AS $$ $$;"#,
    r#"CREATE SERVER ldo_srv2 FOREIGN DATA WRAPPER ldo_fdw;"#,
    r#"CREATE USER MAPPING FOR ldo_user SERVER ldo_srv2;"#,
    r#"ALTER DEFAULT PRIVILEGES FOR ROLE ldo_user IN SCHEMA public GRANT ALL ON TABLES TO ldo_user;"#,
    r#"ALTER DEFAULT PRIVILEGES FOR ROLE ldo_user REVOKE DELETE ON TABLES FROM ldo_user;"#,
    r#"CREATE TRANSFORM FOR int LANGUAGE SQL (FROM SQL WITH FUNCTION prsd_lextype(internal), TO SQL WITH FUNCTION int4recv(internal));"#,
    r#"SET client_min_messages = 'ERROR';"#,
    r#"CREATE PUBLICATION ldo_pub FOR TABLE ldo_nsp.gentable;"#,
    r#"CREATE PUBLICATION ldo_pub_schema FOR TABLES IN SCHEMA ldo_nsp;"#,
    r#"RESET client_min_messages;"#,
    r#"CREATE SUBSCRIPTION ldo_sub CONNECTION '' PUBLICATION bar WITH (connect = false, slot_name = NONE);"#,
    r#"CREATE STATISTICS ldo_nsp.gentable_stat ON a, b FROM ldo_nsp.gentable;"#,
    r#"CREATE FUNCTION ldo_evtfn() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;"#,
    r#"CREATE EVENT TRIGGER ldo_evt ON ddl_command_start EXECUTE FUNCTION ldo_evtfn();"#,
    r#"GRANT SET ON PARAMETER work_mem TO ldo_user;"#,
    r#"SELECT lo_create(424242);"#,
    r#"SELECT pg_get_object_address('stone', '{}', '{}');"#,
    r#"SELECT pg_get_object_address('table', '{}', '{}');"#,
    r#"SELECT pg_get_object_address('table', '{NULL}', '{}');"#,
    r#"SELECT pg_get_object_address('language', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('language', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('large object', '{123}', '{}');"#,
    r#"SELECT pg_get_object_address('large object', '{123,456}', '{}');"#,
    r#"SELECT pg_get_object_address('large object', '{blargh}', '{}');"#,
    r#"SELECT pg_get_object_address('schema', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('schema', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('role', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('role', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('database', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('database', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('tablespace', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('tablespace', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('foreign-data wrapper', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('foreign-data wrapper', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('server', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('server', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('extension', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('extension', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('event trigger', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('event trigger', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('access method', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('access method', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('publication', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('publication', '{one,two}', '{}');"#,
    r#"SELECT pg_get_object_address('subscription', '{one}', '{}');"#,
    r#"SELECT pg_get_object_address('subscription', '{one,two}', '{}');"#,
    r#"SELECT * FROM pg_get_object_address('operator of access method', '{btree,integer_ops,1}', '{int4,bool}');"#,
    r#"SELECT * FROM pg_get_object_address('operator of access method', '{btree,integer_ops,99}', '{int4,int4}');"#,
    r#"SELECT * FROM pg_get_object_address('function of access method', '{btree,integer_ops,1}', '{int4,bool}');"#,
    r#"SELECT * FROM pg_get_object_address('function of access method', '{btree,integer_ops,99}', '{int4,int4}');"#,
    r#"WITH objects (type, name, args) AS (VALUES ('table', '{ldo_nsp, gentable}'::text[], '{}'::text[]), ('table', '{ldo_nsp, parttable}', '{}'), ('index', '{ldo_nsp, gentable_pkey}', '{}'), ('index', '{ldo_nsp, parttable_pkey}', '{}'), ('sequence', '{ldo_nsp, gentable_a_seq}', '{}'), ('view', '{ldo_nsp, genview}', '{}'), ('materialized view', '{ldo_nsp, genmatview}', '{}'), ('foreign table', '{ldo_nsp, genftable}', '{}'), ('table column', '{ldo_nsp, gentable, b}', '{}'), ('foreign table column', '{ldo_nsp, genftable, a}', '{}'), ('aggregate', '{ldo_nsp, genaggr}', '{int4}'), ('function', '{pg_catalog, pg_identify_object}', '{pg_catalog.oid, pg_catalog.oid, int4}'), ('procedure', '{ldo_nsp, proc}', '{int4}'), ('type', '{pg_catalog._int4}', '{}'), ('type', '{ldo_nsp.gendomain}', '{}'), ('type', '{ldo_nsp.gencomptype}', '{}'), ('type', '{ldo_nsp.genenum}', '{}'), ('cast', '{int8}', '{int4}'), ('collation', '{default}', '{}'), ('table constraint', '{ldo_nsp, gentable, ldo_a_chk}', '{}'), ('domain constraint', '{ldo_nsp.gendomain}', '{ldo_domconstr}'), ('conversion', '{pg_catalog, koi8_r_to_mic}', '{}'), ('default value', '{ldo_nsp, gentable, b}', '{}'), ('language', '{plpgsql}', '{}'), ('large object', '{424242}', '{}'), ('operator', '{+}', '{int4, int4}'), ('operator class', '{btree, int4_ops}', '{}'), ('operator family', '{btree, integer_ops}', '{}'), ('operator of access method', '{btree,integer_ops,1}', '{integer,integer}'), ('function of access method', '{btree,integer_ops,2}', '{integer,integer}'), ('rule', '{ldo_nsp, genview, _RETURN}', '{}'), ('trigger', '{ldo_nsp, gentable, ldo_t}', '{}'), ('schema', '{ldo_nsp}', '{}'), ('text search parser', '{ldo_ts_prs}', '{}'), ('text search dictionary', '{ldo_ts_dict}', '{}'), ('text search template', '{ldo_ts_temp}', '{}'), ('text search configuration', '{ldo_ts_conf}', '{}'), ('role', '{ldo_user}', '{}'), ('foreign-data wrapper', '{ldo_fdw}', '{}'), ('server', '{ldo_fserv}', '{}'), ('user mapping', '{ldo_user}', '{ldo_srv2}'), ('default acl', '{ldo_user,public}', '{r}'), ('default acl', '{ldo_user}', '{r}'), ('extension', '{plpgsql}', '{}'), ('event trigger', '{ldo_evt}', '{}'), ('policy', '{ldo_nsp, gentable, ldo_genpol}', '{}'), ('transform', '{int}', '{sql}'), ('access method', '{btree}', '{}'), ('publication', '{ldo_pub}', '{}'), ('publication namespace', '{ldo_nsp}', '{ldo_pub_schema}'), ('publication relation', '{ldo_nsp, gentable}', '{ldo_pub}'), ('subscription', '{ldo_sub}', '{}'), ('statistics object', '{ldo_nsp, gentable_stat}', '{}') ) SELECT (pg_identify_object(addr1.classid, addr1.objid, addr1.objsubid)).*, pg_describe_object(addr1.classid, addr1.objid, addr1.objsubid) AS descr, ROW(pg_identify_object(addr1.classid, addr1.objid, addr1.objsubid)) = ROW(pg_identify_object(addr2.classid, addr2.objid, addr2.objsubid)) AS roundtrip FROM objects, pg_get_object_address(type, name, args) AS addr1, pg_identify_object_as_address(classid, objid, objsubid) AS ioa (typ, nms, args), pg_get_object_address(typ, nms, ioa.args) AS addr2 ORDER BY addr1.classid, addr1.objid, addr1.objsubid;"#,
    r#"SELECT (i).type, (i).schema, (i).name, (i).identity FROM (SELECT pg_identify_object('pg_database'::regclass, oid, 0) AS i FROM pg_database WHERE datname = current_database()) s;"#,
    r#"SELECT pg_describe_object('pg_database'::regclass, oid, 0) = 'database ' || quote_ident(current_database()) FROM pg_database WHERE datname = current_database();"#,
    r#"SELECT (i).type, (i).schema, (i).name, (i).identity FROM (SELECT pg_identify_object('pg_tablespace'::regclass, oid, 0) AS i FROM pg_tablespace WHERE spcname = 'pg_default') s;"#,
    r#"SELECT pg_describe_object('pg_tablespace'::regclass, oid, 0) FROM pg_tablespace WHERE spcname = 'pg_default';"#,
    r#"SELECT (i).type, (i).schema, (i).name, (i).identity FROM (SELECT pg_identify_object('pg_auth_members'::regclass, am.oid, 0) AS i FROM pg_auth_members am WHERE am.roleid = 'ldo_user2'::regrole AND am.member = 'ldo_user'::regrole) s;"#,
    r#"SELECT pg_describe_object('pg_auth_members'::regclass, am.oid, 0) FROM pg_auth_members am WHERE am.roleid = 'ldo_user2'::regrole AND am.member = 'ldo_user'::regrole;"#,
    r#"SELECT (i).type, (i).schema, (i).name, (i).identity FROM (SELECT pg_identify_object('pg_parameter_acl'::regclass, oid, 0) AS i FROM pg_parameter_acl WHERE parname = 'work_mem') s;"#,
    r#"SELECT pg_describe_object('pg_parameter_acl'::regclass, oid, 0) FROM pg_parameter_acl WHERE parname = 'work_mem';"#,
    r#"SELECT a.type, a.object_names, a.object_args FROM pg_auth_members am, LATERAL pg_identify_object_as_address('pg_auth_members'::regclass, am.oid, 0) a WHERE am.roleid = 'ldo_user2'::regrole AND am.member = 'ldo_user'::regrole;"#,
    r#"SELECT a.type, a.object_names, a.object_args FROM pg_parameter_acl p, LATERAL pg_identify_object_as_address('pg_parameter_acl'::regclass, p.oid, 0) a WHERE p.parname = 'work_mem';"#,
    r#"WITH objects (classid, objid, objsubid) AS (VALUES ('pg_class'::regclass, 0, 0), ('pg_class'::regclass, 'pg_class'::regclass::oid::int, 100), ('pg_proc'::regclass, 0, 0), ('pg_type'::regclass, 0, 0), ('pg_cast'::regclass, 0, 0), ('pg_collation'::regclass, 0, 0), ('pg_constraint'::regclass, 0, 0), ('pg_conversion'::regclass, 0, 0), ('pg_attrdef'::regclass, 0, 0), ('pg_language'::regclass, 0, 0), ('pg_largeobject'::regclass, 0, 0), ('pg_operator'::regclass, 0, 0), ('pg_opclass'::regclass, 0, 0), ('pg_opfamily'::regclass, 0, 0), ('pg_am'::regclass, 0, 0), ('pg_amop'::regclass, 0, 0), ('pg_amproc'::regclass, 0, 0), ('pg_rewrite'::regclass, 0, 0), ('pg_trigger'::regclass, 0, 0), ('pg_namespace'::regclass, 0, 0), ('pg_statistic_ext'::regclass, 0, 0), ('pg_ts_parser'::regclass, 0, 0), ('pg_ts_dict'::regclass, 0, 0), ('pg_ts_template'::regclass, 0, 0), ('pg_ts_config'::regclass, 0, 0), ('pg_authid'::regclass, 0, 0), ('pg_auth_members'::regclass, 0, 0), ('pg_database'::regclass, 0, 0), ('pg_tablespace'::regclass, 0, 0), ('pg_foreign_data_wrapper'::regclass, 0, 0), ('pg_foreign_server'::regclass, 0, 0), ('pg_user_mapping'::regclass, 0, 0), ('pg_default_acl'::regclass, 0, 0), ('pg_extension'::regclass, 0, 0), ('pg_event_trigger'::regclass, 0, 0), ('pg_parameter_acl'::regclass, 0, 0), ('pg_policy'::regclass, 0, 0), ('pg_publication'::regclass, 0, 0), ('pg_publication_namespace'::regclass, 0, 0), ('pg_publication_rel'::regclass, 0, 0), ('pg_subscription'::regclass, 0, 0), ('pg_transform'::regclass, 0, 0) ) SELECT ROW(pg_identify_object(objects.classid, objects.objid, objects.objsubid)) AS ident, ROW(pg_identify_object_as_address(objects.classid, objects.objid, objects.objsubid)) AS addr, pg_describe_object(objects.classid, objects.objid, objects.objsubid) AS descr FROM objects ORDER BY objects.classid, objects.objid, objects.objsubid;"#,
    r#"SELECT lo_unlink(424242);"#,
    r#"DROP EVENT TRIGGER ldo_evt;"#,
    r#"DROP FUNCTION ldo_evtfn();"#,
    r#"DROP SUBSCRIPTION ldo_sub;"#,
    r#"DROP PUBLICATION ldo_pub;"#,
    r#"DROP PUBLICATION ldo_pub_schema;"#,
    r#"DROP TRANSFORM FOR int LANGUAGE SQL;"#,
    r#"DROP FOREIGN DATA WRAPPER ldo_fdw CASCADE;"#,
    r#"DROP SCHEMA ldo_nsp CASCADE;"#,
    r#"DROP TEXT SEARCH CONFIGURATION ldo_ts_conf;"#,
    r#"DROP TEXT SEARCH DICTIONARY ldo_ts_dict;"#,
    r#"DROP TEXT SEARCH TEMPLATE ldo_ts_temp;"#,
    r#"DROP TEXT SEARCH PARSER ldo_ts_prs;"#,
    r#"REVOKE SET ON PARAMETER work_mem FROM ldo_user;"#,
    r#"ALTER DEFAULT PRIVILEGES FOR ROLE ldo_user IN SCHEMA public REVOKE ALL ON TABLES FROM ldo_user;"#,
    r#"ALTER DEFAULT PRIVILEGES FOR ROLE ldo_user GRANT DELETE ON TABLES TO ldo_user;"#,
    r#"DROP OWNED BY ldo_user;"#,
    r#"DROP ROLE ldo_user;"#,
    r#"DROP ROLE ldo_user2;"#,
];

const DEPARSE_DECK: &[&str] = &[
    r#"CREATE TYPE ldp_comp AS (x int, y text);"#,
    r#"CREATE TYPE ldp_enum AS ENUM ('red', 'green', 'blue');"#,
    r#"CREATE DOMAIN ldp_dom AS int CHECK (VALUE > 0 AND VALUE < 1000000);"#,
    r#"CREATE TABLE ldp_t ( a int PRIMARY KEY, b text, vb varchar(10), arr int[], num numeric(8,2), ts timestamptz, tsn timestamp, dt date, tm time, tmz timetz, iv interval, js jsonb, j json, x xml, bt bit(4), vbt varbit, byt bytea, r int4range, comp ldp_comp, dom ldp_dom, en ldp_enum, flag boolean );"#,
    r#"CREATE TABLE ldp_t2 (a int PRIMARY KEY, b text, c int);"#,
    r#"CREATE TABLE ldp_par (a int, b text);"#,
    r#"CREATE TABLE ldp_chi (extra int) INHERITS (ldp_par);"#,
    r#"CREATE TABLE ldp_ids (id int GENERATED ALWAYS AS IDENTITY, v int);"#,
    r#"CREATE TABLE ldp_hashp (a int, b text) PARTITION BY HASH (a);"#,
    r#"CREATE TABLE ldp_hashp0 PARTITION OF ldp_hashp FOR VALUES WITH (MODULUS 2, REMAINDER 0);"#,
    r#"CREATE TABLE ldp_listp (a text COLLATE "C", b int) PARTITION BY LIST (a);"#,
    r#"CREATE TABLE ldp_listp1 PARTITION OF ldp_listp FOR VALUES IN ('x', 'y', NULL);"#,
    r#"CREATE TABLE ldp_listpd PARTITION OF ldp_listp DEFAULT;"#,
    r#"CREATE TABLE ldp_rangep (a int, b int, c text) PARTITION BY RANGE (a, (a + b));"#,
    r#"CREATE TABLE ldp_rangep1 PARTITION OF ldp_rangep FOR VALUES FROM (MINVALUE, MINVALUE) TO (0, 10);"#,
    r#"CREATE TABLE ldp_rangep2 PARTITION OF ldp_rangep FOR VALUES FROM (0, 10) TO (100, MAXVALUE);"#,
    r#"CREATE VIEW ldp_v01 AS SELECT a, b, GREATEST(a, c) AS g, LEAST(a, c, 0) AS l, COALESCE(b, 'x') AS co, NULLIF(a, c) AS ni FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v02 AS SELECT a IS DISTINCT FROM c AS d1, a IS NOT DISTINCT FROM c AS d2, b IS NULL AS n1, b IS NOT NULL AS n2, ROW(a, c) IS NULL AS rn FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v03 AS SELECT flag IS TRUE AS b1, flag IS NOT TRUE AS b2, flag IS FALSE AS b3, flag IS NOT FALSE AS b4, flag IS UNKNOWN AS b5, flag IS NOT UNKNOWN AS b6 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v04 AS SELECT CASE WHEN a > 0 THEN 'pos' WHEN a < 0 THEN 'neg' ELSE 'zero' END AS c1, CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END AS c2 FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v05 AS SELECT ARRAY[a, c, 3] AS a1, ARRAY[[1, 2], [3, 4]] AS a2, ARRAY[]::int[] AS a3, ARRAY(SELECT x.a FROM ldp_t2 x WHERE x.a < ldp_t2.a ORDER BY x.a) AS a4 FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v06 AS SELECT ROW(a, b::text)::ldp_comp AS r1, ROW(a, c)::ldp_comp IS NOT NULL AS rc, (ROW(a, b) = ROW(c, 'z')) AS req, (a, c) < (c, a) AS rcmp FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v07 AS SELECT arr[1] AS s1, arr[1:2] AS s2, arr[:2] AS s3, arr[2:] AS s4, arr[:] AS s5, (comp).x AS f1, (comp).y AS f2, (ldp_t.*).a AS f3 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v08 AS SELECT a = ANY (arr) AS any1, a = ALL (arr) AS all1, a IN (1, 2, 3) AS in1, b LIKE 'x%' AS lk, b NOT LIKE 'y%' AS nlk, b SIMILAR TO 'x_' AS sim FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v09 AS SELECT vb::text AS relab, b COLLATE "C" AS coll, a::bigint AS cast1, num::int AS cast2, b::xml AS civ, arr::numeric[] AS acoerce, dom AS domout, (a + 1)::ldp_dom AS domin FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v10 AS SELECT (ldp_chi.*)::ldp_par AS cvt, ldp_chi.extra FROM ldp_chi;"#,
    r#"CREATE VIEW ldp_v11 AS SELECT CURRENT_DATE AS d1, CURRENT_TIME AS d2, CURRENT_TIME(2) AS d3, CURRENT_TIMESTAMP AS d4, CURRENT_TIMESTAMP(1) AS d5, LOCALTIME AS d6, LOCALTIME(0) AS d7, LOCALTIMESTAMP AS d8, LOCALTIMESTAMP(3) AS d9;"#,
    r#"CREATE VIEW ldp_v12 AS SELECT CURRENT_ROLE AS u1, CURRENT_USER AS u2, USER AS u3, SESSION_USER AS u4, SYSTEM_USER AS u5, CURRENT_CATALOG AS u6, CURRENT_SCHEMA AS u7;"#,
    r#"CREATE VIEW ldp_v13 AS SELECT XMLCONCAT(x, x) AS x1 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v14 AS SELECT XMLROOT(x, VERSION '1.0', STANDALONE YES) AS x1, XMLROOT(x, VERSION NO VALUE, STANDALONE NO) AS x2, XMLROOT(x, VERSION '1.1', STANDALONE NO VALUE) AS x3, XMLSERIALIZE(DOCUMENT x AS text) AS x4, XMLSERIALIZE(CONTENT x AS varchar INDENT) AS x5, x IS DOCUMENT AS x6, XMLEXISTS('//p' PASSING x) AS x7 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v15 AS SELECT EXISTS (SELECT 1 FROM ldp_t2 i WHERE i.a = o.a) AS e1, o.a = ANY (SELECT a FROM ldp_t2) AS e2, o.a < ALL (SELECT c FROM ldp_t2) AS e3, (o.a, o.c) = (SELECT a, c FROM ldp_t2 LIMIT 1) AS e4, (SELECT max(a) FROM ldp_t2) AS e5, (o.a, o.c) < (SELECT a, c FROM ldp_t2 LIMIT 1) AS e6, ARRAY(SELECT a FROM ldp_t2 ORDER BY a) AS e7 FROM ldp_t2 o;"#,
    r#"CREATE VIEW ldp_v16 AS SELECT count(*) AS c1, count(DISTINCT a) AS c2, array_agg(a ORDER BY a DESC) AS c3, sum(a) FILTER (WHERE a > 0) AS c4, string_agg(b, ',' ORDER BY b) AS c5, percentile_cont(0.5) WITHIN GROUP (ORDER BY a::float8) AS c6, mode() WITHIN GROUP (ORDER BY a) AS c7 FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v17 AS SELECT a, c, GROUPING(a, c) AS g1, sum(c) AS s FROM ldp_t2 GROUP BY GROUPING SETS ((a), (c), ()) HAVING GROUPING(a) = 0;"#,
    r#"CREATE VIEW ldp_v18 AS SELECT a, rank() OVER w AS r1, sum(c) OVER (PARTITION BY b ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r2, count(*) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r3, ntile(4) OVER (ORDER BY a GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING EXCLUDE TIES) AS r4, lag(c, 1, 0) OVER w AS r5 FROM ldp_t2 WINDOW w AS (PARTITION BY c ORDER BY a);"#,
    r#"CREATE VIEW ldp_v19 AS SELECT EXTRACT(EPOCH FROM ts) AS f1, EXTRACT(YEAR FROM dt) AS f2, EXTRACT(DOW FROM tsn) AS f3, EXTRACT(MICROSECONDS FROM tm) AS f4, EXTRACT(HOUR FROM tmz) AS f5, EXTRACT(DAY FROM iv) AS f6, date_part('epoch', ts) AS f7 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v20 AS SELECT OVERLAY(b PLACING 'XX' FROM 2) AS o1, OVERLAY(b PLACING 'XX' FROM 2 FOR 1) AS o2, OVERLAY(byt PLACING byt FROM 1) AS o3, OVERLAY(bt PLACING bt FROM 1 FOR 2) AS o4, POSITION('x' IN b) AS p1, POSITION(byt IN byt) AS p2, POSITION(bt IN bt) AS p3 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v21 AS SELECT SUBSTRING(b FROM 2) AS s1, SUBSTRING(b FROM 2 FOR 3) AS s2, SUBSTRING(byt FROM 2) AS s3, SUBSTRING(bt FROM 1 FOR 2) AS s4, SUBSTRING(b SIMILAR 'x#"_#"' ESCAPE '#') AS s5, TRIM(BOTH ' ' FROM b) AS t1, TRIM(LEADING FROM b) AS t2, TRIM(TRAILING 'z' FROM b) AS t3, TRIM(BOTH byt FROM byt) AS t4 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v22 AS SELECT ts AT TIME ZONE 'UTC' AS z1, tsn AT TIME ZONE 'America/New_York' AS z2, tmz AT TIME ZONE INTERVAL '05:00' AS z3, ts AT LOCAL AS z4, tmz AT LOCAL AS z5, (dt, dt + 1) OVERLAPS (dt, dt + 2) AS ov1, (ts, iv) OVERLAPS (ts, iv) AS ov2 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v23 AS SELECT NORMALIZE(b) AS n1, NORMALIZE(b, NFD) AS n2, b IS NFC NORMALIZED AS n3, b IS NFKD NORMALIZED AS n4, COLLATION FOR (b) AS n5 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v24 AS SELECT JSON_OBJECT('k1': a, 'k2': b ABSENT ON NULL WITH UNIQUE KEYS) AS j1, JSON_ARRAY(a, c NULL ON NULL) AS j2, JSON_OBJECT(RETURNING jsonb) AS j3, JSON_ARRAY(RETURNING text) AS j4 FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v25 AS SELECT JSON_EXISTS(js, '$.k' PASSING a AS pa) AS q1, JSON_QUERY(js, '$.arr[*]' WITH WRAPPER) AS q2, JSON_QUERY(js, '$.s' RETURNING text OMIT QUOTES EMPTY ARRAY ON EMPTY ERROR ON ERROR) AS q3, JSON_VALUE(js, '$.n' RETURNING int DEFAULT 0 ON ERROR) AS q4, js IS JSON OBJECT AS q5, b IS JSON SCALAR AS q6, b IS NOT JSON ARRAY WITH UNIQUE KEYS AS q7, JSON(b) AS q8, JSON_SCALAR(a) AS q9, JSON_SERIALIZE(js RETURNING bytea) AS q10 FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v26 AS SELECT json_agg(a ORDER BY a) AS g1, json_object_agg(b, a) AS g2, JSON_ARRAYAGG(a ORDER BY a RETURNING jsonb) AS g3, JSON_OBJECTAGG(b: a ABSENT ON NULL RETURNING jsonb) AS g4 FROM ldp_t2;"#,
    r#"CREATE VIEW ldp_v27 AS WITH RECURSIVE mat AS MATERIALIZED (SELECT a, c FROM ldp_t2), nomat AS NOT MATERIALIZED (SELECT a FROM ldp_t2), rec (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM rec WHERE n < 5) SELECT mat.a, rec.n FROM mat, nomat, rec WHERE mat.a = nomat.a;"#,
    r#"CREATE VIEW ldp_v28 AS WITH RECURSIVE srch (id, parent) AS (SELECT a, c FROM ldp_t2 UNION ALL SELECT t.a, t.c FROM ldp_t2 t JOIN srch s ON t.c = s.id) SEARCH DEPTH FIRST BY id SET ord CYCLE id SET is_cycle USING path SELECT id, is_cycle FROM srch;"#,
    r#"CREATE VIEW ldp_v29 AS SELECT j1.a, j2.bb FROM ldp_t2 j1 JOIN ldp_t2 j2 (aa, bb, cc) ON j1.a = j2.aa LEFT JOIN ldp_t2 j3 USING (a) AS ua FULL JOIN ldp_t2 j4 ON j1.a = j4.a AND j4.c > 0 CROSS JOIN LATERAL (SELECT j1.a + 1 AS lat) l;"#,
    r#"CREATE VIEW ldp_v30 AS SELECT s.a, v.q FROM ldp_t2 s TABLESAMPLE SYSTEM (50) REPEATABLE (1), (VALUES (1, 'x'), (2, 'y')) v (q, w), generate_series(1, 3) WITH ORDINALITY gs (g, ord), ROWS FROM (generate_series(1, 2), generate_series(3, 4)) rf (r1, r2), json_to_record('{"p": 1}') AS jr (p int);"#,
    r#"CREATE VIEW ldp_v31 AS SELECT (sub).a AS f1, (sub).b AS f2 FROM (SELECT t AS sub FROM ldp_t2 t) s;"#,
    r#"CREATE VIEW ldp_v32 AS SELECT (cte_row).x FROM (WITH c AS (SELECT ROW(1, 'z')::ldp_comp AS cte_row) SELECT cte_row FROM c) s;"#,
    r#"CREATE VIEW ldp_v33 AS SELECT xt.* FROM ldp_t, XMLTABLE('//row' PASSING x COLUMNS id int PATH '@id', val text PATH 'v' DEFAULT 'none', ord FOR ORDINALITY) xt;"#,
    r#"CREATE VIEW ldp_v34 AS SELECT jt.* FROM ldp_t, JSON_TABLE(js, '$.rows[*]' PASSING a AS pa COLUMNS (rn FOR ORDINALITY, id int PATH '$.id', txt text PATH '$.t' DEFAULT 'x' ON EMPTY, NESTED PATH '$.sub[*]' COLUMNS (s1 int PATH '$'))) jt;"#,
    r#"CREATE VIEW ldp_v35 AS SELECT DISTINCT ON (c) a, c FROM ldp_t2 ORDER BY c, a DESC NULLS LAST LIMIT 10 OFFSET 2;"#,
    r#"CREATE VIEW ldp_v36 AS SELECT a FROM ldp_t2 UNION ALL SELECT a FROM ldp_t2 INTERSECT SELECT c FROM ldp_t2 EXCEPT ALL SELECT 0;"#,
    r#"CREATE VIEW ldp_v37 AS SELECT a, b FROM ldp_t2 ORDER BY a FETCH FIRST 3 ROWS WITH TIES;"#,
    r#"CREATE VIEW ldp_v38 AS SELECT ('{"a":1}'::jsonb)['a'] AS jsub, ('[[1,2],[3,4]]'::jsonb)[0][1] AS jsub2, num + 1.5 AS arith, a % 3 AS modulo, |/ (a::float8 + 1) AS sqrtop, a BETWEEN dom AND dom + 10 AS betw, a NOT BETWEEN SYMMETRIC dom AND 0 AS betws FROM ldp_t;"#,
    r#"CREATE VIEW ldp_v39 AS SELECT concat_ws(',', VARIADIC ARRAY[b, 'x']) AS var1, format('%s-%s', a, b) AS fmt, make_date(year => 2024, month => a % 12 + 1, day => 2) AS namedarg, ntile(a) OVER () AS wfarg FROM ldp_t2 WHERE a > 0;"#,
    r#"CREATE VIEW ldp_v40 AS WITH cc AS (SELECT 2 AS q, 'w'::text AS w) SELECT (s.*).one AS a1, (cc.*).q AS a2, (v.*).column1 AS a3, (j.*).ax AS a4, (rf.*).r1 AS a5 FROM (SELECT 1 AS one, 2 AS two) s, cc, (VALUES (3, 4)) v, (ldp_t2 t1 JOIN ldp_t2 t2 USING (a)) AS j (ax, b1, c1, b2, c2), ROWS FROM (generate_series(1, 2), generate_series(3, 4)) AS rf (r1, r2);"#,
    r#"CREATE VIEW ldp_v41 AS SELECT (ss.sub).one AS f1, (ss.jrow).a AS f2, (ss.crow).q AS f3, (ss.vrow).column1 AS f4, (ss.frow).x AS f5 FROM (SELECT s AS sub, jj AS jrow, cc AS crow, v AS vrow, fr AS frow FROM (SELECT 1 AS one, 2 AS two) s, (ldp_t2 t1 JOIN ldp_t2 t2 USING (a)) jj, (WITH c AS (SELECT 3 AS q) SELECT q FROM c) cc, (VALUES (4, 5)) v, json_to_record('{"x": 6}') AS fr (x int)) ss;"#,
    r#"CREATE RULE ldp_r1 AS ON INSERT TO ldp_par DO INSTEAD INSERT INTO ldp_t2 (a, b, c) VALUES (NEW.a, NEW.b, DEFAULT) RETURNING a, b;"#,
    r#"CREATE RULE ldp_r2 AS ON UPDATE TO ldp_par DO INSTEAD UPDATE ldp_t2 SET (a, b) = (SELECT NEW.a, NEW.b), c = OLD.a WHERE a = OLD.a RETURNING a, b;"#,
    r#"CREATE RULE ldp_r3 AS ON DELETE TO ldp_par WHERE OLD.a > 10 DO INSTEAD DELETE FROM ldp_t2 WHERE a = OLD.a;"#,
    r#"CREATE RULE ldp_r4 AS ON UPDATE TO ldp_t DO ALSO UPDATE ldp_t2 SET c = NEW.a WHERE b = OLD.b;"#,
    r#"CREATE RULE ldp_r5 AS ON INSERT TO ldp_t2 WHERE NEW.a IS NULL DO INSTEAD NOTHING;"#,
    r#"CREATE RULE ldp_r6 AS ON INSERT TO ldp_par DO ALSO NOTIFY ldp_chan;"#,
    r#"CREATE RULE ldp_r7 AS ON UPDATE TO ldp_chi DO INSTEAD UPDATE ldp_t SET comp.x = NEW.extra, arr[1] = NEW.extra, arr[2:3] = ARRAY[1, 2] WHERE a = OLD.extra;"#,
    r#"CREATE RULE ldp_r8 AS ON INSERT TO ldp_chi DO INSTEAD INSERT INTO ldp_t2 VALUES (NEW.extra, 'z', 0) ON CONFLICT (a) DO UPDATE SET b = excluded.b || ldp_t2.b WHERE ldp_t2.c > 0;"#,
    r#"CREATE RULE ldp_r9 AS ON DELETE TO ldp_chi DO INSTEAD WITH del AS (SELECT 1 AS one) DELETE FROM ldp_t2 USING del WHERE a = OLD.extra RETURNING WITH (OLD AS o, NEW AS n) o.a, n.b, o.a + 1;"#,
    r#"ALTER TABLE ldp_t2 ADD CONSTRAINT ldp_c_chk CHECK (a > c OR c IS NULL) NO INHERIT NOT VALID;"#,
    r#"ALTER TABLE ldp_t2 ADD CONSTRAINT ldp_c_uniq UNIQUE NULLS NOT DISTINCT (b, c) DEFERRABLE INITIALLY DEFERRED;"#,
    r#"ALTER TABLE ldp_t ADD CONSTRAINT ldp_c_fk FOREIGN KEY (a) REFERENCES ldp_t2 (a) MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL DEFERRABLE;"#,
    r#"ALTER TABLE ldp_t2 ADD CONSTRAINT ldp_c_fk2 FOREIGN KEY (c) REFERENCES ldp_t2 (a) ON DELETE SET DEFAULT NOT VALID;"#,
    r#"ALTER TABLE ldp_t ADD CONSTRAINT ldp_c_excl EXCLUDE USING gist (r WITH &&) WHERE (flag);"#,
    r#"ALTER TABLE ldp_t2 ADD CONSTRAINT ldp_c_nn NOT NULL c;"#,
    r#"CREATE INDEX ldp_ix1 ON ldp_t2 (a DESC NULLS LAST, lower(b) text_pattern_ops, (a + c)) INCLUDE (b) WHERE c IS NOT NULL;"#,
    r#"CREATE INDEX ldp_ix2 ON ldp_t2 USING hash (b);"#,
    r#"CREATE INDEX ldp_ix3 ON ldp_t USING gin (js jsonb_path_ops);"#,
    r#"CREATE INDEX ldp_ix4 ON ldp_t USING gist (r);"#,
    r#"CREATE INDEX ldp_ix5 ON ldp_t USING brin (a int4_minmax_multi_ops (values_per_range = 16));"#,
    r#"CREATE INDEX ldp_ix6 ON ldp_t2 (b COLLATE "C" varchar_pattern_ops);"#,
    r#"CREATE UNIQUE INDEX ldp_ix7 ON ldp_t2 (a) NULLS NOT DISTINCT;"#,
    r#"CREATE STATISTICS ldp_st1 (ndistinct, dependencies, mcv) ON a, c FROM ldp_t2;"#,
    r#"CREATE STATISTICS ldp_st2 ON (a + c), lower(b) FROM ldp_t2;"#,
    r#"CREATE FUNCTION ldp_tgfn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;"#,
    r#"CREATE TRIGGER ldp_tg1 BEFORE UPDATE OF a, c ON ldp_t2 FOR EACH ROW WHEN (OLD.a IS DISTINCT FROM NEW.a) EXECUTE FUNCTION ldp_tgfn('arg1', 'arg2');"#,
    r#"CREATE TRIGGER ldp_tg2 AFTER UPDATE ON ldp_t2 REFERENCING OLD TABLE AS oldtab NEW TABLE AS newtab FOR EACH STATEMENT EXECUTE FUNCTION ldp_tgfn();"#,
    r#"CREATE TRIGGER ldp_tg3 INSTEAD OF DELETE ON ldp_v01 FOR EACH ROW EXECUTE FUNCTION ldp_tgfn();"#,
    r#"CREATE CONSTRAINT TRIGGER ldp_tg4 AFTER DELETE ON ldp_t2 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ldp_tgfn();"#,
    r#"CREATE TRIGGER ldp_tg5 BEFORE TRUNCATE ON ldp_t2 FOR EACH STATEMENT EXECUTE FUNCTION ldp_tgfn();"#,
    r#"CREATE FUNCTION ldp_fn1(int, b text DEFAULT 'd', VARIADIC rest int[] DEFAULT '{}') RETURNS int LANGUAGE sql IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE COST 42 AS 'SELECT $1';"#,
    r#"CREATE FUNCTION ldp_fn2(IN p1 int, OUT o1 int, INOUT io1 text) LANGUAGE sql SECURITY DEFINER SET work_mem = '12MB' SET search_path = public, pg_temp AS 'SELECT $1 + 1, $2';"#,
    r#"CREATE FUNCTION ldp_fn3(p int) RETURNS TABLE (t1 int, t2 text) LANGUAGE sql STABLE ROWS 7 AS 'SELECT p, ''x''';"#,
    r#"CREATE FUNCTION ldp_fn4(p int) RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT p + 1; END;"#,
    r#"CREATE FUNCTION ldp_fn5(p int) RETURNS int LANGUAGE sql RETURN p * 2 + (SELECT count(*) FROM ldp_t2)::int;"#,
    r#"CREATE PROCEDURE ldp_pr1(INOUT x int) LANGUAGE sql BEGIN ATOMIC SELECT x + 1; END;"#,
    r#"CREATE FUNCTION ldp_fn6() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT EXTRACT(DOY FROM CURRENT_DATE)::int + POSITION('a' IN 'abc') + length(TRIM(BOTH 'x' FROM 'xax')); END;"#,
    r#"SELECT viewname, pg_get_viewdef(('public.' || viewname)::regclass) FROM pg_views WHERE viewname LIKE 'ldp\_v%' AND viewname <> 'ldp_v28' ORDER BY viewname;"#,
    r#"SELECT viewname, pg_get_viewdef(('public.' || viewname)::regclass, true) FROM pg_views WHERE viewname LIKE 'ldp\_v%' AND viewname <> 'ldp_v28' ORDER BY viewname;"#,
    r#"SELECT pg_get_viewdef('ldp_v15'::regclass, 30);"#,
    r#"SELECT pg_get_viewdef('ldp_v29'::regclass, 0);"#,
    r#"SELECT r.rulename, pg_get_ruledef(r.oid), pg_get_ruledef(r.oid, true) FROM pg_rewrite r WHERE r.rulename LIKE 'ldp\_r%' ORDER BY r.rulename;"#,
    r#"SELECT conname, pg_get_constraintdef(oid), pg_get_constraintdef(oid, true) FROM pg_constraint WHERE conname LIKE 'ldp\_c\_%' ORDER BY conname;"#,
    r#"SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE contype = 'c' AND conname = 'ldp_dom_check' ORDER BY conname;"#,
    r#"SELECT c.relname, pg_get_indexdef(c.oid), pg_get_indexdef(c.oid, 1, true) FROM pg_class c WHERE c.relname LIKE 'ldp\_ix%' ORDER BY c.relname;"#,
    r#"SELECT t.tgname, pg_get_triggerdef(t.oid), pg_get_triggerdef(t.oid, true) FROM pg_trigger t WHERE t.tgname LIKE 'ldp\_tg%' ORDER BY t.tgname;"#,
    r#"SELECT s.stxname, pg_get_statisticsobjdef(s.oid) FROM pg_statistic_ext s WHERE s.stxname LIKE 'ldp\_st%' ORDER BY s.stxname;"#,
    r#"SELECT relname, pg_get_partkeydef(oid) FROM pg_class WHERE relname IN ('ldp_hashp', 'ldp_listp', 'ldp_rangep') ORDER BY relname;"#,
    r#"SELECT c.relname, pg_get_expr(c.relpartbound, c.oid), pg_get_expr(c.relpartbound, c.oid, true) FROM pg_class c WHERE c.relname ~ '^ldp_(hashp|listp|rangep)[0-9d]' ORDER BY c.relname;"#,
    r#"SELECT p.proname, pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname LIKE 'ldp\_fn%' ORDER BY p.proname;"#,
    r#"SELECT pg_get_functiondef('ldp_pr1'::regproc::oid);"#,
    r#"SELECT pg_get_functiondef('ldp_tgfn'::regproc::oid);"#,
    r#"SELECT p.proname, pg_get_function_arguments(p.oid), pg_get_function_identity_arguments(p.oid), pg_get_function_result(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname LIKE 'ldp\_%' ORDER BY p.proname;"#,
    r#"SELECT a.attname, pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE d.adrelid = 'ldp_ids'::regclass ORDER BY a.attnum;"#,
    r#"SELECT pg_get_serial_sequence('ldp_ids', 'id');"#,
    r#"SELECT pg_get_expr(i.indexprs, i.indrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'ldp_ix1';"#,
    r#"SELECT pg_get_expr(i.indpred, i.indrelid, true) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'ldp_ix1';"#,
];

const EXPLAIN_DECK: &[&str] = &[
    r#"EXPLAIN (VERBOSE, COSTS OFF) SELECT a FROM ldp_t2 o WHERE EXISTS (SELECT 1 FROM ldp_t2 i WHERE i.a = o.c) OR a = 0;"#,
    r#"EXPLAIN (VERBOSE, COSTS OFF) SELECT (SELECT max(a) FROM ldp_t2 WHERE c = o.a) FROM ldp_t2 o;"#,
    r#"EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ldp_ids (v) VALUES (1);"#,
    r#"EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ldp_t2 VALUES (1, 'x', 0) ON CONFLICT (a) DO UPDATE SET c = excluded.c + 1 WHERE ldp_t2.b <> 'q';"#,
    r#"EXPLAIN (VERBOSE, COSTS OFF) UPDATE ldp_t2 SET (a, b) = (SELECT 1, 'x'), c = 0 WHERE a = 3;"#,
    r#"BEGIN;"#,
    r#"DECLARE ldp_cur CURSOR FOR SELECT a FROM ldp_t2 WHERE c > 0 FOR UPDATE;"#,
    r#"EXPLAIN (VERBOSE, COSTS OFF) UPDATE ldp_t2 SET c = 1 WHERE CURRENT OF ldp_cur;"#,
    r#"ROLLBACK;"#,
];

const DEPARSE_CLEANUP: &[&str] = &[
    r#"DROP TABLE ldp_rangep, ldp_listp, ldp_hashp;"#,
    r#"DROP TABLE ldp_ids;"#,
    r#"DROP VIEW ldp_v41, ldp_v40, ldp_v39, ldp_v38, ldp_v37, ldp_v36, ldp_v35, ldp_v34, ldp_v33, ldp_v32, ldp_v31, ldp_v30, ldp_v29, ldp_v28, ldp_v27, ldp_v26, ldp_v25, ldp_v24, ldp_v23, ldp_v22, ldp_v21, ldp_v20, ldp_v19, ldp_v18, ldp_v17, ldp_v16, ldp_v15, ldp_v14, ldp_v13, ldp_v12, ldp_v11, ldp_v10, ldp_v09, ldp_v08, ldp_v07, ldp_v06, ldp_v05, ldp_v04, ldp_v03, ldp_v02, ldp_v01;"#,
    r#"DROP FUNCTION ldp_fn1(int, text, int[]);"#,
    r#"DROP FUNCTION ldp_fn2(int, text);"#,
    r#"DROP FUNCTION ldp_fn3(int);"#,
    r#"DROP FUNCTION ldp_fn4(int);"#,
    r#"DROP FUNCTION ldp_fn5(int);"#,
    r#"DROP FUNCTION ldp_fn6();"#,
    r#"DROP PROCEDURE ldp_pr1(int);"#,
    r#"DROP TABLE ldp_chi, ldp_par CASCADE;"#,
    r#"DROP TABLE ldp_t CASCADE;"#,
    r#"DROP TABLE ldp_t2 CASCADE;"#,
    r#"DROP FUNCTION ldp_tgfn();"#,
    r#"DROP DOMAIN ldp_dom;"#,
    r#"DROP TYPE ldp_comp;"#,
    r#"DROP TYPE ldp_enum;"#,
];

const SHAPES: &[&str] = &["objid:address", "objid:deparse", "objid:explain"];

/// Optional seeded extra probes, each independently parity-verified:
/// appended between the battery and its cleanup. All are read-only.
const ADDRESS_EXTRAS: &[&str] = &[
    r#"SELECT pg_describe_object('pg_class'::regclass, 'ldo_nsp.gentable'::regclass, 2);"#,
    r#"SELECT pg_identify_object('pg_class'::regclass, 'ldo_nsp.genview'::regclass, 0);"#,
    r#"SELECT pg_identify_object_as_address('pg_class'::regclass, 'ldo_nsp.genmatview'::regclass, 0);"#,
    r#"SELECT pg_describe_object('pg_type'::regclass, 'ldo_nsp.genenum'::regtype, 0);"#,
];
const DEPARSE_EXTRAS: &[&str] = &[
    r#"SELECT pg_get_viewdef('ldp_v15'::regclass, 60);"#,
    r#"SELECT pg_get_viewdef('ldp_v18'::regclass, 8);"#,
    r#"SELECT pg_get_ruledef(r.oid) FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class WHERE c.relname = 'ldp_v15' AND r.rulename = '_RETURN';"#,
    r#"SELECT pg_get_indexdef(c.oid, 2, false) FROM pg_class c WHERE c.relname = 'ldp_ix1';"#,
];

fn raw_deck(out: &mut Vec<StmtKind>, deck: &[&str]) {
    out.extend(deck.iter().map(|s| StmtKind::Raw((*s).to_string())));
}

/// Registry entry point (stmt::STMT_MODULES): one self-contained
/// identity/deparse battery group.
pub fn gen_objid_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objid");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    let mut out = Vec::new();
    match shape {
        "objid:address" => {
            // Deck order is load-bearing (creates -> probes -> cleanup);
            // the seeded extras slot in before the first cleanup stmt.
            let split = ADDRESS_DECK
                .iter()
                .position(|s| s.starts_with("SELECT lo_unlink"))
                .unwrap();
            raw_deck(&mut out, &ADDRESS_DECK[..split]);
            if g.rng.chance(1, 2) {
                g.fire("objid:address:extras");
                raw_deck(&mut out, ADDRESS_EXTRAS);
            }
            raw_deck(&mut out, &ADDRESS_DECK[split..]);
        }
        "objid:deparse" => {
            raw_deck(&mut out, DEPARSE_DECK);
            if g.rng.chance(1, 2) {
                g.fire("objid:deparse:extras");
                raw_deck(&mut out, DEPARSE_EXTRAS);
            }
            raw_deck(&mut out, DEPARSE_CLEANUP);
        }
        "objid:explain" => {
            // The EXPLAIN battery needs the deparse fixtures; reuse the
            // fixture prefix of the deck (everything before the first
            // CREATE VIEW) plus the identity table, then clean up.
            let vfirst = DEPARSE_DECK
                .iter()
                .position(|s| s.starts_with("CREATE VIEW"))
                .unwrap();
            raw_deck(&mut out, &DEPARSE_DECK[..vfirst]);
            raw_deck(&mut out, EXPLAIN_DECK);
            raw_deck(&mut out, DEPARSE_CLEANUP);
        }
        other => unreachable!("unknown objid shape {other}"),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, w: &WeightTable) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            groups.push(
                gen_objid_module(&mut g).iter().map(|s| s.to_sql()).collect::<Vec<_>>(),
            );
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    /// Deck hygiene: single-line semicolon-terminated statements only, and
    /// the known-divergent SEARCH/CYCLE viewdef probe is never embedded.
    #[test]
    fn decks_are_single_line_and_exclude_ld1_f1() {
        for deck in [ADDRESS_DECK, DEPARSE_DECK, EXPLAIN_DECK, DEPARSE_CLEANUP,
                     ADDRESS_EXTRAS, DEPARSE_EXTRAS] {
            for s in deck {
                assert!(s.ends_with(';'), "{s}");
                assert!(!s.contains('\n'), "{s}");
                assert!(
                    !s.contains("pg_get_viewdef('ldp_v28'"),
                    "LD1-F1 divergent probe must stay deck-only: {s}"
                );
                assert!(!s.contains("XMLPARSE") && !s.contains("XMLELEMENT"),
                    "no-libxml A side rejects this at CREATE: {s}");
            }
        }
        // The sweep that IS embedded must exclude the LD1-F1 view.
        assert!(DEPARSE_DECK
            .iter()
            .filter(|s| s.contains("pg_get_viewdef(('public.' || viewname)"))
            .all(|s| s.contains("viewname <> 'ldp_v28'")));
    }

    /// Every group is self-contained: object classes created in a group
    /// are dropped (or cascade-dropped) before the group ends, and txn
    /// brackets balance.
    #[test]
    fn groups_are_self_contained() {
        let (groups, prods) = gen_groups(0x1D01, 60, &WeightTable::defaults());
        for group in &groups {
            let opens = group.iter().filter(|s| *s == "BEGIN;").count();
            let closes = group
                .iter()
                .filter(|s| *s == "COMMIT;" || *s == "ROLLBACK;")
                .count();
            assert_eq!(opens, closes, "unbalanced txn bracket");
            for (create, drop) in [
                ("CREATE ROLE", "DROP ROLE"),
                ("CREATE SCHEMA", "DROP SCHEMA"),
                ("CREATE PUBLICATION", "DROP PUBLICATION"),
                ("CREATE SUBSCRIPTION", "DROP SUBSCRIPTION"),
                ("CREATE EVENT TRIGGER", "DROP EVENT TRIGGER"),
            ] {
                let c = group.iter().filter(|s| s.starts_with(create)).count();
                let d = group.iter().filter(|s| s.starts_with(drop)).count();
                assert_eq!(c, d, "{create} without {drop}");
            }
            // Tables/types/views/functions are dropped explicitly or via
            // CASCADE; every group that creates any ldo_/ldp_ object must
            // end in a cleanup tail containing DROP statements.
            if group.iter().any(|s| s.starts_with("CREATE")) {
                assert!(group.iter().any(|s| s.starts_with("DROP")), "no cleanup tail");
            }
            // Any SET opened by the address battery is RESET in-group.
            let sets = group.iter().filter(|s| s.starts_with("SET ")).count();
            let resets = group.iter().filter(|s| s.starts_with("RESET ")).count();
            assert_eq!(sets, resets, "SET without RESET");
        }
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
    }

    #[test]
    fn objid_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_groups(7, 40, &w);
        let (b, _) = gen_groups(7, 40, &w);
        assert_eq!(a, b);
    }
}
