//! ERROR-ARM drain module (LD6, line-drain queue chunks `ddl-cmds-residue`
//! + `parser-arms` + `catalog-residue` + `ddl-tablecmds`).
//!
//! The ERROR-ARM bucket is the ereport(ERROR)/validation mass the
//! happy-path corpus never triggers: `ereport` branches in DDL commands
//! (typecmds/indexcmds/aggregate/sequence/statscmds/collationcmds/user/
//! opclasscmds/tablecmds), parse-analysis validation (parse_coerce/
//! parse_agg/parse_func/parse_expr/parse_relation/parse_cte/parse_utilcmd)
//! and catalog-side checks (aclchk's per-object-type denial switch,
//! dropcmds' does_not_exist_skipping IF EXISTS ladder). Draining them
//! means GENERATING THE INVALID INPUT each arm rejects — so every probe
//! here is deliberately erroring, and the differential bar is the ERROR
//! IDENTITY: both engines must raise the same SQLSTATE (diff::classify
//! matches errors on SQLSTATE and records message drift in the detail).
//! Error-arm draining is therefore also error-conformance testing.
//!
//! Rules of the module (LD2/nodes discipline, plus error-arm specifics):
//!   - every group is self-contained: fixtures live under fixed `ea_`
//!     names, created and dropped inside the group; erroring probes
//!     cannot create state, so bracket accounting only tracks the
//!     fixtures and the two probes that intentionally SUCCEED and are
//!     dropped in-group (range type ea_r1, collation "C" clone);
//!   - every probe was hand-verified on the A/B pair before landing
//!     (docs/fuzzing/findings-ld6.md): identical SQLSTATE + message on
//!     both engines, except the four banked message-identity findings
//!     (LD6-F1..F3 + the enum-oid DETAIL, all SQLSTATE-matched);
//!   - probes that expose rig asymmetry rather than conformance surface
//!     (CREATE COLLATION provider=icu — the in-lane cpg-ref is built
//!     --without-icu, pgrust carries its own ICU) are EXCLUDED here and
//!     ruled in the findings doc;
//!   - role fixtures are cluster-global: SET ROLE probes always sit
//!     between a CREATE ROLE and a RESET ROLE + DROP ROLE in the same
//!     group, and denial probes never run as a role that could succeed;
//!   - GUC-check probes (SET datestyle/timezone/...) are almost all
//!     failing SETs; the group still ends with RESETs for every GUC a
//!     probe could have set, so the session pin (runner::DATETIME_GUC_PIN)
//!     is never poisoned for later groups.

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "earm:schema",
    "earm:drop",
    "earm:typmod",
    "earm:acl",
    "earm:role",
    "earm:seq",
    "earm:idx",
    "earm:agg",
    "earm:type",
    "earm:coerce",
    "earm:colref",
    "earm:aggplace",
    "earm:srf",
    "earm:cte",
    "earm:altable",
    "earm:fk",
    "earm:stats",
    "earm:guc",
    "earm:opclass",
    "earm:tabdef",
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

/// Pick `k` distinct probes from `pool` (order-preserving) — the per-group
/// sampler for the big stateless probe pools. Every probe stays reachable
/// (all_shapes_fire covers shapes; pool coverage accrues over groups).
fn sample<'a>(g: &mut Gen, pool: &[&'a str], k: usize) -> Vec<&'a str> {
    let k = k.min(pool.len());
    let mut idx: Vec<usize> = (0..pool.len()).collect();
    // Partial Fisher-Yates driven by the session PRNG.
    for i in 0..k {
        let j = i + g.rng.below((idx.len() - i) as u64) as usize;
        idx.swap(i, j);
    }
    let mut picked: Vec<usize> = idx[..k].to_vec();
    picked.sort_unstable();
    picked.iter().map(|&i| pool[i]).collect()
}

/// DROP <objtype> ladder: does_not_exist_skipping / schema-missing arms
/// (IF EXISTS: NOTICE on both sides) + the hard 42704/42P01/42883 arms.
const DROP_PROBES: &[&str] = &[
    "DROP TABLE ea_nosuch;",
    "DROP TABLE IF EXISTS ea_nosuch;",
    "DROP TABLE IF EXISTS ea_noschema.t;",
    "DROP VIEW ea_nosuch;",
    "DROP VIEW IF EXISTS ea_nosuch;",
    "DROP MATERIALIZED VIEW ea_nosuch;",
    "DROP MATERIALIZED VIEW IF EXISTS ea_nosuch;",
    "DROP INDEX ea_nosuch;",
    "DROP INDEX IF EXISTS ea_nosuch;",
    "DROP SEQUENCE ea_nosuch;",
    "DROP SEQUENCE IF EXISTS ea_nosuch;",
    "DROP TYPE ea_nosuch;",
    "DROP TYPE IF EXISTS ea_nosuch;",
    "DROP TYPE IF EXISTS ea_noschema.t;",
    "DROP DOMAIN ea_nosuch;",
    "DROP DOMAIN IF EXISTS ea_nosuch;",
    "DROP COLLATION ea_nosuch;",
    "DROP COLLATION IF EXISTS ea_nosuch;",
    "DROP CONVERSION ea_nosuch;",
    "DROP CONVERSION IF EXISTS ea_nosuch;",
    "DROP SCHEMA ea_nosuch;",
    "DROP SCHEMA IF EXISTS ea_nosuch;",
    "DROP EXTENSION ea_nosuch;",
    "DROP EXTENSION IF EXISTS ea_nosuch;",
    "DROP FUNCTION ea_nosuch(int);",
    "DROP FUNCTION IF EXISTS ea_nosuch(int);",
    "DROP FUNCTION IF EXISTS ea_nosuch(ea_nosuchtype);",
    "DROP PROCEDURE ea_nosuch(int);",
    "DROP PROCEDURE IF EXISTS ea_nosuch(int);",
    "DROP ROUTINE ea_nosuch(int);",
    "DROP ROUTINE IF EXISTS ea_nosuch(int);",
    "DROP AGGREGATE ea_nosuch(int);",
    "DROP AGGREGATE IF EXISTS ea_nosuch(int);",
    "DROP OPERATOR ### (int, int);",
    "DROP OPERATOR IF EXISTS ### (int, int);",
    "DROP OPERATOR IF EXISTS ### (ea_nosuchtype, int);",
    "DROP OPERATOR CLASS ea_nosuch USING btree;",
    "DROP OPERATOR CLASS IF EXISTS ea_nosuch USING btree;",
    "DROP OPERATOR CLASS IF EXISTS ea_nosuch USING ea_nosucham;",
    "DROP OPERATOR FAMILY ea_nosuch USING btree;",
    "DROP OPERATOR FAMILY IF EXISTS ea_nosuch USING btree;",
    "DROP LANGUAGE ea_nosuch;",
    "DROP LANGUAGE IF EXISTS ea_nosuch;",
    "DROP CAST (text AS point);",
    "DROP CAST IF EXISTS (text AS point);",
    "DROP CAST IF EXISTS (ea_nosuchtype AS point);",
    "DROP EVENT TRIGGER ea_nosuch;",
    "DROP EVENT TRIGGER IF EXISTS ea_nosuch;",
    "DROP PUBLICATION ea_nosuch;",
    "DROP PUBLICATION IF EXISTS ea_nosuch;",
    "DROP STATISTICS ea_nosuch;",
    "DROP STATISTICS IF EXISTS ea_nosuch;",
    "DROP TEXT SEARCH PARSER ea_nosuch;",
    "DROP TEXT SEARCH PARSER IF EXISTS ea_nosuch;",
    "DROP TEXT SEARCH DICTIONARY ea_nosuch;",
    "DROP TEXT SEARCH DICTIONARY IF EXISTS ea_nosuch;",
    "DROP TEXT SEARCH TEMPLATE ea_nosuch;",
    "DROP TEXT SEARCH TEMPLATE IF EXISTS ea_nosuch;",
    "DROP TEXT SEARCH CONFIGURATION ea_nosuch;",
    "DROP TEXT SEARCH CONFIGURATION IF EXISTS ea_nosuch;",
    "DROP SERVER ea_nosuch;",
    "DROP SERVER IF EXISTS ea_nosuch;",
    "DROP FOREIGN DATA WRAPPER ea_nosuch;",
    "DROP FOREIGN DATA WRAPPER IF EXISTS ea_nosuch;",
    "DROP FOREIGN TABLE ea_nosuch;",
    "DROP FOREIGN TABLE IF EXISTS ea_nosuch;",
    "DROP ACCESS METHOD ea_nosuch;",
    "DROP ACCESS METHOD IF EXISTS ea_nosuch;",
    "DROP TRANSFORM FOR int LANGUAGE ea_nosuch;",
    "DROP TRANSFORM IF EXISTS FOR int LANGUAGE ea_nosuch;",
    "DROP TRANSFORM IF EXISTS FOR ea_nosuchtype LANGUAGE plpgsql;",
    "DROP DATABASE ea_nosuch;",
    "DROP DATABASE IF EXISTS ea_nosuch;",
    "DROP ROLE ea_nosuch;",
    "DROP ROLE IF EXISTS ea_nosuch;",
    "DROP TABLESPACE ea_nosuch;",
    "DROP TABLESPACE IF EXISTS ea_nosuch;",
    "DROP SUBSCRIPTION IF EXISTS ea_nosuch;",
];

/// namespace.c missing-schema / reserved-schema lookup arms (v2 rider).
/// The pg_temp create/drop pair intentionally succeeds (temp-namespace
/// resolution happy exit the error arms branch around).
const SCHEMA_PROBES: &[&str] = &[
    "CREATE TABLE ea_noschema.t (x int);",
    "CREATE VIEW ea_noschema.v AS SELECT 1;",
    "CREATE SEQUENCE ea_noschema.s;",
    "CREATE TYPE ea_noschema.ty AS (p int);",
    "CREATE FUNCTION ea_noschema.f() RETURNS int LANGUAGE sql AS 'SELECT 1';",
    "CREATE TABLE pg_catalog.ea_bad (x int);",
    "CREATE SCHEMA pg_ea_reserved;",
    "CREATE TEMP TABLE pg_temp.ea_tmpok (x int);",
    "DROP TABLE IF EXISTS pg_temp.ea_tmpok;",
    "SELECT nosuchschema.f();",
    "SELECT pg_catalog.nosuchfunc(1);",
];

/// Bad type modifiers: typenameTypeMod's "not allowed" arm + the per-type
/// typmodin range checks (varchar/bpchar/bit/numeric).
const TYPMOD_PROBES: &[&str] = &[
    "CREATE TABLE ea_bad (v varchar(0));",
    "CREATE TABLE ea_bad (v char(0));",
    "CREATE TABLE ea_bad (v bit(0));",
    "CREATE TABLE ea_bad (v numeric(0));",
    "CREATE TABLE ea_bad (v numeric(1001));",
    "CREATE TABLE ea_bad (v numeric(5,2000));",
    "CREATE TABLE ea_bad (v text(5));",
    "CREATE TABLE ea_bad (v int(4));",
    "CREATE TABLE ea_bad (v varchar(10485761));",
    "SELECT 'x'::varchar(0);",
    "SELECT 1.5::numeric(2000,5);",
    "SELECT '10:00'::time(-1);",
];

/// aclchk per-object-type denial arms, probed as an unprivileged role:
/// no-privilege (42501) and must-be-owner (42501, per-objtype message
/// switch in aclcheck_error). All fixtures are group-local.
const ACL_PROBES: &[&str] = &[
    "SELECT a FROM ea_base;",
    "INSERT INTO ea_base VALUES (99, 'no', 0);",
    "UPDATE ea_base SET b = 'no';",
    "DELETE FROM ea_base;",
    "TRUNCATE ea_base;",
    "LOCK TABLE ea_base IN ACCESS EXCLUSIVE MODE;",
    "SELECT nextval('ea_seq');",
    "SELECT setval('ea_seq', 5);",
    "SELECT ea_f(1);",
    "CREATE TABLE ea_s.deny (z int);",
    "CREATE SCHEMA ea_denied;",
    "ALTER TABLE ea_base ADD COLUMN z int;",
    "ALTER SEQUENCE ea_seq RESTART;",
    "ALTER VIEW ea_v RENAME TO ea_v2;",
    "ALTER MATERIALIZED VIEW ea_mv RENAME TO ea_mv2;",
    "ALTER TYPE ea_comp ADD ATTRIBUTE r int;",
    "ALTER DOMAIN ea_dom ADD CHECK (VALUE < 100);",
    "ALTER FUNCTION ea_f(int) RENAME TO ea_f2;",
    "ALTER SCHEMA ea_s RENAME TO ea_s2;",
    "DROP TABLE ea_base;",
    "DROP VIEW ea_v;",
    "DROP SEQUENCE ea_seq;",
    "DROP TYPE ea_comp;",
    "DROP FUNCTION ea_f(int);",
    "DROP SCHEMA ea_s;",
    "COMMENT ON TABLE ea_base IS 'nope';",
    "COMMENT ON FUNCTION ea_f(int) IS 'nope';",
    "COMMENT ON TYPE ea_comp IS 'nope';",
    "COMMENT ON SCHEMA ea_s IS 'nope';",
    "COMMENT ON SEQUENCE ea_seq IS 'nope';",
    "GRANT SELECT ON ea_base TO ea_aclowner;",
    "CREATE INDEX ea_deny_idx ON ea_base (a);",
    "ALTER ROLE ea_aclowner NOLOGIN;",
    "CREATE ROLE ea_sneak;",
    "DROP ROLE ea_aclowner;",
    // v2 rider: more objtype arms in aclcheck_error / per-command
    // ownership checks (text search, database, event trigger, publication,
    // extension, tablespace, maintenance commands, security labels).
    "ALTER TEXT SEARCH CONFIGURATION ea_tsc ALTER MAPPING FOR word WITH simple;",
    "ALTER TEXT SEARCH DICTIONARY ea_tsd (StopWords = ea);",
    "COMMENT ON TEXT SEARCH CONFIGURATION ea_tsc IS 'no';",
    "COMMENT ON TEXT SEARCH DICTIONARY ea_tsd IS 'no';",
    "COMMENT ON ROLE ea_aclowner IS 'no';",
    "COMMENT ON DATABASE fuzz IS 'no';",
    "CREATE PUBLICATION ea_pub;",
    "CREATE EVENT TRIGGER ea_et ON ddl_command_start EXECUTE FUNCTION ea_f();",
    "CREATE EXTENSION ea_nosuchext;",
    "ALTER DATABASE fuzz SET work_mem = '8MB';",
    "ALTER DATABASE fuzz RENAME TO fuzz2;",
    "CREATE TABLESPACE ea_ts LOCATION '/nonexistent';",
    "VACUUM ea_base;",
    "ANALYZE ea_base;",
    "CLUSTER ea_base;",
    "REINDEX TABLE ea_base;",
    "SECURITY LABEL ON TABLE ea_base IS 'no';",
    "ALTER FUNCTION ea_f(int) OWNER TO ea_aclowner;",
    "ALTER TABLE ea_base OWNER TO ea_aclowner;",
];

/// CreateRole/AlterRole/role-grant validation arms.
const ROLE_PROBES: &[&str] = &[
    "CREATE ROLE ea_aclrole;",
    "CREATE ROLE pg_ea_reserved;",
    "CREATE ROLE \"none\";",
    "ALTER ROLE ea_nosuchrole LOGIN;",
    "ALTER ROLE pg_read_all_data LOGIN;",
    "DROP ROLE current_user;",
    "GRANT ea_aclrole TO ea_aclrole;",
    "GRANT ea_nosuchrole TO ea_aclrole;",
    "GRANT ea_aclrole TO ea_nosuchrole;",
    "SET ROLE ea_nosuchrole;",
    "CREATE ROLE ea_badconn CONNECTION LIMIT -2;",
    "CREATE ROLE ea_dupopt LOGIN LOGIN;",
    "CREATE USER MAPPING FOR ea_nosuchrole SERVER ea_nosuch;",
    "REVOKE ea_aclrole FROM ea_nosuchrole;",
    // v2 rider: user.c breadth (rename guards, per-db settings, VALID
    // UNTIL parsing, DROP/REASSIGN OWNED role resolution).
    "ALTER ROLE ea_aclrole RENAME TO pg_ea_sneak;",
    "ALTER ROLE current_user RENAME TO ea_self;",
    "ALTER ROLE ea_aclrole IN DATABASE ea_nosuchdb SET work_mem = '8MB';",
    "ALTER ROLE ea_aclrole SET nosuch_guc = 1;",
    "ALTER ROLE ea_aclrole SET work_mem = 'bogus';",
    "ALTER ROLE ea_aclrole VALID UNTIL 'notadate';",
    "DROP ROLE ea_aclrole, ea_nosuchrole;",
    "DROP OWNED BY ea_nosuchrole;",
    "REASSIGN OWNED BY ea_nosuchrole TO ea_aclrole;",
    "REASSIGN OWNED BY ea_aclrole TO ea_nosuchrole;",
];

/// sequence.c init_params arms.
const SEQ_PROBES: &[&str] = &[
    "CREATE SEQUENCE ea_s0 INCREMENT 0;",
    "CREATE SEQUENCE ea_s1 MINVALUE 10 MAXVALUE 5;",
    "CREATE SEQUENCE ea_s2 START 100 MAXVALUE 50;",
    "CREATE SEQUENCE ea_s3 START 0 MINVALUE 5;",
    "CREATE SEQUENCE ea_s4 AS text;",
    "CREATE SEQUENCE ea_s5 AS smallint MAXVALUE 100000;",
    "CREATE SEQUENCE ea_s6 AS smallint MINVALUE -100000;",
    "CREATE SEQUENCE ea_s7 CACHE 0;",
    "CREATE SEQUENCE ea_s8 AS numeric;",
    "CREATE TEMP SEQUENCE ea_s9 INCREMENT 0;",
    "ALTER SEQUENCE ea_seq RESTART WITH 0 MINVALUE 1;",
    "ALTER SEQUENCE ea_seq MAXVALUE 5 RESTART WITH 10;",
    "SELECT setval('ea_seq', 0);",
];

/// DefineIndex / ComputeIndexAttrs / transformIndexConstraint arms.
const IDX_PROBES: &[&str] = &[
    "CREATE UNIQUE INDEX ea_bad_idx ON ea_base USING hash (a);",
    "CREATE UNIQUE INDEX ea_bad_idx ON ea_base USING gin (b);",
    "CREATE INDEX ea_bad_idx ON ea_base USING gin (b DESC);",
    "CREATE INDEX ea_bad_idx ON ea_base USING gin (b NULLS FIRST);",
    "CREATE INDEX ea_bad_idx ON ea_base USING brin (a) INCLUDE (b);",
    "CREATE INDEX ea_bad_idx ON ea_base (b ea_nosuch_ops);",
    "CREATE INDEX ea_bad_idx ON ea_base (b int4_ops);",
    "CREATE INDEX ea_bad_idx ON ea_base USING gin (a);",
    "CREATE INDEX ea_bad_idx ON ea_v (a);",
    "CREATE INDEX ea_bad_idx ON ea_seq (last_value);",
    "CREATE INDEX ea_bad_idx ON ea_base (ctid);",
    "CREATE INDEX ea_bad_idx ON ea_base ((generate_series(1, a)));",
    "CREATE INDEX ea_bad_idx ON ea_base (a) WHERE (SELECT true);",
    "CREATE INDEX ea_bad_idx ON ea_base ((random() > 0.5));",
    "CREATE INDEX ea_bad_idx ON ea_base (a COLLATE \"C\");",
    "CREATE UNIQUE INDEX ea_bad_idx ON ea_part (id);",
    "CREATE INDEX CONCURRENTLY ea_bad_idx ON ea_part (id);",
    "CREATE INDEX ea_bad_idx ON ea_base USING ea_nosucham (a);",
    "CREATE INDEX ea_bad_idx ON ea_nosuchtab (a);",
    "ALTER TABLE ea_base ADD CONSTRAINT ea_uniq_dup UNIQUE USING INDEX ea_nosuchidx;",
    "ALTER TABLE ea_part ADD CONSTRAINT ea_excl EXCLUDE USING gist (id WITH =);",
    "ALTER TABLE ea_base ADD UNIQUE (nosuchcol);",
    "ALTER TABLE ea_base ADD PRIMARY KEY (a);",
    // v2 rider: tablespace/option/window-in-index arms + brin/hash limits.
    "CREATE INDEX ea_bad_idx ON ea_base (a) TABLESPACE ea_nosuch;",
    "CREATE INDEX ea_bad_idx ON ea_base (a) INCLUDE (a);",
    "CREATE UNIQUE INDEX ea_bad_idx ON ea_base USING brin (a);",
    "CREATE INDEX ea_bad_idx ON ea_base (a) WITH (bogus_opt = 1);",
    "CREATE INDEX ea_bad_idx ON ea_base (a) WITH (fillfactor = 5);",
    "CREATE INDEX ea_bad_idx ON ea_base ((rank() OVER ()));",
    "CREATE INDEX ea_bad_idx ON ea_base (a) WHERE rank() OVER () = 1;",
    "ALTER TABLE ea_base ADD CONSTRAINT ea_x EXCLUDE USING btree (a WITH <>);",
    "ALTER TABLE ea_part ADD EXCLUDE USING gist (ts WITH &&);",
    "ALTER TABLE ea_base ADD PRIMARY KEY (a) DEFERRABLE NOT DEFERRABLE;",
];

/// Fixed order-sensitive USING INDEX blocks (transformIndexConstraint
/// rejection arms need a real index of the wrong shape first; every
/// helper index is dropped in-block).
const IDX_USING_BLOCK: &[&str] = &[
    "CREATE UNIQUE INDEX ea_uniq_part ON ea_part (id, ts);",
    "ALTER TABLE ea_part ADD PRIMARY KEY USING INDEX ea_uniq_part;",
    "DROP INDEX ea_uniq_part;",
    "CREATE UNIQUE INDEX ea_partial ON ea_base (a) WHERE a > 0;",
    "ALTER TABLE ea_base ADD UNIQUE USING INDEX ea_partial;",
    "DROP INDEX ea_partial;",
    "CREATE UNIQUE INDEX ea_desc ON ea_base (a DESC);",
    "ALTER TABLE ea_base ADD PRIMARY KEY USING INDEX ea_desc;",
    "DROP INDEX ea_desc;",
    "CREATE UNIQUE INDEX ea_nonuniq_helper ON ea_ref (x, y);",
    "ALTER TABLE ea_ref ADD PRIMARY KEY USING INDEX ea_nonuniq_helper;",
    "DROP INDEX ea_nonuniq_helper;",
    "CREATE INDEX ea_expr_helper ON ea_base (((a + 0)));",
    "ALTER TABLE ea_base ADD PRIMARY KEY USING INDEX ea_expr_helper;",
    "DROP INDEX ea_expr_helper;",
];

/// DefineAggregate / AggregateCreate parameter-validation arms.
const AGG_PROBES: &[&str] = &[
    "CREATE AGGREGATE ea_a0 (int) (STYPE = int);",
    "CREATE AGGREGATE ea_a1 (int) (SFUNC = int4pl);",
    "CREATE AGGREGATE ea_a2 (int) (SFUNC = ea_nosuchfn, STYPE = int);",
    "CREATE AGGREGATE ea_a3 (int) (SFUNC = int4pl, STYPE = point);",
    "CREATE AGGREGATE ea_a4 (int) (SFUNC = int4pl, STYPE = int, FINALFUNC = ea_nosuchfn);",
    "CREATE AGGREGATE ea_a5 (int) (SFUNC = int4pl, STYPE = int, SORTOP = ###);",
    "CREATE AGGREGATE ea_a6 (int) (SFUNC = int4pl, STYPE = int, PARALLEL = bogus);",
    "CREATE AGGREGATE ea_a7 (int) (SFUNC = int4pl, STYPE = int, FINALFUNC_MODIFY = bogus);",
    "CREATE AGGREGATE ea_a8 (int) (SFUNC = int4pl, STYPE = int, MSFUNC = int4pl);",
    "CREATE AGGREGATE ea_a9 (int) (SFUNC = int4pl, STYPE = int, MSTYPE = int);",
    "CREATE AGGREGATE ea_a10 (int) (SFUNC = int4pl, STYPE = int, MSTYPE = int, MSFUNC = int4pl);",
    "CREATE AGGREGATE ea_a11 (int) (SFUNC = int4pl, STYPE = int, SERIALFUNC = numeric_avg_serialize);",
    "CREATE AGGREGATE ea_a12 (int) (SFUNC = int4pl, STYPE = int, HYPOTHETICAL);",
    "CREATE AGGREGATE ea_a13 (int) (SFUNC = int4pl, STYPE = int, INITCOND = 'notanint');",
    "CREATE AGGREGATE ea_a14 (*) (SFUNC = int4pl, STYPE = int);",
    "CREATE AGGREGATE ea_a15 (int ORDER BY int) (SFUNC = int4pl, STYPE = int, SORTOP = >);",
    "CREATE AGGREGATE ea_a16 (int) (SFUNC = int4larger, STYPE = int, SORTOP = ea_nosuchop);",
    "CREATE AGGREGATE ea_a17 (text) (SFUNC = textcat, STYPE = text, COMBINEFUNC = int4pl);",
    "CREATE AGGREGATE ea_a18 (int) (SFUNC = int4pl, STYPE = int, MSTYPE = int, MSFUNC = int4pl, MINVFUNC = int4mi, MFINALFUNC = ea_nosuchfn);",
];

/// DefineType / DefineRange / DefineDomain / AlterEnum / typecmds arms.
/// (ea_r1 — range over a composite — intentionally SUCCEEDS on both
/// engines and is dropped by the group tail.)
const TYPE_PROBES: &[&str] = &[
    "CREATE TYPE ea_t1 (INPUT = ea_nosuch_in, OUTPUT = ea_nosuch_out);",
    "CREATE TYPE ea_t2 (INPUT = int4in);",
    "CREATE TYPE ea_t3 (INPUT = int4in, OUTPUT = int4out, INTERNALLENGTH = -3);",
    "CREATE TYPE ea_t4 (INPUT = int4in, OUTPUT = int4out, ALIGNMENT = wonky);",
    "CREATE TYPE ea_t5 (INPUT = int4in, OUTPUT = int4out, STORAGE = bogus);",
    "CREATE TYPE ea_t6 (INPUT = int4in, OUTPUT = int4out, CATEGORY = 'xx');",
    "CREATE TYPE ea_comp;",
    "CREATE TYPE ea_r1 AS RANGE (SUBTYPE = ea_comp);",
    "CREATE TYPE ea_r2 AS RANGE (COLLATION = \"C\");",
    "CREATE TYPE ea_r3 AS RANGE (SUBTYPE = int, COLLATION = \"C\");",
    "CREATE TYPE ea_r4 AS RANGE (SUBTYPE = int, SUBTYPE_OPCLASS = text_ops);",
    "CREATE TYPE ea_r5 AS RANGE (SUBTYPE = int, MULTIRANGE_TYPE_NAME = ea_comp);",
    "CREATE TYPE ea_e2 AS ENUM ('a', 'a');",
    "CREATE DOMAIN ea_d1 AS int NOT NULL NULL;",
    "CREATE DOMAIN ea_d2 AS int DEFAULT 1 DEFAULT 2;",
    "CREATE DOMAIN ea_d3 AS int PRIMARY KEY;",
    "CREATE DOMAIN ea_d4 AS int UNIQUE;",
    "CREATE DOMAIN ea_d5 AS int REFERENCES ea_base;",
    "CREATE DOMAIN ea_d6 AS ea_nosuchtype;",
    "ALTER TYPE ea_enum ADD VALUE 'r';",
    "ALTER TYPE ea_enum ADD VALUE 'x' BEFORE 'nosuch';",
    "ALTER TYPE ea_enum RENAME VALUE 'r' TO 'g';",
    "ALTER TYPE ea_enum RENAME VALUE 'nosuch' TO 'z';",
    "ALTER TYPE ea_comp DROP ATTRIBUTE nosuch;",
    "ALTER TYPE ea_comp DROP ATTRIBUTE IF EXISTS nosuch;",
    "ALTER TYPE ea_enum ADD ATTRIBUTE z int;",
    "ALTER TYPE ea_comp ADD ATTRIBUTE p int;",
    "ALTER DOMAIN ea_dom DROP CONSTRAINT ea_nosuch;",
    "ALTER DOMAIN ea_dom DROP CONSTRAINT IF EXISTS ea_nosuch;",
    "ALTER DOMAIN ea_dom ADD PRIMARY KEY;",
    "ALTER TYPE ea_dom ADD ATTRIBUTE z int;",
    "ALTER TYPE ea_comp RENAME ATTRIBUTE nosuch TO other;",
    // v2 rider: DefineType I/O-function ladder + composite/domain breadth.
    "CREATE TYPE ea_t7 (INPUT = int4in, OUTPUT = int4out, RECEIVE = ea_nosuchrecv);",
    "CREATE TYPE ea_t8 (INPUT = int4in, OUTPUT = int4out, SEND = ea_nosuchsend);",
    "CREATE TYPE ea_t9 (INPUT = int4in, OUTPUT = int4out, TYPMOD_IN = ea_nosuchtmin);",
    "CREATE TYPE ea_t10 (INPUT = int4in, OUTPUT = int4out, ANALYZE = ea_nosuchan);",
    "CREATE TYPE ea_t11 (INPUT = int4in, OUTPUT = int4out, SUBSCRIPT = ea_nosuchsub);",
    "CREATE TYPE ea_t12 (INPUT = int4in, OUTPUT = int4out, ELEMENT = ea_nosuchtype);",
    "CREATE TYPE ea_t13 (INPUT = int4in, OUTPUT = int4out, DELIMITER = 'toolong');",
    "CREATE TYPE ea_t14 (INPUT = int4in, OUTPUT = int4out, LIKE = ea_nosuchtype);",
    "CREATE TYPE ea_t15 (INPUT = int4in, OUTPUT = int4out, INTERNALLENGTH = 100000000000);",
    "CREATE TYPE ea_t16 (INPUT = int4in, OUTPUT = int4out, PASSEDBYVALUE, INTERNALLENGTH = 100);",
    "CREATE TYPE ea_t17 (BOGUSATTR = 1);",
    "CREATE TYPE ea_t18 AS (p int, p int);",
    "CREATE TYPE ea_t19 AS (p int COLLATE ea_nosuchcoll);",
    "ALTER TYPE ea_base ADD ATTRIBUTE z int;",
    "CREATE DOMAIN ea_d7 AS int COLLATE \"C\";",
    "CREATE DOMAIN ea_d8 AS int[] DEFAULT 'notanarray';",
    "CREATE DOMAIN ea_d9 AS anyelement;",
    "ALTER DOMAIN ea_nosuchdom ADD CHECK (VALUE > 0);",
    "ALTER DOMAIN int4 ADD CHECK (VALUE > 0);",
];

/// enforce_generic_type_consistency + ParseFuncOrColumn resolution arms.
const COERCE_PROBES: &[&str] = &[
    "SELECT ea_any(1, 'x'::text);",
    "SELECT ea_any('a', 'b');",
    "SELECT ea_anyarr(ARRAY[1], 'x'::text);",
    "SELECT ea_anyarr(1, 2);",
    "SELECT ea_anyarr('{1}', '{2}');",
    "SELECT array_append(ARRAY[1], 'x'::text);",
    "SELECT array_cat(ARRAY[1], ARRAY['x'::text]);",
    "SELECT ea_f('nope');",
    "SELECT ea_f(1, 2);",
    "SELECT ea_nosuchfn(1);",
    "SELECT nosuchagg(a) FROM ea_base;",
    // v2 rider: the anycompatible/anyrange/anyenum resolution arms.
    "SELECT ea_ac(1, point '(1,1)');",
    "SELECT ea_ac('a', 'b');",
    "SELECT ea_acarr(ARRAY[1], point '(1,1)');",
    "SELECT ea_acarr('{1}', '{2}');",
    "SELECT ea_acarr(1, 2);",
    "SELECT ea_ar(int4range(1,2), 'x'::text);",
    "SELECT ea_ar('x', 'y');",
    "SELECT ea_ae(1);",
    "SELECT ea_ae('r');",
];

/// transformColumnRef / expandRTE / field-selection arms.
const COLREF_PROBES: &[&str] = &[
    "SELECT nosuchcol FROM ea_base;",
    "SELECT ea_base.nosuchcol FROM ea_base;",
    "SELECT missingalias.a FROM ea_base;",
    "SELECT missingtab.* FROM ea_base;",
    "SELECT a FROM ea_base e1, ea_base e2;",
    "SELECT * FROM ea_base, ea_base;",
    "SELECT ea_base.b FROM ea_base e1;",
    "SELECT e.a.b FROM ea_base e;",
    "SELECT * FROM ea_rec();",
    "SELECT * FROM ea_rec() AS t(r1 int, r2 text, r3 int);",
    "SELECT * FROM ea_f(1) AS t(f1 int, f2 int);",
    "SELECT * FROM generate_series(1, 3) AS g(a int, b int);",
    "SELECT (e).nosuchfield FROM ea_base e;",
    "SELECT (ROW(1,2)).nosuchfield;",
];

/// check_agglevels_and_constraints / transformWindowFuncCall placement arms.
const AGGPLACE_PROBES: &[&str] = &[
    "SELECT count(*) FROM ea_base WHERE count(a) > 0;",
    "SELECT a FROM ea_base GROUP BY count(a);",
    "SELECT count(a) FROM ea_base GROUP BY 1;",
    "SELECT sum(count(a)) FROM ea_base;",
    "SELECT a FROM ea_base HAVING a > 0;",
    "CREATE TABLE ea_bad (x int CHECK (sum(x) > 0));",
    "CREATE INDEX ea_bad_idx ON ea_base ((sum(a)));",
    "UPDATE ea_base SET a = count(*);",
    "DELETE FROM ea_base WHERE sum(a) > 0;",
    "INSERT INTO ea_ref VALUES (count(1), 2);",
    "SELECT generate_series(1, count(a)) FROM ea_base;",
    "SELECT count(rank() OVER ()) FROM ea_base;",
    "SELECT rank() OVER (ORDER BY count(a)) FROM ea_base;",
    "SELECT a, rank() OVER () FROM ea_base WHERE rank() OVER () = 1;",
    "SELECT a FROM ea_base GROUP BY rank() OVER ();",
    "SELECT rank() OVER (), sum(rank() OVER ()) FROM ea_base;",
    "SELECT lag(a) FROM ea_base;",
    "SELECT count(*) FILTER (WHERE rank() OVER () = 1) FROM ea_base;",
    "SELECT rank(a) WITHIN GROUP (ORDER BY a) OVER () FROM ea_base;",
];

/// check_srf_call_placement arms.
const SRF_PROBES: &[&str] = &[
    "SELECT 1 FROM ea_base WHERE generate_series(1, 2) = 1;",
    "SELECT CASE WHEN a > 0 THEN generate_series(1, 2) END FROM ea_base;",
    "SELECT COALESCE(generate_series(1, 2), a) FROM ea_base;",
    "UPDATE ea_base SET a = generate_series(1, 2);",
    "SELECT 1 LIMIT generate_series(1, 2);",
    "SELECT 1 OFFSET generate_series(1, 2);",
    "SELECT sum(generate_series(1, 2)) OVER () FROM ea_base;",
    "SELECT count(generate_series(1, 2)) FROM ea_base;",
    "SELECT a FROM ea_base HAVING generate_series(1, 2) > 0;",
    "VALUES (generate_series(1, 2));",
    "SELECT generate_series(1, generate_series(1, 2));",
    "INSERT INTO ea_ref VALUES (1, 2) RETURNING generate_series(1, x);",
];

/// parse_cte checkWellFormedRecursionWalker / SEARCH-CYCLE arms. All
/// stateless (no fixture); the one legal LATERAL form is bounded.
const CTE_PROBES: &[&str] = &[
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE EXISTS (SELECT * FROM r)) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n FROM r EXCEPT SELECT n FROM r) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT r1.n FROM r r1, r r2) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT b.n + 1 FROM r b LEFT JOIN (SELECT 1 AS m) q ON true WHERE b.n < 3) SELECT count(*) FROM r;",
    "WITH RECURSIVE r AS (SELECT n FROM r UNION ALL SELECT 1) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n INTERSECT SELECT n FROM r) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT sum(n) FROM r) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT sum(n)::int FROM r) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n FROM r ORDER BY n) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n FROM r LIMIT 5) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n FROM r FOR UPDATE) SELECT * FROM r;",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT lateral_ref.n + 1 FROM LATERAL (SELECT n FROM r) lateral_ref WHERE lateral_ref.n < 3) SELECT count(*) FROM r;",
    "WITH r AS (SELECT 1 AS n), r AS (SELECT 2 AS n) SELECT * FROM r;",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SEARCH DEPTH FIRST BY nosuch SET pathcol SELECT * FROM r;",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) CYCLE nosuch SET is_cycle USING path SELECT * FROM r;",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) CYCLE n SET is_cycle TO 1 DEFAULT 0 USING n SELECT * FROM r;",
];

/// ALTER TABLE ladder: ATPrepCmd/ATExecCmd wrong-relkind + per-subcommand
/// validation, MergeAttributes/inheritance, partition attach/detach.
/// (The ADD GENERATED / DROP IDENTITY pair intentionally succeeds and
/// self-reverts; SET WITHOUT OIDS / FORCE RLS succeed on a group-local
/// table that the tail drops.)
const ALTABLE_PROBES: &[&str] = &[
    "ALTER TABLE ea_v ADD COLUMN z int;",
    "ALTER TABLE ea_seq ADD COLUMN z int;",
    "ALTER TABLE ea_mv ADD COLUMN z int;",
    "ALTER TABLE ea_nosuchtab ADD COLUMN z int;",
    "ALTER TABLE IF EXISTS ea_nosuchtab ADD COLUMN z int;",
    "ALTER TABLE ea_base DROP COLUMN nosuch;",
    "ALTER TABLE ea_base DROP COLUMN IF EXISTS nosuch;",
    "ALTER TABLE ea_base ADD COLUMN a int;",
    "ALTER TABLE ea_base ADD COLUMN IF NOT EXISTS a int;",
    "ALTER TABLE ea_base ALTER COLUMN a TYPE point;",
    "ALTER TABLE ea_base ALTER COLUMN a TYPE varchar(0);",
    "ALTER TABLE ea_base ALTER COLUMN nosuch TYPE int;",
    "ALTER TABLE ea_base ALTER COLUMN a SET DEFAULT random() * nosuchcol;",
    "ALTER TABLE ea_ref ALTER COLUMN x SET NOT NULL;",
    "ALTER TABLE ea_ref ADD CONSTRAINT ea_chk CHECK (x > 100);",
    "ALTER TABLE ea_dup ADD CONSTRAINT ea_uni UNIQUE (d);",
    "ALTER TABLE ea_base DROP CONSTRAINT nosuch;",
    "ALTER TABLE ea_base DROP CONSTRAINT IF EXISTS nosuch;",
    "ALTER TABLE ea_base INHERIT ea_base;",
    "ALTER TABLE ea_base INHERIT ea_v;",
    "ALTER TABLE ea_base INHERIT ea_part;",
    "ALTER TABLE ea_ref INHERIT ea_base;",
    "CREATE TABLE ea_child (a text) INHERITS (ea_base);",
    "CREATE TABLE ea_child () INHERITS (ea_v);",
    "CREATE TABLE ea_child () INHERITS (ea_part);",
    "CREATE TABLE ea_child () INHERITS (ea_base, ea_base);",
    "ALTER TABLE ea_base ALTER COLUMN b ADD GENERATED ALWAYS AS IDENTITY;",
    "ALTER TABLE ea_base ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;",
    "ALTER TABLE ea_base ALTER COLUMN a DROP IDENTITY;",
    "ALTER TABLE ea_base ALTER COLUMN a DROP IDENTITY IF EXISTS;",
    "ALTER TABLE ea_base ALTER COLUMN a DROP EXPRESSION;",
    "ALTER TABLE ea_base ALTER COLUMN a DROP EXPRESSION IF EXISTS;",
    "ALTER TABLE ea_base SET (fillfactor = 0);",
    "ALTER TABLE ea_base SET (ea_bogus_option = 1);",
    "ALTER TABLE ea_base SET TABLESPACE ea_nosuch;",
    "ALTER TABLE ea_base SET SCHEMA ea_nosuchschema;",
    "ALTER TABLE ea_base ALTER COLUMN a SET STATISTICS 20000;",
    "ALTER TABLE ea_base ALTER COLUMN a SET STATISTICS -5;",
    "ALTER TABLE ea_base ALTER COLUMN b SET STORAGE bogus;",
    "ALTER TABLE ea_base ALTER COLUMN a SET COMPRESSION bogus;",
    "ALTER TABLE ea_base OF ea_comp;",
    "ALTER TABLE ea_base NOT OF;",
    "ALTER TABLE ea_part ALTER COLUMN ts TYPE text;",
    "ALTER TABLE ea_part DROP COLUMN ts;",
    "ALTER TABLE ONLY ea_part ADD COLUMN extra int;",
    "ALTER TABLE ea_part ATTACH PARTITION ea_ref FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');",
    "ALTER TABLE ea_part ATTACH PARTITION ea_v FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');",
    "ALTER TABLE ea_part ATTACH PARTITION ea_p1 FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');",
    "ALTER TABLE ea_part DETACH PARTITION ea_nosuchpart;",
    "ALTER TABLE ea_base DETACH PARTITION ea_ref;",
    "ALTER TABLE ea_part ATTACH PARTITION ea_part FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');",
    "ALTER TABLE ea_p1 RENAME TO ea_p1;",
    "ALTER TABLE ea_base RENAME COLUMN a TO b;",
    "ALTER TABLE ea_base RENAME COLUMN nosuch TO z;",
    "ALTER TABLE ea_base RENAME CONSTRAINT nosuch TO z;",
    "ALTER TABLE ea_base REPLICA IDENTITY USING INDEX ea_nosuchidx;",
    "ALTER TABLE ea_base CLUSTER ON ea_nosuchidx;",
    "ALTER TABLE ea_base SET WITHOUT OIDS;",
    "ALTER TABLE ea_base ENABLE TRIGGER ea_nosuchtrig;",
    "ALTER TABLE ea_base DISABLE TRIGGER ea_nosuchtrig;",
    "ALTER TABLE ea_base VALIDATE CONSTRAINT ea_nosuchcon;",
    "ALTER TABLE ea_base FORCE ROW LEVEL SECURITY;",
    "ALTER TABLE ea_v ENABLE ROW LEVEL SECURITY;",
    // v2 rider: access-method/reloption/attoption/partition-column arms +
    // relkind-mismatch ALTER forms (with and without IF EXISTS).
    "ALTER TABLE ea_base SET ACCESS METHOD ea_nosucham;",
    "ALTER TABLE ea_base OWNER TO ea_nosuchrole;",
    "ALTER TABLE ea_base SET (toast.bogus = 1);",
    "ALTER TABLE ea_base ALTER COLUMN a SET (n_distinct = 'x');",
    "ALTER TABLE ea_base ALTER COLUMN a SET (bogus_attopt = 1);",
    "ALTER TABLE ea_base ADD COLUMN q ea_nosuchtype;",
    "ALTER TABLE ea_p1 ADD COLUMN q int;",
    "ALTER TABLE ea_p1 DROP COLUMN id;",
    "ALTER TABLE ea_p1 ALTER COLUMN id TYPE bigint;",
    "ALTER TABLE ea_base ALTER CONSTRAINT nosuchcon DEFERRABLE;",
    "ALTER INDEX ea_nosuchidx SET (fillfactor = 50);",
    "ALTER INDEX ALL IN TABLESPACE ea_nosuch SET TABLESPACE pg_default;",
    "ALTER MATERIALIZED VIEW ea_nosuchmv SET SCHEMA public;",
    "ALTER VIEW ea_nosuchview RENAME TO x;",
    "ALTER SEQUENCE ea_nosuchseq RESTART;",
    "ALTER SEQUENCE IF EXISTS ea_nosuchseq RESTART;",
    "ALTER VIEW IF EXISTS ea_nosuchview RENAME TO x;",
    "ALTER TABLE ea_base ALTER COLUMN a TYPE int USING nosuchcol + 1;",
    "ALTER TABLE ea_base ALTER COLUMN a TYPE int USING sum(a);",
    "ALTER TABLE ea_base ALTER COLUMN a TYPE int USING (SELECT 1);",
    "TRUNCATE ea_nosuchtab;",
    "TRUNCATE ONLY ea_part;",
    "COMMENT ON COLUMN ea_base.nosuchcol IS 'x';",
    "COMMENT ON CONSTRAINT nosuchcon ON ea_base IS 'x';",
];

/// ATAddForeignKeyConstraint arms (the add/drop pair in the middle
/// intentionally succeeds — transformFkeyCheckAttrs happy exit feeds the
/// surrounding error arms' branch coverage).
const FK_PROBES: &[&str] = &[
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_nosuchtab;",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_base (b);",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_v;",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (nosuchcol) REFERENCES ea_base;",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_base (nosuchcol);",
    "ALTER TABLE ea_reftext ADD FOREIGN KEY (tx) REFERENCES ea_base (a);",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x, y) REFERENCES ea_base (a);",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_base (a) MATCH PARTIAL;",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_base (a) ON DELETE SET NULL (y);",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_base (a) ON DELETE SET DEFAULT (nosuchcol);",
    "ALTER TABLE ea_ref ADD CONSTRAINT ea_fk_ok FOREIGN KEY (x) REFERENCES ea_base (a);",
    "ALTER TABLE ea_ref DROP CONSTRAINT ea_fk_ok;",
    "ALTER TABLE ea_ref ADD FOREIGN KEY (x) REFERENCES ea_seq;",
];

/// CreateStatistics arms (the first ea_st create succeeds — the 42710
/// duplicate arm needs it; the tail drops it).
const STATS_PROBES: &[&str] = &[
    "CREATE STATISTICS ea_stbad ON a FROM ea_base;",
    "CREATE STATISTICS ea_stbad ON a, a FROM ea_base;",
    "CREATE STATISTICS ea_stbad ON a, nosuch FROM ea_base;",
    "CREATE STATISTICS ea_stbad ON a, b FROM ea_base, ea_ref;",
    "CREATE STATISTICS ea_stbad ON ctid, a FROM ea_base;",
    "CREATE STATISTICS ea_stbad (bogus_kind) ON a, b FROM ea_base;",
    "CREATE STATISTICS ea_stbad ON a, b FROM ea_v;",
    "CREATE STATISTICS ea_stbad ON a, b FROM ea_nosuchtab;",
    "CREATE STATISTICS ea_st ON x, y FROM ea_ref;",
    "CREATE STATISTICS ea_st ON x, y FROM ea_ref;",
    "CREATE STATISTICS IF NOT EXISTS ea_st ON x, y FROM ea_ref;",
];

/// DefineCollation + GUC check-hook arms (check_datestyle/check_timezone/
/// assign paths in variable.c). provider=icu is EXCLUDED: the in-lane
/// cpg-ref is built --without-icu, so that probe measures the rig, not
/// the engine (ruled in findings-ld6.md). The "C"-clone create succeeds
/// and is dropped in the tail; all failing SETs leave no state, and the
/// tail RESETs every GUC a probe touches anyway.
const GUC_PROBES: &[&str] = &[
    "CREATE COLLATION ea_c1 (lc_collate = 'C');",
    "CREATE COLLATION ea_c2 ();",
    "CREATE COLLATION ea_c3 (locale = 'C', lc_collate = 'C');",
    "CREATE COLLATION ea_c4 (provider = 'bogus', locale = 'C');",
    "CREATE COLLATION ea_c6 FROM ea_nosuchcoll;",
    "CREATE COLLATION ea_c7 (locale = 'C', deterministic = false);",
    "CREATE COLLATION \"C\" FROM \"C\";",
    "SET datestyle = 'bogus';",
    "SET datestyle = 'ISO, MDY, DMY';",
    "SET datestyle = 'SQL, ISO';",
    "SET timezone = 'Bogus/Zone';",
    "SET TIME ZONE INTERVAL '25:00' HOUR TO MINUTE;",
    "SET client_encoding = 'BOGUS_ENC';",
    "SET transaction_isolation = 'bogus';",
    "SET seq_page_cost = -1;",
    "SET default_transaction_isolation = 'nope';",
    "SET search_path = ea_nosuchschema;",
    "SELECT set_config('datestyle', 'nonsense', false);",
];

/// opclasscmds arms (invalid operator/function numbers, missing support,
/// bad storage, system-family duplicate).
const OPCLASS_PROBES: &[&str] = &[
    "CREATE OPERATOR CLASS ea_oc1 FOR TYPE int USING btree AS OPERATOR 0 = (int, int);",
    "CREATE OPERATOR CLASS ea_oc2 FOR TYPE int USING btree AS OPERATOR 1 = (int, int), FUNCTION 99 btint4cmp(int, int);",
    "CREATE OPERATOR CLASS ea_oc3 FOR TYPE int USING btree AS OPERATOR 1 ea_nosuchop (int, int);",
    "CREATE OPERATOR CLASS ea_oc4 FOR TYPE int USING btree AS FUNCTION 1 ea_nosuchfn(int, int);",
    "CREATE OPERATOR CLASS ea_oc5 FOR TYPE int USING btree AS OPERATOR 1 = (int, int), FUNCTION 1 btint4cmp(int, int), STORAGE text;",
    "CREATE OPERATOR CLASS ea_oc6 FOR TYPE int USING ea_nosucham AS OPERATOR 1 = (int, int);",
    "CREATE OPERATOR CLASS ea_oc7 FOR TYPE ea_nosuchtype USING btree AS OPERATOR 1 = (int, int);",
    "CREATE OPERATOR FAMILY ea_of1 USING ea_nosucham;",
    "CREATE OPERATOR FAMILY pg_catalog.integer_ops USING btree;",
    "ALTER OPERATOR FAMILY ea_nosuchfam USING btree ADD OPERATOR 1 = (int, int);",
    "ALTER OPERATOR CLASS ea_nosuchoc USING btree RENAME TO ea_x;",
];

/// transformColumnDefinition / CREATE TABLE definition + partition-bound
/// validation arms.
const TABDEF_PROBES: &[&str] = &[
    "CREATE TABLE ea_bad (x int DEFAULT 1 DEFAULT 2);",
    "CREATE TABLE ea_bad (x int NULL NOT NULL);",
    "CREATE TABLE ea_bad (x serial DEFAULT 5);",
    "CREATE TABLE ea_bad (x int GENERATED ALWAYS AS (1) STORED DEFAULT 5);",
    "CREATE TABLE ea_bad (x int GENERATED ALWAYS AS IDENTITY GENERATED ALWAYS AS IDENTITY);",
    "CREATE TABLE ea_bad (x int GENERATED ALWAYS AS (1) STORED GENERATED ALWAYS AS (2) STORED);",
    "CREATE TABLE ea_bad (x text GENERATED ALWAYS AS IDENTITY);",
    "CREATE TABLE ea_bad (x int, PRIMARY KEY (x), PRIMARY KEY (x));",
    "CREATE TABLE ea_bad (x int CHECK (x > 0) DEFERRABLE);",
    "CREATE TABLE ea_bad (x int, y int GENERATED ALWAYS AS (z + 1) STORED);",
    "CREATE TABLE ea_bad (x int, y int GENERATED ALWAYS AS (random()::int) STORED);",
    "CREATE TABLE ea_bad (x int, y int GENERATED ALWAYS AS ((SELECT 1)) STORED);",
    "CREATE TABLE ea_bad (x int, y int GENERATED ALWAYS AS (sum(x)) STORED);",
    "CREATE TABLE ea_bad (x ea_nosuchtype);",
    "CREATE TABLE ea_bad (x int COLLATE \"C\");",
    "CREATE TABLE ea_bad (x text COLLATE ea_nosuchcoll);",
    "CREATE TEMP TABLE public.ea_bad (x int);",
    "CREATE TABLE ea_typed OF ea_enum;",
    "CREATE TABLE ea_typed OF ea_dom;",
    "CREATE TABLE ea_bad (LIKE ea_seq);",
    "CREATE TABLE ea_bad (LIKE ea_nosuchtab);",
    "CREATE TABLE ea_bad (LIKE ea_v INCLUDING INDEXES);",
    "CREATE TABLE ea_bad (x int, LIKE ea_bad);",
    "CREATE TABLE ea_base (a int);",
    "CREATE TABLE IF NOT EXISTS ea_base (a int);",
    "CREATE TABLE ea_badpart (id int) PARTITION BY bogus (id);",
    "CREATE TABLE ea_badpart (id int) PARTITION BY RANGE (nosuchcol);",
    "CREATE TABLE ea_badpart (id int) PARTITION BY RANGE (ctid);",
    "CREATE TABLE ea_badpart (id int) PARTITION BY RANGE ((random()));",
    "CREATE TABLE ea_badpart (id int, t text) PARTITION BY RANGE (t COLLATE ea_nosuchcoll);",
    "CREATE TABLE ea_badp PARTITION OF ea_nosuchtab FOR VALUES FROM (1) TO (2);",
    "CREATE TABLE ea_badp PARTITION OF ea_base FOR VALUES FROM (1) TO (2);",
    "CREATE TABLE ea_badp PARTITION OF ea_part FOR VALUES FROM ('2020-06-01') TO ('2020-03-01');",
    "CREATE TABLE ea_badp PARTITION OF ea_part FOR VALUES FROM ('2020-06-01') TO ('2020-09-01');",
    "CREATE TABLE ea_badp PARTITION OF ea_part FOR VALUES IN (5);",
    "CREATE TABLE ea_badp PARTITION OF ea_part FOR VALUES WITH (MODULUS 4, REMAINDER 1);",
];

// ------------------------------------------------------------ fixtures ----

/// The base-table fixture pair used by most probe families.
fn fx_tables() -> Vec<StmtKind> {
    raws(&[
        "CREATE TABLE ea_base (a int PRIMARY KEY, b text, c numeric(10,2));",
        "INSERT INTO ea_base VALUES (1, 'one', 1.10), (2, 'two', 2.20);",
        "CREATE TABLE ea_ref (x int, y int);",
        "INSERT INTO ea_ref VALUES (1, 1), (NULL, 2), (1, 3);",
    ])
}

fn fx_tables_drop() -> Vec<StmtKind> {
    raws(&["DROP TABLE ea_ref;", "DROP TABLE ea_base;"])
}

/// The partitioned-parent + relkind-menagerie fixture for idx/altable.
fn fx_kinds() -> Vec<StmtKind> {
    raws(&[
        "CREATE VIEW ea_v AS SELECT a, b FROM ea_base;",
        "CREATE MATERIALIZED VIEW ea_mv AS SELECT 1 AS m;",
        "CREATE SEQUENCE ea_seq;",
        "CREATE TABLE ea_part (id int, ts date) PARTITION BY RANGE (ts);",
        "CREATE TABLE ea_p1 PARTITION OF ea_part FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');",
    ])
}

fn fx_kinds_drop() -> Vec<StmtKind> {
    raws(&[
        "DROP TABLE ea_part;",
        "DROP SEQUENCE ea_seq;",
        "DROP MATERIALIZED VIEW ea_mv;",
        "DROP VIEW ea_v;",
    ])
}

fn body(g: &mut Gen, shape: &str) -> Vec<StmtKind> {
    match shape {
        // Whole pool every time: the pg_temp create/drop pair inside is
        // order-sensitive, and the pool is small.
        "earm:schema" => raws(SCHEMA_PROBES),
        "earm:drop" => sample(g, DROP_PROBES, 14).into_iter().map(raw).collect(),
        "earm:typmod" => sample(g, TYPMOD_PROBES, 6).into_iter().map(raw).collect(),
        "earm:acl" => {
            // Unprivileged-role denial bracket. Objects the probes are
            // denied ON are group-local; the role pair is created first
            // and dropped last, and the RESET ROLE always runs because
            // erroring probes cannot terminate the group.
            let mut v = fx_tables();
            v.extend(raws(&[
                "CREATE VIEW ea_v AS SELECT a, b FROM ea_base;",
                "CREATE MATERIALIZED VIEW ea_mv AS SELECT 1 AS m;",
                "CREATE SEQUENCE ea_seq;",
                "CREATE TYPE ea_comp AS (p int, q text);",
                "CREATE DOMAIN ea_dom AS int CHECK (VALUE > 0);",
                "CREATE SCHEMA ea_s;",
                "REVOKE ALL ON SCHEMA ea_s FROM PUBLIC;",
                "CREATE FUNCTION ea_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1';",
                "REVOKE ALL ON FUNCTION ea_f(int) FROM PUBLIC;",
                "REVOKE ALL ON ea_base, ea_seq FROM PUBLIC;",
                "CREATE TEXT SEARCH CONFIGURATION ea_tsc (COPY = simple);",
                "CREATE TEXT SEARCH DICTIONARY ea_tsd (TEMPLATE = simple);",
                "CREATE ROLE ea_aclnobody;",
                "CREATE ROLE ea_aclowner;",
                "SET ROLE ea_aclnobody;",
            ]));
            v.extend(sample(g, ACL_PROBES, 12).into_iter().map(raw));
            v.extend(raws(&[
                "RESET ROLE;",
                "DROP ROLE ea_aclnobody;",
                "DROP ROLE ea_aclowner;",
                "DROP TEXT SEARCH DICTIONARY ea_tsd;",
                "DROP TEXT SEARCH CONFIGURATION ea_tsc;",
                "DROP FUNCTION ea_f(int);",
                "DROP SCHEMA ea_s;",
                "DROP DOMAIN ea_dom;",
                "DROP TYPE ea_comp;",
                "DROP SEQUENCE ea_seq;",
                "DROP MATERIALIZED VIEW ea_mv;",
                "DROP VIEW ea_v;",
            ]));
            v.extend(fx_tables_drop());
            v
        }
        "earm:role" => {
            let mut v = raws(&["CREATE ROLE ea_aclrole;"]);
            // The CREATE above makes the duplicate/self-grant probes
            // deterministic; skip the pool's own copy of the create.
            v.extend(
                sample(g, &ROLE_PROBES[1..], 10).into_iter().map(raw),
            );
            // Fixed membership block (order-sensitive: the circular grant
            // needs the first grant in place; DROP ROLE clears the
            // memberships either way).
            v.extend(raws(&[
                "CREATE ROLE ea_aclmem;",
                "GRANT ea_aclrole TO ea_aclmem WITH ADMIN OPTION;",
                "GRANT ea_aclmem TO ea_aclrole;",
                "REVOKE ADMIN OPTION FOR ea_aclrole FROM ea_aclmem;",
                "REVOKE ea_aclrole FROM ea_aclmem;",
                "DROP ROLE ea_aclmem;",
            ]));
            v.push(raw("DROP ROLE ea_aclrole;"));
            v
        }
        "earm:seq" => {
            let mut v = raws(&["CREATE SEQUENCE ea_seq;"]);
            v.extend(sample(g, SEQ_PROBES, 8).into_iter().map(raw));
            v.push(raw("DROP SEQUENCE ea_seq;"));
            v
        }
        "earm:idx" => {
            let mut v = fx_tables();
            v.extend(fx_kinds());
            v.extend(sample(g, IDX_PROBES, 12).into_iter().map(raw));
            if g.rng.below(2) == 0 {
                v.extend(raws(IDX_USING_BLOCK));
            }
            v.extend(fx_kinds_drop());
            v.extend(fx_tables_drop());
            v
        }
        "earm:agg" => sample(g, AGG_PROBES, 10).into_iter().map(raw).collect(),
        "earm:type" => {
            let mut v = fx_tables();
            v.extend(raws(&[
                "CREATE TYPE ea_comp AS (p int, q text);",
                "CREATE TYPE ea_enum AS ENUM ('r','g','b');",
                "CREATE DOMAIN ea_dom AS int CHECK (VALUE > 0);",
            ]));
            v.extend(sample(g, TYPE_PROBES, 12).into_iter().map(raw));
            v.extend(raws(&[
                // ea_r1 (range over composite) legitimately succeeds when
                // its probe fired; unconditional IF EXISTS keeps the
                // bracket balanced either way.
                "DROP TYPE IF EXISTS ea_r1;",
                "DROP DOMAIN ea_dom;",
                "DROP TYPE ea_enum;",
                "DROP TYPE ea_comp;",
            ]));
            v.extend(fx_tables_drop());
            v
        }
        "earm:coerce" => {
            let mut v = fx_tables();
            v.extend(raws(&[
                "CREATE FUNCTION ea_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1';",
                "CREATE FUNCTION ea_any(x anyelement, y anyelement) RETURNS anyelement LANGUAGE sql AS 'SELECT $1';",
                "CREATE FUNCTION ea_anyarr(x anyarray, y anyelement) RETURNS anyelement LANGUAGE sql AS 'SELECT $2';",
                "CREATE FUNCTION ea_ac(x anycompatible, y anycompatible) RETURNS anycompatible LANGUAGE sql AS 'SELECT $1';",
                "CREATE FUNCTION ea_acarr(x anycompatiblearray, y anycompatible) RETURNS anycompatible LANGUAGE sql AS 'SELECT $2';",
                "CREATE FUNCTION ea_ar(r anyrange, e anyelement) RETURNS anyelement LANGUAGE sql AS 'SELECT $2';",
                "CREATE FUNCTION ea_ae(e anyenum) RETURNS anyenum LANGUAGE sql AS 'SELECT $1';",
            ]));
            v.extend(sample(g, COERCE_PROBES, 10).into_iter().map(raw));
            v.extend(raws(&[
                "DROP FUNCTION ea_ae(anyenum);",
                "DROP FUNCTION ea_ar(anyrange, anyelement);",
                "DROP FUNCTION ea_acarr(anycompatiblearray, anycompatible);",
                "DROP FUNCTION ea_ac(anycompatible, anycompatible);",
                "DROP FUNCTION ea_anyarr(anyarray, anyelement);",
                "DROP FUNCTION ea_any(anyelement, anyelement);",
                "DROP FUNCTION ea_f(int);",
            ]));
            v.extend(fx_tables_drop());
            v
        }
        "earm:colref" => {
            let mut v = fx_tables();
            v.extend(raws(&[
                "CREATE FUNCTION ea_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1';",
                "CREATE FUNCTION ea_rec() RETURNS record LANGUAGE sql AS $$SELECT 1 AS r1, 'x'::text AS r2$$;",
            ]));
            v.extend(sample(g, COLREF_PROBES, 9).into_iter().map(raw));
            v.extend(raws(&["DROP FUNCTION ea_rec();", "DROP FUNCTION ea_f(int);"]));
            v.extend(fx_tables_drop());
            v
        }
        "earm:aggplace" => {
            let mut v = fx_tables();
            v.extend(sample(g, AGGPLACE_PROBES, 10).into_iter().map(raw));
            v.extend(fx_tables_drop());
            v
        }
        "earm:srf" => {
            let mut v = fx_tables();
            v.extend(sample(g, SRF_PROBES, 8).into_iter().map(raw));
            v.extend(fx_tables_drop());
            v
        }
        "earm:cte" => sample(g, CTE_PROBES, 8).into_iter().map(raw).collect(),
        "earm:altable" => {
            let mut v = fx_tables();
            v.extend(raws(&[
                "CREATE TABLE ea_dup (d int);",
                "INSERT INTO ea_dup VALUES (1), (1);",
                "CREATE TYPE ea_comp AS (p int, q text);",
            ]));
            v.extend(fx_kinds());
            v.extend(sample(g, ALTABLE_PROBES, 14).into_iter().map(raw));
            v.extend(fx_kinds_drop());
            v.extend(raws(&["DROP TYPE ea_comp;", "DROP TABLE ea_dup;"]));
            v.extend(fx_tables_drop());
            v
        }
        "earm:fk" => {
            let mut v = fx_tables();
            v.extend(raws(&[
                "CREATE TABLE ea_reftext (tx text);",
                "CREATE VIEW ea_v AS SELECT a, b FROM ea_base;",
                "CREATE SEQUENCE ea_seq;",
            ]));
            v.extend(sample(g, FK_PROBES, 8).into_iter().map(raw));
            v.extend(raws(&[
                // The intentional-success FK may or may not have fired;
                // IF-EXISTS-free DROPs stay on the fixture objects only.
                "ALTER TABLE ea_ref DROP CONSTRAINT IF EXISTS ea_fk_ok;",
                "DROP SEQUENCE ea_seq;",
                "DROP VIEW ea_v;",
                "DROP TABLE ea_reftext;",
            ]));
            v.extend(fx_tables_drop());
            v
        }
        "earm:stats" => {
            let mut v = fx_tables();
            v.push(raw("CREATE VIEW ea_v AS SELECT a, b FROM ea_base;"));
            v.extend(sample(g, STATS_PROBES, 8).into_iter().map(raw));
            v.extend(raws(&[
                "DROP STATISTICS IF EXISTS ea_st;",
                "DROP VIEW ea_v;",
            ]));
            v.extend(fx_tables_drop());
            v
        }
        "earm:guc" => {
            let mut v: Vec<StmtKind> =
                sample(g, GUC_PROBES, 9).into_iter().map(raw).collect();
            v.extend(raws(&[
                // The "C" clone succeeds when its probe fired.
                "DROP COLLATION IF EXISTS \"C\";",
                // Failing SETs leave no state; RESET anyway so the
                // datetime GUC pin can never be poisoned for later groups.
                "RESET datestyle;",
                "RESET timezone;",
                "RESET client_encoding;",
                "RESET search_path;",
                "RESET seq_page_cost;",
                "RESET default_transaction_isolation;",
            ]));
            v
        }
        "earm:opclass" => sample(g, OPCLASS_PROBES, 7).into_iter().map(raw).collect(),
        "earm:tabdef" => {
            let mut v = fx_tables();
            v.extend(raws(&[
                "CREATE VIEW ea_v AS SELECT a, b FROM ea_base;",
                "CREATE SEQUENCE ea_seq;",
                "CREATE TYPE ea_enum AS ENUM ('r','g','b');",
                "CREATE DOMAIN ea_dom AS int CHECK (VALUE > 0);",
                "CREATE TABLE ea_part (id int, ts date) PARTITION BY RANGE (ts);",
                "CREATE TABLE ea_p1 PARTITION OF ea_part FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');",
            ]));
            v.extend(sample(g, TABDEF_PROBES, 12).into_iter().map(raw));
            v.extend(raws(&[
                // The two in-range partition-bound probes succeed when
                // sampled (they are the happy exits the error arms branch
                // around); unconditional IF EXISTS keeps balance.
                "DROP TABLE IF EXISTS ea_badp;",
                "DROP TABLE ea_part;",
                "DROP DOMAIN ea_dom;",
                "DROP TYPE ea_enum;",
                "DROP SEQUENCE ea_seq;",
                "DROP VIEW ea_v;",
            ]));
            v.extend(fx_tables_drop());
            v
        }
        other => unreachable!("earm shape {other}"),
    }
}

pub fn gen_earm_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("earm");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    body(g, shape)
}

/// Round-13 (run 72b2e74701d0e1310d59d39345e716da-59-13, seeds
/// 4078551634953133971 / 1653252951681713200): the earm-family and
/// ddldeep fixed CREATE/DROP role decks are the SAME cluster-global
/// pg_authid hazard the aclrls (FP-12, #1550), gramwalk (#1565) and
/// nodes ns_role (#1568) rebases already retired — concurrent batches'
/// DROP ROLE ea3_wown brackets raced each other and the A/B
/// interleavings diverged in both directions. Every deck-owned role
/// TOKEN (word-boundary match, quoted material included so pg_roles /
/// aclitem probe literals move coherently) is rewritten into the
/// batch-unique `{tag}_` namespace; helper_diffrun's `{db}_*` role
/// reclaim covers the leftovers of a crashed batch. Deliberately NOT
/// rebased: pg_-prefixed reserved-name error decks (`pg_ea_reserved`,
/// `pg_ea2b_reserved`, `pg_bogus` — the 42939 is the point and the role
/// never exists), keyword-spelled matched-error names (`public`,
/// `current_role`, `"none"`, `and`, `has` — never created), and
/// `*_nosuch_*` missing-role probes. Unlike the aclrls PREFIX rewrite,
/// this is a token list: the ea_/ea2_/ea3_/ea4_ prefixes also own
/// hundreds of db-local tables, and moving those would double-rebase
/// the deck tablespaces (ea_ts*) already handled by the whole-stream
/// tablespace pass. The drift guard in each module's tests asserts every
/// deck CREATE ROLE stays covered.
pub const FIXED_ROLE_TOKENS: &[&str] = &[
    // earm
    "ea_sneak",
    "ea_aclrole",
    "ea_aclmem",
    "ea_aclnobody",
    "ea_aclowner",
    "ea_badconn",
    "ea_dupopt",
    // earm2
    "ea2_owner",
    "ea2_grantee",
    "ea2_cyc1",
    "ea2_cyc2",
    "ea2b_r1",
    "ea2b_r2",
    "ea2b_r3",
    "ea2b_r4",
    "ea2b_r5",
    "ea2b_r7",
    "ea2b_r8",
    "ea2b_r9",
    "ea2b_rx",
    "ea2b_u",
    // earm3
    "ea3_u1",
    "ea3_u2",
    "ea3_cr",
    "ea3_sub",
    "ea3_bad",
    "ea3_m1",
    "ea3_m2",
    "ea3_wown",
    "ea3_own1",
    "ea3_own2",
    // earm4
    "ea4_o1",
    "ea4_o2",
    // ddldeep
    "dd_u",
    "dd_pu",
    // objid (round-14, run 37aa8abec580e5f2d184aa2b00ea1060-59-13, seed
    // 1777368629953620144): the largeobj/objid deck's cluster-global
    // ldo_user/ldo_user2 CREATE/DROP brackets raced concurrent batches
    // the same way (one-sided 2BP01 on DROP ROLE ldo_user; XX000 "tuple
    // concurrently deleted" on the concurrent DROP ROLE ldo_user2) — the
    // #1569 deck rebase never covered them. ldo_user2 sorts before
    // ldo_user only for reading order; replace_word's boundary check
    // keeps the prefix overlap safe either way.
    "ldo_user2",
    "ldo_user",
];

fn replace_word(s: &str, from: &str, to: &str) -> String {
    let b = s.as_bytes();
    let fb = from.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i..].starts_with(fb) {
            let before_ok =
                i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
            let j = i + fb.len();
            let after_ok =
                j >= b.len() || !(b[j].is_ascii_alphanumeric() || b[j] == b'_');
            if before_ok && after_ok {
                out.extend_from_slice(to.as_bytes());
                i = j;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    // `from` is ASCII and every splice point sits on an ASCII byte, so the
    // result stays valid UTF-8.
    String::from_utf8(out).expect("ASCII token splice kept UTF-8 valid")
}

/// Applied by the runner to the WHOLE statement stream (see diffrunner).
pub fn rebase_role_names(sql: &str, tag: &str) -> String {
    let mut out = sql.to_string();
    for tok in FIXED_ROLE_TOKENS {
        if out.contains(tok) {
            out = replace_word(&out, tok, &format!("{tag}_{tok}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> Vec<Vec<String>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_earm_module(&mut g);
            out.push(
                stmts
                    .into_iter()
                    .map(|s| match s {
                        StmtKind::Raw(t) => t,
                        other => panic!("earm emits Raw only, got {other:?}"),
                    })
                    .collect(),
            );
        }
        out
    }

    /// Same seed, same stream — the reproducibility witness.
    #[test]
    fn deterministic_by_seed() {
        assert_eq!(gen_groups(42, 150), gen_groups(42, 150));
    }

    /// Round-13: the earm-family role rebase rewrites deck role tokens
    /// (quoted material included), respects word boundaries, and leaves
    /// the intentional reserved-name / keyword error decks alone.
    #[test]
    fn rebase_role_names_rewrites_deck_roles_only() {
        let t = "fuzz_mixed_1a2b";
        assert_eq!(
            rebase_role_names("CREATE ROLE ea3_wown;", t),
            "CREATE ROLE fuzz_mixed_1a2b_ea3_wown;"
        );
        assert_eq!(
            rebase_role_names("ALTER TABLE ea3_wt OWNER TO ea3_wown;", t),
            "ALTER TABLE ea3_wt OWNER TO fuzz_mixed_1a2b_ea3_wown;"
        );
        assert_eq!(
            rebase_role_names(
                "SELECT rolname FROM pg_roles WHERE rolname = 'ea_aclrole';",
                t
            ),
            "SELECT rolname FROM pg_roles WHERE rolname = 'fuzz_mixed_1a2b_ea_aclrole';"
        );
        assert_eq!(
            rebase_role_names("GRANT SELECT ON dd_tp TO dd_pu;", t),
            "GRANT SELECT ON dd_tp TO fuzz_mixed_1a2b_dd_pu;"
        );
        // Word boundary: table/typed names sharing a prefix stay put.
        assert_eq!(
            rebase_role_names("SELECT * FROM ea3_wownx, ea3_m1_extra;", t),
            "SELECT * FROM ea3_wownx, ea3_m1_extra;"
        );
        // Intentional error decks stay put.
        for sql in [
            "CREATE ROLE pg_ea_reserved;",
            "CREATE ROLE pg_bogus;",
            "CREATE ROLE \"none\";",
            "CREATE ROLE public;",
            "CREATE ROLE current_role;",
            "COMMENT ON ROLE ea4_nosuch_role IS 'x';",
        ] {
            assert_eq!(rebase_role_names(sql, t), sql);
        }
        // Idempotent: a rebased name is not rebased again.
        let once = rebase_role_names("DROP ROLE ea3_wown;", t);
        assert_eq!(rebase_role_names(&once, t), once);
    }

    /// Drift guard: every CREATE ROLE any module in the earm family (or
    /// ddldeep) can emit is either covered by FIXED_ROLE_TOKENS or is an
    /// intentional never-created error name. A new deck role added
    /// without extending the token list fails here instead of racing
    /// concurrent batches in the CI cluster.
    #[test]
    fn rebase_covers_every_deck_role() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let gens: &[(&str, fn(&mut Gen) -> Vec<StmtKind>)] = &[
            ("earm", gen_earm_module),
            ("earm2", crate::earm2::gen_earm2_module),
            ("earm3", crate::earm3::gen_earm3_module),
            ("earm4", crate::earm4::gen_earm4_module),
            ("ddldeep", crate::ddldeep::gen_ddldeep_module),
            // Round-14: the objid deck's ldo_user/ldo_user2 joined the list.
            ("objid", crate::objid::gen_objid_module),
        ];
        for (name, f) in gens {
            let mut rng = Rng::new(1234);
            for _ in 0..4000 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                for s in f(&mut g) {
                    let sql = s.to_sql();
                    for line in sql.lines() {
                        let Some(rest) = line.trim_start().strip_prefix("CREATE ROLE ")
                        else {
                            continue;
                        };
                        let role: String = rest
                            .chars()
                            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '"')
                            .collect();
                        let intentional = role.starts_with("pg_")
                            || role.starts_with('"')
                            || role.contains("nosuch")
                            || ["public", "current_role", "none"].contains(&role.as_str());
                        assert!(
                            intentional || FIXED_ROLE_TOKENS.contains(&role.as_str()),
                            "{name}: deck role {role:?} not covered by \
                             FIXED_ROLE_TOKENS (statement: {line:?})"
                        );
                    }
                }
            }
        }
    }

    /// Every shape is reachable from the default weight table.
    #[test]
    fn all_shapes_fire() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(7);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            gen_earm_module(&mut g);
            for p in prods {
                seen.insert(p);
            }
        }
        for shape in SHAPES {
            assert!(seen.contains(*shape), "shape never fired: {shape}");
        }
    }

    /// Bracket discipline: every group that emits SET ROLE emits RESET
    /// ROLE after it, and every fixture CREATE of an `ea_` object that
    /// can succeed has a matching DROP later in the same group. Probes
    /// are deliberately erroring, so only the known-success statements
    /// are counted as creates: the fixture section (before the first
    /// probe) plus the documented intentional successes, all of which
    /// end with an unconditional or IF EXISTS drop in the tail.
    #[test]
    fn role_and_guc_brackets_close() {
        for group in gen_groups(0xa11, 400) {
            let joined = group.join(" ");
            if joined.contains("SET ROLE ea_aclnobody") {
                let set = joined.find("SET ROLE ea_aclnobody").unwrap();
                let reset = joined.rfind("RESET ROLE;").expect("SET ROLE without RESET ROLE");
                assert!(reset > set, "RESET ROLE precedes SET ROLE");
                // The role drops come after the RESET.
                let drop = joined.rfind("DROP ROLE ea_aclnobody").expect("role not dropped");
                assert!(drop > reset);
            }
            if joined.contains("SET datestyle") || joined.contains("SET TIME ZONE") {
                assert!(joined.contains("RESET datestyle;"), "datestyle not reset: {joined}");
                assert!(joined.contains("RESET timezone;"), "timezone not reset: {joined}");
            }
        }
    }

    /// Fixture creates all have a drop in the same group (string-level:
    /// for each "CREATE <kind> ea_<name>" in the fixture prefix there is
    /// a later "DROP ... ea_<name>"). Erroring probes reuse fixture names
    /// (ea_bad/ea_badp/ea_r1/ea_st/"C") which the tails drop IF EXISTS.
    #[test]
    fn fixtures_are_dropped() {
        // (create-statement fragment, required drop fragment) pairs for
        // every fixture create a group can emit. Probe creates all error
        // (or are covered by the tails' IF EXISTS drops), so a group is
        // balanced iff each fired create fragment has its drop fragment.
        const PAIRS: &[(&str, &str)] = &[
            ("CREATE TABLE ea_base (a int PRIMARY KEY", "DROP TABLE ea_base;"),
            ("CREATE TABLE ea_ref (x int", "DROP TABLE ea_ref;"),
            ("CREATE TABLE ea_reftext ", "DROP TABLE ea_reftext;"),
            ("CREATE TABLE ea_dup ", "DROP TABLE ea_dup;"),
            ("CREATE VIEW ea_v ", "DROP VIEW ea_v;"),
            ("CREATE MATERIALIZED VIEW ea_mv ", "DROP MATERIALIZED VIEW ea_mv;"),
            ("CREATE SEQUENCE ea_seq;", "DROP SEQUENCE ea_seq;"),
            ("CREATE TABLE ea_part ", "DROP TABLE ea_part;"),
            ("CREATE TYPE ea_comp AS ", "DROP TYPE ea_comp;"),
            ("CREATE TYPE ea_enum AS ", "DROP TYPE ea_enum;"),
            ("CREATE DOMAIN ea_dom AS ", "DROP DOMAIN ea_dom;"),
            ("CREATE SCHEMA ea_s;", "DROP SCHEMA ea_s;"),
            ("CREATE FUNCTION ea_f(", "DROP FUNCTION ea_f(int);"),
            ("CREATE FUNCTION ea_any(", "DROP FUNCTION ea_any(anyelement, anyelement);"),
            ("CREATE FUNCTION ea_anyarr(", "DROP FUNCTION ea_anyarr(anyarray, anyelement);"),
            ("CREATE FUNCTION ea_rec(", "DROP FUNCTION ea_rec();"),
            ("CREATE ROLE ea_aclnobody;", "DROP ROLE ea_aclnobody;"),
            ("CREATE ROLE ea_aclowner;", "DROP ROLE ea_aclowner;"),
            ("CREATE ROLE ea_aclrole;", "DROP ROLE ea_aclrole;"),
        ];
        for group in gen_groups(0xf17, 300) {
            let joined = group.join("\n");
            for (create, drop) in PAIRS {
                if joined.contains(create) {
                    assert!(
                        joined.contains(drop),
                        "fixture create {create:?} without {drop:?}:\n{joined}"
                    );
                }
            }
        }
    }
}
