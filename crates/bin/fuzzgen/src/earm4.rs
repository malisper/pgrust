//! ERROR-ARM drain module, round 4 (lane ERR3; line-drain queue chunks
//! `catalog-residue` + `ddl-cmds-residue` + `parser-arms`, the residue the
//! `earm`/`earm2`/`earm3` passes left — see docs/fuzzing/findings-err3.md).
//!
//! Same charter as `earm`/`earm2`/`earm3`: the unhit mass is
//! ereport(ERROR)/validation arms only invalid input reaches, so probes
//! deliberately error and the differential bar is the ERROR IDENTITY
//! (SQLSTATE via diff::classify, message drift carried in the detail).
//! Round-4 surface deliberately targets the DDL/catalog error paths the
//! first three passes stayed away from:
//!   - domain constraint/default validation (domaincmds.c/typecmds.c:
//!     AlterDomainNotNull/AlterDomainAddConstraint/validateDomainCheck,
//!     RENAME/DROP CONSTRAINT, default coercion, missing base type);
//!   - deeper ALTER TABLE constraint validation (tablecmds.c: ADD CHECK
//!     over violating rows, VALIDATE CONSTRAINT NOT VALID, SET NOT NULL
//!     over nulls, ADD PK/FK with no matching unique, ALTER CONSTRAINT on
//!     a non-FK);
//!   - ALTER TABLE misc validation (SET ACCESS METHOD missing, CLUSTER ON
//!     missing index, SET STATISTICS out of range, reloption bounds,
//!     DISABLE TRIGGER/RULE missing, REPLICA IDENTITY USING missing,
//!     OF missing type, INHERIT missing);
//!   - tablespace arms (SET TABLESPACE / TABLESPACE clause / ALL IN
//!     TABLESPACE with a missing tablespace, pg_global refusal);
//!   - ownership/dependency arms (alter.c/dependency.c/user.c: OWNER TO
//!     missing role + not-owner, DROP RESTRICT with dependents,
//!     REASSIGN/DROP OWNED for a missing role);
//!   - RLS/policy definition errors (policy.c: duplicate policy, WITH
//!     CHECK on SELECT/DELETE, wrong relkind, missing policy/table);
//!   - RULE definition errors (rewriteDefine.c: duplicate rule, ON SELECT
//!     name rule, missing rule/table);
//!   - publication DDL validation (publicationcmds.c: FOR ALL TABLES /
//!     FOR TABLE mix, mutable WHERE, missing add-table, bad option);
//!   - subscription DDL validation (subscriptioncmds.c: connect=false
//!     mutually-exclusive option matrix, bad option value, missing sub) —
//!     every probe rejects before any network connect and creates no
//!     subscription object;
//!   - COMMENT / SECURITY LABEL error arms (comment.c/seclabel.c: missing
//!     object of each kind, no-provider / unknown-provider);
//!   - sequence/identity deeper (sequence.c/tablecmds.c: init_params
//!     bounds + AS-type range, non-integer identity, already/not identity,
//!     GENERATED ALWAYS non-default insert);
//!   - remaining parser grammar-error productions (gram.y/analyze:
//!     UNION width, VALUES length, LOCK mode, EXPLAIN/VACUUM/REINDEX
//!     option, MySQL LIMIT, window-frame ordering, PREPARE untyped param,
//!     EXECUTE of a missing statement).
//!
//! Rules (inherited from earm/earm2/earm3):
//!   - each shape is a SELF-CONTAINED section applied per-statement to the
//!     A/B pair (identical SQLSTATE everywhere; the only residual diff is
//!     the already-banked LD6-F1 ERRPOS-MISSING / LD6-F2
//!     CONSTRAINT-ERRFIELDS wire-field classes, which are NOT re-filed);
//!   - fixtures live under fixed `ea4_` names, created and dropped inside
//!     the section; erroring probes cannot create state, so a poisoned
//!     probe aborts only itself under the rig's per-statement isolation;
//!   - roles are cluster-global: every CREATE ROLE has its DROP in the
//!     same section past the probes that need it; SET ROLE spans end with
//!     RESET ROLE before any DDL the superuser must perform;
//!   - no BEGIN spans and no `SET search_path` (GUC-pollution guard);
//!   - probes never emit result sets whose column types are user OIDs
//!     (LD6/LD8 ROWSET-drift trap).

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "earm4:domcon",
    "earm4:chkval",
    "earm4:atmisc",
    "earm4:tblspc",
    "earm4:owndep",
    "earm4:policy",
    "earm4:rules",
    "earm4:pubval",
    "earm4:subval",
    "earm4:commlbl",
    "earm4:seqid2",
    "earm4:pgram2",
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

// ---------------------------------------------------------------------------
// domcon: CREATE/ALTER DOMAIN validation (domaincmds.c / typecmds.c).
// ---------------------------------------------------------------------------
const DOMCON: &[&str] = &[
    "CREATE DOMAIN ea4_bt AS nosuch_base;",
    "CREATE DOMAIN ea4_dsub AS int CHECK (VALUE > (SELECT 1));",
    "CREATE DOMAIN ea4_ddef AS int DEFAULT 'notanint';",
    "CREATE DOMAIN ea4_dd AS int;",
    "CREATE TABLE ea4_ddt (a ea4_dd);",
    "INSERT INTO ea4_ddt VALUES (NULL), (-1);",
    "ALTER DOMAIN ea4_dd SET NOT NULL;",
    "ALTER DOMAIN ea4_dd ADD CONSTRAINT ea4_dpos CHECK (VALUE > 0);",
    "ALTER DOMAIN ea4_dd ADD CONSTRAINT ea4_dnv CHECK (VALUE > 0) NOT VALID;",
    "ALTER DOMAIN ea4_dd VALIDATE CONSTRAINT ea4_dnv;",
    "ALTER DOMAIN ea4_dd DROP CONSTRAINT ea4_missing;",
    "ALTER DOMAIN ea4_dd RENAME CONSTRAINT ea4_missing TO ea4_x;",
    "ALTER DOMAIN ea4_dd SET DEFAULT 'notanint';",
    "ALTER DOMAIN ea4_dd ADD CONSTRAINT ea4_dnn NOT NULL;",
    "DROP DOMAIN ea4_dd;",
    "DROP DOMAIN ea4_dd RESTRICT;",
    "DROP TABLE ea4_ddt;",
    "DROP DOMAIN ea4_dd;",
];

// ---------------------------------------------------------------------------
// chkval: deeper ALTER TABLE constraint validation (tablecmds.c).
// ---------------------------------------------------------------------------
const CHKVAL: &[&str] = &[
    "CREATE TABLE ea4_ck (a int, b int);",
    "INSERT INTO ea4_ck VALUES (1, 1), (NULL, 2), (-1, 3);",
    "ALTER TABLE ea4_ck ADD CONSTRAINT ea4_ckc CHECK (a > 0);",
    "ALTER TABLE ea4_ck ALTER COLUMN a SET NOT NULL;",
    "ALTER TABLE ea4_ck ADD PRIMARY KEY (a);",
    "ALTER TABLE ea4_ck ADD CONSTRAINT ea4_cknv CHECK (a > 0) NOT VALID;",
    "ALTER TABLE ea4_ck VALIDATE CONSTRAINT ea4_cknv;",
    "ALTER TABLE ea4_ck ADD CONSTRAINT ea4_ckfk FOREIGN KEY (a) REFERENCES ea4_ck (b);",
    "ALTER TABLE ea4_ck ALTER CONSTRAINT ea4_cknv DEFERRABLE;",
    "ALTER TABLE ea4_ck VALIDATE CONSTRAINT ea4_missing;",
    "ALTER TABLE ea4_ck DROP CONSTRAINT ea4_missing;",
    "DROP TABLE ea4_ck;",
];

// ---------------------------------------------------------------------------
// atmisc: ALTER TABLE misc subcommand validation (tablecmds.c / reloptions).
// ---------------------------------------------------------------------------
const ATMISC: &[&str] = &[
    "CREATE TABLE ea4_am (a int);",
    "ALTER TABLE ea4_am SET ACCESS METHOD ea4_nosuch_am;",
    "ALTER TABLE ea4_am ALTER COLUMN a SET STORAGE EXTERNAL;",
    "ALTER TABLE ea4_am ALTER COLUMN a SET STATISTICS -5;",
    "ALTER TABLE ea4_am CLUSTER ON ea4_nosuch_idx;",
    "ALTER TABLE ea4_am SET (fillfactor = 5);",
    "ALTER TABLE ea4_am SET (fillfactor = 200);",
    "ALTER TABLE ea4_am SET (autovacuum_enabled = maybe);",
    "ALTER TABLE ea4_am DISABLE TRIGGER ea4_nosuch_trg;",
    "ALTER TABLE ea4_am DISABLE RULE ea4_nosuch_rule;",
    "ALTER TABLE ea4_am REPLICA IDENTITY USING INDEX ea4_nosuch_idx;",
    "ALTER TABLE ea4_am OF ea4_nosuch_type;",
    "ALTER TABLE ea4_am INHERIT ea4_nosuch_parent;",
    "ALTER TABLE ea4_am ALTER COLUMN a SET (n_distinct = 'x');",
    "DROP TABLE ea4_am;",
];

// ---------------------------------------------------------------------------
// tblspc: tablespace placement arms (tablecmds.c / tablespace.c).
// ---------------------------------------------------------------------------
const TBLSPC: &[&str] = &[
    "CREATE TABLE ea4_ts (a int);",
    "ALTER TABLE ea4_ts SET TABLESPACE ea4_nosuch_ts;",
    "ALTER TABLE ea4_ts SET TABLESPACE pg_global;",
    "CREATE TABLE ea4_ts2 (a int) TABLESPACE ea4_nosuch_ts;",
    "CREATE INDEX ea4_tsi ON ea4_ts (a) TABLESPACE ea4_nosuch_ts;",
    "ALTER TABLE ALL IN TABLESPACE ea4_nosuch_ts SET TABLESPACE pg_default;",
    "ALTER INDEX ALL IN TABLESPACE ea4_nosuch_ts SET TABLESPACE pg_default;",
    "DROP TABLE ea4_ts;",
];

// ---------------------------------------------------------------------------
// owndep: ownership + dependency arms (alter.c / dependency.c / user.c).
// ---------------------------------------------------------------------------
const OWNDEP: &[&str] = &[
    "CREATE ROLE ea4_o1 NOSUPERUSER;",
    "CREATE ROLE ea4_o2 NOSUPERUSER;",
    "CREATE TABLE ea4_ot (a int);",
    "ALTER TABLE ea4_ot OWNER TO ea4_nosuch_role;",
    "SET ROLE ea4_o1;",
    "ALTER TABLE ea4_ot OWNER TO ea4_o2;",
    "DROP TABLE ea4_ot;",
    "RESET ROLE;",
    "REASSIGN OWNED BY ea4_nosuch_role TO postgres;",
    "DROP OWNED BY ea4_nosuch_role;",
    "CREATE TABLE ea4_dp (a int);",
    "CREATE VIEW ea4_dv AS SELECT a FROM ea4_dp;",
    "DROP TABLE ea4_dp;",
    "DROP TABLE ea4_dp RESTRICT;",
    "ALTER TABLE ea4_dp OWNER TO ea4_o1;",
    "DROP VIEW ea4_dv;",
    "DROP TABLE ea4_dp;",
    "DROP TABLE ea4_ot;",
    "DROP ROLE ea4_o1;",
    "DROP ROLE ea4_o2;",
];

// ---------------------------------------------------------------------------
// policy: RLS / policy definition errors (policy.c).
// ---------------------------------------------------------------------------
const POLICY: &[&str] = &[
    "CREATE TABLE ea4_pt (a int, b int);",
    "CREATE VIEW ea4_pv AS SELECT 1 AS a;",
    "CREATE POLICY ea4_pol ON ea4_pt USING (a > 0);",
    "CREATE POLICY ea4_pol ON ea4_pt USING (a > 0);",
    "CREATE POLICY ea4_psel ON ea4_pt FOR SELECT WITH CHECK (a > 0);",
    "CREATE POLICY ea4_pdel ON ea4_pt FOR DELETE WITH CHECK (a > 0);",
    "CREATE POLICY ea4_pmiss ON ea4_nosuch_tab USING (true);",
    "CREATE POLICY ea4_pcol ON ea4_pt USING (nosuchcol > 0);",
    "CREATE POLICY ea4_pview ON ea4_pv USING (true);",
    "ALTER POLICY ea4_nosuch_pol ON ea4_pt USING (true);",
    "DROP POLICY ea4_nosuch_pol ON ea4_pt;",
    "ALTER TABLE ea4_pv ENABLE ROW LEVEL SECURITY;",
    "ALTER TABLE ea4_pv FORCE ROW LEVEL SECURITY;",
    "DROP VIEW ea4_pv;",
    "DROP TABLE ea4_pt;",
];

// ---------------------------------------------------------------------------
// rules: RULE definition errors (rewriteDefine.c).
// ---------------------------------------------------------------------------
const RULES: &[&str] = &[
    "CREATE TABLE ea4_rt (a int);",
    "CREATE RULE ea4_rins AS ON INSERT TO ea4_rt DO INSTEAD NOTHING;",
    "CREATE RULE ea4_rins AS ON INSERT TO ea4_rt DO INSTEAD NOTHING;",
    "CREATE RULE ea4_rsel AS ON SELECT TO ea4_rt DO INSTEAD SELECT a FROM ea4_rt;",
    "CREATE RULE ea4_rmiss AS ON INSERT TO ea4_nosuch_tab DO NOTHING;",
    "DROP RULE ea4_nosuch_rule ON ea4_rt;",
    "DROP RULE ea4_rins ON ea4_nosuch_tab;",
    "DROP TABLE ea4_rt;",
];

// ---------------------------------------------------------------------------
// pubval: publication DDL validation (publicationcmds.c).
// ---------------------------------------------------------------------------
const PUBVAL: &[&str] = &[
    "CREATE TABLE ea4_put (a int PRIMARY KEY, b int);",
    "CREATE PUBLICATION ea4_pub FOR TABLE ea4_put;",
    "CREATE PUBLICATION ea4_pub FOR TABLE ea4_put;",
    "CREATE PUBLICATION ea4_pub2 FOR ALL TABLES, TABLE ea4_put;",
    "CREATE PUBLICATION ea4_pub3 FOR TABLE ea4_put WHERE (b > random());",
    "CREATE PUBLICATION ea4_pub4 WITH (publish = 'bogus');",
    "CREATE PUBLICATION ea4_pub5 FOR TABLE ea4_nosuch_tab;",
    "ALTER PUBLICATION ea4_pub ADD TABLE ea4_nosuch_tab;",
    "ALTER PUBLICATION ea4_pub SET (publish = 'bogus');",
    "ALTER PUBLICATION ea4_nosuch_pub ADD TABLE ea4_put;",
    "DROP PUBLICATION ea4_pub;",
    "DROP TABLE ea4_put;",
];

// ---------------------------------------------------------------------------
// subval: subscription option validation (subscriptioncmds.c). Every arm
// rejects before any network connect and creates no subscription object.
// ---------------------------------------------------------------------------
const SUBVAL: &[&str] = &[
    "CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (connect = false, copy_data = true);",
    "CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (connect = false, create_slot = true);",
    "CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (connect = false, enabled = true);",
    "CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (slot_name = NONE, enabled = true);",
    "CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (streaming = 'bogus');",
    "CREATE SUBSCRIPTION ea4_sub CONNECTION 'dbname=ea4_x' PUBLICATION ea4_p WITH (synchronous_commit = 'bogus');",
    "ALTER SUBSCRIPTION ea4_nosuch_sub SET (streaming = true);",
    "ALTER SUBSCRIPTION ea4_nosuch_sub OWNER TO postgres;",
    "DROP SUBSCRIPTION ea4_nosuch_sub;",
];

// ---------------------------------------------------------------------------
// commlbl: COMMENT / SECURITY LABEL error arms (comment.c / seclabel.c).
// ---------------------------------------------------------------------------
const COMMLBL: &[&str] = &[
    "CREATE TABLE ea4_ct (a int);",
    "COMMENT ON TABLE ea4_nosuch_tab IS 'x';",
    "COMMENT ON COLUMN ea4_ct.nosuchcol IS 'x';",
    "COMMENT ON FUNCTION ea4_nosuch_fn() IS 'x';",
    "COMMENT ON SCHEMA ea4_nosuch_schema IS 'x';",
    "COMMENT ON ROLE ea4_nosuch_role IS 'x';",
    "COMMENT ON CONSTRAINT ea4_nosuch_con ON ea4_ct IS 'x';",
    "COMMENT ON DOMAIN ea4_nosuch_dom IS 'x';",
    "SECURITY LABEL ON TABLE ea4_ct IS 'x';",
    "SECURITY LABEL FOR ea4_nosuch_provider ON TABLE ea4_ct IS 'x';",
    "SECURITY LABEL FOR ea4_nosuch_provider ON ROLE postgres IS 'x';",
    "DROP TABLE ea4_ct;",
];

// ---------------------------------------------------------------------------
// seqid2: sequence + identity validation (sequence.c / tablecmds.c).
// ---------------------------------------------------------------------------
const SEQID2: &[&str] = &[
    "CREATE SEQUENCE ea4_s1;",
    "ALTER SEQUENCE ea4_s1 INCREMENT BY 0;",
    "ALTER SEQUENCE ea4_s1 MINVALUE 100 MAXVALUE 10;",
    "ALTER SEQUENCE ea4_s1 START WITH 5 MINVALUE 10;",
    "ALTER SEQUENCE ea4_s1 AS smallint MAXVALUE 100000;",
    "ALTER SEQUENCE ea4_s1 OWNED BY ea4_nosuch_tab.col;",
    "CREATE SEQUENCE ea4_s2 AS boolean;",
    "CREATE TABLE ea4_it (a int GENERATED ALWAYS AS IDENTITY, b text);",
    "ALTER TABLE ea4_it ALTER COLUMN b ADD GENERATED ALWAYS AS IDENTITY;",
    "ALTER TABLE ea4_it ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;",
    "ALTER TABLE ea4_it ALTER COLUMN b DROP IDENTITY;",
    "ALTER TABLE ea4_it ALTER COLUMN b SET GENERATED BY DEFAULT;",
    "CREATE TABLE ea4_it2 (a int GENERATED ALWAYS AS IDENTITY DEFAULT 5);",
    "INSERT INTO ea4_it (a) VALUES (1);",
    "DROP TABLE ea4_it;",
    "DROP SEQUENCE ea4_s1;",
];

// ---------------------------------------------------------------------------
// pgram2: remaining parser grammar-error productions (gram.y / analyze).
// ---------------------------------------------------------------------------
const PGRAM2: &[&str] = &[
    "CREATE TABLE ea4_g (a int, b int);",
    "SELECT a FROM ea4_g UNION SELECT a, b FROM ea4_g;",
    "INSERT INTO ea4_g VALUES (1, 2), (3);",
    "LOCK TABLE ea4_g IN ea4_bogus MODE;",
    "EXPLAIN (FORMAT ea4_bogus) SELECT 1;",
    "EXPLAIN (ea4_bogus) SELECT 1;",
    "VACUUM (ea4_bogus) ea4_g;",
    "REINDEX (ea4_bogus) TABLE ea4_g;",
    "SELECT * FROM ea4_g LIMIT 1, 2;",
    "SELECT sum(a) OVER (ORDER BY a ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM ea4_g;",
    "SELECT $1;",
    "EXECUTE ea4_nosuch_ps;",
    "DEALLOCATE ea4_nosuch_ps;",
    "SELECT a FROM ea4_g GROUP BY ROLLUP;",
    "DROP TABLE ea4_g;",
];

fn body(shape: &str) -> Vec<StmtKind> {
    let pool: &[&str] = match shape {
        "earm4:domcon" => DOMCON,
        "earm4:chkval" => CHKVAL,
        "earm4:atmisc" => ATMISC,
        "earm4:tblspc" => TBLSPC,
        "earm4:owndep" => OWNDEP,
        "earm4:policy" => POLICY,
        "earm4:rules" => RULES,
        "earm4:pubval" => PUBVAL,
        "earm4:subval" => SUBVAL,
        "earm4:commlbl" => COMMLBL,
        "earm4:seqid2" => SEQID2,
        "earm4:pgram2" => PGRAM2,
        other => unreachable!("earm4 shape {other}"),
    };
    raws(pool)
}

pub fn gen_earm4_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("earm4");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    body(shape)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_sections() -> Vec<(&'static str, &'static [&'static str])> {
        vec![
            ("domcon", DOMCON),
            ("chkval", CHKVAL),
            ("atmisc", ATMISC),
            ("tblspc", TBLSPC),
            ("owndep", OWNDEP),
            ("policy", POLICY),
            ("rules", RULES),
            ("pubval", PUBVAL),
            ("subval", SUBVAL),
            ("commlbl", COMMLBL),
            ("seqid2", SEQID2),
            ("pgram2", PGRAM2),
        ]
    }

    #[test]
    fn sections_are_self_contained() {
        for (name, pool) in all_sections() {
            let joined = pool.join("\n");
            // No transaction spans.
            assert!(!joined.contains("BEGIN;"), "{name}: unexpected BEGIN span");
            // No GUC pollution via search_path.
            assert!(
                !joined.contains("SET search_path"),
                "{name}: search_path touched"
            );
            // Cluster-global roles must be dropped in the same section.
            for line in pool.iter() {
                if let Some(rest) = line.strip_prefix("CREATE ROLE ") {
                    let role = rest
                        .split(|c: char| c == ' ' || c == ';')
                        .next()
                        .unwrap_or("");
                    assert!(!role.is_empty(), "{name}: unparsable CREATE ROLE");
                    let drop = format!("DROP ROLE {role};");
                    let ci = joined.find(&format!("CREATE ROLE {role}")).unwrap();
                    let di = joined
                        .rfind(&drop)
                        .unwrap_or_else(|| panic!("{name}: CREATE ROLE {role} without DROP"));
                    assert!(di > ci, "{name}: DROP ROLE {role} not after CREATE");
                }
                if let Some(rest) = line.strip_prefix("CREATE SCHEMA ") {
                    let sc = rest
                        .split(|c: char| c == ' ' || c == ';')
                        .next()
                        .unwrap_or("");
                    let ci = joined.find(&format!("CREATE SCHEMA {sc}")).unwrap();
                    let di = joined
                        .rfind(&format!("DROP SCHEMA {sc}"))
                        .unwrap_or_else(|| panic!("{name}: CREATE SCHEMA {sc} without DROP"));
                    assert!(di > ci, "{name}: DROP SCHEMA {sc} not after CREATE");
                }
            }
            // Every SET ROLE returns to the superuser before section end.
            if let Some(sr) = joined.rfind("SET ROLE ") {
                let rr = joined.rfind("RESET ROLE;");
                assert!(
                    rr.is_some() && rr.unwrap() > sr,
                    "{name}: SET ROLE without later RESET ROLE"
                );
            }
        }
    }

    #[test]
    fn every_shape_generates() {
        for shape in SHAPES {
            let stmts = body(shape);
            assert!(!stmts.is_empty(), "{shape} empty");
        }
    }
}
