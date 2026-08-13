//! Trigger / rule / event-trigger EXECUTION drain module (Track-B
//! coverage-breadth; line-gap-report-004 + gap-report-009 residue in
//! backend/commands/trigger.c, backend/commands/event_trigger.c,
//! backend/rewrite/rewriteDefine.c and backend/rewrite/rewriteHandler.c).
//!
//! Where `earm2` drains the trigger/rule *error* arms (deliberately-invalid
//! CREATE TRIGGER / CREATE RULE, error-identity bar), this module drains the
//! *successful execution* arms the happy-path corpus never reaches:
//!
//!   - `AfterTriggerSetState` (SET CONSTRAINTS ALL/name DEFERRED|IMMEDIATE
//!     flipping deferred constraint triggers mid-transaction);
//!   - `EnableDisableTrigger` + `ATExecEnableDisableTrigger` and the
//!     session_replication_role interplay (DISABLE / ENABLE / ENABLE REPLICA
//!     / ENABLE ALWAYS TRIGGER, then origin vs replica DML);
//!   - `renametrig` / `renametrig_internal` / `renametrig_partition`
//!     (ALTER TRIGGER ... RENAME on a plain and a partitioned table, the
//!     latter recursing to leaves);
//!   - `TriggerSetParentTrigger` + `CreateTriggerFiringOn` (CREATE TRIGGER on
//!     a partitioned parent cloned to existing leaves, plus a post-hoc
//!     ATTACH PARTITION cloning the parent trigger to the new child);
//!   - `CreateEventTrigger` / `insert_event_trigger_tuple` / `EventTriggerInvoke`
//!     / `plpgsql_exec_event_trigger` / `pg_event_trigger_ddl_commands` /
//!     `stringify_grant_objtype` / `stringify_adefprivs_objtype` (a plpgsql
//!     ddl_command_end handler inspecting the ddl-commands SRF over CREATE /
//!     ALTER / COMMENT / GRANT / REVOKE);
//!   - `EventTriggerSQLDropAddObject` / `pg_event_trigger_dropped_objects` /
//!     `obtain_object_name_namespace` (a sql_drop handler over the
//!     dropped-objects SRF);
//!   - `EnableDisableRule` + `ATExecEnableDisableRule` and `RenameRewriteRule`
//!     (ALTER TABLE ENABLE/DISABLE RULE, ALTER RULE ... RENAME, DO ALSO rule
//!     firing witnessed by a log table);
//!   - `error_view_not_updatable` / `relation_is_updatable` (DML on a
//!     non-auto-updatable view without an INSTEAD trigger/rule — error
//!     identity — plus the auto-updatable subset-column path through
//!     `findDefaultOnlyColumns`).
//!
//! Discipline (execution-drain specifics on top of the LD2/nodes rules):
//!   - Every group is SELF-CONTAINED: fixtures live under fixed `trg_`
//!     names, created and dropped inside the group. The firing surface is
//!     ORDER + COUNT, so every group ends by reading a per-group log table
//!     (`trg_log`, a serial `seq` + text `tag`) with a TOTAL `ORDER BY seq`
//!     probe — the differ compares the exact fired-trigger transcript, not
//!     just the final table image.
//!   - Data is deterministic and float-free; no now()/random()/timestamp
//!     surfaces. Trigger names are chosen so same-event firing order (which
//!     Postgres resolves alphabetically) is well-defined.
//!   - Cluster/database-global objects are the hazard here: event triggers
//!     and session_replication_role. Every event-trigger group leads with a
//!     defensive `DROP EVENT TRIGGER IF EXISTS` (also draining the IF EXISTS
//!     skip arm), disables the trigger before the witness read, and drops it
//!     inside the group; every session_replication_role group RESETs it. No
//!     group can leak state into a later group.
//!   - Sections are VERBATIM (the PRNG only picks the shape), so each group
//!     is byte-identical for a given seed. FLEET-PENDING: authored against
//!     PG18 semantics; A/B identity + coverage delta are witnessed on the
//!     CI cluster (docs/fuzzing/findings-triggers.md).

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

const SHAPES: &[&str] = &[
    "triggers:setconstr",
    "triggers:enabledisable",
    "triggers:replrole",
    "triggers:rename",
    "triggers:parted",
    "triggers:eventddl",
    "triggers:eventdrop",
    "triggers:rules",
    "triggers:viewupd",
];

pub fn gen_triggers_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("triggers");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    match shape {
        "triggers:setconstr" => raws(SETCONSTR),
        "triggers:enabledisable" => raws(ENABLEDISABLE),
        "triggers:replrole" => raws(REPLROLE),
        "triggers:rename" => raws(RENAME),
        "triggers:parted" => raws(PARTED),
        "triggers:eventddl" => raws(EVENTDDL),
        "triggers:eventdrop" => raws(EVENTDROP),
        "triggers:rules" => raws(RULES),
        "triggers:viewupd" => raws(VIEWUPD),
        other => unreachable!("triggers shape {other}"),
    }
}

// AfterTriggerSetState: deferred constraint triggers whose firing time is
// flipped by SET CONSTRAINTS inside a transaction. The log table witnesses
// the exact commit-time vs immediate firing transcript.
const SETCONSTR: &[&str] = &[
    "CREATE TABLE trg_sc (pk int PRIMARY KEY, v int);",
    "CREATE TABLE trg_log (seq serial PRIMARY KEY, tag text);",
    "CREATE FUNCTION trg_scf() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN INSERT INTO trg_log(tag) VALUES (TG_NAME || ':' || TG_OP || ':' || coalesce(NEW.pk, OLD.pk)::text); RETURN coalesce(NEW, OLD); END $t$;",
    "CREATE CONSTRAINT TRIGGER trg_c1 AFTER INSERT OR UPDATE ON trg_sc DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION trg_scf();",
    "CREATE CONSTRAINT TRIGGER trg_c2 AFTER INSERT OR UPDATE ON trg_sc DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION trg_scf();",
    "BEGIN;",
    "SET CONSTRAINTS ALL DEFERRED;",
    "INSERT INTO trg_sc VALUES (1, 10), (2, 20);",
    "INSERT INTO trg_log(tag) VALUES ('after-insert-before-flip');",
    "SET CONSTRAINTS trg_c2 IMMEDIATE;",
    "INSERT INTO trg_log(tag) VALUES ('after-c2-immediate');",
    "UPDATE trg_sc SET v = v + 1 WHERE pk = 1;",
    "SET CONSTRAINTS ALL IMMEDIATE;",
    "INSERT INTO trg_log(tag) VALUES ('after-all-immediate');",
    "COMMIT;",
    "BEGIN;",
    "SET CONSTRAINTS trg_c1 DEFERRED;",
    "UPDATE trg_sc SET v = v + 100 WHERE pk = 2;",
    "SET CONSTRAINTS trg_c1 IMMEDIATE;",
    "ROLLBACK;",
    "SELECT seq, tag FROM trg_log ORDER BY seq;",
    "SELECT pk, v FROM trg_sc ORDER BY pk;",
    "DROP TABLE trg_sc;",
    "DROP TABLE trg_log;",
    "DROP FUNCTION trg_scf();",
];

// EnableDisableTrigger / ATExecEnableDisableTrigger: DISABLE / ENABLE /
// ENABLE ALWAYS / ENABLE REPLICA TRIGGER, plus DISABLE TRIGGER USER / ALL,
// witnessed by DML between each state change.
const ENABLEDISABLE: &[&str] = &[
    "CREATE TABLE trg_ed (pk int PRIMARY KEY, v int);",
    "CREATE TABLE trg_log (seq serial PRIMARY KEY, tag text);",
    "CREATE FUNCTION trg_edf() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN INSERT INTO trg_log(tag) VALUES (TG_NAME || ':' || NEW.pk::text); RETURN NEW; END $t$;",
    "CREATE TRIGGER trg_a AFTER INSERT ON trg_ed FOR EACH ROW EXECUTE FUNCTION trg_edf();",
    "CREATE TRIGGER trg_b AFTER INSERT ON trg_ed FOR EACH ROW EXECUTE FUNCTION trg_edf();",
    "INSERT INTO trg_ed VALUES (1, 1);",
    "ALTER TABLE trg_ed DISABLE TRIGGER trg_a;",
    "INSERT INTO trg_ed VALUES (2, 2);",
    "ALTER TABLE trg_ed ENABLE ALWAYS TRIGGER trg_a;",
    "INSERT INTO trg_ed VALUES (3, 3);",
    "ALTER TABLE trg_ed ENABLE REPLICA TRIGGER trg_b;",
    "INSERT INTO trg_ed VALUES (4, 4);",
    "ALTER TABLE trg_ed DISABLE TRIGGER USER;",
    "INSERT INTO trg_ed VALUES (5, 5);",
    "ALTER TABLE trg_ed ENABLE TRIGGER ALL;",
    "INSERT INTO trg_ed VALUES (6, 6);",
    "ALTER TABLE trg_ed DISABLE TRIGGER ALL;",
    "INSERT INTO trg_ed VALUES (7, 7);",
    "ALTER TABLE trg_ed ENABLE TRIGGER trg_a;",
    "ALTER TABLE trg_ed ENABLE TRIGGER trg_b;",
    "INSERT INTO trg_ed VALUES (8, 8);",
    "SELECT seq, tag FROM trg_log ORDER BY seq;",
    "SELECT pk, v FROM trg_ed ORDER BY pk;",
    "DROP TABLE trg_ed;",
    "DROP TABLE trg_log;",
    "DROP FUNCTION trg_edf();",
];

// session_replication_role interplay: an ENABLE REPLICA trigger fires only
// under role='replica'; an ENABLE ALWAYS trigger fires in both; a plain
// (origin) trigger is suppressed under 'replica'.
const REPLROLE: &[&str] = &[
    "CREATE TABLE trg_rr (pk int PRIMARY KEY, v int);",
    "CREATE TABLE trg_log (seq serial PRIMARY KEY, tag text);",
    "CREATE FUNCTION trg_rrf() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN INSERT INTO trg_log(tag) VALUES (TG_NAME || ':' || NEW.pk::text); RETURN NEW; END $t$;",
    "CREATE TRIGGER trg_origin AFTER INSERT ON trg_rr FOR EACH ROW EXECUTE FUNCTION trg_rrf();",
    "CREATE TRIGGER trg_replica AFTER INSERT ON trg_rr FOR EACH ROW EXECUTE FUNCTION trg_rrf();",
    "CREATE TRIGGER trg_always AFTER INSERT ON trg_rr FOR EACH ROW EXECUTE FUNCTION trg_rrf();",
    "ALTER TABLE trg_rr ENABLE REPLICA TRIGGER trg_replica;",
    "ALTER TABLE trg_rr ENABLE ALWAYS TRIGGER trg_always;",
    "INSERT INTO trg_rr VALUES (1, 1);",
    "SET session_replication_role = 'replica';",
    "INSERT INTO trg_rr VALUES (2, 2);",
    "SET session_replication_role = 'local';",
    "INSERT INTO trg_rr VALUES (3, 3);",
    "RESET session_replication_role;",
    "INSERT INTO trg_rr VALUES (4, 4);",
    "SELECT seq, tag FROM trg_log ORDER BY seq;",
    "SELECT pk, v FROM trg_rr ORDER BY pk;",
    "DROP TABLE trg_rr;",
    "DROP TABLE trg_log;",
    "DROP FUNCTION trg_rrf();",
];

// renametrig / renametrig_internal / renametrig_partition + RenameRewriteRule:
// ALTER TRIGGER ... RENAME on a plain table and on a partitioned table (the
// rename recurses to every leaf), then a catalog probe confirms identical
// renaming. ALTER RULE ... RENAME drains RenameRewriteRule.
const RENAME: &[&str] = &[
    "CREATE TABLE trg_rn (pk int PRIMARY KEY, v int);",
    "CREATE FUNCTION trg_rnf() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN RETURN NEW; END $t$;",
    "CREATE TRIGGER trg_before AFTER INSERT ON trg_rn FOR EACH ROW EXECUTE FUNCTION trg_rnf();",
    "ALTER TRIGGER trg_before ON trg_rn RENAME TO trg_after;",
    "CREATE TABLE trg_rp (pk int, v int) PARTITION BY RANGE (pk);",
    "CREATE TABLE trg_rp_a PARTITION OF trg_rp FOR VALUES FROM (0) TO (100);",
    "CREATE TABLE trg_rp_b PARTITION OF trg_rp FOR VALUES FROM (100) TO (200);",
    "CREATE TRIGGER trg_p AFTER INSERT ON trg_rp FOR EACH ROW EXECUTE FUNCTION trg_rnf();",
    "ALTER TRIGGER trg_p ON trg_rp RENAME TO trg_p_renamed;",
    "SELECT c.relname, t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE NOT t.tgisinternal AND c.relname LIKE 'trg_r%' ORDER BY c.relname, t.tgname;",
    "CREATE RULE trg_rule AS ON UPDATE TO trg_rn DO ALSO NOTHING;",
    "ALTER RULE trg_rule ON trg_rn RENAME TO trg_rule2;",
    "SELECT rulename FROM pg_rules WHERE tablename = 'trg_rn' ORDER BY rulename;",
    "DROP TABLE trg_rp;",
    "DROP TABLE trg_rn;",
    "DROP FUNCTION trg_rnf();",
];

// TriggerSetParentTrigger / CreateTriggerFiringOn: a row trigger on a
// partitioned parent is cloned to existing leaves, and a post-hoc ATTACH
// clones it to the new child. DML into each leaf fires the cloned trigger;
// the log witnesses the per-partition firing transcript.
const PARTED: &[&str] = &[
    "CREATE TABLE trg_pt (pk int, v int) PARTITION BY RANGE (pk);",
    "CREATE TABLE trg_pt_a PARTITION OF trg_pt FOR VALUES FROM (0) TO (100);",
    "CREATE TABLE trg_pt_b PARTITION OF trg_pt FOR VALUES FROM (100) TO (200);",
    "CREATE TABLE trg_log (seq serial PRIMARY KEY, tag text);",
    "CREATE FUNCTION trg_ptf() RETURNS trigger LANGUAGE plpgsql AS $t$ BEGIN INSERT INTO trg_log(tag) VALUES (TG_TABLE_NAME || ':' || TG_OP || ':' || NEW.pk::text); RETURN NEW; END $t$;",
    "CREATE TRIGGER trg_clone AFTER INSERT ON trg_pt FOR EACH ROW EXECUTE FUNCTION trg_ptf();",
    "INSERT INTO trg_pt VALUES (10, 1), (110, 2);",
    "CREATE TABLE trg_pt_c (pk int, v int);",
    "ALTER TABLE trg_pt ATTACH PARTITION trg_pt_c FOR VALUES FROM (200) TO (300);",
    "INSERT INTO trg_pt VALUES (210, 3);",
    "SELECT seq, tag FROM trg_log ORDER BY seq;",
    "SELECT c.relname, t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE NOT t.tgisinternal AND c.relname LIKE 'trg_pt%' ORDER BY c.relname, t.tgname;",
    "SELECT pk, v FROM trg_pt ORDER BY pk;",
    "DROP TABLE trg_pt;",
    "DROP TABLE trg_log;",
    "DROP FUNCTION trg_ptf();",
];

// Event trigger on ddl_command_end: a plpgsql handler loops
// pg_event_trigger_ddl_commands() and logs command_tag/object_type/identity.
// The measured DDL includes GRANT/REVOKE (stringify_grant_objtype /
// stringify_adefprivs_objtype) and COMMENT. Drains CreateEventTrigger,
// insert_event_trigger_tuple, EventTriggerInvoke, plpgsql_exec_event_trigger.
const EVENTDDL: &[&str] = &[
    "DROP EVENT TRIGGER IF EXISTS trg_evt;",
    "CREATE TABLE trg_log (seq serial PRIMARY KEY, kind text, tag text, ident text);",
    "CREATE FUNCTION trg_evtf() RETURNS event_trigger LANGUAGE plpgsql AS $t$ DECLARE r record; BEGIN FOR r IN SELECT command_tag, object_type, object_identity FROM pg_event_trigger_ddl_commands() ORDER BY object_identity, command_tag LOOP INSERT INTO trg_log(kind, tag, ident) VALUES (r.object_type, r.command_tag, r.object_identity); END LOOP; END $t$;",
    "CREATE EVENT TRIGGER trg_evt ON ddl_command_end EXECUTE FUNCTION trg_evtf();",
    "CREATE TABLE trg_ddl (a int PRIMARY KEY, b text);",
    "ALTER TABLE trg_ddl ADD COLUMN c int;",
    "CREATE INDEX trg_ddl_ix ON trg_ddl (b);",
    "COMMENT ON TABLE trg_ddl IS 'trg comment';",
    "GRANT SELECT ON trg_ddl TO PUBLIC;",
    "REVOKE SELECT ON trg_ddl FROM PUBLIC;",
    "ALTER EVENT TRIGGER trg_evt DISABLE;",
    "SELECT kind, tag, ident FROM trg_log ORDER BY seq;",
    "DROP EVENT TRIGGER trg_evt;",
    "DROP TABLE trg_ddl;",
    "DROP TABLE trg_log;",
    "DROP FUNCTION trg_evtf();",
];

// Event trigger on sql_drop: a plpgsql handler loops
// pg_event_trigger_dropped_objects() and logs object_type/schema/identity.
// Drains EventTriggerSQLDropAddObject, pg_event_trigger_dropped_objects,
// obtain_object_name_namespace.
const EVENTDROP: &[&str] = &[
    "DROP EVENT TRIGGER IF EXISTS trg_evd;",
    "CREATE TABLE trg_log (seq serial PRIMARY KEY, otype text, schema text, ident text);",
    "CREATE FUNCTION trg_evdf() RETURNS event_trigger LANGUAGE plpgsql AS $t$ DECLARE r record; BEGIN FOR r IN SELECT object_type, schema_name, object_identity FROM pg_event_trigger_dropped_objects() ORDER BY object_identity, object_type LOOP INSERT INTO trg_log(otype, schema, ident) VALUES (r.object_type, coalesce(r.schema_name, ''), r.object_identity); END LOOP; END $t$;",
    "CREATE EVENT TRIGGER trg_evd ON sql_drop EXECUTE FUNCTION trg_evdf();",
    "CREATE TABLE trg_drop (a int PRIMARY KEY, b text);",
    "CREATE INDEX trg_drop_ix ON trg_drop (b);",
    "CREATE VIEW trg_drop_v AS SELECT a FROM trg_drop;",
    "DROP VIEW trg_drop_v;",
    "DROP INDEX trg_drop_ix;",
    "DROP TABLE trg_drop;",
    "ALTER EVENT TRIGGER trg_evd DISABLE;",
    "SELECT otype, schema, ident FROM trg_log ORDER BY seq;",
    "DROP EVENT TRIGGER trg_evd;",
    "DROP TABLE trg_log;",
    "DROP FUNCTION trg_evdf();",
];

// EnableDisableRule / ATExecEnableDisableRule + RenameRewriteRule: a DO ALSO
// rule whose firing is toggled by ALTER TABLE ENABLE/DISABLE/ENABLE ALWAYS
// RULE and whose name is changed by ALTER RULE ... RENAME. The log witnesses
// which DML fired the rule under each state.
const RULES: &[&str] = &[
    "CREATE TABLE trg_ru (pk int PRIMARY KEY, v int);",
    "CREATE TABLE trg_log (seq serial PRIMARY KEY, tag text);",
    "CREATE RULE trg_lograte AS ON INSERT TO trg_ru DO ALSO INSERT INTO trg_log(tag) VALUES ('rule:' || NEW.pk::text);",
    "INSERT INTO trg_ru VALUES (1, 1);",
    "ALTER TABLE trg_ru DISABLE RULE trg_lograte;",
    "INSERT INTO trg_ru VALUES (2, 2);",
    "ALTER TABLE trg_ru ENABLE RULE trg_lograte;",
    "INSERT INTO trg_ru VALUES (3, 3);",
    "ALTER TABLE trg_ru ENABLE ALWAYS RULE trg_lograte;",
    "INSERT INTO trg_ru VALUES (4, 4);",
    "ALTER TABLE trg_ru ENABLE REPLICA RULE trg_lograte;",
    "INSERT INTO trg_ru VALUES (5, 5);",
    "ALTER TABLE trg_ru ENABLE RULE trg_lograte;",
    "ALTER RULE trg_lograte ON trg_ru RENAME TO trg_lograte2;",
    "INSERT INTO trg_ru VALUES (6, 6);",
    "SELECT seq, tag FROM trg_log ORDER BY seq;",
    "SELECT pk, v FROM trg_ru ORDER BY pk;",
    "SELECT rulename FROM pg_rules WHERE tablename = 'trg_ru' ORDER BY rulename;",
    "DROP TABLE trg_ru;",
    "DROP TABLE trg_log;",
];

// error_view_not_updatable / relation_is_updatable / findDefaultOnlyColumns:
// a non-auto-updatable view (DISTINCT) rejects INSERT/UPDATE/DELETE with the
// updatable-view error identity; an auto-updatable subset-column view accepts
// DML routed through the rewriter (findDefaultOnlyColumns for the omitted
// columns). Both engines must agree on both the SQLSTATE and the rewritten
// result.
const VIEWUPD: &[&str] = &[
    "CREATE TABLE trg_vb (pk int PRIMARY KEY, a int, b text, c int DEFAULT 7);",
    "INSERT INTO trg_vb VALUES (1, 10, 'x', 1), (2, 20, 'y', 2);",
    "CREATE VIEW trg_vd AS SELECT DISTINCT a, b FROM trg_vb;",
    "INSERT INTO trg_vd VALUES (30, 'z');",
    "UPDATE trg_vd SET b = 'q' WHERE a = 10;",
    "DELETE FROM trg_vd WHERE a = 20;",
    "CREATE VIEW trg_vu AS SELECT pk, a FROM trg_vb;",
    "INSERT INTO trg_vu (pk, a) VALUES (3, 30);",
    "UPDATE trg_vu SET a = a + 1 WHERE pk = 1;",
    "SELECT pk, a, b, c FROM trg_vb ORDER BY pk;",
    "SELECT relname, pg_relation_is_updatable(oid, true) AS flags FROM pg_class WHERE relname IN ('trg_vd', 'trg_vu') ORDER BY relname;",
    "DROP VIEW trg_vd;",
    "DROP VIEW trg_vu;",
    "DROP TABLE trg_vb;",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    #[test]
    fn every_shape_emits_balanced_terminated_singleline() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x7216);
        for _ in 0..400 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_triggers_module(&mut g);
            assert!(!stmts.is_empty());
            for s in &stmts {
                let sql = s.to_sql();
                assert!(!sql.contains('\n'), "multiline: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
        }
    }

    #[test]
    fn all_sections_reachable_and_self_dropping() {
        // Each section that creates an event trigger must drop it; each
        // section that sets session_replication_role must reset it.
        for pool in [
            SETCONSTR, ENABLEDISABLE, REPLROLE, RENAME, PARTED, EVENTDDL, EVENTDROP, RULES, VIEWUPD,
        ] {
            let joined = pool.join(" ");
            if joined.contains("CREATE EVENT TRIGGER") {
                assert!(
                    joined.contains("DROP EVENT TRIGGER trg_"),
                    "event-trigger section must drop its trigger: {joined}"
                );
            }
            if joined.contains("SET session_replication_role") {
                assert!(
                    joined.contains("RESET session_replication_role"),
                    "replrole section must reset the GUC"
                );
            }
        }
    }
}
