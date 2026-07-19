//! Plan-validity coherence gate (review BLOCKING-1, 2026-07-17): replay every
//! generated plan against a reference PostgreSQL transaction model and reject
//! any statement that would deterministically error on a live server.
//!
//! The specific bug class this pins: DDL and (non-LOCAL) SET are TRANSACTIONAL
//! in PostgreSQL — ROLLBACK / ROLLBACK TO SAVEPOINT / a disconnect that aborts
//! an open tx revert them on the server. A generator whose schema model keeps
//! rolled-back DDL produces plans whose tails address tables the server rolled
//! away (42P01 storms, 25P02 poisoning, 3B001 phantom savepoints). The
//! reference model here applies real tx semantics (snapshot at BEGIN /
//! SAVEPOINT, restore on ROLLBACK / ROLLBACK TO / aborting disconnect) and
//! asserts:
//!   - every table referenced by DDL/DML/queries exists at execution point,
//!     and CREATE TABLE / CREATE INDEX / RENAME targets never collide;
//!   - tx control is well-formed (no nested BEGIN, no COMMIT/ROLLBACK outside
//!     a tx, ROLLBACK TO only to a live savepoint — the 3B001 guard);
//!   - reserved fault tags are never emitted;
//!   - the plan ends outside a tx with no GUC left set (GUC-leak law).
//!
//! Sweep covers the review's traced failures (seed 42020/default, seed
//! 42105/float-lenient) inside 42000..42400 x all 6 profiles.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use simharness::gen::generate_plan;
use simharness::gen::profile::GenProfile;
use simharness::plan::{
    ArmCtl, FaultPoint, Mark, Plan, PlanItem, Sql, SqlFlags, Step, TxCtl,
};

fn profile_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/gen_profiles")
}

/// Server-visible state that transactions snapshot/restore.
#[derive(Clone, Default)]
struct Db {
    tables: BTreeSet<String>,
    indexes: BTreeSet<String>,
    /// H6: file_fdw foreign tables (read-only relations; SELECT-able only).
    foreign_tables: BTreeSet<String>,
    guc_set: bool,
}

#[derive(Default)]
struct RefServer {
    db: Db,
    in_tx: bool,
    tx_base: Option<Db>,
    savepoints: Vec<(String, Db)>,
}

/// First identifier token starting at `rest` (ends at space, '(', ')', ','
/// or ';' — ')' and ',' close H6 subquery/CTE bodies, e.g.
/// `... (SELECT id FROM t1)`).
fn ident_prefix(rest: &str) -> Result<&str, String> {
    let end = rest
        .find(|c: char| c == ' ' || c == '(' || c == ')' || c == ',' || c == ';')
        .ok_or_else(|| format!("cannot find identifier end in '{rest}'"))?;
    let id = &rest[..end];
    if id.is_empty() {
        return Err(format!("empty identifier in '{rest}'"));
    }
    Ok(id)
}

fn after<'a>(text: &'a str, kw: &str) -> Result<&'a str, String> {
    let i = text.find(kw).ok_or_else(|| format!("expected '{kw}' in '{text}'"))?;
    Ok(&text[i + kw.len()..])
}

impl RefServer {
    fn require_table(&self, name: &str, text: &str) -> Result<(), String> {
        if self.db.tables.contains(name) {
            Ok(())
        } else {
            Err(format!("42P01: relation '{name}' does not exist at: {text}"))
        }
    }

    fn require_readable(&self, name: &str, text: &str) -> Result<(), String> {
        if self.db.tables.contains(name) || self.db.foreign_tables.contains(name) {
            Ok(())
        } else {
            Err(format!("42P01: relation '{name}' does not exist at: {text}"))
        }
    }

    fn apply_sql(&mut self, text: &str) -> Result<(), String> {
        if let Ok(rest) = after(text, "CREATE FOREIGN TABLE ") {
            // H6 fdw chain tail. Transactional like all DDL; SELECT-only.
            let name = ident_prefix(rest)?;
            if !self.db.foreign_tables.insert(name.to_string()) {
                return Err(format!("42P07: foreign table '{name}' already exists at: {text}"));
            }
        } else if text.starts_with("CREATE EXTENSION IF NOT EXISTS ")
            || text.starts_with("CREATE SERVER IF NOT EXISTS ")
        {
            // Idempotent database-global fdw setup (H6); nothing to model.
        } else if text.starts_with("COPY (SELECT ") {
            // H6 deterministic CSV writer; references no user relation.
        } else if text.starts_with("ANALYZE ") {
            let name = ident_prefix(after(text, "ANALYZE ")?)?;
            self.require_table(name, text)?;
        } else if let Ok(rest) = after(text, "DROP INDEX ") {
            let iname = ident_prefix(rest)?;
            if !self.db.indexes.remove(iname) {
                return Err(format!("42P01: index '{iname}' does not exist at: {text}"));
            }
        } else if let Ok(rest) = after(text, "CREATE TABLE ") {
            let name = ident_prefix(rest)?;
            if !self.db.tables.insert(name.to_string()) {
                return Err(format!("42P07: relation '{name}' already exists at: {text}"));
            }
        } else if let Ok(rest) = after(text, "CREATE INDEX ") {
            let iname = ident_prefix(rest)?;
            let tname = ident_prefix(after(rest, " ON ")?)?;
            self.require_table(tname, text)?;
            if !self.db.indexes.insert(iname.to_string()) {
                return Err(format!("42P07: index '{iname}' already exists at: {text}"));
            }
        } else if let Ok(rest) = after(text, "ALTER TABLE ") {
            let old = ident_prefix(rest)?;
            let new = ident_prefix(after(rest, " RENAME TO ")?)?;
            self.require_table(old, text)?;
            if self.db.tables.contains(new) {
                return Err(format!("42P07: rename target '{new}' exists at: {text}"));
            }
            self.db.tables.remove(old);
            self.db.tables.insert(new.to_string());
        } else if let Ok(rest) = after(text, "DROP TABLE ") {
            let name = ident_prefix(rest)?;
            self.require_table(name, text)?;
            self.db.tables.remove(name);
        } else if text.starts_with("MERGE INTO ") {
            // H6 dml:merge — key-addressed MERGE; only the target must exist
            // (the USING source is a constant VALUES list). Checked BEFORE
            // the substring-matched UPDATE/DELETE branches: the WHEN MATCHED
            // action text contains "UPDATE SET".
            let rest = after(text, "MERGE INTO ")?;
            self.require_table(ident_prefix(rest)?, text)?;
        } else if let Ok(rest) = after(text, "INSERT INTO ") {
            self.require_table(ident_prefix(rest)?, text)?;
        } else if let Ok(rest) = after(text, "UPDATE ") {
            self.require_table(ident_prefix(rest)?, text)?;
        } else if let Ok(rest) = after(text, "DELETE FROM ") {
            self.require_table(ident_prefix(rest)?, text)?;
        } else if let Ok(rest) = after(text, "TRUNCATE ") {
            self.require_table(ident_prefix(rest)?, text)?;
        } else if text.starts_with("SELECT ") || text.starts_with("WITH ") {
            // Every FROM/JOIN relation target must exist. SRF calls
            // (unnest/generate_series — H4 function-call grammar),
            // json_table (H6 table-function grammar), and pg_catalog
            // relations always exist; subquery parens recurse via their own
            // inner FROM occurrence; CTE names (H6 WITH grammar) are
            // statement-local relations.
            let mut cte_names: BTreeSet<String> = BTreeSet::new();
            if let Ok(rest) = after(text, "WITH ") {
                // Our grammar emits exactly one CTE per statement:
                // `WITH [RECURSIVE] <name>[(cols)] AS ...`.
                let rest = rest.strip_prefix("RECURSIVE ").unwrap_or(rest);
                cte_names.insert(ident_prefix(rest)?.to_string());
            }
            let mut checked_any = false;
            for kw in [" FROM ", " JOIN "] {
                let mut hay = text;
                while let Ok(rest) = after(hay, kw) {
                    if !rest.starts_with('(') {
                        let id = ident_prefix(rest)?;
                        if id != "unnest"
                            && id != "generate_series"
                            && id != "json_table"
                            && !id.starts_with("pg_")
                            && !cte_names.contains(id)
                        {
                            // H6-state: foreign tables are readable relations
                            // too, so route through require_readable.
                            self.require_readable(id, text)?;
                        }
                        checked_any = true;
                    } else {
                        checked_any = true;
                    }
                    hay = rest;
                }
            }
            // H6 res:no-from — a FROM-less constant SELECT is valid; only a
            // statement that HAS a FROM the scanner failed to check is a
            // parse-model gap.
            if !checked_any && text.contains(" FROM ") {
                return Err(format!("SELECT with unparsed FROM target: {text}"));
            }
        } else {
            return Err(format!("unrecognized statement shape: {text}"));
        }
        Ok(())
    }

    fn apply_step(&mut self, step: &Step) -> Result<(), String> {
        match step {
            Step::Ddl(s) | Step::Dml(s) | Step::Query(s) => self.apply_sql(s.text()),
            Step::Tx(TxCtl::Begin(_)) => {
                if self.in_tx {
                    return Err("BEGIN inside an open tx (nested BEGIN warning)".into());
                }
                self.tx_base = Some(self.db.clone());
                self.in_tx = true;
                Ok(())
            }
            Step::Tx(TxCtl::Commit) => {
                if !self.in_tx {
                    return Err("COMMIT outside a tx".into());
                }
                self.in_tx = false;
                self.tx_base = None;
                self.savepoints.clear();
                Ok(())
            }
            Step::Tx(TxCtl::Rollback) => {
                if !self.in_tx {
                    return Err("ROLLBACK outside a tx".into());
                }
                self.db = self.tx_base.take().expect("in_tx implies base");
                self.in_tx = false;
                self.savepoints.clear();
                Ok(())
            }
            Step::Tx(TxCtl::Savepoint(n)) => {
                if !self.in_tx {
                    return Err(format!("SAVEPOINT {n} outside a tx (25P01)"));
                }
                self.savepoints.push((n.clone(), self.db.clone()));
                Ok(())
            }
            Step::Tx(TxCtl::RollbackTo(n)) => {
                if !self.in_tx {
                    return Err(format!("ROLLBACK TO {n} outside a tx"));
                }
                let Some(i) = self.savepoints.iter().rposition(|(sn, _)| sn == n) else {
                    return Err(format!("3B001: no such savepoint '{n}'"));
                };
                self.db = self.savepoints[i].1.clone();
                self.savepoints.truncate(i + 1);
                Ok(())
            }
            Step::Arm(ArmCtl::SetGuc(_, _)) => {
                self.db.guc_set = true;
                Ok(())
            }
            Step::Arm(ArmCtl::ResetAll) => {
                self.db.guc_set = false;
                Ok(())
            }
            Step::Fault(FaultPoint::Disconnect) => {
                // Server aborts any open tx (DDL rolls back with it); the
                // fresh session after reconnect has no GUCs set.
                if self.in_tx {
                    self.db = self.tx_base.take().expect("in_tx implies base");
                    self.in_tx = false;
                    self.savepoints.clear();
                }
                self.db.guc_set = false;
                Ok(())
            }
            Step::Fault(FaultPoint::ReconnectServer) => Ok(()),
            Step::Fault(f) => Err(format!("reserved fault tag emitted by generator: {f:?}")),
            Step::Assumption(_) | Step::Assertion(_) => Ok(()),
            // H8 session-family steps: property-internal choreography. The
            // single-session coherence model does not track cross-session
            // state; steps are structurally valid by property construction.
            Step::Session(_) | Step::AsyncDml(_) | Step::Join(_) | Step::WaitUntil(_) => Ok(()),
        }
    }
}

fn replay(plan: &Plan) -> Result<(), String> {
    let mut srv = RefServer::default();
    let mut step_no = 0usize;
    let mut go = |step: &Step| -> Result<(), String> {
        step_no += 1;
        srv.apply_step(step).map_err(|e| format!("step {step_no}: {e}"))
    };
    for item in &plan.items {
        match item {
            PlanItem::Step(s) => go(s)?,
            PlanItem::Property { steps, .. } => {
                for s in steps {
                    go(s)?;
                }
            }
        }
    }
    if srv.in_tx {
        return Err("plan ends inside an open tx (cleanup tail missing)".into());
    }
    if srv.db.guc_set {
        return Err("plan ends with a GUC set (GUC-leak law)".into());
    }
    Ok(())
}

/// The sweep: every plan a live server would see must replay error-free.
#[test]
fn generated_plans_are_tx_coherent_400_seeds_all_profiles() {
    let mut profiles: Vec<PathBuf> = fs::read_dir(profile_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    profiles.sort();
    assert_eq!(profiles.len(), 7, "expected 7 gen profiles");
    let mut plans = 0u64;
    for ppath in &profiles {
        let bytes = fs::read(ppath).unwrap();
        let (profile, sha) = GenProfile::from_bytes(&bytes).unwrap();
        // Covers the review's traced seeds 42020 (default) and 42105
        // (float-lenient).
        for seed in 42000..42400u64 {
            let plan = generate_plan(seed, &profile, &sha, "cohtest", &[]);
            if let Err(e) = replay(&plan) {
                panic!(
                    "plan incoherent: profile={} seed={seed}: {e}\n--- plan ---\n{}",
                    profile.name,
                    simharness::plan::render(&plan)
                );
            }
            plans += 1;
        }
    }
    assert_eq!(plans, 2800);
}

// ---------------------------------------------------------------------------
// Red battery: the reference model must itself catch each bug-class shape
// (test-the-test; these are the exact signatures from the review trace).
// ---------------------------------------------------------------------------

fn ddl(text: &str) -> Step {
    Step::Ddl(Sql::new(text, Mark::Mutation, SqlFlags::default()).unwrap())
}

fn query(text: &str) -> Step {
    Step::Query(Sql::new(text, Mark::Read, SqlFlags::default()).unwrap())
}

fn replay_steps(steps: Vec<Step>) -> Result<(), String> {
    let mut srv = RefServer::default();
    for s in &steps {
        srv.apply_step(s)?;
    }
    Ok(())
}

#[test]
fn red_rename_in_rolled_back_tx_is_caught() {
    // The seed-42020 signature: RENAME inside a tx, disconnect-free ROLLBACK,
    // then the old model would keep addressing the rolled-back name.
    let err = replay_steps(vec![
        ddl("CREATE TABLE t1 (id bigint PRIMARY KEY);"),
        Step::Tx(TxCtl::Begin(simharness::plan::IsoLevel::ReadCommitted)),
        ddl("ALTER TABLE t1 RENAME TO t1_r1;"),
        Step::Tx(TxCtl::Rollback),
        query("SELECT count(*) FROM t1_r1;"),
    ])
    .unwrap_err();
    assert!(err.contains("42P01"), "expected 42P01, got: {err}");
}

#[test]
fn red_ddl_lost_to_aborting_disconnect_is_caught() {
    // The traced live failure: DDL inside an open tx, then `-- FAULT
    // disconnect` aborts the tx and the server reverts the DDL.
    let err = replay_steps(vec![
        ddl("CREATE TABLE t1 (id bigint PRIMARY KEY);"),
        Step::Tx(TxCtl::Begin(simharness::plan::IsoLevel::ReadCommitted)),
        ddl("CREATE TABLE t2 (id bigint PRIMARY KEY);"),
        Step::Fault(FaultPoint::Disconnect),
        Step::Fault(FaultPoint::ReconnectServer),
        query("SELECT count(*) FROM t2;"),
    ])
    .unwrap_err();
    assert!(err.contains("42P01"), "expected 42P01, got: {err}");
}

#[test]
fn red_phantom_savepoint_is_caught() {
    // The 3B001 signature: ROLLBACK TO a savepoint that does not exist.
    let err = replay_steps(vec![
        ddl("CREATE TABLE t1 (id bigint PRIMARY KEY);"),
        Step::Tx(TxCtl::Begin(simharness::plan::IsoLevel::ReadCommitted)),
        Step::Tx(TxCtl::Savepoint("sp1".into())),
        Step::Tx(TxCtl::RollbackTo("sp2".into())),
    ])
    .unwrap_err();
    assert!(err.contains("3B001"), "expected 3B001, got: {err}");
}

#[test]
fn rollback_to_savepoint_restores_and_keeps_savepoint() {
    // Green sanity for the model itself: ROLLBACK TO reverts DDL made after
    // the savepoint, keeps the savepoint live, and commit preserves the rest.
    let steps = vec![
        ddl("CREATE TABLE t1 (id bigint PRIMARY KEY);"),
        Step::Tx(TxCtl::Begin(simharness::plan::IsoLevel::ReadCommitted)),
        Step::Tx(TxCtl::Savepoint("sp1".into())),
        ddl("CREATE TABLE t2 (id bigint PRIMARY KEY);"),
        Step::Tx(TxCtl::RollbackTo("sp1".into())),
        query("SELECT count(*) FROM t1;"),
        Step::Tx(TxCtl::RollbackTo("sp1".into())), // savepoint still live
        Step::Tx(TxCtl::Commit),
    ];
    replay_steps(steps).unwrap();
    // ...and t2 must NOT be addressable afterwards.
    let err = replay_steps(vec![
        ddl("CREATE TABLE t1 (id bigint PRIMARY KEY);"),
        Step::Tx(TxCtl::Begin(simharness::plan::IsoLevel::ReadCommitted)),
        Step::Tx(TxCtl::Savepoint("sp1".into())),
        ddl("CREATE TABLE t2 (id bigint PRIMARY KEY);"),
        Step::Tx(TxCtl::RollbackTo("sp1".into())),
        Step::Tx(TxCtl::Commit),
        query("SELECT count(*) FROM t2;"),
    ])
    .unwrap_err();
    assert!(err.contains("42P01"), "expected 42P01, got: {err}");
}
