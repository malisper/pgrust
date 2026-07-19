//! Session driver + mark-honoring dispatch (contract §4.1.1).
//!
//! - READ statements are compared (in `--diff-c`), MUTATIONs execute exactly
//!   once per engine (double execution REFUSED — dualexec mutation-split
//!   law), PASSTHROUGH executes uncompared (both engines in diff-c so state
//!   stays lockstep).
//! - `Fault(Disconnect | ReconnectServer)` executes; reserved fault tags
//!   (Crash/TornWrite/Env) REFUSE at execute time (H4 — contract §1.5).
//! - Mutation-outcome divergence in diff-c ⇒ HALT (state forked; halt index
//!   recorded) — the dualexec --cpg law, kept verbatim. TX/ARM divergence
//!   also HALTs (session state forked).
//!
//! Checker semantics and the full classification ladder are WS-ORACLE's
//! (`src/oracle/`); this module calls through the `CheckEval` and
//! `DiffClassifier` hook traits and ships basic scaffold impls only.

use super::planface::{ArmCtl, FaultPoint, IsoLevel, Mark, Plan, Sql, Step, TxCtl};
use std::collections::BTreeMap;

// ---------------------------------------------------------------- sessions

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOutcome {
    Rows { rows: Vec<Vec<Option<String>>> },
    Command { tag: String },
    SqlError { sqlstate: String, message: String },
    /// Server went away / client-side failure. In the pinned vocabulary a
    /// fetch-side client exception is `harness-fetch` (P2), NOT a crash;
    /// only a dead server is `rust-crash`/`c-crash`.
    ConnectionLost { message: String },
}

impl ExecOutcome {
    pub fn is_error(&self) -> bool {
        matches!(self, ExecOutcome::SqlError { .. } | ExecOutcome::ConnectionLost { .. })
    }
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            ExecOutcome::SqlError { sqlstate, .. } => Some(sqlstate),
            _ => None,
        }
    }
}

pub trait Session {
    fn engine(&self) -> &str;
    fn execute(&mut self, sql: &str) -> ExecOutcome;
    fn reconnect(&mut self) -> Result<(), String>;
}

/// Live PG-wire session over the `postgres` crate (simple query protocol —
/// text-format results, right for byte comparison).
pub struct PgSession {
    engine: String,
    conninfo: String,
    session_setup: Vec<String>,
    client: Option<postgres::Client>,
}

impl PgSession {
    pub fn connect(engine: &str, conninfo: &str, session_setup: &[String]) -> Result<Self, String> {
        let mut s = PgSession {
            engine: engine.to_string(),
            conninfo: conninfo.to_string(),
            session_setup: session_setup.to_vec(),
            client: None,
        };
        s.reconnect()?;
        Ok(s)
    }

    /// Wire cancel token for this connection (H8 worker teardown hygiene:
    /// a blocked worker statement is cancelled, never orphaned).
    pub fn cancel_token(&self) -> Option<postgres::CancelToken> {
        self.client.as_ref().map(|c| c.cancel_token())
    }
}

impl Session for PgSession {
    fn engine(&self) -> &str {
        &self.engine
    }

    fn execute(&mut self, sql: &str) -> ExecOutcome {
        let client = match self.client.as_mut() {
            Some(c) => c,
            None => {
                return ExecOutcome::ConnectionLost { message: "not connected".into() };
            }
        };
        match client.simple_query(sql) {
            Ok(msgs) => {
                let mut rows: Vec<Vec<Option<String>>> = Vec::new();
                let mut tag = String::new();
                let mut saw_rows = false;
                for m in msgs {
                    match m {
                        postgres::SimpleQueryMessage::Row(r) => {
                            saw_rows = true;
                            let mut row = Vec::with_capacity(r.len());
                            for i in 0..r.len() {
                                row.push(r.get(i).map(|v| v.to_string()));
                            }
                            rows.push(row);
                        }
                        // A RowDescription marks a row-returning statement
                        // even when ZERO rows follow: an empty SELECT must be
                        // Rows{[]} — never Command (an empty TLP/NoREC
                        // partition arm is a routine, legitimate result).
                        postgres::SimpleQueryMessage::RowDescription(_) => {
                            saw_rows = true;
                        }
                        postgres::SimpleQueryMessage::CommandComplete(n) => {
                            tag = n.to_string();
                        }
                        _ => {}
                    }
                }
                if saw_rows {
                    ExecOutcome::Rows { rows }
                } else {
                    ExecOutcome::Command { tag }
                }
            }
            Err(e) => {
                if let Some(db) = e.as_db_error() {
                    // NOTE: no recovery ROLLBACK here. Recovering only the
                    // erroring leg forks diff-c state (the other leg's open
                    // tx keeps running) — recovery is the DISPATCHER's job,
                    // symmetric across legs (Dispatcher::recover_both).
                    ExecOutcome::SqlError {
                        sqlstate: db.code().code().to_string(),
                        message: db.message().to_string(),
                    }
                } else if e.is_closed() {
                    self.client = None;
                    ExecOutcome::ConnectionLost { message: e.to_string() }
                } else {
                    // Client-side (fetch) failure: harness-fetch class territory.
                    ExecOutcome::ConnectionLost { message: format!("client: {}", e) }
                }
            }
        }
    }

    fn reconnect(&mut self) -> Result<(), String> {
        self.client = None;
        let client = postgres::Client::connect(&self.conninfo, postgres::NoTls)
            .map_err(|e| format!("{}: connect: {}", self.engine, e))?;
        self.client = Some(client);
        for s in self.session_setup.clone() {
            if let ExecOutcome::SqlError { sqlstate, message } = self.execute(&s) {
                return Err(format!("{}: session setup '{}': {} {}", self.engine, s, sqlstate, message));
            }
        }
        Ok(())
    }
}

/// TEETH INSTRUMENT (metamorphic-oracle validation only — never in battery
/// configs): a synthetic wrong-DUT that mis-evaluates NULL predicates in
/// WHERE clauses. Every `IS NULL` AFTER the first `WHERE` of a SELECT is
/// rewritten to `IS NULL AND false` before reaching the wrapped session —
/// modeling an engine whose filter-side IS NULL evaluation is broken
/// ("predicates are never NULL in a WHERE") while projection-side evaluation
/// stays correct. A correct TLP oracle MUST fire on this (the `(p) IS NULL`
/// partition arm loses its rows while the whole query keeps them), and a
/// correct NoREC oracle MUST fire when the predicate carries an IS NULL leaf
/// (optimized WHERE loses rows; the non-optimizable projection keeps them) —
/// exactly the filter/projection asymmetry NoREC exists to catch. Enabled by
/// `--test-null-bug`.
pub struct NullBugShim<S: Session> {
    pub inner: S,
}

impl<S: Session> Session for NullBugShim<S> {
    fn engine(&self) -> &str {
        self.inner.engine()
    }

    fn execute(&mut self, sql: &str) -> ExecOutcome {
        let head = sql.trim_start();
        let is_select = head.get(..7).is_some_and(|h| h.eq_ignore_ascii_case("SELECT "));
        if is_select {
            if let Some(pos) = sql.find(" WHERE ") {
                let (pre, tail) = sql.split_at(pos);
                if tail.contains(" IS NULL") {
                    let doctored =
                        format!("{pre}{}", tail.replace(" IS NULL", " IS NULL AND false"));
                    return self.inner.execute(&doctored);
                }
            }
        }
        self.inner.execute(sql)
    }

    fn reconnect(&mut self) -> Result<(), String> {
        self.inner.reconnect()
    }
}

// ------------------------------------------------------------------- hooks

pub struct ResultStack {
    stack: Vec<ExecOutcome>,
}

impl ResultStack {
    pub fn new() -> Self {
        ResultStack { stack: Vec::new() }
    }
    pub fn push(&mut self, o: ExecOutcome) {
        self.stack.push(o);
    }
    pub fn top(&self) -> Option<&ExecOutcome> {
        self.stack.last()
    }
}

pub enum CheckVerdict {
    Pass,
    Fail(String),
    Skip(String),
}

/// WS-ORACLE owns real check semantics; the runner calls through this trait.
/// Post-integration the property hooks feed the oracle's ledger + slot stack
/// (`bridge::OracleCheckEval`); implementations without model state keep the
/// default no-op hooks.
pub trait CheckEval {
    fn eval(&self, check_json: &str, stack: &ResultStack) -> CheckVerdict;

    /// A `-- begin property` marker was reached.
    fn on_property_begin(&self, _seq: u32) {}

    /// The matching `-- end property` marker was reached.
    fn on_property_end(&self, _seq: u32) {}

    /// An executed Sql/Tx/Arm step INSIDE a property block, with the DUT
    /// outcome. Returns a violation detail when the model reconciliation
    /// diverges (engine-ok/model-errs or the converse — both directions).
    fn on_step_outcome(&self, _outcome: &ExecOutcome) -> Option<String> {
        None
    }
}

/// SCAFFOLD checker: rowcount ops against the top of the result stack.
/// Replaced by WS-ORACLE's checker at integration.
pub struct BasicCheckEval;

impl CheckEval for BasicCheckEval {
    fn eval(&self, check_json: &str, stack: &ResultStack) -> CheckVerdict {
        let v: serde_json::Value = match serde_json::from_str(check_json) {
            Ok(v) => v,
            Err(e) => return CheckVerdict::Fail(format!("bad check json: {}", e)),
        };
        let op = v.get("op").and_then(|o| o.as_str()).unwrap_or("");
        let want = v.get("value").and_then(|o| o.as_i64()).unwrap_or(-1);
        let got = match stack.top() {
            Some(ExecOutcome::Rows { rows }) => rows.len() as i64,
            Some(ExecOutcome::Command { .. }) => 0,
            Some(_) => return CheckVerdict::Skip("top of stack is an error".into()),
            None => return CheckVerdict::Skip("empty result stack".into()),
        };
        let pass = match op {
            "rowcount-eq" => got == want,
            "rowcount-ge" => got >= want,
            "rowcount-le" => got <= want,
            other => return CheckVerdict::Fail(format!("unknown check op '{}'", other)),
        };
        if pass {
            CheckVerdict::Pass
        } else {
            CheckVerdict::Fail(format!("{} want {} got {}", op, want, got))
        }
    }
}

/// No-context checker: every check is a counted skip. Used when a plan is
/// executed WITHOUT its seed-regenerated oracle context (`test -b`, `loop`) —
/// a check that cannot be evaluated must never mint a verdict either way.
pub struct SkipAllCheckEval;

impl CheckEval for SkipAllCheckEval {
    fn eval(&self, _check_json: &str, _stack: &ResultStack) -> CheckVerdict {
        CheckVerdict::Skip("no-oracle-ctx".into())
    }
}

/// WS-ORACLE owns the real ladder (byte → semantic under the §1.3 vocabulary
/// + warts.toml). The runner calls through this trait.
pub trait DiffClassifier {
    /// -> (class, severity) per the pinned triage.py vocabulary.
    fn classify_read(&self, sql: &Sql, dut: &ExecOutcome, cpg: &ExecOutcome) -> (String, String);
}

/// SCAFFOLD classifier: byte compare, sort-normalized fallback for
/// order-underdetermined reads, SQLSTATE-class compare for errors. Class
/// names are the pinned §1.3 vocabulary — never invented.
pub struct BasicDiffClassifier;

impl DiffClassifier for BasicDiffClassifier {
    fn classify_read(&self, sql: &Sql, dut: &ExecOutcome, cpg: &ExecOutcome) -> (String, String) {
        use ExecOutcome::*;
        match (dut, cpg) {
            (ConnectionLost { .. }, _) => ("rust-crash".into(), "P1".into()),
            (_, ConnectionLost { .. }) => ("c-crash".into(), "P1".into()),
            (SqlError { sqlstate: rs, .. }, SqlError { sqlstate: cs, .. }) => {
                if rs.get(..2) == cs.get(..2) {
                    if rs == cs {
                        ("err-match".into(), "fine".into())
                    } else {
                        ("err-text-drift".into(), "P3".into())
                    }
                } else if rs == "57014" || cs == "57014" {
                    ("err-timeout-one-side".into(), "P3".into())
                } else {
                    ("err-state-mismatch".into(), "P2".into())
                }
            }
            (SqlError { sqlstate, message }, _) => {
                if sqlstate == "XX000" && message.contains("not ported") {
                    ("rust-err-c-ok-coverage".into(), "fine".into())
                } else {
                    ("rust-err-c-ok".into(), "P2".into())
                }
            }
            (_, SqlError { .. }) => ("c-err-rust-ok".into(), "P2".into()),
            (Rows { rows: r }, Rows { rows: c }) => {
                if r == c {
                    return ("ok".into(), "fine".into());
                }
                if sql.meta.order_underdetermined {
                    let mut rs = r.clone();
                    let mut cs = c.clone();
                    rs.sort();
                    cs.sort();
                    if rs == cs {
                        return ("ok".into(), "fine".into());
                    }
                }
                ("wrong-results".into(), "P1".into())
            }
            (Command { .. }, Command { .. }) => ("ok".into(), "fine".into()),
            _ => ("wrong-results".into(), "P1".into()),
        }
    }
}

// ---------------------------------------------------------------- dispatch

/// Failure signature (contract §4.1.3): SQLSTATE + class + normalized site —
/// never raw error text vs C; raw-string equality only for same-engine panics.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Signature {
    pub class: String,
    pub sqlstate: String,
    pub site: String,
}

impl Signature {
    pub fn to_key(&self) -> String {
        format!("{}|{}|{}", self.class, self.sqlstate, self.site)
    }
}

/// Normalized site: whitespace-collapsed statement head with numeric and
/// string literals blanked (stable across seeds).
pub fn normalize_site(stmt: &str) -> String {
    let mut out = String::new();
    let mut in_str = false;
    let mut last_space = false;
    for ch in stmt.chars() {
        if in_str {
            if ch == '\'' {
                in_str = false;
            }
            continue;
        }
        match ch {
            '\'' => {
                in_str = true;
                out.push('$');
            }
            '0'..='9' => {
                if !out.ends_with('#') {
                    out.push('#');
                }
            }
            c if c.is_whitespace() => {
                if !last_space {
                    out.push(' ');
                }
                last_space = true;
                continue;
            }
            c => out.push(c),
        }
        last_space = false;
    }
    // Byte cap on a char boundary (a multibyte literal from the real WS-GEN
    // generator must not panic the harness).
    if out.len() > 80 {
        let mut end = 80;
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
    }
    out
}

#[derive(Debug, Clone)]
pub struct StepRecord {
    pub idx: usize,
    pub class: String,
    pub sev: String,
    pub detail: String,
    pub stmt_head: String,
}

#[derive(Debug, Clone)]
pub struct Failure {
    pub step_idx: usize,
    pub signature: Signature,
    pub class: String,
    pub sev: String,
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub struct RunReport {
    pub records: Vec<StepRecord>,
    pub class_counts: BTreeMap<String, u64>,
    pub failure: Option<Failure>,
    pub halted_at: Option<usize>,
    pub steps_executed: usize,
    /// H5 rung B: canonicalized plan fingerprints of sampled Query steps
    /// (EXPLAIN (COSTS OFF) against the DUT; `ExecOptions::explain_every`).
    pub plan_fingerprints: Vec<String>,
    /// EXPLAIN statements that errored — counted here for metrics.json,
    /// deliberately NEVER in `class_counts` (the pinned census vocabulary
    /// must not see metrics-infrastructure statements).
    pub explain_errors: u64,
    /// First few EXPLAIN error signatures (sqlstate + head), for metrics
    /// diagnosability (a pgrust EXPLAIN coverage gap is itself a finding).
    pub explain_error_samples: Vec<String>,
}

impl RunReport {
    fn count(&mut self, class: &str) {
        *self.class_counts.entry(class.to_string()).or_insert(0) += 1;
    }
}

pub struct ExecOptions {
    /// Shell command that restarts the DUT server (for Fault ReconnectServer).
    pub restart_cmd: Option<String>,
    /// Stop on first failure (replay/shrink use this).
    pub stop_on_failure: bool,
    /// Harness session statements replayed on BOTH legs after any
    /// `ARM reset-all` (RESET ALL reverts session GUCs to connection
    /// defaults, which nukes the harness's `SET search_path` — the find-1
    /// FP class). Symmetric, uncounted infrastructure statements.
    pub post_reset_sql: Vec<String>,
    /// H2 seam: reserved fault tags (Crash/TornWrite/Env) dispatch through
    /// this driver instead of refusing inline. The NoOp default preserves
    /// H1's `fault-reserved-refused` P1 on the main lineage; the
    /// SimVfs-backed driver lights up at t28 (runner::faultdriver doc).
    pub fault_driver: Box<dyn super::faultdriver::FaultDriver>,
    /// H5 rung B: fingerprint every Nth successfully-executed Query step via
    /// `EXPLAIN (COSTS OFF)` on the DUT session (0 = off). DUT-only and
    /// state-neutral; an EXPLAIN error triggers the symmetric resync
    /// (`recover_both`) so it can never fork diff-c state.
    pub explain_every: u32,
    /// H8: connection recipe for worker sessions (multi-session plans).
    /// None => any session-family step is a `session-refusal` P1.
    pub session_pool: Option<super::sessions::SessionPoolConfig>,
}

impl Default for ExecOptions {
    fn default() -> Self {
        ExecOptions {
            restart_cmd: None,
            stop_on_failure: true,
            post_reset_sql: Vec::new(),
            fault_driver: Box::new(super::faultdriver::NoOpFaultDriver),
            explain_every: 0,
            session_pool: None,
        }
    }
}

/// Mark-honoring dispatcher. Tracks executed MUTATION step indexes and
/// refuses double execution (G-R1 unit target).
///
/// H8: the dispatcher owns the worker-session pool and the ACTIVE session
/// index. Session 0 is the primary pair passed in (pre-H8 behavior,
/// byte-identical when no session steps appear); sessions >= 1 are lazy
/// thread-backed workers. Every statement path resolves through `active`.
pub struct Dispatcher<'a> {
    pub dut: &'a mut dyn Session,
    pub cpg: Option<&'a mut dyn Session>,
    executed_mutations: std::collections::BTreeSet<usize>,
    pub pool: super::sessions::SessionPool,
    pub active: u32,
}

impl<'a> Dispatcher<'a> {
    pub fn new(dut: &'a mut dyn Session, cpg: Option<&'a mut dyn Session>) -> Self {
        Dispatcher {
            dut,
            cpg,
            executed_mutations: std::collections::BTreeSet::new(),
            pool: super::sessions::SessionPool::new(None),
            active: 0,
        }
    }

    pub fn with_pool(
        dut: &'a mut dyn Session,
        cpg: Option<&'a mut dyn Session>,
        cfg: Option<super::sessions::SessionPoolConfig>,
    ) -> Self {
        Self::with_session_pool(dut, cpg, super::sessions::SessionPool::new(cfg))
    }

    /// SIM-CONVERGE inc-3: a caller-built pool (the sim bridge's PREPARED
    /// replay pool — per-session `ReplayWorker`s over recorded artifacts).
    pub fn with_session_pool(
        dut: &'a mut dyn Session,
        cpg: Option<&'a mut dyn Session>,
        pool: super::sessions::SessionPool,
    ) -> Self {
        Dispatcher {
            dut,
            cpg,
            executed_mutations: std::collections::BTreeSet::new(),
            pool,
            active: 0,
        }
    }

    /// Switch the active session, lazily spawning workers. Errors only on a
    /// missing pool config or an out-of-range id (harness bug class).
    pub fn set_active(&mut self, id: u32) -> Result<(), String> {
        if id > crate::plan::MAX_SESSION_ID {
            return Err(format!("session id {id} out of range"));
        }
        if id > 0 {
            self.pool.ensure(id, self.cpg.is_some())?;
        }
        self.active = id;
        Ok(())
    }

    /// The active session's legs. Session 0 = the primary pair; workers
    /// otherwise (already ensured by `set_active`).
    #[allow(clippy::type_complexity)]
    fn legs(&mut self) -> (&mut dyn Session, Option<&mut dyn Session>) {
        if self.active == 0 {
            let d: &mut dyn Session = &mut *self.dut;
            let c = self.cpg.as_deref_mut().map(|x| x as &mut dyn Session);
            (d, c)
        } else {
            let (d, c) = self.pool.pair(self.active);
            (d.as_session(), c.map(|w| w.as_session()))
        }
    }

    /// Execute one marked statement. Returns (dut outcome, optional C outcome).
    /// A second call for the same step index with Mark::Mutation is REFUSED.
    pub fn dispatch(
        &mut self,
        idx: usize,
        sql: &Sql,
    ) -> Result<(ExecOutcome, Option<ExecOutcome>), String> {
        if sql.mark == Mark::Mutation {
            if !self.executed_mutations.insert(idx) {
                return Err(format!(
                    "dispatch refusal: MUTATION step {} would execute twice (mutation-split law)",
                    idx
                ));
            }
        }
        let (dut, cpg) = self.legs();
        let dut_out = dut.execute(&sql.text);
        let cpg_out = match (cpg, sql.mark) {
            (Some(c), Mark::Read) | (Some(c), Mark::Mutation) | (Some(c), Mark::Passthrough) => {
                // All marks run on both engines in diff mode (state lockstep);
                // only READ outcomes are *compared*, PASSTHROUGH never.
                Some(c.execute(&sql.text))
            }
            (None, _) => None,
        };
        // Statement-level error recovery is SYMMETRIC (resync law): a failed
        // statement poisons its own leg's tx; rolling back only that leg
        // while the other leg's open tx keeps running is a permanent state
        // fork (cascading false wrong-results downstream). ROLLBACK both.
        let any_sql_error = matches!(dut_out, ExecOutcome::SqlError { .. })
            || matches!(cpg_out, Some(ExecOutcome::SqlError { .. }));
        if any_sql_error {
            self.recover_both();
        }
        Ok((dut_out, cpg_out))
    }

    /// Post-error resync: ROLLBACK on every leg OF THE ACTIVE SESSION (no-op
    /// warning outside a tx, aborts the poisoned/healthy txs symmetrically
    /// inside one). Outcomes deliberately ignored — a dead leg is detected
    /// by the caller's crash classification, not here.
    pub fn recover_both(&mut self) {
        let (dut, cpg) = self.legs();
        let _ = dut.execute("ROLLBACK");
        if let Some(c) = cpg {
            let _ = c.execute("ROLLBACK");
        }
    }
}

/// DUT-side panic escalation (see `vocab::is_panic_signature`): Some(P1
/// failure) when the DUT outcome is a SqlError carrying a contained-panic
/// signature. The pinned triage classification of the same statement is
/// emitted by the caller as usual — this is an additional escalation record,
/// so the G-O2 parity gate never sees a changed class.
fn panic_escalation(idx: usize, dut_out: &ExecOutcome, stmt: &str) -> Option<Failure> {
    let ExecOutcome::SqlError { sqlstate, message } = dut_out else {
        return None;
    };
    if !crate::vocab::is_panic_signature(sqlstate, message) {
        return None;
    }
    Some(Failure {
        step_idx: idx,
        signature: Signature {
            class: "panic-signature".into(),
            sqlstate: sqlstate.clone(),
            site: normalize_site(stmt),
        },
        class: "panic-signature".into(),
        sev: "P1".into(),
        detail: format!("contained backend panic surfaced as SQL error: {}", message),
    })
}

pub fn tx_sql(tx: &TxCtl) -> String {
    match tx {
        TxCtl::Begin(IsoLevel::ReadCommitted) => "BEGIN ISOLATION LEVEL READ COMMITTED".into(),
        TxCtl::Begin(IsoLevel::RepeatableRead) => "BEGIN ISOLATION LEVEL REPEATABLE READ".into(),
        TxCtl::Begin(IsoLevel::Serializable) => "BEGIN ISOLATION LEVEL SERIALIZABLE".into(),
        TxCtl::Commit => "COMMIT".into(),
        TxCtl::Rollback => "ROLLBACK".into(),
        TxCtl::Savepoint(n) => format!("SAVEPOINT {}", n),
        TxCtl::RollbackTo(n) => format!("ROLLBACK TO SAVEPOINT {}", n),
    }
}

pub fn arm_sql(arm: &ArmCtl) -> String {
    match arm {
        ArmCtl::SetGuc(k, v) => format!("SET {} = '{}'", k, v),
        ArmCtl::ResetAll => "RESET ALL".into(),
    }
}

/// Execute a plan. `cpg` present = --diff-c semantics.
pub fn execute_plan<'a>(
    plan: &Plan,
    dut: &'a mut dyn Session,
    cpg: Option<&'a mut dyn Session>,
    checks: &dyn CheckEval,
    classifier: &dyn DiffClassifier,
    opts: &ExecOptions,
) -> RunReport {
    let pool = super::sessions::SessionPool::new(opts.session_pool.clone());
    execute_plan_pooled(plan, dut, cpg, checks, classifier, opts, pool)
}

/// SIM-CONVERGE inc-3: `execute_plan` with a caller-provided session pool.
/// The sim bridge passes a PREPARED replay pool (per-session recorded
/// streams) so v2 multi-session plans are walked NATIVELY — Session
/// switches, Tx steps, AsyncDml/Join, and WaitUntil all route through the
/// same code the live driver runs.
pub fn execute_plan_pooled<'a>(
    plan: &Plan,
    dut: &'a mut dyn Session,
    cpg: Option<&'a mut dyn Session>,
    checks: &dyn CheckEval,
    classifier: &dyn DiffClassifier,
    opts: &ExecOptions,
    pool: super::sessions::SessionPool,
) -> RunReport {
    let mut report = RunReport::default();
    let mut stack = ResultStack::new();
    let mut disp = Dispatcher::with_session_pool(dut, cpg, pool);
    let mut in_skipped_property: Option<u32> = None;
    let mut in_property: Option<u32> = None;
    // H5 rung B: count of successfully-executed Query steps (sampling base).
    let mut query_seen: u64 = 0;

    for (idx, step) in plan.steps.iter().enumerate() {
        report.steps_executed = idx + 1;
        match step {
            Step::BeginProperty { seq, .. } => {
                in_property = Some(*seq);
                checks.on_property_begin(*seq);
            }
            Step::EndProperty { seq } => {
                if let Some(skip_seq) = in_skipped_property {
                    if *seq == skip_seq {
                        in_skipped_property = None;
                    }
                }
                in_property = None;
                // H8 invariant: properties leave the plan on session 0 by
                // construction; enforce mechanically so a skipped/failed
                // multi-session property can never leak its active session
                // into the noise stream.
                disp.active = 0;
                checks.on_property_end(*seq);
            }
            _ if in_skipped_property.is_some() => {
                // ASSUME failed for this property: remaining steps of the span
                // are skipped (counted, never silent).
                report.count("property-skipped");
            }
            Step::Ddl(sql) | Step::Dml(sql) | Step::Query(sql) => {
                let head = sql.text.chars().take(60).collect::<String>();
                match disp.dispatch(idx, sql) {
                    Ok((dut_out, cpg_out)) => {
                        if sql.mark == Mark::Read {
                            stack.push(dut_out.clone());
                        }
                        // Same-engine crash detection first (any mark, any mode).
                        if let ExecOutcome::ConnectionLost { message } = &dut_out {
                            let class = if message.starts_with("client:") {
                                "harness-fetch"
                            } else {
                                "rust-crash"
                            };
                            let sev = if class == "rust-crash" { "P1" } else { "P2" };
                            report.count(class);
                            report.records.push(StepRecord {
                                idx,
                                class: class.into(),
                                sev: sev.into(),
                                detail: message.clone(),
                                stmt_head: head.clone(),
                            });
                            report.failure = Some(Failure {
                                step_idx: idx,
                                signature: Signature {
                                    class: class.into(),
                                    sqlstate: "".into(),
                                    site: normalize_site(&sql.text),
                                },
                                class: class.into(),
                                sev: sev.into(),
                                detail: message.clone(),
                            });
                            if opts.stop_on_failure {
                                return report;
                            }
                            continue;
                        }
                        // C-leg crash detection, ANY mark (pinned §1.3: a
                        // dead server is c-crash P1, never an err-ladder P2;
                        // the diff stream is broken => HALT).
                        if let Some(ExecOutcome::ConnectionLost { message }) = &cpg_out {
                            let class = if message.starts_with("client:") {
                                "harness-fetch"
                            } else {
                                "c-crash"
                            };
                            let sev = if class == "c-crash" { "P1" } else { "P2" };
                            report.count(class);
                            report.records.push(StepRecord {
                                idx,
                                class: class.into(),
                                sev: sev.into(),
                                detail: format!("C leg: {}", message),
                                stmt_head: head.clone(),
                            });
                            report.failure = Some(Failure {
                                step_idx: idx,
                                signature: Signature {
                                    class: class.into(),
                                    sqlstate: "".into(),
                                    site: normalize_site(&sql.text),
                                },
                                class: class.into(),
                                sev: sev.into(),
                                detail: format!("C leg: {}", message),
                            });
                            if class == "c-crash" {
                                report.halted_at = Some(idx);
                                return report; // dead C server: HALT unconditional.
                            }
                            if opts.stop_on_failure {
                                return report;
                            }
                            continue;
                        }
                        // H5 rung B: plan-species fingerprint via EXPLAIN
                        // (COSTS OFF) on the DUT — sampled, DUT-only,
                        // state-neutral. Head-word gate: property noise emits
                        // Query-marked utility statements (PREPARE/
                        // DEALLOCATE, X4) that EXPLAIN rejects — those are
                        // not plan species. H6 widening: WITH-headed reads
                        // (CTE grammar) and DML EXPLAINs (ModifyTable
                        // species; EXPLAIN of DML plans without executing, so
                        // it stays state-neutral — the statement itself was
                        // already executed once via dispatch above, and this
                        // extra plan-only look never re-runs it).
                        let head_word = sql
                            .text
                            .trim_start()
                            .split(|c: char| !c.is_ascii_alphabetic())
                            .next()
                            .unwrap_or("");
                        let explainable = match step {
                            Step::Query(_) => {
                                head_word.eq_ignore_ascii_case("SELECT")
                                    || head_word.eq_ignore_ascii_case("WITH")
                            }
                            Step::Dml(_) => {
                                head_word.eq_ignore_ascii_case("INSERT")
                                    || head_word.eq_ignore_ascii_case("UPDATE")
                                    || head_word.eq_ignore_ascii_case("DELETE")
                                    || head_word.eq_ignore_ascii_case("MERGE")
                            }
                            _ => false,
                        };
                        // H8: sample only on session 0 (worker sessions may
                        // hold open RR snapshots; EXPLAIN there is not
                        // state-neutral for the choreography).
                        if explainable && !dut_out.is_error() && disp.active == 0 {
                            query_seen += 1;
                            if opts.explain_every > 0
                                && (query_seen - 1) % opts.explain_every as u64 == 0
                            {
                                let esql = format!("EXPLAIN (COSTS OFF) {}", sql.text);
                                match disp.dut.execute(&esql) {
                                    ExecOutcome::Rows { rows } => {
                                        let lines: Vec<String> = rows
                                            .iter()
                                            .filter_map(|r| r.first().cloned().flatten())
                                            .collect();
                                        report.plan_fingerprints.push(
                                            crate::metrics::canonicalize_explain(&lines),
                                        );
                                    }
                                    ExecOutcome::SqlError { sqlstate, message } => {
                                        // Metrics infrastructure error: count
                                        // in metrics only, resync BOTH legs
                                        // (an EXPLAIN error inside an open tx
                                        // poisons the DUT leg alone — the
                                        // symmetric-recovery law applies).
                                        report.explain_errors += 1;
                                        if report.explain_error_samples.len() < 5 {
                                            report.explain_error_samples.push(format!(
                                                "{} {} @ {}",
                                                sqlstate,
                                                message.chars().take(80).collect::<String>(),
                                                normalize_site(&sql.text)
                                            ));
                                        }
                                        disp.recover_both();
                                    }
                                    ExecOutcome::Command { .. } => {
                                        report.explain_errors += 1;
                                    }
                                    ExecOutcome::ConnectionLost { message } => {
                                        // A DUT death on a metrics EXPLAIN is
                                        // a real crash finding, never hidden.
                                        report.count("rust-crash");
                                        report.failure = Some(Failure {
                                            step_idx: idx,
                                            signature: Signature {
                                                class: "rust-crash".into(),
                                                sqlstate: "".into(),
                                                site: format!(
                                                    "explain:{}",
                                                    normalize_site(&sql.text)
                                                ),
                                            },
                                            class: "rust-crash".into(),
                                            sev: "P1".into(),
                                            detail: format!(
                                                "DUT connection lost during metrics EXPLAIN: {}",
                                                message
                                            ),
                                        });
                                        return report;
                                    }
                                }
                            }
                        }
                        // Panic escalation (H3 finding 1): a CONTAINED backend
                        // panic surfaces as XX000 + panic-payload message and
                        // the pinned ladder files it P2 (rust-err-c-ok) — the
                        // campaign verdict would PASS over a panicking DUT.
                        // Mint the harness-mechanics `panic-signature` P1
                        // ALONGSIDE (never instead of) the pinned class.
                        if let Some(f) = panic_escalation(idx, &dut_out, &sql.text) {
                            report.count("panic-signature");
                            report.records.push(StepRecord {
                                idx,
                                class: f.class.clone(),
                                sev: f.sev.clone(),
                                detail: f.detail.clone(),
                                stmt_head: head.clone(),
                            });
                            report.failure = Some(f);
                            if opts.stop_on_failure {
                                return report;
                            }
                        }
                        // Oracle model step (ledger apply + reconcile, slot
                        // capture) for statements inside a property block.
                        // Both-direction reconcile divergence is a
                        // property-violation P1 (contract §3.1.2).
                        if in_property.is_some() {
                            if let Some(why) = checks.on_step_outcome(&dut_out) {
                                report.count("property-violation");
                                let f = Failure {
                                    step_idx: idx,
                                    signature: Signature {
                                        class: "property-violation".into(),
                                        sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                                        site: normalize_site(&sql.text),
                                    },
                                    class: "property-violation".into(),
                                    sev: "P1".into(),
                                    detail: why,
                                };
                                report.records.push(StepRecord {
                                    idx,
                                    class: f.class.clone(),
                                    sev: f.sev.clone(),
                                    detail: f.detail.clone(),
                                    stmt_head: head.clone(),
                                });
                                report.failure = Some(f);
                                if opts.stop_on_failure {
                                    return report;
                                }
                            }
                        }
                        match (&cpg_out, sql.mark) {
                            (Some(c_out), Mark::Read) => {
                                let (class, sev) = classifier.classify_read(sql, &dut_out, c_out);
                                report.count(&class);
                                if sev != "fine" {
                                    let f = Failure {
                                        step_idx: idx,
                                        signature: Signature {
                                            class: class.clone(),
                                            sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                                            site: normalize_site(&sql.text),
                                        },
                                        class: class.clone(),
                                        sev: sev.clone(),
                                        detail: format!("read divergence at step {}", idx),
                                    };
                                    report.records.push(StepRecord {
                                        idx,
                                        class,
                                        sev,
                                        detail: f.detail.clone(),
                                        stmt_head: head.clone(),
                                    });
                                    if f.sev == "P1" {
                                        report.failure = Some(f);
                                        if opts.stop_on_failure {
                                            return report;
                                        }
                                    } else if report.failure.is_none() {
                                        report.failure = Some(f);
                                    }
                                }
                            }
                            (Some(c_out), Mark::Mutation) => {
                                // Outcome-class comparison only; divergence = state fork = HALT.
                                let dut_err = dut_out.is_error();
                                let c_err = c_out.is_error();
                                let state_diverged = dut_err != c_err
                                    || (dut_err
                                        && dut_out.sqlstate().map(|s| s.get(..2).map(|p| p.to_string()))
                                            != c_out.sqlstate().map(|s| s.get(..2).map(|p| p.to_string())));
                                if state_diverged {
                                    let class = if dut_err && !c_err {
                                        "rust-err-c-ok"
                                    } else if !dut_err && c_err {
                                        "c-err-rust-ok"
                                    } else {
                                        "err-state-mismatch"
                                    };
                                    report.count(class);
                                    report.halted_at = Some(idx);
                                    report.failure = Some(Failure {
                                        step_idx: idx,
                                        signature: Signature {
                                            class: class.into(),
                                            sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                                            site: normalize_site(&sql.text),
                                        },
                                        class: class.into(),
                                        sev: "P2".into(),
                                        detail: format!("mutation-outcome divergence at step {} — HALT (state forked)", idx),
                                    });
                                    return report; // HALT is unconditional.
                                }
                                report.count("ok");
                            }
                            _ => {
                                // standalone, or PASSTHROUGH (never compared).
                                match &dut_out {
                                    ExecOutcome::SqlError { .. } => report.count("err-uncompared"),
                                    _ => report.count("ok"),
                                }
                            }
                        }
                    }
                    Err(refusal) => {
                        report.count("dispatch-refusal");
                        report.failure = Some(Failure {
                            step_idx: idx,
                            signature: Signature {
                                class: "dispatch-refusal".into(),
                                sqlstate: "".into(),
                                site: normalize_site(&sql.text),
                            },
                            class: "dispatch-refusal".into(),
                            sev: "P1".into(),
                            detail: refusal,
                        });
                        return report;
                    }
                }
            }
            Step::Tx(tx) => {
                let sql = tx_sql(tx);
                let hook = if in_property.is_some() { Some(checks) } else { None };
                if run_ctl_step(&mut disp, &mut report, idx, &sql, "tx-control", hook, opts) {
                    return report;
                }
            }
            Step::Arm(arm) => {
                let sql = arm_sql(arm);
                let hook = if in_property.is_some() { Some(checks) } else { None };
                if run_ctl_step(&mut disp, &mut report, idx, &sql, "arm", hook, opts) {
                    return report;
                }
                if matches!(arm, ArmCtl::ResetAll) {
                    // RESET ALL reverted session GUCs to connection defaults
                    // on both legs; replay the harness session setup (e.g.
                    // search_path) symmetrically so generated unqualified
                    // names keep resolving (find-1 law).
                    for s in &opts.post_reset_sql {
                        let _ = disp.dut.execute(s);
                        if let Some(c) = disp.cpg.as_deref_mut() {
                            let _ = c.execute(s);
                        }
                    }
                }
            }
            Step::Assumption(j) => match checks.eval(j, &stack) {
                CheckVerdict::Pass => report.count("ok"),
                CheckVerdict::Skip(w) if w.starts_with("no-hook") => {
                    // Contract §0 A5: hook absent — counted, never a silent
                    // pass (`SIMHARNESS|skipped-no-hook|n`).
                    report.count("skipped-no-hook");
                    in_skipped_property = enclosing_property_seq(plan, idx);
                }
                CheckVerdict::Fail(_) | CheckVerdict::Skip(_) => {
                    // Failed ASSUME = property does not apply: skip the rest
                    // of the enclosing property span, counted.
                    report.count("property-skipped");
                    in_skipped_property = enclosing_property_seq(plan, idx);
                }
            },
            Step::Assertion(j) => match checks.eval(j, &stack) {
                CheckVerdict::Pass => report.count("ok"),
                CheckVerdict::Skip(w) => {
                    report.count("property-skipped");
                    report.records.push(StepRecord {
                        idx,
                        class: "property-skipped".into(),
                        sev: "fine".into(),
                        detail: w,
                        stmt_head: j.chars().take(60).collect(),
                    });
                }
                CheckVerdict::Fail(why) => {
                    report.count("property-violation");
                    let f = Failure {
                        step_idx: idx,
                        signature: Signature {
                            class: "property-violation".into(),
                            sqlstate: "".into(),
                            site: normalize_site(j),
                        },
                        class: "property-violation".into(),
                        sev: "P1".into(),
                        detail: why,
                    };
                    report.records.push(StepRecord {
                        idx,
                        class: f.class.clone(),
                        sev: f.sev.clone(),
                        detail: f.detail.clone(),
                        stmt_head: j.chars().take(60).collect(),
                    });
                    report.failure = Some(f);
                    if opts.stop_on_failure {
                        return report;
                    }
                }
            },
            Step::Fault(f) => match f {
                FaultPoint::Disconnect => {
                    // Client-side disconnect/reconnect; both legs so session
                    // state (GUCs, tx) stays lockstep.
                    if let Err(e) = disp.dut.reconnect() {
                        report.count("rust-crash");
                        report.failure = Some(Failure {
                            step_idx: idx,
                            signature: Signature {
                                class: "rust-crash".into(),
                                sqlstate: "".into(),
                                site: "fault:disconnect".into(),
                            },
                            class: "rust-crash".into(),
                            sev: "P1".into(),
                            detail: format!("reconnect after disconnect failed: {}", e),
                        });
                        return report;
                    }
                    if let Some(c) = disp.cpg.as_deref_mut() {
                        if let Err(e) = c.reconnect() {
                            report.count("c-crash");
                            report.halted_at = Some(idx);
                            report.failure = Some(Failure {
                                step_idx: idx,
                                signature: Signature {
                                    class: "c-crash".into(),
                                    sqlstate: "".into(),
                                    site: "fault:disconnect".into(),
                                },
                                class: "c-crash".into(),
                                sev: "P1".into(),
                                detail: format!("C leg reconnect failed: {}", e),
                            });
                            return report;
                        }
                    }
                    report.count("ok");
                }
                FaultPoint::ReconnectServer => match &opts.restart_cmd {
                    None => {
                        report.count("fault-skipped-no-restart-cmd");
                        // The generator models reconnect-server as ending any
                        // open tx (stubgen gen_fault, in_tx=false). A SKIPPED
                        // fault must not leave executed session state
                        // diverging from that model — a still-open tx turns
                        // every later generated BEGIN into a 25001. Resync
                        // both legs to the model (ROLLBACK, no-op outside a
                        // tx).
                        disp.recover_both();
                    }
                    Some(cmd) => {
                        let st = std::process::Command::new("sh").arg("-c").arg(cmd).status();
                        match st {
                            Ok(s) if s.success() => {
                                // Refuse no-op "restarts": if the OLD DUT
                                // connection still answers, the server never
                                // restarted — reconnecting would silently
                                // reset the DUT session (tx/GUCs) while the
                                // C leg keeps its state: a state fork that
                                // mints false wrong-results P1s. Harness
                                // misconfiguration, P1 self-check.
                                let mut probe =
                                    disp.dut.execute("SELECT 1 /* simharness restart-probe */");
                                if let ExecOutcome::SqlError { sqlstate, .. } = &probe {
                                    // A fast shutdown leaves a buffered FATAL
                                    // (57P01 admin-shutdown / 57P02 crash-
                                    // shutdown) on the old connection; the
                                    // first read surfaces it as a SqlError
                                    // although the connection underneath is
                                    // dead. Drain it and probe again.
                                    if sqlstate == "57P01" || sqlstate == "57P02" {
                                        probe = disp
                                            .dut
                                            .execute("SELECT 1 /* simharness restart-probe-2 */");
                                    }
                                }
                                if !matches!(probe, ExecOutcome::ConnectionLost { .. }) {
                                    report.count("fault-restart-noop");
                                    report.failure = Some(Failure {
                                        step_idx: idx,
                                        signature: Signature {
                                            class: "fault-restart-noop".into(),
                                            sqlstate: "".into(),
                                            site: "fault:reconnect-server".into(),
                                        },
                                        class: "fault-restart-noop".into(),
                                        sev: "P1".into(),
                                        detail: format!(
                                            "restart_cmd '{}' exited 0 but the DUT session survived — configure a real restart or drop --restart-cmd (counted skip)",
                                            cmd
                                        ),
                                    });
                                    return report;
                                }
                                if let Err(e) = disp.dut.reconnect() {
                                    report.count("rust-crash");
                                    report.failure = Some(Failure {
                                        step_idx: idx,
                                        signature: Signature {
                                            class: "rust-crash".into(),
                                            sqlstate: "".into(),
                                            site: "fault:reconnect-server".into(),
                                        },
                                        class: "rust-crash".into(),
                                        sev: "P1".into(),
                                        detail: format!("reconnect after server restart failed: {}", e),
                                    });
                                    return report;
                                }
                                // Resynchronize the C leg: the restart reset
                                // the DUT session (open tx aborted, GUCs
                                // gone) while the C session kept its state.
                                // Reconnect it too so both legs land in
                                // identical fresh-session state (zero-FP
                                // law; same shape as Fault(Disconnect)).
                                if let Some(c) = disp.cpg.as_deref_mut() {
                                    if let Err(e) = c.reconnect() {
                                        report.count("c-crash");
                                        report.halted_at = Some(idx);
                                        report.failure = Some(Failure {
                                            step_idx: idx,
                                            signature: Signature {
                                                class: "c-crash".into(),
                                                sqlstate: "".into(),
                                                site: "fault:reconnect-server".into(),
                                            },
                                            class: "c-crash".into(),
                                            sev: "P1".into(),
                                            detail: format!("C leg resync reconnect failed: {}", e),
                                        });
                                        return report;
                                    }
                                }
                                report.count("ok");
                            }
                            other => {
                                report.count("fault-restart-cmd-failed");
                                report.records.push(StepRecord {
                                    idx,
                                    class: "fault-restart-cmd-failed".into(),
                                    sev: "fine".into(),
                                    detail: format!("{:?}", other),
                                    stmt_head: cmd.chars().take(60).collect(),
                                });
                                // Same resync-to-model as the counted skip
                                // above: the restart did not happen, but the
                                // generator assumes the tx is gone.
                                disp.recover_both();
                            }
                        }
                    }
                },
                reserved => {
                    // Crash/TornWrite/Env: the H2 FaultDriver seam. The
                    // driver maps the step to a SimVfs fault-plan spec and
                    // arms it; the NoOp default refuses, which keeps H1's
                    // `fault-reserved-refused` P1 verbatim on the main
                    // lineage (never a silent skip).
                    let drv = opts.fault_driver.as_ref();
                    let armed = drv
                        .map(reserved, plan.header.seed, idx)
                        .and_then(|spec| drv.arm(&spec).map(|()| spec));
                    match armed {
                        Ok(spec) => {
                            report.count("fault-armed");
                            report.records.push(StepRecord {
                                idx,
                                class: "fault-armed".into(),
                                sev: "fine".into(),
                                detail: format!(
                                    "driver={} seed={} rules={}",
                                    drv.name(),
                                    spec.seed,
                                    spec.rules.len()
                                ),
                                stmt_head: format!("{:?}", reserved).chars().take(60).collect(),
                            });
                        }
                        Err(e) => {
                            report.count("fault-reserved-refused");
                            report.failure = Some(Failure {
                                step_idx: idx,
                                signature: Signature {
                                    class: "fault-reserved-refused".into(),
                                    sqlstate: "".into(),
                                    site: format!("{:?}", reserved).chars().take(40).collect(),
                                },
                                class: "fault-reserved-refused".into(),
                                sev: "P1".into(),
                                detail: format!(
                                    "reserved fault tag (H4), driver '{}' refuses: {} ({:?})",
                                    drv.name(),
                                    e,
                                    reserved
                                ),
                            });
                            return report;
                        }
                    }
                }
            },
            // H8 session-family steps (plan-format v2). Execution requires a
            // configured session pool; without one every arm below reports a
            // loud `session-refusal` P1 — never a silent skip.
            Step::Session(_) | Step::AsyncDml(_) | Step::Join(_) | Step::WaitUntil(_) => {
                if run_session_step(
                    &mut disp,
                    &mut report,
                    idx,
                    step,
                    if in_property.is_some() { Some(checks) } else { None },
                    opts,
                ) {
                    return report;
                }
            }
        }
    }
    report
}

/// TX/ARM control-statement step. Crash detection matches the statement
/// arms: a dead DUT on COMMIT (the classic engine-bug site) is rust-crash
/// P1, a dead C leg is c-crash P1 — never a silent "ok" and never an
/// err-ladder P2. Symmetric SqlErrors recover BOTH legs (resync law).
/// `hook` (present inside property blocks) steps the oracle model (ledger
/// tx bookkeeping); a model violation is a property-violation P1.
/// Returns true when the plan must stop.
fn run_ctl_step(
    disp: &mut Dispatcher,
    report: &mut RunReport,
    idx: usize,
    sql: &str,
    what: &str,
    hook: Option<&dyn CheckEval>,
    opts: &ExecOptions,
) -> bool {
    let head: String = sql.chars().take(60).collect();
    let (dut_leg, cpg_leg) = disp.legs();
    let dut_out = dut_leg.execute(sql);
    let cpg_out = cpg_leg.map(|c| c.execute(sql));
    if let ExecOutcome::ConnectionLost { message } = &dut_out {
        let class = if message.starts_with("client:") { "harness-fetch" } else { "rust-crash" };
        let sev = if class == "rust-crash" { "P1" } else { "P2" };
        report.count(class);
        report.records.push(StepRecord {
            idx,
            class: class.into(),
            sev: sev.into(),
            detail: message.clone(),
            stmt_head: head,
        });
        report.failure = Some(Failure {
            step_idx: idx,
            signature: Signature {
                class: class.into(),
                sqlstate: "".into(),
                site: normalize_site(sql),
            },
            class: class.into(),
            sev: sev.into(),
            detail: message.clone(),
        });
        return true; // control-statement stream broken either way: stop.
    }
    let mut c_err = None;
    if let Some(c_out) = cpg_out {
        if let ExecOutcome::ConnectionLost { message } = &c_out {
            let class = if message.starts_with("client:") { "harness-fetch" } else { "c-crash" };
            let sev = if class == "c-crash" { "P1" } else { "P2" };
            report.count(class);
            report.records.push(StepRecord {
                idx,
                class: class.into(),
                sev: sev.into(),
                detail: format!("C leg: {}", message),
                stmt_head: head,
            });
            if class == "c-crash" {
                report.halted_at = Some(idx);
            }
            report.failure = Some(Failure {
                step_idx: idx,
                signature: Signature {
                    class: class.into(),
                    sqlstate: "".into(),
                    site: normalize_site(sql),
                },
                class: class.into(),
                sev: sev.into(),
                detail: format!("C leg: {}", message),
            });
            return true;
        }
        c_err = Some(c_out.is_error());
    }
    // Panic escalation on control statements too (COMMIT is the classic
    // engine-bug site); same additional-record posture as the Sql arm.
    if let Some(f) = panic_escalation(idx, &dut_out, sql) {
        report.count("panic-signature");
        report.records.push(StepRecord {
            idx,
            class: f.class.clone(),
            sev: f.sev.clone(),
            detail: f.detail.clone(),
            stmt_head: sql.chars().take(60).collect(),
        });
        report.failure = Some(f);
        if opts.stop_on_failure {
            return true;
        }
    }
    // Oracle model step (ledger tx bookkeeping) inside property blocks.
    if let Some(h) = hook {
        if let Some(why) = h.on_step_outcome(&dut_out) {
            report.count("property-violation");
            report.records.push(StepRecord {
                idx,
                class: "property-violation".into(),
                sev: "P1".into(),
                detail: why.clone(),
                stmt_head: sql.chars().take(60).collect(),
            });
            report.failure = Some(Failure {
                step_idx: idx,
                signature: Signature {
                    class: "property-violation".into(),
                    sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                    site: normalize_site(sql),
                },
                class: "property-violation".into(),
                sev: "P1".into(),
                detail: why,
            });
            if opts.stop_on_failure {
                return true;
            }
        }
    }
    let dut_err = dut_out.is_error();
    if let Some(c_err) = c_err {
        if dut_err != c_err {
            report.count("err-state-mismatch");
            report.halted_at = Some(idx);
            report.failure = Some(Failure {
                step_idx: idx,
                signature: Signature {
                    class: "err-state-mismatch".into(),
                    sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                    site: normalize_site(sql),
                },
                class: "err-state-mismatch".into(),
                sev: "P2".into(),
                detail: format!("{} divergence at step {} — HALT", what, idx),
            });
            return true;
        }
    }
    if dut_err {
        // Symmetric (or standalone) control-statement error: recover both
        // legs to autocommit so session state stays lockstep, and count
        // honestly (never "ok" for an uncompared error).
        disp.recover_both();
        if c_err.is_some() {
            report.count("ok"); // both legs erred identically: lockstep held.
        } else {
            report.count("err-uncompared");
        }
    } else {
        report.count("ok");
    }
    false
}

/// H8 session-family step execution. Returns true when the plan must stop.
///
/// Semantics (all loud, never silent):
///   * Session(k): switch the active session (lazy worker spawn). Missing
///     pool config / out-of-range id => `session-refusal` P1.
///   * AsyncDml: dispatch on the ACTIVE session without waiting — refused on
///     session 0 (the primary pair is the plan walker's own leg; a blocked
///     statement there would wedge the walk) and when a statement is already
///     outstanding.
///   * Join(k): collect session k's outstanding statement on both legs;
///     panic escalation + oracle hook + mutation-style outcome-parity
///     comparison (divergence = state fork = HALT).
///   * WaitUntil: poll the active session (each leg independently) until
///     the query returns the single scalar 't'; bounded — timeout is a
///     `wait-timeout` P1 (a choreography gate that never cleared).
fn run_session_step(
    disp: &mut Dispatcher,
    report: &mut RunReport,
    idx: usize,
    step: &Step,
    hook: Option<&dyn CheckEval>,
    opts: &ExecOptions,
) -> bool {
    let refuse = |report: &mut RunReport, site: &str, detail: String| -> bool {
        report.count("session-refusal");
        report.failure = Some(Failure {
            step_idx: idx,
            signature: Signature {
                class: "session-refusal".into(),
                sqlstate: "".into(),
                site: site.into(),
            },
            class: "session-refusal".into(),
            sev: "P1".into(),
            detail,
        });
        true
    };
    match step {
        Step::Session(id) => {
            if let Err(e) = disp.set_active(*id) {
                return refuse(report, "session:switch", e);
            }
            report.count("ok");
            false
        }
        Step::AsyncDml(sql) => {
            if disp.active == 0 {
                return refuse(
                    report,
                    "session:async",
                    "async dispatch on session 0 (would wedge the plan walker)".into(),
                );
            }
            if sql.mark == Mark::Mutation && !disp.executed_mutations.insert(idx) {
                report.count("dispatch-refusal");
                report.failure = Some(Failure {
                    step_idx: idx,
                    signature: Signature {
                        class: "dispatch-refusal".into(),
                        sqlstate: "".into(),
                        site: normalize_site(&sql.text),
                    },
                    class: "dispatch-refusal".into(),
                    sev: "P1".into(),
                    detail: format!(
                        "dispatch refusal: MUTATION step {} would execute twice (mutation-split law)",
                        idx
                    ),
                });
                return true;
            }
            let active = disp.active;
            let has_cpg = disp.cpg.is_some();
            let dut_res = disp
                .pool
                .dut_of(active)
                .map(|w| w.dispatch_async(&sql.text))
                .unwrap_or(Err("no worker for active session".into()));
            let cpg_res = if has_cpg {
                disp.pool
                    .cpg_of(active)
                    .map(|w| w.dispatch_async(&sql.text))
                    .unwrap_or(Err("no C worker for active session".into()))
            } else {
                Ok(())
            };
            if let Err(e) = dut_res.and(cpg_res) {
                return refuse(report, "session:async", e);
            }
            report.count("async-dispatched");
            false
        }
        Step::Join(id) => {
            let dut_out = match disp.pool.dut_of(*id) {
                Some(w) => w.join_pending(),
                None => {
                    return refuse(report, "session:join", format!("join {id}: no such worker"))
                }
            };
            let cpg_out = disp.pool.cpg_of(*id).map(|w| w.join_pending());
            let head = format!("join {id}");
            // Crash classification mirrors the Sql arm: a dead DUT worker is
            // rust-crash P1 (the whole threaded server died — the p3 plant's
            // SIGABRT shape); "client:"-prefixed = harness-fetch P2.
            if let ExecOutcome::ConnectionLost { message } = &dut_out {
                let class =
                    if message.starts_with("client:") { "harness-fetch" } else { "rust-crash" };
                let sev = if class == "rust-crash" { "P1" } else { "P2" };
                report.count(class);
                report.records.push(StepRecord {
                    idx,
                    class: class.into(),
                    sev: sev.into(),
                    detail: message.clone(),
                    stmt_head: head.clone(),
                });
                report.failure = Some(Failure {
                    step_idx: idx,
                    signature: Signature {
                        class: class.into(),
                        sqlstate: "".into(),
                        site: format!("session:join:{id}"),
                    },
                    class: class.into(),
                    sev: sev.into(),
                    detail: message.clone(),
                });
                return true;
            }
            if let Some(ExecOutcome::ConnectionLost { message }) = &cpg_out {
                let class =
                    if message.starts_with("client:") { "harness-fetch" } else { "c-crash" };
                let sev = if class == "c-crash" { "P1" } else { "P2" };
                report.count(class);
                report.failure = Some(Failure {
                    step_idx: idx,
                    signature: Signature {
                        class: class.into(),
                        sqlstate: "".into(),
                        site: format!("session:join:{id}"),
                    },
                    class: class.into(),
                    sev: sev.into(),
                    detail: format!("C leg: {message}"),
                });
                return true;
            }
            // Panic escalation (the COMMIT-site posture applies to joins).
            if let Some(f) = panic_escalation(idx, &dut_out, &head) {
                report.count("panic-signature");
                report.records.push(StepRecord {
                    idx,
                    class: f.class.clone(),
                    sev: f.sev.clone(),
                    detail: f.detail.clone(),
                    stmt_head: head.clone(),
                });
                report.failure = Some(f);
                if opts.stop_on_failure {
                    return true;
                }
            }
            // Oracle model step: the joined outcome lands in the instance's
            // Join slot (bridge advances past the silent session steps).
            if let Some(h) = hook {
                if let Some(why) = h.on_step_outcome(&dut_out) {
                    report.count("property-violation");
                    report.failure = Some(Failure {
                        step_idx: idx,
                        signature: Signature {
                            class: "property-violation".into(),
                            sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                            site: format!("session:join:{id}"),
                        },
                        class: "property-violation".into(),
                        sev: "P1".into(),
                        detail: why,
                    });
                    if opts.stop_on_failure {
                        return true;
                    }
                }
            }
            // Mutation-style outcome-class parity (state forked = HALT).
            if let Some(c_out) = &cpg_out {
                let dut_err = dut_out.is_error();
                let c_err = c_out.is_error();
                let diverged = dut_err != c_err
                    || (dut_err
                        && dut_out.sqlstate().map(|s| s.get(..2).map(|p| p.to_string()))
                            != c_out.sqlstate().map(|s| s.get(..2).map(|p| p.to_string())));
                if diverged {
                    let class = if dut_err && !c_err {
                        "rust-err-c-ok"
                    } else if !dut_err && c_err {
                        "c-err-rust-ok"
                    } else {
                        "err-state-mismatch"
                    };
                    report.count(class);
                    report.halted_at = Some(idx);
                    report.failure = Some(Failure {
                        step_idx: idx,
                        signature: Signature {
                            class: class.into(),
                            sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                            site: format!("session:join:{id}"),
                        },
                        class: class.into(),
                        sev: "P2".into(),
                        detail: format!(
                            "async-join outcome divergence at step {idx} — HALT (state forked)"
                        ),
                    });
                    return true;
                }
            }
            report.count("ok");
            false
        }
        Step::WaitUntil(sql) => {
            // Poll each leg independently until the gate clears ('t').
            // 400 x 25ms = 10s cap per leg (statement_timeout resolves any
            // genuinely wedged blocker at 5s, flipping the gate or erroring).
            let (dut_leg, cpg_leg) = disp.legs();
            let mut legs: Vec<&mut dyn Session> = vec![dut_leg];
            if let Some(c) = cpg_leg {
                legs.push(c);
            }
            for leg in legs {
                let mut cleared = false;
                for _ in 0..400 {
                    match leg.execute(&sql.text) {
                        ExecOutcome::Rows { rows }
                            if rows.len() == 1
                                && rows[0].len() == 1
                                && rows[0][0].as_deref() == Some("t") =>
                        {
                            cleared = true;
                            break;
                        }
                        ExecOutcome::ConnectionLost { message } => {
                            let is_dut = leg.engine().starts_with("pgrust");
                            let class = if message.starts_with("client:") {
                                "harness-fetch"
                            } else if is_dut {
                                "rust-crash"
                            } else {
                                "c-crash"
                            };
                            let sev = if class == "harness-fetch" { "P2" } else { "P1" };
                            report.count(class);
                            report.failure = Some(Failure {
                                step_idx: idx,
                                signature: Signature {
                                    class: class.into(),
                                    sqlstate: "".into(),
                                    site: format!("session:wait:{}", normalize_site(&sql.text)),
                                },
                                class: class.into(),
                                sev: sev.into(),
                                detail: message,
                            });
                            return true;
                        }
                        _ => {}
                    }
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
                if !cleared {
                    report.count("wait-timeout");
                    report.failure = Some(Failure {
                        step_idx: idx,
                        signature: Signature {
                            class: "wait-timeout".into(),
                            sqlstate: "".into(),
                            site: format!("session:wait:{}", normalize_site(&sql.text)),
                        },
                        class: "wait-timeout".into(),
                        sev: "P1".into(),
                        detail: format!(
                            "observable-state gate never cleared on {}: {}",
                            leg.engine(),
                            sql.text.chars().take(120).collect::<String>()
                        ),
                    });
                    return true;
                }
            }
            report.count("ok");
            false
        }
        _ => unreachable!("run_session_step called with a non-session step"),
    }
}

fn enclosing_property_seq(plan: &Plan, idx: usize) -> Option<u32> {
    let mut open: Option<u32> = None;
    for step in plan.steps.iter().take(idx + 1) {
        match step {
            Step::BeginProperty { seq, .. } => open = Some(*seq),
            Step::EndProperty { .. } => open = None,
            _ => {}
        }
    }
    open
}
