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
                    let out = ExecOutcome::SqlError {
                        sqlstate: db.code().code().to_string(),
                        message: db.message().to_string(),
                    };
                    // Failed statement poisons the tx; recover to autocommit
                    // like triage.py does (rollback, ignore failure).
                    let _ = client.simple_query("ROLLBACK");
                    out
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
pub trait CheckEval {
    fn eval(&self, check_json: &str, stack: &ResultStack) -> CheckVerdict;
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
    out.truncate(80);
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
}

impl Default for ExecOptions {
    fn default() -> Self {
        ExecOptions { restart_cmd: None, stop_on_failure: true }
    }
}

/// Mark-honoring dispatcher. Tracks executed MUTATION step indexes and
/// refuses double execution (G-R1 unit target).
pub struct Dispatcher<'a> {
    pub dut: &'a mut dyn Session,
    pub cpg: Option<&'a mut dyn Session>,
    executed_mutations: std::collections::BTreeSet<usize>,
}

impl<'a> Dispatcher<'a> {
    pub fn new(dut: &'a mut dyn Session, cpg: Option<&'a mut dyn Session>) -> Self {
        Dispatcher { dut, cpg, executed_mutations: std::collections::BTreeSet::new() }
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
        let dut_out = self.dut.execute(&sql.text);
        let cpg_out = match (&mut self.cpg, sql.mark) {
            (Some(c), Mark::Read) | (Some(c), Mark::Mutation) | (Some(c), Mark::Passthrough) => {
                // All marks run on both engines in diff mode (state lockstep);
                // only READ outcomes are *compared*, PASSTHROUGH never.
                Some(c.execute(&sql.text))
            }
            (None, _) => None,
        };
        Ok((dut_out, cpg_out))
    }
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
    let mut report = RunReport::default();
    let mut stack = ResultStack::new();
    let mut disp = Dispatcher::new(dut, cpg);
    let mut in_skipped_property: Option<u32> = None;

    for (idx, step) in plan.steps.iter().enumerate() {
        report.steps_executed = idx + 1;
        match step {
            Step::BeginProperty { .. } | Step::EndProperty { .. } => {
                if let (Step::EndProperty { seq }, Some(skip_seq)) = (step, in_skipped_property) {
                    if *seq == skip_seq {
                        in_skipped_property = None;
                    }
                }
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
                let dut_out = disp.dut.execute(&sql);
                if let Some(c) = disp.cpg.as_deref_mut() {
                    let c_out = c.execute(&sql);
                    if dut_out.is_error() != c_out.is_error() {
                        report.count("err-state-mismatch");
                        report.halted_at = Some(idx);
                        report.failure = Some(Failure {
                            step_idx: idx,
                            signature: Signature {
                                class: "err-state-mismatch".into(),
                                sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                                site: normalize_site(&sql),
                            },
                            class: "err-state-mismatch".into(),
                            sev: "P2".into(),
                            detail: format!("tx-control divergence at step {} — HALT", idx),
                        });
                        return report;
                    }
                }
                report.count("ok");
            }
            Step::Arm(arm) => {
                let sql = arm_sql(arm);
                let dut_out = disp.dut.execute(&sql);
                if let Some(c) = disp.cpg.as_deref_mut() {
                    let c_out = c.execute(&sql);
                    if dut_out.is_error() != c_out.is_error() {
                        report.count("err-state-mismatch");
                        report.halted_at = Some(idx);
                        report.failure = Some(Failure {
                            step_idx: idx,
                            signature: Signature {
                                class: "err-state-mismatch".into(),
                                sqlstate: dut_out.sqlstate().unwrap_or("").into(),
                                site: normalize_site(&sql),
                            },
                            class: "err-state-mismatch".into(),
                            sev: "P2".into(),
                            detail: format!("arm divergence at step {} — HALT", idx),
                        });
                        return report;
                    }
                }
                report.count("ok");
            }
            Step::Assumption(j) => match checks.eval(j, &stack) {
                CheckVerdict::Pass => report.count("ok"),
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
                    }
                    Some(cmd) => {
                        let st = std::process::Command::new("sh").arg("-c").arg(cmd).status();
                        match st {
                            Ok(s) if s.success() => {
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
                            }
                        }
                    }
                },
                reserved => {
                    // Crash/TornWrite/Env: render+parse OK, EXECUTE refuses (H4).
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
                        detail: format!("reserved fault tag (H4), execute refuses: {:?}", reserved),
                    });
                    return report;
                }
            },
        }
    }
    report
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
