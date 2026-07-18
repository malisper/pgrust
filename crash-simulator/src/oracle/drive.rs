//! Property-instance evaluation semantics — the oracle side of the run loop.
//!
//! WS-RUNNER's session driver executes steps against the real engine and
//! feeds results through this module (it *calls* checkers and *emits* their
//! verdicts, contract §4.2). The module also ships `LedgerSimExecutor`, a
//! ledger-backed perfect-engine double used by the oracle's own unit tests,
//! standalone smoke, and determinism gates — no server required.

use std::collections::BTreeMap;

use crate::oracle::check::{
    eval_check, CheckOutcome, HookProbe, ResultStack, Row, StmtResult, Value,
};
use crate::oracle::ledger::{
    reconcile, ApplyOutcome, EngineDmlResult, Ledger, SQLSTATE_UNDEFINED_TABLE,
};
use crate::oracle::props::PropertyId;
use crate::oracle::pstep::{
    ArmCtl, PredSpec, ProbeSpec, PropertyInstance, PStep, SqlStep, TriSel, TxCtl,
};
use crate::vocab::Class;

/// Executes steps somewhere (real engine via WS-RUNNER, or the sim double).
pub trait StepExecutor {
    fn exec_sql(&mut self, step: &SqlStep) -> StmtResult;
    fn exec_tx(&mut self, ctl: &TxCtl);
    fn exec_arm(&mut self, ctl: &ArmCtl);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyOutcome {
    Pass,
    /// A check failed or a DML reconcile diverged — `property-violation` P1.
    Violation,
    /// An ASSUME failed on a non-hook precondition: skip, not a bug.
    AssumptionFailed,
    /// Contract §0 A5: hook absent — counted `SIMHARNESS|skipped-no-hook|n`.
    SkippedNoHook,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyReport {
    pub property: PropertyId,
    pub outcome: PropertyOutcome,
    /// step index + reason for Violation.
    pub detail: Option<(usize, String)>,
}

impl PropertyReport {
    pub fn class(&self) -> Class {
        match self.outcome {
            PropertyOutcome::Pass => Class::Ok,
            PropertyOutcome::Violation => Class::PropertyViolation,
            PropertyOutcome::AssumptionFailed | PropertyOutcome::SkippedNoHook => {
                Class::PropertySkipped
            }
        }
    }
}

/// Deterministic serialization for verdict-stream comparison (G-O4: same
/// inputs => byte-identical stream).
pub fn report_line(seed: u64, r: &PropertyReport) -> String {
    match &r.detail {
        Some((i, why)) => format!(
            "{seed}|{}|{}|step{}:{}",
            r.property.as_str(),
            r.class().as_str(),
            i,
            why
        ),
        None => format!("{seed}|{}|{}|", r.property.as_str(), r.class().as_str()),
    }
}

/// Class-count accumulator; BTreeMap so census lines are ordered
/// deterministically (determinism law: no unordered iteration in any
/// verdict-affecting path).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OutcomeCounts {
    pub by_class: BTreeMap<String, u64>,
    pub by_property: BTreeMap<String, u64>,
    pub skipped_no_hook: u64,
}

impl OutcomeCounts {
    pub fn record(&mut self, report: &PropertyReport) {
        *self
            .by_class
            .entry(report.class().as_str().to_string())
            .or_insert(0) += 1;
        *self
            .by_property
            .entry(report.property.as_str().to_string())
            .or_insert(0) += 1;
        if report.outcome == PropertyOutcome::SkippedNoHook {
            self.skipped_no_hook += 1;
        }
    }
}

fn dml_result(res: &StmtResult) -> EngineDmlResult {
    match res {
        StmtResult::Command { affected } => EngineDmlResult::Ok { affected: *affected },
        StmtResult::Rows { rows } => EngineDmlResult::Ok { affected: rows.len() as u64 },
        StmtResult::Error { sqlstate } => EngineDmlResult::Err { sqlstate: sqlstate.clone() },
    }
}

/// Evaluate one property instance: execute steps, mirror ledger ops with
/// both-direction reconciliation, evaluate ASSUME/ASSERT against the result
/// stack. Stops at the first violation (state past a divergence is not
/// trustworthy).
pub fn evaluate_instance(
    inst: &PropertyInstance,
    exec: &mut dyn StepExecutor,
    ledger: &mut Ledger,
    hooks: &dyn HookProbe,
) -> PropertyReport {
    let mut stack = ResultStack::new();
    for (i, step) in inst.steps.iter().enumerate() {
        match step {
            PStep::Sql(s) => {
                let res = exec.exec_sql(s);
                if let Some(op) = &s.ledger_op {
                    let expected = match ledger.apply(op) {
                        Ok(exp) => exp,
                        Err(misuse) => {
                            return PropertyReport {
                                property: inst.property,
                                outcome: PropertyOutcome::Violation,
                                detail: Some((i, format!("ledger misuse: {}", misuse.0))),
                            }
                        }
                    };
                    let verdict = reconcile(&expected, &dml_result(&res));
                    if verdict.is_violation() {
                        return PropertyReport {
                            property: inst.property,
                            outcome: PropertyOutcome::Violation,
                            detail: Some((i, format!("reconcile: {verdict:?}"))),
                        };
                    }
                }
                if let Some(slot) = s.stackref {
                    stack.put(slot, res);
                }
            }
            PStep::Tx(ctl) => {
                exec.exec_tx(ctl);
                match ctl {
                    TxCtl::Begin(iso) => ledger.begin(*iso),
                    TxCtl::Commit => ledger.commit(),
                    TxCtl::Rollback => ledger.rollback(),
                    TxCtl::Savepoint(n) => ledger.savepoint(n),
                    TxCtl::RollbackTo(n) => {
                        if let Err(m) = ledger.rollback_to(n) {
                            return PropertyReport {
                                property: inst.property,
                                outcome: PropertyOutcome::Violation,
                                detail: Some((i, format!("ledger misuse: {}", m.0))),
                            };
                        }
                    }
                }
            }
            PStep::Arm(ctl) => exec.exec_arm(ctl),
            PStep::Assume(check) => match eval_check(check, &stack, ledger, hooks) {
                CheckOutcome::Pass => {}
                CheckOutcome::SkipNoHook => {
                    return PropertyReport {
                        property: inst.property,
                        outcome: PropertyOutcome::SkippedNoHook,
                        detail: None,
                    }
                }
                CheckOutcome::Fail(_) => {
                    return PropertyReport {
                        property: inst.property,
                        outcome: PropertyOutcome::AssumptionFailed,
                        detail: None,
                    }
                }
            },
            PStep::Assert(check) => match eval_check(check, &stack, ledger, hooks) {
                CheckOutcome::Pass => {}
                CheckOutcome::SkipNoHook => {
                    return PropertyReport {
                        property: inst.property,
                        outcome: PropertyOutcome::SkippedNoHook,
                        detail: None,
                    }
                }
                CheckOutcome::Fail(why) => {
                    return PropertyReport {
                        property: inst.property,
                        outcome: PropertyOutcome::Violation,
                        detail: Some((i, why)),
                    }
                }
            },
            // WS-GEN substitutes noise; standalone oracle evaluation skips it.
            PStep::NoiseSlot(_) => {}
        }
    }
    PropertyReport { property: inst.property, outcome: PropertyOutcome::Pass, detail: None }
}

// ---- the ledger-backed perfect-engine double ----

/// Answers every step from its own private ledger — a correct engine by
/// construction. Used by unit tests (green legs), the standalone smoke, and
/// (wrapped) planted-red tests.
#[derive(Debug, Default)]
pub struct LedgerSimExecutor {
    ledger: Ledger,
}

impl LedgerSimExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    fn eval_pred(pred: &PredSpec, row: &Row) -> Option<bool> {
        match pred {
            PredSpec::ColModEq { col, m, r } => match row.0.get(*col) {
                Some(Value::Int(v)) => Some(v.rem_euclid(*m) == *r),
                Some(Value::Null) | None => None,
                Some(_) => None,
            },
        }
    }

    fn probe(&self, spec: &ProbeSpec) -> StmtResult {
        let missing = || StmtResult::Error { sqlstate: SQLSTATE_UNDEFINED_TABLE.to_string() };
        match spec {
            ProbeSpec::CountAll { table } => match self.ledger.table_cardinality(table) {
                None => missing(),
                Some(n) => StmtResult::Rows { rows: vec![Row(vec![Value::Int(n as i64)])] },
            },
            ProbeSpec::CountWhereKeyEq { table, key } => match self.ledger.table_rows(table) {
                None => missing(),
                Some(rows) => {
                    let key_idx = self.ledger.table(table).and_then(|t| t.key).unwrap_or(0);
                    let n = rows.iter().filter(|r| r.0.get(key_idx) == Some(key)).count();
                    StmtResult::Rows { rows: vec![Row(vec![Value::Int(n as i64)])] }
                }
            },
            ProbeSpec::SelectAll { table } => match self.ledger.table_rows(table) {
                None => missing(),
                Some(rows) => StmtResult::Rows { rows },
            },
            ProbeSpec::SelectColByKey { table, col, key } => match self.ledger.table_rows(table)
            {
                None => missing(),
                Some(rows) => {
                    let key_idx = self.ledger.table(table).and_then(|t| t.key).unwrap_or(0);
                    let out: Vec<Row> = rows
                        .iter()
                        .filter(|r| r.0.get(key_idx) == Some(key))
                        .map(|r| Row(vec![r.0[*col].clone()]))
                        .collect();
                    StmtResult::Rows { rows: out }
                }
            },
            ProbeSpec::CountWherePred { table, pred, sel } => {
                match self.ledger.table_rows(table) {
                    None => missing(),
                    Some(rows) => {
                        let n = rows
                            .iter()
                            .filter(|r| {
                                let p = Self::eval_pred(pred, r);
                                match sel {
                                    TriSel::True => p == Some(true),
                                    TriSel::False => p == Some(false),
                                    TriSel::Null => p.is_none(),
                                }
                            })
                            .count();
                        StmtResult::Rows { rows: vec![Row(vec![Value::Int(n as i64)])] }
                    }
                }
            }
            ProbeSpec::SumCol { table, col, filter } => match self.ledger.table_rows(table) {
                None => missing(),
                Some(rows) => {
                    let mut any = false;
                    let mut sum = 0i64;
                    for r in rows.iter().filter(|r| match filter {
                        None => true,
                        Some((pred, sel)) => {
                            let p = Self::eval_pred(pred, r);
                            match sel {
                                TriSel::True => p == Some(true),
                                TriSel::False => p == Some(false),
                                TriSel::Null => p.is_none(),
                            }
                        }
                    }) {
                        if let Some(Value::Int(v)) = r.0.get(*col) {
                            any = true;
                            sum = sum.wrapping_add(*v);
                        }
                    }
                    let v = if any { Value::Int(sum) } else { Value::Null };
                    StmtResult::Rows { rows: vec![Row(vec![v])] }
                }
            },
            ProbeSpec::NoRecSum { table, pred } => match self.ledger.table_rows(table) {
                None => missing(),
                Some(rows) => {
                    let n = rows
                        .iter()
                        .filter(|r| Self::eval_pred(pred, r) == Some(true))
                        .count();
                    StmtResult::Rows { rows: vec![Row(vec![Value::Int(n as i64)])] }
                }
            },
            ProbeSpec::SelectColAll { table, col, doubled } => {
                match self.ledger.table_rows(table) {
                    None => missing(),
                    Some(rows) => {
                        let mut out: Vec<Row> =
                            rows.iter().map(|r| Row(vec![r.0[*col].clone()])).collect();
                        if *doubled {
                            let copy = out.clone();
                            out.extend(copy);
                        }
                        StmtResult::Rows { rows: out }
                    }
                }
            }
            ProbeSpec::HookScalar { .. } => {
                // A well-behaved hook channel: constant watermark.
                StmtResult::Rows { rows: vec![Row(vec![Value::Int(0)])] }
            }
            ProbeSpec::Opaque => StmtResult::Command { affected: 0 },
        }
    }
}

impl StepExecutor for LedgerSimExecutor {
    fn exec_sql(&mut self, step: &SqlStep) -> StmtResult {
        if let Some(op) = &step.ledger_op {
            return match self.ledger.apply(op) {
                Ok(ApplyOutcome::Ok(eff)) => StmtResult::Command { affected: eff.affected },
                Ok(ApplyOutcome::Err(e)) => StmtResult::Error { sqlstate: e.sqlstate },
                Err(m) => panic!("sim executor misuse: {}", m.0),
            };
        }
        if let Some(spec) = &step.probe {
            return self.probe(spec);
        }
        StmtResult::Command { affected: 0 }
    }

    fn exec_tx(&mut self, ctl: &TxCtl) {
        match ctl {
            TxCtl::Begin(iso) => self.ledger.begin(*iso),
            TxCtl::Commit => self.ledger.commit(),
            TxCtl::Rollback => self.ledger.rollback(),
            TxCtl::Savepoint(n) => self.ledger.savepoint(n),
            TxCtl::RollbackTo(n) => {
                self.ledger.rollback_to(n).expect("sim savepoint exists");
            }
        }
    }

    fn exec_arm(&mut self, _ctl: &ArmCtl) {}
}

/// Wraps an executor and corrupts the result landing in one target slot —
/// the planted-red instrument for checker tests (G-O1) and smoke red legs.
pub struct PerturbExecutor<E: StepExecutor> {
    pub inner: E,
    pub target_slot: u32,
}

impl<E: StepExecutor> StepExecutor for PerturbExecutor<E> {
    fn exec_sql(&mut self, step: &SqlStep) -> StmtResult {
        let res = self.inner.exec_sql(step);
        if step.stackref == Some(self.target_slot) {
            return match res {
                StmtResult::Rows { mut rows } => {
                    // Corrupt: add a poison row (wrong multiset AND wrong scalar).
                    rows.push(Row(vec![Value::Int(999_999_999)]));
                    StmtResult::Rows { rows }
                }
                StmtResult::Command { affected } => {
                    StmtResult::Command { affected: affected + 1 }
                }
                StmtResult::Error { .. } => StmtResult::Command { affected: 0 },
            };
        }
        res
    }
    fn exec_tx(&mut self, ctl: &TxCtl) {
        self.inner.exec_tx(ctl)
    }
    fn exec_arm(&mut self, ctl: &ArmCtl) {
        self.inner.exec_arm(ctl)
    }
}
