//! Result stack + ASSERT/ASSUME check evaluation (contract §3.1.3).
//!
//! A `Check` serializes to single-line JSON — the `-- ASSERT <check-json>` /
//! `-- ASSUME <check-json>` annotation payload of plan-format v1 (§1.5).
//! Round-trip is property-tested. Evaluation is pure over (result stack,
//! ledger, hook probe): same inputs => same outcome, no ambient state
//! (determinism law §0 A3 — ordered containers only).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::oracle::ledger::Ledger;

/// Ledger value domain (deliberately tiny — the punt fence).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Text(String),
}

impl Value {
    /// SQL literal rendering for generated statements.
    pub fn sql(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Text(t) => format!("'{}'", t.replace('\'', "''")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Row(pub Vec<Value>);

/// Result of one executed statement, in comparable form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StmtResult {
    Rows { rows: Vec<Row> },
    Command { affected: u64 },
    Error { sqlstate: String },
}

/// Slot-addressed result stack. Slots are assigned at generation time and
/// survive noise insertion / shrinking (see pstep.rs).
#[derive(Debug, Clone, Default)]
pub struct ResultStack {
    slots: BTreeMap<u32, StmtResult>,
}

impl ResultStack {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn put(&mut self, slot: u32, res: StmtResult) {
        self.slots.insert(slot, res);
    }
    pub fn get(&self, slot: u32) -> Option<&StmtResult> {
        self.slots.get(&slot)
    }
    pub fn clear(&mut self) {
        self.slots.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookKind {
    /// F7 memory-context watermark channel (engine mini-lane, contract §0 A5).
    Memory,
    /// F8 fd/pin/lock/snapshot counters.
    Resource,
}

/// Engine-hook capability probe (contract §0 A5). Hook absent => property
/// auto-skipped with a counted `SIMHARNESS|skipped-no-hook|n`, never a
/// silent pass.
pub trait HookProbe {
    fn has(&self, hook: HookKind) -> bool;
}

/// The default H1 posture: no engine hooks present.
pub struct NoHooks;
impl HookProbe for NoHooks {
    fn has(&self, _hook: HookKind) -> bool {
        false
    }
}

/// All hooks present (tests / future engine mini-lane wiring).
pub struct AllHooks;
impl HookProbe for AllHooks {
    fn has(&self, _hook: HookKind) -> bool {
        true
    }
}

/// The check vocabulary (single-line JSON in plan annotations).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Check {
    /// Slot's result is a single row, single column equal to `value`.
    ScalarEq { slot: u32, value: Value },
    /// Two scalar slots are equal (NoREC count equivalence).
    ScalarPairEq { a: u32, b: u32 },
    /// Sum of scalar parts (NULL => 0) equals scalar whole (TLP partitions,
    /// SUM-composes). Exact integer types only (R7).
    ScalarSumEq { parts: Vec<u32>, whole: u32 },
    /// Slot's row count equals `value`.
    RowCountEq { slot: u32, value: u64 },
    /// Slot's row count <= `value` (LIMIT bound identity).
    CardLe { slot: u32, value: u64 },
    /// Two slots hold multiset-equal row sets (relaxed order law: multiset
    /// comparison always; order divergence is not a finding — wrongresults R1).
    MultisetEq { a: u32, b: u32 },
    /// UNION ALL doubling: multiset(doubled) == single ⊎ single.
    UnionDoubling { single: u32, doubled: u32 },
    /// Slot errored with this SQLSTATE class (compared on the 2-char class,
    /// never message text — spec §1.2).
    SqlStateClass { slot: u32, class: String },
    /// Slot did not error.
    StmtOk { slot: u32 },
    /// Slot's rows are multiset-equal to the ledger's expectation for the
    /// table (check_after_dml / read-your-writes against tx working state).
    LedgerTable { table: String, slot: u32 },
    /// Slot's scalar equals the ledger cardinality of the table.
    CountMatchesLedger { table: String, slot: u32 },
    /// ASSUME-gate for F7/F8: the engine hook is present.
    HookPresent { hook: HookKind },
    /// F7/F8 watermark equality between two hook-probe scalars.
    HookBaseline { hook: HookKind, before: u32, after: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    Pass,
    Fail(String),
    /// Contract §0 A5: hook absent — counted skip, never a silent pass.
    SkipNoHook,
}

fn slot_res<'a>(stack: &'a ResultStack, slot: u32) -> Result<&'a StmtResult, String> {
    stack
        .get(slot)
        .ok_or_else(|| format!("missing result-stack slot {slot} (harness wiring bug)"))
}

fn scalar(stack: &ResultStack, slot: u32) -> Result<Value, String> {
    match slot_res(stack, slot)? {
        StmtResult::Rows { rows } => {
            if rows.len() != 1 || rows[0].0.len() != 1 {
                return Err(format!(
                    "slot {slot}: expected 1x1 scalar result, got {}x{}",
                    rows.len(),
                    rows.first().map(|r| r.0.len()).unwrap_or(0)
                ));
            }
            Ok(rows[0].0[0].clone())
        }
        StmtResult::Command { .. } => Err(format!("slot {slot}: command result, not scalar")),
        StmtResult::Error { sqlstate } => {
            Err(format!("slot {slot}: errored ({sqlstate}) where scalar expected"))
        }
    }
}

fn rows_of<'a>(stack: &'a ResultStack, slot: u32) -> Result<&'a [Row], String> {
    match slot_res(stack, slot)? {
        StmtResult::Rows { rows } => Ok(rows),
        StmtResult::Command { .. } => Err(format!("slot {slot}: command result, not rows")),
        StmtResult::Error { sqlstate } => {
            Err(format!("slot {slot}: errored ({sqlstate}) where rows expected"))
        }
    }
}

fn scalar_as_i64(v: &Value, ctx: &str) -> Result<i64, String> {
    match v {
        Value::Null => Ok(0), // SUM over empty partition is NULL; identity treats it as 0
        Value::Int(i) => Ok(*i),
        other => Err(format!("{ctx}: non-integer scalar {other:?} (R7: exact types only)")),
    }
}

fn sorted(rows: &[Row]) -> Vec<Row> {
    let mut v = rows.to_vec();
    v.sort();
    v
}

pub fn eval_check(
    check: &Check,
    stack: &ResultStack,
    ledger: &Ledger,
    hooks: &dyn HookProbe,
) -> CheckOutcome {
    let r = eval_inner(check, stack, ledger, hooks);
    match r {
        Ok(Some(())) => CheckOutcome::Pass,
        Ok(None) => CheckOutcome::SkipNoHook,
        Err(e) => CheckOutcome::Fail(e),
    }
}

fn eval_inner(
    check: &Check,
    stack: &ResultStack,
    ledger: &Ledger,
    hooks: &dyn HookProbe,
) -> Result<Option<()>, String> {
    match check {
        Check::ScalarEq { slot, value } => {
            let got = scalar(stack, *slot)?;
            if &got == value {
                Ok(Some(()))
            } else {
                Err(format!("slot {slot}: scalar {got:?} != expected {value:?}"))
            }
        }
        Check::ScalarPairEq { a, b } => {
            let va = scalar(stack, *a)?;
            let vb = scalar(stack, *b)?;
            if va == vb {
                Ok(Some(()))
            } else {
                Err(format!("scalar mismatch: slot {a}={va:?} vs slot {b}={vb:?}"))
            }
        }
        Check::ScalarSumEq { parts, whole } => {
            let mut sum = 0i64;
            for p in parts {
                sum = sum.wrapping_add(scalar_as_i64(&scalar(stack, *p)?, "part")?);
            }
            let w = scalar_as_i64(&scalar(stack, *whole)?, "whole")?;
            if sum == w {
                Ok(Some(()))
            } else {
                Err(format!("partition sum {sum} != whole {w} (parts {parts:?} whole {whole})"))
            }
        }
        Check::RowCountEq { slot, value } => {
            let n = rows_of(stack, *slot)?.len() as u64;
            if n == *value {
                Ok(Some(()))
            } else {
                Err(format!("slot {slot}: row count {n} != expected {value}"))
            }
        }
        Check::CardLe { slot, value } => {
            let n = rows_of(stack, *slot)?.len() as u64;
            if n <= *value {
                Ok(Some(()))
            } else {
                Err(format!("slot {slot}: row count {n} exceeds LIMIT bound {value}"))
            }
        }
        Check::MultisetEq { a, b } => {
            let ra = sorted(rows_of(stack, *a)?);
            let rb = sorted(rows_of(stack, *b)?);
            if ra == rb {
                Ok(Some(()))
            } else {
                Err(format!(
                    "multiset mismatch: slot {a} ({} rows) vs slot {b} ({} rows)",
                    ra.len(),
                    rb.len()
                ))
            }
        }
        Check::UnionDoubling { single, doubled } => {
            let s = rows_of(stack, *single)?;
            let mut twice = s.to_vec();
            twice.extend_from_slice(s);
            twice.sort();
            let d = sorted(rows_of(stack, *doubled)?);
            if twice == d {
                Ok(Some(()))
            } else {
                Err(format!(
                    "UNION ALL doubling violated: 2x{} rows expected, got {}",
                    s.len(),
                    d.len()
                ))
            }
        }
        Check::SqlStateClass { slot, class } => match slot_res(stack, *slot)? {
            StmtResult::Error { sqlstate } => {
                if sqlstate.get(..2) == class.get(..2) {
                    Ok(Some(()))
                } else {
                    Err(format!(
                        "slot {slot}: SQLSTATE {sqlstate} not in expected class {class}"
                    ))
                }
            }
            _ => Err(format!(
                "slot {slot}: engine succeeded where SQLSTATE class {class} expected \
                 (engine-ok/ledger-errors direction)"
            )),
        },
        Check::StmtOk { slot } => match slot_res(stack, *slot)? {
            StmtResult::Error { sqlstate } => Err(format!(
                "slot {slot}: engine rejected ({sqlstate}) a statement the oracle accepts \
                 (engine-rejects/ledger-accepts direction)"
            )),
            _ => Ok(Some(())),
        },
        Check::LedgerTable { table, slot } => {
            let got = sorted(rows_of(stack, *slot)?);
            let want = ledger
                .table_rows(table)
                .ok_or_else(|| format!("ledger has no table {table}"))?;
            if got == want {
                Ok(Some(()))
            } else {
                Err(format!(
                    "table {table}: engine multiset ({} rows) != ledger ({} rows)",
                    got.len(),
                    want.len()
                ))
            }
        }
        Check::CountMatchesLedger { table, slot } => {
            let got = scalar_as_i64(&scalar(stack, *slot)?, "count")?;
            let want = ledger
                .table_cardinality(table)
                .ok_or_else(|| format!("ledger has no table {table}"))? as i64;
            if got == want {
                Ok(Some(()))
            } else {
                Err(format!("table {table}: engine count {got} != ledger cardinality {want}"))
            }
        }
        Check::HookPresent { hook } => {
            if hooks.has(*hook) {
                Ok(Some(()))
            } else {
                Ok(None)
            }
        }
        Check::HookBaseline { hook, before, after } => {
            if !hooks.has(*hook) {
                return Ok(None);
            }
            let b = scalar(stack, *before)?;
            let a = scalar(stack, *after)?;
            if a == b {
                Ok(Some(()))
            } else {
                Err(format!("{hook:?} baseline not restored: before {b:?} after {a:?}"))
            }
        }
    }
}

/// Single-line JSON for `-- ASSERT` / `-- ASSUME` annotations.
pub fn check_to_json(check: &Check) -> String {
    serde_json::to_string(check).expect("check serializes")
}

pub fn check_from_json(s: &str) -> Result<Check, String> {
    serde_json::from_str(s).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_json_roundtrip() {
        let checks = vec![
            Check::ScalarEq { slot: 3, value: Value::Int(7) },
            Check::ScalarPairEq { a: 1, b: 2 },
            Check::ScalarSumEq { parts: vec![1, 2, 3], whole: 0 },
            Check::RowCountEq { slot: 9, value: 0 },
            Check::CardLe { slot: 4, value: 10 },
            Check::MultisetEq { a: 5, b: 6 },
            Check::UnionDoubling { single: 1, doubled: 2 },
            Check::SqlStateClass { slot: 2, class: "23".into() },
            Check::StmtOk { slot: 8 },
            Check::LedgerTable { table: "t1".into(), slot: 0 },
            Check::CountMatchesLedger { table: "t1".into(), slot: 1 },
            Check::HookPresent { hook: HookKind::Memory },
            Check::HookBaseline { hook: HookKind::Resource, before: 1, after: 2 },
        ];
        for c in checks {
            let j = check_to_json(&c);
            assert!(!j.contains('\n'), "single-line JSON law: {j}");
            let back = check_from_json(&j).unwrap();
            assert_eq!(back, c, "round-trip: {j}");
        }
    }
}
