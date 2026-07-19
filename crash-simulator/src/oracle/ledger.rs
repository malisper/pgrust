//! The model oracle: a LEDGER, not a database (spec §1.2, contract §3.1.2).
//!
//! Scope, exactly: row multisets for property-local tables under
//! ledger-understood DML (INSERT ... VALUES, key-addressed single-row
//! UPDATE/DELETE, TRUNCATE, transactional CREATE/DROP TABLE), per-connection
//! transaction bookkeeping (snapshot copy + op-log replay, savepoints as
//! op-log marks), SQLSTATE expectations (42P07, 23xxx, 42P01, ...),
//! checked in BOTH directions.
//!
//! ## THE PUNT FENCE (review fence — a PR adding any of these is rejected
//! on sight; Turso shadow-scaling lesson, load-bearing):
//! The ledger MUST NOT attempt expression evaluation, type coercion,
//! collation, aggregates/GROUP BY/window, ORDER BY semantics, LIKE/regex,
//! planner-dependent anything, TOAST, or functions. Anything beyond the
//! `LedgerOp` enum routes to the C differential. The enum IS the fence:
//! non-representable statements simply carry `ledger_op: None`.

use std::collections::BTreeMap;

use crate::oracle::check::{Row, Value};
use crate::oracle::pstep::IsoLevel;

pub const SQLSTATE_DUP_TABLE: &str = "42P07";
pub const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";
pub const SQLSTATE_UNIQUE_VIOLATION: &str = "23505";
pub const SQLSTATE_NOT_NULL_VIOLATION: &str = "23502";
pub const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";
pub const SQLSTATE_SERIALIZATION_FAILURE: &str = "40001";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub not_null: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    pub cols: Vec<ColumnDef>,
    /// Unique-key column index (properties generate key-addressed DML only
    /// against keyed tables).
    pub key: Option<usize>,
    /// Row multiset.
    rows: BTreeMap<Row, u64>,
}

impl TableState {
    fn cardinality(&self) -> u64 {
        self.rows.values().sum()
    }
    fn add_row(&mut self, row: Row) {
        *self.rows.entry(row).or_insert(0) += 1;
    }
    fn remove_row(&mut self, row: &Row) -> bool {
        if let Some(n) = self.rows.get_mut(row) {
            *n -= 1;
            if *n == 0 {
                self.rows.remove(row);
            }
            true
        } else {
            false
        }
    }
    fn find_by_key(&self, key_idx: usize, key: &Value) -> Option<Row> {
        self.rows.keys().find(|r| r.0.get(key_idx) == Some(key)).cloned()
    }
    fn expanded_rows(&self) -> Vec<Row> {
        let mut out = Vec::new();
        for (row, n) in &self.rows {
            for _ in 0..*n {
                out.push(row.clone());
            }
        }
        out // BTreeMap iteration => sorted, deterministic
    }
}

/// Ledger-understood operations. This enum is the punt fence (see module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerOp {
    CreateTable { table: String, cols: Vec<ColumnDef>, key: Option<usize> },
    DropTable { table: String },
    Truncate { table: String },
    InsertValues { table: String, rows: Vec<Row> },
    /// Single-row UPDATE addressed by unique key.
    UpdateByKey { table: String, key: Value, sets: Vec<(usize, Value)> },
    /// Single-row DELETE addressed by unique key.
    DeleteByKey { table: String, key: Value },
}

/// What the ledger expects the engine to do for an op: succeed with an
/// effect, or fail with a SQLSTATE (compared at class granularity).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    Ok(LedgerEffect),
    Err(ExpectedError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEffect {
    pub affected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedError {
    pub sqlstate: String,
}

/// Harness-side misuse (never an engine expectation): e.g. key-addressed op
/// against an unkeyed table. Tests panic on these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerMisuse(pub String);

#[derive(Debug, Clone)]
struct TxState {
    snapshot: BTreeMap<String, TableState>,
    /// Successful ops since BEGIN (statement-level rollback means failed
    /// statements never enter the log).
    oplog: Vec<LedgerOp>,
    /// (name, oplog mark). Savepoints are op-log marks (spec §1.2).
    savepoints: Vec<(String, usize)>,
    #[allow(dead_code)]
    iso: IsoLevel,
}

/// The ledger. Single-session at H1 (contract §0 A1) — one tx context.
#[derive(Debug, Clone, Default)]
pub struct Ledger {
    tables: BTreeMap<String, TableState>,
    tx: Option<TxState>,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn in_tx(&self) -> bool {
        self.tx.is_some()
    }

    /// Expected engine rows for a table (tx working state when in tx —
    /// which is exactly what read-your-writes must see). Sorted (multiset
    /// canonical form).
    pub fn table_rows(&self, table: &str) -> Option<Vec<Row>> {
        self.tables.get(table).map(|t| t.expanded_rows())
    }

    pub fn table_cardinality(&self, table: &str) -> Option<u64> {
        self.tables.get(table).map(|t| t.cardinality())
    }

    pub fn table(&self, name: &str) -> Option<&TableState> {
        self.tables.get(name)
    }

    // ---- transaction bookkeeping: snapshot copy + op-log replay ----

    pub fn begin(&mut self, iso: IsoLevel) {
        debug_assert!(self.tx.is_none(), "nested BEGIN (harness bug)");
        self.tx = Some(TxState {
            snapshot: self.tables.clone(),
            oplog: Vec::new(),
            savepoints: Vec::new(),
            iso,
        });
    }

    pub fn commit(&mut self) {
        self.tx = None; // working state is already the committed state
    }

    pub fn rollback(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.tables = tx.snapshot;
        }
    }

    pub fn savepoint(&mut self, name: &str) {
        if let Some(tx) = self.tx.as_mut() {
            tx.savepoints.push((name.to_string(), tx.oplog.len()));
        }
    }

    /// ROLLBACK TO SAVEPOINT: truncate the op-log to the (most recent) mark
    /// with this name and rebuild working state from snapshot + replay.
    /// The named savepoint survives (PG semantics).
    pub fn rollback_to(&mut self, name: &str) -> Result<(), LedgerMisuse> {
        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| LedgerMisuse("ROLLBACK TO outside tx".into()))?;
        let idx = tx
            .savepoints
            .iter()
            .rposition(|(n, _)| n == name)
            .ok_or_else(|| LedgerMisuse(format!("no savepoint {name}")))?;
        let mark = tx.savepoints[idx].1;
        tx.savepoints.truncate(idx + 1);
        let replay: Vec<LedgerOp> = tx.oplog[..mark].to_vec();
        tx.oplog.truncate(mark);
        let snapshot = tx.snapshot.clone();
        self.tables = snapshot;
        for op in &replay {
            let out = apply_to(&mut self.tables, op)
                .map_err(|m| LedgerMisuse(format!("replay misuse: {}", m.0)))?;
            debug_assert!(
                matches!(out, ApplyOutcome::Ok(_)),
                "op-log replay must re-succeed: {op:?}"
            );
        }
        Ok(())
    }

    /// SIM-HARNESS-CONVERGE (fault leg): the crash-committed view — the
    /// state a correct engine must expose after crash recovery if the node
    /// died NOW: an open transaction rolls back (crash semantics), committed
    /// multisets remain. Returns (table, column names in order, committed
    /// rows — sorted, the multiset canonical form) for every tracked table.
    pub fn crash_committed(&self) -> Vec<(String, Vec<String>, Vec<Row>)> {
        let mut l = self.clone();
        l.rollback(); // no-op outside a tx
        l.tables
            .iter()
            .map(|(name, ts)| {
                (
                    name.clone(),
                    ts.cols.iter().map(|c| c.name.clone()).collect(),
                    ts.expanded_rows(),
                )
            })
            .collect()
    }

    /// Apply a ledger op to working state; returns what the ENGINE is
    /// expected to do. State changes only on Ok (statement-level rollback:
    /// an expected-error op leaves the ledger untouched).
    pub fn apply(&mut self, op: &LedgerOp) -> Result<ApplyOutcome, LedgerMisuse> {
        let out = apply_to(&mut self.tables, op)?;
        if matches!(out, ApplyOutcome::Ok(_)) {
            if let Some(tx) = self.tx.as_mut() {
                tx.oplog.push(op.clone());
            }
        }
        Ok(out)
    }
}

fn apply_to(
    tables: &mut BTreeMap<String, TableState>,
    op: &LedgerOp,
) -> Result<ApplyOutcome, LedgerMisuse> {
    fn err(state: &str) -> Result<ApplyOutcome, LedgerMisuse> {
        Ok(ApplyOutcome::Err(ExpectedError { sqlstate: state.to_string() }))
    }
    match op {
        LedgerOp::CreateTable { table, cols, key } => {
            if tables.contains_key(table) {
                return err(SQLSTATE_DUP_TABLE);
            }
            if let Some(k) = key {
                if *k >= cols.len() {
                    return Err(LedgerMisuse(format!("key index {k} out of range")));
                }
            }
            tables.insert(
                table.clone(),
                TableState { cols: cols.clone(), key: *key, rows: BTreeMap::new() },
            );
            Ok(ApplyOutcome::Ok(LedgerEffect { affected: 0 }))
        }
        LedgerOp::DropTable { table } => {
            if tables.remove(table).is_none() {
                return err(SQLSTATE_UNDEFINED_TABLE);
            }
            Ok(ApplyOutcome::Ok(LedgerEffect { affected: 0 }))
        }
        LedgerOp::Truncate { table } => match tables.get_mut(table) {
            None => err(SQLSTATE_UNDEFINED_TABLE),
            Some(t) => {
                t.rows.clear();
                Ok(ApplyOutcome::Ok(LedgerEffect { affected: 0 }))
            }
        },
        LedgerOp::InsertValues { table, rows } => {
            let t = match tables.get(table) {
                None => return err(SQLSTATE_UNDEFINED_TABLE),
                Some(t) => t,
            };
            // Validate the whole statement first (statement-level rollback:
            // all-or-nothing).
            for row in rows {
                if row.0.len() != t.cols.len() {
                    return Err(LedgerMisuse(format!(
                        "row arity {} != table arity {}",
                        row.0.len(),
                        t.cols.len()
                    )));
                }
                for (i, col) in t.cols.iter().enumerate() {
                    if col.not_null && row.0[i] == Value::Null {
                        return err(SQLSTATE_NOT_NULL_VIOLATION);
                    }
                }
            }
            if let Some(k) = t.key {
                let mut seen: Vec<&Value> = Vec::new();
                for row in rows {
                    let kv = &row.0[k];
                    if seen.contains(&kv) || t.find_by_key(k, kv).is_some() {
                        return err(SQLSTATE_UNIQUE_VIOLATION);
                    }
                    seen.push(kv);
                }
            }
            let t = tables.get_mut(table).expect("checked above");
            for row in rows {
                t.add_row(row.clone());
            }
            Ok(ApplyOutcome::Ok(LedgerEffect { affected: rows.len() as u64 }))
        }
        LedgerOp::UpdateByKey { table, key, sets } => {
            let t = match tables.get(table) {
                None => return err(SQLSTATE_UNDEFINED_TABLE),
                Some(t) => t,
            };
            let k = t
                .key
                .ok_or_else(|| LedgerMisuse(format!("UpdateByKey on unkeyed table {table}")))?;
            let old = match t.find_by_key(k, key) {
                None => return Ok(ApplyOutcome::Ok(LedgerEffect { affected: 0 })),
                Some(r) => r,
            };
            let mut new = old.clone();
            for (i, v) in sets {
                if *i >= new.0.len() {
                    return Err(LedgerMisuse(format!("set index {i} out of range")));
                }
                if t.cols[*i].not_null && *v == Value::Null {
                    return err(SQLSTATE_NOT_NULL_VIOLATION);
                }
                new.0[*i] = v.clone();
            }
            // Key collision with a DIFFERENT row?
            let new_key = &new.0[k];
            if new_key != key {
                if t.find_by_key(k, new_key).is_some() {
                    return err(SQLSTATE_UNIQUE_VIOLATION);
                }
            }
            let t = tables.get_mut(table).expect("checked above");
            t.remove_row(&old);
            t.add_row(new);
            Ok(ApplyOutcome::Ok(LedgerEffect { affected: 1 }))
        }
        LedgerOp::DeleteByKey { table, key } => {
            let t = match tables.get(table) {
                None => return err(SQLSTATE_UNDEFINED_TABLE),
                Some(t) => t,
            };
            let k = t
                .key
                .ok_or_else(|| LedgerMisuse(format!("DeleteByKey on unkeyed table {table}")))?;
            match t.find_by_key(k, key) {
                None => Ok(ApplyOutcome::Ok(LedgerEffect { affected: 0 })),
                Some(row) => {
                    let t = tables.get_mut(table).expect("checked above");
                    t.remove_row(&row);
                    Ok(ApplyOutcome::Ok(LedgerEffect { affected: 1 }))
                }
            }
        }
    }
}

// ---- both-direction reconciliation (spec §1.2: engine-ok-but-ledger-errors
// is a bug; engine-rejects-but-ledger-accepts is a bug) ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineDmlResult {
    Ok { affected: u64 },
    Err { sqlstate: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileVerdict {
    Match,
    /// Engine succeeded where the ledger says the op must fail.
    EngineOkLedgerErr { expected: String },
    /// Engine rejected an op the ledger accepts.
    EngineErrLedgerOk { got: String },
    /// Both error, different SQLSTATE class.
    SqlStateClassMismatch { expected: String, got: String },
    /// Both ok, different affected-row counts.
    EffectMismatch { expected: u64, got: u64 },
}

impl ReconcileVerdict {
    pub fn is_violation(&self) -> bool {
        !matches!(self, ReconcileVerdict::Match)
    }
}

pub fn reconcile(expected: &ApplyOutcome, engine: &EngineDmlResult) -> ReconcileVerdict {
    match (expected, engine) {
        (ApplyOutcome::Ok(eff), EngineDmlResult::Ok { affected }) => {
            if eff.affected == *affected {
                ReconcileVerdict::Match
            } else {
                ReconcileVerdict::EffectMismatch { expected: eff.affected, got: *affected }
            }
        }
        (ApplyOutcome::Ok(_), EngineDmlResult::Err { sqlstate }) => {
            ReconcileVerdict::EngineErrLedgerOk { got: sqlstate.clone() }
        }
        (ApplyOutcome::Err(e), EngineDmlResult::Ok { .. }) => {
            ReconcileVerdict::EngineOkLedgerErr { expected: e.sqlstate.clone() }
        }
        (ApplyOutcome::Err(e), EngineDmlResult::Err { sqlstate }) => {
            if e.sqlstate.get(..2) == sqlstate.get(..2) {
                ReconcileVerdict::Match
            } else {
                ReconcileVerdict::SqlStateClassMismatch {
                    expected: e.sqlstate.clone(),
                    got: sqlstate.clone(),
                }
            }
        }
    }
}

/// check_after_dml comparator: engine `SELECT *` multiset vs ledger.
pub fn check_table_multiset(expected: &[Row], engine_rows: &[Row]) -> Result<(), String> {
    let mut got = engine_rows.to_vec();
    got.sort();
    // `expected` from Ledger::table_rows is already sorted.
    if expected == got.as_slice() {
        Ok(())
    } else {
        Err(format!(
            "post-DML content mismatch: ledger {} rows, engine {} rows",
            expected.len(),
            got.len()
        ))
    }
}
