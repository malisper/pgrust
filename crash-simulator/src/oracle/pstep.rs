//! Property-step IR — the WS-ORACLE side of the property generate/check
//! surface (contract §3.1.1: "the generate-side struct definitions live with
//! the property in src/oracle/ so one file owns a property end-to-end;
//! WS-GEN calls through a trait").
//!
//! COORDINATION NOTE (worklog notes/h1-ws-oracle.md): WS-GEN's `plan.rs`
//! (inc-1) is the authoritative plan IR and had not landed when this module
//! was written. Properties emit this minimal oracle-side IR; WS-GEN lowers
//! `PStep` into plan steps 1:1 (Sql -> Ddl/Dml/Query by mark+content, Tx ->
//! Tx, Arm -> Arm, Assume/Assert -> Assumption/Assertion, NoiseSlot ->
//! constrained placeholder noise). If inc-1's trait surface differs, this
//! module adapts to it — not the reverse.

use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::ledger::LedgerOp;
use crate::oracle::props::PropertyId;

/// Mutation-split mark (dualexec law; ambiguous => Mutation, fail-safe).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Read,
    Mutation,
    Passthrough,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsoLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsoLevel {
    pub fn begin_sql(&self) -> &'static str {
        match self {
            IsoLevel::ReadCommitted => "BEGIN ISOLATION LEVEL READ COMMITTED",
            IsoLevel::RepeatableRead => "BEGIN ISOLATION LEVEL REPEATABLE READ",
            IsoLevel::Serializable => "BEGIN ISOLATION LEVEL SERIALIZABLE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxCtl {
    Begin(IsoLevel),
    Commit,
    Rollback,
    Savepoint(String),
    RollbackTo(String),
}

/// GUC arms. RESET ALL per the 1session GUC-leak law.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArmCtl {
    SetGuc(String, String),
    ResetAll,
}

/// Per-Sql metadata consumed by the comparison ladder (wrongresults R-screens
/// are set at generation; the triage.py regexes are the backstop).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlMeta {
    /// R2: true when the statement's correct answer is not unique (e.g.
    /// LIMIT without a same-depth ORDER BY over a unique key). The ladder
    /// then compares shape only.
    pub order_underdetermined: bool,
    /// R7: true only under a float-lenient profile; float aggregates never
    /// appear in compared positions otherwise.
    pub float_lenient: bool,
}

/// Structured semantics of generated probe queries. Because WE generate the
/// SQL, its meaning is knowable without parsing; this is what lets the
/// ledger-backed sim executor (and, later, an in-sim driver) answer probes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeSpec {
    /// SELECT count(*) FROM table
    CountAll { table: String },
    /// SELECT count(*) FROM table WHERE <key> = key
    CountWhereKeyEq { table: String, key: crate::oracle::check::Value },
    /// SELECT * FROM table (all columns, generation order)
    SelectAll { table: String },
    /// SELECT <col> FROM table WHERE <key> = key
    SelectColByKey { table: String, col: usize, key: crate::oracle::check::Value },
    /// SELECT count(*) FROM table WHERE <pred 3VL-selected partition>
    CountWherePred { table: String, pred: PredSpec, sel: TriSel },
    /// SELECT sum(<col>) FROM table [WHERE pred partition]
    SumCol { table: String, col: usize, filter: Option<(PredSpec, TriSel)> },
    /// NoREC form: SELECT sum(CASE WHEN p THEN 1 ELSE 0 END) FROM table
    NoRecSum { table: String, pred: PredSpec },
    /// SELECT <all cols> FROM table [WHERE <pred partition>] — full rows of a
    /// 3VL partition; `sel: None` = the unpartitioned whole (WHERE-TLP).
    RowsWherePred { table: String, pred: PredSpec, sel: Option<TriSel> },
    /// SELECT DISTINCT <col> FROM table [WHERE <pred partition>] (DISTINCT-TLP)
    DistinctCol { table: String, col: usize, filter: Option<(PredSpec, TriSel)> },
    /// SELECT min|max(<col>) FROM table [WHERE <pred partition>] (MIN/MAX-TLP)
    ExtremeCol { table: String, col: usize, filter: Option<(PredSpec, TriSel)>, max: bool },
    /// SELECT (p) FROM table — the NoREC non-optimizable form: one boolean
    /// column per row ('t'/'f'/NULL); trues are counted harness-side.
    PredProjection { table: String, pred: PredSpec },
    /// SELECT <col> FROM table [UNION ALL SELECT <col> FROM table]
    SelectColAll { table: String, col: usize, doubled: bool },
    /// Engine-hook scalar probe (F7/F8); sim answers a constant.
    HookScalar { hook: crate::oracle::check::HookKind },
    /// H8 cursor walks: the generator computed this step's expected rows
    /// from the cursor position model; the sim answers them verbatim (a
    /// well-behaved portal), keeping RowsEq asserts sim-green.
    KnownRows { rows: Vec<crate::oracle::check::Row> },
    /// H8 MOVE steps: known command-tag count (`MOVE n`).
    KnownCommand { count: u64 },
    /// Anything the oracle does not model (noise, PREPARE/DEALLOCATE, index
    /// DDL). Sim answers Command{0}.
    Opaque,
}

/// Comparison operator for `PredSpec::ColCmp` (SQL 3VL comparison: a NULL
/// operand makes the comparison UNKNOWN).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Lt,
    Le,
    Eq,
    Ge,
    Gt,
    Ne,
}

impl CmpOp {
    pub fn sql(self) -> &'static str {
        match self {
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Eq => "=",
            CmpOp::Ge => ">=",
            CmpOp::Gt => ">",
            CmpOp::Ne => "<>",
        }
    }
}

/// Predicate vocabulary for TLP/NoREC/partition identities. 3-valued
/// (Kleene logic, PostgreSQL semantics): a NULL column makes a comparison
/// UNKNOWN; AND/OR/NOT propagate UNKNOWN per Kleene truth tables; IS NULL is
/// always two-valued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredSpec {
    /// (col % m) = r  — NULL col => UNKNOWN
    ColModEq { col: usize, m: i64, r: i64 },
    /// col <op> literal — NULL col (or NULL literal) => UNKNOWN
    ColCmp { col: usize, op: CmpOp, lit: crate::oracle::check::Value },
    /// col IS NULL — two-valued, never UNKNOWN
    ColIsNull { col: usize },
    Not(Box<PredSpec>),
    And(Box<PredSpec>, Box<PredSpec>),
    Or(Box<PredSpec>, Box<PredSpec>),
}

impl PredSpec {
    /// SQL rendering. `cols[i]` is the SQL name (possibly qualified, e.g.
    /// `b.id`) of predicate column index `i`.
    pub fn sql(&self, cols: &[&str]) -> String {
        match self {
            PredSpec::ColModEq { col, m, r } => format!("({} % {m}) = {r}", cols[*col]),
            PredSpec::ColCmp { col, op, lit } => {
                format!("{} {} {}", cols[*col], op.sql(), lit.sql())
            }
            PredSpec::ColIsNull { col } => format!("{} IS NULL", cols[*col]),
            PredSpec::Not(p) => format!("NOT ({})", p.sql(cols)),
            PredSpec::And(a, b) => format!("({}) AND ({})", a.sql(cols), b.sql(cols)),
            PredSpec::Or(a, b) => format!("({}) OR ({})", a.sql(cols), b.sql(cols)),
        }
    }

    /// Kleene 3VL evaluation over a row of `check::Value`s: `Some(bool)` when
    /// determined, `None` = UNKNOWN. Must agree with PostgreSQL's semantics
    /// for the generated vocabulary — this is the sim/oracle side of every
    /// partition probe.
    pub fn eval3(&self, row: &crate::oracle::check::Row) -> Option<bool> {
        use crate::oracle::check::Value;
        match self {
            PredSpec::ColModEq { col, m, r } => match row.0.get(*col) {
                Some(Value::Int(v)) => Some(v.rem_euclid(*m) == *r),
                _ => None,
            },
            PredSpec::ColCmp { col, op, lit } => {
                let v = row.0.get(*col)?;
                let ord = match (v, lit) {
                    (Value::Null, _) | (_, Value::Null) => return None,
                    (Value::Int(a), Value::Int(b)) => a.cmp(b),
                    // C-collation byte compare (harness boots initdb
                    // --no-locale); generation only pairs text with text.
                    (Value::Text(a), Value::Text(b)) => a.cmp(b),
                    _ => return None, // type-mismatched compare is never generated
                };
                Some(match op {
                    CmpOp::Lt => ord.is_lt(),
                    CmpOp::Le => ord.is_le(),
                    CmpOp::Eq => ord.is_eq(),
                    CmpOp::Ge => ord.is_ge(),
                    CmpOp::Gt => ord.is_gt(),
                    CmpOp::Ne => ord.is_ne(),
                })
            }
            PredSpec::ColIsNull { col } => {
                Some(matches!(row.0.get(*col), Some(Value::Null) | None))
            }
            PredSpec::Not(p) => p.eval3(row).map(|b| !b),
            PredSpec::And(a, b) => match (a.eval3(row), b.eval3(row)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            PredSpec::Or(a, b) => match (a.eval3(row), b.eval3(row)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
        }
    }

    /// Does `row` fall in the `sel` partition of this predicate?
    pub fn in_partition(&self, row: &crate::oracle::check::Row, sel: TriSel) -> bool {
        let p = self.eval3(row);
        match sel {
            TriSel::True => p == Some(true),
            TriSel::False => p == Some(false),
            TriSel::Null => p.is_none(),
        }
    }
}

/// Render one TLP partition arm of `pred` as a WHERE-clause body.
pub fn partition_sql(pred: &PredSpec, sel: TriSel, cols: &[&str]) -> String {
    let p = pred.sql(cols);
    match sel {
        TriSel::True => format!("({p})"),
        TriSel::False => format!("NOT ({p})"),
        TriSel::Null => format!("({p}) IS NULL"),
    }
}

/// Which 3VL partition a probe selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriSel {
    True,
    False,
    Null,
}

/// One SQL step emitted by a property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlStep {
    pub sql: String,
    pub mark: Mark,
    pub meta: SqlMeta,
    /// Ledger-understood DML/DDL: the oracle applies + reconciles.
    /// None for probes/noise (the punt fence: anything not expressible as a
    /// LedgerOp routes to the C differential, never to ledger emulation).
    pub ledger_op: Option<LedgerOp>,
    /// Structured probe semantics (see ProbeSpec).
    pub probe: Option<ProbeSpec>,
    /// Result-stack slot this step's result lands in. Slot ids are assigned
    /// at generation and are stable under noise insertion and shrinking
    /// (checks reference slots, never positional indices).
    pub stackref: Option<u32>,
}

/// Constraint on a noise slot (contract §2.1.2 constrained placeholder
/// noise; WS-GEN substitutes real queries filtered through this).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoiseConstraint {
    /// Noise must not read or write these tables (conservative form of
    /// "must not delete the inserted row / drop/rename the table").
    MustNotTouch(BTreeSet<String>),
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PStep {
    Sql(SqlStep),
    Tx(TxCtl),
    Arm(ArmCtl),
    Assume(Check),
    Assert(Check),
    /// Placeholder for WS-GEN's constrained noise substitution.
    NoiseSlot(NoiseConstraint),
    /// H8 multi-session estate (plan-format v2). `Session` switches the
    /// active session for the following steps (0 = primary); `AsyncSql`
    /// dispatches without waiting (the statement is expected to block);
    /// `Join` collects a session's outstanding statement — its outcome
    /// lands in `slot` for slot-addressed checks; `WaitUntil` polls the
    /// active session until the query returns scalar 't'.
    ///
    /// Oracle alignment law: the runner reports NO outcome for `Session` /
    /// `AsyncSql` / `WaitUntil` (silent steps) and reports the joined
    /// outcome for `Join` — `bridge::OracleCheckEval` advances its cursor
    /// past silent steps symmetrically. The ledger is single-session: while
    /// the model session is non-zero, `Tx` steps do NOT touch the ledger
    /// (multi-session properties assert through slots, never the ledger).
    Session(u32),
    AsyncSql(SqlStep),
    Join { session: u32, slot: Option<u32> },
    WaitUntil(SqlStep),
}

/// A property instance compiled to steps. Serial properties never contain
/// session-family steps (§0 A1 posture preserved); H8 multi-session
/// properties (M2/S1 class) do, and must return to session 0 before their
/// last step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyInstance {
    pub property: PropertyId,
    pub steps: Vec<PStep>,
    /// Touched-table set (feeds WS-GEN's table-dependency API for the
    /// WS-RUNNER shrinker).
    pub tables: BTreeSet<String>,
}
