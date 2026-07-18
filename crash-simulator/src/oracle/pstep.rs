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
    /// SELECT <col> FROM table [UNION ALL SELECT <col> FROM table]
    SelectColAll { table: String, col: usize, doubled: bool },
    /// Engine-hook scalar probe (F7/F8); sim answers a constant.
    HookScalar { hook: crate::oracle::check::HookKind },
    /// Anything the oracle does not model (noise, PREPARE/DEALLOCATE, index
    /// DDL). Sim answers Command{0}.
    Opaque,
}

/// Predicate vocabulary for TLP/NoREC/partition identities. 3-valued: a NULL
/// column makes the predicate UNKNOWN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredSpec {
    /// (col % m) = r  — NULL col => UNKNOWN
    ColModEq { col: usize, m: i64, r: i64 },
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
}

/// A property instance compiled to steps (the serial subset — no
/// SessionSwitch exists in this IR by construction, per contract §0 A1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyInstance {
    pub property: PropertyId,
    pub steps: Vec<PStep>,
    /// Touched-table set (feeds WS-GEN's table-dependency API for the
    /// WS-RUNNER shrinker).
    pub tables: BTreeSet<String>,
}
