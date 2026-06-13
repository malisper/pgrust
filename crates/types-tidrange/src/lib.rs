//! Node-state vocabulary for `backend-executor-nodeTidrangescan`.
//!
//! These types appear in the signatures of the node's seams, so they live in a
//! types crate that both the owning node crate and its `-seams` crate can name.
//!
//! `TidOpExpr` / `TidRangeScanState` mirror `nodeTidrangescan.c`. The C node's
//! `trss_tidexprs` is a `List *` of `palloc`'d `TidOpExpr` records; here it is a
//! [`PgVec`] charged to the per-query memory context (the `'mcx` the executor
//! tree carries), freed when that context is reset at `ExecEndNode` — the
//! faithful analog of the C node's per-query context.
//!
//! The C node keeps `ss_currentRelation` / `ss_currentScanDesc` in its embedded
//! `ScanState`. The shared [`ScanStateData`] is trimmed and does not carry a
//! table-AM scan descriptor (that would force a `types-nodes -> types-tableam`
//! cycle), so this crate keeps the faithful node shape by holding those two
//! fields on the node-state struct directly.

#![allow(non_snake_case)]

use mcx::PgVec;
use types_rel::Relation;
use types_tableam::relscan::TableScanDesc;
use types_tuple::heaptuple::ItemPointerData;

pub use types_nodes::execnodes::ScanStateData;

/// `TidExprType` (nodeTidrangescan.c) — type of TID-yielding op: lower or upper
/// range bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TidExprType {
    /// `TIDEXPR_UPPER_BOUND`.
    UpperBound,
    /// `TIDEXPR_LOWER_BOUND`.
    LowerBound,
}

/// Opaque handle to a compiled `ExprState`, owned by the executor's expression
/// subsystem (behind the seam). The node identifies a bound's compiled
/// expression by this handle, which the `exec_eval_expr_switch_context` seam
/// resolves back to the live `ExprState` at evaluation time. Modelled as a
/// generation index so this crate never holds a raw pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ExprStateHandle(pub u64);

/// `TidOpExpr` (nodeTidrangescan.c) — an upper or lower range bound for the
/// scan. In C this is a `palloc`'d node referenced from `trss_tidexprs`.
#[derive(Clone, Copy, Debug)]
pub struct TidOpExpr {
    /// `TidExprType exprtype` — type of op; lower or upper.
    pub exprtype: TidExprType,
    /// `ExprState *exprstate` — compiled `ExprState` for a TID-yielding
    /// subexpr, addressed by the opaque [`ExprStateHandle`].
    pub exprstate: ExprStateHandle,
    /// `bool inclusive` — whether op is inclusive.
    pub inclusive: bool,
}

/// Which operand of a binary `OpExpr` a seam should address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperandSide {
    /// `get_leftop(expr)`.
    Left,
    /// `get_rightop(expr)`.
    Right,
}

/// `TidRangeScanState` (execnodes.h) — per-node execution state. The leading
/// `ss` field carries the embedded `ScanState`/`PlanState` heads; the remaining
/// fields are the TID-range working state.
///
/// Not `Debug`: the embedded `TableScanDesc` (the AM-private scan descriptor)
/// is not `Debug`, mirroring its C `TableScanDescData *` opacity.
pub struct TidRangeScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `Relation ss.ss_currentRelation` — the relation being scanned (held on
    /// the node-state struct; see the module note on why it is not on the
    /// shared `ScanStateData`).
    pub ss_currentRelation: Option<Relation<'mcx>>,
    /// `TableScanDesc ss.ss_currentScanDesc` — the active table-AM scan
    /// descriptor, `None` until `table_beginscan_tidrange`.
    pub ss_currentScanDesc: Option<TableScanDesc<'mcx>>,
    /// `List *trss_tidexprs` — compiled TID-yielding bound expressions.
    pub trss_tidexprs: PgVec<'mcx, TidOpExpr>,
    /// `ItemPointerData trss_mintid` — lower bound of the TID range.
    pub trss_mintid: ItemPointerData,
    /// `ItemPointerData trss_maxtid` — upper bound of the TID range.
    pub trss_maxtid: ItemPointerData,
    /// `bool trss_inScan` — are we in a scan?
    pub trss_inScan: bool,
}
