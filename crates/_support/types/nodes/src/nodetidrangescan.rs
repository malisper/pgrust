//! `TidRangeScan` plan-node vocabulary (nodes/plannodes.h), trimmed.

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_error::PgResult;
use rel::Relation;
use types_tableam::relscan::TableScanDesc;
use types_tuple::heaptuple::ItemPointerData;

use crate::execexpr::ExprState;
use crate::execnodes::ScanStateData;
use crate::nodeindexscan::Scan;
use crate::primnodes::Expr;

/// `TidRangeScan` (nodes/plannodes.h) — TID range scan node. The `tidrangequals`
/// list holds the qual(s) involving `CTID op something`.
#[derive(Debug, Default)]
pub struct TidRangeScan<'mcx> {
    /// `Scan scan` — the abstract scan base (`scan.scanrelid` is the RT index;
    /// `scan.plan.qual` is the residual qual).
    pub scan: Scan<'mcx>,
    /// `List *tidrangequals` — qual(s) involving CTID op something. `None` is
    /// the C `NIL`.
    pub tidrangequals: Option<PgVec<'mcx, Expr<'mcx>>>,
}

impl TidRangeScan<'_> {
    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TidRangeScan<'b>> {
        let tidrangequals = match &self.tidrangequals {
            Some(quals) => {
                let mut out = vec_with_capacity_in(mcx, quals.len())?;
                for q in quals.iter() {
                    // Deep-copy via `clone_in`, not the derived `Expr::clone`
                    // (which panics on a `SubPlan` arm).
                    out.push(q.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(TidRangeScan {
            scan: self.scan.clone_in(mcx)?,
            tidrangequals,
        })
    }
}

/// `TidExprType` (nodeTidrangescan.c) — type of TID-yielding op: lower or upper
/// range bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TidExprType {
    /// `TIDEXPR_UPPER_BOUND`.
    UpperBound,
    /// `TIDEXPR_LOWER_BOUND`.
    LowerBound,
}

/// `TidOpExpr` (nodeTidrangescan.c) — an upper or lower range bound for the
/// scan. In C this is a `palloc`'d node referenced from `trss_tidexprs`.
pub struct TidOpExpr<'mcx> {
    /// `TidExprType exprtype` — type of op; lower or upper.
    pub exprtype: TidExprType,
    /// `ExprState *exprstate` — compiled `ExprState` for a TID-yielding
    /// subexpr (the real `ExprState` the expression subsystem owns).
    pub exprstate: Option<PgBox<'mcx, ExprState<'mcx>>>,
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
/// The C node keeps `ss_currentRelation` / `ss_currentScanDesc` in its embedded
/// `ScanState`. The shared [`ScanStateData`] is trimmed and does not carry a
/// table-AM scan descriptor, so this struct keeps the faithful node shape by
/// holding those two fields directly.
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
    pub trss_tidexprs: PgVec<'mcx, TidOpExpr<'mcx>>,
    /// `ItemPointerData trss_mintid` — lower bound of the TID range.
    pub trss_mintid: ItemPointerData,
    /// `ItemPointerData trss_maxtid` — upper bound of the TID range.
    pub trss_maxtid: ItemPointerData,
    /// `bool trss_inScan` — are we in a scan?
    pub trss_inScan: bool,
}

// Manual `Debug` (not `derive`): `ss_currentScanDesc`'s `TableScanDescData`
// carries the AM's opaque `dyn Any` tail (not `Debug`), and `TidOpExpr` holds an
// `ExprState` that is likewise not `Debug`. The central `PlanStateNode` enum
// derives `Debug`, so this variant payload must be `Debug`; the scan descriptor
// and relation are printed as opaque present/absent markers and the compiled
// bound expressions as a count.
impl core::fmt::Debug for TidRangeScanState<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TidRangeScanState")
            .field("ss", &self.ss)
            .field(
                "ss_currentRelation",
                &self.ss_currentRelation.as_ref().map(|_| "<Relation>"),
            )
            .field(
                "ss_currentScanDesc",
                &self.ss_currentScanDesc.as_ref().map(|_| "<TableScanDesc>"),
            )
            .field("trss_tidexprs", &self.trss_tidexprs.len())
            .field("trss_mintid", &self.trss_mintid)
            .field("trss_maxtid", &self.trss_maxtid)
            .field("trss_inScan", &self.trss_inScan)
            .finish()
    }
}
