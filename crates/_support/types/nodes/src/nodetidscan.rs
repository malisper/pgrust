//! TidScan node vocabulary (`executor/execnodes.h` `TidScanState`,
//! `nodes/plannodes.h` `TidScan` — the latter lives in [`crate::nodeindexscan`]).
//!
//! `TidScanState` is the executor state for a direct TID scan of a relation. It
//! relocates here (out of the `backend-executor-nodeTidscan` crate) because the
//! central [`PlanStateNode`](crate::planstate::PlanStateNode) dispatch enum —
//! which lives in `types-nodes` — must name `TidScanState` as a variant. The one
//! field whose type sits above the shared executor-node knot, the
//! `TableScanDesc` `ss_currentScanDesc`, is nameable here now that `types-nodes`
//! depends on `types-tableam` (the slot-vocab F0 "Edge A"); every other field is
//! shared vocabulary. The executor logic (`ExecInitTidScan` / `TidNext` / ...)
//! stays in the node crate and imports these structs from here.
//!
//! This is the same relocation pattern as the landed `types-relcache-entry` /
//! `BitmapOr` keystones (real types move down to break a cycle —
//! opacity-inherited-never-introduced; no handles, no stand-ins).

use mcx::{Mcx, PgBox, PgVec};
use ::types_core::primitive::Index;
use ::types_tableam::relscan::TableScanDesc;
use ::types_tuple::heaptuple::ItemPointerData;

use crate::execexpr::ExprState;
use crate::execnodes::ScanStateData;
use crate::primnodes::CurrentOfExpr;

pub use crate::nodes::T_TidScanState;

/// `TidExpr` (nodeTidscan.c) — a compiled TID-yielding subexpression, or a
/// `WHERE CURRENT OF`:
///
/// ```c
/// typedef struct TidExpr {
///     ExprState  *exprstate;   /* ExprState for a TID-yielding subexpr */
///     bool        isarray;     /* if true, it yields tid[] not just tid */
///     CurrentOfExpr *cexpr;    /* alternatively, we can have CURRENT OF */
/// } TidExpr;
/// ```
#[derive(Debug)]
pub struct TidExpr<'mcx> {
    /// `ExprState *exprstate` — compiled TID-yielding subexpr, or `None`.
    pub exprstate: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `bool isarray` — if true, it yields `tid[]` not just `tid`.
    pub isarray: bool,
    /// `CurrentOfExpr *cexpr` — alternatively, we can have CURRENT OF.
    pub cexpr: Option<CurrentOfExpr>,
}

impl Default for TidExpr<'_> {
    fn default() -> Self {
        TidExpr {
            exprstate: None,
            isarray: false,
            cexpr: None,
        }
    }
}

/// `TidScanState` (execnodes.h) — the TID-scan executor node state, in the
/// owned shape the node crate works with. The leading `ss` field carries the
/// embedded `ScanState`/`PlanState`/`Scan` heads (shared [`ScanStateData`]);
/// the remaining fields are the TID-scan working state.
///
/// `ss_currentScanDesc` (the C `ScanState.ss_currentScanDesc`) lives here, not
/// in the shared `ScanStateData`, because its type [`TableScanDesc`] sits above
/// the shared executor-node knot (docs/types.md rule 3 — the type lives where it
/// is nameable; `types-nodes` now depends on `types-tableam`).
pub struct TidScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `TableScanDesc ss_currentScanDesc` — the on-demand TID scan descriptor;
    /// `None` is the C `NULL` (no scan started yet).
    pub ss_currentScanDesc: Option<TableScanDesc<'mcx>>,
    /// `List *tss_tidexprs` — compiled TID-yielding subexpressions.
    pub tss_tidexprs: PgVec<'mcx, TidExpr<'mcx>>,
    /// `bool tss_isCurrentOf` — true if this is a `WHERE CURRENT OF` scan.
    pub tss_isCurrentOf: bool,
    /// `int tss_NumTids` — number of valid TIDs in `tss_TidList`.
    pub tss_NumTids: i32,
    /// `int tss_TidPtr` — index of the current TID, or `-1` before the scan.
    pub tss_TidPtr: i32,
    /// `ItemPointerData *tss_TidList` — sorted, de-duplicated TID array.
    /// `None` is the C `NULL` (TID list not computed yet).
    pub tss_TidList: Option<PgVec<'mcx, ItemPointerData>>,
    /// `((Scan *) node->ss.ps.plan)->scanrelid` — the range-table index of the
    /// scanned base relation. In C this is read off the `Scan` plan node via
    /// the `PlanState.plan` back-link; the trimmed [`ScanStateData`]/
    /// `PlanStateData` does not retain that borrow, so `ExecInitTidScan`
    /// captures the plan's `scanrelid` here for the EvalPlanQual path
    /// (`ExecScanFetch`) to recover. A `TidScan` always scans a base relation,
    /// so this is always a real positive RTE index (never the `0`
    /// ForeignScan/CustomScan pushed-down-join sentinel).
    pub scanrelid: Index,
}

// Manual `Debug` (not `derive`): `ss_currentScanDesc`'s
// [`TableScanDescData`](::types_tableam::relscan::TableScanDescData) carries the
// AM's opaque `dyn Any` tail, which is not `Debug`. The central
// `PlanStateNode` enum derives `Debug`, so this variant payload must be
// `Debug`; the scan descriptor is printed as an opaque present/absent marker.
impl core::fmt::Debug for TidScanState<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TidScanState")
            .field("ss", &self.ss)
            .field(
                "ss_currentScanDesc",
                &self.ss_currentScanDesc.as_ref().map(|_| "<TableScanDesc>"),
            )
            .field("tss_tidexprs", &self.tss_tidexprs)
            .field("tss_isCurrentOf", &self.tss_isCurrentOf)
            .field("tss_NumTids", &self.tss_NumTids)
            .field("tss_TidPtr", &self.tss_TidPtr)
            .field("tss_TidList", &self.tss_TidList)
            .field("scanrelid", &self.scanrelid)
            .finish()
    }
}

impl<'mcx> TidScanState<'mcx> {
    /// `makeNode(TidScanState)` — a freshly zeroed node, allocating its
    /// collections in `mcx` (the EState per-query context).
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        TidScanState {
            ss: ScanStateData::default(),
            ss_currentScanDesc: None,
            tss_tidexprs: PgVec::new_in(mcx),
            tss_isCurrentOf: false,
            tss_NumTids: 0,
            tss_TidPtr: -1,
            tss_TidList: None,
            scanrelid: 0,
        }
    }
}
