//! Seam declarations for `backend-executor-nodeTidrangescan`.
//!
//! Each slot stands in for one operation in a subsystem below the executor node
//! layer that `nodeTidrangescan.c` cannot implement on its own (expression
//! compilation/evaluation, the execUtils/execScan init helpers, the table
//! access methods, and the `execScan.c` leaf operations / EvalPlanQual
//! machinery). Every seam defaults to a loud panic until the owning subsystem
//! installs a real implementation.
//!
//! The planner-primnode field reads (`IsCTIDVar`, `get_leftop`/`get_rightop`,
//! `OpExpr.opno`, `IsA(node, OpExpr)`) are NOT seams: they are pure data reads
//! on the owned `TidRangeScan` plan node, implemented in-crate by the node.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use mcx::PgBox;
use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::{EStateData, EcxtId};
use types_tidrange::{OperandSide, TidRangeScanState};
use types_nodes::nodetidrangescan::TidRangeScan;
use types_tuple::heaptuple::ItemPointerData;

// --- node factory / makeNode / plan-state links ---

seam_core::seam!(
    /// Wire `tidrangestate->ss.ps.plan = (Plan *) node`,
    /// `tidrangestate->ss.ps.state = estate`, and install
    /// `tidrangestate->ss.ps.ExecProcNode = ExecTidRangeScan`.
    pub fn init_plan_state_links<'mcx>(
        tidrangestate: &mut TidRangeScanState<'mcx>,
        node: &TidRangeScan<'mcx>,
    ) -> PgResult<()>
);

// --- expression evaluation ---

seam_core::seam!(
    /// `ExecInitExpr((Expr *) get_leftop/get_rightop(expr), &tidstate->ss.ps)` —
    /// compile the `side` operand of the `qual_index`-th qual `OpExpr` into an
    /// `ExprState`, returning the executor-owned compiled expression.
    pub fn exec_init_expr<'mcx>(
        tidstate: &mut TidRangeScanState<'mcx>,
        node: &TidRangeScan<'mcx>,
        qual_index: usize,
        side: OperandSide,
    ) -> PgResult<PgBox<'mcx, ExprState>>
);

seam_core::seam!(
    /// `(ItemPointer) DatumGetPointer(ExecEvalExprSwitchContext(exprstate,
    /// econtext, &isNull))` — evaluate the bound expression `exprstate` in the
    /// node's per-tuple `ExprContext` (`econtext`), returning the resulting TID.
    /// Sets `is_null` when the bound is SQL NULL.
    pub fn exec_eval_expr_switch_context<'mcx>(
        exprstate: &ExprState,
        econtext: EcxtId,
        is_null: &mut bool,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<ItemPointerData>
);

seam_core::seam!(
    /// `tidrangestate->ss.ps.qual =
    /// ExecInitQual(node->scan.plan.qual, tidrangestate)`.
    pub fn exec_init_qual<'mcx>(
        tidrangestate: &mut TidRangeScanState<'mcx>,
        node: &TidRangeScan<'mcx>,
    ) -> PgResult<()>
);

// --- execUtils / execScan init helpers ---

seam_core::seam!(
    /// `ExecAssignExprContext(estate, &tidrangestate->ss.ps)`.
    pub fn exec_assign_expr_context<'mcx>(
        tidrangestate: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tidrangestate->ss.ss_currentRelation =
    /// ExecOpenScanRelation(estate, node->scan.scanrelid, eflags)`.
    pub fn exec_open_scan_relation<'mcx>(
        tidrangestate: &mut TidRangeScanState<'mcx>,
        node: &TidRangeScan<'mcx>,
        eflags: i32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitScanTupleSlot(estate, &tidrangestate->ss, RelationGetDescr(rel),
    /// table_slot_callbacks(rel))`.
    pub fn exec_init_scan_tuple_slot<'mcx>(
        tidrangestate: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitResultTypeTL(&tidrangestate->ss.ps)`.
    pub fn exec_init_result_type_tl<'mcx>(
        tidrangestate: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignScanProjectionInfo(&tidrangestate->ss)`.
    pub fn exec_assign_scan_projection_info<'mcx>(
        tidrangestate: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)`.
    pub fn exec_scan_rescan<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- per-tuple ExprContext / scan-slot plumbing ---

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()`.
    pub fn check_for_interrupts() -> PgResult<()>
);

seam_core::seam!(
    /// `ResetExprContext(node->ss.ps.ps_ExprContext)`.
    pub fn reset_per_tuple_expr_context<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `econtext->ecxt_scantuple = node->ss.ss_ScanTupleSlot`.
    pub fn set_econtext_scantuple_to_scan_slot<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(node->ss.ss_ScanTupleSlot)`.
    pub fn exec_clear_scan_tuple<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(projInfo->pi_state.resultslot)`.
    pub fn exec_clear_proj_result_slot<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecQual(node->ss.ps.qual, node->ss.ps.ps_ExprContext)`.
    pub fn exec_qual<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecProject(node->ss.ps.ps_ProjInfo)`; `Ok(true)` since a tuple is
    /// always produced.
    pub fn exec_project<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

// --- table access methods ---

seam_core::seam!(
    /// `node->ss.ss_currentScanDesc =
    /// table_beginscan_tidrange(node->ss.ss_currentRelation, estate->es_snapshot,
    /// &node->trss_mintid, &node->trss_maxtid)`.
    pub fn table_beginscan_tidrange<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `table_rescan_tidrange(scandesc, &node->trss_mintid, &node->trss_maxtid)`.
    pub fn table_rescan_tidrange<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `table_scan_getnextslot_tidrange(scandesc, estate->es_direction,
    /// node->ss.ss_ScanTupleSlot)` — fetch the next tuple into the node's scan
    /// slot; `Ok(true)` if a tuple is available.
    pub fn table_scan_getnextslot_tidrange<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_endscan(node->ss.ss_currentScanDesc)`.
    pub fn table_endscan<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- EvalPlanQual machinery (execScan.c) ---

seam_core::seam!(
    /// `((Scan *) node->ps.plan)->scanrelid`.
    pub fn scan_scanrelid<'mcx>(node: &TidRangeScanState<'mcx>) -> PgResult<u32>
);

seam_core::seam!(
    /// `node->ps.state->es_epq_active != NULL`.
    pub fn es_epq_active_present<'mcx>(node: &TidRangeScanState<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `bms_is_member(epqstate->epqParam, node->ps.plan->extParam)`.
    pub fn epq_param_is_member_of_ext_param<'mcx>(
        node: &TidRangeScanState<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `epqstate->relsubs_done[index]`.
    pub fn epq_relsubs_done<'mcx>(node: &TidRangeScanState<'mcx>, index: u32) -> PgResult<bool>
);

seam_core::seam!(
    /// `epqstate->relsubs_done[index] = value`.
    pub fn epq_set_relsubs_done<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        index: u32,
        value: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `epqstate->relsubs_slot[index] != NULL`.
    pub fn epq_relsubs_slot_present<'mcx>(
        node: &TidRangeScanState<'mcx>,
        index: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// Copy `epqstate->relsubs_slot[index]` into the node's scan slot.
    pub fn epq_load_relsubs_slot<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        index: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `epqstate->relsubs_rowmark[index] != NULL`.
    pub fn epq_relsubs_rowmark_present<'mcx>(
        node: &TidRangeScanState<'mcx>,
        index: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `EvalPlanQualFetchRowMark(epqstate, scanrelid, node->ss.ss_ScanTupleSlot)`.
    pub fn eval_plan_qual_fetch_row_mark<'mcx>(
        node: &mut TidRangeScanState<'mcx>,
        scanrelid: u32,
    ) -> PgResult<bool>
);
