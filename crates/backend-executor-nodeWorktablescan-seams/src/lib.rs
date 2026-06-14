//! Seam declarations for `backend-executor-nodeWorktablescan`.
//!
//! Each slot stands in for one operation in a subsystem below the executor node
//! layer that `nodeWorktablescan.c` cannot implement on its own: the
//! execUtils/execScan init helpers, the `execScan.c` leaf operations /
//! EvalPlanQual machinery, the ancestor `RecursiveUnion`'s work-table tuplestore
//! (`tuplestore_gettupleslot` / `tuplestore_rescan` — `tuplestore.c`), and the
//! resolution of that ancestor's executor state from the work-table `Param`
//! slot. Every seam defaults to a loud panic until the owning subsystem installs
//! a real implementation.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_error::PgResult;
use types_nodes::execnodes::EStateData;
use types_nodes::nodeworktablescan::{WorkTableScan, WorkTableScanStateData};

// --- node factory / plan-state links ---

seam_core::seam!(
    /// Wire `scanstate->ss.ps.plan = (Plan *) node`,
    /// `scanstate->ss.ps.state = estate`, install
    /// `scanstate->ss.ps.ExecProcNode = ExecWorkTableScan`, and leave
    /// `scanstate->rustate = NULL`.
    pub fn init_plan_state_links<'mcx>(
        scanstate: &mut WorkTableScanStateData<'mcx>,
        node: &WorkTableScan<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- execUtils / execScan / execTuples init helpers ---

seam_core::seam!(
    /// `ExecAssignExprContext(estate, &scanstate->ss.ps)`.
    pub fn exec_assign_expr_context<'mcx>(
        scanstate: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitResultTypeTL(&scanstate->ss.ps)`.
    pub fn exec_init_result_type_tl<'mcx>(
        scanstate: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitScanTupleSlot(estate, &scanstate->ss, NULL, &TTSOpsMinimalTuple)`.
    pub fn exec_init_scan_tuple_slot<'mcx>(
        scanstate: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `scanstate->ss.ps.qual =
    /// ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate)`.
    pub fn exec_init_qual<'mcx>(
        scanstate: &mut WorkTableScanStateData<'mcx>,
        node: &WorkTableScan<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- first-call deferred setup (depends on the resolved RecursiveUnion) ---

seam_core::seam!(
    /// Resolve `node->rustate` from the work-table `Param` slot:
    /// `param = &estate->es_param_exec_vals[plan->wtParam];
    ///  node->rustate = castNode(RecursiveUnionState, DatumGetPointer(param->value))`.
    /// Asserts `param->execPlan == NULL` and `!param->isnull`.
    pub fn resolve_rustate<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignScanType(&node->ss, ExecGetResultType(&node->rustate->ps))` —
    /// the scan tuple type equals the ancestor `RecursiveUnion`'s result rowtype.
    pub fn exec_assign_scan_type_from_rustate<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignScanProjectionInfo(&node->ss)`.
    pub fn exec_assign_scan_projection_info<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- access method: ancestor RecursiveUnion's work-table tuplestore ---

seam_core::seam!(
    /// `(void) tuplestore_gettupleslot(node->rustate->working_table, true, false,
    /// node->ss.ss_ScanTupleSlot)` — fetch the next work-table tuple into the
    /// node's scan slot (forward, no copy). `Ok(true)` if a tuple is available,
    /// `Ok(false)` once the work table is exhausted (slot left empty).
    pub fn tuplestore_gettupleslot<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_rescan(node->rustate->working_table)`.
    pub fn tuplestore_rescan<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- rescan (ExecReScanWorkTableScan) ---

seam_core::seam!(
    /// `ExecClearTuple(node->ss.ps.ps_ResultTupleSlot)`.
    pub fn exec_clear_result_tuple_slot<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)`.
    pub fn exec_scan_rescan<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- execScan.c driver leaf operations (linked into the node TU) ---

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()`.
    pub fn check_for_interrupts() -> PgResult<()>
);

seam_core::seam!(
    /// `ResetExprContext(node->ss.ps.ps_ExprContext)`.
    pub fn reset_per_tuple_expr_context<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `econtext->ecxt_scantuple = node->ss.ss_ScanTupleSlot`.
    pub fn set_econtext_scantuple_to_scan_slot<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(node->ss.ss_ScanTupleSlot)`.
    pub fn exec_clear_scan_tuple<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(projInfo->pi_state.resultslot)`.
    pub fn exec_clear_proj_result_slot<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecQual(node->ss.ps.qual, node->ss.ps.ps_ExprContext)`.
    pub fn exec_qual<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecProject(node->ss.ps.ps_ProjInfo)`; `Ok(true)` since a tuple is
    /// always produced.
    pub fn exec_project<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

// --- EvalPlanQual machinery (execScan.c) ---

seam_core::seam!(
    /// `((Scan *) node->ps.plan)->scanrelid`.
    pub fn scan_scanrelid<'mcx>(node: &WorkTableScanStateData<'mcx>) -> PgResult<u32>
);

seam_core::seam!(
    /// `node->ps.state->es_epq_active != NULL`.
    pub fn es_epq_active_present<'mcx>(node: &WorkTableScanStateData<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `bms_is_member(epqstate->epqParam, node->ps.plan->extParam)`.
    pub fn epq_param_is_member_of_ext_param<'mcx>(
        node: &WorkTableScanStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `epqstate->relsubs_done[index]`.
    pub fn epq_relsubs_done<'mcx>(
        node: &WorkTableScanStateData<'mcx>,
        index: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `epqstate->relsubs_done[index] = value`.
    pub fn epq_set_relsubs_done<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        index: u32,
        value: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `epqstate->relsubs_slot[index] != NULL`.
    pub fn epq_relsubs_slot_present<'mcx>(
        node: &WorkTableScanStateData<'mcx>,
        index: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// Copy `epqstate->relsubs_slot[index]` into the node's scan slot.
    pub fn epq_load_relsubs_slot<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        index: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `epqstate->relsubs_rowmark[index] != NULL`.
    pub fn epq_relsubs_rowmark_present<'mcx>(
        node: &WorkTableScanStateData<'mcx>,
        index: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `EvalPlanQualFetchRowMark(epqstate, scanrelid, node->ss.ss_ScanTupleSlot)`.
    pub fn eval_plan_qual_fetch_row_mark<'mcx>(
        node: &mut WorkTableScanStateData<'mcx>,
        scanrelid: u32,
    ) -> PgResult<bool>
);
