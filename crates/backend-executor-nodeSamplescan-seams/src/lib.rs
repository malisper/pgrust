//! Seam declarations for `backend-executor-nodeSamplescan`.
//!
//! Each slot stands in for one operation in a subsystem below the executor node
//! layer that `nodeSamplescan.c` cannot implement on its own: the table access
//! methods (`table_beginscan_sampling`, `table_scan_sample_next_block`, …), the
//! expression-compilation/evaluation layer (`ExecInitQual`, `ExecInitExprList`,
//! `ExecEvalExprSwitchContext`, `ExecQual`, `ExecProject`), the
//! execUtils/execScan init helpers (`ExecAssignExprContext`,
//! `ExecOpenScanRelation`, …), the tablesample-method registry and its
//! callbacks (`GetTsmRoutine`, `InitSampleScan`, `BeginSampleScan`,
//! `EndSampleScan`, the `NextSampleBlock == NULL` test), the PRNG / hash helpers
//! (`pg_prng_uint32`, `hashfloat8`), and the `execScan.c` leaf operations /
//! EvalPlanQual machinery. Every seam defaults to a loud panic until the owning
//! subsystem installs a real implementation.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_core::primitive::uint32;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_nodes::execnodes::EStateData;
use types_samplescan::{SampleScan, SampleScanState};

// --- node factory / makeNode / plan-state links ---

seam_core::seam!(
    /// Wire `scanstate->ss.ps.plan = (Plan *) node`,
    /// `scanstate->ss.ps.state = estate`, and install
    /// `scanstate->ss.ps.ExecProcNode = ExecSampleScan`.
    pub fn init_plan_state_links<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        node: &SampleScan<'mcx>,
    ) -> PgResult<()>
);

// --- execUtils / execScan init helpers ---

seam_core::seam!(
    /// `ExecAssignExprContext(estate, &scanstate->ss.ps)`.
    pub fn exec_assign_expr_context<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `scanstate->ss.ss_currentRelation =
    /// ExecOpenScanRelation(estate, node->scan.scanrelid, eflags)`.
    pub fn exec_open_scan_relation<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        node: &SampleScan<'mcx>,
        eflags: i32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitScanTupleSlot(estate, &scanstate->ss, RelationGetDescr(rel),
    /// table_slot_callbacks(rel))`.
    pub fn exec_init_scan_tuple_slot<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecInitResultTypeTL(&scanstate->ss.ps)`.
    pub fn exec_init_result_type_tl<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecAssignScanProjectionInfo(&scanstate->ss)`.
    pub fn exec_assign_scan_projection_info<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecScanReScan(&node->ss)` — the C `ExecReScanSampleScan(SampleScanState
    /// *)` takes only the node; the owner resolves the EState from
    /// `node->ss.ps.state`.
    pub fn exec_scan_rescan<'mcx>(node: &mut SampleScanState<'mcx>) -> PgResult<()>
);

// --- expression compilation ---

seam_core::seam!(
    /// `scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, scanstate)`.
    pub fn exec_init_qual<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        node: &SampleScan<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `scanstate->args = ExecInitExprList(tsc->args, scanstate)` — compile the
    /// TABLESAMPLE argument expressions into the node's `args` ExprState list.
    pub fn exec_init_expr_list<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        node: &SampleScan<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `scanstate->repeatable = ExecInitExpr(tsc->repeatable, scanstate)` —
    /// compile the REPEATABLE expression (no-op when there is no clause).
    pub fn exec_init_repeatable_expr<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        node: &SampleScan<'mcx>,
    ) -> PgResult<()>
);

// --- per-tuple expression evaluation ---

seam_core::seam!(
    /// `params[i] = ExecEvalExprSwitchContext((ExprState *) lfirst(arg),
    /// econtext, &isnull)` — evaluate the `i`-th TABLESAMPLE argument in the
    /// node's per-tuple `ExprContext`, returning the resulting `Datum` and
    /// setting `is_null`.
    pub fn exec_eval_arg_in_per_tuple_context<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        i: usize,
        is_null: &mut bool,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `datum = ExecEvalExprSwitchContext(scanstate->repeatable, econtext,
    /// &isnull)` — evaluate the REPEATABLE expression in the node's per-tuple
    /// `ExprContext`.
    pub fn exec_eval_repeatable_in_per_tuple_context<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        is_null: &mut bool,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Datum>
);

// --- PRNG / hash helpers ---

seam_core::seam!(
    /// `pg_prng_uint32(&pg_global_prng_state)` — pick a random scan seed.
    pub fn pg_prng_uint32_global() -> PgResult<uint32>
);

seam_core::seam!(
    /// `DatumGetUInt32(DirectFunctionCall1(hashfloat8, datum))` — convert the
    /// REPEATABLE float8 value into a scan seed.
    pub fn hashfloat8(datum: Datum) -> PgResult<uint32>
);

// The tablesample-method registry/callback seams (access/tsmapi.h:
// `get_tsm_routine_oid`, `tsm_has_init_sample_scan`, `tsm_init_sample_scan`,
// `tsm_begin_sample_scan`, `tsm_has_next_sample_block`,
// `tsm_has_end_sample_scan`, `tsm_end_sample_scan`) were moved to
// `backend-access-tablesample-core-seams`, whose stem matches their true owner
// `backend-access-tablesample-core`.

// --- table access methods ---

seam_core::seam!(
    /// `scanstate->ss.ss_currentScanDesc = table_beginscan_sampling(rel,
    /// es_snapshot, 0, NULL, use_bulkread, allow_sync, use_pagemode)`.
    pub fn table_beginscan_sampling<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        allow_sync: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `table_rescan_set_params(scan, NULL, use_bulkread, allow_sync,
    /// use_pagemode)`.
    pub fn table_rescan_set_params<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        allow_sync: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `table_endscan(node->ss.ss_currentScanDesc)`.
    pub fn table_endscan<'mcx>(node: &mut SampleScanState<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `table_scan_sample_next_block(scan, scanstate)` — `Ok(true)` when a block
    /// is available for sampling.
    pub fn table_scan_sample_next_block<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `table_scan_sample_next_tuple(scan, scanstate, slot)` — store the next
    /// visible tuple in the node's scan slot; `Ok(true)` when one is available.
    pub fn table_scan_sample_next_tuple<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

// --- per-tuple ExprContext / scan-slot plumbing (execScan.c) ---

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()`.
    pub fn check_for_interrupts() -> PgResult<()>
);

seam_core::seam!(
    /// `ResetExprContext(node->ss.ps.ps_ExprContext)`.
    pub fn reset_per_tuple_expr_context<'mcx>(
        node: &mut SampleScanState<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `econtext->ecxt_scantuple = node->ss.ss_ScanTupleSlot`.
    pub fn set_econtext_scantuple_to_scan_slot<'mcx>(
        node: &mut SampleScanState<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(node->ss.ss_ScanTupleSlot)`.
    pub fn exec_clear_scan_tuple<'mcx>(
        node: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(projInfo->pi_state.resultslot)`.
    pub fn exec_clear_proj_result_slot<'mcx>(
        node: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecQual(node->ss.ps.qual, node->ss.ps.ps_ExprContext)`.
    pub fn exec_qual<'mcx>(
        node: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecProject(node->ss.ps.ps_ProjInfo)`; `Ok(true)` since a tuple is
    /// always produced.
    pub fn exec_project<'mcx>(
        node: &mut SampleScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

// --- EvalPlanQual machinery (execScan.c) ---

seam_core::seam!(
    /// `((Scan *) node->ps.plan)->scanrelid`.
    pub fn scan_scanrelid<'mcx>(node: &SampleScanState<'mcx>) -> PgResult<u32>
);

seam_core::seam!(
    /// `node->ps.state->es_epq_active != NULL`.
    pub fn es_epq_active_present<'mcx>(node: &SampleScanState<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `bms_is_member(epqstate->epqParam, node->ps.plan->extParam)`.
    pub fn epq_param_is_member_of_ext_param<'mcx>(
        node: &SampleScanState<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `epqstate->relsubs_done[index]`.
    pub fn epq_relsubs_done<'mcx>(node: &SampleScanState<'mcx>, index: u32) -> PgResult<bool>
);

seam_core::seam!(
    /// `epqstate->relsubs_done[index] = value`.
    pub fn epq_set_relsubs_done<'mcx>(
        node: &mut SampleScanState<'mcx>,
        index: u32,
        value: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `epqstate->relsubs_slot[index] != NULL`.
    pub fn epq_relsubs_slot_present<'mcx>(
        node: &SampleScanState<'mcx>,
        index: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// Copy `epqstate->relsubs_slot[index]` into the node's scan slot.
    pub fn epq_load_relsubs_slot<'mcx>(
        node: &mut SampleScanState<'mcx>,
        index: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `epqstate->relsubs_rowmark[index] != NULL`.
    pub fn epq_relsubs_rowmark_present<'mcx>(
        node: &SampleScanState<'mcx>,
        index: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `EvalPlanQualFetchRowMark(epqstate, scanrelid, node->ss.ss_ScanTupleSlot)`.
    pub fn eval_plan_qual_fetch_row_mark<'mcx>(
        node: &mut SampleScanState<'mcx>,
        scanrelid: u32,
    ) -> PgResult<bool>
);
