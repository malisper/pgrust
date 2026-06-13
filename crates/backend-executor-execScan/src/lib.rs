//! Port of `src/backend/executor/execScan.c` plus the `pg_attribute_always_inline`
//! driver helpers in `src/include/executor/execScan.h` — generalized
//! relation-scan support for the executor.
//!
//! `ExecScan` wraps a per-node "access method" (return the next candidate tuple)
//! and a "recheck method" (validate a tuple under an EvalPlanQual recheck) with
//! qualification evaluation, projection, and the EPQ replacement-tuple decision
//! tree. The four public C entry points are:
//!
//! - `ExecScan` / the inline `ExecScanExtended` / `ExecScanFetch` driver
//! - `ExecAssignScanProjectionInfo` / `ExecAssignScanProjectionInfoWithVarno`
//! - `ExecScanReScan`
//!
//! ## Owned-tree shape vs. the C ABI
//!
//! In C the driver reads two `extern "C"` method pointers off the leaf scan
//! node and returns an aliasing `TupleTableSlot *`. Here the access/recheck
//! methods are concrete `fn(&mut NodeState, &mut EStateData) -> PgResult<...>`
//! pointers passed in by each scan-node crate (the C `(ExecScanAccessMtd)
//! XxxNext` casts), and the result is the [`SlotId`] of the produced slot (or
//! `None`, the C `NULL`). The active `EPQState` is read from
//! `estate.es_epq_active` (the C `node->ps.state->es_epq_active`), exactly as
//! the sibling scan-node reproductions do.
//!
//! The generic driver core ([`exec_scan_core`] / [`exec_scan_extended`] /
//! [`exec_scan_fetch`]) works over the node's embedded [`ScanStateData`] head
//! (reached through an accessor, since the access/recheck callbacks need the
//! full concrete node). The per-node seam entry points installed by
//! [`init_seams`] marshal each concrete node type to that single core, which is
//! the model the seam crate documents ("when execScan.c lands it installs a
//! single generic implementation and the per-node entry points marshal to it").

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use backend_executor_execExpr_seams as execExpr;
use backend_executor_execMain_seams as execMain;
use backend_executor_execScan_seams as execScan_seams;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_nodes_core_seams as bitmapset;
use backend_tcop_postgres_seams as tcop_postgres;

use types_error::PgResult;
use types_nodes::execnodes::ScanStateData;
use types_nodes::nodes::Node;
use types_nodes::{EStateData, IndexOnlyScanState, SlotId, TableFuncScanState};

/// A scan node whose embedded `ScanState` head ([`ScanStateData`]) the generic
/// `execScan.c` driver mutates. In C the driver receives a `ScanState *` and
/// reaches the concrete node only through the access/recheck method pointers;
/// here the access/recheck callbacks need the full concrete node, so the driver
/// is generic over it and reaches the shared head through this accessor.
trait ScanNode<'mcx> {
    /// `&node->ss` — the node's [`ScanStateData`] head.
    fn ss(&mut self) -> &mut ScanStateData<'mcx>;
}

impl<'mcx> ScanNode<'mcx> for TableFuncScanState<'mcx> {
    #[inline]
    fn ss(&mut self) -> &mut ScanStateData<'mcx> {
        &mut self.ss
    }
}

impl<'mcx> ScanNode<'mcx> for IndexOnlyScanState<'mcx> {
    #[inline]
    fn ss(&mut self) -> &mut ScanStateData<'mcx> {
        &mut self.ss
    }
}

/// Install this crate's implementations into the `execScan` seam slots.
///
/// Every seam declared in `backend-executor-execScan-seams` is set here to the
/// real driver, so the per-node scan crates (nodeSeqscan / nodeForeignscan /
/// nodeTableFuncscan / nodeIndexonlyscan, …) stop panicking once this crate is
/// initialized.
pub fn init_seams() {
    execScan_seams::exec_scan::set(exec_scan_tablefunc);
    execScan_seams::exec_assign_scan_projection_info::set(exec_assign_scan_projection_info);
    execScan_seams::exec_scan_rescan::set(exec_scan_rescan_tablefunc);
    execScan_seams::exec_scan_indexonly::set(exec_scan_indexonly);
    execScan_seams::exec_scan_rescan_ss::set(exec_scan_rescan_ss);
}

// ===========================================================================
// execScan.h inline helpers (TupIsNull / InstrCountFiltered1 / ResetExprContext)
// ===========================================================================

/// `TupIsNull(slot)` (`executor/tuptable.h`) for the slot identified by `id`
/// (id into the EState slot pool): true if the slot is empty.
#[inline]
fn tup_is_null(estate: &EStateData<'_>, id: Option<SlotId>) -> bool {
    match id {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `InstrCountFiltered1(node, delta)` (`executor.h`) — bump
/// `node->ps.instrument->nfiltered1` when instrumentation is enabled.
#[inline]
fn instr_count_filtered1(ss: &mut ScanStateData<'_>, delta: u64) {
    if let Some(instr) = ss.ps.instrument.as_mut() {
        instr.nfiltered1 += delta as f64;
    }
}

/// `ResetExprContext(node->ps.ps_ExprContext)` (`executor.h`) —
/// `MemoryContextReset(econtext->ecxt_per_tuple_memory)`.
#[inline]
fn reset_expr_context(ss: &ScanStateData<'_>, estate: &mut EStateData<'_>) {
    if let Some(ecxt) = ss.ps.ps_ExprContext {
        if let Some(Some(ec)) = estate.es_exprcontexts.get_mut(ecxt.0 as usize) {
            ec.ecxt_per_tuple_memory.reset();
        }
    }
}

// ===========================================================================
// EPQ-state reads off `estate.es_epq_active` (the C `epqstate->...` accesses).
// ===========================================================================

/// `epqstate->epqParam`.
#[inline]
fn epq_param(estate: &EStateData<'_>) -> i32 {
    estate
        .es_epq_active
        .as_deref()
        .map(|e| e.epqParam)
        .expect("epq_param: es_epq_active not set")
}

/// `epqstate->relsubs_done[idx]`.
#[inline]
fn epq_relsubs_done(estate: &EStateData<'_>, idx: u32) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_done.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `epqstate->relsubs_done[idx] = value`.
#[inline]
fn epq_set_relsubs_done(estate: &mut EStateData<'_>, idx: u32, value: bool) {
    if let Some(e) = estate.es_epq_active.as_deref_mut() {
        if let Some(v) = e.relsubs_done.as_mut() {
            if let Some(slot) = v.get_mut(idx as usize) {
                *slot = value;
            }
        }
    }
}

/// `epqstate->relsubs_blocked[idx]`.
#[inline]
fn epq_relsubs_blocked(estate: &EStateData<'_>, idx: u32) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_blocked.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `epqstate->relsubs_slot[idx]` (`Some` = a non-NULL C entry).
#[inline]
fn epq_relsubs_slot(estate: &EStateData<'_>, idx: u32) -> Option<SlotId> {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_slot.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .flatten()
}

/// `epqstate->relsubs_rowmark[idx] != NULL`.
#[inline]
fn epq_relsubs_rowmark_present(estate: &EStateData<'_>, idx: u32) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_rowmark.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `((Scan *) node->ps.plan)->scanrelid` — read the scan plan node's
/// `scanrelid` off the owned plan view that the `ScanStateData` head aliases.
/// All `Scan`-derived plan nodes carry a `Scan` base whose first field is
/// `scanrelid`; the variants without one (e.g. a pushed-down ForeignScan/
/// CustomScan join) report `0`, as the C `scanrelid == 0`.
#[inline]
fn scan_scanrelid(ss: &ScanStateData<'_>) -> u32 {
    match ss.ps.plan {
        Some(Node::SeqScan(s)) => s.scan.scanrelid,
        Some(Node::TidRangeScan(s)) => s.scan.scanrelid,
        Some(Node::IndexOnlyScan(s)) => s.scan.scanrelid,
        Some(Node::TableFuncScan(s)) => s.scan.scanrelid,
        Some(Node::ForeignScan(s)) => s.scan.scanrelid,
        Some(other) => panic!("scan_scanrelid: plan is not a Scan node: {other:?}"),
        None => panic!("scan_scanrelid: ScanStateData has no plan"),
    }
}

// ===========================================================================
// execScan.h: ExecScanFetch / ExecScanExtended / ExecScan (generic core)
// ===========================================================================

/// `ExecScanFetch` (execScan.h) — check interrupts & fetch the next potential
/// tuple, substituting an EvalPlanQual test tuple if inside a recheck. Returns
/// the slot id of the fetched tuple, or `None` (the C `NULL` / a cleared slot).
fn exec_scan_fetch<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    epq_active: bool,
    access_mtd: fn(&mut N, &mut EStateData<'mcx>) -> PgResult<bool>,
    recheck_mtd: fn(&mut N, &mut EStateData<'mcx>) -> PgResult<bool>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    if epq_active {
        // We are inside an EvalPlanQual recheck.
        //   Index scanrelid = ((Scan *) node->ps.plan)->scanrelid;
        let scanrelid = scan_scanrelid(node.ss());

        if scanrelid == 0 {
            // ForeignScan/CustomScan which has pushed down a join to the remote
            // side. If it is a descendant of the EPQ recheck plan tree, run the
            // recheck method; otherwise fall through to the access method below.
            //   if (bms_is_member(epqstate->epqParam, node->ps.plan->extParam))
            let epq_param = epq_param(estate);
            let is_member = {
                let ext_param = node.ss()
                    .ps
                    .plan
                    .map(|p| p.plan_head())
                    .and_then(|ph| ph.extParam.as_deref());
                bitmapset::bms_is_member::call(epq_param, ext_param)
            };
            if is_member {
                // The recheck method is responsible not only for rechecking the
                // scan/join quals but also for storing the correct tuple in the
                // slot.
                //   TupleTableSlot *slot = node->ss_ScanTupleSlot;
                let slot = node.ss()
                    .ss_ScanTupleSlot
                    .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
                //   if (!(*recheckMtd)(node, slot)) ExecClearTuple(slot);
                if !recheck_mtd(node, estate)? {
                    execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
                }
                //   return slot;
                return Ok(Some(slot));
            }
        } else if epq_relsubs_done(estate, scanrelid - 1) {
            // Either there is no EPQ tuple for this rel or we already returned
            // it: return ExecClearTuple(node->ss_ScanTupleSlot).
            let slot = node.ss()
                .ss_ScanTupleSlot
                .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
            execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
            return Ok(Some(slot));
        } else if let Some(epq_slot) = epq_relsubs_slot(estate, scanrelid - 1) {
            // Return replacement tuple provided by the EPQ caller.
            //   TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];
            //   Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);
            debug_assert!(!epq_relsubs_rowmark_present(estate, scanrelid - 1));
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);
            //   if (TupIsNull(slot)) return NULL;
            if estate.slot(epq_slot).is_empty() {
                return Ok(None);
            }
            //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
            // The recheck method reads the PASSED replacement slot (e.g.
            // IndexRecheck sets econtext->ecxt_scantuple = slot); the recheck
            // callbacks of all scan-node consumers do this themselves, so the
            // EPQ replacement slot is the operative one.
            if !recheck_mtd(node, estate)? {
                execTuples::exec_clear_tuple::call(estate.slot_mut(epq_slot))?;
                return Ok(None);
            }
            //   return slot;
            return Ok(Some(epq_slot));
        } else if epq_relsubs_rowmark_present(estate, scanrelid - 1) {
            // Fetch and return replacement tuple using a non-locking rowmark.
            //   TupleTableSlot *slot = node->ss_ScanTupleSlot;
            let slot = node.ss()
                .ss_ScanTupleSlot
                .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);
            //   if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)) return NULL;
            if !execMain::eval_plan_qual_fetch_row_mark::call(estate, scanrelid, slot)? {
                return Ok(None);
            }
            //   if (TupIsNull(slot)) return NULL;
            if estate.slot(slot).is_empty() {
                return Ok(None);
            }
            //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
            if !recheck_mtd(node, estate)? {
                execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
                return Ok(None);
            }
            //   return slot;
            return Ok(Some(slot));
        }
    }

    // Run the node-type-specific access method function to get the next tuple.
    //   return (*accessMtd)(node);
    // The access method stores the next tuple into node->ss_ScanTupleSlot and
    // reports (the C slot/NULL return as) whether a non-empty tuple is there.
    let got = access_mtd(node, estate)?;
    let scan_slot = node.ss().ss_ScanTupleSlot;
    if got && !tup_is_null(estate, scan_slot) {
        Ok(scan_slot)
    } else {
        Ok(None)
    }
}

/// `ExecScanExtended` (execScan.h) — scan using the access method, optionally
/// checking the tuple against `qual` (`has_qual`) and applying projection
/// (`has_proj`). Returns the slot id of the produced (possibly projected)
/// tuple, or `None` (the C `NULL`).
fn exec_scan_extended<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    epq_active: bool,
    access_mtd: fn(&mut N, &mut EStateData<'mcx>) -> PgResult<bool>,
    recheck_mtd: fn(&mut N, &mut EStateData<'mcx>) -> PgResult<bool>,
    has_qual: bool,
    has_proj: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // ExprContext *econtext = node->ps.ps_ExprContext;
    // (interrupt checks are in ExecScanFetch)

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj {
        // ResetExprContext(econtext);
        reset_expr_context(node.ss(), estate);
        return exec_scan_fetch(node, epq_active, access_mtd, recheck_mtd, estate);
    }

    // Reset per-tuple memory context to free any expression-evaluation storage
    // allocated in the previous tuple cycle.
    reset_expr_context(node.ss(), estate);

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        let slot = exec_scan_fetch(node, epq_active, access_mtd, recheck_mtd, estate)?;

        // If the slot returned by the accessMtd contains NULL, there is nothing
        // more to scan, so return an empty slot --- being careful to use the
        // projection result slot so it has the correct tupleDesc.
        let Some(slot) = slot else {
            if has_proj {
                // return ExecClearTuple(projInfo->pi_state.resultslot);
                let result_slot = projection_result_slot(node.ss());
                execTuples::exec_clear_tuple::call(estate.slot_mut(result_slot))?;
                return Ok(Some(result_slot));
            } else {
                return Ok(None);
            }
        };

        // Place the current tuple into the expr context.
        //   econtext->ecxt_scantuple = slot;
        if let Some(ecxt) = node.ss().ps.ps_ExprContext {
            if let Some(Some(ec)) = estate.es_exprcontexts.get_mut(ecxt.0 as usize) {
                ec.ecxt_scantuple = Some(slot);
            }
        }

        // Check that the current tuple satisfies the qual-clause. Check for
        // non-null qual here to avoid a call to ExecQual when the qual is null.
        //   if (qual == NULL || ExecQual(qual, econtext))
        let passes = if !has_qual {
            true
        } else {
            let ss = node.ss();
            match (ss.ps.qual.as_deref(), ss.ps.ps_ExprContext) {
                (Some(state), Some(econtext)) => {
                    execExpr::exec_qual::call(state, econtext, estate)?
                }
                // qual == NULL after all (defensive; has_qual implies Some).
                _ => true,
            }
        };

        if passes {
            // Found a satisfactory scan tuple.
            if has_proj {
                // Form a projection tuple, store it in the result tuple slot,
                // and return it.
                //   return ExecProject(projInfo);
                let produced = execExpr::exec_project::call(&mut node.ss().ps, estate)?;
                return Ok(Some(produced));
            } else {
                // Not projecting, so just return the scan tuple.
                return Ok(Some(slot));
            }
        } else {
            // InstrCountFiltered1(node, 1);
            instr_count_filtered1(node.ss(), 1);
        }

        // Tuple fails qual, so free per-tuple memory and try again.
        reset_expr_context(node.ss(), estate);
    }
}

/// `ExecScan` (execScan.c) — scan the relation using the indicated access
/// method, returning the next qualifying tuple after checking the node's qual
/// and applying projection. Generic over the concrete node type `N`.
fn exec_scan_core<'mcx, N: ScanNode<'mcx>>(
    node: &mut N,
    access_mtd: fn(&mut N, &mut EStateData<'mcx>) -> PgResult<bool>,
    recheck_mtd: fn(&mut N, &mut EStateData<'mcx>) -> PgResult<bool>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // epqstate = node->ps.state->es_epq_active;
    // qual = node->ps.qual; projInfo = node->ps.ps_ProjInfo;
    let epq_active = estate.es_epq_active.is_some();
    let has_qual = node.ss().ps.qual.is_some();
    let has_proj = node.ss().ps.ps_ProjInfo.is_some();

    exec_scan_extended(
        node, epq_active, access_mtd, recheck_mtd, has_qual, has_proj, estate,
    )
}

/// `projInfo->pi_state.resultslot` — the projection's output slot. In the
/// owned model the projection writes the node's result tuple slot
/// (`ps_ResultTupleSlot`), which `pi_state.resultslot` aliases.
#[inline]
fn projection_result_slot(ss: &ScanStateData<'_>) -> SlotId {
    ss.ps
        .ps_ResultTupleSlot
        .expect("ExecScanExtended: ps_ResultTupleSlot not initialized for projection")
}

// ===========================================================================
// execScan.c: ExecScanReScan (generic core over the ScanState head)
// ===========================================================================

/// `ExecScanReScan` (execScan.c) — must be called within the ReScan function of
/// any plan node type that uses `ExecScan`.
fn exec_scan_rescan_ss<'mcx>(
    node: &mut ScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Clear the scan tuple so observers (e.g., execCurrent.c) can tell this plan
    // node is not positioned on a tuple.
    //   ExecClearTuple(node->ss_ScanTupleSlot);
    if let Some(slot) = node.ss_ScanTupleSlot {
        execTuples::exec_clear_tuple::call(estate.slot_mut(slot))?;
    }

    // Rescan EvalPlanQual tuple(s) if we're inside an EvalPlanQual recheck, but
    // don't lose the "blocked" status of blocked target relations.
    //   if (estate->es_epq_active != NULL)
    if estate.es_epq_active.is_some() {
        //   Index scanrelid = ((Scan *) node->ps.plan)->scanrelid;
        let scanrelid = scan_scanrelid(node);

        if scanrelid > 0 {
            //   epqstate->relsubs_done[scanrelid - 1] =
            //       epqstate->relsubs_blocked[scanrelid - 1];
            let blocked = epq_relsubs_blocked(estate, scanrelid - 1);
            epq_set_relsubs_done(estate, scanrelid - 1, blocked);
        } else {
            // An FDW or custom scan provider replaced the join with a scan, so
            // there are multiple RTIs; reset relsubs_done for all of them. The
            // relid set lives on the concrete ForeignScan->fs_base_relids /
            // CustomScan->custom_relids plan node and iteration is over
            // bms_next_member — neither field is modeled in types-nodes yet
            // (the join-pushdown EPQ path), so mirror PG and panic here rather
            // than approximate. Lands with the FDW/CustomScan join-pushdown
            // relid model.
            match node.ps.plan {
                Some(Node::ForeignScan(_)) => panic!(
                    "ExecScanReScan: ForeignScan join-pushdown EPQ rescan \
                     (fs_base_relids loop) not yet modeled in types-nodes"
                ),
                // CustomScan has no Node variant yet.
                Some(other) => panic!(
                    "ExecScanReScan: unexpected scan node: {:?}",
                    other.tag()
                ),
                None => panic!("ExecScanReScan: ScanStateData has no plan"),
            }
        }
    }

    Ok(())
}

// ===========================================================================
// execScan.c: ExecAssignScanProjectionInfo[WithVarno]
// ===========================================================================

/// `ExecAssignScanProjectionInfo` (execScan.c) — set up projection info for a
/// scan node, if necessary, using the scan plan node's `scanrelid` as the
/// expected varno. Delegates to `ExecConditionalAssignProjectionInfo` (here the
/// `ExecAssignScanProjectionInfoWithVarno` provider in execUtils).
///
/// The scan slot's descriptor must have been set already.
fn exec_assign_scan_projection_info<'mcx>(
    node: &mut ScanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Scan *scan = (Scan *) node->ps.plan;
    // ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, scan->scanrelid);
    let varno = scan_scanrelid(node) as i32;
    execUtils::exec_assign_scan_projection_info_with_varno::call(node, estate, varno)
}

// ===========================================================================
// Per-node seam entry points (marshal the concrete node type to the core).
// ===========================================================================

/// `exec_scan` seam — `ExecScan(&node->ss, accessMtd, recheckMtd)` for a
/// table-func-scan node. The access/recheck methods are the node's own
/// `TableFuncNext` / `TableFuncRecheck`.
fn exec_scan_tablefunc<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    access: execScan_seams::TableFuncScanAccessMtd,
    recheck: execScan_seams::TableFuncScanRecheckMtd,
) -> PgResult<Option<SlotId>> {
    exec_scan_core(node, access, recheck, estate)
}

/// `exec_scan_rescan` seam — `ExecScanReScan(&node->ss)` for a table-func-scan
/// node.
fn exec_scan_rescan_tablefunc<'mcx>(
    node: &mut TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exec_scan_rescan_ss(&mut node.ss, estate)
}

/// `exec_scan_indexonly` seam — `ExecScan(&node->ss, IndexOnlyNext,
/// IndexOnlyRecheck)` for an index-only scan node. The C returns the result
/// `TupleTableSlot *`; this seam reports whether a qualifying tuple was
/// produced (the C non-NULL return), matching the declared `bool` contract.
fn exec_scan_indexonly<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    access: execScan_seams::IndexOnlyScanAccessMtd,
    recheck: execScan_seams::IndexOnlyScanRecheckMtd,
) -> PgResult<bool> {
    let produced = exec_scan_core(node, access, recheck, estate)?;
    Ok(produced.is_some())
}
