//! Port of `src/backend/executor/nodeSeqscan.c` — support routines for
//! sequential scans of relations.
//!
//! INTERFACE ROUTINES
//! - [`ExecSeqScan`]            sequentially scans a relation
//! - `SeqNext`                  retrieve next tuple in sequential order
//! - [`ExecInitSeqScan`]        create and initialize a seqscan node
//! - [`ExecEndSeqScan`]         release any storage allocated
//! - [`ExecReScanSeqScan`]      rescan the relation
//! - [`ExecSeqScanEstimate`] / [`ExecSeqScanInitializeDSM`] /
//!   [`ExecSeqScanReInitializeDSM`] / [`ExecSeqScanInitializeWorker`] — parallel
//!
//! The `execScan.h` inline helpers (`ExecScanFetch`, `ExecScanExtended`) are
//! compiled into `nodeSeqscan.o` in C, so their control flow is reproduced here
//! as private functions; the non-inlined `ExecScan` driver is a thin wrapper
//! over `ExecScanExtended` and is likewise reproduced. Every leaf operation
//! into another subsystem goes through that owner's `-seams` crate (or a direct
//! dep where acyclic).
//!
//! The node state is the owned [`SeqScanState`] mutated through `&mut`; the C
//! `PlanState.state` back-pointer is replaced by threading `&mut EStateData`.
//! `ExecSeqScan*` returns `Ok(true)` when a tuple is in the node's scan slot
//! (no projection) or its projection result slot (projection), `Ok(false)` for
//! the C end-of-scan `NULL`.
//!
//! The active table scan descriptor is the table-AM-owned `TableScanDesc`,
//! the C-faithful value type the tableam.c owner was ported with. The node
//! stores it in [`SeqScanState::ss_currentScanDesc`] (the C
//! `ScanState.ss_currentScanDesc`) and threads it back into `getnextslot` /
//! `rescan` / `endscan`.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use backend_access_table_tableam_seams as tableam_seam;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execMain_seams as execMain;
use backend_executor_execScan_seams as execScan;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop;
use backend_access_transam_parallel_seams as parallel;
use backend_nodes_core_seams as bitmapset;

use types_error::PgResult;
use types_execparallel::{
    ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle, SerializeCursor,
};
use types_nodes::execnodes::ScanDirection;
use types_nodes::{EStateData, SeqScan, SeqScanState, SlotId, TupleTableSlot};
use types_tableam::relscan::ParallelTableScanDescData;

// ===========================================================================
// Access/recheck method types.
//
// In C these are `ExecScanAccessMtd`/`ExecScanRecheckMtd` (function pointers
// reinterpreted from `SeqNext`/`SeqRecheck`). Within this crate the scan
// helpers always use `SeqNext`/`SeqRecheck`, so we model the method "pointers"
// as plain function items.
//
// `SeqNext` stores the next tuple in the node's own `ss_ScanTupleSlot` and
// returns `true` when a tuple is available; `false` means the C accessMtd's
// NULL. `SeqRecheck` rechecks the node's current scan tuple.
// ===========================================================================

// The C access method returns a `TupleTableSlot *` — the produced slot (the
// node's `ss_ScanTupleSlot`) or NULL at end of scan. We model that as the
// produced slot id, `None` for NULL.
type AccessMtd =
    for<'mcx> fn(&mut SeqScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<Option<SlotId>>;
type RecheckMtd = for<'mcx> fn(&mut SeqScanState<'mcx>, &mut EStateData<'mcx>) -> bool;

/// Install this crate's seam implementations: the four parallel-scan entry
/// points other units (execParallel) call into.
pub fn init_seams() {
    backend_executor_nodeSeqscan_seams::exec_seqscan_estimate::set(seam_exec_seqscan_estimate);
    backend_executor_nodeSeqscan_seams::exec_seqscan_initialize_dsm::set(
        seam_exec_seqscan_initialize_dsm,
    );
    backend_executor_nodeSeqscan_seams::exec_seqscan_reinitialize_dsm::set(
        seam_exec_seqscan_reinitialize_dsm,
    );
    backend_executor_nodeSeqscan_seams::exec_seqscan_initialize_worker::set(
        seam_exec_seqscan_initialize_worker,
    );
}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `TupIsNull(slot)` — true if the slot is absent or marked empty.
#[inline]
fn tup_is_null(slot: Option<&TupleTableSlot>) -> bool {
    match slot {
        None => true,
        Some(slot) => slot.is_empty(),
    }
}

/// `TupIsNull(node->ss.ss_ScanTupleSlot)` for the node's scan slot.
#[inline]
fn scan_tuple_is_null(node: &SeqScanState<'_>, estate: &EStateData<'_>) -> bool {
    tup_is_null(node.ss.ss_ScanTupleSlot.map(|id| estate.slot(id)))
}

/// `SeqNext(node)` — the workhorse for `ExecSeqScan`. Lazily creates the
/// (serial) scan descriptor on first call, then fetches the next tuple into the
/// node's scan slot. Returns `Ok(Some(scan_slot))` if a tuple was stored
/// (the C `return slot`, where `slot == ss_ScanTupleSlot`), `Ok(None)` at
/// end-of-scan (the C `return NULL`).
fn SeqNext<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // get information from the estate and scan state
    // direction = estate->es_direction; slot = node->ss.ss_ScanTupleSlot;
    let direction: ScanDirection = estate.es_direction;
    let slot_id = node
        .ss
        .ss_ScanTupleSlot
        .expect("SeqNext: ss_ScanTupleSlot not initialized");

    // if (scandesc == NULL) { scandesc = table_beginscan(rel, es_snapshot, 0,
    //                                                     NULL);
    //                         node->ss.ss_currentScanDesc = scandesc; }
    if node.ss_currentScanDesc.is_none() {
        // We reach here if the scan is not parallel, or if we're serially
        // executing a scan that was planned to be parallel.
        let rel = node
            .ss
            .ss_currentRelation
            .as_ref()
            .expect("SeqNext: ss_currentRelation not opened");
        // C: table_beginscan(rel, estate->es_snapshot, 0, NULL). A seqscan
        // always runs under the query snapshot.
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("SeqNext: es_snapshot is NULL (C would pass NULL to the AM)");
        let scandesc = tableam_seam::table_beginscan::call(estate.es_query_cxt, rel, snapshot)?;
        node.ss_currentScanDesc = Some(scandesc);
    }

    // get the next tuple from the table:
    // if (table_scan_getnextslot(scandesc, direction, slot)) return slot;
    // return NULL;
    let scandesc = node
        .ss_currentScanDesc
        .as_deref_mut()
        .expect("SeqNext: ss_currentScanDesc not set");
    let mcx = estate.es_query_cxt;
    if tableam_seam::table_scan_getnextslot_direction::call(
        mcx,
        scandesc,
        direction,
        estate.slot_data_mut(slot_id),
    )? {
        Ok(Some(slot_id))
    } else {
        Ok(None)
    }
}

/// `SeqRecheck(node, slot)` — access-method routine to recheck a tuple in
/// EvalPlanQual. SeqScan never uses keys in `heap_beginscan`, so there is
/// nothing to recheck: always true.
fn SeqRecheck<'mcx>(_node: &mut SeqScanState<'mcx>, _estate: &mut EStateData<'mcx>) -> bool {
    true
}

// --- The one genuinely-unresolved parallel-scan primitive -------------------
//
// In C a `ParallelTableScanDesc` is a pointer into DSM bytes that `shm_toc_
// allocate`/`shm_toc_lookup` hand back as a raw address (`SerializeCursor`
// here). Typing a `ParallelTableScanDescData` over those raw shared bytes — and
// mutating one in place through its shared (`Arc`) alias — is the DSM
// byte-cursor-to-typed-shared-object model that the parallel-executor
// subsystem (`execParallel` / `shm_toc`) owns and has not yet landed. The three
// parallel DSM entry points carry their full table-AM / shm_toc orchestration
// over these helpers; only these primitives panic.

/// `(ParallelTableScanDesc) <dsm bytes at cursor>` — reinterpret a shm_toc DSM
/// byte cursor as the typed parallel-scan descriptor placed over it.
fn pscan_over_chunk(_cursor: SerializeCursor) -> alloc::boxed::Box<ParallelTableScanDescData> {
    panic!(
        "ParallelTableScanDescData over a shm_toc DSM byte cursor requires the \
         DSM/shm_toc typed-shared-object resolution (execParallel), not yet landed"
    );
}

/// `*pscan` as a mutable reference — C mutates the DSM-resident shared
/// descriptor in place; the shared `Arc` alias here cannot yield `&mut` until
/// the DSM interior-mutability model lands.
fn pscan_arc_get_mut(
    _pscan: &std::sync::Arc<ParallelTableScanDescData>,
) -> &mut ParallelTableScanDescData {
    panic!(
        "mutating the shared (DSM-resident, Arc) ParallelTableScanDescData in \
         place requires the parallel-scan shared-descriptor DSM interior- \
         mutability resolution (execParallel), not yet landed"
    );
}

// ===========================================================================
// `execScan.h` inline helpers, compiled into `nodeSeqscan.o` in C; reproduced
// here as private functions.
// ===========================================================================

/// `ExecScanFetch(node, epqstate, accessMtd, recheckMtd)` — check interrupts
/// and fetch the next potential tuple, substituting an EvalPlanQual test tuple
/// when inside an EPQ recheck. Returns `Ok(Some(slot))` with the produced slot
/// id (the C `return slot`), `Ok(None)` for the C `return NULL`. Note the
/// produced slot is the node's scan slot in most paths, but is the EPQ
/// `relsubs_slot` in the replacement-tuple branch — matching C, which returns
/// that distinct slot directly.
fn ExecScanFetch<'mcx>(
    node: &mut SeqScanState<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop::check_for_interrupts::call()?;

    // if (epqstate != NULL) — we are inside an EvalPlanQual recheck.
    if estate.es_epq_active.is_some() {
        // scanrelid = ((Scan *) node->ps.plan)->scanrelid;
        let scanrelid = scan_scanrelid(node);
        let scan_slot = node
            .ss
            .ss_ScanTupleSlot
            .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");

        if scanrelid == 0 {
            // This is a ForeignScan or CustomScan which has pushed down a join
            // to the remote side. If it is a descendant node in the EPQ recheck
            // plan tree, run the recheck method function. Otherwise fall through
            // to the access method below.
            //
            // if (bms_is_member(epqstate->epqParam, node->ps.plan->extParam))
            if epq_param_is_member_of_ext_param(node, estate) {
                // The recheck method is responsible for rechecking the
                // scan/join quals and storing the correct tuple in the slot.
                // TupleTableSlot *slot = node->ss_ScanTupleSlot;
                if !recheck_mtd(node, estate) {
                    // would not be returned by scan
                    execTuples::exec_clear_tuple::call(estate, scan_slot)?;
                }
                // return slot; (the node's scan slot)
                return Ok(Some(scan_slot));
            }
        } else if relsubs_done(estate, scanrelid - 1) {
            // Return empty slot, as either there is no EPQ tuple for this rel
            // or we already returned it.
            // TupleTableSlot *slot = node->ss_ScanTupleSlot;
            // return ExecClearTuple(slot);
            execTuples::exec_clear_tuple::call(estate, scan_slot)?;
            return Ok(Some(scan_slot));
        } else if let Some(repl) = relsubs_slot(estate, scanrelid - 1) {
            // Return replacement tuple provided by the EPQ caller.
            // TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];
            //
            // Mark to remember that we shouldn't return it again.
            set_relsubs_done(estate, scanrelid - 1, true);

            // Return empty slot if we haven't got a test tuple.
            // if (TupIsNull(slot)) return NULL;
            if tup_is_null(Some(estate.slot(repl))) {
                return Ok(None);
            }
            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate) {
                // would not be returned by scan
                // return ExecClearTuple(slot);
                execTuples::exec_clear_tuple::call(estate, repl)?;
                return Ok(Some(repl));
            }
            // return slot; — the EPQ replacement slot itself is the produced
            // tuple (a distinct slot from ss_ScanTupleSlot, as in C).
            return Ok(Some(repl));
        } else if relsubs_rowmark_present(estate, scanrelid - 1) {
            // Fetch and return replacement tuple using a non-locking rowmark.
            // TupleTableSlot *slot = node->ss_ScanTupleSlot;
            //
            // Mark to remember that we shouldn't return more.
            set_relsubs_done(estate, scanrelid - 1, true);

            // if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot))
            //     return NULL;
            if !eval_plan_qual_fetch_row_mark(node, scanrelid, estate)? {
                return Ok(None);
            }
            // if (TupIsNull(slot)) return NULL;
            if scan_tuple_is_null(node, estate) {
                return Ok(None);
            }
            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate) {
                // return ExecClearTuple(slot);
                execTuples::exec_clear_tuple::call(estate, scan_slot)?;
                return Ok(Some(scan_slot));
            }
            // return slot;
            return Ok(Some(scan_slot));
        }
    }

    // Run the node-type-specific access method function to get the next tuple.
    // return (*accessMtd) (node);
    access_mtd(node, estate)
}

/// `ExecScanExtended(node, accessMtd, recheckMtd, epqstate, qual, projInfo)`
/// (pg_attribute_always_inline): scan using the access method; optionally
/// evaluate `qual` and apply `projInfo`.
///
/// `has_qual` / `has_proj_info` mirror the C `qual != NULL` / `projInfo != NULL`
/// tests. On success the resulting tuple lives in the node's scan slot (no
/// projection) or the projection result slot (projection); the boolean says
/// whether a tuple is available.
#[allow(clippy::too_many_arguments)]
fn ExecScanExtended<'mcx>(
    node: &mut SeqScanState<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    has_qual: bool,
    has_proj_info: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .expect("ExecScanExtended: ps_ExprContext not initialized");

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj_info {
        // ResetExprContext(econtext);
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
        return ExecScanFetch(node, access_mtd, recheck_mtd, estate);
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        // slot = ExecScanFetch(node, epqstate, accessMtd, recheckMtd);
        let slot = ExecScanFetch(node, access_mtd, recheck_mtd, estate)?;

        // If the slot returned by the accessMtd contains NULL, then it means
        // there is nothing more to scan so we just return an empty slot, being
        // careful to use the projection result slot so it has correct tupleDesc.
        // if (TupIsNull(slot)) ...
        let slot_is_null = match slot {
            None => true,
            Some(s) => tup_is_null(Some(estate.slot(s))),
        };
        if slot_is_null {
            if has_proj_info {
                let result_slot = node
                    .ss
                    .ps
                    .ps_ResultTupleSlot
                    .expect("ExecScanExtended: ps_ResultTupleSlot not initialized");
                // return ExecClearTuple(projInfo->pi_state.resultslot);
                execTuples::exec_clear_tuple::call(estate, result_slot)?;
                return Ok(Some(result_slot));
            } else {
                // return slot;
                return Ok(slot);
            }
        }
        let slot = slot.expect("ExecScanExtended: non-null slot");

        // Place the current tuple into the expr context.
        // econtext->ecxt_scantuple = slot;
        estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

        // Check that the current tuple satisfies the qual-clause.
        //
        // Check for non-null qual here to avoid a function call to ExecQual()
        // when the qual is null.
        let passes = if has_qual {
            let qual = node
                .ss
                .ps
                .qual
                .as_deref_mut()
                .expect("ExecScanExtended: has_qual but qual is None");
            execExpr::exec_qual::call(qual, econtext, estate)?
        } else {
            true
        };

        if passes {
            // Found a satisfactory scan tuple.
            if has_proj_info {
                // Form a projection tuple, store it in the result tuple slot
                // and return it.
                // return ExecProject(projInfo);  /* -> resultslot */
                let result_slot = execExpr::exec_project::call(&mut node.ss.ps, estate)?;
                return Ok(Some(result_slot));
            } else {
                // Here, we aren't projecting, so just return scan tuple.
                // return slot;
                return Ok(Some(slot));
            }
        } else {
            // InstrCountFiltered1(node, 1);
            InstrCountFiltered1(node, 1);
        }

        // Tuple fails qual, so free per-tuple memory and try again.
        // ResetExprContext(econtext);
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }
}

/// `ExecScan(node, accessMtd, recheckMtd)` — the non-inlined `execScan.c`
/// driver used by the EPQ variant. Equivalent to `ExecScanExtended(node,
/// accessMtd, recheckMtd, node->ps.state->es_epq_active, node->ps.qual,
/// node->ps.ps_ProjInfo)`. Reproduced as a thin wrapper over the inlined
/// `ExecScanExtended` above.
fn ExecScan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let has_qual = node.ss.ps.qual.is_some();
    let has_proj_info = node.ss.ps.ps_ProjInfo.is_some();
    ExecScanExtended(node, access_mtd, recheck_mtd, has_qual, has_proj_info, estate)
}

/// `InstrCountFiltered1(node, delta)` — bump `node->ps.instrument->nfiltered1`
/// when instrumentation is enabled (`executor/executor.h` macro).
#[inline]
fn InstrCountFiltered1(node: &mut SeqScanState<'_>, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered1 += delta as f64;
    }
}

// --- EPQ-state readers (the `epqstate->relsubs_*` arrays live in
// `estate.es_epq_active`; the canonical owned `EPQState` carries them as
// `Option<PgVec<..>>`: the outer `None` is the C `NULL` array, and a per-entry
// value mirrors the C non-NULL entry) ----------------------------------------

/// `((Scan *) node->ps.plan)->scanrelid`.
fn scan_scanrelid(node: &SeqScanState<'_>) -> u32 {
    match node.ss.ps.plan {
        Some(types_nodes::nodes::Node::SeqScan(s)) => s.scan.scanrelid,
        Some(other) => panic!("SeqScan node has unexpected plan node: {:?}", other.tag()),
        None => panic!("SeqScan node has no plan"),
    }
}

/// `bms_is_member(epqstate->epqParam, node->ps.plan->extParam)`.
fn epq_param_is_member_of_ext_param(node: &SeqScanState<'_>, estate: &EStateData<'_>) -> bool {
    let epqstate = estate
        .es_epq_active
        .as_deref()
        .expect("epq_param_is_member_of_ext_param: no active EPQ state");
    let bit = epqstate.epqParam;
    let ext_param = node.ss.ps.plan.and_then(|p| p.plan_head().extParam.as_deref());
    bitmapset::bms_is_member::call(bit, ext_param)
}

/// `epqstate->relsubs_done[idx]`. The canonical array is `Option<PgVec<bool>>`;
/// a `None` array (the C `NULL`) or a missing entry reads as `false`.
fn relsubs_done(estate: &EStateData<'_>, idx: u32) -> bool {
    let epqstate = estate
        .es_epq_active
        .as_deref()
        .expect("relsubs_done: no active EPQ state");
    epqstate
        .relsubs_done
        .as_ref()
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `epqstate->relsubs_done[idx] = v`.
fn set_relsubs_done(estate: &mut EStateData<'_>, idx: u32, v: bool) {
    let epqstate = estate
        .es_epq_active
        .as_deref_mut()
        .expect("set_relsubs_done: no active EPQ state");
    let arr = epqstate
        .relsubs_done
        .as_mut()
        .expect("set_relsubs_done: relsubs_done array is NULL");
    arr[idx as usize] = v;
}

/// `epqstate->relsubs_slot[idx]` — the substitute slot id, if non-NULL. The
/// canonical array is `Option<PgVec<Option<SlotId>>>`; the outer `None` (C
/// `NULL` array) or an inner `None` entry (C `NULL`) is "no substitute".
fn relsubs_slot(estate: &EStateData<'_>, idx: u32) -> Option<SlotId> {
    let epqstate = estate
        .es_epq_active
        .as_deref()
        .expect("relsubs_slot: no active EPQ state");
    epqstate
        .relsubs_slot
        .as_ref()
        .and_then(|v| v.get(idx as usize).copied())
        .flatten()
}

/// `epqstate->relsubs_rowmark[idx] != NULL`. The canonical array is
/// `Option<PgVec<bool>>` where `true` is the C non-NULL `ExecAuxRowMark *`.
fn relsubs_rowmark_present(estate: &EStateData<'_>, idx: u32) -> bool {
    let epqstate = estate
        .es_epq_active
        .as_deref()
        .expect("relsubs_rowmark_present: no active EPQ state");
    epqstate
        .relsubs_rowmark
        .as_ref()
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)` (execMain.c) — fetch
/// the replacement tuple for a non-locking rowmark into the node's scan slot.
/// Owned by execMain; routed through its seam.
fn eval_plan_qual_fetch_row_mark<'mcx>(
    node: &mut SeqScanState<'mcx>,
    scanrelid: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let scan_slot = node
        .ss
        .ss_ScanTupleSlot
        .expect("eval_plan_qual_fetch_row_mark: ss_ScanTupleSlot not initialized");
    execMain::eval_plan_qual_fetch_row_mark::call(estate, scanrelid, scan_slot)
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// `ExecSeqScan(pstate)` — no EPQ, no qual, no projection. Returns the produced
/// tuple's slot id (the C `return slot`), `None` at end of scan.
pub fn ExecSeqScan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // Assert(pstate->state->es_epq_active == NULL);
    // Assert(pstate->qual == NULL); Assert(pstate->ps_ProjInfo == NULL);
    debug_assert!(estate.es_epq_active.is_none());
    debug_assert!(node.ss.ps.qual.is_none());
    debug_assert!(node.ss.ps.ps_ProjInfo.is_none());
    ExecScanExtended(node, SeqNext, SeqRecheck, false, false, estate)
}

/// Variant of `ExecSeqScan` when qual evaluation is required.
pub fn ExecSeqScanWithQual<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    debug_assert!(estate.es_epq_active.is_none());
    debug_assert!(node.ss.ps.qual.is_some());
    debug_assert!(node.ss.ps.ps_ProjInfo.is_none());
    ExecScanExtended(node, SeqNext, SeqRecheck, true, false, estate)
}

/// Variant of `ExecSeqScan` when projection is required.
pub fn ExecSeqScanWithProject<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    debug_assert!(estate.es_epq_active.is_none());
    debug_assert!(node.ss.ps.qual.is_none());
    debug_assert!(node.ss.ps.ps_ProjInfo.is_some());
    ExecScanExtended(node, SeqNext, SeqRecheck, false, true, estate)
}

/// Variant of `ExecSeqScan` when qual evaluation and projection are required.
pub fn ExecSeqScanWithQualProject<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    debug_assert!(estate.es_epq_active.is_none());
    debug_assert!(node.ss.ps.qual.is_some());
    debug_assert!(node.ss.ps.ps_ProjInfo.is_some());
    ExecScanExtended(node, SeqNext, SeqRecheck, true, true, estate)
}

/// Variant of `ExecSeqScan` for when EvalPlanQual evaluation is required.
pub fn ExecSeqScanEPQ<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    ExecScan(node, SeqNext, SeqRecheck, estate)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitSeqScan`]:
/// `castNode(SeqScanState, pstate)` then run the installed variant, returning
/// the produced tuple's slot id (the C `return slot`) or `None`.
fn exec_seq_scan_node<'mcx>(
    pstate: &mut types_nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        types_nodes::PlanStateNode::SeqScan(node) => node,
        other => panic!("castNode(SeqScanState, pstate) failed: tag {}", other.tag()),
    };

    // Re-derive the variant the install selected (same selection as
    // ExecInitSeqScan). The EPQ install path goes through ExecScan. Each variant
    // already returns the produced slot id (the C `return slot`), which we pass
    // straight back — this is the EPQ `relsubs_slot` in the replacement-tuple
    // path, the projection result slot when projecting, else the scan slot.
    let has_qual = node.ss.ps.qual.is_some();
    let has_proj = node.ss.ps.ps_ProjInfo.is_some();
    if estate.es_epq_active.is_some() {
        ExecSeqScanEPQ(node, estate)
    } else {
        match (has_qual, has_proj) {
            (false, false) => ExecSeqScan(node, estate),
            (true, false) => ExecSeqScanWithQual(node, estate),
            (false, true) => ExecSeqScanWithProject(node, estate),
            (true, true) => ExecSeqScanWithQualProject(node, estate),
        }
    }
}

/// `ExecInitSeqScan(node, estate, eflags)` — create and initialize a
/// `SeqScanState`. Allocates the state tree in `estate.es_query_cxt` (C:
/// `makeNode` in the per-query context current during `ExecInitNode`), so it is
/// fallible on OOM.
pub fn ExecInitSeqScan<'mcx>(
    node: &'mcx SeqScan<'mcx>,
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<mcx::PgBox<'mcx, SeqScanState<'mcx>>> {
    // Once upon a time it was possible to have an outerPlan of a SeqScan, but
    // not any more.
    // Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(node.scan.plan.lefttree.is_none());
    debug_assert!(node.scan.plan.righttree.is_none());

    let mcx = estate.es_query_cxt;

    // create state structure (makeNode(SeqScanState))
    //
    // scanstate->ss.ps.plan = (Plan *) node; scanstate->ss.ps.state = estate;
    // The plan back-link aliases the caller's read-only plan node; the EState
    // back-link is the threaded `estate` parameter.
    let mut scanstate = mcx::alloc_in(mcx, SeqScanState::default())?;
    scanstate.ss.ps.plan = Some(plan_node);

    // Miscellaneous initialization: create expression context for node.
    // ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    // open the scan relation
    // scanstate->ss.ss_currentRelation =
    //     ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    let rel = execUtils::exec_open_scan_relation::call(estate, node.scan.scanrelid, eflags)?;
    scanstate.ss.ss_currentRelation = Some(rel);

    // and create slot with the appropriate rowtype
    // ExecInitScanTupleSlot(estate, &scanstate->ss,
    //     RelationGetDescr(rel), table_slot_callbacks(rel));
    let rel_ref = scanstate
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("ExecInitSeqScan: relation just opened");
    let tts_ops = backend_access_table_tableam::table_slot_callbacks(rel_ref);
    let tupdesc = Some(mcx::alloc_in(mcx, rel_ref.rd_att.clone_in(mcx)?)?);
    execTuples::exec_init_scan_tuple_slot::call(estate, &mut scanstate.ss, tupdesc, tts_ops)?;

    // Initialize result type and projection.
    // ExecInitResultTypeTL(&scanstate->ss.ps);
    execTuples::exec_init_result_type_tl::call(&mut scanstate.ss.ps, estate)?;
    // ExecAssignScanProjectionInfo(&scanstate->ss);
    execScan::exec_assign_scan_projection_info::call(&mut scanstate.ss, estate)?;

    // initialize child expressions
    // scanstate->ss.ps.qual =
    //     ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
    let qual_list = node.scan.plan.qual.as_deref();
    scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual_list, &mut scanstate.ss.ps, estate)?;

    // When EvalPlanQual() is not in use, assign ExecProcNode for this node based
    // on the presence of qual and projection. The owned model uses a single
    // dispatch callback (exec_seq_scan_node) that re-derives the variant; the C
    // ExecProcNode-pointer specialization is a per-combination optimization that
    // does not change observable behaviour.
    scanstate.ss.ps.ExecProcNode = Some(exec_seq_scan_node);

    Ok(scanstate)
}

/// `ExecEndSeqScan(node)` — free any storage allocated through table-AM
/// routines.
pub fn ExecEndSeqScan<'mcx>(node: &mut SeqScanState<'mcx>) -> PgResult<()> {
    // scanDesc = node->ss.ss_currentScanDesc;
    // if (scanDesc != NULL) table_endscan(scanDesc);
    if let Some(scandesc) = node.ss_currentScanDesc.take() {
        tableam_seam::table_endscan::call(scandesc)?;
    }
    Ok(())
}

/// `ExecReScanSeqScan(node)` — rescan the relation.
pub fn ExecReScanSeqScan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // scan = node->ss.ss_currentScanDesc;
    // if (scan != NULL) table_rescan(scan, NULL);  /* scan desc / new keys */
    if let Some(scandesc) = node.ss_currentScanDesc.as_deref_mut() {
        tableam_seam::table_rescan::call(scandesc)?;
    }

    // ExecScanReScan((ScanState *) node);
    execScan::exec_scan_rescan_ss::call(&mut node.ss, estate)
}

// ===========================================================================
//                          Parallel Scan Support
//
// The parallel-query DSM framework (execParallel.c / parallel.c) reaches these
// entry points through the opaque `PlanStateHandle` / `ParallelContextHandle`
// vocabulary (`types-execparallel`). Each real function below carries the full
// `nodeSeqscan.c` logic over the concrete `SeqScanState`; the `init_seams()`
// installers are the opaque-handle adapters the framework calls. Bridging an
// opaque `PlanStateHandle` back to the concrete `SeqScanState` is owned by the
// execParallel PlanState resolution (not yet landed), so the installed adapter
// panics there — mirror-PG-and-panic, exactly as a call into a not-yet-resolved
// owner does.
// ===========================================================================

/// `ExecSeqScanEstimate(node, pcxt)` — compute the DSM space we'll need and
/// inform `pcxt->estimator`.
pub fn ExecSeqScanEstimate<'mcx>(
    node: &mut SeqScanState<'mcx>,
    pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // node->pscan_len = table_parallelscan_estimate(rel, estate->es_snapshot);
    let snapshot: types_tableam::Snapshot = estate.es_snapshot.as_ref().map(|rc| (**rc).clone());
    let rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("ExecSeqScanEstimate: ss_currentRelation not opened");
    node.pscan_len = backend_access_table_tableam::table_parallelscan_estimate(rel, &snapshot)?;

    // shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
    let estimator = parallel::pcxt_estimator::call(pcxt);
    parallel::shm_toc_estimate_chunk::call(estimator, node.pscan_len);
    // shm_toc_estimate_keys(&pcxt->estimator, 1);
    parallel::shm_toc_estimate_keys::call(estimator, 1);
    Ok(())
}

/// `ExecSeqScanInitializeDSM(node, pcxt)` — set up a parallel heap scan
/// descriptor in the DSM.
pub fn ExecSeqScanInitializeDSM<'mcx>(
    node: &mut SeqScanState<'mcx>,
    pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // EState *estate = node->ss.ps.state;  (threaded in as `estate`)
    // ParallelTableScanDesc pscan;

    // pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
    let toc = parallel::pcxt_toc::call(pcxt);
    let pscan_cursor = parallel::shm_toc_allocate::call(toc, node.pscan_len);

    // table_parallelscan_initialize(node->ss.ss_currentRelation, pscan,
    //                               estate->es_snapshot);
    //
    // `pscan` is a `ParallelTableScanDescData` placed over the DSM bytes named
    // by `pscan_cursor` (a `SerializeCursor`). Typing that struct over the raw
    // shm_toc byte cursor — so it can be handed (`&mut`) to
    // `table_parallelscan_initialize` and (`Arc`) to `table_beginscan_parallel`
    // — is owned by the DSM/shm_toc byte-cursor-to-typed-shared-object
    // resolution (execParallel), which has not yet landed. The two table-AM
    // callees and `shm_toc_insert` are all wired and ready for it.
    let snapshot: types_tableam::Snapshot = estate.es_snapshot.as_ref().map(|rc| (**rc).clone());
    let plan_node_id = node
        .ss
        .ps
        .plan
        .map(|p| p.plan_head().plan_node_id)
        .expect("ExecSeqScanInitializeDSM: no plan");

    let mut pscan: alloc::boxed::Box<ParallelTableScanDescData> = pscan_over_chunk(pscan_cursor);
    {
        let rel = node
            .ss
            .ss_currentRelation
            .as_ref()
            .expect("ExecSeqScanInitializeDSM: ss_currentRelation not opened");
        backend_access_table_tableam::table_parallelscan_initialize(rel, &mut pscan, &snapshot)?;
    }

    // shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
    parallel::shm_toc_insert::call(toc, plan_node_id as u64, pscan_cursor);

    // node->ss.ss_currentScanDesc = table_beginscan_parallel(rel, pscan);
    let pscan_arc: std::sync::Arc<ParallelTableScanDescData> = std::sync::Arc::from(pscan);
    let rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("ExecSeqScanInitializeDSM: ss_currentRelation not opened");
    let scandesc =
        backend_access_table_tableam::table_beginscan_parallel(estate.es_query_cxt, rel, pscan_arc)?;
    node.ss_currentScanDesc = Some(scandesc);
    Ok(())
}

/// `ExecSeqScanReInitializeDSM(node, pcxt)` — reset shared state before a fresh
/// scan.
pub fn ExecSeqScanReInitializeDSM<'mcx>(
    node: &mut SeqScanState<'mcx>,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // ParallelTableScanDesc pscan;

    // pscan = node->ss.ss_currentScanDesc->rs_parallel;
    //
    // `rs_parallel` is the shared (DSM-resident, refcounted) descriptor; C
    // dereferences and mutates it in place. Producing both the typed descriptor
    // from the opaque carrier and the `&mut ParallelTableScanDescData` the wired
    // `table_parallelscan_reinitialize` seam expects out of the shared `Arc` is
    // the parallel-scan shared-descriptor resolution (DSM interior mutability),
    // not yet landed.
    let pscan = parallel_scandesc_rs_parallel(&node.ss_currentScanDesc);

    // table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
    let rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("ExecSeqScanReInitializeDSM: ss_currentRelation not opened");
    let pscan: &mut ParallelTableScanDescData = pscan_arc_get_mut(&pscan);
    tableam_seam::table_parallelscan_reinitialize::call(rel, pscan)
}

/// `ExecSeqScanInitializeWorker(node, pwcxt)` — copy relevant information from
/// the TOC into planstate (in a parallel worker).
pub fn ExecSeqScanInitializeWorker<'mcx>(
    node: &mut SeqScanState<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParallelTableScanDesc pscan;

    // pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
    let plan_node_id = node
        .ss
        .ps
        .plan
        .map(|p| p.plan_head().plan_node_id)
        .expect("ExecSeqScanInitializeWorker: no plan");
    let toc = parallel::pwcxt_toc::call(pwcxt);
    let pscan_cursor = parallel::shm_toc_lookup::call(toc, plan_node_id as u64, false)
        .expect("ExecSeqScanInitializeWorker: shm_toc_lookup(noError=false) returned NULL");

    // node->ss.ss_currentScanDesc = table_beginscan_parallel(rel, pscan);
    //
    // `pscan` is the typed `ParallelTableScanDescData` placed over the DSM bytes
    // named by `pscan_cursor`; producing the `Arc` view of it over the raw
    // shm_toc byte cursor is the DSM byte-cursor-to-typed-shared-object
    // resolution (execParallel), not yet landed. `table_beginscan_parallel` is
    // wired and ready for it.
    let rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .expect("ExecSeqScanInitializeWorker: ss_currentRelation not opened");
    let pscan_arc: std::sync::Arc<ParallelTableScanDescData> =
        std::sync::Arc::from(pscan_over_chunk(pscan_cursor));
    let scandesc =
        backend_access_table_tableam::table_beginscan_parallel(estate.es_query_cxt, rel, pscan_arc)?;
    node.ss_currentScanDesc = Some(scandesc);
    Ok(())
}

// --- Parallel `ss_currentScanDesc` access -----------------------------------
//
// The parallel paths store/read the typed `TableScanDesc` that
// `table_beginscan_parallel` returns directly in the node's
// `ss_currentScanDesc` field (the value model). Reading the shared parallel
// descriptor off `rs_parallel` is a plain field read.

/// `node->ss.ss_currentScanDesc->rs_parallel` — the shared (DSM-resident,
/// refcounted) parallel-scan descriptor. C dereferences `ss_currentScanDesc`
/// unconditionally here (a NULL would be a crash), so a missing descriptor /
/// non-parallel scan panics loudly.
fn parallel_scandesc_rs_parallel(
    slot: &Option<types_tableam::relscan::TableScanDesc<'_>>,
) -> std::sync::Arc<ParallelTableScanDescData> {
    let scan = slot
        .as_ref()
        .expect("ExecSeqScanReInitializeDSM: ss_currentScanDesc not set");
    std::sync::Arc::clone(
        scan.rs_parallel
            .as_ref()
            .expect("ExecSeqScanReInitializeDSM: scan descriptor is not parallel (rs_parallel NULL)"),
    )
}

// --- Inward seam installers (opaque-handle adapters) ------------------------

fn seam_exec_seqscan_estimate(
    _node: PlanStateHandle,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    panic!(
        "exec_seqscan_estimate: bridging the opaque PlanStateHandle to the \
         concrete SeqScanState (and reaching its EState) is owned by the \
         execParallel PlanState resolution, not yet landed — call \
         ExecSeqScanEstimate over the concrete node instead"
    );
}

fn seam_exec_seqscan_initialize_dsm(
    _node: PlanStateHandle,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    panic!(
        "exec_seqscan_initialize_dsm: bridging the opaque PlanStateHandle to the \
         concrete SeqScanState is owned by the execParallel PlanState resolution, \
         not yet landed"
    );
}

fn seam_exec_seqscan_reinitialize_dsm(
    _node: PlanStateHandle,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    panic!(
        "exec_seqscan_reinitialize_dsm: bridging the opaque PlanStateHandle to the \
         concrete SeqScanState is owned by the execParallel PlanState resolution, \
         not yet landed"
    );
}

fn seam_exec_seqscan_initialize_worker(
    _node: PlanStateHandle,
    _pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    panic!(
        "exec_seqscan_initialize_worker: bridging the opaque PlanStateHandle to \
         the concrete SeqScanState is owned by the execParallel PlanState \
         resolution, not yet landed"
    );
}
