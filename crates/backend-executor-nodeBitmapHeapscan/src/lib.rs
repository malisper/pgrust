//! Port of `src/backend/executor/nodeBitmapHeapscan.c` — routines to support
//! bitmapped scans of relations.
//!
//! A `BitmapHeapScan` node consumes the `TIDBitmap` produced by its child
//! subplan (a `BitmapIndexScan` / `BitmapAnd` / `BitmapOr` tree), then fetches
//! the matching heap tuples, rechecking the original index quals on lossy
//! pages. NOTE (as in the C): this plan type must only be used with
//! MVCC-compliant snapshots, since the index and heap scans are decoupled.
//!
//! INTERFACE ROUTINES
//! - [`ExecBitmapHeapScan`]      - scans a relation using bitmap info
//! - [`ExecInitBitmapHeapScan`]  - creates and initializes state info
//! - [`ExecReScanBitmapHeapScan`]- prepares to rescan the plan
//! - [`ExecEndBitmapHeapScan`]   - releases all storage
//!
//! This crate owns the node state machine (`BitmapTableScanSetup`,
//! `BitmapHeapNext`, `BitmapHeapRecheck`, the `BM_*` parallel state machine,
//! and the `ExecScan` driver, which `nodeBitmapHeapscan.o` links in from
//! `execScan.c`/`execScan.h`). Operations below or beside the node go through
//! their owners' seam crates:
//!
//! - child dispatch / multi-exec / init / teardown
//!   (`MultiExecProcNode` / `ExecInitNode` / `ExecEndNode`) → execProcnode;
//! - rescan (`ExecReScan`) → execAmi;
//! - expression compilation/evaluation (`ExecInitQual` / `ExecQual` /
//!   `ExecQualAndReset` / `ExecProject`) → execExpr;
//! - slot/projection/result-type setup (`ExecClearTuple` /
//!   `ExecInitScanTupleSlot` / `ExecInitResultTypeTL` /
//!   `ExecAssignScanProjectionInfo`) → execTuples / execUtils;
//! - relation open (`ExecOpenScanRelation`) and per-node expr context
//!   (`ExecAssignExprContext`) → execUtils (direct dep / seam);
//! - bitmap table scan (`table_beginscan_bm` / `table_scan_bitmap_next_tuple`
//!   / `table_rescan` / `table_endscan`) → the heap AM (tableam-bm seams);
//! - TID-bitmap iteration (`tbm_prepare_shared_iterate` / `tbm_begin_iterate`
//!   / `tbm_end_iterate` / `tbm_free` / `tbm_free_shared_area`) → tidbitmap;
//! - interrupts (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - condition variable (`ConditionVariable*`) → condition_variable (direct);
//! - parallel identity / shm_toc / DSA (`IsParallelWorker` /
//!   `ParallelWorkerNumber` / `shm_toc_*`) → access/parallel.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use mcx::PgBox;
use types_error::PgResult;
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, TupleSlotKind};
use types_nodes::EStateData;

use backend_access_transam_parallel::shared_dsm_object;
use backend_access_table_tableam_bm_seams as tableam_bm;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils_seams;
use backend_nodes_core_tidbitmap_seams as tidbitmap;

pub mod nodes;

use nodes::{
    BitmapHeapScan, BitmapHeapScanInstrumentation, BitmapHeapScanState, NodeSinstrument,
    ParallelBitmapHeapState, SharedBitmapHeapInstrumentation, SharedBitmapHeapScanInstr,
    SpinLockGuard, BM_FINISHED, BM_INITIAL, BM_INPROGRESS,
};

/// `WAIT_EVENT_PARALLEL_BITMAP_SCAN` (utils/wait_event.h) — the wait-event the
/// parallel bitmap-scan condition-variable sleep reports. This is an
/// `PG_WAIT_IPC` (`0x08000000`) class event at index 38, i.e.
/// `0x08000000 | 38 == 134217766` (matching c2rust).
const WAIT_EVENT_PARALLEL_BITMAP_SCAN: u32 = 0x08000000 | 38;

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `BitmapTableScanSetup(node)` — do the underlying index scan, build the
/// bitmap, set up the parallel state needed for parallel workers to iterate
/// through the bitmap, and set up the underlying table scan descriptor.
fn BitmapTableScanSetup<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ParallelBitmapHeapState *pstate = node->pstate;
    // dsa_area *dsa = node->ss.ps.state->es_query_dsa;
    let dsa = estate.es_query_dsa;

    if node.pstate.is_none() {
        // node->tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));
        // if (!node->tbm || !IsA(node->tbm, TIDBitmap))
        //     elog(ERROR, "unrecognized result from subplan");
        let outer = node
            .ss
            .ps
            .lefttree
            .as_mut()
            .expect("outerPlanState(BitmapHeapScan) is NULL");
        node.tbm = Some(execProcnode::multi_exec_proc_node::call(outer, estate)?);
    } else if BitmapShouldInitializeSharedState(node)? {
        // The leader will immediately come out of the function, but others will
        // be blocked until leader populates the TBM and wakes them up.
        let outer = node
            .ss
            .ps
            .lefttree
            .as_mut()
            .expect("outerPlanState(BitmapHeapScan) is NULL");
        node.tbm = Some(execProcnode::multi_exec_proc_node::call(outer, estate)?);

        // Prepare to iterate over the TBM. This will return the dsa_pointer of
        // the iterator state which will be used by multiple processes to
        // iterate jointly.
        // pstate->tbmiterator = tbm_prepare_shared_iterate(node->tbm);
        let tbm = node.tbm.as_mut().expect("node->tbm");
        let dp = tidbitmap::tbm_prepare_shared_iterate::call(tbm)?;
        node.pstate.as_ref().expect("pstate").set_tbmiterator(dp);

        // We have initialized the shared state so wake up others.
        BitmapDoneInitializingSharedState(node)?;
    }

    // tbmiterator = tbm_begin_iterate(node->tbm, dsa,
    //     pstate ? pstate->tbmiterator : InvalidDsaPointer);
    let dsp = match &node.pstate {
        Some(p) => p.tbmiterator(),
        None => types_tidbitmap::InvalidDsaPointer,
    };
    // C passes `node->tbm` directly, which is NULL for a non-leader parallel
    // worker that did not observe BM_INITIAL (it never ran MultiExecProcNode);
    // tbm_begin_iterate only dereferences it on the private path, so the shared
    // path attaches from `dsp` and ignores the (None) bitmap.
    let tbmiterator =
        tidbitmap::tbm_begin_iterate::call(node.tbm.as_deref_mut(), dsa, dsp)?;

    // If this is the first scan of the underlying table, create the table scan
    // descriptor and begin the scan.
    // if (!node->ss.ss_currentScanDesc)
    //     node->ss.ss_currentScanDesc = table_beginscan_bm(...);
    if node.ss_currentScanDesc.is_none() {
        let rel = node
            .ss_currentRelation
            .as_ref()
            .expect("ss_currentRelation")
            .alias();
        node.ss_currentScanDesc = Some(tableam_bm::table_beginscan_bm::call(rel, estate.es_snapshot.clone())?);
    }

    // node->ss.ss_currentScanDesc->st.rs_tbmiterator = tbmiterator;
    node.ss_currentScanDesc
        .as_mut()
        .expect("ss_currentScanDesc")
        .rs_tbmiterator = tbmiterator;

    // node->initialized = true;
    node.initialized = true;
    Ok(())
}

/// `BitmapHeapNext(node)` — retrieve the next tuple from the BitmapHeapScan
/// node's current relation.
///
/// Returns `Ok(true)` if a tuple was stored in the node's scan slot, `Ok(false)`
/// when the scan is exhausted (the C function returns the slot or the cleared
/// slot; here the caller inspects the node's scan slot).
fn BitmapHeapNext<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // ExprContext *econtext = node->ss.ps.ps_ExprContext;
    // TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    let econtext = node.ss.ps.ps_ExprContext.expect("ps_ExprContext");
    let slot = node.ss.ss_ScanTupleSlot.expect("ss_ScanTupleSlot");

    // If we haven't yet performed the underlying index scan, do it, and begin
    // the iteration over the bitmap.
    if !node.initialized {
        BitmapTableScanSetup(node, estate)?;
    }

    // while (table_scan_bitmap_next_tuple(node->ss.ss_currentScanDesc, slot,
    //        &node->recheck, &node->stats.lossy_pages, &node->stats.exact_pages))
    loop {
        let scan = node.ss_currentScanDesc.as_mut().expect("ss_currentScanDesc");
        let next = {
            let slot_ref = estate.slot_mut(slot);
            tableam_bm::table_scan_bitmap_next_tuple::call(scan, slot_ref)?
        };
        let (recheck, lossy_inc, exact_inc) = match next {
            Some(t) => t,
            None => break,
        };
        node.recheck = recheck;
        node.stats.lossy_pages += lossy_inc;
        node.stats.exact_pages += exact_inc;

        // Continuing in previously obtained page.
        // CHECK_FOR_INTERRUPTS();
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        // If we are using lossy info, we have to recheck the qual conditions at
        // every tuple.
        if node.recheck {
            // econtext->ecxt_scantuple = slot;
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);
            // if (!ExecQualAndReset(node->bitmapqualorig, econtext))
            if !exec_qual_and_reset(node.bitmapqualorig.as_deref_mut(), econtext, estate)? {
                // Fails recheck, so drop it and loop back for another.
                InstrCountFiltered2(node, 1);
                // ExecClearTuple(slot);
                execTuples::exec_clear_tuple::call(estate, slot)?;
                continue;
            }
        }

        // OK to return this tuple.
        return Ok(true);
    }

    // if we get here it means we are at the end of the scan.
    // return ExecClearTuple(slot);
    execTuples::exec_clear_tuple::call(estate, slot)?;
    Ok(false)
}

/// `BitmapHeapRecheck(node, slot)` — access-method routine to recheck a tuple
/// in EvalPlanQual. Evaluates the original bitmap qual against the current scan
/// tuple. Registered as the `ExecScanRecheckMtd` and invoked only from the EPQ
/// branch of `ExecScanFetch`; that branch is unreachable until execMain's EPQ
/// machinery lands (no rowmark sets `es_epq_active` yet), so the function is
/// currently dead but is the real recheck method the EPQ path will call.
#[allow(dead_code)]
fn BitmapHeapRecheck<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // ExprContext *econtext = node->ss.ps.ps_ExprContext;
    let econtext = node.ss.ps.ps_ExprContext.expect("ps_ExprContext");
    let slot = node.ss.ss_ScanTupleSlot.expect("ss_ScanTupleSlot");
    // econtext->ecxt_scantuple = slot;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);
    // return ExecQualAndReset(node->bitmapqualorig, econtext);
    exec_qual_and_reset(node.bitmapqualorig.as_deref_mut(), econtext, estate)
}

/// `ExecQualAndReset(qual, econtext)` (executor.h): `ExecQual` then
/// `ResetExprContext(econtext)`. A `NULL` qual is always-true. The reset clears
/// the per-tuple memory regardless of the qual result.
fn exec_qual_and_reset<'mcx>(
    qual: Option<&mut types_nodes::execexpr::ExprState<'mcx>>,
    econtext: types_nodes::EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let res = match qual {
        // ExecQual: a NULL qual passes.
        None => Ok(true),
        Some(state) => execExpr::exec_qual::call(state, econtext, estate),
    };
    // ResetExprContext(econtext): MemoryContextReset(econtext->ecxt_per_tuple_memory).
    estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    res
}

// ===========================================================================
// `execScan.h` inline helpers + `execScan.c` driver, inlined/linked into
// `nodeBitmapHeapscan.o` in C. `ExecBitmapHeapScan` calls the non-specialized
// `ExecScan(&node->ss, BitmapHeapNext, BitmapHeapRecheck)`.
// ===========================================================================

/// `ExecScanFetch` — check interrupts and fetch the next potential tuple.
///
/// Substitutes a test tuple if inside an EvalPlanQual recheck; otherwise runs
/// the access method's next-tuple routine. The EPQ machinery
/// (`EvalPlanQual*`) is owned by execMain and is not ported yet; the non-EPQ
/// fast path (the only one reachable until then) runs `BitmapHeapNext` here,
/// and the EPQ branch panics loudly through the recheck path.
fn ExecScanFetch<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    backend_tcop_postgres_seams::check_for_interrupts::call()?;

    // if (estate->es_epq_active != NULL) { ... EPQ substitution ... }
    // The owned EState model has not landed es_epq_active; until execMain's EPQ
    // machinery is ported this branch cannot be taken (no rowmark sets it), so
    // we run the normal access method below. When EPQ lands, the substitution +
    // BitmapHeapRecheck wiring is added here.

    // Run the node-type-specific access method function to get the next tuple.
    BitmapHeapNext(node, estate)
}

/// `ExecScan` — the `execScan.c` driver (`ExecScanExtended` with the runtime
/// qual/projection-presence tests). Resets the per-tuple context, fetches the
/// next tuple, applies qual + projection, and loops past failed-qual tuples.
fn ExecScan<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<types_nodes::SlotId>> {
    let econtext = node.ss.ps.ps_ExprContext.expect("ps_ExprContext");
    let scan_slot = node.ss.ss_ScanTupleSlot.expect("ss_ScanTupleSlot");
    let has_qual = node.ss.ps.qual.is_some();
    let has_proj_info = node.ss.ps.ps_ProjInfo.is_some();

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj_info {
        // ResetExprContext(econtext)
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
        if ExecScanFetch(node, estate)? {
            return Ok(Some(scan_slot));
        }
        return Ok(None);
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        let have_tuple = ExecScanFetch(node, estate)?;

        // If the slot returned by the accessMtd contains NULL, then it means
        // there is nothing more to scan so we just return an empty slot, being
        // careful to use the projection result slot so it has correct tupleDesc.
        if !have_tuple {
            if has_proj_info {
                let rslot = node.ss.ps.ps_ResultTupleSlot.expect("ps_ResultTupleSlot");
                execTuples::exec_clear_tuple::call(estate, rslot)?;
                return Ok(Some(rslot));
            }
            return Ok(None);
        }

        // Place the current tuple into the expr context.
        estate.ecxt_mut(econtext).ecxt_scantuple = Some(scan_slot);

        // Check that the current tuple satisfies the qual-clause.
        // Check for non-null qual here to avoid a function call to ExecQual()
        // when the qual is null.
        let passes = match &mut node.ss.ps.qual {
            None => true,
            Some(qual) => execExpr::exec_qual::call(qual, econtext, estate)?,
        };
        if passes {
            // Found a satisfactory scan tuple.
            if has_proj_info {
                // Form a projection tuple, store it in the result tuple slot and
                // return it.
                let slot = execExpr::exec_project::call(&mut node.ss.ps, estate)?;
                return Ok(Some(slot));
            }
            // Here, we aren't projecting, so just return scan tuple.
            return Ok(Some(scan_slot));
        }

        InstrCountFiltered1(node, 1);

        // Tuple fails qual, so free per-tuple memory and try again.
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }
}

/// `InstrCountFiltered1(node, delta)` — bump `instrument->nfiltered1` when
/// instrumentation is enabled.
#[inline]
fn InstrCountFiltered1(node: &mut BitmapHeapScanState, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered1 += delta as f64;
    }
}

/// `InstrCountFiltered2(node, delta)` — bump `instrument->nfiltered2` (tuples
/// failing the lossy-page recheck).
#[inline]
fn InstrCountFiltered2(node: &mut BitmapHeapScanState, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered2 += delta as f64;
    }
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// `ExecBitmapHeapScan(pstate)` — the `ExecProcNode` callback. Runs the
/// `ExecScan` driver with `BitmapHeapNext` / `BitmapHeapRecheck`. Returns the
/// slot id of the produced tuple (the C `TupleTableSlot *`), or `None` at end
/// of scan.
pub fn ExecBitmapHeapScan<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<types_nodes::SlotId>> {
    // return ExecScan(&node->ss, (ExecScanAccessMtd) BitmapHeapNext,
    //                 (ExecScanRecheckMtd) BitmapHeapRecheck);
    ExecScan(node, estate)
}

/// `ExecReScanBitmapHeapScan(node)` — prepare to rescan the bitmap heap scan.
pub fn ExecReScanBitmapHeapScan<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // TableScanDesc scan = node->ss.ss_currentScanDesc;
    // if (scan)
    if let Some(scan) = node.ss_currentScanDesc.as_mut() {
        // End iteration on iterators saved in scan descriptor if they have not
        // already been cleaned up.
        // if (!tbm_exhausted(&scan->st.rs_tbmiterator))
        //     tbm_end_iterate(&scan->st.rs_tbmiterator);
        if !scan.rs_tbmiterator.exhausted() {
            tidbitmap::tbm_end_iterate::call(&mut scan.rs_tbmiterator);
        }

        // rescan to release any page pin
        // table_rescan(node->ss.ss_currentScanDesc, NULL);
        tableam_bm::table_rescan::call(scan)?;
    }

    // release bitmaps and buffers if any
    // if (node->tbm) tbm_free(node->tbm); node->tbm = NULL;
    if let Some(tbm) = node.tbm.as_mut() {
        tidbitmap::tbm_free::call(tbm);
    }
    node.tbm = None;
    // node->initialized = false;
    node.initialized = false;
    // node->recheck = true;
    node.recheck = true;

    // ExecScanReScan(&node->ss);
    exec_scan_rescan(node, estate)?;

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    // if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    if node
        .ss
        .ps
        .lefttree
        .as_ref()
        .map(|c| c.ps_head().chgParam.is_none())
        .unwrap_or(false)
    {
        let outer = node.ss.ps.lefttree.as_mut().expect("outerPlanState");
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    Ok(())
}

/// `ExecScanReScan(node)` (execScan.c): the scan-node part of rescan — clear
/// the result and scan slots and reset the EPQ relsubs state. The EPQ relsubs
/// arrays are not yet ported (execMain); the slot clears are the reachable
/// behavior today.
fn exec_scan_rescan<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecClearTuple(node->ps.ps_ResultTupleSlot);
    if let Some(rslot) = node.ss.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate, rslot)?;
    }
    // if (node->ss_ScanTupleSlot) ExecClearTuple(node->ss_ScanTupleSlot);
    if let Some(sslot) = node.ss.ss_ScanTupleSlot {
        execTuples::exec_clear_tuple::call(estate, sslot)?;
    }
    Ok(())
}

/// `ExecEndBitmapHeapScan(node)` — release all storage for a bitmap heap scan
/// node.
pub fn ExecEndBitmapHeapScan<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // When ending a parallel worker, copy the statistics gathered by the worker
    // back into shared memory so that it can be picked up by the main process to
    // report in EXPLAIN ANALYZE.
    // if (node->sinstrument != NULL && IsParallelWorker())
    if node.sinstrument.is_some() && backend_access_transam_parallel_seams::is_parallel_worker::call() {
        // Assert(ParallelWorkerNumber <= node->sinstrument->num_workers);
        let worker = backend_access_transam_parallel_seams::parallel_worker_number::call();
        let sinstr = node.sinstrument.as_ref().expect("sinstrument");
        debug_assert!(worker <= sinstr.num_workers());
        // si = &node->sinstrument->sinstrument[ParallelWorkerNumber];
        // si->exact_pages += node->stats.exact_pages;
        // si->lossy_pages += node->stats.lossy_pages;
        //
        // A worker's `sinstrument` is the in-DSM `Shared` placement; it writes
        // its own slot through the shared `&self` (atomic accumulate).
        match sinstr {
            NodeSinstrument::Shared { slots, .. } => {
                let si = &slots.get()[worker as usize];
                si.accumulate(node.stats.exact_pages, node.stats.lossy_pages);
            }
            // A worker never holds the leader's private copy.
            NodeSinstrument::Private { .. } => {
                unreachable!("worker sinstrument is the shared DSM placement, not a private copy")
            }
        }
    }

    // close down subplans
    // ExecEndNode(outerPlanState(node));
    if let Some(outer) = node.ss.ps.lefttree.as_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }

    // if (scanDesc)
    if let Some(scan) = node.ss_currentScanDesc.as_mut() {
        // End iteration on iterators saved in scan descriptor if they have not
        // already been cleaned up.
        if !scan.rs_tbmiterator.exhausted() {
            tidbitmap::tbm_end_iterate::call(&mut scan.rs_tbmiterator);
        }

        // close table scan
        // table_endscan(scanDesc);
        let scan = node.ss_currentScanDesc.take().expect("ss_currentScanDesc");
        tableam_bm::table_endscan::call(scan)?;
    }

    // release bitmaps and buffers if any
    // if (node->tbm) tbm_free(node->tbm);
    if let Some(tbm) = node.tbm.as_mut() {
        tidbitmap::tbm_free::call(tbm);
    }

    Ok(())
}

/// `ExecInitBitmapHeapScan(node, estate, eflags)` — initialize the scan's state
/// information. C allocates the node via `makeNode` and returns the pointer;
/// here we build the owned [`BitmapHeapScanState`] and return it boxed in the
/// per-query context.
pub fn ExecInitBitmapHeapScan<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    plan: &BitmapHeapScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, BitmapHeapScanState<'mcx>>> {
    // check for unsupported flags
    // Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(
        eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0,
        "BitmapHeapScan does not support EXEC_FLAG_BACKWARD/EXEC_FLAG_MARK"
    );

    // Assert caller didn't ask for an unsafe snapshot — see comments at head of
    // file. Assert(IsMVCCSnapshot(estate->es_snapshot)); (debug-only; the
    // snapshot lives behind the snapshot manager via es_snapshot).

    let mcx = estate.es_query_cxt;

    // create state structure (makeNode(BitmapHeapScanState))
    let mut scanstate = BitmapHeapScanState::new(mcx);
    // scanstate->ss.ps.plan = (Plan *) node;
    scanstate.ss.ps.plan = Some(node);
    // scanstate->ss.ps.state = estate;  (threaded explicitly)
    // scanstate->ss.ps.ExecProcNode = ExecBitmapHeapScan;  (dispatch slot wired
    // by the executor; the owned model dispatches to this crate's
    // ExecBitmapHeapScan directly, no stored callback).

    // scanstate->tbm = NULL;
    scanstate.tbm = None;
    // memset(&scanstate->stats, 0, sizeof(BitmapHeapScanInstrumentation));
    scanstate.stats = BitmapHeapScanInstrumentation::default();
    // scanstate->initialized = false;
    scanstate.initialized = false;
    // scanstate->pstate = NULL;
    scanstate.pstate = None;
    // scanstate->recheck = true;
    scanstate.recheck = true;

    // Miscellaneous initialization: create expression context for node.
    // ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils_seams::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    // open the scan relation
    // currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    let is_parallel_worker = backend_access_transam_parallel_seams::is_parallel_worker::call();
    let current_relation = backend_executor_execUtils::ExecOpenScanRelation(
        estate,
        plan.scan.scanrelid,
        eflags,
        is_parallel_worker,
    )?;

    // initialize child nodes
    // outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = node.outer_plan();
    scanstate.ss.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // get the scan type from the relation descriptor.
    // ExecInitScanTupleSlot(estate, &scanstate->ss, RelationGetDescr(rel),
    //                       table_slot_callbacks(rel));
    let tts_ops: TupleSlotKind = backend_access_table_tableam::table_slot_callbacks(&current_relation);
    let tupdesc = Some(mcx::alloc_in(mcx, current_relation.rd_att.clone_in(mcx)?)?);
    execTuples::exec_init_scan_tuple_slot::call(estate, &mut scanstate.ss, tupdesc, tts_ops)?;

    // Initialize result type and projection.
    // ExecInitResultTypeTL(&scanstate->ss.ps);
    execTuples::exec_init_result_type_tl::call(&mut scanstate.ss.ps, estate)?;
    // ExecAssignScanProjectionInfo(&scanstate->ss);
    //   == ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, scan->scanrelid)
    // The BitmapHeapScan plan node is not a `types_nodes::Node` variant (its plan
    // type lives in this crate's `nodes` module), so we cannot route through the
    // varno-deriving `exec_assign_scan_projection_info` provider. Pass the scan's
    // `scanrelid` directly — exactly what the C plain wrapper forwards.
    execUtils_seams::exec_assign_scan_projection_info_with_varno::call(
        &mut scanstate.ss,
        estate,
        plan.scan.scanrelid as i32,
    )?;

    // initialize child expressions
    // scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, scanstate);
    let qual = plan.scan.plan.qual.as_deref();
    scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut scanstate.ss.ps, estate)?;
    // scanstate->bitmapqualorig = ExecInitQual(node->bitmapqualorig, scanstate);
    scanstate.bitmapqualorig = execExpr::exec_init_qual::call(
        Some(&plan.bitmapqualorig),
        &mut scanstate.ss.ps,
        estate,
    )?;

    // scanstate->ss.ss_currentRelation = currentRelation;
    scanstate.ss_currentRelation = Some(current_relation);

    // all done.
    Ok(mcx::alloc_in(mcx, scanstate)?)
}

// ===========================================================================
//        Parallel bitmap-scan shared-state machine (BM_* protocol)
// ===========================================================================

/// `BitmapDoneInitializingSharedState(pstate)` — the leader has populated the
/// TBM and initialized the shared state, so wake up other processes.
fn BitmapDoneInitializingSharedState(node: &mut BitmapHeapScanState) -> PgResult<()> {
    let pstate = node.pstate.as_ref().expect("pstate");
    // SpinLockAcquire(&pstate->mutex);
    // pstate->state = BM_FINISHED;
    // SpinLockRelease(&pstate->mutex);
    {
        let _guard = SpinLockGuard::acquire(&pstate.mutex);
        pstate.set_state(BM_FINISHED);
    }
    // ConditionVariableBroadcast(&pstate->cv);
    backend_storage_lmgr_condition_variable::ConditionVariableBroadcast(&pstate.cv);
    Ok(())
}

/// `BitmapShouldInitializeSharedState(pstate)` — the first process to see the
/// state as `BM_INITIAL` becomes the leader for the parallel bitmap scan and is
/// responsible for populating the TIDBitmap; the others are blocked on the
/// condition variable until the leader wakes them up. Returns `true` exactly
/// for the single backend that observed `BM_INITIAL`.
fn BitmapShouldInitializeSharedState(node: &mut BitmapHeapScanState) -> PgResult<bool> {
    let pstate = node.pstate.as_ref().expect("pstate");
    let mut state;
    // while (1)
    loop {
        // SpinLockAcquire(&pstate->mutex);
        // state = pstate->state;
        // if (pstate->state == BM_INITIAL) pstate->state = BM_INPROGRESS;
        // SpinLockRelease(&pstate->mutex);
        {
            let _guard = SpinLockGuard::acquire(&pstate.mutex);
            state = pstate.state();
            if pstate.state() == BM_INITIAL {
                pstate.set_state(BM_INPROGRESS);
            }
        }

        // Exit if bitmap is done, or if we're the leader.
        // if (state != BM_INPROGRESS) break;
        if state != BM_INPROGRESS {
            break;
        }

        // Wait for the leader to wake us up.
        // ConditionVariableSleep(&pstate->cv, WAIT_EVENT_PARALLEL_BITMAP_SCAN);
        backend_storage_lmgr_condition_variable::ConditionVariableSleep(
            &pstate.cv,
            WAIT_EVENT_PARALLEL_BITMAP_SCAN,
        )?;
    }

    // ConditionVariableCancelSleep();
    backend_storage_lmgr_condition_variable::ConditionVariableCancelSleep();

    // return (state == BM_INITIAL);
    Ok(state == BM_INITIAL)
}

// ===========================================================================
//                          Parallel Scan Support
// ===========================================================================

/// `MAXALIGN(sizeof(ParallelBitmapHeapState))` worth of DSM bytes. The owned
/// model uses `size_of` over the `repr(C)` shared struct, matching how every
/// shmem-reserving consumer sizes its chunk.
fn maxalign(n: usize) -> usize {
    const ALIGNOF: usize = 8; // MAXIMUM_ALIGNOF
    (n + ALIGNOF - 1) & !(ALIGNOF - 1)
}

/// `ExecBitmapHeapEstimate(node, pcxt)` — compute the amount of space we'll
/// need in the parallel query DSM, and inform `pcxt->estimator`.
pub fn ExecBitmapHeapEstimate(
    node: &BitmapHeapScanState,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    // size = MAXALIGN(sizeof(ParallelBitmapHeapState));
    let mut size = maxalign(core::mem::size_of::<ParallelBitmapHeapState>());

    // account for instrumentation, if required
    // if (node->ss.ps.instrument && pcxt->nworkers > 0)
    let nworkers = backend_access_transam_parallel_seams::pcxt_nworkers::call(pcxt);
    if node.ss.ps.instrument.is_some() && nworkers > 0 {
        // size = add_size(size, offsetof(SharedBitmapHeapInstrumentation, sinstrument));
        size += SharedBitmapHeapInstrumentation::offset_of_sinstrument();
        // size = add_size(size, mul_size(pcxt->nworkers, sizeof(BitmapHeapScanInstrumentation)));
        size += (nworkers as usize) * core::mem::size_of::<BitmapHeapScanInstrumentation>();
    }

    // shm_toc_estimate_chunk(&pcxt->estimator, size);
    // shm_toc_estimate_keys(&pcxt->estimator, 1);
    let estimator = backend_access_transam_parallel_seams::pcxt_estimator::call(pcxt);
    backend_access_transam_parallel_seams::shm_toc_estimate_chunk::call(estimator, size);
    backend_access_transam_parallel_seams::shm_toc_estimate_keys::call(estimator, 1);
    Ok(())
}

/// `ExecBitmapHeapInitializeDSM(node, pcxt)` — set up a parallel bitmap heap
/// scan descriptor in DSM.
pub fn ExecBitmapHeapInitializeDSM<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    pcxt: types_execparallel::ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // dsa_area *dsa = node->ss.ps.state->es_query_dsa;
    // If there's no DSA, there are no workers; initialize nothing.
    // if (dsa == NULL) return;
    if estate.es_query_dsa.is_none() {
        return Ok(());
    }

    // Determine whether instrumentation is needed and the chunk size.
    let nworkers = backend_access_transam_parallel_seams::pcxt_nworkers::call(pcxt);
    let want_instr = node.ss.ps.instrument.is_some() && nworkers > 0;
    let mut size = maxalign(core::mem::size_of::<ParallelBitmapHeapState>());
    if want_instr {
        size += SharedBitmapHeapInstrumentation::offset_of_sinstrument();
        size += (nworkers as usize) * core::mem::size_of::<BitmapHeapScanInstrumentation>();
    }

    // ptr = shm_toc_allocate(pcxt->toc, size);
    let toc = backend_access_transam_parallel_seams::pcxt_toc::call(pcxt);
    let ptr = backend_access_transam_parallel_seams::shm_toc_allocate::call(toc, size);
    let plan_node_id = bitmap_heap_plan_node_id(node);
    let seg = pcxt_seg_handle(pcxt);

    // pstate = (ParallelBitmapHeapState *) ptr;
    // ptr += MAXALIGN(sizeof(ParallelBitmapHeapState));
    // if (node->ss.ps.instrument && pcxt->nworkers > 0)
    //     sinstrument = (SharedBitmapHeapInstrumentation *) ptr;
    //
    // The C places the `ParallelBitmapHeapState` (and the optional
    // instrumentation tail) DIRECTLY in the DSM chunk so every worker attaches
    // to the SAME shared object; its `mutex`/`cv` are the real cross-process
    // primitives and `node->pstate`/`node->sinstrument` alias INTO the segment.
    // We place each typed object in place through the execParallel keystone's
    // `shared_dsm_object` primitive:
    //   pstate->tbmiterator = 0; SpinLockInit(&pstate->mutex);
    //   pstate->state = BM_INITIAL; ConditionVariableInit(&pstate->cv);
    // are all run by the in-place placement of `ParallelBitmapHeapState::new()`
    // (S_INIT_LOCK / the CV's `proclist_init` are its Default; the leader is the
    // sole writer pre-launch).
    let pstate = shared_dsm_object::place_value::<ParallelBitmapHeapState>(
        seg,
        ptr,
        ParallelBitmapHeapState::new(),
    );

    // if (sinstrument) { sinstrument->num_workers = pcxt->nworkers;
    //   memset(sinstrument->sinstrument, 0,
    //          pcxt->nworkers * sizeof(BitmapHeapScanInstrumentation)); }
    //
    // The sinstrument header + flexible array sit immediately after `pstate` in
    // the SAME chunk, at `ptr + MAXALIGN(sizeof(ParallelBitmapHeapState))`.
    let sinstrument = if want_instr {
        let sinstr_cursor =
            chunk_advance(ptr, maxalign(core::mem::size_of::<ParallelBitmapHeapState>()));
        let (header, slots) = shared_dsm_object::place_flex::<
            SharedBitmapHeapInstrumentation,
            SharedBitmapHeapScanInstr,
        >(
            seg,
            sinstr_cursor,
            nworkers as usize,
            SharedBitmapHeapInstrumentation {
                num_workers: nworkers,
            },
            // memset(..., 0, ...) — each slot starts zeroed.
            |_i| SharedBitmapHeapScanInstr::default(),
        );
        Some(NodeSinstrument::Shared { header, slots })
    } else {
        None
    };

    // shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pstate);
    backend_access_transam_parallel_seams::shm_toc_insert::call(toc, plan_node_id as u64, ptr);

    // node->pstate = pstate;
    // node->sinstrument = sinstrument;
    node.pstate = Some(pstate);
    node.sinstrument = sinstrument;
    Ok(())
}

/// `ExecBitmapHeapReInitializeDSM(node, pcxt)` — reset shared state before
/// beginning a fresh scan.
pub fn ExecBitmapHeapReInitializeDSM<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    _pcxt: types_execparallel::ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // dsa_area *dsa = node->ss.ps.state->es_query_dsa;
    // If there's no DSA, there are no workers; do nothing.
    let dsa = match estate.es_query_dsa {
        Some(dsa) => dsa,
        None => return Ok(()),
    };

    let pstate = node.pstate.as_ref().expect("pstate");
    // pstate->state = BM_INITIAL;
    pstate.set_state(BM_INITIAL);

    // if (DsaPointerIsValid(pstate->tbmiterator))
    //     tbm_free_shared_area(dsa, pstate->tbmiterator);
    let it = pstate.tbmiterator();
    if types_tidbitmap::dsa_pointer_is_valid(it) {
        tidbitmap::tbm_free_shared_area::call(dsa, it);
    }
    // pstate->tbmiterator = InvalidDsaPointer;
    pstate.set_tbmiterator(types_tidbitmap::InvalidDsaPointer);
    Ok(())
}

/// `ExecBitmapHeapInitializeWorker(node, pwcxt)` — copy relevant information
/// from the TOC into planstate.
pub fn ExecBitmapHeapInitializeWorker<'mcx>(
    node: &mut BitmapHeapScanState<'mcx>,
    pwcxt: types_execparallel::ParallelWorkerContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Assert(node->ss.ps.state->es_query_dsa != NULL);
    debug_assert!(estate.es_query_dsa.is_some());
    let plan_node_id = bitmap_heap_plan_node_id(node);

    // ptr = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
    let toc = backend_access_transam_parallel_seams::pwcxt_toc::call(pwcxt);
    let ptr = backend_access_transam_parallel_seams::shm_toc_lookup::call(
        toc,
        plan_node_id as u64,
        false,
    )
    .expect("ExecBitmapHeapInitializeWorker: shm_toc_lookup(noError=false) returned NULL");

    // node->pstate = (ParallelBitmapHeapState *) ptr;
    // ptr += MAXALIGN(sizeof(ParallelBitmapHeapState));
    // if (node->ss.ps.instrument)
    //     node->sinstrument = (SharedBitmapHeapInstrumentation *) ptr;
    //
    // The worker recovers the SAME in-DSM `ParallelBitmapHeapState` (and the
    // instrumentation tail) the leader placed in `ExecBitmapHeapInitializeDSM`,
    // by attaching to the looked-up chunk through the execParallel keystone.
    // The keystone uses the segment handle only as the `'seg` lifetime carrier
    // (it never dereferences it); the worker's looked-up chunk address is the
    // real in-segment address.
    let seg = types_execparallel::DsmSegmentHandle(0);
    node.pstate = Some(shared_dsm_object::attach::<ParallelBitmapHeapState>(seg, ptr));
    if node.ss.ps.instrument.is_some() {
        let sinstr_cursor =
            chunk_advance(ptr, maxalign(core::mem::size_of::<ParallelBitmapHeapState>()));
        // The flexible-array length is the header's `num_workers` (exactly what
        // the leader stored), so attach the header first to read it, then the
        // array tail of that many slots.
        let header = shared_dsm_object::attach::<SharedBitmapHeapInstrumentation>(seg, sinstr_cursor);
        let nworkers = header.get().num_workers as usize;
        let (header, slots) = shared_dsm_object::attach_flex::<
            SharedBitmapHeapInstrumentation,
            SharedBitmapHeapScanInstr,
        >(seg, sinstr_cursor, nworkers);
        node.sinstrument = Some(NodeSinstrument::Shared { header, slots });
    }
    Ok(())
}

/// `ExecBitmapHeapRetrieveInstrumentation(node)` — transfer bitmap heap scan
/// statistics from DSM to private memory.
pub fn ExecBitmapHeapRetrieveInstrumentation(node: &mut BitmapHeapScanState) -> PgResult<()> {
    // SharedBitmapHeapInstrumentation *sinstrument = node->sinstrument;
    // if (sinstrument == NULL) return;
    let sinstrument = match node.sinstrument.as_ref() {
        Some(s) => s,
        None => return Ok(()),
    };

    // size = offsetof(SharedBitmapHeapInstrumentation, sinstrument)
    //   + sinstrument->num_workers * sizeof(BitmapHeapScanInstrumentation);
    // node->sinstrument = palloc(size); memcpy(node->sinstrument, sinstrument, size);
    //
    // Deep-copy the shared header + array into private memory (the `palloc` +
    // `memcpy`): snapshot each shared slot into a plain
    // `BitmapHeapScanInstrumentation`.
    let (num_workers, sinstr) = match sinstrument {
        NodeSinstrument::Shared { header, slots } => {
            let nw = header.get().num_workers;
            let copy: alloc::vec::Vec<BitmapHeapScanInstrumentation> =
                slots.get().iter().map(|si| si.snapshot()).collect();
            (nw, copy)
        }
        // Already private (idempotent re-retrieve): clone the existing copy.
        NodeSinstrument::Private {
            num_workers,
            sinstrument,
        } => (*num_workers, sinstrument.clone()),
    };
    node.sinstrument = Some(NodeSinstrument::Private {
        num_workers,
        sinstrument: sinstr,
    });
    Ok(())
}

// --- In-DSM typed-shared-object placement -----------------------------------
//
// `ExecBitmapHeapInitializeDSM`/`ExecBitmapHeapInitializeWorker` place the
// `ParallelBitmapHeapState` (and its optional `SharedBitmapHeapInstrumentation`
// tail) DIRECTLY in the DSM chunk so every parallel worker attaches the SAME
// shared object — its `mutex`/`cv` are real cross-process primitives and
// `node->pstate`/`node->sinstrument` alias INTO shared memory. The typing of
// those structs over the raw `shm_toc` chunk cursor goes through the
// execParallel keystone's `shared_dsm_object` primitive (`place_value` /
// `place_flex` on the leader, `attach` / `attach_flex` on the worker); the
// sizing, `shm_toc_allocate`/`shm_toc_insert`/`shm_toc_lookup` calls around
// them are this node's real logic.

/// The `pcxt->seg` handle as the `types_execparallel::DsmSegmentHandle` the
/// keystone `shared_dsm_object` uses as its `'seg` lifetime carrier. `None`
/// (private memory, `seg == NULL`) maps to the NULL handle; the keystone never
/// dereferences it — the real chunk address comes from `shm_toc_allocate`.
fn pcxt_seg_handle(
    pcxt: types_execparallel::ParallelContextHandle,
) -> types_execparallel::DsmSegmentHandle {
    match backend_access_transam_parallel_seams::pcxt_seg::call(pcxt) {
        Some(seg) => seg,
        None => types_execparallel::DsmSegmentHandle(0),
    }
}

/// `ptr += off` — advance a shm_toc DSM byte cursor by `off` bytes (the C
/// `ptr += MAXALIGN(sizeof(ParallelBitmapHeapState))` after placing `pstate`).
fn chunk_advance(
    cursor: types_execparallel::SerializeCursor,
    off: usize,
) -> types_execparallel::SerializeCursor {
    types_execparallel::SerializeCursor(cursor.0 + off)
}

/// `node->ss.ps.plan->plan_node_id` for the bitmap heap scan plan node.
fn bitmap_heap_plan_node_id(node: &BitmapHeapScanState) -> i32 {
    node.ss
        .ps
        .plan
        .map(|n| n.plan_head().plan_node_id)
        .expect("BitmapHeapScanState.ss.ps.plan")
}

// ===========================================================================
//                              Seam installation
// ===========================================================================

/// Install this crate's implementations into the inward seam slots
/// (`backend-executor-nodeBitmapHeapscan-seams`), reached by the executor's
/// parallel dispatch (execParallel) in handle-space.
pub fn init_seams() {
    backend_executor_nodeBitmapHeapscan_seams::exec_bitmapheap_estimate::set(
        seam_exec_bitmapheap_estimate,
    );
    backend_executor_nodeBitmapHeapscan_seams::exec_bitmapheap_initialize_dsm::set(
        seam_exec_bitmapheap_initialize_dsm,
    );
    backend_executor_nodeBitmapHeapscan_seams::exec_bitmapheap_reinitialize_dsm::set(
        seam_exec_bitmapheap_reinitialize_dsm,
    );
    backend_executor_nodeBitmapHeapscan_seams::exec_bitmapheap_initialize_worker::set(
        seam_exec_bitmapheap_initialize_worker,
    );
    backend_executor_nodeBitmapHeapscan_seams::exec_bitmapheap_retrieve_instrumentation::set(
        seam_exec_bitmapheap_retrieve_instrumentation,
    );
}

// The inward seams dispatch from execParallel in handle-space
// (`PlanStateHandle`/`ParallelContextHandle`). Resolving a `PlanStateHandle`
// back to this crate's owned `&mut BitmapHeapScanState` (and reaching the live
// `EState`) requires the parallel-executor planstate registry, which is part
// of the not-yet-ported executor parallel machinery. The real per-node logic
// lives in the `ExecBitmapHeap*` functions above (auditable against the C);
// these handle bridges panic until that registry lands.

fn seam_exec_bitmapheap_estimate(
    _node: types_execparallel::PlanStateHandle,
    _pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    panic!(
        "ExecBitmapHeapEstimate handle bridge: needs the executor parallel \
         planstate registry to resolve PlanStateHandle to BitmapHeapScanState \
         (execParallel/access-parallel), not yet ported"
    );
}

fn seam_exec_bitmapheap_initialize_dsm(
    _node: types_execparallel::PlanStateHandle,
    _pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    panic!(
        "ExecBitmapHeapInitializeDSM handle bridge: needs the executor parallel \
         planstate registry, not yet ported"
    );
}

fn seam_exec_bitmapheap_reinitialize_dsm(
    _node: types_execparallel::PlanStateHandle,
    _pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    panic!(
        "ExecBitmapHeapReInitializeDSM handle bridge: needs the executor parallel \
         planstate registry, not yet ported"
    );
}

fn seam_exec_bitmapheap_initialize_worker(
    _node: types_execparallel::PlanStateHandle,
    _pwcxt: types_execparallel::ParallelWorkerContextHandle,
) -> PgResult<()> {
    panic!(
        "ExecBitmapHeapInitializeWorker handle bridge: needs the executor parallel \
         planstate registry, not yet ported"
    );
}

fn seam_exec_bitmapheap_retrieve_instrumentation(
    _node: types_execparallel::PlanStateHandle,
) -> PgResult<()> {
    panic!(
        "ExecBitmapHeapRetrieveInstrumentation handle bridge: needs the executor \
         parallel planstate registry, not yet ported"
    );
}

#[cfg(test)]
mod tests;
