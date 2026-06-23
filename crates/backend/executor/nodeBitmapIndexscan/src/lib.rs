//! Port of `src/backend/executor/nodeBitmapIndexscan.c` — routines to support
//! bitmapped index scans of relations.
//!
//! INTERFACE ROUTINES
//! - [`MultiExecBitmapIndexScan`]  - scans a relation using an index into a
//!   `TIDBitmap`.
//! - [`ExecInitBitmapIndexScan`]   - creates and initializes state info.
//! - [`ExecReScanBitmapIndexScan`] - prepares to rescan the plan.
//! - [`ExecEndBitmapIndexScan`]    - releases all storage.
//! - the four `ExecBitmapIndexScan*` parallel entry points.
//!
//! A `BitmapIndexScan` node scans an index, ORs the matching TIDs into a
//! `TIDBitmap`, and hands that bitmap up to its parent (`BitmapHeapScan` /
//! `BitmapAnd` / `BitmapOr`). The node never participates in the ordinary
//! `ExecProcNode` (one-tuple-at-a-time) call convention; its only execute entry
//! point is [`MultiExecBitmapIndexScan`], dispatched by the execProcnode
//! `MultiExecProcNode` switch.
//!
//! The node's own control flow (the scan loop, runtime/array-key setup, the
//! init/rescan/teardown/parallel logic) is this crate's owned logic. Operations
//! below the executor-node layer go through their owners' seam crates: the
//! generic index AM (indexam: `index_open`/`index_close`/`index_beginscan_bitmap`/
//! `index_getbitmap`/`index_endscan`/`index_rescan_bis`), the TID-bitmap
//! constructor (tidbitmap: `tbm_create`), the shared index-scan-key builders
//! (nodeIndexscan: `ExecIndexBuildScanKeys`/`ExecIndexEvalRuntimeKeys`/
//! `ExecIndexEvalArrayKeys`/`ExecIndexAdvanceArrayKeys`), the expression-context
//! helpers (execUtils), instrumentation (instrument), interrupts/work_mem
//! (postgres/globals), and the DSM/parallel-shm plumbing (shm_toc/parallel).

#![allow(non_snake_case)]

use indexam_seams as indexam;
use transam_parallel as parallel;
use execUtils_seams as execUtils;
use instrument_seams as instrument;
use nodeIndexscan_seams as nodeIndexscan;
use core_tidbitmap_seams as tidbitmap;
use storage_shm_toc_seams as shm_toc;
use postgres_seams as tcop_postgres;
use init_small_seams as globals;

use mcx::{Mcx, PgBox};
use types_error::{PgError, PgResult};
use execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};
use nodes::nodebitmapindexscan::{BitmapIndexScan, BitmapIndexScanState};
use nodes::nodeindexonlyscan::{IndexScanInstrumentation, SharedIndexScanInstrumentation};
use nodes::EStateData;
use tidbitmap::TIDBitmap;

/// `EXEC_FLAG_EXPLAIN_ONLY` (executor/executor.h) — "EXPLAIN, no ANALYZE".
const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
/// `EXEC_FLAG_BACKWARD` (executor/executor.h) — need backward scan.
const EXEC_FLAG_BACKWARD: i32 = 0x0008;
/// `EXEC_FLAG_MARK` (executor/executor.h) — need mark/restore.
const EXEC_FLAG_MARK: i32 = 0x0010;

use types_storage::lock::NoLock;

/// `elog(ERROR, msg)` — plain internal error.
fn elog(message: &'static str) -> PgError {
    PgError::error(message)
}

// ===========================================================================
// MultiExecBitmapIndexScan — the node's only "execute" entry point.
// ===========================================================================

/// `MultiExecBitmapIndexScan(node)` — scan the index, ORing matching TIDs into a
/// `TIDBitmap`, and return it.
///
/// 1:1 with `Node *MultiExecBitmapIndexScan(BitmapIndexScanState *node)`. The C
/// returns the bitmap as a `Node *`; the owned surface returns the real
/// `PgBox<TIDBitmap>`. The C `biss_result` pre-made-bitmap handoff (used when a
/// parent `BitmapOr` hands the child a running bitmap to OR into) is honored
/// when present; otherwise a fresh bitmap is created via `tbm_create`.
pub fn MultiExecBitmapIndexScan<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, TIDBitmap>> {
    // Prepare the result bitmap. Normally we create a new one; however our
    // parent node is allowed to store a pre-made one into node->biss_result, in
    // which case we just OR our tuple IDs into the existing bitmap.
    //   if (node->biss_result) { tbm = node->biss_result; node->biss_result = NULL; }
    //   else tbm = tbm_create(work_mem * (Size) 1024,
    //       ((BitmapIndexScan *) node->ss.ps.plan)->isshared ?
    //       node->ss.ps.state->es_query_dsa : NULL);
    //
    // Note the C prepares `tbm` only after instrumentation start + the runtime
    // key rescan; the bitmap create has no dependency on either, so hoisting it
    // ahead of `multi_exec_scan_into` (which holds `&mut tbm` for the scan loop)
    // is behaviour-preserving and lets the owned `PgBox` ownership flow cleanly.
    let mut tbm = match node.biss_result.take() {
        Some(tbm) => tbm,
        None => {
            // XXX should we use less than work_mem for this?
            let maxbytes = (globals::work_mem::call() as usize).wrapping_mul(1024);
            let isshared = plan_isshared(node)?;
            let dsa = if isshared {
                estate.es_query_dsa.clone()
            } else {
                None
            };
            // The C `tbm_create` palloc's the bitmap in CurrentMemoryContext,
            // which during MultiExec is the per-query context; the landed seam
            // boxes it into the supplied `mcx` and returns the `PgBox`.
            tidbitmap::tbm_create::call(estate.es_query_cxt, maxbytes, dsa)?
        }
    };

    // Run the actual scan, ORing matching TIDs into `tbm` in place.
    multi_exec_scan_into(node, &mut tbm, estate)?;

    // return (Node *) tbm;
    Ok(tbm)
}

/// Shared body of `MultiExecBitmapIndexScan`: do instrumentation start, the
/// runtime-key rescan, the index scan loop ORing matching TIDs into the
/// caller-supplied `tbm`, and instrumentation stop.
///
/// In the C this is inlined in `MultiExecBitmapIndexScan`; it is factored out so
/// the `BitmapOr`/`BitmapAnd` child path (which hands a *borrowed* running
/// bitmap to OR into via `biss_result`) can drive the same scan without taking
/// ownership of the bitmap.
fn multi_exec_scan_into<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    tbm: &mut TIDBitmap,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // double nTuples = 0;
    let mut n_tuples: f64 = 0.0;

    // must provide our own instrumentation support
    //   if (node->ss.ps.instrument) InstrStartNode(node->ss.ps.instrument);
    if let Some(instr) = node.ss.ps.instrument.as_deref_mut() {
        instrument::instr_start_node::call(instr)?;
    }

    // If we have runtime keys and they've not already been set up, do it now.
    // Array keys are also treated as runtime keys; note that if ExecReScan
    // returns with biss_RuntimeKeysReady still false, then there is an empty
    // array key so we should do nothing.
    //   if (!node->biss_RuntimeKeysReady &&
    //       (node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0))
    //   { ExecReScan((PlanState *) node); doscan = node->biss_RuntimeKeysReady; }
    //   else doscan = true;
    let mut doscan;
    if !node.biss_RuntimeKeysReady
        && (node.biss_NumRuntimeKeys != 0 || node.biss_NumArrayKeys != 0)
    {
        // The C `ExecReScan((PlanState *) node)` dispatches (via execAmi) to
        // ExecReScanBitmapIndexScan; the bitmap index scan has no chgParam /
        // exprcontext rescan side effects beyond the node-specific rescan, so
        // the owned model calls it directly.
        ExecReScanBitmapIndexScan(node, estate)?;
        doscan = node.biss_RuntimeKeysReady;
    } else {
        doscan = true;
    }

    // Get TIDs from index and insert into bitmap.
    //   while (doscan)
    //   {
    //       nTuples += (double) index_getbitmap(scandesc, tbm);
    //       CHECK_FOR_INTERRUPTS();
    //       doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys, node->biss_NumArrayKeys);
    //       if (doscan) index_rescan(node->biss_ScanDesc, node->biss_ScanKeys,
    //                                node->biss_NumScanKeys, NULL, 0);
    //   }
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    while doscan {
        {
            let scandesc = node
                .biss_ScanDesc
                .as_mut()
                .ok_or_else(|| elog("bitmap index scan has no scan descriptor"))?;
            n_tuples += indexam::index_getbitmap::call(mcx, scandesc, tbm)? as f64;
        }

        tcop_postgres::check_for_interrupts::call()?;

        doscan = nodeIndexscan::exec_index_advance_array_keys_bis::call(node, estate)?;
        if doscan {
            // reset index scan
            indexam::index_rescan_bis::call(mcx, node)?;
        }
    }

    // Mirror the AM-updated search counter into biss_Instrument (C aliases
    // scan->instrument to &biss_Instrument; the owned port passes it by value
    // into index_beginscan). EXPLAIN ANALYZE reads biss_Instrument before
    // ExecutorEnd via show_indexsearches_info.
    if let Some(scandesc) = node.biss_ScanDesc.as_ref() {
        if let Some(instr) = scandesc.instrument.as_ref() {
            node.biss_Instrument.nsearches = instr.nsearches;
        }
    }

    // must provide our own instrumentation support
    //   if (node->ss.ps.instrument) InstrStopNode(node->ss.ps.instrument, nTuples);
    if let Some(instr) = node.ss.ps.instrument.as_deref_mut() {
        instrument::instr_stop_node::call(instr, n_tuples)?;
    }

    Ok(())
}

// ===========================================================================
// ExecReScanBitmapIndexScan — recompute runtime keys and rescan.
// ===========================================================================

/// `ExecReScanBitmapIndexScan(node)` — recalculate the values of any scan keys
/// whose value depends on information known at runtime, then rescan the indexed
/// relation.
///
/// 1:1 with `void ExecReScanBitmapIndexScan(BitmapIndexScanState *node)`.
pub fn ExecReScanBitmapIndexScan<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExprContext *econtext = node->biss_RuntimeContext;
    let econtext = node.biss_RuntimeContext;

    // Reset the runtime-key context so we don't leak memory as each outer tuple
    // is scanned. Note this assumes that we will recalculate *all* runtime keys
    // on each call.
    //   if (econtext) ResetExprContext(econtext);
    if let Some(econtext) = econtext {
        execUtils::reset_expr_context::call(estate, econtext)?;
    }

    // If we are doing runtime key calculations (ie, any of the index key values
    // weren't simple Consts), compute the new key values.
    //
    // Array keys are also treated as runtime keys; note that if we return with
    // biss_RuntimeKeysReady still false, then there is an empty array key so no
    // index scan is needed.
    //   if (node->biss_NumRuntimeKeys != 0)
    //       ExecIndexEvalRuntimeKeys(econtext, node->biss_RuntimeKeys, node->biss_NumRuntimeKeys);
    if node.biss_NumRuntimeKeys != 0 {
        // econtext is non-NULL here: the C only sets biss_NumRuntimeKeys != 0
        // together with a freshly-assigned biss_RuntimeContext in ExecInit.
        let econtext =
            econtext.ok_or_else(|| elog("bitmap index scan runtime keys without runtime context"))?;
        nodeIndexscan::exec_index_eval_runtime_keys_bis::call(node, estate, econtext)?;
    }
    //   if (node->biss_NumArrayKeys != 0)
    //       node->biss_RuntimeKeysReady = ExecIndexEvalArrayKeys(...);
    //   else node->biss_RuntimeKeysReady = true;
    if node.biss_NumArrayKeys != 0 {
        let econtext =
            econtext.ok_or_else(|| elog("bitmap index scan array keys without runtime context"))?;
        node.biss_RuntimeKeysReady =
            nodeIndexscan::exec_index_eval_array_keys_bis::call(node, estate, econtext)?;
    } else {
        node.biss_RuntimeKeysReady = true;
    }

    // reset index scan
    //   if (node->biss_RuntimeKeysReady)
    //       index_rescan(node->biss_ScanDesc, node->biss_ScanKeys, node->biss_NumScanKeys, NULL, 0);
    if node.biss_RuntimeKeysReady {
        let mcx: Mcx<'mcx> = estate.es_query_cxt;
        indexam::index_rescan_bis::call(mcx, node)?;
    }

    Ok(())
}

// ===========================================================================
// ExecEndBitmapIndexScan — release all storage.
// ===========================================================================

/// `ExecEndBitmapIndexScan(node)` — release all storage.
///
/// 1:1 with `void ExecEndBitmapIndexScan(BitmapIndexScanState *node)`.
pub fn ExecEndBitmapIndexScan<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    // When ending a parallel worker, copy the statistics gathered by the worker
    // back into shared memory so the main process can report them in EXPLAIN
    // ANALYZE.
    //   if (node->biss_SharedInfo != NULL && IsParallelWorker())
    //   {
    //       Assert(ParallelWorkerNumber <= node->biss_SharedInfo->num_workers);
    //       winstrument = &node->biss_SharedInfo->winstrument[ParallelWorkerNumber];
    //       winstrument->nsearches += node->biss_Instrument.nsearches;
    //   }
    if node.biss_SharedInfo.is_some() && parallel::is_parallel_worker() {
        // We have to accumulate the stats rather than performing a memcpy: when a
        // Gather/GatherMerge node finishes it performs planner shutdown on the
        // workers; on rescan it spins up new workers with a new
        // BitmapIndexScanState and zeroed stats.
        let nsearches = node.biss_Instrument.nsearches;
        let shared = node.biss_SharedInfo.as_mut().unwrap();
        parallel::accumulate_shared_index_searches(shared, nsearches);
    }

    // close the index relation (no-op if we didn't open it)
    //   if (indexScanDesc) index_endscan(indexScanDesc);
    if let Some(scandesc) = node.biss_ScanDesc.take() {
        indexam::index_endscan::call(mcx, scandesc)?;
    }
    //   if (indexRelationDesc) index_close(indexRelationDesc, NoLock);
    if let Some(index_rel) = node.biss_RelationDesc.take() {
        index_rel.close(NoLock)?;
    }

    Ok(())
}

// ===========================================================================
// ExecInitBitmapIndexScan — build the node state.
// ===========================================================================

/// `ExecInitBitmapIndexScan(node, estate, eflags)` — initialize the index scan's
/// state information.
///
/// 1:1 with `BitmapIndexScanState *ExecInitBitmapIndexScan(BitmapIndexScan
/// *node, EState *estate, int eflags)`.
pub fn ExecInitBitmapIndexScan<'mcx>(
    node: &'mcx nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, BitmapIndexScanState<'mcx>>> {
    let mcx: Mcx<'mcx> = estate.es_query_cxt;

    // BitmapIndexScan *node — the enclosing plan-tree node (castNode).
    let bis: &'mcx BitmapIndexScan<'mcx> = node.expect_bitmapindexscan();

    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(
        eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0,
        "BitmapIndexScan does not support EXEC_FLAG_BACKWARD/EXEC_FLAG_MARK"
    );

    // create state structure (makeNode(BitmapIndexScanState))
    //   indexstate->ss.ps.plan = (Plan *) node;
    //   indexstate->ss.ps.state = estate;            (back-link, executor-owned)
    //   indexstate->ss.ps.ExecProcNode = ExecBitmapIndexScan;  (pro-forma stub)
    let mut indexstate = BitmapIndexScanState::make_boxed_in(mcx)?;
    indexstate.ss.ps.plan = Some(node);
    indexstate.ss.ps.ExecProcNode = Some(exec_proc_node_trampoline);

    // normally we don't make the result bitmap till runtime
    //   indexstate->biss_result = NULL;
    indexstate.biss_result = None;

    // We do not open or lock the base relation here. We assume that an ancestor
    // BitmapHeapScan node is holding AccessShareLock (or better) on the heap
    // relation throughout the execution of the plan tree.
    //   indexstate->ss.ss_currentRelation = NULL;
    //   indexstate->ss.ss_currentScanDesc = NULL;
    indexstate.ss.ss_currentRelation = None;
    indexstate.ss.ss_currentScanDesc = None;

    // Miscellaneous initialization: we do not need a standard exprcontext for
    // this node (we may decide below to create a runtime-key exprcontext).
    //
    // initialize child expressions: we don't need targetlist or qual, neither
    // are used. (We don't initialize all of the indexqual expression, only the
    // sub-parts corresponding to runtime keys, below.)

    // If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
    // here. This allows an index-advisor plugin to EXPLAIN a plan containing
    // references to nonexistent indexes.
    //   if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return indexstate;
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(indexstate);
    }

    // Open the index relation.
    //   lockmode = exec_rt_fetch(node->scan.scanrelid, estate)->rellockmode;
    //   indexstate->biss_RelationDesc = index_open(node->indexid, lockmode);
    let lockmode = execUtils::exec_rt_fetch_rellockmode::call(estate, bis.scan.scanrelid);
    let index_relation = indexam::index_open::call(mcx, bis.indexid, lockmode)?;
    indexstate.biss_RelationDesc = Some(index_relation);

    // Initialize index-specific scan state.
    //   indexstate->biss_RuntimeKeysReady = false;
    //   indexstate->biss_RuntimeKeys = NULL;
    //   indexstate->biss_NumRuntimeKeys = 0;
    indexstate.biss_RuntimeKeysReady = false;
    indexstate.biss_RuntimeKeys.clear();
    indexstate.biss_NumRuntimeKeys = 0;

    // build the index scan keys from the index qualification
    //   ExecIndexBuildScanKeys((PlanState *) indexstate, indexstate->biss_RelationDesc,
    //       node->indexqual, false, &indexstate->biss_ScanKeys, &indexstate->biss_NumScanKeys,
    //       &indexstate->biss_RuntimeKeys, &indexstate->biss_NumRuntimeKeys,
    //       &indexstate->biss_ArrayKeys, &indexstate->biss_NumArrayKeys);
    {
        let index = indexstate
            .biss_RelationDesc
            .as_ref()
            .map(|r| r.alias())
            .ok_or_else(|| elog("bitmap index scan has no index relation"))?;
        nodeIndexscan::exec_index_build_scan_keys_bis::call(
            &mut indexstate,
            estate,
            index,
            bis.indexqual.as_deref(),
        )?;
    }

    // If we have runtime keys or array keys, we need an ExprContext to evaluate
    // them. We could just create a "standard" plan node exprcontext, but to keep
    // the code looking similar to nodeIndexscan.c, it seems better to stick with
    // the approach of using a separate ExprContext.
    //   if (indexstate->biss_NumRuntimeKeys != 0 || indexstate->biss_NumArrayKeys != 0)
    //   {
    //       ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;
    //       ExecAssignExprContext(estate, &indexstate->ss.ps);
    //       indexstate->biss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
    //       indexstate->ss.ps.ps_ExprContext = stdecontext;
    //   }
    //   else indexstate->biss_RuntimeContext = NULL;
    if indexstate.biss_NumRuntimeKeys != 0 || indexstate.biss_NumArrayKeys != 0 {
        let stdecontext = indexstate.ss.ps.ps_ExprContext;
        execUtils::exec_assign_expr_context::call(estate, &mut indexstate.ss.ps)?;
        indexstate.biss_RuntimeContext = indexstate.ss.ps.ps_ExprContext;
        indexstate.ss.ps.ps_ExprContext = stdecontext;
    } else {
        indexstate.biss_RuntimeContext = None;
    }

    // Initialize scan descriptor.
    //   indexstate->biss_ScanDesc = index_beginscan_bitmap(indexstate->biss_RelationDesc,
    //       estate->es_snapshot, &indexstate->biss_Instrument, indexstate->biss_NumScanKeys);
    {
        let index = indexstate
            .biss_RelationDesc
            .as_ref()
            .map(|r| r.alias())
            .ok_or_else(|| elog("bitmap index scan has no index relation"))?;
        let num_scan_keys = indexstate.biss_NumScanKeys;
        let scandesc = indexam::index_beginscan_bitmap::call(
            mcx,
            index,
            estate.es_snapshot.clone(),
            indexstate.biss_Instrument,
            num_scan_keys,
        )?;
        indexstate.biss_ScanDesc = Some(scandesc);
    }

    // If no run-time keys to calculate, go ahead and pass the scankeys to the
    // index AM.
    //   if (indexstate->biss_NumRuntimeKeys == 0 && indexstate->biss_NumArrayKeys == 0)
    //       index_rescan(indexstate->biss_ScanDesc, indexstate->biss_ScanKeys,
    //                    indexstate->biss_NumScanKeys, NULL, 0);
    if indexstate.biss_NumRuntimeKeys == 0 && indexstate.biss_NumArrayKeys == 0 {
        indexam::index_rescan_bis::call(mcx, &mut indexstate)?;
    }

    // all done.
    Ok(indexstate)
}

// ===========================================================================
// Parallel Bitmap Index Scan Support (1:1).
//
// Parallel bitmap index scans are not themselves parallel-aware, but they still
// store their per-worker instrumentation in DSM during parallel query. Reached
// through the opaque-handle seams in `backend-executor-nodeBitmapIndexscan-seams`,
// which execParallel calls during parallel-query setup. The handle->node
// resolution is owned by execParallel; the node-level logic below operates on
// the real `BitmapIndexScanState`.
// ===========================================================================

/// `ExecBitmapIndexScanEstimate(node, pcxt)` — compute the amount of space the
/// parallel query DSM needs and inform `pcxt->estimator`.
///
/// 1:1 with `void ExecBitmapIndexScanEstimate(BitmapIndexScanState *node,
/// ParallelContext *pcxt)`.
pub fn ExecBitmapIndexScanEstimate<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // Parallel bitmap index scans are not supported, but we still need to store
    // the scan's instrumentation in DSM during parallel query.
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    let nworkers = shm_toc::pcxt_nworkers::call(pcxt);
    if node.ss.ps.instrument.is_none() || nworkers == 0 {
        return Ok(());
    }

    //   size = offsetof(SharedIndexScanInstrumentation, winstrument) +
    //          pcxt->nworkers * sizeof(IndexScanInstrumentation);
    //   shm_toc_estimate_chunk(&pcxt->estimator, size);
    //   shm_toc_estimate_keys(&pcxt->estimator, 1);
    let size = shared_info_size(nworkers);
    shm_toc::estimate_chunk_and_key::call(pcxt, size);

    Ok(())
}

/// `ExecBitmapIndexScanInitializeDSM(node, pcxt)` — set up bitmap index scan
/// shared instrumentation.
///
/// 1:1 with `void ExecBitmapIndexScanInitializeDSM(BitmapIndexScanState *node,
/// ParallelContext *pcxt)`.
pub fn ExecBitmapIndexScanInitializeDSM<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    let nworkers = shm_toc::pcxt_nworkers::call(pcxt);
    if node.ss.ps.instrument.is_none() || nworkers == 0 {
        return Ok(());
    }

    // The C: shm_toc_allocate a zeroed `size`-byte chunk, shm_toc_insert it under
    // the plan node id, memset(0), set num_workers. The owned model builds the
    // fully-zeroed, correctly-sized SharedIndexScanInstrumentation (num_workers
    // set, winstrument all-zero with nworkers slots); the DSM allocator returns
    // the shared-backed handle stored in biss_SharedInfo.
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let plan_node_id = plan_node_id(node)?;
    let descriptor = SharedIndexScanInstrumentation {
        num_workers: nworkers,
        winstrument: new_zeroed_winstrument(mcx, nworkers)?,
    };
    node.biss_SharedInfo = Some(shm_toc::toc_allocate_and_insert_bitmap_instr::call(
        mcx,
        pcxt,
        plan_node_id,
        descriptor,
    )?);

    Ok(())
}

/// `ExecBitmapIndexScanInitializeWorker(node, pwcxt)` — copy relevant
/// information from the TOC into planstate.
///
/// 1:1 with `void ExecBitmapIndexScanInitializeWorker(BitmapIndexScanState
/// *node, ParallelWorkerContext *pwcxt)`.
pub fn ExecBitmapIndexScanInitializeWorker<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // don't need this if not instrumenting
    //   if (!node->ss.ps.instrument) return;
    if node.ss.ps.instrument.is_none() {
        return Ok(());
    }

    // node->biss_SharedInfo = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let plan_node_id = plan_node_id(node)?;
    node.biss_SharedInfo = Some(shm_toc::toc_lookup_bitmap_instr::call(
        mcx,
        pwcxt,
        plan_node_id,
    )?);

    Ok(())
}

/// `ExecBitmapIndexScanRetrieveInstrumentation(node)` — transfer bitmap index
/// scan statistics from DSM to private memory.
///
/// 1:1 with `void ExecBitmapIndexScanRetrieveInstrumentation(
/// BitmapIndexScanState *node)`. The C makes a backend-local `palloc` + `memcpy`
/// copy of the DSM SharedInfo; the owned model is a deep clone of the
/// `SharedIndexScanInstrumentation` into a fresh `mcx` allocation (`palloc` is
/// the allocator behind it).
pub fn ExecBitmapIndexScanRetrieveInstrumentation<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    //   SharedIndexScanInstrumentation *SharedInfo = node->biss_SharedInfo;
    //   if (SharedInfo == NULL) return;
    let shared = match &node.biss_SharedInfo {
        None => return Ok(()),
        Some(s) => s,
    };

    // Create a copy of SharedInfo in backend-local memory.
    //   size = offsetof(...) + SharedInfo->num_workers * sizeof(IndexScanInstrumentation);
    //   node->biss_SharedInfo = palloc(size);
    //   memcpy(node->biss_SharedInfo, SharedInfo, size);
    //
    // The clone reproduces the `memcpy` of exactly `num_workers` slots.
    let copy = clone_shared_info(mcx, shared)?;
    node.biss_SharedInfo = Some(mcx::alloc_in(mcx, copy)?);

    Ok(())
}

// ===========================================================================
// Small helpers reading the node's own/plan state (no foreign owner).
// ===========================================================================

/// `((BitmapIndexScan *) node->ss.ps.plan)->isshared`.
fn plan_isshared(node: &BitmapIndexScanState<'_>) -> PgResult<bool> {
    match node.ss.ps.plan {
        Some(p) if p.node_tag() == nodes::nodes::ntag::T_BitmapIndexScan => Ok(p.expect_bitmapindexscan().isshared),
        _ => Err(elog("BitmapIndexScan node has wrong plan type")),
    }
}

/// `node->ss.ps.plan->plan_node_id` — the planner-assigned id used as the DSM
/// TOC key.
fn plan_node_id(node: &BitmapIndexScanState<'_>) -> PgResult<i32> {
    match node.ss.ps.plan {
        Some(n) => Ok(n.plan_head().plan_node_id),
        None => Err(elog("bitmap index scan has no plan")),
    }
}

/// `offsetof(SharedIndexScanInstrumentation, winstrument) + nworkers *
/// sizeof(IndexScanInstrumentation)` — the DSM chunk size for `nworkers`
/// per-worker slots. The bound is the parallel-context worker count, a small
/// validated integer, not data-derived growth.
#[inline]
fn shared_info_size(nworkers: i32) -> usize {
    // offsetof(SharedIndexScanInstrumentation, winstrument): the `num_workers`
    // header before the flexible-array member.
    let header = core::mem::size_of::<i32>();
    let slots = usize::try_from(nworkers.max(0)).unwrap_or(0);
    header + slots.saturating_mul(core::mem::size_of::<IndexScanInstrumentation>())
}

/// Build the `winstrument` flexible-array member of `nworkers` zeroed slots,
/// allocated in `mcx` (the C `shm_toc_allocate` + `memset(0)`). `nworkers` is the
/// bounded parallel-context worker count; the buffer is reserved fallibly (the
/// `try_reserve` rule) so OOM surfaces recoverably.
fn new_zeroed_winstrument<'mcx>(
    mcx: Mcx<'mcx>,
    nworkers: i32,
) -> PgResult<alloc::vec::Vec<IndexScanInstrumentation>> {
    let n = usize::try_from(nworkers.max(0)).unwrap_or(0);
    let mut v: alloc::vec::Vec<IndexScanInstrumentation> = alloc::vec::Vec::new();
    v.try_reserve_exact(n)
        .map_err(|_| mcx.oom(n * core::mem::size_of::<IndexScanInstrumentation>()))?;
    for _ in 0..n {
        v.push(IndexScanInstrumentation::default());
    }
    Ok(v)
}

/// Deep-clone `SharedInfo` into backend-local memory, copying exactly
/// `num_workers` per-worker slots (the C `memcpy size`). Any short-fall against
/// the present slots is zero-filled, matching a memcpy of a fully-populated
/// `num_workers`-slot region.
fn clone_shared_info<'mcx>(
    mcx: Mcx<'mcx>,
    shared: &SharedIndexScanInstrumentation,
) -> PgResult<SharedIndexScanInstrumentation> {
    let n = usize::try_from(shared.num_workers.max(0)).unwrap_or(0);
    let mut winstrument: alloc::vec::Vec<IndexScanInstrumentation> = alloc::vec::Vec::new();
    winstrument
        .try_reserve_exact(n)
        .map_err(|_| mcx.oom(n * core::mem::size_of::<IndexScanInstrumentation>()))?;
    for i in 0..n {
        winstrument.push(shared.winstrument.get(i).copied().unwrap_or_default());
    }
    Ok(SharedIndexScanInstrumentation {
        num_workers: shared.num_workers,
        winstrument,
    })
}

/// The `ExecProcNode` callback trampoline installed into `ps.ExecProcNode`.
///
/// `ExecBitmapIndexScan` (the C pro-forma stub) does
/// `elog(ERROR, "BitmapIndexScan node does not support ExecProcNode call
/// convention")` — a BitmapIndexScan is only ever driven through
/// `MultiExecBitmapIndexScan`, so an ordinary per-tuple dispatch is a bug.
fn exec_proc_node_trampoline<'mcx>(
    _pstate: &mut nodes::PlanStateNode<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<Option<nodes::SlotId>> {
    Err(elog(
        "BitmapIndexScan node does not support ExecProcNode call convention",
    ))
}

extern crate alloc;

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install this crate's parallel-executor methods into the
/// `backend-executor-nodeBitmapIndexscan-seams` slots. The seams are declared
/// over execParallel's opaque `PlanStateHandle`/`ParallelContextHandle`; the
/// handle->node resolution belongs to execParallel and is not yet wired, so the
/// installed bridges panic at that frontier (mirror-PG-and-panic). The node's
/// own parallel logic lives in the real `ExecBitmapIndexScan*` functions above.
///
/// `ExecBitmapIndexScanRetrieveInstrumentation` is reached by execParallel
/// through a different path (it takes only the node, not a handle pair) and is
/// not one of the four handle-pair seams; it is exposed as a public function
/// and called directly by its consumer.
/// Adapter installed into the `multi_exec_bitmap_index_child` seam.
///
/// The seam is declared over the generic `PlanStateNode` (the C
/// `(BitmapIndexScanState *) subnode` downcast lives in nodeBitmapOr.c /
/// nodeBitmapAnd.c) and a *borrowed* running `result` bitmap. It mirrors the C:
///
/// ```c
/// ((BitmapIndexScanState *) subnode)->biss_result = result;
/// subresult = (TIDBitmap *) MultiExecProcNode(subnode);
/// if (subresult != result) elog(ERROR, "unrecognized result from subplan");
/// ```
///
/// We downcast `subnode` to its `BitmapIndexScanState` and drive the scan body
/// directly into the caller's `result` bitmap (`multi_exec_scan_into`), which is
/// exactly the in-place OR the C achieves by stashing `result` in `biss_result`
/// and relying on `MultiExecBitmapIndexScan` returning that same bitmap (the
/// `subresult == result` identity check is therefore structurally guaranteed).
fn bridge_multi_exec_bitmap_index_child<'mcx>(
    subnode: &mut nodes::PlanStateNode<'mcx>,
    result: &mut TIDBitmap,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ((BitmapIndexScanState *) subnode) — the C unconditionally downcasts
    // because the caller already gated on `IsA(subnode, BitmapIndexScanState)`.
    let node = match subnode {
        nodes::PlanStateNode::BitmapIndexScan(node) => &mut **node,
        _ => {
            return Err(elog(
                "multi_exec_bitmap_index_child: subnode is not a BitmapIndexScanState",
            ))
        }
    };
    multi_exec_scan_into(node, result, estate)
}

pub fn init_seams() {
    nodeBitmapIndexscan_seams::multi_exec_bitmap_index_child::set(
        bridge_multi_exec_bitmap_index_child,
    );
    nodeBitmapIndexscan_seams::exec_bitmapindexscan_estimate::set(bridge_estimate);
    nodeBitmapIndexscan_seams::exec_bitmapindexscan_initialize_dsm::set(
        bridge_initialize_dsm,
    );
    nodeBitmapIndexscan_seams::exec_bitmapindexscan_initialize_worker::set(
        bridge_initialize_worker,
    );
    nodeBitmapIndexscan_seams::exec_bitmapindexscan_retrieve_instrumentation::set(
        bridge_retrieve_instrumentation,
    );
}

fn bridge_estimate(_node: PlanStateHandle, _pcxt: ParallelContextHandle) -> PgResult<()> {
    panic!(
        "exec_bitmapindexscan_estimate: PlanStateHandle->BitmapIndexScanState resolution is \
         owned by execParallel and not yet wired"
    )
}
fn bridge_initialize_dsm(_node: PlanStateHandle, _pcxt: ParallelContextHandle) -> PgResult<()> {
    panic!(
        "exec_bitmapindexscan_initialize_dsm: PlanStateHandle->BitmapIndexScanState resolution is \
         owned by execParallel and not yet wired"
    )
}
fn bridge_initialize_worker(
    _node: PlanStateHandle,
    _pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    panic!(
        "exec_bitmapindexscan_initialize_worker: PlanStateHandle->BitmapIndexScanState resolution \
         is owned by execParallel and not yet wired"
    )
}
fn bridge_retrieve_instrumentation(_node: PlanStateHandle) -> PgResult<()> {
    panic!(
        "exec_bitmapindexscan_retrieve_instrumentation: PlanStateHandle->BitmapIndexScanState \
         resolution is owned by execParallel and not yet wired"
    )
}
