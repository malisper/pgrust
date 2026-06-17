//! Port of `src/backend/executor/nodeIndexonlyscan.c` — routines to support
//! index-only scans.
//!
//! INTERFACE ROUTINES
//! - [`ExecIndexOnlyScan`]            - scans an index.
//! - [`IndexOnlyNext`]                - retrieve next tuple (the access method).
//! - [`ExecInitIndexOnlyScan`]        - create and initialize state info.
//! - [`ExecReScanIndexOnlyScan`]      - rescan the indexed relation.
//! - [`ExecEndIndexOnlyScan`]         - release all storage.
//! - [`ExecIndexOnlyMarkPos`] / [`ExecIndexOnlyRestrPos`] - mark/restore.
//! - the five `ExecIndexOnlyScan*` parallel entry points.
//!
//! The node's own state machine — the [`IndexOnlyNext`] scan loop (VM check,
//! heap-fetch fallback, lossy recheck, predicate-lock), [`StoreIndexTuple`]'s
//! slot fill and name-cstring fix-up decision, the EvalPlanQual-recheck stub
//! [`IndexOnlyRecheck`], and the init/rescan/teardown/parallel control flow —
//! is this crate's owned logic. The `execScan.c` driver
//! (`ExecScan`/`ExecScanExtended`/`ExecScanFetch`) is a separate translation
//! unit (`execScan.o`); this node delegates to it through the execScan seam,
//! passing its own `IndexOnlyNext`/`IndexOnlyRecheck` methods, so the
//! qual/projection/EPQ control flow lives in its owning unit. Operations below the
//! executor-node layer go through their owners' seam crates: the generic index
//! AM (genam/indexam), the visibility map, the buffer manager, predicate
//! locking, expression eval (execExpr), slots/tupdesc (execTuples), the
//! execUtils/execScan init helpers, the EvalPlanQual machinery (execMain), the
//! scan-key builders (nodeIndexscan), and the DSM/parallel-shm plumbing.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_access_common_indextuple_seams as indextuple;
use backend_access_heap_visibilitymap_seams as visibilitymap;
use backend_access_index_indexam_seams as indexam;
use backend_access_transam_parallel as parallel;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execScan_seams as execScan;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_executor_nodeIndexscan_seams as nodeIndexscan;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_ipc_shm_toc_seams as shm_toc;
use backend_storage_lmgr_predicate_seams as predicate;
use backend_tcop_postgres_seams as tcop_postgres;

use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};
use types_nodes::nodeindexonlyscan::{IndexOnlyScan, IndexOnlyScanState};
use types_nodes::{EStateData, InvalidBuffer, SlotId, TupleSlotKind};
use types_scan::sdir::ScanDirection;

/// `EXEC_FLAG_EXPLAIN_ONLY` (`executor/executor.h`) — "EXPLAIN, no ANALYZE".
pub const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;

/// `INDEX_VAR` (`nodes/primnodes.h`) — varno of Vars referencing the scan
/// tuple of an index-only scan's targetlist.
const INDEX_VAR: i32 = -3;

/// `elog(ERROR, msg)` — plain internal error.
fn elog(message: &'static str) -> PgError {
    PgError::error(message)
}

/// `ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), ...))`.
fn feature(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

// --- EvalPlanQual per-relation array reads (owned EPQState in es_epq_active) -
// C: `epqstate->relsubs_slot[idx] != NULL`.
#[inline]
fn epq_relsubs_slot_present(epqstate: &types_nodes::EPQState<'_>, idx: usize) -> bool {
    epqstate
        .relsubs_slot
        .as_ref()
        .and_then(|v| v.get(idx))
        .map(|s| s.is_some())
        .unwrap_or(false)
}

// C: `epqstate->relsubs_rowmark[idx] != NULL`.
#[inline]
fn epq_relsubs_rowmark_present(epqstate: &types_nodes::EPQState<'_>, idx: usize) -> bool {
    epqstate
        .relsubs_rowmark
        .as_ref()
        .and_then(|v| v.get(idx).copied())
        .unwrap_or(false)
}

// C: `epqstate->relsubs_done[idx]`.
#[inline]
fn epq_relsubs_done(epqstate: &types_nodes::EPQState<'_>, idx: usize) -> bool {
    epqstate
        .relsubs_done
        .as_ref()
        .and_then(|v| v.get(idx).copied())
        .unwrap_or(false)
}

// ===========================================================================
// nodeIndexonlyscan.c — scan support (1:1).
// ===========================================================================

/// `IndexOnlyNext(node)` — retrieve a tuple from the IndexOnlyScan node's
/// index. On success the tuple lives in `node.ss.ss_ScanTupleSlot`; returns
/// `Ok(true)` if a tuple is available, `Ok(false)` when the scan is exhausted.
fn IndexOnlyNext<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // estate = node->ss.ps.state;
    // direction = ScanDirectionCombine(estate->es_direction, indexorderdir);
    let plan_dir = match node.ss.ps.plan {
        Some(types_nodes::nodes::Node::IndexOnlyScan(ios)) => ios.indexorderdir,
        // The node's plan is always an IndexOnlyScan.
        _ => return Err(elog("IndexOnlyScan node has wrong plan type")),
    };
    let direction = scan_direction_combine(estate.es_direction, plan_dir);

    let scan_slot = node
        .ss
        .ss_ScanTupleSlot
        .ok_or_else(|| elog("index-only scan has no scan tuple slot"))?;
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .ok_or_else(|| elog("index-only scan has no expr context"))?;
    let table_slot = node
        .ioss_TableSlot
        .ok_or_else(|| elog("index-only scan has no table slot"))?;

    if node.ioss_ScanDesc.is_none() {
        // Not parallel (or serially executing a parallel-planned scan):
        // scandesc = index_beginscan(heapRel, indexRel, snapshot, instrument,
        //                            numScanKeys, numOrderByKeys);
        let heap_rel = node
            .ss
            .ss_currentRelation
            .as_ref()
            .map(|r| r.alias())
            .ok_or_else(|| elog("index-only scan has no current relation"))?;
        let index_rel = node
            .ioss_RelationDesc
            .as_ref()
            .map(|r| r.alias())
            .ok_or_else(|| elog("index-only scan has no index relation"))?;
        let mcx: Mcx<'_> = estate.es_query_cxt;
        let mut scandesc = indexam::index_beginscan::call(
            mcx,
            heap_rel,
            index_rel,
            estate.es_snapshot.clone(),
            node.ioss_Instrument,
            node.ioss_NumScanKeys,
            node.ioss_NumOrderByKeys,
        )?;

        // Set it up for index-only scan; node->ioss_VMBuffer = InvalidBuffer;
        scandesc.xs_want_itup = true;
        node.ioss_ScanDesc = Some(scandesc);
        node.ioss_VMBuffer = InvalidBuffer;

        // If no run-time keys to calculate or they are ready, pass the scankeys
        // to the index AM.
        if node.ioss_NumRuntimeKeys == 0 || node.ioss_RuntimeKeysReady {
            indexam::index_rescan::call(mcx, node)?;
        }
    }

    // OK, now that we have what we need, fetch the next tuple.
    let mcx: Mcx<'_> = estate.es_query_cxt;
    loop {
        let tid = {
            let scandesc = node.ioss_ScanDesc.as_mut().unwrap();
            match indexam::index_getnext_tid::call(mcx, scandesc, direction)? {
                Some(tid) => tid,
                None => break,
            }
        };

        let mut tuple_from_heap = false;

        tcop_postgres::check_for_interrupts::call()?;

        // We can skip the heap fetch if the TID references a heap page on which
        // all tuples are known visible to everybody. We use the index tuple as
        // the data source in any case.
        //
        // if (!VM_ALL_VISIBLE(scandesc->heapRelation,
        //                     ItemPointerGetBlockNumber(tid), &node->ioss_VMBuffer))
        let blkno = tid.ip_blkid.block_number();
        let all_visible = {
            let heap_rel = node
                .ioss_ScanDesc
                .as_ref()
                .unwrap()
                .heap_relation
                .as_ref()
                .map(|r| r.alias())
                .ok_or_else(|| elog("index-only scan descriptor has no heap relation"))?;
            let (status, vmbuf) = visibilitymap::visibilitymap_get_status::call(
                heap_rel,
                blkno,
                node.ioss_VMBuffer,
            )?;
            node.ioss_VMBuffer = vmbuf;
            status & visibilitymap::VISIBILITYMAP_ALL_VISIBLE != 0
        };

        if !all_visible {
            // Rats, we have to visit the heap to check visibility.
            instr_count_tuples2(node, 1);

            // if (!index_fetch_heap(scandesc, node->ioss_TableSlot)) continue;
            let fetched = {
                let scandesc = node.ioss_ScanDesc.as_mut().unwrap();
                indexam::index_fetch_heap::call(scandesc, estate, table_slot)?
            };
            if !fetched {
                // no visible tuple, try next index entry
                continue;
            }

            // ExecClearTuple(node->ioss_TableSlot);
            execTuples::exec_clear_tuple::call(estate, table_slot)?;

            // Only MVCC snapshots are supported, so no need to keep following
            // the HOT chain once a visible entry has been found.
            if node.ioss_ScanDesc.as_ref().unwrap().xs_heap_continue {
                return Err(elog(
                    "non-MVCC snapshots are not supported in index-only scans",
                ));
            }

            // Note: we are holding a pin on the heap page (scandesc->xs_cbuf).
            tuple_from_heap = true;
        }

        // Fill the scan tuple slot with data from the index — in HeapTuple or
        // IndexTuple format. If both are filled, prefer the heap format.
        let has_hitup = node.ioss_ScanDesc.as_ref().unwrap().xs_hitup.is_some();
        let has_itup = node.ioss_ScanDesc.as_ref().unwrap().xs_itup.is_some();
        if has_hitup {
            // Quick check on the number of fields (Assert in C):
            // slot->tts_tupleDescriptor->natts == scandesc->xs_hitupdesc->natts.
            // ExecForceStoreHeapTuple(scandesc->xs_hitup, slot, false);
            let hitup = node
                .ioss_ScanDesc
                .as_ref()
                .unwrap()
                .xs_hitup
                .as_ref()
                .unwrap()
                .clone();
            execTuples::exec_force_store_heap_tuple::call(estate, scan_slot, &hitup, false)?;
        } else if has_itup {
            // StoreIndexTuple(node, slot, scandesc->xs_itup, scandesc->xs_itupdesc);
            StoreIndexTuple(node, estate, scan_slot, econtext)?;
        } else {
            return Err(elog("no data returned for index-only scan"));
        }

        // If the index was lossy, we have to recheck the index quals.
        if node.ioss_ScanDesc.as_ref().unwrap().xs_recheck {
            // econtext->ecxt_scantuple = slot;
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(scan_slot);
            // if (!ExecQualAndReset(node->recheckqual, econtext))
            let passed = match &mut node.recheckqual {
                Some(rq) => {
                    let r = execExpr::exec_qual::call(rq, econtext, estate)?;
                    execUtils::reset_expr_context::call(estate, econtext)?;
                    r
                }
                None => {
                    execUtils::reset_expr_context::call(estate, econtext)?;
                    true
                }
            };
            if !passed {
                // Fails recheck, so drop it and loop back for another.
                instr_count_filtered2(node, 1);
                continue;
            }
        }

        // We don't currently support rechecking ORDER BY distances.
        // if (scandesc->numberOfOrderBys > 0 && scandesc->xs_recheckorderby)
        {
            let scandesc = node.ioss_ScanDesc.as_ref().unwrap();
            if scandesc.number_of_order_bys > 0 && scandesc.xs_recheckorderby {
                return Err(feature(
                    "lossy distance functions are not supported in index-only scans",
                ));
            }
        }

        // If we didn't access the heap, take a predicate lock explicitly, at
        // page level.
        if !tuple_from_heap {
            let heap_rel = node
                .ioss_ScanDesc
                .as_ref()
                .unwrap()
                .heap_relation
                .as_ref()
                .map(|r| r.alias())
                .ok_or_else(|| elog("index-only scan descriptor has no heap relation"))?;
            predicate::predicate_lock_page::call(heap_rel, blkno, estate.es_snapshot.clone())?;
        }

        return Ok(true);
    }

    // End of scan: return ExecClearTuple(slot);
    execTuples::exec_clear_tuple::call(estate, scan_slot)?;
    Ok(false)
}

/// `StoreIndexTuple(node, slot, itup, itupdesc)` — fill the slot with data from
/// the index tuple. The AM-supplied `itupdesc` (not the slot's) is used in
/// `index_deform_tuple`, in case datatypes differ (btree name_ops).
fn StoreIndexTuple<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    econtext: types_nodes::EcxtId,
) -> PgResult<()> {
    // ExecClearTuple(slot);
    // index_deform_tuple(itup, itupdesc, slot->tts_values, slot->tts_isnull);
    //
    // `index_deform_tuple` deforms the on-disk `xs_itup` byte image into the
    // per-attribute `(value, isnull)` pairs; the owned model returns them and
    // we write them into the slot's `tts_values`/`tts_isnull` via
    // `store_virtual_values` (the clear + fill + ExecStoreVirtualTuple
    // primitive). The AM-supplied `itupdesc` is used for the deform (not the
    // slot's), in case datatypes differ (btree name_ops).
    {
        let mcx = estate.es_query_cxt;
        let scandesc = node.ioss_ScanDesc.as_ref().unwrap();
        let itup = scandesc
            .xs_itup
            .as_ref()
            .ok_or_else(|| elog("index-only scan: no index tuple to store"))?;
        let itupdesc = scandesc
            .xs_itupdesc
            .as_ref()
            .ok_or_else(|| elog("index-only scan: no index tuple descriptor"))?;
        let columns = indextuple::index_deform_tuple::call(mcx, itup.as_slice(), itupdesc)?;
        let mut values = mcx::vec_with_capacity_in(mcx, columns.len())?;
        let mut isnull = mcx::vec_with_capacity_in(mcx, columns.len())?;
        for (value, null) in columns.iter() {
            values.push(value.clone());
            isnull.push(*null);
        }
        execTuples::store_virtual_values::call(estate, slot, values.as_slice(), isnull.as_slice())?;
    }

    // Copy all name columns stored as cstrings back into NAMEDATALEN-byte
    // allocations. Marked unlikely: "name" is generally only the system
    // catalogs.
    //
    // if (unlikely(node->ioss_NameCStringAttNums != NULL)) { ... }
    if !node.ioss_NameCStringAttNums.is_empty() {
        // The decision of which attnums to fix up is this crate's owned logic
        // (computed in ExecInitIndexOnlyScan, NAMEDATALEN constant); the
        // per-attribute slot-value read/write the slot owns.
        let attnums: &[types_core::AttrNumber] = &node.ioss_NameCStringAttNums;
        execTuples::pad_name_cstring_columns::call(estate, slot, econtext, attnums)?;
    }

    // ExecStoreVirtualTuple(slot);
    execTuples::exec_store_virtual_tuple::call(estate, slot)?;
    Ok(())
}

/// `IndexOnlyRecheck(node, slot)` — EvalPlanQual recheck access method. This
/// can't really happen for an index-only scan (an index can't supply CTID), so
/// throw an error.
fn IndexOnlyRecheck<'mcx>(
    _node: &mut IndexOnlyScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    Err(elog(
        "EvalPlanQual recheck is not supported in index-only scans",
    ))
}

// ===========================================================================
// Public node entry points (1:1).
// ===========================================================================

/// `ExecIndexOnlyScan(pstate)` — scan the index, returning whether the next
/// qualifying tuple is available (in the node's scan/result slot).
///
/// Takes the enclosing `PlanStateNode` (C: `PlanState *pstate`) so the
/// runtime-key setup can go through the generic `ExecReScan` dispatcher
/// (execAmi) exactly as the C does — that path also runs `InstrEndLoop`,
/// `chgParam` propagation, and `ReScanExprContext` before the node-specific
/// rescan, none of which `ExecReScanIndexOnlyScan` does on its own.
pub fn ExecIndexOnlyScan<'mcx>(
    pstate: &mut types_nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // If we have runtime keys and they've not already been set up, do it now.
    //   if (node->ioss_NumRuntimeKeys != 0 && !node->ioss_RuntimeKeysReady)
    //       ExecReScan((PlanState *) node);
    let (num_runtime_keys, ready) = match pstate {
        types_nodes::PlanStateNode::IndexOnlyScan(n) => {
            (n.ioss_NumRuntimeKeys, n.ioss_RuntimeKeysReady)
        }
        _ => return Err(elog("ExecIndexOnlyScan dispatched to wrong node type")),
    };
    if num_runtime_keys != 0 && !ready {
        // Generic ExecReScan dispatcher (execAmi) — instrument loop end,
        // chgParam propagation, expr-context rescan, then the node switch
        // dispatches to ExecReScanIndexOnlyScan.
        execAmi::exec_re_scan::call(pstate, estate)?;
    }

    let node = match pstate {
        types_nodes::PlanStateNode::IndexOnlyScan(n) => &mut **n,
        _ => return Err(elog("ExecIndexOnlyScan dispatched to wrong node type")),
    };

    // return ExecScan(&node->ss, IndexOnlyNext, IndexOnlyRecheck);
    // The execScan.c driver (qual/projection + the EvalPlanQual
    // replacement-tuple decision tree) is owned by the execScan unit; we pass
    // this node's own access/recheck methods.
    execScan::exec_scan_indexonly::call(node, estate, IndexOnlyNext, IndexOnlyRecheck)
}

/// `ExecReScanIndexOnlyScan(node)` — recalculate runtime scan keys, then rescan
/// the indexed relation.
pub fn ExecReScanIndexOnlyScan<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Recompute runtime key values, resetting the runtime context first so we
    // don't leak memory per outer tuple.
    if node.ioss_NumRuntimeKeys != 0 {
        let econtext = node
            .ioss_RuntimeContext
            .ok_or_else(|| elog("index-only scan has no runtime context"))?;
        // ResetExprContext(econtext);
        execUtils::reset_expr_context::call(estate, econtext)?;
        // ExecIndexEvalRuntimeKeys(econtext, ioss_RuntimeKeys, ioss_NumRuntimeKeys);
        nodeIndexscan::exec_index_eval_runtime_keys_ios::call(node, estate, econtext)?;
    }
    node.ioss_RuntimeKeysReady = true;

    // reset index scan
    if node.ioss_ScanDesc.is_some() {
        let mcx: Mcx<'_> = estate.es_query_cxt;
        indexam::index_rescan::call(mcx, node)?;
    }

    // ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_ss::call(&mut node.ss, estate)
}

/// `ExecEndIndexOnlyScan(node)` — release all storage.
pub fn ExecEndIndexOnlyScan<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // indexRelationDesc = node->ioss_RelationDesc;
    // indexScanDesc = node->ioss_ScanDesc;

    // Release VM buffer pin, if any.
    if node.ioss_VMBuffer != InvalidBuffer {
        bufmgr::release_buffer::call(node.ioss_VMBuffer);
        node.ioss_VMBuffer = InvalidBuffer;
    }

    // When ending a parallel worker, accumulate the gathered stats back into
    // shared memory for EXPLAIN ANALYZE.
    //
    // if (node->ioss_SharedInfo != NULL && IsParallelWorker())
    if node.ioss_SharedInfo.is_some() && parallel::is_parallel_worker() {
        // winstrument[ParallelWorkerNumber].nsearches += ioss_Instrument.nsearches;
        let nsearches = node.ioss_Instrument.nsearches;
        let shared = node.ioss_SharedInfo.as_mut().unwrap();
        parallel::accumulate_shared_index_searches(shared, nsearches);
    }

    // close the index relation (no-op if we didn't open it)
    // if (indexScanDesc) index_endscan(indexScanDesc);
    if let Some(scandesc) = node.ioss_ScanDesc.take() {
        let mcx: Mcx<'_> = estate.es_query_cxt;
        indexam::index_endscan::call(mcx, scandesc)?;
    }
    // if (indexRelationDesc) index_close(indexRelationDesc, NoLock);
    if let Some(index_rel) = node.ioss_RelationDesc.take() {
        // index_close(rel, NoLock) — the handle's drop releases with NoLock.
        let _ = estate;
        drop(index_rel);
    }

    Ok(())
}

/// `ExecIndexOnlyMarkPos(node)` — mark scan position.
///
/// We assume no caller sets a mark before reading at least one tuple (else
/// `ioss_ScanDesc` could still be NULL).
pub fn ExecIndexOnlyMarkPos<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(epqstate) = estate.es_epq_active.as_deref() {
        // Inside an EvalPlanQual recheck. If a test tuple exists for this rel,
        // don't access the index; given no caller sets a mark at scan start, we
        // can only get here with relsubs_done already set (verified below).
        let scanrelid = scan_scanrelid(node)?;
        debug_assert!(scanrelid > 0);
        let idx = (scanrelid - 1) as usize;
        // epqstate->relsubs_slot[scanrelid - 1] != NULL ||
        // epqstate->relsubs_rowmark[scanrelid - 1] != NULL
        if epq_relsubs_slot_present(epqstate, idx) || epq_relsubs_rowmark_present(epqstate, idx) {
            // if (!epqstate->relsubs_done[scanrelid - 1])
            if !epq_relsubs_done(epqstate, idx) {
                return Err(elog("unexpected ExecIndexOnlyMarkPos call in EPQ recheck"));
            }
            return Ok(());
        }
    }

    // index_markpos(node->ioss_ScanDesc);
    let mcx: Mcx<'_> = estate.es_query_cxt;
    let scandesc = node
        .ioss_ScanDesc
        .as_mut()
        .ok_or_else(|| elog("index-only scan: mark before scan started"))?;
    indexam::index_markpos::call(mcx, scandesc)
}

/// `ExecIndexOnlyRestrPos(node)` — restore scan position.
pub fn ExecIndexOnlyRestrPos<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(epqstate) = estate.es_epq_active.as_deref() {
        // See comments in ExecIndexOnlyMarkPos.
        let scanrelid = scan_scanrelid(node)?;
        debug_assert!(scanrelid > 0);
        let idx = (scanrelid - 1) as usize;
        if epq_relsubs_slot_present(epqstate, idx) || epq_relsubs_rowmark_present(epqstate, idx) {
            if !epq_relsubs_done(epqstate, idx) {
                return Err(elog("unexpected ExecIndexOnlyRestrPos call in EPQ recheck"));
            }
            return Ok(());
        }
    }

    // index_restrpos(node->ioss_ScanDesc);
    let mcx: Mcx<'_> = estate.es_query_cxt;
    let scandesc = node
        .ioss_ScanDesc
        .as_mut()
        .ok_or_else(|| elog("index-only scan: restore before scan started"))?;
    indexam::index_restrpos::call(mcx, scandesc)
}

/// `ExecInitIndexOnlyScan(node, estate, eflags)` — initialize the index scan's
/// state, create scan keys, and open the base and index relations.
pub fn ExecInitIndexOnlyScan<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<mcx::PgBox<'mcx, IndexOnlyScanState<'mcx>>> {
    let mcx: Mcx<'mcx> = estate.es_query_cxt;

    // IndexOnlyScan *node — the enclosing plan-tree node (the C `IndexOnlyScan
    // *` is the same pointer via struct embedding). Panics if it is not an
    // `IndexOnlyScan` (the C `castNode`).
    let ios: &'mcx IndexOnlyScan<'mcx> = match node {
        types_nodes::nodes::Node::IndexOnlyScan(n) => n,
        other => panic!("castNode(IndexOnlyScan, node) failed: {other:?}"),
    };

    // create state structure (makeNode(IndexOnlyScanState))
    let mut indexstate = IndexOnlyScanState::make_boxed_in(mcx)?;

    // indexstate->ss.ps.plan = (Plan *) node;
    // indexstate->ss.ps.ExecProcNode = ExecIndexOnlyScan;
    // (The plan-node link aliases the shared read-only plan tree.)
    indexstate.ss.ps.plan = Some(node);
    indexstate.ss.ps.ExecProcNode = Some(exec_proc_node_trampoline);

    // Miscellaneous initialization: create expression context for node.
    execUtils::exec_assign_expr_context::call(estate, &mut indexstate.ss.ps)?;

    // open the scan relation
    let scanrelid = ios.scan.scanrelid;
    let current_relation = execUtils::exec_open_scan_relation::call(estate, scanrelid, eflags)?;
    indexstate.ss.ss_currentRelation = Some(current_relation);
    // indexstate->ss.ss_currentScanDesc = NULL; (no heap scan here)
    indexstate.ss.ss_currentScanDesc = None;

    // Build the scan tuple type from the planner's indextlist; create the scan
    // slot (virtual ops).
    let tup_desc = execTuples::exec_type_from_tl::call(
        mcx,
        ios.indextlist.as_deref().unwrap_or(&[]),
    )?;
    execTuples::exec_init_scan_tuple_slot::call(
        estate,
        &mut indexstate.ss,
        tup_desc,
        TupleSlotKind::Virtual,
    )?;

    // Another slot, in a table-AM-suitable format, for visibility rechecks.
    let table_desc = relation_get_descr(estate, &indexstate)?;
    let table_ops = table_slot_callbacks(estate, &indexstate);
    let table_slot = execTuples::exec_alloc_table_slot::call(estate, table_desc, table_ops)?;
    indexstate.ioss_TableSlot = Some(table_slot);

    // Initialize result type and projection info. The targetlist's Vars use
    // varno = INDEX_VAR.
    execTuples::exec_init_result_type_tl::call(&mut indexstate.ss.ps, estate)?;
    execUtils::exec_assign_scan_projection_info_with_varno::call(
        &mut indexstate.ss,
        estate,
        INDEX_VAR,
    )?;

    // initialize child expressions (qual + recheckqual)
    indexstate.ss.ps.qual = execExpr::exec_init_qual::call(
        ios.scan.plan.qual.as_deref(),
        &mut indexstate.ss.ps,
        estate,
    )?;
    indexstate.recheckqual = execExpr::exec_init_qual::call(
        ios.recheckqual.as_deref(),
        &mut indexstate.ss.ps,
        estate,
    )?;

    // If EXPLAIN only, stop here (allows EXPLAIN of plans with nonexistent
    // indexes).
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(indexstate);
    }

    // Open the index relation.
    // lockmode = exec_rt_fetch(scanrelid, estate)->rellockmode;
    // indexRelation = index_open(node->indexid, lockmode);
    let lockmode = execUtils::exec_rt_fetch_rellockmode::call(estate, scanrelid);
    let index_relation = indexam::index_open::call(mcx, ios.indexid, lockmode)?;
    indexstate.ioss_RelationDesc = Some(index_relation);

    // Initialize index-specific scan state.
    indexstate.ioss_RuntimeKeysReady = false;
    indexstate.ioss_RuntimeKeys.clear();
    indexstate.ioss_NumRuntimeKeys = 0;

    // build the index scan keys from the index qualification
    {
        let index = indexstate
            .ioss_RelationDesc
            .as_ref()
            .map(|r| r.alias())
            .ok_or_else(|| elog("index-only scan has no index relation"))?;
        nodeIndexscan::exec_index_build_scan_keys_ios::call(
            &mut indexstate,
            estate,
            index,
            ios.indexqual.as_deref(),
            false,
        )?;
    }
    // any ORDER BY exprs become scankeys the same way
    {
        let index = indexstate
            .ioss_RelationDesc
            .as_ref()
            .map(|r| r.alias())
            .ok_or_else(|| elog("index-only scan has no index relation"))?;
        nodeIndexscan::exec_index_build_scan_keys_ios::call(
            &mut indexstate,
            estate,
            index,
            ios.indexorderby.as_deref(),
            true,
        )?;
    }

    // If we have runtime keys, build a separate ExprContext to evaluate them
    // (the node's standard context gets reset every tuple). -tgl 7/11/00
    if indexstate.ioss_NumRuntimeKeys != 0 {
        let stdecontext = indexstate.ss.ps.ps_ExprContext;
        execUtils::exec_assign_expr_context::call(estate, &mut indexstate.ss.ps)?;
        indexstate.ioss_RuntimeContext = indexstate.ss.ps.ps_ExprContext;
        indexstate.ss.ps.ps_ExprContext = stdecontext;
    } else {
        indexstate.ioss_RuntimeContext = None;
    }

    // Detect "name" btree columns stored as cstrings (opcintype NAMEOID, index
    // tupdesc CSTRINGOID) so StoreIndexTuple can pad them to NAMEDATALEN.
    indexstate.ioss_NameCStringAttNums.clear();
    let indnkeyatts = index_indnkeyatts(&indexstate)?;
    let mut namecount: i32 = 0;

    // First, count the number of such index keys.
    for attnum in 0..indnkeyatts {
        if index_attr_is_namecstring(&indexstate, attnum)? {
            namecount += 1;
        }
    }

    if namecount > 0 {
        // Fallible reservation up front (the C `palloc` analog).
        indexstate
            .ioss_NameCStringAttNums
            .try_reserve_exact(namecount as usize)
            .map_err(|_| {
                mcx.oom(namecount as usize * core::mem::size_of::<types_core::AttrNumber>())
            })?;
        for attnum in 0..indnkeyatts {
            if index_attr_is_namecstring(&indexstate, attnum)? {
                indexstate
                    .ioss_NameCStringAttNums
                    .push(attnum as types_core::AttrNumber);
            }
        }
    }

    indexstate.ioss_NameCStringCount = namecount;

    Ok(indexstate)
}

// ===========================================================================
// Parallel Index-only Scan Support (1:1).
//
// Reached through the opaque-handle seams in
// `backend-executor-nodeIndexonlyscan-seams`, which execParallel calls during
// parallel-query setup. The handle→node resolution and the DSM TOC plumbing
// are owned by execParallel / shm_toc; the node-level logic below is complete
// and operates on the real `IndexOnlyScanState`.
// ===========================================================================

/// `ExecIndexOnlyScanEstimate(node, pcxt)` — estimate DSM space for the
/// parallel index-only scan.
pub fn ExecIndexOnlyScanEstimate<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let instrument = node.ss.ps.instrument.is_some();
    let parallel_aware = plan_parallel_aware(node)?;

    if !instrument && !parallel_aware {
        // No DSM required by the scan.
        return Ok(());
    }

    let index = node
        .ioss_RelationDesc
        .as_ref()
        .map(|r| r.alias())
        .ok_or_else(|| elog("index-only scan has no index relation"))?;
    let nworkers = shm_toc::pcxt_nworkers::call(pcxt);
    // node->ioss_PscanLen = index_parallelscan_estimate(...);
    node.ioss_PscanLen = indexam::index_parallelscan_estimate::call(
        index,
        node.ioss_NumScanKeys,
        node.ioss_NumOrderByKeys,
        estate.es_snapshot.clone(),
        instrument,
        parallel_aware,
        nworkers,
    )?;
    // shm_toc_estimate_chunk(&pcxt->estimator, len); shm_toc_estimate_keys(&pcxt->estimator, 1);
    shm_toc::estimate_chunk_and_key::call(pcxt, node.ioss_PscanLen);
    Ok(())
}

/// `ExecIndexOnlyScanInitializeDSM(node, pcxt)` — set up a parallel index-only
/// scan descriptor.
pub fn ExecIndexOnlyScanInitializeDSM<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let instrument = node.ss.ps.instrument.is_some();
    let parallel_aware = plan_parallel_aware(node)?;

    if !instrument && !parallel_aware {
        return Ok(());
    }

    let mcx: Mcx<'_> = estate.es_query_cxt;
    let plan_node_id = plan_node_id(node)?;

    // piscan = shm_toc_allocate(pcxt->toc, len);
    // index_parallelscan_initialize(currentRel, indexRel, snapshot, instrument,
    //     parallel_aware, nworkers, &node->ioss_SharedInfo, piscan);
    // shm_toc_insert(pcxt->toc, plan_node_id, piscan);
    let heap_rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .map(|r| r.alias())
        .ok_or_else(|| elog("index-only scan has no current relation"))?;
    let index_rel = node
        .ioss_RelationDesc
        .as_ref()
        .map(|r| r.alias())
        .ok_or_else(|| elog("index-only scan has no index relation"))?;
    let nworkers = shm_toc::pcxt_nworkers::call(pcxt);
    let descriptor = indexam::index_parallelscan_initialize::call(
        mcx,
        heap_rel,
        index_rel,
        estate.es_snapshot.clone(),
        instrument,
        parallel_aware,
        nworkers,
        types_nodes::ParallelIndexScanDescData::default(),
    )?;
    let piscan = shm_toc::toc_allocate_and_insert_piscan::call(
        mcx,
        pcxt,
        plan_node_id,
        (*descriptor).clone(),
    )?;

    if !parallel_aware {
        // Only here to initialize SharedInfo in DSM.
        return Ok(());
    }

    // node->ioss_ScanDesc = index_beginscan_parallel(...);
    let heap_rel = node.ss.ss_currentRelation.as_ref().map(|r| r.alias()).unwrap();
    let index_rel = node.ioss_RelationDesc.as_ref().map(|r| r.alias()).unwrap();
    let mut scandesc = indexam::index_beginscan_parallel::call(
        mcx,
        heap_rel,
        index_rel,
        node.ioss_Instrument,
        node.ioss_NumScanKeys,
        node.ioss_NumOrderByKeys,
        piscan,
    )?;
    scandesc.xs_want_itup = true;
    node.ioss_ScanDesc = Some(scandesc);
    node.ioss_VMBuffer = InvalidBuffer;

    if node.ioss_NumRuntimeKeys == 0 || node.ioss_RuntimeKeysReady {
        indexam::index_rescan::call(mcx, node)?;
    }

    Ok(())
}

/// `ExecIndexOnlyScanReInitializeDSM(node, pcxt)` — reset shared state before a
/// fresh scan.
pub fn ExecIndexOnlyScanReInitializeDSM<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    _pcxt: ParallelContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(plan_parallel_aware(node).unwrap_or(true));
    let mcx: Mcx<'mcx> = estate.es_query_cxt;
    let scandesc = node
        .ioss_ScanDesc
        .as_mut()
        .ok_or_else(|| elog("index-only parallel rescan before scan started"))?;
    indexam::index_parallelrescan::call(mcx, scandesc)
}

/// `ExecIndexOnlyScanInitializeWorker(node, pwcxt)` — copy info from the TOC
/// into planstate in a parallel worker.
pub fn ExecIndexOnlyScanInitializeWorker<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let instrument = node.ss.ps.instrument.is_some();
    let parallel_aware = plan_parallel_aware(node)?;

    if !instrument && !parallel_aware {
        return Ok(());
    }

    let mcx: Mcx<'_> = estate.es_query_cxt;
    let plan_node_id = plan_node_id(node)?;

    // piscan = shm_toc_lookup(pwcxt->toc, plan_node_id, false);
    let piscan = shm_toc::toc_lookup_piscan::call(mcx, pwcxt, plan_node_id)?;

    // if (instrument)
    //     node->ioss_SharedInfo = (SharedIndexScanInstrumentation *)
    //         OffsetToPointer(piscan, piscan->ps_offset_ins);
    //
    // The offset arithmetic into the DSM blob is owned by the parallel
    // index-scan infrastructure (the seam); the assignment to the worker
    // node's SharedInfo is this node's own logic.
    if instrument {
        let shared = indexam::index_scan_resolve_shared_info::call(&piscan)?;
        node.ioss_SharedInfo = Some(mcx::alloc_in(mcx, shared)?);
    }

    if !parallel_aware {
        // Only here to set up the worker node's SharedInfo.
        return Ok(());
    }

    // node->ioss_ScanDesc = index_beginscan_parallel(...);
    let heap_rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .map(|r| r.alias())
        .ok_or_else(|| elog("index-only scan has no current relation"))?;
    let index_rel = node
        .ioss_RelationDesc
        .as_ref()
        .map(|r| r.alias())
        .ok_or_else(|| elog("index-only scan has no index relation"))?;
    let mut scandesc = indexam::index_beginscan_parallel::call(
        mcx,
        heap_rel,
        index_rel,
        node.ioss_Instrument,
        node.ioss_NumScanKeys,
        node.ioss_NumOrderByKeys,
        piscan,
    )?;
    scandesc.xs_want_itup = true;
    node.ioss_ScanDesc = Some(scandesc);

    if node.ioss_NumRuntimeKeys == 0 || node.ioss_RuntimeKeysReady {
        indexam::index_rescan::call(mcx, node)?;
    }

    Ok(())
}

/// `ExecIndexOnlyScanRetrieveInstrumentation(node)` — transfer index-only scan
/// statistics from DSM to private memory.
pub fn ExecIndexOnlyScanRetrieveInstrumentation<'mcx>(
    node: &mut IndexOnlyScanState<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // if (SharedInfo == NULL) return;
    let shared = match &node.ioss_SharedInfo {
        None => return Ok(()),
        Some(s) => s,
    };

    // Create a backend-local copy of SharedInfo (C: palloc + memcpy of
    // offsetof(winstrument) + num_workers * sizeof(IndexScanInstrumentation)).
    let copy = types_nodes::SharedIndexScanInstrumentation {
        num_workers: shared.num_workers,
        winstrument: {
            let mut v = mcx::vec_with_capacity_in(mcx, shared.winstrument.len())?;
            for w in shared.winstrument.iter() {
                v.push(*w);
            }
            // The owned copy lives in `mcx` via PgBox below; the Vec uses the
            // global allocator only as the carrier of a Copy payload.
            v.into_iter().collect::<alloc::vec::Vec<_>>()
        },
    };
    node.ioss_SharedInfo = Some(mcx::alloc_in(mcx, copy)?);
    Ok(())
}

// ===========================================================================
// Small helpers reading the node's own/plan state (no foreign owner).
// ===========================================================================

extern crate alloc;

/// `ScanDirectionCombine(a, b)` (sdir.h): `(a) * (b)`, preserving the -1/0/1
/// values via the enum's `repr(i32)`.
fn scan_direction_combine(a: ScanDirection, b: ScanDirection) -> ScanDirection {
    match (a as i32) * (b as i32) {
        -1 => ScanDirection::BackwardScanDirection,
        1 => ScanDirection::ForwardScanDirection,
        _ => ScanDirection::NoMovementScanDirection,
    }
}

/// `InstrCountTuples2(node, delta)` (instrument.h): bump the node's
/// instrumentation `ntuples2` if instrumented.
fn instr_count_tuples2(node: &mut IndexOnlyScanState<'_>, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.ntuples2 += delta as f64;
    }
}

/// `InstrCountFiltered2(node, delta)` (instrument.h): bump `nfiltered2`.
fn instr_count_filtered2(node: &mut IndexOnlyScanState<'_>, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered2 += delta as f64;
    }
}

/// `((Scan *) node->ss.ps.plan)->scanrelid`.
fn scan_scanrelid(node: &IndexOnlyScanState<'_>) -> PgResult<u32> {
    match node.ss.ps.plan {
        Some(types_nodes::nodes::Node::IndexOnlyScan(ios)) => Ok(ios.scan.scanrelid),
        _ => Err(elog("IndexOnlyScan node has wrong plan type")),
    }
}

/// `node->ss.ps.plan->parallel_aware`.
fn plan_parallel_aware(node: &IndexOnlyScanState<'_>) -> PgResult<bool> {
    match node.ss.ps.plan {
        Some(n) => Ok(n.plan_head().parallel_aware),
        None => Err(elog("index-only scan has no plan")),
    }
}

/// `node->ss.ps.plan->plan_node_id` — the planner-assigned id used as the DSM
/// TOC key.
fn plan_node_id(node: &IndexOnlyScanState<'_>) -> PgResult<i32> {
    match node.ss.ps.plan {
        Some(n) => Ok(n.plan_head().plan_node_id),
        None => Err(elog("index-only scan has no plan")),
    }
}

/// `RelationGetDescr(currentRelation)` — the scan relation's tuple descriptor,
/// copied into the query context for the table slot.
fn relation_get_descr<'mcx>(
    estate: &EStateData<'mcx>,
    node: &IndexOnlyScanState<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    let mcx = estate.es_query_cxt;
    let rel = node
        .ss
        .ss_currentRelation
        .as_ref()
        .ok_or_else(|| elog("index-only scan has no current relation"))?;
    Ok(Some(rel.rd_att_clone_in(mcx)?))
}

/// `table_slot_callbacks(currentRelation)` — the slot class the table AM wants
/// for the recheck slot. Heap relations use buffer-heap-tuple slots.
fn table_slot_callbacks(
    _estate: &EStateData<'_>,
    _node: &IndexOnlyScanState<'_>,
) -> TupleSlotKind {
    // The C `table_slot_callbacks` dispatches through the table AM; for the
    // heap AM (the only one ported) it is `&TTSOpsBufferHeapTuple`.
    TupleSlotKind::BufferHeapTuple
}

/// `indexRelation->rd_index->indnkeyatts`.
fn index_indnkeyatts(node: &IndexOnlyScanState<'_>) -> PgResult<i32> {
    let rel = node
        .ioss_RelationDesc
        .as_ref()
        .ok_or_else(|| elog("index-only scan has no index relation"))?;
    Ok(rel.indnkeyatts())
}

/// `TupleDescAttr(indexRelation->rd_att, attnum)->atttypid == CSTRINGOID &&
///  indexRelation->rd_opcintype[attnum] == NAMEOID`.
fn index_attr_is_namecstring(node: &IndexOnlyScanState<'_>, attnum: i32) -> PgResult<bool> {
    let rel = node
        .ioss_RelationDesc
        .as_ref()
        .ok_or_else(|| elog("index-only scan has no index relation"))?;
    Ok(rel.index_attr_is_namecstring(attnum))
}

/// The `ExecProcNode` callback trampoline installed into `ps.ExecProcNode`.
fn exec_proc_node_trampoline<'mcx>(
    pstate: &mut types_nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let have = ExecIndexOnlyScan(pstate, estate)?;
    if have {
        let node = match pstate {
            types_nodes::PlanStateNode::IndexOnlyScan(n) => &mut **n,
            _ => return Err(elog("ExecProcNode dispatched to wrong node type")),
        };
        // The result tuple is in the projection result slot (or the scan slot
        // when not projecting).
        Ok(node
            .ss
            .ps
            .ps_ResultTupleSlot
            .or(node.ss.ss_ScanTupleSlot))
    } else {
        Ok(None)
    }
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install this crate's parallel-executor methods into the
/// `backend-executor-nodeIndexonlyscan-seams` slots. The seams are declared
/// over execParallel's opaque `PlanStateHandle`/`ParallelContextHandle`; the
/// handle→node resolution belongs to execParallel and is not yet wired, so the
/// installed bridges panic at that frontier (mirror-PG-and-panic). The node's
/// own parallel logic lives in the real `ExecIndexOnlyScan*` functions above.
pub fn init_seams() {
    backend_executor_nodeIndexonlyscan_seams::exec_indexonlyscan_estimate::set(
        bridge_estimate,
    );
    backend_executor_nodeIndexonlyscan_seams::exec_indexonlyscan_initialize_dsm::set(
        bridge_initialize_dsm,
    );
    backend_executor_nodeIndexonlyscan_seams::exec_indexonlyscan_reinitialize_dsm::set(
        bridge_reinitialize_dsm,
    );
    backend_executor_nodeIndexonlyscan_seams::exec_indexonlyscan_initialize_worker::set(
        bridge_initialize_worker,
    );
    backend_executor_nodeIndexonlyscan_seams::exec_indexonlyscan_retrieve_instrumentation::set(
        bridge_retrieve_instrumentation,
    );
}

fn bridge_estimate(_node: PlanStateHandle, _pcxt: ParallelContextHandle) -> PgResult<()> {
    panic!(
        "exec_indexonlyscan_estimate: PlanStateHandle->IndexOnlyScanState resolution is \
         owned by execParallel and not yet wired"
    )
}
fn bridge_initialize_dsm(_node: PlanStateHandle, _pcxt: ParallelContextHandle) -> PgResult<()> {
    panic!(
        "exec_indexonlyscan_initialize_dsm: PlanStateHandle->IndexOnlyScanState resolution is \
         owned by execParallel and not yet wired"
    )
}
fn bridge_reinitialize_dsm(_node: PlanStateHandle, _pcxt: ParallelContextHandle) -> PgResult<()> {
    panic!(
        "exec_indexonlyscan_reinitialize_dsm: PlanStateHandle->IndexOnlyScanState resolution is \
         owned by execParallel and not yet wired"
    )
}
fn bridge_initialize_worker(
    _node: PlanStateHandle,
    _pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    panic!(
        "exec_indexonlyscan_initialize_worker: PlanStateHandle->IndexOnlyScanState resolution \
         is owned by execParallel and not yet wired"
    )
}
fn bridge_retrieve_instrumentation(_node: PlanStateHandle) -> PgResult<()> {
    panic!(
        "exec_indexonlyscan_retrieve_instrumentation: PlanStateHandle->IndexOnlyScanState \
         resolution is owned by execParallel and not yet wired"
    )
}
