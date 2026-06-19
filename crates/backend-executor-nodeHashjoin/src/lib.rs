//! Port of `src/backend/executor/nodeHashjoin.c` — routines to handle hash
//! join nodes (the hybrid hash-join state machine, parallel-oblivious and
//! parallel-aware).
//!
//! The node state is held as an owned [`HashJoinState`] mutated through `&mut`
//! borrows; the C `PlanState.state` back-pointer is replaced by threading
//! `&mut EStateData` explicitly, and `TupleTableSlot *` / `ExprContext *`
//! become ids into the EState pools. `ExecHashJoin` returns `Ok(true)` when a
//! tuple has been projected into the result slot (the C `return slot`) and
//! `Ok(false)` for the C `return NULL` (end of join).
//!
//! The hash-table library (nodeHash.c), expression evaluation (execExpr.c),
//! the `BufFile` batch spill (buffile.c), the shared tuplestores
//! (sharedtuplestore.c), the lsyscache catalog lookups, child node
//! init/exec/teardown (execProcnode.c), slot setup (execTuples/execUtils), and
//! the parallel barriers (owned by nodeHash's shared state) are reached through
//! those owners' seam crates and panic until the owners land. All hash-join
//! state-machine control flow, the batch-skipping rules, the save/get-tuple
//! byte format, and the jointype special cases are in-crate.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use backend_access_transam_parallel::shared_dsm_object;
use backend_access_transam_parallel as parallel_sup;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_executor_nodeHash_seams as nodeHash;
use backend_storage_file_buffile_seams as buffile;
use backend_storage_file_sharedfileset_seams as sharedfileset;
use backend_storage_ipc_barrier_seams as barrier;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_sort_storage_seams as sts;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{Oid, OidIsValid};
use types_error::{PgError, PgResult};
use types_nodes::nodehashjoin::{
    BufFile, HashJoin, HashJoinState, HashJoinTableData, JoinType, ParallelHashJoinState,
    INVALID_SKEW_BUCKET_NO,
    PHJ_BATCH_ALLOCATE, PHJ_BATCH_ELECT, PHJ_BATCH_FREE, PHJ_BATCH_LOAD, PHJ_BATCH_PROBE,
    PHJ_BATCH_SCAN, PHJ_BUILD_FREE, PHJ_BUILD_HASH_OUTER, PHJ_BUILD_RUN,
};
use types_nodes::primnodes::Expr;
use types_nodes::{EStateData, PlanStateNode, SlotId, TupleSlotKind};
use types_tuple::backend_access_common_heaptuple::FormedMinimalTuple;

/// Serialize a [`FormedMinimalTuple`] to its contiguous C `MinimalTuple` byte
/// image (the flat blob, `t_len` first) — the form the batch temp file / shared
/// tuplestore boundary carries. A well-formed tuple can only fail on the
/// allocation `ereport(ERROR)` (OOM).
fn mintuple_to_flat<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: &FormedMinimalTuple<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    use backend_access_common_heaptuple::flat::MinimalTupleFlatError;
    match backend_access_common_heaptuple::flat::minimal_tuple_to_flat(mcx, mtup) {
        Ok(blob) => Ok(blob),
        Err(MinimalTupleFlatError::Pg(err)) => Err(err),
        Err(other) => panic!("minimal_tuple_to_flat failed: {other:?}"),
    }
}

/// Decode a flat C `MinimalTuple` byte image (the blob a batch file / shared
/// tuplestore stored) back into a [`FormedMinimalTuple`].
fn mintuple_from_flat<'mcx>(
    mcx: Mcx<'mcx>,
    blob: &[u8],
) -> PgResult<FormedMinimalTuple<'mcx>> {
    use backend_access_common_heaptuple::flat::MinimalTupleFlatError;
    match backend_access_common_heaptuple::flat::minimal_tuple_from_flat(mcx, blob) {
        Ok(mtup) => Ok(mtup),
        Err(MinimalTupleFlatError::Pg(err)) => Err(err),
        Err(other) => panic!("minimal_tuple_from_flat on a batch-file image failed: {other:?}"),
    }
}

// ===========================================================================
// States of the ExecHashJoin state machine (nodeHashjoin.c).
// ===========================================================================

const HJ_BUILD_HASHTABLE: i32 = 1;
const HJ_NEED_NEW_OUTER: i32 = 2;
const HJ_SCAN_BUCKET: i32 = 3;
const HJ_FILL_OUTER_TUPLE: i32 = 4;
const HJ_FILL_INNER_TUPLES: i32 = 5;
const HJ_NEED_NEW_BATCH: i32 = 6;

/// `EXEC_FLAG_BACKWARD` (executor.h) — a hash join never supports backward scan.
pub const EXEC_FLAG_BACKWARD: i32 = 0x0008;
/// `EXEC_FLAG_MARK` (executor.h) — mark/restore; never for a hash join.
pub const EXEC_FLAG_MARK: i32 = 0x0010;

// Wait-event codes (utils/wait_event_names.txt; class PG_WAIT_IPC = 0x08000000)
// used at parallel barriers. Values verified against the c2rust rendering of
// wait_event.h.
const WAIT_EVENT_HASH_BUILD_HASH_OUTER: u32 = 0x08000014;
const WAIT_EVENT_HASH_BATCH_ELECT: u32 = 0x0800000F;
const WAIT_EVENT_HASH_BATCH_ALLOCATE: u32 = 0x0800000E;
const WAIT_EVENT_HASH_BATCH_LOAD: u32 = 0x08000010;

// SEEK_SET (stdio.h) — `BufFileSeek(file, 0, 0, SEEK_SET)` rewinds the file.
const SEEK_SET: i32 = 0;

/// `HJ_FILL_OUTER(hjstate)` — true if doing null-fill on the outer relation.
#[inline]
fn HJ_FILL_OUTER(node: &HashJoinState) -> bool {
    node.hj_NullInnerTupleSlot.is_some()
}

/// `HJ_FILL_INNER(hjstate)` — true if doing null-fill on the inner relation.
#[inline]
fn HJ_FILL_INNER(node: &HashJoinState) -> bool {
    node.hj_NullOuterTupleSlot.is_some()
}

/// `&node->hj_HashTable` — the built hash table, panicking if it is not built
/// (the C unconditional dereference on these paths).
#[inline]
fn hashtable<'a, 'mcx>(node: &'a HashJoinState<'mcx>) -> &'a HashJoinTableData<'mcx> {
    node.hj_HashTable
        .as_deref()
        .expect("nodeHashjoin: hash table must be built")
}

#[inline]
fn hashtable_mut<'a, 'mcx>(node: &'a mut HashJoinState<'mcx>) -> &'a mut HashJoinTableData<'mcx> {
    node.hj_HashTable
        .as_deref_mut()
        .expect("nodeHashjoin: hash table must be built")
}

/// `InstrCountFiltered1(node, 1)` — `if (node->instrument)
/// node->instrument->nfiltered1 += 1` (execnodes.h). In-crate field accumulate.
#[inline]
fn instr_count_filtered1(node: &mut HashJoinState) {
    if let Some(instr) = node.js.ps.instrument.as_deref_mut() {
        instr.nfiltered1 += 1.0;
    }
}

/// `InstrCountFiltered2(node, 1)` — same for `nfiltered2`.
#[inline]
fn instr_count_filtered2(node: &mut HashJoinState) {
    if let Some(instr) = node.js.ps.instrument.as_deref_mut() {
        instr.nfiltered2 += 1.0;
    }
}

/// Install this crate's implementations into its seam slots.
///
/// nodeHashjoin owns one outward-facing seam, `ExecHashJoinSaveTuple`, which
/// `nodeHash.c` calls from `ExecHashIncreaseNumBatches` /
/// `ExecHashRemoveNextSkewBucket` to spill a tuple that has moved to a later
/// batch. The rest of its entry points (execProcnode dispatch) are reached by
/// depending on this crate directly.
pub fn init_seams() {
    backend_executor_nodeHashjoin_seams::ExecHashJoinSaveTuple::set(exec_hash_join_save_tuple_seam);

    // Parallel-executor per-node hooks (`backend-executor-nodeHashjoin-pq-seams`),
    // dispatched generically by execParallel through a `PlanStateHandle`. Each
    // shim re-casts the handle to the live `HashJoinState` and runs the real
    // ported entry point.
    backend_executor_nodeHashjoin_pq_seams::exec_hashjoin_estimate::set(
        exec_hashjoin_estimate_shim,
    );
    backend_executor_nodeHashjoin_pq_seams::exec_hashjoin_initialize_dsm::set(
        exec_hashjoin_initialize_dsm_shim,
    );
    backend_executor_nodeHashjoin_pq_seams::exec_hashjoin_reinitialize_dsm::set(
        exec_hashjoin_reinitialize_dsm_shim,
    );
    backend_executor_nodeHashjoin_pq_seams::exec_hashjoin_initialize_worker::set(
        exec_hashjoin_initialize_worker_shim,
    );
}

/// `ExecHashJoinSaveTuple(tuple, hashvalue, fileptr, hashtable)`
/// (nodeHashjoin.c) — append a `MinimalTuple` + its hash value to a batch temp
/// file at `*fileptr`, creating the `BufFile` in the hash table's `spillCxt`
/// (threaded as `mcx`) on first write. The data recorded for each tuple is its
/// hash value, then the tuple in MinimalTuple format.
fn exec_hash_join_save_tuple_seam<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &[u8],
    hashvalue: u32,
    fileptr: &mut Option<PgBox<'mcx, BufFile>>,
) -> PgResult<()> {
    // if (file == NULL) { /* First write to this batch file, so open it. */
    //     file = BufFileCreateTemp(hashtable->spillCxt, false); *fileptr = file; }
    if fileptr.is_none() {
        let file = buffile::buf_file_create_temp::call(mcx, false)?;
        *fileptr = Some(file);
    }
    let file = fileptr.as_deref_mut().expect("file created above");

    // BufFileWrite(file, &hashvalue, sizeof(uint32));
    // BufFileWrite(file, tuple, tuple->t_len);
    // `tuple` is the tuple's contiguous C MinimalTuple byte image (flat blob,
    // exactly the `tuple->t_len` bytes C writes).
    buffile::buf_file_write::call(file, &hashvalue.to_ne_bytes())?;
    buffile::buf_file_write::call(file, tuple)?;
    Ok(())
}

// ===========================================================================
// ExecHashJoinImpl — the hybrid hashjoin state machine.
// ===========================================================================

/// `ExecHashJoinImpl(pstate, parallel)` — the hybrid hashjoin algorithm. In C
/// this is an always-inline helper specialized into `ExecHashJoin` (parallel ==
/// false) and `ExecParallelHashJoin` (parallel == true); here `parallel` is a
/// runtime parameter, exactly mirroring the C body.
fn ExecHashJoinImpl<'mcx>(
    node: &mut HashJoinState<'mcx>,
    parallel: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // get information from HashJoin node
    let joinqual_present = node.js.joinqual.is_some();
    let otherqual_present = node.js.ps.qual.is_some();

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    execUtils::reset_per_tuple_expr_context::call(estate, &node.js.ps)?;

    // run the hash join state machine
    loop {
        // It's possible to iterate this loop many times before returning a
        // tuple; check for interrupts each time through.
        tcop_postgres::check_for_interrupts::call()?;

        match node.hj_JoinState {
            HJ_BUILD_HASHTABLE => {
                // First time through: build hash table for inner relation.
                debug_assert!(node.hj_HashTable.is_none());

                // If the outer relation is completely empty, and it's not
                // right/right-anti/full join, we can quit without building the
                // hash table. (See the C comment for the full heuristic.) The
                // only way to make the check is to try to fetch a tuple from the
                // outer plan node; if we succeed, stash it for later use by
                // ExecHashJoinOuterGetTuple.
                if HJ_FILL_INNER(node) {
                    // no chance to not build the hash table
                    node.hj_FirstOuterTupleSlot = None;
                } else if parallel {
                    // The empty-outer optimization is not implemented for shared
                    // hash tables, so we have to build the hash table.
                    node.hj_FirstOuterTupleSlot = None;
                } else if HJ_FILL_OUTER(node) || (outer_startup_lt_inner_total(node)? && !node.hj_OuterNotEmpty) {
                    let outer = outer_plan_state(node);
                    let slot = execProcnode::exec_proc_node::call(outer, estate)?;
                    match slot {
                        Some(id) if !estate.slot(id).is_empty() => {
                            node.hj_FirstOuterTupleSlot = Some(id);
                            node.hj_OuterNotEmpty = true;
                        }
                        _ => {
                            node.hj_FirstOuterTupleSlot = None;
                            node.hj_OuterNotEmpty = false;
                            return Ok(None);
                        }
                    }
                } else {
                    node.hj_FirstOuterTupleSlot = None;
                }

                // Create the hash table; whoever gets here first under Parallel
                // Hash creates it, later arrivals attach. Then execute the inner
                // Hash node to build it.
                nodeHash::exec_hash_table_create::call(node, estate)?;
                nodeHash::multi_exec_hash::call(node, estate)?;

                // If the inner relation is completely empty, and we're not doing
                // a left outer join, we can quit without scanning the outer.
                if hashtable(node).totalTuples == 0.0 && !HJ_FILL_OUTER(node) {
                    if parallel {
                        // Advance the build barrier to PHJ_BUILD_RUN before
                        // proceeding so we can negotiate resource cleanup.
                        while nodeHash::build_barrier_phase::call(node) < PHJ_BUILD_RUN {
                            nodeHash::build_barrier_arrive_and_wait::call(node, 0)?;
                        }
                    }
                    return Ok(None);
                }

                // need to remember whether nbatch has increased since we began
                // scanning the outer relation
                {
                    let ht = hashtable_mut(node);
                    ht.nbatch_outstart = ht.nbatch;
                }

                // Reset OuterNotEmpty for scan.
                node.hj_OuterNotEmpty = false;

                if parallel {
                    let phase = nodeHash::build_barrier_phase::call(node);
                    debug_assert!(
                        phase == PHJ_BUILD_HASH_OUTER
                            || phase == PHJ_BUILD_RUN
                            || phase == PHJ_BUILD_FREE
                    );
                    if phase == PHJ_BUILD_HASH_OUTER {
                        // If multi-batch, hash the outer relation up front.
                        if hashtable(node).nbatch > 1 {
                            ExecParallelHashJoinPartitionOuter(node, estate)?;
                        }
                        nodeHash::build_barrier_arrive_and_wait::call(
                            node,
                            WAIT_EVENT_HASH_BUILD_HASH_OUTER,
                        )?;
                    } else if phase == PHJ_BUILD_FREE {
                        // We attached so late the job is finished and the batch
                        // state has been freed; return immediately.
                        return Ok(None);
                    }

                    // Each backend should now select a batch to work on.
                    debug_assert!(nodeHash::build_barrier_phase::call(node) == PHJ_BUILD_RUN);
                    hashtable_mut(node).curbatch = -1;
                    node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                } else {
                    node.hj_JoinState = HJ_NEED_NEW_OUTER;
                }
                // FALL THRU into HJ_NEED_NEW_OUTER.
                handle_need_new_outer(node, parallel, estate)?;
                continue;
            }

            HJ_NEED_NEW_OUTER => {
                handle_need_new_outer(node, parallel, estate)?;
                continue;
            }

            HJ_SCAN_BUCKET => {
                // Scan the selected hash bucket for matches to current outer.
                let got = if parallel {
                    nodeHash::exec_parallel_scan_hash_bucket::call(node, estate)?
                } else {
                    nodeHash::exec_scan_hash_bucket::call(node, estate)?
                };
                if !got {
                    // out of matches; check for possible outer-join fill
                    node.hj_JoinState = HJ_FILL_OUTER_TUPLE;
                    continue;
                }

                // In a right-semijoin, we only need the first match for each
                // inner tuple.
                if node.js.jointype == JoinType::JOIN_RIGHT_SEMI
                    && nodeHash::cur_tuple_has_match::call(node)
                {
                    continue;
                }

                // We've got a match, but still need to test non-hashed quals.
                if !joinqual_present || execExpr::exec_hashjoin_qual::call(node, true, estate)? {
                    node.hj_MatchedOuter = true;

                    // Only really needed if HJ_FILL_INNER(node) or in a
                    // right-semijoin, but we avoid the branch and set it always.
                    if !nodeHash::cur_tuple_has_match::call(node) {
                        nodeHash::cur_tuple_set_match::call(node)?;
                    }

                    // In an antijoin, we never return a matched tuple.
                    if node.js.jointype == JoinType::JOIN_ANTI {
                        node.hj_JoinState = HJ_NEED_NEW_OUTER;
                        continue;
                    }

                    // If we only need the first matching inner tuple, advance to
                    // the next outer tuple after this one.
                    if node.js.single_match {
                        node.hj_JoinState = HJ_NEED_NEW_OUTER;
                    }

                    // In a right-antijoin, we never return a matched tuple; if
                    // not inner_unique, stay on the current outer tuple to keep
                    // scanning the inner side for matches.
                    if node.js.jointype == JoinType::JOIN_RIGHT_ANTI {
                        continue;
                    }

                    if !otherqual_present || execExpr::exec_hashjoin_qual::call(node, false, estate)? {
                        let slot = execExpr::exec_hashjoin_project::call(node, estate)?;
                        return Ok(Some(slot));
                    } else {
                        instr_count_filtered2(node);
                    }
                } else {
                    instr_count_filtered1(node);
                }
                // break out of switch (loop around the for(;;))
                continue;
            }

            HJ_FILL_OUTER_TUPLE => {
                // The current outer tuple has run out of matches; check whether
                // to emit a dummy outer-join tuple. Either way, NEED_NEW_OUTER.
                node.hj_JoinState = HJ_NEED_NEW_OUTER;

                if !node.hj_MatchedOuter && HJ_FILL_OUTER(node) {
                    // Generate a fake join tuple with nulls for the inner tuple,
                    // and return it if it passes the non-join quals.
                    set_econtext_innertuple(node, node.hj_NullInnerTupleSlot, estate)?;

                    if !otherqual_present || execExpr::exec_hashjoin_qual::call(node, false, estate)? {
                        let slot = execExpr::exec_hashjoin_project::call(node, estate)?;
                        return Ok(Some(slot));
                    } else {
                        instr_count_filtered2(node);
                    }
                }
                continue;
            }

            HJ_FILL_INNER_TUPLES => {
                // We have finished a batch, but we are doing right/right-anti/
                // full join, so any unmatched inner tuples have to be emitted
                // before we continue to the next batch.
                let got = if parallel {
                    nodeHash::exec_parallel_scan_hash_table_for_unmatched::call(node, estate)?
                } else {
                    nodeHash::exec_scan_hash_table_for_unmatched::call(node, estate)?
                };
                if !got {
                    // no more unmatched tuples
                    node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }

                // Generate a fake join tuple with nulls for the outer tuple, and
                // return it if it passes the non-join quals.
                set_econtext_outertuple(node, node.hj_NullOuterTupleSlot, estate)?;

                if !otherqual_present || execExpr::exec_hashjoin_qual::call(node, false, estate)? {
                    let slot = execExpr::exec_hashjoin_project::call(node, estate)?;
                    return Ok(Some(slot));
                } else {
                    instr_count_filtered2(node);
                }
                continue;
            }

            HJ_NEED_NEW_BATCH => {
                // Try to advance to next batch. Done if there are no more.
                if parallel {
                    if !ExecParallelHashJoinNewBatch(node, estate)? {
                        return Ok(None); // end of parallel-aware join
                    }
                } else if !ExecHashJoinNewBatch(node, estate)? {
                    return Ok(None); // end of parallel-oblivious join
                }
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
                continue;
            }

            other => {
                return Err(PgError::error(alloc::format!(
                    "unrecognized hashjoin state: {other}"
                )));
            }
        }
    }
}

/// `outerNode->plan->startup_cost < hashNode->ps.plan->total_cost` — the
/// empty-outer prefetch cost heuristic. `outerNode = outerPlan(node)` is the
/// HashJoin plan's left child; `hashNode` is the inner `Hash` plan (the right
/// child), whose `ps.plan` is that same Hash plan. We read both costs straight
/// off the plan tree carried in `node.js.ps.plan`.
fn outer_startup_lt_inner_total(node: &HashJoinState) -> PgResult<bool> {
    let plan = node
        .js
        .ps
        .plan
        .expect("nodeHashjoin: HashJoin plan must be set");
    let outer = plan
        .plan_head()
        .lefttree
        .as_deref()
        .expect("nodeHashjoin: outerPlan(node) must be present");
    let inner = plan
        .plan_head()
        .righttree
        .as_deref()
        .expect("nodeHashjoin: innerPlan(node) must be present");
    Ok(outer.plan_head().startup_cost < inner.plan_head().total_cost)
}

/// `outerPlanState(node)` — the outer child plan-state.
#[inline]
fn outer_plan_state<'a, 'mcx>(node: &'a mut HashJoinState<'mcx>) -> &'a mut PlanStateNode<'mcx> {
    node.js
        .ps
        .lefttree
        .as_deref_mut()
        .expect("nodeHashjoin: outer plan state must be initialized")
}

/// `econtext->ecxt_innertuple = slot` — set the per-node ExprContext's inner
/// tuple (the node's `ps_ExprContext` id resolves the live context).
fn set_econtext_innertuple<'mcx>(
    node: &HashJoinState<'mcx>,
    slot: Option<SlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ecxt = node
        .js
        .ps
        .ps_ExprContext
        .expect("nodeHashjoin: ps_ExprContext must be assigned");
    estate.ecxt_mut(ecxt).ecxt_innertuple = slot;
    Ok(())
}

/// `econtext->ecxt_outertuple = slot`.
fn set_econtext_outertuple<'mcx>(
    node: &HashJoinState<'mcx>,
    slot: Option<SlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ecxt = node
        .js
        .ps
        .ps_ExprContext
        .expect("nodeHashjoin: ps_ExprContext must be assigned");
    estate.ecxt_mut(ecxt).ecxt_outertuple = slot;
    Ok(())
}

/// Shared body of the `HJ_NEED_NEW_OUTER` case, factored out so the
/// fall-through from `HJ_BUILD_HASHTABLE` reuses it exactly as C does. It only
/// updates `hj_JoinState`; the caller `continue`s and re-dispatches.
fn handle_need_new_outer<'mcx>(
    node: &mut HashJoinState<'mcx>,
    parallel: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // We don't have an outer tuple; try to get the next one.
    let outer = if parallel {
        ExecParallelHashJoinOuterGetTuple(node, estate)?
    } else {
        ExecHashJoinOuterGetTuple(node, estate)?
    };

    let hashvalue = match outer {
        Some(hv) => hv,
        None => {
            // end of batch, or maybe whole join
            if HJ_FILL_INNER(node) {
                // set up to scan for unmatched inner tuples
                if parallel {
                    // Only one process may handle each batch's unmatched tuples.
                    if nodeHash::exec_parallel_prep_hash_table_for_unmatched::call(node)? {
                        node.hj_JoinState = HJ_FILL_INNER_TUPLES;
                    } else {
                        node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    }
                } else {
                    nodeHash::exec_prep_hash_table_for_unmatched::call(node)?;
                    node.hj_JoinState = HJ_FILL_INNER_TUPLES;
                }
            } else {
                node.hj_JoinState = HJ_NEED_NEW_BATCH;
            }
            return Ok(());
        }
    };

    // econtext->ecxt_outertuple was set by the get-tuple path (the outer slot).
    set_econtext_outertuple(node, node.hj_OuterTupleSlot, estate)?;
    node.hj_MatchedOuter = false;

    // Find the corresponding bucket for this tuple in the main hash table or
    // skew hash table.
    node.hj_CurHashValue = hashvalue;
    let batchno = nodeHash::exec_hash_get_bucket_and_batch::call(node, hashvalue);
    node.hj_CurSkewBucketNo = nodeHash::exec_hash_get_skew_bucket::call(node, hashvalue);
    node.hj_CurTuple = None;

    // The tuple might not belong to the current batch (where "current batch"
    // includes the skew buckets if any).
    let curbatch = hashtable(node).curbatch;
    if batchno != curbatch && node.hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO {
        // Need to postpone this outer tuple to a later batch. Save it in the
        // corresponding outer-batch file.
        debug_assert!(hashtable(node).parallel_state.is_none());
        debug_assert!(batchno > curbatch);
        save_outer_tuple_to_batch(node, hashvalue, batchno, estate)?;
        // Loop around, staying in HJ_NEED_NEW_OUTER state.
        return Ok(());
    }

    // OK, let's scan the bucket for matches.
    node.hj_JoinState = HJ_SCAN_BUCKET;
    Ok(())
}

/// The `ExecProcNode` callback for the parallel-oblivious hash join.
fn exec_hash_join_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::HashJoin(node) => node,
        other => panic!("castNode(HashJoinState, pstate) failed: {other:?}"),
    };
    ExecHashJoinImpl(node, false, estate)
}

/// The `ExecProcNode` callback for the parallel-aware hash join.
fn exec_parallel_hash_join_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::HashJoin(node) => node,
        other => panic!("castNode(HashJoinState, pstate) failed: {other:?}"),
    };
    ExecHashJoinImpl(node, true, estate)
}

// ===========================================================================
// ExecInitHashJoin — init routine for HashJoin node.
// ===========================================================================

/// `ExecInitHashJoin(node, estate, eflags)`.
pub fn ExecInitHashJoin<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, HashJoinState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let hj: &'mcx HashJoin<'mcx> = node.expect_hashjoin();

    // check for unsupported flags
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // create state structure
    //   hjstate = makeNode(HashJoinState);
    //   hjstate->js.ps.plan = (Plan *) node;  hjstate->js.ps.state = estate;
    //   hjstate->js.ps.ExecProcNode = ExecHashJoin;
    //   hjstate->js.jointype = node->join.jointype;
    let mut hjstate = alloc_in(mcx, HashJoinState::default())?;
    hjstate.js.ps.plan = Some(node);
    hjstate.js.ps.ExecProcNode = Some(exec_hash_join_node);
    hjstate.js.jointype = hj.join.jointype;

    // Miscellaneous initialization: create expression context for node.
    execUtils::exec_assign_expr_context::call(estate, &mut hjstate.js.ps)?;

    // initialize child nodes
    //   outerNode = outerPlan(node); hashNode = (Hash *) innerPlan(node);
    //   outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
    //   innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);
    let outer_plan = hj.join.plan.lefttree.as_deref();
    hjstate.js.ps.lefttree = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    let outer_desc = exec_get_result_type_of(&hjstate.js.ps.lefttree, mcx, estate)?;

    let inner_plan = hj.join.plan.righttree.as_deref();
    hjstate.js.ps.righttree = execProcnode::exec_init_node::call(mcx, inner_plan, estate, eflags)?;
    let inner_desc = exec_get_result_type_of(&hjstate.js.ps.righttree, mcx, estate)?;

    // Initialize result slot, type and projection.
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut hjstate.js.ps,
        estate,
        TupleSlotKind::Virtual,
    )?;
    // C: ExecAssignProjectionInfo(&node->js.ps, NULL) — main's seam orders
    // (planstate, estate, input_desc); the hash-join node passes NULL inputDesc.
    execUtils::exec_assign_projection_info::call(&mut hjstate.js.ps, estate, None)?;

    // tuple table initialization:
    //   ops = ExecGetResultSlotOps(outerPlanState(hjstate), NULL);
    //   hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc, ops);
    let ops = match hjstate.js.ps.lefttree.as_deref() {
        Some(child) => execTuples::exec_get_result_slot_ops::call(child.ps_head()),
        None => TupleSlotKind::Virtual,
    };
    hjstate.hj_OuterTupleSlot =
        Some(execTuples::exec_init_extra_tuple_slot::call(estate, clone_desc(&outer_desc, mcx)?, ops)?);

    // detect whether we need only consider the first matching inner tuple
    hjstate.js.single_match =
        hj.join.inner_unique || hj.join.jointype == JoinType::JOIN_SEMI;

    // set up null tuples for outer joins, if needed
    match hj.join.jointype {
        JoinType::JOIN_INNER | JoinType::JOIN_SEMI | JoinType::JOIN_RIGHT_SEMI => {}
        JoinType::JOIN_LEFT | JoinType::JOIN_ANTI => {
            hjstate.hj_NullInnerTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&inner_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);
        }
        JoinType::JOIN_RIGHT | JoinType::JOIN_RIGHT_ANTI => {
            hjstate.hj_NullOuterTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&outer_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);
        }
        JoinType::JOIN_FULL => {
            hjstate.hj_NullOuterTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&outer_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);
            hjstate.hj_NullInnerTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&inner_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);
        }
        other => {
            return Err(PgError::error(alloc::format!(
                "unrecognized join type: {}",
                other as u32
            )));
        }
    }

    // now for some voodoo: our temporary tuple slot is actually the result tuple
    // slot of the Hash node (our inner plan). The inner HashState's result slot
    // becomes hj_HashTupleSlot. -cim 6/9/91
    hjstate.hj_HashTupleSlot = match hjstate.js.ps.righttree.as_deref() {
        Some(child) => child.ps_head().ps_ResultTupleSlot,
        None => None,
    };

    // Build ExprStates to obtain hash values for either side of the join,
    // walking node->hashoperators (get_op_hash_functions / op_strict per clause),
    // then set up the skew table hash function.
    build_hash_exprs(&mut hjstate, hj, estate)?;

    // initialize child expressions
    execExpr::exec_init_hashjoin_qual::call(&mut hjstate, execExpr::HashJoinQualKind::Qual, estate)?;
    execExpr::exec_init_hashjoin_qual::call(
        &mut hjstate,
        execExpr::HashJoinQualKind::JoinQual,
        estate,
    )?;
    execExpr::exec_init_hashjoin_qual::call(
        &mut hjstate,
        execExpr::HashJoinQualKind::HashClauses,
        estate,
    )?;

    // initialize hash-specific info
    hjstate.hj_HashTable = None;
    hjstate.hj_FirstOuterTupleSlot = None;

    hjstate.hj_CurHashValue = 0;
    hjstate.hj_CurBucketNo = 0;
    hjstate.hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    hjstate.hj_CurTuple = None;

    hjstate.hj_JoinState = HJ_BUILD_HASHTABLE;
    hjstate.hj_MatchedOuter = false;
    hjstate.hj_OuterNotEmpty = false;

    Ok(hjstate)
}

/// Build the per-side hash-value `ExprState`s and the skew hash function — the
/// `ExecInitHashJoin` "voodoo" block (nodeHashjoin.c:819-911). This walks the
/// plan's `hashoperators` to resolve the per-clause LHS/RHS hash functions and
/// strictness (lsyscache), then compiles the outer/inner hash ExprStates and
/// sets up the skew hash function (both owned by nodeHash since they write the
/// HashState). All of this — the per-clause loop and the field plumbing — is
/// the hash join's own init logic.
/// Materialize a `List` of expression `Node`s (the planner's `hashkeys`) into a
/// `Vec<Expr>` for `ExecBuildHash32Expr`. Each hash key is an expression node
/// (`Node::Expr`); anything else is a planner invariant violation.
fn node_list_to_exprs<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    nodes: Option<&[types_nodes::nodes::Node<'mcx>]>,
) -> PgResult<PgVec<'mcx, Expr>> {
    let nodes = nodes.unwrap_or(&[]);
    let mut out: PgVec<'mcx, Expr> = vec_with_capacity_in(mcx, nodes.len())?;
    for n in nodes {
        match n.as_expr() {
            Some(e) => out.push(e.clone()),
            None => panic!("hashkeys list element is not an expression node: {n:?}"),
        }
    }
    Ok(out)
}

fn build_hash_exprs<'mcx>(
    hjstate: &mut HashJoinState<'mcx>,
    hj: &HashJoin<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let nkeys = hj.hashoperators.len();

    // outer_hashfuncid = palloc_array(Oid, nkeys); etc.
    let mut outer_hashfuncid: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, nkeys)?;
    let mut inner_hashfuncid: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, nkeys)?;
    let mut hash_strict: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, nkeys)?;

    // Determine the hash function for each side of the join for each operator.
    for &hashop in hj.hashoperators.iter() {
        match lsyscache::get_op_hash_functions::call(hashop)? {
            Some((lhs, rhs)) => {
                outer_hashfuncid.push(lhs);
                inner_hashfuncid.push(rhs);
            }
            None => {
                return Err(PgError::error(alloc::format!(
                    "could not find hash function for hash operator {hashop}"
                )));
            }
        }
        // hash_strict[i] = op_strict(hashop);
        hash_strict.push(lsyscache::op_strict::call(hashop)?);
    }

    // Collations for ExecBuildHash32Expr (one per clause).
    let collations: &[Oid] = &hj.hashcollations;

    // The per-side hash key expression lists: `node->hashkeys` (outer) and
    // `hash->hashkeys` (the inner Hash plan), each a `List` of expression Nodes.
    let outer_keys: PgVec<'mcx, Expr> = node_list_to_exprs(mcx, hj.hashkeys.as_deref())?;
    let inner_keys: PgVec<'mcx, Expr> = {
        let inner_hash_keys = match hj.join.plan.righttree.as_deref() {
            Some(p) => p.expect_hash().hashkeys.as_deref(),
            None => None,
        };
        node_list_to_exprs(mcx, inner_hash_keys)?
    };

    // Build the outer-side hash ExprState (keep_nulls = HJ_FILL_OUTER).
    let fill_outer = HJ_FILL_OUTER(hjstate);
    let outer_hash = nodeHash::exec_build_hash32_expr::call(
        hjstate,
        true,
        &outer_hashfuncid,
        collations,
        &outer_keys,
        &hash_strict,
        fill_outer,
        estate,
    )?;
    hjstate.hj_OuterHash = Some(outer_hash);

    // Build the inner-side hash ExprState (keep_nulls = HJ_FILL_INNER), stored
    // below on the inner HashState's `hash_expr` (the C `hashstate->hash_expr`).
    let fill_inner = HJ_FILL_INNER(hjstate);
    let inner_hash = nodeHash::exec_build_hash32_expr::call(
        hjstate,
        false,
        &inner_hashfuncid,
        collations,
        &inner_keys,
        &hash_strict,
        fill_inner,
        estate,
    )?;
    // hashstate->hash_expr = ExecBuildHash32Expr(...); store on the inner
    // HashState (`(HashState *) innerPlanState(hjstate)`).
    match hjstate.js.ps.righttree.as_deref_mut() {
        Some(PlanStateNode::Hash(h)) => h.hash_expr = Some(inner_hash),
        _ => panic!("innerPlanState(HashJoin) is not a HashState"),
    }

    // Set up the skew table hash function from the first key's hash function:
    //   if (OidIsValid(hash->skewTable)) { skew_hashfunction = palloc0(FmgrInfo);
    //     skew_collation = linitial_oid(node->hashcollations);
    //     fmgr_info(outer_hashfuncid[0], skew_hashfunction); }
    // The skewTable validity is read off the inner Hash plan by the owner; pass
    // the first key's resolved hash function and collation.
    let skew_hashfuncid = outer_hashfuncid.first().copied().unwrap_or(0);
    let skew_collation = collations.first().copied().unwrap_or(0);
    // `if (OidIsValid(hash->skewTable))` — `hash = (Hash *) innerPlan(node)`;
    // gate on the inner Hash plan's real `skewTable` OID (InvalidOid when skew
    // optimization is disabled). The owner sets up the skew hash function from
    // `outer_hashfuncid[0]` / `linitial_oid(hashcollations)` only when valid.
    let skew_table = match hj.join.plan.righttree.as_deref() {
        Some(p) => p.expect_hash().skewTable,
        None => 0,
    };
    nodeHash::setup_skew_hashfunction::call(
        hjstate,
        skew_hashfuncid,
        skew_collation,
        OidIsValid(skew_table),
        estate,
    )?;

    Ok(())
}

/// `ExecGetResultType(childPlanState)` via the execTuples seam, threading the
/// optional child (`None` → an empty descriptor). Helper so the init path reads
/// both child result types without holding two borrows.
fn exec_get_result_type_of<'mcx>(
    child: &Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    mcx: Mcx<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    match child.as_deref() {
        // main's `exec_get_result_type` borrows the node's own result descriptor
        // (`&PlanStateData` → `Option<&TupleDescData>`); clone it into `mcx` so the
        // owned model can seed several slots from the same source type (C reuses
        // the shared `TupleDesc *`).
        Some(c) => match execTuples::exec_get_result_type::call(c.ps_head()) {
            Some(d) => Ok(Some(alloc_in(mcx, d.clone_in(mcx)?)?)),
            None => Ok(None),
        },
        None => Ok(None),
    }
}

/// Deep-copy a tuple descriptor into `mcx` (so the same source type can seed
/// several owned slots, as C re-uses the shared `TupleDesc` pointer).
fn clone_desc<'mcx>(
    desc: &types_tuple::heaptuple::TupleDesc<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    match desc {
        Some(d) => Ok(Some(alloc_in(mcx, d.as_ref().clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// `ExecEndHashJoin(node)` — clean up routine for the HashJoin node.
pub fn ExecEndHashJoin<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Free hash table
    if node.hj_HashTable.is_some() {
        nodeHash::exec_hash_table_destroy::call(node)?;
        // (destroy seam clears node.hj_HashTable to None)
    }

    // clean up subtrees
    if let Some(outer) = node.js.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    if let Some(inner) = node.js.ps.righttree.as_deref_mut() {
        execProcnode::exec_end_node::call(inner, estate)?;
    }
    Ok(())
}

// ===========================================================================
// ExecHashJoinOuterGetTuple / parallel variant.
// ===========================================================================

/// `ExecHashJoinOuterGetTuple(outerNode, hjstate, hashvalue)` — get the next
/// outer tuple for a parallel-oblivious hashjoin (from the outer plan node in
/// the first pass, or from the temp files for later batches). Returns the
/// tuple's hash value (the tuple lands in `hj_OuterTupleSlot` / the outer slot),
/// or `None` at end of batch.
fn ExecHashJoinOuterGetTuple<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u32>> {
    let curbatch = hashtable(node).curbatch;

    if curbatch == 0 {
        // if it is the first pass: check whether the first outer tuple was
        // already fetched by ExecHashJoin() and not used yet.
        let mut have_slot = if let Some(id) = node.hj_FirstOuterTupleSlot {
            node.hj_FirstOuterTupleSlot = None;
            Some(id)
        } else {
            fetch_outer_slot(node, estate)?
        };

        while let Some(slot) = have_slot {
            // We have to compute the tuple's hash value.
            set_econtext_outertuple(node, Some(slot), estate)?;
            execUtils::reset_per_tuple_expr_context::call(estate, &node.js.ps)?;

            let mut isnull = false;
            let hashvalue = execExpr::eval_outer_hash::call(node, &mut isnull, estate)?;

            if !isnull {
                // remember outer relation is not empty for possible rescan
                node.hj_OuterNotEmpty = true;
                node.hj_OuterTupleSlot = Some(slot);
                return Ok(Some(hashvalue));
            }

            // That tuple couldn't match because of a NULL; discard and continue.
            have_slot = fetch_outer_slot(node, estate)?;
        }
    } else if curbatch < hashtable(node).nbatch {
        // In outer-join cases, we could get here even though the batch file is
        // empty (file == NULL → return NULL).
        if hashtable(node).outerBatchFile[curbatch as usize].is_some() {
            let outer_slot = node
                .hj_OuterTupleSlot
                .expect("nodeHashjoin: hj_OuterTupleSlot must be set up by init");
            if let Some(hashvalue) =
                ExecHashJoinGetSavedTuple(node, BatchFileSide::Outer, curbatch, outer_slot, estate)?
            {
                return Ok(Some(hashvalue));
            }
        }
    }

    // End of this batch
    Ok(None)
}

/// `slot = ExecProcNode(outerNode)` then test `TupIsNull` — fetch one outer
/// tuple, returning its slot id or `None` for end/empty.
fn fetch_outer_slot<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let outer = outer_plan_state(node);
    let slot = execProcnode::exec_proc_node::call(outer, estate)?;
    match slot {
        Some(id) if !estate.slot(id).is_empty() => Ok(Some(id)),
        _ => Ok(None),
    }
}

/// `ExecParallelHashJoinOuterGetTuple` — the parallel variant.
fn ExecParallelHashJoinOuterGetTuple<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u32>> {
    let curbatch = hashtable(node).curbatch;

    // In the Parallel Hash case we only run the outer plan directly for
    // single-batch hash joins. Otherwise we go to batch files, even for batch 0.
    if curbatch == 0 && hashtable(node).nbatch == 1 {
        let mut have_slot = fetch_outer_slot(node, estate)?;

        while let Some(slot) = have_slot {
            set_econtext_outertuple(node, Some(slot), estate)?;
            execUtils::reset_per_tuple_expr_context::call(estate, &node.js.ps)?;

            let mut isnull = false;
            let hashvalue = execExpr::eval_outer_hash::call(node, &mut isnull, estate)?;

            if !isnull {
                node.hj_OuterTupleSlot = Some(slot);
                return Ok(Some(hashvalue));
            }

            // NULL hash value: discard and continue with the next one.
            have_slot = fetch_outer_slot(node, estate)?;
        }
    } else if curbatch < hashtable(node).nbatch {
        // tuple = sts_parallel_scan_next(batches[curbatch].outer_tuples, &hashvalue)
        let mcx = estate.es_query_cxt;
        let next = {
            let ht = hashtable_mut(node);
            let acc = ht.batches[curbatch as usize]
                .outer_tuples
                .as_deref_mut()
                .expect("nodeHashjoin: parallel batch outer_tuples must be attached");
            sts::sts_parallel_scan_next::call(mcx, acc)?
        };
        if let Some((blob, hashvalue)) = next {
            // ExecForceStoreMinimalTuple(tuple, hj_OuterTupleSlot, false)
            let outer_slot = node
                .hj_OuterTupleSlot
                .expect("nodeHashjoin: hj_OuterTupleSlot must be set up by init");
            let tuple = mintuple_from_flat(mcx, &blob)?;
            execTuples::exec_force_store_minimal_tuple::call(outer_slot, tuple, false, estate)?;
            return Ok(Some(hashvalue));
        } else {
            // ExecClearTuple(hj_OuterTupleSlot)
            if let Some(id) = node.hj_OuterTupleSlot {
                execTuples::exec_clear_tuple::call(estate, id)?;
            }
        }
    }

    // End of this batch
    hashtable_mut(node).batches[curbatch as usize].outer_eof = true;
    Ok(None)
}

// ===========================================================================
// ExecHashJoinNewBatch / ExecParallelHashJoinNewBatch.
// ===========================================================================

/// `ExecHashJoinNewBatch(hjstate)` — switch to a new hashjoin batch. Returns
/// true if successful, false if there are no more batches.
fn ExecHashJoinNewBatch<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let nbatch = hashtable(node).nbatch;
    let mut curbatch = hashtable(node).curbatch;

    if curbatch > 0 {
        // We no longer need the previous outer batch file; close it right away
        // to free disk space.
        if let Some(file) = hashtable_mut(node).outerBatchFile[curbatch as usize].take() {
            buffile::buf_file_close::call(file)?;
        }
    } else {
        // we just finished the first batch: reset the skew optimization state,
        // since we no longer need skew tuples after the first batch (the memory
        // context reset about to happen releases the skew hashtable itself).
        let ht = hashtable_mut(node);
        ht.skewEnabled = false;
        ht.nSkewBuckets = 0;
        ht.spaceUsedSkew = 0;
    }

    // We can always skip over batches that are completely empty on both sides.
    // We can sometimes skip over batches empty on only one side, with the
    // exceptions (rules 1-3) the C comment spells out.
    curbatch += 1;
    while curbatch < nbatch
        && (hashtable(node).outerBatchFile[curbatch as usize].is_none()
            || hashtable(node).innerBatchFile[curbatch as usize].is_none())
    {
        let has_outer = hashtable(node).outerBatchFile[curbatch as usize].is_some();
        let has_inner = hashtable(node).innerBatchFile[curbatch as usize].is_some();
        if has_outer && HJ_FILL_OUTER(node) {
            break; // rule 1
        }
        if has_inner && HJ_FILL_INNER(node) {
            break; // rule 1
        }
        if has_inner && nbatch != hashtable(node).nbatch_original {
            break; // rule 2
        }
        if has_outer && nbatch != hashtable(node).nbatch_outstart {
            break; // rule 3
        }
        // We can ignore this batch. Release associated temp files right away.
        if let Some(file) = hashtable_mut(node).innerBatchFile[curbatch as usize].take() {
            buffile::buf_file_close::call(file)?;
        }
        if let Some(file) = hashtable_mut(node).outerBatchFile[curbatch as usize].take() {
            buffile::buf_file_close::call(file)?;
        }
        curbatch += 1;
    }

    if curbatch >= nbatch {
        return Ok(false); // no more batches
    }

    hashtable_mut(node).curbatch = curbatch;

    // Reload the hash table with the new inner batch (which could be empty).
    nodeHash::exec_hash_table_reset::call(node)?;

    let has_inner = hashtable(node).innerBatchFile[curbatch as usize].is_some();
    if has_inner {
        // Rewind the inner file, then re-insert every saved tuple.
        {
            let ht = hashtable_mut(node);
            let file = ht.innerBatchFile[curbatch as usize]
                .as_deref_mut()
                .expect("checked Some above");
            if buffile::buf_file_seek::call(file, 0, 0, SEEK_SET)? != 0 {
                return Err(could_not_rewind_temp_file());
            }
        }

        let hash_tuple_slot = node
            .hj_HashTupleSlot
            .expect("nodeHashjoin: hj_HashTupleSlot must be set up by init");
        // NOTE: some tuples may be sent to future batches; nbatch may increase.
        while let Some(hashvalue) =
            ExecHashJoinGetSavedTuple(node, BatchFileSide::Inner, curbatch, hash_tuple_slot, estate)?
        {
            nodeHash::exec_hash_table_insert::call(node, hash_tuple_slot, hashvalue, estate)?;
        }

        // After building the hash table, the inner batch file is no longer
        // needed.
        if let Some(file) = hashtable_mut(node).innerBatchFile[curbatch as usize].take() {
            buffile::buf_file_close::call(file)?;
        }
    }

    // Rewind outer batch file (if present), so that we can start reading it.
    if hashtable(node).outerBatchFile[curbatch as usize].is_some() {
        let ht = hashtable_mut(node);
        let file = ht.outerBatchFile[curbatch as usize]
            .as_deref_mut()
            .expect("checked Some above");
        if buffile::buf_file_seek::call(file, 0, 0, SEEK_SET)? != 0 {
            return Err(could_not_rewind_temp_file());
        }
    }

    Ok(true)
}

fn could_not_rewind_temp_file() -> PgError {
    PgError::error("could not rewind hash-join temporary file")
}

/// `ExecParallelHashJoinNewBatch(hjstate)` — choose a batch to work on and
/// attach to it. Returns true if successful, false if there are no more.
fn ExecParallelHashJoinNewBatch<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // If we were already attached to a batch, remember not to bother checking it
    // again, and detach from it (possibly freeing the hash table if we are last
    // to detach).
    if nodeHash::parallel_has_curbatch::call(node) {
        let curbatch = hashtable(node).curbatch;
        nodeHash::parallel_batch_set_done::call(node, curbatch)?;
        nodeHash::exec_hash_table_detach_batch::call(node)?;
    }

    // Search for a batch that isn't done. The atomic counter starts our search
    // at a different batch in every participant.
    let nbatch = hashtable(node).nbatch;
    let start_batchno = (nodeHash::parallel_distributor_next_start::call(node) % nbatch as u32) as i32;
    let mut batchno = start_batchno;

    loop {
        if nodeHash::parallel_batch_not_done::call(node, batchno) {
            match nodeHash::parallel_batch_attach::call(node, batchno) {
                PHJ_BATCH_ELECT => {
                    // One backend allocates the hash table.
                    if nodeHash::parallel_batch_arrive_and_wait::call(
                        node,
                        batchno,
                        WAIT_EVENT_HASH_BATCH_ELECT,
                    )? {
                        nodeHash::exec_parallel_hash_table_alloc::call(node, batchno)?;
                    }
                    // Fall through.
                    parallel_batch_allocate_load_probe(node, batchno, true, estate)?;
                    return Ok(true);
                }
                PHJ_BATCH_ALLOCATE => {
                    // Wait for allocation to complete; fall through.
                    parallel_batch_allocate_load_probe(node, batchno, true, estate)?;
                    return Ok(true);
                }
                PHJ_BATCH_LOAD => {
                    // Start (or join in) loading tuples; fall through.
                    parallel_batch_load_probe(node, batchno, estate)?;
                    return Ok(true);
                }
                PHJ_BATCH_PROBE => {
                    // This batch is ready to probe. Return control to caller; we
                    // stay attached to batch_barrier.
                    nodeHash::exec_parallel_hash_table_set_current_batch::call(node, batchno)?;
                    let ht = hashtable_mut(node);
                    let acc = ht.batches[batchno as usize]
                        .outer_tuples
                        .as_deref_mut()
                        .expect("nodeHashjoin: parallel batch outer_tuples must be attached");
                    sts::sts_begin_parallel_scan::call(acc)?;
                    return Ok(true);
                }
                PHJ_BATCH_SCAN => {
                    // For now, we just detach and go around again. We use
                    // ExecHashTableDetachBatch() because there's a chance we'll
                    // be the last to detach, and then we free memory.
                    nodeHash::exec_parallel_hash_table_set_current_batch::call(node, batchno)?;
                    nodeHash::parallel_batch_set_done::call(node, batchno)?;
                    nodeHash::exec_hash_table_detach_batch::call(node)?;
                }
                PHJ_BATCH_FREE => {
                    // Already done. Detach and go around again (if any remain).
                    nodeHash::parallel_batch_detach::call(node, batchno)?;
                    nodeHash::parallel_batch_set_done::call(node, batchno)?;
                    nodeHash::parallel_set_curbatch_invalid::call(node)?;
                }
                other => {
                    // elog(ERROR, "unexpected batch phase %d",
                    //   BarrierPhase(batch_barrier)) — `other` is the phase
                    // BarrierAttach just returned for this batch.
                    return Err(PgError::error(alloc::format!(
                        "unexpected batch phase {other}"
                    )));
                }
            }
        }
        batchno = (batchno + 1) % hashtable(node).nbatch;
        if batchno == start_batchno {
            break;
        }
    }

    Ok(false)
}

/// PHJ_BATCH_ALLOCATE fall-through: wait for allocation, then load + begin
/// probe. `from_elect` is true when arriving from PHJ_BATCH_ELECT (the
/// allocation wait is the same in both).
fn parallel_batch_allocate_load_probe<'mcx>(
    node: &mut HashJoinState<'mcx>,
    batchno: i32,
    _from_elect: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // case PHJ_BATCH_ALLOCATE: BarrierArriveAndWait(..., HASH_BATCH_ALLOCATE);
    nodeHash::parallel_batch_arrive_and_wait::call(node, batchno, WAIT_EVENT_HASH_BATCH_ALLOCATE)?;
    parallel_batch_load_probe(node, batchno, estate)
}

/// PHJ_BATCH_LOAD fall-through into PHJ_BATCH_PROBE: load tuples from the inner
/// shared tuplestore into the current batch, then begin the outer probe scan.
fn parallel_batch_load_probe<'mcx>(
    node: &mut HashJoinState<'mcx>,
    batchno: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
    nodeHash::exec_parallel_hash_table_set_current_batch::call(node, batchno)?;

    let hash_tuple_slot = node
        .hj_HashTupleSlot
        .expect("nodeHashjoin: hj_HashTupleSlot must be set up by init");
    let mcx = estate.es_query_cxt;

    // sts_begin_parallel_scan(inner_tuples);
    {
        let ht = hashtable_mut(node);
        let acc = ht.batches[batchno as usize]
            .inner_tuples
            .as_deref_mut()
            .expect("nodeHashjoin: parallel batch inner_tuples must be attached");
        sts::sts_begin_parallel_scan::call(acc)?;
    }

    // while ((tuple = sts_parallel_scan_next(inner_tuples, &hashvalue))) {
    //     ExecForceStoreMinimalTuple(tuple, hj_HashTupleSlot, false);
    //     ExecParallelHashTableInsertCurrentBatch(hashtable, slot, hashvalue); }
    loop {
        let next = {
            let ht = hashtable_mut(node);
            let acc = ht.batches[batchno as usize]
                .inner_tuples
                .as_deref_mut()
                .expect("checked above");
            sts::sts_parallel_scan_next::call(mcx, acc)?
        };
        let (blob, hashvalue) = match next {
            Some(t) => t,
            None => break,
        };
        let tuple = mintuple_from_flat(mcx, &blob)?;
        execTuples::exec_force_store_minimal_tuple::call(hash_tuple_slot, tuple, false, estate)?;
        nodeHash::exec_parallel_hash_table_insert_current_batch::call(
            node,
            hash_tuple_slot,
            hashvalue,
            estate,
        )?;
    }

    // sts_end_parallel_scan(inner_tuples);
    {
        let ht = hashtable_mut(node);
        let acc = ht.batches[batchno as usize]
            .inner_tuples
            .as_deref_mut()
            .expect("checked above");
        sts::sts_end_parallel_scan::call(acc)?;
    }

    // BarrierArriveAndWait(batch_barrier, HASH_BATCH_LOAD);
    nodeHash::parallel_batch_arrive_and_wait::call(node, batchno, WAIT_EVENT_HASH_BATCH_LOAD)?;

    // Fall through into PHJ_BATCH_PROBE.
    nodeHash::exec_parallel_hash_table_set_current_batch::call(node, batchno)?;
    let ht = hashtable_mut(node);
    let acc = ht.batches[batchno as usize]
        .outer_tuples
        .as_deref_mut()
        .expect("nodeHashjoin: parallel batch outer_tuples must be attached");
    sts::sts_begin_parallel_scan::call(acc)?;
    Ok(())
}

// ===========================================================================
// ExecHashJoinSaveTuple / ExecHashJoinGetSavedTuple — the batch-file byte
// format (this crate's own logic).
// ===========================================================================

/// Which per-batch file array to address.
#[derive(Clone, Copy)]
enum BatchFileSide {
    Inner,
    Outer,
}

/// `ExecHashJoinSaveTuple(tuple, hashvalue, fileptr, hashtable)` — save the
/// outer tuple in `econtext`/the outer slot to the outer-batch file for
/// `batchno`. The data recorded for each tuple is its hash value, then the
/// tuple in MinimalTuple format. The batch file is lazily created in the
/// hashtable's `spillCxt`.
fn save_outer_tuple_to_batch<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hashvalue: u32,
    batchno: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // mintuple = ExecFetchSlotMinimalTuple(outerTupleSlot, &shouldFree);
    let outer_slot = node
        .hj_OuterTupleSlot
        .expect("nodeHashjoin: hj_OuterTupleSlot must be set up by init");
    let mcx = estate.es_query_cxt;
    let (mintuple, _should_free) =
        execTuples::exec_fetch_slot_minimal_tuple::call(mcx, estate, outer_slot)?;

    ExecHashJoinSaveTuple(mcx, node, BatchFileSide::Outer, batchno, &mintuple, hashvalue)?;
    // shouldFree → heap_free_minimal_tuple(mintuple): the minimal tuple was
    // copied into `mcx` (the query context) by the fetch seam; it is freed with
    // the context. No explicit free needed in the owned model.
    Ok(())
}

/// The byte-level save: `BufFileWrite(file, &hashvalue, 4); BufFileWrite(file,
/// tuple, tuple->t_len)`, lazily creating the file in `spillCxt`.
fn ExecHashJoinSaveTuple<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut HashJoinState<'mcx>,
    side: BatchFileSide,
    batchno: i32,
    tuple: &types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
    hashvalue: u32,
) -> PgResult<()> {
    // The batch file is lazily created in spillCxt (NOT batchCxt) on the first
    // tuple written, so its buffer outlives the per-batch context.
    let needs_create = {
        let ht = hashtable(node);
        batch_file(ht, side, batchno).is_none()
    };
    if needs_create {
        let spill = hashtable(node).spillCxt;
        let file = buffile::buf_file_create_temp::call(spill, false)?;
        *batch_file_mut(hashtable_mut(node), side, batchno) = Some(file);
    }

    // The recorded tuple is its contiguous C MinimalTuple byte image (the flat
    // blob, `tuple->t_len` bytes).
    let bytes = mintuple_to_flat(mcx, tuple)?;
    let ht = hashtable_mut(node);
    let file = batch_file_mut(ht, side, batchno)
        .as_deref_mut()
        .expect("file created above");
    buffile::buf_file_write::call(file, &hashvalue.to_ne_bytes())?;
    buffile::buf_file_write::call(file, &bytes)?;
    Ok(())
}

/// `ExecHashJoinGetSavedTuple(hjstate, file, hashvalue, tupleSlot)` — read the
/// next tuple from a batch file into `tuple_slot`. Returns the tuple's hash
/// value, or `None` at end of file.
///
/// The on-disk record is `[uint32 hashvalue][uint32 t_len][t_len-4 tuple body]`
/// — both leading words are `uint32`, so the C reads them together; here the
/// two `read` seam calls cover the same bytes.
fn ExecHashJoinGetSavedTuple<'mcx>(
    node: &mut HashJoinState<'mcx>,
    side: BatchFileSide,
    batchno: i32,
    tuple_slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u32>> {
    // We check for interrupts here because this is taken as an alternative code
    // path to an ExecProcNode() call (which would include such a check).
    tcop_postgres::check_for_interrupts::call()?;

    // Read the two-word header (hashvalue, t_len). eofOK: a clean EOF returns 0.
    let mut header = [0u8; 8];
    let nread = {
        let ht = hashtable_mut(node);
        let file = batch_file_mut(ht, side, batchno)
            .as_deref_mut()
            .expect("nodeHashjoin: batch file must exist on the read path");
        buffile::buf_file_read_maybe_eof::call(file, &mut header, true)?
    };
    if nread == 0 {
        // end of file
        execTuples::exec_clear_tuple::call(estate, tuple_slot)?;
        return Ok(None);
    }

    let hashvalue = u32::from_ne_bytes([header[0], header[1], header[2], header[3]]);
    let t_len = u32::from_ne_bytes([header[4], header[5], header[6], header[7]]);

    // tuple = (MinimalTuple) palloc(t_len); tuple->t_len = t_len;
    // BufFileReadExact(file, (char*)tuple + 4, t_len - 4);
    //
    // Read the rest of the flat MinimalTuple image directly into one blob whose
    // leading word is t_len (the on-disk record is exactly the flat C image), so
    // the body reads into blob[4..].
    let mcx = estate.es_query_cxt;
    let mut blob: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, t_len as usize)?;
    blob.resize(t_len as usize, 0u8);
    blob[0..4].copy_from_slice(&t_len.to_ne_bytes());
    {
        let ht = hashtable_mut(node);
        let file = batch_file_mut(ht, side, batchno)
            .as_deref_mut()
            .expect("file checked above");
        buffile::buf_file_read_exact::call(file, &mut blob[4..])?;
    }

    // Decode the flat MinimalTuple image and
    // ExecForceStoreMinimalTuple(tuple, tupleSlot, true).
    let mtup = mintuple_from_flat(mcx, &blob)?;
    execTuples::exec_force_store_minimal_tuple::call(tuple_slot, mtup, true, estate)?;
    Ok(Some(hashvalue))
}

/// `hashtable->innerBatchFile[batchno]` / `outerBatchFile[batchno]` accessor.
#[inline]
fn batch_file<'a, 'mcx>(
    ht: &'a HashJoinTableData<'mcx>,
    side: BatchFileSide,
    batchno: i32,
) -> &'a Option<PgBox<'mcx, BufFile>> {
    match side {
        BatchFileSide::Inner => &ht.innerBatchFile[batchno as usize],
        BatchFileSide::Outer => &ht.outerBatchFile[batchno as usize],
    }
}

#[inline]
fn batch_file_mut<'a, 'mcx>(
    ht: &'a mut HashJoinTableData<'mcx>,
    side: BatchFileSide,
    batchno: i32,
) -> &'a mut Option<PgBox<'mcx, BufFile>> {
    match side {
        BatchFileSide::Inner => &mut ht.innerBatchFile[batchno as usize],
        BatchFileSide::Outer => &mut ht.outerBatchFile[batchno as usize],
    }
}

// ===========================================================================
// ExecReScanHashJoin / ExecShutdownHashJoin / ExecParallelHashJoinPartitionOuter.
// ===========================================================================

/// `ExecReScanHashJoin(node)` — rescan the hash join.
pub fn ExecReScanHashJoin<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // In a multi-batch join, we currently rescan the hard way. But if it's a
    // single-batch join with no parameter change for the inner subnode, we can
    // re-use the existing hash table without rebuilding it.
    if node.hj_HashTable.is_some() {
        let single_batch = hashtable(node).nbatch == 1;
        let inner_chgparam_null = inner_chgparam_is_null(node);
        if single_batch && inner_chgparam_null {
            // Okay to reuse the hash table; needn't rescan inner. But for a
            // right/right-anti/right-semi/full join, reset the inner-tuple match
            // flags in the table.
            if HJ_FILL_INNER(node) || node.js.jointype == JoinType::JOIN_RIGHT_SEMI {
                nodeHash::exec_hash_table_reset_match_flags::call(node)?;
            }

            // Reset our state about the emptiness of the outer relation so the
            // new scan updates it correctly if it turns out empty this time.
            node.hj_OuterNotEmpty = false;

            // ExecHashJoin can skip the BUILD_HASHTABLE step.
            node.hj_JoinState = HJ_NEED_NEW_OUTER;
        } else {
            // must destroy and rebuild hash table. Accumulate stats from the old
            // hash table (if wanted) and clear the child plan node's pointer
            // (this should match ExecShutdownHash).
            nodeHash::exec_hash_accum_instrumentation::call(node)?;

            nodeHash::exec_hash_table_destroy::call(node)?;
            node.hj_JoinState = HJ_BUILD_HASHTABLE;

            // if chgParam of subnode is not null then plan will be re-scanned by
            // first ExecProcNode.
            if inner_chgparam_is_null(node) {
                if let Some(inner) = node.js.ps.righttree.as_deref_mut() {
                    execAmi::exec_re_scan::call(inner, estate)?;
                }
            }
        }
    }

    // Always reset intra-tuple state
    node.hj_CurHashValue = 0;
    node.hj_CurBucketNo = 0;
    node.hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node.hj_CurTuple = None;

    node.hj_MatchedOuter = false;
    node.hj_FirstOuterTupleSlot = None;

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    if outer_chgparam_is_null(node) {
        if let Some(outer) = node.js.ps.lefttree.as_deref_mut() {
            execAmi::exec_re_scan::call(outer, estate)?;
        }
    }
    Ok(())
}

/// `innerPlan(node)->chgParam == NULL`.
#[inline]
fn inner_chgparam_is_null(node: &HashJoinState) -> bool {
    node.js
        .ps
        .righttree
        .as_deref()
        .map(|c| c.ps_head().chgParam.is_none())
        .unwrap_or(true)
}

/// `outerPlan(node)->chgParam == NULL`.
#[inline]
fn outer_chgparam_is_null(node: &HashJoinState) -> bool {
    node.js
        .ps
        .lefttree
        .as_deref()
        .map(|c| c.ps_head().chgParam.is_none())
        .unwrap_or(true)
}

/// `ExecShutdownHashJoin(node)` — detach from shared state before DSM memory
/// goes away.
pub fn ExecShutdownHashJoin<'mcx>(node: &mut HashJoinState<'mcx>) -> PgResult<()> {
    if node.hj_HashTable.is_some() {
        // Detach from shared state before DSM memory goes away, so we don't hold
        // pointers into DSM memory by the time ExecEndHashJoin runs.
        nodeHash::exec_hash_table_detach_batch::call(node)?;
        nodeHash::exec_hash_table_detach::call(node)?;
    }
    Ok(())
}

/// `ExecParallelHashJoinPartitionOuter(hjstate)` — execute the outer plan,
/// writing all tuples to shared tuplestores partitioned by batch.
pub fn ExecParallelHashJoinPartitionOuter<'mcx>(
    node: &mut HashJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(node.hj_FirstOuterTupleSlot.is_none());

    // Execute outer plan, writing all tuples to shared tuplestores.
    loop {
        let slot = {
            let outer = outer_plan_state(node);
            execProcnode::exec_proc_node::call(outer, estate)?
        };
        let slot = match slot {
            Some(id) if !estate.slot(id).is_empty() => id,
            _ => break,
        };
        set_econtext_outertuple(node, Some(slot), estate)?;
        execUtils::reset_per_tuple_expr_context::call(estate, &node.js.ps)?;

        let mut isnull = false;
        let hashvalue = execExpr::eval_outer_hash::call(node, &mut isnull, estate)?;

        if !isnull {
            // mintup = ExecFetchSlotMinimalTuple(slot, &shouldFree);
            let mcx = estate.es_query_cxt;
            let (mintup, _should_free) =
                execTuples::exec_fetch_slot_minimal_tuple::call(mcx, estate, slot)?;
            // The shared tuplestore stores the tuple's contiguous C MinimalTuple
            // byte image (the flat blob).
            let blob = mintuple_to_flat(mcx, &mintup)?;
            // ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);
            let batchno = nodeHash::exec_hash_get_bucket_and_batch::call(node, hashvalue);
            // sts_puttuple(batches[batchno].outer_tuples, &hashvalue, mintup);
            let ht = hashtable_mut(node);
            let acc = ht.batches[batchno as usize]
                .outer_tuples
                .as_deref_mut()
                .expect("nodeHashjoin: parallel batch outer_tuples must be attached");
            sts::sts_puttuple::call(acc, hashvalue, &blob)?;
            // shouldFree → freed with the query context (owned-model).
        }
        tcop_postgres::check_for_interrupts::call()?;
    }

    // Make sure all outer partitions are readable by any backend.
    let nbatch = hashtable(node).nbatch;
    for i in 0..nbatch {
        let ht = hashtable_mut(node);
        let acc = ht.batches[i as usize]
            .outer_tuples
            .as_deref_mut()
            .expect("nodeHashjoin: parallel batch outer_tuples must be attached");
        sts::sts_end_write::call(acc)?;
    }
    Ok(())
}

// ===========================================================================
// Parallel DSM hooks.
// ===========================================================================

/// `state->js.ps.plan->plan_node_id` — the toc key the shared
/// `ParallelHashJoinState` is registered under.
#[inline]
fn hashjoin_plan_node_id(node: &HashJoinState) -> i32 {
    node.js
        .ps
        .plan
        .map(|n| n.plan_head().plan_node_id)
        .expect("HashJoinState.js.ps.plan")
}

/// `hashNode = (HashState *) innerPlanState(state)` — the inner Hash node's
/// executor state, whose `parallel_state` the hash join wires to the shared
/// object it places/looks-up.
#[inline]
fn inner_hash_state<'a, 'mcx>(
    node: &'a mut HashJoinState<'mcx>,
) -> &'a mut types_nodes::nodehash::HashState<'mcx> {
    match node.js.ps.righttree.as_deref_mut() {
        Some(PlanStateNode::Hash(h)) => h,
        Some(other) => panic!("innerPlanState(HashJoin) is not a Hash node: {other:?}"),
        None => panic!("innerPlanState(HashJoin) is NULL"),
    }
}

/// `ExecHashJoinEstimate(state, pcxt)` — estimate DSM space for the shared
/// `ParallelHashJoinState`. Mirrors the C exactly:
///
/// ```c
/// shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelHashJoinState));
/// shm_toc_estimate_keys(&pcxt->estimator, 1);
/// ```
///
/// The chunk size is the typed-shared-DSM-object primitive's
/// [`shared_dsm_object::estimate`] (`size_of::<ParallelHashJoinState>()`, with
/// `BUFFERALIGN` left to `shm_toc_allocate`); the reservation is recorded
/// through the `ParallelContext`'s estimator (parallel.c owner, via its seam).
pub fn ExecHashJoinEstimate(
    _node: &mut HashJoinState<'_>,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    let estimator = parallel_sup::pcxt_estimator(pcxt);
    parallel_sup::shm_toc_estimate_chunk(
        estimator,
        shared_dsm_object::estimate::<ParallelHashJoinState>(),
    );
    parallel_sup::shm_toc_estimate_keys(estimator, 1);
    Ok(())
}

/// `ExecHashJoinInitializeDSM(state, pcxt)` — set up the shared hash-join state.
///
/// ```c
/// if (pcxt->seg == NULL) return;
/// ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
/// pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelHashJoinState));
/// shm_toc_insert(pcxt->toc, plan_node_id, pstate);
/// pstate->nbatch = 0; ...; pg_atomic_init_u32(&pstate->distributor, 0);
/// pstate->nparticipants = pcxt->nworkers + 1; pstate->total_tuples = 0;
/// LWLockInitialize(&pstate->lock, LWTRANCHE_PARALLEL_HASH_JOIN);
/// BarrierInit(&pstate->build_barrier, 0); ...;
/// SharedFileSetInit(&pstate->fileset, pcxt->seg);
/// hashNode = (HashState *) innerPlanState(state);
/// hashNode->parallel_state = pstate;
/// ```
///
/// The `ParallelHashJoinState` is placed DIRECTLY in the DSM chunk through the
/// typed-shared-DSM-object primitive ([`shared_dsm_object::place_and_init_mut`]),
/// so every worker that `shm_toc_lookup`s/attaches sees the SAME cross-process
/// object — its `lock`/`build_barrier`/`grow_*_barrier`/`distributor`/`fileset`
/// are the real shared primitives. `BarrierInit`/`LWLockInitialize`/
/// `SharedFileSetInit` go through their owners' seams.
pub fn ExecHashJoinInitializeDSM(
    node: &mut HashJoinState<'_>,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    let plan_node_id = hashjoin_plan_node_id(node);

    // Disable shared hash table mode if we failed to create a real DSM segment,
    // because that means that we don't have a DSA area to work with.
    //   if (pcxt->seg == NULL) return;
    let seg = match parallel_sup::pcxt_seg(pcxt) {
        Some(seg) => seg,
        None => return Ok(()),
    };

    // ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
    node.js.ps.ExecProcNode = Some(exec_parallel_hash_join_node);

    // Set up the state needed to coordinate access to the shared hash table(s),
    // using the plan node ID as the toc key.
    //   pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelHashJoinState));
    let nworkers = parallel_sup::pcxt_nworkers(pcxt);
    let toc = parallel_sup::pcxt_toc(pcxt);
    let chunk = parallel_sup::shm_toc_allocate(
        toc,
        shared_dsm_object::estimate::<ParallelHashJoinState>(),
    );

    // Placement-init the shared ParallelHashJoinState in the DSM chunk; the
    // leader is the sole writer until the launch barrier releases. No `unsafe`
    // in this crate — the keystone hands us a plain `&mut`.
    //
    // `SharedFileSetInit` is fallible (its `FileSetInit`/`on_dsm_detach` paths
    // allocate); the `place_and_init_mut` init closure is infallible, so capture
    // its result in `init_result` and propagate it after the closure returns
    // (the closure runs synchronously to completion before then).
    let mut init_result: PgResult<()> = Ok(());
    shared_dsm_object::place_and_init_mut::<ParallelHashJoinState>(
        seg,
        chunk,
        |pstate: &mut ParallelHashJoinState| {
            // Set up the shared hash join state with no batches initially.
            // ExecHashTableCreate() will prepare at least one later and set
            // nbatch and space_allowed.
            pstate.nbatch = 0;
            pstate.space_allowed = 0;
            pstate.batches = types_execparallel::INVALID_DSA_POINTER;
            pstate.old_batches = types_execparallel::INVALID_DSA_POINTER;
            pstate.nbuckets = 0;
            pstate.growth = types_nodes::nodehash::ParallelHashGrowth::PHJ_GROWTH_OK;
            pstate.chunk_work_queue = types_execparallel::INVALID_DSA_POINTER;
            // pg_atomic_init_u32(&pstate->distributor, 0);
            pstate.distributor =
                types_storage::storage::pg_atomic_uint32::new(0);
            // pstate->nparticipants = pcxt->nworkers + 1;
            pstate.nparticipants = nworkers + 1;
            pstate.total_tuples = 0;
            // old_nbatch is not set by C here (left from the chunk); mirror C by
            // matching its untouched-by-init scalar — but a fresh chunk is
            // un-zeroed, so set it deterministically to 0 (C relies on it being
            // written before use by ExecParallelHashIncreaseNumBatches).
            pstate.old_nbatch = 0;
            // LWLockInitialize(&pstate->lock, LWTRANCHE_PARALLEL_HASH_JOIN);
            lwlock::lwlock_initialize::call(
                &mut pstate.lock,
                types_storage::storage::LWTRANCHE_PARALLEL_HASH_JOIN,
            );
            // BarrierInit(&pstate->build_barrier, 0);
            barrier::BarrierInit::call(&mut pstate.build_barrier, 0);
            // BarrierInit(&pstate->grow_batches_barrier, 0);
            barrier::BarrierInit::call(&mut pstate.grow_batches_barrier, 0);
            // BarrierInit(&pstate->grow_buckets_barrier, 0);
            barrier::BarrierInit::call(&mut pstate.grow_buckets_barrier, 0);
            // Set up the space we'll use for shared temporary files.
            //   SharedFileSetInit(&pstate->fileset, pcxt->seg);
            init_result = sharedfileset::SharedFileSetInit::call(&mut pstate.fileset, seg);
        },
    );
    init_result?;

    // shm_toc_insert(pcxt->toc, plan_node_id, pstate);
    parallel_sup::shm_toc_insert(toc, plan_node_id as u64, chunk);

    // Initialize the shared state in the hash node.
    //   hashNode = (HashState *) innerPlanState(state);
    //   hashNode->parallel_state = pstate;
    // The hash node stores the shared object's in-segment address as its
    // `parallel_state` token (it resolves the same bytes back in nodeHash).
    inner_hash_state(node).parallel_state = Some(chunk.0 as types_execparallel::DsaPointer);
    Ok(())
}

/// `ExecHashJoinReInitializeDSM(state, pcxt)` — reset shared state before a
/// fresh scan.
///
/// ```c
/// if (pcxt->seg == NULL) return;
/// pstate = shm_toc_lookup(pcxt->toc, plan_node_id, false);
/// if (state->hj_HashTable != NULL) {
///     ExecHashTableDetachBatch(state->hj_HashTable);
///     ExecHashTableDetach(state->hj_HashTable);
/// }
/// SharedFileSetDeleteAll(&pstate->fileset);
/// BarrierInit(&pstate->build_barrier, 0);
/// ```
pub fn ExecHashJoinReInitializeDSM(
    node: &mut HashJoinState<'_>,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    let plan_node_id = hashjoin_plan_node_id(node);

    // Nothing to do if we failed to create a DSM segment.
    //   if (pcxt->seg == NULL) return;
    let seg = match parallel_sup::pcxt_seg(pcxt) {
        Some(seg) => seg,
        None => return Ok(()),
    };

    // pstate = shm_toc_lookup(pcxt->toc, plan_node_id, false);
    let toc = parallel_sup::pcxt_toc(pcxt);
    let chunk = parallel_sup::shm_toc_lookup(toc, plan_node_id as u64, false)
        .expect("ExecHashJoinReInitializeDSM: shm_toc_lookup(plan_node_id) missing");

    // It would be possible to reuse the shared hash table in single-batch cases
    // by resetting and fast-forwarding the barriers, but currently shared hash
    // tables are already freed by now (by the last participant to detach from
    // the batch). For now we don't try.

    // Detach, freeing any remaining shared memory.
    if node.hj_HashTable.is_some() {
        nodeHash::exec_hash_table_detach_batch::call(node)?;
        nodeHash::exec_hash_table_detach::call(node)?;
    }

    // Reset shared state via a unique `&mut` over the looked-up object: by now
    // every participant has detached from the previous generation, so the
    // leader is the sole accessor (mirrors C's plain `pstate->...` writes here).
    shared_dsm_object::with_mut::<ParallelHashJoinState, _>(seg, chunk, |pstate| {
        // Clear any shared batch files.
        //   SharedFileSetDeleteAll(&pstate->fileset);
        sharedfileset::SharedFileSetDeleteAll::call(&mut pstate.fileset)?;
        // Reset build_barrier to PHJ_BUILD_ELECT so we can go around again.
        //   BarrierInit(&pstate->build_barrier, 0);
        barrier::BarrierInit::call(&mut pstate.build_barrier, 0);
        Ok(())
    })
}

/// `ExecHashJoinInitializeWorker(state, pwcxt)` — attach a worker to the shared
/// state.
///
/// ```c
/// pstate = shm_toc_lookup(pwcxt->toc, plan_node_id, false);
/// SharedFileSetAttach(&pstate->fileset, pwcxt->seg);
/// hashNode = (HashState *) innerPlanState(state);
/// hashNode->parallel_state = pstate;
/// ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
/// ```
pub fn ExecHashJoinInitializeWorker(
    node: &mut HashJoinState<'_>,
    pwcxt: types_execparallel::ParallelWorkerContextHandle,
) -> PgResult<()> {
    let plan_node_id = hashjoin_plan_node_id(node);

    // pstate = shm_toc_lookup(pwcxt->toc, plan_node_id, false);
    let toc = parallel_sup::pwcxt_toc(pwcxt);
    let chunk = parallel_sup::shm_toc_lookup(toc, plan_node_id as u64, false)
        .expect("ExecHashJoinInitializeWorker: shm_toc_lookup(plan_node_id) missing");
    let seg = parallel_sup::pwcxt_seg(pwcxt);

    // Attach to the space for shared temporary files.
    //   SharedFileSetAttach(&pstate->fileset, pwcxt->seg);
    // The worker is attaching pre-launch, so a unique `&mut` over the
    // looked-up object is sound (it is the sole accessor in this window); this
    // is exactly the C `&pstate->fileset` pointer-arg shape. `SharedFileSetAttach`
    // goes through its owner's seam (`backend-storage-file-fileset`).
    shared_dsm_object::with_mut::<ParallelHashJoinState, _>(seg, chunk, |pstate| {
        sharedfileset::SharedFileSetAttach::call(&mut pstate.fileset, seg)
    })?;

    // Attach to the shared state in the hash node.
    //   hashNode = (HashState *) innerPlanState(state);
    //   hashNode->parallel_state = pstate;
    inner_hash_state(node).parallel_state = Some(chunk.0 as types_execparallel::DsaPointer);

    // ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
    node.js.ps.ExecProcNode = Some(exec_parallel_hash_join_node);
    Ok(())
}

// ---------------------------------------------------------------------------
// Seam shims installed into `backend-executor-nodeHashjoin-pq-seams`.
//
// `execParallel` dispatches the per-node parallel hooks generically, holding a
// `PlanState *` (here the opaque `PlanStateHandle`); the C `ExecHashJoinEstimate`
// etc. begin with the `(HashJoinState *) node` cast. Recovering the live
// `HashJoinState` from the handle is the executor's `PlanState`-pointer registry
// — that pointer table is the unported executor surface, so each shim performs
// the C cast through `resolve_hash_join_state` (which panics until that registry
// lands) and then runs the real, ported entry point above. Mirrors
// nodeAgg::aggapi / nodeHash::instrument.
// ---------------------------------------------------------------------------

/// `(HashJoinState *) node` — recover the live `HashJoinState` a
/// `PlanStateHandle` refers to. The executor's `PlanState` pointer registry that
/// backs this lookup is not yet ported.
fn resolve_hash_join_state<'mcx>(
    _node: types_execparallel::PlanStateHandle,
) -> &'mcx mut HashJoinState<'mcx> {
    panic!(
        "backend-executor-nodeHashjoin: resolving a PlanStateHandle to the live HashJoinState \
         needs the executor PlanState pointer registry (unported); the (HashJoinState *) node \
         cast in the ExecHashJoin* parallel hooks cannot run yet"
    );
}

/// Seam shim for `ExecHashJoinEstimate`.
fn exec_hashjoin_estimate_shim(
    node: types_execparallel::PlanStateHandle,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    ExecHashJoinEstimate(resolve_hash_join_state(node), pcxt)
}

/// Seam shim for `ExecHashJoinInitializeDSM`.
fn exec_hashjoin_initialize_dsm_shim(
    node: types_execparallel::PlanStateHandle,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    ExecHashJoinInitializeDSM(resolve_hash_join_state(node), pcxt)
}

/// Seam shim for `ExecHashJoinReInitializeDSM`.
fn exec_hashjoin_reinitialize_dsm_shim(
    node: types_execparallel::PlanStateHandle,
    pcxt: types_execparallel::ParallelContextHandle,
) -> PgResult<()> {
    ExecHashJoinReInitializeDSM(resolve_hash_join_state(node), pcxt)
}

/// Seam shim for `ExecHashJoinInitializeWorker`.
fn exec_hashjoin_initialize_worker_shim(
    node: types_execparallel::PlanStateHandle,
    pwcxt: types_execparallel::ParallelWorkerContextHandle,
) -> PgResult<()> {
    ExecHashJoinInitializeWorker(resolve_hash_join_state(node), pwcxt)
}
