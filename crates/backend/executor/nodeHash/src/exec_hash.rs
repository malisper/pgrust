//! Hash executor-node lifecycle and the `MultiExec` build entry points.

use mcx::{alloc_in, Mcx, PgBox};
use types_error::{PgError, PgResult};
use ::nodes::execnodes::{EStateData, PlanStateData};
use ::nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, TupleSlotKind};
use ::nodes::nodehash::{
    Hash, HashState, ParallelHashJoinState, PHJ_BUILD_ALLOCATE, PHJ_BUILD_FREE,
    PHJ_BUILD_HASH_INNER, PHJ_BUILD_HASH_OUTER, PHJ_BUILD_RUN, PHJ_GROW_BATCHES_ELECT,
    PHJ_GROW_BATCHES_PHASE, PHJ_GROW_BUCKETS_ELECT, PHJ_GROW_BUCKETS_PHASE,
};
use ::nodes::ParallelHashGrowth;
use ::nodes::nodes::Node;

use crate::hash_table::{ExecHashIncreaseNumBuckets, ExecHashTableInsert};
use crate::parallel::{
    ExecParallelHashEnsureBatchAccessors, ExecParallelHashIncreaseNumBatches,
    ExecParallelHashIncreaseNumBuckets, ExecParallelHashMergeCounters,
    ExecParallelHashTableInsert, ExecParallelHashTableSetCurrentBatch,
};
use crate::skew::{ExecHashGetSkewBucket, ExecHashSkewTableInsert};
use crate::INVALID_SKEW_BUCKET_NO;

use execAmi_seams as execAmi;
use instrument_seams as instrument;
use execExpr_seams as execExpr;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use barrier_seams as barrier;

/// `WAIT_EVENT_HASH_BUILD_ALLOCATE` (wait_event.h) — wait event passed to
/// `BarrierArriveAndWait` while everyone synchronizes after allocating the
/// shared hash table.
const WAIT_EVENT_HASH_BUILD_ALLOCATE: u32 = 0x0A00_0001;
/// `WAIT_EVENT_HASH_BUILD_HASH_INNER` (wait_event.h) — wait event for the
/// barrier that ends the inner build phase.
const WAIT_EVENT_HASH_BUILD_HASH_INNER: u32 = 0x0A00_0003;

/// `my_log2(num)` (`port/pg_bitutils.h`) — ceil(log2(num)); `num` is a positive
/// count. Mirrors `pg_ceil_log2_64`.
#[inline]
fn my_log2(num: i64) -> i32 {
    // my_log2 is "1 if num <= 1 else ceil(log2(num))" via pg_leftmost_one_pos.
    // pg_ceil_log2_64(num) = num <= 1 ? 0 : pg_leftmost_one_pos64(num - 1) + 1.
    if num <= 1 {
        0
    } else {
        (64 - ((num - 1) as u64).leading_zeros()) as i32
    }
}

/// Resolve the shared `ParallelHashJoinState` from a hash table's DSA-resident
/// `parallel_state` pointer. C reads `hashtable->parallel_state` directly (it
/// is already a backend-local `ParallelHashJoinState *` resolved by
/// `ExecHashTableCreate`); the owned table stores it as the shared
/// `dsa_pointer`, resolved through the DSA area's `dsa_get_address` (which
/// returns the real address — the seam panics until the DSA owner lands).
///
/// # Safety
/// The pointer addresses a live `ParallelHashJoinState` in the attached DSM
/// segment for as long as this backend is attached to the join (the C
/// invariant). The caller holds `&mut HashJoinTableData`, so no aliasing copy
/// of this resolution is live.
unsafe fn resolve_parallel_state<'a, 'mcx>(
    hashtable: &mut ::nodes::nodehash::HashJoinTableData<'mcx>,
) -> &'a mut ParallelHashJoinState {
    // `parallel_state` holds the backend-local address of the
    // ParallelHashJoinState in the DSM segment (C's `ParallelHashJoinState *`),
    // NOT a dsa_pointer — deref it directly via parallel::pstate_of rather than
    // resolving it through `dsa_get_address` (which would split the raw segment
    // address into a bogus segment-index/offset).
    crate::parallel::pstate_of(hashtable)
}

/// `ExecHash(PlanState *pstate)` (nodeHash.c:91) — the per-node executor
/// callback slot for a Hash node. Hash never returns single tuples this way;
/// it `elog(ERROR)`s, so the body returns `Err`.
pub fn ExecHash(_pstate: &mut PlanStateData<'_>) -> PgResult<()> {
    // elog(ERROR, "Hash node does not support ExecProcNode call convention");
    Err(PgError::error(
        "Hash node does not support ExecProcNode call convention",
    ))
}

/// `MultiExecHash(HashState *node)` (nodeHash.c:105) — build the hash table by
/// pulling every tuple from the outer (inner-relation) child. Dispatches to the
/// serial or parallel build. Returns the node's result (the C returns `Node *`,
/// always NULL here — the table is the side effect).
pub fn MultiExecHash<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    // must provide our own instrumentation support
    //   if (node->ps.instrument) InstrStartNode(node->ps.instrument);
    if let Some(instr) = node.ps.instrument.as_deref_mut() {
        instrument::instr_start_node::call(instr)?;
    }

    //   if (node->parallel_state != NULL) MultiExecParallelHash(node);
    //   else MultiExecPrivateHash(node);
    if node.parallel_state.is_some() {
        MultiExecParallelHash(mcx, node, estate)?;
    } else {
        MultiExecPrivateHash(mcx, node, estate)?;
    }

    // must provide our own instrumentation support
    //   if (node->ps.instrument)
    //       InstrStopNode(node->ps.instrument, node->hashtable->partialTuples);
    if node.ps.instrument.is_some() {
        let partial_tuples = node
            .hashtable
            .as_deref()
            .expect("MultiExecHash: node->hashtable is NULL")
            .partialTuples;
        let instr = node
            .ps
            .instrument
            .as_deref_mut()
            .expect("MultiExecHash: node->ps.instrument is NULL");
        instrument::instr_stop_node::call(instr, partial_tuples)?;
    }

    // We do not return the hash table directly because it's not a subtype of
    // Node, and so would violate the MultiExecProcNode API. ... return NULL;
    Ok(None)
}

/// `MultiExecPrivateHash(HashState *node)` (nodeHash.c:138) — the serial build:
/// fetch outer tuples and insert them into the private hash table.
pub fn MultiExecPrivateHash<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // get state info from node
    //   outerNode = outerPlanState(node);
    //   hashtable = node->hashtable;
    //   econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("MultiExecPrivateHash: node has no ps_ExprContext");

    // The hash_expr ExprState the bucket value is computed from.
    let hash_expr = node
        .hash_expr
        .as_deref_mut()
        .expect("MultiExecPrivateHash: node->hash_expr is NULL")
        as *mut ::nodes::execexpr::ExprState;

    // Get all tuples from the node below the Hash node and insert into the
    // hash table (or temp files).
    loop {
        //   slot = ExecProcNode(outerNode);
        //   if (TupIsNull(slot)) break;
        let slot_id = {
            let outer = node
                .ps
                .lefttree
                .as_deref_mut()
                .expect("MultiExecPrivateHash: outerPlanState is NULL");
            match execProcnode::exec_proc_node::call(outer, estate)? {
                Some(id) => id,
                None => break,
            }
        };
        if estate.slot(slot_id).is_empty() {
            break;
        }

        // We have to compute the hash value
        //   econtext->ecxt_outertuple = slot;
        //   ResetExprContext(econtext);
        {
            let ec = estate.ecxt_mut(econtext);
            ec.ecxt_outertuple = Some(slot_id);
            ec.ecxt_per_tuple_memory.reset();
        }

        //   hashdatum = ExecEvalExprSwitchContext(node->hash_expr, econtext,
        //                                         &isnull);
        // SAFETY: hash_expr aliases node->hash_expr (immutable for the call);
        // the seam takes &ExprState and &mut EStateData, disjoint from it.
        let (hashdatum, isnull) = {
            let expr = unsafe { &mut *hash_expr };
            execExpr::exec_eval_expr_switch_context::call(expr, econtext, estate)?
        };

        if !isnull {
            //   uint32 hashvalue = DatumGetUInt32(hashdatum);
            let hashvalue = hashdatum.as_u32();
            let hashtable = node
                .hashtable
                .as_deref_mut()
                .expect("MultiExecPrivateHash: node->hashtable is NULL");

            //   bucketNumber = ExecHashGetSkewBucket(hashtable, hashvalue);
            let bucket_number = ExecHashGetSkewBucket(hashtable, hashvalue);
            if bucket_number != INVALID_SKEW_BUCKET_NO {
                // It's a skew tuple, so put it into that hash table
                //   ExecHashSkewTableInsert(hashtable, slot, hashvalue,
                //                           bucketNumber);
                //   hashtable->skewTuples += 1;
                ExecHashSkewTableInsert(mcx, hashtable, estate, slot_id, hashvalue, bucket_number)?;
                hashtable.skewTuples += 1.0;
            } else {
                // Not subject to skew optimization, so insert normally
                //   ExecHashTableInsert(hashtable, slot, hashvalue);
                ExecHashTableInsert(mcx, hashtable, estate, slot_id, hashvalue)?;
            }
            //   hashtable->totalTuples += 1;
            hashtable.totalTuples += 1.0;
        }
    }

    let hashtable = node
        .hashtable
        .as_deref_mut()
        .expect("MultiExecPrivateHash: node->hashtable is NULL");

    // resize the hash table if needed (NTUP_PER_BUCKET exceeded)
    //   if (hashtable->nbuckets != hashtable->nbuckets_optimal)
    //       ExecHashIncreaseNumBuckets(hashtable);
    if hashtable.nbuckets != hashtable.nbuckets_optimal {
        ExecHashIncreaseNumBuckets(mcx, hashtable)?;
    }

    // Account for the buckets in spaceUsed (reported in EXPLAIN ANALYZE)
    //   hashtable->spaceUsed += hashtable->nbuckets * sizeof(HashJoinTuple);
    //   if (hashtable->spaceUsed > hashtable->spacePeak)
    //       hashtable->spacePeak = hashtable->spaceUsed;
    hashtable.spaceUsed += hashtable.nbuckets as usize
        * core::mem::size_of::<*const ::nodes::nodehash::HashJoinTupleData<'_>>();
    if hashtable.spaceUsed > hashtable.spacePeak {
        hashtable.spacePeak = hashtable.spaceUsed;
    }

    //   hashtable->partialTuples = hashtable->totalTuples;
    hashtable.partialTuples = hashtable.totalTuples;

    Ok(())
}

/// `MultiExecParallelHash(HashState *node)` (nodeHash.c:219) — the parallel
/// build: coordinate with peers through the build barrier and load tuples into
/// the shared hash table.
pub fn MultiExecParallelHash<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // get state info from node
    //   outerNode = outerPlanState(node);
    //   hashtable = node->hashtable;
    //   econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("MultiExecParallelHash: node has no ps_ExprContext");
    let hash_expr = node
        .hash_expr
        .as_deref_mut()
        .expect("MultiExecParallelHash: node->hash_expr is NULL")
        as *mut ::nodes::execexpr::ExprState;

    // Synchronize the parallel hash table build. ...
    //   pstate = hashtable->parallel_state;
    //   build_barrier = &pstate->build_barrier;
    //   Assert(BarrierPhase(build_barrier) >= PHJ_BUILD_ALLOCATE);
    let pstate: &mut ParallelHashJoinState = {
        let hashtable = node
            .hashtable
            .as_deref_mut()
            .expect("MultiExecParallelHash: node->hashtable is NULL");
        // SAFETY: resolves the live shared state for the duration of this build.
        unsafe { resolve_parallel_state(hashtable) }
    };

    debug_assert!(barrier::BarrierPhase::call(&pstate.build_barrier) >= PHJ_BUILD_ALLOCATE);

    // switch (BarrierPhase(build_barrier))
    let phase = barrier::BarrierPhase::call(&pstate.build_barrier);
    // PHJ_BUILD_ALLOCATE falls through to PHJ_BUILD_HASH_INNER; any other phase
    // (HASH_OUTER / RUN / FREE) skips the switch body entirely.
    if phase == PHJ_BUILD_ALLOCATE || phase == PHJ_BUILD_HASH_INNER {
        if phase == PHJ_BUILD_ALLOCATE {
            // case PHJ_BUILD_ALLOCATE:
            //   BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ALLOCATE);
            //   /* Fall through. */
            barrier::BarrierArriveAndWait::call(
                &mut pstate.build_barrier,
                WAIT_EVENT_HASH_BUILD_ALLOCATE,
            );
        }

        // case PHJ_BUILD_HASH_INNER:
        //   if (PHJ_GROW_BATCHES_PHASE(BarrierAttach(&pstate->grow_batches_barrier)) !=
        //       PHJ_GROW_BATCHES_ELECT)
        //       ExecParallelHashIncreaseNumBatches(hashtable);
        if PHJ_GROW_BATCHES_PHASE(barrier::BarrierAttach::call(&mut pstate.grow_batches_barrier))
            != PHJ_GROW_BATCHES_ELECT
        {
            let hashtable = node.hashtable.as_deref_mut().unwrap();
            ExecParallelHashIncreaseNumBatches(mcx, hashtable)?;
        }
        //   if (PHJ_GROW_BUCKETS_PHASE(BarrierAttach(&pstate->grow_buckets_barrier)) !=
        //       PHJ_GROW_BUCKETS_ELECT)
        //       ExecParallelHashIncreaseNumBuckets(hashtable);
        if PHJ_GROW_BUCKETS_PHASE(barrier::BarrierAttach::call(&mut pstate.grow_buckets_barrier))
            != PHJ_GROW_BUCKETS_ELECT
        {
            let hashtable = node.hashtable.as_deref_mut().unwrap();
            ExecParallelHashIncreaseNumBuckets(mcx, hashtable)?;
        }
        //   ExecParallelHashEnsureBatchAccessors(hashtable);
        //   ExecParallelHashTableSetCurrentBatch(hashtable, 0);
        {
            let hashtable = node.hashtable.as_deref_mut().unwrap();
            ExecParallelHashEnsureBatchAccessors(mcx, hashtable)?;
            ExecParallelHashTableSetCurrentBatch(hashtable, 0);
        }

        // for (;;)
        loop {
            //   slot = ExecProcNode(outerNode);
            //   if (TupIsNull(slot)) break;
            let slot_id = {
                let outer = node
                    .ps
                    .lefttree
                    .as_deref_mut()
                    .expect("MultiExecParallelHash: outerPlanState is NULL");
                match execProcnode::exec_proc_node::call(outer, estate)? {
                    Some(id) => id,
                    None => break,
                }
            };
            if estate.slot(slot_id).is_empty() {
                break;
            }

            //   econtext->ecxt_outertuple = slot;
            //   ResetExprContext(econtext);
            {
                let ec = estate.ecxt_mut(econtext);
                ec.ecxt_outertuple = Some(slot_id);
                ec.ecxt_per_tuple_memory.reset();
            }

            //   hashvalue = DatumGetUInt32(ExecEvalExprSwitchContext(node->hash_expr,
            //                                                        econtext, &isnull));
            // SAFETY: hash_expr aliases node->hash_expr (immutable for the call).
            let (hashdatum, isnull) = {
                let expr = unsafe { &mut *hash_expr };
                execExpr::exec_eval_expr_switch_context::call(expr, econtext, estate)?
            };
            let hashvalue = hashdatum.as_u32();

            //   if (!isnull) ExecParallelHashTableInsert(hashtable, slot, hashvalue);
            //   hashtable->partialTuples++;
            let hashtable = node.hashtable.as_deref_mut().unwrap();
            if !isnull {
                ExecParallelHashTableInsert(mcx, hashtable, estate, slot_id, hashvalue)?;
            }
            hashtable.partialTuples += 1.0;
        }

        // Make sure that any tuples we wrote to disk are visible to others
        // before anyone tries to load them.
        //   for (i = 0; i < hashtable->nbatch; ++i)
        //       sts_end_write(hashtable->batches[i].inner_tuples);
        {
            let hashtable = node.hashtable.as_deref_mut().unwrap();
            let nbatch = hashtable.nbatch;
            for i in 0..nbatch as usize {
                // sts_end_write(hashtable->batches[i].inner_tuples).
                let accessor = hashtable.batches[i]
                    .inner_tuples
                    .as_deref_mut()
                    .expect("MultiExecParallelHash: batch inner_tuples must be attached");
                sort_storage_seams::sts_end_write::call(accessor)?;
            }
        }

        // Update shared counters. ...
        //   ExecParallelHashMergeCounters(hashtable);
        {
            let hashtable = node.hashtable.as_deref_mut().unwrap();
            ExecParallelHashMergeCounters(hashtable);
        }

        //   BarrierDetach(&pstate->grow_buckets_barrier);
        //   BarrierDetach(&pstate->grow_batches_barrier);
        barrier::BarrierDetach::call(&mut pstate.grow_buckets_barrier);
        barrier::BarrierDetach::call(&mut pstate.grow_batches_barrier);

        // Wait for everyone to finish building and flushing files and counters.
        //   if (BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_HASH_INNER))
        //       pstate->growth = PHJ_GROWTH_DISABLED;
        if barrier::BarrierArriveAndWait::call(
            &mut pstate.build_barrier,
            WAIT_EVENT_HASH_BUILD_HASH_INNER,
        ) {
            // Elect one backend to disable any further growth.
            pstate.growth = ParallelHashGrowth::PHJ_GROWTH_DISABLED;
        }
    }

    // We're not yet attached to a batch. We all agree on the dimensions and
    // number of inner tuples (for the empty table optimization).
    //   hashtable->curbatch = -1;
    //   hashtable->nbuckets = pstate->nbuckets;
    //   hashtable->log2_nbuckets = my_log2(hashtable->nbuckets);
    //   hashtable->totalTuples = pstate->total_tuples;
    let pstate_nbuckets = pstate.nbuckets;
    let pstate_total_tuples = pstate.total_tuples;
    let build_phase = barrier::BarrierPhase::call(&pstate.build_barrier);
    {
        let hashtable = node.hashtable.as_deref_mut().unwrap();
        hashtable.curbatch = -1;
        hashtable.nbuckets = pstate_nbuckets;
        hashtable.log2_nbuckets = my_log2(hashtable.nbuckets as i64);
        hashtable.totalTuples = pstate_total_tuples as f64;
    }

    // Unless we're completely done and the batch state has been freed, make
    // sure we have accessors.
    //   if (BarrierPhase(build_barrier) < PHJ_BUILD_FREE)
    //       ExecParallelHashEnsureBatchAccessors(hashtable);
    if build_phase < PHJ_BUILD_FREE {
        let hashtable = node.hashtable.as_deref_mut().unwrap();
        ExecParallelHashEnsureBatchAccessors(mcx, hashtable)?;
    }

    // Assert(BarrierPhase(build_barrier) == PHJ_BUILD_HASH_OUTER ||
    //        BarrierPhase(build_barrier) == PHJ_BUILD_RUN ||
    //        BarrierPhase(build_barrier) == PHJ_BUILD_FREE);
    debug_assert!(
        build_phase == PHJ_BUILD_HASH_OUTER
            || build_phase == PHJ_BUILD_RUN
            || build_phase == PHJ_BUILD_FREE
    );

    Ok(())
}

/// `ExecInitHash(Hash *node, EState *estate, int eflags)` (nodeHash.c:370) —
/// initialize the Hash plan node into a `HashState`. Allocates node state in
/// `mcx`.
pub fn ExecInitHash<'mcx>(
    mcx: Mcx<'mcx>,
    node_enum: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, HashState<'mcx>>> {
    let node: &'mcx Hash<'mcx> = node_enum.expect_hash();
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // create state structure
    //   hashstate = makeNode(HashState);
    //   hashstate->ps.plan = (Plan *) node;
    //   hashstate->ps.state = estate;
    //   hashstate->ps.ExecProcNode = ExecHash;
    //   hashstate->hashtable = NULL;
    //
    // `ps.plan` aliases the read-only `Node::Hash` plan node (the borrow does
    // what C's `hashstate->ps.plan = (Plan *) node` does). The `ExecProcNode`
    // callback (`ExecHash`) is installed by the execProcnode owner when
    // `HashState` joins the `PlanStateNode` dispatch enum (its signature is the
    // error-stub, never actually dispatched — Hash runs via MultiExecProcNode).
    let mut hashstate = alloc_in(
        mcx,
        HashState {
            ps: PlanStateData::default(),
            hashtable: None,
            hash_expr: None,
            skew_hashfunction: None,
            skew_collation: types_core::InvalidOid,
            shared_info: None,
            hinstrument: None,
            parallel_state: None,
        },
    )?;

    hashstate.ps.plan = Some(node_enum);

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &hashstate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut hashstate.ps)?;

    // initialize child nodes
    //   outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = node.plan.lefttree.as_deref();
    hashstate.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // initialize our result slot and type. No need to build projection because
    // this node doesn't do projections.
    //   ExecInitResultTupleSlotTL(&hashstate->ps, &TTSOpsMinimalTuple);
    //   hashstate->ps.ps_ProjInfo = NULL;
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut hashstate.ps,
        estate,
        TupleSlotKind::MinimalTuple,
    )?;
    hashstate.ps.ps_ProjInfo = None;

    //   Assert(node->plan.qual == NIL);
    debug_assert!(node.plan.qual.as_ref().map_or(true, |q| q.is_empty()));

    // Delay initialization of hash_expr until ExecInitHashJoin().
    //   hashstate->hash_expr = NULL;
    hashstate.hash_expr = None;

    Ok(hashstate)
}

/// `ExecEndHash(HashState *node)` (nodeHash.c:427) — shut down the Hash node
/// (frees the expr context and recurses into the outer plan).
pub fn ExecEndHash<'mcx>(
    node: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // shut down the subplan
    //   outerPlan = outerPlanState(node);
    //   ExecEndNode(outerPlan);
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    Ok(())
}

/// `ExecReScanHash(HashState *node)` (nodeHash.c:2381) — rescan support; if the
/// child's chgParam is unchanged the subtree is rescanned.
pub fn ExecReScanHash<'mcx>(
    node: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   PlanState *outerPlan = outerPlanState(node);
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    if let Some(outer) = node.ps.lefttree.as_deref_mut() {
        if outer.ps_head().chgParam.is_none() {
            execAmi::exec_re_scan::call(outer, estate)?;
        }
    }
    Ok(())
}

/// `ExecShutdownHash(HashState *node)` (nodeHash.c:2831) — copy shared-memory
/// instrumentation to local storage before DSM shutdown.
pub fn ExecShutdownHash<'mcx>(mcx: Mcx<'mcx>, node: &mut HashState<'mcx>) -> PgResult<()> {
    // Allocate save space if EXPLAIN'ing and we didn't do so already
    //   if (node->ps.instrument && !node->hinstrument)
    //       node->hinstrument = palloc0_object(HashInstrumentation);
    if node.ps.instrument.is_some() && node.hinstrument.is_none() {
        node.hinstrument = Some(::nodes::nodehash::HashInstrumentSlot::Local(alloc_in(
            mcx,
            ::nodes::nodehash::HashInstrumentation::default(),
        )?));
    }

    // Now accumulate data for the current (final) hash table
    //   if (node->hinstrument && node->hashtable)
    //       ExecHashAccumInstrumentation(node->hinstrument, node->hashtable);
    if node.hinstrument.is_some() && node.hashtable.is_some() {
        let hashtable = node.hashtable.as_deref().unwrap();
        let slot = node.hinstrument.as_mut().unwrap();
        crate::instrument::with_hinstrument_mut(slot, |instrument| {
            crate::instrument::ExecHashAccumInstrumentation(instrument, hashtable);
        });
    }

    Ok(())
}

