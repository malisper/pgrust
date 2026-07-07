// nodeHashjoin.c parallel arms: ExecParallelHashJoin's specialization of the
// HJ state machine over the shared table, plus the PHJ batch selection and
// outer partitioning. The serial machine in lib.rs is untouched.

use ::execexpr::{exec_eval_expr, exec_qual, EvalSlots};
use ::executils::{EStateData, ExecSlotId};
use ::nodehash::parallel::{
    self as phj, ParallelHashJoinTable, PHJ_BATCH_ALLOCATE, PHJ_BATCH_ELECT, PHJ_BATCH_FREE,
    PHJ_BATCH_LOAD, PHJ_BATCH_PROBE, PHJ_BATCH_SCAN, PHJ_BUILD_FREE, PHJ_BUILD_HASH_OUTER,
    PHJ_BUILD_RUN,
};
use ::nodehash::{HashBuildInput, HashJoinTupleHdr, HashState};
use ::types_error::PgResult;
use ::types_nodes::JoinType;

use crate::{
    cfi, eval_probe_qual, HashJoinOuter, HashJoinState, HJ_BUILD_HASHTABLE,
    HJ_FILL_INNER_TUPLES, HJ_FILL_OUTER_TUPLE, HJ_NEED_NEW_BATCH, HJ_NEED_NEW_OUTER,
    HJ_SCAN_BUCKET,
};

// Body-duplicate of lib.rs's project_result: sharing it would add callsites
// to the serial hot path's helper and shift its inlining (M3 flatness gate).
fn project_result<'mcx>(
    node: &mut HashJoinState<'mcx>,
    inner_id: ExecSlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
    if node.proj.has_subplan() {
        let ecxt = node.ps_ExprContext;
        estate.ecxt_mut(ecxt).ecxt_innertuple = Some(inner_id);
        let result_id = node.ps_ResultTupleSlot;
        ::executils::exec_project_with_subplans(&mut node.proj, estate, ecxt, result_id)?;
        return Ok(result_id);
    }
    let mcx = estate.es_query_cxt;
    let outer_id = estate
        .ecxt(node.ps_ExprContext)
        .ecxt_outertuple
        .expect("hashjoin outer tuple set");
    let result_id = node.ps_ResultTupleSlot;
    let table = &mut estate.es_tupleTable[..];
    let [inner, outer, result] = table
        .get_disjoint_mut([inner_id.0 as usize, outer_id.0 as usize, result_id.0 as usize])
        .expect("distinct in-range hashjoin slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
    ::execexpr::exec_project(&mut node.proj, &mut slots, result, mcx)?;
    Ok(result_id)
}

/// `ExecParallelHashJoin` (`ExecHashJoinImpl(pstate, true)`).
/// Never inlined: the serial dispatch (hash_join_arm) must keep its
/// pre-parallel inlining budget so the serial join spine stays bit-flat.
#[inline(never)]
pub fn exec_parallel_hash_join<'mcx, O, C>(
    node: &mut HashJoinState<'mcx>,
    outer: &mut O,
    hash_state: &mut HashState<'mcx>,
    hash_child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>
where
    O: HashJoinOuter<'mcx>,
    C: HashBuildInput<'mcx>,
{
    loop {
        cfi()?;
        match node.hj_JoinState {
            HJ_BUILD_HASHTABLE => {
                debug_assert!(hash_state.ptable.is_none());
                // The empty-outer optimization is not implemented for shared
                // hash tables (C): always build.
                let mut table = phj::exec_parallel_hash_table_create(hash_state, estate)?;

                // MultiExecParallelHash, driven here because the dispatcher
                // owns the build child.
                let instr = node.hash_instr.map(|ix| ix as usize);
                if let Some(ix) = instr {
                    ::instrument::instr_start_node(&mut estate.es_instrumentation[ix]);
                }
                let ran_inner = phj::multi_exec_parallel_hash_begin(&mut table)?;
                if ran_inner {
                    loop {
                        let Some(slot_id) = hash_child.exec_proc(estate)? else {
                            break;
                        };
                        let hashvalue = hash_state.eval_build_hash(estate, slot_id)?;
                        let (ptr, len) = phj::slot_min_tuple_image(
                            estate,
                            slot_id,
                            hash_state.ps_ExprContext,
                        )?;
                        // SAFETY: per-tuple image, copied out by the insert
                        // before the next fetch.
                        let image = unsafe { core::slice::from_raw_parts(ptr, len) };
                        phj::exec_parallel_hash_table_insert(&mut table, image, hashvalue)?;
                        table.partial_tuples += 1.0;
                    }
                }
                phj::multi_exec_parallel_hash_finish(&mut table, ran_inner)?;
                if let Some(ix) = instr {
                    ::instrument::instr_stop_node(
                        &mut estate.es_instrumentation[ix],
                        table.partial_tuples,
                    );
                }

                let build_barrier = &hash_state
                    .parallel_state()
                    .expect("parallel hash state attached")
                    .build_barrier;
                if table.total_tuples == 0.0 && !node.hj_fill_outer {
                    // Advance to PHJ_BUILD_RUN so cleanup can be negotiated.
                    while build_barrier.phase() < PHJ_BUILD_RUN {
                        build_barrier.arrive_and_wait()?;
                    }
                    hash_state.ptable = Some(table);
                    return Ok(None);
                }

                node.hj_OuterNotEmpty = false;

                let phase = build_barrier.phase();
                if phase == PHJ_BUILD_HASH_OUTER {
                    // Multi-batch: hash the outer relation up front.
                    if table.nbatch > 1 {
                        partition_outer(node, outer, &mut table, estate)?;
                    }
                    build_barrier.arrive_and_wait()?;
                } else if phase == PHJ_BUILD_FREE {
                    // Attached too late; the job is already done.
                    hash_state.ptable = Some(table);
                    return Ok(None);
                }
                debug_assert!(build_barrier.phase() == PHJ_BUILD_RUN);
                table.curbatch = -1;
                hash_state.ptable = Some(table);
                node.hj_JoinState = HJ_NEED_NEW_BATCH;
            }
            HJ_NEED_NEW_OUTER => {
                let Some(hashvalue) = parallel_outer_get_tuple(node, outer, hash_state, estate)?
                else {
                    // End of batch (or whole join).
                    if node.hj_fill_inner {
                        if parallel_prep_unmatched(node, hash_state)? {
                            node.hj_JoinState = HJ_FILL_INNER_TUPLES;
                        } else {
                            node.hj_JoinState = HJ_NEED_NEW_BATCH;
                        }
                    } else {
                        node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    }
                    continue;
                };
                node.hj_MatchedOuter = false;
                node.hj_CurHashValue = hashvalue;
                let table = hash_state.ptable.as_ref().expect("shared table built");
                let (bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
                debug_assert!(batchno == table.curbatch);
                node.hj_CurBucketNo = bucketno;
                node.hj_CurTuple = core::ptr::null_mut();
                node.hj_JoinState = HJ_SCAN_BUCKET;
            }
            HJ_SCAN_BUCKET => {
                if !parallel_scan_hash_bucket(node, hash_state, estate)? {
                    node.hj_JoinState = HJ_FILL_OUTER_TUPLE;
                    continue;
                }
                // SAFETY: hj_CurTuple just set non-null by the bucket scan.
                if node.plan.join.jointype == JoinType::JOIN_RIGHT_SEMI
                    && unsafe {
                        (*HashJoinTupleHdr::mintuple(node.hj_CurTuple).as_ptr()).has_match()
                    }
                {
                    continue;
                }
                let ecxt = node.ps_ExprContext;
                let inner_id = hash_state.hash_tuple_slot;
                let matched =
                    eval_probe_qual(node.joinqual.as_deref_mut(), ecxt, inner_id, estate)?;
                if matched {
                    node.hj_MatchedOuter = true;
                    // SAFETY: as above; the racy read-check-set is C's
                    // (idempotent bit, only ever set during this phase).
                    unsafe {
                        let mt = HashJoinTupleHdr::mintuple(node.hj_CurTuple).as_ptr();
                        if !(*mt).has_match() {
                            (*mt).set_match();
                        }
                    }
                    if node.plan.join.jointype == JoinType::JOIN_ANTI {
                        node.hj_JoinState = HJ_NEED_NEW_OUTER;
                        continue;
                    }
                    if node.js_single_match {
                        node.hj_JoinState = HJ_NEED_NEW_OUTER;
                    }
                    if node.plan.join.jointype == JoinType::JOIN_RIGHT_ANTI {
                        continue;
                    }
                    let pass =
                        eval_probe_qual(node.otherqual.as_deref_mut(), ecxt, inner_id, estate)?;
                    if pass {
                        return Ok(Some(project_result(node, inner_id, estate)?));
                    }
                    estate.instr_count_filtered2(node.js_instr);
                } else {
                    estate.instr_count_filtered1(node.js_instr);
                }
            }
            HJ_FILL_OUTER_TUPLE => {
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
                if !node.hj_MatchedOuter && node.hj_fill_outer {
                    let null_inner = node.hj_NullInnerTupleSlot.expect("null inner slot");
                    estate.ecxt_mut(node.ps_ExprContext).ecxt_innertuple = Some(null_inner);
                    let ecxt = node.ps_ExprContext;
                    let pass =
                        eval_probe_qual(node.otherqual.as_deref_mut(), ecxt, null_inner, estate)?;
                    if pass {
                        return Ok(Some(project_result(node, null_inner, estate)?));
                    }
                    estate.instr_count_filtered2(node.js_instr);
                }
            }
            HJ_FILL_INNER_TUPLES => {
                if !parallel_scan_for_unmatched(node, hash_state, estate)? {
                    node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }
                let null_outer = node.hj_NullOuterTupleSlot.expect("null outer slot");
                estate.ecxt_mut(node.ps_ExprContext).ecxt_outertuple = Some(null_outer);
                let ecxt = node.ps_ExprContext;
                let inner_id = hash_state.hash_tuple_slot;
                let pass =
                    eval_probe_qual(node.otherqual.as_deref_mut(), ecxt, inner_id, estate)?;
                if pass {
                    return Ok(Some(project_result(node, inner_id, estate)?));
                }
                estate.instr_count_filtered2(node.js_instr);
            }
            HJ_NEED_NEW_BATCH => {
                if !parallel_new_batch(hash_state)? {
                    return Ok(None);
                }
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
            }
            other => panic!("ExecParallelHashJoin (nodeHashjoin.c): unrecognized state {other}"),
        }
    }
}

// ExecParallelHashJoinOuterGetTuple: the plan child only for single-batch
// batch 0; the batch's shared tuplestore otherwise.
fn parallel_outer_get_tuple<'mcx, O: HashJoinOuter<'mcx>>(
    node: &mut HashJoinState<'mcx>,
    outer: &mut O,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u32>> {
    let table = hash_state.ptable.as_mut().expect("shared table built");
    let curbatch = table.curbatch;
    let ecxt = node.ps_ExprContext;
    if curbatch == 0 && table.nbatch == 1 {
        if let Some(slot_id) = outer.exec_proc(estate)? {
            {
                let e = estate.ecxt_mut(ecxt);
                e.reset();
                e.ecxt_outertuple = Some(slot_id);
            }
            let h = match outer.staged_hash() {
                Some(h) => h,
                None if node.outer_hash_expr.has_subplan() => {
                    ::executils::exec_eval_expr_with_subplans_hashkey(
                        &mut node.outer_hash_expr,
                        estate,
                        ecxt,
                        slot_id,
                    )?
                    .value
                    .as_u32()
                }
                None => {
                    let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
                    let mut slots = EvalSlots { scan: None, inner: Some(slot), outer: None };
                    exec_eval_expr(&mut node.outer_hash_expr, &mut slots)?.value.as_u32()
                }
            };
            node.hj_OuterNotEmpty = true;
            return Ok(Some(h));
        }
    } else if curbatch < table.nbatch {
        let mut meta = [0u8; 4];
        if let Some(tuple) = table.outer_tuples(curbatch).parallel_scan_next(&mut meta)? {
            let mcx = estate.es_query_cxt;
            let oslot = node.hj_OuterTupleSlot;
            // SAFETY: the sts image is live until the next scan call, which
            // happens only after this outer tuple is fully processed.
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(
                    &mut estate.es_tupleTable[oslot.0 as usize],
                    mcx,
                    tuple,
                )
            };
            estate.reset_expr_context(ecxt);
            estate.ecxt_mut(ecxt).ecxt_outertuple = Some(oslot);
            return Ok(Some(u32::from_ne_bytes(meta)));
        }
    }
    table.set_outer_eof(curbatch);
    Ok(None)
}

// ExecParallelHashJoinPartitionOuter.
fn partition_outer<'mcx, O: HashJoinOuter<'mcx>>(
    node: &mut HashJoinState<'mcx>,
    outer: &mut O,
    table: &mut ParallelHashJoinTable<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ecxt = node.ps_ExprContext;
    loop {
        let Some(slot_id) = outer.exec_proc(estate)? else {
            break;
        };
        {
            let e = estate.ecxt_mut(ecxt);
            e.reset();
            e.ecxt_outertuple = Some(slot_id);
        }
        let hashvalue = match outer.staged_hash() {
            Some(h) => h,
            None if node.outer_hash_expr.has_subplan() => {
                ::executils::exec_eval_expr_with_subplans_hashkey(
                    &mut node.outer_hash_expr,
                    estate,
                    ecxt,
                    slot_id,
                )?
                .value
                .as_u32()
            }
            None => {
                let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
                let mut slots = EvalSlots { scan: None, inner: Some(slot), outer: None };
                exec_eval_expr(&mut node.outer_hash_expr, &mut slots)?.value.as_u32()
            }
        };
        let (ptr, len) = phj::slot_min_tuple_image(estate, slot_id, ecxt)?;
        // SAFETY: per-tuple image; put_tuple copies before the next fetch.
        let image = unsafe { core::slice::from_raw_parts(ptr, len) };
        let (_bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
        table
            .outer_tuples(batchno)
            .put_tuple(&hashvalue.to_ne_bytes(), image)?;
        cfi()?;
    }
    for i in 0..table.nbatch {
        table.outer_tuples(i).end_write()?;
    }
    Ok(())
}

// ExecParallelScanHashBucket.
fn parallel_scan_hash_bucket<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let table = hash_state.ptable.as_ref().expect("shared table built");
    let hashvalue = node.hj_CurHashValue;
    let mut cur: *mut HashJoinTupleHdr = if !node.hj_CurTuple.is_null() {
        table.next_tuple(node.hj_CurTuple)
    } else {
        table.first_tuple(node.hj_CurBucketNo)
    };

    if cur.is_null() {
        return Ok(false);
    }
    let hslot = hash_state.hash_tuple_slot;
    let mcx = estate.es_query_cxt;
    let ecxt = node.ps_ExprContext;
    if node.hashclauses.as_deref().is_some_and(|q| q.has_subplan()) {
        estate.ecxt_mut(ecxt).ecxt_innertuple = Some(hslot);
        while !cur.is_null() {
            // SAFETY: chain entries live in the shared arena for the batch.
            let hdr = unsafe { &*cur };
            if hdr.hashvalue() == hashvalue {
                let tuple = unsafe { HashJoinTupleHdr::mintuple(cur) };
                // SAFETY: entry image lives until the batch is freed.
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(
                        &mut estate.es_tupleTable[hslot.0 as usize],
                        mcx,
                        tuple,
                    )
                };
                if ::executils::exec_qual_with_subplans(
                    node.hashclauses.as_deref_mut(),
                    estate,
                    ecxt,
                )? {
                    node.hj_CurTuple = cur;
                    return Ok(true);
                }
            }
            cur = hdr.next();
        }
        return Ok(false);
    }
    let outer_id = estate
        .ecxt(ecxt)
        .ecxt_outertuple
        .expect("hashjoin outer tuple set");
    estate.ecxt_mut(ecxt).ecxt_innertuple = Some(hslot);
    let tbl = &mut estate.es_tupleTable[..];
    let [inner, outer] = tbl
        .get_disjoint_mut([hslot.0 as usize, outer_id.0 as usize])
        .expect("distinct in-range hashjoin slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };

    while !cur.is_null() {
        // SAFETY: chain entries live in the shared arena for the batch.
        let hdr = unsafe { &*cur };
        if hdr.hashvalue() == hashvalue {
            let tuple = unsafe { HashJoinTupleHdr::mintuple(cur) };
            let inner = slots.inner.as_deref_mut().expect("inner slot bound");
            // SAFETY: entry image lives until the batch is freed.
            unsafe { exectuples::exec_store_minimal_tuple_ptr(inner, mcx, tuple) };
            if exec_qual(node.hashclauses.as_deref_mut(), &mut slots)? {
                node.hj_CurTuple = cur;
                return Ok(true);
            }
        }
        cur = hdr.next();
    }
    Ok(false)
}

// ExecParallelPrepHashTableForUnmatched: wait-free election of the one
// participant allowed to run the unmatched scan.
fn parallel_prep_unmatched<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
) -> PgResult<bool> {
    let table = hash_state.ptable.as_mut().expect("shared table built");
    let curbatch = table.curbatch;
    debug_assert!(table.batch_barrier(curbatch).phase() == PHJ_BATCH_PROBE);
    if !table.batch_barrier(curbatch).arrive_and_detach_except_last() {
        phj::parallel_prep_unmatched_lose(table)?;
        return Ok(false);
    }
    debug_assert!(table.batch_barrier(curbatch).phase() == PHJ_BATCH_SCAN);
    if table.skip_unmatched(curbatch) {
        table.set_batch_done(curbatch);
        phj::exec_hash_table_detach_batch(table)?;
        return Ok(false);
    }
    node.hj_CurBucketNo = 0;
    node.hj_CurTuple = core::ptr::null_mut();
    Ok(true)
}

// ExecParallelScanHashTableForUnmatched.
fn parallel_scan_for_unmatched<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let table = hash_state.ptable.as_ref().expect("shared table built");
    let nbuckets = table.nbuckets;
    let mut cur: *mut HashJoinTupleHdr = if !node.hj_CurTuple.is_null() {
        table.next_tuple(node.hj_CurTuple)
    } else {
        core::ptr::null_mut()
    };
    loop {
        while cur.is_null() {
            if node.hj_CurBucketNo >= nbuckets {
                return Ok(false);
            }
            cur = table.first_tuple(node.hj_CurBucketNo);
            node.hj_CurBucketNo += 1;
        }
        // SAFETY: chain entries live in the shared arena for the batch.
        let (tuple, matched) = unsafe {
            let mt = HashJoinTupleHdr::mintuple(cur);
            (mt, (*mt.as_ptr()).has_match())
        };
        if !matched {
            let hslot = hash_state.hash_tuple_slot;
            let mcx = estate.es_query_cxt;
            // SAFETY: as above.
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(
                    &mut estate.es_tupleTable[hslot.0 as usize],
                    mcx,
                    tuple,
                )
            };
            estate.ecxt_mut(node.ps_ExprContext).ecxt_innertuple = Some(hslot);
            estate.reset_expr_context(node.ps_ExprContext);
            node.hj_CurTuple = cur;
            return Ok(true);
        }
        cur = table.next_tuple(cur);
        cfi()?;
    }
}

// ExecParallelHashJoinNewBatch.
fn parallel_new_batch<'mcx>(hash_state: &mut HashState<'mcx>) -> PgResult<bool> {
    let table = hash_state.ptable.as_mut().expect("shared table built");
    if table.curbatch >= 0 {
        let cur = table.curbatch;
        table.set_batch_done(cur);
        phj::exec_hash_table_detach_batch(table)?;
    }

    let start_batchno = table.distribute_batchno();
    let mut batchno = start_batchno;
    loop {
        if !table.batch_done(batchno) {
            let mut phase = table.batch_barrier(batchno).attach();
            if phase == PHJ_BATCH_ELECT {
                if table.batch_barrier(batchno).arrive_and_wait()? {
                    phj::exec_parallel_hash_table_alloc(table, batchno);
                }
                phase = PHJ_BATCH_ALLOCATE;
            }
            if phase == PHJ_BATCH_ALLOCATE {
                table.batch_barrier(batchno).arrive_and_wait()?;
                phase = PHJ_BATCH_LOAD;
            }
            if phase == PHJ_BATCH_LOAD {
                phj::exec_parallel_hash_table_set_current_batch(table, batchno);
                table.inner_tuples(batchno).begin_parallel_scan()?;
                let mut meta = [0u8; 4];
                loop {
                    let Some(mt) = table.inner_tuples(batchno).parallel_scan_next(&mut meta)?
                    else {
                        break;
                    };
                    let hashvalue = u32::from_ne_bytes(meta);
                    // SAFETY: sts image is valid until the next scan call;
                    // the insert copies it into the shared chunk first.
                    let image = unsafe {
                        let t_len = (*mt.as_ptr()).t_len as usize;
                        core::slice::from_raw_parts(mt.as_ptr().cast::<u8>(), t_len)
                    };
                    phj::exec_parallel_hash_table_insert_current_batch(table, image, hashvalue)?;
                }
                table.inner_tuples(batchno).end_parallel_scan()?;
                table.batch_barrier(batchno).arrive_and_wait()?;
                phase = PHJ_BATCH_PROBE;
            }
            match phase {
                PHJ_BATCH_PROBE => {
                    // Ready to probe. Stay attached (no more waiting on this
                    // barrier is allowed once tuples flow).
                    phj::exec_parallel_hash_table_set_current_batch(table, batchno);
                    table.outer_tuples(batchno).begin_parallel_scan()?;
                    return Ok(true);
                }
                PHJ_BATCH_SCAN => {
                    // Another process owns the unmatched scan; move on (the
                    // detach may leave us responsible for freeing).
                    phj::exec_parallel_hash_table_set_current_batch(table, batchno);
                    table.set_batch_done(batchno);
                    phj::exec_hash_table_detach_batch(table)?;
                }
                PHJ_BATCH_FREE => {
                    table.batch_barrier(batchno).detach();
                    table.set_batch_done(batchno);
                    table.curbatch = -1;
                }
                other => panic!("unexpected batch phase {other}"),
            }
        }
        batchno = (batchno + 1) % table.nbatch;
        if batchno == start_batchno {
            break;
        }
    }
    Ok(false)
}
