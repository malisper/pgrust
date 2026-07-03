// nodeHashjoin.c serial state machine, all jointypes, single- and
// multi-batch; parallel is loud. Per-probe bucket scan is allocation-free.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{
    exec_build_hash32_from_attrs, exec_build_projection_info, exec_init_qual, exec_project,
    exec_eval_expr, exec_qual, EvalSlots, ExprState, ParamBind,
};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{PgBox, PgVec};
use ::nodehash::{HashBuildInput, HashJoinTupleHdr, HashState};
use ::types_error::PgResult;
use ::types_nodes::plannodes::HashJoin;
use ::types_nodes::JoinType;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

const HJ_BUILD_HASHTABLE: u8 = 1;
const HJ_NEED_NEW_OUTER: u8 = 2;
const HJ_SCAN_BUCKET: u8 = 3;
const HJ_FILL_OUTER_TUPLE: u8 = 4;
const HJ_FILL_INNER_TUPLES: u8 = 5;
const HJ_NEED_NEW_BATCH: u8 = 6;

#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

pub trait HashJoinOuter<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
}

pub struct HashJoinState<'mcx> {
    pub plan: &'mcx HashJoin<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    hashclauses: Option<PgBox<'mcx, ExprState<'mcx>>>,
    joinqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    otherqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    outer_hash_expr: PgBox<'mcx, ExprState<'mcx>>,
    js_single_match: bool,
    hj_fill_outer: bool,
    hj_fill_inner: bool,
    hj_NullInnerTupleSlot: Option<ExecSlotId>,
    hj_NullOuterTupleSlot: Option<ExecSlotId>,
    hj_OuterTupleSlot: ExecSlotId,
    hj_JoinState: u8,
    hj_CurHashValue: u32,
    hj_CurBucketNo: u32,
    hj_CurTuple: *mut HashJoinTupleHdr,
    hj_MatchedOuter: bool,
    hj_OuterNotEmpty: bool,
    outer_saved_scratch: PgVec<'mcx, u64>,
    inner_saved_scratch: PgVec<'mcx, u64>,
    hash_instr: Option<u32>,
}

/// `ExecInitHashJoin` minus child linkage.
#[allow(clippy::too_many_arguments)]
pub fn exec_init_hash_join<'mcx>(
    node: &'mcx HashJoin<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
    outer_desc: &Rc<TupleDescData<'static>>,
    inner_desc: Rc<TupleDescData<'static>>,
    init_hash: impl FnOnce(
        &mut EStateData<'mcx>,
        Rc<TupleDescData<'static>>,
        &[i16],
        &[::types_core::Oid],
        &[::types_core::Oid],
    ) -> PgResult<HashState<'mcx>>,
) -> PgResult<(HashJoinState<'mcx>, HashState<'mcx>)> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    assert!(
        matches!(
            node.join.jointype,
            JoinType::JOIN_INNER
                | JoinType::JOIN_LEFT
                | JoinType::JOIN_RIGHT
                | JoinType::JOIN_FULL
                | JoinType::JOIN_SEMI
                | JoinType::JOIN_ANTI
                | JoinType::JOIN_RIGHT_SEMI
                | JoinType::JOIN_RIGHT_ANTI
        ),
        "ExecInitHashJoin (nodeHashjoin.c): unrecognized join type {:?}",
        node.join.jointype
    );
    let mcx = estate.es_query_cxt;
    let hj_fill_outer = matches!(
        node.join.jointype,
        JoinType::JOIN_LEFT | JoinType::JOIN_ANTI | JoinType::JOIN_FULL
    );
    let hj_fill_inner = matches!(
        node.join.jointype,
        JoinType::JOIN_RIGHT | JoinType::JOIN_RIGHT_ANTI | JoinType::JOIN_FULL
    );
    let hj_NullInnerTupleSlot = if hj_fill_outer {
        Some(exec_init_null_tuple_slot(estate, inner_desc.clone()))
    } else {
        None
    };
    let hj_NullOuterTupleSlot = if hj_fill_inner {
        Some(exec_init_null_tuple_slot(estate, outer_desc.clone()))
    } else {
        None
    };
    let hj_OuterTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(outer_desc.clone()), TupleSlotKind::MinimalTuple);

    // get_op_hash_functions -> (outer_hashfn, inner_hashfn); outer is left.
    let n = node.hashoperators.len();
    let mut outer_hashfns: ::mcx::PgVec<'mcx, ::types_core::Oid> = ::mcx::PgVec::new_in(mcx);
    let mut inner_hashfns: ::mcx::PgVec<'mcx, ::types_core::Oid> = ::mcx::PgVec::new_in(mcx);
    let mut collations: ::mcx::PgVec<'mcx, ::types_core::Oid> = ::mcx::PgVec::new_in(mcx);
    for i in 0..n {
        let hashop = node.hashoperators.nth(i);
        let (left, right) = lsyscache::get_op_hash_functions(hashop)?
            .unwrap_or_else(|| panic!("ExecInitHashJoin: hash operator {hashop} lacks hash functions"));
        outer_hashfns.push(left);
        inner_hashfns.push(right);
        collations.push(node.hashcollations.nth(i));
    }

    let outer_attnums = hashkey_attnums(mcx, &node.hashkeys);
    let outer_hash_expr = exec_build_hash32_from_attrs(
        mcx,
        outer_desc,
        &outer_hashfns,
        &collations,
        &outer_attnums,
        0,
    )?;

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);
    let proj = exec_build_projection_info(mcx, &node.join.plan.targetlist, None, ParamBind::NONE)?;
    let hashclauses = exec_init_qual(mcx, &node.hashclauses, ParamBind::NONE)?;
    let joinqual = exec_init_qual(mcx, &node.join.joinqual, ParamBind::NONE)?;
    let otherqual = exec_init_qual(mcx, &node.join.plan.qual, ParamBind::NONE)?;

    // Inner keys + inner hash fns feed the Hash sub-node (C builds it here too).
    let hash_node = node
        .join
        .plan
        .righttree
        .expect("HashJoin without a Hash inner plan")
        .as_hash()
        .expect("HashJoin inner is a Hash node");
    let inner_attnums = hashkey_attnums(mcx, &hash_node.hashkeys);
    let hash_state = init_hash(estate, inner_desc, &inner_attnums, &inner_hashfns, &collations)?;

    // The Hash sub-node has no Instrumented wrapper; MultiExecHash provides
    // its own instrumentation over this slot.
    let hash_instr = if estate.es_instrument != 0 {
        let idx = usize::try_from(hash_node.plan.plan_node_id)
            .expect("plan_node_id is non-negative");
        if estate.es_instrumentation.len() <= idx {
            let grow = idx + 1 - estate.es_instrumentation.len();
            estate
                .es_instrumentation
                .try_reserve(grow)
                .map_err(|_| estate.es_query_cxt.oom(grow))?;
            estate
                .es_instrumentation
                .resize(idx + 1, ::types_core::instrument::Instrumentation::default());
        }
        ::instrument::instr_init(&mut estate.es_instrumentation[idx], estate.es_instrument);
        Some(idx as u32)
    } else {
        None
    };

    let hjstate = HashJoinState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        proj,
        hashclauses,
        joinqual,
        otherqual,
        outer_hash_expr,
        js_single_match: node.join.inner_unique
            || node.join.jointype == JoinType::JOIN_SEMI,
        hj_fill_outer,
        hj_fill_inner,
        hj_NullInnerTupleSlot,
        hj_NullOuterTupleSlot,
        hj_OuterTupleSlot,
        hj_JoinState: HJ_BUILD_HASHTABLE,
        hj_CurHashValue: 0,
        hj_CurBucketNo: 0,
        hj_CurTuple: core::ptr::null_mut(),
        hj_MatchedOuter: false,
        hj_OuterNotEmpty: false,
        outer_saved_scratch: PgVec::new_in(mcx),
        inner_saved_scratch: PgVec::new_in(mcx),
        hash_instr,
    };
    Ok((hjstate, hash_state))
}

// Hashkeys are simple Vars after setrefs; non-Var (expression) keys are loud.
fn hashkey_attnums<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    keys: &::types_nodes::list::NodeList<'_>,
) -> ::mcx::PgVec<'mcx, i16> {
    let mut out = ::mcx::PgVec::new_in(mcx);
    for k in keys.iter() {
        let v = k.as_var().unwrap_or_else(|| {
            panic!("ExecInitHashJoin (nodeHashjoin.c): non-Var hash key; expression-hash lane unported")
        });
        assert!(v.varattno > 0, "ExecInitHashJoin: whole-row/system hash key not ported");
        out.push(v.varattno);
    }
    out
}

/// `ExecHashJoin` (serial `ExecHashJoinImpl`).
pub fn exec_hash_join<'mcx, O, C>(
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
                debug_assert!(hash_state.table.is_none());
                hash_state.table = Some(::nodehash::exec_hash_table_create(hash_state, estate)?);
                // MultiExecHash provides its own instrumentation (the Hash
                // node has no ExecProcNode wrapper).
                let instr = node.hash_instr.map(|ix| ix as usize);
                if let Some(ix) = instr {
                    ::instrument::instr_start_node(&mut estate.es_instrumentation[ix]);
                }
                ::nodehash::multi_exec_hash(hash_state, hash_child, estate)?;
                if let Some(ix) = instr {
                    let ntuples =
                        hash_state.table.as_ref().expect("hash table built").total_tuples();
                    ::instrument::instr_stop_node(&mut estate.es_instrumentation[ix], ntuples);
                }
                let table = hash_state.table.as_mut().expect("hash table built");
                if table.total_tuples() == 0.0 && !node.hj_fill_outer {
                    return Ok(None);
                }
                table.nbatch_outstart = table.nbatch;
                node.hj_OuterNotEmpty = false;
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
            }
            HJ_NEED_NEW_OUTER => {
                let Some(hashvalue) = get_outer_tuple(node, outer, hash_state, estate)? else {
                    if node.hj_fill_inner {
                        // ExecPrepHashTableForUnmatched.
                        node.hj_CurBucketNo = 0;
                        node.hj_CurTuple = core::ptr::null_mut();
                        node.hj_JoinState = HJ_FILL_INNER_TUPLES;
                    } else {
                        node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    }
                    continue;
                };
                node.hj_MatchedOuter = false;
                node.hj_CurHashValue = hashvalue;
                let table = hash_state.table.as_ref().expect("hash table built");
                let (bucketno, batchno) = table.get_bucket_and_batch(hashvalue);
                node.hj_CurBucketNo = bucketno;
                node.hj_CurTuple = core::ptr::null_mut();

                if batchno != table.curbatch {
                    // Postpone this outer tuple to its batch's file.
                    debug_assert!(batchno > table.curbatch);
                    let outer_id = estate
                        .ecxt(node.ps_ExprContext)
                        .ecxt_outertuple
                        .expect("outer tuple set");
                    let query_mcx = estate.es_query_cxt;
                    let (slot, scratch_mcx) =
                        estate.slot_and_per_tuple_mcx(outer_id, node.ps_ExprContext);
                    let fetched =
                        exectuples::exec_fetch_slot_minimal_tuple(slot, query_mcx, scratch_mcx)?;
                    let (ptr, t_len): (*const u8, u32) = match &fetched {
                        exectuples::FetchedMinimalTuple::Slot(m, _) => {
                            // SAFETY: live stored image; header read.
                            (m.as_ptr().cast_const().cast(), unsafe { m.as_ref().t_len })
                        }
                        exectuples::FetchedMinimalTuple::Copied(t) => (t.as_ptr(), t.t_len()),
                    };
                    // SAFETY: a minimal tuple image is t_len readable bytes.
                    let bytes = unsafe { core::slice::from_raw_parts(ptr, t_len as usize) };
                    let table = hash_state.table.as_mut().expect("hash table built");
                    ::nodehash::save_tuple(
                        &mut table.outer_batch_file[batchno as usize],
                        hashvalue,
                        bytes,
                        query_mcx,
                    )?;
                    continue;
                }
                node.hj_JoinState = HJ_SCAN_BUCKET;
            }
            HJ_SCAN_BUCKET => {
                if !scan_hash_bucket(node, hash_state, estate)? {
                    node.hj_JoinState = HJ_FILL_OUTER_TUPLE;
                    continue;
                }
                // A right-semijoin needs only the first match per inner tuple.
                // SAFETY: hj_CurTuple just returned non-null by scan_hash_bucket.
                if node.plan.join.jointype == JoinType::JOIN_RIGHT_SEMI
                    && unsafe {
                        (*HashJoinTupleHdr::mintuple(node.hj_CurTuple).as_ptr()).has_match()
                    }
                {
                    continue;
                }
                let ecxt = node.ps_ExprContext;
                let inner_id = hash_state.hash_tuple_slot;
                let matched = match node.joinqual.as_deref_mut() {
                    None => true,
                    joinqual @ Some(_) => {
                        with_probe_slots(ecxt, inner_id, estate, |slots| exec_qual(joinqual, slots))?
                    }
                };
                if matched {
                    node.hj_MatchedOuter = true;
                    // SAFETY: hj_CurTuple set by scan_hash_bucket this pass.
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
                    // RIGHT_ANTI emits nothing here but stays on this outer
                    // to keep marking inner matches.
                    if node.plan.join.jointype == JoinType::JOIN_RIGHT_ANTI {
                        continue;
                    }
                    let pass = match node.otherqual.as_deref_mut() {
                        None => true,
                        otherqual @ Some(_) => with_probe_slots(ecxt, inner_id, estate, |slots| {
                            exec_qual(otherqual, slots)
                        })?,
                    };
                    if pass {
                        return Ok(Some(project_result(node, inner_id, estate)?));
                    }
                }
            }
            HJ_FILL_OUTER_TUPLE => {
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
                if !node.hj_MatchedOuter && node.hj_fill_outer {
                    let null_inner = node.hj_NullInnerTupleSlot.expect("null inner slot");
                    estate.ecxt_mut(node.ps_ExprContext).ecxt_innertuple = Some(null_inner);
                    let ecxt = node.ps_ExprContext;
                    let pass = match node.otherqual.as_deref_mut() {
                        None => true,
                        otherqual @ Some(_) => with_probe_slots(ecxt, null_inner, estate, |slots| {
                            exec_qual(otherqual, slots)
                        })?,
                    };
                    if pass {
                        return Ok(Some(project_result(node, null_inner, estate)?));
                    }
                }
            }
            HJ_FILL_INNER_TUPLES => {
                if !scan_hash_table_for_unmatched(node, hash_state, estate)? {
                    node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }
                let null_outer = node.hj_NullOuterTupleSlot.expect("null outer slot");
                estate.ecxt_mut(node.ps_ExprContext).ecxt_outertuple = Some(null_outer);
                let ecxt = node.ps_ExprContext;
                let inner_id = hash_state.hash_tuple_slot;
                let pass = match node.otherqual.as_deref_mut() {
                    None => true,
                    otherqual @ Some(_) => with_probe_slots(ecxt, inner_id, estate, |slots| {
                        exec_qual(otherqual, slots)
                    })?,
                };
                if pass {
                    return Ok(Some(project_result(node, inner_id, estate)?));
                }
            }
            HJ_NEED_NEW_BATCH => {
                if !new_batch(node, hash_state, estate)? {
                    return Ok(None);
                }
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
            }
            other => panic!("ExecHashJoin (nodeHashjoin.c): unrecognized state {other}"),
        }
    }
}

/// `ExecHashJoinNewBatch`: false when no batches remain.
fn new_batch<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let table = hash_state.table.as_mut().expect("hash table built");
    let nbatch = table.nbatch;
    let mut curbatch = table.curbatch;

    if curbatch > 0 {
        if let Some(f) = table.outer_batch_file[curbatch as usize].take() {
            f.close()?;
        }
    }

    // Skip batches empty on both sides; one-sided emptiness is skippable
    // except for fill requirements and post-growth reassignment scans.
    curbatch += 1;
    while curbatch < nbatch
        && (table.outer_batch_file[curbatch as usize].is_none()
            || table.inner_batch_file[curbatch as usize].is_none())
    {
        if table.outer_batch_file[curbatch as usize].is_some() && node.hj_fill_outer {
            break;
        }
        if table.inner_batch_file[curbatch as usize].is_some() && node.hj_fill_inner {
            break;
        }
        if table.inner_batch_file[curbatch as usize].is_some()
            && nbatch != table.nbatch_original
        {
            break;
        }
        if table.outer_batch_file[curbatch as usize].is_some()
            && nbatch != table.nbatch_outstart
        {
            break;
        }
        if let Some(f) = table.inner_batch_file[curbatch as usize].take() {
            f.close()?;
        }
        if let Some(f) = table.outer_batch_file[curbatch as usize].take() {
            f.close()?;
        }
        curbatch += 1;
    }

    if curbatch >= nbatch {
        return Ok(false);
    }
    table.curbatch = curbatch;
    table.reset(estate);

    let inner_file = hash_state
        .table
        .as_mut()
        .expect("hash table built")
        .inner_batch_file[curbatch as usize]
        .take();
    if let Some(mut inner_file) = inner_file {
        if inner_file.seek(0, 0, ::fd::buffile::SEEK_SET)? != 0 {
            panic!("could not rewind hash-join temporary file");
        }
        let hslot = hash_state.hash_tuple_slot;
        let ecxt = hash_state.ps_ExprContext;
        while let Some((hashvalue, tuple)) =
            ::nodehash::get_saved_tuple(&mut inner_file, &mut node.inner_saved_scratch)?
        {
            let mcx = estate.es_query_cxt;
            // SAFETY: the scratch image is live until the next get_saved_tuple,
            // and insert copies it out before that.
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(
                    &mut estate.es_tupleTable[hslot.0 as usize],
                    mcx,
                    tuple,
                )
            };
            hash_state
                .table
                .as_mut()
                .expect("hash table built")
                .insert(estate, hslot, ecxt, hashvalue)?;
        }
        inner_file.close()?;
    }

    let table = hash_state.table.as_mut().expect("hash table built");
    if let Some(f) = table.outer_batch_file[curbatch as usize].as_mut() {
        if f.seek(0, 0, ::fd::buffile::SEEK_SET)? != 0 {
            panic!("could not rewind hash-join temporary file");
        }
    }
    Ok(true)
}

fn exec_init_null_tuple_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    desc: Rc<TupleDescData<'static>>,
) -> ExecSlotId {
    let mcx = estate.es_query_cxt;
    let slot_id = estate.exec_init_extra_tuple_slot(Some(desc), TupleSlotKind::Virtual);
    exectuples::exec_store_all_null_tuple(&mut estate.es_tupleTable[slot_id.0 as usize], mcx);
    slot_id
}

// ExecScanHashTableForUnmatched: bucket-ordered walk emitting never-matched
// inner tuples into the hash tuple slot.
fn scan_hash_table_for_unmatched<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let table = hash_state.table.as_ref().expect("hash table built");
    let nbuckets = table.nbuckets();
    // SAFETY: chain headers live in the batch arena until reset.
    let mut cur: *mut HashJoinTupleHdr = if !node.hj_CurTuple.is_null() {
        unsafe { (*node.hj_CurTuple).next() }
    } else {
        core::ptr::null_mut()
    };
    loop {
        while cur.is_null() {
            if node.hj_CurBucketNo >= nbuckets {
                return Ok(false);
            }
            cur = table.bucket_head(node.hj_CurBucketNo);
            node.hj_CurBucketNo += 1;
        }
        let (tuple, matched) = unsafe {
            let mt = HashJoinTupleHdr::mintuple(cur);
            (mt, (*mt.as_ptr()).has_match())
        };
        if !matched {
            let hslot = hash_state.hash_tuple_slot;
            let mcx = estate.es_query_cxt;
            // SAFETY: entry images live in the batch arena until reset.
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
        cur = unsafe { (*cur).next() };
    }
}

// ExecHashJoinOuterGetTuple: the plan child on the first pass, the outer
// batch file afterwards.
fn get_outer_tuple<'mcx, O: HashJoinOuter<'mcx>>(
    node: &mut HashJoinState<'mcx>,
    outer: &mut O,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u32>> {
    let curbatch = hash_state.table.as_ref().expect("hash table built").curbatch;
    let ecxt = node.ps_ExprContext;
    if curbatch == 0 {
        let Some(slot_id) = outer.exec_proc(estate)? else {
            return Ok(None);
        };
        {
            let e = estate.ecxt_mut(ecxt);
            e.reset();
            e.ecxt_outertuple = Some(slot_id);
        }
        let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
        let mut slots = EvalSlots { scan: None, inner: Some(slot), outer: None };
        let r = exec_eval_expr(&mut node.outer_hash_expr, &mut slots)?;
        node.hj_OuterNotEmpty = true;
        Ok(Some(r.value.as_u32()))
    } else {
        let table = hash_state.table.as_mut().expect("hash table built");
        // In outer-join cases the batch file can be empty.
        let Some(file) = table.outer_batch_file[curbatch as usize].as_mut() else {
            return Ok(None);
        };
        let Some((hashvalue, tuple)) =
            ::nodehash::get_saved_tuple(file, &mut node.outer_saved_scratch)?
        else {
            return Ok(None);
        };
        let mcx = estate.es_query_cxt;
        let oslot = node.hj_OuterTupleSlot;
        // SAFETY: the scratch image is live until the next saved-tuple read,
        // which happens only after this outer tuple is fully processed.
        unsafe {
            exectuples::exec_store_minimal_tuple_ptr(
                &mut estate.es_tupleTable[oslot.0 as usize],
                mcx,
                tuple,
            )
        };
        estate.reset_expr_context(ecxt);
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(oslot);
        Ok(Some(hashvalue))
    }
}

// ExecScanHashBucket: prefilter on hashvalue, recheck via ExecQual.
fn scan_hash_bucket<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let table = hash_state.table.as_ref().expect("hash table built");
    let hashvalue = node.hj_CurHashValue;
    // SAFETY: chain headers live in the batch arena until reset (C's walk).
    let mut cur: *mut HashJoinTupleHdr = if !node.hj_CurTuple.is_null() {
        unsafe { (*node.hj_CurTuple).next() }
    } else {
        table.bucket_head(node.hj_CurBucketNo)
    };

    if cur.is_null() {
        return Ok(false);
    }
    // Slot pair/ecxt/EvalSlots resolved once per probe row (C's shape).
    let hslot = hash_state.hash_tuple_slot;
    let mcx = estate.es_query_cxt;
    let ecxt = node.ps_ExprContext;
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
        // hashvalue-compare before tuple deref: 2 loads per non-matching link.
        let hdr = unsafe { &*cur };
        if hdr.hashvalue() == hashvalue {
            let tuple = unsafe { HashJoinTupleHdr::mintuple(cur) };
            let inner = slots.inner.as_deref_mut().expect("inner slot bound");
            // SAFETY: entry images live in the batch arena until reset.
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

/// `ExecEndHashJoin`.
pub fn exec_end_hash_join<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    accum_instrumentation(node, hash_state, estate);
    if let Some(table) = hash_state.table.as_mut() {
        table.destroy()?;
        hash_state.table = None;
    }
    node.hashclauses = None;
    node.joinqual = None;
    node.otherqual = None;
    node.proj.release_frames();
    node.outer_hash_expr.release_frames();
    node.ps_ResultTupleDesc = None;
    ::nodehash::exec_end_hash(hash_state);
    Ok(())
}

/// `ExecShutdownHash`'s hand-off, keyed by the Hash sub-node's plan_node_id
/// (EXPLAIN reads before ExecutorEnd).
pub fn shutdown_accum_instrumentation<'mcx>(
    node: &HashJoinState<'mcx>,
    hash_state: &HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    accum_instrumentation(node, hash_state, estate);
}

fn accum_instrumentation<'mcx>(
    node: &HashJoinState<'mcx>,
    hash_state: &HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if estate.es_instrument == 0 {
        return;
    }
    let Some(table) = hash_state.table.as_ref() else { return };
    let hash_plan_id = node
        .plan
        .join
        .plan
        .righttree
        .expect("HashJoin has a Hash inner plan")
        .as_hash()
        .expect("HashJoin inner is a Hash node")
        .plan
        .plan_node_id;
    let hi = table.instrumentation();
    if let Some((_, slot)) = estate
        .es_hash_instrumentation
        .iter_mut()
        .find(|(id, _)| *id == hash_plan_id)
    {
        slot.accum(&hi);
    } else {
        estate.es_hash_instrumentation.push((hash_plan_id, hi));
    }
}

/// ExecReScanHashJoin (nodeHashjoin.c), inner-chgParam-nonnull arm: the
/// build side changed, so the table must be rebuilt.
pub fn exec_rescan_hash_join_chg<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if hash_state.table.is_some() {
        accum_instrumentation(node, hash_state, estate);
        hash_state.table.as_mut().expect("just checked").destroy()?;
        hash_state.table = None;
    }
    node.hj_JoinState = HJ_BUILD_HASHTABLE;
    node.hj_CurHashValue = 0;
    node.hj_CurBucketNo = 0;
    node.hj_CurTuple = core::ptr::null_mut();
    node.hj_MatchedOuter = false;
    node.hj_OuterNotEmpty = false;
    Ok(())
}

/// Multi-batch rescan destroys the table, so the caller must rescan the Hash
/// child subtree too (C's `ExecReScan(innerPlan)`).
#[derive(PartialEq, Eq)]
pub enum RescanInner {
    Keep,
    Rescan,
}

/// `ExecReScanHashJoin`.
pub fn exec_rescan_hash_join<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<RescanInner> {
    let mut rescan_inner = RescanInner::Keep;
    if hash_state.table.is_some() {
        if hash_state.table.as_ref().expect("just checked").nbatch == 1 {
            let table = hash_state.table.as_mut().expect("just checked");
            if node.hj_fill_inner || node.plan.join.jointype == JoinType::JOIN_RIGHT_SEMI {
                table.reset_match_flags();
            }
            node.hj_OuterNotEmpty = false;
            node.hj_JoinState = HJ_NEED_NEW_OUTER;
        } else {
            accum_instrumentation(node, hash_state, estate);
            hash_state
                .table
                .as_mut()
                .expect("just checked")
                .destroy()?;
            hash_state.table = None;
            node.hj_JoinState = HJ_BUILD_HASHTABLE;
            rescan_inner = RescanInner::Rescan;
        }
    }
    node.hj_CurHashValue = 0;
    node.hj_CurBucketNo = 0;
    node.hj_CurTuple = core::ptr::null_mut();
    node.hj_MatchedOuter = false;
    Ok(rescan_inner)
}

// The outer/inner slot pair for qual eval, disjoint &mut of es_tupleTable.
fn with_probe_slots<'mcx, R>(
    ecxt: EcxtId,
    inner_id: ExecSlotId,
    estate: &mut EStateData<'mcx>,
    f: impl FnOnce(&mut EvalSlots<'_, 'mcx>) -> PgResult<R>,
) -> PgResult<R> {
    let outer_id = estate
        .ecxt(ecxt)
        .ecxt_outertuple
        .expect("hashjoin outer tuple set");
    let table = &mut estate.es_tupleTable[..];
    let [inner, outer] = table
        .get_disjoint_mut([inner_id.0 as usize, outer_id.0 as usize])
        .expect("distinct in-range hashjoin slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
    f(&mut slots)
}

fn project_result<'mcx>(
    node: &mut HashJoinState<'mcx>,
    inner_id: ExecSlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
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
    exec_project(&mut node.proj, &mut slots, result, mcx)?;
    Ok(result_id)
}

// Exempt: all released in exec_end_hash_join.
mcx::forget_safe_struct!(
    HashJoinState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot,
        js_single_match, hj_fill_outer, hj_fill_inner, hj_NullInnerTupleSlot,
        hj_NullOuterTupleSlot, hj_JoinState, hj_CurHashValue, hj_CurBucketNo,
        hj_CurTuple, hj_MatchedOuter, hj_OuterNotEmpty, hj_OuterTupleSlot,
        outer_saved_scratch, inner_saved_scratch, hash_instr;
        ps_ResultTupleDesc, proj, hashclauses, joinqual, otherqual,
        outer_hash_expr },
);
