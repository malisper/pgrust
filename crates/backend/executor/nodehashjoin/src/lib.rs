// nodeHashjoin.c serial state machine, all jointypes, single- and
// multi-batch; parallel is loud. Per-probe bucket scan is allocation-free.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{
    exec_build_hash32_from_exprs, exec_build_projection_info_subplans, exec_init_qual_subplans,
    exec_project, exec_qual, EvalSlots, ExprState,
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

mod parallel;
pub use parallel::exec_parallel_hash_join;

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
    /// Precomputed hash of the tuple exec_proc just returned; must be
    /// byte-equal to the outer hash expr (hash32var_low32 cover).
    #[inline(always)]
    fn staged_hash(&self) -> Option<u32> {
        None
    }

    /// Once per build; None disarms a stale filter after a rebuild.
    fn set_hash_filter(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _push: Option<ProbeFilterPush<'mcx>>,
    ) -> PgResult<()> {
        Ok(())
    }

    /// Once, when the dense table seats: probe hashes are dead from here on.
    fn dense_armed(&mut self) {}
}

/// Filter + the 0-based outer-scan attnum of its hashint4/hashoid key.
pub struct ProbeFilterPush<'mcx> {
    pub filter: std::rc::Rc<::nodehash::ProbeBloom<'mcx>>,
    pub key_attnum: u16,
}

#[derive(Clone, Copy)]
struct DenseCols {
    o: u16,
    i: u16,
}
mcx::forget_safe_nodrop!(DenseCols);

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
    // InstrCountFiltered1/2 slot for this join node (nodeHashjoin.c).
    js_instr: Option<u32>,
    dense_cols: Option<DenseCols>,
    dense_on: bool,
    hj_CurDense: u32,
}

impl<'mcx> HashJoinState<'mcx> {
    /// Probe-drive deform prefix; the outer hash expr binds the outer slot as
    /// INNER (get_outer_tuple's EvalSlots shape). None = lazy deform.
    pub fn probe_outer_prefix(&self) -> Option<i32> {
        use ::execexpr::{Kernel, SlotSrc};
        let mut p = match self.outer_hash_expr.kernel() {
            Kernel::Hash32Var { src: SlotSrc::Inner, attnum, .. } => attnum as i32 + 1,
            _ => self.outer_hash_expr.max_fetch(SlotSrc::Inner)?,
        };
        for q in [
            self.hashclauses.as_deref(),
            self.joinqual.as_deref(),
            self.otherqual.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            p = p.max(q.max_fetch(SlotSrc::Outer)?);
        }
        p = p.max(self.proj.max_fetch(SlotSrc::Outer)?);
        Some(p)
    }

    /// 0-based outer key column when the probe hash is columnar-precomputable.
    pub fn probe_hash_col(&self) -> Option<u16> {
        self.outer_hash_expr.hash32var_low32(::execexpr::SlotSrc::Inner)
    }

    // Static gate half: a single Int4Eq var=var hashclause over exactly the
    // hashed key columns, so non-null key equality <=> prefilter + recheck.
    fn dense_cols_of(
        hashclauses: Option<&ExprState<'_>>,
        outer_hash_expr: &ExprState<'_>,
    ) -> Option<DenseCols> {
        use ::execexpr::{CmpOp, Kernel, SlotSrc};
        let Kernel::QualVarCmpVar { a_src, a_attnum, b_src, b_attnum, cmp: CmpOp::Int4Eq } =
            hashclauses?.kernel()
        else {
            return None;
        };
        let (o, i) = match (a_src, b_src) {
            (SlotSrc::Outer, SlotSrc::Inner) => (a_attnum, b_attnum),
            (SlotSrc::Inner, SlotSrc::Outer) => (b_attnum, a_attnum),
            _ => return None,
        };
        (outer_hash_expr.hash32var_low32(SlotSrc::Inner) == Some(o))
            .then_some(DenseCols { o, i })
    }
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

    let params = estate.param_bind();
    // C ExecInitHashJoin compiles the outer hash keys with the HashJoinState
    // parent, so SubPlans are legal in them.
    let outer_hash_expr = ::executils::with_subplan_compile_env(estate, |env| {
        exec_build_hash32_from_exprs(
            mcx,
            outer_desc,
            &node.hashkeys,
            &outer_hashfns,
            &collations,
            0,
            params,
            env,
        )
    })?;

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);
    let (proj, hashclauses, joinqual, otherqual) =
        ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
            let proj = exec_build_projection_info_subplans(
                mcx,
                &node.join.plan.targetlist,
                None,
                params,
                env,
            )?;
            let hashclauses = exec_init_qual_subplans(mcx, &node.hashclauses, params, env)?;
            let joinqual = exec_init_qual_subplans(mcx, &node.join.joinqual, params, env)?;
            let otherqual = exec_init_qual_subplans(mcx, &node.join.plan.qual, params, env)?;
            Ok((proj, hashclauses, joinqual, otherqual))
        })?;

    let hash_state = init_hash(estate, inner_desc, &inner_hashfns, &collations)?;
    let hash_node = node
        .join
        .plan
        .righttree
        .expect("HashJoin without a Hash inner plan")
        .as_hash()
        .expect("HashJoin inner is a Hash node");

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
    // This node's own slot is init'ed by the Instrumented wrapper.
    let js_instr = if estate.es_instrument != 0 {
        Some(u32::try_from(node.join.plan.plan_node_id).expect("plan_node_id is non-negative"))
    } else {
        None
    };

    let dense_cols = HashJoinState::dense_cols_of(hashclauses.as_deref(), &*outer_hash_expr);
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
        js_instr,
        dense_cols,
        dense_on: false,
        hj_CurDense: ::nodehash::DENSE_END,
    };
    Ok((hjstate, hash_state))
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
                let want_filter = !node.hj_fill_outer
                    && node.outer_hash_expr.hash32var_low32(::execexpr::SlotSrc::Inner).is_some();
                hash_state.table =
                    Some(::nodehash::exec_hash_table_create(hash_state, estate, want_filter)?);
                // Instrumented plans never engage dense (fusion precedent).
                if estate.es_instrument == 0 {
                    if let Some(dc) = node.dense_cols {
                        if hash_state.build_hash_col() == Some(dc.i) {
                            hash_state
                                .table
                                .as_mut()
                                .expect("hash table created")
                                .arm_key_track(dc.i);
                        }
                    }
                }
                // MultiExecHash provides its own instrumentation (the Hash
                // node has no ExecProcNode wrapper).
                let instr = node.hash_instr.map(|ix| ix as usize);
                if let Some(ix) = instr {
                    ::instrument::instr_start_node(&mut estate.es_instrumentation[ix]);
                }
                hash_child.multi_exec(hash_state, estate)?;
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
                node.dense_on = table.dense().is_some();
                // fill_outer must null-extend unmatched outers — never arms.
                // Dense seat supersedes bloom: a dense-table miss is an exact
                // miss, so push None (also disarms a stale filter on rebuild).
                let push = if !node.hj_fill_outer && !node.dense_on {
                    node.outer_hash_expr
                        .hash32var_low32(::execexpr::SlotSrc::Inner)
                        .and_then(|key_attnum| {
                            table
                                .take_probe_filter()
                                .map(|filter| ProbeFilterPush { filter, key_attnum })
                        })
                } else {
                    None
                };
                outer.set_hash_filter(estate, push)?;
                if node.dense_on {
                    outer.dense_armed();
                }
                node.hj_OuterNotEmpty = false;
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
            }
            HJ_NEED_NEW_OUTER if node.dense_on => {
                let Some((key, isnull)) = get_outer_key(node, outer, estate)? else {
                    if node.hj_fill_inner {
                        node.hj_CurBucketNo = 0;
                        node.hj_CurTuple = core::ptr::null_mut();
                        node.hj_JoinState = HJ_FILL_INNER_TUPLES;
                    } else {
                        node.hj_JoinState = HJ_NEED_NEW_BATCH;
                    }
                    continue;
                };
                node.hj_MatchedOuter = false;
                node.hj_CurTuple = core::ptr::null_mut();
                let dense = hash_state
                    .table
                    .as_ref()
                    .expect("hash table built")
                    .dense()
                    .expect("dense seated");
                node.hj_CurDense = if isnull { ::nodehash::DENSE_END } else { dense.head_for(key) };
                node.hj_JoinState = HJ_SCAN_BUCKET;
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
                let found = if node.dense_on {
                    scan_dense(node, hash_state, estate)
                } else {
                    scan_hash_bucket(node, hash_state, estate)?
                };
                if !found {
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
                let matched = eval_probe_qual(node.joinqual.as_deref_mut(), ecxt, inner_id, estate)?;
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
                if !scan_hash_table_for_unmatched(node, hash_state, estate)? {
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
        let h = match outer.staged_hash() {
            Some(h) => h,
            None => ::executils::exec_eval_expr_with_subplans_inner_slot(
                &mut node.outer_hash_expr,
                estate,
                ecxt,
                slot_id,
            )?
            .value
            .as_u32(),
        };
        node.hj_OuterNotEmpty = true;
        Ok(Some(h))
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

// Every dense chain entry equals the probe key, so the hashvalue prefilter
// and hashclause recheck are both proven true — no recheck per match.
fn scan_dense<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    let cur = node.hj_CurDense;
    if cur == ::nodehash::DENSE_END {
        return false;
    }
    let table = hash_state.table.as_ref().expect("hash table built");
    node.hj_CurDense = table.dense().expect("dense seated").next(cur);
    let hdr = table.dense_tuple(cur);
    node.hj_CurTuple = hdr.as_ptr();
    let hslot = hash_state.hash_tuple_slot;
    let mcx = estate.es_query_cxt;
    // SAFETY: entry images live in the batch arena until reset.
    unsafe {
        let tuple = HashJoinTupleHdr::mintuple(hdr.as_ptr());
        exectuples::exec_store_minimal_tuple_ptr(
            &mut estate.es_tupleTable[hslot.0 as usize],
            mcx,
            tuple,
        );
    }
    estate.ecxt_mut(node.ps_ExprContext).ecxt_innertuple = Some(hslot);
    true
}

// Dense ExecHashJoinOuterGetTuple: nbatch==1, so no batch files, no hash.
fn get_outer_key<'mcx, O: HashJoinOuter<'mcx>>(
    node: &mut HashJoinState<'mcx>,
    outer: &mut O,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<(i32, bool)>> {
    let Some(slot_id) = outer.exec_proc(estate)? else {
        return Ok(None);
    };
    {
        let e = estate.ecxt_mut(node.ps_ExprContext);
        e.reset();
        e.ecxt_outertuple = Some(slot_id);
    }
    let dc = node.dense_cols.expect("dense gate matched");
    let mut isnull = false;
    let v = exectuples::slot_getattr(
        &mut estate.es_tupleTable[slot_id.0 as usize],
        dc.o as i32 + 1,
        &mut isnull,
    );
    node.hj_OuterNotEmpty = true;
    Ok(Some((v.as_i32(), isnull)))
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
    if node.hashclauses.as_deref().is_some_and(|q| q.has_subplan()) {
        return scan_hash_bucket_subplans(node, hash_state, estate, cur);
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
    // SubPlan-bearing hashclauses (C ExecQual on the HashJoinState) take the
    // per-link driver path; the fused slot-pair fast loop below cannot host
    // a suspension.
    if node.hashclauses.as_ref().is_some_and(|q| q.has_subplan()) {
        while !cur.is_null() {
            let hdr = unsafe { &*cur };
            if hdr.hashvalue() == hashvalue {
                let tuple = unsafe { HashJoinTupleHdr::mintuple(cur) };
                {
                    let inner = &mut estate.es_tupleTable[hslot.0 as usize];
                    // SAFETY: entry images live in the batch arena until reset.
                    unsafe { exectuples::exec_store_minimal_tuple_ptr(inner, mcx, tuple) };
                }
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

// SubPlan-bearing hashclauses recheck (C evaluates via ExecQual with the
// hjstate parent's SubPlanStates; the fast lane's split-borrow EvalSlots
// cannot host the suspended subplan loop).
#[cold]
fn scan_hash_bucket_subplans<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut cur: *mut HashJoinTupleHdr,
) -> PgResult<bool> {
    let hashvalue = node.hj_CurHashValue;
    let hslot = hash_state.hash_tuple_slot;
    let mcx = estate.es_query_cxt;
    let ecxt = node.ps_ExprContext;
    estate.ecxt_mut(ecxt).ecxt_innertuple = Some(hslot);
    while !cur.is_null() {
        // SAFETY: entry images live in the batch arena until reset.
        let hdr = unsafe { &*cur };
        if hdr.hashvalue() == hashvalue {
            let tuple = unsafe { HashJoinTupleHdr::mintuple(cur) };
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(
                    &mut estate.es_tupleTable[hslot.0 as usize],
                    mcx,
                    tuple,
                )
            };
            if ::executils::exec_qual_with_subplans(node.hashclauses.as_deref_mut(), estate, ecxt)? {
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

/// `ExecShutdownHashJoin`: detach from shared state before the parallel
/// context is destroyed (instrumentation accumulates first, as C's
/// ExecShutdownHash ordering).
pub fn exec_shutdown_hash_join<'mcx>(
    node: &HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    accum_instrumentation(node, hash_state, estate);
    if let Some(table) = hash_state.ptable.as_mut() {
        ::nodehash::parallel::exec_hash_table_detach_batch(table)?;
        ::nodehash::parallel::exec_hash_table_detach(table)?;
    }
    Ok(())
}

fn accum_instrumentation<'mcx>(
    node: &HashJoinState<'mcx>,
    hash_state: &HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if estate.es_instrument == 0 {
        return;
    }
    if let Some(t) = hash_state.ptable.as_ref() {
        accum_hash_instr(node, t.instrumentation(), estate);
        return;
    }
    let Some(table) = hash_state.table.as_ref() else { return };
    accum_hash_instr(node, table.instrumentation(), estate);
}

fn accum_hash_instr<'mcx>(
    node: &HashJoinState<'mcx>,
    hi: ::types_core::instrument::HashInstrumentation,
    estate: &mut EStateData<'mcx>,
) {
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
    release_parallel_table(node, hash_state, estate)?;
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
    node.dense_on = false;
    node.hj_CurDense = ::nodehash::DENSE_END;
    Ok(())
}

/// Multi-batch rescan destroys the table, so the caller must rescan the Hash
/// child subtree too (C's `ExecReScan(innerPlan)`).
#[derive(PartialEq, Eq)]
pub enum RescanInner {
    Keep,
    Rescan,
}

// A parallel-hash rescan always rebuilds: the shared table was freed when
// the last participant detached (C forces this via a chgParam dependency).
fn release_parallel_table<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if hash_state.ptable.is_none() {
        return Ok(false);
    }
    accum_instrumentation(node, hash_state, estate);
    {
        let table = hash_state.ptable.as_mut().expect("just checked");
        ::nodehash::parallel::exec_hash_table_detach_batch(table)?;
        ::nodehash::parallel::exec_hash_table_detach(table)?;
    }
    hash_state.ptable = None;
    node.hj_JoinState = HJ_BUILD_HASHTABLE;
    Ok(true)
}

/// `ExecReScanHashJoin`.
pub fn exec_rescan_hash_join<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<RescanInner> {
    let mut rescan_inner = RescanInner::Keep;
    if release_parallel_table(node, hash_state, estate)? {
        rescan_inner = RescanInner::Rescan;
    }
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
            node.dense_on = false;
            rescan_inner = RescanInner::Rescan;
        }
    }
    node.hj_CurHashValue = 0;
    node.hj_CurBucketNo = 0;
    node.hj_CurTuple = core::ptr::null_mut();
    node.hj_MatchedOuter = false;
    node.hj_CurDense = ::nodehash::DENSE_END;
    Ok(rescan_inner)
}

// ===========================================================================
// Lane-executor-v2 join-breaker delegation seams (design §Architecture 1, §8;
// push-executor study Patterns 3+4). The lane's pipeline shapes live in
// `execmain/src/lanev2.rs`; these entry points delegate to the SAME row-path
// state machine (`exec_hash_join`'s arms) over the SAME `HashJoinState` /
// `HashJoinTable`, so falling back to `exec_hash_join` at any call boundary
// resumes from coherent node state, and the lane's join output (probe order ×
// bucket-chain order) is C's exactly. The phase flag is `hj_JoinState` itself
// — C's own cross-call state; no new field.
//
// Admitted shape (everything else refuses in `lanev2`): INNER join, no
// joinqual/otherqual residuals, single batch (nbatch==1 checked after the
// delegated build), serial, uninstrumented, subplan- and param-free
// hashclauses / projection / hash exprs.
// ===========================================================================

/// Where the lane may pick the join up. `EmptyDone` mirrors C's empty-build
/// early return (`total_tuples == 0 && !hj_fill_outer` leaves `hj_JoinState`
/// at HJ_BUILD_HASHTABLE with the table built): the join emits nothing, and
/// the outer child is never pulled.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LaneJoinPhase {
    Build,
    Probe,
    EmptyDone,
}

pub fn lane_join_phase(node: &HashJoinState<'_>, hs: &HashState<'_>) -> LaneJoinPhase {
    if node.hj_JoinState == HJ_BUILD_HASHTABLE {
        if hs.table.is_none() {
            LaneJoinPhase::Build
        } else {
            LaneJoinPhase::EmptyDone
        }
    } else {
        LaneJoinPhase::Probe
    }
}

/// Structural admission, join side: the probe shapes `lane_probe_next`
/// handles. INNER only (no null-fill faces, no match-driven state skips);
/// residual quals refused (the lane emit path evaluates neither); subplan- /
/// initplan-param-free hash + projection exprs (no suspension hosting, no
/// pending-initplan hoist); uninstrumented (`js_instr`/`hash_instr` are Some
/// iff es_instrument != 0 at init).
pub fn lane_join_admissible(node: &HashJoinState<'_>) -> bool {
    node.plan.join.jointype == JoinType::JOIN_INNER
        && node.joinqual.is_none()
        && node.otherqual.is_none()
        && node.js_instr.is_none()
        && node.hash_instr.is_none()
        && !node.outer_hash_expr.has_subplan()
        && node.outer_hash_expr.param_exec_deps().is_empty()
        && node
            .hashclauses
            .as_deref()
            .is_none_or(|q| !q.has_subplan() && q.param_exec_deps().is_empty())
        && !node.proj.has_subplan()
        && node.proj.param_exec_deps().is_empty()
}

/// True while no drive (lane or row-path) has touched this join: the verdict
/// is memoized at first engagement, so admitting only an untouched node
/// guarantees the lane owns the node's whole life (never a mid-stream
/// takeover from row-path-left state).
pub fn lane_join_untouched(node: &HashJoinState<'_>, hs: &HashState<'_>) -> bool {
    node.hj_JoinState == HJ_BUILD_HASHTABLE && hs.table.is_none() && hs.ptable.is_none()
}

/// Build-phase entry: `exec_hash_join`'s HJ_BUILD_HASHTABLE table creation,
/// verbatim — same `want_filter` (bloom sizing counts identically toward the
/// table's space accounting, so nbatch growth points match the row path) and
/// same dense key-track arming. The lane never *uses* the bloom (its probe
/// runs no prefilter; results are prefilter-invariant) but must create the
/// table identically so a spill-refuse fallback resumes bit-equal state.
pub fn lane_build_begin<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(hs.table.is_none());
    let want_filter = !node.hj_fill_outer
        && node.outer_hash_expr.hash32var_low32(::execexpr::SlotSrc::Inner).is_some();
    hs.table = Some(::nodehash::exec_hash_table_create(hs, estate, want_filter)?);
    if estate.es_instrument == 0 {
        if let Some(dc) = node.dense_cols {
            if hs.build_hash_col() == Some(dc.i) {
                hs.table.as_mut().expect("hash table created").arm_key_track(dc.i);
            }
        }
    }
    Ok(())
}

/// Post-build tail of `lane_build_begin`'s phase.
pub struct LaneBuildDone {
    /// C's empty-build early return: emit nothing, never pull the outer child.
    pub empty: bool,
    /// The built table's final batch count; > 1 must refuse the lane probe
    /// (the multi-batch outer postpone/reread machinery stays row-path-only).
    pub nbatch: i32,
}

/// Build-phase exit: `exec_hash_join`'s HJ_BUILD_HASHTABLE tail after
/// MultiExecHash — empty-build early return, `nbatch_outstart`, `dense_on`,
/// the phase flip to HJ_NEED_NEW_OUTER. The bloom push to the outer drive is
/// deliberately skipped: the lane owns only shapes where the row path's
/// `set_hash_filter` is a no-op (plain `PlanStateNode` outers; the fused
/// probe source never coexists with a lane-owned join).
pub fn lane_build_finish<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<LaneBuildDone> {
    ::nodehash::lane_build_finish(hs, estate)?;
    let table = hs.table.as_mut().expect("hash table built");
    if table.total_tuples() == 0.0 && !node.hj_fill_outer {
        // Mirror C: hj_JoinState stays HJ_BUILD_HASHTABLE (rescan handles it:
        // nbatch==1 + table kept -> HJ_NEED_NEW_OUTER, probing the empty
        // table over the rescanned outer, exactly as the row path would).
        return Ok(LaneBuildDone { empty: true, nbatch: table.nbatch });
    }
    table.nbatch_outstart = table.nbatch;
    node.dense_on = table.dense().is_some();
    node.hj_OuterNotEmpty = false;
    node.hj_JoinState = HJ_NEED_NEW_OUTER;
    Ok(LaneBuildDone { empty: false, nbatch: table.nbatch })
}

/// Intra-row expansion pending? (One outer row -> K matches; the position —
/// `hj_CurTuple`/`hj_CurDense` — is node-resident, so a paused expansion
/// resumes exactly across the Volcano call boundary, as C's own
/// HJ_SCAN_BUCKET cross-call state does.)
#[inline]
pub fn lane_probe_pending(node: &HashJoinState<'_>) -> bool {
    node.hj_JoinState == HJ_SCAN_BUCKET
}

/// Accept one outer (probe-side) row: `exec_hash_join`'s HJ_NEED_NEW_OUTER
/// arm for curbatch==0 (dense and hashed variants), minus the child pull —
/// the row arrives pushed. nbatch==1 is an admission invariant, so the
/// postpone-to-batch-file arm is unreachable.
pub fn lane_probe_accept<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_slot: ExecSlotId,
) -> PgResult<()> {
    cfi()?;
    debug_assert_eq!(node.hj_JoinState, HJ_NEED_NEW_OUTER);
    {
        let e = estate.ecxt_mut(node.ps_ExprContext);
        e.reset();
        e.ecxt_outertuple = Some(outer_slot);
    }
    node.hj_MatchedOuter = false;
    node.hj_CurTuple = core::ptr::null_mut();
    if node.dense_on {
        // HJ_NEED_NEW_OUTER dense arm (get_outer_key + head_for).
        let dc = node.dense_cols.expect("dense gate matched");
        let mut isnull = false;
        let v = exectuples::slot_getattr(
            &mut estate.es_tupleTable[outer_slot.0 as usize],
            dc.o as i32 + 1,
            &mut isnull,
        );
        let dense = hs.table.as_ref().expect("hash table built").dense().expect("dense seated");
        node.hj_CurDense =
            if isnull { ::nodehash::DENSE_END } else { dense.head_for(v.as_i32()) };
    } else {
        let ecxt = node.ps_ExprContext;
        let h = ::executils::exec_eval_expr_with_subplans_inner_slot(
            &mut node.outer_hash_expr,
            estate,
            ecxt,
            outer_slot,
        )?
        .value
        .as_u32();
        node.hj_CurHashValue = h;
        let table = hs.table.as_ref().expect("hash table built");
        let (bucketno, batchno) = table.get_bucket_and_batch(h);
        debug_assert_eq!(batchno, 0, "lane join admitted a multi-batch probe");
        node.hj_CurBucketNo = bucketno;
    }
    node.hj_OuterNotEmpty = true;
    node.hj_JoinState = HJ_SCAN_BUCKET;
    Ok(())
}

/// Next joined tuple for the accepted outer row: `exec_hash_join`'s
/// HJ_SCAN_BUCKET arm for the admitted shape (INNER, no joinqual/otherqual —
/// every bucket match passes, is flagged, and projects), stepping back to
/// HJ_NEED_NEW_OUTER on bucket exhaustion (HJ_FILL_OUTER_TUPLE is a no-op for
/// INNER) or after a single-match (inner_unique) emit.
pub fn lane_probe_next<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hs: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(node.joinqual.is_none() && node.otherqual.is_none());
    if node.hj_JoinState != HJ_SCAN_BUCKET {
        return Ok(None);
    }
    cfi()?;
    let found = if node.dense_on {
        scan_dense(node, hs, estate)
    } else {
        scan_hash_bucket(node, hs, estate)?
    };
    if !found {
        node.hj_JoinState = HJ_NEED_NEW_OUTER;
        return Ok(None);
    }
    node.hj_MatchedOuter = true;
    // SAFETY: hj_CurTuple was just set non-null by the bucket/dense scan.
    unsafe {
        let mt = HashJoinTupleHdr::mintuple(node.hj_CurTuple).as_ptr();
        if !(*mt).has_match() {
            (*mt).set_match();
        }
    }
    if node.js_single_match {
        node.hj_JoinState = HJ_NEED_NEW_OUTER;
    }
    let inner_id = hs.hash_tuple_slot;
    Ok(Some(project_result(node, inner_id, estate)?))
}

#[inline(always)]
fn eval_probe_qual<'mcx>(
    qual: Option<&mut ExprState<'mcx>>,
    ecxt: EcxtId,
    inner_id: ExecSlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if qual.is_none() {
        return Ok(true);
    }
    // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter.
    let deps = qual.as_ref().unwrap().param_exec_deps();
    if !deps.is_empty() {
        ::executils::exec_eval_param_exec_params(estate, deps)?;
    }
    if qual.as_ref().is_some_and(|q| q.has_subplan()) {
        estate.ecxt_mut(ecxt).ecxt_innertuple = Some(inner_id);
        return ::executils::exec_qual_with_subplans(qual, estate, ecxt);
    }
    with_probe_slots(ecxt, inner_id, estate, |slots| exec_qual(qual, slots))
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
    // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter.
    let deps = node.proj.param_exec_deps();
    if !deps.is_empty() {
        ::executils::exec_eval_param_exec_params(estate, deps)?;
    }
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
    exec_project(&mut node.proj, &mut slots, result, mcx)?;
    Ok(result_id)
}

// Exempt: all released in exec_end_hash_join.
mcx::forget_safe_struct!(
    HashJoinState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot,
        js_single_match, hj_fill_outer, hj_fill_inner, hj_NullInnerTupleSlot,
        hj_NullOuterTupleSlot, hj_JoinState, hj_CurHashValue, hj_CurBucketNo,
        hj_CurTuple, hj_MatchedOuter, hj_OuterNotEmpty, hj_OuterTupleSlot,
        outer_saved_scratch, inner_saved_scratch, hash_instr, js_instr,
        dense_cols, dense_on, hj_CurDense;
        ps_ResultTupleDesc, proj, hashclauses, joinqual, otherqual,
        outer_hash_expr },
);
