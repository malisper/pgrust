// nodeHashjoin.c JOIN_INNER single-batch machine; the Hash sub-node build runs
// through nodehash. LEFT/SEMI/ANTI, outer-fill, multi-batch, parallel are loud.
// Per-probe bucket scan is allocation-free.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execexpr::{
    exec_build_hash32_from_attrs, exec_build_projection_info, exec_init_qual, exec_project,
    exec_eval_expr, exec_qual, EvalSlots, ExprState, ParamBind,
};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::PgBox;
use ::nodehash::{HashBuildInput, HashState};
use ::types_error::PgResult;
use ::types_nodes::plannodes::HashJoin;
use ::types_nodes::JoinType;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

const HJ_BUILD_HASHTABLE: u8 = 1;
const HJ_NEED_NEW_OUTER: u8 = 2;
const HJ_SCAN_BUCKET: u8 = 3;

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
    pub ps_ResultTupleDesc: Rc<TupleDescData<'static>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    hashclauses: Option<PgBox<'mcx, ExprState<'mcx>>>,
    joinqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    otherqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    outer_hash_expr: PgBox<'mcx, ExprState<'mcx>>,
    js_single_match: bool,
    hj_JoinState: u8,
    hj_CurHashValue: u32,
    hj_CurBucketNo: u32,
    hj_CurTuple: u32,
    hj_MatchedOuter: bool,
    hj_OuterNotEmpty: bool,
}

/// `ExecInitHashJoin` minus child linkage; builds the outer hash program +
/// recheck qual and hands the inner keys to `init_hash` (nodehash).
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
        node.join.jointype == JoinType::JOIN_INNER,
        "ExecInitHashJoin (nodeHashjoin.c): jointype {:?}; LEFT/SEMI/ANTI + outer-fill lane unported",
        node.join.jointype
    );
    let mcx = estate.es_query_cxt;

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

    let hjstate = HashJoinState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: result_desc,
        ps_ResultTupleSlot,
        proj,
        hashclauses,
        joinqual,
        otherqual,
        outer_hash_expr,
        js_single_match: node.join.inner_unique,
        hj_JoinState: HJ_BUILD_HASHTABLE,
        hj_CurHashValue: 0,
        hj_CurBucketNo: 0,
        hj_CurTuple: ::nodehash::HashJoinTable::chain_end(),
        hj_MatchedOuter: false,
        hj_OuterNotEmpty: false,
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

/// `ExecHashJoin`, JOIN_INNER single batch; the Hash sub-node is built once.
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
                ::nodehash::multi_exec_hash(hash_state, hash_child, estate)?;
                let table = hash_state.table.as_ref().expect("hash table built");
                if table.total_tuples() == 0.0 {
                    return Ok(None);
                }
                node.hj_OuterNotEmpty = false;
                node.hj_JoinState = HJ_NEED_NEW_OUTER;
            }
            HJ_NEED_NEW_OUTER => {
                let Some(hashvalue) = get_outer_tuple(node, outer, estate)? else {
                    return Ok(None);
                };
                node.hj_MatchedOuter = false;
                node.hj_CurHashValue = hashvalue;
                node.hj_CurBucketNo =
                    hash_state.table.as_ref().unwrap().bucket_of(hashvalue);
                node.hj_CurTuple = ::nodehash::HashJoinTable::chain_end();
                node.hj_JoinState = HJ_SCAN_BUCKET;
            }
            HJ_SCAN_BUCKET => {
                if !scan_hash_bucket(node, hash_state, estate)? {
                    node.hj_JoinState = HJ_NEED_NEW_OUTER;
                    continue;
                }
                let ecxt = node.ps_ExprContext;
                let inner_id = hash_state.hash_tuple_slot;
                let joinqual = node.joinqual.as_deref_mut();
                let matched =
                    with_probe_slots(ecxt, inner_id, estate, |slots| exec_qual(joinqual, slots))?;
                if matched {
                    node.hj_MatchedOuter = true;
                    if node.js_single_match {
                        node.hj_JoinState = HJ_NEED_NEW_OUTER;
                    }
                    let otherqual = node.otherqual.as_deref_mut();
                    let pass = with_probe_slots(ecxt, inner_id, estate, |slots| {
                        exec_qual(otherqual, slots)
                    })?;
                    if pass {
                        return Ok(Some(project_result(node, hash_state, estate)?));
                    }
                }
            }
            other => panic!("ExecHashJoin (nodeHashjoin.c): state {other} not reachable for inner join"),
        }
    }
}

// ExecHashJoinOuterGetTuple (curbatch==0): compute the outer hash per tuple.
fn get_outer_tuple<'mcx, O: HashJoinOuter<'mcx>>(
    node: &mut HashJoinState<'mcx>,
    outer: &mut O,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u32>> {
    let ecxt = node.ps_ExprContext;
    loop {
        let Some(slot_id) = outer.exec_proc(estate)? else {
            return Ok(None);
        };
        estate.reset_expr_context(ecxt);
        let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
        let mut slots = EvalSlots { scan: None, inner: Some(slot), outer: None };
        let r = exec_eval_expr(&mut node.outer_hash_expr, &mut slots)?;
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(slot_id);
        node.hj_OuterNotEmpty = true;
        return Ok(Some(r.value.as_u32()));
    }
}

// ExecScanHashBucket: walk the chain from hj_CurTuple, prefilter on hashvalue,
// recheck hashclauses via ExecQual. No allocation per probe.
fn scan_hash_bucket<'mcx>(
    node: &mut HashJoinState<'mcx>,
    hash_state: &mut HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let table = hash_state.table.as_ref().expect("hash table built");
    let hashvalue = node.hj_CurHashValue;
    let mut cur = if node.hj_CurTuple != ::nodehash::HashJoinTable::chain_end() {
        table.entry(node.hj_CurTuple).next
    } else {
        table.bucket_head(node.hj_CurBucketNo)
    };

    while cur != ::nodehash::HashJoinTable::chain_end() {
        let e = table.entry(cur);
        if e.hashvalue == hashvalue {
            let hslot = hash_state.hash_tuple_slot;
            let mcx = estate.es_query_cxt;
            // SAFETY: entry images live in the query arena until reset.
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(
                    &mut estate.es_tupleTable[hslot.0 as usize],
                    mcx,
                    e.tuple,
                )
            };
            estate.ecxt_mut(node.ps_ExprContext).ecxt_innertuple = Some(hslot);
            let ecxt = node.ps_ExprContext;
            let hashclauses = node.hashclauses.as_deref_mut();
            let m = with_probe_slots(ecxt, hslot, estate, |slots| exec_qual(hashclauses, slots))?;
            if m {
                node.hj_CurTuple = cur;
                return Ok(true);
            }
        }
        cur = table.entry(cur).next;
    }
    Ok(false)
}

/// `ExecEndHashJoin`: table lives in the query arena; children ended by caller.
pub fn exec_end_hash_join(_node: &mut HashJoinState<'_>, hash_state: &mut HashState<'_>) {
    ::nodehash::exec_end_hash(hash_state);
}

/// `ExecReScanHashJoin` single-batch reuse: keep the table, restart the probe.
pub fn exec_rescan_hash_join(node: &mut HashJoinState<'_>, hash_state: &HashState<'_>) {
    if hash_state.table.is_some() {
        node.hj_OuterNotEmpty = false;
        node.hj_JoinState = HJ_NEED_NEW_OUTER;
    } else {
        node.hj_JoinState = HJ_BUILD_HASHTABLE;
    }
    node.hj_CurTuple = ::nodehash::HashJoinTable::chain_end();
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
    hash_state: &HashState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
    let mcx = estate.es_query_cxt;
    let outer_id = estate
        .ecxt(node.ps_ExprContext)
        .ecxt_outertuple
        .expect("hashjoin outer tuple set");
    let inner_id = hash_state.hash_tuple_slot;
    let result_id = node.ps_ResultTupleSlot;
    let table = &mut estate.es_tupleTable[..];
    let [inner, outer, result] = table
        .get_disjoint_mut([inner_id.0 as usize, outer_id.0 as usize, result_id.0 as usize])
        .expect("distinct in-range hashjoin slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
    exec_project(&mut node.proj, &mut slots, result, mcx)?;
    Ok(result_id)
}
