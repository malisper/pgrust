// nodeMergejoin.c INNER/LEFT/RIGHT/SEMI/ANTI/FULL MJ_* state machine
// (clauseless FULL included); the MergeJoinInner trait carries
// ExecMarkPos/ExecRestrPos. RIGHT_SEMI/RIGHT_ANTI and parallel merge are loud.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::datum::Datum;
use ::execexpr::{
    exec_build_projection_info_subplans, exec_init_expr_subplans,
    exec_init_qual_subplans, exec_project, exec_qual, EvalSlots, ExprState,
};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::PgBox;
use ::tuplesort::{apply_sort_comparator_in, SortSupport};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_nodes::plannodes::MergeJoin;
use ::types_nodes::JoinType;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

const EXEC_MJ_INITIALIZE_OUTER: u8 = 1;
const EXEC_MJ_INITIALIZE_INNER: u8 = 2;
const EXEC_MJ_JOINTUPLES: u8 = 3;
const EXEC_MJ_NEXTOUTER: u8 = 4;
const EXEC_MJ_TESTOUTER: u8 = 5;
const EXEC_MJ_NEXTINNER: u8 = 6;
const EXEC_MJ_SKIP_TEST: u8 = 7;
const EXEC_MJ_SKIPOUTER_ADVANCE: u8 = 8;
const EXEC_MJ_SKIPINNER_ADVANCE: u8 = 9;
const EXEC_MJ_ENDOUTER: u8 = 10;
const EXEC_MJ_ENDINNER: u8 = 11;

#[derive(Clone, Copy, PartialEq, Eq)]
enum MJEvalResult {
    Matchable,
    NonMatchable,
    EndOfJoin,
}

#[derive(Clone, Copy)]
enum Qual {
    Join,
    Other,
}

#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

pub trait MergeJoinOuter<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
}

pub trait MergeJoinInner<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
    fn mark_pos(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
    fn restr_pos(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
}

struct MergeJoinClause<'mcx> {
    lexpr: PgBox<'mcx, ExprState<'mcx>>,
    rexpr: PgBox<'mcx, ExprState<'mcx>>,
    ldatum: Datum,
    rdatum: Datum,
    lisnull: bool,
    risnull: bool,
    ssup: SortSupport,
}

pub struct MergeJoinState<'mcx> {
    pub plan: &'mcx MergeJoin<'mcx>,
    pub ps_ExprContext: EcxtId,
    mj_OuterEContext: EcxtId,
    mj_InnerEContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    joinqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    otherqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    clauses: ::mcx::PgVec<'mcx, MergeJoinClause<'mcx>>,
    mj_JoinState: u8,
    mj_SkipMarkRestore: bool,
    mj_ExtraMarks: bool,
    js_single_match: bool,
    mj_ConstFalseJoin: bool,
    mj_FillOuter: bool,
    mj_FillInner: bool,
    mj_NullInnerTupleSlot: Option<ExecSlotId>,
    mj_NullOuterTupleSlot: Option<ExecSlotId>,
    mj_MatchedOuter: bool,
    mj_MatchedInner: bool,
    mj_OuterTupleSlot: Option<ExecSlotId>,
    mj_InnerTupleSlot: Option<ExecSlotId>,
    mj_MarkedTupleSlot: ExecSlotId,
    // InstrCountFiltered1/2 slot for this join node (nodeMergejoin.c).
    js_instr: Option<u32>,
}

/// `ExecInitMergeJoin` minus child linkage: the caller inits the outer child
/// with `eflags`, the inner child with [`inner_child_eflags`].
pub fn exec_init_merge_join<'mcx>(
    node: &'mcx MergeJoin<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    inner_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
    inner_is_material: bool,
) -> PgResult<MergeJoinState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    if !matches!(
        node.join.jointype,
        JoinType::JOIN_INNER
            | JoinType::JOIN_LEFT
            | JoinType::JOIN_RIGHT
            | JoinType::JOIN_FULL
            | JoinType::JOIN_SEMI
            | JoinType::JOIN_ANTI
            | JoinType::JOIN_RIGHT_ANTI
    ) {
        // unported: ExecInitMergeJoin (nodeMergejoin.c) RIGHT_SEMI lane.
        return Err(Box::new(
            PgError::error(format!(
                "merge join with join type {:?} is not yet implemented",
                node.join.jointype
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    assert!(
        !node.skip_mark_restore || node.join.joinqual.is_nil(),
        "ExecInitMergeJoin (nodeMergejoin.c): skip_mark_restore with joinqual"
    );
    let mcx = estate.es_query_cxt;
    let ps_ExprContext = estate.exec_assign_expr_context();
    let mj_OuterEContext = estate.create_expr_context();
    let mj_InnerEContext = estate.create_expr_context();

    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);
    let mj_MarkedTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(inner_desc.clone()), TupleSlotKind::MinimalTuple);
    let mj_FillOuter = matches!(
        node.join.jointype,
        JoinType::JOIN_LEFT | JoinType::JOIN_ANTI | JoinType::JOIN_FULL
    );
    let mj_FillInner = matches!(
        node.join.jointype,
        JoinType::JOIN_RIGHT | JoinType::JOIN_RIGHT_ANTI | JoinType::JOIN_FULL
    );
    let mut mj_ConstFalseJoin = false;
    if matches!(
        node.join.jointype,
        JoinType::JOIN_RIGHT | JoinType::JOIN_RIGHT_ANTI | JoinType::JOIN_FULL
    ) && !check_constant_qual(&node.join.joinqual, &mut mj_ConstFalseJoin)
    {
        return Err(non_mergeable_join_cond(node.join.jointype));
    }
    let mut null_slot = |desc: &Rc<TupleDescData<'static>>, estate: &mut EStateData<'mcx>| {
        let slot_id =
            estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);
        exectuples::exec_store_all_null_tuple(
            &mut estate.es_tupleTable[slot_id.0 as usize],
            estate.es_query_cxt,
        );
        slot_id
    };
    let mj_NullInnerTupleSlot =
        if mj_FillOuter { Some(null_slot(inner_desc, estate)) } else { None };
    let mj_NullOuterTupleSlot =
        if mj_FillInner { Some(null_slot(outer_desc, estate)) } else { None };

    let params = estate.param_bind();
    let (proj, otherqual, joinqual) =
        ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
            let proj = exec_build_projection_info_subplans(
                mcx,
                &node.join.plan.targetlist,
                None,
                params,
                env,
            )?;
            let otherqual = exec_init_qual_subplans(mcx, &node.join.plan.qual, params, env)?;
            let joinqual = exec_init_qual_subplans(mcx, &node.join.joinqual, params, env)?;
            Ok((proj, otherqual, joinqual))
        })?;

    let clauses = examine_quals(node, estate, mj_OuterEContext, mj_InnerEContext)?;

    // ExtraMarks only helps a Material inner without REWIND.
    let mj_ExtraMarks =
        inner_is_material && (eflags & EXEC_FLAG_REWIND) == 0 && !node.skip_mark_restore;

    Ok(MergeJoinState {
        plan: node,
        ps_ExprContext,
        mj_OuterEContext,
        mj_InnerEContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        proj,
        joinqual,
        otherqual,
        clauses,
        mj_JoinState: EXEC_MJ_INITIALIZE_OUTER,
        mj_SkipMarkRestore: node.skip_mark_restore,
        mj_ExtraMarks,
        js_single_match: node.join.inner_unique
            || node.join.jointype == JoinType::JOIN_SEMI,
        mj_ConstFalseJoin,
        mj_FillOuter,
        mj_FillInner,
        mj_NullInnerTupleSlot,
        mj_NullOuterTupleSlot,
        mj_MatchedOuter: false,
        mj_MatchedInner: false,
        mj_OuterTupleSlot: None,
        mj_InnerTupleSlot: None,
        mj_MarkedTupleSlot,
        js_instr: if estate.es_instrument != 0 {
            Some(u32::try_from(node.join.plan.plan_node_id).expect("plan_node_id is non-negative"))
        } else {
            None
        },
    })
}

/// C shields the inner child with `EXEC_FLAG_MARK` unless `skip_mark_restore`.
pub fn inner_child_eflags(eflags: i32, skip_mark_restore: bool) -> i32 {
    if skip_mark_restore {
        eflags
    } else {
        eflags | EXEC_FLAG_MARK
    }
}

// check_constant_qual (nodeMergejoin.c): the planner throws away non-constant
// terms ANDed with a constant false, so a surviving non-Const term is an error.
fn check_constant_qual(qual: &::types_nodes::list::NodeList<'_>, is_const_false: &mut bool) -> bool {
    for n in qual.iter() {
        let Some(con) = n.as_const() else {
            return false;
        };
        if con.constisnull || !con.constvalue.as_bool() {
            *is_const_false = true;
        }
    }
    true
}

// The two ExecMergeJoin sort-order guards (NEXTINNER compare>0,
// nodeMergejoin.c:902; TESTOUTER compare<0, nodeMergejoin.c:1145) port as
// ERRORS, not can't-happen panics: `elog(ERROR, "mergejoin input data is out
// of order")` is user-reachable on misdeclared collations/opfamilies
// (LANE-MERGEJOIN contract §2.1).
#[track_caller]
#[cold]
#[inline(never)]
fn mergejoin_out_of_order() -> Box<PgError> {
    Box::new(PgError::error("mergejoin input data is out of order".to_string()))
}

#[track_caller]
#[cold]
#[inline(never)]
fn non_mergeable_join_cond(jointype: JoinType) -> Box<PgError> {
    let kind = if jointype == JoinType::JOIN_FULL { "FULL" } else { "RIGHT" };
    Box::new(
        PgError::error(format!(
            "{kind} JOIN is only supported with merge-joinable join conditions"
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn examine_quals<'mcx>(
    node: &'mcx MergeJoin<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_ecxt: EcxtId,
    inner_ecxt: EcxtId,
) -> PgResult<::mcx::PgVec<'mcx, MergeJoinClause<'mcx>>> {
    let mcx = estate.es_query_cxt;
    let params = estate.param_bind();
    let n = node.mergeclauses.len();
    assert_eq!(n, node.mergeFamilies.len());
    let mut out: ::mcx::PgVec<'mcx, MergeJoinClause<'mcx>> = ::mcx::PgVec::new_in(mcx);
    out.reserve(n);
    for (i, qual) in node.mergeclauses.iter().enumerate() {
        let op = qual.as_op_expr().filter(|o| o.args.len() == 2).unwrap_or_else(|| {
            panic!("MJExamineQuals (nodeMergejoin.c): mergeclause is not a binary OpExpr")
        });
        // C MJExamineQuals compiles the operands with the MergeJoinState
        // parent, so SubPlans are legal in them.
        let (mut lexpr, mut rexpr) =
            ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
                let l = exec_init_expr_subplans(mcx, Some(op.args.nth(0)), params, env)?
                    .expect("mergeclause left operand");
                let r = exec_init_expr_subplans(mcx, Some(op.args.nth(1)), params, env)?
                    .expect("mergeclause right operand");
                Ok((l, r))
            })?;
        // C evaluates outer key exprs in mj_OuterEContext's per-tuple memory
        // and inner key exprs in mj_InnerEContext's (MJEvalOuterValues /
        // MJEvalInnerValues); by-ref results ride the armed result mcx.
        // SAFETY: both ExprContexts outlive the programs (same estate).
        unsafe { lexpr.arm_result_mcx_raw(estate.ecxt(outer_ecxt).per_tuple_mcx()) };
        unsafe { rexpr.arm_result_mcx_raw(estate.ecxt(inner_ecxt).per_tuple_mcx()) };

        let opfamily = node.mergeFamilies[i];
        let collation = node.mergeCollations[i];
        let reversed = node.mergeReversals[i];
        let nulls_first = node.mergeNullsFirst[i];

        // Comparator resolve keys on (lefttype, righttype) like C: a
        // cross-type clause resolves to the BTORDER_PROC shim (live via
        // mj_compare's per-tuple mcx).
        let (op_strategy, lefttype, righttype) =
            lsyscache::amop::get_op_opfamily_properties(op.opno, opfamily, false)?;
        assert!(
            op_strategy == lsyscache::COMPARE_EQ,
            "cannot merge using non-equality operator {}",
            op.opno
        );
        let comparator =
            ::tuplesort::comparator_for_opfamily(opfamily, lefttype, righttype, collation)?;
        let ssup = SortSupport {
            ssup_collation: collation,
            ssup_reverse: reversed,
            ssup_nulls_first: nulls_first,
            ssup_attno: 0,
            comparator,
        };

        out.push(MergeJoinClause {
            lexpr,
            rexpr,
            ldatum: Datum::null(),
            rdatum: Datum::null(),
            lisnull: true,
            risnull: true,
            ssup,
        });
    }
    Ok(out)
}

fn eval_outer_values<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<MJEvalResult> {
    let Some(slot_id) = node.mj_OuterTupleSlot else {
        return Ok(MJEvalResult::EndOfJoin);
    };
    estate.reset_expr_context(node.mj_OuterEContext);
    let mut result = MJEvalResult::Matchable;
    for i in 0..node.clauses.len() {
        let v = ::executils::exec_eval_expr_with_subplans_outer_slot(
            &mut node.clauses[i].lexpr,
            estate,
            node.mj_OuterEContext,
            slot_id,
        )?;
        node.clauses[i].ldatum = v.value;
        node.clauses[i].lisnull = v.isnull;
        if v.isnull {
            // A fill-outer join must still emit NULL-keyed outers.
            if i == 0 && !node.clauses[0].ssup.ssup_nulls_first && !node.mj_FillOuter {
                return Ok(MJEvalResult::EndOfJoin);
            }
            if result == MJEvalResult::Matchable {
                result = MJEvalResult::NonMatchable;
            }
        }
    }
    Ok(result)
}

fn eval_inner_values<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: Option<ExecSlotId>,
) -> PgResult<MJEvalResult> {
    let Some(slot_id) = slot_id else {
        return Ok(MJEvalResult::EndOfJoin);
    };
    estate.reset_expr_context(node.mj_InnerEContext);
    let mut result = MJEvalResult::Matchable;
    for i in 0..node.clauses.len() {
        let v = ::executils::exec_eval_expr_with_subplans_inner_slot(
            &mut node.clauses[i].rexpr,
            estate,
            node.mj_InnerEContext,
            slot_id,
        )?;
        node.clauses[i].rdatum = v.value;
        node.clauses[i].risnull = v.isnull;
        if v.isnull {
            if i == 0 && !node.clauses[0].ssup.ssup_nulls_first && !node.mj_FillInner {
                return Ok(MJEvalResult::EndOfJoin);
            }
            if result == MJEvalResult::Matchable {
                result = MJEvalResult::NonMatchable;
            }
        }
    }
    Ok(result)
}

// MJCompare: btree 3-way over the loaded merge keys; a NULL-vs-NULL column
// keeps scanning but forces "unequal" (advance inner) if all columns tie, as
// does a constant-false joinqual (the rescan logic depends on it). Comparator
// shims (cross-type merges) allocate in the per-tuple context, as C.
fn mj_compare(node: &mut MergeJoinState<'_>, estate: &mut EStateData<'_>) -> i32 {
    estate.reset_expr_context(node.ps_ExprContext);
    let mcx = estate.ecxt(node.ps_ExprContext).per_tuple_mcx();
    let mut result = 0i32;
    let mut nulleqnull = false;
    for c in &node.clauses {
        if c.lisnull && c.risnull {
            nulleqnull = true;
            continue;
        }
        result = apply_sort_comparator_in(mcx, c.ldatum, c.lisnull, c.rdatum, c.risnull, &c.ssup);
        if result != 0 {
            break;
        }
    }
    if result == 0 && (nulleqnull || node.mj_ConstFalseJoin) {
        result = 1;
    }
    result
}

fn eval_qual<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: Qual,
) -> PgResult<bool> {
    let inner_id = node.mj_InnerTupleSlot.expect("inner slot set");
    eval_qual_subplan_aware(node, estate, which, inner_id)
}

fn eval_qual_with<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: Qual,
    inner_id: ExecSlotId,
) -> PgResult<bool> {
    let outer_id = node.mj_OuterTupleSlot.expect("outer slot set");
    let table = &mut estate.es_tupleTable[..];
    let [inner, outer] = table
        .get_disjoint_mut([inner_id.0 as usize, outer_id.0 as usize])
        .expect("distinct in-range merge slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
    let state = match which {
        Qual::Join => node.joinqual.as_deref_mut(),
        Qual::Other => node.otherqual.as_deref_mut(),
    };
    exec_qual(state, &mut slots)
}

#[inline(always)]
fn eval_qual_subplan_aware<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: Qual,
    inner_id: ExecSlotId,
) -> PgResult<bool> {
    // C ExecQual(NULL) returns true before any slot access.
    let qual = match which {
        Qual::Join => node.joinqual.as_deref(),
        Qual::Other => node.otherqual.as_deref(),
    };
    let Some(qual) = qual else {
        return Ok(true);
    };
    // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter.
    let deps = qual.param_exec_deps();
    if !deps.is_empty() {
        ::executils::exec_eval_param_exec_params(estate, deps)?;
    }
    let qual = match which {
        Qual::Join => node.joinqual.as_deref(),
        Qual::Other => node.otherqual.as_deref(),
    }
    .expect("qual checked above");
    let has_sub = qual.has_subplan();
    if has_sub {
        let outer_id = node.mj_OuterTupleSlot.expect("outer slot set");
        let ecxt = node.ps_ExprContext;
        {
            let e = estate.ecxt_mut(ecxt);
            e.ecxt_innertuple = Some(inner_id);
            e.ecxt_outertuple = Some(outer_id);
        }
        let state = match which {
            Qual::Join => node.joinqual.as_deref_mut(),
            Qual::Other => node.otherqual.as_deref_mut(),
        };
        return ::executils::exec_qual_with_subplans(state, estate, ecxt);
    }
    eval_qual_with(node, estate, which, inner_id)
}

fn project_result<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
    let inner_id = node.mj_InnerTupleSlot.expect("inner slot set");
    project_result_with(node, estate, inner_id)
}

fn project_result_with<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
    inner_id: ExecSlotId,
) -> PgResult<ExecSlotId> {
    // ExecEvalParamExec pending-initplan arm, hoisted out of the interpreter.
    let deps = node.proj.param_exec_deps();
    if !deps.is_empty() {
        ::executils::exec_eval_param_exec_params(estate, deps)?;
    }
    if node.proj.has_subplan() {
        let outer_id = node.mj_OuterTupleSlot.expect("outer slot set");
        let ecxt = node.ps_ExprContext;
        {
            let e = estate.ecxt_mut(ecxt);
            e.ecxt_innertuple = Some(inner_id);
            e.ecxt_outertuple = Some(outer_id);
        }
        let result_id = node.ps_ResultTupleSlot;
        ::executils::exec_project_with_subplans(&mut node.proj, estate, ecxt, result_id)?;
        return Ok(result_id);
    }
    let mcx = estate.es_query_cxt;
    let outer_id = node.mj_OuterTupleSlot.expect("outer slot set");
    let result_id = node.ps_ResultTupleSlot;
    let table = &mut estate.es_tupleTable[..];
    let [inner, outer, result] = table
        .get_disjoint_mut([inner_id.0 as usize, outer_id.0 as usize, result_id.0 as usize])
        .expect("distinct in-range merge slot ids");
    let mut slots = EvalSlots { scan: None, inner: Some(inner), outer: Some(outer) };
    exec_project(&mut node.proj, &mut slots, result, mcx)?;
    Ok(result_id)
}

/// MJSORT adopted-pair projection (lanev2/runtime_mergejoin — the "merge
/// join after sort" car): project one joined (outer, inner) row pair
/// through the node's OWN projection state. The arm refuses joinqual /
/// otherqual shapes at admission ([`mjsort_has_quals`]), so projection is
/// the whole per-row body; the per-tuple context resets per pull — the
/// FSM's own loop-entry cadence, kept so per-tuple memory never
/// accumulates across an adopted emit stream.
pub fn mjsort_project<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
    inner_id: ExecSlotId,
) -> PgResult<ExecSlotId> {
    estate.reset_expr_context(node.ps_ExprContext);
    node.mj_OuterTupleSlot = Some(outer_id);
    node.mj_InnerTupleSlot = Some(inner_id);
    project_result_with(node, estate, inner_id)
}

/// MJSORT admission probe: any joinqual/otherqual present? (The adopted
/// drive never evaluates quals — shapes carrying them refuse to the FSM.)
pub fn mjsort_has_quals(node: &MergeJoinState<'_>) -> bool {
    node.joinqual.is_some() || node.otherqual.is_some()
}

fn mark_inner_tuple<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let src_id = node.mj_InnerTupleSlot.expect("inner slot to mark");
    let dst_id = node.mj_MarkedTupleSlot;
    let table = &mut estate.es_tupleTable[..];
    let [dst, src] = table
        .get_disjoint_mut([dst_id.0 as usize, src_id.0 as usize])
        .expect("distinct in-range mark slot ids");
    exectuples::exec_copy_slot(dst, src, mcx, mcx)
}

// MJFillOuter: null-extended outer emission (otherqual over outer + nulls).
fn mj_fill_outer<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let null_inner = node.mj_NullInnerTupleSlot.expect("null inner slot");
    if eval_qual_subplan_aware(node, estate, Qual::Other, null_inner)? {
        return Ok(Some(project_result_with(node, estate, null_inner)?));
    }
    estate.instr_count_filtered2(node.js_instr);
    estate.reset_expr_context(node.ps_ExprContext);
    Ok(None)
}

// MJFillInner: null-extended inner emission (otherqual over nulls + inner).
fn mj_fill_inner<'mcx>(
    node: &mut MergeJoinState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let null_outer = node.mj_NullOuterTupleSlot.expect("null outer slot");
    let saved = node.mj_OuterTupleSlot;
    node.mj_OuterTupleSlot = Some(null_outer);
    let emit = (|| -> PgResult<Option<ExecSlotId>> {
        if eval_qual(node, estate, Qual::Other)? {
            return Ok(Some(project_result(node, estate)?));
        }
        estate.instr_count_filtered2(node.js_instr);
        estate.reset_expr_context(node.ps_ExprContext);
        Ok(None)
    })();
    node.mj_OuterTupleSlot = saved;
    emit
}

/// `ExecMergeJoin`, INNER/LEFT/RIGHT/SEMI/ANTI/FULL.
pub fn exec_merge_join<'mcx, O, I>(
    node: &mut MergeJoinState<'mcx>,
    outer: &mut O,
    inner: &mut I,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>
where
    O: MergeJoinOuter<'mcx>,
    I: MergeJoinInner<'mcx>,
{
    cfi()?;
    estate.reset_expr_context(node.ps_ExprContext);

    loop {
        match node.mj_JoinState {
            EXEC_MJ_INITIALIZE_OUTER => {
                node.mj_OuterTupleSlot = outer.exec_proc(estate)?;
                match eval_outer_values(node, estate)? {
                    MJEvalResult::Matchable => node.mj_JoinState = EXEC_MJ_INITIALIZE_INNER,
                    MJEvalResult::NonMatchable => {
                        // Fetch next outer; a fill-outer join emits this one.
                        if node.mj_FillOuter {
                            if let Some(r) = mj_fill_outer(node, estate)? {
                                return Ok(Some(r));
                            }
                        }
                    }
                    MJEvalResult::EndOfJoin => {
                        if node.mj_FillInner {
                            // C: no inner tuple has been fetched yet, so
                            // MatchedInner=true forces ENDOUTER to advance
                            // the inner scan before null-filling.
                            node.mj_JoinState = EXEC_MJ_ENDOUTER;
                            node.mj_MatchedInner = true;
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
            EXEC_MJ_INITIALIZE_INNER => {
                node.mj_InnerTupleSlot = inner.exec_proc(estate)?;
                match eval_inner_values(node, estate, node.mj_InnerTupleSlot)? {
                    MJEvalResult::Matchable => node.mj_JoinState = EXEC_MJ_SKIP_TEST,
                    MJEvalResult::NonMatchable => {
                        if node.mj_ExtraMarks {
                            inner.mark_pos(estate)?;
                        }
                        // Fetch next inner; a fill-inner join emits this one.
                        if node.mj_FillInner {
                            if let Some(r) = mj_fill_inner(node, estate)? {
                                return Ok(Some(r));
                            }
                        }
                    }
                    MJEvalResult::EndOfJoin => {
                        if node.mj_FillOuter {
                            // C: MatchedOuter=false makes ENDINNER emit the
                            // just-fetched outer before advancing.
                            node.mj_JoinState = EXEC_MJ_ENDINNER;
                            node.mj_MatchedOuter = false;
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
            EXEC_MJ_SKIP_TEST => {
                let cmp = mj_compare(node, estate);
                if cmp == 0 {
                    if !node.mj_SkipMarkRestore {
                        inner.mark_pos(estate)?;
                    }
                    mark_inner_tuple(node, estate)?;
                    node.mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if cmp < 0 {
                    node.mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                } else {
                    node.mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                }
            }
            EXEC_MJ_SKIPOUTER_ADVANCE => {
                if node.mj_FillOuter && !node.mj_MatchedOuter {
                    node.mj_MatchedOuter = true;
                    if let Some(r) = mj_fill_outer(node, estate)? {
                        return Ok(Some(r));
                    }
                }
                node.mj_OuterTupleSlot = outer.exec_proc(estate)?;
                node.mj_MatchedOuter = false;
                match eval_outer_values(node, estate)? {
                    MJEvalResult::Matchable => node.mj_JoinState = EXEC_MJ_SKIP_TEST,
                    MJEvalResult::NonMatchable => {}
                    MJEvalResult::EndOfJoin => {
                        if node.mj_FillInner && node.mj_InnerTupleSlot.is_some() {
                            node.mj_JoinState = EXEC_MJ_ENDOUTER;
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
            EXEC_MJ_SKIPINNER_ADVANCE => {
                if node.mj_FillInner && !node.mj_MatchedInner {
                    node.mj_MatchedInner = true;
                    if let Some(r) = mj_fill_inner(node, estate)? {
                        return Ok(Some(r));
                    }
                }
                if node.mj_ExtraMarks {
                    inner.mark_pos(estate)?;
                }
                node.mj_InnerTupleSlot = inner.exec_proc(estate)?;
                node.mj_MatchedInner = false;
                match eval_inner_values(node, estate, node.mj_InnerTupleSlot)? {
                    MJEvalResult::Matchable => node.mj_JoinState = EXEC_MJ_SKIP_TEST,
                    MJEvalResult::NonMatchable => {}
                    MJEvalResult::EndOfJoin => {
                        if node.mj_FillOuter && node.mj_OuterTupleSlot.is_some() {
                            node.mj_JoinState = EXEC_MJ_ENDINNER;
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
            EXEC_MJ_JOINTUPLES => {
                node.mj_JoinState = EXEC_MJ_NEXTINNER;
                let matched =
                    !node.mj_ConstFalseJoin && eval_qual(node, estate, Qual::Join)?;
                if matched {
                    node.mj_MatchedOuter = true;
                    node.mj_MatchedInner = true;
                    // An antijoin never returns a matched tuple.
                    if node.plan.join.jointype == JoinType::JOIN_ANTI {
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                        estate.reset_expr_context(node.ps_ExprContext);
                        continue;
                    }
                    if node.js_single_match {
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                    // A right-antijoin never returns a matched tuple; unless
                    // inner_unique, stay on this outer tuple and keep
                    // scanning the inner side for matches.
                    if node.plan.join.jointype == JoinType::JOIN_RIGHT_ANTI {
                        estate.reset_expr_context(node.ps_ExprContext);
                        continue;
                    }
                    if eval_qual(node, estate, Qual::Other)? {
                        return Ok(Some(project_result(node, estate)?));
                    }
                    estate.instr_count_filtered2(node.js_instr);
                } else {
                    estate.instr_count_filtered1(node.js_instr);
                }
                estate.reset_expr_context(node.ps_ExprContext);
            }
            EXEC_MJ_NEXTINNER => {
                if node.mj_FillInner && !node.mj_MatchedInner {
                    node.mj_MatchedInner = true;
                    if let Some(r) = mj_fill_inner(node, estate)? {
                        return Ok(Some(r));
                    }
                }
                // NB: no ExtraMarks here -- we may still restore to the mark.
                node.mj_InnerTupleSlot = inner.exec_proc(estate)?;
                node.mj_MatchedInner = false;
                match eval_inner_values(node, estate, node.mj_InnerTupleSlot)? {
                    MJEvalResult::Matchable => {
                        let cmp = mj_compare(node, estate);
                        if cmp == 0 {
                            node.mj_JoinState = EXEC_MJ_JOINTUPLES;
                        } else if cmp < 0 {
                            node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                        } else {
                            // nodeMergejoin.c:902 — ERROR, not panic (§2.1).
                            return Err(mergejoin_out_of_order());
                        }
                    }
                    MJEvalResult::NonMatchable => node.mj_JoinState = EXEC_MJ_NEXTOUTER,
                    MJEvalResult::EndOfJoin => {
                        node.mj_InnerTupleSlot = None;
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                }
            }
            EXEC_MJ_NEXTOUTER => {
                if node.mj_FillOuter && !node.mj_MatchedOuter {
                    node.mj_MatchedOuter = true;
                    if let Some(r) = mj_fill_outer(node, estate)? {
                        return Ok(Some(r));
                    }
                }
                node.mj_OuterTupleSlot = outer.exec_proc(estate)?;
                node.mj_MatchedOuter = false;
                match eval_outer_values(node, estate)? {
                    MJEvalResult::Matchable => node.mj_JoinState = EXEC_MJ_TESTOUTER,
                    MJEvalResult::NonMatchable => {}
                    MJEvalResult::EndOfJoin => {
                        if node.mj_FillInner && node.mj_InnerTupleSlot.is_some() {
                            node.mj_JoinState = EXEC_MJ_ENDOUTER;
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
            EXEC_MJ_TESTOUTER => {
                let marked = Some(node.mj_MarkedTupleSlot);
                eval_inner_values(node, estate, marked)?;
                let cmp = mj_compare(node, estate);
                if cmp == 0 {
                    if !node.mj_SkipMarkRestore {
                        inner.restr_pos(estate)?;
                        // ExecRestrPos gives no slot back: the marked slot
                        // stands in for the current inner, as C.
                        node.mj_InnerTupleSlot = Some(node.mj_MarkedTupleSlot);
                    }
                    node.mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if cmp > 0 {
                    match eval_inner_values(node, estate, node.mj_InnerTupleSlot)? {
                        MJEvalResult::Matchable => node.mj_JoinState = EXEC_MJ_SKIP_TEST,
                        MJEvalResult::NonMatchable => {
                            node.mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE
                        }
                        MJEvalResult::EndOfJoin => {
                            if node.mj_FillOuter {
                                node.mj_JoinState = EXEC_MJ_ENDINNER;
                            } else {
                                return Ok(None);
                            }
                        }
                    }
                } else {
                    // nodeMergejoin.c:1145 — ERROR, not panic (§2.1).
                    return Err(mergejoin_out_of_order());
                }
            }
            // EXEC_MJ_ENDOUTER: outer exhausted; null-fill remaining inners.
            EXEC_MJ_ENDOUTER => {
                debug_assert!(node.mj_FillInner);
                if !node.mj_MatchedInner {
                    node.mj_MatchedInner = true;
                    if let Some(r) = mj_fill_inner(node, estate)? {
                        return Ok(Some(r));
                    }
                }
                if node.mj_ExtraMarks {
                    inner.mark_pos(estate)?;
                }
                node.mj_InnerTupleSlot = inner.exec_proc(estate)?;
                node.mj_MatchedInner = false;
                if node.mj_InnerTupleSlot.is_none() {
                    return Ok(None);
                }
            }
            // EXEC_MJ_ENDINNER: inner exhausted; null-fill remaining outers.
            EXEC_MJ_ENDINNER => {
                debug_assert!(node.mj_FillOuter);
                if !node.mj_MatchedOuter {
                    node.mj_MatchedOuter = true;
                    if let Some(r) = mj_fill_outer(node, estate)? {
                        return Ok(Some(r));
                    }
                }
                node.mj_OuterTupleSlot = outer.exec_proc(estate)?;
                node.mj_MatchedOuter = false;
                if node.mj_OuterTupleSlot.is_none() {
                    return Ok(None);
                }
            }
            other => {
                panic!("ExecMergeJoin (nodeMergejoin.c): unrecognized state {other}")
            }
        }
    }
}

/// `ExecEndMergeJoin`: children ended by the caller.
pub fn exec_end_merge_join(node: &mut MergeJoinState<'_>) {
    node.joinqual = None;
    node.otherqual = None;
    node.clauses.clear();
    node.proj.release_frames();
    node.ps_ResultTupleDesc = None;
}

/// `ExecReScanMergeJoin` node-local half; the caller rescans both children.
pub fn exec_rescan_merge_join<'mcx>(node: &mut MergeJoinState<'mcx>, estate: &mut EStateData<'mcx>) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.mj_MarkedTupleSlot), mcx);
    node.mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    node.mj_MatchedOuter = false;
    node.mj_MatchedInner = false;
    node.mj_OuterTupleSlot = None;
    node.mj_InnerTupleSlot = None;
}

/// `ExecGetResultType` for a MergeJoin node.
pub fn merge_join_result_type(node: &MergeJoinState<'_>) -> Rc<TupleDescData<'static>> {
    node.ps_ResultTupleDesc.clone().expect("merge join already ended")
}

// Exempt: all released in exec_end_merge_join; SortSupport no-drop, proven below.
const _: () = assert!(!core::mem::needs_drop::<SortSupport>());
mcx::forget_safe_struct!(
    MergeJoinClause<'_> { ldatum, rdatum, lisnull, risnull;
        lexpr, rexpr, ssup },
    MergeJoinState<'_> { plan, ps_ExprContext, mj_OuterEContext,
        mj_InnerEContext, ps_ResultTupleSlot, mj_JoinState,
        mj_SkipMarkRestore, mj_ExtraMarks, js_single_match, mj_ConstFalseJoin, mj_FillOuter,
        mj_FillInner, mj_NullInnerTupleSlot, mj_NullOuterTupleSlot,
        mj_MatchedOuter, mj_MatchedInner, mj_OuterTupleSlot,
        mj_InnerTupleSlot, mj_MarkedTupleSlot, js_instr;
        ps_ResultTupleDesc, proj, joinqual, otherqual, clauses },
);
