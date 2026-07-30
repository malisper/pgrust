use alloc::boxed::Box;
use alloc::format;

use ::mcx::{Allocator, Mcx, PgBox, PgVec};
use ::types_core::fmgr::FnExprErased;
use ::types_core::{Oid, FUNC_MAX_ARGS};
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_TOO_MANY_ARGUMENTS,
};
use ::types_fmgr::{FmNodePtr, FmgrInfo, TRACK_FUNC_ALL, TRACK_FUNC_OFF};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::primnodes::{Param, ParamKind, Var, VarReturningType};
use ::types_nodes::NodeTag;
use ::types_portal::params::ParamBind;
use ::types_tuple::TupleDescData;

use core::ptr::NonNull;

use crate::steps::{
    AggPerGroup, CmpOp, ExprState, FuncCall, FuncFrame, Kernel, OutRef, SlotSrc, Step,
    EEO_FLAG_IS_QUAL,
};

// Bindings into the AggState's once-allocated result arrays.
#[derive(Clone, Copy)]
pub struct AggBind {
    pub values: NonNull<::datum::Datum>,
    pub nulls: NonNull<bool>,
    pub naggs: u16,
    // EEOP_GROUPING_FUNC cell; None = no grouping sets (C's NIL clauses).
    pub grouping: Option<NonNull<crate::steps::GroupedColsCell>>,
}

pub struct AggTransSpec<'a, 'mcx> {
    pub transfn_oid: Oid,
    // DO_AGGSPLIT_COMBINE: transfn_oid holds the combinefn; the single arg is
    // a transition value, deserialized first when deserialfn_oid != 0.
    pub combine: bool,
    pub deserialfn_oid: Oid,
    pub inputcollid: Oid,
    pub init_value_is_null: bool,
    // C build_aggregate_transfn_expr's arg types: [transtype, input types..].
    pub arg_types: &'a [Oid],
    pub args: &'a NodeList<'mcx>,
    pub aggfilter: Option<Node<'mcx>>,
    pub pergroup: NonNull<AggPerGroup>,
    pub transtype_byval: bool,
    pub transtype_len: i16,
    pub ordered: Option<AggOrderedSpec>,
    // C execExprInterp's "set up aggstate->curpertrans for AggGetAggref()":
    // erased &'query Aggref + pertrans aggshared, armed for ordered-set aggs.
    pub cur_agg: Option<(NonNull<()>, bool)>,
}

// Non-presorted DISTINCT/ORDER BY spec (ExecBuildAggTrans ordered arms): the
// program evaluates args into nodeagg-owned scratch and marks the row live;
// nodeagg feeds the pertrans tuplesort and replays the transfn at the group
// boundary (process_ordered_aggregate_single/multi).
#[derive(Clone, Copy)]
pub struct AggOrderedSpec {
    pub scratch: NonNull<::datum::NullableDatum>,
    pub num_trans_inputs: u16,
    pub flag: NonNull<bool>,
}

// WindowAgg projection binding: same result arrays, indexed by wfuncno,
// resolved by node identity (wfuncnos assigned at ExecInitWindowAgg).
#[derive(Clone, Copy)]
pub struct WinBind<'a, 'mcx> {
    pub agg: AggBind,
    pub wfuncnos: &'a [(Node<'mcx>, u16)],
}

#[derive(Clone, Copy)]
pub(crate) enum Bind<'a, 'mcx> {
    Agg(AggBind),
    Win(WinBind<'a, 'mcx>),
}

pub const INNER_VAR: i32 = -1;
pub const OUTER_VAR: i32 = -2;
pub const INDEX_VAR: i32 = -3;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("execexpr: {what} not ported")
}

// unported: user-reachable unported-feature legs raise a clean
// ERRCODE_FEATURE_NOT_SUPPORTED error instead of panicking; invariant breaks
// keep the loud `unported` panic above.
#[track_caller]
#[cold]
#[inline(never)]
fn feature_unported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{what} is not yet implemented"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

// C ExprEvalPushStep's growth shape: 16 steps up front (new_in), doubling.
#[inline(always)]
pub(crate) fn push_step(state: &mut ExprState<'_>, mcx: Mcx<'_>, step: Step) -> PgResult<()> {
    if state.steps.len() == state.steps.capacity() {
        grow_steps(state, mcx)?;
    }
    state.steps.push(step);
    Ok(())
}

#[cold]
#[inline(never)]
fn grow_steps(state: &mut ExprState<'_>, mcx: Mcx<'_>) -> PgResult<()> {
    let add = state.steps.capacity().max(16);
    state
        .steps
        .try_reserve(add)
        .map_err(|_| mcx.oom(add * core::mem::size_of::<Step>()))?;
    Ok(())
}

/// C ExecInitSubPlan linkage, type-erased against the execexpr<->execmain
/// crate cycle: `estate` is a live `*mut EStateData` the caller must not
/// alias during compile; `init` builds a query-lifetime SubPlanState.
#[derive(Clone, Copy)]
pub struct SubplanCompileEnv {
    pub estate: NonNull<()>,
    /// None when the query has no subplans (the env still carries the
    /// rtable/junk-tlist legs); a SubPlan node reaching compile then louds.
    pub init: Option<for<'x> unsafe fn(NonNull<()>, Node<'x>, Option<AggBind>) -> PgResult<NonNull<()>>>,
    /// Parent Agg's result-array binding: Aggrefs inside the SubPlan's
    /// testexpr/args compile against the owning AggState (C parent PlanState).
    pub agg: Option<AggBind>,
    /// C ExprState.parent, reduced to what EEOP_WHOLEROW consumes: the
    /// executor range table (RTE eref aliases for the RECORD leg). Raw and
    /// 'static-restamped against the same crate cycle as `estate`; lives in
    /// es_query_cxt for the whole plan.
    pub rtable: Option<NonNull<[&'static ::types_nodes::parsenodes::RangeTblEntry<'static>]>>,
    /// The SubqueryScan/CteScan parent's subplan targetlist (C
    /// ExecInitWholeRowVar's junk-filter source); None under other parents.
    pub parent_subplan_tlist: Option<NonNull<NodeList<'static>>>,
}

/// C `ExecInitExpr` (parent-less form; PlanState vocab is the execProcnode
/// unit). NULL expression -> None, as C.
pub fn exec_init_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    params: ParamBind<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    exec_init_expr_subplans(mcx, node, params, None)
}

/// [`exec_init_expr`] with SubPlan compile support wired.
pub fn exec_init_expr_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    exec_init_expr_subplans_agg(mcx, node, params, sub, None)
}

/// [`exec_init_expr_subplans`] under a parent Agg binding (SubPlan testexpr).
pub fn exec_init_expr_subplans_agg<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
    agg: Option<AggBind>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    let Some(node) = node else {
        return Ok(None);
    };
    let mut state = ExprState::new_boxed_in(mcx)?;
    create_expr_setup_steps(&mut state, mcx, &[node], agg.map(Bind::Agg), params, sub)?;
    let rout = state.result_out();
    init_expr_rec(node, &mut state, mcx, rout, agg.map(Bind::Agg), params, sub)?;
    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    Ok(Some(state))
}

/// [`exec_init_expr`] permitting an externally-supplied CaseTestExpr value
/// (C EEOP_CASE_TESTVAL's econtext caseValue leg, the JSON_TABLE colvalexpr
/// shape): the caller writes the cell via [`ExprState::set_case_test`]
/// before each evaluation.
pub fn exec_init_expr_with_case_test<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    params: ParamBind<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    let Some(node) = node else {
        return Ok(None);
    };
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.allow_ext_case_test = true;
    create_expr_setup_steps(&mut state, mcx, &[node], None, params, None)?;
    let rout = state.result_out();
    init_expr_rec(node, &mut state, mcx, rout, None, params, None)?;
    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    Ok(Some(state))
}

/// C `ExecInitQual`: implicit-AND qual list, empty -> None.
pub fn exec_init_qual<'mcx>(
    mcx: Mcx<'mcx>,
    qual: &NodeList<'mcx>,
    params: ParamBind<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    exec_init_qual_subplans(mcx, qual, params, None)
}

/// [`exec_init_qual`] with SubPlan compile support wired.
pub fn exec_init_qual_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    qual: &NodeList<'mcx>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    if qual.is_nil() {
        return Ok(None);
    }
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;
    create_expr_setup_steps(&mut state, mcx, qual.as_slice(), None, params, sub)?;

    for node in qual.iter() {
        let rout = state.result_out();
    init_expr_rec(node, &mut state, mcx, rout, None, params, sub)?;
        push_step(&mut state, mcx, Step::Qual { jumpdone: u32::MAX })?;
    }
    let done = state.steps.len() as u32;
    for step in state.steps.iter_mut() {
        if let Step::Qual { jumpdone } = step {
            debug_assert_eq!(*jumpdone, u32::MAX);
            *jumpdone = done;
        }
    }
    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    // Qual programs run outside exec_project's arming; by-ref-allocating
    // callees get the init context (the exec_project_with_subplans
    // convention; C uses the per-tuple context — leak-shaped divergence).
    state.arm_result_mcx(mcx);
    Ok(Some(state))
}

/// `ExecInitQual` with an Agg parent: Aggrefs bind to the AggState's result
/// arrays (nodeAgg HAVING qual).
pub fn exec_build_agg_qual<'mcx>(
    mcx: Mcx<'mcx>,
    qual: &NodeList<'mcx>,
    agg: AggBind,
    params: ParamBind<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    exec_build_agg_qual_subplans(mcx, qual, agg, params, None)
}

/// [`exec_build_agg_qual`] with SubPlan compile support wired.
pub fn exec_build_agg_qual_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    qual: &NodeList<'mcx>,
    agg: AggBind,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    if qual.is_nil() {
        return Ok(None);
    }
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;
    create_expr_setup_steps(&mut state, mcx, qual.as_slice(), Some(Bind::Agg(agg)), params, sub)?;

    for node in qual.iter() {
        let rout = state.result_out();
        init_expr_rec(node, &mut state, mcx, rout, Some(Bind::Agg(agg)), params, sub)?;
        push_step(&mut state, mcx, Step::Qual { jumpdone: u32::MAX })?;
    }
    let done = state.steps.len() as u32;
    for step in state.steps.iter_mut() {
        if let Step::Qual { jumpdone } = step {
            debug_assert_eq!(*jumpdone, u32::MAX);
            *jumpdone = done;
        }
    }
    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    Ok(Some(state))
}

/// C `ExecBuildProjectionInfo` minus the ProjectionInfo/ExprContext wrapper
/// (execUtils unit): the result slot is bound at [`crate::exec_project`] time.
pub fn exec_build_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, None, params, None)
}

/// [`exec_build_projection_info`] with SubPlan compile support wired.
pub fn exec_build_projection_info_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, None, params, sub)
}

/// [`exec_build_projection_info_subplans`] for a MERGE's RETURNING list:
/// permits MERGE_SUPPORT_FUNC steps (C gates on state->parent being a
/// CMD_MERGE ModifyTableState).
pub fn exec_build_merge_projection_info_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info_ext(mcx, target_list, input_desc, None, params, sub, true)
}

/// Agg-node projection: Aggrefs bound to the AggState's result arrays.
pub fn exec_build_agg_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: AggBind,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, Some(Bind::Agg(agg)), params, None)
}

/// [`exec_build_agg_projection_info`] with SubPlan compile support wired.
pub fn exec_build_agg_projection_info_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: AggBind,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, Some(Bind::Agg(agg)), params, sub)
}

/// WindowAgg-node projection: WindowFuncs bound to the result arrays by
/// wfuncno (C EEOP_WINDOW_FUNC over ExecBuildProjectionInfo).
pub fn exec_build_window_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    win: WinBind<'_, 'mcx>,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, Some(Bind::Win(win)), params, None)
}

/// [`exec_build_window_projection_info`] with SubPlan compile support wired.
pub fn exec_build_window_projection_info_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    win: WinBind<'_, 'mcx>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, Some(Bind::Win(win)), params, sub)
}

fn build_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info_ext(mcx, target_list, input_desc, agg, params, sub, false)
}

fn build_projection_info_ext<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
    merge_support: bool,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.allow_merge_support = merge_support;
    create_expr_setup_steps(&mut state, mcx, target_list.as_slice(), agg, params, sub)?;

    for tle_node in target_list.iter() {
        let tle = tle_node.as_target_entry().unwrap_or_else(|| {
            panic!("expected TargetEntry, got tag {:?}", tle_node.node_tag())
        });
        let mut safe_var: Option<&Var<'_>> = None;
        if let Some(variable) = tle.expr.as_var() {
            if variable.varattno > 0 {
                match input_desc {
                    None => safe_var = Some(variable),
                    Some(desc) => {
                        if (variable.varattno as i32) <= desc.natts {
                            let attr = &desc.attrs[(variable.varattno - 1) as usize];
                            if !attr.attisdropped && variable.vartype == attr.atttypid {
                                safe_var = Some(variable);
                            }
                        }
                    }
                }
            }
        }

        if let Some(variable) = safe_var {
            let attnum = (variable.varattno - 1) as u16;
            let resultnum = (tle.resno - 1) as u16;
            let step = match variable.varno {
                INNER_VAR => Step::AssignInnerVar { attnum, resultnum },
                OUTER_VAR => Step::AssignOuterVar { attnum, resultnum },
                _ => match variable.varreturningtype {
                    VarReturningType::VAR_RETURNING_DEFAULT => {
                        Step::AssignScanVar { attnum, resultnum }
                    }
                    VarReturningType::VAR_RETURNING_OLD => {
                        state.flags |= crate::steps::EEO_FLAG_HAS_OLD;
                        Step::AssignOldVar { attnum, resultnum }
                    }
                    VarReturningType::VAR_RETURNING_NEW => {
                        state.flags |= crate::steps::EEO_FLAG_HAS_NEW;
                        Step::AssignNewVar { attnum, resultnum }
                    }
                },
            };
            push_step(&mut state, mcx, step)?;
        } else {
            let rout = state.result_out();
            init_expr_rec(tle.expr, &mut state, mcx, rout, agg, params, sub)?;
            let resultnum = (tle.resno - 1) as u16;
            let step = if lsyscache::get_typlen(expr_type(tle.expr))? == -1 {
                Step::AssignTmpMakeRo { resultnum }
            } else {
                Step::AssignTmp { resultnum }
            };
            push_step(&mut state, mcx, step)?;
        }
    }

    push_step(&mut state, mcx, Step::DoneNoReturn)?;
    ready_expr(&mut state);
    Ok(state)
}

/// C `ExecBuildAggTrans`, AGG_PLAIN one-set byval slice; unported trans
/// shapes panic at build. `agg_node` rides every transfn fcinfo's `context`.
pub fn exec_build_agg_trans<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_agg_trans(mcx, specs, PergroupMode::Fixed, agg_node, params, None)
}

/// [`exec_build_agg_trans`] with SubPlan compile support wired (aggregated
/// arguments holding SubPlans, e.g. an outer-level agg over a sublink).
pub fn exec_build_agg_trans_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_agg_trans(mcx, specs, PergroupMode::Fixed, agg_node, params, sub)
}

/// Grouping-sets variant: args evaluated once per transno, one trans call
/// per set; pergroup(setno, transno) = set_bases[setno] + transno.
pub fn exec_build_agg_trans_gsets<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    set_bases: &[NonNull<AggPerGroup>],
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_agg_trans(mcx, specs, PergroupMode::Sets(set_bases), agg_node, params, None)
}

enum PergroupMode<'a> {
    Fixed,
    Indirect(NonNull<NonNull<AggPerGroup>>),
    Sets(&'a [NonNull<AggPerGroup>]),
    // C's dosort+dohash program: Sets bases plus one Indirect cell per hash set.
    Mixed(&'a [NonNull<AggPerGroup>], &'a [NonNull<NonNull<AggPerGroup>>]),
}

pub fn exec_build_agg_trans_mixed<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    set_bases: &[NonNull<AggPerGroup>],
    cells: &[NonNull<NonNull<AggPerGroup>>],
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_agg_trans(mcx, specs, PergroupMode::Mixed(set_bases, cells), agg_node, params, None)
}

/// AGG_HASHED variant: pergroup resolves per tuple through `base`, the cell
/// nodeAgg repoints at the current hash entry's pergroup array (spec order is
/// transno order).
pub fn exec_build_agg_trans_hashed<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    base: NonNull<NonNull<AggPerGroup>>,
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_agg_trans(mcx, specs, PergroupMode::Indirect(base), agg_node, params, None)
}

/// [`exec_build_agg_trans_hashed`] with SubPlan compile support wired.
pub fn exec_build_agg_trans_hashed_subplans<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    base: NonNull<NonNull<AggPerGroup>>,
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_agg_trans_masked(mcx, specs, None, PergroupMode::Indirect(base), agg_node, params, sub)
}

/// [`exec_build_agg_trans_hashed_subplans`] over the `keep`-masked subset of
/// `specs` (`keep` is parallel to `specs`): masked-out transitions get no
/// steps, kept ones keep their ORIGINAL index as the indirect pergroup
/// `transno` offset — so the program can run beside a batched fold that
/// covers the complement (lane-v2 hash-agg breaker residual program).
pub fn exec_build_agg_trans_hashed_masked<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    keep: &[bool],
    base: NonNull<NonNull<AggPerGroup>>,
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    debug_assert_eq!(specs.len(), keep.len());
    build_agg_trans_masked(
        mcx,
        specs,
        Some(keep),
        PergroupMode::Indirect(base),
        agg_node,
        params,
        sub,
    )
}

/// [`exec_build_agg_trans_subplans`] over the `keep`-masked subset of `specs`
/// (`keep` is parallel to `specs`): masked-out transitions get no steps, kept
/// ones keep their ORIGINAL `spec.pergroup` fixed target — so the program can
/// run beside a batched fold that covers the complement (lane-v2 plain-agg
/// breaker residual program).
pub fn exec_build_agg_trans_plain_masked<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    keep: &[bool],
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    debug_assert_eq!(specs.len(), keep.len());
    build_agg_trans_masked(mcx, specs, Some(keep), PergroupMode::Fixed, agg_node, params, sub)
}

// The tag proves the FmNodePtr is an AggStateNode (WindowAgg passes None).
fn agg_state_node(agg_node: FmNodePtr) -> PgResult<NonNull<::types_fmgr::AggStateNode>> {
    let Some(p) = agg_node else {
        // unported: by-ref transtype without an AggState (nodeWindowAgg lane).
        return Err(feature_unported(
            "aggregate with a by-reference transition type outside an Agg node \
             (nodeWindowAgg lane)",
        ));
    };
    // SAFETY: build-time read of the caller's live node header.
    assert!(
        unsafe { p.as_ref().tag } == ::types_fmgr::T_AGG_STATE,
        "build_agg_trans: by-ref trans context is not an AggStateNode"
    );
    Ok(p.cast())
}

fn build_agg_trans<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    mode: PergroupMode<'_>,
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_agg_trans_masked(mcx, specs, None, mode, agg_node, params, sub)
}

// `keep` = None compiles every spec (all pre-existing callers); Some(mask)
// compiles only the marked specs, preserving each spec's original index as
// its transno (indirect pergroup offset).
fn build_agg_trans_masked<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    keep: Option<&[bool]>,
    mode: PergroupMode<'_>,
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let kept = |transno: usize| keep.is_none_or(|k| k[transno]);
    let mut state = ExprState::new_boxed_in(mcx)?;
    let mut info = SetupInfo::default();
    for (transno, spec) in specs.iter().enumerate() {
        if !kept(transno) {
            continue;
        }
        for tle in spec.args.iter() {
            setup_walker(tle, &mut info);
        }
        if let Some(f) = spec.aggfilter {
            setup_walker(f, &mut info);
        }
    }
    push_expr_setup_steps(&mut state, mcx, &info, None, params, sub)?;

    for (transno, spec) in specs.iter().enumerate() {
        if !kept(transno) {
            continue;
        }
        // C's numTransInputs: resjunk cells (aggpresorted ORDER BY sort
        // columns) are evaluated nowhere and take no transfn arg slot.
        let num_trans_inputs = spec
            .args
            .iter()
            .filter(|n| !n.as_target_entry().is_some_and(|t| t.resjunk))
            .count();
        let nargs = num_trans_inputs + 1;
        if nargs > FUNC_MAX_ARGS {
            return Err(too_many_args(nargs));
        }
        let mut flinfo = fmgr_core::fmgr_info(spec.transfn_oid)?;
        // SAFETY: arg_types is arena-backed for the query (leaked into
        // es_query_cxt by the caller) and this flinfo dies with the plan it
        // serves — from_node_ref's contract; the carrier stays drop-free.
        let argtypes: &'static [Oid] = unsafe { core::mem::transmute(spec.arg_types) };
        // C build_aggregate_transfn_expr: the fake FuncExpr returns the
        // transition type (carrier slot 0). Unit rigs pass an empty slice;
        // InvalidOid keeps polymorphic resolution declining as before.
        let agg_argtypes = ::mcx::alloc_leak_in(
            mcx,
            ::types_core::fmgr::AggFnArgTypes {
                rettype: argtypes.first().copied().unwrap_or(::types_core::InvalidOid),
                argtypes,
            },
        )?;
        // SAFETY: agg_argtypes is arena-backed for the query, see above.
        flinfo.fn_expr = Some(unsafe { FnExprErased::from_node_ref(agg_argtypes) });
        if flinfo.fn_retset {
            return Err(retset_error());
        }
        let init_strict = flinfo.fn_strict && spec.init_value_is_null;
        let fn_strict = flinfo.fn_strict;
        if let Some(ord) = spec.ordered {
            build_agg_trans_ordered(&mut state, mcx, spec, ord, fn_strict, params)?;
            continue;
        }
        let frame = FuncFrame::new_in(mcx, flinfo, nargs as u16, spec.inputcollid)?;
        // SAFETY: fresh frame image; the caller's agg_node outlives the program.
        unsafe { crate::steps::fcinfo_mut(frame.fcinfo, nargs as u16).context = agg_node };
        let frame_ix = state.frames.len() as u32;
        let call =
            FuncCall { fcinfo: frame.fcinfo, flinfo: frame.flinfo, frame: frame_ix, nargs: nargs as u16 };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);
        let mut filter_jump: Option<usize> = None;
        // When combining, all necessary filtering was done by the partial
        // stage (convert_combining_aggrefs drops the parent's filter).
        if spec.combine {
            debug_assert!(spec.aggfilter.is_none());
        } else if let Some(f) = spec.aggfilter {
            let rout = state.result_out();
            init_expr_rec(f, &mut state, mcx, rout, None, params, sub)?;
            filter_jump = Some(state.steps.len());
            push_step(
                &mut state,
                mcx,
                Step::JumpIfNotTrue { jumpdone: u32::MAX, out: rout },
            )?;
        }
        let mut ds_bailout: Option<usize> = None;
        if spec.combine && spec.deserialfn_oid != 0 {
            let ds_flinfo = fmgr_core::fmgr_info(spec.deserialfn_oid)?;
            let ds_strict = ds_flinfo.fn_strict;
            let ds_frame = FuncFrame::new_in(mcx, ds_flinfo, 2, 0)?;
            // SAFETY: fresh frame image; agg_node outlives the program.
            unsafe { crate::steps::fcinfo_mut(ds_frame.fcinfo, 2).context = agg_node };
            let ds_ix = state.frames.len() as u32;
            let ds_call = FuncCall {
                fcinfo: ds_frame.fcinfo,
                flinfo: ds_frame.flinfo,
                frame: ds_ix,
                nargs: 2,
            };
            state
                .frames
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
            state.frames.push(ds_frame);
            let tle = spec
                .args
                .iter()
                .next()
                .expect("combining Aggref has one argument")
                .as_target_entry()
                .expect("Aggref.args cell is a TargetEntry");
            // SAFETY: slots 0/1 of the 2-arg ds fcinfo image; slot 1 is C's
            // type-safety dummy, written once at build time.
            let ds_arg0 = OutRef(unsafe { crate::steps::arg_slot_of(ds_call.fcinfo, 0) });
            unsafe {
                crate::steps::arg_slot_of(ds_call.fcinfo, 1)
                    .write(::datum::NullableDatum { value: ::datum::Datum::null(), isnull: false })
            };
            init_expr_rec(tle.expr, &mut state, mcx, ds_arg0, None, params, None)?;
            // SAFETY: slot 1 of the nargs >= 2 trans fcinfo image.
            let trans_arg1 = OutRef(unsafe { crate::steps::arg_slot_of(call.fcinfo, 1) });
            if ds_strict {
                ds_bailout = Some(state.steps.len());
                push_step(
                    &mut state,
                    mcx,
                    Step::AggStrictDeserialize { call: ds_call, out: trans_arg1, jumpnull: u32::MAX },
                )?;
            } else {
                push_step(&mut state, mcx, Step::AggDeserialize { call: ds_call, out: trans_arg1 })?;
            }
        } else {
            let mut argno = 0usize;
            for tle_node in spec.args.iter() {
                let tle = tle_node.as_target_entry().unwrap_or_else(|| {
                    panic!("Aggref.args cell: expected TargetEntry, got {:?}", tle_node.node_tag())
                });
                if tle.resjunk {
                    continue;
                }
                // SAFETY: argno + 1 <= num_trans_inputs < nargs of `call.fcinfo`.
                let arg_out =
                    OutRef(unsafe { crate::steps::arg_slot_of(call.fcinfo, argno + 1) });
                init_expr_rec(tle.expr, &mut state, mcx, arg_out, None, params, sub)?;
                argno += 1;
            }
            debug_assert_eq!(argno, num_trans_inputs);
        }
        let mut bailout: Option<usize> = None;
        if fn_strict && num_trans_inputs > 0 {
            // SAFETY: slot 1 of the nargs >= 2 fcinfo image (C's &args[1]).
            let args1 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 1) };
            let step = if num_trans_inputs == 1 {
                Step::AggStrictInputCheck1 { arg: args1, jumpnull: u32::MAX }
            } else {
                Step::AggStrictInputCheck {
                    args: args1,
                    nargs: num_trans_inputs as u16,
                    jumpnull: u32::MAX,
                }
            };
            bailout = Some(state.steps.len());
            push_step(&mut state, mcx, step)?;
        }
        // By-ref trans steps (and AggSetCurrent) need the AggState; resolve it
        // once up front so the unported nodeWindowAgg lane errors cleanly.
        let byref_agg = if !spec.transtype_byval || spec.cur_agg.is_some() {
            Some(agg_state_node(agg_node)?)
        } else {
            None
        };
        // One fixed-pergroup step (byval or by-ref) — Fixed and per-set modes.
        let fixed_step = |pergroup: NonNull<AggPerGroup>| -> Step {
            if spec.transtype_byval {
                match (fn_strict, init_strict) {
                    (_, true) => Step::AggPlainTransInitStrictByVal { call, pergroup },
                    (true, false) => Step::AggPlainTransStrictByVal { call, pergroup },
                    (false, false) => Step::AggPlainTransByVal { call, pergroup },
                }
            } else {
                let byref = crate::steps::AggByRef {
                    agg: byref_agg.expect("by-ref transtype resolved the AggState above"),
                    translen: spec.transtype_len,
                };
                match (fn_strict, spec.init_value_is_null) {
                    (true, true) => Step::AggPlainTransInitStrictByRef { call, pergroup, byref },
                    (true, false) => Step::AggPlainTransStrictByRef { call, pergroup, byref },
                    (false, _) => Step::AggPlainTransByRef { call, pergroup, byref },
                }
            }
        };
        let indirect_step = |base: NonNull<NonNull<AggPerGroup>>| -> Step {
            if spec.transtype_byval {
                match (fn_strict, init_strict) {
                    (_, true) => Step::AggTransInitStrictByValIndirect {
                        call,
                        base,
                        transno: transno as u16,
                    },
                    (true, false) => Step::AggTransStrictByValIndirect {
                        call,
                        base,
                        transno: transno as u16,
                    },
                    (false, false) => {
                        Step::AggTransByValIndirect { call, base, transno: transno as u16 }
                    }
                }
            } else {
                let byref = crate::steps::AggByRef {
                    agg: byref_agg.expect("by-ref transtype resolved the AggState above"),
                    translen: spec.transtype_len,
                };
                let transno = transno as u16;
                match (fn_strict, spec.init_value_is_null) {
                    (true, true) => {
                        Step::AggTransInitStrictByRefIndirect { call, base, transno, byref }
                    }
                    (true, false) => {
                        Step::AggTransStrictByRefIndirect { call, base, transno, byref }
                    }
                    (false, _) => Step::AggTransByRefIndirect { call, base, transno, byref },
                }
            }
        };
        if let Some((aggref, shared)) = spec.cur_agg {
            push_step(
                &mut state,
                mcx,
                Step::AggSetCurrent {
                    agg: byref_agg.expect("cur_agg resolved the AggState above"),
                    aggref,
                    shared,
                },
            )?;
        }
        match &mode {
            PergroupMode::Fixed => push_step(&mut state, mcx, fixed_step(spec.pergroup))?,
            PergroupMode::Sets(bases) => {
                for &base in bases.iter() {
                    // SAFETY: transno < numtrans slots of each once-allocated
                    // per-set pergroup array (nodeAgg contract).
                    let pergroup =
                        unsafe { NonNull::new_unchecked(base.as_ptr().add(transno)) };
                    push_step(&mut state, mcx, fixed_step(pergroup))?;
                }
            }
            PergroupMode::Indirect(base) => {
                push_step(&mut state, mcx, indirect_step(*base))?;
            }
            PergroupMode::Mixed(bases, cells) => {
                for &base in bases.iter() {
                    // SAFETY: as PergroupMode::Sets.
                    let pergroup =
                        unsafe { NonNull::new_unchecked(base.as_ptr().add(transno)) };
                    push_step(&mut state, mcx, fixed_step(pergroup))?;
                }
                for &cell in cells.iter() {
                    push_step(&mut state, mcx, indirect_step(cell))?;
                }
            }
        }
        let target = state.steps.len() as u32;
        if let Some(ix) = filter_jump {
            match &mut state.steps[ix] {
                Step::JumpIfNotTrue { jumpdone, .. } => *jumpdone = target,
                _ => unreachable!(),
            }
        }
        if let Some(ix) = bailout {
            match &mut state.steps[ix] {
                Step::AggStrictInputCheck { jumpnull, .. }
                | Step::AggStrictInputCheck1 { jumpnull, .. } => *jumpnull = target,
                _ => unreachable!(),
            }
        }
        if let Some(ix) = ds_bailout {
            match &mut state.steps[ix] {
                Step::AggStrictDeserialize { jumpnull, .. } => *jumpnull = target,
                _ => unreachable!(),
            }
        }
    }
    push_step(&mut state, mcx, Step::DoneNoReturn)?;
    ready_expr(&mut state);
    Ok(state)
}

// ExecBuildAggTrans non-presorted DISTINCT/ORDER BY arms: every arg (junk
// sort columns included) lands in the pertrans scratch; strict transfns skip
// rows with null trans inputs at sort-insert time (C
// EEOP_AGG_STRICT_INPUT_CHECK_NULLS), then the mark step flags the row for
// nodeagg's tuplesort feed.
fn build_agg_trans_ordered<'mcx>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    spec: &AggTransSpec<'_, 'mcx>,
    ord: crate::compile::AggOrderedSpec,
    fn_strict: bool,
    params: ParamBind<'mcx>,
) -> PgResult<()> {
    debug_assert!(ord.num_trans_inputs as usize <= spec.args.len());
    // C evaluates the FILTER before the aggregated arguments; a false filter
    // skips the row entirely (the parked args never get marked live).
    let mut filter_jump: Option<usize> = None;
    if let Some(f) = spec.aggfilter {
        let rout = state.result_out();
        init_expr_rec(f, state, mcx, rout, None, params, None)?;
        filter_jump = Some(state.steps.len());
        push_step(state, mcx, Step::JumpIfNotTrue { jumpdone: u32::MAX, out: rout })?;
    }
    for (argno, tle_node) in spec.args.iter().enumerate() {
        let tle = tle_node.as_target_entry().unwrap_or_else(|| {
            panic!("Aggref.args cell: expected TargetEntry, got {:?}", tle_node.node_tag())
        });
        // SAFETY: argno < the nodeagg-owned num-inputs scratch array length.
        let out =
            OutRef(unsafe { NonNull::new_unchecked(ord.scratch.as_ptr().add(argno)) });
        init_expr_rec(tle.expr, state, mcx, out, None, params, None)?;
    }
    let mut bailout: Option<usize> = None;
    if fn_strict && ord.num_trans_inputs > 0 {
        let step = if ord.num_trans_inputs == 1 {
            Step::AggStrictInputCheck1 { arg: ord.scratch, jumpnull: u32::MAX }
        } else {
            Step::AggStrictInputCheck {
                args: ord.scratch,
                nargs: ord.num_trans_inputs,
                jumpnull: u32::MAX,
            }
        };
        bailout = Some(state.steps.len());
        push_step(state, mcx, step)?;
    }
    push_step(state, mcx, Step::AggOrderedMark { flag: ord.flag })?;
    let target = state.steps.len() as u32;
    if let Some(ix) = bailout {
        match &mut state.steps[ix] {
            Step::AggStrictInputCheck { jumpnull, .. }
            | Step::AggStrictInputCheck1 { jumpnull, .. } => *jumpnull = target,
            _ => unreachable!(),
        }
    }
    if let Some(ix) = filter_jump {
        match &mut state.steps[ix] {
            Step::JumpIfNotTrue { jumpdone, .. } => *jumpdone = target,
            _ => unreachable!(),
        }
    }
    Ok(())
}

/// C `ExecBuildHash32FromAttrs` (execExpr.c): hash the inner slot's
/// `key_col_idx` attnums (1-based) through the given hash-proc oids, combining
/// per-column values by rotate-xor; resolve-once frames, murmur finish is the
/// caller's (execGrouping.c contract).
pub fn exec_build_hash32_from_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &TupleDescData<'_>,
    hash_fn_oids: &[Oid],
    collations: &[Oid],
    key_col_idx: &[i16],
    init_value: u32,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    debug_assert!(hash_fn_oids.len() == key_col_idx.len() && collations.len() == key_col_idx.len());
    let num_cols = key_col_idx.len();
    let mut state = ExprState::new_boxed_in(mcx)?;

    let iresult = if num_cols as u64 + (init_value != 0) as u64 > 1 {
        Some(alloc_nullable_datum(mcx)?)
    } else {
        None
    };

    let last_attnum = key_col_idx.iter().copied().max().unwrap_or(0);
    if last_attnum > 0 {
        push_step(&mut state, mcx, Step::InnerFetchSome { last_var: last_attnum as u16 })?;
    }

    let mut first = true;
    if init_value != 0 {
        let out = if num_cols > 0 {
            OutRef(iresult.expect("multi-part hash requires an intermediate slot"))
        } else {
            state.result_out()
        };
        push_step(
            &mut state,
            mcx,
            Step::HashDatumSetInitVal { init_value: ::datum::Datum::from_u32(init_value), out },
        )?;
        first = false;
    }
    // Zero key columns with no init value (empty-select-list set ops): the
    // hash is the constant 0, C's uninitialized-iteration result.
    if num_cols == 0 && init_value == 0 {
        let out = state.result_out();
        push_step(
            &mut state,
            mcx,
            Step::Const { value: ::datum::Datum::from_u32(0), isnull: false, out },
        )?;
    }

    for i in 0..num_cols {
        let attnum = (key_col_idx[i] - 1) as u16;
        let flinfo = fmgr_core::fmgr_info(hash_fn_oids[i])?;
        let frame = FuncFrame::new_in(mcx, flinfo, 1, collations[i])?;
        let frame_ix = state.frames.len() as u32;
        let call = FuncCall { fcinfo: frame.fcinfo, flinfo: frame.flinfo, frame: frame_ix, nargs: 1 };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);

        // SAFETY: arg 0 of the frame's freshly allocated 1-arg fcinfo.
        let arg_out = OutRef(unsafe { crate::steps::arg_slot_of(call.fcinfo, 0) });
        let vartype = desc.attrs[attnum as usize].atttypid;
        push_step(&mut state, mcx, Step::InnerVar { attnum, vartype, out: arg_out })?;

        let out = if i == num_cols - 1 {
            state.result_out()
        } else {
            OutRef(iresult.expect("multi-part hash requires an intermediate slot"))
        };
        let step = if first {
            Step::HashDatumFirst { call, out }
        } else {
            Step::HashDatumNext32 {
                call,
                iresult: iresult.expect("NEXT32 requires an intermediate slot"),
                out,
            }
        };
        push_step(&mut state, mcx, step)?;
        first = false;
    }

    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    state.arm_result_mcx(mcx);
    Ok(state)
}

/// C `ExecBuildHash32Expr` (execExpr.c), serial hashjoin arm: hash arbitrary
/// key expressions of the hashed slot, non-strict fold (NULL keys hash as 0,
/// the recheck rejects them). C binds the hashed slot as ecxt_outertuple; our
/// hashjoin eval sites bind it as the inner slot, so outer-slot reads are
/// remapped to inner reads (datum-identical). All-plain-Var keys take the
/// [`exec_build_hash32_from_attrs`] path.
pub fn exec_build_hash32_from_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &TupleDescData<'_>,
    hash_exprs: &NodeList<'mcx>,
    hash_fn_oids: &[Oid],
    collations: &[Oid],
    init_value: u32,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let num_cols = hash_exprs.len();
    debug_assert!(hash_fn_oids.len() == num_cols && collations.len() == num_cols);
    'exprs: {
        let mut attnums: PgVec<'mcx, i16> = PgVec::new_in(mcx);
        attnums.try_reserve(num_cols).map_err(|_| mcx.oom(num_cols * 2))?;
        for k in hash_exprs.iter() {
            match k.as_var() {
                Some(v) if v.varattno > 0 => attnums.push(v.varattno),
                _ => break 'exprs,
            }
        }
        return exec_build_hash32_from_attrs(
            mcx,
            desc,
            hash_fn_oids,
            collations,
            &attnums,
            init_value,
        );
    }

    let mut state = ExprState::new_boxed_in(mcx)?;
    let iresult = if num_cols as u64 + (init_value != 0) as u64 > 1 {
        Some(alloc_nullable_datum(mcx)?)
    } else {
        None
    };

    let mut info = SetupInfo::default();
    for k in hash_exprs.iter() {
        setup_walker(k, &mut info);
    }
    assert!(info.last_scan == 0, "ExecBuildHash32Expr: scan-slot Var in a hash key");
    debug_assert!(info.multiexpr_subplans.is_empty(), "MULTIEXPR SubPlan in a hash key");
    let last_var = info.last_inner.max(info.last_outer);
    if last_var > 0 {
        push_step(&mut state, mcx, Step::InnerFetchSome { last_var: last_var as u16 })?;
    }

    let mut first = true;
    if init_value != 0 {
        let out = if num_cols > 0 {
            OutRef(iresult.expect("multi-part hash requires an intermediate slot"))
        } else {
            state.result_out()
        };
        push_step(
            &mut state,
            mcx,
            Step::HashDatumSetInitVal { init_value: ::datum::Datum::from_u32(init_value), out },
        )?;
        first = false;
    }

    for (i, k) in hash_exprs.iter().enumerate() {
        let flinfo = fmgr_core::fmgr_info(hash_fn_oids[i])?;
        let frame = FuncFrame::new_in(mcx, flinfo, 1, collations[i])?;
        let frame_ix = state.frames.len() as u32;
        let call = FuncCall { fcinfo: frame.fcinfo, flinfo: frame.flinfo, frame: frame_ix, nargs: 1 };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);

        // SAFETY: arg 0 of the frame's freshly allocated 1-arg fcinfo.
        let arg_out = OutRef(unsafe { crate::steps::arg_slot_of(call.fcinfo, 0) });
        // C ExecBuildHash32Expr compiles key exprs with the parent planstate,
        // so SubPlans are legal in hash keys.
        init_expr_rec(k, &mut state, mcx, arg_out, None, params, sub)?;

        let out = if i == num_cols - 1 {
            state.result_out()
        } else {
            OutRef(iresult.expect("multi-part hash requires an intermediate slot"))
        };
        let step = if first {
            Step::HashDatumFirst { call, out }
        } else {
            Step::HashDatumNext32 {
                call,
                iresult: iresult.expect("NEXT32 requires an intermediate slot"),
                out,
            }
        };
        push_step(&mut state, mcx, step)?;
        first = false;
    }

    for s in state.steps.iter_mut() {
        match *s {
            Step::OuterVar { attnum, vartype, out } => {
                *s = Step::InnerVar { attnum, vartype, out }
            }
            Step::OuterSysVar { attnum, out } => *s = Step::InnerSysVar { attnum, out },
            Step::WholeRow { src: SlotSrc::Outer, wr, frame, out } => {
                *s = Step::WholeRow { src: SlotSrc::Inner, wr, frame, out }
            }
            _ => {}
        }
    }

    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    state.arm_result_mcx(mcx);
    Ok(state)
}

/// C `ExecBuildGroupingEqual` (execExpr.c): NOT DISTINCT comparison of the
/// inner (input) and outer (table) slots on `key_col_idx`, compared last
/// column first as C does; evaluated via [`crate::exec_qual`].
pub fn exec_build_grouping_equal<'mcx>(
    mcx: Mcx<'mcx>,
    ldesc: &TupleDescData<'_>,
    rdesc: &TupleDescData<'_>,
    key_col_idx: &[i16],
    eqfuncoids: &[Oid],
    collations: &[Oid],
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    debug_assert!(eqfuncoids.len() == key_col_idx.len() && collations.len() == key_col_idx.len());
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;

    // C pushes an initial TRUE result: with zero key columns (empty-select-
    // list set ops) every pair matches and no fetch/compare steps exist.
    if key_col_idx.is_empty() {
        let rout = state.result_out();
        push_step(&mut state, mcx, Step::Const {
            value: ::datum::Datum::from_bool(true),
            isnull: false,
            out: rout,
        })?;
        push_step(&mut state, mcx, Step::DoneReturn)?;
        ready_expr(&mut state);
        return Ok(state);
    }

    let maxatt = key_col_idx.iter().copied().max().unwrap();
    push_step(&mut state, mcx, Step::InnerFetchSome { last_var: maxatt as u16 })?;
    push_step(&mut state, mcx, Step::OuterFetchSome { last_var: maxatt as u16 })?;

    let userid = miscinit_seams::get_user_id::call();
    for natt in (0..key_col_idx.len()).rev() {
        let attno = key_col_idx[natt];
        let attnum = (attno - 1) as u16;
        let foid = eqfuncoids[natt];
        let aclresult =
            aclchk_seams::object_aclcheck::call(PROCEDURE_RELATION_ID, foid, userid, ACL_EXECUTE)?;
        if aclresult != ACLCHECK_OK {
            return Err(permission_denied(mcx, foid)?);
        }
        let flinfo = fmgr_core::fmgr_info(foid)?;
        let frame = FuncFrame::new_in(mcx, flinfo, 2, collations[natt])?;
        let frame_ix = state.frames.len() as u32;
        let call = FuncCall { fcinfo: frame.fcinfo, flinfo: frame.flinfo, frame: frame_ix, nargs: 2 };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);

        // SAFETY: args 0/1 of the frame's freshly allocated 2-arg fcinfo.
        let (arg0, arg1) = unsafe {
            (
                OutRef(crate::steps::arg_slot_of(call.fcinfo, 0)),
                OutRef(crate::steps::arg_slot_of(call.fcinfo, 1)),
            )
        };
        let ltype = ldesc.attrs[attnum as usize].atttypid;
        let rtype = rdesc.attrs[attnum as usize].atttypid;
        push_step(&mut state, mcx, Step::InnerVar { attnum, vartype: ltype, out: arg0 })?;
        push_step(&mut state, mcx, Step::OuterVar { attnum, vartype: rtype, out: arg1 })?;
        let rout = state.result_out();
        push_step(&mut state, mcx, Step::NotDistinct { call, out: rout })?;
        push_step(&mut state, mcx, Step::Qual { jumpdone: u32::MAX })?;
    }

    let done = state.steps.len() as u32;
    for step in state.steps.iter_mut() {
        if let Step::Qual { jumpdone } = step {
            debug_assert_eq!(*jumpdone, u32::MAX);
            *jumpdone = done;
        }
    }
    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    Ok(state)
}

pub(crate) fn alloc_nullable_datum(mcx: Mcx<'_>) -> PgResult<NonNull<::datum::NullableDatum>> {
    let layout = core::alloc::Layout::new::<::datum::NullableDatum>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<::datum::NullableDatum> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(::datum::NullableDatum::null()) };
    Ok(p)
}

/// C `exprType` over the ported primnode families.
pub fn expr_type(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_Param => node.as_param().unwrap().paramtype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_NullIfExpr => node.as_null_if_expr().unwrap().opresulttype,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wintype,
        NodeTag::T_GroupingFunc => 23,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxtype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().r#type,
        NodeTag::T_MergeSupportFunc => node.as_merge_support_func().unwrap().msftype,
        NodeTag::T_XmlExpr => {
            use ::types_nodes::primnodes::XmlExprOp;
            let x = node.as_xml_expr().unwrap();
            match x.op {
                XmlExprOp::IS_DOCUMENT => 16,
                XmlExprOp::IS_XMLSERIALIZE => 25,
                _ => ::types_core::catalog::XMLOID,
            }
        }
        NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_BooleanTest
        | NodeTag::T_DistinctExpr => 16,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().refrestype,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
        NodeTag::T_RowCompareExpr => 16,
        NodeTag::T_FieldSelect => node.as_field_select().unwrap().resulttype,
        NodeTag::T_FieldStore => node.as_field_store().unwrap().resulttype,
        NodeTag::T_NextValueExpr => {
            node.as_variant::<::types_nodes::primnodes::NextValueExpr>().unwrap().typeId
        }
        NodeTag::T_SubPlan => {
            use ::types_nodes::primnodes::SubLinkType;
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                SubLinkType::EXPR_SUBLINK => sp.firstColType,
                SubLinkType::ARRAY_SUBLINK => ::lsyscache::get_promoted_array_type(sp.firstColType)
                    .expect("array type resolved at plan time"),
                // C: a MULTIEXPR SubPlan returns a dummy NULL::record.
                SubLinkType::MULTIEXPR_SUBLINK => ::types_core::RECORDOID,
                _ => 16,
            }
        }
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeId,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_ArrayCoerceExpr => node.as_array_coerce_expr().unwrap().resulttype,
        NodeTag::T_ConvertRowtypeExpr => node.as_convert_rowtype_expr().unwrap().resulttype,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttype,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().typeId,
        NodeTag::T_JsonValueExpr => {
            expr_type(node.as_json_value_expr().unwrap().formatted_expr.expect("formatted_expr"))
        }
        NodeTag::T_JsonConstructorExpr => {
            node.as_json_constructor_expr().unwrap().returning.expect("returning").typid
        }
        NodeTag::T_JsonIsPredicate => ::types_core::catalog::BOOLOID,
        NodeTag::T_JsonExpr => node.as_json_expr().unwrap().returning.expect("returning").typid,
        NodeTag::T_ReturningExpr => expr_type(node.as_returning_expr().unwrap().retexpr),
        NodeTag::T_SubLink => {
            use ::types_nodes::primnodes::SubLinkType;
            let sl = node.as_sub_link().unwrap();
            match sl.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => {
                    let tent = sl
                        .subselect
                        .as_query()
                        .unwrap_or_else(|| panic!("cannot get type for untransformed sublink"))
                        .targetList
                        .first()
                        .expect("sublink target list")
                        .as_target_entry()
                        .expect("tlist entry");
                    let ty = expr_type(tent.expr);
                    if sl.subLinkType == SubLinkType::ARRAY_SUBLINK {
                        let arraytype = ::lsyscache::get_promoted_array_type(ty)
                            .unwrap_or_else(|e| panic!("get_promoted_array_type({ty}): {e}"));
                        assert!(
                            arraytype != ::types_core::InvalidOid,
                            "could not find array type for data type {ty}"
                        );
                        arraytype
                    } else {
                        ty
                    }
                }
                SubLinkType::MULTIEXPR_SUBLINK => ::types_core::catalog::RECORDOID,
                _ => 16,
            }
        }
        tag => panic!("execexpr exprType: node family {tag:?} not ported"),
    }
}

// C ExecInitSubPlanExpr (execExpr.c): compile the parParam arg expressions
// into EEOP_PARAM_SET steps, then the EEOP_SUBPLAN step itself.
fn init_subplan_expr<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let sp = node.as_sub_plan().expect("SubPlan node");
    let Some(env) = sub else {
        panic!(
            "ExecInitSubPlanExpr (execExpr.c): SubPlan {:?} (plan_id {}) in an expression \
             context without a subplan driver (owning node not wired)",
            sp.plan_name, sp.plan_id
        )
    };
    debug_assert_eq!(sp.parParam.len(), sp.args.len());
    for (paramid, arg) in sp.parParam.iter().zip(sp.args.iter()) {
        init_expr_rec(arg, state, mcx, out, agg, params, sub)?;
        assert!(
            paramid >= 0 && (paramid as u32) < params.n_exec,
            "EEOP_PARAM_SET: paramid {paramid} outside es_param_exec_vals[0..{}]",
            params.n_exec
        );
        let base = params.exec_vals.expect("n_exec > 0 implies a base pointer");
        // SAFETY: paramid bounds-checked against the once-sized array.
        let prm = unsafe { NonNull::new_unchecked(base.as_ptr().add(paramid as usize)) };
        push_step(state, mcx, Step::ParamSet { prm, out })?;
    }
    let aggbind = match agg {
        Some(Bind::Agg(a)) => Some(a),
        _ => env.agg,
    };
    // SAFETY: env.estate is the caller's live estate (SubplanCompileEnv
    // contract: no aliasing borrows during compile).
    let init = env.init.unwrap_or_else(|| {
        panic!(
            "ExecInitSubPlanExpr (execExpr.c): SubPlan {:?} (plan_id {}) compiled in a \
             query whose PlannedStmt has no subplans",
            sp.plan_name, sp.plan_id
        )
    });
    let sstate = unsafe { init(env.estate, node, aggbind) }?;
    state.flags |= crate::steps::EEO_FLAG_HAS_SUBPLAN;
    push_step(state, mcx, Step::SubPlan { sstate, out })
}

// C ExprSetupInfo + expr_setup_walker + ExecPushExprSetupSteps. Slots are not
// knowable here (no PlanState parent), so every referenced slot gets a
// non-fixed FETCHSOME step, C's parent == NULL shape.
#[derive(Default)]
struct SetupInfo<'mcx> {
    last_inner: i16,
    last_outer: i16,
    last_scan: i16,
    last_old: i16,
    last_new: i16,
    multiexpr_subplans: Vec<Node<'mcx>>,
}

#[inline]
pub(crate) fn create_expr_setup_steps<'mcx>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    nodes: &[Node<'mcx>],
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let mut info = SetupInfo::default();
    for &n in nodes {
        setup_walker(n, &mut info);
    }
    push_expr_setup_steps(state, mcx, &info, agg, params, sub)
}

// C ExecPushExprSetupSteps: slot fetches, then any MULTIEXPR SubPlans — they
// must run before any Param referencing their outputs, after the Var fetches
// their args may need.
fn push_expr_setup_steps<'mcx>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    info: &SetupInfo<'mcx>,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    push_fetch_steps(state, mcx, info)?;
    for &sp in &info.multiexpr_subplans {
        // C: the result can be ignored, but it needs to go somewhere.
        let out = state.result_out();
        init_subplan_expr(sp, state, mcx, out, agg, params, sub)?;
    }
    Ok(())
}

#[inline]
fn push_fetch_steps<'mcx>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    info: &SetupInfo<'_>,
) -> PgResult<()> {
    if info.last_inner > 0 {
        push_step(state, mcx, Step::InnerFetchSome { last_var: info.last_inner as u16 })?;
    }
    if info.last_outer > 0 {
        push_step(state, mcx, Step::OuterFetchSome { last_var: info.last_outer as u16 })?;
    }
    if info.last_scan > 0 {
        push_step(state, mcx, Step::ScanFetchSome { last_var: info.last_scan as u16 })?;
    }
    if info.last_old > 0 {
        push_step(state, mcx, Step::OldFetchSome { last_var: info.last_old as u16 })?;
    }
    if info.last_new > 0 {
        push_step(state, mcx, Step::NewFetchSome { last_var: info.last_new as u16 })?;
    }
    Ok(())
}

fn setup_walker<'mcx>(node: Node<'mcx>, info: &mut SetupInfo<'mcx>) {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            match v.varno {
                INNER_VAR => info.last_inner = info.last_inner.max(v.varattno),
                OUTER_VAR => info.last_outer = info.last_outer.max(v.varattno),
                _ => match v.varreturningtype {
                    VarReturningType::VAR_RETURNING_DEFAULT => {
                        info.last_scan = info.last_scan.max(v.varattno)
                    }
                    VarReturningType::VAR_RETURNING_OLD => {
                        info.last_old = info.last_old.max(v.varattno)
                    }
                    VarReturningType::VAR_RETURNING_NEW => {
                        info.last_new = info.last_new.max(v.varattno)
                    }
                },
            }
        }
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr => {}
        // C expr_setup_walker: Aggref/WindowFunc args never eval in the
        // caller's econtext.
        NodeTag::T_Aggref | NodeTag::T_WindowFunc | NodeTag::T_GroupingFunc => {}
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_NullIfExpr => {
            for a in node.as_null_if_expr().unwrap().args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_TargetEntry => setup_walker(node.as_target_entry().unwrap().expr, info),
        NodeTag::T_BoolExpr => {
            for a in node.as_bool_expr().unwrap().args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in node.as_min_max_expr().unwrap().args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            for a in x.named_args.iter() {
                setup_walker(a, info);
            }
            for a in x.args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_NullTest => {
            if let Some(a) = node.as_null_test().unwrap().arg {
                setup_walker(a, info);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(a) = node.as_boolean_test().unwrap().arg {
                setup_walker(a, info);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in node.as_distinct_expr().unwrap().args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            // C expr_setup_walker: collect MULTIEXPR SubPlans for eager
            // execution in the setup steps.
            if sp.subLinkType == ::types_nodes::primnodes::SubLinkType::MULTIEXPR_SUBLINK {
                info.multiexpr_subplans.push(node);
            }
            if let Some(t) = sp.testexpr {
                setup_walker(t, info);
            }
            for a in sp.args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_CaseTestExpr => {}
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                setup_walker(a, info);
            }
            for w in c.args.iter() {
                let cw = w.as_case_when().expect("CaseWhen");
                if let Some(e) = cw.expr {
                    setup_walker(e, info);
                }
                if let Some(r) = cw.result {
                    setup_walker(r, info);
                }
            }
            if let Some(d) = c.defresult {
                setup_walker(d, info);
            }
        }
        NodeTag::T_ReturningExpr => {
            setup_walker(node.as_returning_expr().unwrap().retexpr, info)
        }
        NodeTag::T_RelabelType => setup_walker(node.as_relabel_type().unwrap().arg, info),
        NodeTag::T_CoerceViaIO => setup_walker(node.as_coerce_via_io().unwrap().arg, info),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            setup_walker(a.arg, info);
            if let Some(e) = a.elemexpr {
                setup_walker(e, info);
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            setup_walker(node.as_convert_rowtype_expr().unwrap().arg, info)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in node.as_scalar_array_op_expr().unwrap().args.iter() {
                setup_walker(a, info);
            }
        }
        NodeTag::T_ArrayExpr => {
            for e in node.as_array_expr().unwrap().elements.iter() {
                setup_walker(e, info);
            }
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            for a in sr.refupperindexpr.iter().flatten() {
                setup_walker(a, info);
            }
            for a in sr.reflowerindexpr.iter().flatten() {
                setup_walker(a, info);
            }
            if let Some(a) = sr.refexpr {
                setup_walker(a, info);
            }
            if let Some(a) = sr.refassgnexpr {
                setup_walker(a, info);
            }
        }
        NodeTag::T_RowExpr => {
            for e in node.as_row_expr().unwrap().args.iter() {
                setup_walker(e, info);
            }
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for e in rc.largs.iter().chain(rc.rargs.iter()) {
                setup_walker(e, info);
            }
        }
        NodeTag::T_FieldSelect => setup_walker(node.as_field_select().unwrap().arg, info),
        NodeTag::T_FieldStore => {
            let f = node.as_field_store().unwrap();
            setup_walker(f.arg, info);
            for e in f.newvals.iter() {
                setup_walker(e, info);
            }
        }
        NodeTag::T_CoerceToDomain => setup_walker(node.as_coerce_to_domain().unwrap().arg, info),
        NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_MergeSupportFunc => {}
        NodeTag::T_CoalesceExpr => {
            for e in node.as_coalesce_expr().unwrap().args.iter() {
                setup_walker(e, info);
            }
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                setup_walker(e, info);
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for arg in &c.args {
                setup_walker(arg, info);
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                setup_walker(e, info);
            }
        }
        NodeTag::T_JsonIsPredicate => {
            setup_walker(node.as_json_is_predicate().unwrap().expr.expect("expr"), info)
        }
        NodeTag::T_JsonBehavior => {
            if let Some(e) = node.as_json_behavior().unwrap().expr {
                setup_walker(e, info);
            }
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                setup_walker(e, info);
            }
            for v in &j.passing_values {
                setup_walker(v, info);
            }
        }
        tag => panic!("execexpr setup walker: node family {tag:?} not ported"),
    }
}

// C ExecInitWholeRowVar + ExecEvalWholeRowVar's first-eval split: the
// named-composite tupdesc resolves here (plan-stable typcache row; C defers
// to first eval only to reach the slot); the RECORD leg's descriptor waits
// for the slot at first eval. RETURNING OLD/NEW whole-rows are handled by
// the src legs below.
fn init_whole_row<'mcx>(
    variable: &::types_nodes::primnodes::Var<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    use crate::steps::{SlotSrc, WholeRowState};
    let record = variable.vartype == ::types_core::catalog::RECORDOID;
    let src = match variable.varno {
        INNER_VAR => SlotSrc::Inner,
        OUTER_VAR => SlotSrc::Outer,
        _ => match variable.varreturningtype {
            VarReturningType::VAR_RETURNING_DEFAULT => SlotSrc::Scan,
            VarReturningType::VAR_RETURNING_OLD => {
                state.flags |= crate::steps::EEO_FLAG_HAS_OLD;
                SlotSrc::Old
            }
            VarReturningType::VAR_RETURNING_NEW => {
                state.flags |= crate::steps::EEO_FLAG_HAS_NEW;
                SlotSrc::New
            }
        },
    };
    let desc_ptr: Option<NonNull<::types_tuple::TupleDescData<'static>>> = if record {
        None
    } else {
        // C ExecEvalWholeRowVar uses lookup_rowtype_tupdesc_domain: vartype
        // can be a domain over composite (execExprInterp.c:5431).
        let desc = typcache::lookup_rowtype_tupdesc_copy(
            mcx,
            lsyscache::getBaseType(variable.vartype)?,
            -1,
        )?;
        let desc_layout = core::alloc::Layout::new::<::types_tuple::TupleDescData<'static>>();
        let p: NonNull<::types_tuple::TupleDescData<'static>> =
            mcx.allocate(desc_layout).map_err(|_| mcx.oom(desc_layout.size()))?.cast();
        // SAFETY: fresh exact-layout allocation; the plan mcx outlives every
        // eval of this step, so the 'static restamp never escapes it.
        unsafe {
            p.as_ptr().write(core::mem::transmute::<
                ::types_tuple::TupleDescData<'mcx>,
                ::types_tuple::TupleDescData<'static>,
            >(desc));
        }
        Some(p)
    };
    // C ExecEvalWholeRowVar's RTE lookup (eref column aliases for the RECORD
    // output descriptor), hoisted to compile: the range table is init-stable
    // and the interpreter has no estate. INNER/OUTER varnos are negative and
    // fall outside the 1..=len guard, as C's es_range_table_size check.
    let colnames = sub
        .and_then(|s| s.rtable)
        .and_then(|rt| {
            // SAFETY: env rtable is the live es_range_table (plan-lived).
            let rt = unsafe { rt.as_ref() };
            usize::try_from(variable.varno)
                .ok()
                .and_then(|v| (1..=rt.len()).contains(&v).then(|| rt[v - 1]))
        })
        .and_then(|rte| rte.eref.map(|e| NonNull::from(&e.colnames)));
    let junk = match sub.and_then(|s| s.parent_subplan_tlist) {
        // SAFETY: env tlist is the parent's plan targetlist (plan-lived).
        Some(tl) => init_whole_row_junk(mcx, unsafe { tl.as_ref() })?,
        None => None,
    };
    let wr_layout = core::alloc::Layout::new::<WholeRowState>();
    let wr: NonNull<WholeRowState> =
        mcx.allocate(wr_layout).map_err(|_| mcx.oom(wr_layout.size()))?.cast();
    // SAFETY: fresh exact-layout allocation; 'static restamps stay behind the
    // plan-lived state.
    unsafe {
        wr.as_ptr().write(WholeRowState {
            tupdesc: desc_ptr,
            first: true,
            slow: false,
            record,
            colnames,
            junk,
            mcx: core::mem::transmute::<Mcx<'mcx>, Mcx<'static>>(mcx),
        })
    };

    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    push_step(state, mcx, Step::WholeRow { src, wr, frame: frame_ix, out })
}

// C ExecInitWholeRowVar's junk-filter leg: ExecInitJunkFilter (execJunk.c)
// over the parent subplan's targetlist, with ExecCleanTypeFromTL
// (execTuples.c) inlined — execscan's port sits above this crate.
fn init_whole_row_junk<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &NodeList<'static>,
) -> PgResult<Option<NonNull<crate::steps::WholeRowJunk>>> {
    let tle = |n: Node<'static>| {
        n.as_target_entry().expect("targetlist holds TargetEntries")
    };
    if !tlist.iter().any(|n| tle(n).resjunk) {
        return Ok(None);
    }
    let clean_len = tlist.iter().filter(|&n| !tle(n).resjunk).count();
    let mut desc = ::tupdesc::CreateTemplateTupleDesc(mcx, clean_len as i32)?;
    let mut clean_map: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    clean_map.try_reserve_exact(clean_len).map_err(|_| mcx.oom(clean_len * 2))?;
    let mut resno: i16 = 1;
    for n in tlist.iter() {
        let t = tle(n);
        if t.resjunk {
            continue;
        }
        ::tupdesc::TupleDescInitEntry(
            &mut desc,
            resno,
            t.resname,
            expr_type(t.expr),
            expr_typmod_closed(t.expr),
            0,
        )?;
        ::tupdesc::TupleDescInitEntryCollation(&mut desc, resno, ::nodes_core::expr_collation(t.expr));
        clean_map.push(t.resno);
        resno += 1;
    }
    let slot = ::exectuples::make_tuple_table_slot(
        mcx,
        ::types_slot::TupleSlotKind::Virtual,
        Some(alloc::rc::Rc::new(desc)),
    );
    let slot_layout = core::alloc::Layout::new::<::types_slot::SlotData<'mcx>>();
    let slot_ptr: NonNull<::types_slot::SlotData<'mcx>> =
        mcx.allocate(slot_layout).map_err(|_| mcx.oom(slot_layout.size()))?.cast();
    // SAFETY: fresh exact-layout allocation; plan-mcx slot behind a
    // plan-lived state, so the 'static restamp never escapes it.
    let junk = unsafe {
        slot_ptr.as_ptr().write(slot);
        crate::steps::WholeRowJunk {
            clean_map: NonNull::from(::mcx::slice_borrow_in(mcx, &clean_map)?),
            slot: core::mem::transmute::<
                NonNull<::types_slot::SlotData<'mcx>>,
                NonNull<::types_slot::SlotData<'static>>,
            >(slot_ptr),
        }
    };
    let junk_layout = core::alloc::Layout::new::<crate::steps::WholeRowJunk>();
    let junk_ptr: NonNull<crate::steps::WholeRowJunk> =
        mcx.allocate(junk_layout).map_err(|_| mcx.oom(junk_layout.size()))?.cast();
    // SAFETY: fresh exact-layout allocation.
    unsafe { junk_ptr.as_ptr().write(junk) };
    Ok(Some(junk_ptr))
}

// C ExecInitExprRec over the ported families.
pub(crate) fn init_expr_rec<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let variable = node.as_var().unwrap();
            if variable.varattno == 0 {
                return init_whole_row(variable, state, mcx, out, sub);
            }
            if variable.varattno < 0 {
                let attnum = variable.varattno;
                let step = match variable.varno {
                    INNER_VAR => Step::InnerSysVar { attnum, out },
                    OUTER_VAR => Step::OuterSysVar { attnum, out },
                    _ => match variable.varreturningtype {
                        VarReturningType::VAR_RETURNING_DEFAULT => {
                            Step::ScanSysVar { attnum, out }
                        }
                        VarReturningType::VAR_RETURNING_OLD => {
                            state.flags |= crate::steps::EEO_FLAG_HAS_OLD;
                            Step::OldSysVar { attnum, out }
                        }
                        VarReturningType::VAR_RETURNING_NEW => {
                            state.flags |= crate::steps::EEO_FLAG_HAS_NEW;
                            Step::NewSysVar { attnum, out }
                        }
                    },
                };
                return push_step(state, mcx, step);
            }
            let attnum = (variable.varattno - 1) as u16;
            let vartype = variable.vartype;
            let step = match variable.varno {
                INNER_VAR => Step::InnerVar { attnum, vartype, out },
                OUTER_VAR => Step::OuterVar { attnum, vartype, out },
                _ => match variable.varreturningtype {
                    VarReturningType::VAR_RETURNING_DEFAULT => {
                        Step::ScanVar { attnum, vartype, out }
                    }
                    VarReturningType::VAR_RETURNING_OLD => {
                        state.flags |= crate::steps::EEO_FLAG_HAS_OLD;
                        Step::OldVar { attnum, vartype, out }
                    }
                    VarReturningType::VAR_RETURNING_NEW => {
                        state.flags |= crate::steps::EEO_FLAG_HAS_NEW;
                        Step::NewVar { attnum, vartype, out }
                    }
                },
            };
            push_step(state, mcx, step)
        }
        NodeTag::T_Const => {
            let con = node.as_const().unwrap();
            push_step(
                state,
                mcx,
                Step::Const { value: con.constvalue, isnull: con.constisnull, out },
            )
        }
        NodeTag::T_Param => {
            let p = node.as_param().unwrap();
            let step = init_param(p, params, out)?;
            if p.paramkind == ParamKind::PARAM_EXEC {
                state.param_exec_deps.push(p.paramid as u32);
            }
            push_step(state, mcx, step)
        }
        NodeTag::T_FuncExpr => {
            let func = node.as_func_expr().unwrap();
            let step = init_func(
                node, &func.args, func.funcid, func.inputcollid, state, mcx, out, agg, params, sub,
            )?;
            push_step(state, mcx, step)
        }
        NodeTag::T_OpExpr => {
            let op = node.as_op_expr().unwrap();
            let step = init_func(
                node, &op.args, op.opfuncid, op.inputcollid, state, mcx, out, agg, params, sub,
            )?;
            push_step(state, mcx, step)
        }
        NodeTag::T_DistinctExpr => {
            let op = node.as_distinct_expr().unwrap();
            let step = init_func(
                node, &op.args, op.opfuncid, op.inputcollid, state, mcx, out, agg, params, sub,
            )?;
            let call = match step {
                Step::FuncExpr { call, .. }
                | Step::FuncExprStrict1 { call, .. }
                | Step::FuncExprStrict2 { call, .. }
                | Step::FuncExprStrict { call, .. } => call,
                _ => unreachable!("init_func returns a FuncExpr step"),
            };
            push_step(state, mcx, Step::Distinct { call, out })
        }
        NodeTag::T_NullIfExpr => {
            let op = node.as_null_if_expr().unwrap();
            let step = init_func(
                node, &op.args, op.opfuncid, op.inputcollid, state, mcx, out, agg, params, sub,
            )?;
            let call = match step {
                Step::FuncExpr { call, .. }
                | Step::FuncExprStrict1 { call, .. }
                | Step::FuncExprStrict2 { call, .. }
                | Step::FuncExprStrict { call, .. } => call,
                _ => unreachable!("init_func returns a FuncExpr step"),
            };
            push_step(state, mcx, Step::NullIf { call, out })
        }
        NodeTag::T_RowCompareExpr => {
            init_row_compare(node, state, mcx, out, agg, params, sub)?;
            Ok(())
        }
        NodeTag::T_BooleanTest => {
            use ::types_nodes::BoolTestType;
            let bt = node.as_boolean_test().unwrap();
            init_expr_rec(bt.arg.expect("BooleanTest.arg"), state, mcx, out, agg, params, sub)?;
            let step = match bt.booltesttype {
                BoolTestType::IS_TRUE => Step::BoolTestIsTrue { out },
                BoolTestType::IS_NOT_TRUE => Step::BoolTestIsNotTrue { out },
                BoolTestType::IS_FALSE => Step::BoolTestIsFalse { out },
                BoolTestType::IS_NOT_FALSE => Step::BoolTestIsNotFalse { out },
                BoolTestType::IS_UNKNOWN => Step::NullTestIsNull { out },
                BoolTestType::IS_NOT_UNKNOWN => Step::NullTestIsNotNull { out },
            };
            push_step(state, mcx, step)
        }
        NodeTag::T_Aggref => {
            let aggref = node.as_aggref().unwrap();
            let Some(Bind::Agg(bind)) = agg else {
                unported("EEOP_AGGREF outside an Agg projection (nodeAgg.c)");
            };
            let aggno = aggref.aggno;
            assert!(
                aggno >= 0 && (aggno as u16) < bind.naggs,
                "Aggref.aggno {aggno} outside the AggState's {} slots (planner must set it)",
                bind.naggs
            );
            // SAFETY: aggno bounds-checked against the bind's array length;
            // the arrays are allocated once and stable (steps.rs note).
            let (value, null) = unsafe {
                (
                    NonNull::new_unchecked(bind.values.as_ptr().add(aggno as usize)),
                    NonNull::new_unchecked(bind.nulls.as_ptr().add(aggno as usize)),
                )
            };
            push_step(state, mcx, Step::AggrefEval { value, null, out })
        }
        NodeTag::T_GroupingFunc => {
            let Some(Bind::Agg(bind)) = agg else {
                unported("EEOP_GROUPING_FUNC outside an Agg projection (execExpr.c)");
            };
            let g = node.as_grouping_func().unwrap();
            let cols_src = g.cols.as_slice();
            let ncols = cols_src.len();
            let cols = if bind.grouping.is_some() {
                assert!(ncols > 0, "GroupingFunc.cols unset (setrefs must remap refs)");
                let layout = core::alloc::Layout::array::<i32>(ncols).unwrap();
                let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
                let p: NonNull<i32> = raw.cast();
                // SAFETY: fresh allocation of ncols i32 slots.
                unsafe {
                    core::ptr::copy_nonoverlapping(cols_src.as_ptr(), p.as_ptr(), ncols)
                };
                p
            } else {
                NonNull::dangling()
            };
            push_step(
                state,
                mcx,
                Step::GroupingFuncEval {
                    cols,
                    ncols: ncols as u16,
                    current: bind.grouping,
                    out,
                },
            )
        }
        NodeTag::T_WindowFunc => {
            let Some(Bind::Win(win)) = agg else {
                unported("EEOP_WINDOW_FUNC outside a WindowAgg projection (nodeWindowAgg.c)");
            };
            let wfuncno = win
                .wfuncnos
                .iter()
                .find(|(n, _)| n.ptr_eq(node))
                .map(|&(_, i)| i)
                .unwrap_or_else(|| {
                    panic!("WindowFunc not registered with the WindowAggState (init order bug)")
                });
            assert!(wfuncno < win.agg.naggs);
            // SAFETY: wfuncno bounds-checked against the bind's array length;
            // the arrays are allocated once and stable (steps.rs note).
            let (value, null) = unsafe {
                (
                    NonNull::new_unchecked(win.agg.values.as_ptr().add(wfuncno as usize)),
                    NonNull::new_unchecked(win.agg.nulls.as_ptr().add(wfuncno as usize)),
                )
            };
            push_step(state, mcx, Step::AggrefEval { value, null, out })
        }
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            let step = init_minmax(node, mm, state, mcx, out, agg, params, sub)?;
            push_step(state, mcx, step)
        }
        NodeTag::T_XmlExpr => init_xml_expr(node, state, mcx, out, agg, params, sub),
        NodeTag::T_SQLValueFunction => {
            use ::types_nodes::primnodes::SQLValueFunctionOp;
            let svf = node.as_sql_value_function().unwrap();
            let size = if (svf.op as u32) >= SQLValueFunctionOp::SVFOP_CURRENT_ROLE as u32 {
                core::mem::size_of::<types_tuple::NameData>()
            } else {
                12
            };
            let layout = core::alloc::Layout::from_size_align(size, 8).expect("svf layout");
            let scratch = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
            push_step(
                state,
                mcx,
                Step::SqlValueFunction { op: svf.op, typmod: svf.typmod, scratch, out },
            )
        }
        NodeTag::T_MergeSupportFunc => {
            // C ExecInitExprRec: must be under a CMD_MERGE ModifyTableState.
            if !state.allow_merge_support {
                panic!("MergeSupportFunc found in non-merge plan node");
            }
            let cell = match state.merge_action_cell {
                Some(c) => c,
                None => {
                    let layout = core::alloc::Layout::new::<
                        Option<::types_nodes::nodes_enums::CmdType>,
                    >();
                    let c = mcx
                        .allocate(layout)
                        .map_err(|_| mcx.oom(layout.size()))?
                        .cast::<Option<::types_nodes::nodes_enums::CmdType>>();
                    // SAFETY: fresh exclusive allocation; no action armed yet.
                    unsafe { c.write(None) };
                    state.merge_action_cell = Some(c);
                    c
                }
            };
            // 10-byte text image ("INSERT"/"UPDATE"/"DELETE"), 8-aligned.
            let layout = core::alloc::Layout::from_size_align(12, 8).expect("msf layout");
            let scratch = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
            push_step(state, mcx, Step::MergeSupportFunc { action: cell, scratch, out })
        }
        NodeTag::T_BoolExpr => init_bool_expr(node, state, mcx, out, agg, params, sub),
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            // C ExecInitExprRec T_SubPlan: a MULTIEXPR SubPlan was already
            // executed by the expression's setup steps; in-tree it is only a
            // dummy NULL::record in case the tlist element is assigned.
            if sp.subLinkType == ::types_nodes::primnodes::SubLinkType::MULTIEXPR_SUBLINK {
                return push_step(
                    state,
                    mcx,
                    Step::Const { value: ::datum::Datum::null(), isnull: true, out },
                );
            }
            init_subplan_expr(node, state, mcx, out, agg, params, sub)
        }
        NodeTag::T_BoolExpr => init_bool_expr(node, state, mcx, out, agg, params, sub),
        NodeTag::T_CaseExpr => init_case_expr(node, state, mcx, out, agg, params, sub),
        NodeTag::T_CaseTestExpr => match state.innermost_case {
            Some(slot) => push_step(state, mcx, Step::CaseTestVal { slot, out }),
            None if state.allow_ext_case_test => {
                let slot = match state.ext_case_test {
                    Some(s) => s,
                    None => {
                        let s = alloc_nullable_datum(mcx)?;
                        state.ext_case_test = Some(s);
                        s
                    }
                };
                push_step(state, mcx, Step::CaseTestVal { slot, out })
            }
            // unported: EEOP_CASE_TESTVAL_EXT (externally supplied econtext
            // caseValue — domain checks / ArrayCoerceExpr).
            None => Err(feature_unported(
                "CaseTestExpr with an externally supplied test value \
                 (domain checks over ArrayCoerceExpr)",
            )),
        },
        NodeTag::T_NullTest => {
            use ::types_nodes::primnodes::NullTestType;
            let nt = node.as_null_test().unwrap();
            init_expr_rec(nt.arg.expect("NullTest.arg"), state, mcx, out, agg, params, sub)?;
            let step = if nt.argisrow {
                use crate::steps::RowNullState;
                let rn_layout = core::alloc::Layout::new::<RowNullState>();
                let rn: NonNull<RowNullState> =
                    mcx.allocate(rn_layout).map_err(|_| mcx.oom(rn_layout.size()))?.cast();
                // SAFETY: fresh exact-layout allocation; the compile mcx
                // outlives every eval of this step, so the 'static restamp
                // never escapes it.
                unsafe {
                    rn.as_ptr().write(RowNullState {
                        tup_type: ::types_core::InvalidOid,
                        tup_typmod: -1,
                        desc: None,
                        mcx: core::mem::transmute::<Mcx<'mcx>, Mcx<'static>>(mcx),
                    })
                };
                let frame_ix = state.frames.len() as u32;
                let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
                state
                    .frames
                    .try_reserve(1)
                    .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
                state.frames.push(frame);
                match nt.nulltesttype {
                    NullTestType::IS_NULL => Step::NullTestRowIsNull { rn, frame: frame_ix, out },
                    NullTestType::IS_NOT_NULL => {
                        Step::NullTestRowIsNotNull { rn, frame: frame_ix, out }
                    }
                }
            } else {
                match nt.nulltesttype {
                    NullTestType::IS_NULL => Step::NullTestIsNull { out },
                    NullTestType::IS_NOT_NULL => Step::NullTestIsNotNull { out },
                }
            };
            push_step(state, mcx, step)
        }
        NodeTag::T_RelabelType => {
            init_expr_rec(node.as_relabel_type().unwrap().arg, state, mcx, out, agg, params, sub)
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().unwrap();
            init_expr_rec(f.arg, state, mcx, out, agg, params, sub)?;
            let frame_ix = state.frames.len() as u32;
            let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
            state
                .frames
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
            state.frames.push(frame);
            push_step(
                state,
                mcx,
                Step::FieldSelect {
                    fieldnum: f.fieldnum,
                    resulttype: f.resulttype,
                    frame: frame_ix,
                    out,
                },
            )
        }
        NodeTag::T_FieldStore => init_field_store(node, state, mcx, out, agg, params, sub),
        NodeTag::T_NextValueExpr => {
            let nve = node
                .as_variant::<::types_nodes::primnodes::NextValueExpr>()
                .unwrap();
            push_step(
                state,
                mcx,
                Step::NextValueExpr { seqid: nve.seqid, seqtypid: nve.typeId, out },
            )
        }
        NodeTag::T_ReturningExpr => {
            let rexpr = node.as_returning_expr().unwrap();
            let nullflag = if rexpr.retold {
                crate::steps::EEO_FLAG_OLD_IS_NULL
            } else {
                crate::steps::EEO_FLAG_NEW_IS_NULL
            };
            push_step(state, mcx, Step::ReturningExprStep { nullflag, jumpdone: u32::MAX, out })?;
            let retstep = state.steps.len() - 1;
            init_expr_rec(rexpr.retexpr, state, mcx, out, agg, params, sub)?;
            let done = state.steps.len() as u32;
            if let Step::ReturningExprStep { jumpdone, .. } = &mut state.steps[retstep] {
                *jumpdone = done;
            }
            state.flags |= if rexpr.retold {
                crate::steps::EEO_FLAG_HAS_OLD
            } else {
                crate::steps::EEO_FLAG_HAS_NEW
            };
            Ok(())
        }
        NodeTag::T_CoerceViaIO => init_coerce_via_io(node, state, mcx, out, agg, params, sub),
        NodeTag::T_ArrayCoerceExpr => init_array_coerce(node, state, mcx, out, agg, params, sub),
        NodeTag::T_ConvertRowtypeExpr => {
            init_convert_rowtype(node, state, mcx, out, agg, params, sub)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let saop = node.as_scalar_array_op_expr().unwrap();
            let step = init_scalar_array_op(node, saop, state, mcx, out, agg, params, sub)?;
            push_step(state, mcx, step)
        }
        NodeTag::T_ArrayExpr => {
            let arr = node.as_array_expr().unwrap();
            if arr.multidims {
                init_array_expr_multidim(node, state, mcx, out, agg, params, sub)
            } else {
                let step = init_array_expr(arr, state, mcx, out, agg, params, sub)?;
                push_step(state, mcx, step)
            }
        }
        NodeTag::T_SubscriptingRef => init_subscripting_ref(node, state, mcx, out, agg, params, sub),
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().unwrap();
            let step = init_row_expr(r, state, mcx, out, agg, params, sub)?;
            push_step(state, mcx, step)
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            init_expr_rec(j.raw_expr.expect("raw_expr"), state, mcx, out, agg, params, sub)?;
            init_expr_rec(
                j.formatted_expr.expect("formatted_expr"),
                state,
                mcx,
                out,
                agg,
                params,
                sub,
            )
        }
        NodeTag::T_JsonConstructorExpr => {
            init_json_constructor(node, state, mcx, out, agg, params, sub)
        }
        NodeTag::T_JsonIsPredicate => {
            let p = node.as_json_is_predicate().unwrap();
            let arg = p.expr.expect("expr");
            init_expr_rec(arg, state, mcx, out, agg, params, sub)?;
            let frame_ix = state.frames.len() as u32;
            let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
            state
                .frames
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
            state.frames.push(frame);
            push_step(
                state,
                mcx,
                Step::IsJson {
                    exprtype: expr_type(arg),
                    item_type: p.item_type,
                    unique_keys: p.unique_keys,
                    frame: frame_ix,
                    out,
                },
            )
        }
        NodeTag::T_JsonExpr => {
            let je = node.as_json_expr().unwrap();
            // JSON_TABLE docexpr: tfuncFetchRows only wants formatted_expr.
            if je.op == ::types_nodes::primnodes::JsonExprOp::JSON_TABLE_OP {
                init_expr_rec(
                    je.formatted_expr.expect("JsonExpr.formatted_expr"),
                    state,
                    mcx,
                    out,
                    agg,
                    params,
                    sub,
                )
            } else {
                init_json_expr(node, state, mcx, out, agg, params, sub)
            }
        }
        NodeTag::T_CoerceToDomain => init_coerce_to_domain(node, state, mcx, out, agg, params, sub),
        NodeTag::T_CoerceToDomainValue => match state.innermost_domain {
            Some(src) => push_step(state, mcx, Step::DomainTestval { src, out }),
            // unported: EEOP_DOMAIN_TESTVAL_EXT (CoerceToDomainValue outside
            // a domain-check compile).
            None => Err(feature_unported(
                "VALUE reference outside a domain check constraint",
            )),
        },
        // Each arg evaluates into the result slot; a non-null short-circuits.
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().unwrap();
            debug_assert!(!co.args.is_nil());
            let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
            for e in co.args.iter() {
                init_expr_rec(e, state, mcx, out, agg, params, sub)?;
                adjust_jumps.push(state.steps.len());
                push_step(state, mcx, Step::JumpIfNotNull { jumpdone: u32::MAX, out })?;
            }
            let done = state.steps.len() as u32;
            for ix in adjust_jumps.iter() {
                match &mut state.steps[*ix] {
                    Step::JumpIfNotNull { jumpdone, .. } => {
                        debug_assert_eq!(*jumpdone, u32::MAX);
                        *jumpdone = done;
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        }
        tag => panic!("execexpr ExecInitExprRec: node family {tag:?} not ported"),
    }
}

// C ExecInitExprRec T_ScalarArrayOpExpr, non-hashed leg; the scalar operand
// evaluates into args[0], the array operand into the step's own output.
#[allow(clippy::too_many_arguments)]
fn init_scalar_array_op<'mcx>(
    node: Node<'mcx>,
    saop: &::types_nodes::primnodes::ScalarArrayOpExpr<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Step> {
    debug_assert!(saop.args.len() == 2);
    let scalararg = saop.args.nth(0);
    let arrayarg = saop.args.nth(1);
    // C: hash probes use the equality function (negfuncid) for NOT IN.
    let opfuncid = if saop.hashfuncid != 0 && saop.negfuncid != 0 {
        saop.negfuncid
    } else if saop.opfuncid != 0 {
        saop.opfuncid
    } else {
        // set_sa_opfuncid (nodeFuncs.c).
        lsyscache::get_opcode(saop.opno)?
    };

    let element_type = lsyscache::get_element_type(expr_type(arrayarg))?;
    assert!(element_type != 0, "init_scalar_array_op: operand is not an array");
    let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(element_type)?;

    let mut flinfo = fmgr_core::fmgr_info(opfuncid)?;
    flinfo.fn_expr = Some(erase_fn_expr(mcx, node)?);
    let strict = flinfo.fn_strict;
    let mut frame = FuncFrame::new_in(mcx, flinfo, 2, saop.inputcollid)?;

    let frame_ix = state.frames.len() as u32;
    if let Some(con) = scalararg.as_const() {
        // SAFETY: slot 0 of the frame's freshly allocated fcinfo image.
        unsafe {
            frame.arg_slot(0).write(::datum::NullableDatum {
                value: con.constvalue,
                isnull: con.constisnull,
            })
        };
    }
    let call = FuncCall { fcinfo: frame.fcinfo, flinfo: frame.flinfo, frame: frame_ix, nargs: 2 };
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    if scalararg.as_const().is_none() {
        // SAFETY: arg 0 of the image `call.fcinfo` points at.
        let arg_out = OutRef(unsafe { crate::steps::arg_slot_of(call.fcinfo, 0) });
        init_expr_rec(scalararg, state, mcx, arg_out, agg, params, sub)?;
    }
    init_expr_rec(arrayarg, state, mcx, out, agg, params, sub)?;

    if saop.hashfuncid != 0 {
        let mut hash_flinfo = fmgr_core::fmgr_info(saop.hashfuncid)?;
        hash_flinfo.fn_expr = Some(erase_fn_expr(mcx, node)?);
        let hash_frame = FuncFrame::new_in(mcx, hash_flinfo, 1, saop.inputcollid)?;
        let hash_frame_ix = state.frames.len() as u32;
        let hashcall = FuncCall {
            fcinfo: hash_frame.fcinfo,
            flinfo: hash_frame.flinfo,
            frame: hash_frame_ix,
            nargs: 1,
        };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(hash_frame);

        let table = state.saop_tables.len() as u32;
        state
            .saop_tables
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<crate::steps::SaopTable<'_>>()))?;
        state.saop_tables.push(crate::steps::SaopTable {
            hashcall,
            built: false,
            has_nulls: false,
            null_lhs_result: false,
            null_lhs_isnull: false,
            map: ::mcx::PgFxHashMap::with_hasher_in(Default::default(), mcx),
        });

        return Ok(Step::HashedScalarArrayOp {
            call,
            inclause: saop.useOr,
            typlen,
            typbyval,
            typalign: typalign as u8,
            table,
            out,
        });
    }

    Ok(Step::ScalarArrayOp {
        call,
        use_or: saop.useOr,
        strict,
        typlen,
        typbyval,
        typalign: typalign as u8,
        out,
    })
}

// C ExecInitExprRec T_ArrayExpr, 1-D non-multidims leg.
#[allow(clippy::too_many_arguments)]
fn init_array_expr<'mcx>(
    arr: &::types_nodes::primnodes::ArrayExpr<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Step> {
    if arr.multidims {
        // unported: EEOP_ARRAYEXPR multidimensional leg.
        return Err(feature_unported("multidimensional ARRAY[] expressions"));
    }
    let nelems = arr.elements.len();
    let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(arr.element_typeid)?;

    let layout = core::alloc::Layout::array::<::datum::NullableDatum>(nelems.max(1))
        .expect("elem scratch layout");
    let elems: NonNull<::datum::NullableDatum> =
        mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();

    // An argless frame whose armed fcinfo supplies the per-eval result mcx.
    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    for (i, e) in arr.elements.iter().enumerate() {
        // SAFETY: i < nelems slots of the fresh scratch allocation.
        let slot = unsafe { NonNull::new_unchecked(elems.as_ptr().add(i)) };
        init_expr_rec(e, state, mcx, OutRef(slot), agg, params, sub)?;
    }

    Ok(Step::ArrayExprStep {
        elems,
        nelems: nelems as u32,
        frame: frame_ix,
        elmtype: arr.element_typeid,
        elmlen,
        elmbyval,
        elmalign: elmalign as u8,
        out,
    })
}


// C ExecInitExprRec T_ArrayExpr, multidims leg (ExecEvalArrayExpr concat arm).
fn init_array_expr_multidim<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let a = node.as_array_expr().unwrap();
    let nelems = a.elements.len();
    let (elemlength, elembyval, elemalign) = lsyscache::get_typlenbyvalalign(a.element_typeid)
        .map(|(l, b, al)| (l as i32, b, al as u8))?;

    let elemvalues: NonNull<::datum::NullableDatum> = alloc_array(mcx, nelems)?;
    let scratch_values: NonNull<::datum::Datum> = alloc_array(mcx, nelems)?;
    let scratch_nulls: NonNull<bool> = alloc_array(mcx, nelems)?;

    for (i, e) in a.elements.iter().enumerate() {
        // SAFETY: i < nelems freshly allocated slots.
        let arg_out = OutRef(unsafe { NonNull::new_unchecked(elemvalues.as_ptr().add(i)) });
        init_expr_rec(e, state, mcx, arg_out, agg, params, sub)?;
    }

    let st = crate::arrayops::ArrayExprState {
        elemtype: a.element_typeid,
        elemlength,
        elembyval,
        elemalign,
        multidims: a.multidims,
        nelems: nelems as u32,
        elemvalues,
        scratch_values,
        scratch_nulls,
        resmcx: None,
    };
    let stp = alloc_state(mcx, st)?;
    register_alloc_state(state, mcx, stp)?;
    push_step(state, mcx, Step::ArrayExprEval { state: stp, out })
}

fn alloc_array<'mcx, T>(mcx: Mcx<'mcx>, n: usize) -> PgResult<NonNull<T>> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let layout = core::alloc::Layout::array::<T>(n.max(1)).expect("array layout");
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    // SAFETY: fresh allocation; zero-init keeps padding deterministic.
    unsafe { core::ptr::write_bytes(raw.as_ptr().cast::<u8>(), 0, layout.size()) };
    Ok(raw.cast())
}

// sqljson categorize carriers (ValCategory/TypeCat) embed droppy FmgrInfo
// payloads; they live for the ExprState's life and are never dropped, matching
// C's fn_extra arg_type_cache. No zero-init needed: callers write before read.
fn alloc_array_nodrop_exempt<'mcx, T>(mcx: Mcx<'mcx>, n: usize) -> PgResult<NonNull<T>> {
    let layout = core::alloc::Layout::array::<T>(n.max(1)).expect("scratch layout");
    Ok(mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast())
}

fn alloc_state<'mcx, T>(mcx: Mcx<'mcx>, v: T) -> PgResult<NonNull<T>> {
    const { assert!(!core::mem::needs_drop::<T>()) };
    let layout = core::alloc::Layout::new::<T>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<T> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(v) };
    Ok(p)
}

// The state's resmcx field is the first-arm target of arm_result_mcx.
fn register_alloc_state<'mcx, T>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    stp: NonNull<T>,
) -> PgResult<()>
where
    T: HasResMcx,
{
    // SAFETY: stp is a live compile-allocated state; the field pointer stays
    // valid for 'mcx.
    let slot = unsafe { NonNull::new_unchecked(T::resmcx_ptr(stp.as_ptr())) };
    let _ = mcx;
    state.alloc_mcx_slots.push(slot);
    Ok(())
}

trait HasResMcx {
    /// # Safety
    /// `p` points at a live value.
    unsafe fn resmcx_ptr(p: *mut Self) -> *mut crate::arrayops::ResMcx;
}
impl HasResMcx for crate::arrayops::ArrayExprState {
    unsafe fn resmcx_ptr(p: *mut Self) -> *mut crate::arrayops::ResMcx {
        unsafe { core::ptr::addr_of_mut!((*p).resmcx) }
    }
}
impl HasResMcx for crate::arrayops::SbsRefState {
    unsafe fn resmcx_ptr(p: *mut Self) -> *mut crate::arrayops::ResMcx {
        unsafe { core::ptr::addr_of_mut!((*p).resmcx) }
    }
}
impl HasResMcx for crate::xmlops::XmlExprState {
    unsafe fn resmcx_ptr(p: *mut Self) -> *mut crate::arrayops::ResMcx {
        unsafe { core::ptr::addr_of_mut!((*p).resmcx) }
    }
}
impl HasResMcx for crate::arrayops::ArrayCoerceState {
    unsafe fn resmcx_ptr(p: *mut Self) -> *mut crate::arrayops::ResMcx {
        unsafe { core::ptr::addr_of_mut!((*p).resmcx) }
    }
}
impl HasResMcx for crate::jsonbsubs::JsonbSbsState {
    unsafe fn resmcx_ptr(p: *mut Self) -> *mut crate::arrayops::ResMcx {
        unsafe { core::ptr::addr_of_mut!((*p).resmcx) }
    }
}
impl HasResMcx for crate::hstoresubs::HstoreSbsState {
    unsafe fn resmcx_ptr(p: *mut Self) -> *mut crate::arrayops::ResMcx {
        unsafe { core::ptr::addr_of_mut!((*p).resmcx) }
    }
}

// C ExecInitExprRec T_XmlExpr: per-list arg slot arrays, args evaluated in
// place before the XmlExprEval step runs.
fn init_xml_expr<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let x = node.as_xml_expr().unwrap();
    let n_named = x.named_args.len();
    let n_args = x.args.len();
    let named_slots: NonNull<::datum::NullableDatum> = alloc_array(mcx, n_named)?;
    let arg_slots: NonNull<::datum::NullableDatum> = alloc_array(mcx, n_args)?;
    for (i, arg) in x.named_args.iter().enumerate() {
        // SAFETY: i < n_named of the fresh slot array.
        let arg_out = OutRef(unsafe { NonNull::new_unchecked(named_slots.as_ptr().add(i)) });
        init_expr_rec(arg, state, mcx, arg_out, agg, params, sub)?;
    }
    for (i, arg) in x.args.iter().enumerate() {
        // SAFETY: i < n_args of the fresh slot array.
        let arg_out = OutRef(unsafe { NonNull::new_unchecked(arg_slots.as_ptr().add(i)) });
        init_expr_rec(arg, state, mcx, arg_out, agg, params, sub)?;
    }
    let st = crate::xmlops::XmlExprState {
        xexpr: NonNull::from(x).cast(),
        named_slots,
        arg_slots,
        n_named: n_named as u16,
        n_args: n_args as u16,
        resmcx: None,
    };
    let stp = alloc_state(mcx, st)?;
    register_alloc_state(state, mcx, stp)?;
    push_step(state, mcx, Step::XmlExprEval { state: stp, out })
}

// C ExecInitSubscriptingRef over the closed handler set (array_exec_setup /
// jsonb_exec_setup inlined; fetch_strict = true for both).
fn init_subscripting_ref<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    use crate::arrayops::MAXDIM;
    let sbsref = node.as_subscripting_ref().unwrap();
    const F_JSONB_SUBSCRIPT_HANDLER: Oid = 6098;
    const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;
    const F_RAW_ARRAY_SUBSCRIPT_HANDLER: Oid = 6180;
    let (typsubscript, _) = lsyscache::typ::get_typsubscript(sbsref.refcontainertype)?;
    if typsubscript as Oid == F_JSONB_SUBSCRIPT_HANDLER {
        return init_jsonb_subscripting_ref(node, state, mcx, out, agg, params, sub);
    }
    if !matches!(
        typsubscript as Oid,
        F_ARRAY_SUBSCRIPT_HANDLER | F_RAW_ARRAY_SUBSCRIPT_HANDLER
    ) {
        // Extension handlers carry dynamic oids; match by proname.
        let cx = ::mcx::MemoryContext::new("sbsref handler probe");
        let name = lsyscache::get_func_name(cx.mcx(), typsubscript as Oid)?
            .map(|n| n.as_str().to_string());
        drop(cx);
        match name.as_deref() {
            Some("hstore_subscript_handler") => {
                return init_hstore_subscripting_ref(node, state, mcx, out, agg, params, sub)
            }
            // unported: non-core SubscriptingRef handler (ExecInitSubscriptingRef).
            _ => {
                return Err(feature_unported(&format!(
                    "subscripting via handler function {typsubscript}"
                )))
            }
        }
    }
    let is_assignment = sbsref.refassgnexpr.is_some();
    let nupper = sbsref.refupperindexpr.len();
    let nlower = sbsref.reflowerindexpr.len();
    assert!(nupper <= MAXDIM && nlower <= MAXDIM, "too many subscripts");
    assert!(
        nlower == 0 || nupper == nlower,
        "upper and lower index lists are not same length"
    );
    let is_slice = nlower != 0;

    let refattrlength = lsyscache::get_typlen(sbsref.refcontainertype)? as i32;
    let (refelemlength, refelembyval, refelemalign) =
        lsyscache::get_typlenbyvalalign(sbsref.refelemtype)
            .map(|(l, b, al)| (l as i32, b, al as u8))?;

    let st = crate::arrayops::SbsRefState {
        isassignment: is_assignment,
        numupper: nupper as u8,
        numlower: nlower as u8,
        upperprovided: [false; MAXDIM],
        lowerprovided: [false; MAXDIM],
        upperindex: [::datum::NullableDatum::null(); MAXDIM],
        lowerindex: [::datum::NullableDatum::null(); MAXDIM],
        replace: ::datum::NullableDatum::null(),
        prev: ::datum::NullableDatum::null(),
        refelemtype: sbsref.refelemtype,
        refattrlength,
        refelemlength,
        refelembyval,
        refelemalign,
        upperidx: [0; MAXDIM],
        loweridx: [0; MAXDIM],
        resmcx: None,
    };
    let stp = alloc_state(mcx, st)?;
    register_alloc_state(state, mcx, stp)?;

    // Container value evaluates into `out` (overwritten by the final step).
    init_expr_rec(sbsref.refexpr.expect("SubscriptingRef.refexpr"), state, mcx, out, agg, params, sub)?;

    let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
    if !is_assignment {
        // fetch_strict: NULL container => NULL result.
        adjust_jumps.push(state.steps.len());
        push_step(state, mcx, Step::JumpIfNull { jumpdone: u32::MAX, out })?;
    }

    for (i, e) in sbsref.refupperindexpr.iter().enumerate() {
        // SAFETY: compile-owned state; i < MAXDIM.
        let stref = unsafe { &mut *stp.as_ptr() };
        match e {
            None => {
                stref.upperprovided[i] = false;
                stref.upperindex[i].isnull = true;
            }
            Some(e) => {
                stref.upperprovided[i] = true;
                let slot = unsafe {
                    NonNull::new_unchecked(
                        core::ptr::addr_of_mut!((*stp.as_ptr()).upperindex[i]),
                    )
                };
                init_expr_rec(e, state, mcx, OutRef(slot), agg, params, sub)?;
            }
        }
    }
    for (i, e) in sbsref.reflowerindexpr.iter().enumerate() {
        let stref = unsafe { &mut *stp.as_ptr() };
        match e {
            None => {
                stref.lowerprovided[i] = false;
                stref.lowerindex[i].isnull = true;
            }
            Some(e) => {
                stref.lowerprovided[i] = true;
                let slot = unsafe {
                    NonNull::new_unchecked(
                        core::ptr::addr_of_mut!((*stp.as_ptr()).lowerindex[i]),
                    )
                };
                init_expr_rec(e, state, mcx, OutRef(slot), agg, params, sub)?;
            }
        }
    }

    adjust_jumps.push(state.steps.len());
    push_step(state, mcx, Step::SbsrefSubscripts { state: stp, jumpdone: u32::MAX, out })?;

    if is_assignment {
        let assgn = sbsref.refassgnexpr.unwrap();
        if assgn_needs_old(assgn) {
            push_step(state, mcx, Step::SbsrefOld { state: stp, out })?;
        }
        let replace_slot = unsafe {
            NonNull::new_unchecked(core::ptr::addr_of_mut!((*stp.as_ptr()).replace))
        };
        // SBSREF_OLD puts the extracted value into `prev`; pass it down via
        // the CaseTestExpr mechanism (C innermost_caseval).
        let prev_slot = unsafe {
            NonNull::new_unchecked(core::ptr::addr_of_mut!((*stp.as_ptr()).prev))
        };
        let save_innermost = state.innermost_case;
        state.innermost_case = Some(prev_slot);
        init_expr_rec(assgn, state, mcx, OutRef(replace_slot), agg, params, sub)?;
        state.innermost_case = save_innermost;
        push_step(state, mcx, Step::SbsrefAssign { state: stp, slice: is_slice, out })?;
    } else {
        push_step(state, mcx, Step::SbsrefFetch { state: stp, slice: is_slice, out })?;
    }

    let done = state.steps.len() as u32;
    for ix in adjust_jumps.iter() {
        match &mut state.steps[*ix] {
            Step::JumpIfNull { jumpdone, .. } | Step::SbsrefSubscripts { jumpdone, .. } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = done;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

// C ExecInitSubscriptingRef + jsonb_exec_setup: unbounded subscript count,
// per-subscript expr type recorded for the INT4→text exec conversion.
fn init_jsonb_subscripting_ref<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let sbsref = node.as_subscripting_ref().unwrap();
    let is_assignment = sbsref.refassgnexpr.is_some();
    let nupper = sbsref.refupperindexpr.len();
    assert!(sbsref.reflowerindexpr.len() == 0, "jsonb subscript does not support slices");

    let upperindex: NonNull<::datum::NullableDatum> = alloc_array(mcx, nupper)?;
    let index_oids: NonNull<Oid> = alloc_array(mcx, nupper)?;
    let index: NonNull<::datum::Datum> = alloc_array(mcx, nupper)?;

    let st = crate::jsonbsubs::JsonbSbsState {
        isassignment: is_assignment,
        expect_array: false,
        nupper: nupper as u32,
        upperindex,
        index_oids,
        index,
        replace: ::datum::NullableDatum::null(),
        resmcx: None,
    };
    let stp = alloc_state(mcx, st)?;
    register_alloc_state(state, mcx, stp)?;

    init_expr_rec(sbsref.refexpr.expect("SubscriptingRef.refexpr"), state, mcx, out, agg, params, sub)?;

    let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
    if !is_assignment {
        // fetch_strict: NULL container => NULL result.
        adjust_jumps.push(state.steps.len());
        push_step(state, mcx, Step::JumpIfNull { jumpdone: u32::MAX, out })?;
    }

    for (i, e) in sbsref.refupperindexpr.iter().enumerate() {
        match e {
            Some(e) => {
                // SAFETY: i < nupper slots of the fresh allocations.
                unsafe { index_oids.as_ptr().add(i).write(expr_type(e)) };
                let slot = unsafe { NonNull::new_unchecked(upperindex.as_ptr().add(i)) };
                init_expr_rec(e, state, mcx, OutRef(slot), agg, params, sub)?;
            }
            None => unreachable!("jsonb subscripts are never omitted"),
        }
    }

    adjust_jumps.push(state.steps.len());
    push_step(state, mcx, Step::JsonbSbsrefSubscripts { state: stp, jumpdone: u32::MAX, out })?;

    if is_assignment {
        let assgn = sbsref.refassgnexpr.unwrap();
        if assgn_needs_old(assgn) {
            // unported: jsonb_subscript_fetch_old (EEOP_SBSREF_OLD, jsonbsubs.c).
            return Err(feature_unported(
                "jsonb subscripted assignment referencing the old element",
            ));
        }
        let replace_slot = unsafe {
            NonNull::new_unchecked(core::ptr::addr_of_mut!((*stp.as_ptr()).replace))
        };
        init_expr_rec(assgn, state, mcx, OutRef(replace_slot), agg, params, sub)?;
        push_step(state, mcx, Step::JsonbSbsrefAssign { state: stp, out })?;
    } else {
        push_step(state, mcx, Step::JsonbSbsrefFetch { state: stp, out })?;
    }

    let done = state.steps.len() as u32;
    for ix in adjust_jumps.iter() {
        match &mut state.steps[*ix] {
            Step::JumpIfNull { jumpdone, .. } | Step::JsonbSbsrefSubscripts { jumpdone, .. } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = done;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

// C ExecInitSubscriptingRef + hstore_exec_setup (hstore_subs.c): exactly one
// text subscript, no slices; fetch_strict = true, no fetch_old.
fn init_hstore_subscripting_ref<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let sbsref = node.as_subscripting_ref().unwrap();
    let is_assignment = sbsref.refassgnexpr.is_some();
    assert!(sbsref.refupperindexpr.len() == 1, "hstore allows only one subscript");
    assert!(sbsref.reflowerindexpr.len() == 0, "hstore subscript does not support slices");

    let st = crate::hstoresubs::HstoreSbsState {
        isassignment: is_assignment,
        subscript: ::datum::NullableDatum::null(),
        replace: ::datum::NullableDatum::null(),
        resmcx: None,
    };
    let stp = alloc_state(mcx, st)?;
    register_alloc_state(state, mcx, stp)?;

    init_expr_rec(sbsref.refexpr.expect("SubscriptingRef.refexpr"), state, mcx, out, agg, params, sub)?;

    let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
    if !is_assignment {
        // fetch_strict: NULL container => NULL result.
        adjust_jumps.push(state.steps.len());
        push_step(state, mcx, Step::JumpIfNull { jumpdone: u32::MAX, out })?;
    }

    let e = sbsref.refupperindexpr.iter().next().unwrap().expect("hstore subscript present");
    let sub_slot = unsafe {
        NonNull::new_unchecked(core::ptr::addr_of_mut!((*stp.as_ptr()).subscript))
    };
    init_expr_rec(e, state, mcx, OutRef(sub_slot), agg, params, sub)?;

    if is_assignment {
        let assgn = sbsref.refassgnexpr.unwrap();
        if assgn_needs_old(assgn) {
            // unported: no sbs_fetch_old (EEOP_SBSREF_OLD, hstore_subs.c).
            return Err(feature_unported(
                "hstore subscripted assignment referencing the old element",
            ));
        }
        let replace_slot = unsafe {
            NonNull::new_unchecked(core::ptr::addr_of_mut!((*stp.as_ptr()).replace))
        };
        init_expr_rec(assgn, state, mcx, OutRef(replace_slot), agg, params, sub)?;
        push_step(state, mcx, Step::HstoreSbsrefAssign { state: stp, out })?;
    } else {
        push_step(state, mcx, Step::HstoreSbsrefFetch { state: stp, out })?;
    }

    let done = state.steps.len() as u32;
    for ix in adjust_jumps.iter() {
        match &mut state.steps[*ix] {
            Step::JumpIfNull { jumpdone, .. } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = done;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

// isAssignmentIndirectionExpr: does the replacement value reference the old
// element (CaseTestExpr under FieldStore/SubscriptingRef)?
fn assgn_needs_old(expr: Node<'_>) -> bool {
    match expr.node_tag() {
        NodeTag::T_FieldStore => {
            expr.as_field_store().unwrap().arg.node_tag() == NodeTag::T_CaseTestExpr
        }
        NodeTag::T_SubscriptingRef => {
            let sr = expr.as_subscripting_ref().unwrap();
            sr.refexpr.is_some_and(|e| e.node_tag() == NodeTag::T_CaseTestExpr)
        }
        NodeTag::T_CoerceToDomain => assgn_needs_old(expr.as_coerce_to_domain().unwrap().arg),
        NodeTag::T_RelabelType => assgn_needs_old(expr.as_relabel_type().unwrap().arg),
        _ => false,
    }
}

// C ExecInitExprRec T_FieldStore (EEOP_FIELDSTORE_DEFORM + per-field
// subexpressions + EEOP_FIELDSTORE_FORM); the blessed tupdesc is
// compile-resolved (C: rowcache on first eval).
fn init_field_store<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let fstore = node.as_field_store().unwrap();
    let desc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, fstore.resulttype, -1)?;
    let ncolumns = desc.natts;
    let desc_layout = core::alloc::Layout::new::<TupleDescData<'static>>();
    let desc_ptr: NonNull<TupleDescData<'static>> = mcx
        .allocate(desc_layout)
        .map_err(|_| mcx.oom(desc_layout.size()))?
        .cast();
    // SAFETY: fresh allocation of the exact layout; the plan mcx outlives
    // every eval of this step, so the 'static restamp never escapes it.
    unsafe {
        desc_ptr
            .as_ptr()
            .write(core::mem::transmute::<TupleDescData<'mcx>, TupleDescData<'static>>(desc));
    }
    let columns: NonNull<::datum::NullableDatum> = alloc_array(mcx, ncolumns as usize)?;

    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    let fs = crate::steps::FieldStoreState { ncolumns: ncolumns as u16, desc: desc_ptr, columns };
    let fsp = alloc_state(mcx, fs)?;

    init_expr_rec(fstore.arg, state, mcx, out, agg, params, sub)?;
    push_step(state, mcx, Step::FieldStoreDeForm { fs: fsp, frame: frame_ix, out })?;

    for (e, fieldnum) in fstore.newvals.iter().zip(fstore.fieldnums.iter()) {
        if fieldnum <= 0 || fieldnum > ncolumns {
            return Err(PgError::error(format!(
                "field number {fieldnum} is out of range in FieldStore"
            ))
            .into());
        }
        // The old field value passes down via the CaseTestExpr mechanism
        // (C innermost_caseval); the column slot doubles as caseval source
        // and result address, safe because DEFORM/FORM evaluate arg first.
        // SAFETY: 1 <= fieldnum <= ncolumns slots of the fresh allocation.
        let slot = unsafe {
            NonNull::new_unchecked(columns.as_ptr().add((fieldnum - 1) as usize))
        };
        let save_innermost = state.innermost_case;
        state.innermost_case = Some(slot);
        init_expr_rec(e, state, mcx, OutRef(slot), agg, params, sub)?;
        state.innermost_case = save_innermost;
    }

    push_step(state, mcx, Step::FieldStoreForm { fs: fsp, frame: frame_ix, out })
}

// exprTypmod (nodeFuncs.c) over the families RowExpr args carry.
fn expr_typmod_closed(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().vartypmod,
        NodeTag::T_Const => node.as_const().unwrap().consttypmod,
        NodeTag::T_Param => node.as_param().unwrap().paramtypmod,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttypmod,
        NodeTag::T_CoerceViaIO => -1,
        _ => -1,
    }
}

// C T_RowCompareExpr: per-column BTORDER procs resolve here.
#[allow(clippy::too_many_arguments)]
fn init_row_compare<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    const BTORDER_PROC: i16 = 1;
    let rc = node.as_row_compare_expr().unwrap();
    let nopers = rc.opnos.len();
    assert_eq!(rc.largs.len(), nopers);
    assert_eq!(rc.rargs.len(), nopers);
    assert_eq!(rc.opfamilies.len(), nopers);
    assert_eq!(rc.inputcollids.len(), nopers);

    let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
    for i in 0..nopers {
        let opno = rc.opnos.nth(i);
        let opfamily = rc.opfamilies.nth(i);
        let inputcollid = rc.inputcollids.nth(i);
        let (_strategy, lefttype, righttype) =
            ::lsyscache::get_op_opfamily_properties(opno, opfamily, false)?;
        let proc = ::lsyscache::get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC)?;
        if !::types_core::OidIsValid(proc) {
            panic!(
                "missing support function {BTORDER_PROC}({lefttype},{righttype}) \
                 in opfamily {opfamily}"
            );
        }
        let mut flinfo = fmgr_core::fmgr_info(proc)?;
        flinfo.fn_expr = Some(erase_fn_expr(mcx, node)?);
        let strict = flinfo.fn_strict;
        let frame = FuncFrame::new_in(mcx, flinfo, 2, inputcollid)?;
        let call = crate::steps::Call2 { fcinfo: frame.fcinfo, flinfo: frame.flinfo };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);
        // SAFETY: args 0/1 of the frame's fresh 2-arg fcinfo image.
        let (l_out, r_out) = unsafe {
            (
                OutRef(crate::steps::arg_slot_of(call.fcinfo, 0)),
                OutRef(crate::steps::arg_slot_of(call.fcinfo, 1)),
            )
        };
        init_expr_rec(rc.largs.nth(i), state, mcx, l_out, agg, params, sub)?;
        init_expr_rec(rc.rargs.nth(i), state, mcx, r_out, agg, params, sub)?;
        adjust_jumps.push(state.steps.len());
        push_step(
            state,
            mcx,
            Step::RowCompareStep {
                call,
                strict,
                jumpnull: u32::MAX,
                jumpdone: u32::MAX,
                out,
            },
        )?;
    }
    if nopers == 0 {
        push_step(state, mcx, Step::Const { value: ::datum::Datum::from_i32(0), isnull: false, out })?;
    }
    let final_ix = state.steps.len() as u32;
    push_step(state, mcx, Step::RowCompareFinal { cmptype: rc.cmptype, out })?;
    let done = state.steps.len() as u32;
    for ix in adjust_jumps.iter() {
        match &mut state.steps[*ix] {
            Step::RowCompareStep { jumpnull, jumpdone, .. } => {
                *jumpdone = final_ix;
                *jumpnull = done;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

// C ExecInitExprRec T_RowExpr (execExpr.c:1969-2056); the tupdesc (blessed
// RECORD or named-rowtype copy) is built once at compile.
#[allow(clippy::too_many_arguments)]
fn init_row_expr<'mcx>(
    r: &::types_nodes::primnodes::RowExpr<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Step> {
    let mut nelems = r.args.len();
    let desc = if r.row_typeid == ::types_core::catalog::RECORDOID {
        let mut desc = ::tupdesc::CreateTemplateTupleDesc(mcx, nelems as i32)?;
        for (i, e) in r.args.iter().enumerate() {
            let colname = r
                .colnames
                .nth(i)
                .as_string()
                .expect("RowExpr colnames are String nodes")
                .sval;
            ::tupdesc::TupleDescInitEntry(
                &mut desc,
                (i + 1) as i16,
                Some(colname),
                expr_type(e),
                expr_typmod_closed(e),
                0,
            )?;
        }
        desc.tdtypeid = ::types_core::catalog::RECORDOID;
        desc.tdtypmod = -1;
        ::typcache::assign_record_type_typmod(&mut desc)?;
        desc
    } else {
        // Named type: the tupdesc can have MORE columns than the args list
        // (columns added since the ROW() was parsed); extras read as NULLs
        // (execExpr.c:1990-1998).
        let desc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, r.row_typeid, -1)?;
        assert!(nelems <= desc.natts as usize, "RowExpr args exceed named rowtype");
        nelems = nelems.max(desc.natts as usize);
        desc
    };

    let layout = core::alloc::Layout::array::<::datum::NullableDatum>(nelems.max(1))
        .expect("elem scratch layout");
    let elems: NonNull<::datum::NullableDatum> =
        mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
    // Dropped-column and extra-column slots are never written by a step;
    // preset every slot to NULL (C memsets elemnulls true, execExpr.c:2013).
    for i in 0..nelems.max(1) {
        // SAFETY: i < nelems.max(1) slots of the fresh scratch allocation.
        unsafe {
            elems
                .as_ptr()
                .add(i)
                .write(::datum::NullableDatum { value: ::datum::Datum::null(), isnull: true });
        }
    }

    // An argless frame whose armed fcinfo supplies the per-eval result mcx.
    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    for (i, e) in r.args.iter().enumerate() {
        let att = &desc.attrs[i];
        if att.attisdropped {
            // C substitutes an int4 NULL Const (execExpr.c:2038-2045); the
            // preset-NULL slot is never written, same result.
            continue;
        }
        // Guard against ALTER COLUMN TYPE on the rowtype since the RowExpr
        // was created (execExpr.c:2024-2035).
        if expr_type(e) != att.atttypid {
            let have = ::format_type::format_type_be(expr_type(e))
                .unwrap_or_else(|_| expr_type(e).to_string());
            let want = ::format_type::format_type_be(att.atttypid)
                .unwrap_or_else(|_| att.atttypid.to_string());
            return Err(Box::new(
                PgError::error(format!(
                    "ROW() column has type {have} instead of type {want}"
                ))
                .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
            ));
        }
        // SAFETY: i < nelems slots of the fresh scratch allocation.
        let slot = unsafe { NonNull::new_unchecked(elems.as_ptr().add(i)) };
        init_expr_rec(e, state, mcx, OutRef(slot), agg, params, sub)?;
    }

    let desc_layout = core::alloc::Layout::new::<TupleDescData<'static>>();
    let desc_ptr: NonNull<TupleDescData<'static>> = mcx
        .allocate(desc_layout)
        .map_err(|_| mcx.oom(desc_layout.size()))?
        .cast();
    // SAFETY: fresh allocation of the exact layout; the plan mcx outlives
    // every eval of this step, so the 'static restamp never escapes it.
    unsafe {
        desc_ptr
            .as_ptr()
            .write(core::mem::transmute::<TupleDescData<'mcx>, TupleDescData<'static>>(desc));
    }

    Ok(Step::RowExprStep { elems, nelems: nelems as u32, frame: frame_ix, desc: desc_ptr, out })
}

// C ExecInitExprRec T_JsonConstructorExpr (execExpr.c:2379): args evaluate
// into compile-allocated slots (Consts pre-written); scalar categorize
// carriers resolved once.
#[allow(clippy::too_many_arguments)]
fn init_json_constructor<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    use ::types_nodes::JsonConstructorType as JC;
    let ctor = node.as_json_constructor_expr().unwrap();

    if let Some(func) = ctor.func {
        init_expr_rec(func, state, mcx, out, agg, params, sub)?;
    } else if (ctor.r#type == JC::JSCTOR_JSON_PARSE && !ctor.unique)
        || ctor.r#type == JC::JSCTOR_JSON_SERIALIZE
    {
        init_expr_rec(ctor.args.first().expect("args"), state, mcx, out, agg, params, sub)?;
    } else {
        let nargs = ctor.args.len();
        let n = nargs.max(1);
        let slots: NonNull<::datum::NullableDatum> = alloc_array(mcx, n)?;
        let values: NonNull<::datum::Datum> = alloc_array(mcx, n)?;
        let nulls: NonNull<bool> = alloc_array(mcx, n)?;
        let types: NonNull<::types_core::Oid> = alloc_array(mcx, n)?;

        for (i, arg) in ctor.args.iter().enumerate() {
            // SAFETY: i < n slots of the fresh allocations.
            unsafe {
                types.as_ptr().add(i).write(expr_type(arg));
                if let Some(c) = arg.as_const() {
                    slots.as_ptr().add(i).write(::datum::NullableDatum {
                        value: c.constvalue,
                        isnull: c.constisnull,
                    });
                    continue;
                }
                let slot = NonNull::new_unchecked(slots.as_ptr().add(i));
                init_expr_rec(arg, state, mcx, OutRef(slot), agg, params, sub)?;
            }
        }

        let is_jsonb = ctor.returning.expect("returning").format.expect("format").format_type
            == ::types_nodes::primnodes::JsonFormatType::JS_FORMAT_JSONB;

        let (scalar_json, scalar_jsonb) = if ctor.r#type == JC::JSCTOR_JSON_SCALAR {
            // SAFETY: nargs == 1 for JSCTOR_JSON_SCALAR; types[0] just written.
            let typid = unsafe { types.as_ptr().read() };
            // Raw write: the carriers hold an FmgrInfo (droppy fn_extra slot);
            // like FuncFrame flinfo they are released by plan teardown, never
            // by arena drop.
            if is_jsonb {
                let cat = ::adt_jsonb::tojsonb::json_categorize_type(typid)?;
                let slot: NonNull<::adt_jsonb::tojsonb::ValCategory> = alloc_array_nodrop_exempt(mcx, 1)?;
                // SAFETY: fresh exclusive allocation.
                unsafe { slot.as_ptr().write(cat) };
                (None, Some(slot))
            } else {
                let cat = ::adt_json::tojson::json_categorize_type(typid)?;
                let slot: NonNull<::adt_json::tojson::TypeCat> = alloc_array_nodrop_exempt(mcx, 1)?;
                // SAFETY: fresh exclusive allocation.
                unsafe { slot.as_ptr().write(cat) };
                (Some(slot), None)
            }
        } else {
            (None, None)
        };

        let jcstate = ::mcx::leak_in(::mcx::alloc_in(
            mcx,
            crate::steps::JsonConstructorState {
                ctor_type: ctor.r#type,
                is_jsonb,
                absent_on_null: ctor.absent_on_null,
                unique: ctor.unique,
                nargs: nargs as u16,
                slots,
                values,
                nulls,
                types,
                scalar_json,
                scalar_jsonb,
            },
        )?);

        let frame_ix = state.frames.len() as u32;
        let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);

        push_step(
            state,
            mcx,
            Step::JsonConstructor { jcstate: NonNull::from(jcstate), frame: frame_ix, out },
        )?;
    }

    if let Some(coercion) = ctor.coercion {
        let saved = state.innermost_case;
        state.innermost_case = Some(out.0);
        init_expr_rec(coercion, state, mcx, out, agg, params, sub)?;
        state.innermost_case = saved;
    }
    Ok(())
}

// The escontext embeds droppy PgError storage; like the categorize carriers
// it lives for the ExprState's life and is never arena-dropped.
fn alloc_json_expr_state<'mcx>(
    mcx: Mcx<'mcx>,
    v: crate::steps::JsonExprState,
) -> PgResult<NonNull<crate::steps::JsonExprState>> {
    let p: NonNull<crate::steps::JsonExprState> = alloc_array_nodrop_exempt(mcx, 1)?;
    // SAFETY: fresh exclusive allocation.
    unsafe { p.write(v) };
    Ok(p)
}

// C ExecInitJsonExpr (execExpr.c:4750).
#[allow(clippy::too_many_arguments)]
fn init_json_expr<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    use ::adt_jsonpath_exec::JsonPathVariable;
    use ::types_nodes::primnodes::{
        JsonBehavior, JsonBehaviorType, JsonExprOp, JsonWrapper as NodeWrapper,
    };
    use core::ptr::addr_of_mut;

    let jsexpr = node.as_json_expr().unwrap();
    let on_error: &JsonBehavior<'_> =
        jsexpr.on_error.expect("JsonExpr.on_error").as_json_behavior().unwrap();
    let on_empty: Option<&JsonBehavior<'_>> =
        jsexpr.on_empty.map(|n| n.as_json_behavior().unwrap());
    let returning = jsexpr.returning.expect("JsonExpr.returning");
    let returning_domain = lsyscache::get_typtype(returning.typid)? == lsyscache::TYPTYPE_DOMAIN;

    let nvars = jsexpr.passing_values.len();
    let var_cells: NonNull<::datum::NullableDatum> = alloc_array(mcx, nvars)?;
    // JsonPathVariable holds a name slice: no zero-init, every entry written below.
    let vars: NonNull<JsonPathVariable<'static>> = alloc_array_nodrop_exempt(mcx, nvars)?;

    let wrapper = match jsexpr.wrapper {
        NodeWrapper::JSW_UNSPEC => ::adt_jsonpath_exec::JsonWrapper::Unspec,
        NodeWrapper::JSW_NONE => ::adt_jsonpath_exec::JsonWrapper::None,
        NodeWrapper::JSW_CONDITIONAL => ::adt_jsonpath_exec::JsonWrapper::Conditional,
        NodeWrapper::JSW_UNCONDITIONAL => ::adt_jsonpath_exec::JsonWrapper::Unconditional,
    };
    let jsestate = alloc_json_expr_state(
        mcx,
        crate::steps::JsonExprState {
            op: jsexpr.op,
            // SAFETY: the node arena outlives the program (RowNullState restamp shape).
            column_name: jsexpr.column_name.map(|s| {
                NonNull::from(unsafe { core::mem::transmute::<&str, &'static str>(s) })
            }),
            wrapper,
            returning_typid: returning.typid,
            use_io_coercion: jsexpr.use_io_coercion,
            use_json_coercion: jsexpr.use_json_coercion,
            throw_error: on_error.btype == JsonBehaviorType::JSON_BEHAVIOR_ERROR,
            on_error_btype: on_error.btype,
            on_empty_btype: on_empty.map(|b| b.btype),
            formatted_expr: ::datum::NullableDatum::null(),
            pathspec: ::datum::NullableDatum::null(),
            error: ::datum::NullableDatum::null(),
            empty: ::datum::NullableDatum::null(),
            nvars: nvars as u16,
            vars,
            var_cells,
            jump_error: -1,
            jump_empty: -1,
            jump_eval_coercion: -1,
            jump_end: -1,
            input_fcinfo: None,
            escontext: ::types_fmgr::ErrorSaveNode::new(false),
        },
    )?;
    let jsp = jsestate.as_ptr();
    // SAFETY: field projections of the live compile-allocated state.
    let (fmt_out, path_out, error_out, empty_out) = unsafe {
        (
            OutRef(NonNull::new_unchecked(addr_of_mut!((*jsp).formatted_expr))),
            OutRef(NonNull::new_unchecked(addr_of_mut!((*jsp).pathspec))),
            OutRef(NonNull::new_unchecked(addr_of_mut!((*jsp).error))),
            OutRef(NonNull::new_unchecked(addr_of_mut!((*jsp).empty))),
        )
    };

    let mut jumps_return_null: PgVec<'_, usize> = PgVec::new_in(mcx);
    init_expr_rec(
        jsexpr.formatted_expr.expect("JsonExpr.formatted_expr"),
        state,
        mcx,
        fmt_out,
        agg,
        params,
        sub,
    )?;
    jumps_return_null.push(state.steps.len());
    push_step(state, mcx, Step::JumpIfNull { jumpdone: u32::MAX, out: fmt_out })?;

    init_expr_rec(jsexpr.path_spec.expect("JsonExpr.path_spec"), state, mcx, path_out, agg, params, sub)?;
    jumps_return_null.push(state.steps.len());
    push_step(state, mcx, Step::JumpIfNull { jumpdone: u32::MAX, out: path_out })?;

    for (i, (argexpr, argname)) in
        jsexpr.passing_values.iter().zip(jsexpr.passing_names.iter()).enumerate()
    {
        let name = argname.as_string().expect("passing name is a String node").sval;
        // SAFETY: i < nvars fresh slots; the node arena outlives the program.
        unsafe {
            vars.as_ptr().add(i).write(JsonPathVariable {
                name: core::mem::transmute::<&[u8], &'static [u8]>(name.as_bytes()),
                typid: expr_type(argexpr),
                typmod: expr_typmod_closed(argexpr),
                value: ::datum::Datum::null(),
                isnull: true,
            });
        }
        // SAFETY: i < nvars slots of the fresh cell array.
        let cell = unsafe { NonNull::new_unchecked(var_cells.as_ptr().add(i)) };
        init_expr_rec(argexpr, state, mcx, OutRef(cell), agg, params, sub)?;
    }

    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);
    push_step(state, mcx, Step::JsonExprPath { jsestate, frame: frame_ix, out })?;

    let null_target = state.steps.len() as u32;
    for ix in jumps_return_null.iter() {
        match &mut state.steps[*ix] {
            Step::JumpIfNull { jumpdone, .. } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = null_target;
            }
            _ => unreachable!(),
        }
    }
    push_step(state, mcx, Step::Const { value: ::datum::Datum::null(), isnull: true, out })?;

    let soft = on_error.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR;
    // SAFETY: field projection of the live state; ErrorSaveNode leads with FmNode.
    let esc_ptr: Option<NonNull<::types_fmgr::ErrorSaveNode>> =
        soft.then(|| unsafe { NonNull::new_unchecked(addr_of_mut!((*jsp).escontext)) });

    let mut jump_eval_coercion = -1i32;
    if jsexpr.use_json_coercion {
        jump_eval_coercion = state.steps.len() as i32;
        init_json_coercion(
            state,
            mcx,
            returning,
            esc_ptr,
            jsexpr.omit_quotes,
            jsexpr.op == JsonExprOp::JSON_EXISTS_OP,
            out,
        )?;
    } else if jsexpr.use_io_coercion {
        let (typinput, typioparam) = lsyscache::getTypeInputInfo(returning.typid)?;
        let flinfo = fmgr_core::fmgr_info(typinput)?;
        let in_frame = FuncFrame::new_in(mcx, flinfo, 3, ::types_core::primitive::InvalidOid)?;
        // SAFETY: slots 1/2 of the fresh 3-arg fcinfo image, written once at compile.
        unsafe {
            in_frame.arg_slot(1).write(::datum::NullableDatum {
                value: ::datum::Datum::from_oid(typioparam),
                isnull: false,
            });
            in_frame.arg_slot(2).write(::datum::NullableDatum {
                value: ::datum::Datum::from_i32(returning.typmod),
                isnull: false,
            });
            if let Some(esc) = esc_ptr {
                crate::steps::fcinfo_mut(in_frame.fcinfo, 3).context = Some(esc.cast());
            }
        }
        let call = FuncCall {
            fcinfo: in_frame.fcinfo,
            flinfo: in_frame.flinfo,
            frame: state.frames.len() as u32,
            nargs: 3,
        };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(in_frame);
        // SAFETY: live compile-allocated state, sole reference here.
        unsafe { (*jsp).input_fcinfo = Some(call) };
    }
    // SAFETY: as above.
    unsafe { (*jsp).jump_eval_coercion = jump_eval_coercion };

    if jump_eval_coercion >= 0 && soft {
        push_step(state, mcx, Step::JsonCoercionFinish { jsestate, out })?;
    }

    let null_const_shortcut = |b: &JsonBehavior<'_>| {
        b.expr.and_then(|e| e.as_const()).is_some_and(|c| c.constisnull) && !returning_domain
    };
    let behavior_needs_finish = |b: &JsonBehavior<'_>| {
        b.coerce
            || b.expr.is_some_and(|e| {
                matches!(e.node_tag(), NodeTag::T_CoerceViaIO | NodeTag::T_CoerceToDomain)
            })
    };

    let mut jumps_to_end: PgVec<'_, usize> = PgVec::new_in(mcx);
    if on_error.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR && !null_const_shortcut(on_error) {
        // SAFETY: live compile-allocated state, sole reference here.
        unsafe { (*jsp).jump_error = state.steps.len() as i32 };
        jumps_to_end.push(state.steps.len());
        push_step(state, mcx, Step::JumpIfNotTrue { jumpdone: u32::MAX, out: error_out })?;

        let saved_escontext = state.escontext;
        state.escontext = esc_ptr;
        init_expr_rec(on_error.expr.expect("JsonBehavior.expr"), state, mcx, out, agg, params, sub)?;
        state.escontext = saved_escontext;

        if on_error.coerce {
            init_json_coercion(state, mcx, returning, esc_ptr, jsexpr.omit_quotes, false, out)?;
        }
        if behavior_needs_finish(on_error) {
            push_step(state, mcx, Step::JsonCoercionFinish { jsestate, out })?;
        }

        jumps_to_end.push(state.steps.len());
        push_step(state, mcx, Step::Jump { jumpdone: u32::MAX })?;
    }

    if let Some(on_empty) = on_empty {
        if on_empty.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR
            && !null_const_shortcut(on_empty)
        {
            // SAFETY: live compile-allocated state, sole reference here.
            unsafe { (*jsp).jump_empty = state.steps.len() as i32 };
            jumps_to_end.push(state.steps.len());
            push_step(state, mcx, Step::JumpIfNotTrue { jumpdone: u32::MAX, out: empty_out })?;

            let saved_escontext = state.escontext;
            state.escontext = esc_ptr;
            init_expr_rec(
                on_empty.expr.expect("JsonBehavior.expr"),
                state,
                mcx,
                out,
                agg,
                params,
                sub,
            )?;
            state.escontext = saved_escontext;

            if on_empty.coerce {
                init_json_coercion(state, mcx, returning, esc_ptr, jsexpr.omit_quotes, false, out)?;
            }
            if behavior_needs_finish(on_empty) {
                push_step(state, mcx, Step::JsonCoercionFinish { jsestate, out })?;
            }
        }
    }

    let done = state.steps.len() as u32;
    for ix in jumps_to_end.iter() {
        match &mut state.steps[*ix] {
            Step::JumpIfNotTrue { jumpdone, .. } | Step::Jump { jumpdone } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = done;
            }
            _ => unreachable!(),
        }
    }
    // SAFETY: live compile-allocated state, sole reference here.
    unsafe { (*jsp).jump_end = done as i32 };
    Ok(())
}

// C ExecInitJsonCoercion (execExpr.c:5051). The state embeds the droppy
// json_populate_type cache (FmgrInfo tree); it lives for the ExprState's life
// and is never arena-dropped.
fn init_json_coercion<'mcx>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    returning: &::types_nodes::primnodes::JsonReturning<'_>,
    escontext: Option<NonNull<::types_fmgr::ErrorSaveNode>>,
    omit_quotes: bool,
    exists_coerce: bool,
    out: OutRef,
) -> PgResult<()> {
    let exists_cast_to_int = exists_coerce
        && lsyscache::getBaseType(returning.typid)? == ::types_core::catalog::INT4OID;
    let exists_check_domain =
        exists_coerce && typcache::DomainHasConstraints(returning.typid)?;
    let jc: NonNull<crate::steps::JsonCoercionState> = alloc_array_nodrop_exempt(mcx, 1)?;
    // SAFETY: fresh exclusive allocation; the compile mcx outlives every eval
    // of this step, so the 'static restamp never escapes it.
    unsafe {
        jc.as_ptr().write(crate::steps::JsonCoercionState {
            targettype: returning.typid,
            targettypmod: returning.typmod,
            omit_quotes,
            exists_coerce,
            exists_cast_to_int,
            exists_check_domain,
            escontext,
            cache: None,
            mcx: core::mem::transmute::<Mcx<'mcx>, Mcx<'static>>(mcx),
        });
    }
    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);
    push_step(state, mcx, Step::JsonCoercion { jc, frame: frame_ix, out })
}

// C ExecInitCoerceToDomain (execExpr.c:3524): constraints baked at compile
// (post-v10 shape); NOTNULL reads the arg's own out, CHECK evaluates into a
// shared compile-allocated slot with CoerceToDomainValue reading domainval.
fn init_coerce_to_domain<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let cd = node.as_coerce_to_domain().unwrap();
    init_expr_rec(cd.arg, state, mcx, out, agg, params, sub)?;

    let cref = typcache::DomainConstraintRef::init(cd.resulttype)?;
    let typlen = cref.typlen();
    let mut check_slot: Option<NonNull<::datum::NullableDatum>> = None;
    let mut domainval: Option<OutRef> = None;
    for con in cref.constraints() {
        match con.constrainttype {
            typcache::DomConstraintType::NotNull => {
                push_step(
                    state,
                    mcx,
                    Step::DomainNotNull {
                        resulttype: cd.resulttype,
                        escontext: state.escontext,
                        out,
                    },
                )?;
            }
            typcache::DomConstraintType::Check => {
                let check = match check_slot {
                    Some(c) => c,
                    None => {
                        let c = alloc_nullable_datum(mcx)?;
                        check_slot = Some(c);
                        c
                    }
                };
                let dv = match domainval {
                    Some(dv) => dv,
                    None => {
                        // R/W expanded inputs must be read R/O by the checks.
                        let dv = if typlen == -1 {
                            let ro = OutRef(alloc_nullable_datum(mcx)?);
                            push_step(state, mcx, Step::MakeReadonlyOut { src: out, out: ro })?;
                            ro
                        } else {
                            out
                        };
                        domainval = Some(dv);
                        dv
                    }
                };
                let save = state.innermost_domain;
                state.innermost_domain = Some(dv);
                init_expr_rec(
                    con.check_expr.expect("CHECK DomainConstraintState carries check_expr"),
                    state,
                    mcx,
                    OutRef(check),
                    agg,
                    params,
                    sub,
                )?;
                state.innermost_domain = save;
                let name: &'mcx str = str_in(mcx, con.name)?;
                push_step(
                    state,
                    mcx,
                    Step::DomainCheck {
                        resulttype: cd.resulttype,
                        name: NonNull::from(name),
                        check,
                        escontext: state.escontext,
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = ::mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

// C ExecInitExprRec T_BoolExpr: args evaluate into the BoolExpr's own output,
// AND/OR short-circuit via jumpdone with anynull NULL bookkeeping.
fn init_bool_expr<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    use ::types_nodes::primnodes::BoolExprType;
    let b = node.as_bool_expr().unwrap();
    let nargs = b.args.len();
    if b.boolop == BoolExprType::NOT_EXPR {
        assert!(nargs == 1, "NOT with {nargs} args");
        init_expr_rec(b.args.nth(0), state, mcx, out, agg, params, sub)?;
        return push_step(state, mcx, Step::BoolNotStep { out });
    }
    assert!(nargs >= 2, "{:?} with {nargs} args", b.boolop);
    let anynull = alloc_bool(mcx)?;
    let is_and = b.boolop == BoolExprType::AND_EXPR;
    let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
    for (off, arg) in b.args.iter().enumerate() {
        init_expr_rec(arg, state, mcx, out, agg, params, sub)?;
        let step = match (is_and, off) {
            (true, 0) => Step::BoolAndStepFirst { anynull, jumpdone: u32::MAX, out },
            (true, o) if o + 1 == nargs => Step::BoolAndStepLast { anynull, out },
            (true, _) => Step::BoolAndStep { anynull, jumpdone: u32::MAX, out },
            (false, 0) => Step::BoolOrStepFirst { anynull, jumpdone: u32::MAX, out },
            (false, o) if o + 1 == nargs => Step::BoolOrStepLast { anynull, out },
            (false, _) => Step::BoolOrStep { anynull, jumpdone: u32::MAX, out },
        };
        if !matches!(step, Step::BoolAndStepLast { .. } | Step::BoolOrStepLast { .. }) {
            adjust_jumps.push(state.steps.len());
        }
        push_step(state, mcx, step)?;
    }
    let done = state.steps.len() as u32;
    for ix in adjust_jumps.iter() {
        match &mut state.steps[*ix] {
            Step::BoolAndStepFirst { jumpdone, .. }
            | Step::BoolAndStep { jumpdone, .. }
            | Step::BoolOrStepFirst { jumpdone, .. }
            | Step::BoolOrStep { jumpdone, .. } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = done;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn init_case_expr<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let c = node.as_case_expr().unwrap();
    let caseval = match c.arg {
        Some(arg) => {
            let slot = alloc_nullable_datum(mcx)?;
            init_expr_rec(arg, state, mcx, OutRef(slot), agg, params, sub)?;
            // C: R/O-force only what could be an expanded datum.
            if lsyscache::get_typlen(expr_type(arg))? == -1 {
                push_step(state, mcx, Step::MakeReadonly { slot })?;
            }
            Some(slot)
        }
        None => None,
    };

    let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
    for w in c.args.iter() {
        let cw = w.as_case_when().expect("CaseWhen");

        let save_innermost = state.innermost_case;
        state.innermost_case = caseval;
        init_expr_rec(cw.expr.expect("CaseWhen.expr"), state, mcx, out, agg, params, sub)?;
        state.innermost_case = save_innermost;

        let whenstep = state.steps.len();
        push_step(state, mcx, Step::JumpIfNotTrue { jumpdone: u32::MAX, out })?;

        init_expr_rec(cw.result.expect("CaseWhen.result"), state, mcx, out, agg, params, sub)?;

        adjust_jumps.push(state.steps.len());
        push_step(state, mcx, Step::Jump { jumpdone: u32::MAX })?;

        let next = state.steps.len() as u32;
        match &mut state.steps[whenstep] {
            Step::JumpIfNotTrue { jumpdone, .. } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = next;
            }
            _ => unreachable!(),
        }
    }

    let defresult = c.defresult.expect("transformCaseExpr always adds a default");
    init_expr_rec(defresult, state, mcx, out, agg, params, sub)?;

    let done = state.steps.len() as u32;
    for ix in adjust_jumps.iter() {
        match &mut state.steps[*ix] {
            Step::Jump { jumpdone } => {
                debug_assert_eq!(*jumpdone, u32::MAX);
                *jumpdone = done;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

// C sets fn_expr to the bare node pointer; the node value is arena-leaked so
// the Copy carrier owns nothing.
pub fn erase_fn_expr<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<FnExprErased> {
    let stored: &Node<'mcx> = ::mcx::forget_box_in(mcx, node)?;
    // SAFETY: same-layout lifetime erasure for the Any cast; the plan arena
    // owns the node and outlives the FmgrInfo (from_node_ref's contract).
    let stored: &Node<'static> =
        unsafe { core::mem::transmute::<&Node<'mcx>, &Node<'static>>(stored) };
    // SAFETY: as above — the arena outlives every downcast_ref reader.
    Ok(unsafe { FnExprErased::from_node_ref(stored) })
}

fn alloc_bool(mcx: Mcx<'_>) -> PgResult<NonNull<bool>> {
    let layout = core::alloc::Layout::new::<bool>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<bool> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(false) };
    Ok(p)
}

// C ExecInitExprRec T_MinMaxExpr: btree cmp proc via typcache, resolve-once
// 2-arg frame, args evaluated into a compile-allocated slot array.
fn init_minmax<'mcx>(
    node: Node<'mcx>,
    mm: &'mcx ::types_nodes::primnodes::MinMaxExpr<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Step> {
    let nelems = mm.args.len();
    let entry = typcache::lookup_type_cache(mm.minmaxtype, typcache::TYPECACHE_CMP_PROC)?;
    let cmp_proc = entry.cmp_proc();
    if cmp_proc == 0 {
        return Err(no_cmp_function(mm.minmaxtype)?);
    }
    let mut flinfo = fmgr_core::fmgr_info(cmp_proc)?;
    flinfo.fn_expr = Some(erase_fn_expr(mcx, node)?);
    let frame = FuncFrame::new_in(mcx, flinfo, 2, mm.inputcollid)?;
    let frame_ix = state.frames.len() as u32;
    let call = FuncCall { fcinfo: frame.fcinfo, flinfo: frame.flinfo, frame: frame_ix, nargs: 2 };
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    let layout = core::alloc::Layout::array::<::datum::NullableDatum>(nelems)
        .expect("minmax slots layout");
    let slots: NonNull<::datum::NullableDatum> =
        mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
    for (i, arg) in mm.args.iter().enumerate() {
        // SAFETY: i < nelems of the freshly allocated slot array.
        let arg_out = OutRef(unsafe { NonNull::new_unchecked(slots.as_ptr().add(i)) });
        init_expr_rec(arg, state, mcx, arg_out, agg, params, sub)?;
    }
    Ok(Step::MinMax {
        call,
        slots,
        nelems: nelems as u32,
        least: mm.op == ::types_nodes::primnodes::MinMaxOp::IS_LEAST,
        out,
    })
}

#[cold]
#[inline(never)]
fn no_cmp_function(type_oid: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_oid)?;
    Ok(Box::new(
        PgError::error(format!("could not identify a comparison function for type {name}"))
            .with_sqlstate(::types_error::ERRCODE_UNDEFINED_FUNCTION),
    ))
}

// C's per-eval ExecEvalParamExtern checks hoisted: values are fixed for one
// execution, so the per-tuple read is one load; mismatch guards are compile-time.
fn init_param(param: &Param, params: ParamBind<'_>, out: OutRef) -> PgResult<Step> {
    let paramid = param.paramid;
    match param.paramkind {
        ParamKind::PARAM_EXEC => {
            assert!(
                paramid >= 0 && (paramid as u32) < params.n_exec,
                "EEOP_PARAM_EXEC: paramid {paramid} outside es_param_exec_vals[0..{}]",
                params.n_exec
            );
            let base = params.exec_vals.expect("n_exec > 0 implies a base pointer");
            // SAFETY: paramid bounds-checked against the once-sized array.
            let prm = unsafe { NonNull::new_unchecked(base.as_ptr().add(paramid as usize)) };
            Ok(Step::ParamExec { prm, out })
        }
        ParamKind::PARAM_EXTERN => {
            let list = params.extern_params.unwrap_or(&[]);
            if paramid <= 0 || paramid as usize > list.len() {
                return Ok(Step::ParamExternMissing { paramid });
            }
            let prm = &list[(paramid - 1) as usize];
            if prm.ptype == 0 {
                return Ok(Step::ParamExternMissing { paramid });
            }
            assert!(
                prm.ptype == param.paramtype,
                "EEOP_PARAM_EXTERN: parameter {paramid} bound as type {} but planned as {}",
                prm.ptype,
                param.paramtype
            );
            Ok(Step::ParamExtern { prm: NonNull::from(prm), out })
        }
        other => panic!(
            "execexpr ExecInitExprRec: Param kind {other:?} must not reach the executor \
             (PARAM_SUBLINK/PARAM_MULTIEXPR are rewritten by the planner)"
        ),
    }
}

#[cold]
#[inline(never)]
pub(crate) fn no_param_value(paramid: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("no value found for parameter {paramid}"))
            .with_sqlstate(::types_error::ERRCODE_UNDEFINED_OBJECT),
    )
}

// pg_class.dat / parsenodes.h / acl.h values, verified against 18.3 headers.
const PROCEDURE_RELATION_ID: Oid = 1255;
const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;

#[cold]
#[inline(never)]
fn permission_denied(mcx: Mcx<'_>, funcid: Oid) -> PgResult<Box<PgError>> {
    let name = lsyscache::get_func_name(mcx, funcid)?;
    let name = name.as_ref().map(|n| n.as_str()).unwrap_or("(unknown)");
    Ok(Box::new(
        PgError::error(format!("permission denied for function {name}"))
            .with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_args(nargs: usize) -> Box<PgError> {
    let msg = if FUNC_MAX_ARGS == 1 {
        format!("cannot pass more than {FUNC_MAX_ARGS} argument to a function")
    } else {
        format!("cannot pass more than {FUNC_MAX_ARGS} arguments to a function")
    };
    let _ = nargs;
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_TOO_MANY_ARGUMENTS))
}

#[track_caller]
#[cold]
#[inline(never)]
fn retset_error() -> Box<PgError> {
    Box::new(
        PgError::error("set-valued function called in context that cannot accept a set")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

// C ExecInitFunc: resolve-once FmgrInfo + step-owned fcinfo; Const args are
// written in place at compile time, other args get their fcinfo slot as out.
// C ExecInitExprRec T_CoerceViaIO: arg evaluates into this step's out slot;
// EEOP_IOCOERCE then rewrites it through outfn/infn resolved once here.
fn init_coerce_via_io<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let cio = node.as_coerce_via_io().unwrap();
    init_expr_rec(cio.arg, state, mcx, out, agg, params, sub)?;

    let argtype = expr_type(cio.arg);
    let (outfunc, _) = lsyscache::getTypeOutputInfo(argtype)?;
    let (infunc, typioparam) = lsyscache::getTypeInputInfo(cio.resulttype)?;

    let flinfo_out = fmgr_core::fmgr_info(outfunc)?;
    let frame_out = FuncFrame::new_in(mcx, flinfo_out, 1, ::types_core::primitive::InvalidOid)?;
    let outcall = FuncCall {
        fcinfo: frame_out.fcinfo,
        flinfo: frame_out.flinfo,
        frame: state.frames.len() as u32,
        nargs: 1,
    };
    state.frames.try_reserve(2).map_err(|_| mcx.oom(2 * core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame_out);

    let flinfo_in = fmgr_core::fmgr_info(infunc)?;
    let in_strict = flinfo_in.fn_strict;
    let frame_in = FuncFrame::new_in(mcx, flinfo_in, 3, ::types_core::primitive::InvalidOid)?;
    // SAFETY: slots 1/2 of the frame's freshly allocated 3-arg fcinfo,
    // written once at compile (C sets them in ExecInitExprRec).
    unsafe {
        frame_in.arg_slot(1).write(::datum::NullableDatum {
            value: ::datum::Datum::from_oid(typioparam),
            isnull: false,
        });
        frame_in.arg_slot(2).write(::datum::NullableDatum {
            value: ::datum::Datum::from_i32(-1),
            isnull: false,
        });
    }
    let incall = FuncCall {
        fcinfo: frame_in.fcinfo,
        flinfo: frame_in.flinfo,
        frame: state.frames.len() as u32,
        nargs: 3,
    };
    if let Some(esc) = state.escontext {
        // SAFETY: fresh 3-arg fcinfo image; the ErrorSaveNode outlives the
        // program (it lives in the owning JsonExprState).
        unsafe { crate::steps::fcinfo_mut(frame_in.fcinfo, 3).context = Some(esc.cast()) };
    }
    state.frames.push(frame_in);

    let calls = crate::steps::IoCoerceCalls { outcall, incall, in_strict };
    let raw = mcx
        .allocate(core::alloc::Layout::new::<crate::steps::IoCoerceCalls>())
        .map_err(|_| mcx.oom(core::mem::size_of::<crate::steps::IoCoerceCalls>()))?;
    let p: NonNull<crate::steps::IoCoerceCalls> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(calls) };
    if state.escontext.is_some() {
        push_step(state, mcx, Step::IoCoerceSafe { calls: p, out })
    } else {
        push_step(state, mcx, Step::IoCoerce { calls: p, out })
    }
}

// The elemexpr becomes a standalone program reading one element through its
// CaseTestVal slot; a 1-step CASE_TESTVAL program is C's NULL elemexprstate.
#[allow(clippy::too_many_arguments)]
fn init_array_coerce<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let ace = node.as_array_coerce_expr().unwrap();
    init_expr_rec(ace.arg, state, mcx, out, agg, params, sub)?;

    let resultelemtype = ::lsyscache::get_element_type(ace.resulttype)?;
    if !::types_core::OidIsValid(resultelemtype) {
        return Err(::types_error::PgError::error("target type is not an array".to_string())
            .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }
    let (ret_typlen, ret_typbyval, ret_typalign) =
        ::lsyscache::get_typlenbyvalalign(resultelemtype)?;

    let elemexpr = ace.elemexpr.expect("ArrayCoerceExpr has an elemexpr");
    let slot = alloc_nullable_datum(mcx)?;
    let mut substate = ExprState::new_boxed_in(mcx)?;
    create_expr_setup_steps(&mut substate, mcx, &[elemexpr], None, ParamBind::NONE, None)?;
    substate.innermost_case = Some(slot);
    let rout = substate.result_out();
    init_expr_rec(elemexpr, &mut substate, mcx, rout, None, ParamBind::NONE, None)?;

    let trivial =
        substate.steps.len() == 1 && matches!(substate.steps[0], Step::CaseTestVal { .. });
    let elem = if trivial {
        None
    } else {
        push_step(&mut substate, mcx, Step::DoneReturn)?;
        ready_expr(&mut substate);
        let stp: NonNull<ExprState<'static>> = NonNull::from(&mut *substate).cast();
        // The program leaks into the plan mcx (wholesale reset reclaims it).
        core::mem::forget(substate);
        // SAFETY: plan-mcx state restamped 'static; the plan mcx outlives
        // every eval of this step.
        Some(crate::arrayops::ArrayCoerceElem { slot, state: stp })
    };

    let acs = crate::arrayops::ArrayCoerceState {
        resultelemtype,
        ret_typlen,
        ret_typbyval,
        ret_typalign: ret_typalign as u8,
        inp_elemtype: ::types_core::InvalidOid,
        inp_typlen: 0,
        inp_typbyval: false,
        inp_typalign: 0,
        elem,
        resmcx: None,
    };
    let p = alloc_state(mcx, acs)?;
    register_alloc_state(state, mcx, p)?;
    push_step(state, mcx, Step::ArrayCoerce { state: p, out })
}

#[allow(clippy::too_many_arguments)]
fn init_convert_rowtype<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<()> {
    let convert = node.as_convert_rowtype_expr().unwrap();
    init_expr_rec(convert.arg, state, mcx, out, agg, params, sub)?;

    let intype = expr_type(convert.arg);
    let indesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, intype, -1)?;
    let outdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, convert.resulttype, -1)?;

    let map = build_attrmap_by_name(mcx, &indesc, &outdesc)?;

    let alloc_desc = |desc: TupleDescData<'mcx>| -> PgResult<NonNull<TupleDescData<'static>>> {
        let layout = core::alloc::Layout::new::<TupleDescData<'static>>();
        let p: NonNull<TupleDescData<'static>> =
            mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
        // SAFETY: fresh exact-layout allocation; the plan mcx outlives every
        // eval of this step, so the 'static restamp never escapes it.
        unsafe {
            p.as_ptr().write(core::mem::transmute::<
                TupleDescData<'mcx>,
                TupleDescData<'static>,
            >(desc));
        }
        Ok(p)
    };
    let outdesc_ptr = alloc_desc(outdesc)?;
    let indesc_ptr = alloc_desc(indesc)?;

    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    let crs = crate::steps::ConvertRowtypeState { indesc: indesc_ptr, outdesc: outdesc_ptr, map };
    let p = alloc_state(mcx, crs)?;
    push_step(state, mcx, Step::ConvertRowtype { state: p, frame: frame_ix, out })
}

// attmap.c build_attrmap_by_name + tupconvert.c convert_tuples_by_name's
// identity elision: Ok(None) = physically compatible, relabel only.
fn build_attrmap_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
) -> PgResult<Option<NonNull<[i16]>>> {
    let outnatts = outdesc.natts as usize;
    let innatts = indesc.natts as usize;
    let mut map: ::mcx::PgVec<'mcx, i16> = ::mcx::vec_with_capacity_in(mcx, outnatts)?;
    map.resize(outnatts, 0);
    let mut nextindesc: isize = -1;
    for i in 0..outnatts {
        let outatt = &outdesc.attrs[i];
        if outatt.attisdropped {
            continue;
        }
        for _ in 0..innatts {
            nextindesc += 1;
            if nextindesc >= innatts as isize {
                nextindesc = 0;
            }
            let inatt = &indesc.attrs[nextindesc as usize];
            if inatt.attisdropped {
                continue;
            }
            if inatt.attname.name_str() == outatt.attname.name_str() {
                if outatt.atttypid != inatt.atttypid || outatt.atttypmod != inatt.atttypmod {
                    return Err(row_convert_error(
                        outatt.attname.name_str(),
                        outdesc.tdtypeid,
                        indesc.tdtypeid,
                        true,
                    ));
                }
                map[i] = inatt.attnum;
                break;
            }
        }
        if map[i] == 0 {
            return Err(row_convert_error(
                outatt.attname.name_str(),
                outdesc.tdtypeid,
                indesc.tdtypeid,
                false,
            ));
        }
    }
    // check_attrmap_match (attmap.c): same column order and layout means the
    // tuple converts by header relabel alone.
    if innatts == outnatts {
        let identity = (0..outnatts).all(|i| {
            if map[i] == (i + 1) as i16 {
                return true;
            }
            let inatt = &indesc.attrs[i];
            let outatt = &outdesc.attrs[i];
            map[i] == 0
                && inatt.attisdropped
                && outatt.attisdropped
                && inatt.attlen == outatt.attlen
                && inatt.attalign == outatt.attalign
        });
        if identity {
            return Ok(None);
        }
    }
    let leaked: &'mcx mut [i16] = map.leak();
    Ok(Some(NonNull::from(leaked)))
}

#[cold]
#[inline(never)]
fn row_convert_error(
    attname: &[u8],
    outtype: Oid,
    intype: Oid,
    mismatch: bool,
) -> alloc::boxed::Box<::types_error::PgError> {
    let name = alloc::string::String::from_utf8_lossy(attname).into_owned();
    let outn = ::format_type::format_type_be(outtype).unwrap_or_else(|_| outtype.to_string());
    let inn = ::format_type::format_type_be(intype).unwrap_or_else(|_| intype.to_string());
    let detail = if mismatch {
        format!(
            "Attribute \"{name}\" of type {outn} does not match corresponding attribute of \
             type {inn}."
        )
    } else {
        format!("Attribute \"{name}\" of type {outn} does not exist in type {inn}.")
    };
    Box::new(
        PgError::error("could not convert row type".to_string())
            .with_detail(detail)
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

fn init_func<'mcx>(
    node: Node<'mcx>,
    args: &NodeList<'mcx>,
    funcid: Oid,
    inputcollid: Oid,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<Step> {
    let nargs = args.len();

    let userid = miscinit_seams::get_user_id::call();
    let aclresult =
        aclchk_seams::object_aclcheck::call(PROCEDURE_RELATION_ID, funcid, userid, ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        return Err(permission_denied(mcx, funcid)?);
    }

    if nargs > FUNC_MAX_ARGS {
        return Err(too_many_args(nargs));
    }

    let mut flinfo = fmgr_core::fmgr_info(funcid)?;
    flinfo.fn_expr = Some(erase_fn_expr(mcx, node)?);
    if flinfo.fn_retset {
        return Err(retset_error());
    }

    let fn_strict = flinfo.fn_strict;
    let fn_stats = flinfo.fn_stats;
    let mut frame = FuncFrame::new_in(mcx, flinfo, nargs as u16, inputcollid)?;

    let frame_ix = state.frames.len() as u32;
    let mut const_bits: u16 = 0;
    let mut const_null_bits: u16 = 0;
    for (argno, arg) in args.iter().enumerate() {
        if let Some(con) = arg.as_const() {
            // SAFETY: slot is inside the frame's freshly allocated fcinfo;
            // consts are written in place once at compile, never per row.
            unsafe {
                frame.arg_slot(argno).write(::datum::NullableDatum {
                    value: con.constvalue,
                    isnull: con.constisnull,
                })
            };
            if argno < 16 {
                const_bits |= 1 << argno;
                if con.constisnull {
                    const_null_bits |= 1 << argno;
                }
            }
        }
    }
    frame.const_args = const_bits;
    frame.const_null_args = const_null_bits;
    let call = FuncCall { fcinfo: frame.fcinfo, flinfo: frame.flinfo, frame: frame_ix, nargs: nargs as u16 };
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);
    for (argno, arg) in args.iter().enumerate() {
        if arg.as_const().is_none() {
            // SAFETY: argno < nargs of the image `call.fcinfo` points at.
            let arg_out = OutRef(unsafe { crate::steps::arg_slot_of(call.fcinfo, argno) });
            init_expr_rec(arg, state, mcx, arg_out, agg, params, sub)?;
        }
    }

    // C: `pgstat_track_functions <= flinfo->fn_stats` picks the non-FUSAGE
    // opcodes; builtins carry TRACK_FUNC_ALL (the enum maximum, never
    // trackable), so the GUC read is skipped on the builtin-dominated path.
    let track = if fn_stats >= TRACK_FUNC_ALL {
        TRACK_FUNC_OFF as i32
    } else {
        guc_tables::vars::pgstat_track_functions.read()
    };
    Ok(if track <= fn_stats as i32 {
        if fn_strict && nargs > 0 {
            match nargs {
                1 => Step::FuncExprStrict1 { call, out },
                2 => Step::FuncExprStrict2 { call, out },
                _ => Step::FuncExprStrict { call, out },
            }
        } else {
            Step::FuncExpr { call, out }
        }
    } else if fn_strict && nargs > 0 {
        Step::FuncExprStrictFusage { call, out }
    } else {
        Step::FuncExprFusage { call, out }
    })
}

// C ExecReadyExpr: kernel selection. The interpreter's unchecked cursor/frame
// accesses rest on this module's private build invariants (Done-terminated,
// Qual jumps valid, FuncCall mirrors its frame) — debug-asserted here.
#[inline]
pub(crate) fn ready_expr(state: &mut ExprState<'_>) {
    let steps = state.steps.as_slice();
    let len = steps.len();
    debug_assert!(len >= 1);
    debug_assert!(matches!(steps[len - 1], Step::DoneReturn | Step::DoneNoReturn));
    #[cfg(debug_assertions)]
    for s in steps {
        match s {
            Step::Qual { jumpdone } => {
                assert!((*jumpdone as usize) < len, "qual jump target out of range");
            }
            Step::BoolAndStepFirst { jumpdone, .. }
            | Step::BoolAndStep { jumpdone, .. }
            | Step::BoolOrStepFirst { jumpdone, .. }
            | Step::BoolOrStep { jumpdone, .. } => {
                assert!((*jumpdone as usize) < len, "boolexpr jump target out of range");
            }
            Step::AggStrictInputCheck { jumpnull, .. }
            | Step::AggStrictInputCheck1 { jumpnull, .. }
            | Step::AggStrictDeserialize { jumpnull, .. } => {
                assert!((*jumpnull as usize) < len, "strict-input jump target out of range");
            }
            Step::Jump { jumpdone }
            | Step::JumpIfNotTrue { jumpdone, .. }
            | Step::JumpIfNotNull { jumpdone, .. } => {
                assert!((*jumpdone as usize) < len, "case jump target out of range");
            }
            Step::JumpIfNull { jumpdone, .. }
            | Step::SbsrefSubscripts { jumpdone, .. }
            | Step::JsonbSbsrefSubscripts { jumpdone, .. } => {
                assert!((*jumpdone as usize) < len, "sbsref jump target out of range");
            }
            Step::JsonExprPath { jsestate, .. } => {
                // SAFETY: compile-allocated state fully written by init_json_expr.
                let js = unsafe { jsestate.as_ref() };
                for j in [js.jump_error, js.jump_empty, js.jump_eval_coercion, js.jump_end] {
                    assert!(j < len as i32, "jsonexpr jump target out of range");
                }
            }
            Step::RowCompareStep { jumpnull, jumpdone, .. } => {
                assert!((*jumpnull as usize) < len, "rowcompare jump target out of range");
                assert!((*jumpdone as usize) < len, "rowcompare jump target out of range");
            }
            Step::ReturningExprStep { jumpdone, .. } => {
                assert!((*jumpdone as usize) < len, "returningexpr jump target out of range");
            }
            Step::FuncExpr { call, .. }
            | Step::FuncExprStrict1 { call, .. }
            | Step::FuncExprStrict2 { call, .. }
            | Step::FuncExprStrict { call, .. }
            | Step::AggPlainTransByVal { call, .. }
            | Step::AggPlainTransStrictByVal { call, .. }
            | Step::AggPlainTransInitStrictByRef { call, .. }
            | Step::AggPlainTransStrictByRef { call, .. }
            | Step::AggPlainTransByRef { call, .. }
            | Step::AggTransByValIndirect { call, .. }
            | Step::AggTransStrictByValIndirect { call, .. }
            | Step::AggTransInitStrictByRefIndirect { call, .. }
            | Step::AggTransStrictByRefIndirect { call, .. }
            | Step::AggTransByRefIndirect { call, .. }
            | Step::AggPlainTransInitStrictByVal { call, .. }
            | Step::AggTransInitStrictByValIndirect { call, .. }
            | Step::HashDatumFirst { call, .. }
            | Step::HashDatumNext32 { call, .. }
            | Step::NotDistinct { call, .. }
            | Step::MinMax { call, .. } => {
                let f = &state.frames[call.frame as usize];
                assert!(call.nargs == f.nargs && call.fcinfo == f.fcinfo);
            }
            _ => {}
        }
    }
    state.flags |= crate::steps::EEO_FLAG_INTERPRETER_INITIALIZED;
    state.kernel = select_kernel(state);
    // PROCPERF P2 compile economy (see COMPILE_ECONOMY): censuses + fusion
    // are per-row-payoff passes; under the cheap-statement window they are
    // skipped — a kernelized program skips them anyway, so economy leaves
    // kernelized programs (select1's shape) byte-identical.
    let economy = economy_active();
    // Multi-clause scan-cmp-const census (lane-v2 batched qual tiers): must
    // run on the PRISTINE program — fuse_program below rewrites the clause
    // shapes (FUNCEXPR_STRICT_2 + QUAL -> FuncStrict2Qual etc.). Quals only;
    // the walk is a few steps on the shapes it can match, and it bails on
    // the first non-matching step otherwise.
    if matches!(state.kernel, Kernel::Program) && state.flags & EEO_FLAG_IS_QUAL != 0 && !economy {
        state.scan_cmp_clauses = select_scan_cmp_clauses(state);
        // Contains-LIKE census (lane-v2 strsearch qual kernel): disjoint by
        // construction (the cmp census admits only int comparators), so it
        // only runs when that census refused.
        if state.scan_cmp_clauses.is_none() {
            state.scan_contains_clause = select_scan_contains_clause(state);
        }
    }
    // Scan-projection census (lane-v2 stitched-projection tier): same
    // PRISTINE-program requirement as the qual census above. Non-quals only;
    // the walk bails on the first non-matching step.
    if matches!(state.kernel, Kernel::Program) && state.flags & EEO_FLAG_IS_QUAL == 0 && !economy {
        state.scan_proj_cols = select_scan_proj_cols(state);
        // Expr-key census (lane-v2 expression group keys): same PRISTINE-
        // program requirement; bails on the first non-matching step.
        state.scan_proj_expr_key = select_scan_proj_expr_key(state);
    }
    // Kernelized programs never run their steps: skipping the peephole keeps
    // compile-per-query lanes (point/select1) free of the pass cost.
    if matches!(state.kernel, Kernel::Program) {
        // JIT compiles the unfused program (kernel and interpreter share the
        // step-state contract); fusion only serves the interpreter loop, so
        // a jitted program skips it. Below the planner's jit gate this is a
        // single thread-local read.
        crate::jit::try_compile(state);
        if state.jit.is_none() {
            if economy {
                thin_steps(state);
            } else {
                fuse_program(state);
            }
        }
    }
    if dump_programs_enabled() {
        dump_program(state);
    }
}

// RowCompareStep carries two jump fields (jumpnull, jumpdone) unlike every
// other stepped jump; visit-all rather than Option<&mut> so fuse_program's
// is_target/remap passes don't silently drop one (jit.rs's jump_targets
// covers both fields for the same reason).
fn for_each_jump_field_mut(step: &mut Step, mut f: impl FnMut(&mut u32)) {
    match step {
        Step::Qual { jumpdone }
        | Step::Jump { jumpdone }
        | Step::JumpIfNotTrue { jumpdone, .. }
        | Step::JumpIfNotNull { jumpdone, .. }
        | Step::JumpIfNull { jumpdone, .. }
        | Step::BoolAndStepFirst { jumpdone, .. }
        | Step::BoolAndStep { jumpdone, .. }
        | Step::BoolOrStepFirst { jumpdone, .. }
        | Step::BoolOrStep { jumpdone, .. }
        | Step::SbsrefSubscripts { jumpdone, .. }
        | Step::JsonbSbsrefSubscripts { jumpdone, .. }
        | Step::FuncStrict2Qual { jumpdone, .. }
        | Step::FuncStrict2QualThin { jumpdone, .. }
        | Step::NotDistinctQual { jumpdone, .. }
        | Step::NotDistinctQualThin { jumpdone, .. }
        | Step::ReturningExprStep { jumpdone, .. } => f(jumpdone),
        Step::AggStrictInputCheck { jumpnull, .. }
        | Step::AggStrictInputCheck1 { jumpnull, .. }
        | Step::AggStrictDeserialize { jumpnull, .. } => f(jumpnull),
        Step::RowCompareStep { jumpnull, jumpdone, .. } => {
            f(jumpnull);
            f(jumpdone);
        }
        _ => {}
    }
}

fn arg_index_of(call: &FuncCall, out: OutRef) -> Option<u8> {
    if call.nargs != 2 {
        return None;
    }
    // SAFETY: args 0/1 of the call's live 2-arg fcinfo image.
    unsafe {
        if out.0 == crate::steps::arg_slot_of(call.fcinfo, 0) {
            Some(0)
        } else if out.0 == crate::steps::arg_slot_of(call.fcinfo, 1) {
            Some(1)
        } else {
            None
        }
    }
}

// Thin-ABI carrier when the resolved fn has a referee'd thin twin.
fn thin_call(call: &FuncCall) -> Option<crate::steps::CallThin> {
    // SAFETY: frame-owned mcx-boxed FmgrInfo, live for 'mcx.
    let fl = unsafe { call.flinfo.as_ref() };
    let f = fmgr_core::fmgr_thin_builtin(fl, call.nargs as i16)?;
    Some(crate::steps::CallThin { fcinfo: call.fcinfo, f })
}

fn thin2(call: &FuncCall) -> Option<crate::steps::CallThin> {
    debug_assert!(call.nargs == 2);
    thin_call(call)
}

fn thin_single(step: &Step) -> Option<Step> {
    match step {
        Step::FuncExprStrict1 { call, out } => {
            Some(Step::FuncExprStrict1Thin { call: thin_call(call)?, out: *out })
        }
        Step::FuncExprStrict2 { call, out } => {
            Some(Step::FuncExprStrict2Thin { call: thin_call(call)?, out: *out })
        }
        Step::AggTransStrictByValIndirect { call, base, transno } => {
            Some(Step::AggTransStrictByValIndirectThin {
                call: thin_call(call)?,
                base: *base,
                transno: *transno,
            })
        }
        _ => None,
    }
}

fn try_fuse(a: &Step, b: &Step) -> Option<Step> {
    match (a, b) {
        (Step::ScanVar { attnum, vartype, out }, Step::FuncExprStrict2 { call, out: fout }) => {
            let argno = arg_index_of(call, *out)?;
            Some(match thin2(call) {
                Some(c) => Step::ScanVarFuncStrict2Thin {
                    attnum: *attnum,
                    vartype: *vartype,
                    argno,
                    call: c,
                    out: *fout,
                },
                None => Step::ScanVarFuncStrict2 {
                    attnum: *attnum,
                    vartype: *vartype,
                    argno,
                    call: (*call).into(),
                    out: *fout,
                },
            })
        }
        (
            Step::FuncExprStrict2 { call: call1, out: out1 },
            Step::FuncExprStrict2 { call: call2, out: fout },
        ) => {
            if call1.fcinfo == call2.fcinfo {
                return None;
            }
            let argno = arg_index_of(call2, *out1)?;
            Some(match (thin2(call1), thin2(call2)) {
                (Some(c1), Some(c2)) => {
                    Step::FuncFuncStrict2Thin { call1: c1, argno, call2: c2, out: *fout }
                }
                _ => Step::FuncFuncStrict2 {
                    call1: (*call1).into(),
                    argno,
                    call2: (*call2).into(),
                    out: *fout,
                },
            })
        }
        (Step::FuncExprStrict2 { call, out }, Step::Qual { jumpdone }) => {
            Some(match thin2(call) {
                Some(c) => {
                    Step::FuncStrict2QualThin { call: c, jumpdone: *jumpdone, out: *out }
                }
                None => Step::FuncStrict2Qual {
                    call: (*call).into(),
                    jumpdone: *jumpdone,
                    out: *out,
                },
            })
        }
        (Step::OuterVar { attnum, vartype, out }, Step::NotDistinct { call, out: fout }) => {
            let argno = arg_index_of(call, *out)?;
            Some(match thin2(call) {
                Some(c) => Step::OuterVarNotDistinctThin {
                    attnum: *attnum,
                    vartype: *vartype,
                    argno,
                    call: c,
                    out: *fout,
                },
                None => Step::OuterVarNotDistinct {
                    attnum: *attnum,
                    vartype: *vartype,
                    argno,
                    call: (*call).into(),
                    out: *fout,
                },
            })
        }
        (Step::NotDistinct { call, out }, Step::Qual { jumpdone }) if call.nargs == 2 => {
            Some(match thin2(call) {
                Some(c) => {
                    Step::NotDistinctQualThin { call: c, jumpdone: *jumpdone, out: *out }
                }
                None => Step::NotDistinctQual {
                    call: (*call).into(),
                    jumpdone: *jumpdone,
                    out: *out,
                },
            })
        }
        (
            Step::OuterVar { attnum, vartype, out },
            Step::AggTransByValIndirect { call, base, transno },
        ) => {
            let argno = arg_index_of(call, *out)?;
            Some(Step::OuterVarAggTransByValIndirect {
                attnum: *attnum,
                vartype: *vartype,
                argno,
                call: (*call).into(),
                base: *base,
                transno: *transno,
            })
        }
        (
            Step::AssignScanVar { attnum: attnum1, resultnum: resultnum1 },
            Step::AssignScanVar { attnum: attnum2, resultnum: resultnum2 },
        ) => Some(Step::AssignScanVar2 {
            attnum1: *attnum1,
            resultnum1: *resultnum1,
            attnum2: *attnum2,
            resultnum2: *resultnum2,
        }),
        _ => None,
    }
}

// Ready-time superinstruction peephole: measured-dominant adjacent step
// pairs collapse into fused steps (one dispatch + arg-slot round trip per
// pair). Runs after select_kernel (kernel matchers see raw shapes); a pair
// whose second step is a jump target stays unfused.
// Test hook: the jit single-step cross-check drives exec_one_step over
// unfused programs (the shape jitted programs keep).
#[cfg(test)]
thread_local! {
    pub(crate) static SKIP_FUSE_FOR_TESTS: core::cell::Cell<bool> =
        const { core::cell::Cell::new(false) };
}

thread_local! {
    // PROCPERF P2 compile economy: per-thread window armed by execmain's
    // standard_executor_start over InitPlan when the statement is cost-gated
    // cheap. While active, ready_expr skips the interpreter-loop optimization
    // passes (lane-v2 censuses + the fusion peephole) whose payoff is
    // per-row: an OLTP point statement recompiles its programs on every
    // execution and runs a handful of rows, so the passes cost more than
    // they can return (C's ExecReadyInterpretedExpr is a single
    // dispatch-assignment walk). RAII-scoped (EconomyWindow) so nested
    // executor starts and error unwinds can never leak the flag across
    // statements. NOT session state: the window never spans a statement
    // boundary, and its effect is a per-program compile-cost policy, never a
    // result byte.
    static COMPILE_ECONOMY: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

#[inline]
pub(crate) fn economy_active() -> bool {
    COMPILE_ECONOMY.with(core::cell::Cell::get)
}

/// RAII compile-economy window (PROCPERF P2, see COMPILE_ECONOMY): while
/// held with `active`, ready_expr skips the per-row-payoff passes (censuses
/// + fusion peephole; kernel selection and the thin-ABI rewrite stay ON).
/// Restores the previous state on drop.
pub struct EconomyWindow {
    prev: bool,
}

pub fn economy_window(active: bool) -> EconomyWindow {
    EconomyWindow { prev: COMPILE_ECONOMY.with(|c| c.replace(active)) }
}

impl Drop for EconomyWindow {
    fn drop(&mut self) {
        COMPILE_ECONOMY.with(|c| c.set(self.prev));
    }
}

// The thin-ABI rewrite alone (fuse_program's no-pair arm): one cheap pass
// that lowers per-eval fmgr cost even at row 1, kept under economy.
fn thin_steps(state: &mut ExprState<'_>) {
    for s in state.steps.iter_mut() {
        if let Some(t) = thin_single(s) {
            *s = t;
        }
    }
}

pub(crate) fn fuse_program(state: &mut ExprState<'_>) {
    #[cfg(test)]
    if SKIP_FUSE_FOR_TESTS.with(|c| c.get()) {
        return;
    }
    let len = state.steps.len();
    if len < 3 {
        thin_steps(state);
        return;
    }
    let has_pair = state
        .steps
        .as_slice()
        .windows(2)
        .any(|w| try_fuse(&w[0], &w[1]).is_some());
    // JsonExprPath jump targets live in its state and would not be remapped.
    let fuse_barrier =
        state.steps.as_slice().iter().any(|s| matches!(s, Step::JsonExprPath { .. }));
    if !has_pair || fuse_barrier {
        thin_steps(state);
        return;
    }
    let mcx = *state.steps.allocator();
    let steps = state.steps.as_slice();
    let mut is_target = ::mcx::vec_with_capacity_in_infallible::<bool>(mcx, len);
    is_target.resize(len, false);
    for s in steps {
        let mut s = *s;
        for_each_jump_field_mut(&mut s, |j| is_target[*j as usize] = true);
    }
    let mut map = ::mcx::vec_with_capacity_in_infallible::<u32>(mcx, len);
    let mut out = ::mcx::vec_with_capacity_in_infallible::<Step>(mcx, len);
    let mut i = 0usize;
    while i < len {
        map.push(out.len() as u32);
        if i + 1 < len && !is_target[i + 1] {
            if let Some(f) = try_fuse(&steps[i], &steps[i + 1]) {
                map.push(out.len() as u32);
                out.push(f);
                i += 2;
                continue;
            }
        }
        out.push(thin_single(&steps[i]).unwrap_or(steps[i]));
        i += 1;
    }
    debug_assert_eq!(map.len(), len);
    for s in out.iter_mut() {
        for_each_jump_field_mut(s, |j| *j = map[*j as usize]);
    }
    state.steps = out;
}

fn dump_programs_enabled() -> bool {
    use core::sync::atomic::{AtomicU8, Ordering};
    static FLAG: AtomicU8 = AtomicU8::new(0);
    match FLAG.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var_os("PGRUST_DUMP_EXPR_PROGRAMS").is_some();
            FLAG.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

#[cold]
#[inline(never)]
fn dump_program(state: &ExprState<'_>) {
    fn tag(dbg: &str) -> &str {
        dbg.split([' ', '(']).next().unwrap_or(dbg)
    }
    let mut line = std::string::String::new();
    for s in state.steps.as_slice() {
        if !line.is_empty() {
            line.push(',');
        }
        let d = std::format!("{s:?}");
        line.push_str(tag(&d));
    }
    let k = std::format!("{:?}", state.kernel);
    std::eprintln!("EXPRDUMP kernel={} steps={}", tag(&k), line);
}

fn var_src(step: &Step) -> Option<(SlotSrc, u16, OutRef)> {
    match step {
        Step::ScanVar { attnum, out, .. } => Some((SlotSrc::Scan, *attnum, *out)),
        Step::InnerVar { attnum, out, .. } => Some((SlotSrc::Inner, *attnum, *out)),
        Step::OuterVar { attnum, out, .. } => Some((SlotSrc::Outer, *attnum, *out)),
        _ => None,
    }
}

fn assign_var_src(step: &Step) -> Option<(SlotSrc, u16, u16)> {
    match step {
        Step::AssignScanVar { attnum, resultnum } => Some((SlotSrc::Scan, *attnum, *resultnum)),
        Step::AssignInnerVar { attnum, resultnum } => Some((SlotSrc::Inner, *attnum, *resultnum)),
        Step::AssignOuterVar { attnum, resultnum } => Some((SlotSrc::Outer, *attnum, *resultnum)),
        _ => None,
    }
}

fn fetch_src(step: &Step) -> Option<SlotSrc> {
    match step {
        Step::ScanFetchSome { .. } => Some(SlotSrc::Scan),
        Step::InnerFetchSome { .. } => Some(SlotSrc::Inner),
        Step::OuterFetchSome { .. } => Some(SlotSrc::Outer),
        _ => None,
    }
}

fn select_kernel(state: &ExprState<'_>) -> Kernel {
    let steps = state.steps.as_slice();
    match steps.len() {
        2 => match &steps[0] {
            Step::Const { value, isnull, out } if state.is_result(*out) => {
                Kernel::JustConst { value: *value, isnull: *isnull }
            }
            Step::FuncExpr { call, out }
            | Step::FuncExprStrict1 { call, out }
            | Step::FuncExprStrict2 { call, out }
            | Step::FuncExprStrict { call, out }
                if state.is_result(*out) && all_args_const(state, *call) =>
            {
                Kernel::JustFunc {
                    fn_addr: call.fn_addr(),
                    frame: call.frame,
                    nargs: call.nargs,
                    strict: !matches!(steps[0], Step::FuncExpr { .. }),
                }
            }
            Step::AggPlainTransByVal { call, pergroup }
                if matches!(steps[1], Step::DoneNoReturn) =>
            {
                match thin_call(call) {
                    Some(c) => {
                        Kernel::AggTransByValThin { call: c, pergroup: *pergroup, strict: false }
                    }
                    None => Kernel::AggTransByVal { call: *call, pergroup: *pergroup, strict: false },
                }
            }
            Step::AggPlainTransStrictByVal { call, pergroup }
                if matches!(steps[1], Step::DoneNoReturn) =>
            {
                match thin_call(call) {
                    Some(c) => {
                        Kernel::AggTransByValThin { call: c, pergroup: *pergroup, strict: true }
                    }
                    None => Kernel::AggTransByVal { call: *call, pergroup: *pergroup, strict: true },
                }
            }
            _ => match (var_src(&steps[0]), assign_var_src(&steps[0])) {
                (Some((src, attnum, out)), _) if state.is_result(out) => {
                    Kernel::JustVarVirt { src, attnum }
                }
                (_, Some((src, attnum, resultnum))) => {
                    Kernel::JustAssignVarVirt { src, attnum, resultnum }
                }
                _ => Kernel::Program,
            },
        },
        3 => {
            if let (Some(fsrc), Some((src, attnum, out))) = (fetch_src(&steps[0]), var_src(&steps[1])) {
                if fsrc == src && state.is_result(out) {
                    return Kernel::JustVar { src, attnum };
                }
            }
            if let (Some(fsrc), Some((src, attnum, resultnum))) =
                (fetch_src(&steps[0]), assign_var_src(&steps[1]))
            {
                if fsrc == src {
                    return Kernel::JustAssignVar { src, attnum, resultnum };
                }
            }
            if let (Step::Const { value, isnull, out }, Step::AssignTmp { resultnum }) =
                (&steps[0], &steps[1])
            {
                if state.is_result(*out) {
                    return Kernel::JustConstAssign { value: *value, isnull: *isnull, resultnum: *resultnum };
                }
            }
            Kernel::Program
        }
        4 => select_hash32_var(state).unwrap_or(Kernel::Program),
        5 => select_fused_qual(state).unwrap_or(Kernel::Program),
        7 => select_qual_var_cmp_var(state).unwrap_or(Kernel::Program),
        _ => Kernel::Program,
    }
}

// Single-key hash [FETCHSOME, VAR->arg0, HASHDATUM_FIRST->result, DONE].
fn select_hash32_var(state: &ExprState<'_>) -> Option<Kernel> {
    let steps = state.steps.as_slice();
    let fsrc = fetch_src(&steps[0])?;
    let (src, attnum, var_out) = var_src(&steps[1])?;
    if fsrc != src {
        return None;
    }
    let Step::HashDatumFirst { call, out } = &steps[2] else {
        return None;
    };
    if !state.is_result(*out) || !matches!(steps[3], Step::DoneReturn) {
        return None;
    }
    let frame = &state.frames[call.frame as usize];
    if var_out.0 != frame.arg_slot(0) {
        return None;
    }
    Some(Kernel::Hash32Var { src, attnum, frame: call.frame })
}

// [FETCHSOME x2, VAR->arg x2, FUNCEXPR_STRICT_2 int comparator, QUAL, DONE].
fn select_qual_var_cmp_var(state: &ExprState<'_>) -> Option<Kernel> {
    let steps = state.steps.as_slice();
    let f0 = fetch_src(&steps[0])?;
    let f1 = fetch_src(&steps[1])?;
    let (s0, a0, out0) = var_src(&steps[2])?;
    let (s1, a1, out1) = var_src(&steps[3])?;
    if !((s0 == f0 && s1 == f1) || (s0 == f1 && s1 == f0)) || s0 == s1 {
        return None;
    }
    let Step::FuncExprStrict2 { call, out } = &steps[4] else {
        return None;
    };
    if !state.is_result(*out) {
        return None;
    }
    let Step::Qual { jumpdone } = steps[5] else {
        return None;
    };
    if jumpdone != 6 || !matches!(steps[6], Step::DoneReturn) {
        return None;
    }
    let frame = &state.frames[call.frame as usize];
    // SAFETY: frame-owned mcx-boxed FmgrInfo, read-only here.
    let cmp = CmpOp::for_fn_oid(unsafe { frame.flinfo.as_ref() }.fn_oid)?;
    let (arg0, arg1) = (frame.arg_slot(0), frame.arg_slot(1));
    let (a, b) = if out0.0 == arg0 && out1.0 == arg1 {
        ((s0, a0), (s1, a1))
    } else if out1.0 == arg0 && out0.0 == arg1 {
        ((s1, a1), (s0, a0))
    } else {
        return None;
    };
    Some(Kernel::QualVarCmpVar {
        a_src: a.0,
        a_attnum: a.1,
        b_src: b.0,
        b_attnum: b.1,
        cmp,
    })
}

fn all_args_const(state: &ExprState<'_>, call: FuncCall) -> bool {
    let frame = &state.frames[call.frame as usize];
    call.nargs <= 16 && frame.const_args.count_ones() == call.nargs as u32
}

// The lever-4 fused shape: [SCAN_FETCHSOME, SCAN_VAR -> arg, FUNCEXPR_STRICT_2
// (other arg a compile-time non-null Const), QUAL, DONE_RETURN] with an
// in-core int comparator -> one branch-free kernel, no fmgr call.
fn select_fused_qual(state: &ExprState<'_>) -> Option<Kernel> {
    let steps = state.steps.as_slice();
    let Step::ScanFetchSome { .. } = steps[0] else {
        return None;
    };
    let (src, attnum, var_out) = var_src(&steps[1])?;
    if src != SlotSrc::Scan {
        return None;
    }
    let Step::FuncExprStrict2 { call, out } = &steps[2] else {
        return None;
    };
    if !state.is_result(*out) {
        return None;
    }
    let Step::Qual { jumpdone } = steps[3] else {
        return None;
    };
    if jumpdone != 4 || !matches!(steps[4], Step::DoneReturn) {
        return None;
    }

    let frame = &state.frames[call.frame as usize];
    // SAFETY: frame-owned mcx-boxed FmgrInfo, read-only here.
    let cmp = CmpOp::for_fn_oid(unsafe { frame.flinfo.as_ref() }.fn_oid)?;
    let var_is_arg0 = var_out.0 == frame.arg_slot(0);
    let const_argno = if var_is_arg0 { 1usize } else { 0 };
    if var_out.0 != frame.arg_slot(if var_is_arg0 { 0 } else { 1 }) {
        return None;
    }
    if frame.const_args & (1 << const_argno) == 0 || frame.const_null_args & (1 << const_argno) != 0
    {
        return None;
    }
    // SAFETY: const arg slot was written at compile and never re-targeted.
    let konst = unsafe { frame.arg_slot(const_argno).read().value };
    let cmp = if var_is_arg0 { cmp } else { cmp.commuted() };
    Some(Kernel::QualScanVarCmpConst { attnum, konst, cmp })
}

// Contains-LIKE qual census (the lane-v2 strsearch qual kernel): the same
// 5-step single-clause shape as `select_fused_qual` — [SCAN_FETCHSOME,
// SCAN_VAR -> arg0, FUNCEXPR_STRICT_2 (textlike, arg1 a compile-time
// non-null Const pattern), QUAL, DONE_RETURN] — with the comparator replaced
// by `textlike` and the Const pattern classified as contains-class
// (`%…literal…%`, ScanContainsClause doc). Runs at ready time on the
// PRISTINE program, like the cmp census. Fail-closed: any unrecognized
// step, arg order (LIKE does not commute), pattern shape, encoding, or
// pattern-Const header form refuses.
fn select_scan_contains_clause(state: &ExprState<'_>) -> Option<crate::steps::ScanContainsClause> {
    use ::types_tuple::varatt::{
        varatt_is_1b, varatt_is_1b_e, varatt_is_4b_u, varsize_1b, varsize_4b, VARHDRSZ,
        VARHDRSZ_SHORT,
    };
    let steps = state.steps.as_slice();
    if steps.len() != 5 {
        return None;
    }
    let Step::ScanFetchSome { .. } = steps[0] else {
        return None;
    };
    let (src, attnum, var_out) = var_src(&steps[1])?;
    if src != SlotSrc::Scan {
        return None;
    }
    let Step::FuncExprStrict2 { call, out } = &steps[2] else {
        return None;
    };
    if !state.is_result(*out) {
        return None;
    }
    let Step::Qual { jumpdone } = steps[3] else {
        return None;
    };
    if jumpdone != 4 || !matches!(steps[4], Step::DoneReturn) {
        return None;
    }

    let frame = &state.frames[call.frame as usize];
    // textlike's pg_proc entries (like.c texticlike/nlike stay per-row).
    // SAFETY: frame-owned mcx-boxed FmgrInfo, read-only here.
    let fn_oid = unsafe { frame.flinfo.as_ref() }.fn_oid;
    if !matches!(fn_oid, 850 | 1569 | 1631) {
        return None;
    }
    // LIKE argument order is fixed: text = arg0, pattern = arg1.
    if var_out.0 != frame.arg_slot(0) {
        return None;
    }
    if frame.const_args & (1 << 1) == 0 || frame.const_null_args & (1 << 1) != 0 {
        return None;
    }
    // Encoding gate: only generic_match_text's ported arms (single-byte /
    // UTF-8) — anything else keeps the per-row path and its error surface.
    if ::mbutils::pg_database_encoding_max_length() != 1
        && ::mbutils::GetDatabaseEncoding() != ::wchar::PG_UTF8
    {
        return None;
    }
    // Collation must be valid (invalid errors per-row; refuse so it does).
    // SAFETY: frame-owned live fcinfo image, read-only borrow.
    let collation = unsafe { crate::steps::fcinfo_mut(frame.fcinfo, frame.nargs) }.fncollation;
    if !::types_core::OidIsValid(collation) {
        return None;
    }
    // Pattern classification over the Const's varlena payload. Plan consts
    // are in-memory varlenas (1B short or plain 4B); anything else refuses.
    // SAFETY: const arg slot was written at compile and never re-targeted.
    let pat = unsafe { frame.arg_slot(1).read().value };
    let p = pat.as_usize() as *const u8;
    // SAFETY: non-null compile-time text Const datum, readable via header.
    let bytes: &[u8] = unsafe {
        if varatt_is_1b(p) && !varatt_is_1b_e(p) {
            core::slice::from_raw_parts(p.add(VARHDRSZ_SHORT), varsize_1b(p) - VARHDRSZ_SHORT)
        } else if varatt_is_4b_u(p) {
            core::slice::from_raw_parts(p.add(VARHDRSZ), varsize_4b(p) - VARHDRSZ)
        } else {
            return None;
        }
    };
    // Shape: leading '%' run + metachar-free non-empty literal + trailing
    // '%' run. Any '_' or '\' anywhere refuses (escape/underscore classes);
    // any '%' inside the literal refuses (multi-segment patterns).
    if bytes.len() < 3 || bytes.iter().any(|&b| b == b'_' || b == b'\\') {
        return None;
    }
    let lead = bytes.iter().take_while(|&&b| b == b'%').count();
    let trail = bytes.iter().rev().take_while(|&&b| b == b'%').count();
    if lead == 0 || trail == 0 || lead + trail >= bytes.len() {
        return None;
    }
    let needle = &bytes[lead..bytes.len() - trail];
    if needle.iter().any(|&b| b == b'%') {
        return None;
    }
    Some(crate::steps::ScanContainsClause::new(
        attnum,
        collation,
        // SAFETY: in-bounds pointer into the live const varlena payload.
        unsafe { core::ptr::NonNull::new_unchecked(needle.as_ptr() as *mut u8) },
        needle.len() as u32,
    ))
}

// Multi-clause generalization of `select_fused_qual` (the lane-v2 batched
// qual census): [SCAN_FETCHSOME*, (SCAN_VAR -> arg, FUNCEXPR_STRICT_2 with
// the other arg a compile-time non-null Const, QUAL -> done)+, DONE_RETURN],
// every comparator an in-core int comparator. Runs at ready time on the
// pristine (pre-fusion) program. 2..=SCAN_CMP_MAX_CLAUSES clauses — the
// 1-clause shape is `select_fused_qual`'s kernel. Fail-closed: any
// unrecognized step refuses.
fn select_scan_cmp_clauses(state: &ExprState<'_>) -> Option<crate::steps::ScanCmpClauses> {
    use crate::steps::{ScanCmpClauses, SCAN_CMP_MAX_CLAUSES};
    let steps = state.steps.as_slice();
    let done = steps.len().checked_sub(1)?;
    if !matches!(steps[done], Step::DoneReturn) {
        return None;
    }
    let mut i = 0usize;
    while i < done {
        let Some(src) = fetch_src(&steps[i]) else { break };
        if src != SlotSrc::Scan {
            return None;
        }
        i += 1;
    }
    let mut out = ScanCmpClauses {
        clauses: [(0, CmpOp::Int4Eq, ::datum::Datum::from_usize(0)); SCAN_CMP_MAX_CLAUSES],
        n: 0,
    };
    while i < done {
        if i + 3 > done || out.n as usize == SCAN_CMP_MAX_CLAUSES {
            return None;
        }
        let (src, attnum, var_out) = var_src(&steps[i])?;
        if src != SlotSrc::Scan {
            return None;
        }
        let Step::FuncExprStrict2 { call, out: fout } = &steps[i + 1] else {
            return None;
        };
        if !state.is_result(*fout) {
            return None;
        }
        let Step::Qual { jumpdone } = steps[i + 2] else {
            return None;
        };
        if jumpdone as usize != done {
            return None;
        }
        let frame = &state.frames[call.frame as usize];
        // SAFETY: frame-owned mcx-boxed FmgrInfo, read-only here.
        let cmp = CmpOp::for_fn_oid(unsafe { frame.flinfo.as_ref() }.fn_oid)?;
        let var_is_arg0 = var_out.0 == frame.arg_slot(0);
        let const_argno = if var_is_arg0 { 1usize } else { 0 };
        if var_out.0 != frame.arg_slot(if var_is_arg0 { 0 } else { 1 }) {
            return None;
        }
        if frame.const_args & (1 << const_argno) == 0
            || frame.const_null_args & (1 << const_argno) != 0
        {
            return None;
        }
        // SAFETY: const arg slot was written at compile and never re-targeted.
        let konst = unsafe { frame.arg_slot(const_argno).read().value };
        let cmp = if var_is_arg0 { cmp } else { cmp.commuted() };
        out.clauses[out.n as usize] = (attnum, cmp, konst);
        out.n += 1;
        i += 3;
    }
    (out.n >= 2).then_some(out)
}

/// Scan-projection census (lane-v2 stitched projections): the projection
/// program as a FETCHSOME(scan) prefix followed by per-column units in
/// resultnum order — `AssignScanVar` (Var passthrough), or a strict-2
/// in-core int-arith call over scan Vars / non-null Consts assigned via
/// `AssignTmp`. Anything else refuses (None): fail-closed vocabulary, like
/// `select_scan_cmp_clauses` above. Runs on the PRISTINE program (before
/// `fuse_program` / JIT selection).
fn select_scan_proj_cols(state: &ExprState<'_>) -> Option<crate::steps::ScanProjCols> {
    use crate::steps::{ProjArithOp, ScanProjCol, ScanProjCols, SCAN_PROJ_MAX_COLS};
    let steps = state.steps.as_slice();
    let done = steps.len().checked_sub(1)?;
    if !matches!(steps[done], Step::DoneReturn | Step::DoneNoReturn) {
        return None;
    }
    let mut i = 0usize;
    while i < done {
        let Some(src) = fetch_src(&steps[i]) else { break };
        if src != SlotSrc::Scan {
            return None;
        }
        i += 1;
    }
    let mut out = ScanProjCols { cols: [ScanProjCol::Var { attnum: 0 }; SCAN_PROJ_MAX_COLS], n: 0 };
    // One strict-2 arith call's (op, frame) when its fn is in the census set.
    let arith_call = |call: &crate::steps::FuncCall| -> Option<ProjArithOp> {
        // SAFETY: frame-owned mcx-boxed FmgrInfo, read-only here.
        let oid = unsafe { state.frames[call.frame as usize].flinfo.as_ref() }.fn_oid;
        ProjArithOp::for_fn_oid(oid)
    };
    while i < done {
        if out.n as usize == SCAN_PROJ_MAX_COLS {
            return None;
        }
        let idx = out.n as u16;
        let col = match &steps[i] {
            Step::AssignScanVar { attnum, resultnum } if *resultnum == idx => {
                i += 1;
                ScanProjCol::Var { attnum: *attnum }
            }
            Step::ScanVar { attnum: a, out: a_out, .. } => match steps.get(i + 1) {
                // Var op Var: both args evaluate into the call's arg slots.
                Some(Step::ScanVar { attnum: b, out: b_out, .. }) => {
                    if i + 3 >= done + 1 {
                        return None;
                    }
                    let Step::FuncExprStrict2 { call, out: fout } = &steps[i + 2] else {
                        return None;
                    };
                    let Step::AssignTmp { resultnum } = steps[i + 3] else {
                        return None;
                    };
                    if resultnum != idx || !state.is_result(*fout) {
                        return None;
                    }
                    let op = arith_call(call)?;
                    let frame = &state.frames[call.frame as usize];
                    if a_out.0 != frame.arg_slot(0) || b_out.0 != frame.arg_slot(1) {
                        return None;
                    }
                    i += 4;
                    ScanProjCol::ArithVV { op, a: *a, b: *b }
                }
                // Var op Const / Const op Var: the const arg was prefilled
                // at compile (const_args bit), only the Var evaluates.
                Some(Step::FuncExprStrict2 { call, out: fout }) => {
                    if i + 2 >= done + 1 {
                        return None;
                    }
                    let Step::AssignTmp { resultnum } = steps[i + 2] else {
                        return None;
                    };
                    if resultnum != idx || !state.is_result(*fout) {
                        return None;
                    }
                    let op = arith_call(call)?;
                    let frame = &state.frames[call.frame as usize];
                    let var_is_arg0 = a_out.0 == frame.arg_slot(0);
                    let const_argno = if var_is_arg0 { 1usize } else { 0 };
                    if a_out.0 != frame.arg_slot(if var_is_arg0 { 0 } else { 1 }) {
                        return None;
                    }
                    if frame.const_args & (1 << const_argno) == 0
                        || frame.const_null_args & (1 << const_argno) != 0
                    {
                        return None;
                    }
                    // SAFETY: const arg slot written at compile, never
                    // re-targeted (the qual census reads it the same way).
                    let konst = unsafe { frame.arg_slot(const_argno).read().value };
                    i += 3;
                    ScanProjCol::ArithVK { op, attnum: *a, konst, var_is_arg0 }
                }
                _ => return None,
            },
            _ => return None,
        };
        out.cols[out.n as usize] = col;
        out.n += 1;
    }
    (out.n >= 1).then_some(out)
}

/// Expression-group-key census (lane-v2 expr-key grouping): the projection
/// program as a FETCHSOME(scan) prefix followed by per-column units in
/// resultnum order — `AssignScanVar` (Var passthrough) for every column but
/// EXACTLY ONE, which is a chain of strict fmgr calls over one scan Var
/// (`ScanVar` -> FUNCEXPR_STRICT* -> `AssignTmp`), each call's other args
/// compile-time non-null Consts. Fail-closed like the censuses above; runs
/// on the PRISTINE program. Structural only — the consumer re-gates every
/// fn oid against pg_proc (IMMUTABLE / internal-language / strict).
fn select_scan_proj_expr_key(state: &ExprState<'_>) -> Option<crate::steps::ScanProjExprKey> {
    use crate::steps::{
        ProjKeyCall, ScanProjExprKey, PROJ_KEY_MAX_ARGS, PROJ_KEY_MAX_CALLS, SCAN_PROJ_MAX_COLS,
    };
    let steps = state.steps.as_slice();
    let done = steps.len().checked_sub(1)?;
    if !matches!(steps[done], Step::DoneReturn | Step::DoneNoReturn) {
        return None;
    }
    let mut i = 0usize;
    while i < done {
        let Some(src) = fetch_src(&steps[i]) else { break };
        if src != SlotSrc::Scan {
            return None;
        }
        i += 1;
    }
    let empty_call = ProjKeyCall {
        fn_oid: 0,
        collation: 0,
        var_argno: 0,
        nargs: 0,
        args: [::datum::NullableDatum::null(); PROJ_KEY_MAX_ARGS],
    };
    let mut out = ScanProjExprKey {
        cols: [None; SCAN_PROJ_MAX_COLS],
        n: 0,
        key_out: 0,
        input_col: 0,
        input_type: 0,
        ncalls: 0,
        calls: [empty_call; PROJ_KEY_MAX_CALLS],
    };
    let mut have_key = false;
    while i < done {
        if out.n as usize == SCAN_PROJ_MAX_COLS {
            return None;
        }
        let idx = out.n as u16;
        match &steps[i] {
            Step::AssignScanVar { attnum, resultnum } if *resultnum == idx => {
                out.cols[out.n as usize] = Some(*attnum);
                i += 1;
            }
            Step::ScanVar { attnum, vartype, out: vout } if !have_key => {
                out.input_col = *attnum;
                out.input_type = *vartype;
                out.key_out = idx;
                let mut prev_out = *vout;
                let mut ncalls = 0usize;
                loop {
                    match steps.get(i + 1 + ncalls).filter(|_| i + 1 + ncalls < done) {
                        Some(
                            Step::FuncExprStrict1 { call, out: fout }
                            | Step::FuncExprStrict2 { call, out: fout }
                            | Step::FuncExprStrict { call, out: fout },
                        ) => {
                            if ncalls == PROJ_KEY_MAX_CALLS {
                                return None;
                            }
                            let frame = &state.frames[call.frame as usize];
                            let nargs = frame.nargs as usize;
                            if nargs == 0 || nargs > PROJ_KEY_MAX_ARGS {
                                return None;
                            }
                            // Which arg does the inner value feed?
                            let mut var_argno = None;
                            for a in 0..nargs {
                                if prev_out.0 == frame.arg_slot(a) {
                                    var_argno = Some(a);
                                    break;
                                }
                            }
                            let Some(va) = var_argno else { return None };
                            // Every sibling must be a compile-time non-null
                            // Const, prefilled in its arg slot.
                            let mut args = [::datum::NullableDatum::null(); PROJ_KEY_MAX_ARGS];
                            for a in 0..nargs {
                                if a == va {
                                    continue;
                                }
                                if frame.const_args & (1 << a) == 0
                                    || frame.const_null_args & (1 << a) != 0
                                {
                                    return None;
                                }
                                // SAFETY: const arg slot was written at
                                // compile and never re-targeted.
                                args[a] = unsafe { frame.arg_slot(a).read() };
                            }
                            // SAFETY: frame-owned mcx-boxed FmgrInfo,
                            // read-only here.
                            let fn_oid = unsafe { frame.flinfo.as_ref() }.fn_oid;
                            out.calls[ncalls] = ProjKeyCall {
                                fn_oid,
                                collation: frame.collation(),
                                var_argno: va as u8,
                                nargs: nargs as u8,
                                args,
                            };
                            prev_out = *fout;
                            ncalls += 1;
                        }
                        // Varlena-typed tlist expressions assign via
                        // ASSIGN_TMP_MAKE_RO (C ExecBuildProjectionInfo's
                        // typlen == -1 arm); MakeReadOnly is identity on the
                        // flat varlena results the admitted internal-builtin
                        // chains produce (fmgr results, never expanded).
                        Some(
                            Step::AssignTmp { resultnum }
                            | Step::AssignTmpMakeRo { resultnum },
                        ) if ncalls > 0 => {
                            if *resultnum != idx || !state.is_result(prev_out) {
                                return None;
                            }
                            out.ncalls = ncalls as u8;
                            have_key = true;
                            i += 1 + ncalls + 1;
                            break;
                        }
                        _ => return None,
                    }
                }
                out.cols[out.n as usize] = None;
            }
            _ => return None,
        }
        out.n += 1;
    }
    have_key.then_some(out)
}

// ===========================================================================
// Lane qual shape extraction (the PgrcolumnarSource wiring tranche; harvested
// from the old lane-executor line). `lane_scan_qual` decodes a scan qual's
// compiled step stream into the structural clause vocabulary laneexec's
// fail-closed translate consumes (laneexec re-exports these types as its
// `shape` module). Structural checks only — the fn-oid whitelist that makes
// an accepted clause non-erroring/non-volatile/allocation-free is laneexec's.
// ===========================================================================

/// One implicitly-ANDed comparison clause of a scan qual. `col` is the Var
/// feeding arg0 (or, for const clauses with the Var at arg1, the sole Var
/// with `commuted` set). The comparator's fn oid is carried raw: the legality
/// gate (which oids are in-core non-erroring int comparators) lives in
/// laneexec, so its vocabulary can grow without touching the kernel CmpOp.
pub enum LaneCmpRhs {
    Const(::datum::Datum),
    Col(u16),
}

pub struct LaneCmpClause {
    pub col: u16,
    pub fn_oid: Oid,
    pub commuted: bool,
    /// The call's input collation (fcinfo fncollation) — collation-sensitive
    /// predicates (text eq/LIKE over dict lanes) re-evaluate with it.
    pub collation: Oid,
    pub rhs: LaneCmpRhs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneBoolTest {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
}

pub enum LaneClause {
    Cmp(LaneCmpClause),
    /// col IS [NOT] NULL — NullTest is non-strict, non-erroring, no fn call.
    NullTest { col: u16, want_null: bool },
    /// Bare boolean Var clause (`WHERE boolcol`): the Var writes the result
    /// slot and Qual tests it directly (NULL or false fails).
    BoolVar { col: u16 },
    /// col IS [NOT] TRUE/FALSE — BooleanTest is non-strict, non-erroring.
    BoolTest { col: u16, kind: LaneBoolTest },
    /// col <op> ANY(non-null Const array): useOr SAOP over a strict
    /// comparator, elements decoded at classify time (flat byval arrays
    /// only, structurally capped). NULL elements are kept: they flip a miss
    /// to NULL, which a Qual fails exactly like false, so laneexec may skip
    /// them — the shape stays exact for the census.
    InList { col: u16, fn_oid: Oid, elems: alloc::vec::Vec<::datum::NullableDatum> },
}

/// Trailing clauses the walker could not decode (the hybrid split's per-row
/// suffix). `Calls` carries every call fn oid found there so laneexec can
/// gate on volatility; `Opaque` = the suffix holds step kinds the collector
/// does not enumerate (treated as volatile downstream, fail-closed).
pub enum LaneSuffix {
    None,
    Calls(alloc::vec::Vec<Oid>),
    Opaque,
}

pub struct LaneQualShape {
    pub clauses: alloc::vec::Vec<LaneClause>,
    /// Parsed clauses' columns (laneexec recomputes over its whitelisted
    /// prefix; suffix columns deform lazily from the stored tuple on the
    /// per-row requal).
    pub max_attnum: u16,
    pub suffix: LaneSuffix,
}

/// Fail-closed extraction of a scan qual's compiled step stream into lane
/// clauses. Per clause: [ScanVar (x1|x2) + strict 2-arg comparator call (any
/// post-fusion spelling) + Qual] or [ScanVar + NullTestIs(Not)Null + Qual],
/// each Qual jumping to the shared DoneReturn — structural checks only; the
/// fn-oid whitelist that makes an accepted clause non-erroring/non-volatile/
/// allocation-free is laneexec's. The first undecodable clause ends the
/// prefix: everything from its clause start to DoneReturn is returned as the
/// suffix (sound as a per-row requal target because prefix clauses are a
/// strict prefix of the implicit-AND list). No decodable leading clause =
/// hard refusal.
pub fn lane_scan_qual(state: &ExprState<'_>) -> Result<LaneQualShape, &'static str> {
    if !state.is_qual() {
        return Err("not a qual program");
    }
    if state.has_subplan() {
        return Err("subplan");
    }
    if !state.param_exec_deps().is_empty() {
        return Err("exec params");
    }
    let steps = state.steps.as_slice();
    let done = steps.len().checked_sub(1).ok_or("empty program")?;
    if !matches!(steps[done], Step::DoneReturn) {
        return Err("no DoneReturn tail");
    }
    let mut ix = 0usize;
    while ix < done && fetch_src(&steps[ix]).is_some() {
        if fetch_src(&steps[ix]) != Some(SlotSrc::Scan) {
            return Err("non-scan fetch");
        }
        ix += 1;
    }
    let mut clauses = alloc::vec::Vec::new();
    let mut max_attnum = 0u16;
    let mut suffix = LaneSuffix::None;
    while ix < done {
        match parse_lane_clause(state, steps, done, ix) {
            Ok((clause, clause_max, next_ix)) => {
                max_attnum = max_attnum.max(clause_max);
                clauses.push(clause);
                ix = next_ix;
            }
            Err(reason) => {
                if clauses.is_empty() {
                    return Err(reason);
                }
                suffix = collect_suffix_calls(state, &steps[ix..done]);
                break;
            }
        }
    }
    if clauses.is_empty() {
        return Err("no clauses");
    }
    Ok(LaneQualShape { clauses, max_attnum, suffix })
}

/// One clause starting at `ix`; Ok returns (clause, clause max attnum, index
/// past the clause's Qual terminator).
fn parse_lane_clause(
    state: &ExprState<'_>,
    steps: &[Step],
    done: usize,
    mut ix: usize,
) -> Result<(LaneClause, u16, usize), &'static str> {
    // Up to two leading plain Var loads (unfused clause form, plus the
    // first operand of a fused var-vs-var clause).
    let mut plain: [Option<(u16, OutRef)>; 2] = [None, None];
    let mut nplain = 0usize;
    while nplain < 2 {
        match steps.get(ix).filter(|_| ix < done).and_then(var_src) {
            Some((SlotSrc::Scan, a, out)) => {
                plain[nplain] = Some((a, out));
                nplain += 1;
                ix += 1;
            }
            Some(_) => return Err("non-scan Var"),
            None => break,
        }
    }
    // NullTest clause: never fused (no try_fuse pattern touches NullTest),
    // reads and rewrites the Var's out slot in place.
    if let Some(Step::NullTestIsNull { out } | Step::NullTestIsNotNull { out }) =
        steps.get(ix).filter(|_| ix < done)
    {
        let want_null = matches!(steps[ix], Step::NullTestIsNull { .. });
        let Some((a, vout)) = plain[0] else {
            return Err("null test without a Var");
        };
        if nplain != 1 || vout.0 != out.0 || !state.is_result(*out) {
            return Err("null test does not consume the Var");
        }
        ix += 1;
        let Some(Step::Qual { jumpdone }) = steps.get(ix).filter(|_| ix < done) else {
            return Err("clause does not end in Qual");
        };
        if *jumpdone as usize != done {
            return Err("Qual jump is not the shared done");
        }
        return Ok((LaneClause::NullTest { col: a, want_null }, a, ix + 1));
    }
    // BooleanTest clause: same in-place read/rewrite shape as NullTest.
    if let Some(
        Step::BoolTestIsTrue { out }
        | Step::BoolTestIsNotTrue { out }
        | Step::BoolTestIsFalse { out }
        | Step::BoolTestIsNotFalse { out },
    ) = steps.get(ix).filter(|_| ix < done)
    {
        let kind = match steps[ix] {
            Step::BoolTestIsTrue { .. } => LaneBoolTest::IsTrue,
            Step::BoolTestIsNotTrue { .. } => LaneBoolTest::IsNotTrue,
            Step::BoolTestIsFalse { .. } => LaneBoolTest::IsFalse,
            _ => LaneBoolTest::IsNotFalse,
        };
        let Some((a, vout)) = plain[0] else {
            return Err("bool test without a Var");
        };
        if nplain != 1 || vout.0 != out.0 || !state.is_result(*out) {
            return Err("bool test does not consume the Var");
        }
        ix += 1;
        let Some(Step::Qual { jumpdone }) = steps.get(ix).filter(|_| ix < done) else {
            return Err("clause does not end in Qual");
        };
        if *jumpdone as usize != done {
            return Err("Qual jump is not the shared done");
        }
        return Ok((LaneClause::BoolTest { col: a, kind }, a, ix + 1));
    }
    // Bare boolean Var clause: the Var wrote the result slot; Qual tests it.
    if let Some(Step::Qual { jumpdone }) = steps.get(ix).filter(|_| ix < done) {
        let Some((a, vout)) = plain[0] else {
            return Err("clause Qual without a body");
        };
        if nplain != 1 || !state.is_result(vout) {
            return Err("bare Var does not write the qual result");
        }
        if *jumpdone as usize != done {
            return Err("Qual jump is not the shared done");
        }
        return Ok((LaneClause::BoolVar { col: a }, a, ix + 1));
    }
    // SAOP clause over a non-null Const array: [ScanVar -> arg0; Const ->
    // out; ScalarArrayOp; Qual]. Fail-closed conditions mirror laneexec's
    // eval fast assumptions: OR semantics, strict comparator (NULL scalar ->
    // NULL -> row fails on both drives), flat uncompressed 4B varlena image,
    // byval fixed-width elements, small list.
    if let Some(Step::Const { value, isnull, out: c_out }) = steps.get(ix).filter(|_| ix < done) {
        let Some(Step::ScalarArrayOp { call, use_or, strict, typlen, typbyval, typalign, out }) =
            steps.get(ix + 1).filter(|_| ix + 1 < done)
        else {
            return Err("Const does not feed a SAOP");
        };
        if !*use_or || !*strict {
            return Err("SAOP is not a strict OR form");
        }
        if *isnull {
            return Err("NULL SAOP array");
        }
        if c_out.0 != out.0 || !state.is_result(*out) {
            return Err("SAOP does not write the qual result");
        }
        if !*typbyval || !matches!(*typlen, 1 | 2 | 4 | 8) {
            return Err("SAOP element type is not fixed-width byval");
        }
        let frame = &state.frames[call.frame as usize];
        if frame.nargs != 2 {
            return Err("SAOP comparator is not 2-arg");
        }
        let Some((a, vout)) = plain[0] else {
            return Err("SAOP without a Var scalar");
        };
        if nplain != 1 || vout.0 != frame.arg_slot(0) {
            return Err("SAOP scalar is not the Var");
        }
        // SAFETY: frame-owned mcx-boxed FmgrInfo, read-only here.
        let fn_oid = unsafe { frame.flinfo.as_ref() }.fn_oid;
        let elems = decode_saop_const_array(*value, *typlen, *typalign)?;
        ix += 2;
        let Some(Step::Qual { jumpdone }) = steps.get(ix).filter(|_| ix < done) else {
            return Err("clause does not end in Qual");
        };
        if *jumpdone as usize != done {
            return Err("Qual jump is not the shared done");
        }
        return Ok((LaneClause::InList { col: a, fn_oid, elems }, a, ix + 1));
    }
    // The comparator call in any post-fusion spelling: fuse_program runs
    // on every Kernel::Program qual, so the fused Thin forms are the
    // common case; the unfused forms remain for jit-armed programs,
    // which skip fusion.
    let (fcinfo, out, fused_var, fused_jump) = match steps.get(ix).filter(|_| ix < done) {
        Some(Step::FuncExprStrict2 { call, out }) => {
            (state.frames[call.frame as usize].fcinfo, *out, None, None)
        }
        Some(Step::FuncExprStrict2Thin { call, out }) => (call.fcinfo, *out, None, None),
        Some(Step::ScanVarFuncStrict2 { attnum, argno, call, out, .. }) => {
            (call.fcinfo, *out, Some((*attnum, *argno)), None)
        }
        Some(Step::ScanVarFuncStrict2Thin { attnum, argno, call, out, .. }) => {
            (call.fcinfo, *out, Some((*attnum, *argno)), None)
        }
        Some(Step::FuncStrict2Qual { call, jumpdone, out }) => {
            (call.fcinfo, *out, None, Some(*jumpdone))
        }
        Some(Step::FuncStrict2QualThin { call, jumpdone, out }) => {
            (call.fcinfo, *out, None, Some(*jumpdone))
        }
        _ => return Err("comparator is not a strict 2-arg fn"),
    };
    ix += 1;
    if !state.is_result(out) {
        return Err("comparator does not write the qual result");
    }
    let frame = state
        .frames
        .iter()
        .find(|fr| fr.fcinfo == fcinfo)
        .ok_or("comparator call has no frame")?;
    if frame.nargs != 2 {
        return Err("comparator is not a strict 2-arg fn");
    }
    // SAFETY: frame-owned mcx-boxed FmgrInfo, read-only here.
    let fn_oid = unsafe { frame.flinfo.as_ref() }.fn_oid;
    let collation = frame.collation();
    // Resolve which arg each Var feeds: fused vars carry their argno,
    // plain vars are matched by out-slot address.
    let mut arg_var: [Option<u16>; 2] = [None, None];
    if let Some((a, argno)) = fused_var {
        if argno > 1 {
            return Err("fused Var argno out of range");
        }
        arg_var[argno as usize] = Some(a);
    }
    for &(a, vout) in plain.iter().flatten() {
        let argno = if vout.0 == frame.arg_slot(0) {
            0usize
        } else if vout.0 == frame.arg_slot(1) {
            1
        } else {
            return Err("Var arg does not feed the comparator");
        };
        if arg_var[argno].is_some() {
            return Err("two Vars feed one comparator arg");
        }
        arg_var[argno] = Some(a);
    }
    let (clause, clause_max) = match arg_var {
        [Some(l), Some(r)] => (
            LaneCmpClause { col: l, fn_oid, commuted: false, collation, rhs: LaneCmpRhs::Col(r) },
            l.max(r),
        ),
        [var0, var1] => {
            let (a, var_is_arg0) = match (var0, var1) {
                (Some(a), None) => (a, true),
                (None, Some(a)) => (a, false),
                _ => return Err("comparator has no Var operand"),
            };
            let const_argno = if var_is_arg0 { 1usize } else { 0 };
            if frame.const_args & (1 << const_argno) == 0 {
                return Err("comparator operand is not a Const");
            }
            if frame.const_null_args & (1 << const_argno) != 0 {
                return Err("NULL Const comparison");
            }
            // SAFETY: const arg slot was written at compile and never
            // re-targeted.
            let konst = unsafe { frame.arg_slot(const_argno).read().value };
            (
                LaneCmpClause {
                    col: a,
                    fn_oid,
                    commuted: !var_is_arg0,
                    collation,
                    rhs: LaneCmpRhs::Const(konst),
                },
                a,
            )
        }
    };
    let jumpdone = match fused_jump {
        Some(j) => j,
        None => {
            let Some(Step::Qual { jumpdone }) = steps.get(ix).filter(|_| ix < done) else {
                return Err("clause does not end in Qual");
            };
            ix += 1;
            *jumpdone
        }
    };
    if jumpdone as usize != done {
        return Err("Qual jump is not the shared done");
    }
    Ok((LaneClause::Cmp(clause), clause_max, ix))
}

// Structural cap on decoded IN-list length (laneexec evaluates elements per
// row per code; a long list belongs to the hashed SAOP path anyway).
const LANE_INLIST_MAX: i64 = 16;

fn decode_saop_const_array(
    value: ::datum::Datum,
    typlen: i16,
    typalign: u8,
) -> Result<alloc::vec::Vec<::datum::NullableDatum>, &'static str> {
    let p = value.as_usize() as *const u8;
    // SAFETY: non-null Const array datum addresses a varlena that lives as
    // long as the plan (and therefore any lane program built from it).
    let img: &[u8] = unsafe {
        if !::types_tuple::varatt::varatt_is_4b_u(p) {
            return Err("SAOP array is not a flat 4B varlena");
        }
        core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
    };
    let (ndim, dims, _lbs) = ::arrayfuncs::foundation::read_dims_lbounds(img);
    let mut nitems = 1i64;
    for d in &dims[..ndim as usize] {
        nitems *= *d as i64;
    }
    if ndim == 0 {
        nitems = 0;
    }
    if !(0..=LANE_INLIST_MAX).contains(&nitems) {
        return Err("SAOP array too long for a lane clause");
    }
    let bitmap_off = ::arrayfuncs::foundation::arr_nullbitmap_off(img);
    let mut off = ::arrayfuncs::foundation::arr_data_offset(img);
    let mut bitmask: u32 = 1;
    let mut bitmap_byte = 0usize;
    let mut elems = alloc::vec::Vec::with_capacity(nitems as usize);
    for _ in 0..nitems {
        let elt_null = match bitmap_off {
            Some(bo) => (img[bo + bitmap_byte] as u32 & bitmask) == 0,
            None => false,
        };
        if elt_null {
            elems.push(::datum::NullableDatum::null());
        } else {
            off = ::arrayfuncs::foundation::att_align_nominal(off, typalign);
            // SAFETY: off stays within the VARSIZE image per the array
            // layout; tupmacs::fetch_att (NOT the arrayfuncs one, which
            // zero-extends) keeps the canonical sign-extension the lane
            // engine's wide compares rely on.
            let elt = unsafe {
                ::types_tuple::tupmacs::fetch_att(img.as_ptr().add(off), true, typlen as i32)
            };
            off = ::arrayfuncs::foundation::att_addlength_pointer(off, typlen as i32, unsafe {
                img.as_ptr().add(off)
            });
            elems.push(::datum::NullableDatum { value: elt, isnull: false });
        }
        if bitmap_off.is_some() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmask = 1;
                bitmap_byte += 1;
            }
        }
    }
    Ok(elems)
}

/// Every call fn oid in the undecoded suffix, for laneexec's volatility
/// gate. Enumerated allowlist (no side-effect-free wildcard): any step kind
/// outside it makes the suffix Opaque. The suffix is never interpreted —
/// the per-row requal runs the ORIGINAL ExprState — so this walk only needs
/// to find calls, not understand dataflow.
fn collect_suffix_calls(state: &ExprState<'_>, steps: &[Step]) -> LaneSuffix {
    let mut oids = alloc::vec::Vec::new();
    // SAFETY (all arms): frame-owned mcx-boxed FmgrInfo, read-only here.
    let mut push_flinfo =
        |oids: &mut alloc::vec::Vec<Oid>, fl: NonNull<FmgrInfo>| oids.push(unsafe { fl.as_ref() }.fn_oid);
    for s in steps {
        match s {
            Step::ScanVar { .. }
            | Step::InnerVar { .. }
            | Step::OuterVar { .. }
            | Step::Const { .. }
            | Step::Qual { .. }
            | Step::Jump { .. }
            | Step::JumpIfNotTrue { .. }
            | Step::JumpIfNotNull { .. }
            | Step::JumpIfNull { .. }
            | Step::BoolAndStepFirst { .. }
            | Step::BoolAndStep { .. }
            | Step::BoolAndStepLast { .. }
            | Step::BoolOrStepFirst { .. }
            | Step::BoolOrStep { .. }
            | Step::BoolOrStepLast { .. }
            | Step::BoolNotStep { .. }
            | Step::NullTestIsNull { .. }
            | Step::NullTestIsNotNull { .. }
            | Step::BoolTestIsTrue { .. }
            | Step::BoolTestIsNotTrue { .. }
            | Step::BoolTestIsFalse { .. }
            | Step::BoolTestIsNotFalse { .. }
            | Step::ScanFetchSome { .. }
            | Step::InnerFetchSome { .. }
            | Step::OuterFetchSome { .. } => {}
            Step::FuncExpr { call, .. }
            | Step::FuncExprStrict { call, .. }
            | Step::FuncExprStrict1 { call, .. }
            | Step::FuncExprStrict2 { call, .. }
            | Step::FuncExprStrictFusage { call, .. }
            | Step::Distinct { call, .. }
            | Step::NullIf { call, .. }
            | Step::NotDistinct { call, .. } => push_flinfo(&mut oids, call.flinfo),
            Step::ScanVarFuncStrict2 { call, .. }
            | Step::FuncStrict2Qual { call, .. }
            | Step::NotDistinctQual { call, .. }
            | Step::OuterVarNotDistinct { call, .. } => push_flinfo(&mut oids, call.flinfo),
            Step::FuncFuncStrict2 { call1, call2, .. } => {
                push_flinfo(&mut oids, call1.flinfo);
                push_flinfo(&mut oids, call2.flinfo);
            }
            Step::FuncExprStrict1Thin { call, .. }
            | Step::FuncExprStrict2Thin { call, .. }
            | Step::ScanVarFuncStrict2Thin { call, .. }
            | Step::FuncStrict2QualThin { call, .. }
            | Step::NotDistinctQualThin { call, .. }
            | Step::OuterVarNotDistinctThin { call, .. } => {
                let Some(fr) = state.frames.iter().find(|fr| fr.fcinfo == call.fcinfo) else {
                    return LaneSuffix::Opaque;
                };
                push_flinfo(&mut oids, fr.flinfo);
            }
            Step::FuncFuncStrict2Thin { call1, call2, .. } => {
                for c in [call1, call2] {
                    let Some(fr) = state.frames.iter().find(|fr| fr.fcinfo == c.fcinfo) else {
                        return LaneSuffix::Opaque;
                    };
                    push_flinfo(&mut oids, fr.flinfo);
                }
            }
            // SAOP: the element-comparison fn decides volatility; the array
            // operand's own evaluation steps are walked independently. The
            // hashed form additionally surfaces its element hash fn (in-core
            // hash fns are immutable, but the per-oid check decides — no
            // step-level exemption).
            Step::ScalarArrayOp { call, .. } => push_flinfo(&mut oids, call.flinfo),
            Step::HashedScalarArrayOp { call, table, .. } => {
                push_flinfo(&mut oids, call.flinfo);
                let Some(t) = state.saop_tables.get(*table as usize) else {
                    return LaneSuffix::Opaque;
                };
                push_flinfo(&mut oids, t.hashcall.flinfo);
            }
            _ => return LaneSuffix::Opaque,
        }
    }
    LaneSuffix::Calls(oids)
}
