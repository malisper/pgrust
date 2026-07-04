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
    pub init: for<'x> unsafe fn(NonNull<()>, Node<'x>) -> PgResult<NonNull<()>>,
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
    let Some(node) = node else {
        return Ok(None);
    };
    let mut state = ExprState::new_boxed_in(mcx)?;
    create_expr_setup_steps(&mut state, mcx, &[node])?;
    let rout = state.result_out();
    init_expr_rec(node, &mut state, mcx, rout, None, params, sub)?;
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
    create_expr_setup_steps(&mut state, mcx, qual.as_slice())?;

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
    if qual.is_nil() {
        return Ok(None);
    }
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;
    create_expr_setup_steps(&mut state, mcx, qual.as_slice())?;

    for node in qual.iter() {
        let rout = state.result_out();
        init_expr_rec(node, &mut state, mcx, rout, Some(Bind::Agg(agg)), params, None)?;
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

fn build_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
    sub: Option<SubplanCompileEnv>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mut state = ExprState::new_boxed_in(mcx)?;
    create_expr_setup_steps(&mut state, mcx, target_list.as_slice())?;

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
                    _ => unported("EEOP_ASSIGN_OLD_VAR/EEOP_ASSIGN_NEW_VAR (RETURNING)"),
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
    build_agg_trans(mcx, specs, PergroupMode::Fixed, agg_node, params)
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
    build_agg_trans(mcx, specs, PergroupMode::Sets(set_bases), agg_node, params)
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
    build_agg_trans(mcx, specs, PergroupMode::Mixed(set_bases, cells), agg_node, params)
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
    build_agg_trans(mcx, specs, PergroupMode::Indirect(base), agg_node, params)
}

// The tag proves the FmNodePtr is an AggStateNode (WindowAgg passes None).
fn agg_state_node(agg_node: FmNodePtr) -> NonNull<::types_fmgr::AggStateNode> {
    let p = agg_node
        .unwrap_or_else(|| unported("by-ref transtype without an AggState (nodeWindowAgg lane)"));
    // SAFETY: build-time read of the caller's live node header.
    assert!(
        unsafe { p.as_ref().tag } == ::types_fmgr::T_AGG_STATE,
        "build_agg_trans: by-ref trans context is not an AggStateNode"
    );
    p.cast()
}

fn build_agg_trans<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
    mode: PergroupMode<'_>,
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mut state = ExprState::new_boxed_in(mcx)?;
    let mut info = SetupInfo::default();
    for spec in specs {
        for tle in spec.args.iter() {
            setup_walker(tle, &mut info);
        }
        if let Some(f) = spec.aggfilter {
            setup_walker(f, &mut info);
        }
    }
    push_fetch_steps(&mut state, mcx, &info)?;

    for (transno, spec) in specs.iter().enumerate() {
        let num_trans_inputs = spec.args.len();
        let nargs = num_trans_inputs + 1;
        if nargs > FUNC_MAX_ARGS {
            return Err(too_many_args(nargs));
        }
        let mut flinfo = fmgr_core::fmgr_info(spec.transfn_oid)?;
        // SAFETY: arg_types is arena-backed for the query (leaked into
        // es_query_cxt by the caller) and this flinfo dies with the plan it
        // serves — from_node_ref's contract; the carrier stays drop-free.
        let argtypes: &'static [Oid] = unsafe { core::mem::transmute(spec.arg_types) };
        let agg_argtypes = ::mcx::alloc_leak_in(mcx, ::types_core::fmgr::AggFnArgTypes(argtypes))?;
        // SAFETY: agg_argtypes is arena-backed for the query, see above.
        flinfo.fn_expr = Some(unsafe { FnExprErased::from_node_ref(agg_argtypes) });
        if flinfo.fn_retset {
            return Err(retset_error());
        }
        let init_strict = flinfo.fn_strict && spec.init_value_is_null;
        let fn_strict = flinfo.fn_strict;
        if let Some(ord) = spec.ordered {
            if spec.aggfilter.is_some() {
                panic!(
                    "ExecBuildAggTrans (execExpr.c): FILTER over non-presorted \
                     DISTINCT/ORDER BY aggregate not ported"
                );
            }
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
        if let Some(f) = spec.aggfilter {
            let rout = state.result_out();
            init_expr_rec(f, &mut state, mcx, rout, None, params, None)?;
            filter_jump = Some(state.steps.len());
            push_step(
                &mut state,
                mcx,
                Step::JumpIfNotTrue { jumpdone: u32::MAX, out: rout },
            )?;
        }
        for (argno, tle_node) in spec.args.iter().enumerate() {
            let tle = tle_node.as_target_entry().unwrap_or_else(|| {
                panic!("Aggref.args cell: expected TargetEntry, got {:?}", tle_node.node_tag())
            });
            if tle.resjunk {
                continue;
            }
            // SAFETY: argno + 1 <= num_trans_inputs < nargs of `call.fcinfo`.
            let arg_out =
                OutRef(unsafe { crate::steps::arg_slot_of(call.fcinfo, argno + 1) });
            init_expr_rec(tle.expr, &mut state, mcx, arg_out, None, params, None)?;
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
                    agg: agg_state_node(agg_node),
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
                    agg: agg_state_node(agg_node),
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
    if let Some(ix) = bailout {
        let target = state.steps.len() as u32;
        match &mut state.steps[ix] {
            Step::AggStrictInputCheck { jumpnull, .. }
            | Step::AggStrictInputCheck1 { jumpnull, .. } => *jumpnull = target,
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
    debug_assert!(!key_col_idx.is_empty());
    debug_assert!(eqfuncoids.len() == key_col_idx.len() && collations.len() == key_col_idx.len());
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;

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
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wintype,
        NodeTag::T_GroupingFunc => 23,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxtype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().r#type,
        NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_BooleanTest
        | NodeTag::T_DistinctExpr => 16,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().refrestype,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
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
                SubLinkType::MULTIEXPR_SUBLINK => {
                    panic!("exprType (nodeFuncs.c): MULTIEXPR SubPlan not ported")
                }
                _ => 16,
            }
        }
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeId,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
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
        tag => panic!("execexpr exprType: node family {tag:?} not ported"),
    }
}

// C ExprSetupInfo + expr_setup_walker + ExecPushExprSetupSteps. Slots are not
// knowable here (no PlanState parent), so every referenced slot gets a
// non-fixed FETCHSOME step, C's parent == NULL shape.
#[derive(Default)]
struct SetupInfo {
    last_inner: i16,
    last_outer: i16,
    last_scan: i16,
}

#[inline]
pub(crate) fn create_expr_setup_steps<'mcx>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    nodes: &[Node<'mcx>],
) -> PgResult<()> {
    let mut info = SetupInfo::default();
    for &n in nodes {
        setup_walker(n, &mut info);
    }
    push_fetch_steps(state, mcx, &info)
}

#[inline]
fn push_fetch_steps<'mcx>(
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    info: &SetupInfo,
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
    Ok(())
}

fn setup_walker(node: Node<'_>, info: &mut SetupInfo) {
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
                    _ => unported("OLD/NEW FETCHSOME (RETURNING)"),
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
        NodeTag::T_RelabelType => setup_walker(node.as_relabel_type().unwrap().arg, info),
        NodeTag::T_CoerceViaIO => setup_walker(node.as_coerce_via_io().unwrap().arg, info),
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
        NodeTag::T_CoerceToDomain => setup_walker(node.as_coerce_to_domain().unwrap().arg, info),
        NodeTag::T_CoerceToDomainValue => {}
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

// C ExecInitExprRec whole-row Var + ExecEvalWholeRowVar's first-eval split:
// the composite tupdesc resolves here (plan-stable typcache row; C defers to
// first eval only to reach the slot). RECORD legs (subquery/join whole-row,
// junk filtering) and RETURNING OLD/NEW stay loud.
fn init_whole_row<'mcx>(
    variable: &::types_nodes::primnodes::Var<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
) -> PgResult<()> {
    use crate::steps::{SlotSrc, WholeRowState};
    if variable.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT {
        unported("EEOP_WHOLEROW OLD/NEW (RETURNING)");
    }
    if variable.vartype == ::types_core::catalog::RECORDOID {
        unported("EEOP_WHOLEROW RECORD leg (subquery/CTE whole-row + junkfilter)");
    }
    let src = match variable.varno {
        INNER_VAR => SlotSrc::Inner,
        OUTER_VAR => SlotSrc::Outer,
        _ => SlotSrc::Scan,
    };
    let desc = typcache::lookup_rowtype_tupdesc_copy(mcx, variable.vartype, -1)?;
    let desc_layout = core::alloc::Layout::new::<::types_tuple::TupleDescData<'static>>();
    let desc_ptr: NonNull<::types_tuple::TupleDescData<'static>> =
        mcx.allocate(desc_layout).map_err(|_| mcx.oom(desc_layout.size()))?.cast();
    // SAFETY: fresh exact-layout allocation; the plan mcx outlives every eval
    // of this step, so the 'static restamp never escapes it.
    unsafe {
        desc_ptr.as_ptr().write(core::mem::transmute::<
            ::types_tuple::TupleDescData<'mcx>,
            ::types_tuple::TupleDescData<'static>,
        >(desc));
    }
    let wr_layout = core::alloc::Layout::new::<WholeRowState>();
    let wr: NonNull<WholeRowState> =
        mcx.allocate(wr_layout).map_err(|_| mcx.oom(wr_layout.size()))?.cast();
    // SAFETY: fresh exact-layout allocation.
    unsafe { wr.as_ptr().write(WholeRowState { tupdesc: desc_ptr, first: true, slow: false }) };

    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    push_step(state, mcx, Step::WholeRow { src, wr, frame: frame_ix, out })
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
                return init_whole_row(variable, state, mcx, out);
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
                        _ => unported("EEOP_OLD_SYSVAR/EEOP_NEW_SYSVAR (RETURNING)"),
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
                    _ => unported("EEOP_OLD_VAR/EEOP_NEW_VAR (RETURNING)"),
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
        NodeTag::T_BoolExpr => init_bool_expr(node, state, mcx, out, agg, params, sub),
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let Some(env) = sub else {
                panic!(
                    "ExecInitSubPlanExpr (execExpr.c): SubPlan in an expression context \
                     without a subplan driver (owning node not wired)"
                )
            };
            assert!(
                sp.subLinkType != ::types_nodes::primnodes::SubLinkType::MULTIEXPR_SUBLINK,
                "ExecInitExprRec (execExpr.c): MULTIEXPR SubPlan not ported"
            );
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
            // SAFETY: env.estate is the caller's live estate (SubplanCompileEnv
            // contract: no aliasing borrows during compile).
            let sstate = unsafe { (env.init)(env.estate, node) }?;
            state.flags |= crate::steps::EEO_FLAG_HAS_SUBPLAN;
            push_step(state, mcx, Step::SubPlan { sstate, out })
        }
        NodeTag::T_BoolExpr => init_bool_expr(node, state, mcx, out, agg, params, sub),
        NodeTag::T_CaseExpr => init_case_expr(node, state, mcx, out, agg, params, sub),
        NodeTag::T_CaseTestExpr => match state.innermost_case {
            Some(slot) => push_step(state, mcx, Step::CaseTestVal { slot, out }),
            None => unported(
                "EEOP_CASE_TESTVAL_EXT (externally supplied econtext caseValue — \
                 domain checks / ArrayCoerceExpr)",
            ),
        },
        NodeTag::T_NullTest => {
            use ::types_nodes::primnodes::NullTestType;
            let nt = node.as_null_test().unwrap();
            if nt.argisrow {
                unported("EEOP_NULLTEST_ROWISNULL/ROWISNOTNULL");
            }
            init_expr_rec(nt.arg.expect("NullTest.arg"), state, mcx, out, agg, params, sub)?;
            let step = match nt.nulltesttype {
                NullTestType::IS_NULL => Step::NullTestIsNull { out },
                NullTestType::IS_NOT_NULL => Step::NullTestIsNotNull { out },
            };
            push_step(state, mcx, step)
        }
        NodeTag::T_RelabelType => {
            init_expr_rec(node.as_relabel_type().unwrap().arg, state, mcx, out, agg, params, sub)
        }
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
        NodeTag::T_CoerceViaIO => init_coerce_via_io(node, state, mcx, out, agg, params, sub),
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
        NodeTag::T_JsonExpr => panic!(
            "execexpr ExecInitJsonExpr: JSON_EXISTS/JSON_QUERY/JSON_VALUE execution \
             blocked on the jsonpath lane — interlock: adt_jsonpath jsonpath_exec \
             (JsonPathExists/JsonPathQuery/JsonPathValue + GetJsonPathVar) and \
             json_populate_type (jsonfuncs.c) must land, then ExecInitJsonExpr \
             (execExpr.c:4750) lands here"
        ),
        NodeTag::T_CoerceToDomain => init_coerce_to_domain(node, state, mcx, out, agg, params, sub),
        NodeTag::T_CoerceToDomainValue => match state.innermost_domain {
            Some(src) => push_step(state, mcx, Step::DomainTestval { src, out }),
            None => unported(
                "EEOP_DOMAIN_TESTVAL_EXT (CoerceToDomainValue outside a domain-check compile)",
            ),
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
        unported("EEOP_ARRAYEXPR multidimensional leg");
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
        nelems: nelems as u16,
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

// C ExecInitSubscriptingRef over the closed array handler (arraysubs.c
// array_exec_setup inlined: fetch_strict = true).
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
        if is_slice {
            unported("EEOP_SBSREF_ASSIGN slice (array_set_slice lane)");
        }
        let assgn = sbsref.refassgnexpr.unwrap();
        if assgn_needs_old(assgn) {
            unported("EEOP_SBSREF_OLD (nested-assignment CaseTestExpr passing)");
        }
        let replace_slot = unsafe {
            NonNull::new_unchecked(core::ptr::addr_of_mut!((*stp.as_ptr()).replace))
        };
        init_expr_rec(assgn, state, mcx, OutRef(replace_slot), agg, params, sub)?;
        push_step(state, mcx, Step::SbsrefAssign { state: stp, out })?;
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

// isAssignmentIndirectionExpr: does the replacement value reference the old
// element (CaseTestExpr under FieldStore/SubscriptingRef)?
fn assgn_needs_old(expr: Node<'_>) -> bool {
    match expr.node_tag() {
        NodeTag::T_SubscriptingRef => {
            let sr = expr.as_subscripting_ref().unwrap();
            sr.refexpr.is_some_and(|e| e.node_tag() == NodeTag::T_CaseTestExpr)
        }
        NodeTag::T_RelabelType => assgn_needs_old(expr.as_relabel_type().unwrap().arg),
        _ => false,
    }
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

// C ExecInitExprRec T_RowExpr, anonymous-RECORD leg (named-rowtype casts are
// unported loud); the blessed tupdesc is built once at compile.
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
    if r.row_typeid != ::types_core::catalog::RECORDOID {
        unported("EEOP_ROWEXPR named-rowtype leg");
    }
    let nelems = r.args.len();
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

    let layout = core::alloc::Layout::array::<::datum::NullableDatum>(nelems.max(1))
        .expect("elem scratch layout");
    let elems: NonNull<::datum::NullableDatum> =
        mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();

    // An argless frame whose armed fcinfo supplies the per-eval result mcx.
    let frame_ix = state.frames.len() as u32;
    let frame = FuncFrame::new_in(mcx, FmgrInfo::unresolved(), 0, 0)?;
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    for (i, e) in r.args.iter().enumerate() {
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

    Ok(Step::RowExprStep { elems, nelems: nelems as u16, frame: frame_ix, desc: desc_ptr, out })
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
                let slot: NonNull<::adt_jsonb::tojsonb::ValCategory> = alloc_array(mcx, 1)?;
                // SAFETY: fresh exclusive allocation.
                unsafe { slot.as_ptr().write(cat) };
                (None, Some(slot))
            } else {
                let cat = ::adt_json::tojson::json_categorize_type(typid)?;
                let slot: NonNull<::adt_json::tojson::TypeCat> = alloc_array(mcx, 1)?;
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

fn alloc_array<'mcx, T>(mcx: Mcx<'mcx>, n: usize) -> PgResult<NonNull<T>> {
    let layout = core::alloc::Layout::array::<T>(n).expect("scratch layout");
    Ok(mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast())
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
                push_step(state, mcx, Step::DomainNotNull { resulttype: cd.resulttype, out })?;
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
        nelems: nelems as u16,
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
                return Err(no_param_value(paramid));
            }
            let prm = &list[(paramid - 1) as usize];
            if prm.ptype == 0 {
                return Err(no_param_value(paramid));
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
fn no_param_value(paramid: i32) -> Box<PgError> {
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
    state.frames.push(frame_in);

    let calls = crate::steps::IoCoerceCalls { outcall, incall, in_strict };
    let raw = mcx
        .allocate(core::alloc::Layout::new::<crate::steps::IoCoerceCalls>())
        .map_err(|_| mcx.oom(core::mem::size_of::<crate::steps::IoCoerceCalls>()))?;
    let p: NonNull<crate::steps::IoCoerceCalls> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(calls) };
    push_step(state, mcx, Step::IoCoerce { calls: p, out })
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
    // opcodes; builtins carry TRACK_FUNC_ALL (the enum maximum), so the GUC
    // read is only reachable for the unported PL leg.
    let track = if fn_stats >= TRACK_FUNC_ALL {
        TRACK_FUNC_OFF as i32
    } else {
        guc_tables::vars::pgstat_track_functions.read()
    };
    if track <= fn_stats as i32 {
        Ok(if fn_strict && nargs > 0 {
            match nargs {
                1 => Step::FuncExprStrict1 { call, out },
                2 => Step::FuncExprStrict2 { call, out },
                _ => Step::FuncExprStrict { call, out },
            }
        } else {
            Step::FuncExpr { call, out }
        })
    } else {
        unported("EEOP_FUNCEXPR_FUSAGE/EEOP_FUNCEXPR_STRICT_FUSAGE (pgstat function usage)")
    }
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
            | Step::AggStrictInputCheck1 { jumpnull, .. } => {
                assert!((*jumpnull as usize) < len, "strict-input jump target out of range");
            }
            Step::Jump { jumpdone }
            | Step::JumpIfNotTrue { jumpdone, .. }
            | Step::JumpIfNotNull { jumpdone, .. } => {
                assert!((*jumpdone as usize) < len, "case jump target out of range");
            }
            Step::JumpIfNull { jumpdone, .. } | Step::SbsrefSubscripts { jumpdone, .. } => {
                assert!((*jumpdone as usize) < len, "sbsref jump target out of range");
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
    // Kernelized programs never run their steps: skipping the peephole keeps
    // compile-per-query lanes (point/select1) free of the pass cost.
    if matches!(state.kernel, Kernel::Program) {
        fuse_program(state);
    }
    if dump_programs_enabled() {
        dump_program(state);
    }
}

fn jump_field_mut(step: &mut Step) -> Option<&mut u32> {
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
        | Step::FuncStrict2Qual { jumpdone, .. } => Some(jumpdone),
        Step::AggStrictInputCheck { jumpnull, .. }
        | Step::AggStrictInputCheck1 { jumpnull, .. } => Some(jumpnull),
        _ => None,
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

fn try_fuse(a: &Step, b: &Step) -> Option<Step> {
    match (a, b) {
        (Step::ScanVar { attnum, vartype, out }, Step::FuncExprStrict2 { call, out: fout }) => {
            let argno = arg_index_of(call, *out)?;
            Some(Step::ScanVarFuncStrict2 {
                attnum: *attnum,
                vartype: *vartype,
                argno,
                call: (*call).into(),
                out: *fout,
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
            Some(Step::FuncFuncStrict2 {
                call1: (*call1).into(),
                argno,
                call2: (*call2).into(),
                out: *fout,
            })
        }
        (Step::FuncExprStrict2 { call, out }, Step::Qual { jumpdone }) => {
            Some(Step::FuncStrict2Qual { call: (*call).into(), jumpdone: *jumpdone, out: *out })
        }
        (Step::OuterVar { attnum, vartype, out }, Step::NotDistinct { call, out: fout }) => {
            let argno = arg_index_of(call, *out)?;
            Some(Step::OuterVarNotDistinct {
                attnum: *attnum,
                vartype: *vartype,
                argno,
                call: (*call).into(),
                out: *fout,
            })
        }
        (Step::NotDistinct { call, out }, Step::Qual { jumpdone }) if call.nargs == 2 => {
            Some(Step::NotDistinctQual { call: (*call).into(), jumpdone: *jumpdone, out: *out })
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
pub(crate) fn fuse_program(state: &mut ExprState<'_>) {
    let len = state.steps.len();
    if len < 3 {
        return;
    }
    if !state
        .steps
        .as_slice()
        .windows(2)
        .any(|w| try_fuse(&w[0], &w[1]).is_some())
    {
        return;
    }
    let mcx = *state.steps.allocator();
    let steps = state.steps.as_slice();
    let mut is_target = ::mcx::vec_with_capacity_in_infallible::<bool>(mcx, len);
    is_target.resize(len, false);
    for s in steps {
        let mut s = *s;
        if let Some(j) = jump_field_mut(&mut s) {
            is_target[*j as usize] = true;
        }
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
        out.push(steps[i]);
        i += 1;
    }
    debug_assert_eq!(map.len(), len);
    for s in out.iter_mut() {
        if let Some(j) = jump_field_mut(s) {
            *j = map[*j as usize];
        }
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
                Kernel::AggTransByVal { call: *call, pergroup: *pergroup, strict: false }
            }
            Step::AggPlainTransStrictByVal { call, pergroup }
                if matches!(steps[1], Step::DoneNoReturn) =>
            {
                Kernel::AggTransByVal { call: *call, pergroup: *pergroup, strict: true }
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
