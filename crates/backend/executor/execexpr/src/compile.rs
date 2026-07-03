use alloc::boxed::Box;
use alloc::format;

use ::mcx::{alloc_in, Mcx, PgBox};
use ::types_core::fmgr::FnExprErased;
use ::types_core::{Oid, FUNC_MAX_ARGS};
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_TOO_MANY_ARGUMENTS,
};
use ::types_fmgr::{TRACK_FUNC_ALL, TRACK_FUNC_OFF};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::primnodes::{Var, VarReturningType};
use ::types_nodes::NodeTag;
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
}

pub struct AggTransSpec<'a, 'mcx> {
    pub transfn_oid: Oid,
    pub inputcollid: Oid,
    pub init_value_is_null: bool,
    pub args: &'a NodeList<'mcx>,
    pub pergroup: NonNull<AggPerGroup>,
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
fn push_step(state: &mut ExprState<'_>, mcx: Mcx<'_>, step: Step) -> PgResult<()> {
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

/// C `ExecInitExpr` (parent-less form; PlanState vocab is the execProcnode
/// unit). NULL expression -> None, as C.
pub fn exec_init_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    let Some(node) = node else {
        return Ok(None);
    };
    let mut state = alloc_in(mcx, ExprState::new_in(mcx)?)?;
    create_expr_setup_steps(&mut state, mcx, &[node])?;
    init_expr_rec(node, &mut state, mcx, OutRef::RESULT, None)?;
    push_step(&mut state, mcx, Step::DoneReturn)?;
    ready_expr(&mut state);
    Ok(Some(state))
}

/// C `ExecInitQual`: implicit-AND qual list, empty -> None.
pub fn exec_init_qual<'mcx>(
    mcx: Mcx<'mcx>,
    qual: &NodeList<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    if qual.is_nil() {
        return Ok(None);
    }
    let mut state = alloc_in(mcx, ExprState::new_in(mcx)?)?;
    state.flags = EEO_FLAG_IS_QUAL;
    create_expr_setup_steps(&mut state, mcx, qual.as_slice())?;

    for node in qual.iter() {
        init_expr_rec(node, &mut state, mcx, OutRef::RESULT, None)?;
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
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, None)
}

/// Agg-node projection: Aggrefs bound to the AggState's result arrays.
pub fn exec_build_agg_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: AggBind,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, Some(agg))
}

fn build_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: Option<AggBind>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mut state = alloc_in(mcx, ExprState::new_in(mcx)?)?;
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
            init_expr_rec(tle.expr, &mut state, mcx, OutRef::RESULT, agg)?;
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
/// shapes panic at build, never at run.
pub fn exec_build_agg_trans<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mut state = alloc_in(mcx, ExprState::new_in(mcx)?)?;
    let mut info = SetupInfo::default();
    for spec in specs {
        for tle in spec.args.iter() {
            setup_walker(tle, &mut info);
        }
    }
    push_fetch_steps(&mut state, mcx, &info)?;

    for spec in specs {
        let num_trans_inputs = spec.args.len();
        let nargs = num_trans_inputs + 1;
        if nargs > FUNC_MAX_ARGS {
            return Err(too_many_args(nargs));
        }
        let flinfo = fmgr_core::fmgr_info(spec.transfn_oid)?;
        if flinfo.fn_retset {
            return Err(retset_error());
        }
        if flinfo.fn_strict && num_trans_inputs > 0 {
            unported("EEOP_AGG_STRICT_INPUT_CHECK_ARGS (strict transfn with aggregated args)");
        }
        if flinfo.fn_strict && spec.init_value_is_null {
            unported("EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL (strict transfn, NULL initval)");
        }
        let fn_addr = flinfo.fn_addr;
        let fn_strict = flinfo.fn_strict;
        let frame = FuncFrame::new_in(mcx, flinfo, nargs as u16, spec.inputcollid)?;
        let frame_ix = state.frames.len() as u32;
        let call =
            FuncCall { fn_addr, fcinfo: frame.fcinfo, frame: frame_ix, nargs: nargs as u16 };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);
        for (argno, tle_node) in spec.args.iter().enumerate() {
            let tle = tle_node.as_target_entry().unwrap_or_else(|| {
                panic!("Aggref.args cell: expected TargetEntry, got {:?}", tle_node.node_tag())
            });
            if tle.resjunk {
                continue;
            }
            // SAFETY: argno + 1 <= num_trans_inputs < nargs of `call.fcinfo`.
            let arg_out =
                OutRef(Some(unsafe { crate::steps::arg_slot_of(call.fcinfo, argno + 1) }));
            init_expr_rec(tle.expr, &mut state, mcx, arg_out, None)?;
        }
        let step = if fn_strict {
            Step::AggPlainTransStrictByVal { call, pergroup: spec.pergroup }
        } else {
            Step::AggPlainTransByVal { call, pergroup: spec.pergroup }
        };
        push_step(&mut state, mcx, step)?;
    }
    push_step(&mut state, mcx, Step::DoneNoReturn)?;
    ready_expr(&mut state);
    Ok(state)
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
fn create_expr_setup_steps<'mcx>(
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
        NodeTag::T_Const | NodeTag::T_Param => {}
        // C expr_setup_walker: Aggref args never eval in the caller's econtext.
        NodeTag::T_Aggref => {}
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
        tag => panic!("execexpr setup walker: node family {tag:?} not ported"),
    }
}

// C ExecInitExprRec over the ported families.
fn init_expr_rec<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<AggBind>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let variable = node.as_var().unwrap();
            if variable.varattno == 0 {
                unported("EEOP_WHOLEROW");
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
        NodeTag::T_Param => unported("EEOP_PARAM_EXEC/EEOP_PARAM_EXTERN (ParamListInfo)"),
        NodeTag::T_FuncExpr => {
            let func = node.as_func_expr().unwrap();
            let step =
                init_func(node, &func.args, func.funcid, func.inputcollid, state, mcx, out, agg)?;
            push_step(state, mcx, step)
        }
        NodeTag::T_OpExpr => {
            let op = node.as_op_expr().unwrap();
            let step =
                init_func(node, &op.args, op.opfuncid, op.inputcollid, state, mcx, out, agg)?;
            push_step(state, mcx, step)
        }
        NodeTag::T_Aggref => {
            let aggref = node.as_aggref().unwrap();
            let Some(bind) = agg else {
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
        tag => panic!("execexpr ExecInitExprRec: node family {tag:?} not ported"),
    }
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
fn init_func<'mcx>(
    node: Node<'mcx>,
    args: &NodeList<'mcx>,
    funcid: Oid,
    inputcollid: Oid,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<AggBind>,
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
    flinfo.fn_expr = Some(FnExprErased::from_node_erased::<Node<'mcx>, Node<'static>>(node));
    if flinfo.fn_retset {
        return Err(retset_error());
    }

    let fn_addr = flinfo.fn_addr;
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
    let call = FuncCall { fn_addr, fcinfo: frame.fcinfo, frame: frame_ix, nargs: nargs as u16 };
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);
    for (argno, arg) in args.iter().enumerate() {
        if arg.as_const().is_none() {
            // SAFETY: argno < nargs of the image `call.fcinfo` points at.
            let arg_out = OutRef(Some(unsafe { crate::steps::arg_slot_of(call.fcinfo, argno) }));
            init_expr_rec(arg, state, mcx, arg_out, agg)?;
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

// C ExecReadyExpr -> ExecReadyInterpretedExpr: program sanity + fast-path
// kernel selection (the ExecJust* table, plus the fused monomorphized shapes).
// The interpreter's unchecked cursor/frame accesses rest on construction
// invariants of this module's private program build (Done-terminated, Qual
// jumps patched to a valid index, FuncCall mirrors its pushed frame) —
// debug-asserted here, unreachable to break from outside the crate.
#[inline]
fn ready_expr(state: &mut ExprState<'_>) {
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
            Step::FuncExpr { call, .. }
            | Step::FuncExprStrict1 { call, .. }
            | Step::FuncExprStrict2 { call, .. }
            | Step::FuncExprStrict { call, .. }
            | Step::AggPlainTransByVal { call, .. }
            | Step::AggPlainTransStrictByVal { call, .. } => {
                let f = &state.frames[call.frame as usize];
                assert!(call.nargs == f.nargs && call.fcinfo == f.fcinfo);
            }
            _ => {}
        }
    }
    state.flags |= crate::steps::EEO_FLAG_INTERPRETER_INITIALIZED;
    state.kernel = select_kernel(state);
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
            Step::Const { value, isnull, out } if out.is_result() => {
                Kernel::JustConst { value: *value, isnull: *isnull }
            }
            Step::FuncExpr { call, out }
            | Step::FuncExprStrict1 { call, out }
            | Step::FuncExprStrict2 { call, out }
            | Step::FuncExprStrict { call, out }
                if out.is_result() && all_args_const(state, *call) =>
            {
                Kernel::JustFunc {
                    fn_addr: call.fn_addr,
                    frame: call.frame,
                    nargs: call.nargs,
                    strict: !matches!(steps[0], Step::FuncExpr { .. }),
                }
            }
            _ => match (var_src(&steps[0]), assign_var_src(&steps[0])) {
                (Some((src, attnum, out)), _) if out.is_result() => {
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
                if fsrc == src && out.is_result() {
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
                if out.is_result() {
                    return Kernel::JustConstAssign { value: *value, isnull: *isnull, resultnum: *resultnum };
                }
            }
            Kernel::Program
        }
        5 => select_fused_qual(state).unwrap_or(Kernel::Program),
        _ => Kernel::Program,
    }
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
    if !out.is_result() {
        return None;
    }
    let Step::Qual { jumpdone } = steps[3] else {
        return None;
    };
    if jumpdone != 4 || !matches!(steps[4], Step::DoneReturn) {
        return None;
    }

    let frame = &state.frames[call.frame as usize];
    let cmp = CmpOp::for_fn_oid(frame.flinfo.fn_oid)?;
    let var_is_arg0 = var_out.0 == Some(frame.arg_slot(0));
    let const_argno = if var_is_arg0 { 1usize } else { 0 };
    if var_out.0 != Some(frame.arg_slot(if var_is_arg0 { 0 } else { 1 })) {
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
