use alloc::boxed::Box;
use alloc::format;

use ::mcx::{Allocator, Mcx, PgBox, PgVec};
use ::types_core::fmgr::FnExprErased;
use ::types_core::{Oid, FUNC_MAX_ARGS};
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_TOO_MANY_ARGUMENTS,
};
use ::types_fmgr::{FmNodePtr, TRACK_FUNC_ALL, TRACK_FUNC_OFF};
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
}

pub struct AggTransSpec<'a, 'mcx> {
    pub transfn_oid: Oid,
    pub inputcollid: Oid,
    pub init_value_is_null: bool,
    pub args: &'a NodeList<'mcx>,
    pub pergroup: NonNull<AggPerGroup>,
    pub transtype_byval: bool,
    pub transtype_len: i16,
}

// WindowAgg projection binding: same result arrays, indexed by wfuncno,
// resolved by node identity (wfuncnos assigned at ExecInitWindowAgg).
#[derive(Clone, Copy)]
pub struct WinBind<'a, 'mcx> {
    pub agg: AggBind,
    pub wfuncnos: &'a [(Node<'mcx>, u16)],
}

#[derive(Clone, Copy)]
enum Bind<'a, 'mcx> {
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
    params: ParamBind<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    let Some(node) = node else {
        return Ok(None);
    };
    let mut state = ExprState::new_boxed_in(mcx)?;
    create_expr_setup_steps(&mut state, mcx, &[node])?;
    init_expr_rec(node, &mut state, mcx, OutRef::RESULT, None, params)?;
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
    if qual.is_nil() {
        return Ok(None);
    }
    let mut state = ExprState::new_boxed_in(mcx)?;
    state.flags = EEO_FLAG_IS_QUAL;
    create_expr_setup_steps(&mut state, mcx, qual.as_slice())?;

    for node in qual.iter() {
        init_expr_rec(node, &mut state, mcx, OutRef::RESULT, None, params)?;
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
        init_expr_rec(node, &mut state, mcx, OutRef::RESULT, Some(Bind::Agg(agg)), params)?;
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
    build_projection_info(mcx, target_list, input_desc, None, params)
}

/// Agg-node projection: Aggrefs bound to the AggState's result arrays.
pub fn exec_build_agg_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: AggBind,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    build_projection_info(mcx, target_list, input_desc, Some(Bind::Agg(agg)), params)
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
    build_projection_info(mcx, target_list, input_desc, Some(Bind::Win(win)), params)
}

fn build_projection_info<'mcx>(
    mcx: Mcx<'mcx>,
    target_list: &NodeList<'mcx>,
    input_desc: Option<&TupleDescData<'mcx>>,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
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
            init_expr_rec(tle.expr, &mut state, mcx, OutRef::RESULT, agg, params)?;
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
    build_agg_trans(mcx, specs, None, agg_node, params)
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
    build_agg_trans(mcx, specs, Some(base), agg_node, params)
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
    indirect_base: Option<NonNull<NonNull<AggPerGroup>>>,
    agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mut state = ExprState::new_boxed_in(mcx)?;
    let mut info = SetupInfo::default();
    for spec in specs {
        for tle in spec.args.iter() {
            setup_walker(tle, &mut info);
        }
    }
    push_fetch_steps(&mut state, mcx, &info)?;

    for (transno, spec) in specs.iter().enumerate() {
        let num_trans_inputs = spec.args.len();
        let nargs = num_trans_inputs + 1;
        if nargs > FUNC_MAX_ARGS {
            return Err(too_many_args(nargs));
        }
        let flinfo = fmgr_core::fmgr_info(spec.transfn_oid)?;
        if flinfo.fn_retset {
            return Err(retset_error());
        }
        if flinfo.fn_strict && spec.init_value_is_null && spec.transtype_byval {
            unported("EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL (strict transfn, NULL initval)");
        }
        let fn_addr = flinfo.fn_addr;
        let fn_strict = flinfo.fn_strict;
        let frame = FuncFrame::new_in(mcx, flinfo, nargs as u16, spec.inputcollid)?;
        // SAFETY: fresh frame image; the caller's agg_node outlives the program.
        unsafe { crate::steps::fcinfo_mut(frame.fcinfo, nargs as u16).context = agg_node };
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
            init_expr_rec(tle.expr, &mut state, mcx, arg_out, None, params)?;
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
        let step = if spec.transtype_byval {
            match (indirect_base, fn_strict) {
                (None, true) => Step::AggPlainTransStrictByVal { call, pergroup: spec.pergroup },
                (None, false) => Step::AggPlainTransByVal { call, pergroup: spec.pergroup },
                (Some(base), true) => {
                    Step::AggTransStrictByValIndirect { call, base, transno: transno as u16 }
                }
                (Some(base), false) => {
                    Step::AggTransByValIndirect { call, base, transno: transno as u16 }
                }
            }
        } else {
            let byref = crate::steps::AggByRef {
                agg: agg_state_node(agg_node),
                translen: spec.transtype_len,
            };
            let transno = transno as u16;
            match (indirect_base, fn_strict, spec.init_value_is_null) {
                (None, true, true) => {
                    Step::AggPlainTransInitStrictByRef { call, pergroup: spec.pergroup, byref }
                }
                (None, true, false) => {
                    Step::AggPlainTransStrictByRef { call, pergroup: spec.pergroup, byref }
                }
                (None, false, _) => {
                    Step::AggPlainTransByRef { call, pergroup: spec.pergroup, byref }
                }
                (Some(base), true, true) => {
                    Step::AggTransInitStrictByRefIndirect { call, base, transno, byref }
                }
                (Some(base), true, false) => {
                    Step::AggTransStrictByRefIndirect { call, base, transno, byref }
                }
                (Some(base), false, _) => {
                    Step::AggTransByRefIndirect { call, base, transno, byref }
                }
            }
        };
        push_step(&mut state, mcx, step)?;
        if let Some(ix) = bailout {
            let target = state.steps.len() as u32;
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
        let out = if num_cols > 0 { OutRef(iresult) } else { OutRef::RESULT };
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
        let fn_addr = flinfo.fn_addr;
        let frame = FuncFrame::new_in(mcx, flinfo, 1, collations[i])?;
        let frame_ix = state.frames.len() as u32;
        let call = FuncCall { fn_addr, fcinfo: frame.fcinfo, frame: frame_ix, nargs: 1 };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);

        // SAFETY: arg 0 of the frame's freshly allocated 1-arg fcinfo.
        let arg_out = OutRef(Some(unsafe { crate::steps::arg_slot_of(call.fcinfo, 0) }));
        let vartype = desc.attrs[attnum as usize].atttypid;
        push_step(&mut state, mcx, Step::InnerVar { attnum, vartype, out: arg_out })?;

        let out = if i == num_cols - 1 { OutRef::RESULT } else { OutRef(iresult) };
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
        let fn_addr = flinfo.fn_addr;
        let frame = FuncFrame::new_in(mcx, flinfo, 2, collations[natt])?;
        let frame_ix = state.frames.len() as u32;
        let call = FuncCall { fn_addr, fcinfo: frame.fcinfo, frame: frame_ix, nargs: 2 };
        state
            .frames
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
        state.frames.push(frame);

        // SAFETY: args 0/1 of the frame's freshly allocated 2-arg fcinfo.
        let (arg0, arg1) = unsafe {
            (
                OutRef(Some(crate::steps::arg_slot_of(call.fcinfo, 0))),
                OutRef(Some(crate::steps::arg_slot_of(call.fcinfo, 1))),
            )
        };
        let ltype = ldesc.attrs[attnum as usize].atttypid;
        let rtype = rdesc.attrs[attnum as usize].atttypid;
        push_step(&mut state, mcx, Step::InnerVar { attnum, vartype: ltype, out: arg0 })?;
        push_step(&mut state, mcx, Step::OuterVar { attnum, vartype: rtype, out: arg1 })?;
        push_step(&mut state, mcx, Step::NotDistinct { call, out: OutRef::RESULT })?;
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

fn alloc_nullable_datum(mcx: Mcx<'_>) -> PgResult<NonNull<::datum::NullableDatum>> {
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
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxtype,
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().r#type,
        NodeTag::T_BoolExpr | NodeTag::T_NullTest => 16,
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
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_SQLValueFunction => {}
        // C expr_setup_walker: Aggref/WindowFunc args never eval in the
        // caller's econtext.
        NodeTag::T_Aggref | NodeTag::T_WindowFunc => {}
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
        tag => panic!("execexpr setup walker: node family {tag:?} not ported"),
    }
}

// C ExecInitExprRec over the ported families.
fn init_expr_rec<'mcx>(
    node: Node<'mcx>,
    state: &mut ExprState<'mcx>,
    mcx: Mcx<'mcx>,
    out: OutRef,
    agg: Option<Bind<'_, 'mcx>>,
    params: ParamBind<'mcx>,
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
                node, &func.args, func.funcid, func.inputcollid, state, mcx, out, agg, params,
            )?;
            push_step(state, mcx, step)
        }
        NodeTag::T_OpExpr => {
            let op = node.as_op_expr().unwrap();
            let step = init_func(
                node, &op.args, op.opfuncid, op.inputcollid, state, mcx, out, agg, params,
            )?;
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
            let step = init_minmax(node, mm, state, mcx, out, agg, params)?;
            push_step(state, mcx, step)
        }
        NodeTag::T_SQLValueFunction => {
            let svf = node.as_sql_value_function().unwrap();
            let layout = core::alloc::Layout::from_size_align(12, 8).expect("timetz layout");
            let timetz = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
            push_step(
                state,
                mcx,
                Step::SqlValueFunction { op: svf.op, typmod: svf.typmod, timetz, out },
            )
        }
        NodeTag::T_BoolExpr => init_bool_expr(node, state, mcx, out, agg, params),
        NodeTag::T_NullTest => {
            use ::types_nodes::primnodes::NullTestType;
            let nt = node.as_null_test().unwrap();
            if nt.argisrow {
                unported("EEOP_NULLTEST_ROWISNULL/ROWISNOTNULL");
            }
            init_expr_rec(nt.arg.expect("NullTest.arg"), state, mcx, out, agg, params)?;
            let step = match nt.nulltesttype {
                NullTestType::IS_NULL => Step::NullTestIsNull { out },
                NullTestType::IS_NOT_NULL => Step::NullTestIsNotNull { out },
            };
            push_step(state, mcx, step)
        }
        tag => panic!("execexpr ExecInitExprRec: node family {tag:?} not ported"),
    }
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
) -> PgResult<()> {
    use ::types_nodes::primnodes::BoolExprType;
    let b = node.as_bool_expr().unwrap();
    let nargs = b.args.len();
    if b.boolop == BoolExprType::NOT_EXPR {
        assert!(nargs == 1, "NOT with {nargs} args");
        init_expr_rec(b.args.nth(0), state, mcx, out, agg, params)?;
        return push_step(state, mcx, Step::BoolNotStep { out });
    }
    assert!(nargs >= 2, "{:?} with {nargs} args", b.boolop);
    let anynull = alloc_bool(mcx)?;
    let is_and = b.boolop == BoolExprType::AND_EXPR;
    let mut adjust_jumps: PgVec<'_, usize> = PgVec::new_in(mcx);
    for (off, arg) in b.args.iter().enumerate() {
        init_expr_rec(arg, state, mcx, out, agg, params)?;
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
) -> PgResult<Step> {
    let nelems = mm.args.len();
    let entry = typcache::lookup_type_cache(mm.minmaxtype, typcache::TYPECACHE_CMP_PROC)?;
    let cmp_proc = entry.cmp_proc();
    if cmp_proc == 0 {
        return Err(no_cmp_function(mm.minmaxtype)?);
    }
    let mut flinfo = fmgr_core::fmgr_info(cmp_proc)?;
    flinfo.fn_expr = Some(FnExprErased::from_node_erased::<Node<'mcx>, Node<'static>>(node));
    let fn_addr = flinfo.fn_addr;
    let frame = FuncFrame::new_in(mcx, flinfo, 2, mm.inputcollid)?;
    let frame_ix = state.frames.len() as u32;
    let call = FuncCall { fn_addr, fcinfo: frame.fcinfo, frame: frame_ix, nargs: 2 };
    state.frames.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<FuncFrame<'_>>()))?;
    state.frames.push(frame);

    let layout = core::alloc::Layout::array::<::datum::NullableDatum>(nelems)
        .expect("minmax slots layout");
    let slots: NonNull<::datum::NullableDatum> =
        mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
    for (i, arg) in mm.args.iter().enumerate() {
        // SAFETY: i < nelems of the freshly allocated slot array.
        let arg_out = OutRef(Some(unsafe { NonNull::new_unchecked(slots.as_ptr().add(i)) }));
        init_expr_rec(arg, state, mcx, arg_out, agg, params)?;
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
            init_expr_rec(arg, state, mcx, arg_out, agg, params)?;
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
