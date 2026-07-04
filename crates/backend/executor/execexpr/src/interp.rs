use alloc::boxed::Box;
use alloc::format;

use ::datum::{Datum, NullableDatum};
use ::types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use ::types_slot::SlotData;

use crate::steps::{
    fcinfo_mut, ExprState, FuncCall, Kernel, OutRef, SlotSrc, Step, EEO_FLAG_STILL_VALID_CHECKED,
};

// C ExprContext's slot triple (execnodes/execUtils are the executor-state
// unit); the result slot rides separately, bound per projection call.
#[derive(Default)]
pub struct EvalSlots<'a, 'mcx> {
    pub scan: Option<&'a mut SlotData<'mcx>>,
    pub inner: Option<&'a mut SlotData<'mcx>>,
    pub outer: Option<&'a mut SlotData<'mcx>>,
}

impl<'a, 'mcx> EvalSlots<'a, 'mcx> {
    #[inline(always)]
    fn get(&mut self, src: SlotSrc) -> &mut SlotData<'mcx> {
        let slot = match src {
            SlotSrc::Scan => self.scan.as_deref_mut(),
            SlotSrc::Inner => self.inner.as_deref_mut(),
            SlotSrc::Outer => self.outer.as_deref_mut(),
        };
        match slot {
            Some(s) => s,
            None => missing_slot(src),
        }
    }
}

#[cold]
#[inline(never)]
fn missing_slot(src: SlotSrc) -> ! {
    panic!("execexpr: expression references the {src:?} slot but none was supplied")
}

#[cold]
#[inline(never)]
fn invalid_role_oid(roleid: ::types_core::Oid) -> Box<PgError> {
    PgError::new(::types_error::ERROR, format!("invalid role OID: {roleid}"))
        .with_sqlstate(::types_error::ERRCODE_UNDEFINED_OBJECT)
        .into()
}

#[cold]
#[inline(never)]
fn no_result_slot() -> ! {
    panic!("execexpr: projection step without a result slot")
}

#[cold]
#[inline(never)]
fn param_exec_plan_pending() -> ! {
    panic!(
        "execexpr EEOP_PARAM_EXEC: pending initplan — owning node did not run \
         exec_eval_param_exec_params before evaluation (nodeSubplan.c lane)"
    )
}

#[derive(Clone, Copy, Debug)]
pub struct Suspension {
    pub sstate: core::ptr::NonNull<()>,
    step: u32,
    regs: NullableDatum,
}

#[derive(Clone, Copy, Debug)]
pub struct Resume {
    step: u32,
    regs: NullableDatum,
    result: NullableDatum,
}

impl Suspension {
    pub fn resume_with(self, result: NullableDatum) -> Resume {
        Resume { step: self.step, regs: self.regs, result }
    }
}

pub enum EvalOutcome {
    Done(NullableDatum),
    Suspended(Suspension),
}

#[cold]
#[inline(never)]
fn subplan_without_driver() -> ! {
    panic!(
        "execexpr EEOP_SUBPLAN: SubPlan expression evaluated through a subplan-less \
         entry point — owning node must use the executils subplan driver"
    )
}

/// C `ExecEvalExprSwitchContext`/`ExecInterpExprStillValid`: one-time Var
/// validity check, then kernel dispatch.
#[inline(always)]
pub fn exec_eval_expr<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
) -> PgResult<NullableDatum> {
    check_still_valid(state, slots)?;
    match eval(state, slots, None, None)? {
        EvalOutcome::Done(nd) => Ok(nd),
        EvalOutcome::Suspended(_) => subplan_without_driver(),
    }
}

pub fn exec_eval_expr_outcome<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    resume: Option<Resume>,
) -> PgResult<EvalOutcome> {
    check_still_valid(state, slots)?;
    eval(state, slots, None, resume)
}

pub enum QualOutcome {
    Done(bool),
    Suspended(Suspension),
}

pub fn exec_qual_outcome<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    resume: Option<Resume>,
) -> PgResult<QualOutcome> {
    debug_assert!(state.is_qual());
    check_still_valid(state, slots)?;
    Ok(match eval(state, slots, None, resume)? {
        EvalOutcome::Done(r) => {
            debug_assert!(!r.isnull);
            QualOutcome::Done(r.value.as_bool())
        }
        EvalOutcome::Suspended(s) => QualOutcome::Suspended(s),
    })
}

pub fn exec_project_outcome<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    result_slot: &mut SlotData<'mcx>,
    resume: Option<Resume>,
) -> PgResult<Option<Suspension>> {
    check_still_valid(state, slots)?;
    Ok(match eval(state, slots, Some(result_slot), resume)? {
        EvalOutcome::Done(_) => None,
        EvalOutcome::Suspended(s) => Some(s),
    })
}

/// C `ExecQual`: false on NULL, expression compiled by [`exec_init_qual`];
/// a `None` state is C's NULL ExprState == constant TRUE.
#[inline(always)]
pub fn exec_qual<'mcx>(
    state: Option<&mut ExprState<'mcx>>,
    slots: &mut EvalSlots<'_, 'mcx>,
) -> PgResult<bool> {
    let Some(state) = state else {
        return Ok(true);
    };
    debug_assert!(state.is_qual());
    check_still_valid(state, slots)?;
    if let Kernel::QualScanVarCmpConst { attnum, konst, cmp } = state.kernel {
        let scan = slots.get(SlotSrc::Scan);
        let mut isnull = false;
        let v = exectuples::slot_getattr(scan, attnum as i32 + 1, &mut isnull);
        return Ok(!isnull && cmp.eval(v, konst));
    }
    if let Kernel::QualVarCmpVar { a_src, a_attnum, b_src, b_attnum, cmp } = state.kernel {
        let mut isnull = false;
        let a = exectuples::slot_getattr(slots.get(a_src), a_attnum as i32 + 1, &mut isnull);
        if isnull {
            return Ok(false);
        }
        let b = exectuples::slot_getattr(slots.get(b_src), b_attnum as i32 + 1, &mut isnull);
        return Ok(!isnull && cmp.eval(a, b));
    }
    let r = match eval(state, slots, None, None)? {
        EvalOutcome::Done(nd) => nd,
        EvalOutcome::Suspended(_) => subplan_without_driver(),
    };
    debug_assert!(!r.isnull);
    Ok(r.value.as_bool())
}

/// C `ExecProject` minus the ProjectionInfo wrapper: clear the result slot,
/// run the projection program, store virtual.
pub fn exec_project<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    result_slot: &mut SlotData<'mcx>,
    result_mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    check_still_valid(state, slots)?;
    state.arm_result_mcx(result_mcx);
    exectuples::exec_clear_tuple(result_slot, result_mcx);
    match eval(state, slots, Some(result_slot), None)? {
        EvalOutcome::Done(_) => {}
        EvalOutcome::Suspended(_) => subplan_without_driver(),
    }
    exectuples::exec_store_virtual_tuple(result_slot);
    Ok(())
}

#[inline(always)]
fn eval<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    result_slot: Option<&mut SlotData<'mcx>>,
    resume: Option<Resume>,
) -> PgResult<EvalOutcome> {
    if let Kernel::Program = state.kernel {
        return run_program(state, slots, result_slot, resume);
    }
    debug_assert!(resume.is_none());
    eval_kernel(state, slots, result_slot).map(EvalOutcome::Done)
}

#[inline(always)]
fn eval_kernel<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    result_slot: Option<&mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    match state.kernel {
        Kernel::Program => unreachable!("run_program handled by eval"),
        Kernel::JustConst { value, isnull } => Ok(NullableDatum { value, isnull }),
        Kernel::JustConstAssign { value, isnull, resultnum } => {
            let rslot = result_slot.unwrap_or_else(|| no_result_slot());
            assign_to_result(rslot, resultnum, value, isnull);
            Ok(NullableDatum::null())
        }
        Kernel::JustVar { src, attnum } => {
            let slot = slots.get(src);
            let mut isnull = false;
            let value = exectuples::slot_getattr(slot, attnum as i32 + 1, &mut isnull);
            Ok(NullableDatum { value, isnull })
        }
        Kernel::JustVarVirt { src, attnum } => {
            let base = slots.get(src).base();
            debug_assert!((attnum as i32) < base.tts_nvalid as i32);
            // SAFETY: virtual-slot fast path — the source slot was populated
            // to >= attnum+1 (C ExecJustVarVirtImpl contract, debug-asserted).
            unsafe {
                Ok(NullableDatum {
                    value: *base.tts_values.get_unchecked(attnum as usize),
                    isnull: *base.tts_isnull.get_unchecked(attnum as usize),
                })
            }
        }
        Kernel::JustAssignVar { src, attnum, resultnum } => {
            let rslot = result_slot.unwrap_or_else(|| no_result_slot());
            let slot = slots.get(src);
            let mut isnull = false;
            let value = exectuples::slot_getattr(slot, attnum as i32 + 1, &mut isnull);
            assign_to_result(rslot, resultnum, value, isnull);
            Ok(NullableDatum::null())
        }
        Kernel::JustAssignVarVirt { src, attnum, resultnum } => {
            let rslot = result_slot.unwrap_or_else(|| no_result_slot());
            let base = slots.get(src).base();
            debug_assert!((attnum as i32) < base.tts_nvalid as i32);
            // SAFETY: as JustVarVirt.
            let (value, isnull) = unsafe {
                (
                    *base.tts_values.get_unchecked(attnum as usize),
                    *base.tts_isnull.get_unchecked(attnum as usize),
                )
            };
            assign_to_result(rslot, resultnum, value, isnull);
            Ok(NullableDatum::null())
        }
        Kernel::QualScanVarCmpConst { attnum, konst, cmp } => {
            let scan = slots.get(SlotSrc::Scan);
            let mut isnull = false;
            let v = exectuples::slot_getattr(scan, attnum as i32 + 1, &mut isnull);
            Ok(NullableDatum {
                value: Datum::from_bool(!isnull && cmp.eval(v, konst)),
                isnull: false,
            })
        }
        Kernel::QualVarCmpVar { a_src, a_attnum, b_src, b_attnum, cmp } => {
            let mut isnull = false;
            let a = exectuples::slot_getattr(slots.get(a_src), a_attnum as i32 + 1, &mut isnull);
            if isnull {
                return Ok(NullableDatum { value: Datum::from_bool(false), isnull: false });
            }
            let b = exectuples::slot_getattr(slots.get(b_src), b_attnum as i32 + 1, &mut isnull);
            Ok(NullableDatum {
                value: Datum::from_bool(!isnull && cmp.eval(a, b)),
                isnull: false,
            })
        }
        Kernel::Hash32Var { src, attnum, frame } => {
            let mut isnull = false;
            let v = exectuples::slot_getattr(slots.get(src), attnum as i32 + 1, &mut isnull);
            if isnull {
                return Ok(NullableDatum { value: Datum::from_u32(0), isnull: false });
            }
            let f = &mut state.frames[frame as usize];
            // SAFETY: 'mcx-live frame fcinfo image + boxed FmgrInfo, sole refs.
            let fcinfo = unsafe { fcinfo_mut(f.fcinfo, 1) };
            // SAFETY: arg 0 of the live image, via the reborrow — an older-tag write would invalidate fcinfo.
            unsafe {
                crate::steps::arg_slot_of(core::ptr::NonNull::from(&mut *fcinfo).cast(), 0)
                    .write(NullableDatum { value: v, isnull: false })
            };
            fcinfo.isnull = false;
            let flinfo = unsafe { &mut *f.flinfo.as_ptr() };
            let value = (flinfo.fn_addr)(Some(flinfo), fcinfo)?;
            Ok(NullableDatum { value, isnull: false })
        }
        Kernel::AggTransByVal { call, pergroup, strict } => {
            // SAFETY: once-allocated stable pergroup, sole access here (the
            // interp AggPlainTrans[Strict]ByVal arms' contract verbatim).
            unsafe {
                let pg = pergroup.as_ptr();
                if !strict || !(*pg).trans_value_is_null {
                    crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                        value: (*pg).trans_value,
                        isnull: (*pg).trans_value_is_null,
                    });
                    let (value, isnull) = invoke(&call)?;
                    (*pg).trans_value = value;
                    (*pg).trans_value_is_null = isnull;
                }
            }
            Ok(NullableDatum::null())
        }
        Kernel::AggTransByValThin { call, pergroup, strict } => {
            // SAFETY: as AggTransByVal; thin callee never sets isnull.
            unsafe {
                let pg = pergroup.as_ptr();
                if !strict || !(*pg).trans_value_is_null {
                    crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                        value: (*pg).trans_value,
                        isnull: (*pg).trans_value_is_null,
                    });
                    (*pg).trans_value = invoke_thin(&call)?;
                    (*pg).trans_value_is_null = false;
                }
            }
            Ok(NullableDatum::null())
        }
        Kernel::JustFunc { fn_addr, frame, nargs, strict } => {
            let f = &mut state.frames[frame as usize];
            // SAFETY: the frame's fcinfo image and mcx-boxed FmgrInfo are
            // live for 'mcx; no other references exist during this call.
            let fcinfo = unsafe { fcinfo_mut(f.fcinfo, nargs) };
            if strict && fcinfo.has_null_args() {
                return Ok(NullableDatum::null());
            }
            fcinfo.isnull = false;
            let value = fn_addr(Some(unsafe { &mut *f.flinfo.as_ptr() }), fcinfo)?;
            Ok(NullableDatum { value, isnull: fcinfo.isnull })
        }
    }
}

#[inline(always)]
fn assign_to_result(rslot: &mut SlotData<'_>, resultnum: u16, value: Datum, isnull: bool) {
    let base = rslot.base_mut();
    base.tts_values[resultnum as usize] = value;
    base.tts_isnull[resultnum as usize] = isnull;
}

#[inline(always)]
fn read_var(slot: &SlotData<'_>, attnum: u16) -> NullableDatum {
    let base = slot.base();
    debug_assert!((attnum as i32) < base.tts_nvalid as i32);
    // SAFETY: a preceding FETCHSOME step deformed the slot to >= attnum+1
    // (compile emits FETCHSOME covering every Var; C carries the same Assert).
    unsafe {
        NullableDatum {
            value: *base.tts_values.get_unchecked(attnum as usize),
            isnull: *base.tts_isnull.get_unchecked(attnum as usize),
        }
    }
}

#[inline(always)]
fn write_out(out: OutRef, value: Datum, isnull: bool) {
    // SAFETY: every OutRef is an 'mcx-live fcinfo arg slot or the state's
    // result cell (compile-time invariant); branch-free by design.
    unsafe { out.0.write(NullableDatum { value, isnull }) }
}

// Bool steps read-modify their own output (C's resv/resnull aliasing).
#[inline(always)]
fn read_out(out: OutRef) -> NullableDatum {
    // SAFETY: as write_out.
    unsafe { out.0.read() }
}

// The interpreter: flat step array walked by a pointer cursor, loop { match }
// over the dense tags (perf-doctrine rule 12), enregisterable (cursor,
// result) state; slot bindings hoisted out of the loop as C does.
#[inline(never)]
fn run_program<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    mut result_slot: Option<&mut SlotData<'mcx>>,
    resume: Option<Resume>,
) -> PgResult<EvalOutcome> {
    let ExprState { steps, frames, resnd, saop_tables, .. } = state;
    let res = *resnd;
    let steps = steps.as_slice();
    let mut scan = slots.scan.as_deref_mut();
    let mut inner = slots.inner.as_deref_mut();
    let mut outer = slots.outer.as_deref_mut();
    // No entry reset: as in C, every DONE_RETURN path writes the cell first.
    let base = steps.as_ptr();
    let mut sp = base;
    if let Some(r) = resume {
        // SAFETY: as above.
        unsafe { res.write(r.regs) };
        let Step::SubPlan { out, .. } = steps[r.step as usize] else {
            panic!("resume target is not a SubPlan step")
        };
        write_out(out, r.result.value, r.result.isnull);
        // SAFETY: r.step is a validated in-bounds index; the program is
        // Done-terminated so step+1 is in bounds.
        sp = unsafe { base.add(r.step as usize + 1) };
    }
    loop {
        // SAFETY: ready_expr validated Done-termination and every jump
        // target; the cursor only advances by 1 or to a validated target.
        let step = unsafe { &*sp };
        match step {
            Step::DoneReturn => {
                // SAFETY: res is the state's live result cell.
                return Ok(EvalOutcome::Done(unsafe { res.read() }));
            }
            Step::DoneNoReturn => return Ok(EvalOutcome::Done(NullableDatum::null())),
            Step::ParamSet { prm, out } => {
                let r = read_out(*out);
                // SAFETY: compile-resolved pointer into stable es_param_exec_vals.
                unsafe {
                    let p = prm.as_ptr();
                    (*p).value = r.value;
                    (*p).isnull = r.isnull;
                    (*p).exec_plan = false;
                }
            }
            Step::SubPlan { sstate, out: _ } => {
                // SAFETY: sp is derived from base and in bounds.
                let step_ix = unsafe { sp.offset_from(base) } as u32;
                return Ok(EvalOutcome::Suspended(Suspension {
                    sstate: *sstate,
                    step: step_ix,
                    // SAFETY: res is the state's live result cell.
                    regs: unsafe { res.read() },
                }));
            }
            Step::ScanFetchSome { last_var } => {
                exectuples::slot_getsomeattrs(need_slot(&mut scan), *last_var as i32);
            }
            Step::InnerFetchSome { last_var } => {
                exectuples::slot_getsomeattrs(need_slot(&mut inner), *last_var as i32);
            }
            Step::OuterFetchSome { last_var } => {
                exectuples::slot_getsomeattrs(need_slot(&mut outer), *last_var as i32);
            }
            Step::ScanVar { attnum, out, .. } => {
                let nd = read_var(need_slot(&mut scan), *attnum);
                write_out(*out, nd.value, nd.isnull);
            }
            Step::InnerVar { attnum, out, .. } => {
                let nd = read_var(need_slot(&mut inner), *attnum);
                write_out(*out, nd.value, nd.isnull);
            }
            Step::OuterVar { attnum, out, .. } => {
                let nd = read_var(need_slot(&mut outer), *attnum);
                write_out(*out, nd.value, nd.isnull);
            }
            Step::NextValueExpr { seqid, seqtypid, out } => {
                let newval = sequence_seams::nextval_internal::call(*seqid, false)?;
                let d = match *seqtypid {
                    types_core::INT2OID => Datum::from_i16(newval as i16),
                    types_core::INT4OID => Datum::from_i32(newval as i32),
                    types_core::INT8OID => Datum::from_i64(newval),
                    other => panic!("unsupported sequence type {other}"),
                };
                write_out(*out, d, false);
            }
            Step::WholeRow { src, wr, frame, out } => {
                let slot = match src {
                    crate::steps::SlotSrc::Scan => need_slot(&mut scan),
                    crate::steps::SlotSrc::Inner => need_slot(&mut inner),
                    crate::steps::SlotSrc::Outer => need_slot(&mut outer),
                };
                let (value, isnull) = eval_whole_row(frames, slot, *wr, *frame)?;
                write_out(*out, value, isnull);
            }
            Step::ScanSysVar { attnum, out } => {
                let mut isnull = false;
                let d = exectuples::slot_getsysattr(need_slot(&mut scan), *attnum as i32, &mut isnull)?;
                write_out(*out, d, isnull);
            }
            Step::InnerSysVar { attnum, out } => {
                let mut isnull = false;
                let d =
                    exectuples::slot_getsysattr(need_slot(&mut inner), *attnum as i32, &mut isnull)?;
                write_out(*out, d, isnull);
            }
            Step::OuterSysVar { attnum, out } => {
                let mut isnull = false;
                let d =
                    exectuples::slot_getsysattr(need_slot(&mut outer), *attnum as i32, &mut isnull)?;
                write_out(*out, d, isnull);
            }
            Step::AssignScanVar { attnum, resultnum } => {
                let nd = read_var(need_slot(&mut scan), *attnum);
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                assign_to_result(rslot, *resultnum, nd.value, nd.isnull);
            }
            Step::AssignInnerVar { attnum, resultnum } => {
                let nd = read_var(need_slot(&mut inner), *attnum);
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                assign_to_result(rslot, *resultnum, nd.value, nd.isnull);
            }
            Step::AssignOuterVar { attnum, resultnum } => {
                let nd = read_var(need_slot(&mut outer), *attnum);
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                assign_to_result(rslot, *resultnum, nd.value, nd.isnull);
            }
            Step::AssignTmp { resultnum } => {
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                // SAFETY: res is the state's live result cell.
                let r = unsafe { res.read() };
                assign_to_result(rslot, *resultnum, r.value, r.isnull);
            }
            Step::AssignTmpMakeRo { resultnum } => {
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                // SAFETY: live result cell; non-null by-ref datum = live varlena.
                let r = unsafe { res.read() };
                let value = if !r.isnull {
                    unsafe { datum::expandeddatum::make_expanded_object_read_only_internal(r.value) }
                } else {
                    r.value
                };
                assign_to_result(rslot, *resultnum, value, r.isnull);
            }
            Step::Const { value, isnull, out } => {
                write_out(*out, *value, *isnull);
            }
            Step::ParamExtern { prm, out } => {
                // SAFETY: compile-resolved pointer, portal-lived (steps.rs note).
                let p = unsafe { prm.read() };
                write_out(*out, p.value, p.isnull);
            }
            Step::ParamExec { prm, out } => {
                // SAFETY: compile-resolved pointer into stable es_param_exec_vals.
                let p = unsafe { prm.read() };
                if p.exec_plan {
                    param_exec_plan_pending();
                }
                write_out(*out, p.value, p.isnull);
            }
            Step::FuncExpr { call, out } => {
                let (value, isnull) = invoke(call)?;
                write_out(*out, value, isnull);
            }
            Step::IoCoerce { calls, out } => {
                // SAFETY: 'mcx-owned pair written once at compile.
                let c = unsafe { calls.as_ref() };
                let nd = read_out(*out);
                let strv = if nd.isnull {
                    NullableDatum { value: Datum::null(), isnull: true }
                } else {
                    // SAFETY: arg 0 of the outcall's live fcinfo image.
                    unsafe {
                        crate::steps::arg_slot_of(c.outcall.fcinfo, 0)
                            .write(NullableDatum { value: nd.value, isnull: false })
                    };
                    let (v, isnull) = invoke(&c.outcall)?;
                    NullableDatum { value: v, isnull }
                };
                if strv.isnull && c.in_strict {
                    write_out(*out, Datum::null(), true);
                } else {
                    // SAFETY: arg 0 of the incall's live fcinfo image.
                    unsafe { crate::steps::arg_slot_of(c.incall.fcinfo, 0).write(strv) };
                    let (v, isnull) = invoke(&c.incall)?;
                    write_out(*out, v, isnull);
                }
            }
            Step::ScalarArrayOp { call, use_or, strict, typlen, typbyval, typalign, out } => {
                let arr = read_out(*out);
                let (value, isnull) = eval_scalar_array_op(
                    call, *use_or, *strict, *typlen, *typbyval, *typalign, arr,
                )?;
                write_out(*out, value, isnull);
            }
            Step::HashedScalarArrayOp { call, inclause, typlen, typbyval, typalign, table, out } => {
                let arr = read_out(*out);
                let (value, isnull) = eval_hashed_scalar_array_op(
                    &mut saop_tables[*table as usize],
                    call,
                    *inclause,
                    *typlen,
                    *typbyval,
                    *typalign,
                    arr,
                )?;
                write_out(*out, value, isnull);
            }
            Step::ArrayExprStep {
                elems,
                nelems,
                frame,
                elmtype,
                elmlen,
                elmbyval,
                elmalign,
                out,
            } => {
                let (value, isnull) = eval_array_expr(
                    frames, *elems, *nelems, *frame, *elmtype, *elmlen, *elmbyval, *elmalign,
                )?;
                write_out(*out, value, isnull);
            }
            Step::RowExprStep { elems, nelems, frame, desc, out } => {
                let (value, isnull) = eval_row_expr(frames, *elems, *nelems, *frame, *desc)?;
                write_out(*out, value, isnull);
            }
            Step::JsonConstructor { jcstate, frame, out } => {
                eval_json_constructor_step(frames, *jcstate, *frame, *out)?;
            }
            Step::IsJson { exprtype, item_type, unique_keys, frame, out } => {
                eval_is_json_step(frames, *exprtype, *item_type, *unique_keys, *frame, *out)?;
            }
            Step::FuncExprStrict1 { call, out } => {
                // SAFETY: arg 0 of the call's live fcinfo image.
                let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
                if a0.isnull {
                    write_out(*out, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke(call)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::FuncExprStrict2 { call, out } => {
                // SAFETY: args 0/1 of the call's live fcinfo image.
                let (a0, a1) = unsafe {
                    (
                        crate::steps::arg_slot_of(call.fcinfo, 0).read(),
                        crate::steps::arg_slot_of(call.fcinfo, 1).read(),
                    )
                };
                if a0.isnull || a1.isnull {
                    write_out(*out, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke(call)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::FuncExprStrict { call, out } => {
                // SAFETY: reads nargs arg slots of the call's live image.
                let anynull = (0..call.nargs as usize)
                    .any(|i| unsafe { crate::steps::arg_slot_of(call.fcinfo, i).read().isnull });
                if anynull {
                    write_out(*out, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke(call)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::FuncExprFusage { call, out } => {
                let (value, isnull) = invoke_fusage(call)?;
                write_out(*out, value, isnull);
            }
            Step::FuncExprStrictFusage { call, out } => {
                // SAFETY: reads nargs arg slots of the call's live image.
                let anynull = (0..call.nargs as usize)
                    .any(|i| unsafe { crate::steps::arg_slot_of(call.fcinfo, i).read().isnull });
                if anynull {
                    write_out(*out, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke_fusage(call)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::XmlExprEval { state, out } => {
                // SAFETY: compile-allocated state, live for the program.
                let st = unsafe { state.as_ref() };
                let (value, isnull) = crate::xmlops::eval_xml_expr(st)?;
                write_out(*out, value, isnull);
            }
            Step::MinMax { call, slots, nelems, least, out } => {
                let mut value = Datum::null();
                let mut isnull = true;
                for off in 0..*nelems as usize {
                    // SAFETY: off < nelems of the compile-allocated slot array.
                    let nd = unsafe { slots.as_ptr().add(off).read() };
                    if nd.isnull {
                        continue;
                    }
                    if isnull {
                        value = nd.value;
                        isnull = false;
                        continue;
                    }
                    // SAFETY: args 0/1 of the call's live 2-arg fcinfo image.
                    unsafe {
                        crate::steps::arg_slot_of(call.fcinfo, 0)
                            .write(NullableDatum { value, isnull: false });
                        crate::steps::arg_slot_of(call.fcinfo, 1)
                            .write(NullableDatum { value: nd.value, isnull: false });
                    }
                    let (cmp, cmpnull) = invoke(call)?;
                    if cmpnull {
                        continue;
                    }
                    let cmp = cmp.as_i32();
                    if (cmp > 0 && *least) || (cmp < 0 && !*least) {
                        value = nd.value;
                    }
                }
                write_out(*out, value, isnull);
            }
            Step::SqlValueFunction { op, typmod, scratch, out } => {
                use ::types_nodes::primnodes::SQLValueFunctionOp as Op;
                let value = match op {
                    Op::SVFOP_CURRENT_DATE => Datum::from_i32(adt_date::GetSQLCurrentDate()),
                    Op::SVFOP_CURRENT_TIME | Op::SVFOP_CURRENT_TIME_N => {
                        let t = adt_date::GetSQLCurrentTime(*typmod);
                        // SAFETY: compile-allocated 12-byte 8-aligned image
                        // slot owned by this step (steps.rs note).
                        unsafe {
                            scratch.as_ptr().cast::<i64>().write(t.time);
                            scratch.as_ptr().add(8).cast::<i32>().write(t.zone);
                        }
                        Datum::from_usize(scratch.as_ptr() as usize)
                    }
                    Op::SVFOP_CURRENT_TIMESTAMP | Op::SVFOP_CURRENT_TIMESTAMP_N => {
                        Datum::from_i64(adt_timestamp::GetSQLCurrentTimestamp(*typmod))
                    }
                    Op::SVFOP_LOCALTIME | Op::SVFOP_LOCALTIME_N => {
                        Datum::from_i64(adt_date::GetSQLLocalTime(*typmod))
                    }
                    Op::SVFOP_LOCALTIMESTAMP | Op::SVFOP_LOCALTIMESTAMP_N => {
                        Datum::from_i64(adt_timestamp::GetSQLLocalTimestamp(*typmod)?)
                    }
                    Op::SVFOP_CURRENT_ROLE | Op::SVFOP_CURRENT_USER | Op::SVFOP_USER
                    | Op::SVFOP_SESSION_USER => {
                        let roleid = if matches!(op, Op::SVFOP_SESSION_USER) {
                            miscinit_seams::get_session_user_id::call()
                        } else {
                            miscinit_seams::get_user_id::call()
                        };
                        let shape = syscache_seams::lookup_authid_session_by_oid::call(roleid)?
                            .ok_or_else(|| invalid_role_oid(roleid))?;
                        // SAFETY: compile-allocated NameData-sized image slot
                        // owned by this step (steps.rs note).
                        unsafe {
                            scratch
                                .as_ptr()
                                .cast::<::types_tuple::NameData>()
                                .write(shape.rolname);
                        }
                        Datum::from_usize(scratch.as_ptr() as usize)
                    }
                    other => panic!(
                        "execexpr EEOP_SQLVALUEFUNCTION: op {other:?} unported \
                         (CURRENT_CATALOG/CURRENT_SCHEMA — dbcommands/namespace lanes)"
                    ),
                };
                write_out(*out, value, false);
            }
            Step::Jump { jumpdone } => {
                // SAFETY: jump targets validated < steps.len() at ready.
                sp = unsafe { base.add(*jumpdone as usize) };
                continue;
            }
            Step::JumpIfNotTrue { jumpdone, out } => {
                let r = read_out(*out);
                if r.isnull || !r.value.as_bool() {
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::JumpIfNotNull { jumpdone, out } => {
                let r = read_out(*out);
                if !r.isnull {
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::JumpIfNull { jumpdone, out } => {
                if read_out(*out).isnull {
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::CaseTestVal { slot, out } => {
                // SAFETY: compile-allocated workspace, live for 'mcx.
                let nd = unsafe { slot.read() };
                write_out(*out, nd.value, nd.isnull);
            }
            Step::MakeReadonly { slot } => {
                // SAFETY: compile-allocated workspace holding a live datum.
                unsafe {
                    let nd = slot.read();
                    if !nd.isnull {
                        slot.write(NullableDatum {
                            value: datum::expandeddatum::make_expanded_object_read_only_internal(
                                nd.value,
                            ),
                            isnull: false,
                        });
                    }
                }
            }
            Step::ArrayExprEval { state, out } => {
                // SAFETY: compile-allocated state, live for 'mcx, sole access.
                let st = unsafe { &mut *state.as_ptr() };
                let r = crate::arrayops::eval_array_expr(st)?;
                write_out(*out, r.value, r.isnull);
            }
            Step::SbsrefSubscripts { state, jumpdone, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                if !crate::arrayops::sbsref_check_subscripts(st)? {
                    write_out(*out, Datum::null(), true);
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::SbsrefFetch { state, slice, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                let r = if *slice {
                    crate::arrayops::sbsref_fetch_slice(st, cur)?
                } else {
                    crate::arrayops::sbsref_fetch(st, cur)?
                };
                write_out(*out, r.value, r.isnull);
            }
            Step::SbsrefOld { state, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                crate::arrayops::sbsref_fetch_old(st, cur)?;
            }
            Step::SbsrefAssign { state, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                let r = crate::arrayops::sbsref_assign(st, cur)?;
                write_out(*out, r.value, r.isnull);
            }
            Step::Qual { jumpdone } => {
                // SAFETY: res is the state's live result cell.
                let r = unsafe { res.read() };
                if r.isnull || !r.value.as_bool() {
                    // SAFETY: as above.
                    unsafe {
                        res.write(NullableDatum { value: Datum::from_bool(false), isnull: false })
                    };
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::BoolAndStepFirst { anynull, jumpdone, out }
            | Step::BoolAndStep { anynull, jumpdone, out } => {
                if matches!(step, Step::BoolAndStepFirst { .. }) {
                    // SAFETY: compile-allocated scratch, live for 'mcx.
                    unsafe { anynull.write(false) };
                }
                let r = read_out(*out);
                if r.isnull {
                    // SAFETY: as above.
                    unsafe { anynull.write(true) };
                } else if !r.value.as_bool() {
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::BoolAndStepLast { anynull, out } => {
                let r = read_out(*out);
                // SAFETY: compile-allocated scratch, live for 'mcx.
                if !r.isnull && r.value.as_bool() && unsafe { anynull.read() } {
                    write_out(*out, Datum::null(), true);
                }
            }
            Step::BoolOrStepFirst { anynull, jumpdone, out }
            | Step::BoolOrStep { anynull, jumpdone, out } => {
                if matches!(step, Step::BoolOrStepFirst { .. }) {
                    // SAFETY: compile-allocated scratch, live for 'mcx.
                    unsafe { anynull.write(false) };
                }
                let r = read_out(*out);
                if r.isnull {
                    // SAFETY: as above.
                    unsafe { anynull.write(true) };
                } else if r.value.as_bool() {
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::BoolOrStepLast { anynull, out } => {
                let r = read_out(*out);
                // SAFETY: compile-allocated scratch, live for 'mcx.
                if !r.isnull && !r.value.as_bool() && unsafe { anynull.read() } {
                    write_out(*out, Datum::null(), true);
                }
            }
            Step::BoolNotStep { out } => {
                // NULL in gives NULL out: isnull rides through untouched (C
                // flips the datum even when nominally null).
                let r = read_out(*out);
                write_out(*out, Datum::from_bool(!r.value.as_bool()), r.isnull);
            }
            Step::NullTestRowIsNull { rn, frame, out } => {
                let r = read_out(*out);
                let b = eval_row_null(frames, *rn, *frame, r, true)?;
                write_out(*out, Datum::from_bool(b), false);
            }
            Step::NullTestRowIsNotNull { rn, frame, out } => {
                let r = read_out(*out);
                let b = eval_row_null(frames, *rn, *frame, r, false)?;
                write_out(*out, Datum::from_bool(b), false);
            }
            Step::FieldSelect { fieldnum, resulttype, frame, out } => {
                let r = read_out(*out);
                if !r.isnull {
                    let (value, isnull) =
                        eval_field_select(frames, *fieldnum, *resulttype, *frame, r.value)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::ArrayCoerce { state: acs, out } => {
                let r = read_out(*out);
                if !r.isnull {
                    // SAFETY: compile-allocated state, sole live access.
                    let st = unsafe { &mut *acs.as_ptr() };
                    let nd = crate::arrayops::eval_array_coerce(st, r.value)?;
                    write_out(*out, nd.value, nd.isnull);
                }
            }
            Step::ConvertRowtype { state: crs, frame, out } => {
                let r = read_out(*out);
                if !r.isnull {
                    // SAFETY: compile-allocated state, sole live access.
                    let st = unsafe { crs.as_ref() };
                    let v = eval_convert_rowtype(frames, st, *frame, r.value)?;
                    write_out(*out, v, false);
                }
            }
            Step::NullTestIsNull { out } => {
                let r = read_out(*out);
                write_out(*out, Datum::from_bool(r.isnull), false);
            }
            Step::NullTestIsNotNull { out } => {
                let r = read_out(*out);
                write_out(*out, Datum::from_bool(!r.isnull), false);
            }
            Step::MakeReadonlyOut { src, out } => {
                let r = read_out(*src);
                let v = if r.isnull {
                    r.value
                } else {
                    // SAFETY: non-null by-ref datum of a varlena-typed domain
                    // input (compile emits this step only for typlen -1).
                    unsafe { ::datum::expandeddatum::make_expanded_object_read_only_internal(r.value) }
                };
                write_out(*out, v, r.isnull);
            }
            Step::DomainTestval { src, out } => {
                let r = read_out(*src);
                write_out(*out, r.value, r.isnull);
            }
            Step::DomainNotNull { resulttype, out } => {
                if read_out(*out).isnull {
                    return Err(domain_not_null_violation(*resulttype));
                }
            }
            Step::DomainCheck { resulttype, name, check } => {
                // SAFETY: compile-allocated scratch, live for 'mcx.
                let r = unsafe { check.read() };
                if !r.isnull && !r.value.as_bool() {
                    // SAFETY: name is a compile-copied &'mcx str.
                    return Err(domain_check_violation(*resulttype, unsafe { name.as_ref() }));
                }
            }
            Step::AggStrictInputCheck { args, nargs, jumpnull } => {
                // SAFETY: args[0..nargs] live fcinfo slots; jumps ready-checked.
                let anynull = (0..*nargs as usize)
                    .any(|i| unsafe { args.as_ptr().add(i).read().isnull });
                if anynull {
                    sp = unsafe { base.add(*jumpnull as usize) };
                    continue;
                }
            }
            Step::AggStrictInputCheck1 { arg, jumpnull } => {
                // SAFETY: as AggStrictInputCheck.
                if unsafe { arg.read().isnull } {
                    sp = unsafe { base.add(*jumpnull as usize) };
                    continue;
                }
            }
            Step::AggOrderedMark { flag } => {
                // SAFETY: nodeagg-owned once-allocated flag slot.
                unsafe { flag.write(true) };
            }
            Step::AggrefEval { value, null, out } => {
                // SAFETY: pointers into once-allocated AggState arrays (steps.rs note).
                let (v, n) = unsafe { (value.read(), null.read()) };
                write_out(*out, v, n);
            }
            Step::GroupingFuncEval { cols, ncols, current, out } => {
                let mut result: i64 = 0;
                if let Some(cell) = current {
                    // SAFETY: once-allocated AggState arrays, repointed
                    // before projection.
                    let (grouped, cols) = unsafe {
                        let c = cell.read();
                        (
                            core::slice::from_raw_parts(c.ptr, c.len),
                            core::slice::from_raw_parts(cols.as_ptr(), *ncols as usize),
                        )
                    };
                    for &attno in cols {
                        result <<= 1;
                        if !grouped.contains(&(attno as i16)) {
                            result |= 1;
                        }
                    }
                }
                write_out(*out, Datum::from_i32(result as i32), false);
            }
            Step::AggPlainTransByVal { call, pergroup } => {
                // SAFETY: once-allocated stable pergroup; sole access here.
                unsafe {
                    let pg = pergroup.as_ptr();
                    crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                        value: (*pg).trans_value,
                        isnull: (*pg).trans_value_is_null,
                    });
                    let (value, isnull) = invoke(call)?;
                    (*pg).trans_value = value;
                    (*pg).trans_value_is_null = isnull;
                }
            }
            Step::AggPlainTransStrictByVal { call, pergroup } => {
                // SAFETY: as AggPlainTransByVal.
                unsafe {
                    let pg = pergroup.as_ptr();
                    if !(*pg).trans_value_is_null {
                        crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                            value: (*pg).trans_value,
                            isnull: false,
                        });
                        let (value, isnull) = invoke(call)?;
                        (*pg).trans_value = value;
                        (*pg).trans_value_is_null = isnull;
                    }
                }
            }
            Step::AggPlainTransInitStrictByVal { call, pergroup } => {
                unsafe {
                    let pg = pergroup.as_ptr();
                    if (*pg).no_trans_value {
                        let a1 = crate::steps::arg_slot_of(call.fcinfo, 1).read();
                        (*pg).trans_value = a1.value;
                        (*pg).trans_value_is_null = false;
                        (*pg).no_trans_value = false;
                    } else if !(*pg).trans_value_is_null {
                        crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                            value: (*pg).trans_value,
                            isnull: false,
                        });
                        let (value, isnull) = invoke(call)?;
                        (*pg).trans_value = value;
                        (*pg).trans_value_is_null = isnull;
                    }
                }
            }
            Step::AggTransInitStrictByValIndirect { call, base, transno } => {
                // SAFETY: as AggTransByValIndirect.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    if (*pg).no_trans_value {
                        let a1 = crate::steps::arg_slot_of(call.fcinfo, 1).read();
                        (*pg).trans_value = a1.value;
                        (*pg).trans_value_is_null = false;
                        (*pg).no_trans_value = false;
                    } else if !(*pg).trans_value_is_null {
                        crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                            value: (*pg).trans_value,
                            isnull: false,
                        });
                        let (value, isnull) = invoke(call)?;
                        (*pg).trans_value = value;
                        (*pg).trans_value_is_null = isnull;
                    }
                }
            }
            Step::AggTransByValIndirect { call, base, transno } => {
                // SAFETY: base is a live cell nodeAgg repoints at the current
                // group's once-allocated pergroup array before evaluation;
                // transno < that array's length (build invariant).
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                        value: (*pg).trans_value,
                        isnull: (*pg).trans_value_is_null,
                    });
                    let (value, isnull) = invoke(call)?;
                    (*pg).trans_value = value;
                    (*pg).trans_value_is_null = isnull;
                }
            }
            Step::AggTransStrictByValIndirect { call, base, transno } => {
                // SAFETY: as AggTransByValIndirect.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    if !(*pg).trans_value_is_null {
                        crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                            value: (*pg).trans_value,
                            isnull: false,
                        });
                        let (value, isnull) = invoke(call)?;
                        (*pg).trans_value = value;
                        (*pg).trans_value_is_null = isnull;
                    }
                }
            }
            Step::AggPlainTransInitStrictByRef { call, pergroup, byref } => {
                // SAFETY: once-allocated stable pergroup, sole access here.
                unsafe {
                    let pg = pergroup.as_ptr();
                    if (*pg).no_trans_value {
                        agg_init_group(call, pg, *byref)?;
                    } else if !(*pg).trans_value_is_null {
                        agg_plain_trans_byref(call, pg, *byref)?;
                    }
                }
            }
            Step::AggPlainTransStrictByRef { call, pergroup, byref } => {
                // SAFETY: as AggPlainTransInitStrictByRef.
                unsafe {
                    let pg = pergroup.as_ptr();
                    if !(*pg).trans_value_is_null {
                        agg_plain_trans_byref(call, pg, *byref)?;
                    }
                }
            }
            Step::AggPlainTransByRef { call, pergroup, byref } => {
                // SAFETY: as AggPlainTransInitStrictByRef.
                unsafe { agg_plain_trans_byref(call, pergroup.as_ptr(), *byref)? }
            }
            Step::AggTransInitStrictByRefIndirect { call, base, transno, byref } => {
                // SAFETY: as AggTransByValIndirect + AggPlainTransByRef.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    if (*pg).no_trans_value {
                        agg_init_group(call, pg, *byref)?;
                    } else if !(*pg).trans_value_is_null {
                        agg_plain_trans_byref(call, pg, *byref)?;
                    }
                }
            }
            Step::AggTransStrictByRefIndirect { call, base, transno, byref } => {
                // SAFETY: as AggTransInitStrictByRefIndirect.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    if !(*pg).trans_value_is_null {
                        agg_plain_trans_byref(call, pg, *byref)?;
                    }
                }
            }
            Step::AggTransByRefIndirect { call, base, transno, byref } => {
                // SAFETY: as AggTransInitStrictByRefIndirect.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    agg_plain_trans_byref(call, pg, *byref)?
                }
            }
            Step::HashDatumSetInitVal { init_value, out } => {
                write_out(*out, *init_value, false);
            }
            Step::HashDatumFirst { call, out } => {
                // SAFETY: arg 0 of the call's live fcinfo image; hash fns
                // never return NULL (C reads fn_addr's Datum directly).
                let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
                let v = if a0.isnull { Datum::null() } else { invoke(call)?.0 };
                write_out(*out, v, false);
            }
            Step::HashDatumNext32 { call, iresult, out } => {
                // SAFETY: iresult is a build-owned once-allocated slot; arg 0
                // as HashDatumFirst.
                let existing = unsafe { iresult.read() }.value.as_u32().rotate_left(1);
                let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
                let combined = if a0.isnull {
                    existing
                } else {
                    existing ^ invoke(call)?.0.as_u32()
                };
                write_out(*out, Datum::from_u32(combined), false);
            }
            Step::BoolTestIsTrue { out } => {
                let r = read_out(*out);
                let v = if r.isnull { false } else { r.value.as_bool() };
                write_out(*out, Datum::from_bool(v), false);
            }
            Step::BoolTestIsNotTrue { out } => {
                let r = read_out(*out);
                let v = if r.isnull { true } else { !r.value.as_bool() };
                write_out(*out, Datum::from_bool(v), false);
            }
            Step::BoolTestIsFalse { out } => {
                let r = read_out(*out);
                let v = if r.isnull { false } else { !r.value.as_bool() };
                write_out(*out, Datum::from_bool(v), false);
            }
            Step::BoolTestIsNotFalse { out } => {
                let r = read_out(*out);
                let v = if r.isnull { true } else { r.value.as_bool() };
                write_out(*out, Datum::from_bool(v), false);
            }
            Step::Distinct { call, out } => {
                // SAFETY: args 0/1 of the call's live fcinfo image.
                let (a0, a1) = unsafe {
                    (
                        crate::steps::arg_slot_of(call.fcinfo, 0).read(),
                        crate::steps::arg_slot_of(call.fcinfo, 1).read(),
                    )
                };
                if a0.isnull && a1.isnull {
                    write_out(*out, Datum::from_bool(false), false);
                } else if a0.isnull || a1.isnull {
                    write_out(*out, Datum::from_bool(true), false);
                } else {
                    let (value, isnull) = invoke(call)?;
                    write_out(*out, Datum::from_bool(!value.as_bool()), isnull);
                }
            }
            Step::RowCompareStep { call, strict, jumpnull, jumpdone, out } => {
                match eval_row_compare_step(call, *strict)? {
                    None => {
                        write_out(*out, Datum::null(), true);
                        // SAFETY: jump targets validated < steps.len() at ready.
                        sp = unsafe { base.add(*jumpnull as usize) };
                        continue;
                    }
                    Some(v) => {
                        write_out(*out, Datum::from_i32(v), false);
                        if v != 0 {
                            // SAFETY: jump targets validated < steps.len() at ready.
                            sp = unsafe { base.add(*jumpdone as usize) };
                            continue;
                        }
                    }
                }
            }
            Step::RowCompareFinal { cmptype, out } => {
                let v = eval_row_compare_final(*cmptype, read_out(*out).value.as_i32());
                write_out(*out, Datum::from_bool(v), false);
            }
            Step::ScanVarFuncStrict2 { attnum, argno, call, out, .. } => {
                let nd = read_var(need_slot(&mut scan), *attnum);
                // SAFETY: argno/1-argno are args 0/1 of the live fcinfo image.
                let other = unsafe {
                    crate::steps::arg_slot_of(call.fcinfo, *argno as usize).write(nd);
                    crate::steps::arg_slot_of(call.fcinfo, 1 - *argno as usize).read()
                };
                if nd.isnull || other.isnull {
                    write_out(*out, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke2(call)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::FuncFuncStrict2 { call1, argno, call2, out } => {
                let r1 = strict2_eval(call1)?;
                // SAFETY: as ScanVarFuncStrict2, for call2's image.
                let other = unsafe {
                    crate::steps::arg_slot_of(call2.fcinfo, *argno as usize).write(r1);
                    crate::steps::arg_slot_of(call2.fcinfo, 1 - *argno as usize).read()
                };
                if r1.isnull || other.isnull {
                    write_out(*out, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke2(call2)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::FuncStrict2Qual { call, jumpdone, out } => {
                let r = strict2_eval(call)?;
                if r.isnull || !r.value.as_bool() {
                    write_out(*out, Datum::from_bool(false), false);
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
                write_out(*out, r.value, r.isnull);
            }
            Step::OuterVarNotDistinct { attnum, argno, call, out, .. } => {
                let nd = read_var(need_slot(&mut outer), *attnum);
                // SAFETY: as ScanVarFuncStrict2.
                let other = unsafe {
                    crate::steps::arg_slot_of(call.fcinfo, *argno as usize).write(nd);
                    crate::steps::arg_slot_of(call.fcinfo, 1 - *argno as usize).read()
                };
                if nd.isnull && other.isnull {
                    write_out(*out, Datum::from_bool(true), false);
                } else if nd.isnull || other.isnull {
                    write_out(*out, Datum::from_bool(false), false);
                } else {
                    let (value, isnull) = invoke2(call)?;
                    write_out(*out, value, isnull);
                }
            }
            Step::NotDistinctQual { call, jumpdone, out } => {
                // SAFETY: args 0/1 of the call's live fcinfo image.
                let (a0, a1) = unsafe {
                    (
                        crate::steps::arg_slot_of(call.fcinfo, 0).read(),
                        crate::steps::arg_slot_of(call.fcinfo, 1).read(),
                    )
                };
                let r = if a0.isnull && a1.isnull {
                    NullableDatum { value: Datum::from_bool(true), isnull: false }
                } else if a0.isnull || a1.isnull {
                    NullableDatum { value: Datum::from_bool(false), isnull: false }
                } else {
                    let (value, isnull) = invoke2(call)?;
                    NullableDatum { value, isnull }
                };
                if r.isnull || !r.value.as_bool() {
                    write_out(*out, Datum::from_bool(false), false);
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
                write_out(*out, r.value, r.isnull);
            }
            Step::OuterVarAggTransByValIndirect { attnum, argno, call, base: pgbase, transno, .. } => {
                let nd = read_var(need_slot(&mut outer), *attnum);
                // SAFETY: as ScanVarFuncStrict2 + AggTransByValIndirect.
                unsafe {
                    crate::steps::arg_slot_of(call.fcinfo, *argno as usize).write(nd);
                    let pg = pgbase.read().as_ptr().add(*transno as usize);
                    crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                        value: (*pg).trans_value,
                        isnull: (*pg).trans_value_is_null,
                    });
                    let (value, isnull) = invoke2(call)?;
                    (*pg).trans_value = value;
                    (*pg).trans_value_is_null = isnull;
                }
            }
            Step::AssignScanVar2 { attnum1, resultnum1, attnum2, resultnum2 } => {
                let nd1 = read_var(need_slot(&mut scan), *attnum1);
                let nd2 = read_var(need_slot(&mut scan), *attnum2);
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                assign_to_result(rslot, *resultnum1, nd1.value, nd1.isnull);
                assign_to_result(rslot, *resultnum2, nd2.value, nd2.isnull);
            }
            Step::FuncExprStrict1Thin { call, out } => {
                // SAFETY: arg 0 of the call's live fcinfo image.
                let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
                if a0.isnull {
                    write_out(*out, Datum::null(), true);
                } else {
                    write_out(*out, invoke_thin(call)?, false);
                }
            }
            Step::FuncExprStrict2Thin { call, out } => {
                let r = strict2_thin_eval(call)?;
                write_out(*out, r.value, r.isnull);
            }
            Step::ScanVarFuncStrict2Thin { attnum, argno, call, out, .. } => {
                let nd = read_var(need_slot(&mut scan), *attnum);
                // SAFETY: argno/1-argno are args 0/1 of the live fcinfo image.
                let other = unsafe {
                    crate::steps::arg_slot_of(call.fcinfo, *argno as usize).write(nd);
                    crate::steps::arg_slot_of(call.fcinfo, 1 - *argno as usize).read()
                };
                if nd.isnull || other.isnull {
                    write_out(*out, Datum::null(), true);
                } else {
                    write_out(*out, invoke_thin(call)?, false);
                }
            }
            Step::FuncFuncStrict2Thin { call1, argno, call2, out } => {
                let r1 = strict2_thin_eval(call1)?;
                // SAFETY: as ScanVarFuncStrict2, for call2's image.
                let other = unsafe {
                    crate::steps::arg_slot_of(call2.fcinfo, *argno as usize).write(r1);
                    crate::steps::arg_slot_of(call2.fcinfo, 1 - *argno as usize).read()
                };
                if r1.isnull || other.isnull {
                    write_out(*out, Datum::null(), true);
                } else {
                    write_out(*out, invoke_thin(call2)?, false);
                }
            }
            Step::FuncStrict2QualThin { call, jumpdone, out } => {
                let r = strict2_thin_eval(call)?;
                if r.isnull || !r.value.as_bool() {
                    write_out(*out, Datum::from_bool(false), false);
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
                write_out(*out, r.value, r.isnull);
            }
            Step::OuterVarNotDistinctThin { attnum, argno, call, out, .. } => {
                let nd = read_var(need_slot(&mut outer), *attnum);
                // SAFETY: as ScanVarFuncStrict2.
                let other = unsafe {
                    crate::steps::arg_slot_of(call.fcinfo, *argno as usize).write(nd);
                    crate::steps::arg_slot_of(call.fcinfo, 1 - *argno as usize).read()
                };
                if nd.isnull && other.isnull {
                    write_out(*out, Datum::from_bool(true), false);
                } else if nd.isnull || other.isnull {
                    write_out(*out, Datum::from_bool(false), false);
                } else {
                    write_out(*out, invoke_thin(call)?, false);
                }
            }
            Step::NotDistinctQualThin { call, jumpdone, out } => {
                // SAFETY: args 0/1 of the call's live fcinfo image.
                let (a0, a1) = unsafe {
                    (
                        crate::steps::arg_slot_of(call.fcinfo, 0).read(),
                        crate::steps::arg_slot_of(call.fcinfo, 1).read(),
                    )
                };
                let v = if a0.isnull && a1.isnull {
                    Datum::from_bool(true)
                } else if a0.isnull || a1.isnull {
                    Datum::from_bool(false)
                } else {
                    invoke_thin(call)?
                };
                if !v.as_bool() {
                    write_out(*out, Datum::from_bool(false), false);
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
                write_out(*out, v, false);
            }
            Step::AggTransStrictByValIndirectThin { call, base, transno } => {
                // SAFETY: as AggTransByValIndirect; thin callee never sets isnull.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    if !(*pg).trans_value_is_null {
                        crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                            value: (*pg).trans_value,
                            isnull: false,
                        });
                        (*pg).trans_value = invoke_thin(call)?;
                        (*pg).trans_value_is_null = false;
                    }
                }
            }
            Step::NotDistinct { call, out } => {
                // SAFETY: args 0/1 of the call's live fcinfo image.
                let (a0, a1) = unsafe {
                    (
                        crate::steps::arg_slot_of(call.fcinfo, 0).read(),
                        crate::steps::arg_slot_of(call.fcinfo, 1).read(),
                    )
                };
                if a0.isnull && a1.isnull {
                    write_out(*out, Datum::from_bool(true), false);
                } else if a0.isnull || a1.isnull {
                    write_out(*out, Datum::from_bool(false), false);
                } else {
                    let (value, isnull) = invoke(call)?;
                    write_out(*out, value, isnull);
                }
            }
        }
        // SAFETY: Done-termination validated; +1 stays in bounds.
        sp = unsafe { sp.add(1) };
    }
}

#[inline(always)]
fn need_slot<'a, 'b, 'mcx>(
    slot: &'a mut Option<&'b mut SlotData<'mcx>>,
) -> &'a mut SlotData<'mcx> {
    match slot {
        Some(s) => s,
        None => missing_slot_hoisted(),
    }
}

#[cold]
#[inline(never)]
fn missing_slot_hoisted() -> ! {
    panic!("execexpr: expression references a slot that was not supplied")
}

// ExecEvalScalarArrayOp (execExprInterp.c): in-place walk of the array
// image; the scalar operand sits in args[0], each element lands in args[1].
#[allow(clippy::too_many_arguments)]
fn eval_scalar_array_op(
    call: &FuncCall,
    use_or: bool,
    strict: bool,
    typlen: i16,
    typbyval: bool,
    typalign: u8,
    arr: NullableDatum,
) -> PgResult<(Datum, bool)> {
    if arr.isnull {
        return Ok((Datum::null(), true));
    }
    let p = arr.value.as_usize() as *const u8;
    // DatumGetArrayTypeP: borrow in place on an inline 4-byte header, else
    // detoast/unpack a copy into the armed per-eval result context (C's
    // CurrentMemoryContext at eval).
    // SAFETY: non-null array datum addresses a live varlena.
    let img: &[u8] = unsafe {
        if ::types_tuple::varatt::varatt_is_4b_u(p) {
            core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
        } else {
            let raw = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p));
            let mcx = crate::steps::fcinfo_mut(call.fcinfo, call.nargs).result_mcx();
            let flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
            &*(flat.leak() as *const [u8])
        }
    };
    let (ndim, dims, _lbs) = ::arrayfuncs::foundation::read_dims_lbounds(img);
    let mut nitems = 1i64;
    for d in &dims[..ndim as usize] {
        nitems *= *d as i64;
    }
    if ndim == 0 {
        nitems = 0;
    }

    // SAFETY: arg slot 0 of the call's live fcinfo image.
    let scalar = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
    if scalar.isnull && strict {
        return Ok((Datum::null(), true));
    }

    let mut result = !use_or;
    let mut resultnull = false;
    let bitmap_off = ::arrayfuncs::foundation::arr_nullbitmap_off(img);
    let mut off = ::arrayfuncs::foundation::arr_data_offset(img);
    let mut bitmask: u32 = 1;
    let mut bitmap_byte = 0usize;

    for _ in 0..nitems {
        let elt_null = match bitmap_off {
            Some(bo) => (img[bo + bitmap_byte] as u32 & bitmask) == 0,
            None => false,
        };
        let (elt, this_null) = if elt_null {
            (Datum::null(), true)
        } else {
            off = ::arrayfuncs::foundation::att_align_nominal(off, typalign);
            // SAFETY: off stays within the VARSIZE image per the array layout.
            let ep = unsafe { img.as_ptr().add(off) };
            let elt = ::arrayfuncs::foundation::fetch_att(ep, typbyval, typlen as i32);
            off = ::arrayfuncs::foundation::att_addlength_pointer(off, typlen as i32, ep);
            (elt, false)
        };

        let (thisresult, thisnull) = if strict && (this_null || scalar.isnull) {
            (Datum::null(), true)
        } else {
            // SAFETY: arg slot 1 of the call's live fcinfo image.
            unsafe {
                crate::steps::arg_slot_of(call.fcinfo, 1)
                    .write(NullableDatum { value: elt, isnull: this_null })
            };
            invoke(call)?
        };

        if thisnull {
            resultnull = true;
        } else if use_or {
            if thisresult.as_bool() {
                return Ok((Datum::from_bool(true), false));
            }
        } else if !thisresult.as_bool() {
            return Ok((Datum::from_bool(false), false));
        }

        if bitmap_off.is_some() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmask = 1;
                bitmap_byte += 1;
            }
        }
    }

    if resultnull {
        return Ok((Datum::null(), true));
    }
    Ok((Datum::from_bool(result), false))
}

// ExecEvalHashedScalarArrayOp (execExprInterp.c): OR-semantics probe against
// a table of the const array's elements, built on first evaluation.
#[allow(clippy::too_many_arguments)]
fn eval_hashed_scalar_array_op(
    tab: &mut crate::steps::SaopTable<'_>,
    call: &FuncCall,
    inclause: bool,
    typlen: i16,
    typbyval: bool,
    typalign: u8,
    arr: NullableDatum,
) -> PgResult<(Datum, bool)> {
    // The planner only converts a non-null Const array.
    debug_assert!(!arr.isnull);
    // SAFETY: 'mcx-live mcx-boxed FmgrInfo the step's carrier points at.
    let strictfunc = unsafe { call.flinfo.as_ref() }.fn_strict;
    // SAFETY: arg slot 0 of the call's live fcinfo image.
    let scalar = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };

    if scalar.isnull && strictfunc {
        return Ok((Datum::null(), true));
    }

    let hash_of = |hashcall: &FuncCall, v: Datum| -> PgResult<u32> {
        // SAFETY: arg slot 0 of the hashcall's live fcinfo image.
        unsafe {
            crate::steps::arg_slot_of(hashcall.fcinfo, 0)
                .write(NullableDatum { value: v, isnull: false })
        };
        let (h, _) = invoke(hashcall)?;
        Ok(h.as_i32() as u32)
    };
    let eq_of = |call: &FuncCall, a: Datum, b: Datum| -> PgResult<bool> {
        // SAFETY: arg slots 0/1 of the call's live fcinfo image.
        unsafe {
            crate::steps::arg_slot_of(call.fcinfo, 0)
                .write(NullableDatum { value: a, isnull: false });
            crate::steps::arg_slot_of(call.fcinfo, 1)
                .write(NullableDatum { value: b, isnull: false });
        }
        let (r, _) = invoke(call)?;
        Ok(r.as_bool())
    };

    if !tab.built {
        let hashcall = tab.hashcall;
        let p = arr.value.as_usize() as *const u8;
        // DatumGetArrayTypeP (as the non-hashed SAOP walk): borrow in place on
        // an inline 4-byte header, else detoast/unpack into the table's mcx.
        // SAFETY: non-null array datum addresses a live varlena.
        let img: &[u8] = unsafe {
            if ::types_tuple::varatt::varatt_is_4b_u(p) {
                core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
            } else {
                let raw = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p));
                let flat = ::detoast_seams::detoast_attr::call(*tab.map.allocator(), raw)?;
                &*(flat.leak() as *const [u8])
            }
        };
        let (ndim, dims, _lbs) = ::arrayfuncs::foundation::read_dims_lbounds(img);
        let mut nitems = 1i64;
        for d in &dims[..ndim as usize] {
            nitems *= *d as i64;
        }
        if ndim == 0 {
            nitems = 0;
        }
        let bitmap_off = ::arrayfuncs::foundation::arr_nullbitmap_off(img);
        let mut off = ::arrayfuncs::foundation::arr_data_offset(img);
        let mut bitmask: u32 = 1;
        let mut bitmap_byte = 0usize;
        let mcx = *tab.map.allocator();
        for _ in 0..nitems {
            let elt_null = match bitmap_off {
                Some(bo) => (img[bo + bitmap_byte] as u32 & bitmask) == 0,
                None => false,
            };
            if elt_null {
                tab.has_nulls = true;
            } else {
                off = ::arrayfuncs::foundation::att_align_nominal(off, typalign);
                // SAFETY: off stays within the VARSIZE image per the array layout.
                let ep = unsafe { img.as_ptr().add(off) };
                let elt = ::arrayfuncs::foundation::fetch_att(ep, typbyval, typlen as i32);
                off = ::arrayfuncs::foundation::att_addlength_pointer(off, typlen as i32, ep);

                let h = hash_of(&hashcall, elt)?;
                let bucket = tab
                    .map
                    .entry(h)
                    .or_insert_with(|| ::mcx::PgVec::new_in(mcx));
                let mut found = false;
                for i in 0..bucket.len() {
                    if eq_of(call, elt, bucket[i])? {
                        found = true;
                        break;
                    }
                }
                if !found {
                    bucket.push(elt);
                }
            }
            if bitmap_off.is_some() {
                bitmask <<= 1;
                if bitmask == 0x100 {
                    bitmask = 1;
                    bitmap_byte += 1;
                }
            }
        }
        tab.built = true;
    }

    // Probe (C probes even a null non-strict scalar, value word 0).
    let mut hashfound = false;
    {
        let h = hash_of(&tab.hashcall, scalar.value)?;
        if let Some(bucket) = tab.map.get(&h) {
            for i in 0..bucket.len() {
                if eq_of(call, scalar.value, bucket[i])? {
                    hashfound = true;
                    break;
                }
            }
        }
    }

    let mut result = if inclause { hashfound } else { !hashfound };
    let mut resultnull = false;

    // No match + nulls in the array: strict fns yield NULL; non-strict fns
    // get one call with a null rhs (result negated for NOT IN).
    if !hashfound && tab.has_nulls {
        if strictfunc {
            return Ok((Datum::null(), true));
        }
        // SAFETY: arg slots 0/1 of the call's live fcinfo image.
        unsafe {
            crate::steps::arg_slot_of(call.fcinfo, 0).write(scalar);
            crate::steps::arg_slot_of(call.fcinfo, 1).write(NullableDatum::null());
        }
        let (r, isnull) = invoke(call)?;
        result = r.as_bool();
        resultnull = isnull;
        if !inclause {
            result = !result;
        }
    }

    if resultnull {
        return Ok((Datum::null(), true));
    }
    Ok((Datum::from_bool(result), false))
}

// C ExecEvalRow (execExprInterp.c): form the composite in the armed
// per-eval result context; the header carries the blessed RECORD typmod.
fn eval_row_expr(
    frames: &mut [crate::steps::FuncFrame<'_>],
    elems: core::ptr::NonNull<NullableDatum>,
    nelems: u16,
    frame: u32,
    desc: core::ptr::NonNull<::types_tuple::TupleDescData<'static>>,
) -> PgResult<(Datum, bool)> {
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let n = nelems as usize;
    // SAFETY: n scratch slots written by the element steps just executed.
    let src = unsafe { core::slice::from_raw_parts(elems.as_ptr(), n) };
    let mut values: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: ::mcx::PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for nd in src {
        values.push(nd.value);
        nulls.push(nd.isnull);
    }
    // SAFETY: the compile-time blessed tupdesc is plan-mcx-lived.
    let desc = unsafe { desc.as_ref() };
    let tuple = ::heaptuple::heap_form_tuple(mcx, desc, &values, &nulls)?;
    let d = Datum::from_usize(tuple.image().as_ptr() as usize);
    core::mem::forget(tuple);
    Ok((d, false))
}

// Out of line: json steps are cold relative to the dispatch loop; keeping the
// arm a bare call protects the loop's register allocation (graviton.md flat
// interpreter rule; M3 A/B measured the fat-arm form at +0.3-1.8% instr on
// interpreter-bound lanes).
#[inline(never)]
fn eval_json_constructor_step(
    frames: &mut [crate::steps::FuncFrame<'_>],
    jcstate: core::ptr::NonNull<crate::steps::JsonConstructorState>,
    frame: u32,
    out: crate::steps::OutRef,
) -> PgResult<()> {
    // SAFETY: plan-mcx state, exclusive during this step.
    let jc = unsafe { jcstate.as_ref() };
    let (value, isnull) = eval_json_constructor(frames, jc, frame)?;
    write_out(out, value, isnull);
    Ok(())
}

#[inline(never)]
fn eval_is_json_step(
    frames: &mut [crate::steps::FuncFrame<'_>],
    exprtype: ::types_core::Oid,
    item_type: ::types_nodes::primnodes::JsonValueType,
    unique_keys: bool,
    frame: u32,
    out: crate::steps::OutRef,
) -> PgResult<()> {
    let nd = read_out(out);
    if nd.isnull {
        // C writes false into resvalue but leaves resnull set: NULL result.
        write_out(out, Datum::from_bool(false), true);
        return Ok(());
    }
    let res = eval_is_json(frames, nd.value, exprtype, item_type, unique_keys, frame)?;
    write_out(out, Datum::from_bool(res), false);
    Ok(())
}

// C ExecEvalJsonConstructor (execExprInterp.c:4657); results in the armed
// per-eval result context.
fn eval_json_constructor(
    frames: &mut [crate::steps::FuncFrame<'_>],
    jc: &crate::steps::JsonConstructorState,
    frame: u32,
) -> PgResult<(Datum, bool)> {
    use ::types_nodes::JsonConstructorType as JC;
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let n = jc.nargs as usize;
    // SAFETY: n compile-allocated slots, written by the arg steps just run;
    // values/nulls are same-size split scratch (exclusive during this step).
    unsafe {
        let src = core::slice::from_raw_parts(jc.slots.as_ptr(), n);
        for (i, nd) in src.iter().enumerate() {
            jc.values.as_ptr().add(i).write(nd.value);
            jc.nulls.as_ptr().add(i).write(nd.isnull);
        }
    }
    // SAFETY: just initialized above / at compile.
    let (values, nulls, types) = unsafe {
        (
            core::slice::from_raw_parts(jc.values.as_ptr(), n),
            core::slice::from_raw_parts(jc.nulls.as_ptr(), n),
            core::slice::from_raw_parts(jc.types.as_ptr(), n),
        )
    };

    match jc.ctor_type {
        JC::JSCTOR_JSON_ARRAY => {
            let d = if jc.is_jsonb {
                image_datum(::adt_jsonb::tojsonb::jsonb_build_array_worker(
                    mcx,
                    values,
                    nulls,
                    types,
                    jc.absent_on_null,
                )?)
            } else {
                varlena_datum(::adt_json::tojson::json_build_array_worker(
                    mcx,
                    values,
                    nulls,
                    types,
                    jc.absent_on_null,
                )?)
            };
            Ok((d, false))
        }
        JC::JSCTOR_JSON_OBJECT => {
            let d = if jc.is_jsonb {
                image_datum(::adt_jsonb::tojsonb::jsonb_build_object_worker(
                    mcx,
                    values,
                    nulls,
                    types,
                    jc.absent_on_null,
                    jc.unique,
                )?)
            } else {
                varlena_datum(::adt_json::tojson::json_build_object_worker(
                    mcx,
                    values,
                    nulls,
                    types,
                    jc.absent_on_null,
                    jc.unique,
                )?)
            };
            Ok((d, false))
        }
        JC::JSCTOR_JSON_SCALAR => {
            if nulls[0] {
                return Ok((Datum::null(), true));
            }
            if jc.is_jsonb {
                // SAFETY: compile-resolved carrier, exclusive during this step.
                let cat = unsafe { &mut *jc.scalar_jsonb.expect("scalar_jsonb").as_ptr() };
                Ok((image_datum(::adt_jsonb::tojsonb::datum_to_jsonb_cat(mcx, values[0], cat)?), false))
            } else {
                // SAFETY: compile-resolved carrier, exclusive during this step.
                let cat = unsafe { &mut *jc.scalar_json.expect("scalar_json").as_ptr() };
                Ok((varlena_datum(::adt_json::tojson::datum_to_json_cat(mcx, values[0], cat)?), false))
            }
        }
        JC::JSCTOR_JSON_PARSE => {
            // Reached only with unique_keys (the non-unique leg compiles to
            // the bare argument).
            if nulls[0] {
                return Ok((Datum::null(), true));
            }
            // SAFETY: values[0] is a live text datum from the arg step.
            let text = unsafe { ::types_fmgr::datum_varlena_packed(values[0], mcx)? };
            let js = text.data();
            if jc.is_jsonb {
                let image = ::adt_jsonb::io::jsonb_from_cstring(mcx, js, true, None)?
                    .expect("hard errsave without escontext returns Err");
                Ok((image_datum(image), false))
            } else {
                ::adt_json::funcs::json_validate(js, true, true)?;
                Ok((values[0], false))
            }
        }
        JC::JSCTOR_JSON_OBJECTAGG | JC::JSCTOR_JSON_ARRAYAGG | JC::JSCTOR_JSON_SERIALIZE => {
            panic!("invalid JsonConstructorExpr type {:?} in EEOP_JSON_CONSTRUCTOR", jc.ctor_type)
        }
    }
}

// C ExecEvalJsonIsPredicate (execExprInterp.c:4735).
fn eval_is_json(
    frames: &mut [crate::steps::FuncFrame<'_>],
    js: Datum,
    exprtype: ::types_core::Oid,
    item_type: ::types_nodes::primnodes::JsonValueType,
    unique_keys: bool,
    frame: u32,
) -> PgResult<bool> {
    use ::adt_json::jsonapi::JsonToken;
    use ::types_core::catalog::{JSONBOID, JSONOID, TEXTOID};
    use ::types_nodes::primnodes::JsonValueType as JT;

    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();

    if exprtype == TEXTOID || exprtype == JSONOID {
        // SAFETY: js is a live text/json varlena from the arg step.
        let text = unsafe { ::types_fmgr::datum_varlena_packed(js, mcx)? };
        let json = text.data();
        let mut res = if item_type == JT::JS_TYPE_ANY {
            true
        } else {
            match ::adt_jsonb::builtins::json_get_first_token(json)? {
                Some(JsonToken::ObjectStart) => item_type == JT::JS_TYPE_OBJECT,
                Some(JsonToken::ArrayStart) => item_type == JT::JS_TYPE_ARRAY,
                Some(
                    JsonToken::String
                    | JsonToken::Number
                    | JsonToken::True
                    | JsonToken::False
                    | JsonToken::Null,
                ) => item_type == JT::JS_TYPE_SCALAR,
                _ => false,
            }
        };
        // Full parse only for uniqueness check or json-text validation.
        if res && (unique_keys || exprtype == TEXTOID) {
            res = ::adt_json::funcs::json_validate(json, unique_keys, false)?;
        }
        Ok(res)
    } else if exprtype == JSONBOID {
        if item_type == JT::JS_TYPE_ANY {
            Ok(true)
        } else {
            // SAFETY: js is a live jsonb varlena from the arg step.
            let payload = unsafe { ::adt_jsonb::builtins::jsonb_payload_from_datum(mcx, js)? };
            let c = payload.as_bytes();
            Ok(match item_type {
                JT::JS_TYPE_OBJECT => ::adt_jsonb::container::container_is_object(c),
                JT::JS_TYPE_ARRAY => {
                    ::adt_jsonb::container::container_is_array(c)
                        && !::adt_jsonb::container::container_is_scalar(c)
                }
                JT::JS_TYPE_SCALAR => {
                    ::adt_jsonb::container::container_is_array(c)
                        && ::adt_jsonb::container::container_is_scalar(c)
                }
                JT::JS_TYPE_ANY => true,
            })
        }
    } else {
        Ok(false)
    }
}

fn image_datum(image: ::mcx::PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    d
}

fn varlena_datum(v: ::datum::Varlena<'_>) -> Datum {
    image_datum(v.into_image())
}

// ExecEvalFieldSelect (execExprInterp.c), heap-composite leg; the expanded-
// record fastpath is unported loud. C memoizes the tupdesc in the step's
// rowcache; a per-eval registry copy stands in (cold path, no invalidation).
#[inline(never)]
#[cold]
fn eval_field_select(
    frames: &mut [crate::steps::FuncFrame<'_>],
    fieldnum: i16,
    resulttype: ::types_core::Oid,
    frame: u32,
    value: Datum,
) -> PgResult<(Datum, bool)> {
    use ::types_tuple::{HeapTupleData, HeapTupleHeaderData, ItemPointerData};
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let p = value.as_usize() as *const u8;
    // SAFETY: non-null composite datum per the FieldSelect contract.
    if unsafe { ::types_tuple::varatt::varatt_is_external_expanded(p) } {
        panic!("ExecEvalFieldSelect (execExprInterp.c): expanded-record fastpath unported");
    }
    // SAFETY: a live varlena-headed composite image.
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: `total` readable bytes at p, per the datum contract.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let rec = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    // SAFETY: detoasted composite image; header prefix is in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
    let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, hdr.type_id(), hdr.typmod())?;
    if fieldnum <= 0 || fieldnum as i32 > tupdesc.natts {
        return Err(::types_error::PgError::error(format!(
            "attribute number {fieldnum} exceeds number of columns {}",
            tupdesc.natts
        ))
        .into());
    }
    let att = &tupdesc.attrs[(fieldnum - 1) as usize];
    if att.attisdropped {
        return Ok((Datum::null(), true));
    }
    if resulttype != att.atttypid {
        return Err(::types_error::PgError::error(format!(
            "attribute {fieldnum} has wrong type"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
        .into());
    }
    // SAFETY: MAXALIGN'd detoasted image of datum_length() bytes.
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ItemPointerData::invalid(),
            ::types_core::InvalidOid,
        )
    };
    let mut isnull = false;
    // SAFETY: fieldnum validated against the tuple's descriptor above.
    let v = unsafe { ::types_tuple::heap_getattr(&tuple, fieldnum as i32, &tupdesc, &mut isnull) };
    core::mem::forget(tuple);
    Ok((v, isnull))
}

// ExecEvalConvertRowtype (execExprInterp.c) + execute_attr_map_tuple
// (tupconvert.c); caller has handled the NULL case.
#[inline(never)]
#[cold]
fn eval_convert_rowtype(
    frames: &mut [crate::steps::FuncFrame<'_>],
    st: &crate::steps::ConvertRowtypeState,
    frame: u32,
    value: Datum,
) -> PgResult<Datum> {
    use ::types_tuple::{HeapTupleData, HeapTupleHeaderData, ItemPointerData};
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let p = value.as_usize() as *const u8;
    // SAFETY: non-null composite datum; detoast covers short/compressed forms.
    let raw = unsafe { core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p)) };
    let rec = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    // SAFETY: detoasted composite image; header prefix is in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
    // SAFETY: MAXALIGN'd detoasted image of datum_length() bytes.
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ItemPointerData::invalid(),
            ::types_core::InvalidOid,
        )
    };
    // SAFETY: plan-mcx tupdescs, live for every eval of this step.
    let indesc = unsafe { st.indesc.as_ref() };
    let outdesc = unsafe { st.outdesc.as_ref() };
    let result = match st.map {
        Some(map) => {
            // SAFETY: plan-mcx map slice.
            let map = unsafe { map.as_ref() };
            let innatts = indesc.natts as usize;
            let mut invalues: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, innatts)?;
            let mut innulls: ::mcx::PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, innatts)?;
            invalues.resize(innatts, Datum::null());
            innulls.resize(innatts, true);
            ::types_tuple::heap_deform_tuple(&tuple, indesc, &mut invalues, &mut innulls);
            let outnatts = outdesc.natts as usize;
            let mut outvalues: ::mcx::PgVec<'_, Datum> =
                ::mcx::vec_with_capacity_in(mcx, outnatts)?;
            let mut outnulls: ::mcx::PgVec<'_, bool> =
                ::mcx::vec_with_capacity_in(mcx, outnatts)?;
            for &attno in map {
                if attno > 0 {
                    outvalues.push(invalues[(attno - 1) as usize]);
                    outnulls.push(innulls[(attno - 1) as usize]);
                } else {
                    outvalues.push(Datum::null());
                    outnulls.push(true);
                }
            }
            let out_tuple = ::heaptuple::heap_form_tuple(mcx, outdesc, &outvalues, &outnulls)?;
            let d = Datum::from_usize(out_tuple.image().as_ptr() as usize);
            core::mem::forget(out_tuple);
            d
        }
        None => ::heaptuple::heap_copy_tuple_as_datum(mcx, &tuple, outdesc)?,
    };
    core::mem::forget(tuple);
    Ok(result)
}

// ExecEvalArrayExpr (execExprInterp.c), 1-D leg; the result array lives in
// the armed per-eval result context.
#[allow(clippy::too_many_arguments)]
fn eval_array_expr(
    frames: &mut [crate::steps::FuncFrame<'_>],
    elems: core::ptr::NonNull<NullableDatum>,
    nelems: u16,
    frame: u32,
    elmtype: ::types_core::Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: u8,
) -> PgResult<(Datum, bool)> {
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let n = nelems as usize;
    // SAFETY: n scratch slots written by the element steps just executed.
    let src = unsafe { core::slice::from_raw_parts(elems.as_ptr(), n) };
    let mut values: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: ::mcx::PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for nd in src {
        values.push(nd.value);
        nulls.push(nd.isnull);
    }
    let dims = [n as i32];
    let lbs = [1i32];
    let img = ::arrayfuncs::construct_md_array(
        mcx,
        &values,
        Some(&nulls),
        1,
        &dims,
        &lbs,
        elmtype,
        elmlen as i32,
        elmbyval,
        elmalign,
    )?;
    Ok((Datum::from_usize(img.leak().as_ptr() as usize), false))
}

#[inline(always)]
fn invoke2(call: &crate::steps::Call2) -> PgResult<(Datum, bool)> {
    // SAFETY: 'mcx-live mcx-boxed FmgrInfo + fcinfo image; sole references.
    let flinfo = unsafe { &mut *call.flinfo.as_ptr() };
    let fn_addr = flinfo.fn_addr;
    let fcinfo = unsafe { fcinfo_mut(call.fcinfo, 2) };
    fcinfo.isnull = false;
    let d = fn_addr(Some(flinfo), fcinfo)?;
    Ok((d, fcinfo.isnull))
}

#[inline(always)]
fn strict2_eval(call: &crate::steps::Call2) -> PgResult<NullableDatum> {
    // SAFETY: args 0/1 of the call's live fcinfo image.
    let (a0, a1) = unsafe {
        (
            crate::steps::arg_slot_of(call.fcinfo, 0).read(),
            crate::steps::arg_slot_of(call.fcinfo, 1).read(),
        )
    };
    if a0.isnull || a1.isnull {
        return Ok(NullableDatum::null());
    }
    let (value, isnull) = invoke2(call)?;
    Ok(NullableDatum { value, isnull })
}

// Thin-ABI call: no flinfo arg, no arity check, no isnull round trip — the
// registered callee never writes fcinfo.isnull (fmgr_thin_builtin contract).
#[inline(always)]
fn invoke_thin(call: &crate::steps::CallThin) -> PgResult<Datum> {
    // SAFETY: live 2-arg fcinfo image; thin contract holds at registration.
    unsafe { (call.f)(call.fcinfo.cast()) }
}

#[inline(always)]
fn strict2_thin_eval(call: &crate::steps::CallThin) -> PgResult<NullableDatum> {
    // SAFETY: args 0/1 of the call's live fcinfo image.
    let (a0, a1) = unsafe {
        (
            crate::steps::arg_slot_of(call.fcinfo, 0).read(),
            crate::steps::arg_slot_of(call.fcinfo, 1).read(),
        )
    };
    if a0.isnull || a1.isnull {
        return Ok(NullableDatum::null());
    }
    Ok(NullableDatum { value: invoke_thin(call)?, isnull: false })
}

#[inline(always)]
// ExecEvalFuncExprFusage: an erroring call unwinds past end_function_usage,
// exactly as C's ereport does.
#[cold]
fn invoke_fusage(call: &FuncCall) -> PgResult<(Datum, bool)> {
    // SAFETY: 'mcx-live mcx-boxed FmgrInfo.
    let fn_oid = unsafe { call.flinfo.as_ref() }.fn_oid;
    let fcu = ::pgstat::function::pgstat_init_function_usage(fn_oid)?;
    let r = invoke(call)?;
    ::pgstat::function::pgstat_end_function_usage(&fcu, true);
    Ok(r)
}

fn invoke(call: &FuncCall) -> PgResult<(Datum, bool)> {
    // SAFETY: 'mcx-live mcx-boxed FmgrInfo + fcinfo image; sole references
    // during the call.
    let flinfo = unsafe { &mut *call.flinfo.as_ptr() };
    let fn_addr = flinfo.fn_addr;
    let fcinfo = unsafe { fcinfo_mut(call.fcinfo, call.nargs) };
    fcinfo.isnull = false;
    let d = fn_addr(Some(flinfo), fcinfo)?;
    Ok((d, fcinfo.isnull))
}

// C ExecAggInitGroup. SAFETY contract: live >=2-arg fcinfo image, `pg` the
// sole live pergroup pointer, `byref.agg` a live AggStateNode.
unsafe fn agg_init_group(
    call: &FuncCall,
    pg: *mut crate::steps::AggPerGroup,
    byref: crate::steps::AggByRef,
) -> PgResult<()> {
    // SAFETY: forwarded caller contract.
    unsafe {
        let v = crate::steps::arg_slot_of(call.fcinfo, 1).read();
        debug_assert!(!v.isnull);
        let copied = agg_datum_copy(byref.agg.as_ref().aggcontext(), v.value, byref.translen)?;
        (*pg).trans_value = copied;
        (*pg).trans_value_is_null = false;
        (*pg).no_trans_value = false;
    }
    Ok(())
}

// C ExecAggPlainTransByRef + ExecAggCopyTransValue; C pfrees the replaced
// transvalue, the bump aggcontext reclaims it at group reset instead.
// SAFETY contract: as agg_init_group, with `frames` owning `call`'s frame.
unsafe fn agg_plain_trans_byref(
    call: &FuncCall,
    pg: *mut crate::steps::AggPerGroup,
    byref: crate::steps::AggByRef,
) -> PgResult<()> {
    // SAFETY: forwarded caller contract.
    unsafe {
        crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
            value: (*pg).trans_value,
            isnull: (*pg).trans_value_is_null,
        });
        let (new_val, isnull) = invoke(call)?;
        // NULL transvalues stay at word 0, so the raw compare is null-safe.
        let new_val = if new_val.as_usize() != (*pg).trans_value.as_usize() {
            if !isnull {
                agg_datum_copy(byref.agg.as_ref().aggcontext(), new_val, byref.translen)?
            } else {
                Datum::null()
            }
        } else {
            new_val
        };
        (*pg).trans_value = new_val;
        (*pg).trans_value_is_null = isnull;
    }
    Ok(())
}

/// datumCopy (datum.c), by-ref arms, at palloc (max) alignment.
/// # Safety: `value` is a non-null by-ref datum readable for its full size.
pub unsafe fn agg_datum_copy(
    mcx: ::mcx::Mcx<'_>,
    value: Datum,
    typlen: i16,
) -> PgResult<Datum> {
    let p = value.as_usize() as *const u8;
    // SAFETY: forwarded caller contract.
    let size = unsafe {
        match typlen {
            -1 => {
                // C copies toast pointers verbatim; only expanded flattens.
                if ::types_tuple::varatt::varatt_is_external_expanded(p) {
                    panic!(
                        "datumCopy (datum.c): expanded varlena transvalue — \
                         expanded-object flatten arm has no producers"
                    );
                }
                ::types_tuple::varatt::varsize_any(p)
            }
            n if n > 0 => n as usize,
            n => panic!("datumCopy (datum.c): by-ref transtype with typlen {n} not ported"),
        }
    };
    let layout = core::alloc::Layout::from_size_align(size, 8).expect("datumCopy layout");
    let dst: core::ptr::NonNull<u8> = ::mcx::Allocator::allocate(&mcx, layout)
        .map_err(|_| mcx.oom(size))?
        .cast();
    // SAFETY: fresh `size`-byte allocation; source readable per caller contract.
    unsafe { core::ptr::copy_nonoverlapping(p, dst.as_ptr(), size) };
    Ok(Datum::from_usize(dst.as_ptr() as usize))
}

#[cold]
#[inline(never)]
fn var_slot_mismatch(attnum: u16, why: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "attribute {} of the evaluated slot is not compatible: {why}",
            attnum + 1
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}

// C CheckExprStillValid/CheckVarSlotCompatibility: first-evaluation check of
// every Var step against the live slot descriptors; C swaps evalfunc, the
// owned model records a flag bit (fabled's proven shape).
#[inline(always)]
fn check_still_valid<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
) -> PgResult<()> {
    if state.flags & EEO_FLAG_STILL_VALID_CHECKED != 0 {
        return Ok(());
    }
    check_still_valid_slow(state, slots)
}

// Once per compiled expression (C's CheckExprStillValid cost class).
#[inline(never)]
fn check_still_valid_slow<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
) -> PgResult<()> {
    for step in state.steps.as_slice() {
        let (src, attnum, vartype) = match *step {
            Step::ScanVar { attnum, vartype, .. }
            | Step::ScanVarFuncStrict2 { attnum, vartype, .. } => (SlotSrc::Scan, attnum, vartype),
            Step::InnerVar { attnum, vartype, .. } => (SlotSrc::Inner, attnum, vartype),
            Step::OuterVar { attnum, vartype, .. }
            | Step::OuterVarNotDistinct { attnum, vartype, .. }
            | Step::OuterVarAggTransByValIndirect { attnum, vartype, .. } => {
                (SlotSrc::Outer, attnum, vartype)
            }
            _ => continue,
        };
        let slot = slots.get(src);
        let desc = slot
            .base()
            .tts_tupleDescriptor
            .as_ref()
            .expect("var evaluation against a descriptor-less slot");
        if (attnum as i32) >= desc.natts {
            return Err(var_slot_mismatch(attnum, "attribute number out of range"));
        }
        let attr = &desc.attrs[attnum as usize];
        if attr.attisdropped {
            return Err(var_slot_mismatch(attnum, "attribute has been dropped"));
        }
        if attr.atttypid != vartype {
            return Err(var_slot_mismatch(attnum, "attribute type mismatch"));
        }
    }
    state.flags |= EEO_FLAG_STILL_VALID_CHECKED;
    Ok(())
}

// errdatatype (domains.c): PG_DIAG schema/datatype names off one pg_type probe.
#[cold]
fn errdatatype(e: &mut PgError, typid: u32) {
    if let Ok(Some(t)) = ::syscache_seams::pg_type_domain_shape::call(typid) {
        e.datatype_name =
            core::str::from_utf8(t.typname.name_str()).ok().map(|s| s.to_string());
        let cx = ::mcx::MemoryContext::new("errdatatype");
        let nsp = lsyscache::get_namespace_name(cx.mcx(), t.typnamespace);
        if let Ok(Some(nsp)) = &nsp {
            e.schema_name = Some(nsp.as_str().to_string());
        }
        drop(nsp);
    }
}

// C ExecEvalRowNullInt: SQL-standard row IS [NOT] NULL — per-field primitive
// attisnull tests, not recursive; zero-field rows vacuously satisfy both.
fn eval_row_null(
    frames: &mut [crate::steps::FuncFrame<'_>],
    rn: core::ptr::NonNull<crate::steps::RowNullState>,
    frame: u32,
    r: NullableDatum,
    checkisnull: bool,
) -> PgResult<bool> {
    if r.isnull {
        return Ok(checkisnull);
    }
    let p = r.value.as_usize() as *const u8;
    // SAFETY: a live varlena-headed composite image, per the datum contract.
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: `total` readable bytes at p, per the datum contract.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    // C PG_DETOAST_DATUM returns the original pointer for a plain 4B image;
    // only the toasted leg touches the frame's per-eval mcx.
    let detoasted;
    let rec: &[u8] = if unsafe { ::types_tuple::varatt::varatt_is_4b_u(p) } {
        raw
    } else {
        let f = &mut frames[frame as usize];
        // SAFETY: the argless frame's fcinfo image is live; armed per eval.
        let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
        detoasted = ::detoast_seams::detoast_attr::call(mcx, raw)?;
        &detoasted
    };
    // SAFETY: detoasted composite image; header prefix is in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const ::types_tuple::HeapTupleHeaderData) };
    let tup_type = hdr.type_id();
    let tup_typmod = hdr.typmod();
    // SAFETY: compile-allocated state, single-threaded interpreter.
    let rn = unsafe { &mut *rn.as_ptr() };
    if rn.desc.is_none() || rn.tup_type != tup_type || rn.tup_typmod != tup_typmod {
        use ::mcx::Allocator;
        let desc = typcache::lookup_rowtype_tupdesc_copy(rn.mcx, tup_type, tup_typmod)?;
        let desc_layout = core::alloc::Layout::new::<::types_tuple::TupleDescData<'static>>();
        let desc_ptr: core::ptr::NonNull<::types_tuple::TupleDescData<'static>> = rn
            .mcx
            .allocate(desc_layout)
            .map_err(|_| rn.mcx.oom(desc_layout.size()))?
            .cast();
        // SAFETY: fresh exact-layout allocation; the desc's referents live in
        // rn.mcx, which outlives every eval of this step.
        unsafe {
            desc_ptr.as_ptr().write(core::mem::transmute::<
                ::types_tuple::TupleDescData<'_>,
                ::types_tuple::TupleDescData<'static>,
            >(desc));
        }
        rn.desc = Some(desc_ptr);
        rn.tup_type = tup_type;
        rn.tup_typmod = tup_typmod;
    }
    // SAFETY: rn.mcx-allocated tupdesc, live for the plan.
    let desc = unsafe { rn.desc.expect("refreshed above").as_ref() };
    // SAFETY: detoasted MAXALIGN'd image of datum_length() bytes.
    let tuple = unsafe {
        ::types_tuple::HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ::types_tuple::ItemPointerData::invalid(),
            ::types_core::InvalidOid,
        )
    };
    for att in 1..=desc.natts {
        if desc.compact_attrs[(att - 1) as usize].attisdropped {
            continue;
        }
        if ::types_tuple::heap_attisnull(&tuple, att, Some(desc)) == checkisnull {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}

// C ExecEvalWholeRowVar, named-composite leg. First eval checks the slot's
// physical rowtype against the Var's declared rowtype (dropped-column
// storage mismatches downgrade to the per-row slow path); every eval
// flattens the slot into a composite datum in the armed per-eval mcx.
fn eval_whole_row(
    frames: &mut [crate::steps::FuncFrame<'_>],
    slot: &mut SlotData<'_>,
    wr: core::ptr::NonNull<crate::steps::WholeRowState>,
    frame: u32,
) -> PgResult<(Datum, bool)> {
    // SAFETY: compile-allocated state, single-threaded interpreter.
    let wr = unsafe { &mut *wr.as_ptr() };
    // SAFETY: compile-allocated plan-mcx tupdesc, live for the plan.
    let var_desc = unsafe { wr.tupdesc.as_ref() };
    if wr.first {
        wr.slow = false;
        let slot_desc =
            slot.base().tts_tupleDescriptor.as_ref().expect("slot has a descriptor").clone();
        if var_desc.natts != slot_desc.natts {
            return Err(row_type_mismatch_natts(slot_desc.natts, var_desc.natts));
        }
        for i in 0..var_desc.natts as usize {
            let vattr = &var_desc.attrs[i];
            let sattr = &slot_desc.attrs[i];
            if vattr.atttypid == sattr.atttypid {
                continue;
            }
            if !vattr.attisdropped {
                return Err(row_type_mismatch_type(sattr.atttypid, i, vattr.atttypid));
            }
            if vattr.attlen != sattr.attlen || vattr.attalign != sattr.attalign {
                wr.slow = true;
            }
        }
        wr.first = false;
    }
    exectuples::slot_getallattrs(slot);
    let base = slot.base();
    let slot_desc = base.tts_tupleDescriptor.as_ref().expect("slot has a descriptor");
    if wr.slow {
        for i in 0..var_desc.natts as usize {
            let vattr = &var_desc.compact_attrs[i];
            let sattr = &slot_desc.compact_attrs[i];
            if !var_desc.attrs[i].attisdropped {
                continue;
            }
            if base.tts_isnull[i] {
                continue;
            }
            if vattr.attlen != sattr.attlen || vattr.attalignby != sattr.attalignby {
                return Err(row_type_mismatch_dropped(i));
            }
        }
    }
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let mut tuple = ::heaptoast::toast_build_flattened_tuple(
        mcx,
        slot_desc.as_ref(),
        &base.tts_values,
        &base.tts_isnull,
    )?;
    let img = tuple.image_mut();
    // SAFETY: the header is at the image start (heap_form_tuple contract).
    unsafe {
        let td = &mut *(img.as_mut_ptr() as *mut ::types_tuple::HeapTupleHeaderData);
        td.set_type_id(var_desc.tdtypeid);
        td.set_typmod(var_desc.tdtypmod);
    }
    let d = Datum::from_usize(tuple.image().as_ptr() as usize);
    core::mem::forget(tuple);
    Ok((d, false))
}

#[cold]
#[inline(never)]
pub(crate) fn domain_not_null_violation(typid: u32) -> Box<PgError> {
    let t = format_type::format_type_be(typid).unwrap_or_else(|_| typid.to_string());
    let mut e = PgError::error(format!("domain {t} does not allow null values"))
        .with_sqlstate(::types_error::ERRCODE_NOT_NULL_VIOLATION);
    errdatatype(&mut e, typid);
    Box::new(e)
}

#[cold]
#[inline(never)]
fn row_type_mismatch_natts(slot_natts: i32, var_natts: i32) -> alloc::boxed::Box<PgError> {
    let att = if slot_natts == 1 { "attribute" } else { "attributes" };
    alloc::boxed::Box::new(
        PgError::error("table row type and query-specified row type do not match")
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH)
            .with_detail(alloc::format!(
                "Table row contains {slot_natts} {att}, but query expects {var_natts}."
            )),
    )
}

#[cold]
#[inline(never)]
pub(crate) fn domain_check_violation(typid: u32, name: &str) -> Box<PgError> {
    let t = format_type::format_type_be(typid).unwrap_or_else(|_| typid.to_string());
    let mut e = PgError::error(format!(
        "value for domain {t} violates check constraint \"{name}\""
    ))
    .with_sqlstate(::types_error::ERRCODE_CHECK_VIOLATION);
    errdatatype(&mut e, typid);
    e.constraint_name = Some(name.to_string());
    Box::new(e)
}

#[cold]
#[inline(never)]
fn row_type_mismatch_type(
    slot_type: ::types_core::Oid,
    i: usize,
    var_type: ::types_core::Oid,
) -> alloc::boxed::Box<PgError> {
    let st = ::format_type::format_type_be(slot_type)
        .unwrap_or_else(|_| alloc::format!("{slot_type}"));
    let vt = ::format_type::format_type_be(var_type)
        .unwrap_or_else(|_| alloc::format!("{var_type}"));
    alloc::boxed::Box::new(
        PgError::error("table row type and query-specified row type do not match")
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH)
            .with_detail(alloc::format!(
                "Table has type {st} at ordinal position {}, but query expects {vt}.",
                i + 1
            )),
    )
}

#[cold]
#[inline(never)]
fn row_type_mismatch_dropped(i: usize) -> alloc::boxed::Box<PgError> {
    alloc::boxed::Box::new(
        PgError::error("table row type and query-specified row type do not match")
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH)
            .with_detail(alloc::format!(
                "Physical storage mismatch on dropped attribute at ordinal position {}.",
                i + 1
            )),
    )
}

// Out of line: the kernel fast paths ride the loop's inlining. None = NULL.
#[inline(never)]
fn eval_row_compare_step(call: &crate::steps::Call2, strict: bool) -> PgResult<Option<i32>> {
    // SAFETY: args 0/1 of the call's live fcinfo image.
    let (a0, a1) = unsafe {
        (
            crate::steps::arg_slot_of(call.fcinfo, 0).read(),
            crate::steps::arg_slot_of(call.fcinfo, 1).read(),
        )
    };
    if strict && (a0.isnull || a1.isnull) {
        return Ok(None);
    }
    let (value, isnull) = invoke2(call)?;
    if isnull {
        return Ok(None);
    }
    Ok(Some(value.as_i32()))
}

// CompareType (cmptype.h): LT=1 LE=2 GE=4 GT=5; EQ/NE never reach here.
#[inline(never)]
fn eval_row_compare_final(cmptype: i32, cmpresult: i32) -> bool {
    match cmptype {
        1 => cmpresult < 0,
        2 => cmpresult <= 0,
        4 => cmpresult >= 0,
        5 => cmpresult > 0,
        other => unreachable!("RowCompareFinal cmptype {other}"),
    }
}
