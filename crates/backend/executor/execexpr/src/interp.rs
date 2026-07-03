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
        Kernel::JustFunc { fn_addr, frame, nargs, strict } => {
            let f = &mut state.frames[frame as usize];
            // SAFETY: the frame's fcinfo image is live for 'mcx; no other
            // reference exists during this call.
            let fcinfo = unsafe { fcinfo_mut(f.fcinfo, nargs) };
            if strict && fcinfo.has_null_args() {
                return Ok(NullableDatum::null());
            }
            fcinfo.isnull = false;
            let value = fn_addr(Some(&mut f.flinfo), fcinfo)?;
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

struct ResultRegs {
    value: Datum,
    isnull: bool,
}

// C threads `Datum *resv/bool *resnull`; the flat program's OutRef keeps the
// common target (the result registers) in locals instead of memory.
#[inline(always)]
fn write_out(out: OutRef, regs: &mut ResultRegs, value: Datum, isnull: bool) {
    match out.0 {
        None => {
            regs.value = value;
            regs.isnull = isnull;
        }
        Some(p) => {
            // SAFETY: OutRef targets one arg slot of a frame-owned fcinfo
            // image live for 'mcx (compile-time invariant).
            unsafe { p.write(NullableDatum { value, isnull }) }
        }
    }
}

// Bool steps read-modify their own output (C's resv/resnull aliasing).
#[inline(always)]
fn read_out(out: OutRef, regs: &ResultRegs) -> NullableDatum {
    match out.0 {
        None => NullableDatum { value: regs.value, isnull: regs.isnull },
        // SAFETY: as write_out.
        Some(p) => unsafe { p.read() },
    }
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
    let ExprState { steps, frames, .. } = state;
    let steps = steps.as_slice();
    let mut scan = slots.scan.as_deref_mut();
    let mut inner = slots.inner.as_deref_mut();
    let mut outer = slots.outer.as_deref_mut();
    let mut regs = ResultRegs { value: Datum::null(), isnull: true };
    let base = steps.as_ptr();
    let mut sp = base;
    if let Some(r) = resume {
        regs.value = r.regs.value;
        regs.isnull = r.regs.isnull;
        let Step::SubPlan { out, .. } = steps[r.step as usize] else {
            panic!("resume target is not a SubPlan step")
        };
        write_out(out, &mut regs, r.result.value, r.result.isnull);
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
                return Ok(EvalOutcome::Done(NullableDatum {
                    value: regs.value,
                    isnull: regs.isnull,
                }))
            }
            Step::DoneNoReturn => return Ok(EvalOutcome::Done(NullableDatum::null())),
            Step::ParamSet { prm, out } => {
                let r = read_out(*out, &regs);
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
                    regs: NullableDatum { value: regs.value, isnull: regs.isnull },
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
                write_out(*out, &mut regs, nd.value, nd.isnull);
            }
            Step::InnerVar { attnum, out, .. } => {
                let nd = read_var(need_slot(&mut inner), *attnum);
                write_out(*out, &mut regs, nd.value, nd.isnull);
            }
            Step::OuterVar { attnum, out, .. } => {
                let nd = read_var(need_slot(&mut outer), *attnum);
                write_out(*out, &mut regs, nd.value, nd.isnull);
            }
            Step::ScanSysVar { attnum, out } => {
                let mut isnull = false;
                let d = exectuples::slot_getsysattr(need_slot(&mut scan), *attnum as i32, &mut isnull)?;
                write_out(*out, &mut regs, d, isnull);
            }
            Step::InnerSysVar { attnum, out } => {
                let mut isnull = false;
                let d =
                    exectuples::slot_getsysattr(need_slot(&mut inner), *attnum as i32, &mut isnull)?;
                write_out(*out, &mut regs, d, isnull);
            }
            Step::OuterSysVar { attnum, out } => {
                let mut isnull = false;
                let d =
                    exectuples::slot_getsysattr(need_slot(&mut outer), *attnum as i32, &mut isnull)?;
                write_out(*out, &mut regs, d, isnull);
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
                assign_to_result(rslot, *resultnum, regs.value, regs.isnull);
            }
            Step::AssignTmpMakeRo { resultnum } => {
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                // SAFETY: a non-null by-ref result datum points at a live
                // varlena image (same read exectuples materialize performs).
                let value = if !regs.isnull {
                    unsafe { datum::expandeddatum::make_expanded_object_read_only_internal(regs.value) }
                } else {
                    regs.value
                };
                assign_to_result(rslot, *resultnum, value, regs.isnull);
            }
            Step::Const { value, isnull, out } => {
                write_out(*out, &mut regs, *value, *isnull);
            }
            Step::ParamExtern { prm, out } => {
                // SAFETY: compile-resolved pointer, portal-lived (steps.rs note).
                let p = unsafe { prm.read() };
                write_out(*out, &mut regs, p.value, p.isnull);
            }
            Step::ParamExec { prm, out } => {
                // SAFETY: compile-resolved pointer into stable es_param_exec_vals.
                let p = unsafe { prm.read() };
                if p.exec_plan {
                    param_exec_plan_pending();
                }
                write_out(*out, &mut regs, p.value, p.isnull);
            }
            Step::FuncExpr { call, out } => {
                let (value, isnull) = invoke(frames, call)?;
                write_out(*out, &mut regs, value, isnull);
            }
            Step::IoCoerce { calls, out } => {
                // SAFETY: 'mcx-owned pair written once at compile.
                let c = unsafe { calls.as_ref() };
                let nd = read_out(*out, &regs);
                let strv = if nd.isnull {
                    NullableDatum { value: Datum::null(), isnull: true }
                } else {
                    // SAFETY: arg 0 of the outcall's live fcinfo image.
                    unsafe {
                        crate::steps::arg_slot_of(c.outcall.fcinfo, 0)
                            .write(NullableDatum { value: nd.value, isnull: false })
                    };
                    let (v, isnull) = invoke(frames, &c.outcall)?;
                    NullableDatum { value: v, isnull }
                };
                if strv.isnull && c.in_strict {
                    write_out(*out, &mut regs, Datum::null(), true);
                } else {
                    // SAFETY: arg 0 of the incall's live fcinfo image.
                    unsafe { crate::steps::arg_slot_of(c.incall.fcinfo, 0).write(strv) };
                    let (v, isnull) = invoke(frames, &c.incall)?;
                    write_out(*out, &mut regs, v, isnull);
                }
            }
            Step::FuncExprStrict1 { call, out } => {
                // SAFETY: arg 0 of the call's live fcinfo image.
                let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
                if a0.isnull {
                    write_out(*out, &mut regs, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke(frames, call)?;
                    write_out(*out, &mut regs, value, isnull);
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
                    write_out(*out, &mut regs, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke(frames, call)?;
                    write_out(*out, &mut regs, value, isnull);
                }
            }
            Step::FuncExprStrict { call, out } => {
                // SAFETY: reads nargs arg slots of the call's live image.
                let anynull = (0..call.nargs as usize)
                    .any(|i| unsafe { crate::steps::arg_slot_of(call.fcinfo, i).read().isnull });
                if anynull {
                    write_out(*out, &mut regs, Datum::null(), true);
                } else {
                    let (value, isnull) = invoke(frames, call)?;
                    write_out(*out, &mut regs, value, isnull);
                }
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
                    let (cmp, cmpnull) = invoke(frames, call)?;
                    if cmpnull {
                        continue;
                    }
                    let cmp = cmp.as_i32();
                    if (cmp > 0 && *least) || (cmp < 0 && !*least) {
                        value = nd.value;
                    }
                }
                write_out(*out, &mut regs, value, isnull);
            }
            Step::SqlValueFunction { op, typmod, timetz, out } => {
                use ::types_nodes::primnodes::SQLValueFunctionOp as Op;
                let value = match op {
                    Op::SVFOP_CURRENT_DATE => Datum::from_i32(adt_date::GetSQLCurrentDate()),
                    Op::SVFOP_CURRENT_TIME | Op::SVFOP_CURRENT_TIME_N => {
                        let t = adt_date::GetSQLCurrentTime(*typmod);
                        // SAFETY: compile-allocated 12-byte 8-aligned image
                        // slot owned by this step (steps.rs note).
                        unsafe {
                            timetz.as_ptr().cast::<i64>().write(t.time);
                            timetz.as_ptr().add(8).cast::<i32>().write(t.zone);
                        }
                        Datum::from_usize(timetz.as_ptr() as usize)
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
                    other => panic!(
                        "execexpr EEOP_SQLVALUEFUNCTION: name op {other:?} unported \
                         (grammar arms 2149-2155 are louds)"
                    ),
                };
                write_out(*out, &mut regs, value, false);
            }
            Step::Jump { jumpdone } => {
                // SAFETY: jump targets validated < steps.len() at ready.
                sp = unsafe { base.add(*jumpdone as usize) };
                continue;
            }
            Step::JumpIfNotTrue { jumpdone, out } => {
                let r = read_out(*out, &regs);
                if r.isnull || !r.value.as_bool() {
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::CaseTestVal { slot, out } => {
                // SAFETY: compile-allocated workspace, live for 'mcx.
                let nd = unsafe { slot.read() };
                write_out(*out, &mut regs, nd.value, nd.isnull);
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
            Step::Qual { jumpdone } => {
                if regs.isnull || !regs.value.as_bool() {
                    regs.value = Datum::from_bool(false);
                    regs.isnull = false;
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
                let r = read_out(*out, &regs);
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
                let r = read_out(*out, &regs);
                // SAFETY: compile-allocated scratch, live for 'mcx.
                if !r.isnull && r.value.as_bool() && unsafe { anynull.read() } {
                    write_out(*out, &mut regs, Datum::null(), true);
                }
            }
            Step::BoolOrStepFirst { anynull, jumpdone, out }
            | Step::BoolOrStep { anynull, jumpdone, out } => {
                if matches!(step, Step::BoolOrStepFirst { .. }) {
                    // SAFETY: compile-allocated scratch, live for 'mcx.
                    unsafe { anynull.write(false) };
                }
                let r = read_out(*out, &regs);
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
                let r = read_out(*out, &regs);
                // SAFETY: compile-allocated scratch, live for 'mcx.
                if !r.isnull && !r.value.as_bool() && unsafe { anynull.read() } {
                    write_out(*out, &mut regs, Datum::null(), true);
                }
            }
            Step::BoolNotStep { out } => {
                // NULL in gives NULL out: isnull rides through untouched (C
                // flips the datum even when nominally null).
                let r = read_out(*out, &regs);
                write_out(*out, &mut regs, Datum::from_bool(!r.value.as_bool()), r.isnull);
            }
            Step::NullTestIsNull { out } => {
                let r = read_out(*out, &regs);
                write_out(*out, &mut regs, Datum::from_bool(r.isnull), false);
            }
            Step::NullTestIsNotNull { out } => {
                let r = read_out(*out, &regs);
                write_out(*out, &mut regs, Datum::from_bool(!r.isnull), false);
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
            Step::AggrefEval { value, null, out } => {
                // SAFETY: pointers into once-allocated AggState arrays (steps.rs note).
                let (v, n) = unsafe { (value.read(), null.read()) };
                write_out(*out, &mut regs, v, n);
            }
            Step::AggPlainTransByVal { call, pergroup } => {
                // SAFETY: once-allocated stable pergroup; sole access here.
                unsafe {
                    let pg = pergroup.as_ptr();
                    crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                        value: (*pg).trans_value,
                        isnull: (*pg).trans_value_is_null,
                    });
                    let (value, isnull) = invoke(frames, call)?;
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
                        let (value, isnull) = invoke(frames, call)?;
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
                        let (value, isnull) = invoke(frames, call)?;
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
                        let (value, isnull) = invoke(frames, call)?;
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
                    let (value, isnull) = invoke(frames, call)?;
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
                        let (value, isnull) = invoke(frames, call)?;
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
                        agg_plain_trans_byref(frames, call, pg, *byref)?;
                    }
                }
            }
            Step::AggPlainTransStrictByRef { call, pergroup, byref } => {
                // SAFETY: as AggPlainTransInitStrictByRef.
                unsafe {
                    let pg = pergroup.as_ptr();
                    if !(*pg).trans_value_is_null {
                        agg_plain_trans_byref(frames, call, pg, *byref)?;
                    }
                }
            }
            Step::AggPlainTransByRef { call, pergroup, byref } => {
                // SAFETY: as AggPlainTransInitStrictByRef.
                unsafe { agg_plain_trans_byref(frames, call, pergroup.as_ptr(), *byref)? }
            }
            Step::AggTransInitStrictByRefIndirect { call, base, transno, byref } => {
                // SAFETY: as AggTransByValIndirect + AggPlainTransByRef.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    if (*pg).no_trans_value {
                        agg_init_group(call, pg, *byref)?;
                    } else if !(*pg).trans_value_is_null {
                        agg_plain_trans_byref(frames, call, pg, *byref)?;
                    }
                }
            }
            Step::AggTransStrictByRefIndirect { call, base, transno, byref } => {
                // SAFETY: as AggTransInitStrictByRefIndirect.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    if !(*pg).trans_value_is_null {
                        agg_plain_trans_byref(frames, call, pg, *byref)?;
                    }
                }
            }
            Step::AggTransByRefIndirect { call, base, transno, byref } => {
                // SAFETY: as AggTransInitStrictByRefIndirect.
                unsafe {
                    let pg = base.read().as_ptr().add(*transno as usize);
                    agg_plain_trans_byref(frames, call, pg, *byref)?
                }
            }
            Step::HashDatumSetInitVal { init_value, out } => {
                write_out(*out, &mut regs, *init_value, false);
            }
            Step::HashDatumFirst { call, out } => {
                // SAFETY: arg 0 of the call's live fcinfo image; hash fns
                // never return NULL (C reads fn_addr's Datum directly).
                let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
                let v = if a0.isnull { Datum::null() } else { invoke(frames, call)?.0 };
                write_out(*out, &mut regs, v, false);
            }
            Step::HashDatumNext32 { call, iresult, out } => {
                // SAFETY: iresult is a build-owned once-allocated slot; arg 0
                // as HashDatumFirst.
                let existing = unsafe { iresult.read() }.value.as_u32().rotate_left(1);
                let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
                let combined = if a0.isnull {
                    existing
                } else {
                    existing ^ invoke(frames, call)?.0.as_u32()
                };
                write_out(*out, &mut regs, Datum::from_u32(combined), false);
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
                    write_out(*out, &mut regs, Datum::from_bool(true), false);
                } else if a0.isnull || a1.isnull {
                    write_out(*out, &mut regs, Datum::from_bool(false), false);
                } else {
                    let (value, isnull) = invoke(frames, call)?;
                    write_out(*out, &mut regs, value, isnull);
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

#[inline(always)]
fn invoke(
    frames: &mut [crate::steps::FuncFrame<'_>],
    call: &FuncCall,
) -> PgResult<(Datum, bool)> {
    debug_assert!((call.frame as usize) < frames.len());
    // SAFETY: frame index validated against frames.len() at ready time.
    let f = unsafe { frames.get_unchecked_mut(call.frame as usize) };
    // SAFETY: the call's fcinfo image is live for 'mcx; this is the only
    // reference during the call (arg OutRef raw writes are not live borrows).
    let fcinfo = unsafe { fcinfo_mut(call.fcinfo, call.nargs) };
    fcinfo.isnull = false;
    let d = (call.fn_addr)(Some(&mut f.flinfo), fcinfo)?;
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
    frames: &mut [crate::steps::FuncFrame<'_>],
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
        let (new_val, isnull) = invoke(frames, call)?;
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
            Step::ScanVar { attnum, vartype, .. } => (SlotSrc::Scan, attnum, vartype),
            Step::InnerVar { attnum, vartype, .. } => (SlotSrc::Inner, attnum, vartype),
            Step::OuterVar { attnum, vartype, .. } => (SlotSrc::Outer, attnum, vartype),
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
