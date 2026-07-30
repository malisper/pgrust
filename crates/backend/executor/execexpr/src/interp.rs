use alloc::boxed::Box;
use alloc::format;

use ::datum::{Datum, NullableDatum};
use ::mcx::Allocator;
use ::types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED};
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
            // OLD/NEW ride in RetSlots (RETURNING projections only).
            SlotSrc::Old | SlotSrc::New => missing_slot(src),
        };
        match slot {
            Some(s) => s,
            None => missing_slot(src),
        }
    }
}

/// One RETURNING OLD/NEW source (C econtext ecxt_oldtuple/ecxt_newtuple).
/// `Scan` marks C's slot-aliasing (ExecProcessReturning points scantuple and
/// old/newtuple at the same slot); a second `&mut` would alias.
pub enum RetSlot<'a, 'mcx> {
    None,
    Scan,
    Slot(&'a mut SlotData<'mcx>),
}

/// The RETURNING projection's OLD/NEW slot pair; every non-RETURNING entry
/// point evaluates with [`RetSlots::none`].
pub struct RetSlots<'a, 'mcx> {
    pub old: RetSlot<'a, 'mcx>,
    pub new: RetSlot<'a, 'mcx>,
}

impl RetSlots<'_, '_> {
    pub fn none() -> Self {
        RetSlots { old: RetSlot::None, new: RetSlot::None }
    }
}

#[cold]
#[inline(never)]
fn missing_slot(src: SlotSrc) -> ! {
    panic!("execexpr: expression references the {src:?} slot but none was supplied")
}

#[track_caller]
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

    pub(crate) fn new(sstate: core::ptr::NonNull<()>, step: u32, regs: NullableDatum) -> Suspension {
        Suspension { sstate, step, regs }
    }
}

impl Resume {
    pub(crate) fn into_parts(self) -> (NullableDatum, NullableDatum, u32) {
        (self.regs, self.result, self.step)
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
    match eval(state, slots, &mut RetSlots::none(), None, None)? {
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
    eval(state, slots, &mut RetSlots::none(), None, resume)
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
    Ok(match eval(state, slots, &mut RetSlots::none(), None, resume)? {
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
    Ok(match eval(state, slots, &mut RetSlots::none(), Some(result_slot), resume)? {
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
    let r = match eval(state, slots, &mut RetSlots::none(), None, None)? {
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
    state.arm_result_mcx(result_mcx);
    exec_project_prearmed(state, slots, result_slot, result_mcx)
}

/// [`exec_project`] for callers that armed the program themselves (the
/// per-tuple-mcx scan path); `slot_mcx` is only the result slot's owner.
pub fn exec_project_prearmed<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    result_slot: &mut SlotData<'mcx>,
    slot_mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    check_still_valid(state, slots)?;
    exectuples::exec_clear_tuple(result_slot, slot_mcx);
    match eval(state, slots, &mut RetSlots::none(), Some(result_slot), None)? {
        EvalOutcome::Done(_) => {}
        EvalOutcome::Suspended(_) => subplan_without_driver(),
    }
    exectuples::exec_store_virtual_tuple(result_slot);
    Ok(())
}

/// [`exec_project`] with RETURNING OLD/NEW sources (C ExecProject through
/// ExecProcessReturning's econtext old/new slots).
/// [`exec_project_returning`] outcome form for owning-node subplan drivers:
/// no clear/store (the driver clears before its resume loop and stores on
/// Done), suspension surfaces instead of panicking.
pub fn exec_project_returning_outcome<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    ret: &mut RetSlots<'_, 'mcx>,
    result_slot: &mut SlotData<'mcx>,
    resume: Option<Resume>,
) -> PgResult<Option<Suspension>> {
    check_still_valid_ret(state, slots, ret)?;
    Ok(match eval(state, slots, ret, Some(result_slot), resume)? {
        EvalOutcome::Done(_) => None,
        EvalOutcome::Suspended(s) => Some(s),
    })
}

pub fn exec_project_returning<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    ret: &mut RetSlots<'_, 'mcx>,
    result_slot: &mut SlotData<'mcx>,
    result_mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    state.arm_result_mcx(result_mcx);
    check_still_valid_ret(state, slots, ret)?;
    exectuples::exec_clear_tuple(result_slot, result_mcx);
    match eval(state, slots, ret, Some(result_slot), None)? {
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
    ret: &mut RetSlots<'_, 'mcx>,
    result_slot: Option<&mut SlotData<'mcx>>,
    resume: Option<Resume>,
) -> PgResult<EvalOutcome> {
    if let Kernel::Program = state.kernel {
        if state.jit.is_some() {
            return crate::jit::run_jit(state, slots, ret, result_slot, resume);
        }
        return run_program(state, slots, ret, result_slot, resume);
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
    ret: &mut RetSlots<'_, 'mcx>,
    mut result_slot: Option<&mut SlotData<'mcx>>,
    resume: Option<Resume>,
) -> PgResult<EvalOutcome> {
    let flags = state.flags;
    let ExprState { steps, frames, resnd, saop_tables, .. } = state;
    let res = *resnd;
    let steps = steps.as_slice();
    let mut scan = slots.scan.as_deref_mut();
    let mut inner = slots.inner.as_deref_mut();
    let mut outer = slots.outer.as_deref_mut();
    let (mut old, old_is_scan) = match &mut ret.old {
        RetSlot::None => (None, false),
        RetSlot::Scan => (None, true),
        RetSlot::Slot(s) => (Some(&mut **s), false),
    };
    let (mut new, new_is_scan) = match &mut ret.new {
        RetSlot::None => (None, false),
        RetSlot::Scan => (None, true),
        RetSlot::Slot(s) => (Some(&mut **s), false),
    };
    macro_rules! old_slot {
        () => {
            if old_is_scan { need_slot(&mut scan) } else { need_slot(&mut old) }
        };
    }
    macro_rules! new_slot {
        () => {
            if new_is_scan { need_slot(&mut scan) } else { need_slot(&mut new) }
        };
    }
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
            Step::OldFetchSome { last_var } => {
                exectuples::slot_getsomeattrs(old_slot!(), *last_var as i32);
            }
            Step::NewFetchSome { last_var } => {
                exectuples::slot_getsomeattrs(new_slot!(), *last_var as i32);
            }
            Step::OldVar { attnum, out, .. } => {
                let nd = read_var(old_slot!(), *attnum);
                write_out(*out, nd.value, nd.isnull);
            }
            Step::NewVar { attnum, out, .. } => {
                let nd = read_var(new_slot!(), *attnum);
                write_out(*out, nd.value, nd.isnull);
            }
            Step::OldSysVar { attnum, out } => {
                // C ExecEvalSysVar: OLD system attribute is NULL when the
                // OLD row doesn't exist.
                if flags & crate::steps::EEO_FLAG_OLD_IS_NULL != 0 {
                    write_out(*out, Datum::null(), true);
                } else {
                    let mut isnull = false;
                    let d =
                        exectuples::slot_getsysattr(old_slot!(), *attnum as i32, &mut isnull)?;
                    write_out(*out, d, isnull);
                }
            }
            Step::NewSysVar { attnum, out } => {
                if flags & crate::steps::EEO_FLAG_NEW_IS_NULL != 0 {
                    write_out(*out, Datum::null(), true);
                } else {
                    let mut isnull = false;
                    let d =
                        exectuples::slot_getsysattr(new_slot!(), *attnum as i32, &mut isnull)?;
                    write_out(*out, d, isnull);
                }
            }
            Step::ReturningExprStep { nullflag, jumpdone, out } => {
                if flags & *nullflag != 0 {
                    write_out(*out, Datum::null(), true);
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
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
                // C ExecEvalWholeRowVar: an OLD/NEW whole-row is NULL when
                // that row doesn't exist.
                if (matches!(src, crate::steps::SlotSrc::Old)
                    && flags & crate::steps::EEO_FLAG_OLD_IS_NULL != 0)
                    || (matches!(src, crate::steps::SlotSrc::New)
                        && flags & crate::steps::EEO_FLAG_NEW_IS_NULL != 0)
                {
                    write_out(*out, Datum::null(), true);
                } else {
                    let slot = match src {
                        crate::steps::SlotSrc::Scan => need_slot(&mut scan),
                        crate::steps::SlotSrc::Inner => need_slot(&mut inner),
                        crate::steps::SlotSrc::Outer => need_slot(&mut outer),
                        crate::steps::SlotSrc::Old => old_slot!(),
                        crate::steps::SlotSrc::New => new_slot!(),
                    };
                    let (value, isnull) = eval_whole_row(frames, slot, *wr, *frame)?;
                    write_out(*out, value, isnull);
                }
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
            Step::AssignOldVar { attnum, resultnum } => {
                let nd = read_var(old_slot!(), *attnum);
                let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
                assign_to_result(rslot, *resultnum, nd.value, nd.isnull);
            }
            Step::AssignNewVar { attnum, resultnum } => {
                let nd = read_var(new_slot!(), *attnum);
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
            Step::ParamExternMissing { paramid } => {
                return Err(crate::compile::no_param_value(*paramid));
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
                step_io_coerce(*calls, *out)?;
            }
            Step::IoCoerceSafe { calls, out } => {
                step_io_coerce_safe(*calls, *out)?;
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
            Step::JsonExprPath { jsestate, frame, out } => {
                let target = eval_json_expr_path(frames, *jsestate, *frame, *out)?;
                // SAFETY: jump targets validated < steps.len() at ready.
                sp = unsafe { base.add(target as usize) };
                continue;
            }
            Step::JsonCoercion { jc, frame, out } => {
                eval_json_coercion(frames, *jc, *frame, *out)?;
            }
            Step::JsonCoercionFinish { jsestate, out } => {
                eval_json_coercion_finish(*jsestate, *out)?;
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
                step_min_max(call, *slots, *nelems, *least, *out)?;
            }
            Step::SqlValueFunction { op, typmod, scratch, out } => {
                step_sql_value_function(*op, *typmod, *scratch, *out)?;
            }
            Step::MergeSupportFunc { action, scratch, out } => {
                step_merge_support_func(*action, *scratch, *out)?;
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
            Step::JsonbSbsrefSubscripts { state, jumpdone, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                if !crate::jsonbsubs::check_subscripts(st)? {
                    write_out(*out, Datum::null(), true);
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
                    continue;
                }
            }
            Step::JsonbSbsrefFetch { state, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                let r = crate::jsonbsubs::fetch(st, cur)?;
                write_out(*out, r.value, r.isnull);
            }
            Step::JsonbSbsrefAssign { state, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                let r = crate::jsonbsubs::assign(st, cur)?;
                write_out(*out, r.value, r.isnull);
            }
            Step::HstoreSbsrefFetch { state, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                let r = crate::hstoresubs::fetch(st, cur)?;
                write_out(*out, r.value, r.isnull);
            }
            Step::HstoreSbsrefAssign { state, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                let r = crate::hstoresubs::assign(st, cur)?;
                write_out(*out, r.value, r.isnull);
            }
            Step::SbsrefAssign { state, slice, out } => {
                // SAFETY: as ArrayExprEval.
                let st = unsafe { &mut *state.as_ptr() };
                let cur = read_out(*out);
                let r = if *slice {
                    crate::arrayops::sbsref_assign_slice(st, cur)?
                } else {
                    crate::arrayops::sbsref_assign(st, cur)?
                };
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
            Step::FieldStoreDeForm { fs, frame, out } => {
                let r = read_out(*out);
                // SAFETY: compile-allocated state, sole live access.
                let st = unsafe { fs.as_ref() };
                eval_field_store_deform(frames, st, *frame, r)?;
            }
            Step::FieldStoreForm { fs, frame, out } => {
                // SAFETY: compile-allocated state, sole live access.
                let st = unsafe { fs.as_ref() };
                let v = eval_field_store_form(frames, st, *frame)?;
                write_out(*out, v, false);
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
            Step::DomainNotNull { resulttype, escontext, out } => {
                if read_out(*out).isnull {
                    // SAFETY: escontext points at the owning JsonExprState's
                    // node, live for the program.
                    errsave(*escontext, || domain_not_null_violation(*resulttype))?;
                }
            }
            Step::DomainCheck { resulttype, name, check, escontext } => {
                // SAFETY: compile-allocated scratch, live for 'mcx.
                let r = unsafe { check.read() };
                if !r.isnull && !r.value.as_bool() {
                    // SAFETY: name is a compile-copied &'mcx str; escontext as
                    // in DomainNotNull.
                    errsave(*escontext, || {
                        domain_check_violation(*resulttype, unsafe { name.as_ref() })
                    })?;
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
                step_grouping_func(*cols, *ncols, *current, *out);
            }
            Step::AggSetCurrent { agg, aggref, shared } => {
                // SAFETY: the caller's query-lifetime AggStateNode; no &mut
                // is live across expression evaluation.
                unsafe { agg.as_ref() }.set_current_agg(*aggref, *shared);
            }
            Step::AggDeserialize { call, out } => {
                let (value, isnull) = invoke(call)?;
                write_out(*out, value, isnull);
            }
            Step::AggStrictDeserialize { call, out, jumpnull } => {
                // SAFETY: slot 0 of the live 2-arg ds fcinfo image.
                if unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read().isnull } {
                    sp = unsafe { base.add(*jumpnull as usize) };
                    continue;
                }
                let (value, isnull) = invoke(call)?;
                write_out(*out, value, isnull);
            }
            Step::AggPlainTransByVal { call, pergroup } => {
                // SAFETY: once-allocated stable pergroup; sole access here.
                unsafe { agg_trans_byval(call, pergroup.as_ptr())? }
            }
            Step::AggPlainTransStrictByVal { call, pergroup } => {
                // SAFETY: as AggPlainTransByVal.
                unsafe { agg_trans_strict_byval(call, pergroup.as_ptr())? }
            }
            Step::AggPlainTransInitStrictByVal { call, pergroup } => {
                // SAFETY: as AggPlainTransByVal.
                unsafe { agg_trans_init_strict_byval(call, pergroup.as_ptr())? }
            }
            Step::AggTransInitStrictByValIndirect { call, base, transno } => {
                // SAFETY: as AggTransByValIndirect.
                unsafe {
                    agg_trans_init_strict_byval(call, base.read().as_ptr().add(*transno as usize))?
                }
            }
            Step::AggTransByValIndirect { call, base, transno } => {
                // SAFETY: base is a live cell nodeAgg repoints at the current
                // group's once-allocated pergroup array before evaluation;
                // transno < that array's length (build invariant).
                unsafe { agg_trans_byval(call, base.read().as_ptr().add(*transno as usize))? }
            }
            Step::AggTransStrictByValIndirect { call, base, transno } => {
                // SAFETY: as AggTransByValIndirect.
                unsafe {
                    agg_trans_strict_byval(call, base.read().as_ptr().add(*transno as usize))?
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
                step_hash_datum_first(call, *out)?;
            }
            Step::HashDatumNext32 { call, iresult, out } => {
                step_hash_datum_next32(call, *iresult, *out)?;
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
                step_distinct(call, *out)?;
            }
            Step::NullIf { call, out } => {
                step_nullif(call, *out)?;
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
                step_not_distinct(call, *out)?;
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
    // C: the empty-array result precedes the strict NULL-scalar check.
    if nitems <= 0 {
        return Ok((Datum::from_bool(!use_or), false));
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

        // A non-strict equality function may treat NULL as equal to some
        // value: linear-search the array once with a null lhs (OR semantics,
        // as the non-hashed SAOP) and cache the outcome (C 035c520).
        if !strictfunc {
            let mut result = false;
            let mut resultnull = false;
            let mut off = ::arrayfuncs::foundation::arr_data_offset(img);
            let mut bitmask: u32 = 1;
            let mut bitmap_byte = 0usize;
            for _ in 0..nitems {
                let elt_null = match bitmap_off {
                    Some(bo) => (img[bo + bitmap_byte] as u32 & bitmask) == 0,
                    None => false,
                };
                let rhs = if elt_null {
                    NullableDatum::null()
                } else {
                    off = ::arrayfuncs::foundation::att_align_nominal(off, typalign);
                    // SAFETY: off stays within the VARSIZE image per the array layout.
                    let ep = unsafe { img.as_ptr().add(off) };
                    let elt = ::arrayfuncs::foundation::fetch_att(ep, typbyval, typlen as i32);
                    off = ::arrayfuncs::foundation::att_addlength_pointer(off, typlen as i32, ep);
                    NullableDatum { value: elt, isnull: false }
                };
                // SAFETY: arg slots 0/1 of the call's live fcinfo image.
                unsafe {
                    crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum::null());
                    crate::steps::arg_slot_of(call.fcinfo, 1).write(rhs);
                }
                let (r, isnull) = invoke(call)?;
                if isnull {
                    resultnull = true;
                } else if r.as_bool() {
                    result = true;
                    resultnull = false;
                    break;
                }
                if bitmap_off.is_some() {
                    bitmask <<= 1;
                    if bitmask == 0x100 {
                        bitmask = 1;
                        bitmap_byte += 1;
                    }
                }
            }
            // Invert non-NULL results for NOT IN.
            tab.null_lhs_result = if !resultnull && !inclause { !result } else { result };
            tab.null_lhs_isnull = resultnull;
        }
        tab.built = true;
    }

    // Null scalar with a non-strict function: the cached null-lhs result.
    if scalar.isnull {
        debug_assert!(!strictfunc);
        if tab.null_lhs_isnull {
            return Ok((Datum::null(), true));
        }
        return Ok((Datum::from_bool(tab.null_lhs_result), false));
    }

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
    nelems: u32,
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
    // Composite Datums must not carry external toast pointers
    // (C HeapTupleHeaderGetDatum, execTuples.c:2413).
    if tuple.as_tuple().has_external() {
        let d = ::detoast_seams::toast_flatten_tuple_to_datum::call(mcx, tuple.as_tuple(), desc)?;
        return Ok((d, false));
    }
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
                ::adt_json::funcs::json_validate(mcx, js, true, true)?;
                Ok((values[0], false))
            }
        }
        JC::JSCTOR_JSON_OBJECTAGG | JC::JSCTOR_JSON_ARRAYAGG | JC::JSCTOR_JSON_SERIALIZE => {
            panic!("invalid JsonConstructorExpr type {:?} in EEOP_JSON_CONSTRUCTOR", jc.ctor_type)
        }
    }
}

/// C DatumGetJsonbP/DatumGetJsonPathP: detoast; short varlenas expand to an
/// aligned 4B-header copy (containers hold int32 words).
/// # Safety
/// `d` is a live by-ref varlena datum readable through its header.
unsafe fn varlena_image_4b<'m>(mcx: ::mcx::Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    use ::types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    // SAFETY: forwarded caller contract.
    unsafe {
        if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p))
        {
            let image = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            Ok(detoast_seams::detoast_attr::call(mcx, image)?.leak())
        } else if varatt::varatt_is_1b(p) {
            let payload = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let mut v: ::mcx::PgVec<'m, u8> =
                ::mcx::vec_with_capacity_in(mcx, ::datum::varlena::VARHDRSZ + payload.len())?;
            ::mcx::vec_append_bytes(
                &mut v,
                &::datum::varlena::set_varsize_4b(::datum::varlena::VARHDRSZ + payload.len()),
            )?;
            ::mcx::vec_append_bytes(&mut v, payload)?;
            Ok(v.leak())
        } else {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        }
    }
}

fn cstring_in<'m>(mcx: ::mcx::Mcx<'m>, bytes: &[u8]) -> PgResult<::mcx::PgVec<'m, u8>> {
    let mut v = ::mcx::vec_with_capacity_in(mcx, bytes.len() + 1)?;
    ::mcx::vec_append_bytes(&mut v, bytes)?;
    v.push(0);
    Ok(v)
}

// C ExecGetJsonValueItemString minus the jbvNull leg (the caller's Null
// variant covers it); NUL-terminated cstring image in mcx.
fn json_value_item_cstring<'m>(
    mcx: ::mcx::Mcx<'m>,
    item: &::adt_jsonpath_exec::JbV<'_>,
) -> PgResult<::mcx::PgVec<'m, u8>> {
    use ::adt_formatting::ParsedDatetime;
    use ::adt_jsonpath_exec::JbV;
    match item {
        JbV::Null => panic!("unexpected jbvNull in ExecGetJsonValueItemString"),
        JbV::String(s) => cstring_in(mcx, s),
        JbV::Numeric(img) => {
            let mut buf = alloc::vec::Vec::new();
            ::adt_numeric::numeric_out_into(::adt_numeric::Num::from_payload(&img[4..]), &mut buf);
            cstring_in(mcx, &buf)
        }
        JbV::Bool(b) => cstring_in(mcx, if *b { b"t" } else { b"f" }),
        JbV::Datetime { value, .. } => {
            let mut buf = [0u8; ::adt_datetime::consts::MAXDATELEN + 1];
            let n = match value {
                ParsedDatetime::Date(d) => ::adt_date::date_out(*d, &mut buf),
                ParsedDatetime::Time(t) => ::adt_date::time_out(*t, &mut buf),
                ParsedDatetime::TimeTz(t) => ::adt_date::timetz_out(t, &mut buf),
                ParsedDatetime::Timestamp(ts) => ::adt_timestamp::timestamp_out(*ts, &mut buf)?,
                ParsedDatetime::TimestampTz(ts) => {
                    ::adt_timestamp::timestamptz_out(*ts, &mut buf)?
                }
            };
            cstring_in(mcx, &buf[..n])
        }
        JbV::Binary(_) => {
            let img = ::adt_jsonpath_exec::jbv_to_jsonb_image(mcx, item)?;
            ::adt_jsonb::io::jsonb_out(mcx, &img[4..])
        }
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_json_item(column_name: Option<&str>) -> Box<PgError> {
    let msg = match column_name {
        Some(col) => {
            format!("no SQL/JSON item found for specified path of column \"{col}\"")
        }
        None => "no SQL/JSON item found for specified path".to_string(),
    };
    Box::new(PgError::error(msg).with_sqlstate(::types_error::ERRCODE_NO_SQL_JSON_ITEM))
}

// C ExecEvalJsonExprPath (execExprInterp.c:4834); returns the next step
// address (jump_error/jump_empty/jump_eval_coercion/jump_end).
#[inline(never)]
fn eval_json_expr_path(
    frames: &mut [crate::steps::FuncFrame<'_>],
    jsestate: core::ptr::NonNull<crate::steps::JsonExprState>,
    frame: u32,
    out: crate::steps::OutRef,
) -> PgResult<u32> {
    use ::adt_jsonpath_exec::{JsonPathQueryResult, JsonPathValueResult};
    use ::types_core::catalog::{JSONBOID, JSONOID};
    use ::types_nodes::primnodes::{JsonBehaviorType as JBT, JsonExprOp};

    // The input fn's fcinfo context points into this state: no reference may
    // live across invoke(); every access below is a short-scoped borrow.
    let jsp = jsestate.as_ptr();
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();

    // SAFETY: compile-allocated state; shared borrow of Copy fields, ends here.
    let (op, formatted, pathspec, wrapper, returning_typid, use_io, use_json, throw_error, nvars, vars_p, var_cells, input_fcinfo, column_name_p, on_error_btype, on_empty_btype, jump_error, jump_empty, jump_eval_coercion, jump_end) = unsafe {
        let js = &*jsp;
        (
            js.op,
            js.formatted_expr,
            js.pathspec,
            js.wrapper,
            js.returning_typid,
            js.use_io_coercion,
            js.use_json_coercion,
            js.throw_error,
            js.nvars as usize,
            js.vars,
            js.var_cells,
            js.input_fcinfo,
            js.column_name,
            js.on_error_btype,
            js.on_empty_btype,
            js.jump_error,
            js.jump_empty,
            js.jump_eval_coercion,
            js.jump_end,
        )
    };

    debug_assert!(!formatted.isnull && !pathspec.isnull);
    // SAFETY: by-ref jsonb/jsonpath datums written by the sub-expr steps just run.
    let doc_image = unsafe { varlena_image_4b(mcx, formatted.value) }?;
    // SAFETY: as above.
    let path_image = unsafe { varlena_image_4b(mcx, pathspec.value) }?;
    let doc = &doc_image[::datum::varlena::VARHDRSZ..];

    // SAFETY: short-scoped exclusive borrow of the live state.
    unsafe {
        let js = &mut *jsp;
        js.error = NullableDatum { value: Datum::null(), isnull: false };
        js.empty = NullableDatum { value: Datum::null(), isnull: false };
        js.escontext.ctx = ::types_error::SoftErrorContext::new(false);
    }

    // SAFETY: parallel compile-allocated nvars-long arrays; cells were written
    // by the PASSING arg steps just run.
    let vars: &[::adt_jsonpath_exec::JsonPathVariable<'static>] = unsafe {
        for i in 0..nvars {
            let cell = var_cells.as_ptr().add(i).read();
            let v = &mut *vars_p.as_ptr().add(i);
            v.value = cell.value;
            v.isnull = cell.isnull;
        }
        core::slice::from_raw_parts(vars_p.as_ptr(), nvars)
    };

    let soft = !throw_error;
    let column_name: Option<&str> = column_name_p.map(|p| {
        // SAFETY: node-arena str restamped at compile; outlives the program.
        unsafe { p.as_ref() }
    });
    let mut error = false;
    let mut empty = false;
    let mut val_string: Option<::mcx::PgVec<'_, u8>> = None;

    match op {
        JsonExprOp::JSON_EXISTS_OP => {
            match ::adt_jsonpath_exec::json_path_exists(mcx, doc, path_image, soft, vars)? {
                None => error = true,
                Some(exists) => write_out(out, Datum::from_bool(exists), false),
            }
        }
        JsonExprOp::JSON_QUERY_OP => {
            match ::adt_jsonpath_exec::json_path_query(
                mcx,
                doc,
                path_image,
                wrapper,
                soft,
                vars,
                column_name,
            )? {
                JsonPathQueryResult::Image(img) => write_out(out, image_datum(img), false),
                JsonPathQueryResult::Empty => {
                    empty = true;
                    write_out(out, Datum::null(), true);
                }
                JsonPathQueryResult::Error => {
                    error = true;
                    write_out(out, Datum::null(), true);
                }
            }
        }
        JsonExprOp::JSON_VALUE_OP => {
            match ::adt_jsonpath_exec::json_path_value(
                mcx,
                doc,
                path_image,
                soft,
                vars,
                column_name,
            )? {
                JsonPathValueResult::Empty => {
                    empty = true;
                    write_out(out, Datum::null(), true);
                }
                JsonPathValueResult::Error => {
                    error = true;
                    write_out(out, Datum::null(), true);
                }
                JsonPathValueResult::Null => write_out(out, Datum::null(), true),
                JsonPathValueResult::Value(jbv) => {
                    if returning_typid == JSONOID || returning_typid == JSONBOID {
                        let img = ::adt_jsonpath_exec::jbv_to_jsonb_image(mcx, &jbv)?;
                        val_string =
                            Some(::adt_jsonb::io::jsonb_out(mcx, &img[::datum::varlena::VARHDRSZ..])?);
                        // C leaves resnull untouched here (zeroed = false on the
                        // reachable path); the io-coercion guard below must see
                        // non-null for val_string to reach the input function.
                        let cur = read_out(out);
                        write_out(out, cur.value, false);
                    } else if use_json {
                        let img = ::adt_jsonpath_exec::jbv_to_jsonb_image(mcx, &jbv)?;
                        write_out(out, image_datum(img), false);
                    } else {
                        let s = json_value_item_cstring(mcx, &jbv)?;
                        if use_io {
                            let cur = read_out(out);
                            write_out(out, cur.value, false);
                        } else {
                            // C DirectFunctionCall1(textin, val_string).
                            let text = &s[..s.len() - 1];
                            let mut img = ::mcx::vec_with_capacity_in(
                                mcx,
                                ::datum::varlena::VARHDRSZ + text.len(),
                            )?;
                            ::mcx::vec_append_bytes(
                                &mut img,
                                &::datum::varlena::set_varsize_4b(
                                    ::datum::varlena::VARHDRSZ + text.len(),
                                ),
                            )?;
                            ::mcx::vec_append_bytes(&mut img, text)?;
                            write_out(out, image_datum(img), false);
                        }
                        val_string = Some(s);
                    }
                }
            }
        }
        JsonExprOp::JSON_TABLE_OP => {
            panic!("unrecognized SQL/JSON expression op {op:?} in EEOP_JSONEXPR_PATH")
        }
    }

    let cur = read_out(out);
    if !cur.isnull && use_io {
        debug_assert!(jump_eval_coercion < 0);
        let call = input_fcinfo.expect("io-coercion input call resolved at compile");
        let s = val_string
            .as_ref()
            .unwrap_or_else(|| panic!("EEOP_JSONEXPR_PATH: use_io_coercion without a value string"));
        // SAFETY: arg 0 of the compile-resolved 3-arg input fcinfo.
        unsafe {
            crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
                value: Datum::from_usize(s.as_ptr() as usize),
                isnull: false,
            });
        }
        let (v, _) = invoke(&call)?;
        write_out(out, v, false);
        // SAFETY: short-scoped shared borrow after the call completed.
        if unsafe { (*jsp).escontext.ctx.error_occurred() } {
            error = true;
        }
    }

    // SAFETY (all below): short-scoped exclusive borrows of the live state;
    // no foreign call runs while one is held.
    if empty {
        write_out(out, Datum::null(), true);
        if let Some(on_empty_btype) = on_empty_btype {
            if on_empty_btype != JBT::JSON_BEHAVIOR_ERROR {
                unsafe {
                    let js = &mut *jsp;
                    js.empty.value = Datum::from_bool(true);
                    js.escontext.ctx = ::types_error::SoftErrorContext::new(true);
                }
                return Ok(if jump_empty >= 0 { jump_empty } else { jump_end } as u32);
            }
        } else if on_error_btype != JBT::JSON_BEHAVIOR_ERROR {
            unsafe {
                let js = &mut *jsp;
                js.error.value = Datum::from_bool(true);
                js.escontext.ctx = ::types_error::SoftErrorContext::new(true);
            }
            debug_assert!(soft);
            return Ok(if jump_error >= 0 { jump_error } else { jump_end } as u32);
        }
        return Err(no_json_item(column_name));
    }

    if error {
        debug_assert!(soft);
        write_out(out, Datum::null(), true);
        unsafe {
            let js = &mut *jsp;
            js.error.value = Datum::from_bool(true);
            js.escontext.ctx = ::types_error::SoftErrorContext::new(true);
        }
        return Ok(if jump_error >= 0 { jump_error } else { jump_end } as u32);
    }

    Ok(if jump_eval_coercion >= 0 { jump_eval_coercion } else { jump_end } as u32)
}

// C ExecEvalJsonCoercion (execExprInterp.c:5111); the composite/record
// json_populate_type leg is a loud panic (jsonfuncs lane).
#[inline(never)]
fn eval_json_coercion(
    frames: &mut [crate::steps::FuncFrame<'_>],
    jc: core::ptr::NonNull<crate::steps::JsonCoercionState>,
    frame: u32,
    out: crate::steps::OutRef,
) -> PgResult<()> {
    // SAFETY: compile-allocated state, exclusive during this step (the
    // json_populate_type cache fills on first eval).
    let st = unsafe { &mut *jc.as_ptr() };
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    // SAFETY: the owning JsonExprState's compile-armed ErrorSaveNode,
    // exclusive during this step.
    let mut esc = st.escontext.map(|p| unsafe { &mut *p.as_ptr() });

    if st.exists_coerce {
        let cur = read_out(out);
        if st.exists_cast_to_int {
            if st.exists_check_domain {
                ::typcache_seams::domain_check_input::call(
                    cur.value,
                    cur.isnull,
                    st.targettype,
                    esc.as_deref_mut().map(|n| &mut n.ctx),
                )?;
                if esc.as_ref().is_some_and(|n| n.ctx.error_occurred()) {
                    write_out(out, Datum::null(), true);
                    return Ok(());
                }
            }
            write_out(out, Datum::from_i32(cur.value.as_bool() as i32), cur.isnull);
            return Ok(());
        }
        let img = ::adt_jsonb::io::jsonb_in(
            mcx,
            if cur.value.as_bool() { b"true" } else { b"false" },
            None,
        )?
        .expect("hard errsave without escontext returns Err");
        write_out(out, image_datum(img), cur.isnull);
        if st.targettype == ::types_core::catalog::JSONBOID {
            // C runs json_populate_type(jsonb -> jsonb, typmod -1): identity.
            return Ok(());
        }
    }

    let cur = read_out(out);
    let mut isnull = cur.isnull;
    // SAFETY: a non-null out is a live jsonb varlena produced by the jsonpath
    // steps or the jsonb_in leg above.
    let value = unsafe {
        ::adt_jsonb::populate::json_populate_type(
            cur.value,
            ::types_core::catalog::JSONBOID,
            st.targettype,
            st.targettypmod,
            &mut st.cache,
            st.mcx,
            mcx,
            &mut isnull,
            st.omit_quotes,
            esc,
        )?
    };
    write_out(out, value, isnull);
    Ok(())
}

// C ExecEvalJsonCoercionFinish (execExprInterp.c:5191).
#[inline(never)]
fn eval_json_coercion_finish(
    jsestate: core::ptr::NonNull<crate::steps::JsonExprState>,
    out: crate::steps::OutRef,
) -> PgResult<()> {
    // SAFETY: compile-allocated state, exclusive during this step.
    let js = unsafe { &mut *jsestate.as_ptr() };
    if !js.escontext.ctx.error_occurred() {
        return Ok(());
    }
    if js.error.value.as_bool() {
        return Err(behavior_coercion_error(
            "ON ERROR",
            js.on_error_btype,
            js.escontext.ctx.take_error(),
        ));
    }
    if js.empty.value.as_bool() {
        return Err(behavior_coercion_error(
            "ON EMPTY",
            js.on_empty_btype.expect("ON EMPTY coercion implies on_empty"),
            js.escontext.ctx.take_error(),
        ));
    }
    write_out(out, Datum::null(), true);
    js.error.value = Datum::from_bool(true);
    js.escontext.ctx = ::types_error::SoftErrorContext::new(true);
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn behavior_coercion_error(
    clause: &str,
    btype: ::types_nodes::primnodes::JsonBehaviorType,
    saved: Option<PgError>,
) -> Box<PgError> {
    // C GetJsonBehaviorValueString: order matches JsonBehaviorType.
    const NAMES: [&str; 9] = [
        "NULL", "ERROR", "EMPTY", "TRUE", "FALSE", "UNKNOWN", "EMPTY ARRAY", "EMPTY OBJECT",
        "DEFAULT",
    ];
    let mut e = PgError::error(format!(
        "could not coerce {clause} expression ({}) to the RETURNING type",
        NAMES[btype as usize]
    ))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH);
    if let Some(saved) = saved {
        e = e.with_detail(saved.message().to_string());
    }
    Box::new(e)
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
            res = ::adt_json::funcs::json_validate(mcx, json, unique_keys, false)?;
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
        // unported: ExecEvalFieldSelect (execExprInterp.c) expanded-record
        // fastpath (the expandeddatum unit is not ported).
        return Err(PgError::error(
            "field selection from an expanded record is not yet implemented",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into());
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
            // C finishes through HeapTupleHeaderGetDatum (execTuples.c:2413),
            // which re-flattens if any field is an external toast pointer. We
            // skip that check: the input here is a composite DATUM, and the
            // composite-datum law (fill_val's HEAP_HASEXTERNAL + the flatten
            // in heap_copy_tuple_as_datum / eval_row_expr) guarantees its
            // fields are already flat, so the remapped tuple can't be
            // external either.
            debug_assert!(!out_tuple.as_tuple().has_external());
            let d = Datum::from_usize(out_tuple.image().as_ptr() as usize);
            core::mem::forget(out_tuple);
            d
        }
        None => ::heaptuple::heap_copy_tuple_as_datum(mcx, &tuple, outdesc)?,
    };
    core::mem::forget(tuple);
    Ok(result)
}

// ExecEvalFieldStoreDeForm (execExprInterp.c): a NULL input tuple deforms to
// an all-nulls row; the detoasted image lives in the armed per-eval mcx, so
// the deformed column datums stay live through FIELDSTORE_FORM.
#[inline(never)]
#[cold]
fn eval_field_store_deform(
    frames: &mut [crate::steps::FuncFrame<'_>],
    st: &crate::steps::FieldStoreState,
    frame: u32,
    r: NullableDatum,
) -> PgResult<()> {
    use ::types_tuple::{HeapTupleData, HeapTupleHeaderData, ItemPointerData};
    let n = st.ncolumns as usize;
    // SAFETY: compile-allocated workspace of ncolumns slots, sole live access.
    let columns = unsafe { core::slice::from_raw_parts_mut(st.columns.as_ptr(), n) };
    if r.isnull {
        for c in columns {
            *c = NullableDatum { value: Datum::null(), isnull: true };
        }
        return Ok(());
    }
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let p = r.value.as_usize() as *const u8;
    // SAFETY: non-null composite datum; detoast covers short/compressed forms.
    let raw = unsafe { core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p)) };
    // Leaked into the per-eval mcx: the deformed column datums reference the
    // detoasted image until FIELDSTORE_FORM copies them out.
    let rec = ::detoast_seams::detoast_attr::call(mcx, raw)?.leak();
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
    // SAFETY: compile-time blessed tupdesc, plan-mcx-lived.
    let desc = unsafe { st.desc.as_ref() };
    let mut values: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: ::mcx::PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, n)?;
    values.resize(n, Datum::null());
    nulls.resize(n, true);
    ::types_tuple::heap_deform_tuple(&tuple, desc, &mut values, &mut nulls);
    core::mem::forget(tuple);
    for (i, c) in columns.iter_mut().enumerate() {
        *c = NullableDatum { value: values[i], isnull: nulls[i] };
    }
    Ok(())
}

// ExecEvalFieldStoreForm (execExprInterp.c): re-form the composite in the
// armed per-eval result context.
#[inline(never)]
#[cold]
fn eval_field_store_form(
    frames: &mut [crate::steps::FuncFrame<'_>],
    st: &crate::steps::FieldStoreState,
    frame: u32,
) -> PgResult<Datum> {
    let f = &mut frames[frame as usize];
    // SAFETY: the argless frame's fcinfo image is live; armed per eval.
    let mcx = unsafe { fcinfo_mut(f.fcinfo, 0) }.result_mcx();
    let n = st.ncolumns as usize;
    // SAFETY: workspace written by DEFORM + the per-field steps just executed.
    let src = unsafe { core::slice::from_raw_parts(st.columns.as_ptr(), n) };
    let mut values: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: ::mcx::PgVec<'_, bool> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for nd in src {
        values.push(nd.value);
        nulls.push(nd.isnull);
    }
    // SAFETY: compile-time blessed tupdesc, plan-mcx-lived.
    let desc = unsafe { st.desc.as_ref() };
    let tuple = ::heaptuple::heap_form_tuple(mcx, desc, &values, &nulls)?;
    let d = Datum::from_usize(tuple.image().as_ptr() as usize);
    core::mem::forget(tuple);
    Ok(d)
}

// ExecEvalArrayExpr (execExprInterp.c), 1-D leg; the result array lives in
// the armed per-eval result context.
#[allow(clippy::too_many_arguments)]
fn eval_array_expr(
    frames: &mut [crate::steps::FuncFrame<'_>],
    elems: core::ptr::NonNull<NullableDatum>,
    nelems: u32,
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
pub(crate) fn invoke_fusage(call: &FuncCall) -> PgResult<(Datum, bool)> {
    // SAFETY: 'mcx-live mcx-boxed FmgrInfo.
    let fn_oid = unsafe { call.flinfo.as_ref() }.fn_oid;
    let fcu = ::pgstat::function::pgstat_init_function_usage(fn_oid)?;
    let r = invoke(call)?;
    ::pgstat::function::pgstat_end_function_usage(&fcu, true);
    Ok(r)
}

pub(crate) fn invoke(call: &FuncCall) -> PgResult<(Datum, bool)> {
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
                    return ::adt_scalar::datum_ops::datum_copy(mcx, value, false, -1);
                }
                ::types_tuple::varatt::varsize_any(p)
            }
            n if n > 0 => n as usize,
            // cstring (datumGetSize's -2 arm): strlen + terminator.
            -2 => {
                let mut len = 0usize;
                while *p.add(len) != 0 {
                    len += 1;
                }
                len + 1
            }
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

/// [`agg_datum_copy`] of `new` REPLACING a stored by-ref transvalue: C's
/// ExecAggCopyTransValue discipline copies the new value and pfrees the
/// prior one (nodeAgg.c). The deallocate is allocator-exact for the flat
/// varlena copies this module's agg copy paths produce (size =
/// VARSIZE_ANY at align 8 — the same layout `agg_datum_copy` allocated);
/// non-flat forms skip the free, and bump contexts no-op the deallocate
/// either way (bump.c has no BumpFree), so classic-build behavior — bytes
/// AND allocation sequence — is unchanged. On a FREEING context (the sink
/// drains' byref-state child) the pfree bounds the replace churn the way
/// C's aset does: without it every superseded copy accumulates in the
/// never-reset agg context for the build's whole life — the unspillable
/// byref-floor class (the str sibling of the avgpack finding).
///
/// # Safety
/// As [`agg_datum_copy`]; `old` (when non-null-pointer) is a live by-ref
/// datum previously stored by this module's copy paths INTO `mcx`, with no
/// other live reference to it (flushed runs snapshot superseded values
/// never — the whole-table flush law).
pub unsafe fn agg_datum_replace(
    mcx: ::mcx::Mcx<'_>,
    old: Datum,
    new: Datum,
    typlen: i16,
) -> PgResult<Datum> {
    // Copy FIRST (an OOM must leave the stored value intact).
    // SAFETY: forwarded caller contract.
    let copied = unsafe { agg_datum_copy(mcx, new, typlen) }?;
    let op = old.as_usize() as *const u8;
    if !op.is_null() && typlen == -1 {
        // SAFETY: `old` is a live varlena per the caller contract.
        unsafe {
            if !::types_tuple::varatt::varatt_is_1b_e(op) {
                let size = ::types_tuple::varatt::varsize_any(op);
                let layout =
                    core::alloc::Layout::from_size_align(size, 8).expect("datumCopy layout");
                ::mcx::Allocator::deallocate(
                    &mcx,
                    core::ptr::NonNull::new_unchecked(op as *mut u8),
                    layout,
                );
            }
        }
    }
    Ok(copied)
}

// CheckVarSlotCompatibility (execExprInterp.c): C-exact messages.
#[track_caller]
#[cold]
#[inline(never)]
fn var_slot_dropped(attnum: u16, tdtypeid: ::types_core::Oid) -> Box<PgError> {
    let t = format_type::format_type_be(tdtypeid).unwrap_or_else(|_| tdtypeid.to_string());
    Box::new(
        PgError::error(format!("attribute {} of type {t} has been dropped", attnum + 1))
            .with_sqlstate(::types_error::ERRCODE_UNDEFINED_COLUMN),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn var_slot_wrong_type(
    attnum: u16,
    tdtypeid: ::types_core::Oid,
    tabletype: ::types_core::Oid,
    vartype: ::types_core::Oid,
) -> Box<PgError> {
    let f = |o: ::types_core::Oid| {
        format_type::format_type_be(o).unwrap_or_else(|_| o.to_string())
    };
    Box::new(
        PgError::error(format!("attribute {} of type {} has wrong type", attnum + 1, f(tdtypeid)))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
            .with_detail(format!(
                "Table has type {}, but query expects {}.",
                f(tabletype),
                f(vartype)
            )),
    )
}

#[cold]
#[inline(never)]
fn var_slot_out_of_range(attnum: u16, natts: i32) -> ! {
    panic!("attribute number {} exceeds number of columns {natts}", attnum + 1);
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
    check_still_valid_slow(state, slots, &mut RetSlots::none())
}

fn check_still_valid_ret<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    ret: &mut RetSlots<'_, 'mcx>,
) -> PgResult<()> {
    if state.flags & EEO_FLAG_STILL_VALID_CHECKED != 0 {
        return Ok(());
    }
    check_still_valid_slow(state, slots, ret)
}

// Once per compiled expression (C's CheckExprStillValid cost class).
#[inline(never)]
fn check_still_valid_slow<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    ret: &mut RetSlots<'_, 'mcx>,
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
            Step::OldVar { attnum, vartype, .. } => (SlotSrc::Old, attnum, vartype),
            Step::NewVar { attnum, vartype, .. } => (SlotSrc::New, attnum, vartype),
            _ => continue,
        };
        let slot = match src {
            SlotSrc::Old => match &mut ret.old {
                RetSlot::Slot(s) => &mut **s,
                RetSlot::Scan => slots.get(SlotSrc::Scan),
                RetSlot::None => continue,
            },
            SlotSrc::New => match &mut ret.new {
                RetSlot::Slot(s) => &mut **s,
                RetSlot::Scan => slots.get(SlotSrc::Scan),
                RetSlot::None => continue,
            },
            other => slots.get(other),
        };
        let desc = slot
            .base()
            .tts_tupleDescriptor
            .as_ref()
            .expect("var evaluation against a descriptor-less slot");
        if (attnum as i32) >= desc.natts {
            // C: elog(ERROR) — "should never happen".
            var_slot_out_of_range(attnum, desc.natts);
        }
        let attr = &desc.attrs[attnum as usize];
        if attr.attisdropped {
            return Err(var_slot_dropped(attnum, desc.tdtypeid));
        }
        if attr.atttypid != vartype {
            return Err(var_slot_wrong_type(attnum, desc.tdtypeid, attr.atttypid, vartype));
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

// C ExecEvalWholeRowVar. First eval resolves the output descriptor: the
// named-composite leg checks the slot's physical rowtype against the Var's
// declared rowtype (dropped-column storage mismatches downgrade to the
// per-row slow path); the RECORD leg copies the slot's descriptor, adopts
// the RTE eref aliases, and blesses it. Every eval flattens the slot into a
// composite datum in the armed per-eval mcx.
fn eval_whole_row(
    frames: &mut [crate::steps::FuncFrame<'_>],
    slot: &mut SlotData<'_>,
    wr: core::ptr::NonNull<crate::steps::WholeRowState>,
    frame: u32,
) -> PgResult<(Datum, bool)> {
    // SAFETY: compile-allocated state, single-threaded interpreter.
    let wr = unsafe { &mut *wr.as_ptr() };
    // C applies the junkfilter before descriptor capture and flattening.
    let slot: &mut SlotData<'_> = match wr.junk {
        // SAFETY: compile-allocated junk state and slot; the source slot is
        // never the state-owned result slot.
        Some(j) => unsafe {
            let j = j.as_ref();
            // The 'static restamp narrows back to the eval lifetime here.
            let result = &mut *(j.slot.as_ptr() as *mut SlotData<'_>);
            exectuples::slot_getallattrs(slot);
            let old = slot.base();
            exectuples::exec_clear_tuple(result, wr.mcx);
            let rb = result.base_mut();
            for (i, &attno) in j.clean_map.as_ref().iter().enumerate() {
                rb.tts_values[i] = old.tts_values[attno as usize - 1];
                rb.tts_isnull[i] = old.tts_isnull[attno as usize - 1];
            }
            exectuples::exec_store_virtual_tuple(result);
            result
        },
        None => slot,
    };
    if wr.first {
        wr.slow = false;
        let slot_desc =
            slot.base().tts_tupleDescriptor.as_ref().expect("slot has a descriptor").clone();
        if !wr.record {
            // SAFETY: compile-allocated plan-mcx tupdesc, live for the plan.
            let var_desc = unsafe { wr.tupdesc.expect("named leg compiles a tupdesc").as_ref() };
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
        } else {
            let mut desc = ::tupdesc::CreateTupleDescCopy(wr.mcx, slot_desc.as_ref())?;
            // A relation scan slot arrives stamped with the relation's
            // rowtype; we return RECORD.
            desc.tdtypeid = ::types_core::catalog::RECORDOID;
            desc.tdtypmod = -1;
            if let Some(cn) = wr.colnames {
                // SAFETY: plan-lived eref colnames captured at compile.
                exec_type_set_col_names(&mut desc, unsafe { cn.as_ref() });
            }
            ::typcache::assign_record_type_typmod(&mut desc)?;
            let layout = core::alloc::Layout::new::<types_tuple::TupleDescData<'static>>();
            let p: core::ptr::NonNull<types_tuple::TupleDescData<'static>> = wr
                .mcx
                .allocate(layout)
                .map_err(|_| wr.mcx.oom(layout.size()))?
                .cast();
            // SAFETY: fresh exact-layout plan-mcx allocation; the desc is
            // already 'static (wr.mcx is the restamped compile mcx).
            unsafe { p.as_ptr().write(desc) };
            wr.tupdesc = Some(p);
        }
        wr.first = false;
    }
    // SAFETY: compile- or first-eval-allocated plan-mcx tupdesc.
    let var_desc = unsafe { wr.tupdesc.expect("resolved above").as_ref() };
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

// C ExecTypeSetColNames (execTuples.c): overwrite attribute names from the
// alias list; empty aliases and dropped columns keep their names.
pub(crate) fn exec_type_set_col_names(
    desc: &mut ::types_tuple::TupleDescData<'_>,
    colnames: &::types_nodes::list::NodeList<'_>,
) {
    for (colno, cn) in colnames.iter().enumerate() {
        if colno >= desc.natts as usize {
            break;
        }
        let cname = cn.as_string().expect("colnames are String nodes").sval;
        let att = desc.attr_mut(colno);
        if cname.is_empty() || att.attisdropped {
            continue;
        }
        att.attname.namestrcpy(cname);
    }
}

// C errsave (miscnodes.h): with a soft context the error is recorded (details
// only when wanted) and evaluation continues; without one it throws.
fn errsave(
    escontext: Option<core::ptr::NonNull<::types_fmgr::ErrorSaveNode>>,
    err: impl FnOnce() -> Box<PgError>,
) -> PgResult<()> {
    let Some(esc) = escontext else {
        return Err(err());
    };
    // SAFETY: caller contract — the node outlives the program and no other
    // reference is live during this step.
    let ctx = unsafe { &mut (*esc.as_ptr()).ctx };
    if ctx.details_wanted() {
        ctx.save(*err());
    } else {
        ctx.mark_error_occurred();
    }
    Ok(())
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

// ---- Shared step bodies (run_program arm + exec_one_step arm) ----
//
// #[inline(always)] keeps run_program's codegen identical to the previous
// inline form (the interpreter is instruction-count-gated).

#[inline(always)]
// NULLIF: null-or-unequal keeps arg0; strict equality only when both
// non-null (C ExecEvalFuncExpr + NULLIF special case semantics, shared by
// run_program and the JIT single-step tier).
#[inline(always)]
fn step_nullif(call: &FuncCall, out: OutRef) -> PgResult<()> {
    // SAFETY: args 0/1 of the call's live fcinfo image.
    let (a0, a1) = unsafe {
        (
            crate::steps::arg_slot_of(call.fcinfo, 0).read(),
            crate::steps::arg_slot_of(call.fcinfo, 1).read(),
        )
    };
    if a0.isnull || a1.isnull {
        write_out(out, a0.value, a0.isnull);
    } else {
        let (value, isnull) = invoke(call)?;
        if !isnull && value.as_bool() {
            write_out(out, Datum::null(), true);
        } else {
            write_out(out, a0.value, false);
        }
    }
    Ok(())
}

// SQL/JSON safe I/O coercion (ERROR ON ERROR OFF): soft errors land in the
// compile-armed ErrorSaveNode instead of unwinding. Shared by run_program
// and the JIT single-step tier.
#[inline(always)]
fn step_io_coerce_safe(
    calls: core::ptr::NonNull<crate::steps::IoCoerceCalls>,
    out: OutRef,
) -> PgResult<()> {
    // SAFETY: 'mcx-owned pair written once at compile.
    let c = unsafe { calls.as_ref() };
    let nd = read_out(out);
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
    if !c.in_strict || !strv.isnull {
        // SAFETY: arg 0 of the incall's live fcinfo image.
        unsafe {
            crate::steps::arg_slot_of(c.incall.fcinfo, 0)
                .write(NullableDatum { value: strv.value, isnull: nd.isnull })
        };
        let (v, _) = invoke(&c.incall)?;
        // SAFETY: context is the compile-armed ErrorSaveNode
        // (IoCoerceSafe invariant), no other reference live.
        let soft = unsafe { fcinfo_mut(c.incall.fcinfo, 3).soft_error_context() }
            .is_some_and(|ctx| ctx.error_occurred());
        if soft {
            write_out(out, Datum::null(), true);
        } else {
            write_out(out, v, nd.isnull);
        }
    }
    Ok(())
}

fn step_io_coerce(calls: core::ptr::NonNull<crate::steps::IoCoerceCalls>, out: OutRef) -> PgResult<()> {
    // SAFETY: 'mcx-owned pair written once at compile.
    let c = unsafe { calls.as_ref() };
    let nd = read_out(out);
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
        write_out(out, Datum::null(), true);
    } else {
        // SAFETY: arg 0 of the incall's live fcinfo image.
        unsafe { crate::steps::arg_slot_of(c.incall.fcinfo, 0).write(strv) };
        let (v, isnull) = invoke(&c.incall)?;
        write_out(out, v, isnull);
    }
    Ok(())
}

#[inline(always)]
fn step_min_max(
    call: &FuncCall,
    slots: core::ptr::NonNull<NullableDatum>,
    nelems: u32,
    least: bool,
    out: OutRef,
) -> PgResult<()> {
    let mut value = Datum::null();
    let mut isnull = true;
    for off in 0..nelems as usize {
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
        if (cmp > 0 && least) || (cmp < 0 && !least) {
            value = nd.value;
        }
    }
    write_out(out, value, isnull);
    Ok(())
}

/// C ExecEvalMergeSupportFunc (execExprInterp.c): the active MERGE action's
/// command word as a text datum, written into the step's scratch image.
#[inline(always)]
fn step_merge_support_func(
    action: core::ptr::NonNull<Option<::types_nodes::nodes_enums::CmdType>>,
    scratch: core::ptr::NonNull<u8>,
    out: OutRef,
) -> PgResult<()> {
    use ::types_nodes::nodes_enums::CmdType;
    // SAFETY: compile-allocated cell owned by the state, armed by the owning
    // ModifyTable node before each RETURNING projection.
    let word: &[u8; 6] = match unsafe { action.read() } {
        Some(CmdType::CMD_INSERT) => b"INSERT",
        Some(CmdType::CMD_UPDATE) => b"UPDATE",
        Some(CmdType::CMD_DELETE) => b"DELETE",
        Some(CmdType::CMD_NOTHING) => panic!("unexpected merge action: DO NOTHING"),
        Some(other) => panic!("unrecognized commandType: {}", other as i32),
        None => panic!("no merge action in progress"),
    };
    // SAFETY: compile-allocated 12-byte 8-aligned image slot owned by this
    // step; a 10-byte 4-byte-header text varlena is written per eval.
    unsafe {
        let hdr = ::datum::varlena::set_varsize_4b(::datum::varlena::VARHDRSZ + 6);
        core::ptr::copy_nonoverlapping(hdr.as_ptr(), scratch.as_ptr(), 4);
        core::ptr::copy_nonoverlapping(word.as_ptr(), scratch.as_ptr().add(4), 6);
    }
    write_out(out, Datum::from_usize(scratch.as_ptr() as usize), false);
    Ok(())
}

#[inline(always)]
fn step_sql_value_function(
    op: ::types_nodes::primnodes::SQLValueFunctionOp,
    typmod: i32,
    scratch: core::ptr::NonNull<u8>,
    out: OutRef,
) -> PgResult<()> {
    use ::types_nodes::primnodes::SQLValueFunctionOp as Op;
    let value = match op {
        Op::SVFOP_CURRENT_DATE => Datum::from_i32(adt_date::GetSQLCurrentDate()),
        Op::SVFOP_CURRENT_TIME | Op::SVFOP_CURRENT_TIME_N => {
            let t = adt_date::GetSQLCurrentTime(typmod);
            // SAFETY: compile-allocated 12-byte 8-aligned image
            // slot owned by this step (steps.rs note).
            unsafe {
                scratch.as_ptr().cast::<i64>().write(t.time);
                scratch.as_ptr().add(8).cast::<i32>().write(t.zone);
            }
            Datum::from_usize(scratch.as_ptr() as usize)
        }
        Op::SVFOP_CURRENT_TIMESTAMP | Op::SVFOP_CURRENT_TIMESTAMP_N => {
            Datum::from_i64(adt_timestamp::GetSQLCurrentTimestamp(typmod))
        }
        Op::SVFOP_LOCALTIME | Op::SVFOP_LOCALTIME_N => {
            Datum::from_i64(adt_date::GetSQLLocalTime(typmod))
        }
        Op::SVFOP_LOCALTIMESTAMP | Op::SVFOP_LOCALTIMESTAMP_N => {
            Datum::from_i64(adt_timestamp::GetSQLLocalTimestamp(typmod)?)
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
        Op::SVFOP_CURRENT_CATALOG => {
            let dbname = ::dbcommands_seams::get_database_name::call(
                ::init_small::globals::MyDatabaseId(),
            )?
            .expect("current database has a pg_database row");
            let mut name = ::types_tuple::NameData::default();
            name.namestrcpy(&dbname);
            // SAFETY: compile-allocated NameData-sized image slot
            // owned by this step (steps.rs note).
            unsafe {
                scratch.as_ptr().cast::<::types_tuple::NameData>().write(name);
            }
            Datum::from_usize(scratch.as_ptr() as usize)
        }
        Op::SVFOP_CURRENT_SCHEMA => {
            // C current_schema (name.c): first search-path schema, or NULL
            // when the path resolves empty.
            let cx = ::mcx::MemoryContext::new("current_schema");
            let path = ::namespace_seams::fetch_search_path::call(cx.mcx(), false)?;
            let Some(&first) = path.first() else {
                write_out(out, Datum::null(), true);
                return Ok(());
            };
            let Some(nspname) = ::lsyscache::misc::get_namespace_name(cx.mcx(), first)? else {
                write_out(out, Datum::null(), true);
                return Ok(());
            };
            let mut name = ::types_tuple::NameData::default();
            name.namestrcpy(&nspname);
            // SAFETY: compile-allocated NameData-sized image slot
            // owned by this step (steps.rs note).
            unsafe {
                scratch.as_ptr().cast::<::types_tuple::NameData>().write(name);
            }
            Datum::from_usize(scratch.as_ptr() as usize)
        }
    };
    write_out(out, value, false);
    Ok(())
}

#[inline(always)]
fn step_grouping_func(
    cols: core::ptr::NonNull<i32>,
    ncols: u16,
    current: Option<core::ptr::NonNull<crate::steps::GroupedColsCell>>,
    out: OutRef,
) {
    let mut result: i64 = 0;
    if let Some(cell) = current {
        // SAFETY: once-allocated AggState arrays, repointed
        // before projection.
        let (grouped, cols) = unsafe {
            let c = cell.read();
            (
                core::slice::from_raw_parts(c.ptr, c.len),
                core::slice::from_raw_parts(cols.as_ptr(), ncols as usize),
            )
        };
        for &attno in cols {
            result <<= 1;
            if !grouped.contains(&(attno as i16)) {
                result |= 1;
            }
        }
    }
    write_out(out, Datum::from_i32(result as i32), false);
}

#[inline(always)]
fn step_distinct(call: &FuncCall, out: OutRef) -> PgResult<()> {
    // SAFETY: args 0/1 of the call's live fcinfo image.
    let (a0, a1) = unsafe {
        (
            crate::steps::arg_slot_of(call.fcinfo, 0).read(),
            crate::steps::arg_slot_of(call.fcinfo, 1).read(),
        )
    };
    if a0.isnull && a1.isnull {
        write_out(out, Datum::from_bool(false), false);
    } else if a0.isnull || a1.isnull {
        write_out(out, Datum::from_bool(true), false);
    } else {
        let (value, isnull) = invoke(call)?;
        write_out(out, Datum::from_bool(!value.as_bool()), isnull);
    }
    Ok(())
}

#[inline(always)]
fn step_not_distinct(call: &FuncCall, out: OutRef) -> PgResult<()> {
    // SAFETY: args 0/1 of the call's live fcinfo image.
    let (a0, a1) = unsafe {
        (
            crate::steps::arg_slot_of(call.fcinfo, 0).read(),
            crate::steps::arg_slot_of(call.fcinfo, 1).read(),
        )
    };
    if a0.isnull && a1.isnull {
        write_out(out, Datum::from_bool(true), false);
    } else if a0.isnull || a1.isnull {
        write_out(out, Datum::from_bool(false), false);
    } else {
        let (value, isnull) = invoke(call)?;
        write_out(out, value, isnull);
    }
    Ok(())
}

#[inline(always)]
fn step_hash_datum_first(call: &FuncCall, out: OutRef) -> PgResult<()> {
    // SAFETY: arg 0 of the call's live fcinfo image; hash fns
    // never return NULL (C reads fn_addr's Datum directly).
    let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
    let v = if a0.isnull { Datum::null() } else { invoke(call)?.0 };
    write_out(out, v, false);
    Ok(())
}

#[inline(always)]
fn step_hash_datum_next32(
    call: &FuncCall,
    iresult: core::ptr::NonNull<NullableDatum>,
    out: OutRef,
) -> PgResult<()> {
    // SAFETY: iresult is a build-owned once-allocated slot; arg 0
    // as HashDatumFirst.
    let existing = unsafe { iresult.read() }.value.as_u32().rotate_left(1);
    let a0 = unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read() };
    let combined = if a0.isnull {
        existing
    } else {
        existing ^ invoke(call)?.0.as_u32()
    };
    write_out(out, Datum::from_u32(combined), false);
    Ok(())
}

// SAFETY contract: live fcinfo image; `pg` the sole live pergroup pointer.
#[inline(always)]
unsafe fn agg_trans_byval(call: &FuncCall, pg: *mut crate::steps::AggPerGroup) -> PgResult<()> {
    // SAFETY: forwarded caller contract.
    unsafe {
        crate::steps::arg_slot_of(call.fcinfo, 0).write(NullableDatum {
            value: (*pg).trans_value,
            isnull: (*pg).trans_value_is_null,
        });
        let (value, isnull) = invoke(call)?;
        (*pg).trans_value = value;
        (*pg).trans_value_is_null = isnull;
    }
    Ok(())
}

// SAFETY contract: as agg_trans_byval.
#[inline(always)]
unsafe fn agg_trans_strict_byval(
    call: &FuncCall,
    pg: *mut crate::steps::AggPerGroup,
) -> PgResult<()> {
    // SAFETY: forwarded caller contract.
    unsafe {
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
    Ok(())
}

// SAFETY contract: as agg_trans_byval.
#[inline(always)]
unsafe fn agg_trans_init_strict_byval(
    call: &FuncCall,
    pg: *mut crate::steps::AggPerGroup,
) -> PgResult<()> {
    // SAFETY: forwarded caller contract.
    unsafe {
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
    Ok(())
}

// ---- Single-step execution for the copy-and-patch JIT (jit.rs) ----
//
// C llvm_compile_expr's external-function tier: kernel code calls
// jitq_step(env, ix), which executes exactly one interpreter step and
// reports the control flow. Every arm here MUST match run_program's arm for
// the same step byte-for-byte in effect; nontrivial bodies are shared
// helper functions, and this match carries no wildcard so a new Step variant
// fails compilation until both sites are updated. Steps open-coded as
// stencils by the emitter never reach here (their arms panic).

pub(crate) enum StepFlow {
    Next,
    Jump(u32),
    Suspend(core::ptr::NonNull<()>),
}

// exec_one_step's OLD/NEW resolution; borrows the scan Option directly so
// the reborrow outlives the consuming statement (run_program's macros bind
// named locals instead).
fn ret_slot<'r, 'a, 'b, 'mcx>(
    which: &'r mut RetSlot<'a, 'mcx>,
    scan: &'r mut Option<&'b mut SlotData<'mcx>>,
    src: SlotSrc,
) -> &'r mut SlotData<'mcx> {
    match which {
        RetSlot::Scan => match scan {
            Some(s) => s,
            None => missing_slot(src),
        },
        RetSlot::Slot(s) => s,
        RetSlot::None => missing_slot(src),
    }
}

pub(crate) fn exec_one_step<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    ret: &mut RetSlots<'_, 'mcx>,
    mut result_slot: Option<&mut SlotData<'mcx>>,
    ix: u32,
) -> PgResult<StepFlow> {
    // OLD/NEW-is-null bits are rewritten per row by the RETURNING driver:
    // read at every step, exactly like run_program's per-call read.
    let flags = state.flags;
    let ExprState { steps, frames, resnd, saop_tables, .. } = state;
    let res = *resnd;
    let step = steps[ix as usize];
    // run_program's OLD/NEW resolution (RetSlot::Scan aliases the scan slot).
    macro_rules! old_slot {
        () => {
            ret_slot(&mut ret.old, &mut slots.scan, SlotSrc::Old)
        };
    }
    macro_rules! new_slot {
        () => {
            ret_slot(&mut ret.new, &mut slots.scan, SlotSrc::New)
        };
    }
    match step {
        // Open-coded by the emitter (jit.rs step_stencilable).
        Step::DoneReturn
        | Step::DoneNoReturn
        | Step::ScanVar { .. }
        | Step::InnerVar { .. }
        | Step::OuterVar { .. }
        | Step::Const { .. }
        | Step::CaseTestVal { .. }
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
        | Step::FuncExpr { .. }
        | Step::FuncExprStrict1 { .. }
        | Step::FuncExprStrict2 { .. }
        | Step::FuncExprStrict { .. }
        | Step::FuncExprFusage { .. }
        | Step::FuncExprStrictFusage { .. } => {
            unreachable!("stenciled step never routed to the helper")
        }
        // JIT compiles before fuse_program and skips fusion on success
        // (ready_expr), so fused/thinned steps never appear in a jitted
        // program.
        Step::ScanVarFuncStrict2 { .. }
        | Step::FuncFuncStrict2 { .. }
        | Step::FuncStrict2Qual { .. }
        | Step::OuterVarNotDistinct { .. }
        | Step::NotDistinctQual { .. }
        | Step::OuterVarAggTransByValIndirect { .. }
        | Step::AssignScanVar2 { .. }
        | Step::FuncExprStrict1Thin { .. }
        | Step::FuncExprStrict2Thin { .. }
        | Step::ScanVarFuncStrict2Thin { .. }
        | Step::FuncFuncStrict2Thin { .. }
        | Step::FuncStrict2QualThin { .. }
        | Step::OuterVarNotDistinctThin { .. }
        | Step::NotDistinctQualThin { .. }
        | Step::AggTransStrictByValIndirectThin { .. } => {
            unreachable!("step refused by the emitter; never in jitted programs")
        }
        Step::OldFetchSome { last_var } => {
            exectuples::slot_getsomeattrs(old_slot!(), last_var as i32);
        }
        Step::NewFetchSome { last_var } => {
            exectuples::slot_getsomeattrs(new_slot!(), last_var as i32);
        }
        Step::OldVar { attnum, out, .. } => {
            let nd = read_var(old_slot!(), attnum);
            write_out(out, nd.value, nd.isnull);
        }
        Step::NewVar { attnum, out, .. } => {
            let nd = read_var(new_slot!(), attnum);
            write_out(out, nd.value, nd.isnull);
        }
        Step::OldSysVar { attnum, out } => {
            // C ExecEvalSysVar: OLD system attribute is NULL when the OLD
            // row doesn't exist.
            if flags & crate::steps::EEO_FLAG_OLD_IS_NULL != 0 {
                write_out(out, Datum::null(), true);
            } else {
                let mut isnull = false;
                let d = exectuples::slot_getsysattr(old_slot!(), attnum as i32, &mut isnull)?;
                write_out(out, d, isnull);
            }
        }
        Step::NewSysVar { attnum, out } => {
            if flags & crate::steps::EEO_FLAG_NEW_IS_NULL != 0 {
                write_out(out, Datum::null(), true);
            } else {
                let mut isnull = false;
                let d = exectuples::slot_getsysattr(new_slot!(), attnum as i32, &mut isnull)?;
                write_out(out, d, isnull);
            }
        }
        Step::AssignOldVar { attnum, resultnum } => {
            let nd = read_var(old_slot!(), attnum);
            let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
            assign_to_result(rslot, resultnum, nd.value, nd.isnull);
        }
        Step::AssignNewVar { attnum, resultnum } => {
            let nd = read_var(new_slot!(), attnum);
            let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
            assign_to_result(rslot, resultnum, nd.value, nd.isnull);
        }
        Step::ReturningExprStep { nullflag, jumpdone, out } => {
            if flags & nullflag != 0 {
                write_out(out, Datum::null(), true);
                return Ok(StepFlow::Jump(jumpdone));
            }
        }
        Step::NullIf { call, out } => {
            step_nullif(&call, out)?;
        }
        Step::IoCoerceSafe { calls, out } => {
            step_io_coerce_safe(calls, out)?;
        }
        Step::JsonExprPath { jsestate, frame, out } => {
            return Ok(StepFlow::Jump(eval_json_expr_path(frames, jsestate, frame, out)?));
        }
        Step::JsonCoercion { jc, frame, out } => {
            eval_json_coercion(frames, jc, frame, out)?;
        }
        Step::JsonCoercionFinish { jsestate, out } => {
            eval_json_coercion_finish(jsestate, out)?;
        }
        // Combine-phase deserialize (EEOP_AGG_DESERIALIZE): leader-side
        // consumption of worker transstates; same fcinfo/out contract as the
        // interpreter arms.
        Step::AggDeserialize { call, out } => {
            let (value, isnull) = invoke(&call)?;
            write_out(out, value, isnull);
        }
        Step::AggStrictDeserialize { call, out, jumpnull } => {
            // SAFETY: slot 0 of the live 2-arg ds fcinfo image.
            if unsafe { crate::steps::arg_slot_of(call.fcinfo, 0).read().isnull } {
                return Ok(StepFlow::Jump(jumpnull));
            }
            let (value, isnull) = invoke(&call)?;
            write_out(out, value, isnull);
        }
        Step::ScanFetchSome { last_var } => {
            exectuples::slot_getsomeattrs(slots.get(SlotSrc::Scan), last_var as i32);
        }
        Step::InnerFetchSome { last_var } => {
            exectuples::slot_getsomeattrs(slots.get(SlotSrc::Inner), last_var as i32);
        }
        Step::OuterFetchSome { last_var } => {
            exectuples::slot_getsomeattrs(slots.get(SlotSrc::Outer), last_var as i32);
        }
        Step::AssignScanVar { attnum, resultnum } => {
            let nd = read_var(slots.get(SlotSrc::Scan), attnum);
            let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
            assign_to_result(rslot, resultnum, nd.value, nd.isnull);
        }
        Step::AssignInnerVar { attnum, resultnum } => {
            let nd = read_var(slots.get(SlotSrc::Inner), attnum);
            let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
            assign_to_result(rslot, resultnum, nd.value, nd.isnull);
        }
        Step::AssignOuterVar { attnum, resultnum } => {
            let nd = read_var(slots.get(SlotSrc::Outer), attnum);
            let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
            assign_to_result(rslot, resultnum, nd.value, nd.isnull);
        }
        Step::AssignTmp { resultnum } => {
            let rslot = result_slot.as_deref_mut().unwrap_or_else(|| no_result_slot());
            // SAFETY: res is the state's live result cell.
            let r = unsafe { res.read() };
            assign_to_result(rslot, resultnum, r.value, r.isnull);
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
            assign_to_result(rslot, resultnum, value, r.isnull);
        }
        Step::ScanSysVar { attnum, out } => {
            let mut isnull = false;
            let d = exectuples::slot_getsysattr(slots.get(SlotSrc::Scan), attnum as i32, &mut isnull)?;
            write_out(out, d, isnull);
        }
        Step::InnerSysVar { attnum, out } => {
            let mut isnull = false;
            let d =
                exectuples::slot_getsysattr(slots.get(SlotSrc::Inner), attnum as i32, &mut isnull)?;
            write_out(out, d, isnull);
        }
        Step::OuterSysVar { attnum, out } => {
            let mut isnull = false;
            let d =
                exectuples::slot_getsysattr(slots.get(SlotSrc::Outer), attnum as i32, &mut isnull)?;
            write_out(out, d, isnull);
        }
        Step::ParamExtern { prm, out } => {
            // SAFETY: compile-resolved pointer, portal-lived (steps.rs note).
            let p = unsafe { prm.read() };
            write_out(out, p.value, p.isnull);
        }
        Step::ParamExternMissing { paramid } => {
            return Err(crate::compile::no_param_value(paramid));
        }
        Step::ParamExec { prm, out } => {
            // SAFETY: compile-resolved pointer into stable es_param_exec_vals.
            let p = unsafe { prm.read() };
            if p.exec_plan {
                param_exec_plan_pending();
            }
            write_out(out, p.value, p.isnull);
        }
        Step::ParamSet { prm, out } => {
            let r = read_out(out);
            // SAFETY: compile-resolved pointer into stable es_param_exec_vals.
            unsafe {
                let p = prm.as_ptr();
                (*p).value = r.value;
                (*p).isnull = r.isnull;
                (*p).exec_plan = false;
            }
        }
        Step::SubPlan { sstate, out: _ } => return Ok(StepFlow::Suspend(sstate)),
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
        Step::MakeReadonlyOut { src, out } => {
            let r = read_out(src);
            let value = if r.isnull {
                r.value
            } else {
                // SAFETY: non-null by-ref datum = live varlena.
                unsafe { datum::expandeddatum::make_expanded_object_read_only_internal(r.value) }
            };
            write_out(out, value, r.isnull);
        }
        Step::NextValueExpr { seqid, seqtypid, out } => {
            let newval = sequence_seams::nextval_internal::call(seqid, false)?;
            let d = match seqtypid {
                types_core::INT2OID => Datum::from_i16(newval as i16),
                types_core::INT4OID => Datum::from_i32(newval as i32),
                types_core::INT8OID => Datum::from_i64(newval),
                other => panic!("unsupported sequence type {other}"),
            };
            write_out(out, d, false);
        }
        Step::WholeRow { src, wr, frame, out } => {
            // C ExecEvalWholeRowVar: OLD/NEW whole-row is NULL when that
            // row doesn't exist (run_program parity).
            if (matches!(src, SlotSrc::Old) && flags & crate::steps::EEO_FLAG_OLD_IS_NULL != 0)
                || (matches!(src, SlotSrc::New)
                    && flags & crate::steps::EEO_FLAG_NEW_IS_NULL != 0)
            {
                write_out(out, Datum::null(), true);
            } else {
                let slot = match src {
                    SlotSrc::Old => old_slot!(),
                    SlotSrc::New => new_slot!(),
                    other => slots.get(other),
                };
                let (value, isnull) = eval_whole_row(frames, slot, wr, frame)?;
                write_out(out, value, isnull);
            }
        }
        Step::IoCoerce { calls, out } => {
            step_io_coerce(calls, out)?;
        }
        Step::ScalarArrayOp { call, use_or, strict, typlen, typbyval, typalign, out } => {
            let arr = read_out(out);
            let (value, isnull) =
                eval_scalar_array_op(&call, use_or, strict, typlen, typbyval, typalign, arr)?;
            write_out(out, value, isnull);
        }
        Step::HashedScalarArrayOp { call, inclause, typlen, typbyval, typalign, table, out } => {
            let arr = read_out(out);
            let (value, isnull) = eval_hashed_scalar_array_op(
                &mut saop_tables[table as usize],
                &call,
                inclause,
                typlen,
                typbyval,
                typalign,
                arr,
            )?;
            write_out(out, value, isnull);
        }
        Step::ArrayExprStep { elems, nelems, frame, elmtype, elmlen, elmbyval, elmalign, out } => {
            let (value, isnull) =
                eval_array_expr(frames, elems, nelems, frame, elmtype, elmlen, elmbyval, elmalign)?;
            write_out(out, value, isnull);
        }
        Step::RowExprStep { elems, nelems, frame, desc, out } => {
            let (value, isnull) = eval_row_expr(frames, elems, nelems, frame, desc)?;
            write_out(out, value, isnull);
        }
        Step::JsonConstructor { jcstate, frame, out } => {
            eval_json_constructor_step(frames, jcstate, frame, out)?;
        }
        Step::IsJson { exprtype, item_type, unique_keys, frame, out } => {
            eval_is_json_step(frames, exprtype, item_type, unique_keys, frame, out)?;
        }
        Step::XmlExprEval { state: xs, out } => {
            // SAFETY: compile-allocated state, live for the program.
            let st = unsafe { xs.as_ref() };
            let (value, isnull) = crate::xmlops::eval_xml_expr(st)?;
            write_out(out, value, isnull);
        }
        Step::MinMax { call, slots: vals, nelems, least, out } => {
            step_min_max(&call, vals, nelems, least, out)?;
        }
        Step::SqlValueFunction { op, typmod, scratch, out } => {
            step_sql_value_function(op, typmod, scratch, out)?;
        }
        Step::MergeSupportFunc { action, scratch, out } => {
            step_merge_support_func(action, scratch, out)?;
        }
        Step::NullTestRowIsNull { rn, frame, out } => {
            let r = read_out(out);
            let b = eval_row_null(frames, rn, frame, r, true)?;
            write_out(out, Datum::from_bool(b), false);
        }
        Step::NullTestRowIsNotNull { rn, frame, out } => {
            let r = read_out(out);
            let b = eval_row_null(frames, rn, frame, r, false)?;
            write_out(out, Datum::from_bool(b), false);
        }
        Step::FieldSelect { fieldnum, resulttype, frame, out } => {
            let r = read_out(out);
            if !r.isnull {
                let (value, isnull) =
                    eval_field_select(frames, fieldnum, resulttype, frame, r.value)?;
                write_out(out, value, isnull);
            }
        }
        Step::ArrayCoerce { state: acs, out } => {
            let r = read_out(out);
            if !r.isnull {
                // SAFETY: compile-allocated state, sole live access.
                let st = unsafe { &mut *acs.as_ptr() };
                let nd = crate::arrayops::eval_array_coerce(st, r.value)?;
                write_out(out, nd.value, nd.isnull);
            }
        }
        Step::ConvertRowtype { state: crs, frame, out } => {
            let r = read_out(out);
            if !r.isnull {
                // SAFETY: compile-allocated state, sole live access.
                let st = unsafe { crs.as_ref() };
                let v = eval_convert_rowtype(frames, st, frame, r.value)?;
                write_out(out, v, false);
            }
        }
        Step::FieldStoreDeForm { fs, frame, out } => {
            let r = read_out(out);
            // SAFETY: compile-allocated state, sole live access.
            let st = unsafe { fs.as_ref() };
            eval_field_store_deform(frames, st, frame, r)?;
        }
        Step::FieldStoreForm { fs, frame, out } => {
            // SAFETY: compile-allocated state, sole live access.
            let st = unsafe { fs.as_ref() };
            let v = eval_field_store_form(frames, st, frame)?;
            write_out(out, v, false);
        }
        Step::DomainTestval { src, out } => {
            let r = read_out(src);
            write_out(out, r.value, r.isnull);
        }
        Step::DomainNotNull { resulttype, escontext, out } => {
            if read_out(out).isnull {
                // SAFETY: escontext points at the owning JsonExprState's node,
                // live for the program.
                errsave(escontext, || domain_not_null_violation(resulttype))?;
            }
        }
        Step::DomainCheck { resulttype, name, check, escontext } => {
            // SAFETY: compile-allocated scratch, live for 'mcx.
            let r = unsafe { check.read() };
            if !r.isnull && !r.value.as_bool() {
                // SAFETY: name is a compile-copied &'mcx str; escontext as in
                // DomainNotNull.
                errsave(escontext, || {
                    domain_check_violation(resulttype, unsafe { name.as_ref() })
                })?;
            }
        }
        Step::ArrayExprEval { state: aes, out } => {
            // SAFETY: compile-allocated state, live for 'mcx, sole access.
            let st = unsafe { &mut *aes.as_ptr() };
            let r = crate::arrayops::eval_array_expr(st)?;
            write_out(out, r.value, r.isnull);
        }
        Step::SbsrefSubscripts { state: sref, jumpdone, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            if !crate::arrayops::sbsref_check_subscripts(st)? {
                write_out(out, Datum::null(), true);
                return Ok(StepFlow::Jump(jumpdone));
            }
        }
        Step::SbsrefFetch { state: sref, slice, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            let cur = read_out(out);
            let r = if slice {
                crate::arrayops::sbsref_fetch_slice(st, cur)?
            } else {
                crate::arrayops::sbsref_fetch(st, cur)?
            };
            write_out(out, r.value, r.isnull);
        }
        Step::SbsrefOld { state: sref, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            let cur = read_out(out);
            crate::arrayops::sbsref_fetch_old(st, cur)?;
        }
        Step::SbsrefAssign { state: sref, slice, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            let cur = read_out(out);
            let r = if slice {
                crate::arrayops::sbsref_assign_slice(st, cur)?
            } else {
                crate::arrayops::sbsref_assign(st, cur)?
            };
            write_out(out, r.value, r.isnull);
        }
        Step::JsonbSbsrefSubscripts { state: sref, jumpdone, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            if !crate::jsonbsubs::check_subscripts(st)? {
                write_out(out, Datum::null(), true);
                return Ok(StepFlow::Jump(jumpdone));
            }
        }
        Step::JsonbSbsrefFetch { state: sref, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            let cur = read_out(out);
            let r = crate::jsonbsubs::fetch(st, cur)?;
            write_out(out, r.value, r.isnull);
        }
        Step::JsonbSbsrefAssign { state: sref, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            let cur = read_out(out);
            let r = crate::jsonbsubs::assign(st, cur)?;
            write_out(out, r.value, r.isnull);
        }
        Step::HstoreSbsrefFetch { state: sref, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            let cur = read_out(out);
            let r = crate::hstoresubs::fetch(st, cur)?;
            write_out(out, r.value, r.isnull);
        }
        Step::HstoreSbsrefAssign { state: sref, out } => {
            // SAFETY: as ArrayExprEval.
            let st = unsafe { &mut *sref.as_ptr() };
            let cur = read_out(out);
            let r = crate::hstoresubs::assign(st, cur)?;
            write_out(out, r.value, r.isnull);
        }
        Step::Distinct { call, out } => {
            step_distinct(&call, out)?;
        }
        Step::NotDistinct { call, out } => {
            step_not_distinct(&call, out)?;
        }
        Step::AggrefEval { value, null, out } => {
            // SAFETY: pointers into once-allocated AggState arrays (steps.rs note).
            let (v, n) = unsafe { (value.read(), null.read()) };
            write_out(out, v, n);
        }
        Step::GroupingFuncEval { cols, ncols, current, out } => {
            step_grouping_func(cols, ncols, current, out);
        }
        Step::AggSetCurrent { agg, aggref, shared } => {
            // SAFETY: the caller's query-lifetime AggStateNode; no &mut
            // is live across expression evaluation.
            unsafe { agg.as_ref() }.set_current_agg(aggref, shared);
        }
        Step::AggStrictInputCheck { args, nargs, jumpnull } => {
            // SAFETY: args[0..nargs] live fcinfo slots; jumps ready-checked.
            let anynull =
                (0..nargs as usize).any(|i| unsafe { args.as_ptr().add(i).read().isnull });
            if anynull {
                return Ok(StepFlow::Jump(jumpnull));
            }
        }
        Step::AggStrictInputCheck1 { arg, jumpnull } => {
            // SAFETY: as AggStrictInputCheck.
            if unsafe { arg.read().isnull } {
                return Ok(StepFlow::Jump(jumpnull));
            }
        }
        Step::AggOrderedMark { flag } => {
            // SAFETY: nodeagg-owned once-allocated flag slot.
            unsafe { flag.write(true) };
        }
        Step::AggPlainTransByVal { call, pergroup } => {
            // SAFETY: once-allocated stable pergroup; sole access here.
            unsafe { agg_trans_byval(&call, pergroup.as_ptr())? }
        }
        Step::AggPlainTransStrictByVal { call, pergroup } => {
            // SAFETY: as AggPlainTransByVal.
            unsafe { agg_trans_strict_byval(&call, pergroup.as_ptr())? }
        }
        Step::AggPlainTransInitStrictByVal { call, pergroup } => {
            // SAFETY: as AggPlainTransByVal.
            unsafe { agg_trans_init_strict_byval(&call, pergroup.as_ptr())? }
        }
        Step::AggTransByValIndirect { call, base, transno } => {
            // SAFETY: live repointed pergroup cell (run_program contract).
            unsafe { agg_trans_byval(&call, base.read().as_ptr().add(transno as usize))? }
        }
        Step::AggTransStrictByValIndirect { call, base, transno } => {
            // SAFETY: as AggTransByValIndirect.
            unsafe { agg_trans_strict_byval(&call, base.read().as_ptr().add(transno as usize))? }
        }
        Step::AggTransInitStrictByValIndirect { call, base, transno } => {
            // SAFETY: as AggTransByValIndirect.
            unsafe {
                agg_trans_init_strict_byval(&call, base.read().as_ptr().add(transno as usize))?
            }
        }
        Step::AggPlainTransInitStrictByRef { call, pergroup, byref } => {
            // SAFETY: once-allocated stable pergroup, sole access here.
            unsafe {
                let pg = pergroup.as_ptr();
                if (*pg).no_trans_value {
                    agg_init_group(&call, pg, byref)?;
                } else if !(*pg).trans_value_is_null {
                    agg_plain_trans_byref(&call, pg, byref)?;
                }
            }
        }
        Step::AggPlainTransStrictByRef { call, pergroup, byref } => {
            // SAFETY: as AggPlainTransInitStrictByRef.
            unsafe {
                let pg = pergroup.as_ptr();
                if !(*pg).trans_value_is_null {
                    agg_plain_trans_byref(&call, pg, byref)?;
                }
            }
        }
        Step::AggPlainTransByRef { call, pergroup, byref } => {
            // SAFETY: as AggPlainTransInitStrictByRef.
            unsafe { agg_plain_trans_byref(&call, pergroup.as_ptr(), byref)? }
        }
        Step::AggTransInitStrictByRefIndirect { call, base, transno, byref } => {
            // SAFETY: as AggTransByValIndirect + AggPlainTransByRef.
            unsafe {
                let pg = base.read().as_ptr().add(transno as usize);
                if (*pg).no_trans_value {
                    agg_init_group(&call, pg, byref)?;
                } else if !(*pg).trans_value_is_null {
                    agg_plain_trans_byref(&call, pg, byref)?;
                }
            }
        }
        Step::AggTransStrictByRefIndirect { call, base, transno, byref } => {
            // SAFETY: as AggTransInitStrictByRefIndirect.
            unsafe {
                let pg = base.read().as_ptr().add(transno as usize);
                if !(*pg).trans_value_is_null {
                    agg_plain_trans_byref(&call, pg, byref)?;
                }
            }
        }
        Step::AggTransByRefIndirect { call, base, transno, byref } => {
            // SAFETY: as AggTransInitStrictByRefIndirect.
            unsafe {
                agg_plain_trans_byref(&call, base.read().as_ptr().add(transno as usize), byref)?
            }
        }
        Step::HashDatumSetInitVal { init_value, out } => {
            write_out(out, init_value, false);
        }
        Step::HashDatumFirst { call, out } => {
            step_hash_datum_first(&call, out)?;
        }
        Step::HashDatumNext32 { call, iresult, out } => {
            step_hash_datum_next32(&call, iresult, out)?;
        }
        Step::RowCompareStep { call, strict, jumpnull, jumpdone, out } => {
            match eval_row_compare_step(&call, strict)? {
                None => {
                    write_out(out, Datum::null(), true);
                    return Ok(StepFlow::Jump(jumpnull));
                }
                Some(v) => {
                    write_out(out, Datum::from_i32(v), false);
                    if v != 0 {
                        return Ok(StepFlow::Jump(jumpdone));
                    }
                }
            }
        }
        Step::RowCompareFinal { cmptype, out } => {
            let v = eval_row_compare_final(cmptype, read_out(out).value.as_i32());
            write_out(out, Datum::from_bool(v), false);
        }
    }
    Ok(StepFlow::Next)
}

/// Steps exec_one_step executes: every variant that is neither open-coded by
/// the emitter nor a ready-time fused/thinned form (which never appear in
/// jitted programs). Exhaustive on purpose: a new Step variant must be
/// classified here and in exec_one_step before it compiles.
pub(crate) fn step_has_helper(step: &Step) -> bool {
    match step {
        Step::DoneReturn
        | Step::DoneNoReturn
        | Step::ScanVar { .. }
        | Step::InnerVar { .. }
        | Step::OuterVar { .. }
        | Step::Const { .. }
        | Step::CaseTestVal { .. }
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
        | Step::FuncExpr { .. }
        | Step::FuncExprStrict1 { .. }
        | Step::FuncExprStrict2 { .. }
        | Step::FuncExprStrict { .. }
        | Step::FuncExprFusage { .. }
        | Step::FuncExprStrictFusage { .. } => false,
        Step::ScanVarFuncStrict2 { .. }
        | Step::FuncFuncStrict2 { .. }
        | Step::FuncStrict2Qual { .. }
        | Step::OuterVarNotDistinct { .. }
        | Step::NotDistinctQual { .. }
        | Step::OuterVarAggTransByValIndirect { .. }
        | Step::AssignScanVar2 { .. }
        | Step::FuncExprStrict1Thin { .. }
        | Step::FuncExprStrict2Thin { .. }
        | Step::ScanVarFuncStrict2Thin { .. }
        | Step::FuncFuncStrict2Thin { .. }
        | Step::FuncStrict2QualThin { .. }
        | Step::OuterVarNotDistinctThin { .. }
        | Step::NotDistinctQualThin { .. }
        | Step::AggTransStrictByValIndirectThin { .. } => false,
        Step::NullIf { .. }
        | Step::JsonExprPath { .. }
        | Step::JsonCoercion { .. }
        | Step::JsonCoercionFinish { .. }
        | Step::IoCoerceSafe { .. } => true,
        Step::AggDeserialize { .. } | Step::AggStrictDeserialize { .. } => true,
        Step::OldFetchSome { .. }
        | Step::NewFetchSome { .. }
        | Step::OldVar { .. }
        | Step::NewVar { .. }
        | Step::OldSysVar { .. }
        | Step::NewSysVar { .. }
        | Step::AssignOldVar { .. }
        | Step::AssignNewVar { .. }
        | Step::ReturningExprStep { .. } => true,
        Step::ScanFetchSome { .. }
        | Step::InnerFetchSome { .. }
        | Step::OuterFetchSome { .. }
        | Step::ScanSysVar { .. }
        | Step::InnerSysVar { .. }
        | Step::OuterSysVar { .. }
        | Step::AssignScanVar { .. }
        | Step::AssignInnerVar { .. }
        | Step::AssignOuterVar { .. }
        | Step::AssignTmp { .. }
        | Step::AssignTmpMakeRo { .. }
        | Step::ParamExtern { .. }
        | Step::ParamExternMissing { .. }
        | Step::ParamExec { .. }
        | Step::ParamSet { .. }
        | Step::SubPlan { .. }
        | Step::MakeReadonly { .. }
        | Step::MakeReadonlyOut { .. }
        | Step::NextValueExpr { .. }
        | Step::WholeRow { .. }
        | Step::IoCoerce { .. }
        | Step::ScalarArrayOp { .. }
        | Step::HashedScalarArrayOp { .. }
        | Step::ArrayExprStep { .. }
        | Step::RowExprStep { .. }
        | Step::JsonConstructor { .. }
        | Step::IsJson { .. }
        | Step::XmlExprEval { .. }
        | Step::MinMax { .. }
        | Step::SqlValueFunction { .. }
        | Step::MergeSupportFunc { .. }
        | Step::NullTestRowIsNull { .. }
        | Step::NullTestRowIsNotNull { .. }
        | Step::FieldSelect { .. }
        | Step::ArrayCoerce { .. }
        | Step::ConvertRowtype { .. }
        | Step::FieldStoreDeForm { .. }
        | Step::FieldStoreForm { .. }
        | Step::DomainTestval { .. }
        | Step::DomainNotNull { .. }
        | Step::DomainCheck { .. }
        | Step::ArrayExprEval { .. }
        | Step::SbsrefSubscripts { .. }
        | Step::SbsrefFetch { .. }
        | Step::SbsrefOld { .. }
        | Step::SbsrefAssign { .. }
        | Step::JsonbSbsrefSubscripts { .. }
        | Step::JsonbSbsrefFetch { .. }
        | Step::JsonbSbsrefAssign { .. }
        | Step::HstoreSbsrefFetch { .. }
        | Step::HstoreSbsrefAssign { .. }
        | Step::Distinct { .. }
        | Step::NotDistinct { .. }
        | Step::AggrefEval { .. }
        | Step::GroupingFuncEval { .. }
        | Step::AggSetCurrent { .. }
        | Step::AggStrictInputCheck { .. }
        | Step::AggStrictInputCheck1 { .. }
        | Step::AggOrderedMark { .. }
        | Step::AggPlainTransByVal { .. }
        | Step::AggPlainTransStrictByVal { .. }
        | Step::AggPlainTransInitStrictByVal { .. }
        | Step::AggTransByValIndirect { .. }
        | Step::AggTransStrictByValIndirect { .. }
        | Step::AggTransInitStrictByValIndirect { .. }
        | Step::AggPlainTransInitStrictByRef { .. }
        | Step::AggPlainTransStrictByRef { .. }
        | Step::AggPlainTransByRef { .. }
        | Step::AggTransInitStrictByRefIndirect { .. }
        | Step::AggTransStrictByRefIndirect { .. }
        | Step::AggTransByRefIndirect { .. }
        | Step::HashDatumSetInitVal { .. }
        | Step::HashDatumFirst { .. }
        | Step::HashDatumNext32 { .. }
        | Step::RowCompareStep { .. }
        | Step::RowCompareFinal { .. } => true,
    }
}
