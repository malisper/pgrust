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

/// C `ExecEvalExprSwitchContext`/`ExecInterpExprStillValid`: one-time Var
/// validity check, then kernel dispatch.
#[inline(always)]
pub fn exec_eval_expr<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
) -> PgResult<NullableDatum> {
    check_still_valid(state, slots)?;
    eval(state, slots, None)
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
    let r = eval(state, slots, None)?;
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
    eval(state, slots, Some(result_slot))?;
    exectuples::exec_store_virtual_tuple(result_slot);
    Ok(())
}

#[inline(always)]
fn eval<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    result_slot: Option<&mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    match state.kernel {
        Kernel::Program => run_program(state, slots, result_slot),
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

// The interpreter: flat step array walked by a pointer cursor, loop { match }
// over the dense tags (perf-doctrine rule 12), enregisterable (cursor,
// result) state; slot bindings hoisted out of the loop as C does.
#[inline(never)]
fn run_program<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    mut result_slot: Option<&mut SlotData<'mcx>>,
) -> PgResult<NullableDatum> {
    let ExprState { steps, frames, .. } = state;
    let steps = steps.as_slice();
    let mut scan = slots.scan.as_deref_mut();
    let mut inner = slots.inner.as_deref_mut();
    let mut outer = slots.outer.as_deref_mut();
    let mut regs = ResultRegs { value: Datum::null(), isnull: true };
    let base = steps.as_ptr();
    let mut sp = base;
    loop {
        // SAFETY: ready_expr validated Done-termination and every jump
        // target; the cursor only advances by 1 or to a validated target.
        let step = unsafe { &*sp };
        match step {
            Step::DoneReturn => {
                return Ok(NullableDatum { value: regs.value, isnull: regs.isnull })
            }
            Step::DoneNoReturn => return Ok(NullableDatum::null()),
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
            Step::Qual { jumpdone } => {
                if regs.isnull || !regs.value.as_bool() {
                    regs.value = Datum::from_bool(false);
                    regs.isnull = false;
                    // SAFETY: jump targets validated < steps.len() at ready.
                    sp = unsafe { base.add(*jumpdone as usize) };
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
