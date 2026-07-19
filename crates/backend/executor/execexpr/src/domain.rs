//! domains.c domain_check_input engine: adt_domains sits under fmgr_core and
//! reaches this through typcache_seams::domain_check_input. The per-domain
//! memo is this backend's fn_extra analog (rule 5), keyed by domain OID and
//! revalidated against the dcc identity per call (UpdateDomainConstraintRef).

use core::cell::RefCell;
use core::mem::ManuallyDrop;
use core::ptr::NonNull;

use ::datum::{Datum, NullableDatum};
use ::mcx::{Mcx, MemoryContext, PgBox, PgHashMap};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_nodes::Node;
use ::types_portal::params::ParamBind;

use crate::interp::{domain_check_violation, domain_not_null_violation, exec_eval_expr, EvalSlots};
use crate::steps::{ExprState, OutRef, Step};

struct CompiledCheck {
    name: &'static str,
    slot: NonNull<NullableDatum>,
    state: PgBox<'static, ExprState<'static>>,
}

struct DomainMemo {
    cref: typcache::DomainConstraintRef,
    dcc_addr: usize,
    typlen: i16,
    checks: Vec<CompiledCheck>,
}

struct EngineState {
    mcx: Mcx<'static>,
    // C evaluates checks in the caller's per-tuple context; this scratch is
    // the same lifetime class, reset at depth 0 (checks can re-enter for
    // nested domains).
    scratch: NonNull<MemoryContext>,
    depth: u32,
    memos: PgHashMap<'static, Oid, DomainMemo>,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<EngineState>>> = const { RefCell::new(None) };
}

fn with_state<R>(f: impl FnOnce(&mut EngineState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("DomainCheckEngine").mcx();
            // The scratch NonNull must carry WRITE provenance: depth-0 calls
            // reset() through it (as_mut below). session_root's shared
            // &'static only grants SharedReadOnly — the _mut variant is the
            // reset-per-use scratch-holder shape (regexp's RegexpExecScratch).
            let scratch =
                NonNull::from(::mcx::session_root_mut(MemoryContext::new("DomainCheckScratch")));
            ManuallyDrop::new(EngineState {
                mcx,
                scratch,
                depth: 0,
                memos: PgHashMap::with_capacity_in(4, mcx),
            })
        });
        f(st)
    })
}

// The standalone check program: CoerceToDomainValue reads a dedicated slot
// (the EXT step's econtext channel collapsed to compile-time wiring).
fn compile_check(
    mcx: Mcx<'static>,
    expr: Node<'static>,
    name: &'static str,
) -> PgResult<CompiledCheck> {
    let slot = crate::compile::alloc_nullable_datum(mcx)?;
    let mut state = ExprState::new_boxed_in(mcx)?;
    crate::compile::create_expr_setup_steps(&mut state, mcx, &[expr], None, ParamBind::NONE, None)?;
    state.innermost_domain = Some(OutRef(slot));
    let rout = state.result_out();
    crate::compile::init_expr_rec(expr, &mut state, mcx, rout, None, ParamBind::NONE, None)?;
    crate::compile::push_step(&mut state, mcx, Step::DoneReturn)?;
    crate::compile::ready_expr(&mut state);
    Ok(CompiledCheck { name, slot, state })
}

// ALTER DOMAIN validation programs (typecmds.c validateDomainCheckConstraint's
// ExecPrepareExpr): same wiring as compile_check, caller-lifetime.
pub struct DomainCheckExpr<'mcx> {
    slot: NonNull<NullableDatum>,
    state: PgBox<'mcx, ExprState<'mcx>>,
}

impl<'mcx> DomainCheckExpr<'mcx> {
    pub fn eval(&mut self, value: Datum, isnull: bool) -> PgResult<NullableDatum> {
        // SAFETY: slot lives in the program's mcx alongside the state.
        unsafe { self.slot.as_ptr().write(NullableDatum { value, isnull }) };
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        exec_eval_expr(&mut self.state, &mut slots)
    }
}

pub fn prepare_domain_check_expr<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Node<'mcx>,
) -> PgResult<DomainCheckExpr<'mcx>> {
    let slot = crate::compile::alloc_nullable_datum(mcx)?;
    let mut state = ExprState::new_boxed_in(mcx)?;
    crate::compile::create_expr_setup_steps(&mut state, mcx, &[expr], None, ParamBind::NONE, None)?;
    state.innermost_domain = Some(OutRef(slot));
    let rout = state.result_out();
    crate::compile::init_expr_rec(expr, &mut state, mcx, rout, None, ParamBind::NONE, None)?;
    crate::compile::push_step(&mut state, mcx, Step::DoneReturn)?;
    crate::compile::ready_expr(&mut state);
    // By-ref call results (record detoast, coercions) land in the caller's
    // validation-scan mcx (C: per-tuple econtext).
    state.arm_result_mcx(mcx);
    Ok(DomainCheckExpr { slot, state })
}

fn rebuild_memo(memo: &mut DomainMemo, mcx: Mcx<'static>) -> PgResult<()> {
    // Old programs leak into the engine mcx (constraint changes are DDL-rare;
    // typcache's dcc takes the same stance).
    memo.checks.clear();
    for con in memo.cref.constraints() {
        if con.constrainttype == typcache::DomConstraintType::Check {
            let expr = con.check_expr.expect("CHECK DomainConstraintState carries check_expr");
            memo.checks.push(compile_check(mcx, expr, con.name)?);
        }
    }
    memo.dcc_addr = memo.cref.dcc_addr();
    memo.typlen = memo.cref.typlen();
    Ok(())
}

/// typcache_seams::domain_check_input target (domains.c domain_check_input).
pub fn domain_check_input(
    value: Datum,
    isnull: bool,
    domain_type: Oid,
    escontext: Option<&mut ::types_error::SoftErrorContext>,
) -> PgResult<()> {
    let present = with_state(|st| st.memos.contains_key(&domain_type));
    if !present {
        let cref = typcache::DomainConstraintRef::init(domain_type)?;
        let mut memo =
            DomainMemo { cref, dcc_addr: usize::MAX, typlen: 0, checks: Vec::new() };
        let mcx = with_state(|st| st.mcx);
        rebuild_memo(&mut memo, mcx)?;
        with_state(|st| st.memos.insert(domain_type, memo));
    } else {
        let (changed, mcx) = with_state(|st| {
            let memo = st.memos.get_mut(&domain_type).unwrap();
            (memo.cref.update(), st.mcx)
        });
        if changed? {
            let mut memo = with_state(|st| st.memos.remove(&domain_type)).unwrap();
            rebuild_memo(&mut memo, mcx)?;
            with_state(|st| st.memos.insert(domain_type, memo));
        }
    }

    // Evaluate with the memo temporarily out of the map: a CHECK expression
    // may re-enter this engine for a different domain (map growth would move
    // entries). Ownership round-trips instead of a raw pointer.
    let mut memo = with_state(|st| st.memos.remove(&domain_type)).unwrap();
    let (scratch, depth) = with_state(|st| {
        if st.depth == 0 {
            // SAFETY: single-threaded TLS engine at depth 0 — no live
            // borrows of scratch allocations survive a completed call.
            unsafe { st.scratch.as_mut().reset() };
        }
        st.depth += 1;
        (st.scratch, st.depth)
    });
    let _ = depth;
    // SAFETY: the leaked scratch context is 'static; reset only at depth 0.
    let scratch_mcx = unsafe { scratch.as_ref() }.mcx();
    let result = run_checks(&mut memo, scratch_mcx, value, isnull, domain_type, escontext);
    with_state(|st| {
        st.depth -= 1;
        st.memos.insert(domain_type, memo)
    });
    result
}

fn run_checks(
    memo: &mut DomainMemo,
    scratch_mcx: Mcx<'_>,
    value: Datum,
    isnull: bool,
    domain_type: Oid,
    mut escontext: Option<&mut ::types_error::SoftErrorContext>,
) -> PgResult<()> {
    let mut check_ix = 0;
    for con in memo.cref.constraints() {
        match con.constrainttype {
            typcache::DomConstraintType::NotNull => {
                if isnull {
                    return ::types_error::ereturn(
                        escontext.as_deref_mut(),
                        (),
                        *domain_not_null_violation(domain_type),
                    );
                }
            }
            typcache::DomConstraintType::Check => {
                let check = &mut memo.checks[check_ix];
                check_ix += 1;
                let ro = if isnull {
                    value
                } else {
                    // SAFETY: non-null datum of the domain's own typlen class.
                    unsafe {
                        ::datum::expandeddatum::make_expanded_object_read_only(
                            value, isnull, memo.typlen,
                        )
                    }
                };
                // SAFETY: slot is an engine-mcx allocation owned by this memo.
                unsafe { check.slot.write(NullableDatum { value: ro, isnull }) };
                // SAFETY: scratch outlives this eval; re-armed every call.
                unsafe { check.state.arm_result_mcx_raw(scratch_mcx) };
                let r = exec_eval_expr(&mut check.state, &mut EvalSlots::default())?;
                // C ExecCheck: NULL is not a failure.
                if !r.isnull && !r.value.as_bool() {
                    return ::types_error::ereturn(
                        escontext.as_deref_mut(),
                        (),
                        *domain_check_violation(domain_type, check.name),
                    );
                }
            }
        }
    }
    Ok(())
}
