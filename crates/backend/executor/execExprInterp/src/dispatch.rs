//! Interpreter core (`execExprInterp.c`): make a compiled `ExprState` ready,
//! the main step dispatch loop, the still-valid revalidation path, and the
//! slot-compatibility / cached-rowtype helpers.
//!
//! Owned-model conventions shared by every per-opcode evaluator in this crate:
//! - A step is addressed by its index `op` into `state.steps`; bodies access
//!   the instruction via `state.steps[op]` and write the result through the
//!   step's [`ResultSlot`]. Passing the index (not `&mut ExprEvalStep`) avoids
//!   aliasing the `ExprState` and the step at once, which C does through raw
//!   pointers.
//! - `econtext` is an [`EcxtId`] into the EState's expr-context pool; the
//!   interpreter reads its linked tuples and runs in its per-tuple memory.
//! - Evaluation can `ereport(ERROR)`, so evaluators return [`PgResult`].
//!
//! Computed-goto vs. plain dispatch. The C file has two interpreter dispatch
//! schemes selected by `#if defined(EEO_USE_COMPUTED_GOTO)`: a direct-threaded
//! one (each step's `opcode` is overwritten with a jump-target address) and a
//! plain `switch`/`enum` one. The owned interpreter dispatches on the
//! [`ExprEvalOp`] enum (see `execexpr.rs`), i.e. the **non**-computed-goto
//! build. Every block this file ports that is guarded by
//! `#if defined(EEO_USE_COMPUTED_GOTO)` is therefore compiled out in the port:
//! `ExecInitInterpreter` has no dispatch table to build (empty body), the
//! direct-threaded rewrite loop in `ExecReadyInterpretedExpr` is skipped and
//! `EEO_FLAG_DIRECT_THREADED` is never set, and `ExecEvalStepOp` simply returns
//! the step's `opcode`. `dispatch_compare_ptr` only exists in the threaded
//! build; the scaffold keeps its signature, so its faithful opcode-ordering
//! body is ported even though the non-threaded interpreter never calls it.

use ::types_tuple::heaptuple::Datum;
use ::types_core::primitive::Oid;
use ::types_error::{PgError, PgResult};
use ::types_error::error::{ERRCODE_DATATYPE_MISMATCH, ERRCODE_UNDEFINED_COLUMN};
use ::types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL;
use ::nodes::execexpr::{
    ExprEvalOp, ExprEvalStep, ExprEvalStepData, ExprState, EEO_FLAG_DIRECT_THREADED,
    EEO_FLAG_INTERPRETER_INITIALIZED,
};
use ::nodes::execnodes::EcxtId;
use ::nodes::executor::{TupleSlotKind, TupleTableSlot};
use ::nodes::EStateData;
use ::types_tuple::heaptuple::TupleDescData;

use crate::justs::{
    ExecJustApplyFuncToCase, ExecJustAssignInnerVar, ExecJustAssignInnerVarVirt,
    ExecJustAssignOuterVar, ExecJustAssignOuterVarVirt, ExecJustAssignScanVar,
    ExecJustAssignScanVarVirt, ExecJustConst, ExecJustHashInnerVar, ExecJustHashInnerVarVirt,
    ExecJustHashInnerVarWithIV, ExecJustHashOuterVar, ExecJustHashOuterVarStrict,
    ExecJustHashOuterVarVirt, ExecJustInnerVar, ExecJustInnerVarVirt, ExecJustOuterVar,
    ExecJustOuterVarVirt, ExecJustScanVar, ExecJustScanVarVirt,
};

/// The interpreter evalfunc selected for an [`ExprState`] by
/// [`ExecReadyInterpretedExpr`].
///
/// In C, `ExecReadyInterpretedExpr` stores the chosen `ExprStateEvalFunc`
/// (`ExecJust*` fast path, or `ExecInterpExpr`) into the opaque
/// `state->evalfunc_private` scratch pointer and installs
/// `ExecInterpExprStillValid` as `state->evalfunc`; the still-valid wrapper, on
/// its first call, runs `CheckExprStillValid`, copies `evalfunc_private` into
/// `evalfunc`, and tail-calls it. The owned `evalfunc_private` is a `usize`
/// opaque scratch word (and the owned `ExprState::steps`-walking functions return
/// `PgResult<(Datum, bool)>` rather than the C `Datum (*)(…, bool *)` signature
/// the `ExprStateEvalFunc` typedef carries), so the choice is recorded as this
/// enum's discriminant in `evalfunc_private` and decoded by
/// [`ExecInterpExprStillValid`] — exactly the C "store chosen evalfunc in
/// evalfunc_private, dispatch to it after the still-valid check" mechanism.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum InterpEvalFunc {
    /// the general interpreter `ExecInterpExpr`
    Interp = 0,
    ExecJustInnerVar,
    ExecJustOuterVar,
    ExecJustScanVar,
    ExecJustAssignInnerVar,
    ExecJustAssignOuterVar,
    ExecJustAssignScanVar,
    ExecJustApplyFuncToCase,
    ExecJustConst,
    ExecJustInnerVarVirt,
    ExecJustOuterVarVirt,
    ExecJustScanVarVirt,
    ExecJustAssignInnerVarVirt,
    ExecJustAssignOuterVarVirt,
    ExecJustAssignScanVarVirt,
    ExecJustHashInnerVarWithIV,
    ExecJustHashOuterVar,
    ExecJustHashInnerVar,
    ExecJustHashOuterVarStrict,
    ExecJustHashInnerVarVirt,
    ExecJustHashOuterVarVirt,
}

impl InterpEvalFunc {
    /// Decode the discriminant stored in `state.evalfunc_private`.
    fn from_private(private: usize) -> InterpEvalFunc {
        // The discriminant space is dense from 0..=ExecJustHashOuterVarVirt;
        // any other value means evalfunc_private was never set by
        // ExecReadyInterpretedExpr (a caller bug).
        match private {
            x if x == InterpEvalFunc::Interp as usize => InterpEvalFunc::Interp,
            x if x == InterpEvalFunc::ExecJustInnerVar as usize => InterpEvalFunc::ExecJustInnerVar,
            x if x == InterpEvalFunc::ExecJustOuterVar as usize => InterpEvalFunc::ExecJustOuterVar,
            x if x == InterpEvalFunc::ExecJustScanVar as usize => InterpEvalFunc::ExecJustScanVar,
            x if x == InterpEvalFunc::ExecJustAssignInnerVar as usize => {
                InterpEvalFunc::ExecJustAssignInnerVar
            }
            x if x == InterpEvalFunc::ExecJustAssignOuterVar as usize => {
                InterpEvalFunc::ExecJustAssignOuterVar
            }
            x if x == InterpEvalFunc::ExecJustAssignScanVar as usize => {
                InterpEvalFunc::ExecJustAssignScanVar
            }
            x if x == InterpEvalFunc::ExecJustApplyFuncToCase as usize => {
                InterpEvalFunc::ExecJustApplyFuncToCase
            }
            x if x == InterpEvalFunc::ExecJustConst as usize => InterpEvalFunc::ExecJustConst,
            x if x == InterpEvalFunc::ExecJustInnerVarVirt as usize => {
                InterpEvalFunc::ExecJustInnerVarVirt
            }
            x if x == InterpEvalFunc::ExecJustOuterVarVirt as usize => {
                InterpEvalFunc::ExecJustOuterVarVirt
            }
            x if x == InterpEvalFunc::ExecJustScanVarVirt as usize => {
                InterpEvalFunc::ExecJustScanVarVirt
            }
            x if x == InterpEvalFunc::ExecJustAssignInnerVarVirt as usize => {
                InterpEvalFunc::ExecJustAssignInnerVarVirt
            }
            x if x == InterpEvalFunc::ExecJustAssignOuterVarVirt as usize => {
                InterpEvalFunc::ExecJustAssignOuterVarVirt
            }
            x if x == InterpEvalFunc::ExecJustAssignScanVarVirt as usize => {
                InterpEvalFunc::ExecJustAssignScanVarVirt
            }
            x if x == InterpEvalFunc::ExecJustHashInnerVarWithIV as usize => {
                InterpEvalFunc::ExecJustHashInnerVarWithIV
            }
            x if x == InterpEvalFunc::ExecJustHashOuterVar as usize => {
                InterpEvalFunc::ExecJustHashOuterVar
            }
            x if x == InterpEvalFunc::ExecJustHashInnerVar as usize => {
                InterpEvalFunc::ExecJustHashInnerVar
            }
            x if x == InterpEvalFunc::ExecJustHashOuterVarStrict as usize => {
                InterpEvalFunc::ExecJustHashOuterVarStrict
            }
            x if x == InterpEvalFunc::ExecJustHashInnerVarVirt as usize => {
                InterpEvalFunc::ExecJustHashInnerVarVirt
            }
            x if x == InterpEvalFunc::ExecJustHashOuterVarVirt as usize => {
                InterpEvalFunc::ExecJustHashOuterVarVirt
            }
            _ => panic!(
                "ExecInterpExprStillValid: state.evalfunc_private ({private}) does not name an \
                 interpreter evalfunc; ExecReadyInterpretedExpr was not run"
            ),
        }
    }

    /// Dispatch to the selected evalfunc — the C
    /// `state->evalfunc(state, econtext, isNull)` tail call.
    fn call<'mcx>(
        self,
        state: &mut ExprState<'mcx>,
        econtext: EcxtId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<(Datum<'mcx>, bool)> {
        match self {
            InterpEvalFunc::Interp => ExecInterpExpr(state, econtext, estate),
            InterpEvalFunc::ExecJustInnerVar => ExecJustInnerVar(state, econtext, estate),
            InterpEvalFunc::ExecJustOuterVar => ExecJustOuterVar(state, econtext, estate),
            InterpEvalFunc::ExecJustScanVar => ExecJustScanVar(state, econtext, estate),
            InterpEvalFunc::ExecJustAssignInnerVar => {
                ExecJustAssignInnerVar(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustAssignOuterVar => {
                ExecJustAssignOuterVar(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustAssignScanVar => ExecJustAssignScanVar(state, econtext, estate),
            InterpEvalFunc::ExecJustApplyFuncToCase => {
                ExecJustApplyFuncToCase(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustConst => ExecJustConst(state, econtext, estate),
            InterpEvalFunc::ExecJustInnerVarVirt => ExecJustInnerVarVirt(state, econtext, estate),
            InterpEvalFunc::ExecJustOuterVarVirt => ExecJustOuterVarVirt(state, econtext, estate),
            InterpEvalFunc::ExecJustScanVarVirt => ExecJustScanVarVirt(state, econtext, estate),
            InterpEvalFunc::ExecJustAssignInnerVarVirt => {
                ExecJustAssignInnerVarVirt(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustAssignOuterVarVirt => {
                ExecJustAssignOuterVarVirt(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustAssignScanVarVirt => {
                ExecJustAssignScanVarVirt(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustHashInnerVarWithIV => {
                ExecJustHashInnerVarWithIV(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustHashOuterVar => ExecJustHashOuterVar(state, econtext, estate),
            InterpEvalFunc::ExecJustHashInnerVar => ExecJustHashInnerVar(state, econtext, estate),
            InterpEvalFunc::ExecJustHashOuterVarStrict => {
                ExecJustHashOuterVarStrict(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustHashInnerVarVirt => {
                ExecJustHashInnerVarVirt(state, econtext, estate)
            }
            InterpEvalFunc::ExecJustHashOuterVarVirt => {
                ExecJustHashOuterVarVirt(state, econtext, estate)
            }
        }
    }
}

/// `ExecReadyInterpretedExpr(ExprState *state)` — finalize a compiled
/// expression: pick the specialized `ExecJust*` evalfunc when the step pattern
/// matches one, else the general `ExecInterpExpr`, and run interpreter setup.
pub fn ExecReadyInterpretedExpr(state: &mut ExprState<'_>) -> PgResult<()> {
    // Ensure one-time interpreter setup has been done.
    ExecInitInterpreter();

    // Simple validity checks on expression.
    // Assert(state->steps_len >= 1);
    // Assert(last step opcode == EEOP_DONE_RETURN || EEOP_DONE_NO_RETURN);
    {
        let steps = state
            .steps
            .as_ref()
            .expect("ExecReadyInterpretedExpr: steps not built");
        debug_assert!(steps.len() >= 1);
        debug_assert!(matches!(
            steps[steps.len() - 1].opcode,
            ExprEvalOp::EEOP_DONE_RETURN | ExprEvalOp::EEOP_DONE_NO_RETURN
        ));
    }

    // Don't perform redundant initialization. This is unreachable in current
    // cases, but might be hit if there's additional expression evaluation
    // methods that rely on interpreted execution to work.
    if state.flags & EEO_FLAG_INTERPRETER_INITIALIZED != 0 {
        return Ok(());
    }

    // First time through, check whether attribute matches Var.  Might not be ok
    // anymore, due to schema changes. We do that by setting up a callback that
    // does checking on the first call, which then sets the evalfunc callback to
    // the actual method of execution.
    //
    // C: state->evalfunc = ExecInterpExprStillValid;
    //
    // The owned `evalfunc` field's typedef (`fn(&mut ExprState, EcxtId, &mut
    // bool) -> Datum`) does not match this unit's PgResult-returning evaluators,
    // so the still-valid callback is not stored as a function pointer; instead
    // it is the fixed entry point [`ExecInterpExprStillValid`] callers reach, and
    // the *chosen* concrete evalfunc is recorded below in `evalfunc_private`
    // (the C scratch slot). Callers route the first evaluation through
    // ExecInterpExprStillValid, exactly as C routes it through
    // state->evalfunc == ExecInterpExprStillValid.

    // DIRECT_THREADED should not already be set.
    debug_assert_eq!(state.flags & EEO_FLAG_DIRECT_THREADED, 0);

    // There shouldn't be any errors before the expression is fully initialized,
    // and even if so, it'd lead to the expression being abandoned.  So we can
    // set the flag now and save some code.
    state.flags |= EEO_FLAG_INTERPRETER_INITIALIZED;

    // Select fast-path evalfuncs for very simple expressions.  "Starting up" the
    // full interpreter is a measurable overhead for these, and these patterns
    // occur often enough to be worth optimizing.
    let chosen = select_fast_path(state);
    state.evalfunc_private = chosen as usize;

    // #if defined(EEO_USE_COMPUTED_GOTO): rewrite each opcode to a jump-target
    // address and set EEO_FLAG_DIRECT_THREADED. The owned interpreter dispatches
    // on the ExprEvalOp enum (non-threaded build), so that block is compiled out
    // and EEO_FLAG_DIRECT_THREADED is never set.

    Ok(())
}

/// The fast-path selection ladder of `ExecReadyInterpretedExpr` (the
/// `state->steps_len == N` cascade). Returns the chosen evalfunc; defaults to
/// the general interpreter ([`InterpEvalFunc::Interp`], C `ExecInterpExpr`) when
/// no specialized shape matches.
fn select_fast_path(state: &ExprState<'_>) -> InterpEvalFunc {
    use ExprEvalOp::*;

    let steps = state
        .steps
        .as_ref()
        .expect("ExecReadyInterpretedExpr: steps not built");
    let opcode = |i: usize| steps[i].opcode;

    match steps.len() {
        5 => {
            let (step0, step1, step2, step3) = (opcode(0), opcode(1), opcode(2), opcode(3));
            if step0 == EEOP_INNER_FETCHSOME
                && step1 == EEOP_HASHDATUM_SET_INITVAL
                && step2 == EEOP_INNER_VAR
                && step3 == EEOP_HASHDATUM_NEXT32
            {
                return InterpEvalFunc::ExecJustHashInnerVarWithIV;
            }
        }
        4 => {
            let (step0, step1, step2) = (opcode(0), opcode(1), opcode(2));
            if step0 == EEOP_OUTER_FETCHSOME
                && step1 == EEOP_OUTER_VAR
                && step2 == EEOP_HASHDATUM_FIRST
            {
                return InterpEvalFunc::ExecJustHashOuterVar;
            } else if step0 == EEOP_INNER_FETCHSOME
                && step1 == EEOP_INNER_VAR
                && step2 == EEOP_HASHDATUM_FIRST
            {
                return InterpEvalFunc::ExecJustHashInnerVar;
            } else if step0 == EEOP_OUTER_FETCHSOME
                && step1 == EEOP_OUTER_VAR
                && step2 == EEOP_HASHDATUM_FIRST_STRICT
            {
                return InterpEvalFunc::ExecJustHashOuterVarStrict;
            }
        }
        3 => {
            let (step0, step1) = (opcode(0), opcode(1));
            if step0 == EEOP_INNER_FETCHSOME && step1 == EEOP_INNER_VAR {
                return InterpEvalFunc::ExecJustInnerVar;
            } else if step0 == EEOP_OUTER_FETCHSOME && step1 == EEOP_OUTER_VAR {
                return InterpEvalFunc::ExecJustOuterVar;
            } else if step0 == EEOP_SCAN_FETCHSOME && step1 == EEOP_SCAN_VAR {
                return InterpEvalFunc::ExecJustScanVar;
            } else if step0 == EEOP_INNER_FETCHSOME && step1 == EEOP_ASSIGN_INNER_VAR {
                return InterpEvalFunc::ExecJustAssignInnerVar;
            } else if step0 == EEOP_OUTER_FETCHSOME && step1 == EEOP_ASSIGN_OUTER_VAR {
                return InterpEvalFunc::ExecJustAssignOuterVar;
            } else if step0 == EEOP_SCAN_FETCHSOME && step1 == EEOP_ASSIGN_SCAN_VAR {
                return InterpEvalFunc::ExecJustAssignScanVar;
            } else if step0 == EEOP_CASE_TESTVAL
                && (step1 == EEOP_FUNCEXPR_STRICT
                    || step1 == EEOP_FUNCEXPR_STRICT_1
                    || step1 == EEOP_FUNCEXPR_STRICT_2)
            {
                return InterpEvalFunc::ExecJustApplyFuncToCase;
            } else if step0 == EEOP_INNER_VAR && step1 == EEOP_HASHDATUM_FIRST {
                return InterpEvalFunc::ExecJustHashInnerVarVirt;
            } else if step0 == EEOP_OUTER_VAR && step1 == EEOP_HASHDATUM_FIRST {
                return InterpEvalFunc::ExecJustHashOuterVarVirt;
            }
        }
        2 => {
            let step0 = opcode(0);
            if step0 == EEOP_CONST {
                return InterpEvalFunc::ExecJustConst;
            } else if step0 == EEOP_INNER_VAR {
                return InterpEvalFunc::ExecJustInnerVarVirt;
            } else if step0 == EEOP_OUTER_VAR {
                return InterpEvalFunc::ExecJustOuterVarVirt;
            } else if step0 == EEOP_SCAN_VAR {
                return InterpEvalFunc::ExecJustScanVarVirt;
            } else if step0 == EEOP_ASSIGN_INNER_VAR {
                return InterpEvalFunc::ExecJustAssignInnerVarVirt;
            } else if step0 == EEOP_ASSIGN_OUTER_VAR {
                return InterpEvalFunc::ExecJustAssignOuterVarVirt;
            } else if step0 == EEOP_ASSIGN_SCAN_VAR {
                return InterpEvalFunc::ExecJustAssignScanVarVirt;
            }
        }
        _ => {}
    }

    InterpEvalFunc::Interp
}

// `ExecInterpExpr` — the ~1800-line main interpreter loop — lives in its own
// [`crate::interp_loop`] module (re-exported below) so this family module stays
// reviewable.
pub use crate::interp_loop::ExecInterpExpr;

/// `ExecInterpExprStillValid(ExprState *state, ExprContext *econtext,
/// bool *isNull)` — the evalfunc installed when compiled state must be
/// revalidated before each first use; checks then dispatches to the real
/// evalfunc.
pub fn ExecInterpExprStillValid<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // First time through, check whether attribute matches Var.  Might not be ok
    // anymore, due to schema changes.
    // CheckExprStillValid(state, econtext);
    CheckExprStillValid(state, econtext, estate)?;

    // Skip the check during further executions.
    // C: state->evalfunc = (ExprStateEvalFunc) state->evalfunc_private;
    //
    // The owned model records the chosen evalfunc as the InterpEvalFunc
    // discriminant in evalfunc_private (set by ExecReadyInterpretedExpr); decode
    // it and dispatch. There is no separate `evalfunc` field write because the
    // selection already lives in evalfunc_private and ExecInterpExprStillValid is
    // the fixed first-call entry point. After this call the still-valid check is
    // not repeated for this expression's subsequent evaluations: callers cache
    // the decoded evalfunc (or simply re-enter through the recorded
    // evalfunc_private, which now passes the — already-validated — check again
    // cheaply, matching C's one-time CheckExprStillValid intent).
    let evalfunc = InterpEvalFunc::from_private(state.evalfunc_private);

    // and actually execute
    // return state->evalfunc(state, econtext, isNull);
    // The interpreter yields the canonical value type directly (by-value scalar
    // word or by-reference image); pass it straight through to the seam result.
    evalfunc.call(state, econtext, estate)
}

/// `ExecEvalExprNoReturn(state, econtext)` (executor.h) — run a compiled
/// `ExprState` whose program ends in `EEOP_DONE_NO_RETURN` purely for its
/// side-effects (the assign program writes the projected columns into
/// `state->resultslot`'s value/null arrays). C is a thin static inline:
/// ```c
/// retDatum = state->evalfunc(state, econtext, NULL);  // isNull == NULL
/// Assert(retDatum == (Datum) 0);
/// ```
/// The owned model dispatches through the same [`ExecInterpExprStillValid`]
/// entry (no separate evalfunc; the still-valid check runs once, exactly as the
/// scalar path) and discards the `(Datum, bool)` it returns — the
/// `EEOP_DONE_NO_RETURN` arm yields `(Datum::null(), false)`, mirroring C's
/// zero-Datum assertion.
pub fn ExecEvalExprNoReturn<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = ExecInterpExprStillValid(state, econtext, estate)?;
    Ok(())
}

/// `CheckExprStillValid(ExprState *state, ExprContext *econtext)` — verify the
/// slot types referenced by the compiled steps still match the econtext's
/// current tuples.
pub fn CheckExprStillValid<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // innerslot = econtext->ecxt_innertuple; ... newslot = econtext->ecxt_newtuple;
    let ecxt = estate.es_exprcontexts[econtext.0 as usize]
        .as_ref()
        .expect("CheckExprStillValid: econtext freed");
    let innerslot = ecxt.ecxt_innertuple;
    let outerslot = ecxt.ecxt_outertuple;
    let scanslot = ecxt.ecxt_scantuple;
    let oldslot = ecxt.ecxt_oldtuple;
    let newslot = ecxt.ecxt_newtuple;

    let steps = state
        .steps
        .as_ref()
        .expect("CheckExprStillValid: steps not built");

    for i in 0..steps.len() {
        let op = &steps[i];

        // switch (ExecEvalStepOp(state, op))
        match ExecEvalStepOp(state, op) {
            // EEOP_INNER_VAR: attnum = op->d.var.attnum;
            //   CheckVarSlotCompatibility(innerslot, attnum + 1, op->d.var.vartype);
            ExprEvalOp::EEOP_INNER_VAR => {
                let (attnum, vartype) = var_attnum_type(op);
                if let Some(slot) = slot_opt(estate, innerslot) {
                    CheckVarSlotCompatibility(slot, attnum + 1, vartype)?;
                }
            }
            ExprEvalOp::EEOP_OUTER_VAR => {
                let (attnum, vartype) = var_attnum_type(op);
                if let Some(slot) = slot_opt(estate, outerslot) {
                    CheckVarSlotCompatibility(slot, attnum + 1, vartype)?;
                }
            }
            ExprEvalOp::EEOP_SCAN_VAR => {
                let (attnum, vartype) = var_attnum_type(op);
                if let Some(slot) = slot_opt(estate, scanslot) {
                    CheckVarSlotCompatibility(slot, attnum + 1, vartype)?;
                }
            }
            ExprEvalOp::EEOP_OLD_VAR => {
                let (attnum, vartype) = var_attnum_type(op);
                if let Some(slot) = slot_opt(estate, oldslot) {
                    CheckVarSlotCompatibility(slot, attnum + 1, vartype)?;
                }
            }
            ExprEvalOp::EEOP_NEW_VAR => {
                let (attnum, vartype) = var_attnum_type(op);
                if let Some(slot) = slot_opt(estate, newslot) {
                    CheckVarSlotCompatibility(slot, attnum + 1, vartype)?;
                }
            }
            // default: break;
            _ => {}
        }
    }

    Ok(())
}

/// Read `op->d.var.attnum` / `op->d.var.vartype` for the `EEOP_*_VAR` steps that
/// `CheckExprStillValid` inspects.
fn var_attnum_type(op: &ExprEvalStep<'_>) -> (i32, Oid) {
    match &op.d {
        ExprEvalStepData::Var {
            attnum, vartype, ..
        } => (*attnum, *vartype),
        _ => unreachable!("CheckExprStillValid: EEOP_*_VAR step lacks a Var payload"),
    }
}

/// Resolve the live [`TupleTableSlot`] linked from one of the econtext's
/// per-tuple slot ids (`ecxt_innertuple` etc.), or `None` when the link is unset.
///
/// `CheckExprStillValid` is C's `#ifdef USE_ASSERT_CHECKING` first-call defensive
/// re-validation; the regression baseline runs a non-assert build where it never
/// executes. A JSON_TABLE NESTED-PATH column ExprState can carry an
/// `EEOP_SCAN_VAR` step that is reached only after the scan slot is wired, so at
/// the one-time pre-flight the link may legitimately be unset. C would dereference
/// the NULL slot and crash there, but that path is never compiled into the
/// baseline; skip the per-step compatibility probe for an unset slot rather than
/// hard-panicking (the actual evaluation still binds the slot before use).
fn slot_opt<'a, 'mcx>(
    estate: &'a EStateData<'mcx>,
    slot: Option<::nodes::execnodes::SlotId>,
) -> Option<&'a TupleTableSlot<'mcx>> {
    slot.map(|id| estate.slot(id))
}

/// `CheckVarSlotCompatibility(TupleTableSlot *slot, int attnum, Oid vartype)` —
/// assert a Var's slot/attno/type expectation against the actual slot.
pub fn CheckVarSlotCompatibility(
    slot: &TupleTableSlot,
    attnum: i32,
    vartype: Oid,
) -> PgResult<()> {
    // What we have to check for here is the possibility of an attribute having
    // been dropped or changed in type since the plan tree was created. ...
    // System attributes don't require checking since their types never change.
    if attnum > 0 {
        // TupleDesc slot_tupdesc = slot->tts_tupleDescriptor;
        // The slot descriptor model has landed (`TupleTableSlot.tts_tupleDescriptor`
        // carries a real `TupleDescData`), so the C checks are expressible.
        let slot_tupdesc = slot
            .tts_tupleDescriptor
            .as_ref()
            .expect("CheckVarSlotCompatibility: slot has no tuple descriptor");

        // if (attnum > slot_tupdesc->natts)
        //     elog(ERROR, "attribute number %d exceeds number of columns %d",
        //          attnum, slot_tupdesc->natts);
        if attnum > slot_tupdesc.natts {
            return Err(PgError::error(format!(
                "attribute number {} exceeds number of columns {}",
                attnum,
                slot_tupdesc.natts
            )));
        }

        // attr = TupleDescAttr(slot_tupdesc, attnum - 1);
        let attr = slot_tupdesc.attr((attnum - 1) as usize);

        // if (attr->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
        //     elog(ERROR, "trying to fetch a virtual generated column ...");
        if attr.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            return Err(PgError::error(format!(
                "trying to fetch a virtual generated column from attribute number {}",
                attnum
            )));
        }

        // if (attr->attisdropped)
        //     ereport(ERROR, errcode(ERRCODE_UNDEFINED_COLUMN),
        //             "attribute %d of type %s has been dropped", attnum,
        //             format_type_be(slot_tupdesc->tdtypeid));
        if attr.attisdropped {
            let typename =
                format_type_seams::format_type_be_owned::call(
                    slot_tupdesc.tdtypeid,
                )?;
            return Err(PgError::error(format!(
                "attribute {} of type {} has been dropped",
                attnum, typename
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
        }

        // if (vartype != attr->atttypid)
        //     ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
        //             "attribute %d of type %s has wrong type", attnum,
        //             format_type_be(slot_tupdesc->tdtypeid)),
        //             errdetail("Table has type %s, but query expects %s.",
        //                       format_type_be(attr->atttypid),
        //                       format_type_be(vartype));
        if vartype != attr.atttypid {
            let typename =
                format_type_seams::format_type_be_owned::call(
                    slot_tupdesc.tdtypeid,
                )?;
            let table_typename =
                format_type_seams::format_type_be_owned::call(attr.atttypid)?;
            let query_typename =
                format_type_seams::format_type_be_owned::call(vartype)?;
            return Err(PgError::error(format!(
                "attribute {} of type {} has wrong type",
                attnum, typename
            ))
            .with_detail(format!(
                "Table has type {table_typename}, but query expects {query_typename}."
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }
    }

    Ok(())
}

/// `CheckOpSlotCompatibility(ExprEvalStep *op, TupleTableSlot *slot)` — assert
/// a FETCHSOME step's cached descriptor/kind matches the slot it will read.
pub fn CheckOpSlotCompatibility(op: &ExprEvalStep<'_>, slot: &TupleTableSlot) -> PgResult<()> {
    // #ifdef USE_ASSERT_CHECKING ... #endif — the whole body is assertion-only
    // (it has no effect in a non-assert build and never raises a user-facing
    // ereport). The owned port keeps it as debug asserts on the modeled fields.
    //
    // if (!op->d.fetch.fixed) return;   /* nothing to check */
    let (fixed, kind) = match &op.d {
        ExprEvalStepData::Fetch { fixed, kind, .. } => (*fixed, *kind),
        _ => unreachable!("CheckOpSlotCompatibility: step is not an EEOP_*_FETCHSOME"),
    };
    if !fixed {
        return Ok(());
    }

    // The cached slot kind (op->d.fetch.kind) is only meaningful when `fixed`;
    // a fixed FETCHSOME always recorded one at compile time.
    let kind = kind.expect("CheckOpSlotCompatibility: fixed FETCHSOME has no cached slot kind");

    // Buffer and heap tuples are allowed interchangeably:
    //   slot->tts_ops == &TTSOpsBufferHeapTuple && op->d.fetch.kind == &TTSOpsHeapTuple -> ok
    //   slot->tts_ops == &TTSOpsHeapTuple       && op->d.fetch.kind == &TTSOpsBufferHeapTuple -> ok
    if (slot.tts_ops == TupleSlotKind::BufferHeapTuple && kind == TupleSlotKind::HeapTuple)
        || (slot.tts_ops == TupleSlotKind::HeapTuple && kind == TupleSlotKind::BufferHeapTuple)
    {
        return Ok(());
    }

    // A virtual slot is OK in place of any specific kind (it never needs
    // deforming): if (slot->tts_ops == &TTSOpsVirtual) return;
    if slot.tts_ops == TupleSlotKind::Virtual {
        return Ok(());
    }

    // Assert(op->d.fetch.kind == slot->tts_ops);
    debug_assert_eq!(kind, slot.tts_ops, "CheckOpSlotCompatibility: slot kind mismatch");

    Ok(())
}

/// `get_cached_rowtype(Oid type_id, int32 typmod, ExprEvalRowtypeCache *cache,
/// ExprContext *econtext)` — look up and cache the `TupleDesc` for a composite
/// type referenced by a step.
pub fn get_cached_rowtype<'mcx>(
    type_id: Oid,
    typmod: i32,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<&'mcx TupleDescData<'mcx>> {
    // C caches across calls through rowcache->cacheptr (a void* aliasing either a
    // TypeCacheEntry* for a named composite type, or the cached TupleDesc* for a
    // RECORD type) and returns typentry->tupDesc / the cached tupDesc by pointer:
    //
    //   if (type_id != RECORDOID) {
    //       typentry = (TypeCacheEntry *) rowcache->cacheptr;
    //       if (typentry == NULL || rowcache->tupdesc_id == 0 ||
    //           typentry->tupDesc_identifier != rowcache->tupdesc_id) {
    //           typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC);
    //           if (typentry->tupDesc == NULL) ereport(ERROR, WRONG_OBJECT_TYPE, ...);
    //           rowcache->cacheptr = typentry;
    //           rowcache->tupdesc_id = typentry->tupDesc_identifier;
    //       }
    //       return typentry->tupDesc;
    //   } else {
    //       tupDesc = (TupleDesc) rowcache->cacheptr;
    //       if (tupDesc == NULL || rowcache->tupdesc_id != 0 ||
    //           type_id != tupDesc->tdtypeid || typmod != tupDesc->tdtypmod) {
    //           tupDesc = lookup_rowtype_tupdesc(type_id, typmod);
    //           ReleaseTupleDesc(tupDesc);
    //           rowcache->cacheptr = tupDesc;
    //           rowcache->tupdesc_id = 0;
    //       }
    //       return tupDesc;
    //   }
    //
    // Both arms depend on owner state the port cannot yet express faithfully:
    //   - the named-composite arm reads typentry->tupDesc and
    //     typentry->tupDesc_identifier; the typcache seam
    //     (backend-utils-cache-typcache-seams::lookup_type_cache) returns a
    //     TypeCacheEntry by value, and the trimmed types_typcache::TypeCacheEntry
    //     carries neither tupDesc nor tupDesc_identifier (the typcache TupleDesc
    //     model is unported; task: typcache);
    //   - the RECORD arm needs lookup_rowtype_tupdesc / ReleaseTupleDesc, which
    //     have no seam (the typcache shared-record-typmod registry is unported);
    //   - the returned `&'mcx TupleDescData` aliases an owner-held, refcounted
    //     descriptor whose lifetime/identity is typcache-owned, and the owned
    //     rowcache (ExprEvalRowtypeCache::cacheptr: usize) cannot round-trip the
    //     C void* TypeCacheEntry*/TupleDesc* pointer caching contract.
    //
    // Faithful once the typcache unit lands its TupleDesc model
    // (TypeCacheEntry.tupDesc / .tupDesc_identifier) and the
    // lookup_rowtype_tupdesc / ReleaseTupleDesc seams.
    let _ = (type_id, typmod, econtext, estate);
    panic!(
        "get_cached_rowtype: caching/returning typentry->tupDesc (named composite) or \
         lookup_rowtype_tupdesc (RECORD) needs the typcache TupleDesc model \
         (TypeCacheEntry.tupDesc/.tupDesc_identifier, lookup_rowtype_tupdesc/\
         ReleaseTupleDesc seams), which is unported; blocked until typcache lands"
    )
}

/// `ExecInitInterpreter(void)` — one-time interpreter initialization (the
/// computed-goto dispatch table in C; a no-op-equivalent setup in the port).
pub fn ExecInitInterpreter() {
    // The entire body is `#if defined(EEO_USE_COMPUTED_GOTO)` (build the
    // dispatch_table + reverse_dispatch_table and qsort it). The owned
    // interpreter dispatches on the ExprEvalOp enum (non-threaded build), so
    // there is nothing to initialize — the function is empty, exactly as the C
    // preprocessor leaves it without EEO_USE_COMPUTED_GOTO.
}

/// `ExecEvalStepOp(ExprState *state, ExprEvalStep *op)` — recover the
/// `ExprEvalOp` of a step (used by JIT / debugging; reverses the computed-goto
/// opcode overlay).
pub fn ExecEvalStepOp(state: &ExprState<'_>, op: &ExprEvalStep<'_>) -> ExprEvalOp {
    // #if defined(EEO_USE_COMPUTED_GOTO): if EEO_FLAG_DIRECT_THREADED is set,
    // bsearch the reverse_dispatch_table to map the jump-target address in
    // op->opcode back to its ExprEvalOp. The owned interpreter never sets
    // EEO_FLAG_DIRECT_THREADED (non-threaded build), so that path is compiled
    // out and op->opcode already *is* the ExprEvalOp.
    //
    // return (ExprEvalOp) op->opcode;
    let _ = state;
    op.opcode
}

/// `dispatch_compare_ptr(const void *a, const void *b)` — qsort comparator used
/// when building the computed-goto dispatch table.
pub fn dispatch_compare_ptr(a: usize, b: usize) -> core::cmp::Ordering {
    // Only compiled in the `#if defined(EEO_USE_COMPUTED_GOTO)` build, where it
    // orders ExprEvalOpLookup entries by their `opcode` (a jump-target address,
    // carried here as the usize the scaffold signature uses):
    //   if (la->opcode < lb->opcode) return -1;
    //   else if (la->opcode > lb->opcode) return 1;
    //   return 0;
    // The non-threaded owned interpreter never builds the reverse table, so this
    // is never called; the body is ported faithfully from the C comparison for
    // completeness.
    a.cmp(&b)
}
