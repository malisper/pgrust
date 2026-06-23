//! Seam declarations for the `backend-executor-execExprInterp` unit
//! (`executor/execExprInterp.c`) — the expression *interpreter*, the cycle
//! partner of `execExpr` (the compiler).
//!
//! `execExpr` finishes compiling an `ExprState` by calling
//! `ExecReadyInterpretedExpr` (sets `state->evalfunc` to the interpreter
//! entry), and the executor evaluates a compiled program by calling the
//! interpreter dispatch (`ExecInterpExpr` / `ExecEvalExprSwitchContext`).
//! Both live in execExprInterp; `execExpr` and the executor nodes call them
//! through these seams. Until execExprInterp lands, a call panics loudly.

#![allow(non_snake_case)]

use nodes::execexpr::ExprState;
use nodes::EStateData;

seam_core::seam!(
    /// `ExecReadyInterpretedExpr(state)` (execExprInterp.c): finalize a freshly
    /// compiled `ExprState` for interpreted execution — pick the specialized
    /// `evalfunc` based on the step program and set
    /// `EEO_FLAG_INTERPRETER_INITIALIZED`. Called by `execExpr`'s `ExecReadyExpr`
    /// once the steps array (terminated by an `EEOP_DONE_*` step) is complete.
    pub fn exec_ready_interpreted_expr<'mcx>(
        state: &mut ExprState<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecEvalExprSwitchContext(state, econtext, &isnull)` (executor.h /
    /// execExprInterp.c): run a compiled `ExprState`'s step program in
    /// `econtext`'s per-tuple memory and return the result `Datum` + is-null.
    /// This is the interpreter's main dispatch loop; it is the only thing that
    /// understands the `ExprEvalStep` opcodes. Can `ereport(ERROR)`.
    pub fn exec_eval_expr_switch_context<'mcx>(
        state: &mut ExprState<'mcx>,
        econtext: nodes::EcxtId,
        estate: &mut EStateData<'mcx>,
    ) -> types_error::PgResult<(
        types_tuple::heaptuple::Datum<'mcx>,
        bool,
    )>
);

seam_core::seam!(
    /// `ExecEvalExprNoReturn(state, econtext)` (executor.h / execExprInterp.c):
    /// run a compiled `ExprState` whose program ends in `EEOP_DONE_NO_RETURN`
    /// (e.g. a projection's assign program) in `econtext`'s per-tuple memory,
    /// writing its results directly into `state->resultslot` rather than
    /// returning a `Datum`. This is the interpreter entry `ExecProject`'s inline
    /// invokes after `ExecClearTuple(slot)`. Can `ereport(ERROR)`.
    pub fn exec_eval_expr_no_return<'mcx>(
        state: &mut ExprState<'mcx>,
        econtext: nodes::EcxtId,
        estate: &mut EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
