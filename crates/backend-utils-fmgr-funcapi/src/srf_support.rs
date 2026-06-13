//! Set Returning Function (SRF) plumbing — `funcapi.c` lines 58–256.
//!
//! `InitMaterializedSRF` (materialize-mode tuplestore + descriptor setup) and
//! the `*_MultiFuncCall` cross-call helpers that thread a [`FuncCallContext`]
//! across fmgr calls via `flinfo->fn_extra` and register an `ExprContext`
//! shutdown callback. Also hosts the two thin fmgr-call-frame seam readers the
//! inward seam crate exposes (`srf_arg0_oid`, `cstring_get_text_datum`).

use mcx::Mcx;
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::{FuncCallContext, ReturnSetInfo};

/// `InitMaterializedSRF(fcinfo, flags)` (funcapi.c:76) — sanity-check the
/// `ReturnSetInfo` at `fcinfo->resultinfo`, create the materialize-mode
/// Tuplestore and result `TupleDesc` in the per-query context, and store them
/// back (`returnMode`/`setResult`/`setDesc`). `MAT_SRF_USE_EXPECTED_DESC` uses
/// `rsinfo->expectedDesc`; `MAT_SRF_BLESS` blesses a transient RECORD desc.
pub fn InitMaterializedSRF<'mcx>(
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    _flags: u32,
) -> PgResult<()> {
    todo!("funcapi.c:76 InitMaterializedSRF")
}

/// The `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls)`
/// append against an `InitMaterializedSRF`-prepared `ReturnSetInfo`. funcapi
/// resolves `setResult`/`setDesc`; the append delegates to the tuplestore unit.
pub fn materialized_srf_putvalues<'mcx>(
    _rsinfo: &mut ReturnSetInfo<'mcx>,
    _values: &[Datum],
    _nulls: &[bool],
) -> PgResult<()> {
    todo!("funcapi.c InitMaterializedSRF callers: tuplestore_putvalues append")
}

/// `init_MultiFuncCall(PG_FUNCTION_ARGS)` (funcapi.c:133) — first-call setup:
/// verify the `ReturnSetInfo` context, create the long-lived multi-call memory
/// context, allocate and zero a [`FuncCallContext`], stash it in
/// `flinfo->fn_extra`, and register [`shutdown_MultiFuncCall`] on the
/// `ExprContext`. `elog(ERROR)` on a second call.
pub fn init_MultiFuncCall<'mcx>(
    _mcx: Mcx<'mcx>,
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<FuncCallContext<'mcx>> {
    todo!("funcapi.c:133 init_MultiFuncCall")
}

/// `per_MultiFuncCall(PG_FUNCTION_ARGS)` (funcapi.c:208) — return the
/// `FuncCallContext` saved in `flinfo->fn_extra` for the current per-call step.
pub fn per_MultiFuncCall<'mcx>(
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<FuncCallContext<'mcx>> {
    todo!("funcapi.c:208 per_MultiFuncCall")
}

/// `end_MultiFuncCall(PG_FUNCTION_ARGS, funcctx)` (funcapi.c:220) — deregister
/// the shutdown callback and run [`shutdown_MultiFuncCall`] to tear down the
/// multi-call context.
pub fn end_MultiFuncCall<'mcx>(
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    _funcctx: &mut FuncCallContext<'mcx>,
) -> PgResult<()> {
    todo!("funcapi.c:220 end_MultiFuncCall")
}

/// `shutdown_MultiFuncCall(Datum arg)` (funcapi.c:238) — the `ExprContext`
/// shutdown callback: unbind `flinfo->fn_extra` and delete the multi-call
/// memory context (freeing the `FuncCallContext` itself).
pub fn shutdown_MultiFuncCall<'mcx>(_funcctx: &mut FuncCallContext<'mcx>) -> PgResult<()> {
    todo!("funcapi.c:238 shutdown_MultiFuncCall")
}

/// `PG_ARGISNULL(0) ? None : Some(PG_GETARG_OID(0))` — read the optional
/// leading `Oid` argument of a SQL-callable function (the SRF-filter helper the
/// inward seam exposes). fmgr owns the `args`/`isnull` arrays.
pub fn srf_arg0_oid<'mcx>(_fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Option<Oid> {
    todo!("funcapi SRF arg0 OID read (PG_GETARG_OID(0))")
}

/// `CStringGetTextDatum(s)` — build a `text *` Datum from a string in `mcx`
/// (the SRF text-column helper the inward seam exposes).
pub fn cstring_get_text_datum<'mcx>(_mcx: Mcx<'mcx>, _s: &str) -> PgResult<Datum> {
    todo!("funcapi CStringGetTextDatum")
}
