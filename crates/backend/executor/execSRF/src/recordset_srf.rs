//! Executor-frame registration of the materialize-mode `json_to_recordset` /
//! `jsonb_to_recordset` SRFs (jsonfuncs.c:3995/3981 `populate_recordset_worker`
//! with `have_record_arg = false`).
//!
//! These build a *set* of composite values (one tuple per top-level array
//! element) from a json/jsonb array of objects, returning the whole result
//! through the materialize protocol: `InitMaterializedSRF` blesses the
//! `setDesc` from the query's expected record type, the worker deforms each
//! produced tuple back to `(values, nulls)` and `materialized_srf_putvalues`-es
//! it, and the fmgr entry point returns SQL NULL. The full bodies live in
//! [`adt_jsonfuncs::recordset`]; this unit only adapts the owned
//! `(mcx, fcinfo, ...)` worker signature to the executor-frame
//! [`::nodes::execexpr::PGFunction`] ABI and registers each under its
//! `pg_proc` OID, exactly as `fmgr_builtins[]` would add an ordinary row.
//!
//! The sibling `json[b]_populate_recordset` variants are NOT registered here:
//! they read an optional composite `record` argument through the funcapi
//! `srf_arg_record` seam, which is uninstalled (the project-wide fmgr
//! composite-argument detoast boundary that funcapi has not yet grown — see
//! `seams-init` `TD-JSONFUNCS-FMGR-ARG-DETOAST`). The `to_recordset` variants
//! never take a record argument (`have_record_arg = false`), so they never
//! reach that seam and work end-to-end today.
//!
//! Dispatched by `ExecMakeTableFunctionResult` through the executor-frame SRF
//! table (the frame whose `resultinfo` carries the live `ReturnSetInfo` the
//! worker reads/writes); the by-OID fmgr-core registry's tag-only `resultinfo`
//! cannot carry it.

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `json_to_recordset(json)` (OID 3205).
const JSON_TO_RECORDSET: Oid = 3205;
/// `jsonb_to_recordset(jsonb)` (OID 3491).
const JSONB_TO_RECORDSET: Oid = 3491;

/// Register the materialize-mode `json[b]_to_recordset` SRFs in the
/// executor-frame SRF table.
pub(crate) fn register_recordset_srfs() {
    register_srf(JSON_TO_RECORDSET, json_to_recordset);
    register_srf(JSONB_TO_RECORDSET, jsonb_to_recordset);
}

/// The per-query memory context the SRF caller threads onto the executor frame
/// (`fcinfo->fn_mcxt`).
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("recordset SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// Reborrow the executor's call frame at the `'mcx` lifetime the recordset
/// worker requires.
///
/// The `populate_recordset_worker` signature is `&'mcx mut
/// FunctionCallInfoBaseData<'mcx>` (it hands a `&'mcx` shared view of the frame
/// to the `get_call_result_type` resolution chain, which returns `&'mcx`
/// `fn_expr` nodes that live in the call arena). The executor-frame
/// [`PGFunction`] ABI delivers the frame as `&'a mut
/// FunctionCallInfoBaseData<'mcx>` with an anonymous `'a`.
///
/// SAFETY: `ExecMakeTableFunctionResult` allocates `setexpr.fcinfo` in the
/// per-query memory context (`es_query_cxt`, the `'mcx` arena) and dispatches
/// the SRF through it; the frame therefore lives for `'mcx`, so extending the
/// exclusive borrow from `'a` to `'mcx` does not outlive the referent. The
/// caller (`srf_invoke_by_oid`) holds the only `&mut` to the frame across this
/// call, so no aliasing `&mut` exists. This mirrors the same trimmed-call-frame
/// borrow idiom `populate_recordset_worker` itself uses internally.
unsafe fn frame_mcx<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> &'mcx mut FunctionCallInfoBaseData<'mcx> {
    &mut *(fcinfo as *mut FunctionCallInfoBaseData<'mcx>)
}

/// `json_to_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3995) over the executor
/// frame.
fn json_to_recordset<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `frame_mcx`.
    let frame = unsafe { frame_mcx(fcinfo) };
    adt_jsonfuncs::recordset::json_to_recordset(mcx, frame)
}

/// `jsonb_to_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3981) over the executor
/// frame.
fn jsonb_to_recordset<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `frame_mcx`.
    let frame = unsafe { frame_mcx(fcinfo) };
    adt_jsonfuncs::recordset::jsonb_to_recordset(mcx, frame)
}
