//! Executor-frame registration of the materialize-mode `json[b]_each[_text]`
//! SRFs (jsonfuncs.c:1949-1971 `each_worker` / `each_worker_jsonb`).
//!
//! Unlike `json_array_elements` / `json_object_keys` (the value-per-call SRFs in
//! [`crate::json_srf`]), the `each` family returns its whole result through the
//! materialize protocol: `InitMaterializedSRF(fcinfo, MAT_SRF_BLESS)` builds the
//! `(key text, value json[b]/text)` tuplestore on `rsinfo->setResult`, the
//! worker `materialized_srf_putvalues`-es one row per object field, and the
//! fmgr entry point returns SQL NULL. The full bodies live in
//! [`adt_jsonfuncs::each`]; this unit only adapts the owned
//! `(mcx, fcinfo, ...)` worker signature to the executor-frame [`PGFunction`]
//! ABI (`fn(&mut FunctionCallInfoBaseData) -> Datum`) and registers each under
//! its `pg_proc` OID, exactly as `fmgr_builtins[]` would add an ordinary row.
//!
//! These are dispatched by `ExecMakeTableFunctionResult` through the
//! executor-frame SRF table (the frame whose `resultinfo` carries the live
//! `ReturnSetInfo` the worker reads/writes); the by-OID fmgr-core registry's
//! tag-only `resultinfo` cannot carry it, which is why these are registered
//! here and NOT in `register_jsonfuncs_builtins` (jsonfuncs fmgr_builtins.rs).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `json_each(json)` (OID 3958).
const JSON_EACH: Oid = 3958;
/// `json_each_text(json)` (OID 3959).
const JSON_EACH_TEXT: Oid = 3959;
/// `jsonb_each(jsonb)` (OID 3208).
const JSONB_EACH: Oid = 3208;
/// `jsonb_each_text(jsonb)` (OID 3932).
const JSONB_EACH_TEXT: Oid = 3932;

/// Register the materialize-mode `json[b]_each[_text]` SRFs in the
/// executor-frame SRF table.
pub(crate) fn register_json_each_srfs() {
    register_srf(JSON_EACH, json_each);
    register_srf(JSON_EACH_TEXT, json_each_text);
    register_srf(JSONB_EACH, jsonb_each);
    register_srf(JSONB_EACH_TEXT, jsonb_each_text);
}

/// The per-query memory context the SRF caller threads onto the executor frame
/// (`fcinfo->fn_mcxt`, the `multi_call_memory_ctx`/`per_query` arena the
/// materialize tuplestore + descriptor are allocated in).
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("json_each SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// `json_each(PG_FUNCTION_ARGS)` (jsonfuncs.c:1950) over the executor frame.
fn json_each<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    adt_jsonfuncs::each::json_each(mcx, fcinfo)
}

/// `json_each_text(PG_FUNCTION_ARGS)` (jsonfuncs.c:1962) over the executor frame.
fn json_each_text<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    adt_jsonfuncs::each::json_each_text(mcx, fcinfo)
}

/// `jsonb_each(PG_FUNCTION_ARGS)` (jsonfuncs.c:1956) over the executor frame.
fn jsonb_each<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    adt_jsonfuncs::each::jsonb_each(mcx, fcinfo)
}

/// `jsonb_each_text(PG_FUNCTION_ARGS)` (jsonfuncs.c:1968) over the executor frame.
fn jsonb_each_text<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    adt_jsonfuncs::each::jsonb_each_text(mcx, fcinfo)
}
