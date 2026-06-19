//! Executor-frame registration of the materialize-mode `jsonb_array_elements`,
//! `jsonb_array_elements_text`, and `jsonb_object_keys` SRFs (jsonfuncs.c:2207
//! `elements_worker_jsonb` / jsonfuncs.c:568 `jsonb_object_keys`).
//!
//! Unlike the json (text) `array_elements` / `object_keys` SRFs (the
//! value-per-call SRFs in [`crate::json_srf`]), the jsonb variants return their
//! whole result through the materialize protocol: `InitMaterializedSRF` builds
//! the single-column (`jsonb` / `text`) tuplestore on `rsinfo->setResult`, the
//! worker `materialized_srf_putvalues`-es one row per element / key, and the
//! fmgr entry point returns SQL NULL. The full bodies live in
//! [`backend_utils_adt_jsonfuncs::{elements,keys}`]; this unit only adapts the
//! owned `(mcx, fcinfo, ...)` worker signature to the executor-frame
//! [`types_nodes::execexpr::PGFunction`] ABI (`fn(&mut FunctionCallInfoBaseData)
//! -> Datum`) and registers each under its `pg_proc` OID, exactly as
//! `fmgr_builtins[]` would add an ordinary row.
//!
//! These are dispatched by `ExecMakeTableFunctionResult` through the
//! executor-frame SRF table (the frame whose `resultinfo` carries the live
//! `ReturnSetInfo` the worker reads/writes); the by-OID fmgr-core registry's
//! tag-only `resultinfo` cannot carry it, which is why these are registered
//! here and NOT in `register_jsonfuncs_builtins` (jsonfuncs fmgr_builtins.rs).

use mcx::Mcx;
use types_core::Oid;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `jsonb_array_elements(jsonb)` (OID 3219).
const JSONB_ARRAY_ELEMENTS: Oid = 3219;
/// `jsonb_array_elements_text(jsonb)` (OID 3465).
const JSONB_ARRAY_ELEMENTS_TEXT: Oid = 3465;
/// `jsonb_object_keys(jsonb)` (OID 3931).
const JSONB_OBJECT_KEYS: Oid = 3931;

/// Register the materialize-mode `jsonb_array_elements[_text]` /
/// `jsonb_object_keys` SRFs in the executor-frame SRF table.
pub(crate) fn register_jsonb_srfs() {
    register_srf(JSONB_ARRAY_ELEMENTS, jsonb_array_elements);
    register_srf(JSONB_ARRAY_ELEMENTS_TEXT, jsonb_array_elements_text);
    register_srf(JSONB_OBJECT_KEYS, jsonb_object_keys);
}

/// The per-query memory context the SRF caller threads onto the executor frame
/// (`fcinfo->fn_mcxt`, the materialize tuplestore + descriptor arena).
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("jsonb SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// `jsonb_array_elements(PG_FUNCTION_ARGS)` (jsonfuncs.c:2207) over the executor
/// frame.
fn jsonb_array_elements<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx = srf_mcx(fcinfo);
    backend_utils_adt_jsonfuncs::elements::jsonb_array_elements(mcx, fcinfo)
        .unwrap_or_else(|e| std::panic::panic_any(e))
}

/// `jsonb_array_elements_text(PG_FUNCTION_ARGS)` (jsonfuncs.c:2213) over the
/// executor frame.
fn jsonb_array_elements_text<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx = srf_mcx(fcinfo);
    backend_utils_adt_jsonfuncs::elements::jsonb_array_elements_text(mcx, fcinfo)
        .unwrap_or_else(|e| std::panic::panic_any(e))
}

/// `jsonb_object_keys(PG_FUNCTION_ARGS)` (jsonfuncs.c:568) over the executor
/// frame.
fn jsonb_object_keys<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx = srf_mcx(fcinfo);
    backend_utils_adt_jsonfuncs::keys::jsonb_object_keys(mcx, fcinfo)
        .unwrap_or_else(|e| std::panic::panic_any(e))
}
