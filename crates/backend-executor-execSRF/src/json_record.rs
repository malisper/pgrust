//! Executor-frame registration of the json/jsonb composite-record SRFs
//! (jsonfuncs.c `populate_record_worker` / `populate_recordset_worker`) that the
//! sibling `recordset_srf` module does NOT already own:
//!
//!   * `json_to_record` (OID 3204)         / `jsonb_to_record` (OID 3490)
//!   * `json_populate_record` (OID 3960)   / `jsonb_populate_record` (OID 3209)
//!   * `json_populate_recordset` (OID 3961)/ `jsonb_populate_recordset` (OID 3475)
//!   * `jsonb_populate_record_valid` (OID 6338)
//!
//! (`json_to_recordset` (3205) / `jsonb_to_recordset` (3491) — the no-seed-record
//! materialize-set pair — are registered by `recordset_srf`; the seed-record
//! `populate_recordset` siblings here read the composite arg through the
//! now-installed funcapi `srf_arg_record` seam, which `recordset_srf` could not.)
//!
//! Two return shapes, both reached through `nodeFunctionscan.c` →
//! [`crate::ExecMakeTableFunctionResult`] → the executor-frame SRF table (the
//! frame whose `resultinfo` carries the live `ReturnSetInfo` the workers read for
//! the `AS (col type, ...)` column-definition-list `expectedDesc` and write for
//! the materialize tuplestore):
//!
//!   * the `*_record` family (`json_to_record`/`json_populate_record` and the
//!     jsonb/`_valid` siblings) returns exactly one composite row — the worker
//!     hands back the `HeapTupleGetDatum(...)` (a `Datum::Composite`) or SQL NULL;
//!     the value-per-call loop stores the single row with `isDone` left at
//!     `ExprSingleResult`, exactly as `pg_input_error_info` does.
//!   * the `*_recordset` family returns its whole result through the materialize
//!     protocol: the worker runs `InitMaterializedSRF` + `materialized_srf_putvalues`
//!     onto `rsinfo->setResult`/`setDesc` and returns SQL NULL.
//!
//! The full bodies (the coldeflist/`expectedDesc` → `TupleDesc` resolution via
//! `get_call_result_type`/`internal_get_result_type`, the optional seed-record
//! argument read via `srf_arg_record`, the json SAX walk, the per-column
//! `populate_record_field` coercions) live in
//! [`backend_utils_adt_jsonfuncs::{populate,recordset}`]; this unit only adapts
//! the owned `(mcx, fcinfo)` worker signature to the executor-frame [`PGFunction`]
//! ABI (`fn(&mut FunctionCallInfoBaseData) -> Datum`) and registers each under its
//! `pg_proc` OID, exactly as `fmgr_builtins[]` would add an ordinary row.
//!
//! These are registered here and NOT in jsonfuncs' `register_jsonfuncs_builtins`
//! because the by-OID fmgr-core registry's `types_fmgr::PGFunction` frame carries
//! a tag-only `resultinfo` that cannot deliver the live `ReturnSetInfo` /
//! `expectedDesc` these record functions need — the WONTFIX dual-home.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `json_to_record(json)` (OID 3204).
const JSON_TO_RECORD: Oid = 3204;
/// `jsonb_to_record(jsonb)` (OID 3490).
const JSONB_TO_RECORD: Oid = 3490;
/// `json_populate_record(anyelement, json, bool)` (OID 3960).
const JSON_POPULATE_RECORD: Oid = 3960;
/// `json_populate_recordset(anyelement, json, bool)` (OID 3961).
const JSON_POPULATE_RECORDSET: Oid = 3961;
/// `jsonb_populate_record(anyelement, jsonb)` (OID 3209).
const JSONB_POPULATE_RECORD: Oid = 3209;
/// `jsonb_populate_recordset(anyelement, jsonb)` (OID 3475).
const JSONB_POPULATE_RECORDSET: Oid = 3475;
/// `jsonb_populate_record_valid(anyelement, jsonb)` (OID 6338).
const JSONB_POPULATE_RECORD_VALID: Oid = 6338;

/// Register the json/jsonb composite-record SRFs in the executor-frame SRF table.
pub(crate) fn register_json_record_srfs() {
    register_srf(JSON_TO_RECORD, json_to_record);
    register_srf(JSONB_TO_RECORD, jsonb_to_record);
    register_srf(JSON_POPULATE_RECORD, json_populate_record);
    register_srf(JSON_POPULATE_RECORDSET, json_populate_recordset);
    register_srf(JSONB_POPULATE_RECORD, jsonb_populate_record);
    register_srf(JSONB_POPULATE_RECORDSET, jsonb_populate_recordset);
    register_srf(JSONB_POPULATE_RECORD_VALID, jsonb_populate_record_valid);
}

/// The per-query memory context the SRF caller threads onto the executor frame
/// (`fcinfo->fn_mcxt`) — the arena the resolved descriptor, the materialize
/// tuplestore, the seed-record `FormedTuple`, and the formed result tuple are
/// allocated in.
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("json record SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// Reborrow the executor frame as `&'mcx mut` for the jsonfuncs worker.
///
/// The jsonfuncs record workers take `fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>`
/// because their first step (`get_record_type_from_query` →
/// `get_call_result_type` → `fn_oid_and_expr`) hands back a `&'mcx` `fn_expr`
/// node that lives in the call arena, so the worker holds a `'mcx`-scoped view of
/// the frame. The executor-frame [`PGFunction`] ABI hands the dispatcher only a
/// shorter `&mut FunctionCallInfoBaseData<'mcx>` borrow; this extends it to the
/// `'mcx` the worker requires.
///
/// SAFETY: the dispatcher (`srf_invoke_by_oid`) owns the frame for the whole
/// `'mcx` call (it lives in `SetExprState.fcinfo`, kept alive across the row
/// series), so the frame genuinely outlives `'mcx`; the reborrow only widens the
/// borrow's region to match, and the worker is the sole accessor for its
/// duration. This is the same trimmed-call-frame boundary the workers themselves
/// already resolve internally (e.g. `recordset.rs`'s
/// `unsafe { &*(fcinfo as *const _) }`).
#[allow(clippy::needless_lifetimes)]
unsafe fn reborrow_mcx<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> &'mcx mut FunctionCallInfoBaseData<'mcx> {
    &mut *(fcinfo as *mut FunctionCallInfoBaseData<'mcx>)
}

// ===========================================================================
//  json_to_record family (single composite row)
// ===========================================================================

/// `json_to_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2502) over the executor frame.
fn json_to_record<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    backend_utils_adt_jsonfuncs::populate::json_to_record(mcx, fc)
}

/// `jsonb_to_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2488) over the executor frame.
fn jsonb_to_record<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    backend_utils_adt_jsonfuncs::populate::jsonb_to_record(mcx, fc)
}

/// `json_populate_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2495) over the executor
/// frame.
fn json_populate_record<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    backend_utils_adt_jsonfuncs::populate::json_populate_record(mcx, fc)
}

/// `jsonb_populate_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2471) over the executor
/// frame.
fn jsonb_populate_record<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    backend_utils_adt_jsonfuncs::populate::jsonb_populate_record(mcx, fc)
}

/// `jsonb_populate_record_valid(PG_FUNCTION_ARGS)` (jsonfuncs.c:2477) over the
/// executor frame.
fn jsonb_populate_record_valid<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    backend_utils_adt_jsonfuncs::populate::jsonb_populate_record_valid(mcx, fc)
}

// ===========================================================================
//  json_populate_recordset family (materialize-mode set, with seed record arg)
// ===========================================================================

/// `json_populate_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3988) over the
/// executor frame.
fn json_populate_recordset<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    backend_utils_adt_jsonfuncs::recordset::json_populate_recordset(mcx, fc)
}

/// `jsonb_populate_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3974) over the
/// executor frame.
fn jsonb_populate_recordset<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    backend_utils_adt_jsonfuncs::recordset::jsonb_populate_recordset(mcx, fc)
}
