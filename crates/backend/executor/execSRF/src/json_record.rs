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
//! [`adt_jsonfuncs::{populate,recordset}`]; this unit only adapts
//! the owned `(mcx, fcinfo)` worker signature to the executor-frame [`PGFunction`]
//! ABI (`fn(&mut FunctionCallInfoBaseData) -> Datum`) and registers each under its
//! `pg_proc` OID, exactly as `fmgr_builtins[]` would add an ordinary row.
//!
//! These are registered here and NOT in jsonfuncs' `register_jsonfuncs_builtins`
//! because the by-OID fmgr-core registry's `fmgr::PGFunction` frame carries
//! a tag-only `resultinfo` that cannot deliver the live `ReturnSetInfo` /
//! `expectedDesc` these record functions need — the WONTFIX dual-home.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

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
    adt_jsonfuncs::populate::json_to_record(mcx, fc)
}

/// `jsonb_to_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2488) over the executor frame.
fn jsonb_to_record<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    adt_jsonfuncs::populate::jsonb_to_record(mcx, fc)
}

/// `json_populate_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2495) over the executor
/// frame.
fn json_populate_record<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    adt_jsonfuncs::populate::json_populate_record(mcx, fc)
}

/// `jsonb_populate_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2471) over the executor
/// frame.
fn jsonb_populate_record<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    adt_jsonfuncs::populate::jsonb_populate_record(mcx, fc)
}

/// `jsonb_populate_record_valid(PG_FUNCTION_ARGS)` (jsonfuncs.c:2477) over the
/// executor frame.
fn jsonb_populate_record_valid<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    adt_jsonfuncs::populate::jsonb_populate_record_valid(mcx, fc)
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
    adt_jsonfuncs::recordset::json_populate_recordset(mcx, fc)
}

/// `jsonb_populate_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3974) over the
/// executor frame.
fn jsonb_populate_recordset<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // SAFETY: see `reborrow_mcx`.
    let fc = unsafe { reborrow_mcx(fcinfo) };
    adt_jsonfuncs::recordset::jsonb_populate_recordset(mcx, fc)
}

// ===========================================================================
//  Scalar-expression entry for the NON-SET record family.
//
//  `json[b]_populate_record`, `json[b]_to_record`, and `jsonb_populate_record_valid`
//  are `proretset => 'f'`: they are also callable as ordinary scalar expressions
//  (`SELECT json_populate_record(null::jpop, '{...}')`), where the result row
//  type is resolved from the call's polymorphic argument (`get_call_result_type`
//  off `flinfo->fn_expr`), NOT from a FROM-clause `AS (col type)` list. The
//  scalar `EEOP_FUNCEXPR` interpreter step routes these OIDs here instead of the
//  by-OID fmgr-core builtin table (whose tag-only `resultinfo` ABI frame cannot
//  carry the live record protocol — the WONTFIX dual-home documented above).
//
//  This builds the executor-frame [`FunctionCallInfoBaseData`] the workers
//  require (a real `FmgrInfo` carrying `fn_oid` + the call node's `fn_expr`, the
//  `fn_mcxt` per-call arena, the by-value/by-reference split argument frame) from
//  the interpreter's canonical `Datum` argument vector, then dispatches through
//  the same `srf_invoke_by_oid` table the FROM-clause path uses. The non-set
//  `*_record` workers return the single composite row (or SQL NULL) directly as
//  the `SrfDispatch::Builtin` datum.
// ===========================================================================

/// `true` iff `foid` is one of the non-set json/jsonb record functions this
/// module serves as a scalar expression (the interpreter consults this to route
/// the call here rather than through the fmgr-core builtin table).
pub fn is_scalar_record_function(foid: Oid) -> bool {
    matches!(
        foid,
        JSON_TO_RECORD
            | JSONB_TO_RECORD
            | JSON_POPULATE_RECORD
            | JSONB_POPULATE_RECORD
            | JSONB_POPULATE_RECORD_VALID
    )
}

/// Convert one interpreter-canonical argument into the executor-frame call
/// frame's `(NullableDatum, ref_args[i])` split (mirrors fmgr-core's
/// `datum_to_ref_arg`): a by-value word stays in `args[i].value`; a by-reference
/// varlena/composite/cstring rides the `ref_args[i]` side channel as the
/// header-ful image the `srf_arg_*` readers consume.
fn canon_arg_to_frame<'mcx>(
    val: &Datum<'mcx>,
    isnull: bool,
) -> (datum::NullableDatum, Option<::nodes::fmgr::FmgrArgRef>) {
    use datum::NullableDatum;
    use ::nodes::fmgr::FmgrArgRef;
    if isnull {
        return (NullableDatum::null(), None);
    }
    match val {
        Datum::ByVal(w) => (
            NullableDatum::value(datum::Datum::from_usize(*w)),
            None,
        ),
        Datum::ByRef(bytes) => (
            NullableDatum::value(datum::Datum::from_usize(0)),
            Some(FmgrArgRef::Varlena(bytes.as_slice().to_vec())),
        ),
        // A composite Datum is varlena-shaped: its header-ful disk image is what
        // `srf_arg_record` reads (`FmgrArgRef::Varlena`), the inverse of
        // `from_datum_image`.
        Datum::Composite(t) => (
            NullableDatum::value(datum::Datum::from_usize(0)),
            Some(FmgrArgRef::Varlena(t.to_datum_image())),
        ),
        Datum::Cstring(s) => (
            NullableDatum::value(datum::Datum::from_usize(0)),
            Some(FmgrArgRef::Cstring(s.clone())),
        ),
        Datum::Expanded(_) | Datum::Internal(_) => (
            NullableDatum::value(datum::Datum::from_usize(0)),
            None,
        ),
    }
}

/// Dispatch a non-set json/jsonb record function as a scalar expression.
///
/// `args`/`nulls` are the interpreter's gathered canonical argument vector;
/// `fn_expr` is the call node `ExecInitFunc` stamped onto the step's `FmgrInfo`
/// (the polymorphic result-type source). Returns `(result_datum, isnull)` —
/// the single composite row or SQL NULL.
pub fn invoke_scalar_record_function<'mcx>(
    mcx: Mcx<'mcx>,
    foid: Oid,
    collation: Oid,
    args: &[Datum<'mcx>],
    nulls: &[bool],
    fn_expr: Option<types_core::fmgr::FnExprErased>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let mut flinfo = types_core::fmgr::FmgrInfo::empty();
    flinfo.fn_oid = foid;
    flinfo.fn_expr = fn_expr;

    let mut fcinfo: FunctionCallInfoBaseData<'mcx> = FunctionCallInfoBaseData {
        flinfo: Some(flinfo),
        context: None,
        resultinfo: None,
        fncollation: collation,
        isnull: false,
        nargs: args.len() as i16,
        args: alloc::vec::Vec::with_capacity(args.len()),
        ref_args: alloc::vec::Vec::with_capacity(args.len()),
        // The per-call arena the workers charge the composite result / cache to
        // (C: `fcinfo->flinfo->fn_mcxt`); for a scalar call this is the caller's
        // per-query context.
        fn_mcxt: Some(mcx),
        ..Default::default()
    };
    for (i, val) in args.iter().enumerate() {
        let (nd, refp) = canon_arg_to_frame(val, nulls.get(i).copied().unwrap_or(false));
        fcinfo.args.push(nd);
        fcinfo.ref_args.push(refp);
    }

    let dispatch = crate::srf_invoke_by_oid(foid, &mut fcinfo)?;
    let isnull = fcinfo.isnull;
    match dispatch {
        crate::srf_registry::SrfDispatch::Builtin(d) => Ok((d, isnull)),
        crate::srf_registry::SrfDispatch::Materialized(_) => Err(types_error::PgError::error(
            "invoke_scalar_record_function: non-set record function unexpectedly materialized",
        )),
    }
}
