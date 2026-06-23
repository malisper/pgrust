//! `json_array_elements(json)` (OID 3955) and `json_object_keys(json)` (OID 3957)
//! registered as executor-frame set-returning functions.
//!
//! `jsonfuncs.c`'s `json_array_elements` / `json_object_keys` are value-per-call
//! SRFs (C's `SRF_FIRSTCALL_INIT` + `SRF_RETURN_NEXT`): the first call parses the
//! whole json document, collecting the array-element rows (resp. the object-key
//! rows), then each call emits one row. The SAX-callback collection core (the
//! `elements_worker` / `json_object_keys_worker` parse walks) is ported in
//! `backend-utils-adt-jsonfuncs::{elements,keys}`.
//!
//! Here those cores are assembled into [`::nodes::execexpr::PGFunction`]s (the
//! executor-frame ABI whose call frame carries the LIVE `ReturnSetInfo`) and
//! registered in this unit's executor-frame SRF table from [`register_json_srfs`]
//! (called by `init_seams`) — the executor-frame `fmgrtab.c` analogue for these
//! SRFs, exactly as `generate_series`/`unnest` are. It bypasses the by-OID builtin
//! registry (whose `fmgr::PGFunction` frame's `resultinfo` is tag-only — the
//! WONTFIX dual-home) so the function reads/writes a live `ReturnSetInfo`. `SELECT
//! json_array_elements('[1,2,3]')` reaches this via nodeProjectSet →
//! ExecMakeFunctionResultSet; `SELECT * FROM json_array_elements(...)` via
//! nodeFunctionscan → ExecMakeTableFunctionResult.
//!
//! The owned model parses the document once on the first call (preserving C's
//! per-row ordering and NULL flags), then emits one `json`/`text` row per call.

use core::any::Any;

use mcx::{Mcx, PgBox};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::execexpr::ExprDoneCond;
use ::nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `json_array_elements(json)` (OID 3955).
const JSON_ARRAY_ELEMENTS: Oid = 3955;
/// `json_array_elements_text(json)` (OID 3969).
const JSON_ARRAY_ELEMENTS_TEXT: Oid = 3969;
/// `json_object_keys(json)` (OID 3957).
const JSON_OBJECT_KEYS: Oid = 3957;

/// Register the json (text) array-elements / object-keys SRFs in the
/// executor-frame SRF table.
pub(crate) fn register_json_srfs() {
    register_srf(JSON_ARRAY_ELEMENTS, json_array_elements);
    register_srf(JSON_ARRAY_ELEMENTS_TEXT, json_array_elements_text);
    register_srf(JSON_OBJECT_KEYS, json_object_keys);
}

/// One emitted row: `None` is a SQL NULL (json `null` in text mode); `Some(bytes)`
/// is the raw payload (a json-text fragment, or a de-escaped `text` value).
type Row = Option<Vec<u8>>;

/// The materialized cross-call state for a json (text) SRF (C: the per-call
/// `SRF_PERCALL_SETUP` walk over `funcctx->user_fctx`). The whole document is
/// parsed once on the first call; rows are emitted one per call.
struct JsonSrfFctx {
    /// The collected rows in document order.
    rows: Vec<Row>,
    /// The next row index to emit (C: `funcctx->call_cntr`).
    next: usize,
    /// Whether each row's payload is a `text` value (`json_array_elements_text`)
    /// versus a `json` fragment (`json_array_elements`, `json_object_keys`). Both
    /// build a text-like varlena, so this is informational parity only.
    _as_text: bool,
}

/// Erase a `'static` cross-call state value into the `FuncCallContext.user_fctx`
/// carrier (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = ::mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// Read a by-reference `json`/`text` argument `index` as its VARDATA payload
/// bytes (C: `PG_GETARG_TEXT_PP` → `VARDATA_ANY`). The executor frame carries the
/// header-ful varlena image on the by-ref side channel; skip the 4-byte length
/// word to reach the payload.
fn arg_json_payload(fcinfo: &FunctionCallInfoBaseData<'_>, index: usize) -> Vec<u8> {
    let image = match fcinfo.ref_arg(index) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
        _ => panic!("json SRF: json argument {index} missing from by-ref lane"),
    };
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. A small stored json/text reaches an fmgr arg
    // verbatim once `SHORT_VARLENA_PACKING` is on; a fixed 4-byte strip would drop
    // three payload bytes. No-op while the flag is off (every value is 4-byte).
    let payload: &[u8] = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    };
    payload.to_vec()
}

/// `json_array_elements(PG_FUNCTION_ARGS)` (jsonfuncs.c:2295) over the executor
/// frame.
fn json_array_elements<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    json_array_elements_impl(fcinfo, "json_array_elements", false)
}

/// `json_array_elements_text(PG_FUNCTION_ARGS)` (jsonfuncs.c:2301) over the
/// executor frame.
fn json_array_elements_text<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    json_array_elements_impl(fcinfo, "json_array_elements_text", true)
}

/// Shared value-per-call driver for the two json (text) array-elements entry
/// points. `SRF_RETURN_NEXT` / `SRF_RETURN_DONE` are the `isDone` writes + the
/// multi-call teardown.
fn json_array_elements_impl<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    funcname: &str,
    as_text: bool,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("json_array_elements: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { ... elements_worker(json, ...) ... }
    if fcinfo.fn_extra.is_none() {
        let rows: Vec<Row> = {
            let json = arg_json_payload(fcinfo, 0);
            adt_jsonfuncs::elements::elements_worker(&json, funcname, as_text)?
        };
        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(
            mcx,
            JsonSrfFctx {
                rows,
                next: 0,
                _as_text: as_text,
            },
        );
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    emit_next(fcinfo, mcx)
}

/// `json_object_keys(PG_FUNCTION_ARGS)` (jsonfuncs.c:601) over the executor frame.
fn json_object_keys<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("json_object_keys: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { ... json_object_keys_worker(json) ... }
    if fcinfo.fn_extra.is_none() {
        let rows: Vec<Row> = {
            let json = arg_json_payload(fcinfo, 0);
            adt_jsonfuncs::keys::json_object_keys_worker(&json)?
                .into_iter()
                .map(Some)
                .collect()
        };
        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(
            mcx,
            JsonSrfFctx {
                rows,
                next: 0,
                _as_text: true,
            },
        );
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    emit_next(fcinfo, mcx)
}

/// The shared `SRF_PERCALL_SETUP` + `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` tail: emit
/// the next collected row as a text-like varlena Datum, or finish the set.
fn emit_next<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut JsonSrfFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<JsonSrfFctx>()
        .expect("user_fctx is JsonSrfFctx");

    if state.next < state.rows.len() {
        let (value, isnull): (Datum<'mcx>, bool) = match &state.rows[state.next] {
            None => (Datum::null(), true),
            Some(bytes) => (
                varlena_seams::bytes_to_varlena_v::call(mcx, bytes)?,
                false,
            ),
        };
        state.next += 1;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = isnull;
        Ok(value)
    } else {
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Ok(Datum::null())
    }
}

/// `rsi->isDone = cond` (the `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` write onto the
/// live `ReturnSetInfo` the executor frame carries).
fn set_isdone(fcinfo: &mut FunctionCallInfoBaseData<'_>, cond: ExprDoneCond) {
    fcinfo
        .resultinfo
        .as_mut()
        .expect("resultinfo present for an SRF call")
        .isDone = cond;
}
