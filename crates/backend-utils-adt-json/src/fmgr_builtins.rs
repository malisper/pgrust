//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `json.c` functions whose argument/result types are expressible at the
//! current fmgr boundary: the `json` type's I/O quartet (`json_in`/`json_out`/
//! `json_recv`/`json_send`) and `json_typeof`.
//!
//! `json` is a pass-by-reference varlena whose internal representation is the
//! same as `text` (the validated UTF-8 bytes verbatim). Its values cross the
//! fmgr boundary on the by-reference side channel exactly like `text`: an arg
//! arrives header-stripped (`VARDATA_ANY`, via `fcinfo.ref_arg(i)
//! .as_varlena()`), and a by-reference result is the payload bytes set via
//! `fcinfo.set_ref_result(RefPayload::Varlena(..))`. The bare by-value word is
//! the null/dummy word.
//!
//! Each entry is a `fc_<name>` adapter that reads its args off the fmgr call
//! frame, calls the matching value core, and writes the result. OIDs / nargs /
//! strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! The K5-gated variadic-`"any"` constructors (`json_build_object` /
//! `json_build_array` / `json_object`) and the `to_json` / `row_to_json` /
//! `array_to_json` family (which need the arg's resolved type Oid plus
//! arbitrary-type output dispatch) are NOT registered here; the SRF / aggregate
//! entries are likewise deferred.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` payload bytes (`VARDATA_ANY`): the lane carries
/// `json`/`text` args header-stripped.
#[inline]
fn arg_text_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("json fn: by-ref `json`/`text` arg missing from by-ref lane")
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("json fn: cstring arg missing from by-ref lane")
}

/// Set a `json`/`text`/`bytea` (by-reference) result on the by-ref lane and
/// return the dummy by-value word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// bytes are copied onto the by-ref lane before it drops.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("json fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// I/O adapters (json.c).
// ---------------------------------------------------------------------------

/// `json_in(cstring) -> json` (oid 321). The validated text bytes become the
/// `json` value's content; cross back on the by-ref lane header-stripped.
fn fc_json_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let m = scratch_mcx();
    // A swallowed soft error would yield None via an errsave path that does not
    // run here (hard errors raise above), so the Null arm is unreachable in the
    // ERROR-context dispatch; produce empty content bytes for completeness.
    let bytes = ok(crate::json_in(m.mcx(), s.as_bytes()))
        .map(|image| image.as_slice().to_vec())
        .unwrap_or_default();
    ret_varlena(fcinfo, bytes)
}

/// `json_out(json) -> cstring` (oid 322): a `json` value is its own text.
fn fc_json_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let json = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::json_out(m.mcx(), json));
    ret_cstring(fcinfo, String::from_utf8_lossy(bytes.as_slice()).into_owned())
}

/// `json_recv(internal) -> json` (oid 323): read + validate the message bytes.
fn fc_json_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let image = ok(crate::json_recv(m.mcx(), buf));
    ret_varlena(fcinfo, image.as_slice().to_vec())
}

/// `json_send(json) -> bytea` (oid 324): the text bytes framed by the wire
/// layer; the body is the value's content bytes.
fn fc_json_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let json = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::json_send(m.mcx(), json));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// `json_typeof(json) -> text` (oid 3968).
fn fc_json_typeof(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let json = arg_text_payload(fcinfo, 0);
    let typ = ok(crate::json_typeof(json));
    ret_varlena(fcinfo, typ.as_bytes().to_vec())
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the expressible scalar `json.c` builtins. Called from this crate's
/// `init_seams()`. OIDs/nargs/strict/retset transcribed from `pg_proc.dat`.
pub fn register_json_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(321, "json_in", 1, true, false, fc_json_in),
        builtin(322, "json_out", 1, true, false, fc_json_out),
        builtin(323, "json_recv", 1, true, false, fc_json_recv),
        builtin(324, "json_send", 1, true, false, fc_json_send),
        builtin(3968, "json_typeof", 1, true, false, fc_json_typeof),
    ]);
}
