//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `jsonpath.c` I/O functions whose argument/result types are expressible at the
//! current fmgr boundary: the `jsonpath` type's text I/O pair `jsonpath_in`
//! (oid 4001) and `jsonpath_out` (oid 4003).
//!
//! `jsonpath` is a pass-by-reference varlena whose internal representation is the
//! flattened on-disk image built by [`crate::jsonpath_in`]. Like `json`/`text`
//! it crosses the fmgr boundary on the by-reference side channel: a by-reference
//! result is set via `fcinfo.set_ref_result(RefPayload::Varlena(..))` framed with
//! the 4-byte varlena header, and a by-reference arg arrives header-stripped
//! (`VARDATA_ANY`). The bare by-value word is the null/dummy word.
//!
//! `jsonpath_recv` / `jsonpath_send` are NOT registered here: their only
//! non-core work is the libpq binary framing (the version byte + remaining
//! text), the systemic wire-protocol deferral documented in `lib.rs`. The
//! grammar/scanner (`parsejsonpath`) is still seamed/unported, so a literal cast
//! `'$.a'::jsonpath` is only as live as that seam — but the fmgr dispatch entry
//! must exist for the `internal lookup table` resolution to succeed.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// `VARHDRSZ` — the uncompressed 4-byte varlena length-word size.
const VARHDRSZ: usize = 4;

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("jsonpath fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_JSONPATH_P(i)` payload bytes (`VARDATA_ANY`): under the header-ful
/// convention the lane carries the full `jsonpath` varlena image, so strip its
/// leading `VARHDRSZ` header to recover the payload the cores consume.
#[inline]
fn arg_jsonpath_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonpath fn: by-ref `jsonpath` arg missing from by-ref lane");
    &image[VARHDRSZ..]
}

/// Set a by-reference varlena (`_in`) result on the by-ref lane: the cores
/// return the bare payload, so frame it as a full 4-byte-header varlena image
/// (`SET_VARSIZE(image, VARHDRSZ + len)`, native-order `(total) << 2`).
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let total = VARHDRSZ + payload.len();
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    image.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
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
    mcx::MemoryContext::new("jsonpath fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
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
// I/O adapters (jsonpath.c).
// ---------------------------------------------------------------------------

/// `jsonpath_in(cstring) -> jsonpath` (oid 4001): parse text into the flattened
/// on-disk `jsonpath` image; cross back on the by-ref lane header-stripped.
///
/// The ERROR-context fmgr dispatch passes no `escontext`; a swallowed soft error
/// (the `None` return) would only arise on the `errsave` path that does not run
/// here (hard errors raise above), so `None` yields empty content bytes for
/// completeness.
fn fc_jsonpath_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::jsonpath_in(m.mcx(), s.as_bytes(), None))
        .map(|image| image.as_slice().to_vec())
        .unwrap_or_default();
    ret_varlena(fcinfo, bytes)
}

/// `jsonpath_out(jsonpath) -> cstring` (oid 4003): render the on-disk image to
/// text.
fn fc_jsonpath_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let jp = arg_jsonpath_payload(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::jsonpath_out(m.mcx(), jp));
    ret_cstring(fcinfo, String::from_utf8_lossy(bytes.as_slice()).into_owned())
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

/// `Gen_fmgrtab.pl` builds `fmgr_builtins[]` from `pg_proc.dat`; here each entry
/// is transcribed by hand. OIDs/nargs/strict/retset come straight from
/// `pg_proc.dat`.
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

/// Register the expressible `jsonpath.c` I/O builtins. Called from this crate's
/// `init_seams()`. OIDs/nargs/strict/retset transcribed from `pg_proc.dat`.
pub fn register_jsonpath_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(4001, "jsonpath_in", 1, true, false, fc_jsonpath_in),
        builtin(4003, "jsonpath_out", 1, true, false, fc_jsonpath_out),
    ]);
}
