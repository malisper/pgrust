//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! entry points defined in `quote.c`.
//!
//! Each entry is a `fc_<name>` adapter that reads its `text` argument off the
//! fmgr call frame's by-ref lane (the boundary delivers `text` header-stripped,
//! i.e. the raw content bytes, symmetric with how the value cores here
//! consume/produce header-less payloads), calls the matching value core, and
//! writes the `text` result back onto the by-ref lane.
//! [`register_quote_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs / nargs
//! / strict / retset are transcribed exactly from `pg_proc.dat`:
//!
//! * `1282 quote_ident`    — nargs 1, strict, retset f
//! * `1283 quote_literal`  — nargs 1, strict, retset f
//! * `1289 quote_nullable` — nargs 1, **not strict** (`proisstrict => 'f'`), retset f
//!
//! `quote_ident` delegates to `quote_identifier` (`ruleutils.c`), owned by the
//! ruleutils unit and reached across its seam; that seam panics loudly until
//! ruleutils lands. This mirrors the C call path exactly — the value core
//! exists and its `text`/`text` boundary is fully expressible here — so it is
//! registered faithfully (it will raise the ruleutils seam panic at call time
//! until that unit lands, which is the same deferral every cross-unit seam
//! carries).

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// `VARDATA_ANY` of an inline (non-compressed, non-external) varlena image: skip
/// ONE header byte for a short (1-byte) header, else `VARHDRSZ`. A small stored
/// value arrives short-headed once `SHORT_VARLENA_PACKING` is on; a fixed
/// `VARHDRSZ` strip would drop three payload bytes. No-op while packing is off.
#[inline]
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// A `text` arg's `VARDATA_ANY` content. Under the header-ful-everywhere
/// convention the by-ref lane carries the full varlena image (4-byte length
/// word + payload); this skips the header.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    varlena_payload(
        fcinfo
            .ref_arg(i)
            .and_then(|p| p.as_varlena())
            .expect("quote fn: text arg missing from by-ref lane"),
    )
}

/// A possibly-NULL `text` arg: `None` models `PG_ARGISNULL(i)` (the by-ref lane
/// carries no payload for an SQL NULL). Used by the non-strict `quote_nullable`.
#[inline]
fn arg_text_opt<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> Option<&'a [u8]> {
    fcinfo.ref_arg(i).and_then(|p| p.as_varlena()).map(varlena_payload)
}

/// Set a `text` result on the by-ref lane. Under the header-ful-everywhere
/// convention this stamps the 4-byte uncompressed varlena length word in front
/// of the payload (`SET_VARSIZE`), symmetric with how `arg_text` reads args
/// back. Returns the dummy by-value word.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    let mut image = Vec::with_capacity(bytes.len() + VARHDRSZ);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(bytes.len() + VARHDRSZ));
    image.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd `text` result lives in the caller's context; here it crosses by
/// value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("quote fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `Datum quote_ident(PG_FUNCTION_ARGS)` — `quote.c`.
fn fc_quote_ident(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let t = arg_text(fcinfo, 0);
    // C: text_to_cstring(txt) → a NUL-terminated cstring; the input is valid
    // UTF-8 text content. quote_identifier takes &str.
    let s = match core::str::from_utf8(t) {
        Ok(s) => s,
        Err(_) => return Err(::types_error::PgError::error("invalid byte sequence for encoding")),
    };
    let bytes = crate::quote_ident(m.mcx(), s)?.as_slice().to_vec();
    Ok(ret_text(fcinfo, bytes))
}

/// `Datum quote_literal(PG_FUNCTION_ARGS)` — `quote.c`.
fn fc_quote_literal(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let t = arg_text(fcinfo, 0);
    let bytes = crate::quote_literal(m.mcx(), t)?.as_slice().to_vec();
    Ok(ret_text(fcinfo, bytes))
}

/// `Datum quote_nullable(PG_FUNCTION_ARGS)` — `quote.c` (non-strict: handles
/// SQL NULL by returning the text `'NULL'`).
fn fc_quote_nullable(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let arg = arg_text_opt(fcinfo, 0);
    let bytes = crate::quote_nullable(m.mcx(), arg)?.as_slice().to_vec();
    Ok(ret_text(fcinfo, bytes))
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
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: String::from(name),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register every SQL-callable `quote.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs / nargs / strict /
/// retset transcribed exactly from `pg_proc.dat`.
pub fn register_quote_builtins() {
    fmgr_core::register_builtins_native([
        builtin(1282, "quote_ident", 1, true, false, fc_quote_ident),
        builtin(1283, "quote_literal", 1, true, false, fc_quote_literal),
        // quote_nullable is proisstrict => 'f'.
        builtin(1289, "quote_nullable", 1, false, false, fc_quote_nullable),
    ]);
}
