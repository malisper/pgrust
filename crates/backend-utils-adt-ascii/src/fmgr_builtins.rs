//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `to_ascii` family from `ascii.c`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core ([`crate::to_ascii_default`] /
//! [`crate::to_ascii_enc`] / [`crate::to_ascii_encname`]), and writes back the
//! result. The `text` arg arrives on the by-ref lane as its detoasted
//! `VARDATA_ANY` content bytes (the boundary strips the varlena header) and is
//! passed straight through as a borrowed [`FmgrArg::Ref`]; the cores re-wrap the
//! transliterated content as an owned `text` payload ([`FmgrOut::Ref`] /
//! [`RefPayload::Varlena`]). The `int4` encoding arg is read off the by-val word
//! (`PG_GETARG_INT32`); the `name` encoding arg arrives on the by-ref lane as
//! its fixed `NAMEDATALEN` buffer, which is NUL-trimmed to the C string
//! `pg_char_to_encoding` resolves.
//!
//! [`register_ascii_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch (and the `fmgr_isbuiltin`
//! fast path) resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`: `proisstrict` default `'t'`, `proretset` default
//! `'f'`; `nargs` is the `proargtypes` count (1 for `to_ascii_default`, 2 for
//! the two-arg forms).

use types_datum::Datum;
use types_fmgr::boundary::{FmgrArg, FmgrOut, RefPayload};
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(0)` → `VARDATA_ANY`: borrow arg `i`'s by-ref `text`
/// content as an [`FmgrArg::Ref`] for the value cores.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> FmgrArg<'a, 'a> {
    let payload = fcinfo
        .ref_arg(i)
        .expect("to_ascii: text arg missing from by-ref lane");
    FmgrArg::Ref(payload)
}

/// `PG_GETARG_INT32(i)`: the encoding code on the by-val word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("to_ascii: missing arg").value.as_i32()
}

/// `PG_GETARG_NAME(i)` → `NameStr`: the `name` arg on the by-ref lane, as the
/// C string the encoding-name resolver expects. A `name` is a fixed
/// `NAMEDATALEN` buffer, so the buffer is NUL-trimmed before being read as
/// UTF-8 (an encoding name is always plain ASCII).
#[inline]
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let payload = fcinfo
        .ref_arg(i)
        .expect("to_ascii: name arg missing from by-ref lane");
    // Under the header-ful-everywhere convention a `name` arrives as a
    // varlena-framed NAMEDATALEN buffer; skip the 4-byte length word.
    const VARHDRSZ: usize = 4;
    let bytes: &[u8] = match payload {
        RefPayload::Varlena(b) => {
            let image = b.as_slice();
            if image.len() >= VARHDRSZ {
                &image[VARHDRSZ..]
            } else {
                &[]
            }
        }
        RefPayload::Cstring(s) => s.as_bytes(),
        other => panic!("to_ascii: name arg has unexpected by-ref payload {other:?}"),
    };
    // C: NameStr stops at the first NUL within the NAMEDATALEN buffer.
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("to_ascii: name arg not valid UTF-8")
}

/// Extract the owned by-ref `text` payload from a core's [`FmgrOut`]. (Done
/// while the input arg is still borrowed; the returned payload is owned, so the
/// caller can then take a fresh `&mut` to set the result.)
#[inline]
fn into_payload(out: FmgrOut<'_>) -> RefPayload {
    match out {
        FmgrOut::Ref(payload) => payload,
        FmgrOut::ByVal(_) => unreachable!("to_ascii always returns a by-ref text result"),
    }
}

/// Set the `text` result on the by-ref lane and return the dummy word.
#[inline]
fn ret(fcinfo: &mut FunctionCallInfoBaseData, payload: RefPayload) -> Datum {
    fcinfo.set_ref_result(payload);
    Datum::from_usize(0)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `to_ascii(text)` (OID 1845, `to_ascii_default`): transliterate using the
/// current database encoding.
fn fc_to_ascii_default(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let payload = {
        let data = arg_text(fcinfo, 0);
        match crate::to_ascii_default(data) {
            Ok(out) => into_payload(out),
            Err(e) => raise(e),
        }
    };
    ret(fcinfo, payload)
}

/// `to_ascii(text, int4)` (OID 1846, `to_ascii_enc`): transliterate using the
/// given encoding code.
fn fc_to_ascii_enc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let enc = arg_i32(fcinfo, 1);
    let payload = {
        let data = arg_text(fcinfo, 0);
        match crate::to_ascii_enc(data, enc) {
            Ok(out) => into_payload(out),
            Err(e) => raise(e),
        }
    };
    ret(fcinfo, payload)
}

/// `to_ascii(text, name)` (OID 1847, `to_ascii_encname`): transliterate using
/// the named encoding.
fn fc_to_ascii_encname(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let payload = {
        let data = arg_text(fcinfo, 0);
        let encname = arg_name(fcinfo, 1);
        match crate::to_ascii_encname(data, encname) {
            Ok(out) => into_payload(out),
            Err(e) => raise(e),
        }
    };
    ret(fcinfo, payload)
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

/// Register every SQL-callable `ascii.c` `to_ascii` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs /
/// nargs / strict / retset transcribed exactly from `pg_proc.dat` (all named
/// `to_ascii`, all strict by default, none retset).
pub fn register_ascii_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(1845, "to_ascii_default", 1, true, false, fc_to_ascii_default),
        builtin(1846, "to_ascii_enc", 2, true, false, fc_to_ascii_enc),
        builtin(1847, "to_ascii_encname", 2, true, false, fc_to_ascii_encname),
    ]);
}
