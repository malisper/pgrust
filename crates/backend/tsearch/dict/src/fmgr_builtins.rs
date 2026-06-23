//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for `dict.c`'s
//! SQL-callable `ts_lexize(regdictionary, text)` debug function.
//!
//! `ts_lexize` arg 0 is a by-value `regdictionary` `Oid`; arg 1 is a `text`
//! whose `VARDATA_ANY` payload (header stripped) is the word to normalize. The
//! result is a `text[]` array of the resulting lexemes — a flat header-ful
//! varlena image that crosses VERBATIM on the by-ref `RefPayload::Varlena` lane
//! (built by the `construct_text_array_bytes` seam). `PG_RETURN_NULL()` (no
//! lexemes / the dictionary filtered the word) sets the fmgr null flag.
//!
//! [`register_dict_builtins`] registers the row into fmgr-core's by-OID dispatch
//! table (C: its `fmgr_builtins[]` row). OID / nargs / strict / retset are from
//! `pg_proc.dat` (`ts_lexize` is `proisstrict => 't'`, not retset).

use alloc::string::ToString;

use ::arrayfuncs_seams::construct_text_array_bytes;
use ::dict_seams::subdict_lexize;
use ::mcx::MemoryContext;
use ::types_core::Oid;
use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

const VARHDRSZ: usize = 4;

/// `PG_GETARG_OID(i)`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("ts_lexize: missing oid arg")
        .value
        .as_oid()
}

/// `VARDATA_ANY(PG_GETARG_TEXT_PP(i))`: the payload bytes of a header-ful `text`
/// arg. `PG_GETARG_TEXT_PP` is `VARDATA_ANY`, header-form-agnostic: a small stored
/// `text` token arrives with a 1-byte ("short") header once
/// `SHORT_VARLENA_PACKING` is on, so skip ONE byte for a genuine short header (low
/// bit set, but not the lone `0x01` external tag), else the 4-byte `VARHDRSZ`. A
/// fixed `VARHDRSZ` strip would land 3 bytes into (or past) a short value's
/// payload — e.g. `ts_debug`'s lexize token `abc` came back empty. No-op while the
/// flag is OFF (every stored value is 4-byte).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ts_lexize: by-ref text arg missing from by-ref lane");
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `ts_lexize(PG_FUNCTION_ARGS)` (dict.c). Looks up the dictionary, runs its
/// `lexize` method on the word, and returns a `text[]` of the resulting lexemes
/// (or `NULL` when the dictionary recognizes/filters the word into nothing).
///
/// Mirrors [`crate::dict::ts_lexize`], but lowers the result onto the fmgr
/// by-ref lane as a flat `text[]` varlena image (via the
/// `construct_text_array_bytes` seam) rather than a bare in-`mcx` pointer word.
fn fc_ts_lexize(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let dict_id = arg_oid(fcinfo, 0);
    let input = arg_text(fcinfo, 1).to_vec();

    // res = FunctionCall4(&dict->lexize, ...); the single-shot dispatch crosses
    // the seam (see crate::dict::ts_lexize for the two-shot C modeling note).
    let res = subdict_lexize::call(dict_id, input)?;

    // if (!res) PG_RETURN_NULL();
    let Some(res) = res else {
        fcinfo.set_result_null(true);
        return Ok(Datum::null());
    };

    // Build the text[] of every (non-terminator) lexeme.
    let elems: alloc::vec::Vec<&str> = res.iter().map(|lex| lex.lexeme.as_str()).collect();

    let m = MemoryContext::new("ts_lexize fmgr scratch");
    let image = construct_text_array_bytes::call(m.mcx(), &elems)?;
    fcinfo.set_ref_result(RefPayload::Varlena(image.to_vec()));
    Ok(Datum::from_usize(0))
}

fn builtin(foid: u32, name: &str, nargs: i16, native: PgFnNative) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the `dict.c` fmgr builtins. Called from this crate's `init_seams()`.
/// OID / nargs from `pg_proc.dat`.
pub fn register_dict_builtins() {
    fmgr_core::register_builtins_native([
        builtin(3723, "ts_lexize", 2, fc_ts_lexize),
    ]);
}
