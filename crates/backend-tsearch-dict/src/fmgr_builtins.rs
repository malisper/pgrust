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

use backend_utils_adt_arrayfuncs_seams::construct_text_array_bytes;
use backend_tsearch_dict_seams::subdict_lexize;
use mcx::MemoryContext;
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

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
/// arg (after the 4-byte length word).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ts_lexize: by-ref text arg missing from by-ref lane");
    if image.len() >= VARHDRSZ {
        &image[VARHDRSZ..]
    } else {
        &[]
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
    backend_utils_fmgr_core::register_builtins_native([
        builtin(3723, "ts_lexize", 2, fc_ts_lexize),
    ]);
}
