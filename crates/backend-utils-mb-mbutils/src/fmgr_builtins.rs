//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `mbutils.c` encoding functions whose argument/result types are expressible at
//! the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core (already ported in this crate),
//! and writes back the result word / by-reference payload.
//! [`register_mbutils_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch and the `fmgr_isbuiltin`
//! fast path resolve them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.
//!
//! A `name` value is pass-by-reference; it crosses the boundary as a varlena
//! image (C passes the `NameData` by pointer). `text`/`bytea` cross as the
//! [`RefPayload::Varlena`] byte image. The conversion cores share the C quirk
//! that when no conversion is required the *source* bytes stand unchanged
//! (`Ok(None)`); at the SQL boundary that means the result is the input image.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::NAMEDATALEN;
use types_tuple::heaptuple::NameData;

use crate::{
    PG_char_to_encoding, PG_encoding_to_char, getdatabaseencoding, length_in_encoding,
    pg_client_encoding, pg_convert, pg_convert_from, pg_convert_to, pg_encoding_max_length_sql,
};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// `VARDATA_ANY` of a header-ful varlena image: the payload after the 4-byte
/// length word.
#[inline]
fn varlena_payload(image: &[u8]) -> &[u8] {
    if image.len() >= VARHDRSZ {
        &image[VARHDRSZ..]
    } else {
        &[]
    }
}

/// Stamp a 4-byte uncompressed varlena length word in front of `payload`
/// (`SET_VARSIZE`), producing the full header-ful image.
#[inline]
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let mut image = Vec::with_capacity(payload.len() + VARHDRSZ);
    image.extend_from_slice(&types_datum::varlena::set_varsize_4b(payload.len() + VARHDRSZ));
    image.extend_from_slice(payload);
    image
}

/// `PG_GETARG_NAME(i)`: a `name` value as a `NameData` (a copy of the varlena
/// payload, NUL-padded to the fixed size). Under the header-ful-everywhere
/// convention the by-ref lane carries the full varlena image; this skips the
/// 4-byte header.
#[inline]
fn arg_name(fcinfo: &FunctionCallInfoBaseData, i: usize) -> NameData {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("mbutils fn: name arg missing from by-ref lane");
    let bytes = varlena_payload(image);
    let mut nd = NameData::default();
    let n = bytes.len().min(NAMEDATALEN as usize);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// `PG_GETARG_TEXT_PP(i)` / `PG_GETARG_BYTEA_PP(i)`: the varlena payload bytes
/// (C reads `VARDATA_ANY`/`VARSIZE_ANY_EXHDR`). Under the header-ful-everywhere
/// convention the by-ref lane carries the full varlena image; this skips the
/// 4-byte header.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    varlena_payload(
        fcinfo
            .ref_arg(i)
            .and_then(|p| p.as_varlena())
            .expect("mbutils fn: varlena arg missing from by-ref lane"),
    )
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// Set a `name` result: the full `NAMEDATALEN`-byte buffer, framed as a
/// header-ful varlena on the by-ref lane (4-byte length word + buffer),
/// symmetric with how `arg_name` reads it back.
#[inline]
fn ret_name(fcinfo: &mut FunctionCallInfoBaseData, nd: &NameData) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(&nd.data)));
    Datum::from_usize(0)
}

/// Set a `text`/`bytea` result: the varlena payload, framed as a header-ful
/// varlena on the by-ref lane (4-byte length word + payload), symmetric with
/// how `arg_varlena` reads it back.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(&bytes)));
    Datum::from_usize(0)
}

/// The `name`'s bytes up to the first NUL, as `&str` (C `NameStr`).
fn name_str(nd: &NameData) -> &str {
    std::str::from_utf8(nd.name_str()).unwrap_or("")
}

fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("mbutils fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

macro_rules! ok_or_raise {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => raise(e),
        }
    };
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_client_encoding()` → `name`. No args.
fn fc_pg_client_encoding(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nd = ok_or_raise!(pg_client_encoding());
    ret_name(fcinfo, &nd)
}

/// `getdatabaseencoding()` → `name`. No args.
fn fc_getdatabaseencoding(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nd = ok_or_raise!(getdatabaseencoding());
    ret_name(fcinfo, &nd)
}

/// `pg_char_to_encoding(name)` → `int4`.
fn fc_pg_char_to_encoding(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nd = arg_name(fcinfo, 0);
    ret_i32(PG_char_to_encoding(&nd))
}

/// `pg_encoding_to_char(int4)` → `name`.
fn fc_pg_encoding_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let encoding = fcinfo
        .arg(0)
        .expect("pg_encoding_to_char: missing arg")
        .value
        .as_i32();
    let nd = ok_or_raise!(PG_encoding_to_char(encoding));
    ret_name(fcinfo, &nd)
}

/// `length(bytea, name)` (`length_in_encoding`) → `int4`.
fn fc_length(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let string = arg_varlena(fcinfo, 0).to_vec();
    let enc = arg_name(fcinfo, 1);
    ret_i32(ok_or_raise!(length_in_encoding(&string, name_str(&enc))))
}

/// `convert_from(bytea, name)` (`pg_convert_from`) → `text`.
fn fc_convert_from(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let string = arg_varlena(fcinfo, 0).to_vec();
    let src = arg_name(fcinfo, 1);
    let m = scratch_mcx();
    let converted = ok_or_raise!(pg_convert_from(m.mcx(), &string, name_str(&src)))
        .map(|v| v.as_slice().to_vec());
    // A performed conversion uses the converted bytes; when no conversion was
    // required (`None`) C returns the source pointer unchanged.
    ret_varlena(fcinfo, converted.unwrap_or(string))
}

/// `convert_to(text, name)` (`pg_convert_to`) → `bytea`.
fn fc_convert_to(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let string = arg_varlena(fcinfo, 0).to_vec();
    let dest = arg_name(fcinfo, 1);
    let m = scratch_mcx();
    let converted = ok_or_raise!(pg_convert_to(m.mcx(), &string, name_str(&dest)))
        .map(|v| v.as_slice().to_vec());
    ret_varlena(fcinfo, converted.unwrap_or(string))
}

/// `convert(bytea, name, name)` (`pg_convert`) → `bytea`.
fn fc_convert(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let string = arg_varlena(fcinfo, 0).to_vec();
    let src = arg_name(fcinfo, 1);
    let dest = arg_name(fcinfo, 2);
    let m = scratch_mcx();
    let converted = ok_or_raise!(pg_convert(m.mcx(), &string, name_str(&src), name_str(&dest)))
        .map(|v| v.as_slice().to_vec());
    ret_varlena(fcinfo, converted.unwrap_or(string))
}

/// `pg_encoding_max_length(int4)` (`pg_encoding_max_length_sql`) → `int4`,
/// returning SQL NULL for an invalid encoding.
fn fc_pg_encoding_max_length(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let encoding = fcinfo
        .arg(0)
        .expect("pg_encoding_max_length: missing arg")
        .value
        .as_i32();
    match pg_encoding_max_length_sql(encoding) {
        Some(v) => ret_i32(v),
        None => {
            // C: PG_RETURN_NULL().
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
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

/// Register every SQL-callable `mbutils.c` encoding builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed from `pg_proc.dat` (none override
/// `proisstrict` so all are strict; none is a `proretset`).
pub fn register_mbutils_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(810, "pg_client_encoding", 0, true, false, fc_pg_client_encoding),
        builtin(1039, "getdatabaseencoding", 0, true, false, fc_getdatabaseencoding),
        builtin(1264, "PG_char_to_encoding", 1, true, false, fc_pg_char_to_encoding),
        builtin(1597, "PG_encoding_to_char", 1, true, false, fc_pg_encoding_to_char),
        builtin(1713, "length_in_encoding", 2, true, false, fc_length),
        builtin(1714, "pg_convert_from", 2, true, false, fc_convert_from),
        builtin(1717, "pg_convert_to", 2, true, false, fc_convert_to),
        builtin(1813, "pg_convert", 3, true, false, fc_convert),
        builtin(2319, "pg_encoding_max_length_sql", 1, true, false, fc_pg_encoding_max_length),
    ]);
}
