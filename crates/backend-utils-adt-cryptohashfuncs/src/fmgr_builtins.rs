//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `cryptohashfuncs.c` functions: `md5(text)`, `md5(bytea)`, and
//! `sha224`/`sha256`/`sha384`/`sha512` over `bytea`.
//!
//! Each `fc_<name>` adapter reads its single `text`/`bytea` argument off the
//! fmgr call frame's by-ref lane (the boundary strips the varlena header, so we
//! get the detoasted `VARDATA_ANY` payload), calls the matching value core, and
//! writes the `text`/`bytea` result back on the by-ref `RefPayload::Varlena`
//! lane.
//!
//! [`register_cryptohashfuncs_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`). OIDs / nargs / strict /
//! retset are transcribed exactly from `pg_proc.dat`: every row is `nargs => 1`,
//! `proisstrict 't'`, `proretset 'f'`.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// `VARDATA_ANY` of a header-ful varlena image: payload after the 4-byte header.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    if image.len() >= VARHDRSZ {
        &image[VARHDRSZ..]
    } else {
        &[]
    }
}

/// Build a header-ful 4-byte-header varlena image from a payload.
#[inline]
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(payload);
    img
}

/// A `text`/`bytea` arg's detoasted by-ref payload bytes (`VARDATA_ANY`).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    vardata_any(
        fcinfo
            .ref_arg(i)
            .and_then(|p| p.as_varlena())
            .expect("cryptohashfuncs fn: by-ref arg missing from by-ref lane"),
    )
}

/// Set a `text`/`bytea` varlena result on the by-ref lane and return the dummy
/// word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(&bytes)));
    Datum::from_usize(0)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_md5_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let out = ok(crate::md5_text(arg_bytes(fcinfo, 0)));
    ret_varlena(fcinfo, out)
}
fn fc_md5_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let out = ok(crate::md5_bytea(arg_bytes(fcinfo, 0)));
    ret_varlena(fcinfo, out)
}
fn fc_sha224_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let out = crate::sha224_bytea(arg_bytes(fcinfo, 0));
    ret_varlena(fcinfo, out)
}
fn fc_sha256_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let out = crate::sha256_bytea(arg_bytes(fcinfo, 0));
    ret_varlena(fcinfo, out)
}
fn fc_sha384_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let out = crate::sha384_bytea(arg_bytes(fcinfo, 0));
    ret_varlena(fcinfo, out)
}
fn fc_sha512_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let out = crate::sha512_bytea(arg_bytes(fcinfo, 0));
    ret_varlena(fcinfo, out)
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

/// Register every SQL-callable `cryptohashfuncs.c` builtin. OIDs / nargs /
/// strict / retset transcribed exactly from `pg_proc.dat` (all `nargs => 1`,
/// all strict, none retset).
pub fn register_cryptohashfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // md5(text) / md5(bytea) — the builtin `name` is the `prosrc` C symbol
        // (canonical fmgr_builtins[] keys on prosrc, not the SQL proname).
        builtin(2311, "md5_text", 1, true, false, fc_md5_text),
        builtin(2321, "md5_bytea", 1, true, false, fc_md5_bytea),
        // sha224/256/384/512(bytea) — prosrc sha224_bytea … sha512_bytea.
        builtin(3419, "sha224_bytea", 1, true, false, fc_sha224_bytea),
        builtin(3420, "sha256_bytea", 1, true, false, fc_sha256_bytea),
        builtin(3421, "sha384_bytea", 1, true, false, fc_sha384_bytea),
        builtin(3422, "sha512_bytea", 1, true, false, fc_sha512_bytea),
    ]);
}
