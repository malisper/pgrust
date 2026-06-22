//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `pg_crc.c` functions `crc32(bytea)` and `crc32c(bytea)`.
//!
//! Each `fc_<name>` adapter reads its single `bytea` argument off the fmgr call
//! frame's by-ref lane (the boundary strips the varlena header, so we get the
//! detoasted `VARDATA_ANY` payload), calls the matching value core, and returns
//! the CRC as an `int8` on the by-value lane (`PG_RETURN_INT64`).
//!
//! [`register_pg_crc_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`). OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`: both rows are `nargs => 1`, `proisstrict 't'`,
//! `proretset 'f'`.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// `VARDATA_ANY` of an inline (non-compressed, non-external) varlena image: skip
/// ONE header byte for a short (1-byte) header, else `VARHDRSZ`. A small stored
/// value arrives short-headed once `SHORT_VARLENA_PACKING` is on; a fixed
/// `VARHDRSZ` strip would drop three payload bytes. No-op while packing is off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// A `bytea` arg's detoasted by-ref payload bytes (`VARDATA_ANY`,
/// `len = VARSIZE_ANY_EXHDR`).
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    vardata_any(
        fcinfo
            .ref_arg(i)
            .and_then(|p| p.as_varlena())
            .expect("pg_crc fn: by-ref bytea arg missing from by-ref lane"),
    )
}

/// `PG_RETURN_INT64(crc)`: a `pg_crc32` (uint32) zero-extends to `int64`.
#[inline]
fn ret_crc(crc: u32) -> Datum {
    Datum::from_i64(crc as i64)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_crc32_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_crc(crate::crc32_bytea(arg_bytes(fcinfo, 0))))
}
fn fc_crc32c_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_crc(crate::crc32c_bytea(arg_bytes(fcinfo, 0))))
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
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register every SQL-callable `pg_crc.c` builtin. OIDs / nargs / strict /
/// retset transcribed exactly from `pg_proc.dat` (both `nargs => 1`, strict,
/// none retset). The builtin `name` is the `prosrc` C symbol (the canonical
/// `fmgr_builtins[]` keys on prosrc, not the SQL proname).
pub fn register_pg_crc_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(6364, "crc32_bytea", 1, true, false, fc_crc32_bytea),
        builtin(6365, "crc32c_bytea", 1, true, false, fc_crc32c_bytea),
    ]);
}
