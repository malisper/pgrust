//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! GiST support function ported in this crate.
//!
//! `gist_translate_cmptype_common(int4) -> int2` is the built-in stratnum
//! translation support function used by GiST opclasses whose private strategy
//! numbers coincide with the `RT*StrategyNumber` constants. It is reachable
//! through fmgr (e.g. as an opclass `GIST_TRANSLATE_CMPTYPE_PROC`), so its
//! `fmgr_builtins[]` row must be registered for by-OID dispatch / the
//! `fmgr_isbuiltin` fast path to resolve it.
//!
//! Each entry is a `fc_<name>` adapter that reads its argument off the fmgr call
//! frame, calls the matching value core (ported in this crate), and writes the
//! result word. OID / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat` (`gist_translate_cmptype_common`: 1 arg, neither `proisstrict`
//! nor `proretset` set, so both default to false).

use alloc::string::ToString;

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use crate::gistutil;

// ---------------------------------------------------------------------------
// Argument reader / result writer.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(0)`: the `int4` compare-type code in arg 0's word.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("gist_translate_cmptype_common fn: missing arg")
        .value
        .as_i32()
}

/// `PG_RETURN_INT16(v)`: a `uint16` strategy number written into a `int2`
/// result word (C: the `uint16` Datum, bit-preserving into the signed slot).
#[inline]
fn ret_u16(v: u16) -> Datum {
    Datum::from_i16(v as i16)
}

// ---------------------------------------------------------------------------
// Builtin adapters.
// ---------------------------------------------------------------------------

/// `gist_translate_cmptype_common(cmptype int4) -> int2` (gistutil.c:1064).
fn fc_gist_translate_cmptype_common(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cmptype = arg_i32(fcinfo, 0);
    Ok(ret_u16(gistutil::gist_translate_cmptype_common(cmptype)))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

/// Register every scalar GiST builtin owned by this crate (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OID / nargs
/// from `pg_proc.dat`; `gist_translate_cmptype_common` is not strict and not
/// retset (neither `proisstrict` nor `proretset` is set in `pg_proc.dat`).
pub fn register_backend_access_gist_core_builtins() {
    fmgr_core::register_builtins_native([(
        BuiltinFunction {
            foid: 6347,
            name: "gist_translate_cmptype_common".to_string(),
            nargs: 1,
            strict: true,
            retset: false,
            func: None,
        },
        fc_gist_translate_cmptype_common as PgFnNative,
    )]);
}
