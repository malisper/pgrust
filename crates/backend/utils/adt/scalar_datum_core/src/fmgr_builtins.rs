//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions of `datum.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_datum_core_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them.
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`.

use ::datum::Datum;
use ::types_error::PgResult;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("datum-core fn: missing arg")
        .value
        .as_oid()
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `btequalimage(opcintype oid) -> bool` (datum.c): the generic "equalimage"
/// support function. C: `PG_RETURN_BOOL(true)`, the `opcintype` argument unused.
fn fc_btequalimage(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_bool(crate::btequalimage(arg_oid(fcinfo, 0))))
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

/// Register `datum.c`'s SQL-callable builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict from
/// `pg_proc.dat` (`btequalimage`: 1 arg, strict default `t`, not retset).
pub fn register_datum_core_builtins() {
    fmgr_core::register_builtins_native([builtin(
        5051,
        "btequalimage",
        1,
        true,
        false,
        fc_btequalimage,
    )]);
}
