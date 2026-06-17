//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `network.c`
//! session-info functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame (none here — both functions are 0-ary), calls the matching value
//! core, and writes back the result word (or sets `isnull` for the SQL-NULL
//! return). [`register_network_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! Only the `int4`-returning port functions are registered here: `int4` is a
//! by-value scalar, fully expressible. The `inet`-returning session functions
//! (`inet_client_addr` / `inet_server_addr`) are NOT registered — they return
//! the varlena `inet` type, which has no settled by-ref carrier at this
//! boundary (no `inet` send/out result lane modeled on the frame).

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_error::PgError;

// ---------------------------------------------------------------------------
// Result writers.
// ---------------------------------------------------------------------------

/// Write an `int4` (`PG_RETURN_INT32`) result word.
#[inline]
fn ret_int32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// `PG_RETURN_NULL()`: set `fcinfo->isnull` and return a dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_inet_client_port(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::inet_client_port() {
        Ok(Some(p)) => ret_int32(p),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

fn fc_inet_server_port(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::inet_server_port() {
        Ok(Some(p)) => ret_int32(p),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
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

/// Register the `network.c` `int4`-returning session-info builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`: both are
/// `proisstrict => 'f'`, 0-ary (`proargtypes => ''`), non-retset.
pub fn register_network_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(2197, "inet_client_port", 0, false, false, fc_inet_client_port),
        builtin(2199, "inet_server_port", 0, false, false, fc_inet_server_port),
    ]);
}
