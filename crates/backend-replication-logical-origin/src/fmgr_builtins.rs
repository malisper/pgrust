//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `pg_replication_origin_*` functions in `origin.c` whose argument/result
//! types are expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core (the real ported
//! `pg_replication_origin_*` function in `lib.rs`), and writes back the result
//! word / by-reference payload. [`register_backend_replication_logical_origin_builtins`]
//! registers every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so by-OID dispatch resolves them. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! Registered (all `text`-or-no-arg, scalar `oid`/`void`/`bool` results):
//!
//! * 6003 `pg_replication_origin_create` (text → oid)
//! * 6004 `pg_replication_origin_drop` (text → void)
//! * 6005 `pg_replication_origin_oid` (text → oid, NULL-able)
//! * 6006 `pg_replication_origin_session_setup` (text → void)
//! * 6007 `pg_replication_origin_session_reset` (→ void)
//! * 6008 `pg_replication_origin_session_is_setup` (→ bool)
//! * 6011 `pg_replication_origin_xact_reset` (→ void)
//! * 6009 `pg_replication_origin_session_progress` (bool → pg_lsn, NULL-able)
//! * 6010 `pg_replication_origin_xact_setup` (pg_lsn, timestamptz → void)
//! * 6012 `pg_replication_origin_advance` (text, pg_lsn → void)
//! * 6013 `pg_replication_origin_progress` (text, bool → pg_lsn, NULL-able)
//!
//! The set-returning sibling 6014 `pg_show_replication_origin_status` is an SRF
//! and is left to its own (materialized-SRF) registration.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::{Oid, TimestampTz, XLogRecPtr};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` → `text_to_cstring`: a `text` arg's detoasted
/// `VARDATA_ANY` payload bytes on the by-ref lane, decoded as UTF-8 (the C
/// origin functions all immediately `text_to_cstring` the name).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("origin fn: text arg missing from by-ref lane");
    // `VARDATA_ANY`: skip the 4-byte header on the header-ful image.
    let bytes = if image.len() >= 4 { &image[4..] } else { &[][..] };
    core::str::from_utf8(bytes).expect("origin fn: text arg not valid UTF-8")
}

/// `PG_GETARG_LSN(i)`: the `pg_lsn`/`XLogRecPtr` word.
#[inline]
fn arg_lsn(fcinfo: &FunctionCallInfoBaseData, i: usize) -> XLogRecPtr {
    fcinfo.arg(i).expect("origin fn: missing pg_lsn arg").value.as_u64()
}

/// `PG_GETARG_TIMESTAMPTZ(i)`: an 8-byte microsecond `TimestampTz` word.
#[inline]
fn arg_timestamptz(fcinfo: &FunctionCallInfoBaseData, i: usize) -> TimestampTz {
    fcinfo.arg(i).expect("origin fn: missing timestamptz arg").value.as_i64()
}

/// `PG_GETARG_BOOL(i)`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("origin fn: missing bool arg").value.as_bool()
}

/// `PG_RETURN_LSN(v)`: a `pg_lsn`/`XLogRecPtr` result word.
#[inline]
fn ret_lsn(v: XLogRecPtr) -> Datum {
    Datum::from_u64(v)
}

/// Set a NULL-able `pg_lsn` result; `None` is the C `PG_RETURN_NULL()`.
#[inline]
fn ret_lsn_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<XLogRecPtr>) -> Datum {
    match v {
        Some(l) => ret_lsn(l),
        None => {
            fcinfo.set_result_null(true);
            ret_void()
        }
    }
}

/// `PG_RETURN_OID(v)`: the assigned origin oid in the low 32 bits of the word.
#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}

/// `PG_RETURN_BOOL(v)`.
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// `PG_RETURN_VOID()`: a `void`-returning function's dummy result word.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

/// Set a NULL-able `oid` result; `None` is the C `PG_RETURN_NULL()`.
#[inline]
fn ret_oid_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<Oid>) -> Datum {
    match v {
        Some(o) => ret_oid(o),
        None => {
            fcinfo.set_result_null(true);
            ret_void()
        }
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_pg_replication_origin_create(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    Ok(ret_oid(crate::pg_replication_origin_create(name)?))
}

fn fc_pg_replication_origin_drop(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    crate::pg_replication_origin_drop(name)?;
    Ok(ret_void())
}

fn fc_pg_replication_origin_oid(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    let res = crate::pg_replication_origin_oid(name)?;
    Ok(ret_oid_opt(fcinfo, res))
}

fn fc_pg_replication_origin_session_setup(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    crate::pg_replication_origin_session_setup(name)?;
    Ok(ret_void())
}

fn fc_pg_replication_origin_session_reset(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    crate::pg_replication_origin_session_reset()?;
    Ok(ret_void())
}

fn fc_pg_replication_origin_session_is_setup(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::pg_replication_origin_session_is_setup()?))
}

fn fc_pg_replication_origin_xact_reset(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    crate::pg_replication_origin_xact_reset()?;
    Ok(ret_void())
}

fn fc_pg_replication_origin_session_progress(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let flush = arg_bool(fcinfo, 0);
    let res = crate::pg_replication_origin_session_progress(flush)?;
    Ok(ret_lsn_opt(fcinfo, res))
}

fn fc_pg_replication_origin_xact_setup(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let location = arg_lsn(fcinfo, 0);
    let timestamp = arg_timestamptz(fcinfo, 1);
    crate::pg_replication_origin_xact_setup(location, timestamp)?;
    Ok(ret_void())
}

fn fc_pg_replication_origin_advance(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    let remote_commit = arg_lsn(fcinfo, 1);
    crate::pg_replication_origin_advance(name, remote_commit)?;
    Ok(ret_void())
}

fn fc_pg_replication_origin_progress(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    let flush = arg_bool(fcinfo, 1);
    let res = crate::pg_replication_origin_progress(name, flush)?;
    Ok(ret_lsn_opt(fcinfo, res))
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

/// Register the `pg_replication_origin_*` builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/retset are
/// transcribed exactly from `pg_proc.dat`; none set `proisstrict` explicitly,
/// so all default to strict (`'t'`), and none are set-returning.
pub fn register_backend_replication_logical_origin_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(6003, "pg_replication_origin_create", 1, true, false, fc_pg_replication_origin_create),
        builtin(6004, "pg_replication_origin_drop", 1, true, false, fc_pg_replication_origin_drop),
        builtin(6005, "pg_replication_origin_oid", 1, true, false, fc_pg_replication_origin_oid),
        builtin(6006, "pg_replication_origin_session_setup", 1, true, false, fc_pg_replication_origin_session_setup),
        builtin(6007, "pg_replication_origin_session_reset", 0, true, false, fc_pg_replication_origin_session_reset),
        builtin(6008, "pg_replication_origin_session_is_setup", 0, true, false, fc_pg_replication_origin_session_is_setup),
        builtin(6011, "pg_replication_origin_xact_reset", 0, true, false, fc_pg_replication_origin_xact_reset),
        builtin(6009, "pg_replication_origin_session_progress", 1, true, false, fc_pg_replication_origin_session_progress),
        builtin(6010, "pg_replication_origin_xact_setup", 2, true, false, fc_pg_replication_origin_xact_setup),
        builtin(6012, "pg_replication_origin_advance", 2, true, false, fc_pg_replication_origin_advance),
        builtin(6013, "pg_replication_origin_progress", 2, true, false, fc_pg_replication_origin_progress),
    ]);
}
