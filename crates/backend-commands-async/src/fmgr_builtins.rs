//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! async functions in `async.c`: `pg_notify` and `pg_notification_queue_usage`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in this crate, and writes back the
//! result word. [`register_async_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`) so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! Note `pg_notify` is `proisstrict => 'f'`: it is *not* strict, so the C body
//! (`async.c:556`) substitutes `""` for a NULL channel/payload rather than
//! returning NULL. The adapter mirrors that NULL-decision before calling the
//! core.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_is_null(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `PG_GETARG_TEXT_PP(i)` → `text_to_cstring`: a `text` arg's `VARDATA_ANY`
/// payload bytes on the by-ref lane, decoded as UTF-8.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("async fn: text arg missing from by-ref lane");
    // `VARDATA_ANY`: skip the 4-byte header on the header-ful image.
    let bytes = if image.len() >= 4 { &image[4..] } else { &[][..] };
    core::str::from_utf8(bytes).expect("async fn: text arg not valid UTF-8")
}

/// `PG_RETURN_VOID()`: the dummy result word for a `void`-returning function.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

/// `PG_RETURN_FLOAT8(v)`.
#[inline]
fn ret_f64(v: f64) -> Datum {
    Datum::from_f64(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `Datum pg_notify(PG_FUNCTION_ARGS)` (async.c:556). Not strict: a NULL
/// channel or payload becomes `""`.
fn fc_pg_notify(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let channel = if arg_is_null(fcinfo, 0) { "" } else { arg_text(fcinfo, 0) };
    let payload = if arg_is_null(fcinfo, 1) { "" } else { arg_text(fcinfo, 1) };
    match crate::pg_notify_core(channel, payload) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}

/// `Datum pg_notification_queue_usage(PG_FUNCTION_ARGS)` (async.c:1481).
fn fc_pg_notification_queue_usage(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::pg_notification_queue_usage_core() {
        Ok(usage) => ret_f64(usage),
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

/// Register the async-function builtins into the fmgr-core builtin table.
pub fn register_async_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // pg_proc.dat oid 3036: pg_notify(text, text) -> void, proisstrict='f'.
        builtin(3036, "pg_notify", 2, false, false, fc_pg_notify),
        // pg_proc.dat oid 3296: pg_notification_queue_usage() -> float8.
        builtin(3296, "pg_notification_queue_usage", 0, true, false,
            fc_pg_notification_queue_usage),
    ]);
}
