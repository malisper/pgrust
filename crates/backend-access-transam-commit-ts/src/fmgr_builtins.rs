//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! commit-timestamp functions in `commit_ts.c` whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word. [`register_backend_access_transam_commit_ts_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.
//!
//! Registered:
//!
//! * 3581 `pg_xact_commit_timestamp` (xid → timestamptz, NULL-able)
//!
//! The composite-record-returning siblings (3583 `pg_last_committed_xact`,
//! 6168 `pg_xact_commit_timestamp_origin`) are NOT expressible at this boundary
//! and are left to their own registration.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::{TimestampTz, TransactionId};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TRANSACTIONID(i)`: a 32-bit `xid` word.
#[inline]
fn arg_xid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> TransactionId {
    fcinfo
        .arg(i)
        .expect("commit_ts fn: missing xid arg")
        .value
        .as_u32()
}

/// `PG_RETURN_VOID()`: a dummy result word (used when the result is NULL).
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

/// Set a NULL-able `timestamptz` result; `None` is the C `PG_RETURN_NULL()`.
#[inline]
fn ret_timestamptz_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<TimestampTz>) -> Datum {
    match v {
        Some(ts) => Datum::from_i64(ts),
        None => {
            fcinfo.set_result_null(true);
            ret_void()
        }
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_pg_xact_commit_timestamp(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let xid = arg_xid(fcinfo, 0);
    let ts = crate::with_commit_ts_state(|state| crate::pg_xact_commit_timestamp(state, xid))?;
    Ok(ret_timestamptz_opt(fcinfo, ts))
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

/// Register the commit-timestamp builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/retset are transcribed
/// exactly from `pg_proc.dat`; `pg_xact_commit_timestamp` has no explicit
/// `proisstrict`, so it defaults to strict (`'t'`), and is not set-returning.
pub fn register_backend_access_transam_commit_ts_builtins() {
    backend_utils_fmgr_core::register_builtins_native([builtin(
        3581,
        "pg_xact_commit_timestamp",
        1,
        true,
        false,
        fc_pg_xact_commit_timestamp,
    )]);
}
