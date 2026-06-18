//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for every SQL-callable
//! function in `date.c`, `timestamp.c`, and `datetime.c`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core (already ported in this crate),
//! and writes back the result word / by-reference payload. `register_datetime_builtins`
//! registers every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so the executor's by-OID dispatch resolves them. OIDs / nargs / strict /
//! retset are transcribed exactly from `pg_proc.dat`.
//!
//! Marshaling convention (mirrors `backend-access-hashfunc`):
//!  * by-value arg `i`: `fcinfo.arg(i).unwrap().value.as_i32()/as_i64()/...`
//!  * cstring arg: `fcinfo.ref_arg(i).unwrap().as_cstring().unwrap()`
//!  * varlena/by-ref arg (interval/timetz/text/bytea): `fcinfo.ref_arg(i)
//!    .unwrap().as_varlena().unwrap()`; interval/timetz are POD, decoded with
//!    the little-endian field readers below.
//!  * by-value result: `Datum::from_i32/.../from_bool`.
//!  * cstring result (`_out`): `set_ref_result(Cstring(s))` + a 0 word.
//!  * varlena result (interval/timetz/bytea/text): `set_ref_result(Varlena(b))`.
//!  * a fallible core's `Err` is raised through the one fmgr dispatch point via
//!    `raise()` (a structured SQLSTATE panic the dispatcher rebuilds).

use types_fmgr::FunctionCallInfoBaseData;
use types_datum::Datum;
use types_datetime::{Interval, TimeTzADT};

use crate::binio::WireReader;

// ===========================================================================
// Argument readers / result writers.
// ===========================================================================

#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("datetime fn: missing arg").value.as_i32()
}
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("datetime fn: missing arg").value.as_i64()
}
#[inline]
fn arg_u64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> u64 {
    fcinfo.arg(i).expect("datetime fn: missing arg").value.as_u64()
}
#[inline]
fn arg_f64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f64 {
    fcinfo.arg(i).expect("datetime fn: missing arg").value.as_f64()
}
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("datetime fn: missing arg").value.as_bool()
}
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("datetime fn: cstring arg missing from by-ref lane")
}
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("datetime fn: by-ref arg missing from by-ref lane")
}

/// Decode a `text` argument's varlena image into a `&str` (C: `text_to_cstring`).
/// The value cores take ordinary `&str`; the varlena 4-byte length header is
/// already stripped by the boundary, which hands us the bare payload bytes.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    core::str::from_utf8(arg_varlena(fcinfo, i)).expect("datetime fn: invalid UTF-8 text arg")
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}
#[inline]
fn ret_u32(v: u32) -> Datum {
    Datum::from_u32(v)
}
#[inline]
fn ret_u64(v: u64) -> Datum {
    Datum::from_u64(v)
}
#[inline]
fn ret_f64(v: f64) -> Datum {
    Datum::from_f64(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a varlena result (interval/timetz/bytea/text) on the by-ref lane.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

// ---------------------------------------------------------------------------
// POD byte (de)serializers for the by-reference fixed-length types.
//
// On-disk/wire image is the C struct's little-endian field layout with no
// alignment padding in the boundary byte image:
//   Interval  = time:i64, day:i32, month:i32          (16 bytes)
//   TimeTzADT = time:i64(TimeADT), zone:i32           (12 bytes)
// ---------------------------------------------------------------------------

fn interval_from_bytes(b: &[u8]) -> Interval {
    Interval {
        time: i64::from_le_bytes(b[0..8].try_into().expect("interval image >= 16 bytes")),
        day: i32::from_le_bytes(b[8..12].try_into().expect("interval image >= 16 bytes")),
        month: i32::from_le_bytes(b[12..16].try_into().expect("interval image >= 16 bytes")),
    }
}
fn interval_to_bytes(iv: &Interval) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&(iv.time as i64).to_le_bytes());
    v.extend_from_slice(&iv.day.to_le_bytes());
    v.extend_from_slice(&iv.month.to_le_bytes());
    v
}
fn timetz_from_bytes(b: &[u8]) -> TimeTzADT {
    TimeTzADT {
        time: i64::from_le_bytes(b[0..8].try_into().expect("timetz image >= 12 bytes")),
        zone: i32::from_le_bytes(b[8..12].try_into().expect("timetz image >= 12 bytes")),
    }
}
fn timetz_to_bytes(t: &TimeTzADT) -> Vec<u8> {
    let mut v = Vec::with_capacity(12);
    v.extend_from_slice(&(t.time as i64).to_le_bytes());
    v.extend_from_slice(&t.zone.to_le_bytes());
    v
}

#[inline]
fn arg_interval(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Interval {
    interval_from_bytes(arg_varlena(fcinfo, i))
}
#[inline]
fn arg_timetz(fcinfo: &FunctionCallInfoBaseData, i: usize) -> TimeTzADT {
    timetz_from_bytes(arg_varlena(fcinfo, i))
}
#[inline]
fn ret_interval(fcinfo: &mut FunctionCallInfoBaseData, iv: &Interval) -> Datum {
    ret_varlena(fcinfo, interval_to_bytes(iv))
}
#[inline]
fn ret_timetz(fcinfo: &mut FunctionCallInfoBaseData, t: &TimeTzADT) -> Datum {
    ret_varlena(fcinfo, timetz_to_bytes(t))
}

/// Serialize a `NumericVar` to its numeric varlena byte image (C: `make_result`),
/// copying into an owned `Vec` so the scratch context can drop. Raises on OOM.
fn numericvar_to_bytes(var: &types_numeric::var::NumericVar<'_>) -> Vec<u8> {
    let m = scratch_mcx();
    let pgvec = match backend_utils_adt_numeric::convert::make_result(m.mcx(), var) {
        Ok(b) => b,
        Err(e) => raise(e),
    };
    let copy = pgvec.as_slice().to_vec();
    drop(pgvec);
    copy
}

/// A scratch context for cores that allocate their result through `Mcx`
/// (`extract`/`date_part` numeric results, `to_timestamp`). PG allocates in the
/// current per-call context; here the owned result is read out and copied into
/// the by-value word / by-ref bytes before the scratch context drops.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("datetime fmgr scratch")
}

/// Marshal an `ExtractResult` (date_part float8 / EXTRACT numeric / NULL) into
/// the fmgr result. `retnumeric` selects the numeric varlena lane.
fn ret_extract(
    fcinfo: &mut FunctionCallInfoBaseData,
    r: crate::extract::ExtractResult<'_>,
    retnumeric: bool,
) -> Datum {
    match r {
        crate::extract::ExtractResult::Float8(f) => ret_f64(f),
        crate::extract::ExtractResult::Numeric(var) => {
            let _ = retnumeric;
            let bytes = numericvar_to_bytes(&var);
            ret_varlena(fcinfo, bytes)
        }
        crate::extract::ExtractResult::Null => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

// ===========================================================================
// date.c builtins.
// ===========================================================================

fn fc_date_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    match crate::date::date_in(s) {
        Ok(d) => ret_i32(d),
        Err(e) => raise(e),
    }
}
fn fc_date_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let d = arg_i32(fcinfo, 0);
    let s = crate::date::date_out(d);
    ret_cstring(fcinfo, s)
}
fn fc_date_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let bytes = arg_varlena(fcinfo, 0);
    let mut r = WireReader::new(bytes);
    match crate::binio::date_recv(&mut r) {
        Ok(d) => ret_i32(d),
        Err(e) => raise(e),
    }
}
fn fc_date_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let d = arg_i32(fcinfo, 0);
    ret_varlena(fcinfo, crate::binio::date_send(d))
}
fn fc_make_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::make_date(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1), arg_i32(fcinfo, 2)) {
        Ok(d) => ret_i32(d),
        Err(e) => raise(e),
    }
}
fn fc_date_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::date::date_eq(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::date::date_ne(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::date::date_lt(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::date::date_le(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::date::date_gt(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::date::date_ge(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::date::date_cmp(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_hashdate(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(crate::hash::hashdate(arg_i32(fcinfo, 0)))
}
fn fc_hashdateextended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u64(crate::hash::hashdateextended(arg_i32(fcinfo, 0), arg_u64(fcinfo, 1)))
}
fn fc_date_finite(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::date::date_finite(arg_i32(fcinfo, 0)))
}
fn fc_date_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::date::date_larger(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::date::date_smaller(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_date_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::date_mi(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)) {
        Ok(v) => ret_i32(v),
        Err(e) => raise(e),
    }
}
fn fc_date_pli(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::date_pli(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)) {
        Ok(v) => ret_i32(v),
        Err(e) => raise(e),
    }
}
fn fc_date_mii(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::date_mii(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)) {
        Ok(v) => ret_i32(v),
        Err(e) => raise(e),
    }
}

// date vs timestamp / timestamptz comparisons (via the internal-cmp cores).
macro_rules! date_ts_cmp {
    ($fn:ident, $core:path, $op:tt) => {
        fn $fn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let d = arg_i32(fcinfo, 0);
            let ts = arg_i64(fcinfo, 1);
            ret_bool($core(d, ts) $op 0)
        }
    };
}
date_ts_cmp!(fc_date_eq_timestamp, crate::date::date_cmp_timestamp_internal, ==);
date_ts_cmp!(fc_date_ne_timestamp, crate::date::date_cmp_timestamp_internal, !=);
date_ts_cmp!(fc_date_lt_timestamp, crate::date::date_cmp_timestamp_internal, <);
date_ts_cmp!(fc_date_gt_timestamp, crate::date::date_cmp_timestamp_internal, >);
date_ts_cmp!(fc_date_le_timestamp, crate::date::date_cmp_timestamp_internal, <=);
date_ts_cmp!(fc_date_ge_timestamp, crate::date::date_cmp_timestamp_internal, >=);
date_ts_cmp!(fc_date_eq_timestamptz, crate::date::date_cmp_timestamptz_internal, ==);
date_ts_cmp!(fc_date_ne_timestamptz, crate::date::date_cmp_timestamptz_internal, !=);
date_ts_cmp!(fc_date_lt_timestamptz, crate::date::date_cmp_timestamptz_internal, <);
date_ts_cmp!(fc_date_gt_timestamptz, crate::date::date_cmp_timestamptz_internal, >);
date_ts_cmp!(fc_date_le_timestamptz, crate::date::date_cmp_timestamptz_internal, <=);
date_ts_cmp!(fc_date_ge_timestamptz, crate::date::date_cmp_timestamptz_internal, >=);

fn fc_date_cmp_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::date::date_cmp_timestamp_internal(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_date_cmp_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::date::date_cmp_timestamptz_internal(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)))
}

// timestamp/timestamptz vs date comparisons (args swapped; negate the cmp).
macro_rules! ts_date_cmp {
    ($fn:ident, $core:path, $op:tt) => {
        fn $fn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let ts = arg_i64(fcinfo, 0);
            let d = arg_i32(fcinfo, 1);
            ret_bool((-$core(d, ts)) $op 0)
        }
    };
}
ts_date_cmp!(fc_timestamp_eq_date, crate::date::date_cmp_timestamp_internal, ==);
ts_date_cmp!(fc_timestamp_ne_date, crate::date::date_cmp_timestamp_internal, !=);
ts_date_cmp!(fc_timestamp_lt_date, crate::date::date_cmp_timestamp_internal, <);
ts_date_cmp!(fc_timestamp_gt_date, crate::date::date_cmp_timestamp_internal, >);
ts_date_cmp!(fc_timestamp_le_date, crate::date::date_cmp_timestamp_internal, <=);
ts_date_cmp!(fc_timestamp_ge_date, crate::date::date_cmp_timestamp_internal, >=);
ts_date_cmp!(fc_timestamptz_eq_date, crate::date::date_cmp_timestamptz_internal, ==);
ts_date_cmp!(fc_timestamptz_ne_date, crate::date::date_cmp_timestamptz_internal, !=);
ts_date_cmp!(fc_timestamptz_lt_date, crate::date::date_cmp_timestamptz_internal, <);
ts_date_cmp!(fc_timestamptz_gt_date, crate::date::date_cmp_timestamptz_internal, >);
ts_date_cmp!(fc_timestamptz_le_date, crate::date::date_cmp_timestamptz_internal, <=);
ts_date_cmp!(fc_timestamptz_ge_date, crate::date::date_cmp_timestamptz_internal, >=);

fn fc_timestamp_cmp_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(-crate::date::date_cmp_timestamp_internal(arg_i32(fcinfo, 1), arg_i64(fcinfo, 0)))
}
fn fc_timestamptz_cmp_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(-crate::date::date_cmp_timestamptz_internal(arg_i32(fcinfo, 1), arg_i64(fcinfo, 0)))
}

fn fc_in_range_date_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_i32(fcinfo, 0);
    let base = arg_i32(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    let sub = arg_bool(fcinfo, 3);
    let less = arg_bool(fcinfo, 4);
    match crate::in_range::in_range_date_interval(val, base, &offset, sub, less) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}
fn fc_extract_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // extract(text, date): unit lowercased, returns numeric.
    let units = arg_text(fcinfo, 0).to_lowercase();
    let date = arg_i32(fcinfo, 1);
    match crate::date::extract_date(&units, date) {
        Ok(r) => ret_extract_date(fcinfo, r),
        Err(e) => raise(e),
    }
}
fn ret_extract_date(
    fcinfo: &mut FunctionCallInfoBaseData,
    r: crate::date::ExtractDateResult,
) -> Datum {
    use crate::date::ExtractDateResult::*;
    use types_numeric::var::{NumericSign, NumericVar};
    let m = scratch_mcx();
    let var: NumericVar = match r {
        Int(i) => match backend_utils_adt_numeric::kernel_transcendental::int64_to_numericvar(
            m.mcx(),
            i,
        ) {
            Ok(v) => v,
            Err(e) => raise(e),
        },
        Null => {
            fcinfo.set_result_null(true);
            return Datum::from_usize(0);
        }
        PosInfinity => NumericVar::special(m.mcx(), NumericSign::PInf),
        NegInfinity => NumericVar::special(m.mcx(), NumericSign::NInf),
    };
    let bytes = numericvar_to_bytes(&var);
    ret_varlena(fcinfo, bytes)
}
fn fc_date_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let d = arg_i32(fcinfo, 0);
    let iv = arg_interval(fcinfo, 1);
    match crate::date::date_pl_interval(d, &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_date_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let d = arg_i32(fcinfo, 0);
    let iv = arg_interval(fcinfo, 1);
    match crate::date::date_mi_interval(d, &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_date_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::date2timestamp(arg_i32(fcinfo, 0)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::timestamp_date(arg_i64(fcinfo, 0)) {
        Ok(d) => ret_i32(d),
        Err(e) => raise(e),
    }
}
fn fc_date_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::date2timestamptz(arg_i32(fcinfo, 0)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::date::timestamptz_date(arg_i64(fcinfo, 0)) {
        Ok(d) => ret_i32(d),
        Err(e) => raise(e),
    }
}

// --- TIME (date.c) ---------------------------------------------------------

fn fc_time_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 2);
    match crate::time::time_in(s, typmod) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_time_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = crate::time::time_out(arg_i64(fcinfo, 0));
    ret_cstring(fcinfo, s)
}
fn fc_time_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_i32(fcinfo, 2);
    let bytes = arg_varlena(fcinfo, 0);
    let mut r = WireReader::new(bytes);
    match crate::binio::time_recv(&mut r, typmod) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_time_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_varlena(fcinfo, crate::binio::time_send(arg_i64(fcinfo, 0)))
}
fn fc_make_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::time::make_time(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1), arg_f64(fcinfo, 2)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_time_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut t = arg_i64(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 1);
    crate::time::AdjustTimeForTypmod(&mut t, typmod);
    ret_i64(t)
}
fn fc_time_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::time::time_eq(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::time::time_ne(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::time::time_lt(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::time::time_le(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::time::time_gt(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::time::time_ge(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::time::time_cmp(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(crate::hash::time_hash(arg_i64(fcinfo, 0)))
}
fn fc_time_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u64(crate::hash::time_hash_extended(arg_i64(fcinfo, 0), arg_u64(fcinfo, 1)))
}
fn fc_time_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i64(crate::time::time_larger(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_time_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i64(crate::time::time_smaller(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_overlaps_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // overlaps is NOT strict: each arg may be NULL. The core takes Option<TimeADT>.
    let ts1 = nullable_i64(fcinfo, 0);
    let te1 = nullable_i64(fcinfo, 1);
    let ts2 = nullable_i64(fcinfo, 2);
    let te2 = nullable_i64(fcinfo, 3);
    match crate::overlaps::overlaps_time(ts1, te1, ts2, te2) {
        Some(b) => ret_bool(b),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}
fn fc_timestamp_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamp_time(arg_i64(fcinfo, 0)) {
        Ok(Some(t)) => ret_i64(t),
        Ok(None) => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamptz_time(arg_i64(fcinfo, 0)) {
        Ok(Some(t)) => ret_i64(t),
        Ok(None) => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
        Err(e) => raise(e),
    }
}
fn fc_datetime_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::datetime_timestamp(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_time_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = crate::time::time_interval(arg_i64(fcinfo, 0));
    ret_interval(fcinfo, &iv)
}
fn fc_interval_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    match crate::time::interval_time(&iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_time_mi_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = crate::time::time_mi_time(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1));
    ret_interval(fcinfo, &iv)
}
fn fc_time_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    match crate::time::time_pl_interval(arg_i64(fcinfo, 0), &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_time_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    match crate::time::time_mi_interval(arg_i64(fcinfo, 0), &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_in_range_time_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_i64(fcinfo, 0);
    let base = arg_i64(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    let sub = arg_bool(fcinfo, 3);
    let less = arg_bool(fcinfo, 4);
    match crate::in_range::in_range_time_interval(val, base, &offset, sub, less) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}
fn fc_time_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_time_part(fcinfo, false)
}
fn fc_extract_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_time_part(fcinfo, true)
}
fn ret_time_part(fcinfo: &mut FunctionCallInfoBaseData, retnumeric: bool) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    let time = arg_i64(fcinfo, 1);
    let m = scratch_mcx();
    let r = match crate::time::time_part_common(m.mcx(), &units, time, retnumeric) {
        Ok(r) => r,
        Err(e) => raise(e),
    };
    ret_time_part_result(fcinfo, r)
}
fn ret_time_part_result(
    fcinfo: &mut FunctionCallInfoBaseData,
    r: crate::time::TimePartResult<'_>,
) -> Datum {
    use crate::time::TimePartResult::*;
    match r {
        Float(f) => ret_f64(f),
        // The `Int` variant is produced only on the EXTRACT (retnumeric) path
        // (the core returns `Float` for `date_part`); C does
        // `PG_RETURN_NUMERIC(int64_to_numeric(intresult))` (date.c:2302), so the
        // integer field must be returned as a numeric varlena, not a float8.
        Int(i) => ret_int64_numeric(fcinfo, i),
        Numeric(var) => {
            let bytes = numericvar_to_bytes(&var);
            ret_varlena(fcinfo, bytes)
        }
    }
}

/// `int64_to_numeric(i)` -> numeric varlena result (the EXTRACT integer-field
/// path, C `PG_RETURN_NUMERIC(int64_to_numeric(intresult))`).
fn ret_int64_numeric(fcinfo: &mut FunctionCallInfoBaseData, i: i64) -> Datum {
    let m = scratch_mcx();
    let var = match backend_utils_adt_numeric::kernel_transcendental::int64_to_numericvar(
        m.mcx(),
        i,
    ) {
        Ok(v) => v,
        Err(e) => raise(e),
    };
    let bytes = numericvar_to_bytes(&var);
    ret_varlena(fcinfo, bytes)
}

// --- TIMETZ (date.c) -------------------------------------------------------

fn fc_timetz_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 2);
    match crate::timetz::timetz_in(s, typmod) {
        Ok(t) => ret_timetz(fcinfo, &t),
        Err(e) => raise(e),
    }
}
fn fc_timetz_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    ret_cstring(fcinfo, crate::timetz::timetz_out(&t))
}
fn fc_timetz_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_i32(fcinfo, 2);
    let bytes = arg_varlena(fcinfo, 0);
    let mut r = WireReader::new(bytes);
    match crate::binio::timetz_recv(&mut r, typmod) {
        Ok(t) => ret_timetz(fcinfo, &t),
        Err(e) => raise(e),
    }
}
fn fc_timetz_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    ret_varlena(fcinfo, crate::binio::timetz_send(&t))
}
fn fc_timetz_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 1);
    let r = crate::timetz::timetz_scale(&t, typmod);
    ret_timetz(fcinfo, &r)
}
macro_rules! timetz_cmp_bool {
    ($fn:ident, $core:path) => {
        fn $fn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_timetz(fcinfo, 0);
            let b = arg_timetz(fcinfo, 1);
            ret_bool($core(&a, &b))
        }
    };
}
timetz_cmp_bool!(fc_timetz_eq, crate::timetz::timetz_eq);
timetz_cmp_bool!(fc_timetz_ne, crate::timetz::timetz_ne);
timetz_cmp_bool!(fc_timetz_lt, crate::timetz::timetz_lt);
timetz_cmp_bool!(fc_timetz_le, crate::timetz::timetz_le);
timetz_cmp_bool!(fc_timetz_gt, crate::timetz::timetz_gt);
timetz_cmp_bool!(fc_timetz_ge, crate::timetz::timetz_ge);
fn fc_timetz_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_timetz(fcinfo, 0);
    let b = arg_timetz(fcinfo, 1);
    ret_i32(crate::timetz::timetz_cmp(&a, &b))
}
fn fc_timetz_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    ret_u32(crate::hash::timetz_hash(&t))
}
fn fc_timetz_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    ret_u64(crate::hash::timetz_hash_extended(&t, arg_u64(fcinfo, 1)))
}
fn fc_timetz_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_timetz(fcinfo, 0);
    let b = arg_timetz(fcinfo, 1);
    let r = crate::timetz::timetz_larger(a, b);
    ret_timetz(fcinfo, &r)
}
fn fc_timetz_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_timetz(fcinfo, 0);
    let b = arg_timetz(fcinfo, 1);
    let r = crate::timetz::timetz_smaller(a, b);
    ret_timetz(fcinfo, &r)
}
fn fc_timetz_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    let iv = arg_interval(fcinfo, 1);
    match crate::timetz::timetz_pl_interval(&t, &iv) {
        Ok(r) => ret_timetz(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_timetz_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    let iv = arg_interval(fcinfo, 1);
    match crate::timetz::timetz_mi_interval(&t, &iv) {
        Ok(r) => ret_timetz(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_in_range_timetz_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_timetz(fcinfo, 0);
    let base = arg_timetz(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    let sub = arg_bool(fcinfo, 3);
    let less = arg_bool(fcinfo, 4);
    match crate::in_range::in_range_timetz_interval(&val, &base, &offset, sub, less) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}
fn fc_overlaps_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ts1 = nullable_timetz(fcinfo, 0);
    let te1 = nullable_timetz(fcinfo, 1);
    let ts2 = nullable_timetz(fcinfo, 2);
    let te2 = nullable_timetz(fcinfo, 3);
    match crate::overlaps::overlaps_timetz(ts1, te1, ts2, te2) {
        Some(b) => ret_bool(b),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}
fn fc_timetz_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let t = arg_timetz(fcinfo, 0);
    ret_i64(crate::timetz::timetz_time(&t))
}
fn fc_time_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let r = crate::date::time_timetz(arg_i64(fcinfo, 0));
    ret_timetz(fcinfo, &r)
}
fn fc_timestamptz_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamptz_timetz(arg_i64(fcinfo, 0)) {
        Ok(Some(t)) => ret_timetz(fcinfo, &t),
        Ok(None) => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
        Err(e) => raise(e),
    }
}
fn fc_datetimetz_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let d = arg_i32(fcinfo, 0);
    let t = arg_timetz(fcinfo, 1);
    match crate::timestamp::datetimetz_timestamptz(d, &t) {
        Ok(ts) => ret_i64(ts),
        Err(e) => raise(e),
    }
}
fn fc_timetz_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_timetz_part(fcinfo, false)
}
fn fc_extract_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_timetz_part(fcinfo, true)
}
fn ret_timetz_part(fcinfo: &mut FunctionCallInfoBaseData, retnumeric: bool) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    let t = arg_timetz(fcinfo, 1);
    let m = scratch_mcx();
    let r = match crate::timetz::timetz_part_common(m.mcx(), &units, &t, retnumeric) {
        Ok(r) => r,
        Err(e) => raise(e),
    };
    ret_timetz_part_result(fcinfo, r)
}
fn ret_timetz_part_result(
    fcinfo: &mut FunctionCallInfoBaseData,
    r: crate::timetz::TimetzPartResult<'_>,
) -> Datum {
    use crate::timetz::TimetzPartResult::*;
    match r {
        Float(f) => ret_f64(f),
        // EXTRACT (retnumeric) integer-field path: C returns
        // `int64_to_numeric(intresult)` (date.c:3102), a numeric varlena.
        Int(i) => ret_int64_numeric(fcinfo, i),
        Numeric(var) => {
            let bytes = numericvar_to_bytes(&var);
            ret_varlena(fcinfo, bytes)
        }
    }
}
fn fc_timetz_zone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = arg_text(fcinfo, 0);
    let t = arg_timetz(fcinfo, 1);
    match crate::timetz::timetz_zone(zone, &t) {
        Ok(r) => ret_timetz(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_timetz_izone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = arg_interval(fcinfo, 0);
    let t = arg_timetz(fcinfo, 1);
    match crate::timetz::timetz_izone(&zone, &t) {
        Ok(r) => ret_timetz(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_timetz_at_local(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: timetz_at_local(time) = timetz_zone(pg_get_timezone_name(session), time).
    let t = arg_timetz(fcinfo, 0);
    let stz = state_pgtz::session_timezone();
    let tzn = backend_timezone_localtime::pg_get_timezone_name(&stz).to_string();
    match crate::timetz::timetz_zone(&tzn, &t) {
        Ok(r) => ret_timetz(fcinfo, &r),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// timestamp.c builtins.
// ===========================================================================

fn fc_timestamp_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 2);
    match crate::timestamp::timestamp_in(s, typmod) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamp_out(arg_i64(fcinfo, 0)) {
        Ok(s) => ret_cstring(fcinfo, s),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_i32(fcinfo, 2);
    let bytes = arg_varlena(fcinfo, 0);
    let mut r = WireReader::new(bytes);
    match crate::binio::timestamp_recv(&mut r, typmod) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_varlena(fcinfo, crate::binio::timestamp_send(arg_i64(fcinfo, 0)))
}
fn fc_timestamp_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut t = arg_i64(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 1);
    match crate::timestamp::AdjustTimestampForTypmod(&mut t, typmod) {
        Ok(()) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 2);
    match crate::timestamp::timestamptz_in(s, typmod) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_make_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::make_timestamp(
        arg_i32(fcinfo, 0),
        arg_i32(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        arg_i32(fcinfo, 4),
        arg_f64(fcinfo, 5),
    ) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_make_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::make_timestamptz(
        arg_i32(fcinfo, 0),
        arg_i32(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        arg_i32(fcinfo, 4),
        arg_f64(fcinfo, 5),
    ) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_make_timestamptz_at_timezone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = arg_text(fcinfo, 6);
    match crate::timestamp::make_timestamptz_at_timezone(
        arg_i32(fcinfo, 0),
        arg_i32(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        arg_i32(fcinfo, 4),
        arg_f64(fcinfo, 5),
        zone,
    ) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_float8_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::float8_timestamptz(arg_f64(fcinfo, 0)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamptz_out(arg_i64(fcinfo, 0)) {
        Ok(s) => ret_cstring(fcinfo, s),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_i32(fcinfo, 2);
    let bytes = arg_varlena(fcinfo, 0);
    let mut r = WireReader::new(bytes);
    match crate::binio::timestamptz_recv(&mut r, typmod) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_varlena(fcinfo, crate::binio::timestamptz_send(arg_i64(fcinfo, 0)))
}
fn fc_timestamptz_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut t = arg_i64(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 1);
    match crate::timestamp::AdjustTimestampForTypmod(&mut t, typmod) {
        Ok(()) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_interval_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 2);
    match crate::interval::interval_in(s, typmod) {
        Ok(iv) => ret_interval(fcinfo, &iv),
        Err(e) => raise(e),
    }
}
fn fc_interval_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    ret_cstring(fcinfo, crate::interval::interval_out(&iv))
}
fn fc_interval_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_i32(fcinfo, 2);
    let bytes = arg_varlena(fcinfo, 0);
    let mut r = WireReader::new(bytes);
    match crate::binio::interval_recv(&mut r, typmod) {
        Ok(iv) => ret_interval(fcinfo, &iv),
        Err(e) => raise(e),
    }
}
fn fc_interval_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    ret_varlena(fcinfo, crate::binio::interval_send(&iv))
}
fn fc_interval_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut iv = arg_interval(fcinfo, 0);
    let typmod = arg_i32(fcinfo, 1);
    match crate::interval::AdjustIntervalForTypmod(&mut iv, typmod) {
        Ok(()) => ret_interval(fcinfo, &iv),
        Err(e) => raise(e),
    }
}
fn fc_make_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::make_interval(
        arg_i32(fcinfo, 0),
        arg_i32(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_i32(fcinfo, 3),
        arg_i32(fcinfo, 4),
        arg_i32(fcinfo, 5),
        arg_f64(fcinfo, 6),
    ) {
        Ok(iv) => ret_interval(fcinfo, &iv),
        Err(e) => raise(e),
    }
}

// now-family.
fn fc_now(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _ = fcinfo;
    ret_i64(crate::current::now())
}
fn fc_transaction_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _ = fcinfo;
    ret_i64(crate::current::transaction_timestamp())
}
fn fc_statement_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _ = fcinfo;
    ret_i64(crate::current::statement_timestamp())
}
fn fc_clock_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _ = fcinfo;
    ret_i64(crate::current::clock_timestamp())
}
fn fc_timeofday(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // returns text.
    let s = crate::current::timeofday();
    ret_varlena(fcinfo, s.into_bytes())
}

fn fc_timestamp_finite(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(!crate::timestamp::TIMESTAMP_NOT_FINITE(arg_i64(fcinfo, 0)))
}
fn fc_interval_finite(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    ret_bool(!crate::interval::INTERVAL_NOT_FINITE(&iv))
}

// timestamp(tz) comparison/min/max/diff (the bare timestamp_* core works on
// both since the representation is identical i64; the timestamptz variants in
// pg_proc map to the SAME C function).
fn fc_timestamp_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::timestamp::timestamp_eq(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::timestamp::timestamp_ne(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::timestamp::timestamp_lt(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::timestamp::timestamp_gt(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::timestamp::timestamp_le(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::timestamp::timestamp_ge(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::timestamp::timestamp_cmp(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(crate::hash::timestamp_hash(arg_i64(fcinfo, 0)))
}
fn fc_timestamp_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u64(crate::hash::timestamp_hash_extended(arg_i64(fcinfo, 0), arg_u64(fcinfo, 1)))
}
fn fc_timestamptz_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(crate::hash::timestamptz_hash(arg_i64(fcinfo, 0)))
}
fn fc_timestamptz_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u64(crate::hash::timestamptz_hash_extended(arg_i64(fcinfo, 0), arg_u64(fcinfo, 1)))
}
fn fc_timestamp_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i64(crate::timestamp::timestamp_smaller(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i64(crate::timestamp::timestamp_larger(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamp_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamp_mi(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)) {
        Ok(iv) => ret_interval(fcinfo, &iv),
        Err(e) => raise(e),
    }
}

// timestamp vs timestamptz cross-type comparisons (via the internal core).
macro_rules! ts_tstz_cmp {
    ($fn:ident, $op:tt, $swap:expr) => {
        fn $fn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            // C compares a timestamp against a timestamptz via
            // timestamp_cmp_timestamptz_internal; the *_timestamptz row has the
            // timestamp first, the timestamptz_* row has the timestamptz first.
            let (tsval, tstz) = if $swap {
                (arg_i64(fcinfo, 1), arg_i64(fcinfo, 0))
            } else {
                (arg_i64(fcinfo, 0), arg_i64(fcinfo, 1))
            };
            let c = crate::timestamp::timestamp_cmp_timestamptz_internal(tsval, tstz);
            let c = if $swap { -c } else { c };
            ret_bool(c $op 0)
        }
    };
}
ts_tstz_cmp!(fc_timestamp_eq_timestamptz, ==, false);
ts_tstz_cmp!(fc_timestamp_ne_timestamptz, !=, false);
ts_tstz_cmp!(fc_timestamp_lt_timestamptz, <, false);
ts_tstz_cmp!(fc_timestamp_gt_timestamptz, >, false);
ts_tstz_cmp!(fc_timestamp_le_timestamptz, <=, false);
ts_tstz_cmp!(fc_timestamp_ge_timestamptz, >=, false);
ts_tstz_cmp!(fc_timestamptz_eq_timestamp, ==, true);
ts_tstz_cmp!(fc_timestamptz_ne_timestamp, !=, true);
ts_tstz_cmp!(fc_timestamptz_lt_timestamp, <, true);
ts_tstz_cmp!(fc_timestamptz_gt_timestamp, >, true);
ts_tstz_cmp!(fc_timestamptz_le_timestamp, <=, true);
ts_tstz_cmp!(fc_timestamptz_ge_timestamp, >=, true);
fn fc_timestamp_cmp_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::timestamp::timestamp_cmp_timestamptz_internal(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_timestamptz_cmp_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(-crate::timestamp::timestamp_cmp_timestamptz_internal(arg_i64(fcinfo, 1), arg_i64(fcinfo, 0)))
}

// interval comparisons / hash.
macro_rules! interval_cmp_bool {
    ($fn:ident, $core:path) => {
        fn $fn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_interval(fcinfo, 0);
            let b = arg_interval(fcinfo, 1);
            ret_bool($core(&a, &b))
        }
    };
}
interval_cmp_bool!(fc_interval_eq, crate::interval::interval_eq);
interval_cmp_bool!(fc_interval_ne, crate::interval::interval_ne);
interval_cmp_bool!(fc_interval_lt, crate::interval::interval_lt);
interval_cmp_bool!(fc_interval_gt, crate::interval::interval_gt);
interval_cmp_bool!(fc_interval_le, crate::interval::interval_le);
interval_cmp_bool!(fc_interval_ge, crate::interval::interval_ge);
fn fc_interval_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    let b = arg_interval(fcinfo, 1);
    ret_i32(crate::interval::interval_cmp(&a, &b))
}
fn fc_interval_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    ret_u32(crate::hash::interval_hash(&a))
}
fn fc_interval_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    ret_u64(crate::hash::interval_hash_extended(&a, arg_u64(fcinfo, 1)))
}

fn fc_overlaps_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ts1 = nullable_i64(fcinfo, 0);
    let te1 = nullable_i64(fcinfo, 1);
    let ts2 = nullable_i64(fcinfo, 2);
    let te2 = nullable_i64(fcinfo, 3);
    match crate::overlaps::overlaps_timestamp(ts1, te1, ts2, te2) {
        Some(b) => ret_bool(b),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

// interval justify / arithmetic.
fn fc_interval_justify_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    match crate::interval::interval_justify_interval(&iv) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_interval_justify_hours(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    match crate::interval::interval_justify_hours(&iv) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_interval_justify_days(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    match crate::interval::interval_justify_days(&iv) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    match crate::timestamp::timestamp_pl_interval(arg_i64(fcinfo, 0), &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    match crate::timestamp::timestamp_mi_interval(arg_i64(fcinfo, 0), &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    match crate::timestamp::timestamptz_pl_interval(arg_i64(fcinfo, 0), &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    match crate::timestamp::timestamptz_mi_interval(arg_i64(fcinfo, 0), &iv) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_pl_interval_at_zone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    let zone = arg_text(fcinfo, 2);
    match crate::timestamp::timestamptz_pl_interval_at_zone(arg_i64(fcinfo, 0), &iv, zone) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_mi_interval_at_zone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 1);
    let zone = arg_text(fcinfo, 2);
    match crate::timestamp::timestamptz_mi_interval_at_zone(arg_i64(fcinfo, 0), &iv, zone) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_interval_um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let iv = arg_interval(fcinfo, 0);
    match crate::interval::interval_um(&iv) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_interval_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    let b = arg_interval(fcinfo, 1);
    let r = crate::interval::interval_smaller(a, b);
    ret_interval(fcinfo, &r)
}
fn fc_interval_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    let b = arg_interval(fcinfo, 1);
    let r = crate::interval::interval_larger(a, b);
    ret_interval(fcinfo, &r)
}
fn fc_interval_pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    let b = arg_interval(fcinfo, 1);
    match crate::interval::interval_pl(&a, &b) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_interval_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    let b = arg_interval(fcinfo, 1);
    match crate::interval::interval_mi(&a, &b) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_interval_mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    let f = arg_f64(fcinfo, 1);
    match crate::interval::interval_mul(&a, f) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_mul_d_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C mul_d_interval(float8, interval) = interval_mul(interval, float8).
    let f = arg_f64(fcinfo, 0);
    let a = arg_interval(fcinfo, 1);
    match crate::interval::interval_mul(&a, f) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_interval_div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_interval(fcinfo, 0);
    let f = arg_f64(fcinfo, 1);
    match crate::interval::interval_div(&a, f) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_in_range_timestamptz_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_i64(fcinfo, 0);
    let base = arg_i64(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    let sub = arg_bool(fcinfo, 3);
    let less = arg_bool(fcinfo, 4);
    match crate::in_range::in_range_timestamptz_interval(val, base, &offset, sub, less) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}
fn fc_in_range_timestamp_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_i64(fcinfo, 0);
    let base = arg_i64(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    let sub = arg_bool(fcinfo, 3);
    let less = arg_bool(fcinfo, 4);
    match crate::in_range::in_range_timestamp_interval(val, base, &offset, sub, less) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}
fn fc_in_range_interval_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_interval(fcinfo, 0);
    let base = arg_interval(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    let sub = arg_bool(fcinfo, 3);
    let less = arg_bool(fcinfo, 4);
    match crate::in_range::in_range_interval_interval(&val, &base, &offset, sub, less) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_age(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamp_age(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)) {
        Ok(iv) => ret_interval(fcinfo, &iv),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_age(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamptz_age(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)) {
        Ok(iv) => ret_interval(fcinfo, &iv),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_bin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let stride = arg_interval(fcinfo, 0);
    let ts = arg_i64(fcinfo, 1);
    let origin = arg_i64(fcinfo, 2);
    match crate::timestamp::timestamp_bin(&stride, ts, origin) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_bin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let stride = arg_interval(fcinfo, 0);
    let ts = arg_i64(fcinfo, 1);
    let origin = arg_i64(fcinfo, 2);
    match crate::timestamp::timestamptz_bin(&stride, ts, origin) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    match crate::extract::timestamp_trunc(&units, arg_i64(fcinfo, 1)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    match crate::timestamp::timestamptz_trunc(&units, arg_i64(fcinfo, 1)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_trunc_zone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    let ts = arg_i64(fcinfo, 1);
    let zone = arg_text(fcinfo, 2);
    match crate::timestamp::timestamptz_trunc_zone(&units, ts, zone) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_interval_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    let iv = arg_interval(fcinfo, 1);
    match crate::extract::interval_trunc(&units, &iv) {
        Ok(r) => ret_interval(fcinfo, &r),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_timestamp_part(fcinfo, false)
}
fn fc_extract_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_timestamp_part(fcinfo, true)
}
fn ret_timestamp_part(fcinfo: &mut FunctionCallInfoBaseData, retnumeric: bool) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    let ts = arg_i64(fcinfo, 1);
    let m = scratch_mcx();
    let r = match crate::extract::timestamp_part(m.mcx(), ts, &units, retnumeric) {
        Ok(r) => r,
        Err(e) => raise(e),
    };
    ret_extract(fcinfo, r, retnumeric)
}
fn fc_timestamptz_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_timestamptz_part(fcinfo, false)
}
fn fc_extract_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_timestamptz_part(fcinfo, true)
}
fn ret_timestamptz_part(fcinfo: &mut FunctionCallInfoBaseData, retnumeric: bool) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    let ts = arg_i64(fcinfo, 1);
    let m = scratch_mcx();
    let r = match crate::extract::timestamptz_part(m.mcx(), ts, &units, retnumeric) {
        Ok(r) => r,
        Err(e) => raise(e),
    };
    ret_extract(fcinfo, r, retnumeric)
}
fn fc_interval_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_interval_part(fcinfo, false)
}
fn fc_extract_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_interval_part(fcinfo, true)
}
fn ret_interval_part(fcinfo: &mut FunctionCallInfoBaseData, retnumeric: bool) -> Datum {
    let units = arg_text(fcinfo, 0).to_lowercase();
    let iv = arg_interval(fcinfo, 1);
    let m = scratch_mcx();
    let r = match crate::extract::interval_part(m.mcx(), &iv, &units, retnumeric) {
        Ok(r) => r,
        Err(e) => raise(e),
    };
    ret_extract(fcinfo, r, retnumeric)
}
fn fc_timestamp_zone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = arg_text(fcinfo, 0);
    match crate::timestamp::timestamp_zone(zone, arg_i64(fcinfo, 1)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_izone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = arg_interval(fcinfo, 0);
    match crate::timestamp::timestamp_izone(&zone, arg_i64(fcinfo, 1)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamp2timestamptz(arg_i64(fcinfo, 0)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamptz2timestamp(arg_i64(fcinfo, 0)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_zone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = arg_text(fcinfo, 0);
    match crate::timestamp::timestamptz_zone(zone, arg_i64(fcinfo, 1)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_izone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = arg_interval(fcinfo, 0);
    match crate::timestamp::timestamptz_izone(&zone, arg_i64(fcinfo, 1)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamp_at_local(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // timezone(timestamp) AT LOCAL == timestamp_timestamptz (treat the local
    // wall-clock timestamp as being in the session zone).
    match crate::timestamp::timestamp2timestamptz(arg_i64(fcinfo, 0)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}
fn fc_timestamptz_at_local(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::timestamp::timestamptz_at_local(arg_i64(fcinfo, 0)) {
        Ok(t) => ret_i64(t),
        Err(e) => raise(e),
    }
}

// --- nullable arg helpers (overlaps is not strict) -------------------------

fn nullable_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Option<i64> {
    let a = fcinfo.arg(i)?;
    if a.isnull {
        None
    } else {
        Some(a.value.as_i64())
    }
}
fn nullable_timetz(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Option<TimeTzADT> {
    let a = fcinfo.arg(i)?;
    if a.isnull {
        return None;
    }
    fcinfo.ref_arg(i).and_then(|p| p.as_varlena()).map(timetz_from_bytes)
}

// ===========================================================================
// Registration.
// ===========================================================================

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> types_fmgr::BuiltinFunction {
    types_fmgr::BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register every date.c / timestamp.c / datetime.c builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
///
/// Genuinely-blocked functions NOT registered here (each needs an unported
/// neighbor's infrastructure, and registering a panicking shim would be a
/// misleading stub — they are filled when the owner lands):
///  * the *typmodin/*typmodout pairs (timetypmodin/out, timetztypmodin/out,
///    timestamptypmodin/out, timestamptztypmodin/out, intervaltypmodin/out):
///    need the `cstring[]` array argument decode (ArrayType → Vec<i32>) which
///    is owned by arrayfuncs' deconstruct_array — the core
///    `anytime_typmodin`/`intervaltypmodin` take `&[i32]`, but the boundary has
///    no cstring[] → &[i32] marshaling yet.
///  * the planner-support functions (date_sortsupport, date_skipsupport,
///    time_support, timestamp_support, timestamp_sortsupport,
///    timestamp_skipsupport, interval_support, generate_series_timestamp_support):
///    take an `internal` SupportRequest* node owned by the planner/sortsupport
///    subsystems (SortSupport / SupportRequestSimplify), not yet modeled at the
///    fmgr boundary.
///  * the interval AVG aggregate family (interval_avg_accum/_combine/_serialize/
///    _deserialize/_accum_inv/_avg/_sum): operate on an `internal`
///    IntervalAggState transition value across an aggregate's lifecycle, owned
///    by nodeAgg's transition-value lane.
///  * the set-returning functions (generate_series_timestamp/timestamptz/
///    _at_zone): need the SRF FuncCallContext protocol (funcapi), not the
///    one-shot fmgr boundary.
///  * pg_timezone_abbrevs_zone/_abbrevs/pg_timezone_names: SRFs over the
///    timezone catalog, same SRF-protocol gap.
///  * pg_postmaster_start_time/pg_conf_load_time: read postmaster globals owned
///    by the postmaster subsystem (PgStartTime / PgReloadTime), not this crate.
pub fn register_datetime_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- date.c: DATE ----
        builtin(1084, "date_in", 1, true, false, fc_date_in),
        builtin(1085, "date_out", 1, true, false, fc_date_out),
        builtin(2468, "date_recv", 1, true, false, fc_date_recv),
        builtin(2469, "date_send", 1, true, false, fc_date_send),
        builtin(3846, "make_date", 3, true, false, fc_make_date),
        builtin(1086, "date_eq", 2, true, false, fc_date_eq),
        builtin(1091, "date_ne", 2, true, false, fc_date_ne),
        builtin(1087, "date_lt", 2, true, false, fc_date_lt),
        builtin(1088, "date_le", 2, true, false, fc_date_le),
        builtin(1089, "date_gt", 2, true, false, fc_date_gt),
        builtin(1090, "date_ge", 2, true, false, fc_date_ge),
        builtin(1092, "date_cmp", 2, true, false, fc_date_cmp),
        builtin(6415, "hashdate", 1, true, false, fc_hashdate),
        builtin(6416, "hashdateextended", 2, true, false, fc_hashdateextended),
        builtin(1373, "date_finite", 1, true, false, fc_date_finite),
        builtin(1138, "date_larger", 2, true, false, fc_date_larger),
        builtin(1139, "date_smaller", 2, true, false, fc_date_smaller),
        builtin(1140, "date_mi", 2, true, false, fc_date_mi),
        builtin(1141, "date_pli", 2, true, false, fc_date_pli),
        builtin(1142, "date_mii", 2, true, false, fc_date_mii),
        builtin(2340, "date_eq_timestamp", 2, true, false, fc_date_eq_timestamp),
        builtin(2343, "date_ne_timestamp", 2, true, false, fc_date_ne_timestamp),
        builtin(2338, "date_lt_timestamp", 2, true, false, fc_date_lt_timestamp),
        builtin(2341, "date_gt_timestamp", 2, true, false, fc_date_gt_timestamp),
        builtin(2339, "date_le_timestamp", 2, true, false, fc_date_le_timestamp),
        builtin(2342, "date_ge_timestamp", 2, true, false, fc_date_ge_timestamp),
        builtin(2344, "date_cmp_timestamp", 2, true, false, fc_date_cmp_timestamp),
        builtin(2353, "date_eq_timestamptz", 2, true, false, fc_date_eq_timestamptz),
        builtin(2356, "date_ne_timestamptz", 2, true, false, fc_date_ne_timestamptz),
        builtin(2351, "date_lt_timestamptz", 2, true, false, fc_date_lt_timestamptz),
        builtin(2354, "date_gt_timestamptz", 2, true, false, fc_date_gt_timestamptz),
        builtin(2352, "date_le_timestamptz", 2, true, false, fc_date_le_timestamptz),
        builtin(2355, "date_ge_timestamptz", 2, true, false, fc_date_ge_timestamptz),
        builtin(2357, "date_cmp_timestamptz", 2, true, false, fc_date_cmp_timestamptz),
        builtin(2366, "timestamp_eq_date", 2, true, false, fc_timestamp_eq_date),
        builtin(2369, "timestamp_ne_date", 2, true, false, fc_timestamp_ne_date),
        builtin(2364, "timestamp_lt_date", 2, true, false, fc_timestamp_lt_date),
        builtin(2367, "timestamp_gt_date", 2, true, false, fc_timestamp_gt_date),
        builtin(2365, "timestamp_le_date", 2, true, false, fc_timestamp_le_date),
        builtin(2368, "timestamp_ge_date", 2, true, false, fc_timestamp_ge_date),
        builtin(2370, "timestamp_cmp_date", 2, true, false, fc_timestamp_cmp_date),
        builtin(2379, "timestamptz_eq_date", 2, true, false, fc_timestamptz_eq_date),
        builtin(2382, "timestamptz_ne_date", 2, true, false, fc_timestamptz_ne_date),
        builtin(2377, "timestamptz_lt_date", 2, true, false, fc_timestamptz_lt_date),
        builtin(2380, "timestamptz_gt_date", 2, true, false, fc_timestamptz_gt_date),
        builtin(2378, "timestamptz_le_date", 2, true, false, fc_timestamptz_le_date),
        builtin(2381, "timestamptz_ge_date", 2, true, false, fc_timestamptz_ge_date),
        builtin(2383, "timestamptz_cmp_date", 2, true, false, fc_timestamptz_cmp_date),
        builtin(4133, "in_range_date_interval", 5, true, false, fc_in_range_date_interval),
        builtin(6199, "extract_date", 2, true, false, fc_extract_date),
        builtin(2071, "date_pl_interval", 2, true, false, fc_date_pl_interval),
        builtin(2072, "date_mi_interval", 2, true, false, fc_date_mi_interval),
        builtin(2024, "date_timestamp", 1, true, false, fc_date_timestamp),
        builtin(2029, "timestamp_date", 1, true, false, fc_timestamp_date),
        builtin(1174, "date_timestamptz", 1, true, false, fc_date_timestamptz),
        builtin(1178, "timestamptz_date", 1, true, false, fc_timestamptz_date),
        // ---- date.c: TIME ----
        builtin(1143, "time_in", 3, true, false, fc_time_in),
        builtin(1144, "time_out", 1, true, false, fc_time_out),
        builtin(2470, "time_recv", 3, true, false, fc_time_recv),
        builtin(2471, "time_send", 1, true, false, fc_time_send),
        builtin(3847, "make_time", 3, true, false, fc_make_time),
        builtin(1968, "time_scale", 2, true, false, fc_time_scale),
        builtin(1145, "time_eq", 2, true, false, fc_time_eq),
        builtin(1106, "time_ne", 2, true, false, fc_time_ne),
        builtin(1102, "time_lt", 2, true, false, fc_time_lt),
        builtin(1103, "time_le", 2, true, false, fc_time_le),
        builtin(1104, "time_gt", 2, true, false, fc_time_gt),
        builtin(1105, "time_ge", 2, true, false, fc_time_ge),
        builtin(1107, "time_cmp", 2, true, false, fc_time_cmp),
        builtin(1688, "time_hash", 1, true, false, fc_time_hash),
        builtin(3409, "time_hash_extended", 2, true, false, fc_time_hash_extended),
        builtin(1377, "time_larger", 2, true, false, fc_time_larger),
        builtin(1378, "time_smaller", 2, true, false, fc_time_smaller),
        builtin(1308, "overlaps_time", 4, false, false, fc_overlaps_time),
        builtin(1316, "timestamp_time", 1, true, false, fc_timestamp_time),
        builtin(2019, "timestamptz_time", 1, true, false, fc_timestamptz_time),
        builtin(2025, "datetime_timestamp", 2, true, false, fc_datetime_timestamp),
        builtin(1370, "time_interval", 1, true, false, fc_time_interval),
        builtin(1419, "interval_time", 1, true, false, fc_interval_time),
        builtin(1690, "time_mi_time", 2, true, false, fc_time_mi_time),
        builtin(1747, "time_pl_interval", 2, true, false, fc_time_pl_interval),
        builtin(1748, "time_mi_interval", 2, true, false, fc_time_mi_interval),
        builtin(4137, "in_range_time_interval", 5, true, false, fc_in_range_time_interval),
        builtin(1385, "time_part", 2, true, false, fc_time_part),
        builtin(6200, "extract_time", 2, true, false, fc_extract_time),
        // ---- date.c: TIMETZ ----
        builtin(1350, "timetz_in", 3, true, false, fc_timetz_in),
        builtin(1351, "timetz_out", 1, true, false, fc_timetz_out),
        builtin(2472, "timetz_recv", 3, true, false, fc_timetz_recv),
        builtin(2473, "timetz_send", 1, true, false, fc_timetz_send),
        builtin(1969, "timetz_scale", 2, true, false, fc_timetz_scale),
        builtin(1352, "timetz_eq", 2, true, false, fc_timetz_eq),
        builtin(1353, "timetz_ne", 2, true, false, fc_timetz_ne),
        builtin(1354, "timetz_lt", 2, true, false, fc_timetz_lt),
        builtin(1355, "timetz_le", 2, true, false, fc_timetz_le),
        builtin(1357, "timetz_gt", 2, true, false, fc_timetz_gt),
        builtin(1356, "timetz_ge", 2, true, false, fc_timetz_ge),
        builtin(1358, "timetz_cmp", 2, true, false, fc_timetz_cmp),
        builtin(1696, "timetz_hash", 1, true, false, fc_timetz_hash),
        builtin(3410, "timetz_hash_extended", 2, true, false, fc_timetz_hash_extended),
        builtin(1379, "timetz_larger", 2, true, false, fc_timetz_larger),
        builtin(1380, "timetz_smaller", 2, true, false, fc_timetz_smaller),
        builtin(1749, "timetz_pl_interval", 2, true, false, fc_timetz_pl_interval),
        builtin(1750, "timetz_mi_interval", 2, true, false, fc_timetz_mi_interval),
        builtin(4138, "in_range_timetz_interval", 5, true, false, fc_in_range_timetz_interval),
        builtin(1271, "overlaps_timetz", 4, false, false, fc_overlaps_timetz),
        builtin(2046, "timetz_time", 1, true, false, fc_timetz_time),
        builtin(2047, "time_timetz", 1, true, false, fc_time_timetz),
        builtin(1388, "timestamptz_timetz", 1, true, false, fc_timestamptz_timetz),
        builtin(1359, "datetimetz_timestamptz", 2, true, false, fc_datetimetz_timestamptz),
        builtin(1273, "timetz_part", 2, true, false, fc_timetz_part),
        builtin(6201, "extract_timetz", 2, true, false, fc_extract_timetz),
        builtin(2037, "timetz_zone", 2, true, false, fc_timetz_zone),
        builtin(2038, "timetz_izone", 2, true, false, fc_timetz_izone),
        builtin(6336, "timetz_at_local", 1, true, false, fc_timetz_at_local),
        // ---- timestamp.c: TIMESTAMP / TIMESTAMPTZ ----
        builtin(1312, "timestamp_in", 3, true, false, fc_timestamp_in),
        builtin(1313, "timestamp_out", 1, true, false, fc_timestamp_out),
        builtin(2474, "timestamp_recv", 3, true, false, fc_timestamp_recv),
        builtin(2475, "timestamp_send", 1, true, false, fc_timestamp_send),
        builtin(1961, "timestamp_scale", 2, true, false, fc_timestamp_scale),
        builtin(1150, "timestamptz_in", 3, true, false, fc_timestamptz_in),
        builtin(3461, "make_timestamp", 6, true, false, fc_make_timestamp),
        builtin(3462, "make_timestamptz", 6, true, false, fc_make_timestamptz),
        builtin(3463, "make_timestamptz_at_timezone", 7, true, false, fc_make_timestamptz_at_timezone),
        builtin(1158, "float8_timestamptz", 1, true, false, fc_float8_timestamptz),
        builtin(1151, "timestamptz_out", 1, true, false, fc_timestamptz_out),
        builtin(2476, "timestamptz_recv", 3, true, false, fc_timestamptz_recv),
        builtin(2477, "timestamptz_send", 1, true, false, fc_timestamptz_send),
        builtin(1967, "timestamptz_scale", 2, true, false, fc_timestamptz_scale),
        // ---- timestamp.c: INTERVAL ----
        builtin(1160, "interval_in", 3, true, false, fc_interval_in),
        builtin(1161, "interval_out", 1, true, false, fc_interval_out),
        builtin(2478, "interval_recv", 3, true, false, fc_interval_recv),
        builtin(2479, "interval_send", 1, true, false, fc_interval_send),
        builtin(1200, "interval_scale", 2, true, false, fc_interval_scale),
        builtin(3464, "make_interval", 7, true, false, fc_make_interval),
        // ---- timestamp.c: now-family ----
        builtin(1299, "now", 0, true, false, fc_now),
        builtin(2647, "now", 0, true, false, fc_transaction_timestamp),
        builtin(2648, "statement_timestamp", 0, true, false, fc_statement_timestamp),
        builtin(2649, "clock_timestamp", 0, true, false, fc_clock_timestamp),
        builtin(274, "timeofday", 0, true, false, fc_timeofday),
        // ---- timestamp.c: finite ----
        builtin(1389, "timestamp_finite", 1, true, false, fc_timestamp_finite),
        builtin(2048, "timestamp_finite", 1, true, false, fc_timestamp_finite),
        builtin(1390, "interval_finite", 1, true, false, fc_interval_finite),
        // ---- timestamp.c: comparisons (1152-row = timestamptz, 205x = timestamp; same core) ----
        builtin(1152, "timestamp_eq", 2, true, false, fc_timestamp_eq),
        builtin(2052, "timestamp_eq", 2, true, false, fc_timestamp_eq),
        builtin(1153, "timestamp_ne", 2, true, false, fc_timestamp_ne),
        builtin(2053, "timestamp_ne", 2, true, false, fc_timestamp_ne),
        builtin(1154, "timestamp_lt", 2, true, false, fc_timestamp_lt),
        builtin(2054, "timestamp_lt", 2, true, false, fc_timestamp_lt),
        builtin(1157, "timestamp_gt", 2, true, false, fc_timestamp_gt),
        builtin(2057, "timestamp_gt", 2, true, false, fc_timestamp_gt),
        builtin(1155, "timestamp_le", 2, true, false, fc_timestamp_le),
        builtin(2055, "timestamp_le", 2, true, false, fc_timestamp_le),
        builtin(1156, "timestamp_ge", 2, true, false, fc_timestamp_ge),
        builtin(2056, "timestamp_ge", 2, true, false, fc_timestamp_ge),
        builtin(1314, "timestamp_cmp", 2, true, false, fc_timestamp_cmp),
        builtin(2045, "timestamp_cmp", 2, true, false, fc_timestamp_cmp),
        builtin(2039, "timestamp_hash", 1, true, false, fc_timestamp_hash),
        builtin(3411, "timestamp_hash_extended", 2, true, false, fc_timestamp_hash_extended),
        builtin(6425, "timestamptz_hash", 1, true, false, fc_timestamptz_hash),
        builtin(6426, "timestamptz_hash_extended", 2, true, false, fc_timestamptz_hash_extended),
        builtin(2522, "timestamp_eq_timestamptz", 2, true, false, fc_timestamp_eq_timestamptz),
        builtin(2525, "timestamp_ne_timestamptz", 2, true, false, fc_timestamp_ne_timestamptz),
        builtin(2520, "timestamp_lt_timestamptz", 2, true, false, fc_timestamp_lt_timestamptz),
        builtin(2523, "timestamp_gt_timestamptz", 2, true, false, fc_timestamp_gt_timestamptz),
        builtin(2521, "timestamp_le_timestamptz", 2, true, false, fc_timestamp_le_timestamptz),
        builtin(2524, "timestamp_ge_timestamptz", 2, true, false, fc_timestamp_ge_timestamptz),
        builtin(2526, "timestamp_cmp_timestamptz", 2, true, false, fc_timestamp_cmp_timestamptz),
        builtin(2529, "timestamptz_eq_timestamp", 2, true, false, fc_timestamptz_eq_timestamp),
        builtin(2532, "timestamptz_ne_timestamp", 2, true, false, fc_timestamptz_ne_timestamp),
        builtin(2527, "timestamptz_lt_timestamp", 2, true, false, fc_timestamptz_lt_timestamp),
        builtin(2530, "timestamptz_gt_timestamp", 2, true, false, fc_timestamptz_gt_timestamp),
        builtin(2528, "timestamptz_le_timestamp", 2, true, false, fc_timestamptz_le_timestamp),
        builtin(2531, "timestamptz_ge_timestamp", 2, true, false, fc_timestamptz_ge_timestamp),
        builtin(2533, "timestamptz_cmp_timestamp", 2, true, false, fc_timestamptz_cmp_timestamp),
        // ---- timestamp.c: interval comparisons ----
        builtin(1162, "interval_eq", 2, true, false, fc_interval_eq),
        builtin(1163, "interval_ne", 2, true, false, fc_interval_ne),
        builtin(1164, "interval_lt", 2, true, false, fc_interval_lt),
        builtin(1167, "interval_gt", 2, true, false, fc_interval_gt),
        builtin(1165, "interval_le", 2, true, false, fc_interval_le),
        builtin(1166, "interval_ge", 2, true, false, fc_interval_ge),
        builtin(1315, "interval_cmp", 2, true, false, fc_interval_cmp),
        builtin(1697, "interval_hash", 1, true, false, fc_interval_hash),
        builtin(3418, "interval_hash_extended", 2, true, false, fc_interval_hash_extended),
        // ---- timestamp.c: overlaps / min / max / mi ----
        builtin(1304, "overlaps_timestamp", 4, false, false, fc_overlaps_timestamp),
        builtin(2041, "overlaps_timestamp", 4, false, false, fc_overlaps_timestamp),
        builtin(1195, "timestamp_smaller", 2, true, false, fc_timestamp_smaller),
        builtin(2035, "timestamp_smaller", 2, true, false, fc_timestamp_smaller),
        builtin(1196, "timestamp_larger", 2, true, false, fc_timestamp_larger),
        builtin(2036, "timestamp_larger", 2, true, false, fc_timestamp_larger),
        builtin(1188, "timestamp_mi", 2, true, false, fc_timestamp_mi),
        builtin(2031, "timestamp_mi", 2, true, false, fc_timestamp_mi),
        // ---- timestamp.c: justify / arithmetic ----
        builtin(2711, "interval_justify_interval", 1, true, false, fc_interval_justify_interval),
        builtin(1175, "interval_justify_hours", 1, true, false, fc_interval_justify_hours),
        builtin(1295, "interval_justify_days", 1, true, false, fc_interval_justify_days),
        builtin(2032, "timestamp_pl_interval", 2, true, false, fc_timestamp_pl_interval),
        builtin(2033, "timestamp_mi_interval", 2, true, false, fc_timestamp_mi_interval),
        builtin(1189, "timestamptz_pl_interval", 2, true, false, fc_timestamptz_pl_interval),
        builtin(6221, "timestamptz_pl_interval", 2, true, false, fc_timestamptz_pl_interval),
        builtin(1190, "timestamptz_mi_interval", 2, true, false, fc_timestamptz_mi_interval),
        builtin(6223, "timestamptz_mi_interval", 2, true, false, fc_timestamptz_mi_interval),
        builtin(6222, "timestamptz_pl_interval_at_zone", 3, true, false, fc_timestamptz_pl_interval_at_zone),
        builtin(6273, "timestamptz_mi_interval_at_zone", 3, true, false, fc_timestamptz_mi_interval_at_zone),
        builtin(1168, "interval_um", 1, true, false, fc_interval_um),
        builtin(1197, "interval_smaller", 2, true, false, fc_interval_smaller),
        builtin(1198, "interval_larger", 2, true, false, fc_interval_larger),
        builtin(1169, "interval_pl", 2, true, false, fc_interval_pl),
        builtin(1170, "interval_mi", 2, true, false, fc_interval_mi),
        builtin(1618, "interval_mul", 2, true, false, fc_interval_mul),
        builtin(1624, "mul_d_interval", 2, true, false, fc_mul_d_interval),
        builtin(1326, "interval_div", 2, true, false, fc_interval_div),
        builtin(4135, "in_range_timestamptz_interval", 5, true, false, fc_in_range_timestamptz_interval),
        builtin(4134, "in_range_timestamp_interval", 5, true, false, fc_in_range_timestamp_interval),
        builtin(4136, "in_range_interval_interval", 5, true, false, fc_in_range_interval_interval),
        // ---- timestamp.c: age / bin / trunc / part / zone / conversions ----
        builtin(2058, "timestamp_age", 2, true, false, fc_timestamp_age),
        builtin(1199, "timestamptz_age", 2, true, false, fc_timestamptz_age),
        builtin(6177, "timestamp_bin", 3, true, false, fc_timestamp_bin),
        builtin(2020, "timestamp_trunc", 2, true, false, fc_timestamp_trunc),
        builtin(6178, "timestamptz_bin", 3, true, false, fc_timestamptz_bin),
        builtin(1217, "timestamptz_trunc", 2, true, false, fc_timestamptz_trunc),
        builtin(1284, "timestamptz_trunc_zone", 3, true, false, fc_timestamptz_trunc_zone),
        builtin(1218, "interval_trunc", 2, true, false, fc_interval_trunc),
        builtin(2021, "timestamp_part", 2, true, false, fc_timestamp_part),
        builtin(6202, "extract_timestamp", 2, true, false, fc_extract_timestamp),
        builtin(1171, "timestamptz_part", 2, true, false, fc_timestamptz_part),
        builtin(6203, "extract_timestamptz", 2, true, false, fc_extract_timestamptz),
        builtin(1172, "interval_part", 2, true, false, fc_interval_part),
        builtin(6204, "extract_interval", 2, true, false, fc_extract_interval),
        builtin(2069, "timestamp_zone", 2, true, false, fc_timestamp_zone),
        builtin(2070, "timestamp_izone", 2, true, false, fc_timestamp_izone),
        builtin(2028, "timestamp_timestamptz", 1, true, false, fc_timestamp_timestamptz),
        builtin(2027, "timestamptz_timestamp", 1, true, false, fc_timestamptz_timestamp),
        builtin(1159, "timestamptz_zone", 2, true, false, fc_timestamptz_zone),
        builtin(1026, "timestamptz_izone", 2, true, false, fc_timestamptz_izone),
        builtin(6335, "timestamp_at_local", 1, true, false, fc_timestamp_at_local),
        builtin(6334, "timestamptz_at_local", 1, true, false, fc_timestamptz_at_local),
    ]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_byte_round_trip() {
        let iv = Interval { time: 0x0102_0304_0506_0708, day: 42, month: -7 };
        let b = interval_to_bytes(&iv);
        assert_eq!(b.len(), 16);
        assert_eq!(interval_from_bytes(&b), iv);
    }

    #[test]
    fn timetz_byte_round_trip() {
        let t = TimeTzADT { time: 0x1122_3344_5566_7788, zone: -3600 };
        let b = timetz_to_bytes(&t);
        assert_eq!(b.len(), 12);
        assert_eq!(timetz_from_bytes(&b), t);
    }

    #[test]
    fn register_datetime_builtins_runs_once() {
        // The fmgr-core registry is a per-backend thread-local; registering the
        // datetime builtins must not panic (no duplicate-OID assertion inside).
        register_datetime_builtins();
    }
}
