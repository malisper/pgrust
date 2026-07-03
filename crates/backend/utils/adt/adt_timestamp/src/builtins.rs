//! fmgr wrappers (`fc_*`) + `TIMESTAMP_BUILTINS` for fmgr-core. Not
//! registrable (established precedents): the interval-typed rows (interval
//! unit unported: timestamp[tz]_izone 1026/2070, timestamp_mi 1188/2031, age
//! 1199/2058, timestamp[tz]_pl/mi_interval, interval_part/extract_interval,
//! interval_trunc, timestamp[tz]_bin 6177/6178), overlaps_timestamp
//! 1304/2041 (non-strict 4-arg frame stays with the interval lane),
//! generate_series_timestamp[tz] 938/939/6274 (SRF frame),
//! float8_timestamptz 1158 / timestamptz_float8 (float lane),
//! typmodin/typmodout 2905-2908 (ArrayType), timestamp_support /
//! timestamp_sortsupport 3137 / skipsupport (planner nodes), to_timestamp
//! 1778, pg_postmaster_start_time/pg_conf_load_time (backend globals).

use ::datum::{Datum, Varlena};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use adt_datetime::MAXDATELEN;

use crate::PartValue;

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the nameout/adt_date precedent). The Datum aliases it until the next out
// call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; MAXDATELEN + 1]> =
        const { core::cell::UnsafeCell::new([0; MAXDATELEN + 1]) };
}

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> std::borrow::Cow<'a, str> {
    // SAFETY: catalog arg 0 of the in-functions is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

pub fn fc_timestamp_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(crate::timestamp_in(&s, typmod, esc)?))
}

pub fn fc_timestamptz_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(crate::timestamptz_in(&s, typmod, esc)?))
}

pub fn fc_timestamp_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ts = fcinfo.arg_i64(0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::timestamp_out(ts, buf)?;
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_timestamptz_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ts = fcinfo.arg_i64(0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::timestamptz_out(ts, buf)?;
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_timestamp_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(crate::timestamp_recv(buf, typmod)?))
}

pub fn fc_timestamptz_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(crate::timestamptz_recv(buf, typmod)?))
}

pub fn fc_timestamp_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ts = fcinfo.arg_i64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::timestamp_send(mcx, ts)?))
}

pub fn fc_timestamp_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut result = fcinfo.arg_i64(0);
    crate::AdjustTimestampForTypmod(&mut result, fcinfo.arg_i32(1), None)?;
    Ok(Datum::from_i64(result))
}

pub fn fc_now(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(xact::GetCurrentTransactionStartTimestamp()))
}

pub fn fc_statement_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(xact::GetCurrentStatementStartTimestamp()))
}

pub fn fc_clock_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::GetCurrentTimestamp()))
}

pub fn fc_timeofday(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut buf = [0u8; 128];
    let len = crate::timeofday_into(&mut buf);
    let mcx = fcinfo.result_mcx();
    let mut image = ::mcx::vec_with_capacity_in(mcx, 4 + len)?;
    ::mcx::vec_append_bytes(&mut image, &[0u8; 4])?;
    ::mcx::vec_append_bytes(&mut image, &buf[..len])?;
    Ok(varlena_result(Varlena::from_image(image)))
}

macro_rules! ts_cmp_ops {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = fcinfo.arg_i64(0);
            let b = fcinfo.arg_i64(1);
            Ok(Datum::from_bool(a $op b))
        }
    )*};
}

ts_cmp_ops! {
    fc_timestamp_eq: ==;
    fc_timestamp_ne: !=;
    fc_timestamp_lt: <;
    fc_timestamp_le: <=;
    fc_timestamp_gt: >;
    fc_timestamp_ge: >=;
}

pub fn fc_timestamp_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::timestamp_cmp_internal(
        fcinfo.arg_i64(0),
        fcinfo.arg_i64(1),
    )))
}

pub fn fc_timestamp_finite(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!crate::TIMESTAMP_NOT_FINITE(fcinfo.arg_i64(0))))
}

pub fn fc_timestamp_smaller(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg_i64(0), fcinfo.arg_i64(1));
    Ok(Datum::from_i64(if a < b { a } else { b }))
}

pub fn fc_timestamp_larger(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg_i64(0), fcinfo.arg_i64(1));
    Ok(Datum::from_i64(if a > b { a } else { b }))
}

// hashfunc.c hashint8's fold of int64 to a hashable u32 (hashfunc unit
// unported; adt_date precedent).
#[inline]
fn int64_hash_fold(val: i64) -> u32 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    lohalf ^ if val >= 0 { hihalf } else { !hihalf }
}

pub fn fc_timestamp_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let folded = int64_hash_fold(fcinfo.arg_i64(0));
    Ok(Datum::from_i32(hashfn::hash_bytes_uint32(folded) as i32))
}

pub fn fc_timestamp_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let folded = int64_hash_fold(fcinfo.arg_i64(0));
    Ok(Datum::from_i64(
        hashfn::hash_bytes_uint32_extended(folded, fcinfo.arg_i64(1) as u64) as i64,
    ))
}

macro_rules! ts_tstz_cross {
    ($($fc:ident: $swap:literal $test:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (ts, tstz) = if $swap {
                (fcinfo.arg_i64(1), fcinfo.arg_i64(0))
            } else {
                (fcinfo.arg_i64(0), fcinfo.arg_i64(1))
            };
            let c = crate::timestamp_cmp_timestamptz_internal(ts, tstz);
            Ok(ts_tstz_cross!(@ret c, $swap, $test))
        }
    )*};
    (@ret $c:ident, $swap:literal, cmp) => {
        Datum::from_i32(if $swap { -$c } else { $c })
    };
    (@ret $c:ident, $swap:literal, ($op:tt)) => {
        Datum::from_bool(if $swap { 0 $op $c } else { $c $op 0 })
    };
}

ts_tstz_cross! {
    fc_timestamp_eq_timestamptz: false (==);
    fc_timestamp_ne_timestamptz: false (!=);
    fc_timestamp_lt_timestamptz: false (<);
    fc_timestamp_gt_timestamptz: false (>);
    fc_timestamp_le_timestamptz: false (<=);
    fc_timestamp_ge_timestamptz: false (>=);
    fc_timestamp_cmp_timestamptz: false cmp;
    fc_timestamptz_eq_timestamp: true (==);
    fc_timestamptz_ne_timestamp: true (!=);
    fc_timestamptz_lt_timestamp: true (<);
    fc_timestamptz_gt_timestamp: true (>);
    fc_timestamptz_le_timestamp: true (<=);
    fc_timestamptz_ge_timestamp: true (>=);
    fc_timestamptz_cmp_timestamp: true cmp;
}

pub fn fc_timestamp_timestamptz(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::timestamp2timestamptz(fcinfo.arg_i64(0))?))
}

pub fn fc_timestamptz_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::timestamptz2timestamp(fcinfo.arg_i64(0))?))
}

pub fn fc_timestamp_zone(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let zone = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(crate::timestamp_zone(zone.data(), fcinfo.arg_i64(1))?))
}

pub fn fc_timestamptz_zone(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let zone = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(crate::timestamptz_zone(zone.data(), fcinfo.arg_i64(1))?))
}

pub fn fc_make_timestamp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [y, mo, d, h, mi, s] = fcinfo.args_n::<6>();
    Ok(Datum::from_i64(crate::make_timestamp(
        y.value.as_i32(),
        mo.value.as_i32(),
        d.value.as_i32(),
        h.value.as_i32(),
        mi.value.as_i32(),
        s.value.as_f64(),
    )?))
}

pub fn fc_make_timestamptz(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [y, mo, d, h, mi, s] = fcinfo.args_n::<6>();
    Ok(Datum::from_i64(crate::make_timestamptz(
        y.value.as_i32(),
        mo.value.as_i32(),
        d.value.as_i32(),
        h.value.as_i32(),
        mi.value.as_i32(),
        s.value.as_f64(),
    )?))
}

pub fn fc_make_timestamptz_at_timezone(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [y, mo, d, h, mi, s] = fcinfo.args_n::<6>();
    let (y, mo, d, h, mi, s) = (
        y.value.as_i32(),
        mo.value.as_i32(),
        d.value.as_i32(),
        h.value.as_i32(),
        mi.value.as_i32(),
        s.value.as_f64(),
    );
    // SAFETY: strict fn — arg 6 is a non-null text varlena.
    let zone = unsafe { fcinfo.arg_varlena_packed(6)? };
    Ok(Datum::from_i64(crate::make_timestamptz_at_timezone(
        y,
        mo,
        d,
        h,
        mi,
        s,
        zone.data(),
    )?))
}

pub fn fc_timestamp_trunc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let units = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(crate::timestamp_trunc(units.data(), fcinfo.arg_i64(1))?))
}

pub fn fc_timestamptz_trunc(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let units = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i64(crate::timestamptz_trunc(units.data(), fcinfo.arg_i64(1))?))
}

pub fn fc_timestamptz_trunc_zone(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/2 are non-null text varlenas.
    let (units, zone) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(2)?) };
    Ok(Datum::from_i64(crate::timestamptz_trunc_zone(
        units.data(),
        fcinfo.arg_i64(1),
        zone.data(),
    )?))
}

fn part_result(fcinfo: &mut Fcinfo, v: PartValue) -> PgResult<Datum> {
    match v {
        PartValue::Null => Ok(fcinfo.return_null()),
        PartValue::Float(f) => Ok(Datum::from_f64(f)),
        PartValue::Numeric(img) => byref_result(fcinfo.result_mcx(), img.as_bytes()),
    }
}

macro_rules! ts_part {
    ($($fc:ident: $core:ident($retnumeric:literal);)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: strict fn — arg 0 is a non-null text varlena.
            let units = unsafe { fcinfo.arg_varlena_packed(0)? };
            let v = crate::$core(units.data(), fcinfo.arg_i64(1), $retnumeric)?;
            part_result(fcinfo, v)
        }
    )*};
}

ts_part! {
    fc_timestamp_part: timestamp_part_common(false);
    fc_extract_timestamp: timestamp_part_common(true);
    fc_timestamptz_part: timestamptz_part_common(false);
    fc_extract_timestamptz: timestamptz_part_common(true);
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_proc.dat rows for timestamp.c; alias OIDs over the same prosrc each get
// their row, as in C's fmgr_builtins[] (the 1152-1157/1195-1196/1314/1389
// rows are the timestamptz operators sharing the timestamp prosrc).
pub const TIMESTAMP_BUILTINS: &[FmgrBuiltin] = &[
    b(274, "timeofday", 0, fc_timeofday),
    b(1150, "timestamptz_in", 3, fc_timestamptz_in),
    b(1151, "timestamptz_out", 1, fc_timestamptz_out),
    b(1152, "timestamp_eq", 2, fc_timestamp_eq),
    b(1153, "timestamp_ne", 2, fc_timestamp_ne),
    b(1154, "timestamp_lt", 2, fc_timestamp_lt),
    b(1155, "timestamp_le", 2, fc_timestamp_le),
    b(1156, "timestamp_ge", 2, fc_timestamp_ge),
    b(1157, "timestamp_gt", 2, fc_timestamp_gt),
    b(1159, "timestamptz_zone", 2, fc_timestamptz_zone),
    b(1171, "timestamptz_part", 2, fc_timestamptz_part),
    b(1195, "timestamp_smaller", 2, fc_timestamp_smaller),
    b(1196, "timestamp_larger", 2, fc_timestamp_larger),
    b(1217, "timestamptz_trunc", 2, fc_timestamptz_trunc),
    b(1284, "timestamptz_trunc_zone", 3, fc_timestamptz_trunc_zone),
    b(1299, "now", 0, fc_now),
    b(1312, "timestamp_in", 3, fc_timestamp_in),
    b(1313, "timestamp_out", 1, fc_timestamp_out),
    b(1314, "timestamp_cmp", 2, fc_timestamp_cmp),
    b(1389, "timestamp_finite", 1, fc_timestamp_finite),
    b(1961, "timestamp_scale", 2, fc_timestamp_scale),
    b(1967, "timestamptz_scale", 2, fc_timestamp_scale),
    b(2020, "timestamp_trunc", 2, fc_timestamp_trunc),
    b(2021, "timestamp_part", 2, fc_timestamp_part),
    b(2027, "timestamptz_timestamp", 1, fc_timestamptz_timestamp),
    b(2028, "timestamp_timestamptz", 1, fc_timestamp_timestamptz),
    b(2035, "timestamp_smaller", 2, fc_timestamp_smaller),
    b(2036, "timestamp_larger", 2, fc_timestamp_larger),
    b(2039, "timestamp_hash", 1, fc_timestamp_hash),
    b(2045, "timestamp_cmp", 2, fc_timestamp_cmp),
    b(2048, "timestamp_finite", 1, fc_timestamp_finite),
    b(2052, "timestamp_eq", 2, fc_timestamp_eq),
    b(2053, "timestamp_ne", 2, fc_timestamp_ne),
    b(2054, "timestamp_lt", 2, fc_timestamp_lt),
    b(2055, "timestamp_le", 2, fc_timestamp_le),
    b(2056, "timestamp_ge", 2, fc_timestamp_ge),
    b(2057, "timestamp_gt", 2, fc_timestamp_gt),
    b(2069, "timestamp_zone", 2, fc_timestamp_zone),
    b(2474, "timestamp_recv", 3, fc_timestamp_recv),
    b(2475, "timestamp_send", 1, fc_timestamp_send),
    b(2476, "timestamptz_recv", 3, fc_timestamptz_recv),
    b(2477, "timestamptz_send", 1, fc_timestamp_send),
    b(2520, "timestamp_lt_timestamptz", 2, fc_timestamp_lt_timestamptz),
    b(2521, "timestamp_le_timestamptz", 2, fc_timestamp_le_timestamptz),
    b(2522, "timestamp_eq_timestamptz", 2, fc_timestamp_eq_timestamptz),
    b(2523, "timestamp_gt_timestamptz", 2, fc_timestamp_gt_timestamptz),
    b(2524, "timestamp_ge_timestamptz", 2, fc_timestamp_ge_timestamptz),
    b(2525, "timestamp_ne_timestamptz", 2, fc_timestamp_ne_timestamptz),
    b(2526, "timestamp_cmp_timestamptz", 2, fc_timestamp_cmp_timestamptz),
    b(2527, "timestamptz_lt_timestamp", 2, fc_timestamptz_lt_timestamp),
    b(2528, "timestamptz_le_timestamp", 2, fc_timestamptz_le_timestamp),
    b(2529, "timestamptz_eq_timestamp", 2, fc_timestamptz_eq_timestamp),
    b(2530, "timestamptz_gt_timestamp", 2, fc_timestamptz_gt_timestamp),
    b(2531, "timestamptz_ge_timestamp", 2, fc_timestamptz_ge_timestamp),
    b(2532, "timestamptz_ne_timestamp", 2, fc_timestamptz_ne_timestamp),
    b(2533, "timestamptz_cmp_timestamp", 2, fc_timestamptz_cmp_timestamp),
    b(2647, "now", 0, fc_now),
    b(2648, "statement_timestamp", 0, fc_statement_timestamp),
    b(2649, "clock_timestamp", 0, fc_clock_timestamp),
    b(3411, "timestamp_hash_extended", 2, fc_timestamp_hash_extended),
    b(3461, "make_timestamp", 6, fc_make_timestamp),
    b(3462, "make_timestamptz", 6, fc_make_timestamptz),
    b(3463, "make_timestamptz_at_timezone", 7, fc_make_timestamptz_at_timezone),
    b(6202, "extract_timestamp", 2, fc_extract_timestamp),
    b(6203, "extract_timestamptz", 2, fc_extract_timestamptz),
    b(6334, "timestamptz_at_local", 1, fc_timestamptz_timestamp),
    b(6335, "timestamp_at_local", 1, fc_timestamp_timestamptz),
    b(6425, "timestamptz_hash", 1, fc_timestamp_hash),
    b(6426, "timestamptz_hash_extended", 2, fc_timestamp_hash_extended),
];
