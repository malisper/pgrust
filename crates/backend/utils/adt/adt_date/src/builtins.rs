//! fmgr wrappers (`fc_*`) + `DATE_BUILTINS` for fmgr-core, hosting the
//! timestamp.c rows too (adt_timestamp has no fmgr table; adt_date already
//! deps it). Not registrable (established precedents):
//! sortsupport/skipsupport (planner nodes), age/overlaps_
//! timestamp, in_range, generate_series,
//! date_part(text,date) 1384 (SQL-language). recv/send ride the binary-wire
//! fmgr frame (types_fmgr::wire); interval/timetz by-ref results are
//! 16/12-byte images via byref_result.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, overlaps_common, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::{DateADT, TimeTzADT};
use adt_datetime::{Interval, MAXDATELEN};
use adt_timestamp::{interval, PartValue, TIMESTAMP_NOT_FINITE};

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the nameout precedent). The Datum aliases it until the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; MAXDATELEN + 1]> =
        const { core::cell::UnsafeCell::new([0; MAXDATELEN + 1]) };
}

// PGRUST_ADT_IN_FASTUTF8 (load-speed prototype, DEFAULT OFF): from_utf8_lossy
// walks the bytes with the chunked lossy iterator even when the input is
// entirely valid (the always case for COPY input, which is already
// encoding-verified) — measured ~4% of the 10M-bank COPY wall
// (load-speed lane perf, 2026-07-14; ~20M date/timestamp fields). The fast
// arm validates with core's optimized `str::from_utf8` and borrows; invalid
// input falls back to the identical lossy copy, so semantics are unchanged.
fn in_fastutf8() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_ADT_IN_FASTUTF8").as_deref(), Ok("1") | Ok("on"))
    })
}

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> std::borrow::Cow<'a, str> {
    // SAFETY: catalog arg 0 of the in-functions is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let b = s.to_bytes();
    if in_fastutf8() {
        if let Ok(v) = std::str::from_utf8(b) {
            return std::borrow::Cow::Borrowed(v);
        }
    }
    String::from_utf8_lossy(b)
}

#[inline]
fn arg_date(fcinfo: &Fcinfo, i: usize) -> DateADT {
    fcinfo.arg_i32(i)
}

// timetz typlen is 12: read fields, never form a &TimeTzADT over the tuple
// bytes (the Rust reference would span the 4 padding bytes C never stores).
#[inline]
fn arg_timetz(fcinfo: &Fcinfo, i: usize) -> TimeTzADT {
    // SAFETY: catalog arg i is a non-null timetz (typlen 12, typalign d),
    // live for the call.
    unsafe {
        let p = fcinfo.arg_ptr(i);
        TimeTzADT {
            time: (p as *const i64).read_unaligned(),
            zone: (p.add(8) as *const i32).read_unaligned(),
        }
    }
}

pub fn fc_date_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i32(crate::date_in(&s, esc)?))
}

pub fn fc_date_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let date = arg_date(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::date_out(date, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_date_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i32(crate::date_recv(buf)?))
}

pub fn fc_date_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let date = arg_date(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::date_send(mcx, date)?))
}

pub fn fc_time_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(crate::time_recv(buf, typmod)?))
}

pub fn fc_time_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let time = fcinfo.arg_i64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::time_send(mcx, time)?))
}

pub fn fc_timetz_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let t = crate::timetz_recv(buf, typmod)?;
    let mut img = [0u8; 12];
    img[..8].copy_from_slice(&t.time.to_ne_bytes());
    img[8..].copy_from_slice(&t.zone.to_ne_bytes());
    byref_result(fcinfo.result_mcx(), &img)
}

pub fn fc_timetz_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = arg_timetz(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::timetz_send(mcx, &t)?))
}

pub fn fc_make_date(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [y, m, d] = fcinfo.args_n::<3>();
    Ok(Datum::from_i32(crate::make_date(
        y.value.as_i32(),
        m.value.as_i32(),
        d.value.as_i32(),
    )?))
}

macro_rules! date_cmp_ops {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_date(fcinfo, 0);
            let b = arg_date(fcinfo, 1);
            Ok(Datum::from_bool(a $op b))
        }
    )*};
}

date_cmp_ops! {
    fc_date_eq: ==;
    fc_date_ne: !=;
    fc_date_lt: <;
    fc_date_le: <=;
    fc_date_gt: >;
    fc_date_ge: >=;
}

pub fn fc_date_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::date_cmp_internal(
        arg_date(fcinfo, 0),
        arg_date(fcinfo, 1),
    )))
}

pub fn fc_date_finite(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!crate::DATE_NOT_FINITE(arg_date(fcinfo, 0))))
}

pub fn fc_date_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_date(fcinfo, 0), arg_date(fcinfo, 1));
    Ok(Datum::from_i32(if a > b { a } else { b }))
}

pub fn fc_date_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_date(fcinfo, 0), arg_date(fcinfo, 1));
    Ok(Datum::from_i32(if a < b { a } else { b }))
}

pub fn fc_date_mi(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::date_mi(arg_date(fcinfo, 0), arg_date(fcinfo, 1))?))
}

pub fn fc_date_pli(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::date_pli(arg_date(fcinfo, 0), fcinfo.arg_i32(1))?))
}

pub fn fc_date_mii(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::date_mii(arg_date(fcinfo, 0), fcinfo.arg_i32(1))?))
}

pub fn fc_hashdate(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(hashfn::hash_bytes_uint32(arg_date(fcinfo, 0) as u32) as i32))
}

pub fn fc_hashdateextended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(hashfn::hash_bytes_uint32_extended(
        arg_date(fcinfo, 0) as u32,
        fcinfo.arg_i64(1) as u64,
    ) as i64))
}

pub fn fc_date_timestamp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::date2timestamp(arg_date(fcinfo, 0))?))
}

pub fn fc_timestamp_date(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::timestamp_date(fcinfo.arg_i64(0))?))
}

pub fn fc_date_timestamptz(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::date2timestamptz(arg_date(fcinfo, 0))?))
}

pub fn fc_timestamptz_date(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::timestamptz_date(fcinfo.arg_i64(0))?))
}

pub fn fc_datetime_timestamp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::datetime_timestamp(
        arg_date(fcinfo, 0),
        fcinfo.arg_i64(1),
    )?))
}

pub fn fc_datetimetz_timestamptz(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let tt = arg_timetz(fcinfo, 1);
    Ok(Datum::from_i64(crate::datetimetz_timestamptz(arg_date(fcinfo, 0), &tt)?))
}

macro_rules! date_ts_cross {
    ($($fc:ident: $core:ident($swap:literal) $test:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (d, ts) = if $swap {
                (arg_date(fcinfo, 1), fcinfo.arg_i64(0))
            } else {
                (arg_date(fcinfo, 0), fcinfo.arg_i64(1))
            };
            let c = crate::$core(d, ts);
            Ok(date_ts_cross!(@ret c, $swap, $test))
        }
    )*};
    (@ret $c:ident, $swap:literal, cmp) => {
        Datum::from_i32(if $swap { -$c } else { $c })
    };
    (@ret $c:ident, $swap:literal, ($op:tt)) => {
        Datum::from_bool(if $swap { 0 $op $c } else { $c $op 0 })
    };
}

date_ts_cross! {
    fc_date_eq_timestamp: date_cmp_timestamp_internal(false) (==);
    fc_date_ne_timestamp: date_cmp_timestamp_internal(false) (!=);
    fc_date_lt_timestamp: date_cmp_timestamp_internal(false) (<);
    fc_date_gt_timestamp: date_cmp_timestamp_internal(false) (>);
    fc_date_le_timestamp: date_cmp_timestamp_internal(false) (<=);
    fc_date_ge_timestamp: date_cmp_timestamp_internal(false) (>=);
    fc_date_cmp_timestamp: date_cmp_timestamp_internal(false) cmp;
    fc_date_eq_timestamptz: date_cmp_timestamptz_internal(false) (==);
    fc_date_ne_timestamptz: date_cmp_timestamptz_internal(false) (!=);
    fc_date_lt_timestamptz: date_cmp_timestamptz_internal(false) (<);
    fc_date_gt_timestamptz: date_cmp_timestamptz_internal(false) (>);
    fc_date_le_timestamptz: date_cmp_timestamptz_internal(false) (<=);
    fc_date_ge_timestamptz: date_cmp_timestamptz_internal(false) (>=);
    fc_date_cmp_timestamptz: date_cmp_timestamptz_internal(false) cmp;
    fc_timestamp_eq_date: date_cmp_timestamp_internal(true) (==);
    fc_timestamp_ne_date: date_cmp_timestamp_internal(true) (!=);
    fc_timestamp_lt_date: date_cmp_timestamp_internal(true) (<);
    fc_timestamp_gt_date: date_cmp_timestamp_internal(true) (>);
    fc_timestamp_le_date: date_cmp_timestamp_internal(true) (<=);
    fc_timestamp_ge_date: date_cmp_timestamp_internal(true) (>=);
    fc_timestamp_cmp_date: date_cmp_timestamp_internal(true) cmp;
    fc_timestamptz_eq_date: date_cmp_timestamptz_internal(true) (==);
    fc_timestamptz_ne_date: date_cmp_timestamptz_internal(true) (!=);
    fc_timestamptz_lt_date: date_cmp_timestamptz_internal(true) (<);
    fc_timestamptz_gt_date: date_cmp_timestamptz_internal(true) (>);
    fc_timestamptz_le_date: date_cmp_timestamptz_internal(true) (<=);
    fc_timestamptz_ge_date: date_cmp_timestamptz_internal(true) (>=);
    fc_timestamptz_cmp_date: date_cmp_timestamptz_internal(true) cmp;
}

pub fn fc_time_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(crate::time_in(&s, typmod, esc)?))
}

pub fn fc_time_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let time = fcinfo.arg_i64(0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::time_out(time, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_make_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [h, m, s] = fcinfo.args_n::<3>();
    Ok(Datum::from_i64(crate::make_time(
        h.value.as_i32(),
        m.value.as_i32(),
        s.value.as_f64(),
    )?))
}

macro_rules! time_cmp_ops {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = fcinfo.arg_i64(0);
            let b = fcinfo.arg_i64(1);
            Ok(Datum::from_bool(a $op b))
        }
    )*};
}

time_cmp_ops! {
    fc_time_eq: ==;
    fc_time_ne: !=;
    fc_time_lt: <;
    fc_time_le: <=;
    fc_time_gt: >;
    fc_time_ge: >=;
}

pub fn fc_time_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::time_cmp_internal(
        fcinfo.arg_i64(0),
        fcinfo.arg_i64(1),
    )))
}

pub fn fc_time_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let folded = crate::int64_hash_fold(fcinfo.arg_i64(0));
    Ok(Datum::from_i32(hashfn::hash_bytes_uint32(folded) as i32))
}

pub fn fc_time_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let folded = crate::int64_hash_fold(fcinfo.arg_i64(0));
    Ok(Datum::from_i64(
        hashfn::hash_bytes_uint32_extended(folded, fcinfo.arg_i64(1) as u64) as i64,
    ))
}

pub fn fc_time_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg_i64(0), fcinfo.arg_i64(1));
    Ok(Datum::from_i64(if a > b { a } else { b }))
}

pub fn fc_time_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (fcinfo.arg_i64(0), fcinfo.arg_i64(1));
    Ok(Datum::from_i64(if a < b { a } else { b }))
}

pub fn fc_time_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::time_scale(fcinfo.arg_i64(0), fcinfo.arg_i32(1))))
}

pub fn fc_timestamp_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match crate::timestamp_time(fcinfo.arg_i64(0))? {
        Some(t) => Ok(Datum::from_i64(t)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_timestamptz_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match crate::timestamptz_time(fcinfo.arg_i64(0))? {
        Some(t) => Ok(Datum::from_i64(t)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_timetz_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(arg_timetz(fcinfo, 0).time))
}

pub fn fc_timetz_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let tt = arg_timetz(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::timetz_out(&tt, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! timetz_cmp_ops {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_timetz(fcinfo, 0);
            let b = arg_timetz(fcinfo, 1);
            Ok(Datum::from_bool(crate::timetz_cmp_internal(&a, &b) $op 0))
        }
    )*};
}

timetz_cmp_ops! {
    fc_timetz_eq: ==;
    fc_timetz_ne: !=;
    fc_timetz_lt: <;
    fc_timetz_le: <=;
    fc_timetz_gt: >;
    fc_timetz_ge: >=;
}

pub fn fc_timetz_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_timetz(fcinfo, 0);
    let b = arg_timetz(fcinfo, 1);
    Ok(Datum::from_i32(crate::timetz_cmp_internal(&a, &b)))
}

pub fn fc_timetz_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let tt = arg_timetz(fcinfo, 0);
    // field hashes XORed separately to dodge struct padding, as in C
    let h = hashfn::hash_bytes_uint32(crate::int64_hash_fold(tt.time))
        ^ hashfn::hash_bytes_uint32(tt.zone as u32);
    Ok(Datum::from_i32(h as i32))
}

pub fn fc_timetz_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let tt = arg_timetz(fcinfo, 0);
    let seed = fcinfo.arg_i64(1) as u64;
    let h = hashfn::hash_bytes_uint32_extended(crate::int64_hash_fold(tt.time), seed)
        ^ hashfn::hash_bytes_uint32_extended(tt.zone as u32, seed);
    Ok(Datum::from_i64(h as i64))
}

pub fn fc_timetz_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_timetz(fcinfo, 0);
    let b = arg_timetz(fcinfo, 1);
    // C returns the winning input pointer
    let i = if crate::timetz_cmp_internal(&a, &b) > 0 { 0 } else { 1 };
    Ok(fcinfo.arg(i))
}

pub fn fc_timetz_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_timetz(fcinfo, 0);
    let b = arg_timetz(fcinfo, 1);
    let i = if crate::timetz_cmp_internal(&a, &b) < 0 { 0 } else { 1 };
    Ok(fcinfo.arg(i))
}

pub fn fc_overlaps_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    overlaps_common(fcinfo, |fc, i, j| fc.arg_i64(i) > fc.arg_i64(j))
}

pub fn fc_overlaps_timetz(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    overlaps_common(fcinfo, |fc, i, j| {
        crate::timetz_cmp_internal(&arg_timetz(fc, i), &arg_timetz(fc, j)) > 0
    })
}


#[inline]
fn arg_interval(fcinfo: &Fcinfo, i: usize) -> Interval {
    // SAFETY: catalog arg i is a non-null interval (typlen 16, typalign d),
    // live for the call; read field-wise like arg_timetz.
    unsafe {
        let p = fcinfo.arg_ptr(i);
        Interval {
            time: (p as *const i64).read_unaligned(),
            day: (p.add(8) as *const i32).read_unaligned(),
            month: (p.add(12) as *const i32).read_unaligned(),
        }
    }
}

fn interval_result(fcinfo: &Fcinfo, iv: &Interval) -> PgResult<Datum> {
    let mut img = [0u8; 16];
    img[..8].copy_from_slice(&iv.time.to_ne_bytes());
    img[8..12].copy_from_slice(&iv.day.to_ne_bytes());
    img[12..].copy_from_slice(&iv.month.to_ne_bytes());
    byref_result(fcinfo.result_mcx(), &img)
}

fn timetz_result(fcinfo: &Fcinfo, t: &crate::TimeTzADT) -> PgResult<Datum> {
    let mut img = [0u8; 12];
    img[..8].copy_from_slice(&t.time.to_ne_bytes());
    img[8..].copy_from_slice(&t.zone.to_ne_bytes());
    byref_result(fcinfo.result_mcx(), &img)
}

pub fn fc_interval_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let iv = interval::interval_in(&s, typmod, esc)?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = interval::interval_out(&iv, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_interval_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let iv = interval::interval_recv(buf, typmod)?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(interval::interval_send(mcx, &iv)?))
}

macro_rules! interval_cmp_ops {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_interval(fcinfo, 0);
            let b = arg_interval(fcinfo, 1);
            Ok(Datum::from_bool(interval::interval_cmp_internal(&a, &b) $op 0))
        }
    )*};
}

interval_cmp_ops! {
    fc_interval_eq: ==;
    fc_interval_ne: !=;
    fc_interval_lt: <;
    fc_interval_le: <=;
    fc_interval_gt: >;
    fc_interval_ge: >=;
}

pub fn fc_interval_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_interval(fcinfo, 0);
    let b = arg_interval(fcinfo, 1);
    Ok(Datum::from_i32(interval::interval_cmp_internal(&a, &b)))
}

// C hashes only the low 64 bits of the 128-bit span (pre-INT128 hash compat).
pub fn fc_interval_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let span64 = interval::interval_cmp_value(&arg_interval(fcinfo, 0)) as i64;
    Ok(Datum::from_i32(hashfn::hash_bytes_uint32(crate::int64_hash_fold(span64)) as i32))
}

pub fn fc_interval_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let span64 = interval::interval_cmp_value(&arg_interval(fcinfo, 0)) as i64;
    Ok(Datum::from_i64(hashfn::hash_bytes_uint32_extended(
        crate::int64_hash_fold(span64),
        fcinfo.arg_i64(1) as u64,
    ) as i64))
}

pub fn fc_interval_um(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::interval_um(&arg_interval(fcinfo, 0))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_pl(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::interval_pl(&arg_interval(fcinfo, 0), &arg_interval(fcinfo, 1))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_mi(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::interval_mi(&arg_interval(fcinfo, 0), &arg_interval(fcinfo, 1))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_mul(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::interval_mul(&arg_interval(fcinfo, 0), fcinfo.arg_f64(1))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_mul_d_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::interval_mul(&arg_interval(fcinfo, 1), fcinfo.arg_f64(0))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_div(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::interval_div(&arg_interval(fcinfo, 0), fcinfo.arg_f64(1))?;
    interval_result(fcinfo, &iv)
}

// C returns one of the input pointers: no allocation.
pub fn fc_interval_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    let c =
        interval::interval_cmp_internal(&arg_interval(fcinfo, 0), &arg_interval(fcinfo, 1));
    Ok(if c < 0 { a.value } else { b.value })
}

pub fn fc_interval_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    let c =
        interval::interval_cmp_internal(&arg_interval(fcinfo, 0), &arg_interval(fcinfo, 1));
    Ok(if c > 0 { a.value } else { b.value })
}

pub fn fc_interval_justify_hours(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = interval::interval_justify_hours(&arg_interval(fcinfo, 0))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_justify_days(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = interval::interval_justify_days(&arg_interval(fcinfo, 0))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_justify_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = interval::interval_justify_interval(&arg_interval(fcinfo, 0))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::interval_scale(&arg_interval(fcinfo, 0), fcinfo.arg_i32(1))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_interval_finite(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!arg_interval(fcinfo, 0).not_finite()))
}

pub fn fc_timestamp_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(adt_timestamp::timestamp_in(&s, typmod, esc)?))
}

pub fn fc_timestamptz_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(adt_timestamp::timestamptz_in(&s, typmod, esc)?))
}

pub fn fc_timestamp_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = fcinfo.arg_i64(0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = adt_timestamp::timestamp_out(t, buf)?;
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_timestamptz_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = fcinfo.arg_i64(0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = adt_timestamp::timestamptz_out(t, buf)?;
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_timestamp_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(adt_timestamp::timestamp_recv(buf, typmod)?))
}

pub fn fc_timestamptz_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(adt_timestamp::timestamptz_recv(buf, typmod)?))
}

pub fn fc_timestamp_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = fcinfo.arg_i64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(adt_timestamp::timestamp_send(mcx, t)?))
}

macro_rules! ts_cmp_ops {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (fcinfo.arg_i64(0), fcinfo.arg_i64(1));
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

pub fn fc_timestamp_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let folded = crate::int64_hash_fold(fcinfo.arg_i64(0));
    Ok(Datum::from_i32(hashfn::hash_bytes_uint32(folded) as i32))
}

pub fn fc_timestamp_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let folded = crate::int64_hash_fold(fcinfo.arg_i64(0));
    Ok(Datum::from_i64(
        hashfn::hash_bytes_uint32_extended(folded, fcinfo.arg_i64(1) as u64) as i64,
    ))
}

pub fn fc_timestamp_finite(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!TIMESTAMP_NOT_FINITE(fcinfo.arg_i64(0))))
}

pub fn fc_timestamp_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(adt_timestamp::timestamp_scale(
        fcinfo.arg_i64(0),
        fcinfo.arg_i32(1),
    )?))
}

pub fn fc_timestamp_mi(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = interval::timestamp_mi(fcinfo.arg_i64(0), fcinfo.arg_i64(1))?;
    interval_result(fcinfo, &iv)
}

pub fn fc_timestamp_pl_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(interval::timestamp_pl_interval(fcinfo.arg_i64(0), &iv)?))
}

pub fn fc_timestamp_mi_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(interval::timestamp_mi_interval(fcinfo.arg_i64(0), &iv)?))
}

pub fn fc_timestamptz_pl_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(interval::timestamptz_pl_interval_internal(
        fcinfo.arg_i64(0),
        &iv,
        None,
    )?))
}

pub fn fc_timestamptz_mi_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(interval::timestamptz_mi_interval_internal(
        fcinfo.arg_i64(0),
        &iv,
        None,
    )?))
}

fn part_result(fcinfo: &mut Fcinfo, v: PartValue) -> PgResult<Datum> {
    match v {
        PartValue::Null => Ok(fcinfo.return_null()),
        PartValue::Float(f) => Ok(Datum::from_f64(f)),
        PartValue::Numeric(img) => byref_result(fcinfo.result_mcx(), img.as_bytes()),
    }
}

pub fn fc_date_pl_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(crate::date_pl_interval(arg_date(fcinfo, 0), &iv)?))
}

pub fn fc_date_mi_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(crate::date_mi_interval(arg_date(fcinfo, 0), &iv)?))
}

pub fn fc_time_pl_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(crate::time_pl_interval(fcinfo.arg_i64(0), &iv)?))
}

pub fn fc_time_mi_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(crate::time_mi_interval(fcinfo.arg_i64(0), &iv)?))
}

pub fn fc_timetz_pl_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = arg_timetz(fcinfo, 0);
    let iv = arg_interval(fcinfo, 1);
    timetz_result(fcinfo, &crate::timetz_pl_interval(&t, &iv)?)
}

pub fn fc_timetz_mi_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = arg_timetz(fcinfo, 0);
    let iv = arg_interval(fcinfo, 1);
    timetz_result(fcinfo, &crate::timetz_mi_interval(&t, &iv)?)
}

pub fn fc_time_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    interval_result(fcinfo, &crate::time_interval(fcinfo.arg_i64(0)))
}

pub fn fc_interval_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::interval_time(&arg_interval(fcinfo, 0))?))
}

pub fn fc_time_mi_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    interval_result(fcinfo, &crate::time_mi_time(fcinfo.arg_i64(0), fcinfo.arg_i64(1)))
}

macro_rules! ts_tstz_cross {
    ($($fc:ident: ($swap:literal) $test:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (ts, tstz) = if $swap {
                (fcinfo.arg_i64(1), fcinfo.arg_i64(0))
            } else {
                (fcinfo.arg_i64(0), fcinfo.arg_i64(1))
            };
            let c = interval::timestamp_cmp_timestamptz_internal(ts, tstz)?;
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
    fc_timestamp_lt_timestamptz: (false) (<);
    fc_timestamp_le_timestamptz: (false) (<=);
    fc_timestamp_eq_timestamptz: (false) (==);
    fc_timestamp_gt_timestamptz: (false) (>);
    fc_timestamp_ge_timestamptz: (false) (>=);
    fc_timestamp_ne_timestamptz: (false) (!=);
    fc_timestamp_cmp_timestamptz: (false) cmp;
    fc_timestamptz_lt_timestamp: (true) (<);
    fc_timestamptz_le_timestamp: (true) (<=);
    fc_timestamptz_eq_timestamp: (true) (==);
    fc_timestamptz_gt_timestamp: (true) (>);
    fc_timestamptz_ge_timestamp: (true) (>=);
    fc_timestamptz_ne_timestamp: (true) (!=);
    fc_timestamptz_cmp_timestamp: (true) cmp;
}

pub fn fc_timestamp_timestamptz(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(interval::timestamp2timestamptz(fcinfo.arg_i64(0))?))
}

pub fn fc_timestamptz_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(interval::timestamptz2timestamp(fcinfo.arg_i64(0))?))
}

pub fn fc_timetz_zone(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let zone = unsafe { fcinfo.arg_varlena_packed(0)? };
    let t = arg_timetz(fcinfo, 1);
    timetz_result(fcinfo, &crate::timetz_zone(zone.data(), &t)?)
}

pub fn fc_timetz_izone(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let zone = arg_interval(fcinfo, 0);
    let t = arg_timetz(fcinfo, 1);
    timetz_result(fcinfo, &crate::timetz_izone(&zone, &t)?)
}

pub fn fc_timetz_at_local(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = arg_timetz(fcinfo, 0);
    timetz_result(fcinfo, &crate::timetz_at_local(&t)?)
}

pub fn fc_extract_date(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let units = unsafe { fcinfo.arg_varlena_packed(0)? };
    let v = crate::extract_date(units.data(), arg_date(fcinfo, 1))?;
    part_result(fcinfo, v)
}

macro_rules! time_part_fns {
    ($($fc:ident: $timetz:literal, $retnumeric:literal;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: strict fn — arg 0 is a non-null text varlena.
            let units = unsafe { fcinfo.arg_varlena_packed(0)? };
            let v = if $timetz {
                let t = arg_timetz(fcinfo, 1);
                crate::timetz_part_common(units.data(), &t, $retnumeric)?
            } else {
                crate::time_part_common(units.data(), fcinfo.arg_i64(1), $retnumeric)?
            };
            part_result(fcinfo, v)
        }
    )*};
}

time_part_fns! {
    fc_time_part: false, false;
    fc_extract_time: false, true;
    fc_timetz_part: true, false;
    fc_extract_timetz: true, true;
}

pub fn fc_timetz_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let t = crate::timetz_in(&s, typmod, esc)?;
    timetz_result(fcinfo, &t)
}

pub fn fc_timetz_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let t = arg_timetz(fcinfo, 0);
    timetz_result(fcinfo, &crate::timetz_scale(&t, fcinfo.arg_i32(1)))
}

pub fn fc_time_timetz(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    timetz_result(fcinfo, &crate::time_timetz(fcinfo.arg_i64(0)))
}

pub fn fc_timestamptz_timetz(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match crate::timestamptz_timetz(fcinfo.arg_i64(0))? {
        Some(t) => timetz_result(fcinfo, &t),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_timestamp_bin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let stride = arg_interval(fcinfo, 0);
    Ok(Datum::from_i64(interval::timestamp_bin(
        &stride,
        fcinfo.arg_i64(1),
        fcinfo.arg_i64(2),
    )?))
}

fn anytime_typmodin(fcinfo: &Fcinfo, istz: bool) -> PgResult<Datum> {
    let mut tl = [0i32; 8];
    let n = adt_timestamp::builtins::array_get_integer_typmods(
        fcinfo,
        &mut tl,
        "invalid type modifier",
    )?;
    if n != 1 {
        return Err(Box::new(
            ::types_error::PgError::error("invalid type modifier")
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(Datum::from_i32(crate::anytime_typmod_check(istz, tl[0])?))
}

pub fn fc_timetypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    anytime_typmodin(fcinfo, false)
}

pub fn fc_timetztypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    anytime_typmodin(fcinfo, true)
}

fn anytime_typmodout(fcinfo: &mut Fcinfo, istz: bool) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    let tz: &[u8] = if istz { b" with time zone" } else { b" without time zone" };
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = adt_timestamp::builtins::typmod_paren_suffix_out(typmod, tz, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_timetypmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    anytime_typmodout(fcinfo, false)
}

pub fn fc_timetztypmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    anytime_typmodout(fcinfo, true)
}

#[cold]
#[inline(never)]
fn invalid_in_range_offset() -> Box<::types_error::PgError> {
    Box::new(
        ::types_error::PgError::error("invalid preceding or following size in window function")
            .with_sqlstate(::types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
    )
}

pub fn fc_in_range_date_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let offset = arg_interval(fcinfo, 2);
    let val = crate::date2timestamp(fcinfo.arg_i32(0))?;
    let base = crate::date2timestamp(fcinfo.arg_i32(1))?;
    Ok(Datum::from_bool(adt_timestamp::interval::in_range_timestamp_interval(
        val,
        base,
        &offset,
        fcinfo.arg_bool(3),
        fcinfo.arg_bool(4),
    )?))
}

// C disregards the month/day interval fields (time_pl_interval precedent);
// the non-wrapping sum makes +infinity saturate to `less`.
pub fn fc_in_range_time_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let val = fcinfo.arg_i64(0);
    let base = fcinfo.arg_i64(1);
    let offset = arg_interval(fcinfo, 2);
    let sub = fcinfo.arg_bool(3);
    let less = fcinfo.arg_bool(4);
    if offset.time < 0 {
        return Err(invalid_in_range_offset());
    }
    let sum = if sub {
        base - offset.time
    } else {
        match base.checked_add(offset.time) {
            Some(s) => s,
            None => return Ok(Datum::from_bool(less)),
        }
    };
    Ok(Datum::from_bool(if less { val <= sum } else { val >= sum }))
}

pub fn fc_in_range_timetz_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let val = arg_timetz(fcinfo, 0);
    let base = arg_timetz(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    let sub = fcinfo.arg_bool(3);
    let less = fcinfo.arg_bool(4);
    if offset.time < 0 {
        return Err(invalid_in_range_offset());
    }
    let time = if sub {
        base.time - offset.time
    } else {
        match base.time.checked_add(offset.time) {
            Some(s) => s,
            None => return Ok(Datum::from_bool(less)),
        }
    };
    let sum = TimeTzADT { time, zone: base.zone };
    let cmp = crate::timetz_cmp_internal(&val, &sum);
    Ok(Datum::from_bool(if less { cmp <= 0 } else { cmp >= 0 }))
}

// time_support (date.c): SupportRequestSimplify only — TemporalSimplify
// (datetime.c) with MAX_TIME_PRECISION; a time/timetz typmod cast that
// cannot truncate becomes a RelabelType.
pub fn fc_time_support(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::types_nodes::{supportnodes::SupportRequestSimplify, NodeTag};
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *const NodeTag;
    // SAFETY: prosupport contract — arg points at a live tag-first node.
    if unsafe { *p } != NodeTag::T_SupportRequestSimplify {
        return Ok(Datum::from_usize(0));
    }
    // SAFETY: tag checked; the planner owns the request node for the call.
    let req = unsafe { &*(a.value.as_usize() as *const SupportRequestSimplify) };
    let fexpr = req
        .fcall
        .and_then(|n| n.as_func_expr())
        .unwrap_or_else(|| panic!("time_support: SupportRequestSimplify without a FuncExpr"));
    assert!(fexpr.args.len() >= 2);
    let Some(c) = fexpr.args.nth(1).as_const() else {
        return Ok(Datum::from_usize(0));
    };
    if c.constisnull {
        return Ok(Datum::from_usize(0));
    }
    let source = fexpr.args.nth(0);
    let old_precis = nodes_core::expr_typmod(source);
    let new_precis = c.constvalue.as_i32();
    if new_precis < 0
        || new_precis == adt_datetime::MAX_TIME_PRECISION
        || (old_precis >= 0 && new_precis >= old_precis)
    {
        let mcx = req.mcx.expect("time_support: request carries an mcx");
        let ret = nodes_core::relabel_to_typmod(mcx, source, new_precis)?;
        return Ok(Datum::from_usize(ret.as_raw().as_ptr() as usize));
    }
    Ok(Datum::from_usize(0))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn bn(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

// pg_proc.dat rows for date.c; alias OIDs over the same prosrc each get
// their row, as in C's fmgr_builtins[].
pub const DATE_BUILTINS: &[FmgrBuiltin] = &[
    b(2909, "timetypmodin", 1, fc_timetypmodin),
    b(3944, "time_support", 1, fc_time_support),
    b(2910, "timetypmodout", 1, fc_timetypmodout),
    b(2911, "timetztypmodin", 1, fc_timetztypmodin),
    b(2912, "timetztypmodout", 1, fc_timetztypmodout),
    b(4133, "in_range_date_interval", 5, fc_in_range_date_interval),
    b(4137, "in_range_time_interval", 5, fc_in_range_time_interval),
    b(4138, "in_range_timetz_interval", 5, fc_in_range_timetz_interval),
    b(2468, "date_recv", 1, fc_date_recv),
    b(2469, "date_send", 1, fc_date_send),
    b(2470, "time_recv", 3, fc_time_recv),
    b(2471, "time_send", 1, fc_time_send),
    b(2472, "timetz_recv", 3, fc_timetz_recv),
    b(2473, "timetz_send", 1, fc_timetz_send),
    b(1084, "date_in", 1, fc_date_in),
    b(1085, "date_out", 1, fc_date_out),
    b(1086, "date_eq", 2, fc_date_eq),
    b(1087, "date_lt", 2, fc_date_lt),
    b(1088, "date_le", 2, fc_date_le),
    b(1089, "date_gt", 2, fc_date_gt),
    b(1090, "date_ge", 2, fc_date_ge),
    b(1091, "date_ne", 2, fc_date_ne),
    b(1092, "date_cmp", 2, fc_date_cmp),
    b(1102, "time_lt", 2, fc_time_lt),
    b(1103, "time_le", 2, fc_time_le),
    b(1104, "time_gt", 2, fc_time_gt),
    b(1105, "time_ge", 2, fc_time_ge),
    b(1106, "time_ne", 2, fc_time_ne),
    b(1107, "time_cmp", 2, fc_time_cmp),
    b(1138, "date_larger", 2, fc_date_larger),
    b(1139, "date_smaller", 2, fc_date_smaller),
    b(1140, "date_mi", 2, fc_date_mi),
    b(1141, "date_pli", 2, fc_date_pli),
    b(1142, "date_mii", 2, fc_date_mii),
    b(1143, "time_in", 3, fc_time_in),
    b(1144, "time_out", 1, fc_time_out),
    b(1145, "time_eq", 2, fc_time_eq),
    b(1174, "date_timestamptz", 1, fc_date_timestamptz),
    b(1178, "timestamptz_date", 1, fc_timestamptz_date),
    bn(1271, "overlaps_timetz", 4, fc_overlaps_timetz),
    b(1272, "datetime_timestamp", 2, fc_datetime_timestamp),
    b(1297, "datetimetz_timestamptz", 2, fc_datetimetz_timestamptz),
    bn(1308, "overlaps_time", 4, fc_overlaps_time),
    b(1316, "timestamp_time", 1, fc_timestamp_time),
    b(1351, "timetz_out", 1, fc_timetz_out),
    b(1352, "timetz_eq", 2, fc_timetz_eq),
    b(1353, "timetz_ne", 2, fc_timetz_ne),
    b(1354, "timetz_lt", 2, fc_timetz_lt),
    b(1355, "timetz_le", 2, fc_timetz_le),
    b(1356, "timetz_ge", 2, fc_timetz_ge),
    b(1357, "timetz_gt", 2, fc_timetz_gt),
    b(1358, "timetz_cmp", 2, fc_timetz_cmp),
    b(1359, "datetimetz_timestamptz", 2, fc_datetimetz_timestamptz),
    b(1373, "date_finite", 1, fc_date_finite),
    b(1377, "time_larger", 2, fc_time_larger),
    b(1378, "time_smaller", 2, fc_time_smaller),
    b(1379, "timetz_larger", 2, fc_timetz_larger),
    b(1380, "timetz_smaller", 2, fc_timetz_smaller),
    b(1688, "time_hash", 1, fc_time_hash),
    b(1696, "timetz_hash", 1, fc_timetz_hash),
    b(1968, "time_scale", 2, fc_time_scale),
    b(2019, "timestamptz_time", 1, fc_timestamptz_time),
    b(2024, "date_timestamp", 1, fc_date_timestamp),
    b(2025, "datetime_timestamp", 2, fc_datetime_timestamp),
    b(2029, "timestamp_date", 1, fc_timestamp_date),
    b(2046, "timetz_time", 1, fc_timetz_time),
    b(2338, "date_lt_timestamp", 2, fc_date_lt_timestamp),
    b(2339, "date_le_timestamp", 2, fc_date_le_timestamp),
    b(2340, "date_eq_timestamp", 2, fc_date_eq_timestamp),
    b(2341, "date_gt_timestamp", 2, fc_date_gt_timestamp),
    b(2342, "date_ge_timestamp", 2, fc_date_ge_timestamp),
    b(2343, "date_ne_timestamp", 2, fc_date_ne_timestamp),
    b(2344, "date_cmp_timestamp", 2, fc_date_cmp_timestamp),
    b(2351, "date_lt_timestamptz", 2, fc_date_lt_timestamptz),
    b(2352, "date_le_timestamptz", 2, fc_date_le_timestamptz),
    b(2353, "date_eq_timestamptz", 2, fc_date_eq_timestamptz),
    b(2354, "date_gt_timestamptz", 2, fc_date_gt_timestamptz),
    b(2355, "date_ge_timestamptz", 2, fc_date_ge_timestamptz),
    b(2356, "date_ne_timestamptz", 2, fc_date_ne_timestamptz),
    b(2357, "date_cmp_timestamptz", 2, fc_date_cmp_timestamptz),
    b(2364, "timestamp_lt_date", 2, fc_timestamp_lt_date),
    b(2365, "timestamp_le_date", 2, fc_timestamp_le_date),
    b(2366, "timestamp_eq_date", 2, fc_timestamp_eq_date),
    b(2367, "timestamp_gt_date", 2, fc_timestamp_gt_date),
    b(2368, "timestamp_ge_date", 2, fc_timestamp_ge_date),
    b(2369, "timestamp_ne_date", 2, fc_timestamp_ne_date),
    b(2370, "timestamp_cmp_date", 2, fc_timestamp_cmp_date),
    b(2377, "timestamptz_lt_date", 2, fc_timestamptz_lt_date),
    b(2378, "timestamptz_le_date", 2, fc_timestamptz_le_date),
    b(2379, "timestamptz_eq_date", 2, fc_timestamptz_eq_date),
    b(2380, "timestamptz_gt_date", 2, fc_timestamptz_gt_date),
    b(2381, "timestamptz_ge_date", 2, fc_timestamptz_ge_date),
    b(2382, "timestamptz_ne_date", 2, fc_timestamptz_ne_date),
    b(2383, "timestamptz_cmp_date", 2, fc_timestamptz_cmp_date),
    b(3409, "time_hash_extended", 2, fc_time_hash_extended),
    b(3410, "timetz_hash_extended", 2, fc_timetz_hash_extended),
    b(3846, "make_date", 3, fc_make_date),
    b(3847, "make_time", 3, fc_make_time),
    b(6415, "hashdate", 1, fc_hashdate),
    b(6416, "hashdateextended", 2, fc_hashdateextended),
    b(1160, "interval_in", 3, fc_interval_in),
    b(1161, "interval_out", 1, fc_interval_out),
    b(2478, "interval_recv", 3, fc_interval_recv),
    b(2479, "interval_send", 1, fc_interval_send),
    b(1162, "interval_eq", 2, fc_interval_eq),
    b(1163, "interval_ne", 2, fc_interval_ne),
    b(1164, "interval_lt", 2, fc_interval_lt),
    b(1165, "interval_le", 2, fc_interval_le),
    b(1166, "interval_ge", 2, fc_interval_ge),
    b(1167, "interval_gt", 2, fc_interval_gt),
    b(1315, "interval_cmp", 2, fc_interval_cmp),
    b(1697, "interval_hash", 1, fc_interval_hash),
    b(3418, "interval_hash_extended", 2, fc_interval_hash_extended),
    b(1168, "interval_um", 1, fc_interval_um),
    b(1169, "interval_pl", 2, fc_interval_pl),
    b(1170, "interval_mi", 2, fc_interval_mi),
    b(1618, "interval_mul", 2, fc_interval_mul),
    b(1624, "mul_d_interval", 2, fc_mul_d_interval),
    b(1326, "interval_div", 2, fc_interval_div),
    b(1197, "interval_smaller", 2, fc_interval_smaller),
    b(1198, "interval_larger", 2, fc_interval_larger),
    b(1175, "interval_justify_hours", 1, fc_interval_justify_hours),
    b(1295, "interval_justify_days", 1, fc_interval_justify_days),
    b(2711, "interval_justify_interval", 1, fc_interval_justify_interval),
    b(1200, "interval_scale", 2, fc_interval_scale),
    b(1390, "interval_finite", 1, fc_interval_finite),
    b(1188, "timestamp_mi", 2, fc_timestamp_mi),
    b(2031, "timestamp_mi", 2, fc_timestamp_mi),
    b(2032, "timestamp_pl_interval", 2, fc_timestamp_pl_interval),
    b(2033, "timestamp_mi_interval", 2, fc_timestamp_mi_interval),
    b(1189, "timestamptz_pl_interval", 2, fc_timestamptz_pl_interval),
    b(1190, "timestamptz_mi_interval", 2, fc_timestamptz_mi_interval),
    b(2071, "date_pl_interval", 2, fc_date_pl_interval),
    b(2072, "date_mi_interval", 2, fc_date_mi_interval),
    b(1747, "time_pl_interval", 2, fc_time_pl_interval),
    b(1748, "time_mi_interval", 2, fc_time_mi_interval),
    b(1749, "timetz_pl_interval", 2, fc_timetz_pl_interval),
    b(1750, "timetz_mi_interval", 2, fc_timetz_mi_interval),
    b(1370, "time_interval", 1, fc_time_interval),
    b(1419, "interval_time", 1, fc_interval_time),
    b(1690, "time_mi_time", 2, fc_time_mi_time),
    b(1350, "timetz_in", 3, fc_timetz_in),
    b(1969, "timetz_scale", 2, fc_timetz_scale),
    b(2047, "time_timetz", 1, fc_time_timetz),
    b(1388, "timestamptz_timetz", 1, fc_timestamptz_timetz),
    b(1273, "timetz_part", 2, fc_timetz_part),
    b(1385, "time_part", 2, fc_time_part),
    b(2037, "timetz_zone", 2, fc_timetz_zone),
    b(2038, "timetz_izone", 2, fc_timetz_izone),
    b(6177, "timestamp_bin", 3, fc_timestamp_bin),
    b(6178, "timestamptz_bin", 3, fc_timestamp_bin),
    b(6199, "extract_date", 2, fc_extract_date),
    b(6200, "extract_time", 2, fc_extract_time),
    b(6201, "extract_timetz", 2, fc_extract_timetz),
    b(6336, "timetz_at_local", 1, fc_timetz_at_local),
];
