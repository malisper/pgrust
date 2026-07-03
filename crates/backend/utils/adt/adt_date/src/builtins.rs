//! fmgr wrappers (`fc_*`) + `DATE_BUILTINS` for fmgr-core. Not registrable
//! (established precedents): typmodin (ArrayType), sortsupport/skipsupport/
//! time_support (planner nodes), the interval-typed rows (interval unit
//! deferred), extract/date_part rows (numeric image frame), timetz_zone/izone/
//! at_local (DecodeTimezoneName), and the fresh-TimeTzADT constructors
//! timetz_in/timetz_scale/time_timetz/timestamptz_timetz (their value cores
//! live in the crate root). date/time/timetz recv/send ride the binary-wire
//! fmgr frame (types_fmgr::wire); timetz_recv builds its 12-byte by-ref image
//! via byref_result.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::{DateADT, TimeTzADT};
use adt_datetime::MAXDATELEN;

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the nameout precedent). The Datum aliases it until the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; MAXDATELEN + 1]> =
        const { core::cell::UnsafeCell::new([0; MAXDATELEN + 1]) };
}

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> std::borrow::Cow<'a, str> {
    // SAFETY: catalog arg 0 of the in-functions is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
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

// SQL OVERLAPS; non-strict, argument normalization per spec (nulls swapped
// toward ts, ordering swapped so ts <= te).
fn overlaps_common(
    fcinfo: &mut Fcinfo,
    gt: impl Fn(&Fcinfo, usize, usize) -> bool,
) -> PgResult<Datum> {
    let mut s1 = 0usize;
    let mut e1 = 1usize;
    let mut s2 = 2usize;
    let mut e2 = 3usize;
    let mut e1_null = fcinfo.argisnull(e1);
    let mut e2_null = fcinfo.argisnull(e2);

    if fcinfo.argisnull(s1) {
        if e1_null {
            return Ok(fcinfo.return_null());
        }
        core::mem::swap(&mut s1, &mut e1);
        e1_null = true;
    } else if !e1_null && gt(fcinfo, s1, e1) {
        core::mem::swap(&mut s1, &mut e1);
    }

    if fcinfo.argisnull(s2) {
        if e2_null {
            return Ok(fcinfo.return_null());
        }
        core::mem::swap(&mut s2, &mut e2);
        e2_null = true;
    } else if !e2_null && gt(fcinfo, s2, e2) {
        core::mem::swap(&mut s2, &mut e2);
    }

    if gt(fcinfo, s1, s2) {
        if e2_null {
            return Ok(fcinfo.return_null());
        }
        if gt(fcinfo, e2, s1) {
            return Ok(Datum::from_bool(true));
        }
        if e1_null {
            return Ok(fcinfo.return_null());
        }
        Ok(Datum::from_bool(false))
    } else if gt(fcinfo, s2, s1) {
        if e1_null {
            return Ok(fcinfo.return_null());
        }
        if gt(fcinfo, e1, s2) {
            return Ok(Datum::from_bool(true));
        }
        if e2_null {
            return Ok(fcinfo.return_null());
        }
        Ok(Datum::from_bool(false))
    } else {
        if e1_null || e2_null {
            return Ok(fcinfo.return_null());
        }
        Ok(Datum::from_bool(true))
    }
}

pub fn fc_overlaps_time(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    overlaps_common(fcinfo, |fc, i, j| fc.arg_i64(i) > fc.arg_i64(j))
}

pub fn fc_overlaps_timetz(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    overlaps_common(fcinfo, |fc, i, j| {
        crate::timetz_cmp_internal(&arg_timetz(fc, i), &arg_timetz(fc, j)) > 0
    })
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
];
