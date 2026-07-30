//! fmgr wrappers (`fc_*`) + `TIMESTAMP_BUILTINS` for fmgr-core. The bulk of
//! the interval-typed rows (I/O, cmp/hash, arithmetic, justify, scale,
//! finite, timestamp[tz] +/- interval, timestamp_mi, timestamp[tz]_bin
//! 6177/6178) live in adt_date's table; this file adds only the rows
//! adt_date lacks (typmod I/O, part/trunc, age, izone, make_interval,
//! overlaps_timestamp 1304/2041, date_add/date_subtract 6221-6223/6273,
//! generate_series_timestamp[tz][_at_zone] 938/939/6274 + prosupport 6354,
//! float8_timestamptz 1158, timestamp typmodin/typmodout 2905-2908,
//! interval_support 3918, interval avg/sum aggregates 1843/1844/3325/3549/
//! 6324-6326). Not registrable (established precedents): timestamptz_float8
//! (float lane), timestamp_support 3917 / timestamp_sortsupport 3137 /
//! skipsupport (planner nodes), to_timestamp 1778,
//! pg_postmaster_start_time/pg_conf_load_time (backend globals).

use ::datum::{Datum, Varlena, VarlenaRef};
use ::types_core::{Oid, CSTRINGOID};
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DATATYPE_MISMATCH,
};
use ::types_fmgr::{
    byref_result, overlaps_common, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use adt_datetime::{Interval, MAXDATELEN};

use crate::PartValue;

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the nameout/adt_date precedent). The Datum aliases it until the next out
// call.
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

#[inline]
fn arg_interval(fcinfo: &Fcinfo, i: usize) -> Interval {
    // SAFETY: catalog arg i is a non-null interval (typlen 16, typalign d),
    // live for the call; read field-wise like adt_date's arg_timetz.
    unsafe {
        let p = fcinfo.arg_ptr(i);
        Interval {
            time: (p as *const i64).read_unaligned(),
            day: (p.add(8) as *const i32).read_unaligned(),
            month: (p.add(12) as *const i32).read_unaligned(),
        }
    }
}

fn interval_result(fcinfo: &mut Fcinfo, iv: Interval) -> PgResult<Datum> {
    let mut img = [0u8; 16];
    img[..8].copy_from_slice(&iv.time.to_ne_bytes());
    img[8..12].copy_from_slice(&iv.day.to_ne_bytes());
    img[12..].copy_from_slice(&iv.month.to_ne_bytes());
    byref_result(fcinfo.result_mcx(), &img)
}

// ArrayGetIntegerTypmods (arrayutils.c) over the _cstring argument. C has no
// element cap; `cap_msg` is the caller's own too-many-modifiers error, which
// C would raise one frame up.
pub fn array_get_integer_typmods(
    fcinfo: &Fcinfo,
    out: &mut [i32; 8],
    cap_msg: &'static str,
) -> PgResult<usize> {
    // SAFETY: strict fn — arg 0 is a non-null, detoasted cstring[] datum.
    let image = unsafe { VarlenaRef::from_ptr(fcinfo.arg_ptr(0)) }.as_bytes();
    let rd = |off: usize| i32::from_ne_bytes(image[off..off + 4].try_into().unwrap());
    if rd(12) as Oid != CSTRINGOID {
        return Err(Box::new(
            PgError::error("typmod array must be type cstring[]")
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
        ));
    }
    if rd(4) != 1 {
        return Err(Box::new(
            PgError::error("typmod array must be one-dimensional")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
        ));
    }
    if rd(8) != 0 {
        return Err(Box::new(
            PgError::error("typmod array must not contain nulls")
                .with_sqlstate(::types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    }
    let n = rd(16) as usize;
    if n > out.len() {
        return Err(Box::new(
            PgError::error(cap_msg)
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    let mut off = 24usize;
    for slot in out.iter_mut().take(n) {
        let end = off + image[off..].iter().position(|&b| b == 0).expect("NUL-terminated");
        let s = core::str::from_utf8(&image[off..end]).map_err(|_| {
            Box::new(
                PgError::error("invalid input syntax for type integer")
                    .with_sqlstate(::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
            )
        })?;
        *slot = numutils::pg_strtoint32(s)?;
        off = end + 1;
    }
    Ok(n)
}

pub fn fc_intervaltypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut tl = [0i32; 8];
    let n = array_get_integer_typmods(fcinfo, &mut tl, "invalid INTERVAL type modifier")?;
    Ok(Datum::from_i32(crate::interval::intervaltypmodin(&tl[..n])?))
}

pub fn fc_intervaltypmodout(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let mut tmp = [0u8; 64];
        let len = crate::interval::intervaltypmodout(typmod, &mut tmp)?;
        buf[..len].copy_from_slice(&tmp[..len]);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

fn anytimestamp_typmodin(fcinfo: &Fcinfo, istz: bool) -> PgResult<Datum> {
    let mut tl = [0i32; 8];
    let n = array_get_integer_typmods(fcinfo, &mut tl, "invalid type modifier")?;
    if n != 1 {
        return Err(Box::new(
            PgError::error("invalid type modifier")
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(Datum::from_i32(crate::anytimestamp_typmod_check(istz, tl[0])?))
}

pub fn fc_timestamptypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    anytimestamp_typmodin(fcinfo, false)
}

pub fn fc_timestamptztypmodin(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    anytimestamp_typmodin(fcinfo, true)
}

pub fn typmod_paren_suffix_out(typmod: i32, suffix: &[u8], buf: &mut [u8]) -> usize {
    let mut len = 0;
    if typmod >= 0 {
        buf[len] = b'(';
        len += 1;
        let mut digits = [0u8; 10];
        let mut n = 0;
        let mut p = typmod as u32;
        loop {
            digits[n] = b'0' + (p % 10) as u8;
            n += 1;
            p /= 10;
            if p == 0 {
                break;
            }
        }
        for i in (0..n).rev() {
            buf[len] = digits[i];
            len += 1;
        }
        buf[len] = b')';
        len += 1;
    }
    buf[len..len + suffix.len()].copy_from_slice(suffix);
    len + suffix.len()
}

fn anytimestamp_typmodout(fcinfo: &mut Fcinfo, istz: bool) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    let tz: &[u8] = if istz { b" with time zone" } else { b" without time zone" };
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = typmod_paren_suffix_out(typmod, tz, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_timestamptypmodout(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    anytimestamp_typmodout(fcinfo, false)
}

pub fn fc_timestamptztypmodout(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    anytimestamp_typmodout(fcinfo, true)
}

pub fn fc_timestamptz_pl_interval_at_zone(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    // SAFETY: strict fn — arg 2 is a non-null text varlena.
    let zone = unsafe { fcinfo.arg_varlena_packed(2)? };
    let tzp = crate::lookup_timezone(zone.data())?;
    Ok(Datum::from_i64(crate::interval::timestamptz_pl_interval_internal(
        fcinfo.arg_i64(0),
        &iv,
        Some(tzp),
    )?))
}

pub fn fc_timestamptz_mi_interval_at_zone(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    // SAFETY: strict fn — arg 2 is a non-null text varlena.
    let zone = unsafe { fcinfo.arg_varlena_packed(2)? };
    let tzp = crate::lookup_timezone(zone.data())?;
    Ok(Datum::from_i64(crate::interval::timestamptz_mi_interval_internal(
        fcinfo.arg_i64(0),
        &iv,
        Some(tzp),
    )?))
}

// interval_support (timestamp.c): SupportRequestSimplify only — an
// interval_scale cast that cannot truncate becomes a RelabelType.
// timestamp_support (timestamp.c): SupportRequestSimplify only —
// TemporalSimplify (datetime.c) with MAX_TIMESTAMP_PRECISION.
pub fn fc_timestamp_support(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
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
        .unwrap_or_else(|| panic!("timestamp_support: SupportRequestSimplify without a FuncExpr"));
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
        || new_precis == adt_datetime::MAX_TIMESTAMP_PRECISION
        || (old_precis >= 0 && new_precis >= old_precis)
    {
        let mcx = req.mcx.expect("timestamp_support: request carries an mcx");
        let ret = nodes_core::relabel_to_typmod(mcx, source, new_precis)?;
        return Ok(Datum::from_usize(ret.as_raw().as_ptr() as usize));
    }
    Ok(Datum::from_usize(0))
}

pub fn fc_interval_support(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::types_nodes::{supportnodes::SupportRequestSimplify, NodeTag};
    use crate::interval::{intervaltypmodleastfield, INTERVAL_PRECISION};
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
        .unwrap_or_else(|| panic!("interval_support: SupportRequestSimplify without a FuncExpr"));
    assert!(fexpr.args.len() >= 2);
    let Some(c) = fexpr.args.nth(1).as_const() else {
        return Ok(Datum::from_usize(0));
    };
    if c.constisnull {
        return Ok(Datum::from_usize(0));
    }
    let source = fexpr.args.nth(0);
    let new_typmod = c.constvalue.as_i32();
    let noop = if new_typmod < 0 {
        true
    } else {
        let old_typmod = nodes_core::expr_typmod(source);
        let old_least_field = intervaltypmodleastfield(old_typmod)?;
        let new_least_field = intervaltypmodleastfield(new_typmod)?;
        let old_precis = if old_typmod < 0 {
            crate::interval::INTERVAL_FULL_PRECISION
        } else {
            INTERVAL_PRECISION(old_typmod)
        };
        let new_precis = INTERVAL_PRECISION(new_typmod);
        new_least_field <= old_least_field
            && (old_least_field > 0
                || new_precis >= adt_datetime::MAX_INTERVAL_PRECISION
                || new_precis >= old_precis)
    };
    if noop {
        let mcx = req.mcx.expect("interval_support: request carries an mcx");
        let ret = nodes_core::relabel_to_typmod(mcx, source, new_typmod)?;
        return Ok(Datum::from_usize(ret.as_raw().as_ptr() as usize));
    }
    Ok(Datum::from_usize(0))
}

// The state lives in the agg context (the numeric agg_state_arg precedent).
fn interval_agg_state_arg(
    fcinfo: &Fcinfo,
    arg0: ::datum::NullableDatum,
) -> PgResult<*mut crate::interval::IntervalAggState> {
    use crate::interval::IntervalAggState;
    const { assert!(!core::mem::needs_drop::<IntervalAggState>()) }
    // SAFETY: context, if set, is the evaltrans build's AggStateNode, live
    // across every call through this frame.
    let Some(agg_mcx) = (unsafe { fcinfo.agg_context() }) else {
        return Err(Box::new(PgError::error("aggregate function called in non-aggregate context")));
    };
    if !arg0.isnull {
        return Ok(arg0.value.as_usize() as *mut IntervalAggState);
    }
    let layout = core::alloc::Layout::new::<IntervalAggState>();
    let raw = ::mcx::Allocator::allocate(&agg_mcx, layout).map_err(|_| agg_mcx.oom(layout.size()))?;
    let p = raw.cast::<IntervalAggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(IntervalAggState::default()) };
    Ok(p)
}

pub fn fc_interval_avg_accum(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a, b] = *fcinfo.args_n::<2>();
    let state = interval_agg_state_arg(fcinfo, a)?;
    if !b.isnull {
        let iv = arg_interval(fcinfo, 1);
        // SAFETY: a non-null arg0 is the aggcontext-lived state this transfn
        // chain returned; no other reference is live during the call.
        unsafe { crate::interval::do_interval_accum(&mut *state, &iv)? };
    }
    Ok(Datum::from_usize(state as usize))
}

pub fn fc_interval_avg_accum_inv(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a, b] = *fcinfo.args_n::<2>();
    if a.isnull {
        panic!("interval_avg_accum_inv called with NULL state");
    }
    let state = a.value.as_usize() as *mut crate::interval::IntervalAggState;
    if !b.isnull {
        let iv = arg_interval(fcinfo, 1);
        // SAFETY: as fc_interval_avg_accum.
        unsafe { crate::interval::do_interval_discard(&mut *state, &iv)? };
    }
    Ok(Datum::from_usize(state as usize))
}

pub fn fc_interval_avg_combine(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a, b] = *fcinfo.args_n::<2>();
    if b.isnull {
        // C PG_RETURN_POINTER(state1): a possibly-NULL pointer, isnull unset.
        if a.isnull {
            fcinfo.return_null();
        }
        return Ok(a.value);
    }
    // SAFETY: a non-null state arg is the aggcontext-lived state; state2 is
    // read-only here.
    let state2 = unsafe { &*(b.value.as_usize() as *const crate::interval::IntervalAggState) };
    let state1 = interval_agg_state_arg(fcinfo, a)?;
    if a.isnull {
        // SAFETY: fresh aggcontext allocation from interval_agg_state_arg.
        unsafe { *state1 = *state2 };
    } else {
        // SAFETY: as fc_interval_avg_accum.
        unsafe { crate::interval::interval_agg_combine(&mut *state1, state2)? };
    }
    Ok(Datum::from_usize(state1 as usize))
}

macro_rules! fc_interval_agg_final {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = fcinfo.args_n::<1>()[0];
            if a.isnull {
                return Ok(fcinfo.return_null());
            }
            // SAFETY: a non-null arg0 is the aggcontext-lived state (transfn
            // contract); read-only here.
            let state = unsafe { &*(a.value.as_usize() as *const crate::interval::IntervalAggState) };
            match crate::interval::$core(state)? {
                None => Ok(fcinfo.return_null()),
                Some(iv) => interval_result(fcinfo, iv),
            }
        }
    )*};
}

fc_interval_agg_final! {
    fc_interval_avg: interval_avg_final;
    fc_interval_sum: interval_sum_final;
}

pub fn fc_interval_avg_serialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let a = fcinfo.args_n::<1>()[0];
    // SAFETY: strict fn — arg 0 is the aggcontext-lived state.
    let state = unsafe { &*(a.value.as_usize() as *const crate::interval::IntervalAggState) };
    let mut img = [0u8; 40];
    img[..8].copy_from_slice(&state.N.to_be_bytes());
    img[8..16].copy_from_slice(&state.sumX.time.to_be_bytes());
    img[16..20].copy_from_slice(&state.sumX.day.to_be_bytes());
    img[20..24].copy_from_slice(&state.sumX.month.to_be_bytes());
    img[24..32].copy_from_slice(&state.pInfcount.to_be_bytes());
    img[32..].copy_from_slice(&state.nInfcount.to_be_bytes());
    let mcx = fcinfo.result_mcx();
    let mut image: ::mcx::PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, 4 + img.len())?;
    ::mcx::vec_append_bytes(&mut image, &[0u8; 4])?;
    ::mcx::vec_append_bytes(&mut image, &img)?;
    Ok(varlena_result(Varlena::from_image(image)))
}

pub fn fc_interval_avg_deserialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use crate::interval::IntervalAggState;
    // SAFETY: agg deserialize contract — only reached inside an aggregate.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(Box::new(PgError::error("aggregate function called in non-aggregate context")));
    }
    // SAFETY: strict fn — arg 0 is a non-null bytea varlena.
    let sstate = unsafe { fcinfo.arg_varlena_packed(0)? };
    let d = sstate.data();
    let rd8 = |off: usize| i64::from_be_bytes(d[off..off + 8].try_into().unwrap());
    let rd4 = |off: usize| i32::from_be_bytes(d[off..off + 4].try_into().unwrap());
    let state = IntervalAggState {
        N: rd8(0),
        sumX: Interval { time: rd8(8), day: rd4(16), month: rd4(20) },
        pInfcount: rd8(24),
        nInfcount: rd8(32),
    };
    let mcx = fcinfo.result_mcx();
    let layout = core::alloc::Layout::new::<IntervalAggState>();
    let raw = ::mcx::Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(layout.size()))?;
    let p = raw.cast::<IntervalAggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(state) };
    Ok(Datum::from_usize(p as usize))
}

pub fn fc_make_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [y, mo, w, d, h, mi, s] = fcinfo.args_n::<7>();
    let r = crate::interval::make_interval(
        y.value.as_i32(),
        mo.value.as_i32(),
        w.value.as_i32(),
        d.value.as_i32(),
        h.value.as_i32(),
        mi.value.as_i32(),
        s.value.as_f64(),
    )?;
    interval_result(fcinfo, r)
}

pub fn fc_interval_trunc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let units = unsafe { fcinfo.arg_varlena_packed(0)? };
    let iv = arg_interval(fcinfo, 1);
    let r = crate::interval::interval_trunc(units.data(), &iv)?;
    interval_result(fcinfo, r)
}

macro_rules! interval_part {
    ($($fc:ident: $retnumeric:literal;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: strict fn — arg 0 is a non-null text varlena.
            let units = unsafe { fcinfo.arg_varlena_packed(0)? };
            let iv = arg_interval(fcinfo, 1);
            let v = crate::interval::interval_part_common(units.data(), &iv, $retnumeric)?;
            part_result(fcinfo, v)
        }
    )*};
}

interval_part! {
    fc_interval_part: false;
    fc_extract_interval: true;
}

pub fn fc_timestamptz_pl_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(crate::interval::timestamptz_pl_interval(fcinfo.arg_i64(0), &iv)?))
}

pub fn fc_timestamptz_mi_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let iv = arg_interval(fcinfo, 1);
    Ok(Datum::from_i64(crate::interval::timestamptz_mi_interval(fcinfo.arg_i64(0), &iv)?))
}

pub fn fc_timestamp_age(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let r = crate::interval::timestamp_age(fcinfo.arg_i64(0), fcinfo.arg_i64(1))?;
    interval_result(fcinfo, r)
}

pub fn fc_timestamptz_age(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let r = crate::interval::timestamptz_age(fcinfo.arg_i64(0), fcinfo.arg_i64(1))?;
    interval_result(fcinfo, r)
}

pub fn fc_timestamp_izone(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let zone = arg_interval(fcinfo, 0);
    Ok(Datum::from_i64(crate::interval::timestamp_izone(&zone, fcinfo.arg_i64(1))?))
}

pub fn fc_timestamptz_izone(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let zone = arg_interval(fcinfo, 0);
    Ok(Datum::from_i64(crate::interval::timestamptz_izone(&zone, fcinfo.arg_i64(1))?))
}

pub fn fc_overlaps_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    overlaps_common(fcinfo, |fc, i, j| fc.arg_i64(i) > fc.arg_i64(j))
}

#[track_caller]
#[cold]
#[inline(never)]
fn step_size_err(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE))
}

struct GenSeriesTimestamp {
    current: i64,
    finish: i64,
    step: Interval,
    step_sign: i32,
    attimezone: Option<&'static adt_datetime::tz::PgTz>,
    tz_aware: bool,
}

impl GenSeriesTimestamp {
    fn new(
        current: i64,
        finish: i64,
        step: Interval,
        attimezone: Option<&'static adt_datetime::tz::PgTz>,
        tz_aware: bool,
    ) -> PgResult<Self> {
        let step_sign = crate::interval::interval_sign(&step);
        if step_sign == 0 {
            return Err(step_size_err("step size cannot equal zero"));
        }
        if step.is_nobegin() || step.is_noend() {
            return Err(step_size_err("step size cannot be infinite"));
        }
        Ok(GenSeriesTimestamp { current, finish, step, step_sign, attimezone, tz_aware })
    }

    fn next(&mut self) -> PgResult<Option<i64>> {
        let result = self.current;
        let more = if self.step_sign > 0 {
            crate::timestamp_cmp_internal(result, self.finish) <= 0
        } else {
            crate::timestamp_cmp_internal(result, self.finish) >= 0
        };
        if !more {
            return Ok(None);
        }
        self.current = if self.tz_aware {
            crate::interval::timestamptz_pl_interval_internal(self.current, &self.step, self.attimezone)?
        } else {
            crate::interval::timestamp_pl_interval(self.current, &self.step)?
        };
        Ok(Some(result))
    }
}

fn generate_series_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    tz_aware: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("generate_series_timestamp: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let start = fcinfo.arg_i64(0);
        let finish = fcinfo.arg_i64(1);
        let step = arg_interval(fcinfo, 2);
        let attimezone = if tz_aware && fcinfo.nargs() == 4 {
            // SAFETY: strict fn - arg 3 is a non-null text varlena.
            let zone = unsafe { fcinfo.arg_varlena_packed(3)? };
            Some(crate::lookup_timezone(zone.data())?)
        } else {
            None
        };
        let state = GenSeriesTimestamp::new(start, finish, step, attimezone, tz_aware)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(state));
    }
    let next = funcapi::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("generate_series_timestamp: user_fctx set at first call")
        .downcast_mut::<GenSeriesTimestamp>()
        .expect("generate_series_timestamp: user_fctx is GenSeriesTimestamp")
        .next()?;
    match next {
        Some(v) => Ok(funcapi::srf_return_next(flinfo, fcinfo, Datum::from_i64(v))),
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub fn fc_generate_series_timestamp(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    generate_series_common(flinfo, fcinfo, false)
}

pub fn fc_generate_series_timestamptz(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    generate_series_common(flinfo, fcinfo, true)
}

fn interval_from_const_datum(d: Datum) -> Interval {
    // SAFETY: an interval Const's constvalue points at a live 16-byte
    // interval payload owned by the plan tree.
    unsafe {
        let p = d.as_usize() as *const u8;
        Interval {
            time: (p as *const i64).read_unaligned(),
            day: (p.add(8) as *const i32).read_unaligned(),
            month: (p.add(12) as *const i32).read_unaligned(),
        }
    }
}

fn interval_to_microseconds(i: &Interval) -> f64 {
    ((i.month as f64) * adt_datetime::consts::DAYS_PER_MONTH as f64 + i.day as f64)
        * adt_datetime::consts::USECS_PER_DAY as f64
        + i.time as f64
}

// generate_series_timestamp_support (OID 6354): SupportRequestRows over
// all-Const args; anything else returns NULL so callers fall back (the int4
// support precedent - planner-folded exprs are read as Consts directly).
pub fn fc_generate_series_timestamp_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *mut ();
    // SAFETY: prosupport contract - the internal arg points at a live
    // tag-first support-request node exclusively owned by this call.
    let Some(req) = (unsafe { ::types_nodes::supportnodes::support_request_rows_mut(p) }) else {
        return Ok(Datum::from_usize(0));
    };
    let args = match req.node.and_then(|n| n.as_func_expr()) {
        Some(fe) => &fe.args,
        None => return Ok(Datum::from_usize(0)),
    };
    let mut consts = [Datum::null(); 3];
    for (i, arg) in args.iter().enumerate().take(3) {
        match arg.as_const() {
            Some(c) if c.constisnull => {
                req.rows = 0.0;
                return Ok(Datum::from_usize(p as usize));
            }
            Some(c) => consts[i] = c.constvalue,
            None => return Ok(Datum::from_usize(0)),
        }
    }
    if args.iter().count() < 3 {
        return Ok(Datum::from_usize(0));
    }
    let start = consts[0].as_i64();
    let finish = consts[1].as_i64();
    let step = interval_from_const_datum(consts[2]);

    if crate::TIMESTAMP_NOT_FINITE(start)
        || crate::TIMESTAMP_NOT_FINITE(finish)
        || finish.checked_sub(start).is_none()
    {
        return Ok(Datum::from_usize(0));
    }
    let idiff = crate::interval::timestamp_mi(finish, start)?;
    let dstep = interval_to_microseconds(&step);
    if dstep == 0.0 {
        return Ok(Datum::from_usize(0));
    }
    let ddiff = interval_to_microseconds(&idiff);
    req.rows = (ddiff / dstep + 1.0).floor();
    Ok(Datum::from_usize(p as usize))
}

pub fn fc_float8_timestamptz(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::float8_timestamptz(fcinfo.arg_f64(0))?))
}

pub fn fc_in_range_timestamp_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let offset = arg_interval(fcinfo, 2);
    Ok(Datum::from_bool(crate::interval::in_range_timestamp_interval(
        fcinfo.arg_i64(0),
        fcinfo.arg_i64(1),
        &offset,
        fcinfo.arg_bool(3),
        fcinfo.arg_bool(4),
    )?))
}

pub fn fc_in_range_timestamptz_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let offset = arg_interval(fcinfo, 2);
    Ok(Datum::from_bool(crate::interval::in_range_timestamptz_interval(
        fcinfo.arg_i64(0),
        fcinfo.arg_i64(1),
        &offset,
        fcinfo.arg_bool(3),
        fcinfo.arg_bool(4),
    )?))
}

pub fn fc_in_range_interval_interval(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let val = arg_interval(fcinfo, 0);
    let base = arg_interval(fcinfo, 1);
    let offset = arg_interval(fcinfo, 2);
    Ok(Datum::from_bool(crate::interval::in_range_interval_interval(
        &val,
        &base,
        &offset,
        fcinfo.arg_bool(3),
        fcinfo.arg_bool(4),
    )?))
}

// datetime.c pg_timezone_names. The tuplestore copies each row, so the text
// and interval images are per-row stack/heap scratch, not mcx allocations.
pub fn fc_pg_timezone_names(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use adt_datetime::consts::{
        pg_itm_in, POSTGRES_EPOCH_JDATE, SECS_PER_DAY, UNIX_EPOCH_JDATE, USECS_PER_SEC,
    };

    fn text_image(s: &[u8]) -> Vec<u8> {
        let mut img = Vec::with_capacity(4 + s.len());
        img.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + s.len()));
        img.extend_from_slice(s);
        img
    }

    let flinfo = flinfo.expect("pg_timezone_names: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let now = xact::GetCurrentTransactionStartTimestamp();
    // C: timestamp2tm(now, &tzoff, &tm, &fsec, &tzn, tz), inlined to its
    // pg_localtime branch — `now` is finite and in Julian range so the C
    // failure arms cannot fire, and the enumerator's tz borrow is not
    // 'static as timestamp2tm requires.
    let fsec = now.rem_euclid(USECS_PER_SEC);
    let dt_secs = (now - fsec) / USECS_PER_SEC
        + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64;

    let mut tzenum = pgtz::pg_tzenumerate_start()?;
    loop {
        let (name, abbrev, tzoff, is_dst) = {
            let Some(tz) = pgtz::pg_tzenumerate_next(&mut tzenum)? else {
                break;
            };
            let (tzoff, isdst, tzn) = match localtime::pg_localtime(dt_secs, tz) {
                Some(tx) => (-(tx.tm_gmtoff as i32), tx.tm_isdst, tx.tm_zone),
                // out of pg_time_t range: treat as GMT (C comment)
                None => (0i32, -1i32, None),
            };
            // C rejects >31-byte "abbreviations" (hacked Factory zones).
            if tzn.is_some_and(|n| n.len() > 31) {
                continue;
            }
            (
                text_image(localtime::pg_get_timezone_name(tz)),
                text_image(tzn.unwrap_or("").as_bytes()),
                tzoff,
                isdst > 0,
            )
        };

        let itm_in = pg_itm_in {
            tm_usec: -(tzoff as i64) * USECS_PER_SEC,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        let mut iv = Interval::default();
        // C: "can't overflow"
        crate::interval::itmin2interval(&itm_in, &mut iv)
            .expect("pg_timezone_names: utc_offset interval overflow");
        let mut iv_img = [0u8; 16];
        iv_img[..8].copy_from_slice(&iv.time.to_ne_bytes());
        iv_img[8..12].copy_from_slice(&iv.day.to_ne_bytes());
        iv_img[12..].copy_from_slice(&iv.month.to_ne_bytes());

        let values = [
            Datum::from_usize(name.as_ptr() as usize),
            Datum::from_usize(abbrev.as_ptr() as usize),
            Datum::from_usize(iv_img.as_ptr() as usize),
            Datum::from_bool(is_dst),
        ];
        srf.putvalues(&values, &[false; 4])?;
    }
    pgtz::pg_tzenumerate_end(tzenum)?;

    Ok(srf.finish(fcinfo))
}

fn tz_abbrev_text_image(s: &[u8]) -> Vec<u8> {
    let mut img = Vec::with_capacity(4 + s.len());
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + s.len()));
    img.extend_from_slice(s);
    img
}

// gmtoffset seconds -> interval image; C's itmin2interval "can't overflow".
fn tz_abbrev_interval_image(gmtoffset: i64) -> [u8; 16] {
    use adt_datetime::consts::{pg_itm_in, USECS_PER_SEC};
    let itm_in = pg_itm_in {
        tm_usec: gmtoffset * USECS_PER_SEC,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
    };
    let mut iv = Interval::default();
    crate::interval::itmin2interval(&itm_in, &mut iv)
        .expect("timezone abbrev utc_offset interval overflow");
    let mut img = [0u8; 16];
    img[..8].copy_from_slice(&iv.time.to_ne_bytes());
    img[8..12].copy_from_slice(&iv.day.to_ne_bytes());
    img[12..].copy_from_slice(&iv.month.to_ne_bytes());
    img
}

// datetime.c pg_timezone_abbrevs_zone: abbreviations defined by the IANA
// data for the current session timezone setting.
pub fn fc_pg_timezone_abbrevs_zone(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_timezone_abbrevs_zone: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let now = xact::GetCurrentTransactionStartTimestamp();
    let t = crate::timestamptz_to_time_t(now);
    let session_tz = adt_datetime::tz::session_timezone()
        .expect("pg_timezone_abbrevs_zone: session_timezone not initialized");

    let mut pindex = 0i32;
    while let Some(abbrev) = localtime::pg_get_next_timezone_abbrev(&mut pindex, session_tz) {
        if !abbrev.iter().all(u8::is_ascii_uppercase) {
            continue;
        }
        let Some((gmtoff, isdst)) = localtime::pg_interpret_timezone_abbrev(abbrev, t, session_tz)
        else {
            continue;
        };
        let name = tz_abbrev_text_image(abbrev);
        let iv_img = tz_abbrev_interval_image(gmtoff);
        let values = [
            Datum::from_usize(name.as_ptr() as usize),
            Datum::from_usize(iv_img.as_ptr() as usize),
            Datum::from_bool(isdst != 0),
        ];
        srf.putvalues(&values, &[false; 3])?;
    }

    Ok(srf.finish(fcinfo))
}

// datetime.c pg_timezone_abbrevs_abbrevs: abbreviations defined by the
// timezone_abbreviations setting.
pub fn fc_pg_timezone_abbrevs_abbrevs(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use adt_datetime::consts::{DTERR_BAD_ZONE_ABBREV, DTZ, DYNTZ, TZ};

    let flinfo = flinfo.expect("pg_timezone_abbrevs_abbrevs: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let Some(tbl) = adt_datetime::tz::zoneabbrevtbl() else {
        return Ok(srf.finish(fcinfo));
    };
    for tp in tbl.abbrevs {
        let (gmtoffset, is_dst): (i64, bool) = match tp.typ as i32 {
            TZ => (tp.value as i64, false),
            DTZ => (tp.value as i64, true),
            DYNTZ => {
                let mut extra = adt_datetime::DateTimeErrorExtra::default();
                let Some(tzp) = adt_datetime::tz::FetchDynamicTimeZone(tbl, tp, &mut extra)
                else {
                    adt_datetime::errors::DateTimeParseError(
                        DTERR_BAD_ZONE_ABBREV,
                        Some(&extra),
                        "",
                        "",
                        None,
                    )?;
                    unreachable!("DateTimeParseError returns Err");
                };
                let now = xact::GetCurrentTransactionStartTimestamp();
                let mut isdst = 0i32;
                let off =
                    crate::DetermineTimeZoneAbbrevOffsetTS(now, tp.token_bytes(), tzp, &mut isdst)?;
                (-(off as i64), isdst != 0)
            }
            other => panic!("unrecognized timezone type {other}"),
        };

        // Upcase (inverse of ParseDateTime's downcasing).
        let name = tz_abbrev_text_image(&tp.token_bytes().to_ascii_uppercase());
        let iv_img = tz_abbrev_interval_image(gmtoffset);
        let values = [
            Datum::from_usize(name.as_ptr() as usize),
            Datum::from_usize(iv_img.as_ptr() as usize),
            Datum::from_bool(is_dst),
        ];
        srf.putvalues(&values, &[false; 3])?;
    }

    Ok(srf.finish(fcinfo))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

const fn bn(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

// pg_proc.dat rows for timestamp.c; alias OIDs over the same prosrc each get
// their row, as in C's fmgr_builtins[] (the 1152-1157/1195-1196/1314/1389
// rows are the timestamptz operators sharing the timestamp prosrc).
pub const TIMESTAMP_BUILTINS: &[FmgrBuiltin] = &[
    b(274, "timeofday", 0, fc_timeofday),
    srf(2856, "pg_timezone_names", 0, fc_pg_timezone_names),
    srf(6401, "pg_timezone_abbrevs_zone", 0, fc_pg_timezone_abbrevs_zone),
    srf(2599, "pg_timezone_abbrevs_abbrevs", 0, fc_pg_timezone_abbrevs_abbrevs),
    srf(938, "generate_series_timestamp", 3, fc_generate_series_timestamp),
    srf(939, "generate_series_timestamptz", 3, fc_generate_series_timestamptz),
    srf(6274, "generate_series_timestamptz_at_zone", 4, fc_generate_series_timestamptz),
    b(6354, "generate_series_timestamp_support", 1, fc_generate_series_timestamp_support),
    b(1158, "float8_timestamptz", 1, fc_float8_timestamptz),
    b(4134, "in_range_timestamp_interval", 5, fc_in_range_timestamp_interval),
    b(4135, "in_range_timestamptz_interval", 5, fc_in_range_timestamptz_interval),
    b(4136, "in_range_interval_interval", 5, fc_in_range_interval_interval),
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
    b(1026, "timestamptz_izone", 2, fc_timestamptz_izone),
    b(1172, "interval_part", 2, fc_interval_part),
    b(1199, "timestamptz_age", 2, fc_timestamptz_age),
    b(1218, "interval_trunc", 2, fc_interval_trunc),
    bn(1304, "overlaps_timestamp", 4, fc_overlaps_timestamp),
    bn(2041, "overlaps_timestamp", 4, fc_overlaps_timestamp),
    b(2058, "timestamp_age", 2, fc_timestamp_age),
    b(2070, "timestamp_izone", 2, fc_timestamp_izone),
    b(2903, "intervaltypmodin", 1, fc_intervaltypmodin),
    b(2904, "intervaltypmodout", 1, fc_intervaltypmodout),
    b(2905, "timestamptypmodin", 1, fc_timestamptypmodin),
    b(2906, "timestamptypmodout", 1, fc_timestamptypmodout),
    b(2907, "timestamptztypmodin", 1, fc_timestamptztypmodin),
    b(2908, "timestamptztypmodout", 1, fc_timestamptztypmodout),
    b(3917, "timestamp_support", 1, fc_timestamp_support),
    b(3918, "interval_support", 1, fc_interval_support),
    bn(1843, "interval_avg_accum", 2, fc_interval_avg_accum),
    bn(1844, "interval_avg", 1, fc_interval_avg),
    bn(3325, "interval_avg_combine", 2, fc_interval_avg_combine),
    bn(3549, "interval_avg_accum_inv", 2, fc_interval_avg_accum_inv),
    bn(6326, "interval_sum", 1, fc_interval_sum),
    b(6324, "interval_avg_serialize", 1, fc_interval_avg_serialize),
    b(6325, "interval_avg_deserialize", 2, fc_interval_avg_deserialize),
    b(6222, "timestamptz_pl_interval_at_zone", 3, fc_timestamptz_pl_interval_at_zone),
    b(6273, "timestamptz_mi_interval_at_zone", 3, fc_timestamptz_mi_interval_at_zone),
    b(3464, "make_interval", 7, fc_make_interval),
    b(6204, "extract_interval", 2, fc_extract_interval),
    b(6221, "timestamptz_pl_interval", 2, fc_timestamptz_pl_interval),
    b(6223, "timestamptz_mi_interval", 2, fc_timestamptz_mi_interval),
];
