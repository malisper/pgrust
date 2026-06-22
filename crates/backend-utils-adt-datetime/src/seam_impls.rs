//! Implementations of the inward seams this unit owns:
//!   * `backend-utils-adt-timestamp-seams` (utils/adt/timestamp.c)
//!   * `backend-utils-adt-datetime-seams` (utils/adt/datetime.c)
//!
//! These are the cross-cycle entry points other crates reach the date/time
//! subsystem through (xact, json, guc, timeout, ...). They marshal into the
//! crate's value cores; `init_seams()` installs every one.

use types_core::{Oid, TimestampTz};
use types_datetime::{
    fsec_t, DateADT, TimeADT, Timestamp, TimeTzADT, USE_ISO_DATES, USE_XSD_DATES,
    POSTGRES_EPOCH_JDATE, USECS_PER_SEC,
};
use types_tuple::heaptuple::{DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID};
use types_pgtime::pg_tm;

use crate::calendar::j2date;
use crate::date::DATE_NOT_FINITE;
use crate::encode::{
    EncodeDateOnly, EncodeDateTime, EncodeSpecialTimestamp, EncodeTimeOnly,
};
use crate::time::time2tm;
use crate::timestamp::{timestamp2tm, TIMESTAMP_NOT_FINITE};
use crate::timetz::timetz2tm;

// ---------------------------------------------------------------------------
// timestamp.c: the TimestampDifference family.
// ---------------------------------------------------------------------------

/// `TimestampDifference(start, stop, *secs, *microsecs)` (timestamp.c:1721):
/// `(secs, usecs)` of `stop - start`, both zero when the difference is <= 0.
pub fn timestamp_difference(start_time: TimestampTz, stop_time: TimestampTz) -> (i64, i32) {
    let diff = stop_time - start_time;
    if diff <= 0 {
        (0, 0)
    } else {
        (diff / USECS_PER_SEC, (diff % USECS_PER_SEC) as i32)
    }
}

/// `TimestampDifferenceMilliseconds(start, stop)` (timestamp.c:1757): the
/// difference in milliseconds, rounded up, clamped to `[0, INT_MAX]`.
pub fn timestamp_difference_milliseconds(start_time: TimestampTz, stop_time: TimestampTz) -> i64 {
    // Deal with zero or negative elapsed time quickly.
    if start_time >= stop_time {
        return 0;
    }
    // To not fail with timestamp infinities, we must detect overflow.
    let diff = match stop_time.checked_sub(start_time) {
        Some(d) => d,
        None => return i32::MAX as i64,
    };
    if diff >= (i32::MAX as i64 * 1000 - 999) {
        i32::MAX as i64
    } else {
        (diff + 999) / 1000
    }
}

/// `TimestampDifferenceExceeds(start, stop, msec)` (timestamp.c:1781).
pub fn timestamp_difference_exceeds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    msec: i32,
) -> bool {
    let diff = stop_time - start_time;
    diff >= msec as i64 * 1000
}

/// `TimestampDifferenceExceedsSeconds(start, stop, threshold_sec)`
/// (timestamp.c:1795).
pub fn timestamp_difference_exceeds_seconds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    threshold_sec: i32,
) -> bool {
    let (secs, _usecs) = timestamp_difference(start_time, stop_time);
    secs >= threshold_sec as i64
}

// ---------------------------------------------------------------------------
// timestamp.c: timestamptz_to_str.
// ---------------------------------------------------------------------------

/// `timestamptz_to_str(t)` (timestamp.c:1862): ISO-style render in the session
/// time zone. C uses a static buffer and never errors ("(timestamp out of
/// range)" on conversion failure); we charge the owned copy to `mcx`.
pub fn timestamptz_to_str<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    t: TimestampTz,
) -> types_error::PgResult<mcx::PgString<'mcx>> {
    let mut buf = String::new();
    if TIMESTAMP_NOT_FINITE(t) {
        EncodeSpecialTimestamp(t, &mut buf);
    } else {
        let mut tz: i32 = 0;
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tzn: Option<String> = None;
        if timestamp2tm(t, Some(&mut tz), &mut tm, &mut fsec, Some(&mut tzn), None).is_ok() {
            EncodeDateTime(
                &mut tm,
                fsec,
                true,
                tz,
                tzn.as_deref(),
                USE_ISO_DATES,
                &mut buf,
            );
        } else {
            buf.push_str("(timestamp out of range)");
        }
    }
    mcx::PgString::from_str_in(&buf, mcx)
}

// ---------------------------------------------------------------------------
// timestamp.c (via xlogrecovery.c's check_recovery_target_time): syntax-only
// parse returning whether the string is a DTK_DATE timestamp in range.
// ---------------------------------------------------------------------------

/// `parse_recovery_target_time` — `ParseDateTime` + `DecodeDateTime` and a
/// final `tm2timestamp` range check, returning `true` when the string parses to
/// a `DTK_DATE` timestamp in range (xlogrecovery.c:4979). The final
/// time-zone-dependent parse is deferred to `timestamptz_in` at assign time.
pub fn parse_recovery_target_time(newval: String) -> bool {
    use crate::decode::{DecodeDateTime, ParseDateTime};
    use crate::timestamp::tm2timestamp;
    use types_datetime::{DTK_DATE, MAXDATEFIELDS, MAXDATELEN};

    let mut field: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();
    let mut nf = 0usize;
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    let mut dtype: i32 = 0;
    let mut extra = types_datetime::DateTimeErrorExtra::default();

    // C workbuf is MAXDATELEN + MAXDATEFIELDS here (not MAXDATELEN + 1).
    let mut dterr = ParseDateTime(
        &newval,
        (MAXDATELEN + MAXDATEFIELDS) as usize,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS as usize,
        &mut nf,
    );
    if dterr == 0 {
        dterr = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
            &mut extra,
        );
    }
    if dterr != 0 {
        return false;
    }
    if dtype != DTK_DATE {
        return false;
    }
    let mut timestamp: TimestampTz = 0;
    tm2timestamp(&tm, fsec, Some(tz), &mut timestamp).is_ok()
}

// ---------------------------------------------------------------------------
// json.c: JsonEncodeDateTime (json.c:309). Pure date/time field-conversion +
// Encode* (XSD date style). Owned by the datetime subsystem.
// ---------------------------------------------------------------------------

/// `JsonEncodeDateTime(buf, value, typid, tzp)` (json.c:309): encode a
/// date/time `Datum` into ISO/XSD format. `value` is the canonical unified
/// value; a datetime is a by-value word except `timetz` (by-reference).
pub fn json_encode_datetime(
    value: &types_tuple::Datum<'_>,
    typid: Oid,
    tzp: Option<i32>,
) -> types_error::PgResult<String> {
    let mut buf = String::new();
    match typid {
        x if x == DATEOID => {
            let date: DateADT = value.as_i32();
            if DATE_NOT_FINITE(date) {
                crate::encode::EncodeSpecialDate(date, &mut buf);
            } else {
                let (y, m, d) = j2date(date + POSTGRES_EPOCH_JDATE);
                let tm = pg_tm {
                    tm_year: y,
                    tm_mon: m,
                    tm_mday: d,
                    ..Default::default()
                };
                EncodeDateOnly(&tm, USE_XSD_DATES, &mut buf);
            }
        }
        x if x == TIMEOID => {
            let time: TimeADT = value.as_i64();
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            time2tm(time, &mut tm, &mut fsec);
            EncodeTimeOnly(&tm, fsec, false, 0, USE_XSD_DATES, &mut buf);
        }
        x if x == TIMETZOID => {
            // C's `JsonEncodeDateTime` reads `DatumGetTimeTzADTP(value)` — the
            // by-reference 12-byte {time:i64, zone:i32}. The jsonpath jbvDatetime
            // port carries a `timetz` losslessly *split* into a by-value time
            // word (`value`) plus the zone threaded alongside in `tzp` (see
            // jsonpath_exec datetime.rs: `value = Datum::from_i64(tt.time);
            // tz = tt.zone`), so reconstruct the `TimeTzADT` from those two
            // halves rather than reading a by-reference image that the by-value
            // word does not carry (`as_ref_bytes` would panic on the `ByVal`
            // arm). `tzp` is always Some here (the jsonb encoder passes the
            // datetime's `tz`); fall back to 0 to mirror an absent zone.
            let time = TimeTzADT {
                time: value.as_i64(),
                zone: tzp.unwrap_or(0),
            };
            let mut tm = pg_tm::default();
            let mut fsec: fsec_t = 0;
            let mut tz: i32 = 0;
            timetz2tm(&time, &mut tm, &mut fsec, &mut tz);
            EncodeTimeOnly(&tm, fsec, true, tz, USE_XSD_DATES, &mut buf);
        }
        x if x == TIMESTAMPOID => {
            let timestamp: Timestamp = value.as_i64();
            if TIMESTAMP_NOT_FINITE(timestamp) {
                EncodeSpecialTimestamp(timestamp, &mut buf);
            } else {
                let mut tm = pg_tm::default();
                let mut fsec: fsec_t = 0;
                if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_ok() {
                    EncodeDateTime(&mut tm, fsec, false, 0, None, USE_XSD_DATES, &mut buf);
                } else {
                    return Err(crate::timestamp::timestamp_out_of_range());
                }
            }
        }
        x if x == TIMESTAMPTZOID => {
            let mut timestamp: TimestampTz = value.as_i64();
            if TIMESTAMP_NOT_FINITE(timestamp) {
                EncodeSpecialTimestamp(timestamp, &mut buf);
            } else {
                let mut tm = pg_tm::default();
                let mut fsec: fsec_t = 0;
                let mut tz: i32 = 0;
                let mut tzn: Option<String> = None;
                // If a tz is specified, shift and convert as if without a tz,
                // then use the specified tz for the string.
                let ok = if let Some(z) = tzp {
                    tz = z;
                    timestamp -= z as i64 * USECS_PER_SEC;
                    timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_ok()
                } else {
                    timestamp2tm(
                        timestamp,
                        Some(&mut tz),
                        &mut tm,
                        &mut fsec,
                        Some(&mut tzn),
                        None,
                    )
                    .is_ok()
                };
                if ok {
                    if tzp.is_some() {
                        tm.tm_isdst = 1; // set time-zone presence flag
                    }
                    EncodeDateTime(
                        &mut tm,
                        fsec,
                        true,
                        tz,
                        tzn.as_deref(),
                        USE_XSD_DATES,
                        &mut buf,
                    );
                } else {
                    return Err(crate::timestamp::timestamp_out_of_range());
                }
            }
        }
        other => {
            return Err(types_error::PgError::error(format!(
                "unknown jsonb value datetime type oid {other}"
            )));
        }
    }
    Ok(buf)
}

// ---------------------------------------------------------------------------
// datetime.c: ConvertTimeZoneAbbrevs (consumed by tzparser's load_tzoffsets).
// ---------------------------------------------------------------------------

/// `ConvertTimeZoneAbbrevs(abbrevs, n)` (datetime.c): convert parsed `tzEntry`
/// rows into the `TimeZoneAbbrevTable` storage format. The owner-side body
/// lives in `crate::tables`; this is the seam marshal point.
pub fn convert_time_zone_abbrevs(
    abbrevs: Vec<types_misc_more2::TzEntry>,
) -> Option<types_misc_more2::TimeZoneAbbrevTable> {
    crate::tables::convert_time_zone_abbrevs(abbrevs)
}

// ===========================================================================
// datetime.c / timestamp.c / date.c / isoweek.c seam adapters.
//
// Thin marshal-and-delegate fns between the owner-installed seam signatures and
// the value cores. They are the cross-cycle entry points formatting.c (DCH)
// reaches the calendar / tz-resolution / tm-conversion cores through.
// ===========================================================================

use std::cell::RefCell;
use std::rc::Rc;
use types_datetime::{
    pg_itm, Interval, Timestamp2TmResult, TzAbbrevMatch, TzHandle, YmdDate,
};
use types_pgtime::pg_tz;
use state_pgtz::session_timezone;

// --- datetime.c calendar / validate / tz-offset adapters ------------------

/// `date2j(y, m, d)` (datetime.c): Julian day number.
pub fn seam_date2j(year: i32, month: i32, day: i32) -> i32 {
    crate::calendar::date2j(year, month, day)
}

/// `j2date(jd, &y, &m, &d)` (datetime.c): the core returns the triple; the seam
/// packages it as a `YmdDate`.
pub fn seam_j2date(jd: i32) -> YmdDate {
    let (year, mon, mday) = crate::calendar::j2date(jd);
    YmdDate { year, mon, mday }
}

/// `ValidateDate(fmask, isjulian, is2digits, bc, tm)` (datetime.c). The sole
/// consumer is `do_to_timestamp` (formatting.c), whose call is
/// `ValidateDate(fmask, true, false, false, tm)` — i.e. `isjulian` is true: by
/// that point the year (incl. BC/century) is already fully computed and must
/// not be re-touched by ValidateDate's AD/BC year fix-up.
pub fn seam_validate_date(fmask: i32, is2digits: bool, bc: bool, tm: &mut pg_tm) -> i32 {
    crate::decode::ValidateDate(fmask, true, is2digits, bc, tm)
}

/// `DetermineTimeZoneOffset(tm, tzp)` (datetime.c): resolve under the session
/// timezone (the seam carries no tzp, mirroring the C call sites that pass
/// `session_timezone`).
pub fn seam_determine_time_zone_offset(tm: &mut pg_tm) -> i32 {
    crate::decode::DetermineTimeZoneOffset(tm, &session_timezone())
}

// --- TzHandle interning registry (datetime.c DYNTZ resolution) ------------
//
// `DecodeTimezoneAbbrevPrefix` may return a `pg_tz *` for a dynamic
// abbreviation, which the DCH caller passes back later to
// `DetermineTimeZoneAbbrevOffset`. The owned surface names that pointer with an
// opaque `TzHandle(u32)`; we intern the `Rc<pg_tz>` in a per-backend registry
// and hand back its id, mirroring the thread-local style of `tz_resolver`.
thread_local! {
    static TZ_HANDLE_REGISTRY: RefCell<Vec<Rc<pg_tz>>> = const { RefCell::new(Vec::new()) };
}

/// Intern a resolved zone, returning its stable `TzHandle`. Identical zones
/// (same `Rc` allocation) re-use the same handle.
fn intern_tz(tz: Rc<pg_tz>) -> TzHandle {
    TZ_HANDLE_REGISTRY.with(|reg| {
        let mut reg = reg.borrow_mut();
        if let Some(idx) = reg.iter().position(|z| Rc::ptr_eq(z, &tz)) {
            return TzHandle(idx as u32);
        }
        let idx = reg.len();
        reg.push(tz);
        TzHandle(idx as u32)
    })
}

/// Resolve a `TzHandle` back to its interned zone (panics on an unknown handle,
/// a wiring bug — C would dereference a stale pointer).
fn resolve_tz(handle: TzHandle) -> Rc<pg_tz> {
    TZ_HANDLE_REGISTRY.with(|reg| {
        reg.borrow()
            .get(handle.0 as usize)
            .cloned()
            .unwrap_or_else(|| panic!("TzHandle({}) not interned", handle.0))
    })
}

/// `DecodeTimezoneAbbrevPrefix(str, &offset, &tz)` (datetime.c:3371): owned
/// output shape. On a dynamic match the resolved zone is interned and its handle
/// returned in `tzp`.
pub fn seam_decode_timezone_abbrev_prefix(s: &[u8]) -> TzAbbrevMatch {
    let (tzlen, gmtoffset, tz) = crate::decode::DecodeTimezoneAbbrevPrefix(s);
    TzAbbrevMatch {
        tzlen,
        gmtoffset,
        tzp: tz.map(intern_tz),
    }
}

/// `DetermineTimeZoneAbbrevOffset(tm, abbr, tzp)` (datetime.c:1765): resolve the
/// dynamic abbreviation's offset under the zone named by `tzp`.
pub fn seam_determine_time_zone_abbrev_offset(tm: &mut pg_tm, abbr: &str, tzp: TzHandle) -> i32 {
    let zone = resolve_tz(tzp);
    crate::decode::DetermineTimeZoneAbbrevOffset(tm, abbr, &zone)
}

// --- timestamp.c / date.c tm-conversion adapters --------------------------

/// `timestamp2tm(dt, &tzp, tm, &fsec, &tzn, attimezone)` (timestamp.c): the seam
/// always uses the session timezone (`attimezone = NULL`); `want_tz` selects
/// whether zone fields (`tz`/`tzn`) are resolved. `Err(())` is the C `-1`.
fn seam_timestamp2tm(dt: Timestamp, want_tz: bool) -> Result<Timestamp2TmResult, ()> {
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    let mut tzn: Option<String> = None;
    if want_tz {
        timestamp2tm(dt, Some(&mut tz), &mut tm, &mut fsec, Some(&mut tzn), None)?;
    } else {
        timestamp2tm(dt, None, &mut tm, &mut fsec, None, None)?;
    }
    Ok(Timestamp2TmResult { tm, fsec, tz, tzn })
}

/// `tm2timestamp(tm, fsec, tzp, &result)` (timestamp.c).
fn seam_tm2timestamp(tm: &pg_tm, fsec: fsec_t, tz: Option<i32>) -> Result<Timestamp, ()> {
    let mut result: Timestamp = 0;
    crate::timestamp::tm2timestamp(tm, fsec, tz, &mut result)?;
    Ok(result)
}

/// `interval2itm(span, itm)` (timestamp.c).
fn seam_interval2itm(span: Interval) -> pg_itm {
    let mut itm = pg_itm::default();
    crate::interval::interval2itm(span, &mut itm);
    itm
}

/// `tm2time(tm, fsec, &result)` (date.c).
fn seam_tm2time(tm: &pg_tm, fsec: fsec_t) -> TimeADT {
    crate::time::tm2time(tm, fsec)
}

/// `tm2timetz(tm, fsec, tz, &result)` (date.c).
fn seam_tm2timetz(tm: &pg_tm, fsec: fsec_t, tz: i32) -> TimeTzADT {
    crate::timetz::tm2timetz(tm, fsec, tz)
}

/// `AdjustTimestampForTypmod(&time, typmod, NULL)` (timestamp.c). `Err` carries
/// the C `ereport(ERROR, "timestamp out of range")`.
fn seam_adjust_timestamp_for_typmod(
    value: Timestamp,
    typmod: i32,
) -> types_error::PgResult<Timestamp> {
    let mut t = value;
    crate::timestamp::AdjustTimestampForTypmod(&mut t, typmod)?;
    Ok(t)
}

/// `AdjustTimeForTypmod(&time, typmod)` (date.c): infallible in C.
fn seam_adjust_time_for_typmod(time: TimeADT, typmod: i32) -> TimeADT {
    let mut t = time;
    crate::time::AdjustTimeForTypmod(&mut t, typmod);
    t
}

// --- isoweek.c adapters ----------------------------------------------------

fn seam_date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    crate::isoweek::date2isoweek(year, mon, mday)
}
fn seam_date2isoyear(year: i32, mon: i32, mday: i32) -> i32 {
    crate::isoweek::date2isoyear(year, mon, mday)
}
fn seam_date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    crate::isoweek::date2isoyearday(year, mon, mday)
}
fn seam_isoweek2date(woy: i32, year: i32) -> YmdDate {
    let mut y = year;
    let mut mon = 0;
    let mut mday = 0;
    crate::isoweek::isoweek2date(woy, &mut y, &mut mon, &mut mday);
    YmdDate { year: y, mon, mday }
}
fn seam_isoweekdate2date(isoweek: i32, wday: i32, year: i32) -> YmdDate {
    let mut y = year;
    let mut mon = 0;
    let mut mday = 0;
    crate::isoweek::isoweekdate2date(isoweek, wday, &mut y, &mut mon, &mut mday);
    YmdDate { year: y, mon, mday }
}
fn seam_isoweek2j(year: i32, week: i32) -> i32 {
    crate::isoweek::isoweek2j(year, week)
}

/// `timestamptz_pl_interval(timestamp, span)` — the `timestamptz + interval`
/// operator (timestamp.c). uuid.c reaches it via `DirectFunctionCall2` from
/// `uuidv7(interval)`. The seam passes `span` by value; the core takes it by
/// reference.
fn seam_timestamptz_pl_interval(
    timestamp: TimestampTz,
    span: types_datetime::Interval,
) -> types_error::PgResult<TimestampTz> {
    crate::timestamp::timestamptz_pl_interval(timestamp, &span)
}

/// `interval_lerp(lo, hi, pct)` — linear interpolation between two intervals
/// (orderedsetaggs.c). Faithful to the C `DirectFunctionCall2` chain:
/// `diff = interval_mi(hi, lo); mul = interval_mul(diff, pct);
/// result = interval_pl(mul, lo)`.
fn seam_interval_lerp(
    lo: types_datetime::Interval,
    hi: types_datetime::Interval,
    pct: f64,
) -> types_error::PgResult<types_datetime::Interval> {
    let diff = crate::interval::interval_mi(&hi, &lo)?;
    let mul = crate::interval::interval_mul(&diff, pct)?;
    crate::interval::interval_pl(&mul, &lo)
}

// ---------------------------------------------------------------------------
// Install every inward seam this unit owns.
// ---------------------------------------------------------------------------

pub fn init_seams() {
    use backend_utils_adt_timestamp_seams as ts;
    ts::get_current_timestamp::set(crate::timestamp::GetCurrentTimestamp);
    ts::timestamp_timestamptz_requires_rewrite::set(
        crate::timestamp::TimestampTimestampTzRequiresRewrite,
    );
    ts::timestamptz_to_time_t::set(crate::convert::timestamptz_to_time_t);
    ts::timestamptz_pl_interval::set(seam_timestamptz_pl_interval);
    ts::interval_lerp::set(seam_interval_lerp);
    ts::parse_recovery_target_time::set(parse_recovery_target_time);
    ts::timestamp_difference::set(timestamp_difference);
    ts::timestamp_difference_exceeds::set(timestamp_difference_exceeds);
    ts::timestamptz_to_str::set(timestamptz_to_str);
    ts::timestamp_difference_exceeds_seconds::set(timestamp_difference_exceeds_seconds);
    ts::timestamp_difference_milliseconds::set(timestamp_difference_milliseconds);
    ts::json_encode_datetime::set(json_encode_datetime);
    ts::timestamp2tm::set(seam_timestamp2tm);
    ts::tm2timestamp::set(seam_tm2timestamp);
    ts::interval2itm::set(seam_interval2itm);
    ts::tm2time::set(seam_tm2time);
    ts::tm2timetz::set(seam_tm2timetz);
    ts::adjust_timestamp_for_typmod::set(seam_adjust_timestamp_for_typmod);
    ts::adjust_time_for_typmod::set(seam_adjust_time_for_typmod);

    // The datetime.c outward seams (`date2j`/`j2date`/`validate_date`/
    // `determine_time_zone_offset`/`determine_time_zone_abbrev_offset`/
    // `decode_timezone_abbrev_prefix`/`convert_time_zone_abbrevs`) were removed:
    // their `seam_*` adapters in `crate::seam_impls` are now `pub` and called
    // directly by the consumers (formatting, timeout). Faithful de-indirection.

    // --- lazy-vacuum driver timestamp reads (vacuumlazy.c logging /
    //     cost-delay; home in vacuumlazy-seams, timestamp.c is their owner) ---
    {
        use backend_access_heap_vacuumlazy_seams as vx;
        vx::get_current_timestamp::set(|| Ok(crate::timestamp::GetCurrentTimestamp()));
        vx::timestamp_difference::set(|start, stop| Ok(timestamp_difference(start, stop)));
        vx::timestamp_difference_exceeds::set(|start, stop, msec| {
            Ok(timestamp_difference_exceeds(start, stop, msec))
        });
    }

    use backend_utils_adt_isoweek_seams as iw;
    iw::date2isoweek::set(seam_date2isoweek);
    iw::date2isoyear::set(seam_date2isoyear);
    iw::date2isoyearday::set(seam_date2isoyearday);
    iw::isoweek2date::set(seam_isoweek2date);
    iw::isoweekdate2date::set(seam_isoweekdate2date);
    iw::isoweek2j::set(seam_isoweek2j);

    // The fmgr builtin layer (date.c / timestamp.c / datetime.c PG_FUNCTION_ARGS
    // shims) is registered alongside the seams.
    crate::fmgr_builtins::register_datetime_builtins();
}
