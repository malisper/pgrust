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
            // DatumGetTimeTzADTP: the by-reference 12-byte {time:i64, zone:i32}.
            let bytes = value.as_ref_bytes();
            let time = TimeTzADT {
                time: i64::from_ne_bytes(bytes[0..8].try_into().unwrap()),
                zone: i32::from_ne_bytes(bytes[8..12].try_into().unwrap()),
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

// ---------------------------------------------------------------------------
// Install every inward seam this unit owns.
// ---------------------------------------------------------------------------

pub fn init_seams() {
    use backend_utils_adt_timestamp_seams as ts;
    ts::get_current_timestamp::set(crate::timestamp::GetCurrentTimestamp);
    ts::parse_recovery_target_time::set(parse_recovery_target_time);
    ts::timestamp_difference::set(timestamp_difference);
    ts::timestamp_difference_exceeds::set(timestamp_difference_exceeds);
    ts::timestamptz_to_str::set(timestamptz_to_str);
    ts::timestamp_difference_exceeds_seconds::set(timestamp_difference_exceeds_seconds);
    ts::timestamp_difference_milliseconds::set(timestamp_difference_milliseconds);
    ts::json_encode_datetime::set(json_encode_datetime);

    backend_utils_adt_datetime_seams::convert_time_zone_abbrevs::set(convert_time_zone_abbrevs);
}
