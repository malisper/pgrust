#![allow(non_snake_case)]

use numutils::{pg_ultostr, pg_ultostr_zeropad};

use crate::calendar::{date2j, j2day, DAYS, MONTHS};
use crate::consts::*;
use crate::settings::date_order;

/// C `AppendSeconds`: writes at `buf[p..]`, returns the new end offset.
/// No NUL terminator; any sign is stripped from sec/fsec.
pub fn AppendSeconds(
    buf: &mut [u8],
    mut p: usize,
    sec: i32,
    fsec: fsec_t,
    precision: i32,
    fillzeros: bool,
) -> usize {
    debug_assert!(precision >= 0);

    if fillzeros {
        p += pg_ultostr_zeropad(&mut buf[p..], sec.unsigned_abs(), 2);
    } else {
        p += pg_ultostr(&mut buf[p..], sec.unsigned_abs());
    }

    if fsec != 0 {
        let mut value = fsec.unsigned_abs();
        let precision = precision as usize;
        buf[p] = b'.';
        p += 1;
        let mut end = p + precision;
        let mut gotnonzero = false;

        // build the fraction in reverse, dropping trailing zeros
        for k in (0..precision).rev() {
            let oldval = value;
            value /= 10;
            let remainder = oldval - value * 10;
            if remainder != 0 {
                gotnonzero = true;
            }
            if gotnonzero {
                buf[p + k] = b'0' + remainder as u8;
            } else {
                end = p + k;
            }
        }

        // nonzero remainder means precision didn't suffice; punt to pg_ultostr
        if value != 0 {
            return p + pg_ultostr(&mut buf[p..], fsec.unsigned_abs());
        }
        end
    } else {
        p
    }
}

fn AppendTimestampSeconds(buf: &mut [u8], p: usize, tm: &pg_tm, fsec: fsec_t) -> usize {
    AppendSeconds(buf, p, tm.tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true)
}

/// C `EncodeTimezone`: appends the numeric zone at `buf[p..]`, returns the new
/// end offset. tz is negated compared to the displayed sign.
pub fn EncodeTimezone(buf: &mut [u8], mut p: usize, tz: i32, style: i32) -> usize {
    let mut sec = tz.unsigned_abs();
    let mut min = sec / SECS_PER_MINUTE as u32;
    sec -= min * SECS_PER_MINUTE as u32;
    let hour = min / MINS_PER_HOUR as u32;
    min -= hour * MINS_PER_HOUR as u32;

    buf[p] = if tz <= 0 { b'+' } else { b'-' };
    p += 1;

    if sec != 0 {
        p += pg_ultostr_zeropad(&mut buf[p..], hour, 2);
        buf[p] = b':';
        p += 1;
        p += pg_ultostr_zeropad(&mut buf[p..], min, 2);
        buf[p] = b':';
        p += 1;
        p += pg_ultostr_zeropad(&mut buf[p..], sec, 2);
    } else if min != 0 || style == USE_XSD_DATES {
        p += pg_ultostr_zeropad(&mut buf[p..], hour, 2);
        buf[p] = b':';
        p += 1;
        p += pg_ultostr_zeropad(&mut buf[p..], min, 2);
    } else {
        p += pg_ultostr_zeropad(&mut buf[p..], hour, 2);
    }
    p
}

#[inline]
fn display_year(year: i32) -> u32 {
    (if year > 0 { year } else { -(year - 1) }) as u32
}

#[inline]
fn put(buf: &mut [u8], p: usize, c: u8) -> usize {
    buf[p] = c;
    p + 1
}

/// C `EncodeDateOnly`. Returns the output length (no NUL).
pub fn EncodeDateOnly(tm: &pg_tm, style: i32, buf: &mut [u8]) -> usize {
    debug_assert!(tm.tm_mon >= 1 && tm.tm_mon <= MONTHS_PER_YEAR);
    let mut p = 0usize;

    match style {
        USE_ISO_DATES | USE_XSD_DATES => {
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
            p = put(buf, p, b'-');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
            p = put(buf, p, b'-');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
        }
        USE_SQL_DATES => {
            if date_order() == DATEORDER_DMY {
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
                p = put(buf, p, b'/');
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
            } else {
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
                p = put(buf, p, b'/');
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
            }
            p = put(buf, p, b'/');
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
        }
        USE_GERMAN_DATES => {
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
            p = put(buf, p, b'.');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
            p = put(buf, p, b'.');
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
        }
        _ => {
            // USE_POSTGRES_DATES: traditional date-only style
            if date_order() == DATEORDER_DMY {
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
                p = put(buf, p, b'-');
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
            } else {
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
                p = put(buf, p, b'-');
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
            }
            p = put(buf, p, b'-');
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
        }
    }

    if tm.tm_year <= 0 {
        buf[p..p + 3].copy_from_slice(b" BC");
        p += 3;
    }
    p
}

/// C `EncodeTimeOnly`. Returns the output length (no NUL).
pub fn EncodeTimeOnly(
    tm: &pg_tm,
    fsec: fsec_t,
    print_tz: bool,
    tz: i32,
    style: i32,
    buf: &mut [u8],
) -> usize {
    let mut p = 0usize;
    p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_hour as u32, 2);
    p = put(buf, p, b':');
    p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_min as u32, 2);
    p = put(buf, p, b':');
    p = AppendSeconds(buf, p, tm.tm_sec, fsec, MAX_TIME_PRECISION, true);
    if print_tz {
        p = EncodeTimezone(buf, p, tz, style);
    }
    p
}

/// C `EncodeDateTime`. Returns the output length (no NUL).
///
/// Supported date styles:
///   Postgres - day mon hh:mm:ss yyyy tz
///   SQL - mm/dd/yyyy hh:mm:ss.ss tz
///   ISO - yyyy-mm-dd hh:mm:ss+/-tz
///   German - dd.mm.yyyy hh:mm:ss tz
///   XSD - yyyy-mm-ddThh:mm:ss.ss+/-tz
pub fn EncodeDateTime(
    tm: &mut pg_tm,
    fsec: fsec_t,
    mut print_tz: bool,
    tz: i32,
    tzn: Option<&[u8]>,
    style: i32,
    buf: &mut [u8],
) -> usize {
    debug_assert!(tm.tm_mon >= 1 && tm.tm_mon <= MONTHS_PER_YEAR);

    // negative tm_isdst means we have no valid time zone translation
    if tm.tm_isdst < 0 {
        print_tz = false;
    }

    let mut p = 0usize;

    match style {
        USE_ISO_DATES | USE_XSD_DATES => {
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
            p = put(buf, p, b'-');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
            p = put(buf, p, b'-');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
            p = put(buf, p, if style == USE_ISO_DATES { b' ' } else { b'T' });
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_hour as u32, 2);
            p = put(buf, p, b':');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_min as u32, 2);
            p = put(buf, p, b':');
            p = AppendTimestampSeconds(buf, p, tm, fsec);
            if print_tz {
                p = EncodeTimezone(buf, p, tz, style);
            }
        }
        USE_SQL_DATES => {
            if date_order() == DATEORDER_DMY {
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
                p = put(buf, p, b'/');
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
            } else {
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
                p = put(buf, p, b'/');
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
            }
            p = put(buf, p, b'/');
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
            p = put(buf, p, b' ');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_hour as u32, 2);
            p = put(buf, p, b':');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_min as u32, 2);
            p = put(buf, p, b':');
            p = AppendTimestampSeconds(buf, p, tm, fsec);
            if print_tz {
                p = append_tzn_or_numeric(buf, p, tzn, tz, style);
            }
        }
        USE_GERMAN_DATES => {
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
            p = put(buf, p, b'.');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mon as u32, 2);
            p = put(buf, p, b'.');
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
            p = put(buf, p, b' ');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_hour as u32, 2);
            p = put(buf, p, b':');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_min as u32, 2);
            p = put(buf, p, b':');
            p = AppendTimestampSeconds(buf, p, tm, fsec);
            if print_tz {
                p = append_tzn_or_numeric(buf, p, tzn, tz, style);
            }
        }
        _ => {
            // USE_POSTGRES_DATES: traditional Postgres style
            let day = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
            tm.tm_wday = j2day(day);
            buf[p..p + 3].copy_from_slice(&DAYS[tm.tm_wday as usize].as_bytes()[..3]);
            p += 3;
            p = put(buf, p, b' ');
            if date_order() == DATEORDER_DMY {
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
                p = put(buf, p, b' ');
                buf[p..p + 3].copy_from_slice(MONTHS[(tm.tm_mon - 1) as usize].as_bytes());
                p += 3;
            } else {
                buf[p..p + 3].copy_from_slice(MONTHS[(tm.tm_mon - 1) as usize].as_bytes());
                p += 3;
                p = put(buf, p, b' ');
                p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_mday as u32, 2);
            }
            p = put(buf, p, b' ');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_hour as u32, 2);
            p = put(buf, p, b':');
            p += pg_ultostr_zeropad(&mut buf[p..], tm.tm_min as u32, 2);
            p = put(buf, p, b':');
            p = AppendTimestampSeconds(buf, p, tm, fsec);
            p = put(buf, p, b' ');
            p += pg_ultostr_zeropad(&mut buf[p..], display_year(tm.tm_year), 4);
            if print_tz {
                match tzn {
                    Some(name) => p = append_tzn(buf, p, name),
                    None => {
                        // no string form: numeric with a leading space so the
                        // output can be re-parsed
                        p = put(buf, p, b' ');
                        p = EncodeTimezone(buf, p, tz, style);
                    }
                }
            }
        }
    }

    if tm.tm_year <= 0 {
        buf[p..p + 3].copy_from_slice(b" BC");
        p += 3;
    }
    p
}

// C `sprintf(str, " %.*s", MAXTZLEN, tzn)`: safe because IANA abbreviations
// are plain ASCII.
fn append_tzn(buf: &mut [u8], mut p: usize, tzn: &[u8]) -> usize {
    p = put(buf, p, b' ');
    let n = tzn.len().min(MAXTZLEN);
    buf[p..p + n].copy_from_slice(&tzn[..n]);
    p + n
}

fn append_tzn_or_numeric(
    buf: &mut [u8],
    p: usize,
    tzn: Option<&[u8]>,
    tz: i32,
    style: i32,
) -> usize {
    match tzn {
        Some(name) => append_tzn(buf, p, name),
        None => EncodeTimezone(buf, p, tz, style),
    }
}

fn put_i64(buf: &mut [u8], p: usize, v: i64) -> usize {
    p + numutils::pg_lltoa(v, &mut buf[p..])
}

fn put_u64(buf: &mut [u8], p: usize, v: u64) -> usize {
    p + numutils::pg_ulltoa_n(v, &mut buf[p..])
}

// C "%02" PRId64 on a nonnegative magnitude
fn put_u64_pad2(buf: &mut [u8], mut p: usize, v: u64) -> usize {
    if v < 10 {
        p = put(buf, p, b'0');
    }
    put_u64(buf, p, v)
}

fn put_str(buf: &mut [u8], p: usize, s: &[u8]) -> usize {
    buf[p..p + s.len()].copy_from_slice(s);
    p + s.len()
}

fn AddISO8601IntPart(buf: &mut [u8], p: usize, value: i64, units: u8) -> usize {
    if value == 0 {
        return p;
    }
    let p = put_i64(buf, p, value);
    put(buf, p, units)
}

fn AddPostgresIntPart(
    buf: &mut [u8],
    mut p: usize,
    value: i64,
    units: &[u8],
    is_zero: &mut bool,
    is_before: &mut bool,
) -> usize {
    if value == 0 {
        return p;
    }
    if !*is_zero {
        p = put(buf, p, b' ');
    }
    if *is_before && value > 0 {
        p = put(buf, p, b'+');
    }
    p = put_i64(buf, p, value);
    p = put(buf, p, b' ');
    p = put_str(buf, p, units);
    if value != 1 {
        p = put(buf, p, b's');
    }
    // Each nonzero field sets is_before for (only) the next one.  This is a
    // tad bizarre but it's how it worked before...
    *is_before = value < 0;
    *is_zero = false;
    p
}

fn AddVerboseIntPart(
    buf: &mut [u8],
    mut p: usize,
    mut value: i64,
    units: &[u8],
    is_zero: &mut bool,
    is_before: &mut bool,
) -> usize {
    if value == 0 {
        return p;
    }
    // first nonzero value sets is_before
    if *is_zero {
        *is_before = value < 0;
        value = value.wrapping_abs();
    } else if *is_before {
        value = -value;
    }
    p = put(buf, p, b' ');
    p = put_i64(buf, p, value);
    p = put(buf, p, b' ');
    p = put_str(buf, p, units);
    if value != 1 {
        p = put(buf, p, b's');
    }
    *is_zero = false;
    p
}

/// C `EncodeInterval`. Returns the output length (no NUL).
pub fn EncodeInterval(itm: &pg_itm, style: i32, buf: &mut [u8]) -> usize {
    let mut p = 0usize;
    let mut year = itm.tm_year;
    let mut mon = itm.tm_mon;
    let mut mday = itm.tm_mday as i64; // tm_mday could be INT_MIN
    let mut hour = itm.tm_hour;
    let mut min = itm.tm_min;
    let mut sec = itm.tm_sec;
    let mut fsec = itm.tm_usec;
    let mut is_before = false;
    let mut is_zero = true;

    // The sign of year and month are guaranteed to match, since they are
    // stored internally as "month".
    match style {
        s if s == INTSTYLE_SQL_STANDARD => {
            let has_negative =
                year < 0 || mon < 0 || mday < 0 || hour < 0 || min < 0 || sec < 0 || fsec < 0;
            let has_positive =
                year > 0 || mon > 0 || mday > 0 || hour > 0 || min > 0 || sec > 0 || fsec > 0;
            let has_year_month = year != 0 || mon != 0;
            let has_day_time = mday != 0 || hour != 0 || min != 0 || sec != 0 || fsec != 0;
            let has_day = mday != 0;
            let sql_standard_value =
                !(has_negative && has_positive) && !(has_year_month && has_day_time);

            // SQL Standard wants only one "<sign>" preceding the whole
            // interval; not possible with mixed signs.
            if has_negative && sql_standard_value {
                p = put(buf, p, b'-');
                year = -year;
                mon = -mon;
                mday = -mday;
                hour = -hour;
                min = -min;
                sec = -sec;
                fsec = -fsec;
            }

            if !has_negative && !has_positive {
                p = put(buf, p, b'0');
            } else if !sql_standard_value {
                // force signs on all fields to avoid ambiguity
                let year_sign = if year < 0 || mon < 0 { b'-' } else { b'+' };
                let day_sign = if mday < 0 { b'-' } else { b'+' };
                let sec_sign =
                    if hour < 0 || min < 0 || sec < 0 || fsec < 0 { b'-' } else { b'+' };

                p = put(buf, p, year_sign);
                p = put_u64(buf, p, year.unsigned_abs() as u64);
                p = put(buf, p, b'-');
                p = put_u64(buf, p, mon.unsigned_abs() as u64);
                p = put(buf, p, b' ');
                p = put(buf, p, day_sign);
                p = put_u64(buf, p, mday.unsigned_abs());
                p = put(buf, p, b' ');
                p = put(buf, p, sec_sign);
                p = put_u64(buf, p, hour.unsigned_abs());
                p = put(buf, p, b':');
                p += pg_ultostr_zeropad(&mut buf[p..], min.unsigned_abs(), 2);
                p = put(buf, p, b':');
                p = AppendSeconds(buf, p, sec, fsec, MAX_INTERVAL_PRECISION, true);
            } else if has_year_month {
                p = put_i64(buf, p, year as i64);
                p = put(buf, p, b'-');
                p = put_i64(buf, p, mon as i64);
            } else if has_day {
                p = put_i64(buf, p, mday);
                p = put(buf, p, b' ');
                p = put_i64(buf, p, hour);
                p = put(buf, p, b':');
                p += pg_ultostr_zeropad(&mut buf[p..], min.unsigned_abs(), 2);
                p = put(buf, p, b':');
                p = AppendSeconds(buf, p, sec, fsec, MAX_INTERVAL_PRECISION, true);
            } else {
                p = put_i64(buf, p, hour);
                p = put(buf, p, b':');
                p += pg_ultostr_zeropad(&mut buf[p..], min.unsigned_abs(), 2);
                p = put(buf, p, b':');
                p = AppendSeconds(buf, p, sec, fsec, MAX_INTERVAL_PRECISION, true);
            }
        }

        s if s == INTSTYLE_ISO_8601 => {
            // special-case zero to avoid printing nothing
            if year == 0 && mon == 0 && mday == 0 && hour == 0 && min == 0 && sec == 0 && fsec == 0
            {
                return put_str(buf, p, b"PT0S");
            }
            p = put(buf, p, b'P');
            p = AddISO8601IntPart(buf, p, year as i64, b'Y');
            p = AddISO8601IntPart(buf, p, mon as i64, b'M');
            p = AddISO8601IntPart(buf, p, mday, b'D');
            if hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                p = put(buf, p, b'T');
            }
            p = AddISO8601IntPart(buf, p, hour, b'H');
            p = AddISO8601IntPart(buf, p, min as i64, b'M');
            if sec != 0 || fsec != 0 {
                if sec < 0 || fsec < 0 {
                    p = put(buf, p, b'-');
                }
                p = AppendSeconds(buf, p, sec, fsec, MAX_INTERVAL_PRECISION, false);
                p = put(buf, p, b'S');
            }
        }

        s if s == INTSTYLE_POSTGRES => {
            p = AddPostgresIntPart(buf, p, year as i64, b"year", &mut is_zero, &mut is_before);
            // "mon" (not "month") kept for backward compatibility, per C
            p = AddPostgresIntPart(buf, p, mon as i64, b"mon", &mut is_zero, &mut is_before);
            p = AddPostgresIntPart(buf, p, mday, b"day", &mut is_zero, &mut is_before);
            if is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                let minus = hour < 0 || min < 0 || sec < 0 || fsec < 0;
                if !is_zero {
                    p = put(buf, p, b' ');
                }
                if minus {
                    p = put(buf, p, b'-');
                } else if is_before {
                    p = put(buf, p, b'+');
                }
                p = put_u64_pad2(buf, p, hour.unsigned_abs());
                p = put(buf, p, b':');
                p += pg_ultostr_zeropad(&mut buf[p..], min.unsigned_abs(), 2);
                p = put(buf, p, b':');
                p = AppendSeconds(buf, p, sec, fsec, MAX_INTERVAL_PRECISION, true);
            }
        }

        // INTSTYLE_POSTGRES_VERBOSE and default
        _ => {
            p = put(buf, p, b'@');
            p = AddVerboseIntPart(buf, p, year as i64, b"year", &mut is_zero, &mut is_before);
            p = AddVerboseIntPart(buf, p, mon as i64, b"mon", &mut is_zero, &mut is_before);
            p = AddVerboseIntPart(buf, p, mday, b"day", &mut is_zero, &mut is_before);
            p = AddVerboseIntPart(buf, p, hour, b"hour", &mut is_zero, &mut is_before);
            p = AddVerboseIntPart(buf, p, min as i64, b"min", &mut is_zero, &mut is_before);
            if sec != 0 || fsec != 0 {
                p = put(buf, p, b' ');
                if sec < 0 || (sec == 0 && fsec < 0) {
                    if is_zero {
                        is_before = true;
                    } else if !is_before {
                        p = put(buf, p, b'-');
                    }
                } else if is_before {
                    p = put(buf, p, b'-');
                }
                p = AppendSeconds(buf, p, sec, fsec, MAX_INTERVAL_PRECISION, false);
                // we output "ago", not negatives, so use abs()
                p = put_str(buf, p, b" sec");
                if sec.unsigned_abs() != 1 || fsec != 0 {
                    p = put(buf, p, b's');
                }
                is_zero = false;
            }
            // identically zero? then put in a unitless zero
            if is_zero {
                p = put_str(buf, p, b" 0");
            }
            if is_before {
                p = put_str(buf, p, b" ago");
            }
        }
    }
    p
}
