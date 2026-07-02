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
