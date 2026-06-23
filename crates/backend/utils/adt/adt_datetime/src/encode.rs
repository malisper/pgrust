//! Date/time *encoders*: the ENCODE half of `src/backend/utils/adt/datetime.c`
//! (plus `EncodeSpecial{Date,Timestamp}` from `date.c`/`timestamp.c`).
//!
//! These are ported faithfully from the C originals, but instead of writing
//! into a caller-supplied `char *` buffer they build into a Rust [`String`]
//! that the caller passes by `&mut`.  The number-formatting primitives come
//! from `numutils` (`pg_ultostr_zeropad`/`pg_ultostr`), which
//! return owned `String`s, so we append their results rather than advancing a
//! raw pointer.  The visible byte-for-byte output is identical to the C code;
//! these strings drive `*_out` parity for the date/time types.

/// `pg_ultostr_zeropad(value, minwidth)` — String-returning wrapper over the
/// buffer form `numutils::pg_ultostr_zeropad`. A `u32`
/// renders in at most 10 digits, and `minwidth` is always small here, so a
/// fixed 32-byte buffer cannot overflow.
fn pg_ultostr_zeropad(value: u32, minwidth: i32) -> String {
    let mut buf = [0u8; 32];
    let n = numutils::pg_ultostr_zeropad(&mut buf, value, minwidth);
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// `pg_ultostr(value)` — String-returning wrapper over the buffer form.
fn pg_ultostr(value: u32) -> String {
    let mut buf = [0u8; 32];
    let n = numutils::pg_ultostr(&mut buf, value);
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

use types_datetime::{
    pg_itm, DATEORDER_DMY, DT_NOBEGIN, DT_NOEND, INTSTYLE_ISO_8601, INTSTYLE_POSTGRES,
    INTSTYLE_SQL_STANDARD, MAX_INTERVAL_PRECISION, MAX_TIMESTAMP_PRECISION, MAX_TIME_PRECISION,
    MINS_PER_HOUR, MONTHS_PER_YEAR, SECS_PER_MINUTE, USE_GERMAN_DATES, USE_ISO_DATES,
    USE_SQL_DATES, USE_XSD_DATES,
};
use types_datetime::{fsec_t, DateADT, Timestamp};

use pgtime::pg_tm;

use crate::calendar::{date2j, j2day};
use crate::consts::{EARLY, LATE};
use crate::settings::date_order;
use crate::tables::{days, months};

/// `MAXTZLEN` -- max textual TZ name length, not counting the trailing NUL.
/// (`src/include/miscadmin.h`)
const MAXTZLEN: usize = 10;

/// `DATEVAL_NOBEGIN` -- reserved DateADT for `-infinity`. (`utils/date.h`)
const DATEVAL_NOBEGIN: DateADT = i32::MIN;
/// `DATEVAL_NOEND` -- reserved DateADT for `infinity`. (`utils/date.h`)
const DATEVAL_NOEND: DateADT = i32::MAX;

// ---------------------------------------------------------------------------
// AppendSeconds / AppendTimestampSeconds
// ---------------------------------------------------------------------------

/// `AppendSeconds()` -- append seconds and fractional seconds (if any) to `cp`.
///
/// `precision` is the max number of fraction digits; `fillzeros` pads the
/// integral seconds to two digits.  Any sign is stripped from `sec`/`fsec`.
///
/// (`utils/adt/datetime.c`)
fn AppendSeconds(cp: &mut String, sec: i32, fsec: fsec_t, precision: i32, fillzeros: bool) {
    debug_assert!(precision >= 0);

    if fillzeros {
        cp.push_str(&pg_ultostr_zeropad(sec.unsigned_abs(), 2));
    } else {
        cp.push_str(&pg_ultostr(sec.unsigned_abs()));
    }

    /* fsec_t is just an int32 */
    if fsec != 0 {
        // C: int32 value = abs(fsec).  Hold the magnitude as u32 (unsigned_abs)
        // so the loop arithmetic matches C exactly without a debug-mode panic on
        // i32::MIN.
        let mut value: u32 = fsec.unsigned_abs();

        cp.push('.');

        /*
         * Append the fractional seconds part.  Note that we don't want any
         * trailing zeros here, so since we're building the number in reverse
         * we'll skip appending zeros until we've output a non-zero digit.
         *
         * We build `precision` digit slots, then keep only up to `end` of
         * them (mirroring the C `end = &cp[...]` bookkeeping that trims
         * trailing zeros).  `precision` is bounded by MAX_*_PRECISION (<= 6),
         * a compile-time constant, so a fixed-size stack buffer suffices and
         * no data-derived allocation occurs.
         */
        let precision = precision as usize;
        debug_assert!(precision <= MAX_TIMESTAMP_PRECISION as usize);
        let mut digits = [b'0'; MAX_TIMESTAMP_PRECISION as usize];
        let digits = &mut digits[..precision];
        let mut end = precision; // number of digits to keep
        let mut gotnonzero = false;

        // C loop runs with the post-decremented index precision-1 .. 0.
        for idx in (0..precision).rev() {
            let oldval = value;
            value /= 10;
            let remainder = oldval - value * 10;

            if remainder != 0 {
                gotnonzero = true;
            }

            if gotnonzero {
                digits[idx] = b'0' + remainder as u8;
            } else {
                end = idx;
            }
        }

        /*
         * If we still have a non-zero value then precision must have not been
         * enough to print the number.  We punt the problem to pg_ultostr(),
         * which will generate a correct answer in the minimum valid width.
         */
        if value != 0 {
            cp.push_str(&pg_ultostr(fsec.unsigned_abs()));
            return;
        }

        // SAFETY: digits are all ASCII '0'..'9'.
        cp.push_str(core::str::from_utf8(&digits[..end]).unwrap());
    }
}

/// `AppendTimestampSeconds()` -- variant of [`AppendSeconds`] specialized to
/// the timestamp case.  (`utils/adt/datetime.c`)
fn AppendTimestampSeconds(cp: &mut String, tm: &pg_tm, fsec: fsec_t) {
    AppendSeconds(cp, tm.tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true);
}

// ---------------------------------------------------------------------------
// EncodeTimezone
// ---------------------------------------------------------------------------

/// `EncodeTimezone()` -- append the representation of a numeric timezone offset
/// (`+-HH[:MM[:SS]]`) to `str`.  (`utils/adt/datetime.c`)
fn EncodeTimezone(str: &mut String, tz: i32, style: i32) {
    // C: int sec = abs(tz); then signed division by SECS_PER_MINUTE/MINS_PER_HOUR.
    // The magnitude is always non-negative, so we carry it in u32 (unsigned_abs)
    // to mirror C's abs() wrap semantics without a debug-mode panic on i32::MIN.
    let secs_per_minute = SECS_PER_MINUTE as u32;
    let mins_per_hour = MINS_PER_HOUR as u32;
    let mut sec = tz.unsigned_abs();
    let mut min = sec / secs_per_minute;
    sec -= min * secs_per_minute;
    let hour = min / mins_per_hour;
    min -= hour * mins_per_hour;

    /* TZ is negated compared to sign we wish to display ... */
    str.push(if tz <= 0 { '+' } else { '-' });

    if sec != 0 {
        str.push_str(&pg_ultostr_zeropad(hour, 2));
        str.push(':');
        str.push_str(&pg_ultostr_zeropad(min, 2));
        str.push(':');
        str.push_str(&pg_ultostr_zeropad(sec, 2));
    } else if min != 0 || style == USE_XSD_DATES {
        str.push_str(&pg_ultostr_zeropad(hour, 2));
        str.push(':');
        str.push_str(&pg_ultostr_zeropad(min, 2));
    } else {
        str.push_str(&pg_ultostr_zeropad(hour, 2));
    }
}

// ---------------------------------------------------------------------------
// EncodeDateOnly
// ---------------------------------------------------------------------------

/// Format the (year, BC-adjusted) value the same way the C code does:
/// `(tm_year > 0) ? tm_year : -(tm_year - 1)`.
#[inline]
fn year_for_display(tm_year: i32) -> u32 {
    (if tm_year > 0 { tm_year } else { -(tm_year - 1) }) as u32
}

/// `EncodeDateOnly()` -- encode a date as local time, per `DateStyle`.
///
/// (`utils/adt/datetime.c`)
pub fn EncodeDateOnly(tm: &pg_tm, style: i32, str: &mut String) {
    debug_assert!(tm.tm_mon >= 1 && tm.tm_mon <= MONTHS_PER_YEAR);

    match style {
        s if s == USE_ISO_DATES || s == USE_XSD_DATES => {
            /* compatible with ISO date formats */
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));
            str.push('-');
            str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
            str.push('-');
            str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
        }

        s if s == USE_SQL_DATES => {
            /* compatible with Oracle/Ingres date formats */
            if date_order() == DATEORDER_DMY {
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
                str.push('/');
                str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
            } else {
                str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
                str.push('/');
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
            }
            str.push('/');
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));
        }

        s if s == USE_GERMAN_DATES => {
            /* German-style date format */
            str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
            str.push('.');
            str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
            str.push('.');
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));
        }

        // USE_POSTGRES_DATES and default
        _ => {
            /* traditional date-only style for Postgres */
            if date_order() == DATEORDER_DMY {
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
                str.push('-');
                str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
            } else {
                str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
                str.push('-');
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
            }
            str.push('-');
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));
        }
    }

    if tm.tm_year <= 0 {
        str.push_str(" BC");
    }
}

// ---------------------------------------------------------------------------
// EncodeTimeOnly
// ---------------------------------------------------------------------------

/// `EncodeTimeOnly()` -- encode the time fields only (`h:m:s.fff` plus optional
/// timezone).
///
/// `print_tz` selects whether to include a time zone (time vs timetz); `tz` is
/// the numeric offset; `style` is the date style.
///
/// (`utils/adt/datetime.c`)
pub fn EncodeTimeOnly(tm: &pg_tm, fsec: fsec_t, print_tz: bool, tz: i32, style: i32, str: &mut String) {
    str.push_str(&pg_ultostr_zeropad(tm.tm_hour as u32, 2));
    str.push(':');
    str.push_str(&pg_ultostr_zeropad(tm.tm_min as u32, 2));
    str.push(':');
    AppendSeconds(str, tm.tm_sec, fsec, MAX_TIME_PRECISION, true);
    if print_tz {
        EncodeTimezone(str, tz, style);
    }
}

// ---------------------------------------------------------------------------
// EncodeDateTime
// ---------------------------------------------------------------------------

/// `EncodeDateTime()` -- encode date and time interpreted as local time, per
/// `DateStyle`.
///
/// `print_tz` selects whether to include a time zone (timestamp vs
/// timestamptz); `tz` is the numeric offset; `tzn`, if `Some`, is the textual
/// time zone used instead of `tz` by some styles; `style` is the date style.
///
/// Note: for `USE_POSTGRES_DATES` this mutates `tm.tm_wday` (via [`j2day`]),
/// matching the C original, hence the `&mut pg_tm`.
///
/// (`utils/adt/datetime.c`)
pub fn EncodeDateTime(
    tm: &mut pg_tm,
    fsec: fsec_t,
    mut print_tz: bool,
    tz: i32,
    tzn: Option<&str>,
    style: i32,
    str: &mut String,
) {
    debug_assert!(tm.tm_mon >= 1 && tm.tm_mon <= MONTHS_PER_YEAR);

    /*
     * Negative tm_isdst means we have no valid time zone translation.
     */
    if tm.tm_isdst < 0 {
        print_tz = false;
    }

    match style {
        s if s == USE_ISO_DATES || s == USE_XSD_DATES => {
            /* Compatible with ISO-8601 date formats */
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));
            str.push('-');
            str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
            str.push('-');
            str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
            str.push(if style == USE_ISO_DATES { ' ' } else { 'T' });
            str.push_str(&pg_ultostr_zeropad(tm.tm_hour as u32, 2));
            str.push(':');
            str.push_str(&pg_ultostr_zeropad(tm.tm_min as u32, 2));
            str.push(':');
            AppendTimestampSeconds(str, tm, fsec);
            if print_tz {
                EncodeTimezone(str, tz, style);
            }
        }

        s if s == USE_SQL_DATES => {
            /* Compatible with Oracle/Ingres date formats */
            if date_order() == DATEORDER_DMY {
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
                str.push('/');
                str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
            } else {
                str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
                str.push('/');
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
            }
            str.push('/');
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));
            str.push(' ');
            str.push_str(&pg_ultostr_zeropad(tm.tm_hour as u32, 2));
            str.push(':');
            str.push_str(&pg_ultostr_zeropad(tm.tm_min as u32, 2));
            str.push(':');
            AppendTimestampSeconds(str, tm, fsec);

            /*
             * Note: the uses of %.*s in this function would be risky if the
             * timezone names ever contain non-ASCII characters, since we are
             * not being careful to do encoding-aware clipping.  However, all
             * TZ abbreviations in the IANA database are plain ASCII.
             */
            if print_tz {
                if let Some(tzn) = tzn {
                    str.push(' ');
                    push_clipped_tzn(str, tzn);
                } else {
                    EncodeTimezone(str, tz, style);
                }
            }
        }

        s if s == USE_GERMAN_DATES => {
            /* German variant on European style */
            str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
            str.push('.');
            str.push_str(&pg_ultostr_zeropad(tm.tm_mon as u32, 2));
            str.push('.');
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));
            str.push(' ');
            str.push_str(&pg_ultostr_zeropad(tm.tm_hour as u32, 2));
            str.push(':');
            str.push_str(&pg_ultostr_zeropad(tm.tm_min as u32, 2));
            str.push(':');
            AppendTimestampSeconds(str, tm, fsec);

            if print_tz {
                if let Some(tzn) = tzn {
                    str.push(' ');
                    push_clipped_tzn(str, tzn);
                } else {
                    EncodeTimezone(str, tz, style);
                }
            }
        }

        // USE_POSTGRES_DATES and default
        _ => {
            /* Backward-compatible with traditional Postgres abstime dates */
            let day = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
            tm.tm_wday = j2day(day);
            str.push_str(
                days[tm.tm_wday as usize]
                    .get(..3)
                    .unwrap_or(days[tm.tm_wday as usize]),
            );
            str.push(' ');
            if date_order() == DATEORDER_DMY {
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
                str.push(' ');
                str.push_str(months[(tm.tm_mon - 1) as usize]);
            } else {
                str.push_str(months[(tm.tm_mon - 1) as usize]);
                str.push(' ');
                str.push_str(&pg_ultostr_zeropad(tm.tm_mday as u32, 2));
            }
            str.push(' ');
            str.push_str(&pg_ultostr_zeropad(tm.tm_hour as u32, 2));
            str.push(':');
            str.push_str(&pg_ultostr_zeropad(tm.tm_min as u32, 2));
            str.push(':');
            AppendTimestampSeconds(str, tm, fsec);
            str.push(' ');
            str.push_str(&pg_ultostr_zeropad(year_for_display(tm.tm_year), 4));

            if print_tz {
                if let Some(tzn) = tzn {
                    str.push(' ');
                    push_clipped_tzn(str, tzn);
                } else {
                    /*
                     * We have a time zone, but no string version. Use the
                     * numeric form, but be sure to include a leading space to
                     * avoid formatting something which would be rejected by
                     * the date/time parser later. - thomas 2001-10-19
                     */
                    str.push(' ');
                    EncodeTimezone(str, tz, style);
                }
            }
        }
    }

    if tm.tm_year <= 0 {
        str.push_str(" BC");
    }
}

/// Append a textual TZ name, clipped to `MAXTZLEN` bytes -- mirrors the C
/// `sprintf(str, " %.*s", MAXTZLEN, tzn)` clipping (the leading space is added
/// by the caller).  All IANA TZ abbreviations are plain ASCII, matching the C
/// comment about encoding-unaware clipping.
fn push_clipped_tzn(str: &mut String, tzn: &str) {
    let end = clip_byte_len(tzn, MAXTZLEN);
    str.push_str(&tzn[..end]);
}

/// Largest byte length `<= max` that is a char boundary of `s` (so the clip is
/// ASCII-equivalent to C `%.*s` for ASCII input, but never splits a UTF-8
/// codepoint for safety).
fn clip_byte_len(s: &str, max: usize) -> usize {
    if s.len() <= max {
        return s.len();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

// ---------------------------------------------------------------------------
// EncodeInterval and its Add*IntPart helpers
// ---------------------------------------------------------------------------

/// `AddISO8601IntPart()` -- append an ISO-8601-style interval field, but only
/// if `value` isn't zero.  (`utils/adt/datetime.c`)
fn AddISO8601IntPart(cp: &mut String, value: i64, units: char) {
    if value == 0 {
        return;
    }
    cp.push_str(&value.to_string());
    cp.push(units);
}

/// `AddPostgresIntPart()` -- append a Postgres-style interval field, but only
/// if `value` isn't zero.  (`utils/adt/datetime.c`)
fn AddPostgresIntPart(cp: &mut String, value: i64, units: &str, is_zero: &mut bool, is_before: &mut bool) {
    if value == 0 {
        return;
    }
    cp.push_str(if !*is_zero { " " } else { "" });
    cp.push_str(if *is_before && value > 0 { "+" } else { "" });
    cp.push_str(&value.to_string());
    cp.push(' ');
    cp.push_str(units);
    cp.push_str(if value != 1 { "s" } else { "" });

    /*
     * Each nonzero field sets is_before for (only) the next one.  This is a
     * tad bizarre but it's how it worked before...
     */
    *is_before = value < 0;
    *is_zero = false;
}

/// `AddVerboseIntPart()` -- append a verbose-style interval field, but only if
/// `value` isn't zero.  (`utils/adt/datetime.c`)
fn AddVerboseIntPart(cp: &mut String, mut value: i64, units: &str, is_zero: &mut bool, is_before: &mut bool) {
    if value == 0 {
        return;
    }
    /* first nonzero value sets is_before */
    if *is_zero {
        *is_before = value < 0;
        // C: value = i64abs(value) (llabs); wrapping_abs mirrors C's in-practice
        // wrap at i64::MIN without panicking, and keeps the i64 type for reuse.
        value = value.wrapping_abs();
    } else if *is_before {
        value = -value;
    }
    cp.push(' ');
    cp.push_str(&value.to_string());
    cp.push(' ');
    cp.push_str(units);
    cp.push_str(if value == 1 { "" } else { "s" });
    *is_zero = false;
}

/// `EncodeInterval()` -- interpret a time structure as a delta time and convert
/// to string, per `IntervalStyle`.
///
/// Supports "traditional Postgres", verbose, SQL-standard, and ISO-8601 styles.
///
/// (`utils/adt/datetime.c`)
pub fn EncodeInterval(itm: &pg_itm, style: i32, str: &mut String) {
    let cp = str;
    let mut year: i32 = itm.tm_year;
    let mut mon: i32 = itm.tm_mon;
    let mut mday: i64 = itm.tm_mday as i64; /* tm_mday could be INT_MIN */
    let mut hour: i64 = itm.tm_hour;
    let mut min: i32 = itm.tm_min;
    let mut sec: i32 = itm.tm_sec;
    let mut fsec: i32 = itm.tm_usec;
    let mut is_before = false;
    let mut is_zero = true;

    /*
     * The sign of year and month are guaranteed to match, since they are
     * stored internally as "month". But we'll need to check for is_before and
     * is_zero when determining the signs of day and hour/minute/seconds
     * fields.
     */
    match style {
        /* SQL Standard interval format */
        s if s == INTSTYLE_SQL_STANDARD => {
            let has_negative =
                year < 0 || mon < 0 || mday < 0 || hour < 0 || min < 0 || sec < 0 || fsec < 0;
            let has_positive =
                year > 0 || mon > 0 || mday > 0 || hour > 0 || min > 0 || sec > 0 || fsec > 0;
            let has_year_month = year != 0 || mon != 0;
            let has_day_time = mday != 0 || hour != 0 || min != 0 || sec != 0 || fsec != 0;
            let has_day = mday != 0;
            let sql_standard_value =
                !(has_negative && has_positive || has_year_month && has_day_time);

            /*
             * SQL Standard wants only 1 "<sign>" preceding the whole
             * interval ... but can't do that if mixed signs.
             */
            if has_negative && sql_standard_value {
                cp.push('-');
                year = -year;
                mon = -mon;
                mday = -mday;
                hour = -hour;
                min = -min;
                sec = -sec;
                fsec = -fsec;
            }

            if !has_negative && !has_positive {
                cp.push('0');
            } else if !sql_standard_value {
                /*
                 * For non sql-standard interval values, force outputting
                 * the signs to avoid ambiguities with intervals with
                 * mixed sign components.
                 */
                let year_sign = if year < 0 || mon < 0 { '-' } else { '+' };
                let day_sign = if mday < 0 { '-' } else { '+' };
                let sec_sign = if hour < 0 || min < 0 || sec < 0 || fsec < 0 {
                    '-'
                } else {
                    '+'
                };

                // C uses abs()/i64abs() to print magnitudes; use unsigned_abs()
                // to mirror C's wrap behavior (and avoid a debug-mode panic on
                // i32::MIN / i64::MIN) rather than the panicking signed .abs().
                cp.push(year_sign);
                cp.push_str(&year.unsigned_abs().to_string());
                cp.push('-');
                cp.push_str(&mon.unsigned_abs().to_string());
                cp.push(' ');
                cp.push(day_sign);
                cp.push_str(&mday.unsigned_abs().to_string());
                cp.push(' ');
                cp.push(sec_sign);
                cp.push_str(&hour.unsigned_abs().to_string());
                cp.push(':');
                cp.push_str(&pg_ultostr_zeropad(min.unsigned_abs(), 2));
                cp.push(':');
                AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            } else if has_year_month {
                cp.push_str(&year.to_string());
                cp.push('-');
                cp.push_str(&mon.to_string());
            } else if has_day {
                cp.push_str(&mday.to_string());
                cp.push(' ');
                cp.push_str(&hour.to_string());
                cp.push(':');
                cp.push_str(&pg_ultostr_zeropad(min.unsigned_abs(), 2));
                cp.push(':');
                AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            } else {
                cp.push_str(&hour.to_string());
                cp.push(':');
                cp.push_str(&pg_ultostr_zeropad(min.unsigned_abs(), 2));
                cp.push(':');
                AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            }
        }

        /* ISO 8601 "time-intervals by duration only" */
        s if s == INTSTYLE_ISO_8601 => {
            /* special-case zero to avoid printing nothing */
            if year == 0 && mon == 0 && mday == 0 && hour == 0 && min == 0 && sec == 0 && fsec == 0 {
                cp.push_str("PT0S");
            } else {
                cp.push('P');
                AddISO8601IntPart(cp, year as i64, 'Y');
                AddISO8601IntPart(cp, mon as i64, 'M');
                AddISO8601IntPart(cp, mday, 'D');
                if hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                    cp.push('T');
                }
                AddISO8601IntPart(cp, hour, 'H');
                AddISO8601IntPart(cp, min as i64, 'M');
                if sec != 0 || fsec != 0 {
                    if sec < 0 || fsec < 0 {
                        cp.push('-');
                    }
                    AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                    cp.push('S');
                }
            }
        }

        /* Compatible with postgresql < 8.4 when DateStyle = 'iso' */
        s if s == INTSTYLE_POSTGRES => {
            AddPostgresIntPart(cp, year as i64, "year", &mut is_zero, &mut is_before);

            /*
             * Ideally we should spell out "month" like we do for "year" and
             * "day".  However, for backward compatibility, we can't easily
             * fix this.  bjm 2011-05-24
             */
            AddPostgresIntPart(cp, mon as i64, "mon", &mut is_zero, &mut is_before);
            AddPostgresIntPart(cp, mday, "day", &mut is_zero, &mut is_before);
            if is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                let minus = hour < 0 || min < 0 || sec < 0 || fsec < 0;

                cp.push_str(if is_zero { "" } else { " " });
                cp.push_str(if minus {
                    "-"
                } else if is_before {
                    "+"
                } else {
                    ""
                });
                // C: "%02" PRId64 on int64 hour (i64abs).  Use a 64-bit-wide
                // zero-padded formatter rather than narrowing i64 -> u32 so we
                // mirror C exactly even for out-of-u32-range magnitudes.
                cp.push_str(&format!("{:02}", hour.unsigned_abs()));
                cp.push(':');
                cp.push_str(&pg_ultostr_zeropad(min.unsigned_abs(), 2));
                cp.push(':');
                AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            }
        }

        /* Compatible with postgresql < 8.4 when DateStyle != 'iso' */
        // INTSTYLE_POSTGRES_VERBOSE and default
        _ => {
            cp.push('@');
            AddVerboseIntPart(cp, year as i64, "year", &mut is_zero, &mut is_before);
            AddVerboseIntPart(cp, mon as i64, "mon", &mut is_zero, &mut is_before);
            AddVerboseIntPart(cp, mday, "day", &mut is_zero, &mut is_before);
            AddVerboseIntPart(cp, hour, "hour", &mut is_zero, &mut is_before);
            AddVerboseIntPart(cp, min as i64, "min", &mut is_zero, &mut is_before);
            if sec != 0 || fsec != 0 {
                cp.push(' ');
                if sec < 0 || (sec == 0 && fsec < 0) {
                    if is_zero {
                        is_before = true;
                    } else if !is_before {
                        cp.push('-');
                    }
                } else if is_before {
                    cp.push('-');
                }
                AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                /* We output "ago", not negatives, so use abs(). */
                cp.push_str(" sec");
                cp.push_str(if sec.unsigned_abs() != 1 || fsec != 0 {
                    "s"
                } else {
                    ""
                });
                is_zero = false;
            }
            /* identically zero? then put in a unitless zero... */
            if is_zero {
                cp.push_str(" 0");
            }
            if is_before {
                cp.push_str(" ago");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// EncodeSpecialTimestamp / EncodeSpecialDate
// ---------------------------------------------------------------------------

/// `EncodeSpecialTimestamp()` -- convert a reserved (infinity) timestamp to a
/// string.  (`utils/adt/timestamp.c`)
///
/// Returns `true` if `dt` was a reserved value (and `str` was written), `false`
/// otherwise (the C original `elog(ERROR)`s in that "shouldn't happen" case).
pub fn EncodeSpecialTimestamp(dt: Timestamp, str: &mut String) -> bool {
    if dt == DT_NOBEGIN {
        str.push_str(EARLY);
        true
    } else if dt == DT_NOEND {
        str.push_str(LATE);
        true
    } else {
        /* shouldn't happen */
        false
    }
}

/// `EncodeSpecialDate()` -- convert a reserved (infinity) date to a string.
/// (`utils/adt/date.c`)
///
/// Returns `true` if `dt` was a reserved value (and `str` was written), `false`
/// otherwise (the C original `elog(ERROR)`s in that "shouldn't happen" case).
pub fn EncodeSpecialDate(dt: DateADT, str: &mut String) -> bool {
    if dt == DATEVAL_NOBEGIN {
        str.push_str(EARLY);
        true
    } else if dt == DATEVAL_NOEND {
        str.push_str(LATE);
        true
    } else {
        /* shouldn't happen */
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Crate-wide serialization lock for the process-wide `DateOrder` atomic.
    /// See `settings::DATE_ORDER_TEST_LOCK`.  The default cargo test runner runs
    /// tests in parallel, so every test that mutates `DateOrder` and asserts on
    /// output derived from it holds this single lock to avoid racing with the
    /// settings-module tests (and each other) on the shared global.
    use crate::settings::DATE_ORDER_TEST_LOCK as DATE_ORDER_LOCK;
    use crate::settings::{date_order, set_date_order};
    use types_datetime::{
        DATEORDER_DMY, DATEORDER_MDY, INTSTYLE_POSTGRES_VERBOSE, USE_POSTGRES_DATES,
    };

    /// Build a `pg_tm` with the time fields we use throughout the tests.
    fn tm_2024_01_15() -> pg_tm {
        pg_tm {
            tm_year: 2024,
            tm_mon: 1,
            tm_mday: 15,
            tm_hour: 10,
            tm_min: 30,
            tm_sec: 45,
            tm_isdst: 0,
            ..pg_tm::default()
        }
    }

    const FSEC: fsec_t = 123_456;

    // -- EncodeDateOnly under each DateStyle -------------------------------

    #[test]
    fn encode_date_only_all_styles() {
        let _guard = DATE_ORDER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved = date_order();
        set_date_order(DATEORDER_MDY);
        let tm = tm_2024_01_15();

        let mut s = String::new();
        EncodeDateOnly(&tm, USE_ISO_DATES, &mut s);
        assert_eq!(s, "2024-01-15");

        let mut s = String::new();
        EncodeDateOnly(&tm, USE_POSTGRES_DATES, &mut s);
        assert_eq!(s, "01-15-2024");

        let mut s = String::new();
        EncodeDateOnly(&tm, USE_SQL_DATES, &mut s);
        assert_eq!(s, "01/15/2024");

        let mut s = String::new();
        EncodeDateOnly(&tm, USE_GERMAN_DATES, &mut s);
        assert_eq!(s, "15.01.2024");

        set_date_order(saved);
    }

    // -- EncodeDateTime under each DateStyle -------------------------------

    #[test]
    fn encode_date_time_all_styles() {
        let _guard = DATE_ORDER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved = date_order();
        set_date_order(DATEORDER_MDY);

        let mut tm = tm_2024_01_15();
        let mut s = String::new();
        EncodeDateTime(&mut tm, FSEC, false, 0, None, USE_ISO_DATES, &mut s);
        assert_eq!(s, "2024-01-15 10:30:45.123456");

        let mut tm = tm_2024_01_15();
        let mut s = String::new();
        EncodeDateTime(&mut tm, FSEC, false, 0, None, USE_POSTGRES_DATES, &mut s);
        // 2024-01-15 is a Monday.
        assert_eq!(s, "Mon Jan 15 10:30:45.123456 2024");

        let mut tm = tm_2024_01_15();
        let mut s = String::new();
        EncodeDateTime(&mut tm, FSEC, false, 0, None, USE_SQL_DATES, &mut s);
        assert_eq!(s, "01/15/2024 10:30:45.123456");

        let mut tm = tm_2024_01_15();
        let mut s = String::new();
        EncodeDateTime(&mut tm, FSEC, false, 0, None, USE_GERMAN_DATES, &mut s);
        assert_eq!(s, "15.01.2024 10:30:45.123456");

        set_date_order(saved);
    }

    #[test]
    fn encode_date_time_iso_with_numeric_tz() {
        // +05:30 == -19800 seconds offset stored in tz.
        let mut tm = tm_2024_01_15();
        let mut s = String::new();
        EncodeDateTime(&mut tm, FSEC, true, -19800, None, USE_ISO_DATES, &mut s);
        assert_eq!(s, "2024-01-15 10:30:45.123456+05:30");
    }

    #[test]
    fn encode_date_time_isdst_negative_omits_tz() {
        let mut tm = tm_2024_01_15();
        tm.tm_isdst = -1;
        let mut s = String::new();
        EncodeDateTime(&mut tm, FSEC, true, -19800, None, USE_ISO_DATES, &mut s);
        assert_eq!(s, "2024-01-15 10:30:45.123456");
    }

    #[test]
    fn encode_date_time_postgres_with_tzn_clipped() {
        let _guard = DATE_ORDER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved = date_order();
        set_date_order(DATEORDER_MDY);
        let mut tm = tm_2024_01_15();
        let mut s = String::new();
        // tzn longer than MAXTZLEN (10) must be clipped.
        EncodeDateTime(
            &mut tm,
            0,
            true,
            0,
            Some("ABCDEFGHIJKLMNOP"),
            USE_POSTGRES_DATES,
            &mut s,
        );
        assert_eq!(s, "Mon Jan 15 10:30:45 2024 ABCDEFGHIJ");
        set_date_order(saved);
    }

    #[test]
    fn encode_date_time_bc_suffix() {
        let mut tm = tm_2024_01_15();
        tm.tm_year = 0; // year 0 == 1 BC
        let mut s = String::new();
        EncodeDateTime(&mut tm, 0, false, 0, None, USE_ISO_DATES, &mut s);
        assert_eq!(s, "0001-01-15 10:30:45 BC");
    }

    // -- EncodeTimeOnly ----------------------------------------------------

    #[test]
    fn encode_time_only_no_tz() {
        let tm = tm_2024_01_15();
        let mut s = String::new();
        EncodeTimeOnly(&tm, FSEC, false, 0, USE_ISO_DATES, &mut s);
        assert_eq!(s, "10:30:45.123456");
    }

    #[test]
    fn encode_time_only_with_tz() {
        let tm = tm_2024_01_15();
        let mut s = String::new();
        // -08:00 == +28800 seconds stored in tz.
        EncodeTimeOnly(&tm, FSEC, true, 28800, USE_ISO_DATES, &mut s);
        assert_eq!(s, "10:30:45.123456-08");
    }

    #[test]
    fn encode_time_only_trailing_zeros_trimmed() {
        let mut tm = tm_2024_01_15();
        tm.tm_sec = 6;
        let mut s = String::new();
        // .5 of a second == 500000 microseconds, must trim to ".5".
        EncodeTimeOnly(&tm, 500_000, false, 0, USE_ISO_DATES, &mut s);
        assert_eq!(s, "10:30:06.5");
    }

    // -- EncodeInterval under each IntervalStyle ---------------------------

    /// A zeroed `pg_itm`.
    fn itm_zero() -> pg_itm {
        pg_itm::default()
    }

    /// pg_itm for "1 year 2 mons 3 days 04:05:06.5".
    fn itm_sample() -> pg_itm {
        let mut itm = itm_zero();
        itm.tm_year = 1;
        itm.tm_mon = 2;
        itm.tm_mday = 3;
        itm.tm_hour = 4;
        itm.tm_min = 5;
        itm.tm_sec = 6;
        itm.tm_usec = 500_000;
        itm
    }

    #[test]
    fn encode_interval_postgres() {
        let itm = itm_sample();
        let mut s = String::new();
        EncodeInterval(&itm, INTSTYLE_POSTGRES, &mut s);
        assert_eq!(s, "1 year 2 mons 3 days 04:05:06.5");
    }

    #[test]
    fn encode_interval_postgres_verbose() {
        let itm = itm_sample();
        let mut s = String::new();
        EncodeInterval(&itm, INTSTYLE_POSTGRES_VERBOSE, &mut s);
        assert_eq!(s, "@ 1 year 2 mons 3 days 4 hours 5 mins 6.5 secs");
    }

    #[test]
    fn encode_interval_sql_standard() {
        let itm = itm_sample();
        let mut s = String::new();
        EncodeInterval(&itm, INTSTYLE_SQL_STANDARD, &mut s);
        // Mixed year-month and day-time -> non-sql-standard, signs forced.
        assert_eq!(s, "+1-2 +3 +4:05:06.5");
    }

    #[test]
    fn encode_interval_iso_8601() {
        let itm = itm_sample();
        let mut s = String::new();
        EncodeInterval(&itm, INTSTYLE_ISO_8601, &mut s);
        assert_eq!(s, "P1Y2M3DT4H5M6.5S");
    }

    #[test]
    fn encode_interval_iso_8601_zero() {
        let itm = itm_zero();
        let mut s = String::new();
        EncodeInterval(&itm, INTSTYLE_ISO_8601, &mut s);
        assert_eq!(s, "PT0S");
    }

    // -- EncodeTimezone ----------------------------------------------------

    #[test]
    fn encode_timezone_plus_0530() {
        let mut s = String::new();
        // +05:30 displayed -> tz is negated, so -19800.
        EncodeTimezone(&mut s, -19800, USE_ISO_DATES);
        assert_eq!(s, "+05:30");
    }

    #[test]
    fn encode_timezone_minus_08() {
        let mut s = String::new();
        // -08:00 displayed -> +28800.
        EncodeTimezone(&mut s, 28800, USE_ISO_DATES);
        assert_eq!(s, "-08");
    }

    // -- Special values ----------------------------------------------------

    #[test]
    fn encode_special_timestamp_infinity() {
        let mut s = String::new();
        assert!(EncodeSpecialTimestamp(DT_NOEND, &mut s));
        assert_eq!(s, "infinity");

        let mut s = String::new();
        assert!(EncodeSpecialTimestamp(DT_NOBEGIN, &mut s));
        assert_eq!(s, "-infinity");

        let mut s = String::new();
        assert!(!EncodeSpecialTimestamp(0, &mut s));
        assert_eq!(s, "");
    }

    #[test]
    fn encode_special_date_infinity() {
        let mut s = String::new();
        assert!(EncodeSpecialDate(DATEVAL_NOEND, &mut s));
        assert_eq!(s, "infinity");

        let mut s = String::new();
        assert!(EncodeSpecialDate(DATEVAL_NOBEGIN, &mut s));
        assert_eq!(s, "-infinity");
    }

    // -- AppendSeconds edge cases ------------------------------------------

    #[test]
    fn append_seconds_overflow_punts_to_full_width() {
        // precision smaller than the number of significant digits forces the
        // punt-to-pg_ultostr path.
        let mut s = String::new();
        AppendSeconds(&mut s, 6, 123_456, 3, true);
        assert_eq!(s, "06.123456");
    }

    #[test]
    fn append_seconds_no_fraction() {
        let mut s = String::new();
        AppendSeconds(&mut s, 6, 0, 6, true);
        assert_eq!(s, "06");
    }

    #[test]
    fn date_order_dmy_postgres_datetime() {
        let _guard = DATE_ORDER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved = date_order();
        set_date_order(DATEORDER_DMY);
        let mut tm = tm_2024_01_15();
        let mut s = String::new();
        EncodeDateTime(&mut tm, 0, false, 0, None, USE_POSTGRES_DATES, &mut s);
        assert_eq!(s, "Mon 15 Jan 10:30:45 2024");
        set_date_order(saved);
    }
}
