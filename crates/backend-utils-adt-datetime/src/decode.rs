//! The date/time decode engine, ported from
//! `src/backend/utils/adt/datetime.c` (idiomatic, safe Rust).
//!
//! Safe Rust over the shared `types::datetime` vocabulary and the reused
//! timezone engine (`backend_timezone_localtime`).  All public decode entry
//! points preserve the C `DTERR_*` return discipline: `0` (or, for
//! `DecodeNumberField`, a non-negative DTK token) on success, a negative
//! `DTERR_*` code on failure.  The caller maps `DTERR_*` to ereport / soft
//! errors in a later phase.
//!
//! Faithfulness notes:
//! * `pg_tm` is reused from `backend_timezone_localtime`; datetime uses
//!   `tm_mon` 1-based and `tm_year` as-is (full year). The `+1900`/`+1`
//!   boundary adjust vs `pg_localtime` is a P2 concern, deliberately not done
//!   here.
//! * The C code mutates its working buffer in place (e.g. `*cp = '\0'`,
//!   `ftype[i] = ...`).  We model `field` as `&mut [String]` and `ftype` as
//!   `&mut [i32]` so the same in-place edits are reproducible.
//! * `dt2time` / `time_overflows` / `IS_VALID_JULIAN` are the canonical
//!   cores in `crate::convert` / `crate::time`; we delegate to them.
//! * The reused timezone API is `&str`-based (idiomatic), so the `CString`
//!   wrappers the C-ABI reference used are dropped.

// Several routines faithfully mirror C's "reset then conditionally set"
// idioms (e.g. `tmask = 0;` before a switch), which Rust flags as dead stores.
#![allow(unused_assignments)]

use std::rc::Rc;

use types_pgtime::{pg_tm, pg_tz};
use types_core::pg_time_t;
use state_pgtz::session_timezone;
use backend_timezone_localtime::{pg_interpret_timezone_abbrev, pg_next_dst_boundary_tristate, pg_timezone_abbrev_is_known, NextDstBoundary};
use backend_timezone_pgtz::{pg_tzset, pg_tzset_offset};

use types_datetime::*;
use types_error::{ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INVALID_PARAMETER_VALUE};
use types_datetime::{fsec_t, TimestampTz};
use types_error::{PgError, PgResult};

// Charged alloc-tracking collection types (see AGENTS.md HARD RULE): use the
// MemoryContext-charged `Pg*` containers for crate-local growable allocations.

use crate::calendar::{date2j, day_tab, isleap, j2date};
use crate::consts::{DTK_ALL_SECS_M, DTK_DATE_M, DTK_TIME_M};
use crate::settings::{date_order, interval_style};
use crate::tables::{datetktbl, deltatktbl};

// `datetkn` is the shared ABI keyword-table entry (idiomatic `DateToken`).
use types_datetime::DateToken as datetkn;

// ---------------------------------------------------------------------------
// date.c / timestamp.c helpers (canonical homes).
//
// `dt2time` -> timestamp.c core (now in `crate::convert`), `time_overflows` ->
// date.c core (in `crate::time`), `is_valid_julian` -> the timestamp.h
// `IS_VALID_JULIAN` macro (in `crate::convert`).
// ---------------------------------------------------------------------------

use crate::convert::dt2time;
use crate::convert::IS_VALID_JULIAN as is_valid_julian;
use crate::time::time_overflows;

// ---------------------------------------------------------------------------
// Small C-library shims (strtoint / strtoi64 style: parse a numeric prefix,
// returning the value, the byte offset just past the parsed digits, and an
// ERANGE flag).  These mirror C's strtol semantics used throughout datetime.c.
// ---------------------------------------------------------------------------

/// Result of a C-style `strtol`-family parse: value, end offset (bytes parsed),
/// and whether the magnitude overflowed the target type (C's `errno == ERANGE`).
struct StrtoResult<T> {
    val: T,
    end: usize,
    erange: bool,
}

/// C `strtoint(str, &cp, 10)` -- parse a leading optionally-signed base-10
/// integer into `i32`.  `end` is the byte offset of the first unparsed char.
fn strtoint(s: &str) -> StrtoResult<i32> {
    let r = strtoi64(s);
    if r.erange || r.val < i32::MIN as i64 || r.val > i32::MAX as i64 {
        // Clamp like C strtol on overflow, and report ERANGE.
        let clamped = if r.val < 0 { i32::MIN } else { i32::MAX };
        StrtoResult {
            val: clamped,
            end: r.end,
            erange: true,
        }
    } else {
        StrtoResult {
            val: r.val as i32,
            end: r.end,
            erange: false,
        }
    }
}

/// C `strtoi64(str, &cp, 10)` -- parse a leading optionally-signed base-10
/// integer into `i64`.
fn strtoi64(s: &str) -> StrtoResult<i64> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    // C strtol skips leading whitespace; datetime inputs are pre-tokenized so
    // this is rarely exercised, but match the semantics anyway.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut neg = false;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        neg = bytes[i] == b'-';
        i += 1;
    }
    let digits_start = i;
    let mut acc: i64 = 0;
    let mut erange = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        let d = (bytes[i] - b'0') as i64;
        if !erange {
            match acc
                .checked_mul(10)
                .and_then(|a| a.checked_add(if neg { -d } else { d }))
            {
                Some(v) => acc = v,
                None => {
                    erange = true;
                    acc = if neg { i64::MIN } else { i64::MAX };
                }
            }
        }
        i += 1;
    }
    if i == digits_start {
        // No digits consumed: C's strtol/strtoll resets *endptr to the original
        // `str`, i.e. it reports having advanced nothing -- not even past any
        // leading whitespace or a lone sign. Mirror that with end == 0 so the
        // `r.end == 0` / C `cp == str` no-progress check in callers (e.g.
        // DecodeNumber) fires for inputs like "+", "-", or "  " just as C does.
        return StrtoResult {
            val: 0,
            end: 0,
            erange: false,
        };
    }
    StrtoResult {
        val: acc,
        end: i,
        erange,
    }
}

/// C `strtod` for the fraction parser: parse a leading double, returning the
/// value and the byte offset of the first unparsed char.  Returns `None` if no
/// number could be parsed.
fn strtod_prefix(s: &str) -> Option<(f64, usize)> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let start = i;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let mut saw_digit = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
        saw_digit = true;
    }
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
            saw_digit = true;
        }
    }
    if saw_digit && i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let mut j = i + 1;
        if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
            j += 1;
        }
        let exp_start = j;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j > exp_start {
            i = j;
        }
    }
    if !saw_digit {
        return None;
    }
    let parsed = &s[start..i];
    parsed.parse::<f64>().ok().map(|v| (v, i))
}

// ---------------------------------------------------------------------------
// pg_*_overflow helpers (from src/include/common/int.h), the subset used here.
// ---------------------------------------------------------------------------

#[inline]
fn pg_mul_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_mul(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => true,
    }
}

#[inline]
fn pg_add_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => true,
    }
}

#[inline]
fn pg_mul_s32_overflow(a: i32, b: i32, res: &mut i32) -> bool {
    match a.checked_mul(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => true,
    }
}

#[inline]
fn pg_add_s32_overflow(a: i32, b: i32, res: &mut i32) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => true,
    }
}

// ---------------------------------------------------------------------------
// datebsearch()
// ---------------------------------------------------------------------------

/// Compare `key` against a token the way C `strncmp(key, token, TOKMAXLEN)`
/// does: byte-wise over at most `TOKMAXLEN` bytes, where a NUL (end of either
/// string) sorts before any other byte.  Returns the sign of the comparison.
fn strncmp_tokmaxlen(key: &str, token: &str) -> core::cmp::Ordering {
    let kb = key.as_bytes();
    let tb = token.as_bytes();
    let n = TOKMAXLEN as usize;
    for i in 0..n {
        // A position past the end of a string acts as NUL (0), which is less
        // than any real byte; equal NULs end the comparison as equal.
        let kc = if i < kb.len() { kb[i] as i32 } else { 0 };
        let tc = if i < tb.len() { tb[i] as i32 } else { 0 };
        let diff = kc - tc;
        if diff != 0 {
            return diff.cmp(&0);
        }
        if kc == 0 {
            break;
        }
    }
    core::cmp::Ordering::Equal
}

/// `datebsearch()` -- binary search of a sorted [`datetkn`] table.
///
/// Matches the C algorithm exactly: compare the first byte, then `strncmp`
/// over `TOKMAXLEN` bytes; the table is sorted by that key.  `key` must be
/// lowercased already (as the callers guarantee).
///
/// (`utils/adt/datetime.c`)
pub fn datebsearch<'a>(key: &str, base: &'a [datetkn]) -> Option<&'a datetkn> {
    let nel = base.len();
    if nel == 0 {
        return None;
    }
    let mut lo = 0i64;
    let mut hi = nel as i64 - 1;
    let kb = key.as_bytes();
    let key0 = if kb.is_empty() { 0i32 } else { kb[0] as i32 };
    while hi >= lo {
        let pos = lo + ((hi - lo) >> 1);
        let position = &base[pos as usize];
        let tb = position.token.as_bytes();
        let tok0 = if tb.is_empty() { 0i32 } else { tb[0] as i32 };
        /* precheck the first character for a bit of extra speed */
        let mut result = (key0 - tok0).cmp(&0);
        if result == core::cmp::Ordering::Equal {
            result = strncmp_tokmaxlen(key, position.token);
            if result == core::cmp::Ordering::Equal {
                return Some(position);
            }
        }
        if result == core::cmp::Ordering::Less {
            hi = pos - 1;
        } else {
            lo = pos + 1;
        }
    }
    None
}

// ---------------------------------------------------------------------------
// ParseFraction / ParseFractionalSecond
// ---------------------------------------------------------------------------

/// `ParseFraction()` -- parse a `.ddddd` fraction in `[0, 1)`.
///
/// `cp` must start with `'.'`.  Returns `0` on success (storing into `*frac`)
/// or a DTERR code.  (`utils/adt/datetime.c`)
fn ParseFraction(cp: &str, frac: &mut f64) -> i32 {
    debug_assert!(cp.as_bytes().first() == Some(&b'.'));

    // We want to allow just "." with no digits.
    if cp.len() == 1 {
        *frac = 0.0;
        return 0;
    }
    // Reject anything that's not digits after the ".".
    let after = &cp[1..];
    if !after.bytes().all(|b| b.is_ascii_digit()) {
        return DTERR_BAD_FORMAT;
    }
    match strtod_prefix(cp) {
        Some((v, end)) if end == cp.len() => {
            *frac = v;
            0
        }
        _ => DTERR_BAD_FORMAT,
    }
}

/// `ParseFractionalSecond()` -- like [`ParseFraction`] but converts to integer
/// microseconds.  (`utils/adt/datetime.c`)
fn ParseFractionalSecond(cp: &str, fsec: &mut fsec_t) -> i32 {
    let mut frac = 0.0f64;
    let dterr = ParseFraction(cp, &mut frac);
    if dterr != 0 {
        return dterr;
    }
    // C: `*fsec = rint(frac * 1000000)` -- rint() rounds half to even, not the
    // half-away-from-zero behavior of f64::round().  (datetime.c:736)
    *fsec = (frac * 1_000_000.0).round_ties_even() as fsec_t;
    0
}

// ---------------------------------------------------------------------------
// int64_multiply_add + Adjust* helpers
// ---------------------------------------------------------------------------

fn int64_multiply_add(val: i64, multiplier: i64, sum: &mut i64) -> bool {
    let mut product = 0i64;
    if pg_mul_s64_overflow(val, multiplier, &mut product) || pg_add_s64_overflow(*sum, product, sum)
    {
        return false;
    }
    true
}

fn AdjustFractMicroseconds(mut frac: f64, scale: i64, itm_in: &mut pg_itm_in) -> bool {
    if frac == 0.0 {
        return true;
    }
    frac *= scale as f64;
    let mut usec = frac as i64;
    frac -= usec as f64;
    if frac > 0.5 {
        usec += 1;
    } else if frac < -0.5 {
        usec -= 1;
    }
    !pg_add_s64_overflow(itm_in.tm_usec, usec, &mut itm_in.tm_usec)
}

fn AdjustFractDays(mut frac: f64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if frac == 0.0 {
        return true;
    }
    frac *= scale as f64;
    let extra_days = frac as i32;
    if pg_add_s32_overflow(itm_in.tm_mday, extra_days, &mut itm_in.tm_mday) {
        return false;
    }
    frac -= extra_days as f64;
    AdjustFractMicroseconds(frac, USECS_PER_DAY, itm_in)
}

fn AdjustFractYears(frac: f64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    // C: `(int) rint(frac * scale * MONTHS_PER_YEAR)` -- rint() rounds half to
    // even, matching e.g. "0.375 years" (4.5 mons) -> 4 mons.  (datetime.c:618)
    let extra_months = (frac * scale as f64 * MONTHS_PER_YEAR as f64).round_ties_even() as i32;
    !pg_add_s32_overflow(itm_in.tm_mon, extra_months, &mut itm_in.tm_mon)
}

fn AdjustMicroseconds(val: i64, fval: f64, scale: i64, itm_in: &mut pg_itm_in) -> bool {
    if !int64_multiply_add(val, scale, &mut itm_in.tm_usec) {
        return false;
    }
    AdjustFractMicroseconds(fval, scale, itm_in)
}

fn AdjustDays(val: i64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    let mut days = 0i32;
    !pg_mul_s32_overflow(val as i32, scale, &mut days)
        && !pg_add_s32_overflow(itm_in.tm_mday, days, &mut itm_in.tm_mday)
}

fn AdjustMonths(val: i64, itm_in: &mut pg_itm_in) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    !pg_add_s32_overflow(itm_in.tm_mon, val as i32, &mut itm_in.tm_mon)
}

fn AdjustYears(val: i64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    let mut years = 0i32;
    !pg_mul_s32_overflow(val as i32, scale, &mut years)
        && !pg_add_s32_overflow(itm_in.tm_year, years, &mut itm_in.tm_year)
}

fn ClearPgItmIn(itm_in: &mut pg_itm_in) {
    itm_in.tm_usec = 0;
    itm_in.tm_mday = 0;
    itm_in.tm_mon = 0;
    itm_in.tm_year = 0;
}

// ---------------------------------------------------------------------------
// ParseDateTime()
// ---------------------------------------------------------------------------

#[inline]
fn is_space(c: u8) -> bool {
    // C isspace() for the "C" locale.
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

#[inline]
fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

#[inline]
fn is_alpha(c: u8) -> bool {
    c.is_ascii_alphabetic()
}

#[inline]
fn is_alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric()
}

#[inline]
fn is_punct(c: u8) -> bool {
    // C ispunct(): printable, not space, not alnum.
    c.is_ascii_punctuation()
}

#[inline]
fn to_lower(c: u8) -> u8 {
    c.to_ascii_lowercase()
}

/// `ParseDateTime()` -- break `timestr` into tokens based on a date/time
/// context.  Returns `0` on success, a DTERR code on bogus input.
///
/// On success, `field` receives the (NUL-stripped, possibly lowercased) field
/// strings and `ftype` the matching `DTK_*` field-type codes; both are cleared
/// and refilled.  `maxfields` bounds the field count (`MAXDATEFIELDS` ->
/// `DTERR_BAD_FORMAT` on overflow).  `numfields` reports the count.
///
/// (`utils/adt/datetime.c`)
pub fn ParseDateTime(
    timestr: &str,
    buflen: usize,
    field: &mut Vec<String>,
    ftype: &mut Vec<i32>,
    maxfields: usize,
    numfields: &mut usize,
) -> i32 {
    field.truncate(0);
    ftype.truncate(0);

    let bytes = timestr.as_bytes();
    let mut cp = 0usize;
    let len = bytes.len();

    // C tokenizes into a fixed `workbuf[buflen]`, appending each kept byte plus
    // a NUL terminator after every field; once that buffer fills it returns
    // DTERR_BAD_FORMAT (datetime.c APPEND_CHAR / `*bufp++ = '\0'`).  Without this
    // bound, over-long inputs that C rejects would be wrongly accepted here.
    //
    // The exact buffer size differs per C caller: `workbuf[MAXDATELEN + 1]` (129)
    // for date_in/time_in/timetz_in, `workbuf[MAXDATELEN + MAXDATEFIELDS]` (153)
    // for timestamp_in/timestamptz_in, and `workbuf[256]` for interval_in.  We
    // thread it in as `buflen` so each call site can pass its own `sizeof(workbuf)`
    // and accept/reject exactly the inputs C does.
    //
    // `bufp` tracks how many bytes of `workbuf` are consumed so far (the C
    // `bufp - workbuf` offset).
    let mut bufp = 0usize;

    // C's APPEND_CHAR: returns DTERR_BAD_FORMAT if there is no room left for the
    // char *and* its eventual terminator (`(bufp + 1) >= bufend`), else appends.
    macro_rules! append_char {
        ($buf:expr, $ch:expr) => {{
            if bufp + 1 >= buflen {
                return DTERR_BAD_FORMAT;
            }
            bufp += 1;
                        // (charging-only: the C-style i32 DTERR protocol has no OOM error
            // channel; the buffer is bounded by `buflen` <= 256 bytes).
            $buf.push($ch);
        }};
    }

    // outer loop through fields
    while cp < len {
        // Ignore spaces between fields
        if is_space(bytes[cp]) {
            cp += 1;
            continue;
        }

        // Record start of current field
        if field.len() >= maxfields {
            return DTERR_BAD_FORMAT;
        }
        let mut buf = String::new();
        let mut this_ftype: i32;

        // leading digit? then date or time
        if is_digit(bytes[cp]) {
            append_char!(buf, bytes[cp] as char);
            cp += 1;
            while cp < len && is_digit(bytes[cp]) {
                append_char!(buf, bytes[cp] as char);
                cp += 1;
            }

            // time field?
            if cp < len && bytes[cp] == b':' {
                this_ftype = DTK_TIME;
                append_char!(buf, bytes[cp] as char);
                cp += 1;
                while cp < len && (is_digit(bytes[cp]) || bytes[cp] == b':' || bytes[cp] == b'.') {
                    append_char!(buf, bytes[cp] as char);
                    cp += 1;
                }
            }
            // date field? allow embedded text month
            else if cp < len && (bytes[cp] == b'-' || bytes[cp] == b'/' || bytes[cp] == b'.') {
                let delim = bytes[cp];
                append_char!(buf, bytes[cp] as char);
                cp += 1;
                // second field is all digits? then no embedded text month
                if cp < len && is_digit(bytes[cp]) {
                    this_ftype = if delim == b'.' { DTK_NUMBER } else { DTK_DATE };
                    while cp < len && is_digit(bytes[cp]) {
                        append_char!(buf, bytes[cp] as char);
                        cp += 1;
                    }
                    // insist that the delimiters match for a three-field date.
                    if cp < len && bytes[cp] == delim {
                        this_ftype = DTK_DATE;
                        append_char!(buf, bytes[cp] as char);
                        cp += 1;
                        while cp < len && (is_digit(bytes[cp]) || bytes[cp] == delim) {
                            append_char!(buf, bytes[cp] as char);
                            cp += 1;
                        }
                    }
                } else {
                    this_ftype = DTK_DATE;
                    while cp < len && (is_alnum(bytes[cp]) || bytes[cp] == delim) {
                        append_char!(buf, to_lower(bytes[cp]) as char);
                        cp += 1;
                    }
                }
            }
            // otherwise, number only
            else {
                this_ftype = DTK_NUMBER;
            }
        }
        // Leading decimal point? Then fractional seconds...
        else if bytes[cp] == b'.' {
            append_char!(buf, bytes[cp] as char);
            cp += 1;
            while cp < len && is_digit(bytes[cp]) {
                append_char!(buf, bytes[cp] as char);
                cp += 1;
            }
            this_ftype = DTK_NUMBER;
        }
        // text? then date string, month, day of week, special, or timezone
        else if is_alpha(bytes[cp]) {
            this_ftype = DTK_STRING;
            append_char!(buf, to_lower(bytes[cp]) as char);
            cp += 1;
            while cp < len && is_alpha(bytes[cp]) {
                append_char!(buf, to_lower(bytes[cp]) as char);
                cp += 1;
            }

            // Dates can have embedded '-', '/', or '.' separators; could also
            // be a timezone name with embedded '/', '+', '-', '_', or ':'.
            let mut is_date = false;
            if cp < len && (bytes[cp] == b'-' || bytes[cp] == b'/' || bytes[cp] == b'.') {
                is_date = true;
            } else if cp < len && (bytes[cp] == b'+' || is_digit(bytes[cp])) {
                // null-terminate current field value -> just search buf
                if datebsearch(&buf, datetktbl).is_none() {
                    is_date = true;
                }
            }
            if is_date {
                this_ftype = DTK_DATE;
                loop {
                    if cp >= len {
                        break;
                    }
                    append_char!(buf, to_lower(bytes[cp]) as char);
                    cp += 1;
                    if !(cp < len
                        && (bytes[cp] == b'+'
                            || bytes[cp] == b'-'
                            || bytes[cp] == b'/'
                            || bytes[cp] == b'_'
                            || bytes[cp] == b'.'
                            || bytes[cp] == b':'
                            || is_alnum(bytes[cp])))
                    {
                        break;
                    }
                }
            }
        }
        // sign? then special or numeric timezone
        else if bytes[cp] == b'+' || bytes[cp] == b'-' {
            append_char!(buf, bytes[cp] as char);
            cp += 1;
            // soak up leading whitespace
            while cp < len && is_space(bytes[cp]) {
                cp += 1;
            }
            // numeric timezone?
            if cp < len && is_digit(bytes[cp]) {
                this_ftype = DTK_TZ;
                append_char!(buf, bytes[cp] as char);
                cp += 1;
                while cp < len
                    && (is_digit(bytes[cp])
                        || bytes[cp] == b':'
                        || bytes[cp] == b'.'
                        || bytes[cp] == b'-')
                {
                    append_char!(buf, bytes[cp] as char);
                    cp += 1;
                }
            }
            // special?
            else if cp < len && is_alpha(bytes[cp]) {
                this_ftype = DTK_SPECIAL;
                append_char!(buf, to_lower(bytes[cp]) as char);
                cp += 1;
                while cp < len && is_alpha(bytes[cp]) {
                    append_char!(buf, to_lower(bytes[cp]) as char);
                    cp += 1;
                }
            }
            // otherwise something wrong...
            else {
                return DTERR_BAD_FORMAT;
            }
        }
        // ignore other punctuation but use as delimiter
        else if is_punct(bytes[cp]) {
            cp += 1;
            continue;
        }
        // otherwise, something is not right...
        else {
            return DTERR_BAD_FORMAT;
        }

        // Force in a delimiter (NUL terminator) after each field; this consumes
        // one more byte of the workbuf, exactly as C's `*bufp++ = '\0'`.
        bufp += 1;
        field.push(buf);
        ftype.push(this_ftype);
    }

    *numfields = field.len();
    0
}

// ---------------------------------------------------------------------------
// DecodeSpecial / DecodeUnits (table lookups)
//
// The C versions keep a per-field datecache[]/deltacache[] of the last hit;
// that is a pure micro-optimization with no behavioral effect, so it is
// omitted here.  `lowtoken` must be lowercased already.
// ---------------------------------------------------------------------------

/// `DecodeSpecial()` -- look `lowtoken` up in [`datetktbl`].  Returns the field
/// type (or `UNKNOWN_FIELD`) and stores the associated value into `*val`.
///
/// (`utils/adt/datetime.c`)
pub fn DecodeSpecial(_field: usize, lowtoken: &str, val: &mut i32) -> i32 {
    match datebsearch(lowtoken, datetktbl) {
        None => {
            *val = 0;
            UNKNOWN_FIELD
        }
        Some(tp) => {
            *val = tp.value;
            tp.r#type
        }
    }
}

/// `DecodeUnits()` -- look `lowtoken` up in [`deltatktbl`] (interval units).
/// Returns the field type (or `UNKNOWN_FIELD`); stores value into `*val`.
///
/// (`utils/adt/datetime.c`)
pub fn DecodeUnits(_field: usize, lowtoken: &str, val: &mut i32) -> i32 {
    match datebsearch(lowtoken, deltatktbl) {
        None => {
            *val = 0;
            UNKNOWN_FIELD
        }
        Some(tp) => {
            *val = tp.value;
            tp.r#type
        }
    }
}

// ---------------------------------------------------------------------------
// DecodeTimezone (numeric +-HH:MM[:SS])
// ---------------------------------------------------------------------------

/// `DecodeTimezone()` -- interpret `str` as a numeric timezone (`+-HH[:MM[:SS]]`).
///
/// Returns `0` and stores the offset (POSIX-flipped sign) into `*tzp`, or a
/// DTERR code.  (`utils/adt/datetime.c`)
pub fn DecodeTimezone(str: &str, tzp: &mut i32) -> i32 {
    let bytes = str.as_bytes();
    // leading character must be "+" or "-"
    if bytes.first() != Some(&b'+') && bytes.first() != Some(&b'-') {
        return DTERR_BAD_FORMAT;
    }

    let min: i32;
    let mut sec = 0i32;

    let r = strtoint(&str[1..]);
    if r.erange {
        return DTERR_TZDISP_OVERFLOW;
    }
    let mut hr = r.val;
    // cp index relative to str: 1 + r.end
    let mut cp = 1 + r.end;

    // explicit delimiter?
    if cp < bytes.len() && bytes[cp] == b':' {
        let r2 = strtoint(&str[cp + 1..]);
        if r2.erange {
            return DTERR_TZDISP_OVERFLOW;
        }
        min = r2.val;
        cp = cp + 1 + r2.end;
        if cp < bytes.len() && bytes[cp] == b':' {
            let r3 = strtoint(&str[cp + 1..]);
            if r3.erange {
                return DTERR_TZDISP_OVERFLOW;
            }
            sec = r3.val;
            cp = cp + 1 + r3.end;
        }
    }
    // otherwise, might have run things together...
    else if cp >= bytes.len() && str.len() > 3 {
        min = hr % 100;
        hr /= 100;
    } else {
        min = 0;
    }

    // Range-check the values.
    if !(0..=MAX_TZDISP_HOUR).contains(&hr) {
        return DTERR_TZDISP_OVERFLOW;
    }
    if !(0..MINS_PER_HOUR).contains(&min) {
        return DTERR_TZDISP_OVERFLOW;
    }
    if !(0..SECS_PER_MINUTE).contains(&sec) {
        return DTERR_TZDISP_OVERFLOW;
    }

    let mut tz = (hr * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec;
    if bytes[0] == b'-' {
        tz = -tz;
    }

    *tzp = -tz;

    if cp < bytes.len() {
        return DTERR_BAD_FORMAT;
    }

    0
}

// ---------------------------------------------------------------------------
// DecodeTimezoneAbbrev
//
// The session_timezone leg (the real working path for IANA-known abbrevs such
// as "EST") is implemented via the reused timezone engine.  The runtime-loaded
// abbreviation table (`zoneabbrevtbl`, populated from the
// `timezone_abbreviations` GUC) is reached through the installable
// `TimezoneResolver` hook (see `crate::tz_resolver`); with no resolver
// installed it behaves as an empty table, i.e. abbrevs not known to the session
// zone return `UNKNOWN_FIELD`, exactly as C does when `zoneabbrevtbl == NULL`.
// ---------------------------------------------------------------------------

/// `TimeZoneAbbrevIsKnown()` -- does `session_timezone` know `abbr`?  On a hit,
/// fills `(isfixed, offset, isdst)` with the *flipped-sign* offset to agree
/// with `DetermineTimeZoneOffset()`.  (`utils/adt/datetime.c`)
fn TimeZoneAbbrevIsKnown(
    abbr: &str,
    tzp: &pg_tz,
    isfixed: &mut bool,
    offset: &mut i32,
    isdst: &mut i32,
) -> bool {
    let upper = abbr.to_ascii_uppercase();
    match pg_timezone_abbrev_is_known(&upper, tzp) {
        Some(k) => {
            *isfixed = k.isfixed;
            *isdst = k.isdst;
            // Change sign to agree with DetermineTimeZoneOffset().
            *offset = -(k.gmtoff as i32);
            true
        }
        None => false,
    }
}

/// `DecodeTimezoneAbbrev()` -- interpret `lowtoken` as a timezone abbreviation.
///
/// Sets `*ftype` to `TZ`/`DTZ`/`DYNTZ` on a hit, else `UNKNOWN_FIELD`.  On a
/// fixed-offset hit, `*offset` holds the offset; on a dynamic hit, `*tz` holds
/// the underlying zone.  `lowtoken` must be lowercased already.  Returns `0` or
/// a DTERR code.
///
/// The `session_timezone` leg (checked first, so abbreviations whose meaning
/// varies across zones such as "LMT" resolve correctly) is handled by the
/// reused timezone engine.  The runtime abbreviation-table leg (C:
/// `zoneabbrevtbl` + `FetchDynamicTimeZone`) is delegated to the installed
/// [`TimezoneResolver`]; with no resolver installed it acts as `zoneabbrevtbl
/// == NULL`, so unknown abbrevs return `UNKNOWN_FIELD` (not an error).
///
/// On a `DTERR_BAD_ZONE_ABBREV` return (a `DYNTZ` abbreviation whose configured
/// underlying zone failed to load), `*dyntz_zone` receives the underlying zone
/// name (C: `extra->dtee_timezone = dtza->zone`) so the caller can build the
/// faithful error message, which names the underlying zone, not the
/// abbreviation.
///
/// (`utils/adt/datetime.c`)
pub fn DecodeTimezoneAbbrev(
    _field: usize,
    lowtoken: &str,
    ftype: &mut i32,
    offset: &mut i32,
    tz: &mut Option<Rc<pg_tz>>,
    dyntz_zone: &mut Option<String>,
) -> i32 {
    let mut isfixed = false;
    let mut isdst = 0;
    let stz = session_timezone();
    if TimeZoneAbbrevIsKnown(lowtoken, &stz, &mut isfixed, offset, &mut isdst) {
        *ftype = if isfixed {
            if isdst != 0 {
                DTZ
            } else {
                TZ
            }
        } else {
            DYNTZ
        };
        *tz = if isfixed { None } else { Some(stz) };
        // flip sign to agree with the convention used in zoneabbrevtbl
        *offset = -(*offset);
        return 0;
    }

    // Nope, so look in zoneabbrevtbl, reached through the resolver hook.  With
    // no resolver installed this is the C `zoneabbrevtbl == NULL` path: tp ==
    // NULL.
    match crate::tz_resolver::timezone_resolver().and_then(|r| r.resolve_abbrev(lowtoken)) {
        None => {
            // tp == NULL: not an error, just unrecognized.  (Failures are not
            // cached in C either.)
            *ftype = UNKNOWN_FIELD;
            *offset = 0;
            *tz = None;
            0
        }
        Some(abbrev) => {
            *ftype = abbrev.ftype;
            if abbrev.ftype == DYNTZ {
                *offset = 0;
                match abbrev.tz {
                    // C: FetchDynamicTimeZone returned NULL.  Carry the
                    // underlying zone name out for the error report
                    // (C: extra->dtee_timezone = dtza->zone).
                    None => {
                        *dyntz_zone = abbrev.dyntz_zone;
                        return DTERR_BAD_ZONE_ABBREV;
                    }
                    Some(z) => *tz = Some(z),
                }
            } else {
                *offset = abbrev.gmtoff;
                *tz = None;
            }
            0
        }
    }
}

// ---------------------------------------------------------------------------
// DecodeTimezoneName  (the "AT TIME ZONE by name" entry point)
// ---------------------------------------------------------------------------

/// `DecodeTimezoneName()` -- interpret a string as a timezone abbreviation or a
/// full tzdb zone name.  Returns one of `TZNAME_FIXED_OFFSET`, `TZNAME_DYNTZ`,
/// or `TZNAME_ZONE` (in `Ok`), or a faithful PG error.
///
/// For `TZNAME_FIXED_OFFSET`, `*offset` receives the UTC offset (seconds, ISO
/// sign convention: positive east of Greenwich).  For the other two cases,
/// `*tz` receives the zone struct.
///
/// First the abbreviation table is consulted (to handle e.g. "EST"), then the
/// full zone database (for e.g. "America/New_York"); this order matches the way
/// timestamp input checks the cases.  The full-zone leg calls `pg_tzset(tzname)`
/// directly (C: `*tz = pg_tzset(tzname)`); an unknown name yields the C error
/// `time zone "X" not recognized`.
///
/// (`utils/adt/datetime.c`)
pub fn DecodeTimezoneName(
    tzname: &str,
    offset: &mut i32,
    tz: &mut Option<Rc<pg_tz>>,
) -> PgResult<i32> {
    // DecodeTimezoneAbbrev requires lowercase input.  C uses
    // downcase_truncate_identifier(tzname, ..., false); for ASCII zone names
    // this is a plain ASCII lowercase.
    let lowzone = tzname.to_ascii_lowercase();

    let mut type_: i32 = 0;
    let mut dyntz_zone: Option<String> = None;
    let dterr = DecodeTimezoneAbbrev(0, &lowzone, &mut type_, offset, tz, &mut dyntz_zone);
    if dterr != 0 {
        // C: DateTimeParseError(dterr, &extra, NULL, NULL, NULL).  The only
        // dterr DecodeTimezoneAbbrev can produce here is DTERR_BAD_ZONE_ABBREV
        // (a configured abbreviation whose underlying zone failed to load); the
        // message names that underlying zone (extra->dtee_timezone), the detail
        // names the abbreviation (extra->dtee_abbrev == lowzone here).
        return Err(bad_zone_abbrev_error(
            dyntz_zone.as_deref().unwrap_or(&lowzone),
            &lowzone,
        ));
    }

    if type_ == TZ || type_ == DTZ {
        // fixed-offset abbreviation, return the offset
        Ok(TZNAME_FIXED_OFFSET)
    } else if type_ == DYNTZ {
        // dynamic-offset abbreviation, return its referenced timezone
        Ok(TZNAME_DYNTZ)
    } else {
        // try it as a full zone name (C: *tz = pg_tzset(tzname)).  pg_tzset is
        // a fully-implemented safe-Rust tzdb loader; call it directly, exactly
        // as the UNKNOWN_FIELD legs of DecodeDateTime/DecodeTimeOnly do.
        match pg_tzset(tzname)? {
            Some(z) => {
                *tz = Some(z);
                Ok(TZNAME_ZONE)
            }
            None => Err(time_zone_not_recognized(tzname)),
        }
    }
}

/// C: `ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE),
/// errmsg("time zone \"%s\" not recognized", tzname))`.
fn time_zone_not_recognized(tzname: &str) -> PgError {
    PgError::error(format!("time zone \"{tzname}\" not recognized"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// C `DateTimeParseError(DTERR_BAD_ZONE_ABBREV, ...)` (datetime.c:4252): a
/// configured abbreviation whose underlying zone could not be loaded.  Uses
/// `ERRCODE_CONFIG_FILE_ERROR` and the same principal message + detail as C.
///
/// `zone` is the underlying zone name (C: `extra->dtee_timezone`), named in the
/// principal message; `abbrev` is the abbreviation token (C:
/// `extra->dtee_abbrev`), named in the detail.
fn bad_zone_abbrev_error(zone: &str, abbrev: &str) -> PgError {
    PgError::error(format!("time zone \"{zone}\" not recognized"))
        .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
        .with_detail(format!(
            "This time zone name appears in the configuration file for time zone abbreviation \"{abbrev}\"."
        ))
}

/// `DecodeTimezoneNameToTz()` -- like [`DecodeTimezoneName`] but always yields a
/// `pg_tz` descriptor.  For a fixed-offset abbreviation it obtains a synthetic
/// zone via `pg_tzset_offset(-offset)`.  (`utils/adt/datetime.c`)
pub fn DecodeTimezoneNameToTz(tzname: &str) -> PgResult<Rc<pg_tz>> {
    let mut offset: i32 = 0;
    let mut result: Option<Rc<pg_tz>> = None;
    if DecodeTimezoneName(tzname, &mut offset, &mut result)? == TZNAME_FIXED_OFFSET {
        // fixed-offset abbreviation, get a pg_tz descriptor for that.
        // flip to POSIX sign convention.  pg_tzset_offset effectively never
        // fails for a valid offset; treat a None as an unrecognized zone.
        return pg_tzset_offset(-(offset as i64))?.ok_or_else(|| time_zone_not_recognized(tzname));
    }
    // For TZNAME_DYNTZ / TZNAME_ZONE, DecodeTimezoneName already set `result`.
    result.ok_or_else(|| {
        PgError::error("DecodeTimezoneNameToTz: DecodeTimezoneName sets *tz for DYNTZ/ZONE results")
    })
}

// ---------------------------------------------------------------------------
// DetermineTimeZoneOffset family
// ---------------------------------------------------------------------------

/// `DetermineTimeZoneOffsetInternal()` -- compute the GMT offset (and impute
/// UTC time into `tp`) for the y/m/d/h/m/s in `tm` under zone `tzp`.  Sets
/// `tm.tm_isdst`.  Out-of-range dates punt to UTC.  (`utils/adt/datetime.c`)
fn DetermineTimeZoneOffsetInternal(tm: &mut pg_tm, tzp: &pg_tz, tp: &mut pg_time_t) -> i32 {
    // overflow -> assume UTC
    let overflow = |tm: &mut pg_tm, tp: &mut pg_time_t| -> i32 {
        tm.tm_isdst = 0;
        *tp = 0;
        0
    };

    if !is_valid_julian(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return overflow(tm, tp);
    }
    let date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - UNIX_EPOCH_JDATE;

    let day = (date as pg_time_t) * SECS_PER_DAY as pg_time_t;
    if day / SECS_PER_DAY as pg_time_t != date as pg_time_t {
        return overflow(tm, tp);
    }
    let sec = tm.tm_sec + (tm.tm_min + tm.tm_hour * MINS_PER_HOUR) * SECS_PER_MINUTE;
    let mytime = day + sec as pg_time_t;
    if mytime < 0 && day > 0 {
        return overflow(tm, tp);
    }

    let prevtime = mytime - SECS_PER_DAY as pg_time_t;
    if mytime < 0 && prevtime > 0 {
        return overflow(tm, tp);
    }

    let (before_gmtoff, before_isdst, boundary, after_gmtoff, after_isdst) =
        match pg_next_dst_boundary_tristate(prevtime, tzp) {
            NextDstBoundary::Overflow => return overflow(tm, tp),
            NextDstBoundary::NoTransition {
                before_gmtoff,
                before_isdst,
            } => {
                // Non-DST zone, life is simple.
                tm.tm_isdst = before_isdst;
                *tp = mytime - before_gmtoff as pg_time_t;
                return -(before_gmtoff as i32);
            }
            NextDstBoundary::Boundary(b) => (
                b.before_gmtoff,
                b.before_isdst,
                b.boundary,
                b.after_gmtoff,
                b.after_isdst,
            ),
        };

    let beforetime = mytime - before_gmtoff as pg_time_t;
    if (before_gmtoff > 0 && mytime < 0 && beforetime > 0)
        || (before_gmtoff <= 0 && mytime > 0 && beforetime < 0)
    {
        return overflow(tm, tp);
    }
    let aftertime = mytime - after_gmtoff as pg_time_t;
    if (after_gmtoff > 0 && mytime < 0 && aftertime > 0)
        || (after_gmtoff <= 0 && mytime > 0 && aftertime < 0)
    {
        return overflow(tm, tp);
    }

    if beforetime < boundary && aftertime < boundary {
        tm.tm_isdst = before_isdst;
        *tp = beforetime;
        return -(before_gmtoff as i32);
    }
    if beforetime > boundary && aftertime >= boundary {
        tm.tm_isdst = after_isdst;
        *tp = aftertime;
        return -(after_gmtoff as i32);
    }

    if beforetime > aftertime {
        tm.tm_isdst = before_isdst;
        *tp = beforetime;
        return -(before_gmtoff as i32);
    }
    tm.tm_isdst = after_isdst;
    *tp = aftertime;
    -(after_gmtoff as i32)
}

/// `DetermineTimeZoneOffset()` -- public wrapper of the above; discards `tp`.
/// (`utils/adt/datetime.c`)
pub fn DetermineTimeZoneOffset(tm: &mut pg_tm, tzp: &pg_tz) -> i32 {
    let mut t: pg_time_t = 0;
    DetermineTimeZoneOffsetInternal(tm, tzp, &mut t)
}

/// `DetermineTimeZoneAbbrevOffsetInternal()` -- resolve `abbr` at UTC time `t`
/// in zone `tzp`, returning the flipped-sign offset and DST flag on a match.
/// (`utils/adt/datetime.c`)
fn DetermineTimeZoneAbbrevOffsetInternal(
    t: pg_time_t,
    abbr: &str,
    tzp: &pg_tz,
    offset: &mut i32,
    isdst: &mut i32,
) -> bool {
    let upper = abbr.to_ascii_uppercase();
    match pg_interpret_timezone_abbrev(&upper, t, tzp) {
        Some(a) => {
            *isdst = a.isdst;
            *offset = -(a.gmtoff as i32);
            true
        }
        None => false,
    }
}

/// `DetermineTimeZoneAbbrevOffset()` -- offset/DST for a (possibly dynamic)
/// abbreviation `abbr`, probing at the local time in `tm`.  Sets `tm.tm_isdst`.
/// (`utils/adt/datetime.c`)
pub fn DetermineTimeZoneAbbrevOffset(tm: &mut pg_tm, abbr: &str, tzp: &pg_tz) -> i32 {
    let mut t: pg_time_t = 0;
    let zone_offset = DetermineTimeZoneOffsetInternal(tm, tzp, &mut t);

    let mut abbr_offset = 0;
    let mut abbr_isdst = 0;
    if DetermineTimeZoneAbbrevOffsetInternal(t, abbr, tzp, &mut abbr_offset, &mut abbr_isdst) {
        tm.tm_isdst = abbr_isdst;
        return abbr_offset;
    }
    zone_offset
}

/// `DetermineTimeZoneAbbrevOffsetTS()` -- as [`DetermineTimeZoneAbbrevOffset`]
/// but the probe time is a `TimestampTz` (UTC) and the DST flag is returned via
/// `*isdst` rather than into `tm`.  (`utils/adt/datetime.c`)
///
/// Now that the `timestamp.rs` cores (`timestamp2tm` / `timestamp_out_of_range`
/// / `timestamptz_to_time_t`) are ported, this is no longer deferred; it is the
/// thin wrapper a few `timestamp.c` callers (e.g. `timestamptz_zone`) need.
pub fn DetermineTimeZoneAbbrevOffsetTS(
    ts: TimestampTz,
    abbr: &str,
    tzp: &pg_tz,
    isdst: &mut i32,
) -> PgResult<i32> {
    let t = crate::convert::timestamptz_to_time_t(ts);
    let mut abbr_offset = 0;

    // If the abbrev matches anything in the zone data, this is pretty easy.
    if DetermineTimeZoneAbbrevOffsetInternal(t, abbr, tzp, &mut abbr_offset, isdst) {
        return Ok(abbr_offset);
    }

    // Else, break down the timestamp so we can use DetermineTimeZoneOffset.
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if crate::timestamp::timestamp2tm(ts, Some(&mut tz), &mut tm, &mut fsec, None, Some(tzp))
        .is_err()
    {
        return Err(crate::timestamp::timestamp_out_of_range());
    }

    let zone_offset = DetermineTimeZoneOffset(&mut tm, tzp);
    *isdst = tm.tm_isdst;
    Ok(zone_offset)
}

// ---------------------------------------------------------------------------
// DecodeDate / ValidateDate
// ---------------------------------------------------------------------------

/// `DecodeDate()` -- decode a delimited date string into `tm`.  Sets the
/// `*tmask` of fields found and `*is2digits` for a 2-digit year.  Returns `0`
/// or a DTERR code.  `str` is consumed as owned (the C version splits it in
/// place).  (`utils/adt/datetime.c`)
fn DecodeDate(
    str: &str,
    mut fmask: i32,
    tmask: &mut i32,
    is2digits: &mut bool,
    tm: &mut pg_tm,
) -> i32 {
    let mut fsec: fsec_t = 0;
    let mut haveTextMonth = false;
    let mut dmask;

    *tmask = 0;

    // parse this string into fields...
    let bytes = str.as_bytes();
    let mut pos = 0usize;
    // field[i] holds Some(token) until "completed" (set to None).
    let mut fields: Vec<Option<String>> = Vec::new();
    while pos < bytes.len() && fields.len() < MAXDATEFIELDS as usize {
        // skip field separators
        while pos < bytes.len() && !is_alnum(bytes[pos]) {
            pos += 1;
        }
        if pos >= bytes.len() {
            return DTERR_BAD_FORMAT; // end of string after separator
        }
        let start = pos;
        if is_digit(bytes[pos]) {
            while pos < bytes.len() && is_digit(bytes[pos]) {
                pos += 1;
            }
        } else if is_alpha(bytes[pos]) {
            while pos < bytes.len() && is_alpha(bytes[pos]) {
                pos += 1;
            }
        }
        fields.push(Some(String::from(&str[start..pos])));
        // skip the single delimiting char (C does *str++ = '\0')
        if pos < bytes.len() {
            pos += 1;
        }
    }
    let nf = fields.len();

    // look first for text fields (unambiguous month)
    for (i, slot) in fields.iter_mut().enumerate().take(nf) {
        let f = slot.as_ref().unwrap();
        if f.as_bytes().first().is_some_and(|b| is_alpha(*b)) {
            let mut val = 0;
            let typ = DecodeSpecial(i, f, &mut val);
            if typ == IGNORE_DTF {
                continue;
            }
            dmask = DTK_M(typ);
            match typ {
                t if t == MONTH => {
                    tm.tm_mon = val;
                    haveTextMonth = true;
                }
                _ => return DTERR_BAD_FORMAT,
            }
            if fmask & dmask != 0 {
                return DTERR_BAD_FORMAT;
            }
            fmask |= dmask;
            *tmask |= dmask;
            *slot = None;
        }
    }

    // now pick up remaining numeric fields
    for slot in fields.iter().take(nf) {
        let Some(f) = slot.clone() else {
            continue;
        };
        let len = f.len() as i32;
        if len <= 0 {
            return DTERR_BAD_FORMAT;
        }
        let mut dmask2 = 0;
        let dterr = DecodeNumber(
            len,
            &f,
            haveTextMonth,
            fmask,
            &mut dmask2,
            tm,
            &mut fsec,
            is2digits,
        );
        if dterr != 0 {
            return dterr;
        }
        if fmask & dmask2 != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= dmask2;
        *tmask |= dmask2;
    }

    if (fmask & !(DTK_M(DOY) | DTK_M(TZ))) != DTK_DATE_M {
        return DTERR_BAD_FORMAT;
    }

    // validation of the field values waits until ValidateDate()
    0
}

/// `ValidateDate()` -- check year/month/day, handle BC/2-digit-year and DOY.
/// Returns `0` or a DTERR code.  (`utils/adt/datetime.c`)
pub fn ValidateDate(
    fmask: i32,
    isjulian: bool,
    is2digits: bool,
    bc: bool,
    tm: &mut pg_tm,
) -> i32 {
    if fmask & DTK_M(YEAR) != 0 {
        if isjulian {
            // tm_year is correct and should not be touched
        } else if bc {
            // there is no year zero in AD/BC notation
            if tm.tm_year <= 0 {
                return DTERR_FIELD_OVERFLOW;
            }
            // internally, 1 BC is year zero, 2 BC is -1, etc
            tm.tm_year = -(tm.tm_year - 1);
        } else if is2digits {
            // process 1 or 2-digit input as 1970-2069 AD
            if tm.tm_year < 0 {
                return DTERR_FIELD_OVERFLOW;
            }
            if tm.tm_year < 70 {
                tm.tm_year += 2000;
            } else if tm.tm_year < 100 {
                tm.tm_year += 1900;
            }
        } else {
            // there is no year zero in AD/BC notation
            if tm.tm_year <= 0 {
                return DTERR_FIELD_OVERFLOW;
            }
        }
    }

    // now that we have correct year, decode DOY
    if fmask & DTK_M(DOY) != 0 {
        let (y, m, d) = j2date(date2j(tm.tm_year, 1, 1) + tm.tm_yday - 1);
        tm.tm_year = y;
        tm.tm_mon = m;
        tm.tm_mday = d;
    }

    // check for valid month
    if fmask & DTK_M(MONTH) != 0 && (tm.tm_mon < 1 || tm.tm_mon > MONTHS_PER_YEAR) {
        return DTERR_MD_FIELD_OVERFLOW;
    }

    // minimal check for valid day
    if fmask & DTK_M(DAY) != 0 && (tm.tm_mday < 1 || tm.tm_mday > 31) {
        return DTERR_MD_FIELD_OVERFLOW;
    }

    if (fmask & DTK_DATE_M) == DTK_DATE_M
        && tm.tm_mday > day_tab[isleap(tm.tm_year)][(tm.tm_mon - 1) as usize]
    {
        return DTERR_FIELD_OVERFLOW;
    }

    0
}

// ---------------------------------------------------------------------------
// DecodeTimeCommon / DecodeTime / DecodeTimeForInterval
// ---------------------------------------------------------------------------

/// `DecodeTimeCommon()` -- decode a delimited time string into a `pg_itm`
/// (only `tm_usec`/`tm_sec`/`tm_min`/`tm_hour` are used).  Shared between the
/// timestamp and interval cases.  (`utils/adt/datetime.c`)
fn DecodeTimeCommon(
    str: &str,
    _fmask: i32,
    range: i32,
    tmask: &mut i32,
    itm: &mut pg_itm,
) -> i32 {
    let bytes = str.as_bytes();
    let mut fsec: fsec_t = 0;
    *tmask = DTK_TIME_M;

    let r = strtoi64(str);
    if r.erange {
        return DTERR_FIELD_OVERFLOW;
    }
    itm.tm_hour = r.val;
    let mut cp = r.end;
    if cp >= bytes.len() || bytes[cp] != b':' {
        return DTERR_BAD_FORMAT;
    }
    let r2 = strtoint(&str[cp + 1..]);
    if r2.erange {
        return DTERR_FIELD_OVERFLOW;
    }
    itm.tm_min = r2.val;
    cp = cp + 1 + r2.end;

    if cp >= bytes.len() {
        itm.tm_sec = 0;
        // If it's a MINUTE TO SECOND interval, take 2 fields as mm:ss
        if range == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
            if itm.tm_hour > i32::MAX as i64 || itm.tm_hour < i32::MIN as i64 {
                return DTERR_FIELD_OVERFLOW;
            }
            itm.tm_sec = itm.tm_min;
            itm.tm_min = itm.tm_hour as i32;
            itm.tm_hour = 0;
        }
    } else if bytes[cp] == b'.' {
        // always assume mm:ss.sss is MINUTE TO SECOND
        let dterr = ParseFractionalSecond(&str[cp..], &mut fsec);
        if dterr != 0 {
            return dterr;
        }
        if itm.tm_hour > i32::MAX as i64 || itm.tm_hour < i32::MIN as i64 {
            return DTERR_FIELD_OVERFLOW;
        }
        itm.tm_sec = itm.tm_min;
        itm.tm_min = itm.tm_hour as i32;
        itm.tm_hour = 0;
    } else if bytes[cp] == b':' {
        let r3 = strtoint(&str[cp + 1..]);
        if r3.erange {
            return DTERR_FIELD_OVERFLOW;
        }
        itm.tm_sec = r3.val;
        cp = cp + 1 + r3.end;
        if cp < bytes.len() && bytes[cp] == b'.' {
            let dterr = ParseFractionalSecond(&str[cp..], &mut fsec);
            if dterr != 0 {
                return dterr;
            }
        } else if cp < bytes.len() {
            return DTERR_BAD_FORMAT;
        }
    } else {
        return DTERR_BAD_FORMAT;
    }

    // sanity check (caller must check the range of tm_hour)
    if itm.tm_hour < 0
        || itm.tm_min < 0
        || itm.tm_min > MINS_PER_HOUR - 1
        || itm.tm_sec < 0
        || itm.tm_sec > SECS_PER_MINUTE
        || fsec < 0
        || (fsec as i64) > USECS_PER_SEC
    {
        return DTERR_FIELD_OVERFLOW;
    }

    itm.tm_usec = fsec;
    0
}

/// `DecodeTime()` -- decode a delimited time for timestamps, into
/// `tm`/`fsec`.  (`utils/adt/datetime.c`)
fn DecodeTime(
    str: &str,
    fmask: i32,
    range: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
) -> i32 {
    let mut itm = pg_itm {
        tm_usec: 0,
        tm_sec: 0,
        tm_min: 0,
        tm_hour: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
    };
    let dterr = DecodeTimeCommon(str, fmask, range, tmask, &mut itm);
    if dterr != 0 {
        return dterr;
    }
    if itm.tm_hour > i32::MAX as i64 {
        return DTERR_FIELD_OVERFLOW;
    }
    tm.tm_hour = itm.tm_hour as i32;
    tm.tm_min = itm.tm_min;
    tm.tm_sec = itm.tm_sec;
    *fsec = itm.tm_usec;
    0
}

/// `DecodeTimeForInterval()` -- decode a delimited time for intervals, into
/// `itm_in->tm_usec`.  (`utils/adt/datetime.c`)
fn DecodeTimeForInterval(
    str: &str,
    fmask: i32,
    range: i32,
    tmask: &mut i32,
    itm_in: &mut pg_itm_in,
) -> i32 {
    let mut itm = pg_itm {
        tm_usec: 0,
        tm_sec: 0,
        tm_min: 0,
        tm_hour: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
    };
    let dterr = DecodeTimeCommon(str, fmask, range, tmask, &mut itm);
    if dterr != 0 {
        return dterr;
    }
    itm_in.tm_usec = itm.tm_usec as i64;
    if !int64_multiply_add(itm.tm_hour, USECS_PER_HOUR, &mut itm_in.tm_usec)
        || !int64_multiply_add(itm.tm_min as i64, USECS_PER_MINUTE, &mut itm_in.tm_usec)
        || !int64_multiply_add(itm.tm_sec as i64, USECS_PER_SEC, &mut itm_in.tm_usec)
    {
        return DTERR_FIELD_OVERFLOW;
    }
    0
}

// ---------------------------------------------------------------------------
// DecodeNumber / DecodeNumberField
// ---------------------------------------------------------------------------

/// `DecodeNumber()` -- interpret a plain numeric field as a date value in
/// context.  Returns `0` or a DTERR code.  (`utils/adt/datetime.c`)
fn DecodeNumber(
    flen: i32,
    str: &str,
    haveTextMonth: bool,
    fmask: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    is2digits: &mut bool,
) -> i32 {
    *tmask = 0;

    let r = strtoint(str);
    if r.erange {
        return DTERR_FIELD_OVERFLOW;
    }
    if r.end == 0 {
        return DTERR_BAD_FORMAT;
    }
    let val = r.val;
    let cp = r.end;
    let bytes = str.as_bytes();

    if cp < bytes.len() && bytes[cp] == b'.' {
        // More than two digits before decimal point? could be date or run-together time
        if cp > 2 {
            let dterr =
                DecodeNumberField(flen, str, fmask | DTK_DATE_M, tmask, tm, fsec, is2digits);
            if dterr < 0 {
                return dterr;
            }
            return 0;
        }
        let dterr = ParseFractionalSecond(&str[cp..], fsec);
        if dterr != 0 {
            return dterr;
        }
    } else if cp < bytes.len() {
        return DTERR_BAD_FORMAT;
    }

    // Special case for day of year
    if flen == 3 && (fmask & DTK_DATE_M) == DTK_M(YEAR) && (1..=366).contains(&val) {
        *tmask = DTK_M(DOY) | DTK_M(MONTH) | DTK_M(DAY);
        tm.tm_yday = val;
        return 0;
    }

    // Switch based on what we have so far
    match fmask & DTK_DATE_M {
        0 => {
            if flen >= 3 || date_order() == DATEORDER_YMD {
                *tmask = DTK_M(YEAR);
                tm.tm_year = val;
            } else if date_order() == DATEORDER_DMY {
                *tmask = DTK_M(DAY);
                tm.tm_mday = val;
            } else {
                *tmask = DTK_M(MONTH);
                tm.tm_mon = val;
            }
        }
        m if m == DTK_M(YEAR) => {
            *tmask = DTK_M(MONTH);
            tm.tm_mon = val;
        }
        m if m == DTK_M(MONTH) => {
            if haveTextMonth {
                if flen >= 3 || date_order() == DATEORDER_YMD {
                    *tmask = DTK_M(YEAR);
                    tm.tm_year = val;
                } else {
                    *tmask = DTK_M(DAY);
                    tm.tm_mday = val;
                }
            } else {
                *tmask = DTK_M(DAY);
                tm.tm_mday = val;
            }
        }
        m if m == DTK_M(YEAR) | DTK_M(MONTH) => {
            if haveTextMonth {
                if flen >= 3 && *is2digits {
                    *tmask = DTK_M(DAY);
                    tm.tm_mday = tm.tm_year;
                    tm.tm_year = val;
                    *is2digits = false;
                } else {
                    *tmask = DTK_M(DAY);
                    tm.tm_mday = val;
                }
            } else {
                *tmask = DTK_M(DAY);
                tm.tm_mday = val;
            }
        }
        m if m == DTK_M(DAY) => {
            *tmask = DTK_M(MONTH);
            tm.tm_mon = val;
        }
        m if m == DTK_M(MONTH) | DTK_M(DAY) => {
            *tmask = DTK_M(YEAR);
            tm.tm_year = val;
        }
        m if m == DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY) => {
            let dterr = DecodeNumberField(flen, str, fmask, tmask, tm, fsec, is2digits);
            if dterr < 0 {
                return dterr;
            }
            return 0;
        }
        _ => return DTERR_BAD_FORMAT,
    }

    if *tmask == DTK_M(YEAR) {
        *is2digits = flen <= 2;
    }

    0
}

/// `DecodeNumberField()` -- interpret a numeric string as a concatenated date
/// or time field.  Returns a DTK token (`>= 0`) on success, a DTERR (`< 0`) on
/// failure.  (`utils/adt/datetime.c`)
fn DecodeNumberField(
    mut len: i32,
    str: &str,
    fmask: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    is2digits: &mut bool,
) -> i32 {
    // Reject if not digits and decimal point(s).
    if !str.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return DTERR_BAD_FORMAT;
    }

    // Work on an owned, mutable copy since C truncates in place.
    let mut s: String = String::from(str);

    // Have a decimal point?
    if let Some(dot) = s.find('.') {
        let dterr = ParseFractionalSecond(&s[dot..], fsec);
        if dterr != 0 {
            return dterr;
        }
        s.truncate(dot);
        len = s.len() as i32;
    }
    // No decimal point and no complete date yet?
    else if (fmask & DTK_DATE_M) != DTK_DATE_M && len >= 6 {
        *tmask = DTK_DATE_M;
        let ul = len as usize;
        tm.tm_mday = atoi(&s[ul - 2..]);
        tm.tm_mon = atoi(&s[ul - 4..ul - 2]);
        tm.tm_year = atoi(&s[..ul - 4]);
        if (len - 4) == 2 {
            *is2digits = true;
        }
        return DTK_DATE;
    }

    // not all time fields specified?
    if (fmask & DTK_TIME_M) != DTK_TIME_M {
        let ul = len as usize;
        if len == 6 {
            *tmask = DTK_TIME_M;
            tm.tm_sec = atoi(&s[4..]);
            tm.tm_min = atoi(&s[2..4]);
            tm.tm_hour = atoi(&s[..2]);
            return DTK_TIME;
        } else if len == 4 {
            *tmask = DTK_TIME_M;
            tm.tm_sec = 0;
            tm.tm_min = atoi(&s[2..]);
            tm.tm_hour = atoi(&s[..2]);
            let _ = ul;
            return DTK_TIME;
        }
    }

    DTERR_BAD_FORMAT
}

/// C `atoi()` over a digit prefix (callers guarantee leading digits).
fn atoi(s: &str) -> i32 {
    strtoint(s).val
}

// ---------------------------------------------------------------------------
// GetCurrentDateTime / GetCurrentTimeUsec
//
// In C these wrap timestamp2tm() (timestamp.c, a later phase).  Until that is
// ported, we reconstruct the broken-down current time from the transaction
// start timestamp via the reused timezone engine (pg_localtime), which yields
// the same y/m/d/h/m/s fields.  TODO(P2): route through the ported
// timestamp2tm once available.
// ---------------------------------------------------------------------------

/// `GetCurrentTimeUsec()` -- current transaction time, broken down into `tm`
/// (with full year / 1-based month, per datetime conventions), plus fractional
/// seconds and the GMT offset.  (`utils/adt/datetime.c`)
pub(crate) fn GetCurrentTimeUsec(tm: &mut pg_tm, fsec: &mut fsec_t, tzp: Option<&mut i32>) {
    let cur_ts: i64 = backend_access_transam_xact::GetCurrentTransactionStartTimestamp();
    let stz = session_timezone();

    // Postgres-epoch microseconds -> Unix seconds + leftover microseconds.
    let unix_usec = cur_ts + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * USECS_PER_DAY;
    let mut secs = unix_usec.div_euclid(USECS_PER_DAY) * SECS_PER_DAY as i64;
    let day_usec = unix_usec.rem_euclid(USECS_PER_DAY);
    secs += day_usec / USECS_PER_SEC;
    let leftover_usec = (day_usec % USECS_PER_SEC) as fsec_t;

    if let Some(local) = backend_timezone_localtime::pg_localtime(secs as pg_time_t, &stz) {
        // pg_localtime returns tm_year as (year-1900) and tm_mon 0-based;
        // datetime.c uses full year / 1-based month.
        tm.tm_sec = local.tm_sec;
        tm.tm_min = local.tm_min;
        tm.tm_hour = local.tm_hour;
        tm.tm_mday = local.tm_mday;
        tm.tm_mon = local.tm_mon + 1;
        tm.tm_year = local.tm_year + 1900;
        tm.tm_wday = local.tm_wday;
        tm.tm_yday = local.tm_yday;
        tm.tm_isdst = local.tm_isdst;
        tm.tm_gmtoff = local.tm_gmtoff;
        *fsec = leftover_usec;
        if let Some(tzp) = tzp {
            *tzp = -(local.tm_gmtoff as i32);
        }
    } else {
        *fsec = 0;
        if let Some(tzp) = tzp {
            *tzp = 0;
        }
    }
}

/// `GetCurrentDateTime()` -- like above but only the `tm` fields.
/// (`utils/adt/datetime.c`)
pub(crate) fn GetCurrentDateTime(tm: &mut pg_tm) {
    let mut fsec: fsec_t = 0;
    GetCurrentTimeUsec(tm, &mut fsec, None);
}

// ---------------------------------------------------------------------------
// DecodeDateTime() -- the big fmask-accumulating interpreter.
// ---------------------------------------------------------------------------

/// `DecodeDateTime()` -- interpret previously parsed fields as a general date
/// and time.  Returns `0` for a full date, `1` for time-only, or a negative
/// DTERR code.  Fills `dtype`/`tm`/`fsec`/`tzp` outputs.  `field`/`ftype` are
/// `&mut` because some branches edit them in place (mirroring the C buffer
/// mutation).  (`utils/adt/datetime.c`)
pub fn DecodeDateTime(
    field: &mut [String],
    ftype: &mut [i32],
    nf: usize,
    dtype: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzp: Option<&mut i32>,
) -> i32 {
    let mut fmask = 0;
    let mut tmask;
    let mut type_;
    let mut ptype = 0; // "prefix type" for ISO and Julian formats
    let mut val;
    let mut dterr;
    let mut mer = HR24;
    let mut haveTextMonth = false;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut namedTz: Option<Rc<pg_tz>> = None;
    let mut abbrevTz: Option<Rc<pg_tz>> = None;
    let mut valtz: Option<Rc<pg_tz>>;
    let mut abbrev: Option<String> = None;
    let mut cur_tm = pg_tm::default();

    // A local tzp value we accumulate into; we write through to the caller at
    // the very end.  `have_tzp` mirrors C's `tzp != NULL`.
    let have_tzp = tzp.is_some();
    let mut tzval: i32 = 0;

    *dtype = DTK_DATE;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    *fsec = 0;
    tm.tm_isdst = -1;
    // tzval already 0

    let mut i = 0usize;
    while i < nf {
        tmask = 0;
        match ftype[i] {
            t if t == DTK_DATE => {
                if ptype == DTK_JULIAN {
                    if !have_tzp {
                        return DTERR_BAD_FORMAT;
                    }
                    let r = strtoint(&field[i]);
                    if r.erange || r.val < 0 {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    let jday = r.val;
                    let (y, m, d) = j2date(jday);
                    tm.tm_year = y;
                    tm.tm_mon = m;
                    tm.tm_mday = d;
                    isjulian = true;

                    dterr = DecodeTimezone(&field[i][r.end..], &mut tzval);
                    if dterr != 0 {
                        return dterr;
                    }
                    tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
                    ptype = 0;
                } else if ptype != 0
                    || ((fmask & (DTK_M(MONTH) | DTK_M(DAY))) == (DTK_M(MONTH) | DTK_M(DAY)))
                {
                    if !have_tzp {
                        return DTERR_BAD_FORMAT;
                    }
                    let first_is_digit = field[i].as_bytes().first().is_some_and(|b| is_digit(*b));
                    if first_is_digit || ptype != 0 {
                        if ptype != 0 {
                            if ptype != DTK_TIME {
                                return DTERR_BAD_FORMAT;
                            }
                            ptype = 0;
                        }
                        if (fmask & DTK_TIME_M) == DTK_TIME_M {
                            return DTERR_BAD_FORMAT;
                        }
                        let Some(dashpos) = field[i].find('-') else {
                            return DTERR_BAD_FORMAT;
                        };
                        dterr = DecodeTimezone(&field[i][dashpos..], &mut tzval);
                        if dterr != 0 {
                            return dterr;
                        }
                        field[i].truncate(dashpos);
                        let flen = field[i].len() as i32;
                        let f = field[i].clone();
                        dterr = DecodeNumberField(
                            flen,
                            &f,
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                        tmask |= DTK_M(TZ);
                    } else {
                        match pg_tzset(&field[i]) {
                            Ok(Some(tz)) => {
                                namedTz = Some(tz);
                                tmask = DTK_M(TZ);
                            }
                            _ => return DTERR_BAD_TIMEZONE,
                        }
                    }
                } else {
                    let f = field[i].clone();
                    dterr = DecodeDate(&f, fmask, &mut tmask, &mut is2digits, tm);
                    if dterr != 0 {
                        return dterr;
                    }
                }
            }
            t if t == DTK_TIME => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }
                let f = field[i].clone();
                dterr = DecodeTime(&f, fmask, INTERVAL_FULL_RANGE, &mut tmask, tm, fsec);
                if dterr != 0 {
                    return dterr;
                }
                if time_overflows(tm.tm_hour, tm.tm_min, tm.tm_sec, *fsec) {
                    return DTERR_FIELD_OVERFLOW;
                }
            }
            t if t == DTK_TZ => {
                if !have_tzp {
                    return DTERR_BAD_FORMAT;
                }
                let mut tz = 0;
                dterr = DecodeTimezone(&field[i], &mut tz);
                if dterr != 0 {
                    return dterr;
                }
                tzval = tz;
                tmask = DTK_M(TZ);
            }
            t if t == DTK_NUMBER => {
                if ptype != 0 {
                    let r = strtoint(&field[i]);
                    if r.erange {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    let value = r.val;
                    let cp = r.end;
                    let cbytes = field[i].as_bytes();
                    if cp < cbytes.len() && cbytes[cp] != b'.' {
                        return DTERR_BAD_FORMAT;
                    }
                    match ptype {
                        p if p == DTK_JULIAN => {
                            if value < 0 {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            tmask = DTK_DATE_M;
                            let (y, m, d) = j2date(value);
                            tm.tm_year = y;
                            tm.tm_mon = m;
                            tm.tm_mday = d;
                            isjulian = true;
                            if cp < cbytes.len() && cbytes[cp] == b'.' {
                                let mut time = 0.0f64;
                                dterr = ParseFraction(&field[i][cp..], &mut time);
                                if dterr != 0 {
                                    return dterr;
                                }
                                time *= USECS_PER_DAY as f64;
                                dt2time(
                                    time as i64,
                                    &mut tm.tm_hour,
                                    &mut tm.tm_min,
                                    &mut tm.tm_sec,
                                    fsec,
                                );
                                tmask |= DTK_TIME_M;
                            }
                        }
                        p if p == DTK_TIME => {
                            let flen = field[i].len() as i32;
                            let f = field[i].clone();
                            dterr = DecodeNumberField(
                                flen,
                                &f,
                                fmask | DTK_DATE_M,
                                &mut tmask,
                                tm,
                                fsec,
                                &mut is2digits,
                            );
                            if dterr < 0 {
                                return dterr;
                            }
                            if tmask != DTK_TIME_M {
                                return DTERR_BAD_FORMAT;
                            }
                        }
                        _ => return DTERR_BAD_FORMAT,
                    }
                    ptype = 0;
                    *dtype = DTK_DATE;
                } else {
                    let flen = field[i].len() as i32;
                    let dotpos = field[i].find('.');

                    // Embedded decimal and no date yet?
                    if dotpos.is_some() && (fmask & DTK_DATE_M) == 0 {
                        let f = field[i].clone();
                        dterr = DecodeDate(&f, fmask, &mut tmask, &mut is2digits, tm);
                        if dterr != 0 {
                            return dterr;
                        }
                    }
                    // Embedded decimal and several digits before, or a 6+ digit
                    // YMD/HMS specification: interpret as a concatenated
                    // date or time field (e.g. 20011223 or 040506).
                    else if dotpos.is_some_and(|dp| flen - (field[i].len() - dp) as i32 > 2)
                        || (flen >= 6 && ((fmask & DTK_DATE_M == 0) || (fmask & DTK_TIME_M == 0)))
                    {
                        let f = field[i].clone();
                        dterr = DecodeNumberField(
                            flen,
                            &f,
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                    }
                    // Otherwise it is a single date/time field...
                    else {
                        let f = field[i].clone();
                        dterr = DecodeNumber(
                            flen,
                            &f,
                            haveTextMonth,
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr != 0 {
                            return dterr;
                        }
                    }
                }
            }
            t if t == DTK_STRING || t == DTK_SPECIAL => {
                let mut tz_opt: Option<Rc<pg_tz>> = None;
                let mut typ = 0;
                let mut v = 0;
                let mut dyntz_zone: Option<String> = None;
                dterr = DecodeTimezoneAbbrev(
                    i,
                    &field[i],
                    &mut typ,
                    &mut v,
                    &mut tz_opt,
                    &mut dyntz_zone,
                );
                if dterr != 0 {
                    return dterr;
                }
                type_ = typ;
                val = v;
                valtz = tz_opt;
                if type_ == UNKNOWN_FIELD {
                    type_ = DecodeSpecial(i, &field[i], &mut val);
                }
                if type_ == IGNORE_DTF {
                    i += 1;
                    continue;
                }

                tmask = DTK_M(type_);
                match type_ {
                    tt if tt == RESERV => match val {
                        v if v == DTK_NOW => {
                            tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
                            *dtype = DTK_DATE;
                            GetCurrentTimeUsec(tm, fsec, Some(&mut tzval));
                        }
                        v if v == DTK_YESTERDAY => {
                            tmask = DTK_DATE_M;
                            *dtype = DTK_DATE;
                            GetCurrentDateTime(&mut cur_tm);
                            let (y, m, d) =
                                j2date(date2j(cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday) - 1);
                            tm.tm_year = y;
                            tm.tm_mon = m;
                            tm.tm_mday = d;
                        }
                        v if v == DTK_TODAY => {
                            tmask = DTK_DATE_M;
                            *dtype = DTK_DATE;
                            GetCurrentDateTime(&mut cur_tm);
                            tm.tm_year = cur_tm.tm_year;
                            tm.tm_mon = cur_tm.tm_mon;
                            tm.tm_mday = cur_tm.tm_mday;
                        }
                        v if v == DTK_TOMORROW => {
                            tmask = DTK_DATE_M;
                            *dtype = DTK_DATE;
                            GetCurrentDateTime(&mut cur_tm);
                            let (y, m, d) =
                                j2date(date2j(cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday) + 1);
                            tm.tm_year = y;
                            tm.tm_mon = m;
                            tm.tm_mday = d;
                        }
                        v if v == DTK_ZULU => {
                            tmask = DTK_TIME_M | DTK_M(TZ);
                            *dtype = DTK_DATE;
                            tm.tm_hour = 0;
                            tm.tm_min = 0;
                            tm.tm_sec = 0;
                            tzval = 0;
                        }
                        v if v == DTK_EPOCH || v == DTK_LATE || v == DTK_EARLY => {
                            tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
                            *dtype = val;
                        }
                        _ => return DTERR_BAD_FORMAT,
                    },
                    tt if tt == MONTH => {
                        if (fmask & DTK_M(MONTH)) != 0
                            && !haveTextMonth
                            && (fmask & DTK_M(DAY)) == 0
                            && tm.tm_mon >= 1
                            && tm.tm_mon <= 31
                        {
                            tm.tm_mday = tm.tm_mon;
                            tmask = DTK_M(DAY);
                        }
                        haveTextMonth = true;
                        tm.tm_mon = val;
                    }
                    tt if tt == DTZMOD => {
                        tmask |= DTK_M(DTZ);
                        tm.tm_isdst = 1;
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        tzval -= val;
                    }
                    tt if tt == DTZ => {
                        tmask |= DTK_M(TZ);
                        tm.tm_isdst = 1;
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        tzval = -val;
                    }
                    tt if tt == TZ => {
                        tm.tm_isdst = 0;
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        tzval = -val;
                    }
                    tt if tt == DYNTZ => {
                        tmask |= DTK_M(TZ);
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        abbrevTz = valtz;
                        abbrev = Some(field[i].clone());
                    }
                    tt if tt == AMPM => {
                        mer = val;
                    }
                    tt if tt == ADBC => {
                        bc = val == BC;
                    }
                    tt if tt == DOW => {
                        tm.tm_wday = val;
                    }
                    tt if tt == UNITS => {
                        tmask = 0;
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    tt if tt == ISOTIME => {
                        tmask = 0;
                        if (fmask & DTK_DATE_M) != DTK_DATE_M {
                            return DTERR_BAD_FORMAT;
                        }
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    tt if tt == UNKNOWN_FIELD => {
                        match pg_tzset(&field[i]) {
                            Ok(Some(tz)) => {
                                namedTz = Some(tz);
                                tmask = DTK_M(TZ);
                            }
                            _ => return DTERR_BAD_FORMAT,
                        }
                    }
                    _ => return DTERR_BAD_FORMAT,
                }
            }
            _ => return DTERR_BAD_FORMAT,
        }

        if tmask & fmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;
        i += 1;
    }

    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    if *dtype == DTK_DATE {
        dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
        if dterr != 0 {
            return dterr;
        }

        // handle AM/PM
        if mer != HR24 && tm.tm_hour > HOURS_PER_DAY / 2 {
            return DTERR_FIELD_OVERFLOW;
        }
        if mer == AM && tm.tm_hour == HOURS_PER_DAY / 2 {
            tm.tm_hour = 0;
        } else if mer == PM && tm.tm_hour != HOURS_PER_DAY / 2 {
            tm.tm_hour += HOURS_PER_DAY / 2;
        }

        // check for incomplete input
        if (fmask & DTK_DATE_M) != DTK_DATE_M {
            if (fmask & DTK_TIME_M) == DTK_TIME_M {
                if let Some(tzp) = tzp {
                    *tzp = tzval;
                }
                return 1;
            }
            return DTERR_BAD_FORMAT;
        }

        if let Some(tz) = namedTz {
            if fmask & DTK_M(DTZMOD) != 0 {
                return DTERR_BAD_FORMAT;
            }
            tzval = DetermineTimeZoneOffset(tm, &tz);
        }

        if let Some(tz) = abbrevTz {
            if fmask & DTK_M(DTZMOD) != 0 {
                return DTERR_BAD_FORMAT;
            }
            tzval = DetermineTimeZoneAbbrevOffset(tm, abbrev.as_deref().unwrap_or(""), &tz);
        }

        // timezone not specified? then use session timezone
        if have_tzp && (fmask & DTK_M(TZ)) == 0 {
            if fmask & DTK_M(DTZMOD) != 0 {
                return DTERR_BAD_FORMAT;
            }
            tzval = DetermineTimeZoneOffset(tm, &session_timezone());
        }
    }

    if let Some(tzp) = tzp {
        *tzp = tzval;
    }
    0
}

// ---------------------------------------------------------------------------
// DecodeTimeOnly()
// ---------------------------------------------------------------------------

/// `DecodeTimeOnly()` -- interpret parsed fields as time-only (optionally with
/// time zone).  Returns `0` on success or a negative DTERR code.  `field`/
/// `ftype` are `&mut` to mirror the C in-place edits.  (`utils/adt/datetime.c`)
pub fn DecodeTimeOnly(
    field: &mut [String],
    ftype: &mut [i32],
    nf: usize,
    dtype: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzp: Option<&mut i32>,
) -> i32 {
    let mut fmask = 0;
    let mut tmask;
    let mut type_;
    let mut ptype = 0;
    let mut val;
    let mut dterr;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut mer = HR24;
    let mut namedTz: Option<Rc<pg_tz>> = None;
    let mut abbrevTz: Option<Rc<pg_tz>> = None;
    let mut abbrev: Option<String> = None;
    let mut valtz: Option<Rc<pg_tz>>;

    let have_tzp = tzp.is_some();
    let mut tzval: i32 = 0;

    *dtype = DTK_TIME;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    *fsec = 0;
    tm.tm_isdst = -1;

    let mut i = 0usize;
    while i < nf {
        tmask = 0;
        match ftype[i] {
            t if t == DTK_DATE => {
                if !have_tzp {
                    return DTERR_BAD_FORMAT;
                }
                if i == 0 && nf >= 2 && (ftype[nf - 1] == DTK_DATE || ftype[1] == DTK_TIME) {
                    let f = field[i].clone();
                    dterr = DecodeDate(&f, fmask, &mut tmask, &mut is2digits, tm);
                    if dterr != 0 {
                        return dterr;
                    }
                } else if field[i].as_bytes().first().is_some_and(|b| is_digit(*b)) {
                    if (fmask & DTK_TIME_M) == DTK_TIME_M {
                        return DTERR_BAD_FORMAT;
                    }
                    let Some(dashpos) = field[i].find('-') else {
                        return DTERR_BAD_FORMAT;
                    };
                    dterr = DecodeTimezone(&field[i][dashpos..], &mut tzval);
                    if dterr != 0 {
                        return dterr;
                    }
                    field[i].truncate(dashpos);
                    let flen = field[i].len() as i32;
                    let f = field[i].clone();
                    dterr = DecodeNumberField(
                        flen,
                        &f,
                        fmask | DTK_DATE_M,
                        &mut tmask,
                        tm,
                        fsec,
                        &mut is2digits,
                    );
                    if dterr < 0 {
                        return dterr;
                    }
                    ftype[i] = dterr;
                    tmask |= DTK_M(TZ);
                } else {
                    match pg_tzset(&field[i]) {
                        Ok(Some(tz)) => {
                            namedTz = Some(tz);
                            ftype[i] = DTK_TZ;
                            tmask = DTK_M(TZ);
                        }
                        _ => return DTERR_BAD_TIMEZONE,
                    }
                }
            }
            t if t == DTK_TIME => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }
                let f = field[i].clone();
                dterr = DecodeTime(
                    &f,
                    fmask | DTK_DATE_M,
                    INTERVAL_FULL_RANGE,
                    &mut tmask,
                    tm,
                    fsec,
                );
                if dterr != 0 {
                    return dterr;
                }
            }
            t if t == DTK_TZ => {
                if !have_tzp {
                    return DTERR_BAD_FORMAT;
                }
                let mut tz = 0;
                dterr = DecodeTimezone(&field[i], &mut tz);
                if dterr != 0 {
                    return dterr;
                }
                tzval = tz;
                tmask = DTK_M(TZ);
            }
            t if t == DTK_NUMBER => {
                if ptype != 0 {
                    let r = strtoint(&field[i]);
                    if r.erange {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    let value = r.val;
                    let cp = r.end;
                    let cbytes = field[i].as_bytes();
                    if cp < cbytes.len() && cbytes[cp] != b'.' {
                        return DTERR_BAD_FORMAT;
                    }
                    match ptype {
                        p if p == DTK_JULIAN => {
                            if !have_tzp {
                                return DTERR_BAD_FORMAT;
                            }
                            if value < 0 {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            tmask = DTK_DATE_M;
                            let (y, m, d) = j2date(value);
                            tm.tm_year = y;
                            tm.tm_mon = m;
                            tm.tm_mday = d;
                            isjulian = true;
                            if cp < cbytes.len() && cbytes[cp] == b'.' {
                                let mut time = 0.0f64;
                                dterr = ParseFraction(&field[i][cp..], &mut time);
                                if dterr != 0 {
                                    return dterr;
                                }
                                time *= USECS_PER_DAY as f64;
                                dt2time(
                                    time as i64,
                                    &mut tm.tm_hour,
                                    &mut tm.tm_min,
                                    &mut tm.tm_sec,
                                    fsec,
                                );
                                tmask |= DTK_TIME_M;
                            }
                        }
                        p if p == DTK_TIME => {
                            let flen = field[i].len() as i32;
                            let f = field[i].clone();
                            dterr = DecodeNumberField(
                                flen,
                                &f,
                                fmask | DTK_DATE_M,
                                &mut tmask,
                                tm,
                                fsec,
                                &mut is2digits,
                            );
                            if dterr < 0 {
                                return dterr;
                            }
                            ftype[i] = dterr;
                            if tmask != DTK_TIME_M {
                                return DTERR_BAD_FORMAT;
                            }
                        }
                        _ => return DTERR_BAD_FORMAT,
                    }
                    ptype = 0;
                    *dtype = DTK_DATE;
                } else {
                    let flen = field[i].len() as i32;
                    let dotpos = field[i].find('.');
                    if let Some(dp) = dotpos {
                        if i == 0 && nf >= 2 && ftype[nf - 1] == DTK_DATE {
                            let f = field[i].clone();
                            dterr = DecodeDate(&f, fmask, &mut tmask, &mut is2digits, tm);
                            if dterr != 0 {
                                return dterr;
                            }
                        } else if flen - (field[i].len() - dp) as i32 > 2 {
                            let f = field[i].clone();
                            dterr = DecodeNumberField(
                                flen,
                                &f,
                                fmask | DTK_DATE_M,
                                &mut tmask,
                                tm,
                                fsec,
                                &mut is2digits,
                            );
                            if dterr < 0 {
                                return dterr;
                            }
                            ftype[i] = dterr;
                        } else {
                            return DTERR_BAD_FORMAT;
                        }
                    } else if flen > 4 {
                        let f = field[i].clone();
                        dterr = DecodeNumberField(
                            flen,
                            &f,
                            fmask | DTK_DATE_M,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                        ftype[i] = dterr;
                    } else {
                        let f = field[i].clone();
                        dterr = DecodeNumber(
                            flen,
                            &f,
                            false,
                            fmask | DTK_DATE_M,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr != 0 {
                            return dterr;
                        }
                    }
                }
            }
            t if t == DTK_STRING || t == DTK_SPECIAL => {
                let mut tz_opt: Option<Rc<pg_tz>> = None;
                let mut typ = 0;
                let mut v = 0;
                let mut dyntz_zone: Option<String> = None;
                dterr = DecodeTimezoneAbbrev(
                    i,
                    &field[i],
                    &mut typ,
                    &mut v,
                    &mut tz_opt,
                    &mut dyntz_zone,
                );
                if dterr != 0 {
                    return dterr;
                }
                type_ = typ;
                val = v;
                valtz = tz_opt;
                if type_ == UNKNOWN_FIELD {
                    type_ = DecodeSpecial(i, &field[i], &mut val);
                }
                if type_ == IGNORE_DTF {
                    i += 1;
                    continue;
                }

                tmask = DTK_M(type_);
                match type_ {
                    tt if tt == RESERV => match val {
                        v if v == DTK_NOW => {
                            tmask = DTK_TIME_M;
                            *dtype = DTK_TIME;
                            GetCurrentTimeUsec(tm, fsec, None);
                        }
                        v if v == DTK_ZULU => {
                            tmask = DTK_TIME_M | DTK_M(TZ);
                            *dtype = DTK_TIME;
                            tm.tm_hour = 0;
                            tm.tm_min = 0;
                            tm.tm_sec = 0;
                            tm.tm_isdst = 0;
                        }
                        _ => return DTERR_BAD_FORMAT,
                    },
                    tt if tt == DTZMOD => {
                        tmask |= DTK_M(DTZ);
                        tm.tm_isdst = 1;
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        tzval -= val;
                    }
                    tt if tt == DTZ => {
                        tmask |= DTK_M(TZ);
                        tm.tm_isdst = 1;
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        tzval = -val;
                        ftype[i] = DTK_TZ;
                    }
                    tt if tt == TZ => {
                        tm.tm_isdst = 0;
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        tzval = -val;
                        ftype[i] = DTK_TZ;
                    }
                    tt if tt == DYNTZ => {
                        tmask |= DTK_M(TZ);
                        if !have_tzp {
                            return DTERR_BAD_FORMAT;
                        }
                        abbrevTz = valtz;
                        abbrev = Some(field[i].clone());
                        ftype[i] = DTK_TZ;
                    }
                    tt if tt == AMPM => {
                        mer = val;
                    }
                    tt if tt == ADBC => {
                        bc = val == BC;
                    }
                    tt if tt == UNITS => {
                        tmask = 0;
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    tt if tt == ISOTIME => {
                        tmask = 0;
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    tt if tt == UNKNOWN_FIELD => {
                        match pg_tzset(&field[i]) {
                            Ok(Some(tz)) => {
                                namedTz = Some(tz);
                                tmask = DTK_M(TZ);
                            }
                            _ => return DTERR_BAD_FORMAT,
                        }
                    }
                    _ => return DTERR_BAD_FORMAT,
                }
            }
            _ => return DTERR_BAD_FORMAT,
        }

        if tmask & fmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;
        i += 1;
    }

    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
    if dterr != 0 {
        return dterr;
    }

    if mer != HR24 && tm.tm_hour > HOURS_PER_DAY / 2 {
        return DTERR_FIELD_OVERFLOW;
    }
    if mer == AM && tm.tm_hour == HOURS_PER_DAY / 2 {
        tm.tm_hour = 0;
    } else if mer == PM && tm.tm_hour != HOURS_PER_DAY / 2 {
        tm.tm_hour += HOURS_PER_DAY / 2;
    }

    if time_overflows(tm.tm_hour, tm.tm_min, tm.tm_sec, *fsec) {
        return DTERR_FIELD_OVERFLOW;
    }

    if (fmask & DTK_TIME_M) != DTK_TIME_M {
        return DTERR_BAD_FORMAT;
    }

    if let Some(tz) = namedTz {
        if fmask & DTK_M(DTZMOD) != 0 {
            return DTERR_BAD_FORMAT;
        }
        // if non-DST zone, we do not need to know the date
        if let Some(gmtoff) = backend_timezone_localtime::pg_get_timezone_offset(&tz) {
            tzval = -(gmtoff as i32);
        } else {
            if (fmask & DTK_DATE_M) != DTK_DATE_M {
                return DTERR_BAD_FORMAT;
            }
            tzval = DetermineTimeZoneOffset(tm, &tz);
        }
    }

    if let Some(tz) = abbrevTz {
        if fmask & DTK_M(DTZMOD) != 0 {
            return DTERR_BAD_FORMAT;
        }
        let mut tt = pg_tm::default();
        if (fmask & DTK_DATE_M) == 0 {
            GetCurrentDateTime(&mut tt);
        } else {
            if (fmask & DTK_DATE_M) != DTK_DATE_M {
                return DTERR_BAD_FORMAT;
            }
            tt.tm_year = tm.tm_year;
            tt.tm_mon = tm.tm_mon;
            tt.tm_mday = tm.tm_mday;
        }
        tt.tm_hour = tm.tm_hour;
        tt.tm_min = tm.tm_min;
        tt.tm_sec = tm.tm_sec;
        tzval = DetermineTimeZoneAbbrevOffset(&mut tt, abbrev.as_deref().unwrap_or(""), &tz);
        tm.tm_isdst = tt.tm_isdst;
    }

    if have_tzp && (fmask & DTK_M(TZ)) == 0 {
        if fmask & DTK_M(DTZMOD) != 0 {
            return DTERR_BAD_FORMAT;
        }
        let mut tt = pg_tm::default();
        if (fmask & DTK_DATE_M) == 0 {
            GetCurrentDateTime(&mut tt);
        } else {
            if (fmask & DTK_DATE_M) != DTK_DATE_M {
                return DTERR_BAD_FORMAT;
            }
            tt.tm_year = tm.tm_year;
            tt.tm_mon = tm.tm_mon;
            tt.tm_mday = tm.tm_mday;
        }
        tt.tm_hour = tm.tm_hour;
        tt.tm_min = tm.tm_min;
        tt.tm_sec = tm.tm_sec;
        tzval = DetermineTimeZoneOffset(&mut tt, &session_timezone());
        tm.tm_isdst = tt.tm_isdst;
    }

    if let Some(tzp) = tzp {
        *tzp = tzval;
    }
    0
}

// ---------------------------------------------------------------------------
// DecodeInterval()
// ---------------------------------------------------------------------------

/// `DecodeInterval()` -- interpret parsed fields as a time interval.  Returns
/// `0` on success or a negative DTERR code; fills `dtype`/`itm_in`.
/// (`utils/adt/datetime.c`)
pub fn DecodeInterval(
    field: &mut [String],
    ftype: &mut [i32],
    nf: usize,
    range: i32,
    dtype: &mut i32,
    itm_in: &mut pg_itm_in,
) -> i32 {
    let mut force_negative = false;
    let mut is_before = false;
    let mut parsing_unit_val = false;
    let mut fmask = 0;
    let mut tmask;
    let mut type_ = IGNORE_DTF;
    let mut uval;
    let mut dterr;
    let mut val: i64;
    let mut fval: f64;

    *dtype = DTK_DELTA;
    ClearPgItmIn(itm_in);

    // SQL_STANDARD leading-sign handling.
    if interval_style() == INTSTYLE_SQL_STANDARD
        && nf > 0
        && field[0].as_bytes().first() == Some(&b'-')
    {
        force_negative = true;
        for f in field.iter().take(nf).skip(1) {
            let b = f.as_bytes().first().copied();
            if b == Some(b'-') || b == Some(b'+') {
                force_negative = false;
                break;
            }
        }
    }

    // read through list backwards to pick up units before values
    let mut i = nf as i64 - 1;
    while i >= 0 {
        let idx = i as usize;
        tmask = 0;
        let mut handled = false;
        match ftype[idx] {
            t if t == DTK_TIME => {
                dterr = DecodeTimeForInterval(&field[idx], fmask, range, &mut tmask, itm_in);
                if dterr != 0 {
                    return dterr;
                }
                if force_negative && itm_in.tm_usec > 0 {
                    itm_in.tm_usec = -itm_in.tm_usec;
                }
                type_ = DTK_DAY;
                parsing_unit_val = false;
                handled = true;
            }
            t if t == DTK_TZ => {
                // Check for signed hh:mm[:ss].
                let rest = &field[idx][1..];
                let mut tmp_tmask = 0;
                if rest.contains(':')
                    && DecodeTimeForInterval(rest, fmask, range, &mut tmp_tmask, itm_in) == 0
                {
                    tmask = tmp_tmask;
                    if field[idx].as_bytes()[0] == b'-' {
                        if itm_in.tm_usec == i64::MIN {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        itm_in.tm_usec = -itm_in.tm_usec;
                    }
                    if force_negative && itm_in.tm_usec > 0 {
                        itm_in.tm_usec = -itm_in.tm_usec;
                    }
                    type_ = DTK_DAY;
                    parsing_unit_val = false;
                    handled = true;
                }
                // else fall through to DTK_NUMBER handling below.
            }
            _ => {}
        }

        if !handled && (ftype[idx] == DTK_TZ || ftype[idx] == DTK_DATE || ftype[idx] == DTK_NUMBER)
        {
            // DTK_DATE / DTK_NUMBER / (fallthrough) DTK_TZ
            if type_ == IGNORE_DTF {
                type_ = match range {
                    r if r == INTERVAL_MASK(YEAR) => DTK_YEAR,
                    r if r == INTERVAL_MASK(MONTH)
                        || r == INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH) =>
                    {
                        DTK_MONTH
                    }
                    r if r == INTERVAL_MASK(DAY) => DTK_DAY,
                    r if r == INTERVAL_MASK(HOUR)
                        || r == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) =>
                    {
                        DTK_HOUR
                    }
                    r if r == INTERVAL_MASK(MINUTE)
                        || r == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
                        || r == INTERVAL_MASK(DAY)
                            | INTERVAL_MASK(HOUR)
                            | INTERVAL_MASK(MINUTE) =>
                    {
                        DTK_MINUTE
                    }
                    _ => DTK_SECOND,
                };
            }

            let r = strtoi64(&field[idx]);
            if r.erange {
                return DTERR_FIELD_OVERFLOW;
            }
            val = r.val;
            let mut cp = r.end;
            let cbytes = field[idx].as_bytes();

            if cp < cbytes.len() && cbytes[cp] == b'-' {
                // SQL "years-months" syntax
                let r2 = strtoint(&field[idx][cp + 1..]);
                let val2 = r2.val;
                if r2.erange || !(0..MONTHS_PER_YEAR).contains(&val2) {
                    return DTERR_FIELD_OVERFLOW;
                }
                cp = cp + 1 + r2.end;
                if cp < cbytes.len() {
                    return DTERR_BAD_FORMAT;
                }
                type_ = DTK_MONTH;
                let val2 = if cbytes[0] == b'-' { -val2 } else { val2 };
                let mut tmp = 0i64;
                if pg_mul_s64_overflow(val, MONTHS_PER_YEAR as i64, &mut tmp) {
                    return DTERR_FIELD_OVERFLOW;
                }
                val = tmp;
                if pg_add_s64_overflow(val, val2 as i64, &mut tmp) {
                    return DTERR_FIELD_OVERFLOW;
                }
                val = tmp;
                fval = 0.0;
            } else if cp < cbytes.len() && cbytes[cp] == b'.' {
                fval = 0.0;
                dterr = ParseFraction(&field[idx][cp..], &mut fval);
                if dterr != 0 {
                    return dterr;
                }
                if cbytes[0] == b'-' {
                    fval = -fval;
                }
            } else if cp >= cbytes.len() {
                fval = 0.0;
            } else {
                return DTERR_BAD_FORMAT;
            }

            tmask = 0;

            if force_negative {
                if val > 0 {
                    val = -val;
                }
                if fval > 0.0 {
                    fval = -fval;
                }
            }

            match type_ {
                tt if tt == DTK_MICROSEC => {
                    if !AdjustMicroseconds(val, fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MICROSECOND);
                }
                tt if tt == DTK_MILLISEC => {
                    if !AdjustMicroseconds(val, fval, 1000, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MILLISECOND);
                }
                tt if tt == DTK_SECOND => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = if fval == 0.0 {
                        DTK_M(SECOND)
                    } else {
                        DTK_ALL_SECS_M
                    };
                }
                tt if tt == DTK_MINUTE => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MINUTE);
                }
                tt if tt == DTK_HOUR => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(HOUR);
                    type_ = DTK_DAY;
                }
                tt if tt == DTK_DAY => {
                    if !AdjustDays(val, 1, itm_in)
                        || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(DAY);
                }
                tt if tt == DTK_WEEK => {
                    if !AdjustDays(val, 7, itm_in) || !AdjustFractDays(fval, 7, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(WEEK);
                }
                tt if tt == DTK_MONTH => {
                    if !AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MONTH);
                }
                tt if tt == DTK_YEAR => {
                    if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(YEAR);
                }
                tt if tt == DTK_DECADE => {
                    if !AdjustYears(val, 10, itm_in) || !AdjustFractYears(fval, 10, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(DECADE);
                }
                tt if tt == DTK_CENTURY => {
                    if !AdjustYears(val, 100, itm_in) || !AdjustFractYears(fval, 100, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(CENTURY);
                }
                tt if tt == DTK_MILLENNIUM => {
                    if !AdjustYears(val, 1000, itm_in) || !AdjustFractYears(fval, 1000, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MILLENNIUM);
                }
                _ => return DTERR_BAD_FORMAT,
            }
            parsing_unit_val = false;
            handled = true;
        }

        if !handled {
            match ftype[idx] {
                t if t == DTK_STRING || t == DTK_SPECIAL => {
                    if parsing_unit_val {
                        return DTERR_BAD_FORMAT;
                    }
                    let mut u = 0;
                    type_ = DecodeUnits(idx, &field[idx], &mut u);
                    uval = u;
                    if type_ == UNKNOWN_FIELD {
                        type_ = DecodeSpecial(idx, &field[idx], &mut uval);
                    }
                    if type_ == IGNORE_DTF {
                        i -= 1;
                        continue;
                    }

                    tmask = 0;
                    match type_ {
                        tt if tt == UNITS => {
                            type_ = uval;
                            parsing_unit_val = true;
                        }
                        tt if tt == AGO => {
                            if idx != nf - 1 {
                                return DTERR_BAD_FORMAT;
                            }
                            is_before = true;
                            type_ = uval;
                        }
                        tt if tt == RESERV => {
                            tmask = DTK_DATE_M | DTK_TIME_M;
                            if uval != DTK_LATE && uval != DTK_EARLY {
                                return DTERR_BAD_FORMAT;
                            }
                            if idx != nf - 1 {
                                return DTERR_BAD_FORMAT;
                            }
                            *dtype = uval;
                        }
                        _ => return DTERR_BAD_FORMAT,
                    }
                }
                _ => return DTERR_BAD_FORMAT,
            }
        }

        if tmask & fmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;
        i -= 1;
    }

    if fmask == 0 {
        return DTERR_BAD_FORMAT;
    }

    if parsing_unit_val {
        return DTERR_BAD_FORMAT;
    }

    if is_before {
        if itm_in.tm_usec == i64::MIN
            || itm_in.tm_mday == i32::MIN
            || itm_in.tm_mon == i32::MIN
            || itm_in.tm_year == i32::MIN
        {
            return DTERR_FIELD_OVERFLOW;
        }
        itm_in.tm_usec = -itm_in.tm_usec;
        itm_in.tm_mday = -itm_in.tm_mday;
        itm_in.tm_mon = -itm_in.tm_mon;
        itm_in.tm_year = -itm_in.tm_year;
    }

    0
}

// ---------------------------------------------------------------------------
// ISO 8601 interval support
// ---------------------------------------------------------------------------

/// `ParseISO8601Number()` -- parse a decimal value, splitting into integer and
/// fractional parts; `*end` is set just past the parsed substring (byte
/// offset).  Returns `0` or a DTERR code.  (`utils/adt/datetime.c`)
fn ParseISO8601Number(str: &str, end: &mut usize, ipart: &mut i64, fpart: &mut f64) -> i32 {
    let b = str.as_bytes();
    if !(b.first().is_some_and(|c| is_digit(*c))
        || b.first() == Some(&b'-')
        || b.first() == Some(&b'.'))
    {
        return DTERR_BAD_FORMAT;
    }
    let Some((val, e)) = strtod_prefix(str) else {
        return DTERR_BAD_FORMAT;
    };
    if e == 0 {
        return DTERR_BAD_FORMAT;
    }
    *end = e;
    if val.is_nan() || !(-1.0e15..=1.0e15).contains(&val) {
        return DTERR_FIELD_OVERFLOW;
    }
    if val >= 0.0 {
        *ipart = val.floor() as i64;
    } else {
        *ipart = -((-val).floor() as i64);
    }
    *fpart = val - *ipart as f64;
    0
}

/// `ISO8601IntegerWidth()` -- number of integral digits in a valid ISO 8601
/// number field (ignoring sign and fraction).  (`utils/adt/datetime.c`)
fn ISO8601IntegerWidth(fieldstart: &str) -> i32 {
    let b = fieldstart.as_bytes();
    let mut i = 0;
    if b.first() == Some(&b'-') {
        i = 1;
    }
    let mut n = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        n += 1;
        i += 1;
    }
    n
}

/// `DecodeISO8601Interval()` -- decode an ISO 8601 interval (designator or
/// alternative format).  Returns `0` on success or a DTERR code; fills `dtype`
/// and `itm_in`.  (`utils/adt/datetime.c`)
pub fn DecodeISO8601Interval(str: &str, dtype: &mut i32, itm_in: &mut pg_itm_in) -> i32 {
    let mut datepart = true;
    let mut havefield = false;

    *dtype = DTK_DELTA;
    ClearPgItmIn(itm_in);

    if str.len() < 2 || str.as_bytes()[0] != b'P' {
        return DTERR_BAD_FORMAT;
    }

    // Work over a byte cursor into `str`.
    let bytes = str.as_bytes();
    let mut pos = 1usize; // skip 'P'

    while pos < bytes.len() {
        if bytes[pos] == b'T' {
            datepart = false;
            havefield = false;
            pos += 1;
            continue;
        }

        let fieldstart = pos;
        let mut val: i64 = 0;
        let mut fval: f64 = 0.0;
        let mut adv = 0usize;
        let dterr = ParseISO8601Number(&str[pos..], &mut adv, &mut val, &mut fval);
        if dterr != 0 {
            return dterr;
        }
        pos += adv;

        // Note: we could step off the end of the string here; must exit if unit == '\0'.
        let unit = if pos < bytes.len() { bytes[pos] } else { b'\0' };
        if pos < bytes.len() {
            pos += 1;
        }

        if datepart {
            match unit {
                b'Y' => {
                    if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'M' => {
                    if !AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'W' => {
                    if !AdjustDays(val, 7, itm_in) || !AdjustFractDays(fval, 7, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'D' => {
                    if !AdjustDays(val, 1, itm_in)
                        || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'T' | b'\0' | b'-' => {
                    // alternative-format handling
                    let mut fell_through = unit == b'-';
                    if unit == b'T' || unit == b'\0' {
                        if ISO8601IntegerWidth(&str[fieldstart..]) == 8 && !havefield {
                            if !AdjustYears(val / 10000, 1, itm_in)
                                || !AdjustMonths((val / 100) % 100, itm_in)
                                || !AdjustDays(val % 100, 1, itm_in)
                                || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                            {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            if unit == b'\0' {
                                return 0;
                            }
                            datepart = false;
                            havefield = false;
                            continue;
                        }
                        fell_through = true;
                    }
                    if fell_through {
                        // ISO 8601 4.4.3.3 Alternative Format, Extended
                        if havefield {
                            return DTERR_BAD_FORMAT;
                        }
                        if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if unit == b'\0' {
                            return 0;
                        }
                        if unit == b'T' {
                            datepart = false;
                            havefield = false;
                            continue;
                        }

                        let mut adv2 = 0;
                        let dterr = ParseISO8601Number(&str[pos..], &mut adv2, &mut val, &mut fval);
                        if dterr != 0 {
                            return dterr;
                        }
                        pos += adv2;
                        if !AdjustMonths(val, itm_in)
                            || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if pos >= bytes.len() {
                            return 0;
                        }
                        if bytes[pos] == b'T' {
                            datepart = false;
                            havefield = false;
                            pos += 1;
                            continue;
                        }
                        if bytes[pos] != b'-' {
                            return DTERR_BAD_FORMAT;
                        }
                        pos += 1;

                        let mut adv3 = 0;
                        let dterr = ParseISO8601Number(&str[pos..], &mut adv3, &mut val, &mut fval);
                        if dterr != 0 {
                            return dterr;
                        }
                        pos += adv3;
                        if !AdjustDays(val, 1, itm_in)
                            || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if pos >= bytes.len() {
                            return 0;
                        }
                        if bytes[pos] == b'T' {
                            datepart = false;
                            havefield = false;
                            pos += 1;
                            continue;
                        }
                        return DTERR_BAD_FORMAT;
                    }
                }
                _ => return DTERR_BAD_FORMAT,
            }
        } else {
            match unit {
                b'H' => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'M' => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'S' => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'\0' | b':' => {
                    let mut fell_through = unit == b':';
                    if unit == b'\0' {
                        if ISO8601IntegerWidth(&str[fieldstart..]) == 6 && !havefield {
                            if !AdjustMicroseconds(val / 10000, 0.0, USECS_PER_HOUR, itm_in)
                                || !AdjustMicroseconds(
                                    (val / 100) % 100,
                                    0.0,
                                    USECS_PER_MINUTE,
                                    itm_in,
                                )
                                || !AdjustMicroseconds(val % 100, 0.0, USECS_PER_SEC, itm_in)
                                || !AdjustFractMicroseconds(fval, 1, itm_in)
                            {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            return 0;
                        }
                        fell_through = true;
                    }
                    if fell_through {
                        if havefield {
                            return DTERR_BAD_FORMAT;
                        }
                        if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if unit == b'\0' {
                            return 0;
                        }

                        let mut adv2 = 0;
                        let dterr = ParseISO8601Number(&str[pos..], &mut adv2, &mut val, &mut fval);
                        if dterr != 0 {
                            return dterr;
                        }
                        pos += adv2;
                        if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if pos >= bytes.len() {
                            return 0;
                        }
                        if bytes[pos] != b':' {
                            return DTERR_BAD_FORMAT;
                        }
                        pos += 1;

                        let mut adv3 = 0;
                        let dterr = ParseISO8601Number(&str[pos..], &mut adv3, &mut val, &mut fval);
                        if dterr != 0 {
                            return dterr;
                        }
                        pos += adv3;
                        if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if pos >= bytes.len() {
                            return 0;
                        }
                        return DTERR_BAD_FORMAT;
                    }
                }
                _ => return DTERR_BAD_FORMAT,
            }
        }

        havefield = true;
    }

    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::{date_order, set_date_order};

    fn fresh_tm() -> pg_tm {
        pg_tm::default()
    }

    fn parse(input: &str) -> (Vec<String>, Vec<i32>, usize, i32) {
        parse_buflen(input, MAXDATELEN as usize + MAXDATEFIELDS as usize)
    }

    fn parse_buflen(input: &str, buflen: usize) -> (Vec<String>, Vec<i32>, usize, i32) {
        let mut field = Vec::new();
        let mut ftype = Vec::new();
        let mut nf = 0usize;
        let rc = ParseDateTime(
            input,
            buflen,
            &mut field,
            &mut ftype,
            MAXDATEFIELDS as usize,
            &mut nf,
        );
        (field, ftype, nf, rc)
    }

    #[test]
    fn strtoi64_no_digits_matches_strtol_endptr() {
        // C strtol/strtoll set *endptr == str (offset 0) when no digits are
        // consumed, even after skipping a sign or whitespace. Our `end` must be
        // 0 in those cases so the `cp == str` / `r.end == 0` no-progress check
        // (DecodeNumber, datetime.c:2793) fires identically to C.
        for s in ["", "+", "-", "   ", " +", "+x", "-:", "  -"] {
            let r = strtoi64(s);
            assert_eq!(r.val, 0, "val for {s:?}");
            assert_eq!(
                r.end, 0,
                "end for {s:?} should be 0 (strtol *endptr == str)"
            );
            assert!(!r.erange, "erange for {s:?}");
            // strtoint shares the same no-progress semantics.
            assert_eq!(strtoint(s).end, 0, "strtoint end for {s:?}");
        }
        // Sanity: when digits *are* consumed, end advances past them as before.
        assert_eq!(strtoi64("42x").end, 2);
        assert_eq!(strtoi64("-7:00").end, 2);
        assert_eq!(strtoi64("  9").end, 3);
    }

    #[test]
    fn datebsearch_finds_known_tokens() {
        // datetktbl entries.
        assert_eq!(datebsearch("jan", datetktbl).map(|t| t.value), Some(1));
        assert_eq!(datebsearch("dec", datetktbl).map(|t| t.value), Some(12));
        assert_eq!(
            datebsearch("sunday", datetktbl).map(|t| t.r#type),
            Some(DOW)
        );
        assert_eq!(
            datebsearch("+infinity", datetktbl).map(|t| t.value),
            Some(DTK_LATE)
        );
        assert!(datebsearch("notatoken", datetktbl).is_none());
        // deltatktbl entries.
        assert_eq!(
            datebsearch("years", deltatktbl).map(|t| t.value),
            Some(DTK_YEAR)
        );
        assert_eq!(
            datebsearch("mons", deltatktbl).map(|t| t.value),
            Some(DTK_MONTH)
        );
        assert_eq!(datebsearch("ago", deltatktbl).map(|t| t.r#type), Some(AGO));
    }

    #[test]
    fn parse_iso_date() {
        let (field, ftype, nf, rc) = parse("2024-01-15");
        assert_eq!(rc, 0);
        assert_eq!(nf, 1);
        assert_eq!(ftype[0], DTK_DATE);
        assert_eq!(field[0], "2024-01-15");
    }

    #[test]
    fn decode_iso_date() {
        let (mut field, mut ftype, nf, rc) = parse("2024-01-15");
        assert_eq!(rc, 0);
        let mut tm = fresh_tm();
        let mut fsec = 0;
        let mut dtype = 0;
        let mut tz = 0;
        let r = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
        );
        assert_eq!(r, 0, "DecodeDateTime should succeed");
        assert_eq!((tm.tm_year, tm.tm_mon, tm.tm_mday), (2024, 1, 15));
        assert_eq!(dtype, DTK_DATE);
    }

    #[test]
    fn decode_iso_timestamp_with_fraction() {
        let (mut field, mut ftype, nf, rc) = parse("2024-01-15 10:30:45.123");
        assert_eq!(rc, 0);
        let mut tm = fresh_tm();
        let mut fsec = 0;
        let mut dtype = 0;
        let mut tz = 0;
        let r = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
        );
        assert_eq!(r, 0);
        assert_eq!((tm.tm_year, tm.tm_mon, tm.tm_mday), (2024, 1, 15));
        assert_eq!((tm.tm_hour, tm.tm_min, tm.tm_sec), (10, 30, 45));
        assert_eq!(fsec, 123_000);
    }

    #[test]
    fn decode_text_month_date() {
        let (mut field, mut ftype, nf, rc) = parse("Jan 15 2024");
        assert_eq!(rc, 0);
        let mut tm = fresh_tm();
        let mut fsec = 0;
        let mut dtype = 0;
        let mut tz = 0;
        let r = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
        );
        assert_eq!(r, 0);
        assert_eq!((tm.tm_year, tm.tm_mon, tm.tm_mday), (2024, 1, 15));
    }

    #[test]
    fn decode_dmy_slash_date() {
        // "15/01/2024" only parses as 15 Jan 2024 under DMY date order.
        let saved = date_order();
        set_date_order(DATEORDER_DMY);
        let (mut field, mut ftype, nf, rc) = parse("15/01/2024");
        assert_eq!(rc, 0);
        let mut tm = fresh_tm();
        let mut fsec = 0;
        let mut dtype = 0;
        let mut tz = 0;
        let r = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
        );
        set_date_order(saved);
        assert_eq!(r, 0, "DMY slash date should decode");
        assert_eq!((tm.tm_year, tm.tm_mon, tm.tm_mday), (2024, 1, 15));
    }

    #[test]
    fn decode_leap_second_like_time_is_field_overflow() {
        // "23:59:60" -> sec == 60 is allowed individually, but the whole
        // 23:59:60 exceeds 24:00:00? No -- 23:59:60 == 24:00:00 exactly, which
        // time_overflows() rejects only if > USECS_PER_DAY.  Use 23:59:60.5 to
        // exceed a full day and force DTERR_FIELD_OVERFLOW.
        let (mut field, mut ftype, nf, rc) = parse("1999-12-31 23:59:60.5");
        assert_eq!(rc, 0);
        let mut tm = fresh_tm();
        let mut fsec = 0;
        let mut dtype = 0;
        let mut tz = 0;
        let r = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
        );
        assert_eq!(r, DTERR_FIELD_OVERFLOW);
    }

    #[test]
    #[ignore = "needs tzdb via get_share_path (common/path.c) which is not yet ported"]
    fn malformed_string_is_bad_format() {
        crate::test_install_seams();
        let (mut field, mut ftype, nf, rc) = parse("not a date");
        // Tokenization succeeds (three alpha words); decode must reject.
        assert_eq!(rc, 0);
        let mut tm = fresh_tm();
        let mut fsec = 0;
        let mut dtype = 0;
        let mut tz = 0;
        let r = DecodeDateTime(
            &mut field,
            &mut ftype,
            nf,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
        );
        assert_eq!(r, DTERR_BAD_FORMAT);
    }

    #[test]
    fn parse_too_many_fields_is_bad_format() {
        // Force MAXDATEFIELDS overflow.
        let many = "1 ".repeat((MAXDATEFIELDS as usize) + 5);
        let mut field: Vec<String> = Vec::new();
        let mut ftype: Vec<i32> = Vec::new();
        let mut nf = 0;
        let rc = ParseDateTime(
            &many,
            MAXDATELEN as usize + MAXDATEFIELDS as usize,
            &mut field,
            &mut ftype,
            MAXDATEFIELDS as usize,
            &mut nf,
        );
        assert_eq!(rc, DTERR_BAD_FORMAT);
    }

    /// Regression: C tokenizes into a fixed `workbuf`; once the cumulative
    /// (bytes + per-field NUL) usage would overflow it, `APPEND_CHAR` returns
    /// DTERR_BAD_FORMAT.  Without that bound, over-long single tokens that C
    /// rejects were wrongly accepted.  The buffer size is per-caller, so the
    /// cutoff differs between the date.c and timestamp.c callers.  (datetime.c
    /// ParseDateTime)
    #[test]
    fn parse_overlong_workbuf_is_bad_format() {
        // For a single all-digit token a `buflen`-byte input overflows (the
        // `buflen`-th APPEND_CHAR has no room for the char + its terminator),
        // while a `buflen - 1`-byte token still fits.  Check each C call site's
        // size: 129 (date/time/timetz), 153 (timestamp/timestamptz), 256
        // (interval).
        for buflen in [
            MAXDATELEN as usize + 1,                      // date_in/time_in/timetz_in
            MAXDATELEN as usize + MAXDATEFIELDS as usize, // timestamp_in/timestamptz_in
            256,                                          // interval_in
        ] {
            let just_fits = "1".repeat(buflen - 1);
            let (_, _, _, rc) = parse_buflen(&just_fits, buflen);
            assert_eq!(
                rc,
                0,
                "a {}-byte token must still parse with buflen {buflen}",
                buflen - 1
            );

            let overflows = "1".repeat(buflen);
            let (_, _, _, rc) = parse_buflen(&overflows, buflen);
            assert_eq!(
                rc, DTERR_BAD_FORMAT,
                "a {buflen}-byte token must overflow the workbuf"
            );
        }

        // The date.c callers (buflen 129) reject a 129-152-byte token that the
        // timestamp.c callers (buflen 153) still accept -- the over-permissive
        // behavior this fix removes.
        let token = "1".repeat(140);
        let (_, _, _, rc_date) = parse_buflen(&token, MAXDATELEN as usize + 1);
        assert_eq!(rc_date, DTERR_BAD_FORMAT);
        let (_, _, _, rc_ts) = parse_buflen(&token, MAXDATELEN as usize + MAXDATEFIELDS as usize);
        assert_eq!(rc_ts, 0);
    }

    /// Regression: `ParseFractionalSecond` converts to microseconds with C's
    /// `rint()` (round half to EVEN), not half-away-from-zero.  At the exact
    /// 0.5-usec tie the destination digit must round to even.  (datetime.c:736)
    #[test]
    fn parse_fractional_second_rounds_half_to_even() {
        let mut fsec: fsec_t = -1;
        // ".0000005" * 1e6 = 0.5 -> ties to even -> 0 (old .round() gave 1).
        assert_eq!(ParseFractionalSecond(".0000005", &mut fsec), 0);
        assert_eq!(fsec, 0);

        // ".0000025" * 1e6 = 2.5 -> ties to even -> 2 (old .round() gave 3).
        assert_eq!(ParseFractionalSecond(".0000025", &mut fsec), 0);
        assert_eq!(fsec, 2);
    }

    /// Regression: `AdjustFractYears` uses C `rint()` (round half to EVEN).  For
    /// "0.375 years" (frac = 0.375, scale = 1) the month count is
    /// rint(0.375 * 12) = rint(4.5) = 4, not the 5 produced by half-away
    /// rounding.  (datetime.c:618)
    #[test]
    fn adjust_fract_years_rounds_half_to_even() {
        let mut itm = pg_itm_in {
            tm_usec: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        assert!(AdjustFractYears(0.375, 1, &mut itm));
        assert_eq!(itm.tm_mon, 4);
    }

    #[test]
    fn decode_iso8601_interval() {
        let mut itm = pg_itm_in {
            tm_usec: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        let mut dtype = 0;
        let r = DecodeISO8601Interval("P1Y2M3DT4H5M6S", &mut dtype, &mut itm);
        assert_eq!(r, 0);
        assert_eq!(dtype, DTK_DELTA);
        assert_eq!(itm.tm_year, 1);
        assert_eq!(itm.tm_mon, 2);
        assert_eq!(itm.tm_mday, 3);
        let expected_usec = 4 * USECS_PER_HOUR + 5 * USECS_PER_MINUTE + 6 * USECS_PER_SEC;
        assert_eq!(itm.tm_usec, expected_usec);
    }

    #[test]
    fn decode_postgres_interval_year_mons() {
        let (mut field, mut ftype, nf, rc) = parse("1 year 2 mons");
        assert_eq!(rc, 0);
        let mut itm = pg_itm_in {
            tm_usec: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
        };
        let mut dtype = 0;
        let r = DecodeInterval(
            &mut field,
            &mut ftype,
            nf,
            INTERVAL_FULL_RANGE,
            &mut dtype,
            &mut itm,
        );
        assert_eq!(r, 0);
        assert_eq!(itm.tm_year, 1);
        assert_eq!(itm.tm_mon, 2);
    }

    #[test]
    fn decode_timezone_numeric() {
        let mut tz = 0;
        assert_eq!(DecodeTimezone("+05:30", &mut tz), 0);
        // ISO sign convention: +05:30 -> stored as -(offset) seconds.
        assert_eq!(tz, -(5 * 3600 + 30 * 60));
        let mut tz2 = 0;
        assert_eq!(DecodeTimezone("-08", &mut tz2), 0);
        assert_eq!(tz2, 8 * 3600);
        let mut tz3 = 0;
        assert_eq!(DecodeTimezone("+99", &mut tz3), DTERR_TZDISP_OVERFLOW);
    }

    #[test]
    fn decode_units_and_special() {
        let mut v = 0;
        assert_eq!(DecodeUnits(0, "hour", &mut v), UNITS);
        assert_eq!(v, DTK_HOUR);
        assert_eq!(DecodeSpecial(0, "jan", &mut v), MONTH);
        assert_eq!(v, 1);
        assert_eq!(DecodeSpecial(0, "nosuchtoken", &mut v), UNKNOWN_FIELD);
    }

    #[test]
    fn validate_date_rejects_feb_30() {
        let mut tm = fresh_tm();
        tm.tm_year = 2024;
        tm.tm_mon = 2;
        tm.tm_mday = 30;
        let r = ValidateDate(DTK_DATE_M, false, false, false, &mut tm);
        assert_eq!(r, DTERR_FIELD_OVERFLOW);
    }
}
