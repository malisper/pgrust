//! Port of PostgreSQL's `src/timezone/strftime.c`.
//!
//! Formats a broken-down timestamp ([`pg_tm`]) into a caller-owned buffer
//! using PostgreSQL's C-locale `pg_strftime` rules (conversion specifiers,
//! the `C_time_locale` tables, ISO-week math, `%z`/`%Z` handling, and the
//! truncate-then-detect-overflow buffer semantics).

use std::ffi::CStr;

use ::pgtime::{
    pg_tm, DAYSPERLYEAR, DAYSPERNYEAR, DAYSPERWEEK, HOURSPERDAY, MINSPERHOUR, MONSPERYEAR,
    SECSPERMIN, TM_YEAR_BASE,
};

const DIVISOR: i32 = 100;

// The C-locale `lc_time_T` tables (`C_time_locale` in strftime.c).
const MON: [&[u8]; 12] = [
    b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov", b"Dec",
];
const MONTH: [&[u8]; 12] = [
    b"January",
    b"February",
    b"March",
    b"April",
    b"May",
    b"June",
    b"July",
    b"August",
    b"September",
    b"October",
    b"November",
    b"December",
];
const WDAY: [&[u8]; 7] = [b"Sun", b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat"];
const WEEKDAY: [&[u8]; 7] = [
    b"Sunday",
    b"Monday",
    b"Tuesday",
    b"Wednesday",
    b"Thursday",
    b"Friday",
    b"Saturday",
];
const X_FMT: &[u8] = b"%H:%M:%S";
const X_FMT_LOWER: &[u8] = b"%m/%d/%y";
const C_FMT: &[u8] = b"%a %b %e %T %Y";
const AM: &[u8] = b"AM";
const PM: &[u8] = b"PM";
const DATE_FMT: &[u8] = b"%a %b %e %H:%M:%S %Z %Y";

/// `enum warn { IN_NONE, IN_SOME, IN_THIS, IN_ALL }` from strftime.c.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Warn {
    None,
    Some,
    This,
    All,
}

impl Warn {
    fn raise(&mut self, other: Self) {
        if other > *self {
            *self = other;
        }
    }
}

/// Truncating cursor over the caller's buffer: the `pt`/`ptlim` pair of the C
/// code. Writes past the end are dropped (as `_add` drops them); the caller
/// detects overflow afterwards via `full()` (`p == s + maxsize`).
struct OutBuf<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl OutBuf<'_> {
    fn push(&mut self, b: u8) {
        if self.pos < self.buf.len() {
            self.buf[self.pos] = b;
            self.pos += 1;
        }
    }

    fn full(&self) -> bool {
        self.pos == self.buf.len()
    }
}

/// Converts timestamp `t` to a string in `s`, a caller-allocated buffer,
/// using the given format pattern. Mirrors C `pg_strftime(s, maxsize,
/// format, t)`.
///
/// On success the formatted text plus a trailing NUL is written into `s` and
/// the number of formatted bytes (excluding the NUL) is returned. If the
/// output (plus NUL) does not fit, `s` holds the truncated bytes without a
/// NUL terminator and `None` is returned (the C `return 0` / `ERANGE` path).
pub fn pg_strftime(s: &mut [u8], format: &CStr, t: &pg_tm) -> Option<usize> {
    let mut warn = Warn::None;
    let mut out = OutBuf { buf: s, pos: 0 };

    fmt(format.to_bytes(), t, &mut out, &mut warn);
    // The C `if (!p)` EOVERFLOW branch is unreachable (`_fmt` never returns
    // NULL); only the `p == s + maxsize` ERANGE check remains.
    if out.full() {
        return None;
    }
    let len = out.pos;
    out.push(0);
    Some(len)
}

/// `_fmt` from strftime.c.
fn fmt(format: &[u8], t: &pg_tm, out: &mut OutBuf<'_>, warnp: &mut Warn) {
    let mut i = 0;

    while i < format.len() {
        if format[i] == b'%' {
            i += 1;
            // `label:` — re-entered by the %E/%O locale modifiers.
            'label: loop {
                if i >= format.len() {
                    // case '\0': --format; break;  => the literal write at the
                    // bottom of the loop emits the preceding byte itself
                    // ('%' for a trailing "%", the modifier for "%E"/"%O").
                    if out.full() {
                        return;
                    }
                    out.push(format[i - 1]);
                    return;
                }
                match format[i] {
                    b'A' => add(weekday_name(t.tm_wday, &WEEKDAY), out),
                    b'a' => add(weekday_name(t.tm_wday, &WDAY), out),
                    b'B' => add(month_name(t.tm_mon, &MONTH), out),
                    b'b' | b'h' => add(month_name(t.tm_mon, &MON), out),
                    b'C' => {
                        // %C used to do `_fmt("%a %b %e %X %Y", t)`; POSIX
                        // 1003.2 now calls for the century only.
                        yconv(t.tm_year, TM_YEAR_BASE, true, false, out);
                    }
                    b'c' => {
                        let mut warn2 = Warn::Some;
                        fmt(C_FMT, t, out, &mut warn2);
                        if warn2 == Warn::All {
                            warn2 = Warn::This;
                        }
                        warnp.raise(warn2);
                    }
                    b'D' => fmt(b"%m/%d/%y", t, out, warnp),
                    b'd' => conv(t.tm_mday, IntFmt::Zero2, out),
                    b'E' | b'O' => {
                        // Locale modifiers of C99 and later (%Ec %EC %Ex ...):
                        // ignore the modifier and reprocess the next char.
                        i += 1;
                        continue 'label;
                    }
                    b'e' => conv(t.tm_mday, IntFmt::Space2, out),
                    b'F' => fmt(b"%Y-%m-%d", t, out, warnp),
                    b'H' => conv(t.tm_hour, IntFmt::Zero2, out),
                    b'I' => conv(hour12(t.tm_hour), IntFmt::Zero2, out),
                    b'j' => conv(t.tm_yday + 1, IntFmt::Zero3, out),
                    // "%k" and "%l" are swapped relative to the obvious
                    // reading, matching SunOS 4.1.1 / Arnold Robbins'
                    // strftime 3.0 (see the C comments).
                    b'k' => conv(t.tm_hour, IntFmt::Space2, out),
                    b'l' => conv(hour12(t.tm_hour), IntFmt::Space2, out),
                    b'M' => conv(t.tm_min, IntFmt::Zero2, out),
                    b'm' => conv(t.tm_mon + 1, IntFmt::Zero2, out),
                    b'n' => add(b"\n", out),
                    b'p' => add(if t.tm_hour >= HOURSPERDAY / 2 { PM } else { AM }, out),
                    b'R' => fmt(b"%H:%M", t, out, warnp),
                    b'r' => fmt(b"%I:%M:%S %p", t, out, warnp),
                    b'S' => conv(t.tm_sec, IntFmt::Zero2, out),
                    b'T' => fmt(b"%H:%M:%S", t, out, warnp),
                    b't' => add(b"\t", out),
                    b'U' => conv(
                        (t.tm_yday + DAYSPERWEEK - t.tm_wday) / DAYSPERWEEK,
                        IntFmt::Zero2,
                        out,
                    ),
                    // ISO 8601: weekday as a decimal number [1 (Monday) - 7].
                    b'u' => conv(
                        if t.tm_wday == 0 { DAYSPERWEEK } else { t.tm_wday },
                        IntFmt::Plain,
                        out,
                    ),
                    // %V: ISO 8601 week number; %G/%g: ISO 8601 year (four /
                    // two digits). Week 01 is the first week containing a
                    // Thursday of the year (equivalently, January 4th).
                    spec @ (b'V' | b'G' | b'g') => {
                        let year = t.tm_year;
                        let mut base = TM_YEAR_BASE;
                        let mut yday = t.tm_yday;
                        let wday = t.tm_wday;
                        let w;
                        loop {
                            let len = if isleap_sum(year, base) {
                                DAYSPERLYEAR
                            } else {
                                DAYSPERNYEAR
                            };
                            // What yday (-3 ... 3) does the ISO year begin on?
                            let bot = (yday + 11 - wday) % DAYSPERWEEK - 3;
                            // What yday does the NEXT ISO year begin on?
                            let mut top = bot - len % DAYSPERWEEK;
                            if top < -3 {
                                top += DAYSPERWEEK;
                            }
                            top += len;
                            if yday >= top {
                                base += 1;
                                w = 1;
                                break;
                            }
                            if yday >= bot {
                                w = 1 + (yday - bot) / DAYSPERWEEK;
                                break;
                            }
                            base -= 1;
                            yday += if isleap_sum(year, base) {
                                DAYSPERLYEAR
                            } else {
                                DAYSPERNYEAR
                            };
                        }
                        if spec == b'V' {
                            conv(w, IntFmt::Zero2, out);
                        } else if spec == b'g' {
                            *warnp = Warn::All;
                            yconv(year, base, false, true, out);
                        } else {
                            yconv(year, base, true, true, out);
                        }
                    }
                    b'v' => fmt(b"%e-%b-%Y", t, out, warnp),
                    b'W' => conv(
                        (t.tm_yday + DAYSPERWEEK
                            - if t.tm_wday != 0 {
                                t.tm_wday - 1
                            } else {
                                DAYSPERWEEK - 1
                            })
                            / DAYSPERWEEK,
                        IntFmt::Zero2,
                        out,
                    ),
                    b'w' => conv(t.tm_wday, IntFmt::Plain, out),
                    b'X' => fmt(X_FMT, t, out, warnp),
                    b'x' => {
                        let mut warn2 = Warn::Some;
                        fmt(X_FMT_LOWER, t, out, &mut warn2);
                        if warn2 == Warn::All {
                            warn2 = Warn::This;
                        }
                        warnp.raise(warn2);
                    }
                    b'y' => {
                        *warnp = Warn::All;
                        yconv(t.tm_year, TM_YEAR_BASE, false, true, out);
                    }
                    b'Y' => yconv(t.tm_year, TM_YEAR_BASE, true, true, out),
                    b'Z' => {
                        // C99 and later: %Z is the empty string when the zone
                        // abbreviation is not determinable.
                        if let Some(zone) = &t.tm_zone {
                            add(zone.as_bytes(), out);
                        }
                    }
                    b'z' => {
                        if t.tm_isdst >= 0 {
                            let mut diff = t.tm_gmtoff;
                            let mut negative = diff < 0;
                            if diff == 0 {
                                // A zero offset takes its sign from the zone
                                // abbreviation's leading byte ("-00" -> "-0000").
                                if let Some(zone) = &t.tm_zone {
                                    negative = zone.as_bytes().first() == Some(&b'-');
                                }
                            }
                            if negative {
                                add(b"-", out);
                                diff = -diff;
                            } else {
                                add(b"+", out);
                            }
                            diff /= SECSPERMIN as i64;
                            diff = diff / MINSPERHOUR as i64 * 100 + diff % MINSPERHOUR as i64;
                            conv(diff as i32, IntFmt::Zero4, out);
                        }
                    }
                    b'+' => fmt(DATE_FMT, t, out, warnp),
                    // case '%' / default: X311J/88-090 (4.12.3.5) leaves an
                    // undefined conversion char undefined; print the char
                    // itself, as printf(3) does — via the literal write below.
                    other => {
                        if out.full() {
                            return;
                        }
                        out.push(other);
                    }
                }
                break;
            }
            i += 1;
        } else {
            // The literal write at the bottom of the C loop:
            // `if (pt == ptlim) break; *pt++ = *format;`
            if out.full() {
                return;
            }
            out.push(format[i]);
            i += 1;
        }
    }
}

/// `_add` from strftime.c: append bytes, silently truncating at the limit.
fn add(bytes: &[u8], out: &mut OutBuf<'_>) {
    for &b in bytes {
        out.push(b);
    }
}

/// The `sprintf` format strings `_conv` is called with.
#[derive(Clone, Copy, Eq, PartialEq)]
enum IntFmt {
    /// `"%02d"`
    Zero2,
    /// `"%03d"`
    Zero3,
    /// `"%04d"`
    Zero4,
    /// `"%2d"`
    Space2,
    /// `"%d"`
    Plain,
}

/// `_conv` from strftime.c: render `n` per `format` into a stack scratch
/// buffer (the C `char buf[INT_STRLEN_MAXIMUM(int) + 1]`) and `_add` it.
fn conv(n: i32, format: IntFmt, out: &mut OutBuf<'_>) {
    use std::io::Write;

    // i32 needs at most 11 bytes ("-2147483648"); padding widths are <= 4.
    let mut scratch = [0u8; 16];
    let mut cursor = &mut scratch[..];
    match format {
        IntFmt::Zero2 => write!(cursor, "{n:02}"),
        IntFmt::Zero3 => write!(cursor, "{n:03}"),
        IntFmt::Zero4 => write!(cursor, "{n:04}"),
        IntFmt::Space2 => write!(cursor, "{n:2}"),
        IntFmt::Plain => write!(cursor, "{n}"),
    }
    .expect("integer rendering fits the stack scratch");
    let written = 16 - cursor.len();
    add(&scratch[..written], out);
}

/// `_yconv` from strftime.c.
///
/// POSIX and the C Standard are unclear about %C and %y for negative or
/// >9999 years. Convention: %C concatenated with %y yields the same output
/// as %Y, and %Y contains at least 4 bytes, with more only if necessary.
fn yconv(a: i32, b: i32, convert_top: bool, convert_yy: bool, out: &mut OutBuf<'_>) {
    let mut trail = a % DIVISOR + b % DIVISOR;
    let mut lead = a / DIVISOR + b / DIVISOR + trail / DIVISOR;
    trail %= DIVISOR;

    if trail < 0 && lead > 0 {
        trail += DIVISOR;
        lead -= 1;
    } else if lead < 0 && trail > 0 {
        trail -= DIVISOR;
        lead += 1;
    }

    if convert_top {
        if lead == 0 && trail < 0 {
            add(b"-0", out);
        } else {
            conv(lead, IntFmt::Zero2, out);
        }
    }
    if convert_yy {
        conv(if trail < 0 { -trail } else { trail }, IntFmt::Zero2, out);
    }
}

/// `(t->tm_hour % 12) ? (t->tm_hour % 12) : 12` from %I / %l.
fn hour12(hour: i32) -> i32 {
    if hour % 12 != 0 {
        hour % 12
    } else {
        12
    }
}

fn month_name<'a>(month: i32, names: &'a [&'a [u8]; 12]) -> &'a [u8] {
    if (0..MONSPERYEAR).contains(&month) {
        names[month as usize]
    } else {
        b"?"
    }
}

fn weekday_name<'a>(wday: i32, names: &'a [&'a [u8]; 7]) -> &'a [u8] {
    if (0..DAYSPERWEEK).contains(&wday) {
        names[wday as usize]
    } else {
        b"?"
    }
}

/// `isleap_sum(a, b)` from `private.h`: true if `a + b` is a leap year,
/// assuming the sum stays in range — computed as `isleap(a % 400 + b % 400)`
/// to avoid integer overflow.
fn isleap_sum(a: i32, b: i32) -> bool {
    let sum = a % 400 + b % 400;
    sum % 4 == 0 && (sum % 100 != 0 || sum % 400 == 0)
}

/// Install the seams owned by this crate.
pub fn init_seams() {
    strftime_seams::pg_strftime::set(
        |buf: &mut [u8], format: &str, t: &::pgtime::pg_tm| {
            // The seam takes &str; the implementation requires &CStr.
            // Build a CString from the format string; if it contains an
            // interior NUL the format is malformed and we return 0 (overflow).
            let Ok(cformat) = std::ffi::CString::new(format) else {
                return 0;
            };
            pg_strftime(buf, &cformat, t).unwrap_or(0)
        },
    );
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;

    fn sample_tm(zone: &str) -> pg_tm {
        pg_tm {
            tm_sec: 7,
            tm_min: 6,
            tm_hour: 15,
            tm_mday: 2,
            tm_mon: 0,
            tm_year: 124,
            tm_wday: 2,
            tm_yday: 1,
            tm_isdst: 0,
            tm_gmtoff: -8 * 60 * 60,
            tm_zone: Some(zone.to_string()),
        }
    }

    fn format(format: &str, t: &pg_tm) -> Vec<u8> {
        let format = CString::new(format).unwrap();
        let mut buf = [0u8; 256];
        let len = pg_strftime(&mut buf, &format, t).expect("output fits");
        assert_eq!(buf[len], 0, "NUL terminated");
        buf[..len].to_vec()
    }

    #[test]
    fn formats_common_postgres_timestamp_parts() {
        let t = sample_tm("PST");

        assert_eq!(
            format("%a %b %e %T %Y %Z %z", &t),
            b"Tue Jan  2 15:06:07 2024 PST -0800"
        );
        assert_eq!(format("%F %R %r", &t), b"2024-01-02 15:06 03:06:07 PM");
    }

    #[test]
    fn supports_c_locale_composites_and_unknown_ranges() {
        let mut t = sample_tm("UTC");
        t.tm_wday = 99;
        t.tm_mon = -1;

        assert_eq!(format("%A %a %B %b", &t), b"? ? ? ?");
        assert_eq!(
            format("%c %x %X", &sample_tm("UTC")),
            b"Tue Jan  2 15:06:07 2024 01/02/24 15:06:07"
        );
    }

    #[test]
    fn formats_iso_week_year_boundaries() {
        let mut t = sample_tm("UTC");
        t.tm_year = 119;
        t.tm_mon = 11;
        t.tm_mday = 30;
        t.tm_wday = 1;
        t.tm_yday = 363;

        assert_eq!(format("%G-W%V-%u %g", &t), b"2020-W01-1 20");
    }

    #[test]
    fn iso_week_late_january_of_iso_year_53() {
        // 2021-01-01 was a Friday; it belongs to ISO week 53 of ISO year 2020
        // (the `yday >= bot` is false / `--base` back-up branch).
        let mut t = sample_tm("UTC");
        t.tm_year = 121;
        t.tm_mon = 0;
        t.tm_mday = 1;
        t.tm_wday = 5;
        t.tm_yday = 0;

        assert_eq!(format("%G-W%V-%u", &t), b"2020-W53-5");
    }

    #[test]
    fn omits_timezone_offset_when_dst_unknown() {
        let mut t = sample_tm("UTC");
        t.tm_isdst = -1;

        assert_eq!(format("[%z]", &t), b"[]");
    }

    #[test]
    fn uses_zone_sign_for_zero_offset() {
        let mut t = sample_tm("-00");
        t.tm_gmtoff = 0;

        assert_eq!(format("%z", &t), b"-0000");
    }

    #[test]
    fn no_zone_emits_nothing_for_z_specifier() {
        let mut t = sample_tm("UTC");
        t.tm_zone = None;

        // %Z is empty when the zone is unknown; %z still formats the offset.
        assert_eq!(format("[%Z][%z]", &t), b"[][-0800]");
    }

    #[test]
    fn handles_literal_and_modifier_edge_cases() {
        let t = sample_tm("UTC");

        assert_eq!(format("%", &t), b"%");
        assert_eq!(format("%Q", &t), b"Q");
        assert_eq!(format("%E", &t), b"E");
        assert_eq!(format("%EY", &t), b"2024");
        assert_eq!(format("%%", &t), b"%");
        assert_eq!(format("100%% %j", &t), b"100% 002");
    }

    #[test]
    fn hour_fields_match_swapped_k_and_l() {
        let mut t = sample_tm("UTC");
        t.tm_hour = 5;

        assert_eq!(format("%H|%I|%k|%l|%p", &t), b"05|05| 5| 5|AM");
        t.tm_hour = 0;
        assert_eq!(format("%H|%I|%k|%l|%p", &t), b"00|12| 0|12|AM");
    }

    #[test]
    fn week_of_year_fields() {
        let t = sample_tm("UTC");
        assert_eq!(format("%U %W %w %u %j %C %y", &t), b"00 01 2 2 002 20 24");
    }

    #[test]
    fn negative_year_yconv_paths() {
        // tm_year for year -5 is -1905; %Y must render "-5" via the
        // lead==0/trail<0 "-0" + abs(trail) path ("%C%y" == "%Y").
        let mut t = sample_tm("UTC");
        t.tm_year = -1905;
        assert_eq!(format("%Y", &t), b"-005");
        assert_eq!(format("%C%y", &t), b"-005");
    }

    #[test]
    fn writes_nul_terminated_buffer_on_success() {
        let t = sample_tm("UTC");
        let fmt = CString::new("%Y").unwrap();
        let mut buf = [0u8; 8];

        let len = pg_strftime(&mut buf, &fmt, &t).unwrap();

        assert_eq!(len, 4);
        assert_eq!(&buf[..len], b"2024");
        assert_eq!(buf[len], 0);
    }

    #[test]
    fn overflow_truncates_without_nul_and_returns_none() {
        let t = sample_tm("UTC");
        let fmt = CString::new("%Y").unwrap();
        let mut buf = [b'x'; 4];

        assert_eq!(pg_strftime(&mut buf, &fmt, &t), None);
        // Matches C: the truncated bytes are left behind, no NUL is written.
        assert_eq!(&buf, b"2024");

        // Exactly-fits-without-NUL is still overflow (p == s + maxsize).
        let mut buf5 = [0u8; 5];
        assert_eq!(pg_strftime(&mut buf5, &fmt, &t).unwrap(), 4);
        let mut buf0 = [0u8; 0];
        assert_eq!(pg_strftime(&mut buf0, &fmt, &t), None);
    }

    #[test]
    fn empty_format_returns_zero_length() {
        let t = sample_tm("UTC");
        let fmt = CString::new("").unwrap();
        let mut buf = [b'x'; 2];

        assert_eq!(pg_strftime(&mut buf, &fmt, &t), Some(0));
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn renders_large_year_field_without_truncation() {
        // tm_year is years since 1900; year 10000 exercises a 3-digit lead.
        let mut t = sample_tm("UTC");
        t.tm_year = 8100;
        assert_eq!(format("%Y", &t), b"10000");
    }

    #[test]
    fn date_fmt_and_misc_specifiers() {
        let t = sample_tm("PST");
        assert_eq!(
            format("%+", &t),
            b"Tue Jan  2 15:06:07 PST 2024".as_slice()
        );
        assert_eq!(format("%v", &t), b" 2-Jan-2024");
        assert_eq!(format("%D", &t), b"01/02/24");
        assert_eq!(format("a%nb%tc", &t), b"a\nb\tc");
    }
}
