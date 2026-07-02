//! C-locale pg_strftime (strftime.c).

#[cfg(test)]
mod tests;

use localtime::{PgTm, DAYSPERLYEAR, DAYSPERNYEAR, DAYSPERWEEK, MONSPERYEAR, TM_YEAR_BASE};

const HOURSPERDAY: i32 = 24;
const MINSPERHOUR: i64 = 60;
const SECSPERMIN: i64 = 60;
const DIVISOR: i32 = 100;

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

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
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

// C's pt/ptlim cursor: writes past the end drop; overflow detected via full().
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

/// On success the text plus a trailing NUL is written into `s`; returns the
/// formatted byte count (excluding NUL). `None` is C's `return 0` / ERANGE
/// path: `s` holds truncated bytes without a NUL.
pub fn pg_strftime(s: &mut [u8], format: &[u8], t: &PgTm<'_>) -> Option<usize> {
    let mut warn = Warn::None;
    let mut out = OutBuf { buf: s, pos: 0 };

    fmt(format, t, &mut out, &mut warn);
    // C's `if (!p)` EOVERFLOW branch is unreachable (_fmt never returns NULL).
    if out.full() {
        return None;
    }
    let len = out.pos;
    out.push(0);
    Some(len)
}

fn fmt(format: &[u8], t: &PgTm<'_>, out: &mut OutBuf<'_>, warnp: &mut Warn) {
    let mut i = 0;

    while i < format.len() {
        if format[i] == b'%' {
            i += 1;
            // C's `label:`, re-entered by the %E/%O locale modifiers.
            'label: loop {
                if i >= format.len() {
                    // case '\0': the preceding byte emits itself, as in C.
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
                    b'C' => yconv(t.tm_year, TM_YEAR_BASE, true, false, out),
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
                        i += 1;
                        continue 'label;
                    }
                    b'e' => conv(t.tm_mday, IntFmt::Space2, out),
                    b'F' => fmt(b"%Y-%m-%d", t, out, warnp),
                    b'H' => conv(t.tm_hour, IntFmt::Zero2, out),
                    b'I' => conv(hour12(t.tm_hour), IntFmt::Zero2, out),
                    b'j' => conv(t.tm_yday + 1, IntFmt::Zero3, out),
                    // %k / %l swapped, matching SunOS 4.1.1 (see C comments).
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
                    b'u' => conv(
                        if t.tm_wday == 0 { DAYSPERWEEK } else { t.tm_wday },
                        IntFmt::Plain,
                        out,
                    ),
                    // ISO week 01 is the first week containing a Thursday.
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
                            let bot = (yday + 11 - wday) % DAYSPERWEEK - 3;
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
                    // %Z is empty when the abbreviation is unknown (C99).
                    b'Z' => {
                        if let Some(zone) = t.tm_zone {
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
                                if let Some(zone) = t.tm_zone {
                                    negative = zone.as_bytes().first() == Some(&b'-');
                                }
                            }
                            if negative {
                                add(b"-", out);
                                diff = -diff;
                            } else {
                                add(b"+", out);
                            }
                            diff /= SECSPERMIN;
                            diff = diff / MINSPERHOUR * 100 + diff % MINSPERHOUR;
                            conv(diff as i32, IntFmt::Zero4, out);
                        }
                    }
                    b'+' => fmt(DATE_FMT, t, out, warnp),
                    // '%' and undefined chars print themselves (printf(3)).
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
            if out.full() {
                return;
            }
            out.push(format[i]);
            i += 1;
        }
    }
}

fn add(bytes: &[u8], out: &mut OutBuf<'_>) {
    for &b in bytes {
        out.push(b);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum IntFmt {
    Zero2,
    Zero3,
    Zero4,
    Space2,
    Plain,
}

fn conv(n: i32, format: IntFmt, out: &mut OutBuf<'_>) {
    let mut scratch = [0u8; 16];
    let mut digits = [0u8; 12];
    let neg = n < 0;
    let mut v = (n as i64).unsigned_abs();
    let mut nd = 0usize;
    loop {
        digits[nd] = b'0' + (v % 10) as u8;
        nd += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let width: usize = match format {
        IntFmt::Zero2 | IntFmt::Space2 => 2,
        IntFmt::Zero3 => 3,
        IntFmt::Zero4 => 4,
        IntFmt::Plain => 0,
    };
    let body = nd + neg as usize;
    let pad = width.saturating_sub(body);
    let mut len = 0usize;
    if matches!(format, IntFmt::Space2) {
        for _ in 0..pad {
            scratch[len] = b' ';
            len += 1;
        }
        if neg {
            scratch[len] = b'-';
            len += 1;
        }
    } else {
        if neg {
            scratch[len] = b'-';
            len += 1;
        }
        for _ in 0..pad {
            scratch[len] = b'0';
            len += 1;
        }
    }
    for d in (0..nd).rev() {
        scratch[len] = digits[d];
        len += 1;
    }
    add(&scratch[..len], out);
}

// %C concatenated with %y equals %Y; %Y is at least 4 bytes.
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

fn hour12(hour: i32) -> i32 {
    if hour % 12 != 0 {
        hour % 12
    } else {
        12
    }
}

fn month_name<'a>(month: i32, names: &'a [&'a [u8]; 12]) -> &'a [u8] {
    if (0..MONSPERYEAR as i32).contains(&month) {
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

// private.h isleap_sum: isleap(a % 400 + b % 400), avoiding overflow.
fn isleap_sum(a: i32, b: i32) -> bool {
    let sum = a % 400 + b % 400;
    sum % 4 == 0 && (sum % 100 != 0 || sum % 400 == 0)
}
