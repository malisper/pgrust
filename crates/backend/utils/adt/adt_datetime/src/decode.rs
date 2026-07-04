#![allow(non_snake_case)]

use core::cell::Cell;

use crate::calendar::{date2j, isleap, j2date, DAY_TAB};
use crate::consts::*;
use crate::settings::date_order;
use crate::tables::{DATETKTBL, DELTATKTBL};
use crate::tz::{
    self, session_tz_abbrev_probe, zoneabbrevtbl, DetermineTimeZoneAbbrevOffset,
    DetermineTimeZoneOffset, FetchDynamicTimeZone, PgTz,
};

#[inline]
fn is_space(c: u8) -> bool {
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
    c.is_ascii_punctuation()
}

struct Strto<T> {
    val: T,
    end: usize,
    erange: bool,
}

/// C `strtoi64(str, &cp, 10)`: value, offset of first unparsed byte, ERANGE.
fn strtoi64(s: &[u8]) -> Strto<i64> {
    let mut i = 0usize;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let start = i;
    let mut acc: i64 = 0;
    let mut erange = false;
    while i < s.len() && is_digit(s[i]) {
        let d = (s[i] - b'0') as i64;
        if !erange {
            match acc.checked_mul(10).and_then(|v| {
                if neg { v.checked_sub(d) } else { v.checked_add(d) }
            }) {
                Some(v) => acc = v,
                None => {
                    erange = true;
                    acc = if neg { i64::MIN } else { i64::MAX };
                }
            }
        }
        i += 1;
    }
    if i == start {
        // no digits: C leaves *endptr = str and returns 0
        return Strto { val: 0, end: 0, erange: false };
    }
    Strto { val: acc, end: i, erange }
}

/// C `strtoint(str, &cp, 10)`: i32 with clamp+ERANGE on overflow.
fn strtoint(s: &[u8]) -> Strto<i32> {
    let r = strtoi64(s);
    if r.erange || r.val < i32::MIN as i64 || r.val > i32::MAX as i64 {
        let clamped = if r.val < 0 { i32::MIN } else { i32::MAX };
        Strto { val: clamped, end: r.end, erange: true }
    } else {
        Strto { val: r.val as i32, end: r.end, erange: false }
    }
}

/// C `atoi` on an all-digit prefix (DecodeNumberField segments).
fn atoi(s: &[u8]) -> i32 {
    strtoint(s).val
}

/// C `strncmp(key, token, TOKMAXLEN)` where both are NUL-terminated.
fn tokcmp(key: &[u8], token: &[u8; TOKMAXLEN + 1]) -> i32 {
    for i in 0..TOKMAXLEN {
        let kc = key.get(i).copied().unwrap_or(0);
        let tc = token[i];
        if kc != tc {
            return kc as i32 - tc as i32;
        }
        if kc == 0 {
            return 0;
        }
    }
    0
}

pub fn datebsearch(key: &[u8], table: &'static [DateTkn]) -> Option<&'static DateTkn> {
    if key.is_empty() {
        return None;
    }
    let mut base = 0usize;
    let mut num = table.len();
    while num > 0 {
        let half = (num - 1) >> 1;
        let position = base + half;
        let tp = &table[position];
        let mut result = key[0] as i32 - tp.token[0] as i32;
        if result == 0 {
            result = tokcmp(key, &tp.token);
            if result == 0 {
                return Some(tp);
            }
        }
        if result < 0 {
            num = half;
        } else {
            base = position + 1;
            num -= half + 1;
        }
    }
    None
}

// Per-field-position lookup memos (C statics datecache/deltacache/tzabbrevcache):
// datetime fields in a workload tend to repeat format position-by-position.
thread_local! {
    static DATECACHE: [Cell<Option<&'static DateTkn>>; MAXDATEFIELDS] =
        const { [const { Cell::new(None) }; MAXDATEFIELDS] };
    static DELTACACHE: [Cell<Option<&'static DateTkn>>; MAXDATEFIELDS] =
        const { [const { Cell::new(None) }; MAXDATEFIELDS] };
    static TZABBREVCACHE: [Cell<TzAbbrevCache>; MAXDATEFIELDS] =
        const { [const { Cell::new(TzAbbrevCache::EMPTY) }; MAXDATEFIELDS] };
}

#[derive(Clone, Copy)]
struct TzAbbrevCache {
    abbrev: [u8; TOKMAXLEN + 1],
    ftype: i8,
    offset: i32,
    tz: Option<&'static PgTz>,
}

impl TzAbbrevCache {
    const EMPTY: TzAbbrevCache =
        TzAbbrevCache { abbrev: [0; TOKMAXLEN + 1], ftype: 0, offset: 0, tz: None };
}

pub fn ClearTimeZoneAbbrevCache() {
    TZABBREVCACHE.with(|c| {
        for slot in c {
            slot.set(TzAbbrevCache::EMPTY);
        }
    });
}

fn strlcpy_tok(dst: &mut [u8; TOKMAXLEN + 1], src: &[u8]) {
    let n = src.len().min(TOKMAXLEN);
    dst[..n].copy_from_slice(&src[..n]);
    for b in &mut dst[n..] {
        *b = 0;
    }
}

pub fn DecodeSpecial(field: usize, lowtoken: &[u8], val: &mut i32) -> i32 {
    lookup_cached(&DATECACHE, &DATETKTBL, field, lowtoken, val)
}

pub fn DecodeUnits(field: usize, lowtoken: &[u8], val: &mut i32) -> i32 {
    lookup_cached(&DELTACACHE, &DELTATKTBL, field, lowtoken, val)
}

fn lookup_cached(
    cache: &'static std::thread::LocalKey<[Cell<Option<&'static DateTkn>>; MAXDATEFIELDS]>,
    table: &'static [DateTkn],
    field: usize,
    lowtoken: &[u8],
    val: &mut i32,
) -> i32 {
    cache.with(|c| {
        let mut tp = c[field].get();
        if tp.is_none() || tokcmp(lowtoken, &tp.unwrap().token) != 0 {
            tp = datebsearch(lowtoken, table);
        }
        match tp {
            None => {
                *val = 0;
                UNKNOWN_FIELD
            }
            Some(t) => {
                c[field].set(Some(t));
                *val = t.value;
                t.typ as i32
            }
        }
    })
}

pub fn DecodeTimezoneAbbrev<'a>(
    field: usize,
    lowtoken: &'a [u8],
    ftype: &mut i32,
    offset: &mut i32,
    tz: &mut Option<&'static PgTz>,
    _extra: &mut DateTimeErrorExtra<'a>,
) -> i32 {
    TZABBREVCACHE.with(|cache| {
        let tzc = cache[field].get();
        if tokcmp(lowtoken, &tzc.abbrev) == 0 && tzc.abbrev[0] != 0 {
            *ftype = tzc.ftype as i32;
            *offset = tzc.offset;
            *tz = tzc.tz;
            return 0;
        }

        if let Some((isfixed, off, isdst)) = session_tz_abbrev_probe(lowtoken) {
            *ftype = if isfixed { if isdst != 0 { DTZ } else { TZ } } else { DYNTZ };
            *tz = if isfixed { None } else { tz::session_timezone() };
            *offset = -off;
            let mut ent = TzAbbrevCache::EMPTY;
            strlcpy_tok(&mut ent.abbrev, lowtoken);
            ent.ftype = *ftype as i8;
            ent.offset = *offset;
            ent.tz = *tz;
            cache[field].set(ent);
            return 0;
        }

        let tp = zoneabbrevtbl().and_then(|tbl| datebsearch(lowtoken, tbl.abbrevs));
        match tp {
            None => {
                *ftype = UNKNOWN_FIELD;
                *offset = 0;
                *tz = None;
            }
            Some(tp) => {
                *ftype = tp.typ as i32;
                if tp.typ as i32 == DYNTZ {
                    *offset = 0;
                    *tz = FetchDynamicTimeZone(zoneabbrevtbl().unwrap(), tp, _extra);
                    if tz.is_none() {
                        return DTERR_BAD_ZONE_ABBREV;
                    }
                } else {
                    *offset = tp.value;
                    *tz = None;
                }
                let mut ent = TzAbbrevCache::EMPTY;
                strlcpy_tok(&mut ent.abbrev, lowtoken);
                ent.ftype = *ftype as i8;
                ent.offset = *offset;
                ent.tz = *tz;
                cache[field].set(ent);
            }
        }
        0
    })
}

// Longest-prefix match, so no per-field memo: retries with successively
// shorter tokens would thrash the fixed-slot cache.
pub fn DecodeTimezoneAbbrevPrefix(
    str_: &[u8],
    offset: &mut i32,
    tz: &mut Option<&'static PgTz>,
) -> i32 {
    *offset = 0;
    *tz = None;

    let mut lowtoken = [0u8; TOKMAXLEN + 1];
    let mut len = 0usize;
    while len < TOKMAXLEN {
        match str_.get(len) {
            Some(&c) if c.is_ascii_alphabetic() => lowtoken[len] = c.to_ascii_lowercase(),
            _ => break,
        }
        len += 1;
    }

    while len > 0 {
        lowtoken[len] = 0;
        let tok = &lowtoken[..len];

        if let Some((isfixed, off, _isdst)) = tz::session_tz_abbrev_probe(tok) {
            if isfixed {
                *offset = -off;
            } else {
                *tz = tz::session_timezone();
            }
            return len as i32;
        }

        if let Some(tp) = zoneabbrevtbl().and_then(|tbl| datebsearch(tok, tbl.abbrevs)) {
            if tp.typ as i32 == DYNTZ {
                let mut extra = DateTimeErrorExtra::default();
                if let Some(tzp) = FetchDynamicTimeZone(zoneabbrevtbl().unwrap(), tp, &mut extra) {
                    *tz = Some(tzp);
                    return len as i32;
                }
            } else {
                *offset = tp.value;
                return len as i32;
            }
        }

        len -= 1;
        lowtoken[len] = 0;
    }

    -1
}

pub fn ParseFraction(cp: &[u8], frac: &mut f64) -> i32 {
    debug_assert!(cp.first() == Some(&b'.'));
    if cp.len() == 1 {
        *frac = 0.0;
        return 0;
    }
    if !cp[1..].iter().all(|&c| is_digit(c)) {
        return DTERR_BAD_FORMAT;
    }
    // all-ASCII digits + '.', so from_utf8 cannot fail
    match core::str::from_utf8(cp).ok().and_then(|s| s.parse::<f64>().ok()) {
        Some(v) => {
            *frac = v;
            0
        }
        None => DTERR_BAD_FORMAT,
    }
}

pub fn ParseFractionalSecond(cp: &[u8], fsec: &mut fsec_t) -> i32 {
    let mut frac = 0.0;
    let dterr = ParseFraction(cp, &mut frac);
    if dterr != 0 {
        return dterr;
    }
    *fsec = (frac * 1_000_000.0).round_ties_even() as fsec_t;
    0
}

/// `dt2time` (timestamp.c core; in-unit).
pub fn dt2time(jd: TimeOffset, hour: &mut i32, min: &mut i32, sec: &mut i32, fsec: &mut fsec_t) {
    let mut time = jd;
    *hour = (time / USECS_PER_HOUR) as i32;
    time -= *hour as i64 * USECS_PER_HOUR;
    *min = (time / USECS_PER_MINUTE) as i32;
    time -= *min as i64 * USECS_PER_MINUTE;
    *sec = (time / USECS_PER_SEC) as i32;
    *fsec = (time - *sec as i64 * USECS_PER_SEC) as fsec_t;
}

/// `time_overflows` (date.c core; in-unit).
pub fn time_overflows(hour: i32, min: i32, sec: i32, fsec: fsec_t) -> bool {
    if hour < 0
        || hour > HOURS_PER_DAY
        || min < 0
        || min >= MINS_PER_HOUR
        || sec < 0
        || sec > SECS_PER_MINUTE
        || fsec < 0
        || fsec as i64 > USECS_PER_SEC
    {
        return true;
    }
    (((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec) as i64) * USECS_PER_SEC
        + fsec as i64
        > USECS_PER_DAY
}

/// `float_time_overflows` (date.c core; timestamp.c's make_* need it too).
pub fn float_time_overflows(hour: i32, min: i32, sec: f64) -> bool {
    if hour < 0 || hour > HOURS_PER_DAY || min < 0 || min >= MINS_PER_HOUR {
        return true;
    }
    if sec.is_nan() {
        return true;
    }
    // round before range-checking, as C does with rint()
    let sec = (sec * USECS_PER_SEC as f64).round_ties_even();
    if sec < 0.0 || sec > (SECS_PER_MINUTE as i64 * USECS_PER_SEC) as f64 {
        return true;
    }
    ((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE) as i64 * USECS_PER_SEC + sec as i64
        > USECS_PER_DAY
}

pub fn ParseDateTime<'w>(
    timestr: &[u8],
    workbuf: &'w mut [u8],
    field: &mut [&'w [u8]],
    ftype: &mut [i32],
    maxfields: usize,
    numfields: &mut usize,
) -> i32 {
    let buflen = workbuf.len();
    let mut nf = 0usize;
    let mut cp = 0usize;
    let len = timestr.len();
    let mut bufp = 0usize;
    let mut spans = [(0u32, 0u32); MAXDATEFIELDS];
    let mut ftypes = [0i32; MAXDATEFIELDS];

    macro_rules! append {
        ($ch:expr) => {{
            if bufp + 1 >= buflen {
                return DTERR_BAD_FORMAT;
            }
            workbuf[bufp] = $ch;
            bufp += 1;
        }};
    }
    macro_rules! peek {
        () => {
            if cp < len { timestr[cp] } else { 0 }
        };
    }

    while cp < len {
        if is_space(timestr[cp]) {
            cp += 1;
            continue;
        }

        if nf >= maxfields {
            return DTERR_BAD_FORMAT;
        }
        let start = bufp;

        if is_digit(timestr[cp]) {
            append!(timestr[cp]);
            cp += 1;
            while is_digit(peek!()) {
                append!(timestr[cp]);
                cp += 1;
            }

            if peek!() == b':' {
                ftypes[nf] = DTK_TIME;
                append!(timestr[cp]);
                cp += 1;
                while matches!(peek!(), b'0'..=b'9' | b':' | b'.') {
                    append!(timestr[cp]);
                    cp += 1;
                }
            } else if matches!(peek!(), b'-' | b'/' | b'.') {
                let delim = timestr[cp];
                append!(timestr[cp]);
                cp += 1;
                if is_digit(peek!()) {
                    ftypes[nf] = if delim == b'.' { DTK_NUMBER } else { DTK_DATE };
                    while is_digit(peek!()) {
                        append!(timestr[cp]);
                        cp += 1;
                    }
                    // insist that the delimiters match for a three-field date
                    if peek!() == delim {
                        ftypes[nf] = DTK_DATE;
                        append!(timestr[cp]);
                        cp += 1;
                        while is_digit(peek!()) || peek!() == delim {
                            append!(timestr[cp]);
                            cp += 1;
                        }
                    }
                } else {
                    ftypes[nf] = DTK_DATE;
                    while is_alnum(peek!()) || peek!() == delim {
                        append!(timestr[cp].to_ascii_lowercase());
                        cp += 1;
                    }
                }
            } else {
                ftypes[nf] = DTK_NUMBER;
            }
        } else if timestr[cp] == b'.' {
            append!(timestr[cp]);
            cp += 1;
            while is_digit(peek!()) {
                append!(timestr[cp]);
                cp += 1;
            }
            ftypes[nf] = DTK_NUMBER;
        } else if is_alpha(timestr[cp]) {
            ftypes[nf] = DTK_STRING;
            append!(timestr[cp].to_ascii_lowercase());
            cp += 1;
            while is_alpha(peek!()) {
                append!(timestr[cp].to_ascii_lowercase());
                cp += 1;
            }

            let mut is_date = false;
            if matches!(peek!(), b'-' | b'/' | b'.') {
                is_date = true;
            } else if peek!() == b'+' || is_digit(peek!()) {
                // could be a timezone name unless it's a known keyword
                if datebsearch(&workbuf[start..bufp], &DATETKTBL).is_none() {
                    is_date = true;
                }
            }
            if is_date {
                ftypes[nf] = DTK_DATE;
                loop {
                    append!(timestr[cp].to_ascii_lowercase());
                    cp += 1;
                    if !(matches!(peek!(), b'+' | b'-' | b'/' | b'_' | b'.' | b':')
                        || is_alnum(peek!()))
                        || cp >= len
                    {
                        break;
                    }
                }
            }
        } else if timestr[cp] == b'+' || timestr[cp] == b'-' {
            append!(timestr[cp]);
            cp += 1;
            while cp < len && is_space(timestr[cp]) {
                cp += 1;
            }
            if is_digit(peek!()) {
                // note "DTK_TZ" could also be a signed float or yyyy-mm
                ftypes[nf] = DTK_TZ;
                append!(timestr[cp]);
                cp += 1;
                while matches!(peek!(), b'0'..=b'9' | b':' | b'.' | b'-') {
                    append!(timestr[cp]);
                    cp += 1;
                }
            } else if is_alpha(peek!()) {
                ftypes[nf] = DTK_SPECIAL;
                append!(timestr[cp].to_ascii_lowercase());
                cp += 1;
                while is_alpha(peek!()) {
                    append!(timestr[cp].to_ascii_lowercase());
                    cp += 1;
                }
            } else {
                return DTERR_BAD_FORMAT;
            }
        } else if is_punct(timestr[cp]) {
            cp += 1;
            continue;
        } else {
            return DTERR_BAD_FORMAT;
        }

        // the NUL terminator C forces in consumes one workbuf byte
        bufp += 1;
        spans[nf] = (start as u32, (bufp - 1 - start) as u32);
        nf += 1;
    }

    let frozen: &'w [u8] = workbuf;
    for i in 0..nf {
        let (s, l) = spans[i];
        field[i] = &frozen[s as usize..(s + l) as usize];
        ftype[i] = ftypes[i];
    }
    *numfields = nf;
    0
}

pub fn DecodeDate(
    str_: &[u8],
    mut fmask: i32,
    tmask: &mut i32,
    is2digits: &mut bool,
    tm: &mut pg_tm,
) -> i32 {
    let mut fsec: fsec_t = 0;
    let mut nf = 0usize;
    let mut fields: [Option<&[u8]>; MAXDATEFIELDS] = [None; MAXDATEFIELDS];
    let mut haveTextMonth = false;
    let mut dmask = 0i32;

    *tmask = 0;

    let mut p = 0usize;
    let len = str_.len();
    while p < len && nf < MAXDATEFIELDS {
        while p < len && !is_alnum(str_[p]) {
            p += 1;
        }
        if p >= len {
            return DTERR_BAD_FORMAT; // end of string after separator
        }
        let start = p;
        if is_digit(str_[p]) {
            while p < len && is_digit(str_[p]) {
                p += 1;
            }
        } else if is_alpha(str_[p]) {
            while p < len && is_alpha(str_[p]) {
                p += 1;
            }
        }
        fields[nf] = Some(&str_[start..p]);
        if p < len {
            p += 1; // C overwrites the delimiter with NUL and steps past
        }
        nf += 1;
    }

    // look first for text fields, since that will be unambiguous month
    for i in 0..nf {
        let f = fields[i].unwrap();
        if !f.is_empty() && is_alpha(f[0]) {
            let mut val = 0;
            let type_ = DecodeSpecial(i, f, &mut val);
            if type_ == IGNORE_DTF {
                continue;
            }
            dmask = DTK_M(type_);
            match type_ {
                MONTH => {
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
            fields[i] = None; // mark this field as being completed
        }
    }

    // now pick up remaining numeric fields
    for i in 0..nf {
        let Some(f) = fields[i] else { continue };
        if f.is_empty() {
            return DTERR_BAD_FORMAT;
        }
        let dterr = DecodeNumber(
            f.len(),
            f,
            haveTextMonth,
            fmask,
            &mut dmask,
            tm,
            &mut fsec,
            is2digits,
        );
        if dterr != 0 {
            return dterr;
        }
        if fmask & dmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= dmask;
        *tmask |= dmask;
    }

    if fmask & !(DTK_M(DOY) | DTK_M(TZ)) != DTK_DATE_M {
        return DTERR_BAD_FORMAT;
    }
    0
}

pub fn ValidateDate(fmask: i32, isjulian: bool, is2digits: bool, bc: bool, tm: &mut pg_tm) -> i32 {
    if fmask & DTK_M(YEAR) != 0 {
        if isjulian {
            // tm_year is correct and should not be touched
        } else if bc {
            if tm.tm_year <= 0 {
                return DTERR_FIELD_OVERFLOW;
            }
            // internally, 1 BC is year zero, 2 BC is -1, etc
            tm.tm_year = -(tm.tm_year - 1);
        } else if is2digits {
            if tm.tm_year < 0 {
                return DTERR_FIELD_OVERFLOW;
            }
            if tm.tm_year < 70 {
                tm.tm_year += 2000;
            } else if tm.tm_year < 100 {
                tm.tm_year += 1900;
            }
        } else if tm.tm_year <= 0 {
            return DTERR_FIELD_OVERFLOW;
        }
    }

    if fmask & DTK_M(DOY) != 0 {
        j2date(
            date2j(tm.tm_year, 1, 1) + tm.tm_yday - 1,
            &mut tm.tm_year,
            &mut tm.tm_mon,
            &mut tm.tm_mday,
        );
    }

    if fmask & DTK_M(MONTH) != 0 && (tm.tm_mon < 1 || tm.tm_mon > MONTHS_PER_YEAR) {
        return DTERR_MD_FIELD_OVERFLOW;
    }

    if fmask & DTK_M(DAY) != 0 && (tm.tm_mday < 1 || tm.tm_mday > 31) {
        return DTERR_MD_FIELD_OVERFLOW;
    }

    if fmask & DTK_DATE_M == DTK_DATE_M
        && tm.tm_mday > DAY_TAB[isleap(tm.tm_year) as usize][(tm.tm_mon - 1) as usize]
    {
        return DTERR_FIELD_OVERFLOW;
    }
    0
}

fn DecodeTimeCommon(str_: &[u8], _fmask: i32, range: i32, tmask: &mut i32, itm: &mut pg_itm) -> i32 {
    let mut fsec: fsec_t = 0;

    *tmask = DTK_TIME_M;

    let r = strtoi64(str_);
    if r.erange {
        return DTERR_FIELD_OVERFLOW;
    }
    itm.tm_hour = r.val;
    let mut cp = r.end;
    if str_.get(cp) != Some(&b':') {
        return DTERR_BAD_FORMAT;
    }
    let r = strtoint(&str_[cp + 1..]);
    if r.erange {
        return DTERR_FIELD_OVERFLOW;
    }
    itm.tm_min = r.val;
    cp = cp + 1 + r.end;
    match str_.get(cp) {
        None => {
            itm.tm_sec = 0;
            // a MINUTE TO SECOND interval takes 2 fields as being mm:ss
            if range == INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND) {
                if itm.tm_hour > i32::MAX as i64 || itm.tm_hour < i32::MIN as i64 {
                    return DTERR_FIELD_OVERFLOW;
                }
                itm.tm_sec = itm.tm_min;
                itm.tm_min = itm.tm_hour as i32;
                itm.tm_hour = 0;
            }
        }
        Some(&b'.') => {
            // always assume mm:ss.sss is MINUTE TO SECOND
            let dterr = ParseFractionalSecond(&str_[cp..], &mut fsec);
            if dterr != 0 {
                return dterr;
            }
            if itm.tm_hour > i32::MAX as i64 || itm.tm_hour < i32::MIN as i64 {
                return DTERR_FIELD_OVERFLOW;
            }
            itm.tm_sec = itm.tm_min;
            itm.tm_min = itm.tm_hour as i32;
            itm.tm_hour = 0;
        }
        Some(&b':') => {
            let r = strtoint(&str_[cp + 1..]);
            if r.erange {
                return DTERR_FIELD_OVERFLOW;
            }
            itm.tm_sec = r.val;
            cp = cp + 1 + r.end;
            match str_.get(cp) {
                Some(&b'.') => {
                    let dterr = ParseFractionalSecond(&str_[cp..], &mut fsec);
                    if dterr != 0 {
                        return dterr;
                    }
                }
                None => {}
                Some(_) => return DTERR_BAD_FORMAT,
            }
        }
        Some(_) => return DTERR_BAD_FORMAT,
    }

    // sanity check; caller must check the range of tm_hour
    if itm.tm_hour < 0
        || itm.tm_min < 0
        || itm.tm_min > MINS_PER_HOUR - 1
        || itm.tm_sec < 0
        || itm.tm_sec > SECS_PER_MINUTE
        || fsec < 0
        || fsec as i64 > USECS_PER_SEC
    {
        return DTERR_FIELD_OVERFLOW;
    }

    itm.tm_usec = fsec;
    0
}

pub fn DecodeTime(
    str_: &[u8],
    fmask: i32,
    range: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
) -> i32 {
    let mut itm = pg_itm::default();
    let dterr = DecodeTimeCommon(str_, fmask, range, tmask, &mut itm);
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

pub fn DecodeNumber(
    flen: usize,
    str_: &[u8],
    haveTextMonth: bool,
    fmask: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    is2digits: &mut bool,
) -> i32 {
    *tmask = 0;

    let r = strtoint(str_);
    if r.erange {
        return DTERR_FIELD_OVERFLOW;
    }
    if r.end == 0 {
        return DTERR_BAD_FORMAT;
    }
    let val = r.val;
    let cp = r.end;

    if str_.get(cp) == Some(&b'.') {
        // more than two digits before the decimal: date or run-together time
        if cp > 2 {
            let dterr =
                DecodeNumberField(flen, str_, fmask | DTK_DATE_M, tmask, tm, fsec, is2digits);
            if dterr < 0 {
                return dterr;
            }
            return 0;
        }
        let dterr = ParseFractionalSecond(&str_[cp..], fsec);
        if dterr != 0 {
            return dterr;
        }
    } else if cp != str_.len() {
        return DTERR_BAD_FORMAT;
    }

    // special case for day of year
    if flen == 3 && fmask & DTK_DATE_M == DTK_M(YEAR) && (1..=366).contains(&val) {
        *tmask = DTK_M(DOY) | DTK_M(MONTH) | DTK_M(DAY);
        tm.tm_yday = val;
        // tm_mon and tm_mday can't actually be set yet ...
        return 0;
    }

    match fmask & DTK_DATE_M {
        0 => {
            // must be YYYY-MM-DD (3+ digit year) or the DateOrder-defined order
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
            // must be at second field of YY-MM-DD
            *tmask = DTK_M(MONTH);
            tm.tm_mon = val;
        }
        m if m == DTK_M(MONTH) => {
            if haveTextMonth {
                // first numeric field of a date with a textual month:
                // accept MON-DD-YYYY, DD-MON-YYYY, YYYY-MON-DD, and the
                // two-digit-year variants per DateOrder
                if flen >= 3 || date_order() == DATEORDER_YMD {
                    *tmask = DTK_M(YEAR);
                    tm.tm_year = val;
                } else {
                    *tmask = DTK_M(DAY);
                    tm.tm_mday = val;
                }
            } else {
                // must be at second field of MM-DD-YY
                *tmask = DTK_M(DAY);
                tm.tm_mday = val;
            }
        }
        m if m == DTK_M(YEAR) | DTK_M(MONTH) => {
            if haveTextMonth {
                // need to accept DD-MON-YYYY even in YMD mode
                if flen >= 3 && *is2digits {
                    // guess that first numeric field is day was wrong
                    *tmask = DTK_M(DAY); // YEAR is already set
                    tm.tm_mday = tm.tm_year;
                    tm.tm_year = val;
                    *is2digits = false;
                } else {
                    *tmask = DTK_M(DAY);
                    tm.tm_mday = val;
                }
            } else {
                // must be at third field of YY-MM-DD
                *tmask = DTK_M(DAY);
                tm.tm_mday = val;
            }
        }
        m if m == DTK_M(DAY) => {
            // must be at second field of DD-MM-YY
            *tmask = DTK_M(MONTH);
            tm.tm_mon = val;
        }
        m if m == DTK_M(MONTH) | DTK_M(DAY) => {
            // must be at third field of DD-MM-YY or MM-DD-YY
            *tmask = DTK_M(YEAR);
            tm.tm_year = val;
        }
        m if m == DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY) => {
            // we have all the date, so it must be a time field
            let dterr = DecodeNumberField(flen, str_, fmask, tmask, tm, fsec, is2digits);
            if dterr < 0 {
                return dterr;
            }
            return 0;
        }
        _ => return DTERR_BAD_FORMAT,
    }

    // mark a 1- or 2-digit year field for later adjustment
    if *tmask == DTK_M(YEAR) {
        *is2digits = flen <= 2;
    }
    0
}

pub fn DecodeNumberField(
    len: usize,
    str_: &[u8],
    fmask: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    is2digits: &mut bool,
) -> i32 {
    // reject anything that isn't digits and decimal point(s)
    if !str_.iter().all(|&c| is_digit(c) || c == b'.') {
        return DTERR_BAD_FORMAT;
    }
    debug_assert_eq!(len, str_.len());

    let mut s = str_;
    let mut len = len;
    if let Some(dot) = s.iter().position(|&c| c == b'.') {
        let dterr = ParseFractionalSecond(&s[dot..], fsec);
        if dterr != 0 {
            return dterr;
        }
        // truncate off the fraction for further processing
        s = &s[..dot];
        len = s.len();
    } else if fmask & DTK_DATE_M != DTK_DATE_M {
        // no decimal point and no complete date yet
        if len >= 6 {
            *tmask = DTK_DATE_M;
            // from the end: first 2 are Day, next 2 Month, the rest Year
            tm.tm_mday = atoi(&s[len - 2..]);
            tm.tm_mon = atoi(&s[len - 4..len - 2]);
            tm.tm_year = atoi(&s[..len - 4]);
            if len - 4 == 2 {
                *is2digits = true;
            }
            return DTK_DATE;
        }
    }

    // not all time fields are specified?
    if fmask & DTK_TIME_M != DTK_TIME_M {
        if len == 6 {
            // hhmmss
            *tmask = DTK_TIME_M;
            tm.tm_sec = atoi(&s[4..]);
            tm.tm_min = atoi(&s[2..4]);
            tm.tm_hour = atoi(&s[..2]);
            return DTK_TIME;
        } else if len == 4 {
            // hhmm?
            *tmask = DTK_TIME_M;
            tm.tm_sec = 0;
            tm.tm_min = atoi(&s[2..]);
            tm.tm_hour = atoi(&s[..2]);
            return DTK_TIME;
        }
    }

    DTERR_BAD_FORMAT
}

pub fn DecodeTimezone(str_: &[u8], tzp: &mut i32) -> i32 {
    let mut min = 0i32;
    let mut sec = 0i32;

    if str_.first() != Some(&b'+') && str_.first() != Some(&b'-') {
        return DTERR_BAD_FORMAT;
    }

    let r = strtoint(&str_[1..]);
    if r.erange {
        return DTERR_TZDISP_OVERFLOW;
    }
    let mut hr = r.val;
    let mut cp = 1 + r.end;

    if str_.get(cp) == Some(&b':') {
        let r = strtoint(&str_[cp + 1..]);
        if r.erange {
            return DTERR_TZDISP_OVERFLOW;
        }
        min = r.val;
        cp = cp + 1 + r.end;
        if str_.get(cp) == Some(&b':') {
            let r = strtoint(&str_[cp + 1..]);
            if r.erange {
                return DTERR_TZDISP_OVERFLOW;
            }
            sec = r.val;
            cp = cp + 1 + r.end;
        }
    } else if cp >= str_.len() && str_.len() > 3 {
        // might have run things together (e.g. hhmm)
        min = hr % 100;
        hr /= 100;
        // we could, but don't, support a run-together hhmmss format
    }

    if hr < 0 || hr > MAX_TZDISP_HOUR {
        return DTERR_TZDISP_OVERFLOW;
    }
    if min < 0 || min >= MINS_PER_HOUR {
        return DTERR_TZDISP_OVERFLOW;
    }
    if sec < 0 || sec >= SECS_PER_MINUTE {
        return DTERR_TZDISP_OVERFLOW;
    }

    let mut tz = (hr * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec;
    if str_[0] == b'-' {
        tz = -tz;
    }
    *tzp = -tz;

    if cp < str_.len() {
        return DTERR_BAD_FORMAT;
    }
    0
}

pub fn DecodeDateTime<'a>(
    field: &[&'a [u8]],
    ftype: &[i32],
    nf: usize,
    dtype: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzp: Option<&mut i32>,
    extra: &mut DateTimeErrorExtra<'a>,
) -> i32 {
    let mut fmask = 0i32;
    let mut tmask = 0i32;
    let mut ptype = 0i32; // "prefix type" for ISO and Julian formats
    let mut mer = HR24;
    let mut haveTextMonth = false;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut namedTz: Option<&'static PgTz> = None;
    let mut abbrevTz: Option<&'static PgTz> = None;
    let mut abbrev: Option<&[u8]> = None;

    let have_tz = tzp.is_some();
    let mut tzv = 0i32;

    // insist on at least all of the date fields; initialize the rest
    *dtype = DTK_DATE;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    *fsec = 0;
    tm.tm_isdst = -1; // don't know daylight savings time status apriori

    for i in 0..nf {
        match ftype[i] {
            DTK_DATE => {
                if ptype == DTK_JULIAN {
                    // integral julian day with attached time zone
                    if !have_tz {
                        return DTERR_BAD_FORMAT;
                    }
                    let r = strtoint(field[i]);
                    if r.erange || r.val < 0 {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    j2date(r.val, &mut tm.tm_year, &mut tm.tm_mon, &mut tm.tm_mday);
                    isjulian = true;

                    let dterr = DecodeTimezone(&field[i][r.end..], &mut tzv);
                    if dterr != 0 {
                        return dterr;
                    }
                    tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
                    ptype = 0;
                } else if ptype != 0
                    || fmask & (DTK_M(MONTH) | DTK_M(DAY)) == DTK_M(MONTH) | DTK_M(DAY)
                {
                    // timezone name with embedded punctuation, or a
                    // run-together time with trailing time zone (hhmmss-zz)
                    if !have_tz {
                        return DTERR_BAD_FORMAT;
                    }

                    if field[i].first().is_some_and(|&c| is_digit(c)) || ptype != 0 {
                        if ptype != 0 {
                            // only a preceding "t" field is allowed
                            if ptype != DTK_TIME {
                                return DTERR_BAD_FORMAT;
                            }
                            ptype = 0;
                        }
                        if fmask & DTK_TIME_M == DTK_TIME_M {
                            return DTERR_BAD_FORMAT;
                        }
                        let Some(dash) = field[i].iter().position(|&c| c == b'-') else {
                            return DTERR_BAD_FORMAT;
                        };
                        let dterr = DecodeTimezone(&field[i][dash..], &mut tzv);
                        if dterr != 0 {
                            return dterr;
                        }
                        // read the rest of the field as a concatenated time
                        let head = &field[i][..dash];
                        let dterr = DecodeNumberField(
                            head.len(),
                            head,
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
                        match tz::pg_tzset(field[i]) {
                            Some(z) => namedTz = Some(z),
                            None => {
                                extra.dtee_timezone = Some(field[i]);
                                return DTERR_BAD_TIMEZONE;
                            }
                        }
                        tmask = DTK_M(TZ);
                    }
                } else {
                    let dterr = DecodeDate(field[i], fmask, &mut tmask, &mut is2digits, tm);
                    if dterr != 0 {
                        return dterr;
                    }
                }
            }

            DTK_TIME => {
                // might be an ISO time following a "t" field
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }
                let dterr =
                    DecodeTime(field[i], fmask, INTERVAL_FULL_RANGE, &mut tmask, tm, fsec);
                if dterr != 0 {
                    return dterr;
                }
                if time_overflows(tm.tm_hour, tm.tm_min, tm.tm_sec, *fsec) {
                    return DTERR_FIELD_OVERFLOW;
                }
            }

            DTK_TZ => {
                if !have_tz {
                    return DTERR_BAD_FORMAT;
                }
                let mut tz = 0i32;
                let dterr = DecodeTimezone(field[i], &mut tz);
                if dterr != 0 {
                    return dterr;
                }
                tzv = tz;
                tmask = DTK_M(TZ);
            }

            DTK_NUMBER => {
                if ptype != 0 {
                    // deal with cases where previous field labeled this one
                    let r = strtoint(field[i]);
                    if r.erange {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    let value = r.val;
                    let cp = r.end;
                    if cp < field[i].len() && field[i][cp] != b'.' {
                        return DTERR_BAD_FORMAT;
                    }

                    match ptype {
                        DTK_JULIAN => {
                            if value < 0 {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            tmask = DTK_DATE_M;
                            j2date(value, &mut tm.tm_year, &mut tm.tm_mon, &mut tm.tm_mday);
                            isjulian = true;

                            if field[i].get(cp) == Some(&b'.') {
                                // fractional Julian Day
                                let mut time = 0f64;
                                let dterr = ParseFraction(&field[i][cp..], &mut time);
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
                        DTK_TIME => {
                            // previous field was "t" for ISO time
                            let dterr = DecodeNumberField(
                                field[i].len(),
                                field[i],
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
                    let flen = field[i].len();
                    let dot = field[i].iter().position(|&c| c == b'.');

                    if dot.is_some() && fmask & DTK_DATE_M == 0 {
                        // embedded decimal and no date yet
                        let dterr = DecodeDate(field[i], fmask, &mut tmask, &mut is2digits, tm);
                        if dterr != 0 {
                            return dterr;
                        }
                    } else if dot.is_some_and(|d| flen - (flen - d) > 2) {
                        // embedded decimal with several digits before it:
                        // concatenated date or time (20011223 or 040506)
                        let dterr = DecodeNumberField(
                            flen,
                            field[i],
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                    } else if flen >= 6
                        && (fmask & DTK_DATE_M == 0 || fmask & DTK_TIME_M == 0)
                    {
                        // YMD/HMS concatenation (6+ digits), or a long year
                        let dterr = DecodeNumberField(
                            flen,
                            field[i],
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                    } else {
                        let dterr = DecodeNumber(
                            flen,
                            field[i],
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

            DTK_STRING | DTK_SPECIAL => {
                // timezone abbrevs take precedence over built-in tokens
                let mut type_ = 0;
                let mut val = 0;
                let mut valtz: Option<&'static PgTz> = None;
                let dterr =
                    DecodeTimezoneAbbrev(i, field[i], &mut type_, &mut val, &mut valtz, extra);
                if dterr != 0 {
                    return dterr;
                }
                if type_ == UNKNOWN_FIELD {
                    type_ = DecodeSpecial(i, field[i], &mut val);
                }
                if type_ == IGNORE_DTF {
                    continue;
                }

                tmask = DTK_M(type_);
                match type_ {
                    RESERV => match val {
                        DTK_NOW => {
                            tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
                            *dtype = DTK_DATE;
                            tz::GetCurrentTimeUsec(
                                tm,
                                fsec,
                                if have_tz { Some(&mut tzv) } else { None },
                            );
                        }
                        DTK_YESTERDAY | DTK_TODAY | DTK_TOMORROW => {
                            tmask = DTK_DATE_M;
                            *dtype = DTK_DATE;
                            let mut cur_tm = pg_tm::default();
                            tz::GetCurrentDateTime(&mut cur_tm);
                            let delta = match val {
                                DTK_YESTERDAY => -1,
                                DTK_TODAY => 0,
                                _ => 1,
                            };
                            if delta == 0 {
                                tm.tm_year = cur_tm.tm_year;
                                tm.tm_mon = cur_tm.tm_mon;
                                tm.tm_mday = cur_tm.tm_mday;
                            } else {
                                j2date(
                                    date2j(cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday) + delta,
                                    &mut tm.tm_year,
                                    &mut tm.tm_mon,
                                    &mut tm.tm_mday,
                                );
                            }
                        }
                        DTK_ZULU => {
                            tmask = DTK_TIME_M | DTK_M(TZ);
                            *dtype = DTK_DATE;
                            tm.tm_hour = 0;
                            tm.tm_min = 0;
                            tm.tm_sec = 0;
                            if have_tz {
                                tzv = 0;
                            }
                        }
                        DTK_EPOCH | DTK_LATE | DTK_EARLY => {
                            tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
                            *dtype = val;
                            // caller ignores tm for these dtype codes
                        }
                        _ => panic!("unrecognized RESERV datetime token: {val}"),
                    },

                    MONTH => {
                        // already have a (numeric) month? then try to substitute
                        if fmask & DTK_M(MONTH) != 0
                            && !haveTextMonth
                            && fmask & DTK_M(DAY) == 0
                            && (1..=31).contains(&tm.tm_mon)
                        {
                            tm.tm_mday = tm.tm_mon;
                            tmask = DTK_M(DAY);
                        }
                        haveTextMonth = true;
                        tm.tm_mon = val;
                    }

                    DTZMOD => {
                        // daylight savings time modifier ("MET DST" syntax)
                        tmask |= DTK_M(DTZ);
                        tm.tm_isdst = 1;
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tzv -= val;
                    }

                    DTZ => {
                        tmask |= DTK_M(TZ);
                        tm.tm_isdst = 1;
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tzv = -val;
                    }

                    TZ => {
                        tm.tm_isdst = 0;
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tzv = -val;
                    }

                    DYNTZ => {
                        tmask |= DTK_M(TZ);
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        // determine the actual offset later
                        abbrevTz = valtz;
                        abbrev = Some(field[i]);
                    }

                    AMPM => mer = val,

                    ADBC => bc = val == BC,

                    DOW => tm.tm_wday = val,

                    UNITS => {
                        tmask = 0;
                        // reject consecutive unhandled units
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }

                    ISOTIME => {
                        // filler "t": next field is time
                        tmask = 0;
                        if fmask & DTK_DATE_M != DTK_DATE_M {
                            return DTERR_BAD_FORMAT;
                        }
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }

                    UNKNOWN_FIELD => {
                        // could be an all-alpha timezone name
                        match tz::pg_tzset(field[i]) {
                            Some(z) => namedTz = Some(z),
                            None => return DTERR_BAD_FORMAT,
                        }
                        tmask = DTK_M(TZ);
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
    }

    // reject if prefix type appeared and was never handled
    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    // additional checking for normal date specs (not "infinity" etc)
    if *dtype == DTK_DATE {
        let dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
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

        if fmask & DTK_DATE_M != DTK_DATE_M {
            if fmask & DTK_TIME_M == DTK_TIME_M {
                if let Some(p) = tzp {
                    *p = tzv;
                }
                return 1;
            }
            return DTERR_BAD_FORMAT;
        }

        // a full timezone spec needs the date to resolve DST status
        if let Some(z) = namedTz {
            if fmask & DTK_M(DTZMOD) != 0 {
                return DTERR_BAD_FORMAT;
            }
            tzv = DetermineTimeZoneOffset(tm, z);
        }

        if let Some(z) = abbrevTz {
            if fmask & DTK_M(DTZMOD) != 0 {
                return DTERR_BAD_FORMAT;
            }
            tzv = DetermineTimeZoneAbbrevOffset(tm, abbrev.unwrap(), z);
        }

        // timezone not specified? then use session timezone
        if have_tz && fmask & DTK_M(TZ) == 0 {
            if fmask & DTK_M(DTZMOD) != 0 {
                return DTERR_BAD_FORMAT;
            }
            let Some(z) = tz::session_timezone() else {
                panic!("session timezone not initialized (pg_timezone_initialize) — DecodeDateTime");
            };
            tzv = DetermineTimeZoneOffset(tm, z);
        }
    }

    if let Some(p) = tzp {
        *p = tzv;
    }
    0
}

pub fn DecodeTimeOnly<'a>(
    field: &[&'a [u8]],
    ftype: &mut [i32],
    nf: usize,
    dtype: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzp: Option<&mut i32>,
    extra: &mut DateTimeErrorExtra<'a>,
) -> i32 {
    let mut fmask = 0i32;
    let mut tmask = 0i32;
    let mut ptype = 0i32;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut mer = HR24;
    let mut namedTz: Option<&'static PgTz> = None;
    let mut abbrevTz: Option<&'static PgTz> = None;
    let mut abbrev: Option<&[u8]> = None;

    let have_tz = tzp.is_some();
    let mut tzv = 0i32;

    *dtype = DTK_TIME;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    *fsec = 0;
    tm.tm_isdst = -1;

    for i in 0..nf {
        match ftype[i] {
            DTK_DATE => {
                // time zone not allowed? then no dates or zones at all
                if !have_tz {
                    return DTERR_BAD_FORMAT;
                }

                // under limited circumstances, we will accept a date...
                if i == 0 && nf >= 2 && (ftype[nf - 1] == DTK_DATE || ftype[1] == DTK_TIME) {
                    let dterr = DecodeDate(field[i], fmask, &mut tmask, &mut is2digits, tm);
                    if dterr != 0 {
                        return dterr;
                    }
                } else if field[i].first().is_some_and(|&c| is_digit(c)) {
                    if fmask & DTK_TIME_M == DTK_TIME_M {
                        return DTERR_BAD_FORMAT;
                    }
                    let Some(dash) = field[i].iter().position(|&c| c == b'-') else {
                        return DTERR_BAD_FORMAT;
                    };
                    let dterr = DecodeTimezone(&field[i][dash..], &mut tzv);
                    if dterr != 0 {
                        return dterr;
                    }
                    let head = &field[i][..dash];
                    let dterr = DecodeNumberField(
                        head.len(),
                        head,
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
                    match tz::pg_tzset(field[i]) {
                        Some(z) => namedTz = Some(z),
                        None => {
                            extra.dtee_timezone = Some(field[i]);
                            return DTERR_BAD_TIMEZONE;
                        }
                    }
                    ftype[i] = DTK_TZ;
                    tmask = DTK_M(TZ);
                }
            }

            DTK_TIME => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }
                let dterr = DecodeTime(
                    field[i],
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

            DTK_TZ => {
                if !have_tz {
                    return DTERR_BAD_FORMAT;
                }
                let mut tz = 0i32;
                let dterr = DecodeTimezone(field[i], &mut tz);
                if dterr != 0 {
                    return dterr;
                }
                tzv = tz;
                tmask = DTK_M(TZ);
            }

            DTK_NUMBER => {
                if ptype != 0 {
                    let r = strtoint(field[i]);
                    if r.erange {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    let value = r.val;
                    let cp = r.end;
                    if cp < field[i].len() && field[i][cp] != b'.' {
                        return DTERR_BAD_FORMAT;
                    }

                    match ptype {
                        DTK_JULIAN => {
                            if !have_tz {
                                return DTERR_BAD_FORMAT;
                            }
                            if value < 0 {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            tmask = DTK_DATE_M;
                            j2date(value, &mut tm.tm_year, &mut tm.tm_mon, &mut tm.tm_mday);
                            isjulian = true;

                            if field[i].get(cp) == Some(&b'.') {
                                let mut time = 0f64;
                                let dterr = ParseFraction(&field[i][cp..], &mut time);
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
                        DTK_TIME => {
                            let dterr = DecodeNumberField(
                                field[i].len(),
                                field[i],
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
                    let flen = field[i].len();
                    let dot = field[i].iter().position(|&c| c == b'.');

                    if let Some(d) = dot {
                        // embedded decimal
                        if i == 0 && nf >= 2 && ftype[nf - 1] == DTK_DATE {
                            let dterr =
                                DecodeDate(field[i], fmask, &mut tmask, &mut is2digits, tm);
                            if dterr != 0 {
                                return dterr;
                            }
                        } else if flen - (flen - d) > 2 {
                            let dterr = DecodeNumberField(
                                flen,
                                field[i],
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
                        let dterr = DecodeNumberField(
                            flen,
                            field[i],
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
                        let dterr = DecodeNumber(
                            flen,
                            field[i],
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

            DTK_STRING | DTK_SPECIAL => {
                let mut type_ = 0;
                let mut val = 0;
                let mut valtz: Option<&'static PgTz> = None;
                let dterr =
                    DecodeTimezoneAbbrev(i, field[i], &mut type_, &mut val, &mut valtz, extra);
                if dterr != 0 {
                    return dterr;
                }
                if type_ == UNKNOWN_FIELD {
                    type_ = DecodeSpecial(i, field[i], &mut val);
                }
                if type_ == IGNORE_DTF {
                    continue;
                }

                tmask = DTK_M(type_);
                match type_ {
                    RESERV => match val {
                        DTK_NOW => {
                            tmask = DTK_TIME_M;
                            *dtype = DTK_TIME;
                            tz::GetCurrentTimeUsec(tm, fsec, None);
                        }
                        DTK_ZULU => {
                            tmask = DTK_TIME_M | DTK_M(TZ);
                            *dtype = DTK_TIME;
                            tm.tm_hour = 0;
                            tm.tm_min = 0;
                            tm.tm_sec = 0;
                            tm.tm_isdst = 0;
                        }
                        _ => return DTERR_BAD_FORMAT,
                    },

                    DTZMOD => {
                        tmask |= DTK_M(DTZ);
                        tm.tm_isdst = 1;
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tzv -= val;
                    }

                    DTZ => {
                        tmask |= DTK_M(TZ);
                        tm.tm_isdst = 1;
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tzv = -val;
                        ftype[i] = DTK_TZ;
                    }

                    TZ => {
                        tm.tm_isdst = 0;
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tzv = -val;
                        ftype[i] = DTK_TZ;
                    }

                    DYNTZ => {
                        tmask |= DTK_M(TZ);
                        if !have_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        abbrevTz = valtz;
                        abbrev = Some(field[i]);
                        ftype[i] = DTK_TZ;
                    }

                    AMPM => mer = val,

                    ADBC => bc = val == BC,

                    UNITS | ISOTIME => {
                        tmask = 0;
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }

                    UNKNOWN_FIELD => {
                        match tz::pg_tzset(field[i]) {
                            Some(z) => namedTz = Some(z),
                            None => return DTERR_BAD_FORMAT,
                        }
                        tmask = DTK_M(TZ);
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
    }

    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    let dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
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

    if fmask & DTK_TIME_M != DTK_TIME_M {
        return DTERR_BAD_FORMAT;
    }

    // a full timezone spec may need the date to resolve DST status
    if let Some(z) = namedTz {
        if fmask & DTK_M(DTZMOD) != 0 {
            return DTERR_BAD_FORMAT;
        }
        let mut gmtoff = 0i64;
        if tz::pg_get_timezone_offset(z, &mut gmtoff) {
            // non-DST zone: no date needed
            tzv = -(gmtoff as i32);
        } else {
            // a date has to be specified
            if fmask & DTK_DATE_M != DTK_DATE_M {
                return DTERR_BAD_FORMAT;
            }
            tzv = DetermineTimeZoneOffset(tm, z);
        }
    }

    if let Some(z) = abbrevTz {
        let mut tt = pg_tm::default();
        if fmask & DTK_M(DTZMOD) != 0 {
            return DTERR_BAD_FORMAT;
        }
        if fmask & DTK_DATE_M == 0 {
            tz::GetCurrentDateTime(&mut tt);
        } else {
            if fmask & DTK_DATE_M != DTK_DATE_M {
                return DTERR_BAD_FORMAT;
            }
            tt.tm_year = tm.tm_year;
            tt.tm_mon = tm.tm_mon;
            tt.tm_mday = tm.tm_mday;
        }
        tt.tm_hour = tm.tm_hour;
        tt.tm_min = tm.tm_min;
        tt.tm_sec = tm.tm_sec;
        tzv = DetermineTimeZoneAbbrevOffset(&mut tt, abbrev.unwrap(), z);
        tm.tm_isdst = tt.tm_isdst;
    }

    // timezone not specified? then use session timezone
    if have_tz && fmask & DTK_M(TZ) == 0 {
        let mut tt = pg_tm::default();
        if fmask & DTK_M(DTZMOD) != 0 {
            return DTERR_BAD_FORMAT;
        }
        if fmask & DTK_DATE_M == 0 {
            tz::GetCurrentDateTime(&mut tt);
        } else {
            if fmask & DTK_DATE_M != DTK_DATE_M {
                return DTERR_BAD_FORMAT;
            }
            tt.tm_year = tm.tm_year;
            tt.tm_mon = tm.tm_mon;
            tt.tm_mday = tm.tm_mday;
        }
        tt.tm_hour = tm.tm_hour;
        tt.tm_min = tm.tm_min;
        tt.tm_sec = tm.tm_sec;
        let Some(z) = tz::session_timezone() else {
            panic!("session timezone not initialized (pg_timezone_initialize) — DecodeTimeOnly");
        };
        tzv = DetermineTimeZoneOffset(&mut tt, z);
        tm.tm_isdst = tt.tm_isdst;
    }

    if let Some(p) = tzp {
        *p = tzv;
    }
    0
}

#[inline]
fn int64_multiply_add(val: i64, multiplier: i64, sum: &mut i64) -> bool {
    match val.checked_mul(multiplier).and_then(|p| sum.checked_add(p)) {
        Some(v) => {
            *sum = v;
            true
        }
        None => false,
    }
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
    match itm_in.tm_usec.checked_add(usec) {
        Some(v) => {
            itm_in.tm_usec = v;
            true
        }
        None => false,
    }
}

fn AdjustFractDays(mut frac: f64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if frac == 0.0 {
        return true;
    }
    frac *= scale as f64;
    let extra_days = frac as i32;
    let Some(v) = itm_in.tm_mday.checked_add(extra_days) else {
        return false;
    };
    itm_in.tm_mday = v;
    frac -= extra_days as f64;
    AdjustFractMicroseconds(frac, USECS_PER_DAY, itm_in)
}

fn AdjustFractYears(frac: f64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    // C rint() rounds half to even
    let extra_months = (frac * scale as f64 * MONTHS_PER_YEAR as f64).round_ties_even() as i32;
    match itm_in.tm_mon.checked_add(extra_months) {
        Some(v) => {
            itm_in.tm_mon = v;
            true
        }
        None => false,
    }
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
    match (val as i32).checked_mul(scale).and_then(|days| itm_in.tm_mday.checked_add(days)) {
        Some(v) => {
            itm_in.tm_mday = v;
            true
        }
        None => false,
    }
}

fn AdjustMonths(val: i64, itm_in: &mut pg_itm_in) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    match itm_in.tm_mon.checked_add(val as i32) {
        Some(v) => {
            itm_in.tm_mon = v;
            true
        }
        None => false,
    }
}

fn AdjustYears(val: i64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return false;
    }
    match (val as i32).checked_mul(scale).and_then(|years| itm_in.tm_year.checked_add(years)) {
        Some(v) => {
            itm_in.tm_year = v;
            true
        }
        None => false,
    }
}

fn ClearPgItmIn(itm_in: &mut pg_itm_in) {
    itm_in.tm_usec = 0;
    itm_in.tm_mday = 0;
    itm_in.tm_mon = 0;
    itm_in.tm_year = 0;
}

// On DTERR return tm_usec may already be clobbered (the assignment precedes
// the overflow checks); DecodeInterval's DTK_TZ fallthrough keeps this C shape.
fn DecodeTimeForInterval(
    str_: &[u8],
    fmask: i32,
    range: i32,
    tmask: &mut i32,
    itm_in: &mut pg_itm_in,
) -> i32 {
    let mut itm = pg_itm::default();
    let dterr = DecodeTimeCommon(str_, fmask, range, tmask, &mut itm);
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

pub fn DecodeInterval(
    field: &[&[u8]],
    ftype: &[i32],
    nf: usize,
    range: i32,
    dtype: &mut i32,
    itm_in: &mut pg_itm_in,
) -> i32 {
    use crate::settings::interval_style;

    let mut force_negative = false;
    let mut is_before = false;
    let mut parsing_unit_val = false;
    let mut fmask = 0i32;
    let mut tmask;
    let mut type_ = IGNORE_DTF;
    let mut uval = 0i32;

    *dtype = DTK_DELTA;
    ClearPgItmIn(itm_in);

    // SQL "standard" syntax: the sign applies to the whole thing, but only
    // when there are no other explicit signs
    if interval_style() == INTSTYLE_SQL_STANDARD && nf > 0 && field[0].first() == Some(&b'-') {
        force_negative = true;
        for f in field.iter().take(nf).skip(1) {
            if matches!(f.first(), Some(&b'-') | Some(&b'+')) {
                force_negative = false;
                break;
            }
        }
    }

    // read through list backwards to pick up units before values
    for i in (0..nf).rev() {
        tmask = 0;
        let mut handled = false;
        match ftype[i] {
            t if t == DTK_TIME => {
                let dterr = DecodeTimeForInterval(field[i], fmask, range, &mut tmask, itm_in);
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
                // signed hh:mm or hh:mm:ss? then treat as a time value
                let rest = &field[i][1..];
                let mut tz_tmask = 0;
                if rest.contains(&b':')
                    && DecodeTimeForInterval(rest, fmask, range, &mut tz_tmask, itm_in) == 0
                {
                    tmask = tz_tmask;
                    if field[i][0] == b'-' {
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
                // else fall through to DTK_NUMBER handling
            }
            _ => {}
        }

        if !handled && matches!(ftype[i], t if t == DTK_TZ || t == DTK_DATE || t == DTK_NUMBER) {
            if type_ == IGNORE_DTF {
                // use typmod to decide what rightmost field is
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
                        || r == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) =>
                    {
                        DTK_MINUTE
                    }
                    _ => DTK_SECOND,
                };
            }

            let r = strtoi64(field[i]);
            if r.erange {
                return DTERR_FIELD_OVERFLOW;
            }
            let mut val = r.val;
            let mut cp = r.end;
            let mut fval: f64;

            if field[i].get(cp) == Some(&b'-') {
                // SQL "years-months" syntax
                let r2 = strtoint(&field[i][cp + 1..]);
                let mut val2 = r2.val;
                if r2.erange || val2 < 0 || val2 >= MONTHS_PER_YEAR {
                    return DTERR_FIELD_OVERFLOW;
                }
                cp = cp + 1 + r2.end;
                if cp < field[i].len() {
                    return DTERR_BAD_FORMAT;
                }
                type_ = DTK_MONTH;
                if field[i][0] == b'-' {
                    val2 = -val2;
                }
                let Some(v) = val
                    .checked_mul(MONTHS_PER_YEAR as i64)
                    .and_then(|v| v.checked_add(val2 as i64))
                else {
                    return DTERR_FIELD_OVERFLOW;
                };
                val = v;
                fval = 0.0;
            } else if field[i].get(cp) == Some(&b'.') {
                fval = 0.0;
                let dterr = ParseFraction(&field[i][cp..], &mut fval);
                if dterr != 0 {
                    return dterr;
                }
                if field[i][0] == b'-' {
                    fval = -fval;
                }
            } else if cp >= field[i].len() {
                fval = 0.0;
            } else {
                return DTERR_BAD_FORMAT;
            }

            if force_negative {
                if val > 0 {
                    val = -val;
                }
                if fval > 0.0 {
                    fval = -fval;
                }
            }

            match type_ {
                t if t == DTK_MICROSEC => {
                    if !AdjustMicroseconds(val, fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MICROSECOND);
                }
                t if t == DTK_MILLISEC => {
                    if !AdjustMicroseconds(val, fval, 1000, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MILLISECOND);
                }
                t if t == DTK_SECOND => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    // if any subseconds were specified, this counts as micro-
                    // and millisecond input too
                    tmask = if fval == 0.0 { DTK_M(SECOND) } else { DTK_ALL_SECS_M };
                }
                t if t == DTK_MINUTE => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MINUTE);
                }
                t if t == DTK_HOUR => {
                    if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(HOUR);
                    type_ = DTK_DAY; // set for next field
                }
                t if t == DTK_DAY => {
                    if !AdjustDays(val, 1, itm_in)
                        || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(DAY);
                }
                t if t == DTK_WEEK => {
                    if !AdjustDays(val, 7, itm_in) || !AdjustFractDays(fval, 7, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(WEEK);
                }
                t if t == DTK_MONTH => {
                    if !AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MONTH);
                }
                t if t == DTK_YEAR => {
                    if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(YEAR);
                }
                t if t == DTK_DECADE => {
                    if !AdjustYears(val, 10, itm_in) || !AdjustFractYears(fval, 10, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(DECADE);
                }
                t if t == DTK_CENTURY => {
                    if !AdjustYears(val, 100, itm_in) || !AdjustFractYears(fval, 100, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(CENTURY);
                }
                t if t == DTK_MILLENNIUM => {
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
            match ftype[i] {
                t if t == DTK_STRING || t == DTK_SPECIAL => {
                    // reject consecutive unhandled units
                    if parsing_unit_val {
                        return DTERR_BAD_FORMAT;
                    }
                    type_ = DecodeUnits(i, field[i], &mut uval);
                    if type_ == UNKNOWN_FIELD {
                        type_ = DecodeSpecial(i, field[i], &mut uval);
                    }
                    if type_ == IGNORE_DTF {
                        continue;
                    }

                    tmask = 0;
                    match type_ {
                        t if t == UNITS => {
                            type_ = uval;
                            parsing_unit_val = true;
                        }
                        t if t == AGO => {
                            // "ago" is only allowed to appear at the end
                            if i != nf - 1 {
                                return DTERR_BAD_FORMAT;
                            }
                            is_before = true;
                            type_ = uval;
                        }
                        t if t == RESERV => {
                            tmask = DTK_DATE_M | DTK_TIME_M;
                            // only reserved words for infinite intervals,
                            // standing alone
                            if uval != DTK_LATE && uval != DTK_EARLY {
                                return DTERR_BAD_FORMAT;
                            }
                            if i != nf - 1 {
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
    }

    // ensure that at least one time field has been found
    if fmask == 0 {
        return DTERR_BAD_FORMAT;
    }

    // reject if unit appeared and was never handled
    if parsing_unit_val {
        return DTERR_BAD_FORMAT;
    }

    // finally, AGO negates everything
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

/// C strtod prefix parse: value and byte offset just past the parsed number.
fn strtod_prefix(s: &[u8]) -> Option<(f64, usize)> {
    let mut i = 0usize;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let start = i;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        i += 1;
    }
    let mut saw_digit = false;
    while i < s.len() && is_digit(s[i]) {
        i += 1;
        saw_digit = true;
    }
    if i < s.len() && s[i] == b'.' {
        i += 1;
        while i < s.len() && is_digit(s[i]) {
            i += 1;
            saw_digit = true;
        }
    }
    if saw_digit && i < s.len() && (s[i] == b'e' || s[i] == b'E') {
        let mut j = i + 1;
        if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
            j += 1;
        }
        let exp_start = j;
        while j < s.len() && is_digit(s[j]) {
            j += 1;
        }
        if j > exp_start {
            i = j;
        }
    }
    if !saw_digit {
        return None;
    }
    let parsed = core::str::from_utf8(&s[start..i]).ok()?;
    parsed.parse::<f64>().ok().map(|v| (v, i))
}

fn ParseISO8601Number(s: &[u8], end: &mut usize, ipart: &mut i64, fpart: &mut f64) -> i32 {
    if !(s.first().is_some_and(|&c| is_digit(c))
        || s.first() == Some(&b'-')
        || s.first() == Some(&b'.'))
    {
        return DTERR_BAD_FORMAT;
    }
    let Some((val, e)) = strtod_prefix(s) else {
        return DTERR_BAD_FORMAT;
    };
    if e == 0 {
        return DTERR_BAD_FORMAT;
    }
    *end = e;
    // watch out for overflow, including infinities; reject NaN too
    if val.is_nan() || !(-1.0e15..=1.0e15).contains(&val) {
        return DTERR_FIELD_OVERFLOW;
    }
    // be very sure we truncate towards zero (cf dtrunc())
    *ipart = if val >= 0.0 { val.floor() as i64 } else { -((-val).floor() as i64) };
    *fpart = val - *ipart as f64;
    0
}

fn ISO8601IntegerWidth(fieldstart: &[u8]) -> i32 {
    // we might have had a leading '-'
    let mut i = usize::from(fieldstart.first() == Some(&b'-'));
    let mut n = 0;
    while i < fieldstart.len() && is_digit(fieldstart[i]) {
        n += 1;
        i += 1;
    }
    n
}

pub fn DecodeISO8601Interval(s: &[u8], dtype: &mut i32, itm_in: &mut pg_itm_in) -> i32 {
    let mut datepart = true;
    let mut havefield = false;

    *dtype = DTK_DELTA;
    ClearPgItmIn(itm_in);

    if s.len() < 2 || s[0] != b'P' {
        return DTERR_BAD_FORMAT;
    }

    let mut pos = 1usize;
    while pos < s.len() {
        if s[pos] == b'T' {
            datepart = false;
            havefield = false;
            pos += 1;
            continue;
        }

        let fieldstart = pos;
        let mut val: i64 = 0;
        let mut fval: f64 = 0.0;
        let mut adv = 0usize;
        let dterr = ParseISO8601Number(&s[pos..], &mut adv, &mut val, &mut fval);
        if dterr != 0 {
            return dterr;
        }
        pos += adv;

        // note: we could step off the end of the string here (unit = NUL)
        let unit = s.get(pos).copied().unwrap_or(0);
        if pos < s.len() {
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
                u @ (b'T' | 0 | b'-') => {
                    // ISO 8601 4.4.3.3 Basic Format (yyyymmdd), else Extended
                    if (u == b'T' || u == 0)
                        && ISO8601IntegerWidth(&s[fieldstart..]) == 8
                        && !havefield
                    {
                        if !AdjustYears(val / 10000, 1, itm_in)
                            || !AdjustMonths((val / 100) % 100, itm_in)
                            || !AdjustDays(val % 100, 1, itm_in)
                            || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if u == 0 {
                            return 0;
                        }
                        datepart = false;
                        havefield = false;
                        continue;
                    }

                    if havefield {
                        return DTERR_BAD_FORMAT;
                    }
                    if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if u == 0 {
                        return 0;
                    }
                    if u == b'T' {
                        datepart = false;
                        havefield = false;
                        continue;
                    }

                    let mut adv2 = 0;
                    let dterr = ParseISO8601Number(&s[pos..], &mut adv2, &mut val, &mut fval);
                    if dterr != 0 {
                        return dterr;
                    }
                    pos += adv2;
                    if !AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= s.len() {
                        return 0;
                    }
                    if s[pos] == b'T' {
                        datepart = false;
                        havefield = false;
                        pos += 1;
                        continue;
                    }
                    if s[pos] != b'-' {
                        return DTERR_BAD_FORMAT;
                    }
                    pos += 1;

                    let mut adv3 = 0;
                    let dterr = ParseISO8601Number(&s[pos..], &mut adv3, &mut val, &mut fval);
                    if dterr != 0 {
                        return dterr;
                    }
                    pos += adv3;
                    if !AdjustDays(val, 1, itm_in)
                        || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= s.len() {
                        return 0;
                    }
                    if s[pos] == b'T' {
                        datepart = false;
                        havefield = false;
                        pos += 1;
                        continue;
                    }
                    return DTERR_BAD_FORMAT;
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
                u @ (0 | b':') => {
                    // ISO 8601 4.4.3.3 Basic Format (hhmmss), else Extended
                    if u == 0 && ISO8601IntegerWidth(&s[fieldstart..]) == 6 && !havefield {
                        if !AdjustMicroseconds(val / 10000, 0.0, USECS_PER_HOUR, itm_in)
                            || !AdjustMicroseconds((val / 100) % 100, 0.0, USECS_PER_MINUTE, itm_in)
                            || !AdjustMicroseconds(val % 100, 0.0, USECS_PER_SEC, itm_in)
                            || !AdjustFractMicroseconds(fval, 1, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        return 0;
                    }

                    if havefield {
                        return DTERR_BAD_FORMAT;
                    }
                    if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if u == 0 {
                        return 0;
                    }

                    let mut adv2 = 0;
                    let dterr = ParseISO8601Number(&s[pos..], &mut adv2, &mut val, &mut fval);
                    if dterr != 0 {
                        return dterr;
                    }
                    pos += adv2;
                    if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= s.len() {
                        return 0;
                    }
                    if s[pos] != b':' {
                        return DTERR_BAD_FORMAT;
                    }
                    pos += 1;

                    let mut adv3 = 0;
                    let dterr = ParseISO8601Number(&s[pos..], &mut adv3, &mut val, &mut fval);
                    if dterr != 0 {
                        return dterr;
                    }
                    pos += adv3;
                    if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= s.len() {
                        return 0;
                    }
                    return DTERR_BAD_FORMAT;
                }
                _ => return DTERR_BAD_FORMAT,
            }
        }

        havefield = true;
    }

    0
}

pub fn CheckDateTokenTable(table: &[DateTkn]) -> bool {
    let mut ok = true;
    for i in 0..table.len() {
        if table[i].token[TOKMAXLEN] != 0 {
            return false; // token too long to be NUL-terminated
        }
        if i > 0 && tokcmp(table[i - 1].token_bytes(), &table[i].token) >= 0 {
            ok = false;
        }
    }
    ok
}

pub fn CheckDateTokenTables() -> bool {
    debug_assert_eq!(UNIX_EPOCH_JDATE, date2j(1970, 1, 1));
    debug_assert_eq!(POSTGRES_EPOCH_JDATE, date2j(2000, 1, 1));
    CheckDateTokenTable(&DATETKTBL) && CheckDateTokenTable(&DELTATKTBL)
}
