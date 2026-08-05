use crate::*;
use core::cell::Cell;

pub fn pg_localtime(t: pg_time_t, tz: &PgTz) -> Option<PgTm<'_>> {
    localsub(&tz.state, t)
}

// Outside a repeating zone's table: map by whole 400-year cycles, convert,
// shift tm_year back (timesub's leap scan sees the mapped time, as in C).
fn localsub(sp: &TzState, t: pg_time_t) -> Option<PgTm<'_>> {
    let timecnt = sp.timecnt as usize;
    if timecnt > 0 && ((sp.goback && t < sp.ats[0]) || (sp.goahead && t > sp.ats[timecnt - 1])) {
        let mapping = repeat_mapping(t, sp)?; /* "cannot happen" */
        let mut result = localsub(sp, mapping.newt)?;
        let newy = if t < sp.ats[0] {
            (result.tm_year as i64).wrapping_sub(mapping.years)
        } else {
            (result.tm_year as i64).wrapping_add(mapping.years)
        };
        if !(i32::MIN as i64 <= newy && newy <= i32::MAX as i64) {
            return None;
        }
        result.tm_year = newy as i32;
        return Some(result);
    }
    let i = if timecnt == 0 || t < sp.ats[0] {
        sp.defaulttype as usize
    } else {
        let lo = sp.ats[..timecnt].partition_point(|&at| at <= t);
        sp.types[lo - 1] as usize
    };
    let tt = sp.ttis[i];
    let mut tm = timesub(t, tt.tt_utoff, Some(sp))?;
    tm.tm_isdst = tt.tt_isdst as i32;
    tm.tm_zone = Some(zone_name(sp, tt.tt_desigidx));
    Some(tm)
}

thread_local! {
    // C gmtsub's gmtptr: built fully before publish so a re-entrant pg_gmtime
    // sees None or a finished state (loser leaks).
    static GMT_STATE: Cell<Option<&'static TzState>> = const { Cell::new(None) };
}

fn gmtload(sp: &mut TzState) {
    if tzload(b"GMT", None, sp, true).is_err() {
        tzparse(b"GMT", sp, true);
    }
}

fn gmt_state() -> &'static TzState {
    GMT_STATE.with(|cell| {
        if let Some(gmtptr) = cell.get() {
            return gmtptr;
        }
        let mut boxed = Box::new(TzState::new());
        gmtload(&mut boxed);
        if cell.get().is_none() {
            cell.set(Some(Box::leak(boxed)));
        }
        cell.get().unwrap()
    })
}

fn gmtsub(t: pg_time_t, offset: i32) -> Option<PgTm<'static>> {
    let gmtptr = gmt_state();
    let mut tm = timesub(t, offset, Some(gmtptr))?;
    // "no time for a treasure hunt" (C); PG only calls with offset 0.
    tm.tm_zone = Some(if offset != 0 {
        WILDABBR
    } else {
        cstr_str(&gmtptr.chars, 0)
    });
    Some(tm)
}

pub fn pg_gmtime(t: pg_time_t) -> Option<PgTm<'static>> {
    gmtsub(t, 0)
}

struct RepeatMapping {
    newt: pg_time_t,
    seconds: pg_time_t,
    years: pg_time_t,
}

// C's two cycle-count formulas are equal (truncated division composes);
// arithmetic wraps as C -fwrapv, the trailing range check catches wraps.
fn repeat_mapping(timep: pg_time_t, sp: &TzState) -> Option<RepeatMapping> {
    let timecnt = sp.timecnt as usize;
    let below = timep < sp.ats[0];
    let seconds = if below {
        sp.ats[0].wrapping_sub(timep)
    } else {
        timep.wrapping_sub(sp.ats[timecnt - 1])
    }
    .wrapping_sub(1);
    let years = (seconds / SECSPERREPEAT)
        .wrapping_add(1)
        .wrapping_mul(YEARSPERREPEAT as i64);
    let seconds = years.wrapping_mul(AVGSECSPERYEAR);
    let newt = if below {
        timep.wrapping_add(seconds)
    } else {
        timep.wrapping_sub(seconds)
    };
    if newt < sp.ats[0] || newt > sp.ats[timecnt - 1] {
        return None; /* "cannot happen" */
    }
    Some(RepeatMapping {
        newt,
        seconds,
        years,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DstBoundary {
    pub before_gmtoff: i64,
    pub before_isdst: i32,
    pub boundary: pg_time_t,
    pub after_gmtoff: i64,
    pub after_isdst: i32,
}

/// C `pg_next_dst_boundary`'s -1 / 0 / 1 result surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NextDstBoundary {
    Overflow,
    NoTransition {
        before_gmtoff: i64,
        before_isdst: i32,
    },
    Boundary(DstBoundary),
}

pub fn pg_next_dst_boundary(timep: pg_time_t, tz: &PgTz) -> NextDstBoundary {
    next_dst_boundary_impl(timep, &tz.state)
}

fn next_dst_boundary_impl(timep: pg_time_t, sp: &TzState) -> NextDstBoundary {
    if sp.timecnt == 0 {
        let tt = sp.ttis[sp.defaulttype as usize];
        return NextDstBoundary::NoTransition {
            before_gmtoff: tt.tt_utoff as i64,
            before_isdst: tt.tt_isdst as i32,
        };
    }
    let timecnt = sp.timecnt as usize;

    if (sp.goback && timep < sp.ats[0]) || (sp.goahead && timep > sp.ats[timecnt - 1]) {
        let Some(mapping) = repeat_mapping(timep, sp) else {
            return NextDstBoundary::Overflow;
        };
        return match next_dst_boundary_impl(mapping.newt, sp) {
            NextDstBoundary::Boundary(mut b) => {
                b.boundary = if timep < sp.ats[0] {
                    b.boundary.wrapping_sub(mapping.seconds)
                } else {
                    b.boundary.wrapping_add(mapping.seconds)
                };
                NextDstBoundary::Boundary(b)
            }
            other => other,
        };
    }

    if timep >= sp.ats[timecnt - 1] {
        let tt = sp.ttis[sp.types[timecnt - 1] as usize];
        return NextDstBoundary::NoTransition {
            before_gmtoff: tt.tt_utoff as i64,
            before_isdst: tt.tt_isdst as i32,
        };
    }

    if timep < sp.ats[0] {
        let before = sp.ttis[sp.defaulttype as usize];
        let after = sp.ttis[sp.types[0] as usize];
        return NextDstBoundary::Boundary(DstBoundary {
            before_gmtoff: before.tt_utoff as i64,
            before_isdst: before.tt_isdst as i32,
            boundary: sp.ats[0],
            after_gmtoff: after.tt_utoff as i64,
            after_isdst: after.tt_isdst as i32,
        });
    }

    let mut lo = 1usize;
    let mut hi = timecnt - 1;
    while lo < hi {
        let mid = (lo + hi) >> 1;
        if timep < sp.ats[mid] {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    let before = sp.ttis[sp.types[lo - 1] as usize];
    let after = sp.ttis[sp.types[lo] as usize];
    NextDstBoundary::Boundary(DstBoundary {
        before_gmtoff: before.tt_utoff as i64,
        before_isdst: before.tt_isdst as i32,
        boundary: sp.ats[lo],
        after_gmtoff: after.tt_utoff as i64,
        after_isdst: after.tt_isdst as i32,
    })
}

/// Meaning at or before `timep`, else the first use after it; matched
/// case-sensitively (callers pass all-upper-case).
pub fn pg_interpret_timezone_abbrev(
    abbrev: &[u8],
    timep: pg_time_t,
    tz: &PgTz,
) -> Option<(i64, i32)> {
    let sp = &tz.state;
    let abbrind = find_abbrev(sp, abbrev)?;

    let cutoff = sp.ats[..sp.timecnt as usize].partition_point(|&at| at <= timep);

    for i in (0..cutoff).rev() {
        let tt = sp.ttis[sp.types[i] as usize];
        if tt.tt_desigidx == abbrind {
            return Some((tt.tt_utoff as i64, tt.tt_isdst as i32));
        }
    }

    let tt = sp.ttis[sp.defaulttype as usize];
    if tt.tt_desigidx == abbrind {
        return Some((tt.tt_utoff as i64, tt.tt_isdst as i32));
    }

    for i in cutoff..sp.timecnt as usize {
        let tt = sp.ttis[sp.types[i] as usize];
        if tt.tt_desigidx == abbrind {
            return Some((tt.tt_utoff as i64, tt.tt_isdst as i32));
        }
    }

    None /* hm, not actually used in any interval? */
}

/// `(isfixed, gmtoff, isdst)`: `!isfixed` when the abbrev has several
/// meanings (gmtoff/isdst are then the first use, C's last-stored values).
pub fn pg_timezone_abbrev_is_known(abbrev: &[u8], tz: &PgTz) -> Option<(bool, i64, i32)> {
    let sp = &tz.state;
    let abbrind = find_abbrev(sp, abbrev)?;
    let mut found: Option<(bool, i64, i32)> = None;

    for tt in &sp.ttis[..sp.typecnt as usize] {
        if tt.tt_desigidx != abbrind {
            continue;
        }
        match &mut found {
            None => found = Some((true, tt.tt_utoff as i64, tt.tt_isdst as i32)),
            Some((isfixed, gmtoff, isdst)) => {
                if *gmtoff != tt.tt_utoff as i64 || *isdst != tt.tt_isdst as i32 {
                    *isfixed = false;
                    break;
                }
            }
        }
    }

    found
}

pub fn pg_get_next_timezone_abbrev<'tz>(indx: &mut i32, tz: &'tz PgTz) -> Option<&'tz [u8]> {
    let sp = &tz.state;
    let start = usize::try_from(*indx).ok()?;
    if start >= sp.charcnt as usize {
        return None;
    }
    let abbrev = cstr_bytes(&sp.chars, start);
    *indx = (start + abbrev.len() + 1) as i32;
    Some(abbrev)
}

pub fn pg_get_timezone_offset(tz: &PgTz) -> Option<i64> {
    let sp = &tz.state;
    let first = sp.ttis[0].tt_utoff;
    sp.ttis[..sp.typecnt as usize]
        .iter()
        .all(|tt| tt.tt_utoff == first)
        .then_some(first as i64)
}

pub fn pg_get_timezone_name(tz: &PgTz) -> &[u8] {
    tz.name()
}

// Rejects leap-second-aware timekeeping (tm_sec != 0 at GMT 2000-01-01).
pub fn pg_tz_acceptable(tz: &PgTz) -> bool {
    let time2000 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECSPERDAY;
    pg_localtime(time2000, tz).is_some_and(|tm| tm.tm_sec == 0)
}

fn leaps_thru_end_of_nonneg(y: i32) -> i32 {
    y / 4 - y / 100 + y / 400
}

fn leaps_thru_end_of(y: i32) -> i32 {
    if y < 0 {
        -1 - leaps_thru_end_of_nonneg(-1 - y)
    } else {
        leaps_thru_end_of_nonneg(y)
    }
}

pub(crate) fn timesub(timep: pg_time_t, offset: i32, sp: Option<&TzState>) -> Option<PgTm<'static>> {
    let (corr, hit) = leap_correction(sp, timep);

    let mut y: i32 = EPOCH_YEAR;
    // Truncating division; the loops below absorb the negative remainder.
    let mut tdays: i64 = timep / SECSPERDAY;
    let mut rem: i64 = timep % SECSPERDAY;
    while tdays < 0 || tdays >= year_lengths(is_leap(y)) as i64 {
        let tdelta = tdays / DAYSPERLYEAR as i64;
        let mut idelta = i32::try_from(tdelta).ok()?;
        if idelta == 0 {
            idelta = if tdays < 0 { -1 } else { 1 };
        }
        let mut newy = y;
        if increment_overflow(&mut newy, idelta) {
            return None;
        }
        // wrapping_sub: C (-fwrapv) wraps when newy/y is INT_MIN.
        let leapdays =
            leaps_thru_end_of(newy.wrapping_sub(1)) - leaps_thru_end_of(y.wrapping_sub(1));
        tdays -= (newy as i64 - y as i64) * DAYSPERNYEAR as i64;
        tdays -= leapdays as i64;
        y = newy;
    }

    let mut idays = tdays as i32;
    rem += offset as i64 - corr;
    while rem < 0 {
        rem += SECSPERDAY;
        idays -= 1;
    }
    while rem >= SECSPERDAY {
        rem -= SECSPERDAY;
        idays += 1;
    }
    while idays < 0 {
        if increment_overflow(&mut y, -1) {
            return None;
        }
        idays += year_lengths(is_leap(y));
    }
    while idays >= year_lengths(is_leap(y)) {
        idays -= year_lengths(is_leap(y));
        if increment_overflow(&mut y, 1) {
            return None;
        }
    }

    let mut tm_year = y;
    if increment_overflow(&mut tm_year, -TM_YEAR_BASE) {
        return None;
    }
    let tm_yday = idays;

    // wrapping_sub: C wraps y - EPOCH_YEAR for y within 1970 of INT_MIN.
    let mut tm_wday = EPOCH_WDAY
        + (y.wrapping_sub(EPOCH_YEAR) % DAYSPERWEEK) * (DAYSPERNYEAR % DAYSPERWEEK)
        + leaps_thru_end_of(y - 1)
        - leaps_thru_end_of(EPOCH_YEAR - 1)
        + idays;
    tm_wday %= DAYSPERWEEK;
    if tm_wday < 0 {
        tm_wday += DAYSPERWEEK;
    }

    let tm_hour = (rem / SECSPERHOUR) as i32;
    rem %= SECSPERHOUR;
    let tm_min = (rem / SECSPERMIN) as i32;
    // A positive leap second uses the "??:59:60" representation.
    let tm_sec = (rem % SECSPERMIN) as i32 + hit as i32;

    let leap = is_leap(y);
    let mut idays = idays;
    let mut tm_mon = 0usize;
    while idays >= mon_lengths(leap, tm_mon) {
        idays -= mon_lengths(leap, tm_mon);
        tm_mon += 1;
    }

    Some(PgTm {
        tm_sec,
        tm_min,
        tm_hour,
        tm_mday: idays + 1,
        tm_mon: tm_mon as i32,
        tm_year,
        tm_wday,
        tm_yday,
        tm_isdst: 0,
        tm_gmtoff: offset as i64,
        tm_zone: None,
    })
}

// Total leap correction at `timep`; `hit` = exactly on a positive leap second.
fn leap_correction(sp: Option<&TzState>, timep: pg_time_t) -> (i64, bool) {
    let Some(sp) = sp else {
        return (0, false);
    };
    for i in (0..sp.leapcnt as usize).rev() {
        let leap = sp.lsis[i];
        if timep >= leap.ls_trans {
            let previous = i.checked_sub(1).map(|i| sp.lsis[i].ls_corr).unwrap_or(0);
            return (
                leap.ls_corr,
                timep == leap.ls_trans && previous < leap.ls_corr,
            );
        }
    }
    (0, false)
}

fn find_abbrev(sp: &TzState, abbrev: &[u8]) -> Option<i32> {
    let mut index = 0usize;
    while index < sp.charcnt as usize {
        let cur = cstr_bytes(&sp.chars, index);
        if cur == abbrev {
            return Some(index as i32);
        }
        index += cur.len() + 1;
    }
    None
}

// WILDABBR guards out-of-range desigidx or non-UTF-8 contents.
fn zone_name(sp: &TzState, index: i32) -> &str {
    let Ok(index) = usize::try_from(index) else {
        return WILDABBR;
    };
    if index >= sp.chars.len() {
        return WILDABBR;
    }
    cstr_str(&sp.chars, index)
}
