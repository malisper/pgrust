//! Port of PostgreSQL's `src/timezone/localtime.c`: TZif parsing, POSIX TZ
//! parsing, and the `pg_localtime`/`pg_gmtime`/DST-boundary calendar math.
//!
//! Char buffers are plain `u8` arrays; `pg_tm.tm_zone` borrows the zone
//! abbreviation from the zone's `state.chars` table, exactly as C's pointer
//! does. Timezone names are `&str` and DST-boundary information is returned
//! through enums/structs rather than caller-provided output pointers. The
//! shared `pg_tm`/`pg_tz`/`state` vocabulary (pgtime.h/pgtz.h) lives in the
//! `types-pgtime` crate. The file open goes through the
//! `backend-timezone-pgtz` seam (C `pg_open_tzfile`); everything else is
//! self-contained.

#![allow(non_camel_case_types)]

pub use ::types_core::primitive::pg_time_t;
pub use pgtime::{lsinfo, pg_tm, pg_tz, state, ttinfo, TZ_STRLEN_MAX};

use pgtime::{CHARS_SIZE, TZ_MAX_CHARS, TZ_MAX_LEAPS, TZ_MAX_TIMES, TZ_MAX_TYPES};

const SECSPERMIN: i64 = 60;
const MINSPERHOUR: i64 = 60;
const HOURSPERDAY: i64 = 24;
const DAYSPERWEEK: i32 = 7;
const DAYSPERNYEAR: i32 = 365;
const DAYSPERLYEAR: i32 = 366;
const MONSPERYEAR: usize = 12;
const SECSPERHOUR: i64 = SECSPERMIN * MINSPERHOUR;
const SECSPERDAY: i64 = SECSPERHOUR * HOURSPERDAY;
const YEARSPERREPEAT: i32 = 400;
const AVGSECSPERYEAR: i64 = 31_556_952;
// SECSPERREPEAT == YEARSPERREPEAT * AVGSECSPERYEAR == 146097 * SECSPERDAY,
// the Gregorian repeat period in seconds (private.h).
const SECSPERREPEAT: i64 = YEARSPERREPEAT as i64 * AVGSECSPERYEAR;
const SECSPERREPEAT_BITS: u32 = 34;
const EPOCH_YEAR: i32 = 1970;
const EPOCH_WDAY: i32 = 4;
const TM_YEAR_BASE: i32 = 1900;
// pg_time_t is i64, so TIME_T_MIN/MAX are the full i64 range.
const TIME_T_MIN: pg_time_t = pg_time_t::MIN;
const TIME_T_MAX: pg_time_t = pg_time_t::MAX;
const POSTGRES_EPOCH_JDATE: pg_time_t = 2_451_545;
const UNIX_EPOCH_JDATE: pg_time_t = 2_440_588;
// C TZDEFRULESTRING is ",M3.2.0,M11.1.0"; the leading comma is consumed by
// the rule-separator check, so the rules proper start here.
const TZDEFRULESTRING: &str = "M3.2.0,M11.1.0";
const WILDABBR: &str = "   ";
const GMT: &str = "GMT";

// sizeof(union input_buffer) in localtime.c:
// 2 * sizeof(struct tzhead) + 2 * sizeof(struct state) + 4 * TZ_MAX_TIMES.
// sizeof(struct tzhead) is 44 and sizeof(struct state) is 23440 on LP64; the
// single-read cap is part of the accept/reject behavior (a file larger than
// this is truncated and then fails the length checks).
const INPUT_BUF_SIZE: usize = 2 * 44 + 2 * 23440 + 4 * TZ_MAX_TIMES;

/// Errno-style result of the TZif loader, preserving C's
/// ENOENT/EINVAL/ENOMEM distinction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TzLoadError {
    /// File could not be found/opened (C `ENOENT`).
    NotFound,
    /// File contents were malformed, truncated, or unreadable (C `EINVAL`
    /// and the read-error errno cases).
    Invalid,
    /// The read buffer could not be allocated (C `tzload` returns errno —
    /// `ENOMEM` — when `malloc(sizeof(union local_storage))` fails).
    OutOfMemory,
}

/// Load and parse a TZif file by name. Returns the canonical name on success
/// when `want_canonical`, matching C's `tzload` (which writes `canonname`
/// when non-NULL and returns 0 on success).
pub fn tzload(
    name: &str,
    sp: &mut state,
    want_canonical: bool,
    doextend: bool,
) -> Result<Option<String>, TzLoadError> {
    let name = name.strip_prefix(':').unwrap_or(name);
    let Some((file, canonical)) =
        pgtz_seams::pg_open_tzfile::call(name, want_canonical)
    else {
        return Err(TzLoadError::NotFound);
    };

    // C tzload mallocs its read buffer (`union local_storage`) and returns
    // errno (ENOMEM) when that fails; reserve the full single-read cap up
    // front so the read itself never allocates.
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(INPUT_BUF_SIZE).is_err() {
        return Err(TzLoadError::OutOfMemory);
    }
    {
        use std::io::Read;
        if file
            .take(INPUT_BUF_SIZE as u64)
            .read_to_end(&mut bytes)
            .is_err()
        {
            return Err(TzLoadError::Invalid);
        }
    }
    parse_tzif(&bytes, sp, doextend)?;
    Ok(if want_canonical { canonical } else { None })
}

/// Given a POSIX section 8-style TZ string, fill in the rule tables as
/// appropriate. Returns true on success, false on failure (C `tzparse`).
pub fn tzparse(name: &str, sp: &mut state, lastditch: bool) -> bool {
    let (stdname, stdoffset, rest) = if lastditch {
        // Unlike IANA, don't assume name is exactly "GMT".
        (name, 0, "")
    } else {
        let Some((stdname, rest)) = parse_zone_name(name) else {
            return false;
        };
        // We allow empty STD abbrev, unlike IANA; an empty remainder
        // (no offset) is still rejected.
        if rest.is_empty() {
            return false;
        }
        let Some((stdoffset, rest)) = parse_offset(rest) else {
            return false;
        };
        (stdname, stdoffset, rest)
    };

    let parsed_dst = if rest.is_empty() {
        None
    } else {
        let Some((dstname, rest)) = parse_zone_name(rest) else {
            return false;
        };
        if dstname.is_empty() {
            return false;
        }
        let (dstoffset, rest) = if !rest.is_empty() && !rest.starts_with([',', ';']) {
            let Some((dstoffset, rest)) = parse_offset(rest) else {
                return false;
            };
            (dstoffset, rest)
        } else {
            (stdoffset - SECSPERHOUR as i32, rest)
        };
        // PG desupports TZDEFRULES (load_ok is always false in C): a DST name
        // with no transition rule gets the default USA rules.
        let rules = if rest.is_empty() {
            TZDEFRULESTRING
        } else if let Some(rules) = rest.strip_prefix([',', ';']) {
            rules
        } else {
            // C's leftover-text branch: anything that is neither a rule
            // separator nor end-of-string is rejected.
            return false;
        };
        let Some((start, rules)) = parse_rule(rules) else {
            return false;
        };
        let Some(rules) = rules.strip_prefix(',') else {
            return false;
        };
        let Some((end, rules)) = parse_rule(rules) else {
            return false;
        };
        if !rules.is_empty() {
            return false;
        }
        Some((dstname, dstoffset, start, end))
    };

    let charcnt = stdname.len() + 1 + parsed_dst.map(|dst| dst.0.len() + 1).unwrap_or(0);
    if charcnt > CHARS_SIZE {
        return false;
    }

    *sp = state::default();
    sp.typecnt = if parsed_dst.is_some() { 2 } else { 1 };
    sp.charcnt = charcnt as i32;
    sp.defaulttype = 0;
    sp.ttis[0] = ttinfo {
        tt_utoff: -stdoffset,
        tt_isdst: false,
        tt_desigidx: 0,
        tt_ttisstd: false,
        tt_ttisut: false,
    };
    write_chars_at(&mut sp.chars, 0, stdname.as_bytes());
    if let Some((dstname, dstoffset, start, end)) = parsed_dst {
        let dst_index = stdname.len() + 1;
        sp.ttis[1] = ttinfo {
            tt_utoff: -dstoffset,
            tt_isdst: true,
            tt_desigidx: dst_index as i32,
            tt_ttisstd: false,
            tt_ttisut: false,
        };
        write_chars_at(&mut sp.chars, dst_index, dstname.as_bytes());
        build_posix_transitions(sp, stdoffset, dstoffset, 0, dst_index as i32, start, end);
    }
    true
}

// ---------------------------------------------------------------------------
// GMT state (C gmtsub's static gmtptr + gmtload)

thread_local! {
    static GMT_STATE: std::cell::Cell<Option<&'static state>> =
        const { std::cell::Cell::new(None) };
}

fn gmtload(sp: &mut state) {
    if tzload(GMT, sp, false, true).is_err() {
        tzparse(GMT, sp, true);
    }
}

/// Fetch (loading on first use) this thread's GMT `struct state`, the
/// equivalent of C gmtsub's `static struct state *gmtptr` (allocated once
/// with malloc and never freed; the per-thread leak here matches that).
/// Returns `None` when the allocation fails, mirroring C gmtsub's NULL
/// return on malloc failure.
///
/// The state is built fully in a local before being published: gmtload runs
/// through tzload and the `pg_open_tzfile` seam into not-yet-ported code, so
/// a re-entrant pg_gmtime call during initialization (e.g. log-line
/// timestamp formatting) must find either `None` or a finished state, never
/// a half-initialized one. If such a recursive call won the race to publish,
/// keep its state and quietly leak ours.
fn gmt_state() -> Option<&'static state> {
    GMT_STATE.with(|cell| {
        if let Some(gmtptr) = cell.get() {
            return Some(gmtptr);
        }
        // Fallibly allocate the state (C: malloc(sizeof(struct state))),
        // then load it in place.
        let mut storage: Vec<state> = Vec::new();
        if storage.try_reserve_exact(1).is_err() {
            return None; /* C: errno should be set by malloc */
        }
        storage.push(state::default());
        let mut boxed: Box<[state]> = storage.into_boxed_slice();
        gmtload(&mut boxed[0]);
        if cell.get().is_none() {
            cell.set(Some(&Box::leak(boxed)[0]));
        }
        cell.get()
    })
}

/// gmtsub is to gmtime as localsub is to localtime; the GMT `struct state`
/// is loaded on first use and kept per backend thread.
fn gmtsub(timep: pg_time_t, offset: i32) -> Option<pg_tm> {
    let gmtptr = gmt_state()?;
    let mut tm = timesub(timep, offset, Some(gmtptr))?;
    // Could get fancy here and deliver something such as "+xx" or "-xx"
    // if offset is non-zero, but this is no time for a treasure hunt.
    tm.tm_zone = Some(
        if offset != 0 {
            WILDABBR
        } else {
            cstr_str(&gmtptr.chars, 0)
        }
        .to_string(),
    );
    Some(tm)
}

/// Convert `timep` to broken-down local time in timezone `tz`.
///
/// Returns `None` if the conversion overflows (C returns NULL).
pub fn pg_localtime(timep: pg_time_t, tz: &pg_tz) -> Option<pg_tm> {
    localsub(tz.state(), timep)
}

/// C `localsub`: the guts of localtime, freely callable. For times outside a
/// repeating zone's transition table, the C code maps the time into the table
/// by whole 400-year Gregorian cycles, converts the mapped time, and then
/// shifts `tm_year` back — so the leap-second scan inside `timesub` sees the
/// mapped time, exactly as in C.
fn localsub(sp: &state, t: pg_time_t) -> Option<pg_tm> {
    let timecnt = sp.timecnt as usize;
    if timecnt > 0 && ((sp.goback && t < sp.ats[0]) || (sp.goahead && t > sp.ats[timecnt - 1])) {
        let mapping = repeat_mapping(t, sp)?; /* None: "cannot happen" */
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
    tm.tm_zone = zone_name(sp, tt.tt_desigidx).map(str::to_string);
    Some(tm)
}

/// Convert `timep` to broken-down UTC time.
pub fn pg_gmtime(timep: pg_time_t) -> Option<pg_tm> {
    gmtsub(timep, 0)
}

/// The 400-year-cycle extrapolation shared by C `localsub` and
/// `pg_next_dst_boundary`: `newt` is the time mapped into the transition
/// table, `seconds` the whole-cycle shift in seconds, and `years` the same
/// shift in years. (`pg_next_dst_boundary` computes the cycle count as
/// `seconds / YEARSPERREPEAT / AVGSECSPERYEAR`, which equals localsub's
/// `seconds / SECSPERREPEAT` — truncated division composes.) `None` mirrors
/// C's "cannot happen" range-check failure (NULL / -1). Arithmetic wraps like
/// C built with -fwrapv; the trailing range check catches wrapped results.
struct RepeatMapping {
    newt: pg_time_t,
    seconds: pg_time_t,
    years: pg_time_t,
}

/// Precondition (as in C): `sp.timecnt > 0` and `timep` is outside the table
/// on the goback/goahead side.
fn repeat_mapping(timep: pg_time_t, sp: &state) -> Option<RepeatMapping> {
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

/// Tri-state result of [`pg_next_dst_boundary_tristate`], preserving the full
/// `res < 0` / `res == 0` / `res > 0` distinction returned by C's
/// `pg_next_dst_boundary`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NextDstBoundary {
    /// C `res < 0`: extrapolation failed; the result is undefined. (Cannot
    /// happen with well-formed zone data.)
    Overflow,
    /// C `res == 0`: no DST transition applies; only the "before"
    /// offset/isdst are meaningful.
    NoTransition {
        before_gmtoff: i64,
        before_isdst: i32,
    },
    /// C `res > 0`: a DST boundary was found.
    Boundary(DstBoundary),
}

fn next_dst_boundary_impl(timep: pg_time_t, sp: &state) -> NextDstBoundary {
    if sp.timecnt == 0 {
        // Non-DST zone: use the defaulttype.
        let tt = sp.ttis[sp.defaulttype as usize];
        return NextDstBoundary::NoTransition {
            before_gmtoff: tt.tt_utoff as i64,
            before_isdst: tt.tt_isdst as i32,
        };
    }
    let timecnt = sp.timecnt as usize;

    if (sp.goback && timep < sp.ats[0]) || (sp.goahead && timep > sp.ats[timecnt - 1]) {
        // For values outside the transition table, extrapolate.
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
        // No known transition > t, so use the last known segment's type.
        let tt = sp.ttis[sp.types[timecnt - 1] as usize];
        return NextDstBoundary::NoTransition {
            before_gmtoff: tt.tt_utoff as i64,
            before_isdst: tt.tt_isdst as i32,
        };
    }

    if timep < sp.ats[0] {
        // For "before", use the defaulttype; for "after", the first segment.
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

    // Search to find the boundary following t.
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

/// Find the next DST boundary at or after `timep`, returning `None` when no
/// DST transition applies or when extrapolation failed. Use
/// [`pg_next_dst_boundary_tristate`] to distinguish those two cases.
pub fn pg_next_dst_boundary(timep: pg_time_t, tz: &pg_tz) -> Option<DstBoundary> {
    match next_dst_boundary_impl(timep, tz.state()) {
        NextDstBoundary::Boundary(b) => Some(b),
        _ => None,
    }
}

/// Tri-state wrapper around C's `pg_next_dst_boundary` (see
/// [`NextDstBoundary`]).
pub fn pg_next_dst_boundary_tristate(timep: pg_time_t, tz: &pg_tz) -> NextDstBoundary {
    next_dst_boundary_impl(timep, tz.state())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimezoneAbbrev {
    pub gmtoff: i64,
    pub isdst: i32,
}

/// Identify a timezone abbreviation's meaning in the given zone: the meaning
/// in use at or most recently before `timep`, or the meaning in first use
/// after that time if the abbrev was never used before it. Returns `None` if
/// the abbreviation was never used at all in this zone.
///
/// Note: abbrev is matched case-sensitively; it should be all-upper-case.
pub fn pg_interpret_timezone_abbrev(
    abbrev: &str,
    timep: pg_time_t,
    tz: &pg_tz,
) -> Option<TimezoneAbbrev> {
    let sp = tz.state();
    let abbrind = find_abbrev(sp, abbrev)?;

    // Unlike pg_next_dst_boundary, we needn't sweat about extrapolation
    // (goback/goahead zones): finding the newest or oldest meaning of the
    // abbreviation gets us what we want, since extrapolation would just be
    // repeating the newest or oldest meanings.
    //
    // Find the first transition > the cutoff time (timecnt can be zero, in
    // which case only the defaulttype entry is checked).
    let cutoff = sp.ats[..sp.timecnt as usize].partition_point(|&at| at <= timep);

    // Scan backwards for the latest interval using the abbrev before cutoff.
    for i in (0..cutoff).rev() {
        let tt = sp.ttis[sp.types[i] as usize];
        if tt.tt_desigidx == abbrind {
            return Some(TimezoneAbbrev {
                gmtoff: tt.tt_utoff as i64,
                isdst: tt.tt_isdst as i32,
            });
        }
    }

    // Check the defaulttype, notionally the era before any of the entries.
    let default_tt = sp.ttis[sp.defaulttype as usize];
    if default_tt.tt_desigidx == abbrind {
        return Some(TimezoneAbbrev {
            gmtoff: default_tt.tt_utoff as i64,
            isdst: default_tt.tt_isdst as i32,
        });
    }

    // Not there, so scan forwards for the first one after the cutoff.
    for i in cutoff..sp.timecnt as usize {
        let tt = sp.ttis[sp.types[i] as usize];
        if tt.tt_desigidx == abbrind {
            return Some(TimezoneAbbrev {
                gmtoff: tt.tt_utoff as i64,
                isdst: tt.tt_isdst as i32,
            });
        }
    }

    None /* hm, not actually used in any interval? */
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KnownTimezoneAbbrev {
    pub isfixed: bool,
    pub gmtoff: i64,
    pub isdst: i32,
}

/// Detect whether a timezone abbreviation is defined within the given zone,
/// and if so whether it has one meaning (`isfixed`, with that meaning's
/// gmtoff/isdst) or several (`!isfixed`; gmtoff/isdst are the first use, as
/// in C, where they are simply the last values stored). Returns `None` if
/// the abbreviation is unknown in this zone.
///
/// Note: abbrev is matched case-sensitively; it should be all-upper-case.
pub fn pg_timezone_abbrev_is_known(abbrev: &str, tz: &pg_tz) -> Option<KnownTimezoneAbbrev> {
    let sp = tz.state();
    let abbrind = find_abbrev(sp, abbrev)?;
    let mut found: Option<KnownTimezoneAbbrev> = None;

    for tt in &sp.ttis[..sp.typecnt as usize] {
        if tt.tt_desigidx != abbrind {
            continue;
        }
        match &mut found {
            None => {
                // First usage.
                found = Some(KnownTimezoneAbbrev {
                    isfixed: true, /* for the moment */
                    gmtoff: tt.tt_utoff as i64,
                    isdst: tt.tt_isdst as i32,
                });
            }
            Some(first) => {
                // Second or later usage, does it match?
                if first.gmtoff != tt.tt_utoff as i64 || first.isdst != tt.tt_isdst as i32 {
                    first.isfixed = false;
                    break; /* no point in looking further */
                }
            }
        }
    }

    found
}

/// Iteratively fetch all the abbreviations used in the given time zone.
/// `index` is a state counter the caller initializes to zero before the
/// first call and does not touch between calls. Returns `None` when there
/// are no more abbreviations.
pub fn pg_get_next_timezone_abbrev<'tz>(index: &mut i32, tz: &'tz pg_tz) -> Option<&'tz str> {
    let sp = tz.state();
    let start = usize::try_from(*index).ok()?;
    if start >= sp.charcnt as usize {
        return None;
    }
    let end = start + sp.chars[start..].iter().position(|&byte| byte == 0)?;
    // Advance past this abbrev and its trailing NUL.
    *index = (end + 1) as i32;
    std::str::from_utf8(&sp.chars[start..end]).ok()
}

/// If the given timezone uses only one GMT offset, return it, else `None`.
/// (The zone could have more than one ttinfo if it historically used more
/// than one abbreviation; we succeed as long as they all share one gmtoff.)
pub fn pg_get_timezone_offset(tz: &pg_tz) -> Option<i64> {
    let sp = tz.state();
    let first = sp.ttis[0].tt_utoff;
    sp.ttis[..sp.typecnt as usize]
        .iter()
        .all(|tt| tt.tt_utoff == first)
        .then_some(first as i64)
}

/// Return the name of the timezone (C `pg_get_timezone_name`).
pub fn pg_get_timezone_name(tz: &pg_tz) -> &str {
    tz.name()
}

/// Check whether timezone is acceptable: reject leap-second-aware
/// timekeeping, which would wreak havoc with our date/time arithmetic. Runs
/// pg_localtime for what should be GMT midnight 2000-01-01 and insists the
/// tm_sec value be zero; any other result has to be due to leap seconds.
pub fn pg_tz_acceptable(tz: &pg_tz) -> bool {
    let time2000 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECSPERDAY;
    pg_localtime(time2000, tz).is_some_and(|tm| tm.tm_sec == 0)
}

// ---------------------------------------------------------------------------
// TZif parsing (C tzloadbody)

fn parse_tzif(bytes: &[u8], sp: &mut state, doextend: bool) -> Result<(), TzLoadError> {
    // C tzloadbody parses the 32-bit block first (validating its header and
    // counts), then — when the file's version is nonzero — re-parses the
    // 64-bit block over it. Errors in either block reject the file. There is
    // no magic-bytes check, matching C.
    let first = TzifHeader::parse_at(bytes, 0).ok_or(TzLoadError::Invalid)?;
    let mut cursor = first.data_start;
    parse_tzif_block(bytes, &first, 4, &mut cursor, sp)?;

    // C's loop breaks (before its memmove) when a parsed header's version
    // byte is '\0'; the footer check then runs against whatever the buffer
    // happens to hold. The candidate footer region is therefore: the whole
    // file when the first header's version is 0; the bytes from the second
    // header onward when the second header's version is 0; otherwise the
    // bytes after the 64-bit block. (There is no magic check, so a crafted
    // version-0 file starting and ending with '\n' does reach tzparse in C.)
    let footer_start = if first.version == 0 {
        0
    } else {
        let second_start = cursor;
        let second = TzifHeader::parse_at(bytes, cursor).ok_or(TzLoadError::Invalid)?;
        cursor = second.data_start;
        parse_tzif_block(bytes, &second, 8, &mut cursor, sp)?;
        if second.version == 0 {
            second_start
        } else {
            cursor
        }
    };

    if doextend && sp.typecnt as usize + 2 <= TZ_MAX_TYPES {
        if let Some(posix) = parse_footer_posix(bytes, footer_start) {
            let mut extended = state::default();
            if tzparse(posix, &mut extended, false) {
                extend_with_posix(sp, &mut extended);
            }
        }
    }

    if sp.typecnt == 0 {
        return Err(TzLoadError::Invalid);
    }

    // Set sp->goback / sp->goahead by detecting SECSPERREPEAT-spaced
    // equivalent transitions at the start/end of the table.
    if sp.timecnt > 1 {
        let timecnt = sp.timecnt as usize;
        for i in 1..timecnt {
            if typesequiv(sp, sp.types[i] as i32, sp.types[0] as i32)
                && differ_by_repeat(sp.ats[i], sp.ats[0])
            {
                sp.goback = true;
                break;
            }
        }
        for i in (0..=timecnt - 2).rev() {
            if typesequiv(sp, sp.types[timecnt - 1] as i32, sp.types[i] as i32)
                && differ_by_repeat(sp.ats[timecnt - 1], sp.ats[i])
            {
                sp.goahead = true;
                break;
            }
        }
    }

    set_default_type(sp);
    Ok(())
}

/// True when two transition types are equivalent: same offset, isdst, std/ut
/// flags, and abbreviation (C `typesequiv`).
fn typesequiv(sp: &state, a: i32, b: i32) -> bool {
    let typecnt = sp.typecnt;
    if a < 0 || a >= typecnt || b < 0 || b >= typecnt {
        return false;
    }
    let ap = &sp.ttis[a as usize];
    let bp = &sp.ttis[b as usize];
    ap.tt_utoff == bp.tt_utoff
        && ap.tt_isdst == bp.tt_isdst
        && ap.tt_ttisstd == bp.tt_ttisstd
        && ap.tt_ttisut == bp.tt_ttisut
        && cstr_bytes(&sp.chars, ap.tt_desigidx as usize)
            == cstr_bytes(&sp.chars, bp.tt_desigidx as usize)
}

/// True when `t1 - t0 == SECSPERREPEAT`, accounting for pg_time_t's bit width
/// (C `differ_by_repeat`).
fn differ_by_repeat(t1: pg_time_t, t0: pg_time_t) -> bool {
    // TYPE_BIT(pg_time_t) - TYPE_SIGNED(pg_time_t) == 64 - 1 == 63 >= 34, so
    // the early-return-0 branch never fires for i64.
    if (pg_time_t::BITS - 1) < SECSPERREPEAT_BITS {
        return false;
    }
    t1.checked_sub(t0) == Some(SECSPERREPEAT)
}

#[derive(Clone, Copy)]
struct TzifHeader {
    version: u8,
    ttisutcnt: usize,
    ttisstdcnt: usize,
    leapcnt: usize,
    timecnt: usize,
    typecnt: usize,
    charcnt: usize,
    data_start: usize,
}

impl TzifHeader {
    /// Read a `struct tzhead` at `offset`. The counts are read as int32 by
    /// C's detzcode; negative values become huge `usize`s here and then fail
    /// the strict upper-bound checks, matching C's `0 <= cnt` tests.
    fn parse_at(bytes: &[u8], offset: usize) -> Option<Self> {
        let header = bytes.get(offset..offset.checked_add(44)?)?;
        Some(Self {
            version: header[4],
            ttisutcnt: read_be_i32(&header[20..24])? as usize,
            ttisstdcnt: read_be_i32(&header[24..28])? as usize,
            leapcnt: read_be_i32(&header[28..32])? as usize,
            timecnt: read_be_i32(&header[32..36])? as usize,
            typecnt: read_be_i32(&header[36..40])? as usize,
            charcnt: read_be_i32(&header[40..44])? as usize,
            data_start: offset + 44,
        })
    }
}

// The `at <= TIME_T_MAX` / `at < TIME_T_MIN` / `tr <= TIME_T_MAX` comparisons
// deliberately transcribe C's range checks, which are vacuous when pg_time_t
// is the full i64 range (as here) but kept for fidelity.
#[allow(clippy::absurd_extreme_comparisons)]
fn parse_tzif_block(
    bytes: &[u8],
    header: &TzifHeader,
    time_size: usize,
    cursor: &mut usize,
    sp: &mut state,
) -> Result<(), TzLoadError> {
    // Header validation (localtime.c:264-280). ttisstdcnt and ttisutcnt must
    // each equal typecnt or be zero. Although tzfile(5) currently requires
    // typecnt to be nonzero, support future formats that may allow zero
    // typecnt in files that have a TZ string and no transitions (the
    // typecnt==0 rejection happens after the footer graft).
    if !(header.leapcnt < TZ_MAX_LEAPS
        && header.typecnt < TZ_MAX_TYPES
        && header.timecnt < TZ_MAX_TIMES
        && header.charcnt < TZ_MAX_CHARS
        && (header.ttisstdcnt == header.typecnt || header.ttisstdcnt == 0)
        && (header.ttisutcnt == header.typecnt || header.ttisutcnt == 0))
    {
        return Err(TzLoadError::Invalid);
    }

    *sp = state::default();
    sp.leapcnt = header.leapcnt as i32;
    sp.timecnt = header.timecnt as i32;
    sp.typecnt = header.typecnt as i32;
    sp.charcnt = header.charcnt as i32;

    // Read transitions, discarding those out of pg_time_t range. But pretend
    // the last transition before TIME_T_MIN occurred at TIME_T_MIN. Reject
    // strictly-decreasing transition times.
    //
    // `keep[i]` records C's `sp->types[i] = at <= TIME_T_MAX` flag (which the
    // C code stores transiently in sp->types); it filters the parsed type
    // indices in the following loop.
    let mut keep = [false; TZ_MAX_TIMES];
    let mut timecnt = 0usize;
    for i in 0..header.timecnt {
        let raw = take(bytes, cursor, time_size)?;
        let at: i64 = if time_size == 8 {
            read_be_i64(raw).ok_or(TzLoadError::Invalid)?
        } else {
            read_be_i32(raw).ok_or(TzLoadError::Invalid)? as i64
        };
        keep[i] = at <= TIME_T_MAX;
        if keep[i] {
            let attime = if at < TIME_T_MIN { TIME_T_MIN } else { at };
            if timecnt != 0 && attime <= sp.ats[timecnt - 1] {
                if attime < sp.ats[timecnt - 1] {
                    return Err(TzLoadError::Invalid);
                }
                // Duplicate transition time: drop the previous one and the
                // raw index that referenced it.
                keep[i - 1] = false;
                timecnt -= 1;
            }
            sp.ats[timecnt] = attime;
            timecnt += 1;
        }
    }

    // Read the per-transition type indices, keeping only those whose
    // transition survived the range/dedup pass.
    let types = take(bytes, cursor, header.timecnt)?;
    timecnt = 0;
    for (i, typ) in types.iter().copied().enumerate() {
        if typ as usize >= header.typecnt {
            return Err(TzLoadError::Invalid);
        }
        if keep[i] {
            sp.types[timecnt] = typ;
            timecnt += 1;
        }
    }
    sp.timecnt = timecnt as i32;

    for i in 0..header.typecnt {
        let raw = take(bytes, cursor, 6)?;
        let tt_utoff = read_be_i32(&raw[0..4]).ok_or(TzLoadError::Invalid)?;
        let isdst = raw[4];
        if isdst >= 2 {
            return Err(TzLoadError::Invalid);
        }
        let desigidx = raw[5];
        if desigidx as usize >= header.charcnt {
            return Err(TzLoadError::Invalid);
        }
        sp.ttis[i] = ttinfo {
            tt_utoff,
            tt_isdst: isdst != 0,
            tt_desigidx: desigidx as i32,
            tt_ttisstd: false,
            tt_ttisut: false,
        };
    }

    let chars = take(bytes, cursor, header.charcnt)?;
    sp.chars[..header.charcnt].copy_from_slice(chars);
    sp.chars[header.charcnt] = 0; // ensure '\0' at end

    // Read leap seconds, discarding those out of pg_time_t range, and
    // validate the spacing/correction invariants.
    let mut prevtr: i64 = 0;
    let mut prevcorr: i32 = 0;
    let mut leapcnt = 0usize;
    for _ in 0..header.leapcnt {
        let tr: i64 = if time_size == 8 {
            read_be_i64(take(bytes, cursor, 8)?).ok_or(TzLoadError::Invalid)?
        } else {
            read_be_i32(take(bytes, cursor, 4)?).ok_or(TzLoadError::Invalid)? as i64
        };
        let corr = read_be_i32(take(bytes, cursor, 4)?).ok_or(TzLoadError::Invalid)?;
        // Leap seconds cannot occur before the Epoch.
        if tr < 0 {
            return Err(TzLoadError::Invalid);
        }
        if tr <= TIME_T_MAX {
            // Leap seconds cannot occur more than once per UTC month, and
            // UTC months are at least 28 days long (minus 1 second for a
            // negative leap second). Each leap second's correction must
            // differ from the previous one's by 1 second.
            if tr - prevtr < 28 * SECSPERDAY - 1 || (corr != prevcorr - 1 && corr != prevcorr + 1)
            {
                return Err(TzLoadError::Invalid);
            }
            prevtr = tr;
            prevcorr = corr;
            sp.lsis[leapcnt] = lsinfo {
                ls_trans: tr,
                ls_corr: corr as i64,
            };
            leapcnt += 1;
        }
    }
    sp.leapcnt = leapcnt as i32;

    // tt_ttisstd flags.
    for i in 0..header.typecnt {
        if header.ttisstdcnt == 0 {
            sp.ttis[i].tt_ttisstd = false;
        } else {
            let byte = take(bytes, cursor, 1)?[0];
            if byte != 1 && byte != 0 {
                return Err(TzLoadError::Invalid);
            }
            sp.ttis[i].tt_ttisstd = byte != 0;
        }
    }

    // tt_ttisut flags.
    for i in 0..header.typecnt {
        if header.ttisutcnt == 0 {
            sp.ttis[i].tt_ttisut = false;
        } else {
            let byte = take(bytes, cursor, 1)?[0];
            if byte != 1 && byte != 0 {
                return Err(TzLoadError::Invalid);
            }
            sp.ttis[i].tt_ttisut = byte != 0;
        }
    }

    Ok(())
}

/// The footer shape check of C tzloadbody: the remaining bytes must be more
/// than two, start with '\n', and end with '\n' (the end of the file). C
/// overwrites the final '\n' with '\0' and passes `&buf[1]` as a C string, so
/// the TZ string runs to the first NUL (or to that final newline).
fn parse_footer_posix(bytes: &[u8], start: usize) -> Option<&str> {
    let footer = bytes.get(start..)?;
    if footer.len() <= 2 || footer[0] != b'\n' || footer[footer.len() - 1] != b'\n' {
        return None;
    }
    let tz = &footer[1..footer.len() - 1];
    let tz = match tz.iter().position(|&byte| byte == 0) {
        Some(nul) => &tz[..nul],
        None => tz,
    };
    std::str::from_utf8(tz).ok()
}

/// The `doextend` graft of C tzloadbody (localtime.c:415-491): splice the
/// POSIX-footer-derived transitions in `ts` onto `sp`, reusing existing
/// abbreviations where possible and rewriting the grafted type indices.
/// No change unless every type in `ts` could be matched to an abbreviation
/// slot (C's `gotabbr == ts->typecnt` gate).
fn extend_with_posix(sp: &mut state, ts: &mut state) {
    // Caller (parse_tzif) has already checked sp->typecnt + 2 <= TZ_MAX_TYPES.

    // Attempt to reuse existing abbreviations. Without this,
    // America/Anchorage would be right on the edge after 2037 when
    // TZ_MAX_CHARS is 50, as sp->charcnt equals 40 (for LMT AST AWT APT AHST
    // AHDT YST AKDT AKST) and ts->charcnt equals 10 (for AKST AKDT). Reusing
    // means sp->charcnt can stay 40 in this example.
    let mut gotabbr = 0usize;
    let mut charcnt = sp.charcnt as usize;
    for i in 0..ts.typecnt as usize {
        let tsabbr = cstr_bytes(&ts.chars, ts.ttis[i].tt_desigidx as usize);
        // Search for a matching NUL-terminated string at *every* byte offset
        // j in [0, charcnt) — C's loop steps j by 1, so a suffix of an
        // existing abbreviation (e.g. "ST" inside "AKST\0") also matches.
        let mut matched = None;
        let mut j = 0usize;
        while j < charcnt {
            if cstr_bytes(&sp.chars, j) == tsabbr {
                matched = Some(j);
                break;
            }
            j += 1;
        }
        if let Some(j) = matched {
            ts.ttis[i].tt_desigidx = j as i32;
            gotabbr += 1;
        } else {
            // `j` now equals charcnt (the append point). Append if it fits.
            let tsabbrlen = tsabbr.len();
            if j + tsabbrlen < TZ_MAX_CHARS {
                sp.chars[j..j + tsabbrlen].copy_from_slice(tsabbr);
                sp.chars[j + tsabbrlen] = 0;
                charcnt = j + tsabbrlen + 1;
                ts.ttis[i].tt_desigidx = j as i32;
                gotabbr += 1;
            }
        }
    }

    if gotabbr != ts.typecnt as usize {
        return;
    }
    sp.charcnt = charcnt as i32;

    // Ignore any trailing, no-op transitions generated by zic; they don't
    // help here and can run afoul of bugs in zic 2016j or earlier.
    while sp.timecnt > 1 && sp.types[sp.timecnt as usize - 1] == sp.types[sp.timecnt as usize - 2]
    {
        sp.timecnt -= 1;
    }

    // Find the first POSIX transition strictly later than the last stored
    // one (comparing in leap-corrected coordinates), then graft the
    // remainder. The grafted type index is `sp->typecnt + ts->types[i]`,
    // pointing into the ts ttis appended below.
    let mut i = 0usize;
    while i < ts.timecnt as usize {
        let corrected = ts.ats[i] + leapcorr(sp, ts.ats[i]);
        if sp.timecnt == 0 || sp.ats[sp.timecnt as usize - 1] < corrected {
            break;
        }
        i += 1;
    }
    while i < ts.timecnt as usize && (sp.timecnt as usize) < TZ_MAX_TIMES {
        let idx = sp.timecnt as usize;
        sp.ats[idx] = ts.ats[i] + leapcorr(sp, ts.ats[i]);
        sp.types[idx] = sp.typecnt as u8 + ts.types[i];
        sp.timecnt += 1;
        i += 1;
    }
    for i in 0..ts.typecnt as usize {
        sp.ttis[sp.typecnt as usize] = ts.ttis[i];
        sp.typecnt += 1;
    }
}

/// Total leap-second correction in effect at `t` (C `leapcorr`): the
/// correction of the latest leap entry whose transition is at or before `t`,
/// else 0.
fn leapcorr(sp: &state, t: pg_time_t) -> i64 {
    for i in (0..sp.leapcnt as usize).rev() {
        if t >= sp.lsis[i].ls_trans {
            return sp.lsis[i].ls_corr;
        }
    }
    0
}

/// Infer `sp.defaulttype` from the data (trailing block of C tzloadbody).
/// Although this default type is always zero for data from recent tzdb
/// releases, 2018e-or-earlier data needs the heuristics below (e.g. zones
/// like Australia/Macquarie in 32-bit data from tzdb 2013c or earlier, and
/// EST5EDT-like zones whose first transition is to DST).
fn set_default_type(sp: &mut state) {
    let timecnt = sp.timecnt as usize;
    let typecnt = sp.typecnt;

    // If type 0 is unused in transitions, it's the type to use for early
    // times.
    let mut i: i32 = if sp.types[..timecnt].iter().any(|&t| t == 0) {
        -1
    } else {
        0
    };

    // Absent that, if there are transition times and the first transition is
    // to a daylight time, find the standard type less than and closest to
    // the type of the first transition.
    if i < 0 && timecnt > 0 && sp.ttis[sp.types[0] as usize].tt_isdst {
        i = sp.types[0] as i32;
        loop {
            i -= 1;
            if i < 0 {
                break;
            }
            if !sp.ttis[i as usize].tt_isdst {
                break;
            }
        }
    }

    // If no result yet, find the first standard type; if there is none, punt
    // to type zero.
    if i < 0 {
        i = 0;
        while sp.ttis[i as usize].tt_isdst {
            i += 1;
            if i >= typecnt {
                i = 0;
                break;
            }
        }
    }

    sp.defaulttype = i;
}

// ---------------------------------------------------------------------------
// POSIX TZ string parsing (C getzname/getqzname/getnum/getsecs/getoffset/
// getrule) and rule expansion (C transtime + the tzparse rule loop)

/// C getzname/getqzname: split a zone abbreviation off the front of a POSIX
/// TZ string. A `<`-quoted name runs to the matching `>` (missing `>` is an
/// error); an unquoted name runs until a digit, `,`, `-`, or `+`. Either
/// form may be empty (the caller rejects empty DST names; empty STD names
/// are allowed, unlike IANA).
fn parse_zone_name(input: &str) -> Option<(&str, &str)> {
    if let Some(rest) = input.strip_prefix('<') {
        let end = rest.find('>')?;
        Some((&rest[..end], &rest[end + 1..]))
    } else {
        let end = input
            .find(|ch: char| ch == '+' || ch == '-' || ch == ',' || ch.is_ascii_digit())
            .unwrap_or(input.len());
        Some((&input[..end], &input[end..]))
    }
}

/// Extract a number from `input`, requiring it to fall in `[min, max]`
/// (C `getnum`): the first byte must be a digit, the running value is
/// rejected the moment it exceeds `max`, and the final value is rejected if
/// it is below `min`. There is no digit-count limit.
fn getnum(input: &str, min: i32, max: i32) -> Option<(i32, &str)> {
    let bytes = input.as_bytes();
    if bytes.first().copied().filter(u8::is_ascii_digit).is_none() {
        return None;
    }
    let mut num: i32 = 0;
    let mut idx = 0usize;
    while let Some(&c) = bytes.get(idx).filter(|c| c.is_ascii_digit()) {
        num = num.checked_mul(10)?.checked_add((c - b'0') as i32)?;
        if num > max {
            return None;
        }
        idx += 1;
    }
    if num < min {
        return None;
    }
    Some((num, &input[idx..]))
}

/// Extract a number of seconds in `hh[:mm[:ss]]` form (C `getsecs`): hours
/// in [0,167] (`HOURSPERDAY * DAYSPERWEEK - 1` allows quasi-POSIX rules like
/// "M10.4.6/26"), minutes in [0,59], seconds in [0,60] (allowing leap
/// seconds).
fn getsecs(input: &str) -> Option<(i32, &str)> {
    let (num, rest) = getnum(input, 0, (HOURSPERDAY * DAYSPERWEEK as i64 - 1) as i32)?;
    let mut secs = num.checked_mul(SECSPERHOUR as i32)?;
    let mut rest = rest;
    if let Some(after_colon) = rest.strip_prefix(':') {
        let (num, after_minutes) = getnum(after_colon, 0, (MINSPERHOUR - 1) as i32)?;
        secs = secs.checked_add(num.checked_mul(SECSPERMIN as i32)?)?;
        rest = after_minutes;
        if let Some(after_colon) = rest.strip_prefix(':') {
            // SECSPERMIN (60) allows for leap seconds.
            let (num, after_seconds) = getnum(after_colon, 0, SECSPERMIN as i32)?;
            secs = secs.checked_add(num)?;
            rest = after_seconds;
        }
    }
    Some((secs, rest))
}

/// Extract an offset in `[+-]hh[:mm[:ss]]` form (C `getoffset`).
fn parse_offset(input: &str) -> Option<(i32, &str)> {
    let (neg, rest) = match input.as_bytes().first().copied() {
        Some(b'-') => (true, &input[1..]),
        Some(b'+') => (false, &input[1..]),
        _ => (false, input),
    };
    let (secs, rest) = getsecs(rest)?;
    Some((if neg { secs.checked_neg()? } else { secs }, rest))
}

#[derive(Clone, Copy)]
enum Rule {
    /// Jn: Julian day, leap day never counted.
    JulianNoLeap { day: i32 },
    /// n: zero-based day of year, leap day counted.
    ZeroBasedJulian { day: i32 },
    /// Mm.w.d: month, week, day-of-week.
    MonthWeekDay { month: i32, week: i32, day: i32 },
}

#[derive(Clone, Copy)]
struct TransitionRule {
    rule: Rule,
    /// Transition time of day in seconds (default 2:00:00).
    time: i32,
}

/// Extract a `date[/time]` rule (C `getrule`).
fn parse_rule(input: &str) -> Option<(TransitionRule, &str)> {
    let (rule, rest) = if let Some(rest) = input.strip_prefix('J') {
        // Julian day: 1 .. DAYSPERNYEAR.
        let (day, rest) = getnum(rest, 1, DAYSPERNYEAR)?;
        (Rule::JulianNoLeap { day }, rest)
    } else if let Some(rest) = input.strip_prefix('M') {
        // Month, week, day.
        let (month, rest) = getnum(rest, 1, MONSPERYEAR as i32)?;
        let rest = rest.strip_prefix('.')?;
        let (week, rest) = getnum(rest, 1, 5)?;
        let rest = rest.strip_prefix('.')?;
        let (day, rest) = getnum(rest, 0, DAYSPERWEEK - 1)?;
        (Rule::MonthWeekDay { month, week, day }, rest)
    } else if input.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        // Day of year: 0 .. DAYSPERLYEAR - 1.
        let (day, rest) = getnum(input, 0, DAYSPERLYEAR - 1)?;
        (Rule::ZeroBasedJulian { day }, rest)
    } else {
        return None; // invalid format
    };

    let (time, rest) = if let Some(rest) = rest.strip_prefix('/') {
        parse_offset(rest)?
    } else {
        (2 * SECSPERHOUR as i32, rest) // default = 2:00:00
    };
    Some((TransitionRule { rule, time }, rest))
}

/// Generate two transitions per year for a POSIX rule, exactly as in C
/// tzparse's rule loop: iterate years from `EPOCH_YEAR` (with a bounded
/// look-back so the table covers `goback`), emit only the transitions whose
/// year-pair forms a valid DST window, and guard every accumulation against
/// `pg_time_t` overflow. Sets typecnt/timecnt/goback/goahead and installs
/// the two ttis; an empty table means perpetual DST and collapses to one
/// (DST) type.
fn build_posix_transitions(
    sp: &mut state,
    stdoffset: i32,
    dstoffset: i32,
    std_desigidx: i32,
    dst_desigidx: i32,
    start: TransitionRule,
    end: TransitionRule,
) {
    sp.typecnt = 2; // standard time and DST
    sp.ttis[0] = ttinfo {
        tt_utoff: -stdoffset,
        tt_isdst: false,
        tt_desigidx: std_desigidx,
        tt_ttisstd: false,
        tt_ttisut: false,
    };
    sp.ttis[1] = ttinfo {
        tt_utoff: -dstoffset,
        tt_isdst: true,
        tt_desigidx: dst_desigidx,
        tt_ttisstd: false,
        tt_ttisut: false,
    };
    sp.defaulttype = 0;

    let mut timecnt = 0usize;
    let mut janfirst: pg_time_t = 0;
    let mut janoffset: i32 = 0;
    let mut yearbeg = EPOCH_YEAR;

    // Walk janfirst back to the start of the look-back window, tracking the
    // residual offset that doesn't fit in pg_time_t (janoffset). `yearsecs`
    // fits in int32 (<= 366 * SECSPERDAY), matching C's int32 type.
    loop {
        let yearsecs = year_lengths(is_leap(yearbeg - 1)) as i64 * SECSPERDAY;
        yearbeg -= 1;
        if increment_overflow_time(&mut janfirst, -yearsecs) {
            janoffset = -(yearsecs as i32);
            break;
        }
        if EPOCH_YEAR - YEARSPERREPEAT / 2 >= yearbeg {
            break;
        }
    }

    let mut yearlim = yearbeg + YEARSPERREPEAT + 1;
    let mut year = yearbeg;
    while year < yearlim {
        let mut starttime = transtime(year, start, stdoffset);
        let mut endtime = transtime(year, end, dstoffset);
        let yearsecs = year_lengths(is_leap(year)) as i64 * SECSPERDAY;
        let reversed = endtime < starttime;
        if reversed {
            std::mem::swap(&mut starttime, &mut endtime);
        }
        if reversed
            || (starttime < endtime
                && (endtime - starttime < (yearsecs + (stdoffset as i64 - dstoffset as i64))))
        {
            if TZ_MAX_TIMES - 2 < timecnt {
                break;
            }
            sp.ats[timecnt] = janfirst;
            if !increment_overflow_time(&mut sp.ats[timecnt], janoffset as i64 + starttime) {
                sp.types[timecnt] = (!reversed) as u8;
                timecnt += 1;
            }
            sp.ats[timecnt] = janfirst;
            if !increment_overflow_time(&mut sp.ats[timecnt], janoffset as i64 + endtime) {
                sp.types[timecnt] = reversed as u8;
                timecnt += 1;
                yearlim = year + YEARSPERREPEAT + 1;
            }
        }
        if increment_overflow_time(&mut janfirst, janoffset as i64 + yearsecs) {
            break;
        }
        janoffset = 0;
        year += 1;
    }

    sp.timecnt = timecnt as i32;
    if timecnt == 0 {
        // Perpetual DST: collapse to a single (DST) type.
        sp.ttis[0] = sp.ttis[1];
        sp.typecnt = 1;
    } else if YEARSPERREPEAT < year - yearbeg {
        sp.goback = true;
        sp.goahead = true;
    }
}

/// Year-relative transition time for `rule` in `year`, applied at UT offset
/// `offset` (C `transtime`, including the Zeller's-congruence
/// month/week/day computation).
fn transtime(year: i32, rule: TransitionRule, offset: i32) -> i64 {
    let leapyear = is_leap(year);
    let value: i64 = match rule.rule {
        Rule::JulianNoLeap { day } => {
            // Jn — 1 == Jan 1, 60 == Mar 1 even in leap years.
            let mut v = (day as i64 - 1) * SECSPERDAY;
            if leapyear && day >= 60 {
                v += SECSPERDAY;
            }
            v
        }
        Rule::ZeroBasedJulian { day } => day as i64 * SECSPERDAY,
        Rule::MonthWeekDay { month, week, day } => {
            // Zeller's Congruence: day-of-week of the first day of month.
            let m1 = (month + 9) % 12 + 1;
            let yy0 = if month <= 2 { year - 1 } else { year };
            let yy1 = yy0 / 100;
            let yy2 = yy0 % 100;
            let mut dow = ((26 * m1 - 2) / 10 + 1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
            if dow < 0 {
                dow += DAYSPERWEEK;
            }
            // day-of-month (zero-origin) of the first `day`-weekday of the
            // month, then advance by week-1 weeks (capped at month end).
            let mut d = day - dow;
            if d < 0 {
                d += DAYSPERWEEK;
            }
            for _ in 1..week {
                if d + DAYSPERWEEK >= mon_lengths(leapyear, month as usize - 1) {
                    break;
                }
                d += DAYSPERWEEK;
            }
            let mut v = d as i64 * SECSPERDAY;
            for i in 0..month as usize - 1 {
                v += mon_lengths(leapyear, i) as i64 * SECSPERDAY;
            }
            v
        }
    };
    // "value" is the year-relative time of 00:00:00 UT on the chosen day; add
    // the transition time and the offset to get the year-relative time of
    // the transition expressed in local wall-clock time before the change.
    value + rule.time as i64 + offset as i64
}

fn mon_lengths(leap: bool, month: usize) -> i32 {
    const MON_LENGTHS: [[i32; MONSPERYEAR]; 2] = [
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    ];
    MON_LENGTHS[leap as usize][month]
}

fn year_lengths(leap: bool) -> i32 {
    if leap {
        DAYSPERLYEAR
    } else {
        DAYSPERNYEAR
    }
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0)
}

/// C `increment_overflow`: adds `j` to `*ip`, returning true (and leaving
/// `*ip` unchanged) on int overflow.
fn increment_overflow(ip: &mut i32, j: i32) -> bool {
    match ip.checked_add(j) {
        Some(v) => {
            *ip = v;
            false
        }
        None => true,
    }
}

/// C `increment_overflow_time`: adds `j` to `*tp`, returning true (no
/// change) if the result would leave `pg_time_t` range. For i64 this is
/// exactly a checked add against the full i64 range.
fn increment_overflow_time(tp: &mut pg_time_t, j: i64) -> bool {
    match tp.checked_add(j) {
        Some(v) if (TIME_T_MIN..=TIME_T_MAX).contains(&v) => {
            *tp = v;
            false
        }
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// timesub: seconds-since-epoch -> broken-down calendar time (C timesub)

/// Number of leap years through the end of the given year, where the answer
/// for year zero is defined as zero (C `leaps_thru_end_of`).
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

fn timesub(timep: pg_time_t, offset: i32, sp: Option<&state>) -> Option<pg_tm> {
    let (corr, hit) = leap_correction(sp, timep);

    let mut y: i32 = EPOCH_YEAR;
    // C division truncates toward zero; the normalization loops below handle
    // the negative remainder.
    let mut tdays: i64 = timep / SECSPERDAY;
    let mut rem: i64 = timep % SECSPERDAY;
    while tdays < 0 || tdays >= year_lengths(is_leap(y)) as i64 {
        let tdelta = tdays / DAYSPERLYEAR as i64;
        // C checks INT_MIN <= tdelta <= INT_MAX and goes out_of_range.
        let mut idelta = i32::try_from(tdelta).ok()?;
        if idelta == 0 {
            idelta = if tdays < 0 { -1 } else { 1 };
        }
        let mut newy = y;
        if increment_overflow(&mut newy, idelta) {
            return None; // out of range
        }
        // wrapping_sub: C (built with -fwrapv) wraps when newy/y is INT_MIN.
        let leapdays =
            leaps_thru_end_of(newy.wrapping_sub(1)) - leaps_thru_end_of(y.wrapping_sub(1));
        tdays -= (newy as i64 - y as i64) * DAYSPERNYEAR as i64;
        tdays -= leapdays as i64;
        y = newy;
    }

    // Given the range, we can now fearlessly cast...
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

    // The "extra" mods avoid overflow problems. wrapping_sub: with -fwrapv, C
    // wraps y - EPOCH_YEAR for y within 1970 of INT_MIN (tm_year above only
    // guarantees y >= INT_MIN + TM_YEAR_BASE).
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
    // A positive leap second requires a special representation; this uses
    // "... ??:59:60" et seq.
    let tm_sec = (rem % SECSPERMIN) as i32 + hit as i32;

    let leap = is_leap(y);
    let mut tm_mon = 0usize;
    while idays >= mon_lengths(leap, tm_mon) {
        idays -= mon_lengths(leap, tm_mon);
        tm_mon += 1;
    }

    Some(pg_tm {
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

/// The leap-second scan at the top of C timesub: total correction in effect
/// at `timep`, plus whether `timep` lands exactly on a positive leap second
/// (`hit`, which becomes second 60).
fn leap_correction(sp: Option<&state>, timep: pg_time_t) -> (i64, bool) {
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

// ---------------------------------------------------------------------------
// Abbreviation-table helpers

/// Locate `abbrev` in the zone's abbreviation list, returning its char-table
/// index. Walks abbreviation starts exactly as the C scans do.
fn find_abbrev(sp: &state, abbrev: &str) -> Option<i32> {
    let bytes = &sp.chars;
    let mut index = 0;
    while index < sp.charcnt as usize {
        let end = index + bytes[index..].iter().position(|&byte| byte == 0)?;
        if &bytes[index..end] == abbrev.as_bytes() {
            return Some(index as i32);
        }
        index = end + 1;
    }
    None
}

/// Resolve the timezone abbreviation at `index` within the state's char
/// table, returning a borrow into the table (C's `tm_zone` points at
/// `&sp->chars[ttisp->tt_desigidx]`), or the `wildabbr` sentinel when out of
/// range or not UTF-8.
fn zone_name(sp: &state, index: i32) -> Option<&str> {
    let Ok(index) = usize::try_from(index) else {
        return Some(WILDABBR);
    };
    if index >= sp.chars.len() {
        return Some(WILDABBR);
    }
    Some(cstr_str(&sp.chars, index))
}

/// The NUL-terminated abbreviation starting at `start` in a char table, as
/// raw bytes (C `&chars[start]` viewed as a C string).
fn cstr_bytes(chars: &[u8], start: usize) -> &[u8] {
    let slice = chars.get(start..).unwrap_or(&[]);
    let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
    &slice[..end]
}

/// As [`cstr_bytes`], but as `&str`, falling back to the `wildabbr` sentinel
/// for non-UTF-8 contents.
fn cstr_str(chars: &[u8], start: usize) -> &str {
    std::str::from_utf8(cstr_bytes(chars, start)).unwrap_or(WILDABBR)
}

/// Write `value` plus a trailing NUL at `offset` (C memcpy + '\0').
fn write_chars_at(dst: &mut [u8], offset: usize, value: &[u8]) {
    dst[offset..offset + value.len()].copy_from_slice(value);
    dst[offset + value.len()] = 0;
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8], TzLoadError> {
    let end = cursor.checked_add(len).ok_or(TzLoadError::Invalid)?;
    let slice = bytes.get(*cursor..end).ok_or(TzLoadError::Invalid)?;
    *cursor = end;
    Ok(slice)
}

fn read_be_i32(bytes: &[u8]) -> Option<i32> {
    Some(i32::from_be_bytes(bytes.try_into().ok()?))
}

fn read_be_i64(bytes: &[u8]) -> Option<i64> {
    Some(i64::from_be_bytes(bytes.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Install a pg_open_tzfile stub that reports "no file" — the legitimate
    /// C behavior when no timezone database is present. tzload then fails
    /// with NotFound and gmtload falls back to the lastditch tzparse.
    fn stub_open_tzfile() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| pgtz_seams::pg_open_tzfile::set(|_, _| None));
    }

    fn posix_tz(spec: &str) -> pg_tz {
        let mut sp = state::default();
        assert!(tzparse(spec, &mut sp, false), "POSIX spec must parse: {spec}");
        pg_tz::new(spec.to_owned(), sp)
    }

    #[test]
    fn gmt_lastditch_and_epoch() {
        let mut sp = state::default();
        assert!(tzparse("GMT", &mut sp, true));
        let gmt = pg_tz::new("GMT".to_owned(), sp);
        assert_eq!(pg_get_timezone_offset(&gmt), Some(0));
        assert!(pg_tz_acceptable(&gmt));

        let tm = pg_localtime(0, &gmt).expect("epoch should convert");
        assert_eq!(
            (tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, tm.tm_wday),
            (70, 0, 1, 0, 0, 0, 4)
        );
        assert_eq!(tm.tm_zone.as_deref(), Some("GMT"));
    }

    #[test]
    fn pg_gmtime_with_lastditch_gmt_state() {
        stub_open_tzfile();
        let tm = pg_gmtime(951_868_800).expect("2000-03-01 should convert");
        assert_eq!((tm.tm_year, tm.tm_mon, tm.tm_mday), (100, 2, 1));
        assert_eq!(tm.tm_zone.as_deref(), Some("GMT"));
    }

    #[test]
    fn applies_posix_dst_rules() {
        let est = posix_tz("<EST>5<EDT>");

        // 2024-01-01 05:00 UTC == 2024-01-01 00:00 EST.
        let winter = pg_localtime(1_704_085_200, &est).unwrap();
        assert_eq!((winter.tm_hour, winter.tm_isdst, winter.tm_gmtoff), (0, 0, -18_000));
        assert_eq!(winter.tm_zone.as_deref(), Some("EST"));

        // 2024-07-01 04:00 UTC == 2024-07-01 00:00 EDT.
        let summer = pg_localtime(1_719_806_400, &est).unwrap();
        assert_eq!((summer.tm_hour, summer.tm_isdst, summer.tm_gmtoff), (0, 1, -14_400));
        assert_eq!(summer.tm_zone.as_deref(), Some("EDT"));
    }

    #[test]
    fn dst_boundary_in_posix_zone() {
        let est = posix_tz("EST5EDT,M3.2.0,M11.1.0");
        // 2024-01-01 00:00 UTC; next boundary is 2024-03-10 07:00 UTC.
        match pg_next_dst_boundary_tristate(1_704_067_200, &est) {
            NextDstBoundary::Boundary(b) => {
                assert_eq!(b.boundary, 1_710_054_000);
                assert_eq!((b.before_gmtoff, b.before_isdst), (-18_000, 0));
                assert_eq!((b.after_gmtoff, b.after_isdst), (-14_400, 1));
            }
            other => panic!("expected a boundary, got {other:?}"),
        }
    }

    #[test]
    fn abbrev_lookup_in_posix_zone() {
        let est = posix_tz("EST5EDT,M3.2.0,M11.1.0");
        let edt = pg_interpret_timezone_abbrev("EDT", 1_719_806_400, &est).unwrap();
        assert_eq!((edt.gmtoff, edt.isdst), (-14_400, 1));
        assert!(pg_interpret_timezone_abbrev("XYZ", 0, &est).is_none());

        let known = pg_timezone_abbrev_is_known("EST", &est).unwrap();
        assert_eq!((known.isfixed, known.gmtoff, known.isdst), (true, -18_000, 0));

        let mut index = 0;
        assert_eq!(pg_get_next_timezone_abbrev(&mut index, &est).as_deref(), Some("EST"));
        assert_eq!(pg_get_next_timezone_abbrev(&mut index, &est).as_deref(), Some("EDT"));
        assert_eq!(pg_get_next_timezone_abbrev(&mut index, &est), None);
    }

    #[test]
    fn empty_std_abbrev_is_allowed() {
        // C allows an empty STD abbreviation (unlike IANA), both unquoted and
        // <>-quoted.
        let mut sp = state::default();
        assert!(tzparse("<>5", &mut sp, false));
        // Bare "5": getzname stops at the digit, leaving an empty STD name
        // and "5" as the offset — accepted by C.
        let mut sp = state::default();
        assert!(tzparse("5", &mut sp, false));
        // But a name with no offset at all is rejected.
        let mut sp = state::default();
        assert!(!tzparse("EST", &mut sp, false));
    }

    #[test]
    fn rejects_out_of_range_posix_offset() {
        // getsecs caps hours at HOURSPERDAY * DAYSPERWEEK - 1 == 167.
        assert!(parse_offset("167").is_some());
        assert!(parse_offset("168").is_none());
        // Minutes must be < 60, seconds <= 60.
        assert!(parse_offset("5:59").is_some());
        assert!(parse_offset("5:60").is_none());
        assert!(parse_offset("5:00:60").is_some());
        assert!(parse_offset("5:00:61").is_none());

        let mut sp = state::default();
        assert!(!tzparse("FOO168", &mut sp, false));
        let mut sp = state::default();
        assert!(tzparse("FOO167", &mut sp, false));
    }

    #[test]
    fn getnum_matches_c_incremental_range_check() {
        assert_eq!(getnum("12", 1, 12), Some((12, "")));
        assert_eq!(getnum("13", 1, 12), None); // > max
        assert_eq!(getnum("0", 1, 12), None); // < min
        assert_eq!(getnum("5x", 0, 9), Some((5, "x")));
        assert_eq!(getnum("x", 0, 9), None); // not a digit
    }

    /// Build a minimal version-1 (`'\0'`) TZif byte buffer with a single
    /// transition type, no transitions/leaps/flags, and the given `charcnt`
    /// abbreviation bytes, to exercise the header validation.
    fn synthetic_tzif_v1(charcnt: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"TZif"); // magic (unchecked, as in C)
        buf.push(0); // version '\0'
        buf.extend_from_slice(&[0u8; 15]); // reserved
        let counts: [i32; 6] = [0, 0, 0, 0, 1, charcnt as i32];
        for c in counts {
            buf.extend_from_slice(&c.to_be_bytes());
        }
        // One ttinfo: utoff=0, isdst=0, desigidx=0.
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf.push(0);
        buf.push(0);
        buf.extend(std::iter::repeat(b'A').take(charcnt));
        buf
    }

    #[test]
    fn rejects_charcnt_at_tz_max_chars() {
        // TZ_MAX_CHARS is 50 (tzfile.h:105); the header check is strict.
        let bytes = synthetic_tzif_v1(100);
        let mut sp = state::default();
        assert_eq!(parse_tzif(&bytes, &mut sp, false), Err(TzLoadError::Invalid));

        let ok = synthetic_tzif_v1(49);
        let mut sp = state::default();
        assert!(parse_tzif(&ok, &mut sp, false).is_ok());
    }

    /// C's abbreviation-reuse scan in tzloadbody steps byte-by-byte, so a
    /// footer abbreviation that is a suffix of an existing one ("KST" inside
    /// "AKST\0") is reused rather than appended.
    #[test]
    fn footer_abbrev_reuse_matches_c_suffix_scan() {
        let mut sp = state::default();
        sp.typecnt = 2;
        sp.charcnt = 10;
        sp.chars[..10].copy_from_slice(b"AKST\0AKDT\0");
        sp.ttis[1].tt_desigidx = 5;
        let mut ts = state::default();
        assert!(tzparse("KST9KDT,M3.2.0,M11.1.0", &mut ts, false));
        extend_with_posix(&mut sp, &mut ts);
        // "KST" matches at offset 1 (suffix of "AKST"), "KDT" at offset 6.
        assert_eq!(sp.charcnt, 10, "no new abbreviation bytes are appended");
        assert_eq!(sp.ttis[2].tt_desigidx, 1);
        assert_eq!(sp.ttis[3].tt_desigidx, 6);
    }

    /// The footer-graft path must reference the correctly-grafted POSIX type
    /// indices, not a hardcoded std=0/dst=1.
    #[test]
    fn extend_grafts_footer_types() {
        let mut base = state::default();
        assert!(tzparse("<STD>5", &mut base, false));
        let mut ts = state::default();
        assert!(tzparse("EST5EDT,M3.2.0,M11.1.0", &mut ts, false));
        let base_typecnt = base.typecnt;
        extend_with_posix(&mut base, &mut ts);
        assert!(base.typecnt > base_typecnt, "POSIX types must be appended");
        assert!(base.timecnt > 0, "POSIX transitions must be grafted");
        // Every grafted transition's type index points at an appended tti.
        for i in 0..base.timecnt as usize {
            assert!((base.types[i] as i32) >= base_typecnt);
        }
    }
}
