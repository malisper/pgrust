//! Row sources for the `pg_timezone_names` and `pg_timezone_abbrevs` system
//! views — the materialize-mode set-returning functions
//! `pg_timezone_names(PG_FUNCTION_ARGS)` and
//! `pg_timezone_abbrevs_zone(PG_FUNCTION_ARGS)` of `datetime.c`.
//!
//! These produce the row data (the typed `(name, abbrev, utc_offset, is_dst)` /
//! `(abbrev, utc_offset, is_dst)` tuples). The executor-frame SRF adapter
//! (`backend-executor-execSRF`) drives `InitMaterializedSRF` /
//! `materialized_srf_putvalues` over these rows, exactly as C's loop calls
//! `tuplestore_putvalues` / `SRF_RETURN_NEXT`. The `utc_offset` `Interval` is
//! built here from the GMT offset in seconds (C: `itm_in.tm_usec = (int64) off *
//! USECS_PER_SEC; itmin2interval(&itm_in, resInterval)`).

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use types_datetime::{fsec_t, pg_itm_in, Interval, USECS_PER_SEC};
use ::types_error::PgResult;
use ::pgtime::pg_tm;

use crate::convert::timestamptz_to_time_t;
use crate::interval::itmin2interval;
use crate::timestamp::timestamp2tm;
use ::transam_xact::GetCurrentTransactionStartTimestamp;

use localtime::{
    pg_get_next_timezone_abbrev, pg_get_timezone_name, pg_interpret_timezone_abbrev,
};
use timezone_pgtz::{pg_tzenumerate_next, pg_tzenumerate_start};
use ::state_pgtz::session_timezone;

/// One row of `pg_timezone_names`: `(name text, abbrev text, utc_offset
/// interval, is_dst bool)`.
pub struct TimezoneNameRow {
    pub name: String,
    pub abbrev: String,
    pub utc_offset: Interval,
    pub is_dst: bool,
}

/// One row of `pg_timezone_abbrevs` (the `_zone` half — the abbreviations the
/// IANA data for the current `session_timezone` defines): `(abbrev text,
/// utc_offset interval, is_dst bool)`.
pub struct TimezoneAbbrevRow {
    pub abbrev: String,
    pub utc_offset: Interval,
    pub is_dst: bool,
}

/// `itm_in.tm_usec = (int64) off_secs * USECS_PER_SEC; itmin2interval(&itm_in,
/// resInterval)` — build an `Interval` from a microsecond offset (the
/// `utc_offset` column). Can't overflow (a GMT offset fits comfortably).
fn interval_from_usec(usec: i64) -> Interval {
    let itm_in = pg_itm_in {
        tm_usec: usec,
        ..Default::default()
    };
    let mut span = Interval::default();
    // itmin2interval only overflows on the months field, which is always 0 here.
    let _ = itmin2interval(&itm_in, &mut span);
    span
}

/// `pg_timezone_names()` (datetime.c:5332) row source: enumerate every IANA
/// zone, convert `now()` to local time in that zone (skip on conversion
/// failure), reject ridiculously long abbreviations (> 31 chars), and emit
/// `(name, abbrev, utc_offset = -tzoff, is_dst = tm_isdst > 0)`.
pub fn pg_timezone_names_rows() -> PgResult<Vec<TimezoneNameRow>> {
    let now = GetCurrentTransactionStartTimestamp();
    let mut rows: Vec<TimezoneNameRow> = Vec::new();

    let mut tzenum = pg_tzenumerate_start()?;
    loop {
        let tz = match pg_tzenumerate_next(&mut tzenum)? {
            Some(tz) => tz,
            None => break,
        };

        // C: timestamp2tm(now, &tzoff, &tm, &fsec, &tzn, tz) != 0 => continue.
        let mut tzoff: i32 = 0;
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        let mut tzn: Option<String> = None;
        if timestamp2tm(
            now,
            Some(&mut tzoff),
            &mut tm,
            &mut fsec,
            Some(&mut tzn),
            Some(tz),
        )
        .is_err()
        {
            continue;
        }

        // C: if (tzn && strlen(tzn) > 31) continue;
        let abbrev = tzn.unwrap_or_default();
        if abbrev.len() > 31 {
            continue;
        }

        rows.push(TimezoneNameRow {
            name: pg_get_timezone_name(tz).to_string(),
            abbrev,
            // C: itm_in.tm_usec = (int64) -tzoff * USECS_PER_SEC;
            utc_offset: interval_from_usec(-(tzoff as i64) * USECS_PER_SEC),
            is_dst: tm.tm_isdst > 0,
        });
    }

    Ok(rows)
}

/// `pg_timezone_abbrevs_zone()` (datetime.c:5124) row source: walk the
/// abbreviations defined by the IANA data for the current `session_timezone`,
/// skipping non-all-alphabetic abbrevs and abbrevs not actually used in this
/// zone, emitting `(abbrev, utc_offset = gmtoff, is_dst)`.
pub fn pg_timezone_abbrevs_zone_rows() -> Vec<TimezoneAbbrevRow> {
    let now = GetCurrentTransactionStartTimestamp();
    let t = timestamptz_to_time_t(now);
    let tz = session_timezone();
    let tz_ref: &::pgtime::pg_tz = &tz;

    let mut rows: Vec<TimezoneAbbrevRow> = Vec::new();
    let mut pindex: i32 = 0;
    while let Some(abbrev) = pg_get_next_timezone_abbrev(&mut pindex, tz_ref) {
        // The borrowed `abbrev` aliases `tz`; copy it out before the second
        // (also-immutable) borrow of `tz` below so there is no lifetime tangle.
        let abbrev = abbrev.to_string();

        // C: if (strspn(abbrev, "ABC..Z") != strlen(abbrev)) continue;
        if !abbrev.bytes().all(|b| b.is_ascii_uppercase()) {
            continue;
        }

        // C: pg_interpret_timezone_abbrev(abbrev, &t, &gmtoff, &isdst, tz).
        let interp = match pg_interpret_timezone_abbrev(&abbrev, t, tz_ref) {
            Some(v) => v,
            None => continue, // not actually used in this zone
        };

        rows.push(TimezoneAbbrevRow {
            abbrev,
            // C: itm_in.tm_usec = (int64) gmtoff * USECS_PER_SEC;
            utc_offset: interval_from_usec(interp.gmtoff * USECS_PER_SEC),
            is_dst: interp.isdst != 0,
        });
    }

    rows
}
