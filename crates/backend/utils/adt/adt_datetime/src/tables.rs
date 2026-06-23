//! Static date/time token tables, copied verbatim (entries and values) from
//! `src/backend/utils/adt/datetime.c`.
//!
//! `datetktbl` and `deltatktbl` MUST stay in the exact C order: they are
//! pre-sorted for [`crate::decode::datebsearch`], so the ordering is
//! load-bearing.  A `#[cfg(test)]` assertion verifies each table is sorted by
//! the datebsearch key (the truncated token, `TOKMAXLEN` chars).
//!
//! Each token string here is already truncated to at most `TOKMAXLEN` (10)
//! characters, mirroring the `char token[TOKMAXLEN + 1]` field that the C
//! source pre-truncates via its initializer.

use ::types_datetime::DateToken;
use ::types_datetime::{
    AD, ADBC, AGO, AM, AMPM, BC, DOW, DTK_CENTURY, DTK_DAY, DTK_DECADE, DTK_DOW, DTK_DOY,
    DTK_EARLY, DTK_EPOCH, DTK_HOUR, DTK_ISODOW, DTK_ISOYEAR, DTK_JULIAN, DTK_LATE, DTK_MICROSEC,
    DTK_MILLENNIUM, DTK_MILLISEC, DTK_MINUTE, DTK_MONTH, DTK_NOW, DTK_QUARTER, DTK_SECOND, DTK_TIME,
    DTK_TODAY, DTK_TOMORROW, DTK_TZ, DTK_TZ_HOUR, DTK_TZ_MINUTE, DTK_WEEK, DTK_YEAR, DTK_YESTERDAY,
    DTK_ZULU, DTZMOD, IGNORE_DTF, ISOTIME, MONTH, PM, RESERV, SECS_PER_HOUR, UNITS,
};

use crate::consts::{
    DAGO, DA_D, DB_C, DCENTURY, DDAY, DDECADE, DHOUR, DMICROSEC, DMILLENNIUM, DMILLISEC, DMINUTE,
    DMONTH, DQUARTER, DSECOND, DTIMEZONE, DWEEK, DYEAR, EARLY, EPOCH, LATE, NOW, TODAY, TOMORROW,
    YESTERDAY,
};

/// `datetktbl` -- date/time keywords for [`crate::decode::DecodeSpecial`].
///
/// Strictly alphabetically ordered by the (truncated) `token` for binary
/// search.  Contains no TZ/DTZ/DYNTZ entries; those come from the timezone
/// abbreviation table at runtime.
///
/// (`utils/adt/datetime.c`)
pub static datetktbl: &[DateToken] = &[
    /* token, type, value */
    DateToken { token: "+infinity", r#type: RESERV, value: DTK_LATE }, /* same as "infinity" */
    DateToken { token: EARLY, r#type: RESERV, value: DTK_EARLY }, /* "-infinity" reserved for "early time" */
    DateToken { token: DA_D, r#type: ADBC, value: AD },          /* "ad" for years > 0 */
    DateToken { token: "allballs", r#type: RESERV, value: DTK_ZULU }, /* 00:00:00 */
    DateToken { token: "am", r#type: AMPM, value: AM },
    DateToken { token: "apr", r#type: MONTH, value: 4 },
    DateToken { token: "april", r#type: MONTH, value: 4 },
    DateToken { token: "at", r#type: IGNORE_DTF, value: 0 }, /* "at" (throwaway) */
    DateToken { token: "aug", r#type: MONTH, value: 8 },
    DateToken { token: "august", r#type: MONTH, value: 8 },
    DateToken { token: DB_C, r#type: ADBC, value: BC }, /* "bc" for years <= 0 */
    DateToken { token: "d", r#type: UNITS, value: DTK_DAY }, /* "day of month" for ISO input */
    DateToken { token: "dec", r#type: MONTH, value: 12 },
    DateToken { token: "december", r#type: MONTH, value: 12 },
    DateToken { token: "dow", r#type: UNITS, value: DTK_DOW }, /* day of week */
    DateToken { token: "doy", r#type: UNITS, value: DTK_DOY }, /* day of year */
    DateToken { token: "dst", r#type: DTZMOD, value: SECS_PER_HOUR },
    DateToken { token: EPOCH, r#type: RESERV, value: DTK_EPOCH }, /* "epoch" reserved for system epoch time */
    DateToken { token: "feb", r#type: MONTH, value: 2 },
    DateToken { token: "february", r#type: MONTH, value: 2 },
    DateToken { token: "fri", r#type: DOW, value: 5 },
    DateToken { token: "friday", r#type: DOW, value: 5 },
    DateToken { token: "h", r#type: UNITS, value: DTK_HOUR }, /* "hour" */
    DateToken { token: LATE, r#type: RESERV, value: DTK_LATE }, /* "infinity" reserved for "late time" */
    DateToken { token: "isodow", r#type: UNITS, value: DTK_ISODOW }, /* ISO day of week, Sunday == 7 */
    DateToken { token: "isoyear", r#type: UNITS, value: DTK_ISOYEAR }, /* year in terms of the ISO week date */
    DateToken { token: "j", r#type: UNITS, value: DTK_JULIAN },
    DateToken { token: "jan", r#type: MONTH, value: 1 },
    DateToken { token: "january", r#type: MONTH, value: 1 },
    DateToken { token: "jd", r#type: UNITS, value: DTK_JULIAN },
    DateToken { token: "jul", r#type: MONTH, value: 7 },
    DateToken { token: "julian", r#type: UNITS, value: DTK_JULIAN },
    DateToken { token: "july", r#type: MONTH, value: 7 },
    DateToken { token: "jun", r#type: MONTH, value: 6 },
    DateToken { token: "june", r#type: MONTH, value: 6 },
    DateToken { token: "m", r#type: UNITS, value: DTK_MONTH }, /* "month" for ISO input */
    DateToken { token: "mar", r#type: MONTH, value: 3 },
    DateToken { token: "march", r#type: MONTH, value: 3 },
    DateToken { token: "may", r#type: MONTH, value: 5 },
    DateToken { token: "mm", r#type: UNITS, value: DTK_MINUTE }, /* "minute" for ISO input */
    DateToken { token: "mon", r#type: DOW, value: 1 },
    DateToken { token: "monday", r#type: DOW, value: 1 },
    DateToken { token: "nov", r#type: MONTH, value: 11 },
    DateToken { token: "november", r#type: MONTH, value: 11 },
    DateToken { token: NOW, r#type: RESERV, value: DTK_NOW }, /* current transaction time */
    DateToken { token: "oct", r#type: MONTH, value: 10 },
    DateToken { token: "october", r#type: MONTH, value: 10 },
    DateToken { token: "on", r#type: IGNORE_DTF, value: 0 }, /* "on" (throwaway) */
    DateToken { token: "pm", r#type: AMPM, value: PM },
    DateToken { token: "s", r#type: UNITS, value: DTK_SECOND }, /* "seconds" for ISO input */
    DateToken { token: "sat", r#type: DOW, value: 6 },
    DateToken { token: "saturday", r#type: DOW, value: 6 },
    DateToken { token: "sep", r#type: MONTH, value: 9 },
    DateToken { token: "sept", r#type: MONTH, value: 9 },
    DateToken { token: "september", r#type: MONTH, value: 9 },
    DateToken { token: "sun", r#type: DOW, value: 0 },
    DateToken { token: "sunday", r#type: DOW, value: 0 },
    DateToken { token: "t", r#type: ISOTIME, value: DTK_TIME }, /* Filler for ISO time fields */
    DateToken { token: "thu", r#type: DOW, value: 4 },
    DateToken { token: "thur", r#type: DOW, value: 4 },
    DateToken { token: "thurs", r#type: DOW, value: 4 },
    DateToken { token: "thursday", r#type: DOW, value: 4 },
    DateToken { token: TODAY, r#type: RESERV, value: DTK_TODAY }, /* midnight */
    DateToken { token: TOMORROW, r#type: RESERV, value: DTK_TOMORROW }, /* tomorrow midnight */
    DateToken { token: "tue", r#type: DOW, value: 2 },
    DateToken { token: "tues", r#type: DOW, value: 2 },
    DateToken { token: "tuesday", r#type: DOW, value: 2 },
    DateToken { token: "wed", r#type: DOW, value: 3 },
    DateToken { token: "wednesday", r#type: DOW, value: 3 },
    DateToken { token: "weds", r#type: DOW, value: 3 },
    DateToken { token: "y", r#type: UNITS, value: DTK_YEAR }, /* "year" for ISO input */
    DateToken { token: YESTERDAY, r#type: RESERV, value: DTK_YESTERDAY }, /* yesterday midnight */
];

/// `deltatktbl` -- keywords for time units (intervals, EXTRACT), same format
/// as [`datetktbl`].  Strictly alphabetically ordered by the truncated token.
///
/// (`utils/adt/datetime.c`)
pub static deltatktbl: &[DateToken] = &[
    /* token, type, value */
    DateToken { token: "@", r#type: IGNORE_DTF, value: 0 }, /* postgres relative prefix */
    DateToken { token: DAGO, r#type: AGO, value: 0 }, /* "ago" indicates negative time offset */
    DateToken { token: "c", r#type: UNITS, value: DTK_CENTURY }, /* "century" relative */
    DateToken { token: "cent", r#type: UNITS, value: DTK_CENTURY }, /* "century" relative */
    DateToken { token: "centuries", r#type: UNITS, value: DTK_CENTURY }, /* "centuries" relative */
    DateToken { token: DCENTURY, r#type: UNITS, value: DTK_CENTURY }, /* "century" relative */
    DateToken { token: "d", r#type: UNITS, value: DTK_DAY }, /* "day" relative */
    DateToken { token: DDAY, r#type: UNITS, value: DTK_DAY }, /* "day" relative */
    DateToken { token: "days", r#type: UNITS, value: DTK_DAY }, /* "days" relative */
    DateToken { token: "dec", r#type: UNITS, value: DTK_DECADE }, /* "decade" relative */
    DateToken { token: DDECADE, r#type: UNITS, value: DTK_DECADE }, /* "decade" relative */
    DateToken { token: "decades", r#type: UNITS, value: DTK_DECADE }, /* "decades" relative */
    DateToken { token: "decs", r#type: UNITS, value: DTK_DECADE }, /* "decades" relative */
    DateToken { token: "h", r#type: UNITS, value: DTK_HOUR }, /* "hour" relative */
    DateToken { token: DHOUR, r#type: UNITS, value: DTK_HOUR }, /* "hour" relative */
    DateToken { token: "hours", r#type: UNITS, value: DTK_HOUR }, /* "hours" relative */
    DateToken { token: "hr", r#type: UNITS, value: DTK_HOUR }, /* "hour" relative */
    DateToken { token: "hrs", r#type: UNITS, value: DTK_HOUR }, /* "hours" relative */
    DateToken { token: "m", r#type: UNITS, value: DTK_MINUTE }, /* "minute" relative */
    DateToken { token: "microsecon", r#type: UNITS, value: DTK_MICROSEC }, /* "microsecond" relative */
    DateToken { token: "mil", r#type: UNITS, value: DTK_MILLENNIUM }, /* "millennium" relative */
    DateToken { token: "millennia", r#type: UNITS, value: DTK_MILLENNIUM }, /* "millennia" relative */
    DateToken { token: DMILLENNIUM, r#type: UNITS, value: DTK_MILLENNIUM }, /* "millennium" relative */
    DateToken { token: "millisecon", r#type: UNITS, value: DTK_MILLISEC }, /* relative */
    DateToken { token: "mils", r#type: UNITS, value: DTK_MILLENNIUM }, /* "millennia" relative */
    DateToken { token: "min", r#type: UNITS, value: DTK_MINUTE }, /* "minute" relative */
    DateToken { token: "mins", r#type: UNITS, value: DTK_MINUTE }, /* "minutes" relative */
    DateToken { token: DMINUTE, r#type: UNITS, value: DTK_MINUTE }, /* "minute" relative */
    DateToken { token: "minutes", r#type: UNITS, value: DTK_MINUTE }, /* "minutes" relative */
    DateToken { token: "mon", r#type: UNITS, value: DTK_MONTH }, /* "months" relative */
    DateToken { token: "mons", r#type: UNITS, value: DTK_MONTH }, /* "months" relative */
    DateToken { token: DMONTH, r#type: UNITS, value: DTK_MONTH }, /* "month" relative */
    DateToken { token: "months", r#type: UNITS, value: DTK_MONTH },
    DateToken { token: "ms", r#type: UNITS, value: DTK_MILLISEC },
    DateToken { token: "msec", r#type: UNITS, value: DTK_MILLISEC },
    DateToken { token: DMILLISEC, r#type: UNITS, value: DTK_MILLISEC },
    DateToken { token: "mseconds", r#type: UNITS, value: DTK_MILLISEC },
    DateToken { token: "msecs", r#type: UNITS, value: DTK_MILLISEC },
    DateToken { token: "qtr", r#type: UNITS, value: DTK_QUARTER }, /* "quarter" relative */
    DateToken { token: DQUARTER, r#type: UNITS, value: DTK_QUARTER }, /* "quarter" relative */
    DateToken { token: "s", r#type: UNITS, value: DTK_SECOND },
    DateToken { token: "sec", r#type: UNITS, value: DTK_SECOND },
    DateToken { token: DSECOND, r#type: UNITS, value: DTK_SECOND },
    DateToken { token: "seconds", r#type: UNITS, value: DTK_SECOND },
    DateToken { token: "secs", r#type: UNITS, value: DTK_SECOND },
    DateToken { token: DTIMEZONE, r#type: UNITS, value: DTK_TZ }, /* "timezone" time offset */
    DateToken { token: "timezone_h", r#type: UNITS, value: DTK_TZ_HOUR }, /* timezone hour units */
    DateToken { token: "timezone_m", r#type: UNITS, value: DTK_TZ_MINUTE }, /* timezone minutes units */
    DateToken { token: "us", r#type: UNITS, value: DTK_MICROSEC }, /* "microsecond" relative */
    DateToken { token: "usec", r#type: UNITS, value: DTK_MICROSEC }, /* "microsecond" relative */
    DateToken { token: DMICROSEC, r#type: UNITS, value: DTK_MICROSEC }, /* "microsecond" relative */
    DateToken { token: "useconds", r#type: UNITS, value: DTK_MICROSEC }, /* "microseconds" relative */
    DateToken { token: "usecs", r#type: UNITS, value: DTK_MICROSEC }, /* "microseconds" relative */
    DateToken { token: "w", r#type: UNITS, value: DTK_WEEK }, /* "week" relative */
    DateToken { token: DWEEK, r#type: UNITS, value: DTK_WEEK }, /* "week" relative */
    DateToken { token: "weeks", r#type: UNITS, value: DTK_WEEK }, /* "weeks" relative */
    DateToken { token: "y", r#type: UNITS, value: DTK_YEAR }, /* "year" relative */
    DateToken { token: DYEAR, r#type: UNITS, value: DTK_YEAR }, /* "year" relative */
    DateToken { token: "years", r#type: UNITS, value: DTK_YEAR }, /* "years" relative */
    DateToken { token: "yr", r#type: UNITS, value: DTK_YEAR }, /* "year" relative */
    DateToken { token: "yrs", r#type: UNITS, value: DTK_YEAR }, /* "years" relative */
];

/// Month abbreviations, `months[0..11]` == Jan..Dec.  (`utils/adt/datetime.c`)
pub static months: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Full weekday names, `days[0..6]` == Sunday..Saturday.  (`utils/adt/datetime.c`)
pub static days: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_datetime::TOKMAXLEN;

    /// Compare two tokens the way `datebsearch` does: by their first
    /// `TOKMAXLEN` bytes (which, since the source tokens are already
    /// truncated, is the whole token).
    fn datebsearch_key(tok: &str) -> &[u8] {
        let bytes = tok.as_bytes();
        &bytes[..bytes.len().min(TOKMAXLEN as usize)]
    }

    fn assert_sorted(tbl: &[DateToken], name: &str) {
        for win in tbl.windows(2) {
            let a = datebsearch_key(win[0].token);
            let b = datebsearch_key(win[1].token);
            assert!(
                a < b,
                "{name} not sorted by datebsearch key: {:?} >= {:?}",
                win[0].token,
                win[1].token
            );
        }
    }

    #[test]
    fn datetktbl_is_sorted() {
        assert_sorted(datetktbl, "datetktbl");
    }

    #[test]
    fn deltatktbl_is_sorted() {
        assert_sorted(deltatktbl, "deltatktbl");
    }

    #[test]
    fn tokens_within_tokmaxlen() {
        for t in datetktbl.iter().chain(deltatktbl.iter()) {
            assert!(
                t.token.len() <= TOKMAXLEN as usize,
                "token {:?} exceeds TOKMAXLEN",
                t.token
            );
        }
    }
}

// ---------------------------------------------------------------------------
// ConvertTimeZoneAbbrevs (datetime.c) — consumed by tzparser's load_tzoffsets.
// ---------------------------------------------------------------------------

/// `ConvertTimeZoneAbbrevs(abbrevs, n)` (datetime.c): build the
/// `TimeZoneAbbrevTable` from the parsed (already-validated, already-sorted)
/// `tzEntry` rows. The C packs the rows + dynamic-zone names into one
/// `guc_malloc`'d block keyed by `datetkn` tokens; the idiomatic model stores
/// the owned entries directly (`datebsearch` runs over them). Returns `None`
/// only on the C allocation-failure (`guc_malloc` NULL) path; the owned model
/// cannot fail here, so it always yields `Some`.
pub fn convert_time_zone_abbrevs(
    abbrevs: Vec<misc_more2::TzEntry>,
) -> Option<misc_more2::TimeZoneAbbrevTable> {
    Some(misc_more2::TimeZoneAbbrevTable { abbrevs })
}
