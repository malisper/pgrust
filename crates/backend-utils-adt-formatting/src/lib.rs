#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::if_same_then_else)]

//! `to_char`, `to_date`, `to_timestamp`, `to_number`, and the localized
//! string-case routines.
//!
//! Faithful port of PostgreSQL 18.3 `src/backend/utils/adt/formatting.c`.
//!
//! Cross-subsystem calls route through the per-owner seam crates:
//!   * pg_locale.c — `pg_newlocale_from_collation`, the case transforms
//!     (`pg_strlower`/`pg_strupper`/`pg_strtitle`/`pg_strfold`),
//!     `cache_locale_time`, and the four `localized_*` accessors
//!     (`backend-utils-adt-pg-locale-seams`);
//!   * mbutils.c — `GetDatabaseEncoding`, `pg_mblen`/`pg_mbstrlen`
//!     (`backend-utils-mb-mbutils-seams`);
//!   * datetime.c / isoweek.c / timestamp.c — the broken-down-time, calendar,
//!     and timezone conversions (`backend-utils-adt-{datetime,isoweek,
//!     timestamp}-seams`).
//! The NUM `NumericVar` arithmetic is a direct dependency on the ported
//! `backend-utils-adt-numeric`.

pub mod cache;
pub mod case;
pub mod dch;
pub mod dch_entry;
pub mod dch_fromchar;
pub mod fmgr_boundary;
pub mod fmgr_builtins;
pub mod fromchar;
pub mod num;
pub mod num_entry;
pub mod parse;
pub mod printf;
pub mod tables;

// ---------------------------------------------------------------------------
// Re-exports: the public API surface, keeping the original C function names.
// ---------------------------------------------------------------------------

pub use case::{
    asc_initcap, asc_tolower, asc_tolower_z, asc_toupper, asc_toupper_z, get_th, index_seq_search,
    is_separator_char, str_casefold, str_initcap, str_initcap_z, str_numth, str_tolower,
    str_tolower_z, str_toupper, str_toupper_z, suff_search,
};

pub use dch::{dch_to_char, FmtTm, FmtTz, TmToChar};

pub use dch_fromchar::{dch_datetime_type, dch_from_char, TmFromChar};

pub use dch_entry::{
    datetime_format_has_tz, datetime_to_char_body, do_to_timestamp, interval_to_char,
    parse_datetime, timestamp_to_char, timestamptz_to_char, to_date, to_timestamp,
    ParseDatetimeResult, ToTimestampResult,
};

pub use parse::{numdesc_prepare, parse_format};

pub use cache::{dch_cache_fetch, num_cache_fetch};

pub use fromchar::{
    adjust_partial_year_to_2020, from_char_parse_int, from_char_parse_int_len,
    from_char_seq_search, from_char_set_int, from_char_set_mode, is_next_separator,
    seq_search_ascii, seq_search_localized, strspace_len, FromCharCursor,
};

pub use num::{fill_str, int_to_roman, num_processor, NumProcessed};

pub use num_entry::{
    float4_to_char, float8_to_char, int4_to_char, int8_to_char, numeric_to_char,
    numeric_to_number,
};

pub use fmgr_boundary::{
    float4_to_char_boundary, float8_to_char_boundary, int4_to_char_boundary,
    int8_to_char_boundary, numeric_to_char_boundary, numeric_to_number_boundary,
    F_TO_CHAR_FLOAT4_TEXT, F_TO_CHAR_FLOAT8_TEXT, F_TO_CHAR_INT4_TEXT, F_TO_CHAR_INT8_TEXT,
    F_TO_CHAR_NUMERIC_TEXT, F_TO_NUMBER,
};

pub use tables::{FormatNode, FromCharDateMode, KeySuffix, KeyWord, NUMDesc};

/// Install this crate's inward seams. `str_tolower` is consumed by the tsearch
/// dictionaries through `backend-utils-adt-formatting-seams`.
pub fn init_seams() {
    backend_utils_adt_formatting_seams::str_tolower::set(case::str_tolower);
    fmgr_builtins::register_formatting_builtins();
}

/// Process-wide one-time install of the outward seam stubs the DCH unit tests
/// exercise (LC_TIME-is-C no-ops + the self-contained `date2j`). Guarded by a
/// `Once` because the per-owner seam slots panic on double `set`, and the tests
/// in `dch` and `dch_fromchar` run concurrently in one process.
#[cfg(test)]
pub(crate) fn install_test_seams() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        backend_utils_adt_pg_locale_seams::cache_locale_time::set(|| Ok(()));
        backend_utils_mb_mbutils_seams::pg_mblen_range::set(|_s| 1);
        backend_utils_adt_isoweek_seams::date2isoyearday::set(|_, _, _| 1);
        backend_utils_adt_isoweek_seams::date2isoweek::set(|_, _, _| 1);
        backend_utils_adt_isoweek_seams::date2isoyear::set(|y, _, _| y);
        backend_utils_adt_datetime_seams::date2j::set(|y, m, d| {
            // datetime.c:date2j — self-contained Gregorian->Julian conversion.
            let (year, month) = if m > 2 {
                (y + 4800, m + 1)
            } else {
                (y + 4799, m + 13)
            };
            let century = year / 100;
            let mut julian = year * 365 - 32167;
            julian += year / 4 - century + century / 4;
            julian += 7834 * month / 256 + d;
            julian
        });
    });
}
