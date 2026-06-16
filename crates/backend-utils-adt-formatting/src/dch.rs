//! Date/time format-picture engine — `DCH_to_char` (the `to_char(timestamp/...)`
//! producer side).
//!
//! Faithful idiomatic port of formatting.c:2518-2787 (PG 18.3) — the
//! `DCH_to_char` switch and its `fmt_tm` / `TmToChar` working structs.
//!
//! ## Scope of this module
//!
//! This is the `to_char` *producer* half of the DCH engine: given an already
//! broken-down time (`TmToChar`) and a parsed format picture, it renders the
//! output text. It is fully self-contained apart from a handful of
//! sibling-subsystem boundary calls (see Seams below); all of the format-node
//! tables, suffix helpers, `printf`-style numeric formatting, and case folding
//! live in-crate ([`crate::tables`], [`crate::printf`], [`crate::case`]).
//!
//! ## Seams
//!
//! `DCH_to_char` reaches into two sibling subsystems, routed through the
//! per-owner seam crates:
//!   * pg_locale.c — `cache_locale_time` and the localized day/month-name
//!     accessors (`localized_full_months` / `localized_abbrev_months` /
//!     `localized_full_days` / `localized_abbrev_days`). When `LC_TIME` is the C
//!     locale these return `None` and the engine falls back to the built-in
//!     English names (matching the C build).
//!   * datetime.c / isoweek.c — the pure calendar conversions `date2j`,
//!     `date2isoweek`, `date2isoyear`, `date2isoyearday`.

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_datetime::{HOURS_PER_DAY, MONTHS_PER_YEAR, SECS_PER_HOUR, SECS_PER_MINUTE};
use types_error::{
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_INVALID_DATETIME_FORMAT,
};
use types_core::Oid;
use types_datetime::fsec_t;

use crate::case::{
    asc_tolower_z, asc_toupper_z, get_th, str_initcap_z, str_tolower_z, str_toupper_z,
};
use crate::printf::*;
use crate::tables::*;

// ---------------------------------------------------------------------------
// Seam wrappers (genuine cross-subsystem calls).
// ---------------------------------------------------------------------------

/// C: `cache_locale_time()` via the per-owner seam.
fn cache_locale_time() -> PgResult<()> {
    backend_utils_adt_pg_locale_seams::cache_locale_time::call()
}
fn loc_full_months<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    backend_utils_adt_pg_locale_seams::localized_full_months::call(mcx)
        .map(pgvec_of_pgvec_to_vec)
}
fn loc_abbrev_months<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    backend_utils_adt_pg_locale_seams::localized_abbrev_months::call(mcx)
        .map(pgvec_of_pgvec_to_vec)
}
fn loc_full_days<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    backend_utils_adt_pg_locale_seams::localized_full_days::call(mcx)
        .map(pgvec_of_pgvec_to_vec)
}
fn loc_abbrev_days<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    backend_utils_adt_pg_locale_seams::localized_abbrev_days::call(mcx)
        .map(pgvec_of_pgvec_to_vec)
}

/// The localized-name seams return arena-allocated `PgVec<PgVec<u8>>`; the DCH
/// engine consumes them as plain `Vec<Vec<u8>>` (NUL-free owned names), so copy
/// the bytes out.
fn pgvec_of_pgvec_to_vec(a: mcx::PgVec<'_, mcx::PgVec<'_, u8>>) -> Vec<Vec<u8>> {
    a.iter().map(|e| e.to_vec()).collect()
}

fn date2j(year: i32, month: i32, day: i32) -> i32 {
    backend_utils_adt_datetime_seams::date2j::call(year, month, day)
}
fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    backend_utils_adt_isoweek_seams::date2isoweek::call(year, mon, mday)
}
fn date2isoyear(year: i32, mon: i32, mday: i32) -> i32 {
    backend_utils_adt_isoweek_seams::date2isoyear::call(year, mon, mday)
}
fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    backend_utils_adt_isoweek_seams::date2isoyearday::call(year, mon, mday)
}

// ----------
// fmt_tm / TmToChar (formatting.c:509)
// ----------

/// C: `struct fmt_tm` (formatting.c:509) -- like `pg_tm` but with a 64-bit
/// `tm_hour`, used so intervals can carry large hour counts.
#[derive(Clone, Default)]
pub struct FmtTm {
    pub tm_sec: i32,
    pub tm_min: i32,
    pub tm_hour: i64,
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
    pub tm_wday: i32,
    pub tm_yday: i32,
    pub tm_gmtoff: i64,
}

/// C: `TmToChar` (formatting.c:522).
#[derive(Clone, Default)]
pub struct TmToChar {
    pub tm: FmtTm,
    pub fsec: fsec_t,
    pub tzn: Option<String>,
}

impl TmToChar {
    /// C: `ZERO_tmtc` (formatting.c:554) -- ZERO_tm sets mday/mon to 1.
    pub fn zero() -> Self {
        let mut t = TmToChar::default();
        t.tm.tm_mday = 1;
        t.tm.tm_mon = 1;
        t
    }
}

/// C: `struct fmt_tz` (formatting.c:474) -- do_to_timestamp's tz output.
#[derive(Clone, Copy, Default)]
pub struct FmtTz {
    pub has_tz: bool,
    pub gmtoffset: i32,
}

/// C: `ADJUST_YEAR(year, is_interval)` (formatting.c:208).
#[inline]
fn adjust_year(year: i32, is_interval: bool) -> i32 {
    if is_interval {
        year
    } else if year <= 0 {
        -(year - 1)
    } else {
        year
    }
}

fn invalid_for_interval(is_interval: bool) -> PgResult<()> {
    if is_interval {
        return Err(PgError::error(
            "invalid format specification for an interval value".to_string(),
        )
        .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT)
        .with_hint("Intervals are not tied to specific calendar dates."));
    }
    Ok(())
}

/// Append `bytes` to the working buffer (C's `StringInfo` append analog).
fn pg_append(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes);
}

/// Append a single byte to the working buffer.
fn pg_push(out: &mut Vec<u8>, b: u8) {
    out.push(b);
}

/// Helper for the TM-localized day/month branches: enforce the length cap and
/// append (formatting.c repeated block).
fn append_localized(out: &mut Vec<u8>, str: &[u8], key_len: usize) -> PgResult<()> {
    if str.len() <= (key_len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ {
        pg_append(out, str);
        Ok(())
    } else {
        Err(
            PgError::error("localized string format value too long".to_string())
                .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
        )
    }
}

/// Append a `str_numth` ordinal suffix to `out` for the digits just written
/// starting at `start` (mirrors C's `str_numth(s, s, ...)`).
fn apply_thth(out: &mut Vec<u8>, start: usize, suffix: u8) -> PgResult<()> {
    if s_thth(suffix) {
        // C: str_numth(s, s, ...) — the suffix of the digits just written.
        let num = out[start..].to_vec();
        let th = get_th(&num, s_th_type(suffix))?;
        pg_append(out, th.as_bytes());
    }
    Ok(())
}

/// C: `DCH_to_char` (formatting.c:2518).
///
/// The working output buffer (C's `char *result` / `StringInfo`) is built into
/// a plain owned `Vec<u8>` and returned to the caller (the `pstrdup`-into-the
/// caller analog). `mcx` is threaded for the locale-case / localized-name seam
/// calls (which allocate into the context).
pub fn dch_to_char<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: &[FormatNode],
    is_interval: bool,
    in_: &TmToChar,
    collid: Oid,
) -> PgResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    dch_to_char_inner(mcx, &mut out, nodes, is_interval, in_, collid)?;
    Ok(out)
}

/// The fallible producer body. Mirrors `DCH_to_char`'s switch byte-for-byte.
fn dch_to_char_inner<'mcx>(
    mcx: Mcx<'mcx>,
    out: &mut Vec<u8>,
    nodes: &[FormatNode],
    is_interval: bool,
    in_: &TmToChar,
    collid: Oid,
) -> PgResult<()> {
    cache_locale_time()?;

    let tm = &in_.tm;

    for n in nodes.iter() {
        if n.typ == NODE_TYPE_END {
            break;
        }
        if n.typ != NODE_TYPE_ACTION {
            // strcpy(s, n->character)
            let cs = cstr_to_slice(&n.character);
            pg_append(out, cs);
            continue;
        }

        let key = &DCH_KEYWORDS[n.key as usize];
        let suffix = n.suffix;
        match key.id {
            DCH_A_M | DCH_P_M => {
                pg_append(out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= (HOURS_PER_DAY / 2) as i64 {
                        P_M_STR
                    } else {
                        A_M_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_AM | DCH_PM => {
                pg_append(out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= (HOURS_PER_DAY / 2) as i64 {
                        PM_STR
                    } else {
                        AM_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_A_M_LOWER | DCH_P_M_LOWER => {
                pg_append(out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= (HOURS_PER_DAY / 2) as i64 {
                        P_M_LOWER_STR
                    } else {
                        A_M_LOWER_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_AM_LOWER | DCH_PM_LOWER => {
                pg_append(out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= (HOURS_PER_DAY / 2) as i64 {
                        PM_LOWER_STR
                    } else {
                        AM_LOWER_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_HH | DCH_HH12 => {
                let width = if s_fm(suffix) {
                    0
                } else if tm.tm_hour >= 0 {
                    2
                } else {
                    3
                };
                let half = (HOURS_PER_DAY / 2) as i64;
                let v = if tm.tm_hour % half == 0 {
                    half
                } else {
                    tm.tm_hour % half
                };
                let start = out.len();
                pg_append(out, fmt_0d(width, v).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_HH24 => {
                let width = if s_fm(suffix) {
                    0
                } else if tm.tm_hour >= 0 {
                    2
                } else {
                    3
                };
                let start = out.len();
                pg_append(out, fmt_0d(width, tm.tm_hour).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_MI => {
                let width = if s_fm(suffix) {
                    0
                } else if tm.tm_min >= 0 {
                    2
                } else {
                    3
                };
                let start = out.len();
                pg_append(out, fmt_0d(width, tm.tm_min as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_SS => {
                let width = if s_fm(suffix) {
                    0
                } else if tm.tm_sec >= 0 {
                    2
                } else {
                    3
                };
                let start = out.len();
                pg_append(out, fmt_0d(width, tm.tm_sec as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_FF1 => dch_fsec(out, 1, in_.fsec / 100000, suffix)?,
            DCH_FF2 => dch_fsec(out, 2, in_.fsec / 10000, suffix)?,
            DCH_FF3 | DCH_MS => dch_fsec(out, 3, in_.fsec / 1000, suffix)?,
            DCH_FF4 => dch_fsec(out, 4, in_.fsec / 100, suffix)?,
            DCH_FF5 => dch_fsec(out, 5, in_.fsec / 10, suffix)?,
            DCH_FF6 | DCH_US => dch_fsec(out, 6, in_.fsec, suffix)?,
            DCH_SSSS => {
                let v = tm.tm_hour * SECS_PER_HOUR as i64
                    + (tm.tm_min * SECS_PER_MINUTE) as i64
                    + tm.tm_sec as i64;
                let start = out.len();
                pg_append(out, fmt_d(v).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_TZ_LOWER => {
                invalid_for_interval(is_interval)?;
                if let Some(tzn) = &in_.tzn {
                    let p = asc_tolower_z(tzn.as_bytes());
                    pg_append(out, &p);
                }
            }
            DCH_TZ => {
                invalid_for_interval(is_interval)?;
                if let Some(tzn) = &in_.tzn {
                    pg_append(out, tzn.as_bytes());
                }
            }
            DCH_TZH => {
                invalid_for_interval(is_interval)?;
                let sign = if tm.tm_gmtoff >= 0 { '+' } else { '-' };
                pg_push(out, sign as u8);
                pg_append(out,
                    fmt_0d(
                        2,
                        ((tm.tm_gmtoff as i32).unsigned_abs() / SECS_PER_HOUR as u32) as i64,
                    )
                    .as_bytes(),
                );
            }
            DCH_TZM => {
                invalid_for_interval(is_interval)?;
                let mins = ((tm.tm_gmtoff as i32).unsigned_abs() % SECS_PER_HOUR as u32)
                    / SECS_PER_MINUTE as u32;
                pg_append(out, fmt_0d(2, mins as i64).as_bytes());
            }
            DCH_OF => {
                invalid_for_interval(is_interval)?;
                let sign = if tm.tm_gmtoff >= 0 { '+' } else { '-' };
                let width = if s_fm(suffix) { 0 } else { 2 };
                pg_push(out, sign as u8);
                pg_append(out,
                    fmt_0d(
                        width,
                        ((tm.tm_gmtoff as i32).unsigned_abs() / SECS_PER_HOUR as u32) as i64,
                    )
                    .as_bytes(),
                );
                if (tm.tm_gmtoff as i32).unsigned_abs() % SECS_PER_HOUR as u32 != 0 {
                    pg_push(out, b':');
                    let mins = ((tm.tm_gmtoff as i32).unsigned_abs() % SECS_PER_HOUR as u32)
                        / SECS_PER_MINUTE as u32;
                    pg_append(out, fmt_0d(2, mins as i64).as_bytes());
                }
            }
            DCH_A_D | DCH_B_C => {
                invalid_for_interval(is_interval)?;
                pg_append(out, if tm.tm_year <= 0 { B_C_STR } else { A_D_STR }.as_bytes());
            }
            DCH_AD | DCH_BC => {
                invalid_for_interval(is_interval)?;
                pg_append(out, if tm.tm_year <= 0 { BC_STR } else { AD_STR }.as_bytes());
            }
            DCH_A_D_LOWER | DCH_B_C_LOWER => {
                invalid_for_interval(is_interval)?;
                pg_append(out,
                    if tm.tm_year <= 0 {
                        B_C_LOWER_STR
                    } else {
                        A_D_LOWER_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_AD_LOWER | DCH_BC_LOWER => {
                invalid_for_interval(is_interval)?;
                pg_append(out,
                    if tm.tm_year <= 0 {
                        BC_LOWER_STR
                    } else {
                        AD_LOWER_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_MONTH => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    let s = str_toupper_z(mcx, &loc_month_full(mcx, tm.tm_mon), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out,
                        fmt_pad_str(
                            if s_fm(suffix) { 0 } else { -9 },
                            &String::from_utf8_lossy(&asc_toupper_z(
                                MONTHS_FULL[(tm.tm_mon - 1) as usize].as_bytes(),
                            )),
                        )
                        .as_bytes(),
                    );
                }
            }
            DCH_MONTH_CAP => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    let s = str_initcap_z(mcx, &loc_month_full(mcx, tm.tm_mon), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out,
                        fmt_pad_str(
                            if s_fm(suffix) { 0 } else { -9 },
                            MONTHS_FULL[(tm.tm_mon - 1) as usize],
                        )
                        .as_bytes(),
                    );
                }
            }
            DCH_MONTH_LOWER => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    let s = str_tolower_z(mcx, &loc_month_full(mcx, tm.tm_mon), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out,
                        fmt_pad_str(
                            if s_fm(suffix) { 0 } else { -9 },
                            &String::from_utf8_lossy(&asc_tolower_z(
                                MONTHS_FULL[(tm.tm_mon - 1) as usize].as_bytes(),
                            )),
                        )
                        .as_bytes(),
                    );
                }
            }
            DCH_MON => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    let s = str_toupper_z(mcx, &loc_month_abbrev(mcx, tm.tm_mon), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out, &asc_toupper_z(
                        MONTHS[(tm.tm_mon - 1) as usize].as_bytes(),
                    ));
                }
            }
            DCH_MON_CAP => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    let s = str_initcap_z(mcx, &loc_month_abbrev(mcx, tm.tm_mon), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out, MONTHS[(tm.tm_mon - 1) as usize].as_bytes());
                }
            }
            DCH_MON_LOWER => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    let s = str_tolower_z(mcx, &loc_month_abbrev(mcx, tm.tm_mon), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out, &asc_tolower_z(
                        MONTHS[(tm.tm_mon - 1) as usize].as_bytes(),
                    ));
                }
            }
            DCH_MM => {
                let width = if s_fm(suffix) {
                    0
                } else if tm.tm_mon >= 0 {
                    2
                } else {
                    3
                };
                let start = out.len();
                pg_append(out, fmt_0d(width, tm.tm_mon as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_DAY => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    let s = str_toupper_z(mcx, &loc_day_full(mcx, tm.tm_wday), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out,
                        fmt_pad_str(
                            if s_fm(suffix) { 0 } else { -9 },
                            &String::from_utf8_lossy(&asc_toupper_z(
                                DAYS[tm.tm_wday as usize].as_bytes(),
                            )),
                        )
                        .as_bytes(),
                    );
                }
            }
            DCH_DAY_CAP => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    let s = str_initcap_z(mcx, &loc_day_full(mcx, tm.tm_wday), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out,
                        fmt_pad_str(if s_fm(suffix) { 0 } else { -9 }, DAYS[tm.tm_wday as usize])
                            .as_bytes(),
                    );
                }
            }
            DCH_DAY_LOWER => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    let s = str_tolower_z(mcx, &loc_day_full(mcx, tm.tm_wday), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out,
                        fmt_pad_str(
                            if s_fm(suffix) { 0 } else { -9 },
                            &String::from_utf8_lossy(&asc_tolower_z(
                                DAYS[tm.tm_wday as usize].as_bytes(),
                            )),
                        )
                        .as_bytes(),
                    );
                }
            }
            DCH_DY => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    let s = str_toupper_z(mcx, &loc_day_abbrev(mcx, tm.tm_wday), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out, &asc_toupper_z(
                        DAYS_SHORT[tm.tm_wday as usize].as_bytes(),
                    ));
                }
            }
            DCH_DY_CAP => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    let s = str_initcap_z(mcx, &loc_day_abbrev(mcx, tm.tm_wday), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out, DAYS_SHORT[tm.tm_wday as usize].as_bytes());
                }
            }
            DCH_DY_LOWER => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    let s = str_tolower_z(mcx, &loc_day_abbrev(mcx, tm.tm_wday), collid)?;
                    append_localized(out, &s, key.len)?;
                } else {
                    pg_append(out, &asc_tolower_z(
                        DAYS_SHORT[tm.tm_wday as usize].as_bytes(),
                    ));
                }
            }
            DCH_DDD | DCH_IDDD => {
                let width = if s_fm(suffix) { 0 } else { 3 };
                let v = if key.id == DCH_DDD {
                    tm.tm_yday
                } else {
                    date2isoyearday(tm.tm_year, tm.tm_mon, tm.tm_mday)
                };
                let start = out.len();
                pg_append(out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_DD => {
                let width = if s_fm(suffix) { 0 } else { 2 };
                let start = out.len();
                pg_append(out, fmt_0d(width, tm.tm_mday as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_D => {
                invalid_for_interval(is_interval)?;
                let start = out.len();
                pg_append(out, fmt_d((tm.tm_wday + 1) as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_ID => {
                invalid_for_interval(is_interval)?;
                let v = if tm.tm_wday == 0 { 7 } else { tm.tm_wday };
                let start = out.len();
                pg_append(out, fmt_d(v as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_WW => {
                let width = if s_fm(suffix) { 0 } else { 2 };
                let start = out.len();
                pg_append(out, fmt_0d(width, ((tm.tm_yday - 1) / 7 + 1) as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_IW => {
                let width = if s_fm(suffix) { 0 } else { 2 };
                let start = out.len();
                pg_append(out,
                    fmt_0d(
                        width,
                        date2isoweek(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64,
                    )
                    .as_bytes(),
                );
                apply_thth(out, start, suffix)?;
            }
            DCH_Q => {
                if tm.tm_mon == 0 {
                    continue;
                }
                let start = out.len();
                pg_append(out, fmt_d(((tm.tm_mon - 1) / 3 + 1) as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_CC => {
                let i: i32 = if is_interval {
                    tm.tm_year / 100
                } else if tm.tm_year > 0 {
                    (tm.tm_year - 1) / 100 + 1
                } else {
                    tm.tm_year / 100 - 1
                };
                let start = out.len();
                if (-99..=99).contains(&i) {
                    let width = if s_fm(suffix) {
                        0
                    } else if i >= 0 {
                        2
                    } else {
                        3
                    };
                    pg_append(out, fmt_0d(width, i as i64).as_bytes());
                } else {
                    pg_append(out, fmt_d(i as i64).as_bytes());
                }
                apply_thth(out, start, suffix)?;
            }
            DCH_Y_YYY => {
                let ay = adjust_year(tm.tm_year, is_interval);
                let i = ay / 1000;
                let start = out.len();
                pg_append(out, format!("{},{:03}", i, ay - (i * 1000)).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_YYYY | DCH_IYYY => {
                let ay = adjust_year(tm.tm_year, is_interval);
                let width = if s_fm(suffix) {
                    0
                } else if ay >= 0 {
                    4
                } else {
                    5
                };
                let v = if key.id == DCH_YYYY {
                    ay
                } else {
                    adjust_year(date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday), is_interval)
                };
                let start = out.len();
                pg_append(out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_YYY | DCH_IYY => {
                let ay = adjust_year(tm.tm_year, is_interval);
                let width = if s_fm(suffix) {
                    0
                } else if ay >= 0 {
                    3
                } else {
                    4
                };
                let v = if key.id == DCH_YYY {
                    ay
                } else {
                    adjust_year(date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday), is_interval)
                } % 1000;
                let start = out.len();
                pg_append(out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_YY | DCH_IY => {
                let ay = adjust_year(tm.tm_year, is_interval);
                let width = if s_fm(suffix) {
                    0
                } else if ay >= 0 {
                    2
                } else {
                    3
                };
                let v = if key.id == DCH_YY {
                    ay
                } else {
                    adjust_year(date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday), is_interval)
                } % 100;
                let start = out.len();
                pg_append(out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_Y | DCH_I => {
                let v = if key.id == DCH_Y {
                    adjust_year(tm.tm_year, is_interval)
                } else {
                    adjust_year(date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday), is_interval)
                } % 10;
                let start = out.len();
                // C: "%1d" -> min width 1, sign-aware.  v % 10 is in -9..9.
                pg_append(out, fmt_d(v as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_RM | DCH_RM_LOWER => {
                if tm.tm_mon == 0 && tm.tm_year == 0 {
                    continue;
                }
                let months: &[&str; 12] = if key.id == DCH_RM {
                    &RM_MONTHS_UPPER
                } else {
                    &RM_MONTHS_LOWER
                };
                let mon: i32 = if tm.tm_mon == 0 {
                    if tm.tm_year >= 0 {
                        0
                    } else {
                        MONTHS_PER_YEAR - 1
                    }
                } else if tm.tm_mon < 0 {
                    -(tm.tm_mon + 1)
                } else {
                    MONTHS_PER_YEAR - tm.tm_mon
                };
                pg_append(out,
                    fmt_pad_str(if s_fm(suffix) { 0 } else { -4 }, months[mon as usize]).as_bytes(),
                );
            }
            DCH_W => {
                let start = out.len();
                pg_append(out, fmt_d(((tm.tm_mday - 1) / 7 + 1) as i64).as_bytes());
                apply_thth(out, start, suffix)?;
            }
            DCH_J => {
                let start = out.len();
                pg_append(out,
                    fmt_d(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64).as_bytes(),
                );
                apply_thth(out, start, suffix)?;
            }
            DCH_FX => {}
            _ => {}
        }
    }

    Ok(())
}

/// C: `DCH_to_char_fsec` macro (formatting.c:2602).
fn dch_fsec(out: &mut Vec<u8>, prec: usize, frac_val: i32, suffix: u8) -> PgResult<()> {
    let start = out.len();
    pg_append(out, fmt_0d(prec, frac_val as i64).as_bytes());
    apply_thth(out, start, suffix)
}

fn cstr_to_slice(buf: &[u8; MAX_MULTIBYTE_CHAR_LEN + 1]) -> &[u8] {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    &buf[..end]
}

// Localized array accessors that fall back to the English names if the locale
// cache is not populated (cache_locale_time leaves them as the English defaults
// in the C build too when LC_TIME is C).
fn loc_month_full<'mcx>(mcx: Mcx<'mcx>, mon: i32) -> Vec<u8> {
    match loc_full_months(mcx) {
        Some(a) => a[(mon - 1) as usize].clone(),
        None => MONTHS_FULL[(mon - 1) as usize].as_bytes().to_vec(),
    }
}
fn loc_month_abbrev<'mcx>(mcx: Mcx<'mcx>, mon: i32) -> Vec<u8> {
    match loc_abbrev_months(mcx) {
        Some(a) => a[(mon - 1) as usize].clone(),
        None => MONTHS[(mon - 1) as usize].as_bytes().to_vec(),
    }
}
fn loc_day_full<'mcx>(mcx: Mcx<'mcx>, wday: i32) -> Vec<u8> {
    match loc_full_days(mcx) {
        Some(a) => a[wday as usize].clone(),
        None => DAYS[wday as usize].as_bytes().to_vec(),
    }
}
fn loc_day_abbrev<'mcx>(mcx: Mcx<'mcx>, wday: i32) -> Vec<u8> {
    match loc_abbrev_days(mcx) {
        Some(a) => a[wday as usize].clone(),
        None => DAYS_SHORT[wday as usize].as_bytes().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    // The localized-name seams are not installed in `cargo test` (no provider),
    // so these exercises use the C-locale fallback paths (no `s_tm` suffix),
    // which never call them. `cache_locale_time` must be installed to a no-op
    // for the `to_char` entry; we install minimal test stubs here.
    fn install_test_seams() {
        crate::install_test_seams();
    }

    fn node_action(id: i32, suffix: u8) -> FormatNode {
        // Find the keyword index whose id matches.
        let key = DCH_KEYWORDS.iter().position(|k| k.id == id).unwrap() as i32;
        FormatNode {
            typ: NODE_TYPE_ACTION,
            character: [0; MAX_MULTIBYTE_CHAR_LEN + 1],
            suffix,
            key,
        }
    }

    fn node_char(c: u8) -> FormatNode {
        let mut character = [0u8; MAX_MULTIBYTE_CHAR_LEN + 1];
        character[0] = c;
        FormatNode {
            typ: NODE_TYPE_CHAR,
            character,
            suffix: 0,
            key: -1,
        }
    }

    fn node_end() -> FormatNode {
        FormatNode {
            typ: NODE_TYPE_END,
            character: [0; MAX_MULTIBYTE_CHAR_LEN + 1],
            suffix: 0,
            key: -1,
        }
    }

    fn tmtc(year: i32, mon: i32, mday: i32, hour: i64, min: i32, sec: i32) -> TmToChar {
        let mut t = TmToChar::default();
        t.tm.tm_year = year;
        t.tm.tm_mon = mon;
        t.tm.tm_mday = mday;
        t.tm.tm_hour = hour;
        t.tm.tm_min = min;
        t.tm.tm_sec = sec;
        t
    }

    #[test]
    fn dch_to_char_basic_hms() {
        install_test_seams();
        let ctx = MemoryContext::new("dch-test");
        let mcx = ctx.mcx();
        let t = tmtc(2020, 7, 14, 13, 5, 9);
        // HH24:MI:SS
        let nodes = vec![
            node_action(DCH_HH24, 0),
            node_char(b':'),
            node_action(DCH_MI, 0),
            node_char(b':'),
            node_action(DCH_SS, 0),
            node_end(),
        ];
        let out = dch_to_char(mcx, &nodes, false, &t, 0u32).unwrap();
        assert_eq!(out, b"13:05:09");
    }

    #[test]
    fn dch_to_char_year_and_month_num() {
        install_test_seams();
        let ctx = MemoryContext::new("dch-test");
        let mcx = ctx.mcx();
        let t = tmtc(1999, 1, 8, 0, 0, 0);
        // YYYY-MM-DD
        let nodes = vec![
            node_action(DCH_YYYY, 0),
            node_char(b'-'),
            node_action(DCH_MM, 0),
            node_char(b'-'),
            node_action(DCH_DD, 0),
            node_end(),
        ];
        let out = dch_to_char(mcx, &nodes, false, &t, 0u32).unwrap();
        assert_eq!(out, b"1999-01-08");
    }

    #[test]
    fn dch_to_char_ampm_and_hh12() {
        install_test_seams();
        let ctx = MemoryContext::new("dch-test");
        let mcx = ctx.mcx();
        let t = tmtc(2020, 7, 14, 13, 5, 9);
        // HH12 AM
        let nodes = vec![
            node_action(DCH_HH12, 0),
            node_char(b' '),
            node_action(DCH_AM, 0),
            node_end(),
        ];
        let out = dch_to_char(mcx, &nodes, false, &t, 0u32).unwrap();
        assert_eq!(out, b"01 PM");
    }

    #[test]
    fn dch_to_char_month_name_ascii() {
        install_test_seams();
        let ctx = MemoryContext::new("dch-test");
        let mcx = ctx.mcx();
        let t = tmtc(1999, 1, 8, 0, 0, 0);
        // FMMonth -> "January" (FM trims padding; ASCII path, no TM suffix).
        let nodes = vec![node_action(DCH_MONTH_CAP, DCH_S_FM), node_end()];
        let out = dch_to_char(mcx, &nodes, false, &t, 0u32).unwrap();
        assert_eq!(out, b"January");
    }

    #[test]
    fn dch_to_char_dd_th() {
        install_test_seams();
        let ctx = MemoryContext::new("dch-test");
        let mcx = ctx.mcx();
        let t = tmtc(2020, 7, 14, 0, 0, 0);
        // DDth -> "14th"
        let nodes = vec![node_action(DCH_DD, DCH_S_TH_LOWER), node_end()];
        let out = dch_to_char(mcx, &nodes, false, &t, 0u32).unwrap();
        assert_eq!(out, b"14th");
    }

    #[test]
    fn invalid_for_interval_rejects_tz() {
        install_test_seams();
        let ctx = MemoryContext::new("dch-test");
        let mcx = ctx.mcx();
        let t = tmtc(0, 0, 0, 1, 0, 0);
        let nodes = vec![node_action(DCH_TZ, 0), node_end()];
        let err = dch_to_char(mcx, &nodes, true, &t, 0u32).unwrap_err();
        assert!(format!("{err:?}").contains("interval"));
    }
}
