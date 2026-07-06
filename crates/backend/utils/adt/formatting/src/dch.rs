//! DCH_to_char producer: broken-down time + format picture -> output text
//! (formatting.c:2518-2787). `TM` renders the cache_locale_time names under
//! the call collation's case mapping.

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_INVALID_DATETIME_FORMAT,
};

use ::adt_datetime::{date2j, fsec_t, HOURS_PER_DAY, MONTHS_PER_YEAR, SECS_PER_HOUR, SECS_PER_MINUTE};

use crate::case::{asc_tolower, asc_toupper, get_th};
use crate::isoweek::{date2isoweek, date2isoyear, date2isoyearday};
use crate::num::fmt_pad_str;
use crate::tables::*;

/// C: `struct fmt_tm` — like `pg_tm` but with a 64-bit `tm_hour`.
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

/// C: `TmToChar`.
#[derive(Clone, Default)]
pub struct TmToChar {
    pub tm: FmtTm,
    pub fsec: fsec_t,
    pub tzn: Option<Vec<u8>>,
}

impl TmToChar {
    /// C: `ZERO_tmtc` — ZERO_tm sets mday/mon to 1.
    pub fn zero() -> Self {
        let mut t = TmToChar::default();
        t.tm.tm_mday = 1;
        t.tm.tm_mon = 1;
        t
    }
}

/// C: `struct fmt_tz` — do_to_timestamp's tz output.
#[derive(Clone, Copy, Default)]
pub struct FmtTz {
    pub has_tz: bool,
    pub gmtoffset: i32,
}

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
        .with_hint("Intervals are not tied to specific calendar dates.")
        .into());
    }
    Ok(())
}

/// C: `sprintf(s, "%0*d", width, val)` — sign-aware zero pad (width counts sign).
fn fmt_0d(width: usize, val: i64) -> String {
    let neg = val < 0;
    let digits = (val as i128).unsigned_abs().to_string();
    let sign_len = if neg { 1 } else { 0 };
    let cur = digits.len() + sign_len;
    let pad = width.saturating_sub(cur);
    let mut out = String::with_capacity(width.max(cur));
    if neg {
        out.push('-');
    }
    for _ in 0..pad {
        out.push('0');
    }
    out.push_str(&digits);
    out
}

#[inline]
fn fmt_d(val: i64) -> String {
    val.to_string()
}

fn pg_append(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes);
}

enum TmCaseMode {
    Upper,
    Init,
    Lower,
}

// The S_TM leg shared by the month/day name arms (formatting.c:2596 etc.):
// case-map the localized name under collid, bounded by the C output-buffer
// budget check.
fn append_tm_localized<'mcx>(
    mcx: Mcx<'mcx>,
    out: &mut Vec<u8>,
    name: &[u8],
    mode: TmCaseMode,
    collid: Oid,
    key_len: usize,
) -> PgResult<()> {
    let cased = match mode {
        TmCaseMode::Upper => crate::case::str_toupper(mcx, name, collid)?,
        TmCaseMode::Init => crate::case::str_initcap(mcx, name, collid)?,
        TmCaseMode::Lower => crate::case::str_tolower(mcx, name, collid)?,
    };
    if cased.len() <= (key_len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ {
        pg_append(out, &cased);
        Ok(())
    } else {
        Err(PgError::error("localized string format value too long")
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
            .into())
    }
}

fn apply_thth(out: &mut Vec<u8>, start: usize, suffix: u8) -> PgResult<()> {
    if s_thth(suffix) {
        let num = out[start..].to_vec();
        let th = get_th(&num, s_th_type(suffix))?;
        pg_append(out, th.as_bytes());
    }
    Ok(())
}

/// DCH_processor: C `DCH_to_char` (formatting.c:2518). Renders `nodes` for the
/// broken-down time `in_` into an owned byte buffer.
pub fn dch_to_char<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: &[FormatNode],
    is_interval: bool,
    in_: &TmToChar,
    collid: Oid,
) -> PgResult<Vec<u8>> {
    // C: cache localized days and months (formatting.c:2529).
    let localized = pg_locale::cache_locale_time(mcx)?;
    let mut out: Vec<u8> = Vec::new();
    let tm = &in_.tm;

    for n in nodes.iter() {
        if n.typ == NODE_TYPE_END {
            break;
        }
        if n.typ != NODE_TYPE_ACTION {
            pg_append(&mut out, cstr_to_slice(&n.character));
            continue;
        }

        let key = &DCH_KEYWORDS[n.key as usize];
        let suffix = n.suffix;
        let half = (HOURS_PER_DAY / 2) as i64;
        match key.id {
            DCH_A_M | DCH_P_M => {
                pg_append(
                    &mut out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= half {
                        P_M_STR
                    } else {
                        A_M_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_AM | DCH_PM => {
                pg_append(
                    &mut out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= half {
                        PM_STR
                    } else {
                        AM_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_A_M_LOWER | DCH_P_M_LOWER => {
                pg_append(
                    &mut out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= half {
                        P_M_LOWER_STR
                    } else {
                        A_M_LOWER_STR
                    }
                    .as_bytes(),
                );
            }
            DCH_AM_LOWER | DCH_PM_LOWER => {
                pg_append(
                    &mut out,
                    if tm.tm_hour % HOURS_PER_DAY as i64 >= half {
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
                let v = if tm.tm_hour % half == 0 {
                    half
                } else {
                    tm.tm_hour % half
                };
                let start = out.len();
                pg_append(&mut out, fmt_0d(width, v).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                pg_append(&mut out, fmt_0d(width, tm.tm_hour).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                pg_append(&mut out, fmt_0d(width, tm.tm_min as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                pg_append(&mut out, fmt_0d(width, tm.tm_sec as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_FF1 => dch_fsec(&mut out, 1, in_.fsec / 100000, suffix)?,
            DCH_FF2 => dch_fsec(&mut out, 2, in_.fsec / 10000, suffix)?,
            DCH_FF3 | DCH_MS => dch_fsec(&mut out, 3, in_.fsec / 1000, suffix)?,
            DCH_FF4 => dch_fsec(&mut out, 4, in_.fsec / 100, suffix)?,
            DCH_FF5 => dch_fsec(&mut out, 5, in_.fsec / 10, suffix)?,
            DCH_FF6 | DCH_US => dch_fsec(&mut out, 6, in_.fsec, suffix)?,
            DCH_SSSS => {
                let v = tm.tm_hour * SECS_PER_HOUR as i64
                    + (tm.tm_min * SECS_PER_MINUTE) as i64
                    + tm.tm_sec as i64;
                let start = out.len();
                pg_append(&mut out, fmt_d(v).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_TZ_LOWER => {
                invalid_for_interval(is_interval)?;
                if let Some(tzn) = &in_.tzn {
                    pg_append(&mut out, &asc_tolower(tzn));
                }
            }
            DCH_TZ => {
                invalid_for_interval(is_interval)?;
                if let Some(tzn) = &in_.tzn {
                    pg_append(&mut out, tzn);
                }
            }
            DCH_TZH => {
                invalid_for_interval(is_interval)?;
                let sign = if tm.tm_gmtoff >= 0 { b'+' } else { b'-' };
                out.push(sign);
                pg_append(
                    &mut out,
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
                pg_append(&mut out, fmt_0d(2, mins as i64).as_bytes());
            }
            DCH_OF => {
                invalid_for_interval(is_interval)?;
                let sign = if tm.tm_gmtoff >= 0 { b'+' } else { b'-' };
                let width = if s_fm(suffix) { 0 } else { 2 };
                out.push(sign);
                pg_append(
                    &mut out,
                    fmt_0d(
                        width,
                        ((tm.tm_gmtoff as i32).unsigned_abs() / SECS_PER_HOUR as u32) as i64,
                    )
                    .as_bytes(),
                );
                if (tm.tm_gmtoff as i32).unsigned_abs() % SECS_PER_HOUR as u32 != 0 {
                    out.push(b':');
                    let mins = ((tm.tm_gmtoff as i32).unsigned_abs() % SECS_PER_HOUR as u32)
                        / SECS_PER_MINUTE as u32;
                    pg_append(&mut out, fmt_0d(2, mins as i64).as_bytes());
                }
            }
            DCH_A_D | DCH_B_C => {
                invalid_for_interval(is_interval)?;
                pg_append(&mut out, if tm.tm_year <= 0 { B_C_STR } else { A_D_STR }.as_bytes());
            }
            DCH_AD | DCH_BC => {
                invalid_for_interval(is_interval)?;
                pg_append(&mut out, if tm.tm_year <= 0 { BC_STR } else { AD_STR }.as_bytes());
            }
            DCH_A_D_LOWER | DCH_B_C_LOWER => {
                invalid_for_interval(is_interval)?;
                pg_append(
                    &mut out,
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
                pg_append(
                    &mut out,
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
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.full_months[(tm.tm_mon - 1) as usize],
                        TmCaseMode::Upper,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                let name = asc_toupper(MONTHS_FULL[(tm.tm_mon - 1) as usize].as_bytes());
                pg_append(
                    &mut out,
                    fmt_pad_str(if s_fm(suffix) { 0 } else { -9 }, &String::from_utf8_lossy(&name))
                        .as_bytes(),
                );
            }
            DCH_MONTH_CAP => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.full_months[(tm.tm_mon - 1) as usize],
                        TmCaseMode::Init,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(
                    &mut out,
                    fmt_pad_str(
                        if s_fm(suffix) { 0 } else { -9 },
                        MONTHS_FULL[(tm.tm_mon - 1) as usize],
                    )
                    .as_bytes(),
                );
            }
            DCH_MONTH_LOWER => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.full_months[(tm.tm_mon - 1) as usize],
                        TmCaseMode::Lower,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                let name = asc_tolower(MONTHS_FULL[(tm.tm_mon - 1) as usize].as_bytes());
                pg_append(
                    &mut out,
                    fmt_pad_str(if s_fm(suffix) { 0 } else { -9 }, &String::from_utf8_lossy(&name))
                        .as_bytes(),
                );
            }
            DCH_MON => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.abbrev_months[(tm.tm_mon - 1) as usize],
                        TmCaseMode::Upper,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(&mut out, &asc_toupper(MONTHS[(tm.tm_mon - 1) as usize].as_bytes()));
            }
            DCH_MON_CAP => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.abbrev_months[(tm.tm_mon - 1) as usize],
                        TmCaseMode::Init,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(&mut out, MONTHS[(tm.tm_mon - 1) as usize].as_bytes());
            }
            DCH_MON_LOWER => {
                invalid_for_interval(is_interval)?;
                if tm.tm_mon == 0 {
                    continue;
                }
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.abbrev_months[(tm.tm_mon - 1) as usize],
                        TmCaseMode::Lower,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(&mut out, &asc_tolower(MONTHS[(tm.tm_mon - 1) as usize].as_bytes()));
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
                pg_append(&mut out, fmt_0d(width, tm.tm_mon as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_DAY => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.full_days[tm.tm_wday as usize],
                        TmCaseMode::Upper,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                let name = asc_toupper(DAYS[tm.tm_wday as usize].as_bytes());
                pg_append(
                    &mut out,
                    fmt_pad_str(if s_fm(suffix) { 0 } else { -9 }, &String::from_utf8_lossy(&name))
                        .as_bytes(),
                );
            }
            DCH_DAY_CAP => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.full_days[tm.tm_wday as usize],
                        TmCaseMode::Init,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(
                    &mut out,
                    fmt_pad_str(if s_fm(suffix) { 0 } else { -9 }, DAYS[tm.tm_wday as usize])
                        .as_bytes(),
                );
            }
            DCH_DAY_LOWER => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.full_days[tm.tm_wday as usize],
                        TmCaseMode::Lower,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                let name = asc_tolower(DAYS[tm.tm_wday as usize].as_bytes());
                pg_append(
                    &mut out,
                    fmt_pad_str(if s_fm(suffix) { 0 } else { -9 }, &String::from_utf8_lossy(&name))
                        .as_bytes(),
                );
            }
            DCH_DY => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.abbrev_days[tm.tm_wday as usize],
                        TmCaseMode::Upper,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(&mut out, &asc_toupper(DAYS_SHORT[tm.tm_wday as usize].as_bytes()));
            }
            DCH_DY_CAP => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.abbrev_days[tm.tm_wday as usize],
                        TmCaseMode::Init,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(&mut out, DAYS_SHORT[tm.tm_wday as usize].as_bytes());
            }
            DCH_DY_LOWER => {
                invalid_for_interval(is_interval)?;
                if s_tm(suffix) {
                    append_tm_localized(
                        mcx,
                        &mut out,
                        &localized.abbrev_days[tm.tm_wday as usize],
                        TmCaseMode::Lower,
                        collid,
                        key.len,
                    )?;
                    continue;
                }
                pg_append(&mut out, &asc_tolower(DAYS_SHORT[tm.tm_wday as usize].as_bytes()));
            }
            DCH_DDD | DCH_IDDD => {
                let width = if s_fm(suffix) { 0 } else { 3 };
                let v = if key.id == DCH_DDD {
                    tm.tm_yday
                } else {
                    date2isoyearday(tm.tm_year, tm.tm_mon, tm.tm_mday)
                };
                let start = out.len();
                pg_append(&mut out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_DD => {
                let width = if s_fm(suffix) { 0 } else { 2 };
                let start = out.len();
                pg_append(&mut out, fmt_0d(width, tm.tm_mday as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_D => {
                invalid_for_interval(is_interval)?;
                let start = out.len();
                pg_append(&mut out, fmt_d((tm.tm_wday + 1) as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_ID => {
                invalid_for_interval(is_interval)?;
                let v = if tm.tm_wday == 0 { 7 } else { tm.tm_wday };
                let start = out.len();
                pg_append(&mut out, fmt_d(v as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_WW => {
                let width = if s_fm(suffix) { 0 } else { 2 };
                let start = out.len();
                pg_append(&mut out, fmt_0d(width, ((tm.tm_yday - 1) / 7 + 1) as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_IW => {
                let width = if s_fm(suffix) { 0 } else { 2 };
                let start = out.len();
                pg_append(
                    &mut out,
                    fmt_0d(width, date2isoweek(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64).as_bytes(),
                );
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_Q => {
                if tm.tm_mon == 0 {
                    continue;
                }
                let start = out.len();
                pg_append(&mut out, fmt_d(((tm.tm_mon - 1) / 3 + 1) as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                    pg_append(&mut out, fmt_0d(width, i as i64).as_bytes());
                } else {
                    pg_append(&mut out, fmt_d(i as i64).as_bytes());
                }
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_Y_YYY => {
                let ay = adjust_year(tm.tm_year, is_interval);
                let i = ay / 1000;
                let start = out.len();
                pg_append(&mut out, format!("{},{:03}", i, ay - (i * 1000)).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                pg_append(&mut out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                pg_append(&mut out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                pg_append(&mut out, fmt_0d(width, v as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_Y | DCH_I => {
                let v = if key.id == DCH_Y {
                    adjust_year(tm.tm_year, is_interval)
                } else {
                    adjust_year(date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday), is_interval)
                } % 10;
                let start = out.len();
                pg_append(&mut out, fmt_d(v as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
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
                pg_append(
                    &mut out,
                    fmt_pad_str(if s_fm(suffix) { 0 } else { -4 }, months[mon as usize]).as_bytes(),
                );
            }
            DCH_W => {
                let start = out.len();
                pg_append(&mut out, fmt_d(((tm.tm_mday - 1) / 7 + 1) as i64).as_bytes());
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_J => {
                let start = out.len();
                pg_append(
                    &mut out,
                    fmt_d(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) as i64).as_bytes(),
                );
                apply_thth(&mut out, start, suffix)?;
            }
            DCH_FX => {}
            _ => {}
        }
    }

    Ok(out)
}

fn dch_fsec(out: &mut Vec<u8>, prec: usize, frac_val: i32, suffix: u8) -> PgResult<()> {
    let start = out.len();
    pg_append(out, fmt_0d(prec, frac_val as i64).as_bytes());
    apply_thth(out, start, suffix)
}

fn cstr_to_slice(buf: &[u8; MAX_MULTIBYTE_CHAR_LEN + 1]) -> &[u8] {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    &buf[..end]
}
