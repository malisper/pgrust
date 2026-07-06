//! DCH_from_char consumer + DCH_datetime_type (formatting.c:3165-3831).
//! `TM` matches against the cache_locale_time localized names.

use ::mcx::Mcx;
use ::types_core::{InvalidOid, Oid};
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use ::types_error::{ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_INVALID_DATETIME_FORMAT};

use ::adt_datetime::MONTHS_PER_YEAR;

use crate::fromchar::{
    adjust_partial_year_to_2020, from_char_parse_int, from_char_parse_int_len,
    from_char_seq_search, from_char_set_int, from_char_set_mode, FromCharCursor,
};
use crate::parse::is_c_space;
use crate::tables::*;

fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    ereturn(escontext, (), err)
}

fn pg_mblen_cstr(s: &[u8]) -> i32 {
    mbutils::pg_mblen_range(s).unwrap_or(s.len() as i32)
}

/// C: `TmFromChar` (formatting.c:440). `abbrev` is inline (max TOKMAXLEN
/// bytes, the DecodeTimezoneAbbrevPrefix match cap) instead of C's pnstrdup.
#[derive(Clone, Default)]
pub struct TmFromChar {
    pub mode: FromCharDateMode,
    pub hh: i32,
    pub pm: i32,
    pub mi: i32,
    pub ss: i32,
    pub ssss: i32,
    pub d: i32,
    pub dd: i32,
    pub ddd: i32,
    pub mm: i32,
    pub ms: i32,
    pub year: i32,
    pub bc: i32,
    pub ww: i32,
    pub w: i32,
    pub cc: i32,
    pub j: i32,
    pub us: i32,
    pub yysz: i32,
    pub clock: i32,
    pub tzsign: i32,
    pub tzh: i32,
    pub tzm: i32,
    pub ff: i32,
    pub has_tz: bool,
    pub gmtoffset: i32,
    pub tzp: Option<&'static ::adt_datetime::tz::PgTz>,
    pub abbrev: [u8; ::adt_datetime::TOKMAXLEN],
    pub abbrev_len: u8,
}

fn skip_thth(cur: &mut FromCharCursor, suffix: u8) {
    if s_thth(suffix) {
        if cur.cur() != 0 {
            cur.pos += pg_mblen_cstr(cur.rest()) as usize;
        }
        if cur.cur() != 0 {
            cur.pos += pg_mblen_cstr(cur.rest()) as usize;
        }
    }
}

fn cstr_to_slice(buf: &[u8; MAX_MULTIBYTE_CHAR_LEN + 1]) -> &[u8] {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    &buf[..end]
}

fn parse_int(
    dest: &mut i32,
    cur: &mut FromCharCursor,
    nodes: &[FormatNode],
    idx: usize,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<usize>> {
    from_char_parse_int(Some(dest), cur, nodes, idx, escontext)
}

fn parse_int_len(
    dest: &mut i32,
    cur: &mut FromCharCursor,
    len: usize,
    nodes: &[FormatNode],
    idx: usize,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<usize>> {
    from_char_parse_int_len(Some(dest), cur, len, nodes, idx, escontext)
}

fn is_separator_char_input(cur: &FromCharCursor) -> bool {
    crate::case::is_separator_char(cur.cur())
}

/// C: `DCH_from_char` (formatting.c:3165).
#[allow(clippy::too_many_arguments)]
pub fn dch_from_char<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: &[FormatNode],
    in_: &[u8],
    out: &mut TmFromChar,
    collid: Oid,
    std: bool,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    // C: cache localized days and months (formatting.c:3178).
    let localized = pg_locale::cache_locale_time(mcx)?;
    let mut fx_mode = std;
    let mut extra_skip: i32 = 0;

    let mut cur = FromCharCursor::new(in_);
    let mut idx = 0usize;

    while nodes[idx].typ != NODE_TYPE_END && cur.cur() != 0 {
        let node_typ = nodes[idx].typ;
        let is_first = idx == 0;
        let node_is_fx =
            node_typ == NODE_TYPE_ACTION && DCH_KEYWORDS[nodes[idx].key as usize].id == DCH_FX;

        if !fx_mode && !node_is_fx && (node_typ == NODE_TYPE_ACTION || is_first) {
            while cur.cur() != 0 && is_c_space(cur.cur()) {
                cur.pos += 1;
                extra_skip += 1;
            }
        }

        if node_typ == NODE_TYPE_SPACE || node_typ == NODE_TYPE_SEPARATOR {
            if std {
                let ch = nodes[idx].character[0];
                if cur.cur() == ch {
                    cur.pos += 1;
                } else {
                    errsave(
                        escontext.as_deref_mut(),
                        PgError::error(format!("unmatched format separator \"{}\"", ch as char))
                            .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
                    )?;
                    return Ok(false);
                }
            } else if !fx_mode {
                extra_skip -= 1;
                if is_c_space(cur.cur()) || is_separator_char_input(&cur) {
                    cur.pos += 1;
                    extra_skip += 1;
                }
            } else {
                cur.pos += pg_mblen_cstr(cur.rest()) as usize;
            }
            idx += 1;
            continue;
        } else if node_typ != NODE_TYPE_ACTION {
            if !fx_mode {
                if extra_skip > 0 {
                    extra_skip -= 1;
                } else {
                    cur.pos += pg_mblen_cstr(cur.rest()) as usize;
                }
            } else {
                let chlen = pg_mblen_cstr(cur.rest()) as usize;
                if std && node_typ == NODE_TYPE_CHAR {
                    let nc = cstr_to_slice(&nodes[idx].character);
                    if cur.rest().len() < chlen || &cur.rest()[..chlen] != nc {
                        errsave(
                            escontext.as_deref_mut(),
                            PgError::error(format!(
                                "unmatched format character \"{}\"",
                                String::from_utf8_lossy(nc)
                            ))
                            .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
                        )?;
                        return Ok(false);
                    }
                }
                cur.pos += chlen;
            }
            idx += 1;
            continue;
        }

        let key_id = DCH_KEYWORDS[nodes[idx].key as usize].id;
        let date_mode = DCH_KEYWORDS[nodes[idx].key as usize].date_mode;
        let node_name = DCH_KEYWORDS[nodes[idx].key as usize].name;
        let suffix = nodes[idx].suffix;

        if !from_char_set_mode(&mut out.mode, date_mode, escontext.as_deref_mut())? {
            return Ok(false);
        }

        match key_id {
            DCH_FX => {
                fx_mode = true;
            }
            DCH_A_M | DCH_P_M | DCH_A_M_LOWER | DCH_P_M_LOWER => {
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &AMPM_STRINGS_LONG,
                    None,
                    InvalidOid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.pm, value % 2, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
                out.clock = CLOCK_12_HOUR;
            }
            DCH_AM | DCH_PM | DCH_AM_LOWER | DCH_PM_LOWER => {
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &AMPM_STRINGS,
                    None,
                    InvalidOid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.pm, value % 2, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
                out.clock = CLOCK_12_HOUR;
            }
            DCH_HH | DCH_HH12 => {
                if parse_int_len(&mut out.hh, &mut cur, 2, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
                out.clock = CLOCK_12_HOUR;
                skip_thth(&mut cur, suffix);
            }
            DCH_HH24 => {
                if parse_int_len(&mut out.hh, &mut cur, 2, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_MI => {
                if parse_int(&mut out.mi, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_SS => {
                if parse_int(&mut out.ss, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_MS => {
                let len =
                    match parse_int_len(&mut out.ms, &mut cur, 3, nodes, idx, escontext.as_deref_mut())? {
                        Some(l) => l,
                        None => return Ok(false),
                    };
                out.ms *= if len == 1 {
                    100
                } else if len == 2 {
                    10
                } else {
                    1
                };
                skip_thth(&mut cur, suffix);
            }
            DCH_FF1 | DCH_FF2 | DCH_FF3 | DCH_FF4 | DCH_FF5 | DCH_FF6 | DCH_US => {
                if (DCH_FF1..=DCH_FF6).contains(&key_id) {
                    out.ff = key_id - DCH_FF1 + 1;
                }
                let want = if key_id == DCH_US { 6 } else { out.ff } as usize;
                let len =
                    match parse_int_len(&mut out.us, &mut cur, want, nodes, idx, escontext.as_deref_mut())? {
                        Some(l) => l,
                        None => return Ok(false),
                    };
                out.us *= match len {
                    1 => 100000,
                    2 => 10000,
                    3 => 1000,
                    4 => 100,
                    5 => 10,
                    _ => 1,
                };
                skip_thth(&mut cur, suffix);
            }
            DCH_SSSS => {
                if parse_int(&mut out.ssss, &mut cur, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_TZ_LOWER | DCH_TZ | DCH_OF => {
                let mut fell_through = key_id == DCH_OF;
                if key_id == DCH_TZ_LOWER || key_id == DCH_TZ {
                    let mut gmtoffset = 0i32;
                    let mut tzp = None;
                    let tzlen = ::adt_datetime::DecodeTimezoneAbbrevPrefix(
                        cur.rest(),
                        &mut gmtoffset,
                        &mut tzp,
                    );
                    if tzlen > 0 {
                        out.has_tz = true;
                        out.gmtoffset = gmtoffset;
                        out.tzp = tzp;
                        if tzp.is_some() {
                            out.abbrev[..tzlen as usize]
                                .copy_from_slice(&cur.rest()[..tzlen as usize]);
                            out.abbrev_len = tzlen as u8;
                        }
                        out.tzsign = 0;
                        cur.pos += tzlen as usize;
                    } else if cur.cur().is_ascii_alphabetic() {
                        errsave(
                            escontext.as_deref_mut(),
                            PgError::error(format!(
                                "invalid value \"{}\" for \"{}\"",
                                String::from_utf8_lossy(cur.rest()),
                                node_name
                            ))
                            .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT)
                            .with_detail("Time zone abbreviation is not recognized."),
                        )?;
                        return Ok(false);
                    } else {
                        fell_through = true;
                    }
                }
                if fell_through {
                    if cur.cur() == b'+' || cur.cur() == b'-' || cur.cur() == b' ' {
                        out.tzsign = if cur.cur() == b'-' { -1 } else { 1 };
                        cur.pos += 1;
                    } else if extra_skip > 0 && cur.pos > 0 && cur.bytes[cur.pos - 1] == b'-' {
                        out.tzsign = -1;
                    } else {
                        out.tzsign = 1;
                    }
                    if parse_int_len(&mut out.tzh, &mut cur, 2, nodes, idx, escontext.as_deref_mut())?
                        .is_none()
                    {
                        return Ok(false);
                    }
                    if cur.cur() == b':' {
                        cur.pos += 1;
                        if parse_int_len(&mut out.tzm, &mut cur, 2, nodes, idx, escontext.as_deref_mut())?
                            .is_none()
                        {
                            return Ok(false);
                        }
                    }
                }
            }
            DCH_TZH => {
                if cur.cur() == b'+' || cur.cur() == b'-' || cur.cur() == b' ' {
                    out.tzsign = if cur.cur() == b'-' { -1 } else { 1 };
                    cur.pos += 1;
                } else if extra_skip > 0 && cur.pos > 0 && cur.bytes[cur.pos - 1] == b'-' {
                    out.tzsign = -1;
                } else {
                    out.tzsign = 1;
                }
                if parse_int_len(&mut out.tzh, &mut cur, 2, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
            }
            DCH_TZM => {
                if out.tzsign == 0 {
                    out.tzsign = 1;
                }
                if parse_int_len(&mut out.tzm, &mut cur, 2, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
            }
            DCH_A_D | DCH_B_C | DCH_A_D_LOWER | DCH_B_C_LOWER => {
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &ADBC_STRINGS_LONG,
                    None,
                    InvalidOid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.bc, value % 2, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
            }
            DCH_AD | DCH_BC | DCH_AD_LOWER | DCH_BC_LOWER => {
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &ADBC_STRINGS,
                    None,
                    InvalidOid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.bc, value % 2, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
            }
            DCH_MONTH | DCH_MONTH_CAP | DCH_MONTH_LOWER => {
                let localized_arr = if s_tm(suffix) {
                    Some(localized.full_months.as_slice())
                } else {
                    None
                };
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &MONTHS_FULL,
                    localized_arr,
                    collid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.mm, value + 1, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
            }
            DCH_MON | DCH_MON_CAP | DCH_MON_LOWER => {
                let localized_arr = if s_tm(suffix) {
                    Some(localized.abbrev_months.as_slice())
                } else {
                    None
                };
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &MONTHS,
                    localized_arr,
                    collid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.mm, value + 1, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
            }
            DCH_MM => {
                if parse_int(&mut out.mm, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_DAY | DCH_DAY_CAP | DCH_DAY_LOWER => {
                let localized_arr = if s_tm(suffix) {
                    Some(localized.full_days.as_slice())
                } else {
                    None
                };
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &DAYS,
                    localized_arr,
                    collid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.d, value, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
                out.d += 1;
            }
            DCH_DY | DCH_DY_CAP | DCH_DY_LOWER => {
                let localized_arr = if s_tm(suffix) {
                    Some(localized.abbrev_days.as_slice())
                } else {
                    None
                };
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &DAYS_SHORT,
                    localized_arr,
                    collid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(&mut out.d, value, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
                out.d += 1;
            }
            DCH_DDD => {
                if parse_int(&mut out.ddd, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_IDDD => {
                if parse_int_len(&mut out.ddd, &mut cur, 3, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_DD => {
                if parse_int(&mut out.dd, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_D => {
                if parse_int(&mut out.d, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none() {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_ID => {
                if parse_int_len(&mut out.d, &mut cur, 1, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
                out.d += 1;
                if out.d > 7 {
                    out.d = 1;
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_WW | DCH_IW => {
                if parse_int(&mut out.ww, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_Q => {
                if from_char_parse_int(None, &mut cur, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_CC => {
                if parse_int(&mut out.cc, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none()
                {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_Y_YYY => {
                let (millennia, years0, nch) = match parse_y_yyy(cur.rest()) {
                    Some(t) => t,
                    None => {
                        errsave(
                            escontext.as_deref_mut(),
                            PgError::error(format!(
                                "invalid value \"{}\" for \"{}\"",
                                String::from_utf8_lossy(cur.rest()),
                                "Y,YYY"
                            ))
                            .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
                        )?;
                        return Ok(false);
                    }
                };
                let years = match millennia
                    .checked_mul(1000)
                    .and_then(|m| years0.checked_add(m))
                {
                    Some(v) => v,
                    None => {
                        errsave(
                            escontext.as_deref_mut(),
                            PgError::error(
                                "value for \"Y,YYY\" in source string is out of range".to_string(),
                            )
                            .with_sqlstate(ERRCODE_DATETIME_FIELD_OVERFLOW),
                        )?;
                        return Ok(false);
                    }
                };
                if !from_char_set_int(&mut out.year, years, node_name, escontext.as_deref_mut())? {
                    return Ok(false);
                }
                out.yysz = 4;
                cur.pos += nch;
                skip_thth(&mut cur, suffix);
            }
            DCH_YYYY | DCH_IYYY => {
                if parse_int(&mut out.year, &mut cur, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
                {
                    return Ok(false);
                }
                out.yysz = 4;
                skip_thth(&mut cur, suffix);
            }
            DCH_YYY | DCH_IYY => {
                let len =
                    match parse_int(&mut out.year, &mut cur, nodes, idx, escontext.as_deref_mut())? {
                        Some(l) => l,
                        None => return Ok(false),
                    };
                if len < 4 {
                    out.year = adjust_partial_year_to_2020(out.year);
                }
                out.yysz = 3;
                skip_thth(&mut cur, suffix);
            }
            DCH_YY | DCH_IY => {
                let len =
                    match parse_int(&mut out.year, &mut cur, nodes, idx, escontext.as_deref_mut())? {
                        Some(l) => l,
                        None => return Ok(false),
                    };
                if len < 4 {
                    out.year = adjust_partial_year_to_2020(out.year);
                }
                out.yysz = 2;
                skip_thth(&mut cur, suffix);
            }
            DCH_Y | DCH_I => {
                let len =
                    match parse_int(&mut out.year, &mut cur, nodes, idx, escontext.as_deref_mut())? {
                        Some(l) => l,
                        None => return Ok(false),
                    };
                if len < 4 {
                    out.year = adjust_partial_year_to_2020(out.year);
                }
                out.yysz = 1;
                skip_thth(&mut cur, suffix);
            }
            DCH_RM | DCH_RM_LOWER => {
                let mut value = 0;
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &RM_MONTHS_LOWER,
                    None,
                    InvalidOid,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
                if !from_char_set_int(
                    &mut out.mm,
                    MONTHS_PER_YEAR - value,
                    node_name,
                    escontext.as_deref_mut(),
                )? {
                    return Ok(false);
                }
            }
            DCH_W => {
                if parse_int(&mut out.w, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none() {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            DCH_J => {
                if parse_int(&mut out.j, &mut cur, nodes, idx, escontext.as_deref_mut())?.is_none() {
                    return Ok(false);
                }
                skip_thth(&mut cur, suffix);
            }
            _ => {}
        }

        if !fx_mode {
            extra_skip = 0;
            while cur.cur() != 0 && is_c_space(cur.cur()) {
                cur.pos += 1;
                extra_skip += 1;
            }
        }

        idx += 1;
    }

    if std {
        if nodes[idx].typ != NODE_TYPE_END {
            errsave(
                escontext.as_deref_mut(),
                PgError::error("input string is too short for datetime format".to_string())
                    .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
            )?;
            return Ok(false);
        }
        while cur.cur() != 0 && is_c_space(cur.cur()) {
            cur.pos += 1;
        }
        if cur.cur() != 0 {
            errsave(
                escontext,
                PgError::error(
                    "trailing characters remain in input string after datetime format".to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT),
            )?;
            return Ok(false);
        }
    }

    Ok(true)
}

/// C: `sscanf(s, "%d,%03d%n", ...)` with `matched >= 2`.
fn parse_y_yyy(s: &[u8]) -> Option<(i32, i32, usize)> {
    let mut i = 0usize;
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }
    let (mil, ni) = scan_signed_int(s, i, None)?;
    i = ni;
    if i >= s.len() || s[i] != b',' {
        return None;
    }
    i += 1;
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }
    let (yrs, ni) = scan_signed_int(s, i, Some(3))?;
    i = ni;
    Some((mil, yrs, i))
}

fn scan_signed_int(s: &[u8], start: usize, max_width: Option<usize>) -> Option<(i32, usize)> {
    let mut i = start;
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') && max_width.is_none_or(|max| max > 0) {
        neg = s[i] == b'-';
        i += 1;
    }
    let ds = i;
    let mut acc: i64 = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        if let Some(max) = max_width {
            if i - start >= max {
                break;
            }
        }
        acc = acc * 10 + (s[i] - b'0') as i64;
        if acc > i32::MAX as i64 + 1 {
            acc = i32::MAX as i64 + 1;
        }
        i += 1;
    }
    if i == ds {
        return None;
    }
    let v = if neg { -acc } else { acc };
    Some((v as i32, i))
}

/// C: `DCH_datetime_type` (formatting.c:3737).
pub fn dch_datetime_type(nodes: &[FormatNode]) -> i32 {
    let mut flags = 0;
    for n in nodes.iter() {
        if n.typ == NODE_TYPE_END {
            break;
        }
        if n.typ != NODE_TYPE_ACTION {
            continue;
        }
        match DCH_KEYWORDS[n.key as usize].id {
            DCH_FX => {}
            DCH_A_M | DCH_P_M | DCH_A_M_LOWER | DCH_P_M_LOWER | DCH_AM | DCH_PM | DCH_AM_LOWER
            | DCH_PM_LOWER | DCH_HH | DCH_HH12 | DCH_HH24 | DCH_MI | DCH_SS | DCH_MS | DCH_US
            | DCH_FF1 | DCH_FF2 | DCH_FF3 | DCH_FF4 | DCH_FF5 | DCH_FF6 | DCH_SSSS => {
                flags |= DCH_TIMED;
            }
            DCH_TZ_LOWER | DCH_TZ | DCH_OF | DCH_TZH | DCH_TZM => {
                flags |= DCH_ZONED;
            }
            DCH_A_D | DCH_B_C | DCH_A_D_LOWER | DCH_B_C_LOWER | DCH_AD | DCH_BC | DCH_AD_LOWER
            | DCH_BC_LOWER | DCH_MONTH | DCH_MONTH_CAP | DCH_MONTH_LOWER | DCH_MON | DCH_MON_CAP
            | DCH_MON_LOWER | DCH_MM | DCH_DAY | DCH_DAY_CAP | DCH_DAY_LOWER | DCH_DY | DCH_DY_CAP
            | DCH_DY_LOWER | DCH_DDD | DCH_IDDD | DCH_DD | DCH_D | DCH_ID | DCH_WW | DCH_Q
            | DCH_CC | DCH_Y_YYY | DCH_YYYY | DCH_IYYY | DCH_YYY | DCH_IYY | DCH_YY | DCH_IY
            | DCH_Y | DCH_I | DCH_RM | DCH_RM_LOWER | DCH_W | DCH_J => {
                flags |= DCH_DATED;
            }
            _ => {}
        }
    }
    flags
}
