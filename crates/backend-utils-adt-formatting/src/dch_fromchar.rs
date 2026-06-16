//! Date/time format-picture engine — the `from_char` *consumer* half:
//! `DCH_from_char` and `DCH_datetime_type`.
//!
//! Faithful idiomatic port of formatting.c:3165-3831 (PG 18.3).
//!
//! ## Scope of this module
//!
//! Given a parsed format picture (`&[FormatNode]`) and an input byte string,
//! [`dch_from_char`] walks the nodes and the input together, filling a
//! [`TmFromChar`] with the decoded calendar/clock fields (the inverse of the
//! [`crate::dch::dch_to_char`] producer). [`dch_datetime_type`] classifies a
//! format as `DATED` / `TIMED` / `ZONED`.
//!
//! All the from-char parse primitives (`from_char_parse_int{,_len}`,
//! `from_char_seq_search`, `from_char_set_int`/`from_char_set_mode`,
//! `adjust_partial_year_to_2020`) live in [`crate::fromchar`]; the format-node
//! tables and the suffix helpers live in [`crate::tables`].
//!
//! ## Seams
//!
//! `DCH_from_char` reaches into two sibling subsystems, routed through the
//! centralized `seams::backend_utils_adt_formatting` slots:
//!   * pg_locale.c — `cache_locale_time` and the localized day/month-name
//!     accessors (used by the `TM`-prefixed `Month`/`Day`/… fields);
//!   * mbutils.c — `pg_mblen` (multibyte char length), used to advance the
//!     cursor over text/space nodes in FX mode;
//!   * datetime.c — `decode_timezone_abbrev_prefix`, the timezone-abbreviation
//!     lookup for the `TZ`/`tz` fields.
//!
//! ## `pg_tz` handle
//!
//! C's `TmFromChar.tzp` is a `pg_tz *` into the timezone subsystem
//! (`backend_timezone_localtime`, a separate sibling port). The idiomatic
//! surface does not name that pointer; instead the timezone seam returns an
//! owned [`types::backend_utils_adt_formatting::TzHandle`] id, which the DCH
//! consumer stores in `TmFromChar.tzp` and the (later-ported) `do_to_timestamp`
//! driver hands back to the timezone provider to resolve the offset.

use mcx::Mcx;
use types_error::{PgError, PgResult, SoftErrorContext};
use types_datetime::{TzAbbrevMatch, TzHandle};
use types_datetime::MONTHS_PER_YEAR;
use types_error::{ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_INVALID_DATETIME_FORMAT};
use types_core::{InvalidOid, Oid};

use crate::fromchar::{
    adjust_partial_year_to_2020, from_char_parse_int, from_char_parse_int_len,
    from_char_seq_search, from_char_set_int, from_char_set_mode, FromCharCursor,
};
use crate::parse::is_c_space;
use crate::tables::*;

/// Local `errsave` helper mirroring C's `errsave(escontext, ...)`: routes a
/// complete [`PgError`] through the shared soft-error context discipline.
fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    types_error::ereturn(escontext, (), err)
}

// ---------------------------------------------------------------------------
// Seam wrappers (genuine cross-subsystem calls).
// ---------------------------------------------------------------------------

/// C: `cache_locale_time()` via the per-owner seam.
fn cache_locale_time() -> PgResult<()> {
    backend_utils_adt_pg_locale_seams::cache_locale_time::call()
}

/// The localized-name seams return arena-allocated `PgVec<PgVec<u8>>`; the DCH
/// engine consumes them as plain `Vec<Vec<u8>>` (NUL-free owned names).
fn pgvec_of_pgvec_to_vec(a: mcx::PgVec<'_, mcx::PgVec<'_, u8>>) -> Vec<Vec<u8>> {
    a.iter().map(|e| e.to_vec()).collect()
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

/// C: `pg_mblen(s)` over a NUL-terminated cursor remainder, via the per-owner
/// mbutils seam: the byte length of the multibyte character whose lead byte is
/// `s[0]`. Infallible.
fn pg_mblen_cstr(s: &[u8]) -> i32 {
    backend_utils_mb_mbutils_seams::pg_mblen_range::call(s)
}

/// C: `DecodeTimezoneAbbrevPrefix(str, &gmtoffset, &tzp)` via the per-owner
/// datetime seam.
fn decode_timezone_abbrev_prefix(s: &[u8]) -> TzAbbrevMatch {
    backend_utils_adt_datetime_seams::decode_timezone_abbrev_prefix::call(s)
}

// ---------------------------------------------------------------------------
// TmFromChar (formatting.c:440)
// ---------------------------------------------------------------------------

/// C: `TmFromChar` (formatting.c:440) — the char->date/time accumulator filled
/// by [`dch_from_char`].
///
/// The C struct's `pg_tz *tzp` is carried here as a
/// [`TzHandle`] (an owned timezone id), and `char *abbrev` as an owned
/// `Option<String>`; see the module docs.
#[derive(Clone, Debug, Default)]
pub struct TmFromChar {
    pub mode: FromCharDateMode,
    pub hh: i32,
    pub pm: i32,
    pub mi: i32,
    pub ss: i32,
    pub ssss: i32,
    /// stored as 1-7, Sunday = 1, 0 means missing
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
    /// is it YY or YYYY ?
    pub yysz: i32,
    /// 12 or 24 hour clock?
    pub clock: i32,
    /// +1, -1, or 0 if no TZH/TZM fields
    pub tzsign: i32,
    pub tzh: i32,
    pub tzm: i32,
    /// fractional precision
    pub ff: i32,
    /// was there a TZ field?
    pub has_tz: bool,
    /// GMT offset of fixed-offset zone abbrev
    pub gmtoffset: i32,
    /// `pg_tz` handle for a dynamic abbrev
    pub tzp: Option<TzHandle>,
    /// dynamic abbrev
    pub abbrev: Option<String>,
}

// ---------------------------------------------------------------------------
// Local helpers.
// ---------------------------------------------------------------------------

/// C: `SKIP_THth(ptr, _suf)` (formatting.c:2027).
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

fn localized_months_full_vec<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    loc_full_months(mcx)
}
fn localized_months_abbrev_vec<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    loc_abbrev_months(mcx)
}
fn localized_days_full_vec<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    loc_full_days(mcx)
}
fn localized_days_abbrev_vec<'mcx>(mcx: Mcx<'mcx>) -> Option<Vec<Vec<u8>>> {
    loc_abbrev_days(mcx)
}

/// Read a NUL-terminated keyword-table `character` field as a byte slice.
fn cstr_to_slice(buf: &[u8; MAX_MULTIBYTE_CHAR_LEN + 1]) -> &[u8] {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    &buf[..end]
}

/// Wrapper over `from_char_parse_int` taking a `&mut i32` (the C `dest`).
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

/// `is_separator_char` over the cursor's current byte (formatting.c:`is_separator_char`).
fn is_separator_char_input(cur: &FromCharCursor) -> bool {
    crate::case::is_separator_char(cur.cur())
}

/// C: `DCH_from_char` (formatting.c:3165).
///
/// Returns `Ok(true)` on success, `Ok(false)` if a soft error was recorded in
/// `escontext` (mirroring C's early `return` after `ereturn`), or `Err` on a
/// hard error.
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
    cache_locale_time()?;

    let mut fx_mode = std;
    let mut extra_skip: i32 = 0;

    let mut cur = FromCharCursor::new(in_);
    let mut idx = 0usize;

    while nodes[idx].typ != NODE_TYPE_END && cur.cur() != 0 {
        let node_typ = nodes[idx].typ;
        let is_first = idx == 0;
        let node_is_fx = node_typ == NODE_TYPE_ACTION
            && DCH_KEYWORDS[nodes[idx].key as usize].id == DCH_FX;

        // Ignore spaces at the beginning / before fields when not in FX mode.
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
                        PgError::error(format!(
                            "unmatched format separator \"{}\"",
                            ch as char
                        ))
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
            // Text character.
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

        // NODE_TYPE_ACTION.
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
                let len = match parse_int_len(
                    &mut out.ms,
                    &mut cur,
                    3,
                    nodes,
                    idx,
                    escontext.as_deref_mut(),
                )? {
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
                let len = match parse_int_len(
                    &mut out.us,
                    &mut cur,
                    want,
                    nodes,
                    idx,
                    escontext.as_deref_mut(),
                )? {
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
                // DCH_tz / DCH_TZ try the abbrev prefix first, then fall through
                // to OF parsing.
                let mut fell_through = key_id == DCH_OF;
                if key_id == DCH_TZ_LOWER || key_id == DCH_TZ {
                    let m = decode_timezone_abbrev_prefix(cur.rest());
                    if m.tzlen > 0 {
                        out.has_tz = true;
                        if let Some(tzp) = m.tzp {
                            out.tzp = Some(tzp);
                            out.abbrev = Some(
                                String::from_utf8_lossy(&cur.rest()[..m.tzlen as usize])
                                    .into_owned(),
                            );
                        }
                        out.gmtoffset = m.gmtoffset;
                        out.tzsign = 0;
                        cur.pos += m.tzlen as usize;
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
                    // OF is equivalent to TZH or TZH:TZM.
                    if cur.cur() == b'+' || cur.cur() == b'-' || cur.cur() == b' ' {
                        out.tzsign = if cur.cur() == b'-' { -1 } else { 1 };
                        cur.pos += 1;
                    } else if extra_skip > 0 && cur.pos > 0 && cur.bytes[cur.pos - 1] == b'-' {
                        out.tzsign = -1;
                    } else {
                        out.tzsign = 1;
                    }
                    if parse_int_len(
                        &mut out.tzh,
                        &mut cur,
                        2,
                        nodes,
                        idx,
                        escontext.as_deref_mut(),
                    )?
                    .is_none()
                    {
                        return Ok(false);
                    }
                    if cur.cur() == b':' {
                        cur.pos += 1;
                        if parse_int_len(
                            &mut out.tzm,
                            &mut cur,
                            2,
                            nodes,
                            idx,
                            escontext.as_deref_mut(),
                        )?
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
                let mut value = 0;
                let localized = if s_tm(suffix) {
                    localized_months_full_vec(mcx)
                } else {
                    None
                };
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &MONTHS_FULL,
                    localized.as_deref(),
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
                let mut value = 0;
                let localized = if s_tm(suffix) {
                    localized_months_abbrev_vec(mcx)
                } else {
                    None
                };
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &MONTHS,
                    localized.as_deref(),
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
                let mut value = 0;
                let localized = if s_tm(suffix) {
                    localized_days_full_vec(mcx)
                } else {
                    None
                };
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &DAYS,
                    localized.as_deref(),
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
                let mut value = 0;
                let localized = if s_tm(suffix) {
                    localized_days_abbrev_vec(mcx)
                } else {
                    None
                };
                if !from_char_seq_search(
                    mcx,
                    &mut value,
                    &mut cur,
                    &DAYS_SHORT,
                    localized.as_deref(),
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
                if parse_int(&mut out.ddd, &mut cur, nodes, idx, escontext.as_deref_mut())?
                    .is_none()
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
                // C passes `(int *) NULL`: the integer is parsed but discarded.
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
                // sscanf(s, "%d,%03d%n", &millennia, &years, &nch); matched < 2
                let parsed = parse_y_yyy(cur.rest());
                let (millennia, years0, nch) = match parsed {
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
                // C: pg_mul_s32_overflow(millennia, 1000, &millennia) ||
                //    pg_add_s32_overflow(years, millennia, &years)
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
                let len = match parse_int(
                    &mut out.year,
                    &mut cur,
                    nodes,
                    idx,
                    escontext.as_deref_mut(),
                )? {
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
                let len = match parse_int(
                    &mut out.year,
                    &mut cur,
                    nodes,
                    idx,
                    escontext.as_deref_mut(),
                )? {
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
                let len = match parse_int(
                    &mut out.year,
                    &mut cur,
                    nodes,
                    idx,
                    escontext.as_deref_mut(),
                )? {
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

        // Ignore all spaces after fields (non-FX).
        if !fx_mode {
            extra_skip = 0;
            while cur.cur() != 0 && is_c_space(cur.cur()) {
                cur.pos += 1;
                extra_skip += 1;
            }
        }

        idx += 1;
    }

    // Standard parsing mode: no unmatched patterns / trailing chars allowed.
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

/// C: `sscanf(s, "%d,%03d%n", &millennia, &years, &nch)` with `matched >= 2`.
/// Returns (millennia, years, bytes-consumed) when both integers parse.
///
/// Mirrors C's `scanf` semantics exactly:
/// - each `%d` (and `%03d`) conversion first skips leading whitespace;
/// - the unwidthed `%d` reads digits without a cap;
/// - the `%03d` field is capped at 3 digits (the width is a *maximum*).
fn parse_y_yyy(s: &[u8]) -> Option<(i32, i32, usize)> {
    // %d : skip leading ws, optional sign, unbounded digits.
    let mut i = 0usize;
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }
    let (mil, ni) = scan_signed_int(s, i, None)?;
    i = ni;
    // literal ','
    if i >= s.len() || s[i] != b',' {
        return None;
    }
    i += 1;
    // %03d : skip leading ws, then consume at most a 3-character field
    // (the width includes the optional sign, per C scanf semantics).
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }
    let (yrs, ni) = scan_signed_int(s, i, Some(3))?;
    i = ni;
    Some((mil, yrs, i))
}

/// Scan a C `scanf`-style signed integer starting at `start`.  When
/// `max_width` is `Some(n)`, at most `n` characters are consumed by the
/// conversion (matching a `%nd` field width, which counts the optional sign);
/// `None` means unbounded (a plain `%d`).
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
            acc = i32::MAX as i64 + 1; // clamp; overflow handled by caller via mul
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

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    // The numeric/ASCII from-char paths exercised here call `cache_locale_time`
    // (a no-op when LC_TIME is C) and `pg_mblen` (to advance over text/space
    // nodes). The localized-name and timezone-abbrev seams are not exercised by
    // these tests (no `s_tm` suffix, no `TZ`/`tz` field). We install minimal
    // single-byte/no-op stubs for the two that the numeric path reaches.
    fn install_test_seams() {
        crate::install_test_seams();
    }

    fn node_action(id: i32, suffix: u8) -> FormatNode {
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

    #[test]
    fn from_char_ymd_hms() {
        install_test_seams();
        // YYYY-MM-DD HH24:MI:SS
        let nodes = vec![
            node_action(DCH_YYYY, 0),
            node_char(b'-'),
            node_action(DCH_MM, 0),
            node_char(b'-'),
            node_action(DCH_DD, 0),
            node_char(b' '),
            node_action(DCH_HH24, 0),
            node_char(b':'),
            node_action(DCH_MI, 0),
            node_char(b':'),
            node_action(DCH_SS, 0),
            node_end(),
        ];
        let ctx = MemoryContext::new("dch-test");
        let mut out = TmFromChar::default();
        let ok = dch_from_char(ctx.mcx(), &nodes, b"2020-07-14 13:05:09", &mut out, 0u32, false, None).unwrap();
        assert!(ok);
        assert_eq!(out.year, 2020);
        assert_eq!(out.mm, 7);
        assert_eq!(out.dd, 14);
        assert_eq!(out.hh, 13);
        assert_eq!(out.mi, 5);
        assert_eq!(out.ss, 9);
    }

    #[test]
    fn from_char_month_name_ascii() {
        install_test_seams();
        // Month DD, YYYY -> ASCII month name (no TM suffix).
        let nodes = vec![
            node_action(DCH_MONTH_CAP, 0),
            node_char(b' '),
            node_action(DCH_DD, 0),
            node_char(b','),
            node_char(b' '),
            node_action(DCH_YYYY, 0),
            node_end(),
        ];
        let ctx = MemoryContext::new("dch-test");
        let mut out = TmFromChar::default();
        let ok = dch_from_char(ctx.mcx(), &nodes, b"January 08, 1999", &mut out, 0u32, false, None).unwrap();
        assert!(ok);
        assert_eq!(out.mm, 1);
        assert_eq!(out.dd, 8);
        assert_eq!(out.year, 1999);
    }

    #[test]
    fn from_char_hh12_pm() {
        install_test_seams();
        // HH12:MI:SS AM
        let nodes = vec![
            node_action(DCH_HH12, 0),
            node_char(b':'),
            node_action(DCH_MI, 0),
            node_char(b':'),
            node_action(DCH_SS, 0),
            node_char(b' '),
            node_action(DCH_AM, 0),
            node_end(),
        ];
        let ctx = MemoryContext::new("dch-test");
        let mut out = TmFromChar::default();
        let ok = dch_from_char(ctx.mcx(), &nodes, b"01:05:09 PM", &mut out, 0u32, false, None).unwrap();
        assert!(ok);
        assert_eq!(out.hh, 1);
        assert_eq!(out.clock, CLOCK_12_HOUR);
        assert_eq!(out.pm, 1);
    }

    #[test]
    fn datetime_type_classifies() {
        // YYYY-MM-DD -> DATED only.
        let dated = vec![
            node_action(DCH_YYYY, 0),
            node_action(DCH_MM, 0),
            node_action(DCH_DD, 0),
            node_end(),
        ];
        assert_eq!(dch_datetime_type(&dated), DCH_DATED);

        // HH24:MI:SS TZ -> TIMED | ZONED.
        let timed_zoned = vec![
            node_action(DCH_HH24, 0),
            node_action(DCH_MI, 0),
            node_action(DCH_SS, 0),
            node_action(DCH_TZ, 0),
            node_end(),
        ];
        assert_eq!(dch_datetime_type(&timed_zoned), DCH_TIMED | DCH_ZONED);
    }
}
