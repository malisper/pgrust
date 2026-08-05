//! DCH_from_char support: integer / sequential-search parsers and the
//! per-conversion mode/int setters (formatting.c:2074-2511).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use ::types_error::{ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_INVALID_DATETIME_FORMAT};

use crate::case::{pg_ascii_tolower, str_tolower, str_toupper};
use crate::parse::is_c_space;
use crate::tables::*;

fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    ereturn(escontext, (), err)
}

pub struct FromCharCursor<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
}

impl<'a> FromCharCursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        FromCharCursor { bytes, pos: 0 }
    }
    #[inline]
    pub fn cur(&self) -> u8 {
        if self.pos < self.bytes.len() {
            self.bytes[self.pos]
        } else {
            0
        }
    }
    #[inline]
    pub fn rest(&self) -> &'a [u8] {
        &self.bytes[self.pos.min(self.bytes.len())..]
    }
}

fn invalid_datetime(msg: impl Into<String>) -> PgError {
    PgError::error(msg.into()).with_sqlstate(ERRCODE_INVALID_DATETIME_FORMAT)
}

pub fn is_next_separator(nodes: &[FormatNode], idx: usize) -> bool {
    let n = &nodes[idx];
    if n.typ == NODE_TYPE_END {
        return false;
    }
    if n.typ == NODE_TYPE_ACTION && s_thth(n.suffix) {
        return true;
    }
    let nn = &nodes[idx + 1];
    if nn.typ == NODE_TYPE_END {
        return true;
    }
    if nn.typ == NODE_TYPE_ACTION {
        if DCH_KEYWORDS[nn.key as usize].is_digit {
            return false;
        }
        return true;
    } else if nn.character[1] == 0 && nn.character[0].is_ascii_digit() {
        return false;
    }
    true
}

pub fn adjust_partial_year_to_2020(year: i32) -> i32 {
    if year < 70 {
        year + 2000
    } else if year < 100 {
        year + 1900
    } else if year < 520 {
        year + 2000
    } else if year < 1000 {
        year + 1000
    } else {
        year
    }
}

pub fn strspace_len(bytes: &[u8]) -> usize {
    let mut len = 0;
    while len < bytes.len() && bytes[len] != 0 && is_c_space(bytes[len]) {
        len += 1;
    }
    len
}

pub fn from_char_set_mode(
    cur_mode: &mut FromCharDateMode,
    mode: FromCharDateMode,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if mode != FromCharDateMode::None {
        if *cur_mode == FromCharDateMode::None {
            *cur_mode = mode;
        } else if *cur_mode != mode {
            errsave(
                escontext,
                invalid_datetime("invalid combination of date conventions").with_hint(
                    "Do not mix Gregorian and ISO week date conventions in a formatting template.",
                ),
            )?;
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn from_char_set_int(
    dest: &mut i32,
    value: i32,
    node_name: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if *dest != 0 && *dest != value {
        errsave(
            escontext,
            invalid_datetime(format!(
                "conflicting values for \"{node_name}\" field in formatting string"
            ))
            .with_detail("This value contradicts a previous setting for the same field type."),
        )?;
        return Ok(false);
    }
    *dest = value;
    Ok(true)
}

pub fn from_char_parse_int_len(
    dest: Option<&mut i32>,
    cur: &mut FromCharCursor,
    len: usize,
    nodes: &[FormatNode],
    idx: usize,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<usize>> {
    let node = &nodes[idx];
    let node_name = DCH_KEYWORDS[node.key as usize].name;

    let init = cur.pos;
    cur.pos += strspace_len(cur.rest());
    debug_assert!(len <= DCH_MAX_ITEM_SIZ);

    let src_after_ws = cur.rest();
    let src_nul = src_after_ws
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(src_after_ws.len());
    let mut used = src_nul.min(len);
    let mut copy: Vec<u8> = Vec::new();
    copy.extend_from_slice(&src_after_ws[..used]);

    let result: i64;
    let erange: bool;

    if s_fm(node.suffix) || is_next_separator(nodes, idx) {
        let (val, end_off, rng) = strtol_from(cur.bytes, init);
        result = val;
        erange = rng;
        cur.pos = end_off;
    } else {
        if used < len {
            errsave(
                escontext.as_deref_mut(),
                invalid_datetime(format!(
                    "source string too short for \"{node_name}\" formatting field"
                ))
                .with_detail(format!(
                    "Field requires {len} characters, but only {used} remain."
                ))
                .with_hint(
                    "If your source string is not fixed-width, try using the \"FM\" modifier.",
                ),
            )?;
            return Ok(None);
        }

        let (val, consumed, rng) = strtol_from(&copy, 0);
        result = val;
        erange = rng;
        used = consumed;

        if used > 0 && used < len {
            errsave(
                escontext.as_deref_mut(),
                invalid_datetime(format!(
                    "invalid value \"{}\" for \"{}\"",
                    String::from_utf8_lossy(&copy),
                    node_name
                ))
                .with_detail(format!(
                    "Field requires {len} characters, but only {used} could be parsed."
                ))
                .with_hint(
                    "If your source string is not fixed-width, try using the \"FM\" modifier.",
                ),
            )?;
            return Ok(None);
        }

        cur.pos += used;
    }

    if cur.pos == init {
        errsave(
            escontext.as_deref_mut(),
            invalid_datetime(format!(
                "invalid value \"{}\" for \"{}\"",
                String::from_utf8_lossy(&copy),
                node_name
            ))
            .with_detail("Value must be an integer."),
        )?;
        return Ok(None);
    }

    if erange || result < i32::MIN as i64 || result > i32::MAX as i64 {
        errsave(
            escontext.as_deref_mut(),
            PgError::error(format!(
                "value for \"{node_name}\" in source string is out of range"
            ))
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
            .with_detail(format!(
                "Value must be in the range {} to {}.",
                i32::MIN,
                i32::MAX
            )),
        )?;
        return Ok(None);
    }

    if let Some(dest) = dest {
        if !from_char_set_int(dest, result as i32, node_name, escontext)? {
            return Ok(None);
        }
    }

    Ok(Some(cur.pos - init))
}

pub fn from_char_parse_int(
    dest: Option<&mut i32>,
    cur: &mut FromCharCursor,
    nodes: &[FormatNode],
    idx: usize,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<usize>> {
    let len = DCH_KEYWORDS[nodes[idx].key as usize].len;
    from_char_parse_int_len(dest, cur, len, nodes, idx, escontext)
}

/// strtol(base 10) over `bytes[start..]`; returns (value, end-offset, erange).
pub fn strtol_from(bytes: &[u8], start: usize) -> (i64, usize, bool) {
    let mut i = start;
    let n = bytes.len();
    let at = |i: usize| -> u8 {
        if i < n {
            bytes[i]
        } else {
            0
        }
    };
    while at(i) != 0 && is_c_space(at(i)) {
        i += 1;
    }
    let neg = match at(i) {
        b'-' => {
            i += 1;
            true
        }
        b'+' => {
            i += 1;
            false
        }
        _ => false,
    };
    let digit_start = i;
    let mut acc: i64 = 0;
    let mut erange = false;
    while at(i).is_ascii_digit() {
        let d = (at(i) - b'0') as i64;
        if !erange {
            match acc.checked_mul(10).and_then(|v| v.checked_add(d)) {
                Some(v) => acc = v,
                None => erange = true,
            }
        }
        i += 1;
    }
    if i == digit_start {
        return (0, start, false);
    }
    let val = if erange {
        if neg {
            i64::MIN
        } else {
            i64::MAX
        }
    } else if neg {
        -acc
    } else {
        acc
    };
    (val, i, erange)
}

pub fn seq_search_ascii(name: &[u8], array: &[&str]) -> (i32, usize) {
    if name.is_empty() || name[0] == 0 {
        return (-1, 0);
    }
    let firstc = pg_ascii_tolower(name[0]);

    for (ai, a) in array.iter().enumerate() {
        let ab = a.as_bytes();
        if pg_ascii_tolower(ab[0]) != firstc {
            continue;
        }
        let mut p = 1usize;
        let mut nn = 1usize;
        loop {
            if p >= ab.len() {
                return (ai as i32, nn);
            }
            if nn >= name.len() || name[nn] == 0 {
                break;
            }
            if pg_ascii_tolower(ab[p]) != pg_ascii_tolower(name[nn]) {
                break;
            }
            p += 1;
            nn += 1;
        }
    }
    (-1, 0)
}

pub fn seq_search_localized<'mcx>(
    mcx: Mcx<'mcx>,
    name: &[u8],
    array: &[Vec<u8>],
    collid: Oid,
) -> PgResult<(i32, usize)> {
    if name.is_empty() || name[0] == 0 {
        return Ok((-1, 0));
    }

    for (ai, a) in array.iter().enumerate() {
        let ab = a.as_slice();
        let element_len = ab.len();
        if name.len() >= element_len && &name[..element_len] == ab {
            return Ok((ai as i32, element_len));
        }
    }

    let upper_name = str_toupper(mcx, name, collid)?;
    let lower_name = str_tolower(mcx, &upper_name, collid)?;

    for (ai, a) in array.iter().enumerate() {
        let ab = a.as_slice();
        let upper_element = str_toupper(mcx, ab, collid)?;
        let lower_element = str_tolower(mcx, &upper_element, collid)?;
        let element_len = lower_element.len();

        if lower_name.len() >= element_len && lower_name[..element_len] == lower_element[..] {
            return Ok((ai as i32, element_len));
        }
    }

    Ok((-1, 0))
}

pub fn from_char_seq_search<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut i32,
    cur: &mut FromCharCursor,
    array: &[&str],
    localized_array: Option<&[Vec<u8>]>,
    collid: Oid,
    node_name: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    let (idx, len) = match localized_array {
        None => seq_search_ascii(cur.rest(), array),
        Some(arr) => seq_search_localized(mcx, cur.rest(), arr, collid)?,
    };
    *dest = idx;

    if len == 0 {
        let rest = cur.rest();
        let cut = rest
            .iter()
            .position(|&c| is_scanner_space(c))
            .unwrap_or_else(|| rest.iter().position(|&c| c == 0).unwrap_or(rest.len()));
        let copy = String::from_utf8_lossy(&rest[..cut]).into_owned();

        errsave(
            escontext,
            invalid_datetime(format!("invalid value \"{copy}\" for \"{node_name}\"")).with_detail(
                "The given value did not match any of the allowed values for this field.",
            ),
        )?;
        return Ok(false);
    }
    cur.pos += len;
    Ok(true)
}

#[inline]
pub fn is_scanner_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}
