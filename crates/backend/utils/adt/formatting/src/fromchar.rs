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

// upstream 011384ba45fe (18.6): Fix calculating length of match to localized month/weekday names
// C MAX_L10N_DATA: fixed fold buffers; a fold that would not fit is no match.
const MAX_L10N_DATA: usize = 80;

// C casefold_str_cmp: `name` folded upper-then-lower equals the folded `element`.
fn casefold_str_cmp<'mcx>(
    mcx: Mcx<'mcx>,
    name: &[u8],
    element: &[u8],
    mylocale: &::pg_locale::PgLocale,
) -> PgResult<bool> {
    let mut upper = [0u8; MAX_L10N_DATA];
    let upper_len = ::pg_locale::pg_strupper(mcx, &mut upper, name, mylocale)?;
    if upper_len > MAX_L10N_DATA - 1 {
        return Ok(false);
    }
    let mut lower = [0u8; MAX_L10N_DATA];
    let lower_len = ::pg_locale::pg_strlower(mcx, &mut lower, &upper[..upper_len], mylocale)?;
    if lower_len > MAX_L10N_DATA - 1 {
        return Ok(false);
    }
    Ok(&lower[..lower_len] == element)
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
    let name_len = name.len();

    for (ai, a) in array.iter().enumerate() {
        let ab = a.as_slice();
        let element_len = ab.len();
        if name.len() >= element_len && &name[..element_len] == ab {
            return Ok((ai as i32, element_len));
        }
    }

    let mylocale = ::pg_locale::pg_newlocale_from_collation(collid)?;

    let upper_name = str_toupper(mcx, name, collid)?;
    let lower_name = str_tolower(mcx, &upper_name, collid)?;

    for (ai, a) in array.iter().enumerate() {
        let ab = a.as_slice();
        let mut upper_element = [0u8; MAX_L10N_DATA];
        let upper_element_len = ::pg_locale::pg_strupper(mcx, &mut upper_element, ab, mylocale)?;
        if upper_element_len > MAX_L10N_DATA - 1 {
            continue;
        }
        let mut lower_element = [0u8; MAX_L10N_DATA];
        let lower_element_len = ::pg_locale::pg_strlower(
            mcx,
            &mut lower_element,
            &upper_element[..upper_element_len],
            mylocale,
        )?;
        if lower_element_len > MAX_L10N_DATA - 1 {
            continue;
        }
        let lower_element = &lower_element[..lower_element_len];

        if lower_name.len() < lower_element_len || &lower_name[..lower_element_len] != lower_element
        {
            continue;
        }
        // A match; the folds may have changed either length: recover it in the original.
        if lower_name.len() == lower_element_len {
            return Ok((ai as i32, name_len));
        }
        // Best guess: the folds kept the character count.
        let mut element_nchars = 0usize;
        let mut ep = 0usize;
        while ep < lower_element_len {
            ep += ::mbutils::pg_mblen_range(&lower_element[ep..])? as usize;
            element_nchars += 1;
        }
        let mut substr_len = 0usize;
        let mut substr_nchars = 0usize;
        while substr_nchars < element_nchars && substr_len < name_len {
            substr_len += ::mbutils::pg_mblen_range(&name[substr_len..])? as usize;
            substr_nchars += 1;
        }
        if casefold_str_cmp(mcx, &name[..substr_len], lower_element, mylocale)? {
            return Ok((ai as i32, substr_len));
        }
        // Last resort: every prefix of the original, shortest first.
        substr_len = 0;
        while substr_len < name_len {
            substr_len += ::mbutils::pg_mblen_range(&name[substr_len..])? as usize;
            if casefold_str_cmp(mcx, &name[..substr_len], lower_element, mylocale)? {
                return Ok((ai as i32, substr_len));
            }
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
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

#[cfg(test)]
mod tests {
    use super::*;

    // upstream 011384ba45fe (18.6): the length is the ORIGINAL input's matching prefix;
    // Turkish ı (U+0131, 2 bytes) upper-folds to ASCII I under a non-Turkish ctype.
    #[test]
    fn seq_search_localized_returns_original_prefix_length() {
        ::pg_locale::set_default_locale_builtin_utf8_for_tests();
        ::mbutils::SetDatabaseEncoding(::wchar::PG_UTF8).unwrap();
        let ctx = ::mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let months: Vec<Vec<u8>> = vec![b"Ocak".to_vec(), "Aralık".as_bytes().to_vec()];
        let search = |name: &str| {
            seq_search_localized(
                mcx,
                name.as_bytes(),
                &months,
                ::types_core::DEFAULT_COLLATION_OID,
            )
            .unwrap()
        };
        assert_eq!(search("Aralık 2010"), (1, 7));
        // case-folded matches: 7 bytes of the original, not the 6 of the fold
        assert_eq!(search("aralık 2010"), (1, 7));
        assert_eq!(search("araLık"), (1, 7));
        // ASCII spelling folds to the same 6 bytes as the element
        assert_eq!(search("ARALIK 2010"), (1, 6));
        assert_eq!(search("ocak 1"), (0, 4));
        assert_eq!(search("aral 2010"), (-1, 0));
        assert_eq!(search(""), (-1, 0));
    }

    // scansup.c scanner_isspace: the six C-locale whitespace bytes, so the
    // "invalid value" excerpt stops at a vertical tab too.
    #[test]
    fn seq_search_error_excerpt_stops_at_scanner_whitespace() {
        for c in [b' ', b'\t', b'\n', b'\r', 0x0b, 0x0c] {
            assert!(is_scanner_space(c), "{c:#x}");
        }
        assert!(!is_scanner_space(b'x'));
        let ctx = ::mcx::MemoryContext::new("t");
        let mut dest = 0;
        let mut cur = FromCharCursor::new(b"Foo\x0bbar");
        let err = from_char_seq_search(
            ctx.mcx(),
            &mut dest,
            &mut cur,
            &["Jan", "Feb"],
            None,
            ::types_core::DEFAULT_COLLATION_OID,
            "Mon",
            None,
        )
        .unwrap_err();
        assert_eq!(err.message(), "invalid value \"Foo\" for \"Mon\"");
    }
}
