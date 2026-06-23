//! `DCH_from_char` support helpers: the integer / sequential-search parsers and
//! the per-conversion mode/int setters.
//!
//! Faithful port of formatting.c:2074-2511 (PG 18.3).  Errors route through
//! `errsave` so the `escontext` soft-error discipline matches C's `ereturn`.
//!
//! The reference C-ABI port carried the localized day/month arrays as
//! `std::ffi::CString`s; in the idiomatic surface those are plain owned
//! `Vec<u8>` (NUL-free) elements. The only externals reached are the locale
//! case routines in [`crate::case`], which themselves seam into pg_locale.c.

use mcx::Mcx;
use types_error::{PgError, PgResult, SoftErrorContext};
use types_error::{ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_INVALID_DATETIME_FORMAT};
use types_core::Oid;

use crate::case::{pg_ascii_tolower, str_tolower, str_toupper};
use crate::parse::is_c_space;
use crate::tables::*;

/// Local `errsave` helper mirroring C's `errsave(escontext, ...)` for code that
/// already holds a complete [`PgError`]: routes through the shared
/// [`types_error::ereturn`] (soft-error context => save and return `()`;
/// no context => propagate as a hard error).
fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    types_error::ereturn(escontext, (), err)
}

/// Result of a from-char parse step that advances the input cursor.  We mirror
/// C's `const char **src` by threading a byte cursor (`pos`) and returning the
/// new position.
pub struct FromCharCursor<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
}

impl<'a> FromCharCursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        FromCharCursor { bytes, pos: 0 }
    }
    /// Current byte, or 0 (NUL) at end (mirrors C's `*s`).
    #[inline]
    pub fn cur(&self) -> u8 {
        if self.pos < self.bytes.len() {
            self.bytes[self.pos]
        } else {
            0
        }
    }
    #[inline]
    pub fn at(&self, off: usize) -> u8 {
        let i = self.pos + off;
        if i < self.bytes.len() {
            self.bytes[i]
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

/// C: `is_next_separator` (formatting.c:2074).  `nodes[idx]` is the current
/// node; `nodes[idx+1]` is "next".
pub fn is_next_separator(nodes: &[FormatNode], idx: usize) -> bool {
    let n = &nodes[idx];
    if n.typ == NODE_TYPE_END {
        return false;
    }
    if n.typ == NODE_TYPE_ACTION && s_thth(n.suffix) {
        return true;
    }
    // Next node.
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

/// C: `adjust_partial_year_to_2020` (formatting.c:2107).
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

/// C: `strspace_len` (formatting.c:2131).
pub fn strspace_len(bytes: &[u8]) -> usize {
    let mut len = 0;
    while len < bytes.len() && bytes[len] != 0 && is_c_space(bytes[len]) {
        len += 1;
    }
    len
}

/// C: `from_char_set_mode` (formatting.c:2153).
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

/// C: `from_char_set_int` (formatting.c:2180).
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

/// C: `from_char_parse_int_len` (formatting.c:2217).
///
/// Returns `Some(consumed_bytes)` on success (storing into `dest` if provided),
/// or `None` on a soft error (escontext set) / propagates a hard error.
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

    // Skip leading whitespace.
    cur.pos += strspace_len(cur.rest());

    debug_assert!(len <= DCH_MAX_ITEM_SIZ);

    // strlcpy(copy, *src, len + 1): copy up to `len` source bytes (stopping at
    // NUL), used = number of source bytes that *would* be copied (= source
    // length if it fits in len, else len). strlcpy returns strlen(src), but it
    // is only used here as min(srclen, len) for the "too short" test; C copies
    // exactly min(srclen,len) bytes. We replicate `copy` and `used`.
    let src_after_ws = cur.rest();
    // bytes available until NUL
    let src_nul = src_after_ws
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(src_after_ws.len());
    // strlcpy copies up to len bytes; `used = strlcpy(...)` returns strlen(src)
    // truncated by the dest size, but the C code uses `used` as the count of
    // copied bytes -> effectively min(src_nul, len).
    let mut used = src_nul.min(len);
    // Crate-local working buffer (never escapes; read only as `&[u8]` by
    // `strtol_from`/`from_utf8_lossy`). The C `copy[]` is a fixed stack buffer
    // (DCH_MAX_ITEM_SIZ + 1); a plain owned `Vec` mirrors that scratch lifetime.
    let mut copy: Vec<u8> = Vec::new();
    copy.extend_from_slice(&src_after_ws[..used]);

    let result: i64;
    let erange: bool;

    if s_fm(node.suffix) || is_next_separator(nodes, idx) {
        // Fill Mode / next is non-digit: slurp as many as we can via strtol
        // from `init` (the original position, before whitespace skip — note C
        // calls strtol(init, ...), where init is the pre-skip pointer, but
        // strtol itself skips leading whitespace).
        let (val, end_off, rng) = strtol_from(cur.bytes, init);
        result = val;
        erange = rng;
        cur.pos = end_off;
    } else {
        // Pull exactly `len` characters and convert those.
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

/// C: `from_char_parse_int` (formatting.c:2313).
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

/// A faithful `strtol(base 10)` over `bytes[start..]`: skips leading C
/// whitespace, optional sign, then digits.  Returns (value, end-offset-into-
/// bytes, erange).  `value` saturates at i64 bounds on overflow with erange
/// set (the caller only checks i32 range / the ERANGE flag).
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
            // accumulate with overflow detection
            match acc.checked_mul(10).and_then(|v| v.checked_add(d)) {
                Some(v) => acc = v,
                None => {
                    erange = true;
                }
            }
        }
        i += 1;
    }
    if i == digit_start {
        // No digits consumed: strtol returns 0 and endptr == start of number
        // (after sign). C uses endptr; with no conversion endptr == nptr, so
        // src stays at `init`. Return start so caller detects no progress.
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

/// C: `seq_search_ascii` (formatting.c:2331).  Returns (match-index, match-len)
/// or (-1, 0).
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
        // compare rest
        let mut p = 1usize;
        let mut nn = 1usize;
        loop {
            if p >= ab.len() {
                // matched whole array entry
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

/// C: `seq_search_localized` (formatting.c:2388).  `array` is a localized list
/// of NUL-free owned byte strings; returns (match-index, match-len) or (-1, 0).
pub fn seq_search_localized<'mcx>(
    mcx: Mcx<'mcx>,
    name: &[u8],
    array: &[Vec<u8>],
    collid: Oid,
) -> PgResult<(i32, usize)> {
    if name.is_empty() || name[0] == 0 {
        return Ok((-1, 0));
    }

    // Quick pass: exact prefix match.
    for (ai, a) in array.iter().enumerate() {
        let ab = a.as_slice();
        let element_len = ab.len();
        if name.len() >= element_len && &name[..element_len] == ab {
            return Ok((ai as i32, element_len));
        }
    }

    // Fold to upper then lower for case-insensitive matching.
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

/// C: `from_char_seq_search` (formatting.c:2472).  On match, stores the index
/// into `dest`, advances the cursor, returns `Ok(true)`.  On no match, routes a
/// soft/hard error and returns `Ok(false)`.
#[allow(clippy::too_many_arguments)]
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
        // Truncate at next whitespace for the error report.
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

/// C `scanner_isspace` (parser/scansup.c): space, tab, newline, CR, form-feed.
#[inline]
pub fn is_scanner_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strtol_basic() {
        assert_eq!(strtol_from(b"42abc", 0), (42, 2, false));
        assert_eq!(strtol_from(b"  -7", 0), (-7, 4, false));
        let (v, _e, rng) = strtol_from(b"99999999999999999999", 0);
        assert!(rng);
        assert_eq!(v, i64::MAX);
    }

    #[test]
    fn seq_search_ascii_matches_month() {
        let (idx, len) = seq_search_ascii(b"March 2020", &MONTHS_FULL);
        assert_eq!(idx, 2);
        assert_eq!(len, 5);
    }

    #[test]
    fn seq_search_ascii_case_insensitive() {
        let (idx, len) = seq_search_ascii(b"jan", &MONTHS);
        assert_eq!(idx, 0);
        assert_eq!(len, 3);
    }

    #[test]
    fn adjust_year() {
        assert_eq!(adjust_partial_year_to_2020(69), 2069);
        assert_eq!(adjust_partial_year_to_2020(70), 1970);
        assert_eq!(adjust_partial_year_to_2020(99), 1999);
        assert_eq!(adjust_partial_year_to_2020(1999), 1999);
    }
}
