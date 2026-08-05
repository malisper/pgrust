//! Shared string helpers for the format parser and processors, plus thin
//! re-exports of the locale-aware case transforms (owned by oracle_compat's
//! decomposition of formatting.c str_*).

use ::types_error::{PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION};

use crate::tables::{
    keyword_index_filter, KeySuffix, KeyWord, NUM_TH_LOWER, NUM_TH_UPPER, TH_UPPER,
};

pub use oracle_compat::casemap::{str_casefold, str_initcap, str_tolower, str_toupper};

#[inline]
pub fn pg_ascii_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else {
        ch
    }
}

#[inline]
pub fn pg_ascii_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else {
        ch
    }
}

pub fn index_seq_search(str: &[u8], kw: &[KeyWord], index: &[i32]) -> Option<usize> {
    let first = *str.first()?;
    if !keyword_index_filter(first) {
        return None;
    }

    let poz = index[(first - b' ') as usize];
    if poz > -1 {
        let mut k = poz as usize;
        loop {
            let name = kw[k].name.as_bytes();
            if str.len() >= name.len() && &str[..name.len()] == name {
                return Some(k);
            }
            k += 1;
            if k >= kw.len() {
                return None;
            }
            if first != kw[k].name.as_bytes()[0] {
                break;
            }
        }
    }
    None
}

pub fn suff_search(str: &[u8], suf: &[KeySuffix], typ: i32) -> Option<usize> {
    for (i, s) in suf.iter().enumerate() {
        if s.typ != typ {
            continue;
        }
        let name = s.name.as_bytes();
        if str.len() >= name.len() && &str[..name.len()] == name {
            return Some(i);
        }
    }
    None
}

#[inline]
pub fn is_separator_char(c: u8) -> bool {
    c > 0x20
        && c < 0x7F
        && !c.is_ascii_uppercase()
        && !c.is_ascii_lowercase()
        && !c.is_ascii_digit()
}

/// C: `get_th` (formatting.c) -- ST/ND/RD/TH for a number string.
pub fn get_th(num: &[u8], typ: i32) -> PgResult<&'static str> {
    let len = num.len();
    let mut last = if len > 0 { num[len - 1] } else { 0 };
    if !last.is_ascii_digit() {
        return Err(PgError::error(format!(
            "\"{}\" is not a number",
            String::from_utf8_lossy(num)
        ))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
        .into());
    }

    if len > 1 && num[len - 2] == b'1' {
        last = 0;
    }

    let tbl = if typ == TH_UPPER {
        &NUM_TH_UPPER
    } else {
        &NUM_TH_LOWER
    };
    Ok(match last {
        b'1' => tbl[0],
        b'2' => tbl[1],
        b'3' => tbl[2],
        _ => tbl[3],
    })
}

/// C: `str_numth` -- caller passes `dest` already holding `num`; append suffix.
pub fn str_numth(dest: &mut Vec<u8>, num: &[u8], typ: i32) -> PgResult<()> {
    let th = get_th(num, typ)?;
    dest.extend_from_slice(th.as_bytes());
    Ok(())
}

pub fn asc_tolower(buff: &[u8]) -> Vec<u8> {
    buff.iter().map(|&c| pg_ascii_tolower(c)).collect()
}

pub fn asc_toupper(buff: &[u8]) -> Vec<u8> {
    buff.iter().map(|&c| pg_ascii_toupper(c)).collect()
}

pub fn asc_initcap(buff: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(buff.len());
    let mut wasalnum = false;
    for &b in buff {
        let c = if wasalnum {
            pg_ascii_tolower(b)
        } else {
            pg_ascii_toupper(b)
        };
        result.push(c);
        wasalnum = c.is_ascii_uppercase() || c.is_ascii_lowercase() || c.is_ascii_digit();
    }
    result
}
