#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use elog::ereport;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::{ErrorLocation, PgResult, ERRCODE_NAME_TOO_LONG, NOTICE};
use wchar::{pg_enc, pg_encoding_max_length, pg_encoding_mblen};

pub const NAMEDATALEN: usize = types_core::fmgr::NAMEDATALEN as usize;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("scansup.c", 0, funcname)
}

// Ambient DatabaseEncoding is threaded as a parameter (no ambient-global
// getter seams); callers pass the server encoding.
pub fn downcase_truncate_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &[u8],
    warn: bool,
    encoding: pg_enc,
) -> PgResult<PgVec<'mcx, u8>> {
    downcase_identifier(mcx, ident, warn, true, encoding)
}

pub fn downcase_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &[u8],
    warn: bool,
    truncate: bool,
    encoding: pg_enc,
) -> PgResult<PgVec<'mcx, u8>> {
    let enc_is_single_byte = pg_encoding_max_length(encoding) == 1;
    let mut result = vec_with_capacity_in(mcx, ident.len())?;
    for &b in ident {
        let ch = if b.is_ascii_uppercase() {
            b + (b'a' - b'A')
        } else if enc_is_single_byte && b & 0x80 != 0 && locale_isupper(b) {
            locale_tolower(b)
        } else {
            b
        };
        result.push(ch);
    }
    if ident.len() >= NAMEDATALEN && truncate {
        truncate_identifier(&mut result, warn, encoding)?;
    }
    Ok(result)
}

pub fn truncate_identifier(ident: &mut PgVec<'_, u8>, warn: bool, encoding: pg_enc) -> PgResult<()> {
    if ident.len() >= NAMEDATALEN {
        let clipped = encoding_mbcliplen(encoding, ident, NAMEDATALEN - 1);
        if warn {
            ereport(NOTICE)
                .errcode(ERRCODE_NAME_TOO_LONG)
                .errmsg(format!(
                    "identifier \"{}\" will be truncated to \"{}\"",
                    String::from_utf8_lossy(ident),
                    String::from_utf8_lossy(&ident[..clipped])
                ))
                .finish(loc("truncate_identifier"))?;
        }
        ident.truncate(clipped);
    }
    Ok(())
}

// Must match scan.l's {space} list.
pub fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

pub fn parser_errposition(sourcetext: Option<&[u8]>, location: i32, encoding: pg_enc) -> i32 {
    if location < 0 {
        return 0;
    }
    let Some(src) = sourcetext else {
        return 0;
    };
    mbstrlen_with_len(encoding, src, location) + 1
}

pub fn transformMergeStmt() -> ! {
    panic!(
        "transformMergeStmt (parse_merge.c): sibling parser layer \
         (parse_clause/parse_relation/parse_target/analyze) is unported"
    )
}

// The two helpers below mirror mbutils.c pg_encoding_mbcliplen /
// pg_mbstrlen_with_len over an explicit encoding; they migrate to the
// backend-utils-mb-mbutils unit when it lands.
fn encoding_mbcliplen(encoding: pg_enc, s: &[u8], limit: usize) -> usize {
    if pg_encoding_max_length(encoding) == 1 {
        return cliplen(s, limit);
    }
    let mut clen = 0usize;
    while clen < s.len() && s[clen] != 0 {
        let l = pg_encoding_mblen(encoding, &s[clen..]) as usize;
        if clen + l > limit {
            break;
        }
        clen += l;
        if clen == limit {
            break;
        }
    }
    clen
}

fn cliplen(s: &[u8], limit: usize) -> usize {
    let len = s.len().min(limit);
    s[..len].iter().position(|&b| b == 0).unwrap_or(len)
}

// C's pg_mblen_with_len ereports on a char overrunning `limit`; sourcetext is
// server-encoding-valid on this path, so overrun just ends the count.
fn mbstrlen_with_len(encoding: pg_enc, s: &[u8], limit: i32) -> i32 {
    if pg_encoding_max_length(encoding) == 1 {
        return limit;
    }
    let mut len = 0;
    let mut off = 0usize;
    let mut limit = limit;
    while limit > 0 && off < s.len() && s[off] != 0 {
        let l = pg_encoding_mblen(encoding, &s[off..]);
        limit -= l;
        off += l as usize;
        len += 1;
    }
    len
}

#[cfg(not(target_family = "wasm"))]
#[inline]
fn locale_isupper(ch: u8) -> bool {
    unsafe { libc::isupper(i32::from(ch)) != 0 }
}

#[cfg(not(target_family = "wasm"))]
#[inline]
fn locale_tolower(ch: u8) -> u8 {
    unsafe { libc::tolower(i32::from(ch)) as u8 }
}

// wasm libc has no ctype; the C/POSIX locale never classes a high-bit byte as
// upper-case, so false/identity is the single-locale answer.
#[cfg(target_family = "wasm")]
#[inline]
fn locale_isupper(_ch: u8) -> bool {
    false
}

#[cfg(target_family = "wasm")]
#[inline]
fn locale_tolower(ch: u8) -> u8 {
    ch
}

#[cfg(test)]
mod tests;
