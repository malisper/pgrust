//! `parse_hstore` / `get_val` (hstore_io.c) — the text-input FSM.
//!
//! Parses `key => value, ...` with `"`-quoting and `\`-escapes. An unquoted
//! 4-char value spelled `null` (case-insensitive) is a SQL NULL value.

use ::types_error::PgError;

use crate::{check_key_len, check_val_len, ParseFail};
use crate::repr::Pair;

/// `scanner_isspace(c)` (scansup.c) — the input scanner's whitespace set:
/// space, tab, newline, carriage return, form feed.
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

#[derive(PartialEq)]
enum Gv {
    WaitVal,
    InVal,
    InEscVal,
    WaitEscIn,
    WaitEscEscIn,
}

/// `get_val(state, ignoreeq, &escaped)` — read one token (key or value) starting
/// at `ptr`. Returns `(word, escaped, new_ptr)` on success, `Ok(None)` for EOF
/// at the start of a value (the C `return false` with no soft error), and an
/// error for a syntax error / unexpected EOF.
fn get_val(
    input: &[u8],
    mut ptr: usize,
    ignoreeq: bool,
) -> Result<Option<(Vec<u8>, bool, usize)>, ParseFail> {
    let mut st = Gv::WaitVal;
    let mut word: Vec<u8> = Vec::with_capacity(32);
    let mut escaped = false;

    loop {
        let c = if ptr < input.len() { input[ptr] } else { 0 };
        match st {
            Gv::WaitVal => {
                if c == b'"' {
                    escaped = true;
                    st = Gv::InEscVal;
                } else if c == 0 {
                    return Ok(None);
                } else if c == b'=' && !ignoreeq {
                    return Err(syntax_error(input, ptr));
                } else if c == b'\\' {
                    st = Gv::WaitEscIn;
                } else if !is_space(c) {
                    word.push(c);
                    st = Gv::InVal;
                }
            }
            Gv::InVal => {
                if c == b'\\' {
                    st = Gv::WaitEscIn;
                } else if c == b'=' && !ignoreeq {
                    return Ok(Some((word, escaped, ptr - 1)));
                } else if c == b',' && ignoreeq {
                    return Ok(Some((word, escaped, ptr - 1)));
                } else if is_space(c) {
                    return Ok(Some((word, escaped, ptr)));
                } else if c == 0 {
                    return Ok(Some((word, escaped, ptr.wrapping_sub(1))));
                } else {
                    word.push(c);
                }
            }
            Gv::InEscVal => {
                if c == b'\\' {
                    st = Gv::WaitEscEscIn;
                } else if c == b'"' {
                    return Ok(Some((word, escaped, ptr)));
                } else if c == 0 {
                    return Err(eof_error());
                } else {
                    word.push(c);
                }
            }
            Gv::WaitEscIn => {
                if c == 0 {
                    return Err(eof_error());
                }
                word.push(c);
                st = Gv::InVal;
            }
            Gv::WaitEscEscIn => {
                if c == 0 {
                    return Err(eof_error());
                }
                word.push(c);
                st = Gv::InEscVal;
            }
        }
        ptr = ptr.wrapping_add(1);
    }
}

#[derive(PartialEq)]
enum St {
    Key,
    Eq,
    Gt,
    Val,
    Del,
}

/// `parse_hstore` (hstore_io.c) — drive the key/value FSM over `input`.
pub fn parse_hstore(input: &[u8]) -> Result<Vec<Pair>, ParseFail> {
    let mut st = St::Key;
    let mut ptr = 0usize;
    let mut pairs: Vec<Pair> = Vec::with_capacity(16);
    let mut cur_key: Option<Vec<u8>> = None;

    loop {
        match st {
            St::Key => {
                match get_val(input, ptr, false)? {
                    None => return Ok(pairs), // EOF, all okay
                    Some((word, _esc, newptr)) => {
                        ptr = newptr;
                        check_key_len(word.len()).map_err(ParseFail::Hard)?;
                        cur_key = Some(word);
                        st = St::Eq;
                    }
                }
            }
            St::Eq => {
                let c = at(input, ptr);
                if c == b'=' {
                    st = St::Gt;
                } else if c == 0 {
                    return Err(eof_error());
                } else if !is_space(c) {
                    return Err(syntax_error(input, ptr));
                }
            }
            St::Gt => {
                let c = at(input, ptr);
                if c == b'>' {
                    st = St::Val;
                } else if c == 0 {
                    return Err(eof_error());
                } else {
                    return Err(syntax_error(input, ptr));
                }
            }
            St::Val => {
                match get_val(input, ptr, true)? {
                    None => return Err(eof_error()),
                    Some((word, escaped, newptr)) => {
                        ptr = newptr;
                        check_val_len(word.len()).map_err(ParseFail::Hard)?;
                        let isnull = word.len() == 4
                            && !escaped
                            && word.eq_ignore_ascii_case(b"null");
                        let key = cur_key.take().expect("key set before value");
                        pairs.push(Pair {
                            key,
                            val: if isnull { None } else { Some(word) },
                            needfree: true,
                        });
                        st = St::Del;
                    }
                }
            }
            St::Del => {
                let c = at(input, ptr);
                if c == b',' {
                    st = St::Key;
                } else if c == 0 {
                    return Ok(pairs);
                } else if !is_space(c) {
                    return Err(syntax_error(input, ptr));
                }
            }
        }
        ptr = ptr.wrapping_add(1);
    }
}

#[inline]
fn at(input: &[u8], ptr: usize) -> u8 {
    if ptr < input.len() {
        input[ptr]
    } else {
        0
    }
}

/// `prssyntaxerror` (hstore_io.c): `syntax error in hstore, near "%.*s" at
/// position %d`, where the snippet is the `pg_mblen` bytes at `ptr` and the
/// position is `ptr - begin`.
fn syntax_error(input: &[u8], ptr: usize) -> ParseFail {
    let snippet_len = mblen_at(input, ptr);
    let snippet: &[u8] = if ptr < input.len() {
        &input[ptr..(ptr + snippet_len).min(input.len())]
    } else {
        &[]
    };
    let near = String::from_utf8_lossy(snippet);
    ParseFail::Hard(
        PgError::error(format!(
            "syntax error in hstore, near \"{near}\" at position {ptr}"
        ))
        .with_sqlstate(::types_error::ERRCODE_SYNTAX_ERROR),
    )
}

/// `pg_mblen` of the byte at `ptr` (UTF-8 lead-byte length; 1 for ASCII / EOF).
fn mblen_at(input: &[u8], ptr: usize) -> usize {
    match input.get(ptr) {
        None | Some(0) => 1,
        Some(&b) if b < 0x80 => 1,
        Some(&b) if b >= 0xF0 => 4,
        Some(&b) if b >= 0xE0 => 3,
        Some(&b) if b >= 0xC0 => 2,
        _ => 1,
    }
}

fn eof_error() -> ParseFail {
    ParseFail::Hard(
        PgError::error("syntax error in hstore: unexpected end of string")
            .with_sqlstate(::types_error::ERRCODE_SYNTAX_ERROR),
    )
}
