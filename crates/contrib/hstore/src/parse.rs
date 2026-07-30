//! parse_hstore / get_val (hstore_io.c): the `key => value, ...` FSM with
//! `"`-quoting and `\`-escapes; an unquoted case-insensitive `null` value is
//! SQL NULL.

use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};

use crate::repr::Pair;
use crate::{check_key_len, check_val_len};

// scanner_isspace (scansup.c).
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

enum Gv {
    WaitVal,
    InVal,
    InEscVal,
    WaitEscIn,
    WaitEscEscIn,
}

// get_val: one token starting at ptr; Ok(None) is EOF at token start.
fn get_val(input: &[u8], mut ptr: usize, ignoreeq: bool) -> PgResult<Option<(Vec<u8>, bool, usize)>> {
    let mut st = Gv::WaitVal;
    let mut word: Vec<u8> = Vec::with_capacity(32);
    let mut escaped = false;
    loop {
        let c = at(input, ptr);
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

enum St {
    Key,
    Eq,
    Gt,
    Val,
    Del,
}

pub fn parse_hstore(input: &[u8]) -> PgResult<Vec<Pair>> {
    let mut st = St::Key;
    let mut ptr = 0usize;
    let mut pairs: Vec<Pair> = Vec::with_capacity(16);
    let mut cur_key: Option<Vec<u8>> = None;
    loop {
        match st {
            St::Key => match get_val(input, ptr, false)? {
                None => return Ok(pairs),
                Some((word, _esc, newptr)) => {
                    ptr = newptr;
                    check_key_len(word.len())?;
                    cur_key = Some(word);
                    st = St::Eq;
                }
            },
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
            St::Val => match get_val(input, ptr, true)? {
                None => return Err(eof_error()),
                Some((word, escaped, newptr)) => {
                    ptr = newptr;
                    check_val_len(word.len())?;
                    let isnull =
                        word.len() == 4 && !escaped && word.eq_ignore_ascii_case(b"null");
                    pairs.push(Pair {
                        key: cur_key.take().expect("key set before value"),
                        val: (!isnull).then_some(word),
                        needfree: true,
                    });
                    st = St::Del;
                }
            },
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
    input.get(ptr).copied().unwrap_or(0)
}

// prssyntaxerror: snippet is the pg_mblen bytes at ptr.
#[track_caller]
#[cold]
fn syntax_error(input: &[u8], ptr: usize) -> Box<PgError> {
    let snippet_len = match input.get(ptr) {
        None | Some(0) => 1,
        Some(&b) if b < 0x80 => 1,
        Some(&b) if b >= 0xF0 => 4,
        Some(&b) if b >= 0xE0 => 3,
        Some(&b) if b >= 0xC0 => 2,
        _ => 1,
    };
    let snippet: &[u8] = if ptr < input.len() {
        &input[ptr..(ptr + snippet_len).min(input.len())]
    } else {
        &[]
    };
    let near = String::from_utf8_lossy(snippet);
    Box::new(
        PgError::error(format!(
            "syntax error in hstore, near \"{near}\" at position {ptr}"
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

#[track_caller]
#[cold]
fn eof_error() -> Box<PgError> {
    Box::new(
        PgError::error("syntax error in hstore: unexpected end of string")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(input: &str) -> Vec<(String, Option<String>)> {
        parse_hstore(input.as_bytes())
            .unwrap()
            .into_iter()
            .map(|p| {
                (
                    String::from_utf8(p.key).unwrap(),
                    p.val.map(|v| String::from_utf8(v).unwrap()),
                )
            })
            .collect()
    }

    #[test]
    fn basic_forms() {
        assert_eq!(kv("a=>b"), vec![("a".into(), Some("b".into()))]);
        assert_eq!(kv(" a => NULL "), vec![("a".into(), None)]);
        assert_eq!(kv(r#""x y"=>"z,w""#), vec![("x y".into(), Some("z,w".into()))]);
        assert_eq!(kv(r#"a=>"NULL""#), vec![("a".into(), Some("NULL".into()))]);
        assert_eq!(kv(r"a\=>b=>1"), vec![("a=>b".into(), Some("1".into()))]);
        assert!(kv("").is_empty());
    }

    #[test]
    fn syntax_errors() {
        assert!(parse_hstore(b"a").is_err());
        assert!(parse_hstore(b"a=>").is_err());
        assert!(parse_hstore(b"a=b").is_err());
        assert!(parse_hstore(b"a=>b, ").is_ok());
    }
}
