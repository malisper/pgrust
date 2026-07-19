//! Hand-written port of contrib/seg's flex/bison input parser (segscan.l +
//! segparse.y), matching the C grammar, token set and error texts exactly.
//!
//! Grammar (segparse.y):
//! ```text
//! range    : boundary PLUMIN deviation
//!          | boundary RANGE boundary
//!          | boundary RANGE
//!          | RANGE boundary
//!          | boundary
//! boundary : SEGFLOAT | EXTENSION SEGFLOAT
//! deviation: SEGFLOAT
//! ```
//! Tokens (segscan.l): `RANGE` = `..` or `...`; `PLUMIN` = `'+-'` or
//! `(+-)`; `SEGFLOAT` = signed int/real with optional exponent (the real
//! form REQUIRES digits on both sides of the dot — that is what frees `..`
//! for the range operator); `EXTENSION` = `<` `>` `~`. The grammar is
//! LL(1), so this recursive-descent parser errors on the same token as
//! bison's automaton, with the scanner's `yytext` in the detail.

use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
};

use crate::out::{c_format_g, sig_digits};
use crate::repr::Seg;

// ---------------------------------------------------------------------------
// Scanner (segscan.l)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Float,
    Range,
    Plumin,
    Extension,
    Garbage,
    Eof,
}

#[derive(Clone, Copy)]
struct Tok<'a> {
    kind: Kind,
    text: &'a str,
}

struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a [u8]) -> Self {
        Lexer { input, pos: 0 }
    }

    /// `{float}` = `({integer}|{real})([eE]{integer})?` with
    /// `integer = [+-]?[0-9]+`, `real = [+-]?[0-9]+\.[0-9]+`.
    fn float_len(&self, p: usize) -> usize {
        let s = self.input;
        let mut i = p;
        if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
            i += 1;
        }
        let digits_start = i;
        while i < s.len() && s[i].is_ascii_digit() {
            i += 1;
        }
        if i == digits_start {
            return 0;
        }
        let mut mantissa_end = i;
        // real: the dot must be followed by at least one digit
        if i < s.len() && s[i] == b'.' {
            let mut j = i + 1;
            while j < s.len() && s[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                mantissa_end = j;
            }
        }
        // optional exponent, only if the full sub-pattern matches
        let mut i = mantissa_end;
        if i < s.len() && (s[i] == b'e' || s[i] == b'E') {
            let mut j = i + 1;
            if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
                j += 1;
            }
            let exp_start = j;
            while j < s.len() && s[j].is_ascii_digit() {
                j += 1;
            }
            if j > exp_start {
                i = j;
            }
        }
        i - p
    }

    fn next(&mut self) -> Tok<'a> {
        let s = self.input;
        while self.pos < s.len() && matches!(s[self.pos], b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
        {
            self.pos += 1;
        }
        if self.pos >= s.len() {
            return Tok {
                kind: Kind::Eof,
                text: "",
            };
        }
        let p = self.pos;
        let text = |len: usize| core::str::from_utf8(&s[p..p + len]).unwrap_or("?");

        // {range} = (\.\.)(\.)?
        if s[p] == b'.' && p + 1 < s.len() && s[p + 1] == b'.' {
            let len = if p + 2 < s.len() && s[p + 2] == b'.' { 3 } else { 2 };
            self.pos = p + len;
            return Tok {
                kind: Kind::Range,
                text: text(len),
            };
        }
        // {plumin} = ('+-') | ((+-))
        if s[p..].starts_with(b"'+-'") || s[p..].starts_with(b"(+-)") {
            self.pos = p + 4;
            return Tok {
                kind: Kind::Plumin,
                text: text(4),
            };
        }
        let flen = self.float_len(p);
        if flen > 0 {
            self.pos = p + flen;
            return Tok {
                kind: Kind::Float,
                text: text(flen),
            };
        }
        self.pos = p + 1;
        let kind = match s[p] {
            b'<' | b'>' | b'~' => Kind::Extension,
            _ => Kind::Garbage,
        };
        Tok {
            kind,
            text: text(1),
        }
    }
}

// ---------------------------------------------------------------------------
// Parser (segparse.y)
// ---------------------------------------------------------------------------

/// One parsed boundary (`struct BND`).
#[derive(Clone, Copy)]
struct Bnd {
    val: f32,
    ext: u8,
    sigd: i32,
}

fn syntax_error(tok: &Tok<'_>) -> Box<PgError> {
    let detail = if tok.kind == Kind::Eof {
        "syntax error at end of input".to_string()
    } else {
        format!("syntax error at or near \"{}\"", tok.text)
    };
    Box::new(
        PgError::error("bad seg representation")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_detail(detail),
    )
}

/// `seg_atof`: float4in_internal with the token text as both value and
/// error string.
fn seg_atof(value: &str) -> PgResult<f32> {
    adt_float::io::float4in_internal(value, None, "seg", value, None)
}

struct Parser<'a> {
    lexer: Lexer<'a>,
    cur: Tok<'a>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a [u8]) -> Self {
        let mut lexer = Lexer::new(input);
        let cur = lexer.next();
        Parser { lexer, cur }
    }

    fn advance(&mut self) {
        self.cur = self.lexer.next();
    }

    fn expect(&mut self, kind: Kind) -> PgResult<Tok<'a>> {
        if self.cur.kind != kind {
            return Err(syntax_error(&self.cur));
        }
        let t = self.cur;
        self.advance();
        Ok(t)
    }

    /// `boundary: SEGFLOAT | EXTENSION SEGFLOAT`.
    fn boundary(&mut self) -> PgResult<Bnd> {
        let ext = if self.cur.kind == Kind::Extension {
            let e = self.cur.text.as_bytes()[0];
            self.advance();
            e
        } else {
            0
        };
        let tok = self.expect(Kind::Float)?;
        let val = seg_atof(tok.text)?;
        Ok(Bnd {
            val,
            ext,
            sigd: sig_digits(tok.text.as_bytes()),
        })
    }

    /// `deviation: SEGFLOAT`.
    fn deviation(&mut self) -> PgResult<Bnd> {
        let tok = self.expect(Kind::Float)?;
        let val = seg_atof(tok.text)?;
        Ok(Bnd {
            val,
            ext: 0,
            sigd: sig_digits(tok.text.as_bytes()),
        })
    }
}

/// `seg_in`'s parse step.
pub fn parse_seg(input: &[u8]) -> PgResult<Seg> {
    let mut p = Parser::new(input);

    if p.cur.kind == Kind::Range {
        // RANGE boundary: open lower bound
        p.advance();
        let b = p.boundary()?;
        p.expect(Kind::Eof)?;
        return Ok(Seg {
            lower: f32::NEG_INFINITY,
            upper: b.val,
            l_sigd: 0,
            u_sigd: b.sigd as u8,
            l_ext: b'-',
            u_ext: b.ext,
        });
    }

    let b1 = p.boundary()?;
    match p.cur.kind {
        Kind::Plumin => {
            p.advance();
            let dev = p.deviation()?;
            p.expect(Kind::Eof)?;
            let lower = b1.val - dev.val;
            let upper = b1.val + dev.val;
            let l_sigd = sig_digits(c_format_g(lower as f64).as_bytes())
                .max(b1.sigd.max(dev.sigd));
            let u_sigd = sig_digits(c_format_g(upper as f64).as_bytes())
                .max(b1.sigd.max(dev.sigd));
            Ok(Seg {
                lower,
                upper,
                l_sigd: l_sigd as u8,
                u_sigd: u_sigd as u8,
                l_ext: 0,
                u_ext: 0,
            })
        }
        Kind::Range => {
            p.advance();
            if p.cur.kind == Kind::Float || p.cur.kind == Kind::Extension {
                // boundary RANGE boundary
                let b2 = p.boundary()?;
                p.expect(Kind::Eof)?;
                if b1.val > b2.val {
                    return Err(Box::new(
                        PgError::error(format!(
                            "swapped boundaries: {} is greater than {}",
                            c_format_g(b1.val as f64),
                            c_format_g(b2.val as f64)
                        ))
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                    ));
                }
                Ok(Seg {
                    lower: b1.val,
                    upper: b2.val,
                    l_sigd: b1.sigd as u8,
                    u_sigd: b2.sigd as u8,
                    l_ext: b1.ext,
                    u_ext: b2.ext,
                })
            } else {
                // boundary RANGE: open upper bound
                p.expect(Kind::Eof)?;
                Ok(Seg {
                    lower: b1.val,
                    upper: f32::INFINITY,
                    l_sigd: b1.sigd as u8,
                    u_sigd: 0,
                    l_ext: b1.ext,
                    u_ext: b'-',
                })
            }
        }
        Kind::Eof => {
            // single boundary
            Ok(Seg {
                lower: b1.val,
                upper: b1.val,
                l_sigd: b1.sigd as u8,
                u_sigd: b1.sigd as u8,
                l_ext: b1.ext,
                u_ext: b1.ext,
            })
        }
        _ => Err(syntax_error(&p.cur)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::out::seg_out;

    fn roundtrip(s: &str) -> String {
        String::from_utf8(seg_out(&parse_seg(s.as_bytes()).unwrap())).unwrap()
    }

    fn err(s: &str) -> String {
        let e = parse_seg(s.as_bytes()).unwrap_err();
        format!("{}|{}", e.message(), e.detail().unwrap_or(""))
    }

    #[test]
    fn corpus_forms_roundtrip() {
        assert_eq!(roundtrip("1"), "1");
        assert_eq!(roundtrip("-1.0"), "-1.0");
        assert_eq!(roundtrip("1.0e7"), "1.0e+07");
        assert_eq!(roundtrip("1 .. 2"), "1 .. 2");
        assert_eq!(roundtrip("1..2"), "1 .. 2");
        assert_eq!(roundtrip("1...2"), "1 .. 2");
        assert_eq!(roundtrip("1 .."), "1 ..");
        assert_eq!(roundtrip(".. 2"), ".. 2");
        // bare ".." is NOT grammatical in C (range needs a boundary);
        // the ".." display form only arises from seg_union results
        assert_eq!(
            String::from_utf8(
                parse_seg(b".. 2")
                    .map(|lo| crate::ops::seg_union(&lo, &parse_seg(b"1 ..").unwrap()))
                    .map(|s| crate::out::seg_out(&s))
                    .unwrap()
            )
            .unwrap(),
            ".."
        );
        assert_eq!(roundtrip("<1 .. >2"), "<1 .. >2");
        assert_eq!(roundtrip("~5"), "~5");
        // PLUMIN forms
        assert_eq!(roundtrip("5'+-'1"), "4 .. 6");
        assert_eq!(roundtrip("5(+-)1"), "4 .. 6");
        // PLUMIN sigd law: boundaries pick up %g digits of the results
        assert_eq!(roundtrip("5.0'+-'0.3"), "4.7 .. 5.3");
    }

    #[test]
    fn c_error_texts() {
        assert_eq!(
            err("ABC"),
            "bad seg representation|syntax error at or near \"A\""
        );
        assert_eq!(
            err("1 e7"),
            "bad seg representation|syntax error at or near \"e\""
        );
        assert_eq!(
            err("1 .. .."),
            "bad seg representation|syntax error at or near \"..\""
        );
        assert_eq!(
            err(""),
            "bad seg representation|syntax error at end of input"
        );
        assert_eq!(
            err("3 .. 2"),
            "swapped boundaries: 3 is greater than 2|"
        );
        // range error from float4in_internal
        let e = parse_seg(b"1e39").unwrap_err();
        assert_eq!(e.message(), "\"1e39\" is out of range for type real");
    }
}
