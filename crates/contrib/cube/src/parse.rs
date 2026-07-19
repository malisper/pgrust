//! Hand-written port of contrib/cube's flex/bison input parser
//! (cubescan.l + cubeparse.y), matching the C grammar, token set and error
//! texts exactly.
//!
//! Grammar (cubeparse.y):
//! ```text
//! box       : '[' paren_list ',' paren_list ']'
//!           | paren_list ',' paren_list
//!           | paren_list
//!           | list
//! paren_list: '(' list ')' | '(' ')'
//! list      : CUBEFLOAT | list ',' CUBEFLOAT
//! ```
//! The grammar is LL(1), so this recursive-descent parser raises its syntax
//! error on exactly the token where bison's LALR automaton fails to shift,
//! and reports it with the scanner's `yytext` just like cube_yyerror.
//!
//! Errors are returned as plain `PgError`s; the fmgr wrapper `ereturn`s them
//! against the caller's soft-error context, which is behaviorally identical
//! to C's `errsave(escontext, ...)` + `float8in_internal(..., escontext)`.

use types_error::{PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION};

use crate::repr::{build_image, build_image_auto, CUBE_MAX_DIM};

// ---------------------------------------------------------------------------
// Scanner (cubescan.l)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Float,
    OParen,
    CParen,
    OBracket,
    CBracket,
    Comma,
    Garbage,
    Eof,
}

#[derive(Clone, Copy)]
struct Tok<'a> {
    kind: Kind,
    /// The scanner's `yytext` for this token (error messages).
    text: &'a str,
}

struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

fn is_digit(b: u8) -> bool {
    b.is_ascii_digit()
}

impl<'a> Lexer<'a> {
    fn new(input: &'a [u8]) -> Self {
        Lexer { input, pos: 0 }
    }

    /// Length of a `{float}` match at `p`:
    /// `({integer}|{real})([eE]{integer})?` with
    /// `integer = [+-]?[0-9]+`, `real = [+-]?([0-9]+\.[0-9]*|\.[0-9]+)`.
    fn float_len(&self, p: usize) -> usize {
        let s = self.input;
        let mut i = p;
        if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
            i += 1;
        }
        let digits_start = i;
        while i < s.len() && is_digit(s[i]) {
            i += 1;
        }
        let int_digits = i - digits_start;
        let mut mantissa_end = i;
        if int_digits > 0 {
            // {n}\.{n}? — dot may be followed by zero or more digits.
            if i < s.len() && s[i] == b'.' {
                let mut j = i + 1;
                while j < s.len() && is_digit(s[j]) {
                    j += 1;
                }
                mantissa_end = j;
            }
        } else {
            // \.{n} — needs at least one digit after the dot.
            if i < s.len() && s[i] == b'.' {
                let mut j = i + 1;
                while j < s.len() && is_digit(s[j]) {
                    j += 1;
                }
                if j > i + 1 {
                    mantissa_end = j;
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        }
        // Optional exponent: [eE]{integer}. Only part of the token if the
        // full sub-pattern matches (flex longest-match backtracking).
        let mut i = mantissa_end;
        if i < s.len() && (s[i] == b'e' || s[i] == b'E') {
            let mut j = i + 1;
            if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
                j += 1;
            }
            let exp_digits_start = j;
            while j < s.len() && is_digit(s[j]) {
                j += 1;
            }
            if j > exp_digits_start {
                i = j;
            }
        }
        i - p
    }

    /// Length of `{infinity}` = `[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?`
    /// or `{NaN}` = `[nN][aA][nN]` at `p`.
    fn inf_nan_len(&self, p: usize) -> usize {
        let s = self.input;
        let mut i = p;
        if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
            i += 1;
        }
        let rest = &s[i..];
        let lower3: Vec<u8> = rest.iter().take(8).map(|b| b.to_ascii_lowercase()).collect();
        if lower3.starts_with(b"infinity") {
            return i - p + 8;
        }
        if lower3.starts_with(b"inf") {
            return i - p + 3;
        }
        // NaN takes no sign in the flex pattern.
        if i == p && lower3.starts_with(b"nan") {
            return 3;
        }
        0
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
        // Longest match across {float} / {infinity} / {NaN}; the punctuation
        // and `.` rules are all single-byte.
        let flen = self.float_len(p).max(self.inf_nan_len(p));
        if flen > 0 {
            self.pos = p + flen;
            return Tok {
                kind: Kind::Float,
                text: core::str::from_utf8(&s[p..p + flen]).unwrap_or(""),
            };
        }
        self.pos = p + 1;
        let one = &s[p..p + 1];
        let kind = match s[p] {
            b'[' => Kind::OBracket,
            b']' => Kind::CBracket,
            b'(' => Kind::OParen,
            b')' => Kind::CParen,
            b',' => Kind::Comma,
            _ => Kind::Garbage,
        };
        Tok {
            kind,
            text: core::str::from_utf8(one).unwrap_or("?"),
        }
    }
}

// ---------------------------------------------------------------------------
// Parser (cubeparse.y)
// ---------------------------------------------------------------------------

struct Parser<'a> {
    lexer: Lexer<'a>,
    cur: Tok<'a>,
}

fn syntax_error(tok: &Tok<'_>) -> Box<PgError> {
    let detail = if tok.kind == Kind::Eof {
        "syntax error at end of input".to_string()
    } else {
        format!("syntax error at or near \"{}\"", tok.text)
    };
    Box::new(
        PgError::error("invalid input syntax for cube")
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .with_detail(detail),
    )
}

fn too_many_dimensions() -> Box<PgError> {
    Box::new(
        PgError::error("invalid input syntax for cube")
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .with_detail(format!(
                "A cube cannot have more than {CUBE_MAX_DIM} dimensions."
            )),
    )
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

    /// `list: CUBEFLOAT | list ',' CUBEFLOAT` — the element token texts.
    fn list(&mut self) -> PgResult<Vec<&'a str>> {
        let mut items = Vec::new();
        items.push(self.expect(Kind::Float)?.text);
        while self.cur.kind == Kind::Comma {
            self.advance();
            items.push(self.expect(Kind::Float)?.text);
        }
        Ok(items)
    }

    /// `paren_list: '(' list ')' | '(' ')'`.
    fn paren_list(&mut self) -> PgResult<Vec<&'a str>> {
        self.expect(Kind::OParen)?;
        if self.cur.kind == Kind::CParen {
            self.advance();
            return Ok(Vec::new());
        }
        let items = self.list()?;
        self.expect(Kind::CParen)?;
        Ok(items)
    }
}

/// The joined `$n` string a production built in C (token texts glued with
/// `,`) — used verbatim in error details.
fn joined(items: &[&str]) -> String {
    items.join(",")
}

fn dim_mismatch(l1: &[&str], l2: &[&str]) -> Box<PgError> {
    Box::new(
        PgError::error("invalid input syntax for cube")
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .with_detail(format!(
                "Different point dimensions in ({}) and ({}).",
                joined(l1),
                joined(l2)
            )),
    )
}

/// `float8in_internal(s, &endptr, "cube", str, escontext)` for one element.
/// The element is an exact scanner token, so no trailing-junk handling is
/// needed; errors surface as `PgError`s the fmgr wrapper `ereturn`s.
fn parse_element(element: &str, orig: &str) -> PgResult<f64> {
    adt_float::io::float8in_internal(element, None, "cube", orig, None)
}

/// `write_box(dim, str1, str2, ...)`: ll from `l1`, ur from `l2`, then
/// collapse to a point if every ur equals its ll (NaN keeps the box form).
fn write_box(l1: &[&str], l2: &[&str]) -> PgResult<Vec<u8>> {
    let dim = l1.len();
    let s1 = joined(l1);
    let s2 = joined(l2);
    let mut coords = Vec::with_capacity(2 * dim);
    for e in l1 {
        coords.push(parse_element(e, &s1)?);
    }
    for e in l2 {
        coords.push(parse_element(e, &s2)?);
    }
    Ok(build_image_auto(dim, &coords))
}

/// `write_point_as_box(dim, str, ...)`.
fn write_point_as_box(l: &[&str]) -> PgResult<Vec<u8>> {
    let s = joined(l);
    let mut coords = Vec::with_capacity(l.len());
    for e in l {
        coords.push(parse_element(e, &s)?);
    }
    Ok(build_image(l.len(), true, &coords))
}

fn check_dim(n: usize) -> PgResult<()> {
    if n > CUBE_MAX_DIM {
        return Err(too_many_dimensions());
    }
    Ok(())
}

/// `cube_in`'s parse step: returns the full NDBOX image (with varlena
/// header) or the error C would raise.
pub fn parse_cube(input: &[u8]) -> PgResult<Vec<u8>> {
    let mut p = Parser::new(input);
    match p.cur.kind {
        Kind::OBracket => {
            p.advance();
            let l1 = p.paren_list()?;
            p.expect(Kind::Comma)?;
            let l2 = p.paren_list()?;
            p.expect(Kind::CBracket)?;
            p.expect(Kind::Eof)?;
            if l1.len() != l2.len() {
                return Err(dim_mismatch(&l1, &l2));
            }
            check_dim(l1.len())?;
            write_box(&l1, &l2)
        }
        Kind::OParen => {
            let l1 = p.paren_list()?;
            if p.cur.kind == Kind::Comma {
                p.advance();
                let l2 = p.paren_list()?;
                p.expect(Kind::Eof)?;
                if l1.len() != l2.len() {
                    return Err(dim_mismatch(&l1, &l2));
                }
                check_dim(l1.len())?;
                write_box(&l1, &l2)
            } else {
                p.expect(Kind::Eof)?;
                check_dim(l1.len())?;
                write_point_as_box(&l1)
            }
        }
        Kind::Float => {
            let l = p.list()?;
            p.expect(Kind::Eof)?;
            check_dim(l.len())?;
            write_point_as_box(&l)
        }
        _ => Err(syntax_error(&p.cur)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repr::CubeView;

    fn parse(s: &str) -> PgResult<Vec<u8>> {
        parse_cube(s.as_bytes())
    }

    fn err_detail(s: &str) -> String {
        let e = parse(s).unwrap_err();
        format!("{}|{}", e.message(), e.detail().unwrap_or(""))
    }

    #[test]
    fn accepts_all_forms() {
        // scalar -> 1-D point
        let img = parse("1").unwrap();
        let v = CubeView::from_payload(&img[4..]);
        assert!(v.is_point());
        assert_eq!((v.dim(), v.ll(0)), (1, 1.0));
        // bare list -> N-D point
        let img = parse("1,2,3").unwrap();
        assert_eq!(CubeView::from_payload(&img[4..]).dim(), 3);
        // paren point, box pair, bracketed pair
        assert!(parse("(1,2)").is_ok());
        assert!(parse("(1,2),(3,4)").is_ok());
        assert!(parse("[(1,2),(3,4)]").is_ok());
        // zero-dimensional
        let img = parse("()").unwrap();
        assert_eq!(CubeView::from_payload(&img[4..]).dim(), 0);
        assert!(parse("[(),()]").is_ok());
        // whitespace + inf/nan spellings
        assert!(parse("  ( 1 , +inf )  ").is_ok());
        assert!(parse("(Infinity,-INF,nan,NaN)").is_ok());
        // exponents / fractions
        assert!(parse("(.5,1.,1.5e2,-2E-1)").is_ok());
    }

    #[test]
    fn identical_pair_collapses_to_point() {
        let img = parse("(1,2),(1,2)").unwrap();
        assert!(CubeView::from_payload(&img[4..]).is_point());
        let img = parse("(1,2),(1,3)").unwrap();
        assert!(!CubeView::from_payload(&img[4..]).is_point());
    }

    #[test]
    fn c_error_texts_syntax() {
        // Trailing comma after a complete box errors AT the comma.
        assert_eq!(
            err_detail("(1),(2),"),
            "invalid input syntax for cube|syntax error at or near \",\""
        );
        // ... but a truncated input errors at end of input.
        assert_eq!(
            err_detail("(1),("),
            "invalid input syntax for cube|syntax error at end of input"
        );
        // ']' where a paren_list was required.
        assert_eq!(
            err_detail("[(1),]"),
            "invalid input syntax for cube|syntax error at or near \"]\""
        );
        // Garbage character (scanner `.` rule).
        assert_eq!(
            err_detail("(1,2x)"),
            "invalid input syntax for cube|syntax error at or near \"x\""
        );
        // Float where '[' demands a paren_list.
        assert_eq!(
            err_detail("[1,2]"),
            "invalid input syntax for cube|syntax error at or near \"1\""
        );
        // EOF mid-list.
        assert_eq!(
            err_detail("(1,"),
            "invalid input syntax for cube|syntax error at end of input"
        );
        // Empty input.
        assert_eq!(
            err_detail(""),
            "invalid input syntax for cube|syntax error at end of input"
        );
        // "1e" scans as float "1" + garbage 'e'; error is the trailing 'e'
        // (top level expects end of input after the list).
        assert_eq!(
            err_detail("1e"),
            "invalid input syntax for cube|syntax error at or near \"e\""
        );
    }

    #[test]
    fn c_error_texts_semantic() {
        assert_eq!(
            err_detail("(1,2),(3)"),
            "invalid input syntax for cube|Different point dimensions in (1,2) and (3)."
        );
        let many = (0..101).map(|_| "0").collect::<Vec<_>>().join(",");
        assert_eq!(
            err_detail(&format!("({many})")),
            "invalid input syntax for cube|A cube cannot have more than 100 dimensions."
        );
        // Bad value: float8in_internal's out-of-range error.
        let e = parse("-1e-700").unwrap_err();
        assert_eq!(
            e.message(),
            "\"-1e-700\" is out of range for type double precision"
        );
    }
}
