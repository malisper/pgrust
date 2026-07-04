//! Hand-written equivalent of the flex scanner in jsonpath_scan.l: longest
//! match, ties to the earliest rule, exclusive states xq/xnq/xvq/xc.

use ::mcx::{Mcx, PgVec};
use ::pgstrcasecmp::pg_strncasecmp;
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use ::types_error::{
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_SYNTAX_ERROR, ERRCODE_UNTRANSLATABLE_CHARACTER,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Token {
    ToP,
    NullP,
    TrueP,
    FalseP,
    IsP,
    UnknownP,
    ExistsP,
    IdentP,
    StringP,
    NumericP,
    IntP,
    VariableP,
    OrP,
    AndP,
    NotP,
    LessP,
    LessEqualP,
    EqualP,
    NotEqualP,
    GreaterEqualP,
    GreaterP,
    AnyP,
    StrictP,
    LaxP,
    LastP,
    StartsP,
    WithP,
    LikeRegexP,
    FlagP,
    AbsP,
    SizeP,
    TypeP,
    FloorP,
    DoubleP,
    CeilingP,
    KeyValueP,
    DatetimeP,
    BigintP,
    BooleanP,
    DateP,
    DecimalP,
    IntegerP,
    NumberP,
    StringFuncP,
    TimeP,
    TimeTzP,
    TimestampP,
    TimestampTzP,
    Char(u8),
}

pub struct Lexeme<'mcx> {
    pub token: Token,
    pub value: Option<&'mcx [u8]>,
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Initial,
    Xq,
    Xnq,
    Xvq,
    Xc,
}

pub struct Lexer<'a, 'mcx> {
    input: &'a [u8],
    pos: usize,
    state: State,
    scanstring: PgVec<'mcx, u8>,
    mcx: Mcx<'mcx>,
}

struct Keyword {
    len: i32,
    lowercase: bool,
    val: Token,
    keyword: &'static [u8],
}

// Sorted by length then alphabetically (checkKeyword binary-search order).
static KEYWORDS: &[Keyword] = &[
    Keyword { len: 2, lowercase: false, val: Token::IsP, keyword: b"is" },
    Keyword { len: 2, lowercase: false, val: Token::ToP, keyword: b"to" },
    Keyword { len: 3, lowercase: false, val: Token::AbsP, keyword: b"abs" },
    Keyword { len: 3, lowercase: false, val: Token::LaxP, keyword: b"lax" },
    Keyword { len: 4, lowercase: false, val: Token::DateP, keyword: b"date" },
    Keyword { len: 4, lowercase: false, val: Token::FlagP, keyword: b"flag" },
    Keyword { len: 4, lowercase: false, val: Token::LastP, keyword: b"last" },
    Keyword { len: 4, lowercase: true, val: Token::NullP, keyword: b"null" },
    Keyword { len: 4, lowercase: false, val: Token::SizeP, keyword: b"size" },
    Keyword { len: 4, lowercase: false, val: Token::TimeP, keyword: b"time" },
    Keyword { len: 4, lowercase: true, val: Token::TrueP, keyword: b"true" },
    Keyword { len: 4, lowercase: false, val: Token::TypeP, keyword: b"type" },
    Keyword { len: 4, lowercase: false, val: Token::WithP, keyword: b"with" },
    Keyword { len: 5, lowercase: true, val: Token::FalseP, keyword: b"false" },
    Keyword { len: 5, lowercase: false, val: Token::FloorP, keyword: b"floor" },
    Keyword { len: 6, lowercase: false, val: Token::BigintP, keyword: b"bigint" },
    Keyword { len: 6, lowercase: false, val: Token::DoubleP, keyword: b"double" },
    Keyword { len: 6, lowercase: false, val: Token::ExistsP, keyword: b"exists" },
    Keyword { len: 6, lowercase: false, val: Token::NumberP, keyword: b"number" },
    Keyword { len: 6, lowercase: false, val: Token::StartsP, keyword: b"starts" },
    Keyword { len: 6, lowercase: false, val: Token::StrictP, keyword: b"strict" },
    Keyword { len: 6, lowercase: false, val: Token::StringFuncP, keyword: b"string" },
    Keyword { len: 7, lowercase: false, val: Token::BooleanP, keyword: b"boolean" },
    Keyword { len: 7, lowercase: false, val: Token::CeilingP, keyword: b"ceiling" },
    Keyword { len: 7, lowercase: false, val: Token::DecimalP, keyword: b"decimal" },
    Keyword { len: 7, lowercase: false, val: Token::IntegerP, keyword: b"integer" },
    Keyword { len: 7, lowercase: false, val: Token::TimeTzP, keyword: b"time_tz" },
    Keyword { len: 7, lowercase: false, val: Token::UnknownP, keyword: b"unknown" },
    Keyword { len: 8, lowercase: false, val: Token::DatetimeP, keyword: b"datetime" },
    Keyword { len: 8, lowercase: false, val: Token::KeyValueP, keyword: b"keyvalue" },
    Keyword { len: 9, lowercase: false, val: Token::TimestampP, keyword: b"timestamp" },
    Keyword { len: 10, lowercase: false, val: Token::LikeRegexP, keyword: b"like_regex" },
    Keyword { len: 12, lowercase: false, val: Token::TimestampTzP, keyword: b"timestamp_tz" },
];

fn strncmp(a: &[u8], b: &[u8], n: usize) -> i32 {
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        if ca != cb {
            return ca as i32 - cb as i32;
        }
        if ca == 0 {
            return 0;
        }
    }
    0
}

fn check_keyword(s: &[u8]) -> Token {
    let mut res = Token::IdentP;
    let slen = s.len() as i32;
    if slen > KEYWORDS[KEYWORDS.len() - 1].len {
        return res;
    }
    let mut lo = 0usize;
    let mut hi = KEYWORDS.len();
    while lo < hi {
        let mid = lo + ((hi - lo) >> 1);
        let kw = &KEYWORDS[mid];
        let diff = if kw.len == slen {
            pg_strncasecmp(kw.keyword, s, slen as usize)
        } else {
            kw.len - slen
        };
        if diff < 0 {
            lo = mid + 1;
        } else if diff > 0 {
            hi = mid;
        } else {
            let fdiff = if kw.lowercase { strncmp(kw.keyword, s, slen as usize) } else { 0 };
            if fdiff == 0 {
                res = kw.val;
            }
            break;
        }
    }
    res
}

// flex classes: special / blank / other.
fn is_special(c: u8) -> bool {
    matches!(
        c,
        b'?' | b'%' | b'$' | b'.' | b'[' | b']' | b'{' | b'}' | b'(' | b')' | b'|' | b'&'
            | b'!' | b'=' | b'<' | b'>' | b'@' | b'#' | b',' | b'*' | b':' | b'-' | b'+' | b'/'
    )
}

fn is_blank(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0C)
}

fn is_other(c: u8) -> bool {
    !is_special(c) && !is_blank(c) && c != b'\\' && c != b'"'
}

fn is_utf16_surrogate_first(c: i32) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}

fn is_utf16_surrogate_second(c: i32) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}

fn surrogate_pair_to_codepoint(first: i32, second: i32) -> i32 {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

// Numeric-literal patterns (ECMAScript form, '_' digit separators).
fn is_dec(c: u8) -> bool {
    c.is_ascii_digit()
}

fn match_digits(s: &[u8], p: usize, cls: fn(u8) -> bool) -> Option<usize> {
    if !s.get(p).copied().is_some_and(cls) {
        return None;
    }
    let mut i = p + 1;
    loop {
        if s.get(i) == Some(&b'_') {
            if s.get(i + 1).copied().is_some_and(cls) {
                i += 2;
                continue;
            }
            break;
        }
        if s.get(i).copied().is_some_and(cls) {
            i += 1;
            continue;
        }
        break;
    }
    Some(i)
}

fn match_decinteger(s: &[u8], p: usize) -> Option<usize> {
    match s.get(p).copied() {
        Some(b'0') => Some(p + 1),
        Some(c) if (b'1'..=b'9').contains(&c) => {
            let mut i = p + 1;
            loop {
                if s.get(i) == Some(&b'_') {
                    if s.get(i + 1).copied().is_some_and(is_dec) {
                        i += 2;
                        continue;
                    }
                    break;
                }
                if s.get(i).copied().is_some_and(is_dec) {
                    i += 1;
                    continue;
                }
                break;
            }
            Some(i)
        }
        _ => None,
    }
}

fn match_decimal(s: &[u8], p: usize) -> Option<usize> {
    if let Some(after_int) = match_decinteger(s, p) {
        if s.get(after_int) == Some(&b'.') {
            let after_dot = after_int + 1;
            let end = match_digits(s, after_dot, is_dec).unwrap_or(after_dot);
            return Some(end);
        }
    }
    if s.get(p) == Some(&b'.') {
        if let Some(end) = match_digits(s, p + 1, is_dec) {
            return Some(end);
        }
    }
    None
}

fn match_real(s: &[u8], p: usize) -> Option<usize> {
    let after_mant = match_decimal(s, p).or_else(|| match_decinteger(s, p))?;
    if !matches!(s.get(after_mant), Some(&b'E') | Some(&b'e')) {
        return None;
    }
    let mut i = after_mant + 1;
    if matches!(s.get(i), Some(&b'-') | Some(&b'+')) {
        i += 1;
    }
    match_digits(s, i, is_dec)
}

fn match_realfail(s: &[u8], p: usize) -> Option<usize> {
    let after_mant = match_decimal(s, p).or_else(|| match_decinteger(s, p))?;
    if !matches!(s.get(after_mant), Some(&b'E') | Some(&b'e')) {
        return None;
    }
    let i = after_mant + 1;
    if matches!(s.get(i), Some(&b'-') | Some(&b'+')) {
        Some(i + 1)
    } else {
        None
    }
}

fn match_prefixed_int(s: &[u8], p: usize, prefix: (u8, u8), cls: fn(u8) -> bool) -> Option<usize> {
    if s.get(p) != Some(&b'0') {
        return None;
    }
    let c1 = s.get(p + 1).copied()?;
    if c1 != prefix.0 && c1 != prefix.1 {
        return None;
    }
    match_digits(s, p + 2, cls)
}

// Escape patterns shared by <xnq,xq,xvq>.
fn match_unicode(s: &[u8], p: usize) -> Option<usize> {
    if s.get(p) != Some(&b'\\') || s.get(p + 1) != Some(&b'u') {
        return None;
    }
    let q = p + 2;
    if s.get(q) == Some(&b'{') {
        let mut i = q + 1;
        let mut n = 0;
        while n < 6 && s.get(i).is_some_and(|c| c.is_ascii_hexdigit()) {
            i += 1;
            n += 1;
        }
        if n >= 1 && s.get(i) == Some(&b'}') {
            return Some(i + 1 - p);
        }
        None
    } else if (0..4).all(|k| s.get(q + k).is_some_and(|c| c.is_ascii_hexdigit())) {
        Some(q + 4 - p)
    } else {
        None
    }
}

fn match_unicode_plus(s: &[u8], p: usize) -> Option<usize> {
    let first = match_unicode(s, p)?;
    let mut total = first;
    while let Some(n) = match_unicode(s, p + total) {
        total += n;
    }
    Some(total)
}

fn match_unicodefail(s: &[u8], p: usize) -> Option<usize> {
    if s.get(p) != Some(&b'\\') || s.get(p + 1) != Some(&b'u') {
        return None;
    }
    let q = p + 2;
    if s.get(q) == Some(&b'{') {
        let mut i = q + 1;
        let mut n = 0;
        while n < 6 && s.get(i).is_some_and(|c| c.is_ascii_hexdigit()) {
            i += 1;
            n += 1;
        }
        Some(i - p)
    } else {
        let mut i = q;
        let mut n = 0;
        while n < 3 && s.get(i).is_some_and(|c| c.is_ascii_hexdigit()) {
            i += 1;
            n += 1;
        }
        Some(i - p)
    }
}

fn match_hex_char(s: &[u8], p: usize) -> Option<usize> {
    if s.get(p) == Some(&b'\\')
        && s.get(p + 1) == Some(&b'x')
        && s.get(p + 2).is_some_and(|c| c.is_ascii_hexdigit())
        && s.get(p + 3).is_some_and(|c| c.is_ascii_hexdigit())
    {
        Some(4)
    } else {
        None
    }
}

fn match_hex_fail(s: &[u8], p: usize) -> Option<usize> {
    if s.get(p) == Some(&b'\\') && s.get(p + 1) == Some(&b'x') {
        if s.get(p + 2).is_some_and(|c| c.is_ascii_hexdigit()) {
            Some(3)
        } else {
            Some(2)
        }
    } else {
        None
    }
}

enum Step<'mcx> {
    Emit(Lexeme<'mcx>),
    Continue,
    Terminate,
}

#[derive(Clone, Copy)]
enum Which {
    None,
    Fixed2,
    UnicodePlus,
    HexChar,
    UnicodeFail,
    HexFail,
    UnicodePlusBackslash,
    Dot,
    Backslash,
}

fn consider(best: &mut usize, which: &mut Which, cand: Option<usize>, w: Which) {
    if let Some(n) = cand {
        if n > *best {
            *best = n;
            *which = w;
        }
    }
}

fn emit(token: Token) -> Step<'static> {
    Step::Emit(Lexeme { token, value: None, start: 0, end: 0 })
}

impl<'a, 'mcx> Lexer<'a, 'mcx> {
    pub fn new(mcx: Mcx<'mcx>, input: &'a [u8]) -> Self {
        Lexer { input, pos: 0, state: State::Initial, scanstring: PgVec::new_in(mcx), mcx }
    }

    fn ss_init(&mut self) {
        self.scanstring = PgVec::new_in(self.mcx);
    }

    fn ss_add(&mut self, bytes: &[u8]) -> PgResult<()> {
        ::mcx::vec_append_bytes(&mut self.scanstring, bytes)
    }

    fn ss_push(&mut self, c: u8) -> PgResult<()> {
        ::mcx::vec_append_bytes(&mut self.scanstring, &[c])
    }

    // C's per-token palloc'd scanstring buffer: leaked into the mcx, bulk
    // freed at context reset.
    fn ss_take(&mut self) -> &'mcx [u8] {
        core::mem::replace(&mut self.scanstring, PgVec::new_in(self.mcx)).leak()
    }

    fn emit_value(&mut self, token: Token) -> Step<'mcx> {
        let value = Some(self.ss_take());
        Step::Emit(Lexeme { token, value, start: 0, end: 0 })
    }

    fn emit_keyword(&mut self) -> Step<'mcx> {
        let tok = check_keyword(&self.scanstring);
        self.emit_value(tok)
    }

    fn yyerror(
        &self,
        escontext: &mut Option<&mut SoftErrorContext>,
        message: &str,
    ) -> PgResult<()> {
        jsonpath_yyerror(escontext.as_deref_mut(), self.input, self.pos, message)
    }

    fn yyerror_yytext(
        &self,
        escontext: &mut Option<&mut SoftErrorContext>,
        start: usize,
        end: usize,
        message: &str,
    ) -> PgResult<()> {
        jsonpath_yyerror_yytext(escontext.as_deref_mut(), &self.input[start..end], message)
    }

    fn add_unicode_char(
        &mut self,
        ch: i32,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        if ch == 0 {
            ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error("unsupported Unicode escape sequence")
                    .with_sqlstate(ERRCODE_UNTRANSLATABLE_CHARACTER)
                    .with_detail("\\u0000 cannot be converted to text."),
            )?;
            return Ok(false);
        }
        if escontext.is_none() {
            let cbuf = mbutils::pg_unicode_to_server(self.mcx, ch as u32)?;
            self.ss_add(&cbuf)?;
        } else {
            match mbutils::pg_unicode_to_server_noerror(self.mcx, ch as u32)? {
                Some(cbuf) => self.ss_add(&cbuf)?,
                None => {
                    ereturn(
                        escontext.as_deref_mut(),
                        false,
                        PgError::error("could not convert Unicode to server encoding")
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                    )?;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn add_unicode(
        &mut self,
        mut ch: i32,
        hi_surrogate: &mut i32,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        if is_utf16_surrogate_first(ch) {
            if *hi_surrogate != -1 {
                ereturn(
                    escontext.as_deref_mut(),
                    false,
                    PgError::error("invalid input syntax for type jsonpath")
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                        .with_detail("Unicode high surrogate must not follow a high surrogate."),
                )?;
                return Ok(false);
            }
            *hi_surrogate = ch;
            return Ok(true);
        } else if is_utf16_surrogate_second(ch) {
            if *hi_surrogate == -1 {
                ereturn(
                    escontext.as_deref_mut(),
                    false,
                    PgError::error("invalid input syntax for type jsonpath")
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                        .with_detail("Unicode low surrogate must follow a high surrogate."),
                )?;
                return Ok(false);
            }
            ch = surrogate_pair_to_codepoint(*hi_surrogate, ch);
            *hi_surrogate = -1;
        } else if *hi_surrogate != -1 {
            ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error("invalid input syntax for type jsonpath")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_detail("Unicode low surrogate must follow a high surrogate."),
            )?;
            return Ok(false);
        }
        self.add_unicode_char(ch, escontext)
    }

    fn hexval(
        &self,
        c: u8,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<i32>> {
        if c.is_ascii_digit() {
            return Ok(Some((c - b'0') as i32));
        }
        if (b'a'..=b'f').contains(&c) {
            return Ok(Some((c - b'a') as i32 + 0xA));
        }
        if (b'A'..=b'F').contains(&c) {
            return Ok(Some((c - b'A') as i32 + 0xA));
        }
        self.yyerror(escontext, "invalid hexadecimal digit")?;
        Ok(None)
    }

    // C stride `i += 2` between concatenated escapes: the inner loops leave i
    // just past the escape body; the stride steps over the next `\u`.
    fn parse_unicode(
        &mut self,
        s: &[u8],
        l: usize,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        let mut hi_surrogate = -1i32;
        let mut i = 2usize;
        while i < l {
            let mut ch = 0i32;
            if s[i] == b'{' {
                loop {
                    i += 1;
                    if !(i < l && s[i] != b'}') {
                        break;
                    }
                    match self.hexval(s[i], escontext)? {
                        Some(si) => ch = (ch << 4) | si,
                        None => return Ok(false),
                    }
                }
                i += 1;
            } else {
                let mut j = 0;
                while j < 4 && i < l {
                    match self.hexval(s[i], escontext)? {
                        Some(si) => ch = (ch << 4) | si,
                        None => return Ok(false),
                    }
                    i += 1;
                    j += 1;
                }
            }
            if !self.add_unicode(ch, &mut hi_surrogate, escontext)? {
                return Ok(false);
            }
            i += 2;
        }
        if hi_surrogate != -1 {
            ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error("invalid input syntax for type jsonpath")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_detail("Unicode low surrogate must follow a high surrogate."),
            )?;
            return Ok(false);
        }
        Ok(true)
    }

    fn parse_hex_char(
        &mut self,
        s: &[u8],
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        let s2 = match self.hexval(s[2], escontext)? {
            Some(v) => v,
            None => return Ok(false),
        };
        let s3 = match self.hexval(s[3], escontext)? {
            Some(v) => v,
            None => return Ok(false),
        };
        self.add_unicode_char((s2 << 4) | s3, escontext)
    }

    pub fn next_token(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<Lexeme<'mcx>>> {
        loop {
            let start = self.pos;
            let step = match self.state {
                State::Initial => self.scan_initial(escontext)?,
                State::Xnq => self.scan_xnq(escontext)?,
                State::Xq => self.scan_xq(escontext)?,
                State::Xvq => self.scan_xvq(escontext)?,
                State::Xc => self.scan_xc(escontext)?,
            };
            match step {
                Step::Emit(mut lex) => {
                    lex.start = start;
                    lex.end = self.pos;
                    return Ok(Some(lex));
                }
                Step::Continue => continue,
                Step::Terminate => return Ok(None),
            }
        }
    }

    fn scan_initial(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Step<'mcx>> {
        let s = self.input;
        let p = self.pos;

        if p >= s.len() {
            return Ok(Step::Terminate);
        }

        if s[p] == b'&' && s.get(p + 1) == Some(&b'&') {
            self.pos += 2;
            return Ok(emit(Token::AndP));
        }
        if s[p] == b'|' && s.get(p + 1) == Some(&b'|') {
            self.pos += 2;
            return Ok(emit(Token::OrP));
        }
        if s[p] == b'*' && s.get(p + 1) == Some(&b'*') {
            self.pos += 2;
            return Ok(emit(Token::AnyP));
        }
        if s[p] == b'<' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::LessEqualP));
        }
        if s[p] == b'=' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::EqualP));
        }
        if s[p] == b'<' && s.get(p + 1) == Some(&b'>') {
            self.pos += 2;
            return Ok(emit(Token::NotEqualP));
        }
        if s[p] == b'!' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::NotEqualP));
        }
        if s[p] == b'>' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::GreaterEqualP));
        }
        if s[p] == b'!' {
            self.pos += 1;
            return Ok(emit(Token::NotP));
        }
        if s[p] == b'<' {
            self.pos += 1;
            return Ok(emit(Token::LessP));
        }
        if s[p] == b'>' {
            self.pos += 1;
            return Ok(emit(Token::GreaterP));
        }

        if s[p] == b'$' && s.get(p + 1).copied().is_some_and(is_other) {
            let mut q = p + 1;
            while q < s.len() && is_other(s[q]) {
                q += 1;
            }
            self.ss_init();
            self.ss_add(&s[p + 1..q])?;
            self.pos = q;
            return Ok(self.emit_value(Token::VariableP));
        }

        if s[p] == b'$' && s.get(p + 1) == Some(&b'"') {
            self.ss_init();
            self.pos += 2;
            self.state = State::Xvq;
            return Ok(Step::Continue);
        }

        if s[p] == b'/' && s.get(p + 1) == Some(&b'*') {
            self.ss_init();
            self.pos += 2;
            self.state = State::Xc;
            return Ok(Step::Continue);
        }

        if s[p] == b'"' {
            self.ss_init();
            self.pos += 1;
            self.state = State::Xq;
            return Ok(Step::Continue);
        }

        if let Some(step) = self.scan_number(p, escontext)? {
            return Ok(step);
        }

        if is_special(s[p]) {
            let c = s[p];
            self.pos += 1;
            return Ok(emit(Token::Char(c)));
        }

        if is_blank(s[p]) {
            let mut q = p;
            while q < s.len() && is_blank(s[q]) {
                q += 1;
            }
            self.pos = q;
            return Ok(Step::Continue);
        }

        if s[p] == b'\\' {
            // yyless(0): xnq's shared_escape consumes it.
            self.ss_init();
            self.state = State::Xnq;
            return Ok(Step::Continue);
        }

        if is_other(s[p]) {
            let mut q = p;
            while q < s.len() && is_other(s[q]) {
                q += 1;
            }
            self.ss_init();
            self.ss_add(&s[p..q])?;
            self.pos = q;
            self.state = State::Xnq;
            return Ok(Step::Continue);
        }

        self.ss_init();
        self.ss_add(&s[p..p + 1])?;
        self.pos += 1;
        self.state = State::Xnq;
        Ok(Step::Continue)
    }

    fn scan_number(
        &mut self,
        p: usize,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<Step<'mcx>>> {
        let s = self.input;

        let real = match_real(s, p);
        let decimal = match_decimal(s, p);
        let decint = match_decinteger(s, p);
        let hexint = match_prefixed_int(s, p, (b'x', b'X'), |c| c.is_ascii_hexdigit());
        let octint = match_prefixed_int(s, p, (b'o', b'O'), |c| (b'0'..=b'7').contains(&c));
        let binint = match_prefixed_int(s, p, (b'b', b'B'), |c| c == b'0' || c == b'1');

        let junk = |base: Option<usize>| -> Option<usize> {
            base.and_then(|e| {
                if s.get(e).copied().is_some_and(is_other) {
                    Some(e + 1)
                } else {
                    None
                }
            })
        };
        let realfail = match_realfail(s, p);
        let decint_junk = junk(decint);
        let decimal_junk = junk(decimal);
        let real_junk = junk(real);

        #[derive(Clone, Copy)]
        enum Kind {
            Numeric,
            Int,
            RealFail,
            Junk,
            None,
        }

        let candidates: [(Option<usize>, Kind); 10] = [
            (real, Kind::Numeric),
            (decimal, Kind::Numeric),
            (decint, Kind::Int),
            (hexint, Kind::Int),
            (octint, Kind::Int),
            (binint, Kind::Int),
            (realfail, Kind::RealFail),
            (decint_junk, Kind::Junk),
            (decimal_junk, Kind::Junk),
            (real_junk, Kind::Junk),
        ];

        let mut best_len = 0usize;
        let mut best_kind = Kind::None;
        for (cand, kind) in candidates {
            if let Some(n) = cand {
                let len = n - p;
                if len > best_len {
                    best_len = len;
                    best_kind = kind;
                }
            }
        }
        if matches!(best_kind, Kind::None) {
            return Ok(None);
        }

        // flex global longest-match: when {other}+ runs strictly longer than
        // the best numeric candidate, the catch-all wins the token.
        let mut other_len = 0usize;
        while p + other_len < s.len() && is_other(s[p + other_len]) {
            other_len += 1;
        }
        if other_len > best_len {
            return Ok(None);
        }

        match best_kind {
            Kind::None => Ok(None),
            Kind::Numeric | Kind::Int => {
                self.ss_init();
                self.ss_add(&s[p..p + best_len])?;
                self.pos = p + best_len;
                let tok = if matches!(best_kind, Kind::Numeric) {
                    Token::NumericP
                } else {
                    Token::IntP
                };
                Ok(Some(self.emit_value(tok)))
            }
            Kind::RealFail => {
                self.yyerror_yytext(escontext, p, p + best_len, "invalid numeric literal")?;
                self.pos = p + best_len;
                Ok(Some(Step::Terminate))
            }
            Kind::Junk => {
                self.yyerror_yytext(
                    escontext,
                    p,
                    p + best_len,
                    "trailing junk after numeric literal",
                )?;
                self.pos = p + best_len;
                Ok(Some(Step::Terminate))
            }
        }
    }

    fn shared_escape(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<Step<'mcx>>> {
        let s = self.input;
        let p = self.pos;
        match s.get(p) {
            Some(&b'\\') => {}
            _ => return Ok(None),
        }

        let uni_plus = match_unicode_plus(s, p);
        let uni_plus_bs = uni_plus.and_then(|n| {
            if s.get(p + n) == Some(&b'\\') {
                Some(n + 1)
            } else {
                None
            }
        });
        let unifail = {
            let mut q = p;
            while let Some(n) = match_unicode(s, q) {
                q += n;
            }
            match_unicodefail(s, q).map(|n| (q - p) + n)
        };
        let hexc = match_hex_char(s, p);
        let hexf = match_hex_fail(s, p);
        let fixed2 = matches!(s.get(p + 1), Some(&b'b' | &b'f' | &b'n' | &b'r' | &b't' | &b'v'));

        let mut best: usize = 0;
        let mut which = Which::None;
        consider(&mut best, &mut which, fixed2.then_some(2), Which::Fixed2);
        consider(&mut best, &mut which, uni_plus, Which::UnicodePlus);
        consider(&mut best, &mut which, hexc, Which::HexChar);
        consider(&mut best, &mut which, unifail, Which::UnicodeFail);
        consider(&mut best, &mut which, hexf, Which::HexFail);
        consider(&mut best, &mut which, uni_plus_bs, Which::UnicodePlusBackslash);
        // flex `.` excludes newline: backslash+LF falls to the lone-\\ rule.
        let dot = if s.get(p + 1).is_some_and(|&c| c != b'\n') { Some(2) } else { None };
        consider(&mut best, &mut which, dot, Which::Dot);
        consider(&mut best, &mut which, Some(1), Which::Backslash);

        match which {
            Which::Fixed2 => {
                let ch = match s[p + 1] {
                    b'b' => 0x08,
                    b'f' => 0x0C,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'v' => 0x0B,
                    _ => unreachable!(),
                };
                self.ss_push(ch)?;
                self.pos += 2;
                Ok(Some(Step::Continue))
            }
            Which::UnicodePlus => {
                let n = best;
                self.pos += n;
                let text: &[u8] = &s[p..p + n];
                if !self.parse_unicode(text, n, escontext)? {
                    return Ok(Some(Step::Terminate));
                }
                Ok(Some(Step::Continue))
            }
            Which::HexChar => {
                self.pos += 4;
                let text: &[u8] = &s[p..p + 4];
                if !self.parse_hex_char(text, escontext)? {
                    return Ok(Some(Step::Terminate));
                }
                Ok(Some(Step::Continue))
            }
            Which::UnicodeFail => {
                self.yyerror_yytext(escontext, p, p + best, "invalid Unicode escape sequence")?;
                self.pos += best;
                Ok(Some(Step::Terminate))
            }
            Which::HexFail => {
                self.yyerror_yytext(
                    escontext,
                    p,
                    p + best,
                    "invalid hexadecimal character sequence",
                )?;
                self.pos += best;
                Ok(Some(Step::Terminate))
            }
            Which::UnicodePlusBackslash => {
                // C: yyless(yyleng - 1) throws back the trailing backslash.
                let n = best - 1;
                self.pos += n;
                let text: &[u8] = &s[p..p + n];
                if !self.parse_unicode(text, n, escontext)? {
                    return Ok(Some(Step::Terminate));
                }
                Ok(Some(Step::Continue))
            }
            Which::Dot => {
                self.ss_push(s[p + 1])?;
                self.pos += 2;
                Ok(Some(Step::Continue))
            }
            Which::Backslash => {
                self.yyerror_yytext(escontext, p, p + 1, "unexpected end after backslash")?;
                self.pos += 1;
                Ok(Some(Step::Terminate))
            }
            Which::None => Ok(None),
        }
    }

    fn scan_xnq(&mut self, escontext: &mut Option<&mut SoftErrorContext>) -> PgResult<Step<'mcx>> {
        if let Some(step) = self.shared_escape(escontext)? {
            return Ok(step);
        }

        let s = self.input;
        let p = self.pos;

        if p >= s.len() {
            self.state = State::Initial;
            return Ok(self.emit_keyword());
        }

        if is_other(s[p]) {
            let mut q = p;
            while q < s.len() && is_other(s[q]) {
                q += 1;
            }
            self.ss_add(&s[p..q])?;
            self.pos = q;
            return Ok(Step::Continue);
        }

        if is_blank(s[p]) {
            let mut q = p;
            while q < s.len() && is_blank(s[q]) {
                q += 1;
            }
            self.pos = q;
            self.state = State::Initial;
            return Ok(self.emit_keyword());
        }

        if s[p] == b'/' && s.get(p + 1) == Some(&b'*') {
            self.pos += 2;
            self.state = State::Xc;
            return Ok(Step::Continue);
        }

        if is_special(s[p]) || s[p] == b'"' {
            // yyless(0): the special/quote is re-scanned in INITIAL.
            self.state = State::Initial;
            return Ok(self.emit_keyword());
        }

        self.ss_add(&s[p..p + 1])?;
        self.pos += 1;
        Ok(Step::Continue)
    }

    fn scan_xq(&mut self, escontext: &mut Option<&mut SoftErrorContext>) -> PgResult<Step<'mcx>> {
        if let Some(step) = self.shared_escape(escontext)? {
            return Ok(step);
        }

        let s = self.input;
        let p = self.pos;

        if p >= s.len() {
            self.yyerror(escontext, "unterminated quoted string")?;
            return Ok(Step::Terminate);
        }

        if s[p] == b'"' {
            self.pos += 1;
            self.state = State::Initial;
            return Ok(self.emit_value(Token::StringP));
        }

        if s[p] != b'\\' && s[p] != b'"' {
            let mut q = p;
            while q < s.len() && s[q] != b'\\' && s[q] != b'"' {
                q += 1;
            }
            self.ss_add(&s[p..q])?;
            self.pos = q;
            return Ok(Step::Continue);
        }

        self.ss_add(&s[p..p + 1])?;
        self.pos += 1;
        Ok(Step::Continue)
    }

    fn scan_xvq(&mut self, escontext: &mut Option<&mut SoftErrorContext>) -> PgResult<Step<'mcx>> {
        if let Some(step) = self.shared_escape(escontext)? {
            return Ok(step);
        }

        let s = self.input;
        let p = self.pos;

        if p >= s.len() {
            self.yyerror(escontext, "unterminated quoted string")?;
            return Ok(Step::Terminate);
        }

        if s[p] == b'"' {
            self.pos += 1;
            self.state = State::Initial;
            return Ok(self.emit_value(Token::VariableP));
        }

        if s[p] != b'\\' && s[p] != b'"' {
            let mut q = p;
            while q < s.len() && s[q] != b'\\' && s[q] != b'"' {
                q += 1;
            }
            self.ss_add(&s[p..q])?;
            self.pos = q;
            return Ok(Step::Continue);
        }

        self.ss_add(&s[p..p + 1])?;
        self.pos += 1;
        Ok(Step::Continue)
    }

    fn scan_xc(&mut self, escontext: &mut Option<&mut SoftErrorContext>) -> PgResult<Step<'mcx>> {
        let s = self.input;
        let p = self.pos;

        if p >= s.len() {
            self.yyerror(escontext, "unexpected end of comment")?;
            return Ok(Step::Terminate);
        }

        if s[p] == b'*' && s.get(p + 1) == Some(&b'/') {
            self.pos += 2;
            self.state = State::Initial;
            return Ok(Step::Continue);
        }

        if s[p] != b'*' {
            let mut q = p;
            while q < s.len() && s[q] != b'*' {
                q += 1;
            }
            self.pos = q;
            return Ok(Step::Continue);
        }

        self.pos += 1;
        Ok(Step::Continue)
    }
}

pub fn jsonpath_yyerror(
    escontext: Option<&mut SoftErrorContext>,
    input: &[u8],
    pos: usize,
    message: &str,
) -> PgResult<()> {
    if let Some(ctx) = escontext.as_ref() {
        if ctx.error_occurred() {
            return Ok(());
        }
    }
    let yytext: &[u8] = if pos >= input.len() { &[] } else { &input[pos..] };
    jsonpath_yyerror_yytext(escontext, yytext, message)
}

pub fn jsonpath_yyerror_yytext(
    escontext: Option<&mut SoftErrorContext>,
    yytext: &[u8],
    message: &str,
) -> PgResult<()> {
    if let Some(ctx) = escontext.as_ref() {
        if ctx.error_occurred() {
            return Ok(());
        }
    }
    if yytext.is_empty() {
        ereturn(
            escontext,
            (),
            PgError::error(format!("{message} at end of jsonpath input"))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )
    } else {
        let near = String::from_utf8_lossy(yytext);
        ereturn(
            escontext,
            (),
            PgError::error(format!("{message} at or near \"{near}\" of jsonpath input"))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )
    }
}
