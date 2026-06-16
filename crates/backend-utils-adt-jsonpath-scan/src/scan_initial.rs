//! The `INITIAL` start-condition rules plus the public token driver. This is
//! the top-level flex rule set (operators, `{special}`, blanks, comment/string/
//! variable starts, and the numeric-literal family) and the [`next_token`]
//! dispatcher that drives all five states to a finished token.

use types_error::{PgResult, SoftErrorContext};

use crate::scan_states::Step;
use crate::{is_other, JsonPathLexer, Lexeme, ScanBuf, State, Token};

// ---------------------------------------------------------------------------
// Numeric-literal pattern matchers (flex definitions).
//   decinteger  = (0|[1-9](_?{decdigit})*)
//   decdigits   = {decdigit}(_?{decdigit})*
//   hexinteger  = 0[xX]{hexdigit}(_?{hexdigit})*
//   octinteger  = 0[oO]{octdigit}(_?{octdigit})*
//   bininteger  = 0[bB]{bindigit}(_?{bindigit})*
//   decimal     = ({decinteger}\.{decdigits}?|\.{decdigits})
//   real        = ({decinteger}|{decimal})[Ee][-+]?{decdigits}
//   realfail    = ({decinteger}|{decimal})[Ee][-+]
//   *_junk      = <pattern>{other}
// ---------------------------------------------------------------------------

fn is_dec(c: u8) -> bool {
    c.is_ascii_digit()
}
fn is_hex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}
fn is_oct(c: u8) -> bool {
    (b'0'..=b'7').contains(&c)
}
fn is_bin(c: u8) -> bool {
    c == b'0' || c == b'1'
}

/// Match `{digit}(_?{digit})*` starting at `p`, with `digit` selected by `cls`.
/// Returns the end offset (exclusive) past the digit run, or `None` if there is
/// not even one leading digit.
fn match_digits(s: &[u8], p: usize, cls: fn(u8) -> bool) -> Option<usize> {
    if !s.get(p).copied().is_some_and(cls) {
        return None;
    }
    let mut i = p + 1;
    loop {
        // optional single '_' then a required digit
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

/// Match `decinteger` = `(0|[1-9](_?{decdigit})*)`.
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

/// Match `decimal` = `({decinteger}\.{decdigits}?|\.{decdigits})`.
fn match_decimal(s: &[u8], p: usize) -> Option<usize> {
    // First alternative: decinteger '.' decdigits?
    if let Some(after_int) = match_decinteger(s, p) {
        if s.get(after_int) == Some(&b'.') {
            let after_dot = after_int + 1;
            // optional decdigits
            let end = match_digits(s, after_dot, is_dec).unwrap_or(after_dot);
            return Some(end);
        }
    }
    // Second alternative: '.' decdigits
    if s.get(p) == Some(&b'.') {
        if let Some(end) = match_digits(s, p + 1, is_dec) {
            return Some(end);
        }
    }
    None
}

/// Match `real` = `({decinteger}|{decimal})[Ee][-+]?{decdigits}`.
fn match_real(s: &[u8], p: usize) -> Option<usize> {
    let after_mant = match_decimal(s, p).or_else(|| match_decinteger(s, p))?;
    if !matches!(s.get(after_mant), Some(&b'E') | Some(&b'e')) {
        return None;
    }
    let mut i = after_mant + 1;
    if matches!(s.get(i), Some(&b'-') | Some(&b'+')) {
        i += 1;
    }
    let end = match_digits(s, i, is_dec)?;
    Some(end)
}

/// Match `realfail` = `({decinteger}|{decimal})[Ee][-+]` (no digits after sign).
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

/// Match a non-decimal integer with the given prefix letters and digit class:
/// `0[xX]{hexdigit}(_?{hexdigit})*` and analogues.
fn match_prefixed_int(
    s: &[u8],
    p: usize,
    prefix: (u8, u8),
    cls: fn(u8) -> bool,
) -> Option<usize> {
    if s.get(p) != Some(&b'0') {
        return None;
    }
    let c1 = s.get(p + 1).copied()?;
    if c1 != prefix.0 && c1 != prefix.1 {
        return None;
    }
    match_digits(s, p + 2, cls)
}

impl<'a> JsonPathLexer<'a> {
    /// Produce the next token, or `Ok(None)` at end of input / after a soft
    /// error. Drives all five start conditions to a completed token.
    pub fn next_token(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<Lexeme>> {
        loop {
            let step = match self.state {
                State::Initial => self.scan_initial(escontext)?,
                State::Xnq => self.scan_xnq(escontext)?,
                State::Xq => self.scan_xq(escontext)?,
                State::Xvq => self.scan_xvq(escontext)?,
                State::Xc => self.scan_xc(escontext)?,
            };
            match step {
                Step::Emit(lex) => return Ok(Some(lex)),
                Step::Continue => continue,
                Step::Terminate => return Ok(None),
            }
        }
    }

    /// The `INITIAL` start-condition rules.
    fn scan_initial(&mut self, escontext: &mut Option<&mut SoftErrorContext>) -> PgResult<Step> {
        let s = self.input;
        let p = self.pos;

        // <<EOF>> { yyterminate(); }
        if p >= s.len() {
            return Ok(Step::Terminate);
        }

        // Multi-char operators (longest first, matching flex longest-match).
        // \&\&  \|\|  \*\*  \<\=  \=\=  \<\>  \!\=  \>\=  then single \< \> \!
        if s[p] == b'&' && s.get(p + 1) == Some(&b'&') {
            self.pos += 2;
            return Ok(emit(Token::AND_P));
        }
        if s[p] == b'|' && s.get(p + 1) == Some(&b'|') {
            self.pos += 2;
            return Ok(emit(Token::OR_P));
        }
        if s[p] == b'*' && s.get(p + 1) == Some(&b'*') {
            self.pos += 2;
            return Ok(emit(Token::ANY_P));
        }
        if s[p] == b'<' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::LESSEQUAL_P));
        }
        if s[p] == b'=' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::EQUAL_P));
        }
        if s[p] == b'<' && s.get(p + 1) == Some(&b'>') {
            self.pos += 2;
            return Ok(emit(Token::NOTEQUAL_P));
        }
        if s[p] == b'!' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::NOTEQUAL_P));
        }
        if s[p] == b'>' && s.get(p + 1) == Some(&b'=') {
            self.pos += 2;
            return Ok(emit(Token::GREATEREQUAL_P));
        }
        if s[p] == b'!' {
            self.pos += 1;
            return Ok(emit(Token::NOT_P));
        }
        if s[p] == b'<' {
            self.pos += 1;
            return Ok(emit(Token::LESS_P));
        }
        if s[p] == b'>' {
            self.pos += 1;
            return Ok(emit(Token::GREATER_P));
        }

        // \${other}+  -> bare variable name
        if s[p] == b'$' && s.get(p + 1).copied().is_some_and(is_other) {
            let mut q = p + 1;
            while q < s.len() && is_other(s[q]) {
                q += 1;
            }
            // addstring(true, yytext+1, yyleng-1); addchar(false,'\0');
            self.scanstring.addstring(true, &s[p + 1..q]);
            self.scanstring.addchar(false, 0);
            self.pos = q;
            let value = Some(self.scanstring.clone());
            return Ok(Step::Emit(Lexeme { token: Token::VARIABLE_P, value }));
        }

        // \$\"  -> begin xvq
        if s[p] == b'$' && s.get(p + 1) == Some(&b'"') {
            self.scanstring.addchar(true, 0);
            self.pos += 2;
            self.state = State::Xvq;
            return Ok(Step::Continue);
        }

        // \/\*  -> begin xc
        if s[p] == b'/' && s.get(p + 1) == Some(&b'*') {
            self.scanstring.addchar(true, 0);
            self.pos += 2;
            self.state = State::Xc;
            return Ok(Step::Continue);
        }

        // \"  -> begin xq
        if s[p] == b'"' {
            self.scanstring.addchar(true, 0);
            self.pos += 1;
            self.state = State::Xq;
            return Ok(Step::Continue);
        }

        // Numeric literals. Order mirrors the flex rules: {real} {decimal}
        // {decinteger} {hexinteger} {octinteger} {bininteger}, then the
        // *fail/junk rules. flex still applies global longest-match, so we
        // compute every candidate length and pick the longest (ties by listed
        // order). The "junk" rules are <pattern>{other}, i.e. a valid numeric
        // immediately followed by an `other` char — always one longer than the
        // bare numeric, so they win the longest-match contest when present.
        if let Some(step) = self.scan_number(p, escontext)? {
            return Ok(step);
        }

        // {special}  -> return *yytext
        if crate::is_special(s[p]) {
            let c = s[p];
            self.pos += 1;
            return Ok(Step::Emit(Lexeme { token: Token::Char(c), value: None }));
        }

        // {blank}+  -> ignore
        if crate::is_blank(s[p]) {
            let mut q = p;
            while q < s.len() && crate::is_blank(s[q]) {
                q += 1;
            }
            self.pos = q;
            return Ok(Step::Continue);
        }

        // \\  -> yyless(0); addchar(true,'\0'); BEGIN xnq;
        if s[p] == b'\\' {
            self.scanstring.addchar(true, 0);
            // yyless(0): do not consume; xnq's shared_escape handles it.
            self.state = State::Xnq;
            return Ok(Step::Continue);
        }

        // {other}+  -> addstring(true,...); BEGIN xnq;
        if is_other(s[p]) {
            let mut q = p;
            while q < s.len() && is_other(s[q]) {
                q += 1;
            }
            self.scanstring.addstring(true, &s[p..q]);
            self.pos = q;
            self.state = State::Xnq;
            return Ok(Step::Continue);
        }

        // No rule matched a byte that is not special/blank/other/quote/etc.
        // (8-bit bytes >= 0x80 are `other` per the flex class, so this is only
        // reachable for a stray control byte; flex would report no-match. Treat
        // it as the start of an xnq run to stay progress-making.)
        self.scanstring.addstring(true, &s[p..p + 1]);
        self.pos += 1;
        self.state = State::Xnq;
        Ok(Step::Continue)
    }

    /// The numeric-literal family for the INITIAL state. Returns `Some(step)`
    /// if a numeric (or numeric-fail/junk) rule matched at `p`.
    fn scan_number(
        &mut self,
        p: usize,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<Step>> {
        let s = self.input;

        // Compute candidate lengths for each "valid numeric" rule.
        let real = match_real(s, p);
        let decimal = match_decimal(s, p);
        let decint = match_decinteger(s, p);
        let hexint = match_prefixed_int(s, p, (b'x', b'X'), is_hex);
        let octint = match_prefixed_int(s, p, (b'o', b'O'), is_oct);
        let binint = match_prefixed_int(s, p, (b'b', b'B'), is_bin);

        // "junk" = a valid numeric immediately followed by an {other} char.
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

        // Token kind for each valid-numeric candidate.
        // (real/decimal -> NUMERIC_P; the integer forms -> INT_P.)
        #[derive(Clone, Copy)]
        enum Kind {
            Numeric,
            Int,
            RealFail,
            Junk,
            None,
        }

        // Pick the longest match; ties broken by flex rule order:
        //   {real} {decimal} {decinteger} {hexinteger} {octinteger}
        //   {bininteger} {realfail} {decinteger_junk} {decimal_junk}
        //   {real_junk}
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

        match best_kind {
            Kind::None => Ok(None),
            Kind::Numeric => {
                self.scanstring.addstring(true, &s[p..p + best_len]);
                self.scanstring.addchar(false, 0);
                self.pos = p + best_len;
                let value = Some(self.scanstring.clone());
                Ok(Some(Step::Emit(Lexeme { token: Token::NUMERIC_P, value })))
            }
            Kind::Int => {
                self.scanstring.addstring(true, &s[p..p + best_len]);
                self.scanstring.addchar(false, 0);
                self.pos = p + best_len;
                let value = Some(self.scanstring.clone());
                Ok(Some(Step::Emit(Lexeme { token: Token::INT_P, value })))
            }
            Kind::RealFail => {
                // {realfail} -> yyerror "invalid numeric literal"; yyterminate.
                // C reports at yytext (the literal start), so error before
                // advancing the cursor.
                self.yyerror(escontext, "invalid numeric literal")?;
                self.pos = p + best_len;
                Ok(Some(Step::Terminate))
            }
            Kind::Junk => {
                // {*_junk} -> yyerror "trailing junk after numeric literal".
                self.yyerror(escontext, "trailing junk after numeric literal")?;
                self.pos = p + best_len;
                Ok(Some(Step::Terminate))
            }
        }
    }
}

fn emit(token: Token) -> Step {
    Step::Emit(Lexeme { token, value: None })
}
