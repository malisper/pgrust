//! The exclusive-state scanning rules (`<xnq>`, `<xq>`, `<xvq>`, `<xc>`) plus
//! the shared escape rules that apply in `<xnq,xq,xvq>`. Each function consumes
//! one flex "rule" worth of input from the current cursor and either returns a
//! finished [`Lexeme`] (`Ok(Some(Some(lex)))`), signals "matched, keep
//! scanning" (`Ok(Some(None))`), or signals lexer termination / EOF
//! (`Ok(None)`), mirroring the C `return TOK;` / fallthrough / `yyterminate()`
//! control flow.

use ::types_error::{PgResult, SoftErrorContext};

use crate::{check_keyword, is_other, JsonPathLexer, Lexeme, ScanBuf, State, Token};

/// Outcome of one exclusive-state scan step.
pub(crate) enum Step {
    /// `return TOK` — emit this lexeme.
    Emit(Lexeme),
    /// Matched a rule with an action but no token; continue scanning.
    Continue,
    /// `yyterminate()` / EOF — stop the lexer.
    Terminate,
}

// Shared escape-class helpers --------------------------------------------------

/// flex `unicode` = `\\u({hexdigit}{4}|\{{hexdigit}{1,6}\})`. Returns the match
/// length (including the leading `\u`) if the input at `p` matches, else `None`.
fn match_unicode(s: &[u8], p: usize) -> Option<usize> {
    if s.get(p) != Some(&b'\\') || s.get(p + 1) != Some(&b'u') {
        return None;
    }
    let q = p + 2;
    if s.get(q) == Some(&b'{') {
        // {hexdigit}{1,6}
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
    } else {
        // {hexdigit}{4}
        if (0..4).all(|k| s.get(q + k).is_some_and(|c| c.is_ascii_hexdigit())) {
            Some(q + 4 - p)
        } else {
            None
        }
    }
}

/// Greedy `{unicode}+` — total length of one-or-more consecutive `{unicode}`.
fn match_unicode_plus(s: &[u8], p: usize) -> Option<usize> {
    let first = match_unicode(s, p)?;
    let mut total = first;
    while let Some(n) = match_unicode(s, p + total) {
        total += n;
    }
    Some(total)
}

/// flex `unicodefail` = `\\u({hexdigit}{0,3}|\{{hexdigit}{0,6})` — length if a
/// (malformed) `\u` escape head matches at `p`.
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

/// flex `hex_char` = `\\x{hexdigit}{2}`.
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

/// flex `hex_fail` = `\\x{hexdigit}{0,1}`.
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

impl<'a> JsonPathLexer<'a> {
    /// The escape rules shared by `<xnq,xq,xvq>`. Returns `Some(step)` if one of
    /// them matched at the current cursor (longest-match across the group),
    /// else `None` (the caller then tries its state-specific rules).
    ///
    /// Flex resolves ties by longest match then earliest rule. The fixed
    /// `\\b \\f ...` rules are length 2; `{unicode}+` and `{unicode}+\\` can be
    /// longer, so we evaluate the variable-length rules' lengths and pick the
    /// longest, with the listed order breaking ties.
    pub(crate) fn shared_escape(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<Step>> {
        let s = self.input;
        let p = self.pos;
        let c0 = match s.get(p) {
            Some(&c) => c,
            None => return Ok(None),
        };
        if c0 != b'\\' {
            return Ok(None);
        }

        // Candidate longest variable-length matches.
        // <...>{unicode}+\\  -- unicode run followed by a backslash (thrown back)
        let uni_plus = match_unicode_plus(s, p);
        let uni_plus_bs = uni_plus.and_then(|n| {
            if s.get(p + n) == Some(&b'\\') {
                Some(n + 1)
            } else {
                None
            }
        });
        // <...>{unicode}*{unicodefail}
        let unifail = {
            // {unicode}* then {unicodefail}
            let mut q = p;
            while let Some(n) = match_unicode(s, q) {
                q += n;
            }
            match_unicodefail(s, q).map(|n| (q - p) + n)
        };
        let hexc = match_hex_char(s, p);
        let hexf = match_hex_fail(s, p);

        // Two-char fixed escapes: \b \f \n \r \t \v.
        let fixed2 = matches!(s.get(p + 1), Some(&b'b' | &b'f' | &b'n' | &b'r' | &b't' | &b'v'));

        // Determine longest match length among all candidates. The `\\.` and
        // `\\` single rules are fallbacks (lengths 2 and 1).
        let mut best: usize = 0;
        let mut which = Which::None;

        // Order mirrors the flex rule order for tie-breaking.
        consider(&mut best, &mut which, fixed2.then_some(2), Which::Fixed2);
        consider(&mut best, &mut which, uni_plus, Which::UnicodePlus);
        consider(&mut best, &mut which, hexc, Which::HexChar);
        consider(&mut best, &mut which, unifail, Which::UnicodeFail);
        consider(&mut best, &mut which, hexf, Which::HexFail);
        consider(&mut best, &mut which, uni_plus_bs, Which::UnicodePlusBackslash);
        // `\\.` (any char after backslash): length 2 if there is a next char.
        let dot = if s.get(p + 1).is_some() { Some(2) } else { None };
        consider(&mut best, &mut which, dot, Which::Dot);
        // lone `\\`: length 1.
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
                self.scanstring.addchar(false, ch);
                self.pos += 2;
                Ok(Some(Step::Continue))
            }
            Which::UnicodePlus => {
                let n = best;
                let text = s[p..p + n].to_vec();
                self.pos += n;
                if !self.parse_unicode(&text, n, escontext)? {
                    return Ok(Some(Step::Terminate));
                }
                Ok(Some(Step::Continue))
            }
            Which::HexChar => {
                let text = s[p..p + 4].to_vec();
                self.pos += 4;
                if !self.parse_hex_char(&text, escontext)? {
                    return Ok(Some(Step::Terminate));
                }
                Ok(Some(Step::Continue))
            }
            Which::UnicodeFail => {
                // C: the flex rule matched `yytext` = s[p..p+best]; jsonpath_yyerror
                // (NULL location) formats "at or near \"%s\"" from that matched
                // lexeme, not the input remaining after it. Pass the matched span
                // explicitly (yyerror's pos-based yytext would point past it).
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
                // C: yyless(yyleng - 1) — throw back the `\\`, treat as unicode.
                let n = best - 1; // drop the trailing backslash
                let text = s[p..p + n].to_vec();
                self.pos += n;
                if !self.parse_unicode(&text, n, escontext)? {
                    return Ok(Some(Step::Terminate));
                }
                Ok(Some(Step::Continue))
            }
            Which::Dot => {
                // `\\.` — add the escaped char literally (yytext[1]).
                self.scanstring.addchar(false, s[p + 1]);
                self.pos += 2;
                Ok(Some(Step::Continue))
            }
            Which::Backslash => {
                // lone `\\` at end.
                self.pos += 1;
                self.yyerror(escontext, "unexpected end after backslash")?;
                Ok(Some(Step::Terminate))
            }
            Which::None => Ok(None),
        }
    }

    /// `<xnq>` non-quoted string state.
    pub(crate) fn scan_xnq(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Step> {
        // Shared escapes first (they begin with '\\', longest-match wins).
        if let Some(step) = self.shared_escape(escontext)? {
            return Ok(step);
        }

        let s = self.input;
        let p = self.pos;

        // <xnq><<EOF>>
        if p >= s.len() {
            let value = Some(self.scanstring.clone());
            self.state = State::Initial;
            let tok = check_keyword(&self.scanstring);
            return Ok(Step::Emit(Lexeme { token: tok, value, start: 0, end: 0 }));
        }

        // <xnq>{other}+
        if is_other(s[p]) {
            let mut q = p;
            while q < s.len() && is_other(s[q]) {
                q += 1;
            }
            self.scanstring.addstring(false, &s[p..q]);
            self.pos = q;
            return Ok(Step::Continue);
        }

        // <xnq>{blank}+
        if crate::is_blank(s[p]) {
            let mut q = p;
            while q < s.len() && crate::is_blank(s[q]) {
                q += 1;
            }
            self.pos = q;
            let value = Some(self.scanstring.clone());
            self.state = State::Initial;
            let tok = check_keyword(&self.scanstring);
            return Ok(Step::Emit(Lexeme { token: tok, value, start: 0, end: 0 }));
        }

        // <xnq>\/\* (comment start)
        if s[p] == b'/' && s.get(p + 1) == Some(&b'*') {
            // C: yylval->str = scanstring; BEGIN xc; (no token returned)
            self.pos += 2;
            self.state = State::Xc;
            return Ok(Step::Continue);
        }

        // <xnq>({special}|\")
        if crate::is_special(s[p]) || s[p] == b'"' {
            // C: yylval->str = scanstring; yyless(0); BEGIN INITIAL;
            //    return checkKeyword();  (do NOT consume — yyless(0))
            let value = Some(self.scanstring.clone());
            self.state = State::Initial;
            let tok = check_keyword(&self.scanstring);
            return Ok(Step::Emit(Lexeme { token: tok, value, start: 0, end: 0 }));
        }

        // Unreachable for well-formed 8-bit input; treat as other-byte append.
        self.scanstring.addstring(false, &s[p..p + 1]);
        self.pos += 1;
        Ok(Step::Continue)
    }

    /// `<xq>` quoted-string state.
    pub(crate) fn scan_xq(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Step> {
        if let Some(step) = self.shared_escape(escontext)? {
            return Ok(step);
        }

        let s = self.input;
        let p = self.pos;

        // <xq,xvq><<EOF>>
        if p >= s.len() {
            self.yyerror(escontext, "unterminated quoted string")?;
            return Ok(Step::Terminate);
        }

        // <xq>\"
        if s[p] == b'"' {
            self.pos += 1;
            let value = Some(self.scanstring.clone());
            self.state = State::Initial;
            return Ok(Step::Emit(Lexeme { token: Token::STRING_P, value, start: 0, end: 0 }));
        }

        // <xq,xvq>[^\\\"]+
        if s[p] != b'\\' && s[p] != b'"' {
            let mut q = p;
            while q < s.len() && s[q] != b'\\' && s[q] != b'"' {
                q += 1;
            }
            self.scanstring.addstring(false, &s[p..q]);
            self.pos = q;
            return Ok(Step::Continue);
        }

        // A backslash not consumed by shared_escape would only happen on a
        // partial escape already handled there; defensively append.
        self.scanstring.addstring(false, &s[p..p + 1]);
        self.pos += 1;
        Ok(Step::Continue)
    }

    /// `<xvq>` quoted-variable-name state.
    pub(crate) fn scan_xvq(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Step> {
        if let Some(step) = self.shared_escape(escontext)? {
            return Ok(step);
        }

        let s = self.input;
        let p = self.pos;

        // <xq,xvq><<EOF>>
        if p >= s.len() {
            self.yyerror(escontext, "unterminated quoted string")?;
            return Ok(Step::Terminate);
        }

        // <xvq>\"
        if s[p] == b'"' {
            self.pos += 1;
            let value = Some(self.scanstring.clone());
            self.state = State::Initial;
            return Ok(Step::Emit(Lexeme { token: Token::VARIABLE_P, value, start: 0, end: 0 }));
        }

        // <xq,xvq>[^\\\"]+
        if s[p] != b'\\' && s[p] != b'"' {
            let mut q = p;
            while q < s.len() && s[q] != b'\\' && s[q] != b'"' {
                q += 1;
            }
            self.scanstring.addstring(false, &s[p..q]);
            self.pos = q;
            return Ok(Step::Continue);
        }

        self.scanstring.addstring(false, &s[p..p + 1]);
        self.pos += 1;
        Ok(Step::Continue)
    }

    /// `<xc>` C-style comment state.
    pub(crate) fn scan_xc(
        &mut self,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Step> {
        let s = self.input;
        let p = self.pos;

        // <xc><<EOF>>
        if p >= s.len() {
            self.yyerror(escontext, "unexpected end of comment")?;
            return Ok(Step::Terminate);
        }

        // <xc>\*\/
        if s[p] == b'*' && s.get(p + 1) == Some(&b'/') {
            self.pos += 2;
            self.state = State::Initial;
            return Ok(Step::Continue);
        }

        // <xc>[^\*]+
        if s[p] != b'*' {
            let mut q = p;
            while q < s.len() && s[q] != b'*' {
                q += 1;
            }
            self.pos = q;
            return Ok(Step::Continue);
        }

        // <xc>\*
        self.pos += 1;
        Ok(Step::Continue)
    }
}

/// Which shared-escape rule won the longest-match contest.
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

/// Update `(best, which)` if `cand` is strictly longer (earlier rules, tried
/// first, win ties because we only replace on strictly greater length).
fn consider(best: &mut usize, which: &mut Which, cand: Option<usize>, w: Which) {
    if let Some(n) = cand {
        if n > *best {
            *best = n;
            *which = w;
        }
    }
}
