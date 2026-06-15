// The core `core_yylex` state machine and its lexical matchers.
//
// Included into `lib.rs`.  Each function ports a specific group of scan.l
// rules; `core_yylex` dispatches on the active start condition and applies
// the rules in flex priority order (longest match, ties broken by rule
// order in the file).

// ---------------------------------------------------------------------------
// Character-class predicates (mirror the named flex definitions in scan.l).
// ---------------------------------------------------------------------------

/// `{space}` = `[ \t\n\r\f\v]`.
fn is_space(c: u8) -> bool {
    scanner_isspace(c)
}
/// `{non_newline_space}` = `[ \t\f\v]`.
fn is_non_newline_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | 0x0c | 0x0b)
}
/// `{newline}` = `[\n\r]`.
fn is_newline(c: u8) -> bool {
    matches!(c, b'\n' | b'\r')
}
/// `ident_start` = `[A-Za-z\200-\377_]`.
fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c >= 0x80
}
/// `ident_cont` = `[A-Za-z\200-\377_0-9\$]`.
fn is_ident_cont(c: u8) -> bool {
    is_ident_start(c) || c.is_ascii_digit() || c == b'$'
}
/// `dolq_start` = `[A-Za-z\200-\377_]`.
fn is_dolq_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c >= 0x80
}
/// `dolq_cont` = `[A-Za-z\200-\377_0-9]`.
fn is_dolq_cont(c: u8) -> bool {
    is_dolq_start(c) || c.is_ascii_digit()
}
/// `self` = `[,()\[\].;\:\+\-\*\/\%\^\<\>\=]`.
fn is_self_char(c: u8) -> bool {
    matches!(
        c,
        b',' | b'(' | b')' | b'[' | b']' | b'.' | b';' | b':' | b'+' | b'-' | b'*' | b'/' | b'%'
            | b'^' | b'<' | b'>' | b'='
    )
}
/// `op_chars` = `[\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]`.
fn is_op_char(c: u8) -> bool {
    matches!(
        c,
        b'~' | b'!' | b'@' | b'#' | b'^' | b'&' | b'|' | b'`' | b'?' | b'+' | b'-' | b'*' | b'/'
            | b'%' | b'<' | b'>' | b'='
    )
}
fn is_dec_digit(c: u8) -> bool {
    c.is_ascii_digit()
}
fn is_hex_digit(c: u8) -> bool {
    c.is_ascii_hexdigit()
}
fn is_oct_digit(c: u8) -> bool {
    (b'0'..=b'7').contains(&c)
}
fn is_bin_digit(c: u8) -> bool {
    c == b'0' || c == b'1'
}

impl<'a> Scanner<'a> {
    // -----------------------------------------------------------------------
    // Small buffer helpers operating relative to a position.
    // -----------------------------------------------------------------------

    /// The byte at absolute offset `p`, or NUL past the end (flex sentinel).
    #[inline]
    fn at(&self, p: usize) -> u8 {
        self.scanbuf.get(p).copied().unwrap_or(YY_END_OF_BUFFER_CHAR)
    }

    /// True if offset `p` is at end of input.
    #[inline]
    fn eof_at(&self, p: usize) -> bool {
        p >= self.scanbuf.len()
    }

    /// The text of the current token: `scanbuf[tok_start..pos]`.
    #[inline]
    fn yytext(&self) -> &'a [u8] {
        &self.scanbuf[self.tok_start..self.pos]
    }

    /// `yyleng` -- length of the current token.
    #[inline]
    fn yyleng(&self) -> usize {
        self.pos - self.tok_start
    }

    /// `yyless(n)` -- push back all but the first `n` bytes of the current
    /// token, so the next match resumes at `tok_start + n`.
    #[inline]
    fn yyless(&mut self, n: usize) {
        self.pos = self.tok_start + n;
    }

    // -----------------------------------------------------------------------
    // Greedy matchers returning the end offset of a match starting at `p`.
    // Each returns Some(end) when the named pattern matches a non-empty (or,
    // where flex allows empty, possibly empty) prefix; None otherwise.
    // -----------------------------------------------------------------------

    /// Maximal run of `{space}+`.
    fn match_space_run(&self, p: usize) -> Option<usize> {
        let mut q = p;
        while !self.eof_at(q) && is_space(self.at(q)) {
            q += 1;
        }
        (q > p).then_some(q)
    }

    /// `{comment}` = `--` then `{non_newline}*`.
    fn match_comment(&self, p: usize) -> Option<usize> {
        if self.at(p) == b'-' && self.at(p + 1) == b'-' {
            let mut q = p + 2;
            while !self.eof_at(q) && !is_newline(self.at(q)) {
                q += 1;
            }
            Some(q)
        } else {
            None
        }
    }

    /// `{whitespace}` = `{space}+ | {comment}` (longest of the alternatives,
    /// applied repeatedly because flex's `+`/alternation collapse a run).
    ///
    /// Returns the end offset of the maximal whitespace run, or None.
    fn match_whitespace(&self, p: usize) -> Option<usize> {
        // {whitespace} as a single token is ({space}+|{comment}); but because
        // the rule has no trailing *, flex matches exactly one alternative per
        // token. The `{whitespace}` *rule* (scan.l:443) is `{whitespace}` with
        // no star, i.e. ({space}+|{comment}). So one match is either a maximal
        // run of spaces OR a single comment. We return the longer.
        let sp = self.match_space_run(p);
        let cm = self.match_comment(p);
        match (sp, cm) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// `{identifier}` = `{ident_start}{ident_cont}*`.
    fn match_identifier(&self, p: usize) -> Option<usize> {
        if self.eof_at(p) || !is_ident_start(self.at(p)) {
            return None;
        }
        let mut q = p + 1;
        while !self.eof_at(q) && is_ident_cont(self.at(q)) {
            q += 1;
        }
        Some(q)
    }

    /// `{operator}` = `{op_chars}+`.
    fn match_operator(&self, p: usize) -> Option<usize> {
        let mut q = p;
        while !self.eof_at(q) && is_op_char(self.at(q)) {
            q += 1;
        }
        (q > p).then_some(q)
    }

    /// `{xcstart}` = `\/\*{op_chars}*`.
    fn match_xcstart(&self, p: usize) -> Option<usize> {
        if self.at(p) == b'/' && self.at(p + 1) == b'*' {
            let mut q = p + 2;
            while !self.eof_at(q) && is_op_char(self.at(q)) {
                q += 1;
            }
            Some(q)
        } else {
            None
        }
    }

    /// `{decinteger}` = `{decdigit}(_?{decdigit})*`.
    fn match_decinteger(&self, p: usize) -> Option<usize> {
        if self.eof_at(p) || !is_dec_digit(self.at(p)) {
            return None;
        }
        let mut q = p + 1;
        loop {
            // optional single underscore then a required digit
            let after_us = if self.at(q) == b'_' { q + 1 } else { q };
            if !self.eof_at(after_us) && is_dec_digit(self.at(after_us)) {
                q = after_us + 1;
            } else {
                break;
            }
        }
        Some(q)
    }

    /// `0[xX](_?{hexdigit})+`, etc. -- a radix integer with prefix and an
    /// underscore-separated digit run.
    fn match_radix_integer(
        &self,
        p: usize,
        markers: &[u8],
        digit: fn(u8) -> bool,
    ) -> Option<usize> {
        if self.at(p) != b'0' {
            return None;
        }
        if !markers.contains(&self.at(p + 1)) {
            return None;
        }
        // (_?{digit})+ : at least one digit group required
        let mut q = p + 2;
        let mut groups = 0;
        loop {
            let after_us = if self.at(q) == b'_' { q + 1 } else { q };
            if !self.eof_at(after_us) && digit(self.at(after_us)) {
                q = after_us + 1;
                groups += 1;
            } else {
                break;
            }
        }
        (groups > 0).then_some(q)
    }

    /// `{hexfail}`/`{octfail}`/`{binfail}` = `0[mM]_?` (prefix, optional one
    /// underscore, no following digit group -- the failure rule).
    fn match_radix_fail(&self, p: usize, markers: &[u8]) -> Option<usize> {
        if self.at(p) != b'0' || !markers.contains(&self.at(p + 1)) {
            return None;
        }
        let mut q = p + 2;
        if self.at(q) == b'_' {
            q += 1;
        }
        Some(q)
    }

    /// `{numeric}` = `({decinteger}\.{decinteger}?)|(\.{decinteger})`.
    fn match_numeric(&self, p: usize) -> Option<usize> {
        // alt 1: {decinteger} . {decinteger}?
        if let Some(after_int) = self.match_decinteger(p) {
            if self.at(after_int) == b'.' {
                let after_dot = after_int + 1;
                if let Some(after_frac) = self.match_decinteger(after_dot) {
                    return Some(after_frac);
                }
                return Some(after_dot);
            }
        }
        // alt 2: . {decinteger}
        if self.at(p) == b'.' {
            if let Some(after_frac) = self.match_decinteger(p + 1) {
                return Some(after_frac);
            }
        }
        None
    }

    /// `{numericfail}` = `{decinteger}\.\.`.
    fn match_numericfail(&self, p: usize) -> Option<usize> {
        let after_int = self.match_decinteger(p)?;
        if self.at(after_int) == b'.' && self.at(after_int + 1) == b'.' {
            Some(after_int + 2)
        } else {
            None
        }
    }

    /// `({decinteger}|{numeric})` -- the mantissa shared by `real`/`realfail`.
    fn match_mantissa(&self, p: usize) -> Option<usize> {
        // {numeric} is longer when there's a dot; otherwise {decinteger}.
        let num = self.match_numeric(p);
        let dec = self.match_decinteger(p);
        match (num, dec) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// `{real}` = `({decinteger}|{numeric})[Ee][-+]?{decinteger}`.
    fn match_real(&self, p: usize) -> Option<usize> {
        let after_mant = self.match_mantissa(p)?;
        if !matches!(self.at(after_mant), b'E' | b'e') {
            return None;
        }
        let mut q = after_mant + 1;
        if matches!(self.at(q), b'+' | b'-') {
            q += 1;
        }
        let after_exp = self.match_decinteger(q)?;
        Some(after_exp)
    }

    /// `{realfail}` = `({decinteger}|{numeric})[Ee][-+]`.
    fn match_realfail(&self, p: usize) -> Option<usize> {
        let after_mant = self.match_mantissa(p)?;
        if !matches!(self.at(after_mant), b'E' | b'e') {
            return None;
        }
        let q = after_mant + 1;
        if matches!(self.at(q), b'+' | b'-') {
            Some(q + 1)
        } else {
            None
        }
    }

    /// `{param}` = `\${decdigit}+`.
    fn match_param(&self, p: usize) -> Option<usize> {
        if self.at(p) != b'$' || !is_dec_digit(self.at(p + 1)) {
            return None;
        }
        let mut q = p + 2;
        while !self.eof_at(q) && is_dec_digit(self.at(q)) {
            q += 1;
        }
        Some(q)
    }

    /// `{dolqdelim}` = `\$({dolq_start}{dolq_cont}*)?\$`.
    fn match_dolqdelim(&self, p: usize) -> Option<usize> {
        if self.at(p) != b'$' {
            return None;
        }
        let mut q = p + 1;
        if !self.eof_at(q) && is_dolq_start(self.at(q)) {
            q += 1;
            while !self.eof_at(q) && is_dolq_cont(self.at(q)) {
                q += 1;
            }
        }
        if self.at(q) == b'$' {
            Some(q + 1)
        } else {
            None
        }
    }

    /// `{dolqfailed}` = `\${dolq_start}{dolq_cont}*`.
    fn match_dolqfailed(&self, p: usize) -> Option<usize> {
        if self.at(p) != b'$' || self.eof_at(p + 1) || !is_dolq_start(self.at(p + 1)) {
            return None;
        }
        let mut q = p + 2;
        while !self.eof_at(q) && is_dolq_cont(self.at(q)) {
            q += 1;
        }
        Some(q)
    }

    // =======================================================================
    // The main lexer entry point.
    // =======================================================================

    /// `core_yylex()` -- return the next token.  Mirrors flex's outer loop:
    /// position `tok_start` at the current cursor, find the matching rule in
    /// the active start condition, run its action.  Some actions consume input
    /// silently and loop (whitespace, comments, in-string accumulation); those
    /// are handled here by re-iterating rather than returning.
    pub fn core_yylex(&mut self) -> LexResult {
        loop {
            self.tok_start = self.pos;
            match self.state {
                State::INITIAL => {
                    if let Some(tok) = self.lex_initial()? {
                        return Ok(tok);
                    }
                }
                State::Xc => {
                    if let Some(tok) = self.lex_xc()? {
                        return Ok(tok);
                    }
                }
                State::Xb | State::Xh => {
                    if let Some(tok) = self.lex_xb_xh()? {
                        return Ok(tok);
                    }
                }
                State::Xq | State::Xe | State::Xus => {
                    if let Some(tok) = self.lex_xq_xe_xus()? {
                        return Ok(tok);
                    }
                }
                State::Xqs => {
                    if let Some(tok) = self.lex_xqs()? {
                        return Ok(tok);
                    }
                }
                State::Xdolq => {
                    if let Some(tok) = self.lex_xdolq()? {
                        return Ok(tok);
                    }
                }
                State::Xd | State::Xui => {
                    if let Some(tok) = self.lex_xd_xui()? {
                        return Ok(tok);
                    }
                }
                State::Xeu => {
                    if let Some(tok) = self.lex_xeu()? {
                        return Ok(tok);
                    }
                }
            }
        }
    }
}
