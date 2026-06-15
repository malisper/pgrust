// The exclusive start conditions (`<xc>`, `<xb>`/`<xh>`, `<xq>`/`<xe>`/`<xus>`,
// `<xqs>`, `<xdolq>`, `<xd>`/`<xui>`, `<xeu>`).
//
// Each handler returns `Ok(Some(token))` to emit a token, `Ok(None)` to
// continue scanning (accumulating into the literal buffer / switching state),
// or `Err` for a lexer error.

impl<'a> Scanner<'a> {
    // -----------------------------------------------------------------------
    // <xc> extended C-style comments (scan.l:456-485).
    // -----------------------------------------------------------------------
    fn lex_xc(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;
        // <<EOF>>
        if self.eof_at(p) {
            return Err(self.lexerr("unterminated /* comment"));
        }
        // {xcstart}: nested comment open.
        if let Some(end) = self.match_xcstart(p) {
            self.pos = end;
            self.xcdepth += 1;
            self.yyless(2);
            return Ok(None);
        }
        // {xcstop} = \*+\/
        if self.at(p) == b'*' {
            // match \*+ then optional '/'
            let mut q = p;
            while self.at(q) == b'*' {
                q += 1;
            }
            if self.at(q) == b'/' {
                // {xcstop}
                self.pos = q + 1;
                if self.xcdepth <= 0 {
                    self.state = State::INITIAL;
                } else {
                    self.xcdepth -= 1;
                }
                return Ok(None);
            }
            // \*+ (without trailing slash): the "\*+" ignore rule.
            self.pos = q;
            return Ok(None);
        }
        // {xcinside} = [^*/]+
        if self.at(p) != b'/' {
            let mut q = p;
            while !self.eof_at(q) && self.at(q) != b'*' && self.at(q) != b'/' {
                q += 1;
            }
            if q > p {
                self.pos = q;
                return Ok(None);
            }
        }
        // {op_chars} ignore rule: a single op-char (covers a lone '/').
        if is_op_char(self.at(p)) {
            self.pos = p + 1;
            return Ok(None);
        }
        // Any other single char (e.g. a lone '/') -- flex's default would be
        // {xcinside}'s complement; consume one byte to make progress.
        self.pos = p + 1;
        Ok(None)
    }

    // -----------------------------------------------------------------------
    // <xb>/<xh> bit and hex string bodies + shared end-quote (scan.l:499-503,
    // 574-585 for the {quote} rule, and the xqs handler below).
    // -----------------------------------------------------------------------
    fn lex_xb_xh(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;
        // <xb><<EOF>> / <xh><<EOF>>
        if self.eof_at(p) {
            return Err(match self.state {
                State::Xb => self.lexerr("unterminated bit string literal"),
                _ => self.lexerr("unterminated hexadecimal string literal"),
            });
        }
        // <xb,xh,xq,xe,xus>{quote}: end quote -> look ahead via xqs.
        if self.at(p) == b'\'' {
            self.pos = p + 1;
            self.state_before_str_stop = self.state;
            self.state = State::Xqs;
            return Ok(None);
        }
        // {xhinside}/{xbinside} = [^']*  (accumulate; the [^']+ run).
        let mut q = p;
        while !self.eof_at(q) && self.at(q) != b'\'' {
            q += 1;
        }
        // [^']* can match empty; but we only reach here with at(p) != '\'',
        // so q > p.
        let text = self.scanbuf[p..q].to_vec();
        self.pos = q;
        self.addlit(&text);
        Ok(None)
    }

    // -----------------------------------------------------------------------
    // <xq>/<xe>/<xus> quoted string bodies (scan.l:633-742).
    // -----------------------------------------------------------------------
    fn lex_xq_xe_xus(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;
        // <xq,xe,xus><<EOF>>
        if self.eof_at(p) {
            return Err(self.lexerr("unterminated quoted string"));
        }
        let st = self.state;

        // <xb,xh,xq,xe,xus>{quote}: end quote -> xqs lookahead. (For xq/xe/xus.)
        // But {xqdouble} = '' is longer, so check it first per longest-match.
        if self.at(p) == b'\'' {
            if self.at(p + 1) == b'\'' {
                // <xq,xe,xus>{xqdouble}: embedded ''
                self.pos = p + 2;
                self.addlitchar(b'\'');
                return Ok(None);
            }
            // single quote: end of string body
            self.pos = p + 1;
            self.state_before_str_stop = st;
            self.state = State::Xqs;
            return Ok(None);
        }

        match st {
            State::Xq | State::Xus => {
                // {xqinside} = [^']+
                let mut q = p;
                while !self.eof_at(q) && self.at(q) != b'\'' {
                    q += 1;
                }
                let text = self.scanbuf[p..q].to_vec();
                self.pos = q;
                self.addlit(&text);
                Ok(None)
            }
            State::Xe => self.lex_xe_body(),
            _ => unreachable!("lex_xq_xe_xus called in non-string state"),
        }
    }

    /// The `<xe>`-specific rules (escapes, unicode, octal/hex), scan.l:639-741.
    fn lex_xe_body(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;

        // <xe>{xeunicode} = \\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})
        if let Some(end) = self.match_xeunicode(p) {
            let hexstart = p + 2;
            let c = parse_hex(&self.scanbuf[hexstart..end]);
            self.pos = end;
            self.check_escape_warning();
            // PUSH_YYLLOC / SET_YYLLOC: error cursor at this esc seq, restored
            // after. yylloc reporting for warnings already done above.
            let saved = self.save_push_yylloc();
            self.set_esc_yylloc();
            let res = if is_utf16_surrogate_first(c) {
                self.utf16_first_part = c;
                self.state = State::Xeu;
                Ok(())
            } else if is_utf16_surrogate_second(c) {
                Err(self.lexerr("invalid Unicode surrogate pair"))
            } else {
                self.addunicode(c)
            };
            self.pop_yylloc(saved);
            return res.map(|_| None);
        }

        // <xe,xeu>{xeunicodefail} = \\(u[0-9A-Fa-f]{0,3}|U[0-9A-Fa-f]{0,7})
        if let Some(end) = self.match_xeunicodefail(p) {
            self.pos = end;
            self.set_esc_yylloc();
            return Err(self.lexerr_full(
                ERRCODE_INVALID_ESCAPE_SEQUENCE,
                "invalid Unicode escape",
                None,
                Some("Unicode escapes must be \\uXXXX or \\UXXXXXXXX."),
            ));
        }

        // <xe>{xeescape} = \\[^0-7]
        if self.at(p) == b'\\' && !self.eof_at(p + 1) && !is_oct_digit(self.at(p + 1)) {
            // (xeunicode/xeunicodefail already excluded the \u/\U/\x cases that
            // are longer; xehexesc handled below is longer for \xHH.)
            // xehexesc = \\x[0-9A-Fa-f]{1,2} is longer, so check it first.
            if self.at(p + 1) == b'x' && is_hex_digit(self.at(p + 2)) {
                // fall through to xehexesc below
            } else {
                let escaped = self.at(p + 1);
                self.pos = p + 2;
                if escaped == b'\'' && self.backslash_quote_forbidden() {
                    return Err(self.lexerr_full(
                        ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
                        "unsafe use of \\' in a string literal",
                        None,
                        Some(
                            "Use '' to write quotes in strings. \\' is insecure in \
                             client-only encodings.",
                        ),
                    ));
                }
                self.check_string_escape_warning(escaped);
                let c = self.unescape_single_char(escaped);
                self.addlitchar(c);
                return Ok(None);
            }
        }

        // <xe>{xehexesc} = \\x[0-9A-Fa-f]{1,2}
        if self.at(p) == b'\\' && self.at(p + 1) == b'x' && is_hex_digit(self.at(p + 2)) {
            let mut end = p + 3;
            if is_hex_digit(self.at(end)) {
                end += 1;
            }
            let c = parse_hex(&self.scanbuf[p + 2..end]) as u8;
            self.pos = end;
            self.check_escape_warning();
            self.addlitchar(c);
            if c == b'\0' || is_highbit_set(c) {
                self.saw_non_ascii = true;
            }
            return Ok(None);
        }

        // <xe>{xeoctesc} = \\[0-7]{1,3}
        if self.at(p) == b'\\' && is_oct_digit(self.at(p + 1)) {
            let mut end = p + 2;
            while end < p + 4 && is_oct_digit(self.at(end)) {
                end += 1;
            }
            let c = parse_oct(&self.scanbuf[p + 1..end]) as u8;
            self.pos = end;
            self.check_escape_warning();
            self.addlitchar(c);
            if c == b'\0' || is_highbit_set(c) {
                self.saw_non_ascii = true;
            }
            return Ok(None);
        }

        // <xe>{xeinside} = [^\\']+
        if self.at(p) != b'\\' && self.at(p) != b'\'' {
            let mut q = p;
            while !self.eof_at(q) && self.at(q) != b'\\' && self.at(q) != b'\'' {
                q += 1;
            }
            let text = self.scanbuf[p..q].to_vec();
            self.pos = q;
            self.addlit(&text);
            return Ok(None);
        }

        // <xe>. : only needed for a backslash just before EOF.
        let c = self.at(p);
        self.pos = p + 1;
        self.addlitchar(c);
        Ok(None)
    }

    // -----------------------------------------------------------------------
    // <xqs> quote-stop continuation lookahead (scan.l:586-631).
    // -----------------------------------------------------------------------
    fn lex_xqs(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;
        // <xqs>{quotecontinue}: whitespace-with-newline then a quote.
        if let Some(end) = self.match_quotecontinue(p) {
            self.pos = end;
            self.state = self.state_before_str_stop;
            return Ok(None);
        }
        // <xqs>{quotecontinuefail} | <xqs>{other} | <xqs><<EOF>>: no
        // continuation. Throw back everything after the end quote and finish.
        // We do NOT need to actually consume quotecontinuefail; yyless(0)
        // resets to tok_start.
        self.yyless(0);
        self.state = State::INITIAL;
        let prev = self.state_before_str_stop;
        match prev {
            State::Xb => Ok(Some(self.make_token(tokens::BCONST, CoreYYSTYPE::Str(self.litbufdup())))),
            State::Xh => Ok(Some(self.make_token(tokens::XCONST, CoreYYSTYPE::Str(self.litbufdup())))),
            State::Xq | State::Xe => {
                if self.saw_non_ascii {
                    let buf = self.litbufdup();
                    // C scan.l:619-622 lets pg_verifymbstr raise its own error
                    // (ERRCODE_CHARACTER_NOT_IN_REPERTOIRE); propagate it verbatim
                    // rather than rewriting it to a generic syntax error.
                    pg_verifymbstr(&buf, false).map_err(|e| self.lexerr_propagate(e))?;
                }
                Ok(Some(self.make_token(tokens::SCONST, CoreYYSTYPE::Str(self.litbufdup()))))
            }
            State::Xus => Ok(Some(self.make_token(tokens::USCONST, CoreYYSTYPE::Str(self.litbufdup())))),
            _ => Err(self.lexerr("unhandled previous state in xqs")),
        }
    }

    // -----------------------------------------------------------------------
    // <xdolq> dollar-quoted strings (scan.l:757-787).
    // -----------------------------------------------------------------------
    fn lex_xdolq(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;
        // <xdolq><<EOF>>
        if self.eof_at(p) {
            return Err(self.lexerr("unterminated dollar-quoted string"));
        }
        // <xdolq>{dolqdelim}
        if let Some(end) = self.match_dolqdelim(p) {
            let text = self.scanbuf[p..end].to_vec();
            self.pos = end;
            let matches = self.dolqstart.as_deref() == Some(text.as_slice());
            if matches {
                self.dolqstart = None;
                self.state = State::INITIAL;
                return Ok(Some(self.make_token(tokens::SCONST, CoreYYSTYPE::Str(self.litbufdup()))));
            } else {
                // transfer all but the final '$' and put back the final '$'.
                let n = text.len();
                self.addlit(&text[..n - 1]);
                self.yyless(n - 1);
                return Ok(None);
            }
        }
        // <xdolq>{dolqinside} = [^$]+
        if self.at(p) != b'$' {
            let mut q = p;
            while !self.eof_at(q) && self.at(q) != b'$' {
                q += 1;
            }
            let text = self.scanbuf[p..q].to_vec();
            self.pos = q;
            self.addlit(&text);
            return Ok(None);
        }
        // <xdolq>{dolqfailed} = \${dolq_start}{dolq_cont}*
        if let Some(end) = self.match_dolqfailed(p) {
            let text = self.scanbuf[p..end].to_vec();
            self.pos = end;
            self.addlit(&text);
            return Ok(None);
        }
        // <xdolq>. : a lone '$' inside the quoted text.
        let c = self.at(p);
        self.pos = p + 1;
        self.addlitchar(c);
        Ok(None)
    }

    // -----------------------------------------------------------------------
    // <xd>/<xui> delimited identifiers (scan.l:799-825).
    // -----------------------------------------------------------------------
    fn lex_xd_xui(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;
        // <xd,xui><<EOF>>
        if self.eof_at(p) {
            return Err(self.lexerr("unterminated quoted identifier"));
        }
        let st = self.state;
        // {xddouble} = "" (longer than the single closing quote)
        if self.at(p) == b'"' && self.at(p + 1) == b'"' {
            self.pos = p + 2;
            self.addlitchar(b'"');
            return Ok(None);
        }
        // <xd>{xdstop} = "  /  <xui>{dquote} = "
        if self.at(p) == b'"' {
            self.pos = p + 1;
            self.state = State::INITIAL;
            if self.literalbuf.is_empty() {
                return Err(self.lexerr("zero-length delimited identifier"));
            }
            match st {
                State::Xd => {
                    // truncate as appropriate, return IDENT
                    let ident = self.litbufdup();
                    let ident = self.truncate_xd_identifier(ident)?;
                    Ok(Some(self.make_token(tokens::IDENT, CoreYYSTYPE::Str(ident))))
                }
                State::Xui => {
                    // can't truncate till after de-escape; return UIDENT
                    Ok(Some(self.make_token(tokens::UIDENT, CoreYYSTYPE::Str(self.litbufdup()))))
                }
                _ => unreachable!(),
            }
        } else {
            // <xd,xui>{xdinside} = [^"]+
            let mut q = p;
            while !self.eof_at(q) && self.at(q) != b'"' {
                q += 1;
            }
            let text = self.scanbuf[p..q].to_vec();
            self.pos = q;
            self.addlit(&text);
            Ok(None)
        }
    }

    // -----------------------------------------------------------------------
    // <xeu> UTF-16 surrogate-pair completion (scan.l:670-696).
    // -----------------------------------------------------------------------
    fn lex_xeu(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;
        // <xeu>{xeunicode}
        if let Some(end) = self.match_xeunicode(p) {
            let hexstart = p + 2;
            let mut c = parse_hex(&self.scanbuf[hexstart..end]);
            self.pos = end;
            let saved = self.save_push_yylloc();
            self.set_esc_yylloc();
            let res = (|| {
                if !is_utf16_surrogate_second(c) {
                    return Err(self.lexerr("invalid Unicode surrogate pair"));
                }
                c = surrogate_pair_to_codepoint(self.utf16_first_part, c);
                self.addunicode(c)
            })();
            self.pop_yylloc(saved);
            self.state = State::Xe;
            return res.map(|_| None);
        }
        // <xe,xeu>{xeunicodefail}
        if let Some(end) = self.match_xeunicodefail(p) {
            self.pos = end;
            self.set_esc_yylloc();
            return Err(self.lexerr_full(
                ERRCODE_INVALID_ESCAPE_SEQUENCE,
                "invalid Unicode escape",
                None,
                Some("Unicode escapes must be \\uXXXX or \\UXXXXXXXX."),
            ));
        }
        // <xeu>. | <xeu>\n | <xeu><<EOF>>: missing second escape sequence.
        self.set_esc_yylloc();
        Err(self.lexerr("invalid Unicode surrogate pair"))
    }

    // -----------------------------------------------------------------------
    // Helpers used by the string states.
    // -----------------------------------------------------------------------

    /// `{xeunicode}` = `\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})`.
    fn match_xeunicode(&self, p: usize) -> Option<usize> {
        if self.at(p) != b'\\' {
            return None;
        }
        match self.at(p + 1) {
            b'u' => {
                let end = p + 2 + 4;
                (p + 2..end).all(|i| is_hex_digit(self.at(i))).then_some(end)
            }
            b'U' => {
                let end = p + 2 + 8;
                (p + 2..end).all(|i| is_hex_digit(self.at(i))).then_some(end)
            }
            _ => None,
        }
    }

    /// `{xeunicodefail}` = `\\(u[0-9A-Fa-f]{0,3}|U[0-9A-Fa-f]{0,7})` -- the
    /// failure rule (a `\u`/`\U` with too few hex digits).
    fn match_xeunicodefail(&self, p: usize) -> Option<usize> {
        if self.at(p) != b'\\' {
            return None;
        }
        let max = match self.at(p + 1) {
            b'u' => 3,
            b'U' => 7,
            _ => return None,
        };
        let mut q = p + 2;
        let mut n = 0;
        while n < max && is_hex_digit(self.at(q)) {
            q += 1;
            n += 1;
        }
        Some(q)
    }

    /// `{quotecontinue}` = `{whitespace_with_newline}{quote}` -- whitespace
    /// (comments allowed) containing at least one newline, then a `'`.
    fn match_quotecontinue(&self, p: usize) -> Option<usize> {
        let end_ws = self.match_whitespace_with_newline(p)?;
        if self.at(end_ws) == b'\'' {
            Some(end_ws + 1)
        } else {
            None
        }
    }

    /// `{whitespace_with_newline}` = `{non_newline_whitespace}*{newline}
    /// {special_whitespace}*`.
    fn match_whitespace_with_newline(&self, p: usize) -> Option<usize> {
        // non_newline_whitespace = (non_newline_space | comment)
        let mut q = p;
        loop {
            if is_non_newline_space(self.at(q)) {
                q += 1;
            } else if let Some(end) = self.match_comment(q) {
                // a comment (not necessarily ending in a newline) -- but a
                // {comment} consumes to end-of-line, so it cannot be followed
                // by the required {newline} unless EOF. We still consume it.
                if end == q {
                    break;
                }
                q = end;
            } else {
                break;
            }
        }
        // required {newline}
        if !is_newline(self.at(q)) {
            return None;
        }
        q += 1;
        // special_whitespace = ({space}+ | {comment}{newline})*
        loop {
            if let Some(end) = self.match_space_run(q) {
                q = end;
            } else if let Some(end) = self.match_comment(q) {
                if is_newline(self.at(end)) {
                    q = end + 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        Some(q)
    }

    /// Whether `\'` is forbidden by the current `backslash_quote` setting.
    fn backslash_quote_forbidden(&self) -> bool {
        self.backslash_quote == BACKSLASH_QUOTE_OFF
            || (self.backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING
                && PG_ENCODING_IS_CLIENT_ONLY(pg_get_client_encoding()))
    }

    /// `truncate_identifier(ident, literallen, true)` for `<xd>` identifiers.
    fn truncate_xd_identifier(&self, ident: Vec<u8>) -> Result<Vec<u8>, LexError> {
        let len = ident.len();
        if len >= pgrust_pg_ffi::NAMEDATALEN as usize {
            let mut buf = ident.clone();
            buf.push(0);
            backend_parser_scansup::truncate_identifier(&mut buf, len as core::ffi::c_int, true)
                .map_err(|_| self.lexerr("identifier truncation failed"))?;
            let nul = buf.iter().position(|&b| b == 0).unwrap_or(len);
            Ok(buf[..nul].to_vec())
        } else {
            Ok(ident)
        }
    }

    // PUSH_YYLLOC/POP_YYLLOC + SET_YYLLOC at an escape position.
    fn save_push_yylloc(&mut self) -> i32 {
        self.save_yylloc = self.yylloc;
        self.save_yylloc
    }
    fn pop_yylloc(&mut self, _saved: i32) {
        self.yylloc = self.save_yylloc;
    }
    /// SET_YYLLOC() at the current token start (used to point the error cursor
    /// at an escape sequence inside a string).
    fn set_esc_yylloc(&mut self) {
        self.yylloc = self.tok_start as i32;
    }
}

/// Parse an ASCII hex byte-string into a code point (`strtoul(.., 16)`).
fn parse_hex(bytes: &[u8]) -> PgWchar {
    let mut v: PgWchar = 0;
    for &b in bytes {
        v = v * 16 + (b as char).to_digit(16).unwrap_or(0);
    }
    v
}

/// Parse an ASCII octal byte-string (`strtoul(.., 8)`).
fn parse_oct(bytes: &[u8]) -> u32 {
    let mut v: u32 = 0;
    for &b in bytes {
        v = v * 8 + (b - b'0') as u32;
    }
    v
}
