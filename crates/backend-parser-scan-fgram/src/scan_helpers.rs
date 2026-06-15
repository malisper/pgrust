// Token constructors, the `{operator}` rule, and the scan.l subroutines
// (`process_integer_literal`, `unescape_single_char`, the escape-warning
// helpers, `addunicode`, `litbufdup`/identifier handling).

impl<'a> Scanner<'a> {
    // -----------------------------------------------------------------------
    // Token constructors.
    // -----------------------------------------------------------------------

    /// Build a token with the current `yylloc` and the given value.
    fn make_token(&self, token: i32, value: CoreYYSTYPE) -> Token {
        Token {
            token,
            value,
            location: self.current_location(),
        }
    }

    /// A token with no semantic value (TYPECAST, DOT_DOT, ...).
    fn simple_token(&self, token: i32) -> Token {
        self.make_token(token, CoreYYSTYPE::None)
    }

    /// `return yytext[0];` -- a single-character token (its ASCII value is the
    /// token code).
    fn char_token(&self, ch: u8) -> Token {
        self.make_token(ch as i32, CoreYYSTYPE::None)
    }

    /// `yyterminate()` -- the end-of-input token (token code 0).
    fn eof_token(&self) -> Token {
        self.make_token(YY_NULL, CoreYYSTYPE::None)
    }

    /// Emit a keyword token: set `yylval->keyword` to the canonical spelling
    /// and return `keyword_tokens[kwnum]`, i.e. the bison token for that
    /// keyword (from the generated `SCAN_KEYWORD_TOKENS` table).
    fn keyword_token(&self, kwnum: usize, kw: &'static str) -> Token {
        let token = SCAN_KEYWORD_TOKENS[kwnum] as i32;
        self.make_token(token, CoreYYSTYPE::Keyword(kw))
    }

    /// `downcase_truncate_identifier(yytext, yyleng, true)` returning the
    /// downcased/truncated identifier bytes (no trailing NUL).
    fn downcase_truncate(&self, ident: &[u8]) -> Result<Vec<u8>, LexError> {
        // The scansup routine downcases/truncates against the database
        // encoding; it operates within a memory context and returns a palloc'd
        // result. We bridge through a transient owned context, then copy out
        // the bytes (the C caller stores the palloc'd pointer in yylval->str).
        use backend_utils_mmgr::OwnedMemoryContext;
        let ctx = OwnedMemoryContext::alloc_set(None, "scan-ident", 1024, 8192, 8192)
            .map_err(|_| self.lexerr("out of memory"))?;
        let scope = ctx.scope();
        let id = backend_parser_scansup::downcase_truncate_identifier(
            &scope,
            ident,
            ident.len() as core::ffi::c_int,
            true,
        )
        .map_err(|_| self.lexerr("identifier downcasing failed"))?;
        Ok(id.as_bytes().to_vec())
    }

    // -----------------------------------------------------------------------
    // The {operator} rule (scan.l:886).
    // -----------------------------------------------------------------------

    /// Port of the `{operator}` action.  The cursor has already consumed the
    /// maximal `{op_chars}+` run (`yytext`); apply the comment/`+`/`-`
    /// trimming, the `self`/two-char re-classification, and the length check.
    fn lex_operator(&mut self) -> Result<Option<Token>, LexError> {
        let yytext = self.yytext().to_vec();
        let mut nchars = yytext.len();

        // Check for embedded slash-star or dash-dash (comment starts); if both
        // appear, the operator must stop at the first one.
        let slashstar = find_sub(&yytext, b"/*");
        let dashdash = find_sub(&yytext, b"--");
        let cut = match (slashstar, dashdash) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(c) = cut {
            nchars = c;
        }

        // '+'/'-' cannot be the last char of a multi-char operator unless the
        // operator contains a char outside the SQL-operator set.
        if nchars > 1
            && (yytext[nchars - 1] == b'+' || yytext[nchars - 1] == b'-')
        {
            let mut ic = nchars as isize - 2;
            while ic >= 0 {
                let c = yytext[ic as usize];
                if matches!(
                    c,
                    b'~' | b'!' | b'@' | b'#' | b'^' | b'&' | b'|' | b'`' | b'?' | b'%'
                ) {
                    break;
                }
                ic -= 1;
            }
            if ic < 0 {
                // no qualifying char: strip all trailing [+-]
                loop {
                    nchars -= 1;
                    if !(nchars > 1
                        && (yytext[nchars - 1] == b'+' || yytext[nchars - 1] == b'-'))
                    {
                        break;
                    }
                }
            }
        }

        self.set_yylloc();

        if nchars < self.yyleng() {
            // Strip the unwanted chars from the token.
            self.yyless(nchars);
            // If what's left is a single self-char, return it as a char token.
            if nchars == 1 && b",()[].;:+-*/%^<>=".contains(&yytext[0]) {
                return Ok(Some(self.char_token(yytext[0])));
            }
            // If what's left is a two-char operator token, return that token.
            if nchars == 2 {
                if yytext[0] == b'=' && yytext[1] == b'>' {
                    return Ok(Some(self.simple_token(tokens::EQUALS_GREATER)));
                }
                if yytext[0] == b'>' && yytext[1] == b'=' {
                    return Ok(Some(self.simple_token(tokens::GREATER_EQUALS)));
                }
                if yytext[0] == b'<' && yytext[1] == b'=' {
                    return Ok(Some(self.simple_token(tokens::LESS_EQUALS)));
                }
                if yytext[0] == b'<' && yytext[1] == b'>' {
                    return Ok(Some(self.simple_token(tokens::NOT_EQUALS)));
                }
                if yytext[0] == b'!' && yytext[1] == b'=' {
                    return Ok(Some(self.simple_token(tokens::NOT_EQUALS)));
                }
            }
        }

        // Complain if the operator is too long.
        if nchars >= pgrust_pg_ffi::NAMEDATALEN as usize {
            return Err(self.lexerr("operator too long"));
        }

        let op = yytext[..nchars].to_vec();
        Ok(Some(self.make_token(tokens::Op, CoreYYSTYPE::Str(op))))
    }

    // -----------------------------------------------------------------------
    // scan.l subroutines.
    // -----------------------------------------------------------------------

    /// `process_integer_literal()` (scan.l:1361).  Parses `token` as an int32
    /// via `pg_strtoint32_safe` (which itself understands the `0x`/`0o`/`0b`
    /// prefixes and `_` separators, so `base` is unused exactly as in C); on
    /// the soft "error_occurred" path it becomes an `FCONST` carrying the
    /// original text, else an `ICONST` with the value.
    fn process_integer_literal(&self, token: &[u8], _base: i32) -> Token {
        use backend_utils_error::SoftErrorContext;
        let s = std::str::from_utf8(token).unwrap_or("\u{FFFF}");
        let mut escontext = SoftErrorContext::new(false);
        let result = backend_utils_adt_numutils::pg_strtoint32_safe(s, Some(&mut escontext));
        match result {
            Ok(v) if !escontext.error_occurred() => {
                self.make_token(tokens::ICONST, CoreYYSTYPE::Ival(v))
            }
            // integer too large (or contains decimal pt) -> treat as float
            _ => self.make_token(tokens::FCONST, CoreYYSTYPE::Str(token.to_vec())),
        }
    }

    /// `unescape_single_char()` (scan.l:1397).
    fn unescape_single_char(&mut self, c: u8) -> u8 {
        match c {
            b'b' => 0x08, // '\b'
            b'f' => 0x0c, // '\f'
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => 0x0b, // '\v'
            _ => {
                if c == b'\0' || is_highbit_set(c) {
                    self.saw_non_ascii = true;
                }
                c
            }
        }
    }

    /// `check_string_escape_warning()` (scan.l:1423).
    fn check_string_escape_warning(&mut self, ychar: u8) {
        if ychar == b'\'' {
            if self.warn_on_first_escape && self.escape_string_warning {
                self.warnings.push(Warning {
                    sqlstate: ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
                    message: "nonstandard use of \\' in a string literal",
                    hint: "Use '' to write quotes in strings, or use the escape string syntax (E'...').",
                    location: self.current_location(),
                });
            }
            self.warn_on_first_escape = false;
        } else if ychar == b'\\' {
            if self.warn_on_first_escape && self.escape_string_warning {
                self.warnings.push(Warning {
                    sqlstate: ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
                    message: "nonstandard use of \\\\ in a string literal",
                    hint: "Use the escape string syntax for backslashes, e.g., E'\\\\'.",
                    location: self.current_location(),
                });
            }
            self.warn_on_first_escape = false;
        } else {
            self.check_escape_warning();
        }
    }

    /// `check_escape_warning()` (scan.l:1450).
    fn check_escape_warning(&mut self) {
        if self.warn_on_first_escape && self.escape_string_warning {
            self.warnings.push(Warning {
                sqlstate: ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
                message: "nonstandard use of escape in a string literal",
                hint: "Use the escape string syntax for escapes, e.g., E'\\r\\n'.",
                location: self.current_location(),
            });
        }
        self.warn_on_first_escape = false;
    }

    /// `addunicode()` (scan.l:1378).  Validate the code point, convert it to
    /// the server encoding via the seam, and append the bytes to the literal.
    fn addunicode(&mut self, c: PgWchar) -> Result<(), LexError> {
        if !is_valid_unicode_codepoint(c) {
            return Err(self.lexerr("invalid Unicode escape value"));
        }
        // C scan.l:1391-1393 expects pg_unicode_to_server() to ereport() its own
        // error (under a scanner errposition callback) for any unconvertible code
        // point; propagate that error verbatim rather than rewriting it to a
        // generic syntax error.
        let bytes = self
            .unicode_seam
            .pg_unicode_to_server(c)
            .map_err(|e| self.lexerr_propagate(e))?;
        self.addlit(&bytes);
        Ok(())
    }
}

/// Find the first occurrence of `needle` in `hay`, returning its start index.
fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}
