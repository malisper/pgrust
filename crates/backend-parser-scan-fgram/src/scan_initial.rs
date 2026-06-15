// The `INITIAL` start condition: all non-exclusive scan.l rules.
//
// Flex picks the longest match; among equal-length matches it picks the rule
// that appears first in the file.  `lex_initial` reproduces that exactly by
// collecting every candidate rule's match length and selecting the longest,
// with ties broken by scan.l rule order (encoded by [`InitialRule`]'s
// declaration order).

/// The `INITIAL`-state rules, in scan.l source order (so a `<`-ordered compare
/// breaks length ties exactly as flex's "first rule wins" does).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum InitialRule {
    Whitespace,
    Xcstart,
    Xbstart,
    Xhstart,
    Xnstart,
    Xqstart,
    Xestart,
    Xusstart,
    Dolqdelim,
    Dolqfailed,
    Xdstart,
    Xuistart,
    Xufailed,
    Typecast,
    DotDot,
    ColonEquals,
    EqualsGreater,
    LessEquals,
    GreaterEquals,
    LessGreater,
    NotEquals,
    SelfChar,
    Operator,
    Param,
    ParamJunk,
    Decinteger,
    Hexinteger,
    Octinteger,
    Bininteger,
    Hexfail,
    Octfail,
    Binfail,
    Numeric,
    Numericfail,
    Real,
    Realfail,
    IntegerJunk,
    NumericJunk,
    RealJunk,
    Identifier,
    Other,
}

impl<'a> Scanner<'a> {
    /// Fixed-string match: returns `Some(end)` if `s` occurs at `p`.
    fn match_lit(&self, p: usize, s: &[u8]) -> Option<usize> {
        for (i, &b) in s.iter().enumerate() {
            if self.at(p + i) != b {
                return None;
            }
        }
        Some(p + s.len())
    }

    /// `[bB]{quote}` etc. -- a letter prefix immediately followed by `'`.
    fn match_letter_quote(&self, p: usize, letters: &[u8]) -> Option<usize> {
        if letters.contains(&self.at(p)) && self.at(p + 1) == b'\'' {
            Some(p + 2)
        } else {
            None
        }
    }

    /// `{xuistart}`/`{xusstart}` = `[uU]&` then `"` or `'`.
    fn match_u_amp(&self, p: usize, closer: u8) -> Option<usize> {
        if matches!(self.at(p), b'u' | b'U') && self.at(p + 1) == b'&' && self.at(p + 2) == closer {
            Some(p + 3)
        } else {
            None
        }
    }

    /// `{xufailed}` = `[uU]&` (not followed by `"`/`'`, else a longer rule wins).
    fn match_xufailed(&self, p: usize) -> Option<usize> {
        if matches!(self.at(p), b'u' | b'U') && self.at(p + 1) == b'&' {
            Some(p + 2)
        } else {
            None
        }
    }

    /// `{integer_junk}` = `{decinteger}{identifier}` (and serves for hex/oct/bin
    /// junk too, since `{decinteger}{identifier}` matches the same strings).
    fn match_integer_junk(&self, p: usize) -> Option<usize> {
        let after = self.match_decinteger(p)?;
        self.match_identifier(after)
    }

    /// `{numeric_junk}` = `{numeric}{identifier}`.
    fn match_numeric_junk(&self, p: usize) -> Option<usize> {
        let after = self.match_numeric(p)?;
        self.match_identifier(after)
    }

    /// `{real_junk}` = `{real}{identifier}`.
    fn match_real_junk(&self, p: usize) -> Option<usize> {
        let after = self.match_real(p)?;
        self.match_identifier(after)
    }

    /// `{param_junk}` = `\${decdigit}+{identifier}`.
    fn match_param_junk(&self, p: usize) -> Option<usize> {
        let after = self.match_param(p)?;
        self.match_identifier(after)
    }

    /// Compute the winning `INITIAL` rule (longest match, ties by rule order).
    fn pick_initial_rule(&self, p: usize) -> Option<(InitialRule, usize)> {
        let mut best: Option<(InitialRule, usize)> = None;
        let consider = |rule: InitialRule, end: Option<usize>, best: &mut Option<(InitialRule, usize)>| {
            if let Some(end) = end {
                // All candidate matches start at the same `p`, so the match
                // length is `end - p`; comparing `end` directly is equivalent to
                // comparing lengths.  (`best` stores the winning end offset.)
                let len = end - p;
                // Flex requires a nonzero match (no rule here matches empty
                // except via constructs that always consume >=1 byte).
                if len == 0 {
                    return;
                }
                match best {
                    // Keep the existing match if it is strictly longer ...
                    Some((_, bend)) if *bend > end => {}
                    // ... or equally long but from an earlier (<=) rule, since
                    // flex breaks length ties in favour of the earlier rule and
                    // `consider` is invoked in rule-priority order.
                    Some((brule, bend)) if *bend == end && *brule <= rule => {}
                    _ => *best = Some((rule, end)),
                }
            }
        };

        consider(InitialRule::Whitespace, self.match_whitespace(p), &mut best);
        consider(InitialRule::Xcstart, self.match_xcstart(p), &mut best);
        consider(InitialRule::Xbstart, self.match_letter_quote(p, b"bB"), &mut best);
        consider(InitialRule::Xhstart, self.match_letter_quote(p, b"xX"), &mut best);
        consider(InitialRule::Xnstart, self.match_letter_quote(p, b"nN"), &mut best);
        consider(InitialRule::Xqstart, (self.at(p) == b'\'').then_some(p + 1), &mut best);
        consider(InitialRule::Xestart, self.match_letter_quote(p, b"eE"), &mut best);
        consider(InitialRule::Xusstart, self.match_u_amp(p, b'\''), &mut best);
        consider(InitialRule::Dolqdelim, self.match_dolqdelim(p), &mut best);
        consider(InitialRule::Dolqfailed, self.match_dolqfailed(p), &mut best);
        consider(InitialRule::Xdstart, (self.at(p) == b'"').then_some(p + 1), &mut best);
        consider(InitialRule::Xuistart, self.match_u_amp(p, b'"'), &mut best);
        consider(InitialRule::Xufailed, self.match_xufailed(p), &mut best);
        consider(InitialRule::Typecast, self.match_lit(p, b"::"), &mut best);
        consider(InitialRule::DotDot, self.match_lit(p, b".."), &mut best);
        consider(InitialRule::ColonEquals, self.match_lit(p, b":="), &mut best);
        consider(InitialRule::EqualsGreater, self.match_lit(p, b"=>"), &mut best);
        consider(InitialRule::LessEquals, self.match_lit(p, b"<="), &mut best);
        consider(InitialRule::GreaterEquals, self.match_lit(p, b">="), &mut best);
        consider(InitialRule::LessGreater, self.match_lit(p, b"<>"), &mut best);
        consider(InitialRule::NotEquals, self.match_lit(p, b"!="), &mut best);
        consider(InitialRule::SelfChar, is_self_char(self.at(p)).then_some(p + 1), &mut best);
        consider(InitialRule::Operator, self.match_operator(p), &mut best);
        consider(InitialRule::Param, self.match_param(p), &mut best);
        consider(InitialRule::ParamJunk, self.match_param_junk(p), &mut best);
        consider(InitialRule::Decinteger, self.match_decinteger(p), &mut best);
        consider(InitialRule::Hexinteger, self.match_radix_integer(p, b"xX", is_hex_digit), &mut best);
        consider(InitialRule::Octinteger, self.match_radix_integer(p, b"oO", is_oct_digit), &mut best);
        consider(InitialRule::Bininteger, self.match_radix_integer(p, b"bB", is_bin_digit), &mut best);
        consider(InitialRule::Hexfail, self.match_radix_fail(p, b"xX"), &mut best);
        consider(InitialRule::Octfail, self.match_radix_fail(p, b"oO"), &mut best);
        consider(InitialRule::Binfail, self.match_radix_fail(p, b"bB"), &mut best);
        consider(InitialRule::Numeric, self.match_numeric(p), &mut best);
        consider(InitialRule::Numericfail, self.match_numericfail(p), &mut best);
        consider(InitialRule::Real, self.match_real(p), &mut best);
        consider(InitialRule::Realfail, self.match_realfail(p), &mut best);
        consider(InitialRule::IntegerJunk, self.match_integer_junk(p), &mut best);
        consider(InitialRule::NumericJunk, self.match_numeric_junk(p), &mut best);
        consider(InitialRule::RealJunk, self.match_real_junk(p), &mut best);
        consider(InitialRule::Identifier, self.match_identifier(p), &mut best);
        // {other} = any single char. Only matches when nothing else does (it's
        // length 1 and last in order), so always offer it.
        consider(InitialRule::Other, (!self.eof_at(p)).then_some(p + 1), &mut best);

        best
    }

    /// Run the `INITIAL`-state rules.  Returns `Ok(Some(token))` to emit a
    /// token, `Ok(None)` to continue scanning (whitespace/comment/state
    /// switch), or `Err` for a lexer error.
    fn lex_initial(&mut self) -> Result<Option<Token>, LexError> {
        let p = self.pos;

        // <<EOF>> (scan.l:1102): at end of buffer, terminate.
        if self.eof_at(p) {
            self.set_yylloc();
            return Ok(Some(self.eof_token()));
        }

        let Some((rule, end)) = self.pick_initial_rule(p) else {
            // Should not happen: {other} always matches one byte.
            self.set_yylloc();
            return Ok(Some(self.eof_token()));
        };
        self.pos = end;

        match rule {
            InitialRule::Whitespace => Ok(None), // ignore
            InitialRule::Xcstart => {
                self.set_yylloc();
                self.xcdepth = 0;
                self.state = State::Xc;
                self.yyless(2); // put back chars past slash-star
                Ok(None)
            }
            InitialRule::Xbstart => {
                self.set_yylloc();
                self.state = State::Xb;
                self.startlit();
                self.addlitchar(b'b');
                Ok(None)
            }
            InitialRule::Xhstart => {
                self.set_yylloc();
                self.state = State::Xh;
                self.startlit();
                self.addlitchar(b'x');
                Ok(None)
            }
            InitialRule::Xnstart => {
                // National character: re-emit only the leading 'n'.
                self.set_yylloc();
                self.yyless(1);
                let kwnum = common_keywords::ScanKeywordLookup("nchar", &common_keywords::ScanKeywords);
                if kwnum >= 0 {
                    let kw = common_keywords::GetScanKeyword(kwnum as usize, &common_keywords::ScanKeywords)
                        .expect("nchar keyword index valid");
                    Ok(Some(self.keyword_token(kwnum as usize, kw)))
                } else {
                    Ok(Some(self.make_token(tokens::IDENT, CoreYYSTYPE::Str(b"n".to_vec()))))
                }
            }
            InitialRule::Xqstart => {
                self.warn_on_first_escape = true;
                self.saw_non_ascii = false;
                self.set_yylloc();
                self.state = if self.standard_conforming_strings {
                    State::Xq
                } else {
                    State::Xe
                };
                self.startlit();
                Ok(None)
            }
            InitialRule::Xestart => {
                self.warn_on_first_escape = false;
                self.saw_non_ascii = false;
                self.set_yylloc();
                self.state = State::Xe;
                self.startlit();
                Ok(None)
            }
            InitialRule::Xusstart => {
                self.set_yylloc();
                if !self.standard_conforming_strings {
                    return Err(LexError {
                        sqlstate: ERRCODE_FEATURE_NOT_SUPPORTED,
                        message: "unsafe use of string constant with Unicode escapes",
                        detail: Some(
                            "String constants with Unicode escapes cannot be used when \
                             \"standard_conforming_strings\" is off.",
                        ),
                        hint: None,
                        location: self.current_location(),
                        source: None,
                    });
                }
                self.state = State::Xus;
                self.startlit();
                Ok(None)
            }
            InitialRule::Dolqdelim => {
                self.set_yylloc();
                self.dolqstart = Some(self.yytext().to_vec());
                self.state = State::Xdolq;
                self.startlit();
                Ok(None)
            }
            InitialRule::Dolqfailed => {
                self.set_yylloc();
                self.yyless(1); // throw back all but initial "$"
                let ch = self.yytext()[0];
                Ok(Some(self.char_token(ch)))
            }
            InitialRule::Xdstart => {
                self.set_yylloc();
                self.state = State::Xd;
                self.startlit();
                Ok(None)
            }
            InitialRule::Xuistart => {
                self.set_yylloc();
                self.state = State::Xui;
                self.startlit();
                Ok(None)
            }
            InitialRule::Xufailed => {
                self.set_yylloc();
                self.yyless(1); // throw back all but initial u/U
                // treat it as {identifier}: downcase_truncate_identifier("u")
                let ident = self.downcase_truncate(self.yytext())?;
                Ok(Some(self.make_token(tokens::IDENT, CoreYYSTYPE::Str(ident))))
            }
            InitialRule::Typecast => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::TYPECAST)))
            }
            InitialRule::DotDot => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::DOT_DOT)))
            }
            InitialRule::ColonEquals => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::COLON_EQUALS)))
            }
            InitialRule::EqualsGreater => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::EQUALS_GREATER)))
            }
            InitialRule::LessEquals => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::LESS_EQUALS)))
            }
            InitialRule::GreaterEquals => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::GREATER_EQUALS)))
            }
            InitialRule::LessGreater => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::NOT_EQUALS)))
            }
            InitialRule::NotEquals => {
                self.set_yylloc();
                Ok(Some(self.simple_token(tokens::NOT_EQUALS)))
            }
            InitialRule::SelfChar => {
                self.set_yylloc();
                let ch = self.yytext()[0];
                Ok(Some(self.char_token(ch)))
            }
            InitialRule::Operator => self.lex_operator(),
            InitialRule::Param => {
                use backend_utils_error::SoftErrorContext;
                self.set_yylloc();
                let digits = &self.yytext()[1..];
                let s = std::str::from_utf8(digits).unwrap_or("\u{FFFF}");
                let mut escontext = SoftErrorContext::new(false);
                let val = backend_utils_adt_numutils::pg_strtoint32_safe(s, Some(&mut escontext));
                match val {
                    Ok(v) if !escontext.error_occurred() => {
                        Ok(Some(self.make_token(tokens::PARAM, CoreYYSTYPE::Ival(v))))
                    }
                    _ => Err(self.lexerr("parameter number too large")),
                }
            }
            InitialRule::ParamJunk => {
                self.set_yylloc();
                Err(self.lexerr("trailing junk after parameter"))
            }
            InitialRule::Decinteger => {
                self.set_yylloc();
                let tok = self.process_integer_literal(self.yytext(), 10);
                Ok(Some(tok))
            }
            InitialRule::Hexinteger => {
                self.set_yylloc();
                let tok = self.process_integer_literal(self.yytext(), 16);
                Ok(Some(tok))
            }
            InitialRule::Octinteger => {
                self.set_yylloc();
                let tok = self.process_integer_literal(self.yytext(), 8);
                Ok(Some(tok))
            }
            InitialRule::Bininteger => {
                self.set_yylloc();
                let tok = self.process_integer_literal(self.yytext(), 2);
                Ok(Some(tok))
            }
            InitialRule::Hexfail => {
                self.set_yylloc();
                Err(self.lexerr("invalid hexadecimal integer"))
            }
            InitialRule::Octfail => {
                self.set_yylloc();
                Err(self.lexerr("invalid octal integer"))
            }
            InitialRule::Binfail => {
                self.set_yylloc();
                Err(self.lexerr("invalid binary integer"))
            }
            InitialRule::Numeric => {
                self.set_yylloc();
                let s = self.yytext().to_vec();
                Ok(Some(self.make_token(tokens::FCONST, CoreYYSTYPE::Str(s))))
            }
            InitialRule::Numericfail => {
                // throw back the "..", and treat as integer
                self.yyless(self.yyleng() - 2);
                self.set_yylloc();
                let tok = self.process_integer_literal(self.yytext(), 10);
                Ok(Some(tok))
            }
            InitialRule::Real => {
                self.set_yylloc();
                let s = self.yytext().to_vec();
                Ok(Some(self.make_token(tokens::FCONST, CoreYYSTYPE::Str(s))))
            }
            InitialRule::Realfail => {
                self.set_yylloc();
                Err(self.lexerr("trailing junk after numeric literal"))
            }
            InitialRule::IntegerJunk
            | InitialRule::NumericJunk
            | InitialRule::RealJunk => {
                self.set_yylloc();
                Err(self.lexerr("trailing junk after numeric literal"))
            }
            InitialRule::Identifier => {
                self.set_yylloc();
                let kwnum = {
                    let text = std::str::from_utf8(self.yytext()).unwrap_or("\u{FFFF}");
                    common_keywords::ScanKeywordLookup(text, &common_keywords::ScanKeywords)
                };
                if kwnum >= 0 {
                    let kw = common_keywords::GetScanKeyword(
                        kwnum as usize,
                        &common_keywords::ScanKeywords,
                    )
                    .expect("keyword index valid");
                    Ok(Some(self.keyword_token(kwnum as usize, kw)))
                } else {
                    let ident = self.downcase_truncate(self.yytext())?;
                    Ok(Some(self.make_token(tokens::IDENT, CoreYYSTYPE::Str(ident))))
                }
            }
            InitialRule::Other => {
                self.set_yylloc();
                let ch = self.yytext()[0];
                Ok(Some(self.char_token(ch)))
            }
        }
    }
}
