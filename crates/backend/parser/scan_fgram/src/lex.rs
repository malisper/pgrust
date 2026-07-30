// Actions for scan.l's 72 rules plus the per-state <<EOF>> rules, dispatched
// by flex rule number from the DFA walk.

use crate::dfa::{YY_END_OF_BUFFER, YY_STATE_EOF_BASE};
use crate::tokens;
use crate::{CoreYYSTYPE, Scanner, State, Token, BACKSLASH_QUOTE_OFF, BACKSLASH_QUOTE_SAFE_ENCODING};
use elog::ereport;
use types_error::{
    ErrorLocation, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_ESCAPE_SEQUENCE, ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER, WARNING,
};

const NAMEDATALEN: usize = types_core::fmgr::NAMEDATALEN as usize;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

enum Lex<'mcx> {
    Tok(Token<'mcx>),
    Continue,
}

impl<'mcx> Scanner<'mcx> {
    // C core_yylex's exact boundary: token code in the return register,
    // value/location written through the parser's own slots (no >16B struct
    // return crossing the per-token call).
    pub fn core_yylex(
        &mut self,
        lvalp: &mut CoreYYSTYPE<'mcx>,
        llocp: &mut i32,
    ) -> PgResult<i32> {
        loop {
            self.tok_start = self.pos;
            let (mut act, mut end) = self.dfa_match();
            if act == YY_END_OF_BUFFER {
                (act, end) = self.handle_eob();
            }
            self.pos = end;
            if let Lex::Tok(tok) = self.do_action(act)? {
                *lvalp = tok.value;
                *llocp = tok.location;
                return Ok(tok.token);
            }
        }
    }

    fn make(&self, token: i32, value: CoreYYSTYPE<'mcx>) -> Lex<'mcx> {
        Lex::Tok(Token {
            token,
            value,
            location: self.yylloc,
        })
    }

    fn simple(&self, token: i32) -> Lex<'mcx> {
        self.make(token, CoreYYSTYPE::None)
    }

    fn char_token(&self, ch: u8) -> Lex<'mcx> {
        self.make(ch as i32, CoreYYSTYPE::None)
    }

    fn do_action(&mut self, act: i32) -> PgResult<Lex<'mcx>> {
        match act {
            1 => Ok(Lex::Continue),
            2 => {
                self.set_yylloc();
                self.xcdepth = 0;
                self.state = State::Xc;
                self.yyless(2);
                Ok(Lex::Continue)
            }
            3 => {
                self.xcdepth += 1;
                self.yyless(2);
                Ok(Lex::Continue)
            }
            4 => {
                if self.xcdepth <= 0 {
                    self.state = State::Initial;
                } else {
                    self.xcdepth -= 1;
                }
                Ok(Lex::Continue)
            }
            5 | 6 | 7 => Ok(Lex::Continue),
            8 => {
                self.set_yylloc();
                self.state = State::Xb;
                self.startlit();
                self.addlitchar(b'b')?;
                Ok(Lex::Continue)
            }
            9 | 10 => {
                self.addlit(self.yytext())?;
                Ok(Lex::Continue)
            }
            11 => {
                self.set_yylloc();
                self.state = State::Xh;
                self.startlit();
                self.addlitchar(b'x')?;
                Ok(Lex::Continue)
            }
            12 => {
                self.set_yylloc();
                self.yyless(1);
                let kwnum = keywords::ScanKeywordLookup(b"nchar", &keywords::ScanKeywords);
                if kwnum >= 0 {
                    Ok(self.keyword_token(kwnum as usize))
                } else {
                    Ok(self.make(tokens::IDENT, CoreYYSTYPE::Str(b"n")))
                }
            }
            13 => {
                self.warn_on_first_escape = true;
                self.saw_non_ascii = false;
                self.set_yylloc();
                self.state = if self.standard_conforming_strings {
                    State::Xq
                } else {
                    State::Xe
                };
                self.startlit();
                Ok(Lex::Continue)
            }
            14 => {
                self.warn_on_first_escape = false;
                self.saw_non_ascii = false;
                self.set_yylloc();
                self.state = State::Xe;
                self.startlit();
                Ok(Lex::Continue)
            }
            15 => {
                self.set_yylloc();
                if !self.standard_conforming_strings {
                    return Err(self.lexerr(
                        ERRCODE_FEATURE_NOT_SUPPORTED,
                        "unsafe use of string constant with Unicode escapes",
                        Some(
                            "String constants with Unicode escapes cannot be used when \
                             \"standard_conforming_strings\" is off.",
                        ),
                        None,
                    ));
                }
                self.state = State::Xus;
                self.startlit();
                Ok(Lex::Continue)
            }
            16 => {
                self.state_before_str_stop = self.state;
                self.state = State::Xqs;
                Ok(Lex::Continue)
            }
            17 => {
                self.state = self.state_before_str_stop;
                Ok(Lex::Continue)
            }
            18 | 19 => self.stop_string(),
            20 => {
                self.addlitchar(b'\'')?;
                Ok(Lex::Continue)
            }
            21 | 22 => {
                self.addlit(self.yytext())?;
                Ok(Lex::Continue)
            }
            23 => {
                let c = parse_hex(&self.yytext()[2..]);
                self.check_escape_warning()?;
                let save = self.yylloc;
                self.set_yylloc();
                let res = if wchar::is_utf16_surrogate_first(c) {
                    self.utf16_first_part = c;
                    self.state = State::Xeu;
                    Ok(())
                } else if wchar::is_utf16_surrogate_second(c) {
                    Err(self.yyerr("invalid Unicode surrogate pair"))
                } else {
                    self.addunicode(c)
                };
                self.yylloc = save;
                res.map(|()| Lex::Continue)
            }
            24 => {
                let c = parse_hex(&self.yytext()[2..]);
                let save = self.yylloc;
                self.set_yylloc();
                let res = if !wchar::is_utf16_surrogate_second(c) {
                    Err(self.yyerr("invalid Unicode surrogate pair"))
                } else {
                    self.addunicode(wchar::surrogate_pair_to_codepoint(self.utf16_first_part, c))
                };
                self.yylloc = save;
                self.state = State::Xe;
                res.map(|()| Lex::Continue)
            }
            25 | 26 => {
                self.set_yylloc();
                Err(self.yyerr("invalid Unicode surrogate pair"))
            }
            27 => {
                self.set_yylloc();
                Err(self.lexerr(
                    ERRCODE_INVALID_ESCAPE_SEQUENCE,
                    "invalid Unicode escape",
                    None,
                    Some("Unicode escapes must be \\uXXXX or \\UXXXXXXXX."),
                ))
            }
            28 => {
                let escaped = self.yytext()[1];
                if escaped == b'\'' && self.backslash_quote_forbidden() {
                    return Err(self.lexerr(
                        ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
                        "unsafe use of \\' in a string literal",
                        None,
                        Some(
                            "Use '' to write quotes in strings. \\' is insecure in \
                             client-only encodings.",
                        ),
                    ));
                }
                self.check_string_escape_warning(escaped)?;
                let c = self.unescape_single_char(escaped);
                self.addlitchar(c)?;
                Ok(Lex::Continue)
            }
            29 => {
                let c = parse_oct(&self.yytext()[1..]) as u8;
                self.check_escape_warning()?;
                self.addlitchar(c)?;
                if c == 0 || c & 0x80 != 0 {
                    self.saw_non_ascii = true;
                }
                Ok(Lex::Continue)
            }
            30 => {
                let c = parse_hex(&self.yytext()[2..]) as u8;
                self.check_escape_warning()?;
                self.addlitchar(c)?;
                if c == 0 || c & 0x80 != 0 {
                    self.saw_non_ascii = true;
                }
                Ok(Lex::Continue)
            }
            31 | 37 => {
                self.addlitchar(self.yytext()[0])?;
                Ok(Lex::Continue)
            }
            32 => {
                self.set_yylloc();
                self.dolqstart = Some(self.yytext());
                self.state = State::Xdolq;
                self.startlit();
                Ok(Lex::Continue)
            }
            33 => {
                self.set_yylloc();
                self.yyless(1);
                Ok(self.char_token(self.yytext()[0]))
            }
            34 => {
                let text = self.yytext();
                if Some(text) == self.dolqstart {
                    self.dolqstart = None;
                    self.state = State::Initial;
                    let s = self.litbufdup()?;
                    Ok(self.make(tokens::SCONST, CoreYYSTYPE::Str(s)))
                } else {
                    self.addlit(&text[..text.len() - 1])?;
                    self.yyless(text.len() - 1);
                    Ok(Lex::Continue)
                }
            }
            35 | 36 => {
                self.addlit(self.yytext())?;
                Ok(Lex::Continue)
            }
            38 => {
                self.set_yylloc();
                self.state = State::Xd;
                self.startlit();
                Ok(Lex::Continue)
            }
            39 => {
                self.set_yylloc();
                self.state = State::Xui;
                self.startlit();
                Ok(Lex::Continue)
            }
            40 => {
                self.state = State::Initial;
                if self.literalbuf.is_empty() {
                    return Err(self.yyerr("zero-length delimited identifier"));
                }
                let ident = if self.literalbuf.len() >= NAMEDATALEN {
                    let mut v = mcx::slice_in(self.mcx, &self.literalbuf)?;
                    parser_small1::truncate_identifier(&mut v, true, self.encoding)?;
                    mcx::vec_borrow_in(self.mcx, v)?
                } else {
                    self.litbufdup()?
                };
                Ok(self.make(tokens::IDENT, CoreYYSTYPE::Str(ident)))
            }
            41 => {
                self.state = State::Initial;
                if self.literalbuf.is_empty() {
                    return Err(self.yyerr("zero-length delimited identifier"));
                }
                let s = self.litbufdup()?;
                Ok(self.make(tokens::UIDENT, CoreYYSTYPE::Str(s)))
            }
            42 => {
                self.addlitchar(b'"')?;
                Ok(Lex::Continue)
            }
            43 => {
                self.addlit(self.yytext())?;
                Ok(Lex::Continue)
            }
            44 => {
                self.set_yylloc();
                self.yyless(1);
                let ident = parser_small1::downcase_truncate_identifier(
                    self.mcx,
                    self.yytext(),
                    true,
                    self.encoding,
                )?;
                let ident = mcx::vec_borrow_in(self.mcx, ident)?;
                Ok(self.make(tokens::IDENT, CoreYYSTYPE::Str(ident)))
            }
            45 => {
                self.set_yylloc();
                Ok(self.simple(tokens::TYPECAST))
            }
            46 => {
                self.set_yylloc();
                Ok(self.simple(tokens::DOT_DOT))
            }
            47 => {
                self.set_yylloc();
                Ok(self.simple(tokens::COLON_EQUALS))
            }
            48 => {
                self.set_yylloc();
                Ok(self.simple(tokens::EQUALS_GREATER))
            }
            49 => {
                self.set_yylloc();
                Ok(self.simple(tokens::LESS_EQUALS))
            }
            50 => {
                self.set_yylloc();
                Ok(self.simple(tokens::GREATER_EQUALS))
            }
            51 | 52 => {
                self.set_yylloc();
                Ok(self.simple(tokens::NOT_EQUALS))
            }
            53 => {
                self.set_yylloc();
                Ok(self.char_token(self.yytext()[0]))
            }
            54 => self.operator_action(),
            55 => {
                self.set_yylloc();
                match parse_int32(&self.yytext()[1..]) {
                    Some(val) => Ok(self.make(tokens::PARAM, CoreYYSTYPE::Ival(val))),
                    None => Err(self.yyerr("parameter number too large")),
                }
            }
            56 => {
                self.set_yylloc();
                Err(self.yyerr("trailing junk after parameter"))
            }
            57 | 58 | 59 | 60 => {
                self.set_yylloc();
                Ok(self.process_integer_literal())
            }
            61 => {
                self.set_yylloc();
                Err(self.yyerr("invalid hexadecimal integer"))
            }
            62 => {
                self.set_yylloc();
                Err(self.yyerr("invalid octal integer"))
            }
            63 => {
                self.set_yylloc();
                Err(self.yyerr("invalid binary integer"))
            }
            64 | 66 => {
                self.set_yylloc();
                Ok(self.make(tokens::FCONST, CoreYYSTYPE::Str(self.yytext())))
            }
            65 => {
                self.yyless(self.yyleng() - 2);
                self.set_yylloc();
                Ok(self.process_integer_literal())
            }
            67 | 68 | 69 | 70 => {
                self.set_yylloc();
                Err(self.yyerr("trailing junk after numeric literal"))
            }
            71 => {
                self.set_yylloc();
                let kwnum = keywords::ScanKeywordLookup(self.yytext(), &keywords::ScanKeywords);
                if kwnum >= 0 {
                    return Ok(self.keyword_token(kwnum as usize));
                }
                let ident = parser_small1::downcase_truncate_identifier(
                    self.mcx,
                    self.yytext(),
                    true,
                    self.encoding,
                )?;
                let ident = mcx::vec_borrow_in(self.mcx, ident)?;
                Ok(self.make(tokens::IDENT, CoreYYSTYPE::Str(ident)))
            }
            72 => {
                self.set_yylloc();
                Ok(self.char_token(self.yytext()[0]))
            }
            _ => self.do_eof_action(act),
        }
    }

    #[cold]
    fn do_eof_action(&mut self, act: i32) -> PgResult<Lex<'mcx>> {
        let eof_state = act - YY_STATE_EOF_BASE;
        match eof_state {
            s if s == State::Initial as i32 => {
                self.set_yylloc();
                Ok(self.simple(crate::YY_NULL))
            }
            s if s == State::Xb as i32 => Err(self.yyerr("unterminated bit string literal")),
            s if s == State::Xc as i32 => Err(self.yyerr("unterminated /* comment")),
            s if s == State::Xd as i32 || s == State::Xui as i32 => {
                Err(self.yyerr("unterminated quoted identifier"))
            }
            s if s == State::Xh as i32 => {
                Err(self.yyerr("unterminated hexadecimal string literal"))
            }
            s if s == State::Xq as i32 || s == State::Xe as i32 || s == State::Xus as i32 => {
                Err(self.yyerr("unterminated quoted string"))
            }
            s if s == State::Xqs as i32 => self.stop_string(),
            s if s == State::Xdolq as i32 => Err(self.yyerr("unterminated dollar-quoted string")),
            s if s == State::Xeu as i32 => {
                self.set_yylloc();
                Err(self.yyerr("invalid Unicode surrogate pair"))
            }
            _ => panic!("scan_fgram: no action for flex rule {act} in state {:?}", self.state),
        }
    }

    // <xqs>{quotecontinuefail} | <xqs>{other} | <xqs><<EOF>> (scan.l:596).
    fn stop_string(&mut self) -> PgResult<Lex<'mcx>> {
        self.yyless(0);
        self.state = State::Initial;
        match self.state_before_str_stop {
            State::Xb => {
                let s = self.litbufdup()?;
                Ok(self.make(tokens::BCONST, CoreYYSTYPE::Str(s)))
            }
            State::Xh => {
                let s = self.litbufdup()?;
                Ok(self.make(tokens::XCONST, CoreYYSTYPE::Str(s)))
            }
            State::Xq | State::Xe => {
                if self.saw_non_ascii {
                    self.verifymbstr()?;
                }
                let s = self.litbufdup()?;
                Ok(self.make(tokens::SCONST, CoreYYSTYPE::Str(s)))
            }
            State::Xus => {
                let s = self.litbufdup()?;
                Ok(self.make(tokens::USCONST, CoreYYSTYPE::Str(s)))
            }
            _ => Err(self.yyerr("unhandled previous state in xqs")),
        }
    }

    fn keyword_token(&self, kwnum: usize) -> Lex<'mcx> {
        let kw = keywords::keyword_text(kwnum).expect("keyword index valid");
        self.make(
            crate::SCAN_KEYWORD_TOKENS[kwnum] as i32,
            CoreYYSTYPE::Keyword(kw),
        )
    }

    // {operator} (scan.l:886).
    fn operator_action(&mut self) -> PgResult<Lex<'mcx>> {
        let yytext = self.yytext();
        let mut nchars = yytext.len();

        let slashstar = find_sub(yytext, b"/*");
        let dashdash = find_sub(yytext, b"--");
        if let Some(cut) = match (slashstar, dashdash) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        } {
            nchars = cut;
        }

        if nchars > 1 && matches!(yytext[nchars - 1], b'+' | b'-') {
            let qualifying = yytext[..nchars - 1].iter().any(|&c| {
                matches!(c, b'~' | b'!' | b'@' | b'#' | b'^' | b'&' | b'|' | b'`' | b'?' | b'%')
            });
            if !qualifying {
                loop {
                    nchars -= 1;
                    if !(nchars > 1 && matches!(yytext[nchars - 1], b'+' | b'-')) {
                        break;
                    }
                }
            }
        }

        self.set_yylloc();

        if nchars < yytext.len() {
            self.yyless(nchars);
            if nchars == 1 && b",()[].;:+-*/%^<>=".contains(&yytext[0]) {
                return Ok(self.char_token(yytext[0]));
            }
            if nchars == 2 {
                match (yytext[0], yytext[1]) {
                    (b'=', b'>') => return Ok(self.simple(tokens::EQUALS_GREATER)),
                    (b'>', b'=') => return Ok(self.simple(tokens::GREATER_EQUALS)),
                    (b'<', b'=') => return Ok(self.simple(tokens::LESS_EQUALS)),
                    (b'<', b'>') | (b'!', b'=') => return Ok(self.simple(tokens::NOT_EQUALS)),
                    _ => {}
                }
            }
        }

        if nchars >= NAMEDATALEN {
            return Err(self.yyerr("operator too long"));
        }

        Ok(self.make(tokens::Op, CoreYYSTYPE::Str(&yytext[..nchars])))
    }

    // process_integer_literal (scan.l:1391): out-of-range integers fall back
    // to FCONST carrying the original text.
    fn process_integer_literal(&self) -> Lex<'mcx> {
        match parse_int32(self.yytext()) {
            Some(val) => self.make(tokens::ICONST, CoreYYSTYPE::Ival(val)),
            None => self.make(tokens::FCONST, CoreYYSTYPE::Str(self.yytext())),
        }
    }

    // unescape_single_char (scan.l:1427).
    fn unescape_single_char(&mut self, c: u8) -> u8 {
        match c {
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => 0x0b,
            _ => {
                if c == 0 || c & 0x80 != 0 {
                    self.saw_non_ascii = true;
                }
                c
            }
        }
    }

    fn backslash_quote_forbidden(&self) -> bool {
        self.backslash_quote == BACKSLASH_QUOTE_OFF
            || (self.backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING
                && wchar::pg_encoding_is_client_only(self.client_encoding))
    }

    // check_string_escape_warning (scan.l:1453).
    fn check_string_escape_warning(&mut self, ychar: u8) -> PgResult<()> {
        if ychar == b'\'' {
            if self.warn_on_first_escape && self.escape_string_warning {
                ereport(WARNING)
                    .errcode(ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER)
                    .errmsg("nonstandard use of \\' in a string literal")
                    .errhint(
                        "Use '' to write quotes in strings, or use the escape string \
                         syntax (E'...').",
                    )
                    .errposition(self.scanner_errposition(self.yylloc))
                    .finish(loc("check_string_escape_warning"))?;
            }
            self.warn_on_first_escape = false;
        } else if ychar == b'\\' {
            if self.warn_on_first_escape && self.escape_string_warning {
                ereport(WARNING)
                    .errcode(ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER)
                    .errmsg("nonstandard use of \\\\ in a string literal")
                    .errhint("Use the escape string syntax for backslashes, e.g., E'\\\\'.")
                    .errposition(self.scanner_errposition(self.yylloc))
                    .finish(loc("check_string_escape_warning"))?;
            }
            self.warn_on_first_escape = false;
        } else {
            self.check_escape_warning()?;
        }
        Ok(())
    }

    // check_escape_warning (scan.l:1480).
    fn check_escape_warning(&mut self) -> PgResult<()> {
        if self.warn_on_first_escape && self.escape_string_warning {
            ereport(WARNING)
                .errcode(ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER)
                .errmsg("nonstandard use of escape in a string literal")
                .errhint("Use the escape string syntax for escapes, e.g., E'\\r\\n'.")
                .errposition(self.scanner_errposition(self.yylloc))
                .finish(loc("check_escape_warning"))?;
        }
        self.warn_on_first_escape = false;
        Ok(())
    }

    // addunicode (scan.l:1408) with pg_unicode_to_server inlined for the
    // UTF-8 / ASCII cases; other server encodings need the (unported)
    // conversion subsystem.
    fn addunicode(&mut self, c: u32) -> PgResult<()> {
        if !wchar::is_valid_unicode_codepoint(c) {
            return Err(self.yyerr("invalid Unicode escape value"));
        }
        if c <= 0x7F {
            self.addlitchar(c as u8)?;
        } else if self.encoding == wchar::PG_UTF8 {
            let mut buf = [0u8; 4];
            wchar::unicode_to_utf8(c, &mut buf);
            let len = wchar::unicode_utf8len(c) as usize;
            self.addlit(&buf[..len])?;
        } else {
            // unported: pg_unicode_to_server's conversion-proc lane. The main
            // SQL parser pins UTF-8, so this only fires off-path (e.g. the
            // pg_stat_statements normalizer on a non-UTF8 database) — raise a
            // clean 0A000 rather than panic.
            return Err(self.lexerr(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                "Unicode escape values above 007F are not supported yet for \
                 non-UTF8 server encodings",
                None,
                None,
            ));
        }
        self.saw_non_ascii = true;
        Ok(())
    }

    // pg_verifymbstr(literalbuf, false), reached when escapes wrote NUL or
    // high-bit bytes (scan.l:619); reports C's invalid-byte-sequence error.
    #[cold]
    fn verifymbstr(&self) -> PgResult<()> {
        let buf = &self.literalbuf;
        let nul = buf.iter().position(|&b| b == 0);
        let valid = wchar::pg_encoding_verifymbstr(self.encoding, buf) as usize;
        let bad = match nul {
            Some(n) if n < valid => n,
            _ if valid == buf.len() => return Ok(()),
            _ => valid,
        };
        let mblen = (wchar::pg_encoding_mblen(self.encoding, &buf[bad..]) as usize)
            .min(buf.len() - bad);
        let mut seq = String::new();
        for (i, b) in buf[bad..bad + mblen].iter().enumerate() {
            if i > 0 {
                seq.push(' ');
            }
            seq.push_str(&format!("0x{b:02x}"));
        }
        Err(Box::new(
            types_error::PgError::error(format!(
                "invalid byte sequence for encoding \"{}\": {}",
                encoding_name(self.encoding),
                seq
            ))
            .with_sqlstate(types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
        ))
    }
}

// pg_enc2name_tbl (encnames.c), backend-legal encodings only.
fn encoding_name(enc: wchar::pg_enc) -> &'static str {
    match enc {
        wchar::PG_SQL_ASCII => "SQL_ASCII",
        wchar::PG_EUC_JP => "EUC_JP",
        wchar::PG_EUC_CN => "EUC_CN",
        wchar::PG_EUC_KR => "EUC_KR",
        wchar::PG_EUC_TW => "EUC_TW",
        wchar::PG_EUC_JIS_2004 => "EUC_JIS_2004",
        wchar::PG_UTF8 => "UTF8",
        wchar::PG_MULE_INTERNAL => "MULE_INTERNAL",
        wchar::PG_LATIN1 => "LATIN1",
        wchar::PG_LATIN2 => "LATIN2",
        wchar::PG_LATIN3 => "LATIN3",
        wchar::PG_LATIN4 => "LATIN4",
        wchar::PG_LATIN5 => "LATIN5",
        wchar::PG_LATIN6 => "LATIN6",
        wchar::PG_LATIN7 => "LATIN7",
        wchar::PG_LATIN8 => "LATIN8",
        wchar::PG_LATIN9 => "LATIN9",
        wchar::PG_LATIN10 => "LATIN10",
        wchar::PG_WIN1256 => "WIN1256",
        wchar::PG_WIN1258 => "WIN1258",
        wchar::PG_WIN866 => "WIN866",
        wchar::PG_WIN874 => "WIN874",
        wchar::PG_KOI8R => "KOI8R",
        wchar::PG_WIN1251 => "WIN1251",
        wchar::PG_WIN1252 => "WIN1252",
        wchar::PG_ISO_8859_5 => "ISO_8859_5",
        wchar::PG_ISO_8859_6 => "ISO_8859_6",
        wchar::PG_ISO_8859_7 => "ISO_8859_7",
        wchar::PG_ISO_8859_8 => "ISO_8859_8",
        wchar::PG_WIN1250 => "WIN1250",
        wchar::PG_WIN1253 => "WIN1253",
        wchar::PG_WIN1254 => "WIN1254",
        wchar::PG_WIN1255 => "WIN1255",
        wchar::PG_WIN1257 => "WIN1257",
        wchar::PG_KOI8U => "KOI8U",
        _ => "???",
    }
}

// pg_strtoint32_safe over the matched literal bytes; the DFA guarantees pure
// ASCII, and soft errors (out of range) yield None.
fn parse_int32(bytes: &[u8]) -> Option<i32> {
    debug_assert!(bytes.is_ascii());
    // SAFETY: matched decinteger/hexinteger/... text is ASCII by the DFA.
    let s = unsafe { std::str::from_utf8_unchecked(bytes) };
    let mut escontext = SoftErrorContext::new(false);
    match numutils::pg_strtoint32_safe(s, Some(&mut escontext)) {
        Ok(v) if !escontext.error_occurred() => Some(v),
        _ => None,
    }
}

fn parse_hex(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0u32, |v, &b| {
        v * 16 + (b as char).to_digit(16).unwrap_or(0)
    })
}

fn parse_oct(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0u32, |v, &b| v * 8 + (b - b'0') as u32)
}

fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() > hay.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}
