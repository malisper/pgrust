use core::mem;

use mcx::Mcx;
use scan_fgram::{tokens, CoreVal, CoreYYSTYPE, Scanner, ScannerSettings};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};
use types_nodes::NodeList;

use crate::stack::Stacks;
use crate::tables::*;
use crate::yystype::YYSTYPE;

pub(crate) const YYINITDEPTH: usize = 200;

pub(crate) struct Parser<'mcx> {
    pub(crate) mcx: Mcx<'mcx>,
    scanbuf: &'mcx [u8],
    settings: ScannerSettings,
    scanner: Scanner<'mcx>,
    have_lookahead: bool,
    la_tok: i32,
    la_val: YYSTYPE<'mcx>,
    la_loc: i32,
    // C's lookahead_end/lookahead_hold_char discipline (parser.c): while a
    // peeked token sits in the buffer, the hold-char NUL is re-placed at the
    // END OF THE CURRENT TOKEN; scanner_yyerror tails stop there. Without a
    // buffered peek the hold char is the scanner's live position.
    cur_tok_end: usize,
    la_end: usize,
    // C's yylloc variable as flex/actions see it: the location of the token
    // most recently returned by base_yylex (parser_yyerror renders here).
    last_yylloc: i32,
    pub(crate) parsetree: NodeList<'mcx>,
}

impl<'mcx> Parser<'mcx> {
    pub(crate) fn new(
        mcx: Mcx<'mcx>,
        scanbuf: &'mcx [u8],
        mode_token: i32,
    ) -> PgResult<Parser<'mcx>> {
        let settings = ScannerSettings::default();
        Ok(Parser {
            mcx,
            scanbuf,
            settings,
            scanner: Scanner::new(scanbuf, mcx, settings),
            have_lookahead: mode_token != 0,
            la_tok: mode_token,
            la_val: YYSTYPE::None,
            la_loc: 0,
            cur_tok_end: 0,
            la_end: 0,
            last_yylloc: 0,
            parsetree: NodeList::nil(),
        })
    }

    // parser.c base_yylex, C's exact boundary (token code returned, value and
    // location through the caller's yylval/yylloc): the one-token-lookahead
    // merge filter keeping the grammar LALR(1).
    fn base_yylex(&mut self, lvalp: &mut YYSTYPE<'mcx>, llocp: &mut i32) -> PgResult<i32> {
        let cur_tok = if self.have_lookahead {
            self.have_lookahead = false;
            *lvalp = mem::take(&mut self.la_val);
            *llocp = self.la_loc;
            self.cur_tok_end = self.la_end;
            self.la_tok
        } else {
            let tok = self.next_token(lvalp, llocp)?;
            self.cur_tok_end = self.scanner.tok_end();
            tok
        };

        self.last_yylloc = *llocp;
        match cur_tok {
            t if t == tokens::FORMAT
                || t == tokens::NOT
                || t == tokens::NULLS_P
                || t == tokens::WITH
                || t == tokens::WITHOUT
                || t == tokens::UIDENT
                || t == tokens::USCONST => {}
            _ => return Ok(cur_tok),
        }

        let mut next_val = YYSTYPE::None;
        let mut next_loc = 0;
        let next_tok = self.next_token(&mut next_val, &mut next_loc)?;
        self.la_end = self.scanner.tok_end();
        self.la_tok = next_tok;
        self.la_val = next_val;
        self.la_loc = next_loc;
        self.have_lookahead = true;

        let merged = match cur_tok {
            t if t == tokens::FORMAT && next_tok == tokens::JSON => tokens::FORMAT_LA,
            t if t == tokens::NOT
                && (next_tok == tokens::BETWEEN
                    || next_tok == tokens::IN_P
                    || next_tok == tokens::LIKE
                    || next_tok == tokens::ILIKE
                    || next_tok == tokens::SIMILAR) =>
            {
                tokens::NOT_LA
            }
            t if t == tokens::NULLS_P
                && (next_tok == tokens::FIRST_P || next_tok == tokens::LAST_P) =>
            {
                tokens::NULLS_LA
            }
            t if t == tokens::WITH
                && (next_tok == tokens::TIME || next_tok == tokens::ORDINALITY) =>
            {
                tokens::WITH_LA
            }
            t if t == tokens::WITHOUT && next_tok == tokens::TIME => tokens::WITHOUT_LA,
            t if t == tokens::UIDENT || t == tokens::USCONST => {
                let escape = if next_tok == tokens::UESCAPE {
                    let mut esc_val = YYSTYPE::None;
                    let mut esc_loc = 0;
                    let esc_tok = self.next_token(&mut esc_val, &mut esc_loc)?;
                    // All three tokens consumed (C clears have_lookahead and
                    // restores the hold char BEFORE validating the escape, so
                    // the two escape errors tail at the third token).
                    self.have_lookahead = false;
                    if esc_tok != tokens::SCONST {
                        return Err(self.syntax_error(
                            "UESCAPE must be followed by a simple string literal",
                            esc_loc,
                        ));
                    }
                    let escstr = esc_val.str_val().as_bytes();
                    if escstr.len() != 1
                        || !parser_small1::udeescape::check_uescapechar(escstr[0])
                    {
                        return Err(
                            self.syntax_error("invalid Unicode escape character", esc_loc)
                        );
                    }
                    escstr[0]
                } else {
                    b'\\'
                };
                let raw = mem::take(lvalp).str_val();
                let mut decoded = parser_small1::udeescape::str_udeescape(
                    self.mcx,
                    raw.as_bytes(),
                    escape,
                    *llocp,
                    self.settings.encoding,
                )
                .map_err(|e| self.udeescape_failure(e))?;
                let out_tok = if t == tokens::UIDENT {
                    parser_small1::truncate_identifier(
                        &mut decoded,
                        true,
                        self.settings.encoding,
                    )?;
                    tokens::IDENT
                } else {
                    tokens::SCONST
                };
                let s = mcx::vec_borrow_in(self.mcx, decoded)?;
                // SAFETY: str_udeescape emits server-encoding (UTF-8) bytes.
                *lvalp = YYSTYPE::Str(unsafe { core::str::from_utf8_unchecked(s) });
                out_tok
            }
            t => t,
        };
        Ok(merged)
    }

    #[track_caller]
    #[cold]
    fn udeescape_error(&self, e: parser_small1::udeescape::UdeescapeError) -> Box<PgError> {
        let err = self.errposition_error(e.message.to_string(), e.location);
        match e.hint {
            Some(h) => Box::new((*err).with_hint(h)),
            None => err,
        }
    }

    // str_udeescape failures: C wraps each escape's processing in
    // setup_scanner_errposition_callback (parser.c), so BOTH the function's
    // own escape errors and hard pg_unicode_to_server errors carry an error
    // cursor pointing at the escape.
    #[cold]
    pub(crate) fn udeescape_failure(
        &self,
        e: parser_small1::udeescape::UdeescapeFailure,
    ) -> Box<PgError> {
        match e {
            parser_small1::udeescape::UdeescapeFailure::Escape(e) => self.udeescape_error(e),
            parser_small1::udeescape::UdeescapeFailure::Hard { error, location } => Box::new(
                (*error).with_cursor_position(parser_small1::parser_errposition_source(
                    Some(self.scanbuf),
                    location,
                    self.settings.encoding,
                )),
            ),
        }
    }

    // gram.y parser_yyerror: scanner_yyerror at the current token.
    #[cold]
    pub(crate) fn parser_yyerror(&self, message: &str) -> Box<PgError> {
        self.syntax_error(message, self.last_yylloc)
    }

    #[cold]
    pub(crate) fn errposition_error(&self, message: String, location: i32) -> Box<PgError> {
        Box::new(
            PgError::error(message)
                .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                .with_cursor_position(parser_small1::parser_errposition_source(
                    Some(self.scanbuf),
                    location,
                    self.settings.encoding,
                )),
        )
    }

    #[cold]
    pub(crate) fn errposition_error_code(
        &self,
        sqlstate: types_error::SqlState,
        message: String,
        location: i32,
    ) -> Box<PgError> {
        Box::new(
            PgError::error(message)
                .with_sqlstate(sqlstate)
                .with_cursor_position(parser_small1::parser_errposition_source(
                    Some(self.scanbuf),
                    location,
                    self.settings.encoding,
                )),
        )
    }

    fn next_token(&mut self, lvalp: &mut YYSTYPE<'mcx>, llocp: &mut i32) -> PgResult<i32> {
        let mut v = CoreYYSTYPE::None;
        let tok = self.scanner.core_yylex(&mut v, llocp)?;
        *lvalp = yystype_from(v);
        Ok(tok)
    }

    // gram.c yyparse; gram.y has no error-recovery productions and yyerror
    // ereports (longjmp), so error labels collapse to returning Err.
    #[inline(never)]
    pub(crate) fn yyparse(&mut self) -> PgResult<()> {
        let mcx = self.mcx;
        // C's yyssa/yyvsa/yylsa: no arena traffic until deep nesting.
        let mut vs_buf: [mem::MaybeUninit<YYSTYPE<'mcx>>; YYINITDEPTH] =
            [const { mem::MaybeUninit::uninit() }; YYINITDEPTH];
        let mut ls_buf: [mem::MaybeUninit<i32>; YYINITDEPTH] =
            [const { mem::MaybeUninit::uninit() }; YYINITDEPTH];
        let mut ss_buf: [mem::MaybeUninit<i16>; YYINITDEPTH + 1] =
            [const { mem::MaybeUninit::uninit() }; YYINITDEPTH + 1];
        // SAFETY: the arrays outlive yyparse; capacities match YYINITDEPTH.
        let mut stk = unsafe {
            Stacks::from_frame(
                vs_buf.as_mut_ptr().cast(),
                ls_buf.as_mut_ptr().cast(),
                ss_buf.as_mut_ptr().cast(),
                YYINITDEPTH,
            )
        };
        let mut yystate: i32 = 0;
        let mut yychar: i32 = YYEMPTY;
        let mut yylval = YYSTYPE::None;
        let mut yylloc: i32 = 0;
        // sp mirrors C's register-resident stack pointers.
        let mut sp: usize = 0;
        // SAFETY (stack ops throughout): indices bounded by `ensure`; every
        // value slot is written before it can be read, and read once.
        unsafe {
            stk.write_state(0, 0);
        }

        'newstate: loop {
            let rule: usize;
            let pact = YYPACT[yystate as usize];
            'decide: {
                if pact != YYPACT_NINF {
                    if yychar == YYEMPTY {
                        yychar = self.base_yylex(&mut yylval, &mut yylloc)?;
                    }
                    let yytoken = if yychar <= YYEOF { YYEOF } else { yytranslate(yychar) };
                    let idx = pact + yytoken;
                    if idx < 0 || idx > YYLAST || YYCHECK[idx as usize] as i32 != yytoken {
                        break 'decide;
                    }
                    let act = YYTABLE[idx as usize] as i32;
                    if act <= 0 {
                        if act == 0 || act == YYTABLE_NINF {
                            return Err(self.syntax_error("syntax error", yylloc));
                        }
                        rule = (-act) as usize;
                    } else {
                        if act == YYFINAL {
                            return Ok(());
                        }
                        yystate = act;
                        self.ensure_stack(&mut stk, sp + 1)?;
                        // SAFETY: see above.
                        unsafe {
                            stk.write_val(sp, mem::take(&mut yylval), yylloc);
                            stk.write_state(sp + 1, act as i16);
                        }
                        sp += 1;
                        if yychar != YYEOF {
                            yychar = YYEMPTY;
                        }
                        continue 'newstate;
                    }
                    yystate = self.reduce_and_goto(&mut stk, rule, &mut sp)?;
                    continue 'newstate;
                }
            }
            rule = YYDEFACT[yystate as usize] as usize;
            if rule == 0 {
                return Err(self.syntax_error("syntax error", yylloc));
            }
            yystate = self.reduce_and_goto(&mut stk, rule, &mut sp)?;
        }
    }

    // gram.c yyoverflow: at YYMAXDEPTH it does `yyerror(&yylloc, "memory
    // exhausted")` = scanner_yyerror, which renders the cursor position + the
    // "at or near <token>" tail and raises ERRCODE_SYNTAX_ERROR. The bare,
    // position-less/sqlstate-less PgError from Stacks::grow diverged on all
    // three (found by gram_core_diff fleet campaign 2026-08-01); route the
    // exhaustion through the same parser_yyerror path C uses.
    #[inline(always)]
    fn ensure_stack(&self, stk: &mut Stacks<'mcx>, new_sp: usize) -> PgResult<()> {
        // C exhausts when the just-pushed state index reaches YYMAXDEPTH-1
        // (`yyss + yystacksize - 1 <= yyssp` with yystacksize == YYMAXDEPTH),
        // reporting the token driving THAT push. new_sp is our push target
        // index, so the boundary is YYMAXDEPTH-1 (not YYMAXDEPTH) — the
        // one-token-deeper cursorpos was caught by gram_core_diff 2026-08-01.
        if new_sp >= YYMAXDEPTH - 1 {
            return Err(self.parser_yyerror("memory exhausted"));
        }
        stk.ensure(self.mcx, new_sp)
    }

    #[inline(always)]
    fn reduce_and_goto(
        &mut self,
        stk: &mut Stacks<'mcx>,
        rule: usize,
        sp: &mut usize,
    ) -> PgResult<i32> {
        let yylen = YYR2[rule] as usize;
        debug_assert!(yylen <= *sp);
        let base = *sp - yylen;
        // YYLLOC_DEFAULT: first non-empty (>= 0) RHS location, else -1.
        let mut yyloc = -1;
        for i in base..*sp {
            // SAFETY: i < sp.
            let l = unsafe { stk.loc(i) };
            if l >= 0 {
                yyloc = l;
                break;
            }
        }
        // DISPATCH folds bison's default action and gram.c's mechanical
        // `$$ = $n` / NIL / NULL bodies; 0 = ported (or panicking) action fn.
        // SAFETY: slot indices < sp; base < cap after ensure.
        unsafe {
            match DISPATCH[rule] {
                0 => {
                    let mut yyval = YYSTYPE::None;
                    self.reduce(stk.action_view(base), rule, &mut yyval, yyloc)?;
                    self.ensure_stack(stk, base + 1)?;
                    stk.write_val(base, yyval, yyloc);
                }
                255 => {
                    self.ensure_stack(stk, base + 1)?;
                    stk.write_val(base, YYSTYPE::None, yyloc);
                }
                // Constant arms can come from empty productions (base == sp).
                254 => {
                    self.ensure_stack(stk, base + 1)?;
                    stk.write_val(base, YYSTYPE::List(NodeList::nil()), yyloc);
                }
                253 => {
                    self.ensure_stack(stk, base + 1)?;
                    stk.write_val(base, YYSTYPE::Node(None), yyloc);
                }
                252 => {
                    self.ensure_stack(stk, base + 1)?;
                    stk.write_val(base, YYSTYPE::Alias(None), yyloc);
                }
                251 => {
                    self.ensure_stack(stk, base + 1)?;
                    stk.write_val(base, YYSTYPE::Limit(None), yyloc);
                }
                n => {
                    // `$$ = $n`: for n == 1 the value already sits at `base`.
                    if n > 1 {
                        let v = stk.take_val(base + n as usize - 1);
                        stk.write_val(base, v, yyloc);
                    } else {
                        stk.write_loc(base, yyloc);
                    }
                }
            }
        }

        let lhs = YYR1[rule] as i32 - YYNTOKENS;
        // SAFETY: state slots 0..=base are live; base + 1 <= cap.
        let state = unsafe {
            let top = stk.state(base) as i32;
            let g = YYPGOTO[lhs as usize] as i32 + top;
            let state = if (0..=YYLAST).contains(&g) && YYCHECK[g as usize] as i32 == top {
                YYTABLE[g as usize] as i32
            } else {
                YYDEFGOTO[lhs as usize] as i32
            };
            stk.write_state(base + 1, state as i16);
            state
        };
        *sp = base + 1;
        Ok(state)
    }

    // scanner_yyerror: "at or near" quotes the raw text from the error
    // position up to C's hold-char NUL. The hold char always sits at the
    // byte just past the MOST RECENTLY SCANNED core token — which is the
    // live scanner's current position, NOT the end of the failing token:
    // when base_yylex consumed lookahead (the USCONST/UIDENT + UESCAPE +
    // SCONST composite, or the WITH/NULLS/NOT peeks), C's tail spans the
    // whole consumed run (`at or near "U&'a' UESCAPE '-'"`; found by
    // gram_core_diff fleet run 2026-08-01 after directed U& seeding).
    #[cold]
    pub(crate) fn syntax_error(&self, message: &str, yylloc: i32) -> Box<PgError> {
        let loc = (yylloc.max(0) as usize).min(self.scanbuf.len());
        let hold = if self.have_lookahead {
            self.cur_tok_end
        } else {
            self.scanner.tok_end()
        };
        let end = hold.clamp(loc, self.scanbuf.len());
        let tail = &self.scanbuf[loc..end];
        let tail = &tail[..tail.iter().position(|&b| b == 0).unwrap_or(tail.len())];
        let err = if tail.is_empty() {
            PgError::error(format!("{message} at end of input"))
        } else {
            PgError::error(format!(
                "{message} at or near \"{}\"",
                String::from_utf8_lossy(tail)
            ))
        };
        Box::new(
            err.with_sqlstate(ERRCODE_SYNTAX_ERROR)
                .with_cursor_position(parser_small1::parser_errposition_source(
                    Some(self.scanbuf),
                    yylloc,
                    self.settings.encoding,
                )),
        )
    }

}

// Bit-identical repack (tag values aligned across the two 16B carriers).
fn yystype_from(v: CoreYYSTYPE<'_>) -> YYSTYPE<'_> {
    match v.get() {
        CoreVal::None => YYSTYPE::None,
        CoreVal::Ival(i) => YYSTYPE::Ival(i),
        CoreVal::Str(bytes) => {
            // Input is &str and the scanner verifies escape-built literals,
            // so values are valid UTF-8 while the server encoding is UTF-8.
            debug_assert!(core::str::from_utf8(bytes).is_ok());
            // SAFETY: see above.
            YYSTYPE::Str(unsafe { core::str::from_utf8_unchecked(bytes) })
        }
        CoreVal::Keyword(kw) => YYSTYPE::Keyword(kw),
    }
}
