use core::mem;

use mcx::Mcx;
use scan_fgram::{tokens, CoreYYSTYPE, Scanner, ScannerSettings, Token};
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
            last_yylloc: 0,
            parsetree: NodeList::nil(),
        })
    }

    // parser.c base_yylex: the one-token-lookahead merge filter keeping the
    // grammar LALR(1) (FORMAT/NOT/NULLS/WITH/WITHOUT + UIDENT/USCONST merges).
    fn base_yylex(&mut self) -> PgResult<(i32, YYSTYPE<'mcx>, i32)> {
        let (cur_tok, cur_val, cur_loc) = if self.have_lookahead {
            self.have_lookahead = false;
            (self.la_tok, mem::take(&mut self.la_val), self.la_loc)
        } else {
            self.next_token()?
        };

        self.last_yylloc = cur_loc;
        match cur_tok {
            t if t == tokens::FORMAT
                || t == tokens::NOT
                || t == tokens::NULLS_P
                || t == tokens::WITH
                || t == tokens::WITHOUT => {}
            t if t == tokens::UIDENT || t == tokens::USCONST => {
                panic!(
                    "gram_core: UIDENT/USCONST merge (parser.c base_yylex UESCAPE \
                     + str_udeescape) not ported"
                )
            }
            _ => return Ok((cur_tok, cur_val, cur_loc)),
        }

        let (next_tok, next_val, next_loc) = self.next_token()?;
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
            t => t,
        };
        Ok((merged, cur_val, cur_loc))
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

    fn next_token(&mut self) -> PgResult<(i32, YYSTYPE<'mcx>, i32)> {
        let tok = self.scanner.core_yylex()?;
        Ok((tok.token, yystype_from(tok.value), tok.location))
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
                        let (t, v, l) = self.base_yylex()?;
                        yychar = t;
                        yylval = v;
                        yylloc = l;
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
                        stk.ensure(mcx, sp + 1)?;
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
                    stk.set_sp(*sp);
                    let mut yyval = YYSTYPE::None;
                    self.reduce(stk, rule, yylen, &mut yyval, yyloc)?;
                    stk.ensure(self.mcx, base + 1)?;
                    stk.write_val(base, yyval, yyloc);
                }
                255 => {
                    stk.ensure(self.mcx, base + 1)?;
                    stk.write_val(base, YYSTYPE::None, yyloc);
                }
                // Constant arms can come from empty productions (base == sp).
                254 => {
                    stk.ensure(self.mcx, base + 1)?;
                    stk.write_val(base, YYSTYPE::List(NodeList::nil()), yyloc);
                }
                253 => {
                    stk.ensure(self.mcx, base + 1)?;
                    stk.write_val(base, YYSTYPE::Node(None), yyloc);
                }
                252 => {
                    stk.ensure(self.mcx, base + 1)?;
                    stk.write_val(base, YYSTYPE::Alias(None), yyloc);
                }
                251 => {
                    stk.ensure(self.mcx, base + 1)?;
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

    // scanner_yyerror: "at or near" quotes the failing token's raw text up
    // to C's hold-char NUL = the match end (recovered by token_extent).
    #[cold]
    pub(crate) fn syntax_error(&self, message: &str, yylloc: i32) -> Box<PgError> {
        let loc = (yylloc.max(0) as usize).min(self.scanbuf.len());
        let end = self.token_extent(loc);
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

    // End of the token starting at `loc`: minimal prefix of scanbuf[loc..]
    // whose first token equals the full-input token (the scanner is in INITIAL
    // at parser token boundaries, so a fresh scan reproduces the match; the
    // predicate is monotone between the match end and the next token's start).
    fn token_extent(&self, loc: usize) -> usize {
        let sub = &self.scanbuf[loc..];
        let mut s = Scanner::new(sub, self.mcx, self.settings);
        let Ok(reference) = s.core_yylex() else {
            return self.scanbuf.len();
        };
        if reference.token == YYEOF {
            return loc;
        }
        let ub = match s.core_yylex() {
            Ok(t2) if t2.token != YYEOF => t2.location as usize,
            _ => sub.len(),
        };
        let (mut lo, mut hi) = (1usize, ub);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.first_token_eq(&sub[..mid], &reference) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        loc + lo
    }

    fn first_token_eq(&self, prefix: &'mcx [u8], reference: &Token<'mcx>) -> bool {
        let mut s = Scanner::new(prefix, self.mcx, self.settings);
        match s.core_yylex() {
            Ok(t) => {
                t.token == reference.token
                    && t.location == reference.location
                    && t.value == reference.value
            }
            Err(_) => false,
        }
    }
}

fn yystype_from(v: CoreYYSTYPE<'_>) -> YYSTYPE<'_> {
    match v {
        CoreYYSTYPE::None => YYSTYPE::None,
        CoreYYSTYPE::Ival(i) => YYSTYPE::Ival(i),
        CoreYYSTYPE::Str(bytes) => {
            // Input is &str and the scanner verifies escape-built literals,
            // so values are valid UTF-8 while the server encoding is UTF-8.
            debug_assert!(core::str::from_utf8(bytes).is_ok());
            // SAFETY: see above.
            YYSTYPE::Str(unsafe { core::str::from_utf8_unchecked(bytes) })
        }
        CoreYYSTYPE::Keyword(kw) => YYSTYPE::Keyword(kw),
    }
}
