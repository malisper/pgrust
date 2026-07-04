// pl_scanner.c. The core lexer is scan_fgram::Scanner; C hands it PL/pgSQL's
// reserved keyword list, so internal_yylex reclassifies: reserved-PL words
// (from IDENT or core-keyword tokens) become K_*, any other unquoted core
// keyword demotes to IDENT (C's core-with-PL-list returns IDENT for those).
use mcx::Mcx;
use scan_fgram::{tokens, CoreVal, CoreYYSTYPE, Scanner, ScannerSettings};
use types_error::{PgError, PgResult};

pub use tokens::{
    COLON_EQUALS, EQUALS_GREATER, FCONST, ICONST, IDENT, Op, PARAM, SCONST, TYPECAST, UIDENT,
};

pub const T_WORD: i32 = 275;
pub const T_CWORD: i32 = 276;
pub const T_DATUM: i32 = 277;
pub const LESS_LESS: i32 = 278;
pub const GREATER_GREATER: i32 = 279;

pub const K_ABSOLUTE: i32 = 280;
pub const K_ALIAS: i32 = 281;
pub const K_ALL: i32 = 282;
pub const K_AND: i32 = 283;
pub const K_ARRAY: i32 = 284;
pub const K_ASSERT: i32 = 285;
pub const K_BACKWARD: i32 = 286;
pub const K_BEGIN: i32 = 287;
pub const K_BY: i32 = 288;
pub const K_CALL: i32 = 289;
pub const K_CASE: i32 = 290;
pub const K_CHAIN: i32 = 291;
pub const K_CLOSE: i32 = 292;
pub const K_COLLATE: i32 = 293;
pub const K_COLUMN: i32 = 294;
pub const K_COLUMN_NAME: i32 = 295;
pub const K_COMMIT: i32 = 296;
pub const K_CONSTANT: i32 = 297;
pub const K_CONSTRAINT: i32 = 298;
pub const K_CONSTRAINT_NAME: i32 = 299;
pub const K_CONTINUE: i32 = 300;
pub const K_CURRENT: i32 = 301;
pub const K_CURSOR: i32 = 302;
pub const K_DATATYPE: i32 = 303;
pub const K_DEBUG: i32 = 304;
pub const K_DECLARE: i32 = 305;
pub const K_DEFAULT: i32 = 306;
pub const K_DETAIL: i32 = 307;
pub const K_DIAGNOSTICS: i32 = 308;
pub const K_DO: i32 = 309;
pub const K_DUMP: i32 = 310;
pub const K_ELSE: i32 = 311;
pub const K_ELSIF: i32 = 312;
pub const K_END: i32 = 313;
pub const K_ERRCODE: i32 = 314;
pub const K_ERROR: i32 = 315;
pub const K_EXCEPTION: i32 = 316;
pub const K_EXECUTE: i32 = 317;
pub const K_EXIT: i32 = 318;
pub const K_FETCH: i32 = 319;
pub const K_FIRST: i32 = 320;
pub const K_FOR: i32 = 321;
pub const K_FOREACH: i32 = 322;
pub const K_FORWARD: i32 = 323;
pub const K_FROM: i32 = 324;
pub const K_GET: i32 = 325;
pub const K_HINT: i32 = 326;
pub const K_IF: i32 = 327;
pub const K_IMPORT: i32 = 328;
pub const K_IN: i32 = 329;
pub const K_INFO: i32 = 330;
pub const K_INSERT: i32 = 331;
pub const K_INTO: i32 = 332;
pub const K_IS: i32 = 333;
pub const K_LAST: i32 = 334;
pub const K_LOG: i32 = 335;
pub const K_LOOP: i32 = 336;
pub const K_MERGE: i32 = 337;
pub const K_MESSAGE: i32 = 338;
pub const K_MESSAGE_TEXT: i32 = 339;
pub const K_MOVE: i32 = 340;
pub const K_NEXT: i32 = 341;
pub const K_NO: i32 = 342;
pub const K_NOT: i32 = 343;
pub const K_NOTICE: i32 = 344;
pub const K_NULL: i32 = 345;
pub const K_OPEN: i32 = 346;
pub const K_OPTION: i32 = 347;
pub const K_OR: i32 = 348;
pub const K_PERFORM: i32 = 349;
pub const K_PG_CONTEXT: i32 = 350;
pub const K_PG_DATATYPE_NAME: i32 = 351;
pub const K_PG_EXCEPTION_CONTEXT: i32 = 352;
pub const K_PG_EXCEPTION_DETAIL: i32 = 353;
pub const K_PG_EXCEPTION_HINT: i32 = 354;
pub const K_PG_ROUTINE_OID: i32 = 355;
pub const K_PRINT_STRICT_PARAMS: i32 = 356;
pub const K_PRIOR: i32 = 357;
pub const K_QUERY: i32 = 358;
pub const K_RAISE: i32 = 359;
pub const K_RELATIVE: i32 = 360;
pub const K_RETURN: i32 = 361;
pub const K_RETURNED_SQLSTATE: i32 = 362;
pub const K_REVERSE: i32 = 363;
pub const K_ROLLBACK: i32 = 364;
pub const K_ROW_COUNT: i32 = 365;
pub const K_ROWTYPE: i32 = 366;
pub const K_SCHEMA: i32 = 367;
pub const K_SCHEMA_NAME: i32 = 368;
pub const K_SCROLL: i32 = 369;
pub const K_SLICE: i32 = 370;
pub const K_SQLSTATE: i32 = 371;
pub const K_STACKED: i32 = 372;
pub const K_STRICT: i32 = 373;
pub const K_TABLE: i32 = 374;
pub const K_TABLE_NAME: i32 = 375;
pub const K_THEN: i32 = 376;
pub const K_TO: i32 = 377;
pub const K_TYPE: i32 = 378;
pub const K_USE_COLUMN: i32 = 379;
pub const K_USE_VARIABLE: i32 = 380;
pub const K_USING: i32 = 381;
pub const K_VARIABLE_CONFLICT: i32 = 382;
pub const K_WARNING: i32 = 383;
pub const K_WHEN: i32 = 384;
pub const K_WHILE: i32 = 385;

// pl_reserved_kwlist.h, ASCII order.
pub static RESERVED_PL_KEYWORDS: &[(&str, i32)] = &[
    ("all", K_ALL),
    ("begin", K_BEGIN),
    ("by", K_BY),
    ("case", K_CASE),
    ("declare", K_DECLARE),
    ("else", K_ELSE),
    ("end", K_END),
    ("execute", K_EXECUTE),
    ("for", K_FOR),
    ("foreach", K_FOREACH),
    ("from", K_FROM),
    ("if", K_IF),
    ("in", K_IN),
    ("into", K_INTO),
    ("loop", K_LOOP),
    ("not", K_NOT),
    ("null", K_NULL),
    ("or", K_OR),
    ("strict", K_STRICT),
    ("then", K_THEN),
    ("to", K_TO),
    ("using", K_USING),
    ("when", K_WHEN),
    ("while", K_WHILE),
];

// pl_unreserved_kwlist.h, ASCII order.
pub static UNRESERVED_PL_KEYWORDS: &[(&str, i32)] = &[
    ("absolute", K_ABSOLUTE),
    ("alias", K_ALIAS),
    ("and", K_AND),
    ("array", K_ARRAY),
    ("assert", K_ASSERT),
    ("backward", K_BACKWARD),
    ("call", K_CALL),
    ("chain", K_CHAIN),
    ("close", K_CLOSE),
    ("collate", K_COLLATE),
    ("column", K_COLUMN),
    ("column_name", K_COLUMN_NAME),
    ("commit", K_COMMIT),
    ("constant", K_CONSTANT),
    ("constraint", K_CONSTRAINT),
    ("constraint_name", K_CONSTRAINT_NAME),
    ("continue", K_CONTINUE),
    ("current", K_CURRENT),
    ("cursor", K_CURSOR),
    ("datatype", K_DATATYPE),
    ("debug", K_DEBUG),
    ("default", K_DEFAULT),
    ("detail", K_DETAIL),
    ("diagnostics", K_DIAGNOSTICS),
    ("do", K_DO),
    ("dump", K_DUMP),
    ("elseif", K_ELSIF),
    ("elsif", K_ELSIF),
    ("errcode", K_ERRCODE),
    ("error", K_ERROR),
    ("exception", K_EXCEPTION),
    ("exit", K_EXIT),
    ("fetch", K_FETCH),
    ("first", K_FIRST),
    ("forward", K_FORWARD),
    ("get", K_GET),
    ("hint", K_HINT),
    ("import", K_IMPORT),
    ("info", K_INFO),
    ("insert", K_INSERT),
    ("is", K_IS),
    ("last", K_LAST),
    ("log", K_LOG),
    ("merge", K_MERGE),
    ("message", K_MESSAGE),
    ("message_text", K_MESSAGE_TEXT),
    ("move", K_MOVE),
    ("next", K_NEXT),
    ("no", K_NO),
    ("notice", K_NOTICE),
    ("open", K_OPEN),
    ("option", K_OPTION),
    ("perform", K_PERFORM),
    ("pg_context", K_PG_CONTEXT),
    ("pg_datatype_name", K_PG_DATATYPE_NAME),
    ("pg_exception_context", K_PG_EXCEPTION_CONTEXT),
    ("pg_exception_detail", K_PG_EXCEPTION_DETAIL),
    ("pg_exception_hint", K_PG_EXCEPTION_HINT),
    ("pg_routine_oid", K_PG_ROUTINE_OID),
    ("print_strict_params", K_PRINT_STRICT_PARAMS),
    ("prior", K_PRIOR),
    ("query", K_QUERY),
    ("raise", K_RAISE),
    ("relative", K_RELATIVE),
    ("return", K_RETURN),
    ("returned_sqlstate", K_RETURNED_SQLSTATE),
    ("reverse", K_REVERSE),
    ("rollback", K_ROLLBACK),
    ("row_count", K_ROW_COUNT),
    ("rowtype", K_ROWTYPE),
    ("schema", K_SCHEMA),
    ("schema_name", K_SCHEMA_NAME),
    ("scroll", K_SCROLL),
    ("slice", K_SLICE),
    ("sqlstate", K_SQLSTATE),
    ("stacked", K_STACKED),
    ("table", K_TABLE),
    ("table_name", K_TABLE_NAME),
    ("type", K_TYPE),
    ("use_column", K_USE_COLUMN),
    ("use_variable", K_USE_VARIABLE),
    ("variable_conflict", K_VARIABLE_CONFLICT),
    ("warning", K_WARNING),
];

pub fn scan_keyword_lookup(s: &str, keywords: &[(&'static str, i32)]) -> Option<i32> {
    if s.len() > 20 {
        return None;
    }
    let mut buf = [0u8; 20];
    for (i, &b) in s.as_bytes().iter().enumerate() {
        buf[i] = if b.is_ascii_uppercase() { b + (b'a' - b'A') } else { b };
    }
    let lower = &buf[..s.len()];
    keywords
        .iter()
        .find(|(kw, _)| kw.as_bytes() == lower)
        .map(|&(_, tok)| tok)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IdentifierLookup {
    /// Normal processing of var names (IDENTIFIER_LOOKUP_NORMAL).
    Normal,
    /// Do not lookup identifiers as variables (IDENTIFIER_LOOKUP_DECLARE).
    Declare,
    /// Lookup, but T_DATUM only for known vars (IDENTIFIER_LOOKUP_EXPR).
    Expr,
}

#[derive(Debug, Clone, Default)]
pub struct PLword {
    pub ident: String,
    pub quoted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PLcword {
    pub idents: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PLwdatum {
    pub dno: i32,
    pub ident: String,
    pub quoted: bool,
    pub idents: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Yystype {
    pub str_: Option<String>,
    pub ival: i32,
    pub keyword: Option<&'static str>,
    pub word: Option<PLword>,
    pub cword: Option<PLcword>,
    pub wdatum: Option<PLwdatum>,
}

pub enum WordRes {
    Datum(PLwdatum),
    Word(PLword),
}

pub enum CwordRes {
    Datum(PLwdatum),
    Cword(PLcword),
}

pub trait WordResolver {
    fn parse_word(&mut self, word: &str, yytxt: &str, lookup: bool) -> PgResult<WordRes>;
    fn parse_dblword(&mut self, a: &str, b: &str) -> PgResult<CwordRes>;
    fn parse_tripword(&mut self, a: &str, b: &str, c: &str) -> PgResult<CwordRes>;
    fn identifier_lookup(&self) -> IdentifierLookup;
}

const MAX_PUSHBACKS: usize = 4;

#[derive(Debug, Clone, Default)]
struct TokenAux {
    lval: Yystype,
    lloc: i32,
    leng: i32,
}

fn at_stmt_start(prev_token: i32) -> bool {
    prev_token == (';' as i32)
        || prev_token == K_BEGIN
        || prev_token == K_THEN
        || prev_token == K_ELSE
        || prev_token == K_LOOP
}

fn is_identifier_word(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && b.iter().all(|&c| c.is_ascii_alphanumeric() || c == b'_' || c == b'$')
}

pub struct PlScanner<'mcx> {
    core: Scanner<'mcx>,
    scanbuf: &'mcx [u8],
    pub yyleng: i32,
    yytoken: i32,
    num_pushbacks: usize,
    pushback_token: [i32; MAX_PUSHBACKS],
    pushback_aux: [TokenAux; MAX_PUSHBACKS],
    cur_line_start: usize,
    cur_line_end: Option<usize>,
    cur_line_num: i32,
}

impl<'mcx> PlScanner<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, scanbuf: &'mcx [u8]) -> PlScanner<'mcx> {
        PlScanner {
            core: Scanner::new(scanbuf, mcx, ScannerSettings::default()),
            scanbuf,
            yyleng: 0,
            yytoken: 0,
            num_pushbacks: 0,
            pushback_token: [0; MAX_PUSHBACKS],
            pushback_aux: Default::default(),
            cur_line_start: 0,
            cur_line_end: scanbuf.iter().position(|&c| c == b'\n'),
            cur_line_num: 1,
        }
    }

    pub fn scanbuf(&self) -> &'mcx [u8] {
        self.scanbuf
    }

    fn span_text(&self, start: i32, end: i32) -> &str {
        let s = start.max(0) as usize;
        let e = (end.max(start)) as usize;
        core::str::from_utf8(&self.scanbuf[s.min(self.scanbuf.len())..e.min(self.scanbuf.len())])
            .unwrap_or("")
    }

    fn internal_yylex(&mut self, aux: &mut TokenAux) -> PgResult<i32> {
        if self.num_pushbacks > 0 {
            self.num_pushbacks -= 1;
            *aux = self.pushback_aux[self.num_pushbacks].clone();
            return Ok(self.pushback_token[self.num_pushbacks]);
        }
        let mut lval = CoreYYSTYPE::None;
        let mut lloc: i32 = 0;
        let mut token = self.core.core_yylex(&mut lval, &mut lloc)?;
        let tok_end = self.core.tok_end() as i32;
        aux.lloc = lloc;
        aux.lval = Yystype::default();
        match lval.get() {
            CoreVal::Str(s) => {
                aux.lval.str_ = Some(String::from_utf8_lossy(s).into_owned());
            }
            CoreVal::Keyword(k) => {
                aux.lval.keyword = Some(k);
                aux.lval.str_ = Some(k.to_string());
            }
            CoreVal::Ival(v) => aux.lval.ival = v,
            CoreVal::None => {}
        }
        let yytext = self.span_text(lloc, tok_end).to_string();
        aux.leng = yytext.len() as i32;

        if token == Op {
            match aux.lval.str_.as_deref() {
                Some("<<") => token = LESS_LESS,
                Some(">>") => token = GREATER_GREATER,
                Some("#") => token = '#' as i32,
                _ => {}
            }
        } else if token == PARAM {
            aux.lval.str_ = Some(yytext.clone());
        }

        let quoted = yytext.as_bytes().first() == Some(&b'"');
        if !quoted {
            if let Some(k) = scan_keyword_lookup(&yytext, RESERVED_PL_KEYWORDS) {
                token = k;
            } else if token != IDENT
                && token != UIDENT
                && token != PARAM
                && is_identifier_word(&yytext)
            {
                // A core-SQL keyword that is not PL-reserved: C's reserved-only
                // core keyword list would have returned plain IDENT.
                token = IDENT;
                if aux.lval.str_.is_none() {
                    aux.lval.str_ = Some(yytext.to_ascii_lowercase());
                }
            }
        }
        Ok(token)
    }

    fn push_back(&mut self, token: i32, aux: &TokenAux) -> PgResult<()> {
        if self.num_pushbacks >= MAX_PUSHBACKS {
            return Err(PgError::error("too many tokens pushed back".to_string()).into());
        }
        self.pushback_token[self.num_pushbacks] = token;
        self.pushback_aux[self.num_pushbacks] = aux.clone();
        self.num_pushbacks += 1;
        Ok(())
    }

    pub fn push_back_token(
        &mut self,
        token: i32,
        lval: &Yystype,
        lloc: i32,
        leng: i32,
    ) -> PgResult<()> {
        let aux = TokenAux { lval: lval.clone(), lloc, leng };
        self.push_back(token, &aux)
    }

    fn finish_dblword(&mut self, aux1: &mut TokenAux, aux3: &TokenAux, res: CwordRes) -> i32 {
        let tok = match res {
            CwordRes::Datum(w) => {
                aux1.lval.wdatum = Some(w);
                T_DATUM
            }
            CwordRes::Cword(c) => {
                aux1.lval.cword = Some(c);
                T_CWORD
            }
        };
        aux1.leng = aux3.lloc - aux1.lloc + aux3.leng;
        tok
    }

    fn finish_word(&self, aux1: &mut TokenAux, res: WordRes) -> i32 {
        match res {
            WordRes::Datum(w) => {
                aux1.lval.wdatum = Some(w);
                T_DATUM
            }
            WordRes::Word(word) => {
                if !word.quoted {
                    if let Some(tok) = scan_keyword_lookup(&word.ident, UNRESERVED_PL_KEYWORDS) {
                        let canonical = UNRESERVED_PL_KEYWORDS
                            .iter()
                            .find(|&&(_, t)| t == tok)
                            .map(|&(kw, _)| kw)
                            .expect("token from this table");
                        aux1.lval.word = Some(word);
                        aux1.lval.keyword = Some(canonical);
                        return tok;
                    }
                }
                aux1.lval.word = Some(word);
                T_WORD
            }
        }
    }

    // plpgsql_yylex (pl_scanner.c); returns (token, lval, lloc, leng).
    pub fn yylex(
        &mut self,
        resolver: &mut dyn WordResolver,
    ) -> PgResult<(i32, Yystype, i32, i32)> {
        let mut aux1 = TokenAux::default();
        let mut tok1 = self.internal_yylex(&mut aux1)?;

        if tok1 == IDENT || tok1 == PARAM {
            let mut aux2 = TokenAux::default();
            let tok2 = self.internal_yylex(&mut aux2)?;
            if tok2 == ('.' as i32) {
                let mut aux3 = TokenAux::default();
                let tok3 = self.internal_yylex(&mut aux3)?;
                if tok3 == IDENT {
                    let mut aux4 = TokenAux::default();
                    let tok4 = self.internal_yylex(&mut aux4)?;
                    if tok4 == ('.' as i32) {
                        let mut aux5 = TokenAux::default();
                        let tok5 = self.internal_yylex(&mut aux5)?;
                        if tok5 == IDENT {
                            let res = resolver.parse_tripword(
                                aux1.lval.str_.as_deref().unwrap_or(""),
                                aux3.lval.str_.as_deref().unwrap_or(""),
                                aux5.lval.str_.as_deref().unwrap_or(""),
                            )?;
                            tok1 = self.finish_dblword(&mut aux1, &aux5, res);
                        } else {
                            self.push_back(tok5, &aux5)?;
                            self.push_back(tok4, &aux4)?;
                            let res = resolver.parse_dblword(
                                aux1.lval.str_.as_deref().unwrap_or(""),
                                aux3.lval.str_.as_deref().unwrap_or(""),
                            )?;
                            tok1 = self.finish_dblword(&mut aux1, &aux3, res);
                        }
                    } else {
                        self.push_back(tok4, &aux4)?;
                        let res = resolver.parse_dblword(
                            aux1.lval.str_.as_deref().unwrap_or(""),
                            aux3.lval.str_.as_deref().unwrap_or(""),
                        )?;
                        tok1 = self.finish_dblword(&mut aux1, &aux3, res);
                    }
                } else {
                    self.push_back(tok3, &aux3)?;
                    self.push_back(tok2, &aux2)?;
                    let yytxt = self.span_text(aux1.lloc, aux1.lloc + aux1.leng).to_string();
                    let res = resolver.parse_word(
                        aux1.lval.str_.as_deref().unwrap_or(""),
                        &yytxt,
                        true,
                    )?;
                    tok1 = self.finish_word(&mut aux1, res);
                }
            } else {
                self.push_back(tok2, &aux2)?;
                let lookup = !at_stmt_start(self.yytoken)
                    || tok2 == ('=' as i32)
                    || tok2 == COLON_EQUALS
                    || tok2 == ('[' as i32);
                let yytxt = self.span_text(aux1.lloc, aux1.lloc + aux1.leng).to_string();
                let res = resolver.parse_word(
                    aux1.lval.str_.as_deref().unwrap_or(""),
                    &yytxt,
                    lookup,
                )?;
                tok1 = self.finish_word(&mut aux1, res);
            }
        }

        self.yyleng = aux1.leng;
        self.yytoken = tok1;
        Ok((tok1, aux1.lval, aux1.lloc, aux1.leng))
    }

    /// plpgsql_token_length.
    pub fn token_length(&self) -> i32 {
        self.yyleng
    }

    fn location_lineno_init(&mut self) {
        self.cur_line_start = 0;
        self.cur_line_num = 1;
        self.cur_line_end = self.scanbuf.iter().position(|&c| c == b'\n');
    }

    /// plpgsql_location_to_lineno: incremental walk; updates the "latest" line.
    pub fn lineno_for(&mut self, location: i32) -> i32 {
        if location < 0 || location as usize > self.scanbuf.len() {
            return 0;
        }
        let loc = location as usize;
        if loc < self.cur_line_start {
            self.location_lineno_init();
        }
        while let Some(end) = self.cur_line_end {
            if loc <= end {
                break;
            }
            self.cur_line_start = end + 1;
            self.cur_line_num += 1;
            self.cur_line_end = self.scanbuf[self.cur_line_start..]
                .iter()
                .position(|&c| c == b'\n')
                .map(|i| self.cur_line_start + i);
        }
        self.cur_line_num
    }

    /// plpgsql_latest_lineno: line of the most recently computed location.
    pub fn latest_lineno(&self) -> i32 {
        self.cur_line_num
    }

    /// plpgsql_scanner_errposition (pl_scanner.c): 1-based char position.
    pub fn errposition(&self, location: i32) -> i32 {
        parser_small1::parser_errposition_source(
            Some(self.scanbuf),
            location,
            wchar::PG_UTF8,
        )
    }

    /// plpgsql_yyerror: "syntax error at or near ..." with position.
    pub fn syntax_error(&self, message: &str, lloc: i32) -> Box<PgError> {
        let end = self.scanbuf.len() as i32;
        let mut e = lloc;
        while (e as usize) < self.scanbuf.len()
            && !self.scanbuf[e as usize].is_ascii_whitespace()
            && e < lloc + 32
        {
            e += 1;
        }
        let near = self.span_text(lloc, e.min(end));
        Box::new(
            elog::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("{message} at or near \"{near}\""))
                .errposition(self.errposition(lloc))
                .into_error(),
        )
    }
}
