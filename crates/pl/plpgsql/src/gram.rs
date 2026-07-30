// pl_gram.y as recursive descent (keyword-led statements; the bison
// grammar's pushback tricks map 1:1). Named louds: CASE, FOREACH,
// FOR ... IN EXECUTE, OPEN/FETCH/MOVE/CLOSE, DO, COMMIT/ROLLBACK,
// RETURN NEXT/QUERY, cursor declarations, %ROWTYPE, qualified %TYPE,
// #option dump.
use parser_seams::RawParseMode;
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERROR};

use crate::ast::*;
use crate::comp::CompState;
use crate::errcodes::EXCEPTION_LABEL_MAP;
use crate::scanner::*;

const INT4OID: Oid = 23;
const VOIDOID: Oid = 2278;
const REFCURSOROID: Oid = 1790;
const PROKIND_PROCEDURE: i8 = b'p' as i8;
const CURSOR_OPT_SCROLL: i32 = 0x0002;
const CURSOR_OPT_NO_SCROLL: i32 = 0x0004;
const CURSOR_OPT_FAST_PLAN: i32 = 0x0100;

pub const LABEL_BLOCK: i32 = 0;
pub const LABEL_LOOP: i32 = 1;

// elog levels (elog.h) used by RAISE.
pub const DEBUG1: i32 = 14;
pub const LOG: i32 = 15;
pub const INFO: i32 = 17;
pub const NOTICE: i32 = 18;
pub const WARNING: i32 = 19;
pub const ELOG_ERROR: i32 = 21;

const DOT_DOT: i32 = 269;

pub struct Parser<'a, 'mcx> {
    pub sc: PlScanner<'mcx>,
    pub comp: &'a mut CompState,
    pub check_syntax: bool,
    pub fn_rettype: Oid,
    pub fn_retset: bool,
    pub fn_prokind: i8,
    pub fn_input_collation: Oid,
    pub fn_is_trigger: bool,
    pub out_param_varno: Dno,
    pub scratch: mcx::Mcx<'mcx>,
    // Location of the until-token that ended the last read_sql_construct
    // (C leaves it in *yyllocp; read_cursor_args reports errors there,
    // pl_gram.y:4022-4035).
    pub last_endtoken_loc: i32,
}

type Tok = (i32, Yystype, i32, i32);

impl<'a, 'mcx> Parser<'a, 'mcx> {
    fn yylex(&mut self) -> PgResult<Tok> {
        self.sc.yylex(self.comp)
    }

    fn push_back(&mut self, t: &Tok) -> PgResult<()> {
        self.sc.push_back_token(t.0, &t.1, t.2, t.3)
    }

    fn peek(&mut self) -> PgResult<i32> {
        let t = self.yylex()?;
        let tok = t.0;
        self.push_back(&t)?;
        Ok(tok)
    }

    fn lineno(&mut self, loc: i32) -> i32 {
        self.sc.lineno_for(loc)
    }

    fn source_span(&self, start: i32, end: i32) -> String {
        let buf = self.sc.scanbuf();
        let s = (start.max(0) as usize).min(buf.len());
        let e = (end.max(start) as usize).min(buf.len());
        String::from_utf8_lossy(&buf[s..e]).into_owned()
    }

    fn yyerror(&self, message: &str, lloc: i32) -> Box<PgError> {
        self.sc.syntax_error(message, lloc)
    }

    #[track_caller]
    #[cold]
    fn gram_err(&self, code: types_error::SqlState, msg: String) -> Box<PgError> {
        Box::new(elog::ereport(ERROR).errcode(code).errmsg(msg).into_error())
    }

    #[track_caller]
    #[cold]
    fn gram_err_pos(
        &self,
        code: types_error::SqlState,
        msg: String,
        location: i32,
    ) -> Box<PgError> {
        Box::new(
            elog::ereport(ERROR)
                .errcode(code)
                .errmsg(msg)
                .errposition(self.sc.errposition(location))
                .into_error(),
        )
    }

    fn expect(&mut self, tok: i32, expected_msg: &str) -> PgResult<Tok> {
        let t = self.yylex()?;
        if t.0 != tok {
            return Err(self.yyerror(expected_msg, t.2));
        }
        Ok(t)
    }

    // opt_transaction_chain (pl_gram.y): [AND [NO] CHAIN] ';'.
    fn parse_opt_transaction_chain(&mut self) -> PgResult<bool> {
        let t = self.yylex()?;
        let chain = if Self::tok_is_keyword(&t, K_AND, "and") {
            let t2 = self.yylex()?;
            if Self::tok_is_keyword(&t2, K_NO, "no") {
                self.expect(K_CHAIN, "syntax error")?;
                false
            } else {
                self.push_back(&t2)?;
                self.expect(K_CHAIN, "syntax error")?;
                true
            }
        } else {
            self.push_back(&t)?;
            false
        };
        self.expect(';' as i32, "syntax error")?;
        Ok(chain)
    }

    fn tok_is_keyword(t: &Tok, kw_token: i32, kw_str: &str) -> bool {
        if t.0 == kw_token {
            return true;
        }
        if t.0 == T_DATUM {
            if let Some(w) = &t.1.wdatum {
                return !w.quoted && !w.ident.is_empty() && w.ident == kw_str;
            }
        }
        false
    }

    fn unreserved_keyword_name(t: &Tok) -> Option<&'static str> {
        if (K_ABSOLUTE..=K_WHILE).contains(&t.0)
            && !RESERVED_PL_KEYWORDS.iter().any(|&(_, k)| k == t.0)
        {
            return t.1.keyword;
        }
        None
    }

    fn token_is_unreserved_keyword(t: &Tok) -> bool {
        Self::unreserved_keyword_name(t).is_some()
    }

    fn word_is_not_variable(&self, ident: &str, loc: i32) -> Box<PgError> {
        self.gram_err_pos(
            ERRCODE_SYNTAX_ERROR,
            format!("\"{ident}\" is not a known variable"),
            loc,
        )
    }

    fn current_token_is_not_variable(&self, t: &Tok) -> Box<PgError> {
        if t.0 == T_WORD {
            let ident = t.1.word.as_ref().map(|w| w.ident.as_str()).unwrap_or("");
            self.word_is_not_variable(ident, t.2)
        } else if t.0 == T_CWORD {
            let name = t.1.cword.as_ref().map(|c| c.idents.join(".")).unwrap_or_default();
            self.word_is_not_variable(&name, t.2)
        } else {
            self.yyerror("syntax error", t.2)
        }
    }

    fn make_expr(&mut self, query: String, mode: RawParseMode, ns: i32) -> PlExpr {
        PlExpr {
            query,
            parse_mode: mode,
            ns,
            expr_id: self.comp.new_expr_id(),
            target_param: -1,
        }
    }

    // check_sql_expr: raw-parse the saved text early so statement-boundary
    // confusion surfaces at CREATE time (only when check_function_bodies).
    fn check_sql_expr(&self, query: &str, mode: RawParseMode, location: i32) -> PgResult<()> {
        if !self.check_syntax {
            return Ok(());
        }
        // plpgsql_sql_error_callback runs for every elevel: warnings emitted
        // by the raw parse (scanner escape warnings) get the same
        // expr-to-function-source cursor remap as the Err path below.
        let base = self.sc.errposition(location);
        let cb = elog::push_emit_context_callback(Box::new(move |e| {
            if let Some(p) = e.cursor_position.filter(|&p| p > 0) {
                e.cursor_position = Some(base + p - 1);
            }
        }));
        let r = parser_seams::raw_parser::call(self.scratch, query, mode);
        elog::pop_emit_context_callback(cb);
        match r {
            Ok(_) => Ok(()),
            Err(mut e) => {
                // plpgsql_sql_error_callback: expr-relative cursor becomes a
                // function-source cursor (both 1-based char positions).
                if let Some(p) = e.cursor_position.filter(|&p| p > 0) {
                    e.cursor_position = Some(base + p - 1);
                }
                Err(e)
            }
        }
    }

    // read_sql_construct (pl_gram.y).
    #[allow(clippy::too_many_arguments)]
    fn read_sql_construct(
        &mut self,
        until: i32,
        until2: i32,
        until3: i32,
        expected: &str,
        parsemode: RawParseMode,
        isexpression: bool,
        valid_sql: bool,
        startloc_out: Option<&mut i32>,
        endtoken_out: Option<&mut i32>,
    ) -> PgResult<PlExpr> {
        let save = self.comp.identifier_lookup;
        self.comp.identifier_lookup = IdentifierLookup::Expr;
        let mut startlocation = -1i32;
        let mut endlocation = -1i32;
        let mut parenlevel = 0i32;
        let tok_final;
        loop {
            let t = self.yylex()?;
            let (tok, _, lloc) = (t.0, &t.1, t.2);
            if startlocation < 0 {
                startlocation = lloc;
            }
            if parenlevel == 0 && (tok == until || (until2 != 0 && tok == until2) || (until3 != 0 && tok == until3)) {
                tok_final = tok;
                self.last_endtoken_loc = lloc;
                break;
            }
            if tok == ('(' as i32) || tok == ('[' as i32) {
                parenlevel += 1;
            } else if tok == (')' as i32) || tok == (']' as i32) {
                parenlevel -= 1;
                if parenlevel < 0 {
                    return Err(self.yyerror("mismatched parentheses", lloc));
                }
            }
            if tok == 0 || tok == (';' as i32) {
                if parenlevel != 0 {
                    return Err(self.yyerror("mismatched parentheses", lloc));
                }
                let what = if isexpression { "expression" } else { "statement" };
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!("missing \"{expected}\" at end of SQL {what}"),
                    lloc,
                ));
            }
            endlocation = lloc + self.sc.token_length();
        }
        self.comp.identifier_lookup = save;
        if let Some(s) = startloc_out {
            *s = startlocation;
        }
        if let Some(e) = endtoken_out {
            *e = tok_final;
        }
        if startlocation >= endlocation {
            let msg = if isexpression { "missing expression" } else { "missing SQL statement" };
            return Err(self.yyerror(msg, startlocation));
        }
        let text = self.source_span(startlocation, endlocation);
        let expr = self.make_expr(text, parsemode, self.comp.ns_top);
        if valid_sql {
            self.check_sql_expr(&expr.query, expr.parse_mode, startlocation)?;
        }
        Ok(expr)
    }

    fn read_sql_expression(&mut self, until: i32, expected: &str) -> PgResult<PlExpr> {
        self.read_sql_construct(
            until,
            0,
            0,
            expected,
            RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
            true,
            true,
            None,
            None,
        )
    }

    fn read_sql_expression2(
        &mut self,
        until: i32,
        until2: i32,
        expected: &str,
        endtoken: &mut i32,
    ) -> PgResult<PlExpr> {
        self.read_sql_construct(
            until,
            until2,
            0,
            expected,
            RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
            true,
            true,
            None,
            Some(endtoken),
        )
    }

    // parse_datatype (pl_gram.y) via parseTypeString; declared datatypes
    // carry the function's input collation (pl_gram.y parse_datatype).
    fn parse_datatype(&mut self, type_name: &str, _location: i32) -> PgResult<PlType> {
        let (typoid, typmod) = parse_utilcmd::parseTypeString(self.scratch, type_name)?;
        CompState::build_datatype(typoid, typmod, self.fn_input_collation)
    }

    // read_datatype (pl_gram.y); the lookahead token is passed in.
    fn read_datatype(&mut self, tok_in: Option<Tok>) -> PgResult<PlType> {
        let mut t = match tok_in {
            Some(t) => t,
            None => self.yylex()?,
        };
        let startlocation = t.2;
        let mut result: Option<PlType> = None;

        let dtname: Option<String> = if t.0 == T_WORD {
            t.1.word.as_ref().map(|w| w.ident.clone())
        } else if Self::token_is_unreserved_keyword(&t) {
            Self::unreserved_keyword_name(&t).map(|s| s.to_string())
        } else {
            None
        };
        if let Some(name) = dtname {
            let t2 = self.yylex()?;
            if t2.0 == ('%' as i32) {
                let t3 = self.yylex()?;
                if Self::tok_is_keyword(&t3, K_TYPE, "type") {
                    result = Some(self.comp.parse_wordtype(&name)?);
                } else if Self::tok_is_keyword(&t3, K_ROWTYPE, "rowtype") {
                    result = Some(self.comp.parse_wordrowtype(&name)?);
                } else {
                    self.push_back(&t3)?;
                    self.push_back(&t2)?;
                }
            } else {
                self.push_back(&t2)?;
            }
        } else if t.0 == T_CWORD {
            let idents = t.1.cword.as_ref().map(|c| c.idents.clone()).unwrap_or_default();
            let t2 = self.yylex()?;
            if t2.0 == ('%' as i32) {
                let t3 = self.yylex()?;
                if Self::tok_is_keyword(&t3, K_TYPE, "type") {
                    result = Some(self.comp.parse_cwordtype(&idents)?);
                } else if Self::tok_is_keyword(&t3, K_ROWTYPE, "rowtype") {
                    result = Some(self.comp.parse_cwordrowtype(&idents)?);
                } else {
                    self.push_back(&t3)?;
                    self.push_back(&t2)?;
                }
            } else {
                self.push_back(&t2)?;
            }
        }

        if let Some(mut ty) = result {
            // Optional array decoration after %TYPE.
            let mut is_array = false;
            let mut t = self.yylex()?;
            if Self::tok_is_keyword(&t, K_ARRAY, "array") {
                is_array = true;
                t = self.yylex()?;
            }
            while t.0 == ('[' as i32) {
                is_array = true;
                t = self.yylex()?;
                if t.0 == ICONST {
                    t = self.yylex()?;
                }
                if t.0 != (']' as i32) {
                    return Err(self.yyerror("syntax error, expected \"]\"", t.2));
                }
                t = self.yylex()?;
            }
            self.push_back(&t)?;
            // pl_comp.c:2094-2095: already-array types pass through unchanged.
            if is_array && !ty.typisarray {
                let arr = lsyscache::typ::get_array_type(ty.typoid)?;
                if !OidIsValid(arr) {
                    return Err(self.gram_err(
                        types_error::ERRCODE_UNDEFINED_OBJECT,
                        format!(
                            "could not find array type for data type {}",
                            format_type::format_type_be(ty.typoid)?
                        ),
                    ));
                }
                ty = CompState::build_datatype(arr, ty.atttypmod, ty.collation)?;
            }
            return Ok(ty);
        }

        let mut parenlevel = 0;
        let mut endloc;
        loop {
            endloc = t.2;
            if t.0 == 0 {
                if parenlevel != 0 {
                    return Err(self.yyerror("mismatched parentheses", t.2));
                }
                return Err(self.yyerror("incomplete data type declaration", t.2));
            }
            if t.0 == K_COLLATE
                || t.0 == K_NOT
                || t.0 == ('=' as i32)
                || t.0 == COLON_EQUALS
                || t.0 == K_DEFAULT
                || t.0 == (';' as i32)
            {
                break;
            }
            if (t.0 == (',' as i32) || t.0 == (')' as i32)) && parenlevel == 0 {
                break;
            }
            if t.0 == ('(' as i32) {
                parenlevel += 1;
            } else if t.0 == (')' as i32) {
                parenlevel -= 1;
            }
            t = self.yylex()?;
        }
        let type_name = self.source_span(startlocation, endloc);
        let type_name = type_name.trim_end();
        if type_name.is_empty() {
            return Err(self.yyerror("missing data type declaration", t.2));
        }
        let result = self.parse_datatype(type_name, startlocation)?;
        self.push_back(&t)?;
        Ok(result)
    }

    // pl_function: comp_options pl_block opt_semi (entry point).
    pub fn parse_function_body(&mut self) -> PgResult<PlBlock> {
        self.parse_comp_options()?;
        let t = self.yylex()?;
        let label = if t.0 == LESS_LESS {
            let (label, _) = self.any_identifier()?;
            self.expect(GREATER_GREATER, "syntax error")?;
            Some(label)
        } else {
            self.push_back(&t)?;
            None
        };
        let block = self.parse_block_after_label(label, None)?;
        // opt_semi, then EOF.
        let t = self.yylex()?;
        if t.0 == (';' as i32) {
            let t2 = self.yylex()?;
            if t2.0 != 0 {
                return Err(self.yyerror("syntax error", t2.2));
            }
        } else if t.0 != 0 {
            return Err(self.yyerror("syntax error", t.2));
        }
        Ok(block)
    }

    fn parse_comp_options(&mut self) -> PgResult<()> {
        loop {
            let t = self.yylex()?;
            if t.0 != ('#' as i32) {
                self.push_back(&t)?;
                return Ok(());
            }
            let opt = self.yylex()?;
            if opt.0 == K_OPTION {
                let v = self.yylex()?;
                if v.0 == K_DUMP {
                    panic!(
                        "comp_option '#option dump' (pl_gram.y): plpgsql_DumpExecTree \
                         unported — unit backend-pl-plpgsql-gram"
                    );
                }
                return Err(self.yyerror("syntax error", v.2));
            } else if opt.0 == K_PRINT_STRICT_PARAMS {
                // pl_gram.y:389-397: option_value is T_WORD | unreserved_keyword;
                // "on"/"off" set the flag, anything else is elog(ERROR).
                let v = self.yylex()?;
                let val: Option<String> = if v.0 == T_WORD {
                    v.1.word.as_ref().map(|w| w.ident.clone())
                } else {
                    Self::unreserved_keyword_name(&v).map(|s| s.to_string())
                };
                let Some(val) = val else {
                    return Err(self.yyerror("syntax error", v.2));
                };
                if val == "on" {
                    self.comp.print_strict_params = true;
                } else if val == "off" {
                    self.comp.print_strict_params = false;
                } else {
                    return Err(self.gram_err(
                        types_error::ERRCODE_INTERNAL_ERROR,
                        format!("unrecognized print_strict_params option {val}"),
                    ));
                }
            } else if opt.0 == K_VARIABLE_CONFLICT {
                let v = self.yylex()?;
                if Self::tok_is_keyword(&v, K_ERROR, "error") {
                    self.comp.resolve_option = crate::comp::PLPGSQL_RESOLVE_ERROR;
                } else if Self::tok_is_keyword(&v, K_USE_VARIABLE, "use_variable") {
                    self.comp.resolve_option = crate::comp::PLPGSQL_RESOLVE_VARIABLE;
                } else if Self::tok_is_keyword(&v, K_USE_COLUMN, "use_column") {
                    self.comp.resolve_option = crate::comp::PLPGSQL_RESOLVE_COLUMN;
                } else {
                    return Err(self.yyerror("unrecognized option", v.2));
                }
            } else {
                return Err(self.yyerror("unrecognized option", opt.2));
            }
        }
    }

    fn any_identifier(&mut self) -> PgResult<(String, i32)> {
        let t = self.yylex()?;
        if t.0 == T_WORD {
            return Ok((t.1.word.as_ref().expect("T_WORD").ident.clone(), t.2));
        }
        if let Some(kw) = Self::unreserved_keyword_name(&t) {
            return Ok((kw.to_string(), t.2));
        }
        if t.0 == T_DATUM {
            let w = t.1.wdatum.as_ref().expect("T_DATUM");
            if w.ident.is_empty() {
                return Err(self.yyerror("syntax error", t.2));
            }
            return Ok((w.ident.clone(), t.2));
        }
        Err(self.yyerror("syntax error", t.2))
    }

    fn check_labels(&self, start: Option<&str>, end: Option<&str>, end_loc: i32) -> PgResult<()> {
        if let Some(el) = end {
            match start {
                None => Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!("end label \"{el}\" specified for unlabeled block"),
                    end_loc,
                )),
                Some(sl) if sl != el => Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!("end label \"{el}\" differs from block's label \"{sl}\""),
                    end_loc,
                )),
                _ => Ok(()),
            }
        } else {
            Ok(())
        }
    }

    // opt_label after END/END LOOP.
    fn opt_label(&mut self) -> PgResult<(Option<String>, i32)> {
        let t = self.yylex()?;
        let loc = t.2;
        match t.0 {
            T_WORD => Ok((Some(t.1.word.as_ref().expect("T_WORD").ident.clone()), loc)),
            T_DATUM => {
                let w = t.1.wdatum.as_ref().expect("T_DATUM");
                if w.ident.is_empty() {
                    return Err(self.yyerror("syntax error", loc));
                }
                Ok((Some(w.ident.clone()), loc))
            }
            _ => {
                if let Some(kw) = Self::unreserved_keyword_name(&t) {
                    Ok((Some(kw.to_string()), loc))
                } else {
                    self.push_back(&t)?;
                    Ok((None, loc))
                }
            }
        }
    }

    // pl_block after the opt_block_label has pushed the ns level. `label` is
    // the block label; entry token must be K_DECLARE or K_BEGIN (consumed
    // here).
    fn parse_block_after_label(
        &mut self,
        label: Option<String>,
        _label_loc: Option<i32>,
    ) -> PgResult<PlBlock> {
        self.comp.ns_push_label(label.as_deref(), LABEL_BLOCK);

        let mut initvarnos: Vec<Dno> = Vec::new();
        let mut t = self.yylex()?;
        let mut begin_loc = t.2;
        if t.0 == K_DECLARE {
            self.comp.add_initdatums();
            self.comp.identifier_lookup = IdentifierLookup::Declare;
            loop {
                let dt = self.yylex()?;
                match dt.0 {
                    K_BEGIN => {
                        begin_loc = dt.2;
                        break;
                    }
                    K_DECLARE => continue,
                    LESS_LESS => {
                        return Err(self.gram_err(
                            ERRCODE_SYNTAX_ERROR,
                            "block label must be placed before DECLARE, not after".to_string(),
                        ));
                    }
                    _ => {
                        self.push_back(&dt)?;
                        self.parse_decl_statement()?;
                    }
                }
            }
            initvarnos = self.comp.add_initdatums();
            self.comp.identifier_lookup = IdentifierLookup::Normal;
        } else if t.0 != K_BEGIN {
            return Err(self.yyerror("syntax error", t.2));
        }

        let body = self.parse_proc_sect(&[K_END, K_EXCEPTION])?;
        t = self.yylex()?;
        let exceptions = if t.0 == K_EXCEPTION {
            let exc = self.parse_exception_sect(t.2)?;
            self.expect(K_END, "syntax error")?;
            Some(exc)
        } else {
            debug_assert_eq!(t.0, K_END);
            None
        };
        let (end_label, end_loc) = self.opt_label()?;
        self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
        self.comp.ns_pop();
        self.comp.nstatements += 1;
        Ok(PlBlock {
            lineno: self.lineno(begin_loc),
            label,
            body,
            initvarnos,
            exceptions,
        })
    }

    // exception_sect (pl_gram.y): sqlstate/sqlerrm scoped to the block's
    // handlers, then WHEN proc_conditions THEN proc_sect list.
    fn parse_exception_sect(&mut self, exc_loc: i32) -> PgResult<ExceptionBlock> {
        const TEXTOID: Oid = 25;
        let lineno = self.lineno(exc_loc);
        let coll = self.fn_input_collation;
        let sqlstate_varno = self.comp.build_variable(
            "sqlstate",
            lineno,
            CompState::build_datatype(TEXTOID, -1, coll)?,
            true,
        )?;
        let sqlerrm_varno = self.comp.build_variable(
            "sqlerrm",
            lineno,
            CompState::build_datatype(TEXTOID, -1, coll)?,
            true,
        )?;
        for dno in [sqlstate_varno, sqlerrm_varno] {
            if let PlDatum::Var(v) = &mut self.comp.datums[dno as usize] {
                v.isconst = true;
            }
        }

        let mut exc_list = Vec::new();
        loop {
            let wt = self.expect(K_WHEN, "syntax error")?;
            let mut conditions = self.parse_proc_condition()?;
            loop {
                let t = self.yylex()?;
                if t.0 == K_OR {
                    conditions.extend(self.parse_proc_condition()?);
                } else {
                    self.push_back(&t)?;
                    break;
                }
            }
            self.expect(K_THEN, "syntax error")?;
            let action = self.parse_proc_sect(&[K_WHEN, K_END])?;
            exc_list.push(PlException {
                lineno: self.lineno(wt.2),
                conditions,
                action,
            });
            if self.peek()? != K_WHEN {
                break;
            }
        }
        Ok(ExceptionBlock { sqlstate_varno, sqlerrm_varno, exc_list })
    }

    // proc_condition (pl_gram.y): one name may map to several SQLSTATEs.
    fn parse_proc_condition(&mut self) -> PgResult<Vec<PlCondition>> {
        let (name, _loc) = self.any_identifier()?;
        if name == "sqlstate" {
            let s = self.yylex()?;
            if s.0 != SCONST {
                return Err(self.yyerror("syntax error", s.2));
            }
            let code = s.1.str_.clone().unwrap_or_default();
            if code.len() != 5
                || !code.bytes().all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
            {
                return Err(self.yyerror("invalid SQLSTATE code", s.2));
            }
            let b = code.as_bytes();
            return Ok(vec![PlCondition {
                sqlerrstate: types_error::make_sqlstate([b[0], b[1], b[2], b[3], b[4]]),
                condname: code,
            }]);
        }
        // plpgsql_parse_err_condition (pl_comp.c): OTHERS is grammar-special;
        // otherwise every plerrcodes entry with the name chains in.
        if name == "others" {
            return Ok(vec![PlCondition {
                sqlerrstate: types_error::SqlState(0),
                condname: name,
            }]);
        }
        let mut out = Vec::new();
        for &(n, code) in EXCEPTION_LABEL_MAP {
            if n == name {
                out.push(PlCondition {
                    sqlerrstate: types_error::make_sqlstate(code),
                    condname: name.clone(),
                });
            }
        }
        if out.is_empty() {
            return Err(self.gram_err(
                types_error::ERRCODE_UNDEFINED_OBJECT,
                format!("unrecognized exception condition \"{name}\""),
            ));
        }
        Ok(out)
    }

    // decl_statement.
    fn parse_decl_statement(&mut self) -> PgResult<()> {
        let (name, name_loc) = self.decl_varname()?;
        // decl_varname action (pl_gram.y:716): lineno computed before the
        // datatype is read, so %TYPE compile errors report this line.
        let lineno = self.lineno(name_loc);

        // ALIAS / CURSOR forms peek after the name.
        let t = self.yylex()?;
        if Self::tok_is_keyword(&t, K_ALIAS, "alias") {
            self.expect(K_FOR, "syntax error")?;
            let (itemtype, itemno) = self.decl_aliasitem()?;
            self.expect(';' as i32, "syntax error")?;
            self.comp.ns_additem(itemtype, itemno, &name);
            return Ok(());
        }
        if Self::tok_is_keyword(&t, K_CURSOR, "cursor")
            || Self::tok_is_keyword(&t, K_SCROLL, "scroll")
            || Self::tok_is_keyword(&t, K_NO, "no")
        {
            return self.parse_decl_cursor(&name, lineno, t);
        }
        let isconst = if Self::tok_is_keyword(&t, K_CONSTANT, "constant") {
            true
        } else {
            self.push_back(&t)?;
            false
        };

        let mut datatype = self.read_datatype(None)?;

        // decl_collate (pl_gram.y:789-806) + the decl_statement collation
        // insertion (pl_gram.y:516-526); errposition is the COLLATE keyword.
        let t = self.yylex()?;
        if t.0 == K_COLLATE {
            let collate_loc = t.2;
            let ct = self.yylex()?;
            let names: Vec<String> = if ct.0 == T_WORD {
                vec![ct.1.word.as_ref().expect("T_WORD").ident.clone()]
            } else if ct.0 == T_CWORD {
                ct.1.cword.as_ref().expect("T_CWORD").idents.clone()
            } else if let Some(kw) = Self::unreserved_keyword_name(&ct) {
                vec![kw.to_string()]
            } else {
                return Err(self.yyerror("syntax error", ct.2));
            };
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let collation = catalog_namespace::get_collation_oid(&refs, false)?;
            if !types_core::OidIsValid(datatype.collation) {
                return Err(self.gram_err_pos(
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    format!(
                        "collations are not supported by type {}",
                        format_type::format_type_be(datatype.typoid)?
                    ),
                    collate_loc,
                ));
            }
            datatype.collation = collation;
        } else {
            self.push_back(&t)?;
        }

        // decl_notnull.
        let mut notnull = false;
        let mut notnull_loc = name_loc;
        let t = self.yylex()?;
        if t.0 == K_NOT {
            notnull_loc = t.2;
            self.expect(K_NULL, "syntax error")?;
            notnull = true;
        } else {
            self.push_back(&t)?;
        }

        // decl_defval.
        let t = self.yylex()?;
        let default_val = if t.0 == (';' as i32) {
            None
        } else if t.0 == ('=' as i32) || t.0 == COLON_EQUALS || t.0 == K_DEFAULT {
            Some(self.read_sql_expression(';' as i32, ";")?)
        } else {
            return Err(self.yyerror("syntax error", t.2));
        };

        let dno = self.comp.build_variable(&name, lineno, datatype, true)?;
        if notnull && default_val.is_none() {
            return Err(self.gram_err_pos(
                types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                format!(
                    "variable \"{name}\" must have a default value, since it's declared NOT NULL"
                ),
                notnull_loc,
            ));
        }
        if let PlDatum::Var(v) = &mut self.comp.datums[dno as usize] {
            v.isconst = isconst;
            v.notnull = notnull;
            if let Some(mut e) = default_val {
                e.target_param = dno;
                v.default_val = Some(e);
            }
        }
        Ok(())
    }

    // decl_statement cursor arm (pl_gram.y:553-577) + opt_scrollable +
    // decl_cursor_args + decl_cursor_query.
    fn parse_decl_cursor(&mut self, name: &str, lineno: i32, t: Tok) -> PgResult<()> {
        let mut cursor_options = CURSOR_OPT_FAST_PLAN;
        let mut t = t;
        if Self::tok_is_keyword(&t, K_NO, "no") {
            self.expect(K_SCROLL, "syntax error")?;
            cursor_options |= CURSOR_OPT_NO_SCROLL;
            t = self.yylex()?;
        } else if Self::tok_is_keyword(&t, K_SCROLL, "scroll") {
            cursor_options |= CURSOR_OPT_SCROLL;
            t = self.yylex()?;
        }
        if !Self::tok_is_keyword(&t, K_CURSOR, "cursor") {
            return Err(self.yyerror("syntax error", t.2));
        }

        self.comp.ns_push_label(Some(name), LABEL_BLOCK);
        let t = self.yylex()?;
        let argrow: Dno = if t.0 == ('(' as i32) {
            let arg_loc = t.2;
            self.comp.identifier_lookup = IdentifierLookup::Declare;
            let mut varnos = Vec::new();
            loop {
                let (argname, arg_name_loc) = self.decl_varname()?;
                let arg_lineno = self.lineno(arg_name_loc);
                let datatype = self.read_datatype(None)?;
                let dno = self.comp.build_variable(&argname, arg_lineno, datatype, true)?;
                varnos.push(dno);
                let sep = self.yylex()?;
                if sep.0 == (',' as i32) {
                    continue;
                }
                if sep.0 == (')' as i32) {
                    break;
                }
                return Err(self.yyerror("syntax error", sep.2));
            }
            self.comp.identifier_lookup = IdentifierLookup::Normal;
            let row_lineno = self.lineno(arg_loc);
            self.comp.build_row("(unnamed row)", row_lineno, varnos)
        } else {
            self.push_back(&t)?;
            -1
        };

        // decl_is_for.
        let t = self.yylex()?;
        if t.0 != K_FOR && !Self::tok_is_keyword(&t, K_IS, "is") {
            return Err(self.yyerror("syntax error", t.2));
        }
        let query = self.read_sql_construct(
            ';' as i32,
            0,
            0,
            ";",
            RawParseMode::RAW_PARSE_DEFAULT,
            false,
            true,
            None,
            None,
        )?;
        self.comp.ns_pop();

        let dno = self.comp.build_variable(
            name,
            lineno,
            CompState::build_datatype(REFCURSOROID, -1, types_core::InvalidOid)?,
            true,
        )?;
        if let PlDatum::Var(v) = &mut self.comp.datums[dno as usize] {
            v.cursor_explicit_expr = Some(query);
            v.cursor_explicit_argrow = argrow;
            v.cursor_options = cursor_options;
        }
        Ok(())
    }

    fn decl_varname(&mut self) -> PgResult<(String, i32)> {
        let t = self.yylex()?;
        let (name, loc) = if t.0 == T_WORD {
            (t.1.word.as_ref().expect("T_WORD").ident.clone(), t.2)
        } else if let Some(kw) = Self::unreserved_keyword_name(&t) {
            (kw.to_string(), t.2)
        } else {
            return Err(self.yyerror("syntax error", t.2));
        };
        if self
            .comp
            .ns_lookup(self.comp.ns_top, true, &name, None, None)
            .is_some()
        {
            return Err(self.yyerror("duplicate declaration", loc));
        }
        // Shadowing check (pl_gram.y:726-738): DUPLICATE_ALIAS at ERROR when
        // extra_errors carries shadowed_variables, WARNING when only
        // extra_warnings does; both are validator-only (pl_comp.c:249-250).
        if (self.comp.extra_warnings | self.comp.extra_errors) & crate::comp::XCHECK_SHADOWVAR != 0
            && self.comp.ns_lookup(self.comp.ns_top, false, &name, None, None).is_some()
        {
            let is_error = self.comp.extra_errors & crate::comp::XCHECK_SHADOWVAR != 0;
            let b = elog::ereport(if is_error { ERROR } else { types_error::WARNING })
                .errcode(types_error::ERRCODE_DUPLICATE_ALIAS)
                .errmsg(format!("variable \"{name}\" shadows a previously defined variable"))
                .errposition(self.sc.errposition(loc));
            if is_error {
                return Err(Box::new(b.into_error()));
            }
            // The warning's cursor transposes onto the CREATE statement via
            // the compile-scope emit callback (do_compile).
            b.finish(types_error::ErrorLocation::new(file!(), line!() as i32, "decl_varname"))?;
        }
        Ok((name, loc))
    }

    fn decl_aliasitem(&mut self) -> PgResult<(NsType, i32)> {
        let t = self.yylex()?;
        let nsi = if t.0 == T_WORD {
            let ident = &t.1.word.as_ref().expect("T_WORD").ident;
            self.comp.ns_lookup(self.comp.ns_top, false, ident, None, None).ok_or_else(|| {
                self.gram_err(
                    types_error::ERRCODE_UNDEFINED_OBJECT,
                    format!("variable \"{ident}\" does not exist"),
                )
            })?
        } else if let Some(kw) = Self::unreserved_keyword_name(&t) {
            self.comp.ns_lookup(self.comp.ns_top, false, kw, None, None).ok_or_else(|| {
                self.gram_err(
                    types_error::ERRCODE_UNDEFINED_OBJECT,
                    format!("variable \"{kw}\" does not exist"),
                )
            })?
        } else if t.0 == T_CWORD {
            let idents = &t.1.cword.as_ref().expect("T_CWORD").idents;
            let found = if idents.len() == 2 {
                self.comp.ns_lookup(self.comp.ns_top, false, &idents[0], Some(&idents[1]), None)
            } else if idents.len() == 3 {
                self.comp.ns_lookup(
                    self.comp.ns_top,
                    false,
                    &idents[0],
                    Some(&idents[1]),
                    Some(&idents[2]),
                )
            } else {
                None
            };
            found.ok_or_else(|| {
                self.gram_err(
                    types_error::ERRCODE_UNDEFINED_OBJECT,
                    format!("variable \"{}\" does not exist", idents.join(".")),
                )
            })?
        } else {
            return Err(self.yyerror("syntax error", t.2));
        };
        let item = &self.comp.ns[nsi.0 as usize];
        Ok((item.itemtype, item.itemno))
    }

    // proc_sect: statements until one of `stops` (stop token pushed back).
    fn parse_proc_sect(&mut self, stops: &[i32]) -> PgResult<Vec<PlStmt>> {
        let mut out = Vec::new();
        loop {
            let t = self.yylex()?;
            if t.0 == 0 {
                return Err(self.yyerror("unexpected end of function definition", t.2));
            }
            if stops.contains(&t.0) {
                self.push_back(&t)?;
                return Ok(out);
            }
            if let Some(stmt) = self.parse_statement(t)? {
                out.push(stmt);
            }
        }
    }

    fn parse_statement(&mut self, t: Tok) -> PgResult<Option<PlStmt>> {
        self.comp.nstatements += 1;
        let lloc = t.2;
        match t.0 {
            LESS_LESS => {
                let (label, _lloc) = self.any_identifier()?;
                self.expect(GREATER_GREATER, "syntax error")?;
                let nt = self.yylex()?;
                match nt.0 {
                    K_LOOP => Ok(Some(self.parse_loop(Some(label), nt.2)?)),
                    K_WHILE => Ok(Some(self.parse_while(Some(label), nt.2)?)),
                    K_FOR => Ok(Some(self.parse_for(Some(label), nt.2)?)),
                    K_FOREACH => Ok(Some(self.parse_foreach(Some(label), nt.2)?)),
                    K_DECLARE | K_BEGIN => {
                        self.push_back(&nt)?;
                        let b = self.parse_block_after_label(Some(label), Some(lloc))?;
                        self.expect(';' as i32, "syntax error")?;
                        Ok(Some(PlStmt::Block(b)))
                    }
                    _ => Err(self.yyerror("syntax error", nt.2)),
                }
            }
            K_DECLARE | K_BEGIN => {
                self.push_back(&t)?;
                let b = self.parse_block_after_label(None, None)?;
                self.expect(';' as i32, "syntax error")?;
                Ok(Some(PlStmt::Block(b)))
            }
            K_LOOP => Ok(Some(self.parse_loop(None, lloc)?)),
            K_WHILE => Ok(Some(self.parse_while(None, lloc)?)),
            K_FOR => Ok(Some(self.parse_for(None, lloc)?)),
            K_IF => Ok(Some(self.parse_if(lloc)?)),
            K_CASE => Ok(Some(self.parse_case(lloc)?)),
            K_FOREACH => Ok(Some(self.parse_foreach(None, lloc)?)),
            K_EXIT | K_CONTINUE => Ok(Some(self.parse_exit(t.0 == K_EXIT, lloc)?)),
            K_RETURN => Ok(Some(self.parse_return(lloc)?)),
            K_RAISE => Ok(Some(self.parse_raise(lloc)?)),
            K_ASSERT => Ok(Some(self.parse_assert(lloc)?)),
            K_GET => Ok(Some(self.parse_getdiag(lloc)?)),
            K_PERFORM => Ok(Some(self.parse_perform(t, lloc)?)),
            K_EXECUTE => Ok(Some(self.parse_dynexecute(lloc)?)),
            K_OPEN => Ok(Some(self.parse_open(lloc)?)),
            K_FETCH => Ok(Some(self.parse_fetch(lloc)?)),
            K_MOVE => Ok(Some(self.parse_move(lloc)?)),
            K_CLOSE => Ok(Some(self.parse_close(lloc)?)),
            K_CALL => {
                let lineno = self.lineno(lloc);
                self.push_back(&t)?;
                let expr = self.read_sql_construct(
                    ';' as i32,
                    0,
                    0,
                    ";",
                    RawParseMode::RAW_PARSE_DEFAULT,
                    false,
                    true,
                    None,
                    None,
                )?;
                Ok(Some(PlStmt::Call { lineno, expr, is_call: true }))
            }
            K_DO => panic!(
                "stmt_call (pl_gram.y): DO unported — unit backend-pl-plpgsql-gram"
            ),
            K_COMMIT | K_ROLLBACK => {
                let lineno = self.lineno(lloc);
                let chain = self.parse_opt_transaction_chain()?;
                Ok(Some(if t.0 == K_COMMIT {
                    PlStmt::Commit { lineno, chain }
                } else {
                    PlStmt::Rollback { lineno, chain }
                }))
            }
            K_NULL => {
                self.expect(';' as i32, "syntax error")?;
                Ok(None)
            }
            T_DATUM => Ok(Some(self.parse_assign(t)?)),
            K_IMPORT | K_INSERT | K_MERGE => Ok(Some(self.make_execsql_stmt(t)?)),
            T_WORD => {
                let nt = self.yylex()?;
                let ntok = nt.0;
                self.push_back(&nt)?;
                if ntok == ('=' as i32)
                    || ntok == COLON_EQUALS
                    || ntok == ('[' as i32)
                    || ntok == ('.' as i32)
                {
                    let ident = t.1.word.as_ref().expect("T_WORD").ident.clone();
                    return Err(self.word_is_not_variable(&ident, lloc));
                }
                Ok(Some(self.make_execsql_stmt(t)?))
            }
            T_CWORD => {
                let nt = self.yylex()?;
                let ntok = nt.0;
                self.push_back(&nt)?;
                if ntok == ('=' as i32)
                    || ntok == COLON_EQUALS
                    || ntok == ('[' as i32)
                    || ntok == ('.' as i32)
                {
                    let name = t.1.cword.as_ref().expect("T_CWORD").idents.join(".");
                    return Err(self.word_is_not_variable(&name, lloc));
                }
                Ok(Some(self.make_execsql_stmt(t)?))
            }
            _ => Err(self.yyerror("syntax error", lloc)),
        }
    }

    // cursor_variable (pl_gram.y:2282): must be a refcursor-typed Var.
    fn cursor_variable(&mut self) -> PgResult<Dno> {
        let t = self.yylex()?;
        if t.0 == T_DATUM {
            let w = t.1.wdatum.as_ref().expect("T_DATUM");
            if let PlDatum::Var(v) = &self.comp.datums[w.dno as usize] {
                if v.datatype.typoid == REFCURSOROID {
                    return Ok(w.dno);
                }
                return Err(self.gram_err_pos(
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    format!("variable \"{}\" must be of type cursor or refcursor", v.refname),
                    t.2,
                ));
            }
            let name = self.comp.datums[t.1.wdatum.as_ref().unwrap().dno as usize]
                .refname()
                .to_string();
            return Err(self.gram_err_pos(
                types_error::ERRCODE_DATATYPE_MISMATCH,
                format!("variable \"{name}\" must be of type cursor or refcursor"),
                t.2,
            ));
        }
        Err(self.current_token_is_not_variable(&t))
    }

    // stmt_open (pl_gram.y:2100).
    fn parse_open(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let lineno = self.lineno(lloc);
        let curvar = self.cursor_variable()?;
        let mut cursor_options = CURSOR_OPT_FAST_PLAN;
        let mut argquery = None;
        let mut query = None;
        let mut dynquery = None;
        let mut params = Vec::new();

        let explicit = matches!(
            &self.comp.datums[curvar as usize],
            PlDatum::Var(v) if v.cursor_explicit_expr.is_some()
        );
        if !explicit {
            let mut t = self.yylex()?;
            if Self::tok_is_keyword(&t, K_NO, "no") {
                let t2 = self.yylex()?;
                if Self::tok_is_keyword(&t2, K_SCROLL, "scroll") {
                    cursor_options |= CURSOR_OPT_NO_SCROLL;
                    t = self.yylex()?;
                } else {
                    self.push_back(&t2)?;
                }
            } else if Self::tok_is_keyword(&t, K_SCROLL, "scroll") {
                cursor_options |= CURSOR_OPT_SCROLL;
                t = self.yylex()?;
            }
            if t.0 != K_FOR {
                return Err(self.yyerror("syntax error, expected \"FOR\"", t.2));
            }
            let t = self.yylex()?;
            if t.0 == K_EXECUTE {
                let mut endtoken = 0i32;
                dynquery =
                    Some(self.read_sql_expression2(K_USING, ';' as i32, "USING or ;", &mut endtoken)?);
                if endtoken == K_USING {
                    loop {
                        let mut term = 0i32;
                        let expr =
                            self.read_sql_expression2(',' as i32, ';' as i32, ", or ;", &mut term)?;
                        params.push(expr);
                        if term != (',' as i32) {
                            break;
                        }
                    }
                }
            } else {
                self.push_back(&t)?;
                query = Some(self.read_sql_construct(
                    ';' as i32,
                    0,
                    0,
                    ";",
                    RawParseMode::RAW_PARSE_DEFAULT,
                    false,
                    true,
                    None,
                    None,
                )?);
            }
        } else {
            argquery = self.read_cursor_args(curvar, ';' as i32)?;
        }

        Ok(PlStmt::Open {
            lineno,
            curvar,
            cursor_options,
            argquery,
            query,
            dynquery,
            params,
        })
    }

    // read_fetch_direction (pl_gram.y:3197); returns
    // (direction, how_many, expr, returns_multiple_rows).
    fn read_fetch_direction(&mut self) -> PgResult<(i32, i64, Option<PlExpr>, bool)> {
        let mut direction = FETCH_FORWARD;
        let mut how_many: i64 = 1;
        let mut expr: Option<PlExpr> = None;
        let mut multi = false;
        let mut check_from = true;

        let t = self.yylex()?;
        if t.0 == 0 {
            return Err(self.yyerror("unexpected end of function definition", t.2));
        }
        if Self::tok_is_keyword(&t, K_NEXT, "next") {
        } else if Self::tok_is_keyword(&t, K_PRIOR, "prior") {
            direction = FETCH_BACKWARD;
        } else if Self::tok_is_keyword(&t, K_FIRST, "first") {
            direction = FETCH_ABSOLUTE;
        } else if Self::tok_is_keyword(&t, K_LAST, "last") {
            direction = FETCH_ABSOLUTE;
            how_many = -1;
        } else if Self::tok_is_keyword(&t, K_ABSOLUTE, "absolute") {
            direction = FETCH_ABSOLUTE;
            let mut term = 0i32;
            expr = Some(self.read_sql_expression2(K_FROM, K_IN, "FROM or IN", &mut term)?);
            check_from = false;
        } else if Self::tok_is_keyword(&t, K_RELATIVE, "relative") {
            direction = FETCH_RELATIVE;
            let mut term = 0i32;
            expr = Some(self.read_sql_expression2(K_FROM, K_IN, "FROM or IN", &mut term)?);
            check_from = false;
        } else if Self::tok_is_keyword(&t, K_ALL, "all") {
            how_many = FETCH_ALL;
            multi = true;
        } else if Self::tok_is_keyword(&t, K_FORWARD, "forward") {
            self.complete_direction(&mut how_many, &mut expr, &mut multi, &mut check_from)?;
        } else if Self::tok_is_keyword(&t, K_BACKWARD, "backward") {
            direction = FETCH_BACKWARD;
            self.complete_direction(&mut how_many, &mut expr, &mut multi, &mut check_from)?;
        } else if t.0 == K_FROM || t.0 == K_IN {
            check_from = false;
        } else if t.0 == T_DATUM {
            self.push_back(&t)?;
            check_from = false;
        } else {
            self.push_back(&t)?;
            let mut term = 0i32;
            expr = Some(self.read_sql_expression2(K_FROM, K_IN, "FROM or IN", &mut term)?);
            multi = true;
            check_from = false;
        }

        if check_from {
            let t = self.yylex()?;
            if t.0 != K_FROM && t.0 != K_IN {
                return Err(self.yyerror("expected FROM or IN", t.2));
            }
        }
        Ok((direction, how_many, expr, multi))
    }

    // complete_direction (pl_gram.y:3324).
    fn complete_direction(
        &mut self,
        how_many: &mut i64,
        expr: &mut Option<PlExpr>,
        multi: &mut bool,
        check_from: &mut bool,
    ) -> PgResult<()> {
        let t = self.yylex()?;
        if t.0 == 0 {
            return Err(self.yyerror("unexpected end of function definition", t.2));
        }
        if t.0 == K_FROM || t.0 == K_IN {
            *check_from = false;
            return Ok(());
        }
        if t.0 == K_ALL {
            *how_many = FETCH_ALL;
            *multi = true;
            *check_from = true;
            return Ok(());
        }
        self.push_back(&t)?;
        let mut term = 0i32;
        *expr = Some(self.read_sql_expression2(K_FROM, K_IN, "FROM or IN", &mut term)?);
        *multi = true;
        *check_from = false;
        Ok(())
    }

    // stmt_fetch (pl_gram.y:2178).
    fn parse_fetch(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let (direction, how_many, expr, multi) = self.read_fetch_direction()?;
        let curvar = self.cursor_variable()?;
        self.expect(K_INTO, "syntax error")?;
        let (target, _strict) = self.read_into_target(false)?;
        let t = self.yylex()?;
        if t.0 != (';' as i32) {
            return Err(self.yyerror("syntax error", t.2));
        }
        if multi {
            return Err(self.gram_err_pos(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "FETCH statement cannot return multiple rows".to_string(),
                lloc,
            ));
        }
        Ok(PlStmt::Fetch {
            lineno: self.lineno(lloc),
            target,
            curvar,
            direction,
            how_many,
            expr,
            is_move: false,
            returns_multiple_rows: false,
        })
    }

    // stmt_move (pl_gram.y:2208).
    fn parse_move(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let (direction, how_many, expr, multi) = self.read_fetch_direction()?;
        let curvar = self.cursor_variable()?;
        self.expect(';' as i32, "syntax error")?;
        Ok(PlStmt::Fetch {
            lineno: self.lineno(lloc),
            target: -1,
            curvar,
            direction,
            how_many,
            expr,
            is_move: true,
            returns_multiple_rows: multi,
        })
    }

    // stmt_close (pl_gram.y:2226).
    fn parse_close(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let curvar = self.cursor_variable()?;
        self.expect(';' as i32, "syntax error")?;
        Ok(PlStmt::Close { lineno: self.lineno(lloc), curvar })
    }

    // read_cursor_args (pl_gram.y:3908): positional/named args re-serialized
    // as one SELECT-list expression in declared order.
    fn read_cursor_args(&mut self, curvar: Dno, until: i32) -> PgResult<Option<PlExpr>> {
        let (argrow, curname) = match &self.comp.datums[curvar as usize] {
            PlDatum::Var(v) => (v.cursor_explicit_argrow, v.refname.clone()),
            _ => unreachable!("cursor_variable checked"),
        };
        let t = self.yylex()?;
        if argrow < 0 {
            if t.0 == ('(' as i32) {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!("cursor \"{curname}\" has no arguments"),
                    t.2,
                ));
            }
            if t.0 != until {
                return Err(self.yyerror("syntax error", t.2));
            }
            return Ok(None);
        }
        if t.0 != ('(' as i32) {
            return Err(self.gram_err_pos(
                ERRCODE_SYNTAX_ERROR,
                format!("cursor \"{curname}\" has arguments"),
                t.2,
            ));
        }

        let fieldnames = match &self.comp.datums[argrow as usize] {
            PlDatum::Row(r) => r.fieldnames.clone(),
            _ => unreachable!("cursor argrow is a Row"),
        };
        let nfields = fieldnames.len();
        let mut argv: Vec<Option<String>> = vec![None; nfields];
        let mut any_named = false;

        for argc in 0..nfields {
            // Named notation: IDENT := expr / IDENT => expr.
            let mut argpos = argc;
            let t1 = self.yylex()?;
            let arglocation = t1.2;
            let named_candidate = t1.0 == T_WORD || t1.0 == T_DATUM || Self::token_is_unreserved_keyword(&t1);
            let argname: Option<String> = if named_candidate {
                let t2 = self.yylex()?;
                let is_named = t2.0 == COLON_EQUALS || t2.0 == EQUALS_GREATER;
                if is_named {
                    let name = if t1.0 == T_WORD {
                        t1.1.word.as_ref().expect("T_WORD").ident.clone()
                    } else if t1.0 == T_DATUM {
                        let w = t1.1.wdatum.as_ref().expect("T_DATUM");
                        if !w.ident.is_empty() { w.ident.clone() } else { w.idents.join(".") }
                    } else {
                        Self::unreserved_keyword_name(&t1).unwrap_or("").to_string()
                    };
                    Some(name)
                } else {
                    self.push_back(&t2)?;
                    self.push_back(&t1)?;
                    None
                }
            } else {
                self.push_back(&t1)?;
                None
            };
            if let Some(name) = argname {
                match fieldnames.iter().position(|f| *f == name) {
                    Some(p) => argpos = p,
                    None => {
                        return Err(self.gram_err_pos(
                            ERRCODE_SYNTAX_ERROR,
                            format!(
                                "cursor \"{curname}\" has no argument named \"{name}\""
                            ),
                            arglocation,
                        ));
                    }
                }
                any_named = true;
            }
            if argv[argpos].is_some() {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!(
                        "value for parameter \"{}\" of cursor \"{curname}\" specified more than once",
                        fieldnames[argpos]
                    ),
                    arglocation,
                ));
            }
            let mut endtoken = 0i32;
            let item = self.read_sql_construct(
                ',' as i32,
                ')' as i32,
                0,
                ",\" or \")",
                RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
                true,
                true,
                None,
                Some(&mut endtoken),
            )?;
            argv[argpos] = Some(item.query);
            if endtoken == (')' as i32) && argc != nfields - 1 {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!("not enough arguments for cursor \"{curname}\""),
                    self.last_endtoken_loc,
                ));
            }
            if endtoken == (',' as i32) && argc == nfields - 1 {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!("too many arguments for cursor \"{curname}\""),
                    self.last_endtoken_loc,
                ));
            }
        }

        let mut ds = String::new();
        for (i, a) in argv.iter().enumerate() {
            ds.push_str(a.as_ref().expect("all filled"));
            if any_named {
                ds.push_str(" AS ");
                ds.push_str(&format_type::quote_identifier(&fieldnames[i]));
            }
            if i < nfields - 1 {
                ds.push_str(", ");
            }
        }
        let expr = self.make_expr(ds, RawParseMode::RAW_PARSE_PLPGSQL_EXPR, self.comp.ns_top);

        let t = self.yylex()?;
        if t.0 != until {
            return Err(self.yyerror("syntax error", t.2));
        }
        Ok(Some(expr))
    }

    fn parse_assign(&mut self, t: Tok) -> PgResult<PlStmt> {
        let w = t.1.wdatum.clone().expect("T_DATUM");
        let nnames = if !w.ident.is_empty() { 1 } else { w.idents.len() };
        let pmode = match nnames {
            1 => RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN1,
            2 => RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN2,
            3 => RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN3,
            _ => return Err(self.yyerror("syntax error", t.2)),
        };
        self.check_assignable(w.dno, t.2)?;
        let lineno = self.lineno(t.2);
        let varno = w.dno;
        self.push_back(&t)?;
        let mut expr = self.read_sql_construct(
            ';' as i32,
            0,
            0,
            ";",
            pmode,
            false,
            true,
            None,
            None,
        )?;
        if matches!(self.comp.datums[varno as usize], PlDatum::Var(_)) {
            expr.target_param = varno;
        }
        Ok(PlStmt::Assign { lineno, varno, expr })
    }

    fn check_assignable(&self, dno: Dno, location: i32) -> PgResult<()> {
        match &self.comp.datums[dno as usize] {
            PlDatum::Var(v) => {
                if v.isconst {
                    return Err(self.gram_err_pos(
                        types_error::ERRCODE_ERROR_IN_ASSIGNMENT,
                        format!("variable \"{}\" is declared CONSTANT", v.refname),
                        location,
                    ));
                }
                Ok(())
            }
            PlDatum::Rec(_) | PlDatum::Row(_) => Ok(()),
            PlDatum::RecField(f) => self.check_assignable(f.recparentno, location),
        }
    }

    fn parse_if(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let cond = self.read_sql_expression(K_THEN, "THEN")?;
        let then_body = self.parse_proc_sect(&[K_ELSIF, K_ELSE, K_END])?;
        let mut elsifs = Vec::new();
        let mut else_body = None;
        loop {
            let t = self.yylex()?;
            match t.0 {
                K_ELSIF => {
                    let c = self.read_sql_expression(K_THEN, "THEN")?;
                    let b = self.parse_proc_sect(&[K_ELSIF, K_ELSE, K_END])?;
                    elsifs.push((c, b));
                }
                K_ELSE => {
                    else_body = Some(self.parse_proc_sect(&[K_END])?);
                }
                K_END => {
                    self.expect(K_IF, "syntax error")?;
                    self.expect(';' as i32, "syntax error")?;
                    break;
                }
                _ => return Err(self.yyerror("syntax error", t.2)),
            }
        }
        Ok(PlStmt::If {
            lineno: self.lineno(lloc),
            cond,
            then_body,
            elsifs,
            else_body,
        })
    }

    // loop_body: proc_sect K_END K_LOOP opt_label ';'
    fn parse_loop_body(&mut self) -> PgResult<(Vec<PlStmt>, Option<String>, i32)> {
        let stmts = self.parse_proc_sect(&[K_END])?;
        self.expect(K_END, "syntax error")?;
        self.expect(K_LOOP, "syntax error")?;
        let (end_label, end_loc) = self.opt_label()?;
        self.expect(';' as i32, "syntax error")?;
        Ok((stmts, end_label, end_loc))
    }

    fn parse_loop(&mut self, label: Option<String>, lloc: i32) -> PgResult<PlStmt> {
        self.comp.ns_push_label(label.as_deref(), LABEL_LOOP);
        let (body, end_label, end_loc) = self.parse_loop_body()?;
        self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
        self.comp.ns_pop();
        Ok(PlStmt::Loop {
            lineno: self.lineno(lloc),
            label,
            body,
        })
    }

    fn parse_while(&mut self, label: Option<String>, lloc: i32) -> PgResult<PlStmt> {
        self.comp.ns_push_label(label.as_deref(), LABEL_LOOP);
        let cond = self.read_sql_expression(K_LOOP, "LOOP")?;
        let (body, end_label, end_loc) = self.parse_loop_body()?;
        self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
        self.comp.ns_pop();
        Ok(PlStmt::While {
            lineno: self.lineno(lloc),
            label,
            cond,
            body,
        })
    }

    // for_variable: returns (name, lineno, scalar dno, row/rec dno).
    fn parse_for_variable(&mut self) -> PgResult<(String, i32, Option<Dno>, Option<Dno>, i32)> {
        let t = self.yylex()?;
        let loc = t.2;
        match t.0 {
            T_DATUM => {
                let w = t.1.wdatum.clone().expect("T_DATUM");
                let name = if !w.ident.is_empty() {
                    w.ident.clone()
                } else {
                    w.idents.last().cloned().unwrap_or_default()
                };
                let lineno = self.lineno(loc);
                match &self.comp.datums[w.dno as usize] {
                    PlDatum::Row(_) | PlDatum::Rec(_) => {
                        Ok((name, lineno, None, Some(w.dno), loc))
                    }
                    _ => {
                        let nt = self.yylex()?;
                        let is_comma = nt.0 == (',' as i32);
                        self.push_back(&nt)?;
                        if is_comma {
                            let row =
                                self.read_into_scalar_list(&name, w.dno, loc)?;
                            Ok((name, lineno, Some(w.dno), Some(row), loc))
                        } else {
                            Ok((name, lineno, Some(w.dno), None, loc))
                        }
                    }
                }
            }
            T_WORD => {
                let name = t.1.word.as_ref().expect("T_WORD").ident.clone();
                let lineno = self.lineno(loc);
                let nt = self.yylex()?;
                let is_comma = nt.0 == (',' as i32);
                self.push_back(&nt)?;
                if is_comma {
                    return Err(self.word_is_not_variable(&name, loc));
                }
                Ok((name, lineno, None, None, loc))
            }
            T_CWORD => {
                let name = t.1.cword.as_ref().expect("T_CWORD").idents.join(".");
                Err(self.word_is_not_variable(&name, loc))
            }
            _ => Err(self.current_token_is_not_variable(&t)),
        }
    }

    fn query_for_target(
        &mut self,
        name: &str,
        var_lineno: i32,
        var_loc: i32,
        scalar: Option<Dno>,
        rowrec: Option<Dno>,
    ) -> PgResult<Dno> {
        if let Some(r) = rowrec {
            self.check_assignable(r, var_loc)?;
            return Ok(r);
        }
        if let Some(s) = scalar {
            return self.make_scalar_list1(name, s, var_lineno, var_loc);
        }
        Err(self.gram_err_pos(
            types_error::ERRCODE_DATATYPE_MISMATCH,
            "loop variable of loop over rows must be a record variable or list of scalar variables"
                .to_string(),
            var_loc,
        ))
    }

    fn parse_for(&mut self, label: Option<String>, for_loc: i32) -> PgResult<PlStmt> {
        self.comp.ns_push_label(label.as_deref(), LABEL_LOOP);
        let (name, var_lineno, scalar, rowrec, var_loc) = self.parse_for_variable()?;
        self.expect(K_IN, "syntax error")?;

        let t = self.yylex()?;
        let tokloc = t.2;
        if t.0 == K_EXECUTE {
            let mut term = 0i32;
            let dynquery = self.read_sql_expression2(K_LOOP, K_USING, "LOOP", &mut term)?;
            let mut params = Vec::new();
            if term == K_USING {
                loop {
                    let mut t2 = 0i32;
                    let expr =
                        self.read_sql_expression2(',' as i32, K_LOOP, ", or LOOP", &mut t2)?;
                    params.push(expr);
                    if t2 != (',' as i32) {
                        break;
                    }
                }
            }
            let var = self.query_for_target(&name, var_lineno, var_loc, scalar, rowrec)?;
            let (body, end_label, end_loc) = self.parse_loop_body()?;
            self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
            self.comp.ns_pop();
            return Ok(PlStmt::DynForS {
                lineno: self.lineno(for_loc),
                label,
                var,
                query: dynquery,
                params,
                body,
            });
        }
        if t.0 == T_DATUM {
            let w = t.1.wdatum.clone().expect("T_DATUM");
            let is_cursor = matches!(
                &self.comp.datums[w.dno as usize],
                PlDatum::Var(v) if v.datatype.typoid == REFCURSOROID
            );
            if is_cursor {
                let curvar = w.dno;
                // C pl_gram.y: can't use an unbound cursor this way.
                if matches!(
                    &self.comp.datums[curvar as usize],
                    PlDatum::Var(v) if v.cursor_explicit_expr.is_none()
                ) {
                    return Err(self.gram_err_pos(
                        ERRCODE_SYNTAX_ERROR,
                        "cursor FOR loop must use a bound cursor variable".to_string(),
                        t.2,
                    ));
                }
                let var = self.comp.build_rec(&name, var_lineno, true);
                let argquery = self.read_cursor_args(curvar, K_LOOP)?;
                let (body, end_label, end_loc) = self.parse_loop_body()?;
                self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
                self.comp.ns_pop();
                return Ok(PlStmt::ForC {
                    lineno: self.lineno(for_loc),
                    label,
                    var,
                    curvar,
                    argquery,
                    body,
                });
            }
        }
        let mut reverse = false;
        if Self::tok_is_keyword(&t, K_REVERSE, "reverse") {
            reverse = true;
        } else {
            self.push_back(&t)?;
        }

        let mut expr1loc = -1i32;
        let mut term = 0i32;
        let mut expr1 = self.read_sql_construct(
            DOT_DOT,
            K_LOOP,
            0,
            "LOOP",
            RawParseMode::RAW_PARSE_DEFAULT,
            true,
            false,
            Some(&mut expr1loc),
            Some(&mut term),
        )?;

        if term == DOT_DOT {
            expr1.parse_mode = RawParseMode::RAW_PARSE_PLPGSQL_EXPR;
            self.check_sql_expr(&expr1.query, expr1.parse_mode, expr1loc)?;
            let mut term2 = 0i32;
            let expr2 = self.read_sql_expression2(K_LOOP, K_BY, "LOOP", &mut term2)?;
            let expr_by = if term2 == K_BY {
                Some(self.read_sql_expression(K_LOOP, "LOOP")?)
            } else {
                None
            };
            if scalar.is_some() && rowrec.is_some() {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    "integer FOR loop must have only one target variable".to_string(),
                    var_loc,
                ));
            }
            let fvar = self.comp.build_variable(
                &name,
                var_lineno,
                CompState::build_datatype(INT4OID, -1, types_core::InvalidOid)?,
                true,
            )?;
            let (body, end_label, end_loc) = self.parse_loop_body()?;
            self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
            self.comp.ns_pop();
            Ok(PlStmt::ForI {
                lineno: self.lineno(for_loc),
                label,
                var: fvar,
                lower: expr1,
                upper: expr2,
                step: expr_by,
                reverse,
                body,
            })
        } else {
            if reverse {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    "cannot specify REVERSE in query FOR loop".to_string(),
                    tokloc,
                ));
            }
            self.check_sql_expr(&expr1.query, expr1.parse_mode, expr1loc)?;
            let var = self.query_for_target(&name, var_lineno, var_loc, scalar, rowrec)?;
            let (body, end_label, end_loc) = self.parse_loop_body()?;
            self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
            self.comp.ns_pop();
            Ok(PlStmt::ForS {
                lineno: self.lineno(for_loc),
                label,
                var,
                query: expr1,
                body,
            })
        }
    }

    // stmt_case + make_case (pl_gram.y).
    fn parse_case(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let lineno = self.lineno(lloc);
        let t = self.yylex()?;
        let t_expr = if t.0 == K_WHEN {
            None
        } else {
            self.push_back(&t)?;
            let e = self.read_sql_expression(K_WHEN, "WHEN")?;
            Some(e)
        };

        let mut whens: Vec<(PlExpr, Vec<PlStmt>)> = Vec::new();
        loop {
            let expr = self.read_sql_expression(K_THEN, "THEN")?;
            let stmts = self.parse_proc_sect(&[K_WHEN, K_ELSE, K_END])?;
            whens.push((expr, stmts));
            let nt = self.yylex()?;
            if nt.0 != K_WHEN {
                self.push_back(&nt)?;
                break;
            }
        }
        let mut have_else = false;
        let mut else_stmts = Vec::new();
        let t = self.yylex()?;
        if t.0 == K_ELSE {
            have_else = true;
            else_stmts = self.parse_proc_sect(&[K_END])?;
            self.expect(K_END, "syntax error")?;
        } else {
            debug_assert_eq!(t.0, K_END);
        }
        self.expect(K_CASE, "syntax error")?;
        self.expect(';' as i32, "syntax error")?;

        let mut t_varno: Dno = 0;
        if t_expr.is_some() {
            let varname = format!("__Case__Variable_{}__", self.comp.datums.len());
            t_varno = self.comp.build_variable(
                &varname,
                lineno,
                CompState::build_datatype(INT4OID, -1, types_core::InvalidOid)?,
                true,
            )?;
            for (expr, _) in &mut whens {
                debug_assert_eq!(expr.parse_mode, RawParseMode::RAW_PARSE_PLPGSQL_EXPR);
                expr.query = format!("\"{varname}\" IN ({})", expr.query);
                expr.ns = self.comp.ns_top;
            }
        }
        Ok(PlStmt::Case { lineno, t_expr, t_varno, whens, have_else, else_stmts })
    }

    // stmt_foreach_a (pl_gram.y).
    fn parse_foreach(&mut self, label: Option<String>, lloc: i32) -> PgResult<PlStmt> {
        self.comp.ns_push_label(label.as_deref(), LABEL_LOOP);
        let (name, _var_lineno, scalar, rowrec, var_loc) = self.parse_for_variable()?;
        let varno = if let Some(r) = rowrec {
            self.check_assignable(r, var_loc)?;
            r
        } else if let Some(s) = scalar {
            self.check_assignable(s, var_loc)?;
            s
        } else {
            let _ = name;
            return Err(self.gram_err_pos(
                ERRCODE_SYNTAX_ERROR,
                "loop variable of FOREACH must be a known variable or list of variables"
                    .to_string(),
                var_loc,
            ));
        };
        let t = self.yylex()?;
        let slice = if Self::tok_is_keyword(&t, K_SLICE, "slice") {
            let it = self.expect(ICONST, "syntax error")?;
            it.1.ival
        } else {
            self.push_back(&t)?;
            0
        };
        self.expect(K_IN, "syntax error")?;
        self.expect(K_ARRAY, "syntax error")?;
        let expr = self.read_sql_expression(K_LOOP, "LOOP")?;
        let (body, end_label, end_loc) = self.parse_loop_body()?;
        self.check_labels(label.as_deref(), end_label.as_deref(), end_loc)?;
        self.comp.ns_pop();
        Ok(PlStmt::ForEachA {
            lineno: self.lineno(lloc),
            label,
            varno,
            slice,
            expr,
            body,
        })
    }

    fn make_scalar_list1(&mut self, _name: &str, dno: Dno, lineno: i32, loc: i32) -> PgResult<Dno> {
        self.check_assignable(dno, loc)?;
        Ok(self.comp.build_row("(unnamed row)", lineno, vec![dno]))
    }

    fn read_into_scalar_list(
        &mut self,
        initial_name: &str,
        initial_dno: Dno,
        initial_loc: i32,
    ) -> PgResult<Dno> {
        self.check_assignable(initial_dno, initial_loc)?;
        let mut varnos = vec![initial_dno];
        loop {
            let t = self.yylex()?;
            if t.0 != (',' as i32) {
                self.push_back(&t)?;
                break;
            }
            let vt = self.yylex()?;
            if vt.0 == T_DATUM {
                let w = vt.1.wdatum.as_ref().expect("T_DATUM");
                self.check_assignable(w.dno, vt.2)?;
                match &self.comp.datums[w.dno as usize] {
                    PlDatum::Row(_) | PlDatum::Rec(_) => {
                        let nm = if !w.ident.is_empty() {
                            w.ident.clone()
                        } else {
                            w.idents.join(".")
                        };
                        return Err(self.gram_err_pos(
                            ERRCODE_SYNTAX_ERROR,
                            format!("\"{nm}\" is not a scalar variable"),
                            vt.2,
                        ));
                    }
                    _ => varnos.push(w.dno),
                }
            } else {
                return Err(self.current_token_is_not_variable(&vt));
            }
        }
        let _ = initial_name;
        let lineno = self.lineno(initial_loc);
        Ok(self.comp.build_row("(unnamed row)", lineno, varnos))
    }

    fn parse_exit(&mut self, is_exit: bool, lloc: i32) -> PgResult<PlStmt> {
        let (label, label_loc) = self.opt_label()?;
        let t = self.yylex()?;
        let cond = if t.0 == (';' as i32) {
            None
        } else if t.0 == K_WHEN {
            Some(self.read_sql_expression(';' as i32, ";")?)
        } else {
            return Err(self.yyerror("syntax error", t.2));
        };

        if let Some(lbl) = &label {
            match self.comp.ns_lookup_label(self.comp.ns_top, lbl) {
                None => {
                    return Err(self.gram_err_pos(
                        ERRCODE_SYNTAX_ERROR,
                        format!(
                            "there is no label \"{lbl}\" attached to any block or loop enclosing this statement"
                        ),
                        label_loc,
                    ));
                }
                Some(idx) => {
                    if self.comp.ns[idx as usize].itemno != LABEL_LOOP && !is_exit {
                        return Err(self.gram_err_pos(
                            ERRCODE_SYNTAX_ERROR,
                            format!("block label \"{lbl}\" cannot be used in CONTINUE"),
                            label_loc,
                        ));
                    }
                }
            }
        } else if !self.comp.ns_has_loop_label(self.comp.ns_top) {
            let msg = if is_exit {
                "EXIT cannot be used outside a loop, unless it has a label"
            } else {
                "CONTINUE cannot be used outside a loop"
            };
            return Err(self.gram_err_pos(ERRCODE_SYNTAX_ERROR, msg.to_string(), lloc));
        }

        Ok(PlStmt::ExitContinue {
            lineno: self.lineno(lloc),
            is_exit,
            label,
            cond,
        })
    }

    fn parse_return(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let t = self.yylex()?;
        if t.0 == 0 {
            return Err(self.yyerror("unexpected end of function definition", t.2));
        }
        if Self::tok_is_keyword(&t, K_NEXT, "next") {
            return self.make_return_next_stmt(lloc);
        }
        if Self::tok_is_keyword(&t, K_QUERY, "query") {
            return self.make_return_query_stmt(lloc);
        }
        self.push_back(&t)?;

        let lineno = self.lineno(lloc);
        if self.fn_retset {
            let t = self.yylex()?;
            if t.0 != (';' as i32) {
                return Err(Box::new(
                    elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                        .errmsg("RETURN cannot have a parameter in function returning set")
                        .errhint("Use RETURN NEXT or RETURN QUERY.")
                        .errposition(self.sc.errposition(t.2))
                        .into_error(),
                ));
            }
            return Ok(PlStmt::Return { lineno, expr: None, retvarno: -1 });
        }
        if self.fn_rettype == VOIDOID {
            let t = self.yylex()?;
            if t.0 != (';' as i32) {
                if self.fn_prokind == PROKIND_PROCEDURE {
                    return Err(self.gram_err_pos(
                        ERRCODE_SYNTAX_ERROR,
                        "RETURN cannot have a parameter in a procedure".to_string(),
                        t.2,
                    ));
                }
                return Err(self.gram_err_pos(
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    "RETURN cannot have a parameter in function returning void".to_string(),
                    t.2,
                ));
            }
            return Ok(PlStmt::Return { lineno, expr: None, retvarno: -1 });
        }
        if self.out_param_varno >= 0 {
            let t = self.yylex()?;
            if t.0 != (';' as i32) {
                return Err(self.gram_err_pos(
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    "RETURN cannot have a parameter in function with OUT parameters".to_string(),
                    t.2,
                ));
            }
            return Ok(PlStmt::Return { lineno, expr: None, retvarno: self.out_param_varno });
        }

        let t = self.yylex()?;
        // pl_gram.y:3408-3412: fast path only for VAR/PROMISE/ROW/REC datums;
        // RECFIELD falls through to the expression path.
        if t.0 == T_DATUM && self.peek()? == (';' as i32) {
            let retvarno = t.1.wdatum.as_ref().expect("T_DATUM").dno;
            if matches!(
                self.comp.datums[retvarno as usize],
                PlDatum::Var(_) | PlDatum::Row(_) | PlDatum::Rec(_)
            ) {
                let semi = self.yylex()?;
                debug_assert_eq!(semi.0, ';' as i32);
                return Ok(PlStmt::Return { lineno, expr: None, retvarno });
            }
        }
        self.push_back(&t)?;
        let expr = self.read_sql_expression(';' as i32, ";")?;
        Ok(PlStmt::Return { lineno, expr: Some(expr), retvarno: -1 })
    }

    // make_return_next_stmt (pl_gram.y:3437).
    fn make_return_next_stmt(&mut self, lloc: i32) -> PgResult<PlStmt> {
        if !self.fn_retset {
            return Err(self.gram_err_pos(
                types_error::ERRCODE_DATATYPE_MISMATCH,
                "cannot use RETURN NEXT in a non-SETOF function".to_string(),
                lloc,
            ));
        }
        let lineno = self.lineno(lloc);
        if self.out_param_varno >= 0 {
            let t = self.yylex()?;
            if t.0 != (';' as i32) {
                return Err(self.gram_err_pos(
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    "RETURN NEXT cannot have a parameter in function with OUT parameters"
                        .to_string(),
                    t.2,
                ));
            }
            return Ok(PlStmt::ReturnNext { lineno, expr: None, retvarno: self.out_param_varno });
        }
        let t = self.yylex()?;
        if t.0 == T_DATUM && self.peek()? == (';' as i32) {
            let retvarno = t.1.wdatum.as_ref().expect("T_DATUM").dno;
            if matches!(
                self.comp.datums[retvarno as usize],
                PlDatum::Var(_) | PlDatum::Row(_) | PlDatum::Rec(_)
            ) {
                let semi = self.yylex()?;
                debug_assert_eq!(semi.0, ';' as i32);
                return Ok(PlStmt::ReturnNext { lineno, expr: None, retvarno });
            }
        }
        self.push_back(&t)?;
        let expr = self.read_sql_expression(';' as i32, ";")?;
        Ok(PlStmt::ReturnNext { lineno, expr: Some(expr), retvarno: -1 })
    }

    // make_return_query_stmt (pl_gram.y:3501).
    fn make_return_query_stmt(&mut self, lloc: i32) -> PgResult<PlStmt> {
        if !self.fn_retset {
            return Err(self.gram_err_pos(
                types_error::ERRCODE_DATATYPE_MISMATCH,
                "cannot use RETURN QUERY in a non-SETOF function".to_string(),
                lloc,
            ));
        }
        let lineno = self.lineno(lloc);
        let t = self.yylex()?;
        if t.0 != K_EXECUTE {
            self.push_back(&t)?;
            let query = self.read_sql_construct(
                ';' as i32,
                0,
                0,
                ";",
                RawParseMode::RAW_PARSE_DEFAULT,
                false,
                true,
                None,
                None,
            )?;
            return Ok(PlStmt::ReturnQuery { lineno, query: Some(query), dynquery: None, params: Vec::new() });
        }
        let mut term = 0i32;
        let dynquery = self.read_sql_expression2(';' as i32, K_USING, "; or USING", &mut term)?;
        let mut params = Vec::new();
        if term == K_USING {
            loop {
                let mut t2 = 0i32;
                let expr = self.read_sql_expression2(',' as i32, ';' as i32, ", or ;", &mut t2)?;
                params.push(expr);
                if t2 != (',' as i32) {
                    break;
                }
            }
        }
        Ok(PlStmt::ReturnQuery { lineno, query: None, dynquery: Some(dynquery), params })
    }

    fn recognize_err_condition(&self, condname: &str, allow_sqlstate: bool) -> PgResult<()> {
        // plpgsql_recognize_err_condition (pl_comp.c).
        if allow_sqlstate
            && condname.len() == 5
            && condname
                .bytes()
                .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
        {
            return Ok(());
        }
        if EXCEPTION_LABEL_MAP.iter().any(|&(n, _)| n == condname) {
            return Ok(());
        }
        Err(self.gram_err(
            types_error::ERRCODE_UNDEFINED_OBJECT,
            format!("unrecognized exception condition \"{condname}\""),
        ))
    }

    fn parse_raise(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let lineno = self.lineno(lloc);
        let mut elog_level = ELOG_ERROR;
        let mut condname: Option<String> = None;
        let mut message: Option<String> = None;
        let mut params: Vec<PlExpr> = Vec::new();
        let mut options: Vec<RaiseOption> = Vec::new();

        let mut t = self.yylex()?;
        if t.0 == 0 {
            return Err(self.yyerror("unexpected end of function definition", t.2));
        }
        if t.0 != (';' as i32) {
            if Self::tok_is_keyword(&t, K_EXCEPTION, "exception") {
                elog_level = ELOG_ERROR;
                t = self.yylex()?;
            } else if Self::tok_is_keyword(&t, K_WARNING, "warning") {
                elog_level = WARNING;
                t = self.yylex()?;
            } else if Self::tok_is_keyword(&t, K_NOTICE, "notice") {
                elog_level = NOTICE;
                t = self.yylex()?;
            } else if Self::tok_is_keyword(&t, K_INFO, "info") {
                elog_level = INFO;
                t = self.yylex()?;
            } else if Self::tok_is_keyword(&t, K_LOG, "log") {
                elog_level = LOG;
                t = self.yylex()?;
            } else if Self::tok_is_keyword(&t, K_DEBUG, "debug") {
                elog_level = DEBUG1;
                t = self.yylex()?;
            }
            if t.0 == 0 {
                return Err(self.yyerror("unexpected end of function definition", t.2));
            }

            if t.0 == SCONST {
                message = t.1.str_.clone();
                let mut tok = self.yylex()?;
                if tok.0 != (',' as i32) && tok.0 != (';' as i32) && tok.0 != K_USING {
                    return Err(self.yyerror("syntax error", tok.2));
                }
                while tok.0 == (',' as i32) {
                    let mut term = 0i32;
                    let expr = self.read_sql_construct(
                        ',' as i32,
                        ';' as i32,
                        K_USING,
                        ", or ; or USING",
                        RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
                        true,
                        true,
                        None,
                        Some(&mut term),
                    )?;
                    params.push(expr);
                    tok = (term, Yystype::default(), 0, 0);
                }
                t = tok;
            } else if t.0 != K_USING {
                if Self::tok_is_keyword(&t, K_SQLSTATE, "sqlstate") {
                    let s = self.yylex()?;
                    if s.0 != SCONST {
                        return Err(self.yyerror("syntax error", s.2));
                    }
                    let code = s.1.str_.clone().unwrap_or_default();
                    if code.len() != 5
                        || !code
                            .bytes()
                            .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
                    {
                        return Err(self.yyerror("invalid SQLSTATE code", s.2));
                    }
                    condname = Some(code);
                } else {
                    let name = if t.0 == T_WORD {
                        t.1.word.as_ref().expect("T_WORD").ident.clone()
                    } else if let Some(kw) = Self::unreserved_keyword_name(&t) {
                        kw.to_string()
                    } else {
                        return Err(self.yyerror("syntax error", t.2));
                    };
                    self.recognize_err_condition(&name, false)?;
                    condname = Some(name);
                }
                let nt = self.yylex()?;
                if nt.0 != (';' as i32) && nt.0 != K_USING {
                    return Err(self.yyerror("syntax error", nt.2));
                }
                t = nt;
            }

            if t.0 == K_USING {
                loop {
                    let ot = self.yylex()?;
                    if ot.0 == 0 {
                        return Err(self.yyerror("unexpected end of function definition", ot.2));
                    }
                    let opt_type = if Self::tok_is_keyword(&ot, K_ERRCODE, "errcode") {
                        PLPGSQL_RAISEOPTION_ERRCODE
                    } else if Self::tok_is_keyword(&ot, K_MESSAGE, "message") {
                        PLPGSQL_RAISEOPTION_MESSAGE
                    } else if Self::tok_is_keyword(&ot, K_DETAIL, "detail") {
                        PLPGSQL_RAISEOPTION_DETAIL
                    } else if Self::tok_is_keyword(&ot, K_HINT, "hint") {
                        PLPGSQL_RAISEOPTION_HINT
                    } else if Self::tok_is_keyword(&ot, K_COLUMN, "column") {
                        PLPGSQL_RAISEOPTION_COLUMN
                    } else if Self::tok_is_keyword(&ot, K_CONSTRAINT, "constraint") {
                        PLPGSQL_RAISEOPTION_CONSTRAINT
                    } else if Self::tok_is_keyword(&ot, K_DATATYPE, "datatype") {
                        PLPGSQL_RAISEOPTION_DATATYPE
                    } else if Self::tok_is_keyword(&ot, K_TABLE, "table") {
                        PLPGSQL_RAISEOPTION_TABLE
                    } else if Self::tok_is_keyword(&ot, K_SCHEMA, "schema") {
                        PLPGSQL_RAISEOPTION_SCHEMA
                    } else {
                        return Err(self.yyerror("unrecognized RAISE statement option", ot.2));
                    };
                    let et = self.yylex()?;
                    if et.0 != ('=' as i32) && et.0 != COLON_EQUALS {
                        return Err(self.yyerror("syntax error, expected \"=\"", et.2));
                    }
                    let mut term = 0i32;
                    let expr =
                        self.read_sql_expression2(',' as i32, ';' as i32, ", or ;", &mut term)?;
                    options.push(RaiseOption { opt_type, expr });
                    if term == (';' as i32) {
                        break;
                    }
                }
            }
        }

        // RAISE without parameters: only valid inside an exception handler.
        if message.is_none() && condname.is_none() && options.is_empty() && params.is_empty() {
            // Exception blocks are unported, so this is always an error at
            // runtime in C; C checks at parse time only that RAISE; appears
            // inside an exception block? (No: pl_exec checks at runtime.)
        }

        // check_raise_parameters.
        if let Some(msg) = &message {
            let mut expected = 0usize;
            let bytes = msg.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'%' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'%' {
                        i += 1;
                    } else {
                        expected += 1;
                    }
                }
                i += 1;
            }
            if expected < params.len() {
                return Err(self.gram_err(
                    ERRCODE_SYNTAX_ERROR,
                    "too many parameters specified for RAISE".to_string(),
                ));
            }
            if expected > params.len() {
                return Err(self.gram_err(
                    ERRCODE_SYNTAX_ERROR,
                    "too few parameters specified for RAISE".to_string(),
                ));
            }
        }

        Ok(PlStmt::Raise {
            lineno,
            elog_level,
            condname,
            message,
            params,
            options,
        })
    }

    fn parse_assert(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let mut term = 0i32;
        let cond = self.read_sql_expression2(',' as i32, ';' as i32, ", or ;", &mut term)?;
        let message = if term == (',' as i32) {
            Some(self.read_sql_expression(';' as i32, ";")?)
        } else {
            None
        };
        Ok(PlStmt::Assert {
            lineno: self.lineno(lloc),
            cond,
            message,
        })
    }

    fn parse_perform(&mut self, t: Tok, lloc: i32) -> PgResult<PlStmt> {
        let lineno = self.lineno(lloc);
        self.push_back(&t)?;
        let mut startloc = -1i32;
        let mut expr = self.read_sql_construct(
            ';' as i32,
            0,
            0,
            ";",
            RawParseMode::RAW_PARSE_DEFAULT,
            false,
            false,
            Some(&mut startloc),
            None,
        )?;
        // Substitute SELECT for PERFORM ("perform" is 7 chars, "SELECT" 6).
        debug_assert!(expr.query.len() >= 7);
        expr.query.replace_range(0..7, "SELECT");
        self.check_sql_expr(&expr.query, expr.parse_mode, startloc + 1)?;
        Ok(PlStmt::Perform { lineno, expr })
    }

    fn parse_getdiag(&mut self, lloc: i32) -> PgResult<PlStmt> {
        // getdiag_area_opt.
        let mut t = self.yylex()?;
        let is_stacked = if Self::tok_is_keyword(&t, K_CURRENT, "current") {
            t = self.yylex()?;
            false
        } else if Self::tok_is_keyword(&t, K_STACKED, "stacked") {
            t = self.yylex()?;
            true
        } else {
            false
        };
        if t.0 != K_DIAGNOSTICS {
            return Err(self.yyerror("syntax error", t.2));
        }

        let mut items = Vec::new();
        loop {
            // getdiag_target.
            let vt = self.yylex()?;
            let target = if vt.0 == T_DATUM {
                let w = vt.1.wdatum.as_ref().expect("T_DATUM");
                self.check_assignable(w.dno, vt.2)?;
                match &self.comp.datums[w.dno as usize] {
                    PlDatum::Row(_) | PlDatum::Rec(_) => {
                        let nm = if !w.ident.is_empty() {
                            w.ident.clone()
                        } else {
                            w.idents.join(".")
                        };
                        return Err(self.gram_err_pos(
                            ERRCODE_SYNTAX_ERROR,
                            format!("\"{nm}\" is not a scalar variable"),
                            vt.2,
                        ));
                    }
                    _ => w.dno,
                }
            } else {
                return Err(self.current_token_is_not_variable(&vt));
            };
            let at = self.yylex()?;
            if at.0 != ('=' as i32) && at.0 != COLON_EQUALS {
                return Err(self.yyerror("syntax error", at.2));
            }
            // getdiag_item.
            let it = self.yylex()?;
            let kind = if Self::tok_is_keyword(&it, K_ROW_COUNT, "row_count") {
                GETDIAG_ROW_COUNT
            } else if Self::tok_is_keyword(&it, K_PG_ROUTINE_OID, "pg_routine_oid") {
                GETDIAG_ROUTINE_OID
            } else if Self::tok_is_keyword(&it, K_PG_CONTEXT, "pg_context") {
                GETDIAG_CONTEXT
            } else if Self::tok_is_keyword(&it, K_PG_EXCEPTION_CONTEXT, "pg_exception_context") {
                GETDIAG_ERROR_CONTEXT
            } else if Self::tok_is_keyword(&it, K_PG_EXCEPTION_DETAIL, "pg_exception_detail") {
                GETDIAG_ERROR_DETAIL
            } else if Self::tok_is_keyword(&it, K_PG_EXCEPTION_HINT, "pg_exception_hint") {
                GETDIAG_ERROR_HINT
            } else if Self::tok_is_keyword(&it, K_RETURNED_SQLSTATE, "returned_sqlstate") {
                GETDIAG_RETURNED_SQLSTATE
            } else if Self::tok_is_keyword(&it, K_COLUMN_NAME, "column_name") {
                GETDIAG_COLUMN_NAME
            } else if Self::tok_is_keyword(&it, K_CONSTRAINT_NAME, "constraint_name") {
                GETDIAG_CONSTRAINT_NAME
            } else if Self::tok_is_keyword(&it, K_PG_DATATYPE_NAME, "pg_datatype_name") {
                GETDIAG_DATATYPE_NAME
            } else if Self::tok_is_keyword(&it, K_MESSAGE_TEXT, "message_text") {
                GETDIAG_MESSAGE_TEXT
            } else if Self::tok_is_keyword(&it, K_TABLE_NAME, "table_name") {
                GETDIAG_TABLE_NAME
            } else if Self::tok_is_keyword(&it, K_SCHEMA_NAME, "schema_name") {
                GETDIAG_SCHEMA_NAME
            } else {
                return Err(self.yyerror("unrecognized GET DIAGNOSTICS item", it.2));
            };
            let kindname = getdiag_kindname(kind);
            if is_stacked && matches!(kind, GETDIAG_ROW_COUNT | GETDIAG_ROUTINE_OID) {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!(
                        "diagnostics item {kindname} is not allowed in GET STACKED DIAGNOSTICS"
                    ),
                    lloc,
                ));
            }
            if !is_stacked
                && !matches!(kind, GETDIAG_ROW_COUNT | GETDIAG_ROUTINE_OID | GETDIAG_CONTEXT)
            {
                return Err(self.gram_err_pos(
                    ERRCODE_SYNTAX_ERROR,
                    format!(
                        "diagnostics item {kindname} is not allowed in GET CURRENT DIAGNOSTICS"
                    ),
                    lloc,
                ));
            }
            items.push(GetDiagItem { kind, target });

            let nt = self.yylex()?;
            if nt.0 == (',' as i32) {
                continue;
            }
            if nt.0 == (';' as i32) {
                break;
            }
            return Err(self.yyerror("syntax error", nt.2));
        }

        Ok(PlStmt::GetDiag {
            lineno: self.lineno(lloc),
            is_stacked,
            items,
        })
    }

    // make_execsql_stmt (pl_gram.y); `first` is the already-read first token.
    fn make_execsql_stmt(&mut self, first: Tok) -> PgResult<PlStmt> {
        let location = first.2;
        let firsttoken = first.0;
        let first_word = first.1.word.as_ref().map(|w| w.ident.clone());

        let save = self.comp.identifier_lookup;
        self.comp.identifier_lookup = IdentifierLookup::Expr;

        let mut target: Dno = -1;
        let mut have_into = false;
        let mut have_strict = false;
        let mut into_start_loc = -1i32;
        let mut into_end_loc = -1i32;
        let mut paren_depth = 0i32;
        let mut begin_depth = 0i32;
        let mut in_routine_definition = false;
        let mut tokens = [0u8; 4];
        let mut token_count = 0usize;

        let mut tok = firsttoken;
        let mut prev_tok;
        if tok == T_WORD && first_word.as_deref() == Some("create") {
            tokens[0] = b'c';
        }
        token_count += 1;

        let end_loc;
        loop {
            prev_tok = tok;
            let t = self.yylex()?;
            tok = t.0;
            let lloc = t.2;
            if have_into && into_end_loc < 0 {
                into_end_loc = lloc;
            }
            if tokens[0] == b'c' && token_count < 4 {
                if tok == K_OR {
                    tokens[token_count] = b'o';
                } else if tok == T_WORD
                    && t.1.word.as_ref().map(|w| w.ident.as_str()) == Some("replace")
                {
                    tokens[token_count] = b'r';
                } else if tok == T_WORD
                    && matches!(
                        t.1.word.as_ref().map(|w| w.ident.as_str()),
                        Some("function") | Some("procedure")
                    )
                {
                    tokens[token_count] = b'f';
                }
                if tokens[1] == b'f' || (tokens[1] == b'o' && tokens[2] == b'r' && tokens[3] == b'f')
                {
                    in_routine_definition = true;
                }
                token_count += 1;
            }
            if tok == ('(' as i32) {
                paren_depth += 1;
            } else if tok == (')' as i32) && paren_depth > 0 {
                paren_depth -= 1;
            }
            if in_routine_definition && paren_depth == 0 {
                if tok == K_BEGIN || tok == K_CASE {
                    begin_depth += 1;
                } else if tok == K_END && begin_depth > 0 {
                    begin_depth -= 1;
                }
            }
            if tok == (';' as i32) && paren_depth == 0 && begin_depth == 0 {
                end_loc = lloc;
                break;
            }
            if tok == 0 {
                return Err(self.yyerror("unexpected end of function definition", lloc));
            }
            if tok == K_INTO {
                if prev_tok == K_INSERT || prev_tok == K_MERGE || firsttoken == K_IMPORT {
                    continue;
                }
                if have_into {
                    return Err(self.yyerror("INTO specified more than once", lloc));
                }
                have_into = true;
                into_start_loc = lloc;
                self.comp.identifier_lookup = IdentifierLookup::Normal;
                let (tgt, strict) = self.read_into_target(true)?;
                target = tgt;
                have_strict = strict;
                self.comp.identifier_lookup = IdentifierLookup::Expr;
            }
        }
        self.comp.identifier_lookup = save;

        let mut text = if have_into {
            let mut s = self.source_span(location, into_start_loc);
            s.extend(std::iter::repeat(' ').take((into_end_loc - into_start_loc).max(0) as usize));
            s.push_str(&self.source_span(into_end_loc, end_loc));
            s
        } else {
            self.source_span(location, end_loc)
        };
        while text.ends_with(|c: char| c.is_ascii_whitespace()) {
            text.pop();
        }

        let expr = self.make_expr(text, RawParseMode::RAW_PARSE_DEFAULT, self.comp.ns_top);
        self.check_sql_expr(&expr.query, expr.parse_mode, location)?;

        // mod_stmt: computed at exec in C 18 (stmt_execsql.mod_stmt is filled
        // lazily by exec_stmt_execsql); carried false here.
        Ok(PlStmt::ExecSql {
            lineno: self.lineno(location),
            sqlstmt: expr,
            mod_stmt: false,
            into: have_into,
            strict: have_strict,
            target,
        })
    }

    // stmt_dynexecute (pl_gram.y): INTO and USING accepted in either order.
    fn parse_dynexecute(&mut self, lloc: i32) -> PgResult<PlStmt> {
        let mut endtoken = 0i32;
        let query = self.read_sql_construct(
            K_INTO,
            K_USING,
            ';' as i32,
            "INTO or USING or ;",
            RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
            true,
            true,
            None,
            Some(&mut endtoken),
        )?;
        let mut into = false;
        let mut strict = false;
        let mut target: Dno = -1;
        let mut params: Vec<PlExpr> = Vec::new();
        loop {
            if endtoken == K_INTO {
                if into {
                    let t = self.yylex()?;
                    return Err(self.yyerror("syntax error", t.2));
                }
                into = true;
                let (tgt, s) = self.read_into_target(true)?;
                target = tgt;
                strict = s;
                let t = self.yylex()?;
                endtoken = t.0;
                if endtoken != K_USING && endtoken != (';' as i32) {
                    return Err(self.yyerror("syntax error", t.2));
                }
            } else if endtoken == K_USING {
                if !params.is_empty() {
                    let t = self.yylex()?;
                    return Err(self.yyerror("syntax error", t.2));
                }
                loop {
                    let mut term = 0i32;
                    let expr = self.read_sql_construct(
                        ',' as i32,
                        ';' as i32,
                        K_INTO,
                        ", or ; or INTO",
                        RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
                        true,
                        true,
                        None,
                        Some(&mut term),
                    )?;
                    params.push(expr);
                    endtoken = term;
                    if term != (',' as i32) {
                        break;
                    }
                }
            } else if endtoken == (';' as i32) {
                break;
            } else {
                let t = self.yylex()?;
                return Err(self.yyerror("syntax error", t.2));
            }
        }
        Ok(PlStmt::DynExecute {
            lineno: self.lineno(lloc),
            query,
            into,
            strict,
            target,
            params,
        })
    }

    // read_into_target.
    fn read_into_target(&mut self, want_strict: bool) -> PgResult<(Dno, bool)> {
        let mut strict = false;
        let mut t = self.yylex()?;
        if want_strict && t.0 == K_STRICT {
            strict = true;
            t = self.yylex()?;
        }
        if t.0 == T_DATUM {
            let w = t.1.wdatum.clone().expect("T_DATUM");
            match &self.comp.datums[w.dno as usize] {
                PlDatum::Row(_) | PlDatum::Rec(_) => {
                    self.check_assignable(w.dno, t.2)?;
                    let nt = self.yylex()?;
                    if nt.0 == (',' as i32) {
                        return Err(self.gram_err_pos(
                            ERRCODE_SYNTAX_ERROR,
                            "record variable cannot be part of multiple-item INTO list"
                                .to_string(),
                            nt.2,
                        ));
                    }
                    self.push_back(&nt)?;
                    Ok((w.dno, strict))
                }
                _ => {
                    let name = if !w.ident.is_empty() {
                        w.ident.clone()
                    } else {
                        w.idents.join(".")
                    };
                    let row = self.read_into_scalar_list(&name, w.dno, t.2)?;
                    Ok((row, strict))
                }
            }
        } else {
            Err(self.current_token_is_not_variable(&t))
        }
    }
}

pub fn getdiag_kindname(kind: i32) -> &'static str {
    match kind {
        GETDIAG_ROW_COUNT => "ROW_COUNT",
        GETDIAG_ROUTINE_OID => "PG_ROUTINE_OID",
        GETDIAG_CONTEXT => "PG_CONTEXT",
        GETDIAG_ERROR_CONTEXT => "PG_EXCEPTION_CONTEXT",
        GETDIAG_ERROR_DETAIL => "PG_EXCEPTION_DETAIL",
        GETDIAG_ERROR_HINT => "PG_EXCEPTION_HINT",
        GETDIAG_RETURNED_SQLSTATE => "RETURNED_SQLSTATE",
        GETDIAG_COLUMN_NAME => "COLUMN_NAME",
        GETDIAG_CONSTRAINT_NAME => "CONSTRAINT_NAME",
        GETDIAG_DATATYPE_NAME => "PG_DATATYPE_NAME",
        GETDIAG_MESSAGE_TEXT => "MESSAGE_TEXT",
        GETDIAG_TABLE_NAME => "TABLE_NAME",
        GETDIAG_SCHEMA_NAME => "SCHEMA_NAME",
        _ => "unknown",
    }
}
