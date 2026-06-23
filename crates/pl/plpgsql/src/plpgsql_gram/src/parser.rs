//! The `pl_gram.y` recursive-descent parser (see crate docs).

use crate::mem;
use comp_seams as comp_seam;
use funcs as funcs;
use plpgsql_scanner::{
    self as scanner, PlpgsqlScanner, Yyltype, Yystype,
    GREATER_GREATER, LESS_LESS, T_CWORD, T_DATUM, T_WORD,
    K_ABSOLUTE, K_ALIAS, K_ALL, K_AND, K_ARRAY, K_ASSERT, K_BACKWARD, K_BEGIN, K_BY, K_CALL,
    K_CASE, K_CHAIN, K_CLOSE, K_COLLATE, K_COLUMN, K_COLUMN_NAME, K_COMMIT, K_CONSTANT,
    K_CONSTRAINT, K_CONSTRAINT_NAME, K_CONTINUE, K_CURRENT, K_CURSOR, K_DATATYPE, K_DEBUG,
    K_DECLARE, K_DEFAULT, K_DETAIL, K_DIAGNOSTICS, K_DO, K_DUMP, K_ELSE, K_ELSIF, K_END, K_ERRCODE,
    K_ERROR, K_EXCEPTION, K_EXECUTE, K_EXIT, K_FETCH, K_FIRST, K_FOR, K_FOREACH, K_FORWARD,
    K_FROM, K_GET, K_HINT, K_IF, K_IMPORT, K_IN, K_INFO, K_INSERT, K_INTO, K_IS, K_LAST, K_LOG,
    K_LOOP, K_MERGE, K_MESSAGE, K_MESSAGE_TEXT, K_MOVE, K_NEXT, K_NO, K_NOT, K_NOTICE, K_NULL,
    K_OPEN, K_OPTION, K_OR, K_PERFORM, K_PG_CONTEXT, K_PG_DATATYPE_NAME, K_PG_EXCEPTION_CONTEXT,
    K_PG_EXCEPTION_DETAIL, K_PG_EXCEPTION_HINT, K_PG_ROUTINE_OID, K_PRINT_STRICT_PARAMS, K_PRIOR,
    K_QUERY, K_RAISE, K_RELATIVE, K_RETURN, K_RETURNED_SQLSTATE, K_REVERSE, K_ROLLBACK,
    K_ROW_COUNT, K_ROWTYPE, K_SCHEMA, K_SCHEMA_NAME, K_SCROLL, K_SLICE, K_SQLSTATE, K_STACKED,
    K_STRICT, K_TABLE, K_TABLE_NAME, K_THEN, K_TYPE, K_USE_COLUMN, K_USE_VARIABLE, K_USING,
    K_VARIABLE_CONFLICT, K_WARNING, K_WHEN, K_WHILE,
};
use scan_fgram::tokens::{COLON_EQUALS, DOT_DOT, EQUALS_GREATER, ICONST, SCONST};
use utils_error::ereport;
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_ALIAS,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_SYNTAX_ERROR,
    ERROR as ERROR_LEVEL,
};
use parsenodes::RawParseMode;
use plpgsql::*;

// ===========================================================================
// Catalog OID constants the grammar embeds literally.
// ===========================================================================
const INVALID_OID: Oid = 0;
const TEXTOID: Oid = 25;
const INT4OID: Oid = 23;
const VOIDOID: Oid = 2278;
const REFCURSOROID: Oid = 1790;
const PROKIND_PROCEDURE: u8 = b'p';

#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != INVALID_OID
}

// Cursor option flags (`nodes/parsenodes.h`).
const CURSOR_OPT_SCROLL: i32 = 0x0002;
const CURSOR_OPT_NO_SCROLL: i32 = 0x0004;
const CURSOR_OPT_FAST_PLAN: i32 = 0x0100;

// elog levels embedded by stmt_raise (`utils/elog.h`).
const DEBUG1: i32 = 13;
const LOG: i32 = 15;
const INFO: i32 = 17;
const NOTICE: i32 = 18;
const WARNING: i32 = 19;
const ERROR: i32 = 21;

/// `FETCH_ALL` (`LONG_MAX`).
const FETCH_ALL: i64 = i64::MAX;

/// `YYEMPTY` — bison's "no current lookahead token" sentinel.
const YYEMPTY: i32 = -2;

// ===========================================================================
// Local ereport helpers (the `pl_gram.y` actions' `ereport(ERROR, ...)` sites
// that do not carry a token location; positioned variants are `Parser` methods
// routed through the scanner's `plpgsql_yyerror`).
// ===========================================================================

fn syntax_error_plain(msg: &str) -> PgError {
    ereport(ERROR_LEVEL)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg_internal(msg)
        .into_error()
}

fn internal_error(msg: &str) -> PgError {
    PgError::error(msg.to_string())
}

// ===========================================================================
// Parser driver.
// ===========================================================================

/// Recursive-descent PL/pgSQL parser. Owns the [`PlpgsqlScanner`] and threads
/// `yylval`/`yylloc` (semantic value + location of the most recently returned
/// token) just as `pl_gram.y` does.
pub struct Parser<'mcx> {
    pub scanner: PlpgsqlScanner<'mcx>,
    yylval: Yystype,
    yylloc: Yyltype,
}

/// `plpgsql_yyparse()` — run the grammar over an initialized scanner and return
/// the top-level block. The compiler calls this directly.
pub fn plpgsql_yyparse<'mcx>(
    scanner: PlpgsqlScanner<'mcx>,
) -> PgResult<Box<PLpgSQL_stmt_block>> {
    Parser::new(scanner).parse()
}

/// As [`plpgsql_yyparse`], but on the error path also reports
/// `plpgsql_latest_lineno(yyscanner)` — the most recently computed source line.
///
/// `plpgsql_compile_error_callback` (pl_comp.c) reads this scanner-tracked
/// lineno to build its `compilation of PL/pgSQL function "%s" near line %d`
/// fallback context. Because the scanner is consumed by the parse, the lineno
/// has to be read off it on the spot; the parser still owns the scanner when
/// the error surfaces, so this captures `cur_line_num` at exactly the point C's
/// global yyscanner would carry into the callback.
pub fn plpgsql_yyparse_with_lineno<'mcx>(
    scanner: PlpgsqlScanner<'mcx>,
) -> Result<Box<PLpgSQL_stmt_block>, (PgError, i32)> {
    let mut parser = Parser::new(scanner);
    match parser.parse() {
        Ok(block) => Ok(block),
        Err(e) => {
            let lineno = parser.scanner.plpgsql_latest_lineno();
            Err((e, lineno))
        }
    }
}

impl<'mcx> Parser<'mcx> {
    pub fn new(scanner: PlpgsqlScanner<'mcx>) -> Self {
        Parser {
            scanner,
            yylval: Yystype::default(),
            yylloc: 0,
        }
    }

    // -- token plumbing -----------------------------------------------------

    fn yylex(&mut self) -> PgResult<i32> {
        let (tok, lval, lloc) = self.scanner.plpgsql_yylex()?;
        self.yylval = lval;
        self.yylloc = lloc;
        Ok(tok)
    }

    fn push_back_token(&mut self, token: i32) -> PgResult<()> {
        let lval = self.yylval.clone();
        let lloc = self.yylloc;
        self.scanner.plpgsql_push_back_token(token, &lval, lloc)
    }

    fn peek(&mut self) -> PgResult<i32> {
        self.scanner.plpgsql_peek()
    }

    fn peek2(&mut self) -> PgResult<(i32, i32, i32, i32)> {
        self.scanner.plpgsql_peek2()
    }

    /// `yyerror(&yylloc, ...)` at the current token location.
    fn yyerror(&self, message: &str) -> PgError {
        self.scanner.plpgsql_yyerror(self.yylloc, message)
    }

    /// `yyerror` at an explicit location.
    fn yyerror_at(&self, loc: Yyltype, message: &str) -> PgError {
        self.scanner.plpgsql_yyerror(loc, message)
    }

    /// `ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR), errmsg(...),
    /// parser_errposition(loc))` — a positioned syntax error.  Unlike the bison
    /// `plpgsql_yyerror` callback, this uses the message verbatim (no "at or
    /// near <token>" suffix), matching the direct `ereport` sites in the C
    /// grammar (e.g. the cursor-argument errors in `read_cursor_args`).
    fn syntax_at(&self, message: &str, loc: i32) -> PgError {
        self.scanner.syntax_error_at(message, loc)
    }

    /// `ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH), ...,
    /// parser_errposition(loc))`.
    fn datatype_at(&self, message: &str, loc: i32) -> PgError {
        let mut err = ereport(ERROR_LEVEL)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg_internal(message)
            .into_error();
        let pos = self.scanner.plpgsql_scanner_errposition(loc);
        if pos > 0 {
            err = err.with_internal_position(pos);
        }
        err
    }

    /// `word_is_not_variable` / `cword_is_not_variable` — the better-than-syntax
    /// error reported when an identifier that should name a variable does not.
    fn word_is_not_variable(&self, ident: &str, loc: i32) -> PgError {
        self.syntax_at(&format!("\"{ident}\" is not a known variable"), loc)
    }

    fn cword_is_not_variable(&self, idents: &str, loc: i32) -> PgError {
        self.syntax_at(&format!("\"{idents}\" is not a known variable"), loc)
    }

    fn loc_to_lineno(&mut self, loc: i32) -> i32 {
        self.scanner.plpgsql_location_to_lineno(loc)
    }

    fn token_length(&self) -> i32 {
        self.scanner.plpgsql_token_length()
    }

    fn set_identifier_lookup(&mut self, mode: IdentifierLookup) {
        // The authoritative `plpgsql_IdentifierLookup` lives in the compiler
        // unit (consulted by plpgsql_parse_word/dblword/tripword). Keep the
        // scanner-instance mirror in sync for `identifier_lookup()` reads, and
        // push the value across the seam so the resolvers see the right mode.
        self.scanner.identifier_lookup = mode;
        comp_seam::set_identifier_lookup::call(mode);
    }

    fn identifier_lookup(&self) -> IdentifierLookup {
        self.scanner.identifier_lookup
    }
}

impl<'mcx> Parser<'mcx> {
    // -----------------------------------------------------------------------
    // Top-level entry: pl_function : comp_options pl_block opt_semi
    // -----------------------------------------------------------------------

    /// `plpgsql_yyparse()` top production `pl_function`.
    pub fn parse(&mut self) -> PgResult<Box<PLpgSQL_stmt_block>> {
        self.comp_options()?;
        let block = self.pl_block()?;
        self.opt_semi()?;
        match block {
            PLpgSQL_stmt::Block(b) => Ok(b),
            _ => Err(internal_error("pl_block must yield a block statement")),
        }
    }

    // comp_options : | comp_options comp_option
    fn comp_options(&mut self) -> PgResult<()> {
        loop {
            let tok = self.yylex()?;
            if tok != ('#' as i32) {
                self.push_back_token(tok)?;
                break;
            }
            let tok2 = self.yylex()?;
            if tok2 == K_OPTION {
                let tok3 = self.yylex()?;
                if tok3 != K_DUMP {
                    return Err(self.yyerror("syntax error"));
                }
                comp_seam::set_dump_exec_tree::call(true);
            } else if tok2 == K_PRINT_STRICT_PARAMS {
                let val = self.option_value()?;
                if val == "on" {
                    comp_seam::curr_compile_set_print_strict_params::call(true);
                } else if val == "off" {
                    comp_seam::curr_compile_set_print_strict_params::call(false);
                } else {
                    return Err(internal_error(&format!(
                        "unrecognized print_strict_params option {val}"
                    )));
                }
            } else if tok2 == K_VARIABLE_CONFLICT {
                let tok3 = self.yylex()?;
                if tok3 == K_ERROR {
                    comp_seam::curr_compile_set_resolve_option::call(
                        PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR,
                    );
                } else if tok3 == K_USE_VARIABLE {
                    comp_seam::curr_compile_set_resolve_option::call(
                        PLpgSQL_resolve_option::PLPGSQL_RESOLVE_VARIABLE,
                    );
                } else if tok3 == K_USE_COLUMN {
                    comp_seam::curr_compile_set_resolve_option::call(
                        PLpgSQL_resolve_option::PLPGSQL_RESOLVE_COLUMN,
                    );
                } else {
                    return Err(self.yyerror("syntax error"));
                }
            } else {
                return Err(self.yyerror("syntax error"));
            }
        }
        Ok(())
    }

    // option_value : T_WORD { $$ = $1.ident } | unreserved_keyword { pstrdup($1) }
    fn option_value(&mut self) -> PgResult<String> {
        let tok = self.yylex()?;
        if tok == T_WORD {
            Ok(self
                .yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("option_value: T_WORD without word payload"))?
                .ident
                .clone())
        } else if scanner::plpgsql_token_is_unreserved_keyword(tok) {
            Ok(self.yylval.keyword.clone().unwrap_or_default())
        } else {
            Err(self.yyerror("syntax error"))
        }
    }

    // opt_semi : | ';'
    fn opt_semi(&mut self) -> PgResult<()> {
        let tok = self.yylex()?;
        if tok != (';' as i32) {
            self.push_back_token(tok)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // pl_block : decl_sect K_BEGIN proc_sect exception_sect K_END opt_label
    // -----------------------------------------------------------------------
    fn pl_block(&mut self) -> PgResult<PLpgSQL_stmt> {
        self.pl_block_labeled(None)
    }

    /// `pl_block` with an optional already-consumed block label `(name, loc)`
    /// (used when `proc_stmt` peeked past a `<<label>>`). When `pre_label` is
    /// `Some`, `decl_sect` performs the namespace push from the supplied name.
    fn pl_block_labeled(&mut self, pre_label: Option<(String, i32)>) -> PgResult<PLpgSQL_stmt> {
        let declhdr = self.decl_sect(pre_label)?;

        let tok = self.yylex()?;
        if tok != K_BEGIN {
            return Err(self.yyerror("syntax error, expected \"BEGIN\""));
        }
        let begin_loc = self.yylloc;

        let body = self.proc_sect()?;
        let exceptions = self.exception_sect()?;

        let tok = self.yylex()?;
        if tok != K_END {
            return Err(self.yyerror("syntax error, expected \"END\""));
        }
        let (end_label, end_label_loc) = self.opt_label()?;

        let lineno = self.loc_to_lineno(begin_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        self.check_labels(declhdr.label.as_deref(), end_label.as_deref(), end_label_loc)?;
        funcs::plpgsql_ns_pop();

        let new = PLpgSQL_stmt_block {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_BLOCK,
            lineno,
            stmtid,
            label: declhdr.label,
            body,
            n_initvars: declhdr.n_initvars,
            initvarnos: declhdr.initvarnos,
            exceptions,
        };
        Ok(PLpgSQL_stmt::Block(mem::boxed(new)))
    }

    // -----------------------------------------------------------------------
    // decl_sect : opt_block_label [decl_start [decl_stmts]]
    // -----------------------------------------------------------------------
    fn decl_sect(&mut self, pre_label: Option<(String, i32)>) -> PgResult<DeclHdr> {
        let label = match pre_label {
            Some((name, _loc)) => {
                funcs::plpgsql_ns_push(Some(&name), PLpgSQL_label_type::PLPGSQL_LABEL_BLOCK);
                Some(name)
            }
            None => self.opt_block_label()?.0,
        };

        let tok = self.yylex()?;
        if tok != K_DECLARE {
            // decl_sect : opt_block_label
            self.push_back_token(tok)?;
            self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_NORMAL);
            return Ok(DeclHdr {
                label,
                n_initvars: 0,
                initvarnos: Vec::new(),
            });
        }

        // decl_start : K_DECLARE
        comp_seam::plpgsql_add_initdatums_forget::call();
        self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_DECLARE);

        let mut had_decls = false;
        loop {
            let tok = self.yylex()?;
            if tok == K_BEGIN {
                self.push_back_token(tok)?;
                break;
            }
            self.push_back_token(tok)?;
            self.decl_stmt()?;
            had_decls = true;
        }

        self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_NORMAL);

        if had_decls {
            let initvarnos = comp_seam::plpgsql_add_initdatums_collect::call();
            Ok(DeclHdr {
                label,
                n_initvars: initvarnos.len() as i32,
                initvarnos,
            })
        } else {
            Ok(DeclHdr {
                label,
                n_initvars: 0,
                initvarnos: Vec::new(),
            })
        }
    }

    // decl_stmt : decl_statement | K_DECLARE | LESS_LESS any_identifier GREATER_GREATER
    fn decl_stmt(&mut self) -> PgResult<()> {
        let tok = self.yylex()?;
        if tok == K_DECLARE {
            return Ok(()); // allow useless extra DECLAREs
        }
        if tok == LESS_LESS {
            let _id = self.any_identifier()?;
            let tok2 = self.yylex()?;
            if tok2 != GREATER_GREATER {
                return Err(self.yyerror("syntax error"));
            }
            return Err(self.syntax_at(
                "block label must be placed before DECLARE, not after",
                self.yylloc,
            ));
        }
        self.push_back_token(tok)?;
        self.decl_statement()
    }

    // -----------------------------------------------------------------------
    // decl_statement — variable / ALIAS / CURSOR declarations.
    // -----------------------------------------------------------------------
    fn decl_statement(&mut self) -> PgResult<()> {
        let varname = self.decl_varname()?;

        let tok = self.yylex()?;
        if tok == K_ALIAS {
            let tok2 = self.yylex()?;
            if tok2 != K_FOR {
                return Err(self.yyerror("syntax error, expected \"FOR\""));
            }
            let nsi = self.decl_aliasitem()?;
            let tok3 = self.yylex()?;
            if tok3 != (';' as i32) {
                return Err(self.yyerror("syntax error"));
            }
            funcs::plpgsql_ns_additem(nsi.itemtype, nsi.itemno, &varname.name);
            return Ok(());
        }

        if tok == K_CURSOR || tok == K_NO || tok == K_SCROLL {
            self.push_back_token(tok)?;
            let cursor_options = self.opt_scrollable()?;
            let tok2 = self.yylex()?;
            if tok2 != K_CURSOR {
                return Err(self.yyerror("syntax error, expected \"CURSOR\""));
            }
            funcs::plpgsql_ns_push(Some(&varname.name), PLpgSQL_label_type::PLPGSQL_LABEL_OTHER);

            let cursor_args = self.decl_cursor_args()?;
            self.decl_is_for()?;
            let cursor_query = self.decl_cursor_query()?;

            funcs::plpgsql_ns_pop();

            let argrow = cursor_args.unwrap_or(-1);
            comp_seam::plpgsql_build_cursor_variable::call(
                &varname.name,
                varname.lineno,
                REFCURSOROID,
                cursor_query,
                argrow,
                CURSOR_OPT_FAST_PLAN | cursor_options,
            )?;
            return Ok(());
        }

        // Variable form:
        //   decl_varname decl_const decl_datatype decl_collate decl_notnull decl_defval
        self.push_back_token(tok)?;

        let isconst = self.decl_const()?;
        let mut dtype = self.decl_datatype()?;
        let (collation, collate_loc) = self.decl_collate()?;
        let (notnull, notnull_loc) = self.decl_notnull()?;
        let default_val = self.decl_defval()?;

        if oid_is_valid(collation) {
            if !oid_is_valid(dtype.collation) {
                return Err(self.datatype_at(
                    &format!(
                        "collations are not supported by type {}",
                        type_be_placeholder(dtype.typoid)
                    ),
                    collate_loc,
                ));
            }
            dtype.collation = collation;
        }

        let var_dno = comp_seam::plpgsql_build_variable::call(
            &varname.name,
            varname.lineno,
            *dtype,
            true,
        )?;
        let has_default = default_val.is_some();
        comp_seam::plpgsql_var_set_decl_props::call(var_dno, isconst, notnull, default_val);

        if notnull && !has_default {
            let refname = comp_seam::var_refname::call(var_dno);
            return Err(self.null_value_not_allowed(
                &format!(
                    "variable \"{refname}\" must have a default value, since it's declared NOT NULL"
                ),
                notnull_loc,
            ));
        }

        if has_default {
            comp_seam::mark_var_default_as_assignment_source::call(var_dno);
        }
        Ok(())
    }

    // opt_scrollable : | K_NO K_SCROLL | K_SCROLL
    fn opt_scrollable(&mut self) -> PgResult<i32> {
        let tok = self.yylex()?;
        if tok == K_NO {
            let tok2 = self.yylex()?;
            if tok2 != K_SCROLL {
                return Err(self.yyerror("syntax error, expected \"SCROLL\""));
            }
            Ok(CURSOR_OPT_NO_SCROLL)
        } else if tok == K_SCROLL {
            Ok(CURSOR_OPT_SCROLL)
        } else {
            self.push_back_token(tok)?;
            Ok(0)
        }
    }

    // decl_cursor_query : /* empty */ { read_sql_stmt }
    fn decl_cursor_query(&mut self) -> PgResult<Option<Box<PLpgSQL_expr>>> {
        Ok(Some(self.read_sql_stmt()?))
    }

    // decl_cursor_args : /* empty */ | '(' decl_cursor_arglist ')'
    fn decl_cursor_args(&mut self) -> PgResult<Option<i32>> {
        let tok = self.yylex()?;
        if tok != ('(' as i32) {
            self.push_back_token(tok)?;
            return Ok(None);
        }
        let loc = self.yylloc;
        let args = self.decl_cursor_arglist()?;
        let tok2 = self.yylex()?;
        if tok2 != (')' as i32) {
            return Err(self.yyerror("syntax error, expected \")\""));
        }
        let lineno = self.loc_to_lineno(loc);
        let dno = comp_seam::plpgsql_build_cursor_arg_row::call(lineno, args)?;
        Ok(Some(dno))
    }

    // decl_cursor_arglist : decl_cursor_arg | decl_cursor_arglist ',' decl_cursor_arg
    fn decl_cursor_arglist(&mut self) -> PgResult<Vec<i32>> {
        let mut list = Vec::new();
        let a = self.decl_cursor_arg()?;
        mem::vpush(&mut list, a);
        loop {
            let tok = self.yylex()?;
            if tok != (',' as i32) {
                self.push_back_token(tok)?;
                break;
            }
            let a = self.decl_cursor_arg()?;
            mem::vpush(&mut list, a);
        }
        Ok(list)
    }

    // decl_cursor_arg : decl_varname decl_datatype
    fn decl_cursor_arg(&mut self) -> PgResult<i32> {
        let varname = self.decl_varname()?;
        let dtype = self.decl_datatype()?;
        comp_seam::plpgsql_build_variable::call(&varname.name, varname.lineno, *dtype, true)
    }

    // decl_is_for : K_IS | K_FOR
    fn decl_is_for(&mut self) -> PgResult<()> {
        let tok = self.yylex()?;
        if tok != K_IS && tok != K_FOR {
            return Err(self.yyerror("syntax error, expected \"IS\" or \"FOR\""));
        }
        Ok(())
    }

    // decl_aliasitem : T_WORD | unreserved_keyword | T_CWORD
    fn decl_aliasitem(&mut self) -> PgResult<PLpgSQL_nsitem> {
        let tok = self.yylex()?;
        let loc = self.yylloc;
        let names: Vec<String> = if tok == T_WORD {
            vec![self
                .yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("decl_aliasitem: T_WORD without word payload"))?
                .ident
                .clone()]
        } else if scanner::plpgsql_token_is_unreserved_keyword(tok) {
            vec![self.yylval.keyword.clone().unwrap_or_default()]
        } else if tok == T_CWORD {
            self.yylval
                .cword
                .as_ref()
                .ok_or_else(|| internal_error("decl_aliasitem: T_CWORD without cword payload"))?
                .idents
                .clone()
        } else {
            return Err(self.yyerror("syntax error"));
        };
        // `plpgsql_ns_lookup(plpgsql_ns_top(), false, names..., NULL)` — a
        // pl_funcs.c lookup, reachable directly (no cycle).
        match funcs::plpgsql_ns_lookup_alias_snapshot(&names) {
            Some(nsi) => Ok(nsi),
            None => Err(self.variable_does_not_exist(&names.join("."), loc)),
        }
    }

    // decl_varname : T_WORD | unreserved_keyword
    fn decl_varname(&mut self) -> PgResult<VarName> {
        let tok = self.yylex()?;
        let loc = self.yylloc;
        let name = if tok == T_WORD {
            self.yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("decl_varname: T_WORD without word payload"))?
                .ident
                .clone()
        } else if scanner::plpgsql_token_is_unreserved_keyword(tok) {
            self.yylval.keyword.clone().unwrap_or_default()
        } else {
            return Err(self.yyerror("syntax error"));
        };
        let lineno = self.loc_to_lineno(loc);

        if funcs::plpgsql_ns_lookup_local(&name) {
            return Err(self.yyerror_at(loc, "duplicate declaration"));
        }
        match comp_seam::plpgsql_check_shadowvar::call(&name) {
            comp_seam::ShadowVarAction::None => {}
            comp_seam::ShadowVarAction::Warning => {
                self.emit_shadowvar(&name, loc, false)?;
            }
            comp_seam::ShadowVarAction::Error => {
                return Err(self.shadowvar_error(&name, loc));
            }
        }

        Ok(VarName { name, lineno })
    }

    // decl_const : | K_CONSTANT
    fn decl_const(&mut self) -> PgResult<bool> {
        let tok = self.yylex()?;
        if tok == K_CONSTANT {
            Ok(true)
        } else {
            self.push_back_token(tok)?;
            Ok(false)
        }
    }

    // decl_datatype : /* empty */ { read_datatype(YYEMPTY) }
    fn decl_datatype(&mut self) -> PgResult<Box<PLpgSQL_type>> {
        self.read_datatype(YYEMPTY)
    }

    // decl_collate : | K_COLLATE (T_WORD | unreserved_keyword | T_CWORD)
    fn decl_collate(&mut self) -> PgResult<(Oid, i32)> {
        let tok = self.yylex()?;
        if tok != K_COLLATE {
            self.push_back_token(tok)?;
            return Ok((INVALID_OID, -1));
        }
        let tok2 = self.yylex()?;
        let collate_loc = self.yylloc; // @4 — the COLLATE-name location
        let names: Vec<String> = if tok2 == T_WORD {
            vec![self
                .yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("decl_collate: T_WORD without word payload"))?
                .ident
                .clone()]
        } else if scanner::plpgsql_token_is_unreserved_keyword(tok2) {
            vec![self.yylval.keyword.clone().unwrap_or_default()]
        } else if tok2 == T_CWORD {
            self.yylval
                .cword
                .as_ref()
                .ok_or_else(|| internal_error("decl_collate: T_CWORD without cword payload"))?
                .idents
                .clone()
        } else {
            return Err(self.yyerror("syntax error"));
        };
        let oid = comp_seam::get_collation_oid::call(&names, false)?;
        Ok((oid, collate_loc))
    }

    // decl_notnull : | K_NOT K_NULL  (returns (notnull, @5 location))
    fn decl_notnull(&mut self) -> PgResult<(bool, i32)> {
        let tok = self.yylex()?;
        if tok != K_NOT {
            self.push_back_token(tok)?;
            return Ok((false, -1));
        }
        let loc = self.yylloc; // @5 — the NOT location
        let tok2 = self.yylex()?;
        if tok2 != K_NULL {
            return Err(self.yyerror("syntax error, expected \"NULL\""));
        }
        Ok((true, loc))
    }

    // decl_defval : ';' | decl_defkey read_sql_expression(';', ";")
    fn decl_defval(&mut self) -> PgResult<Option<Box<PLpgSQL_expr>>> {
        let tok = self.yylex()?;
        if tok == (';' as i32) {
            return Ok(None);
        }
        if tok == ('=' as i32) || tok == COLON_EQUALS || tok == K_DEFAULT {
            return Ok(Some(self.read_sql_expression(';' as i32, ";")?));
        }
        Err(self.yyerror("syntax error"))
    }
}

impl<'mcx> Parser<'mcx> {
    // -----------------------------------------------------------------------
    // proc_sect : /* empty */ | proc_sect proc_stmt
    // -----------------------------------------------------------------------
    fn proc_sect(&mut self) -> PgResult<Vec<PLpgSQL_stmt>> {
        let mut stmts = Vec::new();
        loop {
            let tok = self.yylex()?;
            self.push_back_token(tok)?;
            if tok == 0
                || tok == K_END
                || tok == K_EXCEPTION
                || tok == K_ELSIF
                || tok == K_ELSE
                || tok == K_WHEN
            {
                break;
            }
            if let Some(stmt) = self.proc_stmt()? {
                mem::vpush(&mut stmts, stmt);
            }
        }
        Ok(stmts)
    }

    // -----------------------------------------------------------------------
    // proc_stmt — dispatch on the leading token. None for stmt_null.
    // -----------------------------------------------------------------------
    fn proc_stmt(&mut self) -> PgResult<Option<PLpgSQL_stmt>> {
        let tok = self.yylex()?;

        // A leading `<<label>>` introduces either a labeled loop statement or a
        // labeled block. Read the label and peek past `>>` to dispatch.
        if tok == LESS_LESS {
            let label_loc = self.yylloc;
            let id = self.any_identifier()?;
            let gg = self.yylex()?;
            if gg != GREATER_GREATER {
                return Err(self.yyerror("syntax error, expected \">>\""));
            }
            let after = self.yylex()?;
            self.push_back_token(after)?;
            let stmt = if after == K_LOOP {
                funcs::plpgsql_ns_push(Some(&id), PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
                self.stmt_loop(Some(id), label_loc)?
            } else if after == K_WHILE {
                funcs::plpgsql_ns_push(Some(&id), PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
                self.stmt_while(Some(id), label_loc)?
            } else if after == K_FOR {
                funcs::plpgsql_ns_push(Some(&id), PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
                self.stmt_for(Some(id), label_loc)?
            } else if after == K_FOREACH {
                funcs::plpgsql_ns_push(Some(&id), PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
                self.stmt_foreach_a(Some(id), label_loc)?
            } else {
                // Labeled block.
                let block = self.pl_block_labeled(Some((id, label_loc)))?;
                let semi = self.yylex()?;
                if semi != (';' as i32) {
                    return Err(self.yyerror("syntax error, expected \";\""));
                }
                block
            };
            return Ok(Some(stmt));
        }

        // pl_block ';' — a nested block beginning with K_DECLARE or K_BEGIN.
        if tok == K_DECLARE || tok == K_BEGIN {
            self.push_back_token(tok)?;
            let block = self.pl_block()?;
            let semi = self.yylex()?;
            if semi != (';' as i32) {
                return Err(self.yyerror("syntax error, expected \";\""));
            }
            return Ok(Some(block));
        }

        let stmt = if tok == T_DATUM {
            self.push_back_token(tok)?;
            self.stmt_assign()?
        } else if tok == K_IF {
            self.push_back_token(tok)?;
            self.stmt_if()?
        } else if tok == K_CASE {
            self.push_back_token(tok)?;
            self.stmt_case()?
        } else if tok == K_LOOP {
            self.push_back_token(tok)?;
            funcs::plpgsql_ns_push(None, PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
            self.stmt_loop(None, -1)?
        } else if tok == K_WHILE {
            self.push_back_token(tok)?;
            funcs::plpgsql_ns_push(None, PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
            self.stmt_while(None, -1)?
        } else if tok == K_FOR {
            self.push_back_token(tok)?;
            funcs::plpgsql_ns_push(None, PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
            self.stmt_for(None, -1)?
        } else if tok == K_FOREACH {
            self.push_back_token(tok)?;
            funcs::plpgsql_ns_push(None, PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
            self.stmt_foreach_a(None, -1)?
        } else if tok == K_EXIT || tok == K_CONTINUE {
            self.push_back_token(tok)?;
            self.stmt_exit()?
        } else if tok == K_RETURN {
            self.push_back_token(tok)?;
            self.stmt_return()?
        } else if tok == K_RAISE {
            self.push_back_token(tok)?;
            self.stmt_raise()?
        } else if tok == K_ASSERT {
            self.push_back_token(tok)?;
            self.stmt_assert()?
        } else if tok == K_IMPORT || tok == K_INSERT || tok == K_MERGE || tok == T_WORD || tok == T_CWORD {
            self.push_back_token(tok)?;
            self.stmt_execsql()?
        } else if tok == K_EXECUTE {
            self.push_back_token(tok)?;
            self.stmt_dynexecute()?
        } else if tok == K_PERFORM {
            self.push_back_token(tok)?;
            self.stmt_perform()?
        } else if tok == K_CALL || tok == K_DO {
            self.push_back_token(tok)?;
            self.stmt_call()?
        } else if tok == K_GET {
            self.push_back_token(tok)?;
            self.stmt_getdiag()?
        } else if tok == K_OPEN {
            self.push_back_token(tok)?;
            self.stmt_open()?
        } else if tok == K_FETCH {
            self.push_back_token(tok)?;
            self.stmt_fetch()?
        } else if tok == K_MOVE {
            self.push_back_token(tok)?;
            self.stmt_move()?
        } else if tok == K_CLOSE {
            self.push_back_token(tok)?;
            self.stmt_close()?
        } else if tok == K_NULL {
            self.push_back_token(tok)?;
            return Ok(self.stmt_null()?);
        } else if tok == K_COMMIT {
            self.push_back_token(tok)?;
            self.stmt_commit()?
        } else if tok == K_ROLLBACK {
            self.push_back_token(tok)?;
            self.stmt_rollback()?
        } else {
            return Err(self.yyerror("syntax error"));
        };
        Ok(Some(stmt))
    }

    // stmt_perform : K_PERFORM <read SELECT-substituted expr>
    fn stmt_perform(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_PERFORM);
        let loc = self.yylloc;
        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        self.push_back_token(K_PERFORM)?;

        let (mut expr, startloc, _endtoken) =
            self.read_sql_construct(';' as i32, 0, 0, ";", RawParseMode::RAW_PARSE_DEFAULT, false, false)?;

        comp_seam::perform_rewrite_query::call(&mut expr);
        self.check_sql_expr(&expr.query, expr.parseMode, startloc + 1)?;

        let new = PLpgSQL_stmt_perform {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_PERFORM,
            lineno,
            stmtid,
            expr: Some(expr),
        };
        Ok(PLpgSQL_stmt::Perform(mem::boxed(new)))
    }

    // stmt_call : K_CALL | K_DO
    fn stmt_call(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        let loc = self.yylloc;
        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let is_call = tok == K_CALL;
        debug_assert!(tok == K_CALL || tok == K_DO);

        self.push_back_token(tok)?;
        let expr = self.read_sql_stmt()?;

        comp_seam::curr_compile_set_requires_procedure_resowner::call();

        let new = PLpgSQL_stmt_call {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_CALL,
            lineno,
            stmtid,
            expr: Some(expr),
            is_call,
            target: None,
        };
        Ok(PLpgSQL_stmt::Call(mem::boxed(new)))
    }

    // stmt_assign : T_DATUM <read assign-mode construct ';'>
    fn stmt_assign(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == T_DATUM);
        let loc = self.yylloc;

        let wdatum = self
            .yylval
            .wdatum
            .clone()
            .ok_or_else(|| internal_error("stmt_assign: T_DATUM without wdatum payload"))?;

        let nnames = if wdatum.ident.is_some() { 1 } else { wdatum.idents.len() };
        let pmode = match nnames {
            1 => RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN1,
            2 => RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN2,
            3 => RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN3,
            _ => return Err(internal_error("unexpected number of names")),
        };

        let datum_dno = wdatum
            .datum
            .ok_or_else(|| internal_error("stmt_assign: T_DATUM without datum dno"))? as i32;
        comp_seam::check_assignable::call(datum_dno, loc)?;

        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();
        let varno = datum_dno;

        self.push_back_token(T_DATUM)?;
        let (mut expr, _startloc, _endtoken) =
            self.read_sql_construct(';' as i32, 0, 0, ";", pmode, false, true)?;
        comp_seam::mark_expr_as_assignment_source::call(&mut expr, datum_dno);

        let new = PLpgSQL_stmt_assign {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_ASSIGN,
            lineno,
            stmtid,
            varno,
            expr: Some(expr),
        };
        Ok(PLpgSQL_stmt::Assign(mem::boxed(new)))
    }
}

impl<'mcx> Parser<'mcx> {
    // stmt_getdiag : K_GET getdiag_area_opt K_DIAGNOSTICS getdiag_list ';'
    fn stmt_getdiag(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_GET);
        let loc = self.yylloc;

        let is_stacked = self.getdiag_area_opt()?;

        let tok2 = self.yylex()?;
        if tok2 != K_DIAGNOSTICS {
            return Err(self.yyerror("syntax error, expected \"DIAGNOSTICS\""));
        }
        let diag_items = self.getdiag_list()?;
        let tok3 = self.yylex()?;
        if tok3 != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }

        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        for ditem in &diag_items {
            match ditem.kind {
                PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ROW_COUNT
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ROUTINE_OID => {
                    if is_stacked {
                        return Err(self.getdiag_invalid(ditem.kind, true, loc));
                    }
                }
                PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ERROR_CONTEXT
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ERROR_DETAIL
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ERROR_HINT
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_RETURNED_SQLSTATE
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_COLUMN_NAME
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_CONSTRAINT_NAME
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_DATATYPE_NAME
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_MESSAGE_TEXT
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_TABLE_NAME
                | PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_SCHEMA_NAME => {
                    if !is_stacked {
                        return Err(self.getdiag_invalid(ditem.kind, false, loc));
                    }
                }
                PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_CONTEXT => {}
            }
        }

        let new = PLpgSQL_stmt_getdiag {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_GETDIAG,
            lineno,
            stmtid,
            is_stacked,
            diag_items,
        };
        Ok(PLpgSQL_stmt::Getdiag(mem::boxed(new)))
    }

    // getdiag_area_opt : | K_CURRENT | K_STACKED
    fn getdiag_area_opt(&mut self) -> PgResult<bool> {
        let tok = self.yylex()?;
        if tok == K_CURRENT {
            Ok(false)
        } else if tok == K_STACKED {
            Ok(true)
        } else {
            self.push_back_token(tok)?;
            Ok(false)
        }
    }

    // getdiag_list : getdiag_list_item | getdiag_list ',' getdiag_list_item
    fn getdiag_list(&mut self) -> PgResult<Vec<PLpgSQL_diag_item>> {
        let mut list = Vec::new();
        let it = self.getdiag_list_item()?;
        mem::vpush(&mut list, it);
        loop {
            let tok = self.yylex()?;
            if tok != (',' as i32) {
                self.push_back_token(tok)?;
                break;
            }
            let it = self.getdiag_list_item()?;
            mem::vpush(&mut list, it);
        }
        Ok(list)
    }

    // getdiag_list_item : getdiag_target assign_operator getdiag_item
    fn getdiag_list_item(&mut self) -> PgResult<PLpgSQL_diag_item> {
        let target = self.getdiag_target()?;
        let tok = self.yylex()?;
        if tok != ('=' as i32) && tok != COLON_EQUALS {
            return Err(self.yyerror("syntax error, expected \"=\""));
        }
        let kind = self.getdiag_item()?;
        Ok(PLpgSQL_diag_item { kind, target })
    }

    // getdiag_item : the tok_is_keyword ladder
    fn getdiag_item(&mut self) -> PgResult<PLpgSQL_getdiag_kind> {
        let tok = self.yylex()?;
        let kind = if self.tok_is_keyword(tok, K_ROW_COUNT, "row_count") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ROW_COUNT
        } else if self.tok_is_keyword(tok, K_PG_ROUTINE_OID, "pg_routine_oid") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ROUTINE_OID
        } else if self.tok_is_keyword(tok, K_PG_CONTEXT, "pg_context") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_CONTEXT
        } else if self.tok_is_keyword(tok, K_PG_EXCEPTION_DETAIL, "pg_exception_detail") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ERROR_DETAIL
        } else if self.tok_is_keyword(tok, K_PG_EXCEPTION_HINT, "pg_exception_hint") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ERROR_HINT
        } else if self.tok_is_keyword(tok, K_PG_EXCEPTION_CONTEXT, "pg_exception_context") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_ERROR_CONTEXT
        } else if self.tok_is_keyword(tok, K_COLUMN_NAME, "column_name") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_COLUMN_NAME
        } else if self.tok_is_keyword(tok, K_CONSTRAINT_NAME, "constraint_name") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_CONSTRAINT_NAME
        } else if self.tok_is_keyword(tok, K_PG_DATATYPE_NAME, "pg_datatype_name") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_DATATYPE_NAME
        } else if self.tok_is_keyword(tok, K_MESSAGE_TEXT, "message_text") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_MESSAGE_TEXT
        } else if self.tok_is_keyword(tok, K_TABLE_NAME, "table_name") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_TABLE_NAME
        } else if self.tok_is_keyword(tok, K_SCHEMA_NAME, "schema_name") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_SCHEMA_NAME
        } else if self.tok_is_keyword(tok, K_RETURNED_SQLSTATE, "returned_sqlstate") {
            PLpgSQL_getdiag_kind::PLPGSQL_GETDIAG_RETURNED_SQLSTATE
        } else {
            return Err(self.yyerror("unrecognized GET DIAGNOSTICS item"));
        };
        Ok(kind)
    }

    // getdiag_target : T_DATUM | T_WORD | T_CWORD  (returns target dno)
    fn getdiag_target(&mut self) -> PgResult<i32> {
        let tok = self.yylex()?;
        let loc = self.yylloc;
        if tok == T_DATUM {
            let wdatum = self
                .yylval
                .wdatum
                .clone()
                .ok_or_else(|| internal_error("getdiag_target: T_DATUM without wdatum payload"))?;
            let datum_dno = wdatum
                .datum
                .ok_or_else(|| internal_error("getdiag_target: T_DATUM without dno"))? as i32;
            let dtype = comp_seam::datum_dtype::call(datum_dno);
            if dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW
                || dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_REC
                || self.peek()? == ('[' as i32)
            {
                return Err(self.not_scalar_variable(&name_of_datum(&wdatum), loc));
            }
            comp_seam::check_assignable::call(datum_dno, loc)?;
            Ok(datum_dno)
        } else if tok == T_WORD {
            let ident = self
                .yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("getdiag_target: T_WORD without word payload"))?
                .ident
                .clone();
            Err(self.word_is_not_variable(&ident, loc))
        } else if tok == T_CWORD {
            let idents = self
                .yylval
                .cword
                .as_ref()
                .ok_or_else(|| internal_error("getdiag_target: T_CWORD without cword payload"))?
                .idents
                .join(".");
            Err(self.cword_is_not_variable(&idents, loc))
        } else {
            Err(self.yyerror("syntax error"))
        }
    }

    // stmt_if : K_IF expr_until_then proc_sect stmt_elsifs stmt_else K_END K_IF ';'
    fn stmt_if(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_IF);
        let loc = self.yylloc;

        let cond = self.expr_until_then()?;
        let then_body = self.proc_sect()?;
        let elsif_list = self.stmt_elsifs()?;
        let else_body = self.stmt_else()?;

        let tok2 = self.yylex()?;
        if tok2 != K_END {
            return Err(self.yyerror("syntax error, expected \"END\""));
        }
        let tok3 = self.yylex()?;
        if tok3 != K_IF {
            return Err(self.yyerror("syntax error, expected \"IF\""));
        }
        let tok4 = self.yylex()?;
        if tok4 != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }

        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let new = PLpgSQL_stmt_if {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_IF,
            lineno,
            stmtid,
            cond: Some(cond),
            then_body,
            elsif_list,
            else_body,
        };
        Ok(PLpgSQL_stmt::If(mem::boxed(new)))
    }

    // stmt_elsifs : | stmt_elsifs K_ELSIF expr_until_then proc_sect
    fn stmt_elsifs(&mut self) -> PgResult<Vec<PLpgSQL_if_elsif>> {
        let mut list = Vec::new();
        loop {
            let tok = self.yylex()?;
            if tok != K_ELSIF {
                self.push_back_token(tok)?;
                break;
            }
            let loc = self.yylloc;
            let cond = self.expr_until_then()?;
            let stmts = self.proc_sect()?;
            let lineno = self.loc_to_lineno(loc);
            mem::vpush(
                &mut list,
                PLpgSQL_if_elsif {
                    lineno,
                    cond: Some(cond),
                    stmts,
                },
            );
        }
        Ok(list)
    }

    // stmt_else : | K_ELSE proc_sect
    fn stmt_else(&mut self) -> PgResult<Vec<PLpgSQL_stmt>> {
        let tok = self.yylex()?;
        if tok != K_ELSE {
            self.push_back_token(tok)?;
            return Ok(Vec::new());
        }
        self.proc_sect()
    }

    // stmt_loop : opt_loop_label K_LOOP loop_body
    fn stmt_loop(&mut self, label: Option<String>, _label_loc: i32) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_LOOP);
        let loc = self.yylloc;
        let (stmts, end_label, end_label_loc) = self.loop_body()?;

        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        self.check_labels(label.as_deref(), end_label.as_deref(), end_label_loc)?;
        funcs::plpgsql_ns_pop();

        let new = PLpgSQL_stmt_loop {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_LOOP,
            lineno,
            stmtid,
            label,
            body: stmts,
        };
        Ok(PLpgSQL_stmt::Loop(mem::boxed(new)))
    }

    // stmt_while : opt_loop_label K_WHILE expr_until_loop loop_body
    fn stmt_while(&mut self, label: Option<String>, _label_loc: i32) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_WHILE);
        let loc = self.yylloc;
        let cond = self.expr_until_loop()?;
        let (stmts, end_label, end_label_loc) = self.loop_body()?;

        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        self.check_labels(label.as_deref(), end_label.as_deref(), end_label_loc)?;
        funcs::plpgsql_ns_pop();

        let new = PLpgSQL_stmt_while {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_WHILE,
            lineno,
            stmtid,
            label,
            cond: Some(cond),
            body: stmts,
        };
        Ok(PLpgSQL_stmt::While(mem::boxed(new)))
    }
}

impl<'mcx> Parser<'mcx> {
    // stmt_for : opt_loop_label K_FOR for_control loop_body
    fn stmt_for(&mut self, label: Option<String>, _label_loc: i32) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_FOR);
        let for_loc = self.yylloc;
        let control = self.for_control()?;
        let (stmts, end_label, end_label_loc) = self.loop_body()?;

        let lineno = self.loc_to_lineno(for_loc);

        let stmt = match control {
            PLpgSQL_stmt::Fori(mut new) => {
                new.lineno = lineno;
                new.label = label.clone();
                new.body = stmts;
                PLpgSQL_stmt::Fori(new)
            }
            PLpgSQL_stmt::Fors(mut new) => {
                new.lineno = lineno;
                new.label = label.clone();
                new.body = stmts;
                PLpgSQL_stmt::Fors(new)
            }
            PLpgSQL_stmt::Forc(mut new) => {
                new.lineno = lineno;
                new.label = label.clone();
                new.body = stmts;
                PLpgSQL_stmt::Forc(new)
            }
            PLpgSQL_stmt::Dynfors(mut new) => {
                new.lineno = lineno;
                new.label = label.clone();
                new.body = stmts;
                PLpgSQL_stmt::Dynfors(new)
            }
            _ => return Err(internal_error("for_control must yield a FOR* statement")),
        };

        self.check_labels(label.as_deref(), end_label.as_deref(), end_label_loc)?;
        funcs::plpgsql_ns_pop();

        Ok(stmt)
    }

    // for_control : for_variable K_IN <complex disambiguation>
    fn for_control(&mut self) -> PgResult<PLpgSQL_stmt> {
        let forvar = self.for_variable()?;
        let fv_loc = forvar.location;

        let tok = self.yylex()?;
        if tok != K_IN {
            return Err(self.yyerror("syntax error, expected \"IN\""));
        }

        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let tok = self.yylex()?;
        let tokloc = self.yylloc;

        if tok == K_EXECUTE {
            // dynamic FOR loop
            let (expr, term) = self.read_sql_expression2(K_LOOP, K_USING, "LOOP or USING")?;
            let var = self.forvar_to_row_or_rec(&forvar, fv_loc)?;

            let mut params: Vec<PLpgSQL_expr> = Vec::new();
            if term == K_USING {
                loop {
                    let (expr2, term2) =
                        self.read_sql_expression2(',' as i32, K_LOOP, ", or LOOP")?;
                    mem::vpush(&mut params, *expr2);
                    if term2 != (',' as i32) {
                        break;
                    }
                }
            }

            let new = PLpgSQL_stmt_dynfors {
                cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_DYNFORS,
                lineno: 0,
                stmtid,
                label: None,
                var,
                body: Vec::new(),
                query: Some(expr),
                params,
            };
            return Ok(PLpgSQL_stmt::Dynfors(mem::boxed(new)));
        }

        if tok == T_DATUM {
            let wdatum = self
                .yylval
                .wdatum
                .clone()
                .ok_or_else(|| internal_error("for_control: T_DATUM without wdatum payload"))?;
            let datum_dno = wdatum
                .datum
                .ok_or_else(|| internal_error("for_control: T_DATUM without dno"))? as i32;
            if comp_seam::datum_dtype::call(datum_dno) == PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
                && comp_seam::var_datatype_typoid::call(datum_dno) == REFCURSOROID
            {
                // FOR var IN cursor
                let curvar = datum_dno;

                if forvar.scalar.is_some() && forvar.row.is_some() {
                    return Err(
                        self.syntax_at("cursor FOR loop must have only one target variable", fv_loc)
                    );
                }
                if !comp_seam::var_has_explicit_expr::call(curvar) {
                    return Err(self.syntax_at(
                        "cursor FOR loop must use a bound cursor variable",
                        tokloc,
                    ));
                }

                let argquery = self.read_cursor_args(curvar, K_LOOP)?;
                let var = comp_seam::plpgsql_build_record_for_loop::call(
                    &forvar.name,
                    forvar.lineno,
                )?;

                let new = PLpgSQL_stmt_forc {
                    cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_FORC,
                    lineno: 0,
                    stmtid,
                    label: None,
                    var,
                    body: Vec::new(),
                    curvar,
                    argquery,
                };
                return Ok(PLpgSQL_stmt::Forc(mem::boxed(new)));
            }
            self.handle_for_int_or_query(tok, tokloc, forvar, fv_loc, stmtid)
        } else {
            self.handle_for_int_or_query(tok, tokloc, forvar, fv_loc, stmtid)
        }
    }

    /// Distinguish integer FOR loop (`FOR var IN a .. b`) from a query loop.
    fn handle_for_int_or_query(
        &mut self,
        tok: i32,
        tokloc: i32,
        forvar: ForVariable,
        fv_loc: i32,
        stmtid: u32,
    ) -> PgResult<PLpgSQL_stmt> {
        let mut reverse = false;
        if self.tok_is_keyword(tok, K_REVERSE, "reverse") {
            reverse = true;
        } else {
            self.push_back_token(tok)?;
        }

        let (mut expr1, expr1loc, endtok) = self.read_sql_construct(
            DOT_DOT,
            K_LOOP,
            0,
            "LOOP",
            RawParseMode::RAW_PARSE_DEFAULT,
            true,
            false,
        )?;

        if endtok == DOT_DOT {
            // integer loop
            expr1.parseMode = RawParseMode::RAW_PARSE_PLPGSQL_EXPR;
            self.check_sql_expr(&expr1.query, expr1.parseMode, expr1loc)?;

            let (expr2, tok2) = self.read_sql_expression2(K_LOOP, K_BY, "LOOP")?;

            let expr_by = if tok2 == K_BY {
                Some(self.read_sql_expression(K_LOOP, "LOOP")?)
            } else {
                None
            };

            if forvar.scalar.is_some() && forvar.row.is_some() {
                return Err(
                    self.syntax_at("integer FOR loop must have only one target variable", fv_loc)
                );
            }

            let fvar = comp_seam::plpgsql_build_int_loop_var::call(&forvar.name, forvar.lineno, INT4OID)?;

            let new = PLpgSQL_stmt_fori {
                cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_FORI,
                lineno: 0,
                stmtid,
                label: None,
                var: Some(mem::boxed(fvar)),
                lower: Some(expr1),
                upper: Some(expr2),
                step: expr_by,
                reverse: reverse as i32,
                body: Vec::new(),
            };
            Ok(PLpgSQL_stmt::Fori(mem::boxed(new)))
        } else {
            // query loop
            if reverse {
                return Err(self.syntax_at("cannot specify REVERSE in query FOR loop", tokloc));
            }
            self.check_sql_expr(&expr1.query, expr1.parseMode, expr1loc)?;

            let var = self.forvar_to_row_or_rec(&forvar, fv_loc)?;

            let new = PLpgSQL_stmt_fors {
                cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_FORS,
                lineno: 0,
                stmtid,
                label: None,
                var,
                body: Vec::new(),
                query: Some(expr1),
            };
            Ok(PLpgSQL_stmt::Fors(mem::boxed(new)))
        }
    }

    /// Shared ROW/REC-vs-scalar-list handling for the dynamic and query
    /// FOR-loop arms.
    fn forvar_to_row_or_rec(
        &mut self,
        forvar: &ForVariable,
        fv_loc: i32,
    ) -> PgResult<Option<Box<PLpgSQL_variable>>> {
        if let Some(row_dno) = forvar.row {
            comp_seam::check_assignable::call(row_dno, fv_loc)?;
            Ok(Some(mem::boxed(comp_seam::datum_as_variable::call(row_dno))))
        } else if let Some(scalar_dno) = forvar.scalar {
            let row_dno =
                comp_seam::make_scalar_list1::call(&forvar.name, scalar_dno, forvar.lineno, fv_loc)?;
            Ok(Some(mem::boxed(comp_seam::datum_as_variable::call(row_dno))))
        } else {
            Err(self.syntax_at(
                "loop variable of loop over rows must be a record variable or list of scalar variables",
                fv_loc,
            ))
        }
    }

    // for_variable : T_DATUM | T_WORD | T_CWORD
    fn for_variable(&mut self) -> PgResult<ForVariable> {
        let tok = self.yylex()?;
        let loc = self.yylloc;
        if tok == T_DATUM {
            let wdatum = self
                .yylval
                .wdatum
                .clone()
                .ok_or_else(|| internal_error("for_variable: T_DATUM without wdatum payload"))?;
            let name = name_of_datum(&wdatum);
            let lineno = self.loc_to_lineno(loc);
            let datum_dno = wdatum
                .datum
                .ok_or_else(|| internal_error("for_variable: T_DATUM without dno"))? as i32;
            let dtype = comp_seam::datum_dtype::call(datum_dno);
            if dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW
                || dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_REC
            {
                Ok(ForVariable {
                    name,
                    lineno,
                    location: loc,
                    scalar: None,
                    row: Some(datum_dno),
                })
            } else {
                let mut row = None;
                let tok2 = self.yylex()?;
                self.push_back_token(tok2)?;
                if tok2 == (',' as i32) {
                    row = Some(self.read_into_scalar_list(&name, datum_dno, loc)?);
                }
                Ok(ForVariable {
                    name,
                    lineno,
                    location: loc,
                    scalar: Some(datum_dno),
                    row,
                })
            }
        } else if tok == T_WORD {
            let word = self
                .yylval
                .word
                .clone()
                .ok_or_else(|| internal_error("for_variable: T_WORD without word payload"))?;
            let name = word.ident.clone();
            let lineno = self.loc_to_lineno(loc);
            let tok2 = self.yylex()?;
            self.push_back_token(tok2)?;
            if tok2 == (',' as i32) {
                return Err(self.word_is_not_variable(&word.ident, loc));
            }
            Ok(ForVariable {
                name,
                lineno,
                location: loc,
                scalar: None,
                row: None,
            })
        } else if tok == T_CWORD {
            let idents = self
                .yylval
                .cword
                .as_ref()
                .ok_or_else(|| internal_error("for_variable: T_CWORD without cword payload"))?
                .idents
                .join(".");
            Err(self.cword_is_not_variable(&idents, loc))
        } else {
            Err(self.yyerror("syntax error"))
        }
    }

    // stmt_foreach_a : opt_loop_label K_FOREACH for_variable foreach_slice
    //                  K_IN K_ARRAY expr_until_loop loop_body
    fn stmt_foreach_a(&mut self, label: Option<String>, _label_loc: i32) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_FOREACH);
        let foreach_loc = self.yylloc;

        let forvar = self.for_variable()?;
        let fv_loc = forvar.location;
        let slice = self.foreach_slice()?;

        let tok2 = self.yylex()?;
        if tok2 != K_IN {
            return Err(self.yyerror("syntax error, expected \"IN\""));
        }
        let tok3 = self.yylex()?;
        if tok3 != K_ARRAY {
            return Err(self.yyerror("syntax error, expected \"ARRAY\""));
        }
        let expr = self.expr_until_loop()?;
        let (stmts, end_label, end_label_loc) = self.loop_body()?;

        let lineno = self.loc_to_lineno(foreach_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let varno = if let Some(row_dno) = forvar.row {
            comp_seam::check_assignable::call(row_dno, fv_loc)?;
            row_dno
        } else if let Some(scalar_dno) = forvar.scalar {
            comp_seam::check_assignable::call(scalar_dno, fv_loc)?;
            scalar_dno
        } else {
            return Err(self.syntax_at(
                "loop variable of FOREACH must be a known variable or list of variables",
                fv_loc,
            ));
        };

        self.check_labels(label.as_deref(), end_label.as_deref(), end_label_loc)?;
        funcs::plpgsql_ns_pop();

        let new = PLpgSQL_stmt_foreach_a {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_FOREACH_A,
            lineno,
            stmtid,
            label,
            varno,
            slice,
            expr: Some(expr),
            body: stmts,
        };
        Ok(PLpgSQL_stmt::ForeachA(mem::boxed(new)))
    }

    // foreach_slice : | K_SLICE ICONST
    fn foreach_slice(&mut self) -> PgResult<i32> {
        let tok = self.yylex()?;
        if tok != K_SLICE {
            self.push_back_token(tok)?;
            return Ok(0);
        }
        let tok2 = self.yylex()?;
        if tok2 != ICONST {
            return Err(self.yyerror("syntax error, expected integer"));
        }
        Ok(self.yylval.ival)
    }
}

impl<'mcx> Parser<'mcx> {
    // stmt_exit : exit_type opt_label opt_exitcond
    fn stmt_exit(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        let exit_loc = self.yylloc;
        let is_exit = if tok == K_EXIT {
            true
        } else if tok == K_CONTINUE {
            false
        } else {
            return Err(self.yyerror("syntax error"));
        };

        let (label, label_loc) = self.opt_label()?;
        let cond = self.opt_exitcond()?;

        let lineno = self.loc_to_lineno(exit_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        if let Some(ref lbl) = label {
            match funcs::plpgsql_ns_lookup_label_itemno(lbl) {
                Some(itemno) => {
                    // CONTINUE only allows loop labels.
                    if itemno != PLpgSQL_label_type::PLPGSQL_LABEL_LOOP as i32 && !is_exit {
                        return Err(self.syntax_at(
                            &format!("block label \"{lbl}\" cannot be used in CONTINUE"),
                            label_loc,
                        ));
                    }
                }
                None => {
                    return Err(self.syntax_at(
                        &format!(
                            "there is no label \"{lbl}\" attached to any block or loop enclosing this statement"
                        ),
                        label_loc,
                    ));
                }
            }
        } else if !funcs::plpgsql_ns_has_nearest_loop() {
            let msg = if is_exit {
                "EXIT cannot be used outside a loop, unless it has a label"
            } else {
                "CONTINUE cannot be used outside a loop"
            };
            return Err(self.syntax_at(msg, exit_loc));
        }

        let new = PLpgSQL_stmt_exit {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_EXIT,
            lineno,
            stmtid,
            is_exit,
            label,
            cond,
        };
        Ok(PLpgSQL_stmt::Exit(mem::boxed(new)))
    }

    // stmt_return : K_RETURN <RETURN | RETURN NEXT | RETURN QUERY>
    fn stmt_return(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_RETURN);
        let ret_loc = self.yylloc;

        let tok = self.yylex()?;
        if tok == 0 {
            return Err(self.yyerror("unexpected end of function definition"));
        }
        if self.tok_is_keyword(tok, K_NEXT, "next") {
            self.make_return_next_stmt(ret_loc)
        } else if self.tok_is_keyword(tok, K_QUERY, "query") {
            self.make_return_query_stmt(ret_loc)
        } else {
            self.push_back_token(tok)?;
            self.make_return_stmt(ret_loc)
        }
    }

    // stmt_raise : K_RAISE ...
    fn stmt_raise(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_RAISE);
        let raise_loc = self.yylloc;

        let lineno = self.loc_to_lineno(raise_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let mut elog_level = ERROR;
        let mut condname: Option<String> = None;
        let mut message: Option<String> = None;
        let mut params: Vec<PLpgSQL_expr> = Vec::new();
        let mut options: Vec<PLpgSQL_raise_option> = Vec::new();

        let mut tok = self.yylex()?;
        if tok == 0 {
            return Err(self.yyerror("unexpected end of function definition"));
        }

        if tok != (';' as i32) {
            // optional elog severity level
            if self.tok_is_keyword(tok, K_EXCEPTION, "exception") {
                elog_level = ERROR;
                tok = self.yylex()?;
            } else if self.tok_is_keyword(tok, K_WARNING, "warning") {
                elog_level = WARNING;
                tok = self.yylex()?;
            } else if self.tok_is_keyword(tok, K_NOTICE, "notice") {
                elog_level = NOTICE;
                tok = self.yylex()?;
            } else if self.tok_is_keyword(tok, K_INFO, "info") {
                elog_level = INFO;
                tok = self.yylex()?;
            } else if self.tok_is_keyword(tok, K_LOG, "log") {
                elog_level = LOG;
                tok = self.yylex()?;
            } else if self.tok_is_keyword(tok, K_DEBUG, "debug") {
                elog_level = DEBUG1;
                tok = self.yylex()?;
            }
            if tok == 0 {
                return Err(self.yyerror("unexpected end of function definition"));
            }

            if tok == SCONST {
                // old style message and parameters
                message = self.yylval.str.clone();
                tok = self.yylex()?;
                if tok != (',' as i32) && tok != (';' as i32) && tok != K_USING {
                    return Err(self.yyerror("syntax error"));
                }
                while tok == (',' as i32) {
                    let (expr, t) = self.read_sql_construct_endtok(
                        ',' as i32,
                        ';' as i32,
                        K_USING,
                        ", or ; or USING",
                        RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
                        true,
                        true,
                    )?;
                    mem::vpush(&mut params, *expr);
                    tok = t;
                }
            } else if tok != K_USING {
                // condition name or SQLSTATE
                if self.tok_is_keyword(tok, K_SQLSTATE, "sqlstate") {
                    let t = self.yylex()?;
                    if t != SCONST {
                        return Err(self.yyerror("syntax error"));
                    }
                    let sqlstatestr = self.yylval.str.clone().unwrap_or_default();
                    if sqlstatestr.len() != 5 || !is_valid_sqlstate(&sqlstatestr) {
                        return Err(self.yyerror("invalid SQLSTATE code"));
                    }
                    condname = Some(sqlstatestr);
                } else {
                    if tok == T_WORD {
                        condname = Some(
                            self.yylval
                                .word
                                .as_ref()
                                .ok_or_else(|| internal_error("stmt_raise: T_WORD without word payload"))?
                                .ident
                                .clone(),
                        );
                    } else if scanner::plpgsql_token_is_unreserved_keyword(tok) {
                        condname = Some(self.yylval.keyword.clone().unwrap_or_default());
                    } else {
                        return Err(self.yyerror("syntax error"));
                    }
                    comp_seam::plpgsql_recognize_err_condition::call(
                        condname.as_deref().unwrap_or(""),
                        false,
                    )?;
                }
                tok = self.yylex()?;
                if tok != (';' as i32) && tok != K_USING {
                    return Err(self.yyerror("syntax error"));
                }
            }

            if tok == K_USING {
                options = self.read_raise_options()?;
            }
        }

        let new = PLpgSQL_stmt_raise {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_RAISE,
            lineno,
            stmtid,
            elog_level,
            condname,
            message,
            params,
            options,
        };
        self.check_raise_parameters(&new)?;
        Ok(PLpgSQL_stmt::Raise(mem::boxed(new)))
    }

    // stmt_assert : K_ASSERT <cond> [, <message>] ;
    fn stmt_assert(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_ASSERT);
        let assert_loc = self.yylloc;

        let lineno = self.loc_to_lineno(assert_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let (cond, endtok) = self.read_sql_expression2(',' as i32, ';' as i32, ", or ;")?;
        let message = if endtok == (',' as i32) {
            Some(self.read_sql_expression(';' as i32, ";")?)
        } else {
            None
        };

        let new = PLpgSQL_stmt_assert {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_ASSERT,
            lineno,
            stmtid,
            cond: Some(cond),
            message,
        };
        Ok(PLpgSQL_stmt::Assert(mem::boxed(new)))
    }

    // loop_body : proc_sect K_END K_LOOP opt_label ';'
    fn loop_body(&mut self) -> PgResult<(Vec<PLpgSQL_stmt>, Option<String>, i32)> {
        let stmts = self.proc_sect()?;
        let tok = self.yylex()?;
        if tok != K_END {
            return Err(self.yyerror("syntax error, expected \"END\""));
        }
        let tok2 = self.yylex()?;
        if tok2 != K_LOOP {
            return Err(self.yyerror("syntax error, expected \"LOOP\""));
        }
        let (end_label, end_label_loc) = self.opt_label()?;
        let tok3 = self.yylex()?;
        if tok3 != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }
        Ok((stmts, end_label, end_label_loc))
    }
}

impl<'mcx> Parser<'mcx> {
    // stmt_execsql : K_IMPORT | K_INSERT | K_MERGE | T_WORD | T_CWORD
    fn stmt_execsql(&mut self) -> PgResult<PLpgSQL_stmt> {
        let firsttoken = self.yylex()?;
        let location = self.yylloc;

        if firsttoken == K_IMPORT {
            self.make_execsql_stmt(K_IMPORT, location, None)
        } else if firsttoken == K_INSERT {
            self.make_execsql_stmt(K_INSERT, location, None)
        } else if firsttoken == K_MERGE {
            self.make_execsql_stmt(K_MERGE, location, None)
        } else if firsttoken == T_WORD {
            let word = self
                .yylval
                .word
                .clone()
                .ok_or_else(|| internal_error("stmt_execsql: T_WORD without word payload"))?;
            let tok = self.yylex()?;
            self.push_back_token(tok)?;
            if tok == ('=' as i32) || tok == COLON_EQUALS || tok == ('[' as i32) || tok == ('.' as i32) {
                return Err(self.word_is_not_variable(&word.ident, location));
            }
            self.make_execsql_stmt(T_WORD, location, Some(word))
        } else if firsttoken == T_CWORD {
            let cword = self
                .yylval
                .cword
                .clone()
                .ok_or_else(|| internal_error("stmt_execsql: T_CWORD without cword payload"))?;
            let tok = self.yylex()?;
            self.push_back_token(tok)?;
            if tok == ('=' as i32) || tok == COLON_EQUALS || tok == ('[' as i32) || tok == ('.' as i32) {
                return Err(self.cword_is_not_variable(&cword.idents.join("."), location));
            }
            self.make_execsql_stmt(T_CWORD, location, None)
        } else {
            Err(self.yyerror("syntax error"))
        }
    }

    // stmt_dynexecute : K_EXECUTE ...
    fn stmt_dynexecute(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_EXECUTE);
        let exec_loc = self.yylloc;

        let (expr, mut endtoken) = self.read_sql_construct_endtok(
            K_INTO,
            K_USING,
            ';' as i32,
            "INTO or USING or ;",
            RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
            true,
            true,
        )?;

        let lineno = self.loc_to_lineno(exec_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let mut into = false;
        let mut strict = false;
        let mut target: Option<Box<PLpgSQL_variable>> = None;
        let mut params: Vec<PLpgSQL_expr> = Vec::new();

        loop {
            if endtoken == K_INTO {
                if into {
                    return Err(self.yyerror("syntax error"));
                }
                into = true;
                let (t, s) = self.read_into_target(true)?;
                target = t;
                strict = s;
                endtoken = self.yylex()?;
            } else if endtoken == K_USING {
                if !params.is_empty() {
                    return Err(self.yyerror("syntax error"));
                }
                loop {
                    let (e, et) = self.read_sql_construct_endtok(
                        ',' as i32,
                        ';' as i32,
                        K_INTO,
                        ", or ; or INTO",
                        RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
                        true,
                        true,
                    )?;
                    mem::vpush(&mut params, *e);
                    endtoken = et;
                    if endtoken != (',' as i32) {
                        break;
                    }
                }
            } else if endtoken == (';' as i32) {
                break;
            } else {
                return Err(self.yyerror("syntax error"));
            }
        }

        let new = PLpgSQL_stmt_dynexecute {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_DYNEXECUTE,
            lineno,
            stmtid,
            query: Some(expr),
            into,
            strict,
            target,
            params,
        };
        Ok(PLpgSQL_stmt::Dynexecute(mem::boxed(new)))
    }

    // -----------------------------------------------------------------------
    // stmt_open : K_OPEN cursor_variable ...
    // -----------------------------------------------------------------------
    fn stmt_open(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_OPEN);
        let open_loc = self.yylloc;

        let curvar = self.cursor_variable()?;
        let lineno = self.loc_to_lineno(open_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();
        let mut cursor_options = CURSOR_OPT_FAST_PLAN;
        let mut argquery: Option<Box<PLpgSQL_expr>> = None;
        let mut query: Option<Box<PLpgSQL_expr>> = None;
        let mut dynquery: Option<Box<PLpgSQL_expr>> = None;
        let mut params: Vec<PLpgSQL_expr> = Vec::new();

        if !comp_seam::var_has_explicit_expr::call(curvar) {
            // be nice if we could use opt_scrollable here
            let mut tok = self.yylex()?;
            if self.tok_is_keyword(tok, K_NO, "no") {
                tok = self.yylex()?;
                if self.tok_is_keyword(tok, K_SCROLL, "scroll") {
                    cursor_options |= CURSOR_OPT_NO_SCROLL;
                    tok = self.yylex()?;
                }
            } else if self.tok_is_keyword(tok, K_SCROLL, "scroll") {
                cursor_options |= CURSOR_OPT_SCROLL;
                tok = self.yylex()?;
            }

            if tok != K_FOR {
                return Err(self.yyerror("syntax error, expected \"FOR\""));
            }

            tok = self.yylex()?;
            if tok == K_EXECUTE {
                let (dq, mut endtoken) =
                    self.read_sql_expression2(K_USING, ';' as i32, "USING or ;")?;
                dynquery = Some(dq);
                if endtoken == K_USING {
                    loop {
                        let (expr, et) =
                            self.read_sql_expression2(',' as i32, ';' as i32, ", or ;")?;
                        mem::vpush(&mut params, *expr);
                        endtoken = et;
                        if endtoken != (',' as i32) {
                            break;
                        }
                    }
                }
            } else {
                self.push_back_token(tok)?;
                query = Some(self.read_sql_stmt()?);
            }
        } else {
            // predefined cursor query, so read args
            argquery = self.read_cursor_args(curvar, ';' as i32)?;
        }

        let new = PLpgSQL_stmt_open {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_OPEN,
            lineno,
            stmtid,
            curvar,
            cursor_options,
            argquery,
            query,
            dynquery,
            params,
        };
        Ok(PLpgSQL_stmt::Open(mem::boxed(new)))
    }

    // stmt_fetch : K_FETCH opt_fetch_direction cursor_variable K_INTO ...
    fn stmt_fetch(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_FETCH);
        let fetch_loc = self.yylloc;

        let mut fetch = self.read_fetch_direction()?;
        let curvar = self.cursor_variable()?;

        let into_tok = self.yylex()?;
        if into_tok != K_INTO {
            return Err(self.yyerror("syntax error, expected \"INTO\""));
        }

        // strict ptr is NULL in C: STRICT is not accepted here.
        let (target, _strict) = self.read_into_target(false)?;

        if self.yylex()? != (';' as i32) {
            return Err(self.yyerror("syntax error"));
        }

        if fetch.returns_multiple_rows {
            return Err(self.fetch_multi_rows(fetch_loc));
        }

        let lineno = self.loc_to_lineno(fetch_loc);
        fetch.lineno = lineno;
        fetch.target = target;
        fetch.curvar = curvar;
        fetch.is_move = false;

        Ok(PLpgSQL_stmt::Fetch(fetch))
    }

    // stmt_move : K_MOVE opt_fetch_direction cursor_variable ';'
    fn stmt_move(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_MOVE);
        let move_loc = self.yylloc;

        let mut fetch = self.read_fetch_direction()?;
        let curvar = self.cursor_variable()?;

        if self.yylex()? != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }

        let lineno = self.loc_to_lineno(move_loc);
        fetch.lineno = lineno;
        fetch.curvar = curvar;
        fetch.is_move = true;

        Ok(PLpgSQL_stmt::Fetch(fetch))
    }

    // stmt_close : K_CLOSE cursor_variable ';'
    fn stmt_close(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_CLOSE);
        let close_loc = self.yylloc;

        let curvar = self.cursor_variable()?;
        if self.yylex()? != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }

        let lineno = self.loc_to_lineno(close_loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let new = PLpgSQL_stmt_close {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_CLOSE,
            lineno,
            stmtid,
            curvar,
        };
        Ok(PLpgSQL_stmt::Close(mem::boxed(new)))
    }

    /// `read_fetch_direction()` — fill in the direction fields of a FETCH/MOVE.
    fn read_fetch_direction(&mut self) -> PgResult<Box<PLpgSQL_stmt_fetch>> {
        let mut check_from = true;

        let mut fetch = Box::new(PLpgSQL_stmt_fetch {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_FETCH,
            lineno: 0,
            stmtid: comp_seam::curr_compile_next_stmtid::call(),
            target: None,
            curvar: 0,
            direction: FetchDirection::FETCH_FORWARD,
            how_many: 1,
            expr: None,
            is_move: false,
            returns_multiple_rows: false,
        });

        let tok = self.yylex()?;
        if tok == 0 {
            return Err(self.yyerror("unexpected end of function definition"));
        }

        if self.tok_is_keyword(tok, K_NEXT, "next") {
            // use defaults
        } else if self.tok_is_keyword(tok, K_PRIOR, "prior") {
            fetch.direction = FetchDirection::FETCH_BACKWARD;
        } else if self.tok_is_keyword(tok, K_FIRST, "first") {
            fetch.direction = FetchDirection::FETCH_ABSOLUTE;
        } else if self.tok_is_keyword(tok, K_LAST, "last") {
            fetch.direction = FetchDirection::FETCH_ABSOLUTE;
            fetch.how_many = -1;
        } else if self.tok_is_keyword(tok, K_ABSOLUTE, "absolute") {
            fetch.direction = FetchDirection::FETCH_ABSOLUTE;
            let (expr, _e) = self.read_sql_expression2(K_FROM, K_IN, "FROM or IN")?;
            fetch.expr = Some(expr);
            check_from = false;
        } else if self.tok_is_keyword(tok, K_RELATIVE, "relative") {
            fetch.direction = FetchDirection::FETCH_RELATIVE;
            let (expr, _e) = self.read_sql_expression2(K_FROM, K_IN, "FROM or IN")?;
            fetch.expr = Some(expr);
            check_from = false;
        } else if self.tok_is_keyword(tok, K_ALL, "all") {
            fetch.how_many = FETCH_ALL;
            fetch.returns_multiple_rows = true;
        } else if self.tok_is_keyword(tok, K_FORWARD, "forward") {
            self.complete_direction(&mut fetch, &mut check_from)?;
        } else if self.tok_is_keyword(tok, K_BACKWARD, "backward") {
            fetch.direction = FetchDirection::FETCH_BACKWARD;
            self.complete_direction(&mut fetch, &mut check_from)?;
        } else if tok == K_FROM || tok == K_IN {
            check_from = false;
        } else if tok == T_DATUM {
            // no direction clause; tok is the cursor name
            self.push_back_token(tok)?;
            check_from = false;
        } else {
            // count expression with no preceding keyword
            self.push_back_token(tok)?;
            let (expr, _e) = self.read_sql_expression2(K_FROM, K_IN, "FROM or IN")?;
            fetch.expr = Some(expr);
            fetch.returns_multiple_rows = true;
            check_from = false;
        }

        if check_from {
            let tok = self.yylex()?;
            if tok != K_FROM && tok != K_IN {
                return Err(self.yyerror("expected FROM or IN"));
            }
        }

        Ok(fetch)
    }

    /// `complete_direction(fetch, &check_FROM)` — remainder of a FORWARD /
    /// BACKWARD direction.
    fn complete_direction(
        &mut self,
        fetch: &mut PLpgSQL_stmt_fetch,
        check_from: &mut bool,
    ) -> PgResult<()> {
        let tok = self.yylex()?;
        if tok == 0 {
            return Err(self.yyerror("unexpected end of function definition"));
        }
        if tok == K_FROM || tok == K_IN {
            *check_from = false;
            return Ok(());
        }
        if tok == K_ALL {
            fetch.how_many = FETCH_ALL;
            fetch.returns_multiple_rows = true;
            *check_from = true;
            return Ok(());
        }
        self.push_back_token(tok)?;
        let (expr, _e) = self.read_sql_expression2(K_FROM, K_IN, "FROM or IN")?;
        fetch.expr = Some(expr);
        fetch.returns_multiple_rows = true;
        *check_from = false;
        Ok(())
    }

    // stmt_null : K_NULL ';'
    fn stmt_null(&mut self) -> PgResult<Option<PLpgSQL_stmt>> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_NULL);
        let tok2 = self.yylex()?;
        if tok2 != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }
        Ok(None)
    }

    // stmt_commit : K_COMMIT opt_transaction_chain ';'
    fn stmt_commit(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_COMMIT);
        let loc = self.yylloc;
        let chain = self.opt_transaction_chain()?;
        let tok2 = self.yylex()?;
        if tok2 != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }
        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();
        let new = PLpgSQL_stmt_commit {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_COMMIT,
            lineno,
            stmtid,
            chain,
        };
        Ok(PLpgSQL_stmt::Commit(mem::boxed(new)))
    }

    // stmt_rollback : K_ROLLBACK opt_transaction_chain ';'
    fn stmt_rollback(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_ROLLBACK);
        let loc = self.yylloc;
        let chain = self.opt_transaction_chain()?;
        let tok2 = self.yylex()?;
        if tok2 != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }
        let lineno = self.loc_to_lineno(loc);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();
        let new = PLpgSQL_stmt_rollback {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_ROLLBACK,
            lineno,
            stmtid,
            chain,
        };
        Ok(PLpgSQL_stmt::Rollback(mem::boxed(new)))
    }

    // opt_transaction_chain : K_AND K_CHAIN | K_AND K_NO K_CHAIN | /* EMPTY */
    fn opt_transaction_chain(&mut self) -> PgResult<bool> {
        let tok = self.yylex()?;
        if tok != K_AND {
            self.push_back_token(tok)?;
            return Ok(false);
        }
        let tok2 = self.yylex()?;
        if tok2 == K_CHAIN {
            Ok(true)
        } else if tok2 == K_NO {
            let tok3 = self.yylex()?;
            if tok3 != K_CHAIN {
                return Err(self.yyerror("syntax error, expected \"CHAIN\""));
            }
            Ok(false)
        } else {
            Err(self.yyerror("syntax error, expected \"CHAIN\" or \"NO CHAIN\""))
        }
    }

    // -----------------------------------------------------------------------
    // stmt_case : K_CASE opt_expr_until_when case_when_list opt_case_else
    //             K_END K_CASE ';'
    // -----------------------------------------------------------------------
    fn stmt_case(&mut self) -> PgResult<PLpgSQL_stmt> {
        let tok = self.yylex()?;
        debug_assert!(tok == K_CASE);
        let case_loc = self.yylloc;

        let t_expr = self.opt_expr_until_when()?;
        let case_when_list = self.case_when_list()?;
        let (have_else, else_stmts) = self.opt_case_else()?;

        let tok2 = self.yylex()?;
        if tok2 != K_END {
            return Err(self.yyerror("syntax error, expected \"END\""));
        }
        let tok3 = self.yylex()?;
        if tok3 != K_CASE {
            return Err(self.yyerror("syntax error, expected \"CASE\""));
        }
        let tok4 = self.yylex()?;
        if tok4 != (';' as i32) {
            return Err(self.yyerror("syntax error, expected \";\""));
        }

        self.make_case(case_loc, t_expr, case_when_list, have_else, else_stmts)
    }

    // opt_expr_until_when : read expr until WHEN (or none if WHEN is next)
    fn opt_expr_until_when(&mut self) -> PgResult<Option<Box<PLpgSQL_expr>>> {
        let tok = self.yylex()?;
        let expr = if tok != K_WHEN {
            self.push_back_token(tok)?;
            Some(self.read_sql_expression(K_WHEN, "WHEN")?)
        } else {
            None
        };
        self.push_back_token(K_WHEN)?;
        Ok(expr)
    }

    // case_when_list : case_when | case_when_list case_when
    fn case_when_list(&mut self) -> PgResult<Vec<PLpgSQL_case_when>> {
        let mut list = Vec::new();
        let cw = self.case_when()?;
        mem::vpush(&mut list, cw);
        loop {
            // case_when begins with K_WHEN.
            let tok = self.yylex()?;
            self.push_back_token(tok)?;
            if tok != K_WHEN {
                break;
            }
            let cw = self.case_when()?;
            mem::vpush(&mut list, cw);
        }
        Ok(list)
    }

    // case_when : K_WHEN expr_until_then proc_sect
    fn case_when(&mut self) -> PgResult<PLpgSQL_case_when> {
        let tok = self.yylex()?;
        if tok != K_WHEN {
            return Err(self.yyerror("syntax error, expected \"WHEN\""));
        }
        let loc = self.yylloc;
        let expr = self.expr_until_then()?;
        let stmts = self.proc_sect()?;
        let lineno = self.loc_to_lineno(loc);
        Ok(PLpgSQL_case_when {
            lineno,
            expr: Some(expr),
            stmts,
        })
    }

    // opt_case_else : /* empty */ | K_ELSE proc_sect
    // Returns (have_else, stmts). `have_else` is true iff K_ELSE was present
    // (this replaces the C list-with-NULL hack that distinguishes "ELSE with
    // empty body" from "no ELSE").
    fn opt_case_else(&mut self) -> PgResult<(bool, Vec<PLpgSQL_stmt>)> {
        let tok = self.yylex()?;
        if tok != K_ELSE {
            self.push_back_token(tok)?;
            return Ok((false, Vec::new()));
        }
        let stmts = self.proc_sect()?;
        Ok((true, stmts))
    }

    // cursor_variable : T_DATUM | T_WORD | T_CWORD  (returns the cursor dno)
    fn cursor_variable(&mut self) -> PgResult<i32> {
        let tok = self.yylex()?;
        let loc = self.yylloc;
        if tok == T_DATUM {
            let wdatum = self
                .yylval
                .wdatum
                .clone()
                .ok_or_else(|| internal_error("cursor_variable: T_DATUM without wdatum payload"))?;
            let datum_dno = wdatum
                .datum
                .ok_or_else(|| internal_error("cursor_variable: T_DATUM without dno"))? as i32;
            if comp_seam::datum_dtype::call(datum_dno) != PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
                || self.peek()? == ('[' as i32)
            {
                return Err(self.datatype_at("cursor variable must be a simple variable", loc));
            }
            if comp_seam::var_datatype_typoid::call(datum_dno) != REFCURSOROID {
                return Err(self.datatype_at(
                    &format!(
                        "variable \"{}\" must be of type cursor or refcursor",
                        comp_seam::var_refname::call(datum_dno)
                    ),
                    loc,
                ));
            }
            Ok(datum_dno)
        } else if tok == T_WORD {
            let ident = self
                .yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("cursor_variable: T_WORD without word payload"))?
                .ident
                .clone();
            Err(self.word_is_not_variable(&ident, loc))
        } else if tok == T_CWORD {
            let idents = self
                .yylval
                .cword
                .as_ref()
                .ok_or_else(|| internal_error("cursor_variable: T_CWORD without cword payload"))?
                .idents
                .join(".");
            Err(self.cword_is_not_variable(&idents, loc))
        } else {
            Err(self.yyerror("syntax error"))
        }
    }
}

impl<'mcx> Parser<'mcx> {
    // exception_sect : /* empty */ | K_EXCEPTION <mid-rule> proc_exceptions
    fn exception_sect(&mut self) -> PgResult<Option<Box<PLpgSQL_exception_block>>> {
        let tok = self.yylex()?;
        if tok != K_EXCEPTION {
            self.push_back_token(tok)?;
            return Ok(None);
        }
        let loc = self.yylloc;

        let lineno = self.loc_to_lineno(loc);
        comp_seam::curr_compile_set_has_exception_block::call();
        let collation = comp_seam::curr_compile_fn_input_collation::call();
        let sqlstate_varno =
            comp_seam::plpgsql_build_exc_special_var::call("sqlstate", lineno, TEXTOID, collation)?;
        let sqlerrm_varno =
            comp_seam::plpgsql_build_exc_special_var::call("sqlerrm", lineno, TEXTOID, collation)?;

        let exc_list = self.proc_exceptions()?;

        Ok(Some(mem::boxed(PLpgSQL_exception_block {
            sqlstate_varno,
            sqlerrm_varno,
            exc_list,
        })))
    }

    // proc_exceptions : proc_exception | proc_exceptions proc_exception
    fn proc_exceptions(&mut self) -> PgResult<Vec<PLpgSQL_exception>> {
        let mut list = Vec::new();
        let e = self.proc_exception()?;
        mem::vpush(&mut list, e);
        loop {
            let tok = self.yylex()?;
            self.push_back_token(tok)?;
            if tok != K_WHEN {
                break;
            }
            let e = self.proc_exception()?;
            mem::vpush(&mut list, e);
        }
        Ok(list)
    }

    // proc_exception : K_WHEN proc_conditions K_THEN proc_sect
    fn proc_exception(&mut self) -> PgResult<PLpgSQL_exception> {
        let tok = self.yylex()?;
        if tok != K_WHEN {
            return Err(self.yyerror("syntax error, expected \"WHEN\""));
        }
        let loc = self.yylloc;
        let conditions = self.proc_conditions()?;
        let tok2 = self.yylex()?;
        if tok2 != K_THEN {
            return Err(self.yyerror("syntax error, expected \"THEN\""));
        }
        let action = self.proc_sect()?;
        let lineno = self.loc_to_lineno(loc);
        Ok(PLpgSQL_exception {
            lineno,
            conditions: Some(mem::boxed(conditions)),
            action,
        })
    }

    // proc_conditions : proc_condition | proc_conditions K_OR proc_condition
    fn proc_conditions(&mut self) -> PgResult<PLpgSQL_condition> {
        let mut head = self.proc_condition()?;
        loop {
            let tok = self.yylex()?;
            if tok != K_OR {
                self.push_back_token(tok)?;
                break;
            }
            let next = self.proc_condition()?;
            let mut cur = &mut head;
            while cur.next.is_some() {
                cur = cur.next.as_mut().unwrap();
            }
            cur.next = Some(mem::boxed(next));
        }
        Ok(head)
    }

    // proc_condition : any_identifier <or SQLSTATE 'xxxxx'>
    fn proc_condition(&mut self) -> PgResult<PLpgSQL_condition> {
        let ident = self.any_identifier()?;
        if ident != "sqlstate" {
            comp_seam::plpgsql_parse_err_condition::call(&ident)
        } else {
            let tok = self.yylex()?;
            if tok != SCONST {
                return Err(self.yyerror("syntax error"));
            }
            let sqlstatestr = self.yylval.str.clone().unwrap_or_default();
            if sqlstatestr.len() != 5 || !is_valid_sqlstate(&sqlstatestr) {
                return Err(self.yyerror("invalid SQLSTATE code"));
            }
            let b = sqlstatestr.as_bytes();
            let sqlerrstate = make_sqlstate(b[0], b[1], b[2], b[3], b[4]);
            Ok(PLpgSQL_condition {
                sqlerrstate,
                condname: sqlstatestr,
                next: None,
            })
        }
    }

    // expr_until_semi / expr_until_then / expr_until_loop
    fn expr_until_semi(&mut self) -> PgResult<Box<PLpgSQL_expr>> {
        self.read_sql_expression(';' as i32, ";")
    }

    fn expr_until_then(&mut self) -> PgResult<Box<PLpgSQL_expr>> {
        self.read_sql_expression(K_THEN, "THEN")
    }

    fn expr_until_loop(&mut self) -> PgResult<Box<PLpgSQL_expr>> {
        self.read_sql_expression(K_LOOP, "LOOP")
    }

    // opt_block_label : { ns_push(NULL, BLOCK) } | LESS_LESS any_identifier GREATER_GREATER
    fn opt_block_label(&mut self) -> PgResult<(Option<String>, i32)> {
        let tok = self.yylex()?;
        if tok != LESS_LESS {
            self.push_back_token(tok)?;
            funcs::plpgsql_ns_push(None, PLpgSQL_label_type::PLPGSQL_LABEL_BLOCK);
            return Ok((None, -1));
        }
        let loc = self.yylloc;
        let id = self.any_identifier()?;
        let tok2 = self.yylex()?;
        if tok2 != GREATER_GREATER {
            return Err(self.yyerror("syntax error, expected \">>\""));
        }
        funcs::plpgsql_ns_push(Some(&id), PLpgSQL_label_type::PLPGSQL_LABEL_BLOCK);
        Ok((Some(id), loc))
    }

    // opt_loop_label (unused directly: opt_loop_label's ns push happens in
    // proc_stmt). Provided for completeness / faithful parity.
    #[allow(dead_code)]
    fn opt_loop_label(&mut self) -> PgResult<(Option<String>, i32)> {
        let tok = self.yylex()?;
        if tok != LESS_LESS {
            self.push_back_token(tok)?;
            funcs::plpgsql_ns_push(None, PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
            return Ok((None, -1));
        }
        let loc = self.yylloc;
        let id = self.any_identifier()?;
        let tok2 = self.yylex()?;
        if tok2 != GREATER_GREATER {
            return Err(self.yyerror("syntax error, expected \">>\""));
        }
        funcs::plpgsql_ns_push(Some(&id), PLpgSQL_label_type::PLPGSQL_LABEL_LOOP);
        Ok((Some(id), loc))
    }

    // opt_label : { NULL } | any_identifier
    fn opt_label(&mut self) -> PgResult<(Option<String>, i32)> {
        let tok = self.yylex()?;
        if tok == T_WORD || tok == T_DATUM || scanner::plpgsql_token_is_unreserved_keyword(tok) {
            let loc = self.yylloc;
            self.push_back_token(tok)?;
            let id = self.any_identifier()?;
            Ok((Some(id), loc))
        } else {
            self.push_back_token(tok)?;
            Ok((None, -1))
        }
    }

    // opt_exitcond : ';' | K_WHEN expr_until_semi
    fn opt_exitcond(&mut self) -> PgResult<Option<Box<PLpgSQL_expr>>> {
        let tok = self.yylex()?;
        if tok == (';' as i32) {
            Ok(None)
        } else if tok == K_WHEN {
            Ok(Some(self.expr_until_semi()?))
        } else {
            Err(self.yyerror("syntax error"))
        }
    }

    // any_identifier : T_WORD | unreserved_keyword | T_DATUM
    fn any_identifier(&mut self) -> PgResult<String> {
        let tok = self.yylex()?;
        if tok == T_WORD {
            Ok(self
                .yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("any_identifier: T_WORD without word payload"))?
                .ident
                .clone())
        } else if scanner::plpgsql_token_is_unreserved_keyword(tok) {
            Ok(self.yylval.keyword.clone().unwrap_or_default())
        } else if tok == T_DATUM {
            let wdatum = self
                .yylval
                .wdatum
                .as_ref()
                .ok_or_else(|| internal_error("any_identifier: T_DATUM without wdatum payload"))?;
            match &wdatum.ident {
                Some(id) => Ok(id.clone()),
                None => Err(self.yyerror("syntax error")), // composite name not OK
            }
        } else {
            Err(self.yyerror("syntax error"))
        }
    }
}

// ===========================================================================
// Static helper functions of pl_gram.y (the %% epilogue), as methods.
// ===========================================================================
impl<'mcx> Parser<'mcx> {
    /// `tok_is_keyword(token, lval, kw_token, kw_str)`.
    fn tok_is_keyword(&self, token: i32, kw_token: i32, kw_str: &str) -> bool {
        if token == kw_token {
            return true;
        }
        if token == T_DATUM {
            if let Some(wdatum) = &self.yylval.wdatum {
                if !wdatum.quoted {
                    if let Some(ident) = &wdatum.ident {
                        if ident == kw_str {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// `read_sql_expression(until, expected)`.
    fn read_sql_expression(&mut self, until: i32, expected: &str) -> PgResult<Box<PLpgSQL_expr>> {
        let (expr, _start, _endtok) = self.read_sql_construct(
            until,
            0,
            0,
            expected,
            RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
            true,
            true,
        )?;
        Ok(expr)
    }

    /// `read_sql_expression2(until, until2, expected, *endtoken)` -> (expr, endtoken).
    fn read_sql_expression2(
        &mut self,
        until: i32,
        until2: i32,
        expected: &str,
    ) -> PgResult<(Box<PLpgSQL_expr>, i32)> {
        let (expr, _start, endtok) = self.read_sql_construct(
            until,
            until2,
            0,
            expected,
            RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
            true,
            true,
        )?;
        Ok((expr, endtok))
    }

    /// `read_sql_stmt()`.
    fn read_sql_stmt(&mut self) -> PgResult<Box<PLpgSQL_expr>> {
        let (expr, _start, _endtok) =
            self.read_sql_construct(';' as i32, 0, 0, ";", RawParseMode::RAW_PARSE_DEFAULT, false, true)?;
        Ok(expr)
    }

    /// `read_sql_construct(...)` -> (expr, startloc, endtoken).
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
    ) -> PgResult<(Box<PLpgSQL_expr>, i32, i32)> {
        let mut ds = String::new();
        let mut startlocation: i32 = -1;
        let mut endlocation: i32 = -1;
        let mut parenlevel: i32 = 0;
        let mut tok;

        let save_lookup = self.identifier_lookup();
        self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_EXPR);

        loop {
            tok = self.yylex()?;
            if startlocation < 0 {
                startlocation = self.yylloc;
            }
            if tok == until && parenlevel == 0 {
                break;
            }
            if until2 != 0 && tok == until2 && parenlevel == 0 {
                break;
            }
            if until3 != 0 && tok == until3 && parenlevel == 0 {
                break;
            }
            if tok == ('(' as i32) || tok == ('[' as i32) {
                parenlevel += 1;
            } else if tok == (')' as i32) || tok == (']' as i32) {
                parenlevel -= 1;
                if parenlevel < 0 {
                    self.set_identifier_lookup(save_lookup);
                    return Err(self.yyerror("mismatched parentheses"));
                }
            }

            if tok == 0 || tok == (';' as i32) {
                self.set_identifier_lookup(save_lookup);
                if parenlevel != 0 {
                    return Err(self.yyerror("mismatched parentheses"));
                }
                let what = if isexpression { "SQL expression" } else { "SQL statement" };
                return Err(self.syntax_at(
                    &format!("missing \"{expected}\" at end of {what}"),
                    self.yylloc,
                ));
            }
            endlocation = self.yylloc + self.token_length();
        }

        self.set_identifier_lookup(save_lookup);

        let startloc_out = startlocation;
        let endtoken_out = tok;

        if startlocation >= endlocation {
            if isexpression {
                return Err(self.yyerror("missing expression"));
            } else {
                return Err(self.yyerror("missing SQL statement"));
            }
        }

        self.scanner
            .plpgsql_append_source_text(&mut ds, startlocation, endlocation);

        let expr = make_plpgsql_expr(&ds, parsemode);

        if valid_sql {
            self.check_sql_expr(&expr.query, expr.parseMode, startlocation)?;
        }

        Ok((expr, startloc_out, endtoken_out))
    }

    /// `read_sql_construct(...)` returning just (expr, endtoken).
    #[allow(clippy::too_many_arguments)]
    fn read_sql_construct_endtok(
        &mut self,
        until: i32,
        until2: i32,
        until3: i32,
        expected: &str,
        parsemode: RawParseMode,
        isexpression: bool,
        valid_sql: bool,
    ) -> PgResult<(Box<PLpgSQL_expr>, i32)> {
        let (expr, _start, endtok) = self.read_sql_construct(
            until, until2, until3, expected, parsemode, isexpression, valid_sql,
        )?;
        Ok((expr, endtok))
    }

    /// `read_datatype(tok, ...)`.
    fn read_datatype(&mut self, mut tok: i32) -> PgResult<Box<PLpgSQL_type>> {
        debug_assert_eq!(
            self.identifier_lookup(),
            IdentifierLookup::IDENTIFIER_LOOKUP_DECLARE
        );

        if tok == YYEMPTY {
            tok = self.yylex()?;
        }

        let startlocation = self.yylloc;

        let mut result: Option<Box<PLpgSQL_type>> = None;

        if tok == T_WORD {
            let dtname = self
                .yylval
                .word
                .as_ref()
                .ok_or_else(|| internal_error("read_datatype: T_WORD without word payload"))?
                .ident
                .clone();
            tok = self.yylex()?;
            if tok == ('%' as i32) {
                tok = self.yylex()?;
                if self.tok_is_keyword(tok, K_TYPE, "type") {
                    result = Some(comp_seam::plpgsql_parse_wordtype::call(&dtname)?);
                } else if self.tok_is_keyword(tok, K_ROWTYPE, "rowtype") {
                    result = Some(comp_seam::plpgsql_parse_wordrowtype::call(&dtname)?);
                }
            }
        } else if scanner::plpgsql_token_is_unreserved_keyword(tok) {
            let dtname = self.yylval.keyword.clone().unwrap_or_default();
            tok = self.yylex()?;
            if tok == ('%' as i32) {
                tok = self.yylex()?;
                if self.tok_is_keyword(tok, K_TYPE, "type") {
                    result = Some(comp_seam::plpgsql_parse_wordtype::call(&dtname)?);
                } else if self.tok_is_keyword(tok, K_ROWTYPE, "rowtype") {
                    result = Some(comp_seam::plpgsql_parse_wordrowtype::call(&dtname)?);
                }
            }
        } else if tok == T_CWORD {
            let dtnames = self
                .yylval
                .cword
                .as_ref()
                .ok_or_else(|| internal_error("read_datatype: T_CWORD without cword payload"))?
                .idents
                .clone();
            tok = self.yylex()?;
            if tok == ('%' as i32) {
                tok = self.yylex()?;
                if self.tok_is_keyword(tok, K_TYPE, "type") {
                    result = Some(comp_seam::plpgsql_parse_cwordtype::call(&dtnames)?);
                } else if self.tok_is_keyword(tok, K_ROWTYPE, "rowtype") {
                    result = Some(comp_seam::plpgsql_parse_cwordrowtype::call(&dtnames)?);
                }
            }
        }

        if let Some(result) = result {
            let mut is_array = false;
            tok = self.yylex()?;
            if self.tok_is_keyword(tok, K_ARRAY, "array") {
                is_array = true;
                tok = self.yylex()?;
            }
            while tok == ('[' as i32) {
                is_array = true;
                tok = self.yylex()?;
                if tok == ICONST {
                    tok = self.yylex()?;
                }
                if tok != (']' as i32) {
                    return Err(self.yyerror("syntax error, expected \"]\""));
                }
                tok = self.yylex()?;
            }
            self.push_back_token(tok)?;

            return if is_array {
                comp_seam::plpgsql_build_datatype_arrayof::call(*result)
            } else {
                Ok(result)
            };
        }

        // Not %TYPE/%ROWTYPE: scan to the end of the datatype declaration.
        let mut parenlevel: i32 = 0;
        while tok != (';' as i32) {
            if tok == 0 {
                if parenlevel != 0 {
                    return Err(self.yyerror("mismatched parentheses"));
                } else {
                    return Err(self.yyerror("incomplete data type declaration"));
                }
            }
            if tok == K_COLLATE
                || tok == K_NOT
                || tok == ('=' as i32)
                || tok == COLON_EQUALS
                || tok == K_DEFAULT
            {
                break;
            }
            if (tok == (',' as i32) || tok == (')' as i32)) && parenlevel == 0 {
                break;
            }
            if tok == ('(' as i32) {
                parenlevel += 1;
            } else if tok == (')' as i32) {
                parenlevel -= 1;
            }
            tok = self.yylex()?;
        }

        let mut ds = String::new();
        let here = self.yylloc;
        self.scanner
            .plpgsql_append_source_text(&mut ds, startlocation, here);
        let type_name = ds;

        if type_name.is_empty() {
            return Err(self.yyerror("missing data type declaration"));
        }

        let result = comp_seam::parse_datatype::call(&type_name, startlocation)?;

        self.push_back_token(tok)?;

        Ok(result)
    }
}

impl<'mcx> Parser<'mcx> {
    /// `make_execsql_stmt(firsttoken, location, word)`.
    fn make_execsql_stmt(
        &mut self,
        firsttoken: i32,
        location: i32,
        word: Option<PLword>,
    ) -> PgResult<PLpgSQL_stmt> {
        let mut ds = String::new();
        let mut target: Option<Box<PLpgSQL_variable>> = None;
        let mut prev_tok;
        let mut have_into = false;
        let mut have_strict = false;
        let mut into_start_loc: i32 = -1;
        let mut into_end_loc: i32 = -1;
        let mut paren_depth: i32 = 0;
        let mut begin_depth: i32 = 0;
        let mut in_routine_definition = false;
        let mut token_count = 0usize;
        let mut tokens = [0u8; 4];

        let save_lookup = self.identifier_lookup();
        self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_EXPR);

        let mut tok = firsttoken;
        if tok == T_WORD {
            if let Some(w) = &word {
                if w.ident == "create" {
                    tokens[token_count] = b'c';
                }
            }
        }
        token_count += 1;

        loop {
            prev_tok = tok;
            tok = self.yylex()?;
            if have_into && into_end_loc < 0 {
                into_end_loc = self.yylloc;
            }
            if tokens[0] == b'c' && token_count < tokens.len() {
                if tok == K_OR {
                    tokens[token_count] = b'o';
                } else if tok == T_WORD
                    && self.yylval.word.as_ref().map(|w| w.ident.as_str()) == Some("replace")
                {
                    tokens[token_count] = b'r';
                } else if tok == T_WORD
                    && self.yylval.word.as_ref().map(|w| w.ident.as_str()) == Some("function")
                {
                    tokens[token_count] = b'f';
                } else if tok == T_WORD
                    && self.yylval.word.as_ref().map(|w| w.ident.as_str()) == Some("procedure")
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
                break;
            }
            if tok == 0 {
                self.set_identifier_lookup(save_lookup);
                return Err(self.yyerror("unexpected end of function definition"));
            }
            if tok == K_INTO {
                if prev_tok == K_INSERT {
                    continue;
                }
                if prev_tok == K_MERGE {
                    continue;
                }
                if firsttoken == K_IMPORT {
                    continue;
                }
                if have_into {
                    self.set_identifier_lookup(save_lookup);
                    return Err(self.yyerror("INTO specified more than once"));
                }
                have_into = true;
                into_start_loc = self.yylloc;
                self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_NORMAL);
                let (t, s) = self.read_into_target(true)?;
                target = t;
                have_strict = s;
                self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_EXPR);
            }
        }

        self.set_identifier_lookup(save_lookup);

        let here = self.yylloc;
        if have_into {
            self.scanner
                .plpgsql_append_source_text(&mut ds, location, into_start_loc);
            for _ in 0..(into_end_loc - into_start_loc) {
                mem::spushc(&mut ds, ' ');
            }
            self.scanner
                .plpgsql_append_source_text(&mut ds, into_end_loc, here);
        } else {
            self.scanner
                .plpgsql_append_source_text(&mut ds, location, here);
        }

        while ds
            .as_bytes()
            .last()
            .map(|&b| scanner_isspace(b))
            .unwrap_or(false)
        {
            ds.pop();
        }

        let expr = make_plpgsql_expr(&ds, RawParseMode::RAW_PARSE_DEFAULT);

        self.check_sql_expr(&expr.query, expr.parseMode, location)?;

        let lineno = self.loc_to_lineno(location);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let execsql = PLpgSQL_stmt_execsql {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_EXECSQL,
            lineno,
            stmtid,
            sqlstmt: Some(expr),
            mod_stmt: false,
            mod_stmt_set: false,
            into: have_into,
            strict: have_strict,
            target,
        };
        Ok(PLpgSQL_stmt::Execsql(mem::boxed(execsql)))
    }

    /// `make_return_stmt(location)`.
    fn make_return_stmt(&mut self, location: i32) -> PgResult<PLpgSQL_stmt> {
        let lineno = self.loc_to_lineno(location);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();
        let mut expr: Option<Box<PLpgSQL_expr>> = None;
        let mut retvarno: i32 = -1;

        if comp_seam::curr_compile_fn_retset::call() {
            let tok = self.yylex()?;
            if tok != (';' as i32) {
                return Err(self.return_param_error("set", false, self.yylloc));
            }
        } else if comp_seam::curr_compile_fn_rettype::call() == VOIDOID {
            let tok = self.yylex()?;
            if tok != (';' as i32) {
                let is_proc = comp_seam::curr_compile_fn_prokind::call() == PROKIND_PROCEDURE;
                return Err(self.return_param_error(
                    if is_proc { "procedure" } else { "void" },
                    is_proc,
                    self.yylloc,
                ));
            }
        } else if comp_seam::curr_compile_out_param_varno::call() >= 0 {
            let tok = self.yylex()?;
            if tok != (';' as i32) {
                return Err(self.return_param_error("out", false, self.yylloc));
            }
            retvarno = comp_seam::curr_compile_out_param_varno::call();
        } else {
            let tok = self.yylex()?;
            if tok == T_DATUM
                && self.peek()? == (';' as i32)
                && is_returnable_datum_dtype(self.datum_dtype_of_current_wdatum()?)
            {
                retvarno = self
                    .yylval
                    .wdatum
                    .as_ref()
                    .ok_or_else(|| internal_error("make_return_stmt: T_DATUM without wdatum"))?
                    .datum
                    .ok_or_else(|| internal_error("make_return_stmt: T_DATUM without dno"))?
                    as i32;
                let semi = self.yylex()?;
                debug_assert!(semi == (';' as i32));
            } else {
                self.push_back_token(tok)?;
                expr = Some(self.read_sql_expression(';' as i32, ";")?);
            }
        }

        let new = PLpgSQL_stmt_return {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_RETURN,
            lineno,
            stmtid,
            expr,
            retvarno,
        };
        Ok(PLpgSQL_stmt::Return(mem::boxed(new)))
    }

    /// `make_return_next_stmt(location)`.
    fn make_return_next_stmt(&mut self, location: i32) -> PgResult<PLpgSQL_stmt> {
        if !comp_seam::curr_compile_fn_retset::call() {
            return Err(self.datatype_at("cannot use RETURN NEXT in a non-SETOF function", location));
        }
        let lineno = self.loc_to_lineno(location);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();
        let mut expr: Option<Box<PLpgSQL_expr>> = None;
        let mut retvarno: i32 = -1;

        if comp_seam::curr_compile_out_param_varno::call() >= 0 {
            let tok = self.yylex()?;
            if tok != (';' as i32) {
                return Err(self.return_param_error("next_out", false, self.yylloc));
            }
            retvarno = comp_seam::curr_compile_out_param_varno::call();
        } else {
            let tok = self.yylex()?;
            if tok == T_DATUM
                && self.peek()? == (';' as i32)
                && is_returnable_datum_dtype(self.datum_dtype_of_current_wdatum()?)
            {
                retvarno = self
                    .yylval
                    .wdatum
                    .as_ref()
                    .ok_or_else(|| internal_error("make_return_next_stmt: T_DATUM without wdatum"))?
                    .datum
                    .ok_or_else(|| internal_error("make_return_next_stmt: T_DATUM without dno"))?
                    as i32;
                let semi = self.yylex()?;
                debug_assert!(semi == (';' as i32));
            } else {
                self.push_back_token(tok)?;
                expr = Some(self.read_sql_expression(';' as i32, ";")?);
            }
        }

        let new = PLpgSQL_stmt_return_next {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_RETURN_NEXT,
            lineno,
            stmtid,
            expr,
            retvarno,
        };
        Ok(PLpgSQL_stmt::ReturnNext(mem::boxed(new)))
    }

    /// `make_return_query_stmt(location)`.
    fn make_return_query_stmt(&mut self, location: i32) -> PgResult<PLpgSQL_stmt> {
        if !comp_seam::curr_compile_fn_retset::call() {
            return Err(self.datatype_at("cannot use RETURN QUERY in a non-SETOF function", location));
        }
        let lineno = self.loc_to_lineno(location);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let mut query: Option<Box<PLpgSQL_expr>> = None;
        let mut dynquery: Option<Box<PLpgSQL_expr>> = None;
        let mut params: Vec<PLpgSQL_expr> = Vec::new();

        let tok = self.yylex()?;
        if tok != K_EXECUTE {
            self.push_back_token(tok)?;
            query = Some(self.read_sql_stmt()?);
        } else {
            let (dq, mut term) = self.read_sql_expression2(';' as i32, K_USING, "; or USING")?;
            dynquery = Some(dq);
            if term == K_USING {
                loop {
                    let (expr, t) = self.read_sql_expression2(',' as i32, ';' as i32, ", or ;")?;
                    mem::vpush(&mut params, *expr);
                    term = t;
                    if term != (',' as i32) {
                        break;
                    }
                }
            }
        }

        let new = PLpgSQL_stmt_return_query {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_RETURN_QUERY,
            lineno,
            stmtid,
            query,
            dynquery,
            params,
        };
        Ok(PLpgSQL_stmt::ReturnQuery(mem::boxed(new)))
    }

    /// dtype of the wdatum currently in `self.yylval`.
    fn datum_dtype_of_current_wdatum(&self) -> PgResult<PLpgSQL_datum_type> {
        let dno = self
            .yylval
            .wdatum
            .as_ref()
            .ok_or_else(|| internal_error("datum_dtype_of_current_wdatum: T_DATUM without wdatum"))?
            .datum
            .ok_or_else(|| internal_error("datum_dtype_of_current_wdatum: T_DATUM without dno"))?
            as i32;
        Ok(comp_seam::datum_dtype::call(dno))
    }

    /// `read_into_target(*target, *strict)`. On entry INTO was just read. When
    /// `accept_strict` is false (FETCH; C passes NULL strict) the STRICT keyword
    /// is not consumed. Returns (target, strict).
    fn read_into_target(
        &mut self,
        accept_strict: bool,
    ) -> PgResult<(Option<Box<PLpgSQL_variable>>, bool)> {
        let mut strict = false;

        let mut tok = self.yylex()?;
        if accept_strict && tok == K_STRICT {
            strict = true;
            tok = self.yylex()?;
        }

        if tok == T_DATUM {
            let wdatum = self
                .yylval
                .wdatum
                .clone()
                .ok_or_else(|| internal_error("read_into_target: T_DATUM without wdatum payload"))?;
            let datum_dno = wdatum
                .datum
                .ok_or_else(|| internal_error("read_into_target: T_DATUM without dno"))? as i32;
            let dtype = comp_seam::datum_dtype::call(datum_dno);
            if dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW
                || dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_REC
            {
                comp_seam::check_assignable::call(datum_dno, self.yylloc)?;
                let target = mem::boxed(comp_seam::datum_as_variable::call(datum_dno));
                let t2 = self.yylex()?;
                if t2 == (',' as i32) {
                    return Err(self.syntax_at(
                        "record variable cannot be part of multiple-item INTO list",
                        self.yylloc,
                    ));
                }
                self.push_back_token(t2)?;
                Ok((Some(target), strict))
            } else {
                let name = name_of_datum(&wdatum);
                let row_dno = self.read_into_scalar_list(&name, datum_dno, self.yylloc)?;
                Ok((Some(mem::boxed(comp_seam::datum_as_variable::call(row_dno))), strict))
            }
        } else {
            Err(self.current_token_is_not_variable(tok))
        }
    }

    /// `read_into_scalar_list(initial_name, initial_datum, initial_location)`.
    fn read_into_scalar_list(
        &mut self,
        initial_name: &str,
        initial_datum_dno: i32,
        initial_location: i32,
    ) -> PgResult<i32> {
        let mut fieldnames: Vec<String> = Vec::new();
        let mut varnos: Vec<i32> = Vec::new();

        comp_seam::check_assignable::call(initial_datum_dno, initial_location)?;
        mem::vpush(&mut fieldnames, mem::sdup(initial_name));
        mem::vpush(&mut varnos, initial_datum_dno);

        let mut tok;
        loop {
            tok = self.yylex()?;
            if tok != (',' as i32) {
                break;
            }
            if fieldnames.len() >= 1024 {
                return Err(self.too_many_into(self.yylloc));
            }
            tok = self.yylex()?;
            if tok == T_DATUM {
                let wdatum = self
                    .yylval
                    .wdatum
                    .clone()
                    .ok_or_else(|| internal_error("read_into_scalar_list: T_DATUM without wdatum"))?;
                let datum_dno = wdatum
                    .datum
                    .ok_or_else(|| internal_error("read_into_scalar_list: T_DATUM without dno"))?
                    as i32;
                comp_seam::check_assignable::call(datum_dno, self.yylloc)?;
                let dtype = comp_seam::datum_dtype::call(datum_dno);
                if dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW
                    || dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_REC
                {
                    return Err(self.not_scalar_variable(&name_of_datum(&wdatum), self.yylloc));
                }
                mem::vpush(&mut fieldnames, name_of_datum(&wdatum));
                mem::vpush(&mut varnos, datum_dno);
            } else {
                return Err(self.current_token_is_not_variable(tok));
            }
        }

        self.push_back_token(tok)?;

        let lineno = self.loc_to_lineno(initial_location);
        comp_seam::plpgsql_build_into_row::call(lineno, fieldnames, varnos)
    }

    /// `current_token_is_not_variable(tok)`.
    fn current_token_is_not_variable(&mut self, tok: i32) -> PgError {
        if tok == T_WORD {
            match self.yylval.word.as_ref() {
                Some(w) => self.word_is_not_variable(&w.ident.clone(), self.yylloc),
                None => internal_error("current_token_is_not_variable: T_WORD without payload"),
            }
        } else if tok == T_CWORD {
            match self.yylval.cword.as_ref() {
                Some(c) => self.cword_is_not_variable(&c.idents.join("."), self.yylloc),
                None => internal_error("current_token_is_not_variable: T_CWORD without payload"),
            }
        } else {
            self.yyerror("syntax error")
        }
    }
}

impl<'mcx> Parser<'mcx> {
    /// `read_cursor_args(cursor, until)`.
    fn read_cursor_args(
        &mut self,
        cursor_dno: i32,
        until: i32,
    ) -> PgResult<Option<Box<PLpgSQL_expr>>> {
        let argrow = comp_seam::var_cursor_explicit_argrow::call(cursor_dno);
        let cursor_refname = comp_seam::var_refname::call(cursor_dno);

        let mut tok = self.yylex()?;
        if argrow < 0 {
            if tok == ('(' as i32) {
                return Err(self.syntax_at(
                    &format!("cursor \"{cursor_refname}\" has no arguments"),
                    self.yylloc,
                ));
            }
            if tok != until {
                return Err(self.yyerror("syntax error"));
            }
            return Ok(None);
        }

        if tok != ('(' as i32) {
            return Err(self.syntax_at(
                &format!("cursor \"{cursor_refname}\" has arguments"),
                self.yylloc,
            ));
        }

        let fieldnames = comp_seam::cursor_argrow_fieldnames::call(argrow);
        let nfields = fieldnames.len();
        let mut argv: Vec<Option<String>> = mem::vzeroed(nfields);
        let mut any_named = false;

        for argc in 0..nfields {
            let argpos;
            let (tok1, tok2, arglocation, _l) = self.peek2()?;
            if tok1 == scan_fgram::tokens::IDENT
                && (tok2 == COLON_EQUALS || tok2 == EQUALS_GREATER)
            {
                let save_lookup = self.identifier_lookup();
                self.set_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_DECLARE);
                self.yylex()?;
                let argname = self.yylval.str.clone().unwrap_or_default();
                self.set_identifier_lookup(save_lookup);

                match fieldnames.iter().position(|n| n == &argname) {
                    Some(p) => argpos = p,
                    None => {
                        return Err(self.syntax_at(
                            &format!(
                                "cursor \"{cursor_refname}\" has no argument named \"{argname}\""
                            ),
                            self.yylloc,
                        ))
                    }
                }

                let t2 = self.yylex()?;
                if t2 != COLON_EQUALS && t2 != EQUALS_GREATER {
                    return Err(self.yyerror("syntax error"));
                }
                any_named = true;
            } else {
                argpos = argc;
            }

            if argv[argpos].is_some() {
                return Err(self.syntax_at(
                    &format!(
                        "value for parameter \"{}\" of cursor \"{cursor_refname}\" specified more than once",
                        fieldnames[argpos]
                    ),
                    arglocation,
                ));
            }

            let (item, endtoken) = self.read_sql_construct_endtok(
                ',' as i32,
                ')' as i32,
                0,
                ",\" or \")",
                RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
                true,
                true,
            )?;
            argv[argpos] = Some(item.query.clone());

            if endtoken == (')' as i32) && argc != nfields - 1 {
                return Err(self.syntax_at(
                    &format!("not enough arguments for cursor \"{cursor_refname}\""),
                    self.yylloc,
                ));
            }
            if endtoken == (',' as i32) && argc == nfields - 1 {
                return Err(self.syntax_at(
                    &format!("too many arguments for cursor \"{cursor_refname}\""),
                    self.yylloc,
                ));
            }
        }

        let mut ds = String::new();
        for argc in 0..nfields {
            let a = argv[argc]
                .as_ref()
                .ok_or_else(|| internal_error("read_cursor_args: missing cursor arg"))?;
            mem::spush(&mut ds, a);
            if any_named {
                mem::spush(
                    &mut ds,
                    &format!(" AS {}", comp_seam::quote_identifier::call(&fieldnames[argc])),
                );
            }
            if argc < nfields - 1 {
                mem::spush(&mut ds, ", ");
            }
        }

        let expr = make_plpgsql_expr(&ds, RawParseMode::RAW_PARSE_PLPGSQL_EXPR);

        tok = self.yylex()?;
        if tok != until {
            return Err(self.yyerror("syntax error"));
        }

        Ok(Some(expr))
    }

    /// `read_raise_options()`.
    fn read_raise_options(&mut self) -> PgResult<Vec<PLpgSQL_raise_option>> {
        let mut result = Vec::new();
        loop {
            let tok = self.yylex()?;
            if tok == 0 {
                return Err(self.yyerror("unexpected end of function definition"));
            }
            let opt_type = if self.tok_is_keyword(tok, K_ERRCODE, "errcode") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_ERRCODE
            } else if self.tok_is_keyword(tok, K_MESSAGE, "message") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_MESSAGE
            } else if self.tok_is_keyword(tok, K_DETAIL, "detail") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_DETAIL
            } else if self.tok_is_keyword(tok, K_HINT, "hint") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_HINT
            } else if self.tok_is_keyword(tok, K_COLUMN, "column") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_COLUMN
            } else if self.tok_is_keyword(tok, K_CONSTRAINT, "constraint") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_CONSTRAINT
            } else if self.tok_is_keyword(tok, K_DATATYPE, "datatype") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_DATATYPE
            } else if self.tok_is_keyword(tok, K_TABLE, "table") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_TABLE
            } else if self.tok_is_keyword(tok, K_SCHEMA, "schema") {
                PLpgSQL_raise_option_type::PLPGSQL_RAISEOPTION_SCHEMA
            } else {
                return Err(self.yyerror("unrecognized RAISE statement option"));
            };

            let tok2 = self.yylex()?;
            if tok2 != ('=' as i32) && tok2 != COLON_EQUALS {
                return Err(self.yyerror("syntax error, expected \"=\""));
            }

            let (expr, endtok) = self.read_sql_expression2(',' as i32, ';' as i32, ", or ;")?;
            mem::vpush(
                &mut result,
                PLpgSQL_raise_option {
                    opt_type,
                    expr: Some(expr),
                },
            );

            if endtok == (';' as i32) {
                break;
            }
        }
        Ok(result)
    }

    /// `check_labels(start_label, end_label, end_location)`.
    fn check_labels(
        &mut self,
        start_label: Option<&str>,
        end_label: Option<&str>,
        end_location: i32,
    ) -> PgResult<()> {
        if let Some(end) = end_label {
            match start_label {
                None => {
                    return Err(self.syntax_at(
                        &format!("end label \"{end}\" specified for unlabeled block"),
                        end_location,
                    ))
                }
                Some(start) => {
                    if start != end {
                        return Err(self.syntax_at(
                            &format!(
                                "end label \"{end}\" differs from block's label \"{start}\""
                            ),
                            end_location,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// `check_raise_parameters(stmt)` — verify the placeholder count in the
    /// old-style RAISE message matches the supplied parameter count.
    fn check_raise_parameters(&self, stmt: &PLpgSQL_stmt_raise) -> PgResult<()> {
        let message = match &stmt.message {
            Some(m) => m,
            None => return Ok(()),
        };
        let mut expected_nparams = 0usize;
        let bytes = message.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'%' {
                    i += 1;
                } else {
                    expected_nparams += 1;
                }
            }
            i += 1;
        }
        let nparams = stmt.params.len();
        if expected_nparams < nparams {
            return Err(syntax_error_plain("too many parameters specified for RAISE"));
        }
        if expected_nparams > nparams {
            return Err(syntax_error_plain("too few parameters specified for RAISE"));
        }
        Ok(())
    }

    /// `make_case(location, t_expr, case_when_list, else_stmts)`.
    fn make_case(
        &mut self,
        location: i32,
        t_expr: Option<Box<PLpgSQL_expr>>,
        mut case_when_list: Vec<PLpgSQL_case_when>,
        have_else: bool,
        else_stmts: Vec<PLpgSQL_stmt>,
    ) -> PgResult<PLpgSQL_stmt> {
        let lineno = self.loc_to_lineno(location);
        let stmtid = comp_seam::curr_compile_next_stmtid::call();

        let mut t_varno = 0i32;

        // When a test expression is present, build a var for it and convert all
        // the WHEN expressions to "VAR IN (original_expression)".
        if t_expr.is_some() {
            let varname = format!("__Case__Variable_{}__", comp_seam::plpgsql_ndatums::call());

            // Build the variable as INT4; fixed at runtime if needed.
            let dtype = comp_seam::plpgsql_build_datatype::call(INT4OID, -1, INVALID_OID)?;
            t_varno = comp_seam::plpgsql_build_variable::call(&varname, lineno, *dtype, true)?;

            let ns = funcs::plpgsql_ns_top();
            for cwt in &mut case_when_list {
                if let Some(expr) = cwt.expr.as_mut() {
                    debug_assert_eq!(expr.parseMode, RawParseMode::RAW_PARSE_PLPGSQL_EXPR);
                    let rewritten = format!("\"{varname}\" IN ({})", expr.query);
                    expr.query = mem::sdup(&rewritten);
                    expr.ns = ns.clone();
                }
            }
        }

        let new = PLpgSQL_stmt_case {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_CASE,
            lineno,
            stmtid,
            t_expr,
            t_varno,
            case_when_list,
            have_else,
            else_stmts,
        };
        Ok(PLpgSQL_stmt::Case(mem::boxed(new)))
    }

    // -- positioned ereport helper methods (routed through the scanner) ------

    /// `ereport(WARNING, errcode(ERRCODE_DUPLICATE_ALIAS), errmsg("variable
    /// \"%s\" shadows a previously defined variable", name),
    /// parser_errposition(loc))` — the `extra_warnings=shadowed_variables`
    /// WARNING (emitted immediately; `errfinish` reports it to the client and
    /// returns).  `is_error=true` would promote to ERROR, but that path goes
    /// through [`Parser::shadowvar_error`] instead so the error propagates.
    ///
    /// The body-relative internal position is relocated into the original CREATE
    /// FUNCTION / DO query text by the compile-wide `plpgsql_compile_error_callback`
    /// (registered by `parse_function_body` as an emit-time error-context
    /// callback), which fires for this WARNING in `errfinish` just as C's
    /// `error_context_stack` walk does — so we emit the positioned WARNING here
    /// WITHOUT pre-transposing it (doing so would double-transpose the position).
    fn emit_shadowvar(&self, name: &str, loc: i32, _is_error: bool) -> PgResult<()> {
        let msg = format!("variable \"{name}\" shadows a previously defined variable");
        let err = self.scanner.positioned_error(
            types_error::WARNING,
            ERRCODE_DUPLICATE_ALIAS,
            &msg,
            loc,
        );
        utils_error::ThrowErrorData(err)
    }

    /// The `extra_errors=shadowed_variables` ERROR variant of
    /// [`Parser::emit_shadowvar`].
    fn shadowvar_error(&self, name: &str, loc: i32) -> PgError {
        let msg = format!("variable \"{name}\" shadows a previously defined variable");
        // The body-relative internal position is relocated into the original
        // CREATE FUNCTION / DO query text by `parse_function_body`'s `map_err`
        // (the `plpgsql_compile_error_callback` → `function_parse_error_transpose`
        // step) once this error has unwound out of `plpgsql_yyparse`; do NOT
        // transpose here, or the position is transposed twice.
        self.scanner.positioned_error(
            types_error::ERROR,
            ERRCODE_DUPLICATE_ALIAS,
            &msg,
            loc,
        )
    }


    /// `check_sql_expr(stmt, parseMode, location, yyscanner)` (pl_gram.y) wrapped
    /// with the `plpgsql_sql_error_callback` error-context behavior.  The seam
    /// raw-parses `stmt` for syntax only; a syntax error carries a cursor
    /// position relative to `stmt` itself.  This codebase has retired
    /// `error_context_stack`, so we apply the C callback explicitly here:
    ///
    ///  - `plpgsql_sql_error_callback`: `parser_errposition(location)` maps the
    ///    statement's byte offset within the function body (`scanorig`) to a
    ///    1-based character position and sets it as the *internal* position with
    ///    `internalerrquery(scanorig)`; the core parser's own (1-based char)
    ///    error position is then transposed onto it (`myerrpos + errpos - 1`) and
    ///    the plain errposition is cleared.
    ///
    /// The body-relative internal position is then relocated into the original
    /// CREATE FUNCTION / DO query text by the `plpgsql_compile_error_callback` →
    /// `function_parse_error_transpose` step — but that runs in
    /// `parse_function_body`'s `map_err` once the error has unwound out of
    /// `plpgsql_yyparse`, so we must NOT transpose here (doing so would
    /// double-transpose the position).
    fn check_sql_expr(
        &self,
        stmt: &str,
        parse_mode: RawParseMode,
        location: i32,
    ) -> Result<(), PgError> {
        // plpgsql_sql_error_callback: parser_errposition(location) →
        // internalerrposition(pos) + internalerrquery(scanorig). `body_pos` is the
        // 1-based character offset of this statement's start within the function
        // body, the same for an error (handled in the `map_err` below) and for a
        // WARNING raised inline by the core scanner (handled by the emit-context
        // callback) during the inner raw-parse.
        let body_pos = self.scanner.plpgsql_scanner_errposition(location);
        let scanorig = self.scanner.scanorig().to_string();

        // C installs `plpgsql_sql_error_callback` on `error_context_stack` for the
        // WHOLE `raw_parser(stmt, parseMode)` call, so it fires for EVERY report
        // raised during the parse — including the inline `ereport(WARNING)` the
        // core scanner emits for a nonstandard backslash escape ("nonstandard use
        // of \\ in a string literal"). That chain is retired here for errors (they
        // attach context on propagation, see the `map_err` below), but warnings do
        // not propagate, so we register a scoped emit-time context callback that
        // applies the identical `plpgsql_sql_error_callback` transposition to the
        // in-flight WARNING just before it is emitted: map the statement's body
        // offset + the scanner's own (statement-relative) cursor to a body-relative
        // `internalerrposition`, with `internalerrquery = scanorig`. The enclosing
        // `plpgsql_compile_error_callback` (the compile-wide callback installed by
        // `parse_function_body`) then relocates that body position into the original
        // CREATE FUNCTION / DO query text, exactly as C's two-callback chain does
        // (this callback is registered on TOP, so it runs first / innermost).
        let scanorig_cb = scanorig.clone();
        let cb_id = utils_error::push_emit_context_callback(Box::new(move |err: &mut PgError| {
            err.internal_query = Some(scanorig_cb.clone());
            let errpos = err.cursor_position.unwrap_or(0);
            let internal = if body_pos > 0 && errpos > 0 {
                body_pos + errpos - 1
            } else {
                body_pos
            };
            err.internal_position = types_error::nonzero_position(internal);
            err.cursor_position = None;
        }));

        let result = comp_seam::check_sql_expr::call(stmt, parse_mode, location);

        // Restore `error_context_stack = syntax_errcontext.previous`.
        utils_error::pop_emit_context_callback(cb_id);

        result.map_err(|err| {
            // The propagated-error path: same transposition, applied on unwind.
            let err = err.with_internal_query(scanorig);
            let errpos = err.cursor_position().unwrap_or(0);
            let internal = if body_pos > 0 && errpos > 0 {
                body_pos + errpos - 1
            } else {
                body_pos
            };
            err.with_internal_position(internal).with_cursor_position(0)
        })
    }

    /// `errmsg("variable \"%s\" does not exist", name), parser_errposition(loc)`.
    fn variable_does_not_exist(&self, name: &str, loc: i32) -> PgError {
        self.syntax_at(&format!("variable \"{name}\" does not exist"), loc)
    }

    /// `errmsg("\"%s\" is not a scalar variable", name), parser_errposition(loc)`.
    fn not_scalar_variable(&self, name: &str, loc: i32) -> PgError {
        let mut err = ereport(ERROR_LEVEL)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg_internal(format!("\"{name}\" is not a scalar variable"))
            .into_error();
        let pos = self.scanner.plpgsql_scanner_errposition(loc);
        if pos > 0 {
            err = err.with_internal_position(pos);
        }
        err
    }

    /// GET DIAGNOSTICS item invalid for the requested area
    /// (`ereport(ERRCODE_SYNTAX_ERROR)`).
    fn getdiag_invalid(&self, kind: PLpgSQL_getdiag_kind, is_stacked: bool, loc: i32) -> PgError {
        let kindname = funcs::plpgsql_getdiag_kindname(kind);
        let msg = if is_stacked {
            format!("diagnostics item {kindname} is not allowed in GET STACKED DIAGNOSTICS")
        } else {
            format!("diagnostics item {kindname} is not allowed in GET CURRENT DIAGNOSTICS")
        };
        self.syntax_at(&msg, loc)
    }

    /// RETURN-with-value not allowed for the function's shape. (`make_return_stmt`
    /// / `make_return_next_stmt`.) The procedure case is `ERRCODE_SYNTAX_ERROR`;
    /// the rest are `ERRCODE_DATATYPE_MISMATCH`. The returning-set case also
    /// carries the `Use RETURN NEXT or RETURN QUERY.` hint.
    fn return_param_error(&self, which: &str, _is_proc: bool, loc: i32) -> PgError {
        let (msg, syntax, hint): (&str, bool, Option<&str>) = match which {
            "set" => (
                "RETURN cannot have a parameter in function returning set",
                false,
                Some("Use RETURN NEXT or RETURN QUERY."),
            ),
            "void" => (
                "RETURN cannot have a parameter in function returning void",
                false,
                None,
            ),
            "procedure" => ("RETURN cannot have a parameter in a procedure", true, None),
            "out" => (
                "RETURN cannot have a parameter in function with OUT parameters",
                false,
                None,
            ),
            "next_out" => (
                "RETURN NEXT cannot have a parameter in function with OUT parameters",
                false,
                None,
            ),
            _ => ("RETURN cannot have a parameter", false, None),
        };
        let mut builder = ereport(ERROR_LEVEL)
            .errcode(if syntax {
                ERRCODE_SYNTAX_ERROR
            } else {
                ERRCODE_DATATYPE_MISMATCH
            })
            .errmsg_internal(msg);
        if let Some(h) = hint {
            builder = builder.errhint_internal(h);
        }
        let mut err = builder.into_error();
        let pos = self.scanner.plpgsql_scanner_errposition(loc);
        if pos > 0 {
            err = err.with_internal_position(pos);
        }
        err
    }

    /// `errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg(...),
    /// parser_errposition(loc)`.
    fn null_value_not_allowed(&self, message: &str, loc: i32) -> PgError {
        let mut err = ereport(ERROR_LEVEL)
            .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .errmsg_internal(message)
            .into_error();
        let pos = self.scanner.plpgsql_scanner_errposition(loc);
        if pos > 0 {
            err = err.with_internal_position(pos);
        }
        err
    }

    /// `errmsg("too many INTO variables specified"), parser_errposition(loc)`.
    fn too_many_into(&self, loc: i32) -> PgError {
        self.syntax_at("too many INTO variables specified", loc)
    }

    /// `errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("FETCH statement cannot
    /// return multiple rows"), parser_errposition(loc)`.
    fn fetch_multi_rows(&self, loc: i32) -> PgError {
        let mut err = ereport(ERROR_LEVEL)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg_internal("FETCH statement cannot return multiple rows")
            .into_error();
        let pos = self.scanner.plpgsql_scanner_errposition(loc);
        if pos > 0 {
            err = err.with_internal_position(pos);
        }
        err
    }
}

// ===========================================================================
// Free-standing helpers (file-local, no parser state needed).
// ===========================================================================

/// Per-parse working struct mirroring the bison `%union` `declhdr` member.
struct DeclHdr {
    label: Option<String>,
    n_initvars: i32,
    initvarnos: Vec<i32>,
}

/// Mirrors the bison `%union` `varname` member.
struct VarName {
    name: String,
    lineno: i32,
}

/// Mirrors the bison `%union` `forvariable` member.
struct ForVariable {
    name: String,
    lineno: i32,
    location: i32,
    scalar: Option<i32>,
    row: Option<i32>,
}

/// `make_plpgsql_expr(query, parsemode)`.
fn make_plpgsql_expr(query: &str, parsemode: RawParseMode) -> Box<PLpgSQL_expr> {
    mem::boxed(PLpgSQL_expr {
        query: mem::sdup(query),
        parseMode: parsemode,
        func: comp_seam::curr_compile_handle::call(),
        ns: funcs::plpgsql_ns_top(),
        target_param: -1,
        target_is_local: false,
        plan: None,
        paramnos: None,
        expr_simple_expr: None,
        expr_simple_type: 0,
        expr_simple_typmod: 0,
        expr_simple_mutable: false,
        expr_rwopt: PLpgSQL_rwopt::PLPGSQL_RWOPT_UNKNOWN,
        expr_rw_param: None,
        expr_simple_plansource: None,
        expr_simple_plan: None,
        expr_simple_plan_lxid: 0,
        expr_simple_state: None,
        expr_simple_in_use: false,
        expr_simple_lxid: 0,
    })
}

/// `NameOfDatum(wdatum)`.
fn name_of_datum(wdatum: &PLwdatum) -> String {
    if let Some(ident) = &wdatum.ident {
        return ident.clone();
    }
    debug_assert!(!wdatum.idents.is_empty());
    wdatum.idents.join(".")
}

/// `format_type_be` is a comp/SQL-engine helper; for the collation error message
/// the grammar only needs a placeholder OID rendering. Until the type-name
/// formatter is reachable, render the OID itself (the real text lands with comp).
fn type_be_placeholder(typoid: Oid) -> String {
    typoid.to_string()
}

/// True if a datum dtype may be returned directly by RETURN / RETURN NEXT.
fn is_returnable_datum_dtype(dtype: PLpgSQL_datum_type) -> bool {
    matches!(
        dtype,
        PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_REC
    )
}

/// `scanner_isspace(ch)` — the SQL scanner's whitespace test.
fn scanner_isspace(ch: u8) -> bool {
    ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r' || ch == 0x0c
}

/// True if `s` is a valid SQLSTATE body: exactly the chars `0-9A-Z`.
fn is_valid_sqlstate(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_digit() || (b'A'..=b'Z').contains(&b))
}

/// `MAKE_SQLSTATE(ch1..ch5)` (`utils/elog.h`).
fn make_sqlstate(ch1: u8, ch2: u8, ch3: u8, ch4: u8, ch5: u8) -> i32 {
    let sixbit = |c: u8| -> i32 { ((c.wrapping_sub(b'0')) & 0x3F) as i32 };
    sixbit(ch1)
        + (sixbit(ch2) << 6)
        + (sixbit(ch3) << 12)
        + (sixbit(ch4) << 18)
        + (sixbit(ch5) << 24)
}
