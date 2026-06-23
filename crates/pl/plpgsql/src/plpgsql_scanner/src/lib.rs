//! Lexical scanning for PL/pgSQL — a faithful port of `pl_scanner.c`.
//!
//! `plpgsql_yylex` is the token reader the PL/pgSQL grammar (`pl_gram.y`) calls.
//! It wraps the *core* SQL scanner (`scan.l`, reached here through the
//! [`core_yylex`](scan_seams::core_yylex) seam) and adds:
//!
//!   * a small token-pushback stack (used to "un-read" lookahead while parsing
//!     compound dotted names like `A.B.C`);
//!   * PL/pgSQL variable recognition — an identifier (or dotted identifier) that
//!     names a function variable is returned as `T_DATUM` (resolved via the
//!     `plpgsql_parse_*` seams owned by `pl_comp.c`), a non-resolving dotted
//!     name as `T_CWORD`, and a non-resolving simple name as `T_WORD` or an
//!     unreserved-keyword token;
//!   * recognition of the operator tokens `<<` / `>>` / `#` that the core lexer
//!     hands back as a generic `Op`.
//!
//! ## Reserved keywords (a deliberate divergence in *mechanism*, not behavior)
//!
//! In C, `plpgsql_scanner_init` hands PL/pgSQL's *reserved* keyword list to the
//! core scanner, so the core returns the PL/pgSQL `K_*` token codes for those
//! words directly. The repo's `core_yylex` seam is stateless and takes no
//! per-caller keyword list, so it can only resolve the *core SQL* keywords.
//! `internal_yylex` therefore performs the reserved-PL-keyword reclassification
//! itself: any token the core returns as `IDENT` (or as a core SQL keyword)
//! whose spelling appears in [`RESERVED_PL_KEYWORDS`] is rewritten to its `K_*`
//! token, reproducing exactly what the C core-with-PL-keyword-list returns.
//!
//! ## Owned-value model
//!
//! The opaque `yyscan_t` + struct-punned `yyextra` become an owned
//! [`PlpgsqlScanner`] whose entry points are inherent methods. The `YYSTYPE`
//! union (restricted to the subset the scanner produces) is [`Yystype`]; a
//! token's location (`YYLTYPE`, a byte offset) is [`Yyltype`]. The scanner owns
//! the [`Mcx`] arena and a borrow of the NUL-padded scan buffer; lexed token
//! strings are copied out of the arena-owned `core_yylex` result into owned
//! `String`s so the scanner state stays self-contained.

extern crate alloc;

use alloc::string::{String, ToString};

use ::scan_fgram::tokens;
use scan_seams as scan_seam;
use comp_seams::{self as comp_seam, CwordResolution, WordResolution};
use ::mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};
use plpgsql::{IdentifierLookup, PLcword, PLwdatum, PLword};

// ===========================================================================
// Token codes (pl_gram.h `enum yytokentype`).
//
// Bison numbers single-character tokens by their ASCII value and named tokens
// sequentially from 258 in declaration order. The non-keyword tokens
// (`IDENT`/`Op`/`PARAM`/`COLON_EQUALS`/...) share the same numbering as the
// *core* gram.h, so we source them from the core `tokens` module to stay in
// lockstep with what `core_yylex` returns. The PL/pgSQL-specific tokens
// (`T_WORD`/`T_CWORD`/`T_DATUM`/`LESS_LESS`/`GREATER_GREATER` and every `K_*`)
// exist only in pl_gram.h and are defined here verbatim from it.
// ===========================================================================

// Non-keyword tokens shared with the core grammar (same codes):
pub use tokens::{COLON_EQUALS, IDENT, Op, PARAM, UIDENT};

// Tokens recognized by plpgsql's lexer interface layer (pl_scanner.c):
pub const T_WORD: i32 = 275;
pub const T_CWORD: i32 = 276;
pub const T_DATUM: i32 = 277;
pub const LESS_LESS: i32 = 278;
pub const GREATER_GREATER: i32 = 279;

// Keyword tokens, in pl_gram.y declaration order (which is *not* the ASCII
// order of the keyword lists — the lists are independent).
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

// ===========================================================================
// Keyword lists (pl_reserved_kwlist.h / pl_unreserved_kwlist.h).
//
// Each is an ASCII-ordered (name, token-code) table. In C the reserved list is
// handed to the core scanner via `scanner_init`; here `internal_yylex` consults
// it directly (see the module docs). The unreserved list is consulted by
// `plpgsql_yylex` after variable lookup fails, exactly as in C.
// ===========================================================================

/// Reserved PL/pgSQL keywords (`pl_reserved_kwlist.h`), ASCII-ordered.
pub static RESERVED_PL_KEYWORDS: &[(&'static str, i32)] = &[
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

/// Unreserved PL/pgSQL keywords (`pl_unreserved_kwlist.h`), ASCII-ordered.
pub static UNRESERVED_PL_KEYWORDS: &[(&'static str, i32)] = &[
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

/// `ScanKeywordLookup(str, keywords)` (`common/kwlookup.c`) — index of `str` in
/// the keyword table, or -1.
///
/// The match is ASCII-case-insensitive (downcasing only `'A'..'Z'`, per the
/// SQL99 rule, never the locale-aware `tolower`). The real C version uses a
/// perfect hash; the *contract* is just "index of the matching keyword, or -1",
/// which a linear scan over the ASCII-ordered table satisfies identically.
pub fn scan_keyword_lookup(s: &str, keywords: &[(&'static str, i32)]) -> i32 {
    // Reject immediately if too long to be any keyword.
    let max_kw_len = keywords.iter().map(|(kw, _)| kw.len()).max().unwrap_or(0);
    if s.len() > max_kw_len {
        return -1;
    }

    let sbytes = s.as_bytes();
    for (h, (kw, _)) in keywords.iter().enumerate() {
        let kwbytes = kw.as_bytes();
        if kwbytes.len() != sbytes.len() {
            continue;
        }
        let mut matched = true;
        for (&sb, &kb) in sbytes.iter().zip(kwbytes.iter()) {
            let mut ch = sb;
            if ch.is_ascii_uppercase() {
                ch += b'a' - b'A';
            }
            if ch != kb {
                matched = false;
                break;
            }
        }
        if matched {
            return h as i32;
        }
    }
    -1
}

/// `GetScanKeyword(n, keywords)` — canonical (lowercase) spelling of the n'th
/// keyword.
pub fn get_scan_keyword(n: i32, keywords: &[(&'static str, i32)]) -> &'static str {
    keywords[n as usize].0
}

// ===========================================================================
// AT_STMT_START macro — tokens that can immediately precede a PL/pgSQL
// executable statement (proc_sect / proc_stmt in the grammar).
// ===========================================================================

#[inline]
fn at_stmt_start(prev_token: i32) -> bool {
    prev_token == (';' as i32)
        || prev_token == K_BEGIN
        || prev_token == K_THEN
        || prev_token == K_ELSE
        || prev_token == K_LOOP
}

// ===========================================================================
// YYSTYPE / YYLTYPE.
// ===========================================================================

/// `YYLTYPE` — a token location, a byte offset from the start of the source
/// (`#define YYLTYPE int`).
pub type Yyltype = i32;

/// `YYSTYPE` — the bison semantic-value union, restricted to the subset the
/// scanner produces and consumes.
///
/// In C this is a real union whose first members overlay `core_YYSTYPE`
/// (`ival` / `str` / `keyword`); we keep them as separate fields and treat the
/// core scanner's output as the authoritative source the scanner copies `str` /
/// `ival` out of.
#[derive(Debug, Clone, Default)]
pub struct Yystype {
    /// `char *str` — identifier / non-integer-literal text (overlays
    /// `core_yystype.str`).
    pub str: Option<String>,
    /// `int ival` — integer literal / PARAM number (overlays
    /// `core_yystype.ival`).
    pub ival: i32,
    /// `const char *keyword` — canonical keyword spelling (overlays
    /// `core_yystype.keyword`).
    pub keyword: Option<String>,
    /// `PLword word` — an unrecognized simple identifier (for `T_WORD`).
    pub word: Option<PLword>,
    /// `PLcword cword` — an unrecognized composite identifier (for `T_CWORD`).
    pub cword: Option<PLcword>,
    /// `PLwdatum wdatum` — a resolved VAR/ROW/REC/RECFIELD (for `T_DATUM`).
    pub wdatum: Option<PLwdatum>,
}

// ===========================================================================
// Auxiliary per-token data + scanner working state.
// ===========================================================================

const MAX_PUSHBACKS: usize = 4;

/// Auxiliary data about a token, other than its token type (`TokenAuxData`).
#[derive(Debug, Clone, Default)]
struct TokenAuxData {
    /// Semantic information (`YYSTYPE lval`).
    lval: Yystype,
    /// Offset in scanbuf (`YYLTYPE lloc`).
    lloc: Yyltype,
    /// Length in bytes (`int leng`).
    leng: i32,
}

/// Scanner working state (`struct plpgsql_yy_extra_type` + the `yyscan_t` it
/// hangs off of), collapsed into one owned struct whose entry points are
/// inherent methods taking `&mut self`.
pub struct PlpgsqlScanner<'mcx> {
    /// Memory arena for the (stateless) core lexer's per-token allocations.
    mcx: Mcx<'mcx>,

    /// The scan buffer fed to the core lexer (`core_yy_extra.scanbuf`): the
    /// query bytes. The repo's `core_yylex` seam is stateless, so unlike C's
    /// in-place flex buffer this is never mutated; locations index into it.
    scanbuf: &'mcx [u8],

    /// The byte position the core lexer resumes at on the next call (the
    /// stateless replacement for flex's internal buffer position).
    pos: i32,

    /// The original input string (`scanorig`). In the stateless model the
    /// scan buffer is never modified, so `scanorig` and `scanbuf` carry the same
    /// bytes; we keep an owned `String` for the error/line-number paths.
    scanorig: String,

    /// Current token's length, corresponding to the last `plpgsql_yylval` /
    /// `plpgsql_yylloc` (`plpgsql_yyleng`).
    plpgsql_yyleng: i32,

    /// Current token's code, corresponding to the last `plpgsql_yylval` /
    /// `plpgsql_yylloc` (`plpgsql_yytoken`).
    plpgsql_yytoken: i32,

    /// Number of tokens currently on the pushback stack (`num_pushbacks`).
    num_pushbacks: usize,
    /// Pushback token-code stack (`pushback_token[MAX_PUSHBACKS]`).
    pushback_token: [i32; MAX_PUSHBACKS],
    /// Pushback auxiliary-data stack (`pushback_auxdata[MAX_PUSHBACKS]`).
    pushback_auxdata: [TokenAuxData; MAX_PUSHBACKS],

    /// State for `plpgsql_location_to_lineno()`: byte offset of the current
    /// line's start within `scanorig` (`cur_line_start`).
    cur_line_start: usize,
    /// Byte offset of the current line's terminating '\n' within `scanorig`, or
    /// `None` if there is no further newline (`cur_line_end == NULL`).
    cur_line_end: Option<usize>,
    /// Current line number (`cur_line_num`).
    cur_line_num: i32,

    /// Klugy flag telling the scanner how to look up identifiers
    /// (`plpgsql_IdentifierLookup`). A global in C; kept here so it is owned and
    /// threaded explicitly.
    pub identifier_lookup: IdentifierLookup,
}

impl<'mcx> PlpgsqlScanner<'mcx> {
    // -----------------------------------------------------------------------
    // plpgsql_yylex — the yylex routine the PL/pgSQL grammar calls.
    // -----------------------------------------------------------------------

    /// `plpgsql_yylex(yylvalp, yyllocp, yyscanner)` — return the next token.
    ///
    /// Returns `(token_code, lval, lloc)`, where `lval`/`lloc` are the
    /// `*yylvalp` / `*yyllocp` outputs of the C signature. `Err` carries a
    /// lexer error (a core-scanner `ereport(ERROR)` or "too many tokens pushed
    /// back").
    pub fn plpgsql_yylex(&mut self) -> PgResult<(i32, Yystype, Yyltype)> {
        let mut aux1 = TokenAuxData::default();
        let mut tok1 = self.internal_yylex(&mut aux1)?;

        if tok1 == IDENT || tok1 == PARAM {
            let mut aux2 = TokenAuxData::default();
            let tok2 = self.internal_yylex(&mut aux2)?;
            if tok2 == ('.' as i32) {
                let mut aux3 = TokenAuxData::default();
                let tok3 = self.internal_yylex(&mut aux3)?;
                if tok3 == IDENT {
                    let mut aux4 = TokenAuxData::default();
                    let tok4 = self.internal_yylex(&mut aux4)?;
                    if tok4 == ('.' as i32) {
                        let mut aux5 = TokenAuxData::default();
                        let tok5 = self.internal_yylex(&mut aux5)?;
                        if tok5 == IDENT {
                            match comp_seam::plpgsql_parse_tripword::call(
                                aux1.lval.str.as_deref().unwrap_or(""),
                                aux3.lval.str.as_deref().unwrap_or(""),
                                aux5.lval.str.as_deref().unwrap_or(""),
                            )? {
                                CwordResolution::Datum(wdatum) => {
                                    aux1.lval.wdatum = Some(wdatum);
                                    tok1 = T_DATUM;
                                }
                                CwordResolution::Cword(cword) => {
                                    aux1.lval.cword = Some(cword);
                                    tok1 = T_CWORD;
                                }
                            }
                            // Adjust token length to include A.B.C
                            aux1.leng = aux5.lloc - aux1.lloc + aux5.leng;
                        } else {
                            // not A.B.C, so just process A.B
                            self.push_back_token(tok5, &aux5)?;
                            self.push_back_token(tok4, &aux4)?;
                            tok1 = self.finish_dblword(&mut aux1, &aux3)?;
                        }
                    } else {
                        // not A.B.C, so just process A.B
                        self.push_back_token(tok4, &aux4)?;
                        tok1 = self.finish_dblword(&mut aux1, &aux3)?;
                    }
                } else {
                    // not A.B, so just process A
                    self.push_back_token(tok3, &aux3)?;
                    self.push_back_token(tok2, &aux2)?;
                    let yytxt = self.scanbuf_token_span(aux1.lloc, aux1.lloc + aux1.leng);
                    let res = comp_seam::plpgsql_parse_word::call(
                        aux1.lval.str.as_deref().unwrap_or(""),
                        &yytxt,
                        true,
                    )?;
                    tok1 = self.finish_word(&mut aux1, res);
                }
            } else {
                // not A.B, so just process A
                self.push_back_token(tok2, &aux2)?;

                // See if it matches a variable name, except in the context where
                // we are at start of statement and the next token isn't
                // assignment or '['. In that case, it couldn't validly be a
                // variable name, and skipping the lookup allows variable names to
                // be used that would conflict with plpgsql or core keywords that
                // introduce statements (e.g., "comment").
                //
                // If it isn't a variable name, try to match against unreserved
                // plpgsql keywords. If not one of those either, it's T_WORD.
                //
                // Note: we must call plpgsql_parse_word even if we don't want to
                // do variable lookup, because it sets up aux1.lval.word for the
                // non-variable cases.
                let lookup = !at_stmt_start(self.plpgsql_yytoken)
                    || (tok2 == ('=' as i32)
                        || tok2 == COLON_EQUALS
                        || tok2 == ('[' as i32));
                let yytxt = self.scanbuf_token_span(aux1.lloc, aux1.lloc + aux1.leng);
                let res = comp_seam::plpgsql_parse_word::call(
                    aux1.lval.str.as_deref().unwrap_or(""),
                    &yytxt,
                    lookup,
                )?;
                tok1 = self.finish_word(&mut aux1, res);
            }
        } else {
            // Not a potential plpgsql variable name, just return the data.
            //
            // Note that we also come through here if the grammar pushed back a
            // T_DATUM, T_CWORD, T_WORD, or unreserved-keyword token returned by a
            // previous lookup cycle; thus, pushbacks do not incur extra lookup
            // work, since we'll never do the above code twice for the same token.
            // This property also makes it safe to rely on the old value of
            // plpgsql_yytoken in the is-this-start-of-statement test above.
        }

        let lval = aux1.lval.clone();
        let lloc = aux1.lloc;
        self.plpgsql_yyleng = aux1.leng;
        self.plpgsql_yytoken = tok1;
        Ok((tok1, lval, lloc))
    }

    /// Shared tail of the three "process A.B" arms: resolve the dotted pair,
    /// set `aux1` accordingly, adjust the token length to span `A.B`, and return
    /// the resulting token code (`T_DATUM` or `T_CWORD`).
    fn finish_dblword(&mut self, aux1: &mut TokenAuxData, aux3: &TokenAuxData) -> PgResult<i32> {
        let tok = match comp_seam::plpgsql_parse_dblword::call(
            aux1.lval.str.as_deref().unwrap_or(""),
            aux3.lval.str.as_deref().unwrap_or(""),
        )? {
            CwordResolution::Datum(wdatum) => {
                aux1.lval.wdatum = Some(wdatum);
                T_DATUM
            }
            CwordResolution::Cword(cword) => {
                aux1.lval.cword = Some(cword);
                T_CWORD
            }
        };
        // Adjust token length to include A.B
        aux1.leng = aux3.lloc - aux1.lloc + aux3.leng;
        Ok(tok)
    }

    /// Shared tail of the two "just process A" arms of `plpgsql_yylex`: turn the
    /// result of `plpgsql_parse_word` into a `T_DATUM` / unreserved-keyword /
    /// `T_WORD` token, mutating `aux1.lval` to carry the chosen payload.
    fn finish_word(&self, aux1: &mut TokenAuxData, res: WordResolution) -> i32 {
        match res {
            WordResolution::Datum(wdatum) => {
                aux1.lval.wdatum = Some(wdatum);
                T_DATUM
            }
            WordResolution::Word(word) => {
                aux1.lval.word = Some(word);
                if let Some(w) = &aux1.lval.word {
                    if !w.quoted {
                        let kwnum = scan_keyword_lookup(&w.ident, UNRESERVED_PL_KEYWORDS);
                        if kwnum >= 0 {
                            aux1.lval.keyword =
                                Some(get_scan_keyword(kwnum, UNRESERVED_PL_KEYWORDS).to_string());
                            return UNRESERVED_PL_KEYWORDS[kwnum as usize].1;
                        }
                    }
                }
                T_WORD
            }
        }
    }

    /// `plpgsql_token_length(yyscanner)` — length of the token last returned by
    /// `plpgsql_yylex()`. For compound tokens, includes all parts.
    pub fn plpgsql_token_length(&self) -> i32 {
        self.plpgsql_yyleng
    }

    // -----------------------------------------------------------------------
    // internal_yylex — wraps the core lexer + adds the token pushback stack.
    // -----------------------------------------------------------------------

    fn internal_yylex(&mut self, auxdata: &mut TokenAuxData) -> PgResult<i32> {
        if self.num_pushbacks > 0 {
            self.num_pushbacks -= 1;
            let token = self.pushback_token[self.num_pushbacks];
            *auxdata = self.pushback_auxdata[self.num_pushbacks].clone();
            Ok(token)
        } else {
            let core = scan_seam::core_yylex::call(self.mcx, self.scanbuf, self.pos)?;
            let token_end = core.end_pos;
            self.pos = core.end_pos;
            let mut token = core.token;
            auxdata.lloc = core.location;

            // Project the core_YYSTYPE union onto our YYSTYPE overlay. The core
            // seam carries the token's string value (IDENT/Op/SCONST/...) in
            // `str_value`; an empty value means the token has no string payload.
            auxdata.lval = Yystype::default();
            auxdata.lval.str = if core.str_value.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(&core.str_value).into_owned())
            };

            // remember the length of yytext before it gets changed.
            //
            // In C, flex zaps a NUL at the end of the matched token, so the
            // scanbuf-anchored `yytext` reads exactly the token via `strlen`.
            // The stateless core lexer here never mutates the scan buffer, so
            // the token text must be bounded by the lexer-reported token span
            // `[location, end_pos)` rather than scanned to the next NUL (which
            // would over-read to the end of the whole input).
            let yytext = self.scanbuf_token_span(auxdata.lloc, token_end);
            auxdata.leng = yytext.len() as i32;

            // Check for << >> and #, which the core considers operators
            if token == Op {
                let s = auxdata.lval.str.as_deref().unwrap_or("");
                if s == "<<" {
                    token = LESS_LESS;
                } else if s == ">>" {
                    token = GREATER_GREATER;
                } else if s == "#" {
                    token = '#' as i32;
                }
            }
            // The core returns PARAM as ival, but we treat it like IDENT
            else if token == PARAM {
                auxdata.lval.str = Some(yytext.clone());
            }
            // The core returns ICONST with its int32 value in `core_yystype.ival`,
            // but the stateless `CoreToken` seam carries only the token's *string*
            // value (empty for ICONST). Re-derive the integer from the token text
            // via the same parser the core scanner uses (`pg_strtoint32_safe`,
            // which understands 0x/0o/0b prefixes and `_` separators) so the
            // grammar's `yylval.ival` reads (K_SLICE ICONST, array subscripts) get
            // the value rather than a stale 0.
            else if token == tokens::ICONST {
                use ::utils_error::SoftErrorContext;
                let mut escontext = SoftErrorContext::new(false);
                if let Ok(v) = numutils::pg_strtoint32_safe(
                    &yytext,
                    Some(&mut escontext),
                ) {
                    if !escontext.error_occurred() {
                        auxdata.lval.ival = v;
                    }
                }
            }

            // Reserved-PL-keyword reclassification.
            //
            // In C the core scanner is handed PL/pgSQL's *reserved* keyword list
            // (with the PL token values), and the core scanner matches an
            // unquoted identifier against the keyword list it was given before
            // returning IDENT — so a word like `begin`, `if`, or `case` comes
            // back already bearing the PL `K_*` token (the PL list takes
            // precedence over, or supplies, the core keyword token).
            //
            // The repo's stateless `core_yylex` takes no per-caller keyword
            // list: it only resolves the *core SQL* keywords, returning IDENT
            // for PL-only reserved words (`begin`/`if`/`loop`/...) and the core
            // keyword token for words that are reserved in both grammars
            // (`case`/`else`/`for`/...). We reproduce scanner_init's effect by
            // matching the token's original *unquoted* source text against the
            // PL reserved list and, on a hit, substituting the PL `K_*` token.
            // This covers both cases (IDENT and core-keyword) uniformly.
            //
            // Only the unquoted {identifier} flex rule consults a keyword list
            // in C; a double-quoted identifier (the <xd> rule) is returned
            // verbatim and never matched. We detect the quoted case exactly as
            // `plpgsql_parse_word` does — the original source text begins with a
            // double quote — and skip reclassification for it. `yytext` is the
            // original source text at this token's location (computed above).
            let quoted = yytext.as_bytes().first() == Some(&b'"');
            if !quoted {
                let kwnum = scan_keyword_lookup(&yytext, RESERVED_PL_KEYWORDS);
                if kwnum >= 0 {
                    token = RESERVED_PL_KEYWORDS[kwnum as usize].1;
                } else if token != IDENT
                    && token != UIDENT
                    && token != PARAM
                    && is_identifier_word(&yytext)
                {
                    // The repo's stateless `core_yylex` resolves the *full* core
                    // SQL keyword list, so an unquoted word that is a core SQL
                    // keyword (`return`, `call`, `fetch`, `table`, `default`, ...)
                    // comes back bearing that core keyword's token. But C hands
                    // the core scanner *only* PL/pgSQL's reserved list, so any
                    // word not in that list — including a core SQL keyword that is
                    // an *unreserved* (or non-) PL keyword — is returned as plain
                    // `IDENT`. Force `IDENT` here so the `plpgsql_yylex` IDENT
                    // path runs (variable lookup + the `UnreservedPLKeywords`
                    // reclassification in `finish_word`), reproducing
                    // `plpgsql_scanner_init`'s reserved-only core keyword list.
                    token = IDENT;
                }
            }

            Ok(token)
        }
    }

    /// `push_back_token(token, auxdata, yyscanner)` — push a token to be re-read
    /// by the next `internal_yylex()` call.
    fn push_back_token(&mut self, token: i32, auxdata: &TokenAuxData) -> PgResult<()> {
        if self.num_pushbacks >= MAX_PUSHBACKS {
            return Err(PgError::error("too many tokens pushed back"));
        }
        self.pushback_token[self.num_pushbacks] = token;
        self.pushback_auxdata[self.num_pushbacks] = auxdata.clone();
        self.num_pushbacks += 1;
        Ok(())
    }

    /// `plpgsql_push_back_token(token, yylvalp, yyllocp, yyscanner)` — push back
    /// a single token to be re-read by the next `plpgsql_yylex()` call.
    ///
    /// NOTE: this does not cause yylval or yylloc to "back up". Also, it is not
    /// a good idea to push back a token code other than what you read.
    pub fn plpgsql_push_back_token(
        &mut self,
        token: i32,
        yylval: &Yystype,
        yylloc: Yyltype,
    ) -> PgResult<()> {
        let auxdata = TokenAuxData {
            lval: yylval.clone(),
            lloc: yylloc,
            leng: self.plpgsql_yyleng,
        };
        self.push_back_token(token, &auxdata)
    }

    /// `plpgsql_append_source_text(buf, startlocation, endlocation, yyscanner)`
    /// — append the function text in `[startlocation, endlocation)` onto `buf`.
    ///
    /// `buf` is the PL/pgSQL-private appendable buffer (a `StringInfo` in C);
    /// modeled here as an owned `String`.
    pub fn plpgsql_append_source_text(
        &self,
        buf: &mut String,
        startlocation: i32,
        endlocation: i32,
    ) {
        assert!(startlocation <= endlocation); // Assert(startlocation <= endlocation)
        let start = startlocation as usize;
        let end = endlocation as usize;
        // appendBinaryStringInfo(buf, scanorig + startlocation, endlocation - startlocation)
        buf.push_str(&self.scanorig[start..end]);
    }

    /// `plpgsql_peek(yyscanner)` — peek one token ahead. Only the token code is
    /// made available, not any auxiliary info.
    ///
    /// NB: no variable or unreserved-keyword lookup is performed here; they will
    /// be returned as IDENT. Reserved keywords are resolved as usual.
    pub fn plpgsql_peek(&mut self) -> PgResult<i32> {
        let mut aux1 = TokenAuxData::default();
        let tok1 = self.internal_yylex(&mut aux1)?;
        self.push_back_token(tok1, &aux1)?;
        Ok(tok1)
    }

    /// `plpgsql_peek2(tok1_p, tok2_p, tok1_loc, tok2_loc, yyscanner)` — peek two
    /// tokens ahead. Returns `(tok1, tok2, tok1_loc, tok2_loc)`.
    ///
    /// NB: no variable or unreserved-keyword lookup is performed here; they will
    /// be returned as IDENT. Reserved keywords are resolved as usual.
    pub fn plpgsql_peek2(&mut self) -> PgResult<(i32, i32, i32, i32)> {
        let mut aux1 = TokenAuxData::default();
        let mut aux2 = TokenAuxData::default();

        let tok1 = self.internal_yylex(&mut aux1)?;
        let tok2 = self.internal_yylex(&mut aux2)?;

        let tok1_loc = aux1.lloc;
        let tok2_loc = aux2.lloc;

        self.push_back_token(tok2, &aux2)?;
        self.push_back_token(tok1, &aux1)?;

        Ok((tok1, tok2, tok1_loc, tok2_loc))
    }

    /// `plpgsql_scanner_errposition(location, yyscanner)` — report an error
    /// cursor position, if possible. Returns the 1-based character cursor (or 0
    /// if the location is unknown), to be attached to an in-flight `PgError`.
    ///
    /// In C this calls `internalerrposition(pos)` + `internalerrquery(scanorig)`
    /// on the ambient ereport; in the value-error model the caller
    /// ([`Self::plpgsql_yyerror`]) attaches the position and the query body to
    /// the constructed `PgError` instead.
    pub fn plpgsql_scanner_errposition(&self, location: i32) -> i32 {
        if location < 0 {
            return 0; // no-op if location is unknown
        }
        // Convert byte offset to character number. `scanorig` is the source
        // text already accepted by the lexer, so it is valid in the server
        // encoding and `pg_mbstrlen_with_len` cannot actually report an invalid
        // byte sequence here (in C it would longjmp; the infallible C signature
        // relies on this). If the dead error path is ever reached, fall back to
        // the byte offset as a best-effort cursor rather than panicking.
        match mbutils_seams::pg_mbstrlen_with_len::call(
            self.scanorig.as_bytes(),
            location,
        ) {
            Ok(nchars) => nchars + 1,
            Err(_) => location + 1,
        }
    }

    /// `plpgsql_yyerror(yyllocp, plpgsql_parse_result_p, yyscanner, message)` —
    /// build a lexer/grammar `ereport(ERROR, ERRCODE_SYNTAX_ERROR)` whose cursor
    /// refers to the current token (the one last returned by `plpgsql_yylex()`).
    pub fn plpgsql_yyerror(&self, yylloc: Yyltype, message: &str) -> PgError {
        // char *yytext = yyextra->core_yy_extra.scanbuf + *yyllocp;
        let buf = self.scanbuf;
        let off = yylloc as usize;
        let first_byte = buf.get(off).copied().unwrap_or(0);

        if first_byte == b'\0' {
            // translator: %s is typically the translation of "syntax error"
            let msg = alloc::format!("{} at end of input", message);
            self.syntax_error(&msg, yylloc)
        } else {
            // If we have done any lookahead then flex would have restored the
            // character after the end-of-token; the single-token report wants
            // only the token. In the stateless model the buffer is never mutated,
            // so we slice exactly `plpgsql_yyleng` bytes from the token start
            // instead of zapping a NUL into the buffer (same observable text).
            let end = (off + self.plpgsql_yyleng.max(0) as usize).min(buf.len());
            let yytext = String::from_utf8_lossy(&buf[off..end]).into_owned();
            // translator: first %s is typically the translation of "syntax error"
            let msg = alloc::format!("{} at or near \"{}\"", message, yytext);
            self.syntax_error(&msg, yylloc)
        }
    }

    /// Build the `ERRCODE_SYNTAX_ERROR` `PgError` with the scanner's error
    /// position (`internalerrposition`) and the function body
    /// (`internalerrquery`), as `plpgsql_scanner_errposition` does in C.
    ///
    /// This is the `ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR), errmsg(msg),
    /// parser_errposition(location))` form — the message is used verbatim, with
    /// no "at or near <token>" suffix (that suffix is added only by the bison
    /// `plpgsql_yyerror` callback).
    pub fn syntax_error_at(&self, msg: &str, location: i32) -> PgError {
        self.syntax_error(msg, location)
    }

    fn syntax_error(&self, msg: &str, location: i32) -> PgError {
        self.positioned_error(
            ::types_error::ERROR,
            ERRCODE_SYNTAX_ERROR,
            msg,
            location,
        )
    }

    /// Build a `PgError` at `level` with `sqlstate`/`msg`, attaching the scanner
    /// error position (`internalerrposition`) and the function body
    /// (`internalerrquery`) when `location` is known, exactly as
    /// `plpgsql_scanner_errposition` does in C's `parser_errposition`.  Used for
    /// the direct-`ereport` grammar sites (e.g. the `decl_varname`
    /// shadowed-variables WARNING/ERROR) so they render with `LINE n: ... ^`.
    pub fn positioned_error(
        &self,
        level: ::types_error::ErrorLevel,
        sqlstate: ::types_error::SqlState,
        msg: &str,
        location: i32,
    ) -> PgError {
        let mut err = PgError::new(level, msg.to_string()).with_sqlstate(sqlstate);
        if location >= 0 {
            let pos = self.plpgsql_scanner_errposition(location);
            err = err
                .with_internal_position(pos)
                .with_internal_query(self.scanorig.clone());
        }
        err
    }

    /// The original PL/pgSQL function source (`scanorig` / the C `prosrc`),
    /// needed by `function_parse_error_transpose` to relocate a body-relative
    /// error position into the original CREATE FUNCTION / DO query text.
    pub fn scanorig(&self) -> &str {
        &self.scanorig
    }

    /// `plpgsql_location_to_lineno(location, yyscanner)` — map a byte offset in
    /// the source text to a line number.
    ///
    /// Typically called for a sequence of increasing locations, so we optimize
    /// by tracking the endpoints of the "current" line.
    pub fn plpgsql_location_to_lineno(&mut self, location: i32) -> i32 {
        if location < 0 {
            return 0; // garbage in, garbage out
        }
        let loc = location as usize; // loc = scanorig + location

        // be correct, but not fast, if input location goes backwards
        if loc < self.cur_line_start {
            self.location_lineno_init();
        }

        // while (cur_line_end != NULL && loc > cur_line_end)
        while let Some(cur_line_end) = self.cur_line_end {
            if loc <= cur_line_end {
                break;
            }
            self.cur_line_start = cur_line_end + 1;
            self.cur_line_num += 1;
            self.cur_line_end = strchr_newline(&self.scanorig, self.cur_line_start);
        }

        self.cur_line_num
    }

    /// `location_lineno_init(yyscanner)` — initialize/reset the state for
    /// `plpgsql_location_to_lineno`.
    fn location_lineno_init(&mut self) {
        self.cur_line_start = 0; // cur_line_start = scanorig
        self.cur_line_num = 1;
        self.cur_line_end = strchr_newline(&self.scanorig, self.cur_line_start);
    }

    /// `plpgsql_latest_lineno(yyscanner)` — most recently computed lineno.
    pub fn plpgsql_latest_lineno(&self) -> i32 {
        self.cur_line_num
    }

    /// `plpgsql_scanner_finish(yyscanner)` — clean up after
    /// `plpgsql_scanner_init()`.
    ///
    /// In C this calls `scanner_finish` to release the core scanner's storage;
    /// here the core lexer is stateless (no live handle), and the scanner's
    /// owned state is freed by dropping `self`.
    pub fn plpgsql_scanner_finish(self) {
        // release storage: dropping `self` releases the owned scanner state.
    }

    /// The exact text of the token the core lexer just matched: the scan-buffer
    /// bytes in `[start, end)` (the lexer-reported token start and resume
    /// position). This mirrors flex's NUL-zapped `yytext` without mutating the
    /// (stateless) scan buffer. A NUL in the span (defensive; tokens never span
    /// one) still terminates the text.
    fn scanbuf_token_span(&self, start: i32, end: i32) -> String {
        let lo = start.max(0) as usize;
        let hi = (end.max(0) as usize).min(self.scanbuf.len());
        if lo >= hi {
            return String::new();
        }
        let slice = &self.scanbuf[lo..hi];
        let n = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
        String::from_utf8_lossy(&slice[..n]).into_owned()
    }
}

/// Is `text` an unquoted identifier-shaped word — the only token shape the core
/// SQL scanner ever returns as a keyword? (A keyword match requires the
/// `{identifier}` flex rule: a leading letter/`_`, then letters/digits/`_`/`$`.)
/// Used to tell a core-SQL-keyword token (which the reserved-only PL core
/// scanner of C would instead have returned as `IDENT`) apart from operator and
/// punctuation tokens, which must keep their token code.
fn is_identifier_word(text: &str) -> bool {
    let bytes = text.as_bytes();
    match bytes.first() {
        Some(&c) if c == b'_' || c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    bytes
        .iter()
        .all(|&c| c == b'_' || c == b'$' || c.is_ascii_alphanumeric())
}

/// `plpgsql_token_is_unreserved_keyword(token)` — is `token` an unreserved
/// keyword?
///
/// (If it is, its lowercased form was returned as the token value, so we do not
/// need to offer that data here.)
pub fn plpgsql_token_is_unreserved_keyword(token: i32) -> bool {
    UNRESERVED_PL_KEYWORDS
        .iter()
        .any(|(_, tokval)| *tokval == token)
}

/// `plpgsql_scanner_init(str)` — called before any actual parsing is done.
///
/// `mcx` is the arena the (stateless) core lexer allocates per-token strings in;
/// `scanbuf` is the NUL-terminated query buffer the core lexer scans (the bytes
/// of `str`); `str` is the original source text cited in error messages. In C
/// `str` must remain valid until `plpgsql_scanner_finish()`; here the scanner
/// owns a copy of `str` and borrows `scanbuf` for the lexer's lifetime.
pub fn plpgsql_scanner_init<'mcx>(
    mcx: Mcx<'mcx>,
    scanbuf: &'mcx [u8],
    str: &str,
) -> PlpgsqlScanner<'mcx> {
    let mut scanner = PlpgsqlScanner {
        mcx,
        scanbuf,
        pos: 0,
        // scanorig points to the original string, which unlike the scanner's
        // scanbuf won't be modified on-the-fly by flex.
        scanorig: str.to_string(),
        plpgsql_yyleng: 0,
        // Other setup: plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
        identifier_lookup: IdentifierLookup::IDENTIFIER_LOOKUP_NORMAL,
        plpgsql_yytoken: 0,
        num_pushbacks: 0,
        pushback_token: [0; MAX_PUSHBACKS],
        pushback_auxdata: Default::default(),
        cur_line_start: 0,
        cur_line_end: None,
        cur_line_num: 0,
    };

    scanner.location_lineno_init();

    scanner
}

/// Byte-offset analogue of `strchr(scanorig + from, '\n')`: the first '\n' at or
/// after byte offset `from` in `s`, returning its absolute byte offset, or
/// `None` if there is none (matching C's `strchr` returning NULL).
fn strchr_newline(s: &str, from: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if from > bytes.len() {
        return None;
    }
    bytes[from..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|rel| from + rel)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reserved/unreserved tables must stay ASCII-ordered (the C
    /// `gen_keywordlist.pl` invariant; `scan_keyword_lookup` relies on neither
    /// order nor uniqueness, but the C headers require ASCII order, so we keep
    /// it as a regression check).
    #[test]
    fn reserved_keywords_are_ascii_ordered() {
        for w in RESERVED_PL_KEYWORDS.windows(2) {
            assert!(w[0].0 <= w[1].0, "{} > {}", w[0].0, w[1].0);
        }
    }

    #[test]
    fn unreserved_keywords_are_ascii_ordered() {
        for w in UNRESERVED_PL_KEYWORDS.windows(2) {
            assert!(w[0].0 <= w[1].0, "{} > {}", w[0].0, w[1].0);
        }
    }

    /// No word may appear in both lists (C: "Be careful not to put the same
    /// word into both headers").
    #[test]
    fn reserved_and_unreserved_are_disjoint() {
        for (r, _) in RESERVED_PL_KEYWORDS {
            assert!(
                !UNRESERVED_PL_KEYWORDS.iter().any(|(u, _)| u == r),
                "{r} is in both keyword lists"
            );
        }
    }

    /// `scan_keyword_lookup` is ASCII-case-insensitive and returns the index of
    /// the matching keyword (or -1).
    #[test]
    fn scan_keyword_lookup_case_insensitive() {
        assert_eq!(scan_keyword_lookup("begin", RESERVED_PL_KEYWORDS), 1);
        assert_eq!(scan_keyword_lookup("BEGIN", RESERVED_PL_KEYWORDS), 1);
        assert_eq!(scan_keyword_lookup("BeGiN", RESERVED_PL_KEYWORDS), 1);
        assert_eq!(RESERVED_PL_KEYWORDS[1].1, K_BEGIN);
        assert_eq!(scan_keyword_lookup("notakeyword", RESERVED_PL_KEYWORDS), -1);
        // Over-long input is rejected before any comparison.
        assert_eq!(
            scan_keyword_lookup("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RESERVED_PL_KEYWORDS),
            -1
        );
    }

    /// Both `elseif` and `elsif` map to K_ELSIF (the C duplicate-spelling rule).
    #[test]
    fn elseif_and_elsif_both_map_to_k_elsif() {
        let a = scan_keyword_lookup("elseif", UNRESERVED_PL_KEYWORDS);
        let b = scan_keyword_lookup("elsif", UNRESERVED_PL_KEYWORDS);
        assert!(a >= 0 && b >= 0);
        assert_eq!(UNRESERVED_PL_KEYWORDS[a as usize].1, K_ELSIF);
        assert_eq!(UNRESERVED_PL_KEYWORDS[b as usize].1, K_ELSIF);
    }

    #[test]
    fn at_stmt_start_recognizes_the_five_predecessors() {
        assert!(at_stmt_start(';' as i32));
        assert!(at_stmt_start(K_BEGIN));
        assert!(at_stmt_start(K_THEN));
        assert!(at_stmt_start(K_ELSE));
        assert!(at_stmt_start(K_LOOP));
        assert!(!at_stmt_start(K_IF));
        assert!(!at_stmt_start(IDENT));
    }

    #[test]
    fn token_is_unreserved_keyword_classifies_correctly() {
        assert!(plpgsql_token_is_unreserved_keyword(K_ABSOLUTE));
        assert!(plpgsql_token_is_unreserved_keyword(K_WARNING));
        // Reserved keywords and non-keywords are not unreserved.
        assert!(!plpgsql_token_is_unreserved_keyword(K_BEGIN));
        assert!(!plpgsql_token_is_unreserved_keyword(T_WORD));
        assert!(!plpgsql_token_is_unreserved_keyword(IDENT));
    }

    #[test]
    fn strchr_newline_finds_next_newline() {
        assert_eq!(strchr_newline("abc\ndef", 0), Some(3));
        assert_eq!(strchr_newline("abc\ndef", 4), None);
        assert_eq!(strchr_newline("abc\ndef\n", 4), Some(7));
        assert_eq!(strchr_newline("no newline", 0), None);
        assert_eq!(strchr_newline("x", 99), None);
    }
}
