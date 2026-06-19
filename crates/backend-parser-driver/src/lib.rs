//! Port of `src/backend/parser/parser.c` (PostgreSQL 18.3): the main entry
//! point / driver for the PostgreSQL grammar.
//!
//! This crate ports parser.c's OWN, node-independent logic 1:1:
//!   * [`BaseLexer::base_yylex`] — the intermediate filter between the bison
//!     grammar and the core lexer (`core_yylex` in scan.l). It implements the
//!     one-token lookahead that merges multiword tokens (`NOT LIKE` → `NOT_LA`,
//!     `WITH TIME` → `WITH_LA`, `FORMAT JSON` → `FORMAT_LA`, …) and converts
//!     `UIDENT`/`USCONST` (Unicode-escaped) tokens into plain `IDENT`/`SCONST`
//!     via [`str_udeescape`].
//!   * [`raw_parser`] — the per-query entry: seed the lookahead with the
//!     `RawParseMode` mode token, drive the grammar, and return the list of
//!     `RawStmt` raw parse trees.
//!   * [`str_udeescape`], [`check_uescapechar`], `check_unicode_value`, `hexval`
//!     — the Unicode de-escaping support routines.
//!   * [`scanner_errposition`] (scan.l:1139) — the byte-offset → 1-based
//!     character-cursor conversion every lexer error location flows through.
//!
//! Three genuine externals are still UNPORTED and are reached through their
//! owners' seam crates (loud-panic until the owner lands, never silent stubs):
//!   * `core_yylex` (scan.l, `backend-parser-scan-seams`) — the core lexer.
//!     The C scanner is stateful and mutates a NUL-padded buffer; the seam
//!     models it *statelessly* (resume at a byte cursor, return token + resume
//!     point), so the filter logic runs unchanged.
//!   * `base_yyparse` (gram.y, `backend-parser-gram-seams`) — the bison parser;
//!     it reentrantly pulls tokens through `base_yylex`, so the driver reaches
//!     it across that cycle through the seam.
//!   * `pg_unicode_to_server` (mbutils.c, `backend-utils-mb-mbutils-seams`) —
//!     the code-point → server-encoding conversion `str_udeescape` calls.
//!   * `truncate_identifier` (scansup.c, `backend-parser-scansup-seams`) — the
//!     identifier truncation applied to a de-escaped `UIDENT`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use mcx::{Mcx, PgVec};
use types_core::PgWChar;
use types_error::PgResult;
use types_nodes::parsestmt::RawStmt;
use types_parsenodes::RawParseMode;

use backend_parser_gram_seams as gram;
use backend_parser_scan_seams as scan;
use backend_parser_scan_seams::CoreToken;
use backend_parser_scansup_seams as scansup;
use backend_utils_mb_mbutils_seams as mb;

mod udeescape;
pub use udeescape::{check_uescapechar, str_udeescape};

#[cfg(test)]
mod tests;

// ===========================================================================
// pg_wchar.h constants and surrogate helpers (used by str_udeescape).
// ===========================================================================

/// `MAX_UNICODE_EQUIVALENT_STRING` (`mb/pg_wchar.h:345`).
pub const MAX_UNICODE_EQUIVALENT_STRING: usize = 16;

/// `is_valid_unicode_codepoint(c)` (`mb/pg_wchar.h`).
pub fn is_valid_unicode_codepoint(c: PgWChar) -> bool {
    c > 0 && c <= 0x10FFFF
}
/// `is_utf16_surrogate_first(c)` (`mb/pg_wchar.h`).
pub fn is_utf16_surrogate_first(c: PgWChar) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}
/// `is_utf16_surrogate_second(c)` (`mb/pg_wchar.h`).
pub fn is_utf16_surrogate_second(c: PgWChar) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}
/// `surrogate_pair_to_codepoint(first, second)` (`mb/pg_wchar.h`).
pub fn surrogate_pair_to_codepoint(first: PgWChar, second: PgWChar) -> PgWChar {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

// ===========================================================================
// Grammar token codes (gram.h `enum yytokentype`, PostgreSQL 18.3).
// ===========================================================================

/// Grammar token codes used by [`BaseLexer::base_yylex`]'s multiword-merge
/// filter. Values are transcribed from the bison-generated `gram.h`
/// `enum yytokentype` for PostgreSQL 18.3's `gram.y`, matching what
/// `core_yylex` returns and what the grammar consumes. Single-character tokens
/// are their ASCII byte value and are not listed.
pub mod tokens {
    /// End-of-input token code (`yyterminate()` returns `YY_NULL`, i.e. 0).
    pub const YY_NULL: i32 = 0;

    pub const IDENT: i32 = 258;
    pub const UIDENT: i32 = 259;
    pub const SCONST: i32 = 261;
    pub const USCONST: i32 = 262;

    pub const BETWEEN: i32 = 307;
    pub const FIRST_P: i32 = 424;
    pub const FORMAT: i32 = 430;
    pub const ILIKE: i32 = 452;
    pub const IN_P: i32 = 457;
    pub const JSON: i32 = 484;
    pub const LAST_P: i32 = 501;
    pub const LIKE: i32 = 508;
    pub const NOT: i32 = 548;
    pub const NULLS_P: i32 = 555;
    pub const ORDINALITY: i32 = 572;
    pub const SIMILAR: i32 = 663;
    pub const TIME: i32 = 701;
    pub const UESCAPE: i32 = 715;
    pub const WITH: i32 = 748;
    pub const WITHOUT: i32 = 750;

    // The look-ahead-merged tokens parser.c synthesizes.
    pub const FORMAT_LA: i32 = 769;
    pub const NOT_LA: i32 = 770;
    pub const NULLS_LA: i32 = 771;
    pub const WITH_LA: i32 = 772;
    pub const WITHOUT_LA: i32 = 773;

    // The `RawParseMode` mode tokens (the seed for `base_yylex`'s lookahead).
    pub const MODE_TYPE_NAME: i32 = 774;
    pub const MODE_PLPGSQL_EXPR: i32 = 775;
    pub const MODE_PLPGSQL_ASSIGN1: i32 = 776;
    pub const MODE_PLPGSQL_ASSIGN2: i32 = 777;
    pub const MODE_PLPGSQL_ASSIGN3: i32 = 778;
}

/// End-of-input token code, re-exported for callers driving [`BaseLexer`].
pub const YY_NULL: i32 = tokens::YY_NULL;

// ===========================================================================
// scanner_errposition.
// ===========================================================================

/// `scanner_errposition(location, yyscanner)` (scan.l:1139) — convert a byte
/// offset within the scan buffer into the 1-based *character* cursor position
/// `errposition()` expects. Returns 0 (no-op) when `location` is negative
/// (unknown), exactly as the C routine does.
///
/// `scanbuf` is the scanner's input buffer (`yyextra->scanbuf`).
pub fn scanner_errposition(location: i32, scanbuf: &[u8]) -> i32 {
    if location < 0 {
        // No-op if location is unknown.
        return 0;
    }
    // Convert byte offset to character number (+1 for the 1-based cursor).
    mb::pg_mbstrlen_with_len::call(scanbuf, location) + 1
}

// ===========================================================================
// base_yylex: the grammar/scanner filter.
// ===========================================================================

/// A filtered token produced by [`BaseLexer::base_yylex`].
#[derive(Clone, Debug)]
pub struct Token<'mcx> {
    /// The (possibly merged) grammar token code.
    pub token: i32,
    /// The token's string semantic value (when it carries one).
    pub str_value: PgVec<'mcx, u8>,
    /// The byte offset of the token start within the scan buffer.
    pub location: i32,
}

/// Look-ahead state for [`BaseLexer::base_yylex`] (`base_yy_extra_type`'s
/// lookahead fields). Drives the core lexer (the `core_yylex` seam) over the
/// scan buffer, tracking the resume cursor and the one-token lookahead, and
/// applies the token-merging filter.
pub struct BaseLexer<'mcx> {
    mcx: Mcx<'mcx>,
    /// The scan buffer (`yyextra->scanbuf`): the query bytes.
    scanbuf: &'mcx [u8],
    /// The byte cursor `core_yylex` resumes scanning at.
    pos: i32,
    /// `have_lookahead` + `lookahead_token`/`lookahead_yylval`/
    /// `lookahead_yylloc`: the one-token lookahead, seeded with the mode token
    /// for non-default `RawParseMode`s.
    lookahead: Option<CoreToken<'mcx>>,
}

impl<'mcx> BaseLexer<'mcx> {
    /// Create a lexer over `scanbuf` with the given lookahead seed (the mode
    /// token for non-default `RawParseMode`s, else `None`).
    pub fn new(mcx: Mcx<'mcx>, scanbuf: &'mcx [u8], seed: Option<CoreToken<'mcx>>) -> Self {
        BaseLexer {
            mcx,
            scanbuf,
            pos: 0,
            lookahead: seed,
        }
    }

    /// Borrow the scan buffer (`yyextra->scanbuf`).
    pub fn scanbuf(&self) -> &[u8] {
        self.scanbuf
    }

    /// Convert a byte `location` to the 1-based character cursor used in
    /// user-facing error messages, via [`scanner_errposition`] over this
    /// lexer's scan buffer.
    fn errpos(&self, location: i32) -> i32 {
        scanner_errposition(location, self.scanbuf)
    }

    /// Run `core_yylex`, resuming at the current cursor and advancing it past
    /// the returned token.
    fn core_yylex(&mut self) -> PgResult<CoreToken<'mcx>> {
        let tok = scan::core_yylex::call(self.mcx, self.scanbuf, self.pos)?;
        self.pos = tok.end_pos;
        Ok(tok)
    }

    /// `base_yylex()` (parser.c:110) — return the next (possibly merged) token.
    ///
    /// Returns the end-of-input token (`YY_NULL`) when the stream is exhausted.
    pub fn base_yylex(&mut self) -> PgResult<Token<'mcx>> {
        // C:119-130 Get next token --- we might already have it (lookahead/seed).
        let cur_core = match self.lookahead.take() {
            Some(tok) => tok,
            None => self.core_yylex()?,
        };
        let mut cur_token = cur_core.token;
        let cur_location = cur_core.location;
        let cur_str = cur_core.str_value;

        // C:138-161 If this token isn't one that requires lookahead, return it.
        let needs_lookahead = cur_token == tokens::FORMAT
            || cur_token == tokens::NOT
            || cur_token == tokens::NULLS_P
            || cur_token == tokens::WITH
            || cur_token == tokens::WITHOUT
            || cur_token == tokens::UIDENT
            || cur_token == tokens::USCONST;
        if !needs_lookahead {
            return Ok(Token {
                token: cur_token,
                str_value: cur_str,
                location: cur_location,
            });
        }

        // C:181-184 Get next token, saving outputs into the lookahead variables.
        let next = self.core_yylex()?;
        let next_token = next.token;
        self.lookahead = Some(next);

        // C:194-321 Replace cur_token if needed, based on lookahead.
        if cur_token == tokens::FORMAT {
            // C:197-205 Replace FORMAT by FORMAT_LA if followed by JSON.
            if next_token == tokens::JSON {
                cur_token = tokens::FORMAT_LA;
            }
        } else if cur_token == tokens::NOT {
            // C:207-219 Replace NOT by NOT_LA if followed by BETWEEN/IN/etc.
            if next_token == tokens::BETWEEN
                || next_token == tokens::IN_P
                || next_token == tokens::LIKE
                || next_token == tokens::ILIKE
                || next_token == tokens::SIMILAR
            {
                cur_token = tokens::NOT_LA;
            }
        } else if cur_token == tokens::NULLS_P {
            // C:221-230 Replace NULLS_P by NULLS_LA if followed by FIRST/LAST.
            if next_token == tokens::FIRST_P || next_token == tokens::LAST_P {
                cur_token = tokens::NULLS_LA;
            }
        } else if cur_token == tokens::WITH {
            // C:232-241 Replace WITH by WITH_LA if followed by TIME/ORDINALITY.
            if next_token == tokens::TIME || next_token == tokens::ORDINALITY {
                cur_token = tokens::WITH_LA;
            }
        } else if cur_token == tokens::WITHOUT {
            // C:243-251 Replace WITHOUT by WITHOUT_LA if followed by TIME.
            if next_token == tokens::TIME {
                cur_token = tokens::WITHOUT_LA;
            }
        } else if cur_token == tokens::UIDENT || cur_token == tokens::USCONST {
            // C:253-320 the Unicode-escape lookahead branch.
            return self.finish_uident_usconst(cur_token, cur_str, cur_location);
        }

        Ok(Token {
            token: cur_token,
            str_value: cur_str,
            location: cur_location,
        })
    }

    /// The `UIDENT`/`USCONST` lookahead branch of `base_yylex` (parser.c:253):
    /// look ahead for `UESCAPE 'c'`, apply the Unicode de-escaping, and convert
    /// the token to `IDENT`/`SCONST`.
    fn finish_uident_usconst(
        &mut self,
        cur_token: i32,
        cur_str: PgVec<'mcx, u8>,
        location: i32,
    ) -> PgResult<Token<'mcx>> {
        // The lookahead currently holds the token following UIDENT/USCONST.
        let next = self
            .lookahead
            .take()
            .expect("base_yylex: lookahead set before finish_uident_usconst");

        let mut escape = b'\\';
        if next.token == tokens::UESCAPE {
            // C:256-279 Yup, so get the third token, which had better be SCONST.
            let third = self.core_yylex()?;

            // C:272-274 If we throw here, the error points at the third token.
            if third.token != tokens::SCONST {
                return Err(udeescape_syntax_error(
                    "UESCAPE must be followed by a simple string literal",
                    self.errpos(third.location),
                ));
            }
            let escstr = &third.str_value;
            // C:277-279 likewise points at the third (UESCAPE string) token.
            if escstr.len() != 1 || !check_uescapechar(escstr[0]) {
                return Err(udeescape_syntax_error(
                    "invalid Unicode escape character",
                    self.errpos(third.location),
                ));
            }
            escape = escstr[0];

            // C:291-296 We don't revert the UESCAPE un-truncation; we clear
            // have_lookahead, consuming all three tokens. (Already taken above.)
        } else {
            // C:298-306 No UESCAPE: convert using the default escape character
            // and keep the lookahead token for the next call.
            self.lookahead = Some(next);
        }

        // C:284-289 Apply Unicode conversion. str_udeescape reports a raw byte
        // offset (`in - str + position + 3`); C runs every such error through
        // scanner_errposition (the active errposition callback). Errors point to
        // the first (UIDENT/USCONST) token's location.
        let deescaped =
            str_udeescape(self.mcx, &cur_str, escape, location).map_err(|e| {
                let byte_pos = e.cursor_position().unwrap_or(0);
                e.with_cursor_position(self.errpos(byte_pos))
            })?;

        if cur_token == tokens::UIDENT {
            // C:308-314 An identifier: truncate as appropriate, then it's IDENT.
            let truncated = scansup::truncate_identifier::call(self.mcx, &deescaped, true)?;
            Ok(Token {
                token: tokens::IDENT,
                str_value: truncated,
                location,
            })
        } else {
            // C:316-319 USCONST -> SCONST.
            Ok(Token {
                token: tokens::SCONST,
                str_value: deescaped,
                location,
            })
        }
    }
}

// ===========================================================================
// raw_parser.
// ===========================================================================

/// `raw_parser()` (parser.c:41) — given a query in string form, do lexical and
/// grammatical analysis, returning the list of raw (un-analyzed) parse trees.
/// The contents of the list have the form required by `mode`.
///
/// In C this initializes the flex scanner, seeds `base_yylex`'s lookahead with
/// the `RawParseMode` mode token, runs `base_yyparse`, and on success returns
/// `yyextra.parsetree` (the `if (yyresult) return NIL` line is unreachable for
/// ordinary syntax errors, which `longjmp` past `raw_parser`). The grammar
/// (`base_yyparse`) drives the `base_yylex` filtered token stream; because that
/// drive is reentrant into this crate's `base_yylex`, it is reached across the
/// cycle through the grammar's seam, which owns the full setup/teardown.
pub fn raw_parser<'mcx>(
    mcx: Mcx<'mcx>,
    str_: &'mcx str,
    mode: RawParseMode,
) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>> {
    gram::base_yyparse::call(mcx, str_, mode)
}

/// The `mode_token[]` array (parser.c:58) — the initial lookahead token for a
/// non-default `RawParseMode`, or `None` for `RAW_PARSE_DEFAULT`.
pub fn mode_token(mode: RawParseMode) -> Option<i32> {
    match mode {
        RawParseMode::RAW_PARSE_DEFAULT => None,
        RawParseMode::RAW_PARSE_TYPE_NAME => Some(tokens::MODE_TYPE_NAME),
        RawParseMode::RAW_PARSE_PLPGSQL_EXPR => Some(tokens::MODE_PLPGSQL_EXPR),
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN1 => Some(tokens::MODE_PLPGSQL_ASSIGN1),
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN2 => Some(tokens::MODE_PLPGSQL_ASSIGN2),
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN3 => Some(tokens::MODE_PLPGSQL_ASSIGN3),
    }
}

/// Build the seed `CoreToken` for a non-default `RawParseMode` (the mode token,
/// at location 0), or `None` for `RAW_PARSE_DEFAULT`.
pub fn mode_seed(mcx: Mcx<'_>, mode: RawParseMode) -> Option<CoreToken<'_>> {
    mode_token(mode).map(|tok| CoreToken {
        token: tok,
        str_value: PgVec::new_in(mcx),
        location: 0,
        end_pos: 0,
    })
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// `raw_parser(str, RAW_PARSE_TYPE_NAME)` + `linitial_node(TypeName, ...)`
/// (parse_type.c `typeStringToTypeName`'s inner drive): parse a type-name string
/// and return the single `TypeName` node it produces.
///
/// The seam contract is `String -> PgResult<types_parsenodes::TypeName>` (owned,
/// no arena lifetime). The grammar drive needs an arena, so a private
/// `MemoryContext` is created for the parse; the decoded arena
/// `types_nodes::rawnodes::TypeName<'mcx>` is bridged into the owned
/// `types_parsenodes::TypeName` before the context drops (the owned node carries
/// no `'mcx`, so it outlives the arena soundly). A grammar/syntax error is
/// raised inside `raw_parser` (with the parser's error position) and propagates
/// on `Err`; this never returns on a malformed string.
fn raw_parse_type_name(
    str_: alloc::string::String,
) -> PgResult<types_parsenodes::TypeName> {
    let ctx = mcx::MemoryContext::new("raw_parse_type_name");
    let mcx = ctx.mcx();

    // C: `raw_parser(str, RAW_PARSE_TYPE_NAME)`.
    let list = raw_parser(mcx, str_.as_str(), RawParseMode::RAW_PARSE_TYPE_NAME)?;

    // C: `Assert(list_length(raw_parsetree_list) == 1)` then
    // `linitial_node(TypeName, raw_parsetree_list)`. The grammar wraps the sole
    // `TypeName` in a `RawStmt`; pull it back out.
    let first = list
        .first()
        .expect("raw_parse_type_name: empty parse-tree list");
    let tn = (*first.stmt).expect_typename();

    raw_typename_to_parse(tn)
}

/// Bridge the arena raw-grammar `types_nodes::rawnodes::TypeName<'mcx>` into the
/// owned resolver-facing `types_parsenodes::TypeName`. Mirrors parse_type.c's
/// `raw_typename_to_parse`: the qualified `names` are `String` nodes; `typmods`
/// carry the simple `A_Const`/identifier values through (else `A_Star`, so the
/// resolver raises the C "must be simple constants or identifiers" error);
/// `arrayBounds` carry the integer bounds through.
fn raw_typename_to_parse(
    tn: &types_nodes::rawnodes::TypeName<'_>,
) -> PgResult<types_parsenodes::TypeName> {
    use alloc::string::ToString;
    use types_nodes::nodes::{ntag, Node as RawNode};

    let mut names: alloc::vec::Vec<types_parsenodes::Node> =
        alloc::vec::Vec::with_capacity(tn.names.len());
    for n in tn.names.iter() {
        match (**n).as_string() {
            Some(s) => names.push(types_parsenodes::Node::String(
                types_parsenodes::StringNode { sval: Some(s.sval.as_str().to_string()) },
            )),
            None => {
                return Err(types_error::PgError::error(alloc::format!(
                    "raw_parse_type_name: TypeName.names element is not a String node (tag {})",
                    (**n).node_tag().0
                )));
            }
        }
    }

    let mut typmods: alloc::vec::Vec<types_parsenodes::Node> =
        alloc::vec::Vec::with_capacity(tn.typmods.len());
    for tm in tn.typmods.iter() {
        let tm_node = &**tm;
        let bridged: types_parsenodes::Node = if let Some(ac) = tm_node.as_a_const() {
            match ac.val.as_deref().map(|v| (v.node_tag(), v)) {
                Some((ntag::T_Integer, v)) => {
                    let i = v.expect_integer();
                    types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: i.ival })
                }
                Some((ntag::T_Float, v)) => {
                    let fl = v.expect_float();
                    types_parsenodes::Node::Float(types_parsenodes::Float {
                        fval: Some(fl.fval.as_str().to_string()),
                    })
                }
                Some((ntag::T_String, v)) => {
                    let s = v.expect_string();
                    types_parsenodes::Node::String(types_parsenodes::StringNode {
                        sval: Some(s.sval.as_str().to_string()),
                    })
                }
                Some((ntag::T_Boolean, v)) => {
                    let b = v.expect_boolean();
                    types_parsenodes::Node::Boolean(types_parsenodes::Boolean { boolval: b.boolval })
                }
                Some((ntag::T_BitString, v)) => {
                    let b = v.expect_bitstring();
                    types_parsenodes::Node::BitString(types_parsenodes::BitString {
                        bsval: Some(b.bsval.as_str().to_string()),
                    })
                }
                _ => types_parsenodes::Node::A_Star,
            }
        } else if let Some(cr) = tm_node.as_columnref() {
            if cr.fields.len() == 1 {
                if let Some(s) = (*cr.fields[0]).as_string() {
                    types_parsenodes::Node::String(types_parsenodes::StringNode {
                        sval: Some(s.sval.as_str().to_string()),
                    })
                } else {
                    types_parsenodes::Node::A_Star
                }
            } else {
                types_parsenodes::Node::A_Star
            }
        } else {
            types_parsenodes::Node::A_Star
        };
        typmods.push(bridged);
    }

    let mut array_bounds: alloc::vec::Vec<types_parsenodes::Node> =
        alloc::vec::Vec::with_capacity(tn.arrayBounds.len());
    for n in tn.arrayBounds.iter() {
        match (**n).as_integer() {
            Some(i) => array_bounds
                .push(types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: i.ival })),
            None => array_bounds
                .push(types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: -1 })),
        }
    }

    Ok(types_parsenodes::TypeName {
        names,
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typmods,
        typemod: tn.typemod,
        arrayBounds: array_bounds,
        location: tn.location,
    })
}

/// Install this crate's owned inward seams. `raw_parse_type_name` (the
/// `parse_type.c` inner drive) is installed here: the driver owns `raw_parser`
/// and bridges its arena `TypeName` into the owned carrier the resolver reads.
pub fn init_seams() {
    backend_parser_driver_seams::raw_parse_type_name::set(raw_parse_type_name);
}

/// Error helper for the `base_yylex` UESCAPE checks (parser.c:273/278): a
/// syntax error whose cursor is the already-converted 1-based character cursor.
fn udeescape_syntax_error(message: &str, char_position: i32) -> types_error::PgError {
    types_error::PgError::error(message)
        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
        .with_cursor_position(char_position)
}
