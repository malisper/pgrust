//! `base_yyparse` / `core_yylex` owner: the bison grammar (`gram.y`) and flex
//! scanner (`scan.l`) for PostgreSQL 18.3.
//!
//! ## Audited-unsafe mechanism (contained), safe owned output
//!
//! Re-deriving bison's ~4800-rule LALR grammar by hand is infeasible, so — like
//! the repo's other generated-machinery crates (`dynahash`, `dshash`, the
//! raw-pointer `lib-*` containers) — this crate is built on a *contained,
//! audited-unsafe* mechanism: the c2rust translation of `gram.c` (the LALR
//! tables + rule actions, in `pgrust-gram-c2rust-fgram`) plus the flex scanner
//! (`backend-parser-scan-fgram`). That mechanism builds an internal, raw
//! `*mut Node` parse graph in C-faithful `#[repr(C)]` node structs
//! (`backend-nodes-types-fgram` + `pgrust-pg-ffi-fgram`). The unsafe lives
//! entirely inside those copied crates.
//!
//! THIS crate is the safe boundary: [`convert`] walks that raw graph once, at
//! the `base_yyparse` return, and rebuilds it as the repo's *owned*
//! `types_nodes` parse tree (`Mcx`/`PgBox`/`PgVec`/`PgString` — no raw
//! pointers). Everything this crate hands out is safe and owned.
//!
//! ## The C shim is gone
//!
//! src-idiomatic compiled a small C variadic shim (`csupport.c`) for
//! `psprintf`/`errmsg` and a `setjmp`/`longjmp` error escape. This port compiles
//! no C: the variadic calls were rewritten to Rust `%s`/`%d` formatting macros,
//! and the `ereport(ERROR)` escape is a Rust panic caught by `catch_unwind`
//! (the copied parser frames are plain Rust, so the unwind is sound). See
//! `pgrust-gram-c2rust-fgram`'s `support.rs`.
//!
//! ## F1 scope
//!
//! [`convert`] implements the conversion for the DML + expression core (the 59
//! `types_nodes` parse-node types that exist today): SELECT/INSERT/UPDATE/
//! DELETE/MERGE, the `A_*`/`ColumnRef`/`FuncCall`/`ResTarget`/`RangeVar`/value
//! nodes, and the grammar-produced `Expr` leaves. The ~148 absent DDL/utility
//! node types (`Create*`/`Alter*`/`Copy`/`Grant`/…) are F2/F3/F4: their
//! converter arms `panic!` loudly (mirror-PG-and-panic) behind `base_yyparse`
//! until those node types are authored in `types-nodes`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult};
use types_error::error::{make_sqlstate, SqlState};
use types_nodes::parsestmt::RawStmt;
use types_parsenodes::RawParseMode;

use backend_parser_gram_seams as gram_seam;
use backend_parser_scan_seams as scan_seam;
use backend_parser_scan_seams::CoreToken;

mod convert;

use pgrust_gram_c2rust as mech;
use pgrust_pg_ffi::List as RawList;

// ===========================================================================
// base_yyparse — run the c2rust mechanism, convert the raw graph to owned.
// ===========================================================================

/// `raw_parser` drive (`parser.c:42`): scanner_init + parser_init +
/// `base_yyparse` + scanner_finish, then convert the raw `List *` of `RawStmt`
/// into the repo's owned [`RawStmt`] vector.
///
/// The mechanism runs the whole parse (its own internal scanner + the LALR
/// tables) and yields a raw `*mut List` of `*mut RawStmt`. A grammar/lexer
/// error returns NIL with a recorded message/SQLSTATE/cursor, which we surface
/// as `Err` (the C `ereport(ERROR)` longjmp path). An empty NIL with no
/// recorded error is genuinely empty input (`PgVec` of length 0).
pub fn base_yyparse<'mcx>(
    mcx: Mcx<'mcx>,
    str_: &'mcx str,
    mode: RawParseMode,
) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>> {
    let mech_mode = raw_parse_mode_to_mech(mode);

    // SAFETY: the returned `*mut List` and the whole node graph it references
    // live in the mechanism's per-parse arena, which is leaked for the process
    // lifetime (mirroring PostgreSQL's parse memory context). We read it once
    // here and copy every reachable node into `mcx`; no raw pointer escapes.
    let raw: *mut RawList = mech::raw_parser_bytes(str_.as_bytes(), mech_mode);

    if raw.is_null() {
        // NIL: either a parse error (longjmp'd to NIL) or genuinely empty input.
        if let Some((msg, state, cursor)) = mech::last_error() {
            return Err(parse_error(&msg, state, cursor));
        }
        return Ok(PgVec::new_in(mcx));
    }

    // Convert each cell (a `*mut RawStmt`) into an owned RawStmt.
    let list: &RawList = unsafe { &*raw };
    let mut out: PgVec<'mcx, RawStmt<'mcx>> =
        mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let rs: *mut backend_nodes_types::parsenodes_stmts::RawStmt = cell.ptr();
        if rs.is_null() {
            continue;
        }
        out.push(convert::convert_raw_stmt(mcx, rs)?);
    }
    Ok(out)
}

/// Map the repo `RawParseMode` enum to the mechanism's int-width `RawParseMode`
/// (identical discriminants; `parser/parser.h`).
fn raw_parse_mode_to_mech(mode: RawParseMode) -> mech::RawParseMode {
    match mode {
        RawParseMode::RAW_PARSE_DEFAULT => pgrust_pg_ffi::spi::RAW_PARSE_DEFAULT,
        RawParseMode::RAW_PARSE_TYPE_NAME => pgrust_pg_ffi::spi::RAW_PARSE_TYPE_NAME,
        RawParseMode::RAW_PARSE_PLPGSQL_EXPR => pgrust_pg_ffi::spi::RAW_PARSE_PLPGSQL_EXPR,
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN1 => pgrust_pg_ffi::spi::RAW_PARSE_PLPGSQL_ASSIGN1,
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN2 => pgrust_pg_ffi::spi::RAW_PARSE_PLPGSQL_ASSIGN2,
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN3 => pgrust_pg_ffi::spi::RAW_PARSE_PLPGSQL_ASSIGN3,
    }
}

/// Build the `PgError` for a recorded grammar/lexer error: message, the 5-char
/// SQLSTATE, and (when set) the 1-based cursor position
/// (`scanner_yyerror`/`errposition`).
fn parse_error(msg: &str, state: [u8; 5], cursor: i32) -> PgError {
    let sqlstate: SqlState = make_sqlstate(state);
    let mut e = PgError::error(msg.to_string()).with_sqlstate(sqlstate);
    if cursor > 0 {
        e = e.with_cursor_position(cursor);
    }
    e
}

// ===========================================================================
// core_yylex — the stateless scanner seam.
// ===========================================================================

use backend_parser_scan_mech::{CoreYYSTYPE, Scanner, ScannerSettings};

/// `core_yylex(lvalp, llocp, yyscanner)` (`scan.l`) — the stateless scanner
/// seam: build a scanner over `scanbuf`, resume at byte `pos`, return one token
/// plus the resume cursor. Each call resumes at a token boundary, where the C
/// scanner is in `INITIAL`, so a fresh scanner seeked to `pos` reproduces the
/// in-place scanner's token stream and locations exactly.
fn core_yylex<'mcx>(
    mcx: Mcx<'mcx>,
    scanbuf: &'mcx [u8],
    pos: i32,
) -> PgResult<CoreToken<'mcx>> {
    let mut scanner = Scanner::new(scanbuf, ScannerSettings::live());
    scanner.seek(pos.max(0) as usize);
    let tok = scanner
        .core_yylex()
        .map_err(|e| scan_lex_error(&scanner, e))?;

    let mut str_value: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    match &tok.value {
        CoreYYSTYPE::Str(bytes) => {
            str_value.reserve(bytes.len());
            str_value.extend_from_slice(bytes);
        }
        CoreYYSTYPE::Keyword(kw) => {
            let b = kw.as_bytes();
            str_value.reserve(b.len());
            str_value.extend_from_slice(b);
        }
        // Ival / None carry no string payload (Ival rides the grammar's
        // `core_yystype.ival`, reconstructed grammar-side; the stateless seam
        // contract carries only the string value, matching the driver's use).
        CoreYYSTYPE::Ival(_) | CoreYYSTYPE::None => {}
    }

    Ok(CoreToken {
        token: tok.token,
        str_value,
        location: tok.location,
        end_pos: scanner.pos() as i32,
    })
}

/// Convert a mechanism `LexError` into the repo `PgError` (the scanner's
/// `ereport(ERROR)` path: SQLSTATE + message + the byte location of the failing
/// match, used as the error cursor). When the error simply propagates a
/// called-out routine's `ereport` (`e.source`), its dynamic message/SQLSTATE
/// are carried through; otherwise the static lexer message is used. The two
/// `SqlState` types share PostgreSQL's `MAKE_SQLSTATE` 6-bit encoding, so the
/// inner integer transfers directly.
fn scan_lex_error(scanner: &Scanner<'_>, e: backend_parser_scan_mech::LexError) -> PgError {
    let (message, mech_state) = match &e.source {
        Some(src) => (src.message().to_string(), src.sqlstate().0),
        None => (e.message.to_string(), e.sqlstate.0),
    };
    let cursor = e.location.max(scanner.yylloc()).max(0) + 1;
    PgError::error(message)
        .with_sqlstate(SqlState(mech_state))
        .with_cursor_position(cursor)
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install this crate's inward seams: `base_yyparse` (the grammar) and
/// `core_yylex` (the scanner). This crate is the C-source owner of `gram.y` and
/// `scan.l`, so it owns and installs both per-file seam crates.
pub fn init_seams() {
    gram_seam::base_yyparse::set(base_yyparse);
    scan_seam::core_yylex::set(core_yylex);
}

#[cfg(test)]
mod tests;
