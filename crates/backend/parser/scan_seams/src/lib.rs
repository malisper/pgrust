//! Seam declarations for the core lexer (`backend/parser/scan.l`, `core_yylex`).
//!
//! `scan.l` (the flex scanner) is not yet ported. `base_yylex` in the
//! `backend-parser-driver` unit is the filter layer that sits between the
//! grammar and this core lexer; it calls `core_yylex` here to pull each raw
//! token. The owning unit installs this seam from its `init_seams()` when it
//! lands; until then a call panics loudly (mirror-PG-and-panic).
//!
//! The C scanner is stateful: it mutates a NUL-padded buffer in place and
//! tracks an internal resume position plus the `lookahead_end`/
//! `lookahead_hold_char` `\0` un-truncation trick used to point error cursors
//! at the right token. That mutable-buffer/longjmp coupling does not cross a
//! seam, so the seam is modelled *statelessly*: the caller passes the scan
//! buffer and the byte position to resume at, and the lexer returns the token,
//! its semantic string value (when it carries one), its start location, and the
//! byte position to resume the next call at. The merged token stream and error
//! locations come out identical to the in-place C scanner.

extern crate alloc;

use mcx::{Mcx, PgVec};
use ::types_error::PgResult;

/// One token produced by `core_yylex` (the `core_YYSTYPE`/`YYLTYPE` triple plus
/// the resume cursor).
///
/// `str_value` carries the token's semantic string value (C's
/// `core_yystype.str`) for the tokens that have one (`IDENT`/`UIDENT`/`SCONST`/
/// `USCONST`/...); it is empty for tokens with no string payload. `location` is
/// the byte offset of the token start within the scan buffer (C's `*llocp`).
/// `end_pos` is the byte offset to resume scanning at on the next call (the
/// stateless replacement for flex's internal buffer position).
#[derive(Clone, Debug)]
pub struct CoreToken<'mcx> {
    /// The grammar token code (C `core_yylex` return value).
    pub token: i32,
    /// The token's semantic string value, or empty when it has none.
    pub str_value: PgVec<'mcx, u8>,
    /// The byte offset of the token start within the scan buffer.
    pub location: i32,
    /// The byte offset to resume scanning at on the next call.
    pub end_pos: i32,
}

seam_core::seam!(
    /// `core_yylex(lvalp, llocp, yyscanner)` (`backend/parser/scan.l`) — return
    /// the next raw token from the scan buffer, resuming at byte offset `pos`.
    ///
    /// `scanbuf` is the full query buffer (C `yyextra->scanbuf`, NUL-padded);
    /// `pos` is the byte offset to resume at (`0` on the first call). On
    /// end-of-input the scanner returns the `YY_NULL` (0) token. A lexer error
    /// (`scanner_yyerror`/`ereport(ERROR)`) is carried on `Err`. Any allocated
    /// token string value lives in `mcx` (C pallocs in the parse context).
    pub fn core_yylex<'mcx>(
        mcx: Mcx<'mcx>,
        scanbuf: &'mcx [u8],
        pos: i32,
    ) -> PgResult<CoreToken<'mcx>>
);

// ===========================================================================
// PL/pgSQL body-scan WARNING arming.
// ===========================================================================

use core::cell::Cell;

std::thread_local! {
    static PLPGSQL_BODY_WARNINGS_ARMED: Cell<bool> = const { Cell::new(false) };
}

/// Whether the core lexer should replay its deferred string-literal WARNINGs
/// (`check_string_escape_warning`/`check_escape_warning`, "nonstandard use of
/// \\…").
///
/// scan.l emits these inline on *every* scan of the affected literal. C scans a
/// PL/pgSQL function body once — at the `forValidator` compile (CREATE FUNCTION)
/// — and caches the compiled function, so the WARNING is observed exactly once.
/// This codebase has no PL/pgSQL function cache yet (the `funccache`/`fn_extra`
/// bridge is unwired), so a PL/pgSQL function is re-parsed on *every* execution;
/// replaying the WARNING unconditionally would re-emit it on every call. The
/// `core_yylex` seam therefore replays the scanner WARNINGs only while this flag
/// is armed, and `parse_function_body` arms it only for the validator compile —
/// reproducing C's observable "once, at CREATE FUNCTION" behavior. (The regular
/// SQL parse path uses the C-FFI `base_yyparse` and emits these WARNINGs
/// natively, so it never consults this flag.)
pub fn plpgsql_body_warnings_armed() -> bool {
    PLPGSQL_BODY_WARNINGS_ARMED.with(Cell::get)
}

/// Set the arm flag, returning the previous value (so callers can restore it for
/// safe nesting).
pub fn set_plpgsql_body_warnings_armed(value: bool) -> bool {
    PLPGSQL_BODY_WARNINGS_ARMED.with(|c| c.replace(value))
}
