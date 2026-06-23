//! Seam declarations for the `backend-replication-syncrep-scanner` unit
//! (`replication/syncrep_scanner.l`), the flex lexer for the
//! `synchronous_standby_names` GUC value.
//!
//! The grammar unit (`syncrep_gram.y`, crate
//! `backend-replication-syncrep-gram`) drives the scanner: `syncrep_yyparse`
//! repeatedly calls `syncrep_yylex` and, on a parse failure, `syncrep_yyerror`.
//! The scanner in turn references the grammar's token codes (emitted into
//! `syncrep_gram.h`), so the two translation units form a dependency cycle.
//! These seams are the grammar -> scanner edge of that cycle. The owning unit
//! installs them from its `init_seams()` when it lands; until then a call
//! panics loudly.

use mcx::{Mcx, PgString};
use ::types_error::PgResult;

/// Opaque token standing in for C's reentrant `yyscan_t` (a `void *`) while the
/// scanner runtime (`syncrep_scanner.l`) owns the live scanner state — the
/// input buffer, scan position, the most recent `yytext`, the doubled-quote
/// accumulation buffer (`yyextra->xdbuf`), and the first recorded parse-error
/// message. Valid from `syncrep_scanner_init` until `syncrep_scanner_finish`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncrepScannerHandle(pub u64);

/// One token returned by `syncrep_yylex`: the Bison token code (the values from
/// `syncrep_gram.h`, or a single-character token's ASCII byte value, or `0` for
/// end of input) together with the semantic value the C scanner stores in
/// `yylval->str` for `NAME`/`NUM` tokens (`pstrdup`'d into the caller's memory
/// context). Tokens that carry no semantic value leave `value` as `None`.
#[derive(Debug)]
pub struct SyncrepLexeme<'mcx> {
    pub token: i32,
    pub value: Option<PgString<'mcx>>,
}

seam_core::seam!(
    /// `syncrep_scanner_init(str, &yyscanner)` (`syncrep_scanner.l`): allocate
    /// and initialize a reentrant scanner over a copy of `input`. `Err` carries
    /// the `palloc` OOM ereport surface.
    pub fn syncrep_scanner_init<'mcx>(
        mcx: Mcx<'mcx>,
        input: &str,
    ) -> PgResult<SyncrepScannerHandle>
);

seam_core::seam!(
    /// `syncrep_yylex(&yylval, &error_msg, yyscanner)` (`syncrep_scanner.l`):
    /// return the next token. The `NAME`/`NUM` semantic string is `pstrdup`'d
    /// into `mcx` (the C parse `CurrentMemoryContext`). `Err` carries the
    /// `palloc` OOM ereport surface. An unterminated quoted identifier records
    /// the scanner's first error message (via the internal `syncrep_yyerror`)
    /// and yields a `JUNK` token, exactly as the C `<xd><<EOF>>` rule does.
    pub fn syncrep_yylex<'mcx>(
        mcx: Mcx<'mcx>,
        scanner: SyncrepScannerHandle,
    ) -> PgResult<SyncrepLexeme<'mcx>>
);

seam_core::seam!(
    /// `syncrep_yyerror(result_p, error_msg_p, yyscanner, message)`
    /// (`syncrep_scanner.l`): record `message` against the scanner, qualified
    /// with the current `yytext` (`"... at or near \"<yytext>\""`) or, when
    /// `yytext` is empty, `"... at end of input"`. Only the first error of a
    /// parse is kept (the C `if (*syncrep_parse_error_msg_p) return;` guard).
    pub fn syncrep_yyerror(scanner: SyncrepScannerHandle, message: &str)
);

seam_core::seam!(
    /// Read back the scanner's first recorded parse-error message (the C
    /// `*syncrep_parse_error_msg_p`), copied into `mcx`, or `None` if no error
    /// was recorded. `Err` carries the copy's `palloc` OOM surface.
    pub fn syncrep_scanner_error_msg<'mcx>(
        mcx: Mcx<'mcx>,
        scanner: SyncrepScannerHandle,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `syncrep_scanner_finish(yyscanner)` (`syncrep_scanner.l`): tear down the
    /// reentrant scanner and free its state.
    pub fn syncrep_scanner_finish(scanner: SyncrepScannerHandle)
);
