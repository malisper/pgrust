//! Seam declarations for the `backend-replication-repl-scanner` unit
//! (`replication/repl_scanner.l`).
//!
//! The replication command parser (`repl_gram.y`, the
//! `backend-replication-repl-gram` crate) drives the flex scanner via
//! `replication_yylex()` and the WalSender gate uses
//! `replication_scanner_is_replication_command()`. The scanner is a separate
//! unit; the grammar reaches it through these seams. The owning scanner unit
//! installs them from its `init_seams()` when it lands; until then a call
//! panics loudly.

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use replication::repl_token::Token;

seam_core::seam!(
    /// Lex an entire replication-command string into its token stream
    /// (`repl_scanner.l`'s `replication_yylex()` driven to `<<EOF>>`). The
    /// returned vector holds the tokens in order, terminated by a single
    /// [`Token::Eof`]. `Err` carries the scanner's `ereport(ERROR,
    /// ERRCODE_SYNTAX_ERROR)` (unterminated quoted string, invalid streaming
    /// start location, ...) as a recoverable [`PgError`].
    ///
    /// Bundling the whole stream is behavior-preserving for this grammar: the
    /// LALR(1) parser only ever consumes tokens left-to-right with one token of
    /// lookahead, and the scanner keeps no state the grammar can observe other
    /// than the token sequence and its first error.
    pub fn replication_lex_all(input: &str) -> PgResult<Vec<Token>>
);

seam_core::seam!(
    /// `replication_scanner_is_replication_command(yyscanner)`
    /// (`repl_scanner.l`): lex only the first token of `input` and report
    /// whether it is one of the WalSender command-introducing keywords. `Err`
    /// carries a scanner lexical error, mirroring the C path where the first
    /// `replication_yylex` can `ereport(ERROR)`.
    pub fn replication_scanner_is_replication_command(input: &str) -> PgResult<bool>
);
