//! Seam declarations for the bison grammar (`backend/parser/gram.y`,
//! `base_yyparse` and the `scanner_init`/`parser_init`/`scanner_finish` setup
//! it drives).
//!
//! `gram.y` is not yet ported. `raw_parser` in the `backend-parser-driver`
//! unit sets up the flex scanner, seeds the lookahead with the `RawParseMode`
//! mode token, runs `base_yyparse`, and returns the resulting `List` of raw
//! parse trees. The grammar reentrantly pulls each token through `base_yylex`
//! (owned by the driver unit) — so the grammar depends on the driver, and the
//! driver reaches the grammar across that cycle through this seam. The owning
//! unit installs it from its `init_seams()` when it lands; until then a call
//! panics loudly (mirror-PG-and-panic).

extern crate alloc;

use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::nodes::parsestmt::RawStmt;
use ::parsenodes::RawParseMode;

seam_core::seam!(
    /// The `raw_parser` drive owned by the grammar/scanner: `scanner_init` +
    /// `parser_init` + `base_yyparse` + `scanner_finish` (parser.c:42-86).
    ///
    /// `str_` is the query string and `mode` the `RawParseMode`. Returns the
    /// `List` of raw (un-analyzed) parse trees (`yyextra.parsetree`), the form
    /// of whose contents is determined by `mode`. A grammar/syntax error
    /// (`scanner_yyerror`/`ereport(ERROR)`) is carried on `Err`; in C this
    /// `longjmp`s past `raw_parser` to the error handler (the `if (yyresult)
    /// return NIL` line is unreachable for ordinary syntax errors). The trees
    /// are allocated in `mcx`.
    pub fn base_yyparse<'mcx>(
        mcx: Mcx<'mcx>,
        str_: &'mcx str,
        mode: RawParseMode,
    ) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>>
);
