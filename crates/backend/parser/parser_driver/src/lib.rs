#![allow(non_snake_case)]

extern crate alloc;

use mcx::{Mcx, PgVec};
use parser_seams::RawParseMode;
use types_error::PgResult;
use types_nodes::rawnodes::RawStmt;

mod udeescape;
pub use udeescape::{check_uescapechar, str_udeescape, UdeescapeError};

pub fn raw_parser<'mcx>(
    _mcx: Mcx<'mcx>,
    _query_string: &str,
    _mode: RawParseMode,
) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>> {
    panic!(
        "raw_parser (parser.c): the core scanner (backend-parser-scan, crate scan_fgram, \
         in flight) and the bison grammar (backend-parser-gram, c2rust gram unit) are not \
         available; base_yylex's merge filter and the mode-token seed land with them"
    )
}

pub fn base_yylex() -> ! {
    panic!(
        "base_yylex (parser.c): the lookahead merge filter needs the scanner's token \
         stream and gram.y token codes (backend-parser-scan in flight, gram unported)"
    )
}

pub fn init_seams() {
    parser_seams::raw_parser::set(raw_parser);
}

#[cfg(test)]
mod tests;
