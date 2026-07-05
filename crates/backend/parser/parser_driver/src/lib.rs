#![allow(non_snake_case)]

extern crate alloc;

use mcx::{Mcx, PgVec};
use parser_seams::RawParseMode;
use types_error::PgResult;
use types_nodes::rawnodes::RawStmt;

pub use parser_small1::udeescape::{check_uescapechar, str_udeescape, UdeescapeError};

pub fn raw_parser<'mcx>(
    mcx: Mcx<'mcx>,
    query_string: &str,
    mode: RawParseMode,
) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>> {
    let list = gram_core::raw_parser(mcx, query_string, mode)?;
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(list.len()).map_err(|_| mcx.oom(list.len()))?;
    for n in list.iter() {
        let rs = n.as_raw_stmt().expect("raw_parser yields RawStmt");
        v.push(RawStmt { stmt: rs.stmt, stmt_location: rs.stmt_location, stmt_len: rs.stmt_len });
    }
    Ok(v)
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
