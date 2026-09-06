#![allow(non_snake_case)]

extern crate alloc;

use mcx::{Mcx, PgVec};
use parser_seams::RawParseMode;
use types_error::{PgError, PgResult};
use types_nodes::rawnodes::RawStmt;

pub use parser_small1::udeescape::{
    check_uescapechar, str_udeescape, UdeescapeError, UdeescapeFailure,
};

// raw_parser (parser.c:42) over the RawStmt-typed seam: gram.y's parse_toplevel
// yields RawStmt for RAW_PARSE_DEFAULT and the PL/pgSQL modes; RAW_PARSE_TYPE_NAME
// yields a bare TypeName list, which this seam's shape cannot carry — that mode's
// consumer (parse_type.c typeStringToTypeName) reads gram_core::raw_parser
// directly. Any other node shape here is the C `elog(ERROR, "unexpected node
// type")` idiom: a catchable XX000, never a process panic.
pub fn raw_parser<'mcx>(
    mcx: Mcx<'mcx>,
    query_string: &str,
    mode: RawParseMode,
) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>> {
    let list = gram_core::raw_parser(mcx, query_string, mode)?;
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(list.len()).map_err(|_| mcx.oom(list.len()))?;
    for n in list.iter() {
        let Some(rs) = n.as_raw_stmt() else {
            return Err(Box::new(PgError::error(format!(
                "unexpected node type: {:?}",
                n.node_tag()
            ))));
        };
        v.push(RawStmt { stmt: rs.stmt, stmt_location: rs.stmt_location, stmt_len: rs.stmt_len });
    }
    Ok(v)
}

pub fn init_seams() {
    parser_seams::raw_parser::set(raw_parser);
}

#[cfg(test)]
mod tests;
