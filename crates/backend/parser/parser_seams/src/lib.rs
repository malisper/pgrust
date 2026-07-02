#![allow(non_camel_case_types)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::rawnodes::RawStmt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum RawParseMode {
    #[default]
    RAW_PARSE_DEFAULT = 0,
    RAW_PARSE_TYPE_NAME,
    RAW_PARSE_PLPGSQL_EXPR,
    RAW_PARSE_PLPGSQL_ASSIGN1,
    RAW_PARSE_PLPGSQL_ASSIGN2,
    RAW_PARSE_PLPGSQL_ASSIGN3,
}

seam_core::seam!(
    // raw_parser (parser/parser.c); RawStmt list is arena-owned.
    pub fn raw_parser<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        query_string: &'a str,
        mode: RawParseMode,
    ) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>>
);
