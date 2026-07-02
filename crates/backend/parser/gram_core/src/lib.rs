mod actions;
mod parse;
mod stack;
mod tables;
mod yystype;

use mcx::Mcx;
use parser_seams::RawParseMode;
use scan_fgram::tokens;
use types_error::PgResult;
use types_nodes::NodeList;

// raw_parser (parser.c): scanner over an arena copy of the query (C's
// scanner_init palloc+memcpy), optional RawParseMode seed token, base_yyparse.
// Returns gram.y's parsetree: a list of RawStmt nodes.
pub fn raw_parser<'mcx>(
    mcx: Mcx<'mcx>,
    query: &str,
    mode: RawParseMode,
) -> PgResult<NodeList<'mcx>> {
    let scanbuf = mcx::slice_borrow_in(mcx, query.as_bytes())?;
    let mode_token = match mode {
        RawParseMode::RAW_PARSE_DEFAULT => 0,
        RawParseMode::RAW_PARSE_TYPE_NAME => tokens::MODE_TYPE_NAME,
        RawParseMode::RAW_PARSE_PLPGSQL_EXPR => tokens::MODE_PLPGSQL_EXPR,
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN1 => tokens::MODE_PLPGSQL_ASSIGN1,
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN2 => tokens::MODE_PLPGSQL_ASSIGN2,
        RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN3 => tokens::MODE_PLPGSQL_ASSIGN3,
    };
    let mut parser = parse::Parser::new(mcx, scanbuf, mode_token)?;
    parser.yyparse()?;
    Ok(parser.parsetree)
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_dump;
