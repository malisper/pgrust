//! Idiomatic port of `src/backend/tsearch/wparser.c` — the standard interface
//! to the word parser.
//!
//! The SQL-facing entry points (`ts_token_type(*)`, `ts_parse(*)`, and the
//! `ts_headline[_json[b]](*)` family) are set-returning functions and JSON
//! transforms that lean on genuinely-external machinery (the SRF
//! `FuncCallContext` protocol, `BuildTupleFromCStrings`, the parser cache and
//! `OidFunctionCall1` of the parser's `lextype`, `get_ts_parser_oid`, and the
//! JSON iteration helpers).  Those wrappers belong to a future SQL-binding
//! pass.
//!
//! What is fully in-crate here is the data-producing core of each SRF: the
//! per-parser token-type list ([`tt_storage_list`]) and the tokenization loop
//! used by `prs_setup_firstcall` ([`prs_tokenize`]).  Both run against the
//! default word parser implemented in [`crate::wparser_def`], so they reproduce
//! the exact `(lexid, alias, descr)` / `(type, lexeme)` rows the SQL functions
//! emit.

use utils_error::PgResult;

use crate::wparser_def::{self, TParser};

/// `TSTokenTypeStorage.list`: one row for `ts_token_type`, i.e. the parser's
/// `(lexid, alias, descr)` token-type descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenTypeRow {
    pub lexid: i32,
    pub alias: String,
    pub descr: String,
}

/// The `LexDescr` list backing `ts_token_type(*)` for the default parser.
///
/// In C this is `OidFunctionCall1(prs->lextypeOid, 0)`; for the default
/// parser that is `prsd_lextype`.  `tt_process_call` stops at the first row
/// whose `lexid` is 0, so the trailing sentinel is dropped here.
pub fn tt_storage_list() -> Vec<TokenTypeRow> {
    wparser_def::prsd_lextype()
        .into_iter()
        .take_while(|(lexid, _, _)| *lexid != 0)
        .map(|(lexid, alias, descr)| TokenTypeRow {
            lexid,
            alias,
            descr,
        })
        .collect()
}

/// One row for `ts_parse`: `(type, lexeme)` (the C `LexemeEntry`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LexemeEntry {
    pub type_: i32,
    pub lexeme: Vec<u8>,
}

/// `prs_setup_firstcall`'s core for the default parser: run `prsstart` /
/// `prstoken*` / `prsend` over `txt` and collect every `(type, lexeme)` token
/// (the loop runs until `prstoken` returns type 0).
///
/// Fallible: propagates the soft encoding error the default parser raises on a
/// truncated/invalid multibyte sequence (`pg_mblen_range`).  The `TParser` is
/// freed on every exit path (including the error return).
pub fn prs_tokenize(txt: &[u8]) -> PgResult<Vec<LexemeEntry>> {
    let mut prsdata: TParser = wparser_def::prsd_start(txt.to_vec(), txt.len())?;

    let result = prs_tokenize_loop(&mut prsdata);

    wparser_def::prsd_end(prsdata);

    result
}

fn prs_tokenize_loop(prsdata: &mut TParser) -> PgResult<Vec<LexemeEntry>> {
    let mut st: Vec<LexemeEntry> = Vec::with_capacity(16);

    loop {
        let (type_, lex) = wparser_def::prsd_nexttoken(prsdata)?;
        if type_ == 0 {
            break;
        }
        st.push(LexemeEntry {
            type_,
            lexeme: lex.to_vec(),
        });
    }

    Ok(st)
}
