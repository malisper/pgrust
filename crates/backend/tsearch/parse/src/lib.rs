//! Idiomatic port of the tsearch parsing layer (PostgreSQL 18.3):
//!
//! * `wparser_def.c` — [`wparser_def`]: the default word parser (`prsd_*`) and
//!   the big token-type [`wparser_def::TParser`] state machine;
//! * `ts_parse.c` — [`ts_parse`]: the text parsing driver ([`parsetext`]) and
//!   the lexize state machine that applies a configuration's dictionaries, plus
//!   the headline framework ([`hlparsetext`], [`generateHeadline`]);
//! * `wparser.c` — [`wparser`]: the data-producing core of the SQL-facing
//!   `ts_parse` / `ts_token_type` helpers.
//!
//! The default word parser (the centerpiece of this crate) is implemented
//! fully in-crate: the entire state/action table, the `p_is*` predicates, and
//! the `TParserGet` driver are ported 1:1 from PostgreSQL 18.3.
//!
//! # Owned memory model
//!
//! This is the owned-value port: the parser's input / wide-char buffers and the
//! produced word lists live in owned [`alloc::vec::Vec`]s freed on drop (the
//! idiomatic analog of the `palloc`'d C buffers, which live in the parser's own
//! context and are released by `TParserClose` / `ParsedText` free). There is no
//! `unsafe` anywhere in the crate.
//!
//! # Seams
//!
//! The genuinely-external multibyte-encoding / locale helpers and the
//! text-search configuration / dictionary cache + fmgr `lexize` dispatch + the
//! generic tsquery execution engine cross [`parse_seams`]
//! (function-pointer slots, loud-panic default), installed by their owning
//! subsystems. Everything else is ported in-crate (no in-crate deferrals).

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

extern crate alloc;

pub mod install;
pub mod seam;
pub mod ts_parse;
pub mod wparser;
pub mod wparser_def;

pub use install::init_seams;

pub use ts_parse::{
    generateHeadline, hlparsetext, parsetext, DictSubState, ExecPhraseData, HeadlineParsedText,
    HeadlineWordEntry, LexizeLexeme, ParsedText, ParsedWord, QueryItem, QueryOperand,
    QueryOperator, TSQuery, TS_EXEC_EMPTY,
};
pub use wparser::{prs_tokenize, tt_storage_list, LexemeEntry, TokenTypeRow};
pub use wparser_def::{
    mark_fragment, prsd_end, prsd_headline, prsd_lextype, prsd_nexttoken, prsd_start, TParser,
};

#[cfg(test)]
mod tests;
