//! A runnable, production-quality full-SQL parser built from the c2rust
//! translation of PostgreSQL 18.3's `gram.c` (bison parser), rewired to
//! construct the repo's shared node-struct types (`backend-nodes-types` +
//! `pgrust-pg-ffi`).
//!
//! `gram.rs` holds the LR tables and ~4800 action blocks.  The [`support`]
//! module supplies everything the grammar needs to *run*: the parser memory
//! context (`palloc`), the `make*`/list/value node constructors over repo
//! types, the `ereport`/`scanner_yyerror` error path, and a `base_yylex` bridge
//! over the repo's tested scanner (`backend-parser-scan` + the
//! `backend-parser-driver_fgram` lookahead filter).
//!
//! [`raw_parser`] is the public entry the parser seam
//! (`BootPgRuntime.raw_parser`) calls: it returns a `List *` of repo `RawStmt`
//! nodes for any SQL, replacing the SELECT-only hand-written grammar.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_variables)]

mod gram;
mod support;

#[cfg(test)]
mod tests;

pub use gram::{base_yy_extra_type, base_yyparse, parser_init, YYSTYPE};
pub use pg_ffi_fgram::spi::RawParseMode;
pub use support::{
    last_error, last_error_hint_detail, last_error_message, raw_parser, raw_parser_bytes,
    ParserAbort,
};
