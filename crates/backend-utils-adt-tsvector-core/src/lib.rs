//! Port of the `tsvector` core ADT translation units (PostgreSQL 18.3):
//!
//!  * `tsvector.c` — `tsvector` I/O (`tsvectorin`/`tsvectorout`/`tsvectorsend`/
//!    `tsvectorrecv`) and the qsort/dedup helpers (`compareWordEntryPos`,
//!    `uniquePos`, `compareentry`, `uniqueentry`);
//!  * `tsvector_parser.c` — the shared `tsvector`/`tsquery` value tokenizer
//!    (`init`/`reset`/`gettoken`/`close_tsvector_parser`) behind an opaque
//!    [`TsVectorParseStateHandle`] token;
//!  * `tsvector_op.c` — every operation on `tsvector`: the comparison family,
//!    the manipulation functions (strip/setweight/delete/filter/concat/unnest),
//!    the array bridges, the `TS_execute` query-evaluation engine, the `@@`
//!    match operators, the `ts_stat` statistics aggregator and the
//!    `tsvector_update_trigger`.
//!
//! Memory model: a `tsvector` value is its flat varlena image (`&[u8]` in,
//! `Vec<u8>` out — the `palloc`-into-caller's-context analog), exactly as the
//! sibling `backend-utils-adt-tsquery-core` crate models it. Transient working
//! buffers are charged to a caller-supplied [`mcx::Mcx`].
//!
//! Genuine externals (mirror-and-panic until their owner lands):
//!  * `pg_mblen` / `pg_database_encoding_max_length` (`mbutils.c`), via
//!    `backend-utils-mb-mbutils-seams`;
//!  * `check_stack_depth` / `CHECK_FOR_INTERRUPTS` (`tcop/postgres.c`), via
//!    `backend-tcop-postgres-seams`;
//!  * the array element I/O of the array<->tsvector bridges, via
//!    `backend-utils-adt-array-more-seams`;
//!  * the funcapi SRF emission, the SPI cursor of `ts_stat_sql`, the
//!    text-search-config resolution, and the trigger-manager / `SPI_*` /
//!    dictionary-pipeline primitives of `tsvector_update_trigger`, via
//!    `backend-utils-adt-tsvector-ext-seams`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]

pub mod access;
pub mod fmgr_builtins;
pub mod io;
pub mod op;
pub mod parser;

/// Install this crate's seams. The owner installs the four
/// `tsvector_parser.c` engine seams plus `tsCompareString`,
/// `tsquery_requires_match`, `TS_execute` and `TS_execute_ternary`, which the
/// landed `tsquery` core and the GIN/GiST/rank support functions consume, and
/// registers the `tsvector` fmgr builtins.
pub fn init_seams() {
    use backend_utils_adt_tsvector_core_seams as s;
    s::init_tsvector_parser::set(parser::init_tsvector_parser_seam);
    s::reset_tsvector_parser::set(parser::reset_tsvector_parser_seam);
    s::gettoken_tsvector::set(parser::gettoken_tsvector_seam);
    s::close_tsvector_parser::set(parser::close_tsvector_parser_seam);
    s::ts_compare_string::set(op::tsCompareString);
    s::tsquery_requires_match::set(op::tsquery_requires_match);
    s::ts_execute::set(op::ts_execute_seam);
    s::ts_execute_ternary::set(op::ts_execute_ternary_seam);
    // Headline tsquery execution (checkcondition_HL): tsvector-core owns the
    // generic TS_execute engine, so it installs the parse-seams `*_hl` slots
    // the default parser's `prsd_headline` selector calls.
    backend_tsearch_parse_seams::ts_execute_hl::set(op::ts_execute_hl_seam);
    backend_tsearch_parse_seams::ts_execute_locations_hl::set(op::ts_execute_locations_hl_seam);
    fmgr_builtins::register_tsvector_builtins();
}
