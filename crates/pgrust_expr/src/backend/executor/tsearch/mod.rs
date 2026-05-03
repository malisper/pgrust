mod headline;
mod ts_execute;
mod ts_rank;
mod tsquery_io;
mod tsquery_op;
mod tsvector_io;
mod tsvector_op;

pub use headline::ts_headline;
pub use ts_execute::{eval_tsquery_matches_tsvector, eval_tsvector_matches_tsquery};
pub use ts_rank::{ts_rank, ts_rank_cd};
pub use tsquery_io::{
    decode_tsquery_bytes, encode_tsquery_bytes, parse_tsquery_text, render_tsquery_text,
    tsquery_input_error,
};
pub use tsquery_op::{
    canonicalize_tsquery_rewrite_result, compare_tsquery, numnode, tsquery_and,
    tsquery_contained_by, tsquery_contains, tsquery_not, tsquery_operands, tsquery_or,
    tsquery_phrase, tsquery_rewrite,
};
pub use tsvector_io::{
    decode_tsvector_bytes, encode_tsvector_bytes, parse_tsvector_text, render_tsvector_text,
    tsvector_input_error,
};
pub use tsvector_op::{
    array_to_tsvector, compare_tsvector, concat_tsvector, delete_tsvector_lexemes, filter_tsvector,
    parse_ts_weight, setweight_tsvector, strip_tsvector, text_array_items, tsvector_to_array,
    unnest_tsvector,
};
