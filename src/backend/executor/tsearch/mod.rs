pub(crate) mod headline;
pub(crate) mod ts_execute;
pub(crate) mod ts_rank;
pub(crate) mod tsquery_io;
pub(crate) mod tsquery_op;
pub(crate) mod tsvector_io;
pub(crate) mod tsvector_op;

pub(crate) use headline::ts_headline;
pub(crate) use ts_execute::{eval_tsquery_matches_tsvector, eval_tsvector_matches_tsquery};
pub(crate) use ts_rank::{ts_rank, ts_rank_cd};
pub(crate) use tsquery_io::{
    decode_tsquery_bytes, encode_tsquery_bytes, parse_tsquery_text, render_tsquery_text,
    tsquery_input_error,
};
pub(crate) use tsquery_op::{
    canonicalize_tsquery_rewrite_result, compare_tsquery, numnode, tsquery_and,
    tsquery_contained_by, tsquery_contains, tsquery_not, tsquery_operands, tsquery_or,
    tsquery_phrase, tsquery_rewrite,
};
pub(crate) use tsvector_io::{
    decode_tsvector_bytes, encode_tsvector_bytes, parse_tsvector_text, render_tsvector_text,
    tsvector_input_error,
};
pub(crate) use tsvector_op::{
    array_to_tsvector, compare_tsvector, concat_tsvector, delete_tsvector_lexemes, filter_tsvector,
    parse_ts_weight, setweight_tsvector, strip_tsvector, text_array_items, tsvector_to_array,
    unnest_tsvector,
};
