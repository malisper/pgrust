pub(crate) mod cache;
mod dict_english;
mod dict_simple;
pub(crate) mod parser;
mod to_tsany;
mod ts_utils;

pub(crate) use cache::resolve_config_with_gucs;
pub(crate) use parser::{parse_default, token_kind, token_kinds};
pub(crate) use to_tsany::{
    phraseto_tsquery_with_config_name, plainto_tsquery_with_config_name,
    to_tsquery_with_config_name, to_tsvector_with_config_name, ts_lexize_with_dictionary_name,
    tsvector_lexemes_with_config_name, websearch_to_tsquery_with_config_name,
};
