mod cache;
mod dict_english;
mod dict_simple;
mod to_tsany;
mod ts_utils;

pub(crate) use to_tsany::{
    phraseto_tsquery_with_config_name, plainto_tsquery_with_config_name,
    to_tsquery_with_config_name, to_tsvector_with_config_name, ts_lexize_with_dictionary_name,
    websearch_to_tsquery_with_config_name,
};
