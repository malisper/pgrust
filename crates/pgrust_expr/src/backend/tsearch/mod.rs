pub mod cache;
pub mod dict_english;
pub mod dict_simple;
pub mod parser;
mod to_tsany;
pub mod ts_utils;

pub use cache::{TextSearchConfig, TextSearchDictionary, resolve_config, resolve_config_with_gucs};
pub use parser::{parse_default, token_kind, token_kinds};
pub use to_tsany::{
    phraseto_tsquery_with_config_name, plainto_tsquery_with_config_name,
    to_tsquery_with_config_name, to_tsvector_with_config_name, ts_lexize_with_dictionary_name,
    tsvector_lexemes_with_config_name, websearch_to_tsquery_with_config_name,
};
pub use ts_utils::{
    lexize_token_for_config, lexize_token_for_config_and_type, lexize_token_for_dictionary,
    lexize_token_with_config, lexize_token_with_config_and_type,
};
