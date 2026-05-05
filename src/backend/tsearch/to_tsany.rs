// :HACK: Preserve the historical root text-search path while the analyzer
// catalog adapter lives in `pgrust_analyze`.
pub(crate) use pgrust_analyze::tsearch::{
    phraseto_tsquery_with_config_name, plainto_tsquery_with_config_name,
    to_tsquery_with_config_name, to_tsvector_with_config_name, ts_lexize_with_dictionary_name,
    tsvector_lexemes_with_config_name, websearch_to_tsquery_with_config_name,
};
