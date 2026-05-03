use super::ExecError;
use super::jsonb::validate_json_text_input;
use super::jsonpath::canonicalize_jsonpath;
use pgrust_core::CompactString;

pub fn validate_json_text(text: &str) -> Result<(), ExecError> {
    validate_json_text_input(text)
}

pub fn canonicalize_jsonpath_text(text: &str) -> Result<CompactString, ExecError> {
    canonicalize_jsonpath(text).map(CompactString::from_owned)
}
