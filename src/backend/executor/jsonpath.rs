// :HACK: root compatibility shim while JSONPath parsing/evaluation lives in
// `pgrust_expr`.
pub use pgrust_expr::jsonpath::{
    EvaluationContext, JsonPath, PathMode, evaluate_jsonpath, jsonpath_is_mutable, parse_jsonpath,
    validate_jsonpath,
};

use super::ExecError;

pub fn canonicalize_jsonpath(text: &str) -> Result<String, ExecError> {
    pgrust_expr::jsonpath::canonicalize_jsonpath(text).map_err(Into::into)
}
