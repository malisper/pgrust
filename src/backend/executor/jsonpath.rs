use super::ExecError;

// :HACK: root compatibility shim while JSONPath parsing/evaluation lives in
// `pgrust_expr`.
pub use pgrust_expr::jsonpath::{EvaluationContext, JsonPath, PathMode};

struct RootExprServices;

static ROOT_EXPR_SERVICES: RootExprServices = RootExprServices;

impl pgrust_expr::ExprServices for RootExprServices {
    fn push_warning(&self, message: String) {
        crate::backend::utils::misc::notices::push_warning(message);
    }
}

fn with_root_expr_services<T>(f: impl FnOnce() -> T) -> T {
    pgrust_expr::with_expr_services(&ROOT_EXPR_SERVICES, f)
}

pub fn validate_jsonpath(text: &str) -> Result<(), ExecError> {
    with_root_expr_services(|| pgrust_expr::jsonpath::validate_jsonpath(text).map_err(Into::into))
}

pub fn canonicalize_jsonpath(text: &str) -> Result<String, ExecError> {
    with_root_expr_services(|| {
        pgrust_expr::jsonpath::canonicalize_jsonpath(text).map_err(Into::into)
    })
}

pub fn parse_jsonpath(text: &str) -> Result<JsonPath, ExecError> {
    with_root_expr_services(|| pgrust_expr::jsonpath::parse_jsonpath(text).map_err(Into::into))
}

pub fn jsonpath_is_mutable(
    text: &str,
    passing_types: &[(String, crate::backend::parser::SqlType)],
) -> Result<bool, ExecError> {
    with_root_expr_services(|| {
        pgrust_expr::jsonpath::jsonpath_is_mutable(text, passing_types).map_err(Into::into)
    })
}

pub fn evaluate_jsonpath(
    path: &JsonPath,
    ctx: &EvaluationContext<'_>,
) -> Result<Vec<pgrust_expr::jsonb::JsonbValue>, ExecError> {
    with_root_expr_services(|| {
        pgrust_expr::jsonpath::evaluate_jsonpath(path, ctx).map_err(Into::into)
    })
}
