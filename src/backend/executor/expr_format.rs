use super::ExecError;
use crate::include::nodes::datum::NumericValue;

// :HACK: Keep the historical root executor module path while scalar formatting
// helpers live in `pgrust_expr`.
pub(crate) fn to_char_int(value: i128, format: &str) -> Result<String, ExecError> {
    pgrust_expr::expr_format::to_char_int(value, format).map_err(Into::into)
}

pub(crate) fn to_char_numeric(value: &NumericValue, format: &str) -> Result<String, ExecError> {
    pgrust_expr::expr_format::to_char_numeric(value, format).map_err(Into::into)
}

pub(crate) fn to_char_float(value: f64, format: &str) -> Result<String, ExecError> {
    pgrust_expr::expr_format::to_char_float(value, format).map_err(Into::into)
}

pub(crate) fn to_char_float4(value: f64, format: &str) -> Result<String, ExecError> {
    pgrust_expr::expr_format::to_char_float4(value, format).map_err(Into::into)
}

pub(crate) fn to_number_numeric(input: &str, format: &str) -> Result<NumericValue, ExecError> {
    pgrust_expr::expr_format::to_number_numeric(input, format).map_err(Into::into)
}

pub(crate) fn format_roman(value: i128, fill_mode: bool, lower: bool) -> String {
    pgrust_expr::expr_format::format_roman(value, fill_mode, lower)
}

pub(crate) fn ordinal_suffix(value: i128) -> &'static str {
    pgrust_expr::expr_format::ordinal_suffix(value)
}
