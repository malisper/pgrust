use std::cmp::Ordering;

use super::ExecError;
use super::node_types::{
    BuiltinScalarFunction, MultirangeTypeRef, MultirangeValue, RangeValue, SqlType, Value,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;

// :HACK: Keep the historical root executor module path while multirange scalar
// helpers live in `pgrust_expr`.
pub(crate) fn parse_multirange_text(text: &str, ty: SqlType) -> Result<Value, ExecError> {
    pgrust_expr::expr_multirange::parse_multirange_text(text, ty).map_err(Into::into)
}

pub fn render_multirange_text(value: &Value) -> Option<String> {
    pgrust_expr::expr_multirange::render_multirange_text(value)
}

pub fn render_multirange_text_with_config(
    value: &Value,
    datetime_config: &DateTimeConfig,
) -> Option<String> {
    pgrust_expr::expr_multirange::render_multirange_text_with_config(value, datetime_config)
}

pub(crate) fn render_multirange(multirange: &MultirangeValue) -> String {
    pgrust_expr::expr_multirange::render_multirange(multirange)
}

pub(crate) fn render_multirange_with_config(
    multirange: &MultirangeValue,
    datetime_config: &DateTimeConfig,
) -> String {
    pgrust_expr::expr_multirange::render_multirange_with_config(multirange, datetime_config)
}

pub(crate) fn compare_multirange_values(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> Ordering {
    pgrust_expr::expr_multirange::compare_multirange_values(left, right)
}

pub(crate) fn encode_multirange_bytes(multirange: &MultirangeValue) -> Result<Vec<u8>, ExecError> {
    pgrust_expr::expr_multirange::encode_multirange_bytes(multirange).map_err(Into::into)
}

pub(crate) fn decode_multirange_bytes(
    multirange_type: MultirangeTypeRef,
    bytes: &[u8],
) -> Result<MultirangeValue, ExecError> {
    pgrust_expr::expr_multirange::decode_multirange_bytes(multirange_type, bytes)
        .map_err(Into::into)
}

pub(crate) fn multirange_from_range(range: &RangeValue) -> Result<MultirangeValue, ExecError> {
    pgrust_expr::expr_multirange::multirange_from_range(range).map_err(Into::into)
}

pub(crate) fn normalize_multirange(
    multirange_type: MultirangeTypeRef,
    ranges: Vec<RangeValue>,
) -> Result<MultirangeValue, ExecError> {
    pgrust_expr::expr_multirange::normalize_multirange(multirange_type, ranges).map_err(Into::into)
}

pub(crate) fn range_agg_transition(
    current: Option<MultirangeValue>,
    input: &Value,
) -> Result<Option<MultirangeValue>, ExecError> {
    pgrust_expr::expr_multirange::range_agg_transition(current, input).map_err(Into::into)
}

pub(crate) fn multirange_intersection_agg_transition(
    current: Option<Value>,
    input: &Value,
) -> Result<Option<Value>, ExecError> {
    pgrust_expr::expr_multirange::multirange_intersection_agg_transition(current, input)
        .map_err(Into::into)
}

pub(crate) fn eval_multirange_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
    func_variadic: bool,
) -> Option<Result<Value, ExecError>> {
    pgrust_expr::expr_multirange::eval_multirange_function(func, values, result_type, func_variadic)
        .map(|result| result.map_err(Into::into))
}

pub(crate) fn multirange_contains_element(
    multirange: &MultirangeValue,
    value: &Value,
) -> Result<bool, ExecError> {
    pgrust_expr::expr_multirange::multirange_contains_element(multirange, value).map_err(Into::into)
}

pub(crate) fn multirange_contains_range(multirange: &MultirangeValue, range: &RangeValue) -> bool {
    pgrust_expr::expr_multirange::multirange_contains_range(multirange, range)
}

pub(crate) fn range_contains_multirange(range: &RangeValue, multirange: &MultirangeValue) -> bool {
    pgrust_expr::expr_multirange::range_contains_multirange(range, multirange)
}

pub(crate) fn multirange_contains_multirange(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> bool {
    pgrust_expr::expr_multirange::multirange_contains_multirange(left, right)
}

pub(crate) fn multirange_overlaps_range(multirange: &MultirangeValue, range: &RangeValue) -> bool {
    pgrust_expr::expr_multirange::multirange_overlaps_range(multirange, range)
}

pub(crate) fn multirange_overlaps_multirange(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> bool {
    pgrust_expr::expr_multirange::multirange_overlaps_multirange(left, right)
}

pub(crate) fn multirange_adjacent_range(multirange: &MultirangeValue, range: &RangeValue) -> bool {
    pgrust_expr::expr_multirange::multirange_adjacent_range(multirange, range)
}

pub(crate) fn multirange_adjacent_multirange(
    left: &MultirangeValue,
    right: &MultirangeValue,
) -> bool {
    pgrust_expr::expr_multirange::multirange_adjacent_multirange(left, right)
}

pub(crate) fn span_multirange(multirange: &MultirangeValue) -> RangeValue {
    pgrust_expr::expr_multirange::span_multirange(multirange)
}
