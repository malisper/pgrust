use std::cmp::Ordering;

use super::ExecError;
use super::node_types::{
    BuiltinScalarFunction, RangeBound, RangeTypeRef, RangeValue, SqlType, Value,
};
use crate::backend::parser::CatalogLookup;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;

// :HACK: Keep the historical root executor module path while range scalar
// helpers live in `pgrust_expr`.
pub(crate) fn parse_range_text(text: &str, ty: SqlType) -> Result<Value, ExecError> {
    pgrust_expr::expr_range::parse_range_text(text, ty).map_err(Into::into)
}

pub fn render_range_text(value: &Value) -> Option<String> {
    pgrust_expr::expr_range::render_range_text(value)
}

pub fn render_range_text_with_config(
    value: &Value,
    datetime_config: &DateTimeConfig,
) -> Option<String> {
    pgrust_expr::expr_range::render_range_text_with_config(value, datetime_config)
}

pub(crate) fn render_range_value(range: &RangeValue) -> String {
    pgrust_expr::expr_range::render_range_value(range)
}

pub(crate) fn render_range_value_with_config(
    range: &RangeValue,
    datetime_config: &DateTimeConfig,
) -> String {
    pgrust_expr::expr_range::render_range_value_with_config(range, datetime_config)
}

pub(crate) fn compare_range_values(left: &RangeValue, right: &RangeValue) -> Ordering {
    pgrust_expr::expr_range::compare_range_values(left, right)
}

pub(crate) fn encode_range_bytes(range: &RangeValue) -> Result<Vec<u8>, ExecError> {
    pgrust_expr::expr_range::encode_range_bytes(range).map_err(Into::into)
}

pub(crate) fn decode_range_bytes(
    range_type: RangeTypeRef,
    bytes: &[u8],
) -> Result<RangeValue, ExecError> {
    pgrust_expr::expr_range::decode_range_bytes(range_type, bytes).map_err(Into::into)
}

pub(crate) fn eval_range_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
    func_variadic: bool,
    catalog: Option<&dyn CatalogLookup>,
    datetime_config: &DateTimeConfig,
) -> Option<Result<Value, ExecError>> {
    let adapter = catalog.map(ExprCatalogAdapter);
    let catalog = adapter
        .as_ref()
        .map(|adapter| adapter as &dyn pgrust_expr::ExprCatalogLookup);
    pgrust_expr::expr_range::eval_range_function(
        func,
        values,
        result_type,
        func_variadic,
        catalog,
        datetime_config,
    )
    .map(|result| result.map_err(Into::into))
}

struct ExprCatalogAdapter<'a>(&'a dyn CatalogLookup);

impl pgrust_expr::ExprCatalogLookup for ExprCatalogAdapter<'_> {
    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<pgrust_expr::BoundRelation> {
        self.0.lookup_relation_by_oid(relation_oid).map(|relation| {
            let name = self
                .0
                .class_row_by_oid(relation.relation_oid)
                .map(|row| row.relname)
                .unwrap_or_else(|| relation.relation_oid.to_string());
            pgrust_expr::BoundRelation {
                relation_oid: relation.relation_oid,
                oid: Some(relation.relation_oid),
                name,
                relkind: relation.relkind,
                desc: relation.desc,
            }
        })
    }
}

pub(crate) fn range_intersection_agg_transition(
    current: Option<Value>,
    input: &Value,
) -> Result<Option<Value>, ExecError> {
    pgrust_expr::expr_range::range_intersection_agg_transition(current, input).map_err(Into::into)
}

pub(crate) fn normalize_range(
    range_type: RangeTypeRef,
    lower: Option<RangeBound>,
    upper: Option<RangeBound>,
) -> Result<RangeValue, ExecError> {
    pgrust_expr::expr_range::normalize_range(range_type, lower, upper).map_err(Into::into)
}

pub(crate) fn empty_range(range_type: RangeTypeRef) -> RangeValue {
    pgrust_expr::expr_range::empty_range(range_type)
}

pub(crate) fn range_contains_range(left: &RangeValue, right: &RangeValue) -> bool {
    pgrust_expr::expr_range::range_contains_range(left, right)
}

pub(crate) fn range_contains_element(range: &RangeValue, value: &Value) -> Result<bool, ExecError> {
    pgrust_expr::expr_range::range_contains_element(range, value).map_err(Into::into)
}

pub(crate) fn range_overlap(left: &RangeValue, right: &RangeValue) -> bool {
    pgrust_expr::expr_range::range_overlap(left, right)
}

pub(crate) fn range_adjacent(left: &RangeValue, right: &RangeValue) -> bool {
    pgrust_expr::expr_range::range_adjacent(left, right)
}

pub(crate) fn range_strict_left(left: &RangeValue, right: &RangeValue) -> bool {
    pgrust_expr::expr_range::range_strict_left(left, right)
}

pub(crate) fn range_strict_right(left: &RangeValue, right: &RangeValue) -> bool {
    pgrust_expr::expr_range::range_strict_right(left, right)
}

pub(crate) fn range_over_left_bounds(left: &RangeValue, right: &RangeValue) -> bool {
    pgrust_expr::expr_range::range_over_left_bounds(left, right)
}

pub(crate) fn range_over_right_bounds(left: &RangeValue, right: &RangeValue) -> bool {
    pgrust_expr::expr_range::range_over_right_bounds(left, right)
}

pub(crate) fn range_intersection(left: &RangeValue, right: &RangeValue) -> RangeValue {
    pgrust_expr::expr_range::range_intersection(left, right)
}

pub(crate) fn range_merge(left: &RangeValue, right: &RangeValue) -> RangeValue {
    pgrust_expr::expr_range::range_merge(left, right)
}

pub(crate) fn range_union(left: &RangeValue, right: &RangeValue) -> Result<RangeValue, ExecError> {
    pgrust_expr::expr_range::range_union(left, right).map_err(Into::into)
}

pub(crate) fn range_difference_segments(
    left: &RangeValue,
    right: &RangeValue,
) -> Result<Vec<RangeValue>, ExecError> {
    pgrust_expr::expr_range::range_difference_segments(left, right).map_err(Into::into)
}

pub(crate) fn compare_lower_bounds(
    left: Option<&RangeBound>,
    right: Option<&RangeBound>,
) -> Ordering {
    pgrust_expr::expr_range::compare_lower_bounds(left, right)
}

pub(crate) fn compare_upper_bounds(
    left: Option<&RangeBound>,
    right: Option<&RangeBound>,
) -> Ordering {
    pgrust_expr::expr_range::compare_upper_bounds(left, right)
}

pub(crate) fn bounds_adjacent(upper: Option<&RangeBound>, lower: Option<&RangeBound>) -> bool {
    pgrust_expr::expr_range::bounds_adjacent(upper, lower)
}

pub(crate) fn compare_scalar_values(left: &Value, right: &Value) -> Ordering {
    pgrust_expr::expr_range::compare_scalar_values(left, right)
}
