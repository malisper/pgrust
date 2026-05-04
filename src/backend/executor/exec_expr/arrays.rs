use super::*;

pub(super) fn eval_quantified_array(
    left_value: &Value,
    op: SubqueryComparisonOp,
    collation_oid: Option<u32>,
    is_all: bool,
    array_value: &Value,
) -> Result<Value, ExecError> {
    pgrust_executor::eval_quantified_array(left_value, op, collation_oid, is_all, array_value)
        .map_err(Into::into)
}

pub(super) fn eval_array_subscript(
    value: Value,
    array_type: Option<SqlType>,
    subscripts: &[crate::include::nodes::primnodes::ExprArraySubscript],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let resolved = subscripts
        .iter()
        .map(|subscript| {
            Ok(pgrust_executor::ResolvedArraySubscript {
                is_slice: subscript.is_slice,
                lower_provided: subscript.lower.is_some(),
                upper_provided: subscript.upper.is_some(),
                lower: subscript
                    .lower
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    pgrust_executor::apply_array_subscripts(
        value,
        &resolved,
        preserves_partial_scalar_subscript(array_type, ctx.catalog.as_deref()),
    )
    .map_err(Into::into)
}

pub(super) fn eval_array_subscript_plpgsql(
    value: Value,
    subscripts: &[crate::include::nodes::primnodes::ExprArraySubscript],
    slot: &mut TupleSlot,
) -> Result<Value, ExecError> {
    let resolved = subscripts
        .iter()
        .map(|subscript| {
            Ok(pgrust_executor::ResolvedArraySubscript {
                is_slice: subscript.is_slice,
                lower_provided: subscript.lower.is_some(),
                upper_provided: subscript.upper.is_some(),
                lower: subscript
                    .lower
                    .as_ref()
                    .map(|expr| eval_plpgsql_expr(expr, slot))
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_ref()
                    .map(|expr| eval_plpgsql_expr(expr, slot))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    pgrust_executor::apply_array_subscripts(value, &resolved, false).map_err(Into::into)
}

fn preserves_partial_scalar_subscript(
    array_type: Option<SqlType>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    let (Some(sql_type), Some(catalog)) = (array_type, catalog) else {
        return false;
    };
    if !sql_type.is_array || sql_type.typrelid == 0 {
        return false;
    }
    catalog
        .domain_by_type_oid(sql_type.type_oid)
        .is_some_and(|domain| {
            domain.sql_type.is_array
                && sql_type.typrelid == domain.array_oid
                && catalog
                    .domain_by_type_oid(domain.sql_type.type_oid)
                    .is_some()
        })
}

pub(crate) fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    pgrust_executor::normalize_array_value(value)
}

pub(super) fn eval_array_ndims_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_ndims_function(values).map_err(Into::into)
}

pub(super) fn eval_array_dims_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_dims_function(values).map_err(Into::into)
}

pub(super) fn eval_array_fill_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_fill_function(values).map_err(Into::into)
}

pub(super) fn eval_string_to_array_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_string_to_array_function(values).map_err(Into::into)
}

pub(crate) fn eval_string_to_table_rows(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    pgrust_executor::eval_string_to_table_rows(values).map_err(Into::into)
}

pub(super) fn eval_array_to_string_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_to_string_function(values).map_err(Into::into)
}

pub(super) fn eval_array_length_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_length_function(values).map_err(Into::into)
}

pub(super) fn eval_array_upper_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_upper_function(values).map_err(Into::into)
}

pub(super) fn eval_cardinality_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_cardinality_function(values).map_err(Into::into)
}

pub(super) fn eval_array_append_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_append_function(values).map_err(Into::into)
}

pub(super) fn eval_array_prepend_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_prepend_function(values).map_err(Into::into)
}

pub(super) fn eval_array_cat_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_cat_function(values).map_err(Into::into)
}

pub(super) fn eval_array_position_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_position_function(values).map_err(Into::into)
}

pub(super) fn eval_array_positions_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_positions_function(values).map_err(Into::into)
}

pub(crate) fn append_array_value(
    array: &Value,
    element: &Value,
    prepend: bool,
) -> Result<Value, ExecError> {
    pgrust_executor::append_array_value(array, element, prepend).map_err(Into::into)
}

pub(crate) fn concatenate_arrays(
    left: ArrayValue,
    right: ArrayValue,
) -> Result<ArrayValue, ExecError> {
    pgrust_executor::concatenate_arrays(left, right).map_err(Into::into)
}

pub(super) fn eval_array_remove_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_remove_function(values).map_err(Into::into)
}

pub(super) fn eval_array_replace_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_replace_function(values).map_err(Into::into)
}

pub(super) fn eval_trim_array_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_trim_array_function(values).map_err(Into::into)
}

pub(super) fn eval_array_shuffle_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_shuffle_function(values).map_err(Into::into)
}

pub(super) fn eval_array_sample_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_sample_function(values).map_err(Into::into)
}

pub(super) fn eval_array_reverse_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_reverse_function(values).map_err(Into::into)
}

pub(super) fn eval_array_sort_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_sort_function(values).map_err(Into::into)
}

pub(super) fn eval_width_bucket_thresholds(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_width_bucket_thresholds(values).map_err(Into::into)
}

pub(super) fn eval_array_lower_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_lower_function(values).map_err(Into::into)
}

pub(super) fn eval_array_overlap(left: Value, right: Value) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_overlap(left, right).map_err(Into::into)
}

pub(super) fn eval_array_contains(left: Value, right: Value) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_contains(left, right).map_err(Into::into)
}

pub(super) fn eval_array_contained(left: Value, right: Value) -> Result<Value, ExecError> {
    pgrust_executor::eval_array_contained(left, right).map_err(Into::into)
}
