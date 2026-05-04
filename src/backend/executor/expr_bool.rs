use super::ExecError;
use crate::include::nodes::datum::Value;

// :HACK: Keep the historical root executor module path while boolean scalar
// helpers live in `pgrust_expr`.
pub(crate) fn parse_pg_bool_text(raw: &str) -> Result<bool, ExecError> {
    pgrust_expr::expr_bool::parse_pg_bool_text(raw).map_err(Into::into)
}

pub(super) fn order_bool_values(
    op: &'static str,
    left: &Value,
    right: &Value,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_bool::order_bool_values(op, left, right).map_err(Into::into)
}

pub(super) fn eval_booleq(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_bool::eval_booleq(values).map_err(Into::into)
}

pub(super) fn eval_boolne(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_bool::eval_boolne(values).map_err(Into::into)
}

pub(super) fn eval_booland_statefunc(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_bool::eval_booland_statefunc(values).map_err(Into::into)
}

pub(super) fn eval_boolor_statefunc(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_bool::eval_boolor_statefunc(values).map_err(Into::into)
}

pub(super) fn cast_integer_to_bool(value: i64) -> Value {
    pgrust_expr::expr_bool::cast_integer_to_bool(value)
}
