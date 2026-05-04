use std::cmp::Ordering;

use super::ExecError;
use crate::include::nodes::datum::{InetValue, Value};
use crate::include::nodes::primnodes::BuiltinScalarFunction;

// :HACK: Keep the historical root executor module path while network scalar
// helpers live in `pgrust_expr`.
pub(crate) fn parse_inet_text(text: &str) -> Result<InetValue, ExecError> {
    pgrust_expr::expr_network::parse_inet_text(text).map_err(Into::into)
}

pub(crate) fn parse_cidr_text(text: &str) -> Result<InetValue, ExecError> {
    pgrust_expr::expr_network::parse_cidr_text(text).map_err(Into::into)
}

pub(crate) fn parse_inet_bytes(bytes: &[u8]) -> Result<InetValue, ExecError> {
    pgrust_expr::expr_network::parse_inet_bytes(bytes).map_err(Into::into)
}

pub(crate) fn parse_cidr_bytes(bytes: &[u8]) -> Result<InetValue, ExecError> {
    pgrust_expr::expr_network::parse_cidr_bytes(bytes).map_err(Into::into)
}

pub(crate) fn render_network_text(value: &Value) -> Option<String> {
    pgrust_expr::expr_network::render_network_text(value)
}

pub(crate) fn network_bitwise_not(value: Value) -> Result<Value, ExecError> {
    pgrust_expr::expr_network::network_bitwise_not(value).map_err(Into::into)
}

pub(crate) fn network_bitwise_binary(
    op: &'static str,
    left: Value,
    right: Value,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_network::network_bitwise_binary(op, left, right).map_err(Into::into)
}

pub(crate) fn network_add(left: Value, right: Value) -> Result<Value, ExecError> {
    pgrust_expr::expr_network::network_add(left, right).map_err(Into::into)
}

pub(crate) fn network_sub(left: Value, right: Value) -> Result<Value, ExecError> {
    pgrust_expr::expr_network::network_sub(left, right).map_err(Into::into)
}

pub(crate) fn compare_network_values(left: &InetValue, right: &InetValue) -> Ordering {
    pgrust_expr::expr_network::compare_network_values(left, right)
}

pub(crate) fn encode_network_bytes(value: &InetValue, cidr: bool) -> Vec<u8> {
    pgrust_expr::expr_network::encode_network_bytes(value, cidr)
}

pub(crate) fn eval_network_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    pgrust_expr::expr_network::eval_network_function(func, values)
        .map(|result| result.map_err(Into::into))
}

pub(crate) fn network_prefix(value: &InetValue) -> InetValue {
    pgrust_expr::expr_network::network_prefix(value)
}

pub(crate) fn network_broadcast(value: &InetValue) -> InetValue {
    pgrust_expr::expr_network::network_broadcast(value)
}

pub(crate) fn network_btree_upper_bound(value: &InetValue) -> InetValue {
    pgrust_expr::expr_network::network_btree_upper_bound(value)
}

pub(crate) fn network_merge(left: &InetValue, right: &InetValue) -> InetValue {
    pgrust_expr::expr_network::network_merge(left, right)
}

pub(crate) fn network_contains(container: &InetValue, value: &InetValue, strict: bool) -> bool {
    pgrust_expr::expr_network::network_contains(container, value, strict)
}
