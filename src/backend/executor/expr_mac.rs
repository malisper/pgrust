use super::ExecError;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::BuiltinScalarFunction;

// :HACK: Keep the historical root executor module path while MAC address
// scalar helpers live in `pgrust_expr`.
pub(crate) fn parse_macaddr_text(text: &str) -> Result<[u8; 6], ExecError> {
    pgrust_expr::expr_mac::parse_macaddr_text(text).map_err(Into::into)
}

pub(crate) fn parse_macaddr8_text(text: &str) -> Result<[u8; 8], ExecError> {
    pgrust_expr::expr_mac::parse_macaddr8_text(text).map_err(Into::into)
}

pub(crate) fn parse_macaddr_bytes(bytes: &[u8]) -> Result<[u8; 6], ExecError> {
    pgrust_expr::expr_mac::parse_macaddr_bytes(bytes).map_err(Into::into)
}

pub(crate) fn parse_macaddr8_bytes(bytes: &[u8]) -> Result<[u8; 8], ExecError> {
    pgrust_expr::expr_mac::parse_macaddr8_bytes(bytes).map_err(Into::into)
}

pub fn render_macaddr_text(value: &[u8; 6]) -> String {
    pgrust_expr::expr_mac::render_macaddr_text(value)
}

pub fn render_macaddr8_text(value: &[u8; 8]) -> String {
    pgrust_expr::expr_mac::render_macaddr8_text(value)
}

pub(crate) fn macaddr_to_macaddr8(value: [u8; 6]) -> [u8; 8] {
    pgrust_expr::expr_mac::macaddr_to_macaddr8(value)
}

pub(crate) fn macaddr8_to_macaddr(value: [u8; 8]) -> Result<[u8; 6], ExecError> {
    pgrust_expr::expr_mac::macaddr8_to_macaddr(value).map_err(Into::into)
}

pub(crate) fn eval_macaddr_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    pgrust_expr::expr_mac::eval_macaddr_function(func, values)
        .map(|result| result.map_err(Into::into))
}
