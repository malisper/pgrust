// :HACK: root compatibility shim while scalar money operations live in
// `pgrust_expr`; keep root `ExecError` in the old signatures.
use super::ExecError;
use crate::include::nodes::datum::Value;

pub fn money_format_text(value: i64) -> String {
    pgrust_expr::expr_money::money_format_text(value)
}

pub(crate) fn money_parse_text(input: &str) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_parse_text(input).map_err(Into::into)
}

pub(crate) fn money_numeric_text(value: i64) -> String {
    pgrust_expr::expr_money::money_numeric_text(value)
}

pub(crate) fn money_add(left: i64, right: i64) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_add(left, right).map_err(Into::into)
}

pub(crate) fn money_sub(left: i64, right: i64) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_sub(left, right).map_err(Into::into)
}

pub(crate) fn money_mul_int(left: i64, right: i64) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_mul_int(left, right).map_err(Into::into)
}

pub(crate) fn money_div_int(left: i64, right: i64) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_div_int(left, right).map_err(Into::into)
}

pub(crate) fn money_mul_float(left: i64, right: f64) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_mul_float(left, right).map_err(Into::into)
}

pub(crate) fn money_div_float(left: i64, right: f64) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_div_float(left, right).map_err(Into::into)
}

pub(crate) fn money_from_float(value: f64) -> Result<i64, ExecError> {
    pgrust_expr::expr_money::money_from_float(value).map_err(Into::into)
}

pub(crate) fn money_cash_div(left: i64, right: i64) -> Result<f64, ExecError> {
    pgrust_expr::expr_money::money_cash_div(left, right).map_err(Into::into)
}

pub(crate) fn money_cmp(left: i64, right: i64) -> std::cmp::Ordering {
    pgrust_expr::expr_money::money_cmp(left, right)
}

pub(crate) fn money_larger(left: i64, right: i64) -> i64 {
    pgrust_expr::expr_money::money_larger(left, right)
}

pub(crate) fn money_smaller(left: i64, right: i64) -> i64 {
    pgrust_expr::expr_money::money_smaller(left, right)
}

pub(crate) fn cash_words_text(value: i64) -> String {
    pgrust_expr::expr_money::cash_words_text(value)
}

pub(crate) fn money_value(value: &Value) -> Option<i64> {
    pgrust_expr::expr_money::money_value(value)
}
