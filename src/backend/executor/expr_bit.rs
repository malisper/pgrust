// :HACK: root compatibility shim while scalar bit-string operations live in
// `pgrust_expr`; keep root `ExecError` in the old signatures.
use std::cmp::Ordering;

use super::ExecError;
use crate::include::nodes::datum::BitString;
use crate::include::nodes::parsenodes::SqlType;

pub(crate) fn parse_bit_text(text: &str) -> Result<BitString, ExecError> {
    pgrust_expr::expr_bit::parse_bit_text(text).map_err(Into::into)
}

pub(crate) fn render_bit_text(bits: &BitString) -> String {
    pgrust_expr::expr_bit::render_bit_text(bits)
}

pub(crate) fn coerce_bit_string(
    bits: BitString,
    ty: SqlType,
    explicit: bool,
) -> Result<BitString, ExecError> {
    pgrust_expr::expr_bit::coerce_bit_string(bits, ty, explicit).map_err(Into::into)
}

pub(crate) fn resize_bit_string(bits: BitString, target_len: i32) -> BitString {
    pgrust_expr::expr_bit::resize_bit_string(bits, target_len)
}

pub(crate) fn compare_bit_strings(left: &BitString, right: &BitString) -> Ordering {
    pgrust_expr::expr_bit::compare_bit_strings(left, right)
}

pub(crate) fn concat_bit_strings(left: &BitString, right: &BitString) -> BitString {
    pgrust_expr::expr_bit::concat_bit_strings(left, right)
}

pub(crate) fn bitwise_not(bits: &BitString) -> BitString {
    pgrust_expr::expr_bit::bitwise_not(bits)
}

pub(crate) fn bitwise_binary(
    op: &'static str,
    left: &BitString,
    right: &BitString,
) -> Result<BitString, ExecError> {
    pgrust_expr::expr_bit::bitwise_binary(op, left, right).map_err(Into::into)
}

pub(crate) fn shift_left(bits: &BitString, count: i32) -> BitString {
    pgrust_expr::expr_bit::shift_left(bits, count)
}

pub(crate) fn shift_right(bits: &BitString, count: i32) -> BitString {
    pgrust_expr::expr_bit::shift_right(bits, count)
}

pub(crate) fn bit_length(bits: &BitString) -> i32 {
    pgrust_expr::expr_bit::bit_length(bits)
}

pub(crate) fn substring(
    bits: &BitString,
    start: i32,
    len: Option<i32>,
) -> Result<BitString, ExecError> {
    pgrust_expr::expr_bit::substring(bits, start, len).map_err(Into::into)
}

pub(crate) fn overlay(
    bits: &BitString,
    placing: &BitString,
    start: i32,
    len: Option<i32>,
) -> Result<BitString, ExecError> {
    pgrust_expr::expr_bit::overlay(bits, placing, start, len).map_err(Into::into)
}

pub(crate) fn position(needle: &BitString, haystack: &BitString) -> i32 {
    pgrust_expr::expr_bit::position(needle, haystack)
}

pub(crate) fn get_bit(bits: &BitString, index: i32) -> Result<i32, ExecError> {
    pgrust_expr::expr_bit::get_bit(bits, index).map_err(Into::into)
}

pub(crate) fn set_bit(
    bits: &BitString,
    index: i32,
    new_value: i32,
) -> Result<BitString, ExecError> {
    pgrust_expr::expr_bit::set_bit(bits, index, new_value).map_err(Into::into)
}

pub(crate) fn bit_count(bits: &BitString) -> i64 {
    pgrust_expr::expr_bit::bit_count(bits)
}
