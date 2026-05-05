// :HACK: Preserve the historical root value-I/O array path while the portable
// array storage/text implementation lives in `pgrust_expr`.
use super::*;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;

pub(crate) fn encode_array_bytes(
    element_type: SqlType,
    array: &ArrayValue,
) -> Result<Vec<u8>, ExecError> {
    pgrust_expr::executor::value_io::encode_array_bytes(element_type, array).map_err(Into::into)
}

pub(crate) fn encode_anyarray_bytes(array: &ArrayValue) -> Result<Vec<u8>, ExecError> {
    pgrust_expr::executor::value_io::encode_anyarray_bytes(array).map_err(Into::into)
}

pub(crate) fn decode_array_bytes(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    pgrust_expr::executor::value_io::decode_array_bytes(element_type, bytes).map_err(Into::into)
}

pub(crate) fn decode_anyarray_bytes(bytes: &[u8]) -> Result<Value, ExecError> {
    pgrust_expr::executor::value_io::decode_anyarray_bytes(bytes).map_err(Into::into)
}

pub(crate) fn builtin_type_oid_for_sql_type(sql_type: SqlType) -> Option<u32> {
    pgrust_expr::executor::value_io::builtin_type_oid_for_sql_type(sql_type)
}

pub(crate) fn format_array_text(items: &[Value]) -> String {
    pgrust_expr::executor::value_io::format_array_text(items)
}

pub(crate) fn format_array_text_with_config(
    items: &[Value],
    datetime_config: &DateTimeConfig,
) -> String {
    pgrust_expr::executor::value_io::format_array_text_with_config(items, datetime_config)
}

pub fn format_array_value_text(array: &ArrayValue) -> String {
    pgrust_expr::executor::value_io::format_array_value_text(array)
}

pub fn format_array_value_text_with_config(
    array: &ArrayValue,
    datetime_config: &DateTimeConfig,
) -> String {
    pgrust_expr::executor::value_io::format_array_value_text_with_config(array, datetime_config)
}
