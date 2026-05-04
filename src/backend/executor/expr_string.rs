// :HACK: Keep the historical root executor path while string scalar helpers live in pgrust_expr.
use super::ExecError;
use super::expr_ops::TextCollationSemantics;
use super::node_types::Value;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;

use pgrust_expr::backend::executor::expr_string;

fn map_expr_error(error: pgrust_expr::ExprError) -> ExecError {
    error.into()
}

fn expr_collation(
    collation: TextCollationSemantics,
) -> pgrust_expr::backend::executor::expr_ops::TextCollationSemantics {
    match collation {
        TextCollationSemantics::Default => {
            pgrust_expr::backend::executor::expr_ops::TextCollationSemantics::Default
        }
        TextCollationSemantics::Ascii => {
            pgrust_expr::backend::executor::expr_ops::TextCollationSemantics::Ascii
        }
        TextCollationSemantics::PgCUtf8 => {
            pgrust_expr::backend::executor::expr_ops::TextCollationSemantics::PgCUtf8
        }
        TextCollationSemantics::PgUnicodeFast => {
            pgrust_expr::backend::executor::expr_ops::TextCollationSemantics::PgUnicodeFast
        }
    }
}

macro_rules! forward_values {
    ($vis:vis $name:ident) => {
        $vis fn $name(values: &[Value]) -> Result<Value, ExecError> {
            expr_string::$name(values).map_err(map_expr_error)
        }
    };
}

pub(crate) fn eval_to_char_function(
    values: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    expr_string::eval_to_char_function(values, datetime_config).map_err(map_expr_error)
}

forward_values!(pub(super) eval_to_char_float4_function);
forward_values!(pub(super) eval_to_number_function);
forward_values!(pub(super) eval_pg_size_pretty_function);
forward_values!(pub(super) eval_pg_size_bytes_function);
forward_values!(pub(super) eval_to_bin_function);
forward_values!(pub(super) eval_to_oct_function);
forward_values!(pub(super) eval_to_hex_function);

pub(super) fn eval_concat_function(
    values: &[Value],
    func_variadic: bool,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    expr_string::eval_concat_function(values, func_variadic, datetime_config)
        .map_err(map_expr_error)
}

pub(super) fn eval_concat_ws_function(
    values: &[Value],
    func_variadic: bool,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    expr_string::eval_concat_ws_function(values, func_variadic, datetime_config)
        .map_err(map_expr_error)
}

pub(super) fn eval_format_function(
    values: &[Value],
    func_variadic: bool,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    expr_string::eval_format_function(values, func_variadic, datetime_config)
        .map_err(map_expr_error)
}

forward_values!(pub(super) eval_left_function);
forward_values!(pub(super) eval_right_function);
forward_values!(pub(super) eval_length_function);
forward_values!(pub(super) eval_bit_length_function);
forward_values!(pub(super) eval_octet_length_function);
forward_values!(pub(super) eval_repeat_function);
forward_values!(pub(super) eval_lower_function);

pub(super) fn eval_lower_function_with_collation(
    values: &[Value],
    collation: TextCollationSemantics,
) -> Result<Value, ExecError> {
    expr_string::eval_lower_function_with_collation(values, expr_collation(collation))
        .map_err(map_expr_error)
}

forward_values!(pub(super) eval_upper_function);

pub(super) fn eval_upper_function_with_collation(
    values: &[Value],
    collation: TextCollationSemantics,
) -> Result<Value, ExecError> {
    expr_string::eval_upper_function_with_collation(values, expr_collation(collation))
        .map_err(map_expr_error)
}

forward_values!(pub(super) eval_text_starts_with_function);
forward_values!(pub(super) eval_unistr_function);
forward_values!(pub(super) eval_unicode_version_function);
forward_values!(pub(super) eval_unicode_assigned_function);
forward_values!(pub(super) eval_unicode_normalize_function);
forward_values!(pub(super) eval_unicode_is_normalized_function);
forward_values!(pub(super) eval_initcap_function);

pub(super) fn eval_initcap_function_with_collation(
    values: &[Value],
    collation: TextCollationSemantics,
) -> Result<Value, ExecError> {
    expr_string::eval_initcap_function_with_collation(values, expr_collation(collation))
        .map_err(map_expr_error)
}

pub(super) fn eval_casefold_function_with_collation(
    values: &[Value],
    collation: TextCollationSemantics,
) -> Result<Value, ExecError> {
    expr_string::eval_casefold_function_with_collation(values, expr_collation(collation))
        .map_err(map_expr_error)
}

forward_values!(pub(super) eval_replace_function);
forward_values!(pub(super) eval_split_part_function);
forward_values!(pub(super) eval_lpad_function);
forward_values!(pub(super) eval_rpad_function);
forward_values!(pub(super) eval_translate_function);
forward_values!(pub(super) eval_ascii_function);
forward_values!(pub(super) eval_chr_function);

pub(super) fn eval_trim_function(op: &'static str, values: &[Value]) -> Result<Value, ExecError> {
    expr_string::eval_trim_function(op, values).map_err(map_expr_error)
}

forward_values!(pub(super) eval_text_substring);
forward_values!(pub(super) eval_bytea_substring);

pub(super) fn eval_like(
    left: &Value,
    pattern: &Value,
    escape: Option<&Value>,
    collation_oid: Option<u32>,
    case_insensitive: bool,
    negated: bool,
) -> Result<Value, ExecError> {
    expr_string::eval_like(
        left,
        pattern,
        escape,
        collation_oid,
        case_insensitive,
        negated,
    )
    .map_err(map_expr_error)
}

forward_values!(pub(super) eval_md5_function);
forward_values!(pub(super) eval_reverse_function);
forward_values!(pub(super) eval_quote_literal_function);
forward_values!(pub(super) eval_quote_nullable_function);
forward_values!(pub(super) eval_quote_ident_function);
forward_values!(pub(crate) eval_parse_ident_function);
forward_values!(pub(super) eval_encode_function);
forward_values!(pub(super) eval_decode_function);
forward_values!(pub(super) eval_sha224_function);
forward_values!(pub(super) eval_sha256_function);
forward_values!(pub(super) eval_sha384_function);
forward_values!(pub(super) eval_sha512_function);
forward_values!(pub(super) eval_crc32_function);
forward_values!(pub(super) eval_crc32c_function);
forward_values!(pub(super) eval_bpchar_to_text_function);
forward_values!(pub(super) eval_position_function);
forward_values!(pub(super) eval_strpos_function);
forward_values!(pub(super) eval_bytea_position_function);
forward_values!(pub(super) eval_bytea_overlay);
forward_values!(pub(super) eval_text_overlay);
forward_values!(pub(super) eval_get_bit_bytes);
forward_values!(pub(super) eval_set_bit_bytes);
forward_values!(pub(super) eval_bit_count_bytes);
forward_values!(pub(super) eval_get_byte);
forward_values!(pub(super) eval_set_byte);
forward_values!(pub(super) eval_convert_from_function);
forward_values!(pub(super) eval_convert_to_function);
forward_values!(pub(super) eval_pg_rust_test_enc_setup);
forward_values!(pub(super) eval_pg_rust_test_opclass_options_func);
forward_values!(pub(super) eval_pg_rust_test_fdw_handler);
forward_values!(pub(super) eval_pg_rust_is_catalog_text_unique_index_oid);
forward_values!(pub(super) eval_pg_rust_test_widget_in);
forward_values!(pub(super) eval_pg_rust_test_widget_out);
forward_values!(pub(super) eval_pg_rust_test_int44in);
forward_values!(pub(super) eval_pg_rust_test_int44out);
forward_values!(pub(super) eval_pg_rust_test_pt_in_widget);
forward_values!(pub(super) eval_pg_rust_test_enc_conversion);
