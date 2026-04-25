use num_bigint::{BigInt, Sign};
use num_traits::ToPrimitive;

use crate::backend::utils::time::system_time::{SystemTime, UNIX_EPOCH};
use crate::backend::utils::trigger::format_trigger_definition;
use crate::include::nodes::datetime::MAX_TIME_PRECISION;
use crate::include::nodes::primnodes::expr_sql_type_hint;
use rand::{Rng, RngCore};
use std::sync::Mutex;

use super::expr_agg_support::{
    execute_builtin_scalar_function_value_call, execute_scalar_function_value_call,
};
use super::expr_async::{eval_pg_notification_queue_usage_function, eval_pg_notify_function};
use super::expr_bit::{
    bit_count as eval_bit_count, bit_length as eval_bit_length, get_bit as eval_get_bit,
    overlay as eval_bit_overlay, position as eval_bit_position, set_bit as eval_set_bit,
    substring as eval_bit_substring,
};
use super::expr_bool::{eval_booland_statefunc, eval_booleq, eval_boolne, eval_boolor_statefunc};
use super::expr_casts::{
    cast_value, cast_value_with_config, cast_value_with_source_type_and_config,
    cast_value_with_source_type_catalog_and_config, soft_input_error_info_with_catalog_and_config,
};
pub(crate) use super::expr_compile::{
    CompiledPredicate, compile_predicate, compile_predicate_with_decoder,
};
use super::expr_date::{
    eval_date_part_function, eval_date_trunc_function, eval_isfinite_function,
    eval_make_date_function, eval_make_time_function, eval_timezone_function,
    eval_to_date_function,
};
use super::expr_datetime::{
    current_date_value, current_date_value_from_timestamp_with_config, current_time_value,
    current_time_value_from_timestamp_with_config, current_timestamp_value,
    current_timestamp_value_from_timestamp_with_config, current_timestamp_value_with_config,
    render_datetime_value_text_with_config,
};
use super::expr_geometry::eval_geometry_function;
use super::expr_json::{
    eval_json_builtin_function, eval_json_get, eval_json_path, eval_json_record_builtin_function,
    eval_jsonpath_operator, jsonb_to_tsvector_value,
};
use super::expr_locks::eval_advisory_lock_builtin_function;
use super::expr_math::{
    cosd, cotd, eval_abs_function, eval_acosd, eval_acosh, eval_asind, eval_atanh,
    eval_binary_float_function, eval_bitcast_bigint_to_float8, eval_bitcast_integer_to_float4,
    eval_erf, eval_erfc, eval_float_send_function, eval_gamma, eval_gcd_function,
    eval_lcm_function, eval_lgamma, eval_unary_float_function, sind, snap_degree, tand,
};
use super::expr_money::{cash_words_text, money_larger, money_smaller};
use super::expr_multirange::eval_multirange_function;
use super::expr_numeric::{
    eval_ceil_function, eval_div_function, eval_exp_function, eval_factorial_function,
    eval_floor_function, eval_ln_function, eval_log_function, eval_log10_function,
    eval_min_scale_function, eval_numeric_inc_function, eval_pg_lsn_function, eval_power_function,
    eval_round_function, eval_scale_function, eval_sign_function, eval_sqrt_function,
    eval_trim_scale_function, eval_trunc_function, eval_width_bucket_function,
};
use super::expr_ops::compare_order_values;
use super::expr_ops::{
    add_values, bitwise_and_values, bitwise_not_value, bitwise_or_values, bitwise_xor_values,
    compare_values, compare_values_with_type, concat_values, div_values, eval_and, eval_or,
    mod_values, mul_values, negate_value, not_equal_values, not_equal_values_with_type,
    order_values, shift_left_values, shift_right_values, sub_values, values_are_distinct,
};
pub(crate) use super::expr_ops::{compare_order_by_keys, parse_numeric_text};
use super::expr_range::eval_range_function;
use super::expr_reg;
use super::expr_string::{
    eval_ascii_function, eval_bit_count_bytes, eval_bpchar_to_text_function, eval_bytea_overlay,
    eval_bytea_position_function, eval_bytea_substring, eval_chr_function, eval_concat_function,
    eval_concat_ws_function, eval_convert_from_function, eval_crc32_function, eval_crc32c_function,
    eval_decode_function, eval_encode_function, eval_format_function, eval_get_bit_bytes,
    eval_get_byte, eval_initcap_function, eval_left_function, eval_length_function, eval_like,
    eval_lower_function, eval_lpad_function, eval_md5_function, eval_parse_ident_function,
    eval_pg_rust_test_enc_conversion, eval_pg_rust_test_enc_setup, eval_pg_rust_test_fdw_handler,
    eval_pg_rust_test_opclass_options_func, eval_pg_size_bytes_function,
    eval_pg_size_pretty_function, eval_position_function, eval_quote_literal_function,
    eval_repeat_function, eval_replace_function, eval_reverse_function, eval_right_function,
    eval_rpad_function, eval_set_bit_bytes, eval_set_byte, eval_sha224_function,
    eval_sha256_function, eval_sha384_function, eval_sha512_function, eval_split_part_function,
    eval_strpos_function, eval_text_overlay, eval_text_substring, eval_to_bin_function,
    eval_to_char_float4_function, eval_to_char_function, eval_to_hex_function,
    eval_to_number_function, eval_to_oct_function, eval_translate_function, eval_trim_function,
    eval_unistr_function,
};
use super::expr_txid::eval_txid_builtin_function;
use super::expr_xml::{eval_xml_comment_function, eval_xml_expr, eval_xml_is_well_formed_function};
use super::node_types::*;
use super::pg_regex::{
    eval_regex_match_operator, eval_regexp_count, eval_regexp_instr, eval_regexp_like,
    eval_regexp_match, eval_regexp_replace, eval_regexp_split_to_array, eval_regexp_substr,
    eval_similar, eval_similar_substring, eval_sql_regex_substring,
};
pub(crate) use super::value_io::{format_array_text, format_array_value_text};
use super::{ExecError, ExecutorContext, exec_next, executor_start};
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::catalog::indexing::probe_system_catalog_rows_visible_in_db;
use crate::backend::catalog::rowcodec::pg_description_row_from_values;
use crate::backend::executor::jsonb::{
    JsonbValue, jsonb_contains, jsonb_exists, jsonb_exists_all, jsonb_exists_any, jsonb_from_value,
};
use crate::backend::executor::sqlfunc::execute_user_defined_sql_scalar_function;
use crate::backend::parser::analyze::is_binary_coercible_type;
use crate::backend::parser::{
    CatalogLookup, ParseError, SqlType, SqlTypeKind, SubqueryComparisonOp,
};
use crate::backend::rewrite::format_view_definition;
use crate::backend::statistics::{
    render_pg_dependencies_text, render_pg_mcv_list_text, render_pg_ndistinct_text,
};
use crate::backend::utils::misc::checkpoint::checkpoint_stats_value;
use crate::backend::utils::misc::guc::normalize_guc_name;
use crate::backend::utils::misc::guc::plpgsql_guc_default_value;
use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;
use crate::include::access::toast_compression::ToastCompressionId;
use crate::include::catalog::{
    BOX_SPGIST_OPCLASS_OID, BRIN_AM_OID, BTREE_AM_OID, BYTEA_TYPE_OID, CONSTRAINT_CHECK,
    CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE,
    CURRENT_DATABASE_OID, FLOAT8_TYPE_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID,
    PG_CATALOG_NAMESPACE_OID, PG_CLASS_RELATION_OID, PG_DATABASE_RELATION_OID,
    PG_DEPENDENCIES_TYPE_OID, PG_FOREIGN_DATA_WRAPPER_RELATION_OID, PG_MCV_LIST_TYPE_OID,
    PG_NDISTINCT_TYPE_OID, PG_STATISTIC_EXT_RELATION_OID, PG_TOAST_NAMESPACE_OID,
    POLY_SPGIST_OPCLASS_OID, SPGIST_AM_OID, bootstrap_pg_am_rows,
    builtin_scalar_function_for_proc_oid, builtin_type_name_for_oid,
};
use crate::include::nodes::datum::{ArrayDimension, ArrayValue, NumericValue};
use crate::include::nodes::primnodes::{
    BoolExpr, BoolExprType, FuncExpr, INDEX_VAR, INNER_VAR, OUTER_VAR, OpExpr, OpExprKind,
    SELF_ITEM_POINTER_ATTR_NO, ScalarArrayOpExpr, ScalarFunctionImpl, SubLinkType,
    TABLE_OID_ATTR_NO, attrno_index, is_executor_special_varno,
};
use crate::pgrust::compact_string::CompactString;
use crate::pl::plpgsql::execute_user_defined_scalar_function;

mod arrays;
mod subquery;

pub(crate) use arrays::{append_array_value, concatenate_arrays, eval_string_to_table_rows};
use arrays::{
    eval_array_append_function, eval_array_cat_function, eval_array_contained, eval_array_contains,
    eval_array_dims_function, eval_array_fill_function, eval_array_length_function,
    eval_array_lower_function, eval_array_ndims_function, eval_array_overlap,
    eval_array_position_function, eval_array_positions_function, eval_array_prepend_function,
    eval_array_remove_function, eval_array_replace_function, eval_array_sort_function,
    eval_array_subscript, eval_array_subscript_plpgsql, eval_array_to_string_function,
    eval_array_upper_function, eval_cardinality_function, eval_quantified_array,
    eval_string_to_array_function, eval_width_bucket_thresholds,
};
use subquery::{
    eval_array_subquery, eval_exists_subquery, eval_quantified_subquery, eval_scalar_subquery,
};

extern crate rand;

const INVALID_PARAMETER_VALUE_SQLSTATE: &str = "22023";
const PG_DESCRIPTION_O_C_O_INDEX_OID: u32 = 2675;

fn malformed_expr_error(kind: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("malformed {kind} expression").into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

fn top_level_explicit_collation(expr: &Expr) -> Option<u32> {
    match expr {
        Expr::Collate { collation_oid, .. } => Some(*collation_oid),
        _ => None,
    }
}

fn sql_type_from_builtin_oid(oid: u32) -> Option<SqlType> {
    crate::include::catalog::builtin_type_rows()
        .iter()
        .find(|row| row.oid == oid && row.typrelid == 0)
        .map(|row| row.sql_type)
}

fn stats_oid_arg(values: &[Value], op: &'static str) -> Result<u32, ExecError> {
    match values.first() {
        Some(Value::Int32(v)) if *v >= 0 => Ok(*v as u32),
        Some(Value::Int64(v)) if *v >= 0 && *v <= i64::from(u32::MAX) => Ok(*v as u32),
        Some(other) => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int64(i64::from(crate::include::catalog::OID_TYPE_OID)),
        }),
        None => Err(malformed_expr_error(op)),
    }
}

fn relation_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let entry = ctx
        .session_stats
        .write()
        .visible_relation_entry(&ctx.stats, oid)
        .unwrap_or_default();
    Ok(match func {
        BuiltinScalarFunction::PgStatGetNumscans => Value::Int64(entry.numscans),
        BuiltinScalarFunction::PgStatGetLastscan => entry
            .lastscan
            .map(Value::TimestampTz)
            .unwrap_or(Value::Null),
        BuiltinScalarFunction::PgStatGetTuplesReturned => Value::Int64(entry.tuples_returned),
        BuiltinScalarFunction::PgStatGetTuplesFetched => Value::Int64(entry.tuples_fetched),
        BuiltinScalarFunction::PgStatGetTuplesInserted => Value::Int64(entry.tuples_inserted),
        BuiltinScalarFunction::PgStatGetTuplesUpdated => Value::Int64(entry.tuples_updated),
        BuiltinScalarFunction::PgStatGetTuplesDeleted => Value::Int64(entry.tuples_deleted),
        BuiltinScalarFunction::PgStatGetLiveTuples => Value::Int64(entry.live_tuples),
        BuiltinScalarFunction::PgStatGetDeadTuples => Value::Int64(entry.dead_tuples),
        BuiltinScalarFunction::PgStatGetBlocksFetched => Value::Int64(entry.blocks_fetched),
        BuiltinScalarFunction::PgStatGetBlocksHit => Value::Int64(entry.blocks_hit),
        _ => unreachable!("non-relation stats builtin in relation_stats_value"),
    })
}

fn relation_xact_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let session = ctx.session_stats.read();
    if session.dropped_relations_in_xact.contains(&oid) {
        return Ok(Value::Int64(0));
    }
    let current = session
        .relation_xact
        .get(&oid)
        .map(|entry| &entry.current)
        .cloned()
        .unwrap_or_default();
    Ok(match func {
        BuiltinScalarFunction::PgStatGetXactNumscans => Value::Int64(current.numscans),
        BuiltinScalarFunction::PgStatGetXactTuplesReturned => Value::Int64(current.tuples_returned),
        BuiltinScalarFunction::PgStatGetXactTuplesFetched => Value::Int64(current.tuples_fetched),
        BuiltinScalarFunction::PgStatGetXactTuplesInserted => Value::Int64(current.tuples_inserted),
        BuiltinScalarFunction::PgStatGetXactTuplesUpdated => Value::Int64(current.tuples_updated),
        BuiltinScalarFunction::PgStatGetXactTuplesDeleted => Value::Int64(current.tuples_deleted),
        _ => unreachable!("non-xact relation stats builtin in relation_xact_stats_value"),
    })
}

fn function_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let Some(entry) = ctx
        .session_stats
        .write()
        .visible_function_entry(&ctx.stats, oid)
    else {
        return Ok(Value::Null);
    };
    Ok(match func {
        BuiltinScalarFunction::PgStatGetFunctionCalls => Value::Int64(entry.calls),
        BuiltinScalarFunction::PgStatGetFunctionTotalTime => {
            Value::Float64(entry.total_time_micros as f64 / 1000.0)
        }
        BuiltinScalarFunction::PgStatGetFunctionSelfTime => {
            Value::Float64(entry.self_time_micros as f64 / 1000.0)
        }
        _ => unreachable!("non-function stats builtin in function_stats_value"),
    })
}

fn function_xact_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let session = ctx.session_stats.read();
    if session.dropped_functions_in_xact.contains(&oid) {
        return Ok(Value::Null);
    }
    let Some(entry) = session.function_xact.get(&oid) else {
        return Ok(Value::Null);
    };
    Ok(match func {
        BuiltinScalarFunction::PgStatGetXactFunctionCalls => Value::Int64(entry.calls),
        BuiltinScalarFunction::PgStatGetXactFunctionTotalTime => {
            Value::Float64(entry.total_time_micros as f64 / 1000.0)
        }
        BuiltinScalarFunction::PgStatGetXactFunctionSelfTime => {
            Value::Float64(entry.self_time_micros as f64 / 1000.0)
        }
        _ => unreachable!("non-xact function stats builtin in function_xact_stats_value"),
    })
}

fn oid_arg_to_u32(value: &Value, op: &'static str) -> Result<u32, ExecError> {
    match value {
        Value::Int32(oid) => u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange),
        Value::Int64(oid) => u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange),
        _ if value.as_text().is_some() => value
            .as_text()
            .expect("guarded above")
            .trim()
            .parse::<u32>()
            .map_err(|_| ExecError::TypeMismatch {
                op,
                left: value.clone(),
                right: Value::Int64(i64::from(crate::include::catalog::OID_TYPE_OID)),
            }),
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(i64::from(crate::include::catalog::OID_TYPE_OID)),
        }),
    }
}

fn int32_arg(value: &Value, op: &'static str) -> Result<i32, ExecError> {
    match value {
        Value::Int16(v) => Ok(i32::from(*v)),
        Value::Int32(v) => Ok(*v),
        Value::Int64(v) => i32::try_from(*v).map_err(|_| ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int32(0),
        }),
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int32(0),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexPropertyKind {
    Asc,
    Desc,
    NullsFirst,
    NullsLast,
    Orderable,
    DistanceOrderable,
    Returnable,
    SearchArray,
    SearchNulls,
    Clusterable,
    IndexScan,
    BitmapScan,
    BackwardScan,
    CanOrder,
    CanUnique,
    CanMultiCol,
    CanExclude,
    CanInclude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexReturnability {
    Never,
    Always,
    SpgistBox,
}

#[derive(Debug, Clone, Copy)]
struct IndexAmPropertyProfile {
    amcanorder: bool,
    amcanorderbyop: bool,
    amcanbackward: bool,
    amcanunique: bool,
    amcanmulticol: bool,
    amsearcharray: bool,
    amsearchnulls: bool,
    amclusterable: bool,
    amcanexclude: bool,
    amcaninclude: bool,
    amindexscan: bool,
    ambitmapscan: bool,
    returnability: IndexReturnability,
}

const INDOPTION_DESC: i16 = 0x0001;
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

fn parse_index_property(name: &str) -> Option<IndexPropertyKind> {
    match name.to_ascii_lowercase().as_str() {
        "asc" => Some(IndexPropertyKind::Asc),
        "desc" => Some(IndexPropertyKind::Desc),
        "nulls_first" => Some(IndexPropertyKind::NullsFirst),
        "nulls_last" => Some(IndexPropertyKind::NullsLast),
        "orderable" => Some(IndexPropertyKind::Orderable),
        "distance_orderable" => Some(IndexPropertyKind::DistanceOrderable),
        "returnable" => Some(IndexPropertyKind::Returnable),
        "search_array" => Some(IndexPropertyKind::SearchArray),
        "search_nulls" => Some(IndexPropertyKind::SearchNulls),
        "clusterable" => Some(IndexPropertyKind::Clusterable),
        "index_scan" => Some(IndexPropertyKind::IndexScan),
        "bitmap_scan" => Some(IndexPropertyKind::BitmapScan),
        "backward_scan" => Some(IndexPropertyKind::BackwardScan),
        "can_order" => Some(IndexPropertyKind::CanOrder),
        "can_unique" => Some(IndexPropertyKind::CanUnique),
        "can_multi_col" => Some(IndexPropertyKind::CanMultiCol),
        "can_exclude" => Some(IndexPropertyKind::CanExclude),
        "can_include" => Some(IndexPropertyKind::CanInclude),
        _ => None,
    }
}

fn index_am_profile(am_oid: u32) -> Option<IndexAmPropertyProfile> {
    match am_oid {
        BTREE_AM_OID => Some(IndexAmPropertyProfile {
            amcanorder: true,
            amcanorderbyop: false,
            amcanbackward: true,
            amcanunique: true,
            amcanmulticol: true,
            amsearcharray: true,
            amsearchnulls: true,
            amclusterable: true,
            amcanexclude: true,
            amcaninclude: true,
            amindexscan: true,
            ambitmapscan: true,
            returnability: IndexReturnability::Always,
        }),
        HASH_AM_OID => Some(IndexAmPropertyProfile {
            amcanorder: false,
            amcanorderbyop: false,
            amcanbackward: true,
            amcanunique: false,
            amcanmulticol: false,
            amsearcharray: false,
            amsearchnulls: false,
            amclusterable: false,
            amcanexclude: true,
            amcaninclude: false,
            amindexscan: true,
            ambitmapscan: true,
            returnability: IndexReturnability::Never,
        }),
        GIST_AM_OID => Some(IndexAmPropertyProfile {
            amcanorder: false,
            amcanorderbyop: true,
            amcanbackward: false,
            amcanunique: false,
            amcanmulticol: true,
            amsearcharray: false,
            amsearchnulls: true,
            amclusterable: true,
            amcanexclude: true,
            amcaninclude: true,
            amindexscan: true,
            ambitmapscan: true,
            returnability: IndexReturnability::Never,
        }),
        GIN_AM_OID => Some(IndexAmPropertyProfile {
            amcanorder: false,
            amcanorderbyop: false,
            amcanbackward: false,
            amcanunique: false,
            amcanmulticol: true,
            amsearcharray: false,
            amsearchnulls: false,
            amclusterable: false,
            amcanexclude: false,
            amcaninclude: false,
            amindexscan: false,
            ambitmapscan: true,
            returnability: IndexReturnability::Never,
        }),
        BRIN_AM_OID => Some(IndexAmPropertyProfile {
            amcanorder: false,
            amcanorderbyop: false,
            amcanbackward: false,
            amcanunique: false,
            amcanmulticol: true,
            amsearcharray: false,
            amsearchnulls: true,
            amclusterable: false,
            amcanexclude: false,
            amcaninclude: false,
            amindexscan: false,
            ambitmapscan: true,
            returnability: IndexReturnability::Never,
        }),
        SPGIST_AM_OID => Some(IndexAmPropertyProfile {
            amcanorder: false,
            amcanorderbyop: true,
            amcanbackward: false,
            amcanunique: false,
            amcanmulticol: false,
            amsearcharray: false,
            amsearchnulls: true,
            amclusterable: false,
            amcanexclude: true,
            amcaninclude: true,
            amindexscan: true,
            ambitmapscan: true,
            returnability: IndexReturnability::SpgistBox,
        }),
        _ => crate::backend::access::index::amapi::index_am_handler(am_oid).map(|routine| {
            IndexAmPropertyProfile {
                amcanorder: routine.amcanorder,
                amcanorderbyop: routine.amcanorderbyop,
                amcanbackward: routine.amcanbackward,
                amcanunique: routine.amcanunique,
                amcanmulticol: routine.amcanmulticol,
                amsearcharray: routine.amsearcharray,
                amsearchnulls: routine.amsearchnulls,
                amclusterable: routine.amclusterable,
                amcanexclude: routine.amgettuple.is_some(),
                amcaninclude: false,
                amindexscan: routine.amgettuple.is_some(),
                ambitmapscan: false,
                returnability: IndexReturnability::Never,
            }
        }),
    }
}

fn index_column_has_ordering_operator(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    column_index: usize,
) -> bool {
    index_meta
        .amop_entries
        .get(column_index)
        .is_some_and(|entries| entries.iter().any(|entry| entry.purpose == 'o'))
}

fn eval_pg_indexam_has_property(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [am_oid, property] => {
            let am_oid = oid_arg_to_u32(am_oid, "pg_indexam_has_property")?;
            let Some(property) = property.as_text() else {
                return Err(ExecError::TypeMismatch {
                    op: "pg_indexam_has_property",
                    left: property.clone(),
                    right: Value::Text("".into()),
                });
            };
            let Some(profile) = index_am_profile(am_oid) else {
                return Ok(Value::Null);
            };
            Ok(match parse_index_property(property) {
                Some(IndexPropertyKind::CanOrder) => Value::Bool(profile.amcanorder),
                Some(IndexPropertyKind::CanUnique) => Value::Bool(profile.amcanunique),
                Some(IndexPropertyKind::CanMultiCol) => Value::Bool(profile.amcanmulticol),
                Some(IndexPropertyKind::CanExclude) => Value::Bool(profile.amcanexclude),
                Some(IndexPropertyKind::CanInclude) => Value::Bool(profile.amcaninclude),
                _ => Value::Null,
            })
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_indexam_has_property(oid, text)",
            actual: format!("PgIndexAmHasProperty({} args)", values.len()),
        })),
    }
}

fn eval_pg_index_has_property(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [relation_oid, property] => {
            let relation_oid = oid_arg_to_u32(relation_oid, "pg_index_has_property")?;
            let Some(property) = property.as_text() else {
                return Err(ExecError::TypeMismatch {
                    op: "pg_index_has_property",
                    left: property.clone(),
                    right: Value::Text("".into()),
                });
            };
            let catalog = executor_catalog(ctx)?;
            let Some(entry) = catalog.relcache().get_by_oid(relation_oid) else {
                return Ok(Value::Null);
            };
            let Some(index_meta) = entry.index.as_ref() else {
                return Ok(Value::Null);
            };
            let Some(profile) = index_am_profile(index_meta.am_oid) else {
                return Ok(Value::Null);
            };
            Ok(match parse_index_property(property) {
                Some(IndexPropertyKind::Clusterable) => Value::Bool(profile.amclusterable),
                Some(IndexPropertyKind::IndexScan) => Value::Bool(profile.amindexscan),
                Some(IndexPropertyKind::BitmapScan) => Value::Bool(profile.ambitmapscan),
                Some(IndexPropertyKind::BackwardScan) => Value::Bool(profile.amcanbackward),
                _ => Value::Null,
            })
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_index_has_property(regclass, text)",
            actual: format!("PgIndexHasProperty({} args)", values.len()),
        })),
    }
}

fn eval_pg_index_column_has_property(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [relation_oid, attno, property] => {
            let relation_oid = oid_arg_to_u32(relation_oid, "pg_index_column_has_property")?;
            let attno = int32_arg(attno, "pg_index_column_has_property")?;
            if attno <= 0 {
                return Ok(Value::Null);
            }
            let Some(property) = property.as_text() else {
                return Err(ExecError::TypeMismatch {
                    op: "pg_index_column_has_property",
                    left: property.clone(),
                    right: Value::Text("".into()),
                });
            };
            let catalog = executor_catalog(ctx)?;
            let Some(entry) = catalog.relcache().get_by_oid(relation_oid) else {
                return Ok(Value::Null);
            };
            let Some(index_meta) = entry.index.as_ref() else {
                return Ok(Value::Null);
            };
            let Some(profile) = index_am_profile(index_meta.am_oid) else {
                return Ok(Value::Null);
            };
            let column_index = (attno - 1) as usize;
            if column_index >= usize::try_from(index_meta.indnatts).unwrap_or_default() {
                return Ok(Value::Null);
            }
            let is_key = column_index < usize::try_from(index_meta.indnkeyatts).unwrap_or_default();
            let indoption = index_meta
                .indoption
                .get(column_index)
                .copied()
                .unwrap_or_default();
            Ok(match parse_index_property(property) {
                Some(IndexPropertyKind::Asc) => {
                    if !is_key {
                        Value::Null
                    } else {
                        Value::Bool(profile.amcanorder && (indoption & INDOPTION_DESC) == 0)
                    }
                }
                Some(IndexPropertyKind::Desc) => {
                    if !is_key {
                        Value::Null
                    } else {
                        Value::Bool(profile.amcanorder && (indoption & INDOPTION_DESC) != 0)
                    }
                }
                Some(IndexPropertyKind::NullsFirst) => {
                    if !is_key {
                        Value::Null
                    } else {
                        Value::Bool(profile.amcanorder && (indoption & INDOPTION_NULLS_FIRST) != 0)
                    }
                }
                Some(IndexPropertyKind::NullsLast) => {
                    if !is_key {
                        Value::Null
                    } else {
                        Value::Bool(profile.amcanorder && (indoption & INDOPTION_NULLS_FIRST) == 0)
                    }
                }
                Some(IndexPropertyKind::Orderable) => Value::Bool(is_key && profile.amcanorder),
                Some(IndexPropertyKind::DistanceOrderable) => Value::Bool(
                    is_key
                        && profile.amcanorderbyop
                        && index_column_has_ordering_operator(index_meta, column_index),
                ),
                Some(IndexPropertyKind::Returnable) => Value::Bool(match profile.returnability {
                    IndexReturnability::Never => false,
                    IndexReturnability::Always => true,
                    IndexReturnability::SpgistBox => {
                        matches!(
                            index_meta.indclass.get(column_index).copied(),
                            Some(BOX_SPGIST_OPCLASS_OID | POLY_SPGIST_OPCLASS_OID)
                        )
                    }
                }),
                Some(IndexPropertyKind::SearchArray) => {
                    if is_key {
                        Value::Bool(profile.amsearcharray)
                    } else {
                        Value::Null
                    }
                }
                Some(IndexPropertyKind::SearchNulls) => {
                    if is_key {
                        Value::Bool(profile.amsearchnulls)
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            })
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_index_column_has_property(regclass, int4, text)",
            actual: format!("PgIndexColumnHasProperty({} args)", values.len()),
        })),
    }
}

fn regproc_type_name(sql_type: SqlType) -> &'static str {
    match sql_type.element_type().kind {
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Internal => "internal",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegProc => "regproc",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegNamespace => "regnamespace",
        SqlTypeKind::RegOper => "regoper",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegCollation => "regcollation",
        SqlTypeKind::Text => "text",
        SqlTypeKind::FdwHandler => "fdw_handler",
        _ => "text",
    }
}

fn catalog_lookup(ctx: Option<&ExecutorContext>) -> Option<&dyn CatalogLookup> {
    ctx.and_then(|ctx| {
        ctx.catalog
            .as_ref()
            .map(|catalog| catalog as &dyn CatalogLookup)
    })
}

fn eval_reg_object_to_text(
    value: &Value,
    kind: SqlTypeKind,
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let oid = oid_arg_to_u32(value, "::text")?;
    let catalog = catalog_lookup(ctx);
    let text = match kind {
        SqlTypeKind::RegProc => expr_reg::format_regproc_oid_optional(oid, catalog),
        SqlTypeKind::RegProcedure => expr_reg::format_regprocedure_oid_optional(oid, catalog),
        SqlTypeKind::RegOper => expr_reg::format_regoper_oid_optional(oid, catalog),
        SqlTypeKind::RegOperator => expr_reg::format_regoperator_oid_optional(oid, catalog),
        SqlTypeKind::RegCollation => expr_reg::format_regcollation_oid_optional(oid, catalog),
        _ => None,
    }
    .unwrap_or_else(|| oid.to_string());
    Ok(Value::Text(text.into()))
}

fn eval_regprocedure_to_text(value: &Value, ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let oid = oid_arg_to_u32(value, "regprocedure_to_text")?;
    let Some(proc_row) = role_catalog(ctx)?.proc_row_by_oid(oid) else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(
        function_signature_text(&proc_row, role_catalog(ctx)?).into(),
    ))
}

fn sql_type_identity_text(sql_type: SqlType) -> String {
    let mut name = regproc_type_name(sql_type).to_string();
    if sql_type.is_array {
        name.push_str("[]");
    }
    name
}

fn type_identity_text(catalog: &dyn CatalogLookup, type_oid: u32) -> String {
    catalog
        .type_by_oid(type_oid)
        .map(|row| {
            if !row.sql_type.is_array
                && row.sql_type.type_oid == row.oid
                && row.sql_type.kind == SqlTypeKind::Bytea
                && row.oid != BYTEA_TYPE_OID
            {
                row.typname
            } else {
                sql_type_identity_text(row.sql_type)
            }
        })
        .unwrap_or_else(|| type_oid.to_string())
}

fn function_signature_text(
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> String {
    let arg_types = proc_row
        .proargtypes
        .split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .map(|oid| type_identity_text(catalog, oid))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}({arg_types})", proc_row.proname)
}

fn function_identity_text(
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> String {
    format!("function {}", function_signature_text(proc_row, catalog))
}

fn operator_identity_text(
    operator_row: &crate::include::catalog::PgOperatorRow,
    catalog: &dyn CatalogLookup,
) -> String {
    let left = if operator_row.oprleft == 0 {
        "none".to_string()
    } else {
        type_identity_text(catalog, operator_row.oprleft)
    };
    let right = if operator_row.oprright == 0 {
        "none".to_string()
    } else {
        type_identity_text(catalog, operator_row.oprright)
    };
    format!("operator {}({left},{right})", operator_row.oprname)
}

fn quote_identifier(identifier: &str) -> String {
    if !identifier.is_empty()
        && identifier.chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_lowercase()
            } else {
                ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()
            }
        })
    {
        return identifier.into();
    }
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_qualified_identifier(schema_name: &str, object_name: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(object_name)
    )
}

fn looks_like_function_call(expr: &str) -> bool {
    let trimmed = expr.trim();
    let Some(open_paren) = trimmed.find('(') else {
        return false;
    };
    trimmed.ends_with(')')
        && trimmed[..open_paren].chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_alphabetic()
            } else {
                ch == '_' || ch.is_ascii_alphanumeric()
            }
        })
}

fn statistics_expression_texts(
    statistics: &crate::include::catalog::PgStatisticExtRow,
) -> Vec<String> {
    statistics
        .stxexprs
        .as_deref()
        .and_then(|text| serde_json::from_str::<Vec<String>>(text).ok())
        .unwrap_or_default()
}

fn statistics_relation_display_name(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Option<String> {
    let relation = catalog.relation_by_oid(relation_oid)?;
    let class_row = catalog.class_row_by_oid(relation_oid)?;
    if catalog
        .lookup_any_relation(&class_row.relname)
        .is_some_and(|entry| entry.relation_oid == relation_oid)
    {
        return Some(quote_identifier(&class_row.relname));
    }
    let namespace_name = catalog
        .namespace_row_by_oid(relation.namespace_oid)?
        .nspname;
    Some(quote_qualified_identifier(
        &namespace_name,
        &class_row.relname,
    ))
}

fn statistics_columns_text(
    statistics: &crate::include::catalog::PgStatisticExtRow,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    let relation = catalog.relation_by_oid(statistics.stxrelid)?;
    let mut items = Vec::new();
    for attnum in &statistics.stxkeys {
        let index = usize::try_from(attnum.saturating_sub(1)).ok()?;
        let column = relation.desc.columns.get(index)?;
        items.push(quote_identifier(&column.name));
    }
    for expr in statistics_expression_texts(statistics) {
        if looks_like_function_call(&expr) {
            items.push(expr);
        } else {
            items.push(format!("({expr})"));
        }
    }
    Some(items.join(", "))
}

fn statistics_enabled_kinds(
    statistics: &crate::include::catalog::PgStatisticExtRow,
) -> (bool, bool, bool) {
    let mut ndistinct = false;
    let mut dependencies = false;
    let mut mcv = false;
    for kind in &statistics.stxkind {
        match *kind {
            b'd' => ndistinct = true,
            b'f' => dependencies = true,
            b'm' => mcv = true,
            _ => {}
        }
    }
    (ndistinct, dependencies, mcv)
}

fn statistics_definition_text(
    statistics: &crate::include::catalog::PgStatisticExtRow,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    let namespace_name = catalog
        .namespace_row_by_oid(statistics.stxnamespace)?
        .nspname;
    let columns = statistics_columns_text(statistics, catalog)?;
    let relation_name = statistics_relation_display_name(catalog, statistics.stxrelid)?;
    let expr_count = statistics_expression_texts(statistics).len();
    let ncolumns = statistics.stxkeys.len() + expr_count;
    let (ndistinct, dependencies, mcv) = statistics_enabled_kinds(statistics);
    let mut out = format!(
        "CREATE STATISTICS {}",
        quote_qualified_identifier(&namespace_name, &statistics.stxname)
    );
    if ncolumns > 1 && (!ndistinct || !dependencies || !mcv) {
        let mut kinds = Vec::new();
        if ndistinct {
            kinds.push("ndistinct");
        }
        if dependencies {
            kinds.push("dependencies");
        }
        if mcv {
            kinds.push("mcv");
        }
        out.push_str(&format!(" ({})", kinds.join(", ")));
    }
    out.push_str(" ON ");
    out.push_str(&columns);
    out.push_str(" FROM ");
    out.push_str(&relation_name);
    Some(out)
}

fn statistics_visible_in_search_path(
    statistics: &crate::include::catalog::PgStatisticExtRow,
    catalog: &dyn CatalogLookup,
) -> bool {
    let is_temp_schema = |schema_name: &str| {
        schema_name.eq_ignore_ascii_case("pg_temp")
            || schema_name.to_ascii_lowercase().starts_with("pg_temp_")
    };
    if catalog
        .namespace_row_by_oid(statistics.stxnamespace)
        .is_some_and(|namespace| is_temp_schema(&namespace.nspname))
    {
        return false;
    }
    let search_path = catalog.search_path();
    if search_path.is_empty() {
        return statistics.stxnamespace == PG_CATALOG_NAMESPACE_OID;
    }
    for schema_name in search_path {
        if is_temp_schema(&schema_name) {
            continue;
        }
        for candidate in catalog.statistic_ext_rows() {
            if !candidate.stxname.eq_ignore_ascii_case(&statistics.stxname) {
                continue;
            }
            let Some(namespace) = catalog.namespace_row_by_oid(candidate.stxnamespace) else {
                continue;
            };
            if is_temp_schema(&namespace.nspname) {
                continue;
            }
            if namespace.nspname.eq_ignore_ascii_case(&schema_name) {
                return candidate.oid == statistics.oid;
            }
        }
    }
    false
}

fn eval_pg_rust_internal_binary_coercible(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left_oid = oid_arg_to_u32(left, "pg_rust_internal_binary_coercible")?;
            let right_oid = oid_arg_to_u32(right, "pg_rust_internal_binary_coercible")?;
            let left_type =
                sql_type_from_builtin_oid(left_oid).ok_or_else(|| ExecError::DetailedError {
                    message: format!("type with OID {left_oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            let right_type =
                sql_type_from_builtin_oid(right_oid).ok_or_else(|| ExecError::DetailedError {
                    message: format!("type with OID {right_oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            Ok(Value::Bool(is_binary_coercible_type(left_type, right_type)))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "pg_rust_internal_binary_coercible",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        }),
    }
}

fn lookup_system_binding(
    bindings: &[crate::include::nodes::execnodes::SystemVarBinding],
    varno: usize,
) -> Result<Value, ExecError> {
    bindings
        .iter()
        .find(|binding| binding.varno == varno)
        .map(|binding| Value::Int64(i64::from(binding.table_oid)))
        .ok_or(ExecError::DetailedError {
            message: "tableoid is not available for this row".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn lookup_ctid(slot: &TupleSlot) -> Result<Value, ExecError> {
    slot.tid()
        .map(|tid| {
            Value::Text(CompactString::from_owned(format!(
                "({},{})",
                tid.block_number, tid.offset_number
            )))
        })
        .ok_or(ExecError::DetailedError {
            message: "ctid is not available for this row".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn builtin_function_for_expr(funcid: u32) -> Result<BuiltinScalarFunction, ExecError> {
    builtin_scalar_function_for_proc_oid(funcid).ok_or_else(|| ExecError::DetailedError {
        message: format!("no builtin implementation for function oid {funcid}").into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}

fn role_catalog(
    ctx: &ExecutorContext,
) -> Result<&crate::backend::utils::cache::visible_catalog::VisibleCatalog, ExecError> {
    ctx.catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "role lookup requires a visible catalog".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn auth_role_name(ctx: &ExecutorContext, role_oid: u32) -> Result<Value, ExecError> {
    let catalog = role_catalog(ctx)?;
    let role = catalog
        .authid_rows()
        .into_iter()
        .find(|row| row.oid == role_oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("role with OID {role_oid} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    Ok(Value::Text(role.rolname.into()))
}

fn eval_current_setting(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let (name, missing_ok) = match values {
        [Value::Text(name)] => (normalize_guc_name(name), false),
        [Value::Text(name), Value::Bool(missing_ok)] => (normalize_guc_name(name), *missing_ok),
        [Value::Null] | [Value::Null, _] => return Ok(Value::Null),
        [left] => {
            return Err(ExecError::TypeMismatch {
                op: "current_setting",
                left: left.clone(),
                right: Value::Text("".into()),
            });
        }
        [left, right] => {
            return Err(ExecError::TypeMismatch {
                op: "current_setting",
                left: left.clone(),
                right: right.clone(),
            });
        }
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "current_setting(name [, missing_ok])",
                actual: format!("CurrentSetting({} args)", values.len()),
            }));
        }
    };

    if name == "role" {
        if let Some(role_oid) = ctx.active_role_oid {
            return auth_role_name(ctx, role_oid);
        }
        return Ok(Value::Text("none".into()));
    }
    if name == "timezone" {
        return Ok(Value::Text(ctx.datetime_config.time_zone.clone().into()));
    }
    if name == "datestyle" {
        return Ok(Value::Text(
            crate::backend::utils::misc::guc_datetime::format_datestyle(&ctx.datetime_config)
                .into(),
        ));
    }

    if let Some(value) = ctx
        .gucs
        .get(&name)
        .cloned()
        .or_else(|| plpgsql_guc_default_value(&name).map(str::to_string))
    {
        return Ok(Value::Text(value.into()));
    }

    if missing_ok {
        return Ok(Value::Null);
    }

    Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
        name,
    )))
}

fn eval_current_setting_without_context(values: &[Value]) -> Result<Value, ExecError> {
    let (name, missing_ok) = match values {
        [Value::Text(name)] => (normalize_guc_name(name), false),
        [Value::Text(name), Value::Bool(missing_ok)] => (normalize_guc_name(name), *missing_ok),
        [Value::Null] | [Value::Null, _] => return Ok(Value::Null),
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "current_setting(name [, missing_ok])",
                actual: format!("CurrentSetting({} args)", values.len()),
            }));
        }
    };
    if name == "role" {
        return Ok(Value::Text("none".into()));
    }
    if let Some(value) = plpgsql_guc_default_value(&name) {
        return Ok(Value::Text(value.into()));
    }
    if missing_ok {
        return Ok(Value::Null);
    }
    Err(ExecError::Parse(ParseError::UnknownConfigurationParameter(
        name,
    )))
}

fn quote_identifier_if_needed(identifier: &str) -> String {
    if !identifier.is_empty()
        && identifier.chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_lowercase()
            } else {
                ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()
            }
        })
    {
        return identifier.into();
    }
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn eval_regrole_to_text_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let oid = match value {
        Value::Int32(oid) if *oid >= 0 => *oid as u32,
        Value::Int64(oid) if *oid >= 0 && *oid <= i64::from(u32::MAX) => *oid as u32,
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "::text",
                left: value.clone(),
                right: Value::Text("".into()),
            });
        }
    };
    if oid == 0 {
        return Ok(Value::Text("-".into()));
    }
    if let Some(role_name) = ctx
        .and_then(|ctx| ctx.catalog.as_ref())
        .and_then(|catalog| {
            catalog
                .authid_rows()
                .into_iter()
                .find(|row| row.oid == oid)
                .map(|row| row.rolname)
        })
    {
        return Ok(Value::Text(quote_identifier_if_needed(&role_name).into()));
    }
    Ok(Value::Text(oid.to_string().into()))
}

fn relation_name_for_regclass_oid(
    oid: u32,
    catalog: Option<&crate::backend::utils::cache::visible_catalog::VisibleCatalog>,
) -> Option<String> {
    let catalog = catalog?;
    catalog
        .relcache()
        .entries()
        .find_map(|(name, entry)| (entry.relation_oid == oid).then_some(name))
        .map(|name| {
            name.rsplit_once('.')
                .map(|(_, relname)| relname)
                .unwrap_or(name)
                .to_string()
        })
}

fn eval_regclass_to_text_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let oid = match value {
        Value::Int32(oid) if *oid >= 0 => *oid as u32,
        Value::Int64(oid) if *oid >= 0 && *oid <= i64::from(u32::MAX) => *oid as u32,
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "::text",
                left: value.clone(),
                right: Value::Text("".into()),
            });
        }
    };
    if oid == 0 {
        return Ok(Value::Text("-".into()));
    }
    if let Some(relation_name) =
        relation_name_for_regclass_oid(oid, ctx.and_then(|ctx| ctx.catalog.as_ref()))
    {
        return Ok(Value::Text(
            quote_identifier_if_needed(&relation_name).into(),
        ));
    }
    Ok(Value::Text(oid.to_string().into()))
}

fn eval_regtype_to_text_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let oid = match value {
        Value::Int32(oid) if *oid >= 0 => *oid as u32,
        Value::Int64(oid) if *oid >= 0 && *oid <= i64::from(u32::MAX) => *oid as u32,
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "::text",
                left: value.clone(),
                right: Value::Text("".into()),
            });
        }
    };
    if oid == 0 {
        return Ok(Value::Text("-".into()));
    }
    let text = expr_reg::format_type_optional(Some(oid), None, catalog_lookup(ctx));
    match text {
        Value::Text(text) if text.as_str() != "???" => Ok(Value::Text(text)),
        _ => Ok(Value::Text(oid.to_string().into())),
    }
}

fn eval_text_to_regclass_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(text) = value.as_text() else {
        return Err(ExecError::TypeMismatch {
            op: "::regclass",
            left: value.clone(),
            right: Value::Int64(i64::from(crate::include::catalog::REGCLASS_TYPE_OID)),
        });
    };
    if text
        .chars()
        .all(|ch| ch.is_ascii_digit() || matches!(ch, '+' | '-'))
    {
        let value = text
            .parse::<i128>()
            .map_err(|_| ExecError::InvalidIntegerInput {
                ty: "oid",
                value: text.to_string(),
            })?;
        let oid = if (0..=u32::MAX as i128).contains(&value) {
            value as u32
        } else if (i32::MIN as i128..=-1).contains(&value) {
            (value as i32) as u32
        } else {
            return Err(ExecError::IntegerOutOfRange {
                ty: "oid",
                value: text.to_string(),
            });
        };
        return Ok(Value::Int64(i64::from(oid)));
    }
    let catalog =
        ctx.and_then(|ctx| ctx.catalog.as_ref())
            .ok_or_else(|| ExecError::DetailedError {
                message: "regclass lookup requires a visible catalog".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
    let relation = catalog
        .lookup_any_relation(text)
        .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(text.to_string())))?;
    Ok(Value::Int64(i64::from(relation.relation_oid)))
}

fn eval_to_reg_object_function(
    values: &[Value],
    kind: SqlTypeKind,
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    expr_reg::to_reg_object(value, kind, catalog_lookup(ctx))
}

fn eval_to_regtypemod_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    Ok(expr_reg::to_regtypemod(value, catalog_lookup(ctx)))
}

fn eval_format_type_function(
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    let Some(oid_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(oid_value, Value::Null) {
        return Ok(Value::Null);
    }
    let oid = oid_arg_to_u32(oid_value, "format_type")?;
    let typmod = values.get(1).and_then(|value| {
        if matches!(value, Value::Null) {
            None
        } else {
            int32_arg(value, "format_type").ok()
        }
    });
    Ok(expr_reg::format_type_optional(
        Some(oid),
        typmod,
        catalog_lookup(ctx),
    ))
}

fn ensure_builtin_side_effects_allowed(
    func: BuiltinScalarFunction,
    ctx: &ExecutorContext,
) -> Result<(), ExecError> {
    if matches!(
        func,
        BuiltinScalarFunction::NextVal
            | BuiltinScalarFunction::SetVal
            | BuiltinScalarFunction::PgNotify
            | BuiltinScalarFunction::LoCreate
            | BuiltinScalarFunction::LoUnlink
            | BuiltinScalarFunction::PgAdvisoryLock
            | BuiltinScalarFunction::PgAdvisoryXactLock
            | BuiltinScalarFunction::PgAdvisoryLockShared
            | BuiltinScalarFunction::PgAdvisoryXactLockShared
            | BuiltinScalarFunction::PgTryAdvisoryLock
            | BuiltinScalarFunction::PgTryAdvisoryXactLock
            | BuiltinScalarFunction::PgTryAdvisoryLockShared
            | BuiltinScalarFunction::PgTryAdvisoryXactLockShared
            | BuiltinScalarFunction::PgAdvisoryUnlock
            | BuiltinScalarFunction::PgAdvisoryUnlockShared
            | BuiltinScalarFunction::PgAdvisoryUnlockAll
    ) && !ctx.allow_side_effects
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "{} is not allowed in a read-only execution context",
                match func {
                    BuiltinScalarFunction::NextVal => "nextval",
                    BuiltinScalarFunction::SetVal => "setval",
                    BuiltinScalarFunction::PgNotify => "pg_notify",
                    BuiltinScalarFunction::LoCreate => "lo_create",
                    BuiltinScalarFunction::LoUnlink => "lo_unlink",
                    BuiltinScalarFunction::PgAdvisoryLock => "pg_advisory_lock",
                    BuiltinScalarFunction::PgAdvisoryXactLock => "pg_advisory_xact_lock",
                    BuiltinScalarFunction::PgAdvisoryLockShared => "pg_advisory_lock_shared",
                    BuiltinScalarFunction::PgAdvisoryXactLockShared => {
                        "pg_advisory_xact_lock_shared"
                    }
                    BuiltinScalarFunction::PgTryAdvisoryLock => "pg_try_advisory_lock",
                    BuiltinScalarFunction::PgTryAdvisoryXactLock => {
                        "pg_try_advisory_xact_lock"
                    }
                    BuiltinScalarFunction::PgTryAdvisoryLockShared => {
                        "pg_try_advisory_lock_shared"
                    }
                    BuiltinScalarFunction::PgTryAdvisoryXactLockShared => {
                        "pg_try_advisory_xact_lock_shared"
                    }
                    BuiltinScalarFunction::PgAdvisoryUnlock => "pg_advisory_unlock",
                    BuiltinScalarFunction::PgAdvisoryUnlockShared => {
                        "pg_advisory_unlock_shared"
                    }
                    BuiltinScalarFunction::PgAdvisoryUnlockAll => "pg_advisory_unlock_all",
                    _ => unreachable!(),
                }
            ),
            detail: None,
            hint: None,
            sqlstate: "25006",
        });
    }
    Ok(())
}

fn sequence_catalog(
    ctx: &ExecutorContext,
) -> Result<&crate::backend::utils::cache::visible_catalog::VisibleCatalog, ExecError> {
    ctx.catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence lookup requires a visible catalog".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn executor_catalog(
    ctx: &ExecutorContext,
) -> Result<&crate::backend::utils::cache::visible_catalog::VisibleCatalog, ExecError> {
    ctx.catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "catalog lookup requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn eval_pg_get_userbyid(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let role_oid = oid_arg_to_u32(value, "pg_get_userbyid")?;
            let catalog = executor_catalog(ctx)?;
            Ok(Value::Text(
                catalog
                    .role_name_by_oid(role_oid)
                    .unwrap_or_else(|| format!("unknown (OID={role_oid})"))
                    .into(),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_userbyid(oid)",
            actual: format!("PgGetUserById({} args)", values.len()),
        })),
    }
}

fn text_acl_array(acl: Vec<String>) -> Value {
    Value::Array(
        acl.into_iter()
            .map(|item| Value::Text(item.into()))
            .collect(),
    )
}

fn eval_pg_get_acl(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [classid, objid, objsubid] => {
            let classid = oid_arg_to_u32(classid, "pg_get_acl")?;
            let objid = oid_arg_to_u32(objid, "pg_get_acl")?;
            let objsubid = int32_arg(objsubid, "pg_get_acl")?;
            if classid == 0 && objid == 0 {
                return Ok(Value::Null);
            }
            let catalog = executor_catalog(ctx)?;
            Ok(match classid {
                PG_CLASS_RELATION_OID if objsubid == 0 => catalog
                    .class_row_by_oid(objid)
                    .and_then(|row| row.relacl.map(text_acl_array))
                    .unwrap_or(Value::Null),
                PG_DATABASE_RELATION_OID if objsubid == 0 => catalog
                    .database_row_by_oid(objid)
                    .and_then(|row| row.datacl.map(text_acl_array))
                    .unwrap_or(Value::Null),
                PG_FOREIGN_DATA_WRAPPER_RELATION_OID if objsubid == 0 => catalog
                    .foreign_data_wrapper_row_by_oid(objid)
                    .and_then(|row| row.fdwacl.map(text_acl_array))
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            })
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_acl(classid, objid, objsubid)",
            actual: format!("PgGetAcl({} args)", values.len()),
        })),
    }
}

fn eval_obj_description(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [objoid, class_name] => {
            let objoid = oid_arg_to_u32(objoid, "obj_description")?;
            let Some(class_name) = class_name.as_text() else {
                return Err(ExecError::TypeMismatch {
                    op: "obj_description",
                    left: class_name.clone(),
                    right: Value::Text("".into()),
                });
            };
            let catalog = executor_catalog(ctx)?;
            let Some(classoid) = catalog
                .lookup_any_relation(class_name)
                .map(|rel| rel.relation_oid)
            else {
                return Ok(Value::Null);
            };
            let rows = probe_system_catalog_rows_visible_in_db(
                &ctx.pool,
                &ctx.txns,
                &ctx.snapshot,
                ctx.client_id,
                CURRENT_DATABASE_OID,
                PG_DESCRIPTION_O_C_O_INDEX_OID,
                vec![
                    crate::include::access::scankey::ScanKeyData {
                        attribute_number: 1,
                        strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                        argument: Value::Int64(i64::from(objoid)),
                    },
                    crate::include::access::scankey::ScanKeyData {
                        attribute_number: 2,
                        strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                        argument: Value::Int64(i64::from(classoid)),
                    },
                    crate::include::access::scankey::ScanKeyData {
                        attribute_number: 3,
                        strategy: crate::include::access::nbtree::BT_EQUAL_STRATEGY_NUMBER,
                        argument: Value::Int32(0),
                    },
                ],
            )
            .map_err(|err| ExecError::DetailedError {
                message: format!("pg_description lookup failed: {err:?}"),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
            let Some(row) = rows.into_iter().next() else {
                return Ok(Value::Null);
            };
            let row =
                pg_description_row_from_values(row).map_err(|err| ExecError::DetailedError {
                    message: format!("invalid pg_description row: {err:?}"),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            Ok(Value::Text(row.description.into()))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "obj_description(oid, catalog_name)",
            actual: format!("ObjDescription({} args)", values.len()),
        })),
    }
}

fn eval_pg_describe_object(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [classid, objid, objsubid] => {
            let classid = oid_arg_to_u32(classid, "pg_describe_object")?;
            let objid = oid_arg_to_u32(objid, "pg_describe_object")?;
            let objsubid = oid_arg_to_u32(objsubid, "pg_describe_object")?;
            if classid == 0 && objid == 0 {
                return Ok(Value::Null);
            }
            let catalog = executor_catalog(ctx)?;
            let Some(class_row) = catalog.class_row_by_oid(classid) else {
                return Ok(Value::Null);
            };
            if objsubid != 0 {
                return Ok(Value::Null);
            }
            let description = match class_row.relname.as_str() {
                "pg_namespace" => catalog
                    .namespace_row_by_oid(objid)
                    .map(|row| format!("schema {}", row.nspname)),
                "pg_proc" => catalog
                    .proc_row_by_oid(objid)
                    .map(|row| function_identity_text(&row, catalog)),
                "pg_operator" => catalog
                    .operator_by_oid(objid)
                    .map(|row| operator_identity_text(&row, catalog)),
                "pg_statistic_ext" => catalog.statistic_ext_row_by_oid(objid).and_then(|row| {
                    let namespace = catalog.namespace_row_by_oid(row.stxnamespace)?;
                    Some(format!(
                        "statistics object {}",
                        quote_qualified_identifier(&namespace.nspname, &row.stxname)
                    ))
                }),
                _ => None,
            };
            Ok(description
                .map(|text| Value::Text(text.into()))
                .unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_describe_object(classid, objid, objsubid)",
            actual: format!("PgDescribeObject({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_statisticsobjdef(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_get_statisticsobjdef")?;
            let catalog = executor_catalog(ctx)?;
            Ok(catalog
                .statistic_ext_row_by_oid(oid)
                .and_then(|row| statistics_definition_text(&row, catalog))
                .map(|text| Value::Text(text.into()))
                .unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_statisticsobjdef(oid)",
            actual: format!("PgGetStatisticsObjDef({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_statisticsobjdef_columns(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_get_statisticsobjdef_columns")?;
            let catalog = executor_catalog(ctx)?;
            Ok(catalog
                .statistic_ext_row_by_oid(oid)
                .and_then(|row| statistics_columns_text(&row, catalog))
                .map(|text| Value::Text(text.into()))
                .unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_statisticsobjdef_columns(oid)",
            actual: format!("PgGetStatisticsObjDefColumns({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_statisticsobjdef_expressions(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_get_statisticsobjdef_expressions")?;
            let catalog = executor_catalog(ctx)?;
            let Some(statistics) = catalog.statistic_ext_row_by_oid(oid) else {
                return Ok(Value::Null);
            };
            let expressions = statistics_expression_texts(&statistics);
            if expressions.is_empty() {
                return Ok(Value::Null);
            }
            Ok(Value::Array(
                expressions
                    .into_iter()
                    .map(|expr| Value::Text(expr.into()))
                    .collect(),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_statisticsobjdef_expressions(oid)",
            actual: format!("PgGetStatisticsObjDefExpressions({} args)", values.len()),
        })),
    }
}

fn eval_pg_statistics_obj_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_statistics_obj_is_visible")?;
            let catalog = executor_catalog(ctx)?;
            Ok(Value::Bool(
                catalog
                    .statistic_ext_row_by_oid(oid)
                    .is_some_and(|row| statistics_visible_in_search_path(&row, catalog)),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_statistics_obj_is_visible(oid)",
            actual: format!("PgStatisticsObjIsVisible({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_expr(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [Value::Null, _, _] | [_, Value::Null] | [_, Value::Null, _] => {
            Ok(Value::Null)
        }
        [expr, _relation] => Ok(expr
            .as_text()
            .map(|text| Value::Text(text.into()))
            .unwrap_or(Value::Null)),
        [expr, _relation, _pretty] => Ok(expr
            .as_text()
            .map(|text| Value::Text(text.into()))
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_expr(pg_node_tree, oid [, pretty])",
            actual: format!("PgGetExpr({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_constraintdef(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let constraint_oid = match values {
        [Value::Null] | [Value::Null, _] | [_, Value::Null] => return Ok(Value::Null),
        [constraint_oid] => oid_arg_to_u32(constraint_oid, "pg_get_constraintdef")?,
        [constraint_oid, _pretty] => oid_arg_to_u32(constraint_oid, "pg_get_constraintdef")?,
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_get_constraintdef(oid [, pretty])",
                actual: format!("PgGetConstraintDef({} args)", values.len()),
            }));
        }
    };
    let catalog = executor_catalog(ctx)?;
    let Some(row) = catalog.constraint_row_by_oid(constraint_oid) else {
        return Ok(Value::Null);
    };
    Ok(format_constraintdef_for_catalog(catalog, &row)
        .map(|definition| Value::Text(definition.into()))
        .unwrap_or(Value::Null))
}

fn format_constraintdef_for_catalog(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    match row.contype {
        CONSTRAINT_NOTNULL => Some("NOT NULL".into()),
        CONSTRAINT_CHECK => row
            .conbin
            .as_deref()
            .map(|expr_sql| format!("CHECK ({expr_sql})")),
        CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE => {
            format_index_backed_constraintdef_for_catalog(catalog, row)
        }
        CONSTRAINT_FOREIGN => format_foreign_key_constraintdef_for_catalog(catalog, row),
        _ => None,
    }
}

fn format_index_backed_constraintdef_for_catalog(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let relation = catalog.lookup_relation_by_oid(row.conrelid)?;
    let index = catalog
        .index_relations_for_heap(row.conrelid)
        .into_iter()
        .find(|index| index.relation_oid == row.conindid)?;
    let mut columns = index_column_names_for_heap(&relation.desc, &index.index_meta.indkey)?;
    if row.conperiod
        && let Some(period_column) = columns.last_mut()
    {
        period_column.push_str(" WITHOUT OVERLAPS");
    }
    let prefix = if row.contype == CONSTRAINT_PRIMARY {
        "PRIMARY KEY"
    } else {
        "UNIQUE"
    };
    let mut def = format!("{prefix} ({})", columns.join(", "));
    append_constraint_deferrability(&mut def, row);
    Some(def)
}

fn format_foreign_key_constraintdef_for_catalog(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let relation = catalog.lookup_relation_by_oid(row.conrelid)?;
    let referenced_relation = catalog.lookup_relation_by_oid(row.confrelid)?;
    let local_columns = index_column_names_for_heap(&relation.desc, row.conkey.as_ref()?)?;
    let referenced_columns =
        index_column_names_for_heap(&referenced_relation.desc, row.confkey.as_ref()?)?;
    let referenced_name = catalog
        .class_row_by_oid(row.confrelid)
        .map(|class| class.relname)
        .unwrap_or_else(|| row.confrelid.to_string());
    let mut def = format!(
        "FOREIGN KEY ({}) REFERENCES {}({})",
        local_columns.join(", "),
        referenced_name,
        referenced_columns.join(", ")
    );
    if row.confdeltype == 'r' {
        def.push_str(" ON DELETE RESTRICT");
    }
    if row.confupdtype == 'r' {
        def.push_str(" ON UPDATE RESTRICT");
    }
    append_constraint_deferrability(&mut def, row);
    Some(def)
}

fn append_constraint_deferrability(
    def: &mut String,
    row: &crate::include::catalog::PgConstraintRow,
) {
    if !row.condeferrable {
        return;
    }
    def.push_str(" DEFERRABLE");
    if row.condeferred {
        def.push_str(" INITIALLY DEFERRED");
    }
}

fn eval_pg_get_indexdef(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let (index_oid, column_no) = match values {
        [Value::Null] | [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => {
            return Ok(Value::Null);
        }
        [index_oid] => (oid_arg_to_u32(index_oid, "pg_get_indexdef")?, 0),
        [index_oid, column_no, _pretty] => (
            oid_arg_to_u32(index_oid, "pg_get_indexdef")?,
            int32_arg(column_no, "pg_get_indexdef")?,
        ),
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_get_indexdef(oid [, column_no, pretty])",
                actual: format!("PgGetIndexDef({} args)", values.len()),
            }));
        }
    };
    let catalog = executor_catalog(ctx)?;
    let Some((relation, index)) = index_relation_for_oid(catalog, index_oid) else {
        return Ok(Value::Null);
    };
    if column_no > 0 {
        let columns = index_column_names_for_heap(&relation.desc, &index.index_meta.indkey);
        return Ok(columns
            .and_then(|columns| columns.get((column_no as usize).saturating_sub(1)).cloned())
            .map(|column| Value::Text(column.into()))
            .unwrap_or(Value::Null));
    }
    Ok(Value::Text(
        format_indexdef_for_catalog(catalog, &relation, &index).into(),
    ))
}

fn index_relation_for_oid(
    catalog: &dyn CatalogLookup,
    index_oid: u32,
) -> Option<(
    crate::backend::parser::BoundRelation,
    crate::backend::parser::BoundIndexRelation,
)> {
    catalog
        .constraint_rows_for_index(index_oid)
        .into_iter()
        .next()
        .and_then(|row| {
            let relation = catalog.lookup_relation_by_oid(row.conrelid)?;
            let index = catalog
                .index_relations_for_heap(row.conrelid)
                .into_iter()
                .find(|index| index.relation_oid == index_oid)?;
            Some((relation, index))
        })
}

fn format_indexdef_for_catalog(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    index: &crate::backend::parser::BoundIndexRelation,
) -> String {
    let table_name = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|class| class.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    let amname = bootstrap_pg_am_rows()
        .into_iter()
        .find(|row| row.oid == index.index_meta.am_oid)
        .map(|row| row.amname)
        .unwrap_or_else(|| "btree".into());
    let columns = index_column_names_for_heap(&relation.desc, &index.index_meta.indkey)
        .unwrap_or_else(|| {
            index
                .desc
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        });
    let unique = if index.index_meta.indisunique {
        "UNIQUE "
    } else {
        ""
    };
    let mut definition = format!(
        "CREATE {unique}INDEX {} ON {} USING {} ({})",
        index.name,
        table_name,
        amname,
        columns.join(", ")
    );
    if let Some(predicate) = index
        .index_meta
        .indpred
        .as_deref()
        .filter(|pred| !pred.is_empty())
    {
        definition.push_str(" WHERE (");
        definition.push_str(predicate);
        definition.push(')');
    }
    definition
}

fn index_column_names_for_heap(desc: &RelationDesc, attnums: &[i16]) -> Option<Vec<String>> {
    attnums
        .iter()
        .map(|attnum| {
            (*attnum > 0)
                .then(|| desc.columns.get((*attnum as usize).saturating_sub(1)))
                .flatten()
                .map(|column| column.name.clone())
        })
        .collect()
}

fn eval_pg_get_viewdef(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let catalog = executor_catalog(ctx)?;
    let relation_oid = match values {
        [Value::Null] | [Value::Null, _] | [_, Value::Null] => return Ok(Value::Null),
        [value] | [value, _] => {
            if let Some(text) = value.as_text() {
                catalog
                    .lookup_any_relation(text)
                    .map(|entry| entry.relation_oid)
                    .unwrap_or_default()
            } else {
                oid_arg_to_u32(value, "pg_get_viewdef")?
            }
        }
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_get_viewdef(view [, pretty_or_wrap])",
                actual: format!("PgGetViewDef({} args)", values.len()),
            }));
        }
    };
    if relation_oid == 0 {
        return Ok(Value::Null);
    }
    let Some(relation) = catalog.lookup_relation_by_oid(relation_oid) else {
        return Ok(Value::Null);
    };
    if !matches!(relation.relkind, 'v' | 'm') {
        return Ok(Value::Null);
    }
    let definition =
        format_view_definition(relation_oid, &relation.desc, catalog).map_err(ExecError::Parse)?;
    Ok(Value::Text(definition.into()))
}

fn eval_pg_get_triggerdef(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let catalog = executor_catalog(ctx)?;
    let (trigger_oid, pretty) = match values {
        [Value::Null] | [Value::Null, _] | [_, Value::Null] => return Ok(Value::Null),
        [trigger_oid] => (oid_arg_to_u32(trigger_oid, "pg_get_triggerdef")?, false),
        [trigger_oid, pretty] => (
            oid_arg_to_u32(trigger_oid, "pg_get_triggerdef")?,
            matches!(pretty, Value::Bool(true)),
        ),
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_get_triggerdef(oid [, pretty])",
                actual: format!("PgGetTriggerDef({} args)", values.len()),
            }));
        }
    };
    let Some(trigger_row) = catalog
        .relcache()
        .entries()
        .flat_map(|(_, entry)| catalog.trigger_rows_for_relation(entry.relation_oid))
        .find(|row| row.oid == trigger_oid)
    else {
        return Ok(Value::Null);
    };
    let Some(formatted) = format_trigger_definition(catalog, &trigger_row, pretty) else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(formatted.definition.into()))
}

fn current_slot_raw_attr_bytes<'a>(
    slot: &'a TupleSlot,
    index: usize,
) -> Result<Option<&'a [u8]>, ExecError> {
    match &slot.kind {
        SlotKind::BufferHeapTuple {
            attr_descs,
            tuple_ptr,
            tuple_len,
            ..
        } => {
            let bytes: &'a [u8] = unsafe { std::slice::from_raw_parts(*tuple_ptr, *tuple_len) };
            let raw = crate::include::access::htup::deform_raw(bytes, attr_descs)?;
            Ok(raw.get(index).copied().flatten())
        }
        SlotKind::HeapTuple {
            attr_descs, tuple, ..
        } => {
            let raw = tuple.deform(attr_descs)?;
            Ok(raw.get(index).copied().flatten())
        }
        SlotKind::Virtual | SlotKind::Empty => Ok(None),
    }
}

fn compression_method_name_value(method: u32) -> Result<Value, ExecError> {
    match ToastCompressionId::from_u32(method) {
        Some(ToastCompressionId::Pglz) => Ok(Value::Text("pglz".into())),
        Some(ToastCompressionId::Lz4) => Ok(Value::Text("lz4".into())),
        Some(ToastCompressionId::Invalid) | None => Err(ExecError::DetailedError {
            message: format!("invalid compression method id {method}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn eval_pg_column_compression_raw(raw: &[u8]) -> Result<Value, ExecError> {
    if crate::include::varatt::is_ondisk_toast_pointer(raw) {
        let pointer =
            crate::include::varatt::decode_ondisk_toast_pointer(raw).ok_or_else(|| {
                ExecError::DetailedError {
                    message: "invalid on-disk toast pointer".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                }
            })?;
        if !crate::include::varatt::varatt_external_is_compressed(pointer) {
            return Ok(Value::Null);
        }
        return compression_method_name_value(
            crate::include::varatt::varatt_external_get_compression_method(pointer),
        );
    }
    if crate::include::varatt::is_compressed_inline_datum(raw) {
        let method =
            crate::include::varatt::compressed_inline_compression_method(raw).ok_or_else(|| {
                ExecError::DetailedError {
                    message: "invalid compressed inline datum".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                }
            })?;
        return compression_method_name_value(method);
    }
    Ok(Value::Null)
}

fn eval_pg_column_compression_values(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [_] => Ok(Value::Null),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_column_compression(any)",
            actual: format!("PgColumnCompression({} args)", values.len()),
        })),
    }
}

fn eval_pg_column_size_values(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_column_size(any)",
            actual: format!("PgColumnSize({} args)", values.len()),
        }));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    let size = match value {
        Value::Jsonb(bytes) | Value::Bytea(bytes) => bytes.len(),
        Value::Text(text) | Value::Json(text) | Value::JsonPath(text) | Value::Xml(text) => {
            text.len()
        }
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().len(),
        Value::Int16(_) => 2,
        Value::Int32(_) | Value::Date(_) | Value::InternalChar(_) => 4,
        Value::Int64(_)
        | Value::PgLsn(_)
        | Value::Money(_)
        | Value::Float64(_)
        | Value::Time(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => 8,
        Value::TimeTz(_) => 12,
        Value::Interval(_) | Value::Uuid(_) => 16,
        Value::Bool(_) => 1,
        Value::Numeric(numeric) => numeric.render().len(),
        Value::Bit(bits) => bits.bytes.len(),
        Value::TsVector(vector) => crate::backend::executor::render_tsvector_text(vector).len(),
        Value::TsQuery(query) => crate::backend::executor::render_tsquery_text(query).len(),
        Value::PgArray(array) => format_array_value_text(array).len(),
        Value::Array(array) => format_array_text(array).len(),
        Value::Record(record) => {
            crate::backend::executor::value_io::format_record_text(record).len()
        }
        Value::Range(_) => crate::backend::executor::render_range_text(value)
            .unwrap_or_default()
            .len(),
        Value::Inet(_) | Value::Cidr(_) => crate::backend::executor::render_network_text(value)
            .unwrap_or_default()
            .len(),
        Value::Multirange(_) => super::expr_multirange::render_multirange_text(value)
            .unwrap_or_default()
            .len(),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => crate::backend::executor::render_geometry_text(
            value,
            crate::backend::libpq::pqformat::FloatFormatOptions::default(),
        )
        .unwrap_or_default()
        .len(),
        Value::Null => unreachable!("SQL NULL handled above"),
    };
    Ok(Value::Int32(size.min(i32::MAX as usize) as i32))
}

fn eval_pg_relation_size(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_relation_size(regclass)",
            actual: format!("PgRelationSize({} args)", values.len()),
        }));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    let relation_oid = oid_arg_to_u32(value, "pg_relation_size")?;
    let catalog = executor_catalog(ctx)?;
    let Some(relation) = catalog.relcache().get_by_oid(relation_oid) else {
        return Err(ExecError::DetailedError {
            message: format!("could not open relation with OID {relation_oid}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if relation.relkind == 't'
        && let Some(parent) = catalog
            .relcache()
            .entries()
            .find_map(|(_, entry)| (entry.reltoastrelid == relation_oid).then_some(entry))
    {
        return Ok(Value::Int64(
            if parent_references_toast_relation(parent, relation_oid, ctx)? {
                i64::from(crate::backend::storage::smgr::smgr::BLCKSZ as i32)
            } else {
                0
            },
        ));
    }
    let mut scan =
        heap_scan_begin_visible(&ctx.pool, ctx.client_id, relation.rel, ctx.snapshot.clone())?;
    let txns = ctx.txns.read();
    let mut tuples = 0_i64;
    while heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)?.is_some() {
        tuples += 1;
    }
    Ok(Value::Int64(if tuples == 0 {
        0
    } else {
        i64::from(crate::backend::storage::smgr::smgr::BLCKSZ as i32)
    }))
}

fn parent_references_toast_relation(
    parent: &crate::backend::utils::cache::relcache::RelCacheEntry,
    toast_oid: u32,
    ctx: &ExecutorContext,
) -> Result<bool, ExecError> {
    let attr_descs = parent.desc.attribute_descs();
    let mut scan =
        heap_scan_begin_visible(&ctx.pool, ctx.client_id, parent.rel, ctx.snapshot.clone())?;
    let txns = ctx.txns.read();
    while let Some((_tid, tuple)) =
        heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)?
    {
        let tuple_bytes = tuple.serialize();
        let raw = crate::include::access::htup::deform_raw(&tuple_bytes, &attr_descs)?;
        for bytes in raw.into_iter().flatten() {
            if let Some(pointer) = crate::include::varatt::decode_ondisk_toast_pointer(bytes)
                && pointer.va_toastrelid == toast_oid
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn eval_pg_relation_is_publishable(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let relation_oid = oid_arg_to_u32(value, "pg_relation_is_publishable")?;
            let catalog = executor_catalog(ctx)?;
            let Some(relation) = catalog.relation_by_oid(relation_oid) else {
                return Ok(Value::Null);
            };
            let publishable = matches!(relation.relkind, 'r' | 'p')
                && relation.relpersistence == 'p'
                && relation.namespace_oid != PG_CATALOG_NAMESPACE_OID
                && relation.namespace_oid != PG_TOAST_NAMESPACE_OID;
            Ok(Value::Bool(publishable))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_relation_is_publishable(oid)",
            actual: format!("PgRelationIsPublishable({} args)", values.len()),
        })),
    }
}

fn eval_pg_partition_root(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let relation_oid = oid_arg_to_u32(value, "pg_partition_root")?;
            let catalog = executor_catalog(ctx)?;
            Ok(
                crate::backend::commands::partition::partition_root_oid(catalog, relation_oid)?
                    .map(|oid| Value::Int64(i64::from(oid)))
                    .unwrap_or(Value::Null),
            )
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_partition_root(regclass)",
            actual: format!("PgPartitionRoot({} args)", values.len()),
        })),
    }
}

fn sequence_runtime(
    ctx: &ExecutorContext,
) -> Result<&crate::pgrust::database::SequenceRuntime, ExecError> {
    ctx.sequences
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence runtime is not available".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn sequence_name_for_oid(
    catalog: &crate::backend::utils::cache::visible_catalog::VisibleCatalog,
    relation_oid: u32,
) -> Option<String> {
    catalog
        .relcache()
        .entries()
        .find(|(_, entry)| entry.relation_oid == relation_oid)
        .map(|(name, _)| name.to_string())
}

fn large_object_runtime(
    ctx: &ExecutorContext,
) -> Result<&crate::pgrust::database::LargeObjectRuntime, ExecError> {
    ctx.large_objects
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "large object runtime is not available".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn resolve_sequence_call_target(
    ctx: &ExecutorContext,
    value: &Value,
) -> Result<(u32, bool), ExecError> {
    let catalog = sequence_catalog(ctx)?;
    let relation = match value {
        Value::Int32(oid) => {
            let oid = u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange)?;
            catalog
                .relation_by_oid(oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("sequence with OID {oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                })?
        }
        Value::Int64(oid) => {
            let oid = u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange)?;
            catalog
                .relation_by_oid(oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("sequence with OID {oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                })?
        }
        Value::Text(_) | Value::TextRef(_, _) => {
            let name = value.as_text().expect("text value");
            catalog
                .lookup_any_relation(name)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(name.to_string())))?
        }
        other => {
            return Err(ExecError::TypeMismatch {
                op: "sequence function",
                left: other.clone(),
                right: Value::Text("sequence".into()),
            });
        }
    };
    if relation.relkind != 'S' {
        return Err(ExecError::Parse(ParseError::WrongObjectType {
            name: sequence_name_for_oid(catalog, relation.relation_oid)
                .unwrap_or_else(|| relation.relation_oid.to_string()),
            expected: "sequence",
        }));
    }
    Ok((relation.relation_oid, relation.relpersistence != 't'))
}

fn eval_sequence_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match (func, values) {
        (_, [Value::Null, ..]) | (_, [_, Value::Null, ..]) | (_, [_, _, Value::Null]) => {
            Ok(Value::Null)
        }
        (BuiltinScalarFunction::NextVal, [target]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value =
                sequence_runtime(ctx)?.next_value(ctx.client_id, relation_oid, persistent)?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::CurrVal, [target]) => {
            let (relation_oid, _) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.curr_value(ctx.client_id, relation_oid)?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int64(value)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                *value,
                true,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int32(value)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                i64::from(*value),
                true,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int64(value), Value::Bool(is_called)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                *value,
                *is_called,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int32(value), Value::Bool(is_called)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                i64::from(*value),
                *is_called,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::PgGetSerialSequence, [table, column]) => {
            let table_name = table.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_get_serial_sequence",
                left: table.clone(),
                right: Value::Text("".into()),
            })?;
            let column_name = column.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_get_serial_sequence",
                left: column.clone(),
                right: Value::Text("".into()),
            })?;
            let catalog = sequence_catalog(ctx)?;
            let relation = catalog.lookup_relation(table_name).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(table_name.to_string()))
            })?;
            let Some(column) = relation.desc.columns.iter().find(|candidate| {
                !candidate.dropped && candidate.name.eq_ignore_ascii_case(column_name)
            }) else {
                return Err(ExecError::Parse(ParseError::UnknownColumn(
                    column_name.to_string(),
                )));
            };
            let Some(sequence_oid) = column.default_sequence_oid else {
                return Ok(Value::Null);
            };
            Ok(sequence_name_for_oid(catalog, sequence_oid)
                .map(Into::into)
                .map(Value::Text)
                .unwrap_or(Value::Null))
        }
        (BuiltinScalarFunction::SetVal, [target, other]) => Err(ExecError::TypeMismatch {
            op: "setval",
            left: target.clone(),
            right: other.clone(),
        }),
        (BuiltinScalarFunction::SetVal, [target, other, _]) => Err(ExecError::TypeMismatch {
            op: "setval",
            left: target.clone(),
            right: other.clone(),
        }),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid sequence builtin call",
            actual: format!("{func:?}"),
        })),
    }
}

fn eval_large_object_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match (func, values) {
        (_, [Value::Null]) => Ok(Value::Null),
        (BuiltinScalarFunction::LoCreate, [value]) => {
            let oid = oid_arg_to_u32(value, "lo_create")?;
            Ok(Value::Int64(i64::from(
                large_object_runtime(ctx)?.create(oid, ctx.current_user_oid)?,
            )))
        }
        (BuiltinScalarFunction::LoUnlink, [value]) => {
            let oid = oid_arg_to_u32(value, "lo_unlink")?;
            Ok(Value::Int32(large_object_runtime(ctx)?.unlink(oid)?))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid large object builtin call",
            actual: format!("{func:?}"),
        })),
    }
}

fn eval_op_expr(
    op: &OpExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match (op.op, op.args.as_slice()) {
        (OpExprKind::UnaryPlus, [inner]) => eval_expr(inner, slot, ctx),
        (OpExprKind::Negate, [inner]) => negate_value(eval_expr(inner, slot, ctx)?),
        (OpExprKind::BitNot, [inner]) => bitwise_not_value(eval_expr(inner, slot, ctx)?),
        (OpExprKind::Add, [left, right]) => {
            add_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Sub, [left, right]) => {
            sub_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::BitAnd, [left, right]) => {
            bitwise_and_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::BitOr, [left, right]) => {
            bitwise_or_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::BitXor, [left, right]) => {
            bitwise_xor_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Shl, [left, right]) => {
            shift_left_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Shr, [left, right]) => {
            shift_right_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Mul, [left, right]) => {
            mul_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Div, [left, right]) => {
            div_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Mod, [left, right]) => {
            mod_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Concat, [left, right]) => {
            concat_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Eq, [left, right]) => compare_values_with_type(
            "=",
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
        ),
        (OpExprKind::NotEq, [left, right]) => not_equal_values_with_type(
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
        ),
        (OpExprKind::Lt, [left, right]) => order_values(
            "<",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
            op.collation_oid,
        ),
        (OpExprKind::LtEq, [left, right]) => order_values(
            "<=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
            op.collation_oid,
        ),
        (OpExprKind::Gt, [left, right]) => order_values(
            ">",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
            op.collation_oid,
        ),
        (OpExprKind::GtEq, [left, right]) => order_values(
            ">=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
            op.collation_oid,
        ),
        (OpExprKind::RegexMatch, [left, right]) => {
            let text = eval_expr(left, slot, ctx)?;
            let pattern = eval_expr(right, slot, ctx)?;
            eval_regex_match_operator(&text, &pattern)
        }
        (OpExprKind::ArrayOverlap, [left, right]) => {
            eval_array_overlap(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::ArrayContains, [left, right]) => {
            eval_array_contains(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::ArrayContained, [left, right]) => {
            eval_array_contained(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbContains, [left, right]) => {
            eval_jsonb_contains(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbContained, [left, right]) => {
            eval_jsonb_contained(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbExists, [left, right]) => {
            eval_jsonb_exists(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbExistsAny, [left, right]) => {
            eval_jsonb_exists_any(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbExistsAll, [left, right]) => {
            eval_jsonb_exists_all(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbPathExists, [left, right]) => {
            eval_jsonpath_operator(left, right, false, slot, ctx)
        }
        (OpExprKind::JsonbPathMatch, [left, right]) => {
            eval_jsonpath_operator(left, right, true, slot, ctx)
        }
        (OpExprKind::JsonGet, [left, right]) => eval_json_get(left, right, false, slot, ctx),
        (OpExprKind::JsonGetText, [left, right]) => eval_json_get(left, right, true, slot, ctx),
        (OpExprKind::JsonPath, [left, right]) => eval_json_path(left, right, false, slot, ctx),
        (OpExprKind::JsonPathText, [left, right]) => eval_json_path(left, right, true, slot, ctx),
        _ => Err(malformed_expr_error("operator")),
    }
}

fn eval_bool_expr(
    bool_expr: &BoolExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match bool_expr.boolop {
        BoolExprType::And => {
            let mut result = Value::Bool(true);
            for arg in &bool_expr.args {
                result = eval_and(result, eval_expr(arg, slot, ctx)?)?;
            }
            Ok(result)
        }
        BoolExprType::Or => {
            let mut result = Value::Bool(false);
            for arg in &bool_expr.args {
                result = eval_or(result, eval_expr(arg, slot, ctx)?)?;
            }
            Ok(result)
        }
        BoolExprType::Not => match bool_expr.args.as_slice() {
            [inner] => match eval_expr(inner, slot, ctx)? {
                Value::Bool(value) => Ok(Value::Bool(!value)),
                Value::Null => Ok(Value::Null),
                other => Err(ExecError::NonBoolQual(other)),
            },
            _ => Err(malformed_expr_error("boolean")),
        },
    }
}

fn eval_case_expr(
    case_expr: &crate::include::nodes::primnodes::CaseExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let arg_value = match &case_expr.arg {
        Some(arg) => Some(eval_expr(arg, slot, ctx)?),
        None => None,
    };
    let eval_active =
        |slot: &mut TupleSlot, ctx: &mut ExecutorContext| -> Result<Value, ExecError> {
            for when in &case_expr.args {
                match eval_expr(&when.expr, slot, ctx)? {
                    Value::Bool(true) => return eval_expr(&when.result, slot, ctx),
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            eval_expr(&case_expr.defresult, slot, ctx)
        };
    if let Some(arg_value) = arg_value {
        ctx.case_test_values.push(arg_value);
        let result = eval_active(slot, ctx);
        ctx.case_test_values.pop();
        result
    } else {
        eval_active(slot, ctx)
    }
}

fn eval_func_expr(
    func: &FuncExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => eval_builtin_function(
            builtin,
            func.funcresulttype,
            &func.args,
            func.funcvariadic,
            slot,
            ctx,
        ),
        ScalarFunctionImpl::UserDefined { proc_oid } => {
            let catalog = ctx
                .catalog
                .as_ref()
                .ok_or_else(|| ExecError::DetailedError {
                    message: "user-defined functions require executor catalog context".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                })?;
            let row =
                catalog
                    .proc_row_by_oid(proc_oid)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("unknown function oid {proc_oid}"),
                        detail: None,
                        hint: None,
                        sqlstate: "42883",
                    })?;
            match row.prolang {
                crate::include::catalog::PG_LANGUAGE_SQL_OID => {
                    execute_user_defined_sql_scalar_function(&row, &func.args, slot, ctx)
                }
                _ => execute_user_defined_scalar_function(proc_oid, &func.args, slot, ctx),
            }
        }
    }
}

fn current_temp_namespace_name(ctx: &ExecutorContext) -> Option<CompactString> {
    // :HACK: `pg_my_temp_schema()` needs session temp namespace identity, but
    // executor contexts do not thread that through directly yet. Derive the
    // visible temp schema name from the qualified temp relcache entries until
    // temp namespace metadata is carried explicitly alongside the session.
    ctx.catalog
        .as_ref()?
        .relcache()
        .entries()
        .find_map(|(name, entry)| {
            (entry.relpersistence == 't')
                .then_some(name)
                .and_then(|qualified| qualified.split_once('.'))
                .and_then(|(schema, _)| schema.starts_with("pg_temp_").then_some(schema))
                .map(Into::into)
        })
}

fn configured_current_schema_search_path(ctx: &ExecutorContext) -> Vec<String> {
    ctx.gucs
        .get("search_path")
        .filter(|value| !value.trim().eq_ignore_ascii_case("default"))
        .map(|value| {
            value
                .split(',')
                .map(|schema| {
                    schema
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_ascii_lowercase()
                })
                .filter(|schema| !schema.is_empty())
                .collect()
        })
        .unwrap_or_else(|| vec!["public".into()])
}

fn current_schema_value(ctx: &ExecutorContext) -> Value {
    let Some(catalog) = ctx.catalog.as_ref() else {
        return Value::Text("public".into());
    };
    let namespaces = catalog.namespace_rows();
    let catalog_search_path = catalog.search_path();
    let mut search_path = if catalog_search_path.is_empty() {
        configured_current_schema_search_path(ctx)
    } else {
        catalog_search_path
    };
    if search_path.len() > 1
        && search_path
            .first()
            .is_some_and(|schema| schema == "pg_catalog")
    {
        search_path.remove(0);
    }
    search_path
        .into_iter()
        .filter(|schema| schema != "$user" && schema != "pg_temp")
        .find(|schema| {
            namespaces
                .iter()
                .any(|namespace| namespace.nspname.eq_ignore_ascii_case(schema))
        })
        .map(|schema| Value::Text(schema.into()))
        .unwrap_or(Value::Null)
}

fn current_temp_namespace_oid(ctx: &ExecutorContext) -> Option<u32> {
    let name = current_temp_namespace_name(ctx)?;
    ctx.catalog
        .as_ref()?
        .namespace_rows()
        .into_iter()
        .find(|row| row.nspname == name.as_str())
        .map(|row| row.oid)
}

fn warn_time_precision_overflow(precision: Option<i32>, type_name: &str, suffix: &str) {
    if let Some(precision) = precision
        && precision > MAX_TIME_PRECISION
    {
        crate::backend::utils::misc::notices::push_warning(format!(
            "{type_name}({precision}){suffix} precision reduced to maximum allowed, {MAX_TIME_PRECISION}"
        ));
    }
}

fn eval_scalar_array_op_expr(
    saop: &ScalarArrayOpExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let left_value = eval_expr(&saop.left, slot, ctx)?;
    let right_value = eval_expr(&saop.right, slot, ctx)?;
    eval_quantified_array(
        &left_value,
        saop.op,
        saop.collation_oid,
        !saop.use_or,
        &right_value,
    )
}

fn eval_bound_tuple_var(
    tuple: Option<&Vec<Value>>,
    var: &crate::include::nodes::primnodes::Var,
) -> Result<Value, ExecError> {
    let index = attrno_index(var.varattno).ok_or_else(|| ExecError::DetailedError {
        message: "special executor Var referenced an unsupported system attribute".into(),
        detail: Some(format!(
            "varno={}, varattno={}, varlevelsup={}",
            var.varno, var.varattno, var.varlevelsup
        )),
        hint: None,
        sqlstate: "XX000",
    })?;
    let row = tuple.ok_or_else(|| ExecError::DetailedError {
        message: "special executor Var referenced without a bound tuple".into(),
        detail: Some(format!(
            "varno={}, varattno={}, index={}",
            var.varno, var.varattno, index
        )),
        hint: None,
        sqlstate: "XX000",
    })?;
    row.get(index)
        .cloned()
        .ok_or_else(|| ExecError::DetailedError {
            message: "special executor Var referenced beyond the bound tuple width".into(),
            detail: Some(format!(
                "varno={}, varattno={}, index={}, tuple_width={}",
                var.varno,
                var.varattno,
                index,
                row.len()
            )),
            hint: None,
            sqlstate: "XX000",
        })
}

fn expr_requires_stack_check(expr: &Expr) -> bool {
    !matches!(
        expr,
        Expr::Param(_)
            | Expr::Var(_)
            | Expr::Const(_)
            | Expr::CaseTest(_)
            | Expr::Random
            | Expr::CurrentDate
            | Expr::CurrentCatalog
            | Expr::CurrentSchema
            | Expr::CurrentUser
            | Expr::SessionUser
            | Expr::CurrentRole
            | Expr::CurrentTime { .. }
            | Expr::CurrentTimestamp { .. }
            | Expr::LocalTime { .. }
            | Expr::LocalTimestamp { .. }
    )
}

pub fn eval_expr(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if expr_requires_stack_check(expr) {
        ctx.check_stack_depth()?;
    }
    match expr {
        Expr::Op(op) => eval_op_expr(op, slot, ctx),
        Expr::Bool(bool_expr) => eval_bool_expr(bool_expr, slot, ctx),
        Expr::Case(case_expr) => eval_case_expr(case_expr, slot, ctx),
        Expr::CaseTest(_) => ctx
            .case_test_values
            .last()
            .cloned()
            .ok_or_else(|| malformed_expr_error("CASE test")),
        Expr::Func(func) => eval_func_expr(func, slot, ctx),
        Expr::SetReturning(_) => Err(ExecError::DetailedError {
            message: "set-returning function reached scalar expression evaluation".into(),
            detail: Some(
                "the planner should have lowered set-returning expressions into ProjectSet before execution"
                    .into(),
            ),
            hint: None,
            sqlstate: "XX000",
        }),
        Expr::Aggref(_) => Err(ExecError::DetailedError {
            message: "aggregate reference reached executor outside aggregate lowering".into(),
            detail: Some("the planner should have lowered Aggref nodes to aggregate output references before execution".into()),
            hint: None,
            sqlstate: "XX000",
        }),
        Expr::WindowFunc(_) => Err(ExecError::DetailedError {
            message: "window function reached executor outside window lowering".into(),
            detail: Some(
                "the planner should have lowered WindowFunc nodes to window output references before execution"
                    .into(),
            ),
            hint: None,
            sqlstate: "XX000",
        }),
        Expr::Xml(xml) => eval_xml_expr(xml, slot, ctx),
        Expr::ScalarArrayOp(saop) => eval_scalar_array_op_expr(saop, slot, ctx),
        Expr::SubLink(_) => Err(ExecError::DetailedError {
            message: "unplanned subquery reached executor".into(),
            detail: Some("the planner should have lowered SubLink nodes before execution".into()),
            hint: None,
            sqlstate: "XX000",
        }),
        Expr::Param(param) => ctx
            .expr_bindings
            .exec_params
            .get(&param.paramid)
            .cloned()
            .ok_or(ExecError::DetailedError {
                message: "executor param reached expression evaluation without a binding".into(),
                detail: Some(format!(
                    "paramkind={:?}, paramid={}, paramtype={:?}",
                    param.paramkind, param.paramid, param.paramtype
                )),
                hint: None,
                sqlstate: "XX000",
            }),
        Expr::Var(var) => {
            if var.varno == OUTER_VAR {
                eval_bound_tuple_var(ctx.expr_bindings.outer_tuple.as_ref(), var)
            } else if var.varno == INNER_VAR {
                eval_bound_tuple_var(ctx.expr_bindings.inner_tuple.as_ref(), var)
            } else if var.varno == INDEX_VAR {
                eval_bound_tuple_var(ctx.expr_bindings.index_tuple.as_ref(), var)
            } else if var.varlevelsup == 1 {
                let mut outer_var = var.clone();
                outer_var.varno = OUTER_VAR;
                outer_var.varlevelsup = 0;
                eval_bound_tuple_var(ctx.expr_bindings.outer_tuple.as_ref(), &outer_var)
            } else if var.varlevelsup > 0 {
                Err(ExecError::DetailedError {
                    message: "unlowered outer Var reached executor".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
            } else if var.varattno == TABLE_OID_ATTR_NO {
                lookup_system_binding(&ctx.system_bindings, var.varno)
            } else if var.varattno == SELF_ITEM_POINTER_ATTR_NO {
                lookup_ctid(slot)
            } else {
                let index = attrno_index(var.varattno).ok_or_else(|| {
                    malformed_expr_error("system attribute outside executor support")
                })?;
                let val = slot.get_attr(index)?;
                Ok(val.clone())
            }
        }
        Expr::Const(value) => Ok(value.clone()),
        Expr::Row { descriptor, fields } => Ok(Value::Record(
            crate::include::nodes::datum::RecordValue::from_descriptor(
                descriptor.clone(),
                fields
                    .iter()
                    .map(|(_, expr)| eval_expr(expr, slot, ctx))
                    .collect::<Result<Vec<_>, ExecError>>()?,
            ),
        )),
        Expr::FieldSelect { expr, field, .. } => {
            let value = eval_expr(expr, slot, ctx)?;
            eval_record_field(value, field)
        }
        Expr::Cast(inner, ty) => {
            let value = eval_expr(inner, slot, ctx)?;
            if let Value::Record(record) = value {
                cast_record_value_for_target(record, *ty, ctx)
            } else {
                cast_value_with_source_type_catalog_and_config(
                    value,
                    expr_sql_type_hint(inner),
                    *ty,
                    ctx.catalog
                        .as_ref()
                        .map(|catalog| catalog as &dyn crate::backend::parser::CatalogLookup),
                    &ctx.datetime_config,
                )
            }
        }
        Expr::Collate { expr, .. } => eval_expr(expr, slot, ctx),
        Expr::Coalesce(left, right) => {
            let left = eval_expr(left, slot, ctx)?;
            if !matches!(left, Value::Null) {
                Ok(left)
            } else {
                eval_expr(right, slot, ctx)
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            collation_oid,
            case_insensitive,
            negated,
            ..
        } => {
            let left = eval_expr(expr, slot, ctx)?;
            let pattern = eval_expr(pattern, slot, ctx)?;
            let escape = match escape {
                Some(value) => Some(eval_expr(value, slot, ctx)?),
                None => None,
            };
            eval_like(
                &left,
                &pattern,
                escape.as_ref(),
                *collation_oid,
                *case_insensitive,
                *negated,
            )
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            collation_oid,
            negated,
            ..
        } => {
            let left = eval_expr(expr, slot, ctx)?;
            let pattern = eval_expr(pattern, slot, ctx)?;
            let escape = match escape {
                Some(value) => Some(eval_expr(value, slot, ctx)?),
                None => None,
            };
            eval_similar(&left, &pattern, escape.as_ref(), *collation_oid, *negated)
        }
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(
            eval_expr(inner, slot, ctx)?,
            Value::Null
        ))),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!matches!(
            eval_expr(inner, slot, ctx)?,
            Value::Null
        ))),
        Expr::IsDistinctFrom(left, right) => Ok(Value::Bool(values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Value::Bool(!values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => {
            let element_type = array_type.element_type();
            let mut values = Vec::with_capacity(elements.len());
            let mut has_nested_arrays = false;
            for expr in elements {
                let value = eval_expr(expr, slot, ctx)?;
                if matches!(value, Value::Array(_) | Value::PgArray(_)) {
                    has_nested_arrays = true;
                    values.push(cast_value_with_config(
                        value,
                        *array_type,
                        &ctx.datetime_config,
                    )?);
                } else {
                    values.push(cast_value_with_config(
                        value,
                        element_type,
                        &ctx.datetime_config,
                    )?);
                }
            }
            if has_nested_arrays {
                let array = ArrayValue::from_nested_values(values, vec![1]).map_err(|details| {
                    ExecError::DetailedError {
                        message: "malformed array literal".into(),
                        detail: Some(details),
                        hint: None,
                        sqlstate: "22P02",
                    }
                })?;
                Ok(Value::PgArray(array))
            } else {
                Ok(Value::PgArray(ArrayValue::from_1d(values)))
            }
        }
        Expr::SubPlan(subplan) => match subplan.sublink_type {
            SubLinkType::ExprSubLink => eval_scalar_subquery(subplan, slot, ctx),
            SubLinkType::ArraySubLink => eval_array_subquery(subplan, slot, ctx),
            SubLinkType::ExistsSubLink => eval_exists_subquery(subplan, slot, ctx),
            SubLinkType::AnySubLink(op) => {
                let left = subplan.testexpr.as_ref().ok_or(ExecError::DetailedError {
                    message: "malformed ANY subplan".into(),
                    detail: Some("ANY subplans must carry a test expression".into()),
                    hint: None,
                    sqlstate: "XX000",
                })?;
                let collation_oid = top_level_explicit_collation(left);
                let left_value = eval_expr(left, slot, ctx)?;
                eval_quantified_subquery(
                    &left_value,
                    op,
                    collation_oid,
                    false,
                    subplan,
                    slot,
                    ctx,
                )
            }
            SubLinkType::AllSubLink(op) => {
                let left = subplan.testexpr.as_ref().ok_or(ExecError::DetailedError {
                    message: "malformed ALL subplan".into(),
                    detail: Some("ALL subplans must carry a test expression".into()),
                    hint: None,
                    sqlstate: "XX000",
                })?;
                let collation_oid = top_level_explicit_collation(left);
                let left_value = eval_expr(left, slot, ctx)?;
                eval_quantified_subquery(
                    &left_value,
                    op,
                    collation_oid,
                    true,
                    subplan,
                    slot,
                    ctx,
                )
            }
        },
        Expr::ArraySubscript { array, subscripts } => {
            let value = eval_expr(array, slot, ctx)?;
            eval_array_subscript(value, subscripts, slot, ctx)
        }
        Expr::Random => Ok(Value::Float64(rand::random::<f64>())),
        Expr::CurrentDate => Ok(current_date_value_from_timestamp_with_config(
            &ctx.datetime_config,
            ctx.statement_timestamp_usecs,
        )),
        Expr::CurrentCatalog => Ok(Value::Text(ctx.current_database_name.clone().into())),
        Expr::CurrentSchema => Ok(current_schema_value(ctx)),
        Expr::CurrentUser | Expr::CurrentRole => auth_role_name(ctx, ctx.current_user_oid),
        Expr::SessionUser => auth_role_name(ctx, ctx.session_user_oid),
        Expr::CurrentTime { precision } => {
            warn_time_precision_overflow(*precision, "TIME", " WITH TIME ZONE");
            Ok(current_time_value_from_timestamp_with_config(
                &ctx.datetime_config,
                ctx.statement_timestamp_usecs,
                *precision,
                true,
            ))
        }
        Expr::CurrentTimestamp { precision } => {
            warn_time_precision_overflow(*precision, "TIMESTAMP", " WITH TIME ZONE");
            Ok(current_timestamp_value_from_timestamp_with_config(
                &ctx.datetime_config,
                ctx.statement_timestamp_usecs,
                *precision,
                true,
            ))
        }
        Expr::LocalTime { precision } => {
            warn_time_precision_overflow(*precision, "TIME", "");
            Ok(current_time_value_from_timestamp_with_config(
                &ctx.datetime_config,
                ctx.statement_timestamp_usecs,
                *precision,
                false,
            ))
        }
        Expr::LocalTimestamp { precision } => {
            warn_time_precision_overflow(*precision, "TIMESTAMP", "");
            Ok(current_timestamp_value_from_timestamp_with_config(
                &ctx.datetime_config,
                ctx.statement_timestamp_usecs,
                *precision,
                false,
            ))
        }
    }
}

pub fn eval_plpgsql_expr(expr: &Expr, slot: &mut TupleSlot) -> Result<Value, ExecError> {
    match expr {
        Expr::Op(op) => match (op.op, op.args.as_slice()) {
            (OpExprKind::UnaryPlus, [inner]) => eval_plpgsql_expr(inner, slot),
            (OpExprKind::Negate, [inner]) => negate_value(eval_plpgsql_expr(inner, slot)?),
            (OpExprKind::BitNot, [inner]) => bitwise_not_value(eval_plpgsql_expr(inner, slot)?),
            (OpExprKind::Add, [left, right]) => add_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Sub, [left, right]) => sub_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::BitAnd, [left, right]) => bitwise_and_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::BitOr, [left, right]) => bitwise_or_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::BitXor, [left, right]) => bitwise_xor_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Shl, [left, right]) => shift_left_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Shr, [left, right]) => shift_right_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Mul, [left, right]) => mul_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Div, [left, right]) => div_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Mod, [left, right]) => mod_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Concat, [left, right]) => concat_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Eq, [left, right]) => compare_values_with_type(
                "=",
                eval_plpgsql_expr(left, slot)?,
                expr_sql_type_hint(left),
                eval_plpgsql_expr(right, slot)?,
                expr_sql_type_hint(right),
                op.collation_oid,
            ),
            (OpExprKind::NotEq, [left, right]) => not_equal_values_with_type(
                eval_plpgsql_expr(left, slot)?,
                expr_sql_type_hint(left),
                eval_plpgsql_expr(right, slot)?,
                expr_sql_type_hint(right),
                op.collation_oid,
            ),
            (OpExprKind::Lt, [left, right]) => order_values(
                "<",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
                op.collation_oid,
            ),
            (OpExprKind::LtEq, [left, right]) => order_values(
                "<=",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
                op.collation_oid,
            ),
            (OpExprKind::Gt, [left, right]) => order_values(
                ">",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
                op.collation_oid,
            ),
            (OpExprKind::GtEq, [left, right]) => order_values(
                ">=",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
                op.collation_oid,
            ),
            (OpExprKind::RegexMatch, [left, right]) => {
                let text = eval_plpgsql_expr(left, slot)?;
                let pattern = eval_plpgsql_expr(right, slot)?;
                eval_regex_match_operator(&text, &pattern)
            }
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql expression without subqueries or SQL statements",
                actual: format!("{expr:?}"),
            })),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => {
                let mut result = Value::Bool(true);
                for arg in &bool_expr.args {
                    result = eval_and(result, eval_plpgsql_expr(arg, slot)?)?;
                }
                Ok(result)
            }
            BoolExprType::Or => {
                let mut result = Value::Bool(false);
                for arg in &bool_expr.args {
                    result = eval_or(result, eval_plpgsql_expr(arg, slot)?)?;
                }
                Ok(result)
            }
            BoolExprType::Not => match bool_expr.args.as_slice() {
                [inner] => match eval_plpgsql_expr(inner, slot)? {
                    Value::Bool(value) => Ok(Value::Bool(!value)),
                    Value::Null => Ok(Value::Null),
                    other => Err(ExecError::NonBoolQual(other)),
                },
                _ => Err(malformed_expr_error("boolean")),
            },
        },
        Expr::Func(func) => {
            let builtin = builtin_function_for_expr(func.funcid)?;
            eval_plpgsql_builtin_function(
                builtin,
                func.funcresulttype,
                &func.args,
                func.funcvariadic,
                slot,
            )
        }
        Expr::ScalarArrayOp(saop) => {
            let left_value = eval_plpgsql_expr(&saop.left, slot)?;
            let right_value = eval_plpgsql_expr(&saop.right, slot)?;
            eval_quantified_array(
                &left_value,
                saop.op,
                saop.collation_oid,
                !saop.use_or,
                &right_value,
            )
        }
        Expr::SubLink(_) | Expr::SubPlan(_) => Err(ExecError::DetailedError {
            message: "subqueries are not supported in PL/pgSQL expression evaluation".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
        Expr::Param(_) => Err(ExecError::DetailedError {
            message: "executor params are not supported in PL/pgSQL expression evaluation".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
        Expr::Var(var) => {
            if var.varlevelsup == 0 && var.varattno == TABLE_OID_ATTR_NO {
                slot.table_oid
                    .map(|table_oid| Value::Int64(i64::from(table_oid)))
                    .ok_or_else(|| malformed_expr_error("tableoid in PL/pgSQL"))
            } else if var.varlevelsup == 0 {
                let index = attrno_index(var.varattno).ok_or_else(|| {
                    malformed_expr_error("system attribute outside PL/pgSQL support")
                })?;
                Ok(slot.get_attr(index)?.clone())
            } else {
                Err(ExecError::UnboundOuterColumn {
                    depth: var.varlevelsup - 1,
                    index: attrno_index(var.varattno).unwrap_or(0),
                })
            }
        }
        Expr::Const(value) => Ok(value.clone()),
        Expr::Row { descriptor, fields } => Ok(Value::Record(
            crate::include::nodes::datum::RecordValue::from_descriptor(
                descriptor.clone(),
                fields
                    .iter()
                    .map(|(_, expr)| eval_plpgsql_expr(expr, slot))
                    .collect::<Result<Vec<_>, ExecError>>()?,
            ),
        )),
        Expr::FieldSelect { expr, field, .. } => {
            let value = eval_plpgsql_expr(expr, slot)?;
            eval_record_field(value, field)
        }
        Expr::Collate { expr, .. } => eval_plpgsql_expr(expr, slot),
        Expr::Case(case_expr) => {
            if case_expr.arg.is_some() {
                return Err(malformed_expr_error("CASE in PL/pgSQL"));
            }
            for when in &case_expr.args {
                match eval_plpgsql_expr(&when.expr, slot)? {
                    Value::Bool(true) => return eval_plpgsql_expr(&when.result, slot),
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            eval_plpgsql_expr(&case_expr.defresult, slot)
        }
        Expr::CaseTest(_) => Err(malformed_expr_error("CASE test in PL/pgSQL")),
        Expr::Cast(inner, ty) => cast_value_with_source_type_and_config(
            eval_plpgsql_expr(inner, slot)?,
            expr_sql_type_hint(inner),
            *ty,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
        Expr::Coalesce(left, right) => {
            let left = eval_plpgsql_expr(left, slot)?;
            if !matches!(left, Value::Null) {
                Ok(left)
            } else {
                eval_plpgsql_expr(right, slot)
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            collation_oid,
            case_insensitive,
            negated,
            ..
        } => {
            let left = eval_plpgsql_expr(expr, slot)?;
            let pattern = eval_plpgsql_expr(pattern, slot)?;
            let escape = match escape {
                Some(value) => Some(eval_plpgsql_expr(value, slot)?),
                None => None,
            };
            eval_like(
                &left,
                &pattern,
                escape.as_ref(),
                *collation_oid,
                *case_insensitive,
                *negated,
            )
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            collation_oid,
            negated,
            ..
        } => {
            let left = eval_plpgsql_expr(expr, slot)?;
            let pattern = eval_plpgsql_expr(pattern, slot)?;
            let escape = match escape {
                Some(value) => Some(eval_plpgsql_expr(value, slot)?),
                None => None,
            };
            eval_similar(&left, &pattern, escape.as_ref(), *collation_oid, *negated)
        }
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(
            eval_plpgsql_expr(inner, slot)?,
            Value::Null
        ))),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!matches!(
            eval_plpgsql_expr(inner, slot)?,
            Value::Null
        ))),
        Expr::IsDistinctFrom(left, right) => Ok(Value::Bool(values_are_distinct(
            &eval_plpgsql_expr(left, slot)?,
            &eval_plpgsql_expr(right, slot)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Value::Bool(!values_are_distinct(
            &eval_plpgsql_expr(left, slot)?,
            &eval_plpgsql_expr(right, slot)?,
        ))),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => {
            let element_type = array_type.element_type();
            let mut values = Vec::with_capacity(elements.len());
            let mut has_nested_arrays = false;
            for expr in elements {
                let value = eval_plpgsql_expr(expr, slot)?;
                if matches!(value, Value::Array(_) | Value::PgArray(_)) {
                    has_nested_arrays = true;
                    values.push(cast_value(value, *array_type)?);
                } else {
                    values.push(cast_value(value, element_type)?);
                }
            }
            if has_nested_arrays {
                let array = ArrayValue::from_nested_values(values, vec![1]).map_err(|details| {
                    ExecError::DetailedError {
                        message: "malformed array literal".into(),
                        detail: Some(details),
                        hint: None,
                        sqlstate: "22P02",
                    }
                })?;
                Ok(Value::PgArray(array))
            } else {
                Ok(Value::PgArray(ArrayValue::from_1d(values)))
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            let value = eval_plpgsql_expr(array, slot)?;
            eval_array_subscript_plpgsql(value, subscripts, slot)
        }
        Expr::CurrentUser
        | Expr::CurrentRole
        | Expr::SessionUser
        | Expr::CurrentCatalog
        | Expr::CurrentSchema => Err(ExecError::DetailedError {
            message: "SQL value functions are not supported in PL/pgSQL expression evaluation"
                .into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
        Expr::CurrentDate => Ok(current_date_value()),
        Expr::CurrentTime { precision } => Ok(current_time_value(*precision, true)),
        Expr::CurrentTimestamp { precision } => Ok(current_timestamp_value(*precision, true)),
        Expr::LocalTime { precision } => Ok(current_time_value(*precision, false)),
        Expr::LocalTimestamp { precision } => Ok(current_timestamp_value(*precision, false)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "plpgsql expression without subqueries or SQL statements",
            actual: format!("{expr:?}"),
        })),
    }
}

fn eval_record_field(value: Value, field: &str) -> Result<Value, ExecError> {
    let Value::Record(record) = value else {
        return Err(ExecError::DetailedError {
            message: format!("cannot select field \"{field}\" from non-record value"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    };
    record
        .iter()
        .find(|(desc, _)| desc.name.eq_ignore_ascii_case(field))
        .map(|(_, value)| value.clone())
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("record has no field \"{field}\""),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })
}

fn cast_record_value_for_target(
    record: crate::include::nodes::datum::RecordValue,
    target: SqlType,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(
        target.kind,
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
    ) && !target.is_array
    {
        return Ok(Value::Text(
            crate::backend::executor::value_io::format_record_text(&record).into(),
        ));
    }

    let descriptor = match target {
        SqlType {
            kind: SqlTypeKind::Composite,
            typrelid,
            ..
        } if typrelid != 0 => {
            let catalog = ctx
                .catalog
                .as_ref()
                .ok_or_else(|| ExecError::DetailedError {
                    message: "named composite casts require catalog context".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                })?;
            let relation = catalog.lookup_relation_by_oid(typrelid).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("unknown composite relation oid {typrelid}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
            crate::include::nodes::datum::RecordDescriptor::named(
                target.type_oid,
                target.typrelid,
                target.typmod,
                relation
                    .desc
                    .columns
                    .iter()
                    .filter(|column| !column.dropped)
                    .map(|column| (column.name.clone(), column.sql_type))
                    .collect(),
            )
        }
        SqlType {
            kind: SqlTypeKind::Record,
            typmod,
            ..
        } if typmod > 0 => crate::backend::utils::record::lookup_anonymous_record_descriptor(
            typmod,
        )
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("unknown anonymous record typmod {typmod}"),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })?,
        _ => return Ok(Value::Record(record)),
    };

    if descriptor.fields.len() != record.fields.len() {
        return Err(ExecError::DetailedError {
            message: "cannot cast record to target composite type".into(),
            detail: Some(format!(
                "target expects {} fields but source has {}",
                descriptor.fields.len(),
                record.fields.len()
            )),
            hint: None,
            sqlstate: "42804",
        });
    }

    let fields = record
        .fields
        .into_iter()
        .zip(descriptor.fields.iter())
        .map(|(value, field)| {
            cast_value_with_source_type_catalog_and_config(
                value,
                None,
                field.sql_type,
                ctx.catalog
                    .as_ref()
                    .map(|catalog| catalog as &dyn crate::backend::parser::CatalogLookup),
                &ctx.datetime_config,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Record(
        crate::include::nodes::datum::RecordValue::from_descriptor(descriptor, fields),
    ))
}

fn eval_plpgsql_builtin_function(
    func: BuiltinScalarFunction,
    result_type: Option<SqlType>,
    args: &[Expr],
    func_variadic: bool,
    slot: &mut TupleSlot,
) -> Result<Value, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_plpgsql_expr(arg, slot))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(result) = eval_geometry_function(func, &values) {
        return result;
    }
    if (result_type.is_some_and(SqlType::is_multirange)
        || values
            .iter()
            .any(|value| matches!(value, Value::Multirange(_))))
        && let Some(result) = eval_multirange_function(func, &values, result_type, func_variadic)
    {
        return result;
    }
    if let Some(result) = eval_range_function(func, &values, result_type, func_variadic) {
        return result;
    }
    if let Some(result) = crate::backend::executor::eval_network_function(func, &values) {
        return result;
    }
    match func {
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::JsonbToTsVector
        | BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => eval_text_search_builtin_function(func, &values),
        BuiltinScalarFunction::Length => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_length_function(&values),
        },
        BuiltinScalarFunction::ArrayUpper => eval_array_upper_function(&values),
        BuiltinScalarFunction::CashLarger => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_larger(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashlarger",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashSmaller => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_smaller(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashsmaller",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashWords => match values.as_slice() {
            [Value::Money(value)] => Ok(Value::Text(cash_words_text(*value).into())),
            _ => Err(ExecError::TypeMismatch {
                op: "cash_words",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::Int4Pl
        | BuiltinScalarFunction::Int8Inc
        | BuiltinScalarFunction::Int8IncAny
        | BuiltinScalarFunction::Int4AvgAccum
        | BuiltinScalarFunction::Int8Avg => {
            execute_builtin_scalar_function_value_call(func, &values)
        }
        BuiltinScalarFunction::CurrentSetting => eval_current_setting_without_context(&values),
        BuiltinScalarFunction::PgColumnCompression => eval_pg_column_compression_values(&values),
        BuiltinScalarFunction::PgColumnSize => eval_pg_column_size_values(&values),
        BuiltinScalarFunction::PgSizePretty => eval_pg_size_pretty_function(&values),
        BuiltinScalarFunction::PgSizeBytes => eval_pg_size_bytes_function(&values),
        BuiltinScalarFunction::Lower => eval_lower_function(&values),
        BuiltinScalarFunction::Unistr => eval_unistr_function(&values),
        BuiltinScalarFunction::Initcap => eval_initcap_function(&values),
        BuiltinScalarFunction::BTrim => eval_trim_function("btrim", &values),
        BuiltinScalarFunction::LTrim => eval_trim_function("ltrim", &values),
        BuiltinScalarFunction::RTrim => eval_trim_function("rtrim", &values),
        BuiltinScalarFunction::Concat => eval_concat_function(
            &values,
            func_variadic,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
        BuiltinScalarFunction::ConcatWs => eval_concat_ws_function(
            &values,
            func_variadic,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
        BuiltinScalarFunction::Format => eval_format_function(
            &values,
            func_variadic,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
        BuiltinScalarFunction::Left => eval_left_function(&values),
        BuiltinScalarFunction::Right => eval_right_function(&values),
        BuiltinScalarFunction::LPad => eval_lpad_function(&values),
        BuiltinScalarFunction::RPad => eval_rpad_function(&values),
        BuiltinScalarFunction::Repeat => eval_repeat_function(&values),
        BuiltinScalarFunction::Replace => eval_replace_function(&values),
        BuiltinScalarFunction::SplitPart => eval_split_part_function(&values),
        BuiltinScalarFunction::Translate => eval_translate_function(&values),
        BuiltinScalarFunction::Ascii => eval_ascii_function(&values),
        BuiltinScalarFunction::Chr => eval_chr_function(&values),
        BuiltinScalarFunction::ParseIdent => eval_parse_ident_function(&values),
        BuiltinScalarFunction::TextToRegClass => eval_text_to_regclass_function(&values, None),
        BuiltinScalarFunction::ToRegProc => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegProc, None)
        }
        BuiltinScalarFunction::ToRegProcedure => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegProcedure, None)
        }
        BuiltinScalarFunction::ToRegOper => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegOper, None)
        }
        BuiltinScalarFunction::ToRegOperator => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegOperator, None)
        }
        BuiltinScalarFunction::ToRegClass => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegClass, None)
        }
        BuiltinScalarFunction::ToRegType => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegType, None)
        }
        BuiltinScalarFunction::ToRegTypeMod => eval_to_regtypemod_function(&values, None),
        BuiltinScalarFunction::ToRegRole => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegRole, None)
        }
        BuiltinScalarFunction::ToRegNamespace => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegNamespace, None)
        }
        BuiltinScalarFunction::ToRegCollation => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegCollation, None)
        }
        BuiltinScalarFunction::FormatType => eval_format_type_function(&values, None),
        BuiltinScalarFunction::RegProcToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegProc, None)
        }
        BuiltinScalarFunction::RegClassToText => eval_regclass_to_text_function(&values, None),
        BuiltinScalarFunction::RegTypeToText => eval_regtype_to_text_function(&values, None),
        BuiltinScalarFunction::RegRoleToText => eval_regrole_to_text_function(&values, None),
        BuiltinScalarFunction::RegOperToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegOper, None)
        }
        BuiltinScalarFunction::RegOperatorToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegOperator, None)
        }
        BuiltinScalarFunction::RegProcedureToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegProcedure, None)
        }
        BuiltinScalarFunction::RegCollationToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegCollation, None)
        }
        BuiltinScalarFunction::QuoteLiteral => eval_quote_literal_function(&values),
        BuiltinScalarFunction::BpcharToText => eval_bpchar_to_text_function(&values),
        BuiltinScalarFunction::Strpos => eval_strpos_function(&values),
        BuiltinScalarFunction::Position => match values.as_slice() {
            [Value::Bit(needle), Value::Bit(haystack)] => {
                Ok(Value::Int32(eval_bit_position(needle, haystack)))
            }
            [Value::Bytea(_), Value::Bytea(_)] => eval_bytea_position_function(&values),
            _ => eval_position_function(&values),
        },
        BuiltinScalarFunction::Substring => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, None)?))
            }
            [Value::Bit(bits), Value::Int32(start), Value::Int32(len)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, Some(*len))?))
            }
            [Value::Bytea(_), Value::Int32(_)]
            | [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_bytea_substring(&values),
            [Value::Text(_), Value::Text(_)] => eval_sql_regex_substring(&values),
            _ => eval_text_substring(&values),
        },
        BuiltinScalarFunction::SimilarSubstring => eval_similar_substring(&values),
        BuiltinScalarFunction::Overlay => match values.as_slice() {
            [Value::Bit(bits), Value::Bit(place), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_overlay(bits, place, *start, None)?))
            }
            [
                Value::Bit(bits),
                Value::Bit(place),
                Value::Int32(start),
                Value::Int32(len),
            ] => Ok(Value::Bit(eval_bit_overlay(
                bits,
                place,
                *start,
                Some(*len),
            )?)),
            [Value::Bytea(_), Value::Bytea(_), Value::Int32(_)]
            | [
                Value::Bytea(_),
                Value::Bytea(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_bytea_overlay(&values),
            [Value::Text(_), Value::Text(_), Value::Int32(_)]
            | [
                Value::Text(_),
                Value::Text(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_text_overlay(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::GetBit => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(index)] => {
                Ok(Value::Int32(eval_get_bit(bits, *index)?))
            }
            [Value::Bytea(_), Value::Int32(_)] => eval_get_bit_bytes(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::SetBit => match values.as_slice() {
            [
                Value::Bit(bits),
                Value::Int32(index),
                Value::Int32(new_value),
            ] => Ok(Value::Bit(eval_set_bit(bits, *index, *new_value)?)),
            [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_set_bit_bytes(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::BitCount => match values.as_slice() {
            [Value::Bit(bits)] => Ok(Value::Int64(eval_bit_count(bits))),
            [Value::Bytea(_)] => eval_bit_count_bytes(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::GetByte => eval_get_byte(&values),
        BuiltinScalarFunction::SetByte => eval_set_byte(&values),
        BuiltinScalarFunction::ConvertFrom => eval_convert_from_function(&values),
        BuiltinScalarFunction::Md5 => eval_md5_function(&values),
        BuiltinScalarFunction::Reverse => eval_reverse_function(&values),
        BuiltinScalarFunction::Encode => eval_encode_function(&values),
        BuiltinScalarFunction::Decode => eval_decode_function(&values),
        BuiltinScalarFunction::Sha224 => eval_sha224_function(&values),
        BuiltinScalarFunction::Sha256 => eval_sha256_function(&values),
        BuiltinScalarFunction::Sha384 => eval_sha384_function(&values),
        BuiltinScalarFunction::Sha512 => eval_sha512_function(&values),
        BuiltinScalarFunction::Crc32 => eval_crc32_function(&values),
        BuiltinScalarFunction::Crc32c => eval_crc32c_function(&values),
        BuiltinScalarFunction::ToBin => eval_to_bin_function(&values),
        BuiltinScalarFunction::ToOct => eval_to_oct_function(&values),
        BuiltinScalarFunction::ToHex => eval_to_hex_function(&values),
        BuiltinScalarFunction::RegexpMatch => eval_regexp_match(&values),
        BuiltinScalarFunction::RegexpLike => eval_regexp_like(&values),
        BuiltinScalarFunction::RegexpReplace => eval_regexp_replace(&values),
        BuiltinScalarFunction::RegexpCount => eval_regexp_count(&values),
        BuiltinScalarFunction::RegexpInstr => eval_regexp_instr(&values),
        BuiltinScalarFunction::RegexpSubstr => eval_regexp_substr(&values),
        BuiltinScalarFunction::RegexpSplitToArray => eval_regexp_split_to_array(&values),
        BuiltinScalarFunction::ToChar => eval_to_char_function(&values),
        BuiltinScalarFunction::ToDate => eval_to_date_function(&values),
        BuiltinScalarFunction::ToNumber => eval_to_number_function(&values),
        BuiltinScalarFunction::Abs => eval_abs_function(&values),
        BuiltinScalarFunction::Gcd => eval_gcd_function(&values),
        BuiltinScalarFunction::Lcm => eval_lcm_function(&values),
        BuiltinScalarFunction::ArrayNdims => eval_array_ndims_function(&values),
        BuiltinScalarFunction::ArrayDims => eval_array_dims_function(&values),
        BuiltinScalarFunction::ArrayLower => eval_array_lower_function(&values),
        BuiltinScalarFunction::ArrayFill => eval_array_fill_function(&values),
        BuiltinScalarFunction::StringToArray => eval_string_to_array_function(&values),
        BuiltinScalarFunction::ArrayToString => eval_array_to_string_function(&values),
        BuiltinScalarFunction::ArrayLength => eval_array_length_function(&values),
        BuiltinScalarFunction::Cardinality => eval_cardinality_function(&values),
        BuiltinScalarFunction::ArrayAppend => eval_array_append_function(&values),
        BuiltinScalarFunction::ArrayPrepend => eval_array_prepend_function(&values),
        BuiltinScalarFunction::ArrayCat => eval_array_cat_function(&values),
        BuiltinScalarFunction::ArrayPosition => eval_array_position_function(&values),
        BuiltinScalarFunction::ArrayPositions => eval_array_positions_function(&values),
        BuiltinScalarFunction::ArrayRemove => eval_array_remove_function(&values),
        BuiltinScalarFunction::ArrayReplace => eval_array_replace_function(&values),
        BuiltinScalarFunction::ArraySort => eval_array_sort_function(&values),
        BuiltinScalarFunction::BoolEq => eval_booleq(&values),
        BuiltinScalarFunction::BoolNe => eval_boolne(&values),
        BuiltinScalarFunction::BoolAndStateFunc => eval_booland_statefunc(&values),
        BuiltinScalarFunction::BoolOrStateFunc => eval_boolor_statefunc(&values),
        BuiltinScalarFunction::XmlComment => eval_xml_comment_function(&values, None),
        BuiltinScalarFunction::XmlIsWellFormed => eval_xml_is_well_formed_function(
            &values,
            crate::backend::utils::misc::guc_xml::XmlOptionSetting::Content,
            None,
        ),
        BuiltinScalarFunction::XmlIsWellFormedDocument => eval_xml_is_well_formed_function(
            &values,
            crate::backend::utils::misc::guc_xml::XmlOptionSetting::Document,
            None,
        ),
        BuiltinScalarFunction::XmlIsWellFormedContent => eval_xml_is_well_formed_function(
            &values,
            crate::backend::utils::misc::guc_xml::XmlOptionSetting::Content,
            None,
        ),
        BuiltinScalarFunction::TsMatch => match values.as_slice() {
            [Value::TsVector(vector), Value::TsQuery(query)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsvector_matches_tsquery(vector, query),
            )),
            [Value::TsQuery(query), Value::TsVector(vector)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsquery_matches_tsvector(query, vector),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "@@",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryAnd => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_and(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "&&",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryOr => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_or(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryNot => match values.as_slice() {
            [Value::TsQuery(query)] => Ok(Value::TsQuery(crate::backend::executor::tsquery_not(
                query.clone(),
            ))),
            _ => Err(ExecError::TypeMismatch {
                op: "!!",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::TsVectorConcat => match values.as_slice() {
            [Value::TsVector(left), Value::TsVector(right)] => Ok(Value::TsVector(
                crate::backend::executor::concat_tsvector(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::BitcastIntegerToFloat4 => eval_bitcast_integer_to_float4(&values),
        BuiltinScalarFunction::BitcastBigintToFloat8 => eval_bitcast_bigint_to_float8(&values),
        BuiltinScalarFunction::Random
        | BuiltinScalarFunction::GetDatabaseEncoding
        | BuiltinScalarFunction::PgGetAcl
        | BuiltinScalarFunction::PgGetUserById
        | BuiltinScalarFunction::ObjDescription
        | BuiltinScalarFunction::PgDescribeObject
        | BuiltinScalarFunction::PgGetExpr
        | BuiltinScalarFunction::PgGetStatisticsObjDef
        | BuiltinScalarFunction::PgGetStatisticsObjDefColumns
        | BuiltinScalarFunction::PgGetStatisticsObjDefExpressions
        | BuiltinScalarFunction::PgStatisticsObjIsVisible
        | BuiltinScalarFunction::PgRelationIsPublishable
        | BuiltinScalarFunction::PgIndexAmHasProperty
        | BuiltinScalarFunction::PgIndexHasProperty
        | BuiltinScalarFunction::PgIndexColumnHasProperty
        | BuiltinScalarFunction::ToJson
        | BuiltinScalarFunction::ToJsonb
        | BuiltinScalarFunction::ArrayToJson
        | BuiltinScalarFunction::RowToJson
        | BuiltinScalarFunction::JsonBuildArray
        | BuiltinScalarFunction::JsonBuildObject
        | BuiltinScalarFunction::JsonObject
        | BuiltinScalarFunction::JsonTypeof
        | BuiltinScalarFunction::JsonArrayLength
        | BuiltinScalarFunction::JsonExtractPath
        | BuiltinScalarFunction::JsonExtractPathText
        | BuiltinScalarFunction::JsonbTypeof
        | BuiltinScalarFunction::JsonbArrayLength
        | BuiltinScalarFunction::JsonbExtractPath
        | BuiltinScalarFunction::JsonbExtractPathText
        | BuiltinScalarFunction::JsonbBuildArray
        | BuiltinScalarFunction::JsonbBuildObject
        | BuiltinScalarFunction::JsonbPathExists
        | BuiltinScalarFunction::JsonbPathMatch
        | BuiltinScalarFunction::Trunc
        | BuiltinScalarFunction::Round
        | BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Ceiling
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Sign
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Cbrt
        | BuiltinScalarFunction::Power
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Ln
        | BuiltinScalarFunction::Sinh
        | BuiltinScalarFunction::Cosh
        | BuiltinScalarFunction::Tanh
        | BuiltinScalarFunction::Asinh
        | BuiltinScalarFunction::Acosh
        | BuiltinScalarFunction::Atanh
        | BuiltinScalarFunction::Sind
        | BuiltinScalarFunction::Cosd
        | BuiltinScalarFunction::Tand
        | BuiltinScalarFunction::Cotd
        | BuiltinScalarFunction::Asind
        | BuiltinScalarFunction::Acosd
        | BuiltinScalarFunction::Atand
        | BuiltinScalarFunction::Atan2d
        | BuiltinScalarFunction::Erf
        | BuiltinScalarFunction::Erfc
        | BuiltinScalarFunction::Gamma
        | BuiltinScalarFunction::Lgamma
        | BuiltinScalarFunction::Float4Send
        | BuiltinScalarFunction::Float8Send
        | BuiltinScalarFunction::Float8Accum
        | BuiltinScalarFunction::Float8Combine
        | BuiltinScalarFunction::Float8RegrAccum
        | BuiltinScalarFunction::Float8RegrCombine => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            }))
        }
        _ => {
            let _ = func_variadic;
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            }))
        }
    }
}

fn eval_text_search_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Result<Value, ExecError> {
    fn arg_text(
        values: &[Value],
        index: usize,
        op: &'static str,
    ) -> Result<Option<String>, ExecError> {
        let Some(value) = values.get(index) else {
            return Ok(None);
        };
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        value
            .as_text()
            .map(|text| Some(text.to_string()))
            .ok_or_else(|| ExecError::TypeMismatch {
                op,
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            })
    }

    let parse_error = |op: &'static str, message: String| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid text search input",
            actual: format!("{op}: {message}"),
        })
    };

    match func {
        BuiltinScalarFunction::ToTsVector => {
            if let [Value::Jsonb(_)] = values {
                return jsonb_to_tsvector_value(None, &values[0], None);
            }
            if let [_, Value::Jsonb(_)] = values {
                return jsonb_to_tsvector_value(
                    arg_text(values, 0, "to_tsvector")?.as_deref(),
                    &values[1],
                    None,
                );
            }
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::to_tsvector_with_config_name(
                    None,
                    arg_text(values, 0, "to_tsvector")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::to_tsvector_with_config_name(
                    arg_text(values, 0, "to_tsvector")?.as_deref(),
                    arg_text(values, 1, "to_tsvector")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsVector)
                .map_err(|e| parse_error("to_tsvector", e))
        }
        BuiltinScalarFunction::JsonbToTsVector => match values {
            [Value::Null, _]
            | [_, Value::Null]
            | [Value::Null, _, _]
            | [_, Value::Null, _]
            | [_, _, Value::Null] => {
                return Ok(Value::Null);
            }
            [Value::Jsonb(_), _] => jsonb_to_tsvector_value(None, &values[0], values.get(1)),
            [_, Value::Jsonb(_), _] => jsonb_to_tsvector_value(
                arg_text(values, 0, "jsonb_to_tsvector")?.as_deref(),
                &values[1],
                values.get(2),
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "jsonb_to_tsvector",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::ToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::to_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::to_tsquery_with_config_name(
                    arg_text(values, 0, "to_tsquery")?.as_deref(),
                    arg_text(values, 1, "to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("to_tsquery", e))
        }
        BuiltinScalarFunction::PlainToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::plainto_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "plainto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::plainto_tsquery_with_config_name(
                    arg_text(values, 0, "plainto_tsquery")?.as_deref(),
                    arg_text(values, 1, "plainto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("plainto_tsquery", e))
        }
        BuiltinScalarFunction::PhraseToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::phraseto_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "phraseto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::phraseto_tsquery_with_config_name(
                    arg_text(values, 0, "phraseto_tsquery")?.as_deref(),
                    arg_text(values, 1, "phraseto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("phraseto_tsquery", e))
        }
        BuiltinScalarFunction::WebSearchToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::websearch_to_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "websearch_to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::websearch_to_tsquery_with_config_name(
                    arg_text(values, 0, "websearch_to_tsquery")?.as_deref(),
                    arg_text(values, 1, "websearch_to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("websearch_to_tsquery", e))
        }
        BuiltinScalarFunction::TsLexize => match values {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [_, _] => crate::backend::tsearch::ts_lexize_with_dictionary_name(
                arg_text(values, 0, "ts_lexize")?
                    .as_deref()
                    .unwrap_or_default(),
                arg_text(values, 1, "ts_lexize")?
                    .as_deref()
                    .unwrap_or_default(),
            )
            .map(|lexemes| {
                Value::Array(
                    lexemes
                        .into_iter()
                        .map(|lexeme| Value::Text(lexeme.into()))
                        .collect(),
                )
            })
            .map_err(|e| parse_error("ts_lexize", e)),
            _ => unreachable!(),
        },
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "text search builtin function",
            actual: format!("{func:?}"),
        })),
    }
}

fn eval_float8_accum_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [state, newval] => {
            let state = expect_float8_transition_state("float8_accum", state, 3)?;
            let newval = expect_float8_arg("float8_accum", newval)?;
            let [count, sum, sum_sq] = float8_accum_state(state[0], state[1], state[2], newval)?;
            Ok(encode_float8_transition_state([count, sum, sum_sq]))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "float8_accum(state, value)",
            actual: format!("{} args", values.len()),
        })),
    }
}

fn eval_float8_combine_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left = expect_float8_transition_state("float8_combine", left, 3)?;
            let right = expect_float8_transition_state("float8_combine", right, 3)?;
            let [count, sum, sum_sq] =
                float8_combine_state(left[0], left[1], left[2], right[0], right[1], right[2])?;
            Ok(encode_float8_transition_state([count, sum, sum_sq]))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "float8_combine(state1, state2)",
            actual: format!("{} args", values.len()),
        })),
    }
}

fn eval_float8_regr_accum_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [state, y, x] => {
            let state = expect_float8_transition_state("float8_regr_accum", state, 6)?;
            let y = expect_float8_arg("float8_regr_accum", y)?;
            let x = expect_float8_arg("float8_regr_accum", x)?;
            let [count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy] = float8_regr_accum_state(
                state[0], state[1], state[2], state[3], state[4], state[5], y, x,
            )?;
            Ok(encode_float8_transition_state([
                count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy,
            ]))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "float8_regr_accum(state, y, x)",
            actual: format!("{} args", values.len()),
        })),
    }
}

fn eval_float8_regr_combine_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left = expect_float8_transition_state("float8_regr_combine", left, 6)?;
            let right = expect_float8_transition_state("float8_regr_combine", right, 6)?;
            let [count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy] = float8_regr_combine_state(
                [left[0], left[1], left[2], left[3], left[4], left[5]],
                [right[0], right[1], right[2], right[3], right[4], right[5]],
            )?;
            Ok(encode_float8_transition_state([
                count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy,
            ]))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "float8_regr_combine(state1, state2)",
            actual: format!("{} args", values.len()),
        })),
    }
}

fn expect_float8_transition_state(
    op: &'static str,
    value: &Value,
    expected_len: usize,
) -> Result<Vec<f64>, ExecError> {
    let array = value
        .as_array_value()
        .ok_or_else(|| ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::PgArray(ArrayValue::empty().with_element_type_oid(FLOAT8_TYPE_OID)),
        })?;
    if array.dimensions.len() != 1 || array.dimensions[0].length != expected_len {
        return Err(ExecError::DetailedError {
            message: format!("{op} requires a float8[] transition state of length {expected_len}"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    array
        .elements
        .iter()
        .map(|element| expect_float8_arg(op, element))
        .collect()
}

pub(crate) fn expect_float8_arg(op: &'static str, value: &Value) -> Result<f64, ExecError> {
    match value {
        Value::Int16(v) => Ok(f64::from(*v)),
        Value::Int32(v) => Ok(f64::from(*v)),
        Value::Int64(v) => Ok(*v as f64),
        Value::Float64(v) => Ok(*v),
        Value::Numeric(numeric) => match numeric {
            NumericValue::PosInf => Ok(f64::INFINITY),
            NumericValue::NegInf => Ok(f64::NEG_INFINITY),
            NumericValue::NaN => Ok(f64::NAN),
            NumericValue::Finite { coeff, scale, .. } => {
                let coeff = coeff.to_f64().ok_or_else(|| ExecError::TypeMismatch {
                    op,
                    left: value.clone(),
                    right: Value::Float64(0.0),
                })?;
                Ok(coeff / 10f64.powi(*scale as i32))
            }
        },
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Float64(0.0),
        }),
    }
}

fn encode_float8_transition_state<const N: usize>(values: [f64; N]) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(values.into_iter().map(Value::Float64).collect())
            .with_element_type_oid(FLOAT8_TYPE_OID),
    )
}

fn float8_accum_state(
    prev_count: f64,
    prev_sum: f64,
    mut prev_sum_sq: f64,
    newval: f64,
) -> Result<[f64; 3], ExecError> {
    let count = prev_count + 1.0;
    let sum = prev_sum + newval;
    if prev_count > 0.0 {
        let tmp = newval * count - sum;
        prev_sum_sq += tmp * tmp / (count * prev_count);
        if sum.is_infinite() || prev_sum_sq.is_infinite() {
            if !prev_sum.is_infinite() && !newval.is_infinite() {
                return Err(float8_overflow_error());
            }
            prev_sum_sq = f64::NAN;
        }
    } else if newval.is_nan() || newval.is_infinite() {
        prev_sum_sq = f64::NAN;
    }
    Ok([count, sum, prev_sum_sq])
}

fn float8_combine_state(
    count1: f64,
    sum1: f64,
    sum_sq1: f64,
    count2: f64,
    sum2: f64,
    sum_sq2: f64,
) -> Result<[f64; 3], ExecError> {
    if count1 == 0.0 {
        return Ok([count2, sum2, sum_sq2]);
    }
    if count2 == 0.0 {
        return Ok([count1, sum1, sum_sq1]);
    }
    let count = count1 + count2;
    let sum = sum1 + sum2;
    let tmp = sum1 / count1 - sum2 / count2;
    let sum_sq = sum_sq1 + sum_sq2 + count1 * count2 * tmp * tmp / count;
    if sum_sq.is_infinite() && !sum_sq1.is_infinite() && !sum_sq2.is_infinite() {
        return Err(float8_overflow_error());
    }
    Ok([count, sum, sum_sq])
}

pub(crate) fn float8_regr_accum_state(
    prev_count: f64,
    prev_sum_x: f64,
    mut prev_sum_sq_x: f64,
    prev_sum_y: f64,
    mut prev_sum_sq_y: f64,
    mut prev_sum_xy: f64,
    new_y: f64,
    new_x: f64,
) -> Result<[f64; 6], ExecError> {
    let count = prev_count + 1.0;
    let sum_x = prev_sum_x + new_x;
    let sum_y = prev_sum_y + new_y;
    if prev_count > 0.0 {
        let tmp_x = new_x * count - sum_x;
        let tmp_y = new_y * count - sum_y;
        let scale = 1.0 / (count * prev_count);
        prev_sum_sq_x += tmp_x * tmp_x * scale;
        prev_sum_sq_y += tmp_y * tmp_y * scale;
        prev_sum_xy += tmp_x * tmp_y * scale;
        if sum_x.is_infinite()
            || prev_sum_sq_x.is_infinite()
            || sum_y.is_infinite()
            || prev_sum_sq_y.is_infinite()
            || prev_sum_xy.is_infinite()
        {
            if ((sum_x.is_infinite() || prev_sum_sq_x.is_infinite())
                && !prev_sum_x.is_infinite()
                && !new_x.is_infinite())
                || ((sum_y.is_infinite() || prev_sum_sq_y.is_infinite())
                    && !prev_sum_y.is_infinite()
                    && !new_y.is_infinite())
                || (prev_sum_xy.is_infinite()
                    && !prev_sum_x.is_infinite()
                    && !new_x.is_infinite()
                    && !prev_sum_y.is_infinite()
                    && !new_y.is_infinite())
            {
                return Err(float8_overflow_error());
            }
            if prev_sum_sq_x.is_infinite() {
                prev_sum_sq_x = f64::NAN;
            }
            if prev_sum_sq_y.is_infinite() {
                prev_sum_sq_y = f64::NAN;
            }
            if prev_sum_xy.is_infinite() {
                prev_sum_xy = f64::NAN;
            }
        }
    } else {
        if new_x.is_nan() || new_x.is_infinite() {
            prev_sum_sq_x = f64::NAN;
            prev_sum_xy = f64::NAN;
        }
        if new_y.is_nan() || new_y.is_infinite() {
            prev_sum_sq_y = f64::NAN;
            prev_sum_xy = f64::NAN;
        }
    }
    Ok([
        count,
        sum_x,
        prev_sum_sq_x,
        sum_y,
        prev_sum_sq_y,
        prev_sum_xy,
    ])
}

fn float8_regr_combine_state(left: [f64; 6], right: [f64; 6]) -> Result<[f64; 6], ExecError> {
    let [count1, sum_x1, sum_sq_x1, sum_y1, sum_sq_y1, sum_xy1] = left;
    let [count2, sum_x2, sum_sq_x2, sum_y2, sum_sq_y2, sum_xy2] = right;
    if count1 == 0.0 {
        return Ok(right);
    }
    if count2 == 0.0 {
        return Ok(left);
    }
    let count = count1 + count2;
    let sum_x = sum_x1 + sum_x2;
    let sum_y = sum_y1 + sum_y2;
    let tmp_x = sum_x1 / count1 - sum_x2 / count2;
    let tmp_y = sum_y1 / count1 - sum_y2 / count2;
    let sum_sq_x = sum_sq_x1 + sum_sq_x2 + count1 * count2 * tmp_x * tmp_x / count;
    let sum_sq_y = sum_sq_y1 + sum_sq_y2 + count1 * count2 * tmp_y * tmp_y / count;
    let sum_xy = sum_xy1 + sum_xy2 + count1 * count2 * tmp_x * tmp_y / count;
    if (sum_sq_x.is_infinite() && !sum_sq_x1.is_infinite() && !sum_sq_x2.is_infinite())
        || (sum_sq_y.is_infinite() && !sum_sq_y1.is_infinite() && !sum_sq_y2.is_infinite())
        || (sum_xy.is_infinite() && !sum_xy1.is_infinite() && !sum_xy2.is_infinite())
    {
        return Err(float8_overflow_error());
    }
    Ok([count, sum_x, sum_sq_x, sum_y, sum_sq_y, sum_xy])
}

fn float8_overflow_error() -> ExecError {
    ExecError::DetailedError {
        message: "value out of range: overflow".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

fn pg_version_text() -> String {
    format!("PostgreSQL-compatible pgrust {}", env!("CARGO_PKG_VERSION"))
}

fn eval_builtin_function(
    func: BuiltinScalarFunction,
    result_type: Option<SqlType>,
    args: &[Expr],
    func_variadic: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    ensure_builtin_side_effects_allowed(func, ctx)?;
    if let Some(result) = eval_json_record_builtin_function(func, result_type, args, slot, ctx) {
        return result;
    }
    if matches!(func, BuiltinScalarFunction::PgColumnCompression)
        && let [Expr::Var(var)] = args
        && var.varlevelsup == 0
        && (var.varno == OUTER_VAR || !is_executor_special_varno(var.varno))
        && var.varattno > 0
        && let Some(index) = attrno_index(var.varattno)
        && let Some(raw) = current_slot_raw_attr_bytes(slot, index)?
    {
        return eval_pg_column_compression_raw(raw);
    }
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    if matches!(func, BuiltinScalarFunction::ToChar)
        && matches!(
            args.first(),
            Some(Expr::Cast(
                _,
                SqlType {
                    kind: SqlTypeKind::Float4,
                    ..
                }
            ))
        )
    {
        return eval_to_char_float4_function(&values);
    }
    if let Some(result) = eval_geometry_function(func, &values) {
        return result;
    }
    if (result_type.is_some_and(SqlType::is_multirange)
        || values
            .iter()
            .any(|value| matches!(value, Value::Multirange(_))))
        && let Some(result) = eval_multirange_function(func, &values, result_type, func_variadic)
    {
        return result;
    }
    if let Some(result) = eval_range_function(func, &values, result_type, func_variadic) {
        return result;
    }
    if let Some(result) = crate::backend::executor::eval_network_function(func, &values) {
        return result;
    }
    if let Some(result) = eval_json_builtin_function(
        func,
        &values,
        func_variadic,
        &ctx.datetime_config,
        ctx.catalog.as_ref(),
    ) {
        return result;
    }
    if matches!(func, BuiltinScalarFunction::PgNotify) {
        return eval_pg_notify_function(&values, ctx);
    }
    if matches!(func, BuiltinScalarFunction::PgNotificationQueueUsage) {
        return Ok(eval_pg_notification_queue_usage_function(ctx));
    }
    if matches!(
        func,
        BuiltinScalarFunction::NextVal
            | BuiltinScalarFunction::CurrVal
            | BuiltinScalarFunction::SetVal
            | BuiltinScalarFunction::PgGetSerialSequence
    ) {
        return eval_sequence_builtin_function(func, &values, ctx);
    }
    if let Some(result) = eval_advisory_lock_builtin_function(func, &values, ctx) {
        return result;
    }
    if matches!(
        func,
        BuiltinScalarFunction::LoCreate | BuiltinScalarFunction::LoUnlink
    ) {
        return eval_large_object_builtin_function(func, &values, ctx);
    }
    match func {
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::JsonbToTsVector
        | BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => eval_text_search_builtin_function(func, &values),
        BuiltinScalarFunction::Random => eval_random_function(&values),
        BuiltinScalarFunction::RandomNormal => eval_random_normal_function(&values),
        BuiltinScalarFunction::TxidCurrent
        | BuiltinScalarFunction::TxidCurrentIfAssigned
        | BuiltinScalarFunction::TxidVisibleInSnapshot => {
            eval_txid_builtin_function(func, &values, ctx)
        }
        BuiltinScalarFunction::UuidIn
        | BuiltinScalarFunction::UuidOut
        | BuiltinScalarFunction::UuidRecv
        | BuiltinScalarFunction::UuidSend
        | BuiltinScalarFunction::UuidEq
        | BuiltinScalarFunction::UuidNe
        | BuiltinScalarFunction::UuidLt
        | BuiltinScalarFunction::UuidLe
        | BuiltinScalarFunction::UuidGt
        | BuiltinScalarFunction::UuidGe
        | BuiltinScalarFunction::UuidCmp
        | BuiltinScalarFunction::UuidHash
        | BuiltinScalarFunction::UuidHashExtended
        | BuiltinScalarFunction::GenRandomUuid
        | BuiltinScalarFunction::UuidV7
        | BuiltinScalarFunction::UuidExtractVersion
        | BuiltinScalarFunction::UuidExtractTimestamp => eval_uuid_function(func, &values),
        BuiltinScalarFunction::CashLarger => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_larger(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashlarger",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashSmaller => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_smaller(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashsmaller",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashWords => match values.as_slice() {
            [Value::Money(value)] => Ok(Value::Text(cash_words_text(*value).into())),
            _ => Err(ExecError::TypeMismatch {
                op: "cash_words",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::Int4Pl
        | BuiltinScalarFunction::Int8Inc
        | BuiltinScalarFunction::Int8IncAny
        | BuiltinScalarFunction::Int4AvgAccum
        | BuiltinScalarFunction::Int8Avg => {
            execute_builtin_scalar_function_value_call(func, &values)
        }
        BuiltinScalarFunction::RegProcToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegProc, Some(ctx))
        }
        BuiltinScalarFunction::RegOperToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegOper, Some(ctx))
        }
        BuiltinScalarFunction::RegOperatorToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegOperator, Some(ctx))
        }
        BuiltinScalarFunction::RegProcedureToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegProcedure, Some(ctx))
        }
        BuiltinScalarFunction::RegCollationToText => {
            eval_reg_object_to_text(&values[0], SqlTypeKind::RegCollation, Some(ctx))
        }
        BuiltinScalarFunction::PgGetUserById => eval_pg_get_userbyid(&values, ctx),
        BuiltinScalarFunction::PgGetAcl => eval_pg_get_acl(&values, ctx),
        BuiltinScalarFunction::PgGetStatisticsObjDef => eval_pg_get_statisticsobjdef(&values, ctx),
        BuiltinScalarFunction::PgGetStatisticsObjDefColumns => {
            eval_pg_get_statisticsobjdef_columns(&values, ctx)
        }
        BuiltinScalarFunction::PgGetStatisticsObjDefExpressions => {
            eval_pg_get_statisticsobjdef_expressions(&values, ctx)
        }
        BuiltinScalarFunction::PgStatisticsObjIsVisible => {
            eval_pg_statistics_obj_is_visible(&values, ctx)
        }
        BuiltinScalarFunction::Now
        | BuiltinScalarFunction::TransactionTimestamp
        | BuiltinScalarFunction::StatementTimestamp => {
            Ok(current_timestamp_value_from_timestamp_with_config(
                &ctx.datetime_config,
                ctx.statement_timestamp_usecs,
                None,
                true,
            ))
        }
        BuiltinScalarFunction::ClockTimestamp => Ok(current_timestamp_value_with_config(
            &ctx.datetime_config,
            None,
            true,
        )),
        BuiltinScalarFunction::TimeOfDay => {
            let value = current_timestamp_value_with_config(&ctx.datetime_config, None, true);
            Ok(Value::Text(
                render_datetime_value_text_with_config(&value, &ctx.datetime_config)
                    .unwrap_or_else(render_current_timestamp)
                    .into(),
            ))
        }
        BuiltinScalarFunction::NextVal
        | BuiltinScalarFunction::CurrVal
        | BuiltinScalarFunction::SetVal
        | BuiltinScalarFunction::PgGetSerialSequence => {
            unreachable!("sequence builtins handled earlier");
        }
        BuiltinScalarFunction::DatePart => eval_date_part_function(&values),
        BuiltinScalarFunction::DateTrunc => eval_date_trunc_function(&values, &ctx.datetime_config),
        BuiltinScalarFunction::TimeZone => eval_timezone_function(&values, &ctx.datetime_config),
        BuiltinScalarFunction::IsFinite => eval_isfinite_function(&values),
        BuiltinScalarFunction::MakeDate => eval_make_date_function(&values),
        BuiltinScalarFunction::MakeTime => eval_make_time_function(&values),
        BuiltinScalarFunction::GetDatabaseEncoding => Ok(Value::Text("UTF8".into())),
        BuiltinScalarFunction::PgMyTempSchema => Ok(Value::Int64(i64::from(
            current_temp_namespace_oid(ctx).unwrap_or(0),
        ))),
        BuiltinScalarFunction::PgRustInternalBinaryCoercible => {
            eval_pg_rust_internal_binary_coercible(&values)
        }
        BuiltinScalarFunction::PgRustTestOpclassOptionsFunc => {
            eval_pg_rust_test_opclass_options_func(&values)
        }
        BuiltinScalarFunction::PgRustTestFdwHandler => eval_pg_rust_test_fdw_handler(&values),
        BuiltinScalarFunction::PgRustTestEncSetup => eval_pg_rust_test_enc_setup(&values),
        BuiltinScalarFunction::PgRustTestEncConversion => eval_pg_rust_test_enc_conversion(&values),
        BuiltinScalarFunction::CurrentSetting => eval_current_setting(&values, ctx),
        BuiltinScalarFunction::PgNotify => unreachable!("pg_notify handled earlier"),
        BuiltinScalarFunction::PgNotificationQueueUsage => {
            unreachable!("pg_notification_queue_usage handled earlier")
        }
        BuiltinScalarFunction::PgStatGetCheckpointerNumTimed
        | BuiltinScalarFunction::PgStatGetCheckpointerNumRequested
        | BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed
        | BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten
        | BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten
        | BuiltinScalarFunction::PgStatGetCheckpointerWriteTime
        | BuiltinScalarFunction::PgStatGetCheckpointerSyncTime
        | BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime => {
            Ok(checkpoint_stats_value(func, &ctx.checkpoint_stats)
                .expect("checkpoint stats builtin must map to a value"))
        }
        BuiltinScalarFunction::PgStatForceNextFlush => {
            ctx.session_stats.write().flush_pending(&ctx.stats);
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatGetSnapshotTimestamp => Ok(ctx
            .session_stats
            .read()
            .snapshot_timestamp()
            .map(Value::TimestampTz)
            .unwrap_or(Value::Null)),
        BuiltinScalarFunction::PgStatClearSnapshot => {
            ctx.session_stats.write().clear_snapshot();
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatHaveStats => {
            let kind =
                values
                    .first()
                    .and_then(Value::as_text)
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op: "pg_stat_have_stats",
                        left: values.first().cloned().unwrap_or(Value::Null),
                        right: Value::Text("".into()),
                    })?;
            let objid = stats_oid_arg(&values[1..], "pg_stat_have_stats")?;
            let objsubid = values
                .get(2)
                .map(|value| match value {
                    Value::Int64(v) => Ok(*v),
                    Value::Int32(v) => Ok(i64::from(*v)),
                    other => Err(ExecError::TypeMismatch {
                        op: "pg_stat_have_stats",
                        left: other.clone(),
                        right: Value::Int64(0),
                    }),
                })
                .transpose()?
                .unwrap_or_default();
            let has_stats = match kind.to_ascii_lowercase().as_str() {
                "bgwriter" | "checkpointer" | "wal" => objid == 0 && objsubid == 0,
                "database" => objid != 0 && (objsubid == 0 || objsubid == 1),
                "relation" => ctx
                    .session_stats
                    .write()
                    .has_visible_relation_stats(&ctx.stats, objid),
                "function" => ctx
                    .session_stats
                    .write()
                    .has_visible_function_stats(&ctx.stats, objid),
                other => {
                    return Err(ExecError::DetailedError {
                        message: format!("unrecognized statistics kind \"{other}\""),
                        detail: None,
                        hint: None,
                        sqlstate: "22023",
                    });
                }
            };
            Ok(Value::Bool(has_stats))
        }
        BuiltinScalarFunction::PgStatGetNumscans
        | BuiltinScalarFunction::PgStatGetLastscan
        | BuiltinScalarFunction::PgStatGetTuplesReturned
        | BuiltinScalarFunction::PgStatGetTuplesFetched
        | BuiltinScalarFunction::PgStatGetTuplesInserted
        | BuiltinScalarFunction::PgStatGetTuplesUpdated
        | BuiltinScalarFunction::PgStatGetTuplesDeleted
        | BuiltinScalarFunction::PgStatGetLiveTuples
        | BuiltinScalarFunction::PgStatGetDeadTuples
        | BuiltinScalarFunction::PgStatGetBlocksFetched
        | BuiltinScalarFunction::PgStatGetBlocksHit => {
            relation_stats_value(func, stats_oid_arg(&values, "pg_stat_get_*")?, ctx)
        }
        BuiltinScalarFunction::PgStatGetXactNumscans
        | BuiltinScalarFunction::PgStatGetXactTuplesReturned
        | BuiltinScalarFunction::PgStatGetXactTuplesFetched
        | BuiltinScalarFunction::PgStatGetXactTuplesInserted
        | BuiltinScalarFunction::PgStatGetXactTuplesUpdated
        | BuiltinScalarFunction::PgStatGetXactTuplesDeleted => {
            relation_xact_stats_value(func, stats_oid_arg(&values, "pg_stat_get_xact_*")?, ctx)
        }
        BuiltinScalarFunction::PgStatGetFunctionCalls
        | BuiltinScalarFunction::PgStatGetFunctionTotalTime
        | BuiltinScalarFunction::PgStatGetFunctionSelfTime => {
            function_stats_value(func, stats_oid_arg(&values, "pg_stat_get_function_*")?, ctx)
        }
        BuiltinScalarFunction::PgStatGetXactFunctionCalls
        | BuiltinScalarFunction::PgStatGetXactFunctionTotalTime
        | BuiltinScalarFunction::PgStatGetXactFunctionSelfTime => function_xact_stats_value(
            func,
            stats_oid_arg(&values, "pg_stat_get_xact_function_*")?,
            ctx,
        ),
        BuiltinScalarFunction::PgInputIsValid => {
            let input = values[0].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_is_valid",
                left: values[0].clone(),
                right: Value::Text("".into()),
            })?;
            let ty = values[1].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_is_valid",
                left: values[1].clone(),
                right: Value::Text("".into()),
            })?;
            Ok(Value::Bool(
                soft_input_error_info_with_catalog_and_config(
                    input,
                    ty,
                    catalog_lookup(Some(ctx)),
                    &ctx.datetime_config,
                )?
                .is_none(),
            ))
        }
        BuiltinScalarFunction::PgInputErrorMessage
        | BuiltinScalarFunction::PgInputErrorDetail
        | BuiltinScalarFunction::PgInputErrorHint
        | BuiltinScalarFunction::PgInputErrorSqlState => {
            let input = values[0].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_error_info",
                left: values[0].clone(),
                right: Value::Text("".into()),
            })?;
            let ty = values[1].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_error_info",
                left: values[1].clone(),
                right: Value::Text("".into()),
            })?;
            let info = soft_input_error_info_with_catalog_and_config(
                input,
                ty,
                catalog_lookup(Some(ctx)),
                &ctx.datetime_config,
            )?;
            Ok(match (func, info) {
                (_, None) => Value::Null,
                (BuiltinScalarFunction::PgInputErrorMessage, Some(info)) => {
                    Value::Text(info.message.into())
                }
                (BuiltinScalarFunction::PgInputErrorDetail, Some(info)) => info
                    .detail
                    .map(Into::into)
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                (BuiltinScalarFunction::PgInputErrorHint, Some(info)) => info
                    .hint
                    .map(Into::into)
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                (BuiltinScalarFunction::PgInputErrorSqlState, Some(info)) => {
                    Value::Text(info.sqlstate.into())
                }
                _ => Value::Null,
            })
        }
        BuiltinScalarFunction::Abs => eval_abs_function(&values),
        BuiltinScalarFunction::Log => eval_log_function(&values),
        BuiltinScalarFunction::Log10 => eval_log10_function(&values),
        BuiltinScalarFunction::Div => eval_div_function(&values),
        BuiltinScalarFunction::Mod => mod_values(values[0].clone(), values[1].clone()),
        BuiltinScalarFunction::Scale => eval_scale_function(&values),
        BuiltinScalarFunction::MinScale => eval_min_scale_function(&values),
        BuiltinScalarFunction::TrimScale => eval_trim_scale_function(&values),
        BuiltinScalarFunction::NumericInc => eval_numeric_inc_function(&values),
        BuiltinScalarFunction::Factorial => eval_factorial_function(&values),
        BuiltinScalarFunction::ArrayNdims => eval_array_ndims_function(&values),
        BuiltinScalarFunction::ArrayDims => eval_array_dims_function(&values),
        BuiltinScalarFunction::ArrayLower => eval_array_lower_function(&values),
        BuiltinScalarFunction::ArrayUpper => eval_array_upper_function(&values),
        BuiltinScalarFunction::ArrayFill => eval_array_fill_function(&values),
        BuiltinScalarFunction::StringToArray => eval_string_to_array_function(&values),
        BuiltinScalarFunction::ArrayToString => eval_array_to_string_function(&values),
        BuiltinScalarFunction::ArrayLength => eval_array_length_function(&values),
        BuiltinScalarFunction::Cardinality => eval_cardinality_function(&values),
        BuiltinScalarFunction::ArrayAppend => eval_array_append_function(&values),
        BuiltinScalarFunction::ArrayPrepend => eval_array_prepend_function(&values),
        BuiltinScalarFunction::ArrayCat => eval_array_cat_function(&values),
        BuiltinScalarFunction::ArrayPosition => eval_array_position_function(&values),
        BuiltinScalarFunction::ArrayPositions => eval_array_positions_function(&values),
        BuiltinScalarFunction::ArrayRemove => eval_array_remove_function(&values),
        BuiltinScalarFunction::ArrayReplace => eval_array_replace_function(&values),
        BuiltinScalarFunction::ArraySort => eval_array_sort_function(&values),
        BuiltinScalarFunction::CurrentDatabase => {
            Ok(Value::Text(ctx.current_database_name.clone().into()))
        }
        BuiltinScalarFunction::Version => Ok(Value::Text(pg_version_text().into())),
        BuiltinScalarFunction::PgBackendPid => Ok(Value::Int32(ctx.client_id as i32)),
        BuiltinScalarFunction::PgColumnCompression => eval_pg_column_compression_values(&values),
        BuiltinScalarFunction::PgColumnSize => eval_pg_column_size_values(&values),
        BuiltinScalarFunction::PgRelationSize => eval_pg_relation_size(&values, ctx),
        BuiltinScalarFunction::PgPartitionRoot => eval_pg_partition_root(&values, ctx),
        BuiltinScalarFunction::ObjDescription => eval_obj_description(&values, ctx),
        BuiltinScalarFunction::PgDescribeObject => eval_pg_describe_object(&values, ctx),
        BuiltinScalarFunction::PgGetExpr => eval_pg_get_expr(&values),
        BuiltinScalarFunction::PgGetConstraintDef => eval_pg_get_constraintdef(&values, ctx),
        BuiltinScalarFunction::PgGetIndexDef => eval_pg_get_indexdef(&values, ctx),
        BuiltinScalarFunction::PgGetViewDef => eval_pg_get_viewdef(&values, ctx),
        BuiltinScalarFunction::PgGetTriggerDef => eval_pg_get_triggerdef(&values, ctx),
        BuiltinScalarFunction::PgTriggerDepth => Ok(Value::Int32(ctx.trigger_depth as i32)),
        BuiltinScalarFunction::PgRelationIsPublishable => {
            eval_pg_relation_is_publishable(&values, ctx)
        }
        BuiltinScalarFunction::PgIndexAmHasProperty => eval_pg_indexam_has_property(&values),
        BuiltinScalarFunction::PgIndexHasProperty => eval_pg_index_has_property(&values, ctx),
        BuiltinScalarFunction::PgIndexColumnHasProperty => {
            eval_pg_index_column_has_property(&values, ctx)
        }
        BuiltinScalarFunction::PgSizePretty => eval_pg_size_pretty_function(&values),
        BuiltinScalarFunction::PgSizeBytes => eval_pg_size_bytes_function(&values),
        BuiltinScalarFunction::PgLsn => eval_pg_lsn_function(&values),
        BuiltinScalarFunction::Trunc => eval_trunc_function(&values),
        BuiltinScalarFunction::Round => eval_round_function(&values),
        BuiltinScalarFunction::WidthBucket => {
            if values.len() == 2 {
                eval_width_bucket_thresholds(&values)
            } else {
                eval_width_bucket_function(&values)
            }
        }
        BuiltinScalarFunction::Ceil | BuiltinScalarFunction::Ceiling => eval_ceil_function(&values),
        BuiltinScalarFunction::Floor => eval_floor_function(&values),
        BuiltinScalarFunction::Sign => eval_sign_function(&values),
        BuiltinScalarFunction::Sqrt => eval_sqrt_function(&values),
        BuiltinScalarFunction::Cbrt => eval_unary_float_function("cbrt", &values, |v| Ok(v.cbrt())),
        BuiltinScalarFunction::Power => eval_power_function(&values),
        BuiltinScalarFunction::Exp => eval_exp_function(&values),
        BuiltinScalarFunction::Ln => eval_ln_function(&values),
        BuiltinScalarFunction::Sinh => eval_unary_float_function("sinh", &values, |v| Ok(v.sinh())),
        BuiltinScalarFunction::Cosh => eval_unary_float_function("cosh", &values, |v| Ok(v.cosh())),
        BuiltinScalarFunction::Tanh => eval_unary_float_function("tanh", &values, |v| Ok(v.tanh())),
        BuiltinScalarFunction::Asinh => {
            eval_unary_float_function("asinh", &values, |v| Ok(v.asinh()))
        }
        BuiltinScalarFunction::Acosh => eval_unary_float_function("acosh", &values, eval_acosh),
        BuiltinScalarFunction::Atanh => eval_unary_float_function("atanh", &values, eval_atanh),
        BuiltinScalarFunction::Sind => eval_unary_float_function("sind", &values, |v| Ok(sind(v))),
        BuiltinScalarFunction::Cosd => eval_unary_float_function("cosd", &values, |v| Ok(cosd(v))),
        BuiltinScalarFunction::Tand => eval_unary_float_function("tand", &values, |v| Ok(tand(v))),
        BuiltinScalarFunction::Cotd => eval_unary_float_function("cotd", &values, |v| Ok(cotd(v))),
        BuiltinScalarFunction::Asind => eval_unary_float_function("asind", &values, eval_asind),
        BuiltinScalarFunction::Acosd => eval_unary_float_function("acosd", &values, eval_acosd),
        BuiltinScalarFunction::Atand => {
            eval_unary_float_function("atand", &values, |v| Ok(snap_degree(v.atan().to_degrees())))
        }
        BuiltinScalarFunction::Atan2d => eval_binary_float_function("atan2d", &values, |y, x| {
            Ok(snap_degree(y.atan2(x).to_degrees()))
        }),
        BuiltinScalarFunction::Float4Send => eval_float_send_function("float4send", &values, true),
        BuiltinScalarFunction::Float8Send => eval_float_send_function("float8send", &values, false),
        BuiltinScalarFunction::Float8Accum => eval_float8_accum_function(&values),
        BuiltinScalarFunction::Float8Combine => eval_float8_combine_function(&values),
        BuiltinScalarFunction::Float8RegrAccum => eval_float8_regr_accum_function(&values),
        BuiltinScalarFunction::Float8RegrCombine => eval_float8_regr_combine_function(&values),
        BuiltinScalarFunction::Erf => eval_unary_float_function("erf", &values, eval_erf),
        BuiltinScalarFunction::Erfc => eval_unary_float_function("erfc", &values, eval_erfc),
        BuiltinScalarFunction::Gamma => eval_unary_float_function("gamma", &values, eval_gamma),
        BuiltinScalarFunction::Lgamma => eval_unary_float_function("lgamma", &values, eval_lgamma),
        BuiltinScalarFunction::BoolEq => eval_booleq(&values),
        BuiltinScalarFunction::BoolNe => eval_boolne(&values),
        BuiltinScalarFunction::BoolAndStateFunc => eval_booland_statefunc(&values),
        BuiltinScalarFunction::BoolOrStateFunc => eval_boolor_statefunc(&values),
        BuiltinScalarFunction::XmlComment => eval_xml_comment_function(&values, Some(ctx)),
        BuiltinScalarFunction::XmlIsWellFormed => {
            eval_xml_is_well_formed_function(&values, ctx.datetime_config.xml.option, Some(ctx))
        }
        BuiltinScalarFunction::XmlIsWellFormedDocument => eval_xml_is_well_formed_function(
            &values,
            crate::backend::utils::misc::guc_xml::XmlOptionSetting::Document,
            Some(ctx),
        ),
        BuiltinScalarFunction::XmlIsWellFormedContent => eval_xml_is_well_formed_function(
            &values,
            crate::backend::utils::misc::guc_xml::XmlOptionSetting::Content,
            Some(ctx),
        ),
        BuiltinScalarFunction::TsMatch => match values.as_slice() {
            [Value::TsVector(vector), Value::TsQuery(query)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsvector_matches_tsquery(vector, query),
            )),
            [Value::TsQuery(query), Value::TsVector(vector)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsquery_matches_tsvector(query, vector),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "@@",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryAnd => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_and(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "&&",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryOr => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_or(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryNot => match values.as_slice() {
            [Value::TsQuery(query)] => Ok(Value::TsQuery(crate::backend::executor::tsquery_not(
                query.clone(),
            ))),
            _ => Err(ExecError::TypeMismatch {
                op: "!!",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::TsVectorConcat => match values.as_slice() {
            [Value::TsVector(left), Value::TsVector(right)] => Ok(Value::TsVector(
                crate::backend::executor::concat_tsvector(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::BitcastIntegerToFloat4 => eval_bitcast_integer_to_float4(&values),
        BuiltinScalarFunction::BitcastBigintToFloat8 => eval_bitcast_bigint_to_float8(&values),
        BuiltinScalarFunction::Gcd => eval_gcd_function(&values),
        BuiltinScalarFunction::Lcm => eval_lcm_function(&values),
        BuiltinScalarFunction::Length => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_length_function(&values),
        },
        BuiltinScalarFunction::Concat => {
            eval_concat_function(&values, func_variadic, &ctx.datetime_config)
        }
        BuiltinScalarFunction::ConcatWs => {
            eval_concat_ws_function(&values, func_variadic, &ctx.datetime_config)
        }
        BuiltinScalarFunction::Format => {
            eval_format_function(&values, func_variadic, &ctx.datetime_config)
        }
        BuiltinScalarFunction::Left => eval_left_function(&values),
        BuiltinScalarFunction::Right => eval_right_function(&values),
        BuiltinScalarFunction::LPad => eval_lpad_function(&values),
        BuiltinScalarFunction::RPad => eval_rpad_function(&values),
        BuiltinScalarFunction::Repeat => eval_repeat_function(&values),
        BuiltinScalarFunction::Lower => eval_lower_function(&values),
        BuiltinScalarFunction::Unistr => eval_unistr_function(&values),
        BuiltinScalarFunction::Initcap => eval_initcap_function(&values),
        BuiltinScalarFunction::BTrim => eval_trim_function("btrim", &values),
        BuiltinScalarFunction::LTrim => eval_trim_function("ltrim", &values),
        BuiltinScalarFunction::RTrim => eval_trim_function("rtrim", &values),
        BuiltinScalarFunction::Md5 => eval_md5_function(&values),
        BuiltinScalarFunction::Reverse => eval_reverse_function(&values),
        BuiltinScalarFunction::TextToRegClass => eval_text_to_regclass_function(&values, Some(ctx)),
        BuiltinScalarFunction::ToRegProc => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegProc, Some(ctx))
        }
        BuiltinScalarFunction::ToRegProcedure => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegProcedure, Some(ctx))
        }
        BuiltinScalarFunction::ToRegOper => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegOper, Some(ctx))
        }
        BuiltinScalarFunction::ToRegOperator => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegOperator, Some(ctx))
        }
        BuiltinScalarFunction::ToRegClass => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegClass, Some(ctx))
        }
        BuiltinScalarFunction::ToRegType => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegType, Some(ctx))
        }
        BuiltinScalarFunction::ToRegTypeMod => eval_to_regtypemod_function(&values, Some(ctx)),
        BuiltinScalarFunction::ToRegRole => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegRole, Some(ctx))
        }
        BuiltinScalarFunction::ToRegNamespace => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegNamespace, Some(ctx))
        }
        BuiltinScalarFunction::ToRegCollation => {
            eval_to_reg_object_function(&values, SqlTypeKind::RegCollation, Some(ctx))
        }
        BuiltinScalarFunction::FormatType => eval_format_type_function(&values, Some(ctx)),
        BuiltinScalarFunction::RegClassToText => eval_regclass_to_text_function(&values, Some(ctx)),
        BuiltinScalarFunction::RegTypeToText => eval_regtype_to_text_function(&values, Some(ctx)),
        BuiltinScalarFunction::RegRoleToText => eval_regrole_to_text_function(&values, Some(ctx)),
        BuiltinScalarFunction::BpcharToText => eval_bpchar_to_text_function(&values),
        BuiltinScalarFunction::QuoteLiteral => eval_quote_literal_function(&values),
        BuiltinScalarFunction::Replace => eval_replace_function(&values),
        BuiltinScalarFunction::SplitPart => eval_split_part_function(&values),
        BuiltinScalarFunction::Translate => eval_translate_function(&values),
        BuiltinScalarFunction::Ascii => eval_ascii_function(&values),
        BuiltinScalarFunction::Chr => eval_chr_function(&values),
        BuiltinScalarFunction::ParseIdent => eval_parse_ident_function(&values),
        BuiltinScalarFunction::Strpos => eval_strpos_function(&values),
        BuiltinScalarFunction::Position => match values.as_slice() {
            [Value::Bit(needle), Value::Bit(haystack)] => {
                Ok(Value::Int32(eval_bit_position(needle, haystack)))
            }
            [Value::Bytea(_), Value::Bytea(_)] => eval_bytea_position_function(&values),
            _ => eval_position_function(&values),
        },
        BuiltinScalarFunction::Substring => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, None)?))
            }
            [Value::Bit(bits), Value::Int32(start), Value::Int32(len)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, Some(*len))?))
            }
            [Value::Bytea(_), Value::Int32(_)]
            | [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_bytea_substring(&values),
            [Value::Text(_), Value::Text(_)] => eval_sql_regex_substring(&values),
            _ => eval_text_substring(&values),
        },
        BuiltinScalarFunction::SimilarSubstring => eval_similar_substring(&values),
        BuiltinScalarFunction::Overlay => match values.as_slice() {
            [Value::Bit(bits), Value::Bit(place), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_overlay(bits, place, *start, None)?))
            }
            [
                Value::Bit(bits),
                Value::Bit(place),
                Value::Int32(start),
                Value::Int32(len),
            ] => Ok(Value::Bit(eval_bit_overlay(
                bits,
                place,
                *start,
                Some(*len),
            )?)),
            [Value::Bytea(_), Value::Bytea(_), Value::Int32(_)]
            | [
                Value::Bytea(_),
                Value::Bytea(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_bytea_overlay(&values),
            [Value::Text(_), Value::Text(_), Value::Int32(_)]
            | [
                Value::Text(_),
                Value::Text(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_text_overlay(&values),
            _ => unreachable!("validated overlay arguments"),
        },
        BuiltinScalarFunction::GetBit => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(index)] => {
                Ok(Value::Int32(eval_get_bit(bits, *index)?))
            }
            [Value::Bytea(_), Value::Int32(_)] => eval_get_bit_bytes(&values),
            _ => unreachable!("validated get_bit arguments"),
        },
        BuiltinScalarFunction::SetBit => match values.as_slice() {
            [
                Value::Bit(bits),
                Value::Int32(index),
                Value::Int32(new_value),
            ] => Ok(Value::Bit(eval_set_bit(bits, *index, *new_value)?)),
            [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_set_bit_bytes(&values),
            _ => unreachable!("validated set_bit arguments"),
        },
        BuiltinScalarFunction::BitCount => match values.as_slice() {
            [Value::Bit(bits)] => Ok(Value::Int64(eval_bit_count(bits))),
            [Value::Bytea(_)] => eval_bit_count_bytes(&values),
            _ => unreachable!("validated bit_count arguments"),
        },
        BuiltinScalarFunction::GetByte => eval_get_byte(&values),
        BuiltinScalarFunction::SetByte => eval_set_byte(&values),
        BuiltinScalarFunction::ConvertFrom => eval_convert_from_function(&values),
        BuiltinScalarFunction::Encode => eval_encode_function(&values),
        BuiltinScalarFunction::Decode => eval_decode_function(&values),
        BuiltinScalarFunction::Sha224 => eval_sha224_function(&values),
        BuiltinScalarFunction::Sha256 => eval_sha256_function(&values),
        BuiltinScalarFunction::Sha384 => eval_sha384_function(&values),
        BuiltinScalarFunction::Sha512 => eval_sha512_function(&values),
        BuiltinScalarFunction::Crc32 => eval_crc32_function(&values),
        BuiltinScalarFunction::Crc32c => eval_crc32c_function(&values),
        BuiltinScalarFunction::ToBin => eval_to_bin_function(&values),
        BuiltinScalarFunction::ToOct => eval_to_oct_function(&values),
        BuiltinScalarFunction::ToHex => eval_to_hex_function(&values),
        BuiltinScalarFunction::RegexpMatch => eval_regexp_match(&values),
        BuiltinScalarFunction::RegexpLike => eval_regexp_like(&values),
        BuiltinScalarFunction::RegexpReplace => eval_regexp_replace(&values),
        BuiltinScalarFunction::RegexpCount => eval_regexp_count(&values),
        BuiltinScalarFunction::RegexpInstr => eval_regexp_instr(&values),
        BuiltinScalarFunction::RegexpSubstr => eval_regexp_substr(&values),
        BuiltinScalarFunction::RegexpSplitToArray => eval_regexp_split_to_array(&values),
        BuiltinScalarFunction::ToChar => eval_to_char_function(&values),
        BuiltinScalarFunction::ToDate => eval_to_date_function(&values),
        BuiltinScalarFunction::ToNumber => eval_to_number_function(&values),
        _ => unreachable!("json builtins handled by expr_json"),
    }
}

fn eval_jsonb_contains(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let left_jsonb = jsonb_from_value(
        &left,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    )?;
    let right_jsonb = jsonb_from_value(
        &right,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    )?;
    Ok(Value::Bool(jsonb_contains(&left_jsonb, &right_jsonb)))
}

fn eval_jsonb_contained(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let left_jsonb = jsonb_from_value(
        &left,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    )?;
    let right_jsonb = jsonb_from_value(
        &right,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    )?;
    Ok(Value::Bool(jsonb_contains(&right_jsonb, &left_jsonb)))
}

fn eval_jsonb_exists(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let key = right.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "?",
        left: left.clone(),
        right: right.clone(),
    })?;
    let jsonb = jsonb_from_value(
        &left,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    )?;
    Ok(Value::Bool(jsonb_exists(&jsonb, key)))
}

fn eval_jsonb_exists_any(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_jsonb_exists_list(left, right, "?|", jsonb_exists_any)
}

fn eval_jsonb_exists_all(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_jsonb_exists_list(left, right, "?&", jsonb_exists_all)
}

fn eval_jsonb_exists_list(
    left: Value,
    right: Value,
    op: &'static str,
    pred: fn(&JsonbValue, &[String]) -> bool,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let keys = match right {
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_text()
                    .map(|text| text.to_string())
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op,
                        left: left.clone(),
                        right: item.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        Value::PgArray(array) => array
            .elements
            .iter()
            .map(|item| {
                item.as_text()
                    .map(|text| text.to_string())
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op,
                        left: left.clone(),
                        right: item.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left,
                right: other,
            });
        }
    };
    let jsonb = jsonb_from_value(
        &left,
        &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
    )?;
    Ok(Value::Bool(pred(&jsonb, &keys)))
}

fn render_current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("{}.{:06}+00", dur.as_secs(), dur.subsec_micros()),
        Err(_) => "0.000000+00".to_string(),
    }
}

fn eval_random_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] => Ok(Value::Float64(rand::random::<f64>())),
        [Value::Int32(min), Value::Int32(max)] => {
            if min > max {
                return Err(invalid_random_bound_error(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            Ok(Value::Int32(rand::thread_rng().gen_range(*min..=*max)))
        }
        [Value::Int64(min), Value::Int64(max)] => {
            if min > max {
                return Err(invalid_random_bound_error(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            Ok(Value::Int64(rand::thread_rng().gen_range(*min..=*max)))
        }
        [Value::Numeric(min), Value::Numeric(max)] => eval_random_numeric_range(min, max),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "random",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("Random({} args)", values.len()),
        })),
    }
}

fn eval_uuid_function(func: BuiltinScalarFunction, values: &[Value]) -> Result<Value, ExecError> {
    match func {
        BuiltinScalarFunction::UuidIn => match values {
            [Value::Text(text)] => Ok(Value::Uuid(super::expr_casts::parse_uuid_text(text)?)),
            [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuid_in",
                left: value.clone(),
                right: Value::Text("".into()),
            }),
            _ => Err(malformed_expr_error("uuid_in")),
        },
        BuiltinScalarFunction::UuidOut => match values {
            [Value::Uuid(value)] => {
                Ok(Value::Text(super::value_io::render_uuid_text(value).into()))
            }
            [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuid_out",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(malformed_expr_error("uuid_out")),
        },
        BuiltinScalarFunction::UuidRecv => match values {
            [Value::Bytea(bytes)] if bytes.len() == 16 => {
                Ok(Value::Uuid(bytes.as_slice().try_into().unwrap()))
            }
            [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuid_recv",
                left: value.clone(),
                right: Value::Bytea(vec![0; 16]),
            }),
            _ => Err(malformed_expr_error("uuid_recv")),
        },
        BuiltinScalarFunction::UuidSend => match values {
            [Value::Uuid(value)] => Ok(Value::Bytea(value.to_vec())),
            [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuid_send",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(malformed_expr_error("uuid_send")),
        },
        BuiltinScalarFunction::UuidEq
        | BuiltinScalarFunction::UuidNe
        | BuiltinScalarFunction::UuidLt
        | BuiltinScalarFunction::UuidLe
        | BuiltinScalarFunction::UuidGt
        | BuiltinScalarFunction::UuidGe
        | BuiltinScalarFunction::UuidCmp => match values {
            [Value::Uuid(left), Value::Uuid(right)] => Ok(match func {
                BuiltinScalarFunction::UuidEq => Value::Bool(left == right),
                BuiltinScalarFunction::UuidNe => Value::Bool(left != right),
                BuiltinScalarFunction::UuidLt => Value::Bool(left < right),
                BuiltinScalarFunction::UuidLe => Value::Bool(left <= right),
                BuiltinScalarFunction::UuidGt => Value::Bool(left > right),
                BuiltinScalarFunction::UuidGe => Value::Bool(left >= right),
                BuiltinScalarFunction::UuidCmp => Value::Int32(match left.cmp(right) {
                    std::cmp::Ordering::Less => -1,
                    std::cmp::Ordering::Equal => 0,
                    std::cmp::Ordering::Greater => 1,
                }),
                _ => unreachable!(),
            }),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [left, right] => Err(ExecError::TypeMismatch {
                op: "uuid",
                left: left.clone(),
                right: right.clone(),
            }),
            _ => Err(malformed_expr_error("uuid")),
        },
        BuiltinScalarFunction::UuidHash => match values {
            [Value::Uuid(value)] => Ok(Value::Int32(uuid_hash(value) as i32)),
            [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuid_hash",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(malformed_expr_error("uuid_hash")),
        },
        BuiltinScalarFunction::UuidHashExtended => match values {
            [Value::Uuid(value), Value::Int64(seed)] => {
                Ok(Value::Int64(uuid_hash_extended(value, *seed as u64) as i64))
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [left, right] => Err(ExecError::TypeMismatch {
                op: "uuid_hash_extended",
                left: left.clone(),
                right: right.clone(),
            }),
            _ => Err(malformed_expr_error("uuid_hash_extended")),
        },
        BuiltinScalarFunction::GenRandomUuid => match values {
            [] => Ok(Value::Uuid(generate_uuid_v4())),
            _ => Err(malformed_expr_error("gen_random_uuid")),
        },
        BuiltinScalarFunction::UuidV7 => match values {
            [] => Ok(Value::Uuid(generate_uuid_v7(0))),
            [Value::Interval(interval)] => {
                let shift_millis = interval.time_micros / 1_000
                    + i64::from(interval.days) * 86_400_000
                    + i64::from(interval.months) * 30 * 86_400_000;
                Ok(Value::Uuid(generate_uuid_v7(shift_millis)))
            }
            [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuidv7",
                left: value.clone(),
                right: Value::Interval(crate::include::nodes::datum::IntervalValue::zero()),
            }),
            _ => Err(malformed_expr_error("uuidv7")),
        },
        BuiltinScalarFunction::UuidExtractVersion => match values {
            [Value::Uuid(value)] => {
                Ok(uuid_version(value).map(Value::Int16).unwrap_or(Value::Null))
            }
            [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuid_extract_version",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(malformed_expr_error("uuid_extract_version")),
        },
        BuiltinScalarFunction::UuidExtractTimestamp => match values {
            [Value::Uuid(value)] if uuid_version(value) == Some(1) => uuid_v1_timestamp(value)
                .map_or(Ok(Value::Null), |postgres_usecs| {
                    Ok(Value::TimestampTz(
                        crate::include::nodes::datetime::TimestampTzADT(postgres_usecs),
                    ))
                }),
            [Value::Uuid(value)] if uuid_version(value) == Some(7) => Ok(Value::TimestampTz(
                crate::include::nodes::datetime::TimestampTzADT(uuid_v7_timestamp(value)),
            )),
            [Value::Uuid(_)] | [Value::Null] => Ok(Value::Null),
            [value] => Err(ExecError::TypeMismatch {
                op: "uuid_extract_timestamp",
                left: value.clone(),
                right: Value::Uuid([0; 16]),
            }),
            _ => Err(malformed_expr_error("uuid_extract_timestamp")),
        },
        _ => unreachable!("uuid dispatcher called for non-uuid builtin"),
    }
}

fn generate_uuid_v4() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

static UUID_V7_STATE: Mutex<(u64, u64)> = Mutex::new((0, 0));

fn generate_uuid_v7(shift_millis: i64) -> [u8; 16] {
    let millis = current_postgres_timestamp_usecs()
        .saturating_div(1_000)
        .saturating_add(10_957 * 86_400_000)
        .saturating_add(shift_millis)
        .max(0) as u64;
    let mut bytes = [0u8; 16];
    bytes[0] = (millis >> 40) as u8;
    bytes[1] = (millis >> 32) as u8;
    bytes[2] = (millis >> 24) as u8;
    bytes[3] = (millis >> 16) as u8;
    bytes[4] = (millis >> 8) as u8;
    bytes[5] = millis as u8;
    rand::thread_rng().fill_bytes(&mut bytes[6..]);
    let sequence = {
        let mut state = UUID_V7_STATE.lock().expect("uuidv7 state mutex poisoned");
        if state.0 == millis {
            state.1 = state.1.wrapping_add(1) & ((1u64 << 42) - 1);
        } else {
            state.0 = millis;
            state.1 = 0;
        }
        state.1
    };
    bytes[6] = 0x70 | (((sequence >> 38) as u8) & 0x0f);
    bytes[7] = (sequence >> 30) as u8;
    bytes[8] = 0x80 | (((sequence >> 24) as u8) & 0x3f);
    bytes[9] = (sequence >> 16) as u8;
    bytes[10] = (sequence >> 8) as u8;
    bytes[11] = sequence as u8;
    bytes
}

fn uuid_version(value: &[u8; 16]) -> Option<i16> {
    ((value[8] & 0xc0) == 0x80).then_some(i16::from(value[6] >> 4))
}

fn uuid_v1_timestamp(value: &[u8; 16]) -> Option<i64> {
    let timestamp_100ns = ((u64::from(value[6] & 0x0f)) << 56)
        | (u64::from(value[7]) << 48)
        | (u64::from(value[4]) << 40)
        | (u64::from(value[5]) << 32)
        | (u64::from(value[0]) << 24)
        | (u64::from(value[1]) << 16)
        | (u64::from(value[2]) << 8)
        | u64::from(value[3]);
    let unix_100ns = timestamp_100ns.checked_sub(122_192_928_000_000_000)?;
    let unix_usecs = i64::try_from(unix_100ns / 10).ok()?;
    Some(unix_usecs - 10_957 * 86_400_000_000)
}

fn uuid_v7_timestamp(value: &[u8; 16]) -> i64 {
    let millis = ((value[0] as i64) << 40)
        | ((value[1] as i64) << 32)
        | ((value[2] as i64) << 24)
        | ((value[3] as i64) << 16)
        | ((value[4] as i64) << 8)
        | value[5] as i64;
    millis * 1_000 - 10_957 * 86_400_000_000
}

fn uuid_hash(value: &[u8; 16]) -> u32 {
    let hash = crate::backend::access::hash::support::hash_bytes_extended(value, 0);
    (hash as u32) ^ ((hash >> 32) as u32)
}

fn uuid_hash_extended(value: &[u8; 16], seed: u64) -> u64 {
    crate::backend::access::hash::support::hash_bytes_extended(value, seed)
}

fn eval_random_normal_function(values: &[Value]) -> Result<Value, ExecError> {
    let (mean, stddev) = match values {
        [] => (0.0, 1.0),
        [Value::Float64(mean), Value::Float64(stddev)] => (*mean, *stddev),
        [left, right] => {
            return Err(ExecError::TypeMismatch {
                op: "random_normal",
                left: left.clone(),
                right: right.clone(),
            });
        }
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid builtin function arity",
                actual: format!("RandomNormal({} args)", values.len()),
            }));
        }
    };

    if stddev == 0.0 {
        return Ok(Value::Float64(mean));
    }

    Ok(Value::Float64((sample_standard_normal() * stddev) + mean))
}

fn eval_random_numeric_range(min: &NumericValue, max: &NumericValue) -> Result<Value, ExecError> {
    match min {
        NumericValue::NaN => return Err(invalid_random_bound_error("lower bound cannot be NaN")),
        NumericValue::PosInf | NumericValue::NegInf => {
            return Err(invalid_random_bound_error("lower bound cannot be infinity"));
        }
        NumericValue::Finite { .. } => {}
    }
    match max {
        NumericValue::NaN => return Err(invalid_random_bound_error("upper bound cannot be NaN")),
        NumericValue::PosInf | NumericValue::NegInf => {
            return Err(invalid_random_bound_error("upper bound cannot be infinity"));
        }
        NumericValue::Finite { .. } => {}
    }
    if min.cmp(max).is_gt() {
        return Err(invalid_random_bound_error(
            "lower bound must be less than or equal to upper bound",
        ));
    }

    let (
        NumericValue::Finite {
            coeff: min_coeff,
            scale: min_scale,
            ..
        },
        NumericValue::Finite {
            coeff: max_coeff,
            scale: max_scale,
            ..
        },
    ) = (min, max)
    else {
        unreachable!();
    };

    let scale = (*min_scale).max(*max_scale);
    let min_aligned = align_numeric_coeff(min_coeff.clone(), *min_scale, scale);
    let max_aligned = align_numeric_coeff(max_coeff.clone(), *max_scale, scale);

    if min_aligned == max_aligned {
        return Ok(Value::Numeric(min.clone()));
    }

    let span = (&max_aligned - &min_aligned) + BigInt::from(1u8);
    let offset = random_bigint_below(&span, &mut rand::thread_rng());
    Ok(Value::Numeric(
        NumericValue::finite(min_aligned + offset, scale)
            .with_dscale(scale)
            .normalize(),
    ))
}

fn invalid_random_bound_error(message: &str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
    }
}

fn sample_standard_normal() -> f64 {
    let mut rng = rand::thread_rng();
    loop {
        let u1 = rng.r#gen::<f64>();
        if u1 == 0.0 {
            continue;
        }
        let u2 = rng.r#gen::<f64>();
        let radius = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        return radius * theta.cos();
    }
}

fn align_numeric_coeff(coeff: BigInt, from_scale: u32, to_scale: u32) -> BigInt {
    coeff * pow10_bigint(to_scale.saturating_sub(from_scale))
}

fn pow10_bigint(exp: u32) -> BigInt {
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

fn random_bigint_below(upper_exclusive: &BigInt, rng: &mut impl RngCore) -> BigInt {
    debug_assert!(*upper_exclusive > BigInt::from(0u8));
    let (_, upper_bytes) = upper_exclusive.to_bytes_be();
    let mut candidate_bytes = vec![0u8; upper_bytes.len().max(1)];
    let high_mask = if upper_bytes.is_empty() {
        0xff
    } else {
        0xff_u8 >> upper_bytes[0].leading_zeros()
    };

    loop {
        rng.fill_bytes(&mut candidate_bytes);
        candidate_bytes[0] &= high_mask;
        let candidate = BigInt::from_bytes_be(Sign::Plus, &candidate_bytes);
        if candidate < *upper_exclusive {
            return candidate;
        }
    }
}
