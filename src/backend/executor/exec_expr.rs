use num_traits::ToPrimitive;
use std::cmp::Ordering;

use crate::backend::parser::analyze::sql_type_name;
use crate::backend::storage::smgr::{ForkNumber, StorageManager};
use crate::backend::utils::sql_deparse::{
    normalize_check_expr_sql, normalize_index_expression_sql, normalize_index_predicate_sql,
};
use crate::backend::utils::time::system_time::{SystemTime, UNIX_EPOCH};
use crate::backend::utils::time::timestamp::{timestamp_at_time_zone, timestamptz_at_time_zone};
use crate::backend::utils::trigger::format_trigger_definition;
use crate::include::nodes::datetime::{
    MAX_TIME_PRECISION, TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_SEC,
};
use crate::include::nodes::primnodes::expr_sql_type_hint;
use rand::RngCore;
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
    cast_value_with_source_type_catalog_and_config, parse_interval_text_value,
    soft_input_error_info_with_catalog_and_config,
};
pub(crate) use super::expr_compile::{
    CompiledPredicate, compile_predicate, compile_predicate_with_decoder,
};
use super::expr_date::{
    eval_age_function, eval_date_bin_function, eval_date_part_function_with_config,
    eval_date_trunc_function, eval_datetime_add_function, eval_extract_function,
    eval_extract_function_with_config, eval_isfinite_function, eval_justify_days_function,
    eval_justify_hours_function, eval_justify_interval_function, eval_make_date_function,
    eval_make_interval_function, eval_make_time_function, eval_make_timestamp_function,
    eval_make_timestamptz_function, eval_timestamptz_constructor_function,
    eval_timezone_function as eval_timetz_timezone_function, eval_to_date_function,
    eval_to_timestamp_function, timezone_interval_seconds, timezone_target_offset_seconds,
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
    eval_jsonpath_operator, eval_sql_json_query_function_expr, jsonb_to_tsvector_value,
};
use super::expr_locks::eval_advisory_lock_builtin_function;
use super::expr_mac::eval_macaddr_function;
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
    add_values, add_values_with_config, bitwise_and_values, bitwise_not_value, bitwise_or_values,
    bitwise_xor_values, compare_values, compare_values_with_type, concat_values,
    concat_values_with_cast_context, div_values, eval_and, eval_or, mixed_date_timestamp_ordering,
    mod_values, mul_values, negate_value, not_equal_values, not_equal_values_with_type,
    order_values, shift_left_values, shift_right_values, sub_values, sub_values_with_config,
    values_are_distinct,
};
pub(crate) use super::expr_ops::{compare_order_by_keys, parse_numeric_text};
use super::expr_partition::eval_satisfies_hash_partition;
use super::expr_range::eval_range_function;
use super::expr_reg;
use super::expr_string::{
    eval_ascii_function, eval_bit_count_bytes, eval_bit_length_function,
    eval_bpchar_to_text_function, eval_bytea_overlay, eval_bytea_position_function,
    eval_bytea_substring, eval_chr_function, eval_concat_function, eval_concat_ws_function,
    eval_convert_from_function, eval_convert_to_function, eval_crc32_function,
    eval_crc32c_function, eval_decode_function, eval_encode_function, eval_format_function,
    eval_get_bit_bytes, eval_get_byte, eval_initcap_function, eval_left_function,
    eval_length_function, eval_like, eval_lower_function, eval_lpad_function, eval_md5_function,
    eval_parse_ident_function, eval_pg_rust_is_catalog_text_unique_index_oid,
    eval_pg_rust_test_enc_conversion, eval_pg_rust_test_enc_setup, eval_pg_rust_test_fdw_handler,
    eval_pg_rust_test_int44in, eval_pg_rust_test_int44out, eval_pg_rust_test_opclass_options_func,
    eval_pg_rust_test_pt_in_widget, eval_pg_rust_test_widget_in, eval_pg_rust_test_widget_out,
    eval_pg_size_bytes_function, eval_pg_size_pretty_function, eval_position_function,
    eval_quote_ident_function, eval_quote_literal_function, eval_repeat_function,
    eval_replace_function, eval_reverse_function, eval_right_function, eval_rpad_function,
    eval_set_bit_bytes, eval_set_byte, eval_sha224_function, eval_sha256_function,
    eval_sha384_function, eval_sha512_function, eval_split_part_function, eval_strpos_function,
    eval_text_overlay, eval_text_starts_with_function, eval_text_substring, eval_to_bin_function,
    eval_to_char_float4_function, eval_to_char_function, eval_to_hex_function,
    eval_to_number_function, eval_to_oct_function, eval_translate_function, eval_trim_function,
    eval_unicode_assigned_function, eval_unicode_is_normalized_function,
    eval_unicode_normalize_function, eval_unicode_version_function, eval_unistr_function,
    eval_upper_function,
};
use super::expr_txid::eval_txid_builtin_function;
use super::expr_xml::{
    eval_xml_comment_function, eval_xml_expr, eval_xml_is_well_formed_function,
    eval_xml_text_function, eval_xpath_exists_function, eval_xpath_function,
    unsupported_xml_feature_error,
};
use super::node_types::*;
use super::pg_regex::{
    eval_regex_match_operator, eval_regexp_count, eval_regexp_instr, eval_regexp_like,
    eval_regexp_match, eval_regexp_replace, eval_regexp_split_to_array, eval_regexp_substr,
    eval_similar, eval_similar_substring, eval_sql_regex_substring,
};
pub(crate) use super::value_io::{format_array_text, format_array_value_text};
use super::{ExecError, ExecutorContext, TypedFunctionArg, exec_next, executor_start};
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::catalog::indexing::probe_system_catalog_rows_visible_in_db;
use crate::backend::catalog::rowcodec::pg_description_row_from_values;
use crate::backend::executor::jsonb::{
    JsonbValue, jsonb_contains, jsonb_exists, jsonb_exists_all, jsonb_exists_any, jsonb_from_value,
};
use crate::backend::parser::analyze::is_binary_coercible_type;
use crate::backend::parser::{
    CatalogLookup, LoweredPartitionSpec, ParseError, PartitionBoundSpec, PartitionRangeDatumValue,
    PartitionStrategy, SerializedPartitionValue, SqlType, SqlTypeKind, SubqueryComparisonOp,
    bind_relation_expr, deserialize_partition_bound, partition_value_to_value,
    relation_partition_spec,
};
use crate::backend::rewrite::{
    format_stored_rule_definition_with_catalog, format_view_definition, render_relation_expr_sql,
};
use crate::backend::statistics::{
    render_pg_dependencies_text, render_pg_mcv_list_text, render_pg_ndistinct_text,
};
use crate::backend::utils::misc::checkpoint::checkpoint_stats_value;
use crate::backend::utils::misc::guc::normalize_guc_name;
use crate::backend::utils::misc::guc::plpgsql_guc_default_value;
use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;
use crate::include::access::toast_compression::ToastCompressionId;
use crate::include::catalog::pg_proc::bootstrap_proc_execute_acl_has_grantee;
use crate::include::catalog::{
    ANYOID, ARRAY_BTREE_OPCLASS_OID, BOX_SPGIST_OPCLASS_OID, BPCHAR_HASH_OPCLASS_OID, BRIN_AM_OID,
    BTREE_AM_OID, BYTEA_TYPE_OID, CONSTRAINT_CHECK, CONSTRAINT_EXCLUSION, CONSTRAINT_FOREIGN,
    CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, CURRENT_DATABASE_OID,
    DEFAULT_COLLATION_OID, DEFAULT_TABLESPACE_OID, FLOAT8_TYPE_OID, GIN_AM_OID, GIST_AM_OID,
    GLOBAL_TABLESPACE_OID, HASH_AM_OID, INET_SPGIST_OPCLASS_OID, KD_POINT_SPGIST_OPCLASS_OID,
    NAME_TYPE_OID, PG_AUTHID_RELATION_OID, PG_CATALOG_NAMESPACE_OID, PG_CLASS_RELATION_OID,
    PG_DATABASE_OWNER_OID, PG_DATABASE_RELATION_OID, PG_DEPENDENCIES_TYPE_OID,
    PG_EVENT_TRIGGER_RELATION_OID, PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
    PG_LARGEOBJECT_RELATION_OID, PG_MCV_LIST_TYPE_OID, PG_NDISTINCT_TYPE_OID, PG_READ_ALL_DATA_OID,
    PG_STATISTIC_EXT_RELATION_OID, PG_TOAST_NAMESPACE_OID, PG_WRITE_ALL_DATA_OID,
    POLY_SPGIST_OPCLASS_OID, PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgClassRow,
    PgConversionRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgTsConfigRow, PgTsDictRow,
    PgTsParserRow, PgTsTemplateRow, PgTypeRow, QUAD_POINT_SPGIST_OPCLASS_OID, SPGIST_AM_OID,
    TEXT_SPGIST_OPCLASS_OID, TEXT_TYPE_OID, bootstrap_pg_am_rows,
    builtin_scalar_function_for_proc_oid, builtin_type_name_for_oid, default_btree_opclass_oid,
    default_hash_opclass_oid,
};
use crate::include::nodes::datum::{ArrayDimension, ArrayValue, NumericValue, RecordValue};
use crate::include::nodes::primnodes::{
    BoolExpr, BoolExprType, FuncExpr, HashFunctionKind, INDEX_VAR, INNER_VAR, OUTER_VAR, OpExpr,
    OpExprKind, SELF_ITEM_POINTER_ATTR_NO, ScalarArrayOpExpr, SubLinkType, TABLE_OID_ATTR_NO,
    attrno_index, is_executor_special_varno, is_system_attr,
};
use crate::pgrust::compact_string::CompactString;
use crate::pl::plpgsql::current_event_trigger_table_rewrite;

mod arrays;
mod subquery;

pub(crate) use arrays::normalize_array_value;
pub(crate) use arrays::{append_array_value, concatenate_arrays, eval_string_to_table_rows};
use arrays::{
    eval_array_append_function, eval_array_cat_function, eval_array_contained, eval_array_contains,
    eval_array_dims_function, eval_array_fill_function, eval_array_length_function,
    eval_array_lower_function, eval_array_ndims_function, eval_array_overlap,
    eval_array_position_function, eval_array_positions_function, eval_array_prepend_function,
    eval_array_remove_function, eval_array_replace_function, eval_array_reverse_function,
    eval_array_sample_function, eval_array_shuffle_function, eval_array_sort_function,
    eval_array_subscript, eval_array_subscript_plpgsql, eval_array_to_string_function,
    eval_array_upper_function, eval_cardinality_function, eval_quantified_array,
    eval_string_to_array_function, eval_trim_array_function, eval_width_bucket_thresholds,
};
use subquery::{
    eval_array_subquery, eval_exists_subquery, eval_quantified_subquery, eval_row_compare_subquery,
    eval_scalar_subquery,
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
        BuiltinScalarFunction::PgStatGetTuplesHotUpdated => Value::Int64(entry.tuples_hot_updated),
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
    let mut session = ctx.session_stats.write();
    let entry = if let Some(entry) = session.visible_function_entry(&ctx.stats, oid) {
        entry
    } else if session.function_xact.contains_key(&oid) {
        crate::backend::utils::activity::FunctionStatsEntry::default()
    } else {
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
    SpgistCanReturnData,
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
            returnability: IndexReturnability::SpgistCanReturnData,
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
    catalog: &dyn CatalogLookup,
    index_meta: &crate::include::catalog::PgIndexRow,
    column_index: usize,
) -> bool {
    let Some(opclass_oid) = index_meta.indclass.get(column_index).copied() else {
        return false;
    };
    let Some(opclass) = catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)
    else {
        return false;
    };
    catalog
        .amop_rows()
        .into_iter()
        .any(|row| row.amopfamily == opclass.opcfamily && row.amoppurpose == 'o')
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
            let Some(index_meta) = catalog.index_row_by_oid(relation_oid) else {
                return Ok(Value::Null);
            };
            let Some(class) = catalog.class_row_by_oid(index_meta.indexrelid) else {
                return Ok(Value::Null);
            };
            let Some(profile) = index_am_profile(class.relam) else {
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
            let Some(index_meta) = catalog.index_row_by_oid(relation_oid) else {
                return Ok(Value::Null);
            };
            let Some(class) = catalog.class_row_by_oid(index_meta.indexrelid) else {
                return Ok(Value::Null);
            };
            let Some(profile) = index_am_profile(class.relam) else {
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
                        && index_column_has_ordering_operator(catalog, &index_meta, column_index),
                ),
                Some(IndexPropertyKind::Returnable) => Value::Bool(match profile.returnability {
                    IndexReturnability::Never => false,
                    IndexReturnability::Always => true,
                    IndexReturnability::SpgistCanReturnData => {
                        matches!(
                            index_meta.indclass.get(column_index).copied(),
                            Some(
                                BOX_SPGIST_OPCLASS_OID
                                    | POLY_SPGIST_OPCLASS_OID
                                    | INET_SPGIST_OPCLASS_OID
                                    | QUAD_POINT_SPGIST_OPCLASS_OID
                                    | KD_POINT_SPGIST_OPCLASS_OID
                                    | TEXT_SPGIST_OPCLASS_OID
                            )
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

fn catalog_lookup(ctx: Option<&ExecutorContext>) -> Option<&dyn CatalogLookup> {
    ctx.and_then(|ctx| ctx.catalog.as_deref())
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

fn type_identity_text(catalog: &dyn CatalogLookup, type_oid: u32) -> String {
    expr_reg::format_type_text(type_oid, None, catalog)
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
    let object_kind = if proc_row.prokind == 'p' {
        "procedure"
    } else {
        "function"
    };
    format!(
        "{object_kind} {}",
        function_signature_text(proc_row, catalog)
    )
}

fn function_arguments_text(
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> String {
    if proc_row.prokind == 'a'
        && let Some(aggregate_row) = catalog.aggregate_by_fnoid(proc_row.oid)
        && matches!(aggregate_row.aggkind, 'o' | 'h')
    {
        return aggregate_function_arguments_text(proc_row, &aggregate_row, catalog);
    }
    let names = proc_row.proargnames.as_deref().unwrap_or(&[]);
    let defaults = proc_arg_defaults(proc_row);
    if let (Some(types), Some(modes)) = (
        proc_row.proallargtypes.as_deref(),
        proc_row.proargmodes.as_deref(),
    ) {
        let mut input_index = 0usize;
        return types
            .iter()
            .copied()
            .enumerate()
            .map(|(index, type_oid)| {
                let mode = modes.get(index).copied().unwrap_or(b'i');
                let default = if matches!(mode, b'i' | b'b' | b'v') {
                    let default = defaults.get(input_index).and_then(|value| value.as_deref());
                    input_index += 1;
                    default
                } else {
                    None
                };
                format_function_arg(
                    mode,
                    names.get(index).map(String::as_str),
                    type_oid,
                    default,
                    catalog,
                    proc_row.prokind == 'p',
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
    }
    proc_row
        .proargtypes
        .split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .enumerate()
        .map(|(index, type_oid)| {
            format_function_arg(
                b'i',
                names.get(index).map(String::as_str),
                type_oid,
                defaults.get(index).and_then(|value| value.as_deref()),
                catalog,
                proc_row.prokind == 'p',
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn proc_arg_defaults(proc_row: &crate::include::catalog::PgProcRow) -> Vec<Option<String>> {
    let input_count = proc_row.pronargs.max(0) as usize;
    let Some(defaults) = proc_row.proargdefaults.as_deref() else {
        return vec![None; input_count];
    };
    if let Ok(parsed) = serde_json::from_str::<Vec<Option<String>>>(defaults)
        && parsed.len() == input_count
    {
        return parsed;
    }
    let legacy = defaults
        .split_whitespace()
        .map(|default| Some(default.to_string()))
        .collect::<Vec<_>>();
    let mut aligned = vec![None; input_count.saturating_sub(legacy.len())];
    aligned.extend(legacy);
    aligned.resize(input_count, None);
    aligned
}

fn aggregate_function_arguments_text(
    proc_row: &crate::include::catalog::PgProcRow,
    aggregate_row: &crate::include::catalog::PgAggregateRow,
    catalog: &dyn CatalogLookup,
) -> String {
    let types = proc_arg_type_oids(proc_row);
    let names = proc_row.proargnames.as_deref().unwrap_or(&[]);
    let modes = proc_row.proargmodes.as_deref().unwrap_or(&[]);
    let direct_count = usize::try_from(aggregate_row.aggnumdirectargs)
        .unwrap_or(0)
        .min(types.len());
    let direct_text = types
        .iter()
        .copied()
        .take(direct_count)
        .enumerate()
        .map(|(index, type_oid)| {
            format_function_arg(
                modes.get(index).copied().unwrap_or(b'i'),
                names.get(index).map(String::as_str),
                type_oid,
                None,
                catalog,
                false,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let order_text = types
        .iter()
        .copied()
        .enumerate()
        .skip(direct_count)
        .map(|(index, type_oid)| {
            format_function_arg(
                modes.get(index).copied().unwrap_or(b'i'),
                names.get(index).map(String::as_str),
                type_oid,
                None,
                catalog,
                false,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    match (direct_text.is_empty(), order_text.is_empty()) {
        (true, true) => String::new(),
        (false, true) => direct_text,
        (true, false) => format!("ORDER BY {order_text}"),
        (false, false) => format!("{direct_text} ORDER BY {order_text}"),
    }
}

fn proc_arg_type_oids(proc_row: &crate::include::catalog::PgProcRow) -> Vec<u32> {
    proc_row.proallargtypes.clone().unwrap_or_else(|| {
        proc_row
            .proargtypes
            .split_whitespace()
            .filter_map(|oid| oid.parse::<u32>().ok())
            .collect()
    })
}

fn format_function_arg(
    mode: u8,
    name: Option<&str>,
    type_oid: u32,
    default: Option<&str>,
    catalog: &dyn CatalogLookup,
    include_in_mode: bool,
) -> String {
    let mode_text = match mode {
        b'i' if include_in_mode => Some("IN"),
        b'o' => Some("OUT"),
        b'b' => Some("INOUT"),
        b'v' => Some("VARIADIC"),
        b't' => Some("TABLE"),
        _ => None,
    };
    let mut parts = Vec::new();
    if let Some(mode_text) = mode_text {
        parts.push(mode_text.to_string());
    }
    if let Some(name) = name.filter(|name| !name.is_empty()) {
        parts.push(quote_identifier(name));
    }
    parts.push(type_identity_text(catalog, type_oid));
    if let Some(default) = default {
        parts.push("DEFAULT".into());
        parts.push(default.into());
    }
    parts.join(" ")
}

fn function_result_text(
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    if proc_row.prokind == 'p' {
        return None;
    }
    let result = type_identity_text(catalog, proc_row.prorettype);
    Some(if proc_row.proretset {
        format!("SETOF {result}")
    } else {
        result
    })
}

fn function_definition_text(
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> String {
    let kind = if proc_row.prokind == 'p' {
        "PROCEDURE"
    } else {
        "FUNCTION"
    };
    let schema = catalog
        .namespace_row_by_oid(proc_row.pronamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| proc_row.pronamespace.to_string());
    let language = catalog
        .language_row_by_oid(proc_row.prolang)
        .map(|row| row.lanname)
        .unwrap_or_else(|| proc_row.prolang.to_string());
    let mut lines = vec![format!(
        "CREATE OR REPLACE {kind} {}({})",
        quote_qualified_identifier(&schema, &proc_row.proname),
        function_arguments_text(proc_row, catalog)
    )];
    if let Some(result) = function_result_text(proc_row, catalog) {
        lines.push(format!(" RETURNS {result}"));
    }
    lines.push(format!(" LANGUAGE {}", quote_identifier(&language)));
    let attributes = function_definition_attributes(proc_row, &language);
    if !attributes.is_empty() {
        lines.push(format!(" {}", attributes.join(" ")));
    }
    let signature = lines.join("\n");
    if proc_row.prokind == 'p'
        && let Some(body) = format_sql_standard_procedure_body(proc_row, catalog)
    {
        return format!("{signature}\n {body}\n");
    }
    if let Some(body) = sql_standard_function_body(&proc_row.prosrc) {
        return format!("{signature}\n{body}\n");
    }
    let tag = if proc_row.prokind == 'p' {
        "$procedure$"
    } else {
        "$function$"
    };
    let body = proc_row.prosrc.trim().replace(tag, &format!("{tag} "));
    format!("{signature}\nAS {tag}\n{body}\n{tag}\n")
}

fn function_definition_attributes(
    proc_row: &crate::include::catalog::PgProcRow,
    language: &str,
) -> Vec<String> {
    let mut attributes = Vec::new();
    match proc_row.provolatile {
        'i' => attributes.push("IMMUTABLE".to_string()),
        's' => attributes.push("STABLE".to_string()),
        _ => {}
    }
    match proc_row.proparallel {
        's' => attributes.push("PARALLEL SAFE".to_string()),
        'r' => attributes.push("PARALLEL RESTRICTED".to_string()),
        _ => {}
    }
    if proc_row.proisstrict {
        attributes.push("STRICT".to_string());
    }
    if proc_row.prosecdef {
        attributes.push("SECURITY DEFINER".to_string());
    }
    if proc_row.proleakproof {
        attributes.push("LEAKPROOF".to_string());
    }
    let default_cost = if language.eq_ignore_ascii_case("sql") {
        100.0
    } else {
        1.0
    };
    if (proc_row.procost - default_cost).abs() > f64::EPSILON {
        attributes.push(format!("COST {}", format_function_cost(proc_row.procost)));
    }
    attributes
}

fn format_function_cost(cost: f64) -> String {
    if cost.fract().abs() < f64::EPSILON {
        format!("{cost:.0}")
    } else {
        cost.to_string()
    }
}

fn sql_standard_function_body(body: &str) -> Option<String> {
    let trimmed = body.trim();
    let lowered = trimmed.to_ascii_lowercase();
    (lowered.starts_with("begin atomic") || lowered.starts_with("return "))
        .then(|| trimmed.trim_end_matches(';').trim_end().to_string())
}

fn sql_standard_procedure_body(body: &str) -> Option<&str> {
    body.trim_start()
        .to_ascii_lowercase()
        .starts_with("begin atomic")
        .then_some(body.trim())
}

fn sql_standard_procedure_body_inner(body: &str) -> Option<&str> {
    let trimmed = sql_standard_procedure_body(body)?;
    let without_trailing_semicolon = trimmed.trim_end_matches(';').trim_end();
    let lowered_without_semicolon = without_trailing_semicolon.to_ascii_lowercase();
    let end = if lowered_without_semicolon.ends_with("end") {
        without_trailing_semicolon.len().saturating_sub("end".len())
    } else {
        trimmed.len()
    };
    trimmed.get("begin atomic".len()..end).map(str::trim)
}

fn format_sql_standard_procedure_body(
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    let inner = sql_standard_procedure_body_inner(&proc_row.prosrc)?;
    let mut lines = vec!["BEGIN ATOMIC".to_string()];
    for statement in split_sql_standard_body_statements_for_display(inner) {
        if let Some(insert) = format_sql_standard_insert_statement(&statement, proc_row, catalog) {
            lines.extend(insert.lines().map(str::to_string));
        } else {
            lines.push(format!("  {};", statement.trim().trim_end_matches(';')));
        }
    }
    lines.push("END".to_string());
    Some(lines.join("\n"))
}

fn split_sql_standard_body_statements_for_display(body: &str) -> Vec<String> {
    body.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty() && !statement.eq_ignore_ascii_case("end"))
        .map(str::to_string)
        .collect()
}

fn format_sql_standard_insert_statement(
    statement: &str,
    proc_row: &crate::include::catalog::PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let rest = lower
        .starts_with("insert into ")
        .then(|| trimmed.get("insert into ".len()..))??;
    let values_pos = rest.to_ascii_lowercase().find(" values ")?;
    let target = rest[..values_pos].trim();
    if target.contains('(') {
        return None;
    }
    let values = rest[values_pos + " values ".len()..].trim();
    let relation = catalog.lookup_any_relation(target)?;
    let column_names = relation
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>();
    if column_names.is_empty() {
        return None;
    }
    let qualified_values = qualify_sql_standard_body_args(values, proc_row);
    Some(format!(
        "  INSERT INTO {target} ({})\n    VALUES {qualified_values};",
        column_names.join(", ")
    ))
}

fn qualify_sql_standard_body_args(
    sql: &str,
    proc_row: &crate::include::catalog::PgProcRow,
) -> String {
    let Some(names) = proc_row.proargnames.as_ref() else {
        return sql.to_string();
    };
    let modes = proc_row.proargmodes.as_deref().unwrap_or(&[]);
    let input_names = names
        .iter()
        .enumerate()
        .filter(|(index, name)| {
            !name.is_empty() && !matches!(modes.get(*index).copied().unwrap_or(b'i'), b'o' | b't')
        })
        .map(|(_, name)| name.as_str())
        .collect::<Vec<_>>();
    if input_names.is_empty() {
        return sql.to_string();
    }

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            let start = index;
            index += 1;
            while index < bytes.len() {
                if bytes[index] == b'\'' {
                    index += 1;
                    if bytes.get(index) == Some(&b'\'') {
                        index += 1;
                        continue;
                    }
                    break;
                }
                index += 1;
            }
            out.push_str(&sql[start..index]);
            continue;
        }
        if is_sql_identifier_start(bytes[index]) {
            let start = index;
            index += 1;
            while index < bytes.len() && is_sql_identifier_continue(bytes[index]) {
                index += 1;
            }
            let ident = &sql[start..index];
            let preceded_by_dot = sql[..start].trim_end().ends_with('.');
            if !preceded_by_dot
                && input_names
                    .iter()
                    .any(|name| ident.eq_ignore_ascii_case(name))
            {
                out.push_str(&quote_identifier(&proc_row.proname));
                out.push('.');
                out.push_str(&quote_identifier(ident));
            } else {
                out.push_str(ident);
            }
            continue;
        }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
}

fn is_sql_identifier_start(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphabetic()
}

fn is_sql_identifier_continue(byte: u8) -> bool {
    is_sql_identifier_start(byte) || byte.is_ascii_digit()
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

fn catalog_is_temp_schema_name(schema_name: &str) -> bool {
    schema_name.eq_ignore_ascii_case("pg_temp")
        || schema_name.to_ascii_lowercase().starts_with("pg_temp_")
}

fn catalog_visibility_search_path(catalog: &dyn CatalogLookup) -> Vec<String> {
    let configured = catalog.search_path();
    let mut search_path = Vec::new();
    if !configured
        .iter()
        .any(|schema| schema.eq_ignore_ascii_case("pg_catalog"))
    {
        search_path.push("pg_catalog".into());
    }
    search_path.extend(configured);
    search_path
}

fn catalog_object_visible_in_search_path(
    catalog: &dyn CatalogLookup,
    target_oid: u32,
    target_namespace_oid: u32,
    target_name: &str,
    mut same_name_oid_in_namespace: impl FnMut(u32, &str) -> Option<u32>,
) -> bool {
    if catalog
        .namespace_row_by_oid(target_namespace_oid)
        .is_some_and(|namespace| catalog_is_temp_schema_name(&namespace.nspname))
    {
        return false;
    }
    for schema_name in catalog_visibility_search_path(catalog) {
        if catalog_is_temp_schema_name(&schema_name) {
            continue;
        }
        let Some(namespace) = catalog
            .namespace_rows()
            .into_iter()
            .find(|row| row.nspname.eq_ignore_ascii_case(&schema_name))
        else {
            continue;
        };
        if let Some(candidate_oid) = same_name_oid_in_namespace(namespace.oid, target_name) {
            return candidate_oid == target_oid;
        }
    }
    false
}

fn eval_catalog_visibility_result(
    values: &[Value],
    function_name: &'static str,
    mut is_visible: impl FnMut(u32) -> Result<Option<bool>, ExecError>,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let oid = oid_arg_to_u32(value, function_name)?;
            Ok(is_visible(oid)?.map(Value::Bool).unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: function_name,
            actual: format!("{function_name}({} args)", values.len()),
        })),
    }
}

fn eval_pg_type_is_visible(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_type_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog.type_by_oid(oid) else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.typnamespace,
            &row.typname,
            |namespace_oid, typname| {
                catalog
                    .type_rows()
                    .into_iter()
                    .find(|candidate: &PgTypeRow| {
                        candidate.typnamespace == namespace_oid
                            && candidate.typname.eq_ignore_ascii_case(typname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_operator_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_operator_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog.operator_by_oid(oid) else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.oprnamespace,
            &row.oprname,
            |namespace_oid, oprname| {
                catalog
                    .operator_rows()
                    .into_iter()
                    .find(|candidate: &PgOperatorRow| {
                        candidate.oprnamespace == namespace_oid
                            && candidate.oprname.eq_ignore_ascii_case(oprname)
                            && candidate.oprleft == row.oprleft
                            && candidate.oprright == row.oprright
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_opclass_is_visible(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_opclass_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog
            .opclass_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.opcnamespace,
            &row.opcname,
            |namespace_oid, opcname| {
                catalog
                    .opclass_rows()
                    .into_iter()
                    .find(|candidate: &PgOpclassRow| {
                        candidate.opcnamespace == namespace_oid
                            && candidate.opcmethod == row.opcmethod
                            && candidate.opcname.eq_ignore_ascii_case(opcname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_opfamily_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_opfamily_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog
            .opfamily_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.opfnamespace,
            &row.opfname,
            |namespace_oid, opfname| {
                catalog
                    .opfamily_rows()
                    .into_iter()
                    .find(|candidate: &PgOpfamilyRow| {
                        candidate.opfnamespace == namespace_oid
                            && candidate.opfmethod == row.opfmethod
                            && candidate.opfname.eq_ignore_ascii_case(opfname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_conversion_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_conversion_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog
            .conversion_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.connamespace,
            &row.conname,
            |namespace_oid, conname| {
                catalog
                    .conversion_rows()
                    .into_iter()
                    .find(|candidate: &PgConversionRow| {
                        candidate.connamespace == namespace_oid
                            && candidate.conname.eq_ignore_ascii_case(conname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_ts_parser_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_ts_parser_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog
            .ts_parser_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.prsnamespace,
            &row.prsname,
            |namespace_oid, prsname| {
                catalog
                    .ts_parser_rows()
                    .into_iter()
                    .find(|candidate: &PgTsParserRow| {
                        candidate.prsnamespace == namespace_oid
                            && candidate.prsname.eq_ignore_ascii_case(prsname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_ts_dict_is_visible(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_ts_dict_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog
            .ts_dict_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.dictnamespace,
            &row.dictname,
            |namespace_oid, dictname| {
                catalog
                    .ts_dict_rows()
                    .into_iter()
                    .find(|candidate: &PgTsDictRow| {
                        candidate.dictnamespace == namespace_oid
                            && candidate.dictname.eq_ignore_ascii_case(dictname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_ts_template_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_ts_template_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog
            .ts_template_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.tmplnamespace,
            &row.tmplname,
            |namespace_oid, tmplname| {
                catalog
                    .ts_template_rows()
                    .into_iter()
                    .find(|candidate: &PgTsTemplateRow| {
                        candidate.tmplnamespace == namespace_oid
                            && candidate.tmplname.eq_ignore_ascii_case(tmplname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_ts_config_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    eval_catalog_visibility_result(values, "pg_ts_config_is_visible", |oid| {
        let catalog = executor_catalog(ctx)?;
        let Some(row) = catalog
            .ts_config_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.cfgnamespace,
            &row.cfgname,
            |namespace_oid, cfgname| {
                catalog
                    .ts_config_rows()
                    .into_iter()
                    .find(|candidate: &PgTsConfigRow| {
                        candidate.cfgnamespace == namespace_oid
                            && candidate.cfgname.eq_ignore_ascii_case(cfgname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

fn eval_pg_rust_internal_binary_coercible(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left_oid = oid_arg_to_u32(left, "pg_rust_internal_binary_coercible")?;
            let right_oid = oid_arg_to_u32(right, "pg_rust_internal_binary_coercible")?;
            if left_oid == 0 || right_oid == 0 {
                return Ok(Value::Bool(false));
            }
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

fn eval_pg_encoding_to_char(values: &[Value]) -> Result<Value, ExecError> {
    let encoding = match values {
        [Value::Int16(value)] => i32::from(*value),
        [Value::Int32(value)] => *value,
        [Value::Int64(value)] => i32::try_from(*value).unwrap_or(-1),
        [Value::Null] => return Ok(Value::Null),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "pg_encoding_to_char",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Int32(0),
            });
        }
    };
    let name = match encoding {
        0 => "SQL_ASCII",
        6 => "UTF8",
        _ => "",
    };
    Ok(Value::Text(name.into()))
}

fn eval_convert(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [Value::Bytea(bytes), _, _] => Ok(Value::Bytea(bytes.clone())),
        _ => Err(ExecError::TypeMismatch {
            op: "convert",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        }),
    }
}

fn eval_greatest(values: &[Value]) -> Result<Value, ExecError> {
    let mut best: Option<Value> = None;
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        let replace = match best.as_ref() {
            None => true,
            Some(current) => {
                compare_order_values(current, value, None, None, false)? == std::cmp::Ordering::Less
            }
        };
        if replace {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(Value::Null))
}

fn eval_least(values: &[Value]) -> Result<Value, ExecError> {
    let mut best: Option<Value> = None;
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        let replace = match best.as_ref() {
            None => true,
            Some(current) => {
                compare_order_values(current, value, None, None, false)?
                    == std::cmp::Ordering::Greater
            }
        };
        if replace {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(Value::Null))
}

fn lookup_system_binding(
    bindings: &[crate::include::nodes::execnodes::SystemVarBinding],
    varno: usize,
) -> Option<Value> {
    bindings
        .iter()
        .find(|binding| binding.varno == varno)
        .map(|binding| Value::Int64(i64::from(binding.table_oid)))
}

fn ctid_value(tid: crate::include::access::htup::ItemPointerData) -> Value {
    Value::Text(CompactString::from_owned(format!(
        "({},{})",
        tid.block_number, tid.offset_number
    )))
}

fn lookup_ctid_binding(
    bindings: &[crate::include::nodes::execnodes::SystemVarBinding],
    varno: usize,
) -> Option<Value> {
    bindings
        .iter()
        .find(|binding| binding.varno == varno)
        .and_then(|binding| binding.tid)
        .map(ctid_value)
}

fn builtin_function_for_expr(funcid: u32) -> Result<BuiltinScalarFunction, ExecError> {
    builtin_scalar_function_for_proc_oid(funcid).ok_or_else(|| ExecError::DetailedError {
        message: format!("no builtin implementation for function oid {funcid}").into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}

fn role_catalog(ctx: &ExecutorContext) -> Result<&dyn CatalogLookup, ExecError> {
    ctx.catalog
        .as_deref()
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
    if name == "server_encoding" {
        return Ok(Value::Text("UTF8".into()));
    }
    if name == "datestyle" {
        return Ok(Value::Text(
            crate::backend::utils::misc::guc_datetime::format_datestyle(&ctx.datetime_config)
                .into(),
        ));
    }
    match name.as_str() {
        "block_size" => return Ok(Value::Text("8192".into())),
        "fsync" => return Ok(Value::Text("on".into())),
        "synchronous_commit" => return Ok(Value::Text("on".into())),
        "wal_sync_method" => return Ok(Value::Text("fsync".into())),
        _ => {}
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
    match name.as_str() {
        "block_size" => return Ok(Value::Text("8192".into())),
        "fsync" => return Ok(Value::Text("on".into())),
        "server_encoding" => return Ok(Value::Text("UTF8".into())),
        "synchronous_commit" => return Ok(Value::Text("on".into())),
        "wal_sync_method" => return Ok(Value::Text("fsync".into())),
        _ => {}
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

fn eval_pg_settings_get_flags(values: &[Value]) -> Result<Value, ExecError> {
    let name = match values {
        [Value::Text(name)] => normalize_guc_name(name),
        [Value::Null] => return Ok(Value::Null),
        [other] => {
            return Err(ExecError::TypeMismatch {
                op: "pg_settings_get_flags",
                left: other.clone(),
                right: Value::Text("".into()),
            });
        }
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_settings_get_flags(name)",
                actual: format!("pg_settings_get_flags({} args)", values.len()),
            }));
        }
    };
    let flags: &[&str] = match name.as_str() {
        "default_statistics_target" => &[],
        _ => return Ok(Value::Null),
    };
    Ok(Value::PgArray(
        ArrayValue::from_1d(
            flags
                .iter()
                .map(|flag| Value::Text((*flag).into()))
                .collect(),
        )
        .with_element_type_oid(TEXT_TYPE_OID),
    ))
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
        .and_then(|ctx| ctx.catalog.as_deref())
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

fn relation_name_for_regclass_oid(oid: u32, catalog: Option<&dyn CatalogLookup>) -> Option<String> {
    let catalog = catalog?;
    catalog.class_row_by_oid(oid).map(|row| row.relname)
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
        relation_name_for_regclass_oid(oid, ctx.and_then(|ctx| ctx.catalog.as_deref()))
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
        ctx.and_then(|ctx| ctx.catalog.as_deref())
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

#[derive(Clone, Copy)]
enum ForeignPrivilegeKind {
    ForeignDataWrapper,
    Server,
}

fn eval_has_foreign_privilege_function(
    kind: ForeignPrivilegeKind,
    values: &[Value],
    ctx: Option<&ExecutorContext>,
) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let ctx = ctx.ok_or_else(|| ExecError::DetailedError {
        message: "foreign privilege lookup requires executor context".into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    let catalog = ctx
        .catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "foreign privilege lookup requires a visible catalog".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    let (role_oid, object_value, privilege_value) = match values {
        [object_value, privilege_value] => (ctx.current_user_oid, object_value, privilege_value),
        [role_value, object_value, privilege_value] => (
            foreign_privilege_role_oid(role_value, catalog)?,
            object_value,
            privilege_value,
        ),
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "has foreign privilege arguments",
                actual: format!("{} arguments", values.len()),
            }));
        }
    };
    if !privilege_value
        .as_text()
        .is_some_and(|privilege| privilege.eq_ignore_ascii_case("USAGE"))
    {
        return Ok(Value::Bool(false));
    }
    let authids = CatalogLookup::authid_rows(catalog);
    let auth_members = CatalogLookup::auth_members_rows(catalog);
    let Some(role) = authids.iter().find(|role| role.oid == role_oid) else {
        return Ok(Value::Bool(false));
    };
    if role.rolsuper {
        return Ok(Value::Bool(true));
    }
    let Some((owner_oid, acl)) = foreign_privilege_object_acl(kind, object_value, catalog)? else {
        return Ok(Value::Bool(false));
    };
    if crate::backend::catalog::role_memberships::has_effective_membership(
        role_oid,
        owner_oid,
        &authids,
        &auth_members,
    ) {
        return Ok(Value::Bool(true));
    }
    let effective_names = authids
        .iter()
        .filter(|candidate| {
            crate::backend::catalog::role_memberships::has_effective_membership(
                role_oid,
                candidate.oid,
                &authids,
                &auth_members,
            )
        })
        .map(|role| role.rolname.as_str())
        .chain(std::iter::once(""))
        .collect::<Vec<_>>();
    Ok(Value::Bool(acl.unwrap_or_default().iter().any(|item| {
        let Some((grantee, rest)) = item.split_once('=') else {
            return false;
        };
        let Some((privileges, _)) = rest.split_once('/') else {
            return false;
        };
        effective_names.contains(&grantee) && privileges.contains('U')
    })))
}

fn foreign_privilege_role_oid(
    value: &Value,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ExecError> {
    if let Some(role_name) = value.as_text() {
        return CatalogLookup::authid_rows(catalog)
            .into_iter()
            .find(|role| role.rolname == role_name)
            .map(|role| role.oid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("role \"{role_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
    }
    oid_arg_to_u32(value, "has foreign privilege")
}

fn foreign_privilege_object_acl(
    kind: ForeignPrivilegeKind,
    value: &Value,
    catalog: &dyn CatalogLookup,
) -> Result<Option<(u32, Option<Vec<String>>)>, ExecError> {
    match kind {
        ForeignPrivilegeKind::ForeignDataWrapper => {
            let rows = CatalogLookup::foreign_data_wrapper_rows(catalog);
            Ok(if let Some(name) = value.as_text() {
                rows.into_iter()
                    .find(|row| row.fdwname.eq_ignore_ascii_case(name))
                    .map(|row| (row.fdwowner, row.fdwacl))
            } else {
                let oid = oid_arg_to_u32(value, "has_foreign_data_wrapper_privilege")?;
                rows.into_iter()
                    .find(|row| row.oid == oid)
                    .map(|row| (row.fdwowner, row.fdwacl))
            })
        }
        ForeignPrivilegeKind::Server => {
            let rows = CatalogLookup::foreign_server_rows(catalog);
            Ok(if let Some(name) = value.as_text() {
                rows.into_iter()
                    .find(|row| row.srvname.eq_ignore_ascii_case(name))
                    .map(|row| (row.srvowner, row.srvacl))
            } else {
                let oid = oid_arg_to_u32(value, "has_server_privilege")?;
                rows.into_iter()
                    .find(|row| row.oid == oid)
                    .map(|row| (row.srvowner, row.srvacl))
            })
        }
    }
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
            | BuiltinScalarFunction::PgRestoreRelationStats
            | BuiltinScalarFunction::PgClearRelationStats
            | BuiltinScalarFunction::PgRestoreAttributeStats
            | BuiltinScalarFunction::PgClearAttributeStats
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
                    BuiltinScalarFunction::PgRestoreRelationStats => "pg_restore_relation_stats",
                    BuiltinScalarFunction::PgClearRelationStats => "pg_clear_relation_stats",
                    BuiltinScalarFunction::PgRestoreAttributeStats => "pg_restore_attribute_stats",
                    BuiltinScalarFunction::PgClearAttributeStats => "pg_clear_attribute_stats",
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

fn sequence_catalog(ctx: &ExecutorContext) -> Result<&dyn CatalogLookup, ExecError> {
    ctx.catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence lookup requires a visible catalog".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn executor_catalog(ctx: &ExecutorContext) -> Result<&dyn CatalogLookup, ExecError> {
    ctx.catalog
        .as_deref()
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
                PG_CLASS_RELATION_OID if objsubid > 0 => catalog
                    .attribute_rows_for_relation(objid)
                    .into_iter()
                    .find(|row| i32::from(row.attnum) == objsubid)
                    .and_then(|row| row.attacl.map(text_acl_array))
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

fn eval_make_acl_item(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _, _]
        | [_, Value::Null, _, _]
        | [_, _, Value::Null, _]
        | [_, _, _, Value::Null] => Ok(Value::Null),
        [grantee, grantor, privileges, grant_option] => {
            let grantee = oid_arg_to_u32(grantee, "makeaclitem")?;
            let grantor = oid_arg_to_u32(grantor, "makeaclitem")?;
            let Some(privileges) = privileges.as_text() else {
                return Err(ExecError::TypeMismatch {
                    op: "makeaclitem",
                    left: privileges.clone(),
                    right: Value::Text("".into()),
                });
            };
            let Value::Bool(grant_option) = grant_option else {
                return Err(ExecError::TypeMismatch {
                    op: "makeaclitem",
                    left: grant_option.clone(),
                    right: Value::Bool(false),
                });
            };
            let mut privilege_bits = acl_privilege_abbrev(privileges);
            if *grant_option {
                privilege_bits = privilege_bits
                    .chars()
                    .flat_map(|ch| [ch, '*'])
                    .collect::<String>();
            }
            Ok(Value::Text(
                format!("{grantee}={privilege_bits}/{grantor}").into(),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "makeaclitem(grantee, grantor, privileges, grant_option)",
            actual: format!("MakeAclItem({} args)", values.len()),
        })),
    }
}

fn acl_privilege_abbrev(privileges: &str) -> String {
    privileges
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let lower = part.to_ascii_lowercase();
            match lower.as_str() {
                "select" => "r",
                "insert" => "a",
                "update" => "w",
                "delete" => "d",
                "truncate" => "D",
                "references" => "x",
                "trigger" => "t",
                "execute" => "X",
                "usage" => "U",
                "create" => "C",
                "connect" => "c",
                "temporary" | "temp" => "T",
                _ => part,
            }
        })
        .collect()
}

fn eval_obj_description(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [objoid] => {
            let objoid = oid_arg_to_u32(objoid, "obj_description")?;
            eval_obj_description_for_classoid(objoid, PG_CLASS_RELATION_OID, ctx)
        }
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
            eval_obj_description_for_classoid(objoid, classoid, ctx)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "obj_description(oid, catalog_name)",
            actual: format!("ObjDescription({} args)", values.len()),
        })),
    }
}

fn eval_shobj_description(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [objoid, class_name] => {
            let objoid = oid_arg_to_u32(objoid, "shobj_description")?;
            let Some(class_name) = class_name.as_text() else {
                return Err(ExecError::TypeMismatch {
                    op: "shobj_description",
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
            eval_obj_description_for_classoid(objoid, classoid, ctx)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "shobj_description(oid, catalog_name)",
            actual: format!("ShobjDescription({} args)", values.len()),
        })),
    }
}

fn eval_obj_description_for_classoid(
    objoid: u32,
    classoid: u32,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
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
    let row = pg_description_row_from_values(row).map_err(|err| ExecError::DetailedError {
        message: format!("invalid pg_description row: {err:?}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })?;
    Ok(Value::Text(row.description.into()))
}

pub(crate) fn eval_pg_describe_object(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
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
                "pg_type" => catalog.type_by_oid(objid).map(|row| {
                    format!(
                        "type {}",
                        expr_reg::format_type_text(row.oid, None, catalog)
                    )
                }),
                "pg_proc" => catalog
                    .proc_row_by_oid(objid)
                    .map(|row| function_identity_text(&row, catalog)),
                "pg_cast" => catalog
                    .cast_rows()
                    .into_iter()
                    .find(|row| row.oid == objid)
                    .map(|row| {
                        format!(
                            "cast from {} to {}",
                            expr_reg::format_type_text(row.castsource, None, catalog),
                            expr_reg::format_type_text(row.casttarget, None, catalog)
                        )
                    }),
                "pg_operator" => catalog
                    .operator_by_oid(objid)
                    .map(|row| operator_identity_text(&row, catalog)),
                "pg_event_trigger" => catalog
                    .event_trigger_row_by_oid(objid)
                    .map(|row| format!("event trigger {}", quote_identifier(&row.evtname))),
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

pub(crate) fn eval_pg_identify_object(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [classid, objid, objsubid] => {
            let address = object_address_args(classid, objid, objsubid, "pg_identify_object")?;
            let catalog = executor_catalog(ctx)?;
            if let Some(evtname) = event_trigger_name_for_address(catalog, address) {
                return Ok(Value::Record(RecordValue::anonymous(vec![
                    ("type".into(), Value::Text("event trigger".into())),
                    ("schema".into(), Value::Null),
                    ("name".into(), Value::Text(evtname.clone().into())),
                    ("identity".into(), Value::Text(evtname.into())),
                ])));
            }
            Ok(Value::Null)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_identify_object(classid, objid, objsubid)",
            actual: format!("PgIdentifyObject({} args)", values.len()),
        })),
    }
}

pub(crate) fn eval_pg_identify_object_as_address(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [classid, objid, objsubid] => {
            let address =
                object_address_args(classid, objid, objsubid, "pg_identify_object_as_address")?;
            let catalog = executor_catalog(ctx)?;
            if let Some(evtname) = event_trigger_name_for_address(catalog, address) {
                return Ok(Value::Record(RecordValue::anonymous(vec![
                    ("type".into(), Value::Text("event trigger".into())),
                    (
                        "object_names".into(),
                        Value::Array(vec![Value::Text(evtname.into())]),
                    ),
                    ("object_args".into(), Value::Array(Vec::new())),
                ])));
            }
            Ok(Value::Null)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_identify_object_as_address(classid, objid, objsubid)",
            actual: format!("PgIdentifyObjectAsAddress({} args)", values.len()),
        })),
    }
}

pub(crate) fn eval_pg_get_object_address(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
        [object_type, object_names, object_args] => {
            let object_type = object_type.as_text().unwrap_or_default();
            let object_names = text_array_values(object_names);
            let object_args = text_array_values(object_args);
            let catalog = executor_catalog(ctx)?;
            // :HACK: This covers the event_trigger regression's object-address
            // round trip. The long-term shape should route through a shared
            // PostgreSQL-like object-address resolver for all catalog classes.
            if object_type.eq_ignore_ascii_case("event trigger")
                && object_names.len() == 1
                && object_args.is_empty()
                && let Some(row) = catalog.event_trigger_row_by_name(&object_names[0])
            {
                return Ok(Value::Record(RecordValue::anonymous(vec![
                    (
                        "classid".into(),
                        Value::Int64(i64::from(PG_EVENT_TRIGGER_RELATION_OID)),
                    ),
                    ("objid".into(), Value::Int64(i64::from(row.oid))),
                    ("objsubid".into(), Value::Int32(0)),
                ])));
            }
            Ok(Value::Null)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_object_address(type, object_names, object_args)",
            actual: format!("PgGetObjectAddress({} args)", values.len()),
        })),
    }
}

fn object_address_args(
    classid: &Value,
    objid: &Value,
    objsubid: &Value,
    func_name: &'static str,
) -> Result<(u32, u32, u32), ExecError> {
    Ok((
        oid_arg_to_u32(classid, func_name)?,
        oid_arg_to_u32(objid, func_name)?,
        oid_arg_to_u32(objsubid, func_name)?,
    ))
}

fn event_trigger_name_for_address(
    catalog: &dyn CatalogLookup,
    address: (u32, u32, u32),
) -> Option<String> {
    let (classid, objid, objsubid) = address;
    if classid != PG_EVENT_TRIGGER_RELATION_OID || objsubid != 0 {
        return None;
    }
    catalog
        .event_trigger_row_by_oid(objid)
        .map(|row| row.evtname)
}

fn text_array_values(value: &Value) -> Vec<String> {
    match value {
        Value::Array(values) => values
            .iter()
            .filter_map(Value::as_text)
            .map(str::to_string)
            .collect(),
        Value::PgArray(array) => array
            .elements
            .iter()
            .filter_map(Value::as_text)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

fn eval_pg_event_trigger_table_rewrite_oid() -> Result<Value, ExecError> {
    current_event_trigger_table_rewrite()
        .map(|(oid, _)| Value::Int64(i64::from(oid)))
        .ok_or_else(|| ExecError::DetailedError {
            message: "pg_event_trigger_table_rewrite_oid() can only be called in a table_rewrite event trigger function".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })
}

fn eval_pg_event_trigger_table_rewrite_reason() -> Result<Value, ExecError> {
    current_event_trigger_table_rewrite()
        .map(|(_, reason)| Value::Int32(reason))
        .ok_or_else(|| ExecError::DetailedError {
            message: "pg_event_trigger_table_rewrite_reason() can only be called in a table_rewrite event trigger function".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })
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

fn eval_pg_function_is_visible(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_function_is_visible")?;
            Ok(Value::Bool(
                role_catalog(ctx)?.proc_row_by_oid(oid).is_some(),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_function_is_visible(oid)",
            actual: format!("PgFunctionIsVisible({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_function_arguments(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_get_function_arguments")?;
            let catalog = role_catalog(ctx)?;
            let Some(proc_row) = catalog.proc_row_by_oid(oid) else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(
                function_arguments_text(&proc_row, catalog).into(),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_function_arguments(oid)",
            actual: format!("PgGetFunctionArguments({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_function_result(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_get_function_result")?;
            let catalog = role_catalog(ctx)?;
            let Some(proc_row) = catalog.proc_row_by_oid(oid) else {
                return Ok(Value::Null);
            };
            Ok(function_result_text(&proc_row, catalog)
                .map(|text| Value::Text(text.into()))
                .unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_function_result(oid)",
            actual: format!("PgGetFunctionResult({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_functiondef(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [oid] => {
            let oid = oid_arg_to_u32(oid, "pg_get_functiondef")?;
            let catalog = role_catalog(ctx)?;
            let Some(proc_row) = catalog.proc_row_by_oid(oid) else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(
                function_definition_text(&proc_row, catalog).into(),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_functiondef(oid)",
            actual: format!("PgGetFunctionDef({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_expr(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [Value::Null, _, _] | [_, Value::Null] | [_, Value::Null, _] => {
            Ok(Value::Null)
        }
        [expr, relation] => eval_pg_get_expr_text(expr, relation, ctx),
        [expr, relation, _pretty] => eval_pg_get_expr_text(expr, relation, ctx),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_get_expr(pg_node_tree, oid [, pretty])",
            actual: format!("PgGetExpr({} args)", values.len()),
        })),
    }
}

fn eval_pg_get_expr_text(
    expr: &Value,
    relation: &Value,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let Some(text) = expr.as_text() else {
        return Ok(Value::Null);
    };
    let relation_oid = oid_arg_to_u32(relation, "pg_get_expr")?;
    if let Ok(bound) = deserialize_partition_bound(text) {
        let catalog = executor_catalog(ctx)?;
        if catalog
            .relation_by_oid(relation_oid)
            .and_then(|relation| relation.relpartbound)
            .as_deref()
            == Some(text)
        {
            return Ok(Value::Text(
                format_partition_bound_for_catalog(&bound).into(),
            ));
        }
    }
    let catalog = executor_catalog(ctx)?;
    if catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| row.contype == CONSTRAINT_CHECK && row.conbin.as_deref() == Some(text))
    {
        return Ok(Value::Text(normalize_check_expr_sql(text).into()));
    }
    Ok(Value::Text(
        canonicalize_catalog_expr_sql(text, relation_oid, catalog)
            .unwrap_or_else(|| text.to_string())
            .into(),
    ))
}

fn eval_pg_get_partkeydef(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let relation_oid = match values {
        [Value::Null] => return Ok(Value::Null),
        [relation_oid] => oid_arg_to_u32(relation_oid, "pg_get_partkeydef")?,
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_get_partkeydef(oid)",
                actual: format!("PgGetPartKeyDef({} args)", values.len()),
            }));
        }
    };
    let catalog = executor_catalog(ctx)?;
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Ok(Value::Null);
    };
    let Some(row) = relation.partitioned_table.as_ref() else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(
        format_partition_keydef_for_catalog(catalog, &relation, row)?.into(),
    ))
}

fn eval_pg_get_partition_constraintdef(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let relation_oid = match values {
        [Value::Null] => return Ok(Value::Null),
        [relation_oid] => oid_arg_to_u32(relation_oid, "pg_get_partition_constraintdef")?,
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_get_partition_constraintdef(oid)",
                actual: format!("PgGetPartitionConstraintDef({} args)", values.len()),
            }));
        }
    };
    let catalog = executor_catalog(ctx)?;
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Ok(Value::Null);
    };
    if !relation.relispartition {
        return Ok(Value::Null);
    }
    let mut relation_oid = relation.relation_oid;
    let mut levels = Vec::new();
    while let Some(parent_row) = catalog
        .inheritance_parents(relation_oid)
        .into_iter()
        .find(|row| !row.inhdetachpending)
    {
        let Some(child) = catalog.relation_by_oid(relation_oid) else {
            break;
        };
        let Some(parent) = catalog.relation_by_oid(parent_row.inhparent) else {
            break;
        };
        let Some(bound_text) = child.relpartbound.as_deref() else {
            break;
        };
        let bound = deserialize_partition_bound(bound_text).map_err(ExecError::Parse)?;
        if let Some(constraints) =
            partition_constraint_conditions_for_catalog(catalog, &parent, &bound)?
        {
            levels.push(constraints);
        }
        relation_oid = parent.relation_oid;
    }
    if levels.is_empty() {
        return Ok(Value::Null);
    }
    levels.reverse();
    let parts = levels.into_iter().flatten().collect::<Vec<_>>();
    Ok(Value::Text(format!("({})", parts.join(" AND ")).into()))
}

fn format_partition_bound_for_catalog(bound: &PartitionBoundSpec) -> String {
    match bound {
        PartitionBoundSpec::List {
            is_default: true, ..
        }
        | PartitionBoundSpec::Range {
            is_default: true, ..
        } => "DEFAULT".into(),
        PartitionBoundSpec::List { values, .. } => format!(
            "FOR VALUES IN ({})",
            values
                .iter()
                .map(partition_value_bound_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PartitionBoundSpec::Range { from, to, .. } => format!(
            "FOR VALUES FROM ({}) TO ({})",
            from.iter()
                .map(partition_range_datum_bound_literal)
                .collect::<Vec<_>>()
                .join(", "),
            to.iter()
                .map(partition_range_datum_bound_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PartitionBoundSpec::Hash { modulus, remainder } => {
            format!("FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})")
        }
    }
}

fn partition_range_datum_bound_literal(value: &PartitionRangeDatumValue) -> String {
    match value {
        PartitionRangeDatumValue::MinValue => "MINVALUE".into(),
        PartitionRangeDatumValue::MaxValue => "MAXVALUE".into(),
        PartitionRangeDatumValue::Value(value) => partition_value_bound_literal(value),
    }
}

fn partition_value_bound_literal(value: &SerializedPartitionValue) -> String {
    match value {
        SerializedPartitionValue::Null => "NULL".into(),
        SerializedPartitionValue::Text(text)
        | SerializedPartitionValue::Json(text)
        | SerializedPartitionValue::JsonPath(text)
        | SerializedPartitionValue::Xml(text)
        | SerializedPartitionValue::Numeric(text)
        | SerializedPartitionValue::Float64(text) => quote_sql_literal(text),
        SerializedPartitionValue::Jsonb(bytes) => {
            let text =
                crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap_or_default();
            quote_sql_literal(&text)
        }
        SerializedPartitionValue::Int16(value) if *value < 0 => {
            quote_sql_literal(&value.to_string())
        }
        SerializedPartitionValue::Int32(value) if *value < 0 => {
            quote_sql_literal(&value.to_string())
        }
        SerializedPartitionValue::Int64(value) if *value < 0 => {
            quote_sql_literal(&value.to_string())
        }
        SerializedPartitionValue::Int16(value) => value.to_string(),
        SerializedPartitionValue::Int32(value) => value.to_string(),
        SerializedPartitionValue::Int64(value) => value.to_string(),
        SerializedPartitionValue::Money(value) => value.to_string(),
        SerializedPartitionValue::Bool(value) => value.to_string(),
        SerializedPartitionValue::Date(_)
        | SerializedPartitionValue::Time(_)
        | SerializedPartitionValue::TimeTz { .. }
        | SerializedPartitionValue::Timestamp(_)
        | SerializedPartitionValue::TimestampTz(_)
        | SerializedPartitionValue::Array(_)
        | SerializedPartitionValue::Range(_)
        | SerializedPartitionValue::Multirange(_) => {
            quote_sql_literal(&partition_value_text(value))
        }
        SerializedPartitionValue::Bytea(bytes) => {
            let mut out = String::from("'\\\\x");
            for byte in bytes {
                out.push_str(&format!("{byte:02x}"));
            }
            out.push('\'');
            out
        }
        SerializedPartitionValue::InternalChar(byte) => {
            quote_sql_literal(&(*byte as char).to_string())
        }
    }
}

pub(crate) fn partition_constraint_conditions_for_catalog(
    catalog: &dyn CatalogLookup,
    parent: &crate::backend::parser::BoundRelation,
    bound: &PartitionBoundSpec,
) -> Result<Option<Vec<String>>, ExecError> {
    let Some(row) = parent.partitioned_table.as_ref() else {
        return Ok(None);
    };
    let key_names = partition_key_constraint_names_for_catalog(parent, row)?;
    let conditions = match bound {
        PartitionBoundSpec::List {
            is_default: true, ..
        }
        | PartitionBoundSpec::Range {
            is_default: true, ..
        }
        | PartitionBoundSpec::Hash { .. } => return Ok(None),
        PartitionBoundSpec::List { values, .. } => {
            if key_names.is_empty() {
                return Ok(None);
            }
            list_partition_constraint_conditions(&key_names[0], values)
        }
        PartitionBoundSpec::Range { from, to, .. } => {
            range_partition_constraint_conditions(&key_names, from, to)
        }
    };
    let _ = catalog;
    Ok(Some(conditions))
}

fn list_partition_constraint_conditions(
    key: &str,
    values: &[SerializedPartitionValue],
) -> Vec<String> {
    let mut conditions = Vec::new();
    let non_null = values
        .iter()
        .filter(|value| !matches!(value, SerializedPartitionValue::Null))
        .collect::<Vec<_>>();
    if !non_null.is_empty() {
        conditions.push(format!("({key} IS NOT NULL)"));
    }
    let value_conditions = values
        .iter()
        .map(|value| {
            if matches!(value, SerializedPartitionValue::Null) {
                format!("({key} IS NULL)")
            } else {
                format!("({key} = {})", partition_value_constraint_literal(value))
            }
        })
        .collect::<Vec<_>>();
    if value_conditions.len() == 1 {
        conditions.push(value_conditions[0].clone());
    } else if !value_conditions.is_empty() {
        conditions.push(format!("({})", value_conditions.join(" OR ")));
    }
    conditions
}

fn range_partition_constraint_conditions(
    keys: &[String],
    from: &[PartitionRangeDatumValue],
    to: &[PartitionRangeDatumValue],
) -> Vec<String> {
    let mut conditions = keys
        .iter()
        .map(|key| format!("({key} IS NOT NULL)"))
        .collect::<Vec<_>>();
    if let Some(lower) = range_partition_side_constraint(keys, from, true) {
        conditions.push(lower);
    }
    if let Some(upper) = range_partition_side_constraint(keys, to, false) {
        conditions.push(upper);
    }
    conditions
}

fn range_partition_side_constraint(
    keys: &[String],
    values: &[PartitionRangeDatumValue],
    lower: bool,
) -> Option<String> {
    let concrete = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| match value {
            PartitionRangeDatumValue::Value(value) => Some((index, value)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if concrete.is_empty() {
        return None;
    }
    let last_index = concrete.last().map(|(index, _)| *index)?;
    let upper_has_trailing_max = !lower
        && values
            .iter()
            .skip(last_index + 1)
            .any(|value| matches!(value, PartitionRangeDatumValue::MaxValue));
    let mut disjuncts = Vec::new();
    for (position, (index, value)) in concrete.iter().enumerate() {
        let key = keys.get(*index)?;
        let mut terms = concrete
            .iter()
            .take(position)
            .filter_map(|(prev_index, prev_value)| {
                keys.get(*prev_index).map(|prev_key| {
                    format!(
                        "({prev_key} = {})",
                        partition_value_constraint_literal(prev_value)
                    )
                })
            })
            .collect::<Vec<_>>();
        let op = if *index == last_index {
            if lower {
                ">="
            } else if upper_has_trailing_max {
                "<="
            } else {
                "<"
            }
        } else if lower {
            ">"
        } else {
            "<"
        };
        terms.push(format!(
            "({key} {op} {})",
            partition_value_constraint_literal(value)
        ));
        disjuncts.push(if terms.len() == 1 {
            terms.remove(0)
        } else {
            format!("({})", terms.join(" AND "))
        });
    }
    if disjuncts.len() == 1 {
        Some(disjuncts.remove(0))
    } else {
        Some(format!("({})", disjuncts.join(" OR ")))
    }
}

fn partition_key_constraint_names_for_catalog(
    relation: &crate::backend::parser::BoundRelation,
    row: &crate::include::catalog::PgPartitionedTableRow,
) -> Result<Vec<String>, ExecError> {
    let exprs = deserialize_partition_key_exprs(row)?;
    Ok(row
        .partattrs
        .iter()
        .enumerate()
        .map(|(index, attnum)| {
            if *attnum > 0 {
                let column_index = (*attnum as usize).saturating_sub(1);
                relation
                    .desc
                    .columns
                    .get(column_index)
                    .map(|column| quote_identifier_if_needed(&column.name))
                    .unwrap_or_else(|| attnum.to_string())
            } else {
                exprs
                    .get(index)
                    .and_then(|expr| expr.as_deref())
                    .map(format_partition_expr_sql_for_constraint)
                    .unwrap_or_else(|| "?column?".into())
            }
        })
        .collect())
}

fn partition_value_constraint_literal(value: &SerializedPartitionValue) -> String {
    match value {
        SerializedPartitionValue::Array(array) => format!(
            "{}::{}",
            quote_sql_literal(&partition_value_text(value)),
            array.type_name
        ),
        SerializedPartitionValue::Text(_)
        | SerializedPartitionValue::Date(_)
        | SerializedPartitionValue::Time(_)
        | SerializedPartitionValue::TimeTz { .. }
        | SerializedPartitionValue::Timestamp(_)
        | SerializedPartitionValue::TimestampTz(_)
        | SerializedPartitionValue::Range(_)
        | SerializedPartitionValue::Multirange(_) => format!(
            "{}::{}",
            quote_sql_literal(&partition_value_text(value)),
            partition_value_type_name(value)
        ),
        SerializedPartitionValue::Int16(value) if *value < 0 => {
            format!("{}::smallint", quote_sql_literal(&value.to_string()))
        }
        SerializedPartitionValue::Int32(value) if *value < 0 => {
            format!("{}::integer", quote_sql_literal(&value.to_string()))
        }
        SerializedPartitionValue::Int64(value) if *value < 0 => {
            format!("{}::bigint", quote_sql_literal(&value.to_string()))
        }
        SerializedPartitionValue::Null => "NULL".into(),
        _ => partition_value_bound_literal(value),
    }
}

fn partition_value_type_name(value: &SerializedPartitionValue) -> &'static str {
    match value {
        SerializedPartitionValue::Null => "unknown",
        SerializedPartitionValue::Int16(_) => "smallint",
        SerializedPartitionValue::Int32(_) => "integer",
        SerializedPartitionValue::Int64(_) => "bigint",
        SerializedPartitionValue::Money(_) => "money",
        SerializedPartitionValue::Float64(_) => "double precision",
        SerializedPartitionValue::Numeric(_) => "numeric",
        SerializedPartitionValue::Text(_) => "text",
        SerializedPartitionValue::Bytea(_) => "bytea",
        SerializedPartitionValue::Json(_) => "json",
        SerializedPartitionValue::Jsonb(_) => "jsonb",
        SerializedPartitionValue::JsonPath(_) => "jsonpath",
        SerializedPartitionValue::Xml(_) => "xml",
        SerializedPartitionValue::InternalChar(_) => "\"char\"",
        SerializedPartitionValue::Bool(_) => "boolean",
        SerializedPartitionValue::Date(_) => "date",
        SerializedPartitionValue::Time(_) => "time without time zone",
        SerializedPartitionValue::TimeTz { .. } => "time with time zone",
        SerializedPartitionValue::Timestamp(_) => "timestamp without time zone",
        SerializedPartitionValue::TimestampTz(_) => "timestamp with time zone",
        SerializedPartitionValue::Array(_) => "text[]",
        SerializedPartitionValue::Range(_) => "text",
        SerializedPartitionValue::Multirange(_) => "text",
    }
}

fn partition_value_text(value: &SerializedPartitionValue) -> String {
    let value = partition_value_to_value(value);
    match &value {
        Value::Null => String::new(),
        Value::Text(_) | Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => render_datetime_value_text_with_config(
            &value,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .unwrap_or_default(),
        Value::Bool(value) => value.to_string(),
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Money(value) => crate::backend::executor::money_format_text(*value),
        Value::Float64(value) => value.to_string(),
        Value::Numeric(value) => value.render(),
        Value::Json(value) => value.to_string(),
        Value::JsonPath(value) | Value::Xml(value) => value.to_string(),
        Value::Array(values) => crate::backend::executor::value_io::format_array_text(values),
        Value::PgArray(array) => crate::backend::executor::value_io::format_array_value_text(array),
        Value::Bytea(bytes) => {
            let mut out = String::from("\\x");
            for byte in bytes {
                out.push_str(&format!("{byte:02x}"));
            }
            out
        }
        _ => format!("{value:?}"),
    }
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn format_partition_keydef_for_catalog(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    row: &crate::include::catalog::PgPartitionedTableRow,
) -> Result<String, ExecError> {
    let strategy = match row.partstrat {
        'l' => "LIST",
        'r' => "RANGE",
        'h' => "HASH",
        other => {
            return Err(ExecError::DetailedError {
                message: format!("unrecognized partition strategy \"{other}\""),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
    };
    let exprs = deserialize_partition_key_exprs(row)?;
    let keys = row
        .partattrs
        .iter()
        .enumerate()
        .map(|(index, attnum)| {
            format_partition_keydef_item(catalog, relation, row, &exprs, index, *attnum)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("{strategy} ({})", keys.join(", ")))
}

fn deserialize_partition_key_exprs(
    row: &crate::include::catalog::PgPartitionedTableRow,
) -> Result<Vec<Option<String>>, ExecError> {
    match row.partexprs.as_deref() {
        Some(text) => serde_json::from_str(text).map_err(|_| ExecError::DetailedError {
            message: "invalid partition expression metadata".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
        None => Ok(vec![None; row.partattrs.len()]),
    }
}

fn format_partition_keydef_item(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    row: &crate::include::catalog::PgPartitionedTableRow,
    exprs: &[Option<String>],
    index: usize,
    attnum: i16,
) -> Result<String, ExecError> {
    let mut key = if attnum > 0 {
        let column_index = (attnum as usize).saturating_sub(1);
        relation
            .desc
            .columns
            .get(column_index)
            .map(|column| quote_identifier_if_needed(&column.name))
            .unwrap_or_else(|| attnum.to_string())
    } else {
        exprs
            .get(index)
            .and_then(|expr| expr.clone())
            .map(|expr| format_partition_expr_sql_for_keydef(&expr))
            .unwrap_or_else(|| "?column?".into())
    };
    if let Some(opclass) = partition_key_opclass_display_name(catalog, relation, row, index, attnum)
    {
        key.push(' ');
        key.push_str(&opclass);
    }
    if let Some(collation) = partition_key_collation_display_name(catalog, row, index) {
        key.push_str(" COLLATE ");
        key.push_str(&collation);
    }
    Ok(key)
}

fn format_partition_expr_sql_for_keydef(expr_sql: &str) -> String {
    let constraint_expr = format_partition_expr_sql_for_constraint(expr_sql);
    if constraint_expr.starts_with('(') || contains_display_operator(&constraint_expr) {
        format!("({constraint_expr})")
    } else {
        constraint_expr
    }
}

fn format_partition_expr_sql_for_constraint(expr_sql: &str) -> String {
    let stripped = strip_outer_expr_parens(expr_sql.trim());
    let normalized = normalize_partition_expr_operator_spacing(stripped);
    if contains_display_operator(&normalized) {
        format!("({normalized})")
    } else {
        normalized
    }
}

fn contains_display_operator(expr: &str) -> bool {
    expr.contains(" + ")
        || expr.contains(" - ")
        || expr.contains(" * ")
        || expr.contains(" / ")
        || expr.contains(" % ")
}

fn strip_outer_expr_parens(expr: &str) -> &str {
    let mut current = expr.trim();
    loop {
        if !current.starts_with('(') || !current.ends_with(')') {
            return current;
        }
        let mut depth = 0_i32;
        let mut wraps = true;
        for (index, ch) in current.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 && index != current.len() - 1 {
                        wraps = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !wraps {
            return current;
        }
        current = current[1..current.len() - 1].trim();
    }
}

fn normalize_partition_expr_operator_spacing(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.chars().peekable();
    while let Some(ch) = chars.next() {
        if matches!(ch, '+' | '*' | '/' | '%') {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push(' ');
            out.push(ch);
            out.push(' ');
            while chars.peek().is_some_and(|next| next.is_ascii_whitespace()) {
                chars.next();
            }
        } else if ch == '-' && !out.trim_end().is_empty() {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push(' ');
            out.push(ch);
            out.push(' ');
            while chars.peek().is_some_and(|next| next.is_ascii_whitespace()) {
                chars.next();
            }
        } else {
            out.push(ch);
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn partition_key_opclass_display_name(
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    row: &crate::include::catalog::PgPartitionedTableRow,
    index: usize,
    attnum: i16,
) -> Option<String> {
    let opclass_oid = *row.partclass.get(index)?;
    if opclass_oid == 0 {
        return None;
    }
    let sql_type = if attnum > 0 {
        let column_index = (attnum as usize).checked_sub(1)?;
        relation.desc.columns.get(column_index)?.sql_type
    } else {
        partition_spec_for_relation_best_effort(relation)
            .and_then(|spec| spec.key_types.get(index).copied())?
    };
    let default_opclass = default_partition_opclass_oid(row.partstrat, sql_type);
    if default_opclass == Some(opclass_oid) {
        return None;
    }
    catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)
        .map(|row| quote_identifier_if_needed(&row.opcname))
}

fn default_partition_opclass_oid(partstrat: char, sql_type: SqlType) -> Option<u32> {
    let type_oid = crate::backend::utils::cache::catcache::sql_type_oid(sql_type);
    match partstrat {
        'h' => default_hash_opclass_oid(type_oid),
        'l' | 'r' if sql_type.is_array => Some(ARRAY_BTREE_OPCLASS_OID),
        'l' | 'r' => default_btree_opclass_oid(type_oid),
        _ => None,
    }
}

fn partition_spec_for_relation_best_effort(
    relation: &crate::backend::parser::BoundRelation,
) -> Option<LoweredPartitionSpec> {
    crate::backend::parser::relation_partition_spec(relation).ok()
}

fn partition_key_collation_display_name(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgPartitionedTableRow,
    index: usize,
) -> Option<String> {
    let collation_oid = *row.partcollation.get(index)?;
    if matches!(collation_oid, 0 | DEFAULT_COLLATION_OID) {
        return None;
    }
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == collation_oid)
        .map(|row| quote_identifier_if_needed(&row.collname))
}

fn canonicalize_catalog_expr_sql(
    expr_sql: &str,
    relation_oid: u32,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    if !contains_sql_json_query_function(expr_sql) {
        return None;
    }
    let relation = catalog.lookup_relation_by_oid(relation_oid);
    let relation_name = relation
        .as_ref()
        .and_then(|_| catalog.class_row_by_oid(relation_oid))
        .map(|row| row.relname);
    let empty_desc = RelationDesc {
        columns: Vec::new(),
    };
    let desc = relation
        .as_ref()
        .map(|relation| &relation.desc)
        .unwrap_or(&empty_desc);
    let bound = bind_relation_expr(expr_sql, relation_name.as_deref(), desc, catalog).ok()?;
    Some(render_relation_expr_sql(
        &bound,
        relation_name.as_deref(),
        desc,
        catalog,
    ))
}

fn contains_sql_json_query_function(expr_sql: &str) -> bool {
    let upper = expr_sql.to_ascii_uppercase();
    upper.contains("JSON_QUERY(") || upper.contains("JSON_VALUE(") || upper.contains("JSON_EXISTS(")
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
    fn append_constraint_state(def: &mut String, row: &crate::include::catalog::PgConstraintRow) {
        if row.connoinherit {
            def.push_str(" NO INHERIT");
        }
        if !row.conenforced {
            def.push_str(" NOT ENFORCED");
        }
        if !row.convalidated {
            def.push_str(" NOT VALID");
        }
    }

    match row.contype {
        CONSTRAINT_NOTNULL => {
            let mut def = "NOT NULL".to_string();
            append_constraint_state(&mut def, row);
            Some(def)
        }
        CONSTRAINT_CHECK => row.conbin.as_deref().map(|expr_sql| {
            let expr_sql = canonicalize_catalog_expr_sql(expr_sql, row.conrelid, catalog)
                .unwrap_or_else(|| expr_sql.to_string());
            let mut def = format!("CHECK {}", normalize_check_expr_sql(&expr_sql));
            append_constraint_state(&mut def, row);
            def
        }),
        CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE => {
            format_index_backed_constraintdef_for_catalog(catalog, row)
        }
        CONSTRAINT_EXCLUSION => format_exclusion_constraintdef_for_catalog(catalog, row),
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
    let all_columns = index_display_names_for_heap(&relation.desc, &index.index_meta)?;
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0)).unwrap_or_default();
    let mut columns = all_columns
        .iter()
        .take(key_count)
        .cloned()
        .collect::<Vec<_>>();
    let include_columns = all_columns
        .iter()
        .skip(key_count)
        .cloned()
        .collect::<Vec<_>>();
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
    if !include_columns.is_empty() {
        def.push_str(" INCLUDE (");
        def.push_str(&include_columns.join(", "));
        def.push(')');
    }
    append_constraint_deferrability(&mut def, row);
    Some(def)
}

fn format_exclusion_constraintdef_for_catalog(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let relation = catalog.lookup_relation_by_oid(row.conrelid)?;
    let index = catalog
        .index_relations_for_heap(row.conrelid)
        .into_iter()
        .find(|index| index.relation_oid == row.conindid)?;
    let all_columns = index_display_names_for_heap(&relation.desc, &index.index_meta)?;
    let operators = row
        .conexclop
        .as_ref()?
        .iter()
        .map(|operator_oid| {
            catalog
                .operator_by_oid(*operator_oid)
                .map(|row| row.oprname)
        })
        .collect::<Option<Vec<_>>>()?;
    let key_count = operators.len();
    let key_columns = all_columns
        .iter()
        .take(key_count)
        .zip(operators.iter())
        .map(|(column, operator)| format!("{column} WITH {operator}"))
        .collect::<Vec<_>>();
    let include_columns = all_columns
        .iter()
        .skip(key_count)
        .cloned()
        .collect::<Vec<_>>();
    let amname = bootstrap_pg_am_rows()
        .into_iter()
        .find(|row| row.oid == index.index_meta.am_oid)
        .map(|row| row.amname)
        .unwrap_or_else(|| "gist".into());
    let mut def = format!("EXCLUDE USING {amname} ({})", key_columns.join(", "));
    if !include_columns.is_empty() {
        def.push_str(" INCLUDE (");
        def.push_str(&include_columns.join(", "));
        def.push(')');
    }
    append_constraint_deferrability(&mut def, row);
    Some(def)
}

fn format_foreign_key_constraintdef_for_catalog(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgConstraintRow,
) -> Option<String> {
    let relation = catalog.lookup_relation_by_oid(row.conrelid)?;
    let referenced_relation = catalog.lookup_relation_by_oid(row.confrelid)?;
    let mut local_columns = index_column_names_for_heap(&relation.desc, row.conkey.as_ref()?)?;
    let mut referenced_columns =
        index_column_names_for_heap(&referenced_relation.desc, row.confkey.as_ref()?)?;
    if row.conperiod {
        if let Some(column) = local_columns.last_mut() {
            *column = format!("PERIOD {column}");
        }
        if let Some(column) = referenced_columns.last_mut() {
            *column = format!("PERIOD {column}");
        }
    }
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
    append_foreign_key_match_type(&mut def, row.confmatchtype);
    append_foreign_key_action(&mut def, "ON UPDATE", row.confupdtype);
    let appended_delete = append_foreign_key_action(&mut def, "ON DELETE", row.confdeltype);
    if appended_delete
        && let Some(set_columns) = row
            .confdelsetcols
            .as_ref()
            .and_then(|attnums| index_column_names_for_heap(&relation.desc, attnums))
        && !set_columns.is_empty()
    {
        def.push_str(" (");
        def.push_str(&set_columns.join(", "));
        def.push(')');
    }
    append_constraint_deferrability(&mut def, row);
    Some(def)
}

fn append_foreign_key_match_type(def: &mut String, match_type: char) {
    match match_type {
        'f' => def.push_str(" MATCH FULL"),
        'p' => def.push_str(" MATCH PARTIAL"),
        _ => {}
    }
}

fn append_foreign_key_action(def: &mut String, clause: &str, action: char) -> bool {
    let Some(keyword) = foreign_key_action_keyword(action) else {
        return false;
    };
    def.push(' ');
    def.push_str(clause);
    def.push(' ');
    def.push_str(keyword);
    true
}

fn foreign_key_action_keyword(action: char) -> Option<&'static str> {
    match action {
        'r' => Some("RESTRICT"),
        'c' => Some("CASCADE"),
        'n' => Some("SET NULL"),
        'd' => Some("SET DEFAULT"),
        _ => None,
    }
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
    let (index_oid, column_no, qualify_table_name) = match values {
        [Value::Null] | [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => {
            return Ok(Value::Null);
        }
        [index_oid] => (oid_arg_to_u32(index_oid, "pg_get_indexdef")?, 0, true),
        [index_oid, column_no, _pretty] => (
            oid_arg_to_u32(index_oid, "pg_get_indexdef")?,
            int32_arg(column_no, "pg_get_indexdef")?,
            false,
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
        format_indexdef_for_catalog(catalog, &relation, &index, qualify_table_name).into(),
    ))
}

fn index_relation_for_oid(
    catalog: &dyn CatalogLookup,
    index_oid: u32,
) -> Option<(
    crate::backend::parser::BoundRelation,
    crate::backend::parser::BoundIndexRelation,
)> {
    if let Some(index_row) = catalog.index_row_by_oid(index_oid) {
        let relation = catalog.lookup_relation_by_oid(index_row.indrelid)?;
        let index = catalog
            .index_relations_for_heap(index_row.indrelid)
            .into_iter()
            .find(|index| index.relation_oid == index_oid)?;
        return Some((relation, index));
    }
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
    qualify_table_name: bool,
) -> String {
    let table_name = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|class| {
            if qualify_table_name {
                catalog
                    .namespace_row_by_oid(class.relnamespace)
                    .map(|namespace| format!("{}.{}", namespace.nspname, class.relname))
                    .unwrap_or(class.relname)
            } else {
                class.relname
            }
        })
        .unwrap_or_else(|| relation.relation_oid.to_string());
    let amname = bootstrap_pg_am_rows()
        .into_iter()
        .find(|row| row.oid == index.index_meta.am_oid)
        .map(|row| row.amname)
        .unwrap_or_else(|| "btree".into());
    let all_columns = index_definition_columns(relation, index);
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0)).unwrap_or_default();
    let key_columns = all_columns
        .iter()
        .take(key_count)
        .cloned()
        .collect::<Vec<_>>();
    let include_columns = all_columns
        .iter()
        .skip(key_count)
        .cloned()
        .collect::<Vec<_>>();
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
        key_columns.join(", ")
    );
    if !include_columns.is_empty() {
        definition.push_str(" INCLUDE (");
        definition.push_str(&include_columns.join(", "));
        definition.push(')');
    }
    if index.index_meta.indnullsnotdistinct {
        definition.push_str(" NULLS NOT DISTINCT");
    }
    if let Some(predicate) = index
        .index_meta
        .indpred
        .as_deref()
        .filter(|pred| !pred.is_empty())
    {
        let predicate = normalize_index_predicate_sql(predicate, Some(&relation.desc));
        definition.push_str(" WHERE (");
        definition.push_str(&predicate);
        definition.push(')');
    }
    definition
}

fn index_definition_columns(
    relation: &crate::backend::parser::BoundRelation,
    index: &crate::backend::parser::BoundIndexRelation,
) -> Vec<String> {
    let expression_sqls = index
        .index_meta
        .indexprs
        .as_deref()
        .and_then(|sql| serde_json::from_str::<Vec<String>>(sql).ok())
        .unwrap_or_default();
    let mut expression_index = 0usize;
    index
        .index_meta
        .indkey
        .iter()
        .enumerate()
        .map(|(index_column, attnum)| {
            if *attnum > 0 {
                return relation
                    .desc
                    .columns
                    .get((*attnum as usize).saturating_sub(1))
                    .map(|column| column.name.clone())
                    .or_else(|| {
                        index
                            .desc
                            .columns
                            .get(index_column)
                            .map(|column| column.name.clone())
                    })
                    .unwrap_or_else(|| format!("column{}", index_column + 1));
            }
            let rendered = expression_sqls
                .get(expression_index)
                .map(|expr| parenthesized_index_expression(&normalize_index_expression_sql(expr)))
                .or_else(|| {
                    index
                        .desc
                        .columns
                        .get(index_column)
                        .map(|column| column.name.clone())
                })
                .unwrap_or_else(|| format!("expr{}", expression_index + 1));
            expression_index += 1;
            rendered
        })
        .collect()
}

fn parenthesized_index_expression(expr_sql: &str) -> String {
    let trimmed = expr_sql.trim();
    if let Some(function_call) = normalized_function_call_expression(trimmed) {
        return function_call;
    }
    if (trimmed.starts_with('(') && trimmed.ends_with(')')) || looks_like_function_call(trimmed) {
        trimmed.to_string()
    } else {
        format!("({trimmed})")
    }
}

fn normalized_function_call_expression(expr_sql: &str) -> Option<String> {
    let trimmed = strip_outer_parens_once(expr_sql.trim());
    if !looks_like_function_call(trimmed) {
        return None;
    }
    let open = trimmed.find('(')?;
    let name = trimmed[..open].trim();
    let args = trimmed[open + 1..trimmed.len().saturating_sub(1)]
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("{name}({args})"))
}

fn strip_outer_parens_once(input: &str) -> &str {
    let trimmed = input.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return trimmed;
    }
    let mut depth = 0i32;
    for (idx, ch) in trimmed.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && idx + ch.len_utf8() < trimmed.len() {
                    return trimmed;
                }
            }
            _ => {}
        }
    }
    trimmed[1..trimmed.len().saturating_sub(1)].trim()
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

fn index_display_names_for_heap(
    desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Option<Vec<String>> {
    let expression_sqls = index_meta
        .indexprs
        .as_deref()
        .and_then(|sql| serde_json::from_str::<Vec<String>>(sql).ok())
        .unwrap_or_default();
    let mut expression_index = 0usize;
    index_meta
        .indkey
        .iter()
        .map(|attnum| {
            if *attnum > 0 {
                return desc
                    .columns
                    .get((*attnum as usize).saturating_sub(1))
                    .map(|column| column.name.clone());
            }
            let rendered = expression_sqls
                .get(expression_index)
                .map(|expr| parenthesized_index_expression(&normalize_index_expression_sql(expr)))
                .unwrap_or_else(|| format!("expr{}", expression_index + 1));
            expression_index += 1;
            Some(rendered)
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

fn eval_pg_get_ruledef(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let catalog = executor_catalog(ctx)?;
    let (rule_oid, pretty) = match values {
        [Value::Null] | [Value::Null, _] | [_, Value::Null] => return Ok(Value::Null),
        [value] => (oid_arg_to_u32(value, "pg_get_ruledef")?, false),
        [value, pretty] => (
            oid_arg_to_u32(value, "pg_get_ruledef")?,
            matches!(pretty, Value::Bool(true)),
        ),
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_get_ruledef(rule [, pretty])",
                actual: format!("PgGetRuleDef({} args)", values.len()),
            }));
        }
    };
    if rule_oid == 0 {
        return Ok(Value::Null);
    }
    let Some(rule) = catalog.rewrite_row_by_oid(rule_oid) else {
        return Ok(Value::Null);
    };
    let relation_name = catalog
        .class_row_by_oid(rule.ev_class)
        .map(|row| row.relname)
        .unwrap_or_else(|| rule.ev_class.to_string());
    let mut definition = format_stored_rule_definition_with_catalog(&rule, &relation_name, catalog);
    if pretty {
        definition = definition
            .replace(" AS ON ", " AS\n    ON ")
            .replace(" DO ALSO ", " DO  ");
    }
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
        .class_rows()
        .into_iter()
        .flat_map(|class| catalog.trigger_rows_for_relation(class.oid))
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

fn eval_pg_column_toast_chunk_id_raw(raw: &[u8]) -> Result<Value, ExecError> {
    if !crate::include::varatt::is_ondisk_toast_pointer(raw) {
        return Ok(Value::Null);
    }
    let pointer = crate::include::varatt::decode_ondisk_toast_pointer(raw).ok_or_else(|| {
        ExecError::DetailedError {
            message: "invalid on-disk toast pointer".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }
    })?;
    Ok(Value::Int64(i64::from(pointer.va_valueid)))
}

fn eval_pg_column_toast_chunk_id_values(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [_] => Ok(Value::Null),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_column_toast_chunk_id(any)",
            actual: format!("PgColumnToastChunkId({} args)", values.len()),
        })),
    }
}

fn eval_num_nulls(values: &[Value], func_variadic: bool, count_nulls: bool) -> Value {
    if func_variadic {
        let Some(value) = values.first() else {
            return Value::Int32(0);
        };
        if matches!(value, Value::Null) {
            return Value::Null;
        }
        let Some(array) = crate::include::nodes::datum::array_value_from_value(value) else {
            return Value::Int32(if matches!(value, Value::Null) == count_nulls {
                1
            } else {
                0
            });
        };
        let count = array
            .elements
            .iter()
            .filter(|value| matches!(value, Value::Null) == count_nulls)
            .count();
        return Value::Int32(count as i32);
    }
    Value::Int32(
        values
            .iter()
            .filter(|value| matches!(value, Value::Null) == count_nulls)
            .count() as i32,
    )
}

fn data_dir_path(ctx: &ExecutorContext) -> Result<std::path::PathBuf, ExecError> {
    ctx.data_dir
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .ok_or_else(|| ExecError::DetailedError {
            message: "data directory is not available".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })
}

fn resolve_data_file(
    ctx: &ExecutorContext,
    filename: &str,
) -> Result<std::path::PathBuf, ExecError> {
    let path = std::path::Path::new(filename);
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(data_dir_path(ctx)?.join(path))
}

fn file_timestamp_value(time: std::io::Result<std::time::SystemTime>) -> Value {
    const UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS: i64 = 10_957;
    match time
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
    {
        Some(duration) => {
            let usecs =
                duration.as_secs() as i64 * USECS_PER_SEC + i64::from(duration.subsec_micros());
            Value::TimestampTz(TimestampTzADT(
                usecs
                    - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS
                        * crate::include::nodes::datetime::USECS_PER_DAY,
            ))
        }
        None => Value::Null,
    }
}

fn read_file_args(values: &[Value]) -> Result<(String, Option<i64>, Option<i64>, bool), ExecError> {
    let filename = values
        .first()
        .and_then(Value::as_text)
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "pg_read_file",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Text("".into()),
        })?
        .to_string();
    let (offset, length, missing_ok) = match values {
        [_] => (None, None, false),
        [_, Value::Bool(missing_ok)] => (None, None, *missing_ok),
        [_, offset, length] => (
            Some(int64_arg(offset, "pg_read_file offset")?),
            Some(int64_arg(length, "pg_read_file length")?),
            false,
        ),
        [_, offset, length, Value::Bool(missing_ok)] => (
            Some(int64_arg(offset, "pg_read_file offset")?),
            Some(int64_arg(length, "pg_read_file length")?),
            *missing_ok,
        ),
        _ => return Err(malformed_expr_error("pg_read_file")),
    };
    if length.is_some_and(|length| length < 0) {
        return Err(ExecError::DetailedError {
            message: "requested length cannot be negative".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok((filename, offset, length, missing_ok))
}

fn int64_arg(value: &Value, op: &'static str) -> Result<i64, ExecError> {
    match value {
        Value::Int16(v) => Ok(i64::from(*v)),
        Value::Int32(v) => Ok(i64::from(*v)),
        Value::Int64(v) => Ok(*v),
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(0),
        }),
    }
}

fn pg_io_error_message(err: &std::io::Error) -> String {
    match err.kind() {
        std::io::ErrorKind::NotFound => "No such file or directory".into(),
        _ => err.to_string(),
    }
}

fn synthetic_postmaster_pid_bytes() -> Vec<u8> {
    b"1\n/var/run/pgrust\n0\n0\n0\n0\n".to_vec()
}

fn eval_pg_read_file(
    values: &[Value],
    ctx: &ExecutorContext,
    binary: bool,
) -> Result<Value, ExecError> {
    let (filename, offset, length, missing_ok) = read_file_args(values)?;
    let path = resolve_data_file(ctx, &filename)?;
    let mut bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if missing_ok && err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Value::Null);
        }
        // :HACK: pgrust test/server clusters do not maintain PostgreSQL's postmaster.pid yet,
        // but SQL-visible admin functions expect the data-directory sentinel to be readable.
        Err(err) if filename == "postmaster.pid" && err.kind() == std::io::ErrorKind::NotFound => {
            synthetic_postmaster_pid_bytes()
        }
        Err(err) => {
            return Err(ExecError::DetailedError {
                message: format!(
                    "could not open file \"{filename}\" for reading: {}",
                    pg_io_error_message(&err)
                ),
                detail: None,
                hint: None,
                sqlstate: "58P01",
            });
        }
    };
    if let Some(offset) = offset {
        let start = offset.max(0) as usize;
        if start >= bytes.len() {
            bytes.clear();
        } else {
            bytes = bytes[start..].to_vec();
        }
    }
    if let Some(length) = length {
        bytes.truncate(length as usize);
    }
    if binary {
        Ok(Value::Bytea(bytes))
    } else {
        Ok(Value::Text(
            String::from_utf8_lossy(&bytes).into_owned().into(),
        ))
    }
}

fn eval_pg_stat_file(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let filename =
        values
            .first()
            .and_then(Value::as_text)
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_stat_file",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Text("".into()),
            })?;
    let missing_ok = values
        .get(1)
        .map(|value| matches!(value, Value::Bool(true)))
        .unwrap_or(false);
    let path = resolve_data_file(ctx, filename)?;
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if missing_ok && err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Value::Null);
        }
        // :HACK: mirror the synthetic postmaster.pid read path until cluster startup writes it.
        Err(err) if filename == "postmaster.pid" && err.kind() == std::io::ErrorKind::NotFound => {
            let now = Value::TimestampTz(TimestampTzADT(ctx.statement_timestamp_usecs));
            return Ok(Value::Record(
                crate::include::nodes::datum::RecordValue::anonymous(vec![
                    (
                        "size".into(),
                        Value::Int64(synthetic_postmaster_pid_bytes().len() as i64),
                    ),
                    ("access".into(), now.clone()),
                    ("modification".into(), now.clone()),
                    ("change".into(), now.clone()),
                    ("creation".into(), now),
                    ("isdir".into(), Value::Bool(false)),
                ]),
            ));
        }
        Err(err) => {
            return Err(ExecError::DetailedError {
                message: format!(
                    "could not stat file \"{filename}\": {}",
                    pg_io_error_message(&err)
                ),
                detail: None,
                hint: None,
                sqlstate: "58P01",
            });
        }
    };
    Ok(Value::Record(
        crate::include::nodes::datum::RecordValue::anonymous(vec![
            ("size".into(), Value::Int64(metadata.len() as i64)),
            ("access".into(), file_timestamp_value(metadata.accessed())),
            (
                "modification".into(),
                file_timestamp_value(metadata.modified()),
            ),
            ("change".into(), file_timestamp_value(metadata.modified())),
            ("creation".into(), file_timestamp_value(metadata.created())),
            ("isdir".into(), Value::Bool(metadata.is_dir())),
        ]),
    ))
}

fn wal_segment_file_name(segno: u64) -> String {
    let segs_per_logid =
        0x1_0000_0000u64 / crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES as u64;
    let log = segno / segs_per_logid;
    let seg = segno % segs_per_logid;
    format!("{:08X}{log:08X}{seg:08X}", 1u32)
}

fn wal_segment_no(lsn: u64) -> u64 {
    lsn / crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES as u64
}

fn wal_segment_offset(lsn: u64) -> u64 {
    lsn % crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES as u64
}

fn parse_wal_file_name(name: &str) -> Option<(u32, u64)> {
    if name.len() != 24 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let timeline = u32::from_str_radix(&name[0..8], 16).ok()?;
    let log = u32::from_str_radix(&name[8..16], 16).ok()? as u64;
    let seg = u32::from_str_radix(&name[16..24], 16).ok()? as u64;
    let segs_per_logid =
        0x1_0000_0000u64 / crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES as u64;
    Some((timeline, log * segs_per_logid + seg))
}

fn eval_pg_walfile_name(values: &[Value]) -> Result<Value, ExecError> {
    let [Value::PgLsn(lsn)] = values else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(
        wal_segment_file_name(wal_segment_no(*lsn)).into(),
    ))
}

fn eval_pg_walfile_name_offset(values: &[Value]) -> Result<Value, ExecError> {
    let [Value::PgLsn(lsn)] = values else {
        return Ok(Value::Record(
            crate::include::nodes::datum::RecordValue::anonymous(vec![
                ("file_name".into(), Value::Null),
                ("file_offset".into(), Value::Null),
            ]),
        ));
    };
    Ok(Value::Record(
        crate::include::nodes::datum::RecordValue::anonymous(vec![
            (
                "file_name".into(),
                Value::Text(wal_segment_file_name(wal_segment_no(*lsn)).into()),
            ),
            (
                "file_offset".into(),
                Value::Int32(wal_segment_offset(*lsn) as i32),
            ),
        ]),
    ))
}

fn eval_pg_split_walfile_name(values: &[Value]) -> Result<Value, ExecError> {
    let Some(name) = values.first().and_then(Value::as_text) else {
        return Ok(Value::Record(
            crate::include::nodes::datum::RecordValue::anonymous(vec![
                ("segment_number".into(), Value::Null),
                ("timeline_id".into(), Value::Null),
            ]),
        ));
    };
    let Some((timeline, segno)) = parse_wal_file_name(name) else {
        return Err(ExecError::DetailedError {
            message: format!("invalid WAL file name \"{name}\""),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    };
    Ok(Value::Record(
        crate::include::nodes::datum::RecordValue::anonymous(vec![
            (
                "segment_number".into(),
                Value::Numeric(NumericValue::from_i64(segno as i64)),
            ),
            ("timeline_id".into(), Value::Int64(i64::from(timeline))),
        ]),
    ))
}

fn eval_pg_control_record(func: BuiltinScalarFunction, ctx: &ExecutorContext) -> Value {
    let now = ctx.statement_timestamp_usecs;
    let wal_seg_size = crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES as i32;
    let fields = match func {
        BuiltinScalarFunction::PgControlSystem => vec![
            ("pg_control_version".into(), Value::Int32(1300)),
            ("catalog_version_no".into(), Value::Int32(0)),
            ("system_identifier".into(), Value::Int64(0)),
            (
                "pg_control_last_modified".into(),
                Value::TimestampTz(TimestampTzADT(now)),
            ),
        ],
        BuiltinScalarFunction::PgControlCheckpoint => vec![
            ("checkpoint_lsn".into(), Value::PgLsn(0)),
            ("redo_lsn".into(), Value::PgLsn(0)),
            (
                "redo_wal_file".into(),
                Value::Text(wal_segment_file_name(0).into()),
            ),
            ("timeline_id".into(), Value::Int32(1)),
            ("prev_timeline_id".into(), Value::Int32(1)),
            ("full_page_writes".into(), Value::Bool(true)),
            ("next_xid".into(), Value::Text("0:1".into())),
            ("next_oid".into(), Value::Int64(1)),
            ("next_multixact_id".into(), Value::Int32(1)),
            ("next_multi_offset".into(), Value::Int32(0)),
            ("oldest_xid".into(), Value::Int32(1)),
            ("oldest_xid_dbid".into(), Value::Int64(1)),
            ("oldest_active_xid".into(), Value::Int32(1)),
            ("oldest_multi_xid".into(), Value::Int32(1)),
            ("oldest_multi_dbid".into(), Value::Int64(1)),
            ("oldest_commit_ts_xid".into(), Value::Int32(0)),
            ("newest_commit_ts_xid".into(), Value::Int32(0)),
            (
                "checkpoint_time".into(),
                Value::TimestampTz(TimestampTzADT(now)),
            ),
        ],
        BuiltinScalarFunction::PgControlRecovery => vec![
            ("min_recovery_end_lsn".into(), Value::PgLsn(0)),
            ("min_recovery_end_timeline".into(), Value::Int32(0)),
            ("backup_start_lsn".into(), Value::PgLsn(0)),
            ("backup_end_lsn".into(), Value::PgLsn(0)),
            ("end_of_backup_record_required".into(), Value::Bool(false)),
        ],
        BuiltinScalarFunction::PgControlInit => vec![
            ("max_data_alignment".into(), Value::Int32(8)),
            ("database_block_size".into(), Value::Int32(8192)),
            ("blocks_per_segment".into(), Value::Int32(131072)),
            ("wal_block_size".into(), Value::Int32(8192)),
            ("bytes_per_wal_segment".into(), Value::Int32(wal_seg_size)),
            ("max_identifier_length".into(), Value::Int32(64)),
            ("max_index_columns".into(), Value::Int32(32)),
            ("max_toast_chunk_size".into(), Value::Int32(1996)),
            ("large_object_chunk_size".into(), Value::Int32(2048)),
            ("float8_pass_by_value".into(), Value::Bool(true)),
            ("data_page_checksum_version".into(), Value::Int32(0)),
            ("default_char_signedness".into(), Value::Bool(true)),
        ],
        _ => unreachable!("non-control builtin"),
    };
    // :HACK: This surfaces a stable pgrust control snapshot through the SQL API
    // until pg_control fields are threaded directly from ControlFileStore.
    Value::Record(crate::include::nodes::datum::RecordValue::anonymous(fields))
}

fn canonicalize_path_text(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            if parts.last().is_some_and(|last| *last != "..") {
                parts.pop();
            } else if !absolute {
                parts.push(part);
            }
        } else {
            parts.push(part);
        }
    }
    if absolute {
        if parts.is_empty() {
            "/".into()
        } else {
            format!("/{}", parts.join("/"))
        }
    } else if parts.is_empty() {
        ".".into()
    } else {
        parts.join("/")
    }
}

fn eval_test_canonicalize_path(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(malformed_expr_error("test_canonicalize_path"));
    };
    Ok(value
        .as_text()
        .map(canonicalize_path_text)
        .map(Into::into)
        .map(Value::Text)
        .unwrap_or(Value::Null))
}

fn eval_gist_translate_cmptype_common(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(malformed_expr_error("gist_translate_cmptype_common"));
    };
    let strategy = int32_arg(value, "gist_translate_cmptype_common")?;
    Ok(Value::Int16(match strategy {
        3 => 18,
        7 => 3,
        other => other as i16,
    }))
}

fn eval_pg_log_backend_memory_contexts(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(malformed_expr_error("pg_log_backend_memory_contexts"));
    };
    let _pid = int32_arg(value, "pg_log_backend_memory_contexts")?;
    Ok(Value::Bool(true))
}

fn eval_pg_current_logfile(values: &[Value]) -> Result<Value, ExecError> {
    if values.len() > 1 {
        return Err(malformed_expr_error("pg_current_logfile"));
    }
    Ok(Value::Null)
}

fn acl_item_parts(item: &str) -> Option<(&str, &str, &str)> {
    let (grantee, rest) = item.split_once('=')?;
    let (privileges, grantor) = rest.split_once('/')?;
    Some((grantee, privileges, grantor))
}

#[derive(Clone, Copy)]
struct PrivilegeSpec {
    acl_char: char,
    grant_option: bool,
}

#[derive(Clone, Copy)]
enum RolePrivilegeSpec {
    Usage,
    Member,
    Set,
    Admin,
}

#[derive(Clone, Copy)]
enum PrivilegeRelationKind {
    Table,
    Sequence,
}

#[derive(Clone, Copy)]
enum ColumnLookup {
    Name,
    Attnum,
}

fn privilege_catalog<'a>(
    ctx: &'a ExecutorContext,
    function_name: &'static str,
) -> Result<&'a dyn CatalogLookup, ExecError> {
    ctx.catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("{function_name} requires catalog context"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })
}

fn invalid_privilege_type_error(privilege: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unrecognized privilege type: \"{privilege}\""),
        detail: None,
        hint: None,
        sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
    }
}

fn parse_privilege_specs(
    value: &Value,
    function_name: &'static str,
    map: &[(&'static str, char, bool)],
) -> Result<Vec<PrivilegeSpec>, ExecError> {
    let Some(privilege_text) = value.as_text() else {
        return Err(ExecError::TypeMismatch {
            op: function_name,
            left: value.clone(),
            right: Value::Text("".into()),
        });
    };
    privilege_text
        .split(',')
        .map(str::trim)
        .map(|chunk| {
            map.iter()
                .find(|(name, _, _)| chunk.eq_ignore_ascii_case(name))
                .map(|(_, acl_char, grant_option)| PrivilegeSpec {
                    acl_char: *acl_char,
                    grant_option: *grant_option,
                })
                .ok_or_else(|| invalid_privilege_type_error(chunk))
        })
        .collect()
}

fn parse_role_privilege_specs(value: &Value) -> Result<Vec<RolePrivilegeSpec>, ExecError> {
    let Some(privilege_text) = value.as_text() else {
        return Err(ExecError::TypeMismatch {
            op: "pg_has_role privilege",
            left: value.clone(),
            right: Value::Text("".into()),
        });
    };
    privilege_text
        .split(',')
        .map(str::trim)
        .map(|chunk| {
            if chunk.eq_ignore_ascii_case("USAGE") {
                Ok(RolePrivilegeSpec::Usage)
            } else if chunk.eq_ignore_ascii_case("MEMBER") {
                Ok(RolePrivilegeSpec::Member)
            } else if chunk.eq_ignore_ascii_case("SET") {
                Ok(RolePrivilegeSpec::Set)
            } else if chunk.eq_ignore_ascii_case("USAGE WITH GRANT OPTION")
                || chunk.eq_ignore_ascii_case("USAGE WITH ADMIN OPTION")
                || chunk.eq_ignore_ascii_case("MEMBER WITH GRANT OPTION")
                || chunk.eq_ignore_ascii_case("MEMBER WITH ADMIN OPTION")
                || chunk.eq_ignore_ascii_case("SET WITH GRANT OPTION")
                || chunk.eq_ignore_ascii_case("SET WITH ADMIN OPTION")
            {
                Ok(RolePrivilegeSpec::Admin)
            } else {
                Err(invalid_privilege_type_error(chunk))
            }
        })
        .collect()
}

fn acl_privileges_contain(privileges: &str, spec: PrivilegeSpec) -> bool {
    let mut chars = privileges.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == spec.acl_char {
            return !spec.grant_option || matches!(chars.peek(), Some('*'));
        }
    }
    false
}

fn acl_grants_privilege_to_names(
    acl: &[String],
    effective_names: &std::collections::BTreeSet<String>,
    spec: PrivilegeSpec,
) -> bool {
    acl.iter().any(|item| {
        acl_item_parts(item).is_some_and(|(grantee, privileges, _)| {
            effective_names.contains(grantee) && acl_privileges_contain(privileges, spec)
        })
    })
}

fn role_row_by_oid(authid_rows: &[PgAuthIdRow], role_oid: u32) -> Option<&PgAuthIdRow> {
    authid_rows.iter().find(|role| role.oid == role_oid)
}

fn role_is_superuser(authid_rows: &[PgAuthIdRow], role_oid: u32) -> bool {
    role_row_by_oid(authid_rows, role_oid).is_some_and(|role| role.rolsuper)
}

fn effective_role_names_for_oid(
    catalog: &dyn CatalogLookup,
    role_oid: u32,
) -> std::collections::BTreeSet<String> {
    let roles = catalog.authid_rows();
    let memberships = catalog.auth_members_rows();
    let mut names = std::collections::BTreeSet::from([String::new()]);
    if role_oid == 0 {
        return names;
    }
    for role in &roles {
        if crate::backend::catalog::role_memberships::has_effective_membership(
            role_oid,
            role.oid,
            &roles,
            &memberships,
        ) {
            names.insert(role.rolname.clone());
        }
    }
    names
}

fn numeric_role_oid_from_value(value: &Value) -> Option<u32> {
    match value {
        Value::Int32(oid) => u32::try_from(*oid).ok(),
        Value::Int64(oid) => u32::try_from(*oid).ok(),
        _ => None,
    }
}

fn role_oid_from_value(
    value: &Value,
    ctx: &ExecutorContext,
    function_name: &'static str,
) -> Result<u32, ExecError> {
    if let Some(oid) = numeric_role_oid_from_value(value) {
        return Ok(oid);
    }
    if matches!(value, Value::Int32(_) | Value::Int64(_)) {
        return Ok(u32::MAX);
    }
    let catalog = privilege_catalog(ctx, function_name)?;
    let Some(name) = value.as_text() else {
        return Err(ExecError::TypeMismatch {
            op: function_name,
            left: value.clone(),
            right: Value::Text("".into()),
        });
    };
    if name.eq_ignore_ascii_case("public") {
        return Ok(0);
    }
    catalog
        .authid_rows()
        .into_iter()
        .find(|row| row.rolname.eq_ignore_ascii_case(name))
        .map(|row| row.oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("role \"{name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })
}

fn relation_class_from_value(
    value: &Value,
    catalog: &dyn CatalogLookup,
    op: &'static str,
) -> Result<(Option<PgClassRow>, bool), ExecError> {
    if let Some(name) = value.as_text() {
        let relation = catalog
            .lookup_any_relation(name)
            .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(name.to_string())))?;
        let class_row = catalog
            .class_row_by_oid(relation.relation_oid)
            .unwrap_or_else(|| PgClassRow {
                oid: relation.relation_oid,
                relname: name.to_string(),
                relnamespace: relation.namespace_oid,
                reltype: 0,
                relowner: relation.owner_oid,
                relam: 0,
                relfilenode: relation.relation_oid,
                reltablespace: 0,
                relpages: 0,
                reltuples: 0.0,
                relallvisible: 0,
                relallfrozen: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relpersistence: relation.relpersistence,
                relkind: relation.relkind,
                relnatts: relation.desc.columns.len() as i16,
                relhassubclass: false,
                relhastriggers: false,
                relrowsecurity: false,
                relforcerowsecurity: false,
                relispopulated: relation.relispopulated,
                relispartition: relation.relispartition,
                relfrozenxid: 0,
                relpartbound: relation.relpartbound,
                reloptions: None,
                relacl: None,
                relreplident: 'd',
                reloftype: relation.of_type_oid,
            });
        return Ok((Some(class_row), false));
    }
    let oid = oid_arg_to_u32(value, op)?;
    Ok((catalog.class_row_by_oid(oid), true))
}

fn relation_name_for_error(class_row: &PgClassRow) -> String {
    class_row.relname.clone()
}

fn is_protected_system_class(class_row: &PgClassRow) -> bool {
    matches!(
        class_row.relnamespace,
        PG_CATALOG_NAMESPACE_OID | PG_TOAST_NAMESPACE_OID
    ) && class_row.relkind != 'v'
}

fn system_catalog_public_select(class_row: &PgClassRow) -> bool {
    class_row.relnamespace == PG_CATALOG_NAMESPACE_OID
        && !matches!(
            class_row.oid,
            PG_AUTHID_RELATION_OID | PG_LARGEOBJECT_RELATION_OID
        )
}

fn role_has_effective_membership(
    role_oid: u32,
    target_oid: u32,
    authid_rows: &[PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
) -> bool {
    if role_oid == 0 {
        return false;
    }
    crate::backend::catalog::role_memberships::has_effective_membership(
        role_oid,
        target_oid,
        authid_rows,
        auth_members_rows,
    )
}

fn relation_acl_allows_role(
    catalog: &dyn CatalogLookup,
    role_oid: u32,
    class_row: &PgClassRow,
    spec: PrivilegeSpec,
) -> bool {
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if !role_is_superuser(&authid_rows, role_oid)
        && is_protected_system_class(class_row)
        && matches!(spec.acl_char, 'a' | 'w' | 'd' | 'D' | 'U')
    {
        return false;
    }
    if role_is_superuser(&authid_rows, role_oid) {
        return true;
    }
    if role_has_effective_membership(
        role_oid,
        class_row.relowner,
        &authid_rows,
        &auth_members_rows,
    ) {
        return true;
    }
    if spec.acl_char == 'r' && system_catalog_public_select(class_row) && !spec.grant_option {
        return true;
    }
    if spec.acl_char == 'r'
        && role_has_effective_membership(
            role_oid,
            PG_READ_ALL_DATA_OID,
            &authid_rows,
            &auth_members_rows,
        )
    {
        return true;
    }
    if matches!(spec.acl_char, 'a' | 'w' | 'd')
        && role_has_effective_membership(
            role_oid,
            PG_WRITE_ALL_DATA_OID,
            &authid_rows,
            &auth_members_rows,
        )
    {
        return true;
    }
    class_row.relacl.as_deref().is_some_and(|acl| {
        acl_grants_privilege_to_names(acl, &effective_role_names_for_oid(catalog, role_oid), spec)
    })
}

fn eval_has_relation_privilege(
    kind: PrivilegeRelationKind,
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (function_name, privilege_map): (&'static str, &[(&'static str, char, bool)]) = match kind {
        PrivilegeRelationKind::Table => (
            "has_table_privilege",
            &[
                ("SELECT", 'r', false),
                ("SELECT WITH GRANT OPTION", 'r', true),
                ("INSERT", 'a', false),
                ("INSERT WITH GRANT OPTION", 'a', true),
                ("UPDATE", 'w', false),
                ("UPDATE WITH GRANT OPTION", 'w', true),
                ("DELETE", 'd', false),
                ("DELETE WITH GRANT OPTION", 'd', true),
                ("TRUNCATE", 'D', false),
                ("TRUNCATE WITH GRANT OPTION", 'D', true),
                ("REFERENCES", 'x', false),
                ("REFERENCES WITH GRANT OPTION", 'x', true),
                ("TRIGGER", 't', false),
                ("TRIGGER WITH GRANT OPTION", 't', true),
                ("MAINTAIN", 'm', false),
                ("MAINTAIN WITH GRANT OPTION", 'm', true),
            ],
        ),
        PrivilegeRelationKind::Sequence => (
            "has_sequence_privilege",
            &[
                ("USAGE", 'U', false),
                ("USAGE WITH GRANT OPTION", 'U', true),
                ("SELECT", 'r', false),
                ("SELECT WITH GRANT OPTION", 'r', true),
                ("UPDATE", 'w', false),
                ("UPDATE WITH GRANT OPTION", 'w', true),
            ],
        ),
    };
    let catalog = privilege_catalog(ctx, function_name)?;
    let (role_oid, relation_value, privilege_value) = match values {
        [relation_value, privilege_value] => {
            (ctx.current_user_oid, relation_value, privilege_value)
        }
        [role_value, relation_value, privilege_value] => (
            role_oid_from_value(role_value, ctx, function_name)?,
            relation_value,
            privilege_value,
        ),
        _ => return Err(malformed_expr_error(function_name)),
    };
    let specs = parse_privilege_specs(privilege_value, function_name, privilege_map)?;
    let (class_row, oid_lookup) =
        relation_class_from_value(relation_value, catalog, function_name)?;
    let Some(class_row) = class_row else {
        return if oid_lookup {
            Ok(Value::Null)
        } else {
            unreachable!("relation name lookup errors before returning None")
        };
    };
    if matches!(kind, PrivilegeRelationKind::Sequence) && class_row.relkind != 'S' {
        return Err(ExecError::DetailedError {
            message: format!(
                "\"{}\" is not a sequence",
                relation_name_for_error(&class_row)
            ),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(Value::Bool(specs.into_iter().any(|spec| {
        relation_acl_allows_role(catalog, role_oid, &class_row, spec)
    })))
}

fn column_attnum_from_value(value: &Value, function_name: &'static str) -> Result<i16, ExecError> {
    match value {
        Value::Int16(v) => Ok(*v),
        Value::Int32(v) => i16::try_from(*v).map_err(|_| ExecError::TypeMismatch {
            op: function_name,
            left: value.clone(),
            right: Value::Int16(0),
        }),
        Value::Int64(v) => i16::try_from(*v).map_err(|_| ExecError::TypeMismatch {
            op: function_name,
            left: value.clone(),
            right: Value::Int16(0),
        }),
        _ => Err(ExecError::TypeMismatch {
            op: function_name,
            left: value.clone(),
            right: Value::Int16(0),
        }),
    }
}

fn column_lookup_kind(value: &Value) -> ColumnLookup {
    if value.as_text().is_some() {
        ColumnLookup::Name
    } else {
        ColumnLookup::Attnum
    }
}

fn attribute_from_value(
    relation: &PgClassRow,
    column_value: &Value,
    catalog: &dyn CatalogLookup,
    function_name: &'static str,
) -> Result<Option<PgAttributeRow>, ExecError> {
    let attributes = catalog.attribute_rows_for_relation(relation.oid);
    match column_lookup_kind(column_value) {
        ColumnLookup::Name => {
            let name = column_value.as_text().expect("text column value");
            if let Some(attr) = attributes
                .into_iter()
                .find(|attr| attr.attname.eq_ignore_ascii_case(name))
            {
                return Ok((!attr.attisdropped).then_some(attr));
            }
            Err(ExecError::DetailedError {
                message: format!(
                    "column \"{name}\" of relation \"{}\" does not exist",
                    relation.relname
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            })
        }
        ColumnLookup::Attnum => {
            let attnum = column_attnum_from_value(column_value, function_name)?;
            Ok(attributes
                .into_iter()
                .find(|attr| attr.attnum == attnum && !attr.attisdropped))
        }
    }
}

fn eval_has_column_privilege(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let function_name = "has_column_privilege";
    let catalog = privilege_catalog(ctx, function_name)?;
    let (role_oid, relation_value, column_value, privilege_value) = match values {
        [relation_value, column_value, privilege_value] => (
            ctx.current_user_oid,
            relation_value,
            column_value,
            privilege_value,
        ),
        [role_value, relation_value, column_value, privilege_value] => (
            role_oid_from_value(role_value, ctx, function_name)?,
            relation_value,
            column_value,
            privilege_value,
        ),
        _ => return Err(malformed_expr_error(function_name)),
    };
    let specs = parse_privilege_specs(
        privilege_value,
        function_name,
        &[
            ("SELECT", 'r', false),
            ("SELECT WITH GRANT OPTION", 'r', true),
            ("INSERT", 'a', false),
            ("INSERT WITH GRANT OPTION", 'a', true),
            ("UPDATE", 'w', false),
            ("UPDATE WITH GRANT OPTION", 'w', true),
            ("REFERENCES", 'x', false),
            ("REFERENCES WITH GRANT OPTION", 'x', true),
        ],
    )?;
    let (relation, oid_lookup) = relation_class_from_value(relation_value, catalog, function_name)?;
    let Some(relation) = relation else {
        return if oid_lookup {
            Ok(Value::Null)
        } else {
            unreachable!("relation name lookup errors before returning None")
        };
    };
    let Some(attribute) = attribute_from_value(&relation, column_value, catalog, function_name)?
    else {
        return Ok(Value::Null);
    };
    Ok(Value::Bool(specs.into_iter().any(|spec| {
        relation_acl_allows_role(catalog, role_oid, &relation, spec)
            || attribute.attacl.as_deref().is_some_and(|acl| {
                acl_grants_privilege_to_names(
                    acl,
                    &effective_role_names_for_oid(catalog, role_oid),
                    spec,
                )
            })
    })))
}

fn eval_has_any_column_privilege(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let function_name = "has_any_column_privilege";
    let catalog = privilege_catalog(ctx, function_name)?;
    let (role_oid, relation_value, privilege_value) = match values {
        [relation_value, privilege_value] => {
            (ctx.current_user_oid, relation_value, privilege_value)
        }
        [role_value, relation_value, privilege_value] => (
            role_oid_from_value(role_value, ctx, function_name)?,
            relation_value,
            privilege_value,
        ),
        _ => return Err(malformed_expr_error(function_name)),
    };
    let specs = parse_privilege_specs(
        privilege_value,
        function_name,
        &[
            ("SELECT", 'r', false),
            ("SELECT WITH GRANT OPTION", 'r', true),
            ("INSERT", 'a', false),
            ("INSERT WITH GRANT OPTION", 'a', true),
            ("UPDATE", 'w', false),
            ("UPDATE WITH GRANT OPTION", 'w', true),
            ("REFERENCES", 'x', false),
            ("REFERENCES WITH GRANT OPTION", 'x', true),
        ],
    )?;
    let (relation, oid_lookup) = relation_class_from_value(relation_value, catalog, function_name)?;
    let Some(relation) = relation else {
        return if oid_lookup {
            Ok(Value::Null)
        } else {
            unreachable!("relation name lookup errors before returning None")
        };
    };
    let effective_names = effective_role_names_for_oid(catalog, role_oid);
    Ok(Value::Bool(specs.into_iter().any(|spec| {
        relation_acl_allows_role(catalog, role_oid, &relation, spec)
            || catalog
                .attribute_rows_for_relation(relation.oid)
                .into_iter()
                .filter(|attr| attr.attnum > 0 && !attr.attisdropped)
                .filter_map(|attr| attr.attacl)
                .any(|acl| acl_grants_privilege_to_names(&acl, &effective_names, spec))
    })))
}

fn eval_has_largeobject_privilege(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let function_name = "has_largeobject_privilege";
    let catalog = privilege_catalog(ctx, function_name)?;
    let (role_oid, object_value, privilege_value) = match values {
        [object_value, privilege_value] => (ctx.current_user_oid, object_value, privilege_value),
        [role_value, object_value, privilege_value] => (
            role_oid_from_value(role_value, ctx, function_name)?,
            object_value,
            privilege_value,
        ),
        _ => return Err(malformed_expr_error(function_name)),
    };
    let specs = parse_privilege_specs(
        privilege_value,
        function_name,
        &[
            ("SELECT", 'r', false),
            ("SELECT WITH GRANT OPTION", 'r', true),
            ("UPDATE", 'w', false),
            ("UPDATE WITH GRANT OPTION", 'w', true),
        ],
    )?;
    let oid = oid_arg_to_u32(object_value, function_name)?;
    let Some(row) = large_object_runtime(ctx)?.metadata_row(oid) else {
        return Ok(Value::Null);
    };
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    Ok(Value::Bool(specs.into_iter().any(|spec| {
        role_is_superuser(&authid_rows, role_oid)
            || role_has_effective_membership(
                role_oid,
                row.lomowner,
                &authid_rows,
                &auth_members_rows,
            )
            || acl_grants_privilege_to_names(
                &row.lomacl,
                &effective_role_names_for_oid(catalog, role_oid),
                spec,
            )
    })))
}

fn membership_path_with(
    start_member: u32,
    target_role: u32,
    rows: &[PgAuthMembersRow],
    edge_allows: impl Fn(&PgAuthMembersRow) -> bool,
) -> bool {
    let mut pending = std::collections::VecDeque::from([start_member]);
    let mut visited = std::collections::BTreeSet::new();
    while let Some(member) = pending.pop_front() {
        if !visited.insert(member) {
            continue;
        }
        for edge in rows
            .iter()
            .filter(|row| row.member == member && edge_allows(row))
        {
            if edge.roleid == target_role {
                return true;
            }
            pending.push_back(edge.roleid);
        }
    }
    false
}

fn current_database_owner_oid(catalog: &dyn CatalogLookup, ctx: &ExecutorContext) -> Option<u32> {
    catalog
        .database_rows()
        .into_iter()
        .find(|row| row.datname.eq_ignore_ascii_case(&ctx.current_database_name))
        .map(|row| row.datdba)
}

fn effective_pg_has_role_target(
    target_oid: u32,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Option<u32> {
    if target_oid == PG_DATABASE_OWNER_OID {
        current_database_owner_oid(catalog, ctx)
    } else {
        Some(target_oid)
    }
}

fn role_privilege_allowed(
    role_oid: u32,
    target_oid: u32,
    spec: RolePrivilegeSpec,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> bool {
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if role_is_superuser(&authid_rows, role_oid) {
        return true;
    }
    let Some(effective_target_oid) = effective_pg_has_role_target(target_oid, catalog, ctx) else {
        return false;
    };
    match spec {
        RolePrivilegeSpec::Usage => role_has_effective_membership(
            role_oid,
            effective_target_oid,
            &authid_rows,
            &auth_members_rows,
        ),
        RolePrivilegeSpec::Member => {
            role_oid == effective_target_oid
                || membership_path_with(role_oid, effective_target_oid, &auth_members_rows, |_| {
                    true
                })
        }
        RolePrivilegeSpec::Set => {
            role_oid == effective_target_oid
                || membership_path_with(
                    role_oid,
                    effective_target_oid,
                    &auth_members_rows,
                    |edge| edge.set_option,
                )
        }
        RolePrivilegeSpec::Admin => {
            target_oid != PG_DATABASE_OWNER_OID
                && membership_path_with(
                    role_oid,
                    effective_target_oid,
                    &auth_members_rows,
                    |edge| edge.admin_option,
                )
        }
    }
}

fn eval_pg_has_role(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let function_name = "pg_has_role";
    let catalog = privilege_catalog(ctx, function_name)?;
    let (role_oid, target_value, privilege_value) = match values {
        [target_value, privilege_value] => (ctx.current_user_oid, target_value, privilege_value),
        [role_value, target_value, privilege_value] => (
            role_oid_from_value(role_value, ctx, function_name)?,
            target_value,
            privilege_value,
        ),
        _ => return Err(malformed_expr_error(function_name)),
    };
    let target_oid = role_oid_from_value(target_value, ctx, function_name)?;
    let specs = parse_role_privilege_specs(privilege_value)?;
    Ok(Value::Bool(specs.into_iter().any(|spec| {
        role_privilege_allowed(role_oid, target_oid, spec, catalog, ctx)
    })))
}

fn function_oid_from_signature(
    signature: &str,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ExecError> {
    let trimmed = signature.trim();
    let Some(open) = trimmed.find('(') else {
        return catalog
            .proc_rows_by_name(trimmed)
            .into_iter()
            .find(|row| row.prokind == 'f')
            .map(|row| row.oid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("function {trimmed} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42883",
            });
    };
    let close = trimmed.rfind(')').ok_or_else(|| ExecError::DetailedError {
        message: format!("invalid function signature \"{signature}\""),
        detail: None,
        hint: None,
        sqlstate: "42601",
    })?;
    let name = trimmed[..open].trim();
    let arg_sql = trimmed[open + 1..close].trim();
    let arg_oids = if arg_sql.is_empty() {
        Vec::new()
    } else {
        arg_sql
            .split(',')
            .map(str::trim)
            .map(|arg| {
                let raw = crate::backend::parser::parse_type_name(arg).map_err(ExecError::Parse)?;
                let ty = crate::backend::parser::resolve_raw_type_name(&raw, catalog)
                    .map_err(ExecError::Parse)?;
                catalog
                    .type_oid_for_sql_type(ty)
                    .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(arg.into())))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let (schema_name, base_name) = name
        .rsplit_once('.')
        .map(|(schema, proc_name)| (Some(schema), proc_name))
        .unwrap_or((None, name));
    let namespace_oid = schema_name.and_then(|schema_name| {
        catalog
            .namespace_rows()
            .into_iter()
            .find(|row| row.nspname.eq_ignore_ascii_case(schema_name))
            .map(|row| row.oid)
    });
    catalog
        .proc_rows_by_name(base_name)
        .into_iter()
        .find(|row| {
            row.prokind == 'f'
                && namespace_oid
                    .map(|oid| row.pronamespace == oid)
                    .unwrap_or(true)
                && row
                    .proargtypes
                    .split_whitespace()
                    .filter_map(|part| part.parse::<u32>().ok())
                    .collect::<Vec<_>>()
                    == arg_oids
        })
        .map(|row| row.oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("function {signature} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })
}

fn function_oid_from_value(value: &Value, catalog: &dyn CatalogLookup) -> Result<u32, ExecError> {
    if let Ok(oid) = oid_arg_to_u32(value, "has_function_privilege function") {
        return Ok(oid);
    }
    let Some(signature) = value.as_text() else {
        return Err(ExecError::TypeMismatch {
            op: "has_function_privilege function",
            left: value.clone(),
            right: Value::Text("".into()),
        });
    };
    function_oid_from_signature(signature, catalog)
}

fn eval_has_function_privilege(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let catalog = ctx
        .catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "has_function_privilege requires catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })?;
    let (role_oid, function_value, privilege_value) = match values {
        [function_value, privilege_value] => {
            (ctx.current_user_oid, function_value, privilege_value)
        }
        [role_value, function_value, privilege_value] => (
            role_oid_from_value(role_value, ctx, "has_function_privilege")?,
            function_value,
            privilege_value,
        ),
        _ => return Err(malformed_expr_error("has_function_privilege")),
    };
    let Some(privilege) = privilege_value.as_text() else {
        return Err(ExecError::TypeMismatch {
            op: "has_function_privilege privilege",
            left: privilege_value.clone(),
            right: Value::Text("".into()),
        });
    };
    if !privilege.eq_ignore_ascii_case("execute") {
        return Ok(Value::Bool(false));
    }
    let function_oid = function_oid_from_value(function_value, catalog)?;
    let row = catalog
        .proc_row_by_oid(function_oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("function with OID {function_oid} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
    let owner_name = catalog
        .authid_rows()
        .into_iter()
        .find(|role| role.oid == row.proowner)
        .map(|role| role.rolname)
        .unwrap_or_else(|| "postgres".into());
    let acl = row.proacl.unwrap_or_else(|| {
        vec![
            format!("{owner_name}=X/{owner_name}"),
            format!("=X/{owner_name}"),
        ]
    });
    let effective_names = effective_role_names_for_oid(catalog, role_oid);
    if effective_names
        .iter()
        .any(|name| bootstrap_proc_execute_acl_has_grantee(function_oid, name))
    {
        return Ok(Value::Bool(true));
    }
    Ok(Value::Bool(acl.iter().any(|item| {
        acl_item_parts(item).is_some_and(|(grantee, privileges, _)| {
            effective_names.contains(grantee) && privileges.contains('X')
        })
    })))
}

fn eval_pg_replication_origin_create(values: &[Value]) -> Result<Value, ExecError> {
    let Some(name) = values.first().and_then(Value::as_text) else {
        return Ok(Value::Null);
    };
    if name.len() > 512 {
        return Err(ExecError::DetailedError {
            message: "replication origin name is too long".into(),
            detail: Some("Replication origin names must be no longer than 512 bytes.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    // :HACK: The durable pg_replication_origin catalog is present, but origin
    // creation is not wired through a transactionally allocated local_id yet.
    Ok(Value::Int64(1))
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
        Value::Int32(_) | Value::EnumOid(_) | Value::Date(_) | Value::InternalChar(_) => 4,
        Value::Int64(_)
        | Value::Xid8(_)
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
        Value::MacAddr(v) => crate::backend::executor::render_macaddr_text(v).len(),
        Value::MacAddr8(v) => crate::backend::executor::render_macaddr8_text(v).len(),
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
    let value = match values {
        [value] | [value, _] => value,
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "pg_relation_size(regclass)",
                actual: format!("PgRelationSize({} args)", values.len()),
            }));
        }
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if values
        .get(1)
        .is_some_and(|fork| matches!(fork, Value::Null))
    {
        return Ok(Value::Null);
    }

    let catalog = executor_catalog(ctx)?;
    let relation_oid = match oid_arg_to_u32(value, "pg_relation_size") {
        Ok(oid) => oid,
        Err(err) if value.as_text().is_some() => {
            let relation_name = value.as_text().expect("guarded above");
            catalog
                .lookup_any_relation(relation_name)
                .map(|relation| relation.relation_oid)
                .ok_or(err)?
        }
        Err(err) => return Err(err),
    };
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Err(ExecError::DetailedError {
            message: format!("could not open relation with OID {relation_oid}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if relation.relkind == 't'
        && let Some(parent) = catalog
            .class_rows()
            .into_iter()
            .find_map(|class| (class.reltoastrelid == relation_oid).then_some(class.oid))
            .and_then(|oid| catalog.relation_by_oid(oid))
    {
        return Ok(Value::Int64(
            if parent_references_toast_relation(&parent, relation_oid, ctx)? {
                i64::from(crate::backend::storage::smgr::smgr::BLCKSZ as i32)
            } else {
                0
            },
        ));
    }
    relation_main_fork_size(&relation, ctx).map(Value::Int64)
}

fn relation_main_fork_size(
    relation: &crate::backend::parser::BoundRelation,
    ctx: &ExecutorContext,
) -> Result<i64, ExecError> {
    let nblocks = ctx
        .pool
        .with_storage_mut(|s| s.smgr.nblocks(relation.rel, ForkNumber::Main))
        .map_err(crate::backend::access::heap::heapam::HeapError::Storage)?;
    Ok(i64::from(nblocks) * i64::from(crate::backend::storage::smgr::smgr::BLCKSZ as i32))
}

fn eval_pg_table_size(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_table_size(regclass)",
            actual: format!("PgTableSize({} args)", values.len()),
        }));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    let catalog = executor_catalog(ctx)?;
    let relation_oid = match oid_arg_to_u32(value, "pg_table_size") {
        Ok(oid) => oid,
        Err(err) if value.as_text().is_some() => {
            let relation_name = value.as_text().expect("guarded above");
            catalog
                .lookup_any_relation(relation_name)
                .map(|relation| relation.relation_oid)
                .ok_or(err)?
        }
        Err(err) => return Err(err),
    };
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Ok(Value::Null);
    };
    if relation.rel.rel_number == 0 {
        return Ok(Value::Int64(0));
    }
    relation_main_fork_size(&relation, ctx).map(Value::Int64)
}

fn eval_pg_tablespace_location(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_tablespace_location(oid)",
            actual: format!("PgTablespaceLocation({} args)", values.len()),
        }));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    let mut tablespace_oid = oid_arg_to_u32(value, "pg_tablespace_location")?;
    if tablespace_oid == 0 {
        tablespace_oid = DEFAULT_TABLESPACE_OID;
    }
    if matches!(
        tablespace_oid,
        DEFAULT_TABLESPACE_OID | GLOBAL_TABLESPACE_OID
    ) {
        return Ok(Value::Text("".into()));
    }

    let Some(data_dir) = &ctx.data_dir else {
        return Ok(Value::Text("".into()));
    };
    let source_path = data_dir.join("pg_tblspc").join(tablespace_oid.to_string());
    let metadata = match std::fs::symlink_metadata(&source_path) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(Value::Null),
    };
    if metadata.file_type().is_symlink()
        && let Ok(target) = std::fs::read_link(&source_path)
    {
        return Ok(Value::Text(target.display().to_string().into()));
    }
    Ok(Value::Text(source_path.display().to_string().into()))
}

fn eval_pg_relation_filenode(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_relation_filenode(regclass)",
            actual: format!("PgRelationFilenode({} args)", values.len()),
        }));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let relation_oid = oid_arg_to_u32(value, "pg_relation_filenode")?;
    let catalog = executor_catalog(ctx)?;
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Ok(Value::Null);
    };
    if relation.rel.rel_number == 0 {
        return Ok(Value::Null);
    }
    Ok(Value::Int64(i64::from(relation.rel.rel_number)))
}

fn eval_pg_filenode_relation(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    let [tablespace, filenode] = values else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_filenode_relation(oid, oid)",
            actual: format!("PgFilenodeRelation({} args)", values.len()),
        }));
    };
    if matches!(tablespace, Value::Null) || matches!(filenode, Value::Null) {
        return Ok(Value::Null);
    }
    let tablespace_oid = oid_arg_to_u32(tablespace, "pg_filenode_relation")?;
    let filenode_oid = oid_arg_to_u32(filenode, "pg_filenode_relation")?;
    if filenode_oid == 0 {
        return Ok(Value::Null);
    }
    let catalog = executor_catalog(ctx)?;
    let relation_oid = catalog.class_rows().into_iter().find_map(|class| {
        let relation = catalog.relation_by_oid(class.oid)?;
        (relation.relpersistence != 't'
            && relation.rel.spc_oid == tablespace_oid
            && relation.rel.rel_number == filenode_oid)
            .then_some(relation.relation_oid)
    });
    Ok(relation_oid
        .map(|oid| Value::Int64(i64::from(oid)))
        .unwrap_or(Value::Null))
}

fn parent_references_toast_relation(
    parent: &crate::backend::parser::BoundRelation,
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

fn eval_pg_table_is_visible(values: &[Value], ctx: &ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let relation_oid = oid_arg_to_u32(value, "pg_table_is_visible")?;
            let catalog = executor_catalog(ctx)?;
            let Some(class_row) = catalog.class_row_by_oid(relation_oid) else {
                return Ok(Value::Null);
            };
            Ok(Value::Bool(
                catalog
                    .lookup_any_relation(&class_row.relname)
                    .is_some_and(|relation| relation.relation_oid == relation_oid),
            ))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "pg_table_is_visible(oid)",
            actual: format!("PgTableIsVisible({} args)", values.len()),
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

fn sequence_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> Option<String> {
    let class = catalog.class_row_by_oid(relation_oid)?;
    if catalog
        .lookup_any_relation(&class.relname)
        .is_some_and(|relation| relation.relation_oid == relation_oid)
    {
        return Some(quote_identifier_if_needed(&class.relname));
    }
    let namespace = catalog
        .namespace_row_by_oid(class.relnamespace)
        .map(|row| row.nspname)?;
    Some(format!(
        "{}.{}",
        quote_identifier_if_needed(&namespace),
        quote_identifier_if_needed(&class.relname)
    ))
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
        (OpExprKind::Add, [left, right]) => add_values_with_config(
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
            &ctx.datetime_config,
        ),
        (OpExprKind::Sub, [left, right]) => sub_values_with_config(
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
            &ctx.datetime_config,
        ),
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
        (OpExprKind::Concat, [left, right]) => concat_values_with_cast_context(
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            ctx.catalog.as_deref(),
            &ctx.datetime_config,
        ),
        (OpExprKind::Eq, [left, right]) => compare_values_with_type(
            "=",
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
            Some(&ctx.datetime_config),
        ),
        (OpExprKind::NotEq, [left, right]) => not_equal_values_with_type(
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
            Some(&ctx.datetime_config),
        ),
        (OpExprKind::Lt, [left, right]) => order_values_with_type(
            "<",
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
            ctx,
        ),
        (OpExprKind::LtEq, [left, right]) => order_values_with_type(
            "<=",
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
            ctx,
        ),
        (OpExprKind::Gt, [left, right]) => order_values_with_type(
            ">",
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
            ctx,
        ),
        (OpExprKind::GtEq, [left, right]) => order_values_with_type(
            ">=",
            eval_expr(left, slot, ctx)?,
            expr_sql_type_hint(left),
            eval_expr(right, slot, ctx)?,
            expr_sql_type_hint(right),
            op.collation_oid,
            ctx,
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

fn order_values_with_type(
    op: &'static str,
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    collation_oid: Option<u32>,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(ordering) = mixed_date_timestamp_ordering(&left, &right, Some(&ctx.datetime_config))
    {
        return Ok(Value::Bool(match op {
            "<" => ordering == Ordering::Less,
            "<=" => ordering != Ordering::Greater,
            ">" => ordering == Ordering::Greater,
            ">=" => ordering != Ordering::Less,
            _ => unreachable!("comparison op not supported by order_values_with_type"),
        }));
    }
    if let (
        Value::EnumOid(left_oid),
        Some(SqlType {
            kind: SqlTypeKind::Enum,
            type_oid: left_type_oid,
            ..
        }),
        Value::EnumOid(right_oid),
        Some(SqlType {
            kind: SqlTypeKind::Enum,
            type_oid: right_type_oid,
            ..
        }),
    ) = (&left, left_type, &right, right_type)
        && left_type_oid == right_type_oid
        && left_type_oid != 0
        && let Some(catalog) = ctx.catalog.as_deref()
    {
        let rows = catalog.enum_rows();
        let left_sort = rows
            .iter()
            .find(|row| row.enumtypid == left_type_oid && row.oid == *left_oid)
            .map(|row| row.enumsortorder);
        let right_sort = rows
            .iter()
            .find(|row| row.enumtypid == right_type_oid && row.oid == *right_oid)
            .map(|row| row.enumsortorder);
        if let (Some(left_sort), Some(right_sort)) = (left_sort, right_sort) {
            let ordering = left_sort
                .partial_cmp(&right_sort)
                .unwrap_or(Ordering::Equal);
            return Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }));
        }
    }
    order_values(op, left, right, collation_oid)
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
    super::fmgr::call_scalar_function(func, slot, ctx)
}

fn current_temp_namespace_name(ctx: &ExecutorContext) -> Option<CompactString> {
    if let Some(db) = ctx.database.as_ref()
        && db.has_active_temp_namespace(ctx.client_id)
    {
        let temp_backend_id = db.temp_backend_id(ctx.client_id);
        return Some(
            crate::pgrust::database::Database::temp_namespace_name(temp_backend_id).into(),
        );
    }
    // :HACK: `pg_my_temp_schema()` needs session temp namespace identity, but
    // executor contexts do not thread that through directly yet. Derive the
    // visible temp schema name from the qualified temp relcache entries until
    // temp namespace metadata is carried explicitly alongside the session.
    ctx.catalog
        .as_deref()?
        .search_path()
        .into_iter()
        .find(|schema| schema.starts_with("pg_temp_"))
        .map(Into::into)
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
    let Some(catalog) = ctx.catalog.as_deref() else {
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

fn current_schemas_value(include_implicit: bool, ctx: &ExecutorContext) -> Value {
    let mut schemas = Vec::<String>::new();
    let mut push_schema = |schema: String| {
        if !schemas
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&schema))
        {
            schemas.push(schema);
        }
    };

    if include_implicit {
        if let Some(temp_schema) = current_temp_namespace_name(ctx) {
            push_schema(temp_schema.to_string());
        }
        push_schema("pg_catalog".into());
    }

    let configured_path = ctx
        .catalog
        .as_deref()
        .map(|catalog| catalog.search_path())
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| configured_current_schema_search_path(ctx));
    for schema in configured_path {
        if schema == "$user" {
            continue;
        }
        if schema.eq_ignore_ascii_case("pg_temp") {
            if include_implicit {
                if let Some(temp_schema) = current_temp_namespace_name(ctx) {
                    push_schema(temp_schema.to_string());
                }
            }
            continue;
        }
        if schema.eq_ignore_ascii_case("pg_catalog") && include_implicit {
            continue;
        }
        push_schema(schema);
    }

    Value::PgArray(
        ArrayValue::from_1d(
            schemas
                .into_iter()
                .map(|schema| Value::Text(schema.into()))
                .collect(),
        )
        .with_element_type_oid(NAME_TYPE_OID),
    )
}

fn pg_stat_get_backend_wal_value(values: &[Value], ctx: &ExecutorContext) -> Value {
    let pid = values.first().and_then(|value| match value {
        Value::Int32(value) => Some(*value),
        Value::Int64(value) => i32::try_from(*value).ok(),
        _ => None,
    });
    if pid != Some(ctx.client_id as i32) {
        return Value::Null;
    }
    let wal_bytes = ctx.session_stats.read().backend_wal_write_bytes();
    Value::Record(crate::include::nodes::datum::RecordValue::anonymous(vec![
        ("wal_records".into(), Value::Int64(0)),
        ("wal_fpi".into(), Value::Int64(0)),
        ("wal_bytes".into(), Value::Int64(wal_bytes)),
        ("wal_buffers_full".into(), Value::Int64(0)),
        (
            "stats_reset".into(),
            Value::TimestampTz(crate::backend::utils::activity::now_timestamptz()),
        ),
    ]))
}

fn current_temp_namespace_oid(ctx: &ExecutorContext) -> Option<u32> {
    let name = current_temp_namespace_name(ctx)?;
    ctx.catalog
        .as_deref()?
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

fn eval_bound_system_var(
    bindings: &[crate::include::nodes::execnodes::SystemVarBinding],
    var: &crate::include::nodes::primnodes::Var,
) -> Option<Value> {
    match var.varattno {
        TABLE_OID_ATTR_NO => lookup_system_binding(bindings, var.varno),
        SELF_ITEM_POINTER_ATTR_NO => lookup_ctid_binding(bindings, var.varno),
        _ => None,
    }
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
        Expr::SqlJsonQueryFunction(func) => eval_sql_json_query_function_expr(func, slot, ctx),
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
                if is_system_attr(var.varattno) {
                    Ok(eval_bound_system_var(&ctx.expr_bindings.outer_system_bindings, var)
                        .unwrap_or(Value::Null))
                } else {
                    eval_bound_tuple_var(ctx.expr_bindings.outer_tuple.as_ref(), var)
                }
            } else if var.varno == INNER_VAR {
                if is_system_attr(var.varattno) {
                    Ok(eval_bound_system_var(&ctx.expr_bindings.inner_system_bindings, var)
                        .unwrap_or(Value::Null))
                } else {
                    eval_bound_tuple_var(ctx.expr_bindings.inner_tuple.as_ref(), var)
                }
            } else if var.varno == INDEX_VAR {
                if is_system_attr(var.varattno) {
                    Ok(eval_bound_system_var(&ctx.expr_bindings.index_system_bindings, var)
                        .unwrap_or(Value::Null))
                } else {
                    eval_bound_tuple_var(ctx.expr_bindings.index_tuple.as_ref(), var)
                }
            } else if var.varlevelsup == 1 {
                let mut outer_var = var.clone();
                outer_var.varno = OUTER_VAR;
                outer_var.varlevelsup = 0;
                if is_system_attr(outer_var.varattno) {
                    Ok(
                        eval_bound_system_var(
                            &ctx.expr_bindings.outer_system_bindings,
                            &outer_var,
                        )
                        .unwrap_or(Value::Null),
                    )
                } else {
                    eval_bound_tuple_var(ctx.expr_bindings.outer_tuple.as_ref(), &outer_var)
                }
            } else if var.varlevelsup > 0 {
                Err(ExecError::DetailedError {
                    message: "unlowered outer Var reached executor".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
            } else if var.varattno == TABLE_OID_ATTR_NO {
                Ok(lookup_system_binding(&ctx.system_bindings, var.varno)
                    .or_else(|| slot.table_oid.map(|table_oid| Value::Int64(i64::from(table_oid))))
                    .unwrap_or(Value::Null))
            } else if var.varattno == SELF_ITEM_POINTER_ATTR_NO {
                Ok(lookup_ctid_binding(&ctx.system_bindings, var.varno)
                    .or_else(|| slot.tid().map(ctid_value))
                    .unwrap_or(Value::Null))
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
                    ctx.catalog.as_deref(),
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
            SubLinkType::RowCompareSubLink(op) => {
                let left = subplan.testexpr.as_ref().ok_or(ExecError::DetailedError {
                    message: "malformed row-comparison subplan".into(),
                    detail: Some("row-comparison subplans must carry a test expression".into()),
                    hint: None,
                    sqlstate: "XX000",
                })?;
                let collation_oid = top_level_explicit_collation(left);
                let left_value = eval_expr(left, slot, ctx)?;
                eval_row_compare_subquery(&left_value, op, collation_oid, subplan, slot, ctx)
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
        Expr::Random => Ok(Value::Float64(ctx.random_state.lock().double())),
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
                None,
            ),
            (OpExprKind::NotEq, [left, right]) => not_equal_values_with_type(
                eval_plpgsql_expr(left, slot)?,
                expr_sql_type_hint(left),
                eval_plpgsql_expr(right, slot)?,
                expr_sql_type_hint(right),
                op.collation_oid,
                None,
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
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
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

    let source_fields = if descriptor.fields.len() == record.fields.len() {
        record
            .fields
            .into_iter()
            .map(|value| (value, None))
            .collect::<Vec<_>>()
    } else if descriptor.typrelid != 0 {
        let mut projected = Vec::with_capacity(descriptor.fields.len());
        for target_field in &descriptor.fields {
            let Some((source_index, source_field)) = record
                .descriptor
                .fields
                .iter()
                .enumerate()
                .find(|(_, source_field)| {
                    source_field.name.eq_ignore_ascii_case(&target_field.name)
                })
            else {
                return Err(ExecError::DetailedError {
                    message: "cannot cast record to target composite type".into(),
                    detail: Some(format!(
                        "target expects field \"{}\" but source record does not provide it",
                        target_field.name
                    )),
                    hint: None,
                    sqlstate: "42804",
                });
            };
            let value = record.fields.get(source_index).cloned().ok_or_else(|| {
                ExecError::DetailedError {
                    message: "cannot cast record to target composite type".into(),
                    detail: Some(format!(
                        "source record is missing value for field \"{}\"",
                        source_field.name
                    )),
                    hint: None,
                    sqlstate: "42804",
                }
            })?;
            projected.push((value, Some(source_field.sql_type)));
        }
        projected
    } else {
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
    };

    let fields = source_fields
        .into_iter()
        .zip(descriptor.fields.iter())
        .map(|((value, source_type), field)| {
            cast_value_with_source_type_catalog_and_config(
                value,
                source_type,
                field.sql_type,
                ctx.catalog.as_deref(),
                &ctx.datetime_config,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Record(
        crate::include::nodes::datum::RecordValue::from_descriptor(descriptor, fields),
    ))
}

fn eval_hash_builtin_function(
    kind: HashFunctionKind,
    extended: bool,
    values: &[Value],
) -> Result<Value, ExecError> {
    let opclass = if kind == HashFunctionKind::BpChar {
        Some(BPCHAR_HASH_OPCLASS_OID)
    } else {
        None
    };
    if extended {
        let [value, seed] = values else {
            return Err(malformed_expr_error("hash_extended"));
        };
        if matches!(value, Value::Null) || matches!(seed, Value::Null) {
            return Ok(Value::Null);
        }
        let seed = match seed {
            Value::Int16(seed) => i64::from(*seed),
            Value::Int32(seed) => i64::from(*seed),
            Value::Int64(seed) => *seed,
            _ => {
                return Err(ExecError::TypeMismatch {
                    op: "hash_extended",
                    left: value.clone(),
                    right: seed.clone(),
                });
            }
        };
        let hash = crate::backend::access::hash::hash_value_extended(value, opclass, seed as u64)
            .map_err(|message| hash_function_error(message, true))?
            .unwrap_or(0);
        return Ok(Value::Int64(hash as i64));
    }

    let [value] = values else {
        return Err(malformed_expr_error("hash"));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let hash = crate::backend::access::hash::hash_value_extended(value, opclass, 0)
        .map_err(|message| hash_function_error(message, false))?
        .unwrap_or(0);
    Ok(Value::Int32(hash as u32 as i32))
}

fn hash_function_error(message: String, extended: bool) -> ExecError {
    let message = if extended {
        message.replacen(
            "could not identify a hash function",
            "could not identify an extended hash function",
            1,
        )
    } else {
        message
    };
    ExecError::DetailedError {
        message,
        detail: None,
        hint: None,
        sqlstate: "42883",
    }
}

fn text_search_parse_error(op: &'static str, message: String) -> ExecError {
    ExecError::Parse(ParseError::UnexpectedToken {
        expected: "valid text search input",
        actual: format!("{op}: {message}"),
    })
}

fn eval_ts_match_values(
    values: &[Value],
    default_config_name: Option<&str>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::TsVector(vector), Value::TsQuery(query)] => Ok(Value::Bool(
            crate::backend::executor::eval_tsvector_matches_tsquery(vector, query),
        )),
        [Value::TsQuery(query), Value::TsVector(vector)] => Ok(Value::Bool(
            crate::backend::executor::eval_tsquery_matches_tsvector(query, vector),
        )),
        [left, Value::TsQuery(query)] => {
            let Some(text) = left.as_text() else {
                return Err(ExecError::TypeMismatch {
                    op: "@@",
                    left: left.clone(),
                    right: Value::TsQuery(query.clone()),
                });
            };
            let vector = crate::backend::tsearch::to_tsvector_with_config_name(
                default_config_name,
                text,
                catalog,
            )
            .map_err(|err| text_search_parse_error("@@", err))?;
            Ok(Value::Bool(
                crate::backend::executor::eval_tsvector_matches_tsquery(&vector, query),
            ))
        }
        [left, right] if left.as_text().is_some() && right.as_text().is_some() => {
            let text = left.as_text().unwrap_or_default();
            let query_text = right.as_text().unwrap_or_default();
            let vector = crate::backend::tsearch::to_tsvector_with_config_name(
                default_config_name,
                text,
                catalog,
            )
            .map_err(|err| text_search_parse_error("@@", err))?;
            let query = crate::backend::tsearch::plainto_tsquery_with_config_name(
                default_config_name,
                query_text,
                catalog,
            )
            .map_err(|err| text_search_parse_error("@@", err))?;
            Ok(Value::Bool(
                crate::backend::executor::eval_tsvector_matches_tsquery(&vector, &query),
            ))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "@@",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        }),
    }
}

fn tsquery_is_empty(query: &crate::include::nodes::tsearch::TsQuery) -> bool {
    crate::backend::executor::render_tsquery_text(query).is_empty()
}

fn eval_ts_headline_values(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (document, query) = match values {
        [document, Value::TsQuery(query)] => (document.as_text(), query),
        [document, Value::TsQuery(query), _options] => (document.as_text(), query),
        [_config, document, Value::TsQuery(query)] => (document.as_text(), query),
        [_config, document, Value::TsQuery(query), _options] => (document.as_text(), query),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "ts_headline",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            });
        }
    };
    let document = document.ok_or_else(|| ExecError::TypeMismatch {
        op: "ts_headline",
        left: values.first().cloned().unwrap_or(Value::Null),
        right: values.get(1).cloned().unwrap_or(Value::Null),
    })?;
    if tsquery_is_empty(query) {
        return Ok(Value::Text(document.into()));
    }
    let lexemes = tsquery_lexemes_for_headline(&query.root);
    if lexemes.is_empty() {
        return Ok(Value::Text(document.into()));
    }
    Ok(Value::Text(
        highlight_headline_text(document, &lexemes).into(),
    ))
}

fn tsquery_lexemes_for_headline(node: &crate::include::nodes::tsearch::TsQueryNode) -> Vec<String> {
    use crate::include::nodes::tsearch::TsQueryNode;

    let mut lexemes = Vec::new();
    fn collect(node: &TsQueryNode, lexemes: &mut Vec<String>) {
        match node {
            TsQueryNode::Operand(operand) if !operand.lexeme.as_str().is_empty() => {
                lexemes.push(operand.lexeme.as_str().to_ascii_lowercase());
            }
            TsQueryNode::Operand(_) => {}
            TsQueryNode::Not(inner) => collect(inner, lexemes),
            TsQueryNode::And(left, right) | TsQueryNode::Or(left, right) => {
                collect(left, lexemes);
                collect(right, lexemes);
            }
            TsQueryNode::Phrase { left, right, .. } => {
                collect(left, lexemes);
                collect(right, lexemes);
            }
        }
    }
    collect(node, &mut lexemes);
    lexemes.sort();
    lexemes.dedup();
    lexemes
}

fn highlight_headline_text(document: &str, lexemes: &[String]) -> String {
    let mut out = String::with_capacity(document.len());
    let mut token = String::new();
    for ch in document.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            token.push(ch);
            continue;
        }
        flush_headline_token(&mut out, &mut token, lexemes);
        out.push(ch);
    }
    flush_headline_token(&mut out, &mut token, lexemes);
    out
}

fn flush_headline_token(out: &mut String, token: &mut String, lexemes: &[String]) {
    if token.is_empty() {
        return;
    }
    let lower = token.to_ascii_lowercase();
    if lexemes.iter().any(|lexeme| {
        lower == *lexeme || lower.starts_with(lexeme) || lexeme.starts_with(lower.as_str())
    }) {
        out.push_str("<b>");
        out.push_str(token);
        out.push_str("</b>");
    } else {
        out.push_str(token);
    }
    token.clear();
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
    if let Some(result) = eval_macaddr_function(func, &values) {
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
    if is_text_search_builtin_function(func) {
        return eval_text_search_builtin_function(func, &values, None);
    }
    match func {
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::JsonbToTsVector
        | BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => eval_text_search_builtin_function(func, &values, None),
        BuiltinScalarFunction::Length => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_length_function(&values),
        },
        BuiltinScalarFunction::BitLength => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_bit_length_function(&values),
        },
        BuiltinScalarFunction::ArrayUpper => eval_array_upper_function(&values),
        BuiltinScalarFunction::PgSleep => eval_pg_sleep_function(&values),
        BuiltinScalarFunction::Timezone => eval_timezone_function(
            &values,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
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
        BuiltinScalarFunction::UnsupportedXmlFeature => Err(unsupported_xml_feature_error()),
        BuiltinScalarFunction::Int4Pl
        | BuiltinScalarFunction::Int4Mi
        | BuiltinScalarFunction::Int8Inc
        | BuiltinScalarFunction::Int8IncAny
        | BuiltinScalarFunction::Int4AvgAccum
        | BuiltinScalarFunction::Int8Avg => {
            execute_builtin_scalar_function_value_call(func, &values)
        }
        BuiltinScalarFunction::CurrentSetting => eval_current_setting_without_context(&values),
        BuiltinScalarFunction::PgSettingsGetFlags => eval_pg_settings_get_flags(&values),
        BuiltinScalarFunction::PgColumnCompression => eval_pg_column_compression_values(&values),
        BuiltinScalarFunction::PgColumnToastChunkId => {
            eval_pg_column_toast_chunk_id_values(&values)
        }
        BuiltinScalarFunction::PgColumnSize => eval_pg_column_size_values(&values),
        BuiltinScalarFunction::NumNulls => Ok(eval_num_nulls(&values, func_variadic, true)),
        BuiltinScalarFunction::NumNonNulls => Ok(eval_num_nulls(&values, func_variadic, false)),
        BuiltinScalarFunction::GistTranslateCmpTypeCommon => {
            eval_gist_translate_cmptype_common(&values)
        }
        BuiltinScalarFunction::TestCanonicalizePath => eval_test_canonicalize_path(&values),
        BuiltinScalarFunction::TestRelpath => Ok(Value::Null),
        BuiltinScalarFunction::PgSizePretty => eval_pg_size_pretty_function(&values),
        BuiltinScalarFunction::PgSizeBytes => eval_pg_size_bytes_function(&values),
        BuiltinScalarFunction::Lower => eval_lower_function(&values),
        BuiltinScalarFunction::Upper => eval_upper_function(&values),
        BuiltinScalarFunction::Unistr => eval_unistr_function(&values),
        BuiltinScalarFunction::UnicodeVersion => eval_unicode_version_function(&values),
        BuiltinScalarFunction::UnicodeAssigned => eval_unicode_assigned_function(&values),
        BuiltinScalarFunction::Normalize => eval_unicode_normalize_function(&values),
        BuiltinScalarFunction::IsNormalized => eval_unicode_is_normalized_function(&values),
        BuiltinScalarFunction::Initcap => eval_initcap_function(&values),
        BuiltinScalarFunction::BTrim => eval_trim_function("btrim", &values),
        BuiltinScalarFunction::LTrim => eval_trim_function("ltrim", &values),
        BuiltinScalarFunction::RTrim => eval_trim_function("rtrim", &values),
        BuiltinScalarFunction::TextCat => match values.as_slice() {
            [left, right] => concat_values(left.clone(), right.clone()),
            _ => Err(ExecError::DetailedError {
                message: "textcat expects exactly two arguments".into(),
                detail: None,
                hint: None,
                sqlstate: "42883",
            }),
        },
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
        BuiltinScalarFunction::HasForeignDataWrapperPrivilege => {
            eval_has_foreign_privilege_function(
                ForeignPrivilegeKind::ForeignDataWrapper,
                &values,
                None,
            )
        }
        BuiltinScalarFunction::HasServerPrivilege => {
            eval_has_foreign_privilege_function(ForeignPrivilegeKind::Server, &values, None)
        }
        BuiltinScalarFunction::PgGetPartKeyDef
        | BuiltinScalarFunction::PgTableIsVisible
        | BuiltinScalarFunction::PgTypeIsVisible
        | BuiltinScalarFunction::PgOperatorIsVisible
        | BuiltinScalarFunction::PgOpclassIsVisible
        | BuiltinScalarFunction::PgOpfamilyIsVisible
        | BuiltinScalarFunction::PgConversionIsVisible
        | BuiltinScalarFunction::PgTsParserIsVisible
        | BuiltinScalarFunction::PgTsDictIsVisible
        | BuiltinScalarFunction::PgTsTemplateIsVisible
        | BuiltinScalarFunction::PgTsConfigIsVisible
        | BuiltinScalarFunction::PgTableSize
        | BuiltinScalarFunction::PgTablespaceLocation => Err(ExecError::DetailedError {
            message: "catalog helper requires executor context".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
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
        BuiltinScalarFunction::QuoteIdent => eval_quote_ident_function(&values),
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
        BuiltinScalarFunction::Convert => eval_convert(&values),
        BuiltinScalarFunction::ConvertFrom => eval_convert_from_function(&values),
        BuiltinScalarFunction::ConvertTo => eval_convert_to_function(&values),
        BuiltinScalarFunction::Md5 => eval_md5_function(&values),
        BuiltinScalarFunction::Reverse => eval_reverse_function(&values),
        BuiltinScalarFunction::TextStartsWith => eval_text_starts_with_function(&values),
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
        BuiltinScalarFunction::ToChar => eval_to_char_function(
            &values,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
        BuiltinScalarFunction::ToDate => eval_to_date_function(&values),
        BuiltinScalarFunction::ToNumber => eval_to_number_function(&values),
        BuiltinScalarFunction::ToTimestamp => eval_to_timestamp_function(
            &values,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
        BuiltinScalarFunction::TimestampTzConstructor => eval_timestamptz_constructor_function(
            &values,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        ),
        BuiltinScalarFunction::Abs => eval_abs_function(&values),
        BuiltinScalarFunction::Gcd => eval_gcd_function(&values),
        BuiltinScalarFunction::Lcm => eval_lcm_function(&values),
        BuiltinScalarFunction::Greatest => eval_greatest(&values),
        BuiltinScalarFunction::Least => eval_least(&values),
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
        BuiltinScalarFunction::TrimArray => eval_trim_array_function(&values),
        BuiltinScalarFunction::ArrayShuffle => eval_array_shuffle_function(&values),
        BuiltinScalarFunction::ArraySample => eval_array_sample_function(&values),
        BuiltinScalarFunction::ArrayReverse => eval_array_reverse_function(&values),
        BuiltinScalarFunction::ArraySort => eval_array_sort_function(&values),
        BuiltinScalarFunction::BoolEq => eval_booleq(&values),
        BuiltinScalarFunction::BoolNe => eval_boolne(&values),
        BuiltinScalarFunction::BoolAndStateFunc => eval_booland_statefunc(&values),
        BuiltinScalarFunction::BoolOrStateFunc => eval_boolor_statefunc(&values),
        BuiltinScalarFunction::Extract => eval_extract_function(&values),
        BuiltinScalarFunction::DateBin => eval_date_bin_function(&values),
        BuiltinScalarFunction::JustifyDays => eval_justify_days_function(&values),
        BuiltinScalarFunction::JustifyHours => eval_justify_hours_function(&values),
        BuiltinScalarFunction::JustifyInterval => eval_justify_interval_function(&values),
        BuiltinScalarFunction::MakeInterval => eval_make_interval_function(&values),
        BuiltinScalarFunction::IntervalHash => {
            eval_hash_builtin_function(HashFunctionKind::Interval, false, &values)
        }
        BuiltinScalarFunction::HashValue(kind) => eval_hash_builtin_function(kind, false, &values),
        BuiltinScalarFunction::HashValueExtended(kind) => {
            eval_hash_builtin_function(kind, true, &values)
        }
        BuiltinScalarFunction::XmlComment => eval_xml_comment_function(&values, None),
        BuiltinScalarFunction::XmlText => eval_xml_text_function(&values, None),
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
        BuiltinScalarFunction::XPath => eval_xpath_function(&values),
        BuiltinScalarFunction::XPathExists => eval_xpath_exists_function(&values),
        BuiltinScalarFunction::TsMatch => eval_ts_match_values(&values, None, None),
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
        BuiltinScalarFunction::TsQueryContains => match values.as_slice() {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::Bool(
                crate::backend::executor::tsquery_contains(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "@>",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryContainedBy => match values.as_slice() {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::Bool(
                crate::backend::executor::tsquery_contained_by(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "<@",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
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
        | BuiltinScalarFunction::PgEncodingToChar
        | BuiltinScalarFunction::PgGetAcl
        | BuiltinScalarFunction::PgGetUserById
        | BuiltinScalarFunction::ObjDescription
        | BuiltinScalarFunction::PgDescribeObject
        | BuiltinScalarFunction::PgGetFunctionArguments
        | BuiltinScalarFunction::PgGetFunctionDef
        | BuiltinScalarFunction::PgGetFunctionResult
        | BuiltinScalarFunction::PgGetExpr
        | BuiltinScalarFunction::PgGetPartitionConstraintDef
        | BuiltinScalarFunction::PgGetStatisticsObjDef
        | BuiltinScalarFunction::PgGetStatisticsObjDefColumns
        | BuiltinScalarFunction::PgGetStatisticsObjDefExpressions
        | BuiltinScalarFunction::PgStatisticsObjIsVisible
        | BuiltinScalarFunction::PgFunctionIsVisible
        | BuiltinScalarFunction::PgRelationIsPublishable
        | BuiltinScalarFunction::PgIndexAmHasProperty
        | BuiltinScalarFunction::PgIndexHasProperty
        | BuiltinScalarFunction::PgIndexColumnHasProperty
        | BuiltinScalarFunction::AmValidate
        | BuiltinScalarFunction::BtEqualImage
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

fn eval_pg_sleep_function(values: &[Value]) -> Result<Value, ExecError> {
    let seconds = match values {
        [Value::Null] => return Ok(Value::Null),
        [Value::Float64(value)] => *value,
        [Value::Int32(value)] => *value as f64,
        [Value::Int64(value)] => *value as f64,
        [Value::Interval(value)] if value.is_finite() => value.cmp_key() as f64 / 1_000_000.0,
        [value] if value.as_text().is_some() => {
            let interval = parse_interval_text_value(value.as_text().unwrap())?;
            interval.cmp_key() as f64 / 1_000_000.0
        }
        [other] => {
            return Err(ExecError::TypeMismatch {
                op: "pg_sleep",
                left: other.clone(),
                right: Value::Null,
            });
        }
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "pg_sleep",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            });
        }
    };
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "non-negative finite sleep duration",
            actual: seconds.to_string(),
        }));
    }
    std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
    Ok(Value::Null)
}

fn eval_timezone_function(
    values: &[Value],
    config: &crate::backend::utils::misc::guc_datetime::DateTimeConfig,
) -> Result<Value, ExecError> {
    let (zone, value) = match values {
        [value] => (
            Value::Text(
                crate::backend::utils::time::datetime::current_timezone_name(config).into(),
            ),
            value,
        ),
        [zone, value] => {
            if matches!(zone, Value::Null) {
                return Ok(Value::Null);
            }
            let zone = if zone.as_text() == Some("__pgrust_local_timezone__") {
                Value::Text(
                    crate::backend::utils::time::datetime::current_timezone_name(config).into(),
                )
            } else {
                zone.clone()
            };
            (zone, value)
        }
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "timezone",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            });
        }
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match value {
        Value::TimeTz(_) => {
            let mut timetz_args = Vec::new();
            if values.len() == 2 {
                timetz_args.push(zone.clone());
            }
            timetz_args.push(value.clone());
            eval_timetz_timezone_function(&timetz_args, config)
        }
        Value::Time(time) => Ok(Value::TimeTz(TimeTzADT {
            time: *time,
            offset_seconds: timezone_target_offset_seconds(&zone, config)?,
        })),
        Value::Timestamp(timestamp) => match &zone {
            Value::Interval(interval) => {
                let micros = timezone_interval_seconds(*interval)?
                    .checked_mul(USECS_PER_SEC)
                    .ok_or_else(timezone_timestamp_out_of_range)?;
                if !timestamp.is_finite() {
                    Ok(Value::TimestampTz(TimestampTzADT(timestamp.0)))
                } else {
                    Ok(Value::TimestampTz(TimestampTzADT(
                        timestamp
                            .0
                            .checked_sub(micros)
                            .ok_or_else(timezone_timestamp_out_of_range)?,
                    )))
                }
            }
            _ => {
                let zone_text = zone.as_text().ok_or_else(|| ExecError::TypeMismatch {
                    op: "timezone",
                    left: zone.clone(),
                    right: value.clone(),
                })?;
                timestamp_at_time_zone(*timestamp, zone_text)
                    .map(Value::TimestampTz)
                    .map_err(|err| ExecError::InvalidStorageValue {
                        column: "timestamptz".into(),
                        details: super::expr_casts::datetime_parse_error_details(
                            "timestamp with time zone",
                            zone_text,
                            err,
                        ),
                    })
            }
        },
        Value::TimestampTz(timestamptz) => match &zone {
            Value::Interval(interval) => {
                let micros = timezone_interval_seconds(*interval)?
                    .checked_mul(USECS_PER_SEC)
                    .ok_or_else(timezone_timestamp_out_of_range)?;
                if !timestamptz.is_finite() {
                    Ok(Value::Timestamp(TimestampADT(timestamptz.0)))
                } else {
                    Ok(Value::Timestamp(TimestampADT(
                        timestamptz
                            .0
                            .checked_add(micros)
                            .ok_or_else(timezone_timestamp_out_of_range)?,
                    )))
                }
            }
            _ => {
                let zone_text = zone.as_text().ok_or_else(|| ExecError::TypeMismatch {
                    op: "timezone",
                    left: zone.clone(),
                    right: value.clone(),
                })?;
                timestamptz_at_time_zone(*timestamptz, zone_text)
                    .map(Value::Timestamp)
                    .map_err(|err| ExecError::InvalidStorageValue {
                        column: "timestamp".into(),
                        details: super::expr_casts::datetime_parse_error_details(
                            "timestamp",
                            zone_text,
                            err,
                        ),
                    })
            }
        },
        other => Err(ExecError::TypeMismatch {
            op: "timezone",
            left: zone,
            right: other.clone(),
        }),
    }
}

fn timezone_timestamp_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "timestamp out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn is_text_search_builtin_function(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::ToTsVector
            | BuiltinScalarFunction::JsonbToTsVector
            | BuiltinScalarFunction::ToTsQuery
            | BuiltinScalarFunction::PlainToTsQuery
            | BuiltinScalarFunction::PhraseToTsQuery
            | BuiltinScalarFunction::WebSearchToTsQuery
            | BuiltinScalarFunction::TsLexize
            | BuiltinScalarFunction::TsHeadline
            | BuiltinScalarFunction::TsMatch
            | BuiltinScalarFunction::TsQueryPhrase
            | BuiltinScalarFunction::TsQueryNumnode
            | BuiltinScalarFunction::TsRewrite
            | BuiltinScalarFunction::TsVectorIn
            | BuiltinScalarFunction::TsVectorOut
            | BuiltinScalarFunction::TsQueryIn
            | BuiltinScalarFunction::TsQueryOut
            | BuiltinScalarFunction::TsVectorStrip
            | BuiltinScalarFunction::TsVectorDelete
            | BuiltinScalarFunction::TsVectorToArray
            | BuiltinScalarFunction::ArrayToTsVector
            | BuiltinScalarFunction::TsVectorSetWeight
            | BuiltinScalarFunction::TsVectorFilter
            | BuiltinScalarFunction::TsRank
            | BuiltinScalarFunction::TsRankCd
    )
}

fn eval_text_search_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: Option<&ExecutorContext>,
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

    fn arg_tsvector<'a>(
        values: &'a [Value],
        index: usize,
        op: &'static str,
    ) -> Result<&'a crate::include::nodes::tsearch::TsVector, ExecError> {
        match values.get(index) {
            Some(Value::TsVector(vector)) => Ok(vector),
            Some(other) => Err(ExecError::TypeMismatch {
                op,
                left: other.clone(),
                right: Value::Null,
            }),
            None => Err(ExecError::TypeMismatch {
                op,
                left: Value::Null,
                right: Value::Null,
            }),
        }
    }

    fn arg_tsquery<'a>(
        values: &'a [Value],
        index: usize,
        op: &'static str,
    ) -> Result<&'a crate::include::nodes::tsearch::TsQuery, ExecError> {
        match values.get(index) {
            Some(Value::TsQuery(query)) => Ok(query),
            Some(other) => Err(ExecError::TypeMismatch {
                op,
                left: other.clone(),
                right: Value::Null,
            }),
            None => Err(ExecError::TypeMismatch {
                op,
                left: Value::Null,
                right: Value::Null,
            }),
        }
    }

    fn phrase_distance(value: Option<&Value>) -> Result<u16, ExecError> {
        let distance = match value {
            Some(value) => int32_arg(value, "tsquery_phrase")?,
            None => 1,
        };
        if !(0..=16_384).contains(&distance) {
            return Err(ExecError::DetailedError {
                message: "distance in phrase operator must be an integer value between zero and 16384 inclusive".into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        Ok(distance as u16)
    }

    fn rank_weights(value: &Value, op: &'static str) -> Result<[f64; 4], ExecError> {
        let items = match value {
            Value::Array(items) => items.clone(),
            Value::PgArray(array) => array.to_nested_values(),
            other => {
                return Err(ExecError::TypeMismatch {
                    op,
                    left: other.clone(),
                    right: Value::Null,
                });
            }
        };
        if items.len() != 4 {
            return Err(ExecError::DetailedError {
                message: "array of weight is too short".into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        Ok([
            expect_float8_arg(op, &items[0])?,
            expect_float8_arg(op, &items[1])?,
            expect_float8_arg(op, &items[2])?,
            expect_float8_arg(op, &items[3])?,
        ])
    }

    fn eval_rank_builtin(
        func: BuiltinScalarFunction,
        values: &[Value],
    ) -> Result<Value, ExecError> {
        let op = if matches!(func, BuiltinScalarFunction::TsRank) {
            "ts_rank"
        } else {
            "ts_rank_cd"
        };
        let mut offset = 0usize;
        let mut weights = None;
        if (values.len() == 3
            && matches!(values.first(), Some(Value::Array(_) | Value::PgArray(_))))
            || values.len() == 4
        {
            weights = Some(rank_weights(&values[0], op)?);
            offset = 1;
        }
        let vector = arg_tsvector(values, offset, op)?;
        let query = arg_tsquery(values, offset + 1, op)?;
        let normalization = if values.len() > offset + 2 {
            int32_arg(&values[offset + 2], op)?
        } else {
            0
        };
        let score = if matches!(func, BuiltinScalarFunction::TsRank) {
            crate::backend::executor::ts_rank(vector, query, weights, normalization)
        } else {
            crate::backend::executor::ts_rank_cd(vector, query, weights, normalization)
        };
        Ok(Value::Float64(score))
    }

    let parse_error = |op: &'static str, message: String| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid text search input",
            actual: format!("{op}: {message}"),
        })
    };
    let default_config_name = || {
        ctx.and_then(|ctx| {
            ctx.gucs
                .get("default_text_search_config")
                .map(String::as_str)
        })
    };
    let catalog = catalog_lookup(ctx);

    match func {
        BuiltinScalarFunction::TsMatch => {
            eval_ts_match_values(values, default_config_name(), catalog)
        }
        BuiltinScalarFunction::TsHeadline => eval_ts_headline_values(values),
        BuiltinScalarFunction::ToTsVector => {
            if let [Value::Jsonb(_)] = values {
                return jsonb_to_tsvector_value(default_config_name(), &values[0], None, catalog);
            }
            if let [_, Value::Jsonb(_)] = values {
                return jsonb_to_tsvector_value(
                    arg_text(values, 0, "to_tsvector")?.as_deref(),
                    &values[1],
                    None,
                    catalog,
                );
            }
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::to_tsvector_with_config_name(
                    default_config_name(),
                    arg_text(values, 0, "to_tsvector")?
                        .as_deref()
                        .unwrap_or_default(),
                    catalog,
                ),
                [_, _] => crate::backend::tsearch::to_tsvector_with_config_name(
                    arg_text(values, 0, "to_tsvector")?.as_deref(),
                    arg_text(values, 1, "to_tsvector")?
                        .as_deref()
                        .unwrap_or_default(),
                    catalog,
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
            [Value::Jsonb(_), _] => {
                jsonb_to_tsvector_value(default_config_name(), &values[0], values.get(1), catalog)
            }
            [_, Value::Jsonb(_), _] => jsonb_to_tsvector_value(
                arg_text(values, 0, "jsonb_to_tsvector")?.as_deref(),
                &values[1],
                values.get(2),
                catalog,
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "jsonb_to_tsvector",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::ToTsQuery => {
            let (config_name, query_text) = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => (
                    default_config_name().map(str::to_string),
                    arg_text(values, 0, "to_tsquery")?,
                ),
                [_, _] => (
                    arg_text(values, 0, "to_tsquery")?,
                    arg_text(values, 1, "to_tsquery")?,
                ),
                _ => unreachable!(),
            };
            let query_text = query_text.unwrap_or_default();
            let query = crate::backend::tsearch::to_tsquery_with_config_name(
                config_name.as_deref(),
                &query_text,
                catalog,
            )
            .map_err(|e| parse_error("to_tsquery", e))?;
            if tsquery_is_empty(&query) {
                if query_text.trim().is_empty() {
                    crate::backend::utils::misc::notices::push_notice(format!(
                        "text-search query doesn't contain lexemes: \"{query_text}\""
                    ));
                } else {
                    crate::backend::utils::misc::notices::push_notice(
                        "text-search query contains only stop words or doesn't contain lexemes, ignored",
                    );
                }
            }
            Ok(Value::TsQuery(query))
        }
        BuiltinScalarFunction::PlainToTsQuery => {
            let (config_name, query_text) = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => (
                    default_config_name().map(str::to_string),
                    arg_text(values, 0, "plainto_tsquery")?,
                ),
                [_, _] => (
                    arg_text(values, 0, "plainto_tsquery")?,
                    arg_text(values, 1, "plainto_tsquery")?,
                ),
                _ => unreachable!(),
            };
            let query = crate::backend::tsearch::plainto_tsquery_with_config_name(
                config_name.as_deref(),
                query_text.as_deref().unwrap_or_default(),
                catalog,
            )
            .map_err(|e| parse_error("plainto_tsquery", e))?;
            if tsquery_is_empty(&query) {
                crate::backend::utils::misc::notices::push_notice(
                    "text-search query contains only stop words or doesn't contain lexemes, ignored",
                );
            }
            Ok(Value::TsQuery(query))
        }
        BuiltinScalarFunction::PhraseToTsQuery => {
            let (config_name, query_text) = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => (
                    default_config_name().map(str::to_string),
                    arg_text(values, 0, "phraseto_tsquery")?,
                ),
                [_, _] => (
                    arg_text(values, 0, "phraseto_tsquery")?,
                    arg_text(values, 1, "phraseto_tsquery")?,
                ),
                _ => unreachable!(),
            };
            let query = crate::backend::tsearch::phraseto_tsquery_with_config_name(
                config_name.as_deref(),
                query_text.as_deref().unwrap_or_default(),
                catalog,
            )
            .map_err(|e| parse_error("phraseto_tsquery", e))?;
            if tsquery_is_empty(&query) {
                crate::backend::utils::misc::notices::push_notice(
                    "text-search query contains only stop words or doesn't contain lexemes, ignored",
                );
            }
            Ok(Value::TsQuery(query))
        }
        BuiltinScalarFunction::WebSearchToTsQuery => {
            let (config_name, query_text) = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => (
                    default_config_name().map(str::to_string),
                    arg_text(values, 0, "websearch_to_tsquery")?,
                ),
                [_, _] => (
                    arg_text(values, 0, "websearch_to_tsquery")?,
                    arg_text(values, 1, "websearch_to_tsquery")?,
                ),
                _ => unreachable!(),
            };
            let query = crate::backend::tsearch::websearch_to_tsquery_with_config_name(
                config_name.as_deref(),
                query_text.as_deref().unwrap_or_default(),
                catalog,
            )
            .map_err(|e| parse_error("websearch_to_tsquery", e))?;
            if tsquery_is_empty(&query) {
                crate::backend::utils::misc::notices::push_notice(
                    "text-search query contains only stop words or doesn't contain lexemes, ignored",
                );
            }
            Ok(Value::TsQuery(query))
        }
        BuiltinScalarFunction::TsVectorIn => match values {
            [Value::Null] => Ok(Value::Null),
            [_] | [_, _, _] => {
                // :HACK: pgrust represents SQL cstring arguments through the
                // existing text value path for type input wrappers.
                let text = arg_text(values, 0, "tsvectorin")?.unwrap_or_default();
                crate::backend::executor::parse_tsvector_text(&text).map(Value::TsVector)
            }
            _ => unreachable!(),
        },
        BuiltinScalarFunction::TsVectorOut => match values {
            [Value::Null] => Ok(Value::Null),
            [Value::TsVector(vector)] => Ok(Value::Text(
                crate::backend::executor::render_tsvector_text(vector).into(),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "tsvectorout",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::TsQueryIn => match values {
            [Value::Null] => Ok(Value::Null),
            [_] | [_, _, _] => {
                // :HACK: pgrust represents SQL cstring arguments through the
                // existing text value path for type input wrappers.
                let text = arg_text(values, 0, "tsqueryin")?.unwrap_or_default();
                crate::backend::executor::parse_tsquery_text(&text).map(Value::TsQuery)
            }
            _ => unreachable!(),
        },
        BuiltinScalarFunction::TsQueryOut => match values {
            [Value::Null] => Ok(Value::Null),
            [Value::TsQuery(query)] => Ok(Value::Text(
                crate::backend::executor::render_tsquery_text(query).into(),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "tsqueryout",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::TsQueryPhrase => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            Ok(Value::TsQuery(crate::backend::executor::tsquery_phrase(
                arg_tsquery(values, 0, "tsquery_phrase")?.clone(),
                arg_tsquery(values, 1, "tsquery_phrase")?.clone(),
                phrase_distance(values.get(2))?,
            )))
        }
        BuiltinScalarFunction::TsQueryNumnode => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            Ok(Value::Int32(crate::backend::executor::numnode(
                arg_tsquery(values, 0, "numnode")?,
            )))
        }
        BuiltinScalarFunction::TsRewrite => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            Ok(Value::TsQuery(crate::backend::executor::tsquery_rewrite(
                arg_tsquery(values, 0, "ts_rewrite")?.clone(),
                arg_tsquery(values, 1, "ts_rewrite")?.clone(),
                arg_tsquery(values, 2, "ts_rewrite")?.clone(),
            )))
        }
        BuiltinScalarFunction::TsVectorStrip => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            Ok(Value::TsVector(crate::backend::executor::strip_tsvector(
                arg_tsvector(values, 0, "strip")?,
            )))
        }
        BuiltinScalarFunction::TsVectorDelete => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            let lexemes = match values.get(1) {
                Some(value @ (Value::Array(_) | Value::PgArray(_))) => {
                    crate::backend::executor::text_array_items(value, "ts_delete")?
                        .into_iter()
                        .flatten()
                        .filter(|text: &String| !text.is_empty())
                        .collect::<Vec<_>>()
                }
                Some(value) if value.as_text().is_some() => {
                    vec![value.as_text().unwrap().to_string()]
                }
                Some(other) => {
                    return Err(ExecError::TypeMismatch {
                        op: "ts_delete",
                        left: other.clone(),
                        right: Value::Null,
                    });
                }
                None => unreachable!(),
            };
            Ok(Value::TsVector(
                crate::backend::executor::delete_tsvector_lexemes(
                    arg_tsvector(values, 0, "ts_delete")?,
                    &lexemes,
                ),
            ))
        }
        BuiltinScalarFunction::TsVectorToArray => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            Ok(crate::backend::executor::tsvector_to_array(arg_tsvector(
                values,
                0,
                "tsvector_to_array",
            )?))
        }
        BuiltinScalarFunction::ArrayToTsVector => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            Ok(Value::TsVector(
                crate::backend::executor::array_to_tsvector(
                    values.first().unwrap_or(&Value::Null),
                )?,
            ))
        }
        BuiltinScalarFunction::TsVectorSetWeight => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            let weight = crate::backend::executor::parse_ts_weight(
                values.get(1).unwrap_or(&Value::Null),
                "setweight",
            )?;
            Ok(Value::TsVector(
                crate::backend::executor::setweight_tsvector(
                    arg_tsvector(values, 0, "setweight")?,
                    weight,
                    values.get(2),
                )?,
            ))
        }
        BuiltinScalarFunction::TsVectorFilter => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            Ok(Value::TsVector(crate::backend::executor::filter_tsvector(
                arg_tsvector(values, 0, "ts_filter")?,
                values.get(1).unwrap_or(&Value::Null),
            )?))
        }
        BuiltinScalarFunction::TsRank | BuiltinScalarFunction::TsRankCd => {
            if values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(Value::Null);
            }
            eval_rank_builtin(func, values)
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
                catalog,
            )
            .map(|lexemes| {
                lexemes
                    .map(|lexemes| {
                        Value::Array(
                            lexemes
                                .into_iter()
                                .map(|lexeme| Value::Text(lexeme.into()))
                                .collect(),
                        )
                    })
                    .unwrap_or(Value::Null)
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

fn eval_domain_check_upper_less_than(values: &[Value]) -> Result<Value, ExecError> {
    let [value, Value::Text(domain_name), Value::Int32(limit)] = values else {
        return Err(malformed_expr_error("domain_check"));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let upper = match value {
        Value::Range(range) => range
            .upper
            .as_ref()
            .map(|bound| bound.value.to_owned_value())
            .unwrap_or(Value::Null),
        Value::Multirange(multirange) => multirange
            .ranges
            .last()
            .and_then(|range| range.upper.as_ref())
            .map(|bound| bound.value.to_owned_value())
            .unwrap_or(Value::Null),
        _ => return Ok(value.clone()),
    };
    let passes = match upper {
        Value::Null => true,
        Value::Int16(v) => i32::from(v) < *limit,
        Value::Int32(v) => v < *limit,
        Value::Int64(v) => v < i64::from(*limit),
        _ => true,
    };
    if passes {
        return Ok(value.clone());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "value for domain {domain_name} violates check constraint \"{domain_name}_check\""
        ),
        detail: None,
        hint: None,
        sqlstate: "23514",
    })
}

pub(crate) fn eval_native_builtin_scalar_value_call(
    func: BuiltinScalarFunction,
    values: &[Value],
    func_variadic: bool,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match func {
        BuiltinScalarFunction::PgColumnToastChunkId => eval_pg_column_toast_chunk_id_values(values),
        BuiltinScalarFunction::NumNulls => Ok(eval_num_nulls(values, func_variadic, true)),
        BuiltinScalarFunction::NumNonNulls => Ok(eval_num_nulls(values, func_variadic, false)),
        BuiltinScalarFunction::PgLogBackendMemoryContexts => {
            eval_pg_log_backend_memory_contexts(values)
        }
        BuiltinScalarFunction::HasFunctionPrivilege => eval_has_function_privilege(values, ctx),
        BuiltinScalarFunction::HasTablePrivilege => {
            eval_has_relation_privilege(PrivilegeRelationKind::Table, values, ctx)
        }
        BuiltinScalarFunction::HasSequencePrivilege => {
            eval_has_relation_privilege(PrivilegeRelationKind::Sequence, values, ctx)
        }
        BuiltinScalarFunction::HasAnyColumnPrivilege => eval_has_any_column_privilege(values, ctx),
        BuiltinScalarFunction::HasColumnPrivilege => eval_has_column_privilege(values, ctx),
        BuiltinScalarFunction::HasLargeObjectPrivilege => {
            eval_has_largeobject_privilege(values, ctx)
        }
        BuiltinScalarFunction::PgHasRole => eval_pg_has_role(values, ctx),
        BuiltinScalarFunction::PgCurrentLogfile => eval_pg_current_logfile(values),
        BuiltinScalarFunction::PgReadFile => eval_pg_read_file(values, ctx, false),
        BuiltinScalarFunction::PgReadBinaryFile => eval_pg_read_file(values, ctx, true),
        BuiltinScalarFunction::PgStatFile => eval_pg_stat_file(values, ctx),
        BuiltinScalarFunction::PgWalfileName => eval_pg_walfile_name(values),
        BuiltinScalarFunction::PgWalfileNameOffset => eval_pg_walfile_name_offset(values),
        BuiltinScalarFunction::PgSplitWalfileName => eval_pg_split_walfile_name(values),
        BuiltinScalarFunction::PgControlSystem
        | BuiltinScalarFunction::PgControlCheckpoint
        | BuiltinScalarFunction::PgControlRecovery
        | BuiltinScalarFunction::PgControlInit => Ok(eval_pg_control_record(func, ctx)),
        BuiltinScalarFunction::PgReplicationOriginCreate => {
            eval_pg_replication_origin_create(values)
        }
        BuiltinScalarFunction::PgDescribeObject => eval_pg_describe_object(values, ctx),
        BuiltinScalarFunction::PgIdentifyObject => eval_pg_identify_object(values, ctx),
        BuiltinScalarFunction::PgIdentifyObjectAsAddress => {
            eval_pg_identify_object_as_address(values, ctx)
        }
        BuiltinScalarFunction::PgGetObjectAddress => eval_pg_get_object_address(values, ctx),
        BuiltinScalarFunction::PgStatGetBackendWal => {
            Ok(pg_stat_get_backend_wal_value(values, ctx))
        }
        BuiltinScalarFunction::GistTranslateCmpTypeCommon => {
            eval_gist_translate_cmptype_common(values)
        }
        BuiltinScalarFunction::TestCanonicalizePath => eval_test_canonicalize_path(values),
        BuiltinScalarFunction::TestRelpath => Ok(Value::Bool(false)),
        _ => execute_builtin_scalar_function_value_call(func, values),
    }
}

pub(crate) fn eval_builtin_function(
    func: BuiltinScalarFunction,
    result_type: Option<SqlType>,
    args: &[Expr],
    func_variadic: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    ensure_builtin_side_effects_allowed(func, ctx)?;
    if matches!(func, BuiltinScalarFunction::SatisfiesHashPartition) {
        return eval_satisfies_hash_partition(args, func_variadic, slot, ctx);
    }
    if let Some(result) = eval_json_record_builtin_function(func, result_type, args, slot, ctx) {
        return result;
    }
    if matches!(
        func,
        BuiltinScalarFunction::PgRestoreRelationStats
            | BuiltinScalarFunction::PgClearRelationStats
            | BuiltinScalarFunction::PgRestoreAttributeStats
            | BuiltinScalarFunction::PgClearAttributeStats
    ) {
        let runtime = ctx
            .stats_import_runtime
            .clone()
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("{func:?} requires database executor context"),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            })?;
        let typed_args = args
            .iter()
            .map(|arg| {
                Ok(TypedFunctionArg {
                    value: eval_expr(arg, slot, ctx)?,
                    sql_type: expr_sql_type_hint(arg),
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        return match func {
            BuiltinScalarFunction::PgRestoreRelationStats => {
                runtime.pg_restore_relation_stats(ctx, typed_args)
            }
            BuiltinScalarFunction::PgClearRelationStats => {
                if typed_args.len() != 2 {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "pg_clear_relation_stats(schemaname, relname)",
                        actual: format!("{} args", typed_args.len()),
                    }));
                }
                runtime.pg_clear_relation_stats(
                    ctx,
                    typed_args[0].value.clone(),
                    typed_args[1].value.clone(),
                )
            }
            BuiltinScalarFunction::PgRestoreAttributeStats => {
                runtime.pg_restore_attribute_stats(ctx, typed_args)
            }
            BuiltinScalarFunction::PgClearAttributeStats => {
                if typed_args.len() != 4 {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "pg_clear_attribute_stats(schemaname, relname, attname, inherited)",
                        actual: format!("{} args", typed_args.len()),
                    }));
                }
                runtime.pg_clear_attribute_stats(
                    ctx,
                    typed_args[0].value.clone(),
                    typed_args[1].value.clone(),
                    typed_args[2].value.clone(),
                    typed_args[3].value.clone(),
                )
            }
            _ => unreachable!(),
        };
    }
    if matches!(
        func,
        BuiltinScalarFunction::PgColumnCompression | BuiltinScalarFunction::PgColumnToastChunkId
    ) && let [Expr::Var(var)] = args
        && var.varlevelsup == 0
        && (var.varno == OUTER_VAR || !is_executor_special_varno(var.varno))
        && var.varattno > 0
        && let Some(index) = attrno_index(var.varattno)
        && let Some(raw) = current_slot_raw_attr_bytes(slot, index)?
    {
        return match func {
            BuiltinScalarFunction::PgColumnCompression => eval_pg_column_compression_raw(raw),
            BuiltinScalarFunction::PgColumnToastChunkId => eval_pg_column_toast_chunk_id_raw(raw),
            _ => unreachable!(),
        };
    }
    if matches!(func, BuiltinScalarFunction::PgTypeof) {
        let ty = args
            .first()
            .and_then(expr_sql_type_hint)
            .unwrap_or(SqlType::new(SqlTypeKind::Text));
        return Ok(Value::Text(sql_type_name(ty).into()));
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
    if matches!(func, BuiltinScalarFunction::PgRustDomainCheckUpperLessThan) {
        return eval_domain_check_upper_less_than(&values);
    }
    if let Some(result) = eval_geometry_function(func, &values) {
        return result;
    }
    if let Some(result) = eval_macaddr_function(func, &values) {
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
    if let Some(result) = eval_enum_function(func, &values, result_type, ctx) {
        return result;
    }
    if let Some(result) = eval_json_builtin_function(
        func,
        &values,
        result_type,
        func_variadic,
        &ctx.datetime_config,
        ctx.catalog.as_deref(),
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
    if is_text_search_builtin_function(func) {
        return eval_text_search_builtin_function(func, &values, Some(ctx));
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
        | BuiltinScalarFunction::TsLexize => {
            eval_text_search_builtin_function(func, &values, Some(ctx))
        }
        BuiltinScalarFunction::Random => eval_random_function(&values, ctx),
        BuiltinScalarFunction::RandomNormal => eval_random_normal_function(&values, ctx),
        BuiltinScalarFunction::SetSeed => eval_setseed_function(&values, ctx),
        BuiltinScalarFunction::TxidCurrent
        | BuiltinScalarFunction::TxidCurrentIfAssigned
        | BuiltinScalarFunction::TxidCurrentSnapshot
        | BuiltinScalarFunction::TxidSnapshotXmin
        | BuiltinScalarFunction::TxidSnapshotXmax
        | BuiltinScalarFunction::TxidVisibleInSnapshot
        | BuiltinScalarFunction::TxidStatus => eval_txid_builtin_function(func, &values, ctx),
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
        BuiltinScalarFunction::Xid8Cmp => match values.as_slice() {
            [Value::Xid8(left), Value::Xid8(right)] => Ok(Value::Int32(match left.cmp(right) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            })),
            [left, right] if left.as_text().is_some() && right.as_text().is_some() => {
                let left = cast_value_with_source_type_catalog_and_config(
                    left.clone(),
                    Some(SqlType::new(SqlTypeKind::Text)),
                    SqlType::new(SqlTypeKind::Int8)
                        .with_identity(crate::include::catalog::XID8_TYPE_OID, 0),
                    catalog_lookup(Some(ctx)),
                    &ctx.datetime_config,
                )?;
                let right = cast_value_with_source_type_catalog_and_config(
                    right.clone(),
                    Some(SqlType::new(SqlTypeKind::Text)),
                    SqlType::new(SqlTypeKind::Int8)
                        .with_identity(crate::include::catalog::XID8_TYPE_OID, 0),
                    catalog_lookup(Some(ctx)),
                    &ctx.datetime_config,
                )?;
                match (left, right) {
                    (Value::Xid8(left), Value::Xid8(right)) => {
                        Ok(Value::Int32(match left.cmp(&right) {
                            std::cmp::Ordering::Less => -1,
                            std::cmp::Ordering::Equal => 0,
                            std::cmp::Ordering::Greater => 1,
                        }))
                    }
                    (left, right) => Err(ExecError::TypeMismatch {
                        op: "xid8cmp",
                        left,
                        right,
                    }),
                }
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [left, right] => Err(ExecError::TypeMismatch {
                op: "xid8cmp",
                left: left.clone(),
                right: right.clone(),
            }),
            _ => Err(malformed_expr_error("xid8cmp")),
        },
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
        | BuiltinScalarFunction::Int4Mi
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
        BuiltinScalarFunction::MakeAclItem => eval_make_acl_item(&values),
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
        BuiltinScalarFunction::PgFunctionIsVisible => eval_pg_function_is_visible(&values, ctx),
        BuiltinScalarFunction::PgTypeIsVisible => eval_pg_type_is_visible(&values, ctx),
        BuiltinScalarFunction::PgOperatorIsVisible => eval_pg_operator_is_visible(&values, ctx),
        BuiltinScalarFunction::PgOpclassIsVisible => eval_pg_opclass_is_visible(&values, ctx),
        BuiltinScalarFunction::PgOpfamilyIsVisible => eval_pg_opfamily_is_visible(&values, ctx),
        BuiltinScalarFunction::PgConversionIsVisible => eval_pg_conversion_is_visible(&values, ctx),
        BuiltinScalarFunction::PgTsParserIsVisible => eval_pg_ts_parser_is_visible(&values, ctx),
        BuiltinScalarFunction::PgTsDictIsVisible => eval_pg_ts_dict_is_visible(&values, ctx),
        BuiltinScalarFunction::PgTsTemplateIsVisible => {
            eval_pg_ts_template_is_visible(&values, ctx)
        }
        BuiltinScalarFunction::PgTsConfigIsVisible => eval_pg_ts_config_is_visible(&values, ctx),
        BuiltinScalarFunction::Now | BuiltinScalarFunction::TransactionTimestamp => {
            let mut config = ctx.datetime_config.clone();
            config
                .transaction_timestamp_usecs
                .get_or_insert(ctx.statement_timestamp_usecs);
            Ok(current_timestamp_value_with_config(&config, None, true))
        }
        BuiltinScalarFunction::StatementTimestamp => {
            let mut config = ctx.datetime_config.clone();
            config.transaction_timestamp_usecs = Some(
                config
                    .statement_timestamp_usecs
                    .unwrap_or(ctx.statement_timestamp_usecs),
            );
            Ok(current_timestamp_value_with_config(&config, None, true))
        }
        BuiltinScalarFunction::ClockTimestamp => {
            let mut config = ctx.datetime_config.clone();
            config.transaction_timestamp_usecs = None;
            Ok(current_timestamp_value_with_config(&config, None, true))
        }
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
        BuiltinScalarFunction::DatePart => {
            eval_date_part_function_with_config(&values, &ctx.datetime_config)
        }
        BuiltinScalarFunction::Extract => {
            eval_extract_function_with_config(&values, &ctx.datetime_config)
        }
        BuiltinScalarFunction::DateTrunc => eval_date_trunc_function(&values, &ctx.datetime_config),
        BuiltinScalarFunction::DateBin => eval_date_bin_function(&values),
        BuiltinScalarFunction::Timezone => eval_timezone_function(&values, &ctx.datetime_config),
        BuiltinScalarFunction::DateAdd => eval_datetime_add_function(&values, false),
        BuiltinScalarFunction::DateSubtract => eval_datetime_add_function(&values, true),
        BuiltinScalarFunction::Age => eval_age_function(&values, &ctx.datetime_config),
        BuiltinScalarFunction::JustifyDays => eval_justify_days_function(&values),
        BuiltinScalarFunction::JustifyHours => eval_justify_hours_function(&values),
        BuiltinScalarFunction::JustifyInterval => eval_justify_interval_function(&values),
        BuiltinScalarFunction::IsFinite => eval_isfinite_function(&values),
        BuiltinScalarFunction::MakeInterval => eval_make_interval_function(&values),
        BuiltinScalarFunction::MakeDate => eval_make_date_function(&values),
        BuiltinScalarFunction::MakeTime => eval_make_time_function(&values),
        BuiltinScalarFunction::MakeTimestamp => eval_make_timestamp_function(&values),
        BuiltinScalarFunction::MakeTimestampTz => {
            eval_make_timestamptz_function(&values, &ctx.datetime_config)
        }
        BuiltinScalarFunction::TimestampTzConstructor => {
            eval_timestamptz_constructor_function(&values, &ctx.datetime_config)
        }
        BuiltinScalarFunction::ToTimestamp => {
            eval_to_timestamp_function(&values, &ctx.datetime_config)
        }
        BuiltinScalarFunction::IntervalHash => {
            eval_hash_builtin_function(HashFunctionKind::Interval, false, &values)
        }
        BuiltinScalarFunction::HashValue(kind) => eval_hash_builtin_function(kind, false, &values),
        BuiltinScalarFunction::HashValueExtended(kind) => {
            eval_hash_builtin_function(kind, true, &values)
        }
        BuiltinScalarFunction::GetDatabaseEncoding => Ok(Value::Text("UTF8".into())),
        BuiltinScalarFunction::UnicodeVersion => eval_unicode_version_function(&values),
        BuiltinScalarFunction::UnicodeAssigned => eval_unicode_assigned_function(&values),
        BuiltinScalarFunction::Normalize => eval_unicode_normalize_function(&values),
        BuiltinScalarFunction::IsNormalized => eval_unicode_is_normalized_function(&values),
        BuiltinScalarFunction::PgEncodingToChar => eval_pg_encoding_to_char(&values),
        BuiltinScalarFunction::PgMyTempSchema => Ok(Value::Int64(i64::from(
            current_temp_namespace_oid(ctx).unwrap_or(0),
        ))),
        BuiltinScalarFunction::PgRustInternalBinaryCoercible => {
            eval_pg_rust_internal_binary_coercible(&values)
        }
        BuiltinScalarFunction::PgRustDomainCheckUpperLessThan => {
            unreachable!("domain check handled earlier")
        }
        BuiltinScalarFunction::PgRustTestOpclassOptionsFunc => {
            eval_pg_rust_test_opclass_options_func(&values)
        }
        BuiltinScalarFunction::PgRustTestFdwHandler => eval_pg_rust_test_fdw_handler(&values),
        BuiltinScalarFunction::PgRustTestEncSetup => eval_pg_rust_test_enc_setup(&values),
        BuiltinScalarFunction::PgRustTestEncConversion => eval_pg_rust_test_enc_conversion(&values),
        BuiltinScalarFunction::PgRustTestWidgetIn => eval_pg_rust_test_widget_in(&values),
        BuiltinScalarFunction::PgRustTestWidgetOut => eval_pg_rust_test_widget_out(&values),
        BuiltinScalarFunction::PgRustTestInt44In => eval_pg_rust_test_int44in(&values),
        BuiltinScalarFunction::PgRustTestInt44Out => eval_pg_rust_test_int44out(&values),
        BuiltinScalarFunction::PgRustTestPtInWidget => eval_pg_rust_test_pt_in_widget(&values),
        BuiltinScalarFunction::PgRustIsCatalogTextUniqueIndexOid => {
            eval_pg_rust_is_catalog_text_unique_index_oid(&values)
        }
        BuiltinScalarFunction::AmValidate | BuiltinScalarFunction::BtEqualImage => {
            Ok(Value::Bool(true))
        }
        BuiltinScalarFunction::CurrentSetting => eval_current_setting(&values, ctx),
        BuiltinScalarFunction::PgSettingsGetFlags => eval_pg_settings_get_flags(&values),
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
            ctx.stats.write().record_database_session_start();
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatGetSnapshotTimestamp => Ok(ctx
            .session_stats
            .read()
            .snapshot_timestamp()
            .map(Value::TimestampTz)
            .unwrap_or(Value::Null)),
        BuiltinScalarFunction::PgStatClearSnapshot => {
            ctx.session_stats.write().force_clear_snapshot();
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatGetBackendPid => {
            let beid = values.first().and_then(|value| match value {
                Value::Int32(value) => Some(*value),
                Value::Int64(value) => i32::try_from(*value).ok(),
                _ => None,
            });
            let current_beid = ctx
                .database
                .as_ref()
                .map(|db| db.temp_backend_id(ctx.client_id) as i32)
                .unwrap_or(ctx.client_id as i32);
            Ok(Value::Int32(
                (beid == Some(current_beid))
                    .then_some(ctx.client_id as i32)
                    .unwrap_or(0),
            ))
        }
        BuiltinScalarFunction::PgStatGetBackendWal => {
            Ok(pg_stat_get_backend_wal_value(&values, ctx))
        }
        BuiltinScalarFunction::PgStatReset => {
            ctx.stats.write().reset_database();
            ctx.session_stats.write().pending_flush.clear();
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatResetShared => {
            let target =
                values
                    .first()
                    .and_then(Value::as_text)
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op: "pg_stat_reset_shared",
                        left: values.first().cloned().unwrap_or(Value::Null),
                        right: Value::Text("".into()),
                    })?;
            if let Err(target) = ctx.stats.write().reset_shared(target) {
                return Err(ExecError::DetailedError {
                    message: format!("unrecognized reset target: \"{target}\""),
                    detail: None,
                    hint: Some(
                        "Target must be \"archiver\", \"bgwriter\", \"checkpointer\", \"io\", \"recovery_prefetch\", \"slru\", or \"wal\"."
                            .into(),
                    ),
                    sqlstate: "22023",
                });
            }
            ctx.session_stats.write().force_clear_snapshot();
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatResetSingleTableCounters => {
            let oid = stats_oid_arg(&values, "pg_stat_reset_single_table_counters")?;
            {
                let mut session = ctx.session_stats.write();
                session.pending_flush.relations.remove(&oid);
                session.force_clear_snapshot();
            }
            ctx.stats.write().reset_relation(oid);
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatResetSingleFunctionCounters => {
            let oid = stats_oid_arg(&values, "pg_stat_reset_single_function_counters")?;
            {
                let mut session = ctx.session_stats.write();
                session.pending_flush.functions.remove(&oid);
                session.force_clear_snapshot();
            }
            ctx.stats.write().reset_function(oid);
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatResetBackendStats => {
            let pid = values.first().and_then(|value| match value {
                Value::Int32(value) => Some(*value),
                Value::Int64(value) => i32::try_from(*value).ok(),
                _ => None,
            });
            if pid == Some(ctx.client_id as i32) {
                ctx.session_stats.write().reset_backend_stats();
            }
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatResetSlru => {
            let target = match values.as_slice() {
                [] | [Value::Null] => None,
                [value] => Some(value.as_text().ok_or_else(|| ExecError::TypeMismatch {
                    op: "pg_stat_reset_slru",
                    left: value.clone(),
                    right: Value::Text("".into()),
                })?),
                _ => None,
            };
            ctx.stats.write().reset_slru(target);
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatResetReplicationSlot
        | BuiltinScalarFunction::PgStatResetSubscriptionStats => Ok(Value::Null),
        BuiltinScalarFunction::PgStatGetReplicationSlot
        | BuiltinScalarFunction::PgStatGetSubscriptionStats => Ok(Value::Null),
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
                "bgwriter" | "checkpointer" | "io" | "wal" => objid == 0 && objsubid == 0,
                "database" => objid != 0 && objsubid == 0,
                "relation" => {
                    let relation_oid = u32::try_from(objsubid).unwrap_or_default();
                    if ctx
                        .session_stats
                        .write()
                        .take_relation_have_stats_false_once(relation_oid)
                    {
                        false
                    } else {
                        executor_catalog(ctx)?
                            .class_row_by_oid(relation_oid)
                            .is_some()
                    }
                }
                "function" => ctx
                    .session_stats
                    .write()
                    .has_visible_function_stats(&ctx.stats, objid),
                other => {
                    return Err(ExecError::DetailedError {
                        message: format!("invalid statistics kind: \"{other}\""),
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
        | BuiltinScalarFunction::PgStatGetTuplesHotUpdated
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
        BuiltinScalarFunction::PgSleep => eval_pg_sleep_function(&values),
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
        BuiltinScalarFunction::TrimArray => eval_trim_array_function(&values),
        BuiltinScalarFunction::ArrayShuffle => eval_array_shuffle_function(&values),
        BuiltinScalarFunction::ArraySample => eval_array_sample_function(&values),
        BuiltinScalarFunction::ArrayReverse => eval_array_reverse_function(&values),
        BuiltinScalarFunction::ArraySort => eval_array_sort_function(&values),
        BuiltinScalarFunction::CurrentDatabase => {
            Ok(Value::Text(ctx.current_database_name.clone().into()))
        }
        BuiltinScalarFunction::CurrentSchemas => {
            let include_implicit = matches!(values.first(), Some(Value::Bool(true)));
            Ok(current_schemas_value(include_implicit, ctx))
        }
        BuiltinScalarFunction::Version => Ok(Value::Text(pg_version_text().into())),
        BuiltinScalarFunction::PgBackendPid => Ok(Value::Int32(ctx.client_id as i32)),
        BuiltinScalarFunction::PgColumnCompression => eval_pg_column_compression_values(&values),
        BuiltinScalarFunction::PgColumnToastChunkId => {
            eval_pg_column_toast_chunk_id_values(&values)
        }
        BuiltinScalarFunction::PgColumnSize => eval_pg_column_size_values(&values),
        BuiltinScalarFunction::PgRelationFilenode => eval_pg_relation_filenode(&values, ctx),
        BuiltinScalarFunction::PgFilenodeRelation => eval_pg_filenode_relation(&values, ctx),
        BuiltinScalarFunction::PgRelationSize => eval_pg_relation_size(&values, ctx),
        BuiltinScalarFunction::PgTableSize => eval_pg_table_size(&values, ctx),
        BuiltinScalarFunction::PgTablespaceLocation => eval_pg_tablespace_location(&values, ctx),
        BuiltinScalarFunction::NumNulls => Ok(eval_num_nulls(&values, func_variadic, true)),
        BuiltinScalarFunction::NumNonNulls => Ok(eval_num_nulls(&values, func_variadic, false)),
        BuiltinScalarFunction::PgLogBackendMemoryContexts => {
            eval_pg_log_backend_memory_contexts(&values)
        }
        BuiltinScalarFunction::HasFunctionPrivilege => eval_has_function_privilege(&values, ctx),
        BuiltinScalarFunction::HasTablePrivilege => {
            eval_has_relation_privilege(PrivilegeRelationKind::Table, &values, ctx)
        }
        BuiltinScalarFunction::HasSequencePrivilege => {
            eval_has_relation_privilege(PrivilegeRelationKind::Sequence, &values, ctx)
        }
        BuiltinScalarFunction::HasAnyColumnPrivilege => eval_has_any_column_privilege(&values, ctx),
        BuiltinScalarFunction::HasColumnPrivilege => eval_has_column_privilege(&values, ctx),
        BuiltinScalarFunction::HasLargeObjectPrivilege => {
            eval_has_largeobject_privilege(&values, ctx)
        }
        BuiltinScalarFunction::PgHasRole => eval_pg_has_role(&values, ctx),
        BuiltinScalarFunction::PgCurrentLogfile => eval_pg_current_logfile(&values),
        BuiltinScalarFunction::PgReadFile => eval_pg_read_file(&values, ctx, false),
        BuiltinScalarFunction::PgReadBinaryFile => eval_pg_read_file(&values, ctx, true),
        BuiltinScalarFunction::PgStatFile => eval_pg_stat_file(&values, ctx),
        BuiltinScalarFunction::PgWalfileName => eval_pg_walfile_name(&values),
        BuiltinScalarFunction::PgWalfileNameOffset => eval_pg_walfile_name_offset(&values),
        BuiltinScalarFunction::PgSplitWalfileName => eval_pg_split_walfile_name(&values),
        BuiltinScalarFunction::PgControlSystem
        | BuiltinScalarFunction::PgControlCheckpoint
        | BuiltinScalarFunction::PgControlRecovery
        | BuiltinScalarFunction::PgControlInit => Ok(eval_pg_control_record(func, ctx)),
        BuiltinScalarFunction::PgReplicationOriginCreate => {
            eval_pg_replication_origin_create(&values)
        }
        BuiltinScalarFunction::GistTranslateCmpTypeCommon => {
            eval_gist_translate_cmptype_common(&values)
        }
        BuiltinScalarFunction::TestCanonicalizePath => eval_test_canonicalize_path(&values),
        BuiltinScalarFunction::TestRelpath => Ok(Value::Null),
        BuiltinScalarFunction::PgPartitionRoot => eval_pg_partition_root(&values, ctx),
        BuiltinScalarFunction::PgGetPartKeyDef => eval_pg_get_partkeydef(&values, ctx),
        BuiltinScalarFunction::PgTableIsVisible => eval_pg_table_is_visible(&values, ctx),
        BuiltinScalarFunction::ObjDescription => eval_obj_description(&values, ctx),
        BuiltinScalarFunction::ShobjDescription => eval_shobj_description(&values, ctx),
        BuiltinScalarFunction::PgDescribeObject => eval_pg_describe_object(&values, ctx),
        BuiltinScalarFunction::PgIdentifyObject => eval_pg_identify_object(&values, ctx),
        BuiltinScalarFunction::PgIdentifyObjectAsAddress => {
            eval_pg_identify_object_as_address(&values, ctx)
        }
        BuiltinScalarFunction::PgGetObjectAddress => eval_pg_get_object_address(&values, ctx),
        BuiltinScalarFunction::PgEventTriggerTableRewriteOid => {
            eval_pg_event_trigger_table_rewrite_oid()
        }
        BuiltinScalarFunction::PgEventTriggerTableRewriteReason => {
            eval_pg_event_trigger_table_rewrite_reason()
        }
        BuiltinScalarFunction::PgGetFunctionArguments => {
            eval_pg_get_function_arguments(&values, ctx)
        }
        BuiltinScalarFunction::PgGetFunctionDef => eval_pg_get_functiondef(&values, ctx),
        BuiltinScalarFunction::PgGetFunctionResult => eval_pg_get_function_result(&values, ctx),
        BuiltinScalarFunction::PgGetExpr => eval_pg_get_expr(&values, ctx),
        BuiltinScalarFunction::PgGetConstraintDef => eval_pg_get_constraintdef(&values, ctx),
        BuiltinScalarFunction::PgGetPartitionConstraintDef => {
            eval_pg_get_partition_constraintdef(&values, ctx)
        }
        BuiltinScalarFunction::PgGetIndexDef => eval_pg_get_indexdef(&values, ctx),
        BuiltinScalarFunction::PgGetRuleDef => eval_pg_get_ruledef(&values, ctx),
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
        BuiltinScalarFunction::Pi => {
            if values.is_empty() {
                Ok(Value::Float64(std::f64::consts::PI))
            } else {
                Err(malformed_expr_error("pi"))
            }
        }
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
        BuiltinScalarFunction::UnsupportedXmlFeature => Err(unsupported_xml_feature_error()),
        BuiltinScalarFunction::XmlComment => eval_xml_comment_function(&values, Some(ctx)),
        BuiltinScalarFunction::XmlText => eval_xml_text_function(&values, Some(ctx)),
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
        BuiltinScalarFunction::XPath => eval_xpath_function(&values),
        BuiltinScalarFunction::XPathExists => eval_xpath_exists_function(&values),
        BuiltinScalarFunction::TsMatch => eval_ts_match_values(
            &values,
            ctx.gucs
                .get("default_text_search_config")
                .map(String::as_str),
            catalog_lookup(Some(ctx)),
        ),
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
        BuiltinScalarFunction::TsQueryContains => match values.as_slice() {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::Bool(
                crate::backend::executor::tsquery_contains(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "@>",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryContainedBy => match values.as_slice() {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::Bool(
                crate::backend::executor::tsquery_contained_by(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "<@",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
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
        BuiltinScalarFunction::Greatest => eval_greatest(&values),
        BuiltinScalarFunction::Least => eval_least(&values),
        BuiltinScalarFunction::Length => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_length_function(&values),
        },
        BuiltinScalarFunction::BitLength => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_bit_length_function(&values),
        },
        BuiltinScalarFunction::Concat => {
            eval_concat_function(&values, func_variadic, &ctx.datetime_config)
        }
        BuiltinScalarFunction::TextCat => match values.as_slice() {
            [left, right] => concat_values(left.clone(), right.clone()),
            _ => Err(ExecError::DetailedError {
                message: "textcat expects exactly two arguments".into(),
                detail: None,
                hint: None,
                sqlstate: "42883",
            }),
        },
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
        BuiltinScalarFunction::Upper => eval_upper_function(&values),
        BuiltinScalarFunction::TextStartsWith => eval_text_starts_with_function(&values),
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
        BuiltinScalarFunction::HasForeignDataWrapperPrivilege => {
            eval_has_foreign_privilege_function(
                ForeignPrivilegeKind::ForeignDataWrapper,
                &values,
                Some(ctx),
            )
        }
        BuiltinScalarFunction::HasServerPrivilege => {
            eval_has_foreign_privilege_function(ForeignPrivilegeKind::Server, &values, Some(ctx))
        }
        BuiltinScalarFunction::RegClassToText => eval_regclass_to_text_function(&values, Some(ctx)),
        BuiltinScalarFunction::RegTypeToText => eval_regtype_to_text_function(&values, Some(ctx)),
        BuiltinScalarFunction::RegRoleToText => eval_regrole_to_text_function(&values, Some(ctx)),
        BuiltinScalarFunction::BpcharToText => eval_bpchar_to_text_function(&values),
        BuiltinScalarFunction::QuoteIdent => eval_quote_ident_function(&values),
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
        BuiltinScalarFunction::Convert => eval_convert(&values),
        BuiltinScalarFunction::ConvertFrom => eval_convert_from_function(&values),
        BuiltinScalarFunction::ConvertTo => eval_convert_to_function(&values),
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
        BuiltinScalarFunction::ToChar => eval_to_char_function(&values, &ctx.datetime_config),
        BuiltinScalarFunction::ToDate => eval_to_date_function(&values),
        BuiltinScalarFunction::ToNumber => eval_to_number_function(&values),
        _ => unreachable!("json builtins handled by expr_json"),
    }
}

fn eval_enum_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
    ctx: &ExecutorContext,
) -> Option<Result<Value, ExecError>> {
    if !matches!(
        func,
        BuiltinScalarFunction::EnumFirst
            | BuiltinScalarFunction::EnumLast
            | BuiltinScalarFunction::EnumRange
    ) {
        return None;
    }
    Some(eval_enum_function_inner(func, values, result_type, ctx))
}

fn eval_enum_function_inner(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
    ctx: &ExecutorContext,
) -> Result<Value, ExecError> {
    let enum_type = result_type
        .map(|ty| if ty.is_array { ty.element_type() } else { ty })
        .filter(|ty| matches!(ty.kind, SqlTypeKind::Enum) && ty.type_oid != 0)
        .ok_or_else(|| ExecError::DetailedError {
            message: "enum support function requires a concrete enum type".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        })?;
    let catalog = executor_catalog(ctx)?;
    let enum_type_oid = if enum_type.typrelid != 0 {
        enum_type.typrelid
    } else {
        enum_type.type_oid
    };
    let mut labels = catalog
        .enum_rows()
        .into_iter()
        .filter(|row| row.enumtypid == enum_type_oid)
        .collect::<Vec<_>>();
    labels.sort_by(|left, right| {
        left.enumsortorder
            .partial_cmp(&right.enumsortorder)
            .unwrap_or(Ordering::Equal)
    });
    match func {
        BuiltinScalarFunction::EnumFirst => labels
            .first()
            .map(|row| {
                ensure_enum_function_label_safe(catalog, enum_type_oid, row.oid)?;
                Ok(Value::EnumOid(row.oid))
            })
            .unwrap_or(Ok(Value::Null)),
        BuiltinScalarFunction::EnumLast => labels
            .last()
            .map(|row| {
                ensure_enum_function_label_safe(catalog, enum_type_oid, row.oid)?;
                Ok(Value::EnumOid(row.oid))
            })
            .unwrap_or(Ok(Value::Null)),
        BuiltinScalarFunction::EnumRange => {
            let lower = values.first().and_then(|value| match value {
                Value::EnumOid(oid) => labels.iter().position(|row| row.oid == *oid),
                Value::Null => Some(0),
                _ => None,
            });
            let upper = values.get(1).and_then(|value| match value {
                Value::EnumOid(oid) => labels.iter().position(|row| row.oid == *oid),
                Value::Null => labels.len().checked_sub(1),
                _ => None,
            });
            let (start, end) = match values.len() {
                1 => (0, labels.len().saturating_sub(1)),
                2 => (lower.unwrap_or(labels.len()), upper.unwrap_or(0)),
                _ => {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "enum_range(anyenum [, anyenum])",
                        actual: format!("enum_range({} args)", values.len()),
                    }));
                }
            };
            let items = if labels.is_empty() || start > end {
                Vec::new()
            } else {
                let mut items = Vec::new();
                for row in &labels[start..=end] {
                    ensure_enum_function_label_safe(catalog, enum_type_oid, row.oid)?;
                    items.push(Value::EnumOid(row.oid));
                }
                items
            };
            Ok(Value::PgArray(
                ArrayValue::from_1d(items).with_element_type_oid(enum_type_oid),
            ))
        }
        _ => unreachable!(),
    }
}

fn ensure_enum_function_label_safe(
    catalog: &dyn CatalogLookup,
    enum_type_oid: u32,
    label_oid: u32,
) -> Result<(), ExecError> {
    if catalog.enum_label_is_committed(enum_type_oid, label_oid) {
        return Ok(());
    }
    let label = catalog
        .enum_label(enum_type_oid, label_oid)
        .or_else(|| catalog.enum_label_by_oid(label_oid))
        .unwrap_or_else(|| label_oid.to_string());
    let type_name = catalog
        .type_by_oid(enum_type_oid)
        .map(|row| row.typname)
        .unwrap_or_else(|| enum_type_oid.to_string());
    Err(ExecError::DetailedError {
        message: format!("unsafe use of new value \"{label}\" of enum type {type_name}"),
        detail: None,
        hint: Some("New enum values must be committed before they can be used.".into()),
        sqlstate: "55P04",
    })
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

fn eval_random_function(values: &[Value], ctx: &mut ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [] => Ok(Value::Float64(ctx.random_state.lock().double())),
        [Value::Int32(min), Value::Int32(max)] => {
            if min > max {
                return Err(invalid_random_bound_error(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            Ok(Value::Int32(
                ctx.random_state
                    .lock()
                    .int64_range(i64::from(*min), i64::from(*max)) as i32,
            ))
        }
        [Value::Int64(min), Value::Int64(max)] => {
            if min > max {
                return Err(invalid_random_bound_error(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            Ok(Value::Int64(
                ctx.random_state.lock().int64_range(*min, *max),
            ))
        }
        [Value::Numeric(min), Value::Numeric(max)] => eval_random_numeric_range(min, max, ctx),
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
    crate::backend::access::hash::support::hash_bytes_extended(value, 0) as u32
}

fn uuid_hash_extended(value: &[u8; 16], seed: u64) -> u64 {
    crate::backend::access::hash::support::hash_bytes_extended(value, seed)
}

fn eval_random_normal_function(
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
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

    Ok(Value::Float64(
        (ctx.random_state.lock().double_normal() * stddev) + mean,
    ))
}

fn eval_setseed_function(values: &[Value], ctx: &mut ExecutorContext) -> Result<Value, ExecError> {
    match values {
        [value] => {
            let seed = expect_float8_arg("setseed", value)?;
            if !seed.is_finite() || !(-1.0..=1.0).contains(&seed) {
                return Err(ExecError::DetailedError {
                    message: format!("setseed parameter {seed} is out of allowed range [-1,1]")
                        .into(),
                    detail: None,
                    hint: None,
                    sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
                });
            }
            ctx.random_state.lock().fseed(seed);
            Ok(Value::Null)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("SetSeed({} args)", values.len()),
        })),
    }
}

fn eval_random_numeric_range(
    min: &NumericValue,
    max: &NumericValue,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
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
    Ok(Value::Numeric(
        ctx.random_state.lock().numeric_range(min, max),
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
