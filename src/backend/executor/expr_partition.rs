use super::exec_expr::eval_expr;
use super::expr_agg_support::execute_scalar_function_value_call;
use super::expr_reg::format_type_text;
use super::{ExecError, ExecutorCatalog, ExecutorContext, TupleSlot};
use crate::backend::access::hash::{HASH_PARTITION_SEED, hash_combine64, hash_value_extended};
use crate::backend::parser::analyze::{is_binary_coercible_type, sql_type_name};
use crate::backend::parser::{CatalogLookup, LoweredPartitionSpec, ParseError, PartitionStrategy};
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{ANYOID, OID_TYPE_OID, builtin_scalar_function_for_proc_oid};
use crate::include::nodes::datum::{ArrayValue, Value};
use crate::include::nodes::parsenodes::SqlType;
use crate::include::nodes::primnodes::{BuiltinScalarFunction, Expr, expr_sql_type_hint};

pub(crate) fn eval_satisfies_hash_partition(
    args: &[Expr],
    func_variadic: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if args.len() < 3 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "satisfies_hash_partition(parent, modulus, remainder, keys...)",
            actual: format!("{} arguments", args.len()),
        }));
    }

    let parent = eval_expr(&args[0], slot, ctx)?;
    let modulus = eval_expr(&args[1], slot, ctx)?;
    let remainder = eval_expr(&args[2], slot, ctx)?;
    if matches!(parent, Value::Null)
        || matches!(modulus, Value::Null)
        || matches!(remainder, Value::Null)
    {
        return Ok(Value::Bool(false));
    }

    let parent_oid = oid_arg_to_u32(&parent, "satisfies_hash_partition")?;
    let modulus = int32_arg(&modulus, "satisfies_hash_partition")?;
    let remainder = int32_arg(&remainder, "satisfies_hash_partition")?;
    validate_hash_partition_modulus_remainder(modulus, remainder)?;

    let catalog = executor_catalog(ctx)?;
    let relation = catalog
        .relation_by_oid(parent_oid)
        .ok_or_else(|| relation_open_error(parent_oid))?;
    let spec = crate::backend::parser::relation_partition_spec(&relation)
        .ok()
        .filter(|spec| spec.strategy == PartitionStrategy::Hash)
        .ok_or_else(|| not_hash_partitioned_error(catalog.as_ref(), parent_oid))?;

    if func_variadic {
        let Some(array_expr) = args.get(3) else {
            return key_count_error(spec.key_types.len(), 0);
        };
        let array_value = eval_expr(array_expr, slot, ctx)?;
        let Some(array) = array_value.as_array_value() else {
            return Err(ExecError::TypeMismatch {
                op: "satisfies_hash_partition",
                left: array_value,
                right: Value::Int64(i64::from(OID_TYPE_OID)),
            });
        };
        return satisfies_hash_partition_variadic(
            &spec, array_expr, &array, modulus, remainder, catalog, ctx,
        );
    }

    satisfies_hash_partition_args(&spec, &args[3..], slot, modulus, remainder, catalog, ctx)
}

fn satisfies_hash_partition_args(
    spec: &LoweredPartitionSpec,
    key_exprs: &[Expr],
    slot: &mut TupleSlot,
    modulus: i32,
    remainder: i32,
    catalog: ExecutorCatalog,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if key_exprs.len() != spec.key_types.len() {
        return key_count_error(spec.key_types.len(), key_exprs.len());
    }

    let mut row_hash = 0_u64;
    for (index, (expr, expected_type)) in key_exprs.iter().zip(&spec.key_types).enumerate() {
        let value = eval_expr(expr, slot, ctx)?;
        let actual_type = expr_sql_type_hint(expr)
            .or_else(|| value.sql_type_hint())
            .unwrap_or(*expected_type);
        if !argument_type_matches(actual_type, *expected_type) {
            return key_type_error(index, *expected_type, actual_type, false, catalog.as_ref());
        }
        if let Some(value_hash) = partition_hash_value(index, &value, spec, catalog.as_ref(), ctx)?
        {
            row_hash = hash_combine64(row_hash, value_hash);
        }
    }

    Ok(Value::Bool(row_hash % modulus as u64 == remainder as u64))
}

fn satisfies_hash_partition_variadic(
    spec: &LoweredPartitionSpec,
    array_expr: &Expr,
    array: &ArrayValue,
    modulus: i32,
    remainder: i32,
    catalog: ExecutorCatalog,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if array.elements.len() != spec.key_types.len() {
        return key_count_error(spec.key_types.len(), array.elements.len());
    }

    let element_type = variadic_element_type(array_expr, array, catalog.as_ref())
        .unwrap_or_else(|| spec.key_types[0]);
    let element_type_oid = sql_type_oid(element_type);
    for (index, key_type) in spec.key_types.iter().copied().enumerate() {
        if sql_type_oid(key_type) != element_type_oid {
            return key_type_error(index, key_type, element_type, true, catalog.as_ref());
        }
    }

    let mut row_hash = 0_u64;
    for (index, value) in array.elements.iter().enumerate() {
        if let Some(value_hash) = partition_hash_value(index, value, spec, catalog.as_ref(), ctx)? {
            row_hash = hash_combine64(row_hash, value_hash);
        }
    }

    Ok(Value::Bool(row_hash % modulus as u64 == remainder as u64))
}

fn partition_hash_value(
    key_index: usize,
    value: &Value,
    spec: &LoweredPartitionSpec,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Option<u64>, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }

    let opclass = spec.partclass.get(key_index).copied();
    if let Some(proc_oid) = hash_support_proc(key_index, spec, catalog) {
        if matches!(
            builtin_scalar_function_for_proc_oid(proc_oid),
            Some(BuiltinScalarFunction::HashValueExtended(_))
        ) {
            return hash_value_extended(value, opclass, HASH_PARTITION_SEED)
                .map_err(unsupported_hash_key_error);
        }
        return execute_partition_hash_support_proc(proc_oid, value, ctx);
    }

    hash_value_extended(value, opclass, HASH_PARTITION_SEED).map_err(unsupported_hash_key_error)
}

fn hash_support_proc(
    key_index: usize,
    spec: &LoweredPartitionSpec,
    catalog: &dyn CatalogLookup,
) -> Option<u32> {
    let opclass_oid = *spec.partclass.get(key_index)?;
    let opclass = catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)?;
    let key_type_oid = sql_type_oid(*spec.key_types.get(key_index)?);
    catalog
        .amproc_rows()
        .into_iter()
        .find(|row| {
            row.amprocfamily == opclass.opcfamily
                && row.amprocnum == 2
                && (row.amproclefttype == key_type_oid || row.amproclefttype == ANYOID)
                && (row.amprocrighttype == key_type_oid || row.amprocrighttype == ANYOID)
        })
        .map(|row| row.amproc)
}

fn execute_partition_hash_support_proc(
    proc_oid: u32,
    value: &Value,
    ctx: &mut ExecutorContext,
) -> Result<Option<u64>, ExecError> {
    let result = execute_scalar_function_value_call(
        proc_oid,
        &[
            value.clone(),
            Value::Int64(crate::backend::access::hash::HASH_PARTITION_SEED as i64),
        ],
        ctx,
    )?;
    match result {
        Value::Null => Ok(None),
        Value::Int64(value) => Ok(Some(value as u64)),
        Value::Int32(value) => Ok(Some(value as u64)),
        Value::Int16(value) => Ok(Some(value as u64)),
        other => Err(ExecError::DetailedError {
            message: "hash partition support function returned non-integer value".into(),
            detail: Some(format!("returned {other:?}")),
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn validate_hash_partition_modulus_remainder(
    modulus: i32,
    remainder: i32,
) -> Result<(), ExecError> {
    if modulus <= 0 {
        return Err(detailed_error(
            "modulus for hash partition must be an integer value greater than zero",
            "22023",
        ));
    }
    if remainder < 0 {
        return Err(detailed_error(
            "remainder for hash partition must be an integer value greater than or equal to zero",
            "22023",
        ));
    }
    if remainder >= modulus {
        return Err(detailed_error(
            "remainder for hash partition must be less than modulus",
            "22023",
        ));
    }
    Ok(())
}

fn argument_type_matches(actual: SqlType, expected: SqlType) -> bool {
    actual == expected || is_binary_coercible_type(actual, expected)
}

fn variadic_element_type(
    array_expr: &Expr,
    array: &ArrayValue,
    catalog: &dyn CatalogLookup,
) -> Option<SqlType> {
    expr_sql_type_hint(array_expr)
        .filter(|ty| ty.is_array)
        .map(SqlType::element_type)
        .or_else(|| {
            array
                .element_type_oid
                .and_then(|oid| catalog.type_by_oid(oid).map(|row| row.sql_type))
        })
        .or_else(|| array.elements.iter().find_map(Value::sql_type_hint))
}

fn executor_catalog(ctx: &ExecutorContext) -> Result<ExecutorCatalog, ExecError> {
    ctx.catalog
        .clone()
        .ok_or_else(|| detailed_error("satisfies_hash_partition requires catalog context", "0A000"))
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
                right: Value::Int64(i64::from(OID_TYPE_OID)),
            }),
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(i64::from(OID_TYPE_OID)),
        }),
    }
}

fn int32_arg(value: &Value, op: &'static str) -> Result<i32, ExecError> {
    match value {
        Value::Int16(value) => Ok(i32::from(*value)),
        Value::Int32(value) => Ok(*value),
        Value::Int64(value) => i32::try_from(*value).map_err(|_| ExecError::TypeMismatch {
            op,
            left: Value::Int64(*value),
            right: Value::Int32(0),
        }),
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int32(0),
        }),
    }
}

fn key_count_error(expected: usize, actual: usize) -> Result<Value, ExecError> {
    Err(detailed_error(
        format!(
            "number of partitioning columns ({expected}) does not match number of partition keys provided ({actual})"
        ),
        "22023",
    ))
}

fn key_type_error(
    key_index: usize,
    expected: SqlType,
    actual: SqlType,
    quoted: bool,
    catalog: &dyn CatalogLookup,
) -> Result<Value, ExecError> {
    let expected = format_partition_type(expected, catalog);
    let actual = format_partition_type(actual, catalog);
    let (expected, actual) = if quoted {
        (format!("\"{expected}\""), format!("\"{actual}\""))
    } else {
        (expected, actual)
    };
    Err(detailed_error(
        format!(
            "column {} of the partition key has type {expected}, but supplied value is of type {actual}",
            key_index + 1
        ),
        "22023",
    ))
}

fn relation_open_error(relation_oid: u32) -> ExecError {
    detailed_error(
        format!("could not open relation with OID {relation_oid}"),
        "42P01",
    )
}

fn not_hash_partitioned_error(catalog: &dyn CatalogLookup, relation_oid: u32) -> ExecError {
    detailed_error(
        format!(
            "\"{}\" is not a hash partitioned table",
            relation_name_for_oid(catalog, relation_oid)
        ),
        "22023",
    )
}

fn unsupported_hash_key_error(message: String) -> ExecError {
    detailed_error(
        format!("unsupported hash partition key value {message}"),
        "0A000",
    )
}

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname.replace('"', "\"\""))
        .unwrap_or_else(|| relation_oid.to_string())
}

fn format_partition_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> String {
    let type_oid = sql_type_oid(sql_type);
    if type_oid != 0 {
        return format_type_text(type_oid, None, catalog);
    }
    sql_type_name(sql_type)
}

fn detailed_error(message: impl Into<String>, sqlstate: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate,
    }
}
