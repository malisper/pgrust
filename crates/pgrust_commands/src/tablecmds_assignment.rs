use pgrust_catalog_data::builtin_ranges::{
    builtin_multirange_name_for_sql_type, builtin_range_name_for_sql_type,
};
use pgrust_nodes::datum::{ArrayDimension, ArrayValue, RecordDescriptor, RecordValue, Value};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};

use crate::tablecmds::{self, TableCmdsError};

#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentError {
    TableCmds(TableCmdsError),
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    InvalidStorageValue {
        column: String,
        details: String,
    },
    Int4OutOfRange,
}

impl From<TableCmdsError> for AssignmentError {
    fn from(err: TableCmdsError) -> Self {
        AssignmentError::TableCmds(err)
    }
}

pub trait AssignmentRuntime {
    fn assignment_navigation_sql_type(&self, sql_type: SqlType) -> SqlType;

    fn assignment_record_descriptor(
        &self,
        sql_type: SqlType,
    ) -> Result<RecordDescriptor, AssignmentError>;

    fn apply_jsonb_subscript_assignment(
        &mut self,
        current: &Value,
        path: &[Value],
        replacement: &Value,
    ) -> Result<Value, AssignmentError>;
}

#[derive(Clone)]
pub struct ResolvedAssignmentSubscript {
    pub is_slice: bool,
    pub lower: Option<Value>,
    pub upper: Option<Value>,
}

#[derive(Clone)]
pub enum ResolvedAssignmentIndirection {
    Subscript(ResolvedAssignmentSubscript),
    Field(String),
}

pub fn assign_typed_value_ordered<R: AssignmentRuntime>(
    current: Value,
    sql_type: SqlType,
    indirection: &[ResolvedAssignmentIndirection],
    replacement: Value,
    runtime: &mut R,
) -> Result<Value, AssignmentError> {
    let Some((first, rest)) = indirection.split_first() else {
        return Ok(replacement);
    };
    let sql_type = runtime.assignment_navigation_sql_type(sql_type);
    match first {
        ResolvedAssignmentIndirection::Field(field) => {
            assign_record_field_ordered(current, sql_type, field, rest, replacement, runtime)
        }
        ResolvedAssignmentIndirection::Subscript(subscript) => {
            let (leading_subscripts, after_subscripts) = leading_assignment_subscripts(indirection);
            if sql_type.kind == SqlTypeKind::Point && !sql_type.is_array {
                if !after_subscripts.is_empty() || leading_subscripts.len() != 1 {
                    return Err(detailed_error(
                        "cannot assign through a subscripted point value",
                        None,
                        None,
                        "42804",
                    ));
                }
                return assign_point_value(current, &leading_subscripts, replacement);
            }
            if sql_type.kind == SqlTypeKind::Jsonb && !sql_type.is_array {
                if !after_subscripts.is_empty() {
                    return Err(detailed_error(
                        "cannot assign through a subscripted jsonb value",
                        None,
                        None,
                        "42804",
                    ));
                }
                return assign_jsonb_value(current, &leading_subscripts, replacement, runtime);
            }
            if after_subscripts.is_empty() {
                return assign_array_value(current, &leading_subscripts, replacement);
            }
            assign_array_value_ordered(current, sql_type, subscript, rest, replacement, runtime)
        }
    }
}

pub fn sql_type_display_name(ty: SqlType) -> String {
    if ty.is_range() {
        let base = builtin_range_name_for_sql_type(ty).unwrap_or("range");
        return if ty.is_array {
            format!("{base}[]")
        } else {
            base.to_string()
        };
    }
    if ty.is_multirange() {
        let base = builtin_multirange_name_for_sql_type(ty).unwrap_or("multirange");
        return if ty.is_array {
            format!("{base}[]")
        } else {
            base.to_string()
        };
    }
    let base = match ty.kind {
        SqlTypeKind::AnyElement => "anyelement",
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::AnyRange => "anyrange",
        SqlTypeKind::AnyMultirange => "anymultirange",
        SqlTypeKind::AnyCompatible => "anycompatible",
        SqlTypeKind::AnyCompatibleArray => "anycompatiblearray",
        SqlTypeKind::AnyCompatibleRange => "anycompatiblerange",
        SqlTypeKind::AnyCompatibleMultirange => "anycompatiblemultirange",
        SqlTypeKind::AnyEnum => "anyenum",
        SqlTypeKind::Enum => return ty.type_oid.to_string(),
        SqlTypeKind::Record | SqlTypeKind::Composite => "record",
        SqlTypeKind::Shell => "shell",
        SqlTypeKind::Internal => "internal",
        SqlTypeKind::Cstring => "cstring",
        SqlTypeKind::Void => "void",
        SqlTypeKind::Trigger => "trigger",
        SqlTypeKind::EventTrigger => "event_trigger",
        SqlTypeKind::FdwHandler => "fdw_handler",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegProc => "regproc",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegType => "regtype",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegNamespace => "regnamespace",
        SqlTypeKind::RegOper => "regoper",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::RegCollation => "regcollation",
        SqlTypeKind::Tid => "tid",
        SqlTypeKind::Xid => "xid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "bit varying",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Uuid => "uuid",
        SqlTypeKind::Inet => "inet",
        SqlTypeKind::Cidr => "cidr",
        SqlTypeKind::MacAddr => "macaddr",
        SqlTypeKind::MacAddr8 => "macaddr8",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Money => "money",
        SqlTypeKind::PgLsn => "pg_lsn",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Xml => "xml",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
    };

    if ty.is_array {
        format!("{base}[]")
    } else {
        base.to_string()
    }
}

fn leading_assignment_subscripts(
    indirection: &[ResolvedAssignmentIndirection],
) -> (
    Vec<ResolvedAssignmentSubscript>,
    &[ResolvedAssignmentIndirection],
) {
    let split = indirection
        .iter()
        .position(|step| matches!(step, ResolvedAssignmentIndirection::Field(_)))
        .unwrap_or(indirection.len());
    let subscripts = indirection[..split]
        .iter()
        .filter_map(|step| match step {
            ResolvedAssignmentIndirection::Subscript(subscript) => Some(subscript.clone()),
            ResolvedAssignmentIndirection::Field(_) => None,
        })
        .collect();
    (subscripts, &indirection[split..])
}

fn assign_point_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, AssignmentError> {
    if subscripts.len() != 1 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }
    let subscript = &subscripts[0];
    if subscript.is_slice {
        return Err(detailed_error(
            "slices of fixed-length arrays not implemented",
            None,
            None,
            "0A000",
        ));
    }
    let Some(index) = assignment_subscript_index(subscript.lower.as_ref())? else {
        return Err(assignment_null_subscript_error());
    };
    if !(0..=1).contains(&index) {
        return Err(array_assignment_error("array subscript out of range"));
    }
    let Value::Point(mut point) = current else {
        return if matches!(current, Value::Null) {
            Ok(Value::Null)
        } else {
            Err(AssignmentError::TypeMismatch {
                op: "array assignment",
                left: current,
                right: Value::Null,
            })
        };
    };
    let coordinate = match replacement {
        Value::Null => return Ok(Value::Point(point)),
        Value::Float64(value) => value,
        other => {
            return Err(AssignmentError::TypeMismatch {
                op: "array assignment",
                left: Value::Point(point),
                right: other,
            });
        }
    };
    if index == 0 {
        point.x = coordinate;
    } else {
        point.y = coordinate;
    }
    Ok(Value::Point(point))
}

fn assign_record_field_ordered<R: AssignmentRuntime>(
    current: Value,
    sql_type: SqlType,
    field: &str,
    rest: &[ResolvedAssignmentIndirection],
    replacement: Value,
    runtime: &mut R,
) -> Result<Value, AssignmentError> {
    let mut record = assignment_record_value(current, sql_type, runtime)?;
    let (field_index, field_type) = record
        .descriptor
        .fields
        .iter()
        .enumerate()
        .find(|(_, candidate)| candidate.name.eq_ignore_ascii_case(field))
        .map(|(index, candidate)| (index, candidate.sql_type))
        .ok_or_else(|| {
            detailed_error(
                format!("record has no field \"{field}\""),
                None,
                None,
                "42703",
            )
        })?;
    record.fields[field_index] = assign_typed_value_ordered(
        record.fields[field_index].clone(),
        field_type,
        rest,
        replacement,
        runtime,
    )?;
    Ok(Value::Record(record))
}

fn assign_array_value_ordered<R: AssignmentRuntime>(
    current: Value,
    array_type: SqlType,
    subscript: &ResolvedAssignmentSubscript,
    rest: &[ResolvedAssignmentIndirection],
    replacement: Value,
    runtime: &mut R,
) -> Result<Value, AssignmentError> {
    if !array_type.is_array {
        return Err(detailed_error(
            format!(
                "cannot subscript type {} because it does not support subscripting",
                sql_type_display_name(array_type)
            ),
            None,
            None,
            "42804",
        ));
    }
    if rest.is_empty() {
        return assign_array_value(current, std::slice::from_ref(subscript), replacement);
    }
    if subscript.is_slice {
        return Err(detailed_error(
            "sliced assignment into nested fields is not supported",
            None,
            None,
            "0A000",
        ));
    }
    let (mut lower_bound, mut items) = assignment_top_level(current)?;
    let Some(index) = assignment_subscript_index(subscript.lower.as_ref())? else {
        return Err(assignment_null_subscript_error());
    };
    if items.is_empty() {
        lower_bound = index;
    }
    extend_assignment_items(&mut lower_bound, &mut items, index, index)?;
    let item_index = usize::try_from(i64::from(index) - i64::from(lower_bound))
        .map_err(|_| array_assignment_limit_error())?;
    items[item_index] = assign_typed_value_ordered(
        items[item_index].clone(),
        array_type.element_type(),
        rest,
        replacement,
        runtime,
    )?;
    build_assignment_array_value(lower_bound, items)
}

fn assign_jsonb_value<R: AssignmentRuntime>(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
    runtime: &mut R,
) -> Result<Value, AssignmentError> {
    let mut path = Vec::with_capacity(subscripts.len());
    for subscript in subscripts {
        if subscript.is_slice {
            return Err(detailed_error(
                "jsonb subscript does not support slices",
                None,
                None,
                "0A000",
            ));
        }
        path.push(subscript.lower.clone().unwrap_or(Value::Int64(1)));
    }
    runtime.apply_jsonb_subscript_assignment(&current, &path, &replacement)
}

fn assignment_record_value<R: AssignmentRuntime>(
    current: Value,
    sql_type: SqlType,
    runtime: &R,
) -> Result<RecordValue, AssignmentError> {
    match current {
        Value::Record(record) => normalize_assignment_record_value(record, sql_type, runtime),
        Value::Null => {
            let descriptor = runtime.assignment_record_descriptor(sql_type)?;
            Ok(RecordValue::from_descriptor(
                descriptor.clone(),
                vec![Value::Null; descriptor.fields.len()],
            ))
        }
        other => Err(AssignmentError::TypeMismatch {
            op: "record assignment",
            left: other,
            right: Value::Null,
        }),
    }
}

fn normalize_assignment_record_value<R: AssignmentRuntime>(
    record: RecordValue,
    sql_type: SqlType,
    runtime: &R,
) -> Result<RecordValue, AssignmentError> {
    let descriptor = runtime.assignment_record_descriptor(sql_type)?;
    if descriptor.fields == record.descriptor.fields {
        return Ok(record);
    }
    let fields = descriptor
        .fields
        .iter()
        .enumerate()
        .map(|(index, target_field)| {
            record
                .descriptor
                .fields
                .iter()
                .position(|source_field| source_field.name.eq_ignore_ascii_case(&target_field.name))
                .and_then(|source_index| record.fields.get(source_index).cloned())
                .or_else(|| record.fields.get(index).cloned())
                .unwrap_or(Value::Null)
        })
        .collect();
    Ok(RecordValue::from_descriptor(descriptor, fields))
}

fn assign_array_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, AssignmentError> {
    if subscripts.is_empty() {
        return Ok(replacement);
    }
    if subscripts.iter().any(|subscript| subscript.is_slice) {
        return assign_array_slice_value(current, subscripts, replacement);
    }
    let subscript = &subscripts[0];
    let (mut lower_bound, mut items) = assignment_top_level(current)?;
    if subscript.is_slice {
        let Some(start) = assignment_subscript_index(subscript.lower.as_ref())? else {
            return Err(assignment_null_subscript_error());
        };
        let Some(end) = assignment_subscript_index(subscript.upper.as_ref())? else {
            return Err(assignment_null_subscript_error());
        };
        let replacement_items = assignment_replacement_items(replacement.clone())?;
        if items.is_empty() {
            lower_bound = start;
        }
        extend_assignment_items(&mut lower_bound, &mut items, start, end)?;
        let start_idx = usize::try_from(i64::from(start) - i64::from(lower_bound))
            .map_err(|_| array_assignment_limit_error())?;
        let end_idx = usize::try_from(i64::from(end) - i64::from(lower_bound))
            .map_err(|_| array_assignment_limit_error())?;
        let span = end_idx - start_idx + 1;
        if replacement_items.len() != span {
            return Err(AssignmentError::TypeMismatch {
                op: "array slice assignment",
                left: build_assignment_array_value(lower_bound, items.clone())?,
                right: replacement,
            });
        }
        for (idx, item) in replacement_items.into_iter().enumerate() {
            items[start_idx + idx] = if subscripts.len() == 1 {
                item
            } else {
                assign_array_value(items[start_idx + idx].clone(), &subscripts[1..], item)?
            };
        }
        build_assignment_array_value(lower_bound, items)
    } else {
        let Some(index) = assignment_subscript_index(subscript.lower.as_ref())? else {
            return Err(assignment_null_subscript_error());
        };
        if items.is_empty() {
            lower_bound = index;
        }
        extend_assignment_items(&mut lower_bound, &mut items, index, index)?;
        let index = usize::try_from(i64::from(index) - i64::from(lower_bound))
            .map_err(|_| array_assignment_limit_error())?;
        items[index] = if subscripts.len() == 1 {
            replacement
        } else {
            assign_array_value(items[index].clone(), &subscripts[1..], replacement)?
        };
        build_assignment_array_value(lower_bound, items)
    }
}

fn assign_array_slice_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, AssignmentError> {
    if matches!(replacement, Value::Null) {
        return Ok(current);
    }

    let current_array = assignment_current_array(current)?;
    let source_array = assignment_source_array(replacement)?;

    if subscripts.len() > 6 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }

    if current_array.ndim() == 0 {
        return assign_array_slice_into_empty(subscripts, source_array);
    }

    let ndim = current_array.ndim();
    if ndim < subscripts.len() || ndim > 6 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }

    let mut dimensions = current_array.dimensions.clone();
    let mut lower_bounds = Vec::with_capacity(ndim);
    let mut upper_bounds = Vec::with_capacity(ndim);

    for (dim_idx, subscript) in subscripts.iter().enumerate() {
        let dim = &dimensions[dim_idx];
        let lower = resolve_assignment_slice_bound(
            subscript.lower.as_ref(),
            dim.lower_bound,
            subscript.is_slice,
        )?;
        let upper = resolve_assignment_slice_bound(
            if subscript.is_slice {
                subscript.upper.as_ref()
            } else {
                subscript.lower.as_ref()
            },
            checked_array_upper_bound(dim.lower_bound, dim.length)?,
            subscript.is_slice,
        )?;
        if lower > upper {
            return Err(array_assignment_error(
                "upper bound cannot be less than lower bound",
            ));
        }

        if ndim == 1 {
            if lower < dimensions[0].lower_bound {
                let extension =
                    usize::try_from(i64::from(dimensions[0].lower_bound) - i64::from(lower))
                        .map_err(|_| array_assignment_limit_error())?;
                dimensions[0].lower_bound = lower;
                dimensions[0].length = dimensions[0]
                    .length
                    .checked_add(extension)
                    .ok_or_else(array_assignment_limit_error)?;
                dimensions[0].length = checked_array_item_count(dimensions[0].length)?;
            }
            let current_upper =
                checked_array_upper_bound(dimensions[0].lower_bound, dimensions[0].length)?;
            if upper > current_upper {
                let extension = usize::try_from(i64::from(upper) - i64::from(current_upper))
                    .map_err(|_| array_assignment_limit_error())?;
                dimensions[0].length = dimensions[0]
                    .length
                    .checked_add(extension)
                    .ok_or_else(array_assignment_limit_error)?;
                dimensions[0].length = checked_array_item_count(dimensions[0].length)?;
            }
        } else if lower < dim.lower_bound
            || upper > checked_array_upper_bound(dim.lower_bound, dim.length)?
        {
            return Err(array_assignment_error("array subscript out of range"));
        }

        lower_bounds.push(lower);
        upper_bounds.push(upper);
    }

    for dim in dimensions.iter().skip(subscripts.len()) {
        lower_bounds.push(dim.lower_bound);
        upper_bounds.push(checked_array_upper_bound(dim.lower_bound, dim.length)?);
    }

    let span_lengths = lower_bounds
        .iter()
        .zip(upper_bounds.iter())
        .map(|(lower, upper)| checked_array_span_length(*lower, *upper))
        .collect::<Result<Vec<_>, _>>()?;
    let target_items = span_lengths
        .iter()
        .try_fold(1usize, |count, span| count.checked_mul(*span))
        .ok_or_else(array_assignment_limit_error)
        .and_then(checked_array_item_count)?;
    if source_array.elements.len() < target_items {
        return Err(array_assignment_error("source array too small"));
    }

    let element_type_oid = current_array
        .element_type_oid
        .or(source_array.element_type_oid);
    if ndim == 1 {
        let mut elements = vec![Value::Null; dimensions[0].length];
        let original_lower = current_array.lower_bound(0).unwrap_or(1);
        for (idx, value) in current_array.elements.iter().enumerate() {
            let target_idx = usize::try_from(
                i64::from(original_lower)
                    + i64::try_from(idx).map_err(|_| array_assignment_limit_error())?
                    - i64::from(dimensions[0].lower_bound),
            )
            .map_err(|_| array_assignment_limit_error())?;
            elements[target_idx] = value.clone();
        }
        let start_idx =
            usize::try_from(i64::from(lower_bounds[0]) - i64::from(dimensions[0].lower_bound))
                .map_err(|_| array_assignment_limit_error())?;
        for (offset, value) in source_array
            .elements
            .into_iter()
            .take(target_items)
            .enumerate()
        {
            elements[start_idx + offset] = value;
        }
        return Ok(Value::PgArray(array_with_element_type(
            ArrayValue::from_dimensions(dimensions, elements),
            element_type_oid,
        )));
    }

    let mut elements = current_array.elements.clone();
    for (offset, value) in source_array
        .elements
        .into_iter()
        .take(target_items)
        .enumerate()
    {
        let coords = linear_index_to_assignment_coords(offset, &lower_bounds, &span_lengths);
        let target_idx = assignment_coords_to_linear_index(&coords, &dimensions);
        elements[target_idx] = value;
    }
    Ok(Value::PgArray(array_with_element_type(
        ArrayValue::from_dimensions(dimensions, elements),
        element_type_oid,
    )))
}

fn assign_array_slice_into_empty(
    subscripts: &[ResolvedAssignmentSubscript],
    source_array: ArrayValue,
) -> Result<Value, AssignmentError> {
    let mut dimensions = Vec::with_capacity(subscripts.len());
    for subscript in subscripts {
        let Some(lower_value) = subscript.lower.as_ref() else {
            return Err(detailed_error(
                "array slice subscript must provide both boundaries",
                Some(
                    "When assigning to a slice of an empty array value, slice boundaries must be fully specified."
                        .into(),
                ),
                None,
                "2202E",
            ));
        };
        let Some(upper_value) = (if subscript.is_slice {
            subscript.upper.as_ref()
        } else {
            subscript.lower.as_ref()
        }) else {
            return Err(detailed_error(
                "array slice subscript must provide both boundaries",
                Some(
                    "When assigning to a slice of an empty array value, slice boundaries must be fully specified."
                        .into(),
                ),
                None,
                "2202E",
            ));
        };
        let lower = assignment_subscript_index(Some(lower_value))?
            .ok_or_else(assignment_null_subscript_error)?;
        let upper = assignment_subscript_index(Some(upper_value))?
            .ok_or_else(assignment_null_subscript_error)?;
        if lower > upper {
            return Err(array_assignment_error(
                "upper bound cannot be less than lower bound",
            ));
        }
        dimensions.push(ArrayDimension {
            lower_bound: lower,
            length: checked_array_span_length(lower, upper)?,
        });
    }

    let target_items = dimensions
        .iter()
        .try_fold(1usize, |count, dim| count.checked_mul(dim.length))
        .ok_or_else(array_assignment_limit_error)
        .and_then(checked_array_item_count)?;
    if source_array.elements.len() < target_items {
        return Err(array_assignment_error("source array too small"));
    }

    Ok(Value::PgArray(array_with_element_type(
        ArrayValue::from_dimensions(
            dimensions,
            source_array
                .elements
                .into_iter()
                .take(target_items)
                .collect(),
        ),
        source_array.element_type_oid,
    )))
}

fn assignment_current_array(current: Value) -> Result<ArrayValue, AssignmentError> {
    tablecmds::assignment_current_array(current).map_err(AssignmentError::from)
}

fn assignment_source_array(replacement: Value) -> Result<ArrayValue, AssignmentError> {
    tablecmds::assignment_source_array(replacement).map_err(AssignmentError::from)
}

fn resolve_assignment_slice_bound(
    value: Option<&Value>,
    default: i32,
    is_slice: bool,
) -> Result<i32, AssignmentError> {
    match value {
        None if is_slice => Ok(default),
        None => assignment_subscript_index(None)?.ok_or_else(assignment_null_subscript_error),
        Some(_) => assignment_subscript_index(value)?.ok_or_else(assignment_null_subscript_error),
    }
}

fn assignment_null_subscript_error() -> AssignmentError {
    AssignmentError::InvalidStorageValue {
        column: "<array>".into(),
        details: "array subscript in assignment must not be null".into(),
    }
}

fn checked_array_item_count(count: usize) -> Result<usize, AssignmentError> {
    tablecmds::checked_array_item_count(count).map_err(AssignmentError::from)
}

fn checked_array_upper_bound(lower_bound: i32, length: usize) -> Result<i32, AssignmentError> {
    tablecmds::checked_array_upper_bound(lower_bound, length).map_err(AssignmentError::from)
}

fn checked_array_span_length(lower: i32, upper: i32) -> Result<usize, AssignmentError> {
    tablecmds::checked_array_span_length(lower, upper).map_err(AssignmentError::from)
}

fn array_assignment_error(message: &str) -> AssignmentError {
    detailed_error(message.to_string(), None, None, "2202E")
}

fn array_assignment_limit_error() -> AssignmentError {
    AssignmentError::from(tablecmds::array_assignment_limit_error())
}

fn array_with_element_type(array: ArrayValue, element_type_oid: Option<u32>) -> ArrayValue {
    tablecmds::array_with_element_type(array, element_type_oid)
}

fn linear_index_to_assignment_coords(
    offset: usize,
    lower_bounds: &[i32],
    lengths: &[usize],
) -> Vec<i32> {
    tablecmds::linear_index_to_assignment_coords(offset, lower_bounds, lengths)
}

fn assignment_coords_to_linear_index(coords: &[i32], dimensions: &[ArrayDimension]) -> usize {
    tablecmds::assignment_coords_to_linear_index(coords, dimensions)
}

fn assignment_top_level(current: Value) -> Result<(i32, Vec<Value>), AssignmentError> {
    tablecmds::assignment_top_level(current).map_err(AssignmentError::from)
}

fn assignment_replacement_items(replacement: Value) -> Result<Vec<Value>, AssignmentError> {
    tablecmds::assignment_replacement_items(replacement).map_err(AssignmentError::from)
}

fn extend_assignment_items(
    lower_bound: &mut i32,
    items: &mut Vec<Value>,
    start: i32,
    end: i32,
) -> Result<(), AssignmentError> {
    tablecmds::extend_assignment_items(lower_bound, items, start, end)
        .map_err(AssignmentError::from)
}

fn build_assignment_array_value(
    lower_bound: i32,
    items: Vec<Value>,
) -> Result<Value, AssignmentError> {
    tablecmds::build_assignment_array_value(lower_bound, items).map_err(AssignmentError::from)
}

fn assignment_subscript_index(value: Option<&Value>) -> Result<Option<i32>, AssignmentError> {
    tablecmds::assignment_subscript_index(value).map_err(|err| match err {
        TableCmdsError::Detailed {
            sqlstate: "22003", ..
        } => AssignmentError::Int4OutOfRange,
        other => AssignmentError::from(other),
    })
}

fn detailed_error(
    message: impl Into<String>,
    detail: Option<String>,
    hint: Option<String>,
    sqlstate: &'static str,
) -> AssignmentError {
    AssignmentError::TableCmds(TableCmdsError::Detailed {
        message: message.into(),
        detail,
        hint,
        sqlstate,
    })
}
