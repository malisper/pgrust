use serde::{Deserialize, Serialize};

use super::collation::default_collation_oid_for_type;
use super::{
    BoundRelation, CatalogLookup, CreateTableStatement, IndexBackedConstraintAction, ParseError,
    PartitionStrategy, RawPartitionBoundSpec, RawPartitionKey, RawPartitionRangeDatum,
    RawPartitionSpec, SqlType, TablePersistence, bind_scalar_expr_in_scope, sql_type_name,
};
use crate::backend::executor::{Value, cast_value};
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{
    PgPartitionedTableRow, RANGE_GIST_OPCLASS_OID, builtin_range_spec_by_multirange_oid,
    builtin_range_spec_by_oid, default_btree_opclass_oid, default_hash_opclass_oid,
    range_type_ref_for_sql_type,
};
use crate::include::nodes::datum::{MultirangeTypeRef, MultirangeValue, RangeBound, RangeValue};
use crate::include::nodes::primnodes::Expr;
use crate::include::nodes::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredPartitionSpec {
    pub strategy: PartitionStrategy,
    pub key_columns: Vec<String>,
    pub partattrs: Vec<i16>,
    pub partclass: Vec<u32>,
    pub partcollation: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredPartitionClause {
    pub parent_oid: Option<u32>,
    pub spec: Option<LoweredPartitionSpec>,
    pub bound: Option<PartitionBoundSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerializedPartitionValue {
    Null,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Money(i64),
    Float64(String),
    Numeric(String),
    Text(String),
    Bytea(Vec<u8>),
    Json(String),
    Jsonb(Vec<u8>),
    JsonPath(String),
    Xml(String),
    InternalChar(u8),
    Bool(bool),
    Date(i32),
    Time(i64),
    TimeTz { time: i64, offset_seconds: i32 },
    Timestamp(i64),
    TimestampTz(i64),
    Range(Box<SerializedPartitionRangeValue>),
    Multirange(Box<SerializedPartitionMultirangeValue>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionRangeBound {
    pub value: SerializedPartitionValue,
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionRangeValue {
    pub range_type_oid: u32,
    pub empty: bool,
    pub lower: Option<SerializedPartitionRangeBound>,
    pub upper: Option<SerializedPartitionRangeBound>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionMultirangeValue {
    pub multirange_type_oid: u32,
    pub ranges: Vec<SerializedPartitionRangeValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionRangeDatumValue {
    MinValue,
    MaxValue,
    Value(SerializedPartitionValue),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionBoundSpec {
    List {
        values: Vec<SerializedPartitionValue>,
        is_default: bool,
    },
    Range {
        from: Vec<PartitionRangeDatumValue>,
        to: Vec<PartitionRangeDatumValue>,
        is_default: bool,
    },
    Hash {
        modulus: i32,
        remainder: i32,
    },
}

impl PartitionBoundSpec {
    pub fn is_default(&self) -> bool {
        match self {
            Self::List { is_default, .. } | Self::Range { is_default, .. } => *is_default,
            Self::Hash { .. } => false,
        }
    }
}

pub(crate) fn lower_partition_clause(
    stmt: &CreateTableStatement,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    persistence: TablePersistence,
) -> Result<LoweredPartitionClause, ParseError> {
    if stmt.partition_spec.is_none()
        && stmt.partition_of.is_none()
        && stmt.partition_bound.is_none()
    {
        return Ok(LoweredPartitionClause {
            parent_oid: None,
            spec: None,
            bound: None,
        });
    }

    if !stmt.inherits.is_empty() {
        return Err(ParseError::InvalidTableDefinition(
            "cannot mix INHERITS with partition syntax".into(),
        ));
    }

    let spec = stmt
        .partition_spec
        .as_ref()
        .map(|spec| lower_partition_spec(spec, relation_desc))
        .transpose()?;

    let Some(parent_name) = stmt.partition_of.as_deref() else {
        if stmt.partition_bound.is_some() {
            return Err(ParseError::InvalidTableDefinition(
                "partition bound specified without PARTITION OF".into(),
            ));
        }
        return Ok(LoweredPartitionClause {
            parent_oid: None,
            spec,
            bound: None,
        });
    };

    let parent = catalog
        .lookup_any_relation(parent_name)
        .ok_or_else(|| ParseError::UnknownTable(parent_name.to_string()))?;
    if parent.relkind != 'p' {
        return Err(ParseError::WrongObjectType {
            name: parent_name.to_string(),
            expected: "partitioned table",
        });
    }
    if relation_persistence_code(persistence) != parent.relpersistence {
        return Err(ParseError::DetailedError {
            message: format!(
                "partition \"{}\" would have different persistence than partitioned table \"{}\"",
                stmt.table_name, parent_name
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    ensure_matching_partition_shape(&stmt.table_name, relation_desc, &parent)?;

    let parent_spec = relation_partition_spec(&parent)?;
    let raw_bound = stmt.partition_bound.as_ref().ok_or_else(|| {
        ParseError::InvalidTableDefinition(format!(
            "partition \"{}\" must specify FOR VALUES",
            stmt.table_name
        ))
    })?;
    let bound = lower_partition_bound(raw_bound, &parent_spec, &parent.desc, catalog)?;

    Ok(LoweredPartitionClause {
        parent_oid: Some(parent.relation_oid),
        spec,
        bound: Some(bound),
    })
}

pub(crate) fn validate_partitioned_index_backed_constraints(
    relation_name: &str,
    partition_spec: Option<&LoweredPartitionSpec>,
    constraint_actions: &[IndexBackedConstraintAction],
) -> Result<(), ParseError> {
    let Some(partition_spec) = partition_spec else {
        return Ok(());
    };
    for action in constraint_actions {
        let normalized_columns = action
            .columns
            .iter()
            .map(|column| column.to_ascii_lowercase())
            .collect::<Vec<_>>();
        for key_column in &partition_spec.key_columns {
            if normalized_columns.contains(&key_column.to_ascii_lowercase()) {
                continue;
            }
            let constraint_kind = if action.primary {
                "PRIMARY KEY"
            } else {
                "UNIQUE"
            };
            return Err(ParseError::DetailedError {
                message:
                    "unique constraint on partitioned table must include all partitioning columns"
                        .into(),
                detail: Some(format!(
                    "{constraint_kind} constraint on table \"{relation_name}\" lacks column \"{key_column}\" which is part of the partition key."
                )),
                hint: None,
                sqlstate: "0A000",
            });
        }
    }
    Ok(())
}

pub(crate) fn relation_partition_spec(
    relation: &BoundRelation,
) -> Result<LoweredPartitionSpec, ParseError> {
    let row = relation.partitioned_table.as_ref().ok_or_else(|| {
        ParseError::InvalidTableDefinition(format!(
            "relation \"{}\" is not declaratively partitioned",
            relation.relation_oid
        ))
    })?;
    let strategy = match row.partstrat {
        'l' => PartitionStrategy::List,
        'r' => PartitionStrategy::Range,
        'h' => PartitionStrategy::Hash,
        _ => {
            return Err(ParseError::InvalidTableDefinition(
                "invalid partition strategy".into(),
            ));
        }
    };
    let mut key_columns = Vec::with_capacity(row.partattrs.len());
    for attnum in &row.partattrs {
        let Some(column) = relation
            .desc
            .columns
            .get(attnum.saturating_sub(1) as usize)
            .filter(|column| !column.dropped)
        else {
            return Err(ParseError::UnexpectedToken {
                expected: "partition key column",
                actual: format!("missing partition key attribute {}", attnum),
            });
        };
        key_columns.push(column.name.clone());
    }
    Ok(LoweredPartitionSpec {
        strategy,
        key_columns,
        partattrs: row.partattrs.clone(),
        partclass: row.partclass.clone(),
        partcollation: row.partcollation.clone(),
    })
}

pub(crate) fn lower_partition_bound_for_relation(
    relation: &BoundRelation,
    bound: &RawPartitionBoundSpec,
    catalog: &dyn CatalogLookup,
) -> Result<PartitionBoundSpec, ParseError> {
    let spec = relation_partition_spec(relation)?;
    lower_partition_bound(bound, &spec, &relation.desc, catalog)
}

pub(crate) fn pg_partitioned_table_row(
    relation_oid: u32,
    spec: &LoweredPartitionSpec,
    partdefid: u32,
) -> PgPartitionedTableRow {
    PgPartitionedTableRow {
        partrelid: relation_oid,
        partstrat: spec.strategy.catalog_code(),
        partnatts: spec.partattrs.len() as i16,
        partdefid,
        partattrs: spec.partattrs.clone(),
        partclass: spec.partclass.clone(),
        partcollation: spec.partcollation.clone(),
        partexprs: None,
    }
}

pub(crate) fn serialize_partition_bound(bound: &PartitionBoundSpec) -> Result<String, ParseError> {
    serde_json::to_string(bound).map_err(|_| ParseError::UnexpectedToken {
        expected: "partition bound metadata",
        actual: "invalid partition bound".into(),
    })
}

pub(crate) fn deserialize_partition_bound(text: &str) -> Result<PartitionBoundSpec, ParseError> {
    serde_json::from_str(text).map_err(|_| ParseError::UnexpectedToken {
        expected: "serialized partition bound metadata",
        actual: text.to_string(),
    })
}

pub(crate) fn partition_value_to_value(value: &SerializedPartitionValue) -> Value {
    match value {
        SerializedPartitionValue::Null => Value::Null,
        SerializedPartitionValue::Int16(v) => Value::Int16(*v),
        SerializedPartitionValue::Int32(v) => Value::Int32(*v),
        SerializedPartitionValue::Int64(v) => Value::Int64(*v),
        SerializedPartitionValue::Money(v) => Value::Money(*v),
        SerializedPartitionValue::Float64(v) => Value::Float64(
            v.parse::<f64>()
                .expect("serialized partition float must parse"),
        ),
        SerializedPartitionValue::Numeric(v) => Value::Numeric(
            crate::backend::executor::exec_expr::parse_numeric_text(v)
                .expect("serialized partition numeric must parse"),
        ),
        SerializedPartitionValue::Text(v) => Value::Text(v.clone().into()),
        SerializedPartitionValue::Bytea(v) => Value::Bytea(v.clone()),
        SerializedPartitionValue::Json(v) => Value::Json(v.clone().into()),
        SerializedPartitionValue::Jsonb(v) => Value::Jsonb(v.clone()),
        SerializedPartitionValue::JsonPath(v) => Value::JsonPath(v.clone().into()),
        SerializedPartitionValue::Xml(v) => Value::Xml(v.clone().into()),
        SerializedPartitionValue::InternalChar(v) => Value::InternalChar(*v),
        SerializedPartitionValue::Bool(v) => Value::Bool(*v),
        SerializedPartitionValue::Date(v) => {
            Value::Date(crate::include::nodes::datetime::DateADT(*v))
        }
        SerializedPartitionValue::Time(v) => {
            Value::Time(crate::include::nodes::datetime::TimeADT(*v))
        }
        SerializedPartitionValue::TimeTz {
            time,
            offset_seconds,
        } => Value::TimeTz(crate::include::nodes::datetime::TimeTzADT {
            time: crate::include::nodes::datetime::TimeADT(*time),
            offset_seconds: *offset_seconds,
        }),
        SerializedPartitionValue::Timestamp(v) => {
            Value::Timestamp(crate::include::nodes::datetime::TimestampADT(*v))
        }
        SerializedPartitionValue::TimestampTz(v) => {
            Value::TimestampTz(crate::include::nodes::datetime::TimestampTzADT(*v))
        }
        SerializedPartitionValue::Range(range) => deserialize_partition_range_value(range),
        SerializedPartitionValue::Multirange(multirange) => {
            deserialize_partition_multirange_value(multirange)
        }
    }
}

pub(crate) fn value_to_partition_value(
    value: &Value,
) -> Result<SerializedPartitionValue, ParseError> {
    Ok(match value {
        Value::Null => SerializedPartitionValue::Null,
        Value::Int16(v) => SerializedPartitionValue::Int16(*v),
        Value::Int32(v) => SerializedPartitionValue::Int32(*v),
        Value::Int64(v) => SerializedPartitionValue::Int64(*v),
        Value::Money(v) => SerializedPartitionValue::Money(*v),
        Value::Float64(v) => SerializedPartitionValue::Float64(v.to_string()),
        Value::Numeric(v) => SerializedPartitionValue::Numeric(v.render()),
        Value::Text(v) => SerializedPartitionValue::Text(v.to_string()),
        Value::TextRef(_, _) => {
            SerializedPartitionValue::Text(value.as_text().unwrap_or_default().to_string())
        }
        Value::Bytea(v) => SerializedPartitionValue::Bytea(v.clone()),
        Value::Json(v) => SerializedPartitionValue::Json(v.to_string()),
        Value::Jsonb(v) => SerializedPartitionValue::Jsonb(v.clone()),
        Value::JsonPath(v) => SerializedPartitionValue::JsonPath(v.to_string()),
        Value::Xml(v) => SerializedPartitionValue::Xml(v.to_string()),
        Value::InternalChar(v) => SerializedPartitionValue::InternalChar(*v),
        Value::Bool(v) => SerializedPartitionValue::Bool(*v),
        Value::Date(v) => SerializedPartitionValue::Date(v.0),
        Value::Time(v) => SerializedPartitionValue::Time(v.0),
        Value::TimeTz(v) => SerializedPartitionValue::TimeTz {
            time: v.time.0,
            offset_seconds: v.offset_seconds,
        },
        Value::Timestamp(v) => SerializedPartitionValue::Timestamp(v.0),
        Value::TimestampTz(v) => SerializedPartitionValue::TimestampTz(v.0),
        Value::Range(range) => {
            SerializedPartitionValue::Range(Box::new(serialize_partition_range_value(range)?))
        }
        Value::Multirange(multirange) => SerializedPartitionValue::Multirange(Box::new(
            serialize_partition_multirange_value(multirange)?,
        )),
        other => {
            return Err(ParseError::FeatureNotSupported(format!(
                "partition key type {:?}",
                other
                    .sql_type_hint()
                    .unwrap_or(SqlType::new(crate::backend::parser::SqlTypeKind::Text))
            )));
        }
    })
}

fn serialize_partition_range_bound(
    bound: &RangeBound,
) -> Result<SerializedPartitionRangeBound, ParseError> {
    Ok(SerializedPartitionRangeBound {
        value: value_to_partition_value(&bound.value)?,
        inclusive: bound.inclusive,
    })
}

fn serialize_partition_range_value(
    range: &RangeValue,
) -> Result<SerializedPartitionRangeValue, ParseError> {
    if builtin_range_spec_by_oid(range.range_type.type_oid()).is_none() {
        return Err(ParseError::FeatureNotSupported(
            "partition key type custom range".into(),
        ));
    }
    Ok(SerializedPartitionRangeValue {
        range_type_oid: range.range_type.type_oid(),
        empty: range.empty,
        lower: range
            .lower
            .as_ref()
            .map(serialize_partition_range_bound)
            .transpose()?,
        upper: range
            .upper
            .as_ref()
            .map(serialize_partition_range_bound)
            .transpose()?,
    })
}

fn serialize_partition_multirange_value(
    multirange: &MultirangeValue,
) -> Result<SerializedPartitionMultirangeValue, ParseError> {
    if builtin_range_spec_by_multirange_oid(multirange.multirange_type.type_oid()).is_none() {
        return Err(ParseError::FeatureNotSupported(
            "partition key type custom multirange".into(),
        ));
    }
    Ok(SerializedPartitionMultirangeValue {
        multirange_type_oid: multirange.multirange_type.type_oid(),
        ranges: multirange
            .ranges
            .iter()
            .map(serialize_partition_range_value)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn deserialize_partition_range_bound(bound: &SerializedPartitionRangeBound) -> RangeBound {
    RangeBound {
        value: Box::new(partition_value_to_value(&bound.value)),
        inclusive: bound.inclusive,
    }
}

fn deserialize_partition_range_value(range: &SerializedPartitionRangeValue) -> Value {
    let Some(spec) = builtin_range_spec_by_oid(range.range_type_oid) else {
        return Value::Null;
    };
    Value::Range(RangeValue {
        range_type: spec.range_type,
        empty: range.empty,
        lower: range.lower.as_ref().map(deserialize_partition_range_bound),
        upper: range.upper.as_ref().map(deserialize_partition_range_bound),
    })
}

fn deserialize_partition_multirange_value(
    multirange: &SerializedPartitionMultirangeValue,
) -> Value {
    let Some(spec) = builtin_range_spec_by_multirange_oid(multirange.multirange_type_oid) else {
        return Value::Null;
    };
    let multirange_type = MultirangeTypeRef {
        sql_type: SqlType::multirange(spec.multirange_oid, spec.oid)
            .with_identity(spec.multirange_oid, 0),
        range_type: spec.range_type,
    };
    let ranges = multirange
        .ranges
        .iter()
        .filter_map(|range| match deserialize_partition_range_value(range) {
            Value::Range(range) => Some(range),
            _ => None,
        })
        .collect();
    Value::Multirange(MultirangeValue {
        multirange_type,
        ranges,
    })
}

fn lower_partition_spec(
    spec: &RawPartitionSpec,
    relation_desc: &RelationDesc,
) -> Result<LoweredPartitionSpec, ParseError> {
    if spec.keys.is_empty() {
        return Err(ParseError::InvalidTableDefinition(
            "partition key must contain at least one column".into(),
        ));
    }
    if spec.strategy == PartitionStrategy::List && spec.keys.len() != 1 {
        return Err(ParseError::DetailedError {
            message: "cannot use list partition strategy with more than one column".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    let mut key_columns = Vec::with_capacity(spec.keys.len());
    let mut partattrs = Vec::with_capacity(spec.keys.len());
    let mut partclass = Vec::with_capacity(spec.keys.len());
    let mut partcollation = Vec::with_capacity(spec.keys.len());

    for key in &spec.keys {
        let RawPartitionKey::Column(name) = key;
        if is_system_column_name(name) {
            return Err(ParseError::InvalidTableDefinition(format!(
                "cannot use system column \"{}\" in partition key",
                name
            )));
        }
        let Some((index, column)) = relation_desc
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| !column.dropped && column.name.eq_ignore_ascii_case(name))
        else {
            return Err(ParseError::UnknownColumn(name.clone()));
        };
        let type_oid = sql_type_oid(column.sql_type);
        let opclass =
            default_opclass_for_partition_strategy(spec.strategy, type_oid, column.sql_type)
                .ok_or_else(|| {
                    ParseError::FeatureNotSupported(format!(
                        "partition key type {}",
                        sql_type_name(column.sql_type)
                    ))
                })?;
        key_columns.push(column.name.clone());
        partattrs.push(index as i16 + 1);
        partclass.push(opclass);
        partcollation.push(default_collation_oid_for_type(column.sql_type).unwrap_or(0));
    }

    Ok(LoweredPartitionSpec {
        strategy: spec.strategy,
        key_columns,
        partattrs,
        partclass,
        partcollation,
    })
}

fn lower_partition_bound(
    bound: &RawPartitionBoundSpec,
    parent_spec: &LoweredPartitionSpec,
    parent_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<PartitionBoundSpec, ParseError> {
    match (bound, parent_spec.strategy) {
        (RawPartitionBoundSpec::List { values, is_default }, PartitionStrategy::List) => {
            let key_type = parent_spec_key_types(parent_spec, parent_desc)
                .into_iter()
                .next()
                .ok_or_else(|| {
                    ParseError::InvalidTableDefinition("missing list partition key".into())
                })?;
            let lowered = values
                .iter()
                .map(|expr| evaluate_partition_bound_expr(expr, key_type, catalog))
                .map(|result| result.and_then(|value| value_to_partition_value(&value)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PartitionBoundSpec::List {
                values: lowered,
                is_default: *is_default,
            })
        }
        (
            RawPartitionBoundSpec::Range {
                from,
                to,
                is_default,
            },
            PartitionStrategy::Range,
        ) => {
            let key_types = parent_spec_key_types(parent_spec, parent_desc);
            if !*is_default && (from.len() != key_types.len() || to.len() != key_types.len()) {
                return Err(ParseError::InvalidTableDefinition(
                    "range partition bound arity does not match partition key".into(),
                ));
            }
            let from = lower_range_datums(from, &key_types, catalog)?;
            let to = lower_range_datums(to, &key_types, catalog)?;
            Ok(PartitionBoundSpec::Range {
                from,
                to,
                is_default: *is_default,
            })
        }
        (RawPartitionBoundSpec::Hash { modulus, remainder }, PartitionStrategy::Hash) => {
            validate_hash_partition_bound(*modulus, *remainder)?;
            Ok(PartitionBoundSpec::Hash {
                modulus: *modulus,
                remainder: *remainder,
            })
        }
        (
            RawPartitionBoundSpec::List {
                is_default: true, ..
            },
            PartitionStrategy::Hash,
        )
        | (
            RawPartitionBoundSpec::Range {
                is_default: true, ..
            },
            PartitionStrategy::Hash,
        ) => Err(ParseError::DetailedError {
            message: "a hash-partitioned table may not have a default partition".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        }),
        (RawPartitionBoundSpec::List { .. }, _)
        | (RawPartitionBoundSpec::Range { .. }, _)
        | (RawPartitionBoundSpec::Hash { .. }, _) => Err(ParseError::InvalidTableDefinition(
            "partition bound does not match parent partition strategy".into(),
        )),
    }
}

fn default_opclass_for_partition_strategy(
    strategy: PartitionStrategy,
    type_oid: u32,
    sql_type: SqlType,
) -> Option<u32> {
    match strategy {
        PartitionStrategy::List | PartitionStrategy::Range => default_btree_opclass_oid(type_oid)
            .or_else(|| range_type_ref_for_sql_type(sql_type).map(|_| RANGE_GIST_OPCLASS_OID)),
        PartitionStrategy::Hash => default_hash_opclass_oid(type_oid),
    }
}

fn validate_hash_partition_bound(modulus: i32, remainder: i32) -> Result<(), ParseError> {
    if modulus <= 0 {
        return Err(ParseError::DetailedError {
            message: "modulus for hash partition must be a positive integer".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if remainder < 0 {
        return Err(ParseError::DetailedError {
            message: "remainder for hash partition must be a non-negative integer".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if remainder >= modulus {
        return Err(ParseError::DetailedError {
            message: "remainder for hash partition must be less than modulus".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    Ok(())
}

fn lower_range_datums(
    values: &[RawPartitionRangeDatum],
    key_types: &[SqlType],
    catalog: &dyn CatalogLookup,
) -> Result<Vec<PartitionRangeDatumValue>, ParseError> {
    values
        .iter()
        .zip(key_types.iter())
        .map(|(value, key_type)| match value {
            RawPartitionRangeDatum::MinValue => Ok(PartitionRangeDatumValue::MinValue),
            RawPartitionRangeDatum::MaxValue => Ok(PartitionRangeDatumValue::MaxValue),
            RawPartitionRangeDatum::Value(expr) => {
                evaluate_partition_bound_expr(expr, *key_type, catalog)
                    .and_then(|value| value_to_partition_value(&value))
                    .map(PartitionRangeDatumValue::Value)
            }
        })
        .collect()
}

fn parent_spec_key_types(spec: &LoweredPartitionSpec, desc: &RelationDesc) -> Vec<SqlType> {
    spec.partattrs
        .iter()
        .filter_map(|attnum| desc.columns.get(attnum.saturating_sub(1) as usize))
        .map(|column| column.sql_type)
        .collect()
}

fn evaluate_partition_bound_expr(
    expr: &crate::backend::parser::SqlExpr,
    target: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Value, ParseError> {
    let (bound, _from_type) = bind_scalar_expr_in_scope(expr, &[], catalog)?;
    let Expr::Const(value) = bound else {
        return Err(ParseError::InvalidTableDefinition(
            "partition bound values must be constant".into(),
        ));
    };
    cast_value(value, target).map_err(|_| {
        ParseError::InvalidTableDefinition(
            "partition bound value does not match partition key type".into(),
        )
    })
}

fn ensure_matching_partition_shape(
    relation_name: &str,
    relation_desc: &RelationDesc,
    parent: &BoundRelation,
) -> Result<(), ParseError> {
    let child_columns = relation_desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    let parent_columns = parent
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    if child_columns.len() != parent_columns.len() {
        return Err(ParseError::DetailedError {
            message: format!(
                "partition \"{}\" has different column count than partitioned table \"{}\"",
                relation_name, parent.relation_oid
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    for (child, parent_column) in child_columns.iter().zip(parent_columns.iter()) {
        if !child.name.eq_ignore_ascii_case(&parent_column.name)
            || child.sql_type != parent_column.sql_type
        {
            return Err(ParseError::DetailedError {
                message: format!(
                    "partition \"{}\" has different column layout than partitioned table",
                    relation_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
    }
    Ok(())
}

fn relation_persistence_code(persistence: TablePersistence) -> char {
    match persistence {
        TablePersistence::Permanent => 'p',
        TablePersistence::Temporary => 't',
    }
}

fn is_system_column_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    )
}
