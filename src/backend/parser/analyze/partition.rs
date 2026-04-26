use serde::{Deserialize, Serialize};

use super::collation::{default_collation_oid_for_type, strip_explicit_collation};
use super::{
    BoundRelation, CatalogLookup, CheckConstraintAction, CreateTableStatement,
    IndexBackedConstraintAction, ParseError, PartitionStrategy, RawPartitionBoundSpec,
    RawPartitionRangeDatum, RawPartitionSpec, SqlExpr, SqlType, SqlTypeKind, TablePersistence,
    bind_expr_with_outer_and_ctes, bind_scalar_expr_in_scope, expr_contains_set_returning,
    infer_sql_expr_type, scope_for_relation, sql_type_name,
};
use crate::backend::executor::{Value, cast_value};
use crate::backend::parser::parse_expr;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{
    ANYARRAYOID, ANYMULTIRANGEOID, ARRAY_BTREE_OPCLASS_OID, BPCHAR_TYPE_OID, BTREE_AM_OID,
    HASH_AM_OID, INT4_TYPE_OID, OID_TYPE_OID, PgPartitionedTableRow, TEXT_TYPE_OID,
    VARCHAR_TYPE_OID, builtin_range_spec_by_multirange_oid, builtin_range_spec_by_oid,
    default_btree_opclass_oid, default_hash_opclass_oid,
};
use crate::include::nodes::datum::{MultirangeTypeRef, MultirangeValue, RangeBound, RangeValue};
use crate::include::nodes::primnodes::{
    Expr, FuncExpr, RelationDesc, ScalarFunctionImpl, Var, attrno_index, user_attrno,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredPartitionSpec {
    pub strategy: PartitionStrategy,
    pub key_columns: Vec<String>,
    pub key_exprs: Vec<Expr>,
    pub key_types: Vec<SqlType>,
    pub key_sqls: Vec<String>,
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
        return Err(ParseError::DetailedError {
            message: if stmt.partition_spec.is_some() && stmt.partition_of.is_none() {
                "cannot create partitioned table as inheritance child".into()
            } else {
                "cannot mix INHERITS with partition syntax".into()
            },
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }

    let spec = stmt
        .partition_spec
        .as_ref()
        .map(|spec| lower_partition_spec(spec, relation_desc, catalog))
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
        return Err(ParseError::DetailedError {
            message: format!("\"{parent_name}\" is not partitioned"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    let child_persistence = relation_persistence_code(persistence);
    if child_persistence != parent.relpersistence {
        return Err(ParseError::DetailedError {
            message: partition_persistence_error(child_persistence, parent.relpersistence, parent_name)
                .unwrap_or_else(|| {
                    format!(
                        "partition \"{}\" would have different persistence than partitioned table \"{}\"",
                        stmt.table_name, parent_name
                    )
                }),
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
    if partition_spec.partattrs.iter().any(|attnum| *attnum == 0) && !constraint_actions.is_empty()
    {
        return Err(ParseError::DetailedError {
            message: "unsupported UNIQUE constraint with partition key expressions".into(),
            detail: Some(format!(
                "Table \"{relation_name}\" uses an expression in the partition key."
            )),
            hint: None,
            sqlstate: "0A000",
        });
    }
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

pub(crate) fn validate_partitioned_check_constraints(
    relation_name: &str,
    partition_spec: Option<&LoweredPartitionSpec>,
    check_actions: &[CheckConstraintAction],
) -> Result<(), ParseError> {
    if partition_spec.is_none() {
        return Ok(());
    }
    if check_actions.iter().any(|action| action.no_inherit) {
        return Err(ParseError::DetailedError {
            message: format!(
                "cannot add NO INHERIT constraint to partitioned table \"{relation_name}\""
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
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
    let serialized_exprs =
        deserialize_partition_exprs(row.partexprs.as_deref(), row.partattrs.len())?;
    let scope = scope_for_relation(None, &relation.desc);
    let catalog = super::LiteralDefaultCatalog;
    let mut key_columns = Vec::with_capacity(row.partattrs.len());
    let mut key_exprs = Vec::with_capacity(row.partattrs.len());
    let mut key_types = Vec::with_capacity(row.partattrs.len());
    let mut key_sqls = Vec::with_capacity(row.partattrs.len());
    for (index, attnum) in row.partattrs.iter().copied().enumerate() {
        if attnum == 0 {
            let Some(expr_sql) = serialized_exprs.get(index).and_then(Option::as_deref) else {
                return Err(ParseError::UnexpectedToken {
                    expected: "partition key expression",
                    actual: "missing partition key expression metadata".into(),
                });
            };
            let raw = parse_expr(expr_sql)?;
            let bound = bind_expr_with_outer_and_ctes(&raw, &scope, &catalog, &[], None, &[])?;
            let key_type = infer_sql_expr_type(&raw, &scope, &catalog, &[], None);
            let (bound, _) = strip_explicit_collation(bound);
            key_exprs.push(bound);
            key_types.push(key_type);
            key_sqls.push(expr_sql.to_string());
            continue;
        }
        let Some(column_index) = attrno_index(attnum.into()) else {
            return Err(ParseError::UnexpectedToken {
                expected: "partition key column",
                actual: format!("invalid partition key attribute {}", attnum),
            });
        };
        let Some(column) = relation
            .desc
            .columns
            .get(column_index)
            .filter(|column| !column.dropped)
        else {
            return Err(ParseError::UnexpectedToken {
                expected: "partition key column",
                actual: format!("missing partition key attribute {}", attnum),
            });
        };
        key_columns.push(column.name.clone());
        key_exprs.push(Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(column_index),
            varlevelsup: 0,
            vartype: column.sql_type,
        }));
        key_types.push(column.sql_type);
        key_sqls.push(column.name.clone());
    }
    Ok(LoweredPartitionSpec {
        strategy,
        key_columns,
        key_exprs,
        key_types,
        key_sqls,
        partattrs: row.partattrs.clone(),
        partclass: row.partclass.clone(),
        partcollation: row.partcollation.clone(),
    })
}

fn deserialize_partition_exprs(
    partexprs: Option<&str>,
    key_count: usize,
) -> Result<Vec<Option<String>>, ParseError> {
    match partexprs {
        Some(text) => serde_json::from_str(text).map_err(|_| ParseError::UnexpectedToken {
            expected: "serialized partition expression metadata",
            actual: text.to_string(),
        }),
        None => Ok(vec![None; key_count]),
    }
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
    let partexprs =
        serialize_partition_exprs(spec).expect("partition expression metadata must serialize");
    PgPartitionedTableRow {
        partrelid: relation_oid,
        partstrat: spec.strategy.catalog_code(),
        partnatts: spec.partattrs.len() as i16,
        partdefid,
        partattrs: spec.partattrs.clone(),
        partclass: spec.partclass.clone(),
        partcollation: spec.partcollation.clone(),
        partexprs,
    }
}

fn serialize_partition_exprs(spec: &LoweredPartitionSpec) -> Result<Option<String>, ParseError> {
    if !spec.partattrs.iter().any(|attnum| *attnum == 0) {
        return Ok(None);
    }
    let exprs = spec
        .partattrs
        .iter()
        .zip(spec.key_sqls.iter())
        .map(|(attnum, expr_sql)| (*attnum == 0).then_some(expr_sql.clone()))
        .collect::<Vec<_>>();
    serde_json::to_string(&exprs)
        .map(Some)
        .map_err(|_| ParseError::UnexpectedToken {
            expected: "partition expression metadata",
            actual: "invalid partition expressions".into(),
        })
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
    catalog: &dyn CatalogLookup,
) -> Result<LoweredPartitionSpec, ParseError> {
    if spec.keys.is_empty() {
        return Err(ParseError::InvalidTableDefinition(
            "partition key must contain at least one column".into(),
        ));
    }
    if spec.strategy == PartitionStrategy::List && spec.keys.len() != 1 {
        return Err(ParseError::DetailedError {
            message: "cannot use \"list\" partition strategy with more than one column".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    let mut key_columns = Vec::with_capacity(spec.keys.len());
    let mut key_exprs = Vec::with_capacity(spec.keys.len());
    let mut key_types = Vec::with_capacity(spec.keys.len());
    let mut key_sqls = Vec::with_capacity(spec.keys.len());
    let mut partattrs = Vec::with_capacity(spec.keys.len());
    let mut partclass = Vec::with_capacity(spec.keys.len());
    let mut partcollation = Vec::with_capacity(spec.keys.len());
    let scope = scope_for_relation(None, relation_desc);

    for key in &spec.keys {
        if let Some(column_name) = simple_partition_key_column_name(&key.expr) {
            let normalized = column_name.to_ascii_lowercase();
            if is_system_column_name(&normalized) {
                return Err(ParseError::DetailedError {
                    message: format!("cannot use system column \"{column_name}\" in partition key"),
                    detail: None,
                    hint: None,
                    sqlstate: "42P16",
                });
            }
            if !relation_desc
                .columns
                .iter()
                .any(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
            {
                return Err(ParseError::DetailedError {
                    message: format!(
                        "column \"{column_name}\" named in partition key does not exist"
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42703",
                });
            }
        }
        validate_partition_key_raw_expr(&key.expr, catalog)?;
        let bound = bind_expr_with_outer_and_ctes(&key.expr, &scope, catalog, &[], None, &[])?;
        if expr_contains_set_returning(&bound) {
            return Err(partition_key_error(
                "set-returning functions are not allowed in partition key expressions",
            ));
        }
        let key_type = infer_sql_expr_type(&key.expr, &scope, catalog, &[], None);
        let (bound, explicit_collation_oid) = strip_explicit_collation(bound);
        let simple_column_index = match &bound {
            Expr::Var(var) if var.varlevelsup == 0 && var.varno == 1 && var.varattno > 0 => {
                attrno_index(var.varattno)
            }
            _ => None,
        };
        if let Some(index) = simple_column_index
            && is_system_column_name(&relation_desc.columns[index].name)
        {
            return Err(partition_key_error(format!(
                "cannot use system column \"{}\" in partition key",
                relation_desc.columns[index].name
            )));
        }
        if let Some(pseudo_name) = partition_key_pseudotype_name(key_type, &key.expr) {
            return Err(partition_key_error(format!(
                "partition key column {} has pseudo-type {pseudo_name}",
                key_exprs.len() + 1
            )));
        }
        if partition_expr_is_mutable(&bound, catalog) {
            return Err(partition_key_error(
                "functions in partition key expression must be marked IMMUTABLE",
            ));
        }
        let planned = crate::backend::optimizer::fold_expr_constants(bound.clone())?;
        if matches!(planned, Expr::Const(_)) || !expr_contains_var(&bound) {
            return Err(partition_key_error(
                "cannot use constant expression as partition key",
            ));
        }
        let key_collation_oid = if let Some(collation_oid) = explicit_collation_oid {
            collation_oid
        } else if let Some(index) = simple_column_index {
            relation_desc.columns[index].collation_oid
        } else {
            default_collation_oid_for_type(key_type).unwrap_or(0)
        };
        let type_oid = sql_type_oid(key_type);
        let opclass = partition_opclass_for_key(
            spec.strategy,
            type_oid,
            key_type,
            key.opclass.as_deref(),
            catalog,
        )?;
        if let Some(index) = simple_column_index {
            let column = &relation_desc.columns[index];
            if column.dropped {
                return Err(partition_key_error(format!(
                    "column \"{}\" named in partition key does not exist",
                    column.name
                )));
            }
            if column.generated.is_some() {
                return Err(ParseError::DetailedError {
                    message: "cannot use generated column in partition key".into(),
                    detail: Some(format!("Column \"{}\" is a generated column.", column.name)),
                    hint: None,
                    sqlstate: "42P17",
                });
            }
            key_columns.push(column.name.clone());
            key_sqls.push(column.name.clone());
            partattrs.push(index as i16 + 1);
        } else {
            key_sqls.push(key.expr_sql.clone());
            partattrs.push(0);
        }
        key_exprs.push(bound);
        key_types.push(key_type);
        partclass.push(opclass);
        partcollation.push(key_collation_oid);
    }

    Ok(LoweredPartitionSpec {
        strategy: spec.strategy,
        key_columns,
        key_exprs,
        key_types,
        key_sqls,
        partattrs,
        partclass,
        partcollation,
    })
}

fn partition_opclass_for_key(
    strategy: PartitionStrategy,
    type_oid: u32,
    sql_type: SqlType,
    explicit_opclass: Option<&str>,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ParseError> {
    let access_method = partition_access_method_oid(strategy, sql_type);
    let access_method_name = partition_access_method_name(strategy);
    if let Some(name) = explicit_opclass {
        let normalized = super::normalize_catalog_lookup_name(name);
        return catalog
            .opclass_rows()
            .into_iter()
            .find(|row| {
                row.opcmethod == access_method
                    && row.opcname.eq_ignore_ascii_case(normalized)
                    && opclass_accepts_type(row.opcintype, type_oid)
            })
            .map(|row| row.oid)
            .ok_or_else(|| ParseError::DetailedError {
                message: format!(
                    "operator class \"{name}\" does not exist for access method \"{access_method_name}\""
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
    }
    default_opclass_for_partition_strategy(strategy, type_oid, sql_type).ok_or_else(|| {
        ParseError::DetailedError {
            message: format!(
                "data type {} has no default operator class for access method \"{access_method_name}\"",
                sql_type_name(sql_type)
            ),
            detail: None,
            hint: Some(format!(
                "You must specify a {access_method_name} operator class or define a default {access_method_name} operator class for the data type."
            )),
            sqlstate: "42704",
        }
    })
}

fn partition_access_method_oid(strategy: PartitionStrategy, _sql_type: SqlType) -> u32 {
    match strategy {
        PartitionStrategy::Hash => HASH_AM_OID,
        PartitionStrategy::List | PartitionStrategy::Range => BTREE_AM_OID,
    }
}

fn partition_access_method_name(strategy: PartitionStrategy) -> &'static str {
    match strategy {
        PartitionStrategy::Hash => "hash",
        PartitionStrategy::List | PartitionStrategy::Range => "btree",
    }
}

fn opclass_accepts_type(opcintype: u32, type_oid: u32) -> bool {
    opcintype == type_oid
        || opcintype == ANYARRAYOID
        || opcintype == ANYMULTIRANGEOID
        || (opcintype == OID_TYPE_OID && type_oid == INT4_TYPE_OID)
        || (is_text_opclass_type(opcintype) && is_text_opclass_type(type_oid))
}

fn is_text_opclass_type(type_oid: u32) -> bool {
    matches!(type_oid, TEXT_TYPE_OID | VARCHAR_TYPE_OID | BPCHAR_TYPE_OID)
}

fn partition_key_error(message: impl Into<String>) -> ParseError {
    ParseError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42P16",
    }
}

fn simple_partition_key_column_name(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Column(name) if !name.contains('.') => Some(name.as_str()),
        _ => None,
    }
}

fn validate_partition_key_raw_expr(
    expr: &SqlExpr,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if super::agg::expr_contains_agg(catalog, expr) {
        return Err(partition_key_error(
            "aggregate functions are not allowed in partition key expressions",
        ));
    }
    if super::window::expr_contains_window(expr) {
        return Err(partition_key_error(
            "window functions are not allowed in partition key expressions",
        ));
    }
    if raw_expr_any(expr, &is_subquery_expr) {
        return Err(partition_key_error(
            "cannot use subquery in partition key expression",
        ));
    }
    Ok(())
}

fn validate_partition_bound_raw_expr(
    expr: &SqlExpr,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if raw_expr_any(expr, &|expr| matches!(expr, SqlExpr::Column(_))) {
        return Err(partition_bound_error(
            "cannot use column reference in partition bound expression",
        ));
    }
    if super::agg::expr_contains_agg(catalog, expr) {
        return Err(partition_bound_error(
            "aggregate functions are not allowed in partition bound",
        ));
    }
    if super::window::expr_contains_window(expr) {
        return Err(partition_bound_error(
            "window functions are not allowed in partition bound",
        ));
    }
    if raw_expr_any(expr, &is_subquery_expr) {
        return Err(partition_bound_error(
            "cannot use subquery in partition bound",
        ));
    }
    Ok(())
}

fn partition_bound_error(message: impl Into<String>) -> ParseError {
    ParseError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42P17",
    }
}

fn is_subquery_expr(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::ScalarSubquery(_)
            | SqlExpr::ArraySubquery(_)
            | SqlExpr::Exists(_)
            | SqlExpr::InSubquery { .. }
            | SqlExpr::QuantifiedSubquery { .. }
    )
}

fn raw_expr_any(expr: &SqlExpr, predicate: &impl Fn(&SqlExpr) -> bool) -> bool {
    if predicate(expr) {
        return true;
    }
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. }
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_) => false,
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            args.args()
                .iter()
                .any(|arg| raw_expr_any(&arg.value, predicate))
                || order_by
                    .iter()
                    .any(|item| raw_expr_any(&item.expr, predicate))
                || within_group.as_deref().is_some_and(|items| {
                    items.iter().any(|item| raw_expr_any(&item.expr, predicate))
                })
                || filter
                    .as_deref()
                    .is_some_and(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::InSubquery { expr, .. } => raw_expr_any(expr, predicate),
        SqlExpr::QuantifiedSubquery { left, .. } => raw_expr_any(left, predicate),
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            raw_expr_any(expr, predicate)
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            elements.iter().any(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            raw_expr_any(array, predicate)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_deref()
                        .is_some_and(|expr| raw_expr_any(expr, predicate))
                        || subscript
                            .upper
                            .as_deref()
                            .is_some_and(|expr| raw_expr_any(expr, predicate))
                })
        }
        SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::QuantifiedArray {
            left, array: right, ..
        }
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::GeometryBinaryOp { left, right, .. }
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        }
        | SqlExpr::BinaryOperator { left, right, .. } => {
            raw_expr_any(left, predicate) || raw_expr_any(right, predicate)
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            raw_expr_any(expr, predicate)
                || raw_expr_any(pattern, predicate)
                || escape
                    .as_ref()
                    .is_some_and(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref()
                .is_some_and(|expr| raw_expr_any(expr, predicate))
                || args.iter().any(|arm| {
                    raw_expr_any(&arm.expr, predicate) || raw_expr_any(&arm.result, predicate)
                })
                || defresult
                    .as_deref()
                    .is_some_and(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::Cast(inner, _)
        | SqlExpr::Collate { expr: inner, .. }
        | SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => raw_expr_any(inner, predicate),
        SqlExpr::Xml(xml) => xml.child_exprs().any(|expr| raw_expr_any(expr, predicate)),
    }
}

fn partition_key_pseudotype_name(key_type: SqlType, raw_expr: &SqlExpr) -> Option<&'static str> {
    if key_type.type_oid == crate::include::catalog::UNKNOWN_TYPE_OID
        || matches!(
            raw_expr,
            SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
        )
    {
        return Some("unknown");
    }
    match key_type.kind {
        SqlTypeKind::Record => Some("record"),
        SqlTypeKind::Cstring => Some("cstring"),
        _ => None,
    }
}

fn partition_expr_is_mutable(expr: &Expr, catalog: &dyn CatalogLookup) -> bool {
    match expr {
        Expr::Func(func) => partition_func_is_mutable(func, catalog),
        Expr::Op(op) => op
            .args
            .iter()
            .any(|arg| partition_expr_is_mutable(arg, catalog)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| partition_expr_is_mutable(arg, catalog)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(|expr| partition_expr_is_mutable(expr, catalog))
                || case_expr.args.iter().any(|arm| {
                    partition_expr_is_mutable(&arm.expr, catalog)
                        || partition_expr_is_mutable(&arm.result, catalog)
                })
                || partition_expr_is_mutable(&case_expr.defresult, catalog)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => partition_expr_is_mutable(inner, catalog),
        Expr::Coalesce(left, right) => {
            partition_expr_is_mutable(left, catalog) || partition_expr_is_mutable(right, catalog)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            partition_expr_is_mutable(expr, catalog)
                || partition_expr_is_mutable(pattern, catalog)
                || escape
                    .as_deref()
                    .is_some_and(|expr| partition_expr_is_mutable(expr, catalog))
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| partition_expr_is_mutable(expr, catalog)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| partition_expr_is_mutable(expr, catalog)),
        Expr::ArraySubscript { array, subscripts } => {
            partition_expr_is_mutable(array, catalog)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| partition_expr_is_mutable(expr, catalog))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| partition_expr_is_mutable(expr, catalog))
                })
        }
        Expr::ScalarArrayOp(saop) => {
            partition_expr_is_mutable(&saop.left, catalog)
                || partition_expr_is_mutable(&saop.right, catalog)
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            partition_expr_is_mutable(left, catalog) || partition_expr_is_mutable(right, catalog)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| partition_expr_is_mutable(expr, catalog)),
        Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => true,
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Aggref(_)
        | Expr::WindowFunc(_)
        | Expr::SetReturning(_)
        | Expr::SubLink(_)
        | Expr::SubPlan(_)
        | Expr::CaseTest(_)
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema => false,
    }
}

fn partition_func_is_mutable(func: &FuncExpr, catalog: &dyn CatalogLookup) -> bool {
    if func
        .args
        .iter()
        .any(|arg| partition_expr_is_mutable(arg, catalog))
    {
        return true;
    }
    match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => matches!(
            builtin,
            crate::include::nodes::primnodes::BuiltinScalarFunction::Random
                | crate::include::nodes::primnodes::BuiltinScalarFunction::RandomNormal
                | crate::include::nodes::primnodes::BuiltinScalarFunction::Now
                | crate::include::nodes::primnodes::BuiltinScalarFunction::ClockTimestamp
                | crate::include::nodes::primnodes::BuiltinScalarFunction::StatementTimestamp
                | crate::include::nodes::primnodes::BuiltinScalarFunction::TransactionTimestamp
        ),
        ScalarFunctionImpl::UserDefined { proc_oid } => catalog
            .proc_row_by_oid(proc_oid)
            .is_some_and(|row| row.prosrc.to_ascii_lowercase().contains("random(")),
    }
}

fn expr_contains_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(_) => true,
        Expr::Func(func) => func.args.iter().any(expr_contains_var),
        Expr::Op(op) => op.args.iter().any(expr_contains_var),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_var),
        Expr::Case(case_expr) => {
            case_expr.arg.as_deref().is_some_and(expr_contains_var)
                || case_expr
                    .args
                    .iter()
                    .any(|arm| expr_contains_var(&arm.expr) || expr_contains_var(&arm.result))
                || expr_contains_var(&case_expr.defresult)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_contains_var(inner),
        Expr::Coalesce(left, right) => expr_contains_var(left) || expr_contains_var(right),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_var(expr)
                || expr_contains_var(pattern)
                || escape.as_deref().is_some_and(expr_contains_var)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_var),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_var(expr)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_var(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(expr_contains_var)
                        || subscript.upper.as_ref().is_some_and(expr_contains_var)
                })
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_var(&saop.left) || expr_contains_var(&saop.right)
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_var(left) || expr_contains_var(right)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_var),
        Expr::SubLink(sublink) => sublink.testexpr.as_deref().is_some_and(expr_contains_var),
        Expr::SubPlan(subplan) => {
            subplan.testexpr.as_deref().is_some_and(expr_contains_var)
                || subplan.args.iter().any(expr_contains_var)
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::Aggref(_)
        | Expr::WindowFunc(_)
        | Expr::SetReturning(_)
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
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn lower_partition_bound(
    bound: &RawPartitionBoundSpec,
    parent_spec: &LoweredPartitionSpec,
    parent_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<PartitionBoundSpec, ParseError> {
    match (bound, parent_spec.strategy) {
        (
            RawPartitionBoundSpec::List {
                is_default: true, ..
            }
            | RawPartitionBoundSpec::Range {
                is_default: true, ..
            },
            PartitionStrategy::Hash,
        ) => Err(ParseError::DetailedError {
            message: "a hash-partitioned table may not have a default partition".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        }),
        (
            RawPartitionBoundSpec::List {
                is_default: true, ..
            },
            PartitionStrategy::List,
        ) => Ok(PartitionBoundSpec::List {
            values: Vec::new(),
            is_default: true,
        }),
        (
            RawPartitionBoundSpec::List {
                is_default: true, ..
            },
            PartitionStrategy::Range,
        ) => Ok(PartitionBoundSpec::Range {
            from: Vec::new(),
            to: Vec::new(),
            is_default: true,
        }),
        (RawPartitionBoundSpec::List { values, is_default }, PartitionStrategy::List) => {
            let key_type = parent_spec_key_types(parent_spec, parent_desc)
                .into_iter()
                .next()
                .ok_or_else(|| {
                    ParseError::InvalidTableDefinition("missing list partition key".into())
                })?;
            let key_name = parent_spec_key_names(parent_spec, parent_desc)
                .into_iter()
                .next()
                .unwrap_or_else(|| "a".into());
            let lowered = values
                .iter()
                .map(|expr| evaluate_partition_bound_expr(expr, key_type, &key_name, catalog))
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
            let key_names = parent_spec_key_names(parent_spec, parent_desc);
            if !*is_default && from.len() != key_types.len() {
                return Err(partition_bound_error(
                    "FROM must specify exactly one value per partitioning column",
                ));
            }
            if !*is_default && to.len() != key_types.len() {
                return Err(partition_bound_error(
                    "TO must specify exactly one value per partitioning column",
                ));
            }
            let from = lower_range_datums(from, &key_types, &key_names, catalog)?;
            let to = lower_range_datums(to, &key_types, &key_names, catalog)?;
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
        (RawPartitionBoundSpec::List { .. }, _)
        | (RawPartitionBoundSpec::Range { .. }, _)
        | (RawPartitionBoundSpec::Hash { .. }, _) => {
            Err(invalid_bound_spec_error(parent_spec.strategy))
        }
    }
}

fn invalid_bound_spec_error(strategy: PartitionStrategy) -> ParseError {
    partition_bound_error(format!(
        "invalid bound specification for a {} partition",
        partition_strategy_name_for_bound(strategy)
    ))
}

fn partition_strategy_name_for_bound(strategy: PartitionStrategy) -> &'static str {
    match strategy {
        PartitionStrategy::List => "list",
        PartitionStrategy::Range => "range",
        PartitionStrategy::Hash => "hash",
    }
}

fn default_opclass_for_partition_strategy(
    strategy: PartitionStrategy,
    type_oid: u32,
    sql_type: SqlType,
) -> Option<u32> {
    match strategy {
        PartitionStrategy::List | PartitionStrategy::Range if sql_type.is_array => {
            Some(ARRAY_BTREE_OPCLASS_OID)
        }
        PartitionStrategy::List | PartitionStrategy::Range => default_btree_opclass_oid(type_oid),
        PartitionStrategy::Hash => default_hash_opclass_oid(type_oid),
    }
}

fn validate_hash_partition_bound(modulus: i32, remainder: i32) -> Result<(), ParseError> {
    if modulus <= 0 {
        return Err(ParseError::DetailedError {
            message: "modulus for hash partition must be an integer value greater than zero".into(),
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
    key_names: &[String],
    catalog: &dyn CatalogLookup,
) -> Result<Vec<PartitionRangeDatumValue>, ParseError> {
    values
        .iter()
        .zip(key_types.iter())
        .enumerate()
        .map(|(index, (value, key_type))| match value {
            RawPartitionRangeDatum::MinValue => Ok(PartitionRangeDatumValue::MinValue),
            RawPartitionRangeDatum::MaxValue => Ok(PartitionRangeDatumValue::MaxValue),
            RawPartitionRangeDatum::Value(expr) => {
                let key_name = key_names
                    .get(index)
                    .map(String::as_str)
                    .unwrap_or("?column?");
                evaluate_partition_bound_expr(expr, *key_type, key_name, catalog)
                    .and_then(|value| {
                        if matches!(value, Value::Null) {
                            return Err(partition_bound_error(
                                "cannot specify NULL in range bound",
                            ));
                        }
                        Ok(value)
                    })
                    .and_then(|value| value_to_partition_value(&value))
                    .map(PartitionRangeDatumValue::Value)
            }
        })
        .collect()
}

fn parent_spec_key_types(spec: &LoweredPartitionSpec, desc: &RelationDesc) -> Vec<SqlType> {
    if !spec.key_types.is_empty() {
        return spec.key_types.clone();
    }
    spec.partattrs
        .iter()
        .filter_map(|attnum| {
            attrno_index(i32::from(*attnum)).and_then(|index| desc.columns.get(index))
        })
        .map(|column| column.sql_type)
        .collect()
}

fn parent_spec_key_names(spec: &LoweredPartitionSpec, desc: &RelationDesc) -> Vec<String> {
    if !spec.key_sqls.is_empty() {
        return spec.key_sqls.clone();
    }
    spec.partattrs
        .iter()
        .filter_map(|attnum| {
            attrno_index(i32::from(*attnum)).and_then(|index| desc.columns.get(index))
        })
        .map(|column| column.name.clone())
        .collect()
}

fn evaluate_partition_bound_expr(
    expr: &crate::backend::parser::SqlExpr,
    target: SqlType,
    key_name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<Value, ParseError> {
    validate_partition_bound_raw_expr(expr, catalog)?;
    let (bound, _from_type) = bind_scalar_expr_in_scope(expr, &[], catalog)?;
    if expr_contains_set_returning(&bound) {
        return Err(partition_bound_error(
            "set-returning functions are not allowed in partition bound",
        ));
    }
    let folded = crate::backend::optimizer::fold_expr_constants(bound)?;
    let Expr::Const(value) = folded else {
        return Err(partition_bound_error(
            "partition bound values must be constant",
        ));
    };
    if matches!(target.kind, SqlTypeKind::Bool)
        && matches!(
            value,
            Value::Int16(_) | Value::Int32(_) | Value::Int64(_) | Value::Numeric(_)
        )
    {
        return Err(partition_bound_cast_error(target, key_name));
    }
    cast_value(value, target).map_err(|_| partition_bound_cast_error(target, key_name))
}

fn partition_bound_cast_error(target: SqlType, key_name: &str) -> ParseError {
    partition_bound_error(format!(
        "specified value cannot be cast to type {} for column \"{}\"",
        sql_type_name(target),
        key_name
    ))
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
        TablePersistence::Unlogged => 'u',
        TablePersistence::Temporary => 't',
    }
}

fn partition_persistence_error(child: char, parent: char, parent_name: &str) -> Option<String> {
    match (child, parent) {
        ('p', 't') => Some(format!(
            "cannot create a permanent relation as partition of temporary relation \"{parent_name}\""
        )),
        ('t', 'p') => Some(format!(
            "cannot create a temporary relation as partition of permanent relation \"{parent_name}\""
        )),
        _ => None,
    }
}

fn is_system_column_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    )
}
