use std::cmp::Ordering;
use std::collections::VecDeque;

use crate::backend::commands::tablecmds::collect_matching_rows_heap;
use crate::backend::executor::value_io::format_failing_row_detail;
use crate::backend::executor::{
    ExecError, ExecutorContext, compare_order_values, render_datetime_value_text_with_config,
};
use crate::backend::parser::{
    BoundRelation, CatalogLookup, LoweredPartitionSpec, PartitionBoundSpec, PartitionRangeDatumValue,
    SerializedPartitionValue, deserialize_partition_bound, partition_value_to_value,
    relation_partition_spec,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::nodes::datum::Value;

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn direct_partition_children(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<BoundRelation>, ExecError> {
    let mut inherits = catalog.inheritance_children(relation_oid);
    inherits.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    inherits
        .into_iter()
        .map(|row| {
            catalog.relation_by_oid(row.inhrelid).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("missing partition relation {}", row.inhrelid),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                }
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionTreeEntry {
    pub relid: u32,
    pub parentrelid: Option<u32>,
    pub isleaf: bool,
    pub level: i32,
}

fn relation_can_participate_in_partition_tree(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> bool {
    catalog
        .relation_by_oid(relation_oid)
        .is_some_and(|relation| relation.relispartition || relation.partitioned_table.is_some())
}

fn declarative_parent(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
) -> Result<Option<BoundRelation>, ExecError> {
    let parent_oid = catalog
        .inheritance_parents(relation.relation_oid)
        .into_iter()
        .find_map(|row| {
            catalog
                .relation_by_oid(row.inhparent)
                .filter(|parent| parent.partitioned_table.is_some())
                .map(|parent| parent.relation_oid)
        });
    parent_oid
        .map(|oid| {
            catalog.relation_by_oid(oid).ok_or_else(|| ExecError::DetailedError {
                message: format!("missing partitioned parent {}", oid),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })
        })
        .transpose()
}

pub(crate) fn partition_parent_oid(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Option<u32>, ExecError> {
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Ok(None);
    };
    declarative_parent(catalog, &relation).map(|parent| parent.map(|parent| parent.relation_oid))
}

pub(crate) fn partition_ancestor_oids(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<u32>, ExecError> {
    if !relation_can_participate_in_partition_tree(catalog, relation_oid) {
        return Ok(Vec::new());
    }

    let mut ancestors = Vec::new();
    let mut current = Some(relation_oid);
    while let Some(relid) = current {
        ancestors.push(relid);
        current = partition_parent_oid(catalog, relid)?;
    }
    Ok(ancestors)
}

pub(crate) fn partition_root_oid(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Option<u32>, ExecError> {
    Ok(partition_ancestor_oids(catalog, relation_oid)?
        .into_iter()
        .last())
}

pub(crate) fn partition_tree_entries(
    catalog: &dyn CatalogLookup,
    root_oid: u32,
) -> Result<Vec<PartitionTreeEntry>, ExecError> {
    if !relation_can_participate_in_partition_tree(catalog, root_oid) {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    let mut queue = VecDeque::from([(root_oid, 0_i32)]);
    while let Some((relation_oid, level)) = queue.pop_front() {
        let Some(relation) = catalog.relation_by_oid(relation_oid) else {
            continue;
        };
        entries.push(PartitionTreeEntry {
            relid: relation_oid,
            parentrelid: partition_parent_oid(catalog, relation_oid)?,
            isleaf: relation.partitioned_table.is_none(),
            level,
        });
        if relation.partitioned_table.is_none() {
            continue;
        }
        for child in direct_partition_children(catalog, relation_oid)? {
            if !child.relispartition {
                continue;
            }
            queue.push_back((child.relation_oid, level + 1));
        }
    }

    Ok(entries)
}

fn child_partition_bound(child: &BoundRelation) -> Result<PartitionBoundSpec, ExecError> {
    let Some(bound) = child.relpartbound.as_deref() else {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition relation \"{}\" is missing relpartbound metadata",
                child.relation_oid
            ),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    deserialize_partition_bound(bound).map_err(ExecError::Parse)
}

fn key_values(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    row: &[Value],
) -> Result<Vec<Value>, ExecError> {
    spec.partattrs
        .iter()
        .map(|attnum| {
            row.get(attnum.saturating_sub(1) as usize)
                .cloned()
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "partition key attribute {} is missing from row for relation {}",
                        attnum, relation.relation_oid
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .collect()
}

fn partition_key_names(relation: &BoundRelation, spec: &LoweredPartitionSpec) -> Vec<String> {
    spec.partattrs
        .iter()
        .filter_map(|attnum| relation.desc.columns.get(attnum.saturating_sub(1) as usize))
        .map(|column| column.name.clone())
        .collect()
}

fn render_partition_key_value(value: &Value, datetime_config: &DateTimeConfig) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".into()
            } else {
                "false".into()
            }
        }
        Value::InternalChar(v) => (*v as char).to_string(),
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Json(text) => text.to_string(),
        Value::JsonPath(text) => text.to_string(),
        Value::Xml(text) => text.to_string(),
        Value::Bytea(bytes) => format!("{bytes:?}"),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            render_datetime_value_text_with_config(value, datetime_config).unwrap_or_default()
        }
        other => format!("{other:?}"),
    }
}

fn no_partition_detail(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    row: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    let names = partition_key_names(relation, spec).join(", ");
    let values = key_values(relation, spec, row)?
        .iter()
        .map(|value| render_partition_key_value(value, datetime_config))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "Partition key of the failing row contains ({names}) = ({values})."
    ))
}

fn partition_constraint_violation(
    relation_name: &str,
    row: &[Value],
    datetime_config: &DateTimeConfig,
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "new row for relation \"{relation_name}\" violates partition constraint"
        ),
        detail: Some(format_failing_row_detail(row, datetime_config)),
        hint: None,
        sqlstate: "23514",
    }
}

fn no_partition_for_row(
    relation_name: &str,
    detail: String,
) -> ExecError {
    ExecError::DetailedError {
        message: format!("no partition of relation \"{relation_name}\" found for row"),
        detail: Some(detail),
        hint: None,
        sqlstate: "23514",
    }
}

fn compare_serialized_values(
    left: &SerializedPartitionValue,
    right: &SerializedPartitionValue,
    collation_oid: Option<u32>,
) -> Result<Ordering, ExecError> {
    compare_order_values(
        &partition_value_to_value(left),
        &partition_value_to_value(right),
        collation_oid,
        None,
        false,
    )
}

fn compare_partition_key_to_bound(
    key_values: &[Value],
    bound: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Result<Ordering, ExecError> {
    for ((key, datum), collation_oid) in key_values
        .iter()
        .zip(bound.iter())
        .zip(collations.iter().copied())
    {
        let cmp = match datum {
            PartitionRangeDatumValue::MinValue => Ordering::Greater,
            PartitionRangeDatumValue::MaxValue => Ordering::Less,
            PartitionRangeDatumValue::Value(value) => compare_order_values(
                key,
                &partition_value_to_value(value),
                (collation_oid != 0).then_some(collation_oid),
                None,
                false,
            )?,
        };
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_range_bounds(
    left: &[PartitionRangeDatumValue],
    right: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Result<Ordering, ExecError> {
    for ((left, right), collation_oid) in left
        .iter()
        .zip(right.iter())
        .zip(collations.iter().copied())
    {
        let cmp = match (left, right) {
            (PartitionRangeDatumValue::MinValue, PartitionRangeDatumValue::MinValue)
            | (PartitionRangeDatumValue::MaxValue, PartitionRangeDatumValue::MaxValue) => {
                Ordering::Equal
            }
            (PartitionRangeDatumValue::MinValue, _) => Ordering::Less,
            (_, PartitionRangeDatumValue::MinValue) => Ordering::Greater,
            (PartitionRangeDatumValue::MaxValue, _) => Ordering::Greater,
            (_, PartitionRangeDatumValue::MaxValue) => Ordering::Less,
            (PartitionRangeDatumValue::Value(left), PartitionRangeDatumValue::Value(right)) => {
                compare_serialized_values(
                    left,
                    right,
                    (collation_oid != 0).then_some(collation_oid),
                )?
            }
        };
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

fn row_matches_explicit_bound(
    relation: &BoundRelation,
    spec: &LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    row: &[Value],
) -> Result<bool, ExecError> {
    match bound {
        PartitionBoundSpec::List { values, .. } => {
            let key = key_values(relation, spec, row)?
                .into_iter()
                .next()
                .unwrap_or(Value::Null);
            values.iter().try_fold(false, |matched, value| {
                if matched {
                    return Ok(true);
                }
                Ok(compare_order_values(
                    &key,
                    &partition_value_to_value(value),
                    spec.partcollation
                        .first()
                        .copied()
                        .filter(|oid| *oid != 0),
                    None,
                    false,
                )? == Ordering::Equal)
            })
        }
        PartitionBoundSpec::Range { from, to, .. } => {
            let keys = key_values(relation, spec, row)?;
            if keys.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(false);
            }
            Ok(compare_partition_key_to_bound(&keys, from, &spec.partcollation)? != Ordering::Less
                && compare_partition_key_to_bound(&keys, to, &spec.partcollation)?
                    == Ordering::Less)
        }
    }
}

fn find_explicit_partition_match(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    row: &[Value],
    skip_child_oid: Option<u32>,
) -> Result<Option<BoundRelation>, ExecError> {
    let spec = relation_partition_spec(parent).map_err(ExecError::Parse)?;
    for child in direct_partition_children(catalog, parent.relation_oid)? {
        if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
            continue;
        }
        let bound = child_partition_bound(&child)?;
        if bound.is_default() {
            continue;
        }
        if row_matches_explicit_bound(parent, &spec, &bound, row)? {
            return Ok(Some(child));
        }
    }
    Ok(None)
}

fn default_partition(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    skip_child_oid: Option<u32>,
) -> Result<Option<BoundRelation>, ExecError> {
    for child in direct_partition_children(catalog, parent.relation_oid)? {
        if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
            continue;
        }
        if child_partition_bound(&child)?.is_default() {
            return Ok(Some(child));
        }
    }
    Ok(None)
}

fn candidate_row_matches_partition(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    bound: &PartitionBoundSpec,
    row: &[Value],
    skip_child_oid: Option<u32>,
) -> Result<bool, ExecError> {
    if bound.is_default() {
        return Ok(find_explicit_partition_match(catalog, parent, row, skip_child_oid)?.is_none());
    }
    let spec = relation_partition_spec(parent).map_err(ExecError::Parse)?;
    row_matches_explicit_bound(parent, &spec, bound, row)
}

fn bounds_overlap(
    spec: &LoweredPartitionSpec,
    left: &PartitionBoundSpec,
    right: &PartitionBoundSpec,
) -> Result<bool, ExecError> {
    match (left, right) {
        (
            PartitionBoundSpec::List { values: left, .. },
            PartitionBoundSpec::List { values: right, .. },
        ) => {
            for left in left {
                for right in right {
                    if compare_serialized_values(
                        left,
                        right,
                        spec.partcollation.first().copied().filter(|oid| *oid != 0),
                    )? == Ordering::Equal
                    {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        (
            PartitionBoundSpec::Range {
                from: left_from,
                to: left_to,
                ..
            },
            PartitionBoundSpec::Range {
                from: right_from,
                to: right_to,
                ..
            },
        ) => Ok(compare_range_bounds(left_to, right_from, &spec.partcollation)? == Ordering::Greater
            && compare_range_bounds(right_to, left_from, &spec.partcollation)? == Ordering::Greater),
        _ => Ok(false),
    }
}

pub(crate) fn validate_new_partition_bound(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    new_relation_name: &str,
    bound: &PartitionBoundSpec,
    skip_child_oid: Option<u32>,
) -> Result<(), ExecError> {
    let spec = relation_partition_spec(parent).map_err(ExecError::Parse)?;
    if bound.is_default()
        && let Some(existing) = default_partition(catalog, parent, skip_child_oid)?
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{new_relation_name}\" conflicts with existing default partition \"{}\"",
                relation_name_for_oid(catalog, existing.relation_oid)
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }

    for child in direct_partition_children(catalog, parent.relation_oid)? {
        if skip_child_oid.is_some_and(|oid| oid == child.relation_oid) {
            continue;
        }
        let existing_bound = child_partition_bound(&child)?;
        if existing_bound.is_default() {
            continue;
        }
        if bounds_overlap(&spec, bound, &existing_bound)? {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition \"{new_relation_name}\" would overlap partition \"{}\"",
                    relation_name_for_oid(catalog, child.relation_oid)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P17",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_partition_relation_compatibility(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    parent_name: &str,
    child: &BoundRelation,
    child_name: &str,
) -> Result<(), ExecError> {
    if parent.relkind != 'p' || parent.partitioned_table.is_none() {
        return Err(ExecError::Parse(crate::backend::parser::ParseError::WrongObjectType {
            name: parent_name.to_string(),
            expected: "partitioned table",
        }));
    }
    if !matches!(child.relkind, 'r' | 'p') {
        return Err(ExecError::Parse(crate::backend::parser::ParseError::WrongObjectType {
            name: child_name.to_string(),
            expected: "table",
        }));
    }
    if child.relpersistence != parent.relpersistence {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{child_name}\" would have different persistence than partitioned table \"{parent_name}\""
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    let parent_columns = parent
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    let child_columns = child
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    if parent_columns.len() != child_columns.len() {
        return Err(ExecError::DetailedError {
            message: format!(
                "partition \"{child_name}\" has different column count than partitioned table \"{parent_name}\""
            ),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    for (parent_column, child_column) in parent_columns.iter().zip(child_columns.iter()) {
        if !parent_column.name.eq_ignore_ascii_case(&child_column.name)
            || parent_column.sql_type != child_column.sql_type
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "partition \"{child_name}\" has different column layout than partitioned table \"{parent_name}\""
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
    }
    if child.relispartition {
        return Err(ExecError::DetailedError {
            message: format!("table \"{child_name}\" is already a partition"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if !catalog.inheritance_parents(child.relation_oid).is_empty() {
        return Err(ExecError::DetailedError {
            message: format!("table \"{child_name}\" is already a child of another relation"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    Ok(())
}

pub(crate) fn validate_relation_rows_for_partition_bound(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
    bound: &PartitionBoundSpec,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let relation_oids = if child.relkind == 'p' {
        catalog
            .find_all_inheritors(child.relation_oid)
            .into_iter()
            .filter(|oid| {
                catalog
                    .relation_by_oid(*oid)
                    .is_some_and(|relation| relation.relkind == 'r')
            })
            .collect::<Vec<_>>()
    } else {
        vec![child.relation_oid]
    };

    for relation_oid in relation_oids {
        let Some(relation) = catalog.relation_by_oid(relation_oid) else {
            continue;
        };
        for (_, row) in collect_matching_rows_heap(
            relation.rel,
            &relation.desc,
            relation.toast,
            None,
            ctx,
        )? {
            if !candidate_row_matches_partition(catalog, parent, bound, &row, None)? {
                return Err(partition_constraint_violation(
                    &relation_name_for_oid(catalog, child.relation_oid),
                    &row,
                    &ctx.datetime_config,
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn route_partition_target(
    catalog: &dyn CatalogLookup,
    target: &BoundRelation,
    row: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<BoundRelation, ExecError> {
    if target.relispartition
        && let Some(parent) = declarative_parent(catalog, target)?
    {
        let selected = if let Some(explicit) =
            find_explicit_partition_match(catalog, &parent, row, None)?
        {
            Some(explicit)
        } else {
            default_partition(catalog, &parent, None)?
        };
        if selected
            .as_ref()
            .is_none_or(|relation| relation.relation_oid != target.relation_oid)
        {
            return Err(partition_constraint_violation(
                &relation_name_for_oid(catalog, target.relation_oid),
                row,
                datetime_config,
            ));
        }
    }

    if target.relkind != 'p' {
        return Ok(target.clone());
    }

    if let Some(explicit) = find_explicit_partition_match(catalog, target, row, None)? {
        return route_partition_target(catalog, &explicit, row, datetime_config);
    }
    if let Some(default_child) = default_partition(catalog, target, None)? {
        return route_partition_target(catalog, &default_child, row, datetime_config);
    }

    let spec = relation_partition_spec(target).map_err(ExecError::Parse)?;
    Err(no_partition_for_row(
        &relation_name_for_oid(catalog, target.relation_oid),
        no_partition_detail(target, &spec, row, datetime_config)?,
    ))
}
