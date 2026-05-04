use std::collections::BTreeSet;
use std::collections::VecDeque;

use pgrust_analyze::{BoundRelation, CatalogLookup};
use pgrust_catalog_data::{CONSTRAINT_CHECK, CONSTRAINT_NOTNULL, PgConstraintRow};
use pgrust_nodes::datum::Value;
use pgrust_nodes::parsenodes::ColumnGeneratedKind;
use pgrust_nodes::partition::{
    PartitionBoundSpec, PartitionRangeDatumValue, SerializedPartitionValue,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionError {
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
    WrongObjectType {
        name: String,
        expected: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTreeEntry {
    pub relid: u32,
    pub parentrelid: Option<u32>,
    pub isleaf: bool,
    pub level: i32,
}

pub fn format_partition_key_expr_name(expr_sql: &str) -> String {
    let stripped = strip_outer_expr_parens(expr_sql.trim());
    let normalized = normalize_partition_expr_operator_spacing(stripped);
    if normalized.contains(" + ")
        || normalized.contains(" - ")
        || normalized.contains(" * ")
        || normalized.contains(" / ")
        || normalized.contains(" % ")
    {
        format!("({normalized})")
    } else {
        normalized
    }
}

pub fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

pub fn generated_kind_name(kind: ColumnGeneratedKind) -> &'static str {
    match kind {
        ColumnGeneratedKind::Virtual => "VIRTUAL",
        ColumnGeneratedKind::Stored => "STORED",
    }
}

pub fn direct_partition_children(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<BoundRelation>, PartitionError> {
    let mut inherits = catalog.inheritance_children(relation_oid);
    inherits.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    inherits
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .map(|row| {
            catalog
                .relation_by_oid(row.inhrelid)
                .ok_or_else(|| PartitionError::Detailed {
                    message: format!("missing partition relation {}", row.inhrelid),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .collect()
}

pub fn relkind_has_partitions(relkind: char) -> bool {
    matches!(relkind, 'p' | 'I')
}

pub fn relation_can_participate_in_partition_tree(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> bool {
    catalog
        .relation_by_oid(relation_oid)
        .is_some_and(|relation| relation.relispartition || relkind_has_partitions(relation.relkind))
}

pub fn declarative_parent(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
) -> Result<Option<BoundRelation>, PartitionError> {
    let parent_oid = catalog
        .inheritance_parents(relation.relation_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .find_map(|row| {
            catalog
                .relation_by_oid(row.inhparent)
                .filter(|parent| relkind_has_partitions(parent.relkind))
                .map(|parent| parent.relation_oid)
        });
    parent_oid
        .map(|oid| {
            catalog
                .relation_by_oid(oid)
                .ok_or_else(|| PartitionError::Detailed {
                    message: format!("missing partitioned parent {}", oid),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .transpose()
}

pub fn partition_parent_oid(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Option<u32>, PartitionError> {
    let Some(relation) = catalog.relation_by_oid(relation_oid) else {
        return Ok(None);
    };
    declarative_parent(catalog, &relation).map(|parent| parent.map(|parent| parent.relation_oid))
}

pub fn partition_ancestor_oids(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Vec<u32>, PartitionError> {
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

pub fn partition_root_oid(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<Option<u32>, PartitionError> {
    Ok(partition_ancestor_oids(catalog, relation_oid)?
        .into_iter()
        .last())
}

pub fn partition_tree_entries(
    catalog: &dyn CatalogLookup,
    root_oid: u32,
) -> Result<Vec<PartitionTreeEntry>, PartitionError> {
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
            isleaf: !relkind_has_partitions(relation.relkind),
            level,
        });
        if !relkind_has_partitions(relation.relkind) {
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

pub fn hash_moduli_compatible(left: i32, right: i32) -> bool {
    let lower = left.min(right);
    let higher = left.max(right);
    higher % lower == 0
}

pub fn hash_modulus_compatibility_detail(
    new_modulus: i32,
    existing_modulus: i32,
    existing_name: &str,
) -> String {
    if new_modulus > existing_modulus {
        format!(
            "The new modulus {new_modulus} is not divisible by {existing_modulus}, the modulus of existing partition \"{existing_name}\"."
        )
    } else {
        format!(
            "The new modulus {new_modulus} is not a factor of {existing_modulus}, the modulus of existing partition \"{existing_name}\"."
        )
    }
}

pub fn hash_bounds_overlap(
    left_modulus: i32,
    left_remainder: i32,
    right_modulus: i32,
    right_remainder: i32,
) -> bool {
    if !hash_moduli_compatible(left_modulus, right_modulus) {
        return false;
    }
    if left_modulus <= right_modulus {
        right_remainder % left_modulus == left_remainder
    } else {
        left_remainder % right_modulus == right_remainder
    }
}

pub fn column_attnum_by_name(relation: &BoundRelation, column_name: &str) -> Option<i16> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                .then_some(index.saturating_add(1) as i16)
        })
}

pub fn column_name_for_attnum(relation: &BoundRelation, attnum: i16) -> Option<&str> {
    relation
        .desc
        .columns
        .get(attnum.saturating_sub(1) as usize)
        .filter(|column| !column.dropped)
        .map(|column| column.name.as_str())
}

pub fn not_null_constraint_for_attnum(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    attnum: i16,
) -> Option<PgConstraintRow> {
    catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| {
            row.contype == CONSTRAINT_NOTNULL
                && row
                    .conkey
                    .as_ref()
                    .is_some_and(|keys| keys.contains(&attnum))
        })
}

pub fn validate_attach_constraint_merge_state(
    parent_constraint: &PgConstraintRow,
    child_constraint: &PgConstraintRow,
    child_name: &str,
) -> Result<(), PartitionError> {
    if child_constraint.connoinherit {
        return Err(PartitionError::Detailed {
            message: format!(
                "constraint \"{}\" conflicts with non-inherited constraint on child table \"{child_name}\"",
                child_constraint.conname
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if parent_constraint.convalidated
        && child_constraint.conenforced
        && !child_constraint.convalidated
    {
        return Err(PartitionError::Detailed {
            message: format!(
                "constraint \"{}\" conflicts with NOT VALID constraint on child table \"{child_name}\"",
                child_constraint.conname
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    if parent_constraint.conenforced && !child_constraint.conenforced {
        return Err(PartitionError::Detailed {
            message: format!(
                "constraint \"{}\" conflicts with NOT ENFORCED constraint on child table \"{child_name}\"",
                child_constraint.conname
            ),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    Ok(())
}

pub fn validate_attach_check_constraints(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
    child_name: &str,
) -> Result<(), PartitionError> {
    let child_constraints = catalog
        .constraint_rows_for_relation(child.relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_CHECK)
        .collect::<Vec<_>>();
    for parent_constraint in catalog
        .constraint_rows_for_relation(parent.relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_CHECK && !row.connoinherit)
    {
        let Some(child_constraint) = child_constraints
            .iter()
            .find(|row| row.conname.eq_ignore_ascii_case(&parent_constraint.conname))
        else {
            return Err(PartitionError::Detailed {
                message: format!(
                    "child table is missing constraint \"{}\"",
                    parent_constraint.conname
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        };
        if child_constraint.conbin != parent_constraint.conbin {
            return Err(PartitionError::Detailed {
                message: format!(
                    "child table \"{child_name}\" has different definition for check constraint \"{}\"",
                    parent_constraint.conname
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        validate_attach_constraint_merge_state(&parent_constraint, child_constraint, child_name)?;
    }
    Ok(())
}

pub fn validate_attach_not_null_constraints(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
    child_name: &str,
) -> Result<(), PartitionError> {
    for parent_constraint in catalog
        .constraint_rows_for_relation(parent.relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_NOTNULL && !row.connoinherit)
    {
        let Some(parent_attnum) = parent_constraint
            .conkey
            .as_ref()
            .and_then(|keys| keys.first())
            .copied()
        else {
            continue;
        };
        let Some(column_name) = column_name_for_attnum(parent, parent_attnum) else {
            continue;
        };
        let Some(child_attnum) = column_attnum_by_name(child, column_name) else {
            continue;
        };
        let Some(child_constraint) =
            not_null_constraint_for_attnum(catalog, child.relation_oid, child_attnum)
        else {
            return Err(PartitionError::Detailed {
                message: format!(
                    "column \"{column_name}\" in child table \"{child_name}\" must be marked NOT NULL"
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        };
        validate_attach_constraint_merge_state(&parent_constraint, &child_constraint, child_name)?;
    }
    Ok(())
}

pub fn validate_attach_partition_constraints(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
) -> Result<(), PartitionError> {
    let child_name = relation_name_for_oid(catalog, child.relation_oid);
    validate_attach_check_constraints(catalog, parent, child, &child_name)?;
    validate_attach_not_null_constraints(catalog, parent, child, &child_name)?;
    Ok(())
}

pub fn validate_partition_relation_compatibility(
    catalog: &dyn CatalogLookup,
    parent: &BoundRelation,
    child: &BoundRelation,
) -> Result<(), PartitionError> {
    let parent_name = relation_name_for_oid(catalog, parent.relation_oid);
    let child_name = relation_name_for_oid(catalog, child.relation_oid);
    if parent.relkind != 'p' || parent.partitioned_table.is_none() {
        if matches!(parent.relkind, 'i' | 'I' | 'p')
            || catalog.index_row_by_oid(parent.relation_oid).is_some()
        {
            return Err(PartitionError::Detailed {
                message: format!("\"{parent_name}\" is not a partitioned table"),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        return Err(PartitionError::Detailed {
            message: format!(
                "ALTER action ATTACH PARTITION cannot be performed on relation \"{parent_name}\""
            ),
            detail: Some("This operation is not supported for tables.".into()),
            hint: None,
            sqlstate: "42809",
        });
    }
    if !matches!(child.relkind, 'r' | 'p' | 'f') {
        return Err(PartitionError::WrongObjectType {
            name: child_name,
            expected: "table",
        });
    }
    if child.relispartition {
        return Err(PartitionError::Detailed {
            message: format!("\"{child_name}\" is already a partition"),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if !catalog.inheritance_parents(child.relation_oid).is_empty() {
        return Err(PartitionError::Detailed {
            message: "cannot attach inheritance child as partition".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if matches!(child.relkind, 'r' | 'f')
        && !catalog.inheritance_children(child.relation_oid).is_empty()
    {
        return Err(PartitionError::Detailed {
            message: "cannot attach inheritance parent as partition".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    if child.relpersistence != parent.relpersistence {
        let child_persistence = if child.relpersistence == 't' {
            "temporary"
        } else {
            "permanent"
        };
        let parent_persistence = if parent.relpersistence == 't' {
            "temporary"
        } else {
            "permanent"
        };
        return Err(PartitionError::Detailed {
            message: format!(
                "cannot attach a {child_persistence} relation as partition of {parent_persistence} relation \"{parent_name}\""
            ),
            detail: None,
            hint: None,
            sqlstate: "42809",
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
    for child_column in &child_columns {
        if parent_columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(&child_column.name))
        {
            continue;
        }
        return Err(PartitionError::Detailed {
            message: format!(
                "table \"{child_name}\" contains column \"{}\" not found in parent \"{parent_name}\"",
                child_column.name
            ),
            detail: Some(
                "The new partition may contain only the columns present in parent.".into(),
            ),
            hint: None,
            sqlstate: "42804",
        });
    }
    for parent_column in &parent_columns {
        let Some(child_column) = child_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&parent_column.name))
        else {
            return Err(PartitionError::Detailed {
                message: format!("child table is missing column \"{}\"", parent_column.name),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        };
        if parent_column.sql_type != child_column.sql_type {
            return Err(PartitionError::Detailed {
                message: format!(
                    "child table \"{child_name}\" has different type for column \"{}\"",
                    parent_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        if parent_column.collation_oid != child_column.collation_oid {
            return Err(PartitionError::Detailed {
                message: format!(
                    "child table \"{child_name}\" has different collation for column \"{}\"",
                    parent_column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P21",
            });
        }
        match (parent_column.generated, child_column.generated) {
            (None, Some(_)) => {
                return Err(PartitionError::Detailed {
                    message: format!(
                        "column \"{}\" in child table must not be a generated column",
                        parent_column.name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42804",
                });
            }
            (Some(_), None) => {
                return Err(PartitionError::Detailed {
                    message: format!(
                        "column \"{}\" in child table must be a generated column",
                        parent_column.name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42804",
                });
            }
            (Some(parent_kind), Some(child_kind)) if parent_kind != child_kind => {
                return Err(PartitionError::Detailed {
                    message: format!(
                        "column \"{}\" inherits from generated column of different kind",
                        parent_column.name
                    ),
                    detail: Some(format!(
                        "Parent column is {}, child column is {}.",
                        generated_kind_name(parent_kind),
                        generated_kind_name(child_kind)
                    )),
                    hint: None,
                    sqlstate: "42P16",
                });
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn describe_partition_bound_text(bound: &PartitionBoundSpec) -> String {
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
                .map(describe_partition_value_text)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PartitionBoundSpec::Range { from, to, .. } => format!(
            "FOR VALUES FROM ({}) TO ({})",
            from.iter()
                .map(describe_partition_range_datum_text)
                .collect::<Vec<_>>()
                .join(", "),
            to.iter()
                .map(describe_partition_range_datum_text)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PartitionBoundSpec::Hash { modulus, remainder } => {
            format!("FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})")
        }
    }
}

pub fn describe_partition_bound_is_default(bound: &PartitionBoundSpec) -> bool {
    bound.is_default()
}

pub fn describe_partition_range_datum_text(value: &PartitionRangeDatumValue) -> String {
    match value {
        PartitionRangeDatumValue::MinValue => "MINVALUE".into(),
        PartitionRangeDatumValue::MaxValue => "MAXVALUE".into(),
        PartitionRangeDatumValue::Value(value) => describe_partition_value_text(value),
    }
}

pub fn describe_partition_value_text(value: &SerializedPartitionValue) -> String {
    match value {
        SerializedPartitionValue::Null => "NULL".into(),
        SerializedPartitionValue::Text(text)
        | SerializedPartitionValue::Json(text)
        | SerializedPartitionValue::JsonPath(text)
        | SerializedPartitionValue::Xml(text)
        | SerializedPartitionValue::Numeric(text)
        | SerializedPartitionValue::Float64(text) => quote_sql_literal_for_describe(text),
        SerializedPartitionValue::Int16(value) if *value < 0 => {
            quote_sql_literal_for_describe(&value.to_string())
        }
        SerializedPartitionValue::Int32(value) if *value < 0 => {
            quote_sql_literal_for_describe(&value.to_string())
        }
        SerializedPartitionValue::Int64(value) if *value < 0 => {
            quote_sql_literal_for_describe(&value.to_string())
        }
        SerializedPartitionValue::Int16(value) => value.to_string(),
        SerializedPartitionValue::Int32(value) => value.to_string(),
        SerializedPartitionValue::Int64(value) => value.to_string(),
        SerializedPartitionValue::Money(value) => value.to_string(),
        SerializedPartitionValue::Bool(value) => value.to_string(),
        SerializedPartitionValue::EnumOid(value) => {
            quote_sql_literal_for_describe(&value.to_string())
        }
        SerializedPartitionValue::Date(_)
        | SerializedPartitionValue::Time(_)
        | SerializedPartitionValue::TimeTz { .. }
        | SerializedPartitionValue::Timestamp(_)
        | SerializedPartitionValue::TimestampTz(_)
        | SerializedPartitionValue::Array(_)
        | SerializedPartitionValue::Record(_)
        | SerializedPartitionValue::Range(_)
        | SerializedPartitionValue::Multirange(_) => {
            let value = pgrust_analyze::partition_value_to_value(value);
            let rendered = render_value_for_describe_bound(&value);
            quote_sql_literal_for_describe(&rendered)
        }
        SerializedPartitionValue::Bytea(bytes) | SerializedPartitionValue::Jsonb(bytes) => {
            let mut out = String::from("'\\\\x");
            for byte in bytes {
                out.push_str(&format!("{byte:02x}"));
            }
            out.push('\'');
            out
        }
        SerializedPartitionValue::InternalChar(byte) => {
            quote_sql_literal_for_describe(&(*byte as char).to_string())
        }
    }
}

pub fn render_value_for_describe_bound(value: &Value) -> String {
    match value {
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            pgrust_expr::backend::executor::render_datetime_value_text(value).unwrap_or_default()
        }
        Value::Array(values) => pgrust_expr::backend::executor::value_io::format_array_text(values),
        Value::PgArray(array) => {
            pgrust_expr::backend::executor::value_io::format_array_value_text(array)
        }
        _ => value.as_text().unwrap_or_default().to_string(),
    }
}

pub fn quote_sql_literal_for_describe(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn strip_outer_expr_parens(expr: &str) -> &str {
    if !expr.starts_with('(') || !expr.ends_with(')') {
        return expr;
    }
    let mut depth = 0_i32;
    for (index, ch) in expr.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && index != expr.len() - 1 {
                    return expr;
                }
            }
            _ => {}
        }
    }
    expr[1..expr.len() - 1].trim()
}

pub fn normalize_partition_expr_operator_spacing(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.chars().peekable();
    while let Some(ch) = chars.next() {
        if matches!(ch, '+' | '*' | '/' | '%') || (ch == '-' && !out.trim_end().is_empty()) {
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

pub fn acl_item_grants_privilege(
    item: &str,
    effective_names: &BTreeSet<String>,
    privilege: char,
) -> bool {
    let Some((grantee, rest)) = item.split_once('=') else {
        return false;
    };
    if !effective_names.contains(grantee) {
        return false;
    }
    let Some((privileges, _grantor)) = rest.split_once('/') else {
        return false;
    };
    privileges.chars().any(|ch| ch == privilege)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::CONSTRAINT_CHECK;
    use pgrust_catalog_data::desc::column_desc;
    use pgrust_core::{PgInheritsRow, PgPartitionedTableRow, RelFileLocator};
    use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
    use pgrust_nodes::primnodes::RelationDesc;

    #[derive(Default)]
    struct TestCatalog {
        relations: Vec<BoundRelation>,
        inherits: Vec<PgInheritsRow>,
        constraints: Vec<PgConstraintRow>,
    }

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }

        fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
            self.relations
                .iter()
                .find(|relation| relation.relation_oid == relation_oid)
                .cloned()
        }

        fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
            self.inherits
                .iter()
                .filter(|row| row.inhrelid == relation_oid)
                .cloned()
                .collect()
        }

        fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
            self.inherits
                .iter()
                .filter(|row| row.inhparent == relation_oid)
                .cloned()
                .collect()
        }

        fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
            self.constraints
                .iter()
                .filter(|row| row.conrelid == relation_oid)
                .cloned()
                .collect()
        }
    }

    fn relation(oid: u32, relkind: char, relispartition: bool) -> BoundRelation {
        BoundRelation {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: oid,
            },
            relation_oid: oid,
            toast: None,
            namespace_oid: 0,
            owner_oid: 10,
            of_type_oid: 0,
            relpersistence: 'p',
            relkind,
            relispopulated: true,
            relispartition,
            relpartbound: None,
            desc: RelationDesc {
                columns: Vec::new(),
            },
            partitioned_table: None,
            partition_spec: None,
        }
    }

    fn relation_with_columns(oid: u32, columns: &[&str]) -> BoundRelation {
        let mut relation = relation(oid, 'r', false);
        relation.desc = RelationDesc {
            columns: columns
                .iter()
                .map(|name| column_desc(*name, SqlType::new(SqlTypeKind::Int4), false))
                .collect(),
        };
        relation
    }

    fn partitioned_parent(oid: u32, columns: &[&str]) -> BoundRelation {
        let mut relation = relation_with_columns(oid, columns);
        relation.relkind = 'p';
        relation.partitioned_table = Some(PgPartitionedTableRow {
            partrelid: oid,
            partstrat: 'l',
            partnatts: 1,
            partdefid: 0,
            partattrs: vec![1],
            partclass: vec![0],
            partcollation: vec![0],
            partexprs: None,
        });
        relation
    }

    fn constraint(
        oid: u32,
        relation_oid: u32,
        name: &str,
        contype: char,
        conkey: Option<Vec<i16>>,
    ) -> PgConstraintRow {
        PgConstraintRow {
            oid,
            conname: name.into(),
            connamespace: 0,
            contype,
            condeferrable: false,
            condeferred: false,
            conenforced: true,
            convalidated: true,
            conrelid: relation_oid,
            contypid: 0,
            conindid: 0,
            conparentid: 0,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey,
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop: None,
            conbin: None,
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod: false,
        }
    }

    #[test]
    fn partition_expr_names_strip_outer_parens_and_space_ops() {
        assert_eq!(format_partition_key_expr_name("(a+b)"), "(a + b)");
        assert_eq!(
            format_partition_key_expr_name("(lower(name))"),
            "lower(name)"
        );
    }

    #[test]
    fn describe_partition_bounds_render_psql_text() {
        let bound = PartitionBoundSpec::List {
            values: vec![
                SerializedPartitionValue::Int32(7),
                SerializedPartitionValue::Text("a'b".into()),
            ],
            is_default: false,
        };
        assert_eq!(
            describe_partition_bound_text(&bound),
            "FOR VALUES IN (7, 'a''b')"
        );

        let range = PartitionBoundSpec::Range {
            from: vec![PartitionRangeDatumValue::MinValue],
            to: vec![PartitionRangeDatumValue::Value(
                SerializedPartitionValue::Int32(-1),
            )],
            is_default: false,
        };
        assert_eq!(
            describe_partition_bound_text(&range),
            "FOR VALUES FROM (MINVALUE) TO ('-1')"
        );

        let default = PartitionBoundSpec::List {
            values: Vec::new(),
            is_default: true,
        };
        assert!(describe_partition_bound_is_default(&default));
        assert_eq!(describe_partition_bound_text(&default), "DEFAULT");
    }

    #[test]
    fn acl_items_match_effective_grantees_and_privilege() {
        let effective = BTreeSet::from(["tenant".to_string()]);
        assert!(acl_item_grants_privilege(
            "tenant=r/postgres",
            &effective,
            'r'
        ));
        assert!(!acl_item_grants_privilege(
            "tenant=w/postgres",
            &effective,
            'r'
        ));
        assert!(!acl_item_grants_privilege(
            "other=r/postgres",
            &effective,
            'r'
        ));
    }

    #[test]
    fn partition_tree_helpers_walk_declarative_parents_and_children() {
        let catalog = TestCatalog {
            relations: vec![relation(1, 'p', false), relation(2, 'r', true)],
            inherits: vec![PgInheritsRow {
                inhrelid: 2,
                inhparent: 1,
                inhseqno: 1,
                inhdetachpending: false,
            }],
            ..Default::default()
        };

        assert_eq!(partition_parent_oid(&catalog, 2).unwrap(), Some(1));
        assert_eq!(partition_root_oid(&catalog, 2).unwrap(), Some(1));
        assert_eq!(partition_ancestor_oids(&catalog, 2).unwrap(), vec![2, 1]);
        assert_eq!(
            partition_tree_entries(&catalog, 1).unwrap(),
            vec![
                PartitionTreeEntry {
                    relid: 1,
                    parentrelid: None,
                    isleaf: false,
                    level: 0,
                },
                PartitionTreeEntry {
                    relid: 2,
                    parentrelid: Some(1),
                    isleaf: true,
                    level: 1,
                },
            ]
        );
    }

    #[test]
    fn hash_bound_helpers_detect_compatible_overlaps() {
        assert!(hash_moduli_compatible(4, 16));
        assert!(hash_moduli_compatible(16, 4));
        assert!(!hash_moduli_compatible(6, 10));

        assert!(hash_bounds_overlap(4, 1, 16, 5));
        assert!(hash_bounds_overlap(16, 5, 4, 1));
        assert!(!hash_bounds_overlap(4, 1, 16, 6));
        assert!(!hash_bounds_overlap(6, 1, 10, 1));

        assert_eq!(
            hash_modulus_compatibility_detail(10, 4, "p4"),
            "The new modulus 10 is not divisible by 4, the modulus of existing partition \"p4\"."
        );
        assert_eq!(
            hash_modulus_compatibility_detail(4, 10, "p10"),
            "The new modulus 4 is not a factor of 10, the modulus of existing partition \"p10\"."
        );
    }

    #[test]
    fn column_and_not_null_helpers_resolve_child_metadata() {
        let relation = relation_with_columns(10, &["id", "Tenant"]);
        let catalog = TestCatalog {
            relations: vec![relation.clone()],
            constraints: vec![constraint(
                20,
                relation.relation_oid,
                "tenant_nn",
                CONSTRAINT_NOTNULL,
                Some(vec![2]),
            )],
            ..Default::default()
        };

        assert_eq!(column_attnum_by_name(&relation, "tenant"), Some(2));
        assert_eq!(column_name_for_attnum(&relation, 1), Some("id"));
        assert_eq!(
            not_null_constraint_for_attnum(&catalog, relation.relation_oid, 2)
                .map(|row| row.conname),
            Some("tenant_nn".into())
        );
        assert!(not_null_constraint_for_attnum(&catalog, relation.relation_oid, 1).is_none());
    }

    #[test]
    fn attach_constraint_merge_state_rejects_conflicting_child_constraints() {
        let parent = constraint(1, 100, "ck_parent", CONSTRAINT_CHECK, None);
        let mut child = constraint(2, 101, "ck_child", CONSTRAINT_CHECK, None);

        child.connoinherit = true;
        assert!(matches!(
            validate_attach_constraint_merge_state(&parent, &child, "child"),
            Err(PartitionError::Detailed {
                sqlstate: "42P17",
                ..
            })
        ));

        child.connoinherit = false;
        child.convalidated = false;
        assert!(matches!(
            validate_attach_constraint_merge_state(&parent, &child, "child"),
            Err(PartitionError::Detailed {
                sqlstate: "42P17",
                ..
            })
        ));

        child.convalidated = true;
        child.conenforced = false;
        assert!(matches!(
            validate_attach_constraint_merge_state(&parent, &child, "child"),
            Err(PartitionError::Detailed {
                sqlstate: "42P17",
                ..
            })
        ));

        child.conenforced = true;
        assert!(validate_attach_constraint_merge_state(&parent, &child, "child").is_ok());
    }

    #[test]
    fn attach_partition_constraints_require_matching_checks_and_not_nulls() {
        let parent = relation_with_columns(100, &["id", "tenant"]);
        let child = relation_with_columns(101, &["id", "tenant"]);
        let mut parent_check = constraint(1, parent.relation_oid, "ck_id", CONSTRAINT_CHECK, None);
        parent_check.conbin = Some("id > 0".into());
        let mut child_check = constraint(2, child.relation_oid, "ck_id", CONSTRAINT_CHECK, None);
        child_check.conbin = Some("id > 0".into());
        let parent_not_null = constraint(
            3,
            parent.relation_oid,
            "tenant_nn",
            CONSTRAINT_NOTNULL,
            Some(vec![2]),
        );
        let child_not_null = constraint(
            4,
            child.relation_oid,
            "tenant_nn",
            CONSTRAINT_NOTNULL,
            Some(vec![2]),
        );
        let catalog = TestCatalog {
            constraints: vec![
                parent_check,
                child_check,
                parent_not_null.clone(),
                child_not_null,
            ],
            ..Default::default()
        };

        assert!(validate_attach_partition_constraints(&catalog, &parent, &child).is_ok());

        let missing_check_catalog = TestCatalog {
            constraints: vec![parent_not_null],
            ..Default::default()
        };
        assert!(matches!(
            validate_attach_partition_constraints(&missing_check_catalog, &parent, &child),
            Err(PartitionError::Detailed {
                sqlstate: "42804",
                ..
            })
        ));
    }

    #[test]
    fn attach_partition_constraints_reject_missing_child_not_null() {
        let parent = relation_with_columns(100, &["id"]);
        let child = relation_with_columns(101, &["id"]);
        let parent_not_null = constraint(
            3,
            parent.relation_oid,
            "id_nn",
            CONSTRAINT_NOTNULL,
            Some(vec![1]),
        );
        let catalog = TestCatalog {
            constraints: vec![parent_not_null],
            ..Default::default()
        };

        assert!(matches!(
            validate_attach_partition_constraints(&catalog, &parent, &child),
            Err(PartitionError::Detailed {
                sqlstate: "42804",
                ..
            })
        ));
    }

    #[test]
    fn relation_compatibility_accepts_matching_partition_child() {
        let catalog = TestCatalog::default();
        let parent = partitioned_parent(1, &["id", "tenant"]);
        let child = relation_with_columns(2, &["ID", "tenant"]);

        assert!(validate_partition_relation_compatibility(&catalog, &parent, &child).is_ok());
    }

    #[test]
    fn relation_compatibility_rejects_non_partition_parent_and_bad_child_shape() {
        let catalog = TestCatalog::default();
        let non_partition_parent = relation_with_columns(1, &["id"]);
        let child = relation_with_columns(2, &["id"]);

        assert!(matches!(
            validate_partition_relation_compatibility(&catalog, &non_partition_parent, &child),
            Err(PartitionError::Detailed {
                sqlstate: "42809",
                ..
            })
        ));

        let parent = partitioned_parent(3, &["id"]);
        let bad_child_kind = relation(4, 'v', false);
        assert_eq!(
            validate_partition_relation_compatibility(&catalog, &parent, &bad_child_kind),
            Err(PartitionError::WrongObjectType {
                name: "4".into(),
                expected: "table",
            })
        );

        let extra_column_child = relation_with_columns(5, &["id", "extra"]);
        assert!(matches!(
            validate_partition_relation_compatibility(&catalog, &parent, &extra_column_child),
            Err(PartitionError::Detailed {
                sqlstate: "42804",
                ..
            })
        ));
    }

    #[test]
    fn relation_compatibility_rejects_inheritance_and_generation_conflicts() {
        let parent = partitioned_parent(1, &["id"]);
        let child = relation_with_columns(2, &["id"]);
        let catalog = TestCatalog {
            inherits: vec![PgInheritsRow {
                inhrelid: 2,
                inhparent: 99,
                inhseqno: 1,
                inhdetachpending: false,
            }],
            ..Default::default()
        };

        assert!(matches!(
            validate_partition_relation_compatibility(&catalog, &parent, &child),
            Err(PartitionError::Detailed {
                sqlstate: "42P16",
                ..
            })
        ));

        let mut generated_parent = partitioned_parent(3, &["id"]);
        generated_parent.desc.columns[0].generated = Some(ColumnGeneratedKind::Stored);
        let generated_child = relation_with_columns(4, &["id"]);
        assert!(matches!(
            validate_partition_relation_compatibility(
                &TestCatalog::default(),
                &generated_parent,
                &generated_child,
            ),
            Err(PartitionError::Detailed {
                sqlstate: "42804",
                ..
            })
        ));
    }
}
