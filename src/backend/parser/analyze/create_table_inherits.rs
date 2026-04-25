use std::collections::{BTreeMap, BTreeSet};

use crate::backend::access::common::toast_compression::compression_name;
use crate::backend::utils::misc::notices::push_notice;

use super::create_table::{LoweredCreateTable, lower_create_table};
use super::{
    BoundRelation, CatalogLookup, ColumnConstraint, ConstraintAttributes, CreateTableElement,
    CreateTableStatement, ParseError, RawTypeName, TableConstraint, TablePersistence,
    lower_partition_clause, validate_partitioned_index_backed_constraints,
};

#[derive(Debug, Clone)]
struct MergedColumnSpec {
    column: crate::backend::parser::ColumnDef,
    attinhcount: i16,
    attislocal: bool,
    conflicting_parent_default: bool,
}

pub fn lower_create_table_with_catalog(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
    persistence: TablePersistence,
) -> Result<LoweredCreateTable, ParseError> {
    let mut stmt_with_resolved_persistence = stmt.clone();
    stmt_with_resolved_persistence.persistence = persistence;
    let stmt = &stmt_with_resolved_persistence;

    if stmt.inherits.is_empty() && stmt.partition_of.is_none() {
        let mut lowered = lower_create_table(stmt, catalog)?;
        let partition = lower_partition_clause(stmt, &lowered.relation_desc, catalog, persistence)?;
        lowered.partition_spec = partition.spec;
        lowered.partition_parent_oid = partition.parent_oid;
        lowered.partition_bound = partition.bound;
        validate_partitioned_index_backed_constraints(
            &stmt.table_name,
            lowered.partition_spec.as_ref(),
            &lowered.constraint_actions,
        )?;
        return Ok(lowered);
    }

    let parents = resolve_parent_relations(stmt, catalog, persistence)?;
    if stmt.partition_of.is_some() && local_primary_key_count(stmt) > 0 {
        let parent_has_primary_key = parents.iter().any(|parent| {
            parent.relkind == 'p'
                && catalog
                    .constraint_rows_for_relation(parent.relation_oid)
                    .into_iter()
                    .any(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
        });
        if parent_has_primary_key {
            return Err(ParseError::DetailedError {
                message: format!(
                    "multiple primary keys for table \"{}\" are not allowed",
                    stmt.table_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
    }
    let merged_columns = merge_inherited_columns(stmt, &parents)?;
    let inherited_constraints = inherited_table_constraints(&parents, catalog);
    let mut synthetic = stmt.clone();
    synthetic.elements = merged_columns
        .iter()
        .map(|column| CreateTableElement::Column(column.column.clone()))
        .chain(
            inherited_constraints
                .iter()
                .cloned()
                .map(CreateTableElement::Constraint),
        )
        .chain(
            stmt.constraints()
                .cloned()
                .map(CreateTableElement::Constraint),
        )
        .collect();
    synthetic.inherits.clear();

    let mut lowered = lower_create_table(&synthetic, catalog)?;
    for (column, merged) in lowered
        .relation_desc
        .columns
        .iter_mut()
        .zip(merged_columns.iter())
    {
        column.attinhcount = merged.attinhcount;
        column.attislocal = merged.attislocal;
    }
    lowered.parent_oids = parents
        .into_iter()
        .map(|parent| parent.relation_oid)
        .collect();
    let partition = lower_partition_clause(stmt, &lowered.relation_desc, catalog, persistence)?;
    lowered.partition_spec = partition.spec;
    lowered.partition_parent_oid = partition.parent_oid;
    lowered.partition_bound = partition.bound;
    validate_partitioned_index_backed_constraints(
        &stmt.table_name,
        lowered.partition_spec.as_ref(),
        &lowered.constraint_actions,
    )?;
    Ok(lowered)
}

fn inherited_table_constraints(
    parents: &[BoundRelation],
    catalog: &dyn CatalogLookup,
) -> Vec<TableConstraint> {
    let mut constraints = Vec::new();
    let mut seen_checks = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();
    for parent in parents {
        for row in catalog
            .constraint_rows_for_relation(parent.relation_oid)
            .into_iter()
        {
            match row.contype {
                crate::include::catalog::CONSTRAINT_CHECK if !row.connoinherit => {
                    let Some(expr_sql) = row.conbin.clone() else {
                        continue;
                    };
                    let key = (
                        row.conname.to_ascii_lowercase(),
                        expr_sql.to_ascii_lowercase(),
                    );
                    if !seen_checks.insert(key) {
                        continue;
                    }
                    constraints.push(TableConstraint::Check {
                        attributes: ConstraintAttributes {
                            name: Some(row.conname),
                            not_valid: !row.convalidated,
                            ..ConstraintAttributes::default()
                        },
                        expr_sql,
                    });
                }
                crate::include::catalog::CONSTRAINT_PRIMARY
                | crate::include::catalog::CONSTRAINT_UNIQUE
                    if parent.relkind == 'p' =>
                {
                    let Some(columns) = row
                        .conkey
                        .as_ref()
                        .map(|attnums| inherited_constraint_columns(parent, attnums))
                    else {
                        continue;
                    };
                    let key = (
                        row.contype,
                        columns
                            .iter()
                            .map(|column| column.to_ascii_lowercase())
                            .collect::<Vec<_>>(),
                    );
                    if !seen_keys.insert(key) {
                        continue;
                    }
                    let without_overlaps = row.conperiod.then(|| columns.last().cloned()).flatten();
                    if row.contype == crate::include::catalog::CONSTRAINT_PRIMARY {
                        constraints.push(TableConstraint::PrimaryKey {
                            attributes: ConstraintAttributes::default(),
                            columns,
                            without_overlaps,
                        });
                    } else {
                        constraints.push(TableConstraint::Unique {
                            attributes: ConstraintAttributes::default(),
                            columns,
                            without_overlaps,
                        });
                    }
                }
                _ => {}
            }
        }
    }
    constraints
}

fn inherited_constraint_columns(parent: &BoundRelation, attnums: &[i16]) -> Vec<String> {
    attnums
        .iter()
        .filter_map(|attnum| {
            parent
                .desc
                .columns
                .get(attnum.saturating_sub(1) as usize)
                .filter(|column| !column.dropped)
                .map(|column| column.name.clone())
        })
        .collect()
}

fn local_primary_key_count(stmt: &CreateTableStatement) -> usize {
    let column_primary_keys = stmt.columns().filter(|column| column.primary_key()).count();
    let table_primary_keys = stmt
        .constraints()
        .filter(|constraint| matches!(constraint, TableConstraint::PrimaryKey { .. }))
        .count();
    column_primary_keys.saturating_add(table_primary_keys)
}

fn resolve_parent_relations(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
    persistence: TablePersistence,
) -> Result<Vec<BoundRelation>, ParseError> {
    let parent_names = if let Some(parent_name) = &stmt.partition_of {
        vec![parent_name.clone()]
    } else {
        stmt.inherits.clone()
    };
    let allow_partitioned_parent = stmt.partition_of.is_some();

    let mut seen = BTreeSet::new();
    let mut parents = Vec::with_capacity(parent_names.len());
    for parent_name in &parent_names {
        let parent = catalog
            .lookup_any_relation(parent_name)
            .ok_or_else(|| ParseError::UnknownTable(parent_name.clone()))?;
        if !(parent.relkind == 'r' || allow_partitioned_parent && parent.relkind == 'p') {
            return Err(ParseError::WrongObjectType {
                name: parent_name.clone(),
                expected: if allow_partitioned_parent {
                    "partitioned table"
                } else {
                    "table"
                },
            });
        }
        if !seen.insert(parent.relation_oid) {
            return Err(ParseError::DuplicateTableName(parent_name.clone()));
        }
        if allow_partitioned_parent
            && relation_persistence_code(persistence) != parent.relpersistence
        {
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
        if !allow_partitioned_parent
            && persistence == TablePersistence::Permanent
            && parent.relpersistence == 't'
        {
            return Err(ParseError::UnexpectedToken {
                expected: "permanent parent for permanent inherited table",
                actual: format!("temporary parent {}", parent_name),
            });
        }
        parents.push(parent);
    }
    Ok(parents)
}

fn relation_persistence_code(persistence: TablePersistence) -> char {
    match persistence {
        TablePersistence::Permanent => 'p',
        TablePersistence::Temporary => 't',
    }
}

fn merge_inherited_columns(
    stmt: &CreateTableStatement,
    parents: &[BoundRelation],
) -> Result<Vec<MergedColumnSpec>, ParseError> {
    let mut merged = Vec::new();
    let mut column_lookup = BTreeMap::<String, usize>::new();

    for parent in parents {
        for column in &parent.desc.columns {
            if column.dropped {
                continue;
            }
            let normalized = column.name.to_ascii_lowercase();
            let generated = column.generated.and_then(|kind| {
                column.default_expr.clone().map(|expr_sql| {
                    crate::include::nodes::parsenodes::ColumnGeneratedDef { expr_sql, kind }
                })
            });
            let inherited = crate::backend::parser::ColumnDef {
                name: column.name.clone(),
                ty: RawTypeName::Builtin(column.sql_type),
                collation: None,
                default_expr: if generated.is_some() {
                    None
                } else {
                    column.default_expr.clone()
                },
                generated,
                identity: column.identity,
                storage: Some(column.storage.attstorage),
                compression: Some(column.storage.attcompression),
                constraints: inherited_constraints_for_parent(column),
            };
            if let Some(index) = column_lookup.get(&normalized).copied() {
                merge_parent_column(&mut merged[index], &inherited)?;
            } else {
                let index = merged.len();
                merged.push(MergedColumnSpec {
                    column: inherited,
                    attinhcount: 1,
                    attislocal: false,
                    conflicting_parent_default: false,
                });
                column_lookup.insert(normalized, index);
            }
        }
    }

    for column in stmt.columns() {
        let normalized = column.name.to_ascii_lowercase();
        if let Some(index) = column_lookup.get(&normalized).copied() {
            merge_local_column(&mut merged[index], column)?;
        } else {
            let index = merged.len();
            merged.push(MergedColumnSpec {
                column: column.clone(),
                attinhcount: 0,
                attislocal: true,
                conflicting_parent_default: false,
            });
            column_lookup.insert(normalized, index);
        }
    }

    if let Some(conflict) = merged
        .iter()
        .find(|column| column.conflicting_parent_default)
    {
        return Err(ParseError::UnexpectedToken {
            expected: "compatible inherited column defaults",
            actual: format!(
                "conflicting inherited defaults for column {}",
                conflict.column.name
            ),
        });
    }

    Ok(merged)
}

fn merge_parent_column(
    merged: &mut MergedColumnSpec,
    parent: &crate::backend::parser::ColumnDef,
) -> Result<(), ParseError> {
    ensure_matching_column_type(&merged.column.name, &merged.column.ty, &parent.ty)?;
    merged.column.compression = merge_parent_column_compression(
        &merged.column.name,
        merged.column.compression,
        parent.compression,
    )?;
    push_notice(format!(
        "merging multiple inherited definitions of column \"{}\"",
        merged.column.name
    ));
    merged.attinhcount = merged.attinhcount.saturating_add(1);
    if !parent.nullable() {
        ensure_not_null_constraint(&mut merged.column);
    }
    ensure_matching_column_generated(
        &merged.column.name,
        merged.column.generated.as_ref(),
        parent.generated.as_ref(),
    )?;
    if merged.column.generated.is_none() {
        merged.column.generated = parent.generated.clone();
    }
    if merged.column.generated.is_none() {
        merged.conflicting_parent_default |= !merge_parent_default(
            &mut merged.column.default_expr,
            parent.default_expr.as_deref(),
        );
    }
    Ok(())
}

fn merge_local_column(
    merged: &mut MergedColumnSpec,
    local: &crate::backend::parser::ColumnDef,
) -> Result<(), ParseError> {
    ensure_matching_column_type(&merged.column.name, &merged.column.ty, &local.ty)?;
    if merged.attinhcount > 0 {
        push_notice(format!(
            "merging column \"{}\" with inherited definition",
            merged.column.name
        ));
    }
    merged.attislocal = true;
    if let Some(local_compression) = local.compression {
        ensure_matching_column_compression(
            &merged.column.name,
            merged.column.compression,
            Some(local_compression),
        )?;
        merged.column.compression = Some(local_compression);
    }
    if let Some(attributes) = local_not_null_constraint_attributes(local) {
        if merged.column.nullable() {
            merged.column.constraints.push(ColumnConstraint::NotNull {
                attributes: attributes.clone(),
            });
        } else if attributes.no_inherit {
            return Err(ParseError::InvalidTableDefinition(format!(
                "cannot define not-null constraint with NO INHERIT on column \"{}\"",
                merged.column.name
            )));
        }
    } else if !local.nullable() {
        ensure_not_null_constraint(&mut merged.column);
    }
    if local.primary_key() {
        ensure_primary_key_constraint(&mut merged.column);
    }
    if local.unique() {
        ensure_unique_constraint(&mut merged.column);
    }
    ensure_matching_column_generated(
        &merged.column.name,
        merged.column.generated.as_ref(),
        local.generated.as_ref(),
    )?;
    if local.generated.is_some() {
        merged.column.generated = local.generated.clone();
        merged.column.default_expr = None;
        merged.conflicting_parent_default = false;
    }
    if local.default_expr.is_some() {
        if merged.column.generated.is_some() {
            return Err(ParseError::DetailedError {
                message: format!(
                    "both default and generation expression specified for column \"{}\"",
                    merged.column.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        merged.column.default_expr = local.default_expr.clone();
        merged.conflicting_parent_default = false;
    }
    Ok(())
}

fn inherited_constraints_for_parent(
    column: &crate::include::nodes::primnodes::ColumnDesc,
) -> Vec<ColumnConstraint> {
    let mut constraints = Vec::new();
    if !column.storage.nullable && !column.not_null_constraint_no_inherit {
        constraints.push(ColumnConstraint::NotNull {
            attributes: ConstraintAttributes::default(),
        });
    }
    constraints
}

fn local_not_null_constraint_attributes(
    column: &crate::backend::parser::ColumnDef,
) -> Option<&ConstraintAttributes> {
    column
        .constraints
        .iter()
        .find_map(|constraint| match constraint {
            ColumnConstraint::NotNull { attributes } => Some(attributes),
            _ => None,
        })
}

fn ensure_not_null_constraint(column: &mut crate::backend::parser::ColumnDef) {
    if !column.nullable() {
        return;
    }
    column.constraints.push(ColumnConstraint::NotNull {
        attributes: ConstraintAttributes::default(),
    });
}

fn ensure_primary_key_constraint(column: &mut crate::backend::parser::ColumnDef) {
    if column.primary_key() {
        return;
    }
    column.constraints.push(ColumnConstraint::PrimaryKey {
        attributes: ConstraintAttributes::default(),
    });
}

fn ensure_unique_constraint(column: &mut crate::backend::parser::ColumnDef) {
    if column.unique() {
        return;
    }
    column.constraints.push(ColumnConstraint::Unique {
        attributes: ConstraintAttributes::default(),
    });
}

fn ensure_matching_column_type(
    name: &str,
    left: &RawTypeName,
    right: &RawTypeName,
) -> Result<(), ParseError> {
    if left == right {
        return Ok(());
    }
    Err(ParseError::UnexpectedToken {
        expected: "matching inherited column types",
        actual: format!("column {name} has incompatible inherited types"),
    })
}

fn ensure_matching_column_compression(
    name: &str,
    left: Option<crate::include::access::htup::AttributeCompression>,
    right: Option<crate::include::access::htup::AttributeCompression>,
) -> Result<(), ParseError> {
    if left == right {
        return Ok(());
    }
    Err(ParseError::DetailedError {
        message: format!("column \"{name}\" has a compression method conflict"),
        detail: Some(format!(
            "{} versus {}",
            format_column_compression(left),
            format_column_compression(right)
        )),
        hint: None,
        sqlstate: "42P16",
    })
}

fn ensure_matching_column_generated(
    name: &str,
    left: Option<&crate::include::nodes::parsenodes::ColumnGeneratedDef>,
    right: Option<&crate::include::nodes::parsenodes::ColumnGeneratedDef>,
) -> Result<(), ParseError> {
    match (left, right) {
        (Some(left), Some(right)) if left.kind != right.kind => Err(ParseError::DetailedError {
            message: format!("column \"{name}\" has a generation conflict"),
            detail: Some("Generated columns must be both virtual or both stored.".into()),
            hint: None,
            sqlstate: "42P16",
        }),
        (Some(_), None) | (None, Some(_)) => Err(ParseError::DetailedError {
            message: format!("column \"{name}\" has a generation conflict"),
            detail: Some(
                "A generated column cannot inherit from or merge with a normal column.".into(),
            ),
            hint: None,
            sqlstate: "42P16",
        }),
        _ => Ok(()),
    }
}

fn merge_parent_column_compression(
    name: &str,
    left: Option<crate::include::access::htup::AttributeCompression>,
    right: Option<crate::include::access::htup::AttributeCompression>,
) -> Result<Option<crate::include::access::htup::AttributeCompression>, ParseError> {
    if left == right {
        return Ok(left);
    }
    if is_default_column_compression(left) {
        return Ok(right);
    }
    if is_default_column_compression(right) {
        return Ok(left);
    }
    ensure_matching_column_compression(name, left, right)?;
    Ok(left)
}

fn is_default_column_compression(
    compression: Option<crate::include::access::htup::AttributeCompression>,
) -> bool {
    matches!(
        compression,
        None | Some(crate::include::access::htup::AttributeCompression::Default)
    )
}

fn format_column_compression(
    compression: Option<crate::include::access::htup::AttributeCompression>,
) -> &'static str {
    compression
        .map(compression_name)
        .unwrap_or(compression_name(
            crate::include::access::htup::AttributeCompression::Default,
        ))
}

fn merge_parent_default(current: &mut Option<String>, incoming: Option<&str>) -> bool {
    match (current.as_deref(), incoming) {
        (_, None) => true,
        (None, Some(incoming)) => {
            *current = Some(incoming.to_string());
            true
        }
        (Some(existing), Some(incoming)) => default_exprs_equal(existing, incoming),
    }
}

fn default_exprs_equal(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    match (
        crate::backend::parser::parse_expr(left),
        crate::backend::parser::parse_expr(right),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::access::htup::AttributeCompression;

    #[test]
    fn default_parent_compression_is_compatible_with_explicit_parent() {
        assert_eq!(
            merge_parent_column_compression(
                "f1",
                Some(AttributeCompression::Pglz),
                Some(AttributeCompression::Default),
            )
            .unwrap(),
            Some(AttributeCompression::Pglz)
        );
        assert_eq!(
            merge_parent_column_compression(
                "f1",
                Some(AttributeCompression::Default),
                Some(AttributeCompression::Pglz),
            )
            .unwrap(),
            Some(AttributeCompression::Pglz)
        );
    }

    #[test]
    fn distinct_explicit_parent_compressions_conflict() {
        let err = merge_parent_column_compression(
            "f1",
            Some(AttributeCompression::Pglz),
            Some(AttributeCompression::Lz4),
        )
        .unwrap_err();
        match err {
            ParseError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(message, "column \"f1\" has a compression method conflict");
                assert_eq!(detail.as_deref(), Some("pglz versus lz4"));
                assert_eq!(sqlstate, "42P16");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
