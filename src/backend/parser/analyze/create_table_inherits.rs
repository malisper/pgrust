use std::collections::{BTreeMap, BTreeSet};

use crate::backend::access::common::toast_compression::compression_name;
use crate::backend::utils::misc::notices::push_notice;

use super::create_table::{LoweredCreateTable, lower_create_table};
use super::{
    BoundRelation, CatalogLookup, ColumnConstraint, ConstraintAttributes, CreateTableElement,
    CreateTableStatement, ParseError, RawTypeName, TableConstraint, TablePersistence,
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
    if stmt.inherits.is_empty() {
        return lower_create_table(stmt, catalog);
    }

    let parents = resolve_parent_relations(stmt, catalog, persistence)?;
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
    Ok(lowered)
}

fn inherited_table_constraints(
    parents: &[BoundRelation],
    catalog: &dyn CatalogLookup,
) -> Vec<TableConstraint> {
    let mut constraints = Vec::new();
    let mut seen = BTreeSet::new();
    for parent in parents {
        for row in catalog
            .constraint_rows_for_relation(parent.relation_oid)
            .into_iter()
            .filter(|row| {
                row.contype == crate::include::catalog::CONSTRAINT_CHECK && !row.connoinherit
            })
        {
            let Some(expr_sql) = row.conbin.clone() else {
                continue;
            };
            let key = (
                row.conname.to_ascii_lowercase(),
                expr_sql.to_ascii_lowercase(),
            );
            if !seen.insert(key) {
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
    }
    constraints
}

fn resolve_parent_relations(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
    persistence: TablePersistence,
) -> Result<Vec<BoundRelation>, ParseError> {
    let mut seen = BTreeSet::new();
    let mut parents = Vec::with_capacity(stmt.inherits.len());
    for parent_name in &stmt.inherits {
        let parent = catalog
            .lookup_any_relation(parent_name)
            .ok_or_else(|| ParseError::UnknownTable(parent_name.clone()))?;
        if parent.relkind != 'r' {
            return Err(ParseError::WrongObjectType {
                name: parent_name.clone(),
                expected: "table",
            });
        }
        if !seen.insert(parent.relation_oid) {
            return Err(ParseError::DuplicateTableName(parent_name.clone()));
        }
        if persistence == TablePersistence::Permanent && parent.relpersistence == 't' {
            return Err(ParseError::UnexpectedToken {
                expected: "permanent parent for permanent inherited table",
                actual: format!("temporary parent {}", parent_name),
            });
        }
        parents.push(parent);
    }
    Ok(parents)
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
            let inherited = crate::backend::parser::ColumnDef {
                name: column.name.clone(),
                ty: RawTypeName::Builtin(column.sql_type),
                default_expr: column.default_expr.clone(),
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
    merged.conflicting_parent_default |= !merge_parent_default(
        &mut merged.column.default_expr,
        parent.default_expr.as_deref(),
    );
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
    if local.default_expr.is_some() {
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
