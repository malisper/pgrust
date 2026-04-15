use std::collections::{BTreeMap, BTreeSet};

use super::create_table::{LoweredCreateTable, lower_create_table};
use super::{
    BoundRelation, CatalogLookup, CreateTableElement, CreateTableStatement, ParseError, RawTypeName,
    TablePersistence,
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
        return lower_create_table(stmt);
    }

    let parents = resolve_parent_relations(stmt, catalog, persistence)?;
    let merged_columns = merge_inherited_columns(stmt, &parents)?;
    let mut synthetic = stmt.clone();
    synthetic.elements = merged_columns
        .iter()
        .map(|column| CreateTableElement::Column(column.column.clone()))
        .chain(stmt.constraints().cloned().map(CreateTableElement::Constraint))
        .collect();
    synthetic.inherits.clear();

    let mut lowered = lower_create_table(&synthetic)?;
    for (column, merged) in lowered.relation_desc.columns.iter_mut().zip(merged_columns.iter()) {
        column.attinhcount = merged.attinhcount;
        column.attislocal = merged.attislocal;
    }
    lowered.parent_oids = parents.into_iter().map(|parent| parent.relation_oid).collect();
    Ok(lowered)
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
                nullable: column.storage.nullable,
                primary_key: false,
                unique: false,
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

    if let Some(conflict) = merged.iter().find(|column| column.conflicting_parent_default) {
        return Err(ParseError::UnexpectedToken {
            expected: "compatible inherited column defaults",
            actual: format!("conflicting inherited defaults for column {}", conflict.column.name),
        });
    }

    Ok(merged)
}

fn merge_parent_column(
    merged: &mut MergedColumnSpec,
    parent: &crate::backend::parser::ColumnDef,
) -> Result<(), ParseError> {
    ensure_matching_column_type(&merged.column.name, &merged.column.ty, &parent.ty)?;
    merged.attinhcount = merged.attinhcount.saturating_add(1);
    merged.column.nullable &= parent.nullable;
    merged.conflicting_parent_default |=
        !merge_parent_default(&mut merged.column.default_expr, parent.default_expr.as_deref());
    Ok(())
}

fn merge_local_column(
    merged: &mut MergedColumnSpec,
    local: &crate::backend::parser::ColumnDef,
) -> Result<(), ParseError> {
    ensure_matching_column_type(&merged.column.name, &merged.column.ty, &local.ty)?;
    merged.attislocal = true;
    merged.column.nullable &= local.nullable;
    merged.column.primary_key |= local.primary_key;
    merged.column.unique |= local.unique;
    if local.default_expr.is_some() {
        merged.column.default_expr = local.default_expr.clone();
        merged.conflicting_parent_default = false;
    }
    Ok(())
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

fn merge_parent_default(
    current: &mut Option<String>,
    incoming: Option<&str>,
) -> bool {
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
