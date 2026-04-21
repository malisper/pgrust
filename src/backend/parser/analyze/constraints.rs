use std::collections::{BTreeMap, BTreeSet};

use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlExpr, SqlType, SqlTypeKind, parse_expr};
use crate::include::catalog::PgConstraintRow;
use crate::include::nodes::parsenodes::{
    ColumnConstraint, ConstraintAttributes, CreateTableStatement, ForeignKeyAction,
    ForeignKeyMatchType, TableConstraint, TablePersistence,
};
use crate::include::nodes::primnodes::Expr;

use super::ParseError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBackedConstraintAction {
    pub constraint_name: Option<String>,
    pub columns: Vec<String>,
    pub primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotNullConstraintAction {
    pub constraint_name: String,
    pub column: String,
    pub not_valid: bool,
    pub no_inherit: bool,
    pub primary_key_owned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintAction {
    pub constraint_name: String,
    pub expr_sql: String,
    pub not_valid: bool,
    pub no_inherit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyConstraintAction {
    pub constraint_name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_relation_oid: u32,
    pub referenced_index_oid: u32,
    // CREATE TABLE self-references are resolved after the table and its key
    // indexes exist, matching PostgreSQL's post-create FK installation order.
    pub self_referential: bool,
    pub referenced_columns: Vec<String>,
    pub match_type: ForeignKeyMatchType,
    pub on_delete: ForeignKeyAction,
    pub on_delete_set_columns: Option<Vec<String>>,
    pub on_update: ForeignKeyAction,
    pub not_valid: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BoundRelationConstraints {
    pub not_nulls: Vec<BoundNotNullConstraint>,
    pub checks: Vec<BoundCheckConstraint>,
    pub foreign_keys: Vec<BoundForeignKeyConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundNotNullConstraint {
    pub column_index: usize,
    pub constraint_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundCheckConstraint {
    pub constraint_name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundForeignKeyConstraint {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub relation_name: String,
    pub column_names: Vec<String>,
    pub column_indexes: Vec<usize>,
    pub match_type: ForeignKeyMatchType,
    pub referenced_relation_name: String,
    pub referenced_relation_oid: u32,
    pub referenced_rel: crate::backend::storage::smgr::RelFileLocator,
    pub referenced_desc: RelationDesc,
    pub referenced_column_indexes: Vec<usize>,
    pub referenced_index: super::BoundIndexRelation,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundReferencedByForeignKey {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub child_relation_name: String,
    pub child_relation_oid: u32,
    pub child_rel: crate::backend::storage::smgr::RelFileLocator,
    pub child_toast: Option<crate::include::nodes::execnodes::ToastRelationRef>,
    pub child_desc: RelationDesc,
    pub child_column_indexes: Vec<usize>,
    pub referenced_column_names: Vec<String>,
    pub referenced_column_indexes: Vec<usize>,
    pub child_index: Option<super::BoundIndexRelation>,
    pub on_delete: ForeignKeyAction,
    pub on_delete_set_column_indexes: Option<Vec<usize>>,
    pub on_update: ForeignKeyAction,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedCreateTableConstraints {
    pub not_nulls: Vec<NotNullConstraintAction>,
    pub checks: Vec<CheckConstraintAction>,
    pub index_backed: Vec<IndexBackedConstraintAction>,
    pub foreign_keys: Vec<ForeignKeyConstraintAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NormalizedAddColumnConstraints {
    pub not_null: Option<NotNullConstraintAction>,
    pub checks: Vec<CheckConstraintAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedAlterTableConstraint {
    NotNull(NotNullConstraintAction),
    Check(CheckConstraintAction),
    IndexBacked(IndexBackedConstraintAction),
    ForeignKey(ForeignKeyConstraintAction),
}

#[derive(Debug, Clone)]
struct PendingIndexConstraint {
    explicit_name: Option<String>,
    generated_base: String,
    columns: Vec<String>,
    primary: bool,
}

#[derive(Debug, Clone)]
struct PendingCheckConstraint {
    explicit_name: Option<String>,
    generated_base: String,
    expr_sql: String,
    not_valid: bool,
    no_inherit: bool,
}

#[derive(Debug, Clone)]
struct PendingNotNullConstraint {
    explicit_name: Option<String>,
    generated_base: String,
    column: String,
    not_valid: bool,
    no_inherit: bool,
    primary_key_owned: bool,
    column_index: usize,
}

#[derive(Debug, Clone)]
struct PendingForeignKeyConstraint {
    explicit_name: Option<String>,
    generated_base: String,
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Option<Vec<String>>,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<Vec<String>>,
    on_update: ForeignKeyAction,
    not_valid: bool,
    enforced: bool,
}

#[derive(Debug, Clone)]
struct ResolvedReferencedKey {
    relation: super::BoundRelation,
    columns: Vec<String>,
    index_oid: u32,
}

fn table_persistence_code(persistence: TablePersistence) -> char {
    match persistence {
        TablePersistence::Permanent => 'p',
        TablePersistence::Temporary => 't',
    }
}

fn validate_foreign_key_persistence(
    child_persistence: char,
    referenced_persistence: char,
) -> Result<(), ParseError> {
    match child_persistence {
        'p' if referenced_persistence != 'p' => Err(ParseError::InvalidTableDefinition(
            "constraints on permanent tables may reference only permanent tables".into(),
        )),
        't' if referenced_persistence != 't' => Err(ParseError::InvalidTableDefinition(
            "constraints on temporary tables may reference only temporary tables".into(),
        )),
        _ => Ok(()),
    }
}

pub fn normalize_create_table_constraints(
    stmt: &CreateTableStatement,
    catalog: &dyn super::CatalogLookup,
) -> Result<NormalizedCreateTableConstraints, ParseError> {
    let columns = stmt.columns().cloned().collect::<Vec<_>>();
    let column_lookup = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.to_ascii_lowercase(), index))
        .collect::<BTreeMap<_, _>>();

    let mut index_constraints = Vec::new();
    let mut check_constraints = Vec::new();
    let mut not_nulls = BTreeMap::<String, PendingNotNullConstraint>::new();
    let mut foreign_keys = Vec::new();

    for column in &columns {
        for constraint in &column.constraints {
            match constraint {
                ColumnConstraint::NotNull { attributes } => {
                    validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
                    merge_not_null_constraint(
                        &mut not_nulls,
                        &column_lookup,
                        &column.name,
                        attributes,
                        false,
                        stmt.table_name.as_str(),
                    )?;
                }
                ColumnConstraint::Check {
                    attributes,
                    expr_sql,
                } => {
                    validate_check_attributes(attributes)?;
                    check_constraints.push(PendingCheckConstraint {
                        explicit_name: attributes.name.clone(),
                        generated_base: format!("{}_{}_check", stmt.table_name, column.name),
                        expr_sql: expr_sql.clone(),
                        not_valid: attributes.not_valid,
                        no_inherit: attributes.no_inherit,
                    });
                }
                ColumnConstraint::PrimaryKey { attributes } => {
                    validate_key_attributes(attributes, "PRIMARY KEY")?;
                    index_constraints.push(PendingIndexConstraint {
                        explicit_name: attributes.name.clone(),
                        generated_base: format!("{}_pkey", stmt.table_name),
                        columns: vec![column.name.clone()],
                        primary: true,
                    });
                }
                ColumnConstraint::Unique { attributes } => {
                    validate_key_attributes(attributes, "UNIQUE")?;
                    index_constraints.push(PendingIndexConstraint {
                        explicit_name: attributes.name.clone(),
                        generated_base: format!("{}_{}_key", stmt.table_name, column.name),
                        columns: vec![column.name.clone()],
                        primary: false,
                    });
                }
                ColumnConstraint::References {
                    attributes,
                    referenced_table,
                    referenced_columns,
                    match_type,
                    on_delete,
                    on_delete_set_columns,
                    on_update,
                } => {
                    validate_create_foreign_key(
                        attributes,
                        *match_type,
                        *on_delete,
                        on_delete_set_columns.as_deref(),
                        *on_update,
                    )?;
                    foreign_keys.push(PendingForeignKeyConstraint {
                        explicit_name: attributes.name.clone(),
                        generated_base: format!("{}_{}_fkey", stmt.table_name, column.name),
                        columns: vec![column.name.clone()],
                        referenced_table: referenced_table.clone(),
                        referenced_columns: referenced_columns.clone(),
                        match_type: *match_type,
                        on_delete: *on_delete,
                        on_delete_set_columns: on_delete_set_columns.clone(),
                        on_update: *on_update,
                        not_valid: attributes.not_valid,
                        enforced: attributes.enforced.unwrap_or(true),
                    });
                }
            }
        }
    }

    for constraint in stmt.constraints() {
        match constraint {
            TableConstraint::NotNull { attributes, column } => {
                validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
                merge_not_null_constraint(
                    &mut not_nulls,
                    &column_lookup,
                    column,
                    attributes,
                    false,
                    stmt.table_name.as_str(),
                )?;
            }
            TableConstraint::Check {
                attributes,
                expr_sql,
            } => {
                validate_check_attributes(attributes)?;
                check_constraints.push(PendingCheckConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: format!("{}_check", stmt.table_name),
                    expr_sql: expr_sql.clone(),
                    not_valid: attributes.not_valid,
                    no_inherit: attributes.no_inherit,
                });
            }
            TableConstraint::PrimaryKey {
                attributes,
                columns: key_columns,
            } => {
                validate_key_attributes(attributes, "PRIMARY KEY")?;
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: format!("{}_pkey", stmt.table_name),
                    columns: resolve_constraint_columns(key_columns, &columns, &column_lookup)?,
                    primary: true,
                });
            }
            TableConstraint::Unique {
                attributes,
                columns: key_columns,
            } => {
                validate_key_attributes(attributes, "UNIQUE")?;
                let resolved = resolve_constraint_columns(key_columns, &columns, &column_lookup)?;
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: format!("{}_{}_key", stmt.table_name, resolved.join("_")),
                    columns: resolved,
                    primary: false,
                });
            }
            TableConstraint::ForeignKey {
                attributes,
                columns: key_columns,
                referenced_table,
                referenced_columns,
                match_type,
                on_delete,
                on_delete_set_columns,
                on_update,
            } => {
                validate_create_foreign_key(
                    attributes,
                    *match_type,
                    *on_delete,
                    on_delete_set_columns.as_deref(),
                    *on_update,
                )?;
                let resolved = resolve_constraint_columns(key_columns, &columns, &column_lookup)?;
                foreign_keys.push(PendingForeignKeyConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: format!("{}_{}_fkey", stmt.table_name, resolved.join("_")),
                    columns: resolved,
                    referenced_table: referenced_table.clone(),
                    referenced_columns: referenced_columns.clone(),
                    match_type: *match_type,
                    on_delete: *on_delete,
                    on_delete_set_columns: on_delete_set_columns.clone(),
                    on_update: *on_update,
                    not_valid: attributes.not_valid,
                    enforced: attributes.enforced.unwrap_or(true),
                });
            }
        }
    }

    if index_constraints
        .iter()
        .filter(|constraint| constraint.primary)
        .count()
        > 1
    {
        return Err(ParseError::UnexpectedToken {
            expected: "at most one PRIMARY KEY",
            actual: "multiple PRIMARY KEY constraints".into(),
        });
    }

    for primary in index_constraints
        .iter()
        .filter(|constraint| constraint.primary)
    {
        for column in &primary.columns {
            merge_not_null_constraint(
                &mut not_nulls,
                &column_lookup,
                column,
                &ConstraintAttributes::default(),
                true,
                stmt.table_name.as_str(),
            )?;
        }
    }

    let mut used_names = BTreeSet::new();
    reserve_explicit_constraint_names(
        &mut used_names,
        not_nulls
            .values()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        check_constraints
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        index_constraints
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        foreign_keys
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;

    let mut finalized_not_nulls = not_nulls.into_values().collect::<Vec<_>>();
    finalized_not_nulls.sort_by_key(|constraint| constraint.column_index);
    let finalized_not_nulls = finalized_not_nulls
        .into_iter()
        .map(|constraint| NotNullConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            column: constraint.column,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
            primary_key_owned: constraint.primary_key_owned,
        })
        .collect();

    let finalized_checks = check_constraints
        .into_iter()
        .map(|constraint| CheckConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            expr_sql: constraint.expr_sql,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
        })
        .collect();

    let finalized_index_backed: Vec<IndexBackedConstraintAction> = index_constraints
        .into_iter()
        .map(|constraint| IndexBackedConstraintAction {
            constraint_name: Some(constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            })),
            columns: constraint.columns,
            primary: constraint.primary,
        })
        .collect();

    let finalized_foreign_keys = foreign_keys
        .into_iter()
        .map(|constraint| {
            let local_columns =
                resolve_constraint_columns(&constraint.columns, &columns, &column_lookup)?;
            let child_types = local_columns
                .iter()
                .map(|column| {
                    let index = *column_lookup
                        .get(&column.to_ascii_lowercase())
                        .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
                    super::resolve_raw_type_name(&columns[index].ty, catalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (
                referenced_table,
                referenced_relation_oid,
                referenced_index_oid,
                self_referential,
                referenced_columns,
            ) = if constraint
                .referenced_table
                .eq_ignore_ascii_case(&stmt.table_name)
            {
                let referenced_columns = resolve_pending_self_referenced_key(
                    &stmt.table_name,
                    &columns,
                    &column_lookup,
                    &finalized_index_backed,
                    constraint.referenced_columns.as_deref(),
                    &child_types,
                    catalog,
                )?;
                (stmt.table_name.clone(), 0, 0, true, referenced_columns)
            } else {
                let referenced = resolve_referenced_key(
                    &stmt.table_name,
                    None,
                    table_persistence_code(stmt.persistence),
                    &constraint.referenced_table,
                    constraint.referenced_columns.as_deref(),
                    &child_types,
                    catalog,
                )?;
                (
                    relation_display_name(
                        catalog,
                        referenced.relation.relation_oid,
                        &constraint.referenced_table,
                    ),
                    referenced.relation.relation_oid,
                    referenced.index_oid,
                    false,
                    referenced.columns,
                )
            };
            Ok(ForeignKeyConstraintAction {
                constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                    choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
                }),
                columns: local_columns,
                referenced_table,
                referenced_relation_oid,
                referenced_index_oid,
                self_referential,
                referenced_columns,
                match_type: constraint.match_type,
                on_delete: constraint.on_delete,
                on_delete_set_columns: resolve_foreign_key_delete_set_columns(
                    constraint.on_delete_set_columns.as_deref(),
                    &constraint.columns,
                )?,
                on_update: constraint.on_update,
                not_valid: constraint.not_valid,
                enforced: constraint.enforced,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(NormalizedCreateTableConstraints {
        not_nulls: finalized_not_nulls,
        checks: finalized_checks,
        index_backed: finalized_index_backed,
        foreign_keys: finalized_foreign_keys,
    })
}

pub fn bind_relation_constraints(
    relation_name: Option<&str>,
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundRelationConstraints, ParseError> {
    let rows = catalog.constraint_rows_for_relation(relation_oid);
    let not_nulls = rows
        .iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_NOTNULL)
        .filter_map(|row| {
            let attnum = *row.conkey.as_ref()?.first()?;
            Some(BoundNotNullConstraint {
                column_index: attnum.saturating_sub(1) as usize,
                constraint_name: row.conname.clone(),
            })
        })
        .collect::<Vec<_>>();
    let checks = rows
        .iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_CHECK)
        .map(|row| {
            let expr_sql = row.conbin.clone().ok_or(ParseError::UnexpectedToken {
                expected: "stored CHECK constraint expression",
                actual: format!("missing expression for constraint {}", row.conname),
            })?;
            Ok(BoundCheckConstraint {
                constraint_name: row.conname.clone(),
                expr: bind_check_constraint_expr(&expr_sql, relation_name, desc, catalog)?,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let foreign_keys = rows
        .into_iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_FOREIGN)
        .map(|row| bind_outbound_foreign_key_constraint(relation_oid, desc, row, catalog))
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(BoundRelationConstraints {
        not_nulls,
        checks,
        foreign_keys,
    })
}

pub fn bind_referenced_by_foreign_keys(
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Vec<BoundReferencedByForeignKey>, ParseError> {
    catalog
        .constraint_rows()
        .into_iter()
        .filter(|row| {
            row.contype == crate::include::catalog::CONSTRAINT_FOREIGN
                && row.confrelid == relation_oid
        })
        .map(|row| bind_inbound_foreign_key_constraint(relation_oid, desc, row, catalog))
        .collect()
}

pub fn normalize_alter_table_add_constraint(
    table_name: &str,
    relation_oid: u32,
    relpersistence: char,
    desc: &RelationDesc,
    existing_constraints: &[PgConstraintRow],
    constraint: &TableConstraint,
    catalog: &dyn super::CatalogLookup,
) -> Result<NormalizedAlterTableConstraint, ParseError> {
    let column_lookup = relation_column_lookup(desc);
    let mut used_names = existing_constraint_names(existing_constraints);

    match constraint {
        TableConstraint::NotNull { attributes, column } => {
            validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
            let column_index = *column_lookup
                .get(&column.to_ascii_lowercase())
                .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
            if desc.columns[column_index].storage.nullable {
                let constraint_name = assign_constraint_name(
                    attributes.name.clone(),
                    format!("{table_name}_{column}_not_null"),
                    &mut used_names,
                )?;
                Ok(NormalizedAlterTableConstraint::NotNull(
                    NotNullConstraintAction {
                        constraint_name,
                        column: desc.columns[column_index].name.clone(),
                        not_valid: attributes.not_valid,
                        no_inherit: attributes.no_inherit,
                        primary_key_owned: false,
                    },
                ))
            } else {
                let existing = &desc.columns[column_index];
                if existing.not_null_constraint_no_inherit != attributes.no_inherit {
                    Err(ParseError::InvalidTableDefinition(format!(
                        "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                        existing
                            .not_null_constraint_name
                            .as_deref()
                            .unwrap_or(column),
                        table_name,
                    )))
                } else {
                    Err(ParseError::UnexpectedToken {
                        expected: "nullable column for NOT NULL constraint",
                        actual: format!("column \"{}\" is already marked NOT NULL", column),
                    })
                }
            }
        }
        TableConstraint::Check {
            attributes,
            expr_sql,
        } => {
            validate_check_attributes(attributes)?;
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                format!("{table_name}_check"),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::Check(
                CheckConstraintAction {
                    constraint_name,
                    expr_sql: expr_sql.clone(),
                    not_valid: attributes.not_valid,
                    no_inherit: attributes.no_inherit,
                },
            ))
        }
        TableConstraint::PrimaryKey {
            attributes,
            columns,
        } => {
            validate_key_attributes(attributes, "PRIMARY KEY")?;
            if existing_constraints
                .iter()
                .any(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "at most one PRIMARY KEY",
                    actual: "multiple PRIMARY KEY constraints".into(),
                });
            }
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                format!("{table_name}_pkey"),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    columns: resolve_relation_constraint_columns(columns, desc, &column_lookup)?,
                    primary: true,
                },
            ))
        }
        TableConstraint::Unique {
            attributes,
            columns,
        } => {
            validate_key_attributes(attributes, "UNIQUE")?;
            let resolved = resolve_relation_constraint_columns(columns, desc, &column_lookup)?;
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                format!("{table_name}_{}_key", resolved.join("_")),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    columns: resolved,
                    primary: false,
                },
            ))
        }
        TableConstraint::ForeignKey {
            attributes,
            columns,
            referenced_table,
            referenced_columns,
            match_type,
            on_delete,
            on_delete_set_columns,
            on_update,
        } => {
            validate_alter_foreign_key(
                attributes,
                *match_type,
                *on_delete,
                on_delete_set_columns.as_deref(),
                *on_update,
            )?;
            let resolved = resolve_relation_constraint_columns(columns, desc, &column_lookup)?;
            let child_types = resolved
                .iter()
                .map(|column| {
                    let index = *column_lookup
                        .get(&column.to_ascii_lowercase())
                        .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
                    Ok(desc.columns[index].sql_type)
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            let referenced = resolve_referenced_key(
                table_name,
                Some(relation_oid),
                relpersistence,
                referenced_table,
                referenced_columns.as_deref(),
                &child_types,
                catalog,
            )?;
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                format!("{table_name}_{}_fkey", resolved.join("_")),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::ForeignKey(
                ForeignKeyConstraintAction {
                    constraint_name,
                    columns: resolved,
                    referenced_table: relation_display_name(
                        catalog,
                        referenced.relation.relation_oid,
                        referenced_table,
                    ),
                    referenced_relation_oid: referenced.relation.relation_oid,
                    referenced_index_oid: referenced.index_oid,
                    self_referential: false,
                    referenced_columns: referenced.columns,
                    match_type: *match_type,
                    on_delete: *on_delete,
                    on_delete_set_columns: resolve_foreign_key_delete_set_columns(
                        on_delete_set_columns.as_deref(),
                        columns,
                    )?,
                    on_update: *on_update,
                    not_valid: attributes.not_valid,
                    enforced: attributes.enforced.unwrap_or(true),
                },
            ))
        }
    }
}

pub fn normalize_alter_table_add_column_constraints(
    table_name: &str,
    column: &crate::include::nodes::parsenodes::ColumnDef,
    existing_constraints: &[PgConstraintRow],
) -> Result<NormalizedAddColumnConstraints, ParseError> {
    let column_lookup = BTreeMap::from([(column.name.to_ascii_lowercase(), 0usize)]);
    let mut not_nulls = BTreeMap::<String, PendingNotNullConstraint>::new();
    let mut checks = Vec::new();

    for constraint in &column.constraints {
        match constraint {
            ColumnConstraint::NotNull { attributes } => {
                validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
                merge_not_null_constraint(
                    &mut not_nulls,
                    &column_lookup,
                    &column.name,
                    attributes,
                    false,
                    table_name,
                )?;
            }
            ColumnConstraint::Check {
                attributes,
                expr_sql,
            } => {
                validate_check_attributes(attributes)?;
                checks.push(PendingCheckConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: format!("{table_name}_{}_check", column.name),
                    expr_sql: expr_sql.clone(),
                    not_valid: attributes.not_valid,
                    no_inherit: attributes.no_inherit,
                });
            }
            ColumnConstraint::PrimaryKey { .. }
            | ColumnConstraint::Unique { .. }
            | ColumnConstraint::References { .. } => {}
        }
    }

    let mut used_names = existing_constraint_names(existing_constraints);
    reserve_explicit_constraint_names(
        &mut used_names,
        not_nulls
            .values()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        checks
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;

    let not_null = not_nulls
        .into_values()
        .next()
        .map(|constraint| NotNullConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            column: constraint.column,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
            primary_key_owned: false,
        });
    let checks = checks
        .into_iter()
        .map(|constraint| CheckConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            expr_sql: constraint.expr_sql,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
        })
        .collect();

    Ok(NormalizedAddColumnConstraints { not_null, checks })
}

pub fn generated_not_null_constraint_name(
    table_name: &str,
    column_name: &str,
    existing_constraints: &[PgConstraintRow],
) -> String {
    let mut used_names = existing_constraint_names(existing_constraints);
    choose_generated_constraint_name(
        &format!("{table_name}_{column_name}_not_null"),
        &mut used_names,
    )
}

pub fn bind_check_constraint_expr(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    bind_check_constraint_sql_expr(&parsed, relation_name, desc, catalog)
}

pub fn bind_check_constraint_sql_expr(
    expr: &SqlExpr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let scope = super::scope_for_relation(relation_name, desc);
    let inferred = super::infer_sql_expr_type_with_ctes(expr, &scope, catalog, &[], None, &[]);
    if inferred != SqlType::new(SqlTypeKind::Bool) {
        return Err(ParseError::UnexpectedToken {
            expected: "boolean CHECK constraint expression",
            actual: "CHECK constraint expression must return boolean".into(),
        });
    }

    let bound = super::bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &[])?;
    reject_unsupported_check_expr(&bound)?;
    Ok(bound)
}

pub(crate) fn infer_relation_expr_sql_type(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<SqlType, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    let scope = super::scope_for_relation(relation_name, desc);
    Ok(super::infer_sql_expr_type_with_ctes(
        &parsed,
        &scope,
        catalog,
        &[],
        None,
        &[],
    ))
}

pub(crate) fn bind_relation_expr(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    let scope = super::scope_for_relation(relation_name, desc);
    super::bind_expr_with_outer_and_ctes(&parsed, &scope, catalog, &[], None, &[])
}

fn validate_not_null_or_check_attributes(
    attributes: &ConstraintAttributes,
    constraint_kind: &'static str,
) -> Result<(), ParseError> {
    if attributes.deferrable.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} DEFERRABLE"
        )));
    }
    if attributes.initially_deferred.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} INITIALLY"
        )));
    }
    if attributes.enforced.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} ENFORCED/NOT ENFORCED"
        )));
    }
    Ok(())
}

fn validate_check_attributes(attributes: &ConstraintAttributes) -> Result<(), ParseError> {
    if attributes.deferrable.is_some() {
        return Err(ParseError::FeatureNotSupported("CHECK DEFERRABLE".into()));
    }
    if attributes.initially_deferred.is_some() {
        return Err(ParseError::FeatureNotSupported("CHECK INITIALLY".into()));
    }
    if attributes.enforced.is_some() {
        return Err(ParseError::FeatureNotSupported(
            "CHECK ENFORCED/NOT ENFORCED".into(),
        ));
    }
    Ok(())
}

fn validate_key_attributes(
    attributes: &ConstraintAttributes,
    constraint_kind: &'static str,
) -> Result<(), ParseError> {
    if attributes.not_valid {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} NOT VALID"
        )));
    }
    validate_not_null_or_check_attributes(attributes, constraint_kind)
}

fn validate_create_foreign_key(
    attributes: &ConstraintAttributes,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<&[String]>,
    on_update: ForeignKeyAction,
) -> Result<(), ParseError> {
    validate_foreign_key(
        attributes,
        match_type,
        on_delete,
        on_delete_set_columns,
        on_update,
    )
}

fn validate_alter_foreign_key(
    attributes: &ConstraintAttributes,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<&[String]>,
    on_update: ForeignKeyAction,
) -> Result<(), ParseError> {
    validate_foreign_key(
        attributes,
        match_type,
        on_delete,
        on_delete_set_columns,
        on_update,
    )
}

fn validate_foreign_key(
    attributes: &ConstraintAttributes,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<&[String]>,
    _on_update: ForeignKeyAction,
) -> Result<(), ParseError> {
    if attributes.deferrable.is_some() || attributes.initially_deferred.is_some() {
        if attributes.enforced == Some(false) {
            return Err(ParseError::FeatureNotSupported(
                "FOREIGN KEY NOT ENFORCED with DEFERRABLE/INITIALLY".into(),
            ));
        }
    }
    if match_type == ForeignKeyMatchType::Partial {
        return Err(ParseError::FeatureNotSupported(format!(
            "FOREIGN KEY MATCH {}",
            foreign_key_match_keyword(match_type)
        )));
    }
    if on_delete_set_columns.is_some()
        && !matches!(
            on_delete,
            ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault
        )
    {
        return Err(ParseError::FeatureNotSupported(
            "ON DELETE column lists require SET NULL or SET DEFAULT".into(),
        ));
    }
    Ok(())
}

fn resolve_foreign_key_delete_set_columns(
    delete_set_columns: Option<&[String]>,
    foreign_key_columns: &[String],
) -> Result<Option<Vec<String>>, ParseError> {
    let Some(delete_set_columns) = delete_set_columns else {
        return Ok(None);
    };
    let mut resolved = Vec::new();
    let mut seen = BTreeSet::new();
    for column_name in delete_set_columns {
        let Some(foreign_key_column) = foreign_key_columns
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(column_name))
        else {
            return Err(ParseError::FeatureNotSupported(format!(
                "column \"{column_name}\" referenced in ON DELETE SET action must be part of foreign key"
            )));
        };
        let normalized = foreign_key_column.to_ascii_lowercase();
        if seen.insert(normalized) {
            resolved.push(foreign_key_column.clone());
        }
    }
    Ok(Some(resolved))
}

fn resolve_pending_self_referenced_key(
    table_name: &str,
    column_defs: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
    index_constraints: &[IndexBackedConstraintAction],
    referenced_columns: Option<&[String]>,
    child_types: &[SqlType],
    catalog: &dyn super::CatalogLookup,
) -> Result<Vec<String>, ParseError> {
    let referenced_columns = if let Some(referenced_columns) = referenced_columns {
        let referenced_columns =
            resolve_constraint_columns(referenced_columns, column_defs, column_lookup)?;
        let referenced_key = referenced_columns
            .iter()
            .map(|column| column.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let matched = index_constraints.iter().find(|constraint| {
            constraint
                .columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect::<Vec<_>>()
                == referenced_key
        });
        if matched.is_none() {
            return Err(ParseError::UnexpectedToken {
                expected: "referenced UNIQUE or PRIMARY KEY index",
                actual: format!("table \"{table_name}\" lacks an exact matching unique key"),
            });
        }
        referenced_columns
    } else {
        let primary = index_constraints
            .iter()
            .filter(|constraint| constraint.primary)
            .collect::<Vec<_>>();
        if primary.len() != 1 {
            return Err(ParseError::UnexpectedToken {
                expected: "referenced PRIMARY KEY",
                actual: if primary.is_empty() {
                    format!("table \"{table_name}\" has no PRIMARY KEY")
                } else {
                    format!("table \"{table_name}\" has multiple PRIMARY KEY constraints")
                },
            });
        }
        primary[0].columns.clone()
    };

    if child_types.len() != referenced_columns.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "matching foreign-key column counts",
            actual: format!(
                "{} referencing column(s) for {} local column(s)",
                referenced_columns.len(),
                child_types.len()
            ),
        });
    }

    let parent_types = referenced_columns
        .iter()
        .map(|column| {
            let index = *column_lookup
                .get(&column.to_ascii_lowercase())
                .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
            super::resolve_raw_type_name(&column_defs[index].ty, catalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if child_types != parent_types {
        return Err(ParseError::FeatureNotSupported(
            "FOREIGN KEY with cross-type columns".into(),
        ));
    }

    Ok(referenced_columns)
}

fn resolve_referenced_key(
    child_relation_name: &str,
    child_relation_oid: Option<u32>,
    child_persistence: char,
    referenced_table: &str,
    referenced_columns: Option<&[String]>,
    child_types: &[SqlType],
    catalog: &dyn super::CatalogLookup,
) -> Result<ResolvedReferencedKey, ParseError> {
    let relation = catalog
        .lookup_relation(referenced_table)
        .ok_or_else(|| ParseError::UnknownTable(referenced_table.to_string()))?;
    let _ = child_relation_name;
    let _ = child_relation_oid;
    validate_foreign_key_persistence(child_persistence, relation.relpersistence)?;

    let relation_lookup = relation_column_lookup(&relation.desc);
    let (columns, referenced_attnums, index_oid) = if let Some(referenced_columns) =
        referenced_columns
    {
        let columns = resolve_relation_constraint_columns(
            referenced_columns,
            &relation.desc,
            &relation_lookup,
        )?;
        let attnums = column_attnums_for_names(&relation.desc, &columns)?;
        let index = find_exact_index_for_attnums(catalog, relation.relation_oid, &attnums, true)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "referenced UNIQUE or PRIMARY KEY index",
                actual: format!(
                    "table \"{}\" lacks an exact matching unique key",
                    referenced_table
                ),
            })?;
        (columns, attnums, index.relation_oid)
    } else {
        let primary = catalog
            .constraint_rows_for_relation(relation.relation_oid)
            .into_iter()
            .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
            .collect::<Vec<_>>();
        if primary.len() != 1 {
            return Err(ParseError::UnexpectedToken {
                expected: "referenced PRIMARY KEY",
                actual: if primary.is_empty() {
                    format!("table \"{}\" has no PRIMARY KEY", referenced_table)
                } else {
                    format!(
                        "table \"{}\" has multiple PRIMARY KEY constraints",
                        referenced_table
                    )
                },
            });
        }
        let row = &primary[0];
        let attnums = constraint_attnums(row, "PRIMARY KEY")?;
        let columns = attnums_to_column_names(&relation.desc, &attnums)?;
        (columns, attnums, row.conindid)
    };

    if child_types.len() != referenced_attnums.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "matching foreign-key column counts",
            actual: format!(
                "{} referencing column(s) for {} local column(s)",
                referenced_attnums.len(),
                child_types.len()
            ),
        });
    }

    let parent_types = referenced_attnums
        .iter()
        .map(|&attnum| {
            column_index_for_attnum(&relation.desc, attnum)
                .map(|index| relation.desc.columns[index].sql_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if child_types != parent_types {
        return Err(ParseError::FeatureNotSupported(
            "FOREIGN KEY with cross-type columns".into(),
        ));
    }

    Ok(ResolvedReferencedKey {
        relation,
        columns,
        index_oid,
    })
}

fn bind_outbound_foreign_key_constraint(
    relation_oid: u32,
    desc: &RelationDesc,
    row: PgConstraintRow,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundForeignKeyConstraint, ParseError> {
    let local_attnums = constraint_attnums(&row, "FOREIGN KEY")?;
    let referenced_attnums = row
        .confkey
        .clone()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "referenced foreign-key columns",
            actual: format!("missing confkey for constraint {}", row.conname),
        })?;
    let referenced_relation = catalog
        .lookup_relation_by_oid(row.confrelid)
        .or_else(|| catalog.relation_by_oid(row.confrelid))
        .ok_or_else(|| ParseError::UnknownTable(row.confrelid.to_string()))?;
    let referenced_index = catalog
        .index_relations_for_heap(referenced_relation.relation_oid)
        .into_iter()
        .find(|index| index.relation_oid == row.conindid)
        .or_else(|| {
            find_exact_index_for_attnums(
                catalog,
                referenced_relation.relation_oid,
                &referenced_attnums,
                true,
            )
        })
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "referenced foreign-key index",
            actual: format!("missing referenced index {}", row.conindid),
        })?;
    Ok(BoundForeignKeyConstraint {
        constraint_oid: row.oid,
        constraint_name: row.conname,
        relation_name: relation_display_name(catalog, relation_oid, &relation_oid.to_string()),
        column_names: attnums_to_column_names(desc, &local_attnums)?,
        column_indexes: attnums_to_column_indexes(desc, &local_attnums)?,
        match_type: foreign_key_match_from_code(row.confmatchtype)?,
        referenced_relation_name: relation_display_name(
            catalog,
            referenced_relation.relation_oid,
            &row.confrelid.to_string(),
        ),
        referenced_relation_oid: referenced_relation.relation_oid,
        referenced_rel: referenced_relation.rel,
        referenced_desc: referenced_relation.desc.clone(),
        referenced_column_indexes: attnums_to_column_indexes(
            &referenced_relation.desc,
            &referenced_attnums,
        )?,
        referenced_index,
        deferrable: row.condeferrable,
        initially_deferred: row.condeferred,
        enforced: row.conenforced,
    })
}

fn bind_inbound_foreign_key_constraint(
    _relation_oid: u32,
    desc: &RelationDesc,
    row: PgConstraintRow,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundReferencedByForeignKey, ParseError> {
    let child_relation = catalog
        .lookup_relation_by_oid(row.conrelid)
        .or_else(|| catalog.relation_by_oid(row.conrelid))
        .ok_or_else(|| ParseError::UnknownTable(row.conrelid.to_string()))?;
    let child_attnums = constraint_attnums(&row, "FOREIGN KEY")?;
    let referenced_attnums = row
        .confkey
        .clone()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "referenced foreign-key columns",
            actual: format!("missing confkey for constraint {}", row.conname),
        })?;
    Ok(BoundReferencedByForeignKey {
        constraint_oid: row.oid,
        constraint_name: row.conname,
        child_relation_name: relation_display_name(catalog, child_relation.relation_oid, "<child>"),
        child_relation_oid: child_relation.relation_oid,
        child_rel: child_relation.rel,
        child_toast: child_relation.toast,
        child_desc: child_relation.desc.clone(),
        child_column_indexes: attnums_to_column_indexes(&child_relation.desc, &child_attnums)?,
        referenced_column_names: attnums_to_column_names(desc, &referenced_attnums)?,
        referenced_column_indexes: attnums_to_column_indexes(desc, &referenced_attnums)?,
        child_index: find_exact_index_for_attnums(
            catalog,
            child_relation.relation_oid,
            &child_attnums,
            false,
        ),
        on_delete: foreign_key_action_from_code(row.confdeltype)?,
        on_delete_set_column_indexes: row
            .confdelsetcols
            .as_ref()
            .map(|attnums| attnums_to_column_indexes(&child_relation.desc, attnums))
            .transpose()?,
        on_update: foreign_key_action_from_code(row.confupdtype)?,
        deferrable: row.condeferrable,
        initially_deferred: row.condeferred,
        enforced: row.conenforced,
    })
}

fn merge_not_null_constraint(
    not_nulls: &mut BTreeMap<String, PendingNotNullConstraint>,
    column_lookup: &BTreeMap<String, usize>,
    column_name: &str,
    attributes: &ConstraintAttributes,
    primary_key_owned: bool,
    relation_name: &str,
) -> Result<(), ParseError> {
    let normalized = column_name.to_ascii_lowercase();
    let Some(&column_index) = column_lookup.get(&normalized) else {
        return Err(ParseError::UnknownColumn(column_name.to_string()));
    };

    let entry = not_nulls
        .entry(normalized)
        .or_insert_with(|| PendingNotNullConstraint {
            explicit_name: None,
            generated_base: format!("{relation_name}_{column_name}_not_null"),
            column: column_name.to_string(),
            not_valid: attributes.not_valid,
            no_inherit: attributes.no_inherit,
            primary_key_owned,
            column_index,
        });

    if let (Some(existing), Some(incoming)) =
        (entry.explicit_name.as_deref(), attributes.name.as_deref())
        && !existing.eq_ignore_ascii_case(incoming)
    {
        return Err(ParseError::UnexpectedToken {
            expected: "matching NOT NULL constraint names",
            actual: format!("conflicting NOT NULL constraint names for column \"{column_name}\""),
        });
    }

    if entry.explicit_name.is_none() {
        entry.explicit_name = attributes.name.clone();
    }
    if entry.no_inherit != attributes.no_inherit {
        return Err(ParseError::InvalidTableDefinition(format!(
            "conflicting NO INHERIT declaration for not-null constraint on column \"{column_name}\""
        )));
    }
    entry.not_valid &= attributes.not_valid;
    entry.primary_key_owned &= primary_key_owned;
    Ok(())
}

fn resolve_constraint_columns(
    referenced: &[String],
    columns: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(referenced.len());
    for column in referenced {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            return Err(ParseError::UnexpectedToken {
                expected: "unique column names in table constraint",
                actual: format!("duplicate column in constraint: {column}"),
            });
        }
        let index = column_lookup
            .get(&normalized)
            .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
        resolved.push(columns[*index].name.clone());
    }
    Ok(resolved)
}

fn resolve_relation_constraint_columns(
    referenced: &[String],
    desc: &RelationDesc,
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(referenced.len());
    for column in referenced {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            return Err(ParseError::UnexpectedToken {
                expected: "unique column names in table constraint",
                actual: format!("duplicate column in constraint: {column}"),
            });
        }
        let index = column_lookup
            .get(&normalized)
            .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
        let desc_column = &desc.columns[*index];
        if desc_column.dropped {
            return Err(ParseError::UnknownColumn(column.clone()));
        }
        resolved.push(desc_column.name.clone());
    }
    Ok(resolved)
}

fn relation_column_lookup(desc: &RelationDesc) -> BTreeMap<String, usize> {
    desc.columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !column.dropped)
        .map(|(index, column)| (column.name.to_ascii_lowercase(), index))
        .collect()
}

fn relation_display_name(
    catalog: &dyn super::CatalogLookup,
    relation_oid: u32,
    fallback: &str,
) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| fallback.to_string())
}

fn column_attnums_for_names(
    desc: &RelationDesc,
    columns: &[String],
) -> Result<Vec<i16>, ParseError> {
    let lookup = relation_column_lookup(desc);
    columns
        .iter()
        .map(|column| {
            lookup
                .get(&column.to_ascii_lowercase())
                .copied()
                .map(|index| (index + 1) as i16)
                .ok_or_else(|| ParseError::UnknownColumn(column.clone()))
        })
        .collect()
}

fn constraint_attnums(
    row: &PgConstraintRow,
    _constraint_kind: &str,
) -> Result<Vec<i16>, ParseError> {
    row.conkey
        .clone()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "constraint columns",
            actual: format!("missing conkey for constraint {}", row.conname),
        })
}

fn column_index_for_attnum(desc: &RelationDesc, attnum: i16) -> Result<usize, ParseError> {
    let index =
        usize::try_from(attnum.saturating_sub(1)).map_err(|_| ParseError::UnexpectedToken {
            expected: "user column attnum",
            actual: format!("invalid attnum {attnum}"),
        })?;
    desc.columns
        .get(index)
        .filter(|column| !column.dropped)
        .map(|_| index)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "user column attnum",
            actual: format!("invalid attnum {attnum}"),
        })
}

fn attnums_to_column_indexes(
    desc: &RelationDesc,
    attnums: &[i16],
) -> Result<Vec<usize>, ParseError> {
    attnums
        .iter()
        .map(|&attnum| column_index_for_attnum(desc, attnum))
        .collect()
}

fn attnums_to_column_names(
    desc: &RelationDesc,
    attnums: &[i16],
) -> Result<Vec<String>, ParseError> {
    attnums
        .iter()
        .map(|&attnum| {
            let index = column_index_for_attnum(desc, attnum)?;
            Ok(desc.columns[index].name.clone())
        })
        .collect()
}

fn find_exact_index_for_attnums(
    catalog: &dyn super::CatalogLookup,
    relation_oid: u32,
    attnums: &[i16],
    unique_required: bool,
) -> Option<super::BoundIndexRelation> {
    catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| {
            (!unique_required || index.index_meta.indisunique)
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.am_oid == crate::include::catalog::BTREE_AM_OID
                && index.index_meta.indkey == attnums
                && !index
                    .index_meta
                    .indpred
                    .as_deref()
                    .is_some_and(|pred| !pred.is_empty())
                && !index
                    .index_meta
                    .indexprs
                    .as_deref()
                    .is_some_and(|exprs| !exprs.is_empty())
        })
}

fn foreign_key_match_keyword(match_type: ForeignKeyMatchType) -> &'static str {
    match match_type {
        ForeignKeyMatchType::Simple => "SIMPLE",
        ForeignKeyMatchType::Full => "FULL",
        ForeignKeyMatchType::Partial => "PARTIAL",
    }
}

fn foreign_key_action_from_code(code: char) -> Result<ForeignKeyAction, ParseError> {
    match code {
        'a' | ' ' => Ok(ForeignKeyAction::NoAction),
        'r' => Ok(ForeignKeyAction::Restrict),
        'c' => Ok(ForeignKeyAction::Cascade),
        'n' => Ok(ForeignKeyAction::SetNull),
        'd' => Ok(ForeignKeyAction::SetDefault),
        other => Err(ParseError::UnexpectedToken {
            expected: "foreign-key action code",
            actual: other.to_string(),
        }),
    }
}

fn foreign_key_match_from_code(code: char) -> Result<ForeignKeyMatchType, ParseError> {
    match code {
        's' | ' ' => Ok(ForeignKeyMatchType::Simple),
        'f' => Ok(ForeignKeyMatchType::Full),
        'p' => Ok(ForeignKeyMatchType::Partial),
        other => Err(ParseError::UnexpectedToken {
            expected: "foreign-key match code",
            actual: other.to_string(),
        }),
    }
}

fn existing_constraint_names(existing_constraints: &[PgConstraintRow]) -> BTreeSet<String> {
    existing_constraints
        .iter()
        .map(|row| row.conname.to_ascii_lowercase())
        .collect()
}

fn assign_constraint_name(
    explicit_name: Option<String>,
    generated_base: String,
    used_names: &mut BTreeSet<String>,
) -> Result<String, ParseError> {
    if let Some(name) = explicit_name {
        if !used_names.insert(name.to_ascii_lowercase()) {
            return Err(ParseError::UnexpectedToken {
                expected: "distinct constraint names",
                actual: format!("duplicate constraint name: {name}"),
            });
        }
        Ok(name)
    } else {
        Ok(choose_generated_constraint_name(
            &generated_base,
            used_names,
        ))
    }
}

fn reserve_explicit_constraint_names<'a>(
    used_names: &mut BTreeSet<String>,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), ParseError> {
    for name in names {
        let normalized = name.to_ascii_lowercase();
        if !used_names.insert(normalized) {
            return Err(ParseError::UnexpectedToken {
                expected: "distinct constraint names",
                actual: format!("duplicate constraint name: {name}"),
            });
        }
    }
    Ok(())
}

fn choose_generated_constraint_name(base: &str, used_names: &mut BTreeSet<String>) -> String {
    if used_names.insert(base.to_ascii_lowercase()) {
        return base.to_string();
    }

    for suffix in 1.. {
        let candidate = format!("{base}{suffix}");
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }

    unreachable!("constraint name suffix space exhausted")
}

fn reject_unsupported_check_expr(expr: &Expr) -> Result<(), ParseError> {
    match expr {
        Expr::Aggref(_) => Err(ParseError::FeatureNotSupported(
            "aggregate functions in CHECK constraints".into(),
        )),
        Expr::WindowFunc(_) => Err(ParseError::FeatureNotSupported(
            "window functions in CHECK constraints".into(),
        )),
        Expr::SubLink(_) | Expr::SubPlan(_) => Err(ParseError::FeatureNotSupported(
            "subqueries in CHECK constraints".into(),
        )),
        Expr::Var(var) if var.varlevelsup > 0 => Err(ParseError::FeatureNotSupported(
            "outer references in CHECK constraints".into(),
        )),
        Expr::Op(op) => {
            for arg in &op.args {
                reject_unsupported_check_expr(arg)?;
            }
            Ok(())
        }
        Expr::Bool(expr) => {
            for arg in &expr.args {
                reject_unsupported_check_expr(arg)?;
            }
            Ok(())
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                reject_unsupported_check_expr(arg)?;
            }
            for arm in &case_expr.args {
                reject_unsupported_check_expr(&arm.expr)?;
                reject_unsupported_check_expr(&arm.result)?;
            }
            reject_unsupported_check_expr(&case_expr.defresult)
        }
        Expr::CaseTest(_) => Ok(()),
        Expr::Func(func) => {
            for arg in &func.args {
                reject_unsupported_check_expr(arg)?;
            }
            Ok(())
        }
        Expr::ScalarArrayOp(expr) => {
            reject_unsupported_check_expr(&expr.left)?;
            reject_unsupported_check_expr(&expr.right)
        }
        Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            reject_unsupported_check_expr(inner)
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
            reject_unsupported_check_expr(expr)?;
            reject_unsupported_check_expr(pattern)?;
            if let Some(escape) = escape {
                reject_unsupported_check_expr(escape)?;
            }
            Ok(())
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            reject_unsupported_check_expr(left)?;
            reject_unsupported_check_expr(right)
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                reject_unsupported_check_expr(element)?;
            }
            Ok(())
        }
        Expr::Row { fields, .. } => {
            for (_, field) in fields {
                reject_unsupported_check_expr(field)?;
            }
            Ok(())
        }
        Expr::FieldSelect { expr, .. } => reject_unsupported_check_expr(expr),
        Expr::ArraySubscript { array, subscripts } => {
            reject_unsupported_check_expr(array)?;
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    reject_unsupported_check_expr(lower)?;
                }
                if let Some(upper) = &subscript.upper {
                    reject_unsupported_check_expr(upper)?;
                }
            }
            Ok(())
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                reject_unsupported_check_expr(child)?;
            }
            Ok(())
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => Ok(()),
    }
}
