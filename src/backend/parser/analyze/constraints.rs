use std::collections::{BTreeMap, BTreeSet};

use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlExpr, SqlType, SqlTypeKind, parse_expr};
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{PgConstraintRow, bootstrap_pg_cast_rows};
use crate::include::nodes::parsenodes::{
    ColumnConstraint, ConstraintAttributes, CreateTableStatement, ForeignKeyAction,
    ForeignKeyMatchType, TableConstraint, TablePersistence,
};
use crate::include::nodes::primnodes::Expr;

use super::ParseError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBackedConstraintAction {
    pub constraint_name: Option<String>,
    pub existing_index_name: Option<String>,
    pub columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub primary: bool,
    pub exclusion: bool,
    pub nulls_not_distinct: bool,
    pub without_overlaps: Option<String>,
    pub access_method: Option<String>,
    pub exclusion_operators: Vec<String>,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotNullConstraintAction {
    pub constraint_name: String,
    pub column: String,
    pub not_valid: bool,
    pub no_inherit: bool,
    pub primary_key_owned: bool,
    pub is_local: bool,
    pub inhcount: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintAction {
    pub constraint_name: String,
    pub expr_sql: String,
    pub not_valid: bool,
    pub no_inherit: bool,
    pub enforced: bool,
    pub parent_constraint_oid: Option<u32>,
    pub is_local: bool,
    pub inhcount: i16,
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
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub not_valid: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BoundRelationConstraints {
    pub not_nulls: Vec<BoundNotNullConstraint>,
    pub checks: Vec<BoundCheckConstraint>,
    pub foreign_keys: Vec<BoundForeignKeyConstraint>,
    pub temporal: Vec<BoundTemporalConstraint>,
    pub exclusions: Vec<BoundExclusionConstraint>,
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
    pub enforced: bool,
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
pub struct BoundTemporalConstraint {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub column_names: Vec<String>,
    pub column_indexes: Vec<usize>,
    pub period_column_index: usize,
    pub primary: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundExclusionConstraint {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub column_names: Vec<String>,
    pub column_indexes: Vec<usize>,
    pub operator_oids: Vec<u32>,
    pub operator_proc_oids: Vec<u32>,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundReferencedByForeignKey {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub referenced_relation_oid: u32,
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
    existing_index_name: Option<String>,
    generated_base: String,
    columns: Vec<String>,
    include_columns: Vec<String>,
    primary: bool,
    exclusion: bool,
    nulls_not_distinct: bool,
    without_overlaps: Option<String>,
    access_method: Option<String>,
    exclusion_operators: Vec<String>,
    deferrable: bool,
    initially_deferred: bool,
}

#[derive(Debug, Clone)]
struct PendingCheckConstraint {
    explicit_name: Option<String>,
    generated_base: String,
    expr_sql: String,
    not_valid: bool,
    no_inherit: bool,
    enforced: bool,
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
    deferrable: bool,
    initially_deferred: bool,
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
    let relation_columns = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
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
                        enforced: attributes.enforced.unwrap_or(true),
                    });
                }
                ColumnConstraint::PrimaryKey { attributes } => {
                    let (deferrable, initially_deferred) =
                        validate_key_attributes(attributes, "PRIMARY KEY")?;
                    index_constraints.push(PendingIndexConstraint {
                        explicit_name: attributes.name.clone(),
                        existing_index_name: None,
                        generated_base: format!("{}_pkey", stmt.table_name),
                        columns: vec![column.name.clone()],
                        include_columns: Vec::new(),
                        primary: true,
                        exclusion: false,
                        nulls_not_distinct: false,
                        without_overlaps: None,
                        access_method: None,
                        exclusion_operators: Vec::new(),
                        deferrable,
                        initially_deferred,
                    });
                }
                ColumnConstraint::Unique { attributes } => {
                    let (deferrable, initially_deferred) =
                        validate_key_attributes(attributes, "UNIQUE")?;
                    index_constraints.push(PendingIndexConstraint {
                        explicit_name: attributes.name.clone(),
                        existing_index_name: None,
                        generated_base: format!("{}_{}_key", stmt.table_name, column.name),
                        columns: vec![column.name.clone()],
                        include_columns: Vec::new(),
                        primary: false,
                        exclusion: false,
                        nulls_not_distinct: attributes.nulls_not_distinct,
                        without_overlaps: None,
                        access_method: None,
                        exclusion_operators: Vec::new(),
                        deferrable,
                        initially_deferred,
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
                    let (deferrable, initially_deferred) = foreign_key_deferrability(attributes);
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
                        deferrable,
                        initially_deferred,
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
                    enforced: attributes.enforced.unwrap_or(true),
                });
            }
            TableConstraint::PrimaryKey {
                attributes,
                columns: key_columns,
                include_columns,
                without_overlaps,
            } => {
                let (deferrable, initially_deferred) =
                    validate_key_attributes(attributes, "PRIMARY KEY")?;
                let resolved = resolve_index_constraint_columns(
                    key_columns,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                )?;
                validate_without_overlaps_column(
                    &resolved,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                    catalog,
                )?;
                let resolved_include =
                    resolve_constraint_columns(include_columns, &columns, &column_lookup)?;
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    existing_index_name: None,
                    generated_base: format!("{}_pkey", stmt.table_name),
                    columns: resolved,
                    include_columns: resolved_include,
                    primary: true,
                    exclusion: false,
                    nulls_not_distinct: false,
                    without_overlaps: without_overlaps.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    deferrable,
                    initially_deferred,
                });
            }
            TableConstraint::Unique {
                attributes,
                columns: key_columns,
                include_columns,
                without_overlaps,
            } => {
                let (deferrable, initially_deferred) =
                    validate_key_attributes(attributes, "UNIQUE")?;
                let resolved = resolve_index_constraint_columns(
                    key_columns,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                )?;
                validate_without_overlaps_column(
                    &resolved,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                    catalog,
                )?;
                let resolved_include =
                    resolve_constraint_columns(include_columns, &columns, &column_lookup)?;
                let generated_columns = resolved
                    .iter()
                    .chain(resolved_include.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    existing_index_name: None,
                    generated_base: format!(
                        "{}_{}_key",
                        stmt.table_name,
                        generated_columns.join("_")
                    ),
                    columns: resolved,
                    include_columns: resolved_include,
                    primary: false,
                    exclusion: false,
                    nulls_not_distinct: attributes.nulls_not_distinct,
                    without_overlaps: without_overlaps.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    deferrable,
                    initially_deferred,
                });
            }
            TableConstraint::Exclusion {
                attributes,
                using_method,
                elements,
                include_columns,
            } => {
                let (deferrable, initially_deferred) =
                    validate_key_attributes(attributes, "EXCLUDE")?;
                let key_columns = elements
                    .iter()
                    .map(|element| element.column.clone())
                    .collect::<Vec<_>>();
                let resolved = resolve_constraint_columns(&key_columns, &columns, &column_lookup)?;
                let resolved_include =
                    resolve_constraint_columns(include_columns, &columns, &column_lookup)?;
                let generated_columns = resolved
                    .iter()
                    .chain(resolved_include.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    existing_index_name: None,
                    generated_base: format!(
                        "{}_{}_excl",
                        stmt.table_name,
                        generated_columns.join("_")
                    ),
                    columns: resolved,
                    include_columns: resolved_include,
                    primary: false,
                    nulls_not_distinct: false,
                    without_overlaps: None,
                    exclusion: true,
                    access_method: Some(using_method.clone()),
                    exclusion_operators: elements
                        .iter()
                        .map(|element| element.operator.clone())
                        .collect(),
                    deferrable,
                    initially_deferred,
                });
            }
            TableConstraint::PrimaryKeyUsingIndex { .. }
            | TableConstraint::UniqueUsingIndex { .. } => {
                return Err(ParseError::UnexpectedToken {
                    expected: "CREATE TABLE constraint",
                    actual: "USING INDEX constraint".into(),
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
                let (deferrable, initially_deferred) = foreign_key_deferrability(attributes);
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
                    deferrable,
                    initially_deferred,
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
            is_local: true,
            inhcount: 0,
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
            enforced: constraint.enforced,
            parent_constraint_oid: None,
            is_local: true,
            inhcount: 0,
        })
        .collect();

    let finalized_index_backed: Vec<IndexBackedConstraintAction> = index_constraints
        .into_iter()
        .map(|constraint| IndexBackedConstraintAction {
            constraint_name: Some(constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            })),
            existing_index_name: constraint.existing_index_name,
            columns: constraint.columns,
            include_columns: constraint.include_columns,
            primary: constraint.primary,
            exclusion: constraint.exclusion,
            nulls_not_distinct: constraint.nulls_not_distinct,
            without_overlaps: constraint.without_overlaps,
            access_method: constraint.access_method,
            exclusion_operators: constraint.exclusion_operators,
            deferrable: constraint.deferrable,
            initially_deferred: constraint.initially_deferred,
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
                constraint_name,
            ) = if constraint
                .referenced_table
                .eq_ignore_ascii_case(&stmt.table_name)
            {
                let constraint_name = constraint.explicit_name.clone().unwrap_or_else(|| {
                    choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
                });
                let referenced_columns = resolve_pending_self_referenced_key(
                    &stmt.table_name,
                    &columns,
                    &column_lookup,
                    &finalized_index_backed,
                    constraint.referenced_columns.as_deref(),
                    &constraint_name,
                    &local_columns,
                    &child_types,
                    catalog,
                )?;
                (
                    stmt.table_name.clone(),
                    0,
                    0,
                    true,
                    referenced_columns,
                    constraint_name,
                )
            } else {
                let constraint_name = constraint.explicit_name.clone().unwrap_or_else(|| {
                    choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
                });
                let referenced = resolve_referenced_key(
                    &stmt.table_name,
                    None,
                    table_persistence_code(stmt.persistence),
                    &constraint.referenced_table,
                    constraint.referenced_columns.as_deref(),
                    &constraint_name,
                    &local_columns,
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
                    constraint_name,
                )
            };
            Ok(ForeignKeyConstraintAction {
                constraint_name,
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
                    &relation_columns,
                    &constraint.columns,
                )?,
                on_update: constraint.on_update,
                deferrable: constraint.deferrable,
                initially_deferred: constraint.initially_deferred,
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
    let mut not_nulls = Vec::new();
    let mut checks = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut temporal = Vec::new();
    let mut exclusions = Vec::new();

    for row in rows {
        match row.contype {
            crate::include::catalog::CONSTRAINT_NOTNULL => {
                let Some(attnum) = row
                    .conkey
                    .as_ref()
                    .and_then(|conkey| conkey.first())
                    .copied()
                else {
                    continue;
                };
                not_nulls.push(BoundNotNullConstraint {
                    column_index: attnum.saturating_sub(1) as usize,
                    constraint_name: row.conname,
                });
            }
            crate::include::catalog::CONSTRAINT_CHECK => {
                let expr_sql = row.conbin.ok_or(ParseError::UnexpectedToken {
                    expected: "stored CHECK constraint expression",
                    actual: format!("missing expression for constraint {}", row.conname),
                })?;
                checks.push(BoundCheckConstraint {
                    constraint_name: row.conname,
                    expr: bind_check_constraint_expr(&expr_sql, relation_name, desc, catalog)?,
                    enforced: row.conenforced,
                });
            }
            crate::include::catalog::CONSTRAINT_FOREIGN => {
                foreign_keys.push(bind_outbound_foreign_key_constraint(
                    relation_oid,
                    desc,
                    row,
                    catalog,
                )?);
            }
            crate::include::catalog::CONSTRAINT_PRIMARY
            | crate::include::catalog::CONSTRAINT_UNIQUE
            | crate::include::catalog::CONSTRAINT_EXCLUSION
                if row.conperiod =>
            {
                temporal.push(bind_temporal_constraint(row, desc)?);
            }
            crate::include::catalog::CONSTRAINT_EXCLUSION => {
                exclusions.push(bind_exclusion_constraint(row, desc, catalog)?);
            }
            _ => {}
        }
    }

    Ok(BoundRelationConstraints {
        not_nulls,
        checks,
        foreign_keys,
        temporal,
        exclusions,
    })
}

pub(crate) fn bind_temporal_constraint(
    row: PgConstraintRow,
    desc: &RelationDesc,
) -> Result<BoundTemporalConstraint, ParseError> {
    let conkey = row
        .conkey
        .clone()
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "temporal constraint columns",
            actual: format!("missing conkey for constraint {}", row.conname),
        })?;
    let mut column_names = Vec::with_capacity(conkey.len());
    let mut column_indexes = Vec::with_capacity(conkey.len());
    for attnum in &conkey {
        let index = usize::try_from(*attnum)
            .ok()
            .and_then(|attnum| attnum.checked_sub(1))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "positive temporal constraint attnum",
                actual: format!("invalid attnum {attnum}"),
            })?;
        let column = desc
            .columns
            .get(index)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "temporal constraint column",
                actual: format!("attnum {attnum} out of range"),
            })?;
        if column.dropped {
            return Err(ParseError::UnexpectedToken {
                expected: "live temporal constraint column",
                actual: format!("constraint {} references dropped column", row.conname),
            });
        }
        column_names.push(column.name.clone());
        column_indexes.push(index);
    }
    let period_column_index =
        *column_indexes
            .last()
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "temporal constraint period column",
                actual: format!("constraint {} has no key columns", row.conname),
            })?;
    Ok(BoundTemporalConstraint {
        constraint_oid: row.oid,
        constraint_name: row.conname,
        column_names,
        column_indexes,
        period_column_index,
        primary: row.contype == crate::include::catalog::CONSTRAINT_PRIMARY,
        enforced: row.conenforced,
    })
}

pub(crate) fn bind_exclusion_constraint(
    row: PgConstraintRow,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundExclusionConstraint, ParseError> {
    let operator_oids = row
        .conexclop
        .clone()
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "exclusion constraint operators",
            actual: format!("missing conexclop for constraint {}", row.conname),
        })?;
    let conkey = row
        .conkey
        .clone()
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "exclusion constraint columns",
            actual: format!("missing conkey for constraint {}", row.conname),
        })?;
    let mut column_names = Vec::with_capacity(operator_oids.len());
    let mut column_indexes = Vec::with_capacity(operator_oids.len());
    for attnum in conkey.iter().take(operator_oids.len()) {
        let index = usize::try_from(*attnum)
            .ok()
            .and_then(|attnum| attnum.checked_sub(1))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "positive exclusion constraint attnum",
                actual: format!("invalid attnum {attnum}"),
            })?;
        let column = desc
            .columns
            .get(index)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "exclusion constraint column",
                actual: format!("attnum {attnum} out of range"),
            })?;
        if column.dropped {
            return Err(ParseError::UnexpectedToken {
                expected: "live exclusion constraint column",
                actual: format!("constraint {} references dropped column", row.conname),
            });
        }
        column_names.push(column.name.clone());
        column_indexes.push(index);
    }
    if column_indexes.len() != operator_oids.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "one exclusion operator per key column",
            actual: format!(
                "constraint {} has {} columns and {} operators",
                row.conname,
                column_indexes.len(),
                operator_oids.len()
            ),
        });
    }
    let operator_proc_oids = operator_oids
        .iter()
        .map(|operator_oid| {
            catalog
                .operator_by_oid(*operator_oid)
                .map(|operator| operator.oprcode)
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "exclusion constraint operator",
                    actual: format!("unknown operator oid {operator_oid}"),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BoundExclusionConstraint {
        constraint_oid: row.oid,
        constraint_name: row.conname,
        column_names,
        column_indexes,
        operator_oids,
        operator_proc_oids,
        enforced: row.conenforced,
    })
}

pub fn bind_referenced_by_foreign_keys(
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Vec<BoundReferencedByForeignKey>, ParseError> {
    catalog
        .foreign_key_constraint_rows_referencing_relation(relation_oid)
        .into_iter()
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
    let relation_columns = desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
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
                        is_local: true,
                        inhcount: 0,
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
                    enforced: attributes.enforced.unwrap_or(true),
                    parent_constraint_oid: None,
                    is_local: true,
                    inhcount: 0,
                },
            ))
        }
        TableConstraint::PrimaryKey {
            attributes,
            columns,
            include_columns,
            without_overlaps,
        } => {
            let (deferrable, initially_deferred) =
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
            let resolved = resolve_relation_index_constraint_columns(
                columns,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            validate_without_overlaps_relation_column(
                &resolved,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            let resolved_include =
                resolve_relation_constraint_columns(include_columns, desc, &column_lookup)?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: None,
                    columns: resolved,
                    include_columns: resolved_include,
                    primary: true,
                    exclusion: false,
                    nulls_not_distinct: false,
                    without_overlaps: without_overlaps.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::Unique {
            attributes,
            columns,
            include_columns,
            without_overlaps,
        } => {
            let (deferrable, initially_deferred) = validate_key_attributes(attributes, "UNIQUE")?;
            let resolved = resolve_relation_index_constraint_columns(
                columns,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            validate_without_overlaps_relation_column(
                &resolved,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            let resolved_include =
                resolve_relation_constraint_columns(include_columns, desc, &column_lookup)?;
            let generated_columns = resolved
                .iter()
                .chain(resolved_include.iter())
                .cloned()
                .collect::<Vec<_>>();
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                format!("{table_name}_{}_key", generated_columns.join("_")),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: None,
                    columns: resolved,
                    include_columns: resolved_include,
                    primary: false,
                    exclusion: false,
                    nulls_not_distinct: attributes.nulls_not_distinct,
                    without_overlaps: without_overlaps.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::Exclusion {
            attributes,
            using_method,
            elements,
            include_columns,
        } => {
            let (deferrable, initially_deferred) = validate_key_attributes(attributes, "EXCLUDE")?;
            let key_columns = elements
                .iter()
                .map(|element| element.column.clone())
                .collect::<Vec<_>>();
            let resolved = resolve_relation_constraint_columns(&key_columns, desc, &column_lookup)?;
            let resolved_include =
                resolve_relation_constraint_columns(include_columns, desc, &column_lookup)?;
            let generated_columns = resolved
                .iter()
                .chain(resolved_include.iter())
                .cloned()
                .collect::<Vec<_>>();
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                format!("{table_name}_{}_excl", generated_columns.join("_")),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: None,
                    columns: resolved,
                    include_columns: resolved_include,
                    primary: false,
                    nulls_not_distinct: false,
                    without_overlaps: None,
                    exclusion: true,
                    access_method: Some(using_method.clone()),
                    exclusion_operators: elements
                        .iter()
                        .map(|element| element.operator.clone())
                        .collect(),
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::PrimaryKeyUsingIndex {
            attributes,
            index_name,
        } => {
            let (deferrable, initially_deferred) =
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
            let (columns, include_columns, nulls_not_distinct) = index_columns_for_existing_index(
                table_name,
                relation_oid,
                desc,
                index_name,
                catalog,
            )?;
            let constraint_name = assign_constraint_name(
                Some(
                    attributes
                        .name
                        .clone()
                        .unwrap_or_else(|| index_name.clone()),
                ),
                index_name.clone(),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: Some(index_name.clone()),
                    columns,
                    include_columns,
                    primary: true,
                    exclusion: false,
                    nulls_not_distinct,
                    without_overlaps: None,
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::UniqueUsingIndex {
            attributes,
            index_name,
        } => {
            let (deferrable, initially_deferred) = validate_key_attributes(attributes, "UNIQUE")?;
            let (columns, include_columns, nulls_not_distinct) = index_columns_for_existing_index(
                table_name,
                relation_oid,
                desc,
                index_name,
                catalog,
            )?;
            let constraint_name = assign_constraint_name(
                Some(
                    attributes
                        .name
                        .clone()
                        .unwrap_or_else(|| index_name.clone()),
                ),
                index_name.clone(),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: Some(index_name.clone()),
                    columns,
                    include_columns,
                    primary: false,
                    exclusion: false,
                    nulls_not_distinct,
                    without_overlaps: None,
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    deferrable,
                    initially_deferred,
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
            let (deferrable, initially_deferred) = foreign_key_deferrability(attributes);
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
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                format!("{table_name}_{}_fkey", resolved.join("_")),
                &mut used_names,
            )?;
            let referenced = resolve_referenced_key(
                table_name,
                Some(relation_oid),
                relpersistence,
                referenced_table,
                referenced_columns.as_deref(),
                &constraint_name,
                &resolved,
                &child_types,
                catalog,
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
                        &relation_columns,
                        columns,
                    )?,
                    on_update: *on_update,
                    deferrable,
                    initially_deferred,
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
                    enforced: attributes.enforced.unwrap_or(true),
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
            is_local: true,
            inhcount: 0,
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
            enforced: constraint.enforced,
            parent_constraint_oid: None,
            is_local: true,
            inhcount: 0,
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

pub fn bind_index_predicate_sql_expr(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    bind_index_predicate_expr(&parsed, relation_name, desc, catalog)
}

pub fn bind_check_constraint_sql_expr(
    expr: &SqlExpr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    bind_boolean_relation_predicate(
        expr,
        relation_name,
        desc,
        catalog,
        "boolean CHECK constraint expression",
        "CHECK constraint expression must return boolean",
    )
}

pub fn bind_index_predicate_expr(
    expr: &SqlExpr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let scope = super::scope_for_base_relation_with_optional_name(relation_name, desc);
    let inferred = super::infer_sql_expr_type_with_ctes(expr, &scope, catalog, &[], None, &[]);
    if inferred != SqlType::new(SqlTypeKind::Bool) {
        return Err(ParseError::UnexpectedToken {
            expected: "boolean index predicate expression",
            actual: "index predicate expression must return boolean".into(),
        });
    }

    let bound = super::bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &[])?;
    reject_unsupported_check_expr(&bound)?;
    Ok(bound)
}

fn bind_boolean_relation_predicate(
    expr: &SqlExpr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
    expected: &'static str,
    actual: &'static str,
) -> Result<Expr, ParseError> {
    let scope = super::scope_for_relation(relation_name, desc);
    let inferred = super::infer_sql_expr_type_with_ctes(expr, &scope, catalog, &[], None, &[]);
    if inferred != SqlType::new(SqlTypeKind::Bool) {
        return Err(ParseError::UnexpectedToken {
            expected,
            actual: actual.into(),
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
    Ok(())
}

fn validate_key_attributes(
    attributes: &ConstraintAttributes,
    constraint_kind: &'static str,
) -> Result<(bool, bool), ParseError> {
    if attributes.not_valid {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} NOT VALID"
        )));
    }
    if attributes.no_inherit {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} NO INHERIT"
        )));
    }
    if attributes.enforced.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} ENFORCED/NOT ENFORCED"
        )));
    }
    let mut deferrable = attributes.deferrable.unwrap_or(false);
    let initially_deferred = attributes.initially_deferred.unwrap_or(false);
    if initially_deferred {
        deferrable = true;
    }
    Ok((deferrable, initially_deferred))
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

fn foreign_key_deferrability(attributes: &ConstraintAttributes) -> (bool, bool) {
    let mut deferrable = attributes.deferrable.unwrap_or(false);
    let initially_deferred = attributes.initially_deferred.unwrap_or(false);
    if initially_deferred {
        deferrable = true;
    }
    (deferrable, initially_deferred)
}

fn resolve_foreign_key_delete_set_columns(
    delete_set_columns: Option<&[String]>,
    relation_columns: &[String],
    foreign_key_columns: &[String],
) -> Result<Option<Vec<String>>, ParseError> {
    let Some(delete_set_columns) = delete_set_columns else {
        return Ok(None);
    };
    let mut resolved = Vec::new();
    let mut seen = BTreeSet::new();
    for column_name in delete_set_columns {
        let Some(relation_column) = relation_columns
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(column_name))
        else {
            return Err(ParseError::DetailedError {
                message: format!(
                    "column \"{column_name}\" referenced in foreign key constraint does not exist"
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            });
        };
        let Some(foreign_key_column) = foreign_key_columns
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(column_name))
        else {
            return Err(ParseError::DetailedError {
                message: format!(
                    "column \"{column_name}\" referenced in ON DELETE SET action must be part of foreign key"
                ),
                detail: None,
                hint: None,
                sqlstate: "42P10",
            });
        };
        let normalized = foreign_key_column.to_ascii_lowercase();
        if seen.insert(normalized) {
            resolved.push(relation_column.clone());
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
    constraint_name: &str,
    child_columns: &[String],
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
    if !foreign_key_types_compatible(child_types, &parent_types) {
        return Err(foreign_key_type_mismatch_error(
            constraint_name,
            child_columns,
            &referenced_columns,
            child_types,
            &parent_types,
            catalog,
        ));
    }

    Ok(referenced_columns)
}

fn foreign_key_types_compatible(child_types: &[SqlType], parent_types: &[SqlType]) -> bool {
    child_types
        .iter()
        .zip(parent_types)
        .all(|(&child, &parent)| foreign_key_type_compatible(child, parent))
}

fn foreign_key_type_mismatch_error(
    constraint_name: &str,
    child_columns: &[String],
    parent_columns: &[String],
    child_types: &[SqlType],
    parent_types: &[SqlType],
    catalog: &dyn super::CatalogLookup,
) -> ParseError {
    let child_names = quoted_column_list(child_columns);
    let parent_names = quoted_column_list(parent_columns);
    let child_type_names = child_types
        .iter()
        .copied()
        .map(|ty| foreign_key_type_name(ty, catalog))
        .collect::<Vec<_>>()
        .join(", ");
    let parent_type_names = parent_types
        .iter()
        .copied()
        .map(|ty| foreign_key_type_name(ty, catalog))
        .collect::<Vec<_>>()
        .join(", ");
    ParseError::DetailedError {
        message: format!("foreign key constraint \"{constraint_name}\" cannot be implemented"),
        detail: Some(format!(
            "Key columns {child_names} of the referencing table and {parent_names} of the referenced table are of incompatible types: {child_type_names} and {parent_type_names}."
        )),
        hint: None,
        sqlstate: "42804",
    }
}

fn quoted_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

fn foreign_key_type_name(ty: SqlType, catalog: &dyn super::CatalogLookup) -> String {
    if !ty.is_array
        && ty.type_oid != 0
        && let Some(row) = catalog.type_by_oid(ty.type_oid)
    {
        return row.typname;
    }
    super::coerce::sql_type_name(ty)
}

fn foreign_key_type_compatible(child: SqlType, parent: SqlType) -> bool {
    if child == parent {
        return true;
    }
    if child.is_array || parent.is_array {
        return false;
    }
    if foreign_key_integer_type(child) && foreign_key_integer_type(parent) {
        return true;
    }
    if foreign_key_text_like_type(child) && foreign_key_text_like_type(parent) {
        return true;
    }
    if foreign_key_integer_type(child) && foreign_key_float_type(parent) {
        return true;
    }

    let child_oid = sql_type_oid(child);
    let parent_oid = sql_type_oid(parent);
    child_oid != 0
        && parent_oid != 0
        && bootstrap_pg_cast_rows().into_iter().any(|row| {
            row.castsource == child_oid && row.casttarget == parent_oid && row.castcontext == 'i'
        })
}

fn foreign_key_integer_type(ty: SqlType) -> bool {
    matches!(
        ty.kind,
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
    )
}

fn foreign_key_float_type(ty: SqlType) -> bool {
    matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8)
}

fn foreign_key_text_like_type(ty: SqlType) -> bool {
    matches!(
        ty.kind,
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
    )
}

fn resolve_referenced_key(
    child_relation_name: &str,
    child_relation_oid: Option<u32>,
    child_persistence: char,
    referenced_table: &str,
    referenced_columns: Option<&[String]>,
    constraint_name: &str,
    child_columns: &[String],
    child_types: &[SqlType],
    catalog: &dyn super::CatalogLookup,
) -> Result<ResolvedReferencedKey, ParseError> {
    let relation = catalog
        .lookup_any_relation(referenced_table)
        .ok_or_else(|| ParseError::UnknownTable(referenced_table.to_string()))?;
    if !matches!(relation.relkind, 'r' | 'p') {
        return Err(ParseError::UnknownTable(referenced_table.to_string()));
    }
    if relation.relkind == 'p' {
        return Err(ParseError::FeatureNotSupported(
            "REFERENCES to partitioned tables".into(),
        ));
    }
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
    if !foreign_key_types_compatible(child_types, &parent_types) {
        return Err(foreign_key_type_mismatch_error(
            constraint_name,
            child_columns,
            &columns,
            child_types,
            &parent_types,
            catalog,
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
    relation_oid: u32,
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
        referenced_relation_oid: relation_oid,
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

fn resolve_index_constraint_columns(
    referenced: &[String],
    without_overlaps: Option<&str>,
    columns: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    resolve_constraint_columns(referenced, columns, column_lookup).map_err(|err| {
        match (&err, without_overlaps) {
            (ParseError::UnknownColumn(column), Some(period_column))
                if column.eq_ignore_ascii_case(period_column) =>
            {
                ParseError::MissingKeyColumn(column.clone())
            }
            _ => err,
        }
    })
}

fn validate_without_overlaps_column(
    columns: &[String],
    without_overlaps: Option<&str>,
    column_defs: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
    catalog: &dyn super::CatalogLookup,
) -> Result<(), ParseError> {
    let Some(period_column) = without_overlaps else {
        return Ok(());
    };
    validate_without_overlaps_shape(columns, period_column)?;
    let index = *column_lookup
        .get(&period_column.to_ascii_lowercase())
        .ok_or_else(|| ParseError::MissingKeyColumn(period_column.to_string()))?;
    let sql_type = super::resolve_raw_type_name(&column_defs[index].ty, catalog)?;
    if !sql_type.is_range() && !sql_type.is_multirange() {
        return Err(ParseError::DetailedError {
            message: format!(
                "column \"{}\" in WITHOUT OVERLAPS is not a range or multirange type",
                column_defs[index].name
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(())
}

fn validate_without_overlaps_relation_column(
    columns: &[String],
    without_overlaps: Option<&str>,
    desc: &RelationDesc,
    column_lookup: &BTreeMap<String, usize>,
) -> Result<(), ParseError> {
    let Some(period_column) = without_overlaps else {
        return Ok(());
    };
    validate_without_overlaps_shape(columns, period_column)?;
    let index = *column_lookup
        .get(&period_column.to_ascii_lowercase())
        .ok_or_else(|| ParseError::MissingKeyColumn(period_column.to_string()))?;
    let column = &desc.columns[index];
    if column.dropped {
        return Err(ParseError::MissingKeyColumn(period_column.to_string()));
    }
    if !column.sql_type.is_range() && !column.sql_type.is_multirange() {
        return Err(ParseError::DetailedError {
            message: format!(
                "column \"{}\" in WITHOUT OVERLAPS is not a range or multirange type",
                column.name
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(())
}

fn validate_without_overlaps_shape(
    columns: &[String],
    period_column: &str,
) -> Result<(), ParseError> {
    if columns.len() < 2 {
        return Err(ParseError::DetailedError {
            message: "constraint using WITHOUT OVERLAPS needs at least two columns".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    let Some(last) = columns.last() else {
        return Err(ParseError::UnexpectedEof);
    };
    if !last.eq_ignore_ascii_case(period_column) {
        return Err(ParseError::DetailedError {
            message: "WITHOUT OVERLAPS column must be the last column in the constraint".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    Ok(())
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

fn index_columns_for_existing_index(
    table_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    index_name: &str,
    catalog: &dyn super::CatalogLookup,
) -> Result<(Vec<String>, Vec<String>, bool), ParseError> {
    let index = catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "existing index on table",
            actual: format!("index \"{index_name}\" does not exist for table \"{table_name}\""),
        })?;
    if !index.index_meta.indisunique {
        return Err(ParseError::UnexpectedToken {
            expected: "unique index",
            actual: format!("index \"{index_name}\" is not unique"),
        });
    }
    let mut all_columns = Vec::with_capacity(index.index_meta.indkey.len());
    for attnum in &index.index_meta.indkey {
        if *attnum <= 0 {
            return Err(ParseError::UnexpectedToken {
                expected: "simple column index",
                actual: format!("index \"{index_name}\" contains expressions"),
            });
        }
        let column = desc
            .columns
            .get((*attnum as usize).saturating_sub(1))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "index column",
                actual: format!("index \"{index_name}\" has invalid column reference"),
            })?;
        all_columns.push(column.name.clone());
    }
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0)).unwrap_or_default();
    let include_columns = all_columns.split_off(key_count.min(all_columns.len()));
    Ok((
        all_columns,
        include_columns,
        index.index_meta.indnullsnotdistinct,
    ))
}

fn resolve_relation_index_constraint_columns(
    referenced: &[String],
    without_overlaps: Option<&str>,
    desc: &RelationDesc,
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    resolve_relation_constraint_columns(referenced, desc, column_lookup).map_err(|err| {
        match (&err, without_overlaps) {
            (ParseError::UnknownColumn(column), Some(period_column))
                if column.eq_ignore_ascii_case(period_column) =>
            {
                ParseError::MissingKeyColumn(column.clone())
            }
            _ => err,
        }
    })
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
        Expr::SetReturning(_) => Err(ParseError::FeatureNotSupported(
            "set-returning functions in CHECK constraints".into(),
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
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => reject_unsupported_check_expr(inner),
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
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => Ok(()),
    }
}
