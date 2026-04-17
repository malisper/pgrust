use std::collections::{BTreeMap, BTreeSet};

use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlExpr, SqlType, SqlTypeKind, parse_expr};
use crate::include::catalog::PgConstraintRow;
use crate::include::nodes::parsenodes::{
    ColumnConstraint, ConstraintAttributes, CreateTableStatement, TableConstraint,
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
    pub primary_key_owned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintAction {
    pub constraint_name: String,
    pub expr_sql: String,
    pub not_valid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BoundRelationConstraints {
    pub not_nulls: Vec<BoundNotNullConstraint>,
    pub checks: Vec<BoundCheckConstraint>,
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
pub struct NormalizedCreateTableConstraints {
    pub not_nulls: Vec<NotNullConstraintAction>,
    pub checks: Vec<CheckConstraintAction>,
    pub index_backed: Vec<IndexBackedConstraintAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedAlterTableConstraint {
    NotNull(NotNullConstraintAction),
    Check(CheckConstraintAction),
    IndexBacked(IndexBackedConstraintAction),
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
}

#[derive(Debug, Clone)]
struct PendingNotNullConstraint {
    explicit_name: Option<String>,
    generated_base: String,
    column: String,
    not_valid: bool,
    primary_key_owned: bool,
    column_index: usize,
}

pub fn normalize_create_table_constraints(
    stmt: &CreateTableStatement,
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
                    validate_not_null_or_check_attributes(attributes, "CHECK")?;
                    check_constraints.push(PendingCheckConstraint {
                        explicit_name: attributes.name.clone(),
                        generated_base: format!("{}_{}_check", stmt.table_name, column.name),
                        expr_sql: expr_sql.clone(),
                        not_valid: attributes.not_valid,
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
                validate_not_null_or_check_attributes(attributes, "CHECK")?;
                check_constraints.push(PendingCheckConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: format!("{}_check", stmt.table_name),
                    expr_sql: expr_sql.clone(),
                    not_valid: attributes.not_valid,
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
        })
        .collect();

    let finalized_index_backed = index_constraints
        .into_iter()
        .map(|constraint| IndexBackedConstraintAction {
            constraint_name: Some(constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            })),
            columns: constraint.columns,
            primary: constraint.primary,
        })
        .collect();

    Ok(NormalizedCreateTableConstraints {
        not_nulls: finalized_not_nulls,
        checks: finalized_checks,
        index_backed: finalized_index_backed,
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
        .into_iter()
        .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_CHECK)
        .map(|row| {
            let expr_sql = row.conbin.ok_or(ParseError::UnexpectedToken {
                expected: "stored CHECK constraint expression",
                actual: format!("missing expression for constraint {}", row.conname),
            })?;
            Ok(BoundCheckConstraint {
                constraint_name: row.conname,
                expr: bind_check_constraint_expr(&expr_sql, relation_name, desc, catalog)?,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(BoundRelationConstraints { not_nulls, checks })
}

pub fn normalize_alter_table_add_constraint(
    table_name: &str,
    desc: &RelationDesc,
    existing_constraints: &[PgConstraintRow],
    constraint: &TableConstraint,
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
                        primary_key_owned: false,
                    },
                ))
            } else {
                Err(ParseError::UnexpectedToken {
                    expected: "nullable column for NOT NULL constraint",
                    actual: format!("column \"{}\" is already marked NOT NULL", column),
                })
            }
        }
        TableConstraint::Check {
            attributes,
            expr_sql,
        } => {
            validate_not_null_or_check_attributes(attributes, "CHECK")?;
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
    }
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
        Expr::Row { fields } => {
            for (_, field) in fields {
                reject_unsupported_check_expr(field)?;
            }
            Ok(())
        }
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
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => Ok(()),
    }
}
