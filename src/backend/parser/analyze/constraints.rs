use std::collections::{BTreeMap, BTreeSet};

use crate::include::nodes::parsenodes::{
    ColumnConstraint, ConstraintAttributes, CreateTableStatement, TableConstraint,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedCreateTableConstraints {
    pub not_nulls: Vec<NotNullConstraintAction>,
    pub checks: Vec<CheckConstraintAction>,
    pub index_backed: Vec<IndexBackedConstraintAction>,
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

    if index_constraints.iter().filter(|constraint| constraint.primary).count() > 1 {
        return Err(ParseError::UnexpectedToken {
            expected: "at most one PRIMARY KEY",
            actual: "multiple PRIMARY KEY constraints".into(),
        });
    }

    for primary in index_constraints.iter().filter(|constraint| constraint.primary) {
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

    if let (Some(existing), Some(incoming)) = (
        entry.explicit_name.as_deref(),
        attributes.name.as_deref(),
    ) && !existing.eq_ignore_ascii_case(incoming)
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
