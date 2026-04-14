use std::collections::{BTreeMap, BTreeSet};

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlTypeKind;

use super::{CreateTableStatement, ParseError, TableConstraint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBackedConstraintAction {
    pub constraint_name: Option<String>,
    pub columns: Vec<String>,
    pub primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredCreateTable {
    pub relation_desc: RelationDesc,
    pub constraint_actions: Vec<IndexBackedConstraintAction>,
}

pub fn create_relation_desc(stmt: &CreateTableStatement) -> Result<RelationDesc, ParseError> {
    Ok(lower_create_table(stmt)?.relation_desc)
}

pub fn lower_create_table(stmt: &CreateTableStatement) -> Result<LoweredCreateTable, ParseError> {
    let columns = stmt.columns().cloned().collect::<Vec<_>>();
    let column_lookup = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.to_ascii_lowercase(), index))
        .collect::<BTreeMap<_, _>>();

    let mut constraint_actions = Vec::new();
    let mut primary_declared = false;

    for column in &columns {
        if column.primary_key {
            if primary_declared {
                return Err(ParseError::UnexpectedToken {
                    expected: "at most one PRIMARY KEY",
                    actual: "multiple PRIMARY KEY constraints".into(),
                });
            }
            primary_declared = true;
            constraint_actions.push(IndexBackedConstraintAction {
                constraint_name: None,
                columns: vec![column.name.clone()],
                primary: true,
            });
        }
        if column.unique {
            constraint_actions.push(IndexBackedConstraintAction {
                constraint_name: None,
                columns: vec![column.name.clone()],
                primary: false,
            });
        }
    }

    for constraint in stmt.constraints() {
        let columns = validate_constraint_columns(constraint, &columns, &column_lookup)?;
        match constraint {
            TableConstraint::PrimaryKey { .. } => {
                if primary_declared {
                    return Err(ParseError::UnexpectedToken {
                        expected: "at most one PRIMARY KEY",
                        actual: "multiple PRIMARY KEY constraints".into(),
                    });
                }
                primary_declared = true;
                constraint_actions.push(IndexBackedConstraintAction {
                    constraint_name: None,
                    columns,
                    primary: true,
                });
            }
            TableConstraint::Unique { .. } => {
                constraint_actions.push(IndexBackedConstraintAction {
                    constraint_name: None,
                    columns,
                    primary: false,
                });
            }
        }
    }

    let mut seen_keys = BTreeSet::new();
    for action in &constraint_actions {
        let key = action
            .columns
            .iter()
            .map(|column| column.to_ascii_lowercase())
            .collect::<Vec<_>>();
        if !seen_keys.insert(key) {
            return Err(ParseError::UnexpectedToken {
                expected: "distinct PRIMARY KEY/UNIQUE definitions",
                actual: format!(
                    "duplicate key definition on ({})",
                    action.columns.join(", ")
                ),
            });
        }
    }

    let primary_columns = constraint_actions
        .iter()
        .filter(|action| action.primary)
        .flat_map(|action| {
            action
                .columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
        })
        .collect::<BTreeSet<_>>();

    let relation_desc = RelationDesc {
        columns: columns
            .iter()
            .map(|column| {
                let sql_type = match &column.ty {
                    crate::backend::parser::RawTypeName::Builtin(sql_type) => *sql_type,
                    crate::backend::parser::RawTypeName::Record => {
                        return Err(ParseError::UnsupportedType("record".into()));
                    }
                    crate::backend::parser::RawTypeName::Named { name } => {
                        return Err(ParseError::UnsupportedType(name.clone()));
                    }
                };
                if sql_type.kind == SqlTypeKind::AnyArray {
                    return Err(ParseError::UnsupportedType("anyarray".into()));
                }
                let nullable =
                    column.nullable && !primary_columns.contains(&column.name.to_ascii_lowercase());
                let mut desc = column_desc(column.name.clone(), sql_type, nullable);
                desc.default_expr = column.default_expr.clone();
                desc.missing_default_value = column
                    .default_expr
                    .as_deref()
                    .and_then(|sql| super::derive_literal_default_value(sql, sql_type).ok());
                Ok(desc)
            })
            .collect::<Result<Vec<_>, _>>()?,
    };

    Ok(LoweredCreateTable {
        relation_desc,
        constraint_actions,
    })
}

fn validate_constraint_columns(
    constraint: &TableConstraint,
    columns: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    let referenced = match constraint {
        TableConstraint::PrimaryKey { columns } | TableConstraint::Unique { columns } => columns,
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{
        ColumnDef, CreateTableElement, OnCommitAction, RawTypeName, SqlType, TablePersistence,
    };

    #[test]
    fn lower_create_table_rejects_anyarray_columns() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "bad_anyarray".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "a".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::AnyArray)),
                default_expr: None,
                nullable: true,
                primary_key: false,
                unique: false,
            })],
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(&stmt),
            Err(ParseError::UnsupportedType("anyarray".into()))
        );
    }
}
