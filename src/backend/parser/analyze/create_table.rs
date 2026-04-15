use std::collections::BTreeSet;

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlTypeKind;

use super::{
    CatalogLookup, CheckConstraintAction, CreateTableStatement, IndexBackedConstraintAction,
    NotNullConstraintAction, ParseError, normalize_create_table_constraints, resolve_raw_type_name,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredCreateTable {
    pub relation_desc: RelationDesc,
    pub not_null_actions: Vec<NotNullConstraintAction>,
    pub check_actions: Vec<CheckConstraintAction>,
    pub constraint_actions: Vec<IndexBackedConstraintAction>,
    pub parent_oids: Vec<u32>,
}

pub fn create_relation_desc(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<RelationDesc, ParseError> {
    Ok(lower_create_table(stmt, catalog)?.relation_desc)
}

pub fn lower_create_table(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<LoweredCreateTable, ParseError> {
    let columns = stmt.columns().cloned().collect::<Vec<_>>();
    let normalized = normalize_create_table_constraints(stmt)?;
    let constraint_actions = normalized.index_backed.clone();

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

    let not_nulls_by_column = normalized
        .not_nulls
        .iter()
        .map(|constraint| (constraint.column.to_ascii_lowercase(), constraint))
        .collect::<std::collections::BTreeMap<_, _>>();

    let relation_desc = RelationDesc {
        columns: columns
            .iter()
            .map(|column| {
                let sql_type = resolve_raw_type_name(&column.ty, catalog)?;
                if sql_type.kind == SqlTypeKind::AnyArray {
                    return Err(ParseError::UnsupportedType("anyarray".into()));
                }
                let not_null = not_nulls_by_column.get(&column.name.to_ascii_lowercase());
                let nullable = not_null.is_none();
                let mut desc = column_desc(column.name.clone(), sql_type, nullable);
                if let Some(not_null) = not_null {
                    desc.not_null_constraint_name = Some(not_null.constraint_name.clone());
                    desc.not_null_constraint_validated = !not_null.not_valid;
                    desc.not_null_primary_key_owned = not_null.primary_key_owned;
                }
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
        not_null_actions: normalized.not_nulls,
        check_actions: normalized.checks,
        constraint_actions,
        parent_oids: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{
        ColumnDef, ConstraintAttributes, CreateTableElement, OnCommitAction, RawTypeName, SqlType,
        TablePersistence,
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
                constraints: vec![],
            })],
            inherits: Vec::new(),
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::UnsupportedType("anyarray".into()))
        );
    }

    #[test]
    fn lower_create_table_materializes_not_null_metadata() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "items".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![
                CreateTableElement::Column(ColumnDef {
                    name: "id".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                    default_expr: None,
                    constraints: vec![crate::backend::parser::ColumnConstraint::PrimaryKey {
                        attributes: ConstraintAttributes::default(),
                    }],
                }),
                CreateTableElement::Column(ColumnDef {
                    name: "note".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                    default_expr: None,
                    constraints: vec![crate::backend::parser::ColumnConstraint::NotNull {
                        attributes: ConstraintAttributes {
                            name: Some("items_note_nn".into()),
                            not_valid: true,
                            ..ConstraintAttributes::default()
                        },
                    }],
                }),
            ],
            inherits: Vec::new(),
            if_not_exists: false,
        };

        let lowered = lower_create_table(
            &stmt,
            &crate::backend::parser::analyze::LiteralDefaultCatalog,
        )
        .unwrap();
        assert_eq!(lowered.not_null_actions.len(), 2);
        assert_eq!(
            lowered.relation_desc.columns[0]
                .not_null_constraint_name
                .as_deref(),
            Some("items_id_not_null")
        );
        assert!(lowered.relation_desc.columns[0].not_null_primary_key_owned);
        assert_eq!(
            lowered.relation_desc.columns[1]
                .not_null_constraint_name
                .as_deref(),
            Some("items_note_nn")
        );
        assert!(!lowered.relation_desc.columns[1].not_null_constraint_validated);
    }
}
