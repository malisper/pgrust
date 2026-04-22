use std::collections::BTreeSet;

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SerialKind, SqlType, SqlTypeKind};

use super::{
    CatalogLookup, CheckConstraintAction, CreateTableStatement, ForeignKeyConstraintAction,
    IndexBackedConstraintAction, LoweredPartitionSpec, NotNullConstraintAction, ParseError,
    PartitionBoundSpec,
    normalize_create_table_constraints, raw_type_name_hint, resolve_raw_type_name,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredCreateTable {
    pub relation_desc: RelationDesc,
    pub not_null_actions: Vec<NotNullConstraintAction>,
    pub check_actions: Vec<CheckConstraintAction>,
    pub constraint_actions: Vec<IndexBackedConstraintAction>,
    pub foreign_key_actions: Vec<ForeignKeyConstraintAction>,
    pub owned_sequences: Vec<OwnedSequenceSpec>,
    pub parent_oids: Vec<u32>,
    pub partition_spec: Option<LoweredPartitionSpec>,
    pub partition_parent_oid: Option<u32>,
    pub partition_bound: Option<PartitionBoundSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedSequenceSpec {
    pub column_index: usize,
    pub column_name: String,
    pub serial_kind: SerialKind,
    pub sql_type: SqlType,
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
    let normalized = normalize_create_table_constraints(stmt, catalog)?;
    let constraint_actions = normalized.index_backed.clone();
    let mut owned_sequences = Vec::new();

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
            .enumerate()
            .map(|(index, column)| {
                let sql_type = match column.ty {
                    crate::backend::parser::RawTypeName::Serial(_) => {
                        raw_type_name_hint(&column.ty)
                    }
                    _ => resolve_raw_type_name(&column.ty, catalog)?,
                };
                if sql_type.kind == SqlTypeKind::AnyArray {
                    return Err(ParseError::UnsupportedType("anyarray".into()));
                }
                let not_null = not_nulls_by_column.get(&column.name.to_ascii_lowercase());
                let serial_kind = match column.ty {
                    crate::backend::parser::RawTypeName::Serial(kind) => Some(kind),
                    _ => None,
                };
                if serial_kind.is_some() && column.default_expr.is_some() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "serial column without explicit DEFAULT",
                        actual: format!(
                            "multiple default values specified for column \"{}\"",
                            column.name
                        ),
                    });
                }
                let nullable = not_null.is_none() && serial_kind.is_none();
                let mut desc = column_desc(column.name.clone(), sql_type, nullable);
                if let Some(not_null) = not_null {
                    desc.not_null_constraint_name = Some(not_null.constraint_name.clone());
                    desc.not_null_constraint_validated = !not_null.not_valid;
                    desc.not_null_constraint_no_inherit = not_null.no_inherit;
                    desc.not_null_primary_key_owned = not_null.primary_key_owned;
                }
                desc.default_expr = column.default_expr.clone();
                desc.missing_default_value = column
                    .default_expr
                    .as_deref()
                    .and_then(|sql| super::derive_literal_default_value(sql, sql_type).ok());
                if let Some(serial_kind) = serial_kind {
                    desc.default_expr = None;
                    desc.missing_default_value = None;
                    owned_sequences.push(OwnedSequenceSpec {
                        column_index: index,
                        column_name: column.name.clone(),
                        serial_kind,
                        sql_type,
                    });
                }
                Ok(desc)
            })
            .collect::<Result<Vec<_>, _>>()?,
    };

    Ok(LoweredCreateTable {
        relation_desc,
        not_null_actions: normalized.not_nulls,
        check_actions: normalized.checks,
        constraint_actions,
        foreign_key_actions: normalized.foreign_keys,
        owned_sequences,
        parent_oids: Vec::new(),
        partition_spec: None,
        partition_parent_oid: None,
        partition_bound: None,
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
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
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
                            no_inherit: true,
                            ..ConstraintAttributes::default()
                        },
                    }],
                }),
            ],
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
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
        assert!(lowered.relation_desc.columns[1].not_null_constraint_no_inherit);
        assert!(lowered.not_null_actions[1].no_inherit);
    }

    #[test]
    fn lower_create_table_rejects_conflicting_not_null_no_inherit() {
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
                CreateTableElement::Constraint(crate::backend::parser::TableConstraint::NotNull {
                    attributes: ConstraintAttributes {
                        no_inherit: true,
                        ..ConstraintAttributes::default()
                    },
                    column: "id".into(),
                }),
            ],
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::InvalidTableDefinition(
                "conflicting NO INHERIT declaration for not-null constraint on column \"id\""
                    .into(),
            ))
        );
    }

    #[test]
    fn lower_create_table_tracks_serial_columns() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "items".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "id".into(),
                ty: RawTypeName::Serial(SerialKind::Regular),
                default_expr: None,
                constraints: vec![],
            })],
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        let lowered = lower_create_table(
            &stmt,
            &crate::backend::parser::analyze::LiteralDefaultCatalog,
        )
        .unwrap();
        assert_eq!(
            lowered.relation_desc.columns[0].sql_type,
            SqlType::new(SqlTypeKind::Int4)
        );
        assert!(!lowered.relation_desc.columns[0].storage.nullable);
        assert_eq!(lowered.owned_sequences.len(), 1);
        assert_eq!(lowered.owned_sequences[0].column_index, 0);
        assert_eq!(lowered.owned_sequences[0].column_name, "id");
        assert_eq!(lowered.owned_sequences[0].serial_kind, SerialKind::Regular);
    }

    #[test]
    fn lower_create_table_rejects_explicit_default_on_serial() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "items".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "id".into(),
                ty: RawTypeName::Serial(SerialKind::Regular),
                default_expr: Some("7".into()),
                constraints: vec![],
            })],
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::UnexpectedToken {
                expected: "serial column without explicit DEFAULT",
                actual: "multiple default values specified for column \"id\"".into(),
            })
        );
    }
}
