use std::collections::BTreeSet;

use crate::backend::access::common::toast_compression::ensure_attribute_compression_supported;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{
    ColumnConstraint, ConstraintAttributes, CreateTableElement, CreateTableLikeClause,
    CreateTableLikeOption, RawTypeName, SerialKind, SqlType, SqlTypeKind, TableConstraint,
};
use crate::include::access::htup::{AttributeCompression, AttributeStorage};
use crate::include::catalog::{CONSTRAINT_CHECK, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE};
use crate::pgrust::database::ddl::format_sql_type_name;

use super::{
    CatalogLookup, CheckConstraintAction, CreateTableStatement, ForeignKeyConstraintAction,
    IndexBackedConstraintAction, LoweredPartitionSpec, NotNullConstraintAction, ParseError,
    PartitionBoundSpec, normalize_create_table_constraints, raw_type_name_hint,
    resolve_raw_type_name,
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
    let expanded = expand_create_table_like_clauses(stmt, catalog)?;
    let columns = expanded.columns().cloned().collect::<Vec<_>>();
    let normalized = normalize_create_table_constraints(&expanded, catalog)?;
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
                if let Some(compression) = column.compression {
                    validate_create_column_compression(sql_type, compression)?;
                    desc.storage.attcompression = compression;
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

fn expand_create_table_like_clauses(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<CreateTableStatement, ParseError> {
    if !stmt
        .elements
        .iter()
        .any(|element| matches!(element, CreateTableElement::Like(_)))
    {
        return Ok(stmt.clone());
    }

    let mut expanded = stmt.clone();
    expanded.elements = Vec::new();
    for element in &stmt.elements {
        match element {
            CreateTableElement::Column(_) | CreateTableElement::Constraint(_) => {
                expanded.elements.push(element.clone());
            }
            CreateTableElement::Like(like) => {
                expanded.elements.extend(expand_like_clause(like, catalog)?);
            }
        }
    }
    Ok(expanded)
}

#[derive(Debug, Clone, Copy)]
struct LikeExpansionOptions {
    defaults: bool,
    constraints: bool,
    indexes: bool,
}

impl LikeExpansionOptions {
    fn from_clause(clause: &CreateTableLikeClause) -> Self {
        let mut options = Self {
            defaults: false,
            constraints: false,
            indexes: false,
        };
        for option in &clause.options {
            match option {
                CreateTableLikeOption::IncludingDefaults => options.defaults = true,
                CreateTableLikeOption::IncludingConstraints => options.constraints = true,
                CreateTableLikeOption::IncludingIndexes => options.indexes = true,
                CreateTableLikeOption::IncludingAll => {
                    options.defaults = true;
                    options.constraints = true;
                    options.indexes = true;
                }
                CreateTableLikeOption::ExcludingDefaults => options.defaults = false,
                CreateTableLikeOption::ExcludingConstraints => options.constraints = false,
                CreateTableLikeOption::ExcludingIndexes => options.indexes = false,
                CreateTableLikeOption::ExcludingAll => {
                    options.defaults = false;
                    options.constraints = false;
                    options.indexes = false;
                }
            }
        }
        options
    }
}

fn expand_like_clause(
    clause: &CreateTableLikeClause,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<CreateTableElement>, ParseError> {
    let source = catalog
        .lookup_any_relation(&clause.relation_name)
        .ok_or_else(|| ParseError::UnknownTable(clause.relation_name.clone()))?;
    if !matches!(source.relkind, 'r' | 'p' | 'v' | 'm' | 'f' | 'c') {
        return Err(ParseError::FeatureNotSupported(format!(
            "CREATE TABLE LIKE source relation kind {}",
            source.relkind
        )));
    }
    let options = LikeExpansionOptions::from_clause(clause);
    let mut elements = source
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| {
            let mut constraints = Vec::new();
            if !column.storage.nullable {
                constraints.push(ColumnConstraint::NotNull {
                    attributes: ConstraintAttributes {
                        name: column.not_null_constraint_name.clone(),
                        not_valid: !column.not_null_constraint_validated,
                        no_inherit: column.not_null_constraint_no_inherit,
                        ..ConstraintAttributes::default()
                    },
                });
            }
            CreateTableElement::Column(crate::backend::parser::ColumnDef {
                name: column.name.clone(),
                ty: RawTypeName::Builtin(column.sql_type),
                default_expr: options
                    .defaults
                    .then(|| column.default_expr.clone())
                    .flatten(),
                compression: match column.storage.attcompression {
                    AttributeCompression::Default => None,
                    compression => Some(compression),
                },
                constraints,
            })
        })
        .collect::<Vec<_>>();

    let constraints = catalog.constraint_rows_for_relation(source.relation_oid);
    if options.constraints {
        elements.extend(constraints.iter().filter_map(|row| {
            if row.contype != CONSTRAINT_CHECK {
                return None;
            }
            Some(CreateTableElement::Constraint(TableConstraint::Check {
                attributes: ConstraintAttributes {
                    name: Some(row.conname.clone()),
                    not_valid: !row.convalidated,
                    no_inherit: row.connoinherit,
                    enforced: Some(row.conenforced),
                    ..ConstraintAttributes::default()
                },
                expr_sql: row.conbin.clone()?,
            }))
        }));
    }

    if options.indexes {
        for row in constraints
            .iter()
            .filter(|row| row.contype == CONSTRAINT_PRIMARY || row.contype == CONSTRAINT_UNIQUE)
        {
            let Some(columns) = constraint_column_names(row.conkey.as_deref(), &source.desc) else {
                continue;
            };
            let attributes = ConstraintAttributes::default();
            let without_overlaps = row.conperiod.then(|| columns.last().cloned()).flatten();
            elements.push(CreateTableElement::Constraint(
                if row.contype == CONSTRAINT_PRIMARY {
                    TableConstraint::PrimaryKey {
                        attributes,
                        columns,
                        without_overlaps,
                    }
                } else {
                    TableConstraint::Unique {
                        attributes,
                        columns,
                        without_overlaps,
                    }
                },
            ));
        }
    }

    Ok(elements)
}

fn constraint_column_names(attnums: Option<&[i16]>, desc: &RelationDesc) -> Option<Vec<String>> {
    attnums?
        .iter()
        .map(|attnum| {
            let index = usize::try_from(*attnum).ok()?.checked_sub(1)?;
            let column = desc.columns.get(index)?;
            (!column.dropped).then(|| column.name.clone())
        })
        .collect()
}

fn validate_create_column_compression(
    sql_type: SqlType,
    compression: AttributeCompression,
) -> Result<(), ParseError> {
    ensure_attribute_compression_supported(compression).map_err(|err| match err {
        crate::backend::executor::ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } => ParseError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
        other => ParseError::FeatureNotSupportedMessage(format!("{other:?}")),
    })?;

    let type_default = column_desc("attcompression_check", sql_type, true)
        .storage
        .attstorage;
    if compression != AttributeCompression::Default && type_default == AttributeStorage::Plain {
        return Err(ParseError::DetailedError {
            message: format!(
                "column data type {} does not support compression",
                format_sql_type_name(sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{
        ColumnDef, ConstraintAttributes, CreateTableElement, OnCommitAction, RawTypeName, SqlType,
        TablePersistence,
    };
    use crate::include::catalog::PgConstraintRow;

    #[derive(Default)]
    struct LikeCatalog {
        relation: Option<crate::backend::parser::BoundRelation>,
        constraints: Vec<PgConstraintRow>,
    }

    impl crate::backend::parser::CatalogLookup for LikeCatalog {
        fn lookup_any_relation(&self, name: &str) -> Option<crate::backend::parser::BoundRelation> {
            self.relation
                .as_ref()
                .filter(|_| name == "source_table")
                .cloned()
        }

        fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
            self.relation
                .as_ref()
                .filter(|relation| relation.relation_oid == relation_oid)
                .map(|_| self.constraints.clone())
                .unwrap_or_default()
        }
    }

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
                compression: None,
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
                    compression: None,
                    constraints: vec![crate::backend::parser::ColumnConstraint::PrimaryKey {
                        attributes: ConstraintAttributes::default(),
                    }],
                }),
                CreateTableElement::Column(ColumnDef {
                    name: "note".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                    default_expr: None,
                    compression: None,
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
    fn lower_create_table_like_including_all_expands_columns_and_constraints() {
        let mut source_desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        source_desc.columns[0].not_null_constraint_name = Some("source_id_not_null".into());
        source_desc.columns[0].default_expr = Some("7".into());
        let catalog = LikeCatalog {
            relation: Some(crate::backend::parser::BoundRelation {
                rel: crate::RelFileLocator {
                    spc_oid: 1,
                    db_oid: 1,
                    rel_number: 42,
                },
                relation_oid: 42,
                toast: None,
                namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
                owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                relpersistence: 'p',
                relkind: 'r',
                relispartition: false,
                relpartbound: None,
                desc: source_desc,
                partitioned_table: None,
            }),
            constraints: vec![
                PgConstraintRow {
                    oid: 100,
                    conname: "source_check".into(),
                    connamespace: crate::include::catalog::PUBLIC_NAMESPACE_OID,
                    contype: CONSTRAINT_CHECK,
                    condeferrable: false,
                    condeferred: false,
                    conenforced: true,
                    convalidated: true,
                    conrelid: 42,
                    contypid: 0,
                    conindid: 0,
                    conparentid: 0,
                    confrelid: 0,
                    confupdtype: 'a',
                    confdeltype: 'a',
                    confmatchtype: 's',
                    conkey: None,
                    confkey: None,
                    conpfeqop: None,
                    conppeqop: None,
                    conffeqop: None,
                    confdelsetcols: None,
                    conexclop: None,
                    conbin: Some("id > 0".into()),
                    conislocal: true,
                    coninhcount: 0,
                    connoinherit: false,
                    conperiod: false,
                },
                PgConstraintRow {
                    oid: 101,
                    conname: "source_pkey".into(),
                    connamespace: crate::include::catalog::PUBLIC_NAMESPACE_OID,
                    contype: CONSTRAINT_PRIMARY,
                    condeferrable: false,
                    condeferred: false,
                    conenforced: true,
                    convalidated: true,
                    conrelid: 42,
                    contypid: 0,
                    conindid: 102,
                    conparentid: 0,
                    confrelid: 0,
                    confupdtype: 'a',
                    confdeltype: 'a',
                    confmatchtype: 's',
                    conkey: Some(vec![1]),
                    confkey: None,
                    conpfeqop: None,
                    conppeqop: None,
                    conffeqop: None,
                    confdelsetcols: None,
                    conexclop: None,
                    conbin: None,
                    conislocal: true,
                    coninhcount: 0,
                    connoinherit: false,
                    conperiod: false,
                },
            ],
        };
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "copy_table".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Like(CreateTableLikeClause {
                relation_name: "source_table".into(),
                options: vec![CreateTableLikeOption::IncludingAll],
            })],
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        let lowered = lower_create_table(&stmt, &catalog).unwrap();
        assert_eq!(lowered.relation_desc.columns.len(), 2);
        assert_eq!(
            lowered.relation_desc.columns[0].default_expr.as_deref(),
            Some("7")
        );
        assert_eq!(lowered.check_actions[0].constraint_name, "source_check");
        assert_eq!(
            lowered.constraint_actions[0].constraint_name.as_deref(),
            Some("copy_table_pkey")
        );
        assert_eq!(lowered.constraint_actions[0].columns, vec!["id"]);
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
                    compression: None,
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
                compression: None,
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
                compression: None,
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

    #[test]
    fn lower_create_table_applies_column_compression() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "cmdata".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "f1".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                default_expr: None,
                compression: Some(AttributeCompression::Pglz),
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
            lowered.relation_desc.columns[0].storage.attcompression,
            AttributeCompression::Pglz
        );
    }

    #[test]
    fn lower_create_table_rejects_compression_for_plain_types() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "cmdata".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "f1".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                default_expr: None,
                compression: Some(AttributeCompression::Pglz),
                constraints: vec![],
            })],
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert!(matches!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::DetailedError { message, sqlstate, .. })
                if message == "column data type integer does not support compression"
                    && sqlstate == "0A000"
        ));
    }
}
