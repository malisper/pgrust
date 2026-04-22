use super::{
    CatalogLookup, ColumnConstraint, ConstraintAttributes, CreateTableElement,
    CreateTableStatement, ParseError, RawTypeName, TableConstraint,
    generated_not_null_constraint_name,
};
use crate::backend::parser::ColumnDef;
use crate::include::catalog::{CONSTRAINT_CHECK, DEFAULT_COLLATION_OID, PgConstraintRow};
use crate::include::nodes::primnodes::ColumnDesc;

pub(crate) fn expand_create_table_like_clauses(
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
    let mut elements = Vec::new();
    for element in &stmt.elements {
        match element {
            CreateTableElement::Column(column) => {
                elements.push(CreateTableElement::Column(column.clone()));
            }
            CreateTableElement::Constraint(constraint) => {
                elements.push(CreateTableElement::Constraint(constraint.clone()));
            }
            CreateTableElement::Like(clause) => {
                validate_supported_table_like_options(clause)?;
                let source = catalog
                    .lookup_any_relation(&clause.relation_name)
                    .ok_or_else(|| ParseError::UnknownTable(clause.relation_name.clone()))?;
                if !matches!(source.relkind, 'r' | 'p') {
                    return Err(ParseError::WrongObjectType {
                        name: clause.relation_name.clone(),
                        expected: "table",
                    });
                }

                let source_constraints = catalog.constraint_rows_for_relation(source.relation_oid);
                let source_table_name = source_relation_base_name(&clause.relation_name);
                for column in &source.desc.columns {
                    if column.dropped {
                        continue;
                    }
                    elements.push(CreateTableElement::Column(copy_like_column(
                        &source_table_name,
                        column,
                        &source_constraints,
                        catalog,
                        clause,
                    )?));
                }
                if clause.options.constraints {
                    elements.extend(
                        copy_like_check_constraints(&source_constraints)
                            .into_iter()
                            .map(CreateTableElement::Constraint),
                    );
                }
            }
        }
    }
    expanded.elements = elements;
    Ok(expanded)
}

fn validate_supported_table_like_options(
    clause: &crate::backend::parser::TableLikeClause,
) -> Result<(), ParseError> {
    let unsupported = [
        (
            clause.options.indexes,
            "CREATE TABLE LIKE INCLUDING INDEXES",
        ),
        (
            clause.options.comments,
            "CREATE TABLE LIKE INCLUDING COMMENTS",
        ),
        (
            clause.options.statistics,
            "CREATE TABLE LIKE INCLUDING STATISTICS",
        ),
        (
            clause.options.generated,
            "CREATE TABLE LIKE INCLUDING GENERATED",
        ),
        (
            clause.options.identity,
            "CREATE TABLE LIKE INCLUDING IDENTITY",
        ),
    ];
    for (enabled, feature) in unsupported {
        if enabled {
            return Err(ParseError::FeatureNotSupported(feature.into()));
        }
    }
    Ok(())
}

fn source_relation_base_name(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn copy_like_column(
    source_table_name: &str,
    column: &ColumnDesc,
    source_constraints: &[PgConstraintRow],
    catalog: &dyn CatalogLookup,
    clause: &crate::backend::parser::TableLikeClause,
) -> Result<ColumnDef, ParseError> {
    let mut constraints = Vec::new();
    if !column.storage.nullable {
        constraints.push(ColumnConstraint::NotNull {
            attributes: ConstraintAttributes {
                name: copied_not_null_constraint_name(
                    source_table_name,
                    column,
                    source_constraints,
                ),
                not_valid: false,
                no_inherit: column.not_null_constraint_no_inherit,
                ..ConstraintAttributes::default()
            },
        });
    }

    Ok(ColumnDef {
        name: column.name.clone(),
        ty: RawTypeName::Builtin(column.sql_type),
        default_expr: clause
            .options
            .defaults
            .then(|| column.default_expr.clone())
            .flatten(),
        collation: copied_collation_name(column, catalog)?,
        storage: clause.options.storage.then_some(column.storage.attstorage),
        compression: clause
            .options
            .compression
            .then_some(column.storage.attcompression),
        constraints,
    })
}

fn copied_not_null_constraint_name(
    source_table_name: &str,
    column: &ColumnDesc,
    source_constraints: &[PgConstraintRow],
) -> Option<String> {
    let constraint_name = column.not_null_constraint_name.as_ref()?;
    let generated =
        generated_not_null_constraint_name(source_table_name, &column.name, source_constraints);
    (constraint_name != &generated).then(|| constraint_name.clone())
}

fn copied_collation_name(
    column: &ColumnDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Option<String>, ParseError> {
    let Some(collation_oid) = column.collation_oid else {
        return Ok(None);
    };
    if collation_oid == DEFAULT_COLLATION_OID {
        return Ok(None);
    }
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == collation_oid)
        .map(|row| Some(row.collname))
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known collation",
            actual: format!("unknown collation oid {collation_oid}"),
        })
}

fn copy_like_check_constraints(source_constraints: &[PgConstraintRow]) -> Vec<TableConstraint> {
    source_constraints
        .iter()
        .filter(|row| row.contype == CONSTRAINT_CHECK)
        .filter_map(|row| {
            row.conbin.clone().map(|expr_sql| TableConstraint::Check {
                attributes: ConstraintAttributes {
                    name: Some(row.conname.clone()),
                    not_valid: false,
                    no_inherit: row.connoinherit,
                    ..ConstraintAttributes::default()
                },
                expr_sql,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RelFileLocator;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::parser::{
        CreateTableElement, OnCommitAction, TableLikeClause, TableLikeOptions, TablePersistence,
    };
    use crate::include::access::htup::{AttributeCompression, AttributeStorage};
    use crate::include::catalog::{
        C_COLLATION_OID, CONSTRAINT_CHECK, CONSTRAINT_NOTNULL, DEFAULT_COLLATION_OID,
        PgConstraintRow,
    };
    use crate::include::nodes::primnodes::RelationDesc;

    struct TestCatalog {
        relations: Vec<(String, super::super::BoundRelation)>,
        constraints: Vec<PgConstraintRow>,
    }

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, name: &str) -> Option<super::super::BoundRelation> {
            self.relations
                .iter()
                .find(|(relation_name, _)| relation_name.eq_ignore_ascii_case(name))
                .map(|(_, relation)| relation.clone())
        }

        fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
            self.constraints
                .iter()
                .filter(|row| row.conrelid == relation_oid)
                .cloned()
                .collect()
        }
    }

    fn relation_with_columns(
        _name: &str,
        relation_oid: u32,
        relkind: char,
        columns: Vec<ColumnDesc>,
    ) -> super::super::BoundRelation {
        super::super::BoundRelation {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: relation_oid,
            },
            relation_oid,
            toast: None,
            namespace_oid: 0,
            owner_oid: 0,
            relpersistence: 'p',
            relkind,
            desc: RelationDesc { columns },
        }
    }

    fn like_stmt(options: TableLikeOptions) -> CreateTableStatement {
        CreateTableStatement {
            schema_name: None,
            table_name: "dest".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Like(TableLikeClause {
                relation_name: "source".into(),
                options,
            })],
            inherits: Vec::new(),
            if_not_exists: false,
        }
    }

    #[test]
    fn expand_plain_like_copies_core_column_shape() {
        let mut id = column_desc(
            "id",
            crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
            false,
        );
        id.not_null_constraint_name = Some("source_id_nn".into());

        let mut note = column_desc(
            "note",
            crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
            true,
        );
        note.collation_oid = Some(C_COLLATION_OID);

        let catalog = TestCatalog {
            relations: vec![(
                "source".into(),
                relation_with_columns("source", 42, 'r', vec![id, note]),
            )],
            constraints: vec![PgConstraintRow {
                oid: 1,
                conname: "source_id_not_null".into(),
                connamespace: 0,
                contype: CONSTRAINT_NOTNULL,
                condeferrable: false,
                condeferred: false,
                conenforced: true,
                convalidated: true,
                conrelid: 42,
                contypid: 0,
                conindid: 0,
                conparentid: 0,
                confrelid: 0,
                conkey: Some(vec![1]),
                confkey: None,
                confupdtype: 'a',
                confdeltype: 'a',
                confmatchtype: 's',
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
            }],
        };

        let expanded =
            expand_create_table_like_clauses(&like_stmt(TableLikeOptions::default()), &catalog)
                .unwrap();

        let columns = expanded.columns().cloned().collect::<Vec<_>>();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].collation, None);
        assert!(columns[0].default_expr.is_none());
        assert!(matches!(
            columns[0].constraints.as_slice(),
            [ColumnConstraint::NotNull { .. }]
        ));
        assert_eq!(columns[1].name, "note");
        assert_eq!(columns[1].collation.as_deref(), Some("C"));
    }

    #[test]
    fn expand_like_copies_requested_options() {
        let mut note = column_desc(
            "note",
            crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
            false,
        );
        note.default_expr = Some("'hello'".into());
        note.collation_oid = Some(DEFAULT_COLLATION_OID);
        note.storage.attstorage = AttributeStorage::External;
        note.storage.attcompression = AttributeCompression::Pglz;

        let catalog = TestCatalog {
            relations: vec![(
                "source".into(),
                relation_with_columns("source", 7, 'r', vec![note]),
            )],
            constraints: vec![
                PgConstraintRow {
                    oid: 1,
                    conname: "source_note_not_null".into(),
                    connamespace: 0,
                    contype: CONSTRAINT_NOTNULL,
                    condeferrable: false,
                    condeferred: false,
                    conenforced: true,
                    convalidated: true,
                    conrelid: 7,
                    contypid: 0,
                    conindid: 0,
                    conparentid: 0,
                    confrelid: 0,
                    conkey: Some(vec![1]),
                    confkey: None,
                    confupdtype: 'a',
                    confdeltype: 'a',
                    confmatchtype: 's',
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
                PgConstraintRow {
                    oid: 2,
                    conname: "source_check".into(),
                    connamespace: 0,
                    contype: CONSTRAINT_CHECK,
                    condeferrable: false,
                    condeferred: false,
                    conenforced: true,
                    convalidated: false,
                    conrelid: 7,
                    contypid: 0,
                    conindid: 0,
                    conparentid: 0,
                    confrelid: 0,
                    conkey: None,
                    confkey: None,
                    confupdtype: 'a',
                    confdeltype: 'a',
                    confmatchtype: 's',
                    conpfeqop: None,
                    conppeqop: None,
                    conffeqop: None,
                    confdelsetcols: None,
                    conexclop: None,
                    conbin: Some("length(note) > 0".into()),
                    conislocal: true,
                    coninhcount: 0,
                    connoinherit: true,
                    conperiod: false,
                },
            ],
        };

        let expanded = expand_create_table_like_clauses(
            &like_stmt(TableLikeOptions {
                defaults: true,
                constraints: true,
                storage: true,
                compression: true,
                ..TableLikeOptions::default()
            }),
            &catalog,
        )
        .unwrap();

        let columns = expanded.columns().cloned().collect::<Vec<_>>();
        assert_eq!(columns[0].default_expr.as_deref(), Some("'hello'"));
        assert_eq!(columns[0].storage, Some(AttributeStorage::External));
        assert_eq!(columns[0].compression, Some(AttributeCompression::Pglz));
        assert_eq!(columns[0].collation, None);
        assert!(matches!(
            expanded.constraints().next(),
            Some(TableConstraint::Check { attributes, expr_sql })
                if attributes.name.as_deref() == Some("source_check")
                    && !attributes.not_valid
                    && attributes.no_inherit
                    && expr_sql == "length(note) > 0"
        ));
    }

    #[test]
    fn expand_like_rejects_non_tables() {
        let catalog = TestCatalog {
            relations: vec![(
                "source".into(),
                relation_with_columns("source", 9, 'S', Vec::new()),
            )],
            constraints: Vec::new(),
        };

        assert_eq!(
            expand_create_table_like_clauses(&like_stmt(TableLikeOptions::default()), &catalog),
            Err(ParseError::WrongObjectType {
                name: "source".into(),
                expected: "table",
            })
        );
    }

    #[test]
    fn expand_like_rejects_unsupported_options_in_fixed_order() {
        let catalog = TestCatalog {
            relations: vec![(
                "source".into(),
                relation_with_columns("source", 9, 'r', Vec::new()),
            )],
            constraints: Vec::new(),
        };

        assert_eq!(
            expand_create_table_like_clauses(
                &like_stmt(TableLikeOptions {
                    comments: true,
                    indexes: true,
                    ..TableLikeOptions::default()
                }),
                &catalog,
            ),
            Err(ParseError::FeatureNotSupported(
                "CREATE TABLE LIKE INCLUDING INDEXES".into(),
            ))
        );
    }

    #[test]
    fn expand_like_preserves_interleaved_element_order() {
        let catalog = TestCatalog {
            relations: vec![(
                "source".into(),
                relation_with_columns(
                    "source",
                    11,
                    'r',
                    vec![column_desc(
                        "copied",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Int4,
                        ),
                        true,
                    )],
                ),
            )],
            constraints: Vec::new(),
        };
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "dest".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![
                CreateTableElement::Column(ColumnDef {
                    name: "before".into(),
                    ty: RawTypeName::Builtin(crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Int4,
                    )),
                    default_expr: None,
                    collation: None,
                    storage: None,
                    compression: None,
                    constraints: Vec::new(),
                }),
                CreateTableElement::Like(TableLikeClause {
                    relation_name: "source".into(),
                    options: TableLikeOptions::default(),
                }),
                CreateTableElement::Column(ColumnDef {
                    name: "after".into(),
                    ty: RawTypeName::Builtin(crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Int4,
                    )),
                    default_expr: None,
                    collation: None,
                    storage: None,
                    compression: None,
                    constraints: Vec::new(),
                }),
            ],
            inherits: Vec::new(),
            if_not_exists: false,
        };

        let expanded = expand_create_table_like_clauses(&stmt, &catalog).unwrap();
        let column_names = expanded
            .columns()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(column_names, vec!["before", "copied", "after"]);
    }
}
