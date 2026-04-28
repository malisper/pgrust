use super::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{Expr, Plan, RelationDesc, Value};
use crate::include::access::htup::{AttributeAlign, AttributeCompression, AttributeStorage};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, JSON_TYPE_OID,
    PUBLIC_NAMESPACE_OID, PgAggregateRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow,
    PgCollationRow, PgLanguageRow, PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgPolicyRow,
    PgProcRow, PgRangeRow, PgRewriteRow, PgTypeRow, PolicyCommand, RECORD_TYPE_OID,
    bootstrap_pg_proc_rows, sort_pg_rewrite_rows,
};
use crate::include::nodes::parsenodes::{
    AggregateArgType, AggregateSignature, AggregateSignatureArg, AggregateSignatureKind,
    AliasColumnDef, AliasColumnSpec, AlterAggregateRenameStatement, AlterColumnExpressionAction,
    AlterColumnIdentityAction, AlterGenericOptionAction, AlterTableTriggerMode,
    AlterTableTriggerStateStatement, AlterTableTriggerTarget, AlterTriggerRenameStatement,
    AlterTypeSetOptionsStatement, CastContext, ColumnConstraint, ColumnGeneratedKind,
    ColumnIdentityKind, CommentOnAggregateStatement, CommentOnColumnStatement,
    CommentOnFunctionStatement, CommentOnOperatorStatement, CommentOnTypeStatement,
    CommentOnViewStatement, CompositeTypeAttributeDef, CreateAggregateStatement,
    CreateBaseTypeOption, CreateBaseTypeStatement, CreateCastMethod, CreateCastStatement,
    CreateCompositeTypeStatement, CreateShellTypeStatement, CreateTriggerStatement,
    CreateTypeStatement, DropAggregateStatement, DropCastStatement, DropTriggerStatement,
    DropTypeStatement, ForeignKeyAction, ForeignKeyMatchType, GrantObjectPrivilege,
    GrantTableColumnPrivilege, IndexColumnDef, InsertSource, InsertStatement, JoinTreeNode,
    OverridingKind, PartitionStrategy, PublicationObjectSpec, PublicationOption,
    PublicationSchemaName, RangeTblEntryKind, RawPartitionBoundSpec, RawPartitionKey,
    RawPartitionRangeDatum, RawPartitionSpec, RawTypeName, SetSessionAuthorizationStatement,
    SqlCallArgs, TableConstraint, TriggerEvent, TriggerEventSpec, TriggerLevel,
    TriggerReferencingSpec, TriggerTiming, UserMappingUser, ViewCheckOption,
};
use crate::include::nodes::primnodes::{
    AttrNumber, INNER_VAR, JoinType, OUTER_VAR, Var, is_system_attr,
};

fn desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("name", SqlType::new(SqlTypeKind::Text), false),
            column_desc("note", SqlType::new(SqlTypeKind::Text), true),
        ],
    }
}

fn builtin_type(ty: SqlType) -> RawTypeName {
    RawTypeName::Builtin(ty)
}

fn aggregate_signature_arg(arg_type: AggregateArgType) -> AggregateSignatureArg {
    AggregateSignatureArg {
        name: None,
        arg_type,
        variadic: false,
    }
}

fn aggregate_signature(args: Vec<AggregateSignatureArg>) -> AggregateSignatureKind {
    AggregateSignatureKind::Args(AggregateSignature {
        args,
        order_by: Vec::new(),
    })
}

fn attrs() -> ConstraintAttributes {
    ConstraintAttributes::default()
}

#[test]
fn parse_create_publication_mixed_targets_and_options() {
    let stmt = parse_statement(
        "create publication pub for tables in schema pub_test, table pub_test.widgets with (publish = 'insert, update', publish_via_partition_root = true)",
    )
    .unwrap();
    match stmt {
        Statement::CreatePublication(stmt) => {
            assert_eq!(stmt.publication_name, "pub");
            assert!(!stmt.target.for_all_tables);
            assert!(!stmt.target.for_all_sequences);
            assert_eq!(stmt.target.objects.len(), 2);
            assert!(matches!(
                &stmt.target.objects[0],
                PublicationObjectSpec::Schema(schema)
                    if schema.schema_name == PublicationSchemaName::Name("pub_test".into())
            ));
            assert!(matches!(
                &stmt.target.objects[1],
                PublicationObjectSpec::Table(table)
                    if table.relation_name == "pub_test.widgets"
            ));
            assert!(stmt.options.options.iter().any(|option| matches!(
                option,
                PublicationOption::Publish(actions)
                    if actions.insert && actions.update && !actions.delete && !actions.truncate
            )));
            assert!(
                stmt.options.options.iter().any(|option| matches!(
                    option,
                    PublicationOption::PublishViaPartitionRoot(true)
                ))
            );
        }
        other => panic!("expected create publication, got {other:?}"),
    }
}

#[test]
fn parse_create_publication_all_sequences_targets() {
    let stmt = parse_statement("create publication pub for all sequences, all tables").unwrap();
    match stmt {
        Statement::CreatePublication(stmt) => {
            assert_eq!(stmt.publication_name, "pub");
            assert!(stmt.target.for_all_sequences);
            assert!(stmt.target.for_all_tables);
            assert!(stmt.target.objects.is_empty());
        }
        other => panic!("expected create publication, got {other:?}"),
    }

    let err =
        parse_statement("create publication pub for all sequences, all tables, all sequences")
            .unwrap_err();
    assert!(matches!(
        err.unpositioned(),
        ParseError::DetailedError { message, detail, .. }
            if message == "invalid publication object list"
                && detail.as_deref() == Some("ALL SEQUENCES can be specified only once.")
    ));
}

#[test]
fn parse_create_publication_for_all_tables_except() {
    let stmt = parse_statement(
        "create publication pub for all tables except (table pub_test.widgets, table only widgets)",
    )
    .unwrap();
    match stmt {
        Statement::CreatePublication(stmt) => {
            assert!(stmt.target.for_all_tables);
            assert!(!stmt.target.for_all_sequences);
            assert!(stmt.target.objects.is_empty());
            assert_eq!(stmt.target.except_tables.len(), 2);
            assert_eq!(
                stmt.target.except_tables[0].relation_name,
                "pub_test.widgets"
            );
            assert!(!stmt.target.except_tables[0].only);
            assert_eq!(stmt.target.except_tables[1].relation_name, "widgets");
            assert!(stmt.target.except_tables[1].only);
        }
        other => panic!("expected create publication, got {other:?}"),
    }

    let err =
        parse_statement("create publication pub for all tables except (widgets)").unwrap_err();
    assert!(matches!(
        err.unpositioned(),
        ParseError::DetailedError { message, detail, .. }
            if message == "invalid publication object list"
                && detail.as_deref() == Some(
                    "One of TABLE or TABLES IN SCHEMA must be specified before a standalone table or schema name."
                )
    ));
}

#[test]
fn parse_alter_publication_set_all_sequences() {
    let stmt = parse_statement("alter publication pub set all sequences").unwrap();
    match stmt {
        Statement::AlterPublication(stmt) => {
            assert_eq!(stmt.publication_name, "pub");
            assert!(matches!(
                stmt.action,
                AlterPublicationAction::SetObjects(target)
                    if target.for_all_sequences
                        && !target.for_all_tables
                        && target.objects.is_empty()
            ));
        }
        other => panic!("expected alter publication, got {other:?}"),
    }
}

#[test]
fn parse_alter_publication_set_all_tables_except() {
    let stmt =
        parse_statement("alter publication pub set all tables except (table widgets)").unwrap();
    match stmt {
        Statement::AlterPublication(stmt) => {
            assert_eq!(stmt.publication_name, "pub");
            assert!(matches!(
                stmt.action,
                AlterPublicationAction::SetObjects(target)
                    if target.for_all_tables
                        && !target.for_all_sequences
                        && target.objects.is_empty()
                        && target.except_tables.len() == 1
                        && target.except_tables[0].relation_name == "widgets"
            ));
        }
        other => panic!("expected alter publication, got {other:?}"),
    }
}

#[test]
fn parse_publication_current_schema_depends_on_target_mode() {
    let table_stmt = parse_statement("create publication pub for table current_schema").unwrap();
    match table_stmt {
        Statement::CreatePublication(stmt) => {
            assert!(matches!(
                &stmt.target.objects[0],
                PublicationObjectSpec::Table(table) if table.relation_name == "current_schema"
            ));
        }
        other => panic!("expected create publication, got {other:?}"),
    }

    let schema_stmt =
        parse_statement("create publication pub for tables in schema current_schema").unwrap();
    match schema_stmt {
        Statement::CreatePublication(stmt) => {
            assert!(matches!(
                &stmt.target.objects[0],
                PublicationObjectSpec::Schema(schema)
                    if schema.schema_name == PublicationSchemaName::CurrentSchema
            ));
        }
        other => panic!("expected create publication, got {other:?}"),
    }
}

#[test]
fn parse_alter_publication_actions() {
    assert!(matches!(
        parse_statement("alter publication pub rename to pub2").unwrap(),
        Statement::AlterPublication(AlterPublicationStatement {
            publication_name,
            action: AlterPublicationAction::Rename { new_name },
        }) if publication_name == "pub" && new_name == "pub2"
    ));
    assert!(matches!(
        parse_statement("alter publication pub owner to app_owner").unwrap(),
        Statement::AlterPublication(AlterPublicationStatement {
            publication_name,
            action: AlterPublicationAction::OwnerTo { new_owner },
        }) if publication_name == "pub" && new_owner == "app_owner"
    ));
    assert!(matches!(
        parse_statement("alter publication pub set (publish_generated_columns = stored)").unwrap(),
        Statement::AlterPublication(AlterPublicationStatement {
            action: AlterPublicationAction::SetOptions(_),
            ..
        })
    ));
    assert!(matches!(
        parse_statement("alter publication pub add tables in schema pub_test, table pub_test.t")
            .unwrap(),
        Statement::AlterPublication(AlterPublicationStatement {
            action: AlterPublicationAction::AddObjects(target),
            ..
        }) if target.objects.len() == 2
    ));
}

#[test]
fn parse_drop_and_comment_on_publication() {
    assert!(matches!(
        parse_statement("drop publication if exists pub1, pub2 cascade").unwrap(),
        Statement::DropPublication(DropPublicationStatement {
            if_exists: true,
            publication_names,
            cascade: true,
        }) if publication_names == vec!["pub1", "pub2"]
    ));
    assert!(matches!(
        parse_statement("comment on publication pub is 'hello'").unwrap(),
        Statement::CommentOnPublication(CommentOnPublicationStatement {
            publication_name,
            comment: Some(comment),
        }) if publication_name == "pub" && comment == "hello"
    ));
}

#[test]
fn parse_set_session_authorization_string_literal() {
    assert!(matches!(
        parse_statement("set session authorization 'tenant'").unwrap(),
        Statement::SetSessionAuthorization(SetSessionAuthorizationStatement { role_name })
            if role_name == "tenant"
    ));
}

#[test]
fn publication_parser_accepts_table_qualifiers_filters_and_columns() {
    let stmt = parse_statement("create publication pub for table only widgets(id) where (id > 0)")
        .unwrap();
    match stmt {
        Statement::CreatePublication(stmt) => {
            assert!(matches!(
                &stmt.target.objects[0],
                PublicationObjectSpec::Table(table)
                    if table.only
                        && table.relation_name == "widgets"
                        && table.column_names == vec!["id"]
                        && table.where_clause.as_deref() == Some("id > 0")
            ));
        }
        other => panic!("expected create publication, got {other:?}"),
    }
}

#[test]
fn publication_parser_reports_postgres_like_option_errors() {
    assert!(matches!(
        parse_statement("create publication pub with (foo)"),
        Err(ParseError::UnrecognizedPublicationParameter(name)) if name == "foo"
    ));
    assert!(matches!(
        parse_statement("create publication pub with (publish = 'cluster, vacuum')"),
        Err(ParseError::UnrecognizedPublicationOptionValue { option, value })
            if option == "publish" && value == "cluster"
    ));
    assert!(matches!(
        parse_statement("create publication pub with (publish_generated_columns = foo)"),
        Err(ParseError::InvalidPublicationParameterValue { parameter, value })
            if parameter == "publish_generated_columns" && value == "foo"
    ));
    assert!(matches!(
        parse_statement("create publication pub with (publish_generated_columns)"),
        Err(ParseError::InvalidPublicationParameterValue { parameter, value })
            if parameter == "publish_generated_columns" && value.is_empty()
    ));
    assert!(matches!(
        parse_statement("create publication pub with (publish_via_partition_root = true, publish_via_partition_root = false)"),
        Err(ParseError::ConflictingOrRedundantOptions { option })
            if option == "publish_via_partition_root"
    ));
}

#[test]
fn publication_parser_reports_invalid_mixed_object_names() {
    assert!(matches!(
        parse_statement(
            "create publication pub for table pub_test.widgets, current_schema"
        ),
        Err(ParseError::InvalidPublicationTableName(name)) if name == "current_schema"
    ));
    assert!(matches!(
        parse_statement("create publication pub for tables in schema foo, test.foo"),
        Err(ParseError::InvalidPublicationSchemaName(name)) if name == "test.foo"
    ));
}

#[test]
fn parse_publication_describe_queries() {
    parse_statement(
        "SELECT pubname AS \"Name\", \
         pg_catalog.pg_get_userbyid(pubowner) AS \"Owner\", \
         puballtables AS \"All tables\", \
         puballsequences AS \"All sequences\", \
         pubinsert AS \"Inserts\", \
         pubupdate AS \"Updates\", \
         pubdelete AS \"Deletes\", \
         pubtruncate AS \"Truncates\", \
         (CASE pubgencols WHEN 'n' THEN 'none' WHEN 's' THEN 'stored' END) AS \"Generated columns\", \
         pubviaroot AS \"Via root\" \
         FROM pg_catalog.pg_publication \
         ORDER BY 1",
    )
    .unwrap();

    parse_statement(
        "SELECT oid, pubname, \
         pg_catalog.pg_get_userbyid(pubowner) AS owner, \
         puballtables, puballsequences, pubinsert, pubupdate, pubdelete, pubtruncate, \
         (CASE pubgencols WHEN 'n' THEN 'none' WHEN 's' THEN 'stored' END) AS \"Generated columns\", \
         pubviaroot \
         FROM pg_catalog.pg_publication \
         WHERE pubname OPERATOR(pg_catalog.~) '^(pub)$' COLLATE pg_catalog.default \
         ORDER BY 2",
    )
    .unwrap();

    parse_statement(
        "SELECT pubname, NULL, NULL \
         FROM pg_catalog.pg_publication p \
              JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid \
              JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid \
         WHERE pc.oid = '1' AND pg_catalog.pg_relation_is_publishable('1') \
         UNION \
         SELECT pubname, pg_get_expr(pr.prqual, c.oid), \
                (CASE WHEN pr.prattrs IS NOT NULL THEN \
                    (SELECT string_agg(attname, ', ') \
                       FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s, \
                            pg_catalog.pg_attribute \
                      WHERE attrelid = pr.prrelid AND attnum = prattrs[s]) \
                 ELSE NULL END) \
         FROM pg_catalog.pg_publication p \
              JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid \
              JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid \
         WHERE pr.prrelid = '1' \
         UNION \
         SELECT pubname, NULL, NULL \
         FROM pg_catalog.pg_publication p \
         WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('1') \
         ORDER BY 1",
    )
    .unwrap();
}

#[test]
fn publication_describe_tokens_inside_literals_remain_literals() {
    let stmt = parse_statement(
        "select 'OPERATOR(pg_catalog.~)' as op, $$COLLATE pg_catalog.default$$ as coll",
    )
    .unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(stmt.targets[0].output_name, "op");
            assert_eq!(
                stmt.targets[0].expr,
                SqlExpr::Const(Value::Text("OPERATOR(pg_catalog.~)".into()))
            );
            assert_eq!(stmt.targets[1].output_name, "coll");
            assert_eq!(
                stmt.targets[1].expr,
                SqlExpr::Const(Value::Text("COLLATE pg_catalog.default".into()))
            );
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn publication_describe_tokens_inside_quoted_identifiers_remain_identifiers() {
    let stmt = parse_statement(
        "select 1 as \"OPERATOR(pg_catalog.~)\", 2 as \"COLLATE pg_catalog.default\"",
    )
    .unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(stmt.targets[0].output_name, "OPERATOR(pg_catalog.~)");
            assert_eq!(stmt.targets[1].output_name, "COLLATE pg_catalog.default");
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_select_with_collate_expression() {
    let stmt = parse_select("select name collate \"C\" from people").unwrap();
    assert_eq!(stmt.targets[0].output_name, "name");
    assert_eq!(
        stmt.targets[0].expr,
        SqlExpr::Collate {
            expr: Box::new(SqlExpr::Column("name".into())),
            collation: "C".into(),
        }
    );
}

#[test]
fn parse_select_with_order_by_collate_and_ordinal() {
    let stmt =
        parse_select("select name, note from people order by name collate \"C\", 2").unwrap();
    assert_eq!(stmt.order_by.len(), 2);
    assert_eq!(
        stmt.order_by[0].expr,
        SqlExpr::Collate {
            expr: Box::new(SqlExpr::Column("name".into())),
            collation: "C".into(),
        }
    );
    assert_eq!(stmt.order_by[1].expr, SqlExpr::IntegerLiteral("2".into()));
}

#[test]
fn parse_select_with_default_collation_keeps_raw_ast() {
    let stmt = parse_select("select name collate pg_catalog.default from people").unwrap();
    assert_eq!(
        stmt.targets[0].expr,
        SqlExpr::Collate {
            expr: Box::new(SqlExpr::Column("name".into())),
            collation: "pg_catalog.default".into(),
        }
    );
    let stmt = parse_select("select 1 collate default").unwrap();
    assert_eq!(
        stmt.targets[0].expr,
        SqlExpr::Collate {
            expr: Box::new(SqlExpr::IntegerLiteral("1".into())),
            collation: "default".into(),
        }
    );
}

fn is_outer_user_var(expr: &Expr, index: usize) -> bool {
    match expr {
        Expr::Var(Var {
            varattno,
            varlevelsup: 0,
            ..
        }) => *varattno == (index + 1) as AttrNumber && !is_system_attr(*varattno),
        Expr::Coalesce(left, right) => {
            is_outer_user_var(left, index) && is_outer_user_var(right, index)
        }
        _ => false,
    }
}

fn plain_inference_target(columns: &[&str]) -> OnConflictTarget {
    OnConflictTarget::Inference(OnConflictInferenceSpec {
        elements: columns
            .iter()
            .map(|column| OnConflictInferenceElem {
                expr: SqlExpr::Column((*column).into()),
                collation: None,
                opclass: None,
            })
            .collect(),
        predicate: None,
    })
}

fn inference_target_with_predicate(columns: &[&str], predicate: SqlExpr) -> OnConflictTarget {
    OnConflictTarget::Inference(OnConflictInferenceSpec {
        elements: columns
            .iter()
            .map(|column| OnConflictInferenceElem {
                expr: SqlExpr::Column((*column).into()),
                collation: None,
                opclass: None,
            })
            .collect(),
        predicate: Some(predicate),
    })
}

fn inference_column_names(target: &OnConflictTarget) -> Option<Vec<String>> {
    match target {
        OnConflictTarget::Inference(spec) if spec.predicate.is_none() => spec
            .elements
            .iter()
            .map(
                |element| match (&element.expr, &element.collation, &element.opclass) {
                    (SqlExpr::Column(name), None, None) => Some(name.clone()),
                    _ => None,
                },
            )
            .collect(),
        _ => None,
    }
}

#[derive(Default)]
struct TypeOnlyCatalog {
    types: Vec<PgTypeRow>,
}

impl CatalogLookup for TypeOnlyCatalog {
    fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
        None
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let mut rows = crate::include::catalog::builtin_type_rows();
        rows.extend(self.types.clone());
        rows
    }
}

fn assert_alias_names(spec: &AliasColumnSpec, expected: &[&str]) {
    assert_eq!(
        spec,
        &AliasColumnSpec::Names(expected.iter().map(|name| (*name).to_string()).collect())
    );
}

fn test_catalog_entry(rel_number: u32, desc: RelationDesc) -> CatalogEntry {
    CatalogEntry {
        rel: crate::RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number,
        },
        relation_oid: 50_000u32.saturating_add(rel_number),
        namespace_oid: 11,
        owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        relacl: None,
        reloptions: None,
        of_type_oid: 0,
        row_type_oid: 60_000u32.saturating_add(rel_number),
        array_type_oid: 61_000u32.saturating_add(rel_number),
        reltoastrelid: 0,
        relhasindex: false,
        relpersistence: 'p',
        relkind: 'r',
        relispopulated: true,
        am_oid: crate::include::catalog::relam_for_relkind('r'),
        relhastriggers: false,
        relhassubclass: false,
        relispartition: false,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc,
        partitioned_table: None,
        index_meta: None,
    }
}

fn pets_entry() -> CatalogEntry {
    test_catalog_entry(
        15001,
        RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("owner_id", SqlType::new(SqlTypeKind::Int4), false),
            ],
        },
    )
}

fn jpop_entry() -> CatalogEntry {
    test_catalog_entry(
        15002,
        RelationDesc {
            columns: vec![
                column_desc("a", SqlType::new(SqlTypeKind::Text), true),
                column_desc("b", SqlType::new(SqlTypeKind::Int4), true),
                column_desc("c", SqlType::new(SqlTypeKind::Timestamp), true),
            ],
        },
    )
}

fn people_view_entry() -> CatalogEntry {
    CatalogEntry {
        rel: crate::RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 15020,
        },
        relation_oid: 50020,
        namespace_oid: 11,
        owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        relacl: None,
        reloptions: None,
        of_type_oid: 0,
        row_type_oid: 60020,
        array_type_oid: 60021,
        reltoastrelid: 0,
        relhasindex: false,
        relpersistence: 'p',
        relkind: 'v',
        relispopulated: true,
        am_oid: crate::include::catalog::relam_for_relkind('v'),
        relhastriggers: false,
        relhassubclass: false,
        relispartition: false,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc: RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("name", SqlType::new(SqlTypeKind::Text), false),
            ],
        },
        partitioned_table: None,
        index_meta: None,
    }
}

fn catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert("people", test_catalog_entry(15000, desc()));
    catalog
}

fn catalog_with_operator_dispatch_table() -> Catalog {
    let mut catalog = catalog();
    catalog.insert(
        "ops",
        test_catalog_entry(
            15003,
            RelationDesc {
                columns: vec![
                    column_desc("left_box", SqlType::new(SqlTypeKind::Box), false),
                    column_desc("right_box", SqlType::new(SqlTypeKind::Box), false),
                    column_desc("left_range", SqlType::new(SqlTypeKind::Int4Range), false),
                    column_desc("right_range", SqlType::new(SqlTypeKind::Int4Range), false),
                ],
            },
        ),
    );
    catalog
}

fn catalog_with_pets() -> Catalog {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    catalog
}

fn catalog_with_jpop() -> Catalog {
    let mut catalog = catalog();
    catalog.insert("jpop", jpop_entry());
    catalog
}

struct RowSecurityTestCatalog {
    base: Catalog,
    current_user_oid: u32,
    authid_rows: Vec<PgAuthIdRow>,
}

impl CatalogLookup for RowSecurityTestCatalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.base.lookup_any_relation(name)
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.base.lookup_relation_by_oid(relation_oid)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.base.relation_by_oid(relation_oid)
    }

    fn current_user_oid(&self) -> u32 {
        self.current_user_oid
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.authid_rows.clone()
    }

    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        self.base.auth_members_rows().to_vec()
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        self.base.rewrite_rows_for_relation(relation_oid).to_vec()
    }

    fn policy_rows_for_relation(&self, relation_oid: u32) -> Vec<PgPolicyRow> {
        self.base.policy_rows_for_relation(relation_oid).to_vec()
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        self.base.class_row_by_oid(relation_oid)
    }
}

fn row_security_test_catalog(base: Catalog, current_user_oid: u32) -> RowSecurityTestCatalog {
    let mut authid_rows = base.authid_rows().to_vec();
    authid_rows.push(PgAuthIdRow {
        oid: current_user_oid,
        rolname: "app_user".into(),
        rolsuper: false,
        rolinherit: true,
        rolcreaterole: false,
        rolcreatedb: false,
        rolcanlogin: true,
        rolreplication: false,
        rolbypassrls: false,
        rolconnlimit: -1,
        rolpassword: None,
        rolvaliduntil: None,
    });
    authid_rows.sort_by_key(|row| row.oid);
    RowSecurityTestCatalog {
        base,
        current_user_oid,
        authid_rows,
    }
}

fn row_security_entry(rel_number: u32, desc: RelationDesc) -> CatalogEntry {
    let mut entry = test_catalog_entry(rel_number, desc);
    entry.relrowsecurity = true;
    entry
}

struct OverrideFunctionCatalog {
    base: Catalog,
    proc_rows: Vec<PgProcRow>,
}

impl CatalogLookup for OverrideFunctionCatalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.base.lookup_any_relation(name)
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.base.lookup_relation_by_oid(relation_oid)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        let normalized = name.strip_prefix("pg_catalog.").unwrap_or(name);
        let matches = self
            .proc_rows
            .iter()
            .filter(|row| row.proname.eq_ignore_ascii_case(normalized))
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            self.base.proc_rows_by_name(name)
        } else {
            matches
        }
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        self.base.type_rows()
    }
}

fn json_each_proc_row() -> PgProcRow {
    bootstrap_pg_proc_rows()
        .into_iter()
        .find(|row| row.proname == "json_each" && row.proargtypes == JSON_TYPE_OID.to_string())
        .expect("json_each row")
}

fn query_column_names_and_types(query: &Query) -> Vec<(String, SqlType)> {
    query
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.sql_type))
        .collect()
}

fn relation_row_type_oid(catalog: &Catalog, name: &str) -> u32 {
    let relation = catalog.lookup_any_relation(name).expect("relation");
    catalog
        .type_rows()
        .into_iter()
        .find(|row| row.typrelid == relation.relation_oid)
        .map(|row| row.oid)
        .expect("row type")
}

fn catalog_with_people_id_index() -> Catalog {
    let mut catalog = catalog();
    catalog.insert(
        "people_id_idx",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15010,
            },
            relation_oid: 50010,
            namespace_oid: 11,
            owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60010,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: 65000,
                indisunique: false,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![1],
                indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID],
                indclass_options: vec![Vec::new()],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: None,
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    catalog
}

fn catalog_with_people_primary_key() -> Catalog {
    catalog_with_people_primary_key_opclass(crate::include::catalog::INT4_BTREE_OPCLASS_OID)
}

fn catalog_with_people_primary_key_opclass(opclass_oid: u32) -> Catalog {
    let mut catalog = catalog();
    catalog.insert(
        "people_pkey",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15011,
            },
            relation_oid: 50011,
            namespace_oid: 11,
            owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60011,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: 65000,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: true,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![1],
                indclass: vec![opclass_oid],
                indclass_options: vec![Vec::new()],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: None,
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    catalog
        .create_index_backed_constraint(65000, 50011, "people_pkey", CONSTRAINT_PRIMARY, &[])
        .unwrap();
    catalog
}

fn add_ready_people_index(
    catalog: &mut Catalog,
    index_name: &str,
    unique: bool,
    primary: bool,
    columns: &[IndexColumnDef],
) -> CatalogEntry {
    let relation_oid = catalog.lookup_any_relation("people").unwrap().relation_oid;
    let entry = catalog
        .create_index_for_relation_with_flags(index_name, relation_oid, unique, primary, columns)
        .unwrap();
    catalog
        .set_index_ready_valid(entry.relation_oid, true, true)
        .unwrap();
    catalog.get(index_name).cloned().unwrap()
}

fn catalog_with_people_id_name_unique_index() -> Catalog {
    let mut catalog = catalog_with_people_primary_key();
    add_ready_people_index(
        &mut catalog,
        "people_id_name_key",
        true,
        false,
        &[IndexColumnDef::from("id"), IndexColumnDef::from("name")],
    );
    catalog
}

fn catalog_with_people_name_unique_constraint() -> Catalog {
    let mut catalog = catalog();
    let relation_oid = catalog.lookup_any_relation("people").unwrap().relation_oid;
    let index = add_ready_people_index(
        &mut catalog,
        "people_name_key",
        true,
        false,
        &[IndexColumnDef::from("name")],
    );
    catalog
        .create_index_backed_constraint(
            relation_oid,
            index.relation_oid,
            "people_name_key",
            CONSTRAINT_UNIQUE,
            &[],
        )
        .unwrap();
    catalog
}

fn catalog_with_memberships_composite_primary_key() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        "memberships",
        test_catalog_entry(
            15020,
            RelationDesc {
                columns: vec![
                    column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                    column_desc("tag", SqlType::new(SqlTypeKind::Int4), false),
                    column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                ],
            },
        ),
    );
    let relation_oid = catalog
        .lookup_any_relation("memberships")
        .unwrap()
        .relation_oid;
    let index = catalog
        .create_index_for_relation_with_flags(
            "memberships_pkey",
            relation_oid,
            true,
            true,
            &[IndexColumnDef::from("id"), IndexColumnDef::from("tag")],
        )
        .unwrap();
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .unwrap();
    catalog
        .create_index_backed_constraint(
            relation_oid,
            index.relation_oid,
            "memberships_pkey",
            CONSTRAINT_PRIMARY,
            &[],
        )
        .unwrap();
    catalog
}

fn catalog_with_products_primary_key_and_sales() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        "products",
        test_catalog_entry(
            15021,
            RelationDesc {
                columns: vec![
                    column_desc("product_id", SqlType::new(SqlTypeKind::Int4), false),
                    column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    column_desc("price", SqlType::new(SqlTypeKind::Int4), true),
                ],
            },
        ),
    );
    catalog.insert(
        "sales",
        test_catalog_entry(
            15022,
            RelationDesc {
                columns: vec![
                    column_desc("product_id", SqlType::new(SqlTypeKind::Int4), true),
                    column_desc("units", SqlType::new(SqlTypeKind::Int4), true),
                ],
            },
        ),
    );
    let relation_oid = catalog
        .lookup_any_relation("products")
        .unwrap()
        .relation_oid;
    let index = catalog
        .create_index_for_relation_with_flags(
            "products_pkey",
            relation_oid,
            true,
            true,
            &[IndexColumnDef::from("product_id")],
        )
        .unwrap();
    catalog
        .set_index_ready_valid(index.relation_oid, true, true)
        .unwrap();
    catalog
        .create_index_backed_constraint(
            relation_oid,
            index.relation_oid,
            "products_pkey",
            CONSTRAINT_PRIMARY,
            &[],
        )
        .unwrap();
    catalog
}

fn catalog_with_people_partial_unique_index() -> Catalog {
    let mut catalog = catalog();
    let people = catalog.lookup_any_relation("people").unwrap();
    catalog.insert(
        "people_partial_key",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15013,
            },
            relation_oid: 50013,
            namespace_oid: 11,
            owner_oid: BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60013,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: people.relation_oid,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![1],
                indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID],
                indclass_options: vec![Vec::new()],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: Some("(id > 0)".into()),
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    catalog
}

fn catalog_with_people_ctid_partial_unique_index() -> Catalog {
    let mut catalog = catalog();
    let people = catalog.lookup_any_relation("people").unwrap();
    catalog.insert(
        "people_ctid_partial_key",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15015,
            },
            relation_oid: 50015,
            namespace_oid: 11,
            owner_oid: BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60015,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: people.relation_oid,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![1],
                indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID],
                indclass_options: vec![Vec::new()],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: Some("ctid >= '(1000,0)'".into()),
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    catalog
}

fn catalog_with_people_expression_unique_index() -> Catalog {
    let mut catalog = catalog();
    let people = catalog.lookup_any_relation("people").unwrap();
    catalog.insert(
        "people_lower_name_key",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15014,
            },
            relation_oid: 50014,
            namespace_oid: 11,
            owner_oid: BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60014,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("lower", SqlType::new(SqlTypeKind::Text), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: people.relation_oid,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![0],
                indclass: vec![crate::include::catalog::TEXT_BTREE_OPCLASS_OID],
                indclass_options: vec![Vec::new()],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: Some(serde_json::to_string(&vec!["lower(name)"]).unwrap()),
                indpred: None,
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    catalog
}

struct PanicIndexDiscoveryCatalog<'a> {
    inner: &'a Catalog,
}

impl CatalogLookup for PanicIndexDiscoveryCatalog<'_> {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.inner.lookup_any_relation(name)
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.inner.lookup_relation_by_oid(relation_oid)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.inner.relation_by_oid(relation_oid)
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        self.inner.namespace_row_by_oid(oid)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        self.inner.proc_rows_by_name(name)
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        self.inner.proc_row_by_oid(oid)
    }

    fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        self.inner.opclass_rows()
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        self.inner.collation_rows()
    }

    fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<PgAggregateRow> {
        self.inner.aggregate_by_fnoid(aggfnoid)
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        self.inner
            .operator_by_name_left_right(name, left_type_oid, right_type_oid)
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        self.inner.operator_by_oid(oid)
    }

    fn cast_by_source_target(
        &self,
        source_type_oid: u32,
        target_type_oid: u32,
    ) -> Option<PgCastRow> {
        self.inner
            .cast_by_source_target(source_type_oid, target_type_oid)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        self.inner.type_rows()
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        self.inner.type_by_oid(oid)
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        self.inner.type_by_name(name)
    }

    fn range_rows(&self) -> Vec<PgRangeRow> {
        self.inner.range_rows()
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        self.inner.type_oid_for_sql_type(sql_type)
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        self.inner.language_rows()
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        self.inner.language_row_by_oid(oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        self.inner.language_row_by_name(name)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        self.inner.class_row_by_oid(relation_oid)
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        panic!("index expression binding discovered indexes for relation {relation_oid}");
    }
}

#[test]
fn bind_expression_index_metadata_does_not_discover_heap_indexes() {
    let mut catalog = Catalog::default();
    catalog.insert(
        "expr_items",
        test_catalog_entry(
            15040,
            RelationDesc {
                columns: vec![column_desc("a", SqlType::new(SqlTypeKind::Int4), false)],
            },
        ),
    );
    let heap = catalog.lookup_any_relation("expr_items").unwrap();
    catalog.insert(
        "expr_items_a_square_idx",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15041,
            },
            relation_oid: 50041,
            namespace_oid: 11,
            owner_oid: BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60041,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("expr", SqlType::new(SqlTypeKind::Int4), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: heap.relation_oid,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![0],
                indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID],
                indclass_options: vec![Vec::new()],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: Some(serde_json::to_string(&vec!["a * a"]).unwrap()),
                indpred: None,
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    let relcache = crate::backend::utils::cache::relcache::RelCache::from_catalog(&catalog);
    let mut index_meta = relcache
        .get_by_name("expr_items_a_square_idx")
        .and_then(|entry| entry.index.clone())
        .unwrap();
    let wrapper = PanicIndexDiscoveryCatalog { inner: &catalog };
    let exprs = relation_get_index_expressions(&mut index_meta, &heap.desc, &wrapper).unwrap();
    assert_eq!(exprs.len(), 1);
    assert!(index_meta.rd_indexprs.is_some());
}

fn catalog_with_people_name_c_collation_index() -> Catalog {
    let mut catalog = catalog();
    let people = catalog.lookup_any_relation("people").unwrap();
    catalog.insert(
        "people_name_c_key",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15015,
            },
            relation_oid: 50015,
            namespace_oid: 11,
            owner_oid: BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60015,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("name", SqlType::new(SqlTypeKind::Text), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: people.relation_oid,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![2],
                indclass: vec![crate::include::catalog::TEXT_BTREE_OPCLASS_OID],
                indclass_options: vec![Vec::new()],
                indcollation: vec![crate::include::catalog::C_COLLATION_OID],
                indoption: vec![0],
                indexprs: None,
                indpred: None,
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    catalog
}

fn catalog_with_text_parent_primary_key() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        "labels",
        test_catalog_entry(
            15030,
            RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Text), false)],
            },
        ),
    );
    catalog.insert(
        "labels_pkey",
        CatalogEntry {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15031,
            },
            relation_oid: 50031,
            namespace_oid: 11,
            owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid: 60031,
            array_type_oid: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'i',
            relispopulated: true,
            am_oid: crate::include::catalog::BTREE_AM_OID,
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Text), false)],
            },
            partitioned_table: None,
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: 65030,
                indisunique: true,
                indnullsnotdistinct: false,
                indisprimary: true,
                indisexclusion: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indimmediate: true,
                indkey: vec![1],
                indclass: vec![crate::include::catalog::TEXT_BTREE_OPCLASS_OID],
                indclass_options: vec![Vec::new()],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: None,
                btree_options: None,
                brin_options: None,
                gin_options: None,
                hash_options: None,
            }),
        },
    );
    catalog
        .create_index_backed_constraint(65030, 50031, "labels_pkey", CONSTRAINT_PRIMARY, &[])
        .unwrap();
    catalog
}

fn visible_catalog_without_text_input_cast(
    target_oid: u32,
) -> crate::backend::utils::cache::visible_catalog::VisibleCatalog {
    let catalog = catalog();
    let relcache = crate::backend::utils::cache::relcache::RelCache::from_catalog(&catalog);
    let base = crate::backend::utils::cache::catcache::CatCache::from_catalog(&catalog);
    let filtered = crate::backend::utils::cache::catcache::CatCache::from_rows(
        base.namespace_rows(),
        base.class_rows(),
        base.attribute_rows(),
        base.attrdef_rows(),
        base.depend_rows(),
        base.inherit_rows(),
        base.index_rows(),
        base.rewrite_rows(),
        base.trigger_rows(),
        base.policy_rows(),
        base.publication_rows(),
        base.publication_rel_rows(),
        base.publication_namespace_rows(),
        base.statistic_ext_rows(),
        base.statistic_ext_data_rows(),
        base.am_rows(),
        base.amop_rows(),
        base.amproc_rows(),
        base.authid_rows(),
        base.auth_members_rows(),
        base.language_rows(),
        base.ts_parser_rows(),
        base.ts_template_rows(),
        base.ts_dict_rows(),
        base.ts_config_rows(),
        base.ts_config_map_rows(),
        base.constraint_rows(),
        base.operator_rows(),
        base.opclass_rows(),
        base.opfamily_rows(),
        base.partitioned_table_rows(),
        base.proc_rows(),
        base.aggregate_rows(),
        base.cast_rows()
            .into_iter()
            .filter(|row| {
                !(row.castsource == crate::include::catalog::TEXT_TYPE_OID
                    && row.casttarget == target_oid
                    && row.castmethod == 'i')
            })
            .collect(),
        base.conversion_rows(),
        base.collation_rows(),
        base.foreign_data_wrapper_rows(),
        base.foreign_server_rows(),
        base.foreign_table_rows(),
        base.user_mapping_rows(),
        base.database_rows(),
        base.tablespace_rows(),
        base.statistic_rows(),
        base.type_rows(),
    );
    crate::backend::utils::cache::visible_catalog::VisibleCatalog::new(relcache, Some(filtered))
}

fn visible_catalog_without_operator(
    name: &str,
    left_oid: u32,
    right_oid: u32,
) -> crate::backend::utils::cache::visible_catalog::VisibleCatalog {
    let catalog = catalog();
    let relcache = crate::backend::utils::cache::relcache::RelCache::from_catalog(&catalog);
    let base = crate::backend::utils::cache::catcache::CatCache::from_catalog(&catalog);
    let filtered = crate::backend::utils::cache::catcache::CatCache::from_rows(
        base.namespace_rows(),
        base.class_rows(),
        base.attribute_rows(),
        base.attrdef_rows(),
        base.depend_rows(),
        base.inherit_rows(),
        base.index_rows(),
        base.rewrite_rows(),
        base.trigger_rows(),
        base.policy_rows(),
        base.publication_rows(),
        base.publication_rel_rows(),
        base.publication_namespace_rows(),
        base.statistic_ext_rows(),
        base.statistic_ext_data_rows(),
        base.am_rows(),
        base.amop_rows(),
        base.amproc_rows(),
        base.authid_rows(),
        base.auth_members_rows(),
        base.language_rows(),
        base.ts_parser_rows(),
        base.ts_template_rows(),
        base.ts_dict_rows(),
        base.ts_config_rows(),
        base.ts_config_map_rows(),
        base.constraint_rows(),
        base.operator_rows()
            .into_iter()
            .filter(|row| {
                !(row.oprname == name && row.oprleft == left_oid && row.oprright == right_oid)
            })
            .collect(),
        base.opclass_rows(),
        base.opfamily_rows(),
        base.partitioned_table_rows(),
        base.proc_rows(),
        base.aggregate_rows(),
        base.cast_rows(),
        base.conversion_rows(),
        base.collation_rows(),
        base.foreign_data_wrapper_rows(),
        base.foreign_server_rows(),
        base.foreign_table_rows(),
        base.user_mapping_rows(),
        base.database_rows(),
        base.tablespace_rows(),
        base.statistic_rows(),
        base.type_rows(),
    );
    crate::backend::utils::cache::visible_catalog::VisibleCatalog::new(relcache, Some(filtered))
}

fn custom_btree_opclass(
    oid: u32,
    name: &str,
    family_oid: u32,
    input_type_oid: u32,
) -> crate::include::catalog::PgOpclassRow {
    crate::include::catalog::PgOpclassRow {
        oid,
        opcmethod: crate::include::catalog::BTREE_AM_OID,
        opcname: name.into(),
        opcnamespace: crate::include::catalog::PG_CATALOG_NAMESPACE_OID,
        opcowner: BOOTSTRAP_SUPERUSER_OID,
        opcfamily: family_oid,
        opcintype: input_type_oid,
        opcdefault: false,
        opckeytype: 0,
    }
}

fn visible_catalog_with_extra_opclasses(
    catalog: &Catalog,
    extra_opclasses: Vec<crate::include::catalog::PgOpclassRow>,
) -> crate::backend::utils::cache::visible_catalog::VisibleCatalog {
    let relcache = crate::backend::utils::cache::relcache::RelCache::from_catalog(catalog);
    let base = crate::backend::utils::cache::catcache::CatCache::from_catalog(catalog);
    let mut opclass_rows = base.opclass_rows();
    opclass_rows.extend(extra_opclasses);
    let filtered = crate::backend::utils::cache::catcache::CatCache::from_rows(
        base.namespace_rows(),
        base.class_rows(),
        base.attribute_rows(),
        base.attrdef_rows(),
        base.depend_rows(),
        base.inherit_rows(),
        base.index_rows(),
        base.rewrite_rows(),
        base.trigger_rows(),
        base.policy_rows(),
        base.publication_rows(),
        base.publication_rel_rows(),
        base.publication_namespace_rows(),
        base.statistic_ext_rows(),
        base.statistic_ext_data_rows(),
        base.am_rows(),
        base.amop_rows(),
        base.amproc_rows(),
        base.authid_rows(),
        base.auth_members_rows(),
        base.language_rows(),
        base.ts_parser_rows(),
        base.ts_template_rows(),
        base.ts_dict_rows(),
        base.ts_config_rows(),
        base.ts_config_map_rows(),
        base.constraint_rows(),
        base.operator_rows(),
        opclass_rows,
        base.opfamily_rows(),
        base.partitioned_table_rows(),
        base.proc_rows(),
        base.aggregate_rows(),
        base.cast_rows(),
        base.conversion_rows(),
        base.collation_rows(),
        base.foreign_data_wrapper_rows(),
        base.foreign_server_rows(),
        base.foreign_table_rows(),
        base.user_mapping_rows(),
        base.database_rows(),
        base.tablespace_rows(),
        base.statistic_rows(),
        base.type_rows(),
    );
    crate::backend::utils::cache::visible_catalog::VisibleCatalog::new(relcache, Some(filtered))
}

fn strip_projections(plan: &Plan) -> &Plan {
    let mut current = plan;
    while let Plan::Projection { input, .. } = current {
        current = input;
    }
    current
}

#[test]
fn pest_matches_basic_select_keyword() {
    let result = gram::pest_parse_keyword(gram::Rule::kw_select_atom, "select").unwrap();
    assert_eq!(result, "select");
}

#[test]
fn analyze_join_using_creates_join_rte_alias_vars() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());

    let stmt = parse_select("select id from people join pets using (id)").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();

    assert_eq!(query.rtable.len(), 3);
    match &query.jointree {
        Some(JoinTreeNode::JoinExpr { rtindex, .. }) => assert_eq!(*rtindex, 3),
        other => panic!("expected join jointree, got {other:?}"),
    }
    match &query.rtable[2].kind {
        RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            joinaliasvars,
            joinleftcols,
            joinrightcols,
        } => {
            assert_eq!(*jointype, JoinType::Inner);
            assert_eq!(*joinmergedcols, 1);
            assert_eq!(joinleftcols, &vec![1, 2, 3]);
            assert_eq!(joinrightcols, &vec![1, 2]);
            match &joinaliasvars[0] {
                Expr::Coalesce(left, right) => {
                    assert_eq!(
                        left.as_ref(),
                        &Expr::Var(Var {
                            varno: 1,
                            varattno: 1,
                            varlevelsup: 0,
                            vartype: SqlType::new(SqlTypeKind::Int4),
                        })
                    );
                    assert_eq!(
                        right.as_ref(),
                        &Expr::Var(Var {
                            varno: 2,
                            varattno: 1,
                            varlevelsup: 0,
                            vartype: SqlType::new(SqlTypeKind::Int4),
                        })
                    );
                }
                other => panic!("expected merged join alias expr, got {other:?}"),
            }
        }
        other => panic!("expected join rte, got {other:?}"),
    }
    assert_eq!(
        query.target_list[0].expr,
        Expr::Var(Var {
            varno: 3,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Int4),
        })
    );
}

#[test]
fn analyze_from_alias_updates_rte_alias() {
    let stmt = parse_select("select t.id from people t").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert_eq!(query.rtable.len(), 1);
    assert_eq!(query.rtable[0].alias.as_deref(), Some("t"));
}

#[test]
fn rewrite_query_expands_view_relation_rtes() {
    let mut catalog = catalog();
    let view = people_view_entry();
    catalog.insert("people_view", view.clone());
    catalog.rewrites.push(PgRewriteRow {
        oid: 70000,
        rulename: "_RETURN".into(),
        ev_class: view.relation_oid,
        ev_type: '1',
        ev_enabled: 'O',
        is_instead: true,
        ev_qual: String::new(),
        ev_action: "select id, name from people".into(),
    });
    sort_pg_rewrite_rows(&mut catalog.rewrites);

    let stmt = parse_select("select id from people_view").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();
    assert!(matches!(
        query.rtable[0].kind,
        RangeTblEntryKind::Relation { relkind: 'v', .. }
    ));

    let rewritten = crate::backend::rewrite::pg_rewrite_query(query, &catalog)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(matches!(
        rewritten.rtable[0].kind,
        RangeTblEntryKind::Subquery { .. }
    ));
}

#[test]
fn rewrite_query_expands_view_relation_rtes_inside_set_operations() {
    let mut catalog = catalog();
    let view = people_view_entry();
    catalog.insert("people_view", view.clone());
    catalog.rewrites.push(PgRewriteRow {
        oid: 70000,
        rulename: "_RETURN".into(),
        ev_class: view.relation_oid,
        ev_type: '1',
        ev_enabled: 'O',
        is_instead: true,
        ev_qual: String::new(),
        ev_action: "select id, name from people".into(),
    });
    sort_pg_rewrite_rows(&mut catalog.rewrites);

    let stmt = parse_select("select name from people union select name from people_view").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();
    let set_operation = query.set_operation.as_ref().expect("set operation");
    assert!(matches!(
        set_operation.inputs[1].rtable[0].kind,
        RangeTblEntryKind::Relation { relkind: 'v', .. }
    ));

    let rewritten = crate::backend::rewrite::pg_rewrite_query(query, &catalog)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let set_operation = rewritten.set_operation.as_ref().expect("set operation");
    assert!(matches!(
        set_operation.inputs[1].rtable[0].kind,
        RangeTblEntryKind::Subquery { .. }
    ));
}

#[test]
fn rewrite_policy_subqueries_apply_nested_row_security() {
    let mut base = Catalog::default();
    base.insert(
        "outer_t",
        row_security_entry(
            15100,
            RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
        ),
    );
    base.insert(
        "inner_t",
        row_security_entry(
            15101,
            RelationDesc {
                columns: vec![
                    column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                    column_desc("owner_id", SqlType::new(SqlTypeKind::Int4), false),
                ],
            },
        ),
    );
    let outer_oid = base.get("outer_t").unwrap().relation_oid;
    let inner_oid = base.get("inner_t").unwrap().relation_oid;
    base.add_policy_row(PgPolicyRow {
        oid: 71000,
        polname: "outer_exists_inner".into(),
        polrelid: outer_oid,
        polcmd: PolicyCommand::Select,
        polpermissive: true,
        polroles: vec![0],
        polqual: Some("exists (select 1 from inner_t where inner_t.owner_id = outer_t.id)".into()),
        polwithcheck: None,
    });
    base.add_policy_row(PgPolicyRow {
        oid: 71001,
        polname: "inner_visible".into(),
        polrelid: inner_oid,
        polcmd: PolicyCommand::Select,
        polpermissive: true,
        polroles: vec![0],
        polqual: Some("owner_id = 7".into()),
        polwithcheck: None,
    });
    let catalog = row_security_test_catalog(base, 71010);

    let stmt = parse_select("select id from outer_t").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();
    let rewritten = crate::backend::rewrite::pg_rewrite_query(query, &catalog)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert!(rewritten.depends_on_row_security);
    let outer_rte = &rewritten.rtable[0];
    let outer_security_qual = outer_rte
        .security_quals
        .first()
        .expect("outer table should have a rewritten policy qual");
    let subquery = match outer_security_qual {
        Expr::SubLink(sublink) => &sublink.subselect,
        other => panic!("expected policy EXISTS subquery, got {other:?}"),
    };
    assert!(subquery.depends_on_row_security);
    let inner_rte = &subquery.rtable[0];
    match &inner_rte.kind {
        RangeTblEntryKind::Relation { relation_oid, .. } => assert_eq!(*relation_oid, inner_oid),
        other => panic!("expected inner policy subquery relation RTE, got {other:?}"),
    }
    assert!(
        !inner_rte.security_quals.is_empty(),
        "inner policy subquery should inherit its own row-security quals"
    );
}

#[test]
fn rewrite_policy_subqueries_reject_infinite_policy_recursion() {
    let mut base = Catalog::default();
    base.insert(
        "rtbl",
        row_security_entry(
            15102,
            RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
        ),
    );
    let relation_oid = base.get("rtbl").unwrap().relation_oid;
    base.add_policy_row(PgPolicyRow {
        oid: 71002,
        polname: "recursive_policy".into(),
        polrelid: relation_oid,
        polcmd: PolicyCommand::Select,
        polpermissive: true,
        polroles: vec![0],
        polqual: Some("exists (select 1 from rtbl inner_rtbl)".into()),
        polwithcheck: None,
    });
    let catalog = row_security_test_catalog(base, 71011);

    let stmt = parse_select("select id from rtbl").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();
    let err = crate::backend::rewrite::pg_rewrite_query(query, &catalog).unwrap_err();
    assert!(
        matches!(
            &err,
            ParseError::DetailedError {
                sqlstate,
                message,
                ..
            } if *sqlstate == "42P17"
                && message == "infinite recursion detected in policy for relation \"rtbl\""
        ),
        "unexpected recursion error: {err:?}"
    );
}

#[test]
fn pest_matches_minimal_select_statement() {
    let stmt = parse_statement("select id from people").unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(
                stmt.from,
                Some(FromItem::Table {
                    name: "people".into(),
                    only: false,
                })
            );
            assert_eq!(stmt.targets.len(), 1);
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_select_table_star_as_inherited_table_reference() {
    let stmt = parse_statement("select id from people*").unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(
                stmt.from,
                Some(FromItem::Table {
                    name: "people".into(),
                    only: false,
                })
            );
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_set_statement() {
    let stmt = parse_statement("set extra_float_digits = 0").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "extra_float_digits".into(),
            value: Some("0".into()),
            is_local: false,
        })
    );
}

#[test]
fn parse_set_statement_to_default() {
    let stmt = parse_statement("set enable_bitmapscan to default").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "enable_bitmapscan".into(),
            value: None,
            is_local: false,
        })
    );

    let stmt = parse_statement("set application_name to 'default'").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "application_name".into(),
            value: Some("default".into()),
            is_local: false,
        })
    );
}

#[test]
fn parse_set_constraints_statement() {
    let stmt = parse_statement("set constraints all deferred").unwrap();
    assert!(matches!(
        stmt,
        Statement::SetConstraints(stmt) if stmt.constraints.is_none() && stmt.deferred
    ));

    let stmt =
        parse_statement("set constraints public.items_pkey, items_code_key immediate").unwrap();
    let Statement::SetConstraints(stmt) = stmt else {
        panic!("expected set constraints");
    };
    assert!(!stmt.deferred);
    let constraints = stmt.constraints.expect("named constraints");
    assert_eq!(constraints.len(), 2);
    assert_eq!(constraints[0].schema_name.as_deref(), Some("public"));
    assert_eq!(constraints[0].name, "items_pkey");
    assert_eq!(constraints[1].schema_name, None);
    assert_eq!(constraints[1].name, "items_code_key");
}

#[test]
fn parse_set_xml_option_statement() {
    let stmt = parse_statement("set xml option document").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "xmloption".into(),
            value: Some("DOCUMENT".into()),
            is_local: false,
        })
    );
}

#[test]
fn parse_xmlelement_expression() {
    let stmt =
        parse_statement("select xmlelement(name employee, xmlattributes(1 as id), 'ok')").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    let SqlExpr::Xml(xml) = &select.targets[0].expr else {
        panic!("expected XML expression");
    };
    assert_eq!(xml.name.as_deref(), Some("employee"));
    assert_eq!(xml.named_args.len(), 1);
    assert_eq!(xml.arg_names, vec!["id"]);
    assert_eq!(xml.args.len(), 1);
    assert_eq!(select.targets[0].output_name, "xmlelement");
}

#[test]
fn parse_xmlserialize_expression() {
    let stmt = parse_statement("select xmlserialize(document '<a/>' as text no indent)").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert!(matches!(select.targets[0].expr, SqlExpr::Xml(_)));
}

#[test]
fn parse_xmlagg_aggregate() {
    let stmt = parse_statement("select xmlagg(x) from t").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert!(matches!(
        select.targets[0].expr,
        SqlExpr::FuncCall { ref name, .. } if name == "xmlagg"
    ));
}

#[test]
fn parse_is_document_expression() {
    let stmt = parse_statement("select xml '<a/>' is document").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert!(matches!(select.targets[0].expr, SqlExpr::Xml(_)));
}

#[test]
fn parse_transaction_alias_statements() {
    assert_eq!(
        parse_statement("begin transaction").unwrap(),
        Statement::Begin(TransactionOptions::default())
    );
    assert_eq!(
        parse_statement("commit transaction").unwrap(),
        Statement::Commit
    );
}

#[test]
fn parse_create_unique_index_statement() {
    let stmt =
        parse_statement("create unique index num_exp_add_idx on num_exp_add (id1, id2)").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: true,
            nulls_not_distinct: false,
            concurrently: false,
            only: false,
            if_not_exists: false,
            index_name: "num_exp_add_idx".into(),
            table_name: "num_exp_add".into(),
            using_method: None,
            columns: vec![
                IndexColumnDef {
                    name: "id1".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: "id2".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
            predicate_sql: None,
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_comment_on_table_statement() {
    let stmt = parse_statement("comment on table items is 'hello world'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnTable(CommentOnTableStatement {
            table_name: "items".into(),
            comment: Some("hello world".into()),
        })
    );
}

#[test]
fn parse_comment_on_table_null_statement() {
    let stmt = parse_statement("comment on table public.items is null").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnTable(CommentOnTableStatement {
            table_name: "public.items".into(),
            comment: None,
        })
    );
}

#[test]
fn parse_comment_on_view_statement() {
    assert_eq!(
        parse_statement("comment on view toyemp is 'is a view'").unwrap(),
        Statement::CommentOnView(CommentOnViewStatement {
            view_name: "toyemp".into(),
            comment: Some("is a view".into()),
        })
    );
}

#[test]
fn parse_create_view_options() {
    assert!(matches!(
        parse_statement("create view secure_names with (security_barrier, security_invoker=false) as select id from people").unwrap(),
        Statement::CreateView(CreateViewStatement { view_name, options, .. })
            if view_name == "secure_names"
                && options.len() == 2
                && options[0].name == "security_barrier"
                && options[0].value == "true"
                && options[1].name == "security_invoker"
                && options[1].value == "false"
    ));
}

#[test]
fn parse_create_view_column_aliases_over_union() {
    let stmt = parse_statement(
        "create view union_names (id, label) as select 1, 'a' union all select 2, 'b'",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateView(CreateViewStatement { view_name, column_names, query, .. })
            if view_name == "union_names"
                && column_names == vec!["id", "label"]
                && query.set_operation.is_some()
    ));
}

#[test]
fn parse_comment_on_index_statement() {
    let stmt = parse_statement("comment on index public.items_idx is 'hello world'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnIndex(CommentOnIndexStatement {
            index_name: "public.items_idx".into(),
            comment: Some("hello world".into()),
        })
    );
}

#[test]
fn parse_comment_on_index_null_statement() {
    let stmt = parse_statement("comment on index items_idx is null").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnIndex(CommentOnIndexStatement {
            index_name: "items_idx".into(),
            comment: None,
        })
    );
}

#[test]
fn parse_comment_on_type_and_column_statements() {
    assert_eq!(
        parse_statement("comment on type public.default_test_row is 'good comment'").unwrap(),
        Statement::CommentOnType(CommentOnTypeStatement {
            type_name: "public.default_test_row".into(),
            comment: Some("good comment".into()),
        })
    );
    assert_eq!(
        parse_statement("comment on column default_test_row.f1 is null").unwrap(),
        Statement::CommentOnColumn(CommentOnColumnStatement {
            table_name: "default_test_row".into(),
            column_name: "f1".into(),
            comment: None,
        })
    );
}

#[test]
fn parse_comment_on_function_statement() {
    let stmt =
        parse_statement("comment on function public.add_one(int4) is 'hello function'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnFunction(CommentOnFunctionStatement {
            schema_name: Some("public".into()),
            function_name: "add_one".into(),
            arg_types: vec!["int4".into()],
            comment: Some("hello function".into()),
        })
    );
}

#[test]
fn parse_comment_on_function_null_statement() {
    let stmt = parse_statement("comment on function noop() is null").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnFunction(CommentOnFunctionStatement {
            schema_name: None,
            function_name: "noop".into(),
            arg_types: Vec::new(),
            comment: None,
        })
    );
}

#[test]
fn parse_comment_on_operator_statement() {
    let stmt =
        parse_statement("comment on operator public.@#@ (none, int8) is 'prefix op'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnOperator(CommentOnOperatorStatement {
            schema_name: Some("public".into()),
            operator_name: "@#@".into(),
            left_arg: None,
            right_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int8))),
            comment: Some("prefix op".into()),
        })
    );
}

#[test]
fn parse_comment_on_operator_null_statement() {
    let stmt = parse_statement("comment on operator ## (path, path) is null").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnOperator(CommentOnOperatorStatement {
            schema_name: None,
            operator_name: "##".into(),
            left_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Path))),
            right_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Path))),
            comment: None,
        })
    );
}

#[test]
fn parse_comment_on_constraint_statement() {
    let stmt =
        parse_statement("comment on constraint items_pkey on public.items is 'hello'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnConstraint(CommentOnConstraintStatement {
            constraint_name: "items_pkey".into(),
            table_name: "public.items".into(),
            comment: Some("hello".into()),
        })
    );
}

#[test]
fn parse_comment_on_rule_statement() {
    let stmt = parse_statement("comment on rule r1 on public.items is 'hello'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnRule(CommentOnRuleStatement {
            rule_name: "r1".into(),
            relation_name: "public.items".into(),
            comment: Some("hello".into()),
        })
    );
}

#[test]
fn parse_create_statistics_statement() {
    let stmt = parse_statement(
        "create statistics if not exists public.tst (ndistinct, dependencies) on a, (b + 1) from items",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateStatistics(CreateStatisticsStatement {
            if_not_exists: true,
            statistics_name: Some("public.tst".into()),
            kinds: vec!["ndistinct".into(), "dependencies".into()],
            targets: vec!["a".into(), "(b + 1)".into()],
            from_clause: "items".into(),
        })
    );
}

#[test]
fn parse_create_statistics_without_explicit_name() {
    let stmt = parse_statement("create statistics on a, (b + 1) from items").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateStatistics(CreateStatisticsStatement {
            if_not_exists: false,
            statistics_name: None,
            kinds: vec![],
            targets: vec!["a".into(), "(b + 1)".into()],
            from_clause: "items".into(),
        })
    );
}

#[test]
fn parse_create_statistics_function_call_targets() {
    let stmt = parse_statement(
        "create statistics s on date_trunc('day', d), public.upper(b), (a + b) from items",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateStatistics(CreateStatisticsStatement {
            if_not_exists: false,
            statistics_name: Some("s".into()),
            kinds: vec![],
            targets: vec![
                "date_trunc('day', d)".into(),
                "public.upper(b)".into(),
                "(a + b)".into(),
            ],
            from_clause: "items".into(),
        })
    );
}

#[test]
fn parse_statistics_ddl_statements() {
    assert!(matches!(
        parse_statement("alter statistics if exists public.tst rename to public.tst2").unwrap(),
        Statement::AlterStatistics(AlterStatisticsStatement {
            if_exists: true,
            statistics_name,
            action: AlterStatisticsAction::Rename { new_name },
        }) if statistics_name == "public.tst" && new_name == "public.tst2"
    ));

    assert!(matches!(
        parse_statement("alter statistics tst set statistics 42").unwrap(),
        Statement::AlterStatistics(AlterStatisticsStatement {
            if_exists: false,
            statistics_name,
            action: AlterStatisticsAction::SetStatistics { target },
        }) if statistics_name == "tst" && target == 42
    ));

    assert!(matches!(
        parse_statement("alter statistics tst owner to app_owner").unwrap(),
        Statement::AlterStatistics(AlterStatisticsStatement {
            if_exists: false,
            statistics_name,
            action: AlterStatisticsAction::OwnerTo { new_owner },
        }) if statistics_name == "tst" && new_owner == "app_owner"
    ));

    assert!(matches!(
        parse_statement("alter statistics tst set schema app_schema").unwrap(),
        Statement::AlterStatistics(AlterStatisticsStatement {
            if_exists: false,
            statistics_name,
            action: AlterStatisticsAction::SetSchema { new_schema },
        }) if statistics_name == "tst" && new_schema == "app_schema"
    ));

    assert!(matches!(
        parse_statement("drop statistics if exists public.tst, tst2 cascade").unwrap(),
        Statement::DropStatistics(DropStatisticsStatement {
            if_exists: true,
            statistics_names,
            cascade: true,
        }) if statistics_names == vec!["public.tst", "tst2"]
    ));

    assert!(matches!(
        parse_statement("comment on statistics public.tst is 'hello'").unwrap(),
        Statement::CommentOnStatistics(CommentOnStatisticsStatement {
            statistics_name,
            comment,
        }) if statistics_name == "public.tst" && comment.as_deref() == Some("hello")
    ));
}

#[test]
fn parse_statistics_rejects_unparenthesized_expression_targets() {
    assert!(matches!(
        parse_statement("create statistics tst on y + z from items"),
        Err(ParseError::UnexpectedToken { .. })
    ));
    assert!(matches!(
        parse_statement("create statistics tst on (x, y) from items"),
        Err(ParseError::UnexpectedToken { .. })
    ));
}

#[test]
fn parse_text_search_dictionary_ddl() {
    let stmt = parse_statement(
        "create text search dictionary ispell (Template=ispell, DictFile=ispell_sample)",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateTextSearchDictionary(CreateTextSearchDictionaryStatement {
            schema_name: None,
            dictionary_name,
            options,
        }) if dictionary_name == "ispell"
            && options.iter().any(|option| option.name == "template" && option.value == "ispell")
            && options.iter().any(|option| option.name == "dictfile" && option.value == "ispell_sample")
    ));

    let stmt = parse_statement(
        "CREATE TEXT SEARCH DICTIONARY ispell (
                        Template=ispell,
                        DictFile=ispell_sample,
                        AffFile=ispell_sample
);",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateTextSearchDictionary(CreateTextSearchDictionaryStatement {
            dictionary_name,
            options,
            ..
        }) if dictionary_name == "ispell"
            && options.iter().any(|option| option.name == "afffile" && option.value == "ispell_sample")
    ));

    let stmt =
        parse_statement(r#"alter text search dictionary synonym (CaseSensitive = off)"#).unwrap();
    assert!(matches!(
        stmt,
        Statement::AlterTextSearchDictionary(AlterTextSearchDictionaryStatement {
            schema_name: None,
            dictionary_name,
            options,
        }) if dictionary_name == "synonym"
            && options.iter().any(|option| option.name == "casesensitive" && option.value == "off")
    ));
}

#[test]
fn parse_text_search_configuration_ddl() {
    let stmt = parse_statement("create text search configuration public.ispell_tst (copy=english)")
        .unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateTextSearchConfiguration(CreateTextSearchConfigurationStatement {
            schema_name: Some(schema_name),
            config_name,
            copy_config_name,
        }) if schema_name == "public" && config_name == "ispell_tst" && copy_config_name == "english"
    ));

    let stmt = parse_statement(
        "alter text search configuration ispell_tst alter mapping for word, asciiword with ispell, english_stem",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::AlterTextSearchConfiguration(AlterTextSearchConfigurationStatement {
            config_name,
            action: AlterTextSearchConfigurationAction::AlterMappingFor { token_names, dictionary_names },
            ..
        }) if config_name == "ispell_tst"
            && token_names == vec!["word", "asciiword"]
            && dictionary_names == vec!["ispell", "english_stem"]
    ));

    let stmt = parse_statement(
        "alter text search configuration hunspell_tst alter mapping
            replace ispell with hunspell",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::AlterTextSearchConfiguration(AlterTextSearchConfigurationStatement {
            action: AlterTextSearchConfigurationAction::AlterMappingReplace {
                old_dictionary_name,
                new_dictionary_name,
            },
            ..
        }) if old_dictionary_name == "ispell" && new_dictionary_name == "hunspell"
    ));

    let stmt = parse_statement("drop text search configuration if exists dummy_tst").unwrap();
    assert!(matches!(
        stmt,
        Statement::DropTextSearchConfiguration(DropTextSearchConfigurationStatement {
            if_exists: true,
            config_name,
            ..
        }) if config_name == "dummy_tst"
    ));
}

#[test]
fn parse_comment_on_trigger_statement() {
    let stmt = parse_statement("comment on trigger trig1 on public.items is 'hello'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnTrigger(CommentOnTriggerStatement {
            trigger_name: "trig1".into(),
            table_name: "public.items".into(),
            comment: Some("hello".into()),
        })
    );
}

#[test]
fn parse_create_index_with_method_and_ordering() {
    let stmt = parse_statement(
        "create index num_exp_add_idx on num_exp_add using btree (id1 desc nulls first, id2 asc)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            nulls_not_distinct: false,
            concurrently: false,
            only: false,
            if_not_exists: false,
            index_name: "num_exp_add_idx".into(),
            table_name: "num_exp_add".into(),
            using_method: Some("btree".into()),
            columns: vec![
                IndexColumnDef {
                    name: "id1".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: true,
                    nulls_first: Some(true),
                },
                IndexColumnDef {
                    name: "id2".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
            predicate_sql: None,
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_create_index_with_if_not_exists_and_opclass() {
    let stmt = parse_statement(
        "create index if not exists onek_unique1 on onek using btree(unique1 int4_ops)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            nulls_not_distinct: false,
            concurrently: false,
            only: false,
            if_not_exists: true,
            index_name: "onek_unique1".into(),
            table_name: "onek".into(),
            using_method: Some("btree".into()),
            columns: vec![IndexColumnDef {
                name: "unique1".into(),
                expr_sql: None,
                expr_type: None,
                collation: None,
                opclass: Some("int4_ops".into()),
                opclass_options: Vec::new(),
                descending: false,
                nulls_first: None,
            }],
            include_columns: Vec::new(),
            predicate: None,
            predicate_sql: None,
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_create_index_column_collation() {
    let stmt =
        parse_statement("create unique index cwi_uniq4_idx on cwi_test (b collate \"POSIX\")")
            .unwrap();
    match stmt {
        Statement::CreateIndex(CreateIndexStatement { columns, .. }) => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].name, "b");
            assert_eq!(columns[0].collation.as_deref(), Some("POSIX"));
        }
        other => panic!("expected create index statement, got {other:?}"),
    }
}

#[test]
fn parse_create_index_with_opclass_options() {
    let stmt = parse_statement(
        "create index wowidx on test_tsvector using gist (a tsvector_ops(siglen=1))",
    )
    .unwrap();
    let Statement::CreateIndex(stmt) = stmt else {
        panic!("expected create index");
    };
    assert_eq!(stmt.using_method.as_deref(), Some("gist"));
    assert_eq!(stmt.columns.len(), 1);
    assert_eq!(stmt.columns[0].opclass.as_deref(), Some("tsvector_ops"));
    assert_eq!(
        stmt.columns[0].opclass_options,
        vec![crate::include::nodes::parsenodes::RelOption {
            name: "siglen".into(),
            value: "1".into(),
        }]
    );
    assert!(stmt.options.is_empty());
}

#[test]
fn parse_create_index_without_name() {
    let stmt = parse_statement("create index on tenk1 (thousand, tenthous)").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            nulls_not_distinct: false,
            concurrently: false,
            only: false,
            if_not_exists: false,
            index_name: String::new(),
            table_name: "tenk1".into(),
            using_method: None,
            columns: vec![
                IndexColumnDef {
                    name: "thousand".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: "tenthous".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
            predicate_sql: None,
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_create_index_if_not_exists_requires_name() {
    let err = parse_statement("create index if not exists on tenk1 (thousand)").unwrap_err();
    assert_eq!(err.to_string(), "syntax error at or near \"ON\"");
}

#[test]
fn parse_create_table_rejects_malformed_tuple_default_expression() {
    let err = parse_statement("CREATE TABLE error_tbl (i int DEFAULT (100, ))").unwrap_err();
    assert_eq!(err.to_string(), "syntax error at or near \")\"");
}

#[test]
fn parse_create_table_rejects_unparenthesized_in_default_expression() {
    let err = parse_statement("CREATE TABLE error_tbl (b1 bool DEFAULT 1 IN (1, 2))").unwrap_err();
    assert_eq!(err.to_string(), "syntax error at or near \"IN\"");
}

#[test]
fn parse_create_operator_class_hash_support() {
    let stmt = parse_statement(
        "create operator class part_test_int4_ops for type int4 using hash as operator 1 =, function 2 part_hashint4_noop(int4, int8)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateOperatorClass(CreateOperatorClassStatement {
            schema_name: None,
            opclass_name: "part_test_int4_ops".into(),
            data_type: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
            access_method: "hash".into(),
            is_default: false,
            items: vec![
                CreateOperatorClassItem::Operator {
                    strategy_number: 1,
                    operator_name: "=".into(),
                },
                CreateOperatorClassItem::Function {
                    support_number: 2,
                    schema_name: None,
                    function_name: "part_hashint4_noop".into(),
                    arg_types: vec![
                        RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                        RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int8)),
                    ],
                },
            ],
        })
    );
}

#[test]
fn parse_operator_family_and_class_alter_statements() {
    assert_eq!(
        parse_statement("create operator family alt_opf1 using hash").unwrap(),
        Statement::CreateOperatorFamily(CreateOperatorFamilyStatement {
            schema_name: None,
            family_name: "alt_opf1".into(),
            access_method: "hash".into(),
        })
    );
    assert_eq!(
        parse_statement("alter operator family alt_opf1 using hash owner to user1").unwrap(),
        Statement::AlterOperatorFamily(AlterOperatorFamilyStatement {
            schema_name: None,
            family_name: "alt_opf1".into(),
            access_method: "hash".into(),
            action: AlterOperatorFamilyAction::OwnerTo {
                new_owner: "user1".into(),
            },
        })
    );
    assert_eq!(
        parse_statement("alter operator class alt_opc1 using hash set schema alt_nsp2").unwrap(),
        Statement::AlterOperatorClass(AlterOperatorClassStatement {
            schema_name: None,
            opclass_name: "alt_opc1".into(),
            access_method: "hash".into(),
            action: AlterOperatorClassAction::SetSchema {
                new_schema: "alt_nsp2".into(),
            },
        })
    );
    assert_eq!(
        parse_statement("drop operator family if exists alt_opf1 using hash").unwrap(),
        Statement::DropOperatorFamily(DropOperatorFamilyStatement {
            if_exists: true,
            schema_name: None,
            family_name: "alt_opf1".into(),
            access_method: "hash".into(),
        })
    );

    let stmt =
        parse_statement("create operator class alt_opc1 for type uuid using hash as storage uuid")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateOperatorClass(CreateOperatorClassStatement {
            schema_name: None,
            opclass_name: "alt_opc1".into(),
            data_type: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Uuid)),
            access_method: "hash".into(),
            is_default: false,
            items: vec![CreateOperatorClassItem::Storage {
                storage_type: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Uuid)),
            }],
        })
    );
}

#[test]
fn parse_text_search_generic_statements() {
    assert_eq!(
        parse_statement("create text search dictionary alt_ts_dict1 (template=simple)").unwrap(),
        Statement::CreateTextSearch(CreateTextSearchStatement {
            kind: TextSearchObjectKind::Dictionary,
            schema_name: None,
            object_name: "alt_ts_dict1".into(),
            parameters: vec![TextSearchParameter {
                name: "template".into(),
                value: "simple".into(),
            }],
        })
    );
    assert_eq!(
        parse_statement("alter text search configuration alt_ts_conf1 owner to user1").unwrap(),
        Statement::AlterTextSearch(AlterTextSearchStatement {
            kind: TextSearchObjectKind::Configuration,
            schema_name: None,
            object_name: "alt_ts_conf1".into(),
            action: AlterTextSearchAction::OwnerTo {
                new_owner: "user1".into(),
            },
        })
    );
    assert_eq!(
        parse_statement("alter text search parser alt_ts_prs1 set schema alt_nsp2").unwrap(),
        Statement::AlterTextSearch(AlterTextSearchStatement {
            kind: TextSearchObjectKind::Parser,
            schema_name: None,
            object_name: "alt_ts_prs1".into(),
            action: AlterTextSearchAction::SetSchema {
                new_schema: "alt_nsp2".into(),
            },
        })
    );
    let err =
        parse_statement(r#"create text search template tstemp_case ("Init" = init_function)"#)
            .unwrap_err();
    assert!(
        err.to_string()
            .contains("parameter \"Init\" not recognized")
    );
}

#[test]
fn parse_partitioned_index_ddl_forms() {
    assert!(matches!(
        parse_statement("create index concurrently on idxpart(a)").unwrap(),
        Statement::CreateIndex(CreateIndexStatement {
            concurrently: true,
            index_name,
            table_name,
            ..
        }) if index_name.is_empty() && table_name == "idxpart"
    ));
    assert!(matches!(
        parse_statement("create index on only idxpart(a)").unwrap(),
        Statement::CreateIndex(CreateIndexStatement {
            only: true,
            index_name,
            table_name,
            ..
        }) if index_name.is_empty() && table_name == "idxpart"
    ));
    assert!(matches!(
        parse_statement("drop index concurrently idxpart_a_idx").unwrap(),
        Statement::DropIndex(DropIndexStatement {
            concurrently: true,
            if_exists: false,
            index_names,
        }) if index_names == vec!["idxpart_a_idx"]
    ));
    assert_eq!(
        parse_statement("alter index idxpart_a_idx attach partition idxpart1_a_idx").unwrap(),
        Statement::AlterIndexAttachPartition(AlterIndexAttachPartitionStatement {
            parent_index_name: "idxpart_a_idx".into(),
            child_index_name: "idxpart1_a_idx".into(),
        })
    );
}

#[test]
fn parse_create_index_with_expression_item() {
    let stmt = parse_statement("create index attmp_idx on attmp (a, (d + e), b)").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            nulls_not_distinct: false,
            concurrently: false,
            only: false,
            if_not_exists: false,
            index_name: "attmp_idx".into(),
            table_name: "attmp".into(),
            using_method: None,
            columns: vec![
                IndexColumnDef {
                    name: "a".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: String::new(),
                    expr_sql: Some("d + e".into()),
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: "b".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
            predicate_sql: None,
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_create_index_with_function_expression_item() {
    let stmt =
        parse_statement("create index on test_range_elem using spgist(int4range(i,i+10))").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            nulls_not_distinct: false,
            concurrently: false,
            only: false,
            if_not_exists: false,
            index_name: String::new(),
            table_name: "test_range_elem".into(),
            using_method: Some("spgist".into()),
            columns: vec![IndexColumnDef {
                name: String::new(),
                expr_sql: Some("int4range(i,i+10)".into()),
                expr_type: None,
                collation: None,
                opclass: None,
                opclass_options: Vec::new(),
                descending: false,
                nulls_first: None,
            }],
            include_columns: Vec::new(),
            predicate: None,
            predicate_sql: None,
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_create_partial_index_statement_captures_predicate_sql() {
    let stmt = parse_statement(
        "create index onek2_u1_prtl on onek2 using btree(unique1 int4_ops) where unique1 < 20 or unique1 > 980",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            nulls_not_distinct: false,
            concurrently: false,
            only: false,
            if_not_exists: false,
            index_name: "onek2_u1_prtl".into(),
            table_name: "onek2".into(),
            using_method: Some("btree".into()),
            columns: vec![IndexColumnDef {
                name: "unique1".into(),
                expr_sql: None,
                expr_type: None,
                collation: None,
                opclass: Some("int4_ops".into()),
                opclass_options: Vec::new(),
                descending: false,
                nulls_first: None,
            }],
            include_columns: Vec::new(),
            predicate: Some(parse_expr("unique1 < 20 or unique1 > 980").unwrap()),
            predicate_sql: Some("unique1 < 20 or unique1 > 980".into()),
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_alter_table_add_column_statement() {
    let stmt = parse_statement("alter table items add column note text default 'hello'").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddColumn(AlterTableAddColumnStatement {
            if_exists: false,
            missing_ok: false,
            only: false,
            table_name: "items".into(),
            column: ColumnDef {
                name: "note".into(),
                ty: builtin_type(SqlType::new(SqlTypeKind::Text)),
                collation: None,
                default_expr: Some("'hello'".into()),
                generated: None,
                identity: None,
                storage: None,
                compression: None,
                constraints: vec![],
            },
            fdw_options: None,
        })
    );
}

#[test]
fn parse_alter_table_multi_add_column_statement() {
    let stmt = parse_statement("alter table mlparted add d int, add e text").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddColumns(AlterTableAddColumnsStatement {
            if_exists: false,
            only: false,
            table_name: "mlparted".into(),
            columns: vec![
                ColumnDef {
                    name: "d".into(),
                    ty: builtin_type(SqlType::new(SqlTypeKind::Int4)),
                    collation: None,
                    default_expr: None,
                    generated: None,
                    identity: None,
                    storage: None,
                    compression: None,
                    constraints: vec![],
                },
                ColumnDef {
                    name: "e".into(),
                    ty: builtin_type(SqlType::new(SqlTypeKind::Text)),
                    collation: None,
                    default_expr: None,
                    generated: None,
                    identity: None,
                    storage: None,
                    compression: None,
                    constraints: vec![],
                },
            ],
        })
    );
}

#[test]
fn parse_alter_table_constraint_statements() {
    let stmt =
        parse_statement("alter table items add constraint items_id_check check (id > 0) not valid")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::Check {
                attributes: ConstraintAttributes {
                    name: Some("items_id_check".into()),
                    not_valid: true,
                    no_inherit: false,
                    deferrable: None,
                    initially_deferred: None,
                    enforced: None,
                    nulls_not_distinct: false,
                },
                expr_sql: "id > 0".into(),
            },
        })
    );

    let stmt = parse_statement(
        "alter table items add constraint items_note_required not null note not valid",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::NotNull {
                attributes: ConstraintAttributes {
                    name: Some("items_note_required".into()),
                    not_valid: true,
                    no_inherit: false,
                    deferrable: None,
                    initially_deferred: None,
                    enforced: None,
                    nulls_not_distinct: false,
                },
                column: "note".into(),
            },
        })
    );

    let stmt = parse_statement(
        "alter table pets add constraint pets_owner_fk foreign key (owner_id, owner_name) references people(id, name) match full",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "pets".into(),
            constraint: TableConstraint::ForeignKey {
                attributes: ConstraintAttributes {
                    name: Some("pets_owner_fk".into()),
                    ..attrs()
                },
                columns: vec!["owner_id".into(), "owner_name".into()],
                period: None,
                referenced_table: "people".into(),
                referenced_columns: Some(vec!["id".into(), "name".into()]),
                referenced_period: None,
                match_type: ForeignKeyMatchType::Full,
                on_delete: ForeignKeyAction::NoAction,
                on_delete_set_columns: None,
                on_update: ForeignKeyAction::NoAction,
            },
        })
    );

    let stmt = parse_statement(
        "alter table pets add foreign key (owner_id, owner_name) references people(id, name) match full",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "pets".into(),
            constraint: TableConstraint::ForeignKey {
                attributes: attrs(),
                columns: vec!["owner_id".into(), "owner_name".into()],
                period: None,
                referenced_table: "people".into(),
                referenced_columns: Some(vec!["id".into(), "name".into()]),
                referenced_period: None,
                match_type: ForeignKeyMatchType::Full,
                on_delete: ForeignKeyAction::NoAction,
                on_delete_set_columns: None,
                on_update: ForeignKeyAction::NoAction,
            },
        })
    );

    let stmt = parse_statement(
        "alter table pets add foreign key (owner_id, owner_name) references people(id, name) on delete set null (owner_name)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "pets".into(),
            constraint: TableConstraint::ForeignKey {
                attributes: attrs(),
                columns: vec!["owner_id".into(), "owner_name".into()],
                period: None,
                referenced_table: "people".into(),
                referenced_columns: Some(vec!["id".into(), "name".into()]),
                referenced_period: None,
                match_type: ForeignKeyMatchType::Simple,
                on_delete: ForeignKeyAction::SetNull,
                on_delete_set_columns: Some(vec!["owner_name".into()]),
                on_update: ForeignKeyAction::NoAction,
            },
        })
    );

    let stmt = parse_statement(
        "alter table pets add constraint pets_owner_fk foreign key (owner_id, period valid_at) references people(id, period valid_at)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "pets".into(),
            constraint: TableConstraint::ForeignKey {
                attributes: ConstraintAttributes {
                    name: Some("pets_owner_fk".into()),
                    ..attrs()
                },
                columns: vec!["owner_id".into(), "valid_at".into()],
                period: Some("valid_at".into()),
                referenced_table: "people".into(),
                referenced_columns: Some(vec!["id".into(), "valid_at".into()]),
                referenced_period: Some("valid_at".into()),
                match_type: ForeignKeyMatchType::Simple,
                on_delete: ForeignKeyAction::NoAction,
                on_delete_set_columns: None,
                on_update: ForeignKeyAction::NoAction,
            },
        })
    );

    let stmt = parse_statement(
        "alter table pets add column valid_at daterange, add constraint pets_pk primary key (owner_id, valid_at without overlaps)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableMulti(vec![
            "ALTER TABLE pets add column valid_at daterange".into(),
            "ALTER TABLE pets add constraint pets_pk primary key (owner_id, valid_at without overlaps)".into(),
        ])
    );

    let stmt = parse_statement("alter table pets replica identity using index pets_pk").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableReplicaIdentity(AlterTableReplicaIdentityStatement {
            if_exists: false,
            only: false,
            table_name: "pets".into(),
            identity: crate::include::nodes::parsenodes::ReplicaIdentityKind::Index(
                "pets_pk".into()
            ),
        })
    );
    assert!(matches!(
        parse_statement("alter table pets replica identity full").unwrap(),
        Statement::AlterTableReplicaIdentity(AlterTableReplicaIdentityStatement {
            identity: crate::include::nodes::parsenodes::ReplicaIdentityKind::Full,
            ..
        })
    ));

    let stmt =
        parse_statement("alter table items add constraint items_key unique using index items_idx")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::UniqueUsingIndex {
                attributes: ConstraintAttributes {
                    name: Some("items_key".into()),
                    ..attrs()
                },
                index_name: "items_idx".into(),
            },
        })
    );

    let stmt =
        parse_statement("alter table items add primary key using index items_pkey_idx").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::PrimaryKeyUsingIndex {
                attributes: attrs(),
                index_name: "items_pkey_idx".into(),
            },
        })
    );
    let stmt = parse_statement("alter table items drop constraint items_id_check").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropConstraint(AlterTableDropConstraintStatement {
            if_exists: false,
            missing_ok: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            cascade: false,
        })
    );

    let stmt =
        parse_statement("alter table items drop constraint items_id_check restrict").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropConstraint(AlterTableDropConstraintStatement {
            if_exists: false,
            missing_ok: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            cascade: false,
        })
    );

    let stmt = parse_statement("alter table items drop constraint items_id_check cascade").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropConstraint(AlterTableDropConstraintStatement {
            if_exists: false,
            missing_ok: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            cascade: true,
        })
    );

    let stmt = parse_statement(
        "alter table items alter constraint items_id_check deferrable initially deferred",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterConstraint(AlterTableAlterConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            not_valid: false,
            inheritability: None,
            deferrable: Some(true),
            initially_deferred: Some(true),
            enforced: None,
        })
    );

    let stmt = parse_statement(
        "alter table items alter constraint items_id_check not enforced not deferrable",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterConstraint(AlterTableAlterConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            not_valid: false,
            inheritability: None,
            deferrable: Some(false),
            initially_deferred: None,
            enforced: Some(false),
        })
    );

    let stmt =
        parse_statement("alter table items alter constraint items_id_check not valid no inherit")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterConstraint(AlterTableAlterConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            not_valid: true,
            inheritability: Some(false),
            deferrable: None,
            initially_deferred: None,
            enforced: None,
        })
    );

    let stmt =
        parse_statement("alter table items alter constraint items_id_check inherit").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterConstraint(AlterTableAlterConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            not_valid: false,
            inheritability: Some(true),
            deferrable: None,
            initially_deferred: None,
            enforced: None,
        })
    );

    let err =
        parse_statement("alter table items alter constraint items_id_check enforced not enforced")
            .unwrap_err();
    assert!(matches!(
        err,
        ParseError::DetailedError { message, .. }
            if message == "conflicting constraint properties"
    ));

    let stmt =
        parse_statement("alter table items rename constraint items_id_check to items_id_guard")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableRenameConstraint(AlterTableRenameConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
            new_constraint_name: "items_id_guard".into(),
        })
    );

    let stmt = parse_statement("alter table items validate constraint items_id_check").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableValidateConstraint(AlterTableValidateConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
        })
    );

    let stmt = parse_statement("alter table items alter column note set not null").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableSetNotNull(AlterTableSetNotNullStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
        })
    );

    let stmt = parse_statement("alter table items alter note drop not null").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropNotNull(AlterTableDropNotNullStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
        })
    );

    let stmt = parse_statement("alter table items alter column note set default 'hello'").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnDefault(AlterTableAlterColumnDefaultStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            default_expr: Some(SqlExpr::Const(Value::Text("hello".into()))),
            default_expr_sql: Some("'hello'".into()),
        })
    );

    let stmt =
        parse_statement("alter table items alter column note set default 'generated'").unwrap();
    assert!(matches!(
        stmt,
        Statement::AlterTableAlterColumnDefault(AlterTableAlterColumnDefaultStatement {
            default_expr: Some(SqlExpr::Const(Value::Text(value))),
            ..
        }) if value.as_str() == "generated"
    ));

    let stmt = parse_statement("alter table items alter note drop default").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnDefault(AlterTableAlterColumnDefaultStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            default_expr: None,
            default_expr_sql: None,
        })
    );
}

#[test]
fn parse_identity_options_alter_and_overriding() {
    let stmt = parse_statement(
        "create table items (id int generated by default as identity (start with 7 increment by 5))",
    )
    .unwrap();
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table");
    };
    let column = create.columns().next().unwrap();
    let identity = column.identity.as_ref().unwrap();
    assert_eq!(identity.kind, ColumnIdentityKind::ByDefault);
    assert_eq!(identity.options.start, Some(7));
    assert_eq!(identity.options.increment, Some(5));

    let stmt = parse_statement(
        "alter table items alter column id set generated always set increment by 2 restart with 100",
    )
    .unwrap();
    let Statement::AlterTableAlterColumnIdentity(alter) = stmt else {
        panic!("expected alter identity");
    };
    match alter.action {
        AlterColumnIdentityAction::Set {
            generation,
            options,
        } => {
            assert_eq!(generation, Some(ColumnIdentityKind::Always));
            assert_eq!(options.increment, Some(2));
            assert_eq!(options.restart, Some(Some(100)));
        }
        other => panic!("expected set identity action, got {other:?}"),
    }

    let stmt = parse_statement("insert into items overriding user value values (42)").unwrap();
    assert!(matches!(
        stmt,
        Statement::Insert(InsertStatement {
            overriding: Some(OverridingKind::User),
            ..
        })
    ));
}

#[test]
fn parse_check_constraint_no_inherit() {
    let stmt = parse_statement(
        "alter table items add constraint items_id_check check (id > 0) no inherit",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::Check {
                attributes: ConstraintAttributes {
                    name: Some("items_id_check".into()),
                    no_inherit: true,
                    ..attrs()
                },
                expr_sql: "id > 0".into(),
            },
        })
    );

    let stmt = parse_statement(
        "create table items (id int4 constraint id_positive check (id > 0) no inherit)",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(
        ct.columns().collect::<Vec<_>>()[0].constraints,
        vec![ColumnConstraint::Check {
            attributes: ConstraintAttributes {
                name: Some("id_positive".into()),
                no_inherit: true,
                ..attrs()
            },
            expr_sql: "id > 0".into(),
        }]
    );
}

#[test]
fn parse_not_null_constraint_no_inherit() {
    let stmt = parse_statement(
        "alter table items add constraint items_note_required not null note no inherit",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::NotNull {
                attributes: ConstraintAttributes {
                    name: Some("items_note_required".into()),
                    no_inherit: true,
                    ..attrs()
                },
                column: "note".into(),
            },
        })
    );

    let stmt = parse_statement("create table items (id int4 not null no inherit)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(
        ct.columns().collect::<Vec<_>>()[0].constraints,
        vec![ColumnConstraint::NotNull {
            attributes: ConstraintAttributes {
                no_inherit: true,
                ..attrs()
            },
        }]
    );
}

#[test]
fn parse_alter_table_set_statement() {
    let stmt = parse_statement("alter table unlogged1 set logged").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableSetPersistence(AlterTableSetPersistenceStatement {
            if_exists: false,
            only: false,
            table_name: "unlogged1".into(),
            persistence: TablePersistence::Permanent,
        })
    );

    let stmt = parse_statement("alter table unlogged1 set unlogged").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableSetPersistence(AlterTableSetPersistenceStatement {
            if_exists: false,
            only: false,
            table_name: "unlogged1".into(),
            persistence: TablePersistence::Unlogged,
        })
    );

    let stmt = parse_statement("alter table num_variance set (parallel_workers = 4)").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableSet(AlterTableSetStatement {
            if_exists: false,
            only: false,
            table_name: "num_variance".into(),
            options: vec![RelOption {
                name: "parallel_workers".into(),
                value: "4".into(),
            }],
        })
    );

    let stmt = parse_statement("alter view rw_view1 set (security_invoker = true)").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableSet(AlterTableSetStatement {
            if_exists: false,
            only: false,
            table_name: "rw_view1".into(),
            options: vec![RelOption {
                name: "security_invoker".into(),
                value: "true".into(),
            }],
        })
    );

    let stmt = parse_statement("alter table vac_truncate_test reset (vacuum_truncate)").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableReset(AlterTableResetStatement {
            if_exists: false,
            only: false,
            table_name: "vac_truncate_test".into(),
            options: vec!["vacuum_truncate".into()],
        })
    );

    let stmt = parse_statement(
        "alter table attmp alter column i set (n_distinct = 1, n_distinct_inherited = 2)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnOptions(AlterTableAlterColumnOptionsStatement {
            if_exists: false,
            only: false,
            table_name: "attmp".into(),
            column_name: "i".into(),
            action: AlterColumnOptionsAction::Set(vec![
                RelOption {
                    name: "n_distinct".into(),
                    value: "1".into(),
                },
                RelOption {
                    name: "n_distinct_inherited".into(),
                    value: "2".into(),
                },
            ]),
        })
    );

    let stmt = parse_statement(
        "alter table if exists only attmp alter column i reset (n_distinct_inherited)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnOptions(AlterTableAlterColumnOptionsStatement {
            if_exists: true,
            only: true,
            table_name: "attmp".into(),
            column_name: "i".into(),
            action: AlterColumnOptionsAction::Reset(vec!["n_distinct_inherited".into()]),
        })
    );

    let stmt = parse_statement("alter table attmp alter column i set statistics 150").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnStatistics(AlterTableAlterColumnStatisticsStatement {
            if_exists: false,
            only: false,
            table_name: "attmp".into(),
            column_name: "i".into(),
            statistics_target: 150,
        })
    );

    let stmt = parse_statement("alter table attmp alter column note set compression pglz").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnCompression(AlterTableAlterColumnCompressionStatement {
            if_exists: false,
            only: false,
            table_name: "attmp".into(),
            column_name: "note".into(),
            compression: AttributeCompression::Pglz,
        })
    );

    let stmt = parse_statement(
        "alter table if exists only attmp alter column note set compression default",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnCompression(AlterTableAlterColumnCompressionStatement {
            if_exists: true,
            only: true,
            table_name: "attmp".into(),
            column_name: "note".into(),
            compression: AttributeCompression::Default,
        })
    );

    let stmt = parse_statement("alter table attmp alter note set compression lz4").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnCompression(AlterTableAlterColumnCompressionStatement {
            if_exists: false,
            only: false,
            table_name: "attmp".into(),
            column_name: "note".into(),
            compression: AttributeCompression::Lz4,
        })
    );

    let stmt = parse_statement("alter table attmp alter column note set storage external").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnStorage(AlterTableAlterColumnStorageStatement {
            if_exists: false,
            only: false,
            table_name: "attmp".into(),
            column_name: "note".into(),
            storage: AttributeStorage::External,
        })
    );
}

#[test]
fn parse_alter_table_inherit_statement() {
    let stmt =
        parse_statement("alter table if exists only child_items inherit parent_items").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableInherit(AlterTableInheritStatement {
            if_exists: true,
            only: true,
            table_name: "child_items".into(),
            parent_name: "parent_items".into(),
        })
    );
}

#[test]
fn parse_alter_table_no_inherit_statement() {
    let stmt =
        parse_statement("alter table if exists only child_items no inherit parent_items").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableNoInherit(AlterTableNoInheritStatement {
            if_exists: true,
            only: true,
            table_name: "child_items".into(),
            parent_name: "parent_items".into(),
        })
    );
}

#[test]
fn parse_alter_table_row_security_statements() {
    let cases = [
        (
            "alter table items enable row level security",
            false,
            false,
            AlterTableRowSecurityAction::Enable,
        ),
        (
            "alter table items disable row level security",
            false,
            false,
            AlterTableRowSecurityAction::Disable,
        ),
        (
            "alter table items force row level security",
            false,
            false,
            AlterTableRowSecurityAction::Force,
        ),
        (
            "alter table if exists only items no force row level security",
            true,
            true,
            AlterTableRowSecurityAction::NoForce,
        ),
    ];

    for (sql, if_exists, only, action) in cases {
        let stmt = parse_statement(sql).unwrap();
        assert_eq!(
            stmt,
            Statement::AlterTableSetRowSecurity(AlterTableSetRowSecurityStatement {
                if_exists,
                only,
                table_name: "items".into(),
                action,
            })
        );
    }
}

#[test]
fn parse_policy_statements() {
    let stmt = parse_statement(
        "create policy p1 on items as restrictive for select to app_role using (a > 0) with check (a > 1)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreatePolicy(CreatePolicyStatement {
            policy_name: "p1".into(),
            table_name: "items".into(),
            permissive: false,
            command: crate::include::catalog::PolicyCommand::Select,
            role_names: vec!["app_role".into()],
            using_expr: Some(parse_expr("a > 0").unwrap()),
            using_sql: Some("a > 0".into()),
            with_check_expr: Some(parse_expr("a > 1").unwrap()),
            with_check_sql: Some("a > 1".into()),
        })
    );

    let stmt = parse_statement("alter policy p1 on items rename to p2").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterPolicy(AlterPolicyStatement {
            policy_name: "p1".into(),
            table_name: "items".into(),
            action: AlterPolicyAction::Rename {
                new_name: "p2".into(),
            },
        })
    );

    let stmt = parse_statement("drop policy if exists p2 on items").unwrap();
    assert_eq!(
        stmt,
        Statement::DropPolicy(DropPolicyStatement {
            if_exists: true,
            policy_name: "p2".into(),
            table_name: "items".into(),
        })
    );

    let stmt =
        parse_statement("create policy p3 on items as permissive\n    using (a > 2);\n").unwrap();
    assert_eq!(
        stmt,
        Statement::CreatePolicy(CreatePolicyStatement {
            policy_name: "p3".into(),
            table_name: "items".into(),
            permissive: true,
            command: crate::include::catalog::PolicyCommand::All,
            role_names: vec!["public".into()],
            using_expr: Some(parse_expr("a > 2").unwrap()),
            using_sql: Some("a > 2".into()),
            with_check_expr: None,
            with_check_sql: None,
        })
    );

    let sql = "CREATE POLICY p4 ON items AS UGLY USING (a > 0)";
    let err = parse_statement(sql).unwrap_err();
    assert_eq!(err.to_string(), "unrecognized row security option \"ugly\"");
    assert_eq!(err.position(), sql.find("UGLY").map(|index| index + 1));
    match err.unpositioned() {
        ParseError::DetailedError {
            hint: Some(hint),
            sqlstate: "42601",
            ..
        } => assert_eq!(
            hint,
            "Only PERMISSIVE or RESTRICTIVE policies are supported currently."
        ),
        other => panic!("expected detailed policy option error, got {other:?}"),
    }
}

#[test]
fn parse_alter_table_if_exists_only_statement() {
    let stmt =
        parse_statement("alter table if exists only items rename column note to body").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableRenameColumn(AlterTableRenameColumnStatement {
            if_exists: true,
            only: true,
            table_name: "items".into(),
            column_name: "note".into(),
            new_column_name: "body".into(),
        })
    );
}

#[test]
fn parse_alter_table_star_target_as_inherited_target() {
    let stmt = parse_statement("alter table items* rename column note to body").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableRenameColumn(AlterTableRenameColumnStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            new_column_name: "body".into(),
        })
    );
}

#[test]
fn parse_alter_table_rename_statement() {
    let stmt = parse_statement("alter table items rename to items_new").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableRename(AlterTableRenameStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            new_table_name: "items_new".into(),
        })
    );
}

#[test]
fn parse_alter_index_rename_statement() {
    let stmt = parse_statement("alter index if exists items_idx rename to items_idx_new").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterIndexRename(AlterTableRenameStatement {
            if_exists: true,
            only: false,
            table_name: "items_idx".into(),
            new_table_name: "items_idx_new".into(),
        })
    );
}

#[test]
fn parse_alter_index_set_statement() {
    let stmt = parse_statement("alter index if exists items_idx set (fillfactor = 10)").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterIndexSet(AlterIndexSetStatement {
            if_exists: true,
            index_name: "items_idx".into(),
            options: vec![RelOption {
                name: "fillfactor".into(),
                value: "10".into(),
            }],
        })
    );
}

#[test]
fn parse_alter_view_rename_statement() {
    let stmt = parse_statement("alter view if exists items_view rename to items_view_new").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterViewRename(AlterTableRenameStatement {
            if_exists: true,
            only: false,
            table_name: "items_view".into(),
            new_table_name: "items_view_new".into(),
        })
    );
}

#[test]
fn parse_alter_view_rename_column_statement() {
    let stmt =
        parse_statement("alter view if exists items_view rename column note to body").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterViewRenameColumn(AlterTableRenameColumnStatement {
            if_exists: true,
            only: false,
            table_name: "items_view".into(),
            column_name: "note".into(),
            new_column_name: "body".into(),
        })
    );
}

#[test]
fn parse_alter_table_set_schema_statement() {
    let stmt = parse_statement("alter table if exists items set schema archive").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableSetSchema(AlterRelationSetSchemaStatement {
            if_exists: true,
            relation_name: "items".into(),
            schema_name: "archive".into(),
        })
    );
}

#[test]
fn parse_alter_view_set_schema_statement() {
    let stmt = parse_statement("alter view if exists items_view set schema archive").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterViewSetSchema(AlterRelationSetSchemaStatement {
            if_exists: true,
            relation_name: "items_view".into(),
            schema_name: "archive".into(),
        })
    );
}

#[test]
fn parse_alter_materialized_view_set_schema_statement() {
    let stmt =
        parse_statement("alter materialized view if exists items_mv set schema archive").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterMaterializedViewSetSchema(AlterRelationSetSchemaStatement {
            if_exists: true,
            relation_name: "items_mv".into(),
            schema_name: "archive".into(),
        })
    );
}

#[test]
fn parse_alter_index_set_statistics_statement() {
    let stmt = parse_statement("alter index attmp_idx alter column 2 set statistics 1000").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterIndexAlterColumnStatistics(AlterIndexAlterColumnStatisticsStatement {
            if_exists: false,
            index_name: "attmp_idx".into(),
            column_number: 2,
            statistics_target: 1000,
        })
    );

    let stmt = parse_statement("alter index if exists attmp_idx alter column 2 set statistics -1")
        .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterIndexAlterColumnStatistics(AlterIndexAlterColumnStatisticsStatement {
            if_exists: true,
            index_name: "attmp_idx".into(),
            column_number: 2,
            statistics_target: -1,
        })
    );
}

#[test]
fn parse_alter_index_set_statistics_rejects_column_zero() {
    match parse_statement("alter index attmp_idx alter column 0 set statistics 1000") {
        Err(ParseError::DetailedError {
            message, sqlstate, ..
        }) if message == "column number must be in range from 1 to 32767"
            && sqlstate == "22023" => {}
        other => panic!("expected column-number range error, got {other:?}"),
    }
}

#[test]
fn parse_alter_table_rename_column_statement() {
    let stmt = parse_statement("alter table items rename column note to body").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableRenameColumn(AlterTableRenameColumnStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            new_column_name: "body".into(),
        })
    );
}

#[test]
fn parse_alter_table_rename_column_shorthand() {
    let stmt = parse_statement("alter table items rename note to body").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableRenameColumn(AlterTableRenameColumnStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            new_column_name: "body".into(),
        })
    );
}

#[test]
fn parse_alter_table_drop_column_statement() {
    let stmt = parse_statement("alter table items drop column note").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropColumn(AlterTableDropColumnStatement {
            if_exists: false,
            missing_ok: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            cascade: false,
        })
    );

    let stmt = parse_statement("alter table items drop column note cascade").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropColumn(AlterTableDropColumnStatement {
            if_exists: false,
            missing_ok: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            cascade: true,
        })
    );
}

#[test]
fn parse_drop_role_statement() {
    let stmt = parse_statement("drop role if exists regress_alter_table_user1").unwrap();
    assert_eq!(
        stmt,
        Statement::DropRole(DropRoleStatement {
            if_exists: true,
            role_names: vec!["regress_alter_table_user1".into()],
        })
    );
}

#[test]
fn parse_create_database_statement() {
    let stmt = parse_statement("create database analytics").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateDatabase(CreateDatabaseStatement {
            database_name: "analytics".into(),
            options: CreateDatabaseOptions::default(),
        })
    );
}

#[test]
fn parse_create_database_options() {
    let stmt = parse_statement(
        "create database analytics encoding utf8 lc_collate \"C\" lc_ctype \"C\" template template0",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateDatabase(CreateDatabaseStatement {
            database_name: "analytics".into(),
            options: CreateDatabaseOptions {
                encoding: Some("utf8".into()),
                lc_collate: Some("C".into()),
                lc_ctype: Some("C".into()),
                template: Some("template0".into()),
                ..CreateDatabaseOptions::default()
            },
        })
    );
}

#[test]
fn parse_alter_database_statement() {
    let stmt = parse_statement("alter database analytics rename to warehouse").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterDatabase(AlterDatabaseStatement {
            database_name: "analytics".into(),
            action: AlterDatabaseAction::Rename {
                new_name: "warehouse".into(),
            },
        })
    );

    let stmt = parse_statement("alter database warehouse connection_limit 123").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterDatabase(AlterDatabaseStatement {
            database_name: "warehouse".into(),
            action: AlterDatabaseAction::ConnectionLimit { limit: 123 },
        })
    );
}

#[test]
fn parse_drop_database_statement() {
    let stmt = parse_statement("drop database if exists analytics").unwrap();
    assert_eq!(
        stmt,
        Statement::DropDatabase(DropDatabaseStatement {
            if_exists: true,
            database_name: "analytics".into(),
        })
    );
}

#[test]
fn parse_alter_table_alter_column_type_statement() {
    let stmt = parse_statement(
        "alter table items alter column note set data type varchar(10) using note::varchar(10)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAlterColumnType(AlterTableAlterColumnTypeStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
            ty: builtin_type(SqlType::with_char_len(SqlTypeKind::Varchar, 10)),
            using_expr: Some(SqlExpr::Cast(
                Box::new(SqlExpr::Column("note".into())),
                builtin_type(SqlType::with_char_len(SqlTypeKind::Varchar, 10)),
            )),
        })
    );
}

#[test]
fn parse_alter_table_owner_statement() {
    let stmt = parse_statement("alter table items owner to app_owner").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableOwner(AlterRelationOwnerStatement {
            if_exists: false,
            only: false,
            relation_name: "items".into(),
            new_owner: "app_owner".into(),
        })
    );
}

#[test]
fn parse_alter_view_owner_statement() {
    let stmt = parse_statement("alter view items_view owner to app_owner").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterViewOwner(AlterRelationOwnerStatement {
            if_exists: false,
            only: false,
            relation_name: "items_view".into(),
            new_owner: "app_owner".into(),
        })
    );
}

#[test]
fn parse_create_role_statement_with_options() {
    let stmt =
        parse_statement("create role regress_role_admin createdb createrole replication bypassrls")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRole(CreateRoleStatement {
            role_name: "regress_role_admin".into(),
            is_user: false,
            options: vec![
                RoleOption::CreateDb(true),
                RoleOption::CreateRole(true),
                RoleOption::Replication(true),
                RoleOption::BypassRls(true),
            ],
        })
    );
}

#[test]
fn parse_create_user_statement() {
    let stmt = parse_statement("create user regress_login login").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRole(CreateRoleStatement {
            role_name: "regress_login".into(),
            is_user: true,
            options: vec![RoleOption::Login(true)],
        })
    );
}

#[test]
fn parse_create_user_with_statement() {
    let stmt = parse_statement("create user regress_login with nocreatedb nocreaterole").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRole(CreateRoleStatement {
            role_name: "regress_login".into(),
            is_user: true,
            options: vec![RoleOption::CreateDb(false), RoleOption::CreateRole(false)],
        })
    );
}

#[test]
fn parse_create_group_statement() {
    let stmt = parse_statement("create group regress_group").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRole(CreateRoleStatement {
            role_name: "regress_group".into(),
            is_user: false,
            options: vec![],
        })
    );
}

#[test]
fn parse_create_group_membership_options() {
    let stmt =
        parse_statement("create group regress_group with admin regress_admin user regress_member")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRole(CreateRoleStatement {
            role_name: "regress_group".into(),
            is_user: false,
            options: vec![
                RoleOption::Admin(vec!["regress_admin".into()]),
                RoleOption::Role(vec!["regress_member".into()]),
            ],
        })
    );
}

#[test]
fn parse_create_role_membership_options() {
    let stmt = parse_statement(
        "create role regress_inroles role regress_createdb, regress_login admin regress_role_super in role regress_createrole",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRole(CreateRoleStatement {
            role_name: "regress_inroles".into(),
            is_user: false,
            options: vec![
                RoleOption::Role(vec!["regress_createdb".into(), "regress_login".into()]),
                RoleOption::Admin(vec!["regress_role_super".into()]),
                RoleOption::InRole(vec!["regress_createrole".into()]),
            ],
        })
    );
}

#[test]
fn parse_multiline_create_role_membership_options() {
    let stmt = parse_statement(
        "create role regress_inroles role\n\tregress_createdb, regress_login\nadmin regress_role_super",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRole(CreateRoleStatement {
            role_name: "regress_inroles".into(),
            is_user: false,
            options: vec![
                RoleOption::Role(vec!["regress_createdb".into(), "regress_login".into()]),
                RoleOption::Admin(vec!["regress_role_super".into()]),
            ],
        })
    );
}

#[test]
fn parse_alter_group_add_user_statement() {
    let stmt = parse_statement("alter group regress_group add user regress_member").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantRoleMembership(GrantRoleMembershipStatement {
            role_names: vec!["regress_group".into()],
            grantee_names: vec!["regress_member".into()],
            admin_option: false,
            admin_option_specified: false,
            inherit_option: None,
            set_option: None,
            granted_by: None,
            legacy_group_syntax: true,
        })
    );
}

#[test]
fn parse_multiline_alter_group_add_user_statement() {
    let stmt =
        parse_statement("alter group regress_group add\n\tuser regress_member, regress_member2")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::GrantRoleMembership(GrantRoleMembershipStatement {
            role_names: vec!["regress_group".into()],
            grantee_names: vec!["regress_member".into(), "regress_member2".into()],
            admin_option: false,
            admin_option_specified: false,
            inherit_option: None,
            set_option: None,
            granted_by: None,
            legacy_group_syntax: true,
        })
    );
}

#[test]
fn parse_alter_group_drop_user_statement() {
    let stmt = parse_statement("alter group regress_group drop user regress_member").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeRoleMembership(RevokeRoleMembershipStatement {
            role_names: vec!["regress_group".into()],
            grantee_names: vec!["regress_member".into()],
            revoke_membership: true,
            admin_option: false,
            inherit_option: false,
            set_option: false,
            cascade: false,
            granted_by: None,
            legacy_group_syntax: true,
        })
    );
}

#[test]
fn parse_alter_role_rename_statement() {
    let stmt = parse_statement("alter role regress_hasprivs rename to regress_tenant").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterRole(AlterRoleStatement {
            role_name: "regress_hasprivs".into(),
            action: AlterRoleAction::Rename {
                new_name: "regress_tenant".into(),
            },
        })
    );

    let stmt = parse_statement("alter table items add primary key (id)").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::PrimaryKey {
                attributes: attrs(),
                columns: vec!["id".into()],
                include_columns: Vec::new(),
                without_overlaps: None,
            },
        })
    );

    let stmt = parse_statement("alter table items add primary key (id, valid_at without overlaps)")
        .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::PrimaryKey {
                attributes: attrs(),
                columns: vec!["id".into(), "valid_at".into()],
                include_columns: Vec::new(),
                without_overlaps: Some("valid_at".into()),
            },
        })
    );

    let stmt = parse_statement("alter table items add unique (note)").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableAddConstraint(AlterTableAddConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint: TableConstraint::Unique {
                attributes: attrs(),
                columns: vec!["note".into()],
                include_columns: Vec::new(),
                without_overlaps: None,
            },
        })
    );
}

#[test]
fn parse_alter_role_option_statement() {
    let stmt =
        parse_statement("alter role regress_tenant noinherit nologin connection limit 7").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterRole(AlterRoleStatement {
            role_name: "regress_tenant".into(),
            action: AlterRoleAction::Options(vec![
                RoleOption::Inherit(false),
                RoleOption::Login(false),
                RoleOption::ConnectionLimit(7),
            ]),
        })
    );

    let stmt = parse_statement("alter role regress_tenant with inherit login").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterRole(AlterRoleStatement {
            role_name: "regress_tenant".into(),
            action: AlterRoleAction::Options(vec![
                RoleOption::Inherit(true),
                RoleOption::Login(true),
            ]),
        })
    );
}

#[test]
fn parse_alter_user_password_statement() {
    let stmt = parse_statement("alter user regress_priv_user2 password 'verysecret'").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterRole(AlterRoleStatement {
            role_name: "regress_priv_user2".into(),
            action: AlterRoleAction::Options(vec![RoleOption::Password(
                Some("verysecret".into(),)
            )]),
        })
    );

    let stmt = parse_statement("alter user regress_priv_user2 with password 'verysecret'").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterRole(AlterRoleStatement {
            role_name: "regress_priv_user2".into(),
            action: AlterRoleAction::Options(vec![RoleOption::Password(
                Some("verysecret".into(),)
            )]),
        })
    );
}

#[test]
fn parse_alter_schema_owner_statement() {
    let stmt = parse_statement("alter schema tenant owner to app_owner").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterSchemaOwner(AlterSchemaOwnerStatement {
            schema_name: "tenant".into(),
            new_owner: "app_owner".into(),
        })
    );
}

#[test]
fn parse_set_session_authorization_statement() {
    let stmt = parse_statement("set session authorization regress_tenant").unwrap();
    assert_eq!(
        stmt,
        Statement::SetSessionAuthorization(SetSessionAuthorizationStatement {
            role_name: "regress_tenant".into(),
        })
    );
}

#[test]
fn parse_regproc_type_name() {
    assert_eq!(
        parse_type_name("regproc").unwrap(),
        RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegProc))
    );
}

#[test]
fn parse_reset_session_authorization_statement() {
    let stmt = parse_statement("reset session authorization").unwrap();
    assert_eq!(
        stmt,
        Statement::ResetSessionAuthorization(ResetSessionAuthorizationStatement)
    );
}

#[test]
fn parse_set_role_statement() {
    let stmt = parse_statement("set role regress_tenant").unwrap();
    assert_eq!(
        stmt,
        Statement::SetRole(SetRoleStatement {
            role_name: Some("regress_tenant".into()),
        })
    );
}

#[test]
fn parse_set_role_none_statement() {
    let stmt = parse_statement("set role none").unwrap();
    assert_eq!(
        stmt,
        Statement::SetRole(SetRoleStatement { role_name: None })
    );
}

#[test]
fn parse_reset_role_statement() {
    let stmt = parse_statement("reset role").unwrap();
    assert_eq!(stmt, Statement::ResetRole(ResetRoleStatement));
}

#[test]
fn parse_comment_on_role_statement() {
    let stmt = parse_statement("comment on role regress_hasprivs is 'some comment'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnRole(CommentOnRoleStatement {
            role_name: "regress_hasprivs".into(),
            comment: Some("some comment".into()),
        })
    );
}

#[test]
fn parse_drop_user_statement() {
    let stmt = parse_statement("drop user if exists regress_login").unwrap();
    assert_eq!(
        stmt,
        Statement::DropRole(DropRoleStatement {
            if_exists: true,
            role_names: vec!["regress_login".into()],
        })
    );
}

#[test]
fn parse_grant_create_on_database_statement() {
    let stmt = parse_statement(
        "grant create on database regression to regress_role_admin with grant option",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::CreateOnDatabase,
            columns: Vec::new(),
            object_names: vec!["regression".into()],
            grantee_names: vec!["regress_role_admin".into()],
            with_grant_option: true,
        })
    );
}

#[test]
fn parse_grant_all_on_schema_statement() {
    let stmt = parse_statement("grant all on schema public to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::AllPrivilegesOnSchema,
            columns: Vec::new(),
            object_names: vec!["public".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_all_on_multiple_schemas_statement() {
    let stmt = parse_statement("grant all on schema alt_nsp1, alt_nsp2 to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::AllPrivilegesOnSchema,
            columns: Vec::new(),
            object_names: vec!["alt_nsp1".into(), "alt_nsp2".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_usage_on_schema_statement() {
    let stmt = parse_statement("grant usage on schema public to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnSchema,
            columns: Vec::new(),
            object_names: vec!["public".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_usage_on_type_statement() {
    let stmt = parse_statement("grant usage on type custom_t to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnType,
            columns: Vec::new(),
            object_names: vec!["custom_t".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_usage_on_domain_statement() {
    let stmt =
        parse_statement("grant usage on domain priv_testdomain1 to regress_priv_user2").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnType,
            columns: Vec::new(),
            object_names: vec!["priv_testdomain1".into()],
            grantee_names: vec!["regress_priv_user2".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_all_on_type_statement() {
    let stmt = parse_statement("grant all privileges on type custom_t to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnType,
            columns: Vec::new(),
            object_names: vec!["custom_t".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_usage_on_language_statement() {
    let stmt = parse_statement("grant usage on language sql to regress_priv_user1").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnLanguage,
            columns: Vec::new(),
            object_names: vec!["sql".into()],
            grantee_names: vec!["regress_priv_user1".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_usage_on_foreign_data_wrapper_statement() {
    let stmt =
        parse_statement("grant usage on foreign data wrapper foo to regress_test_role").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnForeignDataWrapper,
            columns: Vec::new(),
            object_names: vec!["foo".into()],
            grantee_names: vec!["regress_test_role".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_usage_on_foreign_server_with_grant_option_statement() {
    let stmt =
        parse_statement("grant usage on foreign server s1 to regress_test_role with grant option")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnForeignServer,
            columns: Vec::new(),
            object_names: vec!["s1".into()],
            grantee_names: vec!["regress_test_role".into()],
            with_grant_option: true,
        })
    );
}

#[test]
fn parse_grant_select_on_table_statement() {
    let stmt = parse_statement("grant select on uaccount to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::SelectOnTable,
            columns: Vec::new(),
            object_names: vec!["uaccount".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_column_select_and_insert_on_table_statements() {
    let stmt =
        parse_statement("grant select (a) on key_desc_1 to regress_insert_other_user").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::SelectOnTable,
            columns: vec!["a".into()],
            object_names: vec!["key_desc_1".into()],
            grantee_names: vec!["regress_insert_other_user".into()],
            with_grant_option: false,
        })
    );

    let stmt = parse_statement("grant insert (two) on atest5 to regress_priv_user4").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::InsertOnTable,
            columns: vec!["two".into()],
            object_names: vec!["atest5".into()],
            grantee_names: vec!["regress_priv_user4".into()],
            with_grant_option: false,
        })
    );

    let stmt = parse_statement("grant select (one,two) on atest6 to regress_priv_user4").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::SelectOnTable,
            columns: vec!["one".into(), "two".into()],
            object_names: vec!["atest6".into()],
            grantee_names: vec!["regress_priv_user4".into()],
            with_grant_option: false,
        })
    );

    let stmt = parse_statement("grant insert on key_desc to regress_insert_other_user").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::InsertOnTable,
            columns: Vec::new(),
            object_names: vec!["key_desc".into()],
            grantee_names: vec!["regress_insert_other_user".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_mixed_column_privileges_on_table_statement() {
    let stmt = parse_statement(
        "grant select (one), insert (two), update (three) on atest5 to regress_priv_user4",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::TableColumnPrivileges(vec![
                GrantTableColumnPrivilege {
                    privilege: GrantObjectPrivilege::SelectOnTable,
                    columns: vec!["one".into()],
                },
                GrantTableColumnPrivilege {
                    privilege: GrantObjectPrivilege::InsertOnTable,
                    columns: vec!["two".into()],
                },
                GrantTableColumnPrivilege {
                    privilege: GrantObjectPrivilege::UpdateOnTable,
                    columns: vec!["three".into()],
                },
            ]),
            columns: Vec::new(),
            object_names: vec!["atest5".into()],
            grantee_names: vec!["regress_priv_user4".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_on_table_to_group_statement() {
    let stmt = parse_statement("grant delete on atest3 to group regress_priv_group2").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::DeleteOnTable,
            columns: Vec::new(),
            object_names: vec!["atest3".into()],
            grantee_names: vec!["regress_priv_group2".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_all_on_table_statement() {
    let stmt = parse_statement("grant all on uaccount to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::AllPrivilegesOnTable,
            columns: Vec::new(),
            object_names: vec!["uaccount".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_update_on_table_statement() {
    let stmt = parse_statement("grant update on atest2 to regress_priv_user3").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::UpdateOnTable,
            columns: Vec::new(),
            object_names: vec!["atest2".into()],
            grantee_names: vec!["regress_priv_user3".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_multiple_table_privileges_statement() {
    let stmt =
        parse_statement("grant select, update on table grantor_test3 to regress_grantor3").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::TablePrivileges("rw".into()),
            columns: Vec::new(),
            object_names: vec!["grantor_test3".into()],
            grantee_names: vec!["regress_grantor3".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_revoke_all_on_table_statement() {
    let stmt = parse_statement("revoke all on key_desc from regress_insert_other_user").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::AllPrivilegesOnTable,
            columns: Vec::new(),
            object_names: vec!["key_desc".into()],
            grantee_names: vec!["regress_insert_other_user".into()],
            cascade: false,
        })
    );

    let stmt = parse_statement("revoke select on brtrigpartcon from regress_coldesc_role").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::SelectOnTable,
            columns: Vec::new(),
            object_names: vec!["brtrigpartcon".into()],
            grantee_names: vec!["regress_coldesc_role".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_grant_execute_on_function_statement() {
    let stmt = parse_statement("grant execute on function f_leak(text) to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::ExecuteOnFunction,
            columns: Vec::new(),
            object_names: vec!["f_leak(text)".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_all_on_function_statement() {
    let stmt =
        parse_statement("grant all privileges on function priv_testfunc1(int) to public").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::ExecuteOnFunction,
            columns: Vec::new(),
            object_names: vec!["priv_testfunc1(int)".into()],
            grantee_names: vec!["public".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_grant_execute_on_procedure_statement() {
    let stmt =
        parse_statement("grant execute on procedure ptest1(text) to regress_cp_user1").unwrap();
    assert_eq!(
        stmt,
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::ExecuteOnProcedure,
            columns: Vec::new(),
            object_names: vec!["ptest1(text)".into()],
            grantee_names: vec!["regress_cp_user1".into()],
            with_grant_option: false,
        })
    );
}

#[test]
fn parse_create_tablespace_statement() {
    let stmt = parse_statement("create tablespace regress_tblspace location ''").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateTablespace(CreateTablespaceStatement {
            tablespace_name: "regress_tblspace".into(),
            location: "".into(),
        })
    );
}

#[test]
fn parse_revoke_all_privileges_on_table_from_public_statement() {
    let stmt = parse_statement("revoke all privileges on tenant_table from public").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::AllPrivilegesOnTable,
            columns: Vec::new(),
            object_names: vec!["tenant_table".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_delete_on_table_statement() {
    let stmt = parse_statement("revoke delete on atest3 from regress_priv_group2").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::DeleteOnTable,
            columns: Vec::new(),
            object_names: vec!["atest3".into()],
            grantee_names: vec!["regress_priv_group2".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_usage_on_schema_statement() {
    let stmt = parse_statement("revoke usage on schema public from public").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnSchema,
            columns: Vec::new(),
            object_names: vec!["public".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_usage_on_type_statement() {
    let stmt = parse_statement("revoke usage on type custom_t from public").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnType,
            columns: Vec::new(),
            object_names: vec!["custom_t".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_usage_on_domain_statement() {
    let stmt = parse_statement("revoke usage on domain priv_testdomain1 from public").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnType,
            columns: Vec::new(),
            object_names: vec!["priv_testdomain1".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_all_on_domain_statement() {
    let stmt = parse_statement("revoke all on domain priv_testdomain1 from public").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::UsageOnType,
            columns: Vec::new(),
            object_names: vec!["priv_testdomain1".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_usage_on_language_statement() {
    let stmt = parse_statement("revoke all privileges on language sql from public").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::AllPrivilegesOnLanguage,
            columns: Vec::new(),
            object_names: vec!["sql".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_column_update_from_group_statement() {
    let stmt =
        parse_statement("revoke update (three) on atest5 from group regress_priv_group2").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::UpdateOnTable,
            columns: vec!["three".into()],
            object_names: vec!["atest5".into()],
            grantee_names: vec!["regress_priv_group2".into()],
            cascade: false,
        })
    );

    let stmt = parse_statement("revoke all (one,two) on atest5 from regress_priv_user4").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::TablePrivileges("arwx".into()),
            columns: vec!["one".into(), "two".into()],
            object_names: vec!["atest5".into()],
            grantee_names: vec!["regress_priv_user4".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_all_on_foreign_data_wrapper_statement() {
    let stmt =
        parse_statement("revoke all on foreign data wrapper foo from regress_test_role").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::AllPrivilegesOnForeignDataWrapper,
            columns: Vec::new(),
            object_names: vec!["foo".into()],
            grantee_names: vec!["regress_test_role".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_execute_on_function_statement() {
    let stmt = parse_statement("revoke execute on function f_leak(text) from public").unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::ExecuteOnFunction,
            columns: Vec::new(),
            object_names: vec!["f_leak(text)".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_revoke_all_on_function_statement() {
    let stmt = parse_statement("revoke all privileges on function priv_testfunc1(int) from public")
        .unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeObject(RevokeObjectStatement {
            privilege: GrantObjectPrivilege::ExecuteOnFunction,
            columns: Vec::new(),
            object_names: vec!["priv_testfunc1(int)".into()],
            grantee_names: vec!["public".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_grant_role_membership_with_options_statement() {
    let stmt =
        parse_statement("grant regress_tenant2 to regress_createrole with inherit true, set false")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::GrantRoleMembership(GrantRoleMembershipStatement {
            role_names: vec!["regress_tenant2".into()],
            grantee_names: vec!["regress_createrole".into()],
            admin_option: false,
            admin_option_specified: false,
            inherit_option: Some(true),
            set_option: Some(false),
            granted_by: None,
            legacy_group_syntax: false,
        })
    );
}

#[test]
fn parse_grant_role_membership_granted_by_statement() {
    let stmt = parse_statement(
        "grant regress_tenant2 to regress_createrole with admin option granted by current_role",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::GrantRoleMembership(GrantRoleMembershipStatement {
            role_names: vec!["regress_tenant2".into()],
            grantee_names: vec!["regress_createrole".into()],
            admin_option: true,
            admin_option_specified: true,
            inherit_option: None,
            set_option: None,
            granted_by: Some(RoleGrantorSpec::CurrentRole),
            legacy_group_syntax: false,
        })
    );
}

#[test]
fn parse_revoke_role_membership_option_statement() {
    let stmt = parse_statement("revoke inherit option for regress_tenant2 from regress_createrole")
        .unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeRoleMembership(RevokeRoleMembershipStatement {
            role_names: vec!["regress_tenant2".into()],
            grantee_names: vec!["regress_createrole".into()],
            revoke_membership: false,
            admin_option: false,
            inherit_option: true,
            set_option: false,
            cascade: false,
            granted_by: None,
            legacy_group_syntax: false,
        })
    );
}

#[test]
fn parse_revoke_role_membership_granted_by_statement() {
    let stmt = parse_statement(
        "revoke inherit option for regress_tenant2 from regress_createrole granted by current_user",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeRoleMembership(RevokeRoleMembershipStatement {
            role_names: vec!["regress_tenant2".into()],
            grantee_names: vec!["regress_createrole".into()],
            revoke_membership: false,
            admin_option: false,
            inherit_option: true,
            set_option: false,
            cascade: false,
            granted_by: Some(RoleGrantorSpec::CurrentUser),
            legacy_group_syntax: false,
        })
    );
}

#[test]
fn parse_revoke_role_membership_admin_option_granted_by_cascade_statement() {
    let stmt = parse_statement(
        "revoke admin option for regress_tenant2 from regress_createrole granted by regress_admin cascade",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeRoleMembership(RevokeRoleMembershipStatement {
            role_names: vec!["regress_tenant2".into()],
            grantee_names: vec!["regress_createrole".into()],
            revoke_membership: false,
            admin_option: true,
            inherit_option: false,
            set_option: false,
            cascade: true,
            granted_by: Some(RoleGrantorSpec::RoleName("regress_admin".into())),
            legacy_group_syntax: false,
        })
    );
}

#[test]
fn parse_plain_revoke_role_membership_granted_by_cascade_statement() {
    let stmt = parse_statement(
        "revoke regress_tenant2 from regress_createrole granted by regress_admin cascade",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::RevokeRoleMembership(RevokeRoleMembershipStatement {
            role_names: vec!["regress_tenant2".into()],
            grantee_names: vec!["regress_createrole".into()],
            revoke_membership: true,
            admin_option: false,
            inherit_option: false,
            set_option: false,
            cascade: true,
            granted_by: Some(RoleGrantorSpec::RoleName("regress_admin".into())),
            legacy_group_syntax: false,
        })
    );
}

#[test]
fn parse_drop_owned_statement() {
    let stmt = parse_statement("drop owned by regress_tenant, regress_tenant2").unwrap();
    assert_eq!(
        stmt,
        Statement::DropOwned(DropOwnedStatement {
            role_names: vec!["regress_tenant".into(), "regress_tenant2".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_drop_owned_cascade_statement() {
    let stmt = parse_statement("drop owned by regress_tenant cascade").unwrap();
    assert_eq!(
        stmt,
        Statement::DropOwned(DropOwnedStatement {
            role_names: vec!["regress_tenant".into()],
            cascade: true,
        })
    );
}

#[test]
fn parse_reassign_owned_statement() {
    let stmt =
        parse_statement("reassign owned by regress_tenant, regress_tenant2 to regress_createrole")
            .unwrap();
    assert_eq!(
        stmt,
        Statement::ReassignOwned(ReassignOwnedStatement {
            old_roles: vec!["regress_tenant".into(), "regress_tenant2".into()],
            new_role: "regress_createrole".into(),
        })
    );
}

#[test]
fn parse_numeric_type_with_negative_scale() {
    assert_eq!(
        parse_type_name("numeric(3, -6)").unwrap(),
        SqlType::with_numeric_precision_scale(3, -6)
    );
}

#[test]
fn parse_record_and_named_type_names() {
    assert_eq!(parse_type_name("record").unwrap(), RawTypeName::Record);
    assert_eq!(
        parse_type_name("widget_row").unwrap(),
        RawTypeName::Named {
            name: "widget_row".into(),
            array_bounds: 0,
        }
    );
    assert_eq!(
        parse_type_name("widget(42,13)").unwrap(),
        RawTypeName::Named {
            name: "widget(42,13)".into(),
            array_bounds: 0,
        }
    );
    assert_eq!(
        parse_type_name("\"int4\"").unwrap(),
        RawTypeName::Named {
            name: "int4".into(),
            array_bounds: 0,
        }
    );
}

#[test]
fn parse_named_array_type_name() {
    assert_eq!(
        parse_type_name("vc4[]").unwrap(),
        RawTypeName::Named {
            name: "vc4".into(),
            array_bounds: 1,
        }
    );
}

#[test]
fn parse_create_table_preserves_named_array_column_type() {
    let Statement::CreateTable(create) =
        parse_statement("create table darray (f3 insert_test_domain, f4 insert_test_domain[])")
            .unwrap()
    else {
        panic!("expected create table");
    };
    let CreateTableElement::Column(column) = &create.elements[1] else {
        panic!("expected column");
    };
    assert_eq!(
        column.ty,
        RawTypeName::Named {
            name: "insert_test_domain".into(),
            array_bounds: 1,
        }
    );
}

#[test]
fn parse_do_statement_defaults_to_plpgsql() {
    let stmt = parse_statement("do $$ begin null; end $$").unwrap();
    assert_eq!(
        stmt,
        Statement::Do(DoStatement {
            language: None,
            code: " begin null; end ".into(),
        })
    );
}

#[test]
fn parse_do_statement_with_explicit_language() {
    let stmt = parse_statement("do language plpgsql $$ begin null; end $$").unwrap();
    assert_eq!(
        stmt,
        Statement::Do(DoStatement {
            language: Some("plpgsql".into()),
            code: " begin null; end ".into(),
        })
    );
}

#[test]
fn parse_create_cast_with_function_and_implicit_context() {
    let stmt = parse_statement(
        "create cast (int4 as casttesttype) with function pg_catalog.int4_casttesttype(int4) as implicit",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateCast(CreateCastStatement {
            source_type: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
            target_type: RawTypeName::Named {
                name: "casttesttype".into(),
                array_bounds: 0,
            },
            method: CreateCastMethod::Function {
                schema_name: Some("pg_catalog".into()),
                function_name: "int4_casttesttype".into(),
                arg_types: vec![RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4))],
            },
            context: CastContext::Implicit,
        })
    );
}

#[test]
fn parse_create_cast_without_function_and_assignment_context() {
    let stmt = parse_statement("create cast (text as casttesttype) without function as assignment")
        .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateCast(CreateCastStatement {
            source_type: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
            target_type: RawTypeName::Named {
                name: "casttesttype".into(),
                array_bounds: 0,
            },
            method: CreateCastMethod::WithoutFunction,
            context: CastContext::Assignment,
        })
    );
}

#[test]
fn parse_create_cast_with_inout_defaults_to_explicit_context() {
    let stmt = parse_statement("create cast (int4 as casttesttype) with inout").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateCast(CreateCastStatement {
            source_type: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
            target_type: RawTypeName::Named {
                name: "casttesttype".into(),
                array_bounds: 0,
            },
            method: CreateCastMethod::InOut,
            context: CastContext::Explicit,
        })
    );
}

#[test]
fn parse_drop_cast_if_exists_cascade() {
    let stmt = parse_statement("drop cast if exists (int4 as casttesttype) cascade").unwrap();
    assert_eq!(
        stmt,
        Statement::DropCast(DropCastStatement {
            if_exists: true,
            source_type: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
            target_type: RawTypeName::Named {
                name: "casttesttype".into(),
                array_bounds: 0,
            },
            cascade: true,
        })
    );
}

#[test]
fn parse_create_function_statement_with_returns_table() {
    let stmt = parse_statement(
        "create function public.pair_rows(x int4) returns table(a int4, b text) language plpgsql as $$ begin return next; end $$",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            schema_name: Some("public".into()),
            function_name: "pair_rows".into(),
            replace_existing: false,
            cost: None,
            support: None,
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: Some("x".into()),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                type_position: None,
                default_expr: None,
                variadic: false,
            }],
            return_spec: CreateFunctionReturnSpec::Table(vec![
                CreateFunctionTableColumn {
                    name: "a".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                },
                CreateFunctionTableColumn {
                    name: "b".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                },
            ]),
            strict: false,
            leakproof: false,
            volatility: FunctionVolatility::Volatile,
            parallel: FunctionParallel::Unsafe,
            language: "plpgsql".into(),
            body: " begin return next; end ".into(),
            link_symbol: None,
            config: Vec::new(),
        })
    );
}

#[test]
fn parse_create_or_replace_function_statement_with_returns_table() {
    let stmt = parse_statement(
        "create or replace function public.pair_rows(x int4) returns table(a int4, b text) language plpgsql as $$ begin return next; end $$",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            schema_name: Some("public".into()),
            function_name: "pair_rows".into(),
            replace_existing: true,
            cost: None,
            support: None,
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: Some("x".into()),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                type_position: None,
                default_expr: None,
                variadic: false,
            }],
            return_spec: CreateFunctionReturnSpec::Table(vec![
                CreateFunctionTableColumn {
                    name: "a".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                },
                CreateFunctionTableColumn {
                    name: "b".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                },
            ]),
            strict: false,
            leakproof: false,
            volatility: FunctionVolatility::Volatile,
            parallel: FunctionParallel::Unsafe,
            language: "plpgsql".into(),
            body: " begin return next; end ".into(),
            link_symbol: None,
            config: Vec::new(),
        })
    );
}

#[test]
fn parse_create_trigger_statement_with_when_and_update_of() {
    let stmt = parse_statement(
        "create or replace trigger audit_row before insert or update of name, note on public.people for each row when (new.name is not null) execute function public.audit_people('x', arg2)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateTrigger(CreateTriggerStatement {
            replace_existing: true,
            trigger_name: "audit_row".into(),
            schema_name: Some("public".into()),
            table_name: "people".into(),
            timing: TriggerTiming::Before,
            level: TriggerLevel::Row,
            events: vec![
                TriggerEventSpec {
                    event: TriggerEvent::Insert,
                    update_columns: Vec::new(),
                },
                TriggerEventSpec {
                    event: TriggerEvent::Update,
                    update_columns: vec!["name".into(), "note".into()],
                },
            ],
            referencing: Vec::new(),
            when_clause_sql: Some("new.name is not null".into()),
            function_schema_name: Some("public".into()),
            function_name: "audit_people".into(),
            func_args: vec!["x".into(), "arg2".into()],
        })
    );
}

#[test]
fn parse_create_instead_of_trigger_statement() {
    let stmt = parse_statement(
        "create trigger audit_row instead of insert or update on public.people_view for each row execute function public.audit_people()",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateTrigger(CreateTriggerStatement {
            replace_existing: false,
            trigger_name: "audit_row".into(),
            schema_name: Some("public".into()),
            table_name: "people_view".into(),
            timing: TriggerTiming::Instead,
            level: TriggerLevel::Row,
            events: vec![
                TriggerEventSpec {
                    event: TriggerEvent::Insert,
                    update_columns: Vec::new(),
                },
                TriggerEventSpec {
                    event: TriggerEvent::Update,
                    update_columns: Vec::new(),
                },
            ],
            referencing: Vec::new(),
            when_clause_sql: None,
            function_schema_name: Some("public".into()),
            function_name: "audit_people".into(),
            func_args: Vec::new(),
        })
    );
}

#[test]
fn parse_create_trigger_statement_for_statement_without_each() {
    let stmt = parse_statement(
        "create trigger audit_stmt after insert on public.people for statement execute function public.audit_people()",
    )
    .unwrap();
    match stmt {
        Statement::CreateTrigger(CreateTriggerStatement { level, .. }) => {
            assert_eq!(level, TriggerLevel::Statement);
        }
        other => panic!("expected create trigger, got {other:?}"),
    }
}

#[test]
fn parse_alter_table_disable_trigger_user() {
    let stmt = parse_statement("alter table only public.people disable trigger user").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableTriggerState(AlterTableTriggerStateStatement {
            if_exists: false,
            only: true,
            table_name: "public.people".into(),
            target: AlterTableTriggerTarget::User,
            mode: AlterTableTriggerMode::Disable,
        })
    );
}

#[test]
fn parse_alter_table_disable_trigger_all() {
    let stmt = parse_statement("alter table people disable trigger all").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableTriggerState(AlterTableTriggerStateStatement {
            if_exists: false,
            only: false,
            table_name: "people".into(),
            target: AlterTableTriggerTarget::All,
            mode: AlterTableTriggerMode::Disable,
        })
    );
}

#[test]
fn parse_alter_table_enable_always_trigger_name() {
    let stmt =
        parse_statement("alter table if exists people enable always trigger audit_row").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableTriggerState(AlterTableTriggerStateStatement {
            if_exists: true,
            only: false,
            table_name: "people".into(),
            target: AlterTableTriggerTarget::Named("audit_row".into()),
            mode: AlterTableTriggerMode::EnableAlways,
        })
    );
}

#[test]
fn parse_alter_trigger_rename_statement() {
    let stmt =
        parse_statement("alter trigger audit_row on public.people rename to audit_row_v2").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTriggerRename(AlterTriggerRenameStatement {
            trigger_name: "audit_row".into(),
            schema_name: Some("public".into()),
            table_name: "people".into(),
            new_trigger_name: "audit_row_v2".into(),
        })
    );
}

#[test]
fn parse_create_trigger_rejects_duplicate_events() {
    let err = parse_statement(
        "create trigger dup_evt before insert or insert on people execute function audit_people()",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::DetailedError { message, sqlstate, .. }
            if message == "duplicate trigger events specified at or near \"ON\""
                && sqlstate == "42601"
    ));
}

#[test]
fn parse_create_trigger_rejects_insert_of_column_list() {
    let err = parse_statement(
        "create trigger bad_insert before insert of name on people execute function audit_people()",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UnexpectedToken { actual, .. }
            if actual == "syntax error at or near \"OF\""
    ));
}

#[test]
fn parse_enable_replica_trigger_requires_name() {
    let err = parse_statement("alter table people enable replica trigger all").unwrap_err();
    assert!(
        matches!(err, ParseError::UnexpectedToken { expected, .. } if expected == "trigger name")
    );
}

#[test]
fn parse_set_transaction_isolation_level_serializable() {
    assert_eq!(
        parse_statement("set transaction isolation level serializable").unwrap(),
        Statement::SetTransaction(SetTransactionStatement {
            scope: SetTransactionScope::Transaction,
            options: TransactionOptions {
                isolation_level: Some(TransactionIsolationLevel::Serializable),
                ..TransactionOptions::default()
            },
        })
    );
}

#[test]
fn parse_set_session_transaction_characteristics() {
    assert_eq!(
        parse_statement(
            "set session characteristics as transaction isolation level repeatable read",
        )
        .unwrap(),
        Statement::SetTransaction(SetTransactionStatement {
            scope: SetTransactionScope::SessionCharacteristics,
            options: TransactionOptions {
                isolation_level: Some(TransactionIsolationLevel::RepeatableRead),
                ..TransactionOptions::default()
            },
        })
    );
}

#[test]
fn parse_transaction_mode_options() {
    assert_eq!(
        parse_statement("begin isolation level repeatable read, read only, not deferrable")
            .unwrap(),
        Statement::Begin(TransactionOptions {
            isolation_level: Some(TransactionIsolationLevel::RepeatableRead),
            read_only: Some(true),
            deferrable: Some(false),
        })
    );
}

#[test]
fn parse_show_transaction_isolation_level() {
    assert_eq!(
        parse_statement("show transaction isolation level").unwrap(),
        Statement::Show(ShowStatement {
            name: "transaction_isolation".into(),
        })
    );
}

#[test]
fn parse_drop_trigger_statement_on_table() {
    let stmt =
        parse_statement("drop trigger if exists audit_row on public.people cascade").unwrap();
    assert_eq!(
        stmt,
        Statement::DropTrigger(DropTriggerStatement {
            if_exists: true,
            trigger_name: "audit_row".into(),
            schema_name: Some("public".into()),
            table_name: "people".into(),
            cascade: true,
        })
    );
}

#[test]
fn parse_create_trigger_statement_with_referencing_and_truncate() {
    let stmt = parse_statement(
        "create trigger audit_stmt after truncate on people referencing old table as old_rows execute function bad()",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateTrigger(CreateTriggerStatement {
            replace_existing: false,
            trigger_name: "audit_stmt".into(),
            schema_name: None,
            table_name: "people".into(),
            timing: TriggerTiming::After,
            level: TriggerLevel::Statement,
            events: vec![TriggerEventSpec {
                event: TriggerEvent::Truncate,
                update_columns: Vec::new(),
            }],
            referencing: vec![TriggerReferencingSpec {
                is_new: false,
                is_table: true,
                name: "old_rows".into(),
            }],
            when_clause_sql: None,
            function_schema_name: None,
            function_name: "bad".into(),
            func_args: Vec::new(),
        })
    );
}

#[test]
fn parse_create_trigger_statement_with_truncate_event() {
    let stmt = parse_statement(
        "create trigger bad_truncate before truncate on people for each statement execute function bad()",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateTrigger(CreateTriggerStatement {
            timing: TriggerTiming::Before,
            level: TriggerLevel::Statement,
            events,
            ..
        }) if events == vec![TriggerEventSpec {
            event: TriggerEvent::Truncate,
            update_columns: Vec::new(),
        }]
    ));
}

#[test]
fn parse_expression_entrypoint_reuses_sql_expression_grammar() {
    let expr = parse_expr("1 + 2 * 3").unwrap();
    assert!(matches!(expr, SqlExpr::Add(_, _)));

    let expr = parse_expr("a <% b").unwrap();
    assert!(matches!(expr, SqlExpr::BinaryOperator { ref op, .. } if op == "<%"));
}

#[test]
fn parse_create_function_statement_with_unnamed_args() {
    let stmt = parse_statement(
        "create function binary_coercible(oid, oid) returns bool language plpgsql as $$ begin return true; end $$",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            schema_name: None,
            function_name: "binary_coercible".into(),
            replace_existing: false,
            cost: None,
            support: None,
            args: vec![
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
                    type_position: None,
                    default_expr: None,
                    variadic: false,
                },
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
                    type_position: None,
                    default_expr: None,
                    variadic: false,
                }
            ],
            return_spec: CreateFunctionReturnSpec::Type {
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool)),
                setof: false,
            },
            strict: false,
            leakproof: false,
            volatility: FunctionVolatility::Volatile,
            parallel: FunctionParallel::Unsafe,
            language: "plpgsql".into(),
            body: " begin return true; end ".into(),
            link_symbol: None,
            config: Vec::new(),
        })
    );
}

#[test]
fn parse_create_function_statement_with_variadic_arg() {
    let stmt = parse_statement(
        "create function least_accum(variadic items anyarray) returns anyelement language sql as $$ select $1[1] $$",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            schema_name: None,
            function_name: "least_accum".into(),
            replace_existing: false,
            cost: None,
            support: None,
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: Some("items".into()),
                ty: RawTypeName::Named {
                    name: "anyarray".into(),
                    array_bounds: 0,
                },
                type_position: Some(44),
                default_expr: None,
                variadic: true,
            }],
            return_spec: CreateFunctionReturnSpec::Type {
                ty: RawTypeName::Named {
                    name: "anyelement".into(),
                    array_bounds: 0,
                },
                setof: false,
            },
            strict: false,
            leakproof: false,
            volatility: FunctionVolatility::Volatile,
            parallel: FunctionParallel::Unsafe,
            language: "sql".into(),
            body: " select $1[1] ".into(),
            link_symbol: None,
            config: Vec::new(),
        })
    );
}

#[test]
fn parse_create_function_statement_with_pg_clauses_and_link_symbol() {
    let stmt = parse_statement(
        "create function binary_coercible(oid, oid) returns bool as 'regress', 'binary_coercible' language c strict stable parallel safe",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            schema_name: None,
            function_name: "binary_coercible".into(),
            replace_existing: false,
            cost: None,
            support: None,
            args: vec![
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
                    type_position: None,
                    default_expr: None,
                    variadic: false,
                },
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
                    type_position: None,
                    default_expr: None,
                    variadic: false,
                }
            ],
            return_spec: CreateFunctionReturnSpec::Type {
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool)),
                setof: false,
            },
            strict: true,
            leakproof: false,
            volatility: FunctionVolatility::Stable,
            parallel: FunctionParallel::Safe,
            language: "c".into(),
            body: "regress".into(),
            link_symbol: Some("binary_coercible".into()),
            config: Vec::new(),
        })
    );
}

#[test]
fn parse_create_function_statement_with_sql_return_shorthand() {
    let stmt = parse_statement(
        "create function fipshash(bytea) returns text strict immutable parallel safe leakproof return substr(encode(sha256($1), 'hex'), 1, 32)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            schema_name: None,
            function_name: "fipshash".into(),
            replace_existing: false,
            cost: None,
            support: None,
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: None,
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bytea)),
                type_position: None,
                default_expr: None,
                variadic: false,
            }],
            return_spec: CreateFunctionReturnSpec::Type {
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                setof: false,
            },
            strict: true,
            leakproof: true,
            volatility: FunctionVolatility::Immutable,
            parallel: FunctionParallel::Safe,
            language: "sql".into(),
            body: "select substr(encode(sha256($1), 'hex'), 1, 32)".into(),
            link_symbol: None,
            config: Vec::new(),
        })
    );
}

#[test]
fn parse_create_function_statement_with_cost_clause() {
    let stmt = parse_statement(
        "create or replace function f_leak(text) returns bool cost 0.0000001 language plpgsql as $$ begin return true; end $$",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            schema_name: None,
            function_name: "f_leak".into(),
            replace_existing: true,
            cost: Some("0.0000001".into()),
            support: None,
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: None,
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                type_position: None,
                default_expr: None,
                variadic: false,
            }],
            return_spec: CreateFunctionReturnSpec::Type {
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool)),
                setof: false,
            },
            strict: false,
            leakproof: false,
            volatility: FunctionVolatility::Volatile,
            parallel: FunctionParallel::Unsafe,
            language: "plpgsql".into(),
            body: " begin return true; end ".into(),
            link_symbol: None,
            config: Vec::new(),
        })
    );
}

#[test]
fn parse_create_function_statement_with_support_clause() {
    let stmt = parse_statement(
        "create function my_gen_series(int, int) returns setof integer language internal strict immutable parallel safe as $$generate_series_int4$$ support pg_catalog.test_support_func",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateFunction(CreateFunctionStatement {
            function_name,
            support: Some(RoutineSignature {
                schema_name: Some(schema_name),
                routine_name,
                arg_types,
            }),
            ..
        }) if function_name == "my_gen_series"
            && schema_name == "pg_catalog"
            && routine_name == "test_support_func"
            && arg_types.is_empty()
    ));
}

#[test]
fn parse_drop_function_statement_with_signature() {
    let stmt = parse_statement("drop function public.p2text(p2)").unwrap();
    assert_eq!(
        stmt,
        Statement::DropFunction(DropFunctionStatement {
            if_exists: false,
            schema_name: Some("public".into()),
            function_name: "p2text".into(),
            arg_types: vec!["p2".into()],
            cascade: false,
        })
    );
}

#[test]
fn parse_drop_function_statement_without_signature() {
    let stmt = parse_statement("drop function if exists public.p2text cascade").unwrap();
    assert_eq!(
        stmt,
        Statement::DropFunction(DropFunctionStatement {
            if_exists: true,
            schema_name: Some("public".into()),
            function_name: "p2text".into(),
            arg_types: vec![],
            cascade: true,
        })
    );
}

#[test]
fn parse_call_statement_with_named_and_positional_args() {
    let stmt = parse_statement("call public.ptest5(10, b => 'Hello')").unwrap();
    assert_eq!(
        stmt,
        Statement::Call(CallStatement {
            schema_name: Some("public".into()),
            procedure_name: "ptest5".into(),
            args: SqlCallArgs::Args(vec![
                SqlFunctionArg::positional(SqlExpr::IntegerLiteral("10".into())),
                SqlFunctionArg {
                    name: Some("b".into()),
                    value: SqlExpr::Const(Value::Text("Hello".into())),
                },
            ]),
            raw_arg_sql: vec!["10".into(), "'Hello'".into()],
        })
    );
}

#[test]
fn parse_create_procedure_statement() {
    let stmt = parse_statement(
        "create or replace procedure public.ptest1(inout x int4, y text default 'a') language sql as $$ insert into cp_test values (1, y) $$",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateProcedure(CreateProcedureStatement {
            schema_name: Some("public".into()),
            procedure_name: "ptest1".into(),
            replace_existing: true,
            args: vec![
                CreateFunctionArg {
                    mode: FunctionArgMode::InOut,
                    name: Some("x".into()),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                    type_position: None,
                    default_expr: None,
                    variadic: false,
                },
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: Some("y".into()),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                    type_position: None,
                    default_expr: Some("'a'".into()),
                    variadic: false,
                },
            ],
            strict: false,
            volatility: FunctionVolatility::Volatile,
            language: "sql".into(),
            body: " insert into cp_test values (1, y) ".into(),
            sql_standard_body: false,
        })
    );
}

#[test]
fn parse_create_procedure_sql_standard_body() {
    let stmt = parse_statement(
        "create procedure ptest1s(x text) language sql begin atomic insert into cp_test values (1, x); end",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateProcedure(CreateProcedureStatement {
            procedure_name,
            sql_standard_body: true,
            ..
        }) if procedure_name == "ptest1s"
    ));
}

#[test]
fn parse_drop_and_alter_procedure_statements() {
    assert_eq!(
        parse_statement("drop procedure if exists public.ptest1(text) cascade").unwrap(),
        Statement::DropProcedure(DropProcedureStatement {
            if_exists: true,
            procedures: vec![DropRoutineItem {
                schema_name: Some("public".into()),
                routine_name: "ptest1".into(),
                arg_types: vec!["text".into()],
            }],
            cascade: true,
        })
    );
    assert_eq!(
        parse_statement("alter procedure public.ptest1(text) strict").unwrap(),
        Statement::AlterRoutine(AlterRoutineStatement {
            kind: RoutineKind::Procedure,
            signature: RoutineSignature {
                schema_name: Some("public".into()),
                routine_name: "ptest1".into(),
                arg_types: vec!["text".into()],
            },
            action: AlterRoutineAction::Options(vec![AlterRoutineOption::Strict(true)]),
        })
    );
    assert_eq!(
        parse_statement("alter aggregate public.atest1(integer) owner to app_owner").unwrap(),
        Statement::AlterRoutine(AlterRoutineStatement {
            kind: RoutineKind::Aggregate,
            signature: RoutineSignature {
                schema_name: Some("public".into()),
                routine_name: "atest1".into(),
                arg_types: vec!["integer".into()],
            },
            action: AlterRoutineAction::OwnerTo {
                new_owner: "app_owner".into()
            },
        })
    );
    assert_eq!(
        parse_statement("alter function my_int_eq(int, int) support test_support_func").unwrap(),
        Statement::AlterRoutine(AlterRoutineStatement {
            kind: RoutineKind::Function,
            signature: RoutineSignature {
                schema_name: None,
                routine_name: "my_int_eq".into(),
                arg_types: vec!["int".into(), "int".into()],
            },
            action: AlterRoutineAction::Options(vec![AlterRoutineOption::Support(
                RoutineSignature {
                    schema_name: None,
                    routine_name: "test_support_func".into(),
                    arg_types: Vec::new(),
                }
            )]),
        })
    );
    assert_eq!(
        parse_statement("drop routine if exists public.ptest1(text) cascade").unwrap(),
        Statement::DropRoutine(DropProcedureStatement {
            if_exists: true,
            procedures: vec![DropRoutineItem {
                schema_name: Some("public".into()),
                routine_name: "ptest1".into(),
                arg_types: vec!["text".into()],
            }],
            cascade: true,
        })
    );
}

#[test]
fn parse_create_aggregate_statement_with_plain_signature() {
    let stmt = parse_statement(
        "create aggregate newavg(int4) (sfunc = int4_avg_accum, stype = _int8, finalfunc = int8_avg, initcond = '{0,0}', parallel = safe)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateAggregate(CreateAggregateStatement {
            schema_name: None,
            aggregate_name: "newavg".into(),
            replace_existing: false,
            signature: aggregate_signature(vec![aggregate_signature_arg(AggregateArgType::Type(
                RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
            ))]),
            hypothetical: false,
            sfunc_name: "int4_avg_accum".into(),
            stype: RawTypeName::Named {
                name: "_int8".into(),
                array_bounds: 0,
            },
            finalfunc_name: Some("int8_avg".into()),
            initcond: Some("{0,0}".into()),
            parallel: Some(FunctionParallel::Safe),
            transspace: 0,
            combinefunc_name: None,
            serialfunc_name: None,
            deserialfunc_name: None,
            finalfunc_extra: false,
            finalfunc_modify: 'r',
            mstype: None,
            msfunc_name: None,
            minvfunc_name: None,
            mfinalfunc_name: None,
            minitcond: None,
            mtransspace: 0,
            mfinalfunc_extra: false,
            mfinalfunc_modify: 'r',
        })
    );
}

#[test]
fn parse_create_aggregate_statement_with_old_style_basetype() {
    let stmt = parse_statement(
        "create aggregate oldcnt (sfunc = int8inc, basetype = 'ANY', stype = int8, initcond = '0')",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateAggregate(CreateAggregateStatement {
            schema_name: None,
            aggregate_name: "oldcnt".into(),
            replace_existing: false,
            signature: AggregateSignatureKind::Star,
            hypothetical: false,
            sfunc_name: "int8inc".into(),
            stype: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int8)),
            finalfunc_name: None,
            initcond: Some("0".into()),
            parallel: None,
            transspace: 0,
            combinefunc_name: None,
            serialfunc_name: None,
            deserialfunc_name: None,
            finalfunc_extra: false,
            finalfunc_modify: 'r',
            mstype: None,
            msfunc_name: None,
            minvfunc_name: None,
            mfinalfunc_name: None,
            minitcond: None,
            mtransspace: 0,
            mfinalfunc_extra: false,
            mfinalfunc_modify: 'r',
        })
    );
}

#[test]
fn parse_create_or_replace_aggregate_star_signature() {
    let stmt = parse_statement(
        "create or replace aggregate public.newcnt(*) (sfunc = int8inc, stype = int8, initcond = '0')",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateAggregate(CreateAggregateStatement {
            schema_name: Some("public".into()),
            aggregate_name: "newcnt".into(),
            replace_existing: true,
            signature: AggregateSignatureKind::Star,
            hypothetical: false,
            sfunc_name: "int8inc".into(),
            stype: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int8)),
            finalfunc_name: None,
            initcond: Some("0".into()),
            parallel: None,
            transspace: 0,
            combinefunc_name: None,
            serialfunc_name: None,
            deserialfunc_name: None,
            finalfunc_extra: false,
            finalfunc_modify: 'r',
            mstype: None,
            msfunc_name: None,
            minvfunc_name: None,
            mfinalfunc_name: None,
            minitcond: None,
            mtransspace: 0,
            mfinalfunc_extra: false,
            mfinalfunc_modify: 'r',
        })
    );
}

#[test]
fn parse_drop_and_comment_on_aggregate_statements() {
    let stmt = parse_statement("drop aggregate if exists public.newcnt(\"any\") cascade").unwrap();
    assert_eq!(
        stmt,
        Statement::DropAggregate(DropAggregateStatement {
            if_exists: true,
            schema_name: Some("public".into()),
            aggregate_name: "newcnt".into(),
            signature: aggregate_signature(vec![aggregate_signature_arg(
                AggregateArgType::AnyPseudo,
            )]),
            cascade: true,
        })
    );

    let stmt = parse_statement("comment on aggregate newcnt(*) is 'an agg(*) comment'").unwrap();
    assert_eq!(
        stmt,
        Statement::CommentOnAggregate(CommentOnAggregateStatement {
            schema_name: None,
            aggregate_name: "newcnt".into(),
            signature: AggregateSignatureKind::Star,
            comment: Some("an agg(*) comment".into()),
        })
    );
}

#[test]
fn parse_create_aggregate_supports_ordered_variadic_and_hypothetical_forms() {
    let stmt = parse_statement(
        "create aggregate my_percentile_disc(float8 order by anyelement) (sfunc = int8inc, stype = int8)",
    )
    .unwrap();
    match stmt {
        Statement::CreateAggregate(stmt) => {
            assert_eq!(
                stmt.signature,
                AggregateSignatureKind::Args(AggregateSignature {
                    args: vec![aggregate_signature_arg(AggregateArgType::Type(
                        RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float8)),
                    ))],
                    order_by: vec![aggregate_signature_arg(AggregateArgType::Type(
                        RawTypeName::Named {
                            name: "anyelement".into(),
                            array_bounds: 0,
                        },
                    ))],
                })
            );
            assert!(!stmt.hypothetical);
        }
        other => panic!("expected CREATE AGGREGATE, got {other:?}"),
    }

    let stmt = parse_statement(
        "create aggregate least_agg(variadic items anyarray) (sfunc = int8inc, stype = int8)",
    )
    .unwrap();
    match stmt {
        Statement::CreateAggregate(stmt) => {
            assert_eq!(
                stmt.signature,
                aggregate_signature(vec![AggregateSignatureArg {
                    name: Some("items".into()),
                    arg_type: AggregateArgType::Type(RawTypeName::Named {
                        name: "anyarray".into(),
                        array_bounds: 0,
                    }),
                    variadic: true,
                }])
            );
            assert!(!stmt.hypothetical);
        }
        other => panic!("expected CREATE AGGREGATE, got {other:?}"),
    }

    let stmt = parse_statement(
        "create aggregate hypothetical_rank(float8 order by anyelement) (sfunc = int8inc, stype = int8, hypothetical)",
    )
    .unwrap();
    match stmt {
        Statement::CreateAggregate(stmt) => assert!(stmt.hypothetical),
        other => panic!("expected CREATE AGGREGATE, got {other:?}"),
    }

    let stmt = parse_statement(
        "create aggregate badagg(int4) (sfunc = int4pl, stype = int4, combinefunc = int4pl)",
    )
    .unwrap();
    match stmt {
        Statement::CreateAggregate(stmt) => {
            assert_eq!(stmt.combinefunc_name.as_deref(), Some("int4pl"));
        }
        other => panic!("expected CREATE AGGREGATE, got {other:?}"),
    }
}

#[test]
fn parse_alter_aggregate_rename_statement() {
    let stmt = parse_statement(
        "alter aggregate public.my_percentile_disc(float8 order by anyelement) rename to my_percentile_disc2",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::AlterAggregateRename(AlterAggregateRenameStatement {
            schema_name: Some("public".into()),
            aggregate_name: "my_percentile_disc".into(),
            signature: AggregateSignatureKind::Args(AggregateSignature {
                args: vec![aggregate_signature_arg(AggregateArgType::Type(
                    RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float8)),
                ))],
                order_by: vec![aggregate_signature_arg(AggregateArgType::Type(
                    RawTypeName::Named {
                        name: "anyelement".into(),
                        array_bounds: 0,
                    },
                ))],
            }),
            new_name: "my_percentile_disc2".into(),
        })
    );
}

#[test]
fn parse_set_local_statement() {
    let stmt = parse_statement("set local client_min_messages to 'warning'").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "client_min_messages".into(),
            value: Some("warning".into()),
            is_local: true,
        })
    );
}

#[test]
fn parse_set_local_time_zone_statement() {
    let stmt = parse_statement("set local time zone 10.5").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "timezone".into(),
            value: Some("10.5".into()),
            is_local: true,
        })
    );
}

#[test]
fn parse_set_time_zone_negative_offset_statement() {
    let stmt = parse_statement("set time zone -8").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "timezone".into(),
            value: Some("-8".into()),
            is_local: false,
        })
    );
}

#[test]
fn parse_set_statement_with_escape_string() {
    let stmt = parse_statement(r#"set application_name to E'line\nbreak'"#).unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "application_name".into(),
            value: Some("line\nbreak".into()),
            is_local: false,
        })
    );
}

#[test]
fn parse_reset_statement() {
    let stmt = parse_statement("reset extra_float_digits").unwrap();
    assert_eq!(
        stmt,
        Statement::Reset(ResetStatement {
            name: Some("extra_float_digits".into()),
        })
    );
}

#[test]
fn parse_reset_time_zone_statement() {
    let stmt = parse_statement("reset time zone").unwrap();
    assert_eq!(
        stmt,
        Statement::Reset(ResetStatement {
            name: Some("timezone".into()),
        })
    );
}

#[test]
fn parse_checkpoint_statement() {
    assert_eq!(
        parse_statement("checkpoint").unwrap(),
        Statement::Checkpoint(CheckpointStatement)
    );
}

#[test]
fn parse_statement_ignores_embedded_and_leading_comments() {
    let stmt = parse_statement("/* leading */ select /* embedded */ 'x' as value").unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(stmt.targets.len(), 1);
            assert_eq!(stmt.targets[0].output_name, "value");
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_copy_from_file_statement() {
    let stmt = parse_statement("copy test_tsvector from '/tmp/tsearch.data'").unwrap();
    match stmt {
        Statement::CopyFrom(copy) => {
            assert_eq!(copy.table_name, "test_tsvector");
            assert_eq!(copy.columns, None);
            assert_eq!(copy.source, CopySource::File("/tmp/tsearch.data".into()));
            assert_eq!(copy.options, CopyOptions::default());
        }
        other => panic!("expected copy statement, got {other:?}"),
    }
}

#[test]
fn parse_copy_table_to_stdout_with_modern_csv_options() {
    let stmt = parse_statement(
        "copy public.items (id, name) to stdout with (format csv, header, delimiter '|', force_quote (name))",
    )
    .unwrap();
    match stmt {
        Statement::CopyTo(copy) => {
            assert_eq!(
                copy.source,
                CopyToSource::Relation {
                    table_name: "public.items".into(),
                    columns: Some(vec!["id".into(), "name".into()]),
                }
            );
            assert_eq!(copy.destination, CopyToDestination::Stdout);
            assert_eq!(copy.options.format, CopyFormat::Csv);
            assert_eq!(copy.options.delimiter, "|");
            assert!(copy.options.header);
            assert_eq!(
                copy.options.force_quote,
                CopyForceQuote::Columns(vec!["name".into()])
            );
        }
        other => panic!("expected COPY TO statement, got {other:?}"),
    }
}

#[test]
fn parse_copy_from_file_with_csv_encoding_options() {
    let stmt = parse_statement(
        "copy copy_encoding_tab from '/tmp/copyencoding_utf8.csv' with (format csv, encoding 'LATIN1')",
    )
    .unwrap();
    match stmt {
        Statement::CopyFrom(copy) => {
            assert_eq!(copy.table_name, "copy_encoding_tab");
            assert_eq!(
                copy.source,
                CopySource::File("/tmp/copyencoding_utf8.csv".into())
            );
            assert_eq!(copy.options.format, CopyFormat::Csv);
            assert_eq!(copy.options.encoding.as_deref(), Some("LATIN1"));
        }
        other => panic!("expected copy from statement, got {other:?}"),
    }
}

#[test]
fn parse_copy_select_to_file_with_csv_encoding_options() {
    let stmt = parse_statement(
        "copy (select E'\\u3042') to '/tmp/copyencoding_utf8.csv' with (format csv, encoding 'UTF8')",
    )
    .unwrap();
    match stmt {
        Statement::CopyTo(copy) => {
            assert!(matches!(copy.source, CopyToSource::Query { .. }));
            assert_eq!(
                copy.destination,
                CopyToDestination::File("/tmp/copyencoding_utf8.csv".into())
            );
            assert_eq!(copy.options.format, CopyFormat::Csv);
            assert_eq!(copy.options.encoding.as_deref(), Some("UTF8"));
        }
        other => panic!("expected copy to statement, got {other:?}"),
    }
}

#[test]
fn parse_copy_query_to_program_with_legacy_options() {
    let stmt = parse_statement(
        "copy (values (1, 'a,b')) to program 'cat >/tmp/copy.out' csv header force quote *",
    )
    .unwrap();
    match stmt {
        Statement::CopyTo(copy) => {
            assert!(matches!(copy.source, CopyToSource::Query { .. }));
            assert_eq!(
                copy.destination,
                CopyToDestination::Program("cat >/tmp/copy.out".into())
            );
            assert_eq!(copy.options.format, CopyFormat::Csv);
            assert!(copy.options.header);
            assert_eq!(copy.options.force_quote, CopyForceQuote::All);
        }
        other => panic!("expected COPY TO statement, got {other:?}"),
    }
}

#[test]
fn parse_copy_insert_returning_to_stdout_keeps_inner_insert_statement() {
    let stmt =
        parse_statement("copy (insert into items values (1) returning id) to stdout").unwrap();
    match stmt {
        Statement::CopyTo(copy) => {
            assert_eq!(copy.destination, CopyToDestination::Stdout);
            match copy.source {
                CopyToSource::Query { statement, .. } => {
                    assert!(matches!(*statement, Statement::Insert(_)));
                }
                other => panic!("expected query source, got {other:?}"),
            }
        }
        other => panic!("expected copy to statement, got {other:?}"),
    }
}

#[test]
fn parse_copy_query_rejects_from_direction() {
    let err = parse_statement("copy (select 1) from '/tmp/in'").unwrap_err();
    assert!(matches!(
        err,
        ParseError::UnexpectedToken { expected: "TO", .. }
    ));
}

#[test]
fn parse_statement_ignores_deeply_nested_comments() {
    let stmt = parse_statement(
        "select /* one /* two 'still comment' /* three */ back two */ back one */ 'ok' as v",
    )
    .unwrap();
    match stmt {
        Statement::Select(stmt) => assert_eq!(stmt.targets[0].output_name, "v"),
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_with_select_and_values_ctes() {
    let stmt = parse_select(
        "with x(a) as (values (1), (2)), y as (select id from people) select * from x, y",
    )
    .unwrap();
    assert_eq!(stmt.with.len(), 2);
    assert_eq!(stmt.with[0].name, "x");
    assert_eq!(stmt.with[0].column_names, vec!["a"]);
    assert!(matches!(stmt.with[0].body, CteBody::Values(_)));
    assert!(matches!(stmt.with[1].body, CteBody::Select(_)));
    assert!(matches!(stmt.from, Some(FromItem::Join { .. })));
}

#[test]
fn parse_insert_with_writable_insert_cte() {
    let stmt = parse_statement(
        "with moved as (insert into src values (1) returning id) insert into dst select id from moved",
    )
    .unwrap();
    match stmt {
        Statement::Insert(insert) => {
            assert_eq!(insert.with.len(), 1);
            assert!(matches!(insert.with[0].body, CteBody::Insert(_)));
            assert!(matches!(insert.source, InsertSource::Select(_)));
        }
        other => panic!("expected insert statement, got {other:?}"),
    }
}

#[test]
fn parse_select_with_writable_insert_cte_returning_tableoid_and_star() {
    let stmt = parse_statement(
        "with ins (a, b, c) as \
         (insert into mlparted (b, a) select s.a, 1 from generate_series(2, 39) s(a) returning tableoid::regclass, *) \
         select a, b, min(c), max(c) from ins group by a, b order by 1",
    )
    .unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.with.len(), 1);
            match &select.with[0].body {
                CteBody::Insert(insert) => {
                    assert_eq!(insert.returning.len(), 2);
                    assert!(matches!(insert.source, InsertSource::Select(_)));
                }
                other => panic!("expected writable insert CTE, got {other:?}"),
            }
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_top_level_values_with_order_limit_offset() {
    let stmt = parse_statement("values (2), (1) order by 1 desc limit 1 offset 0").unwrap();
    match stmt {
        Statement::Values(values) => {
            assert_eq!(values.rows.len(), 2);
            assert_eq!(values.order_by.len(), 1);
            assert_eq!(values.limit, Some(1));
            assert_eq!(values.offset, Some(0));
        }
        other => panic!("expected values statement, got {other:?}"),
    }
}

#[test]
fn build_plan_rejects_top_level_values_srf() {
    let Statement::Values(values) = parse_statement("values (1, generate_series(1, 2))").unwrap()
    else {
        panic!("expected values statement");
    };
    assert!(matches!(
        build_values_plan(&values, &catalog()),
        Err(ParseError::FeatureNotSupportedMessage(message))
            if message == "set-returning functions are not allowed in VALUES"
    ));
}

#[test]
fn parse_with_on_insert_update_delete() {
    assert!(matches!(
        parse_statement("with q as (select 1) insert into people (id) values ((select 1 from q))")
            .unwrap(),
        Statement::Insert(InsertStatement { with, .. }) if with.len() == 1
    ));
    assert!(matches!(
        parse_statement("with q as (values (1)) update people set note = (select column1::text from q) where id = 1")
            .unwrap(),
        Statement::Update(UpdateStatement { with, .. }) if with.len() == 1
    ));
    assert!(matches!(
        parse_statement("with q as (select 1) delete from people where id in (select 1 from q)")
            .unwrap(),
        Statement::Delete(DeleteStatement { with, .. }) if with.len() == 1
    ));
}

#[test]
fn build_plan_binds_select_ctes_and_nested_subqueries() {
    let stmt = parse_select(
        "with q as (values (2), (1)) select (select column1 from q order by 1 limit 1)",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn build_plan_cte_shadows_catalog_table() {
    let stmt = parse_select("with people as (values (42)) select column1 from people").unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn build_plan_rejects_forward_cte_references() {
    let stmt =
        parse_select("with y as (select * from x), x as (values (1)) select * from y").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::UnknownTable(name)) if name == "x"
    ));
}

#[test]
fn parse_boolean_is_predicates_lower_to_existing_ast() {
    let stmt = parse_select(
        "select b is true, b is not false, b is unknown, b is not unknown from people",
    )
    .unwrap();

    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::IsNotDistinctFrom(_, ref right)
            if matches!(right.as_ref(), SqlExpr::Const(Value::Bool(true)))
    ));
    assert!(matches!(
        stmt.targets[1].expr,
        SqlExpr::IsDistinctFrom(_, ref right)
            if matches!(right.as_ref(), SqlExpr::Const(Value::Bool(false)))
    ));
    assert!(matches!(stmt.targets[2].expr, SqlExpr::IsNull(_)));
    assert!(matches!(stmt.targets[3].expr, SqlExpr::IsNotNull(_)));
}

#[test]
fn parse_position_in_syntax_as_builtin_call() {
    let stmt = parse_select("select position('bc' in 'abcd')").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::FuncCall { ref name, ref args, .. }
            if name == "position" && args.args().len() == 2
    ));
}

#[test]
fn parse_extract_in_syntax_as_extract_call() {
    let stmt = parse_select("select extract(week from date '2020-08-11')").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::FuncCall { ref name, ref args, .. }
            if name == "extract"
                && args.args().len() == 2
                && matches!(args.args()[0].value, SqlExpr::Const(Value::Text(ref field)) if &field[..] == "week")
    ));
}

#[test]
fn parse_variadic_function_call_marks_call_level_flag() {
    std::thread::Builder::new()
        .name("parse_variadic_function_call_marks_call_level_flag".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let stmt = parse_select("select json_build_array(VARIADIC ARRAY[1, 2, 3])").unwrap();
            assert!(matches!(
                stmt.targets[0].expr,
                SqlExpr::FuncCall {
                    ref name,
                    ref args,
                    func_variadic: true,
                    ..
                } if name == "json_build_array" && args.args().len() == 1
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parse_variadic_function_call_with_fixed_prefix_and_cast() {
    std::thread::Builder::new()
        .name("parse_variadic_function_call_with_fixed_prefix_and_cast".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let stmt = parse_select(
                "select json_extract_path('{\"a\":{\"b\":2}}'::json, VARIADIC ARRAY['a', 'b']::text[])",
            )
            .unwrap();
            assert!(matches!(
                stmt.targets[0].expr,
                SqlExpr::FuncCall {
                    ref name,
                    ref args,
                    func_variadic: true,
                    ..
                } if name == "json_extract_path" && args.args().len() == 2
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parse_variadic_must_be_final_supplied_argument() {
    std::thread::Builder::new()
        .name("parse_variadic_must_be_final_supplied_argument".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            assert!(parse_select("select json_build_array(VARIADIC ARRAY[1, 2], 3)").is_err());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parse_multiline_position_convert_from_expression() {
    std::thread::Builder::new()
        .name("parse_multiline_position_convert_from_expression".into())
        .stack_size(128 * 1024 * 1024)
        .spawn(|| {
            let stmt = parse_select(
                    "select position(\n convert_from('\\\\xbcf6c7d0', 'EUC_KR') in\n convert_from('\\\\xb0fac7d02c20bcf6c7d02c20b1e2bcfa2c20bbee', 'EUC_KR'))",
                )
                .unwrap();
            assert!(matches!(
                stmt.targets[0].expr,
                SqlExpr::FuncCall { ref name, ref args, .. }
                    if name == "position" && args.args().len() == 2
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parse_select_with_where() {
    let stmt = parse_select("select name, note from people where id > 1 and note is null").unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Table {
            name: "people".into(),
            only: false,
        })
    );
    assert_eq!(stmt.targets.len(), 2);
    assert!(matches!(stmt.where_clause, Some(SqlExpr::And(_, _))));
}

#[test]
fn parse_null_predicates() {
    let stmt = parse_select(
        "select name from people where note is not null or note is distinct from null",
    )
    .unwrap();
    assert!(matches!(stmt.where_clause, Some(SqlExpr::Or(_, _))));

    let stmt =
        parse_select("select name from people where note is not distinct from null").unwrap();
    assert!(matches!(
        stmt.where_clause,
        Some(SqlExpr::IsNotDistinctFrom(_, _))
    ));
}

#[test]
fn parse_join_select() {
    let stmt = parse_select(
        "select people.name, pets.name from people join pets on people.id = pets.owner_id",
    )
    .unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Join {
            left: Box::new(FromItem::Table {
                name: "people".into(),
                only: false,
            }),
            right: Box::new(FromItem::Table {
                name: "pets".into(),
                only: false,
            }),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(SqlExpr::Eq(
                Box::new(SqlExpr::Column("people.id".into())),
                Box::new(SqlExpr::Column("pets.owner_id".into()))
            )),
        })
    );
}

#[test]
fn parse_cross_join_select() {
    let stmt = parse_select("select people.name, pets.name from people, pets").unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Join {
            left: Box::new(FromItem::Table {
                name: "people".into(),
                only: false,
            }),
            right: Box::new(FromItem::Table {
                name: "pets".into(),
                only: false,
            }),
            kind: JoinKind::Cross,
            constraint: JoinConstraint::None,
        })
    );
}

#[test]
fn parse_table_alias() {
    let stmt = parse_select("select s.name from people s").unwrap();
    assert_eq!(stmt.targets[0].output_name, "name");
    assert_eq!(
        stmt.from,
        Some(FromItem::Alias {
            source: Box::new(FromItem::Table {
                name: "people".into(),
                only: false,
            }),
            alias: "s".into(),
            column_aliases: AliasColumnSpec::None,
            preserve_source_names: false,
        })
    );
}

#[test]
fn parse_table_alias_with_as() {
    let stmt = parse_select("select s.name from people as s").unwrap();
    assert_eq!(stmt.targets[0].output_name, "name");
    assert_eq!(
        stmt.from,
        Some(FromItem::Alias {
            source: Box::new(FromItem::Table {
                name: "people".into(),
                only: false,
            }),
            alias: "s".into(),
            column_aliases: AliasColumnSpec::None,
            preserve_source_names: false,
        })
    );
}

#[test]
fn parse_select_with_quoted_output_alias() {
    let stmt = parse_select("select q1 * 2 as \"twice int4\" from people").unwrap();
    assert_eq!(stmt.targets[0].output_name, "twice int4");
}

#[test]
fn parse_select_star_with_table_alias() {
    let stmt = parse_select("select * from people p").unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Alias {
            source: Box::new(FromItem::Table {
                name: "people".into(),
                only: false,
            }),
            alias: "p".into(),
            column_aliases: AliasColumnSpec::None,
            preserve_source_names: false,
        })
    );
    assert_eq!(stmt.targets[0].output_name, "*");
}

#[test]
fn parse_select_alias_overrides_qualified_column_name() {
    let stmt = parse_select("select p.name as w from people p").unwrap();
    assert_eq!(stmt.targets[0].output_name, "w");
}

#[test]
fn parse_row_constructor_expression() {
    let stmt = parse_select("select row(1, 'x')").unwrap();
    assert_eq!(stmt.targets[0].output_name, "row");
    match &stmt.targets[0].expr {
        SqlExpr::Row(args) => {
            assert_eq!(args.len(), 2);
            assert!(matches!(&args[0], SqlExpr::IntegerLiteral(value) if value == "1"));
            assert!(matches!(&args[1], SqlExpr::Const(Value::Text(text)) if text.as_str() == "x"));
        }
        other => panic!("expected row constructor, got {other:?}"),
    }
}

#[test]
fn parse_implicit_row_constructor_expression() {
    let stmt = parse_select("select (1, 'x')").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Row(args) => {
            assert_eq!(args.len(), 2);
            assert!(matches!(&args[0], SqlExpr::IntegerLiteral(value) if value == "1"));
            assert!(matches!(&args[1], SqlExpr::Const(Value::Text(text)) if text.as_str() == "x"));
        }
        other => panic!("expected implicit row constructor, got {other:?}"),
    }
}

#[test]
fn analyze_extract_keeps_extract_as_default_output_name() {
    let stmt = parse_select("select extract(week from date '2020-08-11')").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![("extract".into(), SqlType::new(SqlTypeKind::Numeric))]
    );
}

#[test]
fn parse_type_cast_expression() {
    let stmt = parse_select("select (p.name)::text from people p").unwrap();
    assert_eq!(stmt.targets[0].output_name, "name");
    match &stmt.targets[0].expr {
        SqlExpr::Cast(inner, ty) => {
            assert_eq!(*ty, SqlType::new(SqlTypeKind::Text));
            assert!(matches!(**inner, SqlExpr::Column(ref name) if name == "p.name"));
        }
        other => panic!("expected cast expression, got {other:?}"),
    }
}

#[test]
fn analyze_unknown_type_cast_outputs_text() {
    let stmt = parse_select("select 'foo'::unknown as u").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    assert_eq!(
        query_column_names_and_types(&query),
        vec![("u".into(), SqlType::new(SqlTypeKind::Text))]
    );
}

#[test]
fn parse_field_select_uses_field_name_as_default_output_name() {
    let stmt = parse_select("select (jsonb_each('{\"a\":1}'::jsonb)).key").unwrap();
    assert_eq!(stmt.targets[0].output_name, "key");
}

#[test]
fn parse_field_star_select_from_record_expression() {
    let stmt =
        parse_select("select (jsonb_populate_record(null::record, '{\"a\":1}'::jsonb)).*").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::FieldSelect { field, .. } => assert_eq!(field, "*"),
        other => panic!("expected field star select, got {other:?}"),
    }
}

#[test]
fn parse_varchar_type_cast_expression() {
    let stmt = parse_select("select 'abc'::varchar(2)").unwrap();
    assert_eq!(stmt.targets[0].output_name, "varchar");
    match &stmt.targets[0].expr {
        SqlExpr::Cast(_, ty) => {
            assert_eq!(*ty, SqlType::with_char_len(SqlTypeKind::Varchar, 2));
        }
        other => panic!("expected cast expression, got {other:?}"),
    }
}

#[test]
fn parse_time_type_cast_uses_short_default_output_name() {
    let stmt = parse_select("select '23:59:59.999999'::time").unwrap();
    assert_eq!(stmt.targets[0].output_name, "time");
}

#[test]
fn parse_timestamptz_type_cast_uses_short_default_output_name() {
    let stmt = parse_select("select '2020-01-01 00:00:00+00'::timestamptz").unwrap();
    assert_eq!(stmt.targets[0].output_name, "timestamptz");
}

#[test]
fn parse_standalone_type_names() {
    assert_eq!(
        parse_type_name("varchar(4)").unwrap(),
        SqlType::with_char_len(SqlTypeKind::Varchar, 4)
    );
    assert_eq!(
        parse_type_name("character varying(4)").unwrap(),
        SqlType::with_char_len(SqlTypeKind::Varchar, 4)
    );
    assert_eq!(
        parse_type_name("varchar").unwrap(),
        SqlType::new(SqlTypeKind::Varchar)
    );
    assert_eq!(
        parse_type_name("\"char\"").unwrap(),
        SqlType::new(SqlTypeKind::InternalChar)
    );
    assert_eq!(
        parse_type_name("bpchar").unwrap(),
        SqlType::new(SqlTypeKind::Char)
    );
    assert_eq!(
        parse_type_name("cstring").unwrap(),
        SqlType::new(SqlTypeKind::Cstring)
    );
    assert_eq!(
        parse_type_name("char").unwrap(),
        SqlType::with_char_len(SqlTypeKind::Char, 1)
    );
    assert_eq!(
        parse_type_name("character").unwrap(),
        SqlType::with_char_len(SqlTypeKind::Char, 1)
    );
    assert_eq!(
        parse_type_name("character(16)").unwrap(),
        SqlType::with_char_len(SqlTypeKind::Char, 16)
    );
    assert_eq!(
        parse_type_name("bytea").unwrap(),
        SqlType::new(SqlTypeKind::Bytea)
    );
    assert_eq!(
        parse_type_name("uuid").unwrap(),
        SqlType::new(SqlTypeKind::Uuid)
    );
    assert_eq!(
        parse_type_name("pg_catalog.uuid").unwrap(),
        SqlType::new(SqlTypeKind::Uuid)
    );
    assert_eq!(
        parse_type_name("uuid[]").unwrap(),
        SqlType::array_of(SqlType::new(SqlTypeKind::Uuid))
    );
    assert_eq!(
        parse_type_name("bit").unwrap(),
        SqlType::with_bit_len(SqlTypeKind::Bit, 1)
    );
    assert_eq!(
        parse_type_name("bit(4)").unwrap(),
        SqlType::with_bit_len(SqlTypeKind::Bit, 4)
    );
    assert_eq!(
        parse_type_name("bit varying(8)").unwrap(),
        SqlType::with_bit_len(SqlTypeKind::VarBit, 8)
    );
    assert_eq!(
        parse_type_name("varbit").unwrap(),
        SqlType::new(SqlTypeKind::VarBit)
    );
    assert_eq!(
        parse_type_name("pg_node_tree").unwrap(),
        SqlType::new(SqlTypeKind::PgNodeTree)
    );
    assert_eq!(
        parse_type_name("date").unwrap(),
        SqlType::new(SqlTypeKind::Date)
    );
    assert_eq!(
        parse_type_name("time(2)").unwrap(),
        SqlType::with_time_precision(SqlTypeKind::Time, 2)
    );
    assert_eq!(
        parse_type_name("time(2) with time zone").unwrap(),
        SqlType::with_time_precision(SqlTypeKind::TimeTz, 2)
    );
    assert_eq!(
        parse_type_name("timestamp(3) without time zone").unwrap(),
        SqlType::with_time_precision(SqlTypeKind::Timestamp, 3)
    );
    assert_eq!(
        parse_type_name("timestamp(4) with time zone").unwrap(),
        SqlType::with_time_precision(SqlTypeKind::TimestampTz, 4)
    );
    assert_eq!(
        parse_type_name("timetz").unwrap(),
        SqlType::new(SqlTypeKind::TimeTz)
    );
    assert_eq!(
        parse_type_name("timetz(4)").unwrap(),
        SqlType::with_time_precision(SqlTypeKind::TimeTz, 4)
    );
    assert_eq!(
        parse_type_name("timestamptz").unwrap(),
        SqlType::new(SqlTypeKind::TimestampTz)
    );
    assert_eq!(
        parse_type_name("timestamptz(5)").unwrap(),
        SqlType::with_time_precision(SqlTypeKind::TimestampTz, 5)
    );
}

#[test]
fn parse_uuid_type_cast_expressions() {
    let stmt = parse_select(
        "select '00000000-0000-0000-0000-000000000001'::uuid, \
         cast('00000000000000000000000000000002' as pg_catalog.uuid), \
         uuid('00000000-0000-0000-0000-000000000003')",
    )
    .unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Cast(_, ty) => assert_eq!(*ty, SqlType::new(SqlTypeKind::Uuid)),
        other => panic!("expected uuid cast expression, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::Cast(_, ty) => assert_eq!(*ty, SqlType::new(SqlTypeKind::Uuid)),
        other => panic!("expected pg_catalog.uuid cast expression, got {other:?}"),
    }
}

#[test]
fn parse_bit_type_cast_expression() {
    let stmt = parse_select("select '0101'::bit(4), cast('0101' as bit varying(8))").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Cast(_, ty) => assert_eq!(*ty, SqlType::with_bit_len(SqlTypeKind::Bit, 4)),
        other => panic!("expected cast expression, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::Cast(_, ty) => {
            assert_eq!(*ty, SqlType::with_bit_len(SqlTypeKind::VarBit, 8))
        }
        other => panic!("expected cast expression, got {other:?}"),
    }
}

#[test]
fn parse_bit_string_literals() {
    let stmt = parse_select("select B'0101', X'0f'").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Const(Value::Bit(bits)) => assert_eq!(bits.render(), "0101"),
        other => panic!("expected bit literal, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::Const(Value::Bit(bits)) => assert_eq!(bits.render(), "00001111"),
        other => panic!("expected bit literal, got {other:?}"),
    }
}

#[test]
fn parse_bit_substring_and_overlay_syntax() {
    let stmt = parse_select(
        "select substring(B'010101' from 2 for 3), overlay(B'010101' placing B'11' from 2)",
    )
    .unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::FuncCall { name, args, .. } => {
            assert_eq!(name, "substring");
            assert_eq!(args.args().len(), 3);
        }
        other => panic!("expected substring call, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::FuncCall { name, args, .. } => {
            assert_eq!(name, "overlay");
            assert_eq!(args.args().len(), 3);
        }
        other => panic!("expected overlay call, got {other:?}"),
    }
}

#[test]
fn parse_substring_for_syntax() {
    let stmt = parse_select("select substring('abcdef' for 3), substring(note for 1) from people")
        .unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::FuncCall { name, args, .. } => {
            assert_eq!(name, "substring");
            assert_eq!(args.args().len(), 3);
            assert!(matches!(
                &args.args()[1].value,
                SqlExpr::IntegerLiteral(value) if value == "1"
            ));
        }
        other => panic!("expected substring call, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::FuncCall { name, args, .. } => {
            assert_eq!(name, "substring");
            assert_eq!(args.args().len(), 3);
            assert!(matches!(
                &args.args()[1].value,
                SqlExpr::IntegerLiteral(value) if value == "1"
            ));
        }
        other => panic!("expected substring call, got {other:?}"),
    }
}

#[test]
fn parse_internal_char_casts() {
    let stmt = parse_select("select 'a'::\"char\", cast('b' as \"char\")").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Cast(_, ty) => assert_eq!(*ty, SqlType::new(SqlTypeKind::InternalChar)),
        other => panic!("expected cast expression, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::Cast(_, ty) => assert_eq!(*ty, SqlType::new(SqlTypeKind::InternalChar)),
        other => panic!("expected cast expression, got {other:?}"),
    }
}

#[test]
fn parse_cast_function_syntax_expression() {
    let stmt = parse_select("select cast(p.name as text) from people p").unwrap();
    assert_eq!(stmt.targets[0].output_name, "name");
    match &stmt.targets[0].expr {
        SqlExpr::Cast(inner, ty) => {
            assert_eq!(*ty, SqlType::new(SqlTypeKind::Text));
            assert!(matches!(**inner, SqlExpr::Column(ref name) if name == "p.name"));
        }
        other => panic!("expected cast expression, got {other:?}"),
    }
}

#[test]
fn parse_oid_cast_type() {
    let stmt = parse_select("select cast(42 as oid)").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::Cast(_, ty) if *ty == SqlType::new(SqlTypeKind::Oid)
    ));
    assert_eq!(stmt.targets[0].output_name, "oid");
}

#[test]
fn parse_typed_string_literal_expression() {
    let stmt = parse_select(
        "select int2 '7', int4 '9', varchar(3) 'abc', date '2024-01-02', timestamp with time zone '2024-01-02 03:04:05+00'",
    )
    .unwrap();
    assert_eq!(stmt.targets.len(), 5);
    assert_eq!(stmt.targets[0].output_name, "int2");
    assert_eq!(stmt.targets[1].output_name, "int4");
    assert_eq!(stmt.targets[2].output_name, "varchar");
    assert_eq!(stmt.targets[3].output_name, "date");
    assert_eq!(stmt.targets[4].output_name, "timestamptz");
    match &stmt.targets[0].expr {
        SqlExpr::Cast(inner, ty) => {
            assert_eq!(*ty, SqlType::new(SqlTypeKind::Int2));
            assert!(
                matches!(**inner, SqlExpr::Const(Value::Text(ref text)) if text.as_str() == "7")
            );
        }
        other => panic!("expected typed string literal cast, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::Cast(inner, ty) => {
            assert_eq!(*ty, SqlType::new(SqlTypeKind::Int4));
            assert!(
                matches!(**inner, SqlExpr::Const(Value::Text(ref text)) if text.as_str() == "9")
            );
        }
        other => panic!("expected typed string literal cast, got {other:?}"),
    }
    match &stmt.targets[2].expr {
        SqlExpr::Cast(inner, ty) => {
            assert_eq!(*ty, SqlType::with_char_len(SqlTypeKind::Varchar, 3));
            assert!(
                matches!(**inner, SqlExpr::Const(Value::Text(ref text)) if text.as_str() == "abc")
            );
        }
        other => panic!("expected typed string literal cast, got {other:?}"),
    }
    assert!(matches!(
        &stmt.targets[3].expr,
        SqlExpr::Cast(_, ty) if *ty == SqlType::new(SqlTypeKind::Date)
    ));
    assert!(matches!(
        &stmt.targets[4].expr,
        SqlExpr::Cast(_, ty) if *ty == SqlType::new(SqlTypeKind::TimestampTz)
    ));
}

#[test]
fn parse_datetime_literal_output_names_use_postgres_aliases() {
    let stmt = parse_select(
        "select '2000-01-01'::timestamp, timestamp without time zone '2000-01-01', time without time zone '04:05', time with time zone '04:05+00'",
    )
    .unwrap();
    assert_eq!(
        stmt.targets
            .iter()
            .map(|target| target.output_name.as_str())
            .collect::<Vec<_>>(),
        vec!["timestamp", "timestamp", "time", "timetz"]
    );
}

#[test]
fn parse_timestamp_output_names_match_postgres_shorthands() {
    let stmt = parse_select(
        "select timestamp with time zone '2024-01-02 03:04:05+00', time with time zone '03:04:05+00'",
    )
    .unwrap();
    assert_eq!(stmt.targets[0].output_name, "timestamptz");
    assert_eq!(stmt.targets[1].output_name, "timetz");
}

#[test]
fn parse_at_time_zone_uses_timezone_output_name() {
    let stmt = parse_select("select '19970210 173201' at time zone 'America/New_York'").unwrap();
    assert_eq!(stmt.targets[0].output_name, "timezone");
}

#[test]
fn parse_interval_typed_string_literals() {
    let stmt = parse_select(
        "select interval '1 day', interval(2) '1 day 01:02:03.456', interval '1-2' year to month, interval '12:34.5678' minute to second(2)",
    )
    .unwrap();
    assert_eq!(stmt.targets.len(), 4);
    for target in &stmt.targets {
        assert!(matches!(
            &target.expr,
            SqlExpr::Cast(inner, ty)
                if ty.as_builtin().is_some_and(|ty| ty.kind == SqlTypeKind::Interval)
                    && matches!(inner.as_ref(), SqlExpr::Const(Value::Text(_)))
        ));
    }
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::Cast(_, ty)
            if ty.as_builtin().is_some_and(|ty| {
                ty.kind == SqlTypeKind::Interval && ty.typmod == 2
            })
    ));
    assert!(matches!(
        &stmt.targets[3].expr,
        SqlExpr::Cast(_, ty)
            if ty.as_builtin().is_some_and(|ty| {
                ty.kind == SqlTypeKind::Interval
                    && ty.interval_precision() == Some(2)
                    && ty.interval_range()
                        == Some(SqlType::INTERVAL_MASK_MINUTE | SqlType::INTERVAL_MASK_SECOND)
            })
    ));
}

#[test]
fn parse_interval_field_qualified_casts() {
    let stmt = parse_select(
        "select f1::interval day to minute, cast(f1 as interval second(2)), f1::interval[] from interval_tbl",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::Cast(_, ty)
            if ty.as_builtin().is_some_and(|ty| {
                ty.kind == SqlTypeKind::Interval
                    && ty.interval_range()
                        == Some(
                            SqlType::INTERVAL_MASK_DAY
                                | SqlType::INTERVAL_MASK_HOUR
                                | SqlType::INTERVAL_MASK_MINUTE
                        )
            })
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::Cast(_, ty)
            if ty.as_builtin().is_some_and(|ty| {
                ty.kind == SqlTypeKind::Interval
                    && ty.interval_precision() == Some(2)
                    && ty.interval_range() == Some(SqlType::INTERVAL_MASK_SECOND)
            })
    ));
    assert!(matches!(
        &stmt.targets[2].expr,
        SqlExpr::Cast(_, ty)
            if *ty == RawTypeName::Builtin(SqlType::array_of(SqlType::new(SqlTypeKind::Interval)))
    ));
}

#[test]
fn parse_timestamptz_typed_string_literal_with_text_cast() {
    let stmt = parse_select("select timestamptz '2024-01-02 03:04:05+00'::text").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Cast(inner, ty) => {
            assert_eq!(*ty, SqlType::new(SqlTypeKind::Text));
            assert!(matches!(
                inner.as_ref(),
                SqlExpr::Cast(_, inner_ty)
                    if *inner_ty == SqlType::new(SqlTypeKind::TimestampTz)
            ));
        }
        other => panic!("expected outer text cast, got {other:?}"),
    }
}

#[test]
fn parse_select_star_with_extra_target() {
    let stmt = parse_select("select *, 'asphalt' from people").unwrap();
    assert_eq!(stmt.targets.len(), 2);
    assert!(matches!(&stmt.targets[0].expr, SqlExpr::Column(name) if name == "*"));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::Const(Value::Text(value)) if value.as_str() == "asphalt"
    ));
}

#[test]
fn parse_qualified_star_select_target() {
    let stmt = parse_select("select p.* from people p").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert_eq!(stmt.targets[0].output_name, "*");
    assert!(matches!(&stmt.targets[0].expr, SqlExpr::Column(name) if name == "p.*"));
}

#[test]
fn parse_qualified_star_inside_row_expr() {
    let stmt = parse_select("select row(p.*, 42) from people p").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    match &stmt.targets[0].expr {
        SqlExpr::Row(items) => {
            assert_eq!(items.len(), 2);
            assert!(matches!(&items[0], SqlExpr::Column(name) if name == "p.*"));
            assert!(matches!(&items[1], SqlExpr::IntegerLiteral(value) if value == "42"));
        }
        other => panic!("expected row expr, got {other:?}"),
    }
}

#[test]
fn parse_shift_expression_precedence() {
    let stmt = parse_select("select (-1::int2<<15)::text").unwrap();
    assert_eq!(stmt.targets[0].output_name, "text");
    match &stmt.targets[0].expr {
        SqlExpr::Cast(inner, ty) => {
            assert_eq!(*ty, SqlType::new(SqlTypeKind::Text));
            match inner.as_ref() {
                SqlExpr::Shl(left, right) => {
                    assert!(
                        matches!(right.as_ref(), SqlExpr::IntegerLiteral(value) if value == "15")
                    );
                    match left.as_ref() {
                        SqlExpr::Cast(inner, ty) => {
                            assert_eq!(*ty, SqlType::new(SqlTypeKind::Int2));
                            assert!(matches!(inner.as_ref(), SqlExpr::Negate(_)));
                        }
                        other => panic!("expected int2 cast on left side, got {other:?}"),
                    }
                }
                other => panic!("expected shift expression, got {other:?}"),
            }
        }
        other => panic!("expected outer cast expression, got {other:?}"),
    }
}

#[test]
fn parse_prefix_float_operator_sugar() {
    let stmt = parse_select("select @x, |/x, ||/x from metrics").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "abs" && args.args().len() == 1
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "sqrt" && args.args().len() == 1
    ));
    assert!(matches!(
        &stmt.targets[2].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "cbrt" && args.args().len() == 1
    ));
}

#[test]
fn parse_custom_prefix_operator_uses_full_token() {
    let stmt = parse_select("select @#@ 24, !=- 10").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::PrefixOperator { op, .. } if op == "@#@"
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::PrefixOperator { op, .. } if op == "!=-"
    ));
}

#[test]
fn parse_postgres_operator_edge_cases() {
    match parse_statement("create operator => (rightarg = int8, procedure = factorial)") {
        Err(ParseError::UnexpectedToken { actual, .. }) => {
            assert_eq!(actual, "syntax error at or near \"=>\"");
        }
        other => panic!("expected => syntax error, got {other:?}"),
    }

    match parse_statement("select 10 !=-;") {
        Err(ParseError::UnexpectedToken { actual, .. }) => {
            assert_eq!(actual, "syntax error at or near \";\"");
        }
        other => panic!("expected postfix operator syntax error, got {other:?}"),
    }

    for sql in [
        "select true<>-1 between 1 and 1",
        "select false<>1 between 1 and 1",
        "select false<=-1 between 1 and 1",
        "select false>=-1 between 1 and 1",
    ] {
        parse_select(sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
}

#[test]
fn parse_syntax_errors_carry_postgres_positions() {
    for (sql, token) in [
        ("select distinct from from tenk1", "from"),
        ("select distinct from tenk1", "from"),
        ("drop function 314159()", "314159"),
        ("drop aggregate 314159(integer)", "314159"),
        ("drop operator (integer, integer)", "("),
    ] {
        let err = parse_statement(sql).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("syntax error at or near \"{token}\"")
        );
        assert_eq!(err.position(), sql.find(token).map(|index| index + 1));
    }
}

#[test]
fn parse_end_of_input_syntax_errors_carry_position() {
    let sql = "CREATE TABLE";
    let err = parse_statement(sql).unwrap_err();

    assert!(err.to_string().contains("end of input"));
    assert_eq!(err.position(), Some(sql.len() + 1));
}

#[test]
fn parse_power_operator_and_in_list() {
    let stmt = parse_select("select x ^ '2.0', x in (0, 1, 2) from metrics").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "power" && args.args().len() == 2
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::QuantifiedArray { is_all: false, .. }
    ));
}

#[test]
fn parse_escape_and_dollar_quoted_strings() {
    let stmt = parse_select(
        r#"select E'abc\tdef', $$a'b$$, $tag$x
y$tag$"#,
    )
    .unwrap();
    assert_eq!(stmt.targets.len(), 3);
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::Const(Value::Text(text)) if text.as_str() == "abc\tdef"
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::Const(Value::Text(text)) if text.as_str() == "a'b"
    ));
    assert!(matches!(
        &stmt.targets[2].expr,
        SqlExpr::Const(Value::Text(text)) if text.as_str() == "x\ny"
    ));
}

#[test]
fn parse_concat_has_lower_precedence_than_addition() {
    let stmt = parse_select("select 'four: ' || 2 + 2").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Concat(_, right) => {
            assert!(matches!(**right, SqlExpr::Add(_, _)));
        }
        other => panic!("expected concat expression, got {other:?}"),
    }
}

#[test]
fn parse_extended_numeric_type_cast_expressions() {
    let stmt = parse_select(
            "select '7'::int2, '9'::bigint, '1.5'::real, '2.5'::double precision, '3.25'::numeric, '4.5'::decimal(10,2)",
        )
        .unwrap();
    assert_eq!(stmt.targets.len(), 6);
    let expected = [
        SqlTypeKind::Int2,
        SqlTypeKind::Int8,
        SqlTypeKind::Float4,
        SqlTypeKind::Float8,
        SqlTypeKind::Numeric,
        SqlTypeKind::Numeric,
    ];
    for (target, kind) in stmt.targets.iter().zip(expected) {
        match &target.expr {
            SqlExpr::Cast(_, ty) => assert_eq!(ty.as_builtin().map(|ty| ty.kind), Some(kind)),
            other => panic!("expected cast expression, got {other:?}"),
        }
    }
}

#[test]
fn parse_create_table_with_numeric_types() {
    let stmt = parse_statement(
        "create table metrics (a numeric, b decimal, c numeric(10), d decimal(12,4), e numeric[])",
    )
    .unwrap();
    match stmt {
        Statement::CreateTable(create) => {
            let columns = create.columns().collect::<Vec<_>>();
            assert_eq!(columns[0].ty, SqlType::new(SqlTypeKind::Numeric));
            assert_eq!(columns[1].ty, SqlType::new(SqlTypeKind::Numeric));
            assert_eq!(columns[2].ty, SqlType::with_numeric_precision_scale(10, 0));
            assert_eq!(columns[3].ty, SqlType::with_numeric_precision_scale(12, 4));
            assert_eq!(
                columns[4].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Numeric))
            );
        }
        other => panic!("expected create table statement, got {other:?}"),
    }
}

#[test]
fn parse_numeric_cast_typmods() {
    let stmt = parse_select("select '12.34'::numeric(8,2), '12'::decimal(5)").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::Cast(_, ty) => {
            assert_eq!(*ty, SqlType::with_numeric_precision_scale(8, 2));
        }
        other => panic!("expected cast expression, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::Cast(_, ty) => {
            assert_eq!(*ty, SqlType::with_numeric_precision_scale(5, 0));
        }
        other => panic!("expected cast expression, got {other:?}"),
    }
}

#[test]
fn parse_cross_join_with_aliases() {
    let stmt = parse_select("select p.name, q.name from people p, pets q").unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Join {
            left: Box::new(FromItem::Alias {
                source: Box::new(FromItem::Table {
                    name: "people".into(),
                    only: false,
                }),
                alias: "p".into(),
                column_aliases: AliasColumnSpec::None,
                preserve_source_names: false,
            }),
            right: Box::new(FromItem::Alias {
                source: Box::new(FromItem::Table {
                    name: "pets".into(),
                    only: false,
                }),
                alias: "q".into(),
                column_aliases: AliasColumnSpec::None,
                preserve_source_names: false,
            }),
            kind: JoinKind::Cross,
            constraint: JoinConstraint::None,
        })
    );
}

#[test]
fn parse_select_without_from() {
    let stmt = parse_select("select 1").unwrap();
    assert_eq!(stmt.from, None);
    assert_eq!(stmt.targets.len(), 1);
}

#[test]
fn parse_select_without_targets_but_with_from() {
    let stmt = parse_select("select from people").unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Table {
            name: "people".into(),
            only: false,
        })
    );
    assert!(stmt.targets.is_empty());
}

#[test]
fn unquoted_identifiers_fold_to_lowercase_for_relation_lookup() {
    let mut catalog = Catalog::default();
    catalog.insert("char_tbl", test_catalog_entry(15030, desc()));
    catalog.insert("varchar_tbl", test_catalog_entry(15031, desc()));
    catalog.insert("text_tbl", test_catalog_entry(15032, desc()));

    for sql in [
        "select id from CHAR_TBL",
        "select id from VARCHAR_TBL",
        "select id from TEXT_TBL",
    ] {
        let stmt = parse_select(sql).unwrap();
        assert!(build_plan(&stmt, &catalog).is_ok(), "{sql}");
    }
}

#[test]
fn quoted_identifiers_preserve_case() {
    let stmt = parse_select("select id from \"CHAR_TBL\"").unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Table {
            name: "CHAR_TBL".into(),
            only: false,
        })
    );
}

#[test]
fn parse_addition_in_where_clause() {
    let stmt =
        parse_select("select * from people, pets where pets.owner_id + 1 = people.id").unwrap();
    assert!(matches!(
        stmt.where_clause,
        Some(SqlExpr::Eq(left, _))
            if matches!(*left, SqlExpr::Add(_, _))
    ));
}

#[test]
fn parse_unary_minus_in_expression() {
    let stmt = parse_statement(
        "update pgbench_accounts set abalance = abalance + -1822 where aid = 82711",
    )
    .unwrap();
    match stmt {
        Statement::Update(UpdateStatement { assignments, .. }) => {
            assert!(matches!(
                &assignments[0].expr,
                SqlExpr::Add(_, right) if matches!(**right, SqlExpr::Negate(_))
            ));
        }
        other => panic!("expected update, got {:?}", other),
    }
}

#[test]
fn parse_array_subscript_expressions_and_targets() {
    let stmt = parse_select("select a[1], b[1:2], c[:], d[2:] from widgets").unwrap();
    assert_eq!(stmt.targets[0].output_name, "a");
    assert_eq!(stmt.targets[1].output_name, "b");
    assert_eq!(stmt.targets[2].output_name, "c");
    assert_eq!(stmt.targets[3].output_name, "d");
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::ArraySubscript { ref subscripts, .. }
            if subscripts.len() == 1 && !subscripts[0].is_slice && subscripts[0].upper.is_none()
    ));
    assert!(matches!(
        stmt.targets[1].expr,
        SqlExpr::ArraySubscript { ref subscripts, .. }
            if subscripts[0].is_slice && subscripts[0].upper.is_some()
    ));
    assert!(matches!(
        stmt.targets[2].expr,
        SqlExpr::ArraySubscript { ref subscripts, .. }
            if subscripts[0].is_slice && subscripts[0].lower.is_none() && subscripts[0].upper.is_none()
    ));
    assert!(matches!(
        stmt.targets[3].expr,
        SqlExpr::ArraySubscript { ref subscripts, .. }
            if subscripts[0].is_slice && subscripts[0].lower.is_some() && subscripts[0].upper.is_none()
    ));

    match parse_statement("update widgets set a[1] = 1, b[1:2] = array[1,2]").unwrap() {
        Statement::Update(UpdateStatement { assignments, .. }) => {
            assert_eq!(assignments[0].target.column, "a");
            assert_eq!(assignments[0].target.subscripts.len(), 1);
            assert!(assignments[0].target.field_path.is_empty());
            assert_eq!(assignments[0].target.indirection.len(), 1);
            assert_eq!(assignments[1].target.column, "b");
            assert_eq!(assignments[1].target.subscripts.len(), 1);
            assert!(assignments[1].target.field_path.is_empty());
            assert_eq!(assignments[1].target.indirection.len(), 1);
        }
        other => panic!("expected update, got {:?}", other),
    }

    match parse_statement("update widgets set a[1].q1 = 1").unwrap() {
        Statement::Update(UpdateStatement { assignments, .. }) => {
            assert_eq!(assignments[0].target.column, "a");
            assert_eq!(assignments[0].target.subscripts.len(), 1);
            assert_eq!(assignments[0].target.field_path, vec!["q1".to_string()]);
            assert_eq!(assignments[0].target.indirection.len(), 2);
        }
        other => panic!("expected update, got {:?}", other),
    }

    match parse_statement("insert into widgets (a[1], b[1:2]) values (1, array[1,2])").unwrap() {
        Statement::Insert(InsertStatement {
            columns: Some(columns),
            ..
        }) => {
            assert_eq!(columns[0].column, "a");
            assert_eq!(columns[0].subscripts.len(), 1);
            assert!(columns[0].field_path.is_empty());
            assert_eq!(columns[0].indirection.len(), 1);
            assert_eq!(columns[1].column, "b");
            assert_eq!(columns[1].subscripts.len(), 1);
            assert!(columns[1].field_path.is_empty());
            assert_eq!(columns[1].indirection.len(), 1);
        }
        other => panic!("expected insert, got {:?}", other),
    }

    match parse_statement("insert into widgets (a[1].q1) values (1)").unwrap() {
        Statement::Insert(InsertStatement {
            columns: Some(columns),
            ..
        }) => {
            assert_eq!(columns[0].column, "a");
            assert_eq!(columns[0].subscripts.len(), 1);
            assert_eq!(columns[0].field_path, vec!["q1".to_string()]);
            assert_eq!(columns[0].indirection.len(), 2);
        }
        other => panic!("expected insert, got {:?}", other),
    }

    match parse_statement("insert into widgets (f3.if2[1]) values ('foo')").unwrap() {
        Statement::Insert(InsertStatement {
            columns: Some(columns),
            ..
        }) => {
            assert_eq!(columns[0].column, "f3");
            assert_eq!(columns[0].field_path, vec!["if2".to_string()]);
            assert_eq!(columns[0].subscripts.len(), 1);
            assert_eq!(columns[0].indirection.len(), 2);
        }
        other => panic!("expected insert, got {:?}", other),
    }

    match parse_statement("insert into widgets (f4[1].if2[1]) values ('foo')").unwrap() {
        Statement::Insert(InsertStatement {
            columns: Some(columns),
            ..
        }) => {
            assert_eq!(columns[0].column, "f4");
            assert_eq!(columns[0].field_path, vec!["if2".to_string()]);
            assert_eq!(columns[0].subscripts.len(), 2);
            assert_eq!(columns[0].indirection.len(), 3);
            assert!(matches!(
                columns[0].indirection[0],
                crate::include::nodes::parsenodes::AssignmentTargetIndirection::Subscript(_)
            ));
            assert!(matches!(
                columns[0].indirection[1],
                crate::include::nodes::parsenodes::AssignmentTargetIndirection::Field(ref field)
                    if field == "if2"
            ));
            assert!(matches!(
                columns[0].indirection[2],
                crate::include::nodes::parsenodes::AssignmentTargetIndirection::Subscript(_)
            ));
        }
        other => panic!("expected insert, got {:?}", other),
    }
}

#[test]
fn parse_qualified_array_subscript_uses_base_column_name() {
    let stmt = parse_select("select w.data[1] from widgets w").unwrap();
    assert_eq!(stmt.targets[0].output_name, "data");
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::ArraySubscript { .. }
    ));
}

#[test]
fn parse_array_subscript_with_omitted_lower_bound_tracks_upper_bound() {
    let stmt = parse_select("select a[:3] from widgets").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::ArraySubscript { ref subscripts, .. }
            if subscripts.len() == 1
                && subscripts[0].is_slice
                && subscripts[0].lower.is_none()
                && subscripts[0].upper.is_some()
    ));
}

#[test]
fn parse_unary_plus_numeric_literal_and_new_operators() {
    let stmt =
        parse_select("select +1.5, 5 - 2, 3 * 4, 8 / 2, 9 % 4, 1 <= 2, 3 >= 2, 4 != 5").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::UnaryPlus(_)));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::Sub(_, _)));
    assert!(matches!(stmt.targets[2].expr, SqlExpr::Mul(_, _)));
    assert!(matches!(stmt.targets[3].expr, SqlExpr::Div(_, _)));
    assert!(matches!(stmt.targets[4].expr, SqlExpr::Mod(_, _)));
    assert!(matches!(stmt.targets[5].expr, SqlExpr::LtEq(_, _)));
    assert!(matches!(stmt.targets[6].expr, SqlExpr::GtEq(_, _)));
    assert!(matches!(stmt.targets[7].expr, SqlExpr::NotEq(_, _)));
}

#[test]
fn parse_prefixed_and_underscored_numeric_literals() {
    let stmt = parse_select(
        "select 0b100101, 0o273, 0x1EEE_FFFF, 1_000_000, \
                1_000.000_005, .000_005, 1_000.5e0_1",
    )
    .unwrap();
    assert!(matches!(&stmt.targets[0].expr, SqlExpr::IntegerLiteral(value) if value == "37"));
    assert!(matches!(&stmt.targets[1].expr, SqlExpr::IntegerLiteral(value) if value == "187"));
    assert!(
        matches!(&stmt.targets[2].expr, SqlExpr::IntegerLiteral(value) if value == "518979583")
    );
    assert!(
        matches!(&stmt.targets[3].expr, SqlExpr::IntegerLiteral(value) if value == "1_000_000")
    );
    assert!(
        matches!(&stmt.targets[4].expr, SqlExpr::NumericLiteral(value) if value == "1_000.000_005")
    );
    assert!(matches!(&stmt.targets[5].expr, SqlExpr::NumericLiteral(value) if value == ".000_005"));
    assert!(
        matches!(&stmt.targets[6].expr, SqlExpr::NumericLiteral(value) if value == "1_000.5e0_1")
    );
}

#[test]
fn parse_rejects_numeric_and_parameter_junk() {
    for (sql, message) in [
        (
            "select 123abc",
            "trailing junk after numeric literal at or near \"123abc\"",
        ),
        (
            "select 0x0o",
            "trailing junk after numeric literal at or near \"0x0o\"",
        ),
        (
            "select 0.a",
            "trailing junk after numeric literal at or near \"0.a\"",
        ),
        (
            "select 0.0a",
            "trailing junk after numeric literal at or near \"0.0a\"",
        ),
        (
            "select .0a",
            "trailing junk after numeric literal at or near \".0a\"",
        ),
        (
            "select 0.0e1a",
            "trailing junk after numeric literal at or near \"0.0e1a\"",
        ),
        (
            "select 0.0e",
            "trailing junk after numeric literal at or near \"0.0e\"",
        ),
        ("select 0b", "invalid binary integer at or near \"0b\""),
        (
            "select 1b",
            "trailing junk after numeric literal at or near \"1b\"",
        ),
        (
            "select 0b0x",
            "trailing junk after numeric literal at or near \"0b0x\"",
        ),
        ("select 0o", "invalid octal integer at or near \"0o\""),
        (
            "select 1o",
            "trailing junk after numeric literal at or near \"1o\"",
        ),
        (
            "select 0o0x",
            "trailing junk after numeric literal at or near \"0o0x\"",
        ),
        ("select 0x", "invalid hexadecimal integer at or near \"0x\""),
        (
            "select 1x",
            "trailing junk after numeric literal at or near \"1x\"",
        ),
        (
            "select 0x0y",
            "trailing junk after numeric literal at or near \"0x0y\"",
        ),
        (
            "select 0.0e+",
            "trailing junk after numeric literal at or near \"0.0e+\"",
        ),
        (
            "select 0.0e+a",
            "trailing junk after numeric literal at or near \"0.0e+\"",
        ),
        (
            "select 100_",
            "trailing junk after numeric literal at or near \"100_\"",
        ),
        (
            "select 100__000",
            "trailing junk after numeric literal at or near \"100__000\"",
        ),
        ("select _1_000.5", "syntax error at or near \".5\""),
        (
            "select 1_000_.5",
            "trailing junk after numeric literal at or near \"1_000_\"",
        ),
        (
            "select 1_000._5",
            "trailing junk after numeric literal at or near \"1_000._5\"",
        ),
        (
            "select 1_000.5_",
            "trailing junk after numeric literal at or near \"1_000.5_\"",
        ),
        (
            "select 1_000.5e_1",
            "trailing junk after numeric literal at or near \"1_000.5e_1\"",
        ),
        (
            "prepare p1 as select $1a",
            "trailing junk after parameter at or near \"$1a\"",
        ),
        (
            "prepare p1 as select $0_1",
            "trailing junk after parameter at or near \"$0_1\"",
        ),
        (
            "prepare p1 as select $2147483648",
            "parameter number too large at or near \"$2147483648\"",
        ),
    ] {
        match parse_statement(sql) {
            Err(ParseError::UnexpectedToken { actual, .. }) => assert_eq!(actual, message),
            other => panic!("expected lexer error for {sql}, got {other:?}"),
        }
    }
}

#[test]
fn float_mod_is_rejected_at_bind_time() {
    let err = build_plan(
        &parse_select("select 1.5::real % 1.0::real").unwrap(),
        &catalog(),
    )
    .unwrap_err();
    assert!(matches!(err, ParseError::UndefinedOperator { op: "%", .. }));
}

#[test]
fn build_plan_accepts_catalog_backed_text_and_bool_comparisons() {
    assert!(build_plan(
        &parse_select(
            "select 'a' < 'b', 'a' >= 'b', true = false, '{\"a\":1}'::jsonb = '{\"a\":1}'::jsonb"
        )
        .unwrap(),
        &catalog(),
    )
    .is_ok());
}

#[test]
fn build_plan_rejects_missing_catalog_comparison_operator() {
    let err = build_plan(
        &parse_select("select '{\"a\":1}'::json = '{\"a\":1}'::json").unwrap(),
        &catalog(),
    )
    .unwrap_err();
    assert!(matches!(err, ParseError::UndefinedOperator { op: "=", .. }));
}

#[test]
fn build_plan_accepts_catalog_backed_text_input_casts() {
    assert!(build_plan(
        &parse_select(
            "select jsonb('{\"a\":1}'), '$.a'::jsonpath, cast('0101' as bit varying(8)), timestamp('2024-01-02 03:04:05')"
        )
        .unwrap(),
        &catalog(),
    )
    .is_ok());
}

#[test]
fn build_plan_accepts_catalog_backed_time_casts_and_comparisons() {
    assert!(
        build_plan(
            &parse_select("select time('05:06:07'), time '05:06:07', time '05:06:07' < '06:07:08'")
                .unwrap(),
            &catalog(),
        )
        .is_ok()
    );
}

#[test]
fn build_plan_coerces_time_comparison_string_literals() {
    let mut catalog = catalog();
    catalog.insert(
        "time_tbl",
        test_catalog_entry(
            15040,
            RelationDesc {
                columns: vec![column_desc("f1", SqlType::new(SqlTypeKind::Time), false)],
            },
        ),
    );
    let plan = build_plan(
        &parse_select("select * from time_tbl where f1 < '05:06:07'").unwrap(),
        &catalog,
    )
    .unwrap();
    let Plan::Filter { predicate, .. } = plan else {
        panic!("expected filter plan");
    };
    assert!(matches!(
        predicate,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::Lt
                && matches!(op.args.as_slice(), [Expr::Var(var), right]
                    if var.vartype == SqlType::new(SqlTypeKind::Time)
                        && (matches!(
                            right,
                            Expr::Cast(inner, ty)
                                if *ty == SqlType::new(SqlTypeKind::Time)
                                    && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))
                        ) || matches!(right, Expr::Const(Value::Time(_)))))
    ));
}

#[test]
fn build_plan_rejects_ambiguous_time_addition() {
    match build_plan(
        &parse_select("select time '01:02' + time '03:04'").unwrap(),
        &catalog(),
    )
    .unwrap_err()
    {
        ParseError::DetailedError {
            message,
            hint,
            sqlstate,
            ..
        } => {
            assert_eq!(
                message,
                "operator is not unique: time without time zone + time without time zone"
            );
            assert_eq!(
                hint.as_deref(),
                Some(
                    "Could not choose a best candidate operator. You might need to add explicit type casts."
                )
            );
            assert_eq!(sqlstate, "42725");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }
}

#[test]
fn build_plan_accepts_catalog_backed_bit_comparisons() {
    assert!(build_plan(
        &parse_select(
            "select '0101'::bit(4) = '0101'::bit(4), '0101'::bit(4) < '1111'::bit(4), cast('0101' as bit varying(8)) <= cast('1111' as bit varying(8))"
        )
        .unwrap(),
        &catalog(),
    )
    .is_ok());
}

#[test]
fn build_plan_accepts_catalog_backed_bytea_comparisons() {
    assert!(
        build_plan(
            &parse_select(
                r"select E'\\x01'::bytea = E'\\x01'::bytea, E'\\x01'::bytea < E'\\x02'::bytea"
            )
            .unwrap(),
            &catalog(),
        )
        .is_ok()
    );
}

#[test]
fn build_plan_accepts_same_type_array_comparisons() {
    assert!(build_plan(
        &parse_select(
            "select ARRAY[1, 2] = ARRAY[1, 2], ARRAY['a']::varchar[] <> ARRAY['b']::varchar[], ARRAY[1, 2] < ARRAY[2, 1], ARRAY['a']::varchar[] >= ARRAY['a']::varchar[]"
        )
        .unwrap(),
        &catalog(),
    )
    .is_ok());
}

#[test]
fn build_plan_coerces_unknown_string_literals_for_array_ops() {
    let plan = build_plan(
        &parse_select(
            "select ARRAY[1, 2] = '{1,2}', ARRAY[1, 2] && '{2,3}', ARRAY[1, 2] @> '{2}', 2 = any ('{1,2,3}')",
        )
        .unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &targets[0].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::Eq
                && matches!(op.args.as_slice(), [left, right]
                    if matches!(left, Expr::ArrayLiteral { array_type, .. }
                if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
                && (matches!(
                    right,
                    Expr::Cast(inner, ty)
                        if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                            && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))
                ) || matches!(right, Expr::Const(Value::PgArray(_)))))
    ));
    assert!(matches!(
        &targets[1].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::ArrayOverlap
                && matches!(op.args.as_slice(), [left, right]
                    if matches!(left, Expr::ArrayLiteral { array_type, .. }
                if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
                && (matches!(
                    right,
                    Expr::Cast(inner, ty)
                        if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                            && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))
                ) || matches!(right, Expr::Const(Value::PgArray(_)))))
    ));
    assert!(matches!(
        &targets[2].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::ArrayContains
                && matches!(op.args.as_slice(), [left, right]
                    if matches!(left, Expr::ArrayLiteral { array_type, .. }
                if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
                && (matches!(
                    right,
                    Expr::Cast(inner, ty)
                        if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                            && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))
                ) || matches!(right, Expr::Const(Value::PgArray(_)))))
    ));
    assert!(matches!(
        &targets[3].expr,
        Expr::ScalarArrayOp(saop)
            if saop.use_or
                && (matches!(
                    saop.right.as_ref(),
                    Expr::Cast(inner, ty)
                        if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                            && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))
                ) || matches!(saop.right.as_ref(), Expr::Const(Value::PgArray(_))))
    ));
}

#[test]
fn build_plan_handles_in_list_nulls_and_not_in_operator() {
    let plan = build_plan(
        &parse_select("select 1 in (null, null), 1 not in (2, null)").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };

    assert!(matches!(
        &targets[0].expr,
        Expr::ScalarArrayOp(saop)
            if saop.op == SubqueryComparisonOp::Eq
                && saop.use_or
                && matches!(saop.right.as_ref(), Expr::ArrayLiteral { array_type, .. }
                    if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
    ));
    assert!(matches!(
        &targets[1].expr,
        Expr::ScalarArrayOp(saop)
            if saop.op == SubqueryComparisonOp::NotEq
                && !saop.use_or
                && matches!(saop.right.as_ref(), Expr::ArrayLiteral { array_type, .. }
                    if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
    ));

    let err = build_plan(
        &parse_select("select '(0,0)'::point in ('(0,0,0,0)'::box, point(0,0))").unwrap(),
        &catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UndefinedOperator {
            op: "=",
            left_type,
            right_type,
        } if left_type == "point" && right_type == "box"
    ));
}

#[test]
fn build_plan_in_list_common_type_includes_left_operand() {
    let plan = build_plan(
        &parse_select("select random() in (1, 4, 8.0), random()::int in (1, 4, 8.0)").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };

    assert!(matches!(
        &targets[0].expr,
        Expr::ScalarArrayOp(saop)
            if matches!(saop.right.as_ref(), Expr::ArrayLiteral { array_type, .. }
                if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Float8)))
    ));
    assert!(matches!(
        &targets[1].expr,
        Expr::ScalarArrayOp(saop)
            if matches!(saop.left.as_ref(), Expr::Cast(_, ty)
                if *ty == SqlType::new(SqlTypeKind::Numeric))
                && matches!(saop.right.as_ref(), Expr::ArrayLiteral { array_type, .. }
                    if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Numeric)))
    ));
}

#[test]
fn build_plan_binds_stats_ext_any_and_function_predicates() {
    let plan = build_plan(
        &parse_select(
            "select (id * 2) < any (array[2, 102]), upper(name) > '1', \
             (id * 2) < any (array[2, 102]) and upper(name) > '1' from people",
        )
        .unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };

    assert_eq!(targets.len(), 3);
    assert!(matches!(
        &targets[0].expr,
        Expr::ScalarArrayOp(saop)
            if saop.op == SubqueryComparisonOp::Lt
                && saop.use_or
                && matches!(saop.right.as_ref(), Expr::ArrayLiteral { array_type, .. }
                    if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
    ));
    assert!(matches!(
        &targets[1].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::Gt
                && matches!(op.args.as_slice(), [Expr::Func(func), _]
                    if func.implementation
                        == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                            crate::include::nodes::primnodes::BuiltinScalarFunction::Upper
                        ))
    ));
    assert!(matches!(
        &targets[2].expr,
        Expr::Bool(bool_expr)
            if bool_expr.boolop == crate::include::nodes::primnodes::BoolExprType::And
                && bool_expr.args.len() == 2
    ));
}

#[test]
fn analyze_timestamptz_typed_string_literal_keeps_timestamp_tz_type() {
    let stmt = parse_select("select timestamptz '2024-01-02 03:04:05+00'").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    assert!(matches!(
        &query.target_list[0].expr,
        Expr::Cast(inner, ty)
            if *ty == SqlType::new(SqlTypeKind::TimestampTz)
                && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))
    ));
}

#[test]
fn parse_at_time_zone_expression() {
    let stmt = parse_select("select timestamp '2001-02-16 20:38:40' at time zone 'America/Denver'")
        .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::AtTimeZone { expr, zone }
            if matches!(expr.as_ref(), SqlExpr::Cast(_, ty) if *ty == SqlType::new(SqlTypeKind::Timestamp))
                && matches!(zone.as_ref(), SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _)))
    ));
}

#[test]
fn parse_overlaps_expression() {
    let stmt = parse_select(
        "select (timestamp '2000-11-27', timestamp '2000-11-28') overlaps (timestamp '2000-11-27 12:00', interval '1 day')",
    )
    .unwrap();
    assert!(matches!(&stmt.targets[0].expr, SqlExpr::Overlaps(_, _)));
}

#[test]
fn analyze_at_time_zone_uses_timezone_function_types() {
    let stmt =
        parse_select("select timestamptz '2001-02-16 20:38:40+00' at time zone 'America/Denver'")
            .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert!(matches!(
        &query.target_list[0].expr,
        Expr::Func(func)
            if func.implementation == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                crate::include::nodes::primnodes::BuiltinScalarFunction::Timezone
            )
                && func.funcresulttype == Some(SqlType::new(SqlTypeKind::Timestamp))
                && func.args.len() == 2
    ));
}

#[test]
fn analyze_timetz_at_time_zone_keeps_timetz_and_interval_types() {
    let stmt = parse_select("select timetz '00:01-07' at time zone interval '00:00'").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    let Expr::Func(func) = &query.target_list[0].expr else {
        panic!("expected timezone function");
    };
    assert_eq!(
        func.implementation,
        crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
            crate::include::nodes::primnodes::BuiltinScalarFunction::Timezone
        )
    );
    assert_eq!(func.funcresulttype, Some(SqlType::new(SqlTypeKind::TimeTz)));
    assert!(matches!(
        func.args.as_slice(),
        [
            Expr::Const(Value::Interval(_))
                | Expr::Cast(
                    _,
                    SqlType {
                        kind: SqlTypeKind::Interval,
                        ..
                    }
                ),
            Expr::Const(Value::TimeTz(_))
                | Expr::Cast(
                    _,
                    SqlType {
                        kind: SqlTypeKind::TimeTz,
                        ..
                    }
                )
        ]
    ));
}

#[test]
fn analyze_date_time_arithmetic_uses_postgres_result_types() {
    let stmt = parse_select(
        "select date '2001-01-02' + time '03:04', date '2001-01-02' + timetz '03:04+02', date '2001-01-02' - time '03:04'",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    assert!(matches!(
        &query.target_list[0].expr,
        Expr::Op(op) if op.opresulttype == SqlType::new(SqlTypeKind::Timestamp)
    ));
    assert!(matches!(
        &query.target_list[1].expr,
        Expr::Op(op) if op.opresulttype == SqlType::new(SqlTypeKind::TimestampTz)
    ));
    assert!(matches!(
        &query.target_list[2].expr,
        Expr::Op(op) if op.opresulttype == SqlType::new(SqlTypeKind::Timestamp)
    ));

    let err = analyze_select_query_with_outer(
        &parse_select("select date '2001-01-02' - timetz '03:04+02'").unwrap(),
        &catalog(),
        &[],
        None,
        None,
        &[],
        &[],
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UndefinedOperator {
            op: "-",
            left_type,
            right_type,
        } if left_type == "date" && right_type == "time with time zone"
    ));
}

#[test]
fn analyze_rejects_unsupported_timetz_interval_casts_with_postgres_error() {
    for (sql, expected) in [
        (
            "select cast(time with time zone '01:02-08' as interval)",
            "cannot cast type time with time zone to interval",
        ),
        (
            "select cast(interval '02:03' as time with time zone)",
            "cannot cast type interval to time with time zone",
        ),
    ] {
        let err = analyze_select_query_with_outer(
            &parse_select(sql).unwrap(),
            &catalog(),
            &[],
            None,
            None,
            &[],
            &[],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ParseError::DetailedError {
                message,
                sqlstate: "42846",
                ..
            } if message == expected
        ));
    }
}

#[test]
fn analyze_timestamptz_date_time_constructor_overloads() {
    let stmt = parse_select(
        "select timestamptz(date '2001-01-02', time '03:04'), timestamptz(date '2001-01-02', timetz '03:04+02')",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    for target in &query.target_list {
        assert!(matches!(
            &target.expr,
            Expr::Func(func)
                if func.implementation == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                    crate::include::nodes::primnodes::BuiltinScalarFunction::TimestampTzConstructor
                )
                    && func.funcresulttype == Some(SqlType::new(SqlTypeKind::TimestampTz))
        ));
    }
}

#[test]
fn analyze_mixed_date_timestamp_comparisons_keep_cross_type_ops() {
    let stmt = parse_select(
        "select date '2001-01-02' < timestamp '2001-01-03', date '2001-01-02' <= timestamptz '2001-01-03 00:00+00', timestamp '2001-01-02' = timestamptz '2001-01-02 00:00+00'",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert!(matches!(
        &query.target_list[0].expr,
        Expr::Op(op)
            if matches!(op.args.as_slice(), [
                Expr::Cast(_, SqlType { kind: SqlTypeKind::Date, .. }) | Expr::Const(Value::Date(_)),
                Expr::Cast(_, SqlType { kind: SqlTypeKind::Timestamp, .. }) | Expr::Const(Value::Timestamp(_)),
            ])
    ));
    assert!(matches!(
        &query.target_list[1].expr,
        Expr::Op(op)
            if matches!(op.args.as_slice(), [
                Expr::Cast(_, SqlType { kind: SqlTypeKind::Date, .. }) | Expr::Const(Value::Date(_)),
                Expr::Cast(_, SqlType { kind: SqlTypeKind::TimestampTz, .. }) | Expr::Const(Value::TimestampTz(_)),
            ])
    ));
    assert!(matches!(
        &query.target_list[2].expr,
        Expr::Op(op)
            if op.args.iter().all(|arg| matches!(
                arg,
                Expr::Const(Value::TimestampTz(_))
                    | Expr::Cast(_, SqlType { kind: SqlTypeKind::TimestampTz, .. })
            ))
    ));
}

#[test]
fn analyze_interval_typed_string_literal_keeps_interval_type() {
    let stmt = parse_select("select interval '1 day'").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    assert!(matches!(
        &query.target_list[0].expr,
        Expr::Cast(inner, ty)
            if *ty == SqlType::new(SqlTypeKind::Interval)
                && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))
    ));
}

#[test]
fn build_plan_accepts_catalog_backed_text_array_casts() {
    assert!(
        build_plan(
            &parse_select("select cast('{1,2}' as int4[]), cast('{\"a\",\"b\"}' as varchar[])")
                .unwrap(),
            &catalog(),
        )
        .is_ok()
    );
}

#[test]
fn analyze_interval_array_text_cast_keeps_outer_text_cast() {
    let stmt =
        parse_select("select '{0 second,1 hour 42 minutes 20 seconds}'::interval[]::text").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    assert!(matches!(
        &query.target_list[0].expr,
        Expr::Cast(inner, ty)
            if *ty == SqlType::new(SqlTypeKind::Text)
                && matches!(
                    inner.as_ref(),
                    Expr::Cast(_, inner_ty) if *inner_ty == SqlType::array_of(SqlType::new(SqlTypeKind::Interval))
                )
    ));
}

#[test]
fn build_plan_rejects_missing_visible_catalog_text_input_cast() {
    let visible = visible_catalog_without_text_input_cast(crate::include::catalog::JSONB_TYPE_OID);
    let err = build_plan(
        &parse_select("select cast('{\"a\":1}' as jsonb)").unwrap(),
        &visible,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UnexpectedToken { actual, .. } if actual == "cannot cast type text to jsonb"
    ));
}

#[test]
fn build_plan_rejects_missing_visible_catalog_comparison_operator() {
    let visible = visible_catalog_without_operator(
        "<",
        crate::include::catalog::BYTEA_TYPE_OID,
        crate::include::catalog::BYTEA_TYPE_OID,
    );
    let err = build_plan(
        &parse_select(r"select E'\\x01'::bytea < E'\\x02'::bytea").unwrap(),
        &visible,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UndefinedOperator { op, left_type, right_type }
            if op == "<" && left_type == "bytea" && right_type == "bytea"
    ));
}

#[test]
fn build_plan_resolves_lower_for_range_type() {
    let plan = build_plan(
        &parse_select("select lower(int4range(1, 10))").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &targets[0].expr,
        Expr::Func(func)
            if func.implementation
                == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                    crate::include::nodes::primnodes::BuiltinScalarFunction::RangeLower
                )
                && func.funcresulttype == Some(SqlType::new(SqlTypeKind::Int4))
    ));
}

#[test]
fn build_plan_dispatches_jsonb_and_range_contains_independently() {
    let array_plan = build_plan(
        &parse_select("select ARRAY[1, 2] @> ARRAY[2], ARRAY[2] <@ ARRAY[1, 2]").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection {
        targets: array_targets,
        ..
    } = array_plan
    else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &array_targets[0].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::ArrayContains
    ));
    assert!(matches!(
        &array_targets[1].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::ArrayContained
    ));

    let json_plan = build_plan(
        &parse_select("select '{\"a\":1}'::jsonb @> '{\"a\":1}'::jsonb").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection {
        targets: json_targets,
        ..
    } = json_plan
    else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &json_targets[0].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::JsonbContains
    ));

    let range_plan = build_plan(
        &parse_select("select int4range(1, 4) @> 2").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection {
        targets: range_targets,
        ..
    } = range_plan
    else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &range_targets[0].expr,
        Expr::Func(func)
            if func.implementation
                == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                    crate::include::nodes::primnodes::BuiltinScalarFunction::RangeContains
                )
                && func.funcresulttype == Some(SqlType::new(SqlTypeKind::Bool))
    ));
}

#[test]
fn build_plan_dispatches_jsonb_populate_record_as_builtin() {
    let plan = build_plan(
        &parse_select("select jsonb_populate_record(row(1,2), '{\"f1\":0,\"f2\":1}')").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };
    assert!(
        matches!(
            &targets[0].expr,
            Expr::Func(func)
                if func.implementation
                    == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                        crate::include::nodes::primnodes::BuiltinScalarFunction::JsonbPopulateRecord
                    )
        ),
        "expr: {:#?}",
        targets[0].expr
    );
}

#[test]
fn build_plan_binds_pg_trigger_depth_as_builtin() {
    let plan = build_plan(
        &parse_select("select pg_trigger_depth()").unwrap(),
        &catalog(),
    )
    .unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };
    assert!(
        matches!(
            &targets[0].expr,
            Expr::Func(func)
                if func.implementation
                    == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                        crate::include::nodes::primnodes::BuiltinScalarFunction::PgTriggerDepth
                    )
        ),
        "expr: {:#?}",
        targets[0].expr
    );
}

#[test]
fn build_plan_dispatches_geometry_and_range_position_operators_independently() {
    let catalog = catalog_with_operator_dispatch_table();
    let geometry_plan = build_plan(
        &parse_select("select left_box &< right_box from ops").unwrap(),
        &catalog,
    )
    .unwrap();
    let Plan::Projection {
        targets: geometry_targets,
        ..
    } = geometry_plan
    else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &geometry_targets[0].expr,
        Expr::Func(func)
            if func.implementation
                == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                    crate::include::nodes::primnodes::BuiltinScalarFunction::GeoOverLeft
                )
    ));

    let overlap_plan = build_plan(
        &parse_select("select left_box && right_box from ops").unwrap(),
        &catalog,
    )
    .unwrap();
    let Plan::Projection {
        targets: overlap_targets,
        ..
    } = overlap_plan
    else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &overlap_targets[0].expr,
        Expr::Func(func)
            if func.implementation
                == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                    crate::include::nodes::primnodes::BuiltinScalarFunction::GeoOverlap
                )
    ));

    let range_plan = build_plan(
        &parse_select("select left_range &< right_range from ops").unwrap(),
        &catalog,
    )
    .unwrap();
    let Plan::Projection {
        targets: range_targets,
        ..
    } = range_plan
    else {
        panic!("expected projection plan");
    };
    assert!(matches!(
        &range_targets[0].expr,
        Expr::Func(func)
            if func.implementation
                == crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                    crate::include::nodes::primnodes::BuiltinScalarFunction::RangeOverLeft
                )
    ));
}

#[test]
fn build_plan_rejects_lseg_point_intersection_operator() {
    let err = build_plan(
        &parse_select("select '[(0,0),(1,1)]'::lseg # '(0,0)'::point").unwrap(),
        &catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UndefinedOperator { op, left_type, right_type }
            if op == "#" && left_type == "lseg" && right_type == "point"
    ));
}

#[test]
fn build_plan_rejects_mixed_range_kinds() {
    let err = build_plan(
        &parse_select("select int4range(1, 4) = numrange(1.0, 4.0)").unwrap(),
        &catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UndefinedOperator { op, left_type, right_type }
            if op == "=" && left_type == "int4range" && right_type == "numrange"
    ));
}

#[test]
fn parse_select_with_order_limit_offset() {
    let stmt = parse_select("select name from people order by id desc limit 2 offset 1").unwrap();
    assert_eq!(stmt.order_by.len(), 1);
    assert!(stmt.order_by[0].descending);
    assert_eq!(stmt.order_by[0].nulls_first, None);
    assert_eq!(stmt.limit, Some(2));
    assert_eq!(stmt.offset, Some(1));
}

#[test]
fn parse_select_with_explicit_nulls_ordering() {
    let stmt = parse_select("select name from people order by note desc nulls last").unwrap();
    assert_eq!(stmt.order_by.len(), 1);
    assert!(stmt.order_by[0].descending);
    assert_eq!(stmt.order_by[0].nulls_first, Some(false));
}

#[test]
fn build_plan_tracks_order_by_collation_for_aliases_and_ordinals() {
    fn order_by_items(plan: &Plan) -> &[crate::include::nodes::primnodes::OrderByEntry] {
        match plan {
            Plan::Projection { input, .. }
            | Plan::Filter { input, .. }
            | Plan::Limit { input, .. }
            | Plan::LockRows { input, .. } => order_by_items(input),
            Plan::OrderBy { items, .. } => items,
            other => panic!("expected ORDER BY plan node, got {other:?}"),
        }
    }

    let stmt = parse_select(
        "select name as alias, note from people order by alias collate \"C\", 2 collate \"POSIX\"",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    let items = order_by_items(&plan);
    assert_eq!(
        items[0].collation_oid,
        Some(crate::include::catalog::C_COLLATION_OID)
    );
    assert_eq!(
        items[1].collation_oid,
        Some(crate::include::catalog::POSIX_COLLATION_OID)
    );
}

#[test]
fn build_plan_tracks_expr_collations_on_bound_nodes() {
    let stmt = parse_select(
        "select \
            name collate \"C\" = note collate \"C\", \
            name collate \"C\" like 'a%', \
            name collate \"POSIX\" similar to 'a.*', \
            name collate \"C\" = any (array['alice']) \
         from people",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    let Plan::Projection { targets, .. } = plan else {
        panic!("expected projection plan");
    };

    assert!(matches!(
        &targets[0].expr,
        Expr::Op(op) if op.collation_oid == Some(crate::include::catalog::C_COLLATION_OID)
    ));
    assert!(matches!(
        &targets[1].expr,
        Expr::Like { collation_oid, .. }
            if *collation_oid == Some(crate::include::catalog::C_COLLATION_OID)
    ));
    assert!(matches!(
        &targets[2].expr,
        Expr::Similar { collation_oid, .. }
            if *collation_oid == Some(crate::include::catalog::POSIX_COLLATION_OID)
    ));
    assert!(matches!(
        &targets[3].expr,
        Expr::ScalarArrayOp(saop)
            if saop.collation_oid == Some(crate::include::catalog::C_COLLATION_OID)
    ));
}

#[test]
fn build_plan_rejects_invalid_collation_usage() {
    let stmt = parse_select("select 1 collate \"C\"").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message == "collations are not supported by type integer"
                && sqlstate == "42804"
    ));

    let stmt =
        parse_select("select name collate \"C\" = note collate \"POSIX\" from people").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message
                == "collation mismatch between explicit collations \"C\" and \"POSIX\""
                && sqlstate == "42P21"
    ));
}

#[test]
fn build_plan_resolves_columns() {
    let stmt = parse_select("select name, note from people where id > 1").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 2);
            match *input {
                Plan::Filter {
                    input, predicate, ..
                } => {
                    assert!(matches!(
                        predicate,
                        Expr::Op(op) if op.op == crate::include::nodes::primnodes::OpExprKind::Gt
                    ));
                    assert!(matches!(*input, Plan::SeqScan { .. }));
                }
                other => panic!("expected filter, got {:?}", other),
            }
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_resolves_aliased_columns() {
    let stmt = parse_select("select s.name from people s where s.id > 1").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].name, "name");
            match *input {
                Plan::Filter { predicate, .. } => {
                    assert!(matches!(
                        predicate,
                        Expr::Op(op) if op.op == crate::include::nodes::primnodes::OpExprKind::Gt
                    ));
                }
                other => panic!("expected filter, got {:?}", other),
            }
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_constant_folds_nullif_filter_to_false() {
    let stmt = parse_select("select * from people where nullif(1, 2) = 2").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();

    match plan {
        Plan::Filter {
            predicate, input, ..
        } => {
            assert_eq!(predicate, Expr::Const(Value::Bool(false)));
            assert!(matches!(*input, Plan::SeqScan { .. }));
        }
        other => panic!("expected filter, got {:?}", other),
    }
}

#[test]
fn build_plan_case_raises_reachable_division_by_zero() {
    let stmt = parse_select("select case when id > 0 then id else 1/0 end from people").unwrap();

    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message == "division by zero" && sqlstate == "22012"
    ));
}

#[test]
fn build_plan_case_skips_unreachable_else_division_by_zero() {
    let stmt =
        parse_select("select case when 1 = 0 then 1/0 when 1 = 1 then 1 else 2/0 end").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();

    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].expr, Expr::Const(Value::Int32(1)));
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_simple_case_skips_unreachable_else_division_by_zero() {
    let stmt = parse_select("select case 1 when 0 then 1/0 when 1 then 1 else 2/0 end").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();

    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].expr, Expr::Const(Value::Int32(1)));
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_join_plan_resolves_qualified_columns() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select people.name, pets.id from people join pets on people.id = pets.owner_id",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 2);
            match strip_projections(&input) {
                Plan::NestedLoopJoin {
                    join_qual, qual, ..
                } => {
                    assert!(qual.is_empty());
                    assert!(matches!(
                        join_qual.as_slice(),
                        [Expr::Op(op)] if op.op == crate::include::nodes::primnodes::OpExprKind::Eq
                    ))
                }
                Plan::HashJoin {
                    kind,
                    hash_clauses,
                    join_qual,
                    qual,
                    ..
                } => {
                    assert_eq!(*kind, JoinType::Inner);
                    assert_eq!(hash_clauses.len(), 1);
                    assert!(join_qual.is_empty());
                    assert!(qual.is_empty());
                    assert!(matches!(
                        hash_clauses.first(),
                        Some(Expr::Op(op))
                            if op.op == crate::include::nodes::primnodes::OpExprKind::Eq
                    ));
                }
                Plan::MergeJoin {
                    kind,
                    merge_clauses,
                    join_qual,
                    qual,
                    ..
                } => {
                    assert_eq!(*kind, JoinType::Inner);
                    assert_eq!(merge_clauses.len(), 1);
                    assert!(join_qual.is_empty());
                    assert!(qual.is_empty());
                    assert!(matches!(
                        merge_clauses.first(),
                        Some(Expr::Op(op))
                            if op.op == crate::include::nodes::primnodes::OpExprKind::Eq
                    ));
                }
                other => panic!("expected join, got {:?}", other),
            }
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_left_join_plan_uses_hash_join_for_equijoin() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select people.name, pets.id from people left join pets on people.id = pets.owner_id",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 2);
            match strip_projections(&input) {
                Plan::HashJoin {
                    kind,
                    hash_clauses,
                    join_qual,
                    qual,
                    ..
                } => {
                    assert_eq!(*kind, JoinType::Left);
                    assert_eq!(hash_clauses.len(), 1);
                    assert!(join_qual.is_empty());
                    assert!(qual.is_empty());
                }
                other => panic!("expected hash join, got {:?}", other),
            }
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_non_equi_join_plan_stays_nested_loop() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select people.name, pets.id from people join pets on people.id > pets.owner_id",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 2);
            match strip_projections(&input) {
                Plan::NestedLoopJoin {
                    join_qual, qual, ..
                } => {
                    assert!(qual.is_empty());
                    assert!(matches!(
                        join_qual.as_slice(),
                        [Expr::Op(op)] if op.op == crate::include::nodes::primnodes::OpExprKind::Gt
                    ))
                }
                other => panic!("expected nested loop join, got {:?}", other),
            }
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn unknown_column_is_rejected() {
    let stmt = parse_select("select missing from people").unwrap();
    assert!(
        matches!(build_plan(&stmt, &catalog()), Err(ParseError::UnknownColumn(name)) if name == "missing")
    );
}

#[test]
fn select_star_expands_to_all_columns() {
    let stmt = parse_select("select * from people").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    // select * is an identity projection — optimized away to bare SeqScan
    assert!(
        matches!(plan, Plan::SeqScan { .. }),
        "expected SeqScan (identity projection elided), got {:?}",
        plan
    );
}

#[test]
fn select_star_with_extra_target_builds_projection() {
    let stmt = parse_select("select *, 'asphalt' from people").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 4);
            assert_eq!(targets.last().unwrap().name, "?column?");
            assert!(matches!(
                &targets.last().unwrap().expr,
                Expr::Const(Value::Text(value)) if value.as_str() == "asphalt"
            ));
        }
        other => panic!("expected projection, got {other:?}"),
    }
}

#[test]
fn build_plan_wraps_order_by_and_limit() {
    let stmt =
        parse_select("select name from people where id > 0 order by id desc limit 2 offset 1")
            .unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 1);
            match *input {
                Plan::Limit {
                    input,
                    limit,
                    offset,
                    ..
                } => {
                    assert_eq!(limit, Some(2));
                    assert_eq!(offset, 1);
                    match strip_projections(input.as_ref()) {
                        Plan::OrderBy { input, items, .. } => {
                            assert_eq!(items.len(), 1);
                            assert!(items[0].descending);
                            assert!(matches!(input.as_ref(), Plan::Filter { .. }));
                        }
                        other => panic!("expected order by, got {:?}", other),
                    }
                }
                other => panic!("expected limit, got {:?}", other),
            }
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_resolves_order_by_ordinal_against_target_list() {
    let stmt = parse_select("select name, id from people order by 2 desc").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, .. } => match strip_projections(input.as_ref()) {
            Plan::OrderBy { items, .. } => {
                assert_eq!(items.len(), 1);
                assert!(items[0].descending);
                assert!(is_outer_user_var(&items[0].expr, 0));
            }
            other => panic!("expected order by, got {:?}", other),
        },
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn parse_insert_update_delete() {
    std::thread::Builder::new()
        .name("parse_insert_update_delete".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
    assert!(matches!(
        parse_statement("explain select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: false,
            buffers: false,
            costs: true,
            ..
        })
    ));
    assert!(matches!(
        parse_statement("explain analyze select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: true,
            buffers: false,
            costs: true,
            ..
        })
    ));
    assert!(matches!(
        parse_statement("explain (analyze, buffers) select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: true,
            buffers: true,
            costs: true,
            ..
        })
    ));
    assert!(matches!(
        parse_statement("explain (costs off) select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: false,
            buffers: false,
            costs: false,
            ..
        })
    ));
    assert!(matches!(
        parse_statement("explain (verbose, costs off) select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: false,
            buffers: false,
            costs: false,
            verbose: true,
            ..
        })
    ));
    assert!(matches!(
        parse_statement(
            "explain (costs off) merge into target t using source as s on t.id = s.id when matched then delete"
        )
        .unwrap(),
        Statement::Explain(ExplainStatement {
            costs: false,
            statement,
            ..
        }) if matches!(statement.as_ref(), Statement::Merge(_))
    ));
    assert!(matches!(
        parse_statement("explain (costs off) insert into people (id, name) values (1, 'alice')")
            .unwrap(),
        Statement::Explain(ExplainStatement {
            costs: false,
            statement,
            ..
        }) if matches!(statement.as_ref(), Statement::Insert(_))
    ));
    assert!(matches!(
        parse_statement(
            "explain (costs off) create materialized view mv_items as select id, name from people with no data"
        )
        .unwrap(),
        Statement::Explain(ExplainStatement {
            costs: false,
            statement,
            ..
        }) if matches!(statement.as_ref(), Statement::CreateTableAs(CreateTableAsStatement {
            object_type: TableAsObjectType::MaterializedView,
            skip_data: true,
            ..
        }))
    ));
    assert!(
        matches!(parse_statement("analyze").unwrap(), Statement::Analyze(AnalyzeStatement { targets, .. }) if targets.is_empty())
    );
    assert!(
        matches!(parse_statement("analyze only vacparted(a, b)").unwrap(), Statement::Analyze(AnalyzeStatement { targets, .. }) if targets == vec![MaintenanceTarget { table_name: "vacparted".into(), columns: vec!["a".into(), "b".into()], only: true }])
    );
    assert!(
        matches!(parse_statement("analyze (verbose, skip_locked, buffer_usage_limit '512 kB') vacparted").unwrap(), Statement::Analyze(AnalyzeStatement { verbose: true, skip_locked: true, buffer_usage_limit: Some(limit), .. }) if limit == "512 kB")
    );
    assert!(
        matches!(
            parse_statement("analyze (nonexistentarg) does_not_exit"),
            Err(ParseError::DetailedError { message, .. }) if message == "unrecognized ANALYZE option \"nonexistentarg\""
        )
    );
    assert!(
        matches!(
            parse_statement("analyze (nonexistent-arg) does_not_exist"),
            Err(ParseError::UnexpectedToken { actual, .. }) if actual == "syntax error at or near \"arg\""
        )
    );
    assert!(
        matches!(parse_statement("insert into people (id, name) values (1, 'alice')").unwrap(), Statement::Insert(InsertStatement { table_name, .. }) if table_name == "people")
    );
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice'), (2, 'bob')").unwrap(),
        Statement::Insert(InsertStatement { table_name, source: InsertSource::Values(values), .. })
            if table_name == "people" && values.len() == 2
    ));
    assert!(matches!(
        parse_statement("insert into people (select 1, 'alice')").unwrap(),
        Statement::Insert(InsertStatement { table_name, source: InsertSource::Select(_), .. })
            if table_name == "people"
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict do nothing")
            .unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: None,
                action: OnConflictAction::Nothing,
                ..
            }),
            ..
        })
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict (id) do update set name = excluded.name where people.id = excluded.id").unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: Some(target),
                action: OnConflictAction::Update,
                assignments,
                where_clause: Some(SqlExpr::Eq(_, _)),
            }),
            ..
        }) if inference_column_names(&target) == Some(vec!["id".into()]) && assignments.len() == 1 && assignments[0].expr == SqlExpr::Column("excluded.name".into())
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict on constraint people_pkey do nothing").unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: Some(OnConflictTarget::Constraint(name)),
                action: OnConflictAction::Nothing,
                ..
            }),
            ..
        }) if name == "people_pkey"
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict (id) do nothing").unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: Some(target),
                action: OnConflictAction::Nothing,
                ..
            }),
            ..
        }) if inference_column_names(&target) == Some(vec!["id".into()])
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict on constraint people_pkey do update set name = excluded.name").unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: Some(OnConflictTarget::Constraint(name)),
                action: OnConflictAction::Update,
                assignments,
                where_clause: None,
            }),
            ..
        }) if name == "people_pkey" && assignments.len() == 1
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict do update set name = excluded.name").unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: None,
                action: OnConflictAction::Update,
                assignments,
                where_clause: None,
            }),
            ..
        }) if assignments.len() == 1
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict (lower(name) collate \"C\" text_pattern_ops) where id > 0 do nothing").unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: Some(OnConflictTarget::Inference(OnConflictInferenceSpec { elements, predicate: Some(SqlExpr::Gt(_, _)) })),
                action: OnConflictAction::Nothing,
                ..
            }),
            ..
        }) if elements.len() == 1
            && elements[0].expr == parse_expr("lower(name)").unwrap()
            && elements[0].collation.as_deref() == Some("C")
            && elements[0].opclass.as_deref() == Some("text_pattern_ops")
    ));
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice') on conflict (lower(name) collate pg_catalog.default) do nothing").unwrap(),
        Statement::Insert(InsertStatement {
            on_conflict: Some(OnConflictClause {
                target: Some(OnConflictTarget::Inference(OnConflictInferenceSpec { elements, predicate: None })),
                action: OnConflictAction::Nothing,
                ..
            }),
            ..
        }) if elements.len() == 1
            && elements[0].expr == parse_expr("lower(name)").unwrap()
            && elements[0].collation.as_deref() == Some("pg_catalog.default")
            && elements[0].opclass.is_none()
    ));
    assert!(
        matches!(parse_statement("create table widgets (id int4 not null, name text)").unwrap(), Statement::CreateTable(ct) if ct.table_name == "widgets" && ct.columns().count() == 2)
    );
    assert!(
        matches!(parse_statement("create table pgbench_history(tid int,bid int,aid int,delta int,mtime timestamp,filler char(22))").unwrap(), Statement::CreateTable(ct) if ct.table_name == "pgbench_history" && ct.columns().count() == 6)
    );
    assert!(
        matches!(parse_statement("create table pgbench_tellers(tid int not null,bid int,tbalance int,filler char(84)) with (fillfactor=100)").unwrap(), Statement::CreateTable(ct) if ct.table_name == "pgbench_tellers" && ct.columns().count() == 4)
    );
    assert!(
        matches!(parse_statement("create temp table tempy ()").unwrap(), Statement::CreateTable(ct) if ct.persistence == TablePersistence::Temporary && ct.table_name == "tempy" && ct.columns().count() == 0)
    );
    assert!(
        matches!(parse_statement("create unlogged table unlogged_hash_table (id int4)").unwrap(), Statement::CreateTable(ct) if ct.persistence == TablePersistence::Unlogged && ct.table_name == "unlogged_hash_table" && ct.columns().count() == 1)
    );
    assert!(
        matches!(parse_statement("create temp table withoutoid() without oids").unwrap(), Statement::CreateTable(ct) if ct.persistence == TablePersistence::Temporary && ct.table_name == "withoutoid" && ct.columns().count() == 0)
    );
    assert!(
        matches!(parse_statement("create temp table withoutoid() with (oids = false)").unwrap(), Statement::CreateTable(ct) if ct.persistence == TablePersistence::Temporary && ct.table_name == "withoutoid" && ct.columns().count() == 0)
    );
    assert!(matches!(
        parse_statement("create table widgets (like source_table including all)").unwrap(),
        Statement::CreateTable(ct)
            if ct.table_name == "widgets"
                && matches!(
                    ct.elements.as_slice(),
                    [CreateTableElement::Like(CreateTableLikeClause { relation_name, options })]
                        if relation_name == "source_table"
                            && options == &[CreateTableLikeOption::IncludingAll]
                )
    ));
    assert!(matches!(
        parse_statement("create table widgets (like source_table including identity including generated including comments including storage including compression including statistics)").unwrap(),
        Statement::CreateTable(ct)
            if ct.table_name == "widgets"
                && matches!(
                    ct.elements.as_slice(),
                    [CreateTableElement::Like(CreateTableLikeClause { relation_name, options })]
                        if relation_name == "source_table"
                            && options == &[
                                CreateTableLikeOption::IncludingIdentity,
                                CreateTableLikeOption::IncludingGenerated,
                                CreateTableLikeOption::IncludingComments,
                                CreateTableLikeOption::IncludingStorage,
                                CreateTableLikeOption::IncludingCompression,
                                CreateTableLikeOption::IncludingStatistics,
                            ]
                )
    ));
    assert!(matches!(
        parse_statement("create table withoid() with (oids)"),
        Err(ParseError::TablesDeclaredWithOidsNotSupported)
    ));
    assert!(matches!(
        parse_statement("create table withoid() with (oids = true)"),
        Err(ParseError::TablesDeclaredWithOidsNotSupported)
    ));
    assert!(matches!(
        parse_statement("create table withoid() with oids"),
        Err(ParseError::UnexpectedToken { actual, .. })
            if actual == "syntax error at or near \"OIDS\""
    ));
    assert!(
        matches!(parse_statement("create table pg_temp.tempy (id int4)").unwrap(), Statement::CreateTable(CreateTableStatement { schema_name: Some(schema), table_name, persistence: TablePersistence::Permanent, .. }) if schema == "pg_temp" && table_name == "tempy")
    );
    assert!(matches!(
        parse_statement("create temp table tempy (id int4) on commit delete rows").unwrap(),
        Statement::CreateTable(CreateTableStatement {
            on_commit: OnCommitAction::DeleteRows,
            ..
        })
    ));
    assert!(matches!(
        parse_statement("create temp table tempy (id int4) on commit drop").unwrap(),
        Statement::CreateTable(CreateTableStatement {
            on_commit: OnCommitAction::Drop,
            ..
        })
    ));
    assert!(
        matches!(parse_statement("create temp table tempy(id) as select 1").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { table_name, column_names, persistence: TablePersistence::Temporary, .. }) if table_name == "tempy" && column_names == vec!["id"])
    );
    assert!(
        matches!(parse_statement("create table value_table(a, b) as values (1, 2)").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { table_name, column_names, query_sql: Some(query_sql), .. }) if table_name == "value_table" && column_names == vec!["a", "b"] && query_sql == "values (1, 2)")
    );
    assert!(
        matches!(parse_statement("create unlogged table unlogged_items(id int4)").unwrap(), Statement::CreateTable(CreateTableStatement { table_name, persistence: TablePersistence::Unlogged, .. }) if table_name == "unlogged_items")
    );
    assert!(
        matches!(parse_statement("create table ctas_opts with (fillfactor=70) as select 1").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { table_name, .. }) if table_name == "ctas_opts")
    );
    assert!(
        matches!(parse_statement("create temp table json_table_test(js) as (values ('1'), ('[]'))").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { table_name, column_names, persistence: TablePersistence::Temporary, query: CreateTableAsQuery::Select(SelectStatement { from: Some(FromItem::Values { .. }), .. }), .. }) if table_name == "json_table_test" && column_names == vec!["js"])
    );
    assert!(
        matches!(parse_statement("CREATE TEMP TABLE json_table_test (js) AS\n\t(VALUES\n\t\t('1'),\n\t\t('[]'),\n\t\t('{}'),\n\t\t('[1, 1.23, \"2\", \"aaaaaaa\", \"foo\", null, false, true, {\"aaa\": 123}, \"[1,2]\", \"\\\"str\\\"\"]')\n\t)").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { table_name, column_names, persistence: TablePersistence::Temporary, query: CreateTableAsQuery::Select(SelectStatement { from: Some(FromItem::Values { .. }), .. }), .. }) if table_name == "json_table_test" && column_names == vec!["js"])
    );
    assert!(
        matches!(
            parse_statement("create materialized view if not exists mv_items(id, name) as select id, name from people with no data").unwrap(),
            Statement::CreateTableAs(CreateTableAsStatement {
                table_name,
                column_names,
                if_not_exists: true,
                object_type: TableAsObjectType::MaterializedView,
                skip_data: true,
                query_sql: Some(query_sql),
                ..
            }) if table_name == "mv_items"
                && column_names == vec!["id", "name"]
                && query_sql == "select id, name from people"
        )
    );
    assert!(
        matches!(
            parse_statement("create materialized view mv_withdata(a) as select generate_series(1, 10) with data").unwrap(),
            Statement::CreateTableAs(CreateTableAsStatement {
                table_name,
                column_names,
                object_type: TableAsObjectType::MaterializedView,
                skip_data: false,
                query_sql: Some(query_sql),
                ..
            }) if table_name == "mv_withdata"
                && column_names == vec!["a"]
                && query_sql == "select generate_series(1, 10)"
        )
    );
    assert!(
        matches!(
            parse_statement("create materialized view mvtest_error as select 1/0 as x with no data").unwrap(),
            Statement::CreateTableAs(CreateTableAsStatement {
                table_name,
                object_type: TableAsObjectType::MaterializedView,
                skip_data: true,
                query_sql: Some(query_sql),
                ..
            }) if table_name == "mvtest_error" && query_sql == "select 1/0 as x"
        )
    );
    assert!(
        matches!(
            parse_statement("explain (analyze, costs off) create materialized view mv_nodata(a) as select generate_series(1, 10) with no data").unwrap(),
            Statement::Explain(explain)
                if matches!(explain.statement.as_ref(), Statement::CreateTableAs(CreateTableAsStatement {
                    table_name,
                    object_type: TableAsObjectType::MaterializedView,
                    skip_data: true,
                    ..
                }) if table_name == "mv_nodata")
        )
    );
    assert!(
        matches!(parse_statement("select * into cmmove1 from cmdata").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { schema_name: None, table_name, persistence: TablePersistence::Permanent, column_names, query: CreateTableAsQuery::Select(SelectStatement { from: Some(FromItem::Table { name, .. }), .. }), .. }) if table_name == "cmmove1" && column_names.is_empty() && name == "cmdata")
    );
    assert!(
        matches!(parse_statement("prepare q as select * from cmdata").unwrap(), Statement::Prepare(PrepareStatement { name, .. }) if name == "q")
    );
    assert!(
        matches!(parse_statement("create table from_prep as execute q").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { query: CreateTableAsQuery::Execute(name), .. }) if name == "q")
    );
    assert!(
        matches!(parse_statement("deallocate prepare q").unwrap(), Statement::Deallocate(DeallocateStatement { name: Some(name) }) if name == "q")
    );
    assert!(
        matches!(parse_statement("select * into temp table tempy from cmdata").unwrap(), Statement::CreateTableAs(CreateTableAsStatement { table_name, persistence: TablePersistence::Temporary, .. }) if table_name == "tempy")
    );
    assert!(
        matches!(parse_statement("drop table widgets").unwrap(), Statement::DropTable(DropTableStatement { if_exists: false, table_names, .. }) if table_names == vec!["widgets"])
    );
    assert!(
        matches!(parse_statement("drop table if exists pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers").unwrap(), Statement::DropTable(DropTableStatement { if_exists: true, table_names, .. }) if table_names == vec!["pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"])
    );
    assert!(
        matches!(parse_statement("drop index tenant_idx").unwrap(), Statement::DropIndex(DropIndexStatement { concurrently: false, if_exists: false, index_names }) if index_names == vec!["tenant_idx"])
    );
    assert!(
        matches!(parse_statement("drop schema if exists tenant_a, tenant_b").unwrap(), Statement::DropSchema(DropSchemaStatement { if_exists: true, schema_names, cascade: false }) if schema_names == vec!["tenant_a", "tenant_b"])
    );
    assert!(
        matches!(parse_statement("drop schema if exists tenant_a cascade").unwrap(), Statement::DropSchema(DropSchemaStatement { if_exists: true, schema_names, cascade: true }) if schema_names == vec!["tenant_a"])
    );
    assert!(
        matches!(parse_statement("create view item_names as select id, name from people").unwrap(), Statement::CreateView(CreateViewStatement { schema_name: None, view_name, query_sql, or_replace: false, check_option: ViewCheckOption::None, .. }) if view_name == "item_names" && query_sql == "select id, name from people")
    );
    assert!(
        matches!(
            parse_statement("create view secure_names with (security_barrier, security_invoker=false) as select id from people").unwrap(),
            Statement::CreateView(CreateViewStatement { view_name, options, .. })
                if view_name == "secure_names"
                    && options.len() == 2
                    && options[0].name == "security_barrier"
                    && options[0].value == "true"
                    && options[1].name == "security_invoker"
                    && options[1].value == "false"
        )
    );
    assert!(
        matches!(
            parse_statement("create or replace view item_names as select id from people with local check option").unwrap(),
            Statement::CreateView(CreateViewStatement {
                schema_name: None,
                view_name,
                query_sql,
                or_replace: true,
                check_option: ViewCheckOption::Local,
                ..
            }) if view_name == "item_names"
                && query_sql == "select id from people with local check option"
        )
    );
    assert!(
        matches!(parse_statement("create schema tenant").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), auth_role: None, if_not_exists: false, elements }) if schema_name == "tenant" && elements.is_empty())
    );
    assert!(
        matches!(parse_statement("create schema if not exists tenant").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), auth_role: None, if_not_exists: true, elements }) if schema_name == "tenant" && elements.is_empty())
    );
    assert!(
        matches!(parse_statement("create schema authorization app_user").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: None, auth_role: Some(RoleSpec::RoleName(auth_role)), if_not_exists: false, elements }) if auth_role == "app_user" && elements.is_empty())
    );
    assert!(
        matches!(parse_statement("create schema tenant authorization app_user").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), auth_role: Some(RoleSpec::RoleName(auth_role)), if_not_exists: false, elements }) if schema_name == "tenant" && auth_role == "app_user" && elements.is_empty())
    );
    assert!(
        matches!(parse_statement("create schema authorization current_role").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: None, auth_role: Some(RoleSpec::CurrentRole), if_not_exists: false, elements }) if elements.is_empty())
    );
    assert!(
        matches!(
            parse_statement("create schema fkpart0 create table pkey (a int primary key) create table fk_part (a int)").unwrap(),
            Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), elements, .. })
                if schema_name == "fkpart0" && elements.len() == 2
        )
    );
    let schema_with_elements = parse_statement(
        "create schema tenant \
         create sequence seq \
         create table tab (id int) \
         create view v as select id from tab \
         create index on tab (id) \
         create trigger trig before insert on tab execute function trig_fn() \
         grant select on tab to public",
    )
    .unwrap();
    let Statement::CreateSchema(CreateSchemaStatement { elements, .. }) = schema_with_elements
    else {
        panic!("expected CREATE SCHEMA");
    };
    assert_eq!(elements.len(), 6);
    assert!(matches!(
        elements[0].as_ref(),
        Statement::CreateSequence(CreateSequenceStatement {
            sequence_name,
            ..
        }) if sequence_name == "seq"
    ));
    assert!(matches!(
        elements[1].as_ref(),
        Statement::CreateTable(CreateTableStatement { table_name, .. }) if table_name == "tab"
    ));
    assert!(matches!(
        elements[2].as_ref(),
        Statement::CreateView(CreateViewStatement { view_name, .. }) if view_name == "v"
    ));
    assert!(matches!(
        elements[3].as_ref(),
        Statement::CreateIndex(CreateIndexStatement { table_name, .. }) if table_name == "tab"
    ));
    assert!(matches!(
        elements[4].as_ref(),
        Statement::CreateTrigger(CreateTriggerStatement { table_name, .. }) if table_name == "tab"
    ));
    assert!(matches!(
        elements[5].as_ref(),
        Statement::GrantObject(GrantObjectStatement {
            privilege: GrantObjectPrivilege::SelectOnTable,
            columns,
            object_names,
            ..
        }) if columns.is_empty() && object_names == &vec!["tab".to_string()]
    ));
    assert!(
        matches!(
            parse_statement("create schema tenant create view v as select 1").unwrap(),
            Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), elements, .. })
                if schema_name == "tenant" && matches!(elements.first().map(|stmt| stmt.as_ref()), Some(Statement::CreateView(_)))
        )
    );
    assert!(
        matches!(parse_statement("drop view if exists item_names, recent_items cascade").unwrap(), Statement::DropView(DropViewStatement { if_exists: true, view_names, cascade: true }) if view_names == vec!["item_names", "recent_items"])
    );
    assert!(
        matches!(parse_statement("drop materialized view if exists mv_items cascade").unwrap(), Statement::DropMaterializedView(DropMaterializedViewStatement { if_exists: true, view_names, cascade: true }) if view_names == vec!["mv_items"])
    );
    assert!(
        matches!(parse_statement("refresh materialized view mv_items").unwrap(), Statement::RefreshMaterializedView(RefreshMaterializedViewStatement { relation_name, concurrently: false, skip_data: false }) if relation_name == "mv_items")
    );
    assert!(
        matches!(parse_statement("refresh materialized view concurrently mv_items with no data").unwrap(), Statement::RefreshMaterializedView(RefreshMaterializedViewStatement { relation_name, concurrently: true, skip_data: true }) if relation_name == "mv_items")
    );
    assert!(
        matches!(parse_statement("truncate table pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers").unwrap(), Statement::TruncateTable(TruncateTableStatement { table_names }) if table_names == vec!["pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"])
    );
    assert!(
        matches!(parse_statement("truncate pgbench_history").unwrap(), Statement::TruncateTable(TruncateTableStatement { table_names }) if table_names == vec!["pgbench_history"])
    );
    assert!(
        matches!(parse_statement("vacuum pgbench_branches").unwrap(), Statement::Vacuum(VacuumStatement { analyze: false, targets, .. }) if targets == vec![MaintenanceTarget { table_name: "pgbench_branches".into(), columns: vec![], only: false }])
    );
    assert!(
        matches!(parse_statement("vacuum analyze vactst, vacparted (a)").unwrap(), Statement::Vacuum(VacuumStatement { analyze: true, targets, .. }) if targets == vec![MaintenanceTarget { table_name: "vactst".into(), columns: vec![], only: false }, MaintenanceTarget { table_name: "vacparted".into(), columns: vec!["a".into()], only: false }])
    );
    assert!(
        matches!(parse_statement("vacuum (analyze, full) vactst").unwrap(), Statement::Vacuum(VacuumStatement { analyze: true, full: true, targets, .. }) if targets == vec![MaintenanceTarget { table_name: "vactst".into(), columns: vec![], only: false }])
    );
    assert!(
        matches!(parse_statement("vacuum full freeze verbose vactst").unwrap(), Statement::Vacuum(VacuumStatement { full: true, freeze: true, verbose: true, targets, .. }) if targets == vec![MaintenanceTarget { table_name: "vactst".into(), columns: vec![], only: false }])
    );
    assert!(
        matches!(parse_statement("vacuum (freeze, disable_page_skipping, parallel -1) vactst").unwrap(), Statement::Vacuum(VacuumStatement { freeze: true, disable_page_skipping: true, parallel: Some(parallel), targets, .. }) if parallel == "-1" && targets == vec![MaintenanceTarget { table_name: "vactst".into(), columns: vec![], only: false }])
    );
    let parsed_vacuum_options = parse_statement("vacuum (index_cleanup auto, truncate false, process_main false, process_toast yes, skip_database_stats, only_database_stats off)").unwrap();
    assert!(
        matches!(&parsed_vacuum_options, Statement::Vacuum(VacuumStatement { index_cleanup: Some(index_cleanup), truncate: Some(false), process_main: Some(false), process_toast: Some(true), skip_database_stats: true, only_database_stats: false, targets, .. }) if index_cleanup == "auto" && targets.is_empty()),
        "{parsed_vacuum_options:?}"
    );
    assert!(
        matches!(parse_statement("vacuum (parallel) pvactst").unwrap(), Statement::Vacuum(VacuumStatement { parallel: None, targets, .. }) if targets == vec![MaintenanceTarget { table_name: "pvactst".into(), columns: vec![], only: false }])
    );
    assert!(
        matches!(parse_statement("update people set note = 'x' where id = 1").unwrap(), Statement::Update(UpdateStatement { table_name, target_alias, only, from, .. }) if table_name == "people" && target_alias.is_none() && !only && from.is_none())
    );
    assert!(
        matches!(parse_statement("update only people set note = 'x' where id = 1").unwrap(), Statement::Update(UpdateStatement { table_name, target_alias, only, from, .. }) if table_name == "people" && target_alias.is_none() && only && from.is_none())
    );
    assert!(
        matches!(parse_statement("delete from people where note is null").unwrap(), Statement::Delete(DeleteStatement { table_name, only, .. }) if table_name == "people" && !only)
    );
    assert!(
        matches!(parse_statement("delete from only people where note is null").unwrap(), Statement::Delete(DeleteStatement { table_name, only, .. }) if table_name == "people" && only)
    );
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parse_update_statement_with_from_and_aliases() {
    let stmt = parse_statement(
        "update only case_tbl c set i = b.i from case2_tbl b where b.j = -c.i returning c.i, b.j",
    )
    .unwrap();
    let stmt = match stmt {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    assert_eq!(stmt.table_name, "case_tbl");
    assert_eq!(stmt.target_alias.as_deref(), Some("c"));
    assert!(stmt.only);
    assert_eq!(stmt.assignments.len(), 1);
    assert_eq!(stmt.returning.len(), 2);
    assert!(matches!(
        stmt.from,
        Some(FromItem::Alias { alias, source, .. })
            if alias == "b"
                && matches!(
                    source.as_ref(),
                    FromItem::Table { name, only } if name == "case2_tbl" && !only
                )
    ));
}

#[test]
fn parse_update_statement_with_as_target_alias() {
    let stmt =
        parse_statement("update case_tbl as c set i = b.i from case2_tbl as b where b.j = -c.i")
            .unwrap();
    let stmt = match stmt {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    assert_eq!(stmt.table_name, "case_tbl");
    assert_eq!(stmt.target_alias.as_deref(), Some("c"));
    assert!(matches!(
        stmt.from,
        Some(FromItem::Alias { alias, source, .. })
            if alias == "b"
                && matches!(
                    source.as_ref(),
                    FromItem::Table { name, only } if name == "case2_tbl" && !only
                )
    ));
}

#[test]
fn parse_merge_statement() {
    let stmt = parse_statement(
        "merge into only target t using source as s on t.tid = s.sid \
         when matched and s.delta > 0 then update set balance = s.delta \
         when not matched then insert (tid, balance) values (s.sid, s.delta) \
         when not matched by source then delete",
    )
    .unwrap();
    let stmt = match stmt {
        Statement::Merge(stmt) => stmt,
        other => panic!("expected merge statement, got {other:?}"),
    };
    assert!(stmt.target_only);
    assert_eq!(stmt.target_table, "target");
    assert_eq!(stmt.target_alias.as_deref(), Some("t"));
    assert_eq!(stmt.when_clauses.len(), 3);
    assert_eq!(stmt.when_clauses[0].match_kind, MergeMatchKind::Matched);
    assert!(stmt.when_clauses[0].condition.is_some());
    assert!(matches!(
        stmt.when_clauses[0].action,
        MergeAction::Update { ref assignments } if assignments.len() == 1
    ));
    assert_eq!(
        stmt.when_clauses[1].match_kind,
        MergeMatchKind::NotMatchedByTarget
    );
    assert!(matches!(
        stmt.when_clauses[1].action,
        MergeAction::Insert {
            columns: Some(ref columns),
            source: MergeInsertSource::Values(ref values),
        } if columns == &vec!["tid".to_string(), "balance".to_string()] && values.len() == 2
    ));
    assert_eq!(
        stmt.when_clauses[2].match_kind,
        MergeMatchKind::NotMatchedBySource
    );
    assert!(matches!(stmt.when_clauses[2].action, MergeAction::Delete));
    assert!(stmt.returning.is_empty());
}

#[test]
fn parse_merge_returning_clause() {
    let stmt = parse_statement(
        "merge into target t using source s on t.tid = s.sid \
         when matched then delete returning merge_action(), old, new, t.*",
    )
    .unwrap();
    let stmt = match stmt {
        Statement::Merge(stmt) => stmt,
        other => panic!("expected merge statement, got {other:?}"),
    };
    assert_eq!(stmt.returning.len(), 4);
}

#[test]
fn parse_merge_rejects_invalid_when_actions() {
    for sql in [
        "merge into target t using source s on t.id = s.id when matched then insert default values",
        "merge into target t using source s on t.id = s.id when not matched then update set balance = 0",
        "merge into target t using source s on t.id = s.id when matched then update target set balance = 0",
        "merge into target t using source s on t.id = s.id when not matched then insert values (1), (2)",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn plan_merge_uses_join_shape_for_explain() {
    let catalog = catalog_with_pets();
    let stmt = match parse_statement(
        "merge into people p using pets s on p.id = s.owner_id when matched then delete",
    )
    .unwrap()
    {
        Statement::Merge(stmt) => stmt,
        other => panic!("expected merge statement, got {other:?}"),
    };
    let bound = plan_merge(&stmt, &catalog).unwrap();
    assert_eq!(bound.target_relation_name, "p");
    assert_eq!(bound.explain_target_name, "people p");
    assert!(matches!(
        strip_projections(&bound.input_plan.plan_tree),
        Plan::NestedLoopJoin { .. } | Plan::HashJoin { .. } | Plan::MergeJoin { .. }
    ));
    assert_eq!(bound.visible_column_count, 5);
    assert_eq!(bound.target_ctid_index, 5);
    assert_eq!(bound.source_present_index, 6);
    assert_eq!(
        bound.input_plan.column_names(),
        [
            "id",
            "name",
            "note",
            "id",
            "owner_id",
            "__merge_target_ctid",
            "__merge_source_present",
        ]
    );
}

#[test]
fn plan_merge_rejects_target_reference_in_source_subquery() {
    let catalog = catalog_with_pets();
    let stmt = match parse_statement(
        "merge into people p using (select * from pets where p.id = owner_id) s \
         on p.id = s.owner_id when matched then delete",
    )
    .unwrap()
    {
        Statement::Merge(stmt) => stmt,
        other => panic!("expected merge statement, got {other:?}"),
    };
    assert!(matches!(
        plan_merge(&stmt, &catalog),
        Err(ParseError::UnknownColumn(name)) if name == "p.id"
    ));
}

#[test]
fn parse_create_rule_single_action() {
    let stmt = parse_statement(
        "create rule r1 as on insert to people where new.id > 0 do instead insert into pets values (new.id, new.id)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRule(CreateRuleStatement {
            rule_name: "r1".into(),
            relation_name: "people".into(),
            event: RuleEvent::Insert,
            do_kind: RuleDoKind::Instead,
            where_clause: Some(SqlExpr::Gt(
                Box::new(SqlExpr::Column("new.id".into())),
                Box::new(SqlExpr::IntegerLiteral("0".into())),
            )),
            where_sql: Some("new.id > 0".into()),
            actions: vec![RuleActionStatement {
                statement: Statement::Insert(InsertStatement {
                    with_recursive: false,
                    with: vec![],
                    table_name: "pets".into(),
                    table_alias: None,
                    columns: None,
                    overriding: None,
                    source: InsertSource::Values(vec![vec![
                        SqlExpr::Column("new.id".into()),
                        SqlExpr::Column("new.id".into()),
                    ]]),
                    on_conflict: None,
                    returning: vec![],
                }),
                sql: "insert into pets values (new.id, new.id)".into(),
            }],
        })
    );
}

#[test]
fn parse_create_rule_multiple_actions() {
    let stmt = parse_statement(
        "create rule r1 as on update to people do also (insert into pets values (new.id, old.id); delete from pets where id = old.id;)",
    )
    .unwrap();
    let Statement::CreateRule(stmt) = stmt else {
        panic!("expected create rule");
    };
    assert_eq!(stmt.rule_name, "r1");
    assert_eq!(stmt.relation_name, "people");
    assert_eq!(stmt.event, RuleEvent::Update);
    assert_eq!(stmt.do_kind, RuleDoKind::Also);
    assert_eq!(stmt.actions.len(), 2);
    assert!(matches!(stmt.actions[0].statement, Statement::Insert(_)));
    assert_eq!(
        stmt.actions[0].sql,
        "insert into pets values (new.id, old.id)"
    );
    assert!(matches!(stmt.actions[1].statement, Statement::Delete(_)));
    assert_eq!(stmt.actions[1].sql, "delete from pets where id = old.id");
}

#[test]
fn parse_create_rule_multiple_actions_multiline() {
    let stmt = parse_statement(
        "create rule r1 as on update to people do also (\n\
\tupdate pets set id = new.id\n\
\t\twhere id = old.id;\n\
\tdelete from toys where id = old.id\n\
)",
    )
    .unwrap();
    let Statement::CreateRule(stmt) = stmt else {
        panic!("expected create rule");
    };
    assert_eq!(stmt.actions.len(), 2);
    assert_eq!(
        stmt.actions[0].sql,
        "update pets set id = new.id\n\t\twhere id = old.id"
    );
    assert_eq!(stmt.actions[1].sql, "delete from toys where id = old.id");
}

#[test]
fn parse_create_rule_multiple_actions_regression_shape() {
    let stmt = parse_statement(
        "create rule rtest_sys_upd as on update to rtest_system do also (\n\
\tupdate rtest_interface set sysname = new.sysname\n\
\t\twhere sysname = old.sysname;\n\
\tupdate rtest_admin set sysname = new.sysname\n\
\t\twhere sysname = old.sysname\n\
\t)",
    )
    .unwrap();
    let Statement::CreateRule(stmt) = stmt else {
        panic!("expected create rule");
    };
    assert_eq!(stmt.actions.len(), 2);
    assert_eq!(
        stmt.actions[0].sql,
        "update rtest_interface set sysname = new.sysname\n\t\twhere sysname = old.sysname"
    );
    assert_eq!(
        stmt.actions[1].sql,
        "update rtest_admin set sysname = new.sysname\n\t\twhere sysname = old.sysname"
    );
}

#[test]
fn parse_create_rule_instead_nothing() {
    let stmt = parse_statement("create rule r1 as on delete to people do instead nothing").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateRule(CreateRuleStatement {
            rule_name: "r1".into(),
            relation_name: "people".into(),
            event: RuleEvent::Delete,
            do_kind: RuleDoKind::Instead,
            where_clause: None,
            where_sql: None,
            actions: vec![],
        })
    );
}

#[test]
fn parse_drop_rule_statement() {
    let stmt = parse_statement("drop rule if exists r1 on people").unwrap();
    assert_eq!(
        stmt,
        Statement::DropRule(DropRuleStatement {
            if_exists: true,
            rule_name: "r1".into(),
            relation_name: "people".into(),
        })
    );
}

#[test]
fn parse_create_rule_preserves_utility_action_statement() {
    let stmt =
        parse_statement("create rule r1 as on insert to people do instead select 1").unwrap();
    let Statement::CreateRule(stmt) = stmt else {
        panic!("expected create rule");
    };
    assert_eq!(stmt.actions.len(), 1);
    assert_eq!(stmt.actions[0].sql, "select 1");
    assert!(matches!(stmt.actions[0].statement, Statement::Select(_)));
}

#[test]
fn bind_update_prefers_index_row_source_for_equality_predicate() {
    let catalog = catalog_with_people_id_index();
    let stmt = match parse_statement("update people set name = 'x' where id = 1").unwrap() {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    let bound = bind_update(&stmt, &catalog).unwrap();
    assert_eq!(bound.targets.len(), 1);
    match &bound.targets[0].row_source {
        BoundModifyRowSource::Index { index, keys } => {
            assert_eq!(index.relation_oid, 50010);
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0].attribute_number, 1);
            assert_eq!(keys[0].strategy, 3);
            assert_eq!(keys[0].argument, Value::Int32(1));
        }
        other => panic!("expected index row source, got {other:?}"),
    }
}

#[test]
fn bind_update_from_uses_source_columns_in_set_where_and_returning() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt =
        match parse_statement(
            "update people p set id = pets.id from pets where pets.owner_id = p.id returning p.id, pets.owner_id",
        )
        .unwrap()
        {
            Statement::Update(stmt) => stmt,
            other => panic!("expected update statement, got {other:?}"),
        };
    let bound = bind_update(&stmt, &catalog).unwrap();
    assert_eq!(bound.target_relation_name, "p");
    assert_eq!(bound.explain_target_name, "people p");
    assert!(bound.input_plan.is_some());
    assert_eq!(bound.target_visible_count, 3);
    assert_eq!(bound.visible_column_count, 5);
    assert_eq!(bound.target_ctid_index, 5);
    assert_eq!(bound.target_tableoid_index, 6);
    assert_eq!(bound.returning.len(), 2);
}

fn assert_returning_var(expr: &Expr, varno: usize, attno: AttrNumber) {
    match expr {
        Expr::Var(Var {
            varno: actual_varno,
            varattno,
            varlevelsup: 0,
            ..
        }) if *actual_varno == varno && *varattno == attno => {}
        other => panic!("expected Var(varno={varno}, attno={attno}), got {other:?}"),
    }
}

fn assert_returning_row(expr: &Expr, varno: usize, attnos: &[AttrNumber]) {
    let Expr::Row { fields, .. } = expr else {
        panic!("expected row expression, got {expr:?}");
    };
    assert_eq!(fields.len(), attnos.len());
    for ((_, field), attno) in fields.iter().zip(attnos.iter().copied()) {
        assert_returning_var(field, varno, attno);
    }
}

struct ReturningTestCatalog {
    relation: BoundRelation,
}

impl CatalogLookup for ReturningTestCatalog {
    fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
        Some(self.relation.clone())
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        (relation_oid == self.relation.relation_oid).then(|| self.relation.clone())
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.lookup_relation_by_oid(relation_oid)
    }
}

fn returning_test_catalog() -> ReturningTestCatalog {
    ReturningTestCatalog {
        relation: BoundRelation {
            rel: crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15000,
            },
            relation_oid: 50000,
            toast: None,
            namespace_oid: 11,
            owner_oid: BOOTSTRAP_SUPERUSER_OID,
            of_type_oid: 0,
            relpersistence: 'p',
            relkind: 'r',
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: desc(),
            partitioned_table: None,
            partition_spec: None,
        },
    }
}

#[test]
fn bind_insert_returning_exposes_old_and_new_pseudo_rows() {
    let catalog = returning_test_catalog();
    let stmt = match parse_statement(
        "insert into people (id, name) values (1, 'alice') returning old, new, old.*, new.*",
    )
    .unwrap()
    {
        Statement::Insert(stmt) => stmt,
        other => panic!("expected insert statement, got {other:?}"),
    };
    let bound = bind_insert(&stmt, &catalog).unwrap();
    assert_eq!(bound.returning.len(), 8);
    assert_returning_row(&bound.returning[0].expr, OUTER_VAR, &[1, 2, 3]);
    assert_returning_row(&bound.returning[1].expr, INNER_VAR, &[1, 2, 3]);
    for (target, attno) in bound.returning[2..5].iter().zip(1..) {
        assert_returning_var(&target.expr, OUTER_VAR, attno);
    }
    for (target, attno) in bound.returning[5..8].iter().zip(1..) {
        assert_returning_var(&target.expr, INNER_VAR, attno);
    }
}

#[test]
fn bind_update_and_delete_returning_expose_old_and_new_pseudo_rows() {
    let catalog = returning_test_catalog();
    let update = match parse_statement("update people set name = 'bob' returning old.id, new.name")
        .unwrap()
    {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    let bound_update = bind_update(&update, &catalog).unwrap();
    assert_returning_var(&bound_update.returning[0].expr, OUTER_VAR, 1);
    assert_returning_var(&bound_update.returning[1].expr, INNER_VAR, 2);

    let delete = match parse_statement("delete from people returning old.id, new.id").unwrap() {
        Statement::Delete(stmt) => stmt,
        other => panic!("expected delete statement, got {other:?}"),
    };
    let bound_delete = bind_delete(&delete, &catalog).unwrap();
    assert_returning_var(&bound_delete.returning[0].expr, OUTER_VAR, 1);
    assert_returning_var(&bound_delete.returning[1].expr, INNER_VAR, 1);
}

#[test]
fn bind_update_alias_hides_base_table_name() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = match parse_statement(
        "update people p set id = pets.id from pets where people.id = pets.owner_id",
    )
    .unwrap()
    {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    match bind_update(&stmt, &catalog) {
        Err(ParseError::MissingFromClauseEntry(name)) if name == "people" => {}
        Err(ParseError::UnknownColumn(name)) if name == "people.id" => {}
        other => panic!("expected hidden-target name resolution error, got {other:?}"),
    }
}

#[test]
fn bind_update_from_rejects_duplicate_target_alias_name() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = match parse_statement("update people p set id = 1 from pets p where p.owner_id = 1")
        .unwrap()
    {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    assert!(matches!(
        bind_update(&stmt, &catalog),
        Err(ParseError::DuplicateTableName(name)) if name == "p"
    ));
}

#[test]
fn bind_update_from_subquery_cannot_see_target_relation() {
    let stmt = match parse_statement("update people p set id = 1 from (select p.id) s where true")
        .unwrap()
    {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    assert!(matches!(
        bind_update(&stmt, &catalog()),
        Err(ParseError::UnknownColumn(name)) if name == "p.id"
    ));
}

#[test]
fn bind_update_uses_partial_index_row_source_when_filter_implies_predicate() {
    std::thread::Builder::new()
        .name("bind_update_uses_partial_index_row_source_when_filter_implies_predicate".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let catalog = catalog_with_people_partial_unique_index();
            let stmt = match parse_statement("update people set name = 'x' where id = 1 and id > 0")
                .unwrap()
            {
                Statement::Update(stmt) => stmt,
                other => panic!("expected update statement, got {other:?}"),
            };
            let bound = bind_update(&stmt, &catalog).unwrap();
            assert_eq!(bound.targets.len(), 1);
            match &bound.targets[0].row_source {
                BoundModifyRowSource::Index { index, .. } => {
                    assert_eq!(index.name, "people_partial_key");
                }
                other => panic!("expected partial index row source, got {other:?}"),
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn bind_delete_ignores_partial_index_when_filter_does_not_imply_predicate() {
    std::thread::Builder::new()
        .name("bind_delete_ignores_partial_index_when_filter_does_not_imply_predicate".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let catalog = catalog_with_people_partial_unique_index();
            let stmt = match parse_statement("delete from people where id = 1").unwrap() {
                Statement::Delete(stmt) => stmt,
                other => panic!("expected delete statement, got {other:?}"),
            };
            let bound = bind_delete(&stmt, &catalog).unwrap();
            assert_eq!(bound.targets.len(), 1);
            assert!(matches!(
                bound.targets[0].row_source,
                BoundModifyRowSource::Heap
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn bind_delete_falls_back_to_heap_for_or_predicate() {
    let catalog = catalog_with_people_id_index();
    let stmt = match parse_statement("delete from people where id = 1 or id = 2").unwrap() {
        Statement::Delete(stmt) => stmt,
        other => panic!("expected delete statement, got {other:?}"),
    };
    let bound = bind_delete(&stmt, &catalog).unwrap();
    assert_eq!(bound.targets.len(), 1);
    assert!(matches!(
        bound.targets[0].row_source,
        BoundModifyRowSource::Heap
    ));
}

fn people_insert_with_on_conflict(
    target: Option<crate::include::nodes::parsenodes::OnConflictTarget>,
    action: crate::include::nodes::parsenodes::OnConflictAction,
    assignments: Vec<crate::include::nodes::parsenodes::Assignment>,
    where_clause: Option<crate::include::nodes::parsenodes::SqlExpr>,
) -> InsertStatement {
    InsertStatement {
        with_recursive: false,
        with: vec![],
        table_name: "people".into(),
        table_alias: None,
        columns: Some(vec![
            crate::include::nodes::parsenodes::AssignmentTarget {
                column: "id".into(),
                subscripts: vec![],
                field_path: vec![],
                indirection: vec![],
            },
            crate::include::nodes::parsenodes::AssignmentTarget {
                column: "name".into(),
                subscripts: vec![],
                field_path: vec![],
                indirection: vec![],
            },
        ]),
        overriding: None,
        source: InsertSource::Values(vec![vec![
            parse_expr("1").unwrap(),
            parse_expr("'alice'").unwrap(),
        ]]),
        on_conflict: Some(crate::include::nodes::parsenodes::OnConflictClause {
            target,
            action,
            assignments,
            where_clause,
        }),
        returning: vec![],
    }
}

#[test]
fn bind_insert_matches_on_conflict_columns_order_insensitively() {
    let catalog = catalog_with_people_id_name_unique_index();
    let stmt = people_insert_with_on_conflict(
        Some(plain_inference_target(&["name", "id", "name"])),
        crate::include::nodes::parsenodes::OnConflictAction::Nothing,
        vec![],
        None,
    );
    let bound =
        stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || bind_insert(&stmt, &catalog)).unwrap();
    let on_conflict = bound.on_conflict.expect("on conflict");
    assert_eq!(on_conflict.arbiter_indexes.len(), 1);
    assert_eq!(on_conflict.arbiter_indexes[0].name, "people_id_name_key");
    assert!(matches!(on_conflict.action, BoundOnConflictAction::Nothing));
}

#[test]
fn bind_insert_returning_relation_name_as_whole_row() {
    let catalog = catalog();
    let mut stmt = people_insert_with_on_conflict(
        None,
        crate::include::nodes::parsenodes::OnConflictAction::Nothing,
        vec![],
        None,
    );
    stmt.on_conflict = None;
    stmt.returning = vec![SelectItem {
        expr: SqlExpr::Column("people".into()),
        output_name: "people".into(),
    }];

    let bound =
        stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || bind_insert(&stmt, &catalog)).unwrap();
    assert!(matches!(bound.returning[0].expr, Expr::Row { .. }));
}

#[test]
fn bind_insert_resolves_on_conflict_constraint_name() {
    let catalog = catalog_with_people_primary_key();
    let stmt = people_insert_with_on_conflict(
        Some(crate::include::nodes::parsenodes::OnConflictTarget::Constraint("people_pkey".into())),
        crate::include::nodes::parsenodes::OnConflictAction::Update,
        vec![crate::include::nodes::parsenodes::Assignment {
            target: crate::include::nodes::parsenodes::AssignmentTarget {
                column: "name".into(),
                subscripts: vec![],
                field_path: vec![],
                indirection: vec![],
            },
            expr: parse_expr("excluded.name").unwrap(),
        }],
        Some(parse_expr("people.id = excluded.id").unwrap()),
    );
    let bound =
        stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || bind_insert(&stmt, &catalog)).unwrap();
    let on_conflict = bound.on_conflict.expect("on conflict");
    assert_eq!(on_conflict.arbiter_indexes.len(), 1);
    assert_eq!(on_conflict.arbiter_indexes[0].name, "people_pkey");
    assert!(matches!(
        on_conflict.action,
        BoundOnConflictAction::Update { .. }
    ));
}

#[test]
fn bind_insert_rejects_on_conflict_do_update_without_target() {
    let catalog = catalog_with_people_primary_key();
    let stmt = people_insert_with_on_conflict(
        None,
        crate::include::nodes::parsenodes::OnConflictAction::Update,
        vec![crate::include::nodes::parsenodes::Assignment {
            target: crate::include::nodes::parsenodes::AssignmentTarget {
                column: "name".into(),
                subscripts: vec![],
                field_path: vec![],
                indirection: vec![],
            },
            expr: parse_expr("excluded.name").unwrap(),
        }],
        Some(parse_expr("people.id = excluded.id").unwrap()),
    );
    let err = stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || bind_insert(&stmt, &catalog))
        .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UnexpectedToken { actual, .. }
            if actual
                == "ON CONFLICT DO UPDATE requires inference specification or constraint name"
    ));
}

#[test]
fn bind_insert_matches_partial_index_when_predicate_implies_index_predicate() {
    std::thread::Builder::new()
        .name("bind_insert_matches_partial_index_when_predicate_implies_index_predicate".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let partial_catalog = catalog_with_people_partial_unique_index();
            let partial_stmt = people_insert_with_on_conflict(
                Some(inference_target_with_predicate(
                    &["id"],
                    parse_expr("id > 0 and name = 'alice'").unwrap(),
                )),
                crate::include::nodes::parsenodes::OnConflictAction::Nothing,
                vec![],
                None,
            );
            let bound = bind_insert(&partial_stmt, &partial_catalog).unwrap();
            let on_conflict = bound.on_conflict.expect("on conflict");
            assert_eq!(on_conflict.arbiter_indexes.len(), 1);
            assert_eq!(on_conflict.arbiter_indexes[0].name, "people_partial_key");
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn bind_insert_rejects_partial_index_when_inference_predicate_is_missing_or_weaker() {
    std::thread::Builder::new()
        .name("bind_insert_rejects_partial_index_when_inference_predicate_is_missing_or_weaker".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let catalog = catalog_with_people_partial_unique_index();

            let expression_stmt =
                people_insert_with_on_conflict(Some(plain_inference_target(&["id"])), OnConflictAction::Nothing, vec![], None);
            assert!(matches!(
                bind_insert(&expression_stmt, &catalog),
                Err(ParseError::UnexpectedToken { actual, .. })
                    if actual
                        == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            ));

            let weaker_stmt = people_insert_with_on_conflict(
                Some(inference_target_with_predicate(
                    &["id"],
                    parse_expr("id > 1").unwrap(),
                )),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&weaker_stmt, &catalog),
                Err(ParseError::UnexpectedToken { actual, .. })
                    if actual
                        == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn bind_insert_rejects_on_conflict_partial_index_with_ctid_predicate() {
    let catalog = catalog_with_people_ctid_partial_unique_index();
    let people = catalog.lookup_any_relation("people").unwrap();
    crate::backend::parser::bind_index_predicate_sql_expr(
        "ctid >= '(1000,0)'",
        Some("people"),
        &people.desc,
        &catalog,
    )
    .expect("bind ctid partial index predicate");
    let stmt = people_insert_with_on_conflict(
        None,
        crate::include::nodes::parsenodes::OnConflictAction::Nothing,
        vec![],
        None,
    );
    let err = stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || bind_insert(&stmt, &catalog))
        .unwrap_err();
    assert!(matches!(
        err,
        ParseError::FeatureNotSupported(message)
            if message == "ON CONFLICT with partial indexes whose predicate uses ctid"
    ));
}

#[test]
fn bind_insert_matches_expression_and_collation_and_opclass_inference_targets() {
    std::thread::Builder::new()
        .name("bind_insert_matches_expression_and_collation_and_opclass_inference_targets".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let expression_catalog = catalog_with_people_expression_unique_index();
            let expression_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: parse_expr("lower(name)").unwrap(),
                        collation: None,
                        opclass: None,
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            let expression_bound = bind_insert(&expression_stmt, &expression_catalog).unwrap();
            assert_eq!(
                expression_bound
                    .on_conflict
                    .expect("on conflict")
                    .arbiter_indexes[0]
                    .name,
                "people_lower_name_key"
            );

            let collation_catalog = catalog_with_people_name_c_collation_index();
            let collation_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("name".into()),
                        collation: Some("C".into()),
                        opclass: None,
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            let collation_bound = bind_insert(&collation_stmt, &collation_catalog).unwrap();
            assert_eq!(
                collation_bound
                    .on_conflict
                    .expect("on conflict")
                    .arbiter_indexes[0]
                    .name,
                "people_name_c_key"
            );

            let opclass_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("id".into()),
                        collation: None,
                        opclass: Some("int4_ops".into()),
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            let opclass_bound =
                bind_insert(&opclass_stmt, &catalog_with_people_primary_key()).unwrap();
            assert_eq!(
                opclass_bound
                    .on_conflict
                    .expect("on conflict")
                    .arbiter_indexes[0]
                    .name,
                "people_pkey"
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn bind_insert_rejects_non_matching_expression_collation_and_opclass_targets() {
    std::thread::Builder::new()
        .name("bind_insert_rejects_non_matching_expression_collation_and_opclass_targets".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let expression_catalog = catalog_with_people_expression_unique_index();
            let expression_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: parse_expr("upper(name)").unwrap(),
                        collation: None,
                        opclass: None,
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&expression_stmt, &expression_catalog),
                Err(ParseError::UnexpectedToken { actual, .. })
                    if actual
                        == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            ));

            let collation_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("name".into()),
                        collation: Some("POSIX".into()),
                        opclass: None,
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&collation_stmt, &catalog_with_people_name_c_collation_index()),
                Err(ParseError::UnexpectedToken { actual, .. })
                    if actual
                        == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            ));

            let opclass_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("id".into()),
                        collation: None,
                        opclass: Some("int8_ops".into()),
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&opclass_stmt, &catalog_with_people_primary_key()),
                Err(ParseError::UnexpectedToken { actual, .. })
                    if actual
                        == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn bind_insert_matches_opclass_inference_by_family_and_input_type() {
    std::thread::Builder::new()
        .name("bind_insert_matches_opclass_inference_by_family_and_input_type".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let requested_opclass = custom_btree_opclass(
                910_001,
                "int4_same_family_alias_ops",
                crate::include::catalog::BTREE_INTEGER_FAMILY_OID,
                crate::include::catalog::INT4_TYPE_OID,
            );
            let base_catalog = catalog_with_people_primary_key();
            let visible = visible_catalog_with_extra_opclasses(
                &base_catalog,
                vec![requested_opclass.clone()],
            );

            let opclass_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("id".into()),
                        collation: None,
                        opclass: Some(requested_opclass.opcname.clone()),
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            let opclass_bound = bind_insert(&opclass_stmt, &visible).unwrap();
            assert_eq!(
                opclass_bound
                    .on_conflict
                    .expect("on conflict")
                    .arbiter_indexes[0]
                    .name,
                "people_pkey"
            );

            let custom_index_opclass = custom_btree_opclass(
                910_002,
                "int4_nondefault_index_ops",
                crate::include::catalog::BTREE_INTEGER_FAMILY_OID,
                crate::include::catalog::INT4_TYPE_OID,
            );
            let custom_index_catalog =
                catalog_with_people_primary_key_opclass(custom_index_opclass.oid);
            let custom_index_visible = visible_catalog_with_extra_opclasses(
                &custom_index_catalog,
                vec![custom_index_opclass],
            );
            let wildcard_stmt = people_insert_with_on_conflict(
                Some(plain_inference_target(&["id"])),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            let wildcard_bound = bind_insert(&wildcard_stmt, &custom_index_visible).unwrap();
            assert_eq!(
                wildcard_bound
                    .on_conflict
                    .expect("on conflict")
                    .arbiter_indexes[0]
                    .name,
                "people_pkey"
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn bind_insert_rejects_opclass_inference_with_wrong_family_or_input_type() {
    std::thread::Builder::new()
        .name("bind_insert_rejects_opclass_inference_with_wrong_family_or_input_type".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let wrong_input = custom_btree_opclass(
                910_003,
                "int8_same_family_alias_ops",
                crate::include::catalog::BTREE_INTEGER_FAMILY_OID,
                crate::include::catalog::INT8_TYPE_OID,
            );
            let wrong_family = custom_btree_opclass(
                910_004,
                "int4_wrong_family_alias_ops",
                crate::include::catalog::BTREE_FLOAT_FAMILY_OID,
                crate::include::catalog::INT4_TYPE_OID,
            );
            let base_catalog = catalog_with_people_primary_key();
            let visible =
                visible_catalog_with_extra_opclasses(&base_catalog, vec![wrong_input, wrong_family]);

            let wrong_input_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("id".into()),
                        collation: None,
                        opclass: Some("int8_same_family_alias_ops".into()),
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&wrong_input_stmt, &visible),
                Err(ParseError::UnexpectedToken { actual, .. })
                    if actual
                        == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            ));

            let wrong_family_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("id".into()),
                        collation: None,
                        opclass: Some("int4_wrong_family_alias_ops".into()),
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&wrong_family_stmt, &visible),
                Err(ParseError::UnexpectedToken { actual, .. })
                    if actual
                        == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parse_create_table_with_varchar_types() {
    match parse_statement(
            "create table widgets (a varchar, b varchar(5), c character varying, d character varying(7))",
        )
        .unwrap()
        {
            Statement::CreateTable(ct) => {
                let columns = ct.columns().collect::<Vec<_>>();
                assert_eq!(columns.len(), 4);
                assert_eq!(columns[0].ty, SqlType::new(SqlTypeKind::Varchar));
                assert_eq!(columns[1].ty, SqlType::with_char_len(SqlTypeKind::Varchar, 5));
                assert_eq!(columns[2].ty, SqlType::new(SqlTypeKind::Varchar));
                assert_eq!(columns[3].ty, SqlType::with_char_len(SqlTypeKind::Varchar, 7));
            }
            other => panic!("expected create table, got {other:?}"),
        }
}

#[test]
fn parse_create_table_with_character_types() {
    match parse_statement("create table widgets (a character, b character(16), c char(7))").unwrap()
    {
        Statement::CreateTable(ct) => {
            let columns = ct.columns().collect::<Vec<_>>();
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0].ty, SqlType::with_char_len(SqlTypeKind::Char, 1));
            assert_eq!(columns[1].ty, SqlType::with_char_len(SqlTypeKind::Char, 16));
            assert_eq!(columns[2].ty, SqlType::with_char_len(SqlTypeKind::Char, 7));
        }
        other => panic!("expected create table, got {other:?}"),
    }
}

#[test]
fn parse_rejects_show_tables() {
    assert!(parse_statement("show tables").is_err());
}

#[test]
fn parse_show_timezone() {
    assert!(matches!(
        parse_statement("show timezone").unwrap(),
        Statement::Show(ShowStatement { name }) if name == "timezone"
    ));
    assert!(matches!(
        parse_statement("show time zone").unwrap(),
        Statement::Show(ShowStatement { name }) if name == "timezone"
    ));
}

#[test]
fn parse_between_symmetric_expression() {
    let stmt = parse_select("select time '01:00' between symmetric time '02:00' and time '00:00'")
        .unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::Or(_, _)));
}

#[test]
fn parse_not_between_lowers_like_postgres() {
    let stmt =
        parse_select("select f1 not between date '1997-01-01' and date '1998-01-01'").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::Or(_, _)));

    let stmt =
        parse_select("select f1 not between symmetric date '1997-01-01' and date '1998-01-01'")
            .unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::And(_, _)));
}

#[test]
fn parse_current_datetime_forms() {
    let stmt = parse_select(
        "select current_date, current_time(2), current_timestamp(3), localtime, localtimestamp(4)",
    )
    .unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::CurrentDate));
    assert!(matches!(
        stmt.targets[1].expr,
        SqlExpr::CurrentTime { precision: Some(2) }
    ));
    assert!(matches!(
        stmt.targets[2].expr,
        SqlExpr::CurrentTimestamp { precision: Some(3) }
    ));
    assert!(matches!(
        stmt.targets[3].expr,
        SqlExpr::LocalTime { precision: None }
    ));
    assert!(matches!(
        stmt.targets[4].expr,
        SqlExpr::LocalTimestamp { precision: Some(4) }
    ));
}

#[test]
fn parse_current_schema_and_catalog() {
    let stmt = parse_select("select current_schema, current_catalog").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::CurrentSchema));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::CurrentCatalog));
}

#[test]
fn parse_current_user_and_legacy_null_predicates() {
    let stmt = parse_select("select current_user, note isnull, note notnull from people").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::CurrentUser));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::IsNull(_)));
    assert!(matches!(stmt.targets[2].expr, SqlExpr::IsNotNull(_)));
}

#[test]
fn create_table_temp_name_validation() {
    let (name, persistence) =
        crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
            schema_name: Some("public".into()),
            table_name: "t".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        })
        .unwrap();
    assert_eq!(name, "t");
    assert_eq!(persistence, TablePersistence::Permanent);

    let err = crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
        schema_name: Some("public".into()),
        table_name: "t".into(),
        of_type_name: None,
        persistence: TablePersistence::Temporary,
        on_commit: OnCommitAction::PreserveRows,
        elements: vec![],
        options: Vec::new(),
        inherits: Vec::new(),
        partition_spec: None,
        partition_of: None,
        partition_bound: None,
        if_not_exists: false,
    })
    .unwrap_err();
    assert!(matches!(err, ParseError::TempTableInNonTempSchema(_)));

    let err = crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
        schema_name: None,
        table_name: "t".into(),
        of_type_name: None,
        persistence: TablePersistence::Permanent,
        on_commit: OnCommitAction::DeleteRows,
        elements: vec![],
        options: Vec::new(),
        inherits: Vec::new(),
        partition_spec: None,
        partition_of: None,
        partition_bound: None,
        if_not_exists: false,
    })
    .unwrap_err();
    assert!(matches!(err, ParseError::OnCommitOnlyForTempTables));
}

#[test]
fn parse_create_table_if_not_exists() {
    match parse_statement("CREATE TABLE IF NOT EXISTS foo (id int4)").unwrap() {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.table_name, "foo");
            assert!(ct.if_not_exists);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
    // Without IF NOT EXISTS
    match parse_statement("CREATE TABLE bar (id int4)").unwrap() {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.table_name, "bar");
            assert!(!ct.if_not_exists);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_inherits_clause() {
    match parse_statement("create table child (id int4) inherits (parent1, parent2)").unwrap() {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.table_name, "child");
            assert_eq!(ct.inherits, vec!["parent1", "parent2"]);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_of_with_typed_column_options() {
    let Statement::CreateTable(ct) = parse_statement(
        "create table persons of person_type (id with options primary key, name not null default 'x')",
    )
    .unwrap()
    else {
        panic!("expected CreateTable");
    };
    assert_eq!(ct.table_name, "persons");
    assert_eq!(ct.of_type_name.as_deref(), Some("person_type"));
    assert_eq!(ct.elements.len(), 2);
    let CreateTableElement::TypedColumnOptions(id_options) = &ct.elements[0] else {
        panic!("expected typed column options");
    };
    assert_eq!(id_options.name, "id");
    assert!(
        id_options
            .constraints
            .iter()
            .any(|constraint| matches!(constraint, ColumnConstraint::PrimaryKey { .. }))
    );
    let CreateTableElement::TypedColumnOptions(name_options) = &ct.elements[1] else {
        panic!("expected typed column options");
    };
    assert_eq!(name_options.name, "name");
    assert_eq!(name_options.default_expr.as_deref(), Some("'x'"));
    assert!(
        name_options
            .constraints
            .iter()
            .any(|constraint| matches!(constraint, ColumnConstraint::NotNull { .. }))
    );
}

#[test]
fn parse_alter_table_of_and_not_of() {
    let Statement::AlterTableOf(alter_of) =
        parse_statement("alter table persons of person_type").unwrap()
    else {
        panic!("expected AlterTableOf");
    };
    assert_eq!(alter_of.table_name, "persons");
    assert_eq!(alter_of.type_name, "person_type");

    let Statement::AlterTableNotOf(not_of) =
        parse_statement("alter table if exists persons not of").unwrap()
    else {
        panic!("expected AlterTableNotOf");
    };
    assert_eq!(not_of.table_name, "persons");
    assert!(not_of.if_exists);
}

#[test]
fn parse_create_table_partition_by_range() {
    match parse_statement("create table measurement (a int, b int) partition by range (a, b)")
        .unwrap()
    {
        Statement::CreateTable(ct) => {
            assert_eq!(
                ct.partition_spec,
                Some(RawPartitionSpec {
                    strategy: PartitionStrategy::Range,
                    keys: vec![
                        RawPartitionKey {
                            expr: SqlExpr::Column("a".into()),
                            expr_sql: "a".into(),
                            opclass: None,
                        },
                        RawPartitionKey {
                            expr: SqlExpr::Column("b".into()),
                            expr_sql: "b".into(),
                            opclass: None,
                        },
                    ],
                })
            );
            assert_eq!(ct.partition_of, None);
            assert_eq!(ct.partition_bound, None);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_column_collation() {
    match parse_statement("create table coll_pruning (a text collate \"C\")").unwrap() {
        Statement::CreateTable(ct) => {
            let column = ct.columns().next().expect("column");
            assert_eq!(column.name, "a");
            assert_eq!(column.collation.as_deref(), Some("C"));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_partition_keys_with_opclasses_and_expressions() {
    match parse_statement(
        "create table hp (a int4, b text) partition by hash (a part_test_int4_ops, b part_test_text_ops)",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            let spec = ct.partition_spec.expect("partition spec");
            assert_eq!(spec.strategy, PartitionStrategy::Hash);
            assert_eq!(spec.keys[0].expr, SqlExpr::Column("a".into()));
            assert_eq!(spec.keys[0].opclass.as_deref(), Some("part_test_int4_ops"));
            assert_eq!(spec.keys[1].expr, SqlExpr::Column("b".into()));
            assert_eq!(spec.keys[1].opclass.as_deref(), Some("part_test_text_ops"));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }

    match parse_statement("create table rlp3 (b varchar, a int) partition by list (b varchar_ops)")
        .unwrap()
    {
        Statement::CreateTable(ct) => {
            let spec = ct.partition_spec.expect("partition spec");
            assert_eq!(spec.keys[0].expr, SqlExpr::Column("b".into()));
            assert_eq!(spec.keys[0].opclass.as_deref(), Some("varchar_ops"));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }

    match parse_statement(
        "create table mc3p (a int, b int, c int) partition by range (a, abs(b), c)",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            let spec = ct.partition_spec.expect("partition spec");
            assert_eq!(spec.keys[1].expr_sql, "abs(b)");
            assert!(matches!(spec.keys[1].expr, SqlExpr::FuncCall { .. }));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }

    match parse_statement(
        "create table cp (a text) partition by range (substr(a, 1) collate \"POSIX\", substr(a, 1) collate \"C\")",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            let spec = ct.partition_spec.expect("partition spec");
            assert!(matches!(spec.keys[0].expr, SqlExpr::Collate { .. }));
            assert!(matches!(spec.keys[1].expr, SqlExpr::Collate { .. }));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_partition_of_with_subpartition_spec() {
    match parse_statement(
        "create table measurement_lo partition of measurement \
         for values from (minvalue) to (10) partition by list (b)",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.partition_of.as_deref(), Some("measurement"));
            assert_eq!(
                ct.partition_bound,
                Some(RawPartitionBoundSpec::Range {
                    from: vec![RawPartitionRangeDatum::MinValue],
                    to: vec![RawPartitionRangeDatum::Value(SqlExpr::IntegerLiteral(
                        "10".into()
                    ))],
                    is_default: false,
                })
            );
            assert_eq!(
                ct.partition_spec,
                Some(RawPartitionSpec {
                    strategy: PartitionStrategy::List,
                    keys: vec![RawPartitionKey {
                        expr: SqlExpr::Column("b".into()),
                        expr_sql: "b".into(),
                        opclass: None,
                    }],
                })
            );
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_partition_of_with_storage_clause() {
    match parse_statement(
        "create table measurement_lo partition of measurement \
         for values from (0) to (10) with (autovacuum_enabled = false)",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.partition_of.as_deref(), Some("measurement"));
            assert_eq!(
                ct.partition_bound,
                Some(RawPartitionBoundSpec::Range {
                    from: vec![RawPartitionRangeDatum::Value(SqlExpr::IntegerLiteral(
                        "0".into()
                    ))],
                    to: vec![RawPartitionRangeDatum::Value(SqlExpr::IntegerLiteral(
                        "10".into()
                    ))],
                    is_default: false,
                })
            );
            assert_eq!(ct.partition_spec, None);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_partition_of_with_table_elements() {
    match parse_statement(
        "create table measurement_lo partition of measurement \
         (a int primary key, constraint measurement_lo_ck check (a > 0)) \
         for values from (0) to (10)",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.partition_of.as_deref(), Some("measurement"));
            assert_eq!(ct.elements.len(), 2);
            assert!(matches!(
                &ct.elements[0],
                CreateTableElement::Column(column)
                    if column.name == "a" && column.primary_key()
            ));
            assert!(matches!(
                &ct.elements[1],
                CreateTableElement::Constraint(TableConstraint::Check { attributes, expr_sql })
                    if attributes.name.as_deref() == Some("measurement_lo_ck")
                        && expr_sql == "a > 0"
            ));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_partition_of_with_column_options() {
    match parse_statement(
        "create table part_b partition of parted \
         (b with options not null default 0) \
         for values in ('b')",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.partition_of.as_deref(), Some("parted"));
            assert_eq!(ct.elements.len(), 1);
            assert!(matches!(
                &ct.elements[0],
                CreateTableElement::PartitionColumnOverride(override_)
                    if override_.name == "b"
                        && override_.default_expr.as_deref() == Some("0")
                        && override_.constraints.iter().any(|constraint| {
                            matches!(constraint, ColumnConstraint::NotNull { .. })
                        })
            ));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_partition_of_with_short_column_options() {
    match parse_statement(
        "create table part_b partition of parted \
         (a not null, b default 1) \
         for values in ('b')",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.partition_of.as_deref(), Some("parted"));
            assert_eq!(ct.elements.len(), 2);
            assert!(matches!(
                &ct.elements[0],
                CreateTableElement::PartitionColumnOverride(override_)
                    if override_.name == "a"
                        && override_.constraints.iter().any(|constraint| {
                            matches!(constraint, ColumnConstraint::NotNull { .. })
                        })
            ));
            assert!(matches!(
                &ct.elements[1],
                CreateTableElement::PartitionColumnOverride(override_)
                    if override_.name == "b" && override_.default_expr.as_deref() == Some("1")
            ));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_alter_table_attach_partition() {
    match parse_statement(
        "alter table measurement attach partition measurement_mid \
         for values from (10) to (20)",
    )
    .unwrap()
    {
        Statement::AlterTableAttachPartition(stmt) => {
            assert_eq!(stmt.parent_table, "measurement");
            assert_eq!(stmt.partition_table, "measurement_mid");
            assert_eq!(
                stmt.bound,
                RawPartitionBoundSpec::Range {
                    from: vec![RawPartitionRangeDatum::Value(SqlExpr::IntegerLiteral(
                        "10".into(),
                    ))],
                    to: vec![RawPartitionRangeDatum::Value(SqlExpr::IntegerLiteral(
                        "20".into(),
                    ))],
                    is_default: false,
                }
            );
        }
        other => panic!("expected AlterTableAttachPartition, got {:?}", other),
    }
}

#[test]
fn parse_alter_table_detach_partition_modes() {
    match parse_statement("alter table measurement detach partition measurement_mid").unwrap() {
        Statement::AlterTableDetachPartition(stmt) => {
            assert_eq!(stmt.parent_table, "measurement");
            assert_eq!(stmt.partition_table, "measurement_mid");
            assert_eq!(stmt.mode, DetachPartitionMode::Immediate);
        }
        other => panic!("expected AlterTableDetachPartition, got {:?}", other),
    }

    match parse_statement(
        "alter table if exists only measurement detach partition measurement_mid concurrently",
    )
    .unwrap()
    {
        Statement::AlterTableDetachPartition(stmt) => {
            assert!(stmt.if_exists);
            assert!(stmt.only);
            assert_eq!(stmt.parent_table, "measurement");
            assert_eq!(stmt.partition_table, "measurement_mid");
            assert_eq!(stmt.mode, DetachPartitionMode::Concurrently);
        }
        other => panic!("expected AlterTableDetachPartition, got {:?}", other),
    }

    match parse_statement("alter table measurement detach partition measurement_mid finalize")
        .unwrap()
    {
        Statement::AlterTableDetachPartition(stmt) => {
            assert_eq!(stmt.mode, DetachPartitionMode::Finalize);
        }
        other => panic!("expected AlterTableDetachPartition, got {:?}", other),
    }
}

#[test]
fn parse_alter_table_detach_partition_rejects_trailing_syntax() {
    assert!(
        parse_statement(
            "alter table measurement detach partition measurement_mid concurrently finalize"
        )
        .is_err()
    );
}

#[test]
fn parse_create_table_partition_by_hash() {
    match parse_statement("create table measurement (a int) partition by hash (a)").unwrap() {
        Statement::CreateTable(ct) => {
            assert_eq!(
                ct.partition_spec,
                Some(RawPartitionSpec {
                    strategy: PartitionStrategy::Hash,
                    keys: vec![RawPartitionKey {
                        expr: SqlExpr::Column("a".into()),
                        expr_sql: "a".into(),
                        opclass: None,
                    }],
                })
            );
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_create_table_hash_partition_bound() {
    match parse_statement(
        "create table measurement_h0 partition of measurement \
         for values with (modulus 4, remainder 0)",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.partition_of.as_deref(), Some("measurement"));
            assert_eq!(
                ct.partition_bound,
                Some(RawPartitionBoundSpec::Hash {
                    modulus: 4,
                    remainder: 0,
                })
            );
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn parse_partition_of_with_using_clause_is_unsupported_without_panicking() {
    match parse_statement("create table child partition of parent for values in ('c') using heap")
        .unwrap()
    {
        Statement::Unsupported(stmt) => {
            assert!(stmt.sql.to_ascii_lowercase().contains("using heap"));
        }
        other => panic!("expected Unsupported, got {:?}", other),
    }
}

#[test]
fn parse_select_from_only_table() {
    match parse_statement("select * from only parent").unwrap() {
        Statement::Select(SelectStatement {
            from: Some(FromItem::Table { name, only }),
            ..
        }) => {
            assert_eq!(name, "parent");
            assert!(only);
        }
        other => panic!("expected Select with ONLY table, got {:?}", other),
    }
}

#[test]
fn parse_select_for_update_clause() {
    match parse_statement("select * from people for update").unwrap() {
        Statement::Select(SelectStatement {
            from: Some(FromItem::Table { name, only: false }),
            locking_clause: Some(SelectLockingClause::ForUpdate),
            ..
        }) => assert_eq!(name, "people"),
        other => panic!("expected Select with FOR UPDATE, got {:?}", other),
    }
}

#[test]
fn parse_select_for_update_of_clause() {
    match parse_statement("select * from people for update of people").unwrap() {
        Statement::Select(SelectStatement {
            from: Some(FromItem::Table { name, only: false }),
            locking_clause: Some(SelectLockingClause::ForUpdate),
            ..
        }) => assert_eq!(name, "people"),
        other => panic!("expected Select with FOR UPDATE OF, got {:?}", other),
    }
}

#[test]
fn parse_select_for_no_key_update_clause() {
    match parse_statement("select * from people for no key update").unwrap() {
        Statement::Select(SelectStatement {
            from: Some(FromItem::Table { name, only: false }),
            locking_clause: Some(SelectLockingClause::ForNoKeyUpdate),
            ..
        }) => assert_eq!(name, "people"),
        other => panic!("expected Select with FOR NO KEY UPDATE, got {:?}", other),
    }
}

#[test]
fn parse_select_for_share_clause() {
    match parse_statement("select * from people for share").unwrap() {
        Statement::Select(SelectStatement {
            from: Some(FromItem::Table { name, only: false }),
            locking_clause: Some(SelectLockingClause::ForShare),
            ..
        }) => assert_eq!(name, "people"),
        other => panic!("expected Select with FOR SHARE, got {:?}", other),
    }
}

#[test]
fn parse_select_for_key_share_clause() {
    match parse_statement("select * from people for key share").unwrap() {
        Statement::Select(SelectStatement {
            from: Some(FromItem::Table { name, only: false }),
            locking_clause: Some(SelectLockingClause::ForKeyShare),
            ..
        }) => assert_eq!(name, "people"),
        other => panic!("expected Select with FOR KEY SHARE, got {:?}", other),
    }
}

#[test]
fn parse_limit_null_as_unbounded_limit() {
    match parse_statement("select * from people limit null").unwrap() {
        Statement::Select(SelectStatement { limit: None, .. }) => {}
        other => panic!("expected SELECT with unbounded LIMIT NULL, got {:?}", other),
    }
}

#[test]
fn parse_distinct_on_as_distinct_select() {
    match parse_statement("select distinct on (id) id, name from people").unwrap() {
        Statement::Select(SelectStatement { distinct: true, .. }) => {}
        other => panic!("expected SELECT DISTINCT ON, got {:?}", other),
    }
}

#[test]
fn parse_create_view_with_for_update_of_clause() {
    match parse_statement("create view locked_people as select * from people for update of people")
        .unwrap()
    {
        Statement::CreateView(CreateViewStatement {
            view_name,
            query:
                SelectStatement {
                    locking_clause: Some(SelectLockingClause::ForUpdate),
                    ..
                },
            ..
        }) => assert_eq!(view_name, "locked_people"),
        other => panic!("expected CREATE VIEW with FOR UPDATE OF, got {:?}", other),
    }
}

#[test]
fn parse_with_recursive_cte_union_all() {
    match parse_statement(
        "with recursive t(n) as (values (1) union all select n + 1 from t) select * from t",
    )
    .unwrap()
    {
        Statement::Select(SelectStatement {
            with_recursive,
            with,
            ..
        }) => {
            assert!(with_recursive);
            assert_eq!(with.len(), 1);
            match &with[0].body {
                crate::backend::parser::CteBody::RecursiveUnion {
                    all,
                    anchor,
                    recursive,
                } => {
                    assert!(*all);
                    assert!(matches!(
                        anchor.as_ref(),
                        crate::backend::parser::CteBody::Values(_)
                    ));
                    assert!(
                        matches!(recursive.from, Some(FromItem::Table { ref name, .. }) if name == "t")
                    );
                }
                other => panic!("expected recursive union CTE body, got {:?}", other),
            }
        }
        other => panic!("expected Select with WITH RECURSIVE, got {:?}", other),
    }
}

#[test]
fn parse_scalar_values_subquery_expr() {
    let stmt = parse_select("select (values (1))").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    match &stmt.targets[0].expr {
        SqlExpr::ScalarSubquery(subquery) => {
            assert!(matches!(subquery.from, Some(FromItem::Values { .. })));
            assert_eq!(subquery.targets.len(), 1);
            assert!(matches!(
                subquery.targets[0],
                SelectItem {
                    ref output_name,
                    expr: SqlExpr::Column(ref name),
                } if output_name == "*" && name == "*"
            ));
        }
        other => panic!("expected scalar subquery, got {other:?}"),
    }
}

#[test]
fn parse_union_all_select_chain() {
    let stmt = parse_select("select 1 as x union all select 2 as x").unwrap();
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(set_operation.op, SetOperator::Union { all: true }));
    assert_eq!(set_operation.inputs.len(), 2);
    assert!(stmt.targets.is_empty());
    assert!(stmt.from.is_none());
}

#[test]
fn parse_values_and_table_as_set_operation_terms() {
    let stmt = parse_select(
        "values (1, 2), (3, 4)
         union all select 5, 6
         union all table int8_tbl",
    )
    .unwrap();
    let outer = stmt.set_operation.expect("set operation");
    assert!(matches!(outer.op, SetOperator::Union { all: true }));
    assert_eq!(outer.inputs.len(), 2);
    assert!(
        matches!(outer.inputs[1].from, Some(FromItem::Table { ref name, .. }) if name == "int8_tbl")
    );
    let inner = outer.inputs[0]
        .set_operation
        .as_ref()
        .expect("left-nested set operation");
    assert!(matches!(inner.op, SetOperator::Union { all: true }));
    assert!(matches!(
        inner.inputs[0].from,
        Some(FromItem::Values { .. })
    ));
}

#[test]
fn parse_parenthesized_union_input_with_extra_parens() {
    let stmt = parse_select("((select 2)) union select 2").unwrap();
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(
        set_operation.op,
        SetOperator::Union { all: false }
    ));
    assert_eq!(set_operation.inputs.len(), 2);
}

#[test]
fn parse_scalar_subquery_with_parenthesized_union_input() {
    let stmt = parse_select("select (((select 2)) union select 2)").unwrap();
    let SqlExpr::ScalarSubquery(subquery) = &stmt.targets[0].expr else {
        panic!("expected scalar subquery, got {:?}", stmt.targets[0].expr);
    };
    assert!(subquery.set_operation.is_some());
}

#[test]
fn parse_empty_select_set_operations() {
    for sql in [
        "select union select",
        "select intersect select",
        "select except select",
    ] {
        let stmt = parse_select(sql).unwrap();
        let set_operation = stmt.set_operation.expect("set operation");
        assert_eq!(set_operation.inputs.len(), 2);
        assert!(
            set_operation
                .inputs
                .iter()
                .all(|input| input.targets.is_empty())
        );
    }
}

#[test]
fn parse_select_distinct_clause() {
    let stmt = parse_select("select distinct x from items").unwrap();
    assert!(stmt.distinct);
    assert!(matches!(stmt.from, Some(FromItem::Table { ref name, .. }) if name == "items"));
}

#[test]
fn parse_mixed_union_chain_preserves_left_associativity() {
    let stmt = parse_select("select 1 as x union select 2 as x union all select 2 as x").unwrap();
    let outer = stmt.set_operation.expect("outer set operation");
    assert!(matches!(outer.op, SetOperator::Union { all: true }));
    assert_eq!(outer.inputs.len(), 2);
    let inner = outer.inputs[0]
        .set_operation
        .as_ref()
        .expect("left-nested set operation");
    assert!(matches!(inner.op, SetOperator::Union { all: false }));
    assert_eq!(inner.inputs.len(), 2);
}

#[test]
fn parse_intersect_precedence_over_union() {
    let stmt = parse_select("select 1 union select 2 intersect select 3").unwrap();
    let outer = stmt.set_operation.expect("outer set operation");
    assert!(matches!(outer.op, SetOperator::Union { all: false }));
    assert_eq!(outer.inputs.len(), 2);
    let right = outer.inputs[1]
        .set_operation
        .as_ref()
        .expect("right-nested set operation");
    assert!(matches!(right.op, SetOperator::Intersect { all: false }));
    assert_eq!(right.inputs.len(), 2);
}

#[test]
fn parse_union_with_top_level_cte_and_order_by() {
    let stmt =
        parse_select("with q(x) as (select 1) select * from q union select * from q order by 1")
            .unwrap();
    assert_eq!(stmt.with.len(), 1);
    assert_eq!(stmt.order_by.len(), 1);
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(
        set_operation.op,
        SetOperator::Union { all: false }
    ));
    assert_eq!(set_operation.inputs.len(), 2);
}

#[test]
fn parse_intersect_all_select_chain() {
    let stmt = parse_select("select 1 intersect all select 1").unwrap();
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(
        set_operation.op,
        SetOperator::Intersect { all: true }
    ));
    assert_eq!(set_operation.inputs.len(), 2);
}

#[test]
fn parse_except_select_chain() {
    let stmt = parse_select("select 1 except select 2").unwrap();
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(
        set_operation.op,
        SetOperator::Except { all: false }
    ));
    assert_eq!(set_operation.inputs.len(), 2);
}

#[test]
fn parse_intersect_with_derived_union_inputs() {
    let stmt = parse_select(
        "select x from (select 1 as x union all select 2 as x) a
         intersect
         select x from (select 2 as x union all select 3 as x) b",
    )
    .unwrap();
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(
        set_operation.op,
        SetOperator::Intersect { all: false }
    ));
    assert_eq!(set_operation.inputs.len(), 2);
}

#[test]
fn parse_parenthesized_select_statement() {
    let stmt = parse_select("((select * from int8_tbl))").unwrap();
    assert!(matches!(
        stmt.from,
        Some(FromItem::Table { ref name, .. }) if name == "int8_tbl"
    ));
}

#[test]
fn parse_parenthesized_set_operation_operand_with_order_limit() {
    let stmt = parse_select(
        "select q1 from int8_tbl except (((select q2 from int8_tbl order by q2 limit 1))) order by 1",
    )
    .unwrap();
    assert_eq!(stmt.order_by.len(), 1);
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(
        set_operation.op,
        SetOperator::Except { all: false }
    ));
    assert_eq!(set_operation.inputs.len(), 2);
    assert_eq!(set_operation.inputs[1].order_by.len(), 1);
    assert_eq!(set_operation.inputs[1].limit, Some(1));
}

#[test]
fn parse_parenthesized_values_set_operation_operand() {
    let stmt = parse_select("select 1 union all (values (2)) limit 1").unwrap();
    assert_eq!(stmt.limit, Some(1));
    let set_operation = stmt.set_operation.expect("set operation");
    assert!(matches!(set_operation.op, SetOperator::Union { all: true }));
    assert!(matches!(
        set_operation.inputs[1].from,
        Some(FromItem::Values { .. })
    ));
}

#[test]
fn parse_cte_materialization_markers() {
    for sql in [
        "with cte as materialized (select 1) select * from cte",
        "with cte as not materialized (select 1) select * from cte",
    ] {
        match parse_statement(sql).unwrap() {
            Statement::Select(SelectStatement { with, .. }) => {
                assert_eq!(with.len(), 1);
                assert!(matches!(
                    with[0].body,
                    crate::backend::parser::CteBody::Select(_)
                ));
            }
            other => panic!("expected Select with CTE, got {other:?}"),
        }
    }
}

#[test]
fn parse_with_recursive_mixed_ctes_and_exists_case() {
    let sql = "with recursive points as (
  select r, c from generate_series(-2, 2, 0.05) a(r)
  cross join generate_series(-2, 2, 0.05) b(c)
  order by r desc, c asc
), iterations as (
   select r, c, c::float as zr, r::float as zc, 0 as iteration from points
   union all
   select r, c, zr*zr - zc*zc + 1 - 1.61803398875 as zr, 2*zr*zc as zc, iteration+1 as iteration
   from iterations where zr*zr + zc*zc < 4 and iteration < 1000
), final_iteration as (
  select * from iterations where iteration = 1000
), marked_points as (
   select r, c, (case when exists (select 1 from final_iteration i where p.r = i.r and p.c = i.c)
                  then '**'
                  else '  '
                  end) as marker
   from points p
   order by r desc, c asc
), rows as (
   select r, string_agg(marker, '') as r_text
   from marked_points
   group by r
   order by r desc
) select string_agg(r_text, e'\\n') from rows";

    match parse_statement(sql).unwrap() {
        Statement::Select(SelectStatement {
            with_recursive,
            with,
            ..
        }) => {
            assert!(with_recursive);
            assert_eq!(with.len(), 5);
            assert!(matches!(with[0].body, CteBody::Select(_)));
            assert!(matches!(
                with[1].body,
                CteBody::RecursiveUnion { all: true, .. }
            ));
            assert!(matches!(with[2].body, CteBody::Select(_)));
            assert!(matches!(with[3].body, CteBody::Select(_)));
            assert!(matches!(with[4].body, CteBody::Select(_)));
        }
        other => panic!("expected Select with WITH RECURSIVE, got {other:?}"),
    }
}

#[test]
fn parse_with_recursive_cte_select_anchor_referencing_prior_cte() {
    let sql = "with recursive points as (select 1 as x), iterations as (
        select x from points
        union all
        select x from iterations
    )
    select * from iterations";
    assert!(parse_select(sql).is_ok());
}

#[test]
fn build_plan_resolves_float_type_alias() {
    let stmt = parse_select("select 1::float, cast(2 as float)").unwrap();
    let planned = build_plan(&stmt, &catalog()).expect("build plan");
    let output = planned.columns();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].sql_type, SqlType::new(SqlTypeKind::Float8));
    assert_eq!(output[1].sql_type, SqlType::new(SqlTypeKind::Float8));
}

#[test]
fn build_plan_for_recursive_mixed_cte_query() {
    fn plan_contains_cte_scan(plan: &Plan) -> bool {
        match plan {
            Plan::CteScan { .. } => true,
            Plan::Append { children, .. }
            | Plan::BitmapOr { children, .. }
            | Plan::MergeAppend { children, .. }
            | Plan::SetOp { children, .. } => children.iter().any(plan_contains_cte_scan),
            Plan::Hash { input, .. }
            | Plan::Filter { input, .. }
            | Plan::OrderBy { input, .. }
            | Plan::IncrementalSort { input, .. }
            | Plan::Limit { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Projection { input, .. }
            | Plan::Unique { input, .. }
            | Plan::Aggregate { input, .. }
            | Plan::WindowAgg { input, .. }
            | Plan::SubqueryScan { input, .. }
            | Plan::ProjectSet { input, .. }
            | Plan::BitmapHeapScan {
                bitmapqual: input, ..
            } => plan_contains_cte_scan(input),
            Plan::NestedLoopJoin { left, right, .. }
            | Plan::HashJoin { left, right, .. }
            | Plan::MergeJoin { left, right, .. } => {
                plan_contains_cte_scan(left) || plan_contains_cte_scan(right)
            }
            Plan::RecursiveUnion {
                anchor, recursive, ..
            } => plan_contains_cte_scan(anchor) || plan_contains_cte_scan(recursive),
            Plan::Result { .. }
            | Plan::SeqScan { .. }
            | Plan::IndexOnlyScan { .. }
            | Plan::IndexScan { .. }
            | Plan::BitmapIndexScan { .. }
            | Plan::FunctionScan { .. }
            | Plan::WorkTableScan { .. }
            | Plan::Values { .. } => false,
        }
    }

    let stmt = parse_select(
        "with recursive points as (
            select r, c from generate_series(-2, 2, 0.05) a(r)
            cross join generate_series(-2, 2, 0.05) b(c)
            order by r desc, c asc
        ), iterations as (
            select r, c, c::float as zr, r::float as zc, 0 as iteration from points
            union all
            select r, c, zr*zr - zc*zc + 1 - 1.61803398875 as zr, 2*zr*zc as zc, iteration+1 as iteration
            from iterations where zr*zr + zc*zc < 4 and iteration < 1000
        ), final_iteration as (
            select * from iterations where iteration = 1000
        ), marked_points as (
            select r, c, (case when exists (
                select 1 from final_iteration i where p.r = i.r and p.c = i.c
            ) then '**' else '  ' end) as marker
            from points p
            order by r desc, c asc
        ), rows as (
            select r, string_agg(marker, '') as r_text
            from marked_points
            group by r
            order by r desc
        )
        select string_agg(r_text, e'\\n') from rows",
    )
    .unwrap();
    let planned = pg_plan_query(&stmt, &catalog()).unwrap();
    assert!(
        plan_contains_cte_scan(&planned.plan_tree),
        "expected outer query plan to use CTE Scan, got {:?}",
        planned.plan_tree
    );
    assert!(
        planned.subplans.iter().any(plan_contains_cte_scan),
        "expected EXISTS subplan to use CTE Scan, got {:?}",
        planned.subplans
    );
}

#[test]
fn parse_recursive_lsystem_segments_query() {
    let sql = "with recursive iterations as (
  select 'FX' as path, 0 as iteration
  union all
  select replace(replace(replace(path, 'X', 'X+ZF+'), 'Y', '-FX-Y'), 'Z', 'Y'), iteration+1 as iteration
  from iterations where iteration < 8
), segments as (
    select
      0 as start_row,
      0 as start_col,
      0 as mid_row,
      0 as mid_col,
      0 as end_row,
      0 as end_col,
      0 as row_diff,
      1 as col_diff,
      (select path from iterations order by iteration desc limit 1) as path_left
  union all
    select
      end_row as start_row,
      end_col as start_col,
      end_row + row_diff * step_size as mid_row,
      end_col + col_diff * step_size as mid_col,
      end_row + 2 * row_diff * step_size as end_row,
      end_col + 2 * col_diff * step_size as end_col,
      case when substring(path_left for 1) = '-' then -col_diff
           when substring(path_left for 1) = '+' then col_diff
           else row_diff
      end as row_diff,
      case when substring(path_left for 1) = '-' then row_diff
           when substring(path_left for 1) = '+' then -row_diff
           else col_diff
      end as col_diff,
      substring(path_left from 2) as path_left
    from segments, lateral (select case when substring(path_left for 1) = 'F' then 1 else 0 end as step_size) sub
    where char_length(path_left) > 0
) select count(*) from segments";
    assert!(parse_select(sql).is_ok());
}

#[test]
fn parse_recursive_lsystem_points_query() {
    let sql = "with recursive iterations as (
  select 'FX' as path, 0 as iteration
  union all
  select replace(replace(replace(path, 'X', 'X+ZF+'), 'Y', '-FX-Y'), 'Z', 'Y'), iteration+1 as iteration
  from iterations where iteration < 8
), segments as (
    select
      0 as start_row,
      0 as start_col,
      0 as mid_row,
      0 as mid_col,
      0 as end_row,
      0 as end_col,
      0 as row_diff,
      1 as col_diff,
      (select path from iterations order by iteration desc limit 1) as path_left
  union all
    select
      end_row as start_row,
      end_col as start_col,
      end_row + row_diff * step_size as mid_row,
      end_col + col_diff * step_size as mid_col,
      end_row + 2 * row_diff * step_size as end_row,
      end_col + 2 * col_diff * step_size as end_col,
      case when substring(path_left for 1) = '-' then -col_diff
           when substring(path_left for 1) = '+' then col_diff
           else row_diff
      end as row_diff,
      case when substring(path_left for 1) = '-' then row_diff
           when substring(path_left for 1) = '+' then -row_diff
           else col_diff
      end as col_diff,
      substring(path_left from 2) as path_left
    from segments, lateral (select case when substring(path_left for 1) = 'F' then 1 else 0 end as step_size) sub
    where char_length(path_left) > 0
), end_points as (
  select start_row as r, start_col as c from segments union select end_row as r, end_col as c from segments
), points as (
  select r, c from generate_series((select min(r) from end_points), (select max(r) from end_points)) a(r)
  cross join generate_series((select min(c) from end_points), (select max(c) from end_points)) b(c)
), marked_points as (
  select r, c, (case when
    exists (select 1 from end_points e where p.r = e.r and p.c = e.c)
    then '*'

    when exists (select 1 from segments s where p.r = s.mid_row and p.c = s.mid_col and col_diff != 0)
    then '-'

    when exists (select 1 from segments s where p.r = s.mid_row and p.c = s.mid_col and row_diff != 0)
    then '|'

    else ' '
    end
    ) as marker
  from points p
), lines as (
   select r, string_agg(marker, '') as row_text
   from marked_points
   group by r
   order by r desc
) select string_agg(row_text, E'\n') from lines";
    assert!(parse_select(sql).is_ok());
}

#[test]
fn parse_create_table_primary_key_and_unique_constraints() {
    let stmt =
        parse_statement("create table items (id int4 primary key, note text unique)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(columns.len(), 2);
    assert!(columns[0].primary_key());
    assert!(columns[1].unique());
    assert_eq!(ct.constraints().count(), 0);

    let stmt = parse_statement(
        "create table items (id int4, note text, primary key (id, note), unique (note, id))",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(ct.columns().count(), 2);
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![
            TableConstraint::PrimaryKey {
                attributes: attrs(),
                columns: vec!["id".into(), "note".into()],
                include_columns: Vec::new(),
                without_overlaps: None,
            },
            TableConstraint::Unique {
                attributes: attrs(),
                columns: vec!["note".into(), "id".into()],
                include_columns: Vec::new(),
                without_overlaps: None,
            },
        ]
    );

    let stmt = parse_statement(
        "create table items (id int4, note text, payload int4, primary key (id, note) include (payload), unique (note) include (id, payload))",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![
            TableConstraint::PrimaryKey {
                attributes: attrs(),
                columns: vec!["id".into(), "note".into()],
                include_columns: vec!["payload".into()],
                without_overlaps: None,
            },
            TableConstraint::Unique {
                attributes: attrs(),
                columns: vec!["note".into()],
                include_columns: vec!["id".into(), "payload".into()],
                without_overlaps: None,
            },
        ]
    );

    let stmt =
        parse_statement("create table items (id int4, constraint named_pk primary key (id))")
            .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![TableConstraint::PrimaryKey {
            attributes: ConstraintAttributes {
                name: Some("named_pk".into()),
                ..attrs()
            },
            columns: vec!["id".into()],
            include_columns: Vec::new(),
            without_overlaps: None,
        }]
    );

    let stmt = parse_statement(
        "create table items (id int4, valid_at int4range, constraint temporal_pk primary key (id, valid_at without overlaps), unique (id, valid_at without overlaps))",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![
            TableConstraint::PrimaryKey {
                attributes: ConstraintAttributes {
                    name: Some("temporal_pk".into()),
                    ..attrs()
                },
                columns: vec!["id".into(), "valid_at".into()],
                include_columns: Vec::new(),
                without_overlaps: Some("valid_at".into()),
            },
            TableConstraint::Unique {
                attributes: attrs(),
                columns: vec!["id".into(), "valid_at".into()],
                include_columns: Vec::new(),
                without_overlaps: Some("valid_at".into()),
            },
        ]
    );

    let stmt = parse_statement("create table items (id int4 unique nulls not distinct, note text)")
        .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(
        columns[0].constraints,
        vec![ColumnConstraint::Unique {
            attributes: ConstraintAttributes {
                nulls_not_distinct: true,
                ..attrs()
            }
        }]
    );
}

#[test]
fn parse_create_table_deferrable_key_constraints() {
    let stmt = parse_statement(
        "create table items (
            id int4 primary key initially deferred,
            code int4,
            constraint items_code_key unique (code) deferrable initially immediate
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };

    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(
        columns[0].constraints,
        vec![ColumnConstraint::PrimaryKey {
            attributes: ConstraintAttributes {
                deferrable: Some(true),
                initially_deferred: Some(true),
                ..attrs()
            }
        }]
    );
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![TableConstraint::Unique {
            attributes: ConstraintAttributes {
                name: Some("items_code_key".into()),
                deferrable: Some(true),
                initially_deferred: Some(false),
                ..attrs()
            },
            columns: vec!["code".into()],
            include_columns: Vec::new(),
            without_overlaps: None,
        }]
    );
}

#[test]
fn parse_create_table_rejects_invalid_key_deferrability_clauses() {
    let err = parse_statement("create table items (id int4 primary key deferrable not deferrable)")
        .unwrap_err();
    assert!(matches!(
        err,
        ParseError::FeatureNotSupportedMessage(message)
            if message == "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"
    ));

    let err = parse_statement(
        "create table items (id int4 primary key initially immediate initially deferred)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::FeatureNotSupportedMessage(message)
            if message == "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"
    ));

    let err = parse_statement(
        "create table items (id int4 primary key not deferrable initially deferred)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::FeatureNotSupportedMessage(message)
            if message == "constraint declared INITIALLY DEFERRED must be DEFERRABLE"
    ));
}

#[test]
fn parse_create_table_named_check_and_not_null_constraints() {
    let stmt = parse_statement(
        "create table items (id int4 constraint id_positive check (id > 0) not valid deferrable initially deferred not enforced, note text, constraint note_present not null note not valid, constraint note_nonempty check (note <> '') not deferrable initially immediate enforced)",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };

    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(
        columns[0].constraints,
        vec![ColumnConstraint::Check {
            attributes: ConstraintAttributes {
                name: Some("id_positive".into()),
                not_valid: true,
                no_inherit: false,
                deferrable: Some(true),
                initially_deferred: Some(true),
                enforced: Some(false),
                nulls_not_distinct: false,
            },
            expr_sql: "id > 0".into(),
        }]
    );
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![
            TableConstraint::NotNull {
                attributes: ConstraintAttributes {
                    name: Some("note_present".into()),
                    not_valid: true,
                    no_inherit: false,
                    deferrable: None,
                    initially_deferred: None,
                    enforced: None,
                    nulls_not_distinct: false,
                },
                column: "note".into(),
            },
            TableConstraint::Check {
                attributes: ConstraintAttributes {
                    name: Some("note_nonempty".into()),
                    not_valid: false,
                    no_inherit: false,
                    deferrable: Some(false),
                    initially_deferred: Some(false),
                    enforced: Some(true),
                    nulls_not_distinct: false,
                },
                expr_sql: "note <> ''".into(),
            },
        ]
    );
}

#[test]
fn parse_create_table_foreign_key_constraints() {
    let stmt = parse_statement(
        "create table pets (
            owner_id int4 references people(id),
            owner_name text,
            foreign key (owner_name) references people(name) match simple on delete restrict on update no action
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(
        columns[0].constraints,
        vec![ColumnConstraint::References {
            attributes: attrs(),
            referenced_table: "people".into(),
            referenced_columns: Some(vec!["id".into()]),
            match_type: ForeignKeyMatchType::Simple,
            on_delete: ForeignKeyAction::NoAction,
            on_delete_set_columns: None,
            on_update: ForeignKeyAction::NoAction,
        }]
    );
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![TableConstraint::ForeignKey {
            attributes: attrs(),
            columns: vec!["owner_name".into()],
            period: None,
            referenced_table: "people".into(),
            referenced_columns: Some(vec!["name".into()]),
            referenced_period: None,
            match_type: ForeignKeyMatchType::Simple,
            on_delete: ForeignKeyAction::Restrict,
            on_delete_set_columns: None,
            on_update: ForeignKeyAction::NoAction,
        }]
    );

    let stmt = parse_statement(
        "create table temporal_fk (
            parent_id int4range,
            valid_at daterange,
            constraint temporal_fk_parent_fk foreign key (parent_id, period valid_at) references temporal_parent(id, period valid_at)
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![TableConstraint::ForeignKey {
            attributes: ConstraintAttributes {
                name: Some("temporal_fk_parent_fk".into()),
                ..attrs()
            },
            columns: vec!["parent_id".into(), "valid_at".into()],
            period: Some("valid_at".into()),
            referenced_table: "temporal_parent".into(),
            referenced_columns: Some(vec!["id".into(), "valid_at".into()]),
            referenced_period: Some("valid_at".into()),
            match_type: ForeignKeyMatchType::Simple,
            on_delete: ForeignKeyAction::NoAction,
            on_delete_set_columns: None,
            on_update: ForeignKeyAction::NoAction,
        }]
    );

    let err = parse_statement(
        "create temp table fktable2 (fk int references pktable enforced not enforced)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::FeatureNotSupportedMessage(message)
            if message == "multiple ENFORCED/NOT ENFORCED clauses not allowed"
    ));

    let err = parse_statement(
        "create table fktable (
            tid int,
            id int,
            foo int,
            foreign key (tid, foo) references pktable on update set null (foo)
        )",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::FeatureNotSupportedMessage(message)
            if message == "a column list with SET NULL is only supported for ON DELETE actions"
    ));
}

#[test]
fn lower_create_table_rejects_invalid_key_constraints() {
    let stmt =
        parse_statement("create table items (id int4 primary key, note text, primary key (note))")
            .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog),
        Err(ParseError::UnexpectedToken { expected, .. }) if expected == "at most one PRIMARY KEY"
    ));

    let stmt = parse_statement("create table items (id int4, note text, unique (id, id))").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog),
        Err(ParseError::UnexpectedToken { expected, actual })
            if expected == "unique column names in table constraint"
                && actual == "duplicate column in constraint: id"
    ));

    let stmt =
        parse_statement("create table items (id int4, note text, unique (missing))").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog),
        Err(ParseError::UnknownColumn(name)) if name == "missing"
    ));

    let stmt = parse_statement("create table items (id int4 primary key, unique (id))").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog),
        Err(ParseError::UnexpectedToken { expected, actual })
            if expected == "distinct PRIMARY KEY/UNIQUE definitions"
                && actual == "duplicate key definition on (id)"
    ));
}

#[test]
fn lower_create_table_accepts_check_not_enforced() {
    let stmt = parse_statement("create table items (id int4 check (id > 0) not enforced)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };

    let lowered = lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog)
        .expect("lower create table");
    assert_eq!(lowered.check_actions.len(), 1);
    assert_eq!(lowered.check_actions[0].constraint_name, "items_id_check");
    assert!(!lowered.check_actions[0].enforced);
}

#[test]
fn lower_create_table_preserves_key_constraint_deferrability() {
    let stmt = parse_statement(
        "create table items (
            id int4 primary key initially deferred,
            code int4,
            constraint items_code_key unique (code) deferrable initially immediate
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };

    let lowered = lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog)
        .expect("lower create table");
    assert_eq!(lowered.constraint_actions.len(), 2);
    assert!(lowered.constraint_actions.iter().any(|action| {
        action.constraint_name.as_deref() == Some("items_pkey")
            && action.primary
            && action.columns == vec!["id".to_string()]
            && action.deferrable
            && action.initially_deferred
    }));
    assert!(lowered.constraint_actions.iter().any(|action| {
        action.constraint_name.as_deref() == Some("items_code_key")
            && !action.primary
            && action.columns == vec!["code".to_string()]
            && action.deferrable
            && !action.initially_deferred
    }));
}

#[test]
fn lower_create_table_uses_foreign_key_column_errors() {
    let catalog = catalog_with_people_primary_key();

    let stmt = parse_statement(
        "create table fktable (
            ftest1 int4,
            constraint fkfail1 foreign key (ftest2) references people
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let err = lower_create_table(&ct, &catalog).unwrap_err();
    assert!(
        matches!(
            &err,
            ParseError::DetailedError {
                message,
                sqlstate,
                ..
            } if message == "column \"ftest2\" referenced in foreign key constraint does not exist"
                && *sqlstate == "42703"
        ),
        "unexpected error: {err:?}"
    );

    let stmt = parse_statement(
        "create table fktable (
            ftest1 int4,
            constraint fkfail1 foreign key (tableoid) references people(id)
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let err = lower_create_table(&ct, &catalog).unwrap_err();
    assert!(
        matches!(
            &err,
            ParseError::DetailedError {
                message,
                sqlstate,
                ..
            } if message == "system columns cannot be used in foreign keys"
                && *sqlstate == "0A000"
        ),
        "unexpected error: {err:?}"
    );

    let stmt = parse_statement(
        "create table fktable (
            ftest1 int4,
            constraint fkfail1 foreign key (ftest1) references people(tableoid)
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let err = lower_create_table(&ct, &catalog).unwrap_err();
    assert!(
        matches!(
            &err,
            ParseError::DetailedError {
                message,
                sqlstate,
                ..
            } if message == "system columns cannot be used in foreign keys"
                && *sqlstate == "0A000"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn lower_create_table_rejects_invalid_foreign_key_delete_set_columns() {
    let catalog = catalog_with_people_id_name_unique_index();

    let stmt = parse_statement(
        "create table fktable (
            owner_id int4,
            owner_name text,
            foo int,
            foreign key (owner_id, owner_name) references people(id, name) on delete set null (bar)
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let err = lower_create_table(&ct, &catalog).unwrap_err();
    assert!(
        matches!(
            &err,
            ParseError::DetailedError {
                message,
                sqlstate,
                ..
            } if message == "column \"bar\" referenced in foreign key constraint does not exist"
                && *sqlstate == "42703"
        ),
        "unexpected error: {err:?}"
    );

    let stmt = parse_statement(
        "create table fktable (
            owner_id int4,
            owner_name text,
            foo int,
            foreign key (owner_id, owner_name) references people(id, name) on delete set null (foo)
        )",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let err = lower_create_table(&ct, &catalog).unwrap_err();
    assert!(
        matches!(
            &err,
            ParseError::DetailedError {
                message,
                sqlstate,
                ..
            } if message == "column \"foo\" referenced in ON DELETE SET action must be part of foreign key"
                && *sqlstate == "42P10"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn lower_create_table_rejects_duplicate_constraint_names() {
    let stmt = parse_statement(
        "create table items (id int4 constraint dup not null, note text, constraint dup unique (note))",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog),
        Err(ParseError::UnexpectedToken { expected, actual })
            if expected == "distinct constraint names"
                && actual == "duplicate constraint name: dup"
    ));
}

#[test]
fn lower_create_table_rejects_unsupported_constraint_attributes() {
    let stmt = parse_statement("create table items (id int4 check (id > 0) deferrable)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog),
        Err(ParseError::FeatureNotSupported(feature)) if feature == "CHECK DEFERRABLE"
    ));

    let stmt = parse_statement("create table items (id int4 primary key not valid)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog),
        Err(ParseError::FeatureNotSupported(feature)) if feature == "PRIMARY KEY NOT VALID"
    ));

    let stmt = parse_statement("create table pets (owner_id int4 references people(id) not valid)")
        .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let lowered = lower_create_table(&ct, &catalog_with_people_primary_key()).unwrap();
    assert_eq!(lowered.foreign_key_actions.len(), 1);
    assert!(lowered.foreign_key_actions[0].not_valid);
}

#[test]
fn lower_create_table_collapses_duplicate_not_null_constraints() {
    let stmt = parse_statement(
        "create table items (id int4 not null, constraint items_id_nn not null id)",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let lowered =
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog).unwrap();
    assert_eq!(lowered.not_null_actions.len(), 1);
    assert_eq!(
        lowered.relation_desc.columns[0]
            .not_null_constraint_name
            .as_deref(),
        Some("items_id_nn")
    );
}

#[test]
fn lower_create_table_resolves_foreign_keys_against_primary_keys() {
    let stmt = parse_statement("create table pets (owner_id int4 references people)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let lowered = lower_create_table(&ct, &catalog_with_people_primary_key()).unwrap();
    assert_eq!(lowered.foreign_key_actions.len(), 1);
    let foreign_key = &lowered.foreign_key_actions[0];
    assert_eq!(foreign_key.constraint_name, "pets_owner_id_fkey");
    assert_eq!(foreign_key.columns, vec!["owner_id".to_string()]);
    assert_eq!(foreign_key.referenced_table, "people");
    assert_eq!(foreign_key.referenced_columns, vec!["id".to_string()]);
    assert_eq!(foreign_key.match_type, ForeignKeyMatchType::Simple);
    assert_eq!(foreign_key.on_delete, ForeignKeyAction::NoAction);
    assert_eq!(foreign_key.on_update, ForeignKeyAction::NoAction);
    assert!(!foreign_key.self_referential);
}

#[test]
fn lower_create_table_supports_match_full_and_foreign_key_actions() {
    let stmt = parse_statement(
        "create table pets (owner_id int4, owner_name text, foreign key (owner_id, owner_name) references people(id, name) match full on delete set null on update cascade)",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let lowered = lower_create_table(&ct, &catalog_with_people_id_name_unique_index()).unwrap();
    assert_eq!(lowered.foreign_key_actions.len(), 1);
    let foreign_key = &lowered.foreign_key_actions[0];
    assert_eq!(foreign_key.match_type, ForeignKeyMatchType::Full);
    assert_eq!(foreign_key.on_delete, ForeignKeyAction::SetNull);
    assert_eq!(foreign_key.on_update, ForeignKeyAction::Cascade);
}

#[test]
fn lower_create_table_resolves_self_referential_foreign_keys_against_pending_primary_key() {
    let stmt = parse_statement(
        "create temp table department (id int4 primary key, parent_department int4 references department)",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let lowered =
        lower_create_table(&ct, &crate::backend::parser::analyze::LiteralDefaultCatalog).unwrap();
    assert_eq!(lowered.foreign_key_actions.len(), 1);
    let foreign_key = &lowered.foreign_key_actions[0];
    assert_eq!(foreign_key.columns, vec!["parent_department".to_string()]);
    assert_eq!(foreign_key.referenced_table, "department");
    assert_eq!(foreign_key.referenced_columns, vec!["id".to_string()]);
    assert!(foreign_key.self_referential);
}

#[test]
fn lower_create_table_rejects_temp_foreign_keys_to_permanent_tables() {
    let stmt = parse_statement("create temp table pets (owner_id int4 references people)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &catalog_with_people_primary_key()),
        Err(ParseError::InvalidTableDefinition(message))
            if message == "constraints on temporary tables may reference only temporary tables"
    ));
}

#[test]
fn lower_create_table_rejects_cross_type_foreign_keys() {
    let stmt = parse_statement("create table pets (owner_id int4 references labels(id))").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct, &catalog_with_text_parent_primary_key()),
        Err(ParseError::DetailedError { message, detail: Some(detail), sqlstate: "42804", .. })
            if message == "foreign key constraint \"pets_owner_id_fkey\" cannot be implemented"
                && detail.contains("incompatible types")
    ));
}

#[test]
fn parse_create_drop_and_comment_on_domain_statements() {
    let Statement::CreateDomain(create) = parse_statement("create domain dom_int as int4").unwrap()
    else {
        panic!("expected create domain");
    };
    assert_eq!(create.domain_name, "dom_int");
    assert_eq!(
        create.ty,
        RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4))
    );
    assert_eq!(create.default, None);
    assert_eq!(create.check, None);
    assert!(!create.not_null);

    let Statement::CreateDomain(create) = parse_statement(
        "create domain small_mr as int4multirange default '{}' check (upper(value) < 10) not null",
    )
    .unwrap() else {
        panic!("expected create domain");
    };
    assert_eq!(
        create.ty,
        RawTypeName::Named {
            name: "int4multirange".into(),
            array_bounds: 0,
        }
    );
    assert_eq!(create.default.as_deref(), Some("'{}'"));
    assert_eq!(create.check.as_deref(), Some("upper(value) < 10"));
    assert!(create.not_null);

    let Statement::CreateDomain(create) =
        parse_statement("create domain restrictedrange as int4range check (upper(value) < 10)")
            .unwrap()
    else {
        panic!("expected create domain");
    };
    assert_eq!(create.domain_name, "restrictedrange");
    assert_eq!(
        create.ty,
        RawTypeName::Named {
            name: "int4range".into(),
            array_bounds: 0
        }
    );
    assert_eq!(create.check.as_deref(), Some("upper(value) < 10"));

    let Statement::DropDomain(drop_stmt) =
        parse_statement("drop domain if exists dom_int cascade").unwrap()
    else {
        panic!("expected drop domain");
    };
    assert!(drop_stmt.if_exists);
    assert!(drop_stmt.cascade);
    assert_eq!(drop_stmt.domain_name, "dom_int");
    assert_eq!(drop_stmt.domain_names, vec!["dom_int"]);

    let Statement::CommentOnDomain(comment) =
        parse_statement("comment on domain dom_int is 'hello'").unwrap()
    else {
        panic!("expected comment on domain");
    };
    assert_eq!(comment.domain_name, "dom_int");
    assert_eq!(comment.comment.as_deref(), Some("hello"));
}

#[test]
fn parse_alter_type_rename_to_statement() {
    let Statement::AlterType(AlterTypeStatement::RenameType(rename)) =
        parse_statement("alter type bogus rename to bogon").unwrap()
    else {
        panic!("expected alter type rename");
    };
    assert_eq!(rename.schema_name, None);
    assert_eq!(rename.type_name, "bogus");
    assert_eq!(rename.new_type_name, "bogon");
}

#[test]
fn parse_alter_type_composite_attribute_actions() {
    let Statement::AlterType(AlterTypeStatement::AlterComposite(stmt)) = parse_statement(
        "alter type person add attribute age int4 cascade, rename attribute name to full_name restrict",
    )
    .unwrap()
    else {
        panic!("expected alter type composite");
    };
    assert_eq!(stmt.type_name, "person");
    assert_eq!(stmt.actions.len(), 2);
    match &stmt.actions[0] {
        AlterCompositeTypeAction::AddAttribute { attribute, cascade } => {
            assert_eq!(attribute.name, "age");
            assert_eq!(attribute.ty, RawTypeName::builtin(SqlTypeKind::Int4));
            assert!(*cascade);
        }
        other => panic!("expected add attribute, got {other:?}"),
    }
    match &stmt.actions[1] {
        AlterCompositeTypeAction::RenameAttribute {
            old_name,
            new_name,
            cascade,
        } => {
            assert_eq!(old_name, "name");
            assert_eq!(new_name, "full_name");
            assert!(!*cascade);
        }
        other => panic!("expected rename attribute, got {other:?}"),
    }
}

#[test]
fn parse_alter_type_set_options_statement() {
    let Statement::AlterType(AlterTypeStatement::SetOptions(stmt)) = parse_statement(
        "alter type public.myvarchar set (storage = extended, send = myvarcharsend, typmod_in = varchartypmodin)",
    )
    .unwrap() else {
        panic!("expected alter type set options");
    };
    assert_eq!(
        stmt,
        AlterTypeSetOptionsStatement {
            schema_name: Some("public".into()),
            type_name: "myvarchar".into(),
            options: vec![
                CreateBaseTypeOption {
                    name: "storage".into(),
                    value: Some("extended".into()),
                },
                CreateBaseTypeOption {
                    name: "send".into(),
                    value: Some("myvarcharsend".into()),
                },
                CreateBaseTypeOption {
                    name: "typmod_in".into(),
                    value: Some("varchartypmodin".into()),
                },
            ],
        }
    );
}

#[test]
fn parse_create_drop_and_comment_on_conversion_statements() {
    let Statement::CreateConversion(create) = parse_statement(
        "create default conversion public.mydef for 'LATIN1' to 'UTF8' from iso8859_1_to_utf8",
    )
    .unwrap() else {
        panic!("expected create conversion");
    };
    assert_eq!(create.conversion_name, "public.mydef");
    assert_eq!(create.for_encoding, "LATIN1");
    assert_eq!(create.to_encoding, "UTF8");
    assert_eq!(create.function_name, "iso8859_1_to_utf8");
    assert!(create.is_default);

    let Statement::DropConversion(drop_stmt) =
        parse_statement("drop conversion if exists myconv cascade").unwrap()
    else {
        panic!("expected drop conversion");
    };
    assert!(drop_stmt.if_exists);
    assert!(drop_stmt.cascade);
    assert_eq!(drop_stmt.conversion_name, "myconv");

    let Statement::CommentOnConversion(comment) =
        parse_statement("comment on conversion myconv is 'hello'").unwrap()
    else {
        panic!("expected comment on conversion");
    };
    assert_eq!(comment.conversion_name, "myconv");
    assert_eq!(comment.comment.as_deref(), Some("hello"));
    let Statement::AlterConversion(alter) =
        parse_statement("alter conversion public.myconv owner to app_owner").unwrap()
    else {
        panic!("expected alter conversion");
    };
    assert_eq!(alter.conversion_name, "public.myconv");
    assert_eq!(
        alter.action,
        AlterConversionAction::OwnerTo {
            new_owner: "app_owner".into()
        }
    );
}

#[test]
fn parse_foreign_data_wrapper_statements() {
    let Statement::CreateForeignDataWrapper(create) = parse_statement(
        "create foreign data wrapper foo handler pg_rust_test_fdw_handler validator postgresql_fdw_validator options (testing '1', another '2')",
    )
    .unwrap() else {
        panic!("expected create foreign data wrapper");
    };
    assert_eq!(create.fdw_name, "foo");
    assert_eq!(
        create.handler_name.as_deref(),
        Some("pg_rust_test_fdw_handler")
    );
    assert_eq!(
        create.validator_name.as_deref(),
        Some("postgresql_fdw_validator")
    );
    assert_eq!(
        create.options,
        vec![
            RelOption {
                name: "testing".into(),
                value: "1".into(),
            },
            RelOption {
                name: "another".into(),
                value: "2".into(),
            },
        ]
    );

    let Statement::AlterForeignDataWrapper(alter) = parse_statement(
        "alter foreign data wrapper foo no validator options (drop a, set b '2', add c '3')",
    )
    .unwrap() else {
        panic!("expected alter foreign data wrapper");
    };
    assert_eq!(alter.fdw_name, "foo");
    assert_eq!(alter.validator_name, Some(None));
    assert_eq!(alter.options.len(), 3);

    let err = parse_statement("alter foreign data wrapper foo;").unwrap_err();
    assert_eq!(err.to_string(), "syntax error at or near \";\"");
    assert!(err.position().is_some());

    let Statement::AlterForeignDataWrapperOwner(owner) =
        parse_statement("alter foreign data wrapper foo owner to regress_test_role").unwrap()
    else {
        panic!("expected alter foreign data wrapper owner");
    };
    assert_eq!(owner.fdw_name, "foo");
    assert_eq!(owner.new_owner, "regress_test_role");

    let Statement::AlterForeignDataWrapperRename(rename) =
        parse_statement("alter foreign data wrapper foo rename to bar").unwrap()
    else {
        panic!("expected alter foreign data wrapper rename");
    };
    assert_eq!(rename.fdw_name, "foo");
    assert_eq!(rename.new_name, "bar");

    let Statement::CreateForeignServer(server) =
        parse_statement("create server srv1 foreign data wrapper foo").unwrap()
    else {
        panic!("expected create foreign server");
    };
    assert_eq!(server.server_name, "srv1");
    assert_eq!(server.fdw_name, "foo");

    let Statement::AlterForeignServerRename(server_rename) =
        parse_statement("alter server srv1 rename to srv2").unwrap()
    else {
        panic!("expected alter foreign server rename");
    };
    assert_eq!(server_rename.server_name, "srv1");
    assert_eq!(server_rename.new_name, "srv2");

    let Statement::DropForeignDataWrapper(drop_stmt) =
        parse_statement("drop foreign data wrapper if exists foo cascade").unwrap()
    else {
        panic!("expected drop foreign data wrapper");
    };
    assert!(drop_stmt.if_exists);
    assert!(drop_stmt.cascade);
    assert_eq!(drop_stmt.fdw_name, "foo");

    let Statement::CommentOnForeignDataWrapper(comment) =
        parse_statement("comment on foreign data wrapper foo is 'hello'").unwrap()
    else {
        panic!("expected comment on foreign data wrapper");
    };
    assert_eq!(comment.fdw_name, "foo");
    assert_eq!(comment.comment.as_deref(), Some("hello"));

    let Statement::CommentOnForeignServer(comment) =
        parse_statement("comment on server srv is 'foreign server'").unwrap()
    else {
        panic!("expected comment on server");
    };
    assert_eq!(comment.server_name, "srv");
    assert_eq!(comment.comment.as_deref(), Some("foreign server"));

    let Statement::CreateForeignServer(create_server) = parse_statement(
        "create server if not exists srv type 'postgres' version '17' foreign data wrapper foo options (host 'localhost', dbname 'regression')",
    )
    .unwrap() else {
        panic!("expected create server");
    };
    assert!(create_server.if_not_exists);
    assert_eq!(create_server.server_name, "srv");
    assert_eq!(create_server.fdw_name, "foo");
    assert_eq!(create_server.server_type.as_deref(), Some("postgres"));
    assert_eq!(create_server.version.as_deref(), Some("17"));
    assert_eq!(
        create_server.options,
        vec![
            RelOption {
                name: "host".into(),
                value: "localhost".into(),
            },
            RelOption {
                name: "dbname".into(),
                value: "regression".into(),
            },
        ]
    );

    let Statement::AlterForeignServer(alter_server) =
        parse_statement("alter server srv version null options (set host '127.0.0.1')").unwrap()
    else {
        panic!("expected alter server");
    };
    assert_eq!(alter_server.server_name, "srv");
    assert_eq!(alter_server.version, Some(None));
    assert_eq!(alter_server.options.len(), 1);
    assert_eq!(
        alter_server.options[0].action,
        AlterGenericOptionAction::Set
    );
    assert_eq!(alter_server.options[0].name, "host");
    assert_eq!(alter_server.options[0].value.as_deref(), Some("127.0.0.1"));

    let err = parse_statement("alter server srv;").unwrap_err();
    assert_eq!(err.to_string(), "syntax error at or near \";\"");
    assert!(err.position().is_some());

    let Statement::CreateForeignTable(create_table) = parse_statement(
        "create foreign table ft (a int options (column_name 'remote_a') not null, b text) server srv options (table_name 'remote_ft')",
    )
    .unwrap() else {
        panic!("expected create foreign table");
    };
    assert_eq!(create_table.create_table.table_name, "ft");
    assert_eq!(create_table.server_name, "srv");
    assert_eq!(
        create_table.options,
        vec![RelOption {
            name: "table_name".into(),
            value: "remote_ft".into(),
        }]
    );
    assert_eq!(
        create_table.column_options,
        vec![(
            "a".into(),
            vec![RelOption {
                name: "column_name".into(),
                value: "remote_a".into(),
            }]
        )]
    );
    assert_eq!(create_table.create_table.elements.len(), 2);

    let Statement::AlterTableAddColumn(add_column) = parse_statement(
        "alter foreign table ft add column if not exists c int options (column_name 'remote_c')",
    )
    .unwrap() else {
        panic!("expected alter foreign table add column");
    };
    assert!(add_column.missing_ok);
    assert_eq!(add_column.table_name, "ft");
    assert_eq!(add_column.column.name, "c");
    assert_eq!(
        add_column.fdw_options,
        Some(vec![RelOption {
            name: "column_name".into(),
            value: "remote_c".into(),
        }])
    );

    let Statement::AlterTableAlterColumnOptions(column_options) =
        parse_statement("alter foreign table ft alter column c options (add p1 'v1', set p2 'v2')")
            .unwrap()
    else {
        panic!("expected alter foreign table column options");
    };
    assert_eq!(column_options.table_name, "ft");
    assert_eq!(column_options.column_name, "c");
    let AlterColumnOptionsAction::Fdw(options) = column_options.action else {
        panic!("expected fdw column options action");
    };
    assert_eq!(options.len(), 2);
    assert_eq!(options[0].action, AlterGenericOptionAction::Add);
    assert_eq!(options[1].action, AlterGenericOptionAction::Set);

    let Statement::AlterTableAddColumn(add_column) = parse_statement(
        "alter foreign table ft add column b text options (column_name 'remote_b')",
    )
    .unwrap() else {
        panic!("expected alter foreign table add column without missing_ok");
    };
    assert!(!add_column.missing_ok);
    assert_eq!(
        add_column.fdw_options,
        Some(vec![RelOption {
            name: "column_name".into(),
            value: "remote_b".into(),
        }])
    );

    let Statement::AlterForeignTableOptions(table_options) = parse_statement(
        "alter foreign table ft options (drop delimiter, set quote '~', add escape '@')",
    )
    .unwrap() else {
        panic!("expected alter foreign table options");
    };
    assert_eq!(table_options.table_name, "ft");
    assert_eq!(table_options.options.len(), 3);
    assert_eq!(
        table_options.options[0].action,
        AlterGenericOptionAction::Drop
    );
    assert_eq!(
        table_options.options[1].action,
        AlterGenericOptionAction::Set
    );
    assert_eq!(
        table_options.options[2].action,
        AlterGenericOptionAction::Add
    );

    let Statement::AlterTableRenameColumn(rename_column) =
        parse_statement("alter foreign table if exists ft rename c1 to c2").unwrap()
    else {
        panic!("expected alter foreign table rename column");
    };
    assert!(rename_column.if_exists);
    assert_eq!(rename_column.table_name, "ft");
    assert_eq!(rename_column.column_name, "c1");
    assert_eq!(rename_column.new_column_name, "c2");

    let Statement::CreateUserMapping(create_mapping) = parse_statement(
        "create user mapping if not exists for current_user server srv options (user 'alice')",
    )
    .unwrap() else {
        panic!("expected create user mapping");
    };
    assert!(create_mapping.if_not_exists);
    assert_eq!(create_mapping.user, UserMappingUser::CurrentUser);
    assert_eq!(create_mapping.server_name, "srv");
    assert_eq!(
        create_mapping.options,
        vec![RelOption {
            name: "user".into(),
            value: "alice".into(),
        }]
    );

    let Statement::DropUserMapping(drop_mapping) =
        parse_statement("drop user mapping if exists for public server srv").unwrap()
    else {
        panic!("expected drop user mapping");
    };
    assert!(drop_mapping.if_exists);
    assert_eq!(drop_mapping.user, UserMappingUser::Public);
    assert_eq!(drop_mapping.server_name, "srv");

    let Statement::ImportForeignSchema(import_schema) = parse_statement(
        "import foreign schema remote_schema except (t1, t2) from server srv into public options (sample 'true')",
    )
    .unwrap() else {
        panic!("expected import foreign schema");
    };
    assert_eq!(import_schema.remote_schema, "remote_schema");
    assert_eq!(
        import_schema.restriction,
        ImportForeignSchemaRestriction::Except(vec!["t1".into(), "t2".into()])
    );
    assert_eq!(import_schema.server_name, "srv");
    assert_eq!(import_schema.local_schema, "public");
    assert_eq!(
        import_schema.options,
        vec![RelOption {
            name: "sample".into(),
            value: "true".into(),
        }]
    );
}

#[test]
fn parse_language_statements() {
    let Statement::CreateLanguage(create) =
        parse_statement("create language alt_lang handler plpgsql_call_handler").unwrap()
    else {
        panic!("expected create language");
    };
    assert_eq!(create.language_name, "alt_lang");
    assert_eq!(create.handler_name, "plpgsql_call_handler");

    let Statement::AlterLanguage(alter) =
        parse_statement("alter language alt_lang owner to app_owner").unwrap()
    else {
        panic!("expected alter language");
    };
    assert_eq!(alter.language_name, "alt_lang");
    assert_eq!(
        alter.action,
        AlterLanguageAction::OwnerTo {
            new_owner: "app_owner".into()
        }
    );

    let Statement::AlterLanguage(rename) =
        parse_statement("alter language alt_lang rename to alt_lang2").unwrap()
    else {
        panic!("expected alter language rename");
    };
    assert_eq!(
        rename.action,
        AlterLanguageAction::Rename {
            new_name: "alt_lang2".into()
        }
    );

    let Statement::DropLanguage(drop_stmt) =
        parse_statement("drop language if exists alt_lang cascade").unwrap()
    else {
        panic!("expected drop language");
    };
    assert!(drop_stmt.if_exists);
    assert_eq!(drop_stmt.language_name, "alt_lang");
    assert!(drop_stmt.cascade);
}

#[test]
fn parse_foreign_data_wrapper_rejects_duplicate_clauses() {
    let err = parse_statement(
        "create foreign data wrapper foo handler pg_rust_test_fdw_handler handler invalid_fdw_handler",
    )
    .expect_err("duplicate handler should fail");
    assert!(matches!(
        err,
        ParseError::FeatureNotSupportedMessage(message)
            if message == "conflicting or redundant options"
    ));

    let err = parse_statement(
        "alter foreign data wrapper foo validator postgresql_fdw_validator no validator",
    )
    .expect_err("duplicate validator should fail");
    assert!(matches!(
        err,
        ParseError::FeatureNotSupportedMessage(message)
            if message == "conflicting or redundant options"
    ));
}

#[test]
fn parse_create_and_drop_type_statements() {
    let Statement::CreateType(CreateTypeStatement::Composite(CreateCompositeTypeStatement {
        schema_name,
        type_name,
        attributes,
    })) = parse_statement("create type complex as (r float8, i float8)").unwrap()
    else {
        panic!("expected create type");
    };
    assert_eq!(schema_name, None);
    assert_eq!(type_name, "complex");
    assert_eq!(
        attributes,
        vec![
            CompositeTypeAttributeDef {
                name: "r".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float8)),
            },
            CompositeTypeAttributeDef {
                name: "i".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float8)),
            },
        ]
    );

    let Statement::DropType(DropTypeStatement {
        if_exists,
        type_names,
        cascade,
    }) = parse_statement("drop type complex").unwrap()
    else {
        panic!("expected drop type");
    };
    assert!(!if_exists);
    assert_eq!(type_names, vec!["complex"]);
    assert!(!cascade);

    let Statement::DropType(DropTypeStatement {
        if_exists,
        type_names,
        cascade,
    }) = parse_statement("drop type if exists complex restrict").unwrap()
    else {
        panic!("expected drop type restrict");
    };
    assert!(if_exists);
    assert_eq!(type_names, vec!["complex"]);
    assert!(!cascade);

    let Statement::DropType(DropTypeStatement {
        if_exists,
        type_names,
        cascade,
    }) = parse_statement("drop type complex cascade").unwrap()
    else {
        panic!("expected drop type cascade");
    };
    assert!(!if_exists);
    assert_eq!(type_names, vec!["complex"]);
    assert!(cascade);

    let Statement::AlterTypeOwner(alter_stmt) =
        parse_statement("alter type complex owner to app_owner").unwrap()
    else {
        panic!("expected alter type owner");
    };
    assert_eq!(alter_stmt.type_name, "complex");
    assert_eq!(alter_stmt.new_owner, "app_owner");

    let Statement::RevokeObject(revoke_stmt) =
        parse_statement("revoke usage on type complex from public").unwrap()
    else {
        panic!("expected revoke type usage");
    };
    assert_eq!(revoke_stmt.privilege, GrantObjectPrivilege::UsageOnType);
    assert_eq!(revoke_stmt.object_names, vec!["complex"]);
    assert_eq!(revoke_stmt.grantee_names, vec!["public"]);
    assert!(!revoke_stmt.cascade);
}

#[test]
fn parse_create_type_supports_base_enum_and_range_forms() {
    let Statement::CreateType(CreateTypeStatement::Shell(CreateShellTypeStatement {
        schema_name,
        type_name,
    })) = parse_statement("create type myint").unwrap()
    else {
        panic!("expected shell create type");
    };
    assert_eq!(schema_name, None);
    assert_eq!(type_name, "myint");
    let Statement::CreateType(CreateTypeStatement::Base(CreateBaseTypeStatement {
        schema_name,
        type_name,
        options,
    })) = parse_statement(
        "create type myint (input = myintin, output = myintout, internallength = 4, passedbyvalue, default = 42)",
    )
    .unwrap()
    else {
        panic!("expected base create type");
    };
    assert_eq!(schema_name, None);
    assert_eq!(type_name, "myint");
    assert_eq!(
        options,
        vec![
            CreateBaseTypeOption {
                name: "input".into(),
                value: Some("myintin".into()),
            },
            CreateBaseTypeOption {
                name: "output".into(),
                value: Some("myintout".into()),
            },
            CreateBaseTypeOption {
                name: "internallength".into(),
                value: Some("4".into()),
            },
            CreateBaseTypeOption {
                name: "passedbyvalue".into(),
                value: None,
            },
            CreateBaseTypeOption {
                name: "default".into(),
                value: Some("42".into()),
            },
        ]
    );
    match parse_statement("create type mood as enum ('sad', 'ok')").unwrap() {
        Statement::CreateType(CreateTypeStatement::Enum(stmt)) => {
            assert_eq!(stmt.schema_name, None);
            assert_eq!(stmt.type_name, "mood");
            assert_eq!(stmt.labels, vec!["sad", "ok"]);
        }
        other => panic!("expected enum create type, got {other:?}"),
    }
    match parse_statement(
        "create type intr as range (subtype = int4, subtype_opclass = int4_ops, subtype_diff = int4mi, collation = \"C\")",
    )
    .unwrap()
    {
        Statement::CreateType(CreateTypeStatement::Range(stmt)) => {
            assert_eq!(stmt.schema_name, None);
            assert_eq!(stmt.type_name, "intr");
            assert_eq!(
                stmt.subtype,
                RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4))
            );
            assert_eq!(stmt.subtype_opclass.as_deref(), Some("int4_ops"));
            assert_eq!(stmt.subtype_diff.as_deref(), Some("int4mi"));
            assert_eq!(stmt.collation.as_deref(), Some("C"));
        }
        other => panic!("expected range create type, got {other:?}"),
    }
}

#[test]
fn parse_create_type_rejects_extended_attribute_syntax() {
    for sql in [
        "create type complex as (r float8 default 0)",
        "create type complex as (r float8 constraint c check (r > 0))",
        "create type complex as (label text collate \"C\")",
    ] {
        assert!(matches!(
            parse_statement(sql),
            Err(ParseError::FeatureNotSupported(feature))
                if feature == "CREATE TYPE attributes only support name and type"
        ));
    }
}

#[test]
fn parse_create_domain_preserves_array_base_type() {
    let Statement::CreateDomain(create) =
        parse_statement("create domain domainchar4arr varchar(4)[2][3]").unwrap()
    else {
        panic!("expected create domain");
    };
    assert_eq!(
        create.ty,
        RawTypeName::Builtin(SqlType::array_of(SqlType::array_of(
            SqlType::with_char_len(SqlTypeKind::Varchar, 4,)
        )))
    );
}

#[test]
fn lower_create_table_resolves_named_domain_types() {
    let stmt = parse_statement("create table items (id dom_int)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let catalog = TypeOnlyCatalog {
        types: vec![PgTypeRow {
            oid: 50_000,
            typname: "dom_int".into(),
            typnamespace: PUBLIC_NAMESPACE_OID,
            typowner: BOOTSTRAP_SUPERUSER_OID,
            typacl: None,
            typlen: 4,
            typbyval: true,
            typtype: 'd',
            typisdefined: true,
            typalign: AttributeAlign::Int,
            typstorage: AttributeStorage::Plain,
            typrelid: 0,
            typsubscript: 0,
            typelem: 0,
            typarray: 0,
            typinput: 0,
            typoutput: 0,
            typreceive: 0,
            typsend: 0,
            typmodin: 0,
            typmodout: 0,
            typdelim: ',',
            typanalyze: 0,
            typbasetype: crate::include::catalog::INT4_TYPE_OID,
            typcollation: 0,
            sql_type: SqlType::new(SqlTypeKind::Int4),
        }],
    };
    let lowered = lower_create_table(&ct, &catalog).unwrap();
    assert_eq!(
        lowered.relation_desc.columns[0].sql_type,
        SqlType::new(SqlTypeKind::Int4)
    );
}

#[test]
fn parse_create_table_with_array_types() {
    match parse_statement("create table widgets (a varchar[], b varchar(5)[], c int4[], d text[])")
        .unwrap()
    {
        Statement::CreateTable(ct) => {
            let columns = ct.columns().collect::<Vec<_>>();
            assert_eq!(
                columns[0].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
            );
            assert_eq!(
                columns[1].ty,
                SqlType::array_of(SqlType::with_char_len(SqlTypeKind::Varchar, 5))
            );
            assert_eq!(
                columns[2].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
            );
            assert_eq!(
                columns[3].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Text))
            );
        }
        other => panic!("expected create table, got {other:?}"),
    }
}

#[test]
fn parse_macaddr_type_names_and_array_aliases() {
    match parse_statement(
        "create table mac_widgets (m macaddr, m8 macaddr8, ma _macaddr, m8a _macaddr8)",
    )
    .unwrap()
    {
        Statement::CreateTable(ct) => {
            let columns = ct.columns().collect::<Vec<_>>();
            assert_eq!(columns[0].ty, SqlType::new(SqlTypeKind::MacAddr));
            assert_eq!(columns[1].ty, SqlType::new(SqlTypeKind::MacAddr8));
            assert_eq!(
                columns[2].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::MacAddr))
            );
            assert_eq!(
                columns[3].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::MacAddr8))
            );
        }
        other => panic!("expected create table, got {other:?}"),
    }

    let stmt =
        parse_select("select macaddr '08:00:2b:01:02:03', '08002b0102030405'::macaddr8").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::Cast(_, ty) if *ty == SqlType::new(SqlTypeKind::MacAddr)
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::Cast(_, ty) if *ty == SqlType::new(SqlTypeKind::MacAddr8)
    ));
}

#[test]
fn parse_multidimensional_array_cast_type() {
    let stmt = parse_select("select '{{1,2},{3,4}}'::int4[][]").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::Cast(_, ty)
            if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
    ));
}

#[test]
fn parse_create_table_with_multidimensional_array_types() {
    match parse_statement("create table widgets (a int4[][][], b text[][])").unwrap() {
        Statement::CreateTable(stmt) => {
            let columns = stmt.columns().collect::<Vec<_>>();
            assert_eq!(
                columns[0].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
            );
            assert_eq!(
                columns[1].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Text))
            );
        }
        other => panic!("expected create table, got {other:?}"),
    }
}

#[test]
fn parse_create_temp_table_with_fixed_length_array_type_syntax() {
    match parse_statement(
        "create temp table arrtest2 (i integer ARRAY[4], f float8[], n numeric[], t text[], d timestamp[])",
    )
    .unwrap()
    {
        Statement::CreateTable(stmt) => {
            let columns = stmt.columns().collect::<Vec<_>>();
            assert_eq!(
                columns[0].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
            );
            assert_eq!(
                columns[1].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Float8))
            );
            assert_eq!(
                columns[2].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Numeric))
            );
            assert_eq!(
                columns[3].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Text))
            );
            assert_eq!(
                columns[4].ty,
                SqlType::array_of(SqlType::new(SqlTypeKind::Timestamp))
            );
        }
        other => panic!("expected create table, got {other:?}"),
    }
}

#[test]
fn parse_array_and_unnest_expressions() {
    let stmt =
        parse_select("select * from unnest(ARRAY['a', 'b']::varchar[], ARRAY[1, 2])").unwrap();
    assert!(
        matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args, .. }) if name == "unnest" && args.len() == 2)
    );

    let stmt = parse_select("select 1 = any (ARRAY[1, 2])").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::QuantifiedArray { is_all: false, .. }
    ));

    let stmt = parse_select("select 1 < all (ARRAY[2, 3])").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::QuantifiedArray { is_all: true, .. }
    ));

    let stmt = parse_select("select ARRAY['a'] && ARRAY['b']").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::ArrayOverlap(_, _)));

    let stmt = parse_select("select ARRAY['a'] @> ARRAY['b'], ARRAY['a'] <@ ARRAY['b']").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::ArrayContains(_, _)));
    assert!(matches!(
        stmt.targets[1].expr,
        SqlExpr::ArrayContained(_, _)
    ));

    let stmt = parse_select("select '(0,0),(1,1)'::box && '(2,2),(3,3)'::box").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::BinaryOperator { ref op, .. } if op == "&&"
    ));
}

#[test]
fn parse_nested_array_constructor_shorthand() {
    let stmt =
        parse_select("select ARRAY[[[111,112],[121,122]],[[211,212],[221,222]]] as f").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::ArrayLiteral(outer) => {
            assert_eq!(outer.len(), 2);
            assert!(matches!(outer[0], SqlExpr::ArrayLiteral(_)));
            assert!(matches!(outer[1], SqlExpr::ArrayLiteral(_)));
        }
        other => panic!("expected nested array literal, got {other:?}"),
    }
}

#[test]
fn build_plan_rejects_untyped_empty_array() {
    let stmt = parse_select("select ARRAY[]").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, hint: Some(hint), sqlstate, .. })
            if message == "cannot determine type of empty array"
                && hint == "Explicitly cast to the desired type, for example ARRAY[]::integer[]."
                && sqlstate == "42P18"
    ));
}

#[test]
fn build_plan_accepts_typed_empty_array() {
    let stmt = parse_select("select ARRAY[]::varchar[]").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert_eq!(
                targets[0].sql_type,
                SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
            );
        }
        other => panic!("expected projection, got {other:?}"),
    }
}

#[test]
fn build_plan_reports_postgres_any_all_array_errors() {
    let stmt = parse_select("select 33 * any ('{1,2,3}')").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message == "op ANY/ALL (array) requires operator to yield boolean"
                && sqlstate == "42809"
    ));

    let stmt = parse_select("select 33 * any (44)").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message == "op ANY/ALL (array) requires array on right side"
                && sqlstate == "42809"
    ));
}

#[test]
fn parse_aggregate_select() {
    let stmt = parse_select("select count(*) from people").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: false,
            ..
        } if name == "count" && args.is_star()
    ));
    assert_eq!(stmt.targets[0].output_name, "count");
}

#[test]
fn parse_string_agg_select() {
    let stmt = parse_select("select string_agg(note, ',') from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: false,
            ..
        } if name == "string_agg" && args.args().len() == 2
    ));
    assert_eq!(stmt.targets[0].output_name, "string_agg");
}

#[test]
fn parse_jsonb_agg_with_local_order_by() {
    let stmt = parse_select("select jsonb_agg(id order by note desc, id) from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            ..
        } if name == "jsonb_agg"
            && args.args().len() == 1
            && order_by.len() == 2
            && order_by[0].descending
            && matches!(order_by[0].expr, SqlExpr::Column(ref name) if name == "note")
            && matches!(order_by[1].expr, SqlExpr::Column(ref name) if name == "id")
    ));
}

#[test]
fn parse_jsonb_agg_with_local_order_by_using_operator() {
    let stmt = parse_select("select jsonb_agg(id order by note using ~>~) from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            ..
        } if name == "jsonb_agg"
            && args.args().len() == 1
            && order_by.len() == 1
            && order_by[0].descending
            && order_by[0].using_operator.as_deref() == Some("~>~")
            && matches!(order_by[0].expr, SqlExpr::Column(ref name) if name == "note")
    ));
}

#[test]
fn parse_hypothetical_within_group_call() {
    let stmt =
        parse_select("select rank(3) within group (order by x) from (values (1),(2),(3)) v(x)")
            .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group: Some(within_group),
            distinct: false,
            func_variadic: false,
            filter: None,
            over: None,
            ..
        } if name == "rank"
            && args.args().len() == 1
            && order_by.is_empty()
            && matches!(args.args()[0].value, SqlExpr::IntegerLiteral(ref value) if value == "3")
            && within_group.len() == 1
            && matches!(within_group[0].expr, SqlExpr::Column(ref name) if name == "x")
    ));
}

#[test]
fn parse_select_order_by_using_operator() {
    let stmt = parse_select("select note from people order by note using > nulls last").unwrap();
    assert_eq!(stmt.order_by.len(), 1);
    assert!(stmt.order_by[0].descending);
    assert_eq!(stmt.order_by[0].nulls_first, Some(false));
    assert_eq!(stmt.order_by[0].using_operator.as_deref(), Some(">"));
}

#[test]
fn parse_within_group_rejects_invalid_clause_combinations() {
    let cases = [
        (
            "select rank(distinct 3) within group (order by x) from (values (1)) v(x)",
            "cannot use DISTINCT with WITHIN GROUP",
            "42601",
        ),
        (
            "select rank(variadic array[3]) within group (order by x) from (values (1)) v(x)",
            "cannot use VARIADIC with WITHIN GROUP",
            "42601",
        ),
        (
            "select rank(3 order by y) within group (order by x) from (values (1,2)) v(x,y)",
            "cannot use multiple ORDER BY clauses with WITHIN GROUP",
            "42601",
        ),
        (
            "select rank(3) within group (order by x) over () from (values (1)) v(x)",
            "OVER is not supported for ordered-set aggregate rank",
            "0A000",
        ),
    ];
    for (sql, expected, sqlstate) in cases {
        assert!(matches!(
            parse_select(sql),
            Err(ParseError::DetailedError { message, sqlstate: actual_sqlstate, .. })
                if message == expected && actual_sqlstate == sqlstate
        ));
    }
}

#[test]
fn parse_aggregate_filter_clause() {
    let stmt = parse_select("select count(*) filter (where note is not null) from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: false,
            filter: Some(filter),
            ..
        } if name == "count"
            && args.is_star()
            && matches!(
                filter.as_ref(),
                SqlExpr::IsNotNull(inner) if matches!(inner.as_ref(), SqlExpr::Column(name) if name == "note")
            )
    ));
}

#[test]
fn parse_range_intersect_agg_select() {
    let stmt = parse_select("select range_intersect_agg(id::int4range) from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: false,
            ..
        } if name == "range_intersect_agg" && args.args().len() == 1
    ));
    assert_eq!(stmt.targets[0].output_name, "range_intersect_agg");
}

#[test]
fn parse_any_value_select() {
    let stmt = parse_select("select any_value(id) from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: false,
            ..
        } if name == "any_value" && args.args().len() == 1
    ));
    assert_eq!(stmt.targets[0].output_name, "any_value");
}

#[test]
fn parse_variadic_aggregate_call_marks_call_level_flag() {
    std::thread::Builder::new()
        .name("parse_variadic_aggregate_call_marks_call_level_flag".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let stmt = parse_select("select count(VARIADIC ARRAY[1, 2]) from people").unwrap();
            assert!(matches!(
                &stmt.targets[0].expr,
                SqlExpr::FuncCall {
                    name,
                    args,
                    func_variadic: true,
                    ..
                } if name == "count" && args.args().len() == 1
            ));
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parse_group_by_and_having() {
    let stmt = parse_select("select name, count(*) from people group by name having count(*) > 1")
        .unwrap();
    assert_eq!(stmt.group_by.len(), 1);
    assert!(matches!(stmt.group_by[0], SqlExpr::Column(ref name) if name == "name"));
    assert!(stmt.having.is_some());
}

#[test]
fn parse_window_calls_capture_over_clause() {
    let stmt = parse_select(
        "select row_number() over (), sum(id) over (partition by name order by id) from people",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            over: Some(RawWindowSpec {
                name: window_name,
                partition_by,
                order_by,
                frame,
            }),
            ..
        } if name == "row_number"
            && window_name.is_none()
            && partition_by.is_empty()
            && order_by.is_empty()
            && frame.is_none()
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::FuncCall {
            name,
            over: Some(RawWindowSpec {
                name: window_name,
                partition_by,
                order_by,
                frame,
            }),
            ..
        } if name == "sum"
            && window_name.is_none()
            && partition_by.len() == 1
            && order_by.len() == 1
            && frame.is_none()
    ));
}

#[test]
fn parse_named_window_clause_and_reference() {
    let stmt =
        parse_select("select row_number() over w from people window w as (order by id)").unwrap();
    assert_eq!(stmt.window_clauses.len(), 1);
    assert_eq!(stmt.window_clauses[0].name, "w");
    assert!(stmt.window_clauses[0].spec.partition_by.is_empty());
    assert_eq!(stmt.window_clauses[0].spec.order_by.len(), 1);
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            over: Some(RawWindowSpec {
                name: Some(window_name),
                partition_by,
                order_by,
                frame,
            }),
            ..
        } if name == "row_number"
            && window_name == "w"
            && partition_by.is_empty()
            && order_by.is_empty()
            && frame.is_none()
    ));
}

#[test]
fn parse_window_frame_clause_and_inherited_reference() {
    let stmt = parse_select(
        "select sum(id) over (w rows between 1 preceding and current row) from people window w as (partition by name order by id)",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            over: Some(RawWindowSpec {
                name: Some(window_name),
                partition_by,
                order_by,
                frame: Some(frame),
            }),
            ..
        } if name == "sum"
            && window_name == "w"
            && partition_by.is_empty()
            && order_by.is_empty()
            && frame.mode == WindowFrameMode::Rows
            && matches!(frame.start_bound, RawWindowFrameBound::OffsetPreceding(_))
            && matches!(frame.end_bound, RawWindowFrameBound::CurrentRow)
            && frame.exclusion == WindowFrameExclusion::NoOthers
    ));
}

#[test]
fn parse_window_frame_exclusion_variants() {
    for (sql, expected) in [
        (
            "select sum(id) over (order by id rows unbounded preceding exclude current row) from people",
            WindowFrameExclusion::CurrentRow,
        ),
        (
            "select sum(id) over (order by id rows unbounded preceding exclude group) from people",
            WindowFrameExclusion::Group,
        ),
        (
            "select sum(id) over (order by id rows unbounded preceding exclude ties) from people",
            WindowFrameExclusion::Ties,
        ),
        (
            "select sum(id) over (order by id rows unbounded preceding exclude no others) from people",
            WindowFrameExclusion::NoOthers,
        ),
    ] {
        let stmt = parse_select(sql).unwrap();
        assert!(matches!(
            &stmt.targets[0].expr,
            SqlExpr::FuncCall {
                over: Some(RawWindowSpec {
                    frame: Some(frame),
                    ..
                }),
                ..
            } if frame.exclusion == expected
        ));
    }
}

#[test]
fn parse_select_target_with_bare_alias() {
    let stmt = parse_select("select id user_id from people").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert_eq!(stmt.targets[0].output_name, "user_id");
    assert!(matches!(stmt.targets[0].expr, SqlExpr::Column(ref name) if name == "id"));
}

#[test]
fn build_plan_with_aggregate() {
    let stmt = parse_select("select name, count(*) from people group by name").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 2);
            assert_eq!(targets[0].name, "name");
            assert_eq!(targets[1].name, "count");
            assert!(matches!(*input, Plan::Aggregate { .. }));
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_rejects_explicit_empty_count_window_call() {
    let stmt = parse_select("select count() over () from people").unwrap();
    let err = build_plan(&stmt, &catalog()).unwrap_err();
    assert!(matches!(
        err,
        ParseError::DetailedError { message, sqlstate, .. }
            if message == "count(*) must be used to call a parameterless aggregate function"
                && sqlstate == "42809"
    ));
}

#[test]
fn build_plan_rejects_over_for_non_window_function() {
    let stmt = parse_select("select generate_series(1, 5) over ()").unwrap();
    let err = build_plan(&stmt, &catalog()).unwrap_err();
    assert!(matches!(
        err,
        ParseError::DetailedError { message, sqlstate, .. }
            if message
                == "OVER specified, but generate_series is not a window function nor an aggregate function"
                && sqlstate == "42809"
    ));
}

#[test]
fn build_plan_rejects_rows_frame_offsets_with_variables() {
    let stmt = parse_select(
        "select sum(id) over (rows between x preceding and current row) \
         from people, (values (1)) v(x)",
    )
    .unwrap();
    let err = build_plan(&stmt, &catalog()).unwrap_err();
    assert!(matches!(
        err,
        ParseError::DetailedError { message, sqlstate, .. }
            if message == "argument of ROWS must not contain variables"
                && sqlstate == "42P10"
    ));
}

#[test]
fn analyze_values_common_type_preserves_unknown_literal_targets() {
    let stmt = parse_select("select x from (values (1::numeric), ('NaN'), (2)) v(x)").unwrap();
    let (_, scope) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    assert_eq!(
        scope.desc.columns[0].sql_type,
        SqlType::new(SqlTypeKind::Numeric)
    );

    let stmt = parse_select("select x from (values (interval '1 day'), ('2 days')) v(x)").unwrap();
    let (_, scope) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
    assert_eq!(
        scope.desc.columns[0].sql_type,
        SqlType::new(SqlTypeKind::Interval)
    );
}

#[test]
fn build_plan_with_group_by_order_by_wraps_aggregate_then_sort() {
    let stmt =
        parse_select("select name, count(*) from people group by name order by name").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 2);
            match *input {
                Plan::OrderBy { input, items, .. } => {
                    assert_eq!(items.len(), 1);
                    assert!(is_outer_user_var(&items[0].expr, 0));
                    assert!(matches!(*input, Plan::Aggregate { .. }));
                }
                other => panic!("expected order by above aggregate, got {:?}", other),
            }
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn grouped_join_using_projects_scanjoin_target_before_aggregate() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());

    let stmt = parse_select(
        "select id, count(owner_id) from people left join pets using (id) group by id order by id",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog).unwrap();
    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 2);
            match *input {
                Plan::OrderBy { input, items, .. } => {
                    assert_eq!(items.len(), 1);
                    assert!(is_outer_user_var(&items[0].expr, 0));
                    match *input {
                        Plan::Aggregate {
                            input, group_by, ..
                        } => {
                            assert_eq!(group_by.len(), 1);
                            assert!(is_outer_user_var(&group_by[0], 0));
                            match *input {
                                Plan::Projection { targets, .. } => {
                                    assert_eq!(targets.len(), 2);
                                }
                                other => panic!(
                                    "expected scan/join projection below aggregate, got {other:?}"
                                ),
                            }
                        }
                        other => panic!("expected aggregate below order by, got {other:?}"),
                    }
                }
                other => panic!("expected order by above aggregate, got {other:?}"),
            }
        }
        other => panic!("expected projection, got {other:?}"),
    }
}

#[test]
fn analyze_grouped_query_keeps_semantic_group_refs() {
    let stmt = parse_select(
        "select name, count(*) from people group by name having name is not null order by name",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    let name_var = Expr::Var(Var {
        varno: 1,
        varattno: 2,
        varlevelsup: 0,
        vartype: SqlType::new(SqlTypeKind::Text),
    });

    assert_eq!(query.group_by, vec![name_var.clone()]);
    assert_eq!(query.target_list[0].expr, name_var);
    assert!(matches!(query.target_list[1].expr, Expr::Aggref(_)));
    assert_eq!(
        query.having_qual,
        Some(Expr::IsNotNull(Box::new(Expr::Var(Var {
            varno: 1,
            varattno: 2,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Text),
        }))))
    );
    assert_eq!(query.sort_clause.len(), 1);
    assert_eq!(
        query.sort_clause[0].expr,
        Expr::Var(Var {
            varno: 1,
            varattno: 2,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Text),
        })
    );
}

#[test]
fn build_plan_allows_primary_key_functional_dependency_in_grouped_output() {
    let stmt = parse_select("select id, name, note from people group by id").unwrap();
    let plan = build_plan(&stmt, &catalog_with_people_primary_key()).unwrap();

    match plan {
        Plan::Projection { input, targets, .. } => {
            assert_eq!(targets.len(), 3);
            match *input {
                Plan::Aggregate {
                    group_by,
                    passthrough_exprs,
                    ..
                } => {
                    assert_eq!(group_by.len(), 1);
                    assert!(is_outer_user_var(&group_by[0], 0));
                    assert_eq!(passthrough_exprs.len(), 2);
                    assert!(is_outer_user_var(&passthrough_exprs[0], 1));
                    assert!(is_outer_user_var(&passthrough_exprs[1], 2));
                }
                other => panic!("expected aggregate below projection, got {other:?}"),
            }
        }
        other => panic!("expected projection, got {other:?}"),
    }
}

#[test]
fn build_plan_does_not_use_unique_constraint_for_grouped_functional_dependency() {
    let stmt = parse_select("select id from people group by name").unwrap();

    assert!(matches!(
        build_plan(&stmt, &catalog_with_people_name_unique_constraint()),
        Err(ParseError::UngroupedColumn { token, .. }) if token == "id"
    ));
}

#[test]
fn build_plan_requires_all_composite_primary_key_columns_for_grouped_functional_dependency() {
    let partial = parse_select("select note from memberships group by id").unwrap();
    assert!(matches!(
        build_plan(&partial, &catalog_with_memberships_composite_primary_key()),
        Err(ParseError::UngroupedColumn { token, .. }) if token == "note"
    ));

    let full = parse_select("select note from memberships group by id, tag").unwrap();
    let plan = build_plan(&full, &catalog_with_memberships_composite_primary_key()).unwrap();
    match plan {
        Plan::Projection { input, .. } => match *input {
            Plan::Aggregate {
                group_by,
                passthrough_exprs,
                ..
            } => {
                assert_eq!(group_by.len(), 2);
                assert_eq!(passthrough_exprs.len(), 1);
                assert!(is_outer_user_var(&passthrough_exprs[0], 2));
            }
            other => panic!("expected aggregate below projection, got {other:?}"),
        },
        other => panic!("expected projection, got {other:?}"),
    }
}

#[test]
fn build_plan_allows_joined_table_column_functionally_dependent_on_grouped_primary_key() {
    let mut catalog = catalog_with_people_primary_key();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select p.id, p.name, count(q.id)
         from people p join pets q on q.owner_id = p.id
         group by p.id",
    )
    .unwrap();

    build_plan(&stmt, &catalog).unwrap();
}

#[test]
fn build_plan_allows_using_merged_primary_key_for_grouped_functional_dependency() {
    let stmt = parse_select(
        "select product_id, p.name, count(s.units)
         from products p left join sales s using (product_id)
         group by product_id",
    )
    .unwrap();

    build_plan(&stmt, &catalog_with_products_primary_key_and_sales()).unwrap();
}

#[test]
fn parse_prepare_and_execute_statements() {
    let stmt = parse_statement("prepare foo as select id from people group by id").unwrap();
    match stmt {
        Statement::Prepare(PrepareStatement { name, query, .. }) => {
            assert_eq!(name, "foo");
            assert_eq!(query.group_by.len(), 1);
        }
        other => panic!("expected PREPARE statement, got {other:?}"),
    }

    let stmt = parse_statement("execute foo").unwrap();
    assert_eq!(
        stmt,
        Statement::Execute(ExecuteStatement { name: "foo".into() })
    );
}

#[test]
fn analyze_grouped_query_matches_bound_equivalent_group_exprs() {
    for sql in [
        "select id % 2, count(name) from people group by people.id % 2 order by people.id % 2",
        "select lower(people.name), count(name) from people group by lower(name) order by lower(name)",
    ] {
        let stmt = parse_select(sql).unwrap();
        let (query, _) =
            analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

        assert_eq!(query.group_by.len(), 1, "{sql}");
        assert_eq!(query.target_list[0].expr, query.group_by[0], "{sql}");
        assert_eq!(query.sort_clause[0].expr, query.group_by[0], "{sql}");
    }
}

#[test]
fn analyze_group_by_resolves_select_alias_when_no_input_column_matches() {
    let stmt = parse_select("select id as two, count(*) from people group by two").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query.group_by,
        vec![Expr::Var(Var {
            varno: 1,
            varattno: 1,
            varlevelsup: 0,
            vartype: SqlType::new(SqlTypeKind::Int4),
        })]
    );
}

#[test]
fn analyze_group_by_prefers_input_column_over_select_alias() {
    let stmt = parse_select("select id as name from people group by name, id").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query.group_by,
        vec![
            Expr::Var(Var {
                varno: 1,
                varattno: 2,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Text),
            }),
            Expr::Var(Var {
                varno: 1,
                varattno: 1,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Int4),
            }),
        ]
    );
}

#[test]
fn analyze_array_subquery_can_order_by_aggregate_alias() {
    let stmt = parse_select(
        "select array(
            select sum(x + y) s
            from generate_series(1, 3) y
            group by y
            order by s
        )
        from generate_series(1, 3) x",
    )
    .unwrap();

    match &stmt.targets[0].expr {
        SqlExpr::ArraySubquery(subquery) => {
            assert_eq!(subquery.targets.len(), 1);
            assert_eq!(subquery.targets[0].output_name, "s");
            assert_eq!(subquery.order_by.len(), 1);
            assert!(matches!(
                &subquery.order_by[0].expr,
                SqlExpr::Column(name) if name == "s"
            ));
        }
        other => panic!("expected array subquery target, got {other:?}"),
    }

    analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
}

#[test]
fn analyze_grouped_outer_expression_inside_sublink() {
    let stmt = parse_select(
        "select id + 1 as g,
                exists(
                    select 1
                    from generate_series(1, 3) as y(val)
                    where val = (id + 1)
                )
         from people
         group by id + 1",
    )
    .unwrap();

    analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();
}

#[test]
fn build_plan_with_window_function_uses_windowagg() {
    let stmt = parse_select("select row_number() over (order by id) from people").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, .. } => match *input {
            Plan::WindowAgg { input, clause, .. } => {
                assert!(clause.spec.partition_by.is_empty());
                assert_eq!(clause.spec.order_by.len(), 1);
                assert!(matches!(*input, Plan::OrderBy { .. }));
            }
            other => panic!("expected window agg below projection, got {other:?}"),
        },
        other => panic!("expected projection, got {other:?}"),
    }
}

#[test]
fn build_plan_with_named_window_clause_uses_windowagg() {
    let stmt =
        parse_select("select row_number() over w from people window w as (order by id)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, .. } => match *input {
            Plan::WindowAgg { input, clause, .. } => {
                assert!(clause.spec.partition_by.is_empty());
                assert_eq!(clause.spec.order_by.len(), 1);
                assert!(matches!(*input, Plan::OrderBy { .. }));
            }
            other => panic!("expected window agg below projection, got {other:?}"),
        },
        other => panic!("expected projection, got {other:?}"),
    }
}

#[test]
fn ungrouped_column_rejected_at_plan_time() {
    let stmt = parse_select("select name, count(*) from people").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::UngroupedColumn { token, .. }) if token == "name"
    ));
}

#[test]
fn aggregate_in_where_rejected() {
    let stmt = parse_select("select name from people where count(*) > 1").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message == "aggregate functions are not allowed in WHERE" && sqlstate == "42803"
    ));
}

#[test]
fn nested_aggregate_calls_are_rejected() {
    let stmt = parse_select("select sum(max(id)) from people").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message == "aggregate function calls cannot be nested" && sqlstate == "42803"
    ));
}

#[test]
fn aggregate_rejects_nested_subquery_reference_to_local_cte() {
    let stmt =
        parse_select("select (with cte1(x) as (values (1)) select count((select x from cte1)))")
            .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::OuterLevelAggregateNestedCte(name)) if name == "cte1"
    ));
}

#[test]
fn recursive_cte_allows_self_reference_inside_intermediate_setop_with() {
    let stmt = parse_select(
        "with recursive outermost(x) as (
            select 1
            union (with innermost as (select 2)
                   select * from outermost
                   union select * from innermost)
        )
        select * from outermost order by 1",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn recursive_cte_allows_non_recursive_union_ctes_inside_recursive_term() {
    let stmt = parse_select(
        "with recursive outermost(x) as (
         select 1
         union (with innermost1 as (
          select 2
          union (with innermost2 as (
           select 3
           union (with innermost3 as (
            select 4
            union (with innermost4 as (
             select 5
             union (with innermost5 as (
              select 6
              union (with innermost6 as
               (select 7)
               select * from innermost6))
              select * from innermost5))
             select * from innermost4))
            select * from innermost3))
           select * from innermost2))
          select * from outermost
          union select * from innermost1)
        )
        select * from outermost order by 1",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn recursive_cte_rejects_self_reference_inside_subquery_cte_of_recursive_term() {
    let stmt = parse_select(
        "with recursive outermost(x) as (
            with innermost as (select 2 from outermost)
              select * from innermost
              union select * from outermost
        )
        select * from outermost order by 1",
    )
    .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::InvalidRecursion(message))
            if message
                == "recursive reference to query \"outermost\" must not appear within a subquery"
    ));
}

#[test]
fn recursive_cte_allows_self_reference_inside_derived_table_of_recursive_term() {
    let stmt = parse_select(
        "with recursive loop(n) as (
            values (1)
            union all
            select n + 1
            from (select n from loop) sub
            where n < 3
        )
        select * from loop order by 1",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn recursive_cte_allows_lisp_interpreter_demo_shape() {
    let stmt = parse_select(
        r#"with recursive loop as (
select '{"stack":[{"type":"expr","env":{"+":"+"},"expr":["+",1,2]}]}'::jsonb as state
union all
select
  case
    when frame_type = 'expr' and op_string = '+'
    then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint + arg2::text::bigint)
    when frame_type = 'expr'
    then jsonb_build_object(
      'stack',
      jsonb_build_array(jsonb_build_object('type', 'eval_if', 'expr', expr, 'env', env)) || (stack - 0)
    )
    when frame_type = 'eval_if' and result::text::boolean
    then jsonb_build_object(
      'stack',
      jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', expr -> 1, 'env', env)) || (stack - 0)
    )
    else jsonb_build_object('stack', stack - 0, 'result', result)
  end
from (
  select
    state -> 'stack' -> 0 ->> 'type' as frame_type,
    state -> 'stack' -> 0 -> 'expr' as expr,
    state -> 'stack' -> 0 -> 'expr' ->> 0 as op_string,
    state -> 'stack' -> 0 -> 'expr' -> 1 as arg1,
    state -> 'stack' -> 0 -> 'expr' -> 2 as arg2,
    state -> 'stack' -> 0 -> 'env' as env,
    state -> 'result' as result,
    state -> 'stack' as stack
  from loop
) sub
)
select jsonb_pretty(state -> 'result')
from loop
where state -> 'result' is not null
limit 1"#,
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

fn assert_lisp_expr_frame_branch_plan(frame: &str, case_arms: &str) {
    let sql = format!(
        r#"with recursive frames as (
  select '{frame}'::jsonb as frame
)
select
  case
{case_arms}
  end
from (
  select
    frame -> 'expr' as expr,
    frame ->> 'expr' as expr_string,
    frame -> 'expr' ->> 0 as op_string,
    frame -> 'expr' -> 1 as arg1,
    frame -> 'expr' -> 2 as arg2,
    frame -> 'env' as env
  from frames
) sub"#
    );
    let stmt = parse_select(&sql).unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn recursive_cte_allows_lisp_expr_number_branch() {
    assert_lisp_expr_frame_branch_plan(
        r#"{"type":"expr","env":{"x":9},"expr":1}"#,
        r#"    when jsonb_typeof(expr) = 'number'
    then jsonb_build_object('result', expr)
    else jsonb_build_object('result', null)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_expr_string_lookup_branch() {
    assert_lisp_expr_frame_branch_plan(
        r#"{"type":"expr","env":{"x":9},"expr":"x"}"#,
        r#"    when jsonb_typeof(expr) = 'string'
    then jsonb_build_object('result', env -> expr_string)
    else jsonb_build_object('result', null)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_expr_if_branch() {
    assert_lisp_expr_frame_branch_plan(
        r#"{"type":"expr","env":{},"expr":["if",true,1,0]}"#,
        r#"    when op_string = 'if'
    then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_if', 'expr', expr, 'env', env)))
    else jsonb_build_object('result', null)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_expr_lambda_branch() {
    assert_lisp_expr_frame_branch_plan(
        r#"{"type":"expr","env":{},"expr":["lambda",["x"],["+", "x", 1]]}"#,
        r#"    when op_string = 'lambda'
    then jsonb_build_object('result', jsonb_build_object('args', arg1, 'body', arg2, 'env', env))
    else jsonb_build_object('result', null)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_expr_eval_args_fallback_branch() {
    assert_lisp_expr_frame_branch_plan(
        r#"{"type":"expr","env":{},"expr":["call",1,2]}"#,
        r#"    when op_string = '__never__'
    then jsonb_build_object('result', null)
    else jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', expr, 'done', '[]'::jsonb, 'env', env)))
"#,
    );
}

fn assert_lisp_eval_args_frame_branch_plan(frame: &str, result: &str, case_arms: &str) {
    let sql = format!(
        r#"with recursive frames as (
  select '{frame}'::jsonb as frame, {result}::jsonb as result
)
select
  case
{case_arms}
  end
from (
  select
    frame -> 'left' as args_left,
    frame -> 'done' as args_done,
    frame -> 'env' as env,
    result
  from frames
) sub"#
    );
    let stmt = parse_select(&sql).unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn recursive_cte_allows_lisp_eval_args_empty_left_branch() {
    assert_lisp_eval_args_frame_branch_plan(
        r#"{"type":"eval_args","left":[],"done":[1,2],"env":{}}"#,
        "null",
        r#"    when result is null and jsonb_array_length(args_left) = 0
    then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_call', 'expr', args_done, 'env', env)))
    else jsonb_build_object('result', null)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_eval_args_schedule_next_arg_branch() {
    assert_lisp_eval_args_frame_branch_plan(
        r#"{"type":"eval_args","left":[1,2],"done":[],"env":{}}"#,
        "null",
        r#"    when result is null
    then jsonb_build_object(
      'stack',
      jsonb_build_array(
        jsonb_build_object('type', 'expr', 'expr', args_left -> 0, 'env', env),
        jsonb_build_object('type', 'eval_args', 'left', args_left - 0, 'done', args_done, 'env', env)
      )
    )
    else jsonb_build_object('result', null)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_eval_args_append_result_branch() {
    assert_lisp_eval_args_frame_branch_plan(
        r#"{"type":"eval_args","left":[2],"done":[1],"env":{}}"#,
        "'9'",
        r#"    when result is null
    then jsonb_build_object('result', null)
    else jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', args_left, 'done', args_done || jsonb_build_array(result), 'env', env)))
"#,
    );
}

fn assert_lisp_eval_call_branch_plan(frames: &str, case_arms: &str) {
    let sql = format!(
        r#"with recursive frames as (
{frames}
)
select
  case
{case_arms}
  end
from (
  select
    expr -> 0 as op,
    expr ->> 0 as op_string,
    expr -> 1 as arg1,
    expr -> 2 as arg2
  from frames
) sub"#
    );
    let stmt = parse_select(&sql).unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn recursive_cte_allows_lisp_eval_call_arithmetic_branches() {
    assert_lisp_eval_call_branch_plan(
        r#"  select '["+",1,2]'::jsonb as expr
  union all select '["*",2,3]'::jsonb
  union all select '["-",3,1]'::jsonb
  union all select '["/",4,2]'::jsonb"#,
        r#"    when op_string = '+'
    then jsonb_build_object('result', arg1::text::bigint + arg2::text::bigint)
    when op_string = '*'
    then jsonb_build_object('result', arg1::text::bigint * arg2::text::bigint)
    when op_string = '-'
    then jsonb_build_object('result', arg1::text::bigint - arg2::text::bigint)
    else jsonb_build_object('result', arg1::text::bigint / arg2::text::bigint)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_eval_call_comparison_branches() {
    assert_lisp_eval_call_branch_plan(
        r#"  select '[">",3,2]'::jsonb as expr
  union all select '["<",2,3]'::jsonb
  union all select '["=",2,2]'::jsonb"#,
        r#"    when op_string = '>'
    then jsonb_build_object('result', arg1::text::bigint > arg2::text::bigint)
    when op_string = '<'
    then jsonb_build_object('result', arg1::text::bigint < arg2::text::bigint)
    else jsonb_build_object('result', arg1 = arg2)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_eval_call_list_branches() {
    assert_lisp_eval_call_branch_plan(
        r#"  select '["head",[1,2]]'::jsonb as expr
  union all select '["tail",[1,2]]'::jsonb
  union all select '["cons",1,[2,3]]'::jsonb
  union all select '["empty"]'::jsonb"#,
        r#"    when op_string = 'head'
    then jsonb_build_object('result', arg1 -> 0)
    when op_string = 'tail'
    then jsonb_build_object('result', arg1 - 0)
    when op_string = 'cons'
    then jsonb_build_object('result', jsonb_build_array(arg1) || arg2)
    else jsonb_build_object('result', '[]'::jsonb)
"#,
    );
}

#[test]
fn recursive_cte_allows_lisp_eval_call_user_function_branch() {
    let stmt = parse_select(
        r#"with recursive frames as (
  select '[{"args":["x","y"],"body":["+", "x", "y"],"env":{"z":0}},1,2]'::jsonb as expr
)
select
  jsonb_build_object(
      'stack',
      jsonb_build_array(
        jsonb_build_object(
          'type', 'expr',
          'expr', op -> 'body',
          'env', (op -> 'env') || jsonb_build_object(
            coalesce(op -> 'args' ->> 0, 'null'), arg1,
            coalesce(op -> 'args' ->> 1, 'null'), arg2
          )
        )
      )
    )
from (
  select
    expr -> 0 as op,
    expr -> 1 as arg1,
    expr -> 2 as arg2
  from frames
) sub"#,
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn window_function_rejected_in_where_group_by_and_having() {
    for sql in [
        "select name from people where row_number() over () > 1",
        "select name from people group by row_number() over ()",
        "select name, count(*) from people group by name having row_number() over () > 1",
    ] {
        let stmt = parse_select(sql).unwrap();
        assert!(matches!(
            build_plan(&stmt, &catalog()),
            Err(ParseError::WindowingError(_)) | Err(ParseError::FeatureNotSupported(_))
        ));
    }
}

#[test]
fn named_windows_parse_and_frames_are_rejected() {
    let stmt = parse_select("select row_number() over w from people window w as ()").unwrap();
    assert_eq!(stmt.window_clauses.len(), 1);
    assert_eq!(stmt.window_clauses[0].name, "w");

    let stmt = parse_select("select row_number() over missing from people").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::WindowingError(message)) if message == "window \"missing\" does not exist"
    ));
    let stmt =
        parse_select("select row_number() over w from people window w as (), w as ()").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::WindowingError(message)) if message == "window \"w\" is already defined"
    ));

    let stmt = parse_select(
        "select sum(id) over (w rows between current row and unbounded following) from people window w as (order by id rows between unbounded preceding and current row)",
    )
    .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::WindowingError(message))
            if message == "cannot copy window \"w\" because it has a frame clause"
    ));
    let stmt = parse_select(
        "select sum(id) over (groups between 1 preceding and 1 following) from people",
    )
    .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::WindowingError(message)) if message == "GROUPS mode requires an ORDER BY clause"
    ));
}

#[test]
fn parse_column_alias() {
    let stmt = parse_select("select count(*) as total from people").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert_eq!(stmt.targets[0].output_name, "total");
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: false,
            ..
        } if name == "count" && args.is_star()
    ));
}

#[test]
fn parse_mixed_aliases() {
    let stmt = parse_select("select name, count(*) as total from people group by name").unwrap();
    assert_eq!(stmt.targets.len(), 2);
    assert_eq!(stmt.targets[0].output_name, "name");
    assert_eq!(stmt.targets[1].output_name, "total");
}

#[test]
fn parse_count_distinct() {
    let stmt = parse_select("select count(distinct name) from people").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: true,
            ..
        } if name == "count" && args.args().len() == 1
    ));
}

#[test]
fn parse_select_distinct_on() {
    let stmt = parse_select("select distinct on (name, note) name from people order by name, note")
        .unwrap();
    assert!(stmt.distinct);
    assert_eq!(stmt.distinct_on.len(), 2);
    assert!(matches!(&stmt.distinct_on[0], SqlExpr::Column(name) if name == "name"));
    assert!(matches!(&stmt.distinct_on[1], SqlExpr::Column(name) if name == "note"));
}

#[test]
fn select_distinct_on_rejects_non_prefix_order_by() {
    let stmt =
        parse_select("select distinct on (name, note) name from people order by name, id, note")
            .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::FeatureNotSupportedMessage(message))
            if message == "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
    ));
}

#[test]
fn parse_generate_series() {
    let stmt = parse_select("select * from generate_series(1, 10)").unwrap();
    assert!(
        matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args, .. }) if name == "generate_series" && args.len() == 2)
    );
}

#[test]
fn parse_generate_series_with_step() {
    let stmt = parse_select("select * from generate_series(1, 10, 2)").unwrap();
    assert!(
        matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args, .. }) if name == "generate_series" && args.len() == 3)
    );
}

#[test]
fn parse_named_function_args_in_select() {
    let stmt = parse_select(
        "select jsonb_path_exists(target => '{}'::jsonb, path := '$', silent => true)",
    )
    .unwrap();
    let SqlExpr::FuncCall { name, args, .. } = &stmt.targets[0].expr else {
        panic!("expected function call");
    };
    assert_eq!(name, "jsonb_path_exists");
    assert_eq!(args.args().len(), 3);
    assert_eq!(args.args()[0].name.as_deref(), Some("target"));
    assert_eq!(args.args()[1].name.as_deref(), Some("path"));
    assert_eq!(args.args()[2].name.as_deref(), Some("silent"));
}

#[test]
fn parse_named_function_args_in_from() {
    let stmt = parse_select("select * from generate_series(start => 1, stop := 3)").unwrap();
    let Some(FromItem::FunctionCall { name, args, .. }) = stmt.from else {
        panic!("expected function call in from");
    };
    assert_eq!(name, "generate_series");
    assert_eq!(args.len(), 2);
    assert_eq!(args[0].name.as_deref(), Some("start"));
    assert_eq!(args[1].name.as_deref(), Some("stop"));
}

#[test]
fn build_plan_rejects_positional_after_named_function_arg() {
    let stmt = parse_select("select jsonb_path_exists(path => '$', '{}'::jsonb)").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::UnexpectedToken { .. })
    ));
}

#[test]
fn build_plan_for_unnest_uses_array_element_types() {
    let stmt = parse_select("select * from unnest(ARRAY['a']::varchar[], ARRAY[1, 2])").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    let output_columns = match plan {
        Plan::FunctionScan {
            call: crate::include::nodes::primnodes::SetReturningCall::Unnest { output_columns, .. },
            ..
        } => output_columns,
        Plan::Projection { input, .. } => match *input {
            Plan::FunctionScan {
                call:
                    crate::include::nodes::primnodes::SetReturningCall::Unnest {
                        output_columns, ..
                    },
                ..
            } => output_columns,
            other => panic!("expected unnest function scan, got {other:?}"),
        },
        other => panic!("expected unnest plan, got {other:?}"),
    };
    {
        assert_eq!(output_columns.len(), 2);
        assert_eq!(
            output_columns[0].sql_type,
            SqlType::new(SqlTypeKind::Varchar)
        );
        assert_eq!(output_columns[1].sql_type, SqlType::new(SqlTypeKind::Int4));
    }
}

#[test]
fn build_plan_for_select_list_generate_series_uses_project_set() {
    let stmt = parse_select("select generate_series(1, 3)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::ProjectSet { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert!(matches!(
                &targets[0],
                crate::include::nodes::primnodes::ProjectSetTarget::Set {
                    call: crate::include::nodes::primnodes::SetReturningCall::GenerateSeries { .. },
                    ..
                }
            ));
        }
        other => panic!("expected project set plan, got {other:?}"),
    }
}

#[test]
fn build_plan_rejects_nested_srf_in_from_function_args() {
    let stmt = parse_select("select * from generate_series(1, generate_series(1, 3))").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::FeatureNotSupportedMessage(message))
            if message == "set-returning functions must appear at top level of FROM"
    ));
}

#[test]
fn build_plan_for_order_by_only_generate_series_uses_project_set() {
    fn plan_contains_project_set(plan: &Plan) -> bool {
        match plan {
            Plan::ProjectSet { .. } => true,
            Plan::OrderBy { input, .. }
            | Plan::IncrementalSort { input, .. }
            | Plan::Projection { input, .. } => plan_contains_project_set(input),
            _ => false,
        }
    }

    let stmt =
        parse_select("select id from people order by id, generate_series(1, 3) desc").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();

    assert!(plan_contains_project_set(&plan), "plan was {plan:?}");
}

#[test]
fn build_plan_rejects_distinct_on_with_target_srf_before_planning() {
    let stmt =
        parse_select("select distinct on (id) id, generate_series(1, 3) from people").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::FeatureNotSupportedMessage(message))
            if message == "SELECT DISTINCT ON with set-returning functions is not supported"
    ));
}

#[test]
fn build_plan_for_aliased_select_list_generate_series_uses_alias() {
    let stmt = parse_select("select generate_series(1, 3) as a").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::ProjectSet { targets, .. } => match &targets[..] {
            [crate::include::nodes::primnodes::ProjectSetTarget::Set { name, .. }] => {
                assert_eq!(name, "a");
            }
            other => panic!("expected single project set target, got {other:?}"),
        },
        other => panic!("expected project set plan, got {other:?}"),
    }
}

#[test]
fn build_plan_for_project_set_keeps_scalar_target_name() {
    let stmt = parse_select(
        "select i, jsonb_populate_recordset(row(i,50), '[{\"a\":2,\"b\":3}]') from (values (1),(2)) v(i)",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();

    assert_eq!(plan.column_names()[0], "i");
}

#[test]
fn build_plan_for_table_aliased_generate_series_keeps_direct_function_scan() {
    let stmt = parse_select("select * from generate_series(1, 3) as g").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::FunctionScan {
            call, table_alias, ..
        } => {
            assert_eq!(table_alias.as_deref(), Some("g"));
            assert_eq!(call.output_columns()[0].name, "g");
        }
        other => panic!("expected function scan plan, got {other:?}"),
    }
}

#[test]
fn build_plan_for_column_aliased_generate_series_keeps_direct_function_scan() {
    let stmt = parse_select("select * from generate_series(1, 3) as g(x)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::FunctionScan {
            call, table_alias, ..
        } => {
            assert_eq!(table_alias.as_deref(), Some("g"));
            assert_eq!(call.output_columns()[0].name, "x");
        }
        other => panic!("expected function scan plan, got {other:?}"),
    }
}

#[test]
fn build_plan_for_select_list_json_each_uses_record_project_set() {
    let stmt = parse_select("select json_each('{\"a\":1}'::json)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::ProjectSet { targets, .. } => {
            assert_eq!(targets.len(), 1);
            match &targets[0] {
                crate::include::nodes::primnodes::ProjectSetTarget::Set {
                    call:
                        crate::include::nodes::primnodes::SetReturningCall::JsonTableFunction {
                            kind: crate::include::nodes::primnodes::JsonTableFunction::Each,
                            ..
                        },
                    sql_type,
                    column_index,
                    ..
                } => {
                    assert_eq!(sql_type.kind, SqlTypeKind::Record);
                    assert_eq!(*column_index, 0);
                }
                other => panic!("expected json_each project set target, got {other:?}"),
            }
        }
        other => panic!("expected project set plan, got {other:?}"),
    }
}

#[test]
fn build_plan_resolves_field_select_from_select_list_record_srf_alias() {
    let stmt =
        parse_select("select (w).size = 16777216 from (select pg_ls_waldir() w) ss").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    assert_eq!(plan.columns()[0].sql_type, SqlType::new(SqlTypeKind::Bool));
}

#[test]
fn build_plan_resolves_pg_lsn_arithmetic_record_function_in_from() {
    let stmt = parse_select(
        "select segment_number, file_offset from pg_walfile_name_offset('0/0'::pg_lsn + 16777216), pg_split_walfile_name(file_name)",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    let columns = plan.columns();
    assert_eq!(columns[0].sql_type, SqlType::new(SqlTypeKind::Numeric));
    assert_eq!(columns[1].sql_type, SqlType::new(SqlTypeKind::Int4));
}

#[test]
fn build_plan_for_select_list_jsonb_each_field_select_projects_key_column() {
    let stmt = parse_select("select (jsonb_each('{\"a\":1}'::jsonb)).key").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::ProjectSet { targets, .. } => {
            assert_eq!(targets.len(), 1);
            match &targets[0] {
                crate::include::nodes::primnodes::ProjectSetTarget::Set {
                    call:
                        crate::include::nodes::primnodes::SetReturningCall::JsonTableFunction {
                            kind: crate::include::nodes::primnodes::JsonTableFunction::JsonbEach,
                            ..
                        },
                    sql_type,
                    column_index,
                    ..
                } => {
                    assert_eq!(*sql_type, SqlType::new(SqlTypeKind::Text));
                    assert_eq!(*column_index, 1);
                }
                other => panic!("expected jsonb_each project set target, got {other:?}"),
            }
        }
        other => panic!("expected project set plan, got {other:?}"),
    }
}

#[test]
fn parse_srf_with_column_definitions() {
    let stmt =
        parse_select("select * from json_each('{\"a\":1}'::json) as j(key text, value json)")
            .unwrap();
    match &stmt.from {
        Some(FromItem::Alias {
            alias,
            column_aliases,
            preserve_source_names,
            ..
        }) => {
            assert_eq!(alias, "j");
            assert_eq!(
                column_aliases,
                &AliasColumnSpec::Definitions(vec![
                    AliasColumnDef {
                        name: "key".into(),
                        ty: builtin_type(SqlType::new(SqlTypeKind::Text)),
                    },
                    AliasColumnDef {
                        name: "value".into(),
                        ty: builtin_type(SqlType::new(SqlTypeKind::Json)),
                    },
                ])
            );
            assert!(!preserve_source_names);
        }
        other => panic!("expected aliased function call, got {other:?}"),
    }
}

#[test]
fn parse_srf_column_definitions_without_alias() {
    let stmt = parse_select(
        "select * from jsonb_populate_record(null::record, '{\"x\":776}') as (x int, y int)",
    )
    .unwrap();
    match &stmt.from {
        Some(FromItem::Alias {
            alias,
            column_aliases,
            ..
        }) => {
            assert_eq!(alias, "jsonb_populate_record");
            assert!(
                matches!(column_aliases, AliasColumnSpec::Definitions(defs) if defs.len() == 2)
            );
        }
        other => panic!("expected aliased function column definitions, got {other:?}"),
    }
}

#[test]
fn analyze_json_each_uses_pg_proc_out_metadata_for_output_columns() {
    let mut row = json_each_proc_row();
    row.proargnames = Some(vec![String::new(), "left_key".into(), "payload".into()]);
    let catalog = OverrideFunctionCatalog {
        base: catalog(),
        proc_rows: vec![row],
    };

    let stmt = parse_select("select * from json_each('{\"a\":1}'::json)").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("left_key".into(), SqlType::new(SqlTypeKind::Text)),
            ("payload".into(), SqlType::new(SqlTypeKind::Json)),
        ]
    );
}

#[test]
fn analyze_pg_locks_uses_expected_columns_and_types() {
    let expected = pg_locks_expected_columns_and_types();
    for sql in ["select * from pg_locks", "select * from pg_lock_status()"] {
        let stmt = parse_select(sql).unwrap();
        let (query, _) =
            analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

        assert_eq!(query_column_names_and_types(&query), expected, "{sql}");
    }
}

fn pg_locks_expected_columns_and_types() -> Vec<(String, SqlType)> {
    vec![
        ("locktype".into(), SqlType::new(SqlTypeKind::Text)),
        ("database".into(), SqlType::new(SqlTypeKind::Oid)),
        ("relation".into(), SqlType::new(SqlTypeKind::Oid)),
        ("page".into(), SqlType::new(SqlTypeKind::Int4)),
        ("tuple".into(), SqlType::new(SqlTypeKind::Int2)),
        ("virtualxid".into(), SqlType::new(SqlTypeKind::Text)),
        ("transactionid".into(), SqlType::new(SqlTypeKind::Xid)),
        ("classid".into(), SqlType::new(SqlTypeKind::Oid)),
        ("objid".into(), SqlType::new(SqlTypeKind::Oid)),
        ("objsubid".into(), SqlType::new(SqlTypeKind::Int2)),
        ("virtualtransaction".into(), SqlType::new(SqlTypeKind::Text)),
        ("pid".into(), SqlType::new(SqlTypeKind::Int4)),
        ("mode".into(), SqlType::new(SqlTypeKind::Text)),
        ("granted".into(), SqlType::new(SqlTypeKind::Bool)),
        ("fastpath".into(), SqlType::new(SqlTypeKind::Bool)),
        ("waitstart".into(), SqlType::new(SqlTypeKind::TimestampTz)),
    ]
}

#[test]
fn analyze_pg_policies_uses_expected_columns_and_types() {
    let stmt = parse_select("select * from pg_policies").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("schemaname".into(), SqlType::new(SqlTypeKind::Name)),
            ("tablename".into(), SqlType::new(SqlTypeKind::Name)),
            ("policyname".into(), SqlType::new(SqlTypeKind::Name)),
            ("permissive".into(), SqlType::new(SqlTypeKind::Text)),
            (
                "roles".into(),
                SqlType::array_of(SqlType::new(SqlTypeKind::Name))
            ),
            ("cmd".into(), SqlType::new(SqlTypeKind::Text)),
            ("qual".into(), SqlType::new(SqlTypeKind::Text)),
            ("with_check".into(), SqlType::new(SqlTypeKind::Text)),
        ]
    );
}

#[test]
fn analyze_json_each_rejects_typed_column_definitions_for_out_parameters() {
    let stmt =
        parse_select("select * from json_each('{\"a\":1}'::json) as j(key text, value json)")
            .unwrap();
    let err = analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[])
        .unwrap_err()
        .to_string();

    assert_eq!(
        err,
        "a column definition list is redundant for a function with OUT parameters"
    );
}

#[test]
fn analyze_record_returning_function_requires_column_definition_list() {
    let mut row = json_each_proc_row();
    row.prorettype = RECORD_TYPE_OID;
    row.proallargtypes = None;
    row.proargmodes = None;
    row.proargnames = None;
    let catalog = OverrideFunctionCatalog {
        base: catalog(),
        proc_rows: vec![row],
    };

    let stmt = parse_select("select * from json_each('{\"a\":1}'::json)").unwrap();
    let err = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .unwrap_err()
        .to_string();

    assert_eq!(
        err,
        "a column definition list is required for functions returning \"record\""
    );
}

#[test]
fn analyze_record_returning_function_accepts_column_definition_list() {
    let mut row = json_each_proc_row();
    row.prorettype = RECORD_TYPE_OID;
    row.proallargtypes = None;
    row.proargmodes = None;
    row.proargnames = None;
    let catalog = OverrideFunctionCatalog {
        base: catalog(),
        proc_rows: vec![row],
    };

    let stmt =
        parse_select("select * from json_each('{\"a\":1}'::json) as j(a int4, b text)").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("a".into(), SqlType::new(SqlTypeKind::Int4)),
            ("b".into(), SqlType::new(SqlTypeKind::Text)),
        ]
    );
}

#[test]
fn analyze_named_composite_returning_function_uses_relation_rowtype() {
    let base = catalog();
    let mut row = json_each_proc_row();
    row.prorettype = relation_row_type_oid(&base, "people");
    row.proallargtypes = None;
    row.proargmodes = None;
    row.proargnames = None;
    let catalog = OverrideFunctionCatalog {
        base,
        proc_rows: vec![row],
    };

    let stmt = parse_select("select * from json_each('{\"a\":1}'::json)").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("id".into(), SqlType::new(SqlTypeKind::Int4)),
            ("name".into(), SqlType::new(SqlTypeKind::Text)),
            ("note".into(), SqlType::new(SqlTypeKind::Text)),
        ]
    );
}

#[test]
fn analyze_named_composite_returning_function_rejects_typed_column_definitions() {
    let base = catalog();
    let mut row = json_each_proc_row();
    row.prorettype = relation_row_type_oid(&base, "people");
    row.proallargtypes = None;
    row.proargmodes = None;
    row.proargnames = None;
    let catalog = OverrideFunctionCatalog {
        base,
        proc_rows: vec![row],
    };

    let stmt =
        parse_select("select * from json_each('{\"a\":1}'::json) as j(key text, value json)")
            .unwrap();
    let err = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .unwrap_err()
        .to_string();

    assert_eq!(
        err,
        "a column definition list is redundant for a function returning a named composite type"
    );
}

#[test]
fn analyze_json_populate_record_from_uses_named_composite_argument_rowtype() {
    let stmt = parse_select(
        "select * from json_populate_record(null::jpop, '{\"a\":\"blurfl\",\"x\":43.2}') q",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog_with_jpop(), &[], None, None, &[], &[])
            .unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("a".into(), SqlType::new(SqlTypeKind::Text)),
            ("b".into(), SqlType::new(SqlTypeKind::Int4)),
            ("c".into(), SqlType::new(SqlTypeKind::Timestamp)),
        ]
    );
}

#[test]
fn analyze_json_populate_recordset_from_uses_named_composite_argument_rowtype() {
    let stmt = parse_select(
        "select * from json_populate_recordset(null::jpop, '[{\"a\":\"blurfl\"},{\"b\":3}]') q",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog_with_jpop(), &[], None, None, &[], &[])
            .unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("a".into(), SqlType::new(SqlTypeKind::Text)),
            ("b".into(), SqlType::new(SqlTypeKind::Int4)),
            ("c".into(), SqlType::new(SqlTypeKind::Timestamp)),
        ]
    );
}

#[test]
fn analyze_jsonb_populate_record_from_uses_named_composite_argument_rowtype() {
    let stmt = parse_select(
        "select * from jsonb_populate_record(null::jpop, '{\"a\":\"blurfl\",\"x\":43.2}') q",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog_with_jpop(), &[], None, None, &[], &[])
            .unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("a".into(), SqlType::new(SqlTypeKind::Text)),
            ("b".into(), SqlType::new(SqlTypeKind::Int4)),
            ("c".into(), SqlType::new(SqlTypeKind::Timestamp)),
        ]
    );
}

#[test]
fn analyze_jsonb_to_record_from_uses_column_definition_list() {
    let stmt = parse_select(
        "select * from jsonb_to_record('{\"a\":1,\"b\":\"foo\"}') as x(a int, b text)",
    )
    .unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("a".into(), SqlType::new(SqlTypeKind::Int4)),
            ("b".into(), SqlType::new(SqlTypeKind::Text)),
        ]
    );
}

#[test]
fn analyze_jsonb_populate_recordset_rejects_mismatched_query_rowtype() {
    let stmt = parse_select(
        "select * from jsonb_populate_recordset(row(0::int), '[{\"a\":\"1\"}]') q (a text, b text)",
    )
    .unwrap();
    let err =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap_err();

    match err {
        ParseError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        } => {
            assert_eq!(
                message,
                "function return row and query-specified return row do not match"
            );
            assert_eq!(
                detail.as_deref(),
                Some("Returned row contains 1 attribute, but query expects 2.")
            );
            assert_eq!(sqlstate, "42804");
        }
        other => panic!("expected detailed rowtype mismatch, got {other:?}"),
    }
}

#[test]
fn analyze_scalar_srf_rejects_typed_column_definitions() {
    let stmt = parse_select("select * from generate_series(1, 3) as g(val int4)").unwrap();
    let err = analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[])
        .unwrap_err()
        .to_string();

    assert_eq!(
        err,
        "a column definition list is only allowed for functions returning \"record\""
    );
}

#[test]
fn parse_srf_with_column_alias() {
    let stmt = parse_select("select * from generate_series(1, 3) as g(val)").unwrap();
    match &stmt.from {
        Some(FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        }) => {
            let FromItem::FunctionCall { name, args, .. } = source.as_ref() else {
                panic!("expected FunctionCall source, got {:?}", source);
            };
            assert_eq!(name, "generate_series");
            assert_eq!(args.len(), 2);
            assert_eq!(alias, "g");
            assert_alias_names(column_aliases, &["val"]);
            assert!(!preserve_source_names);
        }
        other => panic!("expected Alias, got {:?}", other),
    }
}

#[test]
fn parse_srf_with_table_alias_only() {
    let stmt = parse_select("select * from generate_series(1, 3) as g").unwrap();
    match &stmt.from {
        Some(FromItem::Alias {
            alias,
            column_aliases,
            preserve_source_names,
            ..
        }) => {
            assert_eq!(alias, "g");
            assert!(column_aliases.is_empty());
            assert!(!preserve_source_names);
        }
        other => panic!("expected Alias, got {:?}", other),
    }
}

#[test]
fn parse_derived_table_with_alias() {
    let stmt = parse_select("select * from (select id from people) p").unwrap();
    match stmt.from {
        Some(FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        }) => {
            assert_eq!(alias, "p");
            assert!(column_aliases.is_empty());
            assert!(matches!(*source, FromItem::DerivedTable(_)));
            assert!(!preserve_source_names);
        }
        other => panic!("expected aliased derived table, got {:?}", other),
    }
}

#[test]
fn parse_derived_table_with_column_aliases() {
    let stmt = parse_select("select * from (select id, name from people) p(x, y)").unwrap();
    match stmt.from {
        Some(FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        }) => {
            assert_eq!(alias, "p");
            assert_alias_names(&column_aliases, &["x", "y"]);
            assert!(matches!(*source, FromItem::DerivedTable(_)));
            assert!(!preserve_source_names);
        }
        other => panic!("expected aliased derived table, got {:?}", other),
    }
}

#[test]
fn parse_aliasless_derived_table() {
    let stmt = parse_select("select * from (select id from people)").unwrap();
    assert!(matches!(stmt.from, Some(FromItem::DerivedTable(_))));
}

#[test]
fn parse_parenthesized_table_keyword_from_item() {
    let stmt = parse_select("select * from (table people) p").unwrap();
    assert_eq!(
        stmt.from,
        Some(FromItem::Alias {
            source: Box::new(FromItem::Table {
                name: "people".into(),
                only: false,
            }),
            alias: "p".into(),
            column_aliases: AliasColumnSpec::None,
            preserve_source_names: false,
        })
    );
}

#[test]
fn parse_join_with_derived_table() {
    let stmt = parse_select(
        "select * from people p join (select owner_id from pets) q on p.id = q.owner_id",
    )
    .unwrap();
    match stmt.from {
        Some(FromItem::Join {
            left,
            right,
            kind,
            constraint,
        }) => {
            assert_eq!(kind, JoinKind::Inner);
            assert!(matches!(constraint, JoinConstraint::On(_)));
            assert!(matches!(*left, FromItem::Alias { .. }));
            assert!(matches!(*right, FromItem::Alias { .. }));
        }
        other => panic!("expected join with derived table, got {:?}", other),
    }
}

#[test]
fn parse_cross_join_with_derived_table() {
    let stmt = parse_select("select * from people p, (select owner_id from pets) q").unwrap();
    match stmt.from {
        Some(FromItem::Join {
            left,
            right,
            kind,
            constraint,
        }) => {
            assert_eq!(kind, JoinKind::Cross);
            assert!(matches!(constraint, JoinConstraint::None));
            assert!(matches!(*left, FromItem::Alias { .. }));
            assert!(matches!(*right, FromItem::Alias { .. }));
        }
        other => panic!("expected cross join with derived table, got {:?}", other),
    }
}

#[test]
fn parse_join_precedence_binds_tighter_than_comma() {
    let stmt = parse_select("select * from a, b join c on b.id = c.id").unwrap();
    match stmt.from {
        Some(FromItem::Join {
            left,
            right,
            kind: JoinKind::Cross,
            constraint: JoinConstraint::None,
        }) => {
            assert!(matches!(*left, FromItem::Table { name, .. } if name == "a"));
            match *right {
                FromItem::Join {
                    left,
                    right,
                    kind: JoinKind::Inner,
                    constraint: JoinConstraint::On(_),
                } => {
                    assert!(matches!(*left, FromItem::Table { name, .. } if name == "b"));
                    assert!(matches!(*right, FromItem::Table { name, .. } if name == "c"));
                }
                other => panic!("expected inner join on right side, got {:?}", other),
            }
        }
        other => panic!("expected cross join tree, got {:?}", other),
    }
}

#[test]
fn parse_parenthesized_join_alias() {
    let stmt = parse_select("select * from (people p join pets q on p.id = q.owner_id) j").unwrap();
    match stmt.from {
        Some(FromItem::Alias { source, alias, .. }) => {
            assert_eq!(alias, "j");
            assert!(matches!(*source, FromItem::Join { .. }));
        }
        other => panic!("expected aliased parenthesized join, got {:?}", other),
    }
}

#[test]
fn parse_cross_join_keyword() {
    let stmt = parse_select("select * from people cross join pets").unwrap();
    assert!(matches!(
        stmt.from,
        Some(FromItem::Join {
            kind: JoinKind::Cross,
            constraint: JoinConstraint::None,
            ..
        })
    ));
}

#[test]
fn parse_join_using_clause() {
    let stmt = parse_select("select * from people join pets using (id)").unwrap();
    assert!(matches!(
        stmt.from,
        Some(FromItem::Join {
            kind: JoinKind::Inner,
            constraint: JoinConstraint::Using(columns),
            ..
        }) if columns == vec!["id".to_string()]
    ));
}

#[test]
fn parse_natural_left_join_clause() {
    let stmt = parse_select("select * from people natural left join pets").unwrap();
    assert!(matches!(
        stmt.from,
        Some(FromItem::Join {
            kind: JoinKind::Left,
            constraint: JoinConstraint::Natural,
            ..
        })
    ));
}

#[test]
fn parse_join_alias_without_parentheses() {
    let stmt =
        parse_select("select * from people join pets on people.id = pets.owner_id as j").unwrap();
    assert!(matches!(
        stmt.from,
        Some(FromItem::Alias {
            source,
            alias,
            preserve_source_names,
            ..
        }) if alias == "j" && preserve_source_names && matches!(*source, FromItem::Join { .. })
    ));
}

#[test]
fn build_plan_resolves_columns_from_derived_table_alias() {
    let stmt = parse_select("select p.id from (select id from people) p").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].name, "id");
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_aliasless_derived_table_exposes_unqualified_columns() {
    let stmt = parse_select("select id from (select id from people)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    assert!(matches!(
        plan,
        Plan::Projection { .. } | Plan::SeqScan { .. }
    ));
}

#[test]
fn parse_values_from_item() {
    let stmt = parse_select("select * from (values (1), (2)) as t(x)").unwrap();
    match stmt.from {
        Some(FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        }) => {
            assert_eq!(alias, "t");
            assert_alias_names(&column_aliases, &["x"]);
            assert!(!preserve_source_names);
            assert!(
                matches!(*source, FromItem::Values { ref rows } if rows.len() == 2 && rows[0].len() == 1)
            );
        }
        other => panic!("expected aliased values source, got {:?}", other),
    }
}

#[test]
fn build_plan_partial_derived_table_column_aliases_preserve_suffix() {
    let stmt = parse_select("select p.x, p.name from (select id, name from people) p(x)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    assert_eq!(
        plan.column_names(),
        vec!["x".to_string(), "name".to_string()]
    );
}

#[test]
fn build_plan_cross_join_derived_table_column_aliases() {
    let stmt =
        parse_select("select ii, tt, kk from (people cross join pets) as tx (ii, jj, tt, ii2, kk)")
            .unwrap();
    let plan = build_plan(&stmt, &catalog_with_pets()).unwrap();
    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 3);
            assert_eq!(targets[0].name, "ii");
            assert_eq!(targets[1].name, "tt");
            assert_eq!(targets[2].name, "kk");
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_rejects_too_many_derived_table_column_aliases() {
    let stmt = parse_select("select * from (select id from people) p(x, y)").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::UnexpectedToken { actual, .. })
            if actual == "table \"p\" has 1 columns available but 2 columns specified"
    ));
}

#[test]
fn build_plan_rejects_too_many_parenthesized_table_keyword_aliases() {
    let stmt = parse_select("select * from (table people) as p(x, y, z, w)").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::UnexpectedToken { actual, .. })
            if actual == "table \"p\" has 3 columns available but 4 columns specified"
    ));
}

#[test]
fn build_plan_values_alias_exposes_column_aliases() {
    let stmt = parse_select("select t.x from (values (1), (2)) as t(x)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].name, "x");
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_join_alias_hides_inner_relation_names() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt =
        parse_select("select p.id from (people p join pets q on p.id = q.owner_id) j").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::InvalidFromClauseReference(name)) if name == "p"
    ));
}

#[test]
fn build_plan_parenthesized_join_alias_reports_invalid_from_clause_reference() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt =
        parse_select("select * from (people p join pets q on p.id = q.owner_id) j where p.id = 1")
            .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::InvalidFromClauseReference(name)) if name == "p"
    ));
}

#[test]
fn build_plan_wrapped_join_alias_reports_missing_from_clause_entry() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select * from (people join pets on people.id = pets.owner_id as x) xx where x.id = 1",
    )
    .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::MissingFromClauseEntry(name)) if name == "x"
    ));
}

#[test]
fn build_plan_join_alias_rejects_duplicate_table_name() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select("select * from people a1 join pets a2 using (id) as a1").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::DuplicateTableName(name)) if name == "a1"
    ));
}

#[test]
fn build_plan_reports_ambiguous_column_reference() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select("select id from people cross join pets").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::AmbiguousColumn(name)) if name == "id"
    ));
}

#[test]
fn build_plan_lowers_coalesce_to_nested_expr() {
    let stmt = parse_select("select coalesce(null, id, 7) from people").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].sql_type, SqlType::new(SqlTypeKind::Int4));
            assert!(matches!(targets[0].expr, Expr::Coalesce(_, _)));
        }
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn build_plan_join_using_alias_preserves_base_table_names() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select x.id from people J1 join pets J2 using (id) as x where J1.name = 'alice'",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog).is_ok());
}

#[test]
fn build_plan_join_using_alias_exposes_only_merged_columns() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select("select x.name from people join pets using (id) as x").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::UnknownColumn(name)) if name == "x.name"
    ));
}

#[test]
fn build_plan_non_lateral_derived_table_rejects_outer_refs() {
    let stmt = parse_select("select * from people p, (select p.id from people) q").unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog()),
        Err(ParseError::UnknownColumn(name)) if name == "p.id"
    ));
}

#[test]
fn parse_random_function() {
    let stmt = parse_select("select random()").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert!(matches!(stmt.targets[0].expr, SqlExpr::Random));
    assert_eq!(stmt.targets[0].output_name, "random");
}

#[test]
fn parse_json_operators_and_functions() {
    let stmt = parse_select(
            "select '{\"a\":[1,null]}'::json -> 'a', '{\"a\":[1,null]}'::json ->> 'a', '{\"a\":{\"b\":1}}'::json #> ARRAY['a','b']::varchar[], json_typeof('{\"a\":1}'::json)",
        )
        .unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::JsonGet(_, _)));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::JsonGetText(_, _)));
    assert!(matches!(stmt.targets[2].expr, SqlExpr::JsonPath(_, _)));
    assert!(matches!(stmt.targets[3].expr, SqlExpr::FuncCall { .. }));
}

#[test]
fn parse_chained_json_operators() {
    let stmt = parse_select(
        "select '{\"a\":{\"b\":[1]}}'::json -> 'a' -> 'b' -> 0, '{\"a\":{\"b\":1}}'::json #> ARRAY['a']::varchar[] ->> 'b'",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::JsonGet(left, _)
            if matches!(
                left.as_ref(),
                SqlExpr::JsonGet(inner_left, _)
                    if matches!(inner_left.as_ref(), SqlExpr::JsonGet(_, _))
            )
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::JsonGetText(left, _)
            if matches!(left.as_ref(), SqlExpr::JsonPath(_, _))
    ));
}

#[test]
fn parse_json_table_function_in_from() {
    let stmt = parse_select("select * from json_each('{\"a\":1}'::json)").unwrap();
    assert!(matches!(
        stmt.from,
        Some(FromItem::FunctionCall { name, .. }) if name == "json_each"
    ));
}

#[test]
fn parse_json_builder_and_object_agg_functions() {
    let stmt = parse_select(
            "select json_build_array('a', 1), json_build_object('a', 1), json_object('{a,1,b,2}'::varchar[]), json_object_agg(name, note) from people group by name",
        )
        .unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::FuncCall { .. }));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::FuncCall { .. }));
    assert!(matches!(stmt.targets[2].expr, SqlExpr::FuncCall { .. }));
    assert!(matches!(
        &stmt.targets[3].expr,
        SqlExpr::FuncCall {
            name,
            args,
            distinct: false,
            ..
        } if name == "json_object_agg" && args.args().len() == 2
    ));
}

#[test]
fn parse_sql_json_special_syntax() {
    let stmt = parse_select(
        "select json('123'), json_scalar(123), json_serialize('{\"a\":1}' returning text), json_object('a' value 1), json_array(1, 2), '{\"a\":1}' is json object",
    )
    .unwrap();

    let expected = [
        crate::backend::parser::gram::SQL_JSON_FUNC,
        crate::backend::parser::gram::SQL_JSON_SCALAR_FUNC,
        crate::backend::parser::gram::SQL_JSON_SERIALIZE_FUNC,
        crate::backend::parser::gram::SQL_JSON_OBJECT_FUNC,
        crate::backend::parser::gram::SQL_JSON_ARRAY_FUNC,
        crate::backend::parser::gram::SQL_JSON_IS_JSON_FUNC,
    ];

    assert_eq!(stmt.targets.len(), expected.len());
    for (target, expected_name) in stmt.targets.iter().zip(expected) {
        let expr = match &target.expr {
            SqlExpr::Cast(expr, _)
                if expected_name == crate::backend::parser::gram::SQL_JSON_SERIALIZE_FUNC =>
            {
                expr.as_ref()
            }
            expr => expr,
        };
        assert!(
            matches!(
                expr,
                SqlExpr::FuncCall { name, .. } if name == expected_name
            ),
            "expected {expected_name}, got {:?}",
            target.expr
        );
    }

    assert!(parse_select("select json()").is_err());
    assert!(parse_select("select json_scalar()").is_err());
    assert!(parse_select("select json_serialize()").is_err());
}

#[test]
fn parse_jsonb_operators_and_functions() {
    let stmt = parse_select(
            "select '{\"a\":1}'::jsonb @> '{\"a\":1}'::jsonb, '{\"a\":1}'::jsonb ? 'a', '{\"a\":[1,2]}'::jsonb -> 0, jsonb_typeof('{\"a\":1}'::jsonb), jsonb_build_object('a', 1)",
        )
        .unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::JsonbContains(_, _)));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::JsonbExists(_, _)));
    assert!(matches!(stmt.targets[2].expr, SqlExpr::JsonGet(_, _)));
    assert!(matches!(stmt.targets[3].expr, SqlExpr::FuncCall { .. }));
    assert!(matches!(stmt.targets[4].expr, SqlExpr::FuncCall { .. }));
}

#[test]
fn parse_chained_jsonb_operators() {
    let stmt = parse_select(
        "select '{\"a\":{\"b\":1}}'::jsonb -> 'a' ->> 'b', '{\"a\":{\"b\":[1,2]}}'::jsonb -> 'a' -> 'b' -> 0",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::JsonGetText(left, _)
            if matches!(left.as_ref(), SqlExpr::JsonGet(_, _))
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::JsonGet(left, _)
            if matches!(
                left.as_ref(),
                SqlExpr::JsonGet(inner_left, _)
                    if matches!(inner_left.as_ref(), SqlExpr::JsonGet(_, _))
            )
    ));
}

#[test]
fn parse_jsonb_table_function_in_from() {
    let stmt = parse_select("select * from jsonb_each('{\"a\":1}'::jsonb)").unwrap();
    assert!(matches!(
        stmt.from,
        Some(FromItem::FunctionCall { name, .. }) if name == "jsonb_each"
    ));
}

#[test]
fn parse_jsonpath_type_and_operators() {
    let stmt = parse_select(
            "select '$.a'::jsonpath, '{\"a\":1}'::jsonb @? '$.a', '{\"a\":1}'::jsonb @@ '$.a == 1', jsonb_path_query_array('{\"a\":1}'::jsonb, '$.a')",
        )
        .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::Cast(_, ty) if ty.as_builtin().is_some_and(|ty| ty.kind == SqlTypeKind::JsonPath)
    ));
    assert!(matches!(
        stmt.targets[1].expr,
        SqlExpr::JsonbPathExists(_, _)
    ));
    assert!(matches!(
        stmt.targets[2].expr,
        SqlExpr::JsonbPathMatch(_, _)
    ));
    assert!(matches!(stmt.targets[3].expr, SqlExpr::FuncCall { .. }));
}

#[test]
fn parse_current_timestamp() {
    let stmt =
        parse_statement("insert into pgbench_history (mtime) values (current_timestamp)").unwrap();
    match stmt {
        Statement::Insert(InsertStatement {
            source: InsertSource::Values(values),
            ..
        }) => {
            assert!(matches!(
                values[0][0],
                SqlExpr::CurrentTimestamp { precision: None }
            ));
        }
        other => panic!("expected insert, got {:?}", other),
    }
}

#[test]
fn parse_insert_select_default_values_and_table_stmt() {
    let stmt = parse_statement("insert into people select 1, 'alice'").unwrap();
    assert!(matches!(
        stmt,
        Statement::Insert(InsertStatement {
            table_name,
            source: InsertSource::Select(_),
            ..
        }) if table_name == "people"
    ));

    let stmt = parse_statement("insert into people (select 1, 'alice')").unwrap();
    assert!(matches!(
        stmt,
        Statement::Insert(InsertStatement {
            table_name,
            source: InsertSource::Select(_),
            ..
        }) if table_name == "people"
    ));

    let stmt = parse_statement("insert into people default values").unwrap();
    assert!(matches!(
        stmt,
        Statement::Insert(InsertStatement {
            table_name,
            source: InsertSource::DefaultValues,
            ..
        }) if table_name == "people"
    ));

    let stmt = parse_statement("table people").unwrap();
    assert!(matches!(
        stmt,
        Statement::Select(SelectStatement { from: Some(FromItem::Table { name, .. }), .. })
            if name == "people"
    ));
}

#[test]
fn parse_insert_alias_and_begin_isolation_level() {
    let stmt = parse_statement(
        "insert into people as p (id, name) values (1, 'alice') on conflict (id) do update set name = p.name",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::Insert(InsertStatement {
            table_name,
            table_alias,
            ..
        }) if table_name == "people" && table_alias.as_deref() == Some("p")
    ));

    assert!(matches!(
        parse_statement("begin transaction isolation level repeatable read").unwrap(),
        Statement::Begin(TransactionOptions {
            isolation_level: Some(TransactionIsolationLevel::RepeatableRead),
            ..
        })
    ));
}

#[test]
fn parse_cursor_statements() {
    match parse_statement(
        "declare c binary insensitive scroll cursor with hold for select id from people",
    )
    .unwrap()
    {
        Statement::DeclareCursor(stmt) => {
            assert_eq!(stmt.name, "c");
            assert!(stmt.binary);
            assert!(stmt.insensitive);
            assert_eq!(stmt.scroll, CursorScrollOption::Scroll);
            assert!(stmt.hold);
            assert_eq!(stmt.query.targets[0].output_name, "id");
        }
        other => panic!("expected DECLARE CURSOR, got {:?}", other),
    }

    assert!(matches!(
        parse_statement("fetch from c").unwrap(),
        Statement::Fetch(FetchStatement {
            cursor_name,
            direction: FetchDirection::Next,
        }) if cursor_name == "c"
    ));
    assert!(matches!(
        parse_statement("fetch forward c").unwrap(),
        Statement::Fetch(FetchStatement {
            cursor_name,
            direction: FetchDirection::Forward(Some(1)),
        }) if cursor_name == "c"
    ));
    assert!(matches!(
        parse_statement("fetch all c").unwrap(),
        Statement::Fetch(FetchStatement {
            cursor_name,
            direction: FetchDirection::Forward(None),
        }) if cursor_name == "c"
    ));
    assert!(matches!(
        parse_statement("move backward 3 from c").unwrap(),
        Statement::Move(FetchStatement {
            cursor_name,
            direction: FetchDirection::Backward(Some(3)),
        }) if cursor_name == "c"
    ));
    assert!(matches!(
        parse_statement("close all").unwrap(),
        Statement::ClosePortal(ClosePortalStatement { name: None })
    ));
    assert!(matches!(
        parse_statement("close c").unwrap(),
        Statement::ClosePortal(ClosePortalStatement { name: Some(name) }) if name == "c"
    ));
}

#[test]
fn parse_dml_returning_targets() {
    assert!(matches!(
        parse_statement("insert into people (id) values (1) returning *").unwrap(),
        Statement::Insert(InsertStatement {
            table_name,
            returning,
            ..
        }) if table_name == "people"
            && returning == vec![SelectItem {
                output_name: "*".into(),
                expr: SqlExpr::Column("*".into()),
            }]
    ));

    assert!(matches!(
        parse_statement("update people set name = 'alice' returning id, upper(name) as upper_name")
            .unwrap(),
        Statement::Update(UpdateStatement {
            table_name,
            returning,
            ..
        }) if table_name == "people"
            && returning == vec![
                SelectItem {
                    output_name: "id".into(),
                    expr: SqlExpr::Column("id".into()),
                },
                SelectItem {
                    output_name: "upper_name".into(),
                    expr: SqlExpr::FuncCall {
                        name: "upper".into(),
                        args: SqlCallArgs::Args(vec![SqlFunctionArg::positional(
                            SqlExpr::Column("name".into()),
                        )]),
                        order_by: vec![],
                        within_group: None,
                        distinct: false,
                        func_variadic: false,
                        filter: None,
                        over: None,
                        null_treatment: None,
                    },
                },
            ]
    ));

    assert!(matches!(
        parse_statement("delete from people where id = 1 returning id, upper(name) as upper_name")
            .unwrap(),
        Statement::Delete(DeleteStatement {
            table_name,
            returning,
            ..
        }) if table_name == "people"
            && returning == vec![
                SelectItem {
                    output_name: "id".into(),
                    expr: SqlExpr::Column("id".into()),
                },
                SelectItem {
                    output_name: "upper_name".into(),
                    expr: SqlExpr::FuncCall {
                        name: "upper".into(),
                        args: SqlCallArgs::Args(vec![SqlFunctionArg::positional(
                            SqlExpr::Column("name".into()),
                        )]),
                        order_by: vec![],
                        within_group: None,
                        distinct: false,
                        func_variadic: false,
                        filter: None,
                        over: None,
                        null_treatment: None,
                    },
                },
            ]
    ));
}

#[test]
fn parse_create_table_column_defaults() {
    let stmt = parse_statement(
        "create table bit_defaults (b1 bit(4) default '1001', b2 bit varying(5) default B'0101')",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(columns[0].default_expr.as_deref(), Some("'1001'"));
    assert_eq!(columns[1].default_expr.as_deref(), Some("B'0101'"));
}

#[test]
fn parse_create_table_column_storage() {
    let stmt =
        parse_statement("create table test_chunk_id (a text, b text storage external)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(columns[0].storage, None);
    assert_eq!(
        columns[1].storage,
        Some(crate::include::access::tupdesc::AttributeStorage::External)
    );
}

#[test]
fn parse_create_table_generated_columns() {
    let stmt = parse_statement(
        "create table generated_items (a int4, b int4 generated always as (a + 1), c int4 generated always as (a + 2) stored)",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let columns = ct.columns().collect::<Vec<_>>();
    let virtual_column = columns[1].generated.as_ref().expect("virtual generated");
    assert_eq!(virtual_column.kind, ColumnGeneratedKind::Virtual);
    assert_eq!(virtual_column.expr_sql, "a + 1");
    let stored_column = columns[2].generated.as_ref().expect("stored generated");
    assert_eq!(stored_column.kind, ColumnGeneratedKind::Stored);
    assert_eq!(stored_column.expr_sql, "a + 2");
}

#[test]
fn parse_create_table_generated_rejects_by_default_and_duplicate_clause() {
    let by_default =
        parse_statement("create table bad (a int4, b int4 generated by default as (a))")
            .unwrap_err();
    assert!(format!("{by_default:?}").contains("GENERATED ALWAYS"));

    let duplicate = parse_statement(
        "create table bad (a int4, b int4 generated always as (a) generated always as (a))",
    )
    .unwrap_err();
    assert!(format!("{duplicate:?}").contains("multiple generation clauses"));
}

#[test]
fn parse_alter_table_column_expression_statements() {
    let stmt = parse_statement("alter table items alter column total set expression as (qty + 1)")
        .unwrap();
    match stmt {
        Statement::AlterTableAlterColumnExpression(stmt) => {
            assert_eq!(stmt.table_name, "items");
            assert_eq!(stmt.column_name, "total");
            assert!(matches!(
                stmt.action,
                AlterColumnExpressionAction::Set { ref expr_sql, .. } if expr_sql == "qty + 1"
            ));
        }
        other => panic!("expected alter column expression, got {other:?}"),
    }

    let stmt =
        parse_statement("alter table items alter column total drop expression if exists").unwrap();
    match stmt {
        Statement::AlterTableAlterColumnExpression(stmt) => {
            assert!(matches!(
                stmt.action,
                AlterColumnExpressionAction::Drop { missing_ok: true }
            ));
        }
        other => panic!("expected alter column expression, got {other:?}"),
    }
}

#[test]
fn parse_scalar_subquery_expression() {
    assert!(parse_select("select (select 1)").is_ok());
}

#[test]
fn parse_array_subquery_expression() {
    let stmt = parse_select("select array(select 1)").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::ArraySubquery(subquery) => {
            assert_eq!(subquery.targets.len(), 1);
        }
        other => panic!("expected array subquery, got {other:?}"),
    }
}

#[test]
fn parse_exists_subquery_expression() {
    assert!(parse_select("select exists (select 1)").is_ok());
    assert!(parse_select("select not exists (select 1)").is_ok());
}

#[test]
fn parse_in_subquery_expression() {
    assert!(parse_select("select id in (select owner_id from pets) from people").is_ok());
    assert!(parse_select("select id not in (select owner_id from pets) from people").is_ok());
    let stmt = parse_select("select (id, name) in (values (1, 'alice')) from people").unwrap();
    match &stmt.targets[0].expr {
        SqlExpr::InSubquery { expr, subquery, .. } => {
            assert!(matches!(expr.as_ref(), SqlExpr::Row(items) if items.len() == 2));
            assert!(
                matches!(subquery.from.as_ref(), Some(FromItem::Values { rows }) if rows[0].len() == 2)
            );
        }
        other => panic!("expected row-valued IN subquery, got {other:?}"),
    }
}

#[test]
fn parse_any_all_subquery_expressions() {
    assert!(parse_select("select id = any (select owner_id from pets) from people").is_ok());
    assert!(parse_select("select id < all (select owner_id from pets) from people").is_ok());
}

#[test]
fn build_plan_allows_correlated_scalar_subquery_in_target_list() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select p.name, (select count(*) from pets q where q.owner_id = p.id) from people p",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog).is_ok());
}

#[test]
fn build_plan_allows_correlated_exists_in_where() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select p.name from people p where exists (select 1 from pets q where q.owner_id = p.id)",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog).is_ok());
}

#[test]
fn build_plan_allows_nested_outer_correlation() {
    std::thread::Builder::new()
        .name("build_plan_allows_nested_outer_correlation".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let mut catalog = catalog();
            catalog.insert("pets", pets_entry());
            let stmt = parse_select(
                "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id and exists (select 1 from people r where r.id = p.id))",
            )
            .unwrap();
            assert!(build_plan(&stmt, &catalog).is_ok());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn build_plan_treats_subqueries_as_aggregate_scope_boundaries() {
    let stmt =
        parse_select("select name from people where exists (select count(*) from people p2)")
            .unwrap();
    assert!(build_plan(&stmt, &catalog()).is_ok());
}

#[test]
fn analyze_select_does_not_collect_child_local_aggregate_for_parent() {
    let stmt =
        parse_select("select name from people where exists (select count(*) from people p2)")
            .unwrap();
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[])
        .expect("analyze");
    assert!(query.accumulators.is_empty());
}

#[test]
fn build_plan_allows_grouped_outer_column_inside_subquery() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
            "select p.id, count(*) from people p group by p.id having exists (select 1 from pets q where q.owner_id = p.id)",
        )
        .unwrap();
    assert!(build_plan(&stmt, &catalog).is_ok());
}

#[test]
fn build_plan_rejects_ungrouped_outer_column_inside_subquery() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
            "select p.name, count(*) from people p group by p.id having exists (select 1 from pets q where q.owner_id = p.name)",
        )
        .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::UngroupedColumn { token, .. }) if token == "p.name" || token == "name"
    ));
}

#[test]
fn build_plan_allows_grouped_outer_aggregate_inside_subquery_where() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select p.id from people p group by p.id having exists (select 1 from pets q where sum(p.id) = q.owner_id)",
    )
    .unwrap();
    assert!(build_plan(&stmt, &catalog).is_ok());
}

#[test]
fn analyze_select_dedupes_semantically_equivalent_outer_aggregate_calls() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select p.note, sum(id) from people p group by p.note having exists (select 1 from pets q where sum(p.id) = q.owner_id)",
    )
    .unwrap();
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    assert_eq!(query.accumulators.len(), 1);
}

#[test]
fn build_plan_allows_outer_aggregate_with_ungrouped_arg_inside_subquery_where() {
    let catalog = catalog_with_pets();
    let stmt = parse_select(
        "select p.owner_id from pets p group by p.owner_id having exists (select 1 from pets q where sum(distinct p.id) = q.id)",
    )
    .unwrap();
    assert!(analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[]).is_ok());
    assert!(build_plan(&stmt, &catalog).is_ok());
}

#[test]
fn analyze_select_collects_outer_owned_aggregate_from_subquery() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select p.id from people p group by p.id having exists (select 1 from pets q where sum(p.id) = q.owner_id)",
    )
    .unwrap();
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, None, &[], &[])
        .expect("analyze");
    assert_eq!(query.accumulators.len(), 1);
}

#[test]
fn build_plan_rejects_mixed_local_and_outer_aggregate_in_subquery_where() {
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select p.id from people p group by p.id having exists (select 1 from pets q where sum(p.id + q.owner_id) = 1)",
    )
    .unwrap();
    assert!(matches!(
        build_plan(&stmt, &catalog),
        Err(ParseError::DetailedError { message, sqlstate, .. })
            if message == "aggregate functions are not allowed in WHERE" && sqlstate == "42803"
    ));
}

#[test]
fn build_plan_rejects_multi_column_scalar_subquery() {
    let stmt = parse_select("select (select id, name from people)").unwrap();
    assert!(build_plan(&stmt, &catalog()).is_err());
}

#[test]
fn parse_sql_string_continuation_literal() {
    let stmt = parse_statement("select 'first line'\n' - next line' as joined").unwrap();
    match stmt {
        Statement::Select(stmt) => assert_eq!(
            stmt.targets[0].expr,
            SqlExpr::Const(Value::Text("first line - next line".into()))
        ),
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_sql_string_continuation_rejects_comment_between_literals() {
    match parse_statement(
        "select 'first line'\n' - next line' /* blocked */\n' - third line' as joined",
    ) {
        Err(ParseError::UnexpectedToken { actual, .. }) => {
            assert_eq!(actual, "syntax error at or near \"' - third line'\"");
        }
        other => panic!("expected syntax error, got {other:?}"),
    }
}

#[test]
fn parse_unicode_string_and_identifier_literals() {
    let stmt = parse_statement("select U&'d\\0061t\\+000061' as U&\"d\\0061t\\+000061\"").unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(
                stmt.targets[0].expr,
                SqlExpr::Const(Value::Text("data".into()))
            );
            assert_eq!(stmt.targets[0].output_name, "data");
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_unicode_normalization_syntax_lowers_to_function_calls() {
    let stmt = parse_statement(
        "select normalize(U&'\\0061\\0308', nfd) as n, U&'\\00E4' is not nfkc normalized as ok",
    )
    .unwrap();
    let Statement::Select(stmt) = stmt else {
        panic!("expected select statement");
    };

    match &stmt.targets[0].expr {
        SqlExpr::FuncCall { name, args, .. } => {
            assert_eq!(name, "normalize");
            assert_eq!(args.args().len(), 2);
            assert_eq!(
                args.args()[1].value,
                SqlExpr::Const(Value::Text("NFD".into()))
            );
        }
        other => panic!("expected normalize call, got {other:?}"),
    }

    match &stmt.targets[1].expr {
        SqlExpr::Not(inner) => match inner.as_ref() {
            SqlExpr::FuncCall { name, args, .. } => {
                assert_eq!(name, "is_normalized");
                assert_eq!(args.args().len(), 2);
                assert_eq!(
                    args.args()[1].value,
                    SqlExpr::Const(Value::Text("NFKC".into()))
                );
            }
            other => panic!("expected is_normalized call, got {other:?}"),
        },
        other => panic!("expected negated normalization predicate, got {other:?}"),
    }
}

#[test]
fn parse_unicode_uescape_string_and_identifier_literals() {
    let stmt = parse_statement(
        "select U&'d!0061t\\+000061' UESCAPE '!' as U&\"d*0061t\\+000061\" UESCAPE '*'",
    )
    .unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(
                stmt.targets[0].expr,
                SqlExpr::Const(Value::Text("dat\\+000061".into()))
            );
            assert_eq!(stmt.targets[0].output_name, "dat\\+000061");
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_unicode_uescape_allows_non_escape_backslashes() {
    let stmt = parse_statement(r#"select U&' \' UESCAPE '!' as tricky"#).unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(
                stmt.targets[0].expr,
                SqlExpr::Const(Value::Text(" \\".into()))
            );
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_unicode_uescape_requires_simple_string_literal() {
    let err = parse_statement(r#"select U&'wrong: +0061' UESCAPE +"#).unwrap_err();
    assert_eq!(
        err.to_string(),
        "UESCAPE must be followed by a simple string literal at or near \"+\""
    );
}

#[test]
fn parse_unicode_uescape_rejects_invalid_escape_character() {
    let err = parse_statement(r#"select U&'wrong: +0061' UESCAPE '+'"#).unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid Unicode escape character at or near \"'+'\""
    );
}

#[test]
fn parse_unicode_string_rejects_when_standard_conforming_strings_is_off() {
    let err = parse_statement_with_options(
        "select U&'d\\0061ta'",
        ParseOptions {
            standard_conforming_strings: false,
            ..ParseOptions::default()
        },
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "unsafe use of string constant with Unicode escapes"
    );
}

#[test]
fn parse_unicode_string_validates_surrogate_pairs() {
    let stmt = parse_statement(r"select U&'\D83D\DE00'").unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(
                stmt.targets[0].expr,
                SqlExpr::Const(Value::Text("😀".into()))
            );
        }
        other => panic!("expected select statement, got {other:?}"),
    }

    let err = parse_statement(r"select U&'\D83D\0061'").unwrap_err();
    assert_eq!(err.to_string(), "invalid Unicode surrogate pair");
}

#[test]
fn parse_escape_string_validates_unicode_escapes() {
    let stmt = parse_statement(r"select E'\uD83D\uDE00'").unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert_eq!(
                stmt.targets[0].expr,
                SqlExpr::Const(Value::Text("😀".into()))
            );
        }
        other => panic!("expected select statement, got {other:?}"),
    }

    let err = parse_statement(r"select E'\u061'").unwrap_err();
    assert_eq!(err.to_string(), "invalid Unicode escape");

    let err = parse_statement(r"select E'\uD83D\u0061'").unwrap_err();
    assert_eq!(err.to_string(), "invalid Unicode surrogate pair");
}

#[test]
fn parse_like_and_trim_syntax() {
    let stmt =
        parse_statement("select trim(leading 'x' from 'xxxabc'), 'abc' ilike 'A%' escape '#'")
            .unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert!(matches!(
                &stmt.targets[0].expr,
                SqlExpr::FuncCall { name, .. } if name == "ltrim"
            ));
            assert!(matches!(
                &stmt.targets[1].expr,
                SqlExpr::Like {
                    case_insensitive: true,
                    negated: false,
                    ..
                }
            ));
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_quantified_like_syntax() {
    let stmt = parse_select(
        "select 'foo' like any (array['%a', '%o']), 'foo' not ilike all (array['F%', '%O'])",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::QuantifiedArray {
            op: SubqueryComparisonOp::Like,
            is_all: false,
            ..
        }
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::QuantifiedArray {
            op: SubqueryComparisonOp::NotILike,
            is_all: true,
            ..
        }
    ));
}

#[test]
fn parse_quantified_similar_syntax() {
    let stmt = parse_select(
        "select 'foo' similar to any (array['f..', 'b..']), 'foo' not similar to all (array['bar', 'baz'])",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::QuantifiedArray {
            op: SubqueryComparisonOp::Similar,
            is_all: false,
            ..
        }
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::QuantifiedArray {
            op: SubqueryComparisonOp::NotSimilar,
            is_all: true,
            ..
        }
    ));
}

#[test]
fn parse_trim_without_explicit_trim_chars() {
    let stmt = parse_select(
        "select trim(both from '  bunch  '), trim(leading from '  bunch  '), trim(trailing from '  bunch  ')",
    )
    .unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "btrim" && args.args().len() == 1
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "ltrim" && args.args().len() == 1
    ));
    assert!(matches!(
        &stmt.targets[2].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "rtrim" && args.args().len() == 1
    ));
}

#[test]
fn parse_similar_to_syntax() {
    let stmt = parse_statement("select 'abcdefg' similar to '_bcd#%' escape '#'").unwrap();
    match stmt {
        Statement::Select(stmt) => {
            assert!(matches!(
                &stmt.targets[0].expr,
                SqlExpr::Similar { negated: false, .. }
            ));
        }
        other => panic!("expected select statement, got {other:?}"),
    }
}

#[test]
fn parse_case_expressions() {
    let stmt = parse_select(
        "select
            case id when 1 then 'one' else 'other' end,
            case when id > 0 then note else name end
         from people",
    )
    .unwrap();

    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::Case {
            arg: Some(arg),
            args,
            defresult: Some(_),
        } if matches!(arg.as_ref(), SqlExpr::Column(name) if name == "id") && args.len() == 1
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::Case {
            arg: None,
            args,
            defresult: Some(_),
        } if args.len() == 1
    ));
}

#[test]
fn analyze_simple_case_uses_case_test_expr() {
    let stmt =
        parse_select("select case id when 1 then 'one' else 'other' end from people").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, None, &[], &[]).unwrap();

    match &query.target_list[0].expr {
        Expr::Case(case_expr) => {
            assert_eq!(case_expr.casetype, SqlType::new(SqlTypeKind::Text));
            assert!(matches!(
                case_expr.arg.as_deref(),
                Some(Expr::Var(Var {
                    varno: 1,
                    varattno: 1,
                    varlevelsup: 0,
                    vartype,
                })) if *vartype == SqlType::new(SqlTypeKind::Int4)
            ));
            assert_eq!(case_expr.args.len(), 1);
            assert!(matches!(
                &case_expr.args[0].expr,
                Expr::Op(op) if matches!(op.args.as_slice(), [Expr::CaseTest(_), _])
            ));
            assert_eq!(
                case_expr.args[0].result,
                Expr::Const(Value::Text("one".into()))
            );
            assert_eq!(
                case_expr.defresult.as_ref(),
                &Expr::Const(Value::Text("other".into()))
            );
        }
        other => panic!("expected CASE expression, got {other:?}"),
    }
}

#[test]
fn parse_numeric_literals_with_underscores() {
    let stmt = parse_select("select 2_147_483_647, 1_000.5, 1_0e+2").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::IntegerLiteral(ref value) if value == "2_147_483_647"
    ));
    assert!(matches!(
        stmt.targets[1].expr,
        SqlExpr::NumericLiteral(ref value) if value == "1_000.5"
    ));
    assert!(matches!(
        stmt.targets[2].expr,
        SqlExpr::NumericLiteral(ref value) if value == "1_0e+2"
    ));
}

#[test]
fn parse_create_unique_index_nulls_distinct() {
    let stmt =
        parse_statement("create unique index idx_items_id on items (id) nulls distinct").unwrap();
    match stmt {
        Statement::CreateIndex(stmt) => {
            assert!(stmt.unique);
            assert!(!stmt.nulls_not_distinct);
        }
        other => panic!("expected create index, got {other:?}"),
    }
}

#[test]
fn parse_vacuum_full_bare_option() {
    let stmt = parse_statement("vacuum full items").unwrap();
    match stmt {
        Statement::Vacuum(stmt) => {
            assert!(stmt.full);
            assert_eq!(stmt.targets.len(), 1);
            assert_eq!(stmt.targets[0].table_name, "items");
        }
        other => panic!("expected vacuum, got {other:?}"),
    }
}

#[test]
fn parse_reindex_table_concurrently_verbose() {
    let stmt = parse_statement("reindex (verbose, concurrently) table items").unwrap();
    match stmt {
        Statement::ReindexIndex(stmt) => {
            assert_eq!(stmt.kind, ReindexTargetKind::Table);
            assert!(stmt.verbose);
            assert!(stmt.concurrently);
            assert_eq!(stmt.index_name, "items");
        }
        other => panic!("expected reindex, got {other:?}"),
    }
}

#[test]
fn parse_set_session_role() {
    let stmt = parse_statement("set session role regress_reindexuser").unwrap();
    match stmt {
        Statement::SetRole(stmt) => {
            assert_eq!(stmt.role_name.as_deref(), Some("regress_reindexuser"));
        }
        other => panic!("expected set role, got {other:?}"),
    }
}

#[test]
fn parse_compound_alter_table_drop_add_using_index() {
    let stmt = parse_statement(
        "alter table cwi_test drop constraint cwi_uniq_idx, \
         add constraint cwi_replaced_pkey primary key using index cwi_uniq2_idx",
    )
    .unwrap();
    match stmt {
        Statement::AlterTableCompound(stmt) => {
            assert_eq!(stmt.actions.len(), 2);
            assert!(matches!(
                stmt.actions[0],
                Statement::AlterTableDropConstraint(_)
            ));
            assert!(matches!(
                stmt.actions[1],
                Statement::AlterTableAddConstraint(_)
            ));
        }
        other => panic!("expected compound alter table, got {other:?}"),
    }
}
