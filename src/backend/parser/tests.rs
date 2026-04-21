use super::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{AggFunc, Expr, Plan, RelationDesc, Value};
use crate::include::access::htup::{AttributeAlign, AttributeStorage};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CONSTRAINT_PRIMARY, JSON_TYPE_OID, PUBLIC_NAMESPACE_OID, PgProcRow,
    PgRewriteRow, PgTypeRow, RECORD_TYPE_OID, bootstrap_pg_proc_rows, sort_pg_rewrite_rows,
};
use crate::include::nodes::parsenodes::{
    AliasColumnDef, AliasColumnSpec, ColumnConstraint, CompositeTypeAttributeDef,
    CreateCompositeTypeStatement, CreateTriggerStatement, CreateTypeStatement,
    DropTriggerStatement, DropTypeStatement, ForeignKeyAction, ForeignKeyMatchType, IndexColumnDef,
    InsertSource, InsertStatement, JoinTreeNode, RangeTblEntryKind, RawTypeName, TableConstraint,
    TriggerEvent, TriggerEventSpec, TriggerLevel, TriggerTiming,
};
use crate::include::nodes::primnodes::{AttrNumber, JoinType, Var, is_system_attr};

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

fn attrs() -> ConstraintAttributes {
    ConstraintAttributes::default()
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
        row_type_oid: 60_000u32.saturating_add(rel_number),
        array_type_oid: 61_000u32.saturating_add(rel_number),
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'r',
        relhastriggers: false,
        relhassubclass: false,
        relispartition: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        desc,
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
        row_type_oid: 60020,
        array_type_oid: 60021,
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'v',
        relhastriggers: false,
        relhassubclass: false,
        relispartition: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        desc: RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("name", SqlType::new(SqlTypeKind::Text), false),
            ],
        },
        index_meta: None,
    }
}

fn catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert("people", test_catalog_entry(15000, desc()));
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
            row_type_oid: 60010,
            array_type_oid: 0,
            reltoastrelid: 0,
            relpersistence: 'p',
            relkind: 'i',
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: 65000,
                indisunique: false,
                indisprimary: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indkey: vec![1],
                indclass: vec![],
                indcollation: vec![],
                indoption: vec![],
                indexprs: None,
                indpred: None,
            }),
        },
    );
    catalog
}

fn catalog_with_people_primary_key() -> Catalog {
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
            row_type_oid: 60011,
            array_type_oid: 0,
            reltoastrelid: 0,
            relpersistence: 'p',
            relkind: 'i',
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: 65000,
                indisunique: true,
                indisprimary: true,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indkey: vec![1],
                indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: None,
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
    let mut catalog = catalog();
    add_ready_people_index(
        &mut catalog,
        "people_id_name_key",
        true,
        false,
        &[IndexColumnDef::from("id"), IndexColumnDef::from("name")],
    );
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
            row_type_oid: 60013,
            array_type_oid: 0,
            reltoastrelid: 0,
            relpersistence: 'p',
            relkind: 'i',
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
            },
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: people.relation_oid,
                indisunique: true,
                indisprimary: false,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indkey: vec![1],
                indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: Some("(id > 0)".into()),
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
            row_type_oid: 60031,
            array_type_oid: 0,
            reltoastrelid: 0,
            relpersistence: 'p',
            relkind: 'i',
            relhastriggers: false,
            relhassubclass: false,
            relispartition: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            desc: RelationDesc {
                columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Text), false)],
            },
            index_meta: Some(crate::backend::catalog::state::CatalogIndexMeta {
                indrelid: 65030,
                indisunique: true,
                indisprimary: true,
                indisvalid: true,
                indisready: true,
                indislive: true,
                indkey: vec![1],
                indclass: vec![crate::include::catalog::TEXT_BTREE_OPCLASS_OID],
                indcollation: vec![0],
                indoption: vec![0],
                indexprs: None,
                indpred: None,
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
        base.proc_rows(),
        base.cast_rows()
            .into_iter()
            .filter(|row| {
                !(row.castsource == crate::include::catalog::TEXT_TYPE_OID
                    && row.casttarget == target_oid
                    && row.castmethod == 'i')
        })
            .collect(),
        base.collation_rows(),
        base.foreign_data_wrapper_rows(),
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
        base.proc_rows(),
        base.cast_rows(),
        base.collation_rows(),
        base.foreign_data_wrapper_rows(),
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
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[]).unwrap();

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
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[]).unwrap();
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
fn parse_set_statement() {
    let stmt = parse_statement("set extra_float_digits = 0").unwrap();
    assert_eq!(
        stmt,
        Statement::Set(SetStatement {
            name: "extra_float_digits".into(),
            value: "0".into(),
            is_local: false,
        })
    );
}

#[test]
fn parse_transaction_alias_statements() {
    assert_eq!(
        parse_statement("begin transaction").unwrap(),
        Statement::Begin
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
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: "id2".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
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
fn parse_create_index_with_method_and_ordering() {
    let stmt = parse_statement(
        "create index num_exp_add_idx on num_exp_add using btree (id1 desc nulls first, id2 asc)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
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
                    descending: true,
                    nulls_first: Some(true),
                },
                IndexColumnDef {
                    name: "id2".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
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
                descending: false,
                nulls_first: None,
            }],
            include_columns: Vec::new(),
            predicate: None,
            options: Vec::new(),
        })
    );
}

#[test]
fn parse_create_index_without_name() {
    let stmt = parse_statement("create index on tenk1 (thousand, tenthous)").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
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
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: "tenthous".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
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
fn parse_create_index_with_expression_item() {
    let stmt = parse_statement("create index attmp_idx on attmp (a, (d + e), b)").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
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
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: String::new(),
                    expr_sql: Some("d + e".into()),
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: "b".into(),
                    expr_sql: None,
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    descending: false,
                    nulls_first: None,
                },
            ],
            include_columns: Vec::new(),
            predicate: None,
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
            only: false,
            table_name: "items".into(),
            column: ColumnDef {
                name: "note".into(),
                ty: builtin_type(SqlType::new(SqlTypeKind::Text)),
                default_expr: Some("'hello'".into()),
                constraints: vec![],
            },
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
                referenced_table: "people".into(),
                referenced_columns: Some(vec!["id".into(), "name".into()]),
                match_type: ForeignKeyMatchType::Full,
                on_delete: ForeignKeyAction::NoAction,
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
                referenced_table: "people".into(),
                referenced_columns: Some(vec!["id".into(), "name".into()]),
                match_type: ForeignKeyMatchType::Full,
                on_delete: ForeignKeyAction::NoAction,
                on_update: ForeignKeyAction::NoAction,
            },
        })
    );

    let stmt = parse_statement("alter table items drop constraint items_id_check").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropConstraint(AlterTableDropConstraintStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            constraint_name: "items_id_check".into(),
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
            deferrable: Some(false),
            initially_deferred: None,
            enforced: Some(false),
        })
    );

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
fn parse_alter_table_set_statement() {
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

    let stmt =
        parse_statement("alter policy p1 on items rename to p2").unwrap();
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

    let stmt = parse_statement(
        "create policy p3 on items as permissive\n    using (a > 2);\n",
    )
    .unwrap();
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
fn parse_alter_table_drop_column_statement() {
    let stmt = parse_statement("alter table items drop column note").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableDropColumn(AlterTableDropColumnStatement {
            if_exists: false,
            only: false,
            table_name: "items".into(),
            column_name: "note".into(),
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
    let stmt = parse_statement(
        "create group regress_group with admin regress_admin user regress_member",
    )
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
            inherit_option: None,
            set_option: None,
            granted_by: None,
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
            inherit_option: None,
            set_option: None,
            granted_by: None,
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

    let stmt = parse_statement("set session authorization 'regress_tenant'").unwrap();
    assert_eq!(
        stmt,
        Statement::SetSessionAuthorization(SetSessionAuthorizationStatement {
            role_name: "regress_tenant".into(),
        })
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
            object_name: "regression".into(),
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
            object_name: "public".into(),
            grantee_names: vec!["public".into()],
            with_grant_option: false,
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
            object_name: "uaccount".into(),
            grantee_names: vec!["public".into()],
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
            object_name: "uaccount".into(),
            grantee_names: vec!["public".into()],
            with_grant_option: false,
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
            object_name: "f_leak(text)".into(),
            grantee_names: vec!["public".into()],
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
            object_name: "tenant_table".into(),
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
            inherit_option: Some(true),
            set_option: Some(false),
            granted_by: None,
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
            inherit_option: None,
            set_option: None,
            granted_by: Some(RoleGrantorSpec::CurrentRole),
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
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: Some("x".into()),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
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
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: Some("x".into()),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
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
            when_clause_sql: Some("new.name is not null".into()),
            function_schema_name: Some("public".into()),
            function_name: "audit_people".into(),
            func_args: vec!["x".into(), "arg2".into()],
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
fn parse_create_trigger_rejects_unsupported_truncate() {
    let err = parse_statement(
        "create trigger bad_truncate before truncate on people for each statement execute function bad()",
    )
    .unwrap_err();
    assert!(
        matches!(err, ParseError::FeatureNotSupported(message) if message.contains("TRUNCATE triggers are not supported"))
    );
}

#[test]
fn parse_expression_entrypoint_reuses_sql_expression_grammar() {
    let expr = parse_expr("1 + 2 * 3").unwrap();
    assert!(matches!(expr, SqlExpr::Add(_, _)));
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
            args: vec![
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
                },
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
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
            args: vec![
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
                },
                CreateFunctionArg {
                    mode: FunctionArgMode::In,
                    name: None,
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
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
            args: vec![CreateFunctionArg {
                mode: FunctionArgMode::In,
                name: None,
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bytea)),
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
            value: "warning".into(),
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
            value: "10.5".into(),
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
            value: "-8".into(),
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
            value: "line\nbreak".into(),
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
        }
        other => panic!("expected copy statement, got {other:?}"),
    }
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
            if name == "position" && args.len() == 2
    ));
}

#[test]
fn parse_extract_in_syntax_as_date_part_call() {
    let stmt = parse_select("select extract(week from date '2020-08-11')").unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::FuncCall { ref name, ref args, .. }
            if name == "date_part"
                && args.len() == 2
                && matches!(args[0].value, SqlExpr::Const(Value::Text(ref field)) if &field[..] == "week")
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
                } if name == "json_build_array" && args.len() == 1
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
                } if name == "json_extract_path" && args.len() == 2
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
                    if name == "position" && args.len() == 2
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
fn analyze_extract_keeps_extract_as_default_output_name() {
    let stmt = parse_select("select extract(week from date '2020-08-11')").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![("extract".into(), SqlType::new(SqlTypeKind::Float8))]
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
        parse_type_name("bytea").unwrap(),
        SqlType::new(SqlTypeKind::Bytea)
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
        parse_type_name("timestamptz").unwrap(),
        SqlType::new(SqlTypeKind::TimestampTz)
    );
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
            assert_eq!(args.len(), 3);
        }
        other => panic!("expected substring call, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::FuncCall { name, args, .. } => {
            assert_eq!(name, "overlay");
            assert_eq!(args.len(), 3);
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
            assert_eq!(args.len(), 3);
            assert!(matches!(
                &args[1].value,
                SqlExpr::IntegerLiteral(value) if value == "1"
            ));
        }
        other => panic!("expected substring call, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::FuncCall { name, args, .. } => {
            assert_eq!(name, "substring");
            assert_eq!(args.len(), 3);
            assert!(matches!(
                &args[1].value,
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
fn parse_timestamptz_typed_string_literal_with_text_cast() {
    let stmt =
        parse_select("select timestamptz '2024-01-02 03:04:05+00'::text").unwrap();
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
        SqlExpr::FuncCall { name, args, .. } if name == "abs" && args.len() == 1
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "sqrt" && args.len() == 1
    ));
    assert!(matches!(
        &stmt.targets[2].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "cbrt" && args.len() == 1
    ));
}

#[test]
fn parse_power_operator_and_in_list() {
    let stmt = parse_select("select x ^ '2.0', x in (0, 1, 2) from metrics").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "power" && args.len() == 2
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
            assert_eq!(assignments[1].target.column, "b");
            assert_eq!(assignments[1].target.subscripts.len(), 1);
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
            assert_eq!(columns[1].column, "b");
            assert_eq!(columns[1].subscripts.len(), 1);
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
        &parse_select("select ARRAY[1, 2] = '{1,2}', ARRAY[1, 2] && '{2,3}', 2 = any ('{1,2,3}')")
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
                && matches!(right, Expr::Cast(inner, ty)
                    if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                        && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))))
    ));
    assert!(matches!(
        &targets[1].expr,
        Expr::Op(op)
            if op.op == crate::include::nodes::primnodes::OpExprKind::ArrayOverlap
                && matches!(op.args.as_slice(), [left, right]
                    if matches!(left, Expr::ArrayLiteral { array_type, .. }
                if *array_type == SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
                && matches!(right, Expr::Cast(inner, ty)
                    if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                        && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)))))
    ));
    assert!(matches!(
        &targets[2].expr,
        Expr::ScalarArrayOp(saop)
            if saop.use_or
                && matches!(saop.right.as_ref(), Expr::Cast(inner, ty)
                if *ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
                    && matches!(inner.as_ref(), Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _))))
    ));
}

#[test]
fn analyze_timestamptz_typed_string_literal_keeps_timestamp_tz_type() {
    let stmt = parse_select("select timestamptz '2024-01-02 03:04:05+00'").unwrap();
    let (query, _) =
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[]).unwrap();
    assert!(matches!(
        &query.target_list[0].expr,
        Expr::Cast(inner, ty)
            if *ty == SqlType::new(SqlTypeKind::TimestampTz)
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
fn build_plan_dispatches_geometry_and_range_position_operators_independently() {
    let geometry_plan = build_plan(
        &parse_select("select '(0,0),(1,1)'::box &< '(2,2),(3,3)'::box").unwrap(),
        &catalog(),
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
        &parse_select("select '(0,0),(1,1)'::box && '(2,2),(3,3)'::box").unwrap(),
        &catalog(),
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
        &parse_select("select int4range(1, 4) &< int4range(2, 10)").unwrap(),
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
                    crate::include::nodes::primnodes::BuiltinScalarFunction::RangeOverLeft
                )
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
                    match *input {
                        Plan::OrderBy { input, items, .. } => {
                            assert_eq!(items.len(), 1);
                            assert!(items[0].descending);
                            assert!(matches!(*input, Plan::Filter { .. }));
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
        Plan::Projection { input, .. } => match *input {
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
        matches!(parse_statement("insert into people (id, name) values (1, 'alice')").unwrap(), Statement::Insert(InsertStatement { table_name, .. }) if table_name == "people")
    );
    assert!(matches!(
        parse_statement("insert into people (id, name) values (1, 'alice'), (2, 'bob')").unwrap(),
        Statement::Insert(InsertStatement { table_name, source: InsertSource::Values(values), .. })
            if table_name == "people" && values.len() == 2
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
        matches!(parse_statement("create temp table withoutoid() without oids").unwrap(), Statement::CreateTable(ct) if ct.persistence == TablePersistence::Temporary && ct.table_name == "withoutoid" && ct.columns().count() == 0)
    );
    assert!(
        matches!(parse_statement("create temp table withoutoid() with (oids = false)").unwrap(), Statement::CreateTable(ct) if ct.persistence == TablePersistence::Temporary && ct.table_name == "withoutoid" && ct.columns().count() == 0)
    );
    assert!(matches!(
        parse_statement("create table widgets (like source_table)"),
        Err(ParseError::FeatureNotSupported(feature)) if feature == "CREATE TABLE ... LIKE"
    ));
    assert!(matches!(
        parse_statement("create table withoid() with (oids)"),
        Err(ParseError::TablesDeclaredWithOidsNotSupported)
    ));
    assert!(matches!(
        parse_statement("create table withoid() with (oids = true)"),
        Err(ParseError::TablesDeclaredWithOidsNotSupported)
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
        matches!(parse_statement("drop table widgets").unwrap(), Statement::DropTable(DropTableStatement { if_exists: false, table_names, cascade: false }) if table_names == vec!["widgets"])
    );
    assert!(
        matches!(parse_statement("drop table if exists pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers").unwrap(), Statement::DropTable(DropTableStatement { if_exists: true, table_names, cascade: false }) if table_names == vec!["pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"])
    );
    assert!(
        matches!(parse_statement("drop table widgets cascade").unwrap(), Statement::DropTable(DropTableStatement { if_exists: false, table_names, cascade: true }) if table_names == vec!["widgets"])
    );
    assert!(
        matches!(parse_statement("drop index tenant_idx").unwrap(), Statement::DropIndex(DropIndexStatement { if_exists: false, index_names }) if index_names == vec!["tenant_idx"])
    );
    assert!(
        matches!(parse_statement("drop schema if exists tenant_a, tenant_b").unwrap(), Statement::DropSchema(DropSchemaStatement { if_exists: true, schema_names, cascade: false }) if schema_names == vec!["tenant_a", "tenant_b"])
    );
    assert!(
        matches!(parse_statement("drop schema if exists tenant_a cascade").unwrap(), Statement::DropSchema(DropSchemaStatement { if_exists: true, schema_names, cascade: true }) if schema_names == vec!["tenant_a"])
    );
    assert!(
        matches!(parse_statement("create view item_names as select id, name from people").unwrap(), Statement::CreateView(CreateViewStatement { schema_name: None, view_name, query_sql, .. }) if view_name == "item_names" && query_sql == "select id, name from people")
    );
    assert!(
        matches!(parse_statement("create schema tenant").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), auth_role: None, if_not_exists: false }) if schema_name == "tenant")
    );
    assert!(
        matches!(parse_statement("create schema if not exists tenant").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), auth_role: None, if_not_exists: true }) if schema_name == "tenant")
    );
    assert!(
        matches!(parse_statement("create schema authorization app_user").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: None, auth_role: Some(auth_role), if_not_exists: false }) if auth_role == "app_user")
    );
    assert!(
        matches!(parse_statement("create schema tenant authorization app_user").unwrap(), Statement::CreateSchema(CreateSchemaStatement { schema_name: Some(schema_name), auth_role: Some(auth_role), if_not_exists: false }) if schema_name == "tenant" && auth_role == "app_user")
    );
    assert!(
        matches!(parse_statement("drop view if exists item_names, recent_items").unwrap(), Statement::DropView(DropViewStatement { if_exists: true, view_names }) if view_names == vec!["item_names", "recent_items"])
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
        matches!(parse_statement("update people set note = 'x' where id = 1").unwrap(), Statement::Update(UpdateStatement { table_name, only, .. }) if table_name == "people" && !only)
    );
    assert!(
        matches!(parse_statement("update only people set note = 'x' where id = 1").unwrap(), Statement::Update(UpdateStatement { table_name, only, .. }) if table_name == "people" && only)
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
        Plan::NestedLoopJoin { .. } | Plan::HashJoin { .. }
    ));
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
fn parse_rejects_unsupported_rule_action_statement() {
    let err =
        parse_statement("create rule r1 as on insert to people do instead select 1").unwrap_err();
    assert!(matches!(err, ParseError::FeatureNotSupported(_)));
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
            },
            crate::include::nodes::parsenodes::AssignmentTarget {
                column: "name".into(),
                subscripts: vec![],
            },
        ]),
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
fn bind_insert_resolves_on_conflict_constraint_name() {
    let catalog = catalog_with_people_primary_key();
    let stmt = people_insert_with_on_conflict(
        Some(crate::include::nodes::parsenodes::OnConflictTarget::Constraint("people_pkey".into())),
        crate::include::nodes::parsenodes::OnConflictAction::Update,
        vec![crate::include::nodes::parsenodes::Assignment {
            target: crate::include::nodes::parsenodes::AssignmentTarget {
                column: "name".into(),
                subscripts: vec![],
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
fn bind_insert_rejects_non_inferable_on_conflict_indexes() {
    let partial_catalog = catalog_with_people_partial_unique_index();
    let partial_stmt = people_insert_with_on_conflict(
        Some(plain_inference_target(&["id"])),
        crate::include::nodes::parsenodes::OnConflictAction::Nothing,
        vec![],
        None,
    );
    assert!(matches!(
        stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || bind_insert(&partial_stmt, &partial_catalog)),
        Err(ParseError::UnexpectedToken { actual, .. })
            if actual
                == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
    ));
}

#[test]
fn bind_insert_rejects_richer_on_conflict_inference_syntax() {
    std::thread::Builder::new()
        .name("bind_insert_rejects_richer_on_conflict_inference_syntax".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let catalog = catalog_with_people_primary_key();

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
            assert!(matches!(
                bind_insert(&expression_stmt, &catalog),
                Err(ParseError::FeatureNotSupported(feature))
                    if feature == "ON CONFLICT inference expressions"
            ));

            let collation_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("id".into()),
                        collation: Some("C".into()),
                        opclass: None,
                    }],
                    predicate: None,
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&collation_stmt, &catalog),
                Err(ParseError::FeatureNotSupported(feature))
                    if feature == "ON CONFLICT inference collation"
            ));

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
            assert!(matches!(
                bind_insert(&opclass_stmt, &catalog),
                Err(ParseError::FeatureNotSupported(feature))
                    if feature == "ON CONFLICT inference operator class"
            ));

            let predicate_stmt = people_insert_with_on_conflict(
                Some(OnConflictTarget::Inference(OnConflictInferenceSpec {
                    elements: vec![OnConflictInferenceElem {
                        expr: SqlExpr::Column("id".into()),
                        collation: None,
                        opclass: None,
                    }],
                    predicate: Some(parse_expr("id > 0").unwrap()),
                })),
                OnConflictAction::Nothing,
                vec![],
                None,
            );
            assert!(matches!(
                bind_insert(&predicate_stmt, &catalog),
                Err(ParseError::FeatureNotSupported(feature))
                    if feature == "ON CONFLICT inference WHERE"
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
fn parse_rejects_show_tables() {
    assert!(parse_statement("show tables").is_err());
}

#[test]
fn parse_show_timezone() {
    assert!(matches!(
        parse_statement("show timezone").unwrap(),
        Statement::Show(ShowStatement { name }) if name == "timezone"
    ));
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
fn parse_current_user_and_legacy_null_predicates() {
    let stmt = parse_select("select current_user, note isnull, note notnull from people").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::CurrentUser));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::IsNull(_)));
    assert!(matches!(stmt.targets[2].expr, SqlExpr::IsNotNull(_)));
}

#[test]
fn parse_session_user_and_current_role() {
    let stmt = parse_select("select session_user, current_role from people").unwrap();
    assert!(matches!(stmt.targets[0].expr, SqlExpr::SessionUser));
    assert!(matches!(stmt.targets[1].expr, SqlExpr::CurrentRole));
}

#[test]
fn create_table_temp_name_validation() {
    let (name, persistence) =
        crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
            schema_name: Some("public".into()),
            table_name: "t".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![],
            inherits: Vec::new(),
            if_not_exists: false,
        })
        .unwrap();
    assert_eq!(name, "t");
    assert_eq!(persistence, TablePersistence::Permanent);

    let err = crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
        schema_name: Some("public".into()),
        table_name: "t".into(),
        persistence: TablePersistence::Temporary,
        on_commit: OnCommitAction::PreserveRows,
        elements: vec![],
        inherits: Vec::new(),
        if_not_exists: false,
    })
    .unwrap_err();
    assert!(matches!(err, ParseError::TempTableInNonTempSchema(_)));

    let err = crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
        schema_name: None,
        table_name: "t".into(),
        persistence: TablePersistence::Permanent,
        on_commit: OnCommitAction::DeleteRows,
        elements: vec![],
        inherits: Vec::new(),
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
            Plan::Append { children, .. } | Plan::SetOp { children, .. } => {
                children.iter().any(plan_contains_cte_scan)
            }
            Plan::Hash { input, .. }
            | Plan::Filter { input, .. }
            | Plan::OrderBy { input, .. }
            | Plan::Limit { input, .. }
            | Plan::Projection { input, .. }
            | Plan::Aggregate { input, .. }
            | Plan::WindowAgg { input, .. }
            | Plan::SubqueryScan { input, .. }
            | Plan::ProjectSet { input, .. } => plan_contains_cte_scan(input),
            Plan::NestedLoopJoin { left, right, .. } | Plan::HashJoin { left, right, .. } => {
                plan_contains_cte_scan(left) || plan_contains_cte_scan(right)
            }
            Plan::RecursiveUnion {
                anchor, recursive, ..
            } => plan_contains_cte_scan(anchor) || plan_contains_cte_scan(recursive),
            Plan::Result { .. }
            | Plan::SeqScan { .. }
            | Plan::IndexScan { .. }
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
            },
            TableConstraint::Unique {
                attributes: attrs(),
                columns: vec!["note".into(), "id".into()],
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
        }]
    );
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
            on_update: ForeignKeyAction::NoAction,
        }]
    );
    assert_eq!(
        ct.constraints().cloned().collect::<Vec<_>>(),
        vec![TableConstraint::ForeignKey {
            attributes: attrs(),
            columns: vec!["owner_name".into()],
            referenced_table: "people".into(),
            referenced_columns: Some(vec!["name".into()]),
            match_type: ForeignKeyMatchType::Simple,
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::NoAction,
        }]
    );
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
    let lowered = lower_create_table(&ct, &catalog_with_people_id_name_unique_index()).unwrap();
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
    let lowered = lower_create_table(&ct, &catalog_with_people_id_name_unique_index()).unwrap();
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
        Err(ParseError::FeatureNotSupported(feature))
            if feature == "FOREIGN KEY with cross-type columns"
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

    let Statement::DropDomain(drop_stmt) =
        parse_statement("drop domain if exists dom_int cascade").unwrap()
    else {
        panic!("expected drop domain");
    };
    assert!(drop_stmt.if_exists);
    assert!(drop_stmt.cascade);
    assert_eq!(drop_stmt.domain_name, "dom_int");

    let Statement::CommentOnDomain(comment) =
        parse_statement("comment on domain dom_int is 'hello'").unwrap()
    else {
        panic!("expected comment on domain");
    };
    assert_eq!(comment.domain_name, "dom_int");
    assert_eq!(comment.comment.as_deref(), Some("hello"));
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
    assert_eq!(create.handler_name.as_deref(), Some("pg_rust_test_fdw_handler"));
    assert_eq!(create.validator_name.as_deref(), Some("postgresql_fdw_validator"));
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
}

#[test]
fn parse_create_type_supports_enum_and_rejects_other_unsupported_forms() {
    assert!(matches!(
        parse_statement("create type myint"),
        Err(ParseError::FeatureNotSupported(feature))
            if feature == "shell types are not supported in CREATE TYPE"
    ));
    assert!(matches!(
        parse_statement("create type myint (input = myintin, output = myintout, like = int4)"),
        Err(ParseError::FeatureNotSupported(feature))
            if feature == "base type definitions are not supported in CREATE TYPE"
    ));
    match parse_statement("create type mood as enum ('sad', 'ok')").unwrap() {
        Statement::CreateType(CreateTypeStatement::Enum(stmt)) => {
            assert_eq!(stmt.schema_name, None);
            assert_eq!(stmt.type_name, "mood");
            assert_eq!(stmt.labels, vec!["sad", "ok"]);
        }
        other => panic!("expected enum create type, got {other:?}"),
    }
    match parse_statement(
        "create type intr as range (subtype = int4, subtype_diff = int4mi, collation = \"C\")",
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
            typlen: 4,
            typalign: AttributeAlign::Int,
            typstorage: AttributeStorage::Plain,
            typrelid: 0,
            typelem: 0,
            typarray: 0,
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
        Err(ParseError::UnexpectedToken { .. })
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
fn parse_aggregate_select() {
    let stmt = parse_select("select count(*) from people").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::AggCall {
            func: AggFunc::Count,
            args,
            distinct: false,
            ..
        } if args.is_empty()
    ));
    assert_eq!(stmt.targets[0].output_name, "count");
}

#[test]
fn parse_string_agg_select() {
    let stmt = parse_select("select string_agg(note, ',') from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::AggCall {
            func: AggFunc::StringAgg,
            args,
            distinct: false,
            ..
        } if args.len() == 2
    ));
    assert_eq!(stmt.targets[0].output_name, "string_agg");
}

#[test]
fn parse_jsonb_agg_with_local_order_by() {
    let stmt = parse_select("select jsonb_agg(id order by note desc, id) from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::AggCall {
            func: AggFunc::JsonbAgg,
            args,
            order_by,
            ..
        } if args.len() == 1
            && order_by.len() == 2
            && order_by[0].descending
            && matches!(order_by[0].expr, SqlExpr::Column(ref name) if name == "note")
            && matches!(order_by[1].expr, SqlExpr::Column(ref name) if name == "id")
    ));
}

#[test]
fn parse_aggregate_filter_clause() {
    let stmt = parse_select("select count(*) filter (where note is not null) from people").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::AggCall {
            func: AggFunc::Count,
            args,
            distinct: false,
            filter: Some(filter),
            ..
        } if args.is_empty()
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
        SqlExpr::AggCall {
            func: AggFunc::RangeIntersectAgg,
            args,
            distinct: false,
            ..
        } if args.len() == 1
    ));
    assert_eq!(stmt.targets[0].output_name, "range_intersect_agg");
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
                SqlExpr::AggCall {
                    func: AggFunc::Count,
                    args,
                    func_variadic: true,
                    ..
                } if args.len() == 1
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
            }),
            ..
        } if name == "row_number"
            && window_name.is_none()
            && partition_by.is_empty()
            && order_by.is_empty()
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::AggCall {
            func: AggFunc::Sum,
            over: Some(RawWindowSpec {
                name: window_name,
                partition_by,
                order_by,
            }),
            ..
        } if window_name.is_none() && partition_by.len() == 1 && order_by.len() == 1
    ));
}

#[test]
fn parse_named_window_clause_and_reference() {
    let stmt = parse_select("select row_number() over w from people window w as (order by id)")
        .unwrap();
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
            }),
            ..
        } if name == "row_number"
            && window_name == "w"
            && partition_by.is_empty()
            && order_by.is_empty()
    ));
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
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[]).unwrap();

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
    let stmt = parse_select("select row_number() over w from people window w as (order by id)")
        .unwrap();
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
        Err(ParseError::AggInWhere)
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
select '{"stack": [{"type": "expr", "env": {"+": "+", "-": "-", "*": "*", "/": "/", ">": ">", "<": "<", "=": "=", "head": "head", "tail": "tail", "cons": "cons", "empty": "empty"}, "expr": [["lambda", ["f"], ["f", "f", 1, 0, 0]], ["lambda", ["self", "a", "b", "i"], ["if", [">", "i", 10], ["empty"], ["cons", "a", ["self", "self", ["+", "a", "b"], "a", ["+", "i", 1]]]]]]}]}'::jsonb as state
union all
select
  case
    when frame_type = 'expr'
    then case
      when jsonb_typeof(expr) = 'number'
      then jsonb_build_object('stack', stack - 0, 'result', expr)
      when jsonb_typeof(expr) = 'string'
      then jsonb_build_object('stack', stack - 0, 'result', env -> expr_string)
      when op_string = 'if'
      then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_if', 'expr', expr, 'env', env)) || (stack - 0))
      when op_string = 'lambda'
      then jsonb_build_object('stack', stack - 0, 'result', jsonb_build_object('args', arg1, 'body', arg2, 'env', env))
      else jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', expr, 'done', '[]'::jsonb, 'env', env)) || (stack - 0))
    end
    when frame_type = 'eval_args'
    then case
      when result is null and jsonb_array_length(args_left) = 0
      then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_call', 'expr', args_done, 'env', env)) || (stack - 0))
      when result is null
      then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', args_left -> 0, 'env', env), jsonb_build_object('type', 'eval_args', 'left', args_left - 0, 'done', args_done, 'env', env)) || stack - 0)
      else jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', args_left, 'done', args_done || jsonb_build_array(result), 'env', env)) || (stack - 0))
    end
    when frame_type = 'eval_call'
    then case
      when op_string = '+'
      then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint + arg2::text::bigint)
      when op_string = '*'
      then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint * arg2::text::bigint)
      when op_string = '-'
      then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint - arg2::text::bigint)
      when op_string = '/'
      then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint / arg2::text::bigint)
      when op_string = '>'
      then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint > arg2::text::bigint)
      when op_string = '<'
      then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint < arg2::text::bigint)
      when op_string = '='
      then jsonb_build_object('stack', stack - 0, 'result', arg1 = arg2)
      when op_string = 'head'
      then jsonb_build_object('stack', stack - 0, 'result', arg1 -> 0)
      when op_string = 'tail'
      then jsonb_build_object('stack', stack - 0, 'result', arg1 - 0)
      when op_string = 'cons'
      then jsonb_build_object('stack', stack - 0, 'result', jsonb_build_array(arg1) || arg2)
      when op_string = 'empty'
      then jsonb_build_object('stack', stack - 0, 'result', '[]'::jsonb)
      else jsonb_build_object(
        'stack',
        jsonb_build_array(
          jsonb_build_object(
            'type', 'expr',
            'expr', (op -> 'body'),
            'env', (op -> 'env') || jsonb_build_object(
              coalesce(op -> 'args' ->> 0, 'null'), arg1,
              coalesce(op -> 'args' ->> 1, 'null'), arg2,
              coalesce(op -> 'args' ->> 2, 'null'), arg3,
              coalesce(op -> 'args' ->> 3, 'null'), arg4
            )
          )
        ) || (stack - 0)
      )
    end
    when frame_type = 'eval_if'
    then case
      when result is null
      then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg1, 'env', env)) || stack)
      when result is not null and result::text::boolean
      then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg2, 'env', env)) || (stack - 0))
      when result is not null and not result::text::boolean
      then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg3, 'env', env)) || (stack - 0))
    end
  end
from (
  select
    state -> 'stack' -> 0 ->> 'type' as frame_type,
    state -> 'stack' -> 0 -> 'expr' as expr,
    state -> 'stack' -> 0 ->> 'expr' as expr_string,
    state -> 'stack' -> 0 -> 'expr' -> 0 as op,
    state -> 'stack' -> 0 -> 'expr' ->> 0 as op_string,
    state -> 'stack' -> 0 -> 'expr' -> 1 as arg1,
    state -> 'stack' -> 0 -> 'expr' -> 2 as arg2,
    state -> 'stack' -> 0 -> 'expr' -> 3 as arg3,
    state -> 'stack' -> 0 -> 'expr' -> 4 as arg4,
    state -> 'stack' -> 0 -> 'left' as args_left,
    state -> 'stack' -> 0 -> 'done' as args_done,
    state -> 'stack' -> 0 -> 'env' as env,
    state -> 'result' as result,
    state -> 'stack' as stack
  from loop
) sub
)
select jsonb_pretty(state -> 'result')
from loop
where jsonb_array_length(state -> 'stack') = 0
limit 1"#,
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
fn named_window_errors_and_frames_are_rejected() {
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
    assert!(parse_select(
        "select row_number() over (order by id rows between unbounded preceding and current row) from people"
    )
    .is_err());
}

#[test]
fn parse_column_alias() {
    let stmt = parse_select("select count(*) as total from people").unwrap();
    assert_eq!(stmt.targets.len(), 1);
    assert_eq!(stmt.targets[0].output_name, "total");
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::AggCall {
            func: AggFunc::Count,
            args,
            distinct: false,
            ..
        } if args.is_empty()
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
        SqlExpr::AggCall {
            func: AggFunc::Count,
            args,
            distinct: true,
            ..
        } if args.len() == 1
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
    assert_eq!(args.len(), 3);
    assert_eq!(args[0].name.as_deref(), Some("target"));
    assert_eq!(args[1].name.as_deref(), Some("path"));
    assert_eq!(args[2].name.as_deref(), Some("silent"));
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
    match plan {
        Plan::FunctionScan {
            call: crate::include::nodes::primnodes::SetReturningCall::Unnest { output_columns, .. },
            ..
        } => {
            assert_eq!(output_columns.len(), 2);
            assert_eq!(
                output_columns[0].sql_type,
                SqlType::new(SqlTypeKind::Varchar)
            );
            assert_eq!(output_columns[1].sql_type, SqlType::new(SqlTypeKind::Int4));
        }
        other => panic!("expected unnest plan, got {other:?}"),
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
fn build_plan_for_select_list_json_each_is_rejected() {
    let stmt = parse_select("select json_each('{\"a\":1}'::json)").unwrap();
    let err = build_plan(&stmt, &catalog()).unwrap_err();
    assert!(matches!(err, ParseError::UnexpectedToken { .. }));
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
fn analyze_json_each_uses_pg_proc_out_metadata_for_output_columns() {
    let mut row = json_each_proc_row();
    row.proargnames = Some(vec![String::new(), "left_key".into(), "payload".into()]);
    let catalog = OverrideFunctionCatalog {
        base: catalog(),
        proc_rows: vec![row],
    };

    let stmt = parse_select("select * from json_each('{\"a\":1}'::json)").unwrap();
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[]).unwrap();

    assert_eq!(
        query_column_names_and_types(&query),
        vec![
            ("left_key".into(), SqlType::new(SqlTypeKind::Text)),
            ("payload".into(), SqlType::new(SqlTypeKind::Json)),
        ]
    );
}

#[test]
fn analyze_json_each_rejects_typed_column_definitions_for_out_parameters() {
    let stmt =
        parse_select("select * from json_each('{\"a\":1}'::json) as j(key text, value json)")
            .unwrap();
    let err = analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[])
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
    let err = analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[])
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
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[]).unwrap();

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
    let (query, _) = analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[]).unwrap();

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
    let err = analyze_select_query_with_outer(&stmt, &catalog, &[], None, &[], &[])
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
        analyze_select_query_with_outer(&stmt, &catalog_with_jpop(), &[], None, &[], &[]).unwrap();

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
        analyze_select_query_with_outer(&stmt, &catalog_with_jpop(), &[], None, &[], &[]).unwrap();

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
        analyze_select_query_with_outer(&stmt, &catalog_with_jpop(), &[], None, &[], &[]).unwrap();

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
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[]).unwrap();

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
    let err = analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[])
        .unwrap_err()
        .to_string();

    assert!(err.contains("function return row and query-specified return row do not match"));
}

#[test]
fn analyze_scalar_srf_rejects_typed_column_definitions() {
    let stmt = parse_select("select * from generate_series(1, 3) as g(val int4)").unwrap();
    let err = analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[])
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
    match plan {
        Plan::Projection { targets, .. } => {
            assert_eq!(targets.len(), 2);
            assert_eq!(targets[0].name, "x");
            assert_eq!(targets[1].name, "name");
        }
        other => panic!("expected projection, got {:?}", other),
    }
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
fn build_plan_values_mixed_nulls_infer_concrete_column_type() {
    let stmt = parse_select("select t.x from (values (null), (1), (2)) as t(x)").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Projection { input, .. } => match input.as_ref() {
            Plan::Values { output_columns, .. } => {
                assert_eq!(output_columns.len(), 1);
                assert_eq!(output_columns[0].sql_type, SqlType::new(SqlTypeKind::Int4));
            }
            other => panic!("expected values input, got {:?}", other),
        },
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
        SqlExpr::AggCall {
            func: AggFunc::JsonObjectAgg,
            args,
            distinct: false,
            ..
        } if args.len() == 2
    ));
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
        Statement::Begin
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
                        args: vec![SqlFunctionArg::positional(SqlExpr::Column("name".into()))],
                        func_variadic: false,
                        over: None,
                    },
                },
            ]
    ));

    assert!(matches!(
        parse_statement("delete from people where id = 1 returning people.*, id + 1 as next_id")
            .unwrap(),
        Statement::Delete(DeleteStatement {
            table_name,
            returning,
            ..
        }) if table_name == "people"
            && returning == vec![
                SelectItem {
                    output_name: "*".into(),
                    expr: SqlExpr::Column("people.*".into()),
                },
                SelectItem {
                    output_name: "next_id".into(),
                    expr: SqlExpr::Add(
                        Box::new(SqlExpr::Column("id".into())),
                        Box::new(SqlExpr::IntegerLiteral("1".into())),
                    ),
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
        SqlExpr::FuncCall { name, args, .. } if name == "btrim" && args.len() == 1
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "ltrim" && args.len() == 1
    ));
    assert!(matches!(
        &stmt.targets[2].expr,
        SqlExpr::FuncCall { name, args, .. } if name == "rtrim" && args.len() == 1
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
        analyze_select_query_with_outer(&stmt, &catalog(), &[], None, &[], &[]).unwrap();

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
