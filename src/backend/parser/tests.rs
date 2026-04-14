use super::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{AggFunc, Expr, Plan, RelationDesc, Value};
use crate::include::catalog::{PgRewriteRow, sort_pg_rewrite_rows};
use crate::include::nodes::parsenodes::{JoinTreeNode, RangeTblEntryKind};
use crate::include::nodes::primnodes::{JoinType, Var};

fn desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("name", SqlType::new(SqlTypeKind::Text), false),
            column_desc("note", SqlType::new(SqlTypeKind::Text), true),
        ],
    }
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
        row_type_oid: 60_000u32.saturating_add(rel_number),
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'r',
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

fn people_view_entry() -> CatalogEntry {
    CatalogEntry {
        rel: crate::RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 15020,
        },
        relation_oid: 50020,
        namespace_oid: 11,
        row_type_oid: 60020,
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'v',
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
            row_type_oid: 60010,
            reltoastrelid: 0,
            relpersistence: 'p',
            relkind: 'i',
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
        base.index_rows(),
        base.rewrite_rows(),
        base.am_rows(),
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
        base.index_rows(),
        base.rewrite_rows(),
        base.am_rows(),
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
        base.proc_rows(),
        base.cast_rows(),
        base.collation_rows(),
        base.database_rows(),
        base.tablespace_rows(),
        base.statistic_rows(),
        base.type_rows(),
    );
    crate::backend::utils::cache::visible_catalog::VisibleCatalog::new(relcache, Some(filtered))
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
                    name: "people".into()
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
            index_name: "num_exp_add_idx".into(),
            table_name: "num_exp_add".into(),
            using_method: None,
            columns: vec![
                IndexColumnDef {
                    name: "id1".into(),
                    collation: None,
                    opclass: None,
                    descending: false,
                    nulls_first: None,
                },
                IndexColumnDef {
                    name: "id2".into(),
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
fn parse_create_index_with_method_and_ordering() {
    let stmt = parse_statement(
        "create index num_exp_add_idx on num_exp_add using btree (id1 desc nulls first, id2 asc)",
    )
    .unwrap();
    assert_eq!(
        stmt,
        Statement::CreateIndex(CreateIndexStatement {
            unique: false,
            index_name: "num_exp_add_idx".into(),
            table_name: "num_exp_add".into(),
            using_method: Some("btree".into()),
            columns: vec![
                IndexColumnDef {
                    name: "id1".into(),
                    collation: None,
                    opclass: None,
                    descending: true,
                    nulls_first: Some(true),
                },
                IndexColumnDef {
                    name: "id2".into(),
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
            table_name: "items".into(),
            column: ColumnDef {
                name: "note".into(),
                ty: SqlType::new(SqlTypeKind::Text),
                default_expr: Some("'hello'".into()),
                nullable: true,
                primary_key: false,
                unique: false,
            },
        })
    );
}

#[test]
fn parse_alter_table_set_statement() {
    let stmt = parse_statement("alter table num_variance set (parallel_workers = 4)").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableSet(AlterTableSetStatement {
            table_name: "num_variance".into(),
            options: vec![RelOption {
                name: "parallel_workers".into(),
                value: "4".into(),
            }],
        })
    );
}

#[test]
fn parse_alter_table_rename_statement() {
    let stmt = parse_statement("alter table items rename to items_new").unwrap();
    assert_eq!(
        stmt,
        Statement::AlterTableRename(AlterTableRenameStatement {
            table_name: "items".into(),
            new_table_name: "items_new".into(),
        })
    );
}

#[test]
fn parse_unsupported_role_statement_into_placeholder() {
    let stmt = parse_statement("drop role if exists regress_alter_table_user1").unwrap();
    assert_eq!(
        stmt,
        Statement::Unsupported(UnsupportedStatement {
            sql: "drop role if exists regress_alter_table_user1".into(),
            feature: "DROP ROLE",
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
fn parse_expression_entrypoint_reuses_sql_expression_grammar() {
    let expr = parse_expr("1 + 2 * 3").unwrap();
    assert!(matches!(expr, SqlExpr::Add(_, _)));
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
            name: "people".into()
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
            }),
            right: Box::new(FromItem::Table {
                name: "pets".into(),
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
            }),
            right: Box::new(FromItem::Table {
                name: "pets".into(),
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
            }),
            alias: "s".into(),
            column_aliases: vec![],
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
            }),
            alias: "s".into(),
            column_aliases: vec![],
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
            }),
            alias: "p".into(),
            column_aliases: vec![],
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
        stmt.targets[0].expr,
        SqlExpr::Cast(_, ty) if ty == SqlType::new(SqlTypeKind::Oid)
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
        stmt.targets[3].expr,
        SqlExpr::Cast(_, ty) if ty == SqlType::new(SqlTypeKind::Date)
    ));
    assert!(matches!(
        stmt.targets[4].expr,
        SqlExpr::Cast(_, ty) if ty == SqlType::new(SqlTypeKind::TimestampTz)
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
            SqlExpr::Cast(_, ty) => assert_eq!(ty.kind, kind),
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
                }),
                alias: "p".into(),
                column_aliases: vec![],
                preserve_source_names: false,
            }),
            right: Box::new(FromItem::Alias {
                source: Box::new(FromItem::Table {
                    name: "pets".into(),
                }),
                alias: "q".into(),
                column_aliases: vec![],
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
            name: "people".into()
        })
    );
    assert!(stmt.targets.is_empty());
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
            match *input {
                Plan::NestedLoopJoin { on, .. } => assert!(matches!(
                    on,
                    Expr::Op(op) if op.op == crate::include::nodes::primnodes::OpExprKind::Eq
                )),
                other => panic!("expected join, got {:?}", other),
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
                assert!(matches!(
                    items[0].expr,
                    crate::backend::executor::Expr::Column(0)
                ));
            }
            other => panic!("expected order by, got {:?}", other),
        },
        other => panic!("expected projection, got {:?}", other),
    }
}

#[test]
fn parse_insert_update_delete() {
    assert!(matches!(
        parse_statement("explain select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: false,
            buffers: false,
            ..
        })
    ));
    assert!(matches!(
        parse_statement("explain analyze select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: true,
            buffers: false,
            ..
        })
    ));
    assert!(matches!(
        parse_statement("explain (analyze, buffers) select name from people").unwrap(),
        Statement::Explain(ExplainStatement {
            analyze: true,
            buffers: true,
            ..
        })
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
        matches!(parse_statement("drop table widgets").unwrap(), Statement::DropTable(DropTableStatement { if_exists: false, table_names }) if table_names == vec!["widgets"])
    );
    assert!(
        matches!(parse_statement("drop table if exists pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers").unwrap(), Statement::DropTable(DropTableStatement { if_exists: true, table_names }) if table_names == vec!["pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"])
    );
    assert!(
        matches!(parse_statement("create view item_names as select id, name from people").unwrap(), Statement::CreateView(CreateViewStatement { schema_name: None, view_name, query_sql, .. }) if view_name == "item_names" && query_sql == "select id, name from people")
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
        matches!(parse_statement("update people set note = 'x' where id = 1").unwrap(), Statement::Update(UpdateStatement { table_name, .. }) if table_name == "people")
    );
    assert!(
        matches!(parse_statement("delete from people where note is null").unwrap(), Statement::Delete(DeleteStatement { table_name, .. }) if table_name == "people")
    );
}

#[test]
fn bind_update_prefers_index_row_source_for_equality_predicate() {
    let catalog = catalog_with_people_id_index();
    let stmt = match parse_statement("update people set name = 'x' where id = 1").unwrap() {
        Statement::Update(stmt) => stmt,
        other => panic!("expected update statement, got {other:?}"),
    };
    let bound = bind_update(&stmt, &catalog).unwrap();
    match bound.row_source {
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
    assert!(matches!(bound.row_source, BoundModifyRowSource::Heap));
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
fn create_table_temp_name_validation() {
    let (name, persistence) =
        crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
            schema_name: Some("public".into()),
            table_name: "t".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![],
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
fn parse_create_table_primary_key_and_unique_constraints() {
    let stmt =
        parse_statement("create table items (id int4 primary key, note text unique)").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    let columns = ct.columns().collect::<Vec<_>>();
    assert_eq!(columns.len(), 2);
    assert!(columns[0].primary_key);
    assert!(columns[1].unique);
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
                columns: vec!["id".into(), "note".into()],
            },
            TableConstraint::Unique {
                columns: vec!["note".into(), "id".into()],
            },
        ]
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
        lower_create_table(&ct),
        Err(ParseError::UnexpectedToken { expected, .. }) if expected == "at most one PRIMARY KEY"
    ));

    let stmt = parse_statement("create table items (id int4, note text, unique (id, id))").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct),
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
        lower_create_table(&ct),
        Err(ParseError::UnknownColumn(name)) if name == "missing"
    ));

    let stmt = parse_statement("create table items (id int4 primary key, unique (id))").unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected create table");
    };
    assert!(matches!(
        lower_create_table(&ct),
        Err(ParseError::UnexpectedToken { expected, actual })
            if expected == "distinct PRIMARY KEY/UNIQUE definitions"
                && actual == "duplicate key definition on (id)"
    ));

    assert!(
        parse_statement("create table items (id int4, constraint named_pk primary key (id))")
            .is_err()
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
        stmt.targets[0].expr,
        SqlExpr::Cast(_, ty)
            if ty == SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
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
        Plan::Projection { input, .. } => match *input {
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
            other => panic!("expected ProjectSet input, got {other:?}"),
        },
        other => panic!("expected projection over project set, got {other:?}"),
    }
}

#[test]
fn build_plan_for_select_list_json_each_is_rejected() {
    let stmt = parse_select("select json_each('{\"a\":1}'::json)").unwrap();
    let err = build_plan(&stmt, &catalog()).unwrap_err();
    assert!(matches!(err, ParseError::UnexpectedToken { .. }));
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
            assert_eq!(column_aliases, &["val"]);
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
            assert_eq!(column_aliases, vec!["x", "y"]);
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
                name: "people".into()
            }),
            alias: "p".into(),
            column_aliases: vec![],
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
            assert!(matches!(*left, FromItem::Table { name } if name == "a"));
            match *right {
                FromItem::Join {
                    left,
                    right,
                    kind: JoinKind::Inner,
                    constraint: JoinConstraint::On(_),
                } => {
                    assert!(matches!(*left, FromItem::Table { name } if name == "b"));
                    assert!(matches!(*right, FromItem::Table { name } if name == "c"));
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
            assert_eq!(column_aliases, vec!["x"]);
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
    assert!(
        matches!(stmt.targets[0].expr, SqlExpr::Cast(_, ty) if ty.kind == SqlTypeKind::JsonPath)
    );
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
        Statement::Select(SelectStatement { from: Some(FromItem::Table { name }), .. })
            if name == "people"
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
