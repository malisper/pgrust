pub mod parsenodes;
pub mod gram;
pub mod analyze;

pub use parsenodes::*;
pub use gram::parse_statement;
pub use analyze::*;

pub fn parse_select(sql: &str) -> Result<SelectStatement, ParseError> {
    let stmt = parse_statement(sql)?;
    match stmt {
        Statement::Select(stmt) => Ok(stmt),
        other => Err(ParseError::UnexpectedToken {
            expected: "SELECT",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::tuple::{AttributeAlign, AttributeDesc};
    use crate::executor::{AggFunc, ColumnDesc, Expr, Plan, RelationDesc, ScalarType};

    fn desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                ColumnDesc {
                    name: "id".into(),
                    storage: AttributeDesc {
                        name: "id".into(),
                        attlen: 4,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Int32,
                },
                ColumnDesc {
                    name: "name".into(),
                    storage: AttributeDesc {
                        name: "name".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Text,
                },
                ColumnDesc {
                    name: "note".into(),
                    storage: AttributeDesc {
                        name: "note".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: true,
                    },
                    ty: ScalarType::Text,
                },
            ],
        }
    }

    fn catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            "people",
            CatalogEntry {
                rel: crate::RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: 15000,
                },
                desc: desc(),
            },
        );
        catalog
    }

    #[test]
    fn pest_matches_basic_select_keyword() {
        let result = gram::pest_parse_keyword(gram::Rule::kw_select_atom, "select").unwrap();
        assert_eq!(result, "select");
    }

    #[test]
    fn pest_matches_minimal_select_statement() {
        let stmt = parse_statement("select id from people").unwrap();
        match stmt {
            Statement::Select(stmt) => {
                assert_eq!(
                    stmt.from,
                    Some(FromItem::Table(TableRef {
                        name: "people".into(),
                        alias: None,
                    }))
                );
                assert_eq!(stmt.targets.len(), 1);
            }
            other => panic!("expected select statement, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let stmt =
            parse_select("select name, note from people where id > 1 and note is null").unwrap();
        assert_eq!(
            stmt.from,
            Some(FromItem::Table(TableRef {
                name: "people".into(),
                alias: None,
            }))
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
            Some(FromItem::InnerJoin {
                left: TableRef {
                    name: "people".into(),
                    alias: None,
                },
                right: TableRef {
                    name: "pets".into(),
                    alias: None,
                },
                on: SqlExpr::Eq(
                    Box::new(SqlExpr::Column("people.id".into())),
                    Box::new(SqlExpr::Column("pets.owner_id".into()))
                ),
            })
        );
    }

    #[test]
    fn parse_cross_join_select() {
        let stmt = parse_select("select people.name, pets.name from people, pets").unwrap();
        assert_eq!(
            stmt.from,
            Some(FromItem::CrossJoin {
                left: TableRef {
                    name: "people".into(),
                    alias: None,
                },
                right: TableRef {
                    name: "pets".into(),
                    alias: None,
                },
            })
        );
    }

    #[test]
    fn parse_table_alias() {
        let stmt = parse_select("select s.name from people s").unwrap();
        assert_eq!(stmt.targets[0].output_name, "name");
        assert_eq!(
            stmt.from,
            Some(FromItem::Table(TableRef {
                name: "people".into(),
                alias: Some("s".into()),
            }))
        );
    }

    #[test]
    fn parse_table_alias_with_as() {
        let stmt = parse_select("select s.name from people as s").unwrap();
        assert_eq!(stmt.targets[0].output_name, "name");
        assert_eq!(
            stmt.from,
            Some(FromItem::Table(TableRef {
                name: "people".into(),
                alias: Some("s".into()),
            }))
        );
    }

    #[test]
    fn parse_select_star_with_table_alias() {
        let stmt = parse_select("select * from people p").unwrap();
        assert_eq!(
            stmt.from,
            Some(FromItem::Table(TableRef {
                name: "people".into(),
                alias: Some("p".into()),
            }))
        );
        assert_eq!(stmt.targets[0].output_name, "*");
    }

    #[test]
    fn parse_select_alias_overrides_qualified_column_name() {
        let stmt = parse_select("select p.name as w from people p").unwrap();
        assert_eq!(stmt.targets[0].output_name, "w");
    }

    #[test]
    fn parse_cross_join_with_aliases() {
        let stmt = parse_select("select p.name, q.name from people p, pets q").unwrap();
        assert_eq!(
            stmt.from,
            Some(FromItem::CrossJoin {
                left: TableRef {
                    name: "people".into(),
                    alias: Some("p".into()),
                },
                right: TableRef {
                    name: "pets".into(),
                    alias: Some("q".into()),
                },
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
            Some(FromItem::Table(TableRef {
                name: "people".into(),
                alias: None,
            }))
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
        let stmt =
            parse_statement("update pgbench_accounts set abalance = abalance + -1822 where aid = 82711")
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
    fn parse_select_with_order_limit_offset() {
        let stmt =
            parse_select("select name from people order by id desc limit 2 offset 1").unwrap();
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
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 2);
                match *input {
                    Plan::Filter { input, predicate } => {
                        assert!(matches!(predicate, Expr::Gt(_, _)));
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
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 1);
                assert_eq!(targets[0].name, "name");
                match *input {
                    Plan::Filter { predicate, .. } => {
                        assert!(matches!(predicate, Expr::Gt(_, _)));
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
        catalog.insert("pets", CatalogEntry {
            rel: crate::RelFileLocator { spc_oid: 0, db_oid: 1, rel_number: 15001 },
            desc: RelationDesc {
                columns: vec![
                    ColumnDesc { name: "id".into(), storage: AttributeDesc { name: "id".into(), attlen: 4, attalign: AttributeAlign::Int, nullable: false }, ty: ScalarType::Int32 },
                    ColumnDesc { name: "owner_id".into(), storage: AttributeDesc { name: "owner_id".into(), attlen: 4, attalign: AttributeAlign::Int, nullable: false }, ty: ScalarType::Int32 },
                ],
            },
        });
        let stmt = parse_select("select people.name, pets.id from people join pets on people.id = pets.owner_id").unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 2);
                match *input { Plan::NestedLoopJoin { on, .. } => assert!(matches!(on, Expr::Eq(_, _))), other => panic!("expected join, got {:?}", other), }
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn unknown_column_is_rejected() {
        let stmt = parse_select("select missing from people").unwrap();
        assert!(matches!(build_plan(&stmt, &catalog()), Err(ParseError::UnknownColumn(name)) if name == "missing"));
    }

    #[test]
    fn select_star_expands_to_all_columns() {
        let stmt = parse_select("select * from people").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        // select * is an identity projection — optimized away to bare SeqScan
        assert!(matches!(plan, Plan::SeqScan { .. }),
            "expected SeqScan (identity projection elided), got {:?}", plan);
    }

    #[test]
    fn build_plan_wraps_order_by_and_limit() {
        let stmt = parse_select("select name from people where id > 0 order by id desc limit 2 offset 1").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 1);
                match *input {
                    Plan::Limit { input, limit, offset } => {
                        assert_eq!(limit, Some(2)); assert_eq!(offset, 1);
                        match *input { Plan::OrderBy { input, items } => { assert_eq!(items.len(), 1); assert!(items[0].descending); assert!(matches!(*input, Plan::Filter { .. })); } other => panic!("expected order by, got {:?}", other), }
                    }
                    other => panic!("expected limit, got {:?}", other),
                }
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn parse_insert_update_delete() {
        assert!(matches!(parse_statement("explain select name from people").unwrap(), Statement::Explain(ExplainStatement { analyze: false, buffers: false, .. })));
        assert!(matches!(parse_statement("explain analyze select name from people").unwrap(), Statement::Explain(ExplainStatement { analyze: true, buffers: false, .. })));
        assert!(matches!(parse_statement("explain (analyze, buffers) select name from people").unwrap(), Statement::Explain(ExplainStatement { analyze: true, buffers: true, .. })));
        assert!(matches!(parse_statement("insert into people (id, name) values (1, 'alice')").unwrap(), Statement::Insert(InsertStatement { table_name, .. }) if table_name == "people"));
        assert!(matches!(parse_statement("insert into people (id, name) values (1, 'alice'), (2, 'bob')").unwrap(), Statement::Insert(InsertStatement { table_name, values, .. }) if table_name == "people" && values.len() == 2));
        assert!(matches!(parse_statement("create table widgets (id int4 not null, name text)").unwrap(), Statement::CreateTable(CreateTableStatement { table_name, columns }) if table_name == "widgets" && columns.len() == 2));
        assert!(matches!(parse_statement("create table pgbench_history(tid int,bid int,aid int,delta int,mtime timestamp,filler char(22))").unwrap(), Statement::CreateTable(CreateTableStatement { table_name, columns }) if table_name == "pgbench_history" && columns.len() == 6));
        assert!(matches!(parse_statement("create table pgbench_tellers(tid int not null,bid int,tbalance int,filler char(84)) with (fillfactor=100)").unwrap(), Statement::CreateTable(CreateTableStatement { table_name, columns }) if table_name == "pgbench_tellers" && columns.len() == 4));
        assert!(matches!(parse_statement("drop table widgets").unwrap(), Statement::DropTable(DropTableStatement { if_exists: false, table_names }) if table_names == vec!["widgets"]));
        assert!(matches!(parse_statement("drop table if exists pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers").unwrap(), Statement::DropTable(DropTableStatement { if_exists: true, table_names }) if table_names == vec!["pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"]));
        assert!(matches!(parse_statement("truncate table pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers").unwrap(), Statement::TruncateTable(TruncateTableStatement { table_names }) if table_names == vec!["pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"]));
        assert!(matches!(parse_statement("truncate pgbench_history").unwrap(), Statement::TruncateTable(TruncateTableStatement { table_names }) if table_names == vec!["pgbench_history"]));
        assert!(matches!(parse_statement("vacuum pgbench_branches").unwrap(), Statement::Vacuum(VacuumStatement { table_names }) if table_names == vec!["pgbench_branches"]));
        assert!(matches!(parse_statement("update people set note = 'x' where id = 1").unwrap(), Statement::Update(UpdateStatement { table_name, .. }) if table_name == "people"));
        assert!(matches!(parse_statement("delete from people where note is null").unwrap(), Statement::Delete(DeleteStatement { table_name, .. }) if table_name == "people"));
        assert!(matches!(parse_statement("show tables").unwrap(), Statement::ShowTables));
    }

    #[test]
    fn parse_aggregate_select() {
        let stmt = parse_select("select count(*) from people").unwrap();
        assert_eq!(stmt.targets.len(), 1);
        assert!(matches!(stmt.targets[0].expr, SqlExpr::AggCall { func: AggFunc::Count, arg: None, distinct: false }));
        assert_eq!(stmt.targets[0].output_name, "count");
    }

    #[test]
    fn parse_group_by_and_having() {
        let stmt = parse_select("select name, count(*) from people group by name having count(*) > 1").unwrap();
        assert_eq!(stmt.group_by.len(), 1);
        assert!(matches!(stmt.group_by[0], SqlExpr::Column(ref name) if name == "name"));
        assert!(stmt.having.is_some());
    }

    #[test]
    fn build_plan_with_aggregate() {
        let stmt = parse_select("select name, count(*) from people group by name").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 2);
                assert_eq!(targets[0].name, "name");
                assert_eq!(targets[1].name, "count");
                assert!(matches!(*input, Plan::Aggregate { .. }));
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn ungrouped_column_rejected_at_plan_time() {
        let stmt = parse_select("select name, count(*) from people").unwrap();
        assert!(matches!(build_plan(&stmt, &catalog()), Err(ParseError::UngroupedColumn(name)) if name == "name"));
    }

    #[test]
    fn aggregate_in_where_rejected() {
        let stmt = parse_select("select name from people where count(*) > 1").unwrap();
        assert!(matches!(build_plan(&stmt, &catalog()), Err(ParseError::AggInWhere)));
    }

    #[test]
    fn parse_column_alias() {
        let stmt = parse_select("select count(*) as total from people").unwrap();
        assert_eq!(stmt.targets.len(), 1);
        assert_eq!(stmt.targets[0].output_name, "total");
        assert!(matches!(stmt.targets[0].expr, SqlExpr::AggCall { func: AggFunc::Count, arg: None, distinct: false }));
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
        assert!(matches!(stmt.targets[0].expr, SqlExpr::AggCall { func: AggFunc::Count, arg: Some(_), distinct: true }));
    }

    #[test]
    fn parse_generate_series() {
        let stmt = parse_select("select * from generate_series(1, 10)").unwrap();
        assert!(matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args, .. }) if name == "generate_series" && args.len() == 2));
    }

    #[test]
    fn parse_generate_series_with_step() {
        let stmt = parse_select("select * from generate_series(1, 10, 2)").unwrap();
        assert!(matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args, .. }) if name == "generate_series" && args.len() == 3));
    }

    #[test]
    fn parse_srf_with_column_alias() {
        let stmt = parse_select("select * from generate_series(1, 3) as g(val)").unwrap();
        match &stmt.from {
            Some(FromItem::FunctionCall { name, args, alias, column_aliases }) => {
                assert_eq!(name, "generate_series");
                assert_eq!(args.len(), 2);
                assert_eq!(alias.as_deref(), Some("g"));
                assert_eq!(column_aliases, &["val"]);
            }
            other => panic!("expected FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn parse_srf_with_table_alias_only() {
        let stmt = parse_select("select * from generate_series(1, 3) as g").unwrap();
        match &stmt.from {
            Some(FromItem::FunctionCall { alias, column_aliases, .. }) => {
                assert_eq!(alias.as_deref(), Some("g"));
                assert!(column_aliases.is_empty());
            }
            other => panic!("expected FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn parse_random_function() {
        let stmt = parse_select("select random()").unwrap();
        assert_eq!(stmt.targets.len(), 1);
        assert!(matches!(stmt.targets[0].expr, SqlExpr::Random));
        assert_eq!(stmt.targets[0].output_name, "random");
    }

    #[test]
    fn parse_current_timestamp() {
        let stmt =
            parse_statement("insert into pgbench_history (mtime) values (current_timestamp)")
                .unwrap();
        match stmt {
            Statement::Insert(InsertStatement { values, .. }) => {
                assert!(matches!(values[0][0], SqlExpr::CurrentTimestamp));
            }
            other => panic!("expected insert, got {:?}", other),
        }
    }
}
