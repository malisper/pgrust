use super::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{AggFunc, Expr, Plan, RelationDesc, Value};

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
        relkind: 'r',
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

fn catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert("people", test_catalog_entry(15000, desc()));
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
            columns: vec!["id1".into(), "id2".into()],
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
        SqlExpr::FuncCall { ref name, ref args }
            if name == "position" && args.len() == 2
    ));
}

#[test]
fn parse_multiline_position_convert_from_expression() {
    let stmt = parse_select(
            "select position(\n convert_from('\\\\xbcf6c7d0', 'EUC_KR') in\n convert_from('\\\\xb0fac7d02c20bcf6c7d02c20b1e2bcfa2c20bbee', 'EUC_KR'))",
        )
        .unwrap();
    assert!(matches!(
        stmt.targets[0].expr,
        SqlExpr::FuncCall { ref name, ref args }
            if name == "position" && args.len() == 2
    ));
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
            on: Some(SqlExpr::Eq(
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
            on: None,
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
        SqlExpr::FuncCall { name, args } => {
            assert_eq!(name, "substring");
            assert_eq!(args.len(), 3);
        }
        other => panic!("expected substring call, got {other:?}"),
    }
    match &stmt.targets[1].expr {
        SqlExpr::FuncCall { name, args } => {
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
    let stmt = parse_select("select int2 '7', int4 '9', varchar(3) 'abc'").unwrap();
    assert_eq!(stmt.targets.len(), 3);
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
        SqlExpr::FuncCall { name, args } if name == "abs" && args.len() == 1
    ));
    assert!(matches!(
        &stmt.targets[1].expr,
        SqlExpr::FuncCall { name, args } if name == "sqrt" && args.len() == 1
    ));
    assert!(matches!(
        &stmt.targets[2].expr,
        SqlExpr::FuncCall { name, args } if name == "cbrt" && args.len() == 1
    ));
}

#[test]
fn parse_power_operator_and_in_list() {
    let stmt = parse_select("select x ^ '2.0', x in (0, 1, 2) from metrics").unwrap();
    assert!(matches!(
        &stmt.targets[0].expr,
        SqlExpr::FuncCall { name, args } if name == "power" && args.len() == 2
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
            assert_eq!(create.columns[0].ty, SqlType::new(SqlTypeKind::Numeric));
            assert_eq!(create.columns[1].ty, SqlType::new(SqlTypeKind::Numeric));
            assert_eq!(
                create.columns[2].ty,
                SqlType::with_numeric_precision_scale(10, 0)
            );
            assert_eq!(
                create.columns[3].ty,
                SqlType::with_numeric_precision_scale(12, 4)
            );
            assert_eq!(
                create.columns[4].ty,
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
            }),
            right: Box::new(FromItem::Alias {
                source: Box::new(FromItem::Table {
                    name: "pets".into(),
                }),
                alias: "q".into(),
                column_aliases: vec![],
            }),
            kind: JoinKind::Cross,
            on: None,
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
        &parse_select("select 'a' < 'b', 'a' >= 'b', true = false").unwrap(),
        &catalog(),
    )
    .is_ok());
}

#[test]
fn build_plan_rejects_missing_catalog_comparison_operator() {
    let err = build_plan(
        &parse_select("select '{\"a\":1}'::jsonb = '{\"a\":1}'::jsonb").unwrap(),
        &catalog(),
    )
    .unwrap_err();
    assert!(matches!(err, ParseError::UndefinedOperator { op: "=", .. }));
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
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
        "select people.name, pets.id from people join pets on people.id = pets.owner_id",
    )
    .unwrap();
    let plan = build_plan(&stmt, &catalog).unwrap();
    match plan {
        Plan::Projection { input, targets } => {
            assert_eq!(targets.len(), 2);
            match *input {
                Plan::NestedLoopJoin { on, .. } => assert!(matches!(on, Expr::Eq(_, _))),
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
        Plan::Projection { input, targets } => {
            assert_eq!(targets.len(), 1);
            match *input {
                Plan::Limit {
                    input,
                    limit,
                    offset,
                } => {
                    assert_eq!(limit, Some(2));
                    assert_eq!(offset, 1);
                    match *input {
                        Plan::OrderBy { input, items } => {
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
        matches!(parse_statement("create table widgets (id int4 not null, name text)").unwrap(), Statement::CreateTable(CreateTableStatement { table_name, columns, .. }) if table_name == "widgets" && columns.len() == 2)
    );
    assert!(
        matches!(parse_statement("create table pgbench_history(tid int,bid int,aid int,delta int,mtime timestamp,filler char(22))").unwrap(), Statement::CreateTable(CreateTableStatement { table_name, columns, .. }) if table_name == "pgbench_history" && columns.len() == 6)
    );
    assert!(
        matches!(parse_statement("create table pgbench_tellers(tid int not null,bid int,tbalance int,filler char(84)) with (fillfactor=100)").unwrap(), Statement::CreateTable(CreateTableStatement { table_name, columns, .. }) if table_name == "pgbench_tellers" && columns.len() == 4)
    );
    assert!(
        matches!(parse_statement("create temp table tempy ()").unwrap(), Statement::CreateTable(CreateTableStatement { persistence: TablePersistence::Temporary, table_name, columns, .. }) if table_name == "tempy" && columns.is_empty())
    );
    assert!(
        matches!(parse_statement("create temp table withoutoid() without oids").unwrap(), Statement::CreateTable(CreateTableStatement { persistence: TablePersistence::Temporary, table_name, columns, .. }) if table_name == "withoutoid" && columns.is_empty())
    );
    assert!(
        matches!(parse_statement("create temp table withoutoid() with (oids = false)").unwrap(), Statement::CreateTable(CreateTableStatement { persistence: TablePersistence::Temporary, table_name, columns, .. }) if table_name == "withoutoid" && columns.is_empty())
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
    assert!(matches!(
        parse_statement("show tables").unwrap(),
        Statement::ShowTables
    ));
}

#[test]
fn parse_create_table_with_varchar_types() {
    match parse_statement(
            "create table widgets (a varchar, b varchar(5), c character varying, d character varying(7))",
        )
        .unwrap()
        {
            Statement::CreateTable(CreateTableStatement { columns, .. }) => {
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
fn create_table_temp_name_validation() {
    let (name, persistence) =
        crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
            schema_name: Some("public".into()),
            table_name: "t".into(),
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            columns: vec![],
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
        columns: vec![],
        if_not_exists: false,
    })
    .unwrap_err();
    assert!(matches!(err, ParseError::TempTableInNonTempSchema(_)));

    let err = crate::backend::parser::normalize_create_table_name(&CreateTableStatement {
        schema_name: None,
        table_name: "t".into(),
        persistence: TablePersistence::Permanent,
        on_commit: OnCommitAction::DeleteRows,
        columns: vec![],
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
fn parse_create_table_with_array_types() {
    match parse_statement("create table widgets (a varchar[], b varchar(5)[], c int4[], d text[])")
        .unwrap()
    {
        Statement::CreateTable(CreateTableStatement { columns, .. }) => {
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
fn parse_array_and_unnest_expressions() {
    let stmt =
        parse_select("select * from unnest(ARRAY['a', 'b']::varchar[], ARRAY[1, 2])").unwrap();
    assert!(
        matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args }) if name == "unnest" && args.len() == 2)
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
            distinct: false
        } if args.is_empty()
    ));
    assert_eq!(stmt.targets[0].output_name, "count");
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
            distinct: false
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
            distinct: true
        } if args.len() == 1
    ));
}

#[test]
fn parse_generate_series() {
    let stmt = parse_select("select * from generate_series(1, 10)").unwrap();
    assert!(
        matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args }) if name == "generate_series" && args.len() == 2)
    );
}

#[test]
fn parse_generate_series_with_step() {
    let stmt = parse_select("select * from generate_series(1, 10, 2)").unwrap();
    assert!(
        matches!(stmt.from, Some(FromItem::FunctionCall { ref name, ref args }) if name == "generate_series" && args.len() == 3)
    );
}

#[test]
fn build_plan_for_unnest_uses_array_element_types() {
    let stmt = parse_select("select * from unnest(ARRAY['a']::varchar[], ARRAY[1, 2])").unwrap();
    let plan = build_plan(&stmt, &catalog()).unwrap();
    match plan {
        Plan::Unnest { output_columns, .. } => {
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
fn parse_srf_with_column_alias() {
    let stmt = parse_select("select * from generate_series(1, 3) as g(val)").unwrap();
    match &stmt.from {
        Some(FromItem::Alias {
            source,
            alias,
            column_aliases,
        }) => {
            let FromItem::FunctionCall { name, args } = source.as_ref() else {
                panic!("expected FunctionCall source, got {:?}", source);
            };
            assert_eq!(name, "generate_series");
            assert_eq!(args.len(), 2);
            assert_eq!(alias, "g");
            assert_eq!(column_aliases, &["val"]);
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
            ..
        }) => {
            assert_eq!(alias, "g");
            assert!(column_aliases.is_empty());
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
        }) => {
            assert_eq!(alias, "p");
            assert!(column_aliases.is_empty());
            assert!(matches!(*source, FromItem::DerivedTable(_)));
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
        }) => {
            assert_eq!(alias, "p");
            assert_eq!(column_aliases, vec!["x", "y"]);
            assert!(matches!(*source, FromItem::DerivedTable(_)));
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
            on,
        }) => {
            assert_eq!(kind, JoinKind::Inner);
            assert!(on.is_some());
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
            on,
        }) => {
            assert_eq!(kind, JoinKind::Cross);
            assert!(on.is_none());
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
            on: None,
        }) => {
            assert!(matches!(*left, FromItem::Table { name } if name == "a"));
            match *right {
                FromItem::Join {
                    left,
                    right,
                    kind: JoinKind::Inner,
                    on: Some(_),
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
        }) => {
            assert_eq!(alias, "t");
            assert_eq!(column_aliases, vec!["x"]);
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
        Err(ParseError::UnknownColumn(name)) if name == "p.id"
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
            distinct: false
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
            assert!(matches!(values[0][0], SqlExpr::CurrentTimestamp));
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
    let Statement::CreateTable(CreateTableStatement { columns, .. }) = stmt else {
        panic!("expected create table");
    };
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
    let mut catalog = catalog();
    catalog.insert("pets", pets_entry());
    let stmt = parse_select(
            "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id and exists (select 1 from people r where r.id = p.id))",
        )
        .unwrap();
    assert!(build_plan(&stmt, &catalog).is_ok());
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
