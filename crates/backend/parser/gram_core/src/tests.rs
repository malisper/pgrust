use crate::raw_parser;
use mcx::MemoryContext;
use parser_seams::RawParseMode;
use types_nodes::rawnodes::{A_Expr_Kind, ValUnion};
use types_nodes::{NodeList, RawStmt};

// One leaked context per test thread (mcx ACCT_POOL races on concurrent
// context drops across test threads; substrate issue, same as scan_fgram).
fn test_ctx() -> &'static MemoryContext {
    thread_local! {
        static CTX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("gram-test")));
    }
    CTX.with(|c| *c)
}

fn parse(input: &str) -> NodeList<'static> {
    raw_parser(test_ctx().mcx(), input, RawParseMode::RAW_PARSE_DEFAULT)
        .unwrap_or_else(|e| panic!("parse failed for {input:?}: {e:?}"))
}

fn parse_err(input: &str) -> Box<types_error::PgError> {
    match raw_parser(test_ctx().mcx(), input, RawParseMode::RAW_PARSE_DEFAULT) {
        Ok(_) => panic!("expected error for {input:?}"),
        Err(e) => e,
    }
}

fn only_stmt<'a>(list: &NodeList<'a>) -> &'a RawStmt<'a> {
    assert_eq!(list.len(), 1);
    list.nth(0).as_raw_stmt().expect("RawStmt")
}

fn select_of<'a>(rs: &RawStmt<'a>) -> &'a types_nodes::SelectStmt<'a> {
    rs.stmt.expect("stmt").as_select_stmt().expect("SelectStmt")
}

#[track_caller]
fn assert_bare_select(sel: &types_nodes::SelectStmt<'_>) {
    assert!(sel.distinctClause.is_none());
    assert!(sel.intoClause.is_none());
    assert!(sel.whereClause.is_none());
    assert!(sel.groupClause.is_nil());
    assert!(!sel.groupDistinct);
    assert!(sel.havingClause.is_none());
    assert!(sel.windowClause.is_nil());
    assert!(sel.sortClause.is_nil());
    assert!(sel.limitOffset.is_none() && sel.limitCount.is_none());
    assert!(sel.lockingClause.is_nil() && sel.withClause.is_none());
    assert!(sel.larg.is_none() && sel.rarg.is_none());
}

fn target_int<'a>(sel: &types_nodes::SelectStmt<'a>, i: usize) -> (Option<&'a str>, i32, i32, i32) {
    let rt = sel.targetList.nth(i).as_res_target().expect("ResTarget");
    let c = rt.val.expect("val").as_a_const().expect("A_Const");
    let Some(ValUnion::Integer(iv)) = c.val else { panic!("Integer") };
    (rt.name, iv.ival, c.location, rt.location)
}

#[test]
fn select_1() {
    let list = parse("SELECT 1;");
    let rs = only_stmt(&list);
    assert_eq!((rs.stmt_location, rs.stmt_len), (0, 8));
    let sel = select_of(rs);
    assert_bare_select(sel);
    assert!(sel.fromClause.is_nil());
    assert_eq!(sel.targetList.len(), 1);
    assert_eq!(target_int(sel, 0), (None, 1, 7, 7));
}

#[test]
fn select_1_as_x() {
    let list = parse("SELECT 1 AS x;");
    let sel = select_of(only_stmt(&list));
    assert_eq!(target_int(sel, 0), (Some("x"), 1, 7, 7));
}

#[test]
fn select_1_bare_label() {
    let list = parse("SELECT 1 x;");
    let sel = select_of(only_stmt(&list));
    assert_eq!(target_int(sel, 0), (Some("x"), 1, 7, 7));
}

#[test]
fn select_string() {
    let list = parse("SELECT 'foo';");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().unwrap();
    let c = rt.val.unwrap().as_a_const().unwrap();
    let Some(ValUnion::String(s)) = c.val else { panic!("String") };
    assert_eq!(s.sval, "foo");
    assert_eq!(c.location, 7);
}

#[test]
fn select_1_plus_2() {
    let list = parse("SELECT 1 + 2;");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().unwrap();
    let e = rt.val.unwrap().as_a_expr().expect("A_Expr");
    assert!(matches!(e.kind, A_Expr_Kind::AEXPR_OP));
    assert_eq!(e.location, 9);
    assert_eq!(e.name.len(), 1);
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "+");
    let l = e.lexpr.unwrap().as_a_const().unwrap();
    let r = e.rexpr.unwrap().as_a_const().unwrap();
    let (Some(ValUnion::Integer(li)), Some(ValUnion::Integer(ri))) = (l.val, r.val) else {
        panic!("int consts")
    };
    assert_eq!((li.ival, l.location, ri.ival, r.location), (1, 7, 2, 11));
    assert_eq!((e.rexpr_list_start, e.rexpr_list_end), (0, 0));
}

#[test]
fn select_from() {
    let list = parse("SELECT a FROM b;");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().unwrap();
    let cr = rt.val.unwrap().as_column_ref().expect("ColumnRef");
    assert_eq!(cr.fields.len(), 1);
    assert_eq!(cr.fields.nth(0).as_string().unwrap().sval, "a");
    assert_eq!(cr.location, 7);
    assert_eq!(sel.fromClause.len(), 1);
    let rv = sel.fromClause.nth(0).as_range_var().expect("RangeVar");
    assert_eq!(rv.relname, Some("b"));
    assert!(rv.catalogname.is_none() && rv.schemaname.is_none());
    assert!(rv.inh);
    assert_eq!(rv.relpersistence, b'p');
    assert!(rv.alias.is_none());
    assert_eq!(rv.location, 14);
}

#[test]
fn select_from_alias() {
    let list = parse("SELECT a FROM b AS c;");
    let sel = select_of(only_stmt(&list));
    let rv = sel.fromClause.nth(0).as_range_var().unwrap();
    assert_eq!(rv.alias.expect("alias").aliasname, Some("c"));
    assert!(rv.alias.unwrap().colnames.is_nil());
}

#[test]
fn multi_statement() {
    let list = parse("SELECT 1; SELECT 2;\nSELECT 3");
    assert_eq!(list.len(), 3);
    let locs: Vec<(i32, i32)> = (0..3)
        .map(|i| {
            let rs = list.nth(i).as_raw_stmt().unwrap();
            (rs.stmt_location, rs.stmt_len)
        })
        .collect();
    // C: stmt_location = statement start, stmt_len = distance to its ';';
    // the last statement keeps len 0 (runs to end of string).
    assert_eq!(locs, vec![(0, 8), (10, 18 - 10), (20, 0)]);
    let s3 = select_of(list.nth(2).as_raw_stmt().unwrap());
    assert_eq!(target_int(s3, 0).1, 3);
}

#[test]
fn empty_statements_discarded() {
    let list = parse(";;");
    assert!(list.is_nil());
    let list = parse("SELECT 1;;");
    assert_eq!(list.len(), 1);
    let rs = only_stmt(&list);
    assert_eq!((rs.stmt_location, rs.stmt_len), (0, 8));
}

#[test]
fn empty_input() {
    assert!(parse("").is_nil());
    assert!(parse("  -- comment\n").is_nil());
}

#[test]
fn syntax_error_message_and_position() {
    let e = parse_err("SELECT 1 1;");
    assert_eq!(e.message(), "syntax error at or near \"1\"");
    assert_eq!(e.cursor_position(), Some(10));

    let e = parse_err("SELECT FROM FROM");
    assert_eq!(e.message(), "syntax error at or near \"FROM\"");
    assert_eq!(e.cursor_position(), Some(13));

    let e = parse_err("SELECT 1 +");
    assert_eq!(e.message(), "syntax error at end of input");
    assert_eq!(e.cursor_position(), Some(11));

    let e = parse_err("SELECT 'foo' 'bar'");
    assert_eq!(e.message(), "syntax error at or near \"'bar'\"");
    assert_eq!(e.cursor_position(), Some(14));
}

#[test]
fn multiline_error_position() {
    let e = parse_err("SELECT\n1\n1;");
    assert_eq!(e.message(), "syntax error at or near \"1\"");
    assert_eq!(e.cursor_position(), Some(10));
}

#[test]
fn order_by_limit_offset() {
    let list = parse("SELECT a FROM t ORDER BY a DESC NULLS LAST, b LIMIT 10 OFFSET 2;");
    let sel = select_of(only_stmt(&list));
    assert_eq!(sel.sortClause.len(), 2);
    let s0 = sel.sortClause.nth(0).as_sort_by().expect("SortBy");
    assert_eq!(s0.sortby_dir, types_nodes::SortByDir::SORTBY_DESC);
    assert_eq!(s0.sortby_nulls, types_nodes::SortByNulls::SORTBY_NULLS_LAST);
    assert!(s0.useOp.is_nil());
    let s1 = sel.sortClause.nth(1).as_sort_by().unwrap();
    assert_eq!(s1.sortby_dir, types_nodes::SortByDir::SORTBY_DEFAULT);
    let count = sel.limitCount.expect("limitCount").as_a_const().unwrap();
    let Some(ValUnion::Integer(c)) = count.val else { panic!("Integer") };
    assert_eq!(c.ival, 10);
    let off = sel.limitOffset.expect("limitOffset").as_a_const().unwrap();
    let Some(ValUnion::Integer(o)) = off.val else { panic!("Integer") };
    assert_eq!(o.ival, 2);
    assert_eq!(sel.limitOption, types_nodes::LimitOption::LIMIT_OPTION_COUNT);
}

#[test]
fn count_star_func_call() {
    let list = parse("SELECT count(*) FROM t;");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().unwrap();
    let f = rt.val.unwrap().as_func_call().expect("FuncCall");
    assert_eq!(f.funcname.nth(0).as_string().unwrap().sval, "count");
    assert!(f.args.is_nil() && f.agg_star && !f.agg_distinct);
    assert!(f.agg_order.is_nil() && f.agg_filter.is_none() && f.over.is_none());
}

#[test]
fn typecast_and_bool_where() {
    let list = parse("SELECT 'x'::text FROM t WHERE a = 1 AND b IS NOT NULL AND c;");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().unwrap();
    let tc = rt.val.unwrap().as_type_cast().expect("TypeCast");
    let tn = tc.typeName.unwrap().as_type_name().expect("TypeName");
    assert_eq!(tn.names.nth(0).as_string().unwrap().sval, "text");
    assert_eq!(tn.typemod, -1);
    // AND flattens onto one BoolExpr (makeAndExpr).
    let w = sel.whereClause.unwrap().as_bool_expr().expect("BoolExpr");
    assert_eq!(w.boolop, types_nodes::BoolExprType::AND_EXPR);
    assert_eq!(w.args.len(), 3);
    let nt = w.args.nth(1).as_null_test().expect("NullTest");
    assert_eq!(nt.nulltesttype, types_nodes::NullTestType::IS_NOT_NULL);
    assert!(!nt.argisrow);
}

#[test]
fn distinct_clause_repr() {
    let list = parse("SELECT DISTINCT a FROM t;");
    let sel = select_of(only_stmt(&list));
    assert!(matches!(sel.distinctClause, types_nodes::DistinctClause::All));
    let list = parse("SELECT DISTINCT ON (a, b) a FROM t;");
    let sel = select_of(only_stmt(&list));
    let types_nodes::DistinctClause::On(ref l) = sel.distinctClause else { panic!("On") };
    assert_eq!(l.len(), 2);
}

#[test]
fn select_options_errors() {
    let e = parse_err("SELECT a FROM t LIMIT 1, 2;");
    assert_eq!(e.message(), "LIMIT #,# syntax is not supported");
    let e = parse_err("(SELECT a FROM t ORDER BY a) ORDER BY b;");
    assert_eq!(e.message(), "multiple ORDER BY clauses not allowed");
    assert_eq!(e.cursor_position(), Some(39));
    let e = parse_err("SELECT a FROM t FETCH FIRST 2 ROWS WITH TIES;");
    assert_eq!(e.message(), "WITH TIES cannot be specified without ORDER BY clause");
}

#[test]
fn insert_values_shapes() {
    let list = parse("INSERT INTO t VALUES (1, 2);");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().expect("InsertStmt");
    let rv = ins.relation.unwrap().as_range_var().expect("RangeVar");
    assert_eq!(rv.relname, Some("t"));
    assert!(rv.alias.is_none() && ins.cols.is_nil());
    assert!(ins.onConflictClause.is_none() && ins.returningClause.is_none());
    assert!(ins.withClause.is_none());
    let sel = ins.selectStmt.unwrap().as_select_stmt().expect("SelectStmt");
    assert_eq!(sel.valuesLists.len(), 1);
    assert_eq!(sel.valuesLists.nth(0).as_list().unwrap().len(), 2);

    let list = parse("INSERT INTO t AS x (a, b) VALUES (1, 2), (3, 4);");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    let rv = ins.relation.unwrap().as_range_var().unwrap();
    assert_eq!(rv.alias.unwrap().aliasname, Some("x"));
    assert_eq!(ins.cols.len(), 2);
    let col = ins.cols.nth(1).as_res_target().unwrap();
    assert_eq!(col.name, Some("b"));
    assert!(col.indirection.is_nil() && col.val.is_none());
    let sel = ins.selectStmt.unwrap().as_select_stmt().unwrap();
    assert_eq!(sel.valuesLists.len(), 2);

    let list = parse("INSERT INTO t DEFAULT VALUES;");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    assert!(ins.selectStmt.is_none() && ins.cols.is_nil());

    let list = parse("INSERT INTO t SELECT a FROM s;");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    assert!(ins.selectStmt.unwrap().as_select_stmt().unwrap().valuesLists.is_nil());
}

#[test]
fn returning_clause_shapes() {
    let list = parse("INSERT INTO t VALUES (1, 2) RETURNING id;");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().expect("InsertStmt");
    let ret = ins.returningClause.unwrap().as_returning_clause().expect("ReturningClause");
    assert!(ret.options.is_nil());
    assert_eq!(ret.exprs.len(), 1);
    let rt = ret.exprs.nth(0).as_res_target().expect("ResTarget");
    assert!(rt.name.is_none());
    let cr = rt.val.unwrap().as_column_ref().expect("ColumnRef");
    assert_eq!(cr.fields.nth(0).as_string().unwrap().sval, "id");

    let list = parse("UPDATE t SET a = 1 WHERE b = 2 RETURNING a, b + 1 AS c;");
    let upd = only_stmt(&list).stmt.unwrap().as_update_stmt().expect("UpdateStmt");
    let ret = upd.returningClause.unwrap().as_returning_clause().unwrap();
    assert_eq!(ret.exprs.len(), 2);
    assert_eq!(ret.exprs.nth(1).as_res_target().unwrap().name, Some("c"));

    let list = parse("DELETE FROM t WHERE a = 1 RETURNING *;");
    let del = only_stmt(&list).stmt.unwrap().as_delete_stmt().expect("DeleteStmt");
    let ret = del.returningClause.unwrap().as_returning_clause().unwrap();
    assert_eq!(ret.exprs.len(), 1);
    let rt = ret.exprs.nth(0).as_res_target().unwrap();
    let cr = rt.val.unwrap().as_column_ref().unwrap();
    assert!(cr.fields.nth(0).as_a_star().is_some());
}

#[test]
fn copy_stmt_to_file() {
    let list = parse("COPY foo TO '/tmp/x.dat'");
    let cs = only_stmt(&list).stmt.unwrap().as_copy_stmt().expect("CopyStmt");
    let rv = cs.relation.expect("relation").as_range_var().expect("RangeVar");
    assert_eq!(rv.relname, Some("foo"));
    assert!(!cs.is_from && !cs.is_program);
    assert_eq!(cs.filename, Some("/tmp/x.dat"));
    assert!(cs.attlist.is_nil() && cs.options.is_nil() && cs.whereClause.is_none());
    assert!(cs.query.is_none());
}

#[test]
fn copy_stmt_from_with_options() {
    let list = parse(
        "COPY s.foo (a, b) FROM '/tmp/x.dat' WITH (FORMAT text, DELIMITER '|', NULL 'NIL')",
    );
    let cs = only_stmt(&list).stmt.unwrap().as_copy_stmt().expect("CopyStmt");
    let rv = cs.relation.unwrap().as_range_var().unwrap();
    assert_eq!((rv.schemaname, rv.relname), (Some("s"), Some("foo")));
    assert!(cs.is_from);
    assert_eq!(cs.attlist.len(), 2);
    assert_eq!(cs.attlist.nth(0).as_string().unwrap().sval, "a");
    assert_eq!(cs.options.len(), 3);
    let d0 = cs.options.nth(0).as_def_elem().unwrap();
    assert_eq!(d0.defname, Some("format"));
    assert_eq!(d0.arg.unwrap().as_string().unwrap().sval, "text");
    let d1 = cs.options.nth(1).as_def_elem().unwrap();
    assert_eq!(d1.defname, Some("delimiter"));
    assert_eq!(d1.arg.unwrap().as_string().unwrap().sval, "|");
    let d2 = cs.options.nth(2).as_def_elem().unwrap();
    assert_eq!(d2.defname, Some("null"));
    assert_eq!(d2.arg.unwrap().as_string().unwrap().sval, "NIL");
}

#[test]
fn copy_stmt_legacy_options_and_stdin() {
    let list = parse("COPY foo FROM stdin DELIMITER '|' NULL ''");
    let cs = only_stmt(&list).stmt.unwrap().as_copy_stmt().expect("CopyStmt");
    assert!(cs.is_from && cs.filename.is_none());
    assert_eq!(cs.options.len(), 2);
    assert_eq!(cs.options.nth(0).as_def_elem().unwrap().defname, Some("delimiter"));
    assert_eq!(cs.options.nth(1).as_def_elem().unwrap().defname, Some("null"));

    let list = parse("COPY binary foo TO '/tmp/x'");
    let cs = only_stmt(&list).stmt.unwrap().as_copy_stmt().expect("CopyStmt");
    let d = cs.options.nth(0).as_def_elem().unwrap();
    assert_eq!(d.defname, Some("format"));
    assert_eq!(d.arg.unwrap().as_string().unwrap().sval, "binary");
}

#[test]
fn copy_stmt_query_form_and_errors() {
    let list = parse("COPY (SELECT 1) TO '/tmp/x'");
    let cs = only_stmt(&list).stmt.unwrap().as_copy_stmt().expect("CopyStmt");
    assert!(cs.relation.is_none());
    assert!(cs.query.unwrap().as_select_stmt().is_some());

    let e = parse_err("COPY foo TO PROGRAM STDOUT");
    assert!(format!("{e:?}").contains("STDIN/STDOUT not allowed with PROGRAM"), "{e:?}");
    let e = parse_err("COPY foo TO '/tmp/x' WHERE a > 1");
    assert!(format!("{e:?}").contains("WHERE clause not allowed with COPY TO"), "{e:?}");
}

fn vacuum_of<'a>(list: &NodeList<'a>) -> &'a types_nodes::parsenodes::VacuumStmt<'a> {
    only_stmt(list).stmt.unwrap().as_vacuum_stmt().expect("VacuumStmt")
}

#[test]
fn analyze_stmt_forms() {
    let list = parse("ANALYZE");
    let vs = vacuum_of(&list);
    assert!(!vs.is_vacuumcmd && vs.options.is_nil() && vs.rels.is_nil());

    let list = parse("ANALYZE t");
    let vs = vacuum_of(&list);
    assert!(!vs.is_vacuumcmd && vs.options.is_nil());
    assert_eq!(vs.rels.len(), 1);
    let vr = vs.rels.nth(0).as_vacuum_relation().expect("VacuumRelation");
    assert_eq!(vr.relation.unwrap().as_range_var().unwrap().relname, Some("t"));
    assert_eq!(vr.oid, 0);
    assert!(vr.va_cols.is_nil());

    let list = parse("ANALYZE VERBOSE t (a, b)");
    let vs = vacuum_of(&list);
    assert!(!vs.is_vacuumcmd);
    assert_eq!(vs.options.len(), 1);
    let d = vs.options.nth(0).as_def_elem().unwrap();
    assert_eq!(d.defname, Some("verbose"));
    assert!(d.arg.is_none());
    let vr = vs.rels.nth(0).as_vacuum_relation().unwrap();
    assert_eq!(vr.va_cols.len(), 2);
    assert_eq!(vr.va_cols.nth(0).as_string().unwrap().sval, "a");
    assert_eq!(vr.va_cols.nth(1).as_string().unwrap().sval, "b");
}

#[test]
fn analyze_stmt_parenthesized_options() {
    let list = parse("ANALYZE (VERBOSE) t");
    let vs = vacuum_of(&list);
    assert!(!vs.is_vacuumcmd);
    let d = vs.options.nth(0).as_def_elem().unwrap();
    assert_eq!(d.defname, Some("verbose"));
    assert!(d.arg.is_none());
    assert_eq!(vs.rels.len(), 1);

    let list = parse("ANALYZE (VERBOSE false) t");
    let vs = vacuum_of(&list);
    let d = vs.options.nth(0).as_def_elem().unwrap();
    assert_eq!(d.defname, Some("verbose"));
    assert_eq!(d.arg.unwrap().as_string().unwrap().sval, "false");
}

#[test]
fn vacuum_stmt_forms() {
    let list = parse("VACUUM t");
    let vs = vacuum_of(&list);
    assert!(vs.is_vacuumcmd && vs.options.is_nil());
    assert_eq!(vs.rels.len(), 1);

    let list = parse("VACUUM (ANALYZE) t");
    let vs = vacuum_of(&list);
    assert!(vs.is_vacuumcmd);
    let d = vs.options.nth(0).as_def_elem().unwrap();
    assert_eq!(d.defname, Some("analyze"));
    assert!(d.arg.is_none());

    let list = parse("VACUUM FULL FREEZE VERBOSE ANALYZE t");
    let vs = vacuum_of(&list);
    assert!(vs.is_vacuumcmd);
    let names: Vec<_> = (0..vs.options.len())
        .map(|i| vs.options.nth(i).as_def_elem().unwrap().defname.unwrap())
        .collect();
    assert_eq!(names, ["full", "freeze", "verbose", "analyze"]);
}

#[test]
fn vacuum_analyze_rule_numbers_match_tables() {
    use crate::tables::names::{YYRLINE, YYTNAME};
    use crate::tables::YYR1;
    for (rule, name, line) in [
        (1556, "VacuumStmt", 11908),
        (1557, "VacuumStmt", 11929),
        (1558, "AnalyzeStmt", 11940),
        (1559, "AnalyzeStmt", 11952),
        (1560, "utility_option_list", 11964),
        (1561, "utility_option_list", 11968),
        (1564, "utility_option_elem", 11980),
        (1566, "utility_option_name", 11988),
        (1567, "utility_option_name", 11989),
        (1568, "utility_option_arg", 11993),
        (1571, "opt_analyze", 11999),
        (1572, "opt_analyze", 12000),
        (1573, "opt_verbose", 12004),
        (1574, "opt_verbose", 12005),
        (1575, "opt_full", 12008),
        (1576, "opt_full", 12009),
        (1577, "opt_freeze", 12012),
        (1578, "opt_freeze", 12013),
        (1581, "vacuum_relation", 12022),
        (1582, "vacuum_relation_list", 12029),
        (1583, "vacuum_relation_list", 12031),
    ] {
        assert_eq!(YYTNAME[YYR1[rule] as usize], name, "rule {rule}");
        assert_eq!(YYRLINE[rule], line, "rule {rule}");
    }
}

#[test]
fn create_table_two_columns() {
    use types_nodes::rawnodes::{ColumnDef, CreateStmt, OnCommitAction, TypeName};
    let list = parse("CREATE TABLE t2 (a int4, b int8)");
    let rs = only_stmt(&list);
    let n = rs.stmt.expect("stmt").as_variant::<CreateStmt>().expect("CreateStmt");
    let rv = n.relation.expect("relation");
    assert_eq!(rv.relname, Some("t2"));
    assert_eq!(rv.relpersistence, b'p');
    assert!(n.inhRelations.is_nil() && n.options.is_nil() && !n.if_not_exists);
    assert!(n.partspec.is_none() && n.accessMethod.is_none() && n.tablespacename.is_none());
    assert_eq!(n.oncommit, OnCommitAction::ONCOMMIT_NOOP);
    assert_eq!(n.tableElts.len(), 2);
    let expect = [("a", "int4"), ("b", "int8")];
    for (i, (name, tyname)) in expect.iter().enumerate() {
        let cd = n.tableElts.nth(i).as_variant::<ColumnDef>().expect("ColumnDef");
        assert_eq!(cd.colname, Some(*name));
        assert!(cd.is_local && !cd.is_not_null && cd.constraints.is_nil());
        let tn = cd.typeName.expect("typeName").as_variant::<TypeName>().expect("TypeName");
        let last = tn.names.nth(tn.names.len() - 1).as_string().expect("name").sval;
        assert_eq!(last, *tyname);
    }
}

#[test]
fn with_clause_select() {
    use types_nodes::parsenodes::{CTEMaterialize, CommonTableExpr, WithClause};
    let list = parse("WITH x AS (SELECT 1) SELECT * FROM x");
    let rs = only_stmt(&list);
    let sel = rs.stmt.expect("stmt").as_select_stmt().expect("SelectStmt");
    let wc = sel
        .withClause
        .expect("withClause")
        .as_variant::<WithClause>()
        .expect("WithClause");
    assert!(!wc.recursive);
    assert_eq!(wc.location, 0);
    assert_eq!(wc.ctes.len(), 1);
    let cte = wc.ctes.nth(0).as_variant::<CommonTableExpr>().expect("CommonTableExpr");
    assert_eq!(cte.ctename, Some("x"));
    assert!(cte.aliascolnames.is_nil());
    assert_eq!(cte.ctematerialized, CTEMaterialize::CTEMaterializeDefault);
    assert_eq!(cte.location, 5);
    assert!(!cte.cterecursive && cte.cterefcount == 0);
    let cq = cte.ctequery.expect("ctequery").as_select_stmt().expect("SelectStmt");
    assert_eq!(cq.targetList.len(), 1);
    assert!(cte.search_clause.is_none() && cte.cycle_clause.is_none());
}

#[test]
fn with_clause_variants() {
    use types_nodes::parsenodes::{CTEMaterialize, CommonTableExpr, WithClause};
    let list = parse(
        "WITH RECURSIVE x (a, b) AS MATERIALIZED (SELECT 1, 2), \
         y AS NOT MATERIALIZED (SELECT 3) \
         SELECT a FROM x ORDER BY a LIMIT 2",
    );
    let rs = only_stmt(&list);
    let sel = rs.stmt.expect("stmt").as_select_stmt().expect("SelectStmt");
    assert_eq!(sel.sortClause.len(), 1);
    assert!(sel.limitCount.is_some());
    let wc = sel
        .withClause
        .expect("withClause")
        .as_variant::<WithClause>()
        .expect("WithClause");
    assert!(wc.recursive);
    assert_eq!(wc.ctes.len(), 2);
    let x = wc.ctes.nth(0).as_variant::<CommonTableExpr>().expect("cte");
    assert_eq!(x.ctename, Some("x"));
    assert_eq!(x.aliascolnames.len(), 2);
    assert_eq!(x.aliascolnames.nth(0).as_string().expect("colname").sval, "a");
    assert_eq!(x.ctematerialized, CTEMaterialize::CTEMaterializeAlways);
    let y = wc.ctes.nth(1).as_variant::<CommonTableExpr>().expect("cte");
    assert_eq!(y.ctematerialized, CTEMaterialize::CTEMaterializeNever);
}

#[test]
fn multiple_with_clauses_rejected() {
    let e = parse_err("WITH x AS (SELECT 1) (WITH y AS (SELECT 2) SELECT 1) SELECT 1");
    assert_eq!(e.message(), "multiple WITH clauses not allowed");
}

#[test]
fn join_on_shapes() {
    use types_nodes::JoinType;
    let list = parse("SELECT t1.g FROM t t1 JOIN t t2 ON t1.pk = t2.fk;");
    let sel = select_of(only_stmt(&list));
    assert_eq!(sel.fromClause.len(), 1);
    let j = sel.fromClause.nth(0).as_join_expr().expect("JoinExpr");
    assert_eq!(j.jointype, JoinType::JOIN_INNER);
    assert!(!j.isNatural && j.usingClause.is_nil() && j.join_using_alias.is_none());
    assert!(j.alias.is_none() && j.rtindex == 0);
    let l = j.larg.as_range_var().expect("larg RangeVar");
    assert_eq!(l.alias.unwrap().aliasname, Some("t1"));
    let e = j.quals.expect("ON quals").as_a_expr().expect("A_Expr");
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "=");

    let list = parse("SELECT * FROM a INNER JOIN b ON true;");
    let j = select_of(only_stmt(&list)).fromClause.nth(0).as_join_expr().unwrap();
    assert_eq!(j.jointype, JoinType::JOIN_INNER);
    assert!(j.quals.is_some());

    let list = parse("SELECT * FROM a CROSS JOIN b;");
    let j = select_of(only_stmt(&list)).fromClause.nth(0).as_join_expr().unwrap();
    assert_eq!(j.jointype, JoinType::JOIN_INNER);
    assert!(j.quals.is_none());

    let list = parse("SELECT * FROM (a JOIN b ON a.x = b.x) c;");
    let j = select_of(only_stmt(&list)).fromClause.nth(0).as_join_expr().unwrap();
    assert_eq!(j.alias.expect("alias").aliasname, Some("c"));

    let list = parse("SELECT * FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y;");
    let j = select_of(only_stmt(&list)).fromClause.nth(0).as_join_expr().unwrap();
    assert!(j.larg.as_join_expr().is_some());
    assert!(j.rarg.as_range_var().is_some());

    let list = parse("SELECT * FROM a LEFT OUTER JOIN b ON a.x = b.x;");
    let j = select_of(only_stmt(&list)).fromClause.nth(0).as_join_expr().unwrap();
    assert_eq!(j.jointype, JoinType::JOIN_LEFT);
    let list = parse("SELECT * FROM a RIGHT JOIN b ON a.x = b.x;");
    let j = select_of(only_stmt(&list)).fromClause.nth(0).as_join_expr().unwrap();
    assert_eq!(j.jointype, JoinType::JOIN_RIGHT);
    let list = parse("SELECT * FROM a FULL JOIN b ON a.x = b.x;");
    let j = select_of(only_stmt(&list)).fromClause.nth(0).as_join_expr().unwrap();
    assert_eq!(j.jointype, JoinType::JOIN_FULL);
}

#[test]
#[should_panic(expected = "JOIN USING unimplemented")]
fn join_using_is_loud() {
    let _ = parse("SELECT * FROM a JOIN b USING (x);");
}

#[test]
#[should_panic(expected = "NATURAL JOIN unimplemented")]
fn natural_join_is_loud() {
    let _ = parse("SELECT * FROM a NATURAL JOIN b;");
}

#[test]
fn in_subquery_shapes() {
    use types_nodes::{BoolExprType, SubLinkType};

    let list = parse("SELECT * FROM t1 WHERE pk IN (SELECT fk FROM t2);");
    let sel = select_of(only_stmt(&list));
    let sl = sel.whereClause.expect("WHERE").as_sub_link().expect("SubLink");
    assert_eq!(sl.subLinkType, SubLinkType::ANY_SUBLINK);
    assert_eq!(sl.subLinkId, 0);
    assert!(sl.operName.is_nil());
    assert!(sl.testexpr.expect("testexpr").as_column_ref().is_some());
    assert!(sl.subselect.as_select_stmt().is_some());

    let list = parse("SELECT * FROM t1 WHERE pk NOT IN (SELECT fk FROM t2);");
    let sel = select_of(only_stmt(&list));
    let b = sel.whereClause.expect("WHERE").as_bool_expr().expect("NOT");
    assert_eq!(b.boolop, BoolExprType::NOT_EXPR);
    let sl = b.args.nth(0).as_sub_link().expect("SubLink");
    assert_eq!(sl.subLinkType, SubLinkType::ANY_SUBLINK);
    assert!(sl.operName.is_nil());
    assert_eq!(b.location, sl.location);
}

fn set_of<'a>(rs: &RawStmt<'a>) -> &'a types_nodes::parsenodes::VariableSetStmt<'a> {
    rs.stmt.expect("stmt").as_variable_set_stmt().expect("VariableSetStmt")
}

#[test]
fn set_session_and_defaults() {
    use types_nodes::parsenodes::VariableSetKind::*;
    let list = parse("SET SESSION work_mem = '8MB';");
    let n = set_of(only_stmt(&list));
    assert_eq!((n.kind, n.name, n.is_local), (VAR_SET_VALUE, Some("work_mem"), false));

    let n = set_of(only_stmt(&parse("SET work_mem TO DEFAULT;")));
    assert_eq!((n.kind, n.name), (VAR_SET_DEFAULT, Some("work_mem")));
    let n = set_of(only_stmt(&parse("SET work_mem = DEFAULT;")));
    assert_eq!(n.kind, VAR_SET_DEFAULT);
    let n = set_of(only_stmt(&parse("SET work_mem FROM CURRENT;")));
    assert_eq!(n.kind, VAR_SET_CURRENT);

    let n = set_of(only_stmt(&parse("SET statement_timeout = 0;")));
    assert_eq!((n.kind, n.args.len()), (VAR_SET_VALUE, 1));
    let c = n.args.nth(0).as_a_const().unwrap();
    assert!(matches!(c.val, Some(ValUnion::Integer(i)) if i.ival == 0));

    let n = set_of(only_stmt(&parse("SET seed = -0.5;")));
    let c = n.args.nth(0).as_a_const().unwrap();
    assert!(matches!(c.val, Some(ValUnion::Float(f)) if f.fval == "-0.5"));
}

#[test]
fn set_session_authorization_forms() {
    use types_nodes::parsenodes::VariableSetKind::*;
    let n = set_of(only_stmt(&parse("SET SESSION AUTHORIZATION alice;")));
    assert_eq!((n.kind, n.name), (VAR_SET_VALUE, Some("session_authorization")));
    let c = n.args.nth(0).as_a_const().unwrap();
    assert!(matches!(c.val, Some(ValUnion::String(s)) if s.sval == "alice"));

    let n = set_of(only_stmt(&parse("SET SESSION AUTHORIZATION 'bob';")));
    let c = n.args.nth(0).as_a_const().unwrap();
    assert!(matches!(c.val, Some(ValUnion::String(s)) if s.sval == "bob"));

    let n = set_of(only_stmt(&parse("SET SESSION AUTHORIZATION DEFAULT;")));
    assert_eq!((n.kind, n.name), (VAR_SET_DEFAULT, Some("session_authorization")));
    let n = set_of(only_stmt(&parse("RESET SESSION AUTHORIZATION;")));
    assert_eq!((n.kind, n.name), (VAR_RESET, Some("session_authorization")));
}

#[test]
fn reset_and_show_forms() {
    use types_nodes::parsenodes::VariableSetKind::*;
    let n = set_of(only_stmt(&parse("RESET ALL;")));
    assert_eq!((n.kind, n.name), (VAR_RESET_ALL, None));
    let n = set_of(only_stmt(&parse("RESET TIME ZONE;")));
    assert_eq!((n.kind, n.name), (VAR_RESET, Some("timezone")));
    let n = set_of(only_stmt(&parse("RESET TRANSACTION ISOLATION LEVEL;")));
    assert_eq!(n.name, Some("transaction_isolation"));

    for (sql, want) in [
        ("SHOW ALL;", "all"),
        ("SHOW TIME ZONE;", "timezone"),
        ("SHOW TRANSACTION ISOLATION LEVEL;", "transaction_isolation"),
        ("SHOW SESSION AUTHORIZATION;", "session_authorization"),
    ] {
        let list = parse(sql);
        let rs = only_stmt(&list);
        let n = rs.stmt.unwrap().as_variable_show_stmt().expect("VariableShowStmt");
        assert_eq!(n.name, Some(want));
    }
}

fn target_expr<'a>(list: &NodeList<'a>) -> types_nodes::Node<'a> {
    let sel = select_of(only_stmt(list));
    sel.targetList.nth(0).as_res_target().expect("ResTarget").val.expect("val")
}

#[track_caller]
fn assert_system_func<'a>(
    f: &types_nodes::FuncCall<'a>,
    name: &str,
    nargs: usize,
) {
    assert_eq!(f.funcname.len(), 2);
    assert_eq!(f.funcname.nth(0).as_string().unwrap().sval, "pg_catalog");
    assert_eq!(f.funcname.nth(1).as_string().unwrap().sval, name);
    assert_eq!(f.args.len(), nargs);
    assert_eq!(f.funcformat, types_nodes::CoercionForm::COERCE_SQL_SYNTAX);
}

#[test]
fn at_time_zone_and_at_local() {
    let list = parse("SELECT x AT TIME ZONE 'UTC';");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "timezone", 2);
    // C arg order: (zone, operand).
    let z = f.args.nth(0).as_a_const().expect("A_Const");
    let Some(ValUnion::String(s)) = z.val else { panic!("String") };
    assert_eq!(s.sval, "UTC");
    assert!(f.args.nth(1).as_column_ref().is_some());
    assert_eq!(f.location, 9);

    let list = parse("SELECT x AT LOCAL;");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "timezone", 1);
    assert!(f.args.nth(0).as_column_ref().is_some());
    assert_eq!(f.location, -1);
}

#[test]
fn extract_shapes() {
    let list = parse("SELECT EXTRACT(EPOCH FROM x);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "extract", 2);
    let a = f.args.nth(0).as_a_const().expect("A_Const");
    let Some(ValUnion::String(s)) = a.val else { panic!("String") };
    assert_eq!(s.sval, "epoch");
    assert!(f.args.nth(1).as_column_ref().is_some());

    let list = parse("SELECT EXTRACT('timezone_hour' FROM x);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    let a = f.args.nth(0).as_a_const().expect("A_Const");
    let Some(ValUnion::String(s)) = a.val else { panic!("String") };
    assert_eq!(s.sval, "timezone_hour");

    for (sql, kw) in [
        ("SELECT EXTRACT(YEAR FROM x);", "year"),
        ("SELECT EXTRACT(MONTH FROM x);", "month"),
        ("SELECT EXTRACT(DAY FROM x);", "day"),
        ("SELECT EXTRACT(HOUR FROM x);", "hour"),
        ("SELECT EXTRACT(MINUTE FROM x);", "minute"),
        ("SELECT EXTRACT(SECOND FROM x);", "second"),
    ] {
        let list = parse(sql);
        let f = target_expr(&list).as_func_call().expect("FuncCall");
        let a = f.args.nth(0).as_a_const().expect("A_Const");
        let Some(ValUnion::String(s)) = a.val else { panic!("String") };
        assert_eq!(s.sval, kw);
    }
}

#[test]
fn set_time_zone() {
    use types_nodes::parsenodes::VariableSetKind::*;
    let n = set_of(only_stmt(&parse("SET TIME ZONE 'UTC';")));
    assert_eq!((n.kind, n.name, n.jumble_args), (VAR_SET_VALUE, Some("timezone"), true));
    let c = n.args.nth(0).as_a_const().unwrap();
    assert!(matches!(c.val, Some(ValUnion::String(s)) if s.sval == "UTC"));

    let n = set_of(only_stmt(&parse("SET TIME ZONE -7;")));
    let c = n.args.nth(0).as_a_const().unwrap();
    assert!(matches!(c.val, Some(ValUnion::Integer(i)) if i.ival == -7));

    let n = set_of(only_stmt(&parse("SET TIME ZONE DEFAULT;")));
    assert_eq!(n.kind, VAR_SET_DEFAULT);
    let n = set_of(only_stmt(&parse("SET TIME ZONE LOCAL;")));
    assert_eq!(n.kind, VAR_SET_DEFAULT);
}

fn xact_modes<'a>(n: &types_nodes::parsenodes::TransactionStmt<'a>) -> Vec<(&'a str, i32)> {
    n.options
        .iter()
        .map(|o| {
            let d = o.as_def_elem().expect("DefElem");
            let c = d.arg.expect("arg").as_a_const().expect("A_Const");
            let v = match c.val {
                Some(ValUnion::Integer(i)) => i.ival,
                Some(ValUnion::String(_)) => -1,
                _ => panic!("mode arg"),
            };
            (d.defname.unwrap(), v)
        })
        .collect()
}

#[test]
fn transaction_forms() {
    use types_nodes::parsenodes::TransactionStmtKind::*;
    let list = parse("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY, DEFERRABLE;");
    let n = only_stmt(&list).stmt.unwrap().as_transaction_stmt().unwrap();
    assert_eq!(n.kind, TRANS_STMT_BEGIN);
    assert_eq!(
        xact_modes(n),
        [("transaction_isolation", -1), ("transaction_read_only", 1), ("transaction_deferrable", 1)]
    );
    let iso = n.options.nth(0).as_def_elem().unwrap().arg.unwrap().as_a_const().unwrap();
    assert!(matches!(iso.val, Some(ValUnion::String(s)) if s.sval == "repeatable read"));

    let list = parse("START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE;");
    let n = only_stmt(&list).stmt.unwrap().as_transaction_stmt().unwrap();
    assert_eq!(n.kind, TRANS_STMT_START);
    assert_eq!(
        xact_modes(n),
        [("transaction_isolation", -1), ("transaction_read_only", 0)]
    );

    let n = only_stmt(&parse("END;")).stmt.unwrap().as_transaction_stmt().unwrap();
    assert_eq!((n.kind, n.chain), (TRANS_STMT_COMMIT, false));
    let n = only_stmt(&parse("END AND CHAIN;")).stmt.unwrap().as_transaction_stmt().unwrap();
    assert_eq!((n.kind, n.chain), (TRANS_STMT_COMMIT, true));
    let n = only_stmt(&parse("ABORT AND NO CHAIN;")).stmt.unwrap().as_transaction_stmt().unwrap();
    assert_eq!((n.kind, n.chain), (TRANS_STMT_ROLLBACK, false));

    let n = set_of(only_stmt(&parse("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")));
    assert_eq!(
        (n.kind, n.name, n.jumble_args),
        (types_nodes::parsenodes::VariableSetKind::VAR_SET_MULTI, Some("TRANSACTION"), true)
    );
    let n = set_of(only_stmt(&parse("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")));
    assert_eq!(n.name, Some("SESSION CHARACTERISTICS"));
    assert_eq!(xact_modes_of_set(n), [("transaction_read_only", 1)]);
}

fn xact_modes_of_set<'a>(
    n: &types_nodes::parsenodes::VariableSetStmt<'a>,
) -> Vec<(&'a str, i32)> {
    n.args
        .iter()
        .map(|o| {
            let d = o.as_def_elem().expect("DefElem");
            let c = d.arg.expect("arg").as_a_const().expect("A_Const");
            let v = match c.val {
                Some(ValUnion::Integer(i)) => i.ival,
                Some(ValUnion::String(_)) => -1,
                _ => panic!("mode arg"),
            };
            (d.defname.unwrap(), v)
        })
        .collect()
}

#[test]
fn discard_forms() {
    use types_nodes::parsenodes::DiscardMode::*;
    for (sql, want) in [
        ("DISCARD ALL;", DISCARD_ALL),
        ("DISCARD PLANS;", DISCARD_PLANS),
        ("DISCARD SEQUENCES;", DISCARD_SEQUENCES),
        ("DISCARD TEMP;", DISCARD_TEMP),
        ("DISCARD TEMPORARY;", DISCARD_TEMP),
    ] {
        let list = parse(sql);
        let n = only_stmt(&list).stmt.unwrap().as_discard_stmt().expect("DiscardStmt");
        assert_eq!(n.target, want, "{sql}");
    }
}

#[test]
fn listen_notify_unlisten() {
    let n = only_stmt(&parse("LISTEN ch;")).stmt.unwrap().as_listen_stmt().unwrap();
    assert_eq!(n.conditionname, Some("ch"));
    let n = only_stmt(&parse("UNLISTEN ch;")).stmt.unwrap().as_unlisten_stmt().unwrap();
    assert_eq!(n.conditionname, Some("ch"));
    let n = only_stmt(&parse("UNLISTEN *;")).stmt.unwrap().as_unlisten_stmt().unwrap();
    assert_eq!(n.conditionname, None);
    let n = only_stmt(&parse("NOTIFY ch;")).stmt.unwrap().as_notify_stmt().unwrap();
    assert_eq!((n.conditionname, n.payload), (Some("ch"), None));
    let n = only_stmt(&parse("NOTIFY ch, 'pay';")).stmt.unwrap().as_notify_stmt().unwrap();
    assert_eq!((n.conditionname, n.payload), (Some("ch"), Some("pay")));
}

#[test]
fn create_index_statements_parse() {
    for s in [
        "CREATE INDEX ON t (a)",
        "CREATE INDEX i ON t (a, b)",
        "CREATE UNIQUE INDEX i ON t (a)",
        "CREATE INDEX i ON t (a DESC NULLS LAST, b ASC)",
        "CREATE INDEX i ON t USING btree (a)",
        "CREATE INDEX IF NOT EXISTS i ON t (a)",
        "CREATE INDEX i ON t ((a + b))",
        "CREATE INDEX i ON t (a) WHERE a > 0",
        "CREATE INDEX i ON t (a COLLATE \"C\")",
        "CREATE INDEX i ON t (a text_pattern_ops)",
        "CREATE UNIQUE INDEX CONCURRENTLY i ON t (a)",
        "CREATE INDEX i ON t (a) INCLUDE (b)",
        "CREATE INDEX i ON t (a) WITH (fillfactor = 70)",
        "CREATE INDEX i ON t (a) TABLESPACE ts",
        "CREATE INDEX i ON t (lower(a))",
    ] {
        let l = parse(s);
        assert_eq!(l.len(), 1, "{s}");
    }
}

#[test]
fn sql_value_functions() {
    use types_nodes::primnodes::SQLValueFunctionOp as Op;

    for (sql, op, typmod) in [
        ("SELECT CURRENT_DATE;", Op::SVFOP_CURRENT_DATE, -1),
        ("SELECT CURRENT_TIME;", Op::SVFOP_CURRENT_TIME, -1),
        ("SELECT CURRENT_TIME(2);", Op::SVFOP_CURRENT_TIME_N, 2),
        ("SELECT CURRENT_TIMESTAMP;", Op::SVFOP_CURRENT_TIMESTAMP, -1),
        ("SELECT CURRENT_TIMESTAMP(3);", Op::SVFOP_CURRENT_TIMESTAMP_N, 3),
        ("SELECT LOCALTIME;", Op::SVFOP_LOCALTIME, -1),
        ("SELECT LOCALTIME(1);", Op::SVFOP_LOCALTIME_N, 1),
        ("SELECT LOCALTIMESTAMP;", Op::SVFOP_LOCALTIMESTAMP, -1),
        ("SELECT LOCALTIMESTAMP(6);", Op::SVFOP_LOCALTIMESTAMP_N, 6),
    ] {
        let list = parse(sql);
        let svf = target_expr(&list).as_sql_value_function().expect("SQLValueFunction");
        assert_eq!(svf.op, op, "{sql}");
        assert_eq!(svf.typmod, typmod, "{sql}");
        assert_eq!(svf.r#type, 0);
        assert_eq!(svf.location, 7);
    }
}
