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
