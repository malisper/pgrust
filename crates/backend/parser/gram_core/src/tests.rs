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
fn on_conflict_shapes() {
    use types_nodes::OnConflictAction;

    let list = parse("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING;");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().expect("InsertStmt");
    let occ = ins.onConflictClause.unwrap().as_on_conflict_clause().expect("OnConflictClause");
    assert_eq!(occ.action, OnConflictAction::ONCONFLICT_NOTHING);
    assert!(occ.infer.is_none() && occ.targetList.is_nil() && occ.whereClause.is_none());

    let list =
        parse("INSERT INTO t VALUES (1) ON CONFLICT (a) DO UPDATE SET b = excluded.b WHERE t.c > 0;");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    let occ = ins.onConflictClause.unwrap().as_on_conflict_clause().unwrap();
    assert_eq!(occ.action, OnConflictAction::ONCONFLICT_UPDATE);
    let infer = occ.infer.unwrap().as_infer_clause().expect("InferClause");
    assert_eq!(infer.indexElems.len(), 1);
    let elem =
        infer.indexElems.nth(0).as_variant::<types_nodes::IndexElem>().expect("IndexElem");
    assert_eq!(elem.name, Some("a"));
    assert!(infer.whereClause.is_none() && infer.conname.is_none());
    assert_eq!(occ.targetList.len(), 1);
    let rt = occ.targetList.nth(0).as_res_target().expect("ResTarget");
    assert_eq!(rt.name, Some("b"));
    let cr = rt.val.unwrap().as_column_ref().expect("ColumnRef");
    assert_eq!(cr.fields.nth(0).as_string().unwrap().sval, "excluded");
    assert!(occ.whereClause.is_some());

    let list = parse("INSERT INTO t VALUES (1) ON CONFLICT (a) WHERE a > 0 DO NOTHING;");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    let occ = ins.onConflictClause.unwrap().as_on_conflict_clause().unwrap();
    let infer = occ.infer.unwrap().as_infer_clause().unwrap();
    assert!(infer.whereClause.is_some());

    let list = parse("INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING;");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    let occ = ins.onConflictClause.unwrap().as_on_conflict_clause().unwrap();
    let infer = occ.infer.unwrap().as_infer_clause().unwrap();
    assert!(infer.indexElems.is_nil());
    assert_eq!(infer.conname, Some("t_pkey"));
}

#[test]
fn on_conflict_rule_numbers_match_tables() {
    use crate::tables::names::{YYRLINE, YYTNAME};
    use crate::tables::YYR1;
    for (rule, name, line) in [
        (1630, "opt_on_conflict", 12328),
        (1631, "opt_on_conflict", 12338),
        (1632, "opt_on_conflict", 12348),
        (1633, "opt_conf_expr", 12354),
        (1634, "opt_conf_expr", 12363),
        (1635, "opt_conf_expr", 12372),
    ] {
        assert_eq!(YYTNAME[YYR1[rule] as usize], name, "rule {rule}");
        assert_eq!(YYRLINE[rule], line, "rule {rule}");
    }
}

#[test]
fn partition_cmd_rule_numbers_match_tables() {
    use crate::tables::names::{YYRLINE, YYTNAME};
    use crate::tables::YYR1;
    for (rule, name, line) in [
        (141, "opt_concurrently", 1134),
        (142, "opt_concurrently", 1135),
        (277, "AlterTableStmt", 2115),
        (278, "AlterTableStmt", 2125),
        (283, "AlterTableStmt", 2179),
        (298, "partition_cmd", 2326),
        (299, "partition_cmd", 2340),
        (300, "partition_cmd", 2353),
        (301, "index_partition_cmd", 2369),
        (393, "PartitionBoundSpec", 3147),
        (397, "hash_partbound_elem", 3241),
        (398, "hash_partbound", 3248),
        (399, "hash_partbound", 3252),
    ] {
        assert_eq!(YYTNAME[YYR1[rule] as usize], name, "rule {rule}");
        assert_eq!(YYRLINE[rule], line, "rule {rule}");
    }
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
fn cluster_reindex_rule_numbers_match_tables() {
    use crate::tables::names::{YYRLINE, YYTNAME};
    use crate::tables::YYR1;
    for (rule, name, line) in [
        (1263, "ReindexStmt", 9298),
        (1264, "ReindexStmt", 9311),
        (1265, "ReindexStmt", 9324),
        (1266, "reindex_target_relation", 9339),
        (1267, "reindex_target_relation", 9340),
        (1268, "reindex_target_all", 9343),
        (1269, "reindex_target_all", 9344),
        (1270, "opt_reindex_option_list", 9347),
        (1271, "opt_reindex_option_list", 9348),
        (1549, "ClusterStmt", 11838),
        (1550, "ClusterStmt", 11847),
        (1551, "ClusterStmt", 11857),
        (1552, "ClusterStmt", 11869),
        (1553, "ClusterStmt", 11881),
        (1554, "cluster_index_specification", 11895),
        (1555, "cluster_index_specification", 11896),
    ] {
        assert_eq!(YYTNAME[YYR1[rule] as usize], name, "rule {rule}");
        assert_eq!(YYRLINE[rule], line, "rule {rule}");
    }
}

#[test]
fn role_stmt_rule_numbers_match_tables() {
    use crate::tables::names::{YYRLINE, YYTNAME};
    use crate::tables::YYR1;
    for (rule, name, line) in [
        (147, "CreateRoleStmt", 1166),
        (151, "OptRoleList", 1189),
        (153, "AlterOptRoleList", 1194),
        (155, "AlterOptRoleElem", 1199),
        (156, "AlterOptRoleElem", 1204),
        (157, "AlterOptRoleElem", 1208),
        (158, "AlterOptRoleElem", 1218),
        (159, "AlterOptRoleElem", 1226),
        (160, "AlterOptRoleElem", 1230),
        (161, "AlterOptRoleElem", 1234),
        (162, "AlterOptRoleElem", 1239),
        (163, "AlterOptRoleElem", 1243),
        (165, "CreateOptRoleElem", 1293),
        (166, "CreateOptRoleElem", 1297),
        (167, "CreateOptRoleElem", 1301),
        (168, "CreateOptRoleElem", 1305),
        (169, "CreateOptRoleElem", 1309),
        (170, "CreateUserStmt", 1323),
        (171, "AlterRoleStmt", 1342),
        (172, "AlterRoleStmt", 1351),
        (175, "AlterRoleSetStmt", 1368),
        (176, "AlterRoleSetStmt", 1377),
        (177, "AlterRoleSetStmt", 1386),
        (178, "AlterRoleSetStmt", 1395),
        (179, "DropRoleStmt", 1417),
        (180, "DropRoleStmt", 1425),
        (181, "DropRoleStmt", 1433),
        (182, "DropRoleStmt", 1441),
        (183, "DropRoleStmt", 1449),
        (184, "DropRoleStmt", 1457),
        (185, "CreateGroupStmt", 1475),
        (186, "AlterGroupStmt", 1494),
        (187, "add_drop", 1506),
        (188, "add_drop", 1507),
        (217, "set_rest_more", 1754),
        (256, "SetResetClause", 1956),
        (916, "DropOwnedStmt", 6886),
        (917, "ReassignOwnedStmt", 6897),
        (1073, "GrantRoleStmt", 8012),
        (1074, "GrantRoleStmt", 8023),
        (1075, "RevokeRoleStmt", 8037),
        (1076, "RevokeRoleStmt", 8049),
        (1077, "grant_role_opt_list", 8067),
        (1078, "grant_role_opt_list", 8068),
        (1079, "grant_role_opt", 8072),
        (1080, "grant_role_opt_value", 8079),
        (1081, "grant_role_opt_value", 8080),
        (1082, "grant_role_opt_value", 8081),
        (2457, "RoleId", 17461),
        (2462, "role_list", 17544),
        (2463, "role_list", 17546),
    ] {
        assert_eq!(YYTNAME[YYR1[rule] as usize], name, "rule {rule}");
        assert_eq!(YYRLINE[rule], line, "rule {rule}");
    }
}

#[test]
fn cluster_statement_forms() {
    use types_nodes::parsenodes::{ClusterStmt, ReindexObjectType, ReindexStmt};
    let list = parse("CLUSTER t USING idx");
    let rs = only_stmt(&list);
    let n = rs.stmt.unwrap().as_variant::<ClusterStmt>().unwrap();
    assert_eq!(n.relation.unwrap().as_range_var().unwrap().relname, Some("t"));
    assert_eq!(n.indexname, Some("idx"));
    assert!(n.params.is_nil());

    let list = parse("CLUSTER VERBOSE");
    let rs = only_stmt(&list);
    let n = rs.stmt.unwrap().as_variant::<ClusterStmt>().unwrap();
    assert!(n.relation.is_none() && n.indexname.is_none());
    assert_eq!(n.params.nth(0).as_def_elem().unwrap().defname, Some("verbose"));

    let list = parse("CLUSTER idx ON t");
    let rs = only_stmt(&list);
    let n = rs.stmt.unwrap().as_variant::<ClusterStmt>().unwrap();
    assert_eq!(n.relation.unwrap().as_range_var().unwrap().relname, Some("t"));
    assert_eq!(n.indexname, Some("idx"));

    let list = parse("REINDEX INDEX i1");
    let rs = only_stmt(&list);
    let n = rs.stmt.unwrap().as_variant::<ReindexStmt>().unwrap();
    assert_eq!(n.kind, ReindexObjectType::REINDEX_OBJECT_INDEX);
    assert_eq!(n.relation.unwrap().as_range_var().unwrap().relname, Some("i1"));
    assert!(n.params.is_nil() && n.name.is_none());

    let list = parse("REINDEX (VERBOSE) TABLE t");
    let rs = only_stmt(&list);
    let n = rs.stmt.unwrap().as_variant::<ReindexStmt>().unwrap();
    assert_eq!(n.kind, ReindexObjectType::REINDEX_OBJECT_TABLE);
    assert_eq!(n.params.nth(0).as_def_elem().unwrap().defname, Some("verbose"));

    let list = parse("REINDEX DATABASE CONCURRENTLY d");
    let rs = only_stmt(&list);
    let n = rs.stmt.unwrap().as_variant::<ReindexStmt>().unwrap();
    assert_eq!(n.kind, ReindexObjectType::REINDEX_OBJECT_DATABASE);
    assert_eq!(n.name, Some("d"));
    assert_eq!(n.params.nth(0).as_def_elem().unwrap().defname, Some("concurrently"));
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
fn identity_generated_column_constraints() {
    use types_nodes::rawnodes::{ColumnDef, Constraint, ConstrType, CreateStmt};
    let list = parse(
        "CREATE TABLE t (id int GENERATED ALWAYS AS IDENTITY, \
         j bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 5), \
         a int, b int GENERATED ALWAYS AS (a * 2) STORED)",
    );
    let n = only_stmt(&list).stmt.unwrap().as_variant::<CreateStmt>().unwrap();
    assert_eq!(n.tableElts.len(), 4);

    let cd = n.tableElts.nth(0).as_variant::<ColumnDef>().unwrap();
    let c = cd.constraints.nth(0).as_variant::<Constraint>().unwrap();
    assert_eq!(c.contype, ConstrType::CONSTR_IDENTITY);
    assert_eq!(c.generated_when, b'a');
    assert!(c.options.is_nil() && c.raw_expr.is_none());

    let cd = n.tableElts.nth(1).as_variant::<ColumnDef>().unwrap();
    let c = cd.constraints.nth(0).as_variant::<Constraint>().unwrap();
    assert_eq!(c.contype, ConstrType::CONSTR_IDENTITY);
    assert_eq!(c.generated_when, b'd');
    assert_eq!(c.options.len(), 2);

    let cd = n.tableElts.nth(3).as_variant::<ColumnDef>().unwrap();
    let c = cd.constraints.nth(0).as_variant::<Constraint>().unwrap();
    assert_eq!(c.contype, ConstrType::CONSTR_GENERATED);
    assert_eq!(c.generated_when, b'a');
    assert_eq!(c.generated_kind, b's');
    assert!(c.raw_expr.is_some());

    // gram.y: generated columns only allow ALWAYS; VIRTUAL kind parses.
    let e = parse_err("CREATE TABLE t (b int GENERATED BY DEFAULT AS (1) STORED)");
    assert_eq!(e.message(), "for a generated column, GENERATED ALWAYS must be specified");
    assert_eq!(e.cursor_position(), Some(33));
    let list = parse("CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a) VIRTUAL)");
    let n = only_stmt(&list).stmt.unwrap().as_variant::<CreateStmt>().unwrap();
    let cd = n.tableElts.nth(1).as_variant::<ColumnDef>().unwrap();
    let c = cd.constraints.nth(0).as_variant::<Constraint>().unwrap();
    assert_eq!(c.generated_kind, b'v');
}

#[test]
fn insert_overriding_shapes() {
    use types_nodes::OverridingKind;
    let list = parse("INSERT INTO t (a) OVERRIDING SYSTEM VALUE VALUES (1)");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    assert_eq!(ins.r#override, OverridingKind::OVERRIDING_SYSTEM_VALUE);
    assert_eq!(ins.cols.len(), 1);
    assert!(ins.selectStmt.is_some());

    let list = parse("INSERT INTO t OVERRIDING USER VALUE VALUES (1)");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    assert_eq!(ins.r#override, OverridingKind::OVERRIDING_USER_VALUE);
    assert!(ins.cols.is_nil() && ins.selectStmt.is_some());

    let list = parse("INSERT INTO t VALUES (1)");
    let ins = only_stmt(&list).stmt.unwrap().as_insert_stmt().unwrap();
    assert_eq!(ins.r#override, OverridingKind::OVERRIDING_NOT_SET);
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
fn join_using_shapes() {
    use types_nodes::JoinType;
    let list = parse("SELECT * FROM a LEFT JOIN b USING (x, y) AS j;");
    let sel = select_of(only_stmt(&list));
    let j = sel.fromClause.nth(0).as_join_expr().expect("JoinExpr");
    assert_eq!(j.jointype, JoinType::JOIN_LEFT);
    assert!(!j.isNatural && j.quals.is_none());
    assert_eq!(j.usingClause.len(), 2);
    assert_eq!(j.usingClause.nth(0).as_string().unwrap().sval, "x");
    assert_eq!(j.usingClause.nth(1).as_string().unwrap().sval, "y");
    let jua = j.join_using_alias.expect("USING alias");
    assert_eq!(jua.aliasname, Some("j"));
    assert!(jua.colnames.is_nil());
}

#[test]
fn natural_join_shapes() {
    use types_nodes::JoinType;
    let list = parse("SELECT * FROM a NATURAL FULL OUTER JOIN b;");
    let sel = select_of(only_stmt(&list));
    let j = sel.fromClause.nth(0).as_join_expr().expect("JoinExpr");
    assert_eq!(j.jointype, JoinType::JOIN_FULL);
    assert!(j.isNatural && j.usingClause.is_nil() && j.join_using_alias.is_none());
    assert!(j.quals.is_none());
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

#[test]
fn subquery_op_sub_type_shapes() {
    use types_nodes::SubLinkType;

    // a_expr subquery_Op sub_type select_with_parens (rules 2078, 2263-2265).
    let list = parse("SELECT * FROM t1 WHERE pk = ANY (SELECT fk FROM t2);");
    let sel = select_of(only_stmt(&list));
    let sl = sel.whereClause.expect("WHERE").as_sub_link().expect("SubLink");
    assert_eq!(sl.subLinkType, SubLinkType::ANY_SUBLINK);
    assert_eq!(sl.operName.len(), 1);
    assert_eq!(sl.operName.nth(0).as_string().expect("op name").sval, "=");
    assert!(sl.testexpr.expect("testexpr").as_column_ref().is_some());

    let list = parse("SELECT * FROM t1 WHERE pk <> SOME (SELECT fk FROM t2);");
    let sel = select_of(only_stmt(&list));
    let sl = sel.whereClause.expect("WHERE").as_sub_link().expect("SubLink");
    assert_eq!(sl.subLinkType, SubLinkType::ANY_SUBLINK);
    assert_eq!(sl.operName.nth(0).as_string().expect("op name").sval, "<>");

    let list = parse("SELECT * FROM t1 WHERE pk < ALL (SELECT fk FROM t2);");
    let sel = select_of(only_stmt(&list));
    let sl = sel.whereClause.expect("WHERE").as_sub_link().expect("SubLink");
    assert_eq!(sl.subLinkType, SubLinkType::ALL_SUBLINK);
    assert_eq!(sl.operName.nth(0).as_string().expect("op name").sval, "<");
    assert!(sl.subselect.as_select_stmt().is_some());
}

#[test]
fn in_list_shapes() {
    use types_nodes::rawnodes::A_Expr_Kind;

    let list = parse("SELECT 1 WHERE 'r' IN ('r', 'p');");
    let sel = select_of(only_stmt(&list));
    let e = sel.whereClause.expect("WHERE").as_a_expr().expect("A_Expr");
    assert!(matches!(e.kind, A_Expr_Kind::AEXPR_IN));
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "=");
    assert!(e.lexpr.expect("lexpr").as_a_const().is_some());
    let items = e.rexpr.expect("rexpr").as_list().expect("List");
    assert_eq!(items.len(), 2);
    assert_eq!(e.location, 19);
    assert_eq!(e.rexpr_list_start, 22);
    assert_eq!(e.rexpr_list_end, 31);

    let list = parse("SELECT 1 WHERE 'r' NOT IN ('r', 'p');");
    let sel = select_of(only_stmt(&list));
    let e = sel.whereClause.expect("WHERE").as_a_expr().expect("A_Expr");
    assert!(matches!(e.kind, A_Expr_Kind::AEXPR_IN));
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "<>");
    assert_eq!(e.rexpr.expect("rexpr").as_list().expect("List").len(), 2);
    assert_eq!(e.location, 19);
    assert_eq!(e.rexpr_list_start, 26);
    assert_eq!(e.rexpr_list_end, 35);
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

#[test]
fn create_function_sql() {
    use types_nodes::rawnodes::TypeName;
    let list = parse("CREATE FUNCTION add1(int) RETURNS int AS 'select $1 + 1' LANGUAGE sql;");
    let rs = only_stmt(&list);
    let n = rs.stmt.expect("stmt").as_create_function_stmt().expect("CreateFunctionStmt");
    assert!(!n.is_procedure && !n.replace && n.sql_body.is_none());
    assert_eq!(n.funcname.len(), 1);
    assert_eq!(n.funcname.nth(0).as_string().expect("name").sval, "add1");
    assert_eq!(n.parameters.len(), 1);
    let p = n.parameters.nth(0).as_function_parameter().expect("FunctionParameter");
    assert!(p.name.is_none() && p.defexpr.is_none());
    assert_eq!(p.mode, types_nodes::parsenodes::FunctionParameterMode::FUNC_PARAM_DEFAULT);
    let pt = p.argType.expect("argType").as_variant::<TypeName>().expect("TypeName");
    assert_eq!(pt.names.nth(pt.names.len() - 1).as_string().expect("t").sval, "int4");
    let rt = n.returnType.expect("returnType").as_variant::<TypeName>().expect("TypeName");
    assert!(!rt.setof);
    assert_eq!(rt.names.nth(rt.names.len() - 1).as_string().expect("t").sval, "int4");
    assert_eq!(n.options.len(), 2);
    let as_el = n.options.nth(0).as_def_elem().expect("DefElem");
    assert_eq!(as_el.defname, Some("as"));
    let as_list = as_el.arg.expect("arg").as_list().expect("List");
    assert_eq!(as_list.len(), 1);
    assert_eq!(as_list.nth(0).as_string().expect("src").sval, "select $1 + 1");
    let lang = n.options.nth(1).as_def_elem().expect("DefElem");
    assert_eq!(lang.defname, Some("language"));
    assert_eq!(lang.arg.expect("arg").as_string().expect("lang").sval, "sql");
}

#[test]
fn create_or_replace_function_options() {
    let list = parse(
        "CREATE OR REPLACE FUNCTION f() RETURNS int AS 'select 1' LANGUAGE sql STRICT IMMUTABLE COST 100;",
    );
    let n = only_stmt(&list)
        .stmt
        .expect("stmt")
        .as_create_function_stmt()
        .expect("CreateFunctionStmt");
    assert!(n.replace && !n.is_procedure);
    assert!(n.parameters.is_nil());
    assert_eq!(n.options.len(), 5);
    let names: Vec<_> =
        n.options.iter().map(|o| o.as_def_elem().unwrap().defname.unwrap()).collect();
    assert_eq!(names, ["as", "language", "strict", "volatility", "cost"]);
    let strict = n.options.nth(2).as_def_elem().unwrap();
    assert!(strict.arg.expect("arg").as_boolean().expect("Boolean").boolval);
    let vol = n.options.nth(3).as_def_elem().unwrap();
    assert_eq!(vol.arg.expect("arg").as_string().expect("Str").sval, "immutable");
    let cost = n.options.nth(4).as_def_elem().unwrap();
    assert!(cost.arg.expect("arg").as_integer().is_some());
}

#[test]
fn create_function_param_modes_and_default() {
    use types_nodes::parsenodes::{FunctionParameter, FunctionParameterMode as M};
    let list = parse(
        "CREATE FUNCTION g(a int, OUT b int, VARIADIC c int[], IN d text DEFAULT 'x') \
         RETURNS int LANGUAGE sql AS 'select 1';",
    );
    let n = only_stmt(&list)
        .stmt
        .expect("stmt")
        .as_create_function_stmt()
        .expect("CreateFunctionStmt");
    assert_eq!(n.parameters.len(), 4);
    let modes = [M::FUNC_PARAM_DEFAULT, M::FUNC_PARAM_OUT, M::FUNC_PARAM_VARIADIC, M::FUNC_PARAM_IN];
    let names = ["a", "b", "c", "d"];
    for (i, (m, nm)) in modes.iter().zip(names).enumerate() {
        let p = n.parameters.nth(i).as_variant::<FunctionParameter>().expect("FunctionParameter");
        assert_eq!(p.mode, *m, "param {nm}");
        assert_eq!(p.name, Some(nm));
        assert_eq!(p.defexpr.is_some(), i == 3, "param {nm}");
    }
    let d = n.parameters.nth(3).as_variant::<FunctionParameter>().unwrap();
    let c = d.defexpr.expect("defexpr").as_a_const().expect("A_Const");
    assert!(matches!(c.val, Some(ValUnion::String(_))));
}

#[test]
fn create_procedure() {
    let list = parse("CREATE PROCEDURE p(x int) LANGUAGE sql AS 'select 1';");
    let n = only_stmt(&list)
        .stmt
        .expect("stmt")
        .as_create_function_stmt()
        .expect("CreateFunctionStmt");
    assert!(n.is_procedure && !n.replace);
    assert!(n.returnType.is_none());
    assert_eq!(n.parameters.len(), 1);
    assert_eq!(n.options.len(), 2);
}

#[test]
fn parses_sql_value_functions() {
    use types_nodes::SQLValueFunctionOp::*;
    for (sql, op) in [
        ("select current_user", SVFOP_CURRENT_USER),
        ("select session_user", SVFOP_SESSION_USER),
        ("select user", SVFOP_USER),
        ("select current_role", SVFOP_CURRENT_ROLE),
        ("select current_catalog", SVFOP_CURRENT_CATALOG),
        ("select current_schema", SVFOP_CURRENT_SCHEMA),
    ] {
        let list = parse(sql);
        let sel = select_of(only_stmt(&list));
        let rt = sel.targetList.nth(0).as_res_target().expect("ResTarget");
        let svf = rt.val.expect("val").as_sql_value_function().expect("SQLValueFunction");
        assert_eq!(svf.op, op, "{sql}");
        assert_eq!(svf.r#type, 0);
        assert_eq!(svf.typmod, -1);
        assert_eq!(svf.location, 7);
    }
}

#[test]
fn create_materialized_view_stmt() {
    use types_nodes::rawnodes::{CreateTableAsStmt, IntoClause};
    let list = parse("CREATE UNLOGGED MATERIALIZED VIEW IF NOT EXISTS s.mv (x) AS SELECT 1 WITH NO DATA");
    let rs = only_stmt(&list);
    let c = rs.stmt.expect("stmt").as_variant::<CreateTableAsStmt>().expect("CreateTableAsStmt");
    assert_eq!(c.objtype, types_nodes::parsenodes::ObjectType::OBJECT_MATVIEW);
    assert!(c.if_not_exists && !c.is_select_into);
    let into = c.into.expect("into").as_variant::<IntoClause>().expect("IntoClause");
    assert!(into.skipData);
    assert_eq!(into.colNames.len(), 1);
    let rv = into.rel.expect("rel").as_range_var().expect("RangeVar");
    assert_eq!(rv.schemaname, Some("s"));
    assert_eq!(rv.relpersistence, b'u');

    let list = parse("CREATE MATERIALIZED VIEW mv AS SELECT 1");
    let c = only_stmt(&list).stmt.unwrap().as_variant::<CreateTableAsStmt>().unwrap();
    let into = c.into.unwrap().as_variant::<IntoClause>().unwrap();
    assert!(!into.skipData && !c.if_not_exists);
    assert_eq!(into.rel.unwrap().as_range_var().unwrap().relpersistence, b'p');
}

#[test]
fn refresh_materialized_view_stmt() {
    use types_nodes::rawnodes::RefreshMatViewStmt;
    for (sql, concurrent, skip) in [
        ("REFRESH MATERIALIZED VIEW mv", false, false),
        ("REFRESH MATERIALIZED VIEW CONCURRENTLY mv", true, false),
        ("REFRESH MATERIALIZED VIEW mv WITH NO DATA", false, true),
        ("REFRESH MATERIALIZED VIEW mv WITH DATA", false, false),
    ] {
        let list = parse(sql);
        let r = only_stmt(&list).stmt.unwrap().as_variant::<RefreshMatViewStmt>().expect("RefreshMatViewStmt");
        assert_eq!(r.concurrent, concurrent, "{sql}");
        assert_eq!(r.skipData, skip, "{sql}");
        assert_eq!(r.relation.expect("relation").relname, Some("mv"));
    }
}

#[test]
fn parses_qualified_operator() {
    let list = parse("select 'a' operator(pg_catalog.~) 'b'");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().expect("ResTarget");
    let e = rt.val.expect("val").as_a_expr().expect("A_Expr");
    assert_eq!(e.kind, A_Expr_Kind::AEXPR_OP);
    assert_eq!(e.name.len(), 2);
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "pg_catalog");
    assert_eq!(e.name.nth(1).as_string().unwrap().sval, "~");
}

#[test]
fn parses_scalar_in_list() {
    let list = parse("select 1 where 'r' in ('r','p','')");
    let sel = select_of(only_stmt(&list));
    let e = sel.whereClause.expect("where").as_a_expr().expect("A_Expr");
    assert_eq!(e.kind, A_Expr_Kind::AEXPR_IN);
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "=");
    assert_eq!(e.rexpr.expect("rexpr").as_list().expect("List").len(), 3);
    assert!(e.rexpr_list_start > 0 && e.rexpr_list_end > e.rexpr_list_start);

    let list = parse("select 1 where 'r' not in ('r','p')");
    let sel = select_of(only_stmt(&list));
    let e = sel.whereClause.expect("where").as_a_expr().expect("A_Expr");
    assert_eq!(e.kind, A_Expr_Kind::AEXPR_IN);
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "<>");
}

#[test]
fn parses_op_any_array() {
    let list = parse("select 1 where 'x' = any(col)");
    let sel = select_of(only_stmt(&list));
    let e = sel.whereClause.expect("where").as_a_expr().expect("A_Expr");
    assert_eq!(e.kind, A_Expr_Kind::AEXPR_OP_ANY);
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "=");
    assert!(e.rexpr.expect("rexpr").as_column_ref().is_some());

    let list = parse("select 1 where 'x' <> all(col)");
    let sel = select_of(only_stmt(&list));
    let e = sel.whereClause.expect("where").as_a_expr().expect("A_Expr");
    assert_eq!(e.kind, A_Expr_Kind::AEXPR_OP_ALL);
}

#[test]
fn parses_subquery_op_sub_type_select() {
    let list = parse("select 1 where 'x' = any(select 'x')");
    let sel = select_of(only_stmt(&list));
    let sl = sel.whereClause.expect("where").as_sub_link().expect("SubLink");
    assert_eq!(sl.subLinkType, types_nodes::SubLinkType::ANY_SUBLINK);
    assert_eq!(sl.operName.nth(0).as_string().unwrap().sval, "=");
}

#[test]
fn parses_array_bounds() {
    let list = parse("select '1'::pg_catalog.int2[]");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().expect("ResTarget");
    let tc = rt.val.expect("val").as_type_cast().expect("TypeCast");
    let tn = tc.typeName.expect("typeName").as_type_name().expect("TypeName");
    assert_eq!(tn.arrayBounds.len(), 1);
    assert_eq!(tn.arrayBounds.nth(0).as_integer().expect("Integer").ival, -1);
    let list = parse("select '1'::int2[3]");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().expect("ResTarget");
    let tc = rt.val.expect("val").as_type_cast().expect("TypeCast");
    let tn = tc.typeName.expect("typeName").as_type_name().expect("TypeName");
    assert_eq!(tn.arrayBounds.nth(0).as_integer().expect("Integer").ival, 3);
}

#[test]
fn parses_union_all_values() {
    let list = parse("select 1 union all values ('16384'::pg_catalog.regclass)");
    let sel = select_of(only_stmt(&list));
    assert_eq!(sel.op, types_nodes::SetOperation::SETOP_UNION);
    assert!(sel.all);
    let larg = sel.larg.expect("larg");
    assert_eq!(larg.targetList.len(), 1);
    let rarg = sel.rarg.expect("rarg");
    assert_eq!(rarg.valuesLists.len(), 1);
}

#[test]
fn parses_collate_clause() {
    let list = parse("select '^(t)$' collate pg_catalog.default");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().expect("ResTarget");
    let cc = rt.val.expect("val").as_collate_clause().expect("CollateClause");
    assert_eq!(cc.collname.len(), 2);
    assert_eq!(cc.collname.nth(0).as_string().unwrap().sval, "pg_catalog");
    assert_eq!(cc.collname.nth(1).as_string().unwrap().sval, "default");
    assert!(cc.arg.expect("arg").as_a_const().is_some());
}

#[test]
fn parses_regex_operators() {
    for (sql, op) in [
        ("select 1 where nspname !~ '^pg_toast'", "!~"),
        ("select 1 where relname ~ 'x'", "~"),
    ] {
        let list = parse(sql);
        let sel = select_of(only_stmt(&list));
        let e = sel.whereClause.expect("where").as_a_expr().expect("A_Expr");
        assert_eq!(e.kind, A_Expr_Kind::AEXPR_OP);
        assert_eq!(e.name.nth(0).as_string().unwrap().sval, op);
    }
}

#[test]
fn constraint_statements_parse() {
    use types_nodes::rawnodes::{Constraint, ConstrType};
    for s in [
        "create table t (id int primary key, name text default 'x', qty int, check (qty > 0))",
        "create table u2 (a int unique)",
        "create table v (a int, b int, constraint foo primary key (a, b))",
        "create table w (a int, unique (a))",
        "create table x (a int primary key using index tablespace ts)",
        "create table y (a int unique nulls not distinct)",
        "alter table t add check (qty > 0)",
        "alter table t add constraint c1 unique (a)",
        "alter table t add primary key (a)",
    ] {
        let l = parse(s);
        assert_eq!(l.len(), 1, "{s}");
    }

    let l = parse("create table v (a int, b int, constraint foo primary key (a, b))");
    let cs = only_stmt(&l).stmt.unwrap().as_variant::<types_nodes::rawnodes::CreateStmt>().unwrap();
    let con = cs.tableElts.nth(2).as_variant::<Constraint>().expect("Constraint");
    assert_eq!(con.contype, ConstrType::CONSTR_PRIMARY);
    assert_eq!(con.conname, Some("foo"));
    assert_eq!(con.keys.len(), 2);
    assert_eq!(con.keys.nth(0).as_string().unwrap().sval, "a");
    assert!(!con.deferrable && !con.initdeferred);

    let l = parse("create table y (a int unique nulls not distinct)");
    let cs = only_stmt(&l).stmt.unwrap().as_variant::<types_nodes::rawnodes::CreateStmt>().unwrap();
    let cd = cs.tableElts.nth(0).as_variant::<types_nodes::rawnodes::ColumnDef>().unwrap();
    let con = cd.constraints.nth(0).as_variant::<Constraint>().unwrap();
    assert_eq!(con.contype, ConstrType::CONSTR_UNIQUE);
    assert!(con.nulls_not_distinct);
}

#[test]
fn check_constraint_not_enforced() {
    use types_nodes::rawnodes::{Constraint, ConstrType};
    // processCASbits: NOT ENFORCED clears is_enforced and implies NOT VALID.
    let l = parse("create table t (x int, constraint c check (x > 3) not enforced)");
    let cs = only_stmt(&l).stmt.unwrap().as_variant::<types_nodes::rawnodes::CreateStmt>().unwrap();
    let con = cs.tableElts.nth(1).as_variant::<Constraint>().expect("Constraint");
    assert_eq!(con.contype, ConstrType::CONSTR_CHECK);
    assert!(!con.is_enforced);
    assert!(con.skip_validation);
    assert!(!con.initially_valid);

    let l = parse("create table t (x int, check (x > 3) enforced)");
    let cs = only_stmt(&l).stmt.unwrap().as_variant::<types_nodes::rawnodes::CreateStmt>().unwrap();
    let con = cs.tableElts.nth(1).as_variant::<Constraint>().expect("Constraint");
    assert!(con.is_enforced);
    assert!(!con.skip_validation);
    assert!(con.initially_valid);

    let l = parse("alter table t add constraint c2 check (x > 10) not enforced");
    assert_eq!(l.len(), 1);
}

#[test]
fn create_view_stmt() {
    let list = parse("CREATE VIEW v1 AS SELECT t1.a, t2.d FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b > 10");
    let rs = only_stmt(&list);
    let v = rs.stmt.expect("stmt").as_variant::<types_nodes::rawnodes::ViewStmt>().expect("ViewStmt");
    assert_eq!(v.view.expect("view").relname, Some("v1"));
    assert!(!v.replace);
    assert!(v.aliases.is_nil() && v.options.is_nil());
    assert_eq!(v.withCheckOption, types_nodes::rawnodes::ViewCheckOption::NO_CHECK_OPTION);
    let sel = v.query.expect("query").as_select_stmt().expect("SelectStmt");
    assert_eq!(sel.targetList.len(), 2);
    assert!(sel.whereClause.is_some());
}

#[test]
fn create_or_replace_view_with_aliases() {
    let list = parse("CREATE OR REPLACE VIEW v2 (x, y) AS SELECT 1, 2");
    let rs = only_stmt(&list);
    let v = rs.stmt.expect("stmt").as_variant::<types_nodes::rawnodes::ViewStmt>().expect("ViewStmt");
    assert!(v.replace);
    assert_eq!(v.aliases.len(), 2);
    assert_eq!(v.aliases.nth(0).as_string().unwrap().sval, "x");
    assert_eq!(v.withCheckOption, types_nodes::rawnodes::ViewCheckOption::NO_CHECK_OPTION);
}

#[test]
fn create_view_with_check_option_kinds() {
    for (sql, want) in [
        ("CREATE VIEW vc AS SELECT 1 WITH CHECK OPTION",
         types_nodes::rawnodes::ViewCheckOption::CASCADED_CHECK_OPTION),
        ("CREATE VIEW vc AS SELECT 1 WITH CASCADED CHECK OPTION",
         types_nodes::rawnodes::ViewCheckOption::CASCADED_CHECK_OPTION),
        ("CREATE VIEW vc AS SELECT 1 WITH LOCAL CHECK OPTION",
         types_nodes::rawnodes::ViewCheckOption::LOCAL_CHECK_OPTION),
    ] {
        let list = parse(sql);
        let v = only_stmt(&list).stmt.unwrap()
            .as_variant::<types_nodes::rawnodes::ViewStmt>().expect("ViewStmt");
        assert_eq!(v.withCheckOption, want, "{sql}");
    }
}

#[test]
fn create_recursive_view_shapes() {
    use types_nodes::rawnodes::ViewStmt;
    // gram.y makeRecursiveViewSelect: query becomes WITH RECURSIVE vr (n) AS
    // (SELECT 1) SELECT n FROM vr.
    let list = parse("CREATE RECURSIVE VIEW vr (n) AS SELECT 1");
    let v = only_stmt(&list).stmt.unwrap().as_variant::<ViewStmt>().expect("ViewStmt");
    assert_eq!(v.view.expect("view").relname, Some("vr"));
    assert!(!v.replace);
    assert_eq!(v.aliases.len(), 1);
    assert_eq!(v.withCheckOption, types_nodes::rawnodes::ViewCheckOption::NO_CHECK_OPTION);
    let sel = v.query.expect("query").as_select_stmt().expect("SelectStmt");
    let w = sel.withClause.expect("withClause").as_with_clause().expect("WithClause");
    assert!(w.recursive);
    assert_eq!(w.location, -1);
    assert_eq!(w.ctes.len(), 1);
    let cte = w.ctes.nth(0).as_common_table_expr().expect("CommonTableExpr");
    assert_eq!(cte.ctename, Some("vr"));
    assert_eq!(cte.aliascolnames.len(), 1);
    assert_eq!(cte.aliascolnames.nth(0).as_string().unwrap().sval, "n");
    let inner = cte.ctequery.expect("ctequery").as_select_stmt().expect("SelectStmt");
    assert_eq!(inner.targetList.len(), 1);
    assert_eq!(sel.targetList.len(), 1);
    let rt = sel.targetList.nth(0).as_res_target().expect("ResTarget");
    assert!(rt.name.is_none());
    assert_eq!(rt.location, -1);
    let cr = rt.val.unwrap().as_column_ref().expect("ColumnRef");
    assert_eq!(cr.fields.nth(0).as_string().unwrap().sval, "n");
    let rv = sel.fromClause.nth(0).as_variant::<types_nodes::RangeVar>().expect("RangeVar");
    assert_eq!(rv.relname, Some("vr"));
    assert!(rv.schemaname.is_none());

    let list = parse("CREATE OR REPLACE TEMP RECURSIVE VIEW s.vr (a, b) AS SELECT 1, 2");
    let v = only_stmt(&list).stmt.unwrap().as_variant::<ViewStmt>().expect("ViewStmt");
    assert!(v.replace);
    let view_rv = v.view.expect("view");
    assert_eq!(view_rv.relname, Some("vr"));
    assert_eq!(view_rv.schemaname, Some("s"));
    assert_eq!(view_rv.relpersistence, types_core::catalog::RELPERSISTENCE_TEMP);
    assert_eq!(v.aliases.len(), 2);
    let sel = v.query.expect("query").as_select_stmt().expect("SelectStmt");
    assert_eq!(sel.targetList.len(), 2);
    let rt = sel.targetList.nth(1).as_res_target().unwrap();
    let cr = rt.val.unwrap().as_column_ref().expect("ColumnRef");
    assert_eq!(cr.fields.nth(0).as_string().unwrap().sval, "b");
    // The CTE name is the bare relname even for a qualified view name (C
    // passes view->relname only).
    let w = sel.withClause.unwrap().as_with_clause().unwrap();
    let cte = w.ctes.nth(0).as_common_table_expr().unwrap();
    assert_eq!(cte.ctename, Some("vr"));
}

#[test]
fn create_recursive_view_check_option_rejected() {
    let e = parse_err("CREATE RECURSIVE VIEW vr (n) AS SELECT 1 WITH CHECK OPTION");
    assert!(
        format!("{e:?}").contains("WITH CHECK OPTION not supported on recursive views"),
        "unexpected error: {e:?}"
    );
}

#[test]
fn create_rule_full_shape() {
    let list = parse(
        "CREATE OR REPLACE RULE r1 AS ON UPDATE TO s.t WHERE old.a = 1 DO INSTEAD \
         (INSERT INTO log VALUES (new.a); DELETE FROM log2)",
    );
    let rs = only_stmt(&list);
    let r = rs
        .stmt
        .expect("stmt")
        .as_variant::<types_nodes::rawnodes::RuleStmt>()
        .expect("RuleStmt");
    assert!(r.replace);
    assert_eq!(r.rulename, "r1");
    assert_eq!(r.event, types_nodes::nodes_enums::CmdType::CMD_UPDATE);
    assert!(r.instead);
    assert!(r.whereClause.is_some());
    let rel = r.relation.expect("relation");
    assert_eq!(rel.schemaname, Some("s"));
    assert_eq!(rel.relname, Some("t"));
    assert_eq!(r.actions.len(), 2);
    assert!(r.actions.nth(0).as_insert_stmt().is_some());
    assert!(r.actions.nth(1).as_delete_stmt().is_some());
}

#[test]
fn create_rule_events_and_nothing() {
    for (sql, event, instead, nact) in [
        ("CREATE RULE r AS ON INSERT TO t DO ALSO NOTHING",
         types_nodes::nodes_enums::CmdType::CMD_INSERT, false, 0),
        ("CREATE RULE r AS ON DELETE TO t DO INSTEAD NOTHING",
         types_nodes::nodes_enums::CmdType::CMD_DELETE, true, 0),
        ("CREATE RULE r AS ON SELECT TO t DO INSTEAD SELECT 1",
         types_nodes::nodes_enums::CmdType::CMD_SELECT, true, 1),
        ("CREATE RULE r AS ON UPDATE TO t DO UPDATE t2 SET a = 1",
         types_nodes::nodes_enums::CmdType::CMD_UPDATE, false, 1),
    ] {
        let list = parse(sql);
        let r = only_stmt(&list)
            .stmt
            .unwrap()
            .as_variant::<types_nodes::rawnodes::RuleStmt>()
            .expect("RuleStmt");
        assert_eq!(r.event, event, "{sql}");
        assert_eq!(r.instead, instead, "{sql}");
        assert_eq!(r.actions.len(), nact, "{sql}");
        assert!(!r.replace);
        assert!(r.whereClause.is_none());
    }
}

#[test]
fn drop_rule_shapes() {
    let list = parse("DROP RULE r1 ON s.t CASCADE");
    let d = only_stmt(&list)
        .stmt
        .unwrap()
        .as_drop_stmt()
        .expect("DropStmt");
    assert_eq!(d.removeType, types_nodes::parsenodes::ObjectType::OBJECT_RULE);
    assert!(!d.missing_ok);
    assert_eq!(d.behavior, types_nodes::parsenodes::DropBehavior::DROP_CASCADE);
    let names = d.objects.nth(0).as_list().expect("name list");
    assert_eq!(names.len(), 3);
    assert_eq!(names.nth(2).as_string().unwrap().sval, "r1");

    let list = parse("DROP RULE IF EXISTS r1 ON t");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    assert!(d.missing_ok);
    let names = d.objects.nth(0).as_list().expect("name list");
    assert_eq!(names.len(), 2);
    assert_eq!(names.nth(1).as_string().unwrap().sval, "r1");
}

#[test]
fn alter_table_enable_disable_rule() {
    use types_nodes::parsenodes::{AlterTableCmd, AlterTableType};
    for (sql, subtype, name) in [
        ("ALTER TABLE t ENABLE RULE r", AlterTableType::AT_EnableRule, "r"),
        ("ALTER TABLE t ENABLE ALWAYS RULE r", AlterTableType::AT_EnableAlwaysRule, "r"),
        ("ALTER TABLE t ENABLE REPLICA RULE r", AlterTableType::AT_EnableReplicaRule, "r"),
        ("ALTER TABLE t DISABLE RULE r", AlterTableType::AT_DisableRule, "r"),
    ] {
        let list = parse(sql);
        let a = only_stmt(&list)
            .stmt
            .unwrap()
            .as_variant::<types_nodes::parsenodes::AlterTableStmt>()
            .expect("AlterTableStmt");
        let cmd = a.cmds.nth(0).as_variant::<AlterTableCmd>().expect("AlterTableCmd");
        assert_eq!(cmd.subtype, subtype, "{sql}");
        assert_eq!(cmd.name, Some(name), "{sql}");
    }
}

#[test]
fn drop_function_shapes() {
    use types_nodes::parsenodes::{DropBehavior, ObjectType};
    let list = parse("DROP FUNCTION f(int, text), s.g() CASCADE");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    assert_eq!(d.removeType, ObjectType::OBJECT_FUNCTION);
    assert!(!d.missing_ok);
    assert_eq!(d.behavior, DropBehavior::DROP_CASCADE);
    assert_eq!(d.objects.len(), 2);
    let f = d.objects.nth(0).as_object_with_args().expect("ObjectWithArgs");
    assert!(!f.args_unspecified);
    assert_eq!(f.objname.len(), 1);
    assert_eq!(f.objname.nth(0).as_string().unwrap().sval, "f");
    assert_eq!(f.objargs.len(), 2);
    assert_eq!(f.objfuncargs.len(), 2);
    let g = d.objects.nth(1).as_object_with_args().expect("ObjectWithArgs");
    assert_eq!(g.objname.len(), 2);
    assert_eq!(g.objargs.len(), 0);
    assert!(!g.args_unspecified);

    let list = parse("DROP FUNCTION IF EXISTS f");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    assert!(d.missing_ok);
    let f = d.objects.nth(0).as_object_with_args().expect("ObjectWithArgs");
    assert!(f.args_unspecified);
    assert_eq!(f.objargs.len(), 0);

    let list = parse("DROP PROCEDURE p(OUT a int, INOUT b text, VARIADIC c int)");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    assert_eq!(d.removeType, ObjectType::OBJECT_PROCEDURE);
    let p = d.objects.nth(0).as_object_with_args().expect("ObjectWithArgs");
    assert_eq!(p.objfuncargs.len(), 3);
    assert_eq!(p.objargs.len(), 2);

    let list = parse("DROP AGGREGATE agg(int), agg2(*)");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    assert_eq!(d.removeType, ObjectType::OBJECT_AGGREGATE);
    let a = d.objects.nth(0).as_object_with_args().expect("ObjectWithArgs");
    assert_eq!(a.objargs.len(), 1);
    let a2 = d.objects.nth(1).as_object_with_args().expect("ObjectWithArgs");
    assert_eq!(a2.objargs.len(), 0);
    assert!(!a2.args_unspecified);
}

#[test]
fn oper_argtypes_none_shapes() {
    use types_nodes::parsenodes::ObjectType;
    use types_nodes::rawnodes::TypeName;
    // Left unary: NONE cell is None (C's NULL TypeName).
    let list = parse("DROP OPERATOR !!! (NONE, integer)");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    assert_eq!(d.removeType, ObjectType::OBJECT_OPERATOR);
    let o = d.objects.nth(0).as_object_with_args().expect("ObjectWithArgs");
    assert_eq!(o.objname.nth(0).as_string().unwrap().sval, "!!!");
    assert_eq!(o.objargs.len(), 2);
    assert!(o.objargs.nth(0).is_none());
    let r = o.objargs.nth(1).expect("right type").as_variant::<TypeName>().unwrap();
    assert_eq!(r.names.nth(1).as_string().unwrap().sval, "int4");

    // Right unary (postfix operators are gone, but the gram form remains).
    let list = parse("DROP OPERATOR IF EXISTS s.@#@ (bigint, NONE) CASCADE");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    assert!(d.missing_ok);
    let o = d.objects.nth(0).as_object_with_args().expect("ObjectWithArgs");
    assert_eq!(o.objname.len(), 2);
    assert!(o.objargs.nth(0).is_some());
    assert!(o.objargs.nth(1).is_none());

    // Binary form still carries two Some cells.
    let list = parse("DROP OPERATOR + (integer, integer)");
    let d = only_stmt(&list).stmt.unwrap().as_drop_stmt().expect("DropStmt");
    let o = d.objects.nth(0).as_object_with_args().expect("ObjectWithArgs");
    assert!(o.objargs.nth(0).is_some() && o.objargs.nth(1).is_some());

    let list = parse("COMMENT ON OPERATOR !!! (NONE, boolean) IS 'prefix'");
    let c = only_stmt(&list).stmt.unwrap().as_comment_stmt().expect("CommentStmt");
    assert_eq!(c.objtype, ObjectType::OBJECT_OPERATOR);
    let o = c.object.unwrap().as_object_with_args().expect("ObjectWithArgs");
    assert!(o.objargs.nth(0).is_none() && o.objargs.nth(1).is_some());

    // '(' Typename ')' is C's missing-argument syntax error.
    let e = parse_err("DROP OPERATOR !!! (integer)");
    assert!(format!("{e:?}").contains("missing argument"), "unexpected error: {e:?}");
}

#[test]
fn alter_materialized_view_shapes() {
    use types_nodes::parsenodes::{AlterTableStmt, ObjectType};
    let list = parse("ALTER MATERIALIZED VIEW mv OWNER TO r");
    let a = only_stmt(&list).stmt.unwrap().as_variant::<AlterTableStmt>().expect("AlterTableStmt");
    assert_eq!(a.objtype, ObjectType::OBJECT_MATVIEW);
    assert!(!a.missing_ok);
    assert_eq!(a.relation.expect("relation").relname, Some("mv"));
    assert_eq!(a.cmds.len(), 1);

    let list = parse("ALTER MATERIALIZED VIEW IF EXISTS s.mv SET (fillfactor = 70), CLUSTER ON idx");
    let a = only_stmt(&list).stmt.unwrap().as_variant::<AlterTableStmt>().expect("AlterTableStmt");
    assert_eq!(a.objtype, ObjectType::OBJECT_MATVIEW);
    assert!(a.missing_ok);
    let rel = a.relation.expect("relation");
    assert_eq!(rel.schemaname, Some("s"));
    assert_eq!(a.cmds.len(), 2);
}

#[test]
fn grant_all_in_schema_shapes() {
    use types_nodes::parsenodes::{GrantStmt, GrantTargetType, ObjectType};
    let list = parse("GRANT SELECT ON ALL TABLES IN SCHEMA s, s2 TO u");
    let g = only_stmt(&list).stmt.unwrap().as_variant::<GrantStmt>().expect("GrantStmt");
    assert!(g.is_grant);
    assert_eq!(g.targtype, GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA);
    assert_eq!(g.objtype, ObjectType::OBJECT_TABLE);
    assert_eq!(g.objects.len(), 2);

    let list = parse("REVOKE ALL ON ALL SEQUENCES IN SCHEMA s FROM u CASCADE");
    let g = only_stmt(&list).stmt.unwrap().as_variant::<GrantStmt>().expect("GrantStmt");
    assert!(!g.is_grant);
    assert_eq!(g.targtype, GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA);
    assert_eq!(g.objtype, ObjectType::OBJECT_SEQUENCE);
    assert_eq!(g.objects.len(), 1);
}

#[test]
fn alter_move_all_in_tablespace_shapes() {
    use types_nodes::parsenodes::{AlterTableMoveAllStmt, ObjectType};
    let list = parse("ALTER TABLE ALL IN TABLESPACE ts SET TABLESPACE ts2");
    let a = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<AlterTableMoveAllStmt>()
        .expect("AlterTableMoveAllStmt");
    assert_eq!(a.objtype, ObjectType::OBJECT_TABLE);
    assert_eq!(a.orig_tablespacename, Some("ts"));
    assert_eq!(a.new_tablespacename, Some("ts2"));
    assert!(a.roles.is_nil());
    assert!(!a.nowait);

    let list = parse("ALTER TABLE ALL IN TABLESPACE ts OWNED BY r1, r2 SET TABLESPACE ts2 NOWAIT");
    let a = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<AlterTableMoveAllStmt>()
        .expect("AlterTableMoveAllStmt");
    assert_eq!(a.objtype, ObjectType::OBJECT_TABLE);
    assert_eq!(a.roles.len(), 2);
    assert!(a.nowait);

    let list = parse("ALTER INDEX ALL IN TABLESPACE ts SET TABLESPACE ts2 NOWAIT");
    let a = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<AlterTableMoveAllStmt>()
        .expect("AlterTableMoveAllStmt");
    assert_eq!(a.objtype, ObjectType::OBJECT_INDEX);
    assert!(a.nowait);

    let list = parse("ALTER MATERIALIZED VIEW ALL IN TABLESPACE ts OWNED BY r SET TABLESPACE ts2");
    let a = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<AlterTableMoveAllStmt>()
        .expect("AlterTableMoveAllStmt");
    assert_eq!(a.objtype, ObjectType::OBJECT_MATVIEW);
    assert_eq!(a.roles.len(), 1);
    assert_eq!(a.orig_tablespacename, Some("ts"));
    assert_eq!(a.new_tablespacename, Some("ts2"));
}

#[test]
fn gram_train_3_rule_numbers_match_tables() {
    use crate::tables::names::{YYRLINE, YYTNAME};
    use crate::tables::YYR1;
    for (rule, name, line) in [
        (1062, "privilege_target", 7941),
        (1063, "privilege_target", 7950),
        (279, "AlterTableStmt", 2135),
        (280, "AlterTableStmt", 2147),
        (284, "AlterTableStmt", 2189),
        (285, "AlterTableStmt", 2201),
        (292, "AlterTableStmt", 2273),
        (293, "AlterTableStmt", 2285),
    ] {
        assert_eq!(YYTNAME[YYR1[rule] as usize], name, "rule {rule}");
        assert_eq!(YYRLINE[rule], line, "rule {rule}");
    }
}

#[test]
fn gram_train_2_rule_numbers_match_tables() {
    use crate::tables::names::{YYRLINE, YYTNAME};
    use crate::tables::YYR1;
    for (rule, name, line) in [
        (1235, "oper_argtypes", 9104),
        (1236, "oper_argtypes", 9106),
        (1237, "oper_argtypes", 9108),
        (1490, "ViewStmt", 11314),
        (1491, "ViewStmt", 11333),
        (290, "AlterTableStmt", 2253),
        (291, "AlterTableStmt", 2263),
    ] {
        assert_eq!(YYTNAME[YYR1[rule] as usize], name, "rule {rule}");
        assert_eq!(YYRLINE[rule], line, "rule {rule}");
    }
}

#[test]
fn comment_on_function_shape() {
    use types_nodes::parsenodes::ObjectType;
    let list = parse("COMMENT ON FUNCTION s.f(int) IS 'c'");
    let c = only_stmt(&list).stmt.unwrap().as_comment_stmt().expect("CommentStmt");
    assert_eq!(c.objtype, ObjectType::OBJECT_FUNCTION);
    assert_eq!(c.comment, Some("c"));
    let f = c.object.unwrap().as_object_with_args().expect("ObjectWithArgs");
    assert_eq!(f.objname.len(), 2);
    assert_eq!(f.objargs.len(), 1);

    let list = parse("COMMENT ON AGGREGATE agg(text) IS NULL");
    let c = only_stmt(&list).stmt.unwrap().as_comment_stmt().expect("CommentStmt");
    assert_eq!(c.objtype, ObjectType::OBJECT_AGGREGATE);
    assert_eq!(c.comment, None);
}

#[test]
fn alter_function_shapes() {
    use types_nodes::parsenodes::ObjectType;
    let list = parse("ALTER FUNCTION f(int) STRICT IMMUTABLE RESTRICT");
    let a = only_stmt(&list).stmt.unwrap().as_alter_function_stmt().expect("AlterFunctionStmt");
    assert_eq!(a.objtype, ObjectType::OBJECT_FUNCTION);
    assert_eq!(a.actions.len(), 2);
    assert_eq!(a.func.unwrap().objargs.len(), 1);

    let list = parse("ALTER FUNCTION f(int) RENAME TO g");
    let r = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<types_nodes::parsenodes::RenameStmt>()
        .expect("RenameStmt");
    assert_eq!(r.renameType, ObjectType::OBJECT_FUNCTION);
    assert_eq!(r.newname, Some("g"));
    assert!(r.object.unwrap().as_object_with_args().is_some());

    let list = parse("ALTER ROUTINE r(int) OWNER TO alice");
    let o = only_stmt(&list).stmt.unwrap().as_alter_owner_stmt().expect("AlterOwnerStmt");
    assert_eq!(o.objectType, ObjectType::OBJECT_ROUTINE);
    assert_eq!(o.newowner.unwrap().rolename, Some("alice"));
    assert!(o.object.unwrap().as_object_with_args().is_some());
}

#[test]
fn grant_on_function_shapes() {
    use types_nodes::parsenodes::{GrantTargetType, ObjectType};
    let list = parse("GRANT EXECUTE ON FUNCTION f(int), s.g TO u");
    let g = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<types_nodes::parsenodes::GrantStmt>()
        .expect("GrantStmt");
    assert!(g.is_grant);
    assert_eq!(g.objtype, ObjectType::OBJECT_FUNCTION);
    assert_eq!(g.targtype, GrantTargetType::ACL_TARGET_OBJECT);
    assert_eq!(g.objects.len(), 2);
    assert!(g.objects.nth(1).as_object_with_args().unwrap().args_unspecified);

    let list = parse("REVOKE ALL ON ALL PROCEDURES IN SCHEMA s FROM u");
    let g = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<types_nodes::parsenodes::GrantStmt>()
        .expect("GrantStmt");
    assert!(!g.is_grant);
    assert_eq!(g.objtype, ObjectType::OBJECT_PROCEDURE);
    assert_eq!(g.targtype, GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA);
    assert_eq!(g.objects.nth(0).as_string().unwrap().sval, "s");
}

#[test]
fn grant_on_parameter_shapes() {
    use types_nodes::parsenodes::{GrantTargetType, ObjectType};
    let list = parse("GRANT SET ON PARAMETER work_mem, plperl.on_init TO pg_monitor");
    let g = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<types_nodes::parsenodes::GrantStmt>()
        .expect("GrantStmt");
    assert!(g.is_grant);
    assert_eq!(g.objtype, ObjectType::OBJECT_PARAMETER_ACL);
    assert_eq!(g.targtype, GrantTargetType::ACL_TARGET_OBJECT);
    assert_eq!(g.objects.len(), 2);
    assert_eq!(g.objects.nth(0).as_string().unwrap().sval, "work_mem");
    assert_eq!(g.objects.nth(1).as_string().unwrap().sval, "plperl.on_init");
    assert_eq!(g.privileges.nth(0).as_access_priv().unwrap().priv_name, Some("set"));

    let list = parse("REVOKE ALL ON PARAMETER work_mem FROM pg_monitor");
    let g = only_stmt(&list)
        .stmt
        .unwrap()
        .as_variant::<types_nodes::parsenodes::GrantStmt>()
        .expect("GrantStmt");
    assert!(!g.is_grant);
    assert_eq!(g.objtype, ObjectType::OBJECT_PARAMETER_ACL);
    assert!(g.privileges.is_nil());
}

#[test]
fn aggregate_output_args_rejected() {
    let err = parse_err("DROP AGGREGATE a(OUT x int)");
    assert!(err.message().contains("aggregates cannot have output arguments"), "{}", err.message());
}

#[test]
fn create_fdw_and_server() {
    use types_nodes::parsenodes::DefElemAction;
    use types_nodes::rawnodes::{CreateFdwStmt, CreateForeignServerStmt};
    let list =
        parse("CREATE FOREIGN DATA WRAPPER postgresql VALIDATOR postgresql_fdw_validator OPTIONS (debug 'true');");
    let rs = only_stmt(&list);
    let n = rs.stmt.expect("stmt").as_variant::<CreateFdwStmt>().expect("CreateFdwStmt");
    assert_eq!(n.fdwname, Some("postgresql"));
    assert_eq!(n.func_options.len(), 1);
    let v = n.func_options.nth(0).as_def_elem().expect("DefElem");
    assert_eq!(v.defname, Some("validator"));
    let names = v.arg.expect("arg").as_list().expect("handler_name");
    assert_eq!(names.nth(0).as_string().expect("name").sval, "postgresql_fdw_validator");
    assert_eq!(n.options.len(), 1);
    let o = n.options.nth(0).as_def_elem().expect("DefElem");
    assert_eq!(o.defname, Some("debug"));
    assert_eq!(o.defaction, DefElemAction::DEFELEM_UNSPEC);
    assert_eq!(o.arg.expect("arg").as_string().expect("val").sval, "true");

    let list = parse(
        "CREATE SERVER s1 TYPE 'oracle' VERSION '1.0' FOREIGN DATA WRAPPER postgresql OPTIONS (host 'h', dbname 'db');",
    );
    let rs = only_stmt(&list);
    let n = rs
        .stmt
        .expect("stmt")
        .as_variant::<CreateForeignServerStmt>()
        .expect("CreateForeignServerStmt");
    assert_eq!(n.servername, Some("s1"));
    assert_eq!(n.servertype, Some("oracle"));
    assert_eq!(n.version, Some("1.0"));
    assert_eq!(n.fdwname, Some("postgresql"));
    assert!(!n.if_not_exists);
    assert_eq!(n.options.len(), 2);
}

#[test]
fn alter_fdw_and_server_option_actions() {
    use types_nodes::parsenodes::DefElemAction;
    use types_nodes::rawnodes::{AlterFdwStmt, AlterForeignServerStmt};
    let list = parse(
        "ALTER FOREIGN DATA WRAPPER foo NO VALIDATOR OPTIONS (ADD x '1', SET y '2', DROP z);",
    );
    let rs = only_stmt(&list);
    let n = rs.stmt.expect("stmt").as_variant::<AlterFdwStmt>().expect("AlterFdwStmt");
    assert_eq!(n.fdwname, Some("foo"));
    let v = n.func_options.nth(0).as_def_elem().expect("DefElem");
    assert_eq!(v.defname, Some("validator"));
    assert!(v.arg.is_none());
    let actions: Vec<DefElemAction> =
        n.options.iter().map(|o| o.as_def_elem().expect("DefElem").defaction).collect();
    assert_eq!(
        actions,
        [DefElemAction::DEFELEM_ADD, DefElemAction::DEFELEM_SET, DefElemAction::DEFELEM_DROP]
    );
    assert!(n.options.nth(2).as_def_elem().expect("DefElem").arg.is_none());

    let list = parse("ALTER SERVER s1 VERSION NULL;");
    let rs = only_stmt(&list);
    let n =
        rs.stmt.expect("stmt").as_variant::<AlterForeignServerStmt>().expect("AlterForeignServerStmt");
    assert_eq!(n.servername, Some("s1"));
    assert!(n.has_version && n.version.is_none() && n.options.is_nil());
}

#[test]
fn user_mapping_and_foreign_table() {
    use types_nodes::parsenodes::RoleSpecType;
    use types_nodes::rawnodes::{
        CreateForeignTableStmt, CreateUserMappingStmt, DropUserMappingStmt,
        ImportForeignSchemaStmt, ImportForeignSchemaType,
    };
    let list = parse("CREATE USER MAPPING FOR public SERVER s1 OPTIONS (\"user\" 'guest');");
    let rs = only_stmt(&list);
    let n =
        rs.stmt.expect("stmt").as_variant::<CreateUserMappingStmt>().expect("CreateUserMappingStmt");
    assert_eq!(n.user.expect("RoleSpec").roletype, RoleSpecType::ROLESPEC_PUBLIC);
    assert_eq!(n.servername, Some("s1"));
    assert_eq!(n.options.len(), 1);

    let list = parse("DROP USER MAPPING IF EXISTS FOR USER SERVER s1;");
    let rs = only_stmt(&list);
    let n =
        rs.stmt.expect("stmt").as_variant::<DropUserMappingStmt>().expect("DropUserMappingStmt");
    assert_eq!(n.user.expect("RoleSpec").roletype, RoleSpecType::ROLESPEC_CURRENT_USER);
    assert!(n.missing_ok);

    let list = parse("CREATE FOREIGN TABLE ft1 (c1 int NOT NULL, c2 text) SERVER s1 OPTIONS (delimiter ',');");
    let rs = only_stmt(&list);
    let n = rs
        .stmt
        .expect("stmt")
        .as_variant::<CreateForeignTableStmt>()
        .expect("CreateForeignTableStmt");
    assert_eq!(n.base.relation.expect("rv").relname, Some("ft1"));
    assert_eq!(n.base.tableElts.len(), 2);
    assert_eq!(n.servername, Some("s1"));
    assert_eq!(n.options.len(), 1);

    let list = parse("IMPORT FOREIGN SCHEMA rs LIMIT TO (t1, t2) FROM SERVER s1 INTO public;");
    let rs = only_stmt(&list);
    let n = rs
        .stmt
        .expect("stmt")
        .as_variant::<ImportForeignSchemaStmt>()
        .expect("ImportForeignSchemaStmt");
    assert_eq!(n.server_name, Some("s1"));
    assert_eq!(n.remote_schema, Some("rs"));
    assert_eq!(n.local_schema, Some("public"));
    assert_eq!(n.list_type, ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO);
    assert_eq!(n.table_list.len(), 2);
}

#[test]
fn collation_for_shape() {
    let list = parse("SELECT COLLATION FOR (x);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "pg_collation_for", 1);
    assert!(f.args.nth(0).as_column_ref().is_some());
    assert_eq!(f.location, 7);
}

#[test]
fn normalize_shapes() {
    let list = parse("SELECT NORMALIZE(x);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "normalize", 1);
    assert!(f.args.nth(0).as_column_ref().is_some());

    let list = parse("SELECT NORMALIZE(x, NFKC);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "normalize", 2);
    let a = f.args.nth(1).as_a_const().expect("A_Const");
    let Some(ValUnion::String(s)) = a.val else { panic!("String") };
    assert_eq!(s.sval, "NFKC");
}

#[test]
fn overlay_shapes() {
    let list = parse("SELECT OVERLAY(a PLACING b FROM c FOR d);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "overlay", 4);

    let list = parse("SELECT OVERLAY(a PLACING b FROM c);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "overlay", 3);

    let list = parse("SELECT OVERLAY(a, b);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_eq!(f.funcname.len(), 1);
    assert_eq!(f.funcname.nth(0).as_string().unwrap().sval, "overlay");
    assert_eq!(f.args.len(), 2);
    assert_eq!(f.funcformat, types_nodes::CoercionForm::COERCE_EXPLICIT_CALL);
}

#[test]
fn position_shape() {
    let list = parse("SELECT POSITION(a IN b);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_system_func(f, "position", 2);
    // position(A in B) becomes position(B, A).
    let c0 = f.args.nth(0).as_column_ref().expect("ColumnRef");
    let c1 = f.args.nth(1).as_column_ref().expect("ColumnRef");
    assert_eq!(c0.fields.nth(0).as_string().unwrap().sval, "b");
    assert_eq!(c1.fields.nth(0).as_string().unwrap().sval, "a");
}

#[test]
fn treat_shape() {
    let list = parse("SELECT TREAT(x AS text);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert_eq!(f.funcname.len(), 2);
    assert_eq!(f.funcname.nth(0).as_string().unwrap().sval, "pg_catalog");
    assert_eq!(f.funcname.nth(1).as_string().unwrap().sval, "text");
    assert_eq!(f.args.len(), 1);
    assert_eq!(f.funcformat, types_nodes::CoercionForm::COERCE_EXPLICIT_CALL);
}

#[test]
fn trim_shapes() {
    for (sql, name, nargs) in [
        ("SELECT TRIM(BOTH 'x' FROM y);", "btrim", 2),
        ("SELECT TRIM(LEADING 'x' FROM y);", "ltrim", 2),
        ("SELECT TRIM(TRAILING FROM y);", "rtrim", 1),
        ("SELECT TRIM(y);", "btrim", 1),
    ] {
        let list = parse(sql);
        let f = target_expr(&list).as_func_call().expect("FuncCall");
        assert_system_func(f, name, nargs);
    }
    // trim_list: a_expr FROM expr_list is lappend($3, $1): (source, chars).
    let list = parse("SELECT TRIM(BOTH 'x' FROM y);");
    let f = target_expr(&list).as_func_call().expect("FuncCall");
    assert!(f.args.nth(0).as_column_ref().is_some());
    assert!(f.args.nth(1).as_a_const().is_some());
}

#[test]
fn nullif_shape() {
    let list = parse("SELECT NULLIF(a, b);");
    let e = target_expr(&list).as_a_expr().expect("A_Expr");
    assert_eq!(e.kind, A_Expr_Kind::AEXPR_NULLIF);
    assert_eq!(e.name.nth(0).as_string().unwrap().sval, "=");
    assert!(e.lexpr.is_some() && e.rexpr.is_some());
    assert_eq!(e.location, 7);
}

#[test]
fn typed_table_shapes() {
    use types_nodes::rawnodes::{ColumnDef, Constraint, ConstrType, CreateStmt, TypeName};
    let list = parse("CREATE TABLE t OF ty (a WITH OPTIONS NOT NULL, b NOT NULL)");
    let n = only_stmt(&list).stmt.unwrap().as_variant::<CreateStmt>().unwrap();
    assert!(!n.if_not_exists && n.inhRelations.is_nil());
    let of = n.ofTypename.expect("ofTypename").as_variant::<TypeName>().unwrap();
    assert_eq!(of.names.len(), 1);
    assert_eq!(of.names.nth(0).as_string().unwrap().sval, "ty");
    assert_eq!(n.tableElts.len(), 2);
    for (i, name) in [(0usize, "a"), (1, "b")] {
        let cd = n.tableElts.nth(i).as_variant::<ColumnDef>().unwrap();
        assert_eq!(cd.colname, Some(name));
        assert!(cd.typeName.is_none() && cd.is_local);
        let c = cd.constraints.nth(0).as_variant::<Constraint>().unwrap();
        assert_eq!(c.contype, ConstrType::CONSTR_NOTNULL);
    }

    let list = parse("CREATE TABLE IF NOT EXISTS t OF s.ty");
    let n = only_stmt(&list).stmt.unwrap().as_variant::<CreateStmt>().unwrap();
    assert!(n.if_not_exists && n.tableElts.is_nil());
    let of = n.ofTypename.unwrap().as_variant::<TypeName>().unwrap();
    assert_eq!(of.names.len(), 2);
    assert_eq!(of.names.nth(1).as_string().unwrap().sval, "ty");
}

#[test]
fn partition_of_with_column_options_shapes() {
    use types_nodes::rawnodes::{ColumnDef, Constraint, ConstrType, CreateStmt, PartitionBoundSpec};
    let list = parse("CREATE TABLE p2 PARTITION OF p (a NOT NULL) FOR VALUES FROM (1) TO (10)");
    let n = only_stmt(&list).stmt.unwrap().as_variant::<CreateStmt>().unwrap();
    assert!(!n.if_not_exists);
    assert_eq!(n.inhRelations.len(), 1);
    let cd = n.tableElts.nth(0).as_variant::<ColumnDef>().unwrap();
    assert_eq!(cd.colname, Some("a"));
    assert!(cd.typeName.is_none() && cd.is_local);
    let c = cd.constraints.nth(0).as_variant::<Constraint>().unwrap();
    assert_eq!(c.contype, ConstrType::CONSTR_NOTNULL);
    let pb = n.partbound.expect("partbound").as_variant::<PartitionBoundSpec>().unwrap();
    assert_eq!(pb.lowerdatums.len(), 1);
    assert_eq!(pb.upperdatums.len(), 1);

    let list = parse(
        "CREATE TABLE IF NOT EXISTS p3 PARTITION OF p (a WITH OPTIONS NOT NULL) FOR VALUES IN (1)",
    );
    let n = only_stmt(&list).stmt.unwrap().as_variant::<CreateStmt>().unwrap();
    assert!(n.if_not_exists);
    assert_eq!(n.inhRelations.len(), 1);
    let cd = n.tableElts.nth(0).as_variant::<ColumnDef>().unwrap();
    assert_eq!(cd.colname, Some("a"));
    assert!(cd.typeName.is_none());
    let pb = n.partbound.expect("partbound").as_variant::<PartitionBoundSpec>().unwrap();
    assert_eq!(pb.listdatums.len(), 1);
}

#[test]
fn named_arg_shapes() {
    use types_nodes::rawnodes::FuncCall;
    use types_nodes::NamedArgExpr;
    let list = parse("SELECT f(1, silent := true)");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().unwrap();
    let fc = rt.val.unwrap().as_variant::<FuncCall>().unwrap();
    assert_eq!(fc.args.len(), 2);
    let na = fc.args.nth(1).as_variant::<NamedArgExpr>().unwrap();
    assert_eq!(na.name, Some("silent"));
    assert_eq!(na.argnumber, -1);
    assert!(na.arg.expect("NamedArgExpr has an arg").as_a_const().is_some());

    let list = parse("SELECT f(silent => true)");
    let sel = select_of(only_stmt(&list));
    let rt = sel.targetList.nth(0).as_res_target().unwrap();
    let fc = rt.val.unwrap().as_variant::<FuncCall>().unwrap();
    let na = fc.args.nth(0).as_variant::<NamedArgExpr>().unwrap();
    assert_eq!(na.name, Some("silent"));
}

#[test]
fn range_function_pair_and_ordinality_shapes() {
    use types_nodes::RangeFunction;
    let list = parse("SELECT * FROM generate_series(1,3) WITH ORDINALITY");
    let sel = select_of(only_stmt(&list));
    let rf = sel.fromClause.nth(0).as_variant::<RangeFunction>().unwrap();
    assert!(rf.ordinality && !rf.is_rowsfrom);
    assert_eq!(rf.functions.len(), 1);
    let pair = rf.functions.nth(0).as_list().unwrap();
    assert_eq!(pair.len(), 2);
    assert!(pair.nth(1).as_list().unwrap().is_nil());
}

#[test]
fn rows_from_shapes() {
    use types_nodes::rawnodes::ColumnDef;
    use types_nodes::RangeFunction;
    let list = parse(
        "SELECT * FROM ROWS FROM (f() AS (a int, b text), g()) WITH ORDINALITY AS t(x, y, z, o)",
    );
    let sel = select_of(only_stmt(&list));
    let rf = sel.fromClause.nth(0).as_variant::<RangeFunction>().unwrap();
    assert!(rf.ordinality && rf.is_rowsfrom);
    assert_eq!(rf.functions.len(), 2);
    let pair = rf.functions.nth(0).as_list().unwrap();
    let coldefs = pair.nth(1).as_list().unwrap();
    assert_eq!(coldefs.len(), 2);
    let cd = coldefs.nth(0).as_variant::<ColumnDef>().unwrap();
    assert_eq!(cd.colname, Some("a"));
    assert!(cd.typeName.is_some() && cd.is_local);
    let pair2 = rf.functions.nth(1).as_list().unwrap();
    assert!(pair2.nth(1).as_list().unwrap().is_nil());
    assert_eq!(rf.alias.unwrap().aliasname, Some("t"));
}

#[test]
fn func_alias_coldeflist_shapes() {
    use types_nodes::rawnodes::ColumnDef;
    use types_nodes::RangeFunction;
    let list = parse("SELECT * FROM f() AS t(a int, b text COLLATE \"C\")");
    let sel = select_of(only_stmt(&list));
    let rf = sel.fromClause.nth(0).as_variant::<RangeFunction>().unwrap();
    assert_eq!(rf.alias.unwrap().aliasname, Some("t"));
    assert_eq!(rf.coldeflist.len(), 2);
    let cd = rf.coldeflist.nth(1).as_variant::<ColumnDef>().unwrap();
    assert_eq!(cd.colname, Some("b"));
    assert!(cd.collClause.is_some());

    let list = parse("SELECT * FROM f() AS (a int)");
    let sel = select_of(only_stmt(&list));
    let rf = sel.fromClause.nth(0).as_variant::<RangeFunction>().unwrap();
    assert!(rf.alias.is_none());
    assert_eq!(rf.coldeflist.len(), 1);

    let list = parse("SELECT * FROM f() t(a int)");
    let sel = select_of(only_stmt(&list));
    let rf = sel.fromClause.nth(0).as_variant::<RangeFunction>().unwrap();
    assert_eq!(rf.alias.unwrap().aliasname, Some("t"));
    assert_eq!(rf.coldeflist.len(), 1);
}

#[test]
fn alter_system_shapes() {
    use types_nodes::parsenodes::VariableSetKind;

    let list = parse("ALTER SYSTEM SET work_mem = '64MB';");
    let n = only_stmt(&list).stmt.unwrap().as_alter_system_stmt().expect("AlterSystemStmt");
    assert_eq!(n.setstmt.kind, VariableSetKind::VAR_SET_VALUE);
    assert_eq!(n.setstmt.name, Some("work_mem"));
    assert_eq!(n.setstmt.args.len(), 1);

    let list = parse("ALTER SYSTEM RESET work_mem;");
    let n = only_stmt(&list).stmt.unwrap().as_alter_system_stmt().expect("AlterSystemStmt");
    assert_eq!(n.setstmt.kind, VariableSetKind::VAR_RESET);
    assert_eq!(n.setstmt.name, Some("work_mem"));

    let list = parse("ALTER SYSTEM RESET ALL;");
    let n = only_stmt(&list).stmt.unwrap().as_alter_system_stmt().expect("AlterSystemStmt");
    assert_eq!(n.setstmt.kind, VariableSetKind::VAR_RESET_ALL);

    let list = parse("ALTER SYSTEM SET search_path TO DEFAULT;");
    let n = only_stmt(&list).stmt.unwrap().as_alter_system_stmt().expect("AlterSystemStmt");
    assert_eq!(n.setstmt.kind, VariableSetKind::VAR_SET_DEFAULT);
}

#[test]
fn create_function_transform_option() {
    use types_nodes::rawnodes::TypeName;
    // Rules 1197 (createfunc_opt_item TRANSFORM) + 1210/1211
    // (transform_type_list); execution-side coverage lives with the
    // functioncmds lane (panicfix-commands).
    let list = parse(
        "CREATE FUNCTION tf(i int) RETURNS int TRANSFORM FOR TYPE int, FOR TYPE text \
         LANGUAGE sql AS 'select $1';",
    );
    let rs = only_stmt(&list);
    let n = rs.stmt.expect("stmt").as_create_function_stmt().expect("CreateFunctionStmt");
    let tr = n.options.nth(0).as_def_elem().expect("DefElem");
    assert_eq!(tr.defname, Some("transform"));
    let types = tr.arg.expect("arg").as_list().expect("List");
    assert_eq!(types.len(), 2);
    let t0 = types.nth(0).as_variant::<TypeName>().expect("TypeName");
    assert_eq!(t0.names.nth(t0.names.len() - 1).as_string().expect("t").sval, "int4");
    let t1 = types.nth(1).as_variant::<TypeName>().expect("TypeName");
    assert_eq!(t1.names.nth(t1.names.len() - 1).as_string().expect("t").sval, "text");
}

#[test]
fn alter_extension_contents_forms() {
    use types_nodes::parsenodes::ObjectType;
    use types_nodes::rawnodes::{AlterExtensionContentsStmt, TypeName};

    // Rule 708: ADD TRANSFORM FOR Typename LANGUAGE name —
    // object = [TypeName, String(lang)] (C list_make2($7, makeString($9))).
    let list = parse("ALTER EXTENSION ext ADD TRANSFORM FOR int LANGUAGE sql;");
    let rs = only_stmt(&list);
    let n = rs
        .stmt
        .expect("stmt")
        .as_variant::<AlterExtensionContentsStmt>()
        .expect("AlterExtensionContentsStmt");
    assert_eq!(n.extname, Some("ext"));
    assert_eq!(n.action, 1);
    assert_eq!(n.objtype, ObjectType::OBJECT_TRANSFORM);
    let pair = n.object.expect("object").as_list().expect("List");
    assert_eq!(pair.len(), 2);
    assert!(pair.nth(0).as_variant::<TypeName>().is_some());
    assert_eq!(pair.nth(1).as_string().expect("lang").sval, "sql");

    // Rule 700: DROP CAST '(' Typename AS Typename ')' — action -1,
    // object = [TypeName, TypeName].
    let list = parse("ALTER EXTENSION ext DROP CAST (int AS text);");
    let rs = only_stmt(&list);
    let n = rs
        .stmt
        .expect("stmt")
        .as_variant::<AlterExtensionContentsStmt>()
        .expect("AlterExtensionContentsStmt");
    assert_eq!(n.action, -1);
    assert_eq!(n.objtype, ObjectType::OBJECT_CAST);
    let pair = n.object.expect("object").as_list().expect("List");
    assert_eq!(pair.len(), 2);
    assert!(pair.nth(0).as_variant::<TypeName>().is_some());
    assert!(pair.nth(1).as_variant::<TypeName>().is_some());

    // Rule 704: ADD OPERATOR CLASS any_name USING name —
    // object = lcons(makeString($9), $7) = [String(am), name parts...].
    let list = parse("ALTER EXTENSION ext ADD OPERATOR CLASS myops USING btree;");
    let rs = only_stmt(&list);
    let n = rs
        .stmt
        .expect("stmt")
        .as_variant::<AlterExtensionContentsStmt>()
        .expect("AlterExtensionContentsStmt");
    assert_eq!(n.action, 1);
    assert_eq!(n.objtype, ObjectType::OBJECT_OPCLASS);
    let names = n.object.expect("object").as_list().expect("List");
    assert_eq!(names.len(), 2);
    assert_eq!(names.nth(0).as_string().expect("am").sval, "btree");
    assert_eq!(names.nth(1).as_string().expect("name").sval, "myops");

    // Rules 699/701/703/709: object is the $6 node directly.
    let list = parse("ALTER EXTENSION ext ADD TYPE int;");
    let rs = only_stmt(&list);
    let n = rs
        .stmt
        .expect("stmt")
        .as_variant::<AlterExtensionContentsStmt>()
        .expect("AlterExtensionContentsStmt");
    assert_eq!(n.objtype, ObjectType::OBJECT_TYPE);
    assert!(n.object.expect("object").as_variant::<TypeName>().is_some());
}
