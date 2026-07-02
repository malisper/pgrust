use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::catalog::INT4OID;
use types_core::InvalidOid;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{QuerySource, TransactionStmt};
use types_nodes::rawnodes::{RawStmt, SelectStmt, ValUnion};
use types_nodes::{Integer, Node, NodeList, String as PgStr};

use crate::{
    analyze_requires_snapshot, parse_analyze_fixedparams, stmt_requires_parse_analysis,
};

fn int_const<'mcx>(mcx: Mcx<'mcx>, ival: i32, location: i32) -> Node<'mcx> {
    Node::mk_a_const(mcx, Some(ValUnion::Integer(Integer { ival })), location).unwrap()
}

fn select_stmt<'mcx>(mcx: Mcx<'mcx>, targets: &[Node<'mcx>]) -> Node<'mcx> {
    Node::mk(
        mcx,
        SelectStmt { targetList: NodeList::from_slice(mcx, targets).unwrap(), ..Default::default() },
    )
    .unwrap()
}

fn raw<'mcx>(stmt: Node<'mcx>, len: i32) -> RawStmt<'mcx> {
    RawStmt { stmt: Some(stmt), stmt_location: 0, stmt_len: len }
}

fn analyze<'mcx>(
    mcx: Mcx<'mcx>,
    source: &str,
    raw_stmt: &RawStmt<'mcx>,
) -> types_nodes::parsenodes::Query<'mcx> {
    parse_analyze_fixedparams(mcx, raw_stmt, source, &[], Default::default()).unwrap()
}

#[test]
fn select_1_end_to_end() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(int_const(mcx, 1, 7)), 7)
        .unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 8);

    let q = analyze(mcx, "SELECT 1", &raw_stmt);

    assert_eq!(q.commandType, CmdType::CMD_SELECT);
    assert_eq!(q.querySource, QuerySource::QSRC_ORIGINAL);
    assert!(q.canSetTag);
    assert_eq!(q.stmt_location, 0);
    assert_eq!(q.stmt_len, 8);
    assert!(q.rtable.is_nil());
    assert!(q.rteperminfos.is_nil());
    assert!(!q.hasAggs && !q.hasWindowFuncs && !q.hasSubLinks && !q.hasTargetSRFs);

    let jt = q.jointree.unwrap();
    assert!(jt.fromlist.is_nil());
    assert!(jt.quals.is_none());

    assert_eq!(q.targetList.len(), 1);
    let te = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!(te.resno, 1);
    assert_eq!(te.resname, Some("?column?"));
    assert!(!te.resjunk);
    let c = te.expr.as_const().unwrap();
    assert_eq!(c.consttype, INT4OID);
    assert_eq!(c.constvalue, Datum::from_i32(1));
    assert_eq!(c.constlen, 4);
    assert!(c.constbyval);
    assert!(!c.constisnull);
    assert_eq!(c.consttypmod, -1);
    assert_eq!(c.constcollid, InvalidOid);
    assert_eq!(c.location, 7);
}

#[test]
fn select_with_alias_and_multiple_columns() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let t1 = Node::mk_res_target(mcx, Some("foo"), NodeList::nil(), Some(int_const(mcx, 1, 7)), 7)
        .unwrap();
    let t2 = Node::mk_res_target(mcx, None, NodeList::nil(), Some(int_const(mcx, 2, 17)), 17)
        .unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[t1, t2]), 19);

    let q = analyze(mcx, "SELECT 1 AS foo, 2", &raw_stmt);

    assert_eq!(q.targetList.len(), 2);
    let te1 = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!((te1.resno, te1.resname), (1, Some("foo")));
    let te2 = q.targetList.nth(1).as_target_entry().unwrap();
    assert_eq!((te2.resno, te2.resname), (2, Some("?column?")));
}

#[test]
#[should_panic(expected = "make_op")]
fn select_1_plus_1_arms_transform_then_panics_at_oper_lookup() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "+" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        types_nodes::rawnodes::A_Expr_Kind::AEXPR_OP,
        name,
        Some(int_const(mcx, 1, 7)),
        Some(int_const(mcx, 1, 11)),
        9,
    )
    .unwrap();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(aexpr), 7).unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 12);

    let _ = analyze(mcx, "SELECT 1 + 1", &raw_stmt);
}

#[test]
#[should_panic(expected = "coerce_type")]
fn select_string_panics_at_unknown_resolution() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sconst =
        Node::mk_a_const(mcx, Some(ValUnion::String(PgStr { sval: "x" })), 7).unwrap();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(sconst), 7).unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 10);
    let _ = analyze(mcx, "SELECT 'x'", &raw_stmt);
}

#[test]
fn utility_statement_wraps_in_cmd_utility_query() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let txn = Node::mk(mcx, TransactionStmt::default()).unwrap();
    let raw_stmt = raw(txn, 5);

    let q = analyze(mcx, "BEGIN", &raw_stmt);

    assert_eq!(q.commandType, CmdType::CMD_UTILITY);
    assert!(q.canSetTag);
    let wrapped = q.utilityStmt.unwrap();
    assert!(wrapped.as_transaction_stmt().is_some());
    assert!(q.targetList.is_nil());
    assert!(q.jointree.is_none());
}

#[test]
fn requires_parse_analysis_and_snapshot_split_by_tag() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sel = raw(select_stmt(mcx, &[]), 0);
    assert!(stmt_requires_parse_analysis(&sel));
    assert!(analyze_requires_snapshot(&sel));

    let txn = raw(Node::mk(mcx, TransactionStmt::default()).unwrap(), 0);
    assert!(!stmt_requires_parse_analysis(&txn));
    assert!(!analyze_requires_snapshot(&txn));
}

#[test]
fn seams_install_and_dispatch() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    crate::init_seams();

    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(int_const(mcx, 1, 7)), 7)
        .unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 8);

    assert!(analyze_seams::analyze_requires_snapshot::call(&raw_stmt));
    let q = analyze_seams::parse_analyze_fixedparams::call(
        mcx,
        &raw_stmt,
        "SELECT 1",
        &[],
        Default::default(),
    )
    .unwrap();
    assert_eq!(q.commandType, CmdType::CMD_SELECT);
}

fn install_type_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|_| {
            Ok(Some(types_tuple::PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: InvalidOid,
            }))
        });
    });
}

#[test]
fn fixed_params_resolve_paramref() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pref = Node::mk_param_ref(mcx, 1, 7).unwrap();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(pref), 7).unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 9);

    let q = parse_analyze_fixedparams(mcx, &raw_stmt, "SELECT $1", &[INT4OID], Default::default())
        .unwrap();

    let te = q.targetList.nth(0).as_target_entry().unwrap();
    let p = te.expr.as_param().unwrap();
    assert_eq!(p.paramtype, INT4OID);
    assert_eq!(p.paramid, 1);
}

#[test]
fn undefined_param_is_42p02() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pref = Node::mk_param_ref(mcx, 2, 7).unwrap();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(pref), 7).unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 9);

    let err = parse_analyze_fixedparams(
        mcx,
        &raw_stmt,
        "SELECT $2",
        &[INT4OID],
        Default::default(),
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_PARAMETER);
}
