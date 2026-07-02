use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_core::catalog::INT4OID;
use types_core::InvalidOid;
use types_nodes::rawnodes::{A_Expr_Kind, ValUnion};
use types_nodes::{Integer, Node, NodeList, String as PgStr};

use crate::{expr_collation, expr_location, expr_type, transformExpr};

fn int_const<'mcx>(mcx: Mcx<'mcx>, ival: i32, location: i32) -> Node<'mcx> {
    Node::mk_a_const(mcx, Some(ValUnion::Integer(Integer { ival })), location).unwrap()
}

#[test]
fn a_const_transforms_to_const_and_restores_expr_kind() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_OTHER;

    let out = transformExpr(
        mcx,
        &mut pstate,
        int_const(mcx, 42, 7),
        ParseExprKind::EXPR_KIND_SELECT_TARGET,
    )
    .unwrap();

    assert_eq!(pstate.p_expr_kind, ParseExprKind::EXPR_KIND_OTHER);
    let c = out.as_const().unwrap();
    assert_eq!(c.consttype, INT4OID);
    assert_eq!(c.location, 7);
    assert_eq!(expr_type(out), INT4OID);
    assert_eq!(expr_collation(out), InvalidOid);
    assert_eq!(expr_location(out), 7);
}

#[test]
fn already_transformed_var_passes_through() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();

    let out =
        transformExpr(mcx, &mut pstate, var, ParseExprKind::EXPR_KIND_SELECT_TARGET).unwrap();
    assert_eq!(expr_type(out), INT4OID);
    assert!(out.as_var().is_some());
}

#[test]
fn paramref_without_hook_is_42p02() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let pref = Node::mk_param_ref(mcx, 3, 7).unwrap();

    let err = transformExpr(mcx, &mut pstate, pref, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_PARAMETER);
}

fn install_oper_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
            let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
            if name == "+" && l == INT4OID && r == INT4OID {
                v.push((551, 11));
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok((opno == 551).then_some(syscache_seams::PgOperatorShape {
                oprleft: INT4OID,
                oprright: INT4OID,
                oprresult: INT4OID,
                oprcom: 551,
                oprnegate: InvalidOid,
                oprcode: 177,
                oprrest: InvalidOid,
                oprjoin: InvalidOid,
                oprcanmerge: false,
                oprcanhash: false,
            }))
        });
        syscache_seams::pg_operator_name_candidates_exist::set(|name, _| Ok(name == "+"));
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok((funcid == 177).then_some(syscache_seams::PgProcShape {
                pronamespace: 11,
                prorettype: INT4OID,
                provariadic: InvalidOid,
                prosupport: InvalidOid,
                pronargs: 2,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: false,
            }))
        });
    });
}

#[test]
fn a_expr_op_transforms_to_op_expr() {
    install_oper_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "+" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_OP,
        name,
        Some(int_const(mcx, 1, 7)),
        Some(int_const(mcx, 1, 11)),
        9,
    )
    .unwrap();

    let out = transformExpr(mcx, &mut pstate, aexpr, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .unwrap();

    let op = out.as_op_expr().unwrap();
    assert_eq!((op.opno, op.opfuncid, op.opresulttype), (551, 177, INT4OID));
    assert!(!op.opretset);
    assert_eq!(op.args.len(), 2);
    assert_eq!(op.args.nth(0).as_const().unwrap().consttype, INT4OID);
    assert_eq!(op.location, 9);
    assert_eq!(expr_type(out), INT4OID);
}

#[test]
fn transform_null_equals_guc_roundtrip() {
    crate::init_seams();
    assert!(!crate::transform_null_equals());
    guc_tables::vars::Transform_null_equals.write(true);
    assert!(guc_tables::vars::Transform_null_equals.read());
    guc_tables::vars::Transform_null_equals.write(false);
}
