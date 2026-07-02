use std::sync::atomic::{AtomicUsize, Ordering};

use mcx::{Mcx, MemoryContext};
use parser_small1::make_parsestate;
use syscache_seams::{PgOperatorShape, PgProcShape};
use types_core::catalog::{INT4OID, TEXTOID, UNKNOWNOID};
use types_core::InvalidOid;
use types_error::ERRCODE_UNDEFINED_FUNCTION;
use types_nodes::{Node, NodeList, String as PgStr};

use crate::{make_op, oper};

const INT4_PLUS_OP: types_core::Oid = 551;
const INT4PL_PROC: types_core::Oid = 177;
const PG_CATALOG: types_core::Oid = 11;

static CANDIDATE_PROBES: AtomicUsize = AtomicUsize::new(0);

fn install_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
            CANDIDATE_PROBES.fetch_add(1, Ordering::Relaxed);
            let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
            if name == "+" && l == INT4OID && r == INT4OID {
                v.push((INT4_PLUS_OP, PG_CATALOG));
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok((opno == INT4_PLUS_OP).then_some(PgOperatorShape {
                oprleft: INT4OID,
                oprright: INT4OID,
                oprresult: INT4OID,
                oprcom: INT4_PLUS_OP,
                oprnegate: InvalidOid,
                oprcode: INT4PL_PROC,
                oprrest: InvalidOid,
                oprjoin: InvalidOid,
                oprcanmerge: false,
                oprcanhash: false,
            }))
        });
        syscache_seams::pg_operator_name_candidates_exist::set(|name, oprkind| {
            Ok(name == "+" && oprkind == b'b' as i8)
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok((funcid == INT4PL_PROC).then_some(PgProcShape {
                pronamespace: PG_CATALOG,
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
        syscache_seams::pg_type_base_shape::set(|_| {
            Ok(Some(syscache_seams::PgTypeBaseShape {
                typtype: b'b' as i8,
                typbasetype: InvalidOid,
                typtypmod: -1,
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
    });
}

fn plus_name<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "+" }).unwrap()).unwrap()
}

fn int4_const<'mcx>(mcx: Mcx<'mcx>, v: i32) -> Node<'mcx> {
    Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, datum::Datum::from_i32(v), false, true)
        .unwrap()
}

#[test]
fn exact_match_and_memo_hit() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let name = plus_name(mcx);

    let op = oper(&pstate, &name, INT4OID, INT4OID, false, -1).unwrap().unwrap();
    assert_eq!(op.oid, INT4_PLUS_OP);
    assert_eq!(
        (op.shape.oprleft, op.shape.oprright, op.shape.oprresult),
        (INT4OID, INT4OID, INT4OID)
    );
    assert_eq!(op.shape.oprcode, INT4PL_PROC);

    let before = CANDIDATE_PROBES.load(Ordering::Relaxed);
    let op2 = oper(&pstate, &name, INT4OID, INT4OID, false, -1).unwrap().unwrap();
    assert_eq!(op2.oid, INT4_PLUS_OP);
    assert_eq!(CANDIDATE_PROBES.load(Ordering::Relaxed), before, "memo hit must skip catalog");

    inval::invalidate::CallSyscacheCallbacks(cache_syscache::cacheinfo::OPERNAMENSP, 0).unwrap();
    let op3 = oper(&pstate, &name, INT4OID, INT4OID, false, -1).unwrap().unwrap();
    assert_eq!(op3.oid, INT4_PLUS_OP);
    assert_eq!(
        CANDIDATE_PROBES.load(Ordering::Relaxed),
        before + 1,
        "invalidation must flush the memo"
    );
}

#[test]
fn unknown_operand_resolves_via_other_side() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let name = plus_name(mcx);

    let op = oper(&pstate, &name, UNKNOWNOID, INT4OID, false, -1).unwrap().unwrap();
    assert_eq!(op.oid, INT4_PLUS_OP);
}

#[test]
fn undefined_operator_is_42883() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "<%>" }).unwrap()).unwrap();

    let err = oper(&pstate, &name, INT4OID, INT4OID, false, 7).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);

    assert!(oper(&pstate, &name, INT4OID, INT4OID, true, 7).unwrap().is_none());
}

#[test]
#[should_panic(expected = "inexact operator resolution")]
fn inexact_match_panics_loudly() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let name = plus_name(mcx);
    let _ = oper(&pstate, &name, INT4OID, TEXTOID, false, -1);
}

#[test]
fn make_op_builds_op_expr() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = plus_name(mcx);

    let out = make_op(
        mcx,
        &mut pstate,
        &name,
        Some(int4_const(mcx, 1)),
        Some(int4_const(mcx, 1)),
        INT4OID,
        INT4OID,
        None,
        9,
    )
    .unwrap();

    let op = out.as_op_expr().unwrap();
    assert_eq!(op.opno, INT4_PLUS_OP);
    assert_eq!(op.opfuncid, INT4PL_PROC);
    assert_eq!(op.opresulttype, INT4OID);
    assert!(!op.opretset);
    assert_eq!((op.opcollid, op.inputcollid), (InvalidOid, InvalidOid));
    assert_eq!(op.args.len(), 2);
    assert_eq!(op.location, 9);
}

#[test]
fn postfix_operator_is_syntax_error() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = plus_name(mcx);

    let err = make_op(
        mcx,
        &mut pstate,
        &name,
        Some(int4_const(mcx, 1)),
        None,
        INT4OID,
        InvalidOid,
        None,
        9,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
}
