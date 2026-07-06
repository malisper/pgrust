use std::sync::atomic::{AtomicUsize, Ordering};

use mcx::{Mcx, MemoryContext};
use parser_small1::make_parsestate;
use syscache_seams::{PgOperatorShape, PgProcShape};
use types_core::catalog::{INT4OID, TEXTOID, UNKNOWNOID};
use types_core::InvalidOid;
use types_error::ERRCODE_UNDEFINED_FUNCTION;
use types_nodes::{Node, NodeList, String as PgStr};

use crate::{compatible_oper_opid, make_op, oper};

const INT4_PLUS_OP: types_core::Oid = 551;
const INT4PL_PROC: types_core::Oid = 177;
const PG_CATALOG: types_core::Oid = 11;
const INT4_LT: types_core::Oid = 97;
const INT4_EQ: types_core::Oid = 96;
const INT4_GT: types_core::Oid = 521;
const INT4_BTREE_OPCLASS: types_core::Oid = 1978;
const INT4_HASH_OPCLASS: types_core::Oid = 1979;
const INT_BTREE_FAM: types_core::Oid = 1976;
const INT_HASH_FAM: types_core::Oid = 1977;
const NOSORT_OID: types_core::Oid = 9999;

static CANDIDATE_PROBES: AtomicUsize = AtomicUsize::new(0);

fn install_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        // The rig models a live session over pg_catalog-band builtins:
        // always visible, so error strings stay unqualified (C TypeIsVisible).
        namespace_seams::type_is_visible::set(|_| Ok(true));
        pg_inherits_seams::type_inherits_from::set(|_, _| Ok(false));
        syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
            if name == "@@" {
                CANDIDATE_PROBES.fetch_add(1, Ordering::Relaxed);
            }
            let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
            if (name == "+" || name == "@@") && l == INT4OID && r == INT4OID {
                v.push((INT4_PLUS_OP, PG_CATALOG));
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok((opno == INT4_PLUS_OP).then_some(PgOperatorShape {
                oprnamespace: 11,
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
        syscache_seams::lookup_pg_operator_name_candidates::set(|mcx, name| {
            let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
            if name == "+" || name == "@@" {
                v.push(syscache_seams::PgOperatorNameCandidate {
                    oid: INT4_PLUS_OP,
                    oprnamespace: PG_CATALOG,
                    oprkind: b'b' as i8,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                });
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_cast_shape::set(|_, _| Ok(None));
        syscache_seams::pg_type_typrelid::set(|_| Ok(Some(InvalidOid)));
        syscache_seams::pg_type_element_shape::set(|_| {
            Ok(Some(syscache_seams::PgTypeElementShape {
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok((funcid == INT4PL_PROC).then_some(PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
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
        syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
            Ok(match opclass {
                INT4_BTREE_OPCLASS => Some(syscache_seams::PgOpclassShape {
                    opcmethod: types_core::BTREE_AM_OID,
                    opcfamily: INT_BTREE_FAM,
                    opcintype: INT4OID,
                    opckeytype: 0,
                }),
                INT4_HASH_OPCLASS => Some(syscache_seams::PgOpclassShape {
                    opcmethod: lsyscache::HASH_AM_OID,
                    opcfamily: INT_HASH_FAM,
                    opcintype: INT4OID,
                    opckeytype: 0,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, _l, _r, strategy| {
            Ok(match (opfamily, strategy) {
                (INT_BTREE_FAM, 1) => INT4_LT,
                (INT_BTREE_FAM, 3) => INT4_EQ,
                (INT_BTREE_FAM, 5) => INT4_GT,
                (INT_HASH_FAM, 1) => INT4_EQ,
                _ => InvalidOid,
            })
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, _l, _r, procnum| {
            Ok(match (opfamily, procnum) {
                (INT_BTREE_FAM, 1) => 351,
                (INT_HASH_FAM, 1) => 450,
                (INT_HASH_FAM, 2) => 425,
                _ => InvalidOid,
            })
        });
        syscache_seams::syscache_hash_value_typeoid::set(|typid| Ok(typid.wrapping_mul(31)));
        indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
            Ok(match (type_id, am_id) {
                (INT4OID, types_core::BTREE_AM_OID) => INT4_BTREE_OPCLASS,
                (INT4OID, _) => INT4_HASH_OPCLASS,
                _ => InvalidOid,
            })
        });
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            let name = match typid {
                INT4OID => "int4",
                TEXTOID => "text",
                NOSORT_OID => "nosort",
                _ => return Ok(None),
            };
            let mut typname = types_tuple::NameData::default();
            typname.namestrcpy(name);
            Ok(Some(syscache_seams::PgTypeTypcacheShape {
                typname,
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typtype: b'b' as i8,
                typisdefined: true,
                typrelid: InvalidOid,
                typsubscript: InvalidOid,
                typelem: InvalidOid,
                typarray: InvalidOid,
                typcollation: InvalidOid,
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
    // Dedicated name: CANDIDATE_PROBES counts only "@@" (tests share the
    // process-global seams, so "+" probes race across test threads).
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "@@" }).unwrap()).unwrap();

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

    // C parse_oper.c op_error via format_type_be: exact message + hint.
    let err = oper(&pstate, &name, INT4OID, TEXTOID, false, 7).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
    assert_eq!(err.message(), "operator does not exist: integer <%> text");
    assert_eq!(
        err.hint(),
        Some(
            "No operator matches the given name and argument types. \
             You might need to add explicit type casts."
        )
    );
}

#[test]
fn inexact_without_coercible_candidate_is_42883() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);
    let name = plus_name(mcx);
    // int4+int4 is the only "+" candidate and text has no cast to int4 in
    // this fixture, so func_match_argtypes eliminates it (C op_error arm).
    let err = oper(&pstate, &name, INT4OID, TEXTOID, false, -1).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
    assert_eq!(err.message(), "operator does not exist: integer + text");
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

#[test]
fn sort_group_operators_int4() {
    install_fixture();
    let ops = crate::get_sort_group_operators(INT4OID, true, true, true, true).unwrap();
    assert_eq!(
        (ops.lt_opr, ops.eq_opr, ops.gt_opr, ops.hashable),
        (INT4_LT, INT4_EQ, INT4_GT, true)
    );
}

#[test]
fn sort_group_operators_missing_is_42883() {
    install_fixture();
    let err =
        crate::get_sort_group_operators(NOSORT_OID, true, true, false, true).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
    assert_eq!(err.message(), "could not identify an ordering operator for type nosort");
    assert_eq!(err.hint(), Some("Use an explicit ordering operator or modify the query."));

    let err =
        crate::get_sort_group_operators(NOSORT_OID, false, true, false, true).unwrap_err();
    assert_eq!(err.message(), "could not identify an equality operator for type nosort");
    assert_eq!(err.hint(), None);
}

#[test]
fn compatible_oper_opid_exact_and_missing() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let pstate = make_parsestate(mcx, None);

    let opid =
        compatible_oper_opid(&pstate, &plus_name(mcx), INT4OID, INT4OID, false).unwrap();
    assert_eq!(opid, INT4_PLUS_OP);

    let missing = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "<%>" }).unwrap()).unwrap();
    assert_eq!(
        compatible_oper_opid(&pstate, &missing, INT4OID, INT4OID, true).unwrap(),
        InvalidOid
    );
}
