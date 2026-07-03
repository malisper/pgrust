use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::catalog::{BOOLOID, INT4OID, INT8OID, TEXTOID, UNKNOWNOID};
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

// init_seams panics on double-install; every test-side installer funnels here.
fn init_seams_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(crate::init_seams);
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
fn select_1_plus_1_end_to_end() {
    install_type_fixture();
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

    let q = analyze(mcx, "SELECT 1 + 1", &raw_stmt);

    assert_eq!(q.commandType, CmdType::CMD_SELECT);
    let te = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!(te.resname, Some("?column?"));
    let op = te.expr.as_op_expr().unwrap();
    assert_eq!((op.opno, op.opfuncid, op.opresulttype), (551, 177, INT4OID));
    assert!(!op.opretset);
    assert_eq!((op.opcollid, op.inputcollid), (InvalidOid, InvalidOid));
    assert_eq!(op.args.len(), 2);
    let lhs = op.args.nth(0).as_const().unwrap();
    assert_eq!((lhs.consttype, lhs.constvalue), (INT4OID, Datum::from_i32(1)));
    assert_eq!(op.location, 9);
}

#[test]
fn select_string_resolves_unknown_to_text() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sconst =
        Node::mk_a_const(mcx, Some(ValUnion::String(PgStr { sval: "x" })), 7).unwrap();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(sconst), 7).unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 10);

    let q = analyze(mcx, "SELECT 'x'", &raw_stmt);

    let te = q.targetList.nth(0).as_target_entry().unwrap();
    let c = te.expr.as_const().unwrap();
    assert_eq!(c.consttype, TEXTOID);
    assert_eq!((c.constlen, c.constbyval, c.constisnull), (-1, false, false));
    assert_eq!(c.constcollid, 100);
    assert_eq!(c.location, 7);
    // SAFETY: the datum points at a flat 4B-header text varlena owned by mcx.
    let v = unsafe { datum::varlena::VarlenaRef::from_ptr(c.constvalue.as_usize() as *const u8) };
    assert_eq!(v.data(), b"x");
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
    init_seams_once();

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
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(Some(types_tuple::PgTypeShape {
                typlen: if typid == TEXTOID { -1 } else { 4 },
                typbyval: typid != TEXTOID,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: if typid == TEXTOID { 100 } else { InvalidOid },
            }))
        });
        syscache_seams::pg_type_base_shape::set(|typid| {
            Ok(Some(syscache_seams::PgTypeBaseShape {
                typtype: if typid == UNKNOWNOID { b'p' as i8 } else { b'b' as i8 },
                typbasetype: InvalidOid,
                typtypmod: -1,
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok((typid == TEXTOID).then_some(syscache_seams::PgTypeIoShape {
                oid: TEXTOID,
                typinput: 46,
                typoutput: 47,
                typreceive: 2414,
                typsend: 2415,
                typmodin: InvalidOid,
                typmodout: InvalidOid,
                typelem: InvalidOid,
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        miscinit_seams::get_user_id::set(|| 10);
        syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
            let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
            if name == "+" && l == INT4OID && r == INT4OID {
                v.push((551, 11));
            }
            if name == ">" && l == INT4OID && r == INT4OID {
                v.push((521, 11));
            }
            if name == "=" && l == INT4OID && r == INT4OID {
                v.push((96, 11));
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            // 551 = int4pl (proc 177 -> int4); 521 = int4gt (proc 147 -> bool);
            // values verified vs pg_operator.dat/pg_proc.dat.
            Ok(match opno {
                551 => Some(syscache_seams::PgOperatorShape {
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
                }),
                521 => Some(syscache_seams::PgOperatorShape {
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: BOOLOID,
                    oprcom: 97,
                    oprnegate: 523,
                    oprcode: 147,
                    oprrest: InvalidOid,
                    oprjoin: InvalidOid,
                    oprcanmerge: true,
                    oprcanhash: false,
                }),
                // 96 = int4eq (proc 65 -> bool).
                96 => Some(syscache_seams::PgOperatorShape {
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: BOOLOID,
                    oprcom: 96,
                    oprnegate: 518,
                    oprcode: 65,
                    oprrest: 101,
                    oprjoin: 105,
                    oprcanmerge: true,
                    oprcanhash: true,
                }),
                _ => None,
            })
        });
        syscache_seams::pg_operator_name_candidates_exist::set(|name, _| {
            Ok(name == "+" || name == ">" || name == "=")
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(match funcid {
                // 481 = int8(int4), the pg_cast int4->int8 coercion function.
                177 | 147 | 481 | 65 => Some(syscache_seams::PgProcShape {
                    pronamespace: 11,
                    prorettype: match funcid {
                        147 | 65 => BOOLOID,
                        481 => INT8OID,
                        _ => INT4OID,
                    },
                    provariadic: InvalidOid,
                    prosupport: InvalidOid,
                    pronargs: if funcid == 481 { 1 } else { 2 },
                    prokind: b'f' as i8,
                    provolatile: b'i' as i8,
                    proparallel: b's' as i8,
                    proretset: false,
                    proisstrict: true,
                    proleakproof: false,
                }),
                2803 => Some(syscache_seams::PgProcShape {
                    pronamespace: 11,
                    prorettype: 20,
                    provariadic: InvalidOid,
                    prosupport: InvalidOid,
                    pronargs: 0,
                    prokind: b'a' as i8,
                    provolatile: b'i' as i8,
                    proparallel: b's' as i8,
                    proretset: false,
                    proisstrict: false,
                    proleakproof: false,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_proc_name_candidates::set(|mcx, proname| {
            let mut v = mcx::PgVec::new_in(mcx);
            if proname == "count" {
                let mut anyarg = mcx::vec_with_capacity_in(mcx, 1)?;
                anyarg.push(2276);
                v.push(syscache_seams::PgProcCandidate {
                    oid: 2147,
                    pronamespace: 11,
                    pronargs: 1,
                    pronargdefaults: 0,
                    provariadic: InvalidOid,
                    proargtypes: anyarg,
                });
                v.push(syscache_seams::PgProcCandidate {
                    oid: 2803,
                    pronamespace: 11,
                    pronargs: 0,
                    pronargdefaults: 0,
                    provariadic: InvalidOid,
                    proargtypes: mcx::PgVec::new_in(mcx),
                });
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_aggregate_shape::set(|aggfnoid| {
            Ok((aggfnoid == 2803).then_some(syscache_seams::PgAggregateShape {
                aggkind: b'n' as i8,
                aggnumdirectargs: 0,
                aggtransfn: 1219,
                aggfinalfn: InvalidOid,
                aggcombinefn: 463,
                aggserialfn: InvalidOid,
                aggdeserialfn: InvalidOid,
                aggfinalextra: false,
                aggfinalmodify: b'r' as i8,
                aggtranstype: 20,
                aggtransspace: 0,
            }))
        });
        syscache_seams::lookup_pg_cast_shape::set(|src, tgt| {
            // pg_cast: int4 -> int8 via 481 int8(int4), implicit, function.
            Ok((src == INT4OID && tgt == INT8OID).then_some(syscache_seams::PgCastShape {
                oid: 10001,
                castfunc: 481,
                castcontext: b'i' as i8,
                castmethod: b'f' as i8,
            }))
        });
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            let name = match typid {
                INT4OID => "int4",
                INT8OID => "int8",
                TEXTOID => "text",
                _ => return Ok(None),
            };
            let mut typname = types_tuple::NameData::default();
            typname.namestrcpy(name);
            Ok(Some(syscache_seams::PgTypeTypcacheShape {
                typname,
                typlen: match typid {
                    TEXTOID => -1,
                    INT8OID => 8,
                    _ => 4,
                },
                typbyval: typid != TEXTOID,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typtype: b'b' as i8,
                typisdefined: true,
                typrelid: InvalidOid,
                typsubscript: InvalidOid,
                typelem: InvalidOid,
                typarray: InvalidOid,
                typcollation: if typid == TEXTOID { 100 } else { InvalidOid },
            }))
        });
        syscache_seams::syscache_hash_value_typeoid::set(|typid| Ok(typid.wrapping_mul(31)));
        // 1978/1979 = int4 btree/hash default opclasses over the 1976/1977
        // integer_ops families (pg_opclass.dat) — the ORDER BY operator spine.
        syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
            Ok(match opclass {
                1978 => Some(syscache_seams::PgOpclassShape {
                    opcmethod: types_core::BTREE_AM_OID,
                    opcfamily: 1976,
                    opcintype: INT4OID,
                }),
                1979 => Some(syscache_seams::PgOpclassShape {
                    opcmethod: lsyscache::HASH_AM_OID,
                    opcfamily: 1977,
                    opcintype: INT4OID,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, _l, _r, strategy| {
            Ok(match (opfamily, strategy) {
                (1976, 1) => 97,
                (1976, 3) => 96,
                (1976, 5) => 521,
                (1977, 1) => 96,
                _ => InvalidOid,
            })
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, _l, _r, procnum| {
            Ok(match (opfamily, procnum) {
                (1976, 1) => 351,
                (1977, 1) => 450,
                (1977, 2) => 425,
                _ => InvalidOid,
            })
        });
        indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
            Ok(match (type_id, am_id) {
                (INT4OID, types_core::BTREE_AM_OID) => 1978,
                (INT4OID, _) => 1979,
                _ => InvalidOid,
            })
        });
    });
}

#[test]
fn select_1_order_by_1_end_to_end() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(int_const(mcx, 1, 7)), 7)
        .unwrap();
    let sortby = Node::mk(
        mcx,
        types_nodes::rawnodes::SortBy {
            node: Some(int_const(mcx, 1, 18)),
            sortby_dir: types_nodes::rawnodes::SortByDir::SORTBY_DEFAULT,
            sortby_nulls: types_nodes::rawnodes::SortByNulls::SORTBY_NULLS_DEFAULT,
            useOp: NodeList::nil(),
            location: -1,
        },
    )
    .unwrap();
    let stmt = Node::mk(
        mcx,
        SelectStmt {
            targetList: NodeList::make1(mcx, target).unwrap(),
            sortClause: NodeList::make1(mcx, sortby).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let raw_stmt = raw(stmt, 19);

    let q = analyze(mcx, "SELECT 1 ORDER BY 1", &raw_stmt);

    // C Query shape: sortClause = [SortGroupClause(ref 1, eqop 96 int4eq,
    // sortop 97 int4lt, forward, nulls last, hashable)], tle marked.
    assert_eq!(q.sortClause.len(), 1);
    let s = q.sortClause.nth(0).as_sort_group_clause().unwrap();
    assert_eq!(s.tleSortGroupRef, 1);
    assert_eq!((s.eqop, s.sortop), (96, 97));
    assert!(!s.reverse_sort && !s.nulls_first && s.hashable);
    let te = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!(te.ressortgroupref, 1);
    assert!(!te.resjunk);
    assert!(q.limitCount.is_none() && q.limitOffset.is_none());
}

#[test]
fn select_1_limit_1_end_to_end() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(int_const(mcx, 1, 7)), 7)
        .unwrap();
    let stmt = Node::mk(
        mcx,
        SelectStmt {
            targetList: NodeList::make1(mcx, target).unwrap(),
            limitCount: Some(int_const(mcx, 1, 15)),
            limitOption: types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT,
            ..Default::default()
        },
    )
    .unwrap();
    let raw_stmt = raw(stmt, 16);

    let q = analyze(mcx, "SELECT 1 LIMIT 1", &raw_stmt);

    // C Query shape: limitCount = FuncExpr(funcid 481 int8(int4), rettype
    // int8, COERCE_IMPLICIT_CAST, args [Const int4 1]).
    assert!(q.limitOffset.is_none());
    assert_eq!(q.limitOption, types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT);
    let f = q.limitCount.unwrap().as_func_expr().unwrap();
    assert_eq!((f.funcid, f.funcresulttype), (481, INT8OID));
    assert_eq!(f.funcformat, types_nodes::CoercionForm::COERCE_IMPLICIT_CAST);
    assert!(!f.funcretset && !f.funcvariadic);
    assert_eq!((f.funccollid, f.inputcollid), (InvalidOid, InvalidOid));
    assert_eq!(f.args.len(), 1);
    let arg = f.args.nth(0).as_const().unwrap();
    assert_eq!((arg.consttype, arg.constvalue), (INT4OID, Datum::from_i32(1)));
    assert_eq!(arg.location, 15);
    assert!(q.sortClause.is_nil());
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

mod from_where {
    use std::rc::Rc;
    use std::sync::Once;

    use datum::Datum;
    use mcx::{Mcx, MemoryContext, PgVec};
    use types_core::catalog::{BOOLOID, INT4OID, TEXTOID};
    use types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
    use types_error::{PgResult, ERRCODE_UNDEFINED_TABLE};
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::parsenodes::ACL_SELECT;
    use types_nodes::RTEKind;
    use types_rel::{
        AccessShareLock, FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData,
        LOCKMODE, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
    };
    use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    use types_tuple::{FormData_pg_attribute, NameData};

    use crate::parse_analyze_fixedparams;

    const T_OID: Oid = 4242;

    fn make_t(mcx: Mcx<'_>) -> Relation<'_> {
        let mut relname = NameData::default();
        relname.namestrcpy("t");
        let cols = [("x", INT4OID, types_core::InvalidOid), ("y", TEXTOID, 100)];
        let mut attrs = Vec::new();
        for (i, (name, typid, coll)) in cols.iter().enumerate() {
            let mut a = FormData_pg_attribute {
                attrelid: T_OID,
                atttypid: *typid,
                attlen: if *typid == INT4OID { 4 } else { -1 },
                attnum: i as i16 + 1,
                atttypmod: -1,
                attbyval: *typid == INT4OID,
                attalign: b'i' as i8,
                attstorage: b'p' as i8,
                attislocal: true,
                attcollation: *coll,
                ..Default::default()
            };
            a.attname.namestrcpy(name);
            attrs.push(a);
        }
        let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
            rd_id: T_OID,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: std::cell::Cell::new(true),
            rd_createSubid: std::cell::Cell::new(0),
            rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_droppedSubid: std::cell::Cell::new(0),
            rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: T_OID, dbId: 5 } },
            rd_rel: FormData_pg_class {
                relname,
                relnamespace: 2200,
                reltype: 0,
                relowner: 10,
                relam: 2,
                relfilenode: T_OID,
                reltablespace: 0,
                relpages: 0,
                reltuples: -1.0,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: RELPERSISTENCE_PERMANENT,
                relkind: RELKIND_RELATION,
                relhassubclass: false,
                relrowsecurity: false,
                relispopulated: true,
                relreplident: REPLICA_IDENTITY_DEFAULT,
                relispartition: false,
                relfrozenxid: 3,
                relminmxid: 1,
            },
            rd_att: Rc::new(tupdesc::CreateTupleDesc(mcx, &attrs).unwrap()),
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: std::cell::Cell::new(false),
            rd_amcache: Default::default(),
            rd_supportinfo: Default::default(),
            rd_indexlist: Default::default(),
        };
        Relation::open(data, None)
    }

    fn fake_openrv_extended<'mcx>(
        mcx: Mcx<'mcx>,
        rv: &rel_vocab::RangeVar,
        _lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Option<Relation<'mcx>>> {
        match rv.relname {
            "t" => Ok(Some(make_t(mcx))),
            _ if missing_ok => Ok(None),
            _ => Err(types_error::PgError::error("no such relation").into()),
        }
    }

    fn install() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            super::install_type_fixture();
            relation_seams::relation_openrv_extended::set(fake_openrv_extended);
            table::init_seams();
            super::init_seams_once();
        });
    }

    fn analyze_sql<'mcx>(
        mcx: Mcx<'mcx>,
        sql: &str,
    ) -> PgResult<types_nodes::parsenodes::Query<'mcx>> {
        let list =
            gram_core::raw_parser(mcx, sql, parser_seams::RawParseMode::RAW_PARSE_DEFAULT)
                .unwrap();
        assert_eq!(list.len(), 1);
        let raw = list.nth(0).as_raw_stmt().unwrap();
        let src = mcx::slice_borrow_in(mcx, sql.as_bytes()).unwrap();
        // SAFETY: byte-for-byte copy of a &str.
        let sql: &str = unsafe { core::str::from_utf8_unchecked(src) };
        parse_analyze_fixedparams(mcx, raw, sql, &[], Default::default())
    }

    // SQL-text GROUP BY through gram + analyze: the flat groupClause carries
    // int4's default grouping operators and the tlist entry its sortgroupref.
    #[test]
    fn select_group_by_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x, count(*) FROM t GROUP BY x").unwrap();

        assert!(q.hasAggs);
        assert!(q.groupingSets.is_nil());
        assert!(!q.hasGroupRTE, "RTE_GROUP substitution is a recorded divergence");
        assert_eq!(q.groupClause.len(), 1);
        let gc = q.groupClause.nth(0).as_sort_group_clause().unwrap();
        let t0 = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(t0.resname, Some("x"));
        assert_eq!(gc.tleSortGroupRef, t0.ressortgroupref);
        assert!(t0.ressortgroupref > 0);
        // int4: = 96, < 97, hashable.
        assert_eq!((gc.eqop, gc.sortop, gc.hashable), (96, 97, true));
        assert!(!gc.reverse_sort && !gc.nulls_first);
        let t1 = q.targetList.nth(1).as_target_entry().unwrap();
        assert_eq!(t1.expr.as_aggref().unwrap().aggfnoid, 2803);
    }

    #[test]
    fn select_ungrouped_column_via_sql_is_42803() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT x, y, count(*) FROM t GROUP BY x")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_GROUPING_ERROR);
        assert!(
            err.message().contains(
                "column \"t.y\" must appear in the GROUP BY clause or be used in an \
                 aggregate function"
            ),
            "{}",
            err.message()
        );
    }


    // transformFromClause appends one RTE + RangeTblRef per comma-separated
    // from-item (parse_clause.c); explicit JOIN syntax stays loud in
    // transformFromClauseItem.
    #[test]
    fn comma_join_from_items_append_two_rtes() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT t.x FROM t, t u WHERE t.x = u.x").unwrap();

        assert_eq!(q.rtable.len(), 2);
        let rte1 = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        let rte2 = q.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(rte1.rtekind, RTEKind::RTE_RELATION);
        assert_eq!(rte2.rtekind, RTEKind::RTE_RELATION);
        assert_eq!(rte1.relid, T_OID);
        assert_eq!(rte2.relid, T_OID);
        assert!(rte1.alias.is_none());
        assert_eq!(rte2.alias.unwrap().aliasname, Some("u"));
        assert_eq!(q.rteperminfos.len(), 2);

        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 2);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 1);
        assert_eq!(jt.fromlist.nth(1).as_range_tbl_ref().unwrap().rtindex, 2);

        let qual = jt.quals.unwrap().as_op_expr().unwrap();
        let lv = qual.args.nth(0).as_var().unwrap();
        let rv = qual.args.nth(1).as_var().unwrap();
        assert_eq!((lv.varno, lv.varattno), (1, 1));
        assert_eq!((rv.varno, rv.varattno), (2, 1));
    }

    // Query shape asserted against C 18.3: field-by-field vs the Query that
    // `SELECT x FROM t WHERE x > 5` produces for t(x int4, y text).
    #[test]
    fn select_from_where_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x FROM t WHERE x > 5").unwrap();

        assert_eq!(q.commandType, CmdType::CMD_SELECT);

        assert_eq!(q.rtable.len(), 1);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.rtekind, RTEKind::RTE_RELATION);
        assert_eq!(rte.relid, T_OID);
        assert!(rte.inh);
        assert_eq!(rte.relkind, RELKIND_RELATION);
        assert_eq!(rte.rellockmode, AccessShareLock);
        assert_eq!(rte.perminfoindex, 1);
        assert!(rte.alias.is_none());
        assert!(rte.inFromCl && !rte.lateral);
        let eref = rte.eref.unwrap();
        assert_eq!(eref.aliasname, Some("t"));
        let names: Vec<_> =
            eref.colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
        assert_eq!(names, ["x", "y"]);

        assert_eq!(q.rteperminfos.len(), 1);
        let perminfo = q.rteperminfos.nth(0).as_rte_permission_info().unwrap();
        assert_eq!(perminfo.relid, T_OID);
        assert!(perminfo.inh);
        assert_eq!(perminfo.requiredPerms, ACL_SELECT);
        assert!(perminfo.selectedCols.is_member(1 - FirstLowInvalidHeapAttributeNumber));
        assert!(!perminfo.selectedCols.is_member(2 - FirstLowInvalidHeapAttributeNumber));

        assert_eq!(q.targetList.len(), 1);
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te.resno, te.resname, te.resjunk), (1, Some("x"), false));
        assert_eq!((te.resorigtbl, te.resorigcol), (T_OID, 1));
        let v = te.expr.as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (1, 1));
        assert_eq!((v.vartype, v.vartypmod, v.varcollid), (INT4OID, -1, 0));
        assert_eq!(v.varlevelsup, 0);
        assert_eq!((v.varnosyn, v.varattnosyn), (1, 1));
        assert_eq!(v.location, 7);

        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 1);
        let qual = jt.quals.unwrap().as_op_expr().unwrap();
        assert_eq!((qual.opno, qual.opfuncid), (521, 147));
        assert_eq!(qual.opresulttype, BOOLOID);
        assert!(!qual.opretset);
        assert_eq!((qual.opcollid, qual.inputcollid), (0, 0));
        assert_eq!(qual.location, 24);
        assert_eq!(qual.args.len(), 2);
        let lv = qual.args.nth(0).as_var().unwrap();
        assert_eq!((lv.varno, lv.varattno, lv.vartype, lv.location), (1, 1, INT4OID, 22));
        let rc = qual.args.nth(1).as_const().unwrap();
        assert_eq!((rc.consttype, rc.constvalue), (INT4OID, Datum::from_i32(5)));
        assert_eq!(rc.location, 26);

        assert!(q.groupClause.is_nil() && q.sortClause.is_nil() && q.havingQual.is_none());
        assert!(!q.hasAggs && !q.hasWindowFuncs && !q.hasSubLinks && !q.hasTargetSRFs);
    }

    #[test]
    fn select_star_from_t_expands_columns() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT * FROM t").unwrap();
        assert_eq!(q.targetList.len(), 2);
        let te0 = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te0.resno, te0.resname), (1, Some("x")));
        let te1 = q.targetList.nth(1).as_target_entry().unwrap();
        assert_eq!((te1.resno, te1.resname), (2, Some("y")));
        let v1 = te1.expr.as_var().unwrap();
        assert_eq!((v1.varattno, v1.vartype, v1.varcollid), (2, TEXTOID, 100));

        let perminfo = q.rteperminfos.nth(0).as_rte_permission_info().unwrap();
        assert!(perminfo.selectedCols.is_member(1 - FirstLowInvalidHeapAttributeNumber));
        assert!(perminfo.selectedCols.is_member(2 - FirstLowInvalidHeapAttributeNumber));
    }

    #[test]
    fn qualified_column_and_alias() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT c.x FROM t AS c").unwrap();
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.eref.unwrap().aliasname, Some("c"));
        assert_eq!(rte.alias.unwrap().aliasname, Some("c"));
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(te.resname, Some("x"));
        assert_eq!(te.expr.as_var().unwrap().varattno, 1);
    }

    #[test]
    fn insert_values_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "INSERT INTO t VALUES (1, 'foo')").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_INSERT);
        assert_eq!(q.resultRelation, 1);
        assert_eq!(q.rtable.len(), 1);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.rtekind, RTEKind::RTE_RELATION);
        assert_eq!(rte.relid, T_OID);
        assert_eq!(rte.rellockmode, types_rel::RowExclusiveLock);
        assert!(!rte.inh && !rte.inFromCl);
        assert!(q.jointree.unwrap().fromlist.is_nil());

        assert_eq!(q.targetList.len(), 2);
        let te0 = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te0.resno, te0.resname), (1, Some("x")));
        let c0 = te0.expr.as_const().unwrap();
        assert_eq!((c0.consttype, c0.constvalue.as_i32()), (INT4OID, 1));
        let te1 = q.targetList.nth(1).as_target_entry().unwrap();
        assert_eq!((te1.resno, te1.resname), (2, Some("y")));
        // 'foo' (unknown) is coerced to the column type text.
        assert_eq!(parse_expr::expr_type(te1.expr), TEXTOID);

        let perminfo = q.rteperminfos.nth(0).as_rte_permission_info().unwrap();
        assert_eq!(perminfo.requiredPerms, types_nodes::parsenodes::ACL_INSERT);
        assert!(perminfo.insertedCols.is_member(1 - FirstLowInvalidHeapAttributeNumber));
        assert!(perminfo.insertedCols.is_member(2 - FirstLowInvalidHeapAttributeNumber));
    }

    #[test]
    fn insert_multi_row_values_builds_values_rte() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "INSERT INTO t (x) VALUES (1), (2)").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_INSERT);
        assert_eq!(q.resultRelation, 1);
        assert_eq!(q.rtable.len(), 2);
        let vrte = q.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(vrte.rtekind, RTEKind::RTE_VALUES);
        assert_eq!(vrte.values_lists.len(), 2);
        assert_eq!(vrte.eref.unwrap().aliasname, Some("*VALUES*"));
        assert_eq!(vrte.coltypes.nth(0), INT4OID);
        assert_eq!(q.jointree.unwrap().fromlist.len(), 1);

        assert_eq!(q.targetList.len(), 1);
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te.resno, te.resname), (1, Some("x")));
        let var = te.expr.as_var().unwrap();
        assert_eq!((var.varno, var.varattno, var.vartype), (2, 1, INT4OID));
    }

    #[test]
    fn insert_default_values_yields_empty_targetlist() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "INSERT INTO t DEFAULT VALUES").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_INSERT);
        assert!(q.targetList.is_nil());
    }

    #[test]
    fn insert_error_shapes() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "INSERT INTO t VALUES (1, 'a', 3)").map(|_| ()).unwrap_err();
        assert_eq!(err.message, "INSERT has more expressions than target columns");

        let err = analyze_sql(mcx, "INSERT INTO t (x, y) VALUES (1)").map(|_| ()).unwrap_err();
        assert_eq!(err.message, "INSERT has more target columns than expressions");

        let err = analyze_sql(mcx, "INSERT INTO t (nope) VALUES (1)").map(|_| ()).unwrap_err();
        assert_eq!(err.message, "column \"nope\" of relation \"t\" does not exist");

        let err = analyze_sql(mcx, "INSERT INTO t (x, x) VALUES (1, 2)").map(|_| ()).unwrap_err();
        assert_eq!(err.message, "column \"x\" specified more than once");

        let err =
            analyze_sql(mcx, "INSERT INTO t (x) VALUES (1), (2, 3)").map(|_| ()).unwrap_err();
        assert_eq!(err.message, "VALUES lists must all be the same length");
    }

    #[test]
    fn update_set_where_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "UPDATE t SET y = 'bar' WHERE x > 1").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_UPDATE);
        assert_eq!(q.resultRelation, 1);
        assert_eq!(q.rtable.len(), 1);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.relid, T_OID);
        assert_eq!(rte.rellockmode, types_rel::RowExclusiveLock);
        assert!(!rte.inFromCl);
        // alsoSource: the target rel is scanned, so it sits in the jointree.
        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 1);
        assert!(jt.quals.is_some());

        // SET resnos are target attribute numbers, not tlist positions.
        assert_eq!(q.targetList.len(), 1);
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te.resno, te.resname, te.resjunk), (2, Some("y"), false));
        assert_eq!(parse_expr::expr_type(te.expr), TEXTOID);

        let perminfo = q.rteperminfos.nth(0).as_rte_permission_info().unwrap();
        assert_eq!(perminfo.requiredPerms, types_nodes::parsenodes::ACL_UPDATE | ACL_SELECT);
        assert!(perminfo.updatedCols.is_member(2 - FirstLowInvalidHeapAttributeNumber));
        assert!(!perminfo.updatedCols.is_member(1 - FirstLowInvalidHeapAttributeNumber));
    }

    #[test]
    fn update_set_can_reference_target_columns() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "UPDATE t SET x = x + 1").unwrap();
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te.resno, te.resname), (1, Some("x")));
        let op = te.expr.as_op_expr().unwrap();
        let var = op.args.nth(0).as_var().unwrap();
        assert_eq!((var.varno, var.varattno, var.vartype), (1, 1, INT4OID));
    }

    #[test]
    fn update_undefined_set_column_is_42703() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "UPDATE t SET nope = 1").map(|_| ()).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_COLUMN);
        assert_eq!(err.message, "column \"nope\" of relation \"t\" does not exist");
    }

    #[test]
    fn delete_where_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "DELETE FROM t WHERE x > 2").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_DELETE);
        assert_eq!(q.resultRelation, 1);
        assert!(q.targetList.is_nil());
        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert!(jt.quals.is_some());
        let perminfo = q.rteperminfos.nth(0).as_rte_permission_info().unwrap();
        assert_eq!(perminfo.requiredPerms, types_nodes::parsenodes::ACL_DELETE | ACL_SELECT);
    }

    #[test]
    fn delete_without_where_has_no_qual() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "DELETE FROM t").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_DELETE);
        assert!(q.jointree.unwrap().quals.is_none());
    }

    #[test]
    fn update_with_alias_scopes_target() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "UPDATE t AS c SET x = c.x + 1 WHERE c.x > 5").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_UPDATE);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.eref.unwrap().aliasname, Some("c"));
    }

    #[test]
    fn missing_table_is_42p01_with_position() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT x FROM nope").map(|_| ()).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_TABLE);
        assert_eq!(err.message, "relation \"nope\" does not exist");
        assert_eq!(err.cursor_position(), Some(15));
    }

    // EXISTS/scalar sublinks through gram + analyze: the SubLink carries the
    // transformed sub-Query and the outer Query flags hasSubLinks.
    #[test]
    fn exists_sublink_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x FROM t WHERE EXISTS (SELECT 1 FROM t)").unwrap();
        assert!(q.hasSubLinks);
        let sl = q.jointree.unwrap().quals.unwrap().as_sub_link().unwrap();
        assert_eq!(sl.subLinkType, types_nodes::SubLinkType::EXISTS_SUBLINK);
        assert!(sl.testexpr.is_none() && sl.operName.is_nil());
        let sub = sl.subselect.as_query().expect("transformed to Query");
        assert_eq!(sub.commandType, CmdType::CMD_SELECT);
        assert!(!sub.hasSubLinks);
        assert_eq!(sub.rtable.len(), 1);
    }

    #[test]
    fn scalar_sublink_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x FROM t WHERE x = (SELECT x FROM t)").unwrap();
        assert!(q.hasSubLinks);
        let op = q.jointree.unwrap().quals.unwrap().as_op_expr().unwrap();
        assert_eq!(op.opno, 96);
        let sl = op.args.nth(1).as_sub_link().unwrap();
        assert_eq!(sl.subLinkType, types_nodes::SubLinkType::EXPR_SUBLINK);
        let sub = sl.subselect.as_query().unwrap();
        assert_eq!(sub.targetList.len(), 1);
    }

    #[test]
    fn scalar_sublink_multi_column_is_42601() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT x FROM t WHERE x = (SELECT x, y FROM t)")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
        assert!(err.message().contains("subquery must return only one column"), "{}", err.message());
    }
}

fn count_star_call(mcx: Mcx<'_>) -> Node<'_> {
    let funcname = NodeList::make1(
        mcx,
        Node::mk(mcx, PgStr { sval: "count" }).unwrap(),
    )
    .unwrap();
    Node::mk(
        mcx,
        types_nodes::rawnodes::FuncCall {
            funcname,
            args: NodeList::nil(),
            agg_order: NodeList::nil(),
            agg_filter: None,
            over: None,
            agg_within_group: false,
            agg_star: true,
            agg_distinct: false,
            func_variadic: false,
            funcformat: types_nodes::CoercionForm::COERCE_EXPLICIT_CALL,
            location: 7,
        },
    )
    .unwrap()
}

#[test]
fn select_count_star_end_to_end() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let target =
        Node::mk_res_target(mcx, None, NodeList::nil(), Some(count_star_call(mcx)), 7).unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 15);

    let q = analyze(mcx, "SELECT count(*)", &raw_stmt);

    assert!(q.hasAggs);
    let te = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!(te.resname, Some("count"));
    let agg = te.expr.as_aggref().unwrap();
    assert_eq!(agg.aggfnoid, 2803);
    assert_eq!(agg.aggtype, 20);
    assert!(agg.aggstar);
    assert!(agg.args.is_nil());
    assert_eq!((agg.aggcollid, agg.inputcollid), (InvalidOid, InvalidOid));
}

#[test]
fn count_star_in_where_is_42803() {
    install_type_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(int_const(mcx, 1, 7)), 7)
        .unwrap();
    let sel = Node::mk(
        mcx,
        SelectStmt {
            targetList: NodeList::from_slice(mcx, &[target]).unwrap(),
            whereClause: Some(count_star_call(mcx)),
            ..Default::default()
        },
    )
    .unwrap();
    let raw_stmt = raw(sel, 30);

    let err = parse_analyze_fixedparams(
        mcx,
        &raw_stmt,
        "SELECT 1 WHERE count(*)",
        &[],
        Default::default(),
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains("aggregate functions are not allowed in WHERE"),
        "{}",
        err.message()
    );
}
