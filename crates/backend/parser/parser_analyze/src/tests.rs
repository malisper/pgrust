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
            // 1007 = _int4; 6179 = array_subscript_handler (pg_type/pg_proc.dat).
            Ok(Some(syscache_seams::PgTypeBaseShape {
                typtype: if typid == UNKNOWNOID { b'p' as i8 } else { b'b' as i8 },
                typbasetype: InvalidOid,
                typtypmod: -1,
                typelem: if typid == 1007 { INT4OID } else { InvalidOid },
                typsubscript: if typid == 1007 { 6179 } else { InvalidOid },
            }))
        });
        syscache_seams::pg_type_typarray::set(|typid| {
            Ok((typid == INT4OID).then_some(1007))
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
            if name == "<>" && l == INT4OID && r == INT4OID {
                v.push((518, 11));
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            // 551 = int4pl (proc 177 -> int4); 521 = int4gt (proc 147 -> bool);
            // values verified vs pg_operator.dat/pg_proc.dat.
            Ok(match opno {
                551 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
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
                521 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
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
                96 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
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
                // 518 = int4ne (proc 144 -> bool).
                518 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: BOOLOID,
                    oprcom: 518,
                    oprnegate: 96,
                    oprcode: 144,
                    oprrest: 102,
                    oprjoin: 106,
                    oprcanmerge: false,
                    oprcanhash: false,
                }),
                _ => None,
            })
        });
        syscache_seams::pg_operator_name_candidates_exist::set(|name, _| {
            Ok(name == "+" || name == ">" || name == "=" || name == "<>")
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(match funcid {
                // 481 = int8(int4), the pg_cast int4->int8 coercion function;
                // 144 = int4ne.
                177 | 147 | 481 | 65 | 144 => Some(syscache_seams::PgProcShape {
                    prolang: 12,
                    prosecdef: false,
                    proconfig_isnull: true,
                    pronamespace: 11,
                    prorettype: match funcid {
                        147 | 65 | 144 => BOOLOID,
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
                    prolang: 12,
                    prosecdef: false,
                    proconfig_isnull: true,
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
                // sum(int4) 2108 / sum(int8) 2107; row_number/rank/dense_rank
                // 3100-3102.
                2107 => Some(syscache_seams::PgProcShape {
                    prolang: 12,
                    prosecdef: false,
                    proconfig_isnull: true,
                    pronamespace: 11,
                    prorettype: 1700,
                    provariadic: InvalidOid,
                    prosupport: InvalidOid,
                    pronargs: 1,
                    prokind: b'a' as i8,
                    provolatile: b'i' as i8,
                    proparallel: b's' as i8,
                    proretset: false,
                    proisstrict: false,
                    proleakproof: false,
                }),
                2108 => Some(syscache_seams::PgProcShape {
                    prolang: 12,
                    prosecdef: false,
                    proconfig_isnull: true,
                    pronamespace: 11,
                    prorettype: 20,
                    provariadic: InvalidOid,
                    prosupport: InvalidOid,
                    pronargs: 1,
                    prokind: b'a' as i8,
                    provolatile: b'i' as i8,
                    proparallel: b's' as i8,
                    proretset: false,
                    proisstrict: false,
                    proleakproof: false,
                }),
                3100 | 3101 | 3102 => Some(syscache_seams::PgProcShape {
                    prolang: 12,
                    prosecdef: false,
                    proconfig_isnull: true,
                    pronamespace: 11,
                    prorettype: 20,
                    provariadic: InvalidOid,
                    prosupport: InvalidOid,
                    pronargs: 0,
                    prokind: b'w' as i8,
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
            if proname == "sum" {
                let mut int4arg = mcx::vec_with_capacity_in(mcx, 1)?;
                int4arg.push(23);
                v.push(syscache_seams::PgProcCandidate {
                    oid: 2108,
                    pronamespace: 11,
                    pronargs: 1,
                    pronargdefaults: 0,
                    provariadic: InvalidOid,
                    proargtypes: int4arg,
                });
                let mut int8arg = mcx::vec_with_capacity_in(mcx, 1)?;
                int8arg.push(20);
                v.push(syscache_seams::PgProcCandidate {
                    oid: 2107,
                    pronamespace: 11,
                    pronargs: 1,
                    pronargdefaults: 0,
                    provariadic: InvalidOid,
                    proargtypes: int8arg,
                });
            }
            let winoid = match proname {
                "row_number" => Some(3100),
                "rank" => Some(3101),
                "dense_rank" => Some(3102),
                _ => None,
            };
            if let Some(oid) = winoid {
                v.push(syscache_seams::PgProcCandidate {
                    oid,
                    pronamespace: 11,
                    pronargs: 0,
                    pronargdefaults: 0,
                    provariadic: InvalidOid,
                    proargtypes: mcx::PgVec::new_in(mcx),
                });
            }
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
            Ok((aggfnoid == 2803 || aggfnoid == 2108 || aggfnoid == 2107).then_some(syscache_seams::PgAggregateShape {
                aggkind: b'n' as i8,
                aggnumdirectargs: 0,
                aggtransfn: 1219,
                aggfinalfn: InvalidOid,
                aggcombinefn: 463,
                aggserialfn: InvalidOid,
                aggdeserialfn: InvalidOid,
                aggfinalextra: false,
                aggfinalmodify: b'r' as i8,
                aggsortop: 0,
                aggtranstype: 20,
                aggmtransfn: 0,
                aggminvtransfn: 0,
                aggmfinalfn: 0,
                aggmfinalextra: false,
                aggmfinalmodify: b'r' as i8,
                aggmtranstype: 0,
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
        // pg_type.dat typcategory/typispreferred for the fixture types.
        syscache_seams::pg_type_category::set(|typid| {
            Ok(match typid {
                TEXTOID => Some((b'S' as i8, true)),
                INT4OID | INT8OID => Some((b'N' as i8, false)),
                BOOLOID => Some((b'B' as i8, true)),
                UNKNOWNOID => Some((b'X' as i8, false)),
                _ => None,
            })
        });
        // 1978/1979 = int4 btree/hash default opclasses over the 1976/1977
        // integer_ops families (pg_opclass.dat) — the ORDER BY operator spine.
        syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
            Ok(match opclass {
                1978 => Some(syscache_seams::PgOpclassShape {
                    opcmethod: types_core::BTREE_AM_OID,
                    opcfamily: 1976,
                    opcintype: INT4OID,
                    // int4 opclasses store no separate key type (pg_opclass: 0).
                    opckeytype: ::types_core::InvalidOid,
                }),
                1979 => Some(syscache_seams::PgOpclassShape {
                    opcmethod: lsyscache::HASH_AM_OID,
                    opcfamily: 1977,
                    opcintype: INT4OID,
                    // int4 opclasses store no separate key type (pg_opclass: 0).
                    opckeytype: ::types_core::InvalidOid,
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
    // Past FirstUnpinnedObjectId, so ON CONFLICT's catalog-relation gate
    // does not fire.
    const U_OID: Oid = 40000;

    fn make_t(mcx: Mcx<'_>) -> Relation<'_> {
        make_rel(mcx, "t", T_OID)
    }

    fn make_rel<'m>(mcx: Mcx<'m>, name: &str, oid: Oid) -> Relation<'m> {
        let mut relname = NameData::default();
        relname.namestrcpy(name);
        let cols = [("x", INT4OID, types_core::InvalidOid), ("y", TEXTOID, 100)];
        let mut attrs = Vec::new();
        for (i, (name, typid, coll)) in cols.iter().enumerate() {
            let mut a = FormData_pg_attribute {
                attrelid: oid,
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
            rd_id: oid,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: std::cell::Cell::new(true),
            rd_createSubid: std::cell::Cell::new(0),
            rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_droppedSubid: std::cell::Cell::new(0),
            rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: oid, dbId: 5 } },
            rd_rel: FormData_pg_class {
                relname,
                relnamespace: 2200,
                reltype: 0,
                relowner: 10,
                relam: 2,
                relfilenode: oid,
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
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
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
            "u" => Ok(Some(make_rel(mcx, "u", U_OID))),
            _ if missing_ok => Ok(None),
            _ => Err(types_error::PgError::error("no such relation").into()),
        }
    }

    fn install() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            super::install_type_fixture();
            mbutils_seams::pg_mbstrlen_with_len::set(mbutils::pg_mbstrlen_with_len);
            relation_seams::relation_openrv_extended::set(fake_openrv_extended);
            // errorMissingRTE's searchRangeTableForRel probe; InvalidOid =
            // "no such relation", falling back to eref-alias matching.
            namespace_seams::range_var_get_relid::set(|_, _, _, _| Ok(types_core::InvalidOid));
            // check_functional_grouping's pkey projection: fixtures have no
            // primary keys, so None keeps C's 42803 paths live.
            syscache_seams::pg_constraint_primary_key_attnos::set(|_, _, _| Ok(None));
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
        // RTE_GROUP substitution landed with the groupingsets lane (C
        // parse_analyze.c/parse_clause.c since PG17's RTE_GROUP: a query
        // with GROUP BY gets the group RTE and hasGroupRTE = true) — the
        // formerly-recorded divergence is retired; this pin now asserts
        // C's behavior.
        assert!(q.hasGroupRTE, "RTE_GROUP substitution is C's behavior (divergence retired)");
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

    fn simple_refs(gs: &types_nodes::parsenodes::GroupingSet<'_>) -> Vec<i32> {
        use types_nodes::parsenodes::GroupingSetKind;
        assert_eq!(gs.kind, GroupingSetKind::GROUPING_SET_SIMPLE);
        gs.content.iter().map(|n| n.as_integer().unwrap().ival).collect()
    }

    #[test]
    fn group_by_rollup_end_to_end() {
        use types_nodes::parsenodes::GroupingSetKind;
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT t.x, u.x, count(*) FROM t, t u GROUP BY ROLLUP(t.x, u.x)",
        )
        .unwrap();

        assert_eq!(q.groupClause.len(), 2);
        let r0 = q.targetList.nth(0).as_target_entry().unwrap().ressortgroupref;
        let r1 = q.targetList.nth(1).as_target_entry().unwrap().ressortgroupref;
        assert!(r0 > 0 && r1 > 0);
        assert_eq!(q.groupingSets.len(), 1);
        let gs = q.groupingSets.nth(0).as_grouping_set().unwrap();
        assert_eq!(gs.kind, GroupingSetKind::GROUPING_SET_ROLLUP);
        assert_eq!(gs.content.len(), 2);
        assert_eq!(simple_refs(gs.content.nth(0).as_grouping_set().unwrap()), [r0 as i32]);
        assert_eq!(simple_refs(gs.content.nth(1).as_grouping_set().unwrap()), [r1 as i32]);
    }

    #[test]
    fn group_by_cube_end_to_end() {
        use types_nodes::parsenodes::GroupingSetKind;
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT t.x, u.x, count(*) FROM t, t u GROUP BY CUBE(t.x, u.x)",
        )
        .unwrap();

        assert_eq!(q.groupClause.len(), 2);
        assert_eq!(q.groupingSets.len(), 1);
        let gs = q.groupingSets.nth(0).as_grouping_set().unwrap();
        assert_eq!(gs.kind, GroupingSetKind::GROUPING_SET_CUBE);
        assert_eq!(gs.content.len(), 2);
    }

    #[test]
    fn group_by_grouping_sets_end_to_end() {
        use types_nodes::parsenodes::GroupingSetKind;
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT t.x, u.x, count(*) FROM t, t u \
             GROUP BY GROUPING SETS ((t.x), (u.x), ())",
        )
        .unwrap();

        assert_eq!(q.groupClause.len(), 2);
        let r0 = q.targetList.nth(0).as_target_entry().unwrap().ressortgroupref;
        let r1 = q.targetList.nth(1).as_target_entry().unwrap().ressortgroupref;
        assert_eq!(q.groupingSets.len(), 1);
        let gs = q.groupingSets.nth(0).as_grouping_set().unwrap();
        assert_eq!(gs.kind, GroupingSetKind::GROUPING_SET_SETS);
        assert_eq!(gs.content.len(), 3);
        assert_eq!(simple_refs(gs.content.nth(0).as_grouping_set().unwrap()), [r0 as i32]);
        assert_eq!(simple_refs(gs.content.nth(1).as_grouping_set().unwrap()), [r1 as i32]);
        let empty = gs.content.nth(2).as_grouping_set().unwrap();
        assert_eq!(empty.kind, GroupingSetKind::GROUPING_SET_EMPTY);
        assert!(empty.content.is_nil());
    }

    #[test]
    fn group_by_empty_parens_end_to_end() {
        use types_nodes::parsenodes::GroupingSetKind;
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT count(*) FROM t GROUP BY ()").unwrap();
        assert!(q.groupClause.is_nil());
        assert_eq!(q.groupingSets.len(), 1);
        let gs = q.groupingSets.nth(0).as_grouping_set().unwrap();
        assert_eq!(gs.kind, GroupingSetKind::GROUPING_SET_EMPTY);
        assert!(gs.content.is_nil());

        // No aggregates at all: parseCheckAggregates still runs (and rejects
        // ungrouped columns) on groupingSets alone.
        let q = analyze_sql(mcx, "SELECT 1 FROM t GROUP BY ()").unwrap();
        assert!(!q.hasAggs);
        assert_eq!(q.groupingSets.len(), 1);
        let err = analyze_sql(mcx, "SELECT x FROM t GROUP BY ()").map(|_| ()).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_GROUPING_ERROR);
    }

    #[test]
    fn single_grouping_set_collapses_to_plain_group_by() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x, count(*) FROM t GROUP BY GROUPING SETS ((x))")
            .unwrap();
        assert_eq!(q.groupClause.len(), 1);
        assert!(q.groupingSets.is_nil(), "single-set expansion drops the grouping sets");
    }

    #[test]
    fn grouping_func_refs_resolve() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT t.x, u.x, GROUPING(t.x, u.x), count(*) FROM t, t u \
             GROUP BY ROLLUP(t.x, u.x)",
        )
        .unwrap();

        assert!(q.hasAggs);
        let r0 = q.targetList.nth(0).as_target_entry().unwrap().ressortgroupref;
        let r1 = q.targetList.nth(1).as_target_entry().unwrap().ressortgroupref;
        let t2 = q.targetList.nth(2).as_target_entry().unwrap();
        assert_eq!(t2.resname, Some("grouping"));
        let grp = t2.expr.as_grouping_func().unwrap();
        assert_eq!(grp.agglevelsup, 0);
        assert_eq!(grp.args.len(), 2);
        let refs: Vec<i32> = grp.refs.iter().collect();
        assert_eq!(refs, [r0 as i32, r1 as i32]);
    }

    #[test]
    fn grouping_alone_sets_hasaggs_and_runs_check() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT GROUPING(x) FROM t GROUP BY x").unwrap();
        assert!(q.hasAggs);
        let grp =
            q.targetList.nth(0).as_target_entry().unwrap().expr.as_grouping_func().unwrap();
        assert_eq!(grp.refs.len(), 1);
    }

    #[test]
    fn grouping_of_ungrouped_column_is_42803() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT GROUPING(y) FROM t GROUP BY x")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_GROUPING_ERROR);
        assert_eq!(
            err.message(),
            "arguments to GROUPING must be grouping expressions of the associated query level"
        );
    }

    #[test]
    fn grouping_with_32_args_is_54023() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let args = ["x"; 32].join(", ");
        let err = analyze_sql(mcx, &format!("SELECT GROUPING({args}) FROM t GROUP BY x"))
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_TOO_MANY_ARGUMENTS);
        assert_eq!(err.message(), "GROUPING must have fewer than 32 arguments");
    }

    #[test]
    fn cube_with_13_elements_is_54011() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let cols = ["x"; 13].join(", ");
        let err = analyze_sql(
            mcx,
            &format!("SELECT count(*) FROM t GROUP BY CUBE({cols})"),
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_TOO_MANY_COLUMNS);
        assert_eq!(err.message(), "CUBE is limited to 12 elements");
    }

    #[test]
    fn grouping_sets_expansion_over_4096_is_54001() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        // 4^7 = 16384 expanded sets.
        let clause = ["CUBE(x, x)"; 7].join(", ");
        let err = analyze_sql(
            mcx,
            &format!("SELECT x, count(*) FROM t GROUP BY {clause}"),
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_STATEMENT_TOO_COMPLEX);
        assert_eq!(err.message(), "too many grouping sets present (maximum 4096)");
    }

    #[test]
    fn window_functions_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT x, row_number() OVER (PARTITION BY x ORDER BY x), \
             rank() OVER (ORDER BY x), sum(x) OVER (PARTITION BY x) FROM t",
        )
        .unwrap();

        assert!(q.hasWindowFuncs);
        assert!(!q.hasAggs);
        assert_eq!(q.windowClause.len(), 3);

        let wf1 = q.targetList.nth(1).as_target_entry().unwrap().expr.as_window_func().unwrap();
        assert_eq!((wf1.winfnoid, wf1.wintype, wf1.winref, wf1.winagg), (3100, 20, 1, false));
        let wf2 = q.targetList.nth(2).as_target_entry().unwrap().expr.as_window_func().unwrap();
        assert_eq!((wf2.winfnoid, wf2.winref, wf2.winagg), (3101, 2, false));
        let wf3 = q.targetList.nth(3).as_target_entry().unwrap().expr.as_window_func().unwrap();
        assert_eq!((wf3.winfnoid, wf3.winref, wf3.winagg), (2108, 3, true));
        assert_eq!(wf3.args.len(), 1);

        let wc1 = q.windowClause.nth(0).as_window_clause().unwrap();
        assert_eq!(wc1.winref, 1);
        assert_eq!(wc1.partitionClause.len(), 1);
        assert_eq!(wc1.orderClause.len(), 1);
        assert_eq!(
            wc1.frameOptions,
            types_nodes::rawnodes::FRAMEOPTION_DEFAULTS
        );
        let part = wc1.partitionClause.nth(0).as_sort_group_clause().unwrap();
        let t0 = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(part.tleSortGroupRef, t0.ressortgroupref);
        let ord = wc1.orderClause.nth(0).as_sort_group_clause().unwrap();
        // ORDER BY x reuses the non-junk x entry (SQL99 Var-equality leg).
        assert_eq!(ord.tleSortGroupRef, t0.ressortgroupref);

        let wc2 = q.windowClause.nth(1).as_window_clause().unwrap();
        assert_eq!((wc2.winref, wc2.partitionClause.len(), wc2.orderClause.len()), (2, 0, 1));
        let wc3 = q.windowClause.nth(2).as_window_clause().unwrap();
        assert_eq!((wc3.winref, wc3.partitionClause.len(), wc3.orderClause.len()), (3, 1, 0));
    }

    #[test]
    fn window_duplicate_over_specs_share_one_clause() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT rank() OVER (ORDER BY x), dense_rank() OVER (ORDER BY x) FROM t",
        )
        .unwrap();
        assert_eq!(q.windowClause.len(), 1);
        let wf1 = q.targetList.nth(0).as_target_entry().unwrap().expr.as_window_func().unwrap();
        let wf2 = q.targetList.nth(1).as_target_entry().unwrap().expr.as_window_func().unwrap();
        assert_eq!((wf1.winref, wf2.winref), (1, 1));
        assert_eq!(wf2.winfnoid, 3102);
    }

    #[test]
    fn named_window_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT count(*) OVER w FROM t WINDOW w AS (PARTITION BY x ORDER BY x DESC)",
        )
        .unwrap();
        assert_eq!(q.windowClause.len(), 1);
        let wc = q.windowClause.nth(0).as_window_clause().unwrap();
        assert_eq!(wc.name, Some("w"));
        assert_eq!(wc.winref, 1);
        assert_eq!(wc.partitionClause.len(), 1);
        let ord = wc.orderClause.nth(0).as_sort_group_clause().unwrap();
        assert!(ord.reverse_sort);
        let wf = q.targetList.nth(0).as_target_entry().unwrap().expr.as_window_func().unwrap();
        assert_eq!((wf.winfnoid, wf.winref, wf.winagg, wf.winstar), (2803, 1, true, true));
    }

    #[test]
    fn window_refname_copies_partition() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "SELECT rank() OVER (w ORDER BY x) FROM t WINDOW w AS (PARTITION BY x)",
        )
        .unwrap();
        assert_eq!(q.windowClause.len(), 2);
        let wc2 = q.windowClause.nth(1).as_window_clause().unwrap();
        assert_eq!(wc2.refname, Some("w"));
        assert_eq!(wc2.partitionClause.len(), 1);
        assert_eq!(wc2.orderClause.len(), 1);
        assert!(!wc2.copiedOrder);
    }

    #[test]
    fn window_function_without_over_is_42809() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT rank() FROM t").map(|_| ()).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_WRONG_OBJECT_TYPE);
        assert_eq!(err.message(), "window function rank requires an OVER clause");
    }

    #[test]
    fn window_function_in_where_is_42p20() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT 1 FROM t WHERE row_number() OVER () = 1")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_WINDOWING_ERROR);
        assert_eq!(err.message(), "window functions are not allowed in WHERE");
    }

    #[test]
    fn undefined_window_is_42704() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT count(*) OVER w FROM t").map(|_| ()).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_OBJECT);
        assert_eq!(err.message(), "window \"w\" does not exist");
    }

    #[test]
    fn duplicate_window_name_is_42p20() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT 1 FROM t WINDOW w AS (), w AS ()")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_WINDOWING_ERROR);
        assert_eq!(err.message(), "window \"w\" is already defined");
    }

    #[test]
    fn nested_window_calls_are_42p20() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT sum(rank() OVER ()) OVER () FROM t")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_WINDOWING_ERROR);
        assert_eq!(err.message(), "window function calls cannot be nested");
    }

    #[test]
    fn window_arg_must_be_grouped_42803() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT sum(x) OVER () FROM t HAVING true")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_GROUPING_ERROR);
        assert!(err.message().contains("column \"t.x\" must appear"), "{}", err.message());
    }

    #[test]
    fn cannot_override_partition_by_42p20() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "SELECT rank() OVER (w PARTITION BY x) FROM t WINDOW w AS (PARTITION BY x)",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_WINDOWING_ERROR);
        assert_eq!(err.message(), "cannot override PARTITION BY clause of window \"w\"");
    }

    #[test]
    fn cannot_override_order_by_42p20() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "SELECT rank() OVER (w ORDER BY x) FROM t WINDOW w AS (ORDER BY x)",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_WINDOWING_ERROR);
        assert_eq!(err.message(), "cannot override ORDER BY clause of window \"w\"");
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

    // Query shape asserted against C 18.3 for
    // `SELECT t.x FROM t JOIN t u ON t.x = u.x` over t(x int4, y text).
    #[test]
    fn explicit_join_on_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT t.x FROM t JOIN t u ON t.x = u.x").unwrap();

        assert_eq!(q.rtable.len(), 3);
        assert_eq!(q.rteperminfos.len(), 2);
        let jrte = q.rtable.nth(2).as_range_tbl_entry().unwrap();
        assert_eq!(jrte.rtekind, RTEKind::RTE_JOIN);
        assert_eq!(jrte.jointype, types_nodes::JoinType::JOIN_INNER);
        assert_eq!(jrte.joinmergedcols, 0);
        assert!(jrte.join_using_alias.is_none() && jrte.alias.is_none());
        assert_eq!(jrte.perminfoindex, 0);
        assert!(jrte.inFromCl && !jrte.lateral);
        let eref = jrte.eref.unwrap();
        assert_eq!(eref.aliasname, Some("unnamed_join"));
        let names: Vec<_> =
            eref.colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
        assert_eq!(names, ["x", "y", "x", "y"]);
        assert_eq!(jrte.joinaliasvars.len(), 4);
        for (i, (varno, attno)) in [(1, 1), (1, 2), (2, 1), (2, 2)].iter().enumerate() {
            let v = jrte.joinaliasvars.nth(i).as_var().unwrap();
            assert_eq!((v.varno, v.varattno as i32), (*varno, *attno));
        }
        let lcols: Vec<_> = jrte.joinleftcols.iter().collect();
        let rcols: Vec<_> = jrte.joinrightcols.iter().collect();
        assert_eq!(lcols, [1, 2]);
        assert_eq!(rcols, [1, 2]);

        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert!(jt.quals.is_none());
        let j = jt.fromlist.nth(0).as_join_expr().unwrap();
        assert_eq!(j.jointype, types_nodes::JoinType::JOIN_INNER);
        assert_eq!(j.rtindex, 3);
        assert_eq!(j.larg.as_range_tbl_ref().unwrap().rtindex, 1);
        assert_eq!(j.rarg.as_range_tbl_ref().unwrap().rtindex, 2);
        let qual = j.quals.unwrap().as_op_expr().unwrap();
        assert_eq!(qual.opno, 96);
        let lv = qual.args.nth(0).as_var().unwrap();
        let rv = qual.args.nth(1).as_var().unwrap();
        assert_eq!((lv.varno, lv.varattno), (1, 1));
        assert_eq!((rv.varno, rv.varattno), (2, 1));

        // The tlist var resolves through the base rel, not the join RTE.
        let t0 = q.targetList.nth(0).as_target_entry().unwrap();
        let v = t0.expr.as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (1, 1));

        // SELECT * expands the join nsitem to base-rel Vars.
        let q = analyze_sql(mcx, "SELECT * FROM t JOIN t u ON t.x = u.x").unwrap();
        let vars: Vec<_> = q
            .targetList
            .iter()
            .map(|n| {
                let v = n.as_target_entry().unwrap().expr.as_var().unwrap();
                (v.varno, v.varattno)
            })
            .collect();
        assert_eq!(vars, [(1, 1), (1, 2), (2, 1), (2, 2)]);

        // Unqualified refs resolve through the join namespace: ambiguous here.
        let err =
            analyze_sql(mcx, "SELECT x FROM t JOIN t u ON t.x = u.x").map(|_| ()).unwrap_err();
        assert_eq!(err.message(), "column reference \"x\" is ambiguous");

        // Join alias hides the inputs.
        let err = analyze_sql(mcx, "SELECT t.x FROM (t JOIN t u ON t.x = u.x) j")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(
            err.message(),
            "invalid reference to FROM-clause entry for table \"t\""
        );
        let q =
            analyze_sql(mcx, "SELECT j.a FROM (t JOIN t u ON t.x = u.x) AS j(a, b, c, d)")
                .unwrap();
        let jrte = q.rtable.nth(2).as_range_tbl_entry().unwrap();
        assert_eq!(jrte.alias.unwrap().aliasname, Some("j"));
        let eref = jrte.eref.unwrap();
        assert_eq!(eref.aliasname, Some("j"));
        let names: Vec<_> =
            eref.colnames.iter().map(|n| n.as_string().unwrap().sval).collect();
        assert_eq!(names, ["a", "b", "c", "d"]);
        let v = q.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (1, 1));

        // Nested joins chain left-deep with a second join RTE.
        let q = analyze_sql(
            mcx,
            "SELECT t.x FROM t JOIN t u ON t.x = u.x JOIN t v ON u.x = v.x",
        )
        .unwrap();
        assert_eq!(q.rtable.len(), 5);
        let j = q.jointree.unwrap().fromlist.nth(0).as_join_expr().unwrap();
        assert_eq!(j.rtindex, 5);
        assert_eq!(j.larg.as_join_expr().unwrap().rtindex, 3);
        assert_eq!(j.rarg.as_range_tbl_ref().unwrap().rtindex, 4);
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
    fn insert_on_conflict_do_update_end_to_end() {
        use types_nodes::primnodes::OnConflictAction;
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "INSERT INTO u VALUES (1, 'foo') ON CONFLICT (x) DO UPDATE SET y = excluded.y \
             WHERE u.x > 0",
        )
        .unwrap();
        assert_eq!(q.commandType, CmdType::CMD_INSERT);
        let oc = q.onConflict.unwrap().as_on_conflict_expr().unwrap();
        assert_eq!(oc.action, OnConflictAction::ONCONFLICT_UPDATE);
        assert_eq!(oc.constraint, types_core::InvalidOid);

        assert_eq!(q.rtable.len(), 2);
        assert_eq!(oc.exclRelIndex, 2);
        let excl_rte = q.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(excl_rte.relid, U_OID);
        assert_eq!(excl_rte.relkind, types_rel::RELKIND_COMPOSITE_TYPE);
        assert_eq!(excl_rte.eref.unwrap().aliasname, Some("excluded"));

        assert_eq!(oc.arbiterElems.len(), 1);
        let elem = oc.arbiterElems.nth(0).as_inference_elem().unwrap();
        let v = elem.expr.unwrap().as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (1, 1));
        assert!(oc.arbiterWhere.is_none());

        assert_eq!(oc.onConflictSet.len(), 1);
        let te = oc.onConflictSet.nth(0).as_target_entry().unwrap();
        assert_eq!((te.resno, te.resname), (2, Some("y")));
        let sv = te.expr.as_var().unwrap();
        assert_eq!((sv.varno, sv.varattno), (2, 2));

        let wv = oc.onConflictWhere.unwrap();
        assert_eq!(parse_expr::expr_type(wv), BOOLOID);

        // Per-column Vars at resnos 1..natts, then the resjunk whole-row Var.
        assert_eq!(oc.exclRelTlist.len(), 3);
        let t0 = oc.exclRelTlist.nth(0).as_target_entry().unwrap();
        assert_eq!((t0.resno, t0.expr.as_var().unwrap().varattno), (1, 1));
        let tw = oc.exclRelTlist.nth(2).as_target_entry().unwrap();
        assert!(tw.resjunk);
        assert_eq!((tw.resno, tw.expr.as_var().unwrap().varattno), (0, 0));

        let q =
            analyze_sql(mcx, "INSERT INTO u VALUES (1, 'foo') ON CONFLICT DO NOTHING").unwrap();
        let oc = q.onConflict.unwrap().as_on_conflict_expr().unwrap();
        assert_eq!(oc.action, OnConflictAction::ONCONFLICT_NOTHING);
        assert!(oc.arbiterElems.is_nil() && oc.arbiterWhere.is_none());
        assert_eq!(oc.exclRelIndex, 0);
        assert!(oc.exclRelTlist.is_nil() && oc.onConflictSet.is_nil());
        assert_eq!(q.rtable.len(), 1);
    }

    #[test]
    fn insert_on_conflict_error_shapes() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err =
            analyze_sql(mcx, "INSERT INTO u VALUES (1, 'a') ON CONFLICT DO UPDATE SET y = 'b'")
                .map(|_| ())
                .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
        assert_eq!(
            err.message,
            "ON CONFLICT DO UPDATE requires inference specification or constraint name"
        );

        // t's OID is below FirstUnpinnedObjectId, i.e. a catalog relation.
        let err = analyze_sql(mcx, "INSERT INTO t VALUES (1, 'a') ON CONFLICT (x) DO NOTHING")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message, "ON CONFLICT is not supported with system catalog tables");

        let err =
            analyze_sql(mcx, "INSERT INTO u VALUES (1, 'a') ON CONFLICT (x DESC) DO NOTHING")
                .map(|_| ())
                .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message, "ASC/DESC is not allowed in ON CONFLICT clause");

        let err = analyze_sql(mcx, "INSERT INTO u VALUES (1, 'a') ON CONFLICT (nope) DO NOTHING")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_COLUMN);
    }

    #[test]
    fn bare_values_order_by_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "VALUES (3), (1), (2) ORDER BY 1").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_SELECT);
        assert_eq!(q.rtable.len(), 1);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.rtekind, RTEKind::RTE_VALUES);
        assert!(rte.inFromCl && !rte.lateral);
        assert_eq!(rte.values_lists.len(), 3);
        assert_eq!(rte.coltypes.nth(0), INT4OID);
        assert_eq!(rte.coltypmods.nth(0), -1);
        assert_eq!(rte.colcollations.nth(0), types_core::InvalidOid);
        let eref = rte.eref.unwrap();
        assert_eq!(eref.aliasname, Some("*VALUES*"));
        assert_eq!(eref.colnames.nth(0).as_string().unwrap().sval, "column1");
        let row0 = rte.values_lists.nth(0).as_list().unwrap();
        let c = row0.nth(0).as_const().unwrap();
        assert_eq!((c.consttype, c.constvalue.as_i32()), (INT4OID, 3));

        assert_eq!(q.targetList.len(), 1);
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te.resno, te.resname, te.ressortgroupref), (1, Some("column1"), 1));
        let var = te.expr.as_var().unwrap();
        assert_eq!((var.varno, var.varattno, var.vartype), (1, 1, INT4OID));

        assert_eq!(q.sortClause.len(), 1);
        let s = q.sortClause.nth(0).as_sort_group_clause().unwrap();
        assert_eq!((s.tleSortGroupRef, s.eqop, s.sortop), (1, 96, 97));

        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 1);
        assert!(jt.quals.is_none());
    }

    #[test]
    fn bare_values_desc_limit() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "VALUES (3), (1), (2) ORDER BY 1 DESC LIMIT 2").unwrap();
        let s = q.sortClause.nth(0).as_sort_group_clause().unwrap();
        assert_eq!((s.eqop, s.sortop), (96, 521));
        assert!(s.nulls_first);
        assert_eq!(
            q.limitOption,
            types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT
        );
        let f = q.limitCount.unwrap().as_func_expr().unwrap();
        assert_eq!(f.funcid, 481);
        assert_eq!(f.args.nth(0).as_const().unwrap().constvalue.as_i32(), 2);
    }

    #[test]
    fn bare_values_unknown_resolves_to_text() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "VALUES ('a'), ('b')").unwrap();
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.coltypes.nth(0), TEXTOID);
        assert_eq!(rte.colcollations.nth(0), 100);
        for row in &rte.values_lists {
            let c = row.as_list().unwrap().nth(0).as_const().unwrap();
            assert_eq!(c.consttype, TEXTOID);
        }
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(te.expr.as_var().unwrap().vartype, TEXTOID);
    }

    #[test]
    fn bare_values_length_mismatch_is_42601() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "VALUES (1), (2, 3)").map(|_| ()).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
        assert_eq!(err.message, "VALUES lists must all be the same length");
    }

    #[test]
    fn values_in_from_subquery_with_column_aliases() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        // Raw tree built by hand: the AS v(n, s) gram action is another lane's.
        let rows = [
            [int_c(mcx, 2), str_c(mcx, "b")],
            [int_c(mcx, 1), str_c(mcx, "a")],
        ];
        let mut values_lists = types_nodes::NodeList::nil();
        for row in rows {
            let l = types_nodes::NodeList::from_slice(mcx, &row).unwrap();
            values_lists
                .lappend(mcx, types_nodes::Node::mk_list(mcx, l).unwrap())
                .unwrap();
        }
        let sub = types_nodes::Node::mk(
            mcx,
            types_nodes::rawnodes::SelectStmt { valuesLists: values_lists, ..Default::default() },
        )
        .unwrap();
        let mut colnames = types_nodes::NodeList::nil();
        for name in ["n", "s"] {
            colnames
                .lappend(
                    mcx,
                    types_nodes::Node::mk_string(mcx, name).unwrap(),
                )
                .unwrap();
        }
        let alias = &*mcx::leak_in(
            mcx::alloc_in(
                mcx,
                types_nodes::Alias { aliasname: Some("v"), colnames },
            )
            .unwrap(),
        );
        let rss = types_nodes::Node::mk(
            mcx,
            types_nodes::rawnodes::RangeSubselect {
                lateral: false,
                subquery: Some(sub),
                alias: Some(alias),
            },
        )
        .unwrap();
        let star = types_nodes::NodeList::make1(
            mcx,
            types_nodes::Node::mk_a_star(mcx).unwrap(),
        )
        .unwrap();
        let star_ref = types_nodes::Node::mk_column_ref(mcx, star, 7).unwrap();
        let target =
            types_nodes::Node::mk_res_target(mcx, None, types_nodes::NodeList::nil(), Some(star_ref), 7)
                .unwrap();
        let stmt = types_nodes::Node::mk(
            mcx,
            types_nodes::rawnodes::SelectStmt {
                targetList: types_nodes::NodeList::make1(mcx, target).unwrap(),
                fromClause: types_nodes::NodeList::make1(mcx, rss).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let raw_stmt = types_nodes::rawnodes::RawStmt {
            stmt: Some(stmt),
            stmt_location: 0,
            stmt_len: 44,
        };

        let q = parse_analyze_fixedparams(
            mcx,
            &raw_stmt,
            "SELECT * FROM (VALUES (2, 'b'), (1, 'a')) AS v(n, s)",
            &[],
            Default::default(),
        )
        .unwrap();

        assert_eq!(q.rtable.len(), 1);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.rtekind, RTEKind::RTE_SUBQUERY);
        let eref = rte.eref.unwrap();
        assert_eq!(eref.aliasname, Some("v"));
        assert_eq!(eref.colnames.nth(0).as_string().unwrap().sval, "n");
        assert_eq!(eref.colnames.nth(1).as_string().unwrap().sval, "s");
        let sub = rte.subquery.unwrap();
        assert_eq!(sub.commandType, CmdType::CMD_SELECT);
        assert_eq!(
            sub.rtable.nth(0).as_range_tbl_entry().unwrap().rtekind,
            RTEKind::RTE_VALUES
        );

        assert_eq!(q.targetList.len(), 2);
        let t0 = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((t0.resname, t0.expr.as_var().unwrap().vartype), (Some("n"), INT4OID));
        let v0 = t0.expr.as_var().unwrap();
        assert_eq!((v0.varno, v0.varattno), (1, 1));
        let t1 = q.targetList.nth(1).as_target_entry().unwrap();
        assert_eq!((t1.resname, t1.expr.as_var().unwrap().vartype), (Some("s"), TEXTOID));
    }

    fn int_c(mcx: Mcx<'_>, ival: i32) -> types_nodes::Node<'_> {
        types_nodes::Node::mk_a_const(
            mcx,
            Some(types_nodes::rawnodes::ValUnion::Integer(types_nodes::Integer { ival })),
            -1,
        )
        .unwrap()
    }

    fn str_c<'m>(mcx: Mcx<'m>, s: &'m str) -> types_nodes::Node<'m> {
        types_nodes::Node::mk_a_const(
            mcx,
            Some(types_nodes::rawnodes::ValUnion::String(types_nodes::String { sval: s })),
            -1,
        )
        .unwrap()
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
    fn update_from_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "UPDATE t SET y = u.y FROM u WHERE t.x = u.x").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_UPDATE);
        assert_eq!(q.resultRelation, 1);
        assert_eq!(q.rtable.len(), 2);
        let rte_u = q.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(rte_u.relid, U_OID);
        assert!(rte_u.inFromCl);
        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 2);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 1);
        assert_eq!(jt.fromlist.nth(1).as_range_tbl_ref().unwrap().rtindex, 2);
        assert!(jt.quals.is_some());

        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!((te.resno, te.resname), (2, Some("y")));
        let v = te.expr.as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (2, 2));

        let perm_u = q.rteperminfos.nth(1).as_rte_permission_info().unwrap();
        assert_eq!(perm_u.requiredPerms, ACL_SELECT);
        assert!(perm_u.selectedCols.is_member(2 - FirstLowInvalidHeapAttributeNumber));
    }

    #[test]
    fn delete_using_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "DELETE FROM t USING u WHERE t.x = u.x").unwrap();
        assert_eq!(q.commandType, CmdType::CMD_DELETE);
        assert_eq!(q.resultRelation, 1);
        assert_eq!(q.rtable.len(), 2);
        assert_eq!(q.rtable.nth(1).as_range_tbl_entry().unwrap().relid, U_OID);
        let jt = q.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 2);
        assert!(jt.quals.is_some());
    }

    #[test]
    fn with_select_cte_on_update_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "WITH w AS (SELECT x FROM u) UPDATE t SET y = 'z' WHERE t.x IN (SELECT x FROM w)",
        )
        .unwrap();
        assert_eq!(q.commandType, CmdType::CMD_UPDATE);
        assert_eq!(q.cteList.len(), 1);
        assert!(!q.hasModifyingCTE && !q.hasRecursive);
        assert!(q.hasSubLinks);
    }

    #[test]
    fn with_dml_cte_returning_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "WITH w AS (UPDATE t SET y = 'z' RETURNING x) SELECT x FROM w")
            .unwrap();
        assert_eq!(q.commandType, CmdType::CMD_SELECT);
        assert!(q.hasModifyingCTE);
        assert_eq!(q.cteList.len(), 1);
        let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
        let cq = cte.ctequery.unwrap().as_query().unwrap();
        assert_eq!(cq.commandType, CmdType::CMD_UPDATE);
        assert!(!cq.canSetTag);
        assert_eq!(cq.returningList.len(), 1);
        assert_eq!(cte.ctecoltypes.len(), 1);
        assert_eq!(cte.ctecoltypes.nth(0), INT4OID);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.rtekind, RTEKind::RTE_CTE);
    }

    #[test]
    fn with_dml_cte_without_returning_is_0a000() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "WITH w AS (INSERT INTO t VALUES (1, 'a')) SELECT * FROM w")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message, "WITH query \"w\" does not have a RETURNING clause");
    }

    #[test]
    fn recursive_dml_cte_is_invalid_recursion() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE w AS (UPDATE t SET y = 'z' \
             WHERE x IN (SELECT x FROM w) RETURNING x) SELECT * FROM w",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_RECURSION);
        assert_eq!(
            err.message,
            "recursive query \"w\" must not contain data-modifying statements"
        );
    }

    #[test]
    fn returning_old_new_vars_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q =
            analyze_sql(mcx, "UPDATE t SET y = 'b' WHERE x = 1 RETURNING old.y, new.y").unwrap();
        assert_eq!(q.returningOldAlias, Some("old"));
        assert_eq!(q.returningNewAlias, Some("new"));
        assert_eq!(q.returningList.len(), 2);
        use types_nodes::primnodes::VarReturningType;
        let v0 = q.returningList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!(v0.varreturningtype, VarReturningType::VAR_RETURNING_OLD);
        assert_eq!((v0.varno, v0.varattno), (1, 2));
        let v1 = q.returningList.nth(1).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!(v1.varreturningtype, VarReturningType::VAR_RETURNING_NEW);
        assert_eq!((v1.varno, v1.varattno), (1, 2));
    }

    #[test]
    fn returning_with_old_new_aliases_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "UPDATE t SET y = 'b' RETURNING WITH (OLD AS o, NEW AS n) o.y, n.y",
        )
        .unwrap();
        assert_eq!(q.returningOldAlias, Some("o"));
        assert_eq!(q.returningNewAlias, Some("n"));
        use types_nodes::primnodes::VarReturningType;
        let v0 = q.returningList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!(v0.varreturningtype, VarReturningType::VAR_RETURNING_OLD);
        let v1 = q.returningList.nth(1).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!(v1.varreturningtype, VarReturningType::VAR_RETURNING_NEW);
    }

    #[test]
    fn returning_with_repeated_option_is_syntax_error() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "UPDATE t SET y = 'b' RETURNING WITH (old AS a, new AS b, old AS c) *",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
        assert_eq!(err.message(), "OLD cannot be specified multiple times");

        let err = analyze_sql(
            mcx,
            "UPDATE t SET y = 'b' RETURNING WITH (new AS a, new AS b) *",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.message(), "NEW cannot be specified multiple times");
    }

    #[test]
    fn returning_with_alias_conflict_is_42712() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "UPDATE t SET y = 'b' RETURNING WITH (new AS t) *")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DUPLICATE_ALIAS);
        assert_eq!(err.message(), "table name \"t\" specified more than once");

        let err = analyze_sql(mcx, "UPDATE t SET y = 'b' RETURNING WITH (old AS x, new AS x) *")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DUPLICATE_ALIAS);
        assert_eq!(err.message(), "table name \"x\" specified more than once");
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

    // Query shape asserted against C 18.3: x IN (1, 2) becomes
    // ScalarArrayOpExpr(= ANY) over an int4[] ArrayExpr; NOT IN becomes
    // ScalarArrayOpExpr(<> ALL).
    #[test]
    fn in_list_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x FROM t WHERE x IN (1, 2)").unwrap();
        let saop = q.jointree.unwrap().quals.unwrap().as_scalar_array_op_expr().unwrap();
        assert!(saop.useOr);
        // 96 = int4eq operator, 65 = int4eq proc.
        assert_eq!((saop.opno, saop.opfuncid), (96, 65));
        assert_eq!((saop.hashfuncid, saop.negfuncid), (types_core::InvalidOid, types_core::InvalidOid));
        assert_eq!(saop.inputcollid, types_core::InvalidOid);
        assert_eq!(saop.location, 24);
        assert_eq!(saop.args.len(), 2);
        let v = saop.args.nth(0).as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (1, 1));
        let arr = saop.args.nth(1).as_array_expr().unwrap();
        // 1007 = _int4.
        assert_eq!((arr.array_typeid, arr.element_typeid), (1007, 23));
        assert_eq!(arr.array_collid, types_core::InvalidOid);
        assert!(!arr.multidims);
        assert_eq!((arr.list_start, arr.list_end), (27, 32));
        assert_eq!(arr.location, -1);
        assert_eq!(arr.elements.len(), 2);
        assert_eq!(arr.elements.nth(0).as_const().unwrap().constvalue.as_i32(), 1);
        assert_eq!(arr.elements.nth(1).as_const().unwrap().constvalue.as_i32(), 2);

        let q = analyze_sql(mcx, "SELECT x FROM t WHERE x NOT IN (1, 2)").unwrap();
        let saop = q.jointree.unwrap().quals.unwrap().as_scalar_array_op_expr().unwrap();
        assert!(!saop.useOr);
        // 518 = int4ne operator, 144 = int4ne proc.
        assert_eq!((saop.opno, saop.opfuncid), (518, 144));
        assert_eq!(saop.args.nth(1).as_array_expr().unwrap().elements.len(), 2);
    }

    // C: a single-item IN list never builds a ScalarArrayOpExpr — it falls
    // back to the plain operator tree.
    #[test]
    fn in_single_item_end_to_end() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x FROM t WHERE x IN (1)").unwrap();
        let op = q.jointree.unwrap().quals.unwrap().as_op_expr().unwrap();
        assert_eq!(op.opno, 96);
        assert!(op.args.nth(0).as_var().is_some());
        assert_eq!(op.args.nth(1).as_const().unwrap().constvalue.as_i32(), 1);

        let q = analyze_sql(mcx, "SELECT x FROM t WHERE x NOT IN (1)").unwrap();
        let op = q.jointree.unwrap().quals.unwrap().as_op_expr().unwrap();
        assert_eq!(op.opno, 518);
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

    #[test]
    fn with_cte_single_reference() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "WITH w AS (SELECT x FROM t WHERE x > 5) SELECT * FROM w")
            .unwrap();
        assert!(!q.hasRecursive && !q.hasModifyingCTE);
        assert_eq!(q.cteList.len(), 1);
        let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
        assert_eq!(cte.ctename, Some("w"));
        assert!(!cte.cterecursive);
        assert_eq!(cte.cterefcount, 1);
        assert_eq!(cte.ctecolnames.len(), 1);
        assert_eq!(cte.ctecolnames.nth(0).as_string().unwrap().sval, "x");
        assert_eq!(cte.ctecoltypes.as_slice(), &[INT4OID]);
        let cq = cte.ctequery.unwrap().as_query().unwrap();
        assert_eq!(cq.commandType, CmdType::CMD_SELECT);
        assert!(!cq.canSetTag);

        assert_eq!(q.rtable.len(), 1);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(rte.rtekind, types_nodes::RTEKind::RTE_CTE);
        assert_eq!(rte.ctename, Some("w"));
        assert_eq!(rte.ctelevelsup, 0);
        assert!(!rte.self_reference);
        assert_eq!(rte.coltypes.as_slice(), &[INT4OID]);

        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(te.resname, Some("x"));
        let var = te.expr.as_var().unwrap();
        assert_eq!((var.varno, var.varattno, var.vartype), (1, 1, INT4OID));
    }

    #[test]
    fn with_cte_two_references_bumps_refcount() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "WITH w AS (SELECT x FROM t WHERE x > 5) SELECT * FROM w, w w2",
        )
        .unwrap();
        assert_eq!(q.cteList.len(), 1);
        let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
        assert_eq!(cte.cterefcount, 2);
        assert_eq!(q.rtable.len(), 2);
        let rte2 = q.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(rte2.rtekind, types_nodes::RTEKind::RTE_CTE);
        assert_eq!(rte2.eref.unwrap().aliasname, Some("w2"));
        assert_eq!(q.targetList.len(), 2);
    }

    #[test]
    fn with_cte_alias_columns_and_aliasing() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "WITH w (a, b) AS (SELECT x, y FROM t) SELECT a, b FROM w")
            .unwrap();
        let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
        assert_eq!(cte.ctecolnames.len(), 2);
        assert_eq!(cte.ctecolnames.nth(0).as_string().unwrap().sval, "a");
        assert_eq!(cte.ctecolnames.nth(1).as_string().unwrap().sval, "b");
        assert_eq!(cte.ctecoltypes.as_slice(), &[INT4OID, TEXTOID]);
        assert_eq!(cte.ctecolcollations.as_slice(), &[types_core::InvalidOid, 100]);
    }

    #[test]
    fn with_duplicate_cte_name_is_42712() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "WITH w AS (SELECT 1), w AS (SELECT 2) SELECT 1")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DUPLICATE_ALIAS);
        assert_eq!(err.message(), "WITH query name \"w\" specified more than once");
    }

    #[test]
    fn with_too_many_alias_columns_is_42p10() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "WITH w (a, b) AS (SELECT 1) SELECT a FROM w")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_COLUMN_REFERENCE);
        assert!(err.message().contains("has 1 columns available but 2 columns specified"));
    }

    #[test]
    fn with_forward_reference_gets_future_cte_hint() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH a AS (SELECT * FROM b), b AS (SELECT 1) SELECT * FROM a",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_TABLE);
        assert!(err.message().contains("relation \"b\" does not exist"), "{}", err.message());
        assert!(
            err.detail().unwrap_or_default().contains("There is a WITH item named \"b\""),
            "{:?}",
            err.detail()
        );
    }

    #[test]
    fn with_recursive_basic_union_all() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM w WHERE n > 0) \
             SELECT * FROM w",
        )
        .unwrap();
        assert!(q.hasRecursive);
        assert_eq!(q.cteList.len(), 1);
        let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
        assert_eq!(cte.ctename, Some("w"));
        assert!(cte.cterecursive);
        assert_eq!(cte.cterefcount, 1);
        assert_eq!(cte.ctecolnames.len(), 1);
        assert_eq!(cte.ctecolnames.nth(0).as_string().unwrap().sval, "n");
        assert_eq!(cte.ctecoltypes.as_slice(), &[INT4OID]);
        assert_eq!(cte.ctecoltypmods.as_slice(), &[-1]);
        assert_eq!(cte.ctecolcollations.as_slice(), &[types_core::InvalidOid]);

        let cq = cte.ctequery.unwrap().as_query().unwrap();
        assert_eq!(cq.commandType, CmdType::CMD_SELECT);
        assert!(!cq.canSetTag);
        assert!(cq.setOperations.is_some());
        assert_eq!(cq.rtable.len(), 2);
        let rec_leaf =
            cq.rtable.nth(1).as_range_tbl_entry().unwrap().subquery.unwrap();
        let wrte = rec_leaf.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(wrte.rtekind, RTEKind::RTE_CTE);
        assert!(wrte.self_reference);
        assert_eq!(wrte.ctename, Some("w"));
        assert_eq!(wrte.ctelevelsup, 2);
        assert_eq!(wrte.coltypes.as_slice(), &[INT4OID]);
        assert_eq!(wrte.eref.unwrap().colnames.nth(0).as_string().unwrap().sval, "n");

        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(te.resname, Some("n"));
        let var = te.expr.as_var().unwrap();
        assert_eq!((var.varno, var.varattno, var.vartype), (1, 1, INT4OID));
    }

    #[test]
    fn with_recursive_keyword_nonrecursive_cte() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "WITH RECURSIVE w AS (SELECT 1 AS a) SELECT * FROM w").unwrap();
        assert!(q.hasRecursive);
        let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
        assert!(!cte.cterecursive);
        assert_eq!(cte.cterefcount, 1);
        assert_eq!(cte.ctecoltypes.as_slice(), &[INT4OID]);
        let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert!(!rte.self_reference);
    }

    #[test]
    fn with_recursive_forward_reference_topo_sorted() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "WITH RECURSIVE a AS (SELECT * FROM b), b AS (SELECT 1 AS n) SELECT * FROM a",
        )
        .unwrap();
        assert_eq!(q.cteList.len(), 2);
        assert_eq!(q.cteList.nth(0).as_common_table_expr().unwrap().ctename, Some("b"));
        assert_eq!(q.cteList.nth(1).as_common_table_expr().unwrap().ctename, Some("a"));
        assert!(!q.cteList.nth(0).as_common_table_expr().unwrap().cterecursive);
        assert!(!q.cteList.nth(1).as_common_table_expr().unwrap().cterecursive);
    }

    #[test]
    fn with_recursive_unknown_column_resolves_to_text() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(
            mcx,
            "WITH RECURSIVE w AS (SELECT 'foo' AS f UNION ALL SELECT f FROM w) SELECT * FROM w",
        )
        .unwrap();
        let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
        assert!(cte.cterecursive);
        assert_eq!(cte.ctecolnames.nth(0).as_string().unwrap().sval, "f");
        assert_eq!(cte.ctecoltypes.as_slice(), &[TEXTOID]);
        assert_eq!(cte.ctecoltypmods.as_slice(), &[-1]);
        assert_eq!(cte.ctecolcollations.as_slice(), &[100]);
    }

    #[test]
    fn with_recursive_selfref_in_nonrecursive_term_is_42p19() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE w(n) AS (SELECT n FROM w UNION ALL SELECT 1) SELECT * FROM w",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_RECURSION);
        assert_eq!(
            err.message(),
            "recursive reference to query \"w\" must not appear within its non-recursive term"
        );
    }

    #[test]
    fn with_recursive_selfref_in_subquery_is_42p19() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT (SELECT n FROM w) FROM t) \
             SELECT * FROM w",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_RECURSION);
        assert_eq!(
            err.message(),
            "recursive reference to query \"w\" must not appear within a subquery"
        );
    }

    #[test]
    fn with_recursive_selfref_in_outer_join_is_42p19() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT w.n FROM t LEFT JOIN w ON true) \
             SELECT * FROM w",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_RECURSION);
        assert_eq!(
            err.message(),
            "recursive reference to query \"w\" must not appear within an outer join"
        );
    }

    #[test]
    fn with_recursive_selfref_twice_is_42p19() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT w.n FROM w, w w2) \
             SELECT * FROM w",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_RECURSION);
        assert_eq!(
            err.message(),
            "recursive reference to query \"w\" must not appear more than once"
        );
    }

    #[test]
    fn with_recursive_without_union_is_42p19() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "WITH RECURSIVE w(n) AS (SELECT n FROM w) SELECT * FROM w")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_RECURSION);
        assert_eq!(
            err.message(),
            "recursive query \"w\" does not have the form non-recursive-term UNION [ALL] \
             recursive-term"
        );
    }

    #[test]
    fn with_recursive_order_by_is_0a000() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM w ORDER BY n) \
             SELECT * FROM w",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), "ORDER BY in a recursive query is not implemented");
    }

    #[test]
    fn with_recursive_limit_is_0a000() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE w(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM w LIMIT 1) \
             SELECT * FROM w",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), "LIMIT in a recursive query is not implemented");
    }

    #[test]
    fn with_recursive_mutual_recursion_is_0a000() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(
            mcx,
            "WITH RECURSIVE a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT * FROM a",
        )
        .map(|_| ())
        .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), "mutual recursion between WITH items is not implemented");
    }

    // The 42804 nrterm-vs-overall type mismatch needs live operator/cast
    // resolution (no seams in this harness); covered by
    // scripts/recursive-cte-e2e.sh against C.

    #[test]
    fn union_all_int_consts_query_shape() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT 1 UNION ALL SELECT 2").unwrap();

        assert_eq!(q.commandType, CmdType::CMD_SELECT);
        let so = q.setOperations.unwrap().as_set_operation_stmt().unwrap();
        assert_eq!(so.op, types_nodes::SetOperation::SETOP_UNION);
        assert!(so.all);
        assert_eq!(so.larg.unwrap().as_range_tbl_ref().unwrap().rtindex, 1);
        assert_eq!(so.rarg.unwrap().as_range_tbl_ref().unwrap().rtindex, 2);
        assert_eq!(so.colTypes.len(), 1);
        assert_eq!(so.colTypes.nth(0), INT4OID);
        assert_eq!(so.colTypmods.nth(0), -1);
        assert_eq!(so.colCollations.nth(0), types_core::InvalidOid);
        assert!(so.groupClauses.is_nil());

        assert_eq!(q.rtable.len(), 2);
        for (i, rte_node) in q.rtable.iter().enumerate() {
            let rte = rte_node.as_range_tbl_entry().unwrap();
            assert_eq!(rte.rtekind, RTEKind::RTE_SUBQUERY);
            let eref = rte.eref.unwrap();
            assert_eq!(eref.aliasname, Some(if i == 0 { "*SELECT* 1" } else { "*SELECT* 2" }));
            assert!(!rte.inFromCl);
            let sub = rte.subquery.unwrap();
            assert_eq!(sub.commandType, CmdType::CMD_SELECT);
            assert!(sub.canSetTag);
            let te = sub.targetList.nth(0).as_target_entry().unwrap();
            let c = te.expr.as_const().unwrap();
            assert_eq!(c.consttype, INT4OID);
            assert_eq!(c.constvalue, Datum::from_i32(i as i32 + 1));
        }

        assert_eq!(q.targetList.len(), 1);
        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(te.resno, 1);
        assert_eq!(te.resname, Some("?column?"));
        assert!(!te.resjunk);
        let v = te.expr.as_var().unwrap();
        assert_eq!((v.varno, v.varattno), (1, 1));
        assert_eq!((v.vartype, v.vartypmod, v.varcollid), (INT4OID, -1, types_core::InvalidOid));
        assert_eq!(v.varlevelsup, 0);
        assert_eq!((v.varnosyn, v.varattnosyn), (1, 1));
        assert_eq!(v.location, 7);

        let jt = q.jointree.unwrap();
        assert!(jt.fromlist.is_nil() && jt.quals.is_none());
        assert!(q.sortClause.is_nil() && q.limitCount.is_none() && q.limitOffset.is_none());
    }

    #[test]
    fn union_distinct_carries_group_clauses() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT 1 UNION SELECT 2").unwrap();

        let so = q.setOperations.unwrap().as_set_operation_stmt().unwrap();
        assert!(!so.all);
        assert_eq!(so.groupClauses.len(), 1);
        let g = so.groupClauses.nth(0).as_sort_group_clause().unwrap();
        assert_eq!(g.tleSortGroupRef, 0);
        assert_eq!((g.eqop, g.sortop), (96, 97));
        assert!(!g.reverse_sort && !g.nulls_first && g.hashable);
    }

    #[test]
    fn except_and_intersect_ops() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT 1 EXCEPT SELECT 2").unwrap();
        let so = q.setOperations.unwrap().as_set_operation_stmt().unwrap();
        assert_eq!(so.op, types_nodes::SetOperation::SETOP_EXCEPT);
        assert_eq!(so.groupClauses.len(), 1);

        let q = analyze_sql(mcx, "SELECT 1 INTERSECT ALL SELECT 2").unwrap();
        let so = q.setOperations.unwrap().as_set_operation_stmt().unwrap();
        assert_eq!(so.op, types_nodes::SetOperation::SETOP_INTERSECT);
        assert!(so.all);
        assert_eq!(so.groupClauses.len(), 1);
    }

    #[test]
    fn union_all_unknown_consts_resolve_to_text() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT 'a' UNION ALL SELECT 'b'").unwrap();

        let so = q.setOperations.unwrap().as_set_operation_stmt().unwrap();
        assert_eq!(so.colTypes.nth(0), TEXTOID);
        assert_eq!(so.colCollations.nth(0), 100);
        for rte_node in q.rtable.iter() {
            let sub = rte_node.as_range_tbl_entry().unwrap().subquery.unwrap();
            let te = sub.targetList.nth(0).as_target_entry().unwrap();
            assert_eq!(te.expr.as_const().unwrap().consttype, TEXTOID);
        }
        let v = q.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((v.vartype, v.varcollid), (TEXTOID, 100));
    }

    #[test]
    fn nested_union_keeps_tree_shape() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT 1 UNION ALL (SELECT 2 UNION ALL SELECT 3)").unwrap();

        let so = q.setOperations.unwrap().as_set_operation_stmt().unwrap();
        assert_eq!(so.larg.unwrap().as_range_tbl_ref().unwrap().rtindex, 1);
        let inner = so.rarg.unwrap().as_set_operation_stmt().unwrap();
        assert_eq!(inner.larg.unwrap().as_range_tbl_ref().unwrap().rtindex, 2);
        assert_eq!(inner.rarg.unwrap().as_range_tbl_ref().unwrap().rtindex, 3);
        assert_eq!(q.rtable.len(), 3);
        assert_eq!(q.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap().varno, 1);
    }

    #[test]
    fn union_from_table_column_names_and_order_by() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT x FROM t UNION ALL SELECT 2 ORDER BY x").unwrap();

        let te = q.targetList.nth(0).as_target_entry().unwrap();
        assert_eq!(te.resname, Some("x"));
        assert_eq!(te.ressortgroupref, 1);
        assert_eq!(q.sortClause.len(), 1);
        let s = q.sortClause.nth(0).as_sort_group_clause().unwrap();
        assert_eq!((s.tleSortGroupRef, s.eqop, s.sortop), (1, 96, 97));
        // The ORDER BY join RTE is truncated away; only the two leaves stay.
        assert_eq!(q.rtable.len(), 2);
        assert_eq!(q.targetList.len(), 1);
    }

    // Reachable since findTargetlistEntrySQL99's equal() leg went live
    // (grouping-sets lane): expression sort keys land as resjunk and hit C's
    // 0A000 arm.
    #[test]
    fn union_order_by_expression_is_0a000() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT 1 AS x UNION ALL SELECT 2 ORDER BY x + 1")
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), "invalid UNION/INTERSECT/EXCEPT ORDER BY clause");
    }

    #[test]
    fn union_column_count_mismatch_is_42601() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let err = analyze_sql(mcx, "SELECT 1 UNION ALL SELECT 1, 2").map(|_| ()).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
        assert!(
            err.message().contains("each UNION query must have the same number of columns"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn union_limit_offset_transformed_on_top_query() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let q = analyze_sql(mcx, "SELECT 1 UNION ALL SELECT 2 LIMIT 1").unwrap();

        assert!(q.limitCount.is_some());
        assert!(q.limitOffset.is_none());
        for rte_node in q.rtable.iter() {
            let sub = rte_node.as_range_tbl_entry().unwrap().subquery.unwrap();
            assert!(sub.limitCount.is_none());
        }
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

#[test]
fn select_current_timestamp_end_to_end() {
    use types_nodes::primnodes::{SQLValueFunction, SQLValueFunctionOp};

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let svf = Node::mk(
        mcx,
        SQLValueFunction {
            op: SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP,
            r#type: 0,
            typmod: -1,
            location: 7,
        },
    )
    .unwrap();
    let target = Node::mk_res_target(mcx, None, NodeList::nil(), Some(svf), 7).unwrap();
    let raw_stmt = raw(select_stmt(mcx, &[target]), 24);

    let q = analyze(mcx, "SELECT CURRENT_TIMESTAMP", &raw_stmt);

    assert_eq!(q.targetList.len(), 1);
    let te = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!(te.resname, Some("current_timestamp"));
    let out = te.expr.as_sql_value_function().unwrap();
    assert_eq!(out.op, SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP);
    assert_eq!(out.r#type, types_core::catalog::TIMESTAMPTZOID);
    assert_eq!(out.typmod, -1);
}
