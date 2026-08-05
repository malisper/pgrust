use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_core::catalog::{INT4OID, INT8OID};
use types_core::InvalidOid;
use types_nodes::nodes_enums::LimitOption;
use types_nodes::rawnodes::{ColumnRef, SortBy, SortByDir, SortByNulls, ValUnion};
use types_nodes::{Integer, Node, NodeList, String as PgStr};

use crate::{
    transformDistinctClause, transformFromClause, transformGroupClause, transformLimitClause,
    transformSortClause, transformWhereClause, transformWindowDefinitions,
};

const INT4_LT: types_core::Oid = 97;
const INT4_EQ: types_core::Oid = 96;
const INT4_GT: types_core::Oid = 521;
const INT4_BTREE_OPCLASS: types_core::Oid = 1978;
const INT4_HASH_OPCLASS: types_core::Oid = 1979;
const INT_BTREE_FAM: types_core::Oid = 1976;
const INT_HASH_FAM: types_core::Oid = 1977;
const F_INT48: types_core::Oid = 481;

fn install_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(Some(types_tuple::PgTypeShape {
                typlen: if typid == INT8OID { 8 } else { 4 },
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: InvalidOid,
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
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            let name = match typid {
                INT4OID => "int4",
                INT8OID => "int8",
                _ => return Ok(None),
            };
            let mut typname = types_tuple::NameData::default();
            typname.namestrcpy(name);
            Ok(Some(syscache_seams::PgTypeTypcacheShape {
                typname,
                typlen: if typid == INT8OID { 8 } else { 4 },
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
        syscache_seams::syscache_hash_value_typeoid::set(|typid| Ok(typid.wrapping_mul(31)));
        syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
            Ok(match opclass {
                INT4_BTREE_OPCLASS => Some(syscache_seams::PgOpclassShape {
                    opcmethod: types_core::BTREE_AM_OID,
                    opcfamily: INT_BTREE_FAM,
                    opcintype: INT4OID,
                    // int4 opclasses store no separate key type (pg_opclass: 0).
                    opckeytype: ::types_core::InvalidOid,
                }),
                INT4_HASH_OPCLASS => Some(syscache_seams::PgOpclassShape {
                    opcmethod: lsyscache::HASH_AM_OID,
                    opcfamily: INT_HASH_FAM,
                    opcintype: INT4OID,
                    // int4 opclasses store no separate key type (pg_opclass: 0).
                    opckeytype: ::types_core::InvalidOid,
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
        indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
            Ok(match (type_id, am_id) {
                (INT4OID, types_core::BTREE_AM_OID) => INT4_BTREE_OPCLASS,
                (INT4OID, _) => INT4_HASH_OPCLASS,
                _ => InvalidOid,
            })
        });
        syscache_seams::lookup_pg_cast_shape::set(|src, tgt| {
            Ok((src == INT4OID && tgt == INT8OID).then_some(syscache_seams::PgCastShape {
                oid: 10001,
                castfunc: F_INT48,
                castcontext: b'i' as i8,
                castmethod: b'f' as i8,
            }))
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(match funcid {
                F_INT48 => Some(syscache_seams::PgProcShape {
                    prolang: 12,
                    prosecdef: false,
                    proconfig_isnull: true,
                    pronamespace: 11,
                    prorettype: INT8OID,
                    provariadic: InvalidOid,
                    prosupport: InvalidOid,
                    pronargs: 1,
                    prokind: b'f' as i8,
                    provolatile: b'i' as i8,
                    proparallel: b's' as i8,
                    proretset: false,
                    proisstrict: true,
                    proleakproof: true,
                }),
                1066 | 1067 => Some(syscache_seams::PgProcShape {
                    prolang: 12,
                    prosecdef: false,
                    proconfig_isnull: true,
                    pronamespace: 11,
                    prorettype: INT4OID,
                    provariadic: InvalidOid,
                    prosupport: 3994,
                    pronargs: if funcid == 1066 { 3 } else { 2 },
                    prokind: b'f' as i8,
                    provolatile: b'i' as i8,
                    proparallel: b's' as i8,
                    proretset: true,
                    proisstrict: true,
                    proleakproof: false,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_proc_name_candidates::set(|mcx, proname| {
            let mut v = mcx::PgVec::new_in(mcx);
            if proname == "generate_series" {
                let mut two = mcx::vec_with_capacity_in(mcx, 2)?;
                two.extend([INT4OID, INT4OID]);
                v.push(syscache_seams::PgProcCandidate {
                    oid: 1067,
                    pronamespace: 11,
                    pronargs: 2,
                    pronargdefaults: 0,
                    provariadic: InvalidOid,
                    proargtypes: two,
                });
            }
            Ok(v)
        });
        miscinit_seams::get_user_id::set(|| 10);
        syscache_seams::pg_type_typtype::set(|_| Ok(Some(b'b' as i8)));
        syscache_seams::pg_proc_result_arrays::set(|_, _| {
            Ok(Some(syscache_seams::PgProcResultArraysShape {
                proallargtypes: None,
                proargmodes: None,
                proargnames: None,
            }))
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok((typid == INT8OID).then_some(syscache_seams::PgTypeIoShape {
                oid: INT8OID,
                typinput: 460,
                typoutput: 461,
                typreceive: 2408,
                typsend: 2409,
                typmodin: InvalidOid,
                typmodout: InvalidOid,
                typelem: InvalidOid,
                typlen: 8,
                typbyval: true,
                typalign: b'd' as i8,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
    });
}

fn int_a_const<'mcx>(mcx: Mcx<'mcx>, ival: i32, location: i32) -> Node<'mcx> {
    Node::mk_a_const(mcx, Some(ValUnion::Integer(Integer { ival })), location).unwrap()
}

fn int4_tle<'mcx>(mcx: Mcx<'mcx>, v: i32, resno: i16, resname: Option<&'mcx str>) -> Node<'mcx> {
    let c = Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, datum::Datum::from_i32(v), false, true)
        .unwrap();
    Node::mk_target_entry(mcx, c, resno, resname, false).unwrap()
}

fn sort_by<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    dir: SortByDir,
    nulls: SortByNulls,
) -> Node<'mcx> {
    Node::mk(
        mcx,
        SortBy {
            node: Some(node),
            sortby_dir: dir,
            sortby_nulls: nulls,
            useOp: NodeList::nil(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn trivial_arms_are_noops() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    transformFromClause(mcx, &mut pstate, &NodeList::nil()).unwrap();
    assert!(pstate.p_joinlist.is_nil());
    assert!(pstate.p_rtable.is_nil());

    let qual = transformWhereClause(
        mcx,
        &mut pstate,
        None,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )
    .unwrap();
    assert!(qual.is_none());

    let limit = transformLimitClause(
        mcx,
        &mut pstate,
        None,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        LimitOption::default(),
    )
    .unwrap();
    assert!(limit.is_none());

    let mut tlist = NodeList::nil();
    let sort = transformSortClause(
        mcx,
        &mut pstate,
        &NodeList::nil(),
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap();
    assert!(sort.is_nil());

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &NodeList::nil(),
        &mut gsets,
        &mut tlist,
        &sort,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();
    assert!(group.is_nil() && gsets.is_nil());

    let windows =
        transformWindowDefinitions(ctx.mcx(), &mut pstate, &NodeList::nil(), &mut tlist).unwrap();
    assert!(windows.is_nil());
}

#[test]
#[should_panic(expected = "transformFromClauseItem")]
fn non_relation_from_item_panics_loudly() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let star = Node::mk_a_star(mcx).unwrap();
    let from = NodeList::make1(mcx, star).unwrap();
    let _ = transformFromClause(mcx, &mut pstate, &from);
}

#[test]
fn where_clause_boolean_passthrough() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let bconst = Node::mk_a_const(
        mcx,
        Some(ValUnion::Boolean(types_nodes::Boolean { boolval: true })),
        7,
    )
    .unwrap();
    let qual = transformWhereClause(
        mcx,
        &mut pstate,
        Some(bconst),
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )
    .unwrap()
    .unwrap();
    let c = qual.as_const().unwrap();
    assert_eq!(c.consttype, types_core::catalog::BOOLOID);
    assert_eq!(c.constvalue, datum::Datum::from_bool(true));
}

#[test]
fn order_by_position_resolves_default_and_desc() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist =
        NodeList::make2(mcx, int4_tle(mcx, 1, 1, None), int4_tle(mcx, 2, 2, None)).unwrap();

    let orderby = NodeList::make2(
        mcx,
        sort_by(
            mcx,
            int_a_const(mcx, 1, 20),
            SortByDir::SORTBY_DEFAULT,
            SortByNulls::SORTBY_NULLS_DEFAULT,
        ),
        sort_by(
            mcx,
            int_a_const(mcx, 2, 23),
            SortByDir::SORTBY_DESC,
            SortByNulls::SORTBY_NULLS_DEFAULT,
        ),
    )
    .unwrap();

    let sortlist = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap();

    assert_eq!(sortlist.len(), 2);
    let s1 = sortlist.nth(0).as_sort_group_clause().unwrap();
    assert_eq!(
        (s1.tleSortGroupRef, s1.sortop, s1.eqop, s1.reverse_sort, s1.nulls_first, s1.hashable),
        (1, INT4_LT, INT4_EQ, false, false, true)
    );
    let s2 = sortlist.nth(1).as_sort_group_clause().unwrap();
    assert_eq!(
        (s2.tleSortGroupRef, s2.sortop, s2.eqop, s2.reverse_sort, s2.nulls_first, s2.hashable),
        (2, INT4_GT, INT4_EQ, true, true, true)
    );
    assert_eq!(tlist.nth(0).as_target_entry().unwrap().ressortgroupref, 1);
    assert_eq!(tlist.nth(1).as_target_entry().unwrap().ressortgroupref, 2);
}

#[test]
fn order_by_name_nulls_first_and_dedup() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist = NodeList::make1(mcx, int4_tle(mcx, 1, 1, Some("foo"))).unwrap();

    let name_ref = |loc| {
        let f = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "foo" }).unwrap()).unwrap();
        Node::mk(mcx, ColumnRef { fields: f, location: loc }).unwrap()
    };
    let orderby = NodeList::make2(
        mcx,
        sort_by(mcx, name_ref(20), SortByDir::SORTBY_ASC, SortByNulls::SORTBY_NULLS_FIRST),
        sort_by(mcx, name_ref(30), SortByDir::SORTBY_ASC, SortByNulls::SORTBY_NULLS_FIRST),
    )
    .unwrap();

    let sortlist = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap();

    assert_eq!(sortlist.len(), 1, "duplicate ORDER BY item must be suppressed");
    let s = sortlist.nth(0).as_sort_group_clause().unwrap();
    assert_eq!((s.tleSortGroupRef, s.sortop, s.nulls_first), (1, INT4_LT, true));
}

#[test]
fn order_by_bad_position_is_42p10() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist = NodeList::make1(mcx, int4_tle(mcx, 1, 1, None)).unwrap();
    let orderby = NodeList::make1(
        mcx,
        sort_by(
            mcx,
            int_a_const(mcx, 2, 20),
            SortByDir::SORTBY_DEFAULT,
            SortByNulls::SORTBY_NULLS_DEFAULT,
        ),
    )
    .unwrap();

    let err = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_COLUMN_REFERENCE);
    assert_eq!(err.message(), "ORDER BY position 2 is not in select list");
}

#[test]
fn order_by_non_integer_constant_is_42601() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist = NodeList::make1(mcx, int4_tle(mcx, 1, 1, None)).unwrap();
    let sconst = Node::mk_a_const(mcx, Some(ValUnion::String(PgStr { sval: "x" })), 20).unwrap();
    let orderby = NodeList::make1(
        mcx,
        sort_by(mcx, sconst, SortByDir::SORTBY_DEFAULT, SortByNulls::SORTBY_NULLS_DEFAULT),
    )
    .unwrap();

    let err = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message(), "non-integer constant in ORDER BY");
}

#[test]
fn limit_count_coerces_to_int8_funcexpr() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let out = transformLimitClause(
        mcx,
        &mut pstate,
        Some(int_a_const(mcx, 1, 15)),
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        LimitOption::LIMIT_OPTION_COUNT,
    )
    .unwrap()
    .unwrap();

    let f = out.as_func_expr().unwrap();
    assert_eq!(f.funcid, F_INT48);
    assert_eq!(f.funcresulttype, INT8OID);
    assert!(!f.funcretset && !f.funcvariadic);
    assert_eq!(f.funcformat, types_nodes::CoercionForm::COERCE_IMPLICIT_CAST);
    assert_eq!((f.funccollid, f.inputcollid), (InvalidOid, InvalidOid));
    assert_eq!(f.location, -1);
    assert_eq!(f.args.len(), 1);
    let arg = f.args.nth(0).as_const().unwrap();
    assert_eq!((arg.consttype, arg.constvalue), (INT4OID, datum::Datum::from_i32(1)));
    assert_eq!(arg.location, 15);
}

#[test]
fn limit_all_null_becomes_int8_null_const() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let out = transformLimitClause(
        mcx,
        &mut pstate,
        Some(Node::mk_a_const(mcx, None, -1).unwrap()),
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        LimitOption::LIMIT_OPTION_COUNT,
    )
    .unwrap()
    .unwrap();

    let c = out.as_const().unwrap();
    assert_eq!(c.consttype, INT8OID);
    assert!(c.constisnull);
    assert_eq!((c.constlen, c.constbyval), (8, true));
}

#[test]
fn limit_null_with_ties_is_2201w() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let err = transformLimitClause(
        mcx,
        &mut pstate,
        Some(Node::mk_a_const(mcx, None, -1).unwrap()),
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        LimitOption::LIMIT_OPTION_WITH_TIES,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE);
    assert_eq!(err.message(), "row count cannot be null in FETCH FIRST ... WITH TIES clause");
}

#[test]
fn limit_with_variable_is_42p10() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let var = Node::mk_var(mcx, 1, 1, INT8OID, -1, InvalidOid, 0).unwrap();

    let err = transformLimitClause(
        mcx,
        &mut pstate,
        Some(var),
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        LimitOption::LIMIT_OPTION_COUNT,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_COLUMN_REFERENCE);
    assert_eq!(err.message(), "argument of LIMIT must not contain variables");
}

#[test]
fn group_by_name_and_position_with_dedup() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist = NodeList::make2(
        mcx,
        int4_tle(mcx, 1, 1, Some("foo")),
        int4_tle(mcx, 2, 2, Some("bar")),
    )
    .unwrap();

    let name_ref = |name: &'static str, loc| {
        let f = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: name }).unwrap()).unwrap();
        Node::mk(mcx, ColumnRef { fields: f, location: loc }).unwrap()
    };
    let mut grouplist = NodeList::make2(mcx, name_ref("foo", 20), int_a_const(mcx, 2, 28)).unwrap();
    grouplist.lappend(mcx, name_ref("foo", 35)).unwrap();

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &grouplist,
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();

    assert!(gsets.is_nil());
    assert_eq!(group.len(), 2, "duplicate GROUP BY item must be suppressed");
    let g1 = group.nth(0).as_sort_group_clause().unwrap();
    assert_eq!(
        (g1.tleSortGroupRef, g1.eqop, g1.sortop, g1.reverse_sort, g1.nulls_first, g1.hashable),
        (1, INT4_EQ, INT4_LT, false, false, true)
    );
    let g2 = group.nth(1).as_sort_group_clause().unwrap();
    assert_eq!((g2.tleSortGroupRef, g2.eqop, g2.sortop), (2, INT4_EQ, INT4_LT));
    assert_eq!(tlist.nth(0).as_target_entry().unwrap().ressortgroupref, 1);
    assert_eq!(tlist.nth(1).as_target_entry().unwrap().ressortgroupref, 2);
}

#[test]
fn group_by_copies_matching_order_by_operators() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist = NodeList::make1(mcx, int4_tle(mcx, 1, 1, Some("foo"))).unwrap();

    let orderby = NodeList::make1(
        mcx,
        sort_by(
            mcx,
            int_a_const(mcx, 1, 20),
            SortByDir::SORTBY_DESC,
            SortByNulls::SORTBY_NULLS_DEFAULT,
        ),
    )
    .unwrap();
    let sortlist = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap();

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &NodeList::make1(mcx, int_a_const(mcx, 1, 40)).unwrap(),
        &mut gsets,
        &mut tlist,
        &sortlist,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();

    // The GROUP BY item takes the (copied) DESC ORDER BY semantics.
    assert_eq!(group.len(), 1);
    let g = group.nth(0).as_sort_group_clause().unwrap();
    let s = sortlist.nth(0).as_sort_group_clause().unwrap();
    assert!(!group.nth(0).ptr_eq(sortlist.nth(0)), "C copyObject, not a shared node");
    assert_eq!(
        (g.tleSortGroupRef, g.eqop, g.sortop, g.reverse_sort, g.nulls_first),
        (s.tleSortGroupRef, s.eqop, s.sortop, s.reverse_sort, s.nulls_first)
    );
}

#[test]
fn group_by_aggregate_rejected_42803() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_hasAggs.set(true);
    let aggref = Node::mk(
        mcx,
        types_nodes::primnodes::Aggref {
            aggfnoid: 2803,
            aggtype: 20,
            aggstar: true,
            location: 7,
            ..types_nodes::primnodes::Aggref::default()
        },
    )
    .unwrap();
    let tle = Node::mk_target_entry(mcx, aggref, 1, Some("count"), false).unwrap();
    let mut tlist = NodeList::make1(mcx, tle).unwrap();

    let mut gsets = NodeList::nil();
    let err = transformGroupClause(
        mcx,
        &mut pstate,
        &NodeList::make1(mcx, int_a_const(mcx, 1, 40)).unwrap(),
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_GROUPING_ERROR);
    assert_eq!(err.message(), "aggregate functions are not allowed in GROUP BY");
}

#[test]
fn order_by_duplicate_name_same_value_resolves() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    // C: duplicate output names naming equal() values are not ambiguous.
    let mut tlist =
        NodeList::make2(mcx, int4_tle(mcx, 7, 1, Some("foo")), int4_tle(mcx, 7, 2, Some("foo")))
            .unwrap();

    let f = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "foo" }).unwrap()).unwrap();
    let cref = Node::mk(mcx, ColumnRef { fields: f, location: 20 }).unwrap();
    let orderby = NodeList::make1(
        mcx,
        sort_by(mcx, cref, SortByDir::SORTBY_ASC, SortByNulls::SORTBY_NULLS_DEFAULT),
    )
    .unwrap();

    let sortlist = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap();
    assert_eq!(sortlist.len(), 1);
    assert_eq!(sortlist.nth(0).as_sort_group_clause().unwrap().tleSortGroupRef, 1);
    // The first matching entry wins the sortgroupref.
    assert_eq!(tlist.nth(0).as_target_entry().unwrap().ressortgroupref, 1);
    assert_eq!(tlist.nth(1).as_target_entry().unwrap().ressortgroupref, 0);
}

#[test]
fn order_by_duplicate_name_distinct_values_is_42702() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist =
        NodeList::make2(mcx, int4_tle(mcx, 7, 1, Some("foo")), int4_tle(mcx, 8, 2, Some("foo")))
            .unwrap();

    let f = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "foo" }).unwrap()).unwrap();
    let cref = Node::mk(mcx, ColumnRef { fields: f, location: 20 }).unwrap();
    let orderby = NodeList::make1(
        mcx,
        sort_by(mcx, cref, SortByDir::SORTBY_ASC, SortByNulls::SORTBY_NULLS_DEFAULT),
    )
    .unwrap();

    let err = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_AMBIGUOUS_COLUMN);
    assert_eq!(err.message(), "ORDER BY \"foo\" is ambiguous");
}

// DISTINCT = all ORDER BY items (copied) then remaining non-junk tlist items.
#[test]
fn distinct_absorbs_order_by_then_remaining_columns() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist = NodeList::make2(
        mcx,
        int4_tle(mcx, 1, 1, Some("foo")),
        int4_tle(mcx, 2, 2, Some("bar")),
    )
    .unwrap();

    let orderby = NodeList::make1(
        mcx,
        sort_by(
            mcx,
            int_a_const(mcx, 2, 20),
            SortByDir::SORTBY_DESC,
            SortByNulls::SORTBY_NULLS_DEFAULT,
        ),
    )
    .unwrap();
    let sortlist = transformSortClause(
        mcx,
        &mut pstate,
        &orderby,
        &mut tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )
    .unwrap();

    let distinct =
        transformDistinctClause(mcx, &mut pstate, &mut tlist, &sortlist, false).unwrap();
    assert_eq!(distinct.len(), 2);
    // First: the ORDER BY item's copied (DESC) semantics for "bar".
    let d1 = distinct.nth(0).as_sort_group_clause().unwrap();
    let s = sortlist.nth(0).as_sort_group_clause().unwrap();
    assert!(!distinct.nth(0).ptr_eq(sortlist.nth(0)), "C copyObject, not a shared node");
    assert_eq!(
        (d1.tleSortGroupRef, d1.eqop, d1.sortop, d1.reverse_sort),
        (s.tleSortGroupRef, s.eqop, s.sortop, s.reverse_sort)
    );
    // Second: "foo" under default grouping semantics.
    let d2 = distinct.nth(1).as_sort_group_clause().unwrap();
    assert_eq!((d2.eqop, d2.sortop, d2.reverse_sort, d2.hashable), (INT4_EQ, INT4_LT, false, true));
    assert_eq!(tlist.nth(0).as_target_entry().unwrap().ressortgroupref, d2.tleSortGroupRef);
}

// A resjunk ORDER BY expression under SELECT DISTINCT is 42P10.
#[test]
fn distinct_with_junk_order_by_is_42p10() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let mut tlist = NodeList::make1(mcx, int4_tle(mcx, 1, 1, Some("foo"))).unwrap();
    // A junk sort entry, as findTargetlistEntrySQL99 would add.
    let junk_expr = parse_expr::transformExpr(
        mcx,
        &mut pstate,
        int_a_const(mcx, 9, 30),
        ParseExprKind::EXPR_KIND_ORDER_BY,
    )
    .unwrap();
    let junk = Node::mk_target_entry(mcx, junk_expr, 2, None, true).unwrap();
    // SAFETY: freshly built tlist; no other reference is live.
    unsafe {
        junk.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.ressortgroupref = 7)
    }
    .unwrap();
    tlist.lappend(mcx, junk).unwrap();
    let sortlist = NodeList::make1(
        mcx,
        Node::mk(
            mcx,
            types_nodes::parsenodes::SortGroupClause {
                tleSortGroupRef: 7,
                eqop: INT4_EQ,
                sortop: INT4_LT,
                reverse_sort: false,
                nulls_first: false,
                hashable: true,
            },
        )
        .unwrap(),
    )
    .unwrap();

    let err = transformDistinctClause(mcx, &mut pstate, &mut tlist, &sortlist, false)
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_COLUMN_REFERENCE);
    assert_eq!(
        err.message(),
        "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
}

fn generate_series_from_item<'mcx>(
    mcx: Mcx<'mcx>,
    alias: Option<&'mcx str>,
) -> Node<'mcx> {
    use types_nodes::rawnodes::FuncCall;
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "generate_series" }).unwrap())
        .unwrap();
    let mut args = NodeList::nil();
    for v in [1, 10] {
        args.lappend(mcx, int_a_const(mcx, v, -1)).unwrap();
    }
    let fc = Node::mk(
        mcx,
        FuncCall { funcname: name, args, location: 14, ..Default::default() },
    )
    .unwrap();
    let alias = alias.map(|a| {
        Node::mk_mut(mcx, types_nodes::Alias { aliasname: Some(a), colnames: NodeList::nil() })
            .unwrap()
            .seal_ref() as &types_nodes::Alias<'_>
    });
    let mut pair = NodeList::make1(mcx, fc).unwrap();
    pair.lappend(mcx, Node::mk_list(mcx, NodeList::nil()).unwrap()).unwrap();
    Node::mk(
        mcx,
        types_nodes::RangeFunction {
            functions: NodeList::make1(mcx, Node::mk_list(mcx, pair).unwrap()).unwrap(),
            alias,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn from_generate_series_builds_function_rte() {
    use types_nodes::parsenodes::RTEKind;
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let from = NodeList::make1(mcx, generate_series_from_item(mcx, None)).unwrap();
    transformFromClause(mcx, &mut pstate, &from).unwrap();

    assert_eq!(pstate.p_rtable.len(), 1);
    let rte = pstate.p_rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_FUNCTION);
    assert!(!rte.funcordinality && !rte.lateral && rte.inFromCl);
    assert_eq!(rte.functions.len(), 1);
    let rtfunc = rte.functions.nth(0).as_range_tbl_function().unwrap();
    assert_eq!(rtfunc.funccolcount, 1);
    let fe = rtfunc.funcexpr.unwrap().as_func_expr().unwrap();
    assert_eq!(fe.funcid, 1067);
    assert!(fe.funcretset);
    assert_eq!(fe.funcresulttype, INT4OID);
    assert_eq!(fe.args.len(), 2);
    let eref = rte.eref.unwrap();
    assert_eq!(eref.aliasname, Some("generate_series"));
    assert_eq!(eref.colnames.len(), 1);
    assert_eq!(eref.colnames.nth(0).as_string().unwrap().sval, "generate_series");
    // The nsitem drives * expansion off its nscolumns.
    let ns = pstate.p_namespace.last().unwrap();
    assert_eq!(ns.p_nscolumns.len(), 1);
    assert_eq!(ns.p_nscolumns[0].p_vartype, INT4OID);
    assert_eq!(ns.p_nscolumns[0].p_varattno, 1);
}

#[test]
fn from_generate_series_alias_names_the_column() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let from = NodeList::make1(mcx, generate_series_from_item(mcx, Some("g"))).unwrap();
    transformFromClause(mcx, &mut pstate, &from).unwrap();

    let rte = pstate.p_rtable.nth(0).as_range_tbl_entry().unwrap();
    let eref = rte.eref.unwrap();
    assert_eq!(eref.aliasname, Some("g"));
    assert_eq!(eref.colnames.nth(0).as_string().unwrap().sval, "g");
}

use types_nodes::parsenodes::GroupingSetKind;

fn name_ref<'mcx>(mcx: Mcx<'mcx>, name: &'static str, loc: i32) -> Node<'mcx> {
    let f = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: name }).unwrap()).unwrap();
    Node::mk(mcx, ColumnRef { fields: f, location: loc }).unwrap()
}

fn raw_gset<'mcx>(
    mcx: Mcx<'mcx>,
    kind: GroupingSetKind,
    content: &[Node<'mcx>],
    loc: i32,
) -> Node<'mcx> {
    let mut list = NodeList::nil();
    for &n in content {
        list.lappend(mcx, n).unwrap();
    }
    Node::mk_grouping_set(mcx, kind, list, loc).unwrap()
}

fn simple_refs(n: Node<'_>) -> Vec<i32> {
    let gs = n.as_grouping_set().unwrap();
    assert_eq!(gs.kind, GroupingSetKind::GROUPING_SET_SIMPLE);
    gs.content.iter().map(|c| c.as_integer().unwrap().ival).collect()
}

fn group_clause_fixture<'mcx>(
    mcx: Mcx<'mcx>,
) -> (parser_small1::ParseState<'mcx, 'mcx>, NodeList<'mcx>) {
    install_fixture();
    let pstate = make_parsestate(mcx, None);
    let tlist = NodeList::make2(
        mcx,
        int4_tle(mcx, 1, 1, Some("foo")),
        int4_tle(mcx, 2, 2, Some("bar")),
    )
    .unwrap();
    (pstate, tlist)
}

#[test]
fn group_by_rollup_builds_tree_and_flat_clause() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut pstate, mut tlist) = group_clause_fixture(mcx);

    let rollup = raw_gset(
        mcx,
        GroupingSetKind::GROUPING_SET_ROLLUP,
        &[name_ref(mcx, "foo", 20), name_ref(mcx, "bar", 25)],
        13,
    );
    let grouplist = NodeList::make1(mcx, rollup).unwrap();

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &grouplist,
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();

    assert_eq!(group.len(), 2);
    assert_eq!(gsets.len(), 1);
    let gs = gsets.nth(0).as_grouping_set().unwrap();
    assert_eq!((gs.kind, gs.location), (GroupingSetKind::GROUPING_SET_ROLLUP, 13));
    assert_eq!(gs.content.len(), 2);
    assert_eq!(simple_refs(gs.content.nth(0)), [1]);
    assert_eq!(simple_refs(gs.content.nth(1)), [2]);
}

#[test]
fn group_by_expr_beside_cube_wraps_simple_sets() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut pstate, mut tlist) = group_clause_fixture(mcx);

    let cube = raw_gset(
        mcx,
        GroupingSetKind::GROUPING_SET_CUBE,
        &[name_ref(mcx, "bar", 30)],
        24,
    );
    let grouplist = NodeList::make2(mcx, name_ref(mcx, "foo", 20), cube).unwrap();

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &grouplist,
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();

    assert_eq!(group.len(), 2);
    assert_eq!(gsets.len(), 2);
    // The plain expression is wrapped as a SIMPLE set when sets are present.
    assert_eq!(simple_refs(gsets.nth(0)), [1]);
    let cube = gsets.nth(1).as_grouping_set().unwrap();
    assert_eq!(cube.kind, GroupingSetKind::GROUPING_SET_CUBE);
    assert_eq!(simple_refs(cube.content.nth(0)), [2]);
}

#[test]
fn group_by_empty_grouping_set_restores_canonical_form() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut pstate, mut tlist) = group_clause_fixture(mcx);

    let empty = raw_gset(mcx, GroupingSetKind::GROUPING_SET_EMPTY, &[], 9);
    let grouplist = NodeList::make2(
        mcx,
        empty,
        raw_gset(mcx, GroupingSetKind::GROUPING_SET_EMPTY, &[], 13),
    )
    .unwrap();

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &grouplist,
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();

    assert!(group.is_nil());
    assert_eq!(gsets.len(), 1, "empty sets collapse to one canonical GROUP BY ()");
    let gs = gsets.nth(0).as_grouping_set().unwrap();
    assert_eq!((gs.kind, gs.location), (GroupingSetKind::GROUPING_SET_EMPTY, 9));
    assert!(gs.content.is_nil());
}

#[test]
fn nested_grouping_sets_flatten() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut pstate, mut tlist) = group_clause_fixture(mcx);

    let inner = raw_gset(
        mcx,
        GroupingSetKind::GROUPING_SET_SETS,
        &[name_ref(mcx, "bar", 40)],
        35,
    );
    let outer = raw_gset(
        mcx,
        GroupingSetKind::GROUPING_SET_SETS,
        &[name_ref(mcx, "foo", 25), inner],
        13,
    );
    let grouplist = NodeList::make1(mcx, outer).unwrap();

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &grouplist,
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();

    assert_eq!(group.len(), 2);
    assert_eq!(gsets.len(), 1);
    let gs = gsets.nth(0).as_grouping_set().unwrap();
    assert_eq!(gs.kind, GroupingSetKind::GROUPING_SET_SETS);
    assert_eq!(gs.content.len(), 2, "SETS-in-SETS flattens into the outer list");
    assert_eq!(simple_refs(gs.content.nth(0)), [1]);
    assert_eq!(simple_refs(gs.content.nth(1)), [2]);
}

#[test]
fn cube_with_13_elements_is_54011() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut pstate, mut tlist) = group_clause_fixture(mcx);

    let elems: Vec<_> = (0..13).map(|i| name_ref(mcx, "foo", 20 + i)).collect();
    let cube = raw_gset(mcx, GroupingSetKind::GROUPING_SET_CUBE, &elems, 13);
    let grouplist = NodeList::make1(mcx, cube).unwrap();

    let mut gsets = NodeList::nil();
    let err = transformGroupClause(
        mcx,
        &mut pstate,
        &grouplist,
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_TOO_MANY_COLUMNS);
    assert_eq!(err.message(), "CUBE is limited to 12 elements");
}

#[test]
fn grouping_set_sublist_dedups_locally() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut pstate, mut tlist) = group_clause_fixture(mcx);

    // A parenthesized sublist reaches transformGroupingSet as a T_List cell
    // (the implicit-RowExpr flattening product).
    let sublist = Node::mk_list(
        mcx,
        NodeList::make2(mcx, name_ref(mcx, "foo", 30), name_ref(mcx, "foo", 35)).unwrap(),
    )
    .unwrap();
    let sets = raw_gset(mcx, GroupingSetKind::GROUPING_SET_SETS, &[sublist], 13);
    let grouplist = NodeList::make1(mcx, sets).unwrap();

    let mut gsets = NodeList::nil();
    let group = transformGroupClause(
        mcx,
        &mut pstate,
        &grouplist,
        &mut gsets,
        &mut tlist,
        &NodeList::nil(),
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )
    .unwrap();

    assert_eq!(group.len(), 1);
    let gs = gsets.nth(0).as_grouping_set().unwrap();
    assert_eq!(simple_refs(gs.content.nth(0)), [1], "sublist duplicates drop out");
}
