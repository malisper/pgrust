use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_core::catalog::{INT4OID, INT8OID};
use types_core::InvalidOid;
use types_nodes::nodes_enums::LimitOption;
use types_nodes::rawnodes::{ColumnRef, SortBy, SortByDir, SortByNulls, ValUnion};
use types_nodes::{Integer, Node, NodeList, String as PgStr};

use crate::{
    transformFromClause, transformGroupClause, transformLimitClause, transformSortClause,
    transformWhereClause, transformWindowDefinitions,
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
                }),
                INT4_HASH_OPCLASS => Some(syscache_seams::PgOpclassShape {
                    opcmethod: lsyscache::HASH_AM_OID,
                    opcfamily: INT_HASH_FAM,
                    opcintype: INT4OID,
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
            Ok((funcid == F_INT48).then_some(syscache_seams::PgProcShape {
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
        transformWindowDefinitions(&mut pstate, &NodeList::nil(), &mut tlist).unwrap();
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
    pstate.p_hasAggs = true;
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
