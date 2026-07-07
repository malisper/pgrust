use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_nodes::rawnodes::ValUnion;
use types_nodes::{Integer, Node, NodeList, String as PgStr};

use crate::{
    markTargetListOrigins, resolveTargetListUnknowns, transformTargetList, FigureColname,
};

fn int_const<'mcx>(mcx: Mcx<'mcx>, ival: i32, location: i32) -> Node<'mcx> {
    Node::mk_a_const(mcx, Some(ValUnion::Integer(Integer { ival })), location).unwrap()
}

fn res_target<'mcx>(
    mcx: Mcx<'mcx>,
    name: Option<&'mcx str>,
    val: Node<'mcx>,
) -> Node<'mcx> {
    Node::mk_res_target(mcx, name, NodeList::nil(), Some(val), 7).unwrap()
}

#[test]
fn target_list_resnos_names_and_origins() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let raw = NodeList::make2(
        mcx,
        res_target(mcx, Some("a"), int_const(mcx, 1, 7)),
        res_target(mcx, None, int_const(mcx, 2, 12)),
    )
    .unwrap();

    let tlist =
        transformTargetList(mcx, &mut pstate, &raw, ParseExprKind::EXPR_KIND_SELECT_TARGET)
            .unwrap();

    assert_eq!(tlist.len(), 2);
    let te1 = tlist.nth(0).as_target_entry().unwrap();
    let te2 = tlist.nth(1).as_target_entry().unwrap();
    assert_eq!((te1.resno, te1.resname), (1, Some("a")));
    assert_eq!((te2.resno, te2.resname), (2, Some("?column?")));
    assert_eq!(pstate.p_next_resno, 3);

    markTargetListOrigins(&pstate, &tlist).unwrap();
    resolveTargetListUnknowns(mcx, &pstate, &tlist).unwrap();
}

fn install_fixture() {
    use std::sync::Once;
    use types_core::catalog::TEXTOID;
    use types_core::InvalidOid;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::pg_type_base_shape::set(|typid| {
            Ok(Some(syscache_seams::PgTypeBaseShape {
                typtype: if typid == types_core::catalog::UNKNOWNOID { b'p' as i8 } else { b'b' as i8 },
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
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(Some(types_tuple::PgTypeShape {
                typlen: if typid == TEXTOID { -1 } else { 4 },
                typbyval: typid != TEXTOID,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: if typid == TEXTOID { 100 } else { InvalidOid },
            }))
        });
    });
}

#[test]
fn unknown_target_resolves_to_text() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let sconst = Node::mk_a_const(mcx, Some(ValUnion::String(PgStr { sval: "x" })), 7).unwrap();
    let raw = NodeList::make1(mcx, res_target(mcx, None, sconst)).unwrap();

    let tlist =
        transformTargetList(mcx, &mut pstate, &raw, ParseExprKind::EXPR_KIND_SELECT_TARGET)
            .unwrap();
    resolveTargetListUnknowns(mcx, &pstate, &tlist).unwrap();

    let te = tlist.nth(0).as_target_entry().unwrap();
    let c = te.expr.as_const().unwrap();
    assert_eq!(c.consttype, types_core::catalog::TEXTOID);
    assert_eq!((c.constlen, c.constbyval, c.constisnull), (-1, false, false));
    assert_eq!(c.constcollid, 100);
    // SAFETY: the datum points at a flat 4B-header text varlena copied into mcx.
    let v = unsafe { datum::varlena::VarlenaRef::from_ptr(c.constvalue.as_usize() as *const u8) };
    assert_eq!(v.data(), b"x");
}

#[test]
fn bare_star_with_no_tables_is_42601() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let star = Node::mk_a_star(mcx).unwrap();
    let cref = Node::mk_column_ref(mcx, NodeList::make1(mcx, star).unwrap(), 7).unwrap();
    let raw = NodeList::make1(mcx, res_target(mcx, None, cref)).unwrap();
    let err = transformTargetList(mcx, &mut pstate, &raw, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message, "SELECT * with no tables specified is not valid");
}

#[test]
fn figure_colname_arms() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = |v| Node::mk(mcx, PgStr { sval: v }).unwrap();

    let cref =
        Node::mk_column_ref(mcx, NodeList::make2(mcx, s("tab"), s("col")).unwrap(), 0).unwrap();
    assert_eq!(FigureColname(cref), "col");

    let starred = Node::mk_column_ref(
        mcx,
        NodeList::make2(mcx, s("tab"), Node::mk_a_star(mcx).unwrap()).unwrap(),
        0,
    )
    .unwrap();
    assert_eq!(FigureColname(starred), "tab");

    assert_eq!(FigureColname(int_const(mcx, 1, 0)), "?column?");
    assert_eq!(FigureColname(Node::mk_param_ref(mcx, 1, 0).unwrap()), "?column?");

    let collate = Node::mk(
        mcx,
        types_nodes::CollateClause {
            arg: Some(cref),
            collname: NodeList::make1(mcx, s("C")).unwrap(),
            location: 0,
        },
    )
    .unwrap();
    assert_eq!(FigureColname(collate), "col");

    let svf = Node::mk(
        mcx,
        types_nodes::SQLValueFunction {
            op: types_nodes::SQLValueFunctionOp::SVFOP_CURRENT_USER,
            r#type: 0,
            typmod: -1,
            location: 0,
        },
    )
    .unwrap();
    assert_eq!(FigureColname(svf), "current_user");

    let mut sub = Node::build::<types_nodes::SelectStmt>(mcx).unwrap();
    sub.targetList = NodeList::make1(mcx, res_target(mcx, None, cref)).unwrap();
    let sub = sub.seal();
    let sublink = |ty| {
        Node::mk(
            mcx,
            types_nodes::SubLink {
                subLinkType: ty,
                subLinkId: 0,
                testexpr: None,
                operName: NodeList::nil(),
                subselect: sub,
                location: 0,
            },
        )
        .unwrap()
    };
    // A raw (untransformed) EXPR_SUBLINK derives the resname its transform
    // would assign: the first ResTarget's name, else its value's name.
    assert_eq!(FigureColname(sublink(types_nodes::SubLinkType::EXPR_SUBLINK)), "col");
    assert_eq!(FigureColname(sublink(types_nodes::SubLinkType::EXISTS_SUBLINK)), "exists");
    assert_eq!(FigureColname(sublink(types_nodes::SubLinkType::ANY_SUBLINK)), "?column?");
}

fn record_var<'mcx>(mcx: Mcx<'mcx>, varno: i32, varattno: i16) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::Var {
            varno,
            varattno,
            vartype: types_core::catalog::RECORDOID,
            vartypmod: -1,
            varcollid: types_core::InvalidOid,
            varnullingrels: types_nodes::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: types_nodes::VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: varno as types_core::Index,
            varattnosyn: varattno,
            location: -1,
        },
    )
    .unwrap()
}

// A subquery RTE whose tlist is (a int4, b text), with matching eref colnames.
fn subquery_rte<'mcx>(mcx: Mcx<'mcx>, rtable: NodeList<'mcx>) -> Node<'mcx> {
    use types_core::catalog::{DEFAULT_COLLATION_OID, INT4OID, TEXTOID};
    use types_core::InvalidOid;
    let c1 = Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, datum::Datum::null(), true, true)
        .unwrap();
    let c2 = Node::mk_const(
        mcx,
        TEXTOID,
        -1,
        DEFAULT_COLLATION_OID,
        -1,
        datum::Datum::null(),
        true,
        false,
    )
    .unwrap();
    let te1 = Node::mk_target_entry(mcx, c1, 1, Some("a"), false).unwrap();
    let te2 = Node::mk_target_entry(mcx, c2, 2, Some("b"), false).unwrap();
    let q = mcx::leak_in(
        mcx::alloc_in(
            mcx,
            types_nodes::parsenodes::Query {
                targetList: NodeList::make2(mcx, te1, te2).unwrap(),
                rtable,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let eref = Node::mk_mut(
        mcx,
        types_nodes::Alias {
            aliasname: Some("ss"),
            colnames: NodeList::make2(
                mcx,
                Node::mk_string(mcx, "a").unwrap(),
                Node::mk_string(mcx, "b").unwrap(),
            )
            .unwrap(),
        },
    )
    .unwrap()
    .seal_ref();
    Node::mk(
        mcx,
        types_nodes::RangeTblEntry {
            rtekind: types_nodes::RTEKind::RTE_SUBQUERY,
            subquery: Some(q),
            eref: Some(eref),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn expand_record_variable_whole_row_over_subquery() {
    use types_core::catalog::{DEFAULT_COLLATION_OID, INT4OID, TEXTOID};
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_rtable.lappend(mcx, subquery_rte(mcx, NodeList::nil())).unwrap();

    let desc =
        crate::expandRecordVariable(mcx, &pstate, record_var(mcx, 1, 0), 0).unwrap();
    assert_eq!(desc.natts, 2);
    assert_eq!(desc.attr(0).atttypid, INT4OID);
    assert_eq!(desc.attr(0).attname.name_str(), b"a");
    assert_eq!(desc.attr(1).atttypid, TEXTOID);
    assert_eq!(desc.attr(1).attname.name_str(), b"b");
    assert_eq!(desc.attr(1).attcollation, DEFAULT_COLLATION_OID);
}

#[test]
fn expand_record_variable_drills_through_subquery_var() {
    use types_core::catalog::{INT4OID, TEXTOID};
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    // Outer subquery's single output is a whole-row RECORD Var over an inner
    // subquery RTE; the drill recurses through the fake ParseState.
    let inner_rte = subquery_rte(mcx, NodeList::nil());
    let te = Node::mk_target_entry(mcx, record_var(mcx, 1, 0), 1, Some("r"), false).unwrap();
    let q = mcx::leak_in(
        mcx::alloc_in(
            mcx,
            types_nodes::parsenodes::Query {
                targetList: NodeList::make1(mcx, te).unwrap(),
                rtable: NodeList::make1(mcx, inner_rte).unwrap(),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let eref = Node::mk_mut(
        mcx,
        types_nodes::Alias {
            aliasname: Some("outer_ss"),
            colnames: NodeList::make1(mcx, Node::mk_string(mcx, "r").unwrap()).unwrap(),
        },
    )
    .unwrap()
    .seal_ref();
    let outer_rte = Node::mk(
        mcx,
        types_nodes::RangeTblEntry {
            rtekind: types_nodes::RTEKind::RTE_SUBQUERY,
            subquery: Some(q),
            eref: Some(eref),
            ..Default::default()
        },
    )
    .unwrap();
    pstate.p_rtable.lappend(mcx, outer_rte).unwrap();

    let desc =
        crate::expandRecordVariable(mcx, &pstate, record_var(mcx, 1, 1), 0).unwrap();
    assert_eq!(desc.natts, 2);
    assert_eq!(desc.attr(0).atttypid, INT4OID);
    assert_eq!(desc.attr(1).atttypid, TEXTOID);
}
