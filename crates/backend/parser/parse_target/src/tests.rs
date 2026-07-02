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
    resolveTargetListUnknowns(&pstate, &tlist).unwrap();
}

#[test]
#[should_panic(expected = "ExpandColumnRefStar")]
fn star_target_panics_at_expansion() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let star = Node::mk_a_star(mcx).unwrap();
    let cref = Node::mk_column_ref(mcx, NodeList::make1(mcx, star).unwrap(), 7).unwrap();
    let raw = NodeList::make1(mcx, res_target(mcx, None, cref)).unwrap();
    let _ = transformTargetList(mcx, &mut pstate, &raw, ParseExprKind::EXPR_KIND_SELECT_TARGET);
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
}
