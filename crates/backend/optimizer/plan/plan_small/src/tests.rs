//! Unit tests for the in-crate (seam-free) logic of `query_planner`: the
//! single-`RTE_RESULT` trivial-path jointree inspection (`trivial_path_varno`)
//! and the `DEBUG_PARALLEL_OFF` constant.
//!
//! The orchestration body of `query_planner` is delegation to sibling optimizer
//! crates (direct calls and loud-panic seams); the trivial fast path itself
//! needs a fully-built `PlannerInfo` plus a complete provider install, so it is
//! not exercised here. These tests target the genuine in-crate decision logic
//! the C does itself: the `list_length(fromlist) == 1 && IsA(RangeTblRef)` test.

use super::*;

use mcx::{MemoryContext, PgBox, PgVec};
use ::nodes::nodes::Node;
use ::nodes::rawnodes::{FromExpr, RangeTblRef};

fn rtr<'mcx>(mcx: Mcx<'mcx>, rtindex: i32) -> PgBox<'mcx, Node<'mcx>> {
    PgBox::new_in(Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex }), mcx)
}

fn from_expr<'mcx>(
    mcx: Mcx<'mcx>,
    entries: impl IntoIterator<Item = PgBox<'mcx, Node<'mcx>>>,
) -> FromExpr<'mcx> {
    let mut fromlist = PgVec::new_in(mcx);
    for e in entries {
        fromlist.push(e);
    }
    FromExpr {
        fromlist,
        quals: None,
    }
}

#[test]
fn trivial_path_single_rangetblref_returns_rtindex() {
    // list_length(fromlist) == 1 && IsA(jtnode, RangeTblRef) -> Some(rtindex).
    let cx = MemoryContext::new("plan-small-test");
    let mcx = cx.mcx();

    let jt = from_expr(mcx, [rtr(mcx, 1)]);
    assert_eq!(trivial_path_varno(&jt), Some(1));

    let jt = from_expr(mcx, [rtr(mcx, 7)]);
    assert_eq!(trivial_path_varno(&jt), Some(7));
}

#[test]
fn trivial_path_single_non_rangetblref_is_none() {
    // list_length == 1 but the single entry is not a RangeTblRef (a nested
    // FromExpr / JoinExpr leaf) -> None.
    let cx = MemoryContext::new("plan-small-test");
    let mcx = cx.mcx();

    let inner = PgBox::new_in(Node::mk_from_expr(mcx, from_expr(mcx, [rtr(mcx, 1)])), mcx);
    let jt = from_expr(mcx, [inner]);
    assert_eq!(trivial_path_varno(&jt), None);
}

#[test]
fn trivial_path_multiple_entries_is_none() {
    // list_length(fromlist) > 1 -> None (a real join, not the trivial path).
    let cx = MemoryContext::new("plan-small-test");
    let mcx = cx.mcx();

    let jt = from_expr(mcx, [rtr(mcx, 1), rtr(mcx, 2)]);
    assert_eq!(trivial_path_varno(&jt), None);
}

#[test]
#[should_panic(expected = "parse->jointree->fromlist != NIL")]
fn trivial_path_empty_fromlist_panics() {
    // Assert(parse->jointree->fromlist != NIL).
    let cx = MemoryContext::new("plan-small-test");
    let mcx = cx.mcx();

    let jt = from_expr(mcx, []);
    let _ = trivial_path_varno(&jt);
}

#[test]
fn debug_parallel_off_is_zero() {
    assert_eq!(DEBUG_PARALLEL_OFF, 0);
}
