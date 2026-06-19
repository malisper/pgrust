//! Unit tests over the pure (no-catalog, no-ParseState) FigureColname logic.

extern crate std;

use super::*;
use mcx::MemoryContext;
use types_nodes::value::StringNode;

/// Build a `Node::String` value node.
fn string_node<'mcx>(mcx: Mcx<'mcx>, s: &str) -> NodePtr<'mcx> {
    alloc_in(
        mcx,
        Node::mk_string(mcx, StringNode {
            sval: PgString::from_str_in(s, mcx).unwrap(),
        }),
    )
    .unwrap()
}

#[test]
fn figure_colname_columnref_last_field() {
    let root = MemoryContext::new("t");
    let mcx = root.mcx();
    let mut fields: PgVec<NodePtr> = PgVec::new_in(mcx);
    fields.push(string_node(mcx, "rel"));
    fields.push(string_node(mcx, "mycol"));
    let cref = Node::mk_column_ref(mcx, ColumnRef {
        fields,
        location: -1,
    });
    assert_eq!(FigureColname(Some(&cref)).as_deref(), Some("mycol"));
}

#[test]
fn figure_colname_default() {
    // A NULL node yields the "?column?" default.
    assert_eq!(FigureColname(None).as_deref(), Some("?column?"));
}

#[test]
fn figure_index_colname_returns_none_without_name() {
    // FigureIndexColname returns None when no good name can be picked.
    assert_eq!(FigureIndexColname(None), None);
}

#[test]
fn figure_colname_nullif() {
    let root = MemoryContext::new("t");
    let mcx = root.mcx();
    let ae = Node::A_Expr(types_nodes::rawnodes::A_Expr {
        kind: types_nodes::rawnodes::A_Expr_Kind::AEXPR_NULLIF,
        name: PgVec::new_in(mcx),
        lexpr: None,
        rexpr: None,
        rexpr_list_start: -1,
        rexpr_list_end: -1,
        location: -1,
    });
    assert_eq!(FigureColname(Some(&ae)).as_deref(), Some("nullif"));
}

#[test]
fn figure_colname_grouping() {
    let root = MemoryContext::new("t");
    let mcx = root.mcx();
    let gf = Node::mk_grouping_func(mcx, types_nodes::rawexprnodes::GroupingFunc {
        args: PgVec::new_in(mcx),
        refs: PgVec::new_in(mcx),
        cols: PgVec::new_in(mcx),
        agglevelsup: 0,
        location: -1,
    });
    assert_eq!(FigureColname(Some(&gf)).as_deref(), Some("grouping"));
}
