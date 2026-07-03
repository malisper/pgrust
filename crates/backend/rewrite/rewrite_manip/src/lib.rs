// map_variable_attnos expression lane; to_rowtype and SubLink/Query walks loud.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::AttrNumber;
use types_error::PgResult;
use types_nodes::primnodes::Var;
use types_nodes::{Node, NodeTag};

pub fn map_variable_attnos<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    target_varno: i32,
    sublevels_up: u32,
    attnums: &[AttrNumber],
) -> PgResult<(Node<'mcx>, bool)> {
    let mut found_whole_row = false;
    let mapped = mutate(mcx, node, target_varno, sublevels_up, attnums, &mut found_whole_row)?
        .unwrap_or(node);
    Ok((mapped, found_whole_row))
}

fn mutate<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    target_varno: i32,
    sublevels_up: u32,
    attnums: &[AttrNumber],
    found_whole_row: &mut bool,
) -> PgResult<Option<Node<'mcx>>> {
    if node.node_tag() == NodeTag::T_Var {
        let var = node.as_variant::<Var>().expect("Var");
        if var.varno == target_varno && var.varlevelsup == sublevels_up {
            let attno = var.varattno;
            if attno > 0 {
                if attno as usize > attnums.len() || attnums[attno as usize - 1] == 0 {
                    panic!("unexpected varattno {attno} in expression to be mapped");
                }
                let mut newvar = Var {
                    varnullingrels: var.varnullingrels.clone_in(mcx)?,
                    ..*var
                };
                newvar.varattno = attnums[attno as usize - 1];
                if newvar.varnosyn == target_varno as u32 {
                    newvar.varattnosyn = newvar.varattno;
                }
                return Ok(Some(Node::mk(mcx, newvar)?));
            }
            if attno == 0 {
                *found_whole_row = true;
            }
            // attno < 0 (system column): C copies the Var unchanged.
            return Ok(None);
        }
        return Ok(None);
    }
    if node.node_tag() == NodeTag::T_ConvertRowtypeExpr {
        panic!("unported: map_variable_attnos over ConvertRowtypeExpr");
    }
    if node.node_tag() == NodeTag::T_SubLink {
        // nodes_core's SubLink arm skips the subselect C recurses into.
        panic!("unported: map_variable_attnos over SubLink (Query walk)");
    }
    let mut m = |n: Node<'mcx>| mutate(mcx, n, target_varno, sublevels_up, attnums, found_whole_row);
    nodes_core::expression_tree_mutator(mcx, node, &mut m)
}
