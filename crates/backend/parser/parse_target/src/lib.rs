#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use parse_expr::{expr_type, transformExpr};
use parser_small1::{ParseExprKind, ParseState};
use types_core::catalog::UNKNOWNOID;
use types_core::AttrNumber;
use types_error::PgResult;
use types_nodes::rawnodes::A_Expr_Kind;
use types_nodes::{Node, NodeList, NodeTag};

pub fn transformTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
    exprKind: ParseExprKind,
) -> PgResult<NodeList<'mcx>> {
    let mut p_target = NodeList::nil();
    debug_assert!(pstate.p_multiassign_exprs.is_nil());
    let expand_star = exprKind != ParseExprKind::EXPR_KIND_UPDATE_SOURCE;

    for o_target in targetlist {
        let res = o_target
            .as_res_target()
            .unwrap_or_else(|| panic!("targetlist element is not a ResTarget: {o_target:?}"));
        let val = res.val.expect("ResTarget.val is never NULL in a raw targetlist");

        if expand_star {
            if let Some(cref) = val.as_column_ref() {
                if cref.fields.last().is_some_and(|f| f.node_tag() == NodeTag::T_A_Star) {
                    panic!(
                        "transformTargetList (parse_target.c): ExpandColumnRefStar \
                         (something.*) unported — unit backend-parser-parse-target"
                    );
                }
            } else if val.node_tag() == NodeTag::T_A_Indirection {
                panic!(
                    "transformTargetList (parse_target.c): ExpandIndirectionStar \
                     unported — unit backend-parser-parse-target"
                );
            }
        }

        let te = transformTargetEntry(mcx, pstate, val, None, exprKind, res.name, false)?;
        p_target.lappend(mcx, te)?;
    }

    if !pstate.p_multiassign_exprs.is_nil() {
        panic!(
            "transformTargetList (parse_target.c): multiassign resjunk attach \
             (UPDATE tlist) unported — unit backend-parser-parse-target"
        );
    }

    Ok(p_target)
}

pub fn transformTargetEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    expr: Option<Node<'mcx>>,
    exprKind: ParseExprKind,
    colname: Option<&'mcx str>,
    resjunk: bool,
) -> PgResult<Node<'mcx>> {
    let expr = match expr {
        Some(e) => e,
        None => {
            if exprKind == ParseExprKind::EXPR_KIND_UPDATE_SOURCE
                && node.node_tag() == NodeTag::T_SetToDefault
            {
                node
            } else {
                transformExpr(mcx, pstate, node, exprKind)?
            }
        }
    };

    let colname = match colname {
        None if !resjunk => Some(FigureColname(node)),
        other => other,
    };

    let resno = pstate.p_next_resno as AttrNumber;
    pstate.p_next_resno += 1;
    Node::mk_target_entry(mcx, expr, resno, colname, resjunk)
}

pub fn markTargetListOrigins<'mcx>(
    _pstate: &ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<()> {
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().unwrap();
        if tle.expr.node_tag() == NodeTag::T_Var {
            panic!(
                "markTargetListOrigins (parse_target.c): markTargetListOrigin Var arm \
                 (GetNSItemByRangeTablePosn) unported — unit backend-parser-parse-target"
            );
        }
    }
    Ok(())
}

pub fn resolveTargetListUnknowns<'mcx>(
    _pstate: &ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<()> {
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().unwrap();
        if expr_type(tle.expr) == UNKNOWNOID {
            panic!(
                "resolveTargetListUnknowns (parse_target.c): coerce_type \
                 UNKNOWN→TEXT literal path unported — unit backend-parser-coerce"
            );
        }
    }
    Ok(())
}

pub fn FigureColname<'mcx>(node: Node<'mcx>) -> &'mcx str {
    FigureColnameInternal(node).unwrap_or("?column?")
}

fn FigureColnameInternal<'mcx>(node: Node<'mcx>) -> Option<&'mcx str> {
    match node.node_tag() {
        NodeTag::T_ColumnRef => {
            let mut fname = None;
            for f in &node.as_column_ref().unwrap().fields {
                if let Some(s) = f.as_string() {
                    fname = Some(s.sval);
                }
            }
            fname
        }
        NodeTag::T_A_Expr => {
            if node.as_a_expr().unwrap().kind == A_Expr_Kind::AEXPR_NULLIF {
                Some("nullif")
            } else {
                None
            }
        }
        NodeTag::T_A_Const | NodeTag::T_ParamRef => None,
        other => panic!(
            "FigureColnameInternal (parse_target.c): arm for {other:?} unported — \
             unit backend-parser-parse-target"
        ),
    }
}
