// Closed-set slice of nodeFuncs.c exprType/exprTypmod/exprCollation/
// exprLocation over the tags this parser lane can produce; migrates to
// backend-nodes-core when that unit lands its expression accessors.
use types_core::{Oid, ParseLoc};
use types_nodes::{Node, NodeList, NodeTag};

#[cold]
#[inline(never)]
fn deferred(what: &str, tag: NodeTag) -> ! {
    panic!("{what} (nodeFuncs.c): arm for {tag:?} unported — backend-nodes-core lane")
}

pub fn expr_type(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_Param => node.as_param().unwrap().paramtype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        other => deferred("exprType", other),
    }
}

pub fn expr_typmod(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttypmod,
        NodeTag::T_Var => node.as_var().unwrap().vartypmod,
        NodeTag::T_Param => node.as_param().unwrap().paramtypmod,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttypmod,
        NodeTag::T_OpExpr | NodeTag::T_FuncExpr | NodeTag::T_Aggref => -1,
        other => deferred("exprTypmod", other),
    }
}

pub fn expr_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Param => node.as_param().unwrap().paramcollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggcollid,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        other => deferred("exprCollation", other),
    }
}

fn leftmost_loc(loc1: ParseLoc, loc2: ParseLoc) -> ParseLoc {
    if loc1 < 0 {
        loc2
    } else if loc2 < 0 {
        loc1
    } else {
        loc1.min(loc2)
    }
}

fn list_location(list: &NodeList<'_>) -> ParseLoc {
    for n in list {
        let loc = expr_location(n);
        if loc >= 0 {
            return loc;
        }
    }
    -1
}

pub fn expr_location(node: Node<'_>) -> ParseLoc {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().location,
        NodeTag::T_Var => node.as_var().unwrap().location,
        NodeTag::T_Param => node.as_param().unwrap().location,
        NodeTag::T_OpExpr => {
            let op = node.as_op_expr().unwrap();
            leftmost_loc(op.location, list_location(&op.args))
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            leftmost_loc(f.location, list_location(&f.args))
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            leftmost_loc(r.location, expr_location(r.arg))
        }
        NodeTag::T_Aggref => node.as_aggref().unwrap().location,
        NodeTag::T_A_Const => node.as_a_const().unwrap().location,
        NodeTag::T_A_Expr => {
            let a = node.as_a_expr().unwrap();
            leftmost_loc(a.location, a.lexpr.map_or(-1, expr_location))
        }
        NodeTag::T_ColumnRef => node.as_column_ref().unwrap().location,
        NodeTag::T_ParamRef => node.as_param_ref().unwrap().location,
        NodeTag::T_ResTarget => node.as_res_target().unwrap().location,
        other => deferred("exprLocation", other),
    }
}

pub fn expr_is_null_constant(node: Node<'_>) -> bool {
    match node.as_a_const() {
        Some(ac) => ac.isnull(),
        None => false,
    }
}
