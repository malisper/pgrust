// Closed-set slice of nodeFuncs.c exprType/exprTypmod/exprCollation/
// exprLocation over the tags this parser lane can produce; migrates to
// backend-nodes-core when that unit lands its expression accessors.
use types_core::{Oid, ParseLoc};
use types_nodes::{Node, NodeTag};

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
        other => deferred("exprType", other),
    }
}

pub fn expr_typmod(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttypmod,
        NodeTag::T_Var => node.as_var().unwrap().vartypmod,
        NodeTag::T_Param => node.as_param().unwrap().paramtypmod,
        NodeTag::T_OpExpr | NodeTag::T_FuncExpr => -1,
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
        other => deferred("exprCollation", other),
    }
}

pub fn expr_location(node: Node<'_>) -> ParseLoc {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().location,
        NodeTag::T_Var => node.as_var().unwrap().location,
        NodeTag::T_Param => node.as_param().unwrap().location,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().location,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().location,
        NodeTag::T_A_Const => node.as_a_const().unwrap().location,
        NodeTag::T_A_Expr => node.as_a_expr().unwrap().location,
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
