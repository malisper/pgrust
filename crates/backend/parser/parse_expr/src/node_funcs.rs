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
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wintype,
        NodeTag::T_GroupingFunc => types_core::catalog::INT4OID,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_BoolExpr | NodeTag::T_NullTest => types_core::catalog::BOOLOID,
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().typeId,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeId,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxtype,
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().r#type,
        NodeTag::T_SubLink => {
            let (sl, tent) = sublink_first_col(node);
            match sl.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK => expr_type(tent.expect("EXPR").expr),
                types_nodes::SubLinkType::ARRAY_SUBLINK => {
                    deferred("exprType: ARRAY_SUBLINK", NodeTag::T_SubLink)
                }
                _ => types_core::catalog::BOOLOID,
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK
                | types_nodes::SubLinkType::ARRAY_SUBLINK
                | types_nodes::SubLinkType::MULTIEXPR_SUBLINK => sp.firstColType,
                _ => types_core::catalog::BOOLOID,
            }
        }
        other => deferred("exprType", other),
    }
}

// C exprType's untransformed-sublink elog is a panic here (parse always
// rewrites subselect to a Query before any exprType consumer runs).
fn sublink_first_col<'mcx>(
    node: Node<'mcx>,
) -> (&'mcx types_nodes::SubLink<'mcx>, Option<&'mcx types_nodes::TargetEntry<'mcx>>) {
    let sl = node.as_sub_link().unwrap();
    let tent = sl
        .subselect
        .as_query()
        .unwrap_or_else(|| panic!("cannot get type for untransformed sublink"))
        .targetList
        .first()
        .map(|n| n.as_target_entry().expect("tlist entry"));
    (sl, tent)
}

pub fn expr_typmod(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttypmod,
        NodeTag::T_Var => node.as_var().unwrap().vartypmod,
        NodeTag::T_Param => node.as_param().unwrap().paramtypmod,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttypmod,
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().typeMod,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeMod,
        NodeTag::T_OpExpr
        | NodeTag::T_FuncExpr
        | NodeTag::T_Aggref
        | NodeTag::T_GroupingFunc
        | NodeTag::T_WindowFunc
        | NodeTag::T_CoerceViaIO
        | NodeTag::T_BoolExpr
        | NodeTag::T_NullTest => -1,
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let Some(defresult) = c.defresult else { return -1 };
            if expr_type(defresult) != c.casetype {
                return -1;
            }
            let typmod = expr_typmod(defresult);
            if typmod < 0 {
                return -1;
            }
            for w in &c.args {
                let result = w.as_case_when().expect("CaseWhen").result.expect("result");
                if expr_type(result) != c.casetype || expr_typmod(result) != typmod {
                    return -1;
                }
            }
            typmod
        }
        NodeTag::T_CoalesceExpr => {
            let c = node.as_coalesce_expr().unwrap();
            uniform_args_typmod(&c.args, c.coalescetype)
        }
        NodeTag::T_MinMaxExpr => {
            let m = node.as_min_max_expr().unwrap();
            uniform_args_typmod(&m.args, m.minmaxtype)
        }
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().typmod,
        NodeTag::T_SubLink => {
            let (sl, tent) = sublink_first_col(node);
            match sl.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK => expr_typmod(tent.expect("EXPR").expr),
                _ => -1,
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK => sp.firstColTypmod,
                _ => -1,
            }
        }
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
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wincollid,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resultcollid,
        NodeTag::T_BoolExpr | NodeTag::T_NullTest | NodeTag::T_GroupingFunc => {
            types_core::InvalidOid
        }
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().collation,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().collation,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescecollid,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxcollid,
        NodeTag::T_SQLValueFunction => {
            if node.as_sql_value_function().unwrap().r#type == types_core::catalog::NAMEOID {
                types_core::catalog::C_COLLATION_OID
            } else {
                types_core::InvalidOid
            }
        }
        NodeTag::T_SubLink => {
            let (sl, tent) = sublink_first_col(node);
            match sl.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK
                | types_nodes::SubLinkType::ARRAY_SUBLINK => {
                    expr_collation(tent.expect("EXPR/ARRAY").expr)
                }
                _ => 0,
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK
                | types_nodes::SubLinkType::ARRAY_SUBLINK
                | types_nodes::SubLinkType::MULTIEXPR_SUBLINK => sp.firstColCollation,
                _ => 0,
            }
        }
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

/// C `exprLocation` T_List arm: first member with a known location.
pub fn expr_location_list(list: &NodeList<'_>) -> ParseLoc {
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
            leftmost_loc(op.location, expr_location_list(&op.args))
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            leftmost_loc(f.location, expr_location_list(&f.args))
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            leftmost_loc(r.location, expr_location(r.arg))
        }
        NodeTag::T_Aggref => node.as_aggref().unwrap().location,
        NodeTag::T_GroupingFunc => node.as_grouping_func().unwrap().location,
        NodeTag::T_GroupingSet => node.as_grouping_set().unwrap().location,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().location,
        NodeTag::T_List => expr_location_list(node.as_list().unwrap()),
        NodeTag::T_A_Const => node.as_a_const().unwrap().location,
        NodeTag::T_A_Expr => {
            let a = node.as_a_expr().unwrap();
            leftmost_loc(a.location, a.lexpr.map_or(-1, expr_location))
        }
        NodeTag::T_ColumnRef => node.as_column_ref().unwrap().location,
        NodeTag::T_FuncCall => {
            let f = node.as_func_call().unwrap();
            leftmost_loc(f.location, expr_location_list(&f.args))
        }
        NodeTag::T_ParamRef => node.as_param_ref().unwrap().location,
        NodeTag::T_ResTarget => node.as_res_target().unwrap().location,
        NodeTag::T_SubLink => node.as_sub_link().unwrap().location,
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().location,
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            leftmost_loc(c.location, expr_location(c.arg))
        }
        // C: the CASE/WHEN/COALESCE/GREATEST/LEAST keyword is always leftmost.
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().location,
        // C exprLocation default arm: CaseTestExpr carries no location.
        NodeTag::T_CaseTestExpr => -1,
        NodeTag::T_CaseWhen => node.as_case_when().unwrap().location,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().location,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().location,
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().location,
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            leftmost_loc(b.location, expr_location_list(&b.args))
        }
        NodeTag::T_NullTest => {
            let n = node.as_null_test().unwrap();
            leftmost_loc(n.location, n.arg.map_or(-1, expr_location))
        }
        NodeTag::T_TypeCast => {
            let tc = node.as_type_cast().unwrap();
            let tn_loc = tc
                .typeName
                .and_then(|n| n.as_variant::<types_nodes::TypeName>())
                .map_or(-1, |tn| tn.location);
            let loc = leftmost_loc(tc.arg.map_or(-1, expr_location), tn_loc);
            leftmost_loc(loc, tc.location)
        }
        other => deferred("exprLocation", other),
    }
}

// exprTypmod's shared COALESCE/MinMax shape: all args agree on type+typmod.
fn uniform_args_typmod(args: &NodeList<'_>, common_type: Oid) -> i32 {
    let mut typmod = -1;
    for (i, e) in args.iter().enumerate() {
        if expr_type(e) != common_type {
            return -1;
        }
        if i == 0 {
            typmod = expr_typmod(e);
            if typmod < 0 {
                return -1;
            }
        } else if expr_typmod(e) != typmod {
            return -1;
        }
    }
    typmod
}

pub fn expr_is_null_constant(node: Node<'_>) -> bool {
    match node.as_a_const() {
        Some(ac) => ac.isnull(),
        None => false,
    }
}
