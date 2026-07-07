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
        NodeTag::T_ScalarArrayOpExpr => types_core::catalog::BOOLOID,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_NamedArgExpr => {
            expr_type(node.as_named_arg_expr().unwrap().arg.expect("NamedArgExpr has an arg"))
        }
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wintype,
        NodeTag::T_GroupingFunc => types_core::catalog::INT4OID,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_FieldSelect => node.as_field_select().unwrap().resulttype,
        NodeTag::T_CollateExpr => expr_type(node.as_collate_expr().unwrap().arg),
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_ArrayCoerceExpr => node.as_array_coerce_expr().unwrap().resulttype,
        NodeTag::T_ConvertRowtypeExpr => node.as_convert_rowtype_expr().unwrap().resulttype,
        NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest
        | NodeTag::T_CurrentOfExpr => types_core::catalog::BOOLOID,
        NodeTag::T_MergeSupportFunc => node.as_merge_support_func().unwrap().msftype,
        NodeTag::T_DistinctExpr => node.as_distinct_expr().unwrap().opresulttype,
        NodeTag::T_NullIfExpr => node.as_null_if_expr().unwrap().opresulttype,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
        NodeTag::T_FieldStore => node.as_field_store().unwrap().resulttype,
        NodeTag::T_RowCompareExpr => types_core::catalog::BOOLOID,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttype,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().typeId,
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().typeId,
        NodeTag::T_CollateExpr => expr_type(node.as_collate_expr().unwrap().arg),
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().r#type,
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().refrestype,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeId,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxtype,
        NodeTag::T_XmlExpr => match node.as_xml_expr().unwrap().op {
            types_nodes::XmlExprOp::IS_DOCUMENT => types_core::catalog::BOOLOID,
            types_nodes::XmlExprOp::IS_XMLSERIALIZE => types_core::catalog::TEXTOID,
            _ => types_core::catalog::XMLOID,
        },
        NodeTag::T_NextValueExpr => {
            node.as_variant::<types_nodes::primnodes::NextValueExpr>().unwrap().typeId
        }
        NodeTag::T_SubLink => {
            let (sl, tent) = sublink_first_col(node);
            match sl.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK => expr_type(tent.expect("EXPR").expr),
                types_nodes::SubLinkType::ARRAY_SUBLINK => {
                    promoted_array_type(expr_type(tent.expect("ARRAY").expr))
                }
                _ => types_core::catalog::BOOLOID,
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK
                => sp.firstColType,
                // C: MULTIEXPR SubPlans return a dummy NULL::record.
                types_nodes::SubLinkType::MULTIEXPR_SUBLINK => types_core::RECORDOID,
                types_nodes::SubLinkType::ARRAY_SUBLINK => {
                    promoted_array_type(sp.firstColType)
                }
                _ => types_core::catalog::BOOLOID,
            }
        }
        NodeTag::T_PlaceHolderVar => {
            expr_type(node.as_place_holder_var().unwrap().phexpr)
        }
        NodeTag::T_ReturningExpr => expr_type(node.as_returning_expr().unwrap().retexpr),
        NodeTag::T_JsonValueExpr => {
            expr_type(node.as_json_value_expr().unwrap().formatted_expr.expect("formatted_expr"))
        }
        NodeTag::T_JsonConstructorExpr => {
            node.as_json_constructor_expr().unwrap().returning.expect("returning").typid
        }
        NodeTag::T_JsonIsPredicate => types_core::catalog::BOOLOID,
        NodeTag::T_JsonExpr => node.as_json_expr().unwrap().returning.expect("returning").typid,
        // C exprType(NULL) == InvalidOid; ERROR-btype behaviors carry no expr.
        NodeTag::T_JsonBehavior => {
            node.as_json_behavior().unwrap().expr.map_or(types_core::InvalidOid, expr_type)
        }
        NodeTag::T_AlternativeSubPlan => expr_type(
            node.as_alternative_sub_plan().unwrap().subplans.first().expect("subplans non-empty"),
        ),
        other => deferred("exprType", other),
    }
}

// DIVERGENCE: C ereports 42704 for an arrayless element type (e.g. ARRAY
// over a void-returning select); loud panic here since expr_type is
// infallible by signature.
pub fn promoted_array_type(elemtype: Oid) -> Oid {
    let arraytype = lsyscache::get_promoted_array_type(elemtype)
        .unwrap_or_else(|e| panic!("get_promoted_array_type({elemtype}): {e}"));
    assert!(
        arraytype != types_core::InvalidOid,
        "could not find array type for data type {elemtype}"
    );
    arraytype
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

// exprIsLengthCoercion (nodeFuncs.c) FuncExpr arm; the ArrayCoerceExpr arm
// is exprTypmod's direct resulttypmod read. -1 = not one.
fn length_coercion_typmod(f: &types_nodes::FuncExpr<'_>) -> i32 {
    if !matches!(
        f.funcformat,
        types_nodes::CoercionForm::COERCE_EXPLICIT_CAST
            | types_nodes::CoercionForm::COERCE_IMPLICIT_CAST
    ) || !(2..=3).contains(&f.args.len())
    {
        return -1;
    }
    match f.args.nth(1).as_const() {
        Some(c) if c.consttype == types_core::catalog::INT4OID && !c.constisnull => {
            c.constvalue.as_i32()
        }
        _ => -1,
    }
}

pub fn expr_typmod(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttypmod,
        NodeTag::T_Var => node.as_var().unwrap().vartypmod,
        NodeTag::T_Param => node.as_param().unwrap().paramtypmod,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttypmod,
        NodeTag::T_FieldSelect => node.as_field_select().unwrap().resulttypmod,
        NodeTag::T_CollateExpr => expr_typmod(node.as_collate_expr().unwrap().arg),
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().typeMod,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().typeMod,
        NodeTag::T_FuncExpr => length_coercion_typmod(node.as_func_expr().unwrap()),
        NodeTag::T_NamedArgExpr => {
            expr_typmod(node.as_named_arg_expr().unwrap().arg.expect("NamedArgExpr has an arg"))
        }
        NodeTag::T_ArrayCoerceExpr => node.as_array_coerce_expr().unwrap().resulttypmod,
        // Result is either the first argument or NULL: report its typmod.
        NodeTag::T_NullIfExpr => expr_typmod(node.as_null_if_expr().unwrap().args.nth(0)),
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttypmod,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().typeMod,
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().reftypmod,
        NodeTag::T_MergeSupportFunc
        | NodeTag::T_OpExpr
        | NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_ArrayExpr
        | NodeTag::T_Aggref
        | NodeTag::T_GroupingFunc
        | NodeTag::T_WindowFunc
        | NodeTag::T_CoerceViaIO
        | NodeTag::T_ConvertRowtypeExpr
        | NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest
        | NodeTag::T_DistinctExpr
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_XmlExpr
        | NodeTag::T_FieldStore
        | NodeTag::T_RowCompareExpr
        | NodeTag::T_RowExpr => -1,
        NodeTag::T_CollateExpr => expr_typmod(node.as_collate_expr().unwrap().arg),
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().typmod,
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
        NodeTag::T_SubLink => {
            let (sl, tent) = sublink_first_col(node);
            match sl.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK
                | types_nodes::SubLinkType::ARRAY_SUBLINK => {
                    expr_typmod(tent.expect("EXPR/ARRAY").expr)
                }
                _ => -1,
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                types_nodes::SubLinkType::EXPR_SUBLINK
                | types_nodes::SubLinkType::ARRAY_SUBLINK => sp.firstColTypmod,
                _ => -1,
            }
        }
        NodeTag::T_PlaceHolderVar => {
            expr_typmod(node.as_place_holder_var().unwrap().phexpr)
        }
        NodeTag::T_ReturningExpr => expr_typmod(node.as_returning_expr().unwrap().retexpr),
        NodeTag::T_JsonValueExpr => {
            expr_typmod(node.as_json_value_expr().unwrap().formatted_expr.expect("formatted_expr"))
        }
        NodeTag::T_JsonConstructorExpr => {
            node.as_json_constructor_expr().unwrap().returning.expect("returning").typmod
        }
        NodeTag::T_JsonIsPredicate => -1,
        NodeTag::T_JsonExpr => node.as_json_expr().unwrap().returning.expect("returning").typmod,
        NodeTag::T_JsonBehavior => {
            node.as_json_behavior().unwrap().expr.map_or(-1, expr_typmod)
        }
        NodeTag::T_AlternativeSubPlan => expr_typmod(
            node.as_alternative_sub_plan().unwrap().subplans.first().expect("subplans non-empty"),
        ),
        other => deferred("exprTypmod", other),
    }
}

pub fn expr_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Param => node.as_param().unwrap().paramcollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_ScalarArrayOpExpr => types_core::InvalidOid,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_collid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_NamedArgExpr => {
            expr_collation(node.as_named_arg_expr().unwrap().arg.expect("NamedArgExpr has an arg"))
        }
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggcollid,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wincollid,
        NodeTag::T_MergeSupportFunc => node.as_merge_support_func().unwrap().msfcollid,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        NodeTag::T_FieldSelect => node.as_field_select().unwrap().resultcollid,
        NodeTag::T_CollateExpr => node.as_collate_expr().unwrap().collOid,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resultcollid,
        NodeTag::T_ArrayCoerceExpr => node.as_array_coerce_expr().unwrap().resultcollid,
        NodeTag::T_ConvertRowtypeExpr => types_core::InvalidOid,
        NodeTag::T_BoolExpr
        | NodeTag::T_NullTest
        | NodeTag::T_GroupingFunc
        | NodeTag::T_BooleanTest
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_FieldStore
        | NodeTag::T_RowCompareExpr
        | NodeTag::T_RowExpr => types_core::InvalidOid,
        NodeTag::T_DistinctExpr => node.as_distinct_expr().unwrap().opcollid,
        NodeTag::T_NullIfExpr => node.as_null_if_expr().unwrap().opcollid,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resultcollid,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().collation,
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().collation,
        NodeTag::T_CollateExpr => node.as_collate_expr().unwrap().collOid,
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().refcollid,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().collation,
        NodeTag::T_SQLValueFunction => {
            if node.as_sql_value_function().unwrap().r#type == types_core::catalog::NAMEOID {
                types_core::catalog::C_COLLATION_OID
            } else {
                types_core::InvalidOid
            }
        }
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescecollid,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxcollid,
        // C: XMLSERIALIZE returns text from non-collatable inputs; the other
        // ops return boolean or XML, which are non-collatable.
        NodeTag::T_XmlExpr => {
            if node.as_xml_expr().unwrap().op == types_nodes::XmlExprOp::IS_XMLSERIALIZE {
                types_core::catalog::DEFAULT_COLLATION_OID
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
                => sp.firstColCollation,
                // C: the MULTIEXPR dummy RECORD result is uncollatable.
                types_nodes::SubLinkType::MULTIEXPR_SUBLINK => types_core::InvalidOid,
                _ => 0,
            }
        }
        NodeTag::T_PlaceHolderVar => {
            expr_collation(node.as_place_holder_var().unwrap().phexpr)
        }
        NodeTag::T_ReturningExpr => expr_collation(node.as_returning_expr().unwrap().retexpr),
        NodeTag::T_JsonValueExpr => {
            expr_collation(
                node.as_json_value_expr().unwrap().formatted_expr.expect("formatted_expr"),
            )
        }
        NodeTag::T_JsonConstructorExpr => {
            match node.as_json_constructor_expr().unwrap().coercion {
                Some(c) => expr_collation(c),
                None => types_core::InvalidOid,
            }
        }
        NodeTag::T_JsonIsPredicate => types_core::InvalidOid,
        NodeTag::T_JsonExpr => node.as_json_expr().unwrap().collation,
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => expr_collation(e),
            None => types_core::InvalidOid,
        },
        NodeTag::T_AlternativeSubPlan => expr_collation(
            node.as_alternative_sub_plan().unwrap().subplans.first().expect("subplans non-empty"),
        ),
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
        NodeTag::T_MergeSupportFunc => node.as_merge_support_func().unwrap().location,
        NodeTag::T_OpExpr => {
            let op = node.as_op_expr().unwrap();
            leftmost_loc(op.location, expr_location_list(&op.args))
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let s = node.as_scalar_array_op_expr().unwrap();
            leftmost_loc(s.location, expr_location_list(&s.args))
        }
        // C: the ARRAY or [ keyword is always leftmost.
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().location,
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            leftmost_loc(f.location, expr_location_list(&f.args))
        }
        // C: consider both argument name and value.
        NodeTag::T_NamedArgExpr => {
            let na = node.as_named_arg_expr().unwrap();
            leftmost_loc(
                na.location,
                expr_location(na.arg.expect("NamedArgExpr has an arg")),
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            leftmost_loc(r.location, expr_location(r.arg))
        }
        // C: FieldSelect has no location; report the argument's.
        NodeTag::T_FieldSelect => expr_location(node.as_field_select().unwrap().arg),
        NodeTag::T_ReturningExpr => expr_location(node.as_returning_expr().unwrap().retexpr),
        // C: CollateExpr just uses the argument's location.
        NodeTag::T_CollateExpr => expr_location(node.as_collate_expr().unwrap().arg),
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
        // C: SubscriptingRef has no location; report the container's.
        NodeTag::T_SubscriptingRef => {
            node.as_subscripting_ref().unwrap().refexpr.map_or(-1, expr_location)
        }
        NodeTag::T_A_ArrayExpr => node.as_a_array_expr().unwrap().location,
        NodeTag::T_A_Indirection => {
            node.as_a_indirection().unwrap().arg.map_or(-1, expr_location)
        }
        NodeTag::T_ParamRef => node.as_param_ref().unwrap().location,
        NodeTag::T_ResTarget => node.as_res_target().unwrap().location,
        NodeTag::T_ColumnDef => {
            node.as_variant::<types_nodes::rawnodes::ColumnDef>().unwrap().location
        }
        NodeTag::T_SubLink => {
            let s = node.as_sub_link().unwrap();
            leftmost_loc(s.testexpr.map_or(-1, expr_location), s.location)
        }
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().location,
        NodeTag::T_CurrentOfExpr => -1,
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            leftmost_loc(c.location, expr_location(c.arg))
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            leftmost_loc(a.location, expr_location(a.arg))
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            leftmost_loc(c.location, expr_location(c.arg))
        }
        NodeTag::T_CoerceToDomain => {
            let c = node.as_coerce_to_domain().unwrap();
            leftmost_loc(c.location, expr_location(c.arg))
        }
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().location,
        NodeTag::T_CollateExpr => expr_location(node.as_collate_expr().unwrap().arg),
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().location,
        NodeTag::T_CaseTestExpr => -1,
        // C: the CASE/WHEN/COALESCE/GREATEST/LEAST keyword is always leftmost.
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().location,
        NodeTag::T_CaseWhen => node.as_case_when().unwrap().location,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().location,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().location,
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            leftmost_loc(b.location, expr_location_list(&b.args))
        }
        NodeTag::T_NullTest => {
            let n = node.as_null_test().unwrap();
            leftmost_loc(n.location, n.arg.map_or(-1, expr_location))
        }
        NodeTag::T_BooleanTest => {
            let b = node.as_boolean_test().unwrap();
            leftmost_loc(b.location, b.arg.map_or(-1, expr_location))
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            leftmost_loc(d.location, expr_location_list(&d.args))
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            leftmost_loc(d.location, expr_location_list(&d.args))
        }
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().location,
        // C: consider both function name and leftmost arg.
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            leftmost_loc(x.location, expr_location_list(&x.args))
        }
        NodeTag::T_TableFunc => node.as_table_func().unwrap().location,
        // C: XMLSERIALIZE keyword should always be the first thing.
        NodeTag::T_XmlSerialize => node.as_xml_serialize().unwrap().location,
        NodeTag::T_FieldStore => expr_location(node.as_field_store().unwrap().arg),
        NodeTag::T_RowCompareExpr => {
            expr_location_list(&node.as_row_compare_expr().unwrap().largs)
        }
        NodeTag::T_CollateClause => {
            let c = node.as_collate_clause().unwrap();
            leftmost_loc(c.arg.map_or(-1, expr_location), c.location)
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
        NodeTag::T_JsonFormat => node.as_json_format().unwrap().location,
        NodeTag::T_JsonValueExpr => {
            node.as_json_value_expr().unwrap().raw_expr.map_or(-1, expr_location)
        }
        NodeTag::T_JsonConstructorExpr => node.as_json_constructor_expr().unwrap().location,
        NodeTag::T_JsonIsPredicate => node.as_json_is_predicate().unwrap().location,
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            leftmost_loc(j.location, j.formatted_expr.map_or(-1, expr_location))
        }
        NodeTag::T_JsonBehavior => {
            node.as_json_behavior().unwrap().expr.map_or(-1, expr_location)
        }
        NodeTag::T_JsonKeyValue => {
            node.as_json_key_value().unwrap().key.map_or(-1, expr_location)
        }
        NodeTag::T_JsonObjectConstructor => node.as_json_object_constructor().unwrap().location,
        NodeTag::T_JsonArrayConstructor => node.as_json_array_constructor().unwrap().location,
        NodeTag::T_JsonArrayQueryConstructor => {
            node.as_json_array_query_constructor().unwrap().location
        }
        NodeTag::T_JsonAggConstructor => node.as_json_agg_constructor().unwrap().location,
        NodeTag::T_JsonObjectAgg => {
            node.as_json_object_agg().unwrap().constructor.map_or(-1, expr_location)
        }
        NodeTag::T_JsonArrayAgg => {
            node.as_json_array_agg().unwrap().constructor.map_or(-1, expr_location)
        }
        NodeTag::T_RangeVar => node.as_range_var().unwrap().location,
        NodeTag::T_RangeTableSample => node.as_range_table_sample().unwrap().location,
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

// applyRelabelType (nodeFuncs.c); the Const arm rebuilds instead of C's
// overwrite_ok in-place mutation, sharing constvalue (arena-shared, one
// bulk-freed mcx — C copyObject's deep copy exists for eager pfree).
pub fn apply_relabel_type<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    arg: Node<'mcx>,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: types_nodes::CoercionForm,
    rlocation: ParseLoc,
) -> types_error::PgResult<Node<'mcx>> {
    let mut arg = arg;
    while arg.node_tag() == NodeTag::T_RelabelType {
        arg = arg.as_relabel_type().unwrap().arg;
    }
    if let Some(con) = arg.as_const() {
        return Node::mk(
            mcx,
            types_nodes::Const {
                consttype: rtype,
                consttypmod: rtypmod,
                constcollid: rcollid,
                constlen: con.constlen,
                constvalue: con.constvalue,
                constisnull: con.constisnull,
                constbyval: con.constbyval,
                location: con.location,
            },
        );
    }
    if expr_type(arg) == rtype && expr_typmod(arg) == rtypmod && expr_collation(arg) == rcollid {
        return Ok(arg);
    }
    Node::mk(
        mcx,
        types_nodes::RelabelType {
            arg,
            resulttype: rtype,
            resulttypmod: rtypmod,
            resultcollid: rcollid,
            relabelformat: rformat,
            location: rlocation,
        },
    )
}

pub fn expr_input_collation(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Aggref => node.as_aggref().unwrap().inputcollid,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().inputcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().inputcollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().inputcollid,
        NodeTag::T_DistinctExpr => node.as_distinct_expr().unwrap().inputcollid,
        NodeTag::T_NullIfExpr => node.as_null_if_expr().unwrap().inputcollid,
        NodeTag::T_ScalarArrayOpExpr => node.as_scalar_array_op_expr().unwrap().inputcollid,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().inputcollid,
        _ => types_core::InvalidOid,
    }
}

pub fn relabel_to_typmod<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    expr: Node<'mcx>,
    typmod: i32,
) -> types_error::PgResult<Node<'mcx>> {
    apply_relabel_type(
        mcx,
        expr,
        expr_type(expr),
        typmod,
        expr_collation(expr),
        types_nodes::CoercionForm::COERCE_EXPLICIT_CAST,
        -1,
    )
}

// C set_opfuncid/set_sa_opfuncid memo-write into the node; sealed nodes are
// immutable here, so the resolved oid is returned (check_functions_in_node
// precedent).
pub fn set_opfuncid(o: &types_nodes::primnodes::OpExpr<'_>) -> types_error::PgResult<Oid> {
    if o.opfuncid == types_core::InvalidOid {
        lsyscache::operator::get_opcode(o.opno)
    } else {
        Ok(o.opfuncid)
    }
}

pub fn set_sa_opfuncid(
    o: &types_nodes::primnodes::ScalarArrayOpExpr<'_>,
) -> types_error::PgResult<Oid> {
    if o.opfuncid == types_core::InvalidOid {
        lsyscache::operator::get_opcode(o.opno)
    } else {
        Ok(o.opfuncid)
    }
}
