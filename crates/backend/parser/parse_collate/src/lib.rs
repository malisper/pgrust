#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use parse_expr::{expr_collation, expr_location, expr_type, expr_typmod};
use parser_small1::{parser_errposition, ParseState};
use types_core::catalog::DEFAULT_COLLATION_OID;
use types_core::{InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_COLLATION_MISMATCH, ERROR};
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::FromExpr;
use types_nodes::{Node, NodeList, NodeTag};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CollateStrength {
    None = 0,
    Implicit = 1,
    Conflict = 2,
    Explicit = 3,
}

struct AssignCollationsCtx<'a, 'p, 'mcx> {
    mcx: Mcx<'mcx>,
    pstate: &'a ParseState<'p, 'mcx>,
    collation: Oid,
    strength: CollateStrength,
    location: ParseLoc,
    collation2: Oid,
    location2: ParseLoc,
}

impl<'a, 'p, 'mcx> AssignCollationsCtx<'a, 'p, 'mcx> {
    fn new(mcx: Mcx<'mcx>, pstate: &'a ParseState<'p, 'mcx>) -> Self {
        AssignCollationsCtx {
            mcx,
            pstate,
            collation: InvalidOid,
            strength: CollateStrength::None,
            location: -1,
            collation2: InvalidOid,
            location2: -1,
        }
    }
}

pub fn init_seams() {
    parse_collate_seams::assign_expr_collations::set(assign_expr_collations);
}

pub fn assign_query_collations<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    query: &Query<'mcx>,
) -> PgResult<()> {
    // Field set mirrors query_tree_walker under QTW_IGNORE_RANGE_TABLE |
    // QTW_IGNORE_CTE_SUBQUERIES (rtable/CTE subqueries already processed).
    walk_top_list(mcx, pstate, &query.targetList)?;
    walk_top_list(mcx, pstate, &query.withCheckOptions)?;
    walk_top_opt(mcx, pstate, query.onConflict)?;
    walk_top_list(mcx, pstate, &query.mergeActionList)?;
    walk_top_opt(mcx, pstate, query.mergeJoinCondition)?;
    walk_top_list(mcx, pstate, &query.returningList)?;
    if let Some(jt) = query.jointree {
        assign_from_expr_collations(mcx, pstate, jt)?;
    }
    if query.setOperations.is_some() {
        // C's walker skips SetOperationStmt (processed by
        // transformSetOperationStmt).
    }
    walk_top_opt(mcx, pstate, query.havingQual)?;
    walk_top_opt(mcx, pstate, query.limitOffset)?;
    walk_top_opt(mcx, pstate, query.limitCount)?;
    for wc_node in &query.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        walk_top_opt(mcx, pstate, wc.startOffset)?;
        walk_top_opt(mcx, pstate, wc.endOffset)?;
    }
    Ok(())
}

fn walk_top_list<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<()> {
    for node in list {
        assign_expr_collations(mcx, pstate, node)?;
    }
    Ok(())
}

fn walk_top_opt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<()> {
    match node {
        Some(n) if n.node_tag() == NodeTag::T_SetOperationStmt => Ok(()),
        Some(n) => assign_expr_collations(mcx, pstate, n),
        None => Ok(()),
    }
}

pub fn assign_list_collations<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    exprs: &NodeList<'mcx>,
) -> PgResult<()> {
    walk_top_list(mcx, pstate, exprs)
}


pub fn assign_expr_collations<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    expr: Node<'mcx>,
) -> PgResult<()> {
    let mut ctx = AssignCollationsCtx::new(mcx, pstate);
    assign_collations_walker(expr, &mut ctx)
}

pub fn select_common_collation<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    exprs: &NodeList<'mcx>,
    none_ok: bool,
) -> PgResult<Oid> {
    let mut ctx = AssignCollationsCtx::new(mcx, pstate);
    for node in exprs {
        assign_collations_walker(node, &mut ctx)?;
    }
    if ctx.strength == CollateStrength::Conflict {
        if none_ok {
            return Ok(InvalidOid);
        }
        return Err(collation_mismatch_error(
            &ctx,
            "implicit",
            ctx.collation,
            ctx.collation2,
            ctx.location2,
            "select_common_collation",
        ));
    }
    Ok(ctx.collation)
}

fn assign_from_expr_collations<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    jt: &FromExpr<'mcx>,
) -> PgResult<()> {
    let mut loccontext = AssignCollationsCtx::new(mcx, pstate);
    for item in &jt.fromlist {
        assign_collations_walker(item, &mut loccontext)?;
    }
    if let Some(quals) = jt.quals {
        assign_collations_walker(quals, &mut loccontext)?;
    }
    Ok(())
}

fn assign_collations_walker<'mcx>(
    node: Node<'mcx>,
    context: &mut AssignCollationsCtx<'_, '_, 'mcx>,
) -> PgResult<()> {
    let mut loccontext = AssignCollationsCtx::new(context.mcx, context.pstate);
    let collation;
    let strength;
    let location;

    match node.node_tag() {
        NodeTag::T_TargetEntry => {
            let te = node.as_target_entry().unwrap();
            assign_collations_walker(te.expr, &mut loccontext)?;
            collation = loccontext.collation;
            strength = loccontext.strength;
            location = loccontext.location;
            if strength == CollateStrength::Conflict && te.ressortgroupref != 0 {
                return Err(collation_mismatch_error(
                    &loccontext,
                    "implicit",
                    loccontext.collation,
                    loccontext.collation2,
                    loccontext.location2,
                    "assign_collations_walker",
                ));
            }
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            assign_from_expr_collations(context.mcx, context.pstate, f)?;
            return Ok(());
        }
        // C recurses through join nodes only to reach the ON expressions.
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            assign_collations_walker(j.larg, &mut loccontext)?;
            assign_collations_walker(j.rarg, &mut loccontext)?;
            if let Some(quals) = j.quals {
                assign_collations_walker(quals, &mut loccontext)?;
            }
            return Ok(());
        }
        // C recurses through ON CONFLICT nodes only to reach the contained
        // expressions; the nodes themselves carry no collation.
        NodeTag::T_OnConflictExpr => {
            let oc = node.as_on_conflict_expr().unwrap();
            for elem in &oc.arbiterElems {
                assign_collations_walker(elem, &mut loccontext)?;
            }
            if let Some(w) = oc.arbiterWhere {
                assign_collations_walker(w, &mut loccontext)?;
            }
            for tle in &oc.onConflictSet {
                assign_collations_walker(tle, &mut loccontext)?;
            }
            if let Some(w) = oc.onConflictWhere {
                assign_collations_walker(w, &mut loccontext)?;
            }
            for tle in &oc.exclRelTlist {
                assign_collations_walker(tle, &mut loccontext)?;
            }
            return Ok(());
        }
        NodeTag::T_InferenceElem => {
            if let Some(expr) = node.as_inference_elem().unwrap().expr {
                assign_collations_walker(expr, &mut loccontext)?;
            }
            return Ok(());
        }
        NodeTag::T_CollateExpr => {
            let c = node.as_collate_expr().unwrap();
            assign_collations_walker(c.arg, &mut loccontext)?;
            collation = c.collOid;
            debug_assert!(OidIsValid(collation));
            strength = CollateStrength::Explicit;
            location = c.location;
        }
        NodeTag::T_RangeTblRef | NodeTag::T_SortGroupClause => return Ok(()),
        // RowExpr subexpressions are independent (C assign_list_collations);
        // the RECORD result is never collatable, so no impact on the parent.
        NodeTag::T_RowExpr => {
            walk_top_list(context.mcx, context.pstate, &node.as_row_expr().unwrap().args)?;
            return Ok(());
        }
        // Each column pair gets its own common collation; no common collation
        // stores InvalidOid (may or may not error at runtime, per C).
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            let mut colls = types_nodes::list::OidList::nil();
            for (le, re) in rc.largs.iter().zip(rc.rargs.iter()) {
                let pair = types_nodes::NodeList::make2(context.mcx, le, re)?;
                let coll = select_common_collation(context.mcx, context.pstate, &pair, true)?;
                colls.lappend(context.mcx, coll)?;
            }
            // SAFETY: parse analysis exclusively owns the just-built tree; the
            // child borrows above have ended.
            unsafe {
                node.with_mut::<types_nodes::RowCompareExpr, _>(|e| e.inputcollids = colls)
                    .unwrap();
            }
            return Ok(());
        }
        // The field's declared collation was saved in the node; the composite
        // argument is never collatable.
        NodeTag::T_FieldSelect => {
            let fs = node.as_field_select().unwrap();
            assign_collations_walker(fs.arg, &mut loccontext)?;
            let rescoll = fs.resultcollid;
            if OidIsValid(rescoll) {
                collation = rescoll;
                strength = CollateStrength::Implicit;
                location = expr_location(node);
            } else {
                collation = InvalidOid;
                strength = CollateStrength::None;
                location = -1;
            }
        }
        // Non-default domain COLLATE overrides the child; DEFAULT bubbles the
        // child state up; the node ignores input collation.
        NodeTag::T_CoerceToDomain => {
            let c = node.as_coerce_to_domain().unwrap();
            assign_collations_walker(c.arg, &mut loccontext)?;
            let typcollation = lsyscache::get_typcollation(c.resulttype)?;
            if OidIsValid(typcollation) {
                if typcollation == DEFAULT_COLLATION_OID {
                    collation = loccontext.collation;
                    strength = loccontext.strength;
                    location = loccontext.location;
                } else {
                    collation = typcollation;
                    strength = CollateStrength::Implicit;
                    location = expr_location(node);
                }
            } else {
                collation = InvalidOid;
                strength = CollateStrength::None;
                location = -1;
            }
            let set_coll =
                if strength == CollateStrength::Conflict { InvalidOid } else { collation };
            // SAFETY: parse analysis exclusively owns the just-built tree; the
            // child borrows above have ended.
            unsafe {
                node.with_mut::<types_nodes::CoerceToDomain, _>(|cd| cd.resultcollid = set_coll)
                    .unwrap();
            }
        }
        NodeTag::T_MergeAction => {
            let a = node.as_merge_action().unwrap();
            if let Some(q) = a.qual {
                assign_collations_walker(q, &mut loccontext)?;
            }
            for tle in &a.targetList {
                assign_collations_walker(tle, &mut loccontext)?;
            }
            return Ok(());
        }
        NodeTag::T_Query => {
            let qtree = node.as_query().unwrap();
            let Some(first) = qtree.targetList.first() else {
                return Ok(());
            };
            let tent = first.as_target_entry().unwrap();
            if tent.resjunk {
                return Ok(());
            }
            collation = expr_collation(tent.expr);
            strength = CollateStrength::Implicit;
            location = expr_location(tent.expr);
        }
        NodeTag::T_List => {
            for elem in node.as_list().unwrap() {
                assign_collations_walker(elem, &mut loccontext)?;
            }
            collation = loccontext.collation;
            strength = loccontext.strength;
            location = loccontext.location;
        }
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SetToDefault
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CurrentOfExpr => {
            collation = expr_collation(node);
            strength = if OidIsValid(collation) {
                CollateStrength::Implicit
            } else {
                CollateStrength::None
            };
            location = expr_location(node);
        }
        // COLLATE sets an explicitly derived collation regardless of the
        // child state; still recurse to set up collation info below.
        NodeTag::T_CollateExpr => {
            let ce = node.as_collate_expr().unwrap();
            assign_collations_walker(ce.arg, &mut loccontext)?;
            collation = ce.collOid;
            debug_assert!(OidIsValid(collation));
            strength = CollateStrength::Explicit;
            location = ce.location;
        }
        // C's default arm over the closed set this lane can produce.
        // SubLink: children walked (T_Query arm supplies the EXPR sublink's
        // first-column collation); exprSetCollation on SubLink is a C no-op.
        tag @ (NodeTag::T_OpExpr
        | NodeTag::T_NullIfExpr
        | NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_ArrayExpr
        | NodeTag::T_FuncExpr
        | NodeTag::T_NamedArgExpr
        | NodeTag::T_RelabelType
        | NodeTag::T_CoerceViaIO
        | NodeTag::T_ArrayCoerceExpr
        | NodeTag::T_ConvertRowtypeExpr
        | NodeTag::T_BoolExpr
        | NodeTag::T_CaseExpr
        | NodeTag::T_CoalesceExpr
        | NodeTag::T_MinMaxExpr
        | NodeTag::T_Aggref
        | NodeTag::T_GroupingFunc
        | NodeTag::T_WindowFunc
        | NodeTag::T_NullTest
        | NodeTag::T_BooleanTest
        | NodeTag::T_DistinctExpr
        | NodeTag::T_ScalarArrayOpExpr
        | NodeTag::T_ArrayExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_JsonValueExpr
        | NodeTag::T_JsonConstructorExpr
        | NodeTag::T_JsonIsPredicate
        | NodeTag::T_JsonExpr
        | NodeTag::T_JsonBehavior
        | NodeTag::T_SubLink
        | NodeTag::T_XmlExpr
        | NodeTag::T_SubscriptingRef
        | NodeTag::T_FieldStore
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_NamedArgExpr) => {
            match tag {
                // C: never recurse into the CASE test expression — it was
                // collation-marked in transformCaseExpr and doesn't affect
                // the result; when-conditions are boolean, safe to recurse.
                NodeTag::T_CaseExpr => {
                    let c = node.as_case_expr().unwrap();
                    for w in &c.args {
                        let w = w.as_case_when().expect("CaseWhen");
                        if let Some(expr) = w.expr {
                            assign_collations_walker(expr, &mut loccontext)?;
                        }
                        if let Some(result) = w.result {
                            assign_collations_walker(result, &mut loccontext)?;
                        }
                    }
                    if let Some(defresult) = c.defresult {
                        assign_collations_walker(defresult, &mut loccontext)?;
                    }
                }
                NodeTag::T_CoalesceExpr => {
                    for arg in &node.as_coalesce_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_MinMaxExpr => {
                    for arg in &node.as_min_max_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_OpExpr => {
                    for arg in &node.as_op_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                // NullIfExpr is struct-equivalent to OpExpr (nodeFuncs.c).
                NodeTag::T_NullIfExpr => {
                    for arg in &node.as_null_if_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_ScalarArrayOpExpr => {
                    for arg in &node.as_scalar_array_op_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_ArrayExpr => {
                    for e in &node.as_array_expr().unwrap().elements {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                }
                NodeTag::T_CoerceViaIO => {
                    assign_collations_walker(
                        node.as_coerce_via_io().unwrap().arg,
                        &mut loccontext,
                    )?;
                }
                NodeTag::T_ArrayCoerceExpr => {
                    let a = node.as_array_coerce_expr().unwrap();
                    assign_collations_walker(a.arg, &mut loccontext)?;
                    if let Some(e) = a.elemexpr {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                }
                NodeTag::T_ConvertRowtypeExpr => {
                    assign_collations_walker(
                        node.as_convert_rowtype_expr().unwrap().arg,
                        &mut loccontext,
                    )?;
                }
                NodeTag::T_FieldStore => {
                    let f = node.as_field_store().unwrap();
                    assign_collations_walker(f.arg, &mut loccontext)?;
                    for v in &f.newvals {
                        assign_collations_walker(v, &mut loccontext)?;
                    }
                }
                NodeTag::T_BoolExpr => {
                    for arg in &node.as_bool_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_FuncExpr => {
                    for arg in &node.as_func_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_NamedArgExpr => {
                    assign_collations_walker(
                        node.as_named_arg_expr()
                            .unwrap()
                            .arg
                            .expect("NamedArgExpr has an arg"),
                        &mut loccontext,
                    )?;
                }
                NodeTag::T_RelabelType => {
                    assign_collations_walker(
                        node.as_relabel_type().unwrap().arg,
                        &mut loccontext,
                    )?;
                }
                // Resjunk ORDER BY args and FILTER are held at arm's length
                // (independent walks): they must not conflict with regular
                // args or affect the aggregate's collation.
                NodeTag::T_Aggref => {
                    let agg = node.as_aggref().unwrap();
                    match agg.aggkind {
                        types_nodes::primnodes::AGGKIND_ORDERED_SET => {
                            assign_ordered_set_collations(agg, &mut loccontext)?;
                        }
                        types_nodes::primnodes::AGGKIND_HYPOTHETICAL => {
                            assign_hypothetical_collations(agg, &mut loccontext)?;
                        }
                        _ => {
                            debug_assert!(agg.aggdirectargs.is_nil());
                            for tle_node in &agg.args {
                                let tle =
                                    tle_node.as_target_entry().expect("Aggref arg TargetEntry");
                                if tle.resjunk {
                                    assign_expr_collations(
                                        context.mcx,
                                        context.pstate,
                                        tle_node,
                                    )?;
                                } else {
                                    assign_collations_walker(tle_node, &mut loccontext)?;
                                }
                            }
                        }
                    }
                    if let Some(filter) = agg.aggfilter {
                        assign_expr_collations(context.mcx, context.pstate, filter)?;
                    }
                }
                NodeTag::T_GroupingFunc => {
                    for arg in &node.as_grouping_func().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_WindowFunc => {
                    let wf = node.as_window_func().unwrap();
                    for arg in &wf.args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                    if let Some(filter) = wf.aggfilter {
                        assign_expr_collations(context.mcx, context.pstate, filter)?;
                    }
                }
                NodeTag::T_SubLink => {
                    let sl = node.as_sub_link().unwrap();
                    if let Some(te) = sl.testexpr {
                        assign_collations_walker(te, &mut loccontext)?;
                    }
                    assign_collations_walker(sl.subselect, &mut loccontext)?;
                }
                NodeTag::T_NullTest => {
                    if let Some(arg) = node.as_null_test().unwrap().arg {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_BooleanTest => {
                    if let Some(arg) = node.as_boolean_test().unwrap().arg {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_DistinctExpr => {
                    for arg in &node.as_distinct_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_SubscriptingRef => {
                    let s = node.as_subscripting_ref().unwrap();
                    for e in &s.refupperindexpr {
                        if let Some(e) = e {
                            assign_collations_walker(e, &mut loccontext)?;
                        }
                    }
                    for e in &s.reflowerindexpr {
                        if let Some(e) = e {
                            assign_collations_walker(e, &mut loccontext)?;
                        }
                    }
                    if let Some(e) = s.refexpr {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                    if let Some(e) = s.refassgnexpr {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                }
                NodeTag::T_ScalarArrayOpExpr => {
                    for arg in &node.as_scalar_array_op_expr().unwrap().args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                NodeTag::T_ArrayExpr => {
                    for el in &node.as_array_expr().unwrap().elements {
                        assign_collations_walker(el, &mut loccontext)?;
                    }
                }
                NodeTag::T_SQLValueFunction => {}
                NodeTag::T_MergeSupportFunc => {}
                NodeTag::T_NamedArgExpr => {
                    assign_collations_walker(
                        node.as_named_arg_expr().unwrap().arg.expect("NamedArgExpr has an arg"),
                        &mut loccontext,
                    )?;
                }
                NodeTag::T_JsonValueExpr => {
                    let j = node.as_json_value_expr().unwrap();
                    if let Some(e) = j.raw_expr {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                    if let Some(e) = j.formatted_expr {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                }
                NodeTag::T_JsonConstructorExpr => {
                    let c = node.as_json_constructor_expr().unwrap();
                    for arg in &c.args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                    if let Some(f) = c.func {
                        assign_collations_walker(f, &mut loccontext)?;
                    }
                    if let Some(co) = c.coercion {
                        assign_collations_walker(co, &mut loccontext)?;
                    }
                }
                NodeTag::T_JsonIsPredicate => {
                    if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                }
                NodeTag::T_JsonExpr => {
                    let j = node.as_json_expr().unwrap();
                    for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                        .into_iter()
                        .flatten()
                    {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                    for v in &j.passing_values {
                        assign_collations_walker(v, &mut loccontext)?;
                    }
                }
                NodeTag::T_JsonBehavior => {
                    if let Some(e) = node.as_json_behavior().unwrap().expr {
                        assign_collations_walker(e, &mut loccontext)?;
                    }
                }
                NodeTag::T_XmlExpr => {
                    let x = node.as_xml_expr().unwrap();
                    for arg in &x.named_args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                    for arg in &x.args {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                }
                _ => unreachable!(),
            }

            let typcollation = lsyscache::get_typcollation(expr_type(node))?;
            if OidIsValid(typcollation) {
                if loccontext.strength > CollateStrength::None {
                    collation = loccontext.collation;
                    strength = loccontext.strength;
                    location = loccontext.location;
                } else {
                    collation = typcollation;
                    strength = CollateStrength::Implicit;
                    location = expr_location(node);
                }
            } else {
                collation = InvalidOid;
                strength = CollateStrength::None;
                location = -1;
            }

            let set_coll = if strength == CollateStrength::Conflict { InvalidOid } else { collation };
            let input_coll = if loccontext.strength == CollateStrength::Conflict {
                InvalidOid
            } else {
                loccontext.collation
            };
            // SAFETY: parse analysis exclusively owns the just-built tree; the
            // child borrows above have ended.
            unsafe {
                match tag {
                    NodeTag::T_OpExpr => node
                        .with_mut::<types_nodes::OpExpr, _>(|o| {
                            o.opcollid = set_coll;
                            o.inputcollid = input_coll;
                        })
                        .unwrap(),
                    // NullIfExpr is struct-equivalent to OpExpr (nodeFuncs.c).
                    NodeTag::T_NullIfExpr => node
                        .with_mut::<types_nodes::NullIfExpr, _>(|o| {
                            o.opcollid = set_coll;
                            o.inputcollid = input_coll;
                        })
                        .unwrap(),
                    // exprSetCollation(ScalarArrayOpExpr) is assert-only in C
                    // (boolean result); only the input collation is stored.
                    NodeTag::T_ScalarArrayOpExpr => {
                        debug_assert!(!OidIsValid(set_coll));
                        node.with_mut::<types_nodes::ScalarArrayOpExpr, _>(|s| {
                            s.inputcollid = input_coll;
                        })
                        .unwrap()
                    }
                    NodeTag::T_ArrayExpr => node
                        .with_mut::<types_nodes::ArrayExpr, _>(|a| a.array_collid = set_coll)
                        .unwrap(),
                    NodeTag::T_FuncExpr => node
                        .with_mut::<types_nodes::FuncExpr, _>(|f| {
                            f.funccollid = set_coll;
                            f.inputcollid = input_coll;
                        })
                        .unwrap(),
                    NodeTag::T_RelabelType => node
                        .with_mut::<types_nodes::RelabelType, _>(|r| r.resultcollid = set_coll)
                        .unwrap(),
                    NodeTag::T_CoerceViaIO => node
                        .with_mut::<types_nodes::CoerceViaIO, _>(|c| c.resultcollid = set_coll)
                        .unwrap(),
                    NodeTag::T_ArrayCoerceExpr => node
                        .with_mut::<types_nodes::ArrayCoerceExpr, _>(|a| {
                            a.resultcollid = set_coll
                        })
                        .unwrap(),
                    // exprSetCollation(ConvertRowtypeExpr) is assert-only in C.
                    NodeTag::T_ConvertRowtypeExpr => {
                        debug_assert!(!OidIsValid(set_coll))
                    }
                    // exprSetCollation(FieldStore) is assert-only in C
                    // (composite result).
                    NodeTag::T_FieldStore => {
                        debug_assert!(!OidIsValid(set_coll))
                    }
                    // exprSetCollation(BoolExpr/NullTest/GroupingFunc/BooleanTest)
                    // is assert-only in C.
                    NodeTag::T_BoolExpr
                    | NodeTag::T_NullTest
                    | NodeTag::T_GroupingFunc
                    | NodeTag::T_BooleanTest => {
                        debug_assert!(!OidIsValid(set_coll))
                    }
                    // exprSetCollation(NamedArgExpr) is assert-only in C: the
                    // collation lives on the wrapped arg.
                    NodeTag::T_NamedArgExpr => {
                        debug_assert_eq!(set_coll, expr_collation(node))
                    }
                    NodeTag::T_DistinctExpr => node
                        .with_mut::<types_nodes::DistinctExpr, _>(|d| {
                            d.opcollid = set_coll;
                            d.inputcollid = input_coll;
                        })
                        .unwrap(),
                    NodeTag::T_CaseExpr => node
                        .with_mut::<types_nodes::primnodes::CaseExpr, _>(|c| {
                            c.casecollid = set_coll
                        })
                        .unwrap(),
                    NodeTag::T_CoalesceExpr => node
                        .with_mut::<types_nodes::primnodes::CoalesceExpr, _>(|c| {
                            c.coalescecollid = set_coll
                        })
                        .unwrap(),
                    NodeTag::T_MinMaxExpr => node
                        .with_mut::<types_nodes::primnodes::MinMaxExpr, _>(|m| {
                            m.minmaxcollid = set_coll;
                            m.inputcollid = input_coll;
                        })
                        .unwrap(),
                    NodeTag::T_Aggref => node
                        .with_mut::<types_nodes::primnodes::Aggref, _>(|a| {
                            a.aggcollid = set_coll;
                            a.inputcollid = input_coll;
                        })
                        .unwrap(),
                    NodeTag::T_WindowFunc => node
                        .with_mut::<types_nodes::primnodes::WindowFunc, _>(|w| {
                            w.wincollid = set_coll;
                            w.inputcollid = input_coll;
                        })
                        .unwrap(),
                    // exprSetCollation(SubLink) is assert-only in C.
                    NodeTag::T_SubLink => {}
                    // exprSetCollation(NamedArgExpr) is assert-only in C
                    // (collation == exprCollation(arg)).
                    NodeTag::T_NamedArgExpr => {
                        debug_assert_eq!(set_coll, expr_collation(node));
                    }
                    NodeTag::T_SubscriptingRef => node
                        .with_mut::<types_nodes::SubscriptingRef, _>(|s| s.refcollid = set_coll)
                        .unwrap(),
                    // exprSetCollation(ScalarArrayOpExpr) is assert-only
                    // (boolean result); only inputcollid is written.
                    NodeTag::T_ScalarArrayOpExpr => {
                        debug_assert!(!OidIsValid(set_coll));
                        node.with_mut::<types_nodes::ScalarArrayOpExpr, _>(|s| {
                            s.inputcollid = input_coll;
                        })
                        .unwrap()
                    }
                    NodeTag::T_ArrayExpr => node
                        .with_mut::<types_nodes::ArrayExpr, _>(|a| a.array_collid = set_coll)
                        .unwrap(),
                    // exprSetCollation(XmlExpr) is assert-only: the text
                    // result (IS_XMLSERIALIZE) carries DEFAULT_COLLATION_OID,
                    // xml results none.
                    NodeTag::T_XmlExpr => {
                        debug_assert!(
                            if node.as_xml_expr().unwrap().op
                                == types_nodes::XmlExprOp::IS_XMLSERIALIZE
                            {
                                set_coll == types_core::catalog::DEFAULT_COLLATION_OID
                            } else {
                                !OidIsValid(set_coll)
                            }
                        );
                    }
                    NodeTag::T_SQLValueFunction => {
                        debug_assert!(
                            if node.as_sql_value_function().unwrap().r#type
                                == types_core::catalog::NAMEOID
                            {
                                set_coll == types_core::catalog::C_COLLATION_OID
                            } else {
                                !OidIsValid(set_coll)
                            }
                        );
                    }
                    // exprSetCollation Json arms (nodeFuncs.c:1251).
                    NodeTag::T_JsonValueExpr => expr_set_collation(
                        node.as_json_value_expr().unwrap().formatted_expr.expect("formatted"),
                        set_coll,
                    ),
                    NodeTag::T_JsonConstructorExpr => {
                        let c = node.as_json_constructor_expr().unwrap();
                        match c.coercion {
                            Some(co) => expr_set_collation(co, set_coll),
                            None => debug_assert!(!OidIsValid(set_coll)),
                        }
                    }
                    NodeTag::T_JsonIsPredicate => debug_assert!(!OidIsValid(set_coll)),
                    NodeTag::T_JsonExpr => node
                        .with_mut::<types_nodes::JsonExpr, _>(|j| j.collation = set_coll)
                        .unwrap(),
                    NodeTag::T_MergeSupportFunc => node
                        .with_mut::<types_nodes::primnodes::MergeSupportFunc, _>(|m| {
                            m.msfcollid = set_coll
                        })
                        .unwrap(),
                    // exprSetCollation(JsonBehavior) is assert-only: the
                    // behavior expr's collation was already assigned.
                    NodeTag::T_JsonBehavior => {
                        debug_assert!(node
                            .as_json_behavior()
                            .unwrap()
                            .expr
                            .is_none_or(|e| expr_collation(e) == set_coll));
                    }
                    _ => unreachable!(),
                }
            }
        }
        other => panic!(
            "assign_collations_walker (parse_collate.c): general-case arm for {other:?} \
             unported (needs exprSetCollation/exprSetInputCollation on sealed nodes) — \
             unit backend-parser-parse-collate"
        ),
    }

    merge_collation_state(
        collation,
        strength,
        location,
        loccontext.collation2,
        loccontext.location2,
        context,
    )
}

// exprSetCollation over the coercion shapes coerceJsonFuncExpr can emit.
// # Safety contract mirrors the surrounding with_mut uses: parse analysis
// exclusively owns the tree.
unsafe fn expr_set_collation(node: Node<'_>, coll: Oid) {
    // SAFETY: caller contract.
    unsafe {
        match node.node_tag() {
            NodeTag::T_FuncExpr => {
                node.with_mut::<types_nodes::FuncExpr, _>(|f| f.funccollid = coll).unwrap()
            }
            NodeTag::T_OpExpr => {
                node.with_mut::<types_nodes::OpExpr, _>(|o| o.opcollid = coll).unwrap()
            }
            NodeTag::T_RelabelType => node
                .with_mut::<types_nodes::RelabelType, _>(|r| r.resultcollid = coll)
                .unwrap(),
            NodeTag::T_CoerceViaIO => node
                .with_mut::<types_nodes::CoerceViaIO, _>(|c| c.resultcollid = coll)
                .unwrap(),
            NodeTag::T_ArrayCoerceExpr => node
                .with_mut::<types_nodes::ArrayCoerceExpr, _>(|a| a.resultcollid = coll)
                .unwrap(),
            NodeTag::T_CoerceToDomain => node
                .with_mut::<types_nodes::CoerceToDomain, _>(|c| c.resultcollid = coll)
                .unwrap(),
            NodeTag::T_Const => {
                node.with_mut::<types_nodes::Const, _>(|c| c.constcollid = coll).unwrap()
            }
            NodeTag::T_MergeSupportFunc => node
                .with_mut::<types_nodes::primnodes::MergeSupportFunc, _>(|m| m.msfcollid = coll)
                .unwrap(),
            other => panic!(
                "expr_set_collation (exprSetCollation, nodeFuncs.c): arm for {other:?} \
                 unported — sqljson-lane"
            ),
        }
    }
}

fn assign_ordered_set_collations<'mcx>(
    agg: &'mcx types_nodes::primnodes::Aggref<'mcx>,
    loccontext: &mut AssignCollationsCtx<'_, '_, 'mcx>,
) -> PgResult<()> {
    let merge_sort_collations = agg.args.len() == 1
        && lsyscache::function::get_func_variadictype(agg.aggfnoid)? == InvalidOid;
    for d in &agg.aggdirectargs {
        assign_collations_walker(d, loccontext)?;
    }
    for tle_node in &agg.args {
        if merge_sort_collations {
            assign_collations_walker(tle_node, loccontext)?;
        } else {
            assign_expr_collations(loccontext.mcx, loccontext.pstate, tle_node)?;
        }
    }
    Ok(())
}

fn assign_hypothetical_collations<'mcx>(
    agg: &'mcx types_nodes::primnodes::Aggref<'mcx>,
    loccontext: &mut AssignCollationsCtx<'_, '_, 'mcx>,
) -> PgResult<()> {
    let mcx = loccontext.mcx;
    let merge_sort_collations = agg.args.len() == 1
        && lsyscache::function::get_func_variadictype(agg.aggfnoid)? == InvalidOid;

    let extra_args = agg.aggdirectargs.len() - agg.args.len();
    for (i, h_arg) in agg.aggdirectargs.iter().enumerate() {
        if i < extra_args {
            assign_collations_walker(h_arg, loccontext)?;
            continue;
        }
        let s_tle_node = agg.args.nth(i - extra_args);
        let s_tle = s_tle_node.as_target_entry().expect("Aggref arg TargetEntry");

        let mut paircontext = AssignCollationsCtx::new(mcx, loccontext.pstate);
        assign_collations_walker(h_arg, &mut paircontext)?;
        assign_collations_walker(s_tle.expr, &mut paircontext)?;

        if paircontext.strength == CollateStrength::Conflict {
            return Err(collation_mismatch_error(
                &paircontext,
                "implicit",
                paircontext.collation,
                paircontext.collation2,
                paircontext.location2,
                "assign_hypothetical_collations",
            ));
        }

        if OidIsValid(paircontext.collation)
            && paircontext.collation != expr_collation(s_tle.expr)
        {
            let relabel = Node::mk(
                mcx,
                types_nodes::RelabelType {
                    arg: s_tle.expr,
                    resulttype: expr_type(s_tle.expr),
                    resulttypmod: expr_typmod(s_tle.expr),
                    resultcollid: paircontext.collation,
                    relabelformat: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                    location: -1,
                },
            )?;
            // SAFETY: parse analysis holds exclusive access to the tree it is
            // transforming (addTargetToSortList precedent).
            unsafe {
                s_tle_node
                    .with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.expr = relabel)
                    .unwrap();
            }
        }

        if merge_sort_collations {
            let (c, st, l, c2, l2) = (
                paircontext.collation,
                paircontext.strength,
                paircontext.location,
                paircontext.collation2,
                paircontext.location2,
            );
            merge_collation_state(c, st, l, c2, l2, loccontext)?;
        }
    }
    Ok(())
}

fn merge_collation_state(
    collation: Oid,
    strength: CollateStrength,
    location: ParseLoc,
    collation2: Oid,
    location2: ParseLoc,
    context: &mut AssignCollationsCtx<'_, '_, '_>,
) -> PgResult<()> {
    if strength > context.strength {
        context.collation = collation;
        context.strength = strength;
        context.location = location;
        if strength == CollateStrength::Conflict {
            context.collation2 = collation2;
            context.location2 = location2;
        }
    } else if strength == context.strength {
        match strength {
            CollateStrength::None | CollateStrength::Conflict => {}
            CollateStrength::Implicit => {
                if collation != context.collation {
                    if context.collation == DEFAULT_COLLATION_OID {
                        context.collation = collation;
                        context.strength = strength;
                        context.location = location;
                    } else if collation != DEFAULT_COLLATION_OID {
                        context.strength = CollateStrength::Conflict;
                        context.collation2 = collation;
                        context.location2 = location;
                    }
                }
            }
            CollateStrength::Explicit => {
                if collation != context.collation {
                    return Err(collation_mismatch_error(
                        context,
                        "explicit",
                        context.collation,
                        collation,
                        location,
                        "merge_collation_state",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[track_caller]
#[cold]
fn collation_mismatch_error(
    ctx: &AssignCollationsCtx<'_, '_, '_>,
    kind: &str,
    coll1: Oid,
    coll2: Oid,
    errloc: ParseLoc,
    funcname: &'static str,
) -> Box<PgError> {
    let name = |c: Oid| -> String {
        lsyscache::misc::get_collation_name(ctx.mcx, c)
            .ok()
            .flatten()
            .map(|s| s.as_str().to_owned())
            .unwrap_or_else(|| format!("{c}"))
    };
    let encoding = mbutils::GetDatabaseEncoding();
    let mut report = elog::ereport(ERROR).errcode(ERRCODE_COLLATION_MISMATCH).errmsg(format!(
        "collation mismatch between {kind} collations \"{}\" and \"{}\"",
        name(coll1),
        name(coll2)
    ));
    // C attaches the hint only on the implicit variant.
    if kind == "implicit" {
        report = report.errhint(
            "You can choose the collation by applying the COLLATE clause to one or both \
             expressions."
                .to_owned(),
        );
    }
    Box::new(
        report
            .errposition(parser_errposition(ctx.pstate, errloc, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, funcname)),
    )
}
