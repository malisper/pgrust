#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use parse_expr::{expr_collation, expr_location, expr_type};
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
    // Constructed only by the CollateExpr arm (loud until it lands).
    #[allow(dead_code)]
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
    if !query.windowClause.is_nil() {
        panic!(
            "assign_query_collations (parse_collate.c): WindowClause offset walk \
             unported — unit backend-parser-parse-collate"
        );
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
        NodeTag::T_RangeTblRef | NodeTag::T_SortGroupClause => return Ok(()),
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
        | NodeTag::T_CurrentOfExpr => {
            collation = expr_collation(node);
            strength = if OidIsValid(collation) {
                CollateStrength::Implicit
            } else {
                CollateStrength::None
            };
            location = expr_location(node);
        }
        // C's default arm over the closed set this lane can produce.
        // SubLink: children walked (T_Query arm supplies the EXPR sublink's
        // first-column collation); exprSetCollation on SubLink is a C no-op.
        tag @ (NodeTag::T_OpExpr
        | NodeTag::T_FuncExpr
        | NodeTag::T_RelabelType
        | NodeTag::T_CoerceViaIO
        | NodeTag::T_BoolExpr
        | NodeTag::T_CaseExpr
        | NodeTag::T_CoalesceExpr
        | NodeTag::T_MinMaxExpr
        | NodeTag::T_Aggref
        | NodeTag::T_SubLink) => {
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
                NodeTag::T_CoerceViaIO => {
                    assign_collations_walker(
                        node.as_coerce_via_io().unwrap().arg,
                        &mut loccontext,
                    )?;
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
                NodeTag::T_RelabelType => {
                    assign_collations_walker(
                        node.as_relabel_type().unwrap().arg,
                        &mut loccontext,
                    )?;
                }
                NodeTag::T_Aggref => {
                    let agg = node.as_aggref().unwrap();
                    for arg in &agg.aggdirectargs {
                        assign_collations_walker(arg, &mut loccontext)?;
                    }
                    for tle in &agg.args {
                        assign_collations_walker(tle, &mut loccontext)?;
                    }
                    if let Some(filter) = agg.aggfilter {
                        assign_collations_walker(filter, &mut loccontext)?;
                    }
                }
                NodeTag::T_SubLink => {
                    let sl = node.as_sub_link().unwrap();
                    if let Some(te) = sl.testexpr {
                        assign_collations_walker(te, &mut loccontext)?;
                    }
                    assign_collations_walker(sl.subselect, &mut loccontext)?;
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
                    // exprSetCollation(BoolExpr) is assert-only in C.
                    NodeTag::T_BoolExpr => debug_assert!(!OidIsValid(set_coll)),
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
                    // exprSetCollation(SubLink) is assert-only in C.
                    NodeTag::T_SubLink => {}
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
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_COLLATION_MISMATCH)
            .errmsg(format!(
                "collation mismatch between {kind} collations \"{}\" and \"{}\"",
                name(coll1),
                name(coll2)
            ))
            .errhint(
                "You can choose the collation by applying the COLLATE clause to one or both \
                 expressions."
                    .to_owned(),
            )
            .errposition(parser_errposition(ctx.pstate, errloc, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_collate.c", 0, funcname)),
    )
}
