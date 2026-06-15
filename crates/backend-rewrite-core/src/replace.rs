//! `replace_rte_variables` (+mutator), `map_variable_attnos`,
//! `ReplaceVarsFromTargetList` (+callback), `ReplaceVarFromTargetList`
//! (rewriteManip.c:1411-1979).
//!
//! These are the "replace Var with a substitute expression" mutators. The C
//! mutators return a freshly built node; the repo's mutator model owns the tree
//! in place (`&mut Node`), so a matching Var is overwritten with the substitute
//! by assigning through `*node`. The replacement subtrees these build (a
//! whole-row `RowExpr` expansion, a NULL-`Const` domain coercion, a `Var`/
//! `ConvertRowtypeExpr`) are all values of the *lifetime-free* [`Expr`] tree, so
//! the substitute is a plain `Expr` that can be written into a `Node` of any
//! lifetime — matching the central `*_mut` walker whose closure receives a node
//! at a fresh (higher-ranked) lifetime. Caller-side `'mcx` inputs
//! (`target_rte`/`targetlist`/`mcx`) live in the mutator's context struct.

#![allow(non_snake_case)]

extern crate alloc;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use backend_nodes_core::node_walker::{expression_tree_walker_mut, query_tree_mutator};
use backend_parser_coerce::coerce_null_to_domain;
use backend_parser_relation::{expandRTE, get_tle_by_resno};
use backend_utils_cache_lsyscache::type_::get_typlenbyval as lsyscache_get_typlenbyval;
use backend_utils_error::ereport;
use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_nodes::nodes::{Node, NodePtr};
use types_nodes::parsenodes::RangeTblEntry;
use types_nodes::primnodes::{
    CoercionForm, ConvertRowtypeExpr, Expr, ReturningExpr, RowExpr, Var, VarReturningType,
};

use crate::walkers::{checkExprHasSubLink, contains_multiexpr_param};

const RECORDOID: Oid = 2249;
const INVALID_OID: Oid = 0;
const INVALID_ATTR_NUMBER: i16 = 0;

#[inline]
fn oid_is_valid(o: Oid) -> bool {
    o != INVALID_OID
}

/// `elog(ERROR, ...)` shorthand.
fn elog_error(msg: impl Into<String>) -> PgError {
    ereport(ERROR).errmsg_internal(msg.into()).into_error()
}

/// `ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(...))`.
fn feature_not_supported(msg: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(String::from(msg))
        .into_error()
}

// ===========================================================================
// replace_rte_variables (rewriteManip.c:1434)
// ===========================================================================

/// The C `replace_rte_variables_context`. `inserted_sublink` is the only mutable
/// state shared with the callback; the callback itself is threaded separately
/// (a self-referential `&mut dyn FnMut` cannot be stored inside the context).
pub struct ReplaceRteVariablesContext {
    pub target_varno: i32,
    pub sublevels_up: i32,
    pub inserted_sublink: bool,
}

/// `replace_rte_variables_callback` — given a matching `Var` (and the live
/// context), produce its replacement expression. The result is a lifetime-free
/// [`Expr`] (every substitute rewriteManip.c builds is an `Expr`-tree value).
pub type ReplaceRteVariablesCallbackDyn =
    dyn FnMut(&Var, &mut ReplaceRteVariablesContext) -> PgResult<Expr>;

fn replace_rte_variables_mutator(
    node: &mut Node,
    context: &mut ReplaceRteVariablesContext,
    callback: &mut (dyn FnMut(&Var, &mut ReplaceRteVariablesContext) -> PgResult<Expr> + '_),
) -> PgResult<bool> {
    match node {
        Node::Expr(Expr::Var(_)) => {
            let (is_match, var_clone) = {
                if let Node::Expr(Expr::Var(var)) = node {
                    (
                        var.varno == context.target_varno
                            && var.varlevelsup as i32 == context.sublevels_up,
                        var.clone(),
                    )
                } else {
                    unreachable!()
                }
            };
            if is_match {
                // Found a matching variable, make the substitution.
                let newexpr = callback(&var_clone, context)?;
                // Detect if we are adding a sublink to query.
                if !context.inserted_sublink {
                    context.inserted_sublink =
                        checkExprHasSubLink(&Node::Expr(newexpr.clone()));
                }
                *node = Node::Expr(newexpr);
                return Ok(false);
            }
            Ok(false)
        }
        Node::CurrentOfExpr(cexpr) => {
            if cexpr.cvarno as i32 == context.target_varno && context.sublevels_up == 0 {
                return Err(feature_not_supported(
                    "WHERE CURRENT OF on a view is not implemented",
                ));
            }
            Ok(false)
        }
        Node::Query(q) => {
            context.sublevels_up += 1;
            let save_inserted_sublink = context.inserted_sublink;
            context.inserted_sublink = q.hasSubLinks;
            let mut err: Option<PgError> = None;
            query_tree_mutator(
                q,
                &mut |n| {
                    if err.is_some() {
                        return true;
                    }
                    match replace_rte_variables_mutator(n, context, &mut *callback) {
                        Ok(abort) => abort,
                        Err(e) => {
                            err = Some(e);
                            true
                        }
                    }
                },
                0,
            );
            if let Some(e) = err {
                return Err(e);
            }
            q.hasSubLinks |= context.inserted_sublink;
            context.inserted_sublink = save_inserted_sublink;
            context.sublevels_up -= 1;
            Ok(false)
        }
        _ => {
            let mut err: Option<PgError> = None;
            let aborted = expression_tree_walker_mut(node, &mut |n| {
                if err.is_some() {
                    return true;
                }
                match replace_rte_variables_mutator(n, context, &mut *callback) {
                    Ok(abort) => abort,
                    Err(e) => {
                        err = Some(e);
                        true
                    }
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
            Ok(aborted)
        }
    }
}

/// `replace_rte_variables(node, target_varno, sublevels_up, callback,
/// outer_hasSubLinks)` (rewriteManip.c:1434).
///
/// `outer_has_sublinks` is the C `bool *outer_hasSubLinks` (pass `None` for a
/// bare non-Query expression).
pub fn replace_rte_variables(
    node: &mut Node,
    target_varno: i32,
    sublevels_up: i32,
    callback: &mut (dyn FnMut(&Var, &mut ReplaceRteVariablesContext) -> PgResult<Expr> + '_),
    outer_has_sublinks: &mut Option<bool>,
) -> PgResult<()> {
    let mut context = ReplaceRteVariablesContext {
        target_varno,
        sublevels_up,
        inserted_sublink: false,
    };

    if let Node::Query(q) = node {
        context.inserted_sublink = q.hasSubLinks;
    } else if let Some(flag) = outer_has_sublinks {
        context.inserted_sublink = *flag;
    } else {
        context.inserted_sublink = false;
    }

    match &mut *node {
        Node::Query(q) => {
            let mut err: Option<PgError> = None;
            query_tree_mutator(
                q,
                &mut |n| {
                    if err.is_some() {
                        return true;
                    }
                    match replace_rte_variables_mutator(n, &mut context, &mut *callback) {
                        Ok(abort) => abort,
                        Err(e) => {
                            err = Some(e);
                            true
                        }
                    }
                },
                0,
            );
            if let Some(e) = err {
                return Err(e);
            }
        }
        other => {
            replace_rte_variables_mutator(other, &mut context, &mut *callback)?;
        }
    }

    if context.inserted_sublink {
        if let Node::Query(q) = node {
            q.hasSubLinks = true;
        } else if let Some(flag) = outer_has_sublinks {
            *flag = true;
        } else {
            return Err(elog_error(
                "replace_rte_variables inserted a SubLink, but has noplace to record it",
            ));
        }
    }

    Ok(())
}

// ===========================================================================
// map_variable_attnos (rewriteManip.c:1546)
// ===========================================================================

struct MapAttnosCtx<'a> {
    target_varno: i32,
    sublevels_up: i32,
    attno_map: &'a [i16],
    to_rowtype: Oid,
    found_whole_row: &'a mut bool,
}

fn map_variable_attnos_mutator(node: &mut Node, ctx: &mut MapAttnosCtx<'_>) -> PgResult<bool> {
    match node {
        Node::Expr(Expr::Var(_)) => {
            let matched = if let Node::Expr(Expr::Var(var)) = node {
                var.varno == ctx.target_varno && var.varlevelsup as i32 == ctx.sublevels_up
            } else {
                false
            };
            if matched {
                let var = if let Node::Expr(Expr::Var(var)) = node {
                    var
                } else {
                    unreachable!()
                };
                let attno = var.varattno;
                if attno > 0 {
                    // user-defined column, replace attno
                    if attno as usize > ctx.attno_map.len()
                        || ctx.attno_map[(attno - 1) as usize] == 0
                    {
                        return Err(elog_error(format!(
                            "unexpected varattno {attno} in expression to be mapped"
                        )));
                    }
                    var.varattno = ctx.attno_map[(attno - 1) as usize];
                    // If the syntactic referent is same RTE, fix it too.
                    if var.varnosyn as i32 == ctx.target_varno {
                        var.varattnosyn = var.varattno;
                    }
                } else if attno == 0 {
                    // whole-row variable, warn caller
                    *ctx.found_whole_row = true;
                    // If the caller expects us to convert the Var, do so.
                    if oid_is_valid(ctx.to_rowtype) && ctx.to_rowtype != var.vartype {
                        // This certainly won't work for a RECORD variable.
                        debug_assert!(var.vartype != RECORDOID);
                        let orig_vartype = var.vartype;
                        // Var itself is changed to the requested type.
                        var.vartype = ctx.to_rowtype;
                        let newvar = var.clone();
                        // Add a conversion node on top to convert back to the
                        // original type expected by the expression.
                        let r = ConvertRowtypeExpr {
                            arg: Some(Box::new(Expr::Var(newvar))),
                            resulttype: orig_vartype,
                            convertformat: CoercionForm::COERCE_IMPLICIT_CAST,
                            location: -1,
                        };
                        *node = Node::Expr(Expr::ConvertRowtypeExpr(r));
                        return Ok(false);
                    }
                }
            }
            Ok(false)
        }
        Node::Expr(Expr::ConvertRowtypeExpr(_)) => {
            // Simplify var::parenttype::grandparenttype into var::grandparenttype
            // when coercing a whole-row Var we need to convert.
            let do_simplify = if let Node::Expr(Expr::ConvertRowtypeExpr(r)) = node {
                match r.arg.as_deref() {
                    Some(Expr::Var(var)) => {
                        var.varno == ctx.target_varno
                            && var.varlevelsup as i32 == ctx.sublevels_up
                            && var.varattno == 0
                            && oid_is_valid(ctx.to_rowtype)
                            && ctx.to_rowtype != var.vartype
                    }
                    _ => false,
                }
            } else {
                false
            };
            if do_simplify {
                *ctx.found_whole_row = true;
                if let Node::Expr(Expr::ConvertRowtypeExpr(r)) = node {
                    if let Some(Expr::Var(var)) = r.arg.as_deref_mut() {
                        // This certainly won't work for a RECORD variable.
                        debug_assert!(var.vartype != RECORDOID);
                        // Var itself is changed to the requested type.
                        var.vartype = ctx.to_rowtype;
                    }
                }
                return Ok(false);
            }
            // otherwise process the expression normally
            recurse_map_attnos(node, ctx)
        }
        Node::Query(q) => {
            ctx.sublevels_up += 1;
            let mut err: Option<PgError> = None;
            let r = query_tree_mutator(
                q,
                &mut |n| {
                    if err.is_some() {
                        return true;
                    }
                    match map_variable_attnos_mutator(n, ctx) {
                        Ok(abort) => abort,
                        Err(e) => {
                            err = Some(e);
                            true
                        }
                    }
                },
                0,
            );
            ctx.sublevels_up -= 1;
            if let Some(e) = err {
                return Err(e);
            }
            Ok(r)
        }
        _ => recurse_map_attnos(node, ctx),
    }
}

fn recurse_map_attnos(node: &mut Node, ctx: &mut MapAttnosCtx<'_>) -> PgResult<bool> {
    let mut err: Option<PgError> = None;
    let aborted = expression_tree_walker_mut(node, &mut |n| {
        if err.is_some() {
            return true;
        }
        match map_variable_attnos_mutator(n, ctx) {
            Ok(abort) => abort,
            Err(e) => {
                err = Some(e);
                true
            }
        }
    });
    if let Some(e) = err {
        return Err(e);
    }
    Ok(aborted)
}

/// `map_variable_attnos(node, target_varno, sublevels_up, attno_map, to_rowtype,
/// found_whole_row)` (rewriteManip.c:1701). `attno_map` is the `AttrMap.attnums`
/// slice (varattno `n` is replaced by `attno_map[n-1]`).
pub fn map_variable_attnos(
    node: &mut Node,
    target_varno: i32,
    sublevels_up: i32,
    attno_map: &[i16],
    to_rowtype: Oid,
    found_whole_row: &mut bool,
) -> PgResult<()> {
    *found_whole_row = false;
    let mut ctx = MapAttnosCtx {
        target_varno,
        sublevels_up,
        attno_map,
        to_rowtype,
        found_whole_row,
    };
    match &mut *node {
        Node::Query(q) => {
            let mut err: Option<PgError> = None;
            query_tree_mutator(
                q,
                &mut |n| {
                    if err.is_some() {
                        return true;
                    }
                    match map_variable_attnos_mutator(n, &mut ctx) {
                        Ok(abort) => abort,
                        Err(e) => {
                            err = Some(e);
                            true
                        }
                    }
                },
                0,
            );
            if let Some(e) = err {
                return Err(e);
            }
        }
        other => {
            map_variable_attnos_mutator(other, &mut ctx)?;
        }
    }
    Ok(())
}

// ===========================================================================
// ReplaceVarsFromTargetList (rewriteManip.c:1728)
// ===========================================================================

/// `ReplaceVarsNoMatchOption` (rewriteManip.h) — the no-match action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplaceVarsNoMatchOption {
    /// `REPLACEVARS_REPORT_ERROR` — throw an error.
    ReportError,
    /// `REPLACEVARS_CHANGE_VARNO` — change the Var's varno to `nomatch_varno`.
    ChangeVarno,
    /// `REPLACEVARS_SUBSTITUTE_NULL` — replace with a NULL Const of same type.
    SubstituteNull,
}

/// `ReplaceVarFromTargetList(var, target_rte, targetlist, result_relation,
/// nomatch_option, nomatch_varno)` (rewriteManip.c:1791). Returns the
/// replacement as a lifetime-free [`Expr`].
#[allow(clippy::too_many_arguments)]
pub fn ReplaceVarFromTargetList<'mcx>(
    var: &Var,
    target_rte: &RangeTblEntry<'mcx>,
    targetlist: &[types_nodes::primnodes::TargetEntry<'mcx>],
    result_relation: i32,
    nomatch_option: ReplaceVarsNoMatchOption,
    nomatch_varno: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<Expr> {
    if var.varattno == INVALID_ATTR_NUMBER {
        // Must expand whole-tuple reference into RowExpr.
        let mut colnames: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        let mut fields: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        expandRTE(
            mcx,
            target_rte,
            var.varno,
            0, // not varlevelsup
            var.varreturningtype,
            var.location,
            var.vartype != RECORDOID,
            Some(&mut colnames),
            Some(&mut fields),
        )?;

        let mut rowexpr = RowExpr {
            args: Vec::new(),
            row_typeid: var.vartype,
            row_format: CoercionForm::COERCE_IMPLICIT_CAST,
            colnames: if var.vartype == RECORDOID {
                colnames
                    .iter()
                    .map(|n| match &**n {
                        Node::String(s) => String::from(s.sval.as_str()),
                        _ => String::new(),
                    })
                    .collect()
            } else {
                Vec::new()
            },
            location: var.location,
        };

        // Adjust the generated per-field Vars...
        for field_node in fields.iter() {
            let field_expr: Expr = match &**field_node {
                Node::Expr(Expr::Var(field_var)) => ReplaceVarFromTargetList(
                    field_var,
                    target_rte,
                    targetlist,
                    result_relation,
                    nomatch_option,
                    nomatch_varno,
                    mcx,
                )?,
                Node::Expr(e) => e.clone(),
                _ => continue,
            };
            rowexpr.args.push(field_expr);
        }

        // Wrap it in a ReturningExpr, if needed.
        if var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT {
            let rexpr = ReturningExpr {
                retlevelsup: 0,
                retold: var.varreturningtype == VarReturningType::VAR_RETURNING_OLD,
                retexpr: Some(Box::new(Expr::RowExpr(rowexpr))),
            };
            return Ok(Expr::ReturningExpr(rexpr));
        }

        return Ok(Expr::RowExpr(rowexpr));
    }

    // Normal case referencing one targetlist element.
    let tle = get_tle_by_resno(targetlist, var.varattno);

    match tle {
        None => no_match(var, nomatch_option, nomatch_varno, mcx),
        Some(tle) if tle.resjunk => no_match(var, nomatch_option, nomatch_varno, mcx),
        Some(tle) => {
            // Make a copy of the tlist item to return.
            let mut newnode: Expr = tle.expr.as_deref().expect("tle->expr set").clone();

            // Check for a PARAM_MULTIEXPR Param and throw error if so.
            if contains_multiexpr_param(&Node::Expr(newnode.clone())) {
                return Err(feature_not_supported(
                    "NEW variables in ON UPDATE rules cannot reference columns that are part of a multiple assignment in the subject UPDATE command",
                ));
            }

            // Handle any OLD/NEW RETURNING list Vars.
            if var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT {
                if result_relation == 0 {
                    return Err(elog_error(
                        "variable returning old/new found outside RETURNING list",
                    ));
                }
                let mut wrapped = Node::Expr(newnode);
                crate::increment::SetVarReturningType(
                    &mut wrapped,
                    result_relation,
                    0,
                    var.varreturningtype,
                );
                newnode = node_into_expr(wrapped)?;

                // Wrap in a ReturningExpr, if needed.
                let is_plain_result_var = match &newnode {
                    Expr::Var(v) => v.varno == result_relation && v.varlevelsup == 0,
                    _ => false,
                };
                if !is_plain_result_var {
                    let rexpr = ReturningExpr {
                        retlevelsup: 0,
                        retold: var.varreturningtype == VarReturningType::VAR_RETURNING_OLD,
                        retexpr: Some(Box::new(newnode)),
                    };
                    newnode = Expr::ReturningExpr(rexpr);
                }
            }

            Ok(newnode)
        }
    }
}

fn no_match<'mcx>(
    var: &Var,
    nomatch_option: ReplaceVarsNoMatchOption,
    nomatch_varno: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<Expr> {
    match nomatch_option {
        ReplaceVarsNoMatchOption::ReportError => Err(elog_error(format!(
            "could not find replacement targetlist entry for attno {}",
            var.varattno
        ))),
        ReplaceVarsNoMatchOption::ChangeVarno => {
            let mut newvar = var.clone();
            newvar.varno = nomatch_varno;
            newvar.varlevelsup = 0;
            // we leave the syntactic referent alone
            Ok(Expr::Var(newvar))
        }
        ReplaceVarsNoMatchOption::SubstituteNull => {
            // If Var is of domain type, add a CoerceToDomain node.
            let (vartyplen, vartypbyval) = lsyscache_get_typlenbyval(var.vartype)?;
            let e = coerce_null_to_domain(
                mcx,
                var.vartype,
                var.vartypmod,
                var.varcollid,
                vartyplen as i32,
                vartypbyval,
            )?;
            Ok(e)
        }
    }
}

fn node_into_expr(node: Node) -> PgResult<Expr> {
    match node {
        Node::Expr(e) => Ok(e),
        _ => Err(elog_error(
            "ReplaceVarFromTargetList: expected an expression node",
        )),
    }
}

/// `ReplaceVarsFromTargetList(node, target_varno, sublevels_up, target_rte,
/// targetlist, result_relation, nomatch_option, nomatch_varno,
/// outer_hasSubLinks)` (rewriteManip.c:1957).
#[allow(clippy::too_many_arguments)]
pub fn ReplaceVarsFromTargetList<'mcx>(
    node: &mut Node,
    target_varno: i32,
    sublevels_up: i32,
    target_rte: &RangeTblEntry<'mcx>,
    targetlist: &[types_nodes::primnodes::TargetEntry<'mcx>],
    result_relation: i32,
    nomatch_option: ReplaceVarsNoMatchOption,
    nomatch_varno: i32,
    outer_has_sublinks: &mut Option<bool>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // ReplaceVarsFromTargetList_callback: replace each matching Var, then adjust
    // varlevelsup if the replaced Var was within a subquery.
    let mut callback = move |var: &Var, _context: &mut ReplaceRteVariablesContext| -> PgResult<Expr> {
        let newexpr = ReplaceVarFromTargetList(
            var,
            target_rte,
            targetlist,
            result_relation,
            nomatch_option,
            nomatch_varno,
            mcx,
        )?;
        if var.varlevelsup > 0 {
            let mut wrapped = Node::Expr(newexpr);
            crate::increment::IncrementVarSublevelsUp(&mut wrapped, var.varlevelsup as i32, 0)?;
            node_into_expr(wrapped)
        } else {
            Ok(newexpr)
        }
    };

    replace_rte_variables(
        node,
        target_varno,
        sublevels_up,
        &mut callback,
        outer_has_sublinks,
    )
}
