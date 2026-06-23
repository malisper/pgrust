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

use nodes_core::node_walker::{expression_tree_walker_mut, query_tree_mutator};
use coerce::coerce_null_to_domain;
use parser_relation::{expandRTE, get_tle_by_resno};
use lsyscache::type_::get_typlenbyval as lsyscache_get_typlenbyval;
use utils_error::ereport;
use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use nodes::nodes::{ntag, Node, NodePtr};
use nodes::parsenodes::RangeTblEntry;
use nodes::primnodes::{
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
/// context), produce its replacement expression. The substitute is an `Expr`-tree
/// value built into the rewrite arena `'mcx` (every substitute rewriteManip.c
/// builds is an `Expr`-tree value).
pub type ReplaceRteVariablesCallbackDyn<'mcx> =
    dyn FnMut(&Var, &mut ReplaceRteVariablesContext) -> PgResult<Expr<'mcx>>;

fn replace_rte_variables_mutator<'mcx>(
    node: &mut Node<'mcx>,
    context: &mut ReplaceRteVariablesContext,
    callback: &mut (dyn FnMut(&Var, &mut ReplaceRteVariablesContext) -> PgResult<Expr<'mcx>> + '_),
    mcx: Mcx<'mcx>,
) -> PgResult<bool> {
    match node.node_tag() {
        ntag::T_Var => {
            let (is_match, var_clone) = {
                let var = node.as_var().unwrap();
                (
                    var.varno == context.target_varno
                        && var.varlevelsup as i32 == context.sublevels_up,
                    var.clone(),
                )
            };
            if is_match {
                // Found a matching variable, make the substitution.
                let newexpr = callback(&var_clone, context)?;
                // Wrap the replacement into a node once (the C walker inspects
                // `newnode` in-place); inspect via reference, then store it.
                // A plain `.clone()` here would panic on a SubLink replacement
                // (C just re-uses the same pointer, never copies).
                let newnode = Node::mk_expr(mcx, newexpr)?;
                // Detect if we are adding a sublink to query.
                if !context.inserted_sublink {
                    context.inserted_sublink = checkExprHasSubLink(&newnode);
                }
                *node = newnode;
                return Ok(false);
            }
            Ok(false)
        }
        ntag::T_CurrentOfExpr => {
            let cexpr = node.as_currentofexpr().unwrap();
            if cexpr.cvarno as i32 == context.target_varno && context.sublevels_up == 0 {
                return Err(feature_not_supported(
                    "WHERE CURRENT OF on a view is not implemented",
                ));
            }
            Ok(false)
        }
        ntag::T_Query => {
            let q = node.as_query_mut().unwrap();
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
                    match replace_rte_variables_mutator(n, context, &mut *callback, mcx) {
                        Ok(abort) => abort,
                        Err(e) => {
                            err = Some(e);
                            true
                        }
                    }
                },
                0,
                mcx,
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
            let aborted = expression_tree_walker_mut(
                node,
                &mut |n| {
                    if err.is_some() {
                        return true;
                    }
                    match replace_rte_variables_mutator(n, context, &mut *callback, mcx) {
                        Ok(abort) => abort,
                        Err(e) => {
                            err = Some(e);
                            true
                        }
                    }
                },
                mcx,
            );
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
pub fn replace_rte_variables<'mcx>(
    node: &mut Node<'mcx>,
    target_varno: i32,
    sublevels_up: i32,
    callback: &mut (dyn FnMut(&Var, &mut ReplaceRteVariablesContext) -> PgResult<Expr<'mcx>> + '_),
    outer_has_sublinks: &mut Option<bool>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let mut context = ReplaceRteVariablesContext {
        target_varno,
        sublevels_up,
        inserted_sublink: false,
    };

    if let Some(q) = node.as_query() {
        context.inserted_sublink = q.hasSubLinks;
    } else if let Some(flag) = outer_has_sublinks {
        context.inserted_sublink = *flag;
    } else {
        context.inserted_sublink = false;
    }

    if let Some(q) = node.as_query_mut() {
        let mut err: Option<PgError> = None;
        query_tree_mutator(
            q,
            &mut |n| {
                if err.is_some() {
                    return true;
                }
                match replace_rte_variables_mutator(n, &mut context, &mut *callback, mcx) {
                    Ok(abort) => abort,
                    Err(e) => {
                        err = Some(e);
                        true
                    }
                }
            },
            0,
            mcx,
        );
        if let Some(e) = err {
            return Err(e);
        }
    } else {
        replace_rte_variables_mutator(node, &mut context, &mut *callback, mcx)?;
    }

    if context.inserted_sublink {
        if let Some(q) = node.as_query_mut() {
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

struct MapAttnosCtx<'a, 'mcx> {
    target_varno: i32,
    sublevels_up: i32,
    attno_map: &'a [i16],
    to_rowtype: Oid,
    found_whole_row: &'a mut bool,
    mcx: Mcx<'mcx>,
}

fn map_variable_attnos_mutator<'mcx>(node: &mut Node<'mcx>, ctx: &mut MapAttnosCtx<'_, 'mcx>) -> PgResult<bool> {
    match node.node_tag() {
        ntag::T_Var => {
            let matched = {
                let var = node.as_var().unwrap();
                var.varno == ctx.target_varno && var.varlevelsup as i32 == ctx.sublevels_up
            };
            if matched {
                let var = node.as_var_mut().unwrap();
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
                        *node = Node::mk_expr(ctx.mcx, Expr::ConvertRowtypeExpr(r))?;
                        return Ok(false);
                    }
                }
            }
            Ok(false)
        }
        ntag::T_ConvertRowtypeExpr => {
            // Simplify var::parenttype::grandparenttype into var::grandparenttype
            // when coercing a whole-row Var we need to convert.
            let do_simplify = {
                let r = node.as_convertrowtypeexpr().unwrap();
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
            };
            if do_simplify {
                *ctx.found_whole_row = true;
                let r = node.as_convertrowtypeexpr_mut().unwrap();
                if let Some(Expr::Var(var)) = r.arg.as_deref_mut() {
                    // This certainly won't work for a RECORD variable.
                    debug_assert!(var.vartype != RECORDOID);
                    // Var itself is changed to the requested type.
                    var.vartype = ctx.to_rowtype;
                }
                return Ok(false);
            }
            // otherwise process the expression normally
            recurse_map_attnos(node, ctx)
        }
        ntag::T_Query => {
            let mcx = ctx.mcx;
            let q = node.as_query_mut().unwrap();
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
                mcx,
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

fn recurse_map_attnos<'mcx>(node: &mut Node<'mcx>, ctx: &mut MapAttnosCtx<'_, 'mcx>) -> PgResult<bool> {
    let mut err: Option<PgError> = None;
    let scratch_mcx = ctx.mcx;
    let aborted = expression_tree_walker_mut(
        node,
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
        scratch_mcx,
    );
    if let Some(e) = err {
        return Err(e);
    }
    Ok(aborted)
}

/// `map_variable_attnos(node, target_varno, sublevels_up, attno_map, to_rowtype,
/// found_whole_row)` (rewriteManip.c:1701). `attno_map` is the `AttrMap.attnums`
/// slice (varattno `n` is replaced by `attno_map[n-1]`).
pub fn map_variable_attnos<'mcx>(
    node: &mut Node<'mcx>,
    target_varno: i32,
    sublevels_up: i32,
    attno_map: &[i16],
    to_rowtype: Oid,
    found_whole_row: &mut bool,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    *found_whole_row = false;
    let mut ctx = MapAttnosCtx {
        target_varno,
        sublevels_up,
        attno_map,
        to_rowtype,
        found_whole_row,
        mcx,
    };
    if let Some(q) = node.as_query_mut() {
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
            mcx,
        );
        if let Some(e) = err {
            return Err(e);
        }
    } else {
        map_variable_attnos_mutator(node, &mut ctx)?;
    }
    Ok(())
}

/// `map_variable_attnos((Node *) exprs, 1, 0, attmap, InvalidOid,
/// &found_whole_row)` over a `List*` of expressions, as `catalog/index.c`
/// `CompareIndexInfo` calls it on `info2->ii_Expressions` / `info2->ii_Predicate`
/// (a `List *`). `target_varno = 1`, `sublevels_up = 0`, `to_rowtype = InvalidOid`
/// are pinned to that single call site.
///
/// In C `map_variable_attnos((Node *) list, ...)` recurses into the `T_List`
/// arm of the mutator, applying the per-Var rewrite to every list element and
/// OR-ing `found_whole_row` across them. Over the owned model the list is a
/// `PgVec<Expr>`; we map each element in place (wrapping it as a `Node::Expr`
/// for the per-node mutator, mirroring the C in-place rewrite) and accumulate
/// `found_whole_row`. The input vector is consumed and returned mutated.
pub fn map_variable_attnos_expr_list<'mcx>(
    mcx: Mcx<'mcx>,
    exprs: PgVec<'mcx, Expr<'mcx>>,
    attmap: &[i16],
) -> PgResult<(PgVec<'mcx, Expr<'mcx>>, bool)> {
    let mut out: PgVec<'mcx, Expr<'mcx>> = mcx::vec_with_capacity_in(mcx, exprs.len())?;
    let mut found_whole_row = false;
    for owned in exprs.into_iter() {
        // Wrap each list element as a Node::Expr, map it in place (mirroring the
        // C in-place per-element rewrite under the `T_List` mutator arm), and
        // collect it back. found_whole_row is OR-accumulated across the list.
        let mut node = Node::mk_expr(mcx, owned)?;
        let mut one_fwr = false;
        map_variable_attnos(&mut node, 1, 0, attmap, INVALID_OID, &mut one_fwr, mcx)?;
        found_whole_row |= one_fwr;
        match node.into_expr() {
            Some(mapped) => out.push(mapped),
            // map_variable_attnos never changes the top-level node kind for an
            // Expr input.
            None => unreachable!("map_variable_attnos returned a non-Expr for an Expr input"),
        }
    }
    Ok((out, found_whole_row))
}

/// `map_variable_attnos((Node *) clause, target_varno, 0, attmap, to_rowtype,
/// &found_whole_row)` over a `List *` of `Expr`, as `execPartition.c`
/// `ExecInitPartitionInfo` calls it on the ON CONFLICT DO UPDATE WHERE clause
/// (twice — `INNER_VAR` then `firstVarno`). Like
/// [`map_variable_attnos_expr_list`] but with the `target_varno` / `to_rowtype`
/// exposed rather than pinned to the `CompareIndexInfo` call site.
pub fn map_variable_attnos_expr_list_varno<'mcx>(
    mcx: Mcx<'mcx>,
    exprs: PgVec<'mcx, Expr<'mcx>>,
    target_varno: i32,
    attmap: &[i16],
    to_rowtype: Oid,
) -> PgResult<(PgVec<'mcx, Expr<'mcx>>, bool)> {
    let mut out: PgVec<'mcx, Expr<'mcx>> = mcx::vec_with_capacity_in(mcx, exprs.len())?;
    let mut found_whole_row = false;
    for owned in exprs.into_iter() {
        let mut node = Node::mk_expr(mcx, owned)?;
        let mut one_fwr = false;
        map_variable_attnos(&mut node, target_varno, 0, attmap, to_rowtype, &mut one_fwr, mcx)?;
        found_whole_row |= one_fwr;
        match node.into_expr() {
            Some(mapped) => out.push(mapped),
            None => unreachable!("map_variable_attnos returned a non-Expr for an Expr input"),
        }
    }
    Ok((out, found_whole_row))
}

/// `map_variable_attnos((Node *) returningList, firstVarno, 0, attmap,
/// RelationGetForm(partrel)->reltype, &found_whole_row)` over a `List *` of
/// `TargetEntry`, as `execPartition.c` `ExecInitPartitionInfo` calls it on the
/// first plan's RETURNING list.
///
/// In C `map_variable_attnos((Node *) returningList, ...)` recurses into the
/// `T_List` arm of the mutator, then into each `TargetEntry` (the generic
/// `expression_tree_mutator` arm copies the `TargetEntry` and recurses into its
/// `expr`). Over the owned model the list is a `PgVec<TargetEntry>`; we map each
/// element's `expr` in place (wrapping it as a `Node::Expr`, mirroring the C
/// per-element rewrite) with `sublevels_up = 0` and the caller-supplied
/// `target_varno` / `to_rowtype`, and OR-accumulate `found_whole_row`. The input
/// vector is consumed and returned mutated.
pub fn map_variable_attnos_targetentry_list<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: PgVec<'mcx, nodes::primnodes::TargetEntry<'mcx>>,
    target_varno: i32,
    attmap: &[i16],
    to_rowtype: Oid,
) -> PgResult<(PgVec<'mcx, nodes::primnodes::TargetEntry<'mcx>>, bool)> {
    let mut out: PgVec<'mcx, nodes::primnodes::TargetEntry<'mcx>> =
        mcx::vec_with_capacity_in(mcx, tlist.len())?;
    let mut found_whole_row = false;
    for mut tle in tlist.into_iter() {
        if let Some(expr_box) = tle.expr.take() {
            // Move the TargetEntry's expr out, wrap it as a Node::Expr, map it in
            // place (the C generic mutator arm recurses into tle->expr), then
            // write it back. found_whole_row is OR-accumulated across the list.
            let owned: Expr = mcx::box_into_inner_leak(expr_box);
            let mut node = Node::mk_expr(mcx, owned)?;
            let mut one_fwr = false;
            map_variable_attnos(
                &mut node,
                target_varno,
                0,
                attmap,
                to_rowtype,
                &mut one_fwr,
                mcx,
            )?;
            found_whole_row |= one_fwr;
            match node.into_expr() {
                Some(mapped) => {
                    tle.expr = Some(mcx::alloc_in(mcx, mapped)?);
                }
                None => unreachable!(
                    "map_variable_attnos returned a non-Expr for a TargetEntry expr input"
                ),
            }
        }
        out.push(tle);
    }
    Ok((out, found_whole_row))
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
    targetlist: &[nodes::primnodes::TargetEntry<'mcx>],
    result_relation: i32,
    nomatch_option: ReplaceVarsNoMatchOption,
    nomatch_varno: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<Expr<'mcx>> {
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
                    .map(|n| match n.as_string() {
                        Some(s) => String::from(s.sval.as_str()),
                        None => String::new(),
                    })
                    .collect()
            } else {
                Vec::new()
            },
            location: var.location,
        };

        // Adjust the generated per-field Vars...
        for field_node in fields.iter() {
            let field_expr: Expr = if let Some(field_var) = field_node.as_var() {
                ReplaceVarFromTargetList(
                    field_var,
                    target_rte,
                    targetlist,
                    result_relation,
                    nomatch_option,
                    nomatch_varno,
                    mcx,
                )?
            } else if let Some(e) = field_node.as_expr() {
                // C shares the field pointer in-place; the owned model needs a
                // copy. Route through clone_in (not `.clone()`) so a
                // SubLink-bearing field does not hit the panicking derived Clone.
                e.clone_in(mcx)?
            } else {
                continue;
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
            // Make a copy of the tlist item to return (C: copyObject).
            // Must route through clone_in(mcx) — a plain `.clone()` panics when
            // tle->expr carries a SubLink (its derived Clone is a guard;
            // copyObject is the sanctioned deep-copy path).
            let mut newnode: Expr =
                tle.expr.as_deref().expect("tle->expr set").clone_in(mcx)?;

            // Check for a PARAM_MULTIEXPR Param and throw error if so.
            if contains_multiexpr_param(&Node::mk_expr(mcx, newnode.clone_in(mcx)?)?) {
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
                let mut wrapped = Node::mk_expr(mcx, newnode)?;
                crate::increment::SetVarReturningType(
                    &mut wrapped,
                    result_relation,
                    0,
                    var.varreturningtype,
                    mcx,
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
) -> PgResult<Expr<'mcx>> {
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
            // `coerce_null_to_domain` returns the substitute under the parser-
            // coerce seam's `'static` arena-intern convention; re-localize the
            // (self-contained Const/CoerceToDomain) node into the rewrite arena
            // `'mcx` so the substitute matches the rewrite tree it is spliced into.
            e.clone_in(mcx)
        }
    }
}

fn node_into_expr<'mcx>(node: Node<'mcx>) -> PgResult<Expr<'mcx>> {
    match node.into_expr() {
        Some(e) => Ok(e),
        None => Err(elog_error(
            "ReplaceVarFromTargetList: expected an expression node",
        )),
    }
}

/// `ReplaceVarsFromTargetList(node, target_varno, sublevels_up, target_rte,
/// targetlist, result_relation, nomatch_option, nomatch_varno,
/// outer_hasSubLinks)` (rewriteManip.c:1957).
#[allow(clippy::too_many_arguments)]
pub fn ReplaceVarsFromTargetList<'mcx>(
    node: &mut Node<'mcx>,
    target_varno: i32,
    sublevels_up: i32,
    target_rte: &RangeTblEntry<'mcx>,
    targetlist: &[nodes::primnodes::TargetEntry<'mcx>],
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
            let mut wrapped = Node::mk_expr(mcx, newexpr)?;
            crate::increment::IncrementVarSublevelsUp(&mut wrapped, var.varlevelsup as i32, 0, mcx)?;
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
        mcx,
    )
}
