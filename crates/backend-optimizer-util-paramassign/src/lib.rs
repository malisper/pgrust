//! `optimizer/util/paramassign.c` — assigning PARAM_EXEC slots during planning.
//!
//! Ported 1:1 over the arena+handle model of [`types_pathnodes::PlannerInfo`].
//! This module manages the three planner data structures paramassign owns:
//!
//! * `root->glob->paramExecTypes` — the global PARAM_EXEC slot type list
//!   (`param_exec_types` on [`types_pathnodes::PlannerGlobal`]).
//! * `root->plan_params` — `PlannerParamItem`s this query level supplies to a
//!   lower subquery (interned as [`types_pathnodes::NodeId`] handles into the
//!   node arena, resolved via `planner_param_item`).
//! * `root->curOuterParams` — not-yet-assigned `NestLoopParam`s (interned as
//!   [`types_pathnodes::NodeId`] handles, resolved via `nestloop_param`).
//!
//! Node operators that would create a dependency cycle are reached through seam
//! crates (`exprType`/`exprTypmod`/`exprCollation` via nodeFuncs's
//! `expr_type_info`, `exprLocation` via nodeFuncs's `exprLocation`, `equal` via
//! equalfuncs's `equal_expr`, the `Relids` set algebra via relnode's seams).
//! `find_placeholder_info` / `get_placeholder_nulling_relids` (placeholder.c,
//! owned by joininfo) and `IncrementVarSublevelsUp` (rewriteManip.c, owned by
//! backend-rewrite-core) are reached by a direct crate edge — those owners do
//! not cycle back here.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use backend_optimizer_util_joininfo::placeholder::{
    find_placeholder_info, get_placeholder_nulling_relids,
};
use backend_rewrite_core::increment::IncrementVarSublevelsUp;

use mcx::Mcx;
use types_core::{primitive::InvalidOid, Index, Oid};
use types_error::{PgError, PgResult};
use types_nodes::nodes::{CmdType, Node};
use types_nodes::primnodes::{
    Aggref, Expr, ExprRelids, GroupingFunc, MergeSupportFunc, Param, ParamKind, PlaceHolderVar,
    ReturningExpr, Var,
};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    Bitmapset, NestLoopParamNode, NodeId, PlannerGlobal, PlannerInfo, PlannerParamItem, Relids,
};

// Seam modules (call via `::call(...)`).
use backend_nodes_equalfuncs_seams::equal_expr;
use backend_nodes_nodeFuncs_seams::{exprLocation, expr_type_info};
use backend_optimizer_util_relnode_seams::{
    relids_equal, relids_intersect, relids_is_member, relids_is_subset, relids_overlap,
    relids_union,
};

#[cfg(test)]
mod tests;

/// `elog(ERROR, msg)` helper used in this unit's own logic.
fn elog_error(msg: &str) -> PgError {
    PgError::error(msg)
}

/// Append `typ` to `root->glob->paramExecTypes`, returning the new slot's index
/// (the old `list_length`). C: `lappend_oid(root->glob->paramExecTypes, typ)`.
///
/// In C the single `PlannerGlobal` is shared by pointer across every planning
/// level, so this can be appended from any `root`. In the owned model the one
/// `glob` lives on whichever root is currently being planned (the deepest /
/// most-recently-recursed root — e.g. a SubLink's `subroot`), and an *upper*
/// `root` reached by ascending `parent_root` has its `glob` moved out for the
/// duration. Param-exec slots are global, so we must always register against
/// the live `glob`. Callers therefore pass the glob owner here (the original,
/// un-ascended `root`) rather than the ascended level (which holds the
/// `plan_params` list but not the shared `glob`).
fn append_param_exec_type(glob_owner: &mut PlannerInfo, typ: Oid) -> i32 {
    append_param_exec_type_glob(&mut glob_owner.glob, typ)
}

/// Same as [`append_param_exec_type`] but operating on a `glob` `Box` that has
/// been temporarily `take()`n out of its owning root (so the caller can hold a
/// concurrent `&mut` to an *ascended* parent level for the `plan_params` push).
fn append_param_exec_type_glob(
    glob: &mut Option<alloc::boxed::Box<PlannerGlobal>>,
    typ: Oid,
) -> i32 {
    let glob = glob
        .as_mut()
        .expect("paramassign: root->glob must be set");
    let id = glob.param_exec_types.len() as i32;
    glob.param_exec_types.push(typ);
    id
}

/// Walk `root` up `levelsup` `parent_root` links, returning `&mut` to the
/// reached PlannerInfo. C: `for (; levelsup > 0; levelsup--) root = root->parent_root;`
fn ascend_mut(root: &mut PlannerInfo, levelsup: Index) -> &mut PlannerInfo {
    let mut cur = root;
    let mut n = levelsup;
    while n > 0 {
        cur = cur
            .parent_root
            .as_mut()
            .expect("paramassign: parent_root chain shorter than levelsup");
        n -= 1;
    }
    cur
}

/// Wrap an `Expr` as a `Node`, run `IncrementVarSublevelsUp`, and unwrap. C:
/// `IncrementVarSublevelsUp((Node *) expr, delta, min)`.
fn increment_expr_sublevels(e: Expr, delta: i32, min: i32) -> PgResult<Expr> {
    let mut node = Node::Expr(e);
    IncrementVarSublevelsUp(&mut node, delta, min)?;
    Ok(node
        .into_expr()
        .unwrap_or_else(|| unreachable!("IncrementVarSublevelsUp preserves the Node::Expr wrapper")))
}

/// `equal(a, b)` over a `curOuterParams` `paramval` (always a `Var` or
/// `PlaceHolderVar`). C calls `equal()`; equalfuncs.c's `_equalVar` is reached
/// through the `equal_expr` seam, but `_equalPlaceHolderVar` is not yet wired in
/// the equalfuncs port (it panics on PHV), so PHV equality is reproduced here
/// field-for-field (mirroring `_equalPlaceHolderVar`: `phexpr` via `equal_expr`,
/// `phrels`/`phid`/`phlevelsup`/`phnullingrels`; `phnullingrels` IS compared by
/// `_equalPlaceHolderVar`). Mismatched variants are never equal.
fn equal_paramval(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::PlaceHolderVar(pa), Expr::PlaceHolderVar(pb)) => {
            let phexpr_eq = match (&pa.phexpr, &pb.phexpr) {
                (None, None) => true,
                (Some(ea), Some(eb)) => equal_expr::call(ea, eb),
                _ => false,
            };
            phexpr_eq
                && pa.phrels.words == pb.phrels.words
                && pa.phid == pb.phid
                && pa.phlevelsup == pb.phlevelsup
                && pa.phnullingrels.words == pb.phnullingrels.words
        }
        _ => equal_expr::call(a, b),
    }
}

/// Borrow an [`ExprRelids`] (the lifetime-free relids carried on a `Var`/`PHV`)
/// as a [`Relids`] (`Option<Box<Bitmapset>>`) the relnode `relids_*` seams take.
/// The two share the `{ words }` representation; an all-zero/empty `words` is the
/// empty set (`None`).
fn expr_relids_to_relids(er: &ExprRelids) -> Relids {
    if er.words.iter().all(|&w| w == 0) {
        None
    } else {
        Some(alloc::boxed::Box::new(Bitmapset {
            words: er.words.clone(),
        }))
    }
}

/// Inverse of [`expr_relids_to_relids`]: store a [`Relids`] result back into a
/// `Var.varnullingrels` / `PlaceHolderVar.phnullingrels` field.
fn relids_to_expr_relids(relids: &Relids) -> ExprRelids {
    match relids {
        None => ExprRelids { words: Vec::new() },
        Some(bms) => ExprRelids {
            words: bms.words.clone(),
        },
    }
}

/*
 * Select a PARAM_EXEC number to identify the given Var as a parameter for the
 * current subquery.  (It might already have one.)
 * Record the need for the Var in the proper upper-level root->plan_params.
 */
fn assign_param_for_var(root: &mut PlannerInfo, var: &Var) -> PgResult<i32> {
    // In the owned model the single shared `glob` lives on this (deepest) root.
    // We register the param-exec slot against it, but the PlannerParamItem /
    // plan_params list belongs to the *upper* level the Var references, reached
    // by ascending `parent_root`. Take the shared `glob` out of `root` for the
    // duration so we can hold a `&mut` to the ascended level concurrently, then
    // restore it. C never has to do this dance because glob is one shared
    // pointer; here it is moved-down-the-chain ownership.
    let mut glob = root.glob.take();

    let result = (|| {
        // Find the query level the Var belongs to.
        let root = ascend_mut(root, var.varlevelsup);

        // If there's already a matching PlannerParamItem there, just use it.
        for ppl in root.plan_params.clone() {
            let item = root.planner_param_item(ppl).item;
            if let Expr::Var(pvar) = root.node(item) {
                // This comparison must match _equalVar(), except for ignoring
                // varlevelsup.  Note that _equalVar() ignores varnosyn,
                // varattnosyn, and location, so this does too.
                if pvar.varno == var.varno
                    && pvar.varattno == var.varattno
                    && pvar.vartype == var.vartype
                    && pvar.vartypmod == var.vartypmod
                    && pvar.varcollid == var.varcollid
                    && pvar.varreturningtype == var.varreturningtype
                    && relids_equal::call(
                        &expr_relids_to_relids(&pvar.varnullingrels),
                        &expr_relids_to_relids(&var.varnullingrels),
                    )
                {
                    return Ok(root.planner_param_item(ppl).paramId);
                }
            }
        }

        // Nope, so make a new one.
        let mut newvar = var.clone();
        newvar.varlevelsup = 0;
        let vartype = newvar.vartype;

        let param_id = append_param_exec_type_glob(&mut glob, vartype);
        let item = root.alloc_node(Expr::Var(newvar));
        let pitem = root.alloc_planner_param_item(PlannerParamItem {
            item,
            paramId: param_id,
        });
        root.plan_params.push(pitem);

        Ok(param_id)
    })();

    root.glob = glob;
    result
}

/*
 * Generate a Param node to replace the given Var, which is expected to have
 * varlevelsup > 0 (ie, it is not local).
 */
pub fn replace_outer_var(root: &mut PlannerInfo, var: &Var) -> PgResult<Param> {
    debug_assert!(var.varlevelsup > 0 && var.varlevelsup < root.query_level);

    // Find the Var in the appropriate plan_params, or add it if not present.
    let i = assign_param_for_var(root, var)?;

    Ok(Param {
        paramkind: ParamKind::PARAM_EXEC,
        paramid: i,
        paramtype: var.vartype,
        paramtypmod: var.vartypmod,
        paramcollid: var.varcollid,
        location: var.location,
    })
}

/*
 * Select a PARAM_EXEC number to identify the given PlaceHolderVar as a parameter
 * for the current subquery.  (It might already have one.)
 *
 * This is just like assign_param_for_var, except for PlaceHolderVars.
 */
fn assign_param_for_placeholdervar(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    phv: &PlaceHolderVar,
) -> PgResult<i32> {
    // Take the shared `glob` out of this (deepest) root so it can be appended to
    // while a `&mut` to the ascended upper level is held (see
    // `assign_param_for_var` for the rationale).
    let mut glob = root.glob.take();

    let result = (|| {
        // Find the query level the PHV belongs to.
        let root = ascend_mut(root, phv.phlevelsup);

        // If there's already a matching PlannerParamItem there, just use it.
        for ppl in root.plan_params.clone() {
            let item = root.planner_param_item(ppl).item;
            if let Expr::PlaceHolderVar(pphv) = root.node(item) {
                // We assume comparing the PHIDs is sufficient.
                if pphv.phid == phv.phid {
                    return Ok(root.planner_param_item(ppl).paramId);
                }
            }
        }

        // Nope, so make a new one.
        let copied = increment_expr_sublevels(
            Expr::PlaceHolderVar(phv.clone_in(mcx)?),
            -(phv.phlevelsup as i32),
            0,
        )?;
        let newphv = match copied {
            Expr::PlaceHolderVar(p) => p,
            _ => unreachable!(),
        };
        debug_assert!(newphv.phlevelsup == 0);

        let ptype = expr_type_info::call(
            newphv
                .phexpr
                .as_deref()
                .expect("PlaceHolderVar::phexpr must be set"),
        )?
        .typid;

        let param_id = append_param_exec_type_glob(&mut glob, ptype);
        let item = root.alloc_node(Expr::PlaceHolderVar(newphv));
        let pitem = root.alloc_planner_param_item(PlannerParamItem {
            item,
            paramId: param_id,
        });
        root.plan_params.push(pitem);
        Ok(param_id)
    })();

    root.glob = glob;
    result
}

/*
 * Generate a Param node to replace the given PlaceHolderVar, which is expected
 * to have phlevelsup > 0 (ie, it is not local).
 *
 * This is just like replace_outer_var, except for PlaceHolderVars.
 */
pub fn replace_outer_placeholdervar(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    phv: &PlaceHolderVar,
) -> PgResult<Param> {
    debug_assert!(phv.phlevelsup > 0 && phv.phlevelsup < root.query_level);

    let i = assign_param_for_placeholdervar(mcx, root, phv)?;

    let info = expr_type_info::call(phv.phexpr.as_deref().expect("PlaceHolderVar::phexpr"))?;
    Ok(Param {
        paramkind: ParamKind::PARAM_EXEC,
        paramid: i,
        paramtype: info.typid,
        paramtypmod: info.typmod,
        paramcollid: info.collation,
        location: -1,
    })
}

/*
 * Generate a Param node to replace the given Aggref which is expected to have
 * agglevelsup > 0 (ie, it is not local).
 */
pub fn replace_outer_agg(mcx: Mcx<'_>, root: &mut PlannerInfo, agg: &Aggref) -> PgResult<Param> {
    debug_assert!(agg.agglevelsup > 0 && agg.agglevelsup < root.query_level);

    // Take the shared `glob` out of this (deepest) root so it can be appended to
    // while a `&mut` to the ascended upper level is held (see
    // `assign_param_for_var` for the rationale).
    let mut glob = root.glob.take();

    let result = (|| {
        // Find the query level the Aggref belongs to.
        let root = ascend_mut(root, agg.agglevelsup);

        // It does not seem worthwhile to try to de-duplicate references to outer
        // aggs.  Just make a new slot every time.
        let copied = increment_expr_sublevels(
            Expr::Aggref(agg.clone_in(mcx)?),
            -(agg.agglevelsup as i32),
            0,
        )?;
        let newagg = match copied {
            Expr::Aggref(a) => a,
            _ => unreachable!(),
        };
        debug_assert!(newagg.agglevelsup == 0);

        let aggtype = newagg.aggtype;
        let aggcollid = newagg.aggcollid;
        let location = newagg.location;

        let param_id = append_param_exec_type_glob(&mut glob, aggtype);
        let item = root.alloc_node(Expr::Aggref(newagg));
        let pitem = root.alloc_planner_param_item(PlannerParamItem {
            item,
            paramId: param_id,
        });
        root.plan_params.push(pitem);

        Ok(Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: param_id,
            paramtype: aggtype,
            paramtypmod: -1,
            paramcollid: aggcollid,
            location,
        })
    })();

    root.glob = glob;
    result
}

/*
 * Generate a Param node to replace the given GroupingFunc expression which is
 * expected to have agglevelsup > 0 (ie, it is not local).
 */
pub fn replace_outer_grouping(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    grp: &GroupingFunc,
) -> PgResult<Param> {
    let ptype = expr_type_info::call(&Expr::GroupingFunc(grp.clone_in(mcx)?))?.typid;

    debug_assert!(grp.agglevelsup > 0 && grp.agglevelsup < root.query_level);

    // Take the shared `glob` out of this (deepest) root so it can be appended to
    // while a `&mut` to the ascended upper level is held (see
    // `assign_param_for_var` for the rationale).
    let mut glob = root.glob.take();

    let result = (|| {
        // Find the query level the GroupingFunc belongs to.
        let root = ascend_mut(root, grp.agglevelsup);

        // Just make a new slot every time.
        let copied = increment_expr_sublevels(
            Expr::GroupingFunc(grp.clone_in(mcx)?),
            -(grp.agglevelsup as i32),
            0,
        )?;
        let newgrp = match copied {
            Expr::GroupingFunc(g) => g,
            _ => unreachable!(),
        };
        debug_assert!(newgrp.agglevelsup == 0);

        let location = newgrp.location;
        let param_id = append_param_exec_type_glob(&mut glob, ptype);
        let item = root.alloc_node(Expr::GroupingFunc(newgrp));
        let pitem = root.alloc_planner_param_item(PlannerParamItem {
            item,
            paramId: param_id,
        });
        root.plan_params.push(pitem);

        Ok(Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: param_id,
            paramtype: ptype,
            paramtypmod: -1,
            paramcollid: InvalidOid,
            location,
        })
    })();

    root.glob = glob;
    result
}

/*
 * Generate a Param node to replace the given MergeSupportFunc expression which
 * is expected to be in the RETURNING list of an upper-level MERGE query.
 */
pub fn replace_outer_merge_support(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    msf: &MergeSupportFunc,
) -> PgResult<Param> {
    let ptype = expr_type_info::call(&Expr::MergeSupportFunc(msf.clone_in(mcx)?))?.typid;

    debug_assert!(run.resolve(root.parse).commandType != CmdType::CMD_MERGE);

    // Take the shared `glob` out of this (deepest) root so it can be appended to
    // while a `&mut` to the upper MERGE target level is held (see
    // `assign_param_for_var` for the rationale).
    let mut glob = root.glob.take();

    let result = (|| {
        // The parser should have ensured that the MergeSupportFunc is in the
        // RETURNING list of an upper-level MERGE query, so find that query.
        let target: &mut PlannerInfo = {
            let mut cur: &mut PlannerInfo = root;
            loop {
                cur = match cur.parent_root.as_mut() {
                    Some(p) => p,
                    None => return Err(elog_error("MergeSupportFunc found outside MERGE")),
                };
                if run.resolve(cur.parse).commandType == CmdType::CMD_MERGE {
                    break cur;
                }
            }
        };

        // Just make a new slot every time.
        let newmsf = msf.clone_in(mcx)?;
        let location = newmsf.location;
        let param_id = append_param_exec_type_glob(&mut glob, ptype);
        let item = target.alloc_node(Expr::MergeSupportFunc(newmsf));
        let pitem = target.alloc_planner_param_item(PlannerParamItem {
            item,
            paramId: param_id,
        });
        target.plan_params.push(pitem);

        Ok(Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: param_id,
            paramtype: ptype,
            paramtypmod: -1,
            paramcollid: InvalidOid,
            location,
        })
    })();

    root.glob = glob;
    result
}

/*
 * Generate a Param node to replace the given ReturningExpr expression which is
 * expected to have retlevelsup > 0 (ie, it is not local).
 */
pub fn replace_outer_returning(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rexpr: &ReturningExpr,
) -> PgResult<Param> {
    let retexpr = rexpr
        .retexpr
        .as_deref()
        .expect("ReturningExpr::retexpr must be set");
    let info = expr_type_info::call(retexpr)?;
    let ptype = info.typid;
    let retexpr_location = exprLocation::call(retexpr);

    debug_assert!(rexpr.retlevelsup > 0 && (rexpr.retlevelsup as i64) < root.query_level as i64);

    // Take the shared `glob` out of this (deepest) root so it can be appended to
    // while a `&mut` to the ascended upper level is held (see
    // `assign_param_for_var` for the rationale).
    let mut glob = root.glob.take();

    let result = (|| {
        // Find the query level the ReturningExpr belongs to.
        let root = ascend_mut(root, rexpr.retlevelsup as Index);

        // Just make a new slot every time.
        let copied = increment_expr_sublevels(
            Expr::ReturningExpr(rexpr.clone_in(mcx)?),
            -(rexpr.retlevelsup as i32),
            0,
        )?;
        let newrexpr = match copied {
            Expr::ReturningExpr(r) => r,
            _ => unreachable!(),
        };
        debug_assert!(newrexpr.retlevelsup == 0);

        let param_id = append_param_exec_type_glob(&mut glob, ptype);
        let item = root.alloc_node(Expr::ReturningExpr(newrexpr));
        let pitem = root.alloc_planner_param_item(PlannerParamItem {
            item,
            paramId: param_id,
        });
        root.plan_params.push(pitem);

        Ok(Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: param_id,
            paramtype: ptype,
            paramtypmod: info.typmod,
            paramcollid: info.collation,
            location: retexpr_location,
        })
    })();

    root.glob = glob;
    result
}

/*
 * Generate a Param node to replace the given Var, which is expected to come from
 * some upper NestLoop plan node.  Record the need for the Var in
 * root->curOuterParams.
 */
pub fn replace_nestloop_param_var(root: &mut PlannerInfo, var: &Var) -> PgResult<Param> {
    // Is this Var already listed in root->curOuterParams?
    for lc in root.curOuterParams.clone() {
        let nlp = root.nestloop_param(lc);
        if equal_paramval(&Expr::Var(var.clone()), &nlp.paramval) {
            // Yes, so just make a Param referencing this NLP's slot.
            return Ok(Param {
                paramkind: ParamKind::PARAM_EXEC,
                paramid: nlp.paramno,
                paramtype: var.vartype,
                paramtypmod: var.vartypmod,
                paramcollid: var.varcollid,
                location: var.location,
            });
        }
    }

    // No, so assign a PARAM_EXEC slot for a new NLP.
    let mut param = generate_new_exec_param(root, var.vartype, var.vartypmod, var.varcollid)?;
    param.location = var.location;

    // Add it to the list of required NLPs.
    let id = root.alloc_nestloop_param(NestLoopParamNode {
        paramno: param.paramid,
        paramval: Expr::Var(var.clone()),
    });
    root.curOuterParams.push(id);

    Ok(param)
}

/*
 * Generate a Param node to replace the given PlaceHolderVar, which is expected
 * to come from some upper NestLoop plan node.
 *
 * This is just like replace_nestloop_param_var, except for PlaceHolderVars.
 */
pub fn replace_nestloop_param_placeholdervar(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    phv: &PlaceHolderVar,
) -> PgResult<Param> {
    // Is this PHV already listed in root->curOuterParams?
    for lc in root.curOuterParams.clone() {
        let nlp = root.nestloop_param(lc);
        if equal_paramval(&Expr::PlaceHolderVar(phv.clone()), &nlp.paramval) {
            // Yes, so just make a Param referencing this NLP's slot.
            let info = expr_type_info::call(phv.phexpr.as_deref().expect("PlaceHolderVar::phexpr"))?;
            return Ok(Param {
                paramkind: ParamKind::PARAM_EXEC,
                paramid: nlp.paramno,
                paramtype: info.typid,
                paramtypmod: info.typmod,
                paramcollid: info.collation,
                location: -1,
            });
        }
    }

    // No, so assign a PARAM_EXEC slot for a new NLP.
    let info = expr_type_info::call(phv.phexpr.as_deref().expect("PlaceHolderVar::phexpr"))?;
    let param = generate_new_exec_param(root, info.typid, info.typmod, info.collation)?;

    // Add it to the list of required NLPs (the PHV is `nlp->paramval` in C).
    let newphv = phv.clone_in(mcx)?;
    let id = root.alloc_nestloop_param(NestLoopParamNode {
        paramno: param.paramid,
        paramval: Expr::PlaceHolderVar(newphv),
    });
    root.curOuterParams.push(id);

    Ok(param)
}

/*
 * process_subquery_nestloop_params
 *   Handle params of a parameterized subquery that need to be fed from an outer
 *   nestloop.
 */
pub fn process_subquery_nestloop_params(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    subplan_params: &[NodeId],
) -> PgResult<()> {
    for &lc in subplan_params {
        let pitem = root.planner_param_item(lc).clone();
        // Deep-copy via `Expr::clone_in` (a derived `Expr::clone` panics on a
        // context-allocated child such as a PHV over an `Aggref`/`SubLink`).
        let item_expr = root.node(pitem.item).clone_in(mcx)?;

        match item_expr {
            Expr::Var(var) => {
                // If not from a nestloop outer rel, complain.
                if !relids_is_member::call(var.varno, &root.curOuterRels) {
                    return Err(elog_error("non-LATERAL parameter required by subquery"));
                }

                // Is this param already listed in root->curOuterParams?
                let mut present = false;
                for lc2 in root.curOuterParams.clone() {
                    let nlp = root.nestloop_param(lc2);
                    if nlp.paramno == pitem.paramId {
                        debug_assert!(equal_paramval(&Expr::Var(var.clone()), &nlp.paramval));
                        present = true;
                        break;
                    }
                }
                if !present {
                    // No, so add it.
                    let id = root.alloc_nestloop_param(NestLoopParamNode {
                        paramno: pitem.paramId,
                        paramval: Expr::Var(var.clone()),
                    });
                    root.curOuterParams.push(id);
                }
            }
            Expr::PlaceHolderVar(phv) => {
                // If not from a nestloop outer rel, complain.
                let phinfo = find_placeholder_info(root, &phv)?;
                let ph_eval_at = root.phinfo(phinfo).ph_eval_at.clone();
                if !relids_is_subset::call(&ph_eval_at, &root.curOuterRels) {
                    return Err(elog_error("non-LATERAL parameter required by subquery"));
                }

                // Is this param already listed in root->curOuterParams?
                let mut present = false;
                for lc2 in root.curOuterParams.clone() {
                    let nlp = root.nestloop_param(lc2);
                    if nlp.paramno == pitem.paramId {
                        debug_assert!(equal_paramval(&Expr::PlaceHolderVar(phv.clone()), &nlp.paramval));
                        present = true;
                        break;
                    }
                }
                if !present {
                    // No, so add it.
                    let newphv = phv.clone_in(mcx)?;
                    let id = root.alloc_nestloop_param(NestLoopParamNode {
                        paramno: pitem.paramId,
                        paramval: Expr::PlaceHolderVar(newphv),
                    });
                    root.curOuterParams.push(id);
                }
            }
            _ => return Err(elog_error("unexpected type of subquery parameter")),
        }
    }
    Ok(())
}

/*
 * Identify any NestLoopParams that should be supplied by a NestLoop plan node
 * with the specified lefthand rels and required-outer rels.  Remove them from
 * the active root->curOuterParams list and return them as the result list.
 */
pub fn identify_current_nestloop_params(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    leftrelids: &Relids,
    outerrelids: &Relids,
) -> PgResult<Vec<NodeId>> {
    // We'll be able to evaluate a PHV in the lefthand path if it uses the
    // lefthand rels plus any available required-outer rels.  But don't do so if
    // it uses *only* required-outer rels.  For Vars, no such hair-splitting is
    // necessary since they depend on only one relid.
    let allleftrelids = if outerrelids.is_some() {
        relids_union::call(leftrelids, outerrelids)
    } else {
        leftrelids.clone()
    };

    let has_sub_links = run.resolve(root.parse).hasSubLinks;

    let mut result: Vec<NodeId> = Vec::new();
    let mut kept: Vec<NodeId> = Vec::new();

    for lc in root.curOuterParams.clone() {
        // We are looking for Vars and PHVs that can be supplied by the lefthand
        // rels.  When we find one, it's okay to modify it in-place because all
        // the routines above make a fresh copy to put into curOuterParams.
        let pv = root.nestloop_param(lc).paramval.clone();
        match pv {
            Expr::Var(var) if relids_is_member::call(var.varno, leftrelids) => {
                let rel = root.simple_rel_array[var.varno as usize]
                    .expect("paramassign: simple_rel_array slot for nestloop Var");
                let nulling = root.rel_arena[rel.index()].nulling_relids.clone();
                let intersected = relids_intersect::call(&nulling, leftrelids);
                let intersected = relids_to_expr_relids(&intersected);
                if let Expr::Var(v) = &mut root.nestloop_param_mut(lc).paramval {
                    v.varnullingrels = intersected;
                }
                result.push(lc);
            }
            Expr::PlaceHolderVar(phv) => {
                let phinfo = find_placeholder_info(root, &phv)?;
                let eval_at = root.phinfo(phinfo).ph_eval_at.clone();

                if relids_is_subset::call(&eval_at, &allleftrelids) && relids_overlap::call(&eval_at, leftrelids)
                {
                    // Edge case: if the PHV was pulled up out of a subquery and
                    // it contains a subquery that was originally pushed down from
                    // this query level, then it may still be a SubLink (since
                    // SS_process_sublinks won't recurse into outer PHVs).  We
                    // need a version of the PHV that has a SubPlan, which we can
                    // get from the current query level's placeholder_list.
                    if has_sub_links {
                        let ph_var = root.phinfo(phinfo).ph_var.clone_in(mcx)?;
                        // The ph_var has empty nullingrels, but that doesn't
                        // matter since we're about to overwrite phnullingrels.
                        root.nestloop_param_mut(lc).paramval = Expr::PlaceHolderVar(ph_var);
                    }

                    let nulling = get_placeholder_nulling_relids(root, phinfo);
                    let phnullingrels =
                        relids_to_expr_relids(&relids_intersect::call(&nulling, leftrelids));
                    if let Expr::PlaceHolderVar(p) = &mut root.nestloop_param_mut(lc).paramval {
                        p.phnullingrels = phnullingrels;
                    }

                    result.push(lc);
                } else {
                    kept.push(lc);
                }
            }
            _ => kept.push(lc),
        }
    }

    root.curOuterParams = kept;
    Ok(result)
}

/*
 * Generate a new Param node that will not conflict with any other.
 */
pub fn generate_new_exec_param(
    root: &mut PlannerInfo,
    paramtype: Oid,
    paramtypmod: i32,
    paramcollation: Oid,
) -> PgResult<Param> {
    let paramid = append_param_exec_type(root, paramtype);
    Ok(Param {
        paramkind: ParamKind::PARAM_EXEC,
        paramid,
        paramtype,
        paramtypmod,
        paramcollid: paramcollation,
        location: -1,
    })
}

/*
 * Assign a (nonnegative) PARAM_EXEC ID for a special parameter (one that is not
 * actually used to carry a value at runtime).
 */
pub fn assign_special_exec_param(root: &mut PlannerInfo) -> PgResult<i32> {
    Ok(append_param_exec_type(root, InvalidOid))
}

/// Install every seam this crate owns. Called once from `seams-init`.
pub fn init_seams() {
    use backend_optimizer_util_paramassign_seams as s;

    s::replace_nestloop_param_var::set(replace_nestloop_param_var);
    s::replace_nestloop_param_placeholdervar::set(replace_nestloop_param_placeholdervar);
    s::process_subquery_nestloop_params::set(process_subquery_nestloop_params);
    s::identify_current_nestloop_params::set(identify_current_nestloop_params);
    s::generate_new_exec_param::set(generate_new_exec_param);
    s::assign_special_exec_param::set(assign_special_exec_param);
}
