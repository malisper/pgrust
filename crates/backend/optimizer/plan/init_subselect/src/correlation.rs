//! CORRELATION + SUBLINK PROCESSING (subselect.c) — the
//! `SS_replace_correlation_vars` / `SS_process_sublinks` mutators and
//! `SS_identify_outer_params`.
//!
//! # Model reconciliation (read before editing)
//!
//! These functions are 1:1 ports of subselect.c's
//! `replace_correlation_vars_mutator` / `process_sublinks_mutator` /
//! `SS_identify_outer_params`. They operate over this repo's lifetime-free
//! [`Expr`] (the owned unified primnode enum), so the C
//! `expression_tree_mutator` over `Node *` becomes
//! [`expression_tree_mutator`](nodes_core::nodefuncs::expression_tree_mutator)
//! over `Expr`.
//!
//! `process_sublinks_mutator` calls `make_subplan` (in [`crate::subplan`]),
//! which needs `&mut PlannerRun<'mcx>` + `Mcx<'mcx>` to build and intern the
//! SubPlan tree; both ride along the mutator's context, since the C
//! `process_sublinks_context` only carries `root`/`isTopQual`.
//!
//! `SS_identify_outer_params` reads each ancestor level's `plan_params`
//! (a `Vec<NodeId>` of `PlannerParamItem` handles) and `init_plans` (a
//! `Vec<NodeId>` of `Expr::SubPlan` handles), exactly as C walks
//! `proot->plan_params` / `proot->init_plans`.

extern crate alloc;

use mcx::Mcx;
use types_error::PgResult;
use ::nodes::primnodes::Expr;
use pathnodes::planner_run::PlannerRun;
use pathnodes::PlannerInfo;

use nodes_core::makefuncs::{make_andclause, make_orclause};
use nodes_core::nodefuncs::expression_tree_mutator;
use relnode_seams as bms;

use crate::subplan::make_subplan;

/// `is_andclause(node)` — true if `node` is a `BoolExpr` with `AND_EXPR`.
fn is_andclause(node: &Expr) -> bool {
    matches!(
        node,
        Expr::BoolExpr(b) if b.boolop == ::nodes::primnodes::BoolExprType::AND_EXPR
    )
}

/// `is_orclause(node)` — true if `node` is a `BoolExpr` with `OR_EXPR`.
fn is_orclause(node: &Expr) -> bool {
    matches!(
        node,
        Expr::BoolExpr(b) if b.boolop == ::nodes::primnodes::BoolExprType::OR_EXPR
    )
}

// ===========================================================================
// SS_replace_correlation_vars
// ===========================================================================

/// `SS_replace_correlation_vars(root, expr)` (subselect.c): replace correlation
/// (uplevel) vars / PHVs / aggregates / GROUPING() / MergeSupportFuncs /
/// ReturningExprs with Params.
pub fn SS_replace_correlation_vars<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    expr: Expr<'mcx>,
) -> PgResult<Expr<'mcx>> {
    // No setup needed for tree walk, so away we go.
    replace_correlation_vars_mutator(mcx, root, run, expr)
}

/// `replace_correlation_vars_mutator(node, root)` (subselect.c).
fn replace_correlation_vars_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    node: Expr<'mcx>,
) -> PgResult<Expr<'mcx>> {
    // NB: in C each IsA check is a separate `if` (not else-if): a node matching
    // one variant with levelsup==0 falls through to expression_tree_mutator.
    if let Expr::Var(v) = &node {
        if v.varlevelsup > 0 {
            let prm = paramassign::replace_outer_var(root, v)?;
            return Ok(Expr::Param(prm));
        }
    }
    if let Expr::PlaceHolderVar(phv) = &node {
        if phv.phlevelsup > 0 {
            let prm =
                paramassign::replace_outer_placeholdervar(mcx, root, phv)?;
            return Ok(Expr::Param(prm));
        }
    }
    if let Expr::Aggref(agg) = &node {
        if agg.agglevelsup > 0 {
            let prm = paramassign::replace_outer_agg(mcx, root, agg)?;
            return Ok(Expr::Param(prm));
        }
    }
    if let Expr::GroupingFunc(grp) = &node {
        if grp.agglevelsup > 0 {
            let prm = paramassign::replace_outer_grouping(mcx, root, grp)?;
            return Ok(Expr::Param(prm));
        }
    }
    if let Expr::MergeSupportFunc(msf) = &node {
        // C: if (root->parse->commandType != CMD_MERGE)
        if run.resolve(root.parse).commandType != ::nodes::nodes::CmdType::CMD_MERGE {
            let prm = paramassign::replace_outer_merge_support(
                mcx, root, run, msf,
            )?;
            return Ok(Expr::Param(prm));
        }
    }
    if let Expr::ReturningExpr(rexpr) = &node {
        if rexpr.retlevelsup > 0 {
            let prm = paramassign::replace_outer_returning(mcx, root, rexpr)?;
            return Ok(Expr::Param(prm));
        }
    }

    // expression_tree_mutator(node, replace_correlation_vars_mutator, root)
    //
    // The mutator closure must thread `root`/`run`/`mcx`; since `replace_outer_*`
    // mutate `root`, the recursion needs `&mut`. `expression_tree_mutator` is
    // infallible (`FnMut(Expr) -> Expr`), so we stash any error and surface it
    // after the walk, mirroring how other crates bridge fallible mutators.
    let mut err: Option<types_error::PgError> = None;
    let result = expression_tree_mutator(node, &mut |child: Expr| {
        if err.is_some() {
            return child;
        }
        match replace_correlation_vars_mutator(mcx, root, run, child) {
            Ok(c) => c,
            Err(e) => {
                err = Some(e);
                // Return a harmless placeholder; the error short-circuits below.
                Expr::Const(nodes_core::makefuncs::make_bool_const(true, false))
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(result),
    }
}

// ===========================================================================
// SS_process_sublinks
// ===========================================================================

/// `process_sublinks_context` (subselect.c) — the mutator's threaded context,
/// augmented with the `run`/`mcx` `make_subplan` needs.
struct ProcessSublinksContext {
    is_top_qual: bool,
}

/// `SS_process_sublinks(root, expr, isQual)` (subselect.c): expand SubLinks to
/// SubPlans in the given expression.
pub fn SS_process_sublinks<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    expr: Expr<'mcx>,
    is_qual: bool,
) -> PgResult<Expr<'mcx>> {
    let context = ProcessSublinksContext {
        is_top_qual: is_qual,
    };
    process_sublinks_mutator(mcx, root, run, expr, &context)
}

/// `process_sublinks_mutator(node, context)` (subselect.c).
fn process_sublinks_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    node: Expr<'mcx>,
    context: &ProcessSublinksContext,
) -> PgResult<Expr<'mcx>> {
    if let Expr::SubLink(sublink) = node {
        // First, recursively process the lefthand-side expressions, if any.
        // They're not top-level anymore.
        let testexpr = match sublink.testexpr {
            Some(te) => {
                let loc_context = ProcessSublinksContext {
                    is_top_qual: false,
                };
                let processed = process_sublinks_mutator(
                    mcx,
                    root,
                    run,
                    *te,
                    &loc_context,
                )?;
                Some(processed)
            }
            None => None,
        };

        // Now build the SubPlan node and make the expr to return.
        return make_subplan(
            mcx,
            root,
            run,
            sublink.subselect,
            sublink.subLinkType,
            sublink.subLinkId,
            testexpr,
            context.is_top_qual,
        );
    }

    // Don't recurse into the arguments of an outer PHV, Aggref, GroupingFunc, or
    // ReturningExpr here.
    match &node {
        Expr::PlaceHolderVar(phv) if phv.phlevelsup > 0 => return Ok(node),
        Expr::Aggref(agg) if agg.agglevelsup > 0 => return Ok(node),
        Expr::GroupingFunc(grp) if grp.agglevelsup > 0 => return Ok(node),
        Expr::ReturningExpr(rexpr) if rexpr.retlevelsup > 0 => return Ok(node),
        _ => {}
    }

    // We should never see a SubPlan expression in the input, nor a Query.
    debug_assert!(!matches!(node, Expr::SubPlan(_)));
    debug_assert!(!matches!(node, Expr::AlternativeSubPlan(_)));

    // Because make_subplan() could return an AND or OR clause, we have to take
    // steps to preserve AND/OR flatness of a qual.
    if is_andclause(&node) {
        let args = match node {
            Expr::BoolExpr(b) => b.args,
            _ => unreachable!(),
        };
        let mut newargs: alloc::vec::Vec<Expr> = alloc::vec::Vec::new();
        // Still at qual top-level.
        let loc_context = ProcessSublinksContext {
            is_top_qual: context.is_top_qual,
        };
        for arg in args {
            let newarg = process_sublinks_mutator(mcx, root, run, arg, &loc_context)?;
            if is_andclause(&newarg) {
                if let Expr::BoolExpr(b) = newarg {
                    newargs.extend(b.args);
                } else {
                    unreachable!()
                }
            } else {
                newargs.push(newarg);
            }
        }
        return Ok(make_andclause(newargs));
    }

    if is_orclause(&node) {
        let args = match node {
            Expr::BoolExpr(b) => b.args,
            _ => unreachable!(),
        };
        let mut newargs: alloc::vec::Vec<Expr> = alloc::vec::Vec::new();
        let loc_context = ProcessSublinksContext {
            is_top_qual: context.is_top_qual,
        };
        for arg in args {
            let newarg = process_sublinks_mutator(mcx, root, run, arg, &loc_context)?;
            if is_orclause(&newarg) {
                if let Expr::BoolExpr(b) = newarg {
                    newargs.extend(b.args);
                } else {
                    unreachable!()
                }
            } else {
                newargs.push(newarg);
            }
        }
        return Ok(make_orclause(newargs));
    }

    // If we recurse down through anything other than an AND or OR node, we are
    // definitely not at top qual level anymore.
    //
    // expression_tree_mutator(node, process_sublinks_mutator, &locContext)
    let mut err: Option<types_error::PgError> = None;
    let result = expression_tree_mutator(node, &mut |child: Expr| {
        if err.is_some() {
            return child;
        }
        let loc_context = ProcessSublinksContext {
            is_top_qual: false,
        };
        match process_sublinks_mutator(mcx, root, run, child, &loc_context) {
            Ok(c) => c,
            Err(e) => {
                err = Some(e);
                Expr::Const(nodes_core::makefuncs::make_bool_const(true, false))
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(result),
    }
}

// ===========================================================================
// SS_identify_outer_params
// ===========================================================================

/// `SS_identify_outer_params(root)` (subselect.c): identify the Params available
/// from outer levels and record them in `root.outer_params`.
pub fn SS_identify_outer_params(root: &mut PlannerInfo) {
    // If no parameters have been assigned anywhere in the tree, we certainly
    // don't need to do anything here.
    {
        let glob = root
            .glob
            .as_ref()
            .expect("SS_identify_outer_params: root->glob is NULL");
        if glob.param_exec_types.is_empty() {
            return;
        }
    }

    // Scan all query levels above this one. `root.parent_root` is the owned box
    // chain (C `PlannerInfo *parent_root`); we walk it by reference.
    let mut outer_params: pathnodes::Relids = None;
    let mut proot_opt = root.parent_root.as_deref();
    while let Some(proot) = proot_opt {
        // Include ordinary Var/PHV/Aggref/GroupingFunc/ReturningExpr params.
        for &ppl in &proot.plan_params {
            let pid = proot.planner_param_item(ppl).paramId;
            outer_params = bms::relids_add_member::call(outer_params, pid);
        }
        // Include any outputs of outer-level initPlans. Each `init_plans` entry
        // is a NodeId resolving to an `Expr::SubPlan`; read its `setParam`.
        for &ipl in &proot.init_plans {
            if let Expr::SubPlan(splan) = proot.node(ipl) {
                for sp in splan.0.setParam.iter() {
                    outer_params = bms::relids_add_member::call(outer_params, *sp);
                }
            }
        }
        // Include worktable ID, if a recursive query is being planned.
        if proot.wt_param_id >= 0 {
            outer_params = bms::relids_add_member::call(outer_params, proot.wt_param_id);
        }
        proot_opt = proot.parent_root.as_deref();
    }
    root.outer_params = outer_params;
}
