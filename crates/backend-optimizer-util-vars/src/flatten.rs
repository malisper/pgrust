//! `flatten_join_alias_vars` (+ `flatten_join_alias_vars_mutator` and its private
//! helpers) — optimizer/util/var.c:789-1382.
//!
//! Replace any join-alias `Var`s in an expression with the underlying
//! base-relation `Var`s, using the (outer) `Query`'s range table for the join
//! definitions. The C entry takes a `PlannerInfo *root` that may be NULL; at the
//! one call site this seam serves (`pull_up_simple_subquery`, prepjointree.c, via
//! the `flatten_join_alias_vars` seam) it is **always NULL**, so `root` is
//! modeled as absent throughout. The two NULL-`root`-only consequences are
//! faithfully preserved:
//!
//! * `add_nullingrels_if_needed`: with `root == NULL` and a non-"standard" join
//!   alias expression carrying nullingrels, the C `elog(ERROR, "unsupported join
//!   alias expression")` (the `else` arm) — there is no PlaceHolderVar fallback.
//! * `mark_nullable_by_grouping` / `flatten_group_exprs` are a *different* entry
//!   (`flatten_group_exprs`, called only with a real `root`) and are NOT part of
//!   this seam; they stay unported (see `lib.rs` "Genuine remaining gaps").
//!
//! The C mutators build a fresh tree (`copyObject` + `expression_tree_mutator`);
//! the repo's mutator model owns the tree in place (`&mut Node`), so a matching
//! join-alias `Var` is overwritten with its expansion by assigning through
//! `*node`. The expansions are values of the lifetime-free [`Expr`] tree, so they
//! write into a `Node` of any lifetime, matching the central `*_mut` walker whose
//! closure receives a node at a fresh (higher-ranked) lifetime. The outer
//! `Query`'s `'mcx` range table lives in the mutator's context.

#![allow(non_snake_case)]

extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;

use backend_nodes_core::node_walker::{expression_tree_walker_mut, query_tree_mutator};
use backend_optimizer_path_equivclass_ext_seams as equivclass_ext_seam;
use backend_rewrite_core::add_nulling_relids;
use backend_rewrite_core::increment::IncrementVarSublevelsUp;
use backend_rewrite_core::relids as expr_relids;
use backend_rewrite_core::walkers::checkExprHasSubLink;
use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_nodes::copy_query::Query;
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsenodes::RTEKind;
use types_nodes::primnodes::{CoercionForm, Expr, ExprRelids, RowExpr, Var};
use types_pathnodes::PlannerInfo;

const INVALID_ATTR_NUMBER: i16 = 0;

/// `flatten_join_alias_vars_context` (var.c:64-71). The `root` is NULL for the
/// parse_agg.c call sites but live for planner.c / prepjointree.c; it is read
/// only by `add_nullingrels_if_needed`'s PlaceHolderVar fallback. The `query`
/// is the outer `Query` whose range table defines the joins; `'mcx` is its
/// arena lifetime.
struct FlattenCtx<'a, 'mcx> {
    /// `PlannerInfo *root` — NULL (`None`) for the parser call sites; live for
    /// the planner / `pull_up_simple_subquery` sites (needed to build the PHV
    /// wrapper for a non-standard nullingrel-carrying join-alias expression).
    root: Option<&'a mut PlannerInfo>,
    query: &'a Query<'mcx>,
    sublevels_up: i32,
    /// could aliases include a SubLink?
    possible_sublink: bool,
    /// have we inserted a SubLink?
    inserted_sublink: bool,
}

/// `flatten_join_alias_vars(root, query, node)` (var.c:789).
///
/// We do not expect this to be applied to the whole `Query`, only to expressions
/// or LATERAL subqueries; hence if the top node is a `Query`, it's okay to
/// immediately increment `sublevels_up` (the mutator does so).
pub fn flatten_join_alias_vars<'mcx>(
    mcx: Mcx<'mcx>,
    root: Option<&mut PlannerInfo>,
    query: &Node<'mcx>,
    mut node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let query = match query.as_query() {
        Some(q) => q,
        None => {
            return Err(PgError::error(
                "flatten_join_alias_vars: query argument is not a Query node",
            ))
        }
    };
    let mut context = FlattenCtx {
        root,
        query,
        sublevels_up: 0,
        // flag whether join aliases could possibly contain SubLinks
        possible_sublink: query.hasSubLinks,
        // if hasSubLinks is already true, no need to work hard
        inserted_sublink: query.hasSubLinks,
    };

    flatten_join_alias_vars_mutator(mcx, &mut node, &mut context)?;
    Ok(node)
}

/// `flatten_join_alias_vars_mutator(node, context)` (var.c:812). Mutates `node`
/// in place (the owned-tree analogue of the C copy-and-return). Returns `Ok(())`
/// on success; `Err` carries the rewrite `ereport(ERROR)` surface.
fn flatten_join_alias_vars_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    context: &mut FlattenCtx<'_, 'mcx>,
) -> PgResult<()> {
    // Classify the node by peeling the `Expr` first (dual-homed-tag rule); the
    // `Var` / `PlaceHolderVar` legs then dispatch the inner `Expr` enum.
    let is_var = matches!(node.as_expr(), Some(Expr::Var(_)));
    let is_phv = matches!(node.as_expr(), Some(Expr::PlaceHolderVar(_)));
    let is_query = node.node_tag() == ntag::T_Query;
    if is_var {
        {
            let var = match node.as_expr() {
                Some(Expr::Var(v)) => v.clone(),
                _ => unreachable!(),
            };

            // No change unless Var belongs to a JOIN of the target level.
            if var.varlevelsup as i32 != context.sublevels_up {
                return Ok(()); // no need to copy, really
            }
            let rte = rt_fetch(context.query, var.varno)?;
            if rte.rtekind != RTEKind::RTE_JOIN {
                return Ok(());
            }

            if var.varattno == INVALID_ATTR_NUMBER {
                // Must expand whole-row reference.
                let mut fields: Vec<Expr> = Vec::new();
                let mut colnames: Vec<String> = Vec::new();

                debug_assert_eq!(
                    rte.joinaliasvars.len(),
                    rte.eref
                        .as_deref()
                        .map(|a| a.colnames.len())
                        .unwrap_or(0)
                );

                let eref_colnames: &[types_nodes::nodes::NodePtr<'mcx>] = match rte.eref.as_deref()
                {
                    Some(a) => &a.colnames,
                    None => &[],
                };

                for (lv_idx, lv) in rte.joinaliasvars.iter().enumerate() {
                    // Ignore dropped columns. In C these are NULL `joinaliasvars`
                    // list elements; in the owned model a dropped column is a null
                    // `Const` placeholder (see parse_relation.c
                    // `get_rte_attribute_is_dropped`), so skip those.
                    if is_dropped_alias_var(&**lv) {
                        continue;
                    }
                    let aliasvar_expr = match lv.as_expr() {
                        Some(e) => e.clone(),
                        None => {
                            return Err(PgError::error(
                                "flatten_join_alias_vars: join alias var is not an expression",
                            ))
                        }
                    };

                    let mut newvar = Node::mk_expr(mcx, aliasvar_expr)?;

                    // If we are expanding an alias carried down from an upper
                    // query, must adjust its varlevelsup fields.
                    if context.sublevels_up != 0 {
                        IncrementVarSublevelsUp(&mut newvar, context.sublevels_up, 0, mcx)?;
                    }
                    // Preserve original Var's location, if possible.
                    if let Some(nv) = newvar.as_var_mut() {
                        nv.location = var.location;
                    }
                    // Recurse in case join input is itself a join.
                    // (also takes care of setting inserted_sublink if needed)
                    flatten_join_alias_vars_mutator(mcx, &mut newvar, context)?;
                    let field_expr = match newvar.into_expr() {
                        Some(e) => e,
                        None => {
                            return Err(PgError::error(
                                "flatten_join_alias_vars: join alias expansion is not an expression",
                            ))
                        }
                    };
                    fields.push(field_expr);
                    // We need the names of non-dropped columns, too.
                    let cn = eref_colnames
                        .get(lv_idx)
                        .and_then(|n| n.as_string().map(|s| String::from(s.sval.as_str())))
                        .unwrap_or_default();
                    colnames.push(cn);
                }

                let rowexpr = RowExpr {
                    args: fields,
                    row_typeid: var.vartype,
                    row_format: CoercionForm::COERCE_IMPLICIT_CAST,
                    // vartype will always be RECORDOID, so we always need colnames.
                    colnames,
                    location: var.location,
                };

                // Lastly, add any varnullingrels to the replacement expression.
                let replacement = add_nullingrels_if_needed(
                    mcx,
                    context,
                    Node::mk_expr(mcx, Expr::RowExpr(rowexpr))?,
                    &var,
                )?;
                *node = replacement;
                return Ok(());
            }

            // Expand join alias reference.
            debug_assert!(var.varattno > 0);
            let aliasvar = rte
                .joinaliasvars
                .get((var.varattno - 1) as usize)
                .ok_or_else(|| {
                    PgError::error("flatten_join_alias_vars: join alias var index out of range")
                })?;
            let aliasvar_expr = match aliasvar.as_expr() {
                Some(e) => e.clone(),
                None => {
                    return Err(PgError::error(
                        "flatten_join_alias_vars: join alias var is not an expression",
                    ))
                }
            };
            let mut newvar = Node::mk_expr(mcx, aliasvar_expr)?;

            // If we are expanding an alias carried down from an upper query, must
            // adjust its varlevelsup fields.
            if context.sublevels_up != 0 {
                IncrementVarSublevelsUp(&mut newvar, context.sublevels_up, 0, mcx)?;
            }

            // Preserve original Var's location, if possible.
            if let Some(nv) = newvar.as_var_mut() {
                nv.location = var.location;
            }

            // Recurse in case join input is itself a join.
            flatten_join_alias_vars_mutator(mcx, &mut newvar, context)?;

            // Detect if we are adding a sublink to query.
            if context.possible_sublink && !context.inserted_sublink {
                context.inserted_sublink = checkExprHasSubLink(&newvar);
            }

            // Lastly, add any varnullingrels to the replacement expression.
            *node = add_nullingrels_if_needed(mcx, context, newvar, &var)?;
            Ok(())
        }
    } else if is_phv {
        // Copy the PlaceHolderVar node with correct mutation of subnodes: the
        // C `expression_tree_mutator(node, ...)` recurses into the PHV's
        // phexpr. The repo's generic in-place walker does not descend into a
        // PlaceHolderVar's phexpr, so do it explicitly here (matching C).
        if let Some(Expr::PlaceHolderVar(phv)) = node.as_expr_mut() {
            if let Some(phexpr) = phv.phexpr.as_deref_mut() {
                // copyObject shape: the phexpr may carry a SubLink/SubPlan whose
                // derived `Expr::clone` panics; deep-copy through `clone_in`.
                let mut child = Node::mk_expr(mcx, phexpr.clone_in(mcx)?)?;
                flatten_join_alias_vars_mutator(mcx, &mut child, context)?;
                if let Some(e) = child.into_expr() {
                    if let Some(Expr::PlaceHolderVar(phv)) = node.as_expr_mut() {
                        if let Some(phexpr) = phv.phexpr.as_deref_mut() {
                            *phexpr = e;
                        }
                    }
                }
            }
        }
        // Now fix PlaceHolderVar's relid sets.
        if let Some(Expr::PlaceHolderVar(phv)) = node.as_expr() {
            if phv.phlevelsup as i32 == context.sublevels_up {
                let newrels = alias_relid_set(mcx, context.query, &phv.phrels)?;
                if let Some(Expr::PlaceHolderVar(phv)) = node.as_expr_mut() {
                    phv.phrels = newrels;
                    // we *don't* change phnullingrels
                }
            }
        }
        Ok(())
    } else if is_query {
        let q = node.as_query_mut().unwrap();
        // Recurse into RTE subquery or not-yet-planned sublink subquery.
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
                match flatten_join_alias_vars_mutator(mcx, n, context) {
                    Ok(()) => false,
                    Err(e) => {
                        err = Some(e);
                        true
                    }
                }
            },
            backend_nodes_core::node_walker::QTW_IGNORE_JOINALIASES,
            mcx,
        );
        if let Some(e) = err {
            return Err(e);
        }
        q.hasSubLinks |= context.inserted_sublink;
        context.inserted_sublink = save_inserted_sublink;
        context.sublevels_up -= 1;
        Ok(())
    } else {
        // Already-planned tree not supported (SubPlan / AlternativeSubPlan)
        // and planner auxiliary nodes (SpecialJoinInfo / PlaceHolderInfo /
        // MinMaxAggInfo) shouldn't appear here; the central walker has no arms
        // for them, so recursion over the remaining node types is correct.
        let mut err: Option<PgError> = None;
        expression_tree_walker_mut(
            node,
            &mut |n| {
                if err.is_some() {
                    return true;
                }
                match flatten_join_alias_vars_mutator(mcx, n, context) {
                    Ok(()) => false,
                    Err(e) => {
                        err = Some(e);
                        true
                    }
                }
            },
            mcx,
        );
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// A dropped column in a JOIN RTE's `joinaliasvars`. In C this is a NULL list
/// element; in the owned model it is a null `Const` placeholder (matching
/// parse_relation.c `get_rte_attribute_is_dropped`).
fn is_dropped_alias_var(node: &Node) -> bool {
    matches!(node.as_expr(), Some(Expr::Const(c)) if c.constisnull)
}

/// `rt_fetch(varno, query->rtable)` (parsetree.h) — one-based range-table fetch.
fn rt_fetch<'a, 'mcx>(
    query: &'a Query<'mcx>,
    varno: i32,
) -> PgResult<&'a types_nodes::parsenodes::RangeTblEntry<'mcx>> {
    query
        .rtable
        .get((varno - 1) as usize)
        .ok_or_else(|| PgError::error("flatten_join_alias_vars: rangetable index out of range"))
}

/// `add_nullingrels_if_needed(root, newnode, oldvar)` (var.c:1176). If `oldvar`
/// carries no nullingrels, do nothing; else if the expansion is a "standard"
/// join alias expression, push the nullingrels into it in place; else if `root`
/// is live, insert a PlaceHolderVar to carry the nullingrels (the C non-NULL
/// `else if (root)` arm — evaluate the PHV at the natural semantic level of the
/// new expression, falling back to the join's relids when the expression is
/// variable-free); else (NULL `root`, parser call sites) raise the C final
/// `else` arm `elog(ERROR, "unsupported join alias expression")`.
fn add_nullingrels_if_needed<'mcx>(
    mcx: Mcx<'mcx>,
    context: &mut FlattenCtx<'_, 'mcx>,
    mut newnode: Node<'mcx>,
    oldvar: &Var,
) -> PgResult<Node<'mcx>> {
    if expr_relids::is_empty(&oldvar.varnullingrels) {
        return Ok(newnode); // nothing to do
    }
    // If possible, do it by adding to existing nullingrel fields.
    if is_standard_join_alias_expression(&newnode, oldvar) {
        adjust_standard_join_alias_expression(&mut newnode, oldvar);
    } else if context.root.is_some() {
        // We can insert a PlaceHolderVar to carry the nullingrels. Deciding
        // where to evaluate the PHV is slightly tricky: first try the natural
        // semantic level of the new expression, but if that expression is
        // variable-free fall back to the join that oldvar is an alias Var for.
        let levelsup = oldvar.varlevelsup;
        let mut phrels = {
            let root_ref = context.root.as_deref().unwrap();
            let relids_bms =
                crate::var::pull_varnos_of_level(Some(root_ref), &newnode, levelsup as i32);
            bms_to_expr_relids(relids_bms.as_deref())
        };

        if expr_relids::is_empty(&phrels) {
            // variable-free?
            if levelsup != 0 {
                // this won't work otherwise
                return Err(PgError::error("unsupported join alias expression"));
            }
            // phrels = get_relids_for_join(root->parse, oldvar->varno);
            let join_relids =
                backend_optimizer_prep_prepjointree_seams::get_relids_for_join::call(
                    mcx,
                    context.query,
                    oldvar.varno,
                )?;
            // If it's an outer join, eval below not above the join.
            phrels = expr_relids::del_member(join_relids, oldvar.varno);
            debug_assert!(!expr_relids::is_empty(&phrels));
        }

        let expr_val = match newnode.into_expr() {
            Some(e) => e,
            None => {
                return Err(PgError::error(
                    "add_nullingrels_if_needed: replacement is not an expression",
                ))
            }
        };
        // make_placeholder_expr builds over the planner arena's notional
        // `'static`; feed it the erased arena-interned expr and re-localize the
        // resulting PlaceHolderVar into the local `mcx` via `clone_in`.
        let root_mut = context.root.as_deref_mut().unwrap();
        let mut newphv = backend_optimizer_util_placeholder_seams::make_placeholder_expr::call(
            root_mut,
            expr_val.erase_lifetime(),
            expr_relids_to_relids(&phrels),
        )
        .clone_in(mcx)?;
        // newphv has zero phlevelsup and NULL phnullingrels; fix it.
        newphv.phlevelsup = levelsup;
        newphv.phnullingrels = expr_relids::copy(&oldvar.varnullingrels);
        newnode = Node::mk_expr(mcx, Expr::PlaceHolderVar(newphv))?;
    } else {
        // root is NULL (parser call sites): "ooops, we're missing support for
        // something the parser can make".
        return Err(PgError::error("unsupported join alias expression"));
    }
    Ok(newnode)
}

/// `is_standard_join_alias_expression(newnode, oldvar)` (var.c:1228). Can we
/// insert nullingrels into this join alias expression without a separate
/// PlaceHolderVar? Handles `Var`s, `PlaceHolderVar`s, and implicit-coercion /
/// COALESCE expressions built from those.
fn is_standard_join_alias_expression(newnode: &Node, oldvar: &Var) -> bool {
    let expr = match newnode.as_expr() {
        Some(e) => e,
        None => return false,
    };
    is_standard_expr(expr, oldvar)
}

fn is_standard_expr(expr: &Expr, oldvar: &Var) -> bool {
    match expr {
        Expr::Var(v) => v.varlevelsup == oldvar.varlevelsup,
        Expr::PlaceHolderVar(phv) => phv.phlevelsup == oldvar.varlevelsup,
        Expr::FuncExpr(fexpr) => {
            // Assume an implicit coercion wouldn't produce non-NULL from NULL.
            if fexpr.funcformat != CoercionForm::COERCE_IMPLICIT_CAST || fexpr.args.is_empty() {
                return false;
            }
            // Examine only the first argument (coercions might have additional
            // constant arguments).
            is_standard_expr(&fexpr.args[0], oldvar)
        }
        Expr::RelabelType(relabel) => match relabel.arg.as_deref() {
            Some(a) => is_standard_expr(a, oldvar),
            None => false,
        },
        Expr::CoerceViaIO(iocoerce) => match iocoerce.arg.as_deref() {
            Some(a) => is_standard_expr(a, oldvar),
            None => false,
        },
        Expr::ArrayCoerceExpr(acoerce) => match acoerce.arg.as_deref() {
            Some(a) => is_standard_expr(a, oldvar),
            None => false,
        },
        Expr::CoalesceExpr(cexpr) => {
            debug_assert!(!cexpr.args.is_empty());
            cexpr.args.iter().all(|a| is_standard_expr(a, oldvar))
        }
        _ => false,
    }
}

/// `adjust_standard_join_alias_expression(newnode, oldvar)` (var.c:1302). Insert
/// nullingrels into an expression accepted by `is_standard_join_alias_expression`.
fn adjust_standard_join_alias_expression(newnode: &mut Node, oldvar: &Var) {
    if let Some(e) = newnode.as_expr_mut() {
        adjust_standard_expr(e, oldvar);
    }
}

fn adjust_standard_expr(expr: &mut Expr, oldvar: &Var) {
    match expr {
        Expr::Var(newvar) if newvar.varlevelsup == oldvar.varlevelsup => {
            newvar.varnullingrels =
                expr_relids::union(&newvar.varnullingrels, &oldvar.varnullingrels);
        }
        Expr::PlaceHolderVar(newphv) if newphv.phlevelsup == oldvar.varlevelsup => {
            newphv.phnullingrels =
                expr_relids::union(&newphv.phnullingrels, &oldvar.varnullingrels);
        }
        Expr::FuncExpr(fexpr) => {
            if let Some(first) = fexpr.args.first_mut() {
                adjust_standard_expr(first, oldvar);
            }
        }
        Expr::RelabelType(relabel) => {
            if let Some(a) = relabel.arg.as_deref_mut() {
                adjust_standard_expr(a, oldvar);
            }
        }
        Expr::CoerceViaIO(iocoerce) => {
            if let Some(a) = iocoerce.arg.as_deref_mut() {
                adjust_standard_expr(a, oldvar);
            }
        }
        Expr::ArrayCoerceExpr(acoerce) => {
            if let Some(a) = acoerce.arg.as_deref_mut() {
                adjust_standard_expr(a, oldvar);
            }
        }
        Expr::CoalesceExpr(cexpr) => {
            for a in cexpr.args.iter_mut() {
                adjust_standard_expr(a, oldvar);
            }
        }
        // Var/PHV at a different level, or another node type: C `Assert(false)`.
        // Unreachable for inputs that passed is_standard_join_alias_expression.
        _ => debug_assert!(false, "adjust_standard_join_alias_expression: non-standard node"),
    }
}

/// `alias_relid_set(query, relids)` (var.c:1364): in a set of RT indexes, replace
/// joins by their underlying base+OJ relids (`get_relids_for_join`, prepjointree.c
/// — reached via the `get_relids_for_join` seam). `relids` is the PHV's `phrels`
/// (lifetime-free [`ExprRelids`] words).
fn alias_relid_set<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    relids: &ExprRelids,
) -> PgResult<ExprRelids> {
    let mut result = ExprRelids::default();
    let mut rtindex: i32 = -1;
    while let Some(next) = expr_relids::next_member(relids, rtindex) {
        rtindex = next;
        let rte = rt_fetch(query, rtindex)?;
        if rte.rtekind == RTEKind::RTE_JOIN {
            let join_relids = backend_optimizer_prep_prepjointree_seams::get_relids_for_join::call(
                mcx, query, rtindex,
            )?;
            result = expr_relids::union(&result, &join_relids);
        } else {
            result = expr_relids::add_member(result, rtindex);
        }
    }
    Ok(result)
}

// ===========================================================================
// flatten_group_exprs (var.c:972) — replace Vars referencing GROUP outputs with
// the underlying grouping expressions. Unlike flatten_join_alias_vars this
// always runs with a real `root` (needed to preserve varnullingrels).
// ===========================================================================

/// `flatten_group_exprs_mutator` context (var.c:64-71). Carries the real `root`
/// (for `mark_nullable_by_grouping`) plus the `query` whose range table holds
/// the RTE_GROUP entry.
struct FlattenGroupCtx<'a, 'mcx> {
    // C `context.root` is NULL for the ruleutils.c deparse and
    // check_output_expressions() use-cases (root is only needed to preserve
    // varnullingrels, which those callers don't care about).
    root: Option<&'a mut PlannerInfo>,
    query: &'a Query<'mcx>,
    sublevels_up: i32,
    possible_sublink: bool,
    inserted_sublink: bool,
}

/// `flatten_group_exprs(root, query, node)` (var.c:972). Replaces Vars that
/// reference GROUP outputs (an `RTE_GROUP` range-table entry) with the
/// underlying grouping expressions from `rte->groupexprs`.
///
/// The top node is never the whole `Query` (the C `Assert(node != query)`); it
/// is the targetList/havingQual (an expression or list of expressions).
pub fn flatten_group_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    root: Option<&mut PlannerInfo>,
    query: &Query<'mcx>,
    mut node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut context = FlattenGroupCtx {
        root,
        query,
        sublevels_up: 0,
        // flag whether grouping expressions could possibly contain SubLinks
        possible_sublink: query.hasSubLinks,
        // if hasSubLinks is already true, no need to work hard
        inserted_sublink: query.hasSubLinks,
    };
    flatten_group_exprs_mutator(mcx, &mut node, &mut context)?;
    Ok(node)
}

/// Seam shim for the `root == NULL` use-cases (ruleutils.c deparse and
/// check_output_expressions()). Clones the input node into `mcx`, runs the
/// flattener with a NULL root, and returns the result boxed. Installed into
/// `backend_utils_adt_ruleutils_seams::flatten_group_exprs`.
fn seam_flatten_group_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    node: &Node<'mcx>,
) -> PgResult<mcx::PgBox<'mcx, Node<'mcx>>> {
    let owned = node.clone_in(mcx)?;
    let flattened = flatten_group_exprs(mcx, None, query, owned)?;
    mcx::alloc_in(mcx, flattened)
}

pub(crate) fn init_seams() {
    backend_utils_adt_ruleutils_seams::flatten_group_exprs::set(seam_flatten_group_exprs);
}

fn flatten_group_exprs_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    context: &mut FlattenGroupCtx<'_, 'mcx>,
) -> PgResult<()> {
    // Classify by peeling the `Expr` first (dual-homed-tag rule).
    let inner_tag = match node.as_expr() {
        Some(Expr::Var(_)) => 1,
        Some(Expr::Aggref(_)) => 2,
        Some(Expr::GroupingFunc(_)) => 3,
        _ => 0,
    };
    if inner_tag == 1 {
        let var: Var = match node.as_expr() {
            Some(Expr::Var(v)) => v.clone(),
            _ => unreachable!(),
        };

        // No change unless Var belongs to the GROUP of the target level.
        if var.varlevelsup as i32 != context.sublevels_up {
            return Ok(()); // no need to copy, really
        }
        let rte = rt_fetch(context.query, var.varno)?;
        if rte.rtekind != RTEKind::RTE_GROUP {
            return Ok(());
        }

        // Expand group exprs reference: newvar = list_nth(rte->groupexprs,
        // varattno-1). varattno > 0 (parser invariant).
        debug_assert!(var.varattno > 0);
        let idx = (var.varattno - 1) as usize;
        let src = rte.groupexprs.get(idx).ok_or_else(|| {
            PgError::error("flatten_group_exprs: groupexpr index out of range")
        })?;
        let group_expr = match src.as_expr() {
            Some(e) => e.clone_in(mcx)?, // copyObject(newvar)
            None => {
                return Err(PgError::error(
                    "flatten_group_exprs: groupexpr is not an expression",
                ))
            }
        };
        let mut newvar = Node::mk_expr(mcx, group_expr)?;

        // If expanding an expr carried down from an upper query, adjust its
        // varlevelsup fields.
        if context.sublevels_up != 0 {
            IncrementVarSublevelsUp(&mut newvar, context.sublevels_up, 0, mcx)?;
        }

        // Preserve original Var's location, if possible.
        if let Some(nv) = newvar.as_var_mut() {
            nv.location = var.location;
        }

        // Detect if we are adding a sublink to query.
        if context.possible_sublink && !context.inserted_sublink {
            context.inserted_sublink = checkExprHasSubLink(&newvar);
        }

        // Lastly, add any varnullingrels to the replacement expression.
        *node = mark_nullable_by_grouping(
            mcx,
            context.root.as_deref_mut(),
            context.query,
            newvar,
            &var,
        )?;
        Ok(())
    } else if inner_tag == 2 {
        let agglevelsup = match node.as_expr() {
            Some(Expr::Aggref(a)) => a.agglevelsup as i32,
            _ => unreachable!(),
        };
        if agglevelsup == context.sublevels_up {
            // Aggregate of the original level: do not recurse into its
            // normal args / ORDER BY / filter (no grouped vars there), but
            // check direct args as though not in an aggregate.
            if let Some(Expr::Aggref(agg)) = node.as_expr_mut() {
                let mut dargs = core::mem::take(&mut agg.aggdirectargs);
                for e in dargs.iter_mut() {
                    let owned = core::mem::replace(e, Expr::Const(Default::default()));
                    let mut wrapped = Node::mk_expr(mcx, owned)?;
                    flatten_group_exprs_mutator(mcx, &mut wrapped, context)?;
                    if let Some(ne) = wrapped.into_expr() {
                        *e = ne;
                    }
                }
                if let Some(Expr::Aggref(agg)) = node.as_expr_mut() {
                    agg.aggdirectargs = dargs;
                }
            }
            return Ok(());
        }
        if agglevelsup > context.sublevels_up {
            // Aggregates of higher levels cannot contain Vars of concern.
            return Ok(());
        }
        // Lower-level aggregate: fall through to generic recursion below.
        generic_recurse(mcx, node, context)
    } else if inner_tag == 3 {
        let agglevelsup = match node.as_expr() {
            Some(Expr::GroupingFunc(g)) => g.agglevelsup as i32,
            _ => unreachable!(),
        };
        // GroupingFunc of the original or higher level: no grouped vars in
        // its arguments.
        if agglevelsup >= context.sublevels_up {
            return Ok(());
        }
        generic_recurse(mcx, node, context)
    } else if node.node_tag() == ntag::T_Query {
        let q = node.as_query_mut().unwrap();
        // Recurse into RTE subquery or not-yet-planned sublink subquery.
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
                match flatten_group_exprs_mutator(mcx, n, context) {
                    Ok(()) => false,
                    Err(e) => {
                        err = Some(e);
                        true
                    }
                }
            },
            backend_nodes_core::node_walker::QTW_IGNORE_GROUPEXPRS,
            mcx,
        );
        if let Some(e) = err {
            return Err(e);
        }
        q.hasSubLinks |= context.inserted_sublink;
        context.inserted_sublink = save_inserted_sublink;
        context.sublevels_up -= 1;
        Ok(())
    } else {
        generic_recurse(mcx, node, context)
    }
}

/// Generic `expression_tree_mutator(node, flatten_group_exprs_mutator, context)`
/// recursion over a node's expression children (in place).
fn generic_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    context: &mut FlattenGroupCtx<'_, 'mcx>,
) -> PgResult<()> {
    let mut err: Option<PgError> = None;
    expression_tree_walker_mut(
        node,
        &mut |n| {
            if err.is_some() {
                return true;
            }
            match flatten_group_exprs_mutator(mcx, n, context) {
                Ok(()) => false,
                Err(e) => {
                    err = Some(e);
                    true
                }
            }
        },
        mcx,
    );
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `mark_nullable_by_grouping(root, newnode, oldvar)` (var.c:1107). Add
/// `oldvar`'s `varnullingrels`, if any, to the flattened grouping expression
/// `newnode` (already copied, so freely mutable). For the common case (no
/// nullingrels — no GROUPING SETS and no grouping under an outer join) the
/// replacement is returned unchanged.
fn mark_nullable_by_grouping<'mcx>(
    mcx: Mcx<'mcx>,
    root: Option<&mut PlannerInfo>,
    query: &Query<'mcx>,
    mut newnode: Node<'mcx>,
    oldvar: &Var,
) -> PgResult<Node<'mcx>> {
    // C: if (root == NULL) return newnode; — the ruleutils.c deparse and
    // check_output_expressions() callers pass root == NULL and don't need
    // varnullingrels preserved.
    let root = match root {
        Some(r) => r,
        None => return Ok(newnode),
    };
    if expr_relids::is_empty(&oldvar.varnullingrels) {
        return Ok(newnode); // nothing to do
    }

    // Assert(bms_equal(oldvar->varnullingrels,
    //                  bms_make_singleton(root->group_rtindex)));
    debug_assert!({
        let singleton = expr_relids::add_member(
            ExprRelids { words: Vec::new() },
            root.group_rtindex,
        );
        expr_relids::is_empty(&expr_relids::difference(&oldvar.varnullingrels, &singleton))
            && expr_relids::is_empty(&expr_relids::difference(&singleton, &oldvar.varnullingrels))
    });

    // relids = pull_varnos_of_level(root, newnode, oldvar->varlevelsup);
    let relids_bms = crate::var::pull_varnos_of_level(
        Some(root),
        &newnode,
        oldvar.varlevelsup as i32,
    );
    let relids = bms_to_expr_relids(relids_bms.as_deref());

    if !expr_relids::is_empty(&relids) {
        // If the newnode is not variable-free, set the nullingrels of the Vars /
        // PHVs contained in the expression.
        add_nulling_relids(&mut newnode, Some(&relids), &oldvar.varnullingrels, mcx);
    } else {
        // Variable-free: if it contains neither volatile functions nor
        // set-returning functions, wrap it in a new PlaceHolderVar carrying the
        // nullingrels. Aggregate / window functions are not allowed in grouping
        // expressions (the C asserts this).
        let expr_ref = newnode.as_expr();
        debug_assert!(!equivclass_ext_seam::contain_agg_clause::call(
            expr_ref.expect("mark_nullable_by_grouping: grouping expr is not an Expr")
        ));
        debug_assert!(!equivclass_ext_seam::contain_window_function::call(
            expr_ref.expect("mark_nullable_by_grouping: grouping expr is not an Expr")
        ));

        let expr = newnode
            .as_expr()
            .ok_or_else(|| PgError::error("mark_nullable_by_grouping: grouping expr is not an Expr"))?;
        if !equivclass_ext_seam::contain_volatile_functions::call(expr)
            && !equivclass_ext_seam::expression_returns_set::call(expr)
        {
            // phrels = get_relids_in_jointree((Node *) root->parse->jointree,
            //                                 true, false);
            let phrels_expr =
                backend_optimizer_prep_prepjointree_seams::get_relids_in_query_jointree::call(
                    mcx, query,
                )?;
            debug_assert!(!expr_relids::is_empty(&phrels_expr));
            let phrels = expr_relids_to_relids(&phrels_expr);

            let expr_val = match newnode.into_expr() {
                Some(e) => e,
                None => unreachable!(),
            };
            // The placeholder builder is declared over the planner arena's
            // notional `'static`; feed it the (erased) arena-interned expr and
            // re-localize the resulting `PlaceHolderVar` into the local `mcx` via
            // `clone_in` so the rebuilt `Node<'mcx>` is arena-correct.
            let mut newphv =
                backend_optimizer_util_placeholder_seams::make_placeholder_expr::call(
                    root,
                    expr_val.erase_lifetime(),
                    phrels,
                )
                .clone_in(mcx)?;
            // newphv has zero phlevelsup and NULL phnullingrels; fix it.
            newphv.phlevelsup = oldvar.varlevelsup;
            newphv.phnullingrels = expr_relids::copy(&oldvar.varnullingrels);
            newnode = Node::mk_expr(mcx, Expr::PlaceHolderVar(newphv))?;
        }
    }

    Ok(newnode)
}

/// `expr_relids_to_relids(er)` — the [`ExprRelids`] word-vector form (carried on
/// Var/PHV fields) viewed as the `'mcx`-free [`Relids`] (`Bitmapset`) the
/// PlannerInfo-facing seams (`make_placeholder_expr`) accept. Empty word vector
/// maps to the empty (NULL) set.
fn expr_relids_to_relids(er: &ExprRelids) -> types_pathnodes::Relids {
    if er.words.iter().all(|&w| w == 0) {
        None
    } else {
        Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset {
            words: er.words.clone(),
        }))
    }
}

/// Convert an `'mcx` [`Bitmapset`] to the lifetime-free [`ExprRelids`] word
/// vector (Var/PHV relids fields carry [`ExprRelids`]).
fn bms_to_expr_relids(a: Option<&types_pathnodes::Bitmapset>) -> ExprRelids {
    match a {
        None => ExprRelids { words: Vec::new() },
        Some(bms) => ExprRelids {
            words: bms.words.clone(),
        },
    }
}
