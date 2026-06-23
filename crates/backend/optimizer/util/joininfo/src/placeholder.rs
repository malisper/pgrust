//! `optimizer/util/placeholder.c` — PlaceHolderVar / PlaceHolderInfo routines.

use alloc::vec::Vec;

use types_error::{PgError, PgResult};
use ::nodes::primnodes::{Expr, ExprRelids, PlaceHolderVar};
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{
    NodeId, PhInfoId, PlaceHolderInfo, PlannerInfo, Relids, SpecialJoinInfo,
};

use crate::bms;
use crate::ext_seam;

// pull_var_clause flags (optimizer.h).
const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_INCLUDE_PLACEHOLDERS: i32 = 0x0010;

/// Convert an [`ExprRelids`] (carried on a `PlaceHolderVar`) into a [`Relids`].
fn expr_relids_to_relids(er: &ExprRelids) -> Relids {
    if er.words.iter().all(|&w| w == 0) {
        None
    } else {
        Some(alloc::boxed::Box::new(::pathnodes::Bitmapset {
            words: er.words.clone(),
        }))
    }
}

/// Convert a [`Relids`] into an [`ExprRelids`] for storage on a node.
fn relids_to_expr_relids(r: &Relids) -> ExprRelids {
    match r {
        None => ExprRelids { words: Vec::new() },
        Some(b) => ExprRelids {
            words: b.words.clone(),
        },
    }
}

/// `make_placeholder_expr`
///		Make a PlaceHolderVar for the given expression.
///
/// `phrels` is the syntactic location (as a set of relids) to attribute to the
/// expression. The caller is responsible for adjusting phlevelsup and
/// phnullingrels. Touches only `root->glob`.
pub fn make_placeholder_expr(
    root: &mut PlannerInfo,
    expr: Expr<'static>,
    phrels: Relids,
) -> PlaceHolderVar<'static> {
    let glob = root
        .glob
        .as_mut()
        .expect("make_placeholder_expr: root->glob is NULL");
    glob.last_ph_id += 1;
    let phid = glob.last_ph_id;

    PlaceHolderVar {
        phexpr: Some(alloc::boxed::Box::new(expr)),
        phrels: relids_to_expr_relids(&phrels),
        phnullingrels: ExprRelids { words: Vec::new() }, // caller may change later
        phid,
        phlevelsup: 0, // caller may change later
    }
}

/// `find_placeholder_info`
///		Fetch (or, if missing, create) the PlaceHolderInfo for the given PHV.
///
/// `mcx` is the planner-run context the freshly-created `PlaceHolderInfo`'s
/// deep copy (`copyObject(phv)`) is interned into; it must outlive the call's
/// `'a` brand (the planner arena governs validity, not Rust's borrow tracker).
pub fn find_placeholder_info<'a>(
    mcx: mcx::Mcx<'_>,
    root: &mut PlannerInfo,
    phv: &PlaceHolderVar<'a>,
) -> PgResult<PhInfoId> {
    // If this ever isn't true, we'd need to look in parent lists.
    debug_assert!(phv.phlevelsup == 0);

    // Use placeholder_array to look up existing PlaceHolderInfo quickly.
    let existing = if (phv.phid as i32) < root.placeholder_array_size {
        root.placeholder_array
            .get(phv.phid as usize)
            .copied()
            .flatten()
    } else {
        None
    };
    if let Some(phinfo) = existing {
        debug_assert!(root.phinfo(phinfo).phid == phv.phid);
        return Ok(phinfo);
    }

    // Not found, so create it.
    if root.placeholdersFrozen {
        return Err(PgError::error("too late to create a new PlaceHolderInfo"));
    }

    // ph_var = copyObject(phv) with phnullingrels forced empty (placeholder.c
    // convention: the PlaceHolderInfo represents the initially-calculated state).
    // copyObject(phv): deep-copy via `clone_in` (the derived `Clone` panics when
    // `phexpr` carries an owned-subtree variant, e.g. a SubPlan from an
    // `(SELECT ...)` SubLink in a pulled-up subquery), then erase the clone to the
    // planner-run arena's notional 'static at this sanctioned intern boundary.
    let mut ph_var = ::nodes::primnodes::placeholdervar_into_static(
        phv.clone_in(mcx)
            .map_err(|e| PgError::error(alloc::format!("find_placeholder_info: clone_in: {e:?}")))?,
    );
    ph_var.phnullingrels = ExprRelids { words: Vec::new() };

    // Deep-copy the (already-arena-owned) phexpr subtree via `clone_in` — the
    // derived `Expr::clone` panics if it carries a SubPlan.
    let phexpr: Expr<'static> = ::nodes::primnodes::Expr::erase_lifetime(
        ph_var
            .phexpr
            .as_ref()
            .expect("find_placeholder_info: PHV has no phexpr")
            .as_ref()
            .clone_in(mcx)
            .map_err(|e| PgError::error(alloc::format!("find_placeholder_info: clone_in: {e:?}")))?,
    );

    // Any referenced rels outside the PHV's syntactic scope are LATERAL refs
    // (ph_lateral, not ph_eval_at). If no referenced rels are within the
    // syntactic scope, force evaluation at the syntactic location.
    let rels_used = ext_seam::pull_varnos_expr::call(root, &phexpr);
    let phrels = expr_relids_to_relids(&phv.phrels);
    let ph_lateral = bms::relids_difference::call(&rels_used, &phrels);
    let mut ph_eval_at = bms::relids_int_members::call(rels_used, &phrels);
    if bms::relids_is_empty::call(&ph_eval_at) {
        ph_eval_at = bms::relids_copy::call(&phrels);
        debug_assert!(!bms::relids_is_empty::call(&ph_eval_at));
    }

    // estimate width using just the datatype info.
    let typid = ext_seam::expr_type::call(&phexpr);
    let typmod = ext_seam::expr_typmod::call(&phexpr);
    let ph_width = lsyscache_seams::get_typavgwidth::call(typid, typmod)?;

    // Intern phexpr into the node arena for the consumer-facing handle mirror.
    let phexpr_for_arena: Expr<'static> = ::nodes::primnodes::Expr::erase_lifetime(
        phexpr
            .clone_in(mcx)
            .map_err(|e| PgError::error(alloc::format!("find_placeholder_info: clone_in: {e:?}")))?,
    );
    let ph_var_phexpr: NodeId = root.alloc_node(phexpr_for_arena);
    let ph_var_phrels = phrels.clone();

    let phinfo = PlaceHolderInfo {
        phid: phv.phid,
        ph_var,
        ph_var_phexpr,
        ph_var_phrels,
        ph_eval_at,
        ph_lateral,
        ph_needed: None, // initially it's unused
        ph_width,
    };
    let phinfo_id = root.alloc_phinfo(phinfo);

    // Add to placeholder_list and placeholder_array.
    root.placeholder_list.push(phinfo_id);

    if phv.phid as i32 >= root.placeholder_array_size {
        // Must allocate or enlarge placeholder_array.
        let mut new_size = if root.placeholder_array_size != 0 {
            root.placeholder_array_size * 2
        } else {
            8
        };
        while phv.phid as i32 >= new_size {
            new_size *= 2;
        }
        root.placeholder_array.resize(new_size as usize, None);
        root.placeholder_array_size = new_size;
    }
    root.placeholder_array[phv.phid as usize] = Some(phinfo_id);

    // The PHV's contained expression may contain other, lower-level PHVs; get
    // those into the PlaceHolderInfo list too.
    find_placeholders_in_expr(mcx, root, &phexpr)?;

    Ok(phinfo_id)
}

/// `find_placeholder_info(root, phv); phinfo->ph_needed = bms_add_members(
/// phinfo->ph_needed, where_needed)` (initsplan.c:325/382, via placeholder.c) —
/// record that a PlaceHolderVar's value is needed at `where_needed`. Homed here
/// (the joininfo unit ports `find_placeholder_info`); consumed by
/// `add_vars_to_targetlist` / `add_vars_to_attr_needed` in init-subselect.
pub fn phinfo_add_needed<'a>(
    mcx: mcx::Mcx<'_>,
    root: &mut PlannerInfo,
    phv: &PlaceHolderVar<'a>,
    where_needed: &Relids,
) -> PgResult<()> {
    let phinfo_id = find_placeholder_info(mcx, root, phv)?;
    let cur = bms::relids_copy::call(&root.phinfo(phinfo_id).ph_needed);
    root.phinfo_mut(phinfo_id).ph_needed = bms::relids_add_members::call(cur, where_needed);
    Ok(())
}

/// `find_placeholders_in_jointree`
///		Search the jointree for PlaceHolderVars, and build PlaceHolderInfos.
///
/// Walks `root->parse->jointree` (resolved through [`PlannerRun`] from the
/// opaque `PlannerInfo::parse` handle) collecting every PlaceHolderVar that
/// appears in a FROM/JOIN qual, creating a PlaceHolderInfo for each.
pub fn find_placeholders_in_jointree<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
) -> PgResult<()> {
    // This must be done before freezing the set of PHIs.
    debug_assert!(!root.placeholdersFrozen);

    // We need do nothing if the query contains no PlaceHolderVars.
    let last_ph_id = root.glob.as_ref().map(|g| g.last_ph_id).unwrap_or(0);
    if last_ph_id != 0 {
        // Start recursion at top of jointree.
        let jointree = run
            .jointree(root.parse)
            .expect("find_placeholders_in_jointree: root->parse->jointree != NULL");
        find_placeholders_in_from_expr(run.mcx(), root, jointree)?;
    }
    Ok(())
}

/// `find_placeholders_recurse` — the `FromExpr` arm (top-level jointree entry).
fn find_placeholders_in_from_expr<'mcx>(
    mcx: mcx::Mcx<'_>,
    root: &mut PlannerInfo,
    f: &::nodes::rawnodes::FromExpr<'mcx>,
) -> PgResult<()> {
    // First, recurse to handle child joins.
    for item in f.fromlist.iter() {
        find_placeholders_recurse(mcx, root, item)?;
    }
    // Now process the top-level quals.
    find_placeholders_in_quals(mcx, root, f.quals.as_deref())?;
    Ok(())
}

/// `find_placeholders_recurse`
///		Recursively scan a jointree node for PlaceHolderVars in its quals.
fn find_placeholders_recurse<'mcx>(
    mcx: mcx::Mcx<'_>,
    root: &mut PlannerInfo,
    jtnode: &::nodes::nodes::Node<'mcx>,
) -> PgResult<()> {
    use ::nodes::nodes::ntag;
    match jtnode.node_tag() {
        // No quals to deal with here.
        ntag::T_RangeTblRef => {}
        ntag::T_FromExpr => {
            let f = jtnode.expect_fromexpr();
            // First, recurse to handle child joins.
            for item in f.fromlist.iter() {
                find_placeholders_recurse(mcx, root, item)?;
            }
            // Now process the top-level quals.
            find_placeholders_in_quals(mcx, root, f.quals.as_deref())?;
        }
        ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            // First, recurse to handle child joins.
            if let Some(larg) = j.larg.as_deref() {
                find_placeholders_recurse(mcx, root, larg)?;
            }
            if let Some(rarg) = j.rarg.as_deref() {
                find_placeholders_recurse(mcx, root, rarg)?;
            }
            // Process the qual clauses.
            find_placeholders_in_quals(mcx, root, j.quals.as_deref())?;
        }
        other => {
            panic!(
                "find_placeholders_recurse: unrecognized node type: {:?}",
                other
            );
        }
    }
    Ok(())
}

/// Process a jointree node's `quals` field — the `Node *` that
/// `find_placeholders_recurse` / `find_placeholders_in_jointree` pass to
/// `find_placeholders_in_expr((Node *) f->quals, ...)` in C.
///
/// In C `f->quals` / `j->quals` is just cast to `(Node *)` and fed straight to
/// `expression_tree_walker`, which descends into a `List` of clauses
/// transparently. In this owned model the qual is one of two equivalent shapes
/// (cf. `quals_implicit_and` in init-subselect's jointree.c port):
///
///   * `Node::Expr(e)` — a single qual expression (the common case), or
///   * `Node::List([...])` — an already-imploded implicit-AND conjunct list
///     (produced e.g. by `concat_quals` in `remove_useless_result_rtes`, or by
///     the deparse/pullup of an outer-join WHERE clause).
///
/// `Expr::as_expr()` returns `None` for a `T_List` node, so a bare
/// `as_expr()`-then-walk silently skipped the List form, leaving any
/// PlaceHolderVars buried in a `List`-shaped qual unregistered until after
/// `placeholdersFrozen` — the `too late to create a new PlaceHolderInfo` error.
/// Mirror C: walk each element of a `List`, and a single `Expr` directly.
fn find_placeholders_in_quals(
    mcx: mcx::Mcx<'_>,
    root: &mut PlannerInfo,
    quals: Option<&::nodes::nodes::Node<'_>>,
) -> PgResult<()> {
    match quals {
        None => Ok(()),
        Some(n) => {
            if let Some(items) = n.as_list() {
                for it in items.iter() {
                    if let Some(e) = it.as_expr() {
                        find_placeholders_in_expr(mcx, root, e)?;
                    }
                }
                Ok(())
            } else if let Some(e) = n.as_expr() {
                find_placeholders_in_expr(mcx, root, e)
            } else {
                Ok(())
            }
        }
    }
}

/// `find_placeholders_in_expr`
///		Find all PlaceHolderVars in the given expression, and create
///		PlaceHolderInfo entries for them.
fn find_placeholders_in_expr(mcx: mcx::Mcx<'_>, root: &mut PlannerInfo, expr: &Expr<'_>) -> PgResult<()> {
    // pull_var_clause does more than we need, but it's convenient.
    let vars = ext_seam::pull_var_clause_expr::call(
        expr,
        PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );
    for v in vars {
        // Ignore any plain Vars.
        if let Expr::PlaceHolderVar(phv) = v {
            // Create a PlaceHolderInfo entry if there's not one already.
            let _ = find_placeholder_info(mcx, root, &phv)?;
        }
    }
    Ok(())
}

/// Deep-copy the `phexpr` subtree of the PlaceHolderInfo `phid` via `clone_in`
/// (the derived `Expr::clone` panics on a context-allocated child, e.g. a
/// SubPlan from an `(SELECT ...)` SubLink in a pulled-up subquery).
fn clone_phinfo_phexpr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &PlannerInfo,
    phid: PhInfoId,
    ctx: &'static str,
) -> PgResult<Expr<'mcx>> {
    root.phinfo(phid)
        .ph_var
        .phexpr
        .as_ref()
        .unwrap_or_else(|| panic!("{ctx}: PHV has no phexpr"))
        .as_ref()
        .clone_in(mcx)
        .map_err(|e| PgError::error(alloc::format!("{ctx}: clone_in: {e:?}")))
}

/// `fix_placeholder_input_needed_levels`
///		Adjust the "needed at" levels for placeholder inputs.
pub fn fix_placeholder_input_needed_levels(mcx: mcx::Mcx<'_>, root: &mut PlannerInfo) -> PgResult<()> {
    let list = root.placeholder_list.clone();
    for phid in list {
        let phexpr = ::nodes::primnodes::Expr::erase_lifetime(clone_phinfo_phexpr(
            mcx,
            root,
            phid,
            "fix_placeholder_input_needed_levels",
        )?);
        let ph_eval_at = bms::relids_copy::call(&root.phinfo(phid).ph_eval_at);
        let vars = ext_seam::pull_var_clause_expr::call(
            &phexpr,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );
        ext_seam::add_vars_to_targetlist::call(mcx, root, vars, ph_eval_at)?;
    }
    Ok(())
}

/// `rebuild_placeholder_attr_needed`
///	  Put back attr_needed bits for Vars/PHVs needed in PlaceHolderVars.
pub fn rebuild_placeholder_attr_needed(mcx: mcx::Mcx<'_>, root: &mut PlannerInfo) -> PgResult<()> {
    let list = root.placeholder_list.clone();
    for phid in list {
        let phexpr = ::nodes::primnodes::Expr::erase_lifetime(clone_phinfo_phexpr(
            mcx,
            root,
            phid,
            "rebuild_placeholder_attr_needed",
        )?);
        let ph_eval_at = bms::relids_copy::call(&root.phinfo(phid).ph_eval_at);
        let vars = ext_seam::pull_var_clause_expr::call(
            &phexpr,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );
        ext_seam::add_vars_to_attr_needed::call(mcx, root, vars, ph_eval_at)?;
    }
    Ok(())
}

/// `add_placeholders_to_base_rels`
///		Add any required PlaceHolderVars to base rels' targetlists.
pub fn add_placeholders_to_base_rels(mcx: mcx::Mcx<'_>, root: &mut PlannerInfo) -> PgResult<()> {
    let list = root.placeholder_list.clone();
    for phid in list {
        let eval_at = bms::relids_copy::call(&root.phinfo(phid).ph_eval_at);
        let ph_needed = bms::relids_copy::call(&root.phinfo(phid).ph_needed);

        if let Some(varno) = bms::relids_get_singleton_member::call(&eval_at) {
            if bms::relids_nonempty_difference::call(&ph_needed, &eval_at) {
                let rel = relnode_seams::find_base_rel::call(root, varno);

                // A value computed at scan level has not yet been nulled by any
                // outer join, so its phnullingrels should be empty.
                debug_assert!(root.phinfo(phid).ph_var.phnullingrels.words.is_empty());

                // Copy the PHV (deep-clone via `clone_in`; the derived `Clone`
                // panics on a SubPlan-bearing phexpr) and append to the rel's
                // reltarget exprs.
                let phv = ::nodes::primnodes::placeholdervar_into_static(
                    root.phinfo(phid)
                        .ph_var
                        .clone_in(mcx)
                        .map_err(|e| PgError::error(alloc::format!("add_placeholders_to_base_rels: clone_in: {e:?}")))?,
                );
                let phv_node = root.alloc_node(Expr::PlaceHolderVar(phv));
                root.rel_mut(rel)
                    .reltarget
                    .as_mut()
                    .expect("add_placeholders_to_base_rels: rel has no reltarget")
                    .exprs
                    .push(phv_node);
                // reltarget's cost and width fields will be updated later.
            }
        }
    }
    Ok(())
}

/// `add_placeholders_to_joinrel`
///		Add any newly-computable PlaceHolderVars to a join rel's targetlist; and
///		if computable PHVs contain lateral references, add those references to the
///		joinrel's direct_lateral_relids.
pub fn add_placeholders_to_joinrel(
    mcx: mcx::Mcx<'_>,
    root: &mut PlannerInfo,
    joinrel: ::pathnodes::RelId,
    outer_rel: ::pathnodes::RelId,
    inner_rel: ::pathnodes::RelId,
    _sjinfo: &SpecialJoinInfo,
) -> PgResult<()> {
    let relids = root.rel(joinrel).relids.clone();
    let mut tuple_width: i64 = root
        .rel(joinrel)
        .reltarget
        .as_ref()
        .expect("add_placeholders_to_joinrel: joinrel has no reltarget")
        .width as i64;

    let outer_relids = root.rel(outer_rel).relids.clone();
    let inner_relids = root.rel(inner_rel).relids.clone();

    let list = root.placeholder_list.clone();
    for phid in list {
        let ph_eval_at = bms::relids_copy::call(&root.phinfo(phid).ph_eval_at);

        // Is it computable here?
        if bms::relids_is_subset::call(&ph_eval_at, &relids) {
            // Is it still needed above this joinrel?
            let ph_needed = bms::relids_copy::call(&root.phinfo(phid).ph_needed);
            if bms::relids_nonempty_difference::call(&ph_needed, &relids) {
                // Yes, but only add to tlist if it wasn't computed in either
                // input; otherwise it should be there already.  Also charge the
                // cost of evaluating the contained expression if computable here
                // but not in either input.
                if !bms::relids_is_subset::call(&ph_eval_at, &outer_relids)
                    && !bms::relids_is_subset::call(&ph_eval_at, &inner_relids)
                {
                    // Deep-clone via `clone_in` (derived `Clone` panics on a
                    // SubPlan-bearing phexpr).
                    let phv = ::nodes::primnodes::placeholdervar_into_static(
                        root.phinfo(phid)
                            .ph_var
                            .clone_in(mcx)
                            .map_err(|e| PgError::error(alloc::format!("add_placeholders_to_joinrel: clone_in: {e:?}")))?,
                    );
                    // It'll start out not nulled by anything.
                    debug_assert!(phv.phnullingrels.words.is_empty());
                    let phexpr: Expr<'static> = ::nodes::primnodes::Expr::erase_lifetime(
                        phv.phexpr
                            .as_ref()
                            .expect("add_placeholders_to_joinrel: PHV has no phexpr")
                            .as_ref()
                            .clone_in(mcx)
                            .map_err(|e| PgError::error(alloc::format!("add_placeholders_to_joinrel: phexpr clone_in: {e:?}")))?,
                    );
                    let ph_width = root.phinfo(phid).ph_width;

                    let phv_node = root.alloc_node(Expr::PlaceHolderVar(phv));
                    let (cost_startup, cost_per_tuple) =
                        crate::ext_seam::cost_qual_eval_node_expr::call(root, &phexpr);

                    let rt = root
                        .rel_mut(joinrel)
                        .reltarget
                        .as_mut()
                        .expect("add_placeholders_to_joinrel: joinrel has no reltarget");
                    rt.exprs.push(phv_node);
                    rt.cost.startup += cost_startup;
                    rt.cost.per_tuple += cost_per_tuple;
                    tuple_width += ph_width as i64;
                }
            }

            // Adjust joinrel's direct_lateral_relids to include the PHV's source
            // rel(s).  We must do this even if not actually emitting the PHV.
            let ph_lateral = bms::relids_copy::call(&root.phinfo(phid).ph_lateral);
            let cur = root.rel(joinrel).direct_lateral_relids.clone();
            root.rel_mut(joinrel).direct_lateral_relids =
                bms::relids_add_members::call(cur, &ph_lateral);
        }
    }

    let clamped = crate::ext_seam::clamp_width_est::call(tuple_width);
    root.rel_mut(joinrel)
        .reltarget
        .as_mut()
        .expect("add_placeholders_to_joinrel: joinrel has no reltarget")
        .width = clamped;
    Ok(())
}

/// `contain_placeholder_references_to`
///		Detect whether any PlaceHolderVars in the given clause contain references
///		to the given relid (typically an OJ relid).
pub fn contain_placeholder_references_to(root: &PlannerInfo, clause: &Expr, relid: i32) -> bool {
    // We can answer quickly in the common case that there's no PHVs at all.
    let last_ph_id = root.glob.as_ref().map(|g| g.last_ph_id).unwrap_or(0);
    if last_ph_id == 0 {
        return false;
    }
    // Else run the recursive search.
    let mut context = ContainPlaceholderRefsContext {
        relid,
        sublevels_up: 0,
    };
    contain_placeholder_references_walker(Some(clause), &mut context)
}

struct ContainPlaceholderRefsContext {
    relid: i32,
    sublevels_up: i32,
}

fn contain_placeholder_references_walker(
    node: Option<&Expr>,
    context: &mut ContainPlaceholderRefsContext,
) -> bool {
    let node = match node {
        None => return false,
        Some(n) => n,
    };
    if let Expr::PlaceHolderVar(phv) = node {
        // We should just look through PHVs of other query levels.
        if phv.phlevelsup as i32 == context.sublevels_up {
            // If phrels matches, we found what we came for.
            let phrels = expr_relids_to_relids(&phv.phrels);
            if bms::relids_is_member::call(context.relid, &phrels) {
                return true;
            }
            // We don't examine phnullingrels, and don't need to recurse into the
            // contained expression (phrels summarizes it).  So we're done here.
            return false;
        }
    }
    // Note: the C `IsA(node, Query)` arm (sublevels_up++ + query_tree_walker)
    // recurses into RTE-subquery / not-yet-planned sublink subqueries. The arena
    // `Expr` tree has no Query variant (Query subtrees aren't walkable here), so
    // that arm is unreachable for trees this model builds; the comment in C notes
    // the upper-level-PHV handling is "likely dead". The expression walker below
    // covers all Expr children.
    nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut |n: &Expr| {
        contain_placeholder_references_walker(Some(n), context)
    })
}

/// Compute the set of outer-join relids that can null a placeholder.
///
/// Analogous to `RelOptInfo.nulling_relids` for Vars, computed on the fly.
pub fn get_placeholder_nulling_relids(root: &PlannerInfo, phinfo: PhInfoId) -> Relids {
    let mut result: Relids = None;
    let ph_eval_at = root.phinfo(phinfo).ph_eval_at.clone();

    // Form the union of all potential nulling OJs for each baserel in ph_eval_at.
    let mut relid: i32 = -1;
    loop {
        relid = bms::relids_next_member::call(&ph_eval_at, relid);
        // C: `while ((relid = bms_next_member(...)) > 0)` — note `> 0`, so the
        // zero relid (and the -1 terminator) both stop / skip.
        if relid <= 0 {
            break;
        }
        // ignore the RTE_GROUP RTE
        if relid == root.group_rtindex {
            continue;
        }
        let slot = root
            .simple_rel_array
            .get(relid as usize)
            .copied()
            .flatten();
        match slot {
            None => {
                // must be an outer join
                debug_assert!(bms::relids_is_member::call(relid, &root.outer_join_rels));
                continue;
            }
            Some(rel) => {
                let nulling = root.rel(rel).nulling_relids.clone();
                result = bms::relids_add_members::call(result, &nulling);
            }
        }
    }

    // Now remove any OJs already included in ph_eval_at.
    crate::bms_path::relids_del_members::call(result, &ph_eval_at)
}
