//! LATERAL REFERENCES (initsplan.c) — `find_lateral_references`,
//! `extract_lateral_references`, `rebuild_lateral_attr_needed`,
//! `create_lateral_join_info`.
//!
//! # Model reconciliation (read before editing)
//!
//! C reads each baserel's RTE (`root->simple_rte_array[rti]`, a `RangeTblEntry
//! *`) to pull the laterally-referenced Vars/PHVs from the RTE's lateral parse
//! subtrees. This repo's `PlannerInfo.simple_rte_array` carries opaque
//! [`RangeTblEntryId`](types_pathnodes::RangeTblEntryId) handles, resolved to a
//! borrowed `&RangeTblEntry<'mcx>` through the established
//! [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) resolver
//! (`run.resolve_rte`). So `find_lateral_references` and the family take an
//! additional `run: &PlannerRun<'mcx>` parameter alongside `&mut PlannerInfo`,
//! exactly as `jointree.rs`/`quals.rs` do. (`rebuild_lateral_attr_needed` does
//! not actually touch the RTE — it only re-reads `brel->lateral_vars` — but it
//! is a co-installed seam so it carries `run` for signature uniformity.)
//!
//! `RelOptInfo.lateral_vars` is the RESOLVED `Vec<NodeId>` carrier (the keystone
//! that the src-idiomatic base was blocked on): `extract_lateral_references`
//! stores each leveled Var/PHV via `root.alloc_node(expr)`, and
//! `create_lateral_join_info`/`rebuild_lateral_attr_needed` read them back via
//! `root.node(nodeid)`.
//!
//! The vars are gathered from the RTE's lateral parse subtrees
//! (`pull_vars_of_level((Node *) X, level)` in C). Those subtrees are owned
//! `Query`/`List`/`TableFunc` parse structures, not arena `Expr`s, so the
//! `pull_vars_of_level` walk rides the per-`Node`/per-`Query`
//! [`initext::pull_vars_of_level_node`]/[`initext::pull_vars_of_level_query`]
//! seams (var.c is ported but its installed `&Expr`/`NodeId` var seams cannot
//! name a whole parse `Node`).

extern crate alloc;

use alloc::vec::Vec;

use types_nodes::parsenodes::RTEKind;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerInfo, Relids, RELOPT_BASEREL};

use backend_optimizer_util_joininfo as joininfo;
use backend_optimizer_util_joininfo_ext_seams as jiext;
use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

/// `find_lateral_references` (initsplan.c:657).
///
/// For each LATERAL-derived baserel, find the Vars/PHVs it references laterally
/// (in its RTE's lateral expressions) and add them to the appropriate source
/// relations' targetlists, so that those values will be available for evaluation
/// of the subquery. This has to run before `deconstruct_jointree`, since it
/// might result in creation of PlaceHolderInfos.
pub fn find_lateral_references<'mcx>(root: &mut PlannerInfo, run: &mut PlannerRun<'mcx>) -> types_error::PgResult<()> {
    // We need do nothing if the query contains no LATERAL RTEs.
    if !root.hasLateralRTEs {
        return Ok(());
    }

    // Examine all baserels (the rel array has been set up by now).
    for rti in 1..root.simple_rel_array_size {
        // there may be empty slots corresponding to non-baserel RTEs
        let rel_id = match root.simple_rel_array[rti as usize] {
            None => continue,
            Some(id) => id,
        };

        debug_assert!(root.rel(rel_id).relid as i32 == rti); // sanity check

        // Ignore RTEs that are "other rels": we consider only their parent
        // baserels, since it is the parent's relid that will be used for join
        // planning and the parent's RTE contains all the lateral references.
        if root.rel(rel_id).reloptkind != RELOPT_BASEREL {
            continue;
        }

        extract_lateral_references(root, run, rel_id, rti)?;
    }
    Ok(())
}

/// `extract_lateral_references` (static, initsplan.c:705).
///
/// Pull the level-appropriate lateral Vars/PHVs out of one LATERAL baserel's
/// RTE, adjust them to the current query level, push them into their source
/// relations' targetlists (and PHVs into `root->placeholder_list`), and remember
/// them in `brel->lateral_vars`.
fn extract_lateral_references<'mcx>(
    root: &mut PlannerInfo,
    run: &mut PlannerRun<'mcx>,
    rel_id: types_pathnodes::RelId,
    rtindex: i32,
) -> types_error::PgResult<()> {
    let rte_id = root.simple_rte_array[rtindex as usize];
    // Planner-run context: a pulled PlaceHolderVar is deep-copied (copyObject)
    // into here, so its `'mcx`-tagged children outlive the arena handles stored
    // on the rel below. `Mcx` is Copy, so snapshot it before the RTE borrow.
    let mcx = run.mcx();

    // Gather the appropriate variables per RTE kind. We resolve the borrowed
    // RTE and copy out the vars (cloned `Expr`s) before any `&mut root` work,
    // so the immutable RTE borrow is dropped first.
    let vars: Vec<Expr> = {
        let rte = run.resolve_rte(rte_id);

        // No cross-references are possible if it's not LATERAL.
        if !rte.lateral {
            return Ok(());
        }

        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                // pull_vars_of_level((Node *) rte->tablesample, 0)
                match rte.tablesample.as_deref() {
                    Some(ts) => initext::pull_vars_of_level_node::call(mcx, ts, 0)?,
                    None => Vec::new(),
                }
            }
            RTEKind::RTE_SUBQUERY => {
                // pull_vars_of_level((Node *) rte->subquery, 1)
                match rte.subquery.as_deref() {
                    Some(sub) => initext::pull_vars_of_level_query::call(mcx, sub, 1)?,
                    None => Vec::new(),
                }
            }
            RTEKind::RTE_FUNCTION => {
                // pull_vars_of_level((Node *) rte->functions, 0)
                let mut v = Vec::new();
                for func in rte.functions.iter() {
                    v.extend(initext::pull_vars_of_level_node::call(mcx, func, 0)?);
                }
                v
            }
            RTEKind::RTE_TABLEFUNC => {
                // pull_vars_of_level((Node *) rte->tablefunc, 0)
                match rte.tablefunc.as_deref() {
                    Some(tf) => initext::pull_vars_of_level_node::call(mcx, tf, 0)?,
                    None => Vec::new(),
                }
            }
            RTEKind::RTE_VALUES => {
                // pull_vars_of_level((Node *) rte->values_lists, 0)
                let mut v = Vec::new();
                for vl in rte.values_lists.iter() {
                    v.extend(initext::pull_vars_of_level_node::call(mcx, vl, 0)?);
                }
                v
            }
            _ => {
                debug_assert!(false);
                return Ok(()); // keep compiler quiet
            }
        }
    };

    if vars.is_empty() {
        return Ok(()); // nothing to do
    }

    // Copy each Var (or PlaceHolderVar) and adjust it to match our level. The
    // `vars` returned above are already fresh clones (copyObject), so we mutate
    // them in place.
    let mut newvars: Vec<Expr> = Vec::with_capacity(vars.len());
    for node in vars {
        match node {
            Expr::Var(mut var) => {
                // Adjustment is easy since it's just one node.
                var.varlevelsup = 0;
                newvars.push(Expr::Var(var));
            }
            Expr::PlaceHolderVar(mut phv) => {
                let levelsup = phv.phlevelsup;

                // Have to work harder to adjust the contained expression too.
                if levelsup != 0 {
                    // IncrementVarSublevelsUp(node, -levelsup, 0): the whole PHV
                    // (including its phexpr) shifts down by `levelsup`. We thread
                    // it through the per-`Expr` seam and rebuild the PHV from the
                    // result (the result is still an Expr::PlaceHolderVar).
                    let shifted = initext::increment_var_sublevels_up_expr::call(
                        mcx,
                        Expr::PlaceHolderVar(phv),
                        -(levelsup as i32),
                        0,
                    )
                    .expect("increment_var_sublevels_up");
                    phv = match shifted {
                        Expr::PlaceHolderVar(p) => p,
                        other => panic!(
                            "increment_var_sublevels_up of a PHV must yield a PHV, got {:?}",
                            core::mem::discriminant(&other)
                        ),
                    };
                }

                // If we pulled the PHV out of a subquery RTE, its expression
                // needs to be preprocessed. subquery_planner() already did this
                // for level-zero PHVs in function and values RTEs, though.
                if levelsup > 0 {
                    let phexpr = phv
                        .phexpr
                        .take()
                        .map(|b| *b)
                        .expect("upper-level PHV has phexpr");
                    let processed =
                        initext::preprocess_phv_expression::call(root, run, phexpr)
                            .expect("preprocess_phv_expression");
                    phv.phexpr = Some(alloc::boxed::Box::new(processed));
                }

                newvars.push(Expr::PlaceHolderVar(phv));
            }
            other => {
                panic!(
                    "extract_lateral_references: unexpected node (not Var/PHV): {:?}",
                    core::mem::discriminant(&other)
                );
            }
        }
    }

    // We mark the Vars as being "needed" at the LATERAL RTE. This is a bit of a
    // cheat: a more formal approach would be to mark each one as needed at the
    // join of the LATERAL RTE with its source RTE. But it will work.
    let where_needed = bms::relids_make_singleton::call(rtindex);

    // Push Vars into their source relations' targetlists, and PHVs into
    // root->placeholder_list.
    crate::targetlist::add_vars_to_targetlist(
        root,
        clone_exprs(&newvars),
        bms::relids_copy::call(&where_needed),
    )
    .expect("add_vars_to_targetlist");

    // Remember the lateral references for rebuild_lateral_attr_needed and
    // create_lateral_join_info. We intern each Var/PHV into the node arena and
    // store the handles on the rel (the resolved `lateral_vars: Vec<NodeId>`
    // carrier).
    let mut handles = Vec::with_capacity(newvars.len());
    for v in newvars {
        handles.push(root.alloc_node(v));
    }
    root.rel_mut(rel_id).lateral_vars = handles;
    Ok(())
}

/// `rebuild_lateral_attr_needed` (initsplan.c:807).
///
/// Put back `attr_needed`/`ph_needed` bits for Vars/PHVs needed for lateral
/// references. Used to rebuild those sets after removal of a useless outer join;
/// matches `find_lateral_references` except it calls `add_vars_to_attr_needed`
/// instead of `add_vars_to_targetlist`. It reuses the Vars/PHVs that
/// `extract_lateral_references` saved in `lateral_vars`.
pub fn rebuild_lateral_attr_needed(root: &mut PlannerInfo, run: &PlannerRun<'_>) {
    // We need do nothing if the query contains no LATERAL RTEs.
    if !root.hasLateralRTEs {
        return;
    }

    // Examine the same baserels that find_lateral_references did.
    for rti in 1..root.simple_rel_array_size {
        let rel_id = match root.simple_rel_array[rti as usize] {
            None => continue,
            Some(id) => id,
        };
        if root.rel(rel_id).reloptkind != RELOPT_BASEREL {
            continue;
        }

        // No need to repeat extract_lateral_references; it saved the extracted
        // Vars/PHVs in lateral_vars. Resolve them back to owned `Expr`s.
        if root.rel(rel_id).lateral_vars.is_empty() {
            continue;
        }
        // Deep-copy each lateral Var/PHV via `Expr::clone_in` (a derived
        // `Expr::clone` panics on a context-allocated child such as a PHV's
        // contained expr).
        let vars: Vec<Expr> = root
            .rel(rel_id)
            .lateral_vars
            .iter()
            .map(|&nid| root.node(nid).clone_in(run.mcx()).expect("clone_in"))
            .collect();

        let where_needed = bms::relids_make_singleton::call(rti);

        crate::targetlist::add_vars_to_attr_needed(root, vars, where_needed)
            .expect("add_vars_to_attr_needed");
    }
}

/// `create_lateral_join_info` (initsplan.c:844).
///
/// Fill in the per-base-relation `direct_lateral_relids`, `lateral_relids` and
/// `lateral_referencers` sets.
pub fn create_lateral_join_info(root: &mut PlannerInfo, run: &PlannerRun<'_>) {
    let mut found_laterals = false;

    // We need do nothing if the query contains no LATERAL RTEs.
    if !root.hasLateralRTEs {
        return;
    }

    // We'll need to have the ph_eval_at values for PlaceHolderVars.
    debug_assert!(root.placeholdersFrozen);

    // Examine all baserels (the rel array has been set up by now).
    for rti in 1..root.simple_rel_array_size {
        let rel_id = match root.simple_rel_array[rti as usize] {
            None => continue,
            Some(id) => id,
        };

        debug_assert!(root.rel(rel_id).relid as i32 == rti); // sanity check

        // Ignore RTEs that are "other rels".
        if root.rel(rel_id).reloptkind != RELOPT_BASEREL {
            continue;
        }

        let mut lateral_relids: Relids = None;

        // Consider each laterally-referenced Var or PHV. Snapshot the handles to
        // avoid holding a borrow of root across find_placeholder_info.
        let lv: Vec<types_pathnodes::NodeId> = root.rel(rel_id).lateral_vars.clone();
        for nid in lv {
            // Deep-copy via `Expr::clone_in` (a derived `Expr::clone` panics on
            // a context-allocated child such as a PHV's contained expr).
            let node = root.node(nid).clone_in(run.mcx()).expect("clone_in");
            match node {
                Expr::Var(var) => {
                    found_laterals = true;
                    lateral_relids = bms::relids_add_member::call(lateral_relids.take(), var.varno);
                }
                Expr::PlaceHolderVar(phv) => {
                    let phinfo_id = joininfo::find_placeholder_info(root, &phv)
                        .expect("find_placeholder_info");
                    let ph_eval_at = bms::relids_copy::call(&root.phinfo(phinfo_id).ph_eval_at);

                    found_laterals = true;
                    lateral_relids =
                        bms::relids_add_members::call(lateral_relids.take(), &ph_eval_at);
                }
                other => panic!(
                    "create_lateral_join_info: unexpected lateral_vars node: {:?}",
                    core::mem::discriminant(&other)
                ),
            }
        }

        // We now have all the simple lateral refs from this rel.
        let copy = bms::relids_copy::call(&lateral_relids);
        let rel = root.rel_mut(rel_id);
        rel.direct_lateral_relids = lateral_relids;
        rel.lateral_relids = copy;
    }

    // Now check for lateral references within PlaceHolderVars, and mark their
    // eval_at rels as having lateral references to the source rels.
    //
    // For a PHV due to be evaluated at a baserel, mark its source(s) as direct
    // lateral dependencies of the baserel (adding onto the ones recorded above).
    // If it's due to be evaluated at a join, mark its source(s) as indirect
    // lateral dependencies of each baserel in the join, ie put them into
    // lateral_relids but not direct_lateral_relids.
    let phinfo_ids: Vec<types_pathnodes::PhInfoId> = root.placeholder_list.clone();
    for phinfo_id in phinfo_ids {
        // PHV is uninteresting if it has no lateral refs.
        if bms::relids_is_empty::call(&root.phinfo(phinfo_id).ph_lateral) {
            continue;
        }

        found_laterals = true;

        // Include only baserels (not outer joins) in the evaluation sites'
        // lateral relids. This avoids problems when outer-join order gets
        // rearranged, and should still ensure the lateral values are available
        // when needed.
        let ph_lateral = bms::relids_copy::call(&root.phinfo(phinfo_id).ph_lateral);
        let lateral_refs = bms::relids_intersect::call(&ph_lateral, &root.all_baserels);
        debug_assert!(!bms::relids_is_empty::call(&lateral_refs));

        let eval_at = bms::relids_copy::call(&root.phinfo(phinfo_id).ph_eval_at);

        if let Some(varno) = bms::relids_get_singleton_member::call(&eval_at) {
            // Evaluation site is a baserel.
            let brel = bms::find_base_rel::call(root, varno);
            let rel = root.rel_mut(brel);
            rel.direct_lateral_relids = bms::relids_add_members::call(
                rel.direct_lateral_relids.take(),
                &lateral_refs,
            );
            rel.lateral_relids =
                bms::relids_add_members::call(rel.lateral_relids.take(), &lateral_refs);
        } else {
            // Evaluation site is a join.
            let mut varno = -1;
            loop {
                varno = bms::relids_next_member::call(&eval_at, varno);
                if varno < 0 {
                    break;
                }
                match jiext::find_base_rel_ignore_join::call(run, root, varno) {
                    None => continue, // ignore outer joins in eval_at
                    Some(brel) => {
                        let rel = root.rel_mut(brel);
                        rel.lateral_relids = bms::relids_add_members::call(
                            rel.lateral_relids.take(),
                            &lateral_refs,
                        );
                    }
                }
            }
        }
    }

    // If we found no actual lateral references, we're done; but reset the
    // hasLateralRTEs flag to avoid useless work later.
    if !found_laterals {
        root.hasLateralRTEs = false;
        return;
    }

    // Calculate the transitive closure of the lateral_relids sets, so that they
    // describe both direct and indirect lateral references. This is essentially
    // Warshall's algorithm for transitive closure.
    for rti in 1..root.simple_rel_array_size {
        let rel_id = match root.simple_rel_array[rti as usize] {
            None => continue,
            Some(id) => id,
        };
        if root.rel(rel_id).reloptkind != RELOPT_BASEREL {
            continue;
        }

        // Need not consider this baserel further if it has no lateral refs.
        let outer_lateral_relids = bms::relids_copy::call(&root.rel(rel_id).lateral_relids);
        if bms::relids_is_empty::call(&outer_lateral_relids) {
            continue;
        }

        // Else scan all baserels.
        for rti2 in 1..root.simple_rel_array_size {
            let rel2_id = match root.simple_rel_array[rti2 as usize] {
                None => continue,
                Some(id) => id,
            };
            if root.rel(rel2_id).reloptkind != RELOPT_BASEREL {
                continue;
            }

            // If brel2 has a lateral ref to brel (rti), propagate brel's refs.
            if bms::relids_is_member::call(rti, &root.rel(rel2_id).lateral_relids) {
                let rel2 = root.rel_mut(rel2_id);
                rel2.lateral_relids = bms::relids_add_members::call(
                    rel2.lateral_relids.take(),
                    &outer_lateral_relids,
                );
            }
        }
    }

    // Now that we've identified all lateral references, mark each baserel with
    // the set of relids of rels that reference it laterally (possibly
    // indirectly) --- the inverse mapping of lateral_relids.
    for rti in 1..root.simple_rel_array_size {
        let rel_id = match root.simple_rel_array[rti as usize] {
            None => continue,
            Some(id) => id,
        };
        if root.rel(rel_id).reloptkind != RELOPT_BASEREL {
            continue;
        }

        // Nothing to do at rels with no lateral refs.
        let lateral_relids = bms::relids_copy::call(&root.rel(rel_id).lateral_relids);
        if bms::relids_is_empty::call(&lateral_relids) {
            continue;
        }

        // No rel should have a lateral dependency on itself.
        debug_assert!(!bms::relids_is_member::call(rti, &lateral_relids));

        // Mark this rel's referencees.
        let mut rti2 = -1;
        loop {
            rti2 = bms::relids_next_member::call(&lateral_relids, rti2);
            if rti2 < 0 {
                break;
            }
            let rel2_id = match root.simple_rel_array[rti2 as usize] {
                None => continue, // must be an OJ
                Some(id) => id,
            };
            debug_assert!(root.rel(rel2_id).reloptkind == RELOPT_BASEREL);
            let rel2 = root.rel_mut(rel2_id);
            rel2.lateral_referencers =
                bms::relids_add_member::call(rel2.lateral_referencers.take(), rti);
        }
    }
}

/// Deep-clone a slice of owned `Expr`s (the lifetime-free planner `Expr`
/// derives `Clone`, so this is a straightforward element-wise copy — the C
/// `copyObject` over already-fresh nodes).
fn clone_exprs(v: &[Expr]) -> Vec<Expr> {
    v.to_vec()
}
