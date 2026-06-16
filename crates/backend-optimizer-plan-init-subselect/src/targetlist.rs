//! TARGET LISTS (initsplan.c) — `build_base_rel_tlists`,
//! `add_vars_to_targetlist`, `add_vars_to_attr_needed`.

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerInfo, RelId, Relids};

use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_path_equivclass_ext_seams as eqext;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

/// `PVC_*` flags (var.h), transcribed.
const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_INCLUDE_PLACEHOLDERS: i32 = 0x0010;

/// `find_base_rel(root, relid)` (relnode.c) — the baserel `RelOptInfo` handle.
#[inline]
fn find_base_rel(root: &PlannerInfo, relid: i32) -> RelId {
    bms::find_base_rel::call(root, relid)
}

/// `build_base_rel_tlists` (initsplan.c:235).
///
/// Add targetlist entries for each var needed in the query's final tlist (and
/// HAVING clause, if any) to the appropriate base relations, marking them as
/// needed by "relation 0" so they propagate up through all join plan steps.
///
/// The C `final_tlist` argument is always `root->processed_tlist`; we read it
/// off `root`. `processed_tlist` holds `TargetEntry` node handles, so we collect
/// each entry's `expr` and run `pull_var_clause` over the list.
pub fn build_base_rel_tlists(root: &mut PlannerInfo, run: &PlannerRun<'_>) -> PgResult<()> {
    // pull_var_clause((Node *) final_tlist, PVC_RECURSE_AGGREGATES |
    //                 PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS)
    let tlist_exprs: Vec<Expr> = root
        .processed_tlist
        .iter()
        .map(|&te| {
            let expr_id = root.targetentry(te).expr;
            root.node(expr_id).clone()
        })
        .collect();
    let tlist_vars = eqext::pull_var_clause_list::call(
        tlist_exprs,
        PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );

    if !tlist_vars.is_empty() {
        let where_needed = bms::relids_make_singleton::call(0);
        add_vars_to_targetlist(root, tlist_vars, where_needed)?;
    }

    // If there's a HAVING clause, we'll need the Vars it uses, too. Note that
    // HAVING can contain Aggrefs but not WindowFuncs.
    let having: Option<Expr> = run
        .resolve(root.parse)
        .havingQual
        .as_deref()
        .cloned();
    if let Some(having_qual) = having {
        let having_vars = eqext::pull_var_clause::call(
            &having_qual,
            PVC_RECURSE_AGGREGATES | PVC_INCLUDE_PLACEHOLDERS,
        );
        if !having_vars.is_empty() {
            let where_needed = bms::relids_make_singleton::call(0);
            add_vars_to_targetlist(root, having_vars, where_needed)?;
        }
    }
    Ok(())
}

/// `add_vars_to_targetlist` (initsplan.c:282).
///
/// For each variable in the list, add it to the owning relation's targetlist if
/// not already present, and mark it as needed for the indicated join (or for
/// final output if `where_needed` includes "relation 0"). The list may also
/// contain `PlaceHolderVar`s, whose `ph_needed` is updated via the placeholder
/// owner instead.
pub fn add_vars_to_targetlist(
    root: &mut PlannerInfo,
    vars: Vec<Expr>,
    where_needed: Relids,
) -> PgResult<()> {
    debug_assert!(!bms::relids_is_empty::call(&where_needed));

    for node in vars {
        match node {
            Expr::Var(var) => {
                let relid = find_base_rel(root, var.varno);
                let mut attno = var.varattno as i32;
                {
                    let rel = root.rel(relid);
                    if bms::relids_is_subset::call(&where_needed, &rel.relids) {
                        continue;
                    }
                    debug_assert!(attno >= rel.min_attr as i32 && attno <= rel.max_attr as i32);
                    attno -= rel.min_attr as i32;
                }
                let needs_add = root.rel(relid).attr_needed[attno as usize].is_none();
                if needs_add {
                    // Variable not yet requested, so add to rel's targetlist.
                    // The value available at the rel's scan level has not been
                    // nulled by any outer join, so drop its varnullingrels.
                    // (We'll put those back as we climb up the join tree.)
                    let mut newvar = var.clone();
                    newvar.varnullingrels = Default::default();
                    let newvar_id = root.alloc_node(Expr::Var(newvar));
                    let rel = root.rel_mut(relid);
                    if let Some(reltarget) = rel.reltarget.as_mut() {
                        reltarget.exprs.push(newvar_id);
                    }
                    // reltarget cost and width will be computed later
                }
                let cur = root.rel_mut(relid).attr_needed[attno as usize].take();
                root.rel_mut(relid).attr_needed[attno as usize] =
                    bms::relids_add_members::call(cur, &where_needed);
            }
            Expr::PlaceHolderVar(phv) => {
                initext::phinfo_add_needed::call(root, &phv, &where_needed)?;
            }
            other => {
                panic!("unrecognized node type: {:?}", core::mem::discriminant(&other));
            }
        }
    }
    Ok(())
}

/// `add_vars_to_attr_needed` (initsplan.c:353).
///
/// A subset of `add_vars_to_targetlist`: just update `attr_needed` for Vars and
/// `ph_needed` for PlaceHolderVars; the Vars are assumed already present in
/// their relations' targetlists. Used to rebuild attr_needed after removal of a
/// useless outer join.
pub fn add_vars_to_attr_needed(
    root: &mut PlannerInfo,
    vars: Vec<Expr>,
    where_needed: Relids,
) -> PgResult<()> {
    debug_assert!(!bms::relids_is_empty::call(&where_needed));

    for node in vars {
        match node {
            Expr::Var(var) => {
                let relid = find_base_rel(root, var.varno);
                let mut attno = var.varattno as i32;
                {
                    let rel = root.rel(relid);
                    if bms::relids_is_subset::call(&where_needed, &rel.relids) {
                        continue;
                    }
                    debug_assert!(attno >= rel.min_attr as i32 && attno <= rel.max_attr as i32);
                    attno -= rel.min_attr as i32;
                }
                let cur = root.rel_mut(relid).attr_needed[attno as usize].take();
                root.rel_mut(relid).attr_needed[attno as usize] =
                    bms::relids_add_members::call(cur, &where_needed);
            }
            Expr::PlaceHolderVar(phv) => {
                initext::phinfo_add_needed::call(root, &phv, &where_needed)?;
            }
            other => {
                panic!("unrecognized node type: {:?}", core::mem::discriminant(&other));
            }
        }
    }
    Ok(())
}
