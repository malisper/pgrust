//! equivclass.c — EC member matching / FK matching / known-equal tests.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::Oid;
use ::nodes::primnodes::Expr;
use pathnodes::{
    EcId, EmId, EquivalenceMember, ForeignKeyOptInfo, PlannerInfo,
    RelId, Relids,
};

use equivclass_ext_seams as ec_seam;
use relnode_seams as bms;
use lsyscache_seams as cat;

use crate::merge::{em_expr_ref, em_value, strip_relabeltypes};
use crate::relevance::{live_ec_ids, new_iterator, oid_is_valid};

/* PVC flags (optimizer.h) */
const PVC_INCLUDE_AGGREGATES: i32 = 0x0001;
const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_INCLUDE_WINDOWFUNCS: i32 = 0x0004;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_INCLUDE_PLACEHOLDERS: i32 = 0x0010;
const PVC_INCLUDE_CONVERTROWTYPES: i32 = 0x0040;

/* ======================================================================
 * find_ec_member_matching_expr (equivclass.c:916)
 * ==================================================================== */

/// `find_ec_member_matching_expr(ec, expr, relids)` — the EC member equal to
/// `expr` after stripping RelabelTypes, or `None`.
pub fn find_ec_member_matching_expr(
    root: &PlannerInfo,
    ec: EcId,
    expr: &Expr,
    relids: &Relids,
) -> Option<EquivalenceMember> {
    /* ignore binary-compatible relabeling on both ends */
    let expr = strip_relabeltypes(expr);

    let mut it = new_iterator(root, ec, relids);
    while let Some(em_id) = crate::relevance::eclass_member_iterator_next(root, &mut it) {
        let em = root.em(em_id);
        if em.em_is_const {
            continue;
        }
        if em.em_is_child && !bms::relids_is_subset::call(&em.em_relids, relids) {
            continue;
        }
        // Borrow the stored EC-member expr directly (C reuses the `Expr *`);
        // `equal`/`strip_relabeltypes` only read it. A `.clone()` here (via the
        // old `em_expr`) would panic on owned-subtree Exprs like Aggref.
        let emexpr = strip_relabeltypes(root.node(em.em_expr));
        if ec_seam::equal::call(emexpr, expr) {
            return Some(em.clone());
        }
    }
    None
}

/* ======================================================================
 * find_computable_ec_member (equivclass.c:991)
 * ==================================================================== */

/// `find_computable_ec_member(root, ec, exprs, relids, require_parallel_safe)`
/// (equivclass.c:991) — the EC member all of whose Vars/quasi-Vars are present
/// in `exprs`.
pub fn find_computable_ec_member(
    root: &PlannerInfo,
    ec: EcId,
    exprs: &[Expr],
    relids: &Relids,
    require_parallel_safe: bool,
) -> Option<EmId> {
    /* pull the Vars/quasi-Vars present in "exprs" */
    let exprvars = ec_seam::pull_var_clause_list::call(
        exprs,
        PVC_INCLUDE_AGGREGATES
            | PVC_INCLUDE_WINDOWFUNCS
            | PVC_INCLUDE_PLACEHOLDERS
            | PVC_INCLUDE_CONVERTROWTYPES,
    );

    let mut it = new_iterator(root, ec, relids);
    while let Some(em_id) = crate::relevance::eclass_member_iterator_next(root, &mut it) {
        let em = root.em(em_id);
        if em.em_is_const {
            continue;
        }
        if em.em_is_child && !bms::relids_is_subset::call(&em.em_relids, relids) {
            continue;
        }

        /* match if all Vars/quasi-Vars are present in "exprs" */
        let emexpr_owned = em_expr_ref(root, em_id);
        let emvars = ec_seam::pull_var_clause::call(
            emexpr_owned,
            PVC_INCLUDE_AGGREGATES | PVC_INCLUDE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );
        let mut all_present = true;
        for v in &emvars {
            if !list_member_expr(&exprvars, v) {
                all_present = false;
                break;
            }
        }
        if !all_present {
            continue; /* a non-available Var */
        }

        /* reject non-parallel-safe if requested (expensive, so last) */
        if require_parallel_safe && !ec_seam::is_parallel_safe::call(root, emexpr_owned) {
            continue;
        }

        return Some(em_id); /* found usable expression */
    }

    None
}

/* ======================================================================
 * relation_can_be_sorted_early (equivclass.c:1077)
 * ==================================================================== */

/// `relation_can_be_sorted_early(root, rel, ec, require_parallel_safe)`
/// (equivclass.c:1077).
pub fn relation_can_be_sorted_early(
    root: &PlannerInfo,
    rel: RelId,
    ec: EcId,
    require_parallel_safe: bool,
) -> bool {
    /* reject volatile ECs immediately */
    if root.ec(ec).ec_has_volatile {
        return false;
    }

    // Working context for transient deep copies of the reltarget exprs; they are
    // only inspected (`find_ec_member_matching_expr` / `find_computable_ec_member`),
    // never stored, so a function-scoped context is correct. `clone_in` is
    // required because the derived `Expr::clone` panics on an owned-subtree child.
    let work_ctx = mcx::MemoryContext::new("relation_can_be_sorted_early");
    let work_mcx = work_ctx.mcx();
    let target_expr_ids: Vec<pathnodes::NodeId> = {
        let relopt = root.rel(rel);
        let target = relopt
            .reltarget
            .as_ref()
            .expect("relation_can_be_sorted_early: rel has no reltarget");
        target.exprs.clone()
    };
    let target_exprs: Vec<Expr> = target_expr_ids
        .iter()
        .map(|&nid| {
            root.node(nid)
                .clone_in(work_mcx)
                .expect("relation_can_be_sorted_early: clone_in target expr")
        })
        .collect();
    let rel_relids = root.rel(rel).relids.clone();

    /* try to find an EM directly matching some reltarget member */
    for targetexpr in &target_exprs {
        let em = match find_ec_member_matching_expr(root, ec, targetexpr, &rel_relids) {
            None => continue,
            Some(em) => em,
        };
        // Borrow the stored EC-member expr (C reuses the `Expr *`); both checks
        // only read it. A `.clone()` would panic on owned-subtree Exprs.
        let emexpr = root.node(em.em_expr);
        /* reject SRFs (can't be computed early) */
        if ec_seam::expression_returns_set::call(emexpr) {
            continue;
        }
        if require_parallel_safe && !ec_seam::is_parallel_safe::call(root, emexpr) {
            continue;
        }
        return true;
    }

    /* try to find an expression computable from the reltarget */
    let em = match find_computable_ec_member(root, ec, &target_exprs, &rel_relids, require_parallel_safe)
    {
        None => return false,
        Some(em) => em,
    };
    let emexpr = em_expr_ref(root, em);
    if ec_seam::expression_returns_set::call(emexpr) {
        return false;
    }
    true
}

/* ======================================================================
 * exprs_known_equal (equivclass.c:2648)
 * ==================================================================== */

/// `exprs_known_equal(root, item1, item2, opfamily)` (equivclass.c:2648).
pub fn exprs_known_equal(root: &PlannerInfo, item1: &Expr, item2: &Expr, opfamily: Oid) -> bool {
    for ec_id in live_ec_ids(root) {
        let ec = root.ec(ec_id);
        let mut item1member = false;
        let mut item2member = false;

        if ec.ec_has_volatile {
            continue;
        }
        if oid_is_valid(opfamily) && !ec.ec_opfamilies.contains(&opfamily) {
            continue;
        }

        for &em_id in &ec.ec_members {
            debug_assert!(!root.em(em_id).em_is_child);
            let emexpr = em_expr_ref(root, em_id);
            if ec_seam::equal::call(item1, emexpr) {
                item1member = true;
            } else if ec_seam::equal::call(item2, emexpr) {
                item2member = true;
            }
            if item1member && item2member {
                return true;
            }
        }
    }
    false
}

/* ======================================================================
 * match_eclasses_to_foreign_key_col (equivclass.c:2710)
 * ==================================================================== */

/// `match_eclasses_to_foreign_key_col(root, fkinfo, colno)` (equivclass.c:2710).
pub fn match_eclasses_to_foreign_key_col(
    root: &PlannerInfo,
    fkinfo: &mut ForeignKeyOptInfo,
    colno: usize,
) -> Option<EcId> {
    let var1varno = fkinfo.con_relid as i32;
    let var1attno = fkinfo.conkey[colno];
    let var2varno = fkinfo.ref_relid as i32;
    let var2attno = fkinfo.confkey[colno];
    let eqop = fkinfo.conpfeqop[colno];

    let rel1 = root.simple_rel_array[fkinfo.con_relid as usize]
        .expect("match_eclasses_to_foreign_key_col: con_relid has no simple_rel_array entry");
    let rel2 = root.simple_rel_array[fkinfo.ref_relid as usize]
        .expect("match_eclasses_to_foreign_key_col: ref_relid has no simple_rel_array entry");

    let mut opfamilies: Option<Vec<Oid>> = None;

    debug_assert!(root.ec_merging_done);
    let matching_ecs = bms::relids_intersect::call(
        &root.rel(rel1).eclass_indexes,
        &root.rel(rel2).eclass_indexes,
    );

    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(&matching_ecs, i);
        if i < 0 {
            break;
        }
        let ec_id = EcId(i as u32);
        if root.ec(ec_id).ec_has_volatile {
            continue;
        }

        let mut item1_em: Option<EmId> = None;
        let mut item2_em: Option<EmId> = None;

        let members = root.ec(ec_id).ec_members.clone();
        for em_id in members {
            debug_assert!(!root.em(em_id).em_is_child);

            /* EM must be a Var, possibly with RelabelType */
            let emexpr = em_expr_ref(root, em_id);
            let stripped = strip_relabeltypes(emexpr);
            let var = match stripped.as_var() {
                Some(v) => v,
                None => continue,
            };

            if var.varno == var1varno && var.varattno == var1attno {
                item1_em = Some(em_id);
            } else if var.varno == var2varno && var.varattno == var2attno {
                item2_em = Some(em_id);
            }

            if let (Some(_e1), Some(e2)) = (item1_em, item2_em) {
                /* succeed if eqop matches the EC's opfamilies */
                if opfamilies.is_none() {
                    let scratch = mcx::MemoryContext::new("match_eclasses_to_foreign_key_col");
                    let v = cat::get_mergejoin_opfamilies::call(scratch.mcx(), eqop)
                        .expect("get_mergejoin_opfamilies");
                    opfamilies = Some(v.iter().copied().collect());
                }
                if opfamilies.as_deref() == Some(root.ec(ec_id).ec_opfamilies.as_slice()) {
                    fkinfo.eclass[colno] = Some(ec_id);
                    fkinfo.fk_eclass_member[colno] = Some(e2);
                    return Some(ec_id);
                }
                /* otherwise done with this EC, move on */
                break;
            }
        }
    }
    None
}

/// `list_member(exprvars, x)` over a `Vec<Expr>` using node equality (the C
/// `list_member` uses `equal()`).
fn list_member_expr(list: &[Expr], x: &Expr) -> bool {
    list.iter().any(|e| ec_seam::equal::call(e, x))
}

/// re-export the EM value helper for callers that want a value EM.
#[allow(dead_code)]
pub(crate) fn em_clone(root: &PlannerInfo, em: EmId) -> EquivalenceMember {
    em_value(root, em)
}

/* keep PVC_RECURSE_* referenced for callers in other modules */
#[allow(dead_code)]
const _PVC_RECURSE: i32 = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS;
