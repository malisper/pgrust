//! equivclass.c — base implied-equality generation (equivclass.c:1188..1500).

extern crate alloc;

use alloc::vec::Vec;

use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{EcId, EmId, PlannerInfo, RELOPT_BASEREL};

use equivclass_ext_seams as ec_seam;
use relnode_seams as bms;

use crate::derives::ec_add_derived_clause;
use crate::merge::{em_expr_owned, em_expr_ref};
use crate::relevance::{live_ec_ids, select_equality_operator, BMS_MULTIPLE};

const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_INCLUDE_PLACEHOLDERS: i32 = 0x0010;

/* ======================================================================
 * generate_base_implied_equalities (equivclass.c:1188)
 * ==================================================================== */

/// `generate_base_implied_equalities(root)` (equivclass.c:1188).
pub fn generate_base_implied_equalities<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
) -> PgResult<()> {
    /* done absorbing equivalences: no further merging; ECs are canonical */
    root.ec_merging_done = true;

    /* iterate ECs by position (== Ec index used by eclass_indexes). Note we
     * include only live (non-merged) ECs, matching C's post-delete list. */
    let live = live_ec_ids(root);
    for ec in live {
        debug_assert!(root.ec(ec).ec_merged.is_none());
        debug_assert!(!root.ec(ec).ec_broken);

        let mut can_generate_joinclause = false;

        if root.ec(ec).ec_members.len() > 1 {
            if root.ec(ec).ec_has_const {
                generate_base_implied_equalities_const(root, run, ec)?;
            } else {
                generate_base_implied_equalities_no_const(root, run, ec)?;
            }
            /* recover if we failed to generate required derived clauses */
            if root.ec(ec).ec_broken {
                generate_base_implied_equalities_broken(root, run, ec)?;
            }
            let relids = root.ec(ec).ec_relids.clone();
            can_generate_joinclause =
                bms::relids_membership::call(&relids) == BMS_MULTIPLE;
        }

        /* mark base rels cited in the EC with this eclass index */
        let ec_index = ec.0 as i32;
        let ec_relids = root.ec(ec).ec_relids.clone();
        let mut i: i32 = -1;
        loop {
            i = bms::relids_next_member::call(&ec_relids, i);
            if i <= 0 {
                break;
            }
            if i == root.group_rtindex {
                continue;
            }
            match root.simple_rel_array[i as usize] {
                None => {
                    debug_assert!(bms::relids_is_member::call(i, &root.outer_join_rels));
                    continue;
                }
                Some(rel_id) => {
                    debug_assert!(root.rel(rel_id).reloptkind == RELOPT_BASEREL);
                    let cur = root.rel(rel_id).eclass_indexes.clone();
                    let added = bms::relids_add_member::call(cur, ec_index);
                    root.rel_mut(rel_id).eclass_indexes = added;
                    if can_generate_joinclause {
                        root.rel_mut(rel_id).has_eclass_joins = true;
                    }
                }
            }
        }
    }
    Ok(())
}

/* ======================================================================
 * generate_base_implied_equalities_const (equivclass.c:1272)
 * ==================================================================== */

fn generate_base_implied_equalities_const<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    ec: EcId,
) -> PgResult<()> {
    /* trivial single "var = const" clause: push the original back */
    if root.ec(ec).ec_members.len() == 2 && root.ec(ec).ec_sources.len() == 1 {
        let restrictinfo = root.ec(ec).ec_sources[0];
        ec_seam::distribute_restrictinfo_to_rels::call(run, root, restrictinfo)?;
        return Ok(());
    }

    debug_assert!(root.ec(ec).ec_childmembers.is_empty());

    /* find the constant member to use (prefer an actual Const) */
    let mut const_em: Option<EmId> = None;
    for &cur_em in &root.ec(ec).ec_members.clone() {
        if root.em(cur_em).em_is_const {
            const_em = Some(cur_em);
            if em_expr_ref(root, cur_em).is_const() {
                break;
            }
        }
    }
    let const_em = const_em.expect("generate_base_implied_equalities_const: no const member");

    /* derive an equality against each other member */
    let members = root.ec(ec).ec_members.clone();
    for cur_em in members {
        debug_assert!(!root.em(cur_em).em_is_child);
        if cur_em == const_em {
            continue;
        }
        let eq_op = select_equality_operator(
            root.ec(ec),
            root.em(cur_em).em_datatype,
            root.em(const_em).em_datatype,
        );
        if !crate::relevance::oid_is_valid(eq_op) {
            root.ec_mut(ec).ec_broken = true;
            break;
        }

        /* use the constant's em_jdomain as qualscope */
        let collation = root.ec(ec).ec_collation;
        let min_security = root.ec(ec).ec_min_security;
        let cur_expr = em_expr_owned(root, run, cur_em)?;
        let const_expr = em_expr_owned(root, run, const_em)?;
        let const_jd = root
            .em(const_em)
            .em_jdomain
            .as_ref()
            .map(|jd| jd.jd_relids.clone())
            .unwrap_or(None);
        let cur_is_const = root.em(cur_em).em_is_const;

        let rinfo = ec_seam::process_implied_equality::call(
            run,
            root,
            eq_op,
            collation,
            cur_expr,
            const_expr,
            const_jd,
            min_security,
            cur_is_const,
        )?;

        /* if not degenerate and mergejoinable, mark + save as derived */
        if let Some(rinfo) = rinfo {
            if !root.rinfo(rinfo).mergeopfamilies.is_empty() {
                /* not redundant, so don't set parent_ec */
                {
                    let r = root.rinfo_mut(rinfo);
                    r.left_ec = Some(ec);
                    r.right_ec = Some(ec);
                    r.left_em = Some(cur_em);
                    r.right_em = Some(const_em);
                }
                ec_add_derived_clause(root, ec, rinfo);
            }
        }
    }
    Ok(())
}

/* ======================================================================
 * generate_base_implied_equalities_no_const (equivclass.c:1371)
 * ==================================================================== */

fn generate_base_implied_equalities_no_const<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    ec: EcId,
) -> PgResult<()> {
    /* track last-seen member for each base relation */
    let mut prev_ems: Vec<Option<EmId>> = alloc::vec![None; root.simple_rel_array_size as usize];

    debug_assert!(root.ec(ec).ec_childmembers.is_empty());

    let members = root.ec(ec).ec_members.clone();
    'scan: for cur_em in members.iter().copied() {
        debug_assert!(!root.em(cur_em).em_is_child);

        let relid = match bms::relids_get_singleton_member::call(&root.em(cur_em).em_relids) {
            Some(r) => r,
            None => continue,
        };
        debug_assert!((relid as i32) < root.simple_rel_array_size);

        if let Some(prev_em) = prev_ems[relid as usize] {
            let eq_op = select_equality_operator(
                root.ec(ec),
                root.em(prev_em).em_datatype,
                root.em(cur_em).em_datatype,
            );
            if !crate::relevance::oid_is_valid(eq_op) {
                root.ec_mut(ec).ec_broken = true;
                break 'scan;
            }

            let collation = root.ec(ec).ec_collation;
            let min_security = root.ec(ec).ec_min_security;
            let prev_expr = em_expr_owned(root, run, prev_em)?;
            let cur_expr = em_expr_owned(root, run, cur_em)?;
            let cur_relids = root.em(cur_em).em_relids.clone();

            let rinfo = ec_seam::process_implied_equality::call(
                run,
                root,
                eq_op,
                collation,
                prev_expr,
                cur_expr,
                cur_relids,
                min_security,
                false,
            )?;

            if let Some(rinfo) = rinfo {
                if !root.rinfo(rinfo).mergeopfamilies.is_empty() {
                    let r = root.rinfo_mut(rinfo);
                    r.left_ec = Some(ec);
                    r.right_ec = Some(ec);
                    r.left_em = Some(prev_em);
                    r.right_em = Some(cur_em);
                }
            }
        }
        prev_ems[relid as usize] = Some(cur_em);
    }

    /* ensure all Vars used in the member clauses are available at join nodes */
    let members2 = root.ec(ec).ec_members.clone();
    for cur_em in members2 {
        let emexpr = em_expr_ref(root, cur_em);
        let vars = ec_seam::pull_var_clause::call(
            emexpr,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );
        let ec_relids = root.ec(ec).ec_relids.clone();
        ec_seam::add_vars_to_targetlist::call(run.mcx(), root, vars, ec_relids)?;
    }
    Ok(())
}

/* ======================================================================
 * generate_base_implied_equalities_broken (equivclass.c:1487)
 * ==================================================================== */

fn generate_base_implied_equalities_broken<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    ec: EcId,
) -> PgResult<()> {
    let has_const = root.ec(ec).ec_has_const;
    let sources = root.ec(ec).ec_sources.clone();
    for restrictinfo in sources {
        let required = root.rinfo(restrictinfo).required_relids.clone();
        if has_const || bms::relids_membership::call(&required) != BMS_MULTIPLE {
            ec_seam::distribute_restrictinfo_to_rels::call(run, root, restrictinfo)?;
        }
    }
    Ok(())
}

/* keep imports referenced */
#[allow(dead_code)]
fn _unused(_: Oid) {}
