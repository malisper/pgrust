//! equivclass.c — appendrel child-equivalence generation + `rebuild_eclass_
//! attr_needed`.

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use types_pathnodes::{
    EcId, PlannerInfo, RelId, RELOPT_BASEREL, RELOPT_JOINREL, RELOPT_OTHER_JOINREL,
};

use backend_optimizer_path_equivclass_ext_seams as ec_seam;
use backend_optimizer_util_relnode_seams as bms;

use crate::merge::{add_child_eq_member, em_expr};
use crate::relevance::{live_ec_ids, BMS_MULTIPLE};

const PVC_RECURSE_AGGREGATES: i32 = 0x0002;
const PVC_RECURSE_WINDOWFUNCS: i32 = 0x0008;
const PVC_INCLUDE_PLACEHOLDERS: i32 = 0x0010;

/* ======================================================================
 * add_child_rel_equivalences (equivclass.c:2833)
 * ==================================================================== */

/// `add_child_rel_equivalences(root, appinfo, parent_rel, child_rel)`
/// (equivclass.c:2833). `appinfo` is the single [`AppendRelInfo`] (carried by
/// `RelId` of the child here; the translation is routed through the
/// appendinfo.c seam).
pub fn add_child_rel_equivalences(
    root: &mut PlannerInfo,
    appinfo: RelId,
    parent_rel: RelId,
    child_rel: RelId,
) -> PgResult<()> {
    let top_parent_relids = root.rel(child_rel).top_parent_relids.clone();
    let child_relids = root.rel(child_rel).relids.clone();
    let child_relid = root.rel(child_rel).relid;

    debug_assert!(root.ec_merging_done);

    let parent_eci = root.rel(parent_rel).eclass_indexes.clone();
    let parent_is_baserel = root.rel(parent_rel).reloptkind == RELOPT_BASEREL;
    let child_top_parent = root.rel(child_rel).top_parent;

    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(&parent_eci, i);
        if i < 0 {
            break;
        }
        let cur_ec = EcId(i as u32);

        /* skip volatile ECs (would be dangerous to generate child EMs) */
        if root.ec(cur_ec).ec_has_volatile {
            continue;
        }
        debug_assert!(bms::relids_is_subset::call(
            &top_parent_relids,
            &root.ec(cur_ec).ec_relids
        ));

        let members = root.ec(cur_ec).ec_members.clone();
        for cur_em in members {
            if root.em(cur_em).em_is_const {
                continue;
            }
            debug_assert!(!root.em(cur_em).em_is_child);

            let em_relids = root.em(cur_em).em_relids.clone();
            if bms::relids_is_subset::call(&em_relids, &top_parent_relids)
                && !bms::relids_is_empty::call(&em_relids)
            {
                /* generate the transformed child version */
                let parent_expr = em_expr(root, cur_em);
                let child_expr = if parent_is_baserel {
                    ec_seam::adjust_appendrel_attrs::call(root, parent_expr, alloc::vec![appinfo])?
                } else {
                    ec_seam::adjust_appendrel_attrs_multilevel::call(
                        root,
                        parent_expr,
                        child_rel,
                        child_top_parent,
                    )?
                };

                /* transform em_relids (no pull_varnos: may have substituted a
                 * constant, but we don't want the child marked const) */
                let mut new_relids =
                    bms::relids_difference::call(&em_relids, &top_parent_relids);
                new_relids = bms::relids_add_members::call(new_relids, &child_relids);

                let jdomain = root
                    .em(cur_em)
                    .em_jdomain
                    .as_deref()
                    .cloned()
                    .unwrap_or_default();
                let datatype = root.em(cur_em).em_datatype;
                add_child_eq_member(
                    root, cur_ec, i, child_expr, new_relids, jdomain, cur_em, datatype, child_relid,
                );
            }
        }
    }
    Ok(())
}

/* ======================================================================
 * add_child_join_rel_equivalences (equivclass.c:2940)
 * ==================================================================== */

/// `add_child_join_rel_equivalences(root, nappinfos, appinfos, parent_joinrel,
/// child_joinrel)` (equivclass.c:2940). `appinfos` carried as a `Vec<RelId>`.
pub fn add_child_join_rel_equivalences(
    root: &mut PlannerInfo,
    appinfos: Vec<RelId>,
    parent_joinrel: RelId,
    child_joinrel: RelId,
) -> PgResult<()> {
    let top_parent_relids = root.rel(child_joinrel).top_parent_relids.clone();
    let child_relids = root.rel(child_joinrel).relids.clone();
    let parent_is_joinrel = root.rel(parent_joinrel).reloptkind == RELOPT_JOINREL;
    let child_top_parent = root.rel(child_joinrel).top_parent;

    /* consider only ECs mentioning the parent joinrel */
    let matching_ecs = crate::relevance::get_eclass_indexes_for_relids(root, &top_parent_relids);

    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(&matching_ecs, i);
        if i < 0 {
            break;
        }
        let cur_ec = EcId(i as u32);

        if root.ec(cur_ec).ec_has_volatile {
            continue;
        }
        debug_assert!(bms::relids_overlap::call(
            &top_parent_relids,
            &root.ec(cur_ec).ec_relids
        ));

        let members = root.ec(cur_ec).ec_members.clone();
        for cur_em in members {
            if root.em(cur_em).em_is_const {
                continue;
            }
            debug_assert!(!root.em(cur_em).em_is_child);

            let em_relids = root.em(cur_em).em_relids.clone();
            /* single-baserel exprs handled by add_child_rel_equivalences */
            if bms::relids_membership::call(&em_relids) != BMS_MULTIPLE {
                continue;
            }
            if bms::relids_overlap::call(&em_relids, &top_parent_relids) {
                let parent_expr = em_expr(root, cur_em);
                let child_expr = if parent_is_joinrel {
                    ec_seam::adjust_appendrel_attrs::call(root, parent_expr, appinfos.clone())?
                } else {
                    debug_assert!(
                        root.rel(parent_joinrel).reloptkind == RELOPT_OTHER_JOINREL
                    );
                    ec_seam::adjust_appendrel_attrs_multilevel::call(
                        root,
                        parent_expr,
                        child_joinrel,
                        child_top_parent,
                    )?
                };

                let mut new_relids =
                    bms::relids_difference::call(&em_relids, &top_parent_relids);
                new_relids = bms::relids_add_members::call(new_relids, &child_relids);

                let jdomain = root
                    .em(cur_em)
                    .em_jdomain
                    .as_deref()
                    .cloned()
                    .unwrap_or_default();
                let datatype = root.em(cur_em).em_datatype;
                /* store an OTHER_JOINREL child member in only the first
                 * component relid slot (so the iterator finds it once) */
                let first_relid =
                    bms::relids_next_member::call(&root.rel(child_joinrel).relids.clone(), -1);
                add_child_eq_member(
                    root,
                    cur_ec,
                    -1,
                    child_expr,
                    new_relids,
                    jdomain,
                    cur_em,
                    datatype,
                    first_relid as u32,
                );
            }
        }
    }
    Ok(())
}

/* ======================================================================
 * add_setop_child_rel_equivalences (equivclass.c:3084)
 * ==================================================================== */

/// `add_setop_child_rel_equivalences(root, child_rel, child_tlist,
/// setop_pathkeys)` (equivclass.c:3084). `child_tlist` is a list of
/// TargetEntry node handles; `setop_pathkeys` the parent's pathkeys.
pub fn add_setop_child_rel_equivalences(
    root: &mut PlannerInfo,
    child_rel: RelId,
    child_tlist: &[types_pathnodes::NodeId],
    setop_pathkeys: &[types_pathnodes::PathKey],
) -> PgResult<()> {
    let child_relids = root.rel(child_rel).relids.clone();
    let child_relid = root.rel(child_rel).relid;

    let mut pk_iter = setop_pathkeys.iter();
    for &tle in child_tlist {
        if ec_seam::target_entry_resjunk::call(root, tle) {
            continue;
        }
        let pk = pk_iter
            .next()
            .unwrap_or_else(|| panic!("too few pathkeys for set operation"));
        let ec = pk
            .pk_eclass
            .expect("add_setop_child_rel_equivalences: pathkey has no eclass");

        /* parent member is the first member in the EC's ec_members */
        let parent_em = *root
            .ec(ec)
            .ec_members
            .first()
            .expect("add_setop_child_rel_equivalences: EC has no members");
        let jdomain = root
            .em(parent_em)
            .em_jdomain
            .as_deref()
            .cloned()
            .unwrap_or_default();

        let tle_expr = ec_seam::target_entry_expr::call(root, tle);
        let datatype = ec_seam::expr_type::call(&tle_expr);

        add_child_eq_member(
            root,
            ec,
            -1,
            tle_expr,
            bms::relids_copy::call(&child_relids),
            jdomain,
            parent_em,
            datatype,
            child_relid,
        );
    }

    /* every EC received a new member; add them all to child_rel's indexes */
    let n = root.eq_classes.len();
    if n > 0 {
        let cur = root.rel(child_rel).eclass_indexes.clone();
        let added = bms::relids_add_range::call(cur, 0, (n - 1) as i32);
        root.rel_mut(child_rel).eclass_indexes = added;
    }
    Ok(())
}

/* ======================================================================
 * rebuild_eclass_attr_needed (equivclass.c:2574)
 * ==================================================================== */

/// `rebuild_eclass_attr_needed(root)` (equivclass.c:2574).
pub fn rebuild_eclass_attr_needed(root: &mut PlannerInfo) -> PgResult<()> {
    let live = live_ec_ids(root);
    for ec in live {
        debug_assert!(root.ec(ec).ec_childmembers.is_empty());

        if root.ec(ec).ec_members.len() > 1 && !root.ec(ec).ec_has_const {
            let members = root.ec(ec).ec_members.clone();
            for cur_em in members {
                let emexpr = em_expr(root, cur_em);
                let vars = ec_seam::pull_var_clause::call(
                    &emexpr,
                    PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
                );
                let ec_relids = root.ec(ec).ec_relids.clone();
                ec_seam::add_vars_to_attr_needed::call(root, vars, ec_relids)?;
            }
        }
    }
    Ok(())
}

#[allow(unused_imports)]
use types_pathnodes::Relids as _Relids;
