//! GROUP BY (initsplan.c) — `remove_useless_groupby_columns`.

extern crate alloc;

use alloc::vec::Vec;

use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PlannerInfo, Relids};

use backend_optimizer_util_relnode_seams as bms;
use backend_nodes_core_nodefuncs_seams_alias as nf;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

// nodeFuncs seam crate alias (kept as a module re-export for readability).
mod backend_nodes_core_nodefuncs_seams_alias {
    pub use backend_nodes_nodeFuncs_seams::*;
}

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h).
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -8;
/// `RELKIND_PARTITIONED_TABLE` (`'p'`).
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;
/// `RTE_RELATION` discriminant (parsenodes.h `RTEKind`).
const RTE_RELATION: i32 = 0;
/// `bms_membership` codes.
const BMS_MULTIPLE: i32 = 2;
/// `PG_INT32_MAX`.
const PG_INT32_MAX: i32 = i32::MAX;

/// `bms_is_member(x, set)` over a `Relids`.
#[inline]
fn is_member(x: i32, set: &Relids) -> bool {
    bms::relids_is_member::call(x, set)
}

/// `bms_subset_compare(a, b) == BMS_SUBSET1` — a is a *proper* subset of b.
/// (BMS_SUBSET1 is returned only for a strict subset; equal sets yield
/// BMS_EQUAL, which the C `!= BMS_SUBSET1` test rejects.)
#[inline]
fn is_proper_subset1(a: &Relids, b: &Relids) -> bool {
    bms::relids_is_subset::call(a, b) && !bms::relids_equal::call(a, b)
}

/// `remove_useless_groupby_columns` (initsplan.c:412).
///
/// Remove any GROUP BY columns that are redundant due to being functionally
/// dependent on other GROUP BY columns (covered by a NOT NULL unique index).
///
/// Reconciliation: C reads `parse->rtable` (RTE kind/inh/relkind) and
/// `parse->targetList`. `PlannerInfo.parse`/`simple_rte_array` are opaque, so
/// the per-RTE fields are reached through the `rte_kind_inh_relkind` ext-seam,
/// and the `SortGroupClause -> TargetEntry -> Var` resolution through the
/// nodeFuncs `sortgroupclause_info` / `get_sortgroupref_tle` / `targetentry_info`
/// seams over `root.processed_tlist` (the planner's resolved targetlist, which
/// the processed GROUP BY clauses reference).
pub fn remove_useless_groupby_columns(root: &mut PlannerInfo, run: &PlannerRun<'_>) {
    // No chance to do anything if there are less than two GROUP BY items.
    if root.processed_groupClause.len() < 2 {
        return;
    }
    // Don't fiddle with the GROUP BY clause if the query has grouping sets.
    if !run.resolve(root.parse).groupingSets.is_empty() {
        return;
    }

    let rtable_len = run.resolve(root.parse).rtable.len();
    let target_list: Vec<NodeId> = root.processed_tlist.clone();
    let group_clause: Vec<NodeId> = root.processed_groupClause.clone();

    // groupbyattnos[k] = bitmapset of column attnos of RTE k that are GROUP BY
    // items (indexed 0..=rtable_len; slot 0 unused).
    let mut groupbyattnos: Vec<Relids> = (0..=rtable_len).map(|_| None).collect();
    let mut tryremove = false;

    for &sgc in &group_clause {
        let sgc_info = nf::sortgroupclause_info::call(root, sgc);
        let tle = nf::get_sortgroupref_tle::call(root, sgc_info.tle_sort_group_ref, &target_list);
        let te_info = nf::targetentry_info::call(root, tle);
        let expr = root.node(te_info.expr).clone();

        // Ignore non-Vars and Vars from other query levels.
        let var = match &expr {
            types_nodes::primnodes::Expr::Var(v) if v.varlevelsup == 0 => v.clone(),
            _ => continue,
        };

        let relid = var.varno;
        debug_assert!(relid as usize <= rtable_len);

        // If this isn't the first column for this relation we now have multiple
        // columns, so there might be some that can be removed.
        tryremove |= !bms::relids_is_empty::call(&groupbyattnos[relid as usize]);
        let cur = groupbyattnos[relid as usize].take();
        groupbyattnos[relid as usize] =
            bms::relids_add_member::call(cur, var.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER);
    }

    // No Vars or didn't find multiple Vars for any relation? Nothing to remove.
    if !tryremove {
        return;
    }

    // surplusvars[k] = bitmapset of removable GROUP BY column attnos of RTE k.
    let mut surplusvars: Option<Vec<Relids>> = None;

    for relid in 1..=rtable_len {
        let (rtekind, rte_inh, rte_relkind) = initext::rte_kind_inh_relkind::call(root, relid as i32);

        // Only plain relations could have primary-key constraints.
        if rtekind != RTE_RELATION {
            continue;
        }
        // Skip inheritance parent tables (children may cause duplicate rows);
        // partitioned tables are exempt.
        if rte_inh && rte_relkind != RELKIND_PARTITIONED_TABLE {
            continue;
        }
        // Nothing to do unless this rel has multiple Vars in GROUP BY.
        let relattnos = bms::relids_copy::call(&groupbyattnos[relid]);
        if bms::relids_membership::call(&relattnos) != BMS_MULTIPLE {
            continue;
        }

        let rel_id = match root.simple_rel_array[relid] {
            Some(id) => id,
            None => continue,
        };

        let mut best_keycolumns: Relids = None;
        let mut best_nkeycolumns: i32 = PG_INT32_MAX;

        // Check each index for columns that are a proper subset of the grouping
        // columns for this relation.
        let indexlist = root.rel(rel_id).indexlist.clone();
        let notnullattnums = bms::relids_copy::call(&root.rel(rel_id).notnullattnums);
        for index in &indexlist {
            // Skip non-unique and deferrable and predicate indexes.
            if !index.unique || !index.immediate || !index.indpred.is_empty() {
                continue;
            }
            // We currently don't support expression indexes.
            if !index.indexprs.is_empty() {
                continue;
            }

            let mut ind_attnos: Relids = None;
            let mut nulls_check_ok = true;
            for i in 0..index.nkeycolumns as usize {
                // Index columns must all be NOT NULL, unless the index is
                // NULLS NOT DISTINCT (then at most 1 NULL row, FD maintained).
                if !index.nullsnotdistinct
                    && !is_member(index.indexkeys[i], &notnullattnums)
                {
                    nulls_check_ok = false;
                    break;
                }
                ind_attnos = bms::relids_add_member::call(
                    ind_attnos,
                    index.indexkeys[i] - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
                );
            }
            if !nulls_check_ok {
                continue;
            }

            // Skip indexes whose columns aren't a proper subset of the GROUP BY.
            if !is_proper_subset1(&ind_attnos, &relattnos) {
                continue;
            }

            // Record the index with the fewest columns (removes the most).
            if index.nkeycolumns < best_nkeycolumns {
                best_keycolumns = ind_attnos;
                best_nkeycolumns = index.nkeycolumns;
            }
        }

        // Did we find a suitable index?
        if !bms::relids_is_empty::call(&best_keycolumns) {
            if surplusvars.is_none() {
                surplusvars = Some((0..=rtable_len).map(|_| None).collect());
            }
            let sv = surplusvars.as_mut().unwrap();
            sv[relid] = bms::relids_difference::call(&relattnos, &best_keycolumns);
        }
    }

    // If we found any surplus Vars, build a new GROUP BY clause without them.
    if let Some(surplusvars) = surplusvars {
        let mut new_groupby: Vec<NodeId> = Vec::new();

        for &sgc in &group_clause {
            let sgc_info = nf::sortgroupclause_info::call(root, sgc);
            let tle = nf::get_sortgroupref_tle::call(root, sgc_info.tle_sort_group_ref, &target_list);
            let te_info = nf::targetentry_info::call(root, tle);
            let expr = root.node(te_info.expr).clone();

            let keep = match &expr {
                types_nodes::primnodes::Expr::Var(v) if v.varlevelsup == 0 => {
                    !is_member(
                        v.varattno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER,
                        &surplusvars[v.varno as usize],
                    )
                }
                // non-Vars and outer Vars are always kept
                _ => true,
            };
            if keep {
                new_groupby.push(sgc);
            }
        }

        root.processed_groupClause = new_groupby;
    }
}
