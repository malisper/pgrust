//! `optimizer/util/joininfo.c` — joininfo list manipulation.

use backend_nodes_core::makefuncs::make_bool_const;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerInfo, RelId, Relids, RinfoId};

use crate::bms;
use crate::ext_seam;
use crate::restrictinfo::make_restrictinfo;
use backend_optimizer_path_equivclass_seams as ec_seam;

/// `have_relevant_joinclause`
///		Detect whether there is a joinclause that involves the two given
///		relations.
pub fn have_relevant_joinclause(root: &PlannerInfo, rel1: RelId, rel2: RelId) -> bool {
    let mut result = false;

    // We could scan either relation's joininfo list; use the shorter one.
    let r1 = root.rel(rel1);
    let r2 = root.rel(rel2);
    let (joininfo, other_relids): (&[RinfoId], &Relids) = if r1.joininfo.len() <= r2.joininfo.len() {
        (&r1.joininfo, &r2.relids)
    } else {
        (&r2.joininfo, &r1.relids)
    };

    for &rid in joininfo {
        if bms::relids_overlap::call(other_relids, &root.rinfo(rid).required_relids) {
            result = true;
            break;
        }
    }

    // Also check the EquivalenceClass data structure, which might contain
    // relationships not emitted into the joininfo lists.
    if !result && r1.has_eclass_joins && r2.has_eclass_joins {
        result = ec_seam::have_relevant_eclass_joinclause::call(root, rel1, rel2);
    }

    result
}

/// `add_join_clause_to_rels`
///	  Add 'restrictinfo' to the joininfo list of each relation it requires.
pub fn add_join_clause_to_rels<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    restrictinfo: RinfoId,
    join_relids: &Relids,
) -> PgResult<()> {
    // Don't add the clause if it is always true.
    let clause_expr = root.node(root.rinfo(restrictinfo).clause).clone();
    if ext_seam::restriction_is_always_true::call(root, &clause_expr) {
        return Ok(());
    }

    // Substitute the origin qual with constant-FALSE if it is provably always
    // false.  Keep the same rinfo_serial, and reset the last_rinfo_serial
    // counter, to ensure the "same" qual condition gets identical serial numbers.
    let mut restrictinfo = restrictinfo;
    if ext_seam::restriction_is_always_false::call(root, &clause_expr) {
        let ri = root.rinfo(restrictinfo).clone();
        let save_rinfo_serial = ri.rinfo_serial;
        let save_last_rinfo_serial = root.last_rinfo_serial;

        let false_const = Expr::Const(make_bool_const(false, false));
        restrictinfo = make_restrictinfo(
            root,
            false_const,
            ri.is_pushed_down,
            ri.has_clone,
            ri.is_clone,
            ri.pseudoconstant,
            0, /* security_level */
            ri.required_relids.clone(),
            ri.incompatible_relids.clone(),
            ri.outer_relids.clone(),
        )?;
        root.rinfo_mut(restrictinfo).rinfo_serial = save_rinfo_serial;
        root.last_rinfo_serial = save_last_rinfo_serial;
    }

    let mut cur_relid: i32 = -1;
    loop {
        cur_relid = bms::relids_next_member::call(join_relids, cur_relid);
        if cur_relid < 0 {
            break;
        }
        // We only need to add the clause to baserels.
        let rel = match ext_seam::find_base_rel_ignore_join::call(run, root, cur_relid) {
            Some(r) => r,
            None => continue,
        };
        root.rel_mut(rel).joininfo.push(restrictinfo);
    }
    Ok(())
}

/// `remove_join_clause_from_rels`
///	  Delete 'restrictinfo' from all the joininfo lists it is in.
pub fn remove_join_clause_from_rels<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    restrictinfo: RinfoId,
    join_relids: &Relids,
) {
    let mut cur_relid: i32 = -1;
    loop {
        cur_relid = bms::relids_next_member::call(join_relids, cur_relid);
        if cur_relid < 0 {
            break;
        }
        // We would only have added the clause to baserels.
        let rel = match ext_seam::find_base_rel_ignore_join::call(run, root, cur_relid) {
            Some(r) => r,
            None => continue,
        };
        // Remove the restrictinfo from the list.  Pointer (handle) comparison is
        // sufficient.
        let joininfo = &mut root.rel_mut(rel).joininfo;
        debug_assert!(joininfo.contains(&restrictinfo));
        if let Some(pos) = joininfo.iter().position(|&r| r == restrictinfo) {
            joininfo.remove(pos);
        }
    }
}
