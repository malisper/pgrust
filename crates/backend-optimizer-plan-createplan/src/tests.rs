//! Unit tests for the createplan F2b scan-core helpers (`use_physical_tlist`,
//! `get_gating_quals`, the dispatch tlist-selection flags). These exercise the
//! pure-logic branches that don't require the unported per-scan converters
//! (which are seam-panics until F2c).

extern crate std;

use alloc::boxed::Box;

use types_pathnodes::{
    Path, PathNode, PathTarget, PlannerInfo, RelId, RelOptInfo, RELOPT_BASEREL, RTE_RELATION,
};

use crate::{get_gating_quals, use_physical_tlist, CP_EXACT_TLIST, CP_SMALL_TLIST};

/// Build a minimal base-relation `Path` over an `RTE_RELATION` `RelOptInfo`,
/// returning the populated `PlannerInfo` and the path's `PathId`.
fn base_rel_planner() -> (PlannerInfo, types_pathnodes::PathId) {
    let mut root = PlannerInfo::default();

    let mut rel = RelOptInfo::default();
    rel.reloptkind = RELOPT_BASEREL;
    rel.rtekind = RTE_RELATION;
    // No system columns / whole-row vars requested: attr_needed must be present
    // for `i in min_attr..=0`; keep min_attr = 1 so that loop is empty.
    rel.min_attr = 1;
    rel.max_attr = 1;
    let rel_id: RelId = root.alloc_rel(rel);

    let path = Path {
        type_: crate::T_SeqScan,
        pathtype: crate::T_SeqScan,
        parent: rel_id,
        pathtarget: Some(Box::new(PathTarget::default())),
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 0.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        pathkeys: alloc::vec::Vec::new(),
    };
    let path_id = root.alloc_path(PathNode::Path(path));
    (root, path_id)
}

#[test]
fn use_physical_tlist_rejects_exact_or_small_tlist() {
    let (root, path_id) = base_rel_planner();
    // CP_EXACT_TLIST and CP_SMALL_TLIST both forbid the physical tlist.
    assert!(!use_physical_tlist(&root, path_id, CP_EXACT_TLIST));
    assert!(!use_physical_tlist(&root, path_id, CP_SMALL_TLIST));
}

#[test]
fn use_physical_tlist_accepts_plain_base_relation() {
    let (root, path_id) = base_rel_planner();
    // A plain RTE_RELATION base rel with no flags, no placeholders, no system
    // columns requested, and an empty pathtarget should accept the physical
    // tlist.
    assert!(use_physical_tlist(&root, path_id, 0));
}

#[test]
fn get_gating_quals_empty_without_pseudoconstants() {
    let (mut root, _path_id) = base_rel_planner();
    // hasPseudoConstantQuals defaults to false, so get_gating_quals short-circuits
    // to the empty list regardless of the clause list.
    assert!(!root.hasPseudoConstantQuals);
    let gating = get_gating_quals(&mut root, &[]);
    assert!(gating.is_empty());
}
