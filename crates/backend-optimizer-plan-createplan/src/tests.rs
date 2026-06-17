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
fn is_outer_join_classifies_join_types() {
    use crate::is_outer_join;
    // Inner / semi / right-semi / unique are NOT outer joins.
    assert!(!is_outer_join(0)); // JOIN_INNER
    assert!(!is_outer_join(4)); // JOIN_SEMI
    assert!(!is_outer_join(6)); // JOIN_RIGHT_SEMI
    assert!(!is_outer_join(8)); // JOIN_UNIQUE_OUTER
    assert!(!is_outer_join(9)); // JOIN_UNIQUE_INNER
    // LEFT / FULL / RIGHT / ANTI / RIGHT_ANTI ARE outer joins.
    assert!(is_outer_join(1)); // JOIN_LEFT
    assert!(is_outer_join(2)); // JOIN_FULL
    assert!(is_outer_join(3)); // JOIN_RIGHT
    assert!(is_outer_join(5)); // JOIN_ANTI
    assert!(is_outer_join(7)); // JOIN_RIGHT_ANTI
}

#[test]
fn jointype_path_to_node_maps_enum_discriminants() {
    use crate::jointype_path_to_node;
    use types_nodes::jointype::JoinType as N;
    assert_eq!(jointype_path_to_node(0), N::JOIN_INNER);
    assert_eq!(jointype_path_to_node(1), N::JOIN_LEFT);
    assert_eq!(jointype_path_to_node(2), N::JOIN_FULL);
    assert_eq!(jointype_path_to_node(3), N::JOIN_RIGHT);
    assert_eq!(jointype_path_to_node(4), N::JOIN_SEMI);
    assert_eq!(jointype_path_to_node(5), N::JOIN_ANTI);
    assert_eq!(jointype_path_to_node(6), N::JOIN_RIGHT_SEMI);
    assert_eq!(jointype_path_to_node(7), N::JOIN_RIGHT_ANTI);
    assert_eq!(jointype_path_to_node(8), N::JOIN_UNIQUE_OUTER);
    assert_eq!(jointype_path_to_node(9), N::JOIN_UNIQUE_INNER);
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

// ---------------------------------------------------------------------------
// F2c simple-converter tests.
// ---------------------------------------------------------------------------

use mcx::MemoryContext;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::RTEKind;
use types_pathnodes::planner_run::PlannerRun;

/// Build a base-rel `Path` whose parent `RelOptInfo` has `relid = 1`, plus a
/// `PlannerRun` whose `simple_rte_array[1]` resolves to an RTE of the given
/// kind. Mirrors `setup_simple_rel_arrays` interning the top `rtable` entry.
fn scan_setup<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rtekind: RTEKind,
    pathtype: types_nodes::nodes::NodeTag,
) -> (PlannerInfo, PlannerRun<'mcx>, types_pathnodes::PathId) {
    let mut root = PlannerInfo::default();
    let mut run = PlannerRun::new(mcx);

    let mut rel = RelOptInfo::default();
    rel.reloptkind = RELOPT_BASEREL;
    rel.rtekind = rtekind as u32;
    rel.relid = 1;
    rel.min_attr = 1;
    rel.max_attr = 1;
    let rel_id: RelId = root.alloc_rel(rel);

    // simple_rte_array: slot 0 is the unused C placeholder, slot 1 = RT index 1.
    let mut rte0 = types_nodes::parsenodes::RangeTblEntry::new_in(mcx);
    rte0.rtekind = RTEKind::RTE_RELATION;
    let id0 = run.intern_rte(rte0);
    let mut rte1 = types_nodes::parsenodes::RangeTblEntry::new_in(mcx);
    rte1.rtekind = rtekind;
    let id1 = run.intern_rte(rte1);
    root.simple_rte_array = alloc::vec![id0, id1];

    let path = Path {
        type_: pathtype,
        pathtype,
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
    (root, run, path_id)
}

#[test]
fn create_seqscan_plan_builds_seqscan_node() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut root, run, path_id) =
        scan_setup(mcx, RTEKind::RTE_RELATION, crate::T_SeqScan);
    let plan = crate::create_seqscan_plan(
        mcx,
        &mut root,
        &run,
        path_id,
        alloc::vec::Vec::new(),
        alloc::vec::Vec::new(),
    )
    .expect("create_seqscan_plan");
    match plan {
        Node::SeqScan(s) => {
            assert_eq!(s.scan.scanrelid, 1);
            // No clauses, no tlist: NIL qual / tlist.
            assert!(s.scan.plan.qual.is_none());
            assert!(s.scan.plan.targetlist.is_none());
        }
        other => panic!("expected SeqScan, got {:?}", other.tag()),
    }
}

#[test]
fn create_resultscan_plan_builds_result_node() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut root, run, path_id) =
        scan_setup(mcx, RTEKind::RTE_RESULT, crate::T_Result);
    let plan = crate::create_resultscan_plan(
        mcx,
        &mut root,
        &run,
        path_id,
        alloc::vec::Vec::new(),
        alloc::vec::Vec::new(),
    )
    .expect("create_resultscan_plan");
    match plan {
        Node::Result(r) => {
            // No clauses: resconstantqual is NIL.
            assert!(r.resconstantqual.is_none());
            assert!(r.plan.lefttree.is_none());
        }
        other => panic!("expected Result, got {:?}", other.tag()),
    }
}

#[test]
fn create_functionscan_plan_builds_functionscan_node() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut root, run, path_id) =
        scan_setup(mcx, RTEKind::RTE_FUNCTION, crate::T_FunctionScan);
    let plan = crate::create_functionscan_plan(
        mcx,
        &mut root,
        &run,
        path_id,
        alloc::vec::Vec::new(),
        alloc::vec::Vec::new(),
    )
    .expect("create_functionscan_plan");
    match plan {
        Node::FunctionScan(f) => {
            assert_eq!(f.scan.scanrelid, 1);
            // Empty RTE functions -> NIL.
            assert!(f.functions.is_none());
            assert!(!f.funcordinality);
        }
        other => panic!("expected FunctionScan, got {:?}", other.tag()),
    }
}

#[test]
fn create_namedtuplestorescan_plan_builds_node() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut root, run, path_id) =
        scan_setup(mcx, RTEKind::RTE_NAMEDTUPLESTORE, crate::T_NamedTuplestoreScan);
    let plan = crate::create_namedtuplestorescan_plan(
        mcx,
        &mut root,
        &run,
        path_id,
        alloc::vec::Vec::new(),
        alloc::vec::Vec::new(),
    )
    .expect("create_namedtuplestorescan_plan");
    match plan {
        Node::NamedTuplestoreScan(n) => {
            assert_eq!(n.scan.scanrelid, 1);
            assert!(n.enrname.is_none());
        }
        other => panic!("expected NamedTuplestoreScan, got {:?}", other.tag()),
    }
}

#[test]
fn create_tidscan_plan_builds_tidscan_node() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut root, run, _seq_path) =
        scan_setup(mcx, RTEKind::RTE_RELATION, crate::T_SeqScan);
    // Build a TidPath (empty tidquals) over the same base rel.
    let rel_id = root.path(_seq_path).base().parent;
    let tid_path = types_pathnodes::TidPath {
        path: Path {
            type_: crate::T_TidScan,
            pathtype: crate::T_TidScan,
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
        },
        tidquals: alloc::vec::Vec::new(),
    };
    let path_id = root.alloc_path(PathNode::TidPath(tid_path));
    let plan = crate::create_tidscan_plan(
        mcx,
        &mut root,
        &run,
        path_id,
        alloc::vec::Vec::new(),
        alloc::vec::Vec::new(),
    )
    .expect("create_tidscan_plan");
    match plan {
        Node::TidScan(t) => {
            assert_eq!(t.scan.scanrelid, 1);
            assert!(t.tidquals.is_none());
        }
        other => panic!("expected TidScan, got {:?}", other.tag()),
    }
}

#[test]
fn create_valuesscan_plan_builds_valuesscan_node() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let (mut root, run, path_id) =
        scan_setup(mcx, RTEKind::RTE_VALUES, crate::T_ValuesScan);
    let plan = crate::create_valuesscan_plan(
        mcx,
        &mut root,
        &run,
        path_id,
        alloc::vec::Vec::new(),
        alloc::vec::Vec::new(),
    )
    .expect("create_valuesscan_plan");
    match plan {
        Node::ValuesScan(v) => {
            assert_eq!(v.scan.scanrelid, 1);
            // Empty RTE values_lists -> empty carrier.
            assert!(v.values_lists.is_empty());
        }
        other => panic!("expected ValuesScan, got {:?}", other.tag()),
    }
}
