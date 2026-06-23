//! Unit test — `add_paths_to_joinrel` over a synthetic 2-rel JOIN_INNER.
//!
//! The enumeration control flow ports 1:1; every subsystem boundary is a seam
//! defaulting to a loud panic. So the test installs in-test seam impls first
//! (single-threaded; `--test-threads=1`). The relids seams model the planner
//! convention that the empty set is `None`. The synthetic rels are
//! unparameterized baserels, so `path_req_outer` is always the empty `Relids`
//! and `calc_nestloop_required_outer` returns `None`.
//!
//! The GATE drives a JOIN_INNER over two synthetic baserels and asserts that
//! `add_paths_to_joinrel` actually installs a nestloop (`NestPath`) onto the
//! joinrel's pathlist via the real `match_unsorted_outer` → `try_nestloop_path`
//! path (mergejoin/hashjoin/material/memoize gated off so the nestloop branch
//! is exercised).

use super::*;

use mcx::MemoryContext;

use ::nodes::nodes::NodeTag;
use pathnodes::optimizer_plan::JoinCostWorkspace;
use pathnodes::{
    JoinPath as TJoinPath, NestPath, Path, PathNode, RelOptInfo, RELOPT_BASEREL, RTE_RELATION,
};

/// A minimal unparameterized base `Path` owned by `parent`.
fn mk_path(parent: RelId) -> Path {
    Path {
        type_: NodeTag(0),
        pathtype: NodeTag(0),
        parent,
        pathtarget: None,
        param_info: None,
        parallel_aware: false,
        parallel_safe: false,
        parallel_workers: 0,
        rows: 100.0,
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 10.0,
        pathkeys: Vec::new(),
    }
}

/// A minimal baserel `RelOptInfo` at the given RT index (relids = `None`
/// placeholder; the test relids seams treat membership/overlap abstractly).
fn mk_rel(relid: i32) -> RelOptInfo {
    RelOptInfo {
        reloptkind: RELOPT_BASEREL,
        relid: relid as u32,
        rtekind: RTE_RELATION,
        rows: 100.0,
        consider_startup: true,
        ..RelOptInfo::default()
    }
}

/// A minimal `SpecialJoinInfo` for a plain inner join (all sets empty).
fn mk_sjinfo() -> SpecialJoinInfo {
    SpecialJoinInfo {
        min_lefthand: None,
        min_righthand: None,
        syn_lefthand: None,
        syn_righthand: None,
        jointype: JOIN_INNER,
        ojrelid: 0,
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: Vec::new(),
        semi_rhs_exprs: Vec::new(),
    }
}

/// Install in-test seam impls routing JOIN_INNER down the nestloop branch and
/// letting `create_nestloop_path` + `add_path` install a `NestPath`.
fn install_join_seams() {
    // relids: empty set is None; every set op collapses to "empty / no overlap".
    bms::relids_copy::set(|_a| None);
    bms::relids_is_empty::set(|a| a.is_none());
    bms::relids_overlap::set(|_a, _b| false);
    bms::relids_nonempty_difference::set(|_a, _b| false);
    bms::relids_is_member::set(|_x, _a| false);
    bms::relids_is_subset::set(|_a, _b| true);
    bms::relids_add_members::set(|a, _b| a);
    bms::relids_join::set(|a, _b| a);
    bms::relids_intersect::set(|_a, _b| None);

    // joinpath-local bms helper.
    jp::bms_difference::set(|_a, _b| None);
    jp::innerrel_is_unique::set(|_r, _run, _j, _o, _i, _jt, _rl, _f| false);

    // pathkeys: a join keeps no order in this synthetic setup.
    jp::build_join_pathkeys::set(|_r, _jr, _jt, _opk| Ok(Vec::new()));
    jp::find_mergeclauses_for_outer_pathkeys::set(|_r, _pk, _ri| Ok(Vec::new()));

    // nestloop parameterization: unparameterized => empty required_outer.
    jp::calc_nestloop_required_outer::set(|_or, _op, _ir, _ip| None);
    jp::path_is_reparameterizable_by_child::set(|_r, _p, _c| true);

    // preliminary cost + precheck: cheap, always accepted.
    jp::initial_cost_nestloop::set(|_run, _root, _jt, _o, _i, _extra| {
        Ok(JoinCostWorkspace {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 5.0,
            run_cost: 5.0,
            inner_run_cost: 0.0,
            inner_rescan_run_cost: 0.0,
            outer_rows: 0.0,
            inner_rows: 0.0,
            outer_skip_rows: 0.0,
            inner_skip_rows: 0.0,
            numbuckets: 0,
            numbatches: 0,
            inner_rows_total: 0.0,
        })
    });
    jp::add_path_precheck::set(|_root, _pr, _dn, _su, _tot, _pk, _ro| true);

    // memoize: get_memoize_path is now in-crate; with enable_memoize=false in
    // the test flags it short-circuits to None before reaching any seam, so no
    // memoize callee seams need installing for this gate.

    // The two pathnode.c constructors this branch reaches: build a real
    // NestPath and add it to the joinrel pathlist (the C `add_path`).
    jp::create_nestloop_path::set(
        |root, _run, joinrel, jointype, _ws, _extra, outer_path, inner_path, rc, pathkeys, _ro| {
            let mut path = mk_path(joinrel);
            path.pathtype = NodeTag(335); // a join plan tag
            path.pathkeys = pathkeys.to_vec();
            path.total_cost = 5.0;
            let node = PathNode::NestPath(NestPath {
                jpath: TJoinPath {
                    path,
                    jointype,
                    inner_unique: false,
                    outerjoinpath: Some(outer_path),
                    innerjoinpath: Some(inner_path),
                    joinrestrictinfo: rc.to_vec(),
                },
            });
            Ok(root.alloc_path(node))
        },
    );
    jp::add_path::set(|root, parent_rel, new_path| {
        root.rel_mut(parent_rel).pathlist.push(new_path);
        Ok(())
    });

    // FDW + extension hooks at the tail of add_paths_to_joinrel: no-ops.
    jp::fdw_get_foreign_join_paths::set(|_r, _jr, _o, _i, _jt, _e| Ok(()));
    jp::set_join_pathlist_hook::set(|_r, _jr, _o, _i, _jt, _e| Ok(()));
}

/// GATE: `add_paths_to_joinrel` adds a nestloop path to a synthetic 2-rel
/// JOIN_INNER.
#[test]
fn add_paths_to_joinrel_adds_nestloop_for_inner_join() {
    install_join_seams();

    let cx = MemoryContext::new("joinpath-test");
    let mcx = cx.mcx();

    let mut root = PlannerInfo::default();
    let outerrel = root.alloc_rel(mk_rel(1));
    let innerrel = root.alloc_rel(mk_rel(2));
    let joinrel = root.alloc_rel(mk_rel(3));

    // One base path per input rel.
    let outer_path = root.alloc_path(PathNode::Path(mk_path(outerrel)));
    let inner_path = root.alloc_path(PathNode::Path(mk_path(innerrel)));

    // Wire the rels' path lists / cheapest pointers.
    {
        let o = root.rel_mut(outerrel);
        o.pathlist = alloc::vec![outer_path];
        o.cheapest_total_path = Some(outer_path);
    }
    {
        let i = root.rel_mut(innerrel);
        i.pathlist = alloc::vec![inner_path];
        i.cheapest_total_path = Some(inner_path);
        i.cheapest_parameterized_paths = alloc::vec![inner_path];
    }

    let sjinfo = mk_sjinfo();
    let restrictlist: Vec<RinfoId> = Vec::new();

    // All join-method GUCs off: drives the bare nestloop branch.
    let enable = JoinEnableFlags {
        enable_mergejoin: false,
        enable_hashjoin: false,
        enable_material: false,
        enable_parallel_hash: false,
        enable_memoize: false,
    };

    let run = PlannerRun::new(mcx);
    add_paths_to_joinrel(
        mcx, &mut root, &run, joinrel, outerrel, innerrel, JOIN_INNER, &sjinfo, &restrictlist,
        enable,
    )
    .expect("add_paths_to_joinrel");

    // Exactly one path landed on the joinrel: a nestloop linking our inputs.
    let pathlist = &root.rel(joinrel).pathlist;
    assert_eq!(pathlist.len(), 1, "expected one join path on the joinrel");
    let added: PathId = pathlist[0];
    match root.path(added) {
        PathNode::NestPath(np) => {
            assert_eq!(np.jpath.jointype, JOIN_INNER);
            assert_eq!(np.jpath.outerjoinpath, Some(outer_path));
            assert_eq!(np.jpath.innerjoinpath, Some(inner_path));
            assert_eq!(np.jpath.path.parent, joinrel);
        }
        other => panic!("expected NestPath, got {other:?}"),
    }
}
