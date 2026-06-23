//! Unit GATE — exercises the relid-set logic of restrictinfo.c
//! (`join_clause_is_movable_to`/`join_clause_is_movable_into`) and the joininfo
//! relevance/extract routines over a synthetic `PlannerInfo` arena.
//!
//! Cross-subsystem callees are seams defaulting to a loud panic, so the test
//! installs in-test relids-algebra impls first (single-threaded). The relids
//! seams model the planner convention that the empty set is `None`, backed by
//! the canonical `bitmapword[]` layout.

use crate::restrictinfo::{
    extract_actual_join_clauses, join_clause_is_movable_into, join_clause_is_movable_to,
    restriction_is_or_clause,
};

use ::pathnodes::{
    Bitmapset, NodeId, PlannerInfo, RelId, RelOptInfo, RestrictInfo, RinfoId, VOLATILITY_UNKNOWN,
};

use relnode_seams as bms_seam;

fn words_of(a: &::pathnodes::Relids) -> &[u64] {
    match a {
        Some(b) => &b.words,
        None => &[],
    }
}
fn member(x: i32, a: &::pathnodes::Relids) -> bool {
    let w = words_of(a);
    if x < 0 {
        return false;
    }
    let wn = (x / 64) as usize;
    let bn = (x % 64) as u32;
    wn < w.len() && (w[wn] >> bn) & 1 == 1
}
fn singleton(x: i32) -> ::pathnodes::Relids {
    let wn = (x / 64) as usize;
    let bn = (x % 64) as u32;
    let mut w = alloc::vec![0u64; wn + 1];
    w[wn] = 1u64 << bn;
    Some(alloc::boxed::Box::new(Bitmapset { words: w }))
}
fn from_members(xs: &[i32]) -> ::pathnodes::Relids {
    let mut r = None;
    for &x in xs {
        r = union(&r, &singleton(x));
    }
    r
}
fn union(a: &::pathnodes::Relids, b: &::pathnodes::Relids) -> ::pathnodes::Relids {
    let aw = words_of(a);
    let bw = words_of(b);
    let n = aw.len().max(bw.len());
    let mut out = alloc::vec![0u64; n];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = aw.get(i).copied().unwrap_or(0) | bw.get(i).copied().unwrap_or(0);
    }
    if out.iter().all(|&w| w == 0) {
        None
    } else {
        Some(alloc::boxed::Box::new(Bitmapset { words: out }))
    }
}
fn overlap(a: &::pathnodes::Relids, b: &::pathnodes::Relids) -> bool {
    let aw = words_of(a);
    let bw = words_of(b);
    let n = aw.len().min(bw.len());
    (0..n).any(|i| aw[i] & bw[i] != 0)
}
fn is_subset(a: &::pathnodes::Relids, b: &::pathnodes::Relids) -> bool {
    let aw = words_of(a);
    let bw = words_of(b);
    aw.iter()
        .enumerate()
        .all(|(i, &w)| w & !bw.get(i).copied().unwrap_or(0) == 0)
}

fn install_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        bms_seam::relids_is_member::set(member);
        bms_seam::relids_overlap::set(|a, b| overlap(a, b));
        bms_seam::relids_is_subset::set(|a, b| is_subset(a, b));
        bms_seam::relids_is_empty::set(|a| words_of(a).iter().all(|&w| w == 0));
        bms_seam::relids_copy::set(|a| a.clone());
    });
}

fn mk_rel(root: &mut PlannerInfo, relid: u32) -> RelId {
    let rel = RelOptInfo {
        relid,
        ..RelOptInfo::default()
    };
    root.alloc_rel(rel)
}

/// A bare RestrictInfo carrying explicit relid sets (no node interning needed for
/// the movability/extract logic).
fn mk_rinfo(
    root: &mut PlannerInfo,
    clause_relids: ::pathnodes::Relids,
    outer_relids: ::pathnodes::Relids,
    is_clone: bool,
) -> RinfoId {
    // Intern a non-constant clause node so rinfo_is_constant_true can deref it
    // (a Var is never the constant-TRUE the extract routines drop).
    let clause = root.alloc_node(nodes::primnodes::Expr::Var(
        nodes::primnodes::Var::default(),
    ));
    let ri = RestrictInfo {
        clause,
        orclause: None,
        is_pushed_down: false,
        pseudoconstant: false,
        has_clone: false,
        is_clone,
        can_join: false,
        leakproof: false,
        has_volatile: VOLATILITY_UNKNOWN,
        security_level: 0,
        num_base_rels: 0,
        clause_relids,
        required_relids: None,
        incompatible_relids: None,
        outer_relids,
        left_relids: None,
        right_relids: None,
        rinfo_serial: 0,
        parent_ec: None,
        eval_cost: ::pathnodes::QualCost::default(),
        norm_selec: -1.0,
        outer_selec: -1.0,
        mergeopfamilies: alloc::vec::Vec::new(),
        left_ec: None,
        right_ec: None,
        left_em: None,
        right_em: None,
        scansel_cache: alloc::vec::Vec::new(),
        outer_is_left: false,
        hashjoinoperator: 0,
        left_bucketsize: -1.0,
        right_bucketsize: -1.0,
        left_mcvfreq: -1.0,
        right_mcvfreq: -1.0,
        left_hasheqoperator: 0,
        right_hasheqoperator: 0,
    };
    root.alloc_rinfo(ri)
}

/// GATE: a clause {1,2} with no OJ baggage IS movable to baserel 1, but a clone
/// of it, or one whose target rel is in outer_relids / nulled, is not.
#[test]
fn movable_to_gates() {
    install_seams();
    let mut root = PlannerInfo::default();
    let rel1 = mk_rel(&mut root, 1);
    // baserel 1 references {1}; its nulling_relids and lateral_referencers empty.
    root.rel_mut(rel1).relid = 1;

    // Plain join clause {1,2}: movable to rel 1.
    let ri = mk_rinfo(&mut root, from_members(&[1, 2]), None, false);
    assert!(join_clause_is_movable_to(&root, ri, rel1));

    // Clause that doesn't physically reference rel 1: not movable.
    let ri2 = mk_rinfo(&mut root, from_members(&[2, 3]), None, false);
    assert!(!join_clause_is_movable_to(&root, ri2, rel1));

    // Outer-join clause whose outer_relids include rel 1: not movable.
    let ri3 = mk_rinfo(&mut root, from_members(&[1, 2]), singleton(1), false);
    assert!(!join_clause_is_movable_to(&root, ri3, rel1));

    // Clone version: not movable.
    let ri4 = mk_rinfo(&mut root, from_members(&[1, 2]), None, true);
    assert!(!join_clause_is_movable_to(&root, ri4, rel1));

    // Target rel's Vars nulled by an OJ (clause_relids overlaps nulling_relids).
    root.rel_mut(rel1).nulling_relids = singleton(5);
    let ri5 = mk_rinfo(&mut root, from_members(&[1, 5]), None, false);
    assert!(!join_clause_is_movable_to(&root, ri5, rel1));
    root.rel_mut(rel1).nulling_relids = None;

    // Clause uses a rel that has LATERAL refs to rel 1.
    root.rel_mut(rel1).lateral_referencers = singleton(2);
    let ri6 = mk_rinfo(&mut root, from_members(&[1, 2]), None, false);
    assert!(!join_clause_is_movable_to(&root, ri6, rel1));
}

/// GATE: movable_into requires clause_relids ⊆ current_and_outer, at least one
/// current rel referenced, and no overlap with outer_relids.
#[test]
fn movable_into_gates() {
    install_seams();
    let mut root = PlannerInfo::default();

    let current = from_members(&[1]);
    let current_and_outer = from_members(&[1, 2]);

    // Clause {1,2}: subset of {1,2}, references current {1}, no OJ overlap -> ok.
    let ri = mk_rinfo(&mut root, from_members(&[1, 2]), None, false);
    assert!(join_clause_is_movable_into(
        &root,
        ri,
        &current,
        &current_and_outer
    ));

    // Clause {1,2,3}: not a subset of {1,2} -> not movable.
    let ri2 = mk_rinfo(&mut root, from_members(&[1, 2, 3]), None, false);
    assert!(!join_clause_is_movable_into(
        &root,
        ri2,
        &current,
        &current_and_outer
    ));

    // Clause {2}: subset, but references no current rel -> not movable.
    let ri3 = mk_rinfo(&mut root, from_members(&[2]), None, false);
    assert!(!join_clause_is_movable_into(
        &root,
        ri3,
        &current,
        &current_and_outer
    ));

    // Clause {1,2} but outer_relids overlap current {1} -> not movable.
    let ri4 = mk_rinfo(&mut root, from_members(&[1, 2]), singleton(1), false);
    assert!(!join_clause_is_movable_into(
        &root,
        ri4,
        &current,
        &current_and_outer
    ));
}

/// GATE: restriction_is_or_clause keys on the presence of an orclause handle.
#[test]
fn or_clause_detection() {
    install_seams();
    let mut root = PlannerInfo::default();
    let plain = mk_rinfo(&mut root, None, None, false);
    assert!(!restriction_is_or_clause(&root, plain));
    root.rinfo_mut(plain).orclause = Some(NodeId::default());
    assert!(restriction_is_or_clause(&root, plain));
}

/// GATE: extract_actual_join_clauses splits on RINFO_IS_PUSHED_DOWN
/// (required_relids ⊄ joinrelids, or is_pushed_down).
#[test]
fn extract_join_clauses_split() {
    install_seams();
    let mut root = PlannerInfo::default();
    let joinrelids = from_members(&[1, 2]);

    // joinqual: required_relids {1,2} ⊆ joinrelids and not pushed-down.
    let jq = mk_rinfo(&mut root, from_members(&[1, 2]), None, false);
    root.rinfo_mut(jq).required_relids = from_members(&[1, 2]);

    // otherqual: required_relids {1} but is_pushed_down=true.
    let oq = mk_rinfo(&mut root, from_members(&[1]), None, false);
    root.rinfo_mut(oq).required_relids = from_members(&[1]);
    root.rinfo_mut(oq).is_pushed_down = true;

    let (joinquals, otherquals) =
        extract_actual_join_clauses(&root, &[jq, oq], &joinrelids);
    assert_eq!(joinquals.len(), 1);
    assert_eq!(otherquals.len(), 1);
}
