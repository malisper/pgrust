//! Seam-free unit tests for the crate's owned, non-seamed logic. The full
//! `Exec*Gather` paths drive a live executor through many sibling seams whose
//! owners are not yet ported (they panic loudly until then), so they are
//! exercised by the integration suite once those owners land. Here we cover the
//! plan-node vocabulary and the seam-free constants.
use super::*;
use mcx::MemoryContext;
use ::nodes::nodegather::{Gather, T_Gather, T_GatherState};

#[test]
fn outer_var_matches_c() {
    // primnodes.h: #define OUTER_VAR (-2)
    assert_eq!(OUTER_VAR, -2);
}

#[test]
fn node_tags_match_c() {
    // nodetags.h: T_Gather = 368, T_GatherState = 432.
    assert_eq!(T_Gather.0, 368);
    assert_eq!(T_GatherState.0, 432);
}

#[test]
fn gather_plan_default_and_clone() {
    let ctx = MemoryContext::new("gather-test");
    let mcx = ctx.mcx();
    // A default Gather plan with the planner scalars set.
    let mut g = Gather::default();
    g.num_workers = 3;
    g.rescan_param = -1;
    g.single_copy = true;
    g.invisible = false;
    // Deep copy preserves the scalars (the C copyObject shape).
    let c = g.clone_in(mcx).unwrap();
    assert_eq!(c.num_workers, 3);
    assert_eq!(c.rescan_param, -1);
    assert!(c.single_copy);
    assert!(!c.invisible);
    assert!(c.initParam.is_none());
}

#[test]
fn clone_bitmapset_none_is_none() {
    let ctx = MemoryContext::new("gather-bms");
    let mcx = ctx.mcx();
    assert!(clone_bitmapset(mcx, None).unwrap().is_none());
}
