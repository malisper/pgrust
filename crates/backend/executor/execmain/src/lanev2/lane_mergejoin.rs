//! WS-MJ1 — the lane-native MergeJoin engine composition (LANE-MERGEJOIN
//! contract, K4 option-a; inc-1: INNER only, Sort inner only).
//!
//! Merge core: the ported C `ExecMergeJoin` EXEC_MJ_* 11-state machine
//! (`::nodemergejoin::exec_merge_join`, nodeMergejoin.c PG 18.3 — same
//! transitions, same emit points, state node-resident in `MergeJoinState`)
//! instantiated over LANE feed adapters. A second textual port of the FSM
//! was rejected at branch-open (notes/mergejoin-ws-mj1.md §1.6): it would
//! duplicate the mark/restore bug nest with zero protocol delta — the
//! delegation-form idiom (AF1 precedent) is the chartered shape.
//!
//! Feed contracts (contract §1.3, the diffprof design RULE baked in):
//! * NO per-pull lane ceremony on either input — inner pulls and
//!   mark/restore are direct delegated calls on the child node state (the
//!   diffprof lane proved per-pull `pull_step`/`RootAdapter` ceremony on a
//!   read-back path is a pure ~109 Ir/row tax with zero function;
//!   notes/sortfeed-diffprof-lane.md @ 0d4bf241c).
//! * Inner (Sort only at inc-1): the sort breaker's randomAccess Tuplesort
//!   read-back — the `PGRUST_LANE_V2_SORT_RANDOMACCESS` machinery
//!   (default-ON since the SE9-GATES AD2 flip). FEED-ONLY ownership law:
//!   the FIRST pull (sort not yet done) delegates through the full
//!   `exec_proc_node` sort arm so feed admission (lane RA batch feed /
//!   fused arm / Volcano fallback) is exactly today's; every subsequent
//!   read-back is the BARE drain (`exec_sort` on the finished tuplesort,
//!   AD0 D3), and mark/restore ride `exec_sort_mark_pos` /
//!   `exec_sort_restr_pos` on the sort node state directly — identical
//!   whether the sort was lane-fed (RA-admitted, FEED-ONLY) or Volcano-fed.
//! * Outer: C never marks or backs up the outer (nodeMergejoin.c) — any
//!   child the executor can drive, via the direct `exec_proc_node`
//!   delegated call (the same drive the Volcano FSM's trait impl uses; the
//!   OPTIONAL batch-staged outer drive is inc-4, behind its own letter).
//!
//! Mixed-drive coherence (pinned by unit): the lane drive and the Volcano
//! drive share ONE `MergeJoinState` and semantically identical feeds, so a
//! per-pull handover (refusal or knob flip mid-stream) continues the same
//! join byte-identically.
//!
//! Knob: `PGRUST_LANE_V2_MERGEJOIN_NATIVE`, default OFF (`1`/`on` arms).
//! NAME NOTE (worklog §1.4): the contract's spelling
//! `PGRUST_LANE_V2_MERGEJOIN` was already minted by WS-L (wave-2 knob
//! split) for the WS-G row-mode HOSTING verdict (default ON since wave-4
//! FLIP-2, `lanev2/rowmode.rs`); the lane-native engine takes the `_NATIVE`
//! suffix — the band-collision discipline applied to the knob namespace.

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::nodemergejoin::{MergeJoinInner, MergeJoinOuter};
use ::types_error::PgResult;

use crate::procnode::{MergeJoinNode, PlanStateNode, SortNode};

/// `PGRUST_LANE_V2_MERGEJOIN_NATIVE` (default OFF; `1`/`on` arms): the
/// lane-native MergeJoin engine gate. 0 = unresolved (read env on first
/// use), 1 = OFF, 2 = ON. AtomicU8 rather than OnceLock so the unit corpus
/// can A/B both paths in one process (`mergejoin_native_set_for_tests`);
/// env-var (not GUC) per the standing `pg_settings` byte-identity
/// discipline (lanev2 module doc). NEVER default during migration.
static MERGEJOIN_NATIVE: AtomicU8 = AtomicU8::new(0);

/// The §4.1 OFF-arm byte: one relaxed load + compare inlined into the
/// dispatch arm head; the env read is the one-shot `#[cold]` tail.
#[inline]
pub(super) fn mergejoin_native_enabled() -> bool {
    match MERGEJOIN_NATIVE.load(Relaxed) {
        1 => false,
        2 => true,
        _ => mergejoin_native_resolve(),
    }
}

#[cold]
#[inline(never)]
fn mergejoin_native_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_MERGEJOIN_NATIVE").as_deref(),
        Ok("1") | Ok("on")
    );
    MERGEJOIN_NATIVE.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the band-99001 unit corpus.
#[cfg(test)]
pub(crate) fn mergejoin_native_set_for_tests(on: bool) {
    MERGEJOIN_NATIVE.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: lane-native owned drives, per pull (stats.rs
/// ticks arm only via the process-global `PGRUST_LANE_V2_STATS` env,
/// unusable per-test — the `ROWMODE_MJ_OWNED_FOR_TESTS` precedent).
#[cfg(test)]
pub(crate) static MJ_NATIVE_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only mark/restore cadence probes (contract §2.3/§2.4 pins): every
/// `ExecMarkPos`-equivalent / `ExecRestrPos`-equivalent the FSM issues
/// through the lane inner adapter. The skip_mark_restore probe pin
/// (costsize.c:3904-3906 provenance) asserts BOTH stay zero under the flag.
#[cfg(test)]
pub(crate) static MJ_NATIVE_MARKS_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
pub(crate) static MJ_NATIVE_RESTORES_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only NAMED-refusal probes, in the surface's gate order:
/// [epq, backward(RETIRED - B11; slot kept so ids stay stable),
/// instrumented, mergejoin-jointype, mergejoin-inner-feed].
#[cfg(test)]
pub(crate) static MJ_NATIVE_REFUSED_FOR_TESTS: [std::sync::atomic::AtomicU64; 5] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

#[cfg(test)]
#[inline]
pub(super) fn mj_native_refusal_probe(i: usize) {
    MJ_NATIVE_REFUSED_FOR_TESTS[i].fetch_add(1, Relaxed);
}

/// Lane outer feed: direct delegated drive on the child node state. C
/// requires only that the outer arrives sorted per the mergeclauses
/// (planner-guaranteed) and never marks/backs it up (contract §1.3).
pub(super) struct LaneMergeOuter<'a, 'mcx> {
    pub(super) child: &'a mut PlanStateNode<'mcx>,
}

impl<'mcx> MergeJoinOuter<'mcx> for LaneMergeOuter<'_, 'mcx> {
    #[inline]
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        crate::procnode::exec_proc_node(self.child, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self.child, estate)
    }
}

/// Lane inner feed (inc-1: Sort ONLY — everything else refused by name at
/// the surface): the RA Tuplesort read-back family, FEED-ONLY.
pub(super) struct LaneSortInner<'a, 'mcx> {
    pub(super) child: &'a mut PlanStateNode<'mcx>,
    /// Planner-supplied `skip_mark_restore` (costsize.c:3904-3906: set iff
    /// (SEMI ∨ ANTI ∨ inner_unique) ∧ joinrestrictinfo == mergeclauses; the
    /// executor inits the inner WITHOUT EXEC_FLAG_MARK when set). The lane
    /// surface honors it identically: the FSM never issues mark/restore
    /// under it (SKIP_TEST/TESTOUTER guards), probe-pinned here.
    pub(super) skip_mark_restore: bool,
}

impl<'mcx> MergeJoinInner<'mcx> for LaneSortInner<'_, 'mcx> {
    #[inline]
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        if let PlanStateNode::Sort(s) = &mut *self.child {
            if s.state.sort_done() {
                // FEED-ONLY read-back (AD0 D3, diffprof law): the bare
                // drain leg on the finished tuplesort — no dispatch, no
                // lane ceremony. `fetch_outer` is dead post-done (the
                // sort_arm fallback's own closure shape, kept verbatim).
                let SortNode { state, outer, outer_desc, .. } = s;
                let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
                return ::nodesort::exec_sort(state, estate, outer_desc, |es| {
                    crate::procnode::exec_proc_node(outer, es)
                });
            }
        }
        // First pull: the sort is not yet fed — delegate through the full
        // sort arm so feed admission (lane RA batch feed / fused arm /
        // Volcano per-tuple) is exactly today's path (FEED-ONLY: the lane
        // owns the feed through the breaker, never a substituted emit face).
        crate::procnode::exec_proc_node(self.child, estate)
    }

    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        crate::execami::exec_re_scan(self.child, estate)
    }

    fn mark_pos(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // Probe pin: never reached under skip_mark_restore (the executor
        // assert `joinqual == NIL || !skip_mark_restore` and the FSM's
        // SKIP_TEST guard together make this unreachable; contract §2.3).
        debug_assert!(
            !self.skip_mark_restore,
            "ExecMarkPos under skip_mark_restore (costsize.c:3904 contract)"
        );
        #[cfg(test)]
        MJ_NATIVE_MARKS_FOR_TESTS.fetch_add(1, Relaxed);
        let PlanStateNode::Sort(s) = &mut *self.child else {
            unreachable!("lane mergejoin inc-1 admits a Sort inner only");
        };
        // Direct on the sort node state (execami's Sort arm, un-dispatched).
        ::nodesort::exec_sort_mark_pos(&mut s.state)
    }

    fn restr_pos(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        debug_assert!(
            !self.skip_mark_restore,
            "ExecRestrPos under skip_mark_restore (costsize.c:3904 contract)"
        );
        #[cfg(test)]
        MJ_NATIVE_RESTORES_FOR_TESTS.fetch_add(1, Relaxed);
        let PlanStateNode::Sort(s) = &mut *self.child else {
            unreachable!("lane mergejoin inc-1 admits a Sort inner only");
        };
        ::nodesort::exec_sort_restr_pos(&mut s.state)
    }
}

/// The lane-native drive: one pull of the ported EXEC_MJ_* state machine
/// over the lane feed adapters. Caller (the lanev2.rs surface) has already
/// admitted the shape (INNER, Sort inner, forward, non-EPQ, uninstrumented).
pub(super) fn lane_merge_join_drive<'mcx>(
    mj: &mut MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    #[cfg(test)]
    MJ_NATIVE_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    let MergeJoinNode { state, outer, inner, .. } = mj;
    debug_assert!(matches!(&**inner, PlanStateNode::Sort(_)), "admitted at the surface");
    let skip_mark_restore = state.plan.skip_mark_restore;
    let mut o = LaneMergeOuter { child: &mut **outer };
    let mut i = LaneSortInner { child: &mut **inner, skip_mark_restore };
    ::nodemergejoin::exec_merge_join(state, &mut o, &mut i, estate)
}
