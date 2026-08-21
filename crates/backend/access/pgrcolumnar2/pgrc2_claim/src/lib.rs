//! The minimal claim plane — the XC-1/XC-2 substrate of the FROZEN parallel
//! contract (`docs/design/lanev4-parallel-contract.md` §2 worker/claim
//! protocol, §3 batch geometry), pulled from L3 into L2 by the ruled
//! COPY-on-morsels amendment ("shouldn't copy work on morsels so it's
//! scheduled the same as the rest of the queries?" — ruled yes): parallel
//! COPY is this substrate's FIRST consumer; the M3-L3 reader is consumer #2,
//! unchanged. The full statement-grain pool is M4's; this crate is only the
//! claim face M4 consumes.
//!
//! Contract clauses implemented here, by id:
//!
//! - **PC-2.1** Claims are SPAN-shaped: a contiguous unit range that never
//!   splits a unit and never crosses a hard boundary. A [`Span`] is a plain
//!   range value — claims are the ONLY cross-thread work currency.
//! - **PC-2.2** Worker-local claiming: every participant drives its own
//!   source instance over claimed spans; nothing here hosts work under a
//!   scheduler and nothing here is per-row shaped.
//! - **PC-2.3** The claim-source face: total span space,
//!   `next_boundary_after` (hard boundaries), a startup ramp seed,
//!   whole-boundary + coalescing postures, and a source-controlled refusal
//!   of terminal sub-span splits (the #621 whole-band posture). Claim
//!   advance is the only synchronization the claim path needs — ONE atomic
//!   cursor CAS; claim dispatch carries no locks.
//! - **PC-2.4** Pins taken for a claim are released at end_claim BY
//!   CONSTRUCTION: [`ClaimGuard`] owns every release hook attached during
//!   the claim and runs them on drop — the IN-4 part-pin law made
//!   structural (the #792/#802 class is impossible, not retrofitted).
//! - **PC-2.5** Exactly two hook points exist at the claim face —
//!   **claim-begin** (span acquired: cold-readahead issue, prune-verdict
//!   install at M4) and **claim-narrow** (sub-span/row-bounded claim: the
//!   SpanClaimSource refetch face). Consumers attach ONLY here
//!   ([`ClaimObserver`]); new hooks are appended contract clauses.
//! - **PC-3.1** Scan-work claim grain is whole units (whole granules at the
//!   part's SB-10 grain for the reader; whole capture-chunk morsels for
//!   COPY). Claim-narrow is the sole sub-unit face.
//!
//! ## Unit spaces
//!
//! The claim unit is the SOURCE's currency: sealed-part granules for the
//! reader, deterministic capture-chunk morsels for COPY. Boundary placement
//! is a pure function of the input (fixed chunk grain, never adaptive or
//! arrival-dependent), so the unit space itself is DOP- and
//! schedule-independent — the precondition of the PC-3.4 DOP-independence
//! law. Under dynamic claiming only the claim→worker ASSIGNMENT varies;
//! whoever needs output identity puts ordering in its sink (the parallel
//! COPY cut cursor consumes morsels strictly in input order), never in the
//! claim schedule.
//!
//! ## Growing spaces
//!
//! A streaming source (COPY capture) publishes units incrementally: the
//! cursor never claims past [`MorselSource::published`], and a source whose
//! `total_units` is still `None` answers [`ClaimOutcome::NotYetPublished`]
//! at the watermark instead of draining. `close(total)` is the source's
//! act; after it, drain is final.

use std::sync::atomic::{AtomicU64, Ordering};

/// One claim: a contiguous, non-empty unit range `[start, end)`.
/// Plain data — the only cross-thread work currency (PC-2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u64,
    pub end: u64,
}

impl Span {
    pub fn units(&self) -> u64 {
        self.end - self.start
    }

    /// The claim-narrow face (PC-2.5): a sub-span of an owned claim — the
    /// SOLE sub-unit claim shape in the contract (SpanClaimSource winner
    /// refetch narrows through this; scan work stays unit-whole per
    /// PC-3.1). Returns `None` when `sub` is not contained in `self`.
    pub fn narrow(&self, sub: Span) -> Option<Span> {
        if sub.start >= self.start && sub.end <= self.end && sub.start < sub.end {
            Some(sub)
        } else {
            None
        }
    }
}

/// The claim-source face (PC-2.3) — the MorselSource shape of the v3
/// rebuild, minimally. Implementations are the SOURCES of unit spaces:
/// COPY's capture-chunk feed (consumer #1), the reader's granule spans
/// (consumer #2, M3-L3).
pub trait MorselSource: Send + Sync {
    /// Total units in the space, once known. `None` while the space is
    /// still growing (streaming capture before end-of-input).
    fn total_units(&self) -> Option<u64>;

    /// The publish watermark: units `< published()` are claimable NOW.
    /// For a static space this equals `total_units()`.
    fn published(&self) -> u64;

    /// The first HARD boundary strictly after `unit` (part edge,
    /// dict-epoch edge). A claim never crosses it (PC-2.1). Sources with
    /// no interior boundaries answer `u64::MAX`.
    fn next_boundary_after(&self, unit: u64) -> u64;

    /// Startup ramp: the span size the cursor starts claims at.
    fn ramp_seed(&self) -> u64 {
        1
    }

    /// Coalescing posture: the max units one claim may carry.
    fn coalesce_max(&self) -> u64 {
        1
    }

    /// The #621 whole-band posture: `false` refuses terminal sub-span
    /// splits — the tail is claimed whole up to the next hard boundary,
    /// never split just because the space is nearly drained. The minimal
    /// plane never ramps ask sizes down at the tail, so it never
    /// terminal-subsplits regardless; the flag is carried so the PC-2.3
    /// face is contract-complete for the consumers that will (M4's
    /// statement-grain pool ramps).
    fn terminal_subsplit_claims(&self) -> bool {
        false
    }
}

/// One claim-advance outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// A span was claimed; the caller owns it until `end_claim` (guard
    /// drop).
    Claimed(Span),
    /// The space is closed and fully claimed.
    Drained,
    /// The cursor reached the publish watermark of a still-growing space:
    /// retry after the source publishes (or closes).
    NotYetPublished,
}

/// The two contract hook points (PC-2.5). Consumers attach ONLY here; a
/// third hook is an appended contract clause, never an ad-hoc callback.
pub trait ClaimObserver: Send + Sync {
    /// A worker acquired `span` (cold-readahead issue, prune-verdict
    /// install — the M4 chokepoint face).
    fn claim_begin(&self, _worker: usize, _span: Span) {}
    /// A span was narrowed to a sub-claim (the SpanClaimSource refetch
    /// face).
    fn claim_narrow(&self, _worker: usize, _span: Span, _sub: Span) {}
}

/// The default no-op observer.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoObserver;

impl ClaimObserver for NoObserver {}

/// A claimed span with end_claim-by-construction release (PC-2.4): every
/// resource pinned FOR the claim registers its release here, and the
/// guard's drop runs them (reverse order) — the claim cannot outlive its
/// pins' release, structurally. The M3-L3 reader hands real part-pin
/// releases to this; parallel COPY attaches nothing today but claims
/// through the same guard so the lifecycle law is exercised from birth.
pub struct ClaimGuard<'a> {
    span: Span,
    worker: usize,
    observer: &'a dyn ClaimObserver,
    releases: Vec<Box<dyn FnOnce() + Send + 'a>>,
}

impl<'a> ClaimGuard<'a> {
    pub fn span(&self) -> Span {
        self.span
    }

    /// Register a release hook that MUST run at end_claim (pin drops).
    pub fn attach_release(&mut self, f: impl FnOnce() + Send + 'a) {
        self.releases.push(Box::new(f));
    }

    /// The claim-narrow hook face (PC-2.5): narrow this claim to a
    /// sub-span, firing the observer. The narrowed span stays owned by
    /// this guard (release hooks are the guard's either way).
    pub fn narrow(&self, sub: Span) -> Option<Span> {
        let narrowed = self.span.narrow(sub)?;
        self.observer.claim_narrow(self.worker, self.span, narrowed);
        Some(narrowed)
    }
}

impl Drop for ClaimGuard<'_> {
    fn drop(&mut self) {
        // end_claim: releases run in reverse attach order (pin discipline —
        // last-taken released first), unconditionally, panic-or-not.
        while let Some(f) = self.releases.pop() {
            f();
        }
    }
}

/// The shared claim cursor (PC-2.3): ONE atomic; `try_claim` is a bounded
/// CAS loop; dispatch takes no locks. Workers race the cursor; the span a
/// worker wins is exclusively its own until the guard drops.
#[derive(Debug, Default)]
pub struct ClaimCursor {
    next: AtomicU64,
}

impl ClaimCursor {
    pub fn new() -> ClaimCursor {
        ClaimCursor {
            next: AtomicU64::new(0),
        }
    }

    /// Units handed out so far (test/census observability).
    pub fn claimed_units(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }

    /// The ONE claim-advance CAS loop (PC-2.3): span sizing is
    /// `ramp_seed` clamped to `coalesce_max` (the minimal plane keeps ramp
    /// == the source's seed; a full ramp curve is an appended-clause
    /// affair), clipped at the publish watermark, the next hard boundary,
    /// and the space end. Under `terminal_subsplit_claims == false` a tail
    /// shorter than the ask is still claimed WHOLE (up to boundary/
    /// watermark) — refusal of terminal sub-span splits means the tail is
    /// never subdivided below what remains, not that it is left unclaimed.
    fn advance(&self, source: &dyn MorselSource) -> Result<Span, ClaimOutcome> {
        let ask = source.ramp_seed().clamp(1, source.coalesce_max().max(1));
        loop {
            let cur = self.next.load(Ordering::Acquire);
            let published = source.published();
            let total = source.total_units();
            if let Some(t) = total {
                if cur >= t {
                    return Err(ClaimOutcome::Drained);
                }
            }
            if cur >= published {
                // Still-growing space at its watermark (or a torn source
                // whose publish never reached total): the caller re-polls,
                // never spins inside the claim path.
                return Err(ClaimOutcome::NotYetPublished);
            }
            let boundary = source.next_boundary_after(cur);
            let mut end = cur.saturating_add(ask);
            end = end.min(published).min(boundary.max(cur + 1));
            if let Some(t) = total {
                end = end.min(t);
            }
            debug_assert!(end > cur, "claim advance must make progress");
            if self
                .next
                .compare_exchange(cur, end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(Span { start: cur, end });
            }
            // CAS lost: another worker advanced the cursor — retry.
        }
    }

    /// Span-only claim for consumers with nothing to pin: fires
    /// claim-begin and returns the plain span.
    pub fn try_claim(
        &self,
        source: &dyn MorselSource,
        worker: usize,
        observer: &dyn ClaimObserver,
    ) -> ClaimOutcome {
        match self.advance(source) {
            Ok(span) => {
                observer.claim_begin(worker, span);
                ClaimOutcome::Claimed(span)
            }
            Err(o) => o,
        }
    }

    /// Guard-owning claim: like [`ClaimCursor::try_claim`] but returns the
    /// [`ClaimGuard`] so pins can attach (PC-2.4).
    pub fn begin_claim<'a>(
        &self,
        source: &dyn MorselSource,
        worker: usize,
        observer: &'a dyn ClaimObserver,
    ) -> Result<ClaimGuard<'a>, ClaimOutcome> {
        let span = self.advance(source)?;
        observer.claim_begin(worker, span);
        Ok(ClaimGuard {
            span,
            worker,
            observer,
            releases: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    struct FixedSource {
        total: u64,
        published: AtomicU64,
        boundary_every: u64,
        coalesce: u64,
    }

    impl MorselSource for FixedSource {
        fn total_units(&self) -> Option<u64> {
            Some(self.total)
        }
        fn published(&self) -> u64 {
            self.published.load(Ordering::Acquire)
        }
        fn next_boundary_after(&self, unit: u64) -> u64 {
            if self.boundary_every == 0 {
                u64::MAX
            } else {
                (unit / self.boundary_every + 1) * self.boundary_every
            }
        }
        fn ramp_seed(&self) -> u64 {
            self.coalesce
        }
        fn coalesce_max(&self) -> u64 {
            self.coalesce
        }
    }

    #[derive(Default)]
    struct Recorder {
        begins: Mutex<Vec<(usize, Span)>>,
        narrows: Mutex<Vec<(Span, Span)>>,
    }

    impl ClaimObserver for Recorder {
        fn claim_begin(&self, worker: usize, span: Span) {
            self.begins.lock().unwrap().push((worker, span));
        }
        fn claim_narrow(&self, _worker: usize, span: Span, sub: Span) {
            self.narrows.lock().unwrap().push((span, sub));
        }
    }

    /// PC-2.1/PC-2.3: spans are contiguous, disjoint, cover the space
    /// exactly once, and never cross a hard boundary.
    #[test]
    fn spans_partition_the_space_and_respect_boundaries() {
        let src = FixedSource {
            total: 37,
            published: AtomicU64::new(37),
            boundary_every: 8,
            coalesce: 5,
        };
        let cur = ClaimCursor::new();
        let rec = Recorder::default();
        let mut covered = 0u64;
        let mut spans = 0usize;
        loop {
            match cur.try_claim(&src, 0, &rec) {
                ClaimOutcome::Claimed(s) => {
                    assert_eq!(s.start, covered, "contiguous, in order, exactly once");
                    assert!(s.units() >= 1 && s.units() <= 5);
                    // never crosses a hard boundary
                    assert!(
                        s.start / 8 == (s.end - 1) / 8,
                        "span {s:?} crosses an 8-unit boundary"
                    );
                    covered = s.end;
                    spans += 1;
                }
                ClaimOutcome::Drained => break,
                ClaimOutcome::NotYetPublished => unreachable!("static space"),
            }
        }
        assert_eq!(covered, 37, "the tail is claimed whole, never dropped");
        assert_eq!(cur.claimed_units(), 37);
        assert_eq!(
            rec.begins.lock().unwrap().len(),
            spans,
            "claim-begin fires once per claimed span"
        );
    }

    /// Growing space: the cursor never claims past the watermark and
    /// reports NotYetPublished (not Drained) until close.
    #[test]
    fn watermark_bounds_claims_on_a_growing_space() {
        struct Growing {
            published: AtomicU64,
            total: Mutex<Option<u64>>,
        }
        impl MorselSource for Growing {
            fn total_units(&self) -> Option<u64> {
                *self.total.lock().unwrap()
            }
            fn published(&self) -> u64 {
                self.published.load(Ordering::Acquire)
            }
            fn next_boundary_after(&self, _unit: u64) -> u64 {
                u64::MAX
            }
        }
        let src = Growing {
            published: AtomicU64::new(1),
            total: Mutex::new(None),
        };
        let cur = ClaimCursor::new();
        assert!(matches!(
            cur.try_claim(&src, 0, &NoObserver),
            ClaimOutcome::Claimed(Span { start: 0, end: 1 })
        ));
        assert_eq!(
            cur.try_claim(&src, 0, &NoObserver),
            ClaimOutcome::NotYetPublished
        );
        src.published.store(3, Ordering::Release);
        assert!(matches!(
            cur.try_claim(&src, 0, &NoObserver),
            ClaimOutcome::Claimed(Span { start: 1, end: 2 })
        ));
        *src.total.lock().unwrap() = Some(3);
        src.published.store(3, Ordering::Release);
        assert!(matches!(
            cur.try_claim(&src, 0, &NoObserver),
            ClaimOutcome::Claimed(Span { start: 2, end: 3 })
        ));
        assert_eq!(cur.try_claim(&src, 0, &NoObserver), ClaimOutcome::Drained);
    }

    /// PC-2.4: releases attached to a claim run at end_claim (drop), in
    /// reverse attach order, exactly once.
    #[test]
    fn end_claim_releases_by_construction() {
        let src = FixedSource {
            total: 1,
            published: AtomicU64::new(1),
            boundary_every: 0,
            coalesce: 1,
        };
        let cur = ClaimCursor::new();
        let order: Mutex<Vec<u32>> = Mutex::new(Vec::new());
        {
            let mut guard = cur.begin_claim(&src, 7, &NoObserver).expect("claim");
            guard.attach_release(|| order.lock().unwrap().push(1));
            guard.attach_release(|| order.lock().unwrap().push(2));
            // guard drops here — end_claim
        }
        assert_eq!(*order.lock().unwrap(), vec![2, 1], "reverse attach order");
    }

    /// PC-2.5: both hooks fire, and claim-narrow refuses sub-spans that
    /// leak outside the owned claim.
    #[test]
    fn hooks_fire_and_narrow_is_contained() {
        let src = FixedSource {
            total: 10,
            published: AtomicU64::new(10),
            boundary_every: 0,
            coalesce: 10,
        };
        let cur = ClaimCursor::new();
        let rec = Recorder::default();
        let guard = cur.begin_claim(&src, 3, &rec).expect("claim");
        assert_eq!(guard.span(), Span { start: 0, end: 10 });
        assert!(guard.narrow(Span { start: 2, end: 4 }).is_some());
        assert!(guard.narrow(Span { start: 8, end: 12 }).is_none());
        assert!(guard.narrow(Span { start: 5, end: 5 }).is_none());
        drop(guard);
        assert_eq!(rec.begins.lock().unwrap().as_slice(), &[(3, Span { start: 0, end: 10 })]);
        assert_eq!(
            rec.narrows.lock().unwrap().as_slice(),
            &[(Span { start: 0, end: 10 }, Span { start: 2, end: 4 })]
        );
    }

    /// Multi-worker race: the cursor hands every unit out exactly once
    /// across racing threads (the CAS is the whole synchronization).
    #[test]
    fn racing_workers_partition_exactly_once() {
        use std::sync::Arc;
        let src = Arc::new(FixedSource {
            total: 10_000,
            published: AtomicU64::new(10_000),
            boundary_every: 64,
            coalesce: 7,
        });
        let cur = Arc::new(ClaimCursor::new());
        let mut handles = Vec::new();
        for w in 0..8 {
            let src = Arc::clone(&src);
            let cur = Arc::clone(&cur);
            handles.push(std::thread::spawn(move || {
                let mut mine: Vec<Span> = Vec::new();
                loop {
                    match cur.try_claim(&*src, w, &NoObserver) {
                        ClaimOutcome::Claimed(s) => mine.push(s),
                        ClaimOutcome::Drained => break,
                        ClaimOutcome::NotYetPublished => unreachable!(),
                    }
                }
                mine
            }));
        }
        let mut all: Vec<Span> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("worker"))
            .collect();
        all.sort_by_key(|s| s.start);
        let mut covered = 0u64;
        for s in &all {
            assert_eq!(s.start, covered, "no gap, no overlap");
            covered = s.end;
        }
        assert_eq!(covered, 10_000);
    }
}
