//! W2a increment 2 — the transaction-private block-run allocator
//! (scratchpad/night/w2-worker-writes-design.md §2).
//!
//! One allocator per parallel write engagement, shared by every writing
//! participant. Each writer CLAIMS a private, disjoint run of target blocks
//! and fills it through its own `BulkInsertStateData` (the run rides the
//! existing `next_free`/`last_free` bulk-extend protocol in hio's
//! `RelationGetBufferForTuple`, so page fill/WAL logic is byte-identical to
//! the serial bulk-insert shape). No two writers ever hold the same target
//! page, by construction.
//!
//! Physical extension happens INSIDE a claim, under the allocator's own
//! mutex, through the existing bufmgr seam with `EB_SKIP_EXTENSION_LOCK`:
//! the target relation is created by THIS transaction under
//! AccessExclusiveLock, so no other backend can extend or scan it — exactly
//! the condition the skip flag encodes (hio.c's own single-caller assert
//! shape). The heavyweight relation-extension lock is never taken. The
//! extended pages are valid zeroed buffers (`smgr_zeroextend` + buffer-table
//! insert inside the seam); the consumer initializes each page at first use
//! (the `PageIsNew -> PageInit` arm hio already has for `next_free` blocks).
//!
//! Claim sizes ramp 8 -> 64 pages (doubling per claim, capped at hio's
//! `MAX_BUFFERS_TO_EXTEND_BY` parity) so small results bound the trailing
//! zero-page waste while big ones amortize the mutex to ~one acquisition per
//! 64 written pages per worker. A single mutex (not the design sketch's
//! lock-free `fetch_add`) is deliberate: claims are per-run, not per-row, so
//! contention is negligible at any supported DOP and the invariants stay
//! auditable.
//!
//! Crash story: the allocator is memory-only; every durable effect goes
//! through ordinary extend/heap-page writes into a relfilenode that dies
//! with the transaction (the standard orphan-file-after-crash-mid-CTAS
//! shape). There is nothing to recover.

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use ::types_core::primitive::BlockNumber;
use ::types_core::ForkNumber;
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_storage::buf::BufferAccessStrategy;

/// First claim size (pages); doubles per claim up to [`RUN_MAX_PAGES`].
const RUN_START_PAGES: u32 = 8;
/// Claim-size cap — `MAX_BUFFERS_TO_EXTEND_BY` parity (hio.rs), and >= the
/// W1 flush quantum in pages so one flush never spans a run boundary
/// mid-page.
const RUN_MAX_PAGES: u32 = 64;

/// A claimed run: `start .. start + len` target blocks, private to the
/// claiming writer until sealed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRun {
    pub start: BlockNumber,
    pub len: u32,
}

struct AllocInner {
    /// Next unclaimed block (== relation EOF at engage, which is 0 for a
    /// fresh CTAS/matview target; probed lazily on first claim).
    next_unclaimed: Option<BlockNumber>,
    /// Blocks `[0, reserved_end)` exist (extended + zero-filled).
    reserved_end: BlockNumber,
    /// Current per-claim run size (ramping).
    quantum: u32,
    /// Debug/oracle: claimed runs, recorded only when tracing is armed.
    run_map: Vec<BlockRun>,
}

/// The transaction-private block-run allocator. `Sync`: shared via `Arc`
/// across the engagement's writer threads.
pub struct BlockRunAllocator {
    inner: Mutex<AllocInner>,
    /// Cumulative claimed pages (letter/witness accounting).
    claimed_pages: AtomicU64,
    /// Record the run map for the placement oracle (e2e only — O(runs)).
    trace_runs: bool,
}

impl BlockRunAllocator {
    pub fn new(trace_runs: bool) -> BlockRunAllocator {
        BlockRunAllocator {
            inner: Mutex::new(AllocInner {
                next_unclaimed: None,
                reserved_end: 0,
                quantum: RUN_START_PAGES,
                run_map: Vec::new(),
            }),
            claimed_pages: AtomicU64::new(0),
            trace_runs,
        }
    }

    /// Claim the next run for one writer. Extends the relation through the
    /// caller's relation handle when the reservation is exhausted (all
    /// writers share one physical relfilenode; the handle only supplies the
    /// locator and persistence). Returns a non-empty run.
    pub fn claim(
        &self,
        rel: &RelationData<'_>,
        strategy: &BufferAccessStrategy,
    ) -> PgResult<BlockRun> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        // Lazy base: first claim starts at the relation's current EOF (0 for
        // a fresh rewrite target; nonzero only if the engage seam ever
        // admits a pre-filled target).
        if g.next_unclaimed.is_none() {
            let nblocks = ::bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                rel,
                ForkNumber::MAIN_FORKNUM,
            )?;
            g.next_unclaimed = Some(nblocks);
            g.reserved_end = nblocks;
        }
        let start = g.next_unclaimed.expect("seeded above");
        let want = g.quantum;
        // Reserve: one bulk extension per shortfall, serialized here. The
        // seam may extend by LESS than requested (pin-limit clamp), so loop;
        // it extends the file BEFORE publishing, so concurrent readers of
        // already-claimed blocks never race the EOF.
        while g.reserved_end < start + want {
            let ask = (start + want - g.reserved_end).max(want).min(RUN_MAX_PAGES);
            let (first_buf, extended_by) = ::bufmgr_seams::extend_buffered_rel_by::call(
                rel,
                ForkNumber::MAIN_FORKNUM,
                strategy.clone(),
                ::bufmgr_seams::EB_SKIP_EXTENSION_LOCK,
                ask,
            )?;
            debug_assert!(extended_by >= 1);
            // The seam returns the first new buffer pinned (rest already
            // unpinned); the allocator hands out blocks, not pins — release.
            if let Some(pin) = ::bufmgr_seams::BufferPin::adopt(first_buf) {
                pin.release();
            }
            g.reserved_end += extended_by;
        }
        // Never hand out more than is reserved (paranoia against a future
        // under-extension arm; today the loop above guarantees the full
        // quantum).
        let len = want.min(g.reserved_end - start);
        debug_assert!(len >= 1);
        g.next_unclaimed = Some(start + len);
        g.quantum = (g.quantum * 2).min(RUN_MAX_PAGES);
        if self.trace_runs {
            g.run_map.push(BlockRun { start, len });
        }
        drop(g);
        self.claimed_pages.fetch_add(len as u64, Ordering::Relaxed);
        Ok(BlockRun { start, len })
    }

    /// Cumulative pages handed out (workers' trailing partial pages
    /// included — the seal-waste bound the letter reports).
    pub fn claimed_pages(&self) -> u64 {
        self.claimed_pages.load(Ordering::Relaxed)
    }

    /// Snapshot of the recorded run map (empty unless tracing was armed).
    pub fn run_map(&self) -> Vec<BlockRun> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).run_map.clone()
    }

    /// Test-only: seed the reservation so `claim` never reaches the extend
    /// seam (the unit corpus runs without a bufmgr).
    #[cfg(test)]
    fn pre_reserve_for_tests(&self, nblocks: BlockNumber) {
        let mut g = self.inner.lock().unwrap();
        g.next_unclaimed = Some(0);
        g.reserved_end = nblocks;
    }

    /// Test-only twin of `claim` that asserts the reservation suffices
    /// (extension is the e2e's surface; the claim ARITHMETIC is the unit's).
    #[cfg(test)]
    fn claim_reserved_for_tests(&self) -> BlockRun {
        let mut g = self.inner.lock().unwrap();
        let start = g.next_unclaimed.expect("pre_reserve_for_tests ran");
        let want = g.quantum;
        assert!(
            g.reserved_end >= start + want,
            "unit claim exceeded the pre-reservation"
        );
        let len = want;
        g.next_unclaimed = Some(start + len);
        g.quantum = (g.quantum * 2).min(RUN_MAX_PAGES);
        if self.trace_runs {
            g.run_map.push(BlockRun { start, len });
        }
        drop(g);
        self.claimed_pages.fetch_add(len as u64, Ordering::Relaxed);
        BlockRun { start, len }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claims are disjoint, contiguous from the base, and the quantum ramps
    /// 8 -> 16 -> 32 -> 64 (capped) — the seal-waste bound's derivation.
    #[test]
    fn claims_are_disjoint_and_ramp() {
        let a = BlockRunAllocator::new(true);
        a.pre_reserve_for_tests(8 + 16 + 32 + 64 + 64 + 64);
        let mut next_expected = 0u32;
        for expect_len in [8u32, 16, 32, 64, 64, 64] {
            let run = a.claim_reserved_for_tests();
            assert_eq!(run.start, next_expected, "runs must tile without gaps");
            assert_eq!(run.len, expect_len, "quantum ramp");
            next_expected += run.len;
        }
        assert_eq!(a.claimed_pages(), (8 + 16 + 32 + 64 + 64 + 64) as u64);
        // The recorded map replays the exact tiling (the e2e placement
        // oracle's ground truth).
        let map = a.run_map();
        assert_eq!(map.len(), 6);
        for w in map.windows(2) {
            assert_eq!(w[0].start + w[0].len, w[1].start, "disjoint + adjacent");
        }
    }

    /// Concurrent claimants never receive overlapping runs and never lose a
    /// page (the single-mutex protocol's whole guarantee, exercised across
    /// threads).
    #[test]
    fn concurrent_claims_never_overlap() {
        use std::sync::Arc;
        let a = Arc::new(BlockRunAllocator::new(false));
        // Enough for 4 threads x 8 claims at the max ramp.
        a.pre_reserve_for_tests(4 * 8 * RUN_MAX_PAGES);
        let mut handles = Vec::new();
        for _ in 0..4 {
            let a = Arc::clone(&a);
            handles.push(std::thread::spawn(move || {
                let mut got: Vec<BlockRun> = Vec::new();
                for _ in 0..8 {
                    got.push(a.claim_reserved_for_tests());
                }
                got
            }));
        }
        let mut all: Vec<BlockRun> = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort_by_key(|r| r.start);
        let mut covered = 0u64;
        for w in all.windows(2) {
            assert!(
                w[0].start + w[0].len <= w[1].start,
                "overlapping runs: {:?} vs {:?}",
                w[0],
                w[1]
            );
        }
        for r in &all {
            covered += r.len as u64;
        }
        assert_eq!(covered, a.claimed_pages(), "no page double-counted or lost");
    }
}

// NB: `BufferAccessStrategy` is `Option<Rc<...>>` (per-thread); `claim`
// only BORROWS the calling writer's strategy for the duration of the extend
// call on that writer's thread — it is never stored in the (Sync) allocator.
