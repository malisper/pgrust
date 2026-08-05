//! DOMAIN-WORK tripwire counters (proportionality-audit, 2026-07-15).
//!
//! The bug family this watches: per-{plan, open, execution, worker, claim,
//! epoch} work sized by a DOMAIN (dict NDV, row-group count x column count,
//! granule counts, pool width) where the useful work is O(touched data).
//! Confirmed members: CaseDict's exprkey gndv-sized cache fills, the footer_ndv
//! per-plan reparse, dictkey's dense-array zeroing, the ~10M-group byref aggcontext
//! floor. Fix lanes close members; THIS module keeps the class from
//! silently re-entering: every knowingly domain-sized operation left in a
//! hot path ticks its size here, the scan-shutdown SFIN marker dumps the
//! totals next to the touched-side counters (cb_granules / cb_windows),
//! and gates can watch the domain:touched ratio per query class in any
//! banked postmaster log.
//!
//! Discipline:
//! - Tick ONLY domain-sized work (full-domain clears/fills/walks — e.g. a
//!   kill-switch full-clear arm, an eager whole-dict fill, a dense dict
//!   pointer-table build). Proportional (touched-walk) arms do NOT tick —
//!   the counter is the family's exposure meter, not a profiler.
//! - PROCESS-WIDE relaxed atomics (this crate is no_std; no TLS): serial
//!   gate legs read per-query-exact totals; parallel drives smear across
//!   whichever emitter drains first — fine for a tripwire, documented so
//!   nobody reads worker-attribution into the numbers. Tick sites are
//!   per-epoch cold; contention is negligible by construction.
//! - Emission is marker-gated (PGRUST_WFIN, the SFIN channel) so default
//!   runs pay nothing observable beyond the cold adds.

use core::sync::atomic::{AtomicU64, Ordering};

static DZ_BYTES: AtomicU64 = AtomicU64::new(0);
static DZ_ENTRIES: AtomicU64 = AtomicU64::new(0);

/// Record one domain-sized operation: `bytes` initialized/walked over
/// `entries` domain elements. Call from the domain-proportional arm only.
#[inline]
pub fn domain_work_tick(bytes: usize, entries: usize) {
    DZ_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    DZ_ENTRIES.fetch_add(entries as u64, Ordering::Relaxed);
}

/// Drain the accumulated totals (the SFIN emitter's read; also a test
/// seam). Draining keeps successive scans' dumps disjoint.
pub fn domain_work_take() -> (u64, u64) {
    (DZ_BYTES.swap(0, Ordering::Relaxed), DZ_ENTRIES.swap(0, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_accumulates_and_take_drains() {
        // Process-wide statics: drain first so parallel test binaries'
        // other tests can't bleed in (single-test-file discipline here).
        let _ = domain_work_take();
        domain_work_tick(100, 10);
        domain_work_tick(28, 2);
        assert_eq!(domain_work_take(), (128, 12));
        assert_eq!(domain_work_take(), (0, 0));
    }
}
