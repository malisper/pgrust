//! The pgstat change-count protocol — the `static inline` helpers from
//! `utils/pgstat_internal.h` that the per-kind stats files use.
//!
//! The counter is a live shmem-resident `AtomicU32` (held ODD for the whole
//! write window, so concurrent readers retry instead of observing torn
//! stats). The C memory barriers map to atomic orderings/fences:
//! `pg_write_barrier` after the begin-increment is a `Release` fence,
//! `pg_write_barrier` before the end-increment is a `Release` store,
//! `pg_read_barrier` after the begin-read is an `Acquire` load, and
//! `pg_read_barrier` before the end-read is an `Acquire` fence (the standard
//! seqlock mapping). The protected stats fields themselves stay plain
//! non-atomic data, as in C; a torn concurrent copy is detected and retried
//! via the counter, never trusted.
//!
//! Deliberate elisions, both effects of unported subsystems:
//! `START_CRIT_SECTION`/`END_CRIT_SECTION` (no critical-section state exists
//! yet) and the `CHECK_FOR_INTERRUPTS()` in `pgstat_begin_changecount_read`
//! (interrupt processing is not ported; the read loop simply retries).

use core::sync::atomic::{fence, AtomicU32, Ordering};

/// `pgstat_begin_changecount_write(cc)`:
/// `Assert((*cc & 1) == 0); START_CRIT_SECTION(); (*cc)++; pg_write_barrier();`
pub(crate) fn pgstat_begin_changecount_write(cc: &AtomicU32) {
    let before = cc.load(Ordering::Relaxed);
    debug_assert!((before & 1) == 0);
    cc.store(before.wrapping_add(1), Ordering::Relaxed);
    // pg_write_barrier(): the odd counter value becomes visible before any
    // subsequent stats-field store.
    fence(Ordering::Release);
}

/// `pgstat_end_changecount_write(cc)`:
/// `Assert((*cc & 1) == 1); pg_write_barrier(); (*cc)++; END_CRIT_SECTION();`
pub(crate) fn pgstat_end_changecount_write(cc: &AtomicU32) {
    let before = cc.load(Ordering::Relaxed);
    debug_assert!((before & 1) == 1);
    // pg_write_barrier() folded into the Release store: all preceding
    // stats-field stores become visible before the counter turns even again.
    cc.store(before.wrapping_add(1), Ordering::Release);
}

/// `pgstat_begin_changecount_read(cc)`:
/// `before_cc = *cc; CHECK_FOR_INTERRUPTS(); pg_read_barrier(); return before_cc;`
fn pgstat_begin_changecount_read(cc: &AtomicU32) -> u32 {
    // pg_read_barrier() folded into the Acquire load; CHECK_FOR_INTERRUPTS()
    // elided (see module docs).
    cc.load(Ordering::Acquire)
}

/// `pgstat_end_changecount_read(cc, before_cc)` — returns true if the read
/// succeeded, false if it needs to be repeated:
///
/// ```text
/// pg_read_barrier();
/// after_cc = *cc;
/// if (before_cc & 1) return false;   /* a write was in progress when we started */
/// return before_cc == after_cc;      /* did writes start+complete while we read? */
/// ```
fn pgstat_end_changecount_read(cc: &AtomicU32, before_cc: u32) -> bool {
    // pg_read_barrier(): the stats-field reads complete before the counter
    // is re-checked.
    fence(Ordering::Acquire);
    let after_cc = cc.load(Ordering::Relaxed);

    if before_cc & 1 != 0 {
        return false;
    }

    before_cc == after_cc
}

/// `pgstat_copy_changecounted_stats(dst, src, len, cc)` — copy `src` into
/// `dst` under the change-count read protocol:
///
/// ```text
/// do { cc_before = begin_read(cc); memcpy(dst, src, len); }
/// while (!end_read(cc, cc_before));
/// ```
///
/// `src` and `cc` are live references into the shmem struct, re-read on every
/// retry iteration; the loop retries while the counter is odd (a write was in
/// progress) or changed during the copy, so the caller never observes a torn
/// snapshot.
pub(crate) fn pgstat_copy_changecounted_stats<T: Copy>(dst: &mut T, src: &T, cc: &AtomicU32) {
    loop {
        let cc_before = pgstat_begin_changecount_read(cc);

        *dst = *src;

        if pgstat_end_changecount_read(cc, cc_before) {
            break;
        }
    }
}
