//! The pgstat change-count protocol — the `static inline` helpers from
//! `utils/pgstat_internal.h` that the per-kind stats files use.
//!
//! These take a pointer to the LIVE shmem counter in C; they are free
//! functions here so callers pass `&mut stats_shmem.changecount` and the
//! protocol runs in place on the shared counter (the counter is held ODD for
//! the whole write window, so concurrent readers retry instead of observing
//! torn stats). The elided effects — `START_CRIT_SECTION` /
//! `END_CRIT_SECTION`, `CHECK_FOR_INTERRUPTS`, and the memory barriers — are
//! process-global with no file-visible result beyond the counter value and
//! the retry decision.

/// `pgstat_begin_changecount_write(cc)`:
/// `Assert((*cc & 1) == 0); START_CRIT_SECTION(); (*cc)++; pg_write_barrier();`
pub(crate) fn pgstat_begin_changecount_write(cc: &mut u32) {
    debug_assert!((*cc & 1) == 0);
    *cc = cc.wrapping_add(1);
}

/// `pgstat_end_changecount_write(cc)`:
/// `Assert((*cc & 1) == 1); pg_write_barrier(); (*cc)++; END_CRIT_SECTION();`
pub(crate) fn pgstat_end_changecount_write(cc: &mut u32) {
    debug_assert!((*cc & 1) == 1);
    *cc = cc.wrapping_add(1);
}

/// `pgstat_begin_changecount_read(cc)`:
/// `before_cc = *cc; CHECK_FOR_INTERRUPTS(); pg_read_barrier(); return before_cc;`
fn pgstat_begin_changecount_read(cc: &u32) -> u32 {
    *cc
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
fn pgstat_end_changecount_read(cc: &u32, before_cc: u32) -> bool {
    let after_cc = *cc;

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
pub(crate) fn pgstat_copy_changecounted_stats<T: Copy>(dst: &mut T, src: &T, cc: &u32) {
    loop {
        let cc_before = pgstat_begin_changecount_read(cc);

        *dst = *src;

        if pgstat_end_changecount_read(cc, cc_before) {
            break;
        }
    }
}
