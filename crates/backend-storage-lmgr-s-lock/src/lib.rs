//! `backend-storage-lmgr-s-lock` — spinlocks.
//!
//! Port of `src/backend/storage/lmgr/s_lock.c` plus the platform-independent
//! portion of `src/include/storage/s_lock.h` (`S_LOCK`/`S_UNLOCK`/
//! `S_INIT_LOCK`/`S_LOCK_FREE`/`TAS`/`TAS_SPIN`/`SPIN_DELAY`/
//! `init_spin_delay` and the `tas`/`spin_delay` primitives).
//!
//! A PostgreSQL spinlock is a single machine word (`slock_t`) living in shared
//! memory, acquired with an atomic test-and-set and released with a
//! fence-ordered store of zero. It is modeled as a `#[repr(transparent)]`
//! wrapper over an [`AtomicI32`] — the ABI of the `int`-width `slock_t` used
//! on the int-based platforms (arm/aarch64), with the same TAS / release
//! semantics. The lock word itself is genuinely cross-backend shared memory,
//! so it is an atomic, not a `thread_local`; the per-backend `spins_per_delay`
//! estimate, by contrast, is C backend-private global state and is a
//! `thread_local!`.
//!
//! When waiting for a contended spinlock we loop tightly for a while, then
//! delay using `pg_usleep()` and try again. Preferably, "awhile" should be a
//! small multiple of the maximum time we expect a spinlock to be held. 100
//! iterations seems about right as an initial guess. However, on a
//! uniprocessor the loop is a waste of cycles, while in a multi-CPU scenario
//! it's usually better to spin a bit longer than to call the kernel, so we
//! try to adapt the spin loop count depending on whether we seem to be in a
//! uniprocessor or multiprocessor.
//!
//! Once we do decide to block, we use randomly increasing `pg_usleep()`
//! delays. The first delay is 1 msec, then the delay randomly increases to
//! about one second, after which we reset to 1 msec and start again. In the
//! presence of heavy contention we need to increase the delay, else the
//! spinlock holder may never get to run and release the lock (consider a
//! holder nice'd down in priority: it will not get scheduled until all
//! would-be acquirers are sleeping, so a constant 1-msec sleep risks
//! starvation). But we can't just clamp the delay to an upper bound, else it
//! would take a long time to make a reasonable number of tries.
//!
//! We time out and declare error after [`NUM_DELAYS`] delays (thus, exactly
//! that many tries). With the given settings, this will usually take 2 or so
//! minutes. It seems better to fix the total number of tries (and thus the
//! probability of unintended failure) than to fix the total time spent.
//!
//! The `S_LOCK_TEST` standalone test harness (`main()` and its supporting
//! statics, compiled only into the `s_lock_test` binary, never the server) is
//! not ported; the in-crate tests cover the same ground.

use std::sync::atomic::{AtomicI32, Ordering};

use backend_utils_error::elog;
use types_error::PANIC;
use types_pgstat::wait_event::WAIT_EVENT_SPIN_DELAY;

/// Minimum value `spins_per_delay` is allowed to converge to.
///
/// Note: you might think this should be just 1, but you'd be wrong; there are
/// platforms where that can result in a "stuck spinlock" failure (seen
/// particularly on Alphas: the first TAS after returning from kernel space
/// always fails on that hardware).
pub const MIN_SPINS_PER_DELAY: i32 = 10;
/// Maximum value `spins_per_delay` is allowed to converge to.
pub const MAX_SPINS_PER_DELAY: i32 = 1000;
/// Number of blocking delays after which a spinlock is declared stuck.
pub const NUM_DELAYS: i32 = 1000;
/// Initial (and reset) backoff sleep length, in microseconds (1 ms).
pub const MIN_DELAY_USEC: i64 = 1000;
/// Maximum backoff sleep length, in microseconds (1 s); the delay wraps back
/// to [`MIN_DELAY_USEC`] once this is exceeded.
pub const MAX_DELAY_USEC: i64 = 1_000_000;

/// `DEFAULT_SPINS_PER_DELAY` (`s_lock.h`) — the per-backend starting estimate
/// of how many tight-spin iterations to attempt before blocking.
pub const DEFAULT_SPINS_PER_DELAY: i32 = 100;

thread_local! {
    /// `static int spins_per_delay` — backend-local copy of the spin
    /// estimate; the shared estimate is folded in via
    /// [`set_spins_per_delay`]/[`update_spins_per_delay`].
    static SPINS_PER_DELAY: std::cell::Cell<i32> =
        const { std::cell::Cell::new(DEFAULT_SPINS_PER_DELAY) };
}

/// `SpinDelayStatus` (`storage/s_lock.h`) — per-waiter spin-delay bookkeeping
/// for a contended spinlock.
///
/// The C `const char *file`/`func` fields hold `__FILE__`/`__func__` for the
/// stuck-spinlock PANIC diagnostic; `None` corresponds to a NULL pointer,
/// reported as `"(unknown)"`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpinDelayStatus {
    /// Tight-spin iterations since the last block.
    pub spins: i32,
    /// Number of blocking `pg_usleep()` delays performed.
    pub delays: i32,
    /// Current backoff sleep length in microseconds.
    pub cur_delay: i32,
    /// `__FILE__` of the wait site.
    pub file: Option<&'static str>,
    /// `__LINE__` of the wait site.
    pub line: i32,
    /// `__func__` of the wait site.
    pub func: Option<&'static str>,
}

/// A PostgreSQL spinlock word (`slock_t`).
///
/// Acquired with an atomic test-and-set ([`Spinlock::tas`]) and released with
/// a fence-ordered store of zero ([`Spinlock::unlock`]). `#[repr(transparent)]`
/// over an `AtomicI32` so the in-memory layout matches the `int`-width
/// `slock_t`.
#[repr(transparent)]
#[derive(Debug, Default)]
pub struct Spinlock {
    word: AtomicI32,
}

impl Spinlock {
    /// A new, free spinlock.
    pub const fn new() -> Self {
        Self {
            word: AtomicI32::new(0),
        }
    }

    /// `S_INIT_LOCK`/`S_UNLOCK` — store zero, releasing the lock.
    ///
    /// `Release` ordering keeps loads and stores issued before the unlock
    /// from being reordered past it, matching PostgreSQL's `S_UNLOCK` fence
    /// requirement (`__sync_lock_release` semantics).
    pub fn unlock(&self) {
        self.word.store(0, Ordering::Release);
    }

    /// `S_LOCK_FREE(lock)` — true when `*lock == 0`.
    pub fn is_free(&self) -> bool {
        self.word.load(Ordering::Relaxed) == 0
    }

    /// `tas(lock)` — atomically set the word to 1 and return the previous
    /// value (0 if the lock was free and is now ours, nonzero if held).
    ///
    /// `Acquire` ordering keeps loads and stores issued after the TAS from
    /// being reordered before it, matching PostgreSQL's `TAS` fence
    /// requirement (`__sync_lock_test_and_set` semantics).
    pub fn tas(&self) -> i32 {
        self.word.swap(1, Ordering::Acquire)
    }

    /// `TAS_SPIN(lock)` — `*(lock) ? 1 : TAS(lock)`.
    ///
    /// On x86_64 and aarch64 it is a win to do a non-locking read of the word
    /// before attempting the (more expensive) atomic TAS while spinning.
    pub fn tas_spin(&self) -> i32 {
        if self.word.load(Ordering::Relaxed) != 0 {
            1
        } else {
            self.tas()
        }
    }
}

/// `S_INIT_LOCK(lock)` — initialize a spinlock to the free state.
pub fn s_init_lock(lock: &Spinlock) {
    lock.unlock();
}

/// `S_UNLOCK(lock)` — release a spinlock.
pub fn s_unlock(lock: &Spinlock) {
    lock.unlock();
}

/// `S_LOCK_FREE(lock)` — true when the lock is not held.
pub fn s_lock_free(lock: &Spinlock) -> bool {
    lock.is_free()
}

/// `TAS(lock)` — the low-level test-and-set. Returns the previous lock value.
pub fn tas(lock: &Spinlock) -> i32 {
    lock.tas()
}

/// `TAS_SPIN(lock)` — TAS with a non-locking pretest, used while spinning.
pub fn tas_spin(lock: &Spinlock) -> i32 {
    lock.tas_spin()
}

/// `S_LOCK(lock)` (`s_lock.h`) — acquire the lock, returning the number of
/// blocking delays incurred. Tries TAS once; only on contention does it fall
/// through to the out-of-line [`s_lock`] backoff loop:
/// `(TAS(lock) ? s_lock((lock), __FILE__, __LINE__, __func__) : 0)`.
pub fn s_lock_macro(
    lock: &Spinlock,
    file: Option<&'static str>,
    line: i32,
    func: Option<&'static str>,
) -> i32 {
    if tas(lock) != 0 {
        s_lock(lock, file, line, func)
    } else {
        0
    }
}

/// `SPIN_DELAY()` — emit one CPU-specific spin-delay hint.
///
/// `isb` on aarch64 (the instruction PostgreSQL's `s_lock.h` uses on this
/// migration profile); the portable spin-loop hint elsewhere.
pub fn spin_delay() {
    #[cfg(target_arch = "aarch64")]
    // SAFETY: ISB is a hint with no memory effects.
    unsafe {
        core::arch::asm!("isb", options(nomem, nostack, preserves_flags));
    }

    #[cfg(not(target_arch = "aarch64"))]
    core::hint::spin_loop();
}

/// `init_spin_delay(status, file, line, func)` — zero the counters and record
/// the wait-site location.
pub fn init_spin_delay(
    file: Option<&'static str>,
    line: i32,
    func: Option<&'static str>,
) -> SpinDelayStatus {
    SpinDelayStatus {
        spins: 0,
        delays: 0,
        cur_delay: 0,
        file,
        line,
        func,
    }
}

/// `s_lock_stuck()` — complain about a stuck spinlock.
///
/// `elog(PANIC, ...)` never returns: PANIC emits the report and aborts the
/// process (C `errfinish` does the same `abort()`), so this diverges. The
/// `S_LOCK_TEST`-only `fprintf`/`exit(1)` branch is not ported.
fn s_lock_stuck(file: Option<&'static str>, line: i32, func: Option<&'static str>) -> ! {
    let func = func.unwrap_or("(unknown)");
    let file = file.unwrap_or("(unknown)");
    let _ = elog(
        PANIC,
        format!("stuck spinlock detected at {func}, {file}:{line}"),
    );
    unreachable!("elog(PANIC) returned");
}

/// `s_lock()` — platform-independent portion of waiting for a spinlock.
///
/// Returns the number of blocking delays incurred.
pub fn s_lock(
    lock: &Spinlock,
    file: Option<&'static str>,
    line: i32,
    func: Option<&'static str>,
) -> i32 {
    let mut delay_status = init_spin_delay(file, line, func);

    while tas_spin(lock) != 0 {
        perform_spin_delay(&mut delay_status);
    }

    finish_spin_delay(&delay_status);

    delay_status.delays
}

/// `perform_spin_delay()` — wait while spinning on a contended spinlock.
pub fn perform_spin_delay(status: &mut SpinDelayStatus) {
    // CPU-specific delay each time through the loop.
    spin_delay();

    // Block the process every spins_per_delay tries.
    status.spins += 1;
    if status.spins >= spins_per_delay() {
        status.delays += 1;
        if status.delays > NUM_DELAYS {
            s_lock_stuck(status.file, status.line, status.func);
        }

        if status.cur_delay == 0 {
            // First time to delay?
            status.cur_delay = MIN_DELAY_USEC as i32;
        }

        // Once we start sleeping, the overhead of reporting a wait event is
        // justified. Actively spinning easily stands out in profilers, but
        // sleeping with an exponential backoff is harder to spot...
        backend_utils_activity_waitevent_seams::pgstat_report_wait_start::call(
            WAIT_EVENT_SPIN_DELAY,
        );
        port_pgsleep_seams::pg_usleep::call(status.cur_delay as i64);
        backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();

        // Increase delay by a random fraction between 1X and 2X.
        let fraction = pg_prng::global_prng(pg_prng::PgPrng::next_f64);
        status.cur_delay += (status.cur_delay as f64 * fraction + 0.5) as i32;
        // Wrap back to minimum delay when max is exceeded.
        if status.cur_delay as i64 > MAX_DELAY_USEC {
            status.cur_delay = MIN_DELAY_USEC as i32;
        }

        status.spins = 0;
    }
}

/// `finish_spin_delay()` — after acquiring a spinlock, update estimates about
/// how long to loop.
///
/// If we were able to acquire the lock without delaying, it's a good
/// indication we are in a multiprocessor. If we had to delay, it's a sign
/// (but not a sure thing) that we are in a uniprocessor. Hence, we decrement
/// `spins_per_delay` slowly when we had to delay, and increase it rapidly
/// when we didn't. It's expected that `spins_per_delay` will converge to the
/// minimum value on a uniprocessor and to the maximum value on a
/// multiprocessor.
///
/// Note: `spins_per_delay` is local within our current backend. We want to
/// average these observations across multiple backends, since it's relatively
/// rare for this function to even get entered, and so a single backend might
/// not live long enough to converge on a good value. That is handled by
/// [`set_spins_per_delay`]/[`update_spins_per_delay`].
pub fn finish_spin_delay(status: &SpinDelayStatus) {
    let current = spins_per_delay();
    if status.cur_delay == 0 {
        // We never had to delay.
        if current < MAX_SPINS_PER_DELAY {
            set_spins_per_delay((current + 100).min(MAX_SPINS_PER_DELAY));
        }
    } else if current > MIN_SPINS_PER_DELAY {
        set_spins_per_delay((current - 1).max(MIN_SPINS_PER_DELAY));
    }
}

/// `set_spins_per_delay()` — set the backend-local spin estimate during
/// backend startup. NB: has to be pretty fast as it is called while holding a
/// spinlock.
pub fn set_spins_per_delay(shared_spins_per_delay: i32) {
    SPINS_PER_DELAY.with(|s| s.set(shared_spins_per_delay));
}

/// `update_spins_per_delay()` — fold the backend-local estimate into the
/// shared estimate during backend exit. NB: has to be pretty fast as it is
/// called while holding a spinlock.
///
/// We use an exponential moving average with a relatively slow adaption rate,
/// so that noise in any one backend's result won't affect the shared value
/// too much. As long as both inputs are within the allowed range, the result
/// must be too, so we need not worry about clamping the result.
///
/// We deliberately truncate rather than rounding; this is so that single
/// adjustments inside a backend can affect the shared estimate (see the
/// asymmetric adjustment rules in [`finish_spin_delay`]).
pub fn update_spins_per_delay(shared_spins_per_delay: i32) -> i32 {
    (shared_spins_per_delay * 15 + spins_per_delay()) / 16
}

/// Read the current backend-local spin estimate (`spins_per_delay`).
pub fn spins_per_delay() -> i32 {
    SPINS_PER_DELAY.with(std::cell::Cell::get)
}

/// This crate declares no inward seams; nothing to install.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::Once;

    // Each #[test] runs on its own thread, so thread-local recording (and the
    // thread-local spins_per_delay itself) isolates tests from one another;
    // the seam slots are process-global and installed once.
    thread_local! {
        static WAIT_START_INFO: Cell<Option<u32>> = const { Cell::new(None) };
        static WAIT_ENDS: Cell<i32> = const { Cell::new(0) };
        static LAST_SLEEP_USEC: Cell<i64> = const { Cell::new(-1) };
    }

    fn install_test_seams() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            backend_utils_activity_waitevent_seams::pgstat_report_wait_start::set(|info| {
                WAIT_START_INFO.with(|c| c.set(Some(info)));
            });
            backend_utils_activity_waitevent_seams::pgstat_report_wait_end::set(|| {
                WAIT_ENDS.with(|c| c.set(c.get() + 1));
            });
            port_pgsleep_seams::pg_usleep::set(|usec| {
                LAST_SLEEP_USEC.with(|c| c.set(usec));
            });
        });
    }

    #[test]
    fn spinlock_basic_state_matches_postgres_api() {
        let lock = Spinlock::new();
        s_init_lock(&lock);
        assert!(s_lock_free(&lock));
        // First TAS acquires (returns previous value 0).
        assert_eq!(tas(&lock), 0);
        assert!(!s_lock_free(&lock));
        // Second TAS sees it held (returns 1).
        assert_eq!(tas(&lock), 1);
        s_unlock(&lock);
        assert!(s_lock_free(&lock));
    }

    #[test]
    fn tas_spin_pretest_short_circuits_when_held() {
        let lock = Spinlock::new();
        assert_eq!(tas_spin(&lock), 0); // free -> acquires
        assert_eq!(tas_spin(&lock), 1); // held -> pretest returns 1
        s_unlock(&lock);
    }

    #[test]
    fn s_lock_macro_uncontended_returns_zero_delays() {
        let lock = Spinlock::new();

        let delays = s_lock_macro(&lock, None, 0, None);

        assert_eq!(delays, 0);
        assert!(!s_lock_free(&lock));
        // S_LOCK calls s_lock() only when TAS reports contention; on an
        // uncontended acquire the macro short-circuits to 0 and
        // finish_spin_delay() never runs, so the estimate stays put.
        assert_eq!(spins_per_delay(), DEFAULT_SPINS_PER_DELAY);
        s_unlock(&lock);
    }

    #[test]
    fn s_lock_on_free_lock_acquires_without_delay() {
        let lock = Spinlock::new();

        let delays = s_lock(&lock, None, 0, None);

        assert_eq!(delays, 0);
        assert!(!s_lock_free(&lock));
        // finish_spin_delay with cur_delay == 0 bumps the estimate by 100.
        assert_eq!(spins_per_delay(), DEFAULT_SPINS_PER_DELAY + 100);
        s_unlock(&lock);
    }

    #[test]
    fn perform_spin_delay_blocks_and_advances_backoff() {
        install_test_seams();
        // Block on the very first spin.
        set_spins_per_delay(1);

        let mut status = init_spin_delay(Some("file.c"), 7, Some("func"));
        perform_spin_delay(&mut status);

        assert_eq!(status.delays, 1);
        assert_eq!(status.spins, 0);
        // First delay seeds cur_delay to MIN, then grows by a random fraction
        // in [1.0, 2.0); cur_delay += cur_delay * frac + 0.5 truncated.
        assert!(status.cur_delay >= MIN_DELAY_USEC as i32);
        assert!(status.cur_delay <= 2 * MIN_DELAY_USEC as i32 + 1);
        // We reported the spin-delay wait and slept the seeded MIN delay.
        assert_eq!(
            WAIT_START_INFO.with(Cell::get),
            Some(WAIT_EVENT_SPIN_DELAY)
        );
        assert_eq!(WAIT_ENDS.with(Cell::get), 1);
        assert_eq!(LAST_SLEEP_USEC.with(Cell::get), MIN_DELAY_USEC);
    }

    #[test]
    fn perform_spin_delay_does_not_block_before_threshold() {
        install_test_seams();
        set_spins_per_delay(100);

        let mut status = init_spin_delay(None, 0, None);
        perform_spin_delay(&mut status);

        assert_eq!(status.spins, 1);
        assert_eq!(status.delays, 0);
        assert_eq!(status.cur_delay, 0);
        // No block yet, so no wait was reported on this thread.
        assert_eq!(WAIT_ENDS.with(Cell::get), 0);
    }

    #[test]
    fn finish_spin_delay_increases_when_no_delay() {
        set_spins_per_delay(100);
        let status = init_spin_delay(None, 0, None); // cur_delay == 0
        finish_spin_delay(&status);
        assert_eq!(spins_per_delay(), 200);
    }

    #[test]
    fn finish_spin_delay_clamps_increase_to_max() {
        set_spins_per_delay(MAX_SPINS_PER_DELAY - 1);
        let status = init_spin_delay(None, 0, None);
        finish_spin_delay(&status);
        assert_eq!(spins_per_delay(), MAX_SPINS_PER_DELAY);
    }

    #[test]
    fn finish_spin_delay_decreases_after_delay() {
        set_spins_per_delay(100);
        let mut status = init_spin_delay(None, 0, None);
        status.cur_delay = MIN_DELAY_USEC as i32; // we did delay
        finish_spin_delay(&status);
        assert_eq!(spins_per_delay(), 99);
    }

    #[test]
    fn finish_spin_delay_clamps_decrease_to_min() {
        set_spins_per_delay(MIN_SPINS_PER_DELAY);
        let mut status = init_spin_delay(None, 0, None);
        status.cur_delay = MIN_DELAY_USEC as i32;
        finish_spin_delay(&status);
        assert_eq!(spins_per_delay(), MIN_SPINS_PER_DELAY);
    }

    #[test]
    fn update_spins_per_delay_uses_postgres_moving_average() {
        set_spins_per_delay(100);
        // (200 * 15 + 100) / 16 = 3100 / 16 = 193 (truncated).
        assert_eq!(update_spins_per_delay(200), 193);
    }
}
