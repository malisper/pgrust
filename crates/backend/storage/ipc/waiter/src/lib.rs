//! Structured wait primitive (M0 lane C, parallelism-redesign §2.6).
//!
//! One `Waiter` per thread; wakers address it through a token-issued
//! [`WakerHandle`] owned by the waker side. There is NO global pid-keyed
//! registry: a handle names exactly one waiter incarnation (slot + token),
//! so the two P1 wedge classes of the latch substrate are structurally
//! impossible —
//!   * pid-collision hijack: nothing is keyed by (synthetic) pids, so two
//!     threads can never claim each other's wake route;
//!   * stale-rekey deafness: a reused waiter incarnation gets a fresh token,
//!     so a stale handle unparks nobody (`Unparked::Stale` no-op) instead of
//!     silently shadowing the live route. The LIVE route is re-published by
//!     the waiter itself at every wait (e.g. into `Latch.waker`), so there is
//!     no registry entry to forget to re-key.
//!
//! Park modes:
//!   * `park` / `park_timeout` — block on the slot's Mutex+Condvar pair (the
//!     OS futex under std's hood). BOTH are bounded by the optional built-in
//!     recheck cadence (subsumes PGRUST_MQ_RECHECK_MS as a debug backstop,
//!     NOT a correctness crutch): the park returns `ParkResult::Recheck` and
//!     the caller re-tests its predicate, so even an injected lost wake
//!     costs one cadence period. Timed parks sleep min(timeout, cadence) per
//!     lap (GL-RECWAKE-1: a minutes-class caller timeout — the checkpointer
//!     main lap — is no lost-wake backstop; recovery boot and shutdown both
//!     wedged on it). `sleep` alone is exempt: time is its own predicate.
//!   * fd-park (`begin_fd_park` / `end_fd_park`) — for waits that must block
//!     in epoll/kqueue on sockets: the waiter exposes a self-pipe read fd the
//!     WaitEventSet registers; `unpark` of an fd-parked waiter writes one
//!     byte. The pipe is waiter-owned and reached only through a validated
//!     handle — the waker side never touches an fd table keyed by identity.
//!
//! Owner death poisons the slot: the thread-exit guard bumps the token and
//! frees the slot; late unparks observe `Unparked::Stale`.
//!
//! DST hook: all timed parking flows through the installable
//! [`clock::WaiterClock`] provider (real monotonic clock + condvar timeouts
//! in production; `clock::virtual_time::VirtualClock` for deterministic
//! tests).
//!
//! Test fault injection: PGRUST_TEST_DROP_WAKE_1IN=N drops every Nth
//! CROSS-thread unpark before it touches waiter state — exactly the
//! lost-wake shape the old self-pipe injector rendered (condition flags
//! already stored, sleeper never signaled), now at the Waiter layer.
//!
//! Loom: build with RUSTFLAGS="--cfg loom" to compile only the slot core
//! (state machine + park/unpark protocol) against loom's sync primitives;
//! `tests/loom.rs` holds the models (park/unpark races incl. wake-before-
//! park, handle reuse, poison, and the latch-over-Waiter Dekker
//! equivalence).

pub(crate) mod sync {
    // The old per-crate cfg(loom) shim SUPERSEDED by pgsync, THE single lock
    // library (permit-s1 contract §0): call sites import unconditionally;
    // world selection lives in pgsync alone. This hub's blocking rows stay
    // lint-annotated SCHED-VISIBLE — under sim its parks are hooked ops.
    pub(crate) use pgsync::{atomic, Condvar, Mutex, MutexGuard};
}

pub mod clock;
pub mod io;

use sync::MutexGuard;
#[cfg(not(loom))]
use sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
use pgsync::OnceLock;
#[cfg(not(loom))]
use std::time::Duration;

/// Waiter slot capacity: bounds peak concurrent threads-with-waiters, not
/// threads served over the process lifetime (slots are recycled on thread
/// exit). Same sizing rationale as latch's LOCAL_LATCH_CAP.
pub const WAITER_CAP: usize = 8192;

/// Waker-side name for one waiter incarnation: packed (slot+1) << 32 | token.
/// Never zero, so an AtomicU64 of 0 means "no handle published".
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WakerHandle(u64);

impl WakerHandle {
    fn new(slot: usize, token: u32) -> Self {
        WakerHandle(((slot as u64 + 1) << 32) | token as u64)
    }

    /// The packed word, for storage in an AtomicU64 (0 = none).
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Rebuild from a packed word; None for the 0 "no handle" value.
    pub fn from_u64(word: u64) -> Option<Self> {
        ((word >> 32) != 0).then_some(WakerHandle(word))
    }

    fn unpack(self) -> (usize, u32) {
        (((self.0 >> 32) - 1) as usize, self.0 as u32)
    }
}

/// Outcome of a park.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParkResult {
    /// A waker delivered an unpark (possibly before the park began).
    Notified,
    /// The caller's timeout elapsed (only for `park_timeout`).
    TimedOut,
    /// The built-in recheck cadence elapsed before the park's own deadline
    /// (or on an untimed park): the caller must re-test its wait predicate
    /// and re-park (the lost-wake backstop; never returned when the cadence
    /// is disabled).
    Recheck,
}

/// Outcome of an unpark.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Unparked {
    /// The waiter was parked and has been woken.
    Delivered,
    /// The waiter was not parked; the notification is latched and its next
    /// park returns immediately.
    Pending,
    /// The handle no longer names a live waiter incarnation (owner death or
    /// token reissue). Structurally a no-op.
    Stale,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Mode {
    /// Slot not issued to any thread.
    Free,
    /// Issued; owner not blocked.
    Idle,
    /// An unpark is latched; the next park consumes it.
    Notified,
    /// Owner blocked on the slot condvar.
    Parked,
    /// Owner blocked in epoll/kqueue with the wake pipe registered.
    ParkedFd,
}

#[doc(hidden)]
pub struct SlotInner {
    token: u32,
    mode: Mode,
    /// Wake self-pipe (read, write) ends; -1 = not created. Owned by the
    /// slot, closed on retire.
    wake_rfd: i32,
    wake_wfd: i32,
}

/// One waiter's parking state. Production code reaches slots through the
/// global slab + [`WakerHandle`]s; the loom models construct slots directly
/// and drive the `*_token` core methods.
pub struct Slot {
    inner: sync::Mutex<SlotInner>,
    cv: sync::Condvar,
}

impl Slot {
    #[cfg(not(loom))]
    const fn new() -> Self {
        Slot {
            inner: sync::Mutex::new(SlotInner {
                token: 0,
                mode: Mode::Free,
                wake_rfd: -1,
                wake_wfd: -1,
            }),
            cv: sync::Condvar::new(),
        }
    }

    /// Loom-model constructor (loom's Mutex is not const-constructible).
    #[cfg(loom)]
    pub fn new_for_model() -> Self {
        Slot {
            inner: sync::Mutex::new(SlotInner {
                token: 0,
                mode: Mode::Free,
                wake_rfd: -1,
                wake_wfd: -1,
            }),
            cv: sync::Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SlotInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn cv(&self) -> &sync::Condvar {
        &self.cv
    }

    pub(crate) fn lock_inner(&self) -> MutexGuard<'_, SlotInner> {
        self.lock()
    }

    /// Condvar wait hook for loom-model clocks (loom's condvar cannot be
    /// reached through the pub(crate) accessor from an integration test).
    #[cfg(loom)]
    pub fn wait_for_model<'a>(&self, guard: MutexGuard<'a, SlotInner>) -> MutexGuard<'a, SlotInner> {
        self.cv.wait(guard).unwrap_or_else(|e| e.into_inner())
    }

    // -- slot core: the protocol the loom models verify ---------------------

    /// Issue this slot to an owner: Free -> Idle; returns the token that
    /// makes handles to this incarnation live.
    pub fn issue_token(&self) -> u32 {
        let mut g = self.lock();
        assert_eq!(g.mode, Mode::Free, "slot already issued");
        g.mode = Mode::Idle;
        g.token
    }

    /// Retire the incarnation (owner death / poison): token bump makes every
    /// outstanding handle Stale; the slot returns to Free.
    pub fn retire_token(&self) -> (i32, i32) {
        let mut g = self.lock();
        debug_assert!(g.mode != Mode::Free, "waiter slot retired twice");
        g.token = g.token.wrapping_add(1);
        g.mode = Mode::Free;
        (
            std::mem::replace(&mut g.wake_rfd, -1),
            std::mem::replace(&mut g.wake_wfd, -1),
        )
    }

    /// Reissue in place (pooled-thread reuse boundary): outstanding handles
    /// go Stale, a latched notify from the previous incarnation is dropped,
    /// the owner keeps the slot. Returns the fresh token.
    pub fn reissue_token(&self) -> u32 {
        let mut g = self.lock();
        debug_assert!(
            matches!(g.mode, Mode::Idle | Mode::Notified),
            "token reissue while parked"
        );
        g.token = g.token.wrapping_add(1);
        g.mode = Mode::Idle;
        g.token
    }

    /// Wake the incarnation named by `token`. Callable from any thread;
    /// allocation-free; the slot mutex is uncontended and never held across
    /// blocking.
    pub fn unpark_token(&self, token: u32) -> Unparked {
        let mut g = self.lock();
        if g.token != token || g.mode == Mode::Free {
            return Unparked::Stale;
        }
        match g.mode {
            Mode::Free => unreachable!(),
            Mode::Idle => {
                g.mode = Mode::Notified;
                Unparked::Pending
            }
            Mode::Notified => Unparked::Pending,
            Mode::Parked => {
                g.mode = Mode::Notified;
                drop(g);
                self.cv.notify_all();
                Unparked::Delivered
            }
            Mode::ParkedFd => {
                g.mode = Mode::Notified;
                let wfd = g.wake_wfd;
                drop(g);
                if wfd >= 0 {
                    send_wake_byte(wfd);
                }
                Unparked::Delivered
            }
        }
    }

    /// Owner-side park. `timeout_ms` = the caller's own deadline (returns
    /// TimedOut); `cadence_ms` = the lost-wake recheck backstop (returns
    /// Recheck). Both None = park until a real unpark.
    ///
    /// The cadence bounds TIMED parks too (GL-RECWAKE-1): a dropped
    /// cross-thread unpark leaves the caller's predicate flagged but this
    /// slot un-notified, and a caller deadline of minutes-class (checkpointer
    /// main lap = checkpoint_timeout) is no backstop inside a 90s boot window
    /// or a 20s shutdown grace. The park sleeps at most
    /// min(timeout, cadence); a cadence expiry returns Recheck so the caller
    /// re-tests its predicate and re-parks — a lost wake costs one cadence
    /// period on every park mode, not just untimed ones.
    pub fn park_core(
        &self,
        timeout_ms: Option<i64>,
        cadence_ms: Option<i64>,
        clk: &dyn clock::WaiterClock,
    ) -> ParkResult {
        let mut g = self.lock();
        debug_assert!(
            matches!(g.mode, Mode::Idle | Mode::Notified),
            "park while already parked"
        );
        if g.mode == Mode::Notified {
            g.mode = Mode::Idle;
            return ParkResult::Notified;
        }
        g.mode = Mode::Parked;
        let bound_ms = match (timeout_ms, cadence_ms) {
            (Some(t), Some(c)) => Some(t.min(c)),
            (Some(t), None) => Some(t),
            (None, c) => c,
        };
        let start = if bound_ms.is_some() { clk.now_ms() } else { 0 };
        loop {
            let remaining = bound_ms.map(|b| b - (clk.now_ms() - start));
            if let Some(r) = remaining {
                if r <= 0 {
                    g.mode = Mode::Idle;
                    // The caller's own deadline expired => TimedOut; a
                    // shorter cadence lap => Recheck (re-test + re-park).
                    return match timeout_ms {
                        Some(t) if clk.now_ms() - start >= t => ParkResult::TimedOut,
                        _ => ParkResult::Recheck,
                    };
                }
            }
            let (g2, _timed_out) = clk.wait(self, g, remaining);
            g = g2;
            if g.mode == Mode::Notified {
                g.mode = Mode::Idle;
                return ParkResult::Notified;
            }
            // Spurious condvar wake or timeout: re-evaluate the deadline.
        }
    }
}

fn send_wake_byte(write_fd: i32) {
    let dummy = [0u8; 1];
    loop {
        // SAFETY: 1-byte write to a live nonblocking pipe fd owned by a slot
        // whose mutex serialized us against close.
        let rc = unsafe { libc::write(write_fd, dummy.as_ptr().cast(), 1) };
        if rc >= 0 {
            return;
        }
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno == libc::EINTR {
            continue;
        }
        // EAGAIN: queued bytes already wake the waiter; anything else is
        // ignored exactly as the old signal-handler-reachable path was.
        return;
    }
}

// ===========================================================================
// Global layer: slab, thread-owned waiter, handles. Not compiled under loom
// (loom models drive the slot core directly).
// ===========================================================================

#[cfg(not(loom))]
mod global {
    use super::*;

    static SLOTS: [Slot; WAITER_CAP] = [const { Slot::new() }; WAITER_CAP];
    static SLOT_FREE: sync::Mutex<Vec<usize>> = sync::Mutex::new(Vec::new());
    static SLOT_NEXT: sync::Mutex<usize> = sync::Mutex::new(0);

    fn slot(idx: usize) -> &'static Slot {
        &SLOTS[idx]
    }

    struct WaiterGuard {
        slot: usize,
    }

    impl Drop for WaiterGuard {
        // Poison-on-owner-death: bump the token (all outstanding handles go
        // Stale), close the wake pipe, return the slot for reuse.
        fn drop(&mut self) {
            let (rfd, wfd) = slot(self.slot).retire_token();
            for fd in [rfd, wfd] {
                if fd >= 0 {
                    // SAFETY: closing pipe fds this slot owned.
                    unsafe { libc::close(fd) };
                }
            }
            SLOT_FREE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(self.slot);
        }
    }

    thread_local! {
        static CURRENT: std::cell::RefCell<Option<WaiterGuard>> =
            const { std::cell::RefCell::new(None) };
    }

    fn allocate_slot() -> usize {
        let recycled = SLOT_FREE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop();
        let idx = recycled.unwrap_or_else(|| {
            let mut next = SLOT_NEXT.lock().unwrap_or_else(|e| e.into_inner());
            let idx = *next;
            assert!(idx < WAITER_CAP, "waiter slot slab exhausted");
            *next += 1;
            idx
        });
        slot(idx).issue_token();
        idx
    }

    /// This thread's waiter slot, created on first use.
    pub(super) fn current_slot() -> usize {
        CURRENT.with(|c| {
            let mut c = c.borrow_mut();
            if let Some(g) = c.as_ref() {
                return g.slot;
            }
            let idx = allocate_slot();
            *c = Some(WaiterGuard { slot: idx });
            idx
        })
    }

    pub(super) fn is_current_slot(idx: usize) -> bool {
        CURRENT.with(|c| c.borrow().as_ref().map(|g| g.slot) == Some(idx))
    }

    /// The current thread's waiter handle (allocates the waiter on first
    /// use). Publish it (e.g. into `Latch.waker`) BEFORE the final predicate
    /// re-check that precedes the park — the Dekker discipline of the latch.
    pub fn current_handle() -> WakerHandle {
        let idx = current_slot();
        let token = slot(idx).lock().token;
        WakerHandle::new(idx, token)
    }

    /// Reissue the current thread's token: every outstanding handle to this
    /// waiter goes Stale. Pooled-thread reuse boundary (wretain warm claim) —
    /// the new task's waits publish fresh handles, so a stray wake aimed at
    /// the previous task can never reach the new one. Subsumes the old
    /// rekey_wakeup_registry (which mutated a shared pid-keyed table and,
    /// when missed, silently dropped every wake — the P1 stale-rekey wedge).
    pub fn reissue_current_token() {
        let idx = current_slot();
        slot(idx).reissue_token();
    }

    // -- recheck cadence (debug backstop) -----------------------------------

    /// Untimed-park recheck cadence in ms. PGRUST_WAITER_RECHECK_MS wins,
    /// else PGRUST_MQ_RECHECK_MS (the knob this subsumes), default 1000;
    /// <= 0 disables (parks sleep until a real unpark).
    pub fn recheck_cadence_ms() -> i64 {
        static CADENCE: OnceLock<i64> = OnceLock::new();
        *CADENCE.get_or_init(|| {
            for var in ["PGRUST_WAITER_RECHECK_MS", "PGRUST_MQ_RECHECK_MS"] {
                if let Some(v) = std::env::var(var).ok().and_then(|v| v.parse::<i64>().ok()) {
                    return v;
                }
            }
            1_000
        })
    }

    // -- fault injection (test-only) -----------------------------------------

    fn drop_wake_1in() -> u64 {
        static N: OnceLock<u64> = OnceLock::new();
        *N.get_or_init(|| {
            std::env::var("PGRUST_TEST_DROP_WAKE_1IN")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        })
    }

    fn inject_drop_wake(target_slot: usize) -> bool {
        let n = drop_wake_1in();
        if n == 0 {
            return false;
        }
        // Self-wakes are never dropped (parity with the old injector, which
        // hit only WakeupOtherProc's cross-backend pipe writes).
        if is_current_slot(target_slot) {
            return false;
        }
        static DROP_CLOCK: AtomicU64 = AtomicU64::new(0);
        DROP_CLOCK.fetch_add(1, Ordering::Relaxed) % n == n - 1
    }

    // -- unpark ---------------------------------------------------------------

    /// Wake the waiter named by `handle`. Callable from any thread;
    /// allocation-free.
    pub fn unpark(handle: WakerHandle) -> Unparked {
        let (idx, token) = handle.unpack();
        if idx >= WAITER_CAP {
            return Unparked::Stale;
        }
        if inject_drop_wake(idx) {
            // Test-only lost-wake: state untouched, sleeper never signaled —
            // recovered by the recheck cadence, as the wedge e2e proves.
            return Unparked::Delivered;
        }
        slot(idx).unpark_token(token)
    }

    /// `unpark` on a packed AtomicU64 word (0 = no handle, no-op).
    pub fn unpark_word(word: u64) -> Unparked {
        match WakerHandle::from_u64(word) {
            Some(h) => unpark(h),
            None => Unparked::Stale,
        }
    }

    // -- park -----------------------------------------------------------------

    /// Park until unparked. With the recheck cadence enabled the sleep is
    /// bounded and returns `Recheck`; the caller re-tests its predicate and
    /// parks again. Never returns `TimedOut`.
    pub fn park() -> ParkResult {
        let cadence = recheck_cadence_ms();
        let idx = current_slot();
        slot(idx).park_core(None, (cadence > 0).then_some(cadence), clock::provider())
    }

    /// Park until unparked or `timeout` elapses. The sleep is additionally
    /// bounded by the recheck cadence (GL-RECWAKE-1): when the cadence is
    /// shorter than the remaining timeout the park returns `Recheck` and the
    /// caller re-tests its predicate — a lost wake costs one cadence period,
    /// never the full caller timeout (the recovery-boot wedge shape: a
    /// checkpoint_timeout-long lap deaf to an already-set latch).
    pub fn park_timeout(timeout: Duration) -> ParkResult {
        let cadence = recheck_cadence_ms();
        let idx = current_slot();
        slot(idx).park_core(
            Some(timeout.as_millis().min(i64::MAX as u128) as i64),
            (cadence > 0).then_some(cadence),
            clock::provider(),
        )
    }

    /// Timed sleep through the waiter (s_lock backoff etc.): rides the DST
    /// clock hook. A pending unpark aimed at this thread ends the sleep
    /// early — callers are backoff loops for which an early return is
    /// harmless. NO cadence lap: a sleep's predicate is time itself, so a
    /// lost wake cannot extend it and there is nothing to re-test.
    pub fn sleep(timeout: Duration) {
        let idx = current_slot();
        let _ = slot(idx).park_core(
            Some(timeout.as_millis().min(i64::MAX as u128) as i64),
            None,
            clock::provider(),
        );
    }

    /// Monotonic milliseconds from the installed clock provider — wait-site
    /// deadline math must use this (not the OS clock) so virtual-time tests
    /// drive timeouts coherently.
    pub fn now_ms() -> i64 {
        clock::provider().now_ms()
    }

    // -- fd-park mode (epoll/kqueue integration) ------------------------------

    /// Ensure this thread's wake pipe exists; returns the READ end for the
    /// caller to register in its WaitEventSet. Errors are raw errno.
    ///
    /// wasm32: WASI p1 has no pipe(2); fd-park is native-only until the boot
    /// increment lands the WASI waiteventset backend (poll_oneoff/latch-park).
    #[cfg(target_family = "wasm")]
    pub fn ensure_wake_pipe() -> Result<i32, i32> {
        Err(libc::ENOSYS)
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn ensure_wake_pipe() -> Result<i32, i32> {
        let idx = current_slot();
        let s = slot(idx);
        let mut g = s.lock();
        if g.wake_rfd >= 0 {
            return Ok(g.wake_rfd);
        }
        let mut pipefd = [0i32; 2];
        // SAFETY: pipe(2) into a 2-slot array.
        if unsafe { libc::pipe(pipefd.as_mut_ptr()) } < 0 {
            return Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0));
        }
        for fd in pipefd {
            for (cmd, arg) in
                [(libc::F_SETFL, libc::O_NONBLOCK), (libc::F_SETFD, libc::FD_CLOEXEC)]
            {
                // SAFETY: fcntl on fds just created.
                if unsafe { libc::fcntl(fd, cmd, arg) } == -1 {
                    let errno =
                        std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                    for fd in pipefd {
                        // SAFETY: closing the fds we just created.
                        unsafe { libc::close(fd) };
                    }
                    return Err(errno);
                }
            }
        }
        g.wake_rfd = pipefd[0];
        g.wake_wfd = pipefd[1];
        Ok(pipefd[0])
    }

    /// This thread's wake-pipe read fd (must exist).
    pub fn wake_read_fd() -> i32 {
        let idx = current_slot();
        let fd = slot(idx).lock().wake_rfd;
        assert!(fd >= 0, "waiter wake pipe not created (ensure_wake_pipe)");
        fd
    }

    /// Enter fd-park: subsequent unparks write the wake pipe. Returns false
    /// if a notification is already pending (consumed) — the caller should
    /// poll its event set with a zero timeout instead of blocking.
    pub fn begin_fd_park() -> bool {
        let idx = current_slot();
        let mut g = slot(idx).lock();
        // wasm32: no wake pipe can exist (no pipe(2)); "parked" is pure mode
        // bookkeeping — the wasm wait backend blocks on time alone.
        #[cfg(not(target_family = "wasm"))]
        debug_assert!(g.wake_wfd >= 0, "fd-park without a wake pipe");
        match g.mode {
            Mode::Notified => {
                g.mode = Mode::Idle;
                false
            }
            Mode::Idle => {
                g.mode = Mode::ParkedFd;
                true
            }
            m => panic!("begin_fd_park in mode {m:?}"),
        }
    }

    /// Leave fd-park (after the epoll/kqueue call returns), consuming any
    /// notification that arrived meanwhile. The caller's loop re-tests its
    /// predicates (latch is_set etc.) as it always did.
    pub fn end_fd_park() {
        let idx = current_slot();
        let mut g = slot(idx).lock();
        match g.mode {
            Mode::ParkedFd | Mode::Notified => g.mode = Mode::Idle,
            m => panic!("end_fd_park in mode {m:?}"),
        }
    }

    /// Drain the wake pipe (all pending bytes). Call on wake-fd readability.
    pub fn drain_wake_fd() -> Result<(), i32> {
        let fd = wake_read_fd();
        let mut buf = [0u8; 1024];
        loop {
            // SAFETY: read into a stack buffer of the stated length.
            let rc = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
            if rc < 0 {
                let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                    return Ok(());
                }
                if errno == libc::EINTR {
                    continue;
                }
                return Err(errno);
            }
            if rc == 0 {
                return Err(0); // unexpected EOF
            }
            if (rc as usize) < buf.len() {
                return Ok(());
            }
        }
    }

    // -- diagnostics ----------------------------------------------------------

    /// One-line routing state for a published handle word, for stall
    /// self-reports. 0 = no handle published ("MISSING" — nothing can route
    /// a wake); a token mismatch is "STALE" (the deaf-endpoint signature).
    pub fn describe_word(word: u64) -> String {
        let Some(h) = WakerHandle::from_u64(word) else {
            return "MISSING".into();
        };
        let (idx, token) = h.unpack();
        if idx >= WAITER_CAP {
            return format!("INVALID(slot:{idx})");
        }
        let g = slot(idx).lock();
        if g.token != token || g.mode == Mode::Free {
            return format!("STALE(slot:{idx} tok:{token} cur:{})", g.token);
        }
        let mode = match g.mode {
            Mode::Free => "free",
            Mode::Idle => "idle",
            Mode::Notified => "notified",
            Mode::Parked => "parked",
            Mode::ParkedFd => "parked-fd",
        };
        format!("waiter:slot={idx} tok={token} state={mode}")
    }
}

#[cfg(not(loom))]
pub use global::{
    begin_fd_park, current_handle, describe_word, drain_wake_fd, end_fd_park, ensure_wake_pipe,
    now_ms, park, park_timeout, recheck_cadence_ms, reissue_current_token, sleep, unpark,
    unpark_word, wake_read_fd,
};

// ===========================================================================
// Loom-world global layer (LATCH-LOOM lane): the minimal analog of `global`
// that makes latch (and any waiter consumer) loom-BUILDABLE, so loom models
// drive the REAL wait/set paths instead of mirrors. Slots live in a
// per-iteration slab (`pgsync::process_global!` — loom's lazy_static resets
// every `loom::model` iteration); the current thread's slot rides
// loom::thread_local (also per-iteration). No fd-park, no cadence, no time:
// * cadence is DISABLED (parks prove the primitive, not the recheck
//   backstop — the waiter/tests/loom.rs convention);
// * the clock reads 0 forever and waits are untimed condvar waits, so a
//   timed park blocks until a real unpark — a model must always deliver the
//   wake it is proving (pgsync papering row L4: no model depends on real
//   time passing); `sleep` is yield_now for the same reason.
// ===========================================================================

#[cfg(loom)]
mod model_global {
    use super::*;
    use std::cell::Cell;

    /// Model slab bound: loom::MAX_THREADS is 4, one slot per thread.
    pub const MODEL_WAITER_CAP: usize = 4;

    pgsync::process_global! {
        static SLOTS: [Slot; MODEL_WAITER_CAP] =
            model_slot_slab();
        static SLOT_NEXT: sync::atomic::AtomicUsize = sync::atomic::AtomicUsize::new(0);
    }

    fn model_slot_slab() -> [Slot; MODEL_WAITER_CAP] {
        core::array::from_fn(|_| Slot::new_for_model())
    }

    pgsync::__loom::thread_local! {
        static CURRENT: Cell<Option<usize>> = Cell::new(None);
    }

    struct LoomClock;

    impl clock::WaiterClock for LoomClock {
        fn now_ms(&self) -> i64 {
            0
        }
        fn wait<'a>(
            &self,
            slot: &'a Slot,
            guard: MutexGuard<'a, SlotInner>,
            _timeout_ms: Option<i64>,
        ) -> (MutexGuard<'a, SlotInner>, bool) {
            (slot.wait_for_model(guard), false)
        }
    }

    // Zero-sized and stateless: a plain static holds no loom types.
    static CLOCK: LoomClock = LoomClock;

    fn current_slot() -> usize {
        CURRENT.with(|c| {
            if let Some(idx) = c.get() {
                return idx;
            }
            let idx = SLOT_NEXT.fetch_add(1, sync::atomic::Ordering::Relaxed);
            assert!(idx < MODEL_WAITER_CAP, "model waiter slab exhausted");
            SLOTS[idx].issue_token();
            c.set(Some(idx));
            idx
        })
    }

    /// See `global::current_handle` — publish BEFORE the final predicate
    /// re-check that precedes the park (the Dekker discipline of the latch).
    pub fn current_handle() -> WakerHandle {
        let idx = current_slot();
        let token = SLOTS[idx].lock().token;
        WakerHandle::new(idx, token)
    }

    /// Park until unparked. Cadence disabled: the models prove the wake
    /// protocol, never the debug backstop.
    pub fn park() -> ParkResult {
        let idx = current_slot();
        SLOTS[idx].park_core(None, None, &CLOCK)
    }

    /// Timed park under a clock with no time: blocks until a real unpark
    /// (the deadline never advances). Models must deliver the wake.
    pub fn park_timeout(timeout: core::time::Duration) -> ParkResult {
        let _ = timeout;
        let idx = current_slot();
        SLOTS[idx].park_core(None, None, &CLOCK)
    }

    /// Papering row L4 semantics: no model may depend on real time passing.
    pub fn sleep(_timeout: core::time::Duration) {
        pgsync::thread::yield_now();
    }

    /// Cadence is DISABLED under loom (models prove the wake protocol, never
    /// the recheck backstop) — loom-buildable consumers (waiteventset's
    /// fd-park lap) read 0 = off.
    pub fn recheck_cadence_ms() -> i64 {
        0
    }

    /// Monotonic ms of the model clock (constant 0 — deadline math is inert
    /// under loom; see the module comment).
    pub fn now_ms() -> i64 {
        0
    }

    pub fn unpark(handle: WakerHandle) -> Unparked {
        let (idx, token) = handle.unpack();
        if idx >= MODEL_WAITER_CAP {
            return Unparked::Stale;
        }
        SLOTS[idx].unpark_token(token)
    }

    pub fn unpark_word(word: u64) -> Unparked {
        match WakerHandle::from_u64(word) {
            Some(h) => unpark(h),
            None => Unparked::Stale,
        }
    }

    /// Pooled-thread reuse boundary, real semantics over the model slab
    /// (outstanding handles go Stale).
    pub fn reissue_current_token() {
        let idx = current_slot();
        SLOTS[idx].reissue_token();
    }

    // -- fd-park surface (waiteventset consumers must COMPILE under loom;
    //    models never drive epoll paths — wasm-precedent stubs + real mode
    //    bookkeeping where the slot core carries it) -----------------------

    /// No pipe(2) in a loom model (wasm precedent: fd-park is unavailable;
    /// callers see ENOSYS).
    pub fn ensure_wake_pipe() -> Result<i32, i32> {
        Err(libc::ENOSYS)
    }

    /// A model has no wake pipe to hand out — reaching this is a model bug.
    pub fn wake_read_fd() -> i32 {
        panic!("waiter::wake_read_fd: no wake pipe exists under loom models")
    }

    /// Mode bookkeeping identical to the global layer's (the slot core owns
    /// the state machine); there is no pipe to register.
    pub fn begin_fd_park() -> bool {
        let idx = current_slot();
        let mut g = SLOTS[idx].lock();
        match g.mode {
            Mode::Notified => {
                g.mode = Mode::Idle;
                false
            }
            Mode::Idle => {
                g.mode = Mode::ParkedFd;
                true
            }
            m => panic!("begin_fd_park in mode {m:?}"),
        }
    }

    pub fn end_fd_park() {
        let idx = current_slot();
        let mut g = SLOTS[idx].lock();
        match g.mode {
            Mode::ParkedFd | Mode::Notified => g.mode = Mode::Idle,
            m => panic!("end_fd_park in mode {m:?}"),
        }
    }

    /// Nothing can be queued on a pipe that does not exist.
    pub fn drain_wake_fd() -> Result<(), i32> {
        Ok(())
    }

    /// Stall diagnostics over the model slab (same routing states as the
    /// global layer, minus fd modes' pipe detail).
    pub fn describe_word(word: u64) -> String {
        let Some(h) = WakerHandle::from_u64(word) else {
            return "MISSING".into();
        };
        let (idx, token) = h.unpack();
        if idx >= MODEL_WAITER_CAP {
            return format!("INVALID(slot:{idx})");
        }
        let g = SLOTS[idx].lock();
        if g.token != token || g.mode == Mode::Free {
            return format!("STALE(slot:{idx} tok:{token} cur:{})", g.token);
        }
        format!("waiter:slot={idx} tok={token} state={:?}", g.mode)
    }
}

#[cfg(loom)]
pub use model_global::{
    begin_fd_park, current_handle, describe_word, drain_wake_fd, end_fd_park, ensure_wake_pipe,
    now_ms, park, park_timeout, recheck_cadence_ms, reissue_current_token, sleep, unpark,
    unpark_word, wake_read_fd,
};

#[cfg(all(test, not(loom)))]
mod tests;
