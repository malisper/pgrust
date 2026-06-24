//! Port of `src/backend/libpq/pqsignal.c` — backend signal mask setup.
//!
//! PostgreSQL keeps three process-global `sigset_t` values used to block and
//! unblock signals around critical sections, and `pqinitmask()` initializes
//! them. These are backend-private globals (not shared memory), so the port
//! keeps them as an owned [`SignalMasks`] value plus a per-backend
//! `thread_local!` snapshot (one backend per thread; no cross-thread
//! sharing).
//!
//! Scope note: `pqsignal_be`/`pqsignal_fe` (reliable-signal installers) live
//! in `src/port/pqsignal.c`, a *different* C unit; the legacy libpq
//! `pqsignal()` lives in `src/interfaces/libpq/legacy-pqsignal.c` (the
//! `interfaces-libpq-legacy-pqsignal` crate). Neither is part of this crate.

use std::cell::Cell;
#[cfg(not(target_family = "wasm"))]
use std::mem::MaybeUninit;

// ---------------------------------------------------------------------------
// The `sigset_t` representation and the low-level set primitives.
//
// Native: real `libc::sigset_t` manipulated via `sig{empty,fill,add,del,ismember}set`.
//
// wasm (single-process, wasip1): `libc` exposes neither `sigset_t` nor any of
// the `sig*set` helpers nor `sigprocmask`/`SIG*`. wasm never delivers a signal,
// so the kernel-mask installs are no-ops, but the `SignalMasks` membership API
// is still consumed by callers (via seams and directly), so we keep it fully
// functional by representing the set as a `u64` bitmask (bit `n` = signal `n`)
// and the SIG* numbers as their standard Linux values.
// ---------------------------------------------------------------------------

#[cfg(not(target_family = "wasm"))]
pub use libc::sigset_t;

#[cfg(target_family = "wasm")]
/// wasm stand-in for `libc::sigset_t`: a `u64` bitmask, bit `n` ⇒ signal `n`.
/// Signal numbers used by this crate are all < 64, so a single `u64` suffices.
pub type sigset_t = u64;

// SIG* constants. Native uses libc's; wasm uses the standard Linux numbers
// (the only ones this crate references), so the masks behave identically.
#[cfg(target_family = "wasm")]
mod sig {
    use core::ffi::c_int;
    pub const SIGHUP: c_int = 1;
    pub const SIGINT: c_int = 2;
    pub const SIGQUIT: c_int = 3;
    pub const SIGILL: c_int = 4;
    pub const SIGTRAP: c_int = 5;
    pub const SIGABRT: c_int = 6;
    pub const SIGBUS: c_int = 7;
    pub const SIGFPE: c_int = 8;
    pub const SIGSEGV: c_int = 11;
    pub const SIGALRM: c_int = 14;
    pub const SIGTERM: c_int = 15;
    pub const SIGCONT: c_int = 18;
    pub const SIGSYS: c_int = 31;
}

#[cfg(not(target_family = "wasm"))]
use libc::{
    SIGABRT, SIGALRM, SIGBUS, SIGCONT, SIGFPE, SIGILL, SIGQUIT, SIGSEGV, SIGSYS, SIGTERM, SIGTRAP,
};
#[cfg(target_family = "wasm")]
use sig::{
    SIGABRT, SIGALRM, SIGBUS, SIGCONT, SIGFPE, SIGILL, SIGQUIT, SIGSEGV, SIGSYS, SIGTERM, SIGTRAP,
};

/// Install this crate's seams: the `sigprocmask` mask-install primitives that
/// operate over the masks this crate owns.
pub fn init_seams() {
    libpq_pqsignal_seams::block_signals::set(block_signals);
    libpq_pqsignal_seams::unblock_signals::set(unblock_signals);
    libpq_pqsignal_seams::add_unblock_sig::set(unblock_sig_add);
}

/// `sigprocmask(SIG_SETMASK, &BlockSig, NULL)` — block all signals.
#[cfg(not(target_family = "wasm"))]
fn block_signals() {
    let masks = signal_masks();
    // SAFETY: `block_sig()` points to a valid, initialized sigset_t.
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, masks.block_sig(), core::ptr::null_mut());
    }
}

/// `sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)` — restore the normal mask.
#[cfg(not(target_family = "wasm"))]
fn unblock_signals() {
    let masks = signal_masks();
    // SAFETY: `unblock_sig()` points to a valid, initialized sigset_t.
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), core::ptr::null_mut());
    }
}

/// wasm: no kernel signal mask to install — single-process never blocks signals.
#[cfg(target_family = "wasm")]
fn block_signals() {}

/// wasm: no kernel signal mask to install.
#[cfg(target_family = "wasm")]
fn unblock_signals() {}

/// The three backend signal masks initialized by [`pqinitmask`].
///
/// - `block_sig` — the set of signals to block when we are trying to block
///   signals. Includes all signals we normally expect to get, but NOT signals
///   that should never be turned off (SIGTRAP, SIGABRT, SIGILL, SIGFPE,
///   SIGSEGV, SIGBUS, SIGSYS, SIGCONT).
/// - `startup_block_sig` — essentially `block_sig` minus SIGTERM, SIGQUIT,
///   SIGALRM; used during startup packet collection.
/// - `unblock_sig` — empty; the set to install when we don't want to block
///   signals. (Note: in C, `InitializeWaitEventSupport()` modifies
///   `UnBlockSig` afterwards.)
#[derive(Clone, Copy, Debug)]
pub struct SignalMasks {
    unblock_sig: sigset_t,
    block_sig: sigset_t,
    startup_block_sig: sigset_t,
}

/// Signals that should never be blocked (`BlockSig`/`StartupBlockSig` clear
/// these). Mirrors the `#ifdef SIG*` deletions in `pqinitmask`; every one of
/// these names exists on the platforms we build for.
const NEVER_BLOCK_SIGNALS: &[core::ffi::c_int] = &[
    SIGTRAP, SIGABRT, SIGILL, SIGFPE, SIGSEGV, SIGBUS, SIGSYS, SIGCONT,
];

/// Signals unique to startup — additionally cleared only from
/// `StartupBlockSig`.
const STARTUP_UNBLOCKED_SIGNALS: &[core::ffi::c_int] = &[SIGQUIT, SIGTERM, SIGALRM];

impl SignalMasks {
    /// Builds the three masks: `UnBlockSig = sigemptyset`;
    /// `BlockSig = StartupBlockSig = sigfillset`, then clear the never-block
    /// signals from both, and the startup-unique signals from
    /// `StartupBlockSig`.
    pub fn new() -> Self {
        let unblock_sig = empty_signal_set();
        let mut block_sig = full_signal_set();
        let mut startup_block_sig = full_signal_set();

        for &signal in NEVER_BLOCK_SIGNALS {
            delete_signal(&mut block_sig, signal);
            delete_signal(&mut startup_block_sig, signal);
        }

        for &signal in STARTUP_UNBLOCKED_SIGNALS {
            delete_signal(&mut startup_block_sig, signal);
        }

        Self {
            unblock_sig,
            block_sig,
            startup_block_sig,
        }
    }

    /// `UnBlockSig`.
    pub fn unblock_sig(&self) -> &sigset_t {
        &self.unblock_sig
    }

    /// `BlockSig`.
    pub fn block_sig(&self) -> &sigset_t {
        &self.block_sig
    }

    /// `StartupBlockSig`.
    pub fn startup_block_sig(&self) -> &sigset_t {
        &self.startup_block_sig
    }

    /// True iff `signal` is a member of `BlockSig`.
    pub fn block_sig_contains(&self, signal: core::ffi::c_int) -> bool {
        signal_set_contains(&self.block_sig, signal)
    }

    /// True iff `signal` is a member of `StartupBlockSig`.
    pub fn startup_block_sig_contains(&self, signal: core::ffi::c_int) -> bool {
        signal_set_contains(&self.startup_block_sig, signal)
    }

    /// True iff `signal` is a member of `UnBlockSig`.
    pub fn unblock_sig_contains(&self, signal: core::ffi::c_int) -> bool {
        signal_set_contains(&self.unblock_sig, signal)
    }
}

impl Default for SignalMasks {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    /// Per-backend snapshot of the masks, standing in for the C globals
    /// `UnBlockSig`/`BlockSig`/`StartupBlockSig` (backend-private memory in
    /// C, so one copy per backend thread here). Initialized lazily and
    /// rewritten by [`pqinitmask`] (single-set-at-startup in C).
    static MASKS: Cell<SignalMasks> = Cell::new(SignalMasks::new());
}

/// Initialize `BlockSig`, `UnBlockSig`, and `StartupBlockSig` for the calling
/// backend (void, like the C `pqinitmask()`).
pub fn pqinitmask() {
    MASKS.set(SignalMasks::new());
}

/// Reads the calling backend's masks snapshot (the analog of referencing the
/// `BlockSig`/`UnBlockSig`/`StartupBlockSig` globals after `pqinitmask`).
pub fn signal_masks() -> SignalMasks {
    MASKS.get()
}

/// `sigprocmask(SIG_SETMASK, &BlockSig, NULL)` — install the current `BlockSig`
/// as the process signal mask. The public, void-returning analog of the C
/// statement `sigprocmask(SIG_SETMASK, &BlockSig, NULL)` over the globals this
/// crate owns. Used by the `InitPostmasterChild`/`InitStandaloneProcess`
/// startup mask installs (miscinit.c).
pub fn set_block_sig_mask() {
    block_signals();
}

/// `sigdelset(&BlockSig, signal)` — persistently remove `signal` from the
/// owned `BlockSig` snapshot (mutating the C global `BlockSig`, mirroring
/// miscinit.c's `sigdelset(&BlockSig, SIGQUIT)`). The change sticks for every
/// later `set_block_sig_mask()` / [`block_signals`] install, matching the C
/// semantics where `sigdelset` edits the persistent global.
pub fn block_sig_delete(signal: core::ffi::c_int) {
    let mut masks = MASKS.get();
    delete_signal(&mut masks.block_sig, signal);
    MASKS.set(masks);
}

/// `sigaddset(&UnBlockSig, signal)` — persistently add `signal` to the owned
/// `UnBlockSig` snapshot (mutating the C global `UnBlockSig`, mirroring
/// waiteventset.c's `sigaddset(&UnBlockSig, SIGURG)` on the signalfd build).
pub fn unblock_sig_add(signal: core::ffi::c_int) {
    let mut masks = MASKS.get();
    add_signal(&mut masks.unblock_sig, signal);
    MASKS.set(masks);
}

/// `sigaddset(&BlockSig, signal)` — persistently add `signal` to the owned
/// `BlockSig` snapshot (mutating the C global `BlockSig`, mirroring
/// `quickdie`'s `sigaddset(&BlockSig, SIGQUIT)` which prevents nested SIGQUIT
/// handler invocations).
pub fn block_sig_add(signal: core::ffi::c_int) {
    let mut masks = MASKS.get();
    add_signal(&mut masks.block_sig, signal);
    MASKS.set(masks);
}

// ---- low-level set primitives (native: libc; wasm: u64 bitmask) ----

#[cfg(not(target_family = "wasm"))]
fn empty_signal_set() -> sigset_t {
    let mut set = MaybeUninit::<sigset_t>::uninit();
    // SAFETY: `sigemptyset` initializes the whole set; we assume_init after.
    let rc = unsafe { libc::sigemptyset(set.as_mut_ptr()) };
    debug_assert_eq!(rc, 0);
    unsafe { set.assume_init() }
}

#[cfg(not(target_family = "wasm"))]
fn full_signal_set() -> sigset_t {
    let mut set = MaybeUninit::<sigset_t>::uninit();
    // SAFETY: `sigfillset` initializes the whole set; we assume_init after.
    let rc = unsafe { libc::sigfillset(set.as_mut_ptr()) };
    debug_assert_eq!(rc, 0);
    unsafe { set.assume_init() }
}

#[cfg(not(target_family = "wasm"))]
fn delete_signal(set: &mut sigset_t, signal: core::ffi::c_int) {
    // SAFETY: `set` is a valid, initialized sigset_t.
    let rc = unsafe { libc::sigdelset(set, signal) };
    debug_assert_eq!(rc, 0);
}

#[cfg(not(target_family = "wasm"))]
fn add_signal(set: &mut sigset_t, signal: core::ffi::c_int) {
    // SAFETY: `set` is a valid, initialized sigset_t.
    let rc = unsafe { libc::sigaddset(set as *mut sigset_t, signal) };
    debug_assert_eq!(rc, 0);
}

#[cfg(not(target_family = "wasm"))]
fn signal_set_contains(set: &sigset_t, signal: core::ffi::c_int) -> bool {
    // SAFETY: `set` is a valid, initialized sigset_t.
    unsafe { libc::sigismember(set, signal) == 1 }
}

// wasm: the set is a `u64` bitmask, bit `n` ⇒ signal `n`. All signal numbers
// this crate uses are < 64, so a single `u64` is exact. `sigfillset` sets every
// bit; the membership/add/delete ops mirror the libc semantics.
#[cfg(target_family = "wasm")]
fn empty_signal_set() -> sigset_t {
    0
}

#[cfg(target_family = "wasm")]
fn full_signal_set() -> sigset_t {
    u64::MAX
}

#[cfg(target_family = "wasm")]
fn signal_bit(signal: core::ffi::c_int) -> u64 {
    debug_assert!(signal > 0 && (signal as u32) < 64);
    1u64 << (signal as u32)
}

#[cfg(target_family = "wasm")]
fn delete_signal(set: &mut sigset_t, signal: core::ffi::c_int) {
    *set &= !signal_bit(signal);
}

#[cfg(target_family = "wasm")]
fn add_signal(set: &mut sigset_t, signal: core::ffi::c_int) {
    *set |= signal_bit(signal);
}

#[cfg(target_family = "wasm")]
fn signal_set_contains(set: &sigset_t, signal: core::ffi::c_int) -> bool {
    *set & signal_bit(signal) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unblock_sig_starts_empty() {
        let masks = SignalMasks::new();
        assert!(!masks.unblock_sig_contains(libc::SIGTERM));
        assert!(!masks.unblock_sig_contains(libc::SIGINT));
        assert!(!masks.unblock_sig_contains(libc::SIGQUIT));
    }

    #[test]
    fn block_sig_blocks_normal_signals() {
        let masks = SignalMasks::new();
        assert!(masks.block_sig_contains(libc::SIGTERM));
        assert!(masks.block_sig_contains(libc::SIGINT));
        assert!(masks.block_sig_contains(libc::SIGALRM));
        assert!(masks.block_sig_contains(libc::SIGQUIT));
        assert!(masks.block_sig_contains(libc::SIGHUP));
    }

    #[test]
    fn block_sig_excludes_never_block_signals() {
        let masks = SignalMasks::new();
        for &signal in NEVER_BLOCK_SIGNALS {
            assert!(!masks.block_sig_contains(signal), "signal {signal}");
            assert!(
                !masks.startup_block_sig_contains(signal),
                "signal {signal}"
            );
        }
    }

    #[test]
    fn startup_block_sig_excludes_startup_specific_signals() {
        let masks = SignalMasks::new();
        for &signal in STARTUP_UNBLOCKED_SIGNALS {
            assert!(
                !masks.startup_block_sig_contains(signal),
                "signal {signal}"
            );
        }
    }

    #[test]
    fn startup_block_sig_still_blocks_other_normal_signals() {
        let masks = SignalMasks::new();
        assert!(masks.startup_block_sig_contains(libc::SIGINT));
        assert!(masks.startup_block_sig_contains(libc::SIGHUP));
    }

    #[test]
    fn pqinitmask_updates_thread_local_snapshot() {
        pqinitmask();
        let masks = signal_masks();
        // SIGTERM is startup-unblocked but block-blocked.
        assert!(masks.block_sig_contains(libc::SIGTERM));
        assert!(!masks.startup_block_sig_contains(libc::SIGTERM));
        assert!(!masks.unblock_sig_contains(libc::SIGTERM));
    }
}
