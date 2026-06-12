//! Port of `src/backend/libpq/pqsignal.c` — backend signal mask setup.
//!
//! PostgreSQL keeps three process-global `sigset_t` values used to block and
//! unblock signals around critical sections, and `pqinitmask()` initializes
//! them. These are per-process backend globals (not shared memory), set once
//! during single-threaded startup, so the port keeps them as an owned
//! [`SignalMasks`] value plus a process-global snapshot guarded by a lock.
//!
//! Scope note: `pqsignal_be`/`pqsignal_fe` (reliable-signal installers) live
//! in `src/port/pqsignal.c`, a *different* C unit; the legacy libpq
//! `pqsignal()` lives in `src/interfaces/libpq/legacy-pqsignal.c` (the
//! `interfaces-libpq-legacy-pqsignal` crate). Neither is part of this crate.

use std::mem::MaybeUninit;
use std::sync::{OnceLock, RwLock};

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
    unblock_sig: libc::sigset_t,
    block_sig: libc::sigset_t,
    startup_block_sig: libc::sigset_t,
}

/// Signals that should never be blocked (`BlockSig`/`StartupBlockSig` clear
/// these). Mirrors the `#ifdef SIG*` deletions in `pqinitmask`; every one of
/// these names exists on the platforms we build for.
const NEVER_BLOCK_SIGNALS: &[libc::c_int] = &[
    libc::SIGTRAP,
    libc::SIGABRT,
    libc::SIGILL,
    libc::SIGFPE,
    libc::SIGSEGV,
    libc::SIGBUS,
    libc::SIGSYS,
    libc::SIGCONT,
];

/// Signals unique to startup — additionally cleared only from
/// `StartupBlockSig`.
const STARTUP_UNBLOCKED_SIGNALS: &[libc::c_int] =
    &[libc::SIGQUIT, libc::SIGTERM, libc::SIGALRM];

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
    pub fn unblock_sig(&self) -> &libc::sigset_t {
        &self.unblock_sig
    }

    /// `BlockSig`.
    pub fn block_sig(&self) -> &libc::sigset_t {
        &self.block_sig
    }

    /// `StartupBlockSig`.
    pub fn startup_block_sig(&self) -> &libc::sigset_t {
        &self.startup_block_sig
    }

    /// True iff `signal` is a member of `BlockSig`.
    pub fn block_sig_contains(&self, signal: libc::c_int) -> bool {
        signal_set_contains(&self.block_sig, signal)
    }

    /// True iff `signal` is a member of `StartupBlockSig`.
    pub fn startup_block_sig_contains(&self, signal: libc::c_int) -> bool {
        signal_set_contains(&self.startup_block_sig, signal)
    }

    /// True iff `signal` is a member of `UnBlockSig`.
    pub fn unblock_sig_contains(&self, signal: libc::c_int) -> bool {
        signal_set_contains(&self.unblock_sig, signal)
    }
}

impl Default for SignalMasks {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-global snapshot of the masks, standing in for the C globals
/// `UnBlockSig`/`BlockSig`/`StartupBlockSig`. Initialized lazily and rewritten
/// by [`pqinitmask`] (single-set-at-startup in C).
fn global_masks() -> &'static RwLock<SignalMasks> {
    static MASKS: OnceLock<RwLock<SignalMasks>> = OnceLock::new();
    MASKS.get_or_init(|| RwLock::new(SignalMasks::new()))
}

/// Initialize `BlockSig`, `UnBlockSig`, and `StartupBlockSig`.
///
/// The C version returns void and mutates the globals; this also returns the
/// computed masks for the caller's convenience.
pub fn pqinitmask() -> SignalMasks {
    let masks = SignalMasks::new();
    *global_masks().write().expect("signal masks lock poisoned") = masks;
    masks
}

/// Reads the current process-global masks snapshot (the analog of referencing
/// the `BlockSig`/`UnBlockSig`/`StartupBlockSig` globals after `pqinitmask`).
pub fn signal_masks() -> SignalMasks {
    *global_masks().read().expect("signal masks lock poisoned")
}

fn empty_signal_set() -> libc::sigset_t {
    let mut set = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: `sigemptyset` initializes the whole set; we assume_init after.
    let rc = unsafe { libc::sigemptyset(set.as_mut_ptr()) };
    debug_assert_eq!(rc, 0);
    unsafe { set.assume_init() }
}

fn full_signal_set() -> libc::sigset_t {
    let mut set = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: `sigfillset` initializes the whole set; we assume_init after.
    let rc = unsafe { libc::sigfillset(set.as_mut_ptr()) };
    debug_assert_eq!(rc, 0);
    unsafe { set.assume_init() }
}

fn delete_signal(set: &mut libc::sigset_t, signal: libc::c_int) {
    // SAFETY: `set` is a valid, initialized sigset_t.
    let rc = unsafe { libc::sigdelset(set, signal) };
    debug_assert_eq!(rc, 0);
}

fn signal_set_contains(set: &libc::sigset_t, signal: libc::c_int) -> bool {
    // SAFETY: `set` is a valid, initialized sigset_t.
    unsafe { libc::sigismember(set, signal) == 1 }
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
    fn pqinitmask_updates_global_snapshot() {
        let masks = pqinitmask();
        let global = signal_masks();
        assert_eq!(
            masks.block_sig_contains(libc::SIGTERM),
            global.block_sig_contains(libc::SIGTERM)
        );
        assert_eq!(
            masks.startup_block_sig_contains(libc::SIGTERM),
            global.startup_block_sig_contains(libc::SIGTERM)
        );
        // SIGTERM is startup-unblocked but block-blocked.
        assert!(global.block_sig_contains(libc::SIGTERM));
        assert!(!global.startup_block_sig_contains(libc::SIGTERM));
    }
}
