use std::cell::Cell;
#[cfg(not(target_family = "wasm"))]
use std::mem::MaybeUninit;

use core::ffi::c_int;
#[cfg(not(target_family = "wasm"))]
use libc::{
    SIGABRT, SIGALRM, SIGBUS, SIGCONT, SIGFPE, SIGILL, SIGQUIT, SIGSEGV, SIGSYS, SIGTERM, SIGTRAP,
};
#[cfg(not(target_family = "wasm"))]
pub use libc::sigset_t;

// wasm32: WASI p1 has no signals and its libc crate exposes neither sigset_t
// nor SIG* numbers. The mask bookkeeping is kept (callers read the sets);
// sigset_t is a 64-entry bitmask and the numbers are the Linux/wasi-libc
// values. install_mask is a no-op — there is no kernel mask to install.
#[cfg(target_family = "wasm")]
pub type sigset_t = u64;
#[cfg(target_family = "wasm")]
mod wasm_signums {
    use core::ffi::c_int;
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
    pub const SIGQUIT: c_int = 3;
}
#[cfg(target_family = "wasm")]
use wasm_signums::*;

pub fn init_seams() {}

const NEVER_BLOCK_SIGNALS: [c_int; 8] = [
    SIGTRAP, SIGABRT, SIGILL, SIGFPE, SIGSEGV, SIGBUS, SIGSYS, SIGCONT,
];

const STARTUP_UNBLOCKED_SIGNALS: [c_int; 3] = [SIGQUIT, SIGTERM, SIGALRM];

#[derive(Clone, Copy)]
pub struct SignalMasks {
    unblock_sig: sigset_t,
    block_sig: sigset_t,
    startup_block_sig: sigset_t,
}

impl SignalMasks {
    pub fn new() -> Self {
        let unblock_sig = empty_signal_set();
        let mut block_sig = full_signal_set();
        let mut startup_block_sig = full_signal_set();

        for signal in NEVER_BLOCK_SIGNALS {
            delete_signal(&mut block_sig, signal);
            delete_signal(&mut startup_block_sig, signal);
        }
        for signal in STARTUP_UNBLOCKED_SIGNALS {
            delete_signal(&mut startup_block_sig, signal);
        }

        Self { unblock_sig, block_sig, startup_block_sig }
    }

    pub fn unblock_sig(&self) -> &sigset_t {
        &self.unblock_sig
    }

    pub fn block_sig(&self) -> &sigset_t {
        &self.block_sig
    }

    pub fn startup_block_sig(&self) -> &sigset_t {
        &self.startup_block_sig
    }

    pub fn block_sig_contains(&self, signal: c_int) -> bool {
        signal_set_contains(&self.block_sig, signal)
    }

    pub fn startup_block_sig_contains(&self, signal: c_int) -> bool {
        signal_set_contains(&self.startup_block_sig, signal)
    }

    pub fn unblock_sig_contains(&self, signal: c_int) -> bool {
        signal_set_contains(&self.unblock_sig, signal)
    }
}

impl Default for SignalMasks {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    // BlockSig/UnBlockSig/StartupBlockSig: backend-private C globals, one
    // snapshot per backend thread; mutators mirror callers' sigaddset/sigdelset.
    static MASKS: Cell<SignalMasks> = Cell::new(SignalMasks::new());
}

pub fn pqinitmask() {
    MASKS.set(SignalMasks::new());
}

pub fn signal_masks() -> SignalMasks {
    MASKS.get()
}

pub fn block_signals() {
    install_mask(&MASKS.get().block_sig);
}

pub fn unblock_signals() {
    install_mask(&MASKS.get().unblock_sig);
}

pub fn block_startup_signals() {
    install_mask(&MASKS.get().startup_block_sig);
}

pub fn block_sig_add(signal: c_int) {
    let mut masks = MASKS.get();
    add_signal(&mut masks.block_sig, signal);
    MASKS.set(masks);
}

pub fn block_sig_delete(signal: c_int) {
    let mut masks = MASKS.get();
    delete_signal(&mut masks.block_sig, signal);
    MASKS.set(masks);
}

pub fn unblock_sig_add(signal: c_int) {
    let mut masks = MASKS.get();
    add_signal(&mut masks.unblock_sig, signal);
    MASKS.set(masks);
}

#[cfg(not(target_family = "wasm"))]
fn install_mask(set: &sigset_t) {
    // Must be pthread_sigmask: Darwin sigprocmask from a secondary thread
    // blocks process-directed delivery process-wide (a backend thread's
    // block_signals() ate the postmaster's SIGINT).
    // SAFETY: `set` is initialized; a null oldset is only read.
    unsafe {
        libc::pthread_sigmask(libc::SIG_SETMASK, set, core::ptr::null_mut());
    }
}

// wasm32: no kernel signal mask exists; the bookkeeping sets above are the
// only state.
#[cfg(target_family = "wasm")]
fn install_mask(_set: &sigset_t) {}

#[cfg(not(target_family = "wasm"))]
fn empty_signal_set() -> sigset_t {
    let mut set = MaybeUninit::<sigset_t>::uninit();
    // SAFETY: sigemptyset fully initializes the set before assume_init.
    let rc = unsafe { libc::sigemptyset(set.as_mut_ptr()) };
    debug_assert_eq!(rc, 0);
    unsafe { set.assume_init() }
}

#[cfg(target_family = "wasm")]
fn empty_signal_set() -> sigset_t {
    0
}

#[cfg(not(target_family = "wasm"))]
fn full_signal_set() -> sigset_t {
    let mut set = MaybeUninit::<sigset_t>::uninit();
    // SAFETY: sigfillset fully initializes the set before assume_init.
    let rc = unsafe { libc::sigfillset(set.as_mut_ptr()) };
    debug_assert_eq!(rc, 0);
    unsafe { set.assume_init() }
}

#[cfg(target_family = "wasm")]
fn full_signal_set() -> sigset_t {
    !0
}

#[cfg(not(target_family = "wasm"))]
fn add_signal(set: &mut sigset_t, signal: c_int) {
    // SAFETY: `set` is a valid, initialized sigset_t.
    let rc = unsafe { libc::sigaddset(set, signal) };
    debug_assert_eq!(rc, 0);
}

#[cfg(target_family = "wasm")]
fn add_signal(set: &mut sigset_t, signal: c_int) {
    *set |= 1u64 << (signal as u32 & 63);
}

#[cfg(not(target_family = "wasm"))]
fn delete_signal(set: &mut sigset_t, signal: c_int) {
    // SAFETY: `set` is a valid, initialized sigset_t.
    let rc = unsafe { libc::sigdelset(set, signal) };
    debug_assert_eq!(rc, 0);
}

#[cfg(target_family = "wasm")]
fn delete_signal(set: &mut sigset_t, signal: c_int) {
    *set &= !(1u64 << (signal as u32 & 63));
}

#[cfg(not(target_family = "wasm"))]
fn signal_set_contains(set: &sigset_t, signal: c_int) -> bool {
    // SAFETY: `set` is a valid, initialized sigset_t.
    unsafe { libc::sigismember(set, signal) == 1 }
}

#[cfg(target_family = "wasm")]
fn signal_set_contains(set: &sigset_t, signal: c_int) -> bool {
    *set & (1u64 << (signal as u32 & 63)) != 0
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
        for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGALRM, libc::SIGQUIT, libc::SIGHUP] {
            assert!(masks.block_sig_contains(signal), "signal {signal}");
        }
    }

    #[test]
    fn never_block_signals_excluded_from_both() {
        let masks = SignalMasks::new();
        for signal in NEVER_BLOCK_SIGNALS {
            assert!(!masks.block_sig_contains(signal), "signal {signal}");
            assert!(!masks.startup_block_sig_contains(signal), "signal {signal}");
        }
    }

    #[test]
    fn startup_block_sig_unblocks_startup_signals_only() {
        let masks = SignalMasks::new();
        for signal in STARTUP_UNBLOCKED_SIGNALS {
            assert!(!masks.startup_block_sig_contains(signal), "signal {signal}");
        }
        assert!(masks.startup_block_sig_contains(libc::SIGINT));
        assert!(masks.startup_block_sig_contains(libc::SIGHUP));
    }

    #[test]
    fn pqinitmask_resets_thread_local_snapshot() {
        pqinitmask();
        block_sig_delete(libc::SIGQUIT);
        assert!(!signal_masks().block_sig_contains(libc::SIGQUIT));
        block_sig_add(libc::SIGQUIT);
        assert!(signal_masks().block_sig_contains(libc::SIGQUIT));
        unblock_sig_add(libc::SIGURG);
        assert!(signal_masks().unblock_sig_contains(libc::SIGURG));
        pqinitmask();
        let masks = signal_masks();
        assert!(masks.block_sig_contains(libc::SIGTERM));
        assert!(!masks.startup_block_sig_contains(libc::SIGTERM));
        assert!(!masks.unblock_sig_contains(libc::SIGURG));
    }
}
