//! Loom world (`--cfg loom`): loom's checked types, with std API deltas
//! papered HERE (crate doc "loom papering table"; call sites never gain a
//! cfg). Per-crate bounded models keep driving these types exactly as they
//! drove the old `runtime::sync` / waiter `mod sync` shims.

pub use loom::sync::atomic;
pub use loom::sync::{
    Barrier, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    WaitTimeoutResult,
};

// loom re-uses std's result/poison vocabulary (loom::sync does the same).
pub use std::sync::{BarrierWaitResult, LockResult, PoisonError, TryLockError, TryLockResult};

// Papering row L3: loom has no Once/OnceLock. std-backed under loom — the
// sanctioned OnceLock-mediated-global-init pattern must keep working in
// statics ("no statics hold loom types" law), and no current model races a
// lazy init. The `once` lint arm keeps the class enumerated; build a checked
// composite if and when a model needs one.
pub use std::sync::{Once, OnceLock, OnceState};

/// Papering row L6: loom `mpsc` provides `channel` only; `sync_channel` is
/// deliberately absent under loom until a model needs it.
pub mod mpsc {
    pub use loom::sync::mpsc::{channel, Receiver, Sender};
    pub use std::sync::mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError};
}

pub mod thread {
    pub use loom::thread::{
        current, panicking, park, spawn, yield_now, Builder, JoinHandle, Thread, ThreadId,
    };

    /// Papering row L4: loom has no `sleep`; loomfast practice is yield_now
    /// semantics (a model may not depend on real time passing).
    pub fn sleep(_dur: core::time::Duration) {
        loom::thread::yield_now();
    }

    // Papering row L5: `scope` deliberately absent under loom (no model
    // needs it; a spawn+join shim is the recorded fallback if one does).
}
