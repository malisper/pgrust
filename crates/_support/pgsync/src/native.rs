//! Native world: pure `pub use` re-exports of std — no wrapper types, zero
//! cost, provably IDENTICAL types (G1: the pub-use world is free; one asm
//! rider, once — not a letter per train).

pub use std::sync::{
    Barrier, BarrierWaitResult, Condvar, LockResult, Mutex, MutexGuard, Once, OnceLock, OnceState,
    PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError, TryLockResult,
    WaitTimeoutResult,
};

pub use std::sync::atomic;
pub use std::sync::mpsc;

/// Full std thread surface (spawn/scope/sleep/park/join/Builder/…). The
/// `spawn` lint category and the launch_backend spawn door keep governing
/// thread creation — pgsync re-exports the types, it does not sanction raw
/// spawns.
pub mod thread {
    pub use std::thread::*;
}
