//! Loom/std synchronization — now a FACADE over `pgsync`, THE single lock
//! library (permit-s1; docs/design/permit-scheduler.md §2). This module was
//! the repo's first loom pattern; its bodies (Semaphore/IoGuard/ParkLot/
//! `lock`) moved verbatim into `pgsync` and its rules carry there as crate
//! law (Arc stays std everywhere; no statics hold loom types; poison-
//! tolerant `lock()` discipline).
//!
//! The module is KEPT so intra-crate `crate::sync::…` paths and the loom
//! model surface survive unchanged (loom-breadth reconciliation relies on
//! this path staying alive). Call sites never see a world cfg — pgsync is
//! the only cfg site.

pub(crate) use pgsync::{atomic, lock, Condvar, Mutex, MutexGuard, Once, OnceLock, ParkLot};
// Re-exported pub from lib.rs (`pub use sync::{IoGuard, Semaphore}`), so the
// facade rows must themselves be pub (E0365 otherwise).
pub use pgsync::{IoGuard, Semaphore};
