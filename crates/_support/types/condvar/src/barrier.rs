//! `Barrier` (`storage/barrier.h`) — the dynamic-party phased barrier used to
//! coordinate parallel-query backends (e.g. the build/probe/grow phases of a
//! Parallel Hash Join). Only the data shape lives here; the attach/arrive/wait
//! protocol (`storage/ipc/barrier.c`) is reached through that owner's seam
//! crate.

use crate::ConditionVariable;
use types_storage::Spinlock;

/// `Barrier` (`storage/barrier.h`) — a shmem-resident phased barrier. Embedded
/// directly in DSM structs, so it carries a real spinlock and condition
/// variable and is neither `Copy` nor `Clone`.
#[derive(Debug, Default)]
#[repr(C)]
pub struct Barrier {
    /// `slock_t mutex`.
    pub mutex: Spinlock,
    /// `int phase` — phase counter.
    pub phase: i32,
    /// `int participants` — the number of participants attached.
    pub participants: i32,
    /// `int arrived` — the number of participants that have arrived.
    pub arrived: i32,
    /// `int elected` — highest phase elected.
    pub elected: i32,
    /// `bool static_party` — used only for assertions.
    pub static_party: bool,
    /// `ConditionVariable condition_variable`.
    pub condition_variable: ConditionVariable,
}
