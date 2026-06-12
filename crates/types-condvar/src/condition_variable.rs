//! The `ConditionVariable` data shape (`storage/condition_variable.h`).

use std::sync::{Condvar, Mutex};

/// `ConditionVariable` (`storage/condition_variable.h`).
///
/// C: `{ slock_t mutex; proclist_head wakeup; }`. In the threaded-backend
/// model the substrate is a host mutex/condvar pair; the sleep/broadcast
/// protocol over it is owned by `storage/lmgr/condition_variable.c`'s crate.
#[derive(Debug, Default)]
pub struct ConditionVariable {
    /// `mutex` — guards the wakeup state.
    pub mutex: Mutex<()>,
    /// The wakeup channel (`wakeup` in C, where waiters parked on their
    /// latches; here the host condition variable they block on).
    pub condvar: Condvar,
}

impl ConditionVariable {
    /// `ConditionVariableInit(cv)` — C zero/empty-initializes the struct in
    /// place; here construction is initialization.
    pub fn new() -> Self {
        Self::default()
    }
}
