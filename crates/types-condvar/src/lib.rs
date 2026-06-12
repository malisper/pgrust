//! `ConditionVariable` (`storage/condition_variable.h`), the shmem-resident
//! wait/broadcast primitive embedded in shared structures (ProcSignal slots,
//! PGPROC arrays, ...).
//!
//! The C struct is `{ slock_t mutex; proclist_head wakeup; }` — a spinlock
//! plus a list of waiting proc numbers, with the actual sleeping done on each
//! waiter's process latch. In this codebase a backend is a thread and shared
//! memory is explicitly shared, synchronized state (AGENTS.md
//! "Backend-global state"), so the embedded primitive is a standard
//! mutex/condvar pair. The protocol functions
//! (`ConditionVariableTimedSleep`, `ConditionVariableBroadcast`,
//! `ConditionVariableCancelSleep` — `storage/lmgr/condition_variable.c`)
//! belong to that unit's crate and are reached through
//! `backend-storage-lmgr-condition-variable-seams`; this crate holds only the
//! data shape, so embedding structs can construct slots before the owner
//! lands.
//!
//! `ConditionVariableInit` in C just initializes the struct in place
//! (`SpinLockInit` + `proclist_init`); here that is [`ConditionVariable::new`].

#![allow(non_snake_case)]

pub mod condition_variable;

pub use condition_variable::ConditionVariable;
