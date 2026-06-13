//! `ConditionVariable` (`storage/condition_variable.h`), the shmem-resident
//! wait/broadcast primitive embedded in shared structures (ProcSignal slots,
//! PGPROC arrays, ...).
//!
//! Only the data shape lives here, so embedding structs can construct slots
//! without depending on the protocol owner. The sleep/signal/broadcast
//! protocol (`storage/lmgr/condition_variable.c`) belongs to the
//! `backend-storage-lmgr-condition-variable` crate and is reached across
//! dependency cycles through `backend-storage-lmgr-condition-variable-seams`.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod condition_variable;

pub use condition_variable::{ConditionVariable, ConditionVariableMinimallyPadded, CV_MINIMAL_SIZE};
