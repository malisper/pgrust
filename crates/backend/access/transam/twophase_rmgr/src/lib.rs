//! `src/backend/access/transam/twophase_rmgr.c` — two-phase-commit resource
//! managers tables.
//!
//! twophase_rmgr.c is a tables-only translation unit: it defines the four
//! 2PC resource-manager callback dispatch tables, each indexed
//! `[0 ..= TWOPHASE_RM_MAX_ID]` over the built-in resource-manager ids
//! (END / Lock / pgstat / MultiXact / PredicateLock). `ProcessRecords`
//! (twophase.c) walks a prepared transaction's record stream and, for each
//! record, invokes `<phase>_callbacks[rmid]` if it is non-`NULL`.
//!
//! Every non-`NULL` slot is a callback owned by another subsystem
//! (lock-manager, pgstat, multixact, predicate-lock). The C file references
//! them by name across the link; here each slot holds the owner seam's `call`
//! function, which panics loudly until the owning crate lands and installs it.

#![allow(non_upper_case_globals)]

use ::types_core::primitive::TransactionId;
use ::types_error::PgResult;

use multixact_seams as multixact;
use lock_seams as lock;
use predicate_seams as predicate;
use stat_seams as pgstat;

// Resource-manager ids (access/twophase_rmgr.h).

/// `TWOPHASE_RM_END_ID` — the END sentinel (no callback; terminates the walk).
pub const TWOPHASE_RM_END_ID: u8 = 0;
/// `TWOPHASE_RM_LOCK_ID` — the lock manager.
pub const TWOPHASE_RM_LOCK_ID: u8 = 1;
/// `TWOPHASE_RM_PGSTAT_ID` — cumulative statistics.
pub const TWOPHASE_RM_PGSTAT_ID: u8 = 2;
/// `TWOPHASE_RM_MULTIXACT_ID` — the MultiXact manager.
pub const TWOPHASE_RM_MULTIXACT_ID: u8 = 3;
/// `TWOPHASE_RM_PREDICATELOCK_ID` — the predicate (SSI) lock manager.
pub const TWOPHASE_RM_PREDICATELOCK_ID: u8 = 4;
/// `TWOPHASE_RM_MAX_ID` (= `TWOPHASE_RM_PREDICATELOCK_ID`).
pub const TWOPHASE_RM_MAX_ID: u8 = TWOPHASE_RM_PREDICATELOCK_ID;

/// Number of entries in each callback table (`TWOPHASE_RM_MAX_ID + 1`).
pub const NUM_TWOPHASE_RM: usize = (TWOPHASE_RM_MAX_ID as usize) + 1;

/// `typedef void (*TwoPhaseCallback)(TransactionId xid, uint16 info,
/// void *recdata, uint32 len)` (twophase_rmgr.h).
///
/// In the owned form the `(recdata, len)` pair collapses to a `&[u8]`, and
/// the return is `PgResult<()>` so a callback that `ereport(ERROR)`s surfaces
/// a typed `Err` rather than `longjmp`-ing.
pub type TwoPhaseCallback = fn(xid: TransactionId, info: u16, recdata: &[u8]) -> PgResult<()>;

/// `twophase_recover_callbacks`:
/// `{ NULL, lock_twophase_recover, NULL, multixact_twophase_recover,
///    predicatelock_twophase_recover }`
pub static twophase_recover_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,                                              // END ID
    Some(lock::lock_twophase_recover::call),           // Lock
    None,                                              // pgstat
    Some(multixact::multixact_twophase_recover::call), // MultiXact
    Some(predicate::predicatelock_twophase_recover::call), // PredicateLock
];

/// `twophase_postcommit_callbacks`:
/// `{ NULL, lock_twophase_postcommit, pgstat_twophase_postcommit,
///    multixact_twophase_postcommit, NULL }`
pub static twophase_postcommit_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,                                                 // END ID
    Some(lock::lock_twophase_postcommit::call),           // Lock
    Some(pgstat::pgstat_twophase_postcommit::call),       // pgstat
    Some(multixact::multixact_twophase_postcommit::call), // MultiXact
    None,                                                 // PredicateLock
];

/// `twophase_postabort_callbacks`:
/// `{ NULL, lock_twophase_postabort, pgstat_twophase_postabort,
///    multixact_twophase_postabort, NULL }`
pub static twophase_postabort_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,                                                // END ID
    Some(lock::lock_twophase_postabort::call),           // Lock
    Some(pgstat::pgstat_twophase_postabort::call),       // pgstat
    Some(multixact::multixact_twophase_postabort::call), // MultiXact
    None,                                                // PredicateLock
];

/// `twophase_standby_recover_callbacks`:
/// `{ NULL, lock_twophase_standby_recover, NULL, NULL, NULL }`
pub static twophase_standby_recover_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,                                            // END ID
    Some(lock::lock_twophase_standby_recover::call), // Lock
    None,                                            // pgstat
    None,                                            // MultiXact
    None,                                            // PredicateLock
];

#[cfg(test)]
mod tests;
