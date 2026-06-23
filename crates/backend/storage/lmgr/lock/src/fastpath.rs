//! Per-backend fast-path relation locking (`storage/lmgr/lock.c`), deferred to
//! F3.
//!
//! The fast path needs additive `PGPROC` accessor seams from the merged proc
//! crate (`MyProc->fpRelId` / `fpLockBits` / `fpInfoLock`) that have not yet
//! been exposed. F1/F2 take the (correct, slower) shared-table path everywhere
//! the fast path would apply; the single interlock point that can still be
//! reached — `FastPathTransferRelationLocks`, called only when
//! `ConflictsWithRelationFastPath` is true (which is conservatively false until
//! F3) — panics precisely if it is ever hit.

use ::types_error::PgResult;
use ::types_storage::lock::LOCKTAG;

/// `FastPathTransferRelationLocks(lockMethodTable, locktag, hashcode)` (lock.c)
/// — migrate any fast-path locks held by other backends on `locktag` into the
/// main lock table. Reached only on the strong-lock interlock path; deferred to
/// F3 (needs the `MyProc->fpRelId` / `fpLockBits` accessor seams).
pub(crate) fn fast_path_transfer_relation_locks(_locktag: &LOCKTAG, _hashcode: u32) -> PgResult<bool> {
    panic!(
        "FastPathTransferRelationLocks: lock.c fast path not yet ported (F3); \
         needs the additive PGPROC fpRelId/fpLockBits accessor seams from the \
         merged proc crate"
    )
}
