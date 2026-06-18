#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `PgResult` (== `Result<_,
// PgError>`); `PgError`'s size is fixed by `types-error`, and the un-boxed
// `PgResult` is the project-wide contract, so the large-`Err` lint is accepted
// crate-wide.
#![allow(clippy::result_large_err)]
// The `*LockTuple` / object wrappers mirror the C signatures of `lmgr.c`
// one-for-one, so the argument lists are fixed by the source we port.
#![allow(clippy::too_many_arguments)]

//! Lock-manager relation/object wrappers — a port of
//! `src/backend/storage/lmgr/lmgr.c`.
//!
//! lmgr.c is the public lock-manager facade on top of lock.c: it builds the
//! right [`LOCKTAG`] for relations, relation-extension, pages, tuples,
//! transactions, speculative insertions, and shared/database objects, and
//! forwards to lock.c's `LockAcquireExtended`/`LockRelease`. The lmgr-layer
//! algorithms (tag construction, the `XactLockTableWait` retry loop, the
//! `WaitForLockersMultiple` collect-then-wait + progress accounting, the
//! diagnostic formatting) live here; the acquire/release machinery and every
//! cross-subsystem probe go through seams.
//!
//! Where lmgr.c takes a `Relation`, this layer takes the
//! [`LockRelId`](types_storage::lock::LockRelId) value the relcache entry
//! carries in `rd_lockInfo.lockRelId` (the caller reads it off its own
//! relation — `MyDatabaseId`/`IsSharedRelation`/`relisshared` are caller/global
//! state, kept out of this layer except where the lmgr OID entry points must
//! resolve them through their owners' seams).

extern crate alloc;

use core::fmt;

use mcx::{Mcx, PgString, PgVec};
use types_core::primitive::{BlockNumber, OffsetNumber, Oid, TransactionId};
use types_error::{PgError, PgResult};
use types_storage::lock::{
    LockRelId, XLTW_Oper, DEFAULT_LOCKMETHOD, ExclusiveLock, LOCKACQUIRE_ALREADY_CLEAR,
    LOCKACQUIRE_NOT_AVAIL, LOCKMODE, LOCKTAG, LOCKTAG_ADVISORY, LOCKTAG_APPLY_TRANSACTION,
    LOCKTAG_DATABASE_FROZEN_IDS, LOCKTAG_LAST_TYPE, LOCKTAG_OBJECT, LOCKTAG_PAGE, LOCKTAG_RELATION,
    LOCKTAG_RELATION_EXTEND, LOCKTAG_SPECULATIVE_TOKEN, LOCKTAG_TRANSACTION, LOCKTAG_TUPLE,
    LOCKTAG_USERLOCK, LOCKTAG_VIRTUALTRANSACTION, ShareLock,
};
use types_storage::storage::VirtualTransactionId;

use backend_access_transam_subtrans_seams as subtrans;
use backend_catalog_catalog_seams as catalog;
use backend_storage_ipc_procarray_seams as procarray;
use backend_storage_lmgr_lmgr_seams as inward;
use backend_storage_lmgr_lmgr_pc_seams as inward_pc;
use backend_storage_lmgr_lock_seams as lock;
use backend_tcop_postgres_seams as tcop;
use backend_utils_cache_inval_seams as inval;
use backend_utils_init_small_seams as initsmall;

// ===========================================================================
// PROGRESS_WAITFOR_* (commands/progress.h)
// ===========================================================================

const PROGRESS_WAITFOR_TOTAL: i32 = 0;
const PROGRESS_WAITFOR_DONE: i32 = 1;
const PROGRESS_WAITFOR_CURRENT_PID: i32 = 2;

// ===========================================================================
// LockTagTypeNames (lockfuncs.c) + GetLockNameFromTagType (lmgr.c)
// ===========================================================================

/// `LockTagTypeNames[]` (lockfuncs.c) — the human-readable name per lock-tag
/// type. Indexed by `locktag_type`; length is `LOCKTAG_LAST_TYPE + 1`.
pub const LockTagTypeNames: [&str; (LOCKTAG_LAST_TYPE as usize) + 1] = [
    "relation",
    "extend",
    "frozenid",
    "page",
    "tuple",
    "transactionid",
    "virtualxid",
    "spectoken",
    "object",
    "userlock",
    "advisory",
    "applytransaction",
];

/// `GetLockNameFromTagType(locktag_type)` — the lock name for a tag type, or
/// `"???"` when out of range.
pub fn GetLockNameFromTagType(locktag_type: u16) -> &'static str {
    if locktag_type > LOCKTAG_LAST_TYPE as u16 {
        return "???";
    }
    LockTagTypeNames[locktag_type as usize]
}

// ===========================================================================
// Tag constructors (lock.h SET_LOCKTAG_* macros)
// ===========================================================================

/// `SET_LOCKTAG_RELATION` — lock an entire relation.
fn set_locktag_relation(dbid: Oid, relid: Oid) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: relid,
        locktag_field3: 0,
        locktag_field4: 0,
        locktag_type: LOCKTAG_RELATION,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_RELATION_EXTEND` — the relation-extension lock.
fn set_locktag_relation_extend(dbid: Oid, relid: Oid) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: relid,
        locktag_field3: 0,
        locktag_field4: 0,
        locktag_type: LOCKTAG_RELATION_EXTEND,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_DATABASE_FROZEN_IDS` — the per-database datfrozenxid lock.
fn set_locktag_database_frozen_ids(dbid: Oid) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: 0,
        locktag_field3: 0,
        locktag_field4: 0,
        locktag_type: LOCKTAG_DATABASE_FROZEN_IDS,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_PAGE`.
fn set_locktag_page(dbid: Oid, relid: Oid, blocknum: BlockNumber) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: relid,
        locktag_field3: blocknum,
        locktag_field4: 0,
        locktag_type: LOCKTAG_PAGE,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_TUPLE`.
fn set_locktag_tuple(dbid: Oid, relid: Oid, blocknum: BlockNumber, offnum: OffsetNumber) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: relid,
        locktag_field3: blocknum,
        locktag_field4: offnum,
        locktag_type: LOCKTAG_TUPLE,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_TRANSACTION` — lock on a transaction id (xid wait).
fn set_locktag_transaction(xid: TransactionId) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: xid,
        locktag_field2: 0,
        locktag_field3: 0,
        locktag_field4: 0,
        locktag_type: LOCKTAG_TRANSACTION,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_SPECULATIVE_INSERTION` — type `LOCKTAG_SPECULATIVE_TOKEN`.
fn set_locktag_speculative_insertion(xid: TransactionId, token: u32) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: xid,
        locktag_field2: token,
        locktag_field3: 0,
        locktag_field4: 0,
        locktag_type: LOCKTAG_SPECULATIVE_TOKEN,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_OBJECT`.
fn set_locktag_object(dbid: Oid, classid: Oid, objid: Oid, objsubid: u16) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: classid,
        locktag_field3: objid,
        locktag_field4: objsubid,
        locktag_type: LOCKTAG_OBJECT,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SET_LOCKTAG_APPLY_TRANSACTION`.
fn set_locktag_apply_transaction(dbid: Oid, suboid: Oid, xid: TransactionId, objid: u16) -> LOCKTAG {
    LOCKTAG {
        locktag_field1: dbid,
        locktag_field2: suboid,
        locktag_field3: xid,
        locktag_field4: objid,
        locktag_type: LOCKTAG_APPLY_TRANSACTION,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    }
}

/// `SetLocktagRelationOid` — build the relation LOCKTAG from a bare relation
/// OID, choosing the database OID exactly as lmgr.c does: shared (catalog)
/// relations use `InvalidOid` (0), all others use the current database OID.
/// `IsSharedRelation`/`MyDatabaseId` resolve through their owners' seams.
fn set_locktag_relation_oid(relid: Oid) -> LOCKTAG {
    let dbid = if catalog::is_shared_relation::call(relid) {
        0
    } else {
        initsmall::my_database_id::call()
    };
    set_locktag_relation(dbid, relid)
}

// ===========================================================================
// RelationInitLockInfo (lmgr.c)
// ===========================================================================

/// `RelationInitLockInfo(relation)` — compute the `LockRelId` a relcache entry
/// should carry: `relId` is the relation OID, `dbId` is `InvalidOid` (0) for a
/// shared relation else `MyDatabaseId`. In C this writes
/// `relation->rd_lockInfo.lockRelId` in place; here it returns the value so the
/// relcache (the caller) stores it. `relisshared`/`MyDatabaseId` are
/// caller/global state, threaded in.
pub fn RelationInitLockInfo(my_database_id: Oid, relisshared: bool, relid: Oid) -> LockRelId {
    let dbId = if relisshared { 0 } else { my_database_id };
    LockRelId { relId: relid, dbId }
}

// ===========================================================================
// LockAcquireExtended helper (lock.c seam, reportMemoryError always true)
// ===========================================================================

#[inline]
fn lock_acquire(
    tag: &LOCKTAG,
    lockmode: LOCKMODE,
    session_lock: bool,
    dont_wait: bool,
) -> PgResult<types_storage::lock::LockAcquireResult> {
    lock::lock_acquire_extended::call(tag, lockmode, session_lock, dont_wait, false)
}

// ===========================================================================
// Relation lock wrappers (lmgr.c)
// ===========================================================================

/// `LockRelationOid(relid, lockmode)`. Acquire unconditionally, then (unless
/// already clear) absorb invalidation messages and mark the lock clear.
pub fn LockRelationOid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation_oid(relid);
    let res = lock_acquire(&tag, lockmode, false, false)?;
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval::accept_invalidation_messages::call()?;
        lock::mark_lock_clear::call(&tag, lockmode);
    }
    Ok(())
}

/// `ConditionalLockRelationOid(relid, lockmode)` — true iff acquired.
pub fn ConditionalLockRelationOid(relid: Oid, lockmode: LOCKMODE) -> PgResult<bool> {
    let tag = set_locktag_relation_oid(relid);
    let res = lock_acquire(&tag, lockmode, false, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval::accept_invalidation_messages::call()?;
        lock::mark_lock_clear::call(&tag, lockmode);
    }
    Ok(true)
}

/// `LockRelationId(relid, lockmode)` — like `LockRelationOid` but the caller
/// supplies the full `LockRelId` (dbid, relid) directly.
pub fn LockRelationId(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation(relid.dbId, relid.relId);
    let res = lock_acquire(&tag, lockmode, false, false)?;
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval::accept_invalidation_messages::call()?;
        lock::mark_lock_clear::call(&tag, lockmode);
    }
    Ok(())
}

/// `UnlockRelationId(relid, lockmode)`.
pub fn UnlockRelationId(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation(relid.dbId, relid.relId);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

/// `UnlockRelationOid`.
pub fn UnlockRelationOid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation_oid(relid);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

/// `LockRelation(relation, lockmode)` — additional lock on an already-open
/// relation; the relcache's `lockRelId` identifies it.
pub fn LockRelation(lock_rel_id: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation(lock_rel_id.dbId, lock_rel_id.relId);
    let res = lock_acquire(&tag, lockmode, false, false)?;
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval::accept_invalidation_messages::call()?;
        lock::mark_lock_clear::call(&tag, lockmode);
    }
    Ok(())
}

/// `ConditionalLockRelation(relation, lockmode)`.
pub fn ConditionalLockRelation(lock_rel_id: &LockRelId, lockmode: LOCKMODE) -> PgResult<bool> {
    let tag = set_locktag_relation(lock_rel_id.dbId, lock_rel_id.relId);
    let res = lock_acquire(&tag, lockmode, false, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval::accept_invalidation_messages::call()?;
        lock::mark_lock_clear::call(&tag, lockmode);
    }
    Ok(true)
}

/// `UnlockRelation(relation, lockmode)`.
pub fn UnlockRelation(lock_rel_id: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation(lock_rel_id.dbId, lock_rel_id.relId);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

/// `CheckRelationLockedByMe(relation, lockmode, orstronger)`.
pub fn CheckRelationLockedByMe(
    lock_rel_id: &LockRelId,
    lockmode: LOCKMODE,
    orstronger: bool,
) -> bool {
    let tag = set_locktag_relation(lock_rel_id.dbId, lock_rel_id.relId);
    lock::lock_held_by_me::call(&tag, lockmode, orstronger)
}

/// `CheckRelationOidLockedByMe(relid, lockmode, orstronger)`.
pub fn CheckRelationOidLockedByMe(relid: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool {
    let tag = set_locktag_relation_oid(relid);
    lock::lock_held_by_me::call(&tag, lockmode, orstronger)
}

/// `LockHasWaitersRelation(relation, lockmode)`.
pub fn LockHasWaitersRelation(lock_rel_id: &LockRelId, lockmode: LOCKMODE) -> PgResult<bool> {
    let tag = set_locktag_relation(lock_rel_id.dbId, lock_rel_id.relId);
    lock::lock_has_waiters::call(&tag, lockmode, false)
}

/// `LockRelationIdForSession(relid, lockmode)` — a session-level lock that
/// persists across transaction boundaries.
pub fn LockRelationIdForSession(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation(relid.dbId, relid.relId);
    lock_acquire(&tag, lockmode, true, false)?;
    Ok(())
}

/// `UnlockRelationIdForSession(relid, lockmode)`.
pub fn UnlockRelationIdForSession(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation(relid.dbId, relid.relId);
    lock::lock_release_impl::call(&tag, lockmode, true)?;
    Ok(())
}

/// `LockRelationForExtension`.
pub fn LockRelationForExtension(lock_rel_id: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation_extend(lock_rel_id.dbId, lock_rel_id.relId);
    lock_acquire(&tag, lockmode, false, false)?;
    Ok(())
}

/// `ConditionalLockRelationForExtension` — true iff acquired without blocking.
pub fn ConditionalLockRelationForExtension(
    lock_rel_id: &LockRelId,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let tag = set_locktag_relation_extend(lock_rel_id.dbId, lock_rel_id.relId);
    let res = lock_acquire(&tag, lockmode, false, true)?;
    Ok(res != LOCKACQUIRE_NOT_AVAIL)
}

/// `RelationExtensionLockWaiterCount`.
pub fn RelationExtensionLockWaiterCount(lock_rel_id: &LockRelId) -> PgResult<i32> {
    let tag = set_locktag_relation_extend(lock_rel_id.dbId, lock_rel_id.relId);
    lock::lock_waiter_count::call(&tag)
}

/// `UnlockRelationForExtension`.
pub fn UnlockRelationForExtension(lock_rel_id: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_relation_extend(lock_rel_id.dbId, lock_rel_id.relId);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

/// `LockDatabaseFrozenIds(lockmode)` — one backend per database may run
/// `vac_update_datfrozenxid()`. `dbid` is `MyDatabaseId`.
pub fn LockDatabaseFrozenIds(lockmode: LOCKMODE) -> PgResult<()> {
    let tag = set_locktag_database_frozen_ids(initsmall::my_database_id::call());
    lock_acquire(&tag, lockmode, false, false)?;
    Ok(())
}

// ===========================================================================
// Page / tuple lock wrappers (lmgr.c)
// ===========================================================================

/// `LockPage`.
pub fn LockPage(
    lock_rel_id: &LockRelId,
    blkno: BlockNumber,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_page(lock_rel_id.dbId, lock_rel_id.relId, blkno);
    lock_acquire(&tag, lockmode, false, false)?;
    Ok(())
}

/// `ConditionalLockPage` — true iff acquired without blocking.
pub fn ConditionalLockPage(
    lock_rel_id: &LockRelId,
    blkno: BlockNumber,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let tag = set_locktag_page(lock_rel_id.dbId, lock_rel_id.relId, blkno);
    let res = lock_acquire(&tag, lockmode, false, true)?;
    Ok(res != LOCKACQUIRE_NOT_AVAIL)
}

/// `UnlockPage`.
pub fn UnlockPage(
    lock_rel_id: &LockRelId,
    blkno: BlockNumber,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_page(lock_rel_id.dbId, lock_rel_id.relId, blkno);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

/// `LockTuple`.
pub fn LockTuple(
    lock_rel_id: &LockRelId,
    blkno: BlockNumber,
    offnum: OffsetNumber,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_tuple(lock_rel_id.dbId, lock_rel_id.relId, blkno, offnum);
    lock_acquire(&tag, lockmode, false, false)?;
    Ok(())
}

/// `ConditionalLockTuple(relation, tid, lockmode, logLockFailure)` — true iff
/// acquired without blocking.
pub fn ConditionalLockTuple(
    lock_rel_id: &LockRelId,
    blkno: BlockNumber,
    offnum: OffsetNumber,
    lockmode: LOCKMODE,
    log_lock_failure: bool,
) -> PgResult<bool> {
    let tag = set_locktag_tuple(lock_rel_id.dbId, lock_rel_id.relId, blkno, offnum);
    let res = lock::lock_acquire_extended::call(&tag, lockmode, false, true, log_lock_failure)?;
    Ok(res != LOCKACQUIRE_NOT_AVAIL)
}

/// `UnlockTuple`.
pub fn UnlockTuple(
    lock_rel_id: &LockRelId,
    blkno: BlockNumber,
    offnum: OffsetNumber,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_tuple(lock_rel_id.dbId, lock_rel_id.relId, blkno, offnum);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

// ===========================================================================
// Transaction lock wrappers (lmgr.c)
// ===========================================================================

/// `XactLockTableInsert(xid)` — register that `xid` is running.
pub fn XactLockTableInsert(xid: TransactionId) -> PgResult<()> {
    let tag = set_locktag_transaction(xid);
    lock_acquire(&tag, ExclusiveLock, false, false)?;
    Ok(())
}

/// `XactLockTableDelete(xid)` — drop the running-xid lock (subxids only).
pub fn XactLockTableDelete(xid: TransactionId) -> PgResult<()> {
    let tag = set_locktag_transaction(xid);
    lock::lock_release_impl::call(&tag, ExclusiveLock, false)?;
    Ok(())
}

/// `XactLockTableWait(xid, rel, ctid, oper)` — wait for `xid` to commit or
/// abort, following subtransactions up to the topmost parent.
///
/// C installs a verbose error-context callback when `oper != XLTW_None`; per
/// `docs/query-lifecycle-raii.md` error context attaches on propagation, so
/// `wait_ctx` (the rel name + ctid + oper) is supplied and the message is
/// attached via `map_err` on the error path. `None` reproduces C's
/// `oper == XLTW_None` (no context).
pub fn XactLockTableWait(
    xid: TransactionId,
    wait_ctx: Option<&XactLockTableWaitInfo<'_>>,
) -> PgResult<()> {
    xact_lock_table_wait_inner(xid).map_err(|e| match wait_ctx.and_then(xact_wait_error_context) {
        Some(cxt) => e.add_context(cxt),
        None => e,
    })
}

fn xact_lock_table_wait_inner(mut xid: TransactionId) -> PgResult<()> {
    let mut first = true;
    // C: Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny())); the
    // top-xid is ambient transaction state with no caller capability here, so
    // the debug-only Assert is not reproduced (no ambient-global getter seam).
    loop {
        let tag = set_locktag_transaction(xid);
        lock_acquire(&tag, ShareLock, false, false)?;
        lock::lock_release_impl::call(&tag, ShareLock, false)?;

        if !procarray::transaction_id_is_in_progress::call(xid)? {
            break;
        }

        // If the xid belonged to a subtransaction, wait on the topmost
        // transaction instead. Don't sleep the first time through.
        if !first {
            tcop::check_for_interrupts::call()?;
            port_pgsleep_seams::pg_usleep::call(1000);
        }
        first = false;
        xid = subtrans::sub_trans_get_topmost_transaction::call(xid)?;
    }
    Ok(())
}

/// `ConditionalXactLockTableWait(xid, logLockFailure)` — never blocks; true if
/// the lock was acquired (the xact has finished).
pub fn ConditionalXactLockTableWait(
    mut xid: TransactionId,
    log_lock_failure: bool,
) -> PgResult<bool> {
    let mut first = true;
    loop {
        let tag = set_locktag_transaction(xid);
        let res = lock::lock_acquire_extended::call(&tag, ShareLock, false, true, log_lock_failure)?;
        if res == LOCKACQUIRE_NOT_AVAIL {
            return Ok(false);
        }
        lock::lock_release_impl::call(&tag, ShareLock, false)?;

        if !procarray::transaction_id_is_in_progress::call(xid)? {
            break;
        }

        if !first {
            tcop::check_for_interrupts::call()?;
            port_pgsleep_seams::pg_usleep::call(1000);
        }
        first = false;
        xid = subtrans::sub_trans_get_topmost_transaction::call(xid)?;
    }
    Ok(true)
}

/// `SpeculativeInsertionLockAcquire(xid)` — take the `ExclusiveLock` on a
/// speculative-insertion token and return the token used.
///
/// The per-backend `speculativeInsertionToken` counter (a backend-global) is
/// `thread_local!` here; it is incremented (with wrap-around correction) and
/// returned. The first call sees the initial `0`.
pub fn SpeculativeInsertionLockAcquire(xid: TransactionId) -> PgResult<u32> {
    let token = SPECULATIVE_INSERTION_TOKEN.with(|t| {
        let mut v = t.get().wrapping_add(1);
        // Check for wrap-around. Zero means no token is held, so don't use it.
        if v == 0 {
            v = 1;
        }
        t.set(v);
        v
    });

    let tag = set_locktag_speculative_insertion(xid, token);
    lock_acquire(&tag, ExclusiveLock, false, false)?;
    Ok(token)
}

/// `SpeculativeInsertionLockRelease(xid)` — release the speculative-insertion
/// lock previously acquired (using the current `speculativeInsertionToken`).
pub fn SpeculativeInsertionLockRelease(xid: TransactionId) -> PgResult<()> {
    let token = SPECULATIVE_INSERTION_TOKEN.with(|t| t.get());
    let tag = set_locktag_speculative_insertion(xid, token);
    lock::lock_release_impl::call(&tag, ExclusiveLock, false)?;
    Ok(())
}

/// `SpeculativeInsertionWait(xid, token)` — wait for the given speculative
/// insertion to be confirmed or aborted (`ShareLock` acquire+release).
pub fn SpeculativeInsertionWait(xid: TransactionId, token: u32) -> PgResult<()> {
    // C: Assert(TransactionIdIsValid(xid)); Assert(token != 0);
    debug_assert_ne!(token, 0);
    let tag = set_locktag_speculative_insertion(xid, token);
    lock_acquire(&tag, ShareLock, false, false)?;
    lock::lock_release_impl::call(&tag, ShareLock, false)?;
    Ok(())
}

std::thread_local! {
    /// `static uint32 speculativeInsertionToken` (lmgr.c) — per-backend
    /// counter for generating speculative insertion tokens. May wrap; that is
    /// fine (see the C comment on the static).
    static SPECULATIVE_INSERTION_TOKEN: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

// ===========================================================================
// XactLockTableWaitErrorCb (lmgr.c) — error-context message
// ===========================================================================

/// Context for a transaction lock wait, matching C's `XactLockTableWaitInfo`:
/// `oper` plus the address (relation name + ctid) of the tuple being waited
/// for. `rel`/`ctid` are caller state; the relation name and the
/// item-pointer's block/offset are threaded in.
pub struct XactLockTableWaitInfo<'a> {
    pub oper: XLTW_Oper,
    /// `RelationGetRelationName(info->rel)`; `None` mirrors C's
    /// `RelationIsValid(info->rel)` failing.
    pub rel_name: Option<&'a str>,
    /// `ItemPointerGet{BlockNumber,OffsetNumber}(info->ctid)`; `None` mirrors
    /// C's `ItemPointerIsValid(info->ctid)` failing.
    pub ctid: Option<(BlockNumber, OffsetNumber)>,
}

/// `XactLockTableWaitErrorCb` — the error-context string for a transaction
/// lock wait, or `None` when no context applies (C's `default: return;` and
/// the invalid-arg early outs). Message text reproduced from the C
/// `gettext_noop` strings.
fn xact_wait_error_context(info: &XactLockTableWaitInfo<'_>) -> Option<String> {
    // We would like to print schema name too, but that would require a syscache
    // lookup.
    if info.oper == XLTW_Oper::None {
        return None;
    }
    let (block, offset) = info.ctid?;
    let rel_name = info.rel_name?;
    let cxt = match info.oper {
        XLTW_Oper::Update => "while updating tuple ({block},{offset}) in relation \"{rel}\"",
        XLTW_Oper::Delete => "while deleting tuple ({block},{offset}) in relation \"{rel}\"",
        XLTW_Oper::Lock => "while locking tuple ({block},{offset}) in relation \"{rel}\"",
        XLTW_Oper::LockUpdated => {
            "while locking updated version ({block},{offset}) of tuple in relation \"{rel}\""
        }
        XLTW_Oper::InsertIndex => {
            "while inserting index tuple ({block},{offset}) in relation \"{rel}\""
        }
        XLTW_Oper::InsertIndexUnique => {
            "while checking uniqueness of tuple ({block},{offset}) in relation \"{rel}\""
        }
        XLTW_Oper::FetchUpdated => {
            "while rechecking updated tuple ({block},{offset}) in relation \"{rel}\""
        }
        XLTW_Oper::RecheckExclusionConstr => {
            "while checking exclusion constraint on tuple ({block},{offset}) in relation \"{rel}\""
        }
        XLTW_Oper::None => return None,
    };
    Some(
        cxt.replace("{block}", &block.to_string())
            .replace("{offset}", &offset.to_string())
            .replace("{rel}", rel_name),
    )
}

// ===========================================================================
// WaitForLockersMultiple / WaitForLockers (lmgr.c)
// ===========================================================================

/// `WaitForLockersMultiple(locktags, lockmode, progress)` — wait until no
/// transaction holds locks conflicting with `locktags` at `lockmode`.
///
/// Obtain the current list of lockers (collect-all-first), then wait on each
/// holder's vxid. We do not try to acquire the locks themselves, only the
/// vxids/xids of their holders. The `holders` working buffer is allocated in
/// `mcx` (C uses `lappend`/`list_free_deep` in the current context).
pub fn WaitForLockersMultiple(
    mcx: Mcx<'_>,
    locktags: &[LOCKTAG],
    lockmode: LOCKMODE,
    progress: bool,
) -> PgResult<()> {
    let mut total: i32 = 0;
    let mut done: i32 = 0;

    // Done if no locks to wait for.
    if locktags.is_empty() {
        return Ok(());
    }

    // Collect the transactions we need to wait on. The number of holder lists
    // is bounded by `locktags.len()` (caller-supplied, not data-derived), so
    // one sized reservation suffices.
    let mut holders: PgVec<PgVec<VirtualTransactionId>> =
        mcx::vec_with_capacity_in(mcx, locktags.len())?;
    for locktag in locktags {
        let conflicts = lock::get_lock_conflicts::call(mcx, locktag, lockmode)?;
        if progress {
            total += conflicts.len() as i32;
        }
        holders.push(conflicts);
    }

    if progress {
        backend_utils_activity_small::backend_progress::pgstat_progress_update_param(
            PROGRESS_WAITFOR_TOTAL,
            total as i64,
        );
    }

    // Note: GetLockConflicts() never reports our own xid, so we need not check
    // for that. Also, prepared xacts are reported and awaited.

    // Finally wait for each such transaction to complete.
    for lockholders in holders.iter() {
        for vxid in lockholders.iter() {
            // C walks the array up to the trailing invalid terminator; our Vec
            // holds only the valid entries, but mirror the validity guard.
            if !vxid.is_valid() {
                break;
            }
            // If requested, publish who we're going to wait for.
            if progress {
                let pid = procarray::proc_number_get_proc_pid::call(vxid.procNumber);
                if pid != 0 {
                    backend_utils_activity_small::backend_progress::pgstat_progress_update_param(
                        PROGRESS_WAITFOR_CURRENT_PID,
                        pid as i64,
                    );
                }
            }
            lock::virtual_xact_lock::call(*vxid, true)?;

            if progress {
                done += 1;
                backend_utils_activity_small::backend_progress::pgstat_progress_update_param(
                    PROGRESS_WAITFOR_DONE,
                    done as i64,
                );
            }
        }
    }
    if progress {
        let index = [
            PROGRESS_WAITFOR_TOTAL,
            PROGRESS_WAITFOR_DONE,
            PROGRESS_WAITFOR_CURRENT_PID,
        ];
        let values = [0i64, 0, 0];
        backend_utils_activity_small::backend_progress::pgstat_progress_update_multi_param(
            &index, &values,
        );
    }

    Ok(())
}

/// `WaitForLockers(heaplocktag, lockmode, progress)` — single-tag form.
pub fn WaitForLockers(
    mcx: Mcx<'_>,
    heaplocktag: LOCKTAG,
    lockmode: LOCKMODE,
    progress: bool,
) -> PgResult<()> {
    let l = [heaplocktag];
    WaitForLockersMultiple(mcx, &l, lockmode, progress)
}

// ===========================================================================
// Database/shared-object lock wrappers (lmgr.c)
// ===========================================================================

/// `LockDatabaseObject(classid, objid, objsubid, lockmode)` — lock a general
/// object of the current database (`dbid` = `MyDatabaseId`). Absorbs
/// invalidation messages afterwards (C does so unconditionally).
pub fn LockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_object(initsmall::my_database_id::call(), classid, objid, objsubid);
    lock_acquire(&tag, lockmode, false, false)?;
    inval::accept_invalidation_messages::call()?;
    Ok(())
}

/// `ConditionalLockDatabaseObject(classid, objid, objsubid, lockmode)`.
pub fn ConditionalLockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let tag = set_locktag_object(initsmall::my_database_id::call(), classid, objid, objsubid);
    let res = lock_acquire(&tag, lockmode, false, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval::accept_invalidation_messages::call()?;
        lock::mark_lock_clear::call(&tag, lockmode);
    }
    Ok(true)
}

/// `UnlockDatabaseObject`.
pub fn UnlockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_object(initsmall::my_database_id::call(), classid, objid, objsubid);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

/// `LockSharedObject(classid, objid, objsubid, lockmode)` — lock a
/// shared-across-databases object (`dbid` = `InvalidOid` = 0).
pub fn LockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_object(0, classid, objid, objsubid);
    lock_acquire(&tag, lockmode, false, false)?;
    inval::accept_invalidation_messages::call()?;
    Ok(())
}

/// `ConditionalLockSharedObject(classid, objid, objsubid, lockmode)`.
pub fn ConditionalLockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let tag = set_locktag_object(0, classid, objid, objsubid);
    let res = lock_acquire(&tag, lockmode, false, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval::accept_invalidation_messages::call()?;
        lock::mark_lock_clear::call(&tag, lockmode);
    }
    Ok(true)
}

/// `UnlockSharedObject`.
pub fn UnlockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_object(0, classid, objid, objsubid);
    lock::lock_release_impl::call(&tag, lockmode, false)?;
    Ok(())
}

/// `LockSharedObjectForSession(classid, objid, objsubid, lockmode)`.
pub fn LockSharedObjectForSession(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_object(0, classid, objid, objsubid);
    lock_acquire(&tag, lockmode, true, false)?;
    Ok(())
}

/// `UnlockSharedObjectForSession`.
pub fn UnlockSharedObjectForSession(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_object(0, classid, objid, objsubid);
    lock::lock_release_impl::call(&tag, lockmode, true)?;
    Ok(())
}

/// `LockApplyTransactionForSession(suboid, xid, objid, lockmode)`
/// (`dbid` = `MyDatabaseId`).
pub fn LockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_apply_transaction(initsmall::my_database_id::call(), suboid, xid, objid);
    lock_acquire(&tag, lockmode, true, false)?;
    Ok(())
}

/// `UnlockApplyTransactionForSession`.
pub fn UnlockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = set_locktag_apply_transaction(initsmall::my_database_id::call(), suboid, xid, objid);
    lock::lock_release_impl::call(&tag, lockmode, true)?;
    Ok(())
}

// ===========================================================================
// DescribeLockTag (lmgr.c) — diagnostic formatting
// ===========================================================================

/// Append `args` to `buf` through the fallible context allocator — C's
/// `appendStringInfo`, which can `ereport(ERROR)` on OOM (`enlargeStringInfo`).
fn append_fmt(buf: &mut PgString<'_>, args: fmt::Arguments<'_>) -> PgResult<()> {
    struct Sink<'a, 'mcx> {
        buf: &'a mut PgString<'mcx>,
        err: Option<PgError>,
    }
    impl fmt::Write for Sink<'_, '_> {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            self.buf.try_push_str(s).map_err(|e| {
                self.err = Some(e);
                fmt::Error
            })
        }
    }
    let mut sink = Sink { buf, err: None };
    match fmt::Write::write_fmt(&mut sink, args) {
        Ok(()) => Ok(()),
        Err(_) => Err(sink.err.expect("Sink only errors with a stored PgError")),
    }
}

macro_rules! appendf {
    ($buf:expr, $($arg:tt)*) => {
        append_fmt($buf, core::format_args!($($arg)*))
    };
}

/// `DescribeLockTag(buf, tag)` — append a description of a lockable object to
/// `buf`. Numeric values are printed (printing names would require system-table
/// locks, dangerous for deadlock reporting). Message text reproduced from the C
/// `_()` translatable strings.
pub fn DescribeLockTag(buf: &mut PgString<'_>, tag: &LOCKTAG) -> PgResult<()> {
    match tag.locktag_type {
        LOCKTAG_RELATION => appendf!(
            buf,
            "relation {} of database {}",
            tag.locktag_field2,
            tag.locktag_field1
        ),
        LOCKTAG_RELATION_EXTEND => appendf!(
            buf,
            "extension of relation {} of database {}",
            tag.locktag_field2,
            tag.locktag_field1
        ),
        LOCKTAG_DATABASE_FROZEN_IDS => appendf!(
            buf,
            "pg_database.datfrozenxid of database {}",
            tag.locktag_field1
        ),
        LOCKTAG_PAGE => appendf!(
            buf,
            "page {} of relation {} of database {}",
            tag.locktag_field3,
            tag.locktag_field2,
            tag.locktag_field1
        ),
        LOCKTAG_TUPLE => appendf!(
            buf,
            "tuple ({},{}) of relation {} of database {}",
            tag.locktag_field3,
            tag.locktag_field4,
            tag.locktag_field2,
            tag.locktag_field1
        ),
        LOCKTAG_TRANSACTION => appendf!(buf, "transaction {}", tag.locktag_field1),
        LOCKTAG_VIRTUALTRANSACTION => appendf!(
            buf,
            "virtual transaction {}/{}",
            tag.locktag_field1 as i32,
            tag.locktag_field2
        ),
        LOCKTAG_SPECULATIVE_TOKEN => appendf!(
            buf,
            "speculative token {} of transaction {}",
            tag.locktag_field2,
            tag.locktag_field1
        ),
        LOCKTAG_OBJECT => appendf!(
            buf,
            "object {} of class {} of database {}",
            tag.locktag_field3,
            tag.locktag_field2,
            tag.locktag_field1
        ),
        // reserved for old contrib code, now on pgfoundry
        LOCKTAG_USERLOCK => appendf!(
            buf,
            "user lock [{},{},{}]",
            tag.locktag_field1,
            tag.locktag_field2,
            tag.locktag_field3
        ),
        LOCKTAG_ADVISORY => appendf!(
            buf,
            "advisory lock [{},{},{},{}]",
            tag.locktag_field1,
            tag.locktag_field2,
            tag.locktag_field3,
            tag.locktag_field4
        ),
        LOCKTAG_APPLY_TRANSACTION => appendf!(
            buf,
            "remote transaction {} of subscription {} of database {}",
            tag.locktag_field3,
            tag.locktag_field2,
            tag.locktag_field1
        ),
        other => appendf!(buf, "unrecognized locktag type {}", other as i32),
    }
}

// ===========================================================================
// Inward seam implementations + install
// ===========================================================================

/// The `lock_relation_oid` inward seam: `LockRelationOid` returning the held
/// lock as the [`inward::LockGuard`] the consumer holds.
fn seam_lock_relation_oid(relid: Oid, lockmode: LOCKMODE) -> PgResult<inward::LockGuard> {
    LockRelationOid(relid, lockmode)?;
    Ok(inward::LockGuard::relation(relid, lockmode))
}

/// The `conditional_lock_relation_oid` inward seam.
fn seam_conditional_lock_relation_oid(
    relid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<inward::LockGuard>> {
    if ConditionalLockRelationOid(relid, lockmode)? {
        Ok(Some(inward::LockGuard::relation(relid, lockmode)))
    } else {
        Ok(None)
    }
}

/// The `lock_database_object` inward seam.
fn seam_lock_database_object(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<inward::LockGuard> {
    LockDatabaseObject(classid, objid, objsubid, lockmode)?;
    Ok(inward::LockGuard::database_object(classid, objid, objsubid, lockmode))
}

/// The `check_relation_locked_by_me` inward seam (relation crosses as its OID).
fn seam_check_relation_locked_by_me(relation: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool {
    CheckRelationOidLockedByMe(relation, lockmode, orstronger)
}

/// The `lock_shared_object` inward seam: `LockSharedObject` returning the held
/// lock as the [`inward::LockGuard`] (a shared object reuses the OBJECT release
/// path with `dbid = 0`, exactly what `LockGuard::database_object` records).
fn seam_lock_shared_object(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<inward::LockGuard> {
    LockSharedObject(classid, objid, objsubid, lockmode)?;
    Ok(inward::LockGuard::database_object(classid, objid, objsubid, lockmode))
}

/// The `lock_relation_for_extension` inward seam. The C reads
/// `relation->rd_lockInfo.lockRelId`; this layer rebuilds that value from the
/// relation (`relId = rd_id`, `dbId = rd_locator.dbOid`, which is `InvalidOid`
/// for shared relations — the same as `RelationInitLockInfo`). Always
/// `ExclusiveLock` per the C.
fn seam_lock_relation_for_extension(
    rel: &types_rel::Relation<'_>,
) -> PgResult<inward::RelationExtensionLockGuard> {
    let lock_rel_id = LockRelId { relId: rel.rd_id, dbId: rel.rd_locator.dbOid };
    LockRelationForExtension(&lock_rel_id, ExclusiveLock)?;
    Ok(inward::RelationExtensionLockGuard::new(rel.rd_id))
}

/// The `unlock_relation_for_extension` inward seam — the release half, reached
/// only through [`inward::RelationExtensionLockGuard`]. The guard carries the
/// relation OID; the extension lock's `dbId` is irrelevant for release because
/// `LockRelease` matches on the full tag, and the guard was built from the same
/// relation. We rebuild the tag with `dbId = InvalidOid` only when the relation
/// is shared; to stay faithful we reconstruct from the OID alone is impossible,
/// so the guard release path delegates to `UnlockRelationForExtension` with the
/// dbId resolved the same way as acquisition.
fn seam_unlock_relation_for_extension(relid: Oid) -> PgResult<()> {
    // The guard only retains the relation OID. The matching unlock tag must use
    // the same `lockRelId` that acquisition used. `MyDatabaseId` is the dbId for
    // a non-shared relation; a shared relation used `InvalidOid`. The extension
    // lock is only taken on real (always non-shared, local) heap/index
    // relations, so the dbId is `MyDatabaseId` — the same value acquisition
    // recorded from `rd_locator.dbOid`.
    let lock_rel_id = LockRelId { relId: relid, dbId: initsmall::my_database_id::call() };
    UnlockRelationForExtension(&lock_rel_id, ExclusiveLock)
}

/// The `describe_lock_tag` inward seam: render a `LOCKTAG` to a `String`. C
/// appends to a caller `StringInfo`; the seam allocates a transient buffer and
/// returns the rendered text (the deadlock detector appends it itself).
fn seam_describe_lock_tag(tag: LOCKTAG) -> alloc::string::String {
    let ctx = mcx::MemoryContext::new("DescribeLockTag");
    let mut buf = PgString::new_in(ctx.mcx());
    // The descriptions are small, bounded strings; an OOM here would already
    // have aborted the deadlock report that called us.
    DescribeLockTag(&mut buf, &tag).expect("DescribeLockTag formatting cannot fail for a bounded description");
    alloc::string::String::from(buf.as_str())
}

/// The `lock_tuple` inward seam. The C `LockTuple(Relation, tid, lockmode)`
/// reads `relation->rd_lockInfo.lockRelId` for the tag's `dbId`; the seam
/// crosses only the relation OID, so we resolve `dbId` exactly as the OID lock
/// entry points do (`InvalidOid` for a shared relation, else `MyDatabaseId`),
/// then split the `ItemPointerData` into block/offset for `LockTuple`.
fn seam_lock_tuple(
    relid: Oid,
    tid: types_tuple::heaptuple::ItemPointerData,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let lock_rel_id = lock_rel_id_from_oid(relid);
    LockTuple(&lock_rel_id, tid.ip_blkid.block_number(), tid.ip_posid, lockmode)
}

/// The `unlock_tuple` inward seam — release counterpart to [`seam_lock_tuple`],
/// resolving `dbId` the same way so the release tag matches acquisition.
fn seam_unlock_tuple(
    relid: Oid,
    tid: types_tuple::heaptuple::ItemPointerData,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let lock_rel_id = lock_rel_id_from_oid(relid);
    UnlockTuple(&lock_rel_id, tid.ip_blkid.block_number(), tid.ip_posid, lockmode)
}

/// The `conditional_lock_tuple` inward seam — non-blocking counterpart to
/// [`seam_lock_tuple`].
fn seam_conditional_lock_tuple(
    relid: Oid,
    tid: types_tuple::heaptuple::ItemPointerData,
    lockmode: LOCKMODE,
    log_lock_failure: bool,
) -> PgResult<bool> {
    let lock_rel_id = lock_rel_id_from_oid(relid);
    ConditionalLockTuple(
        &lock_rel_id,
        tid.ip_blkid.block_number(),
        tid.ip_posid,
        lockmode,
        log_lock_failure,
    )
}

/// The `xact_lock_table_wait` inward seam — wait for `xid`, building the
/// `XactLockTableWaitInfo` error context from the crossed address.
fn seam_xact_lock_table_wait(
    xid: TransactionId,
    rel_name: alloc::string::String,
    ctid: types_tuple::heaptuple::ItemPointerData,
    oper: XLTW_Oper,
) -> PgResult<()> {
    let info = XactLockTableWaitInfo {
        oper,
        rel_name: Some(rel_name.as_str()),
        ctid: Some((ctid.ip_blkid.block_number(), ctid.ip_posid)),
    };
    XactLockTableWait(xid, Some(&info))
}

/// Resolve the `LockRelId` for a bare relation OID, choosing `dbId` exactly as
/// `SetLocktagRelationOid` does: shared (catalog) relations use `InvalidOid`,
/// all others use the current database OID.
fn lock_rel_id_from_oid(relid: Oid) -> LockRelId {
    let dbId = if catalog::is_shared_relation::call(relid) {
        0
    } else {
        initsmall::my_database_id::call()
    };
    LockRelId { relId: relid, dbId }
}

/// Install every seam declared in `backend-storage-lmgr-lmgr-seams`.
pub fn init_seams() {
    inward::get_lock_name_from_tag_type::set(GetLockNameFromTagType);
    inward::check_relation_locked_by_me::set(seam_check_relation_locked_by_me);
    inward::lock_relation_oid::set(seam_lock_relation_oid);
    inward::conditional_lock_relation_oid::set(seam_conditional_lock_relation_oid);
    inward::lock_database_object::set(seam_lock_database_object);
    inward::lock_shared_object::set(seam_lock_shared_object);
    inward::unlock_relation_oid::set(UnlockRelationOid);
    inward::unlock_database_object::set(UnlockDatabaseObject);
    inward::unlock_shared_object::set(UnlockSharedObject);
    inward::lock_relation_for_extension::set(seam_lock_relation_for_extension);
    inward::unlock_relation_for_extension::set(seam_unlock_relation_for_extension);
    inward::describe_lock_tag::set(seam_describe_lock_tag);
    // XactLockTableInsert/XactLockTableDelete are lmgr.c functions this crate
    // owns; install the inward seams that xact.c consumes.
    inward::xact_lock_table_insert::set(XactLockTableInsert);
    inward::xact_lock_table_delete::set(XactLockTableDelete);
    // Per-database datfrozenxid interlock (vacuum's vac_update_datfrozenxid).
    inward::lock_database_frozen_ids::set(LockDatabaseFrozenIds);
    // Bare-OID relation lock-held probe (check_relation_locked_by_me crosses a
    // LockRelId-derived relation; this is the OID-resolving variant).
    inward::check_relation_oid_locked_by_me::set(CheckRelationOidLockedByMe);
    // Heavyweight tuple-tag lock/unlock (in-place-update tuple lock).
    inward::lock_tuple::set(seam_lock_tuple);
    inward::unlock_tuple::set(seam_unlock_tuple);
    // Conditional heap-AM lock-wait primitives (heap_lock_tuple).
    inward::conditional_lock_tuple::set(seam_conditional_lock_tuple);
    inward::conditional_xact_lock_table_wait::set(ConditionalXactLockTableWait);
    inward::xact_lock_table_wait::set(seam_xact_lock_table_wait);
    inward::speculative_insertion_wait::set(SpeculativeInsertionWait);
    // Session-level apply-transaction locks (parallel-apply deadlock detection).
    inward::lock_apply_transaction_for_session::set(LockApplyTransactionForSession);
    inward::unlock_apply_transaction_for_session::set(UnlockApplyTransactionForSession);
    // Session-level shared-object locks (dbase_redo hot-standby DROP path).
    inward::lock_shared_object_for_session::set(LockSharedObjectForSession);
    inward::unlock_shared_object_for_session::set(UnlockSharedObjectForSession);
    // plancache's slice of lmgr.c (the -pc-seams crate this unit also owns):
    // bare-OID LockRelationOid/UnlockRelationOid for revalidation locking.
    inward_pc::lock_relation_oid::set(LockRelationOid);
    inward_pc::unlock_relation_oid::set(UnlockRelationOid);

    // --- lazy-vacuum driver's relation-lock seams (vacuumlazy.c's truncation
    //     interlock). In C these are macros that take `relation` and derive
    //     `&relation->rd_lockInfo.lockRelId`; here the LockRelId is resolved off
    //     the relcache entry (by `rel.rd_id`) before calling the LockRelId-keyed
    //     lmgr.c primitives this crate owns. They home in vacuumlazy-seams. ---
    use backend_access_heap_vacuumlazy_seams as vx;
    vx::unlock_relation::set(|rel, lockmode| {
        let lri = backend_utils_cache_relcache_seams::rel_lock_relid::call(rel.rd_id)?;
        UnlockRelation(&lri, lockmode)
    });
    vx::conditional_lock_relation::set(|rel, lockmode| {
        let lri = backend_utils_cache_relcache_seams::rel_lock_relid::call(rel.rd_id)?;
        ConditionalLockRelation(&lri, lockmode)
    });
    vx::lock_has_waiters_relation::set(|rel, lockmode| {
        let lri = backend_utils_cache_relcache_seams::rel_lock_relid::call(rel.rd_id)?;
        LockHasWaitersRelation(&lri, lockmode)
    });
}
