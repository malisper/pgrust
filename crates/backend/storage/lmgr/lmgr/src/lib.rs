#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use types_core::{Oid, TransactionId};
use types_error::PgResult;
use types_rel::{LockInfoData, LockRelId, RelationData};
use types_storage::lock::{
    ExclusiveLock, LockAcquireResult, ShareLock, XLTW_Oper, LOCKACQUIRE_ALREADY_CLEAR,
    LOCKACQUIRE_NOT_AVAIL, LOCKMODE, LOCKTAG,
};
use types_tuple::{
    ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid,
};

#[cfg(test)]
mod tests;

pub fn init_seams() {
    lmgr_seams::lock_relation_oid::set(LockRelationOid);
    lmgr_seams::unlock_relation_oid::set(UnlockRelationOid);
    lmgr_seams::conditional_lock_relation_oid::set(ConditionalLockRelationOid);
    lmgr_seams::check_relation_locked_by_me::set(CheckRelationOidLockedByMe);
    lmgr_seams::lock_database_object::set(LockDatabaseObject);
    lmgr_seams::unlock_database_object::set(UnlockDatabaseObject);
    lmgr_seams::describe_lock_tag::set(DescribeLockTag);
}

pub fn DescribeLockTag(tag: LOCKTAG) -> String {
    use types_storage::lock::{
        LOCKTAG_ADVISORY, LOCKTAG_APPLY_TRANSACTION, LOCKTAG_DATABASE_FROZEN_IDS, LOCKTAG_OBJECT,
        LOCKTAG_PAGE, LOCKTAG_RELATION, LOCKTAG_RELATION_EXTEND, LOCKTAG_SPECULATIVE_TOKEN,
        LOCKTAG_TRANSACTION, LOCKTAG_TUPLE, LOCKTAG_USERLOCK, LOCKTAG_VIRTUALTRANSACTION,
    };
    let (f1, f2, f3, f4) = (
        tag.locktag_field1,
        tag.locktag_field2,
        tag.locktag_field3,
        tag.locktag_field4,
    );
    match tag.locktag_type {
        LOCKTAG_RELATION => format!("relation {f2} of database {f1}"),
        LOCKTAG_RELATION_EXTEND => format!("extension of relation {f2} of database {f1}"),
        LOCKTAG_DATABASE_FROZEN_IDS => format!("pg_database.datfrozenxid of database {f1}"),
        LOCKTAG_PAGE => format!("page {f3} of relation {f2} of database {f1}"),
        LOCKTAG_TUPLE => format!("tuple ({f3},{f4}) of relation {f2} of database {f1}"),
        LOCKTAG_TRANSACTION => format!("transaction {f1}"),
        LOCKTAG_VIRTUALTRANSACTION => format!("virtual transaction {}/{f2}", f1 as i32),
        LOCKTAG_SPECULATIVE_TOKEN => format!("speculative token {f2} of transaction {f1}"),
        LOCKTAG_OBJECT => format!("object {f3} of class {f2} of database {f1}"),
        LOCKTAG_USERLOCK => format!("user lock [{f1},{f2},{f3}]"),
        LOCKTAG_ADVISORY => format!("advisory lock [{f1},{f2},{f3},{f4}]"),
        LOCKTAG_APPLY_TRANSACTION => {
            format!("remote transaction {f3} of subscription {f2} of database {f1}")
        }
        other => format!("unrecognized locktag type {other}"),
    }
}

// WaitForLockersMultiple (lmgr.c:986): wait for current lock holders to
// finish without acquiring the lock ourselves; progress reporting unported.
pub fn WaitForLockersMultiple(
    mcx: mcx::Mcx<'_>,
    locktags: &[LOCKTAG],
    lockmode: LOCKMODE,
) -> PgResult<()> {
    for locktag in locktags {
        let holders = lock::GetLockConflicts(mcx, locktag, lockmode)?;
        for &vxid in holders.iter() {
            lock::VirtualXactLock(vxid, true)?;
        }
    }
    Ok(())
}

pub fn RelationInitLockInfo(relid: Oid, relisshared: bool) -> LockInfoData {
    let dbId = if relisshared { 0 } else { init_small::globals::MyDatabaseId() };
    LockInfoData { lockRelId: LockRelId { relId: relid, dbId } }
}

fn SetLocktagRelationOid(relid: Oid) -> LOCKTAG {
    let dbid = if catalog_seams::is_shared_relation::call(relid) {
        0
    } else {
        init_small::globals::MyDatabaseId()
    };
    LOCKTAG::relation(dbid, relid)
}

#[inline]
fn rel_tag(rel: &RelationData<'_>) -> LOCKTAG {
    LOCKTAG::relation(rel.rd_lockInfo.lockRelId.dbId, rel.rd_lockInfo.lockRelId.relId)
}

fn acquire(tag: LOCKTAG, lockmode: LOCKMODE, dont_wait: bool) -> PgResult<LockAcquireResult> {
    lock_seams::lock_acquire_extended::call(tag, lockmode, false, dont_wait, true, false)
}

// Absorb invalidation messages so stale relcache entries are flushed before
// use; skippable when the lock was already held and cleared.
fn absorb_inval_and_clear(tag: LOCKTAG, lockmode: LOCKMODE, res: LockAcquireResult) -> PgResult<()> {
    if res != LOCKACQUIRE_ALREADY_CLEAR {
        inval_seams::accept_invalidation_messages::call()?;
        lock_seams::mark_lock_clear::call(tag, lockmode);
    }
    Ok(())
}

// Per-open hot at M2. The per-backend fastpath C leans on lives in lock.c's
// LockAcquireExtended (fpInfoLock fast-path slots), not in this layer.
pub fn LockRelationOid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = SetLocktagRelationOid(relid);
    let res = acquire(tag, lockmode, false)?;
    absorb_inval_and_clear(tag, lockmode, res)
}

pub fn ConditionalLockRelationOid(relid: Oid, lockmode: LOCKMODE) -> PgResult<bool> {
    let tag = SetLocktagRelationOid(relid);
    let res = acquire(tag, lockmode, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    absorb_inval_and_clear(tag, lockmode, res)?;
    Ok(true)
}

pub fn LockDatabaseFrozenIds(lockmode: LOCKMODE) -> PgResult<()> {
    let tag = LOCKTAG::database_frozen_ids(init_small::globals::MyDatabaseId());
    acquire(tag, lockmode, false)?;
    Ok(())
}

pub fn LockRelationId(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = LOCKTAG::relation(relid.dbId, relid.relId);
    let res = acquire(tag, lockmode, false)?;
    absorb_inval_and_clear(tag, lockmode, res)
}

pub fn UnlockRelationId(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    lock_seams::lock_release::call(LOCKTAG::relation(relid.dbId, relid.relId), lockmode, false)?;
    Ok(())
}

pub fn UnlockRelationOid(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    lock_seams::lock_release::call(SetLocktagRelationOid(relid), lockmode, false)?;
    Ok(())
}

pub fn LockRelationIdForSession(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = LOCKTAG::relation(relid.dbId, relid.relId);
    lock_seams::lock_acquire_extended::call(tag, lockmode, true, false, true, false)?;
    Ok(())
}

pub fn UnlockRelationIdForSession(relid: &LockRelId, lockmode: LOCKMODE) -> PgResult<()> {
    lock_seams::lock_release::call(LOCKTAG::relation(relid.dbId, relid.relId), lockmode, true)?;
    Ok(())
}

pub fn LockRelation(rel: &RelationData<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    let tag = rel_tag(rel);
    let res = acquire(tag, lockmode, false)?;
    absorb_inval_and_clear(tag, lockmode, res)
}

pub fn ConditionalLockRelation(rel: &RelationData<'_>, lockmode: LOCKMODE) -> PgResult<bool> {
    let tag = rel_tag(rel);
    let res = acquire(tag, lockmode, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    absorb_inval_and_clear(tag, lockmode, res)?;
    Ok(true)
}

pub fn UnlockRelation(rel: &RelationData<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    lock_seams::lock_release::call(rel_tag(rel), lockmode, false)?;
    Ok(())
}

pub fn LockHasWaitersRelation(rel: &RelationData<'_>, lockmode: LOCKMODE) -> PgResult<bool> {
    lock_seams::lock_has_waiters::call(rel_tag(rel), lockmode)
}

#[inline]
fn extend_tag(rel: &RelationData<'_>) -> LOCKTAG {
    LOCKTAG::relation_extend(rel.rd_lockInfo.lockRelId.dbId, rel.rd_lockInfo.lockRelId.relId)
}

pub fn LockRelationForExtension(rel: &RelationData<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    lock_seams::lock_acquire_extended::call(extend_tag(rel), lockmode, false, false, true, false)?;
    Ok(())
}

pub fn UnlockRelationForExtension(rel: &RelationData<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    lock_seams::lock_release::call(extend_tag(rel), lockmode, false)?;
    Ok(())
}

pub fn CheckRelationLockedByMe(
    rel: &RelationData<'_>,
    lockmode: LOCKMODE,
    orstronger: bool,
) -> bool {
    lock_seams::lock_held_by_me::call(rel_tag(rel), lockmode, orstronger)
}

pub fn CheckRelationOidLockedByMe(relid: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool {
    lock_seams::lock_held_by_me::call(SetLocktagRelationOid(relid), lockmode, orstronger)
}

#[inline]
fn tuple_tag(rel: &RelationData<'_>, tid: &ItemPointerData) -> LOCKTAG {
    LOCKTAG::tuple(
        rel.rd_lockInfo.lockRelId.dbId,
        rel.rd_lockInfo.lockRelId.relId,
        ItemPointerGetBlockNumber(tid),
        ItemPointerGetOffsetNumber(tid),
    )
}

#[inline]
fn page_tag(rel: &RelationData<'_>, blkno: types_core::BlockNumber) -> LOCKTAG {
    LOCKTAG::page(
        rel.rd_lockInfo.lockRelId.dbId,
        rel.rd_lockInfo.lockRelId.relId,
        blkno,
    )
}

pub fn LockPage(
    rel: &RelationData<'_>,
    blkno: types_core::BlockNumber,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    acquire(page_tag(rel, blkno), lockmode, false)?;
    Ok(())
}

pub fn UnlockPage(
    rel: &RelationData<'_>,
    blkno: types_core::BlockNumber,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    lock_seams::lock_release::call(page_tag(rel, blkno), lockmode, false)?;
    Ok(())
}

pub fn LockTuple(
    rel: &RelationData<'_>,
    tid: &ItemPointerData,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    acquire(tuple_tag(rel, tid), lockmode, false)?;
    Ok(())
}

pub fn ConditionalLockTuple(
    rel: &RelationData<'_>,
    tid: &ItemPointerData,
    lockmode: LOCKMODE,
    log_lock_failure: bool,
) -> PgResult<bool> {
    let res = lock_seams::lock_acquire_extended::call(
        tuple_tag(rel, tid),
        lockmode,
        false,
        true,
        true,
        log_lock_failure,
    )?;
    Ok(res != LOCKACQUIRE_NOT_AVAIL)
}

pub fn UnlockTuple(
    rel: &RelationData<'_>,
    tid: &ItemPointerData,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    lock_seams::lock_release::call(tuple_tag(rel, tid), lockmode, false)?;
    Ok(())
}

pub fn XactLockTableInsert(xid: TransactionId) -> PgResult<()> {
    acquire(LOCKTAG::transaction(xid), ExclusiveLock, false)?;
    Ok(())
}

pub fn XactLockTableDelete(xid: TransactionId) -> PgResult<()> {
    lock_seams::lock_release::call(LOCKTAG::transaction(xid), ExclusiveLock, false)?;
    Ok(())
}

// C keeps this as a static local in lmgr.c; one backend = one thread here.
thread_local! {
    static SPECULATIVE_INSERTION_TOKEN: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

pub fn SpeculativeInsertionLockAcquire(xid: TransactionId) -> PgResult<u32> {
    let token = SPECULATIVE_INSERTION_TOKEN.with(|t| {
        // Zero means no token is held, so skip it on wrap-around.
        let next = t.get().wrapping_add(1).max(1);
        t.set(next);
        next
    });
    acquire(LOCKTAG::speculative_insertion(xid, token), ExclusiveLock, false)?;
    Ok(token)
}

pub fn SpeculativeInsertionLockRelease(xid: TransactionId) -> PgResult<()> {
    let token = SPECULATIVE_INSERTION_TOKEN.with(|t| t.get());
    lock_seams::lock_release::call(
        LOCKTAG::speculative_insertion(xid, token),
        ExclusiveLock,
        false,
    )?;
    Ok(())
}

pub fn SpeculativeInsertionWait(xid: TransactionId, token: u32) -> PgResult<()> {
    debug_assert!(xid != 0);
    debug_assert!(token != 0);
    let tag = LOCKTAG::speculative_insertion(xid, token);
    acquire(tag, ShareLock, false)?;
    lock_seams::lock_release::call(tag, ShareLock, false)?;
    Ok(())
}

// C installs an error_context_stack callback around the wait; per the elog
// stack design, ERROR-path context attaches on propagation instead.
pub fn XactLockTableWait(
    xid: TransactionId,
    rel: Option<&RelationData<'_>>,
    ctid: Option<&ItemPointerData>,
    oper: XLTW_Oper,
) -> PgResult<()> {
    if oper == XLTW_Oper::None {
        return xact_lock_table_wait_loop(xid);
    }
    debug_assert!(rel.is_some());
    debug_assert!(ctid.is_some_and(ItemPointerIsValid));
    xact_lock_table_wait_loop(xid).map_err(|mut e| {
        if let Some(line) = xact_wait_context(rel, ctid, oper) {
            e.add_context_line(line);
        }
        e
    })
}

fn xact_lock_table_wait_loop(mut xid: TransactionId) -> PgResult<()> {
    let mut first = true;
    loop {
        debug_assert!(xid != 0);
        let tag = LOCKTAG::transaction(xid);
        acquire(tag, ShareLock, false)?;
        lock_seams::lock_release::call(tag, ShareLock, false)?;

        if !procarray_seams::transaction_id_is_in_progress::call(xid)? {
            return Ok(());
        }

        // Subxact XID locks vanish when the subxact ends; wait on the topmost
        // parent instead. Same-xid answer = ProcArray-before-locktable race
        // (logical decoding); retry after a short sleep, never the first time.
        if !first {
            check_for_interrupts()?;
            std::thread::sleep(core::time::Duration::from_micros(1000));
        }
        first = false;
        xid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;
    }
}

pub fn ConditionalXactLockTableWait(
    mut xid: TransactionId,
    log_lock_failure: bool,
) -> PgResult<bool> {
    let mut first = true;
    loop {
        debug_assert!(xid != 0);
        let tag = LOCKTAG::transaction(xid);
        let res =
            lock_seams::lock_acquire_extended::call(tag, ShareLock, false, true, true, log_lock_failure)?;
        if res == LOCKACQUIRE_NOT_AVAIL {
            return Ok(false);
        }
        lock_seams::lock_release::call(tag, ShareLock, false)?;

        if !procarray_seams::transaction_id_is_in_progress::call(xid)? {
            return Ok(true);
        }

        if !first {
            check_for_interrupts()?;
            std::thread::sleep(core::time::Duration::from_micros(1000));
        }
        first = false;
        xid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;
    }
}

pub fn LockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = LOCKTAG::object(init_small::globals::MyDatabaseId(), classid, objid, objsubid);
    acquire(tag, lockmode, false)?;
    // Unconditional in C: syscaches must reflect changes we waited out.
    inval_seams::accept_invalidation_messages::call()?;
    Ok(())
}

pub fn ConditionalLockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let tag = LOCKTAG::object(init_small::globals::MyDatabaseId(), classid, objid, objsubid);
    let res = acquire(tag, lockmode, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    absorb_inval_and_clear(tag, lockmode, res)?;
    Ok(true)
}

pub fn UnlockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = LOCKTAG::object(init_small::globals::MyDatabaseId(), classid, objid, objsubid);
    lock_seams::lock_release::call(tag, lockmode, false)?;
    Ok(())
}

pub fn LockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = LOCKTAG::object(types_core::primitive::InvalidOid, classid, objid, objsubid);
    acquire(tag, lockmode, false)?;
    // Unconditional in C: syscaches must reflect changes we waited out.
    inval_seams::accept_invalidation_messages::call()?;
    Ok(())
}

pub fn ConditionalLockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let tag = LOCKTAG::object(types_core::primitive::InvalidOid, classid, objid, objsubid);
    let res = acquire(tag, lockmode, true)?;
    if res == LOCKACQUIRE_NOT_AVAIL {
        return Ok(false);
    }
    absorb_inval_and_clear(tag, lockmode, res)?;
    Ok(true)
}

pub fn UnlockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = LOCKTAG::object(types_core::primitive::InvalidOid, classid, objid, objsubid);
    lock_seams::lock_release::call(tag, lockmode, false)?;
    Ok(())
}

pub fn LockSharedObjectForSession(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = LOCKTAG::object(types_core::primitive::InvalidOid, classid, objid, objsubid);
    lock_seams::lock_acquire_extended::call(tag, lockmode, true, false, true, false)?;
    Ok(())
}

pub fn UnlockSharedObjectForSession(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let tag = LOCKTAG::object(types_core::primitive::InvalidOid, classid, objid, objsubid);
    lock_seams::lock_release::call(tag, lockmode, true)?;
    Ok(())
}

#[cold]
#[inline(never)]
fn xact_wait_context(
    rel: Option<&RelationData<'_>>,
    ctid: Option<&ItemPointerData>,
    oper: XLTW_Oper,
) -> Option<String> {
    let ctid = ctid.filter(|t| ItemPointerIsValid(t))?;
    let rel = rel?;
    let cxt = match oper {
        XLTW_Oper::Update => "while updating tuple ({},{}) in relation \"{}\"",
        XLTW_Oper::Delete => "while deleting tuple ({},{}) in relation \"{}\"",
        XLTW_Oper::Lock => "while locking tuple ({},{}) in relation \"{}\"",
        XLTW_Oper::LockUpdated => {
            "while locking updated version ({},{}) of tuple in relation \"{}\""
        }
        XLTW_Oper::InsertIndex => "while inserting index tuple ({},{}) in relation \"{}\"",
        XLTW_Oper::InsertIndexUnique => {
            "while checking uniqueness of tuple ({},{}) in relation \"{}\""
        }
        XLTW_Oper::FetchUpdated => "while rechecking updated tuple ({},{}) in relation \"{}\"",
        XLTW_Oper::RecheckExclusionConstr => {
            "while checking exclusion constraint on tuple ({},{}) in relation \"{}\""
        }
        XLTW_Oper::None => return None,
    };
    Some(
        cxt.replacen("{}", &ItemPointerGetBlockNumber(ctid).to_string(), 1)
            .replacen("{}", &ItemPointerGetOffsetNumber(ctid).to_string(), 1)
            .replacen("{}", rel.name(), 1),
    )
}

// C's CHECK_FOR_INTERRUPTS() (miscadmin.h): the inline InterruptPending gate,
// then ProcessInterrupts via the tcop seam; a raised cancel/die comes back as
// the Err and unwinds the wait loop (same pattern as nbtree/hash/heapam).
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}
