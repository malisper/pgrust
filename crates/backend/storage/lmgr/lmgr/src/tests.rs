use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::Once;

use types_core::{Oid, TransactionId};
use types_error::{PgError, PgResult};
use types_rel::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, FormData_pg_class, LockInfoData,
    LockRelId, MaxLockMode, RelationData, RowExclusiveLock, ShareLock, LOCKMODE,
    RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use types_storage::lock::{
    LockAcquireResult, DEFAULT_LOCKMETHOD, LOCKACQUIRE_ALREADY_CLEAR, LOCKACQUIRE_ALREADY_HELD,
    LOCKACQUIRE_NOT_AVAIL, LOCKACQUIRE_OK, LOCKTAG, LOCKTAG_OBJECT, LOCKTAG_RELATION,
    LOCKTAG_TRANSACTION, LOCKTAG_TUPLE, USER_LOCKMETHOD, XLTW_Oper,
};
use types_tuple::{ItemPointerData, NameData, TupleDescData};

use crate::*;

const DB: Oid = 5;
const SHARED_REL: Oid = 1260;
const PLAIN_REL: Oid = 16384;

#[derive(Clone, Debug, PartialEq)]
enum Ev {
    Acquire(LOCKTAG, LOCKMODE, bool, bool, bool, bool),
    Release(LOCKTAG, LOCKMODE, bool),
    MarkClear(LOCKTAG, LOCKMODE),
    HeldByMe(LOCKTAG, LOCKMODE, bool),
    Inval,
    InProgress(TransactionId),
    Topmost(TransactionId),
    Cfi,
}

thread_local! {
    static EVENTS: RefCell<Vec<Ev>> = const { RefCell::new(Vec::new()) };
    static ACQUIRE_RESULTS: RefCell<VecDeque<PgResult<LockAcquireResult>>> =
        const { RefCell::new(VecDeque::new()) };
    static HELD: Cell<bool> = const { Cell::new(false) };
    static IN_PROGRESS: RefCell<VecDeque<bool>> = const { RefCell::new(VecDeque::new()) };
    static TOPMOST: RefCell<VecDeque<TransactionId>> = const { RefCell::new(VecDeque::new()) };
    static CFI_RESULTS: RefCell<VecDeque<PgResult<()>>> = const { RefCell::new(VecDeque::new()) };
}

fn log(ev: Ev) {
    EVENTS.with_borrow_mut(|e| e.push(ev));
}

fn take_events() -> Vec<Ev> {
    EVENTS.with_borrow_mut(std::mem::take)
}

fn queue_acquire(results: &[PgResult<LockAcquireResult>]) {
    ACQUIRE_RESULTS.with_borrow_mut(|q| q.extend(results.iter().cloned()));
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        lock_seams::lock_acquire_extended::set(|tag, mode, sess, dw, rme, llf| {
            log(Ev::Acquire(tag, mode, sess, dw, rme, llf));
            ACQUIRE_RESULTS
                .with_borrow_mut(|q| q.pop_front())
                .unwrap_or(Ok(LOCKACQUIRE_OK))
        });
        lock_seams::lock_release::set(|tag, mode, sess| {
            log(Ev::Release(tag, mode, sess));
            Ok(true)
        });
        lock_seams::mark_lock_clear::set(|tag, mode| log(Ev::MarkClear(tag, mode)));
        lock_seams::lock_held_by_me::set(|tag, mode, orstronger| {
            log(Ev::HeldByMe(tag, mode, orstronger));
            HELD.get()
        });
        catalog_seams::is_shared_relation::set(|relid| relid == SHARED_REL);
        inval_seams::accept_invalidation_messages::set(|| {
            log(Ev::Inval);
            Ok(())
        });
        procarray_seams::transaction_id_is_in_progress::set(|xid| {
            log(Ev::InProgress(xid));
            Ok(IN_PROGRESS.with_borrow_mut(|q| q.pop_front()).unwrap_or(false))
        });
        subtrans_seams::sub_trans_get_topmost_transaction::set(|xid| {
            log(Ev::Topmost(xid));
            Ok(TOPMOST.with_borrow_mut(|q| q.pop_front()).unwrap_or(xid))
        });
        postgres_seams::check_for_interrupts::set(|| {
            log(Ev::Cfi);
            CFI_RESULTS.with_borrow_mut(|q| q.pop_front()).unwrap_or(Ok(()))
        });
        crate::init_seams();
    });
    init_small::globals::SetMyDatabaseId(DB);
    init_small::globals::SetInterruptPending(false);
    HELD.set(false);
    take_events();
    ACQUIRE_RESULTS.with_borrow_mut(VecDeque::clear);
    IN_PROGRESS.with_borrow_mut(VecDeque::clear);
    TOPMOST.with_borrow_mut(VecDeque::clear);
    CFI_RESULTS.with_borrow_mut(VecDeque::clear);
}

fn make_rel(mcx: mcx::Mcx<'_>, oid: Oid) -> RelationData<'_> {
    let mut relname = NameData::default();
    relname.namestrcpy("reltest");
    RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: oid, dbId: DB } },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: types_core::RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: std::rc::Rc::new(TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: mcx::PgVec::new_in(mcx),
            attrs: mcx::PgVec::new_in(mcx),
        }),
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily: mcx::PgVec::new_in(mcx),
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: mcx::PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    }
}

fn tag_fields(tag: LOCKTAG) -> (u32, u32, u32, u16, u8, u8) {
    (
        tag.locktag_field1,
        tag.locktag_field2,
        tag.locktag_field3,
        tag.locktag_field4,
        tag.locktag_type,
        tag.locktag_lockmethodid,
    )
}

#[test]
fn set_locktag_encodings_match_lock_h() {
    assert_eq!(tag_fields(LOCKTAG::relation(5, 16384)), (5, 16384, 0, 0, 0, 1));
    assert_eq!(tag_fields(LOCKTAG::relation_extend(5, 16384)), (5, 16384, 0, 0, 1, 1));
    assert_eq!(tag_fields(LOCKTAG::database_frozen_ids(5)), (5, 0, 0, 0, 2, 1));
    assert_eq!(tag_fields(LOCKTAG::page(5, 16384, 7)), (5, 16384, 7, 0, 3, 1));
    assert_eq!(tag_fields(LOCKTAG::tuple(5, 16384, 7, 12)), (5, 16384, 7, 12, 4, 1));
    assert_eq!(tag_fields(LOCKTAG::transaction(724)), (724, 0, 0, 0, 5, 1));
    assert_eq!(tag_fields(LOCKTAG::virtualtransaction(3, 9)), (3, 9, 0, 0, 6, 1));
    assert_eq!(tag_fields(LOCKTAG::speculative_insertion(724, 2)), (724, 2, 0, 0, 7, 1));
    assert_eq!(
        tag_fields(LOCKTAG::object(5, 1259, 16384, 3)),
        (5, 1259, 16384, 3, LOCKTAG_OBJECT as u8, DEFAULT_LOCKMETHOD)
    );
    assert_eq!(tag_fields(LOCKTAG::advisory(1, 2, 3, 4)), (1, 2, 3, 4, 10, USER_LOCKMETHOD));
    assert_eq!(tag_fields(LOCKTAG::apply_transaction(5, 20000, 724, 1)), (5, 20000, 724, 1, 11, 1));
}

#[test]
fn lockmode_single_home_and_reexport() {
    assert_eq!(types_rel::AccessShareLock, types_storage::lock::AccessShareLock);
    assert_eq!(MaxLockMode, AccessExclusiveLock);
    assert_eq!(MaxLockMode, 8);
    let _: types_storage::lock::LOCKMODE = types_rel::NoLock;
}

#[test]
fn lock_relation_oid_absorbs_inval_then_marks_clear() {
    install();
    LockRelationOid(PLAIN_REL, AccessShareLock).unwrap();
    let tag = LOCKTAG::relation(DB, PLAIN_REL);
    assert_eq!(
        take_events(),
        vec![
            Ev::Acquire(tag, AccessShareLock, false, false, true, false),
            Ev::Inval,
            Ev::MarkClear(tag, AccessShareLock),
        ]
    );
}

#[test]
fn lock_relation_oid_already_clear_skips_inval() {
    install();
    queue_acquire(&[Ok(LOCKACQUIRE_ALREADY_CLEAR)]);
    LockRelationOid(PLAIN_REL, AccessShareLock).unwrap();
    assert_eq!(take_events().len(), 1);
}

#[test]
fn shared_relation_tag_uses_invalid_db_oid() {
    install();
    LockRelationOid(SHARED_REL, RowExclusiveLock).unwrap();
    let evs = take_events();
    let Ev::Acquire(tag, ..) = &evs[0] else { panic!("{evs:?}") };
    assert_eq!(tag_fields(*tag), (0, SHARED_REL, 0, 0, LOCKTAG_RELATION as u8, 1));
}

#[test]
fn conditional_lock_relation_oid_not_avail() {
    install();
    queue_acquire(&[Ok(LOCKACQUIRE_NOT_AVAIL)]);
    assert!(!ConditionalLockRelationOid(PLAIN_REL, AccessExclusiveLock).unwrap());
    let evs = take_events();
    assert_eq!(evs.len(), 1);
    let Ev::Acquire(_, _, sess, dont_wait, _, _) = &evs[0] else { panic!() };
    assert!(!sess);
    assert!(*dont_wait);
}

#[test]
fn conditional_lock_relation_oid_acquired_absorbs_inval() {
    install();
    queue_acquire(&[Ok(LOCKACQUIRE_ALREADY_HELD)]);
    assert!(ConditionalLockRelationOid(PLAIN_REL, AccessExclusiveLock).unwrap());
    assert_eq!(take_events().len(), 3);
}

#[test]
fn unlock_relation_oid_releases_transaction_lock() {
    install();
    UnlockRelationOid(PLAIN_REL, AccessShareLock).unwrap();
    assert_eq!(
        take_events(),
        vec![Ev::Release(LOCKTAG::relation(DB, PLAIN_REL), AccessShareLock, false)]
    );
}

#[test]
fn lock_and_unlock_relation_id() {
    install();
    let relid = LockRelId { relId: 999, dbId: 42 };
    LockRelationId(&relid, ShareLock).unwrap();
    UnlockRelationId(&relid, ShareLock).unwrap();
    let tag = LOCKTAG::relation(42, 999);
    assert_eq!(
        take_events(),
        vec![
            Ev::Acquire(tag, ShareLock, false, false, true, false),
            Ev::Inval,
            Ev::MarkClear(tag, ShareLock),
            Ev::Release(tag, ShareLock, false),
        ]
    );
}

#[test]
fn check_relation_oid_locked_by_me_delegates() {
    install();
    HELD.set(true);
    assert!(CheckRelationOidLockedByMe(PLAIN_REL, AccessShareLock, true));
    HELD.set(false);
    assert!(!CheckRelationOidLockedByMe(PLAIN_REL, AccessShareLock, true));
    let evs = take_events();
    assert_eq!(
        evs[0],
        Ev::HeldByMe(LOCKTAG::relation(DB, PLAIN_REL), AccessShareLock, true)
    );
}

#[test]
fn lmgr_seams_route_to_this_crate() {
    install();
    lmgr_seams::lock_relation_oid::call(PLAIN_REL, AccessShareLock).unwrap();
    lmgr_seams::unlock_relation_oid::call(PLAIN_REL, AccessShareLock).unwrap();
    HELD.set(true);
    assert!(lmgr_seams::check_relation_locked_by_me::call(PLAIN_REL, AccessShareLock, true));
    HELD.set(false);
    assert_eq!(take_events().len(), 5);
}

#[test]
fn xact_lock_table_insert_and_delete() {
    install();
    XactLockTableInsert(724).unwrap();
    XactLockTableDelete(724).unwrap();
    let tag = LOCKTAG::transaction(724);
    assert_eq!(tag_fields(tag).4, LOCKTAG_TRANSACTION as u8);
    assert_eq!(
        take_events(),
        vec![
            Ev::Acquire(tag, ExclusiveLock, false, false, true, false),
            Ev::Release(tag, ExclusiveLock, false),
        ]
    );
}

#[test]
fn xact_lock_table_wait_finished_xid() {
    install();
    XactLockTableWait(724, None, None, XLTW_Oper::None).unwrap();
    let tag = LOCKTAG::transaction(724);
    assert_eq!(
        take_events(),
        vec![
            Ev::Acquire(tag, ShareLock, false, false, true, false),
            Ev::Release(tag, ShareLock, false),
            Ev::InProgress(724),
        ]
    );
}

#[test]
fn xact_lock_table_wait_follows_subxact_to_topmost() {
    install();
    IN_PROGRESS.with_borrow_mut(|q| q.extend([true, false]));
    TOPMOST.with_borrow_mut(|q| q.push_back(700));
    XactLockTableWait(724, None, None, XLTW_Oper::None).unwrap();
    let evs = take_events();
    assert_eq!(evs[3], Ev::Topmost(724));
    assert_eq!(evs[4], Ev::Acquire(LOCKTAG::transaction(700), ShareLock, false, false, true, false));
    assert_eq!(evs[6], Ev::InProgress(700));
    assert_eq!(evs.len(), 7);
}

#[test]
fn xact_lock_table_wait_attaches_c_context_on_error() {
    install();
    queue_acquire(&[Err(PgError::error("deadlock detected").into())]);
    let ctid = ItemPointerData::new(3, 7);
    let ctx = mcx::MemoryContext::new("t");
    let rel = make_rel(ctx.mcx(), PLAIN_REL);
    let err = XactLockTableWait(724, Some(&rel), Some(&ctid), XLTW_Oper::Update).unwrap_err();
    assert_eq!(
        err.context.as_deref(),
        Some("while updating tuple (3,7) in relation \"reltest\"")
    );
}

// Born-RED for the retry-arm CFI (lmgr.c XactLockTableWait: the !first arm
// runs CHECK_FOR_INTERRUPTS() and keeps waiting). Same-xid topmost answers
// model the ProcArray-before-locktable race; an interrupt is pending but
// ProcessInterrupts raises nothing (already-serviced/holdoff class), so the
// waiter must wake, service it, and RE-CHECK — not abort.
#[test]
fn xact_wait_retry_arm_services_interrupt_and_retries() {
    install();
    init_small::globals::SetInterruptPending(true);
    IN_PROGRESS.with_borrow_mut(|q| q.extend([true, true, false]));
    XactLockTableWait(724, None, None, XLTW_Oper::None).unwrap();
    init_small::globals::SetInterruptPending(false);
    let evs = take_events();
    // Iteration 1 is the first=true pass (no CFI, C-exact); iteration 2's
    // retry arm consults ProcessInterrupts exactly once; iteration 3 exits
    // on the not-in-progress recheck before reaching the retry arm.
    assert_eq!(evs.iter().filter(|e| **e == Ev::Cfi).count(), 1, "{evs:?}");
    assert_eq!(*evs.last().unwrap(), Ev::InProgress(724), "{evs:?}");
}

// A raised cancel/die in the retry arm must come back as the ERROR channel
// (C: ereport ERROR unwinds the wait), never a panic/abort.
#[test]
fn xact_wait_retry_arm_propagates_cancel_as_error() {
    install();
    init_small::globals::SetInterruptPending(true);
    IN_PROGRESS.with_borrow_mut(|q| q.extend([true, true]));
    CFI_RESULTS.with_borrow_mut(|q| {
        q.push_back(Err(PgError::error("canceling statement due to user request").into()))
    });
    let err = XactLockTableWait(724, None, None, XLTW_Oper::None).unwrap_err();
    init_small::globals::SetInterruptPending(false);
    assert_eq!(err.message(), "canceling statement due to user request");
}

// C's error context callback wraps the whole wait, so a cancel raised from
// the retry arm's CHECK_FOR_INTERRUPTS carries the tuple context line too.
#[test]
fn xact_wait_cancel_during_retry_attaches_wait_context() {
    install();
    init_small::globals::SetInterruptPending(true);
    IN_PROGRESS.with_borrow_mut(|q| q.extend([true, true]));
    CFI_RESULTS.with_borrow_mut(|q| {
        q.push_back(Err(PgError::error("canceling statement due to user request").into()))
    });
    let ctid = ItemPointerData::new(3, 7);
    let ctx = mcx::MemoryContext::new("t");
    let rel = make_rel(ctx.mcx(), PLAIN_REL);
    let err = XactLockTableWait(724, Some(&rel), Some(&ctid), XLTW_Oper::Update).unwrap_err();
    init_small::globals::SetInterruptPending(false);
    assert_eq!(
        err.context.as_deref(),
        Some("while updating tuple (3,7) in relation \"reltest\"")
    );
}

// CHECK_FOR_INTERRUPTS is a macro gated on InterruptPending: with no pending
// interrupt the retry arm must not reach ProcessInterrupts at all.
#[test]
fn xact_wait_retry_arm_skips_process_interrupts_when_none_pending() {
    install();
    IN_PROGRESS.with_borrow_mut(|q| q.extend([true, true, false]));
    XactLockTableWait(724, None, None, XLTW_Oper::None).unwrap();
    let evs = take_events();
    assert!(!evs.contains(&Ev::Cfi), "{evs:?}");
}

// Same retry arm in ConditionalXactLockTableWait (lmgr.c "See
// XactLockTableWait about this case").
#[test]
fn conditional_xact_wait_retry_arm_services_interrupt_and_retries() {
    install();
    init_small::globals::SetInterruptPending(true);
    IN_PROGRESS.with_borrow_mut(|q| q.extend([true, true, false]));
    assert!(ConditionalXactLockTableWait(724, false).unwrap());
    init_small::globals::SetInterruptPending(false);
    let evs = take_events();
    assert_eq!(evs.iter().filter(|e| **e == Ev::Cfi).count(), 1, "{evs:?}");
}

#[test]
fn conditional_xact_wait_retry_arm_propagates_cancel_as_error() {
    install();
    init_small::globals::SetInterruptPending(true);
    IN_PROGRESS.with_borrow_mut(|q| q.extend([true, true]));
    CFI_RESULTS.with_borrow_mut(|q| {
        q.push_back(Err(PgError::error("canceling statement due to user request").into()))
    });
    let err = ConditionalXactLockTableWait(724, false).unwrap_err();
    init_small::globals::SetInterruptPending(false);
    assert_eq!(err.message(), "canceling statement due to user request");
}

#[test]
fn conditional_xact_lock_table_wait_not_avail() {
    install();
    queue_acquire(&[Ok(LOCKACQUIRE_NOT_AVAIL)]);
    assert!(!ConditionalXactLockTableWait(724, true).unwrap());
    let evs = take_events();
    let Ev::Acquire(_, _, _, dont_wait, _, log_failure) = &evs[0] else { panic!() };
    assert!(*dont_wait);
    assert!(*log_failure);
    assert_eq!(evs.len(), 1);
}

#[test]
fn lock_database_object_always_absorbs_inval() {
    install();
    queue_acquire(&[Ok(LOCKACQUIRE_ALREADY_CLEAR)]);
    LockDatabaseObject(1259, PLAIN_REL, 0, AccessExclusiveLock).unwrap();
    let tag = LOCKTAG::object(DB, 1259, PLAIN_REL, 0);
    assert_eq!(
        take_events(),
        vec![
            Ev::Acquire(tag, AccessExclusiveLock, false, false, true, false),
            Ev::Inval,
        ]
    );
    UnlockDatabaseObject(1259, PLAIN_REL, 0, AccessExclusiveLock).unwrap();
    assert_eq!(take_events(), vec![Ev::Release(tag, AccessExclusiveLock, false)]);
}

#[test]
fn tuple_lock_tag_splits_item_pointer() {
    install();
    let ctx = mcx::MemoryContext::new("t");
    let rel = make_rel(ctx.mcx(), PLAIN_REL);
    let tid = ItemPointerData::new(0x0001_0002, 12);
    LockTuple(&rel, &tid, ShareLock).unwrap();
    UnlockTuple(&rel, &tid, ShareLock).unwrap();
    queue_acquire(&[Ok(LOCKACQUIRE_NOT_AVAIL)]);
    assert!(!ConditionalLockTuple(&rel, &tid, ShareLock, true).unwrap());
    let tag = LOCKTAG::tuple(
        rel.rd_lockInfo.lockRelId.dbId,
        rel.rd_lockInfo.lockRelId.relId,
        0x0001_0002,
        12,
    );
    assert_eq!(tag_fields(tag).4, LOCKTAG_TUPLE as u8);
    assert_eq!(
        take_events(),
        vec![
            Ev::Acquire(tag, ShareLock, false, false, true, false),
            Ev::Release(tag, ShareLock, false),
            Ev::Acquire(tag, ShareLock, false, true, true, true),
        ]
    );
}

#[test]
fn relation_init_lock_info_matches_c() {
    install();
    assert_eq!(
        RelationInitLockInfo(PLAIN_REL, false).lockRelId,
        LockRelId { relId: PLAIN_REL, dbId: DB }
    );
    assert_eq!(
        RelationInitLockInfo(SHARED_REL, true).lockRelId,
        LockRelId { relId: SHARED_REL, dbId: 0 }
    );
}

#[test]
fn lockmode_bounds_for_relation_open_assert() {
    assert!(AccessShareLock >= types_rel::NoLock && AccessShareLock <= MaxLockMode);
}

#[test]
fn speculative_insertion_lock_cycle() {
    install();
    let _ = take_events();
    let t1 = SpeculativeInsertionLockAcquire(700).unwrap();
    let t2 = SpeculativeInsertionLockAcquire(700).unwrap();
    assert!(t1 != 0 && t2 == t1 + 1, "monotonic nonzero tokens");
    SpeculativeInsertionLockRelease(700).unwrap();
    SpeculativeInsertionWait(701, t1).unwrap();
    let evs = take_events();
    assert_eq!(
        evs,
        vec![
            Ev::Acquire(LOCKTAG::speculative_insertion(700, t1), ExclusiveLock, false, false, true, false),
            Ev::Acquire(LOCKTAG::speculative_insertion(700, t2), ExclusiveLock, false, false, true, false),
            Ev::Release(LOCKTAG::speculative_insertion(700, t2), ExclusiveLock, false),
            Ev::Acquire(LOCKTAG::speculative_insertion(701, t1), ShareLock, false, false, true, false),
            Ev::Release(LOCKTAG::speculative_insertion(701, t1), ShareLock, false),
        ]
    );
}
