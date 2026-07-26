#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{fence, Ordering};
use std::sync::OnceLock;

use lmgr_proc::{GetPGProcByNumber, MyProc, ProcGlobal};
use lwlock::{LWLockAcquire, LWLockConditionalAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use mcx::{Mcx, PgVec};
use types_core::{
    FirstNormalTransactionId, FullTransactionId, InvalidLocalTransactionId, InvalidTransactionId,
    ProcNumber, TransactionId, TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
    TransactionIdPrecedesOrEquals, INVALID_PROC_NUMBER,
};
use types_error::{PgError, PgResult, ERRCODE_TOO_MANY_CONNECTIONS, FATAL};
use types_snapshot::SnapshotData;
use types_storage::storage::{
    SyncCell, NUM_AUXILIARY_PROCS, PGPROC, PGPROC_MAX_CACHED_SUBXIDS,
    PROC_AFFECTS_ALL_HORIZONS, PROC_IN_LOGICAL_DECODING, PROC_IN_VACUUM, PROC_IS_AUTOVACUUM,
    PROC_VACUUM_FOR_WRAPAROUND, PROC_VACUUM_STATE_MASK, PROC_XMIN_FLAGS,
};

mod known_assigned;
mod running;
pub use known_assigned::{
    ExpireAllKnownAssignedTransactionIds, ExpireOldKnownAssignedTransactionIds,
    ExpireTreeKnownAssignedTransactionIds, KnownAssignedTransactionIdsIdleMaintenance,
    ProcArrayApplyRecoveryInfo, ProcArrayApplyXidAssignment, ProcArrayInitRecovery,
    RecordKnownAssignedTransactionIds,
};
pub use procarray_seams;
pub use running::{GetRunningTransactionData, RunningTransactions};
pub use types_storage::storage::RunningTransactionsData;

#[cfg(test)]
mod tests;

// lwlocklist.h: PG_LWLOCK(3, XidGen), PG_LWLOCK(4, ProcArray).
pub const XID_GEN_LOCK: usize = 3;
pub const PROC_ARRAY_LOCK: usize = 4;

fn ProcArrayLock() -> &'static lwlock::LWLock {
    lwlock::main_lock(PROC_ARRAY_LOCK)
}

fn XidGenLock() -> &'static lwlock::LWLock {
    lwlock::main_lock(XID_GEN_LOCK)
}

pub struct ProcArrayStruct {
    numProcs: SyncCell<i32>,          // [PAL]
    maxProcs: i32,
    replication_slot_xmin: SyncCell<TransactionId>, // [PAL]
    replication_slot_catalog_xmin: SyncCell<TransactionId>, // [PAL]
    pgprocnos: &'static [SyncCell<i32>], // [PAL]
    // KnownAssignedXids ring (startup process is the only writer). Adds
    // publish head with Release, no lock; readers hold PAL shared and load
    // head with Acquire (C's pg_write_barrier/pg_read_barrier pairing).
    // Removal/compression/num/tail/lastOverflowedXid writes hold PAL exclusive.
    maxKnownAssignedXids: i32,
    numKnownAssignedXids: std::sync::atomic::AtomicI32,
    tailKnownAssignedXids: std::sync::atomic::AtomicI32,
    headKnownAssignedXids: std::sync::atomic::AtomicI32,
    lastOverflowedXid: SyncCell<TransactionId>, // [PAL]
    knownAssignedXids: &'static [std::sync::atomic::AtomicU32],
    knownAssignedXidsValid: &'static [std::sync::atomic::AtomicBool],
}

// SAFETY: SyncCell fields are serialized by ProcArrayLock as documented.
unsafe impl Sync for ProcArrayStruct {}

// varsup.c owns the live TransamVariables instance; this direct-dep
// re-export keeps the GetSnapshotData fastpath seam-free.
pub use varsup::{TransamVariables, TransamVariablesShared};

static PROC_ARRAY: OnceLock<&'static ProcArrayStruct> = OnceLock::new();

fn procArray() -> &'static ProcArrayStruct {
    PROC_ARRAY
        .get()
        .unwrap_or_else(|| panic!("ProcArrayShmemInit has not run"))
}

#[derive(Clone, Copy, Default)]
struct GlobalVisState {
    definitely_needed: FullTransactionId,
    maybe_needed: FullTransactionId,
}

thread_local! {
    // DIVERGENCE: TransactionXmin/RecentXmin are snapmgr.c globals in C; the
    // hot writer is GetSnapshotData(Reuse), so they live here (snapmgr re-exports).
    static TRANSACTION_XMIN: Cell<TransactionId> = const { Cell::new(FirstNormalTransactionId) };
    static RECENT_XMIN: Cell<TransactionId> = const { Cell::new(FirstNormalTransactionId) };
    static CACHED_XID_NOT_IN_PROGRESS: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
    static GLOBAL_VIS_SHARED_RELS: Cell<GlobalVisState> = const { Cell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };
    static GLOBAL_VIS_CATALOG_RELS: Cell<GlobalVisState> = const { Cell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };
    static GLOBAL_VIS_DATA_RELS: Cell<GlobalVisState> = const { Cell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };
    static GLOBAL_VIS_TEMP_RELS: Cell<GlobalVisState> = const { Cell::new(GlobalVisState { definitely_needed: FullTransactionId { value: 0 }, maybe_needed: FullTransactionId { value: 0 } }) };
    static IN_PROGRESS_XIDS: RefCell<Vec<TransactionId>> = const { RefCell::new(Vec::new()) };
}

#[cfg(debug_assertions)]
thread_local! {
    static SNAPSHOT_REUSE_HITS: Cell<u64> = const { Cell::new(0) };
    static SNAPSHOT_FULL_BUILDS: Cell<u64> = const { Cell::new(0) };
    // How many times GetSnapshotDataReuse SPECULATIVELY published MyProc->xmin
    // (store + SeqCst fence) — the publish-then-verify write half. Counted so
    // the pre-check that keeps a doomed reuse attempt off that path is
    // testable; the write half is invisible in the function's return value.
    static SNAPSHOT_REUSE_SPECULATIVE_PUBLISHES: Cell<u64> = const { Cell::new(0) };
}

// PGRUST_SNAPSHOT_REUSE_TRACE=1 (debug builds): one stderr line per snapshot
// build/reuse for the visibility e2e's "reuse engages" assertion.
#[cfg(debug_assertions)]
fn reuse_trace(event: &'static str) {
    thread_local! {
        static ON: Cell<Option<bool>> = const { Cell::new(None) };
    }
    let on = ON.get().unwrap_or_else(|| {
        let v = std::env::var_os("PGRUST_SNAPSHOT_REUSE_TRACE").is_some_and(|v| v == "1");
        ON.set(Some(v));
        v
    });
    if on {
        eprintln!("SNAPREUSE {event}");
    }
}

#[cfg(debug_assertions)]
pub fn snapshot_reuse_hits() -> u64 {
    SNAPSHOT_REUSE_HITS.get()
}

#[cfg(debug_assertions)]
pub fn snapshot_full_builds() -> u64 {
    SNAPSHOT_FULL_BUILDS.get()
}

#[cfg(debug_assertions)]
pub fn snapshot_reuse_speculative_publishes() -> u64 {
    SNAPSHOT_REUSE_SPECULATIVE_PUBLISHES.get()
}

pub fn TransactionXmin() -> TransactionId {
    TRANSACTION_XMIN.get()
}

pub fn RecentXmin() -> TransactionId {
    RECENT_XMIN.get()
}

// For snapmgr's SnapshotResetXmin (pairs with its MyProc->xmin write).
pub fn set_transaction_xmin(xmin: TransactionId) {
    TRANSACTION_XMIN.set(xmin);
}

/// InitWalSender's PROC_AFFECTS_ALL_HORIZONS arm (walsender.c:317): a
/// database-less walsender's advertised xmin delays vacuum in all databases.
pub fn ProcSetStatusFlagAffectsAllHorizons() -> PgResult<()> {
    let my_procno = MyProc().expect("ProcSetStatusFlagAffectsAllHorizons without MyProc");
    let me = GetPGProcByNumber(my_procno);
    debug_assert_eq!(me.xmin.read(), InvalidTransactionId);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno)?;
    let flags = me.statusFlags.load(Relaxed) | PROC_AFFECTS_ALL_HORIZONS;
    me.statusFlags.store(flags, Relaxed);
    ProcGlobal().statusFlags[me.pgxactoff.load(Relaxed) as usize].store(flags, Relaxed);
    LWLockRelease(ProcArrayLock())?;
    Ok(())
}

/// vacuum_rel's lazy-vacuum arm (vacuum.c:2066): announce this backend as a
/// lazy VACUUM so other backends may exclude our xmin when computing their
/// removable-XID horizons — without this, the process whose job is to advance
/// the cluster's horizon instead pins it for the whole vacuum. The wraparound
/// bit is what keeps an emergency anti-wraparound autovacuum from being
/// cancelled by a conflicting lock request (read at lock's waitqueue).
///
/// Two ordering rules, both C's, both load-bearing:
///  * call this BEFORE taking our own snapshot, so our xmin never becomes
///    visible to `ComputeXidHorizons` ahead of the flag that tells it to skip
///    us (C: vacuum.c:2062-2064);
///  * do NOT clear it here. Both flags are in `PROC_VACUUM_STATE_MASK` and are
///    reset with xid/xmin at end of transaction (`ProcArrayEndTransaction`,
///    already ported); clearing earlier could make
///    `GetOldestNonRemovableTransactionId` appear to go backwards.
pub fn ProcSetStatusFlagInVacuum(is_wraparound: bool) -> PgResult<()> {
    let my_procno = MyProc().expect("ProcSetStatusFlagInVacuum without MyProc");
    let me = GetPGProcByNumber(my_procno);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno)?;
    let mut flags = me.statusFlags.load(Relaxed) | PROC_IN_VACUUM;
    if is_wraparound {
        flags |= PROC_VACUUM_FOR_WRAPAROUND;
    }
    me.statusFlags.store(flags, Relaxed);
    ProcGlobal().statusFlags[me.pgxactoff.load(Relaxed) as usize].store(flags, Relaxed);
    LWLockRelease(ProcArrayLock())?;
    Ok(())
}

/// StartupDecodingContext's walsender arm (logical.c:199): outside a
/// transaction, announce this backend as a logical-decoding one — its xmin is
/// enforced via the replication slot, so horizon computation may skip it.
pub fn ProcSetStatusFlagInLogicalDecoding() -> PgResult<()> {
    let my_procno = MyProc().expect("ProcSetStatusFlagInLogicalDecoding without MyProc");
    let me = GetPGProcByNumber(my_procno);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno)?;
    let flags = me.statusFlags.load(Relaxed) | PROC_IN_LOGICAL_DECODING;
    me.statusFlags.store(flags, Relaxed);
    ProcGlobal().statusFlags[me.pgxactoff.load(Relaxed) as usize].store(flags, Relaxed);
    LWLockRelease(ProcArrayLock())?;
    Ok(())
}

/// SnapBuildInitialSnapshot's xmin adoption (snapbuild.c): MyProc->xmin =
/// snap->xmin — changing only our own value needs no lock; the caller has
/// already verified the horizon is enforced.
pub fn ProcArraySetOwnXmin(xmin: TransactionId) {
    if let Some(my_procno) = MyProc() {
        GetPGProcByNumber(my_procno).xmin.value.store(xmin, Relaxed);
    }
    TRANSACTION_XMIN.set(xmin);
}

/// MyProc->xmin as seen by other backends (validity check sites).
pub fn ProcArrayOwnXmin() -> TransactionId {
    match MyProc() {
        Some(my_procno) => GetPGProcByNumber(my_procno).xmin.read(),
        None => InvalidTransactionId,
    }
}

/// ProcArrayInstallRestoredXmin (procarray.c): parallel workers pin their xmin
/// under the leader's; PROC_XMIN_FLAGS propagate so vacuum's horizon reads the
/// value the same way. False = source xact no longer running.
pub fn ProcArrayInstallRestoredXmin(
    xmin: TransactionId,
    source_procno: ProcNumber,
) -> PgResult<bool> {
    debug_assert!(TransactionIdIsNormal(xmin));
    let source = GetPGProcByNumber(source_procno);
    let my_procno = MyProc().expect("ProcArrayInstallRestoredXmin without MyProc");
    let me = GetPGProcByNumber(my_procno);
    let mut result = false;

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno)?;
    let xid = source.xmin.read();
    if source.databaseId.load(Relaxed) == init_small::globals::MyDatabaseId()
        && TransactionIdIsNormal(xid)
        && TransactionIdPrecedesOrEquals(xid, xmin)
    {
        me.xmin.value.store(xmin, Relaxed);
        TRANSACTION_XMIN.set(xmin);
        let flags = (me.statusFlags.load(Relaxed) & !PROC_XMIN_FLAGS)
            | (source.statusFlags.load(Relaxed) & PROC_XMIN_FLAGS);
        me.statusFlags.store(flags, Relaxed);
        ProcGlobal().statusFlags[me.pgxactoff.load(Relaxed) as usize].store(flags, Relaxed);
        result = true;
    }
    LWLockRelease(ProcArrayLock())?;
    Ok(result)
}

pub(crate) fn TransactionIdAdvance(xid: &mut TransactionId) {
    *xid = xid.wrapping_add(1);
    if *xid < FirstNormalTransactionId {
        *xid = FirstNormalTransactionId;
    }
}

fn NormalTransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2));
    (id1.wrapping_sub(id2) as i32) < 0
}

fn TransactionIdOlder(a: TransactionId, b: TransactionId) -> TransactionId {
    if !TransactionIdIsValid(a) {
        return b;
    }
    if !TransactionIdIsValid(b) {
        return a;
    }
    if TransactionIdPrecedes(a, b) {
        return a;
    }
    b
}

fn FullTransactionIdNewer(a: FullTransactionId, b: FullTransactionId) -> FullTransactionId {
    if !a.is_valid() {
        return b;
    }
    if !b.is_valid() {
        return a;
    }
    if a.value > b.value {
        return a;
    }
    b
}

pub(crate) fn FullXidRelativeTo(rel: FullTransactionId, xid: TransactionId) -> FullTransactionId {
    let rel_xid = rel.xid();
    debug_assert!(TransactionIdIsValid(xid));
    debug_assert!(TransactionIdIsValid(rel_xid));
    FullTransactionId::from_u64(
        (rel.value as i64 + (xid.wrapping_sub(rel_xid) as i32) as i64) as u64,
    )
}

fn latest_completed_xid() -> FullTransactionId {
    FullTransactionId::from_u64(TransamVariables().latestCompletedXid.load(Relaxed))
}

pub fn ProcArrayShmemInit() {
    let all_procs = ProcGlobal().allProcs.len();
    // PROCARRAY_MAXPROCS: auxiliary slots never join the array.
    let max_procs = all_procs - NUM_AUXILIARY_PROCS as usize;

    let pgprocnos: &'static [SyncCell<i32>] = (0..max_procs)
        .map(|_| SyncCell::new(-1))
        .collect::<Vec<_>>()
        .leak();

    // TOTAL_MAX_CACHED_SUBXIDS.
    let max_kax = (PGPROC_MAX_CACHED_SUBXIDS + 1) * max_procs;
    let known_assigned_xids: &'static [std::sync::atomic::AtomicU32] = (0..max_kax)
        .map(|_| std::sync::atomic::AtomicU32::new(0))
        .collect::<Vec<_>>()
        .leak();
    let known_assigned_xids_valid: &'static [std::sync::atomic::AtomicBool] = (0..max_kax)
        .map(|_| std::sync::atomic::AtomicBool::new(false))
        .collect::<Vec<_>>()
        .leak();

    let array: &'static ProcArrayStruct = Box::leak(Box::new(ProcArrayStruct {
        numProcs: SyncCell::new(0),
        maxProcs: max_procs as i32,
        replication_slot_xmin: SyncCell::new(InvalidTransactionId),
        replication_slot_catalog_xmin: SyncCell::new(InvalidTransactionId),
        pgprocnos,
        maxKnownAssignedXids: max_kax as i32,
        numKnownAssignedXids: std::sync::atomic::AtomicI32::new(0),
        tailKnownAssignedXids: std::sync::atomic::AtomicI32::new(0),
        headKnownAssignedXids: std::sync::atomic::AtomicI32::new(0),
        lastOverflowedXid: SyncCell::new(InvalidTransactionId),
        knownAssignedXids: known_assigned_xids,
        knownAssignedXidsValid: known_assigned_xids_valid,
    }));

    PROC_ARRAY
        .set(array)
        .unwrap_or_else(|_| panic!("ProcArrayShmemInit called twice"));
}

/// Crash-cycle reset in place (notes/crash-restart-design.md): the empty
/// post-ProcArrayShmemInit image; maxProcs is PGC_POSTMASTER-stable.
pub fn ProcArrayShmemResetAfterCrash() {
    let array = procArray();
    assert_eq!(
        array.maxProcs as usize,
        ProcGlobal().allProcs.len() - NUM_AUXILIARY_PROCS as usize
    );
    array.numProcs.set(0);
    array.replication_slot_xmin.set(InvalidTransactionId);
    array.replication_slot_catalog_xmin.set(InvalidTransactionId);
    for slot in array.pgprocnos.iter() {
        slot.set(-1);
    }
    array.numKnownAssignedXids.store(0, Relaxed);
    array.tailKnownAssignedXids.store(0, Relaxed);
    array.headKnownAssignedXids.store(0, Relaxed);
    array.lastOverflowedXid.set(InvalidTransactionId);
    for v in array.knownAssignedXidsValid.iter() {
        v.store(false, Relaxed);
    }
}

pub fn ProcArraySetReplicationSlotXmin(
    xmin: TransactionId,
    catalog_xmin: TransactionId,
    already_locked: bool,
) -> PgResult<()> {
    let arrayP = procArray();
    if !already_locked {
        LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    }
    arrayP.replication_slot_xmin.set(xmin);
    arrayP.replication_slot_catalog_xmin.set(catalog_xmin);
    if !already_locked {
        LWLockRelease(ProcArrayLock())?;
    }
    Ok(())
}

pub fn ProcArrayGetReplicationSlotXmin() -> PgResult<(TransactionId, TransactionId)> {
    let arrayP = procArray();
    LWLockAcquire(ProcArrayLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
    let xmin = arrayP.replication_slot_xmin.get();
    let catalog_xmin = arrayP.replication_slot_catalog_xmin.get();
    LWLockRelease(ProcArrayLock())?;
    Ok((xmin, catalog_xmin))
}

// Caller must hold ProcArrayLock (C asserts LWLockHeldByMe).
pub fn GetOldestSafeDecodingTransactionId(catalog_only: bool) -> PgResult<TransactionId> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let tv = TransamVariables();
    let recovery_in_progress = transam_xlog_seams::recovery_in_progress::call();

    LWLockAcquire(XidGenLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
    let mut oldest_safe_xid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid();

    let slot_xmin = arrayP.replication_slot_xmin.get();
    if TransactionIdIsValid(slot_xmin) && TransactionIdPrecedes(slot_xmin, oldest_safe_xid) {
        oldest_safe_xid = slot_xmin;
    }

    let slot_catalog_xmin = arrayP.replication_slot_catalog_xmin.get();
    if catalog_only
        && TransactionIdIsValid(slot_catalog_xmin)
        && TransactionIdPrecedes(slot_catalog_xmin, oldest_safe_xid)
    {
        oldest_safe_xid = slot_catalog_xmin;
    }

    if !recovery_in_progress {
        let num_procs = arrayP.numProcs.get() as usize;
        for pgxactoff in 0..num_procs {
            // Fetch xid just once - see GetNewTransactionId.
            let xid = hdr.xids[pgxactoff].read();
            if !TransactionIdIsNormal(xid) {
                continue;
            }
            if TransactionIdPrecedes(xid, oldest_safe_xid) {
                oldest_safe_xid = xid;
            }
        }
    }

    LWLockRelease(XidGenLock())?;
    Ok(oldest_safe_xid)
}

pub fn GetMaxSnapshotXidCount() -> usize {
    procArray().maxProcs as usize
}

pub fn GetMaxSnapshotSubxidCount() -> usize {
    (PGPROC_MAX_CACHED_SUBXIDS + 1) * procArray().maxProcs as usize
}

pub fn ProcArrayAdd(procno: ProcNumber) -> PgResult<()> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let proc = GetPGProcByNumber(procno);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, procno)?;
    LWLockAcquire(XidGenLock(), LW_EXCLUSIVE, procno)?;

    let num_procs = arrayP.numProcs.get();
    if num_procs >= arrayP.maxProcs {
        LWLockRelease(XidGenLock())?;
        LWLockRelease(ProcArrayLock())?;
        return Err(Box::new(
            PgError::new(FATAL, "sorry, too many clients already")
                .with_sqlstate(ERRCODE_TOO_MANY_CONNECTIONS),
        ));
    }

    let mut index = 0usize;
    while index < num_procs as usize {
        let this_procno = arrayP.pgprocnos[index].get();
        debug_assert!(this_procno >= 0);
        debug_assert_eq!(
            hdr.allProcs[this_procno as usize].pgxactoff.load(Relaxed),
            index as i32
        );
        if this_procno > procno {
            break;
        }
        index += 1;
    }

    for i in (index..num_procs as usize).rev() {
        arrayP.pgprocnos[i + 1].set(arrayP.pgprocnos[i].get());
        hdr.xids[i + 1].value.store(hdr.xids[i].read(), Relaxed);
        hdr.subxidStates[i + 1].set(hdr.subxidStates[i].get());
        hdr.statusFlags[i + 1].store(hdr.statusFlags[i].load(Relaxed), Relaxed);
    }

    arrayP.pgprocnos[index].set(procno);
    proc.pgxactoff.store(index as i32, Relaxed);
    hdr.xids[index].value.store(proc.xid.read(), Relaxed);
    hdr.subxidStates[index].set(proc.subxidStatus.get());
    hdr.statusFlags[index].store(proc.statusFlags.load(Relaxed), Relaxed);

    arrayP.numProcs.set(num_procs + 1);

    for i in index + 1..(num_procs + 1) as usize {
        let p = arrayP.pgprocnos[i].get();
        hdr.allProcs[p as usize].pgxactoff.store(i as i32, Relaxed);
    }

    // Release in reversed acquisition order.
    LWLockRelease(XidGenLock())?;
    LWLockRelease(ProcArrayLock())
}

pub fn ProcArrayRemove(procno: ProcNumber, latestXid: TransactionId) -> PgResult<()> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let proc = GetPGProcByNumber(procno);
    let my_procno = MyProc().unwrap_or(procno);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno)?;
    LWLockAcquire(XidGenLock(), LW_EXCLUSIVE, my_procno)?;

    let myoff = proc.pgxactoff.load(Relaxed) as usize;
    let num_procs = arrayP.numProcs.get() as usize;
    debug_assert!(myoff < num_procs);
    debug_assert_eq!(arrayP.pgprocnos[myoff].get(), procno);

    if TransactionIdIsValid(latestXid) {
        debug_assert!(TransactionIdIsValid(hdr.xids[myoff].read()));
        MaintainLatestCompletedXid(latestXid);
        let tv = TransamVariables();
        tv.xactCompletionCount
            .store(tv.xactCompletionCount.load(Relaxed) + 1, Relaxed);
        hdr.xids[myoff].value.store(InvalidTransactionId, Relaxed);
        hdr.subxidStates[myoff].set(Default::default());
    } else {
        debug_assert!(!TransactionIdIsValid(hdr.xids[myoff].read()));
    }

    hdr.statusFlags[myoff].store(0, Relaxed);

    for i in myoff..num_procs - 1 {
        arrayP.pgprocnos[i].set(arrayP.pgprocnos[i + 1].get());
        hdr.xids[i].value.store(hdr.xids[i + 1].read(), Relaxed);
        hdr.subxidStates[i].set(hdr.subxidStates[i + 1].get());
        hdr.statusFlags[i].store(hdr.statusFlags[i + 1].load(Relaxed), Relaxed);
    }
    arrayP.pgprocnos[num_procs - 1].set(-1);
    arrayP.numProcs.set(num_procs as i32 - 1);

    for i in myoff..num_procs - 1 {
        let p = arrayP.pgprocnos[i].get();
        hdr.allProcs[p as usize].pgxactoff.store(i as i32, Relaxed);
    }

    LWLockRelease(XidGenLock())?;
    LWLockRelease(ProcArrayLock())
}

/// (xid, xmin, nsubxid, overflowed); may be out of date arbitrarily quickly.
pub fn ProcNumberGetTransactionIds(
    procNumber: ProcNumber,
) -> (TransactionId, TransactionId, i32, bool) {
    if procNumber < 0 || procNumber as usize >= ProcGlobal().allProcs.len() {
        return (InvalidTransactionId, InvalidTransactionId, 0, false);
    }
    let proc = GetPGProcByNumber(procNumber);
    let myprocno = MyProc().expect("ProcNumberGetTransactionIds requires MyProc");

    LWLockAcquire(ProcArrayLock(), LW_SHARED, myprocno).expect("ProcArrayLock");
    let result = if proc.pid.load(Relaxed) != 0 {
        let substate = proc.subxidStatus.get();
        (
            proc.xid.read(),
            proc.xmin.read(),
            substate.count as i32,
            substate.overflowed,
        )
    } else {
        (InvalidTransactionId, InvalidTransactionId, 0, false)
    };
    LWLockRelease(ProcArrayLock()).expect("ProcArrayLock release");
    result
}

pub fn BackendPidGetProc(pid: i32) -> Option<&'static PGPROC> {
    if pid == 0 {
        return None; // never match dummy PGPROCs
    }
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let myprocno = MyProc().expect("BackendPidGetProc requires MyProc");

    LWLockAcquire(ProcArrayLock(), LW_SHARED, myprocno).expect("ProcArrayLock");
    let mut result = None;
    for index in 0..arrayP.numProcs.get() as usize {
        let proc = &hdr.allProcs[arrayP.pgprocnos[index].get() as usize];
        if proc.pid.load(Relaxed) == pid {
            result = Some(proc);
            break;
        }
    }
    LWLockRelease(ProcArrayLock()).expect("ProcArrayLock release");
    result
}

// BackendXidGetPid (procarray.c): the pid of the backend whose current xid
// is `xid`, or 0 if none. Used by pgrowlocks to name a tuple's locker.
pub fn BackendXidGetPid(xid: TransactionId) -> i32 {
    if !TransactionIdIsValid(xid) {
        return 0; // never match invalid xid
    }
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let myprocno = MyProc().expect("BackendXidGetPid requires MyProc");

    LWLockAcquire(ProcArrayLock(), LW_SHARED, myprocno).expect("ProcArrayLock");
    let mut result = 0;
    for index in 0..arrayP.numProcs.get() as usize {
        if hdr.xids[index].read() == xid {
            let proc = &hdr.allProcs[arrayP.pgprocnos[index].get() as usize];
            result = proc.pid.load(Relaxed);
            break;
        }
    }
    LWLockRelease(ProcArrayLock()).expect("ProcArrayLock release");
    result
}

pub fn ProcArrayEndTransaction(procno: ProcNumber, latestXid: TransactionId) -> PgResult<()> {
    let proc = GetPGProcByNumber(procno);

    if TransactionIdIsValid(latestXid) {
        debug_assert!(TransactionIdIsValid(proc.xid.read()));
        if LWLockConditionalAcquire(ProcArrayLock(), LW_EXCLUSIVE)? {
            ProcArrayEndTransactionInternal(proc, latestXid);
            LWLockRelease(ProcArrayLock())
        } else {
            ProcArrayGroupClearXid(procno, latestXid)
        }
    } else {
        debug_assert!(!TransactionIdIsValid(proc.xid.read()));
        debug_assert_eq!(proc.subxidStatus.get().count, 0);
        debug_assert!(!proc.subxidStatus.get().overflowed);

        proc.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);
        proc.xmin.value.store(InvalidTransactionId, Relaxed);
        proc.delayChkptFlags.store(0, Relaxed);
        proc.recoveryConflictPending.store(false, Relaxed);

        // Avoid unnecessarily dirtying shared cachelines.
        if proc.statusFlags.load(Relaxed) & PROC_VACUUM_STATE_MASK != 0 {
            LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, procno)?;
            let flags = proc.statusFlags.load(Relaxed) & !PROC_VACUUM_STATE_MASK;
            proc.statusFlags.store(flags, Relaxed);
            ProcGlobal().statusFlags[proc.pgxactoff.load(Relaxed) as usize].store(flags, Relaxed);
            LWLockRelease(ProcArrayLock())?;
        }
        Ok(())
    }
}

fn ProcArrayEndTransactionInternal(proc: &PGPROC, latestXid: TransactionId) {
    let hdr = ProcGlobal();
    let pgxactoff = proc.pgxactoff.load(Relaxed) as usize;

    debug_assert!(TransactionIdIsValid(hdr.xids[pgxactoff].read()));
    debug_assert_eq!(hdr.xids[pgxactoff].read(), proc.xid.read());

    hdr.xids[pgxactoff].value.store(InvalidTransactionId, Relaxed);
    proc.xid.value.store(InvalidTransactionId, Relaxed);
    proc.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    proc.delayChkptFlags.store(0, Relaxed);
    proc.recoveryConflictPending.store(false, Relaxed);

    if proc.statusFlags.load(Relaxed) & PROC_VACUUM_STATE_MASK != 0 {
        let flags = proc.statusFlags.load(Relaxed) & !PROC_VACUUM_STATE_MASK;
        proc.statusFlags.store(flags, Relaxed);
        hdr.statusFlags[pgxactoff].store(flags, Relaxed);
    }

    let subxid_status = proc.subxidStatus.get();
    debug_assert_eq!(hdr.subxidStates[pgxactoff].get(), subxid_status);
    if subxid_status.count > 0 || subxid_status.overflowed {
        hdr.subxidStates[pgxactoff].set(Default::default());
        proc.subxidStatus.set(Default::default());
    }

    MaintainLatestCompletedXid(latestXid);

    let tv = TransamVariables();
    tv.xactCompletionCount
        .store(tv.xactCompletionCount.load(Relaxed) + 1, Relaxed);
}

/// `ProcArrayClearTransaction(MyProc)` — PREPARE TRANSACTION's clear: the xid
/// keeps running via the gxact's dummy proc already in the array.
pub fn ProcArrayClearTransaction() -> PgResult<()> {
    let proc = GetPGProcByNumber(MyProc().expect("ProcArrayClearTransaction without MyProc"));
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, MyProc().unwrap())?;

    let hdr = ProcGlobal();
    let pgxactoff = proc.pgxactoff.load(Relaxed) as usize;

    hdr.xids[pgxactoff].value.store(InvalidTransactionId, Relaxed);
    proc.xid.value.store(InvalidTransactionId, Relaxed);
    proc.vxid.lxid.store(InvalidLocalTransactionId, Relaxed);
    proc.xmin.value.store(InvalidTransactionId, Relaxed);
    proc.recoveryConflictPending.store(false, Relaxed);

    debug_assert_eq!(proc.statusFlags.load(Relaxed) & PROC_VACUUM_STATE_MASK, 0);
    debug_assert_eq!(proc.delayChkptFlags.load(Relaxed), 0);

    // GetSnapshotData omits our own xid; without the bump a reused snapshot
    // might not count the prepared transaction as running.
    let tv = TransamVariables();
    tv.xactCompletionCount
        .store(tv.xactCompletionCount.load(Relaxed) + 1, Relaxed);

    let subxid_status = proc.subxidStatus.get();
    debug_assert_eq!(hdr.subxidStates[pgxactoff].get(), subxid_status);
    if subxid_status.count > 0 || subxid_status.overflowed {
        hdr.subxidStates[pgxactoff].set(Default::default());
        proc.subxidStatus.set(Default::default());
    }

    LWLockRelease(ProcArrayLock())?;
    Ok(())
}

fn ProcArrayGroupClearXid(procno: ProcNumber, latestXid: TransactionId) -> PgResult<()> {
    let hdr = ProcGlobal();
    let proc = GetPGProcByNumber(procno);

    debug_assert!(TransactionIdIsValid(proc.xid.read()));

    proc.procArrayGroupMember.store(true, Relaxed);
    proc.procArrayGroupMemberXid.store(latestXid, Relaxed);
    let mut nextidx = hdr.procArrayGroupFirst.value.load(Relaxed);
    loop {
        proc.procArrayGroupNext.value.store(nextidx, Relaxed);
        match hdr.procArrayGroupFirst.value.compare_exchange(
            nextidx,
            procno as u32,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(cur) => nextidx = cur,
        }
    }

    if nextidx != INVALID_PROC_NUMBER as u32 {
        let mut extra_waits = 0;
        loop {
            // PGSemaphoreLock acts as a read barrier.
            lmgr_proc_seams::pg_semaphore_lock::call(procno);
            if !proc.procArrayGroupMember.load(Ordering::Acquire) {
                break;
            }
            extra_waits += 1;
        }
        debug_assert_eq!(
            proc.procArrayGroupNext.value.load(Relaxed),
            INVALID_PROC_NUMBER as u32
        );
        while extra_waits > 0 {
            lmgr_proc_seams::pg_semaphore_unlock::call(procno);
            extra_waits -= 1;
        }
        return Ok(());
    }

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, procno)?;

    let mut nextidx = hdr
        .procArrayGroupFirst
        .value
        .swap(INVALID_PROC_NUMBER as u32, Ordering::AcqRel);
    let wakeidx = nextidx;

    while nextidx != INVALID_PROC_NUMBER as u32 {
        let nextproc = &hdr.allProcs[nextidx as usize];
        ProcArrayEndTransactionInternal(nextproc, nextproc.procArrayGroupMemberXid.load(Relaxed));
        nextidx = nextproc.procArrayGroupNext.value.load(Relaxed);
    }

    LWLockRelease(ProcArrayLock())?;

    let mut wakeidx = wakeidx;
    while wakeidx != INVALID_PROC_NUMBER as u32 {
        let next_procno = wakeidx as ProcNumber;
        let nextproc = &hdr.allProcs[wakeidx as usize];
        wakeidx = nextproc.procArrayGroupNext.value.load(Relaxed);
        nextproc
            .procArrayGroupNext
            .value
            .store(INVALID_PROC_NUMBER as u32, Relaxed);
        // pg_write_barrier: all prior writes visible before the follower runs.
        nextproc.procArrayGroupMember.store(false, Ordering::Release);
        if next_procno != procno {
            lmgr_proc_seams::pg_semaphore_unlock::call(next_procno);
        }
    }
    Ok(())
}

// XidCacheRemoveRunningXids (procarray.c): subxact-abort xid cache
// maintenance. ProcArrayLock exclusive per transam/README.
pub fn XidCacheRemoveRunningXids(
    xid: TransactionId,
    xids: &[TransactionId],
    latestXid: TransactionId,
) -> PgResult<()> {
    debug_assert!(TransactionIdIsValid(xid));
    let my_procno = MyProc().expect("XidCacheRemoveRunningXids without MyProc");
    let proc = GetPGProcByNumber(my_procno);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno)?;

    let hdr = ProcGlobal();
    let pgxactoff = proc.pgxactoff.load(Relaxed) as usize;
    let mysubxidstat = &hdr.subxidStates[pgxactoff];

    let remove_one = |anxid: TransactionId| -> bool {
        let mut status = proc.subxidStatus.get();
        let count = status.count as usize;
        for j in (0..count).rev() {
            // SAFETY: own PGPROC subxid slot; owner-only writes serialized by
            // ProcArrayLock exclusive; readers pair with the Release fence.
            unsafe {
                let cache = proc.subxids.ptr();
                if (*cache).xids[j] == anxid {
                    (*cache).xids[j] = (*cache).xids[count - 1];
                    fence(Ordering::Release); // pg_write_barrier
                    let mut shared = mysubxidstat.get();
                    shared.count -= 1;
                    mysubxidstat.set(shared);
                    status.count -= 1;
                    proc.subxidStatus.set(status);
                    return true;
                }
            }
        }
        false
    };

    // A miss without overflow can happen on repeated invocation during a
    // failed AbortSubTransaction — WARNING, as in C.
    for &anxid in xids.iter().rev() {
        if !remove_one(anxid) && !proc.subxidStatus.get().overflowed {
            let _ = elog::elog(
                types_error::WARNING,
                format!("did not find subXID {anxid} in MyProc"),
            );
        }
    }
    if !remove_one(xid) && !proc.subxidStatus.get().overflowed {
        let _ = elog::elog(
            types_error::WARNING,
            format!("did not find subXID {xid} in MyProc"),
        );
    }

    MaintainLatestCompletedXid(latestXid);
    let tv = TransamVariables();
    tv.xactCompletionCount
        .store(tv.xactCompletionCount.load(Relaxed) + 1, Relaxed);

    LWLockRelease(ProcArrayLock())
}

fn MaintainLatestCompletedXid(latestXid: TransactionId) {
    let tv = TransamVariables();
    let cur_latest = FullTransactionId::from_u64(tv.latestCompletedXid.load(Relaxed));
    debug_assert!(cur_latest.is_valid());
    if TransactionIdPrecedes(cur_latest.xid(), latestXid) {
        tv.latestCompletedXid
            .store(FullXidRelativeTo(cur_latest, latestXid).value, Relaxed);
    }
}

// C's GetSnapshotDataReuse runs under ProcArrayLock (its comment: the lock
// "very likely can be evolved to not need ProcArrayLock held"); this is that
// evolution, lock-free via publish-then-verify. Soundness constraints:
// 1. MyProc->xmin is published BEFORE the xactCompletionCount verify-load
//    (SeqCst fence between them). Completions bump the counter under
//    ProcArrayLock exclusive; ComputeXidHorizons reads xmins under the lock.
//    If our verify-load does not observe a completion's bump then (ARMv8
//    multi-copy atomicity + the fence) the xmin store is globally ordered
//    before that bump, so any horizon computation that counts the completion
//    also observes our xmin and cannot advance past snapshot.xmin. A horizon
//    computed before the completion is bounded by that xact's still-running
//    xid. Either way no row visible under the reused snapshot is removable.
// 2. If MyProc->xmin is already valid it is <= snapshot.xmin (every snapshot
//    built at the same counter has identical xmin; registered older ones are
//    older), so the horizon was never past snapshot.xmin — no publish needed.
// 3. On verify failure a speculative publish is retracted; a transiently
//    published too-old xmin only makes concurrent horizons conservative.
// 4. The counter PRE-CHECK below constraint 1's publish is a filter, not the
//    verify-load: it can only turn a would-be reuse into a full build, never
//    the other way round, so it cannot weaken constraint 1. Constraint 1's
//    store/fence/verify triple is entirely unchanged behind it.
fn GetSnapshotDataReuse(
    snapshot: &mut SnapshotData<'_>,
    my_proc: &PGPROC,
    tv: &TransamVariablesShared,
) -> bool {
    if snapshot.snapXactCompletionCount == 0 {
        return false;
    }

    // PRE-CHECK (constraint 4). Publish-then-verify is only needed for a
    // reuse that can still SUCCEED. This load is not the verify-load: it is a
    // pure filter whose only possible error is a false NEGATIVE (a counter
    // that changes between here and the verify-load), and a false negative
    // just builds the snapshot the hard way — always sound. What it buys is
    // the whole write half: a reuse that is already doomed no longer stores
    // MyProc->xmin, no longer executes a full barrier, and no longer retracts
    // the store. In a write-heavy workload at high client counts the global
    // completion counter moves between essentially every pair of snapshots,
    // so the doomed case IS the common case, and the store lands on a line
    // that ComputeXidHorizons sweeps out of this core's cache — making the
    // barrier pay a coherence round trip for a result that is discarded two
    // instructions later. C takes neither the store nor the fence here at all
    // (procarray.c:2095 GetSnapshotDataReuse checks the counter first, under
    // ProcArrayLock); this restores C's ordering for the miss while keeping
    // the lock-free hit.
    if tv.xactCompletionCount.load(Relaxed) != snapshot.snapXactCompletionCount {
        return false;
    }

    // The fence is only needed when publishing: with xmin already valid (and
    // <= snapshot.xmin, constraint 2) the horizon can never pass snapshot.xmin
    // regardless of what the verify-load observes.
    let published = if !TransactionIdIsValid(my_proc.xmin.read()) {
        my_proc.xmin.value.store(snapshot.xmin, Relaxed);
        fence(Ordering::SeqCst);
        #[cfg(debug_assertions)]
        SNAPSHOT_REUSE_SPECULATIVE_PUBLISHES
            .set(SNAPSHOT_REUSE_SPECULATIVE_PUBLISHES.get() + 1);
        true
    } else {
        false
    };
    if tv.xactCompletionCount.load(Relaxed) != snapshot.snapXactCompletionCount {
        if published {
            my_proc.xmin.value.store(InvalidTransactionId, Relaxed);
        }
        return false;
    }

    // Same count => the running-xid set cannot have changed (transam/README).
    if published {
        TRANSACTION_XMIN.set(snapshot.xmin);
    }
    RECENT_XMIN.set(snapshot.xmin);
    debug_assert!(TransactionIdPrecedesOrEquals(
        TRANSACTION_XMIN.get(),
        RECENT_XMIN.get()
    ));

    snapshot.active_count.set(0);
    snapshot.regd_count.set(0);
    snapshot.copied = false;

    #[cfg(debug_assertions)]
    {
        SNAPSHOT_REUSE_HITS.set(SNAPSHOT_REUSE_HITS.get() + 1);
        reuse_trace("hit");
    }

    true
}

pub fn GetSnapshotData<'m>(snapshot: &mut SnapshotData<'m>, mcx: Mcx<'m>) -> PgResult<()> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let tv = TransamVariables();
    let pa_lock = ProcArrayLock();
    let myprocno = MyProc().expect("GetSnapshotData requires MyProc");
    let my_proc = GetPGProcByNumber(myprocno);

    // First call for this struct: size the arrays once, reuse forever (C shape).
    if snapshot.xip.capacity() == 0 {
        reserve_exact(&mut snapshot.xip, GetMaxSnapshotXidCount(), mcx)?;
        debug_assert_eq!(snapshot.subxip.capacity(), 0);
        reserve_exact(&mut snapshot.subxip, GetMaxSnapshotSubxidCount(), mcx)?;
    }

    if GetSnapshotDataReuse(snapshot, my_proc, tv) {
        snapshot.curcid.set(xact::GetCurrentCommandId(false)?);
        return Ok(());
    }

    LWLockAcquire(pa_lock, LW_SHARED, myprocno)?;

    let latest_completed = FullTransactionId::from_u64(tv.latestCompletedXid.load(Relaxed));
    let mypgxactoff = my_proc.pgxactoff.load(Relaxed) as usize;
    let myxid = hdr.xids[mypgxactoff].read();
    debug_assert_eq!(myxid, my_proc.xid.read());

    let oldestxid = tv.oldestXid.load(Relaxed);
    let cur_xact_completion_count = tv.xactCompletionCount.load(Relaxed);

    let mut xmax = latest_completed.xid();
    TransactionIdAdvance(&mut xmax);
    debug_assert!(TransactionIdIsNormal(xmax));

    let mut xmin = xmax;
    if TransactionIdIsNormal(myxid) && NormalTransactionIdPrecedes(myxid, xmin) {
        xmin = myxid;
    }

    snapshot.takenDuringRecovery = transam_xlog_seams::recovery_in_progress::call();

    let mut count = 0usize;
    let mut subcount = 0usize;
    let mut suboverflowed = false;

    if !snapshot.takenDuringRecovery {
        let num_procs = arrayP.numProcs.get() as usize;
        let xip_ptr = snapshot.xip.as_mut_ptr();
        let subxip_ptr = snapshot.subxip.as_mut_ptr();
        debug_assert!(num_procs <= snapshot.xip.capacity());

        for pgxactoff in 0..num_procs {
            // Fetch xid just once - see GetNewTransactionId.
            let xid = hdr.xids[pgxactoff].read();

            if xid == InvalidTransactionId {
                continue;
            }
            if pgxactoff == mypgxactoff {
                continue;
            }
            debug_assert!(TransactionIdIsNormal(xid));
            if !NormalTransactionIdPrecedes(xid, xmax) {
                continue;
            }

            let status_flags = hdr.statusFlags[pgxactoff].load(Relaxed);
            if status_flags & (PROC_IN_LOGICAL_DECODING | PROC_IN_VACUUM) != 0 {
                continue;
            }

            if NormalTransactionIdPrecedes(xid, xmin) {
                xmin = xid;
            }

            // SAFETY: count < numProcs <= xip capacity, reserved above.
            unsafe { xip_ptr.add(count).write(xid) };
            count += 1;

            if !suboverflowed {
                let substate = hdr.subxidStates[pgxactoff].get();
                if substate.overflowed {
                    suboverflowed = true;
                } else {
                    let nsubxids = substate.count as usize;
                    if nsubxids > 0 {
                        let pgprocno = arrayP.pgprocnos[pgxactoff].get();
                        let proc = &hdr.allProcs[pgprocno as usize];

                        fence(Ordering::Acquire); // pairs with GetNewTransactionId
                        // SAFETY: the owner only appends subxids (never
                        // removes under ProcArrayLock), the count was fetched
                        // once, and subxip has TOTAL_MAX_CACHED_SUBXIDS
                        // capacity; mirrors C's locked memcpy.
                        unsafe {
                            let src = (*proc.subxids.ptr()).xids.as_ptr();
                            core::ptr::copy_nonoverlapping(
                                src,
                                subxip_ptr.add(subcount),
                                nsubxids,
                            );
                        }
                        subcount += nsubxids;
                    }
                }
            }
        }
    } else {
        let subxip_ptr = snapshot.subxip.as_mut_ptr();
        debug_assert!(arrayP.maxKnownAssignedXids as usize <= snapshot.subxip.capacity());
        subcount = known_assigned::KnownAssignedXidsGetAndSetXmin(
            // SAFETY: at most maxKnownAssignedXids entries are produced and
            // subxip has TOTAL_MAX_CACHED_SUBXIDS capacity, reserved above.
            |i, kxid| unsafe { subxip_ptr.add(i).write(kxid) },
            &mut xmin,
            xmax,
        );

        if TransactionIdPrecedesOrEquals(xmin, arrayP.lastOverflowedXid.get()) {
            suboverflowed = true;
        }
    }

    let replication_slot_xmin = arrayP.replication_slot_xmin.get();
    let replication_slot_catalog_xmin = arrayP.replication_slot_catalog_xmin.get();

    if !TransactionIdIsValid(my_proc.xmin.read()) {
        my_proc.xmin.value.store(xmin, Relaxed);
        TRANSACTION_XMIN.set(xmin);
    }

    LWLockRelease(pa_lock)?;

    {
        let oldestfxid = FullXidRelativeTo(latest_completed, oldestxid);
        let def_vis_xid_data = TransactionIdOlder(xmin, replication_slot_xmin);
        let def_vis_xid = TransactionIdOlder(replication_slot_catalog_xmin, def_vis_xid_data);
        let def_vis_fxid = FullXidRelativeTo(latest_completed, def_vis_xid);
        let def_vis_fxid_data = FullXidRelativeTo(latest_completed, def_vis_xid_data);

        GLOBAL_VIS_SHARED_RELS.with(|c| {
            let mut s = c.get();
            s.definitely_needed = FullTransactionIdNewer(def_vis_fxid, s.definitely_needed);
            s.maybe_needed = FullTransactionIdNewer(s.maybe_needed, oldestfxid);
            c.set(s);
        });
        GLOBAL_VIS_CATALOG_RELS.with(|c| {
            let mut s = c.get();
            s.definitely_needed = FullTransactionIdNewer(def_vis_fxid, s.definitely_needed);
            s.maybe_needed = FullTransactionIdNewer(s.maybe_needed, oldestfxid);
            c.set(s);
        });
        GLOBAL_VIS_DATA_RELS.with(|c| {
            let mut s = c.get();
            s.definitely_needed = FullTransactionIdNewer(def_vis_fxid_data, s.definitely_needed);
            s.maybe_needed = FullTransactionIdNewer(s.maybe_needed, oldestfxid);
            c.set(s);
        });
        GLOBAL_VIS_TEMP_RELS.with(|c| {
            let mut s = c.get();
            s.definitely_needed = if TransactionIdIsNormal(myxid) {
                FullXidRelativeTo(latest_completed, myxid)
            } else {
                let mut next = latest_completed;
                next.value += 1; // FullTransactionIdAdvance; epoch carries
                if !TransactionIdIsNormal(next.xid()) {
                    next = FullTransactionId::from_u64(
                        next.value + (FirstNormalTransactionId - next.xid()) as u64,
                    );
                }
                next
            };
            s.maybe_needed = s.definitely_needed;
            c.set(s);
        });
    }

    RECENT_XMIN.set(xmin);
    debug_assert!(TransactionIdPrecedesOrEquals(
        TRANSACTION_XMIN.get(),
        RECENT_XMIN.get()
    ));

    // SAFETY: exactly count/subcount elements were written above.
    unsafe {
        snapshot.xip.set_len(count);
        snapshot.subxip.set_len(subcount);
    }
    snapshot.xmin = xmin;
    snapshot.xmax = xmax;
    snapshot.xcnt = count as u32;
    snapshot.subxcnt = subcount as i32;
    snapshot.suboverflowed = suboverflowed;
    snapshot.snapXactCompletionCount = cur_xact_completion_count;
    snapshot.curcid.set(xact::GetCurrentCommandId(false)?);
    snapshot.active_count.set(0);
    snapshot.regd_count.set(0);
    snapshot.copied = false;

    #[cfg(debug_assertions)]
    {
        SNAPSHOT_FULL_BUILDS.set(SNAPSHOT_FULL_BUILDS.get() + 1);
        reuse_trace("full");
    }

    Ok(())
}

fn reserve_exact<'m>(
    v: &mut PgVec<'m, TransactionId>,
    n: usize,
    _mcx: Mcx<'m>,
) -> PgResult<()> {
    v.try_reserve_exact(n)
        .map_err(|_| Box::new(_mcx.oom(n * core::mem::size_of::<TransactionId>())))
}

// Split so the fast exits stay a frameless leaf with register returns; the
// outlined scan body carries the frame (bench procarray_xidinprog_recent).
#[inline]
pub fn TransactionIdIsInProgress(xid: TransactionId) -> PgResult<bool> {
    // Nothing older than RecentXmin can still be running; this also rejects
    // Invalid/Frozen/Bootstrap ids. Fast exits run before any shared-state
    // resolution (C touches only two globals here).
    if TransactionIdPrecedes(xid, RECENT_XMIN.get()) {
        return Ok(false);
    }

    if CACHED_XID_NOT_IN_PROGRESS.get() == xid {
        return Ok(false);
    }

    if xact_seams::transaction_id_is_current_transaction_id::call(xid) {
        return Ok(true);
    }

    transaction_id_is_in_progress_scan(xid)
}

#[inline(never)]
fn transaction_id_is_in_progress_scan(xid: TransactionId) -> PgResult<bool> {
    let recovery = transam_xlog_seams::recovery_in_progress::call();
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let myprocno = MyProc().expect("TransactionIdIsInProgress requires MyProc");

    IN_PROGRESS_XIDS.with(|cell| {
        let mut xids = cell.borrow_mut();
        xids.clear();
        if xids.capacity() == 0 {
            xids.reserve_exact(if recovery {
                GetMaxSnapshotSubxidCount()
            } else {
                arrayP.maxProcs as usize
            });
        }

        LWLockAcquire(ProcArrayLock(), LW_SHARED, myprocno)?;

        // pgxactoff is ProcArrayLock-domain state ([PAL]) and MUST be read
        // under the lock (C procarray.c reads MyProc->pgxactoff after the
        // acquire): dense-array compaction (ProcArrayAdd/Remove under the
        // exclusive lock) MOVES entries, so a pre-lock sample can alias
        // ANOTHER backend's current slot — the self-skip below then skips a
        // RUNNING transaction and the scan answers a false not-in-progress
        // (found under GL-ELR-XIDWAIT's connection-churn hot-row repro:
        // the wrong answer resurrected updated versions via the
        // xmax-aborted visibility arms and forked the version chain; the
        // stale answer was then pinned by CACHED_XID_NOT_IN_PROGRESS).
        // Stock-latent: reachable with connection churn concurrent to any
        // in-progress consult, independent of ELR.
        let mypgxactoff = GetPGProcByNumber(myprocno).pgxactoff.load(Relaxed);

        let latest_completed = latest_completed_xid().xid();
        if TransactionIdPrecedes(latest_completed, xid) {
            LWLockRelease(ProcArrayLock())?;
            return Ok(true);
        }

        let num_procs = arrayP.numProcs.get() as usize;
        for pgxactoff in 0..num_procs {
            if pgxactoff as i32 == mypgxactoff {
                continue;
            }
            // Fetch xid just once - see GetNewTransactionId.
            let pxid = hdr.xids[pgxactoff].read();
            if !TransactionIdIsValid(pxid) {
                continue;
            }
            if pxid == xid {
                LWLockRelease(ProcArrayLock())?;
                return Ok(true);
            }
            // A main xid younger than the target cannot be its parent.
            if TransactionIdPrecedes(xid, pxid) {
                continue;
            }

            let substate = hdr.subxidStates[pgxactoff].get();
            let pxids = substate.count as usize;
            fence(Ordering::Acquire); // pairs with GetNewTransactionId
            let pgprocno = arrayP.pgprocnos[pgxactoff].get();
            let proc = &hdr.allProcs[pgprocno as usize];
            for j in (0..pxids).rev() {
                // SAFETY: owner-only appends; count fetched before the fence.
                let cxid = unsafe { (*proc.subxids.ptr()).xids[j] };
                if cxid == xid {
                    LWLockRelease(ProcArrayLock())?;
                    return Ok(true);
                }
            }

            if substate.overflowed {
                xids.push(pxid);
            }
        }

        if recovery {
            debug_assert!(xids.is_empty());
            if known_assigned::KnownAssignedXidExists(xid) {
                LWLockRelease(ProcArrayLock())?;
                return Ok(true);
            }
            if TransactionIdPrecedesOrEquals(xid, arrayP.lastOverflowedXid.get()) {
                known_assigned::KnownAssignedXidsGet(|_, kxid| xids.push(kxid), xid);
            }
        }

        LWLockRelease(ProcArrayLock())?;

        if xids.is_empty() {
            CACHED_XID_NOT_IN_PROGRESS.set(xid);
            return Ok(false);
        }

        // An overflowed subxid cache: consult pg_xact, then pg_subtrans.
        if transam_seams::transaction_id_did_abort::call(xid)? {
            CACHED_XID_NOT_IN_PROGRESS.set(xid);
            return Ok(false);
        }

        let topxid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;
        debug_assert!(TransactionIdIsValid(topxid));
        if topxid != xid && xids.contains(&topxid) {
            return Ok(true);
        }

        CACHED_XID_NOT_IN_PROGRESS.set(xid);
        Ok(false)
    })
}

struct ComputeXidHorizonsResult {
    latest_completed: FullTransactionId,
    oldest_considered_running: TransactionId,
    shared_oldest_nonremovable: TransactionId,
    // shared horizon before the slot xmins fold in (hot_standby_feedback
    // reports slot effects separately).
    shared_oldest_nonremovable_raw: TransactionId,
    catalog_oldest_nonremovable: TransactionId,
    data_oldest_nonremovable: TransactionId,
    temp_oldest_nonremovable: TransactionId,
    slot_catalog_xmin: TransactionId,
}

thread_local! {
    static COMPUTE_XID_HORIZONS_RESULT_LAST_XMIN: Cell<TransactionId> =
        const { Cell::new(InvalidTransactionId) };
}

fn ComputeXidHorizons() -> PgResult<ComputeXidHorizonsResult> {
    let in_recovery = transam_xlog_seams::recovery_in_progress::call();
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let my_proc = GetPGProcByNumber(MyProc().expect("ComputeXidHorizons requires MyProc"));
    let my_database_id = init_small::globals::MyDatabaseId();

    LWLockAcquire(ProcArrayLock(), LW_SHARED, MyProc().unwrap())?;

    let latest_completed = latest_completed_xid();
    let mut initial = latest_completed.xid();
    debug_assert!(TransactionIdIsValid(initial));
    TransactionIdAdvance(&mut initial);

    let mut h = ComputeXidHorizonsResult {
        latest_completed,
        oldest_considered_running: initial,
        shared_oldest_nonremovable: initial,
        shared_oldest_nonremovable_raw: initial,
        slot_catalog_xmin: InvalidTransactionId,
        catalog_oldest_nonremovable: InvalidTransactionId,
        data_oldest_nonremovable: initial,
        temp_oldest_nonremovable: if TransactionIdIsValid(my_proc.xid.read()) {
            my_proc.xid.read()
        } else {
            initial
        },
    };

    let slot_xmin = arrayP.replication_slot_xmin.get();
    let slot_catalog_xmin = arrayP.replication_slot_catalog_xmin.get();

    for index in 0..arrayP.numProcs.get() as usize {
        let pgprocno = arrayP.pgprocnos[index].get();
        let proc = &hdr.allProcs[pgprocno as usize];
        let status_flags = hdr.statusFlags[index].load(Relaxed);
        let xid = hdr.xids[index].read();
        let xmin = TransactionIdOlder(proc.xmin.read(), xid);

        if !TransactionIdIsValid(xmin) {
            continue;
        }
        // Never skip a proc for running-ness (C: vacuum/decoding still run).
        h.oldest_considered_running =
            TransactionIdOlder(h.oldest_considered_running, xmin);
        if status_flags & (PROC_IN_VACUUM | PROC_IN_LOGICAL_DECODING) != 0 {
            continue;
        }

        h.shared_oldest_nonremovable =
            TransactionIdOlder(h.shared_oldest_nonremovable, xmin);
        if proc.databaseId.load(Relaxed) == my_database_id
            || my_database_id == types_core::InvalidOid
            || (status_flags & PROC_AFFECTS_ALL_HORIZONS) != 0
            || in_recovery
        {
            h.data_oldest_nonremovable = TransactionIdOlder(h.data_oldest_nonremovable, xmin);
        }
    }

    let kaxmin = if in_recovery {
        known_assigned::KnownAssignedXidsGetOldestXmin()
    } else {
        InvalidTransactionId
    };

    LWLockRelease(ProcArrayLock())?;

    if in_recovery {
        h.oldest_considered_running = TransactionIdOlder(h.oldest_considered_running, kaxmin);
        h.shared_oldest_nonremovable =
            TransactionIdOlder(h.shared_oldest_nonremovable, kaxmin);
        h.data_oldest_nonremovable = TransactionIdOlder(h.data_oldest_nonremovable, kaxmin);
    }

    h.shared_oldest_nonremovable_raw = h.shared_oldest_nonremovable;
    h.slot_catalog_xmin = slot_catalog_xmin;
    h.shared_oldest_nonremovable =
        TransactionIdOlder(h.shared_oldest_nonremovable, slot_xmin);
    h.data_oldest_nonremovable = TransactionIdOlder(h.data_oldest_nonremovable, slot_xmin);
    h.shared_oldest_nonremovable =
        TransactionIdOlder(h.shared_oldest_nonremovable, slot_catalog_xmin);
    h.catalog_oldest_nonremovable =
        TransactionIdOlder(h.data_oldest_nonremovable, slot_catalog_xmin);
    h.oldest_considered_running =
        TransactionIdOlder(h.oldest_considered_running, h.shared_oldest_nonremovable);
    h.oldest_considered_running =
        TransactionIdOlder(h.oldest_considered_running, h.catalog_oldest_nonremovable);
    h.oldest_considered_running =
        TransactionIdOlder(h.oldest_considered_running, h.data_oldest_nonremovable);

    GlobalVisUpdateApply(&h);
    Ok(h)
}

fn GlobalVisUpdateApply(h: &ComputeXidHorizonsResult) {
    let apply = |cell: &Cell<GlobalVisState>, nonremovable: TransactionId, temp: bool| {
        let mut s = cell.get();
        s.maybe_needed = FullXidRelativeTo(h.latest_completed, nonremovable);
        s.definitely_needed = if temp {
            s.maybe_needed
        } else {
            FullTransactionIdNewer(s.maybe_needed, s.definitely_needed)
        };
        cell.set(s);
    };
    GLOBAL_VIS_SHARED_RELS.with(|c| apply(c, h.shared_oldest_nonremovable, false));
    GLOBAL_VIS_CATALOG_RELS.with(|c| apply(c, h.catalog_oldest_nonremovable, false));
    GLOBAL_VIS_DATA_RELS.with(|c| apply(c, h.data_oldest_nonremovable, false));
    GLOBAL_VIS_TEMP_RELS.with(|c| apply(c, h.temp_oldest_nonremovable, true));
    COMPUTE_XID_HORIZONS_RESULT_LAST_XMIN.set(RECENT_XMIN.get());
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GlobalVisHorizonKind {
    Shared = 1,
    Catalog = 2,
    Data = 3,
    Temp = 4,
}

fn GlobalVisHorizonKindForRel(rel: &types_rel::RelationData<'_>) -> GlobalVisHorizonKind {
    if rel.rd_rel.relisshared || transam_xlog_seams::recovery_in_progress::call() {
        GlobalVisHorizonKind::Shared
    // C also classifies RelationIsAccessibleInLogicalDecoding rels as catalog;
    // that predicate is false while wal_level < logical (the only shipped level).
    } else if catalog_seams::is_catalog_relation::call(rel) {
        GlobalVisHorizonKind::Catalog
    } else if !(rel.rd_islocaltemp || rel.rd_createSubid.get() != types_core::InvalidSubTransactionId)
    {
        GlobalVisHorizonKind::Data
    } else {
        GlobalVisHorizonKind::Temp
    }
}

// C's rel == NULL arm (GlobalVisHorizonKindForRel(NULL) == VISHORIZON_SHARED).
pub fn GetOldestNonRemovableTransactionIdShared() -> PgResult<TransactionId> {
    Ok(ComputeXidHorizons()?.shared_oldest_nonremovable)
}

/// GetReplicationHorizons (procarray.c): (xmin, catalog_xmin) for a hot
/// standby feedback message.
pub fn GetReplicationHorizons() -> PgResult<(TransactionId, TransactionId)> {
    let h = ComputeXidHorizons()?;
    Ok((h.shared_oldest_nonremovable_raw, h.slot_catalog_xmin))
}

pub fn GetOldestNonRemovableTransactionId(
    rel: &types_rel::RelationData<'_>,
) -> PgResult<TransactionId> {
    let h = ComputeXidHorizons()?;
    Ok(match GlobalVisHorizonKindForRel(rel) {
        GlobalVisHorizonKind::Shared => h.shared_oldest_nonremovable,
        GlobalVisHorizonKind::Catalog => h.catalog_oldest_nonremovable,
        GlobalVisHorizonKind::Data => h.data_oldest_nonremovable,
        GlobalVisHorizonKind::Temp => h.temp_oldest_nonremovable,
    })
}

fn vis_state_cell<R>(handle: types_core::GlobalVisStateHandle, f: impl FnOnce(&Cell<GlobalVisState>) -> R) -> R {
    match handle.id {
        1 => GLOBAL_VIS_SHARED_RELS.with(f),
        2 => GLOBAL_VIS_CATALOG_RELS.with(f),
        3 => GLOBAL_VIS_DATA_RELS.with(f),
        4 => GLOBAL_VIS_TEMP_RELS.with(f),
        id => panic!("invalid GlobalVisStateHandle {id}"),
    }
}

pub fn GlobalVisTestFor(rel: &types_rel::RelationData<'_>) -> types_core::GlobalVisStateHandle {
    let handle = types_core::GlobalVisStateHandle::new(GlobalVisHorizonKindForRel(rel) as u64);
    debug_assert!(vis_state_cell(handle, |c| {
        let s = c.get();
        s.definitely_needed.is_valid() && s.maybe_needed.is_valid()
    }));
    handle
}

fn GlobalVisTestShouldUpdate(state: GlobalVisState) -> bool {
    if !TransactionIdIsValid(COMPUTE_XID_HORIZONS_RESULT_LAST_XMIN.get()) {
        return true;
    }
    if state.maybe_needed.value >= state.definitely_needed.value {
        return false;
    }
    RECENT_XMIN.get() != COMPUTE_XID_HORIZONS_RESULT_LAST_XMIN.get()
}

pub fn GlobalVisTestIsRemovableXid(
    handle: types_core::GlobalVisStateHandle,
    xid: TransactionId,
) -> PgResult<bool> {
    let state = vis_state_cell(handle, |c| c.get());
    let fxid = FullXidRelativeTo(state.definitely_needed, xid);

    if fxid.value < state.maybe_needed.value {
        return Ok(true);
    }
    if fxid.value >= state.definitely_needed.value {
        return Ok(false);
    }
    if GlobalVisTestShouldUpdate(state) {
        ComputeXidHorizons()?;
        let state = vis_state_cell(handle, |c| c.get());
        debug_assert!(fxid.value < state.definitely_needed.value);
        return Ok(fxid.value < state.maybe_needed.value);
    }
    Ok(false)
}

pub fn GlobalVisTestIsRemovableFullXid(
    handle: types_core::GlobalVisStateHandle,
    fxid: FullTransactionId,
) -> PgResult<bool> {
    let state = vis_state_cell(handle, |c| c.get());

    if fxid.value < state.maybe_needed.value {
        return Ok(true);
    }
    if fxid.value >= state.definitely_needed.value {
        return Ok(false);
    }
    if GlobalVisTestShouldUpdate(state) {
        ComputeXidHorizons()?;
        let state = vis_state_cell(handle, |c| c.get());
        debug_assert!(fxid.value < state.definitely_needed.value);
        return Ok(fxid.value < state.maybe_needed.value);
    }
    Ok(false)
}

pub fn GlobalVisCheckRemovableFullXid(
    rel: &types_rel::RelationData<'_>,
    fxid: FullTransactionId,
) -> PgResult<bool> {
    GlobalVisTestIsRemovableFullXid(GlobalVisTestFor(rel), fxid)
}

pub fn GetOldestActiveTransactionId() -> PgResult<TransactionId> {
    debug_assert!(!transam_xlog_seams::recovery_in_progress::call());
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let my_procno = MyProc().expect("no MyProc");

    LWLockAcquire(XidGenLock(), LW_SHARED, my_procno)?;
    let mut oldest_running =
        FullTransactionId::from_u64(TransamVariables().nextXid.load(Relaxed)).xid();
    LWLockRelease(XidGenLock())?;

    LWLockAcquire(ProcArrayLock(), LW_SHARED, my_procno)?;
    for index in 0..arrayP.numProcs.get() as usize {
        let xid = hdr.xids[index].read();
        if !TransactionIdIsNormal(xid) {
            continue;
        }
        if TransactionIdPrecedes(xid, oldest_running) {
            oldest_running = xid;
        }
        // Subtransaction xids never precede their top xid; no subxid walk (C).
    }
    LWLockRelease(ProcArrayLock())?;
    Ok(oldest_running)
}

pub fn GetOldestTransactionIdConsideredRunning() -> PgResult<TransactionId> {
    Ok(ComputeXidHorizons()?.oldest_considered_running)
}

// Seam shape folds C's GetVirtualXIDsDelayingChkpt snapshot + the
// HaveVirtualXIDsDelayingChkpt recheck into one "any current holder" probe.
pub fn HaveVirtualXIDsDelayingChkpt(delay_type: i32) -> bool {
    debug_assert!(delay_type != 0);
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let my_procno = MyProc().expect("no MyProc");

    LWLockAcquire(ProcArrayLock(), LW_SHARED, my_procno)
        .expect("ProcArrayLock for HaveVirtualXIDsDelayingChkpt");
    let mut result = false;
    for index in 0..arrayP.numProcs.get() as usize {
        let pgprocno = arrayP.pgprocnos[index].get();
        let proc = &hdr.allProcs[pgprocno as usize];
        if proc.delayChkptFlags.load(Relaxed) & delay_type != 0
            && proc.vxid.lxid.load(Relaxed) != InvalidLocalTransactionId
        {
            result = true;
            break;
        }
    }
    let _ = LWLockRelease(ProcArrayLock());
    result
}

pub fn GetCurrentVirtualXIDs<'mcx>(
    mcx: Mcx<'mcx>,
    limitXmin: TransactionId,
    excludeXmin0: bool,
    allDbs: bool,
    excludeVacuum: u8,
) -> PgResult<PgVec<'mcx, types_core::VirtualTransactionId>> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let my_procno = MyProc().expect("no MyProc");
    let my_dbid = init_small::globals::MyDatabaseId();
    let mut vxids: PgVec<'mcx, types_core::VirtualTransactionId> = PgVec::new_in(mcx);

    LWLockAcquire(ProcArrayLock(), LW_SHARED, my_procno)?;
    for index in 0..arrayP.numProcs.get() as usize {
        let pgprocno = arrayP.pgprocnos[index].get();
        let proc = &hdr.allProcs[pgprocno as usize];

        if pgprocno == my_procno {
            continue;
        }
        if excludeVacuum & hdr.statusFlags[index].load(Relaxed) != 0 {
            continue;
        }
        if allDbs || proc.databaseId.load(Relaxed) == my_dbid {
            // Fetch xmin just once - might change on us.
            let pxmin = proc.xmin.read();
            if excludeXmin0 && !TransactionIdIsValid(pxmin) {
                continue;
            }
            // InvalidTransactionId precedes all other XIDs: a proc that has
            // not set xmin yet is never rejected by this test.
            if !TransactionIdIsValid(limitXmin)
                || TransactionIdPrecedesOrEquals(pxmin, limitXmin)
            {
                let vxid = types_core::VirtualTransactionId {
                    procNumber: proc.vxid.procNumber.load(Relaxed),
                    localTransactionId: proc.vxid.lxid.load(Relaxed),
                };
                if vxid.localTransactionId != InvalidLocalTransactionId {
                    vxids.push(vxid);
                }
            }
        }
    }
    LWLockRelease(ProcArrayLock())?;
    Ok(vxids)
}

// MinimumActiveBackends (procarray.c): count backends (other than myself)
// that are in active transactions; true once the count reaches `min`. Used
// as the heuristic gate for the pre-flush group-commit delay (XLogFlush's
// commit_delay/commit_siblings arm).
//
// C deliberately reads WITHOUT ProcArrayLock: only zero/nonzero tests, and
// the result is heuristic. A concurrent ProcArrayAdd may have bumped
// numProcs before publishing the slot (pgprocnos entry still -1) and a
// concurrent remove leaves a recycled PGPROC whose fields are nonsense;
// both are acceptable here, exactly as in C. Every pgprocnos write is a
// valid proc number or -1, and the i32 loads don't tear, so the -1 check
// is the only garbage filter needed.
pub fn MinimumActiveBackends(min: i32) -> bool {
    // Quick short-circuit if no minimum is specified.
    if min == 0 {
        return true;
    }

    let arrayP = procArray();
    let hdr = ProcGlobal();
    let my_procno = MyProc();
    let mut count = 0;

    for index in 0..arrayP.numProcs.get() as usize {
        let pgprocno = arrayP.pgprocnos[index].get();
        if pgprocno == -1 {
            continue; // do not count deleted entries
        }
        if Some(pgprocno) == my_procno {
            continue; // do not count myself
        }
        let proc = &hdr.allProcs[pgprocno as usize];
        if proc.xid.read() == InvalidTransactionId {
            continue; // do not count if no XID assigned
        }
        if proc.pid.load(Relaxed) == 0 {
            continue; // do not count prepared xacts
        }
        if !proc.waitLock.get().is_null() {
            continue; // do not count if blocked on a lock
        }
        count += 1;
        if count >= min {
            break;
        }
    }

    count >= min
}

pub fn CountDBConnections(databaseid: types_core::Oid) -> PgResult<i32> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let mut count = 0;

    LWLockAcquire(ProcArrayLock(), LW_SHARED, MyProc().expect("no MyProc"))?;
    for index in 0..arrayP.numProcs.get() as usize {
        let pgprocno = arrayP.pgprocnos[index].get();
        let proc = &hdr.allProcs[pgprocno as usize];
        if proc.pid.load(Relaxed) == 0 {
            continue;
        }
        if !proc.isRegularBackend.load(Relaxed) {
            continue;
        }
        if databaseid == types_core::InvalidOid || proc.databaseId.load(Relaxed) == databaseid {
            count += 1;
        }
    }
    LWLockRelease(ProcArrayLock())?;
    Ok(count)
}

// CountUserBackends(roleid) (procarray.c): rolconnlimit enforcement — the
// same dense-array walk as CountDBConnections keyed on roleId. Skips
// prepared xacts (pid == 0) and non-regular backends; counts the calling
// backend too (roleId is stamped by SetAuthenticatedUserId before
// InitializeSessionUserId runs the limit check, so `count > rolconnlimit`
// matches C's off-by-self semantics exactly).
pub fn CountUserBackends(roleid: types_core::Oid) -> PgResult<i32> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let mut count = 0;

    LWLockAcquire(ProcArrayLock(), LW_SHARED, MyProc().expect("no MyProc"))?;
    for index in 0..arrayP.numProcs.get() as usize {
        let pgprocno = arrayP.pgprocnos[index].get();
        let proc = &hdr.allProcs[pgprocno as usize];
        if proc.pid.load(Relaxed) == 0 {
            continue; // do not count prepared xacts
        }
        if !proc.isRegularBackend.load(Relaxed) {
            continue; // do not count background workers
        }
        if proc.roleId.load(Relaxed) == roleid {
            count += 1;
        }
    }
    LWLockRelease(ProcArrayLock())?;
    Ok(count)
}

// C sends SIGTERM to conflicting autovacuum workers each try; no autovacuum
// exists here, so the walk-and-retry loop is kept without the kill step.
pub fn CountOtherDBBackends(databaseid: types_core::Oid) -> PgResult<Option<(i32, i32)>> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let my_procno = MyProc().expect("no MyProc");

    let mut nbackends = 0;
    let mut nprepared = 0;
    for _ in 0..50 {
        postgres_seams::check_for_interrupts::call()?;

        nbackends = 0;
        nprepared = 0;
        let mut found = false;

        LWLockAcquire(ProcArrayLock(), LW_SHARED, my_procno)?;
        for index in 0..arrayP.numProcs.get() as usize {
            let pgprocno = arrayP.pgprocnos[index].get();
            let proc = &hdr.allProcs[pgprocno as usize];
            if proc.databaseId.load(Relaxed) != databaseid {
                continue;
            }
            if pgprocno == my_procno {
                continue;
            }
            found = true;
            if proc.pid.load(Relaxed) == 0 {
                nprepared += 1;
            } else {
                // C SIGTERMs conflicting autovacuum workers here; none exist
                // yet — trip if one ever does rather than wait out the 5s.
                debug_assert!(
                    hdr.statusFlags[index].load(Relaxed) & PROC_IS_AUTOVACUUM == 0,
                    "CountOtherDBBackends: autovacuum SIGTERM step unported"
                );
                nbackends += 1;
            }
        }
        LWLockRelease(ProcArrayLock())?;

        if !found {
            return Ok(None);
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    // Timed out: C returns the counts from the final try, no recount.
    Ok(Some((nbackends, nprepared)))
}

const ROLE_PG_SIGNAL_BACKEND: types_core::Oid = 4200;

pub fn TerminateOtherDBBackends(databaseId: types_core::Oid) -> PgResult<()> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let my_procno = MyProc().expect("no MyProc");

    let mut pids: Vec<i32> = Vec::new();
    let mut nprepared: u64 = 0;

    LWLockAcquire(ProcArrayLock(), LW_SHARED, my_procno)?;
    for index in 0..arrayP.numProcs.get() as usize {
        let pgprocno = arrayP.pgprocnos[index].get();
        let proc = &hdr.allProcs[pgprocno as usize];
        if proc.databaseId.load(Relaxed) != databaseId {
            continue;
        }
        if pgprocno == my_procno {
            continue;
        }
        let pid = proc.pid.load(Relaxed);
        if pid != 0 {
            pids.push(pid);
        } else {
            nprepared += 1;
        }
    }
    LWLockRelease(ProcArrayLock())?;

    if nprepared > 0 {
        let dbname = dbcommands_seams::get_database_name::call(databaseId)?
            .unwrap_or_else(|| panic!("cache lookup failed for database {databaseId}"));
        return Err(elog::ereport(types_error::ERROR)
            .errcode(types_error::ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "database \"{dbname}\" is being used by prepared transactions"
            ))
            .errdetail(if nprepared == 1 {
                format!("There is {nprepared} prepared transaction using the database.")
            } else {
                format!("There are {nprepared} prepared transactions using the database.")
            })
            .into_error()
            .into());
    }

    let user_id = miscinit_seams::get_user_id::call();
    for &pid in &pids {
        let Some(proc) = BackendPidGetProc(pid) else { continue };
        let role_id = proc.roleId.load(Relaxed);

        if superuser_seams::superuser_arg::call(role_id)?
            && !superuser_seams::superuser::call()?
        {
            return Err(elog::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to terminate process".to_string())
                .errdetail(
                    "Only roles with the SUPERUSER attribute may terminate processes of \
                     roles with the SUPERUSER attribute."
                        .to_string(),
                )
                .into_error()
                .into());
        }

        if !acl_seams::has_privs_of_role::call(user_id, role_id)?
            && !acl_seams::has_privs_of_role::call(user_id, ROLE_PG_SIGNAL_BACKEND)?
        {
            return Err(elog::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to terminate process".to_string())
                .errdetail(
                    "Only roles with privileges of the role whose process is being \
                     terminated or with privileges of the \"pg_signal_backend\" role may \
                     terminate this process."
                        .to_string(),
                )
                .into_error()
                .into());
        }
    }

    // wasm32: the wasi libc crate exposes no SIG* names; 15 is SIGTERM in
    // the thread-signal emulation's Linux-numbered space (procsignal wasm arm).
    #[cfg(not(target_family = "wasm"))]
    const SIGTERM: i32 = libc::SIGTERM;
    #[cfg(target_family = "wasm")]
    const SIGTERM: i32 = 15;
    for &pid in &pids {
        if BackendPidGetProc(pid).is_some() {
            let _ = procsignal::SendThreadSignal(pid, SIGTERM);
        }
    }
    Ok(())
}

pub fn init_seams() {
    procarray_seams::proc_array_add::set(ProcArrayAdd);
    procarray_seams::proc_array_remove::set(ProcArrayRemove);
    procarray_seams::proc_array_clear_transaction::set(ProcArrayClearTransaction);
    procarray_seams::proc_array_end_transaction::set(ProcArrayEndTransaction);
    procarray_seams::transaction_id_is_in_progress::set(TransactionIdIsInProgress);
    procarray_seams::xid_cache_remove_running_xids::set(XidCacheRemoveRunningXids);
    procarray_seams::count_db_connections::set(CountDBConnections);
    procarray_seams::count_user_backends::set(CountUserBackends);
    procarray_seams::minimum_active_backends::set(MinimumActiveBackends);
    procarray_seams::get_oldest_active_transaction_id::set(|| {
        GetOldestActiveTransactionId().expect("GetOldestActiveTransactionId")
    });
    procarray_seams::get_oldest_transaction_id_considered_running::set(|| {
        GetOldestTransactionIdConsideredRunning().expect("GetOldestTransactionIdConsideredRunning")
    });
    procarray_seams::have_virtual_xids_delaying_chkpt::set(HaveVirtualXIDsDelayingChkpt);
    procarray_seams::record_known_assigned_transaction_ids::set(RecordKnownAssignedTransactionIds);
    procarray_seams::expire_tree_known_assigned_transaction_ids::set(
        ExpireTreeKnownAssignedTransactionIds,
    );
    procarray_seams::proc_array_apply_xid_assignment::set(ProcArrayApplyXidAssignment);
    procarray_seams::proc_array_init_recovery::set(ProcArrayInitRecovery);
    procarray_seams::proc_array_apply_recovery_info::set(ProcArrayApplyRecoveryInfo);
    procarray_seams::expire_all_known_assigned_transaction_ids::set(
        ExpireAllKnownAssignedTransactionIds,
    );
    procarray_seams::expire_old_known_assigned_transaction_ids::set(
        ExpireOldKnownAssignedTransactionIds,
    );
    procarray_seams::known_assigned_transaction_ids_idle_maintenance::set(
        KnownAssignedTransactionIdsIdleMaintenance,
    );
    // Tests pre-install controllable global-vis fakes; keep them.
    if !procarray_seams::global_vis_test_for::is_installed() {
        procarray_seams::global_vis_test_for::set(GlobalVisTestFor);
    }
    if !procarray_seams::global_vis_test_is_removable_xid::is_installed() {
        procarray_seams::global_vis_test_is_removable_xid::set(GlobalVisTestIsRemovableXid);
    }
    if !procarray_seams::global_vis_check_removable_full_xid::is_installed() {
        procarray_seams::global_vis_check_removable_full_xid::set(GlobalVisCheckRemovableFullXid);
    }
    if !procarray_seams::get_oldest_non_removable_transaction_id::is_installed() {
        procarray_seams::get_oldest_non_removable_transaction_id::set(
            GetOldestNonRemovableTransactionId,
        );
    }
}

// set_indexsafe_procflags (indexcmds.c:4612): mark this backend safe to skip
// in other sessions' concurrent-index snapshot waits; reset at xact end with
// the other status flags. Must run before any xid/xmin is installed.
pub fn SetIndexsafeProcflags() -> PgResult<()> {
    let procno = MyProc().expect("SetIndexsafeProcflags requires MyProc");
    let proc = GetPGProcByNumber(procno);
    debug_assert!(!TransactionIdIsValid(proc.xid.read()));
    debug_assert!(!TransactionIdIsValid(proc.xmin.read()));
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, procno)?;
    let flags = proc.statusFlags.load(Relaxed) | types_storage::storage::PROC_IN_SAFE_IC;
    proc.statusFlags.store(flags, Relaxed);
    ProcGlobal().statusFlags[proc.pgxactoff.load(Relaxed) as usize].store(flags, Relaxed);
    LWLockRelease(ProcArrayLock())?;
    Ok(())
}

fn proc_vxid(proc: &PGPROC) -> types_core::VirtualTransactionId {
    types_core::VirtualTransactionId {
        procNumber: proc.vxid.procNumber.load(Relaxed),
        localTransactionId: proc.vxid.lxid.load(Relaxed),
    }
}

// C mallocs a permanent maxProcs+1 result array; cold path, plain Vec.
pub fn GetConflictingVirtualXIDs(
    limitXmin: TransactionId,
    dbOid: types_core::Oid,
) -> PgResult<Vec<types_core::VirtualTransactionId>> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let mut vxids = Vec::with_capacity(arrayP.maxProcs as usize);

    LWLockAcquire(ProcArrayLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
    for index in 0..arrayP.numProcs.get() as usize {
        let proc = &hdr.allProcs[arrayP.pgprocnos[index].get() as usize];

        // Exclude prepared transactions.
        if proc.pid.load(Relaxed) == 0 {
            continue;
        }

        if dbOid == types_core::InvalidOid || proc.databaseId.load(Relaxed) == dbOid {
            // An invalid pxmin means no snapshot; the share lock keeps new
            // snapshots from conflicting with this test.
            let pxmin = proc.xmin.read();
            if !TransactionIdIsValid(limitXmin)
                || (TransactionIdIsValid(pxmin)
                    && !types_core::TransactionIdFollows(pxmin, limitXmin))
            {
                let vxid = proc_vxid(proc);
                if vxid.is_valid() {
                    vxids.push(vxid);
                }
            }
        }
    }
    LWLockRelease(ProcArrayLock())?;

    Ok(vxids)
}

// Returns the pid of the process signaled, or 0 if not found.
pub fn CancelVirtualTransaction(
    vxid: types_core::VirtualTransactionId,
    sigmode: types_storage::storage::ProcSignalReason,
) -> PgResult<i32> {
    SignalVirtualTransaction(vxid, sigmode, true)
}

pub fn SignalVirtualTransaction(
    vxid: types_core::VirtualTransactionId,
    sigmode: types_storage::storage::ProcSignalReason,
    conflictPending: bool,
) -> PgResult<i32> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let mut pid = 0;

    LWLockAcquire(ProcArrayLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
    for index in 0..arrayP.numProcs.get() as usize {
        let proc = &hdr.allProcs[arrayP.pgprocnos[index].get() as usize];
        let procvxid = proc_vxid(proc);

        if procvxid.procNumber == vxid.procNumber
            && procvxid.localTransactionId == vxid.localTransactionId
        {
            proc.recoveryConflictPending.store(conflictPending, Relaxed);
            pid = proc.pid.load(Relaxed);
            if pid != 0 {
                // Kill the pid if it's still here; if not, that's what we
                // wanted, so ignore any errors.
                procsignal::SendProcSignal(pid, sigmode, procvxid.procNumber);
            }
            break;
        }
    }
    LWLockRelease(ProcArrayLock())?;

    Ok(pid)
}

pub fn CountDBBackends(databaseid: types_core::Oid) -> PgResult<i32> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let mut count = 0;

    LWLockAcquire(ProcArrayLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
    for index in 0..arrayP.numProcs.get() as usize {
        let proc = &hdr.allProcs[arrayP.pgprocnos[index].get() as usize];
        // Do not count prepared xacts.
        if proc.pid.load(Relaxed) == 0 {
            continue;
        }
        if databaseid == types_core::InvalidOid || proc.databaseId.load(Relaxed) == databaseid {
            count += 1;
        }
    }
    LWLockRelease(ProcArrayLock())?;

    Ok(count)
}

pub fn CancelDBBackends(
    databaseid: types_core::Oid,
    sigmode: types_storage::storage::ProcSignalReason,
    conflictPending: bool,
) -> PgResult<()> {
    let arrayP = procArray();
    let hdr = ProcGlobal();

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    for index in 0..arrayP.numProcs.get() as usize {
        let proc = &hdr.allProcs[arrayP.pgprocnos[index].get() as usize];
        if databaseid == types_core::InvalidOid || proc.databaseId.load(Relaxed) == databaseid {
            let procvxid = proc_vxid(proc);
            proc.recoveryConflictPending.store(conflictPending, Relaxed);
            let pid = proc.pid.load(Relaxed);
            if pid != 0 {
                procsignal::SendProcSignal(pid, sigmode, procvxid.procNumber);
            }
        }
    }
    LWLockRelease(ProcArrayLock())?;

    Ok(())
}

/// Run `f` holding ProcArrayLock exclusively (slotsync.c synchronize_one_slot
/// takes it around GetOldestSafeDecodingTransactionId + xmin install).
pub fn with_procarray_lock_exclusive<R>(
    f: impl FnOnce() -> types_error::PgResult<R>,
) -> types_error::PgResult<R> {
    LWLockAcquire(
        ProcArrayLock(),
        LW_EXCLUSIVE,
        init_small::globals::MyProcNumber(),
    )?;
    let r = f();
    LWLockRelease(ProcArrayLock())?;
    r
}
