use std::cell::Cell;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use elog::elog;
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use types_core::{
    FirstNormalFullTransactionId, FirstNormalTransactionId, FullTransactionId,
    InvalidTransactionId, TimestampTz, TransactionId, TransactionIdFollows,
    TransactionIdFollowsOrEquals, TransactionIdIsNormal, TransactionIdIsValid,
    TransactionIdPrecedes, TransactionIdPrecedesOrEquals,
};
use types_error::{ErrorLevel, PgResult, DEBUG1, DEBUG3, DEBUG4, ERROR, LOG};
use types_storage::storage::{
    RunningTransactionsData, SUBXIDS_IN_ARRAY, SUBXIDS_IN_SUBTRANS, SUBXIDS_MISSING,
};
use xlogutils::{STANDBY_INITIALIZED, STANDBY_SNAPSHOT_PENDING, STANDBY_SNAPSHOT_READY};

use crate::{procArray, FullXidRelativeTo, ProcArrayLock, TransactionIdAdvance, TransamVariables};

fn TransactionIdRetreat(xid: &mut TransactionId) {
    loop {
        *xid = xid.wrapping_sub(1);
        if *xid >= FirstNormalTransactionId {
            break;
        }
    }
}

fn FullTransactionIdRetreat(fxid: &mut FullTransactionId) {
    fxid.value -= 1;
    if fxid.value < FirstNormalFullTransactionId.value {
        return;
    }
    while fxid.xid() < FirstNormalTransactionId {
        fxid.value -= 1;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KAXCompressReason {
    NoSpace,
    Prune,
    TransactionEnd,
    StartupProcessIdle,
}
use KAXCompressReason::*;

const KAX_COMPRESS_FREQUENCY: u32 = 128;
const KAX_COMPRESS_IDLE_INTERVAL_MS: i64 = 1000;

thread_local! {
    static TRANSACTION_ENDS_COUNTER: Cell<u32> = const { Cell::new(0) };
    static LAST_COMPRESS_TS: Cell<TimestampTz> = const { Cell::new(0) };
    static LATEST_OBSERVED_XID: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
    static STANDBY_SNAPSHOT_PENDING_XMIN: Cell<TransactionId> =
        const { Cell::new(InvalidTransactionId) };
}

fn my_procno() -> types_core::ProcNumber {
    init_small::globals::MyProcNumber()
}

fn KnownAssignedXidsCompress(reason: KAXCompressReason, have_lock: bool) -> PgResult<()> {
    let pa = procArray();
    // Only the startup process moves head/tail; lock-free reads are safe here.
    let head = pa.headKnownAssignedXids.load(Relaxed);
    let tail = pa.tailKnownAssignedXids.load(Relaxed);
    let num = pa.numKnownAssignedXids.load(Relaxed);
    let nelements = head - tail;

    if nelements == num {
        if reason != NoSpace {
            return Ok(());
        }
    } else if reason == TransactionEnd {
        let counter = TRANSACTION_ENDS_COUNTER.get();
        TRANSACTION_ENDS_COUNTER.set(counter.wrapping_add(1));
        if counter % KAX_COMPRESS_FREQUENCY != 0 {
            return Ok(());
        }
        if nelements < 2 * num {
            return Ok(());
        }
    } else if reason == StartupProcessIdle {
        let last = LAST_COMPRESS_TS.get();
        if last != 0 {
            let compress_after = last + KAX_COMPRESS_IDLE_INTERVAL_MS * 1000;
            if timestamp_seams::get_current_timestamp::call() < compress_after {
                return Ok(());
            }
        }
    }

    if !have_lock {
        LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno())?;
    }

    let mut compress_index = 0usize;
    for i in tail as usize..head as usize {
        if pa.knownAssignedXidsValid[i].load(Relaxed) {
            let v = pa.knownAssignedXids[i].load(Relaxed);
            pa.knownAssignedXids[compress_index].store(v, Relaxed);
            pa.knownAssignedXidsValid[compress_index].store(true, Relaxed);
            compress_index += 1;
        }
    }
    debug_assert_eq!(compress_index as i32, pa.numKnownAssignedXids.load(Relaxed));

    pa.tailKnownAssignedXids.store(0, Relaxed);
    pa.headKnownAssignedXids.store(compress_index as i32, Release);

    if !have_lock {
        LWLockRelease(ProcArrayLock())?;
    }

    LAST_COMPRESS_TS.set(timestamp_seams::get_current_timestamp::call());
    Ok(())
}

pub(crate) fn KnownAssignedXidsAdd(
    from_xid: TransactionId,
    to_xid: TransactionId,
    exclusive_lock: bool,
) -> PgResult<()> {
    debug_assert!(TransactionIdPrecedesOrEquals(from_xid, to_xid));

    let nxids: i32 = if to_xid >= from_xid {
        (to_xid - from_xid + 1) as i32
    } else {
        let mut n = 1i32;
        let mut next_xid = from_xid;
        while TransactionIdPrecedes(next_xid, to_xid) {
            n += 1;
            TransactionIdAdvance(&mut next_xid);
        }
        n
    };

    let pa = procArray();
    let mut head = pa.headKnownAssignedXids.load(Relaxed);
    let tail = pa.tailKnownAssignedXids.load(Relaxed);

    debug_assert!(head >= 0 && head <= pa.maxKnownAssignedXids);
    debug_assert!(tail >= 0 && tail < pa.maxKnownAssignedXids);

    if head > tail {
        let last = pa.knownAssignedXids[(head - 1) as usize].load(Relaxed);
        if TransactionIdFollowsOrEquals(last, from_xid) {
            KnownAssignedXidsDisplay(LOG);
            return elog(ERROR, "out-of-order XID insertion in KnownAssignedXids");
        }
    }

    if head + nxids > pa.maxKnownAssignedXids {
        KnownAssignedXidsCompress(NoSpace, exclusive_lock)?;
        head = pa.headKnownAssignedXids.load(Relaxed);
        if head + nxids > pa.maxKnownAssignedXids {
            return elog(ERROR, "too many KnownAssignedXids");
        }
    }

    let mut next_xid = from_xid;
    for _ in 0..nxids {
        pa.knownAssignedXids[head as usize].store(next_xid, Relaxed);
        pa.knownAssignedXidsValid[head as usize].store(true, Relaxed);
        TransactionIdAdvance(&mut next_xid);
        head += 1;
    }

    pa.numKnownAssignedXids.fetch_add(nxids, Relaxed);
    // Release-publish so shared-lock readers see the slots before the new
    // head; redundant but harmless when the lock is held exclusively.
    pa.headKnownAssignedXids.store(head, Release);
    Ok(())
}

fn KnownAssignedXidsSearch(xid: TransactionId, remove: bool) -> bool {
    let pa = procArray();
    let tail = pa.tailKnownAssignedXids.load(Relaxed);
    // Only the startup process removes entries; readers pair with the
    // Release publish in KnownAssignedXidsAdd.
    let head = if remove {
        pa.headKnownAssignedXids.load(Relaxed)
    } else {
        pa.headKnownAssignedXids.load(Acquire)
    };

    // Invalid entries still hold sorted xids, so the validity bitmap can be
    // ignored during the binary search.
    let mut result_index: i32 = -1;
    let mut first = tail;
    let mut last = head - 1;
    while first <= last {
        let mid_index = (first + last) / 2;
        let mid_xid = pa.knownAssignedXids[mid_index as usize].load(Relaxed);
        if xid == mid_xid {
            result_index = mid_index;
            break;
        } else if TransactionIdPrecedes(xid, mid_xid) {
            last = mid_index - 1;
        } else {
            first = mid_index + 1;
        }
    }

    if result_index < 0 {
        return false;
    }
    if !pa.knownAssignedXidsValid[result_index as usize].load(Relaxed) {
        return false;
    }

    if remove {
        pa.knownAssignedXidsValid[result_index as usize].store(false, Relaxed);
        let num = pa.numKnownAssignedXids.fetch_sub(1, Relaxed) - 1;
        debug_assert!(num >= 0);

        if result_index == tail {
            let mut tail = tail + 1;
            while tail < head && !pa.knownAssignedXidsValid[tail as usize].load(Relaxed) {
                tail += 1;
            }
            if tail >= head {
                pa.headKnownAssignedXids.store(0, Relaxed);
                pa.tailKnownAssignedXids.store(0, Relaxed);
            } else {
                pa.tailKnownAssignedXids.store(tail, Relaxed);
            }
        }
    }

    true
}

pub(crate) fn KnownAssignedXidExists(xid: TransactionId) -> bool {
    debug_assert!(TransactionIdIsValid(xid));
    KnownAssignedXidsSearch(xid, false)
}

fn KnownAssignedXidsRemove(xid: TransactionId) {
    debug_assert!(TransactionIdIsValid(xid));
    let _ = elog(DEBUG4, format!("remove KnownAssignedXid {xid}"));
    let _ = KnownAssignedXidsSearch(xid, true);
}

fn KnownAssignedXidsRemoveTree(xid: TransactionId, subxids: &[TransactionId]) -> PgResult<()> {
    if TransactionIdIsValid(xid) {
        KnownAssignedXidsRemove(xid);
    }
    for &sub in subxids {
        KnownAssignedXidsRemove(sub);
    }
    KnownAssignedXidsCompress(TransactionEnd, true)
}

fn KnownAssignedXidsRemovePreceding(remove_xid: TransactionId) -> PgResult<()> {
    let pa = procArray();

    if !TransactionIdIsValid(remove_xid) {
        let _ = elog(DEBUG4, "removing all KnownAssignedXids");
        pa.numKnownAssignedXids.store(0, Relaxed);
        pa.headKnownAssignedXids.store(0, Relaxed);
        pa.tailKnownAssignedXids.store(0, Relaxed);
        return Ok(());
    }

    let _ = elog(DEBUG4, format!("prune KnownAssignedXids to {remove_xid}"));

    let tail = pa.tailKnownAssignedXids.load(Relaxed);
    let head = pa.headKnownAssignedXids.load(Relaxed);

    let mut count = 0i32;
    for i in tail as usize..head as usize {
        if pa.knownAssignedXidsValid[i].load(Relaxed) {
            let known_xid = pa.knownAssignedXids[i].load(Relaxed);
            if TransactionIdFollowsOrEquals(known_xid, remove_xid) {
                break;
            }
            if !twophase_seams::standby_transaction_id_is_prepared::call(known_xid)? {
                pa.knownAssignedXidsValid[i].store(false, Relaxed);
                count += 1;
            }
        }
    }

    let num = pa.numKnownAssignedXids.fetch_sub(count, Relaxed) - count;
    debug_assert!(num >= 0);

    let mut i = tail;
    while i < head {
        if pa.knownAssignedXidsValid[i as usize].load(Relaxed) {
            break;
        }
        i += 1;
    }
    if i >= head {
        pa.headKnownAssignedXids.store(0, Relaxed);
        pa.tailKnownAssignedXids.store(0, Relaxed);
    } else {
        pa.tailKnownAssignedXids.store(i, Relaxed);
    }

    KnownAssignedXidsCompress(Prune, true)
}

pub(crate) fn KnownAssignedXidsGet(
    store: impl FnMut(usize, TransactionId),
    xmax: TransactionId,
) -> usize {
    let mut xtmp = InvalidTransactionId;
    KnownAssignedXidsGetAndSetXmin(store, &mut xtmp, xmax)
}

pub(crate) fn KnownAssignedXidsGetAndSetXmin(
    mut store: impl FnMut(usize, TransactionId),
    xmin: &mut TransactionId,
    xmax: TransactionId,
) -> usize {
    let pa = procArray();
    let tail = pa.tailKnownAssignedXids.load(Relaxed);
    // Fetch head once; xids added later are >= xmax so irrelevant. Acquire
    // pairs with the Release publish in KnownAssignedXidsAdd.
    let head = pa.headKnownAssignedXids.load(Acquire);

    let mut count = 0usize;
    for i in tail as usize..head as usize {
        if pa.knownAssignedXidsValid[i].load(Relaxed) {
            let known_xid = pa.knownAssignedXids[i].load(Relaxed);
            if count == 0 && TransactionIdPrecedes(known_xid, *xmin) {
                *xmin = known_xid;
            }
            if TransactionIdIsValid(xmax) && TransactionIdFollowsOrEquals(known_xid, xmax) {
                break;
            }
            store(count, known_xid);
            count += 1;
        }
    }
    count
}

pub(crate) fn KnownAssignedXidsGetOldestXmin() -> TransactionId {
    let pa = procArray();
    let tail = pa.tailKnownAssignedXids.load(Relaxed);
    let head = pa.headKnownAssignedXids.load(Acquire);
    for i in tail as usize..head as usize {
        if pa.knownAssignedXidsValid[i].load(Relaxed) {
            return pa.knownAssignedXids[i].load(Relaxed);
        }
    }
    InvalidTransactionId
}

fn KnownAssignedXidsDisplay(trace_level: ErrorLevel) {
    let pa = procArray();
    let tail = pa.tailKnownAssignedXids.load(Relaxed);
    let head = pa.headKnownAssignedXids.load(Acquire);
    let num = pa.numKnownAssignedXids.load(Relaxed);

    let mut buf = String::new();
    let mut nxids = 0i32;
    for i in tail as usize..head as usize {
        if pa.knownAssignedXidsValid[i].load(Relaxed) {
            nxids += 1;
            buf.push_str(&format!("[{}]={} ", i, pa.knownAssignedXids[i].load(Relaxed)));
        }
    }

    let _ = elog(
        trace_level,
        format!("{nxids} KnownAssignedXids (num={num} tail={tail} head={head}) {buf}"),
    );
}

fn KnownAssignedXidsReset() -> PgResult<()> {
    let pa = procArray();
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno())?;
    pa.numKnownAssignedXids.store(0, Relaxed);
    pa.tailKnownAssignedXids.store(0, Relaxed);
    pa.headKnownAssignedXids.store(0, Relaxed);
    LWLockRelease(ProcArrayLock())
}

fn MaintainLatestCompletedXidRecovery(latest_xid: TransactionId) {
    let tv = TransamVariables();
    let cur_latest = FullTransactionId::from_u64(tv.latestCompletedXid.load(Relaxed));
    debug_assert!(TransactionIdIsValid(latest_xid));

    // latestCompletedXid may be uninitialized in recovery; nextXid is safe to
    // read without XidGenLock from the startup process.
    let rel = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
    debug_assert!(rel.is_valid());
    if !cur_latest.is_valid()
        || TransactionIdPrecedes(cur_latest.xid(), latest_xid)
    {
        tv.latestCompletedXid
            .store(FullXidRelativeTo(rel, latest_xid).value, Relaxed);
    }
}

fn increment_xact_completion_count() {
    let tv = TransamVariables();
    tv.xactCompletionCount
        .store(tv.xactCompletionCount.load(Relaxed) + 1, Relaxed);
}

pub fn ProcArrayInitRecovery(initialized_upto_xid: TransactionId) {
    debug_assert_eq!(xlogutils::standby_state(), STANDBY_INITIALIZED);
    debug_assert!(TransactionIdIsNormal(initialized_upto_xid));

    let mut latest = initialized_upto_xid;
    TransactionIdRetreat(&mut latest);
    LATEST_OBSERVED_XID.set(latest);
}

pub fn ProcArrayApplyRecoveryInfo(running: &RunningTransactionsData<'_>) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state() >= STANDBY_INITIALIZED);
    debug_assert!(TransactionIdIsValid(running.nextXid));
    debug_assert!(TransactionIdIsValid(running.oldestRunningXid));
    debug_assert!(TransactionIdIsNormal(running.latestCompletedXid));

    ExpireOldKnownAssignedTransactionIds(running.oldestRunningXid)?;

    let mut advance_next_xid = running.nextXid;
    TransactionIdRetreat(&mut advance_next_xid);
    varsup::AdvanceNextFullTransactionIdPastXid(advance_next_xid)?;

    procarray_seams::standby_release_old_locks::call(running.oldestRunningXid)?;

    if xlogutils::standby_state() == STANDBY_SNAPSHOT_READY {
        return Ok(());
    }

    if xlogutils::standby_state() == STANDBY_SNAPSHOT_PENDING {
        if running.subxid_status != SUBXIDS_MISSING || running.xcnt == 0 {
            KnownAssignedXidsReset()?;
            xlogutils::set_standby_state(STANDBY_INITIALIZED);
        } else {
            if TransactionIdPrecedes(
                STANDBY_SNAPSHOT_PENDING_XMIN.get(),
                running.oldestRunningXid,
            ) {
                xlogutils::set_standby_state(STANDBY_SNAPSHOT_READY);
                let _ = elog(DEBUG1, "recovery snapshots are now enabled");
            } else {
                let _ = elog(
                    DEBUG1,
                    format!(
                        "recovery snapshot waiting for non-overflowed snapshot or until \
                         oldest active xid on standby is at least {} (now {})",
                        STANDBY_SNAPSHOT_PENDING_XMIN.get(),
                        running.oldestRunningXid
                    ),
                );
            }
            return Ok(());
        }
    }

    debug_assert_eq!(xlogutils::standby_state(), STANDBY_INITIALIZED);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno())?;
    let locked = (|| -> PgResult<()> {
        let total = (running.xcnt + running.subxcnt) as usize;
        let mut xids: Vec<TransactionId> = Vec::with_capacity(total);
        for &xid in &running.xids[..total] {
            if transam_seams::transaction_id_did_commit::call(xid)?
                || transam_seams::transaction_id_did_abort::call(xid)?
            {
                continue;
            }
            xids.push(xid);
        }

        if !xids.is_empty() {
            if procArray().numKnownAssignedXids.load(Relaxed) != 0 {
                return elog(ERROR, "KnownAssignedXids is not empty");
            }

            // xidLogicalComparator: RUNNING_XACTS only carries normal xids
            // of one epoch, so plain unsigned order is the modular order.
            xids.sort_unstable();

            for i in 0..xids.len() {
                if i > 0 && xids[i - 1] == xids[i] {
                    let _ = elog(
                        DEBUG1,
                        format!(
                            "found duplicated transaction {} for KnownAssignedXids insertion",
                            xids[i]
                        ),
                    );
                    continue;
                }
                KnownAssignedXidsAdd(xids[i], xids[i], true)?;
            }

            KnownAssignedXidsDisplay(DEBUG3);
        }

        let mut latest = LATEST_OBSERVED_XID.get();
        debug_assert!(TransactionIdIsNormal(latest));
        TransactionIdAdvance(&mut latest);
        while TransactionIdPrecedes(latest, running.nextXid) {
            subtrans_seams::extend_subtrans::call(latest)?;
            TransactionIdAdvance(&mut latest);
        }
        TransactionIdRetreat(&mut latest); // = running->nextXid - 1
        LATEST_OBSERVED_XID.set(latest);

        if running.subxid_status == SUBXIDS_MISSING {
            xlogutils::set_standby_state(STANDBY_SNAPSHOT_PENDING);
            STANDBY_SNAPSHOT_PENDING_XMIN.set(latest);
            procArray().lastOverflowedXid.set(latest);
        } else {
            xlogutils::set_standby_state(STANDBY_SNAPSHOT_READY);
            STANDBY_SNAPSHOT_PENDING_XMIN.set(InvalidTransactionId);
            if running.subxid_status == SUBXIDS_IN_SUBTRANS {
                procArray().lastOverflowedXid.set(latest);
            } else {
                debug_assert_eq!(running.subxid_status, SUBXIDS_IN_ARRAY);
                procArray().lastOverflowedXid.set(InvalidTransactionId);
            }
        }

        MaintainLatestCompletedXidRecovery(running.latestCompletedXid);
        Ok(())
    })();
    LWLockRelease(ProcArrayLock())?;
    locked?;

    KnownAssignedXidsDisplay(DEBUG3);
    if xlogutils::standby_state() == STANDBY_SNAPSHOT_READY {
        let _ = elog(DEBUG1, "recovery snapshots are now enabled");
    } else {
        let _ = elog(
            DEBUG1,
            format!(
                "recovery snapshot waiting for non-overflowed snapshot or until \
                 oldest active xid on standby is at least {} (now {})",
                STANDBY_SNAPSHOT_PENDING_XMIN.get(),
                running.oldestRunningXid
            ),
        );
    }
    Ok(())
}

pub fn ProcArrayApplyXidAssignment(
    topxid: TransactionId,
    subxids: &[TransactionId],
) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state() >= STANDBY_INITIALIZED);

    let max_xid = transam_seams::transaction_id_latest::call(topxid, subxids);

    RecordKnownAssignedTransactionIds(max_xid)?;

    for &sub in subxids {
        subtrans_seams::sub_trans_set_parent::call(sub, topxid)?;
    }

    if xlogutils::standby_state() == STANDBY_INITIALIZED {
        return Ok(());
    }

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno())?;
    let locked = (|| -> PgResult<()> {
        KnownAssignedXidsRemoveTree(InvalidTransactionId, subxids)?;
        let pa = procArray();
        if TransactionIdPrecedes(pa.lastOverflowedXid.get(), max_xid) {
            pa.lastOverflowedXid.set(max_xid);
        }
        Ok(())
    })();
    LWLockRelease(ProcArrayLock())?;
    locked
}

pub fn RecordKnownAssignedTransactionIds(xid: TransactionId) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state() >= STANDBY_INITIALIZED);
    debug_assert!(TransactionIdIsValid(xid));
    let latest_observed = LATEST_OBSERVED_XID.get();
    debug_assert!(TransactionIdIsValid(latest_observed));

    let _ = elog(
        DEBUG4,
        format!("record known xact {xid} latestObservedXid {latest_observed}"),
    );

    if TransactionIdFollows(xid, latest_observed) {
        let mut next_expected_xid = latest_observed;
        while TransactionIdPrecedes(next_expected_xid, xid) {
            TransactionIdAdvance(&mut next_expected_xid);
            subtrans_seams::extend_subtrans::call(next_expected_xid)?;
        }
        debug_assert_eq!(next_expected_xid, xid);

        if xlogutils::standby_state() <= STANDBY_INITIALIZED {
            LATEST_OBSERVED_XID.set(xid);
            return Ok(());
        }

        let mut next_expected_xid = latest_observed;
        TransactionIdAdvance(&mut next_expected_xid);
        KnownAssignedXidsAdd(next_expected_xid, xid, false)?;

        LATEST_OBSERVED_XID.set(xid);
        varsup::AdvanceNextFullTransactionIdPastXid(xid)?;
    }
    Ok(())
}

pub fn ExpireTreeKnownAssignedTransactionIds(
    xid: TransactionId,
    subxids: &[TransactionId],
    max_xid: TransactionId,
) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state() >= STANDBY_INITIALIZED);

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno())?;
    let locked = (|| -> PgResult<()> {
        KnownAssignedXidsRemoveTree(xid, subxids)?;
        MaintainLatestCompletedXidRecovery(max_xid);
        increment_xact_completion_count();
        Ok(())
    })();
    LWLockRelease(ProcArrayLock())?;
    locked
}

pub fn ExpireAllKnownAssignedTransactionIds() -> PgResult<()> {
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno())?;
    let locked = (|| -> PgResult<()> {
        KnownAssignedXidsRemovePreceding(InvalidTransactionId)?;

        let tv = TransamVariables();
        let mut latest_xid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed));
        debug_assert!(latest_xid.is_valid());
        FullTransactionIdRetreat(&mut latest_xid);
        tv.latestCompletedXid.store(latest_xid.value, Relaxed);

        increment_xact_completion_count();

        procArray().lastOverflowedXid.set(InvalidTransactionId);
        Ok(())
    })();
    LWLockRelease(ProcArrayLock())?;
    locked
}

pub fn ExpireOldKnownAssignedTransactionIds(xid: TransactionId) -> PgResult<()> {
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE, my_procno())?;
    let locked = (|| -> PgResult<()> {
        let mut latest_xid = xid;
        TransactionIdRetreat(&mut latest_xid);
        MaintainLatestCompletedXidRecovery(latest_xid);
        increment_xact_completion_count();

        let pa = procArray();
        if TransactionIdPrecedes(pa.lastOverflowedXid.get(), xid) {
            pa.lastOverflowedXid.set(InvalidTransactionId);
        }
        KnownAssignedXidsRemovePreceding(xid)
    })();
    LWLockRelease(ProcArrayLock())?;
    locked
}

pub fn KnownAssignedTransactionIdsIdleMaintenance() {
    KnownAssignedXidsCompress(StartupProcessIdle, false)
        .expect("KnownAssignedTransactionIdsIdleMaintenance");
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub fn kax_counts() -> (i32, i32, i32) {
        let pa = procArray();
        (
            pa.numKnownAssignedXids.load(Relaxed),
            pa.tailKnownAssignedXids.load(Relaxed),
            pa.headKnownAssignedXids.load(Relaxed),
        )
    }

    pub fn kax_reset() {
        let pa = procArray();
        pa.numKnownAssignedXids.store(0, Relaxed);
        pa.tailKnownAssignedXids.store(0, Relaxed);
        pa.headKnownAssignedXids.store(0, Relaxed);
        pa.lastOverflowedXid.set(InvalidTransactionId);
        for v in pa.knownAssignedXidsValid.iter() {
            v.store(false, Relaxed);
        }
        LATEST_OBSERVED_XID.set(InvalidTransactionId);
        STANDBY_SNAPSHOT_PENDING_XMIN.set(InvalidTransactionId);
    }

    pub fn set_latest_observed_xid(xid: TransactionId) {
        LATEST_OBSERVED_XID.set(xid);
    }

    pub fn latest_observed_xid() -> TransactionId {
        LATEST_OBSERVED_XID.get()
    }

    pub fn standby_snapshot_pending_xmin() -> TransactionId {
        STANDBY_SNAPSHOT_PENDING_XMIN.get()
    }

    pub fn last_overflowed_xid() -> TransactionId {
        procArray().lastOverflowedXid.get()
    }

    pub fn set_last_overflowed_xid(xid: TransactionId) {
        procArray().lastOverflowedXid.set(xid);
    }

    pub fn remove(xid: TransactionId) {
        KnownAssignedXidsRemove(xid);
    }

    pub fn remove_tree(xid: TransactionId, subxids: &[TransactionId]) -> PgResult<()> {
        KnownAssignedXidsRemoveTree(xid, subxids)
    }

    pub fn remove_preceding(xid: TransactionId) -> PgResult<()> {
        KnownAssignedXidsRemovePreceding(xid)
    }

    pub fn compress_no_space(have_lock: bool) -> PgResult<()> {
        KnownAssignedXidsCompress(NoSpace, have_lock)
    }

    pub fn get_all(xmax: TransactionId) -> Vec<TransactionId> {
        let mut out = Vec::new();
        KnownAssignedXidsGet(|_, x| out.push(x), xmax);
        out
    }
}
