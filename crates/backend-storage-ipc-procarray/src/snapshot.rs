//! F2 — snapshot computation (procarray.c). The hot path `GetSnapshotData` and
//! the running-xacts / replication-slot-xmin / decoding-horizon family.
//!
//! Builds on the F0 shmem model + the F3 horizons. Returns
//! `SnapshotData`/`RunningTransactionsData` (types in `types_snapshot` /
//! `types_storage`).

use mcx::PgVec;
use types_core::{
    FirstNormalTransactionId, InvalidLocalTransactionId, InvalidTransactionId, Oid, ProcNumber,
    TransactionId, TransactionIdIsNormal, TransactionIdIsValid, XLogRecPtr, INVALID_PROC_NUMBER,
};
use types_error::PgResult;
use types_snapshot::SnapshotData;
use types_storage::storage::{
    subxids_array_status, PROC_IN_LOGICAL_DECODING, PROC_IN_VACUUM, PROC_XMIN_FLAGS,
    SUBXIDS_IN_ARRAY, SUBXIDS_IN_SUBTRANS,
};
use types_storage::{
    LWLockMode, RunningTransactionLocksHeld, RunningTransactionsData, VirtualTransactionId,
    PROC_ARRAY_LOCK,
};

use backend_access_transam_transam_seams as transam;
use backend_access_transam_varsup_seams as varsup;
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc;

use crate::shmem_model::{
    FullTransactionIdAdvance, FullTransactionIdNewer, FullXidRelativeTo, TransactionIdOlder,
    GLOBAL_VIS_CATALOG_RELS, GLOBAL_VIS_DATA_RELS, GLOBAL_VIS_SHARED_RELS, GLOBAL_VIS_TEMP_RELS,
    PROC_ARRAY,
};

/// `NormalTransactionIdPrecedes(id1, id2)` (`access/transam.h`) — used inside
/// `GetSnapshotData` where both operands are known-normal. Maps onto the transam
/// owner's `TransactionIdPrecedes` (modular comparison; identical result for
/// normal xids).
#[inline]
fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    transam::transaction_id_precedes::call(id1, id2)
}

/// `TransactionIdAdvance(dest)` (`access/transam.h`) — `dest++`, skipping the
/// special low xids on wraparound.
#[inline]
fn transaction_id_advance(dest: TransactionId) -> TransactionId {
    let mut d = dest.wrapping_add(1);
    if d < FirstNormalTransactionId {
        d = FirstNormalTransactionId;
    }
    d
}

/// `GetSnapshotData(Snapshot snapshot)` (procarray.c) — the hot path: fill an
/// MVCC snapshot's xmin/xmax/xip/subxip from the running-transactions state.
/// The seam returns only the computed snapshot fields; snapmgr replays the
/// `MyProc->xmin`/`TransactionXmin`/`RecentXmin` updates via the proc seam.
///
/// NB: the `GetSnapshotDataReuse` fast path (which reuses the caller's previous
/// snapshot arrays when `xactCompletionCount` is unchanged) cannot be expressed
/// across this seam: the owner is not handed the caller's prior `SnapshotData`,
/// so each call recomputes the snapshot from scratch (behaviour-preserving:
/// reuse only changes performance, never the resulting xmin/xmax/xip/subxip).
pub fn GetSnapshotData() -> PgResult<SnapshotData> {
    let mut snapshot = SnapshotData::sentinel(types_snapshot::snapshot::SnapshotType::SNAPSHOT_MVCC);

    let mut count: usize = 0;
    let mut subcount: i32 = 0;
    let mut suboverflowed = false;

    let replication_slot_xmin;
    let replication_slot_catalog_xmin;

    let mypgprocno = proc::my_proc_number::call();
    let mypgxactoff;
    let myxid;
    let latest_completed;
    let oldestxid;
    let cur_xact_completion_count;
    let xmax;
    let mut xmin;

    // We allocate maxProcs xids / TOTAL_MAX_CACHED_SUBXIDS subxids of space,
    // matching the C `malloc(GetMaxSnapshotXidCount() ...)`.
    let max_xip = crate::shmem_model::GetMaxSnapshotXidCount() as usize;
    let max_subxip = crate::shmem_model::GetMaxSnapshotSubxidCount() as usize;
    let mut xip: Vec<TransactionId> = vec![0; max_xip];
    let mut subxip: Vec<TransactionId> = vec![0; max_subxip];

    // It is sufficient to get shared lock on ProcArrayLock, even if we are going
    // to set MyProc->xmin.
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    // GetSnapshotDataReuse(snapshot) is not reachable through this seam (see the
    // doc comment): no prior snapshot is supplied, so we always recompute.

    latest_completed = varsup::get_latest_completed_xid::call();
    mypgxactoff = proc::proc_pgxactoff::call(mypgprocno);
    myxid = PROC_ARRAY.with(|pa| {
        pa.borrow()
            .as_ref()
            .expect("ProcArray accessed before ProcArrayShmemInit");
        proc::proc_array_xid::call(mypgxactoff)
    });
    debug_assert_eq!(myxid, proc::proc_xid::call(mypgprocno));

    oldestxid = varsup::get_oldest_xid::call();
    cur_xact_completion_count = varsup::get_xact_completion_count::call();

    // xmax is always latestCompletedXid + 1
    xmax = transaction_id_advance(latest_completed.xid());
    debug_assert!(TransactionIdIsNormal(xmax));

    // initialize xmin calculation with xmax
    xmin = xmax;

    // take own xid into account, saves a check inside the loop
    if TransactionIdIsNormal(myxid) && transaction_id_precedes(myxid, xmin) {
        xmin = myxid;
    }

    snapshot.takenDuringRecovery = xlog::recovery_in_progress::call();

    if !snapshot.takenDuringRecovery {
        let num_procs = PROC_ARRAY.with(|pa| {
            pa.borrow()
                .as_ref()
                .expect("ProcArray accessed before ProcArrayShmemInit")
                .numProcs
        });

        // First collect set of pgxactoff/xids that need to be included in the
        // snapshot.
        for pgxactoff in 0..num_procs {
            // Fetch xid just once - see GetNewTransactionId
            let xid = proc::proc_array_xid::call(pgxactoff);

            // If the transaction has no XID assigned, we can skip it; it won't
            // have sub-XIDs either.
            if xid == InvalidTransactionId {
                continue;
            }

            // We don't include our own XIDs (if any) in the snapshot. It needs
            // to be included in the xmin computation, but we did so outside the
            // loop.
            if pgxactoff == mypgxactoff {
                continue;
            }

            // The only way we are able to get here with a non-normal xid is
            // during bootstrap - with this backend using BootstrapTransactionId.
            // But the above test should filter that out.
            debug_assert!(TransactionIdIsNormal(xid));

            // If the XID is >= xmax, we can skip it; such transactions will be
            // treated as running anyway (and any sub-XIDs will also be >= xmax).
            if !transaction_id_precedes(xid, xmax) {
                continue;
            }

            // Skip over backends doing logical decoding which manages xmin
            // separately (check below) and ones running LAZY VACUUM.
            let status_flags = proc::proc_global_status_flags::call(pgxactoff);
            if status_flags & (PROC_IN_LOGICAL_DECODING | PROC_IN_VACUUM) != 0 {
                continue;
            }

            if transaction_id_precedes(xid, xmin) {
                xmin = xid;
            }

            // Add XID to snapshot.
            xip[count] = xid;
            count += 1;

            // Save subtransaction XIDs if possible (if we've already overflowed,
            // there's no point). The subxact XIDs must be later than their
            // parent, so no need to check them against xmin.
            if !suboverflowed {
                let (nsubxids, overflowed) = proc::proc_array_subxid_state::call(pgxactoff);
                if overflowed {
                    suboverflowed = true;
                } else if nsubxids > 0 {
                    let pgprocno =
                        PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos()[pgxactoff as usize]);
                    // pg_read_barrier() pairs with GetNewTransactionId; the seam
                    // copies the proc's cached subxids.
                    // C reads `nsubxids` from the dense `subxidStates` array and
                    // `memcpy`s exactly that many entries from the fixed-size
                    // `proc->subxids.xids` array, without re-reading the per-proc
                    // count (which can transiently lag the dense one) and with no
                    // `nxids >= nsubxids` assertion. The fixed array always has all
                    // 64 slots valid, so copying `nsubxids` of them is safe.
                    let (_proc_n, proc_subxids) = proc::proc_subxids::call(pgprocno);
                    subxip[subcount as usize..(subcount as usize + nsubxids as usize)]
                        .copy_from_slice(&proc_subxids[..nsubxids as usize]);
                    subcount += nsubxids;
                }
            }
        }
    } else {
        // We're in hot standby, so get XIDs from KnownAssignedXids (F5). All
        // xids are stored directly into subxip[].
        subcount = crate::knownassignedxids::KnownAssignedXidsGetAndSetXmin(
            &mut subxip,
            &mut xmin,
            xmax,
        );

        let last_overflowed = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().lastOverflowedXid);
        if !transaction_id_precedes(last_overflowed, xmin) {
            // xmin <= lastOverflowedXid  ==>  TransactionIdPrecedesOrEquals
            suboverflowed = true;
        }
    }

    // Fetch into local variable while ProcArrayLock is held - the LWLockRelease
    // below is a barrier, ensuring this happens inside the lock.
    (replication_slot_xmin, replication_slot_catalog_xmin) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let a = b.as_ref().unwrap();
        (a.replication_slot_xmin, a.replication_slot_catalog_xmin)
    });

    // procarray.c sets MyProc->xmin = TransactionXmin = xmin here; the seam
    // contract delegates that replay to snapmgr (see get_snapshot_data_into),
    // which reads the returned xmin.

    lwlock::lwlock_release_proc_array::call()?;

    // maintain state for GlobalVis*
    {
        // Converting oldestXid is only safe when xid horizon cannot advance,
        // i.e. holding locks. While we don't hold the lock anymore, all the
        // necessary data has been gathered with lock held.
        let oldestfxid = FullXidRelativeTo(latest_completed, oldestxid);

        // Check whether there's a replication slot requiring an older xmin.
        let def_vis_xid_data = TransactionIdOlder(xmin, replication_slot_xmin);

        // Rows in non-shared, non-catalog tables possibly could be vacuumed if
        // older than this xid.
        let mut def_vis_xid = def_vis_xid_data;

        // Check whether there's a replication slot requiring an older catalog
        // xmin.
        def_vis_xid = TransactionIdOlder(replication_slot_catalog_xmin, def_vis_xid);

        let def_vis_fxid = FullXidRelativeTo(latest_completed, def_vis_xid);
        let def_vis_fxid_data = FullXidRelativeTo(latest_completed, def_vis_xid_data);

        // Check if we can increase upper bound. As a previous GlobalVisUpdate()
        // might have computed more aggressive values, don't overwrite them if so.
        GLOBAL_VIS_SHARED_RELS.with(|g| {
            let mut s = g.borrow_mut();
            s.definitely_needed = FullTransactionIdNewer(def_vis_fxid, s.definitely_needed);
        });
        GLOBAL_VIS_CATALOG_RELS.with(|g| {
            let mut s = g.borrow_mut();
            s.definitely_needed = FullTransactionIdNewer(def_vis_fxid, s.definitely_needed);
        });
        GLOBAL_VIS_DATA_RELS.with(|g| {
            let mut s = g.borrow_mut();
            s.definitely_needed = FullTransactionIdNewer(def_vis_fxid_data, s.definitely_needed);
        });
        // See temp_oldest_nonremovable computation in ComputeXidHorizons()
        GLOBAL_VIS_TEMP_RELS.with(|g| {
            let mut s = g.borrow_mut();
            if TransactionIdIsNormal(myxid) {
                s.definitely_needed = FullXidRelativeTo(latest_completed, myxid);
            } else {
                let mut d = latest_completed;
                FullTransactionIdAdvance(&mut d);
                s.definitely_needed = d;
            }
        });

        // Check if we know that we can initialize or increase the lower bound.
        // Currently the only cheap way to do so is to use
        // TransamVariables->oldestXid as input.
        GLOBAL_VIS_SHARED_RELS.with(|g| {
            let mut s = g.borrow_mut();
            s.maybe_needed = FullTransactionIdNewer(s.maybe_needed, oldestfxid);
        });
        GLOBAL_VIS_CATALOG_RELS.with(|g| {
            let mut s = g.borrow_mut();
            s.maybe_needed = FullTransactionIdNewer(s.maybe_needed, oldestfxid);
        });
        GLOBAL_VIS_DATA_RELS.with(|g| {
            let mut s = g.borrow_mut();
            s.maybe_needed = FullTransactionIdNewer(s.maybe_needed, oldestfxid);
        });
        // accurate value known
        GLOBAL_VIS_TEMP_RELS.with(|g| {
            let mut s = g.borrow_mut();
            s.maybe_needed = s.definitely_needed;
        });
    }

    // procarray.c: RecentXmin = xmin; replayed by snapmgr from the returned xmin.

    xip.truncate(count);
    subxip.truncate(subcount as usize);

    snapshot.xmin = xmin;
    snapshot.xmax = xmax;
    snapshot.xip = xip;
    snapshot.xcnt = count as u32;
    snapshot.subxip = subxip;
    snapshot.subxcnt = subcount;
    snapshot.suboverflowed = suboverflowed;
    snapshot.snapXactCompletionCount = cur_xact_completion_count;

    snapshot.curcid = xact::get_current_command_id::call(false)?;

    // This is a new snapshot, so set both refcounts to zero, and mark it as not
    // copied in persistent memory.
    snapshot.active_count = 0;
    snapshot.regd_count = 0;
    snapshot.copied = false;

    Ok(snapshot)
}

/// `GetSnapshotDataReuse(Snapshot snapshot)` (procarray.c, static) — the
/// fast-path that re-uses the previous snapshot's arrays when nothing relevant
/// has changed since `xactCompletionCount`. Returns whether the reuse was
/// taken.
///
/// Faithfully ported, but unreachable through the `get_snapshot_data` seam
/// (which never hands the owner a caller-owned prior snapshot to re-validate).
/// Kept for completeness; callers run with `ProcArrayLock` held.
pub fn GetSnapshotDataReuse(snapshot: &mut SnapshotData) -> bool {
    debug_assert!(lwlock::lwlock_held_by_me_proc_array::call());

    if snapshot.snapXactCompletionCount == 0 {
        return false;
    }

    let cur_xact_completion_count = varsup::get_xact_completion_count::call();
    if cur_xact_completion_count != snapshot.snapXactCompletionCount {
        return false;
    }

    // As the snapshot contents are the same as before, it is safe to re-enter
    // the snapshot's xmin into the PGPROC array.
    if !TransactionIdIsValid(proc::my_proc_xmin::call()) {
        proc::set_my_proc_xmin::call(snapshot.xmin);
        backend_utils_time_snapmgr_pc_seams::set_transaction_xmin::call(snapshot.xmin);
    }
    // RecentXmin = snapshot->xmin replayed by snapmgr; curcid set by the caller.

    snapshot.active_count = 0;
    snapshot.regd_count = 0;
    snapshot.copied = false;

    true
}

/// `ProcArrayInstallImportedXmin(TransactionId xmin,
/// VirtualTransactionId *sourcevxid)` (procarray.c) — make our `MyProc->xmin`
/// safe to set from an imported snapshot, verifying the source vxid is still
/// running. `false` when the source vanished.
pub fn ProcArrayInstallImportedXmin(
    xmin: TransactionId,
    sourcevxid: VirtualTransactionId,
) -> PgResult<bool> {
    let mut result = false;

    debug_assert!(TransactionIdIsNormal(xmin));
    // The C returns false on a NULL sourcevxid; here a value is always passed,
    // but an invalid procNumber stands in for the same "no source" case.
    if sourcevxid.procNumber == INVALID_PROC_NUMBER {
        return Ok(false);
    }

    let my_database_id = backend_utils_init_small::globals::MyDatabaseId();

    // Get lock so source xact can't end while we're doing this.
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);

    for index in 0..num_procs {
        let pgprocno = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos()[index as usize]);
        let status_flags = proc::proc_global_status_flags::call(index);

        // Ignore procs running LAZY VACUUM.
        if status_flags & PROC_IN_VACUUM != 0 {
            continue;
        }

        // We are only interested in the specific virtual transaction.
        let (proc_number, lxid) = proc::proc_vxid::call(pgprocno);
        if proc_number != sourcevxid.procNumber {
            continue;
        }
        if lxid != sourcevxid.localTransactionId {
            continue;
        }

        // Paranoia: if it's in another DB then its xmin does not cover us.
        if proc::proc_database_id::call(pgprocno) != my_database_id {
            continue;
        }

        // Make real sure its xmin does cover us.
        let xid = proc::proc_xmin::call(pgprocno);
        if !TransactionIdIsNormal(xid) || transaction_id_precedes(xmin, xid) {
            // !TransactionIdPrecedesOrEquals(xid, xmin) == xmin < xid
            continue;
        }

        // We're good. Install the new xmin. As in GetSnapshotData, set
        // TransactionXmin too.
        proc::set_my_proc_xmin::call(xmin);
        backend_utils_time_snapmgr_pc_seams::set_transaction_xmin::call(xmin);

        result = true;
        break;
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(result)
}

/// `ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc)`
/// (procarray.c) — like the imported variant but the source is a PGPROC
/// (parallel-worker restore).
pub fn ProcArrayInstallRestoredXmin(
    xmin: TransactionId,
    source_proc: ProcNumber,
) -> PgResult<bool> {
    let mut result = false;

    debug_assert!(TransactionIdIsNormal(xmin));

    let my_database_id = backend_utils_init_small::globals::MyDatabaseId();
    let mypgprocno = proc::my_proc_number::call();

    // Get an exclusive lock so that we can copy statusFlags from source proc.
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)?;

    // Be certain that the referenced PGPROC has an advertised xmin which is no
    // later than the one we're installing, so that the system-wide xmin can't go
    // backwards. Also, make sure it's running in the same database.
    let xid = proc::proc_xmin::call(source_proc);
    if proc::proc_database_id::call(source_proc) == my_database_id
        && TransactionIdIsNormal(xid)
        && !transaction_id_precedes(xmin, xid)
    {
        // TransactionIdPrecedesOrEquals(xid, xmin) == !(xmin < xid)
        // Install xmin and propagate the statusFlags that affect how the value
        // is interpreted by vacuum.
        proc::set_my_proc_xmin::call(xmin);
        backend_utils_time_snapmgr_pc_seams::set_transaction_xmin::call(xmin);

        let my_flags = proc::proc_status_flags::call(mypgprocno);
        let src_flags = proc::proc_status_flags::call(source_proc);
        let new_flags = (my_flags & !PROC_XMIN_FLAGS) | (src_flags & PROC_XMIN_FLAGS);
        proc::set_my_proc_status_flags::call(new_flags);
        proc::set_proc_array_status_flags::call(proc::proc_pgxactoff::call(mypgprocno), new_flags);

        result = true;
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(result)
}

/// `GetRunningTransactionData(void)` (procarray.c) — build the running-xacts
/// snapshot for a `XLOG_RUNNING_XACTS` record. The C returns with
/// `ProcArrayLock` + `XidGenLock` held; here the owner holds both across the
/// callback `f` and releases everything still held when `f` returns.
pub fn GetRunningTransactionData(
    f: &mut dyn FnMut(
        &RunningTransactionsData<'_>,
        &mut dyn RunningTransactionLocksHeld,
    ) -> PgResult<XLogRecPtr>,
) -> PgResult<XLogRecPtr> {
    debug_assert!(!xlog::recovery_in_progress::call());

    // The C buffer is a static malloc of TOTAL_MAX_CACHED_SUBXIDS xids reused
    // across calls; here a per-call context owns the `xids` PgVec for the
    // callback's duration (behaviour-preserving: the callback consumes it before
    // returning).
    let ctx = mcx::MemoryContext::new("GetRunningTransactionData");
    let mcx = ctx.mcx();
    let total_max_cached_subxids = crate::shmem_model::GetMaxSnapshotSubxidCount();

    // Ensure that no xids enter or leave the procarray while we obtain snapshot.
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;
    lwlock::lwlock_acquire_xid_gen::call(LWLockMode::LW_SHARED)?;

    let res = get_running_transaction_data_locked(mcx, total_max_cached_subxids, f);

    // The callback may have released ProcArrayLock early; release whatever is
    // still held, XidGenLock last (reverse acquisition order).
    let mut release_err: PgResult<()> = Ok(());
    if lwlock::lwlock_held_by_me_proc_array::call() {
        if let e @ Err(_) = lwlock::lwlock_release_proc_array::call() {
            release_err = e;
        }
    }
    let xg = lwlock::lwlock_release_xid_gen::call();

    let out = res?;
    release_err?;
    xg?;
    Ok(out)
}

fn get_running_transaction_data_locked<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    total_max_cached_subxids: i32,
    f: &mut dyn FnMut(
        &RunningTransactionsData<'_>,
        &mut dyn RunningTransactionLocksHeld,
    ) -> PgResult<XLogRecPtr>,
) -> PgResult<XLogRecPtr> {
    let mut xids: PgVec<'mcx, TransactionId> =
        mcx::vec_with_capacity_in(mcx, total_max_cached_subxids as usize)?;

    let mut count: i32 = 0;
    let mut subcount: i32 = 0;
    let mut suboverflowed = false;

    let next_full_xid = varsup::read_next_full_transaction_id::call();
    let latest_completed_xid = varsup::get_latest_completed_xid::call().xid();
    let mut oldest_running_xid = next_full_xid.xid();
    let mut oldest_database_running_xid = oldest_running_xid;

    let my_database_id = backend_utils_init_small::globals::MyDatabaseId();
    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);

    // Spin over procArray collecting all xids.
    for index in 0..num_procs {
        // Fetch xid just once - see GetNewTransactionId
        let xid = proc::proc_array_xid::call(index);

        // We don't need to store transactions that don't have a TransactionId
        // yet because they will not show as running on a standby server.
        if !TransactionIdIsValid(xid) {
            continue;
        }

        // Be careful not to exclude any xids before calculating the values of
        // oldestRunningXid and suboverflowed.
        if transaction_id_precedes(xid, oldest_running_xid) {
            oldest_running_xid = xid;
        }

        // Also, update the oldest running xid within the current database.
        if transaction_id_precedes(xid, oldest_database_running_xid) {
            let pgprocno =
                PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos()[index as usize]);
            if proc::proc_database_id::call(pgprocno) == my_database_id {
                oldest_database_running_xid = xid;
            }
        }

        if proc::proc_array_subxid_state::call(index).1 {
            suboverflowed = true;
        }

        xids.push(xid);
        count += 1;
    }

    // Spin over procArray collecting all subxids, but only if there hasn't been
    // a suboverflow.
    if !suboverflowed {
        for index in 0..num_procs {
            let pgprocno =
                PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos()[index as usize]);
            let nsubxids = proc::proc_array_subxid_state::call(index).0;
            if nsubxids > 0 {
                // barrier not really required, as XidGenLock is held, but ...
                //
                // C reads `nsubxids` from the dense `ProcGlobal->subxidStates`
                // array, then `memcpy`s exactly that many entries out of the
                // fixed-size `proc->subxids.xids` array. It does NOT re-read the
                // per-proc `subxids.count`, because that field and the dense
                // count are updated at slightly different times and the per-proc
                // value can transiently lag. The fixed array always has the full
                // 64 slots valid, so copying `nsubxids` of them is safe.
                let (_proc_n, proc_subxids) = proc::proc_subxids::call(pgprocno);
                for &sx in &proc_subxids[..nsubxids as usize] {
                    xids.push(sx);
                }
                count += nsubxids;
                subcount += nsubxids;
            }
        }
    }

    // It's important *not* to include the limits set by slots here because
    // snapbuild.c uses oldestRunningXid to manage its xmin horizon.

    let subxid_status: subxids_array_status = if suboverflowed {
        SUBXIDS_IN_SUBTRANS
    } else {
        SUBXIDS_IN_ARRAY
    };

    let running = RunningTransactionsData {
        xcnt: count - subcount,
        subxcnt: subcount,
        subxid_status,
        nextXid: next_full_xid.xid(),
        oldestRunningXid: oldest_running_xid,
        oldestDatabaseRunningXid: oldest_database_running_xid,
        latestCompletedXid: latest_completed_xid,
        xids,
    };

    debug_assert!(TransactionIdIsValid(running.nextXid));
    debug_assert!(TransactionIdIsValid(running.oldestRunningXid));
    debug_assert!(TransactionIdIsNormal(running.latestCompletedXid));

    // We don't release the locks here, the caller (the seam wrapper) is
    // responsible for that.
    let mut held = Held;
    f(&running, &mut held)
}

/// The locks-held token threaded into the `GetRunningTransactionData` callback:
/// lets the callback release `ProcArrayLock` early on the `wal_level < logical`
/// path. `XidGenLock` is always released by the owner after the callback.
struct Held;

impl RunningTransactionLocksHeld for Held {
    fn release_proc_array_lock(&mut self) -> PgResult<()> {
        lwlock::lwlock_release_proc_array::call()
    }
}

/// `GetOldestActiveTransactionId(void)` (procarray.c) — oldest XID still
/// running of any backend (no replication-slot influence).
pub fn GetOldestActiveTransactionId() -> PgResult<TransactionId> {
    debug_assert!(!xlog::recovery_in_progress::call());

    // Read nextXid, as the upper bound of what's still active.
    lwlock::lwlock_acquire_xid_gen::call(LWLockMode::LW_SHARED)?;
    let mut oldest_running_xid = varsup::read_next_full_transaction_id::call().xid();
    lwlock::lwlock_release_xid_gen::call()?;

    // Spin over procArray collecting all xids and subxids.
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;
    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let xid = proc::proc_array_xid::call(index);

        if !TransactionIdIsNormal(xid) {
            continue;
        }

        if transaction_id_precedes(xid, oldest_running_xid) {
            oldest_running_xid = xid;
        }
    }
    lwlock::lwlock_release_proc_array::call()?;

    Ok(oldest_running_xid)
}

/// `GetOldestSafeDecodingTransactionId(bool catalogOnly)` (procarray.c) — the
/// oldest xid it is safe to start logical decoding from. Called with
/// `ProcArrayLock` held.
pub fn GetOldestSafeDecodingTransactionId(catalog_only: bool) -> TransactionId {
    debug_assert!(lwlock::lwlock_held_by_me_proc_array::call());

    let recovery_in_progress = xlog::recovery_in_progress::call();

    // Acquire XidGenLock, so no transactions can acquire an xid while we're
    // running. Initialize to nextXid since that's guaranteed to be safe.
    lwlock::lwlock_acquire_xid_gen::call(LWLockMode::LW_SHARED)
        .expect("GetOldestSafeDecodingTransactionId: XidGenLock acquire");
    let mut oldest_safe_xid = varsup::read_next_full_transaction_id::call().xid();

    let (slot_xmin, slot_catalog_xmin) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let a = b.as_ref().unwrap();
        (a.replication_slot_xmin, a.replication_slot_catalog_xmin)
    });

    // If there's already a slot pegging the xmin horizon, we can start with that
    // value.
    if TransactionIdIsValid(slot_xmin) && transaction_id_precedes(slot_xmin, oldest_safe_xid) {
        oldest_safe_xid = slot_xmin;
    }

    if catalog_only
        && TransactionIdIsValid(slot_catalog_xmin)
        && transaction_id_precedes(slot_catalog_xmin, oldest_safe_xid)
    {
        oldest_safe_xid = slot_catalog_xmin;
    }

    // If we're not in recovery, walk over the procarray and collect the lowest
    // xid.
    if !recovery_in_progress {
        let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
        for index in 0..num_procs {
            let xid = proc::proc_array_xid::call(index);

            if !TransactionIdIsNormal(xid) {
                continue;
            }

            if transaction_id_precedes(xid, oldest_safe_xid) {
                oldest_safe_xid = xid;
            }
        }
    }

    lwlock::lwlock_release_xid_gen::call()
        .expect("GetOldestSafeDecodingTransactionId: XidGenLock release");

    oldest_safe_xid
}

/// `ProcArraySetReplicationSlotXmin(TransactionId xmin,
/// TransactionId catalog_xmin, bool already_locked)` (procarray.c) — publish
/// the aggregate slot xmin horizons into the ProcArray.
pub fn ProcArraySetReplicationSlotXmin(
    xmin: TransactionId,
    catalog_xmin: TransactionId,
    already_locked: bool,
) {
    debug_assert!(!already_locked || lwlock::lwlock_held_by_me_proc_array::call());

    if !already_locked {
        lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)
            .expect("ProcArraySetReplicationSlotXmin: ProcArrayLock acquire");
    }

    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let a = b.as_mut().unwrap();
        a.replication_slot_xmin = xmin;
        a.replication_slot_catalog_xmin = catalog_xmin;
    });

    if !already_locked {
        lwlock::lwlock_release_proc_array::call()
            .expect("ProcArraySetReplicationSlotXmin: ProcArrayLock release");
    }

    // elog(DEBUG1, "xmin required by slots: ...") omitted (DEBUG-level trace).
}

/// `ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
/// TransactionId *catalog_xmin)` (procarray.c) — read back the published slot
/// xmin horizons.
pub fn ProcArrayGetReplicationSlotXmin() -> (TransactionId, TransactionId) {
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)
        .expect("ProcArrayGetReplicationSlotXmin: ProcArrayLock acquire");

    let result = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let a = b.as_ref().unwrap();
        (a.replication_slot_xmin, a.replication_slot_catalog_xmin)
    });

    lwlock::lwlock_release_proc_array::call()
        .expect("ProcArrayGetReplicationSlotXmin: ProcArrayLock release");

    result
}

/// `GetReplicationHorizons(TransactionId *xmin, TransactionId *catalog_xmin)`
/// (procarray.c) — oldest xmins to advertise via hot-standby feedback.
pub fn GetReplicationHorizons() -> (TransactionId, TransactionId) {
    let horizons = crate::horizons::ComputeXidHorizons()
        .expect("GetReplicationHorizons: ComputeXidHorizons");

    // Don't want to use shared_oldest_nonremovable here, as that contains the
    // effect of replication slot's catalog_xmin. We want to send a separate
    // feedback for the catalog horizon.
    (
        horizons.shared_oldest_nonremovable_raw,
        horizons.slot_catalog_xmin,
    )
}

// --- Logical-decoding flag bookkeeping under ProcArrayLock (procarray.c) ---
//
// These are slot.c's out-of-band acquire/release bracket + status-flag pokes
// (`MyProc->statusFlags |= PROC_IN_LOGICAL_DECODING`, mirrored into the dense
// ProcGlobal->statusFlags[]) performed while holding ProcArrayLock exclusively.

/// `LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE)` exposed for logical decoding's
/// out-of-band acquire/release bracket around the decoding-flag set.
pub fn ProcArrayLockAcquireExclusive() {
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)
        .expect("ProcArrayLockAcquireExclusive: ProcArrayLock acquire");
}

/// `LWLockRelease(ProcArrayLock)` — matching release for the above.
pub fn ProcArrayLockRelease() {
    lwlock::lwlock_release_proc_array::call().expect("ProcArrayLockRelease: ProcArrayLock release");
}

/// `MyProc->statusFlags |= PROC_IN_LOGICAL_DECODING` (mirrored into
/// `ProcGlobal->statusFlags[MyProc->pgxactoff]`) while holding `ProcArrayLock`.
pub fn MarkProcInLogicalDecoding() {
    debug_assert!(lwlock::lwlock_held_by_me_in_mode_main::call(
        PROC_ARRAY_LOCK,
        LWLockMode::LW_EXCLUSIVE
    ));
    let mypgprocno = proc::my_proc_number::call();
    let new_flags = proc::proc_status_flags::call(mypgprocno) | PROC_IN_LOGICAL_DECODING;
    proc::set_my_proc_status_flags::call(new_flags);
    proc::set_proc_array_status_flags::call(proc::proc_pgxactoff::call(mypgprocno), new_flags);
}

/// Clear `PROC_IN_LOGICAL_DECODING` on `MyProc` (and its dense mirror) under
/// `ProcArrayLock` exclusive — slot.c `ReplicationSlotRelease`.
pub fn ProcArrayClearLogicalDecodingFlag() {
    debug_assert!(lwlock::lwlock_held_by_me_in_mode_main::call(
        PROC_ARRAY_LOCK,
        LWLockMode::LW_EXCLUSIVE
    ));
    let mypgprocno = proc::my_proc_number::call();
    let new_flags = proc::proc_status_flags::call(mypgprocno) & !PROC_IN_LOGICAL_DECODING;
    proc::set_my_proc_status_flags::call(new_flags);
    proc::set_proc_array_status_flags::call(proc::proc_pgxactoff::call(mypgprocno), new_flags);
}

/// `GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid)`
/// (procarray.c) — VXIDs of backends whose snapshots could still see
/// `limitXmin`. Returns an `mcx`-allocated array (the C `InvalidVirtualTransactionId`
/// terminator is dropped).
pub fn GetConflictingVirtualXIDs<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    limit_xmin: TransactionId,
    db_oid: Oid,
) -> PgResult<PgVec<'mcx, VirtualTransactionId>> {
    let max_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().maxProcs);
    let mut vxids: PgVec<'mcx, VirtualTransactionId> =
        mcx::vec_with_capacity_in(mcx, max_procs as usize)?;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos()[index as usize]);

        // Exclude prepared transactions.
        if proc::proc_pid::call(pgprocno) == 0 {
            continue;
        }

        if !(db_oid != types_core::InvalidOid) || proc::proc_database_id::call(pgprocno) == db_oid {
            // !OidIsValid(dbOid) || proc->databaseId == dbOid
            // Fetch xmin just once.
            let pxmin = proc::proc_xmin::call(pgprocno);

            // We ignore an invalid pxmin because this means that backend has no
            // snapshot currently.
            //   !TransactionIdIsValid(limitXmin) ||
            //   (TransactionIdIsValid(pxmin) && !TransactionIdFollows(pxmin, limitXmin))
            // TransactionIdFollows(pxmin, limitXmin) == transaction_id_precedes(limitXmin, pxmin)
            if !TransactionIdIsValid(limit_xmin)
                || (TransactionIdIsValid(pxmin)
                    && !transaction_id_precedes(limit_xmin, pxmin))
            {
                let (proc_number, lxid) = proc::proc_vxid::call(pgprocno);
                let vxid = VirtualTransactionId {
                    procNumber: proc_number,
                    localTransactionId: lxid,
                };
                // VirtualTransactionIdIsValid: localTransactionId is valid.
                if vxid.localTransactionId != InvalidLocalTransactionId {
                    vxids.push(vxid);
                }
            }
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    // The C InvalidVirtualTransactionId terminator is dropped (the PgVec carries
    // its own length).
    Ok(vxids)
}

/// Install the F2-owned inward seams (snapshot + running-xacts + slot-xmin +
/// decoding-horizon), heavily consumed by snapmgr, slot, and logical decoding.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::get_snapshot_data::set(GetSnapshotData);
    seams::proc_array_install_imported_xmin::set(ProcArrayInstallImportedXmin);
    seams::proc_array_install_restored_xmin::set(ProcArrayInstallRestoredXmin);
    seams::get_running_transaction_data::set(GetRunningTransactionData);
    seams::get_oldest_safe_decoding_transaction_id::set(GetOldestSafeDecodingTransactionId);
    seams::GetOldestSafeDecodingTransactionId::set(GetOldestSafeDecodingTransactionId);
    seams::proc_array_set_replication_slot_xmin::set(ProcArraySetReplicationSlotXmin);
    seams::get_replication_horizons::set(GetReplicationHorizons);
    seams::proc_array_get_replication_slot_xmin::set(ProcArrayGetReplicationSlotXmin);
    seams::get_conflicting_virtual_xids::set(GetConflictingVirtualXIDs);

    // Logical-decoding flag bookkeeping + ProcArrayLock bracket.
    seams::ProcArrayLock_acquire_exclusive::set(ProcArrayLockAcquireExclusive);
    seams::ProcArrayLock_release::set(ProcArrayLockRelease);
    seams::mark_proc_in_logical_decoding::set(MarkProcInLogicalDecoding);
    seams::proc_array_clear_logical_decoding_flag::set(ProcArrayClearLogicalDecodingFlag);
}
