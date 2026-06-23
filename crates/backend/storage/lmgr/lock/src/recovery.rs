//! Recovery / two-phase-commit / introspection entry points of
//! `storage/lmgr/lock.c`:
//!
//! * `AtPrepare_Locks` / `PostPrepare_Locks` — collect a transaction's locks
//!   for the 2PC state file and transfer them to the prepared-xact dummy PGPROC.
//! * `lock_twophase_recover` / `lock_twophase_standby_recover` /
//!   `lock_twophase_postcommit` / `lock_twophase_postabort` — the 2PC resource
//!   manager callbacks (`TWOPHASE_RM_LOCK_ID`).
//! * `GetLockConflicts` / `GetLockStatusData` — read-only snapshots of the lock
//!   tables for snapshot logging / `pg_lock_status`.
//! * `VirtualXactLockTableCleanup` — release this backend's VXID lock at xact
//!   end. The `VirtualXactLock` waiter path (in `locking.rs`) is now ported: the
//!   cross-backend `fpInfoLock`-guarded examination + fast-path→main-table
//!   transfer is performed through proc.c's `virtual_xact_examine_proc` seam.
//! * `proc_locks_hold_masks` — proc.c's per-partition `myProcLocks` hold-mask
//!   walk, served from the ambient PROCLOCK table.
//!
//! The C fast-path arrays (`MyProc->fpRelId` / `fpLockBits`) are deferred to F3
//! (see `crate::fastpath`): no lock is ever recorded in a fast-path slot in this
//! model, so the fast-path scanning legs of `GetLockConflicts` /
//! `GetLockStatusData` observe an empty fast-path and are faithfully skipped —
//! every real lock is in the shared table these functions already scan.

use alloc::vec::Vec;

use ::types_core::ProcNumber;
use ::types_error::PgResult;
use ::types_storage::lock::{
    AccessExclusiveLock, LOCKMASK, LOCKMODE, LOCKTAG, LOCKBIT_ON,
    LOCKTAG_RELATION, LOCKTAG_VIRTUALTRANSACTION, NoLock,
};
use ::types_storage::storage::NUM_LOCK_PARTITIONS;
use ::types_storage::LWLockMode;

use crate::state;
use crate::tables;
use crate::LockTagHashCode;

use twophase_seams as twophase;
use standby_seams as standby;
use lwlock_seams as lwlock;
use lmgr_proc_seams as proc;

use crate::locking::{
    grant_lock_seam, lock_hash_partition, lock_partition_lock_offset,
    lock_partition_lock_offset_by_index, pg_error_internal, setup_lock_in_table_for,
    LockRefindAndRelease,
};

/// `TwoPhaseLockRecord` (`storage/lmgr/lock.c`) — the 2PC state-file record one
/// `AtPrepare_Locks` registers per held lock; `lock_twophase_recover` /
/// `lock_twophase_standby_recover` read it back. `#[repr(C)]` so the
/// register-then-recover byte round-trip (mirroring C's `memcpy` in/out of the
/// 2PC record buffer) has a deterministic layout.
#[repr(C)]
#[derive(Clone, Copy)]
struct TwoPhaseLockRecord {
    locktag: LOCKTAG,
    lockmode: LOCKMODE,
}

/// Serialize a `TwoPhaseLockRecord` into the 2PC record buffer (C `memcpy`).
fn encode_record(rec: &TwoPhaseLockRecord) -> Vec<u8> {
    // SAFETY: `TwoPhaseLockRecord` is `#[repr(C)]` and `Copy` (no padding bytes
    // are read here that matter — they round-trip identically to `decode`).
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (rec as *const TwoPhaseLockRecord) as *const u8,
            core::mem::size_of::<TwoPhaseLockRecord>(),
        )
    };
    bytes.to_vec()
}

/// Deserialize a `TwoPhaseLockRecord` from the 2PC record buffer (C cast).
fn decode_record(recdata: &[u8]) -> TwoPhaseLockRecord {
    debug_assert_eq!(recdata.len(), core::mem::size_of::<TwoPhaseLockRecord>());
    // SAFETY: produced by `encode_record` of the same `#[repr(C)]` type.
    unsafe { core::ptr::read_unaligned(recdata.as_ptr() as *const TwoPhaseLockRecord) }
}

// ===========================================================================
// AtPrepare_Locks / PostPrepare_Locks.
// ===========================================================================

/// `CheckForSessionAndXactLocks()` (lock.c) — error out if any lockable object
/// is held at both session and transaction level. Scans the backend-private
/// LOCALLOCK table, building a per-LOCKTAG `(sessLock, xactLock)` summary.
fn check_for_session_and_xact_locks() -> PgResult<()> {
    use std::collections::HashMap;

    // Snapshot the (LOCKTAG, owners) we care about under the LOCAL borrow, then
    // fold without holding the borrow.
    let per_locallock: Vec<(LOCKTAG, bool, bool)> = state::with_local(|m| {
        m.values()
            .filter(|l| l.tag.lock.locktag_type != LOCKTAG_VIRTUALTRANSACTION)
            .filter(|l| l.nLocks > 0)
            .map(|l| {
                let mut sess = false;
                let mut xact = false;
                for o in l.lockOwners.iter() {
                    if o.owner.is_none() {
                        sess = true;
                    } else {
                        xact = true;
                    }
                }
                (l.tag.lock, sess, xact)
            })
            .collect()
    });

    let mut by_tag: HashMap<LOCKTAG, (bool, bool)> = HashMap::new();
    for (tag, sess, xact) in per_locallock {
        let e = by_tag.entry(tag).or_insert((false, false));
        e.0 |= sess;
        e.1 |= xact;
        if e.0 && e.1 {
            return Err(pg_error_internal(
                ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot PREPARE while holding both session-level and \
                 transaction-level locks on the same object"
                    .into(),
            ));
        }
    }
    Ok(())
}

/// `AtPrepare_Locks()` (lock.c) — collect this transaction's lock data into 2PC
/// state-file records.
pub(crate) fn AtPrepare_Locks() -> PgResult<()> {
    // First, verify there aren't locks of both xact and session level.
    check_for_session_and_xact_locks()?;

    // Now do the per-locallock cleanup work. Snapshot the (tag, mode) pairs that
    // need a 2PC record under the LOCAL borrow, plus clear holdsStrongLockCount
    // for the locks we are handing off (we must retain the strong-lock count
    // until the prepared xact commits/aborts, so the local entry must stop
    // owning it).
    let to_register: Vec<(LOCKTAG, LOCKMODE)> = state::with_local(|m| {
        let mut out = Vec::new();
        for l in m.values_mut() {
            // Ignore VXID locks (not meaningful after a restart).
            if l.tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION {
                continue;
            }
            // Ignore it if we don't actually hold the lock.
            if l.nLocks <= 0 {
                continue;
            }
            // Scan to see whether we hold it at session or transaction level.
            let mut have_session = false;
            let mut have_xact = false;
            for o in l.lockOwners.iter() {
                if o.owner.is_none() {
                    have_session = true;
                } else {
                    have_xact = true;
                }
            }
            // Ignore it if we have only a session lock (CheckForSession... has
            // already rejected the both-levels case, so have_session here means
            // session-only).
            if !have_xact {
                continue;
            }
            debug_assert!(!have_session);

            // (In C, a fast-path-only lock is moved into the primary table here
            // via FastPathGetRelationLockEntry. The fast path is deferred (F3),
            // so every held lock is already in the primary table.)

            // Retain the strong lock count for the prepared transaction.
            l.holdsStrongLockCount = false;

            out.push((l.tag.lock, l.tag.mode));
        }
        out
    });

    for (locktag, lockmode) in to_register {
        let record = TwoPhaseLockRecord { locktag, lockmode };
        let bytes = encode_record(&record);
        twophase::register_two_phase_record::call(
            twophase_rmgr::TWOPHASE_RM_LOCK_ID,
            0,
            &bytes,
        )?;
    }

    Ok(())
}

/// `PostPrepare_Locks(xid)` (lock.c) — transfer this backend's locks to the
/// prepared transaction's dummy PGPROC and drop the LOCALLOCK entries.
pub(crate) fn PostPrepare_Locks(xid: ::types_core::primitive::TransactionId) -> PgResult<()> {
    let newproc = twophase::two_phase_get_dummy_proc_number::call(xid, false)?;
    let myproc = proc::my_proc_number::call();

    // First pass: run through the LOCALLOCK table, dropping entries and marking
    // each surviving PROCLOCK's releaseMask for the modes we must transfer.
    //
    // For each LOCALLOCK we hold at xact level, set the PROCLOCK's releaseMask
    // bit for that mode (the proclock is keyed (LOCKTAG, myproc) in the ambient
    // model), then remove the LOCALLOCK entry.
    let locallocks: Vec<(LOCKTAG, LOCKMODE, bool, bool, bool)> = state::with_local(|m| {
        m.values()
            .map(|l| {
                let proclock_missing = l.nLocks == 0;
                let is_vxid = l.tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION;
                let mut have_session = false;
                let mut have_xact = false;
                for o in l.lockOwners.iter() {
                    if o.owner.is_none() {
                        have_session = true;
                    } else {
                        have_xact = true;
                    }
                }
                let _ = have_session;
                (l.tag.lock, l.tag.mode, proclock_missing, is_vxid, have_xact)
            })
            .collect()
    });

    for (locktag, mode, proclock_missing, is_vxid, have_xact) in locallocks {
        if proclock_missing {
            // Ran out of shared memory while setting up this lock (cannot happen
            // in the HashMap model, but mirror C: just forget the local entry).
            crate::locking::remove_local_lock_for(&locktag, mode);
            continue;
        }
        if is_vxid {
            continue;
        }
        if !have_xact {
            // Session-only lock: leave it in place.
            continue;
        }
        // Mark the proclock to show we need to release this lockmode.
        state::with_shared(|s| {
            s.proclock_update(&locktag, myproc, |pl| {
                pl.release_mask |= LOCKBIT_ON(mode);
            });
        });
        crate::locking::remove_local_lock_for(&locktag, mode);
    }

    // Second pass: scan each lock partition and transfer the proclocks whose
    // releaseMask is non-zero from `myproc` to `newproc`.
    for partition in 0..NUM_LOCK_PARTITIONS as i32 {
        // Collect this partition's proclocks held by myproc by walking myproc's
        // own per-partition myProcLocks list (O(held), not a full-slab scan).
        let in_partition: Vec<LOCKTAG> =
            state::with_shared(|s| s.my_proc_lock_tags(myproc, partition));
        if in_partition.is_empty() {
            continue;
        }

        let _guard = lwlock::lwlock_acquire_main::call(
            lock_partition_lock_offset_by_index(partition),
            LWLockMode::LW_EXCLUSIVE,
        )?;

        for tag in in_partition {
            // Ignore VXID locks.
            if tag.locktag_type == LOCKTAG_VIRTUALTRANSACTION {
                continue;
            }
            let (release_mask, hold_mask, group_leader) = state::with_shared(|s| {
                s.proclock_get(&tag, myproc)
                    .map(|p| (p.release_mask, p.hold_mask, p.group_leader))
                    .unwrap_or((0, 0, myproc))
            });
            // Ignore it if nothing to release (must be a session lock).
            if release_mask == 0 {
                continue;
            }
            if release_mask != hold_mask {
                return Err(pg_error_internal(
                    ::types_error::ERRCODE_INTERNAL_ERROR,
                    "we seem to have dropped a bit somewhere".into(),
                ));
            }
            debug_assert_eq!(group_leader, myproc);

            // Reassign ownership: move the (tag, myproc) PROCLOCK to (tag,
            // newproc), updating the group leader to the new proc. In the
            // ambient model the PROCLOCK key is (LOCKTAG, holder); "rekey" by
            // removing under the old holder and re-inserting under the new one,
            // and update the LOCK's holder list.
            let _ = _guard;
            state::with_shared(|s| {
                s.proclock_rekey_holder(&tag, myproc, newproc, |pl| {
                    pl.group_leader = newproc;
                    pl.release_mask = 0;
                });
            });
        }
    }

    Ok(())
}

// ===========================================================================
// 2PC resource-manager callbacks (TWOPHASE_RM_LOCK_ID).
// ===========================================================================

/// `lock_twophase_recover(xid, info, recdata, len)` (lock.c) — re-acquire a
/// prepared transaction's lock at recovery startup, granting it unconditionally
/// to the dummy PGPROC.
pub(crate) fn lock_twophase_recover(
    xid: ::types_core::primitive::TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let rec = decode_record(recdata);
    let locktag = rec.locktag;
    let lockmode = rec.lockmode;
    let lockmethodid = locktag.locktag_lockmethodid;

    if !tables::is_valid_lockmethodid(lockmethodid as u16) {
        return Err(pg_error_internal(
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock method: {lockmethodid}"),
        ));
    }

    let proc_no = twophase::two_phase_get_dummy_proc_number::call(xid, false)?;
    let hashcode = LockTagHashCode(&locktag);

    let guard = lwlock::lwlock_acquire_main::call(
        lock_partition_lock_offset(hashcode),
        LWLockMode::LW_EXCLUSIVE,
    )?;

    // Find or create the LOCK + PROCLOCK and bump request counts; this is
    // exactly SetupLockInTable's bookkeeping, reused (it also rejects an
    // already-held mode with the matching error).
    setup_lock_in_table_for(proc_no, &locktag, lockmode)?;

    // We ignore any possible conflicts and just grant ourselves the lock (to
    // avoid deadlocks when switching from standby to normal mode).
    grant_lock_seam(&locktag, proc_no, lockmode);

    // Bump the strong lock count so fast-path requests consult the primary table
    // (matches C; conflicts_with_relation_fast_path is conservatively false in
    // the deferred-fast-path model, so this is a no-op here, faithful to a model
    // with no fast-path locks).
    if crate::locking::conflicts_with_relation_fast_path_pub(&locktag, lockmode) {
        let fasthashcode = state::fast_path_strong_lock_hash_partition(hashcode);
        state::FP_STRONG.with(|c| c.borrow_mut().count[fasthashcode] += 1);
    }

    guard.release()?;
    Ok(())
}

/// `lock_twophase_standby_recover(xid, info, recdata, len)` (lock.c) — at hot
/// standby startup, acquire the prepared xact's AccessExclusiveLocks on
/// relations via the standby lock machinery.
pub(crate) fn lock_twophase_standby_recover(
    xid: ::types_core::primitive::TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let rec = decode_record(recdata);
    let locktag = rec.locktag;
    let lockmode = rec.lockmode;
    let lockmethodid = locktag.locktag_lockmethodid;

    if !tables::is_valid_lockmethodid(lockmethodid as u16) {
        return Err(pg_error_internal(
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock method: {lockmethodid}"),
        ));
    }

    if lockmode == AccessExclusiveLock && locktag.locktag_type == LOCKTAG_RELATION {
        standby::standby_acquire_access_exclusive_lock::call(
            xid,
            locktag.locktag_field1, // dbOid
            locktag.locktag_field2, // relOid
        )?;
    }
    Ok(())
}

/// `lock_twophase_postcommit(xid, info, recdata, len)` (lock.c) — release the
/// prepared transaction's lock on COMMIT PREPARED.
pub(crate) fn lock_twophase_postcommit(
    xid: ::types_core::primitive::TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let rec = decode_record(recdata);
    let locktag = rec.locktag;
    let lockmethodid = locktag.locktag_lockmethodid;

    if !tables::is_valid_lockmethodid(lockmethodid as u16) {
        return Err(pg_error_internal(
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock method: {lockmethodid}"),
        ));
    }

    let proc_no = twophase::two_phase_get_dummy_proc_number::call(xid, true)?;
    LockRefindAndRelease(proc_no, &locktag, rec.lockmode, true)
}

/// `lock_twophase_postabort(xid, info, recdata, len)` (lock.c) — identical to
/// the COMMIT PREPARED case.
pub(crate) fn lock_twophase_postabort(
    xid: ::types_core::primitive::TransactionId,
    info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    lock_twophase_postcommit(xid, info, recdata)
}

// ===========================================================================
// GetLockConflicts.
// ===========================================================================

/// `GetLockConflicts(locktag, lockmode, countp)` (lock.c) — the VXIDs of
/// transactions holding a conflicting lock on `locktag`. The C fast-path
/// scanning leg observes an empty fast path (deferred to F3), so only the
/// primary-table holders are examined; `MyProc` never blocks itself.
pub(crate) fn GetLockConflicts<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
) -> PgResult<mcx::PgVec<'mcx, ::types_storage::storage::VirtualTransactionId>> {
    use ::types_storage::storage::VirtualTransactionId;

    let lockmethodid = locktag.locktag_lockmethodid;
    if !tables::is_valid_lockmethodid(lockmethodid as u16) {
        return Err(pg_error_internal(
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock method: {lockmethodid}"),
        ));
    }
    let num_modes = tables::num_lock_modes(lockmethodid as u16);
    if lockmode <= 0 || lockmode > num_modes {
        return Err(pg_error_internal(
            ::types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock mode: {lockmode}"),
        ));
    }

    let conflict_mask = tables::conflict_tab_for(lockmethodid as u16, lockmode);
    let hashcode = LockTagHashCode(locktag);
    let myprocno = proc::my_proc_number::call();

    let mut vxids = mcx::PgVec::new_in(mcx);

    // (Fast-path conflict scan over other backends' fpRelId arrays is empty in
    // the deferred-fast-path model.)

    let guard = lwlock::lwlock_acquire_main::call(
        lock_partition_lock_offset(hashcode),
        LWLockMode::LW_SHARED,
    )?;

    // Examine each existing holder (or awaiter) of the lock.
    let holders: Vec<ProcNumber> = state::with_shared(|s| {
        s.holders(locktag)
            .into_iter()
            .filter(|h| {
                s.proclock_get(locktag, *h)
                    .map(|pl| (conflict_mask & pl.hold_mask) != 0)
                    .unwrap_or(false)
            })
            .collect()
    });

    for holder in holders {
        // A backend never blocks itself.
        if holder == myprocno {
            continue;
        }
        let (procno, lxid) = proc::proc_vxid::call(holder);
        let vxid = VirtualTransactionId {
            procNumber: procno,
            localTransactionId: lxid,
        };
        if vxid.is_valid() {
            vxids.push(vxid);
        }
        // else: xact already committed or aborted.
    }

    guard.release()?;
    Ok(vxids)
}

// ===========================================================================
// GetLockStatusData.
// ===========================================================================

/// `GetLockStatusData()` (lock.c) — one `LockInstanceData` per (PROCLOCK).
/// The fast-path arrays are empty in this model (deferred F3), so only the
/// primary PROCLOCK table is scanned.
pub(crate) fn GetLockStatusData<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, ::types_storage::lock::LockInstanceData>> {
    use ::types_storage::lock::LockInstanceData;
    use ::types_storage::storage::VirtualTransactionId;

    // Acquire all partition locks in partition-number order.
    let mut guards = Vec::with_capacity(NUM_LOCK_PARTITIONS as usize);
    for i in 0..NUM_LOCK_PARTITIONS as i32 {
        guards.push(lwlock::lwlock_acquire_main::call(
            lock_partition_lock_offset_by_index(i),
            LWLockMode::LW_SHARED,
        )?);
    }

    // Scan the PROCLOCK table. Each (LOCKTAG, holder) pair is one instance.
    let entries: Vec<(LOCKTAG, ProcNumber, LOCKMASK, ProcNumber)> = state::with_shared(|s| {
        s.proclock_scan()
            .into_iter()
            .map(|(tag, holder, pl)| (tag, holder, pl.hold_mask, pl.group_leader))
            .collect()
    });

    let mut data = mcx::PgVec::new_in(mcx);
    for (tag, holder, hold_mask, group_leader) in entries {
        let (procno, lxid) = proc::proc_vxid::call(holder);
        let pid = proc::proc_pid::call(holder);
        // waitLockMode: NoLock unless this proc is waiting on exactly this lock
        // (`proc->waitLock == proclock->tag.myLock`). `proc_wait_lock_tag`
        // panics on a non-waiting proc, so gate on `proc_is_waiting_on_lock`.
        let wait_lock_mode = if proc::proc_is_waiting_on_lock::call(holder)
            && proc::proc_wait_lock_tag::call(holder) == tag
        {
            proc::proc_wait_lock_mode::call(holder)
        } else {
            NoLock
        };
        let leader_pid = proc::proc_pid::call(group_leader);
        let wait_start = proc::proc_wait_start::call(holder);

        data.push(LockInstanceData {
            locktag: tag,
            holdMask: hold_mask,
            waitLockMode: wait_lock_mode,
            vxid: VirtualTransactionId {
                procNumber: procno,
                localTransactionId: lxid,
            },
            waitStart: wait_start,
            pid,
            leaderPid: leader_pid,
            fastpath: false,
        });
    }

    // Release in reverse order.
    while let Some(guard) = guards.pop() {
        guard.release()?;
    }

    Ok(data)
}

// ===========================================================================
// proc_locks_hold_masks (proc.c myProcLocks[partition] walk).
// ===========================================================================

/// `GetPGProcByNumber(holder)->myProcLocks[partition]` hold-mask walk (lock.c)
/// — the per-PROCLOCK `holdMask` of every PROCLOCK on `holder`'s partition list.
/// In the ambient model a PROCLOCK belongs to the partition of its LOCK's hash
/// code, keyed `(LOCKTAG, holder)`.
pub(crate) fn proc_locks_hold_masks(holder: ProcNumber, partition: usize) -> Vec<LOCKMASK> {
    state::with_shared(|s| {
        s.proclock_scan()
            .into_iter()
            .filter(|(tag, h, _)| {
                *h == holder
                    && lock_hash_partition(LockTagHashCode(tag)) as usize == partition
            })
            .map(|(_, _, pl)| pl.hold_mask)
            .collect()
    })
}

// ===========================================================================
// GetBlockerStatusData / pg_blocking_pids (lockfuncs.c + lock.c).
// ===========================================================================

/// `pg_blocking_pids(blocked_pid)` = `GetBlockerStatusData(blocked_pid)` +
/// lockfuncs.c's blocker-determination loop, fused. Returns the leader PIDs of
/// the sessions whose heavyweight locks block `blocked_pid` (or any member of its
/// lock group). Duplicates are kept exactly as the C builds them.
///
/// This is the lock-table half of `pg_isolation_test_session_is_blocked`. It
/// holds all partition LWLocks for a self-consistent snapshot (the C
/// `GetBlockerStatusData` contract; ProcArrayLock is not needed here because the
/// PID→ProcNumber lookup goes through procarray's own seam), then for each
/// waiting member of the blocked proc's lock group scans its awaited lock's
/// PROCLOCKs: an entry hard-blocks if it holds a conflicting mode, soft-blocks if
/// it requests a conflicting mode and sits ahead of the blocked proc in the wait
/// queue. Matches C's logic in `pg_blocking_pids` over `GetBlockerStatusData`.
pub(crate) fn blocking_pids<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    blocked_pid: i32,
) -> PgResult<mcx::PgVec<'mcx, i32>> {
    let mut out = mcx::PgVec::new_in(mcx);

    // proc = BackendPidGetProc(blocked_pid); nothing to do if it's gone.
    let blocked_procno = match procarray_seams::backend_pid_get_proc_role::call(
        blocked_pid,
    ) {
        Some((_role, procno)) => procno,
        None => return Ok(out),
    };

    // Acquire all partition LWLocks in partition-number order (the C
    // GetBlockerStatusData snapshot lock; a self-consistent instantaneous state).
    let mut guards = Vec::with_capacity(NUM_LOCK_PARTITIONS as usize);
    for i in 0..NUM_LOCK_PARTITIONS as i32 {
        guards.push(lwlock::lwlock_acquire_main::call(
            lock_partition_lock_offset_by_index(i),
            LWLockMode::LW_SHARED,
        )?);
    }

    // The set of procs to examine = the blocked proc itself, or — if it is a lock
    // group member — every member of its leader's group (the leader points to
    // itself when in a group; INVALID_PROC_NUMBER when not).
    let group_procs: Vec<ProcNumber> = {
        let leader = proc::proc_lock_group_leader::call(blocked_procno);
        if leader == ::types_core::INVALID_PROC_NUMBER {
            alloc::vec![blocked_procno]
        } else {
            proc::proc_lock_group_members::call(leader)
        }
    };

    for member in group_procs {
        // GetSingleProcBlockerStatusData: nothing to do if this member is not
        // blocked (no waitLock).
        if !proc::proc_is_waiting_on_lock::call(member) {
            continue;
        }
        let the_lock: LOCKTAG = proc::proc_wait_lock_tag::call(member);
        let blocked_wait_mode = proc::proc_wait_lock_mode::call(member);
        let blocked_leader_pid = {
            let leader = proc::proc_lock_group_leader::call(member);
            let lp = if leader == ::types_core::INVALID_PROC_NUMBER {
                member
            } else {
                leader
            };
            proc::proc_pid::call(lp)
        };

        let lockmethodid = the_lock.locktag_lockmethodid;
        let conflict_mask = tables::conflict_tab_for(lockmethodid as u16, blocked_wait_mode);

        // Snapshot the PROCLOCKs on theLock and the wait queue ahead of `member`.
        let proclocks: Vec<(ProcNumber, LOCKMASK, ProcNumber)> = state::with_shared(|s| {
            s.holders(&the_lock)
                .into_iter()
                .filter_map(|h| s.proclock_get(&the_lock, h).map(|pl| (h, pl.hold_mask, pl.group_leader)))
                .collect()
        });
        // The PIDs ahead of `member` in theLock's wait queue (the C
        // `preceding_waiters`): walk until we reach `member`.
        let preceding_pids: Vec<i32> = {
            let waiters = state::with_shared(|s| s.waiters(&the_lock));
            let mut v = Vec::new();
            for w in waiters {
                if w == member {
                    break;
                }
                v.push(proc::proc_pid::call(w));
            }
            v
        };

        for (holder, hold_mask, holder_leader) in proclocks {
            // A proc never blocks itself.
            if holder == member {
                continue;
            }
            // Members of the same lock group never block each other. Compare by
            // leader PID, as C does (instance->leaderPid == blocked->leaderPid).
            let holder_leader_pid = proc::proc_pid::call(holder_leader);
            if holder_leader_pid == blocked_leader_pid {
                continue;
            }

            // waitLockMode for this holder is NoLock unless it is itself waiting on
            // exactly theLock.
            let holder_wait_mode = if proc::proc_is_waiting_on_lock::call(holder)
                && proc::proc_wait_lock_tag::call(holder) == the_lock
            {
                proc::proc_wait_lock_mode::call(holder)
            } else {
                NoLock
            };

            let blocks = if (conflict_mask & hold_mask) != 0 {
                // Hard block: blocked by a lock already held by this entry.
                true
            } else if holder_wait_mode != NoLock
                && (conflict_mask & LOCKBIT_ON(holder_wait_mode)) != 0
            {
                // Conflict in lock requests; this entry blocks only if it is ahead
                // of `member` in the wait queue.
                let holder_pid = proc::proc_pid::call(holder);
                preceding_pids.iter().any(|&p| p == holder_pid)
            } else {
                false
            };

            if blocks {
                // Infallible push (mirrors C's palloc'd arrayelems[]; an alloc
                // failure aborts via the allocator, like palloc's ereport).
                out.push(holder_leader_pid);
            }
        }
    }

    // Release in reverse order.
    while let Some(guard) = guards.pop() {
        guard.release()?;
    }

    Ok(out)
}
