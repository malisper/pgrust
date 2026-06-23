//! F5 — hot-standby `KnownAssignedXids` ring + recovery xid bookkeeping
//! (procarray.c).
//!
//! The compressed ring add/search/remove/get operations, the recovery-info
//! application (`ProcArrayApplyRecoveryInfo`/`ProcArrayInitRecovery`/
//! `ProcArrayApplyXidAssignment`), and the expire/idle-maintenance helpers. The
//! ring buffer lives in the F0-owned shmem region (`KNOWN_ASSIGNED_XIDS` +
//! `KNOWN_ASSIGNED_XIDS_VALID` with the cursor bounds in `ProcArrayStruct`); the
//! `RunningTransactionsData` input comes from standby and pg_subtrans is read
//! via the subtrans seam.

use types_core::{
    FirstNormalTransactionId, FullTransactionId, InvalidTransactionId, TransactionId,
    TransactionIdIsNormal, TransactionIdIsValid,
};
use types_error::{ErrorLevel, PgError, PgResult, DEBUG1, DEBUG3, DEBUG4, LOG};
use types_storage::storage::{
    RunningTransactionsData, SUBXIDS_IN_ARRAY, SUBXIDS_IN_SUBTRANS, SUBXIDS_MISSING,
};
use types_storage::LWLockMode::LW_EXCLUSIVE;
use wal::xlogutils::{STANDBY_INITIALIZED, STANDBY_SNAPSHOT_PENDING, STANDBY_SNAPSHOT_READY};

use subtrans_seams as subtrans;
use transam_seams as transam;
use twophase_seams as twophase;
use varsup_seams as varsup;
use xlogutils_seams as xlogutils;
use standby_seams as standby;
use lwlock_seams as lwlock;
use timestamp_seams as timestamp;
use utils_error::elog;
use snapmgr_pc_seams as snapmgr_pc;

use crate::membership::MaintainLatestCompletedXidRecovery;
use crate::shmem_model::{
    KNOWN_ASSIGNED_XIDS, KNOWN_ASSIGNED_XIDS_VALID, LATEST_OBSERVED_XID, PROC_ARRAY,
    STANDBY_SNAPSHOT_PENDING_XMIN,
};

// ---------------------------------------------------------------------------
// Local mirrors of the access/transam.h XID arithmetic macros / wrapper
// functions used pervasively below. The comparison functions reach the real
// modular implementation in transam.c through its seam crate; the advance /
// retreat macros are pure local arithmetic.
// ---------------------------------------------------------------------------

/// `TransactionIdAdvance(dest)` (access/transam.h) — advance a 32-bit XID,
/// handling wraparound over the special XIDs.
#[inline]
fn transaction_id_advance(dest: &mut TransactionId) {
    *dest = dest.wrapping_add(1);
    if *dest < FirstNormalTransactionId {
        *dest = FirstNormalTransactionId;
    }
}

/// `TransactionIdRetreat(dest)` (access/transam.h) — retreat a 32-bit XID,
/// stepping back over the special XIDs.
#[inline]
fn transaction_id_retreat(dest: &mut TransactionId) {
    loop {
        *dest = dest.wrapping_sub(1);
        if *dest >= FirstNormalTransactionId {
            break;
        }
    }
}

/// `FullTransactionIdRetreat(dest)` (access/transam.h) — retreat a 64-bit
/// FullTransactionId, stepping over XIDs that look special only as 32-bit XIDs.
#[inline]
fn full_transaction_id_retreat(dest: &mut FullTransactionId) {
    dest.value -= 1;

    // In contrast to 32bit XIDs don't step over the "actual" special xids.
    if dest.value < types_core::FirstNormalFullTransactionId.value {
        return;
    }

    // But we do need to step over XIDs that'd appear special only for 32bit
    // XIDs.
    while dest.xid() < FirstNormalTransactionId {
        dest.value -= 1;
    }
}

#[inline]
fn precedes(a: TransactionId, b: TransactionId) -> bool {
    transam::transaction_id_precedes::call(a, b)
}

#[inline]
fn precedes_or_equals(a: TransactionId, b: TransactionId) -> bool {
    transam::transaction_id_precedes_or_equals::call(a, b)
}

#[inline]
fn follows(a: TransactionId, b: TransactionId) -> bool {
    transam::transaction_id_follows::call(a, b)
}

#[inline]
fn follows_or_equals(a: TransactionId, b: TransactionId) -> bool {
    transam::transaction_id_follows_or_equals::call(a, b)
}

// ---------------------------------------------------------------------------
// KAXCompressReason (procarray.c) — reason codes for KnownAssignedXidsCompress.
// ---------------------------------------------------------------------------

/// `KAX_NO_SPACE` — need to free up space at array end.
pub const KAX_NO_SPACE: i32 = 0;
/// `KAX_PRUNE` — we just pruned old entries.
pub const KAX_PRUNE: i32 = 1;
/// `KAX_TRANSACTION_END` — we just committed/removed some XIDs.
pub const KAX_TRANSACTION_END: i32 = 2;
/// `KAX_STARTUP_PROCESS_IDLE` — startup process is about to sleep.
pub const KAX_STARTUP_PROCESS_IDLE: i32 = 3;

/// `KAX_COMPRESS_FREQUENCY` (procarray.c) — in transactions.
const KAX_COMPRESS_FREQUENCY: u32 = 128;
/// `KAX_COMPRESS_IDLE_INTERVAL` (procarray.c) — in ms.
const KAX_COMPRESS_IDLE_INTERVAL: i64 = 1000;

// File-static compression heuristic counters (procarray.c
// `KnownAssignedXidsCompress` function-local statics). Backend-private, so
// thread-local per the forked-child convention.
thread_local! {
    static TRANSACTION_ENDS_COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static LAST_COMPRESS_TS: std::cell::Cell<types_core::TimestampTz> = const { std::cell::Cell::new(0) };
}

/// `KnownAssignedXidsCompress(KAXCompressReason reason, bool haveLock)`
/// (procarray.c, static) — slide the valid ring entries down to the front,
/// dropping invalidated slots, when the ring has become sparse.
pub fn KnownAssignedXidsCompress(reason: i32, have_lock: bool) {
    // Since only the startup process modifies the head/tail pointers, we don't
    // need a lock to read them here.
    let (head, tail, num) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let p = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (
            p.headKnownAssignedXids,
            p.tailKnownAssignedXids,
            p.numKnownAssignedXids,
        )
    });
    let nelements = head - tail;

    // If we can choose whether to compress, use a heuristic to avoid
    // compressing too often or not often enough.
    if nelements == num {
        // When there are no gaps between head and tail, don't bother to
        // compress, except in the KAX_NO_SPACE case where we must compress to
        // create some space after the head.
        if reason != KAX_NO_SPACE {
            return;
        }
    } else if reason == KAX_TRANSACTION_END {
        // Consider compressing only once every so many commits.
        let counter = TRANSACTION_ENDS_COUNTER.with(|c| {
            let v = c.get();
            c.set(v.wrapping_add(1));
            v
        });
        if counter % KAX_COMPRESS_FREQUENCY != 0 {
            return;
        }
        // Furthermore, compress only if the used part of the array is less than
        // 50% full.
        if nelements < 2 * num {
            return;
        }
    } else if reason == KAX_STARTUP_PROCESS_IDLE {
        // We're about to go idle for lack of new WAL, so we might as well
        // compress.  But not too often, to avoid ProcArray lock contention.
        let last = LAST_COMPRESS_TS.with(|c| c.get());
        if last != 0 {
            let compress_after = last + KAX_COMPRESS_IDLE_INTERVAL * 1000;
            if timestamp::get_current_timestamp::call() < compress_after {
                return;
            }
        }
    }

    // Need to compress, so get the lock if we don't have it.
    if !have_lock {
        lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)
            .expect("KnownAssignedXidsCompress: ProcArrayLock acquire");
    }

    // We compress the array by reading the valid values from tail to head,
    // re-aligning data to 0th element.
    let compress_index = KNOWN_ASSIGNED_XIDS.with(|kax| {
        KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| {
            let mut kax = kax.borrow_mut();
            let mut kaxv = kaxv.borrow_mut();
            let mut compress_index = 0usize;
            for i in tail as usize..head as usize {
                if kaxv[i] {
                    kax[compress_index] = kax[i];
                    kaxv[compress_index] = true;
                    compress_index += 1;
                }
            }
            compress_index
        })
    });

    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
        debug_assert_eq!(compress_index as i32, p.numKnownAssignedXids);
        p.tailKnownAssignedXids = 0;
        p.headKnownAssignedXids = compress_index as i32;
    });

    if !have_lock {
        lwlock::lwlock_release_proc_array::call()
            .expect("KnownAssignedXidsCompress: ProcArrayLock release");
    }

    // Update timestamp for maintenance.  No need to hold lock for this.
    LAST_COMPRESS_TS.with(|c| c.set(timestamp::get_current_timestamp::call()));
}

/// `KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid,
/// bool exclusive_lock)` (procarray.c) — append the (possibly multi-xid) range
/// to the ring, compressing first if needed.
pub fn KnownAssignedXidsAdd(
    from_xid: TransactionId,
    to_xid: TransactionId,
    exclusive_lock: bool,
) -> PgResult<()> {
    debug_assert!(precedes_or_equals(from_xid, to_xid));

    // Calculate how many array slots we'll need.  Normally this is cheap; in
    // the unusual case where the XIDs cross the wrap point, we do it the hard
    // way.
    let nxids: i32 = if to_xid >= from_xid {
        (to_xid - from_xid + 1) as i32
    } else {
        let mut n = 1i32;
        let mut next_xid = from_xid;
        while precedes(next_xid, to_xid) {
            n += 1;
            transaction_id_advance(&mut next_xid);
        }
        n
    };

    // Since only the startup process modifies the head/tail pointers, we don't
    // need a lock to read them here.
    let (mut head, tail, max) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let p = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (
            p.headKnownAssignedXids,
            p.tailKnownAssignedXids,
            p.maxKnownAssignedXids,
        )
    });

    debug_assert!(head >= 0 && head <= max);
    debug_assert!(tail >= 0 && tail < max);

    // Verify that insertions occur in TransactionId sequence.  Note that even
    // if the last existing element is marked invalid, it must still have a
    // correctly sequenced XID value.
    if head > tail {
        let last = KNOWN_ASSIGNED_XIDS.with(|kax| kax.borrow()[(head - 1) as usize]);
        if follows_or_equals(last, from_xid) {
            KnownAssignedXidsDisplay(LOG);
            return Err(PgError::error(
                "out-of-order XID insertion in KnownAssignedXids",
            ));
        }
    }

    // If our xids won't fit in the remaining space, compress out free space.
    if head + nxids > max {
        KnownAssignedXidsCompress(KAX_NO_SPACE, exclusive_lock);

        head = PROC_ARRAY.with(|pa| {
            pa.borrow()
                .as_ref()
                .expect("ProcArray accessed before ProcArrayShmemInit")
                .headKnownAssignedXids
        });
        // note: we no longer care about the tail pointer

        // If it still won't fit then we're out of memory.
        if head + nxids > max {
            return Err(PgError::error("too many KnownAssignedXids"));
        }
    }

    // Now we can insert the xids into the space starting at head.
    KNOWN_ASSIGNED_XIDS.with(|kax| {
        KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| {
            let mut kax = kax.borrow_mut();
            let mut kaxv = kaxv.borrow_mut();
            let mut next_xid = from_xid;
            for _ in 0..nxids {
                kax[head as usize] = next_xid;
                kaxv[head as usize] = true;
                transaction_id_advance(&mut next_xid);
                head += 1;
            }
        })
    });

    // Adjust count of number of valid entries, and update the head pointer.
    // (The C uses pg_write_barrier() before publishing head when not holding
    // the lock exclusively; in this single-startup-process model the ring +
    // bounds are backend-local so no cross-process barrier is required.)
    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
        p.numKnownAssignedXids += nxids;
        p.headKnownAssignedXids = head;
    });

    Ok(())
}

/// `KnownAssignedXidsSearch(TransactionId xid, bool remove)` (procarray.c,
/// static) — binary-search the ring for `xid`, optionally invalidating its slot.
pub fn KnownAssignedXidsSearch(xid: TransactionId, remove: bool) -> bool {
    let (head, tail) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let p = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (p.headKnownAssignedXids, p.tailKnownAssignedXids)
    });

    // Standard binary search.  Note we can ignore the KnownAssignedXidsValid
    // array here, since even invalid entries will contain sorted XIDs.
    let mut result_index: i32 = -1;
    KNOWN_ASSIGNED_XIDS.with(|kax| {
        let kax = kax.borrow();
        let mut first = tail;
        let mut last = head - 1;
        while first <= last {
            let mid_index = (first + last) / 2;
            let mid_xid = kax[mid_index as usize];

            if xid == mid_xid {
                result_index = mid_index;
                break;
            } else if precedes(xid, mid_xid) {
                last = mid_index - 1;
            } else {
                first = mid_index + 1;
            }
        }
    });

    if result_index < 0 {
        return false; // not in array
    }

    let valid = KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| kaxv.borrow()[result_index as usize]);
    if !valid {
        return false; // in array, but invalid
    }

    if remove {
        KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| kaxv.borrow_mut()[result_index as usize] = false);

        PROC_ARRAY.with(|pa| {
            let mut b = pa.borrow_mut();
            let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
            p.numKnownAssignedXids -= 1;
            debug_assert!(p.numKnownAssignedXids >= 0);
        });

        // If we're removing the tail element then advance tail pointer over any
        // invalid elements.  This will speed future searches.
        if result_index == tail {
            let mut tail = tail;
            tail += 1;
            KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| {
                let kaxv = kaxv.borrow();
                while tail < head && !kaxv[tail as usize] {
                    tail += 1;
                }
            });
            PROC_ARRAY.with(|pa| {
                let mut b = pa.borrow_mut();
                let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
                if tail >= head {
                    // Array is empty, so we can reset both pointers.
                    p.headKnownAssignedXids = 0;
                    p.tailKnownAssignedXids = 0;
                } else {
                    p.tailKnownAssignedXids = tail;
                }
            });
        }
    }

    true
}

/// `KnownAssignedXidExists(TransactionId xid)` (procarray.c).
pub fn KnownAssignedXidExists(xid: TransactionId) -> bool {
    debug_assert!(TransactionIdIsValid(xid));

    KnownAssignedXidsSearch(xid, false)
}

/// `KnownAssignedXidsRemove(TransactionId xid)` (procarray.c).
pub fn KnownAssignedXidsRemove(xid: TransactionId) {
    debug_assert!(TransactionIdIsValid(xid));

    let _ = elog(DEBUG4, format!("remove KnownAssignedXid {xid}"));

    // Note: we cannot consider it an error to remove an XID that's not present.
    // We intentionally remove subxact IDs while processing XLOG_XACT_ASSIGNMENT,
    // to avoid array overflow.  Then those XIDs will be removed again when the
    // top-level xact commits or aborts.  So, just ignore the search result.
    let _ = KnownAssignedXidsSearch(xid, true);
}

/// `KnownAssignedXidsRemoveTree(TransactionId xid, int nsubxids,
/// TransactionId *subxids)` (procarray.c) — remove a top xid and its subxids.
pub fn KnownAssignedXidsRemoveTree(xid: TransactionId, subxids: &[TransactionId]) {
    if TransactionIdIsValid(xid) {
        KnownAssignedXidsRemove(xid);
    }

    for &sub in subxids {
        KnownAssignedXidsRemove(sub);
    }

    // Opportunistically compress the array.
    KnownAssignedXidsCompress(KAX_TRANSACTION_END, true);
}

/// `KnownAssignedXidsRemovePreceding(TransactionId removeXid)` (procarray.c) —
/// drop every entry `<= removeXid`. (May propagate `Err` from the
/// `StandbyTransactionIdIsPrepared` seam.)
pub fn KnownAssignedXidsRemovePreceding(remove_xid: TransactionId) -> PgResult<()> {
    if !TransactionIdIsValid(remove_xid) {
        let _ = elog(DEBUG4, "removing all KnownAssignedXids");
        PROC_ARRAY.with(|pa| {
            let mut b = pa.borrow_mut();
            let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
            p.numKnownAssignedXids = 0;
            p.headKnownAssignedXids = 0;
            p.tailKnownAssignedXids = 0;
        });
        return Ok(());
    }

    let _ = elog(DEBUG4, format!("prune KnownAssignedXids to {remove_xid}"));

    // Mark entries invalid starting at the tail.  Since array is sorted, we can
    // stop as soon as we reach an entry >= removeXid.
    let (head, tail) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let p = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (p.headKnownAssignedXids, p.tailKnownAssignedXids)
    });

    let mut count = 0i32;
    for i in tail..head {
        let valid = KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| kaxv.borrow()[i as usize]);
        if valid {
            let known_xid = KNOWN_ASSIGNED_XIDS.with(|kax| kax.borrow()[i as usize]);

            if follows_or_equals(known_xid, remove_xid) {
                break;
            }

            if !twophase::standby_transaction_id_is_prepared::call(known_xid)? {
                KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| kaxv.borrow_mut()[i as usize] = false);
                count += 1;
            }
        }
    }

    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
        p.numKnownAssignedXids -= count;
        debug_assert!(p.numKnownAssignedXids >= 0);
    });

    // Advance the tail pointer if we've marked the tail item invalid.
    let mut i = tail;
    KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| {
        let kaxv = kaxv.borrow();
        while i < head {
            if kaxv[i as usize] {
                break;
            }
            i += 1;
        }
    });
    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
        if i >= head {
            // Array is empty, so we can reset both pointers.
            p.headKnownAssignedXids = 0;
            p.tailKnownAssignedXids = 0;
        } else {
            p.tailKnownAssignedXids = i;
        }
    });

    // Opportunistically compress the array.
    KnownAssignedXidsCompress(KAX_PRUNE, true);
    Ok(())
}

/// `KnownAssignedXidsGet(TransactionId *xarray, TransactionId xmax)`
/// (procarray.c) — copy the valid entries `<= xmax` into a caller array;
/// returns the count.
pub fn KnownAssignedXidsGet(xarray: &mut [TransactionId], xmax: TransactionId) -> i32 {
    let mut xtmp = InvalidTransactionId;
    KnownAssignedXidsGetAndSetXmin(xarray, &mut xtmp, xmax)
}

/// `KnownAssignedXidsGetAndSetXmin(TransactionId *xarray, TransactionId *xmin,
/// TransactionId xmax)` (procarray.c) — like `Get` but also lowers `*xmin` to
/// the oldest entry seen.
pub fn KnownAssignedXidsGetAndSetXmin(
    xarray: &mut [TransactionId],
    xmin: &mut TransactionId,
    xmax: TransactionId,
) -> i32 {
    // Fetch head just once, since it may change while we loop. We can stop once
    // we reach the initially seen head, since we are certain that an xid cannot
    // enter and then leave the array while we hold ProcArrayLock.
    let (head, tail) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let p = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (p.headKnownAssignedXids, p.tailKnownAssignedXids)
    });

    let mut count = 0usize;
    KNOWN_ASSIGNED_XIDS.with(|kax| {
        KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| {
            let kax = kax.borrow();
            let kaxv = kaxv.borrow();
            for i in tail as usize..head as usize {
                // Skip any gaps in the array.
                if kaxv[i] {
                    let known_xid = kax[i];

                    // Update xmin if required.  Only the first XID need be
                    // checked, since the array is sorted.
                    if count == 0 && precedes(known_xid, *xmin) {
                        *xmin = known_xid;
                    }

                    // Filter out anything >= xmax, again relying on sorted
                    // property of array.
                    if TransactionIdIsValid(xmax) && follows_or_equals(known_xid, xmax) {
                        break;
                    }

                    // Add knownXid into output array.
                    xarray[count] = known_xid;
                    count += 1;
                }
            }
        })
    });

    count as i32
}

/// `KnownAssignedXidsGetOldestXmin(void)` (procarray.c) — the oldest still-valid
/// entry, or `InvalidTransactionId`.
pub fn KnownAssignedXidsGetOldestXmin() -> TransactionId {
    // Fetch head just once, since it may change while we loop.
    let (head, tail) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let p = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (p.headKnownAssignedXids, p.tailKnownAssignedXids)
    });

    KNOWN_ASSIGNED_XIDS.with(|kax| {
        KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| {
            let kax = kax.borrow();
            let kaxv = kaxv.borrow();
            for i in tail as usize..head as usize {
                // Skip any gaps in the array.
                if kaxv[i] {
                    return kax[i];
                }
            }
            InvalidTransactionId
        })
    })
}

/// `KnownAssignedXidsDisplay(int trace_level)` (procarray.c) — debug dump of the
/// ring.
pub fn KnownAssignedXidsDisplay(trace_level: ErrorLevel) {
    let (head, tail, num) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let p = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (
            p.headKnownAssignedXids,
            p.tailKnownAssignedXids,
            p.numKnownAssignedXids,
        )
    });

    let mut buf = String::new();
    let mut nxids = 0i32;

    KNOWN_ASSIGNED_XIDS.with(|kax| {
        KNOWN_ASSIGNED_XIDS_VALID.with(|kaxv| {
            let kax = kax.borrow();
            let kaxv = kaxv.borrow();
            for i in tail as usize..head as usize {
                if kaxv[i] {
                    nxids += 1;
                    buf.push_str(&format!("[{}]={} ", i, kax[i]));
                }
            }
        })
    });

    let _ = elog(
        trace_level,
        format!("{nxids} KnownAssignedXids (num={num} tail={tail} head={head}) {buf}"),
    );
}

/// `KnownAssignedXidsReset(void)` (procarray.c) — drop the entire ring (standby
/// promotion / shutdown).
pub fn KnownAssignedXidsReset() {
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)
        .expect("KnownAssignedXidsReset: ProcArrayLock acquire");

    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
        p.numKnownAssignedXids = 0;
        p.tailKnownAssignedXids = 0;
        p.headKnownAssignedXids = 0;
    });

    lwlock::lwlock_release_proc_array::call()
        .expect("KnownAssignedXidsReset: ProcArrayLock release");
}

/// `KnownAssignedTransactionIdsIdleMaintenance(void)` (procarray.c) — the public
/// idle-maintenance entry the startup process calls.
pub fn KnownAssignedTransactionIdsIdleMaintenance() {
    KnownAssignedXidsCompress(KAX_STARTUP_PROCESS_IDLE, false);
}

// ---------------------------------------------------------------------------
// Recovery xid bookkeeping.
// ---------------------------------------------------------------------------

/// `ProcArrayInitRecovery(TransactionId initializedUptoXID)` (procarray.c) —
/// seed `latestObservedXid` at the start of recovery.
pub fn ProcArrayInitRecovery(initialized_upto_xid: TransactionId) {
    debug_assert_eq!(xlogutils::standby_state::call(), STANDBY_INITIALIZED);
    debug_assert!(TransactionIdIsNormal(initialized_upto_xid));

    // we set latestObservedXid to the xid SUBTRANS has been initialized up to,
    // so we can extend it from that point onwards in
    // RecordKnownAssignedTransactionIds, and when we get consistent in
    // ProcArrayApplyRecoveryInfo().
    let mut latest = initialized_upto_xid;
    transaction_id_retreat(&mut latest);
    LATEST_OBSERVED_XID.with(|l| *l.borrow_mut() = latest);
}

/// `ProcArrayApplyRecoveryInfo(RunningTransactions running)` (procarray.c) —
/// rebuild the KnownAssignedXids ring from a `XLOG_RUNNING_XACTS` record.
pub fn ProcArrayApplyRecoveryInfo(running: &RunningTransactionsData<'_>) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state::call() >= STANDBY_INITIALIZED);
    debug_assert!(TransactionIdIsValid(running.nextXid));
    debug_assert!(TransactionIdIsValid(running.oldestRunningXid));
    debug_assert!(TransactionIdIsNormal(running.latestCompletedXid));

    // Remove stale transactions, if any.
    ExpireOldKnownAssignedTransactionIds(running.oldestRunningXid)?;

    // Adjust TransamVariables->nextXid before StandbyReleaseOldLocks(), because
    // we will need it up to date for accessing two-phase transactions in
    // StandbyReleaseOldLocks().
    let mut advance_next_xid = running.nextXid;
    transaction_id_retreat(&mut advance_next_xid);
    varsup::advance_next_full_xid_past_xid::call(advance_next_xid)?;
    debug_assert!(varsup::read_next_full_transaction_id::call().is_valid());

    // Remove stale locks, if any.
    standby::standby_release_old_locks::call(running.oldestRunningXid)?;

    // If our snapshot is already valid, nothing else to do...
    if xlogutils::standby_state::call() == STANDBY_SNAPSHOT_READY {
        return Ok(());
    }

    // If our initial RunningTransactionsData had an overflowed snapshot then we
    // knew we were missing some subxids from our snapshot.
    if xlogutils::standby_state::call() == STANDBY_SNAPSHOT_PENDING {
        // If the snapshot isn't overflowed or if its empty we can reset our
        // pending state and use this snapshot instead.
        if running.subxid_status != SUBXIDS_MISSING || running.xcnt == 0 {
            // If we have already collected known assigned xids, we need to throw
            // them away before we apply the recovery snapshot.
            KnownAssignedXidsReset();
            xlogutils::set_standby_state::call(STANDBY_INITIALIZED);
        } else {
            let pending = STANDBY_SNAPSHOT_PENDING_XMIN.with(|p| *p.borrow());
            if precedes(pending, running.oldestRunningXid) {
                xlogutils::set_standby_state::call(STANDBY_SNAPSHOT_READY);
                let _ = elog(DEBUG1, "recovery snapshots are now enabled");
            } else {
                let _ = elog(
                    DEBUG1,
                    format!(
                        "recovery snapshot waiting for non-overflowed snapshot or until oldest active xid on standby is at least {} (now {})",
                        pending, running.oldestRunningXid
                    ),
                );
            }
            return Ok(());
        }
    }

    debug_assert_eq!(xlogutils::standby_state::call(), STANDBY_INITIALIZED);

    // NB: this can be reached at least twice, so make sure new code can deal
    // with that.

    // Nobody else is running yet, but take locks anyhow.
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;

    // KnownAssignedXids is sorted so we cannot just add the xids, we have to
    // sort them first.
    //
    // Allocate a temporary array to avoid modifying the array passed as
    // argument, and add to it any xids which have not already completed.
    //
    // C reads the TransactionXmin global inside TransactionIdDidCommit/Abort;
    // here it is threaded explicitly, so source it from snapmgr.
    let transaction_xmin = match snapmgr_pc::transaction_xmin::call() {
        Ok(v) => v,
        Err(e) => {
            lwlock::lwlock_release_proc_array::call()?;
            return Err(e);
        }
    };
    let total = (running.xcnt + running.subxcnt) as usize;
    let mut xids: Vec<TransactionId> = Vec::with_capacity(total);
    for i in 0..total {
        let xid = running.xids[i];

        // The running-xacts snapshot can contain xids that were still visible in
        // the procarray when the snapshot was taken, but were already WAL-logged
        // as completed. They're not running anymore, so ignore them.
        let done = (|| -> PgResult<bool> {
            Ok(transam::transaction_id_did_commit::call(xid, transaction_xmin)?
                || transam::transaction_id_did_abort::call(xid, transaction_xmin)?)
        })();
        match done {
            Ok(true) => continue,
            Ok(false) => xids.push(xid),
            Err(e) => {
                lwlock::lwlock_release_proc_array::call()?;
                return Err(e);
            }
        }
    }

    let mut apply_err: PgResult<()> = Ok(());
    if !xids.is_empty() {
        let num = PROC_ARRAY.with(|pa| {
            pa.borrow()
                .as_ref()
                .expect("ProcArray accessed before ProcArrayShmemInit")
                .numKnownAssignedXids
        });
        if num != 0 {
            lwlock::lwlock_release_proc_array::call()?;
            return Err(PgError::error("KnownAssignedXids is not empty"));
        }

        // Sort the array so that we can add them safely into KnownAssignedXids.
        // We sort logically (xidLogicalComparator), via the modular comparison.
        xids.sort_by(|&a, &b| {
            if precedes(a, b) {
                std::cmp::Ordering::Less
            } else if precedes(b, a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        // Add the sorted snapshot into KnownAssignedXids.  The running-xacts
        // snapshot may include duplicated xids because of prepared transactions,
        // so ignore them.
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
            apply_err = KnownAssignedXidsAdd(xids[i], xids[i], true);
            if apply_err.is_err() {
                break;
            }
        }

        if apply_err.is_ok() {
            KnownAssignedXidsDisplay(DEBUG3);
        }
    }

    if let Err(e) = apply_err {
        lwlock::lwlock_release_proc_array::call()?;
        return Err(e);
    }

    // latestObservedXid is at least set to the point where SUBTRANS was started
    // up to (cf. ProcArrayInitRecovery()) or to the biggest xid
    // RecordKnownAssignedTransactionIds() was called for.  Initialize subtrans
    // from thereon, up to nextXid - 1.
    let mut latest = LATEST_OBSERVED_XID.with(|l| *l.borrow());
    debug_assert!(TransactionIdIsNormal(latest));
    transaction_id_advance(&mut latest);
    while precedes(latest, running.nextXid) {
        if let Err(e) = subtrans::extend_subtrans::call(latest) {
            lwlock::lwlock_release_proc_array::call()?;
            return Err(e);
        }
        transaction_id_advance(&mut latest);
    }
    transaction_id_retreat(&mut latest); // = running->nextXid - 1
    LATEST_OBSERVED_XID.with(|l| *l.borrow_mut() = latest);

    // Now we've got the running xids we need to set the global values that are
    // used to track snapshots as they evolve further.
    if running.subxid_status == SUBXIDS_MISSING {
        xlogutils::set_standby_state::call(STANDBY_SNAPSHOT_PENDING);

        STANDBY_SNAPSHOT_PENDING_XMIN.with(|p| *p.borrow_mut() = latest);
        PROC_ARRAY.with(|pa| {
            pa.borrow_mut()
                .as_mut()
                .expect("ProcArray accessed before ProcArrayShmemInit")
                .lastOverflowedXid = latest;
        });
    } else {
        xlogutils::set_standby_state::call(STANDBY_SNAPSHOT_READY);

        STANDBY_SNAPSHOT_PENDING_XMIN.with(|p| *p.borrow_mut() = InvalidTransactionId);

        // If the 'xids' array didn't include all subtransactions, we have to
        // mark any snapshots taken as overflowed.
        if running.subxid_status == SUBXIDS_IN_SUBTRANS {
            PROC_ARRAY.with(|pa| {
                pa.borrow_mut()
                    .as_mut()
                    .expect("ProcArray accessed before ProcArrayShmemInit")
                    .lastOverflowedXid = latest;
            });
        } else {
            debug_assert_eq!(running.subxid_status, SUBXIDS_IN_ARRAY);
            PROC_ARRAY.with(|pa| {
                pa.borrow_mut()
                    .as_mut()
                    .expect("ProcArray accessed before ProcArrayShmemInit")
                    .lastOverflowedXid = InvalidTransactionId;
            });
        }
    }

    // If a transaction wrote a commit record in the gap between taking and
    // logging the snapshot then latestCompletedXid may already be higher than
    // the value from the snapshot, so check before we use the incoming value.
    MaintainLatestCompletedXidRecovery(running.latestCompletedXid);

    // NB: No need to increment TransamVariables->xactCompletionCount here,
    // nobody can see it yet.

    lwlock::lwlock_release_proc_array::call()?;

    KnownAssignedXidsDisplay(DEBUG3);
    if xlogutils::standby_state::call() == STANDBY_SNAPSHOT_READY {
        let _ = elog(DEBUG1, "recovery snapshots are now enabled");
    } else {
        let pending = STANDBY_SNAPSHOT_PENDING_XMIN.with(|p| *p.borrow());
        let _ = elog(
            DEBUG1,
            format!(
                "recovery snapshot waiting for non-overflowed snapshot or until oldest active xid on standby is at least {} (now {})",
                pending, running.oldestRunningXid
            ),
        );
    }

    Ok(())
}

/// `ProcArrayApplyXidAssignment(TransactionId topxid, int nsubxids,
/// TransactionId *subxids)` (procarray.c) — redo-side subxid bookkeeping.
pub fn ProcArrayApplyXidAssignment(
    xtop: TransactionId,
    subxids: &[TransactionId],
) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state::call() >= STANDBY_INITIALIZED);

    let max_xid = transam::transaction_id_latest::call(xtop, subxids);

    // Mark all the subtransactions as observed.
    RecordKnownAssignedTransactionIds(max_xid)?;

    // Notice that we update pg_subtrans with the top-level xid, rather than the
    // parent xid.  This is a difference between normal processing and recovery,
    // yet is still correct in all cases.
    for &sub in subxids {
        subtrans::sub_trans_set_parent::call(sub, xtop)?;
    }

    // KnownAssignedXids isn't maintained yet, so we're done for now.
    if xlogutils::standby_state::call() == STANDBY_INITIALIZED {
        return Ok(());
    }

    // Uses same locking as transaction commit.
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;

    // Remove subxids from known-assigned-xacts.
    KnownAssignedXidsRemoveTree(InvalidTransactionId, subxids);

    // Advance lastOverflowedXid to be at least the last of these subxids.
    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
        if precedes(p.lastOverflowedXid, max_xid) {
            p.lastOverflowedXid = max_xid;
        }
    });

    lwlock::lwlock_release_proc_array::call()?;
    Ok(())
}

/// `RecordKnownAssignedTransactionIds(TransactionId xid)` (procarray.c) — extend
/// the ring (and `latestObservedXid`) to cover a newly-seen xid during recovery.
pub fn RecordKnownAssignedTransactionIds(xid: TransactionId) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state::call() >= STANDBY_INITIALIZED);
    debug_assert!(TransactionIdIsValid(xid));
    let latest_observed = LATEST_OBSERVED_XID.with(|l| *l.borrow());
    debug_assert!(TransactionIdIsValid(latest_observed));

    let _ = elog(
        DEBUG4,
        format!("record known xact {xid} latestObservedXid {latest_observed}"),
    );

    // When a newly observed xid arrives, it is frequently the case that it is
    // *not* the next xid in sequence. When this occurs, we must treat the
    // intervening xids as running also.
    if follows(xid, latest_observed) {
        // Extend subtrans like we do in GetNewTransactionId() during normal
        // operation using individual extend steps.
        let mut next_expected_xid = latest_observed;
        while precedes(next_expected_xid, xid) {
            transaction_id_advance(&mut next_expected_xid);
            subtrans::extend_subtrans::call(next_expected_xid)?;
        }
        debug_assert_eq!(next_expected_xid, xid);

        // If the KnownAssignedXids machinery isn't up yet, there's nothing more
        // to do since we don't track assigned xids yet.
        if xlogutils::standby_state::call() <= STANDBY_INITIALIZED {
            LATEST_OBSERVED_XID.with(|l| *l.borrow_mut() = xid);
            return Ok(());
        }

        // Add (latestObservedXid, xid] onto the KnownAssignedXids array.
        let mut next_expected_xid = latest_observed;
        transaction_id_advance(&mut next_expected_xid);
        KnownAssignedXidsAdd(next_expected_xid, xid, false)?;

        // Now we can advance latestObservedXid.
        LATEST_OBSERVED_XID.with(|l| *l.borrow_mut() = xid);

        // TransamVariables->nextXid must be beyond any observed xid.
        varsup::advance_next_full_xid_past_xid::call(xid)?;
    }

    Ok(())
}

/// `ExpireTreeKnownAssignedTransactionIds(TransactionId xid, int nsubxids,
/// TransactionId *subxids, TransactionId max_xid)` (procarray.c) — remove a
/// committed/aborted xid tree from the ring and advance latest-completed.
pub fn ExpireTreeKnownAssignedTransactionIds(
    xid: TransactionId,
    subxids: &[TransactionId],
    max_xid: TransactionId,
) -> PgResult<()> {
    debug_assert!(xlogutils::standby_state::call() >= STANDBY_INITIALIZED);

    // Uses same locking as transaction commit.
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;

    KnownAssignedXidsRemoveTree(xid, subxids);

    // As in ProcArrayEndTransaction, advance latestCompletedXid.
    MaintainLatestCompletedXidRecovery(max_xid);

    // ... and xactCompletionCount.
    varsup::increment_xact_completion_count::call();

    lwlock::lwlock_release_proc_array::call()?;
    Ok(())
}

/// `ExpireAllKnownAssignedTransactionIds(void)` (procarray.c) — drop the whole
/// ring (e.g. on `XLOG_RUNNING_XACTS` with an empty snapshot).
pub fn ExpireAllKnownAssignedTransactionIds() -> PgResult<()> {
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;
    if let Err(e) = KnownAssignedXidsRemovePreceding(InvalidTransactionId) {
        lwlock::lwlock_release_proc_array::call()?;
        return Err(e);
    }

    // Reset latestCompletedXid to nextXid - 1.
    let mut latest_xid = varsup::read_next_full_transaction_id::call();
    debug_assert!(latest_xid.is_valid());
    full_transaction_id_retreat(&mut latest_xid);
    varsup::set_latest_completed_xid::call(latest_xid);

    // Any transactions that were in-progress were effectively aborted, so
    // advance xactCompletionCount.
    varsup::increment_xact_completion_count::call();

    // Reset lastOverflowedXid.
    PROC_ARRAY.with(|pa| {
        pa.borrow_mut()
            .as_mut()
            .expect("ProcArray accessed before ProcArrayShmemInit")
            .lastOverflowedXid = InvalidTransactionId;
    });

    lwlock::lwlock_release_proc_array::call()?;
    Ok(())
}

/// `ExpireOldKnownAssignedTransactionIds(TransactionId xid)` (procarray.c) —
/// drop entries older than `xid`.
pub fn ExpireOldKnownAssignedTransactionIds(xid: TransactionId) -> PgResult<()> {
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;

    // As in ProcArrayEndTransaction, advance latestCompletedXid.
    let mut latest_xid = xid;
    transaction_id_retreat(&mut latest_xid);
    MaintainLatestCompletedXidRecovery(latest_xid);

    // ... and xactCompletionCount.
    varsup::increment_xact_completion_count::call();

    // Reset lastOverflowedXid if we know all transactions that have been
    // possibly running are being gone.
    PROC_ARRAY.with(|pa| {
        let mut b = pa.borrow_mut();
        let p = b.as_mut().expect("ProcArray accessed before ProcArrayShmemInit");
        if precedes(p.lastOverflowedXid, xid) {
            p.lastOverflowedXid = InvalidTransactionId;
        }
    });
    let r = KnownAssignedXidsRemovePreceding(xid);
    lwlock::lwlock_release_proc_array::call()?;
    r
}

/// Install the F5-owned inward seams: recovery-info / expire-all /
/// xid-assignment, consumed by standby + xact redo.
pub fn init_seams() {
    use procarray_seams as seams;

    seams::proc_array_apply_recovery_info::set(ProcArrayApplyRecoveryInfo);
    seams::expire_all_known_assigned_transaction_ids::set(ExpireAllKnownAssignedTransactionIds);
    seams::proc_array_apply_xid_assignment::set(ProcArrayApplyXidAssignment);
    seams::record_known_assigned_transaction_ids::set(RecordKnownAssignedTransactionIds);
    seams::expire_tree_known_assigned_transaction_ids::set(ExpireTreeKnownAssignedTransactionIds);
    seams::known_assigned_transaction_ids_idle_maintenance::set(
        KnownAssignedTransactionIdsIdleMaintenance,
    );
}
