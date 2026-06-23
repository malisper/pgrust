//! The `pg_stat_get_wal_senders` SRF and `offset_to_interval`.
//!
//! The per-row decision logic (privilege gate, priority adjustment for
//! invalid-flush standbys, sync-state classification, `offset_to_interval`) is
//! ported here.  The SRF plumbing (`InitMaterializedSRF`, the `Datum`
//! conversions, `tuplestore_putvalues`) belongs to fmgr/funcapi and is reached
//! through its owner; each assembled row is handed to that owner.

#![allow(non_snake_case)]

use alloc::vec::Vec;

use crate::core::{
    proc_get, Interval, SyncState, TimeOffset, WalSenderRow, WalSnd, InvalidXLogRecPtr,
    ROLE_PG_READ_ALL_STATS,
};
use crate::init::WalSndGetStateString;
use crate::{acl, miscinit, syncrep};

/// `Datum pg_stat_get_wal_senders(PG_FUNCTION_ARGS)` — set-returning function
/// listing the active walsenders and their lag/state.
///
/// The fcinfo / tuplestore is owned by funcapi; this builds each row's values
/// and hands them off.  Returns the assembled rows for the caller-owned SRF
/// emission (the privileged-detail and sync classification computed here).
pub fn pg_stat_get_wal_senders() -> Vec<WalSenderRow> {
    // Currently-active synchronous standbys (may be stale; used anyway): the
    // (walsnd_index, pid) pairs the SRF matches on.
    let candidates = syncrep::sync_rep_get_candidate_standbys::call();

    let mut rows = Vec::new();
    let max = proc_get(|p| p.max_wal_senders);
    let me_pid = miscinit::my_proc_pid::call();
    let _ = me_pid;
    let mut i: i32 = 0;
    while i < max {
        // Collect data from shared memory under the slot mutex.
        let snap: WalSnd = crate::shmem_array::slot_snapshot(i);
        if snap.pid == 0 {
            i += 1;
            continue;
        }

        // Detect whether this walsender is/was considered synchronous (the PID
        // guards against stale data).
        let is_sync_standby = candidates
            .iter()
            .any(|&(idx, pid)| idx == i && pid == snap.pid);

        let mut row = WalSenderRow {
            pid: snap.pid,
            has_details: false,
            state: WalSndGetStateString(snap.state),
            sent_ptr: snap.sentPtr,
            write: snap.write,
            flush: snap.flush,
            apply: snap.apply,
            write_lag: None,
            flush_lag: None,
            apply_lag: None,
            sync_priority: snap.sync_standby_priority,
            sync_state: SyncState::Async,
            reply_time: None,
        };

        if acl::has_privs_of_role::call(me_pid_role(), ROLE_PG_READ_ALL_STATS)
            .expect("has_privs_of_role")
        {
            // Only superusers / pg_read_all_stats members see the details.
            row.has_details = true;

            // Treat a standby such as a pg_basebackup background process (always
            // an invalid flush location) as asynchronous.
            let mut priority = snap.sync_standby_priority;
            if snap.flush == InvalidXLogRecPtr {
                priority = 0;
            }
            row.sync_priority = priority;

            row.write_lag = if snap.writeLag < 0 {
                None
            } else {
                Some(offset_to_interval(snap.writeLag))
            };
            row.flush_lag = if snap.flushLag < 0 {
                None
            } else {
                Some(offset_to_interval(snap.flushLag))
            };
            row.apply_lag = if snap.applyLag < 0 {
                None
            } else {
                Some(offset_to_interval(snap.applyLag))
            };

            // More-easily-understood version of the standby state.  In
            // quorum-based sync rep, sync/potential changes constantly, so we
            // report just "quorum" there.
            row.sync_state = if priority == 0 {
                SyncState::Async
            } else if is_sync_standby {
                if syncrep::sync_rep_config_is_priority::call() {
                    SyncState::Sync
                } else {
                    SyncState::Quorum
                }
            } else {
                SyncState::Potential
            };

            row.reply_time = if snap.replyTime == 0 {
                None
            } else {
                Some(snap.replyTime)
            };
        }

        rows.push(row);
        i += 1;
    }

    rows
}

/// `has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS)`: the current user.
fn me_pid_role() -> types_core::Oid {
    miscinit_seams::get_user_id::call()
}

/// `static Interval *offset_to_interval(TimeOffset offset)`.
pub fn offset_to_interval(offset: TimeOffset) -> Interval {
    Interval { month: 0, day: 0, time: offset }
}
