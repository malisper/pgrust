//! Port of `slotsync.c` (PostgreSQL 18.3): synchronizing logical failover
//! slots from the primary to a physical standby — the slot sync worker
//! (`ReplSlotSyncWorkerMain`), the SQL-function path (`SyncReplicationSlots`
//! behind `pg_sync_replication_slots()`), and the promotion interlock
//! (`ShutDownSlotSync`).
//!
//! Thread-model adaptations: `SlotSyncCtxStruct` (C shmem + spinlock) is a
//! process-global `Mutex`; the worker is a postmaster child thread signalled
//! through the thread-signal machinery; `syncing_slots` stays thread-local.

#![allow(non_snake_case)]

use pgsync::Mutex;

use elog::{elog, ereport};
use types_core::{Oid, TransactionId, XLogRecPtr};
use types_tuple::NameData;
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, LOG,
};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use slot::{
    GetSlotInvalidationCause, MyReplicationSlot, ReplicationSlot, ReplicationSlotAcquire,
    ReplicationSlotCleanup, ReplicationSlotCreate, ReplicationSlotCtl, ReplicationSlotDropAcquired,
    ReplicationSlotMarkDirty, ReplicationSlotPersist, ReplicationSlotRelease, ReplicationSlotSave,
    ReplicationSlotsComputeRequiredLSN, ReplicationSlotsComputeRequiredXmin, SlotIsLogical,
};
use walreceiver::client::{self, ExecStatus, PgConn};

const SRCFILE: &str = "src/backend/replication/logical/slotsync.c";

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

const InvalidXLogRecPtr: XLogRecPtr = 0;
const InvalidTransactionId: TransactionId = 0;
const InvalidOid: Oid = types_core::InvalidOid;
const InvalidPid: i32 = 0;

const DatabaseRelationId: Oid = 1262;
const AccessShareLock: i32 = 1;

// RS_INVAL_NONE (slot.h); the slot crate models invalidation as i32/u8 codes.
const RS_INVAL_NONE: i32 = 0; // slot::ondisk::RS_INVAL_NONE.0

// wait_event_names.txt Activity section: index 11 = ReplicationSlotsyncMain,
// 12 = ReplicationSlotsyncShutdown (pg_stat_activity errors on unknown ids).
const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN: u32 = PG_WAIT_ACTIVITY + 11;
const WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN: u32 = PG_WAIT_ACTIVITY + 12;

/// SlotSyncCtxStruct: pid/stopSignaled/syncing/last_start_time behind one
/// mutex (C uses a spinlock in shmem).
#[derive(Default)]
struct SlotSyncCtx {
    pid: i32,
    stop_signaled: bool,
    syncing: bool,
    last_start_time: i64,
}

pgsync::process_global! {
    static SLOT_SYNC_CTX: Mutex<SlotSyncCtx> = Mutex::new(SlotSyncCtx {
        pid: InvalidPid,
        stop_signaled: false,
        syncing: false,
        last_start_time: 0,
    });
}

fn with_ctx<R>(f: impl FnOnce(&mut SlotSyncCtx) -> R) -> R {
    let mut guard = SLOT_SYNC_CTX.lock().unwrap_or_else(|e| e.into_inner());
    f(&mut guard)
}

// The sleep time (ms) between slot-sync cycles.
const MIN_SLOTSYNC_WORKER_NAPTIME_MS: i64 = 200;
const MAX_SLOTSYNC_WORKER_NAPTIME_MS: i64 = 30_000;

const SLOTSYNC_RESTART_INTERVAL_SEC: i64 = 10;

// C static `syncing_slots` (true only if THIS process is syncing) is stored
// in the slot crate so ReplicationSlotCreate's failover-on-standby check can
// read it (slot::syncing_replication_slots).
thread_local! {
    static SLEEP_MS: std::cell::Cell<i64> = const { std::cell::Cell::new(MIN_SLOTSYNC_WORKER_NAPTIME_MS) };
    // Thread-model hazard class 1 (see the catalog): GUC backings are
    // process-shared, so C's pre-read/reload/diff in slotsync_reread_config
    // silently no-ops — the postmaster's own reload already updated the
    // shared value before this worker rereads. The worker instead records
    // the values it STARTED WITH and diffs the current shared values against
    // those (same fix pattern as the walreceiver restart-on-reload, 048).
    static STARTED_WITH: std::cell::RefCell<Option<StartedWith>> =
        const { std::cell::RefCell::new(None) };
}

struct StartedWith {
    primary_conninfo: String,
    primary_slotname: String,
    hot_standby_feedback: bool,
}

/// Information fetched from the primary about one logical slot.
struct RemoteSlot {
    name: String,
    plugin: String,
    database: String,
    two_phase: bool,
    failover: bool,
    restart_lsn: XLogRecPtr,
    confirmed_lsn: XLogRecPtr,
    two_phase_at: XLogRecPtr,
    catalog_xmin: TransactionId,
    /// RS_INVAL_NONE if valid, or the invalidation cause code.
    invalidated: i32,
}

// ---------------------------------------------------------------------------
// Seam: LogicalSlotAdvanceAndCheckSnapState lives in slotfuncs (its C home is
// logical.c, but the rust port hosts it beside pg_replication_slot_advance).
// slotfuncs depends on this crate for SyncReplicationSlots, so the advance
// entry point is injected. Returns (retlsn, found_consistent_snapshot).
// ---------------------------------------------------------------------------
seam_core::seam!(
    pub fn logical_slot_advance_and_check_snap_state(
        moveto: XLogRecPtr
    ) -> PgResult<(XLogRecPtr, bool)>
);

// GetStandbyFlushRecPtr(NULL) (xlog.c:6653), via the recovery seams — what a
// standby has both replayed and flushed on the replay timeline.
fn standby_flush_rec_ptr() -> XLogRecPtr {
    let (receive_ptr, _latest_chunk_start, receive_tli) =
        if walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::is_installed() {
            walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::call()
        } else {
            (0, 0, 0)
        };
    let (replay_ptr, replay_tli) = xlogrecovery_seams::get_xlog_replay_rec_ptr::call();
    let mut result = replay_ptr;
    if receive_tli == replay_tli && receive_ptr > replay_ptr {
        result = receive_ptr;
    }
    result
}

fn name_string(nd: &NameData) -> String {
    String::from_utf8_lossy(nd.name_str()).into_owned()
}

fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

// ---------------------------------------------------------------------------
// update_local_synced_slot (slotsync.c:166).
// ---------------------------------------------------------------------------
fn update_local_synced_slot(
    remote_slot: &RemoteSlot,
    remote_dbid: Oid,
    mut found_consistent_snapshot: Option<&mut bool>,
    mut remote_slot_precedes: Option<&mut bool>,
) -> PgResult<bool> {
    let slot = MyReplicationSlot().expect("update_local_synced_slot without acquired slot");
    let mut updated_xmin_or_lsn = false;
    let mut updated_config = false;

    debug_assert!(slot.data.get().invalidated.0 == RS_INVAL_NONE);

    if let Some(f) = found_consistent_snapshot.as_deref_mut() {
        *f = false;
    }
    if let Some(f) = remote_slot_precedes.as_deref_mut() {
        *f = false;
    }

    let d = slot.data.get();

    // Don't overwrite if we already have a newer catalog_xmin and restart_lsn.
    if remote_slot.restart_lsn < d.restart_lsn
        || transaction_id_precedes(remote_slot.catalog_xmin, d.catalog_xmin)
    {
        let level = if d.persistency == slot::RS_TEMPORARY {
            LOG
        } else {
            DEBUG1
        };
        let _ = ereport(level)
            .errmsg(format!(
                "could not synchronize replication slot \"{}\"",
                remote_slot.name
            ))
            .errdetail(format!(
                "Synchronization could lead to data loss, because the remote slot needs WAL at LSN {} and catalog xmin {}, but the standby has LSN {} and catalog xmin {}.",
                lsn_fmt(remote_slot.restart_lsn),
                remote_slot.catalog_xmin,
                lsn_fmt(d.restart_lsn),
                d.catalog_xmin
            ))
            .finish(loc("update_local_synced_slot"));

        if let Some(f) = remote_slot_precedes.as_deref_mut() {
            *f = true;
        }
        return Ok(false);
    }

    // Attempt to sync LSNs and xmins only if remote slot is ahead.
    if remote_slot.confirmed_lsn > d.confirmed_flush
        || remote_slot.restart_lsn > d.restart_lsn
        || transaction_id_follows(remote_slot.catalog_xmin, d.catalog_xmin)
    {
        if snapbuild::snap_build_snapshot_exists(remote_slot.restart_lsn)? {
            slot.with_mutex(|| {
                let mut d = slot.data.get();
                d.restart_lsn = remote_slot.restart_lsn;
                d.confirmed_flush = remote_slot.confirmed_lsn;
                d.catalog_xmin = remote_slot.catalog_xmin;
                slot.data.set(d);
            });
            if let Some(f) = found_consistent_snapshot.as_deref_mut() {
                *f = true;
            }
        } else {
            let (_retlsn, consistent) =
                logical_slot_advance_and_check_snap_state::call(remote_slot.confirmed_lsn)?;
            if let Some(f) = found_consistent_snapshot.as_deref_mut() {
                *f = consistent;
            }

            // Sanity check.
            let confirmed = slot.data.get().confirmed_flush;
            if confirmed != remote_slot.confirmed_lsn {
                return ereport(ERROR)
                    .errmsg(format!(
                        "synchronized confirmed_flush for slot \"{}\" differs from remote slot",
                        remote_slot.name
                    ))
                    .errdetail(format!(
                        "Remote slot has LSN {} but local slot has LSN {}.",
                        lsn_fmt(remote_slot.confirmed_lsn),
                        lsn_fmt(confirmed)
                    ))
                    .finish(loc("update_local_synced_slot"))
                    .map(|()| false);
            }
        }
        updated_xmin_or_lsn = true;
    }

    let d = slot.data.get();
    if remote_dbid != d.database
        || remote_slot.two_phase != d.two_phase
        || remote_slot.failover != d.failover
        || remote_slot.plugin.as_bytes() != d.plugin.name_str()
        || remote_slot.two_phase_at != d.two_phase_at
    {
        let mut plugin_name = NameData::default();
        plugin_name.namestrcpy(&remote_slot.plugin);
        slot.with_mutex(|| {
            let mut d = slot.data.get();
            d.plugin = plugin_name;
            d.database = remote_dbid;
            d.two_phase = remote_slot.two_phase;
            d.two_phase_at = remote_slot.two_phase_at;
            d.failover = remote_slot.failover;
            slot.data.set(d);
        });
        updated_config = true;

        debug_assert!(slot.data.get().two_phase_at <= slot.data.get().confirmed_flush);
    }

    // Write the changed xmin to disk *before* the in-memory value advances.
    if updated_config || updated_xmin_or_lsn {
        ReplicationSlotMarkDirty();
        ReplicationSlotSave()?;
    }

    if updated_xmin_or_lsn {
        slot.with_mutex(|| {
            slot.effective_catalog_xmin.set(remote_slot.catalog_xmin);
        });
        ReplicationSlotsComputeRequiredXmin(false)?;
        ReplicationSlotsComputeRequiredLSN()?;
    }

    Ok(updated_config || updated_xmin_or_lsn)
}

fn transaction_id_precedes(a: TransactionId, b: TransactionId) -> bool {
    if a == b {
        return false;
    }
    // TransactionIdPrecedes: modulo-2^31 comparison for normal xids.
    if !(a >= 3) || !(b >= 3) {
        return a < b;
    }
    ((a.wrapping_sub(b)) as i32) < 0
}

fn transaction_id_follows(a: TransactionId, b: TransactionId) -> bool {
    if a == b {
        return false;
    }
    if !(a >= 3) || !(b >= 3) {
        return a > b;
    }
    ((a.wrapping_sub(b)) as i32) > 0
}

// ---------------------------------------------------------------------------
// get_local_synced_slots + local_sync_slot_required + drop_local_obsolete_slots.
// ---------------------------------------------------------------------------

fn get_local_synced_slots() -> Vec<&'static ReplicationSlot> {
    let mut local_slots = Vec::new();
    // C holds ReplicationSlotControlLock shared; the slot array is a static
    // and per-slot state is read under each slot's mutex below.
    for s in ReplicationSlotCtl() {
        if s.in_use.get() && s.data.get().synced != 0 {
            debug_assert!(SlotIsLogical(s));
            local_slots.push(s);
        }
    }
    local_slots
}

fn local_sync_slot_required(local_slot: &ReplicationSlot, remote_slots: &[RemoteSlot]) -> bool {
    let local_name = name_string(&local_slot.data.get().name);
    let mut remote_exists = false;
    let mut locally_invalidated = false;

    for remote_slot in remote_slots {
        if remote_slot.name == local_name {
            remote_exists = true;
            locally_invalidated = local_slot.with_mutex(|| {
                remote_slot.invalidated == RS_INVAL_NONE
                    && local_slot.data.get().invalidated.0 != RS_INVAL_NONE
            });
            break;
        }
    }

    remote_exists && !locally_invalidated
}

fn drop_local_obsolete_slots(remote_slot_list: &[RemoteSlot]) -> PgResult<()> {
    for local_slot in get_local_synced_slots() {
        if local_sync_slot_required(local_slot, remote_slot_list) {
            continue;
        }

        let dboid = local_slot.data.get().database;

        // Shared lock prevents a conflict with ReplicationSlotsDropDBSlots
        // during a concurrent drop-database.
        lmgr::LockSharedObject(DatabaseRelationId, dboid, 0, AccessShareLock)?;

        let synced_slot =
            local_slot.with_mutex(|| local_slot.in_use.get() && local_slot.data.get().synced != 0);

        if synced_slot {
            let name = name_string(&local_slot.data.get().name);
            ReplicationSlotAcquire(&name, true, false)?;
            ReplicationSlotDropAcquired()?;
        }

        lmgr::UnlockSharedObject(DatabaseRelationId, dboid, 0, AccessShareLock)?;

        let _ = elog(
            LOG,
            format!(
                "dropped replication slot \"{}\" of database with OID {}",
                name_string(&local_slot.data.get().name),
                dboid
            ),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// reserve_wal_for_local_slot (slotsync.c:487).
// ---------------------------------------------------------------------------
fn reserve_wal_for_local_slot(restart_lsn: XLogRecPtr) -> PgResult<()> {
    let slot = MyReplicationSlot().expect("reserve_wal_for_local_slot without slot");
    debug_assert!(slot.data.get().restart_lsn == InvalidXLogRecPtr);

    // C acquires ReplicationSlotAllocationLock exclusively to fence against
    // the checkpointer's minimum-LSN calculation.
    slot::with_allocation_lock_exclusive(|| -> PgResult<()> {
        let mut min_safe_lsn = transam_xlog::GetRedoRecPtr();
        let ctl = transam_xlog::ctl::XLogCtl();
        let slot_min_lsn = ctl
            .info_lck
            .with(|| ctl.replicationSlotMinLSN.load(std::sync::atomic::Ordering::Relaxed));

        if slot_min_lsn != InvalidXLogRecPtr && min_safe_lsn > slot_min_lsn {
            min_safe_lsn = slot_min_lsn;
        }

        slot.with_mutex(|| {
            let mut d = slot.data.get();
            d.restart_lsn = restart_lsn.max(min_safe_lsn);
            slot.data.set(d);
        });

        ReplicationSlotsComputeRequiredLSN()?;

        let segno = slot.data.get().restart_lsn / transam_xlog::wal_segment_size() as u64;
        let last_removed = ctl
            .info_lck
            .with(|| ctl.lastRemovedSegNo.load(std::sync::atomic::Ordering::Relaxed));
        if last_removed >= segno {
            elog(
                ERROR,
                format!(
                    "WAL required by replication slot {} has been removed concurrently",
                    name_string(&slot.data.get().name)
                ),
            )?;
        }
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// update_and_persist_local_synced_slot (slotsync.c:556).
// ---------------------------------------------------------------------------
fn update_and_persist_local_synced_slot(
    remote_slot: &RemoteSlot,
    remote_dbid: Oid,
) -> PgResult<bool> {
    let slot = MyReplicationSlot().expect("no slot acquired");
    let mut found_consistent_snapshot = false;
    let mut remote_slot_precedes = false;

    update_local_synced_slot(
        remote_slot,
        remote_dbid,
        Some(&mut found_consistent_snapshot),
        Some(&mut remote_slot_precedes),
    )?;

    // The remote slot didn't catch up to the locally reserved position: keep
    // the temporary slot and retry next cycle.
    if remote_slot_precedes {
        return Ok(false);
    }

    // Don't persist if decoding from restart_lsn cannot reach consistency.
    if !found_consistent_snapshot {
        let _ = ereport(LOG)
            .errmsg(format!(
                "could not synchronize replication slot \"{}\"",
                remote_slot.name
            ))
            .errdetail(format!(
                "Synchronization could lead to data loss, because the standby could not build a consistent snapshot to decode WALs at LSN {}.",
                lsn_fmt(slot.data.get().restart_lsn)
            ))
            .finish(loc("update_and_persist_local_synced_slot"));
        return Ok(false);
    }

    ReplicationSlotPersist()?;

    let _ = elog(
        LOG,
        format!(
            "newly created replication slot \"{}\" is sync-ready now",
            remote_slot.name
        ),
    );

    Ok(true)
}

// ---------------------------------------------------------------------------
// synchronize_one_slot (slotsync.c:635).
// ---------------------------------------------------------------------------
fn synchronize_one_slot(remote_slot: &RemoteSlot, remote_dbid: Oid) -> PgResult<bool> {
    let mut slot_updated = false;

    // Concerned WAL must be received and flushed before syncing to the target.
    let latest_flush_ptr = standby_flush_rec_ptr();
    if remote_slot.confirmed_lsn > latest_flush_ptr {
        // Only reachable when 'synchronized_standby_slots' on the primary is
        // not configured correctly.
        let level = if am_slotsync_worker() { LOG } else { ERROR };
        let r = ereport(level)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "skipping slot synchronization because the received slot sync LSN {} for slot \"{}\" is ahead of the standby position {}",
                lsn_fmt(remote_slot.confirmed_lsn),
                remote_slot.name,
                lsn_fmt(latest_flush_ptr)
            ))
            .finish(loc("synchronize_one_slot"));
        if level == ERROR {
            r?;
        }
        return Ok(false);
    }

    if let Some(slot) = slot::SearchNamedReplicationSlot(&remote_slot.name, true)? {
        let synced = slot.with_mutex(|| slot.data.get().synced != 0);

        // User-created slot with the same name: hard error.
        if !synced {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "exiting from slot synchronization because same name slot \"{}\" already exists on the standby",
                    remote_slot.name
                ))
                .finish(loc("synchronize_one_slot"))
                .map(|()| false);
        }

        // Acquire before checking invalidation (race with
        // InvalidatePossiblyObsoleteSlot).
        ReplicationSlotAcquire(&remote_slot.name, true, false)?;

        let slot = MyReplicationSlot().expect("acquired");

        // Copy the invalidation cause from remote only if not locally set.
        if slot.data.get().invalidated.0 == RS_INVAL_NONE
            && remote_slot.invalidated != RS_INVAL_NONE
        {
            slot.with_mutex(|| {
                let mut d = slot.data.get();
                d.invalidated = slot::ReplicationSlotInvalidationCause(remote_slot.invalidated);
                slot.data.set(d);
            });
            ReplicationSlotMarkDirty();
            ReplicationSlotSave()?;
            slot_updated = true;
        }

        // Skip the sync of an invalidated slot.
        if slot.data.get().invalidated.0 != RS_INVAL_NONE {
            ReplicationSlotRelease()?;
            return Ok(slot_updated);
        }

        if slot.data.get().persistency == slot::RS_TEMPORARY {
            // Not yet sync-ready: attempt to make it so.
            slot_updated = update_and_persist_local_synced_slot(remote_slot, remote_dbid)?;
        } else {
            // Sanity check.
            let confirmed = slot.data.get().confirmed_flush;
            if remote_slot.confirmed_lsn < confirmed {
                return ereport(ERROR)
                    .errmsg(format!(
                        "cannot synchronize local slot \"{}\"",
                        remote_slot.name
                    ))
                    .errdetail(format!(
                        "Local slot's start streaming location LSN({}) is ahead of remote slot's LSN({}).",
                        lsn_fmt(confirmed),
                        lsn_fmt(remote_slot.confirmed_lsn)
                    ))
                    .finish(loc("synchronize_one_slot"))
                    .map(|()| false);
            }
            slot_updated = update_local_synced_slot(remote_slot, remote_dbid, None, None)?;
        }
    } else {
        // Otherwise create the slot first.
        if remote_slot.invalidated != RS_INVAL_NONE {
            return Ok(false);
        }

        // Temporary (not ephemeral) so the slot survives release across
        // sync cycles while the remote catches up.
        ReplicationSlotCreate(
            &remote_slot.name,
            true,
            slot::RS_TEMPORARY,
            remote_slot.two_phase,
            remote_slot.failover,
            true,
        )?;

        let slot = MyReplicationSlot().expect("created");

        let mut plugin_name = NameData::default();
        plugin_name.namestrcpy(&remote_slot.plugin);
        slot.with_mutex(|| {
            let mut d = slot.data.get();
            d.database = remote_dbid;
            d.plugin = plugin_name;
            slot.data.set(d);
        });

        reserve_wal_for_local_slot(remote_slot.restart_lsn)?;

        let xmin_horizon = slot::with_control_lock_exclusive(|| -> PgResult<TransactionId> {
            procarray::with_procarray_lock_exclusive(|| -> PgResult<TransactionId> {
                let xmin_horizon = procarray::GetOldestSafeDecodingTransactionId(true)?;
                slot.with_mutex(|| {
                    slot.effective_catalog_xmin.set(xmin_horizon);
                    let mut d = slot.data.get();
                    d.catalog_xmin = xmin_horizon;
                    slot.data.set(d);
                });
                ReplicationSlotsComputeRequiredXmin(true)?;
                Ok(xmin_horizon)
            })
        })?;
        let _ = xmin_horizon;

        update_and_persist_local_synced_slot(remote_slot, remote_dbid)?;

        slot_updated = true;
    }

    ReplicationSlotRelease()?;

    Ok(slot_updated)
}

// ---------------------------------------------------------------------------
// synchronize_slots (slotsync.c:807).
// ---------------------------------------------------------------------------
fn synchronize_slots(conn: &mut PgConn) -> PgResult<bool> {
    const QUERY: &str = "SELECT slot_name, plugin, confirmed_flush_lsn, \
         restart_lsn, catalog_xmin, two_phase, two_phase_at, failover, \
         database, invalidation_reason \
         FROM pg_catalog.pg_replication_slots \
         WHERE failover and NOT temporary";

    let mut started_tx = false;
    if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
        started_tx = true;
    }

    let res = conn.exec(QUERY)?;
    if res.status != ExecStatus::TuplesOk {
        return ereport(ERROR)
            .errmsg(format!(
                "could not fetch failover logical slots info from the primary server: {}",
                res.err
            ))
            .finish(loc("synchronize_slots"))
            .map(|()| false);
    }

    let mut remote_slot_list: Vec<RemoteSlot> = Vec::new();
    for row in &res.rows {
        let text = |i: usize| -> Option<String> {
            row.get(i)
                .and_then(|c| c.as_ref())
                .map(|v| String::from_utf8_lossy(v).into_owned())
        };
        let name = text(0).expect("slot_name is never null");
        let plugin = text(1).expect("plugin is never null");
        // LSN and xmin may be null if the slot is invalidated on the primary.
        let confirmed_lsn = text(2).map(|s| parse_lsn(&s)).unwrap_or(InvalidXLogRecPtr);
        let restart_lsn = text(3).map(|s| parse_lsn(&s)).unwrap_or(InvalidXLogRecPtr);
        let catalog_xmin = text(4)
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(InvalidTransactionId);
        let two_phase = text(5).as_deref() == Some("t");
        let two_phase_at = text(6).map(|s| parse_lsn(&s)).unwrap_or(InvalidXLogRecPtr);
        let failover = text(7).as_deref() == Some("t");
        let database = text(8).expect("database is never null");
        let invalidated = match text(9) {
            None => RS_INVAL_NONE,
            Some(cause) => GetSlotInvalidationCause(&cause).0,
        };

        // An RS_EPHEMERAL remote slot has invalid LSNs/xmin while still
        // valid: skip it this cycle.
        if (restart_lsn == InvalidXLogRecPtr
            || confirmed_lsn == InvalidXLogRecPtr
            || catalog_xmin == InvalidTransactionId)
            && invalidated == RS_INVAL_NONE
        {
            continue;
        }

        remote_slot_list.push(RemoteSlot {
            name,
            plugin,
            database,
            two_phase,
            failover,
            restart_lsn,
            confirmed_lsn,
            two_phase_at,
            catalog_xmin,
            invalidated,
        });
    }

    // Drop local slots that no longer need to be synced.
    drop_local_obsolete_slots(&remote_slot_list)?;

    // Now sync the slots locally.
    let mut some_slot_updated = false;
    let cx = mcx::MemoryContext::new("synchronize_slots");
    for remote_slot in &remote_slot_list {
        let remote_dbid =
            dbcommands_seams::get_database_oid::call(cx.mcx(), &remote_slot.database, false)?;

        lmgr::LockSharedObject(DatabaseRelationId, remote_dbid, 0, AccessShareLock)?;
        some_slot_updated |= synchronize_one_slot(remote_slot, remote_dbid)?;
        lmgr::UnlockSharedObject(DatabaseRelationId, remote_dbid, 0, AccessShareLock)?;
    }

    if started_tx {
        xact::CommitTransactionCommand()?;
    }

    Ok(some_slot_updated)
}

fn parse_lsn(s: &str) -> XLogRecPtr {
    let mut it = s.trim().split('/');
    let hi = it
        .next()
        .and_then(|p| u64::from_str_radix(p, 16).ok())
        .unwrap_or(0);
    let lo = it
        .next()
        .and_then(|p| u64::from_str_radix(p, 16).ok())
        .unwrap_or(0);
    (hi << 32) | lo
}

// ---------------------------------------------------------------------------
// validate_remote_info (slotsync.c:942).
// ---------------------------------------------------------------------------
fn validate_remote_info(conn: &mut PgConn) -> PgResult<()> {
    let primary_slot_name = guc_tables::vars::PrimarySlotName.read().unwrap_or_default();
    let cmd = format!(
        "SELECT pg_is_in_recovery(), count(*) = 1 FROM pg_catalog.pg_replication_slots WHERE slot_type='physical' AND slot_name={}",
        quote_literal(&primary_slot_name)
    );

    let mut started_tx = false;
    if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
        started_tx = true;
    }

    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk {
        return ereport(ERROR)
            .errmsg(format!(
                "could not fetch primary slot name \"{primary_slot_name}\" info from the primary server: {}",
                res.err
            ))
            .errhint("Check if \"primary_slot_name\" is configured correctly.")
            .finish(loc("validate_remote_info"));
    }
    let row = res.rows.first().ok_or_else(|| {
        ereport(ERROR)
            .errmsg(
                "failed to fetch tuple for the primary server slot specified by \"primary_slot_name\""
                    .to_string(),
            )
            .into_error()
    })?;
    let col = |i: usize| -> &str {
        row.get(i)
            .and_then(|c| c.as_deref())
            .map(|v| std::str::from_utf8(v).unwrap_or(""))
            .unwrap_or("")
    };
    let remote_in_recovery = col(0) == "t";
    let primary_slot_valid = col(1) == "t";

    // Slot sync on a cascading standby is not supported.
    if remote_in_recovery {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot synchronize replication slots from a standby server")
            .finish(loc("validate_remote_info"));
    }

    if !primary_slot_valid {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "replication slot \"{primary_slot_name}\" specified by \"primary_slot_name\" does not exist on primary server"
            ))
            .finish(loc("validate_remote_info"));
    }

    if started_tx {
        xact::CommitTransactionCommand()?;
    }
    Ok(())
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

// ---------------------------------------------------------------------------
// CheckAndGetDbnameFromConninfo + ValidateSlotSyncParams.
// ---------------------------------------------------------------------------

pub fn CheckAndGetDbnameFromConninfo() -> PgResult<String> {
    let conninfo = guc_tables::vars::PrimaryConnInfo.read().unwrap_or_default();
    let opts = client::check_conninfo(&conninfo)?;
    let dbname = opts
        .iter()
        .find(|(k, _)| k == "dbname")
        .map(|(_, v)| v.clone());
    match dbname {
        Some(d) if !d.is_empty() => Ok(d),
        _ => ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(
                "replication slot synchronization requires \"dbname\" to be specified in \"primary_conninfo\""
                    .to_string(),
            )
            .finish(loc("CheckAndGetDbnameFromConninfo"))
            .map(|()| unreachable!()),
    }
}

/// ValidateSlotSyncParams (slotsync.c:1061). elevel_error=true renders
/// C's ereport(ERROR) caller; false renders ereport(LOG) + `false` return.
pub fn ValidateSlotSyncParams(elevel_error: bool) -> PgResult<bool> {
    let fail = |msg: String| -> PgResult<bool> {
        if elevel_error {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(msg)
                .finish(loc("ValidateSlotSyncParams"))
                .map(|()| false)
        } else {
            let _ = ereport(LOG)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(msg)
                .finish(loc("ValidateSlotSyncParams"));
            Ok(false)
        }
    };

    if transam_xlog::wal_level() < transam_xlog::WAL_LEVEL_LOGICAL {
        return fail(
            "replication slot synchronization requires \"wal_level\" >= \"logical\"".to_string(),
        );
    }

    let primary_slot_name = guc_tables::vars::PrimarySlotName.read().unwrap_or_default();
    if primary_slot_name.is_empty() {
        return fail(
            "replication slot synchronization requires \"primary_slot_name\" to be set"
                .to_string(),
        );
    }

    if !guc_tables::vars::hot_standby_feedback.read() {
        return fail(
            "replication slot synchronization requires \"hot_standby_feedback\" to be enabled"
                .to_string(),
        );
    }

    let conninfo = guc_tables::vars::PrimaryConnInfo.read().unwrap_or_default();
    if conninfo.is_empty() {
        return fail(
            "replication slot synchronization requires \"primary_conninfo\" to be set".to_string(),
        );
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Worker plumbing.
// ---------------------------------------------------------------------------

fn am_slotsync_worker() -> bool {
    miscinit::GetMyBackendType() == types_core::BackendType::SlotsyncWorker
}

/// slotsync_reread_config: exit (for postmaster restart) if any slot sync GUC
/// changed.
fn slotsync_reread_config() -> PgResult<()> {
    interrupt::SetConfigReloadPending(false);
    guc_file::ProcessConfigFile(types_guc::PGC_SIGHUP)?;

    // Diff against started-with values, not pre-reload reads: the GUC
    // backings are process-shared, so the postmaster's reload already
    // changed them (thread-model hazard class 1).
    let (old_primary_conninfo, old_primary_slotname, old_hot_standby_feedback) =
        STARTED_WITH.with(|sw| {
            let sw = sw.borrow();
            let sw = sw.as_ref().expect("slot sync worker recorded its GUCs");
            (
                sw.primary_conninfo.clone(),
                sw.primary_slotname.clone(),
                sw.hot_standby_feedback,
            )
        });

    let conninfo_changed =
        old_primary_conninfo != guc_tables::vars::PrimaryConnInfo.read().unwrap_or_default();
    let primary_slotname_changed =
        old_primary_slotname != guc_tables::vars::PrimarySlotName.read().unwrap_or_default();

    if !guc_tables::vars::sync_replication_slots.read() {
        let _ = ereport(LOG)
            .errmsg(
                "replication slot synchronization worker will shut down because \"sync_replication_slots\" is disabled"
                    .to_string(),
            )
            .finish(loc("slotsync_reread_config"));
        ipc::proc_exit(0, init_small::globals::MyProcPid());
    }

    if conninfo_changed
        || primary_slotname_changed
        || old_hot_standby_feedback != guc_tables::vars::hot_standby_feedback.read()
    {
        let _ = ereport(LOG)
            .errmsg(
                "replication slot synchronization worker will restart because of a parameter change"
                    .to_string(),
            )
            .finish(loc("slotsync_reread_config"));

        // Reset last-start so the postmaster restarts us immediately.
        with_ctx(|ctx| ctx.last_start_time = 0);

        ipc::proc_exit(0, init_small::globals::MyProcPid());
    }

    Ok(())
}

fn ProcessSlotSyncInterrupts() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()?;

    if with_ctx(|ctx| ctx.stop_signaled) {
        let _ = ereport(LOG)
            .errmsg(
                "replication slot synchronization worker is shutting down because promotion is triggered"
                    .to_string(),
            )
            .finish(loc("ProcessSlotSyncInterrupts"));
        ipc::proc_exit(0, init_small::globals::MyProcPid());
    }

    if interrupt::ConfigReloadPending() {
        slotsync_reread_config()?;
    }
    Ok(())
}

/// slotsync_worker_onexit: slots cleanup exactly as C, then clear pid/syncing.
fn slotsync_worker_onexit_cb(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    slotsync_worker_onexit();
    Ok(())
}

fn slotsync_worker_onexit() {
    if MyReplicationSlot().is_some() {
        let _ = ReplicationSlotRelease();
    }
    let _ = ReplicationSlotCleanup(false);

    with_ctx(|ctx| {
        ctx.pid = InvalidPid;
        if slot::syncing_replication_slots() {
            ctx.syncing = false;
            slot::set_syncing_replication_slots(false);
        }
    });
}

fn wait_for_slot_activity(some_slot_updated: bool) -> PgResult<()> {
    let sleep_ms = SLEEP_MS.with(|c| {
        let v = if !some_slot_updated {
            (c.get() * 2).min(MAX_SLOTSYNC_WORKER_NAPTIME_MS)
        } else {
            MIN_SLOTSYNC_WORKER_NAPTIME_MS
        };
        c.set(v);
        v
    });

    let latch = init_small::globals::MyLatch();
    let rc = latch::WaitLatch(
        latch,
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        sleep_ms,
        WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN,
    )?;
    if rc & WL_LATCH_SET != 0 {
        if let Some(l) = latch {
            latch::ResetLatch(l);
        }
    }
    Ok(())
}

/// check_and_set_sync_info: error on promotion/concurrent sync, else
/// advertise the sync.
fn check_and_set_sync_info(worker_pid: i32) -> PgResult<()> {
    enum Bad {
        Stop,
        Concurrent,
    }
    let bad = with_ctx(|ctx| {
        debug_assert!(worker_pid == InvalidPid || ctx.pid == InvalidPid);
        if ctx.stop_signaled {
            return Some(Bad::Stop);
        }
        if ctx.syncing {
            return Some(Bad::Concurrent);
        }
        ctx.syncing = true;
        ctx.pid = worker_pid;
        None
    });
    match bad {
        Some(Bad::Stop) => ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot synchronize replication slots when standby promotion is ongoing")
            .finish(loc("check_and_set_sync_info")),
        Some(Bad::Concurrent) => ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot synchronize replication slots concurrently")
            .finish(loc("check_and_set_sync_info")),
        None => {
            slot::set_syncing_replication_slots(true);
            Ok(())
        }
    }
}

fn reset_syncing_flag() {
    with_ctx(|ctx| ctx.syncing = false);
    slot::set_syncing_replication_slots(false);
}

// ---------------------------------------------------------------------------
// ReplSlotSyncWorkerMain.
// ---------------------------------------------------------------------------

pub fn ReplSlotSyncWorkerMain(startup_data: &types_startup::StartupData) -> ! {
    debug_assert!(matches!(startup_data, types_startup::StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::SlotsyncWorker);
    ps_status_seams::set_ps_display::call("");

    debug_assert!(miscinit::IsInitProcessingMode());

    match repl_slot_sync_worker_inner() {
        Ok(()) => unreachable!("slot sync worker loops forever"),
        Err(e) => {
            // The C worker's sigsetjmp arm: report and exit 0 so the
            // postmaster treats it as a clean stop and can restart it after
            // SLOTSYNC_RESTART_INTERVAL_SEC.
            elog::emit_error_report_for(&e);
            ipc::proc_exit(0, init_small::globals::MyProcPid())
        }
    }
}

fn repl_slot_sync_worker_inner() -> PgResult<()> {
    lmgr_proc::InitProcess(types_core::BackendType::SlotsyncWorker)?;
    postinit::BaseInit()?;

    // Record the GUC values this worker runs with NOW — the thread-model
    // rendering of C's fork-time process image. slotsync_reread_config diffs
    // future reloads against these (hazard class 1). Capturing any later
    // loses reloads that land mid-startup: 040's GUC test flips
    // hot_standby_feedback within milliseconds of the "slot sync worker
    // started" line (logged before InitPostgres), so a post-InitPostgres
    // capture reads the post-flip value and the diff stays silent forever —
    // the 040 subtest-20 wait_for_log timeout on main (2026-07-18 triage,
    // notes/2pc-decode-lane.md). The capture precedes the SIGHUP handler
    // registration below, so any reload processed before this point simply
    // becomes this worker's baseline, exactly like C's fork inheritance.
    STARTED_WITH.with(|sw| {
        *sw.borrow_mut() = Some(StartedWith {
            primary_conninfo: guc_tables::vars::PrimaryConnInfo.read().unwrap_or_default(),
            primary_slotname: guc_tables::vars::PrimarySlotName.read().unwrap_or_default(),
            hot_standby_feedback: guc_tables::vars::hot_standby_feedback.read(),
        });
    });

    // Signal handling (C: SIGHUP config reload, SIGINT cancel, SIGTERM die,
    // SIGUSR1 procsignal).
    {
        // procsignal::signums, not libc::SIG*: the wasi libc crate exposes
        // no SIG* names (thread-signal emulation numbering, signums law).
        use procsignal::ThreadSignalHandler::{Fallible, Simple};
        procsignal::pqsignal_thread(
            procsignal::signums::SIGHUP,
            Simple(interrupt::SignalHandlerForConfigReload),
        );
        procsignal::pqsignal_thread(procsignal::signums::SIGTERM, Fallible(postgres_tcop::die));
    }

    check_and_set_sync_info(init_small::globals::MyProcPid())?;

    let _ = ereport(LOG)
        .errmsg("slot sync worker started".to_string())
        .finish(loc("ReplSlotSyncWorkerMain"));

    // Register as soon as SlotSyncCtx->pid is initialized.
    ipc::before_shmem_exit(slotsync_worker_onexit_cb, datum::Datum::from_usize(0))?;

    // Establishes the timeout module for this thread; InitPostgres registers
    // timeouts (slotsync.c:1434).
    timeout::InitializeTimeouts();

    let dbname = CheckAndGetDbnameFromConninfo()?;

    // Database connection: walrcv_exec-equivalent queries need syscache.
    let top = mcx::MemoryContext::new("SlotSyncWorkerInit");
    postinit::InitPostgres(top.mcx(), Some(&dbname), InvalidOid, None, InvalidOid, 0, None)?;
    miscinit::SetProcessingMode(types_core::ProcessingMode::NormalProcessing);

    let cluster_name = guc_tables::vars::cluster_name.read().unwrap_or_default();
    let app_name = if !cluster_name.is_empty() {
        format!("{cluster_name}_slotsync worker")
    } else {
        "slotsync worker".to_string()
    };

    // Connect with the baseline conninfo (captured at entry); a conninfo
    // that changed since then triggers the reread-restart path anyway.
    let conninfo = STARTED_WITH.with(|sw| {
        sw.borrow()
            .as_ref()
            .expect("slot sync worker recorded its GUCs at entry")
            .primary_conninfo
            .clone()
    });

    let mut conn = match client::connect_extended(&conninfo, false, false, false, &app_name)? {
        Ok(conn) => conn,
        Err(err) => {
            return ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "synchronization worker \"{app_name}\" could not connect to the primary server: {err}"
                ))
                .finish(loc("ReplSlotSyncWorkerMain"));
        }
    };

    // Not a cascading standby + primary_slot_name exists on the primary.
    validate_remote_info(&mut conn)?;

    // Main loop.
    loop {
        ProcessSlotSyncInterrupts()?;

        let some_slot_updated = synchronize_slots(&mut conn)?;

        wait_for_slot_activity(some_slot_updated)?;
    }
}

// ---------------------------------------------------------------------------
// update_synced_slots_inactive_since + ShutDownSlotSync (startup process).
// ---------------------------------------------------------------------------

fn update_synced_slots_inactive_since() {
    // Only relevant while promoting a standby.
    if !xlogrecovery_seams::standby_mode::call() {
        return;
    }

    let mut now: i64 = 0;
    for s in ReplicationSlotCtl() {
        if s.in_use.get() && s.data.get().synced != 0 {
            debug_assert!(SlotIsLogical(s));
            debug_assert!(s.active_pid.get() == 0);
            if now == 0 {
                now = timestamp_seams::get_current_timestamp::call();
            }
            slot::ReplicationSlotSetInactiveSince(s, now, true);
        }
    }
}

/// ShutDownSlotSync (slotsync.c:1586): signal the worker and wait until no
/// process is syncing. Called by the startup process during promotion.
pub fn ShutDownSlotSync() -> PgResult<()> {
    let (running, worker_pid) = with_ctx(|ctx| {
        ctx.stop_signaled = true;
        (ctx.syncing, ctx.pid)
    });

    if !running {
        update_synced_slots_inactive_since();
        return Ok(());
    }

    if worker_pid != InvalidPid {
        procsignal::SendThreadSignal(worker_pid, procsignal::signums::SIGUSR1);
    }

    // Wait for slot sync to end.
    loop {
        let latch = init_small::globals::MyLatch();
        let rc = latch::WaitLatch(
            latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = latch {
                latch::ResetLatch(l);
            }
            postgres_seams::check_for_interrupts::call()?;
        }

        if !with_ctx(|ctx| ctx.syncing) {
            break;
        }
    }

    update_synced_slots_inactive_since();
    Ok(())
}

/// SlotSyncWorkerCanRestart: at most one start per
/// SLOTSYNC_RESTART_INTERVAL_SEC.
pub fn SlotSyncWorkerCanRestart() -> bool {
    // SAFETY: time(2) with a NULL argument has no failure modes we care for.
    let curtime = unsafe { libc::time(std::ptr::null_mut()) } as i64;
    with_ctx(|ctx| {
        if curtime.wrapping_sub(ctx.last_start_time) < SLOTSYNC_RESTART_INTERVAL_SEC {
            return false;
        }
        ctx.last_start_time = curtime;
        true
    })
}

/// Is the current process syncing replication slots (worker or SQL function)?
pub fn IsSyncingReplicationSlots() -> bool {
    slot::syncing_replication_slots()
}

// ---------------------------------------------------------------------------
// SyncReplicationSlots — the SQL-function path.
// ---------------------------------------------------------------------------

/// slotsync_failure_callback + SyncReplicationSlots: sync once over the given
/// connection with C's ensure-error-cleanup shape.
pub fn SyncReplicationSlots(conn: &mut PgConn) -> PgResult<()> {
    let body = |conn: &mut PgConn| -> PgResult<()> {
        check_and_set_sync_info(InvalidPid)?;
        validate_remote_info(conn)?;
        synchronize_slots(conn)?;

        // Cleanup the synced temporary slots.
        ReplicationSlotCleanup(true)?;

        reset_syncing_flag();
        Ok(())
    };

    match body(conn) {
        Ok(()) => Ok(()),
        Err(e) => {
            // slotsync_failure_callback.
            if MyReplicationSlot().is_some() {
                let _ = ReplicationSlotRelease();
            }
            let _ = ReplicationSlotCleanup(true);
            if slot::syncing_replication_slots() {
                reset_syncing_flag();
            }
            Err(e)
        }
    }
}

/// The whole `pg_sync_replication_slots()` body past its pre-checks
/// (slotfuncs.c): parameter validation, dbname check, primary connection,
/// sync, disconnect. Hosted here so slotfuncs stays connection-free.
pub fn sync_replication_slots_sql_body() -> PgResult<()> {
    ValidateSlotSyncParams(true)?;

    let _ = CheckAndGetDbnameFromConninfo()?;

    let cluster_name = guc_tables::vars::cluster_name.read().unwrap_or_default();
    let app_name = if !cluster_name.is_empty() {
        format!("{cluster_name}_slotsync")
    } else {
        "slotsync".to_string()
    };

    let conninfo = guc_tables::vars::PrimaryConnInfo.read().unwrap_or_default();
    let mut conn = match client::connect_extended(&conninfo, false, false, false, &app_name)? {
        Ok(conn) => conn,
        Err(err) => {
            return ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "synchronization worker \"{app_name}\" could not connect to the primary server: {err}"
                ))
                .finish(loc("pg_sync_replication_slots"));
        }
    };

    SyncReplicationSlots(&mut conn)
    // conn drops = walrcv_disconnect.
}

pub fn init_seams() {
    xlogrecovery_seams::shut_down_slot_sync::set(ShutDownSlotSync);
}
