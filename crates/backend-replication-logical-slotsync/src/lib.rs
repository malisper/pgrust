//! `replication/logical/slotsync.c` — synchronizing logical failover
//! replication slots to a physical standby from the primary server.
//!
//! slotsync.c's OWN logic — the slot-sync state machine, the worker main loop,
//! the `RemoteSlot` decode, the GUC validation / config-reread flow, the
//! shutdown / restart / syncing-flag bookkeeping, and the `SlotSyncCtx`
//! shared-memory control area (a real `slock_t`-guarded struct, owned here) —
//! is ported in this crate. The subsystems it merely calls into (slot.c,
//! snapbuild.c, logical.c, xlog/xlogrecovery, procarray, lmgr/lwlock, the
//! walreceiver, xact, dbcommands, the GUC machinery, the process/init
//! substrate) are reached through their owners' seam crates; those panic
//! loudly until each owner lands.
//!
//! The `ReplicationSlot`, `WalReceiverConn`, `WalRcvExecResult`, and
//! `TupleTableSlot` objects slotsync passes around are owner-resident; they are
//! named through `types_replication` handles carried by value through the
//! owner seams (the owner maps each handle to the live object).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::cell::Cell;

use types_core::primitive::{Oid, Size, TimestampTz, TransactionId, XLogRecPtr, XLogSegNo};
use types_error::error::{DEBUG1, ERROR, LOG};
use types_error::pg_error::{PgError, PgResult};
use types_error::{
    ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ErrorLevel, SqlState,
};
use types_core::xact::{
    FirstNormalTransactionId, InvalidTransactionId, InvalidXLogRecPtr,
};
use types_wal::WAL_LEVEL_LOGICAL;

use types_replication::{
    ReplicationSlotHandle, ReplicationSlotInvalidationCause,
    WrConnHandle, DatabaseRelationId, RS_INVAL_NONE, RS_TEMPORARY,
    WALRCV_OK_TUPLES,
};
use types_storage::lock::AccessShareLock;
use types_storage::storage::{
    LWLockMode, LW_EXCLUSIVE, LW_SHARED, PROC_ARRAY_LOCK, REPLICATION_SLOT_ALLOCATION_LOCK,
    REPLICATION_SLOT_CONTROL_LOCK,
};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use types_pgstat::wait_event::{
    WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN, WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN,
};
use types_datum::Datum;

use backend_storage_lmgr_s_lock::Spinlock;

// Owner seam crate aliases.
use backend_replication_slot_seams as slot;
use backend_replication_walreceiver_seams as walrcv;
use backend_replication_walsender_seams as walsnd;
use backend_replication_snapbuild_seams as snapbuild;
use backend_replication_logical_logical_seams as logical;
use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xlogrecovery_seams as xlogrecovery;
use backend_access_transam_xact_seams as xact;
use backend_storage_ipc_procarray_seams as procarray;
use backend_storage_ipc_seams as ipc;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_lmgr_lmgr_seams as lmgr;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_commands_dbcommands_seams as dbcommands;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_misc_guc_seams as guc;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_tcop_postgres_seams as postgres;
use backend_utils_error_seams as elog;
use backend_utils_adt_quote_seams as quote;

// ===========================================================================
// Compile-time constants (slotsync.c file scope).
// ===========================================================================

/// `#define MIN_SLOTSYNC_WORKER_NAPTIME_MS 200`
const MIN_SLOTSYNC_WORKER_NAPTIME_MS: i64 = 200;
/// `#define MAX_SLOTSYNC_WORKER_NAPTIME_MS 30000` (30s)
const MAX_SLOTSYNC_WORKER_NAPTIME_MS: i64 = 30000;
/// `#define SLOTSYNC_RESTART_INTERVAL_SEC 10`
const SLOTSYNC_RESTART_INTERVAL_SEC: u32 = 10;
/// `#define SLOTSYNC_COLUMN_COUNT 10`
const SLOTSYNC_COLUMN_COUNT: i32 = 10;
/// `#define PRIMARY_INFO_OUTPUT_COL_COUNT 2`
const PRIMARY_INFO_OUTPUT_COL_COUNT: i32 = 2;

// pg_type OIDs for the slot-sync queries (pg_type.h).
const TEXTOID: Oid = 25;
const LSNOID: Oid = 3220;
const XIDOID: Oid = 28;
const BOOLOID: Oid = 16;

/// `InvalidPid` — `(-1)` (miscadmin.h).
const InvalidPid: i32 = -1;

// ===========================================================================
// Per-process file-static tuning state (slotsync.c). These are per-backend
// (not shared memory) -> thread_local.
// ===========================================================================

thread_local! {
    /// `static long sleep_ms = MIN_SLOTSYNC_WORKER_NAPTIME_MS;`
    static SLEEP_MS: Cell<i64> = const { Cell::new(MIN_SLOTSYNC_WORKER_NAPTIME_MS) };
    /// `static bool syncing_slots = false;` — true only if THIS process is
    /// performing slot synchronization.
    static SYNCING_SLOTS: Cell<bool> = const { Cell::new(false) };
    /// `static SlotSyncCtxStruct *SlotSyncCtx = NULL;` — this backend's mapped
    /// pointer to the shared-memory control area (set in `SlotSyncShmemInit`).
    static SLOT_SYNC_CTX: Cell<*mut SlotSyncCtxStruct> = const { Cell::new(core::ptr::null_mut()) };
}

#[inline]
fn sleep_ms() -> i64 {
    SLEEP_MS.with(|c| c.get())
}
#[inline]
fn set_sleep_ms(v: i64) {
    SLEEP_MS.with(|c| c.set(v));
}
#[inline]
fn syncing_slots() -> bool {
    SYNCING_SLOTS.with(|c| c.get())
}
#[inline]
fn set_syncing_slots(v: bool) {
    SYNCING_SLOTS.with(|c| c.set(v));
}

// ===========================================================================
// SlotSyncCtxStruct — the shared-memory control area (slotsync.c). Owned here.
// ===========================================================================

/// `typedef struct SlotSyncCtxStruct` (slotsync.c). `#[repr(C)]` so
/// `size_of` matches the `ShmemInitStruct` reservation.
#[repr(C)]
struct SlotSyncCtxStruct {
    pid: i32,
    stop_signaled: bool,
    syncing: bool,
    last_start_time: i64,
    mutex: Spinlock,
}

/// `&SlotSyncCtx->...` — the live control area for this backend. Panics if
/// accessed before `SlotSyncShmemInit` (C dereferences a NULL `SlotSyncCtx`).
#[inline]
fn ctx<'a>() -> &'a SlotSyncCtxStruct {
    let p = SLOT_SYNC_CTX.with(|c| c.get());
    assert!(!p.is_null(), "SlotSyncCtx accessed before SlotSyncShmemInit");
    unsafe { &*p }
}
#[inline]
fn ctx_mut<'a>() -> &'a mut SlotSyncCtxStruct {
    let p = SLOT_SYNC_CTX.with(|c| c.get());
    assert!(!p.is_null(), "SlotSyncCtx accessed before SlotSyncShmemInit");
    unsafe { &mut *p }
}
#[inline]
fn ctx_is_initialized() -> bool {
    !SLOT_SYNC_CTX.with(|c| c.get()).is_null()
}

/// `SpinLockAcquire(&SlotSyncCtx->mutex)`.
#[inline]
fn ctx_spin_acquire() {
    let lock = &ctx().mutex;
    if lock.tas() != 0 {
        backend_storage_lmgr_s_lock::s_lock(lock, None, 0, None);
    }
}
/// `SpinLockRelease(&SlotSyncCtx->mutex)`.
#[inline]
fn ctx_spin_release() {
    ctx().mutex.unlock();
}

// ===========================================================================
// transam.h xid comparison macros (pure arithmetic; faithful to transam.c).
// ===========================================================================

#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}
#[inline]
fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 > id2;
    }
    (id1.wrapping_sub(id2) as i32) > 0
}

// ===========================================================================
// xlog/lsn helpers used by the owned logic (xlog_internal.h / pg_lsn.h).
// ===========================================================================

#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}
#[inline]
fn XLogRecPtrIsValid(r: XLogRecPtr) -> bool {
    r != InvalidXLogRecPtr
}
#[inline]
fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / (wal_segsz_bytes as u64)
}
#[inline]
fn lsn_str(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}
#[inline]
fn Min(a: i64, b: i64) -> i64 {
    if a < b { a } else { b }
}
#[inline]
fn MaxLsn(a: XLogRecPtr, b: XLogRecPtr) -> XLogRecPtr {
    if a > b { a } else { b }
}

// ===========================================================================
// LWLock offset translation for the named locks slotsync uses.
// ===========================================================================

#[inline]
fn lwlock_offset(lock: usize) -> usize {
    lock
}
#[inline]
fn lwlock_acquire(lock: usize, mode: LWLockMode) -> PgResult<()> {
    lwlock::lwlock_acquire_main::call(lwlock_offset(lock), mode)?;
    Ok(())
}
#[inline]
fn lwlock_release(lock: usize) -> PgResult<()> {
    lwlock::lwlock_release_main::call(lwlock_offset(lock))
}

// ===========================================================================
// emit_log — ereport at LOG/DEBUG levels (sub-ERROR; errfinish returns).
// ===========================================================================

fn emit_log(level: i32, msg: &str, detail: Option<&str>, hint: Option<&str>) {
    let mut err = PgError::new(ErrorLevel(level), msg.to_string());
    if let Some(d) = detail {
        err = err.with_detail(d.to_string());
    }
    if let Some(h) = hint {
        err = err.with_hint(h.to_string());
    }
    // At LOG/DEBUG, ereport emits and returns Ok(()); we discard the unit.
    let _ = elog::ereport::call(err);
}

// ===========================================================================
// RemoteSlot — info fetched from the primary about one logical slot.
// ===========================================================================

/// `typedef struct RemoteSlot` (slotsync.c). Field order faithful to C.
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
    /// RS_INVAL_NONE if valid, or the reason of invalidation.
    invalidated: ReplicationSlotInvalidationCause,
}

impl RemoteSlot {
    /// `palloc0(sizeof(RemoteSlot))` — zero-initialized remote slot.
    fn new() -> Self {
        RemoteSlot {
            name: String::new(),
            plugin: String::new(),
            database: String::new(),
            two_phase: false,
            failover: false,
            restart_lsn: InvalidXLogRecPtr,
            confirmed_lsn: InvalidXLogRecPtr,
            two_phase_at: InvalidXLogRecPtr,
            catalog_xmin: InvalidTransactionId,
            invalidated: RS_INVAL_NONE,
        }
    }
}

// ===========================================================================
// update_local_synced_slot
// ===========================================================================

fn update_local_synced_slot(
    remote_slot: &RemoteSlot,
    remote_dbid: Oid,
    mut found_consistent_snapshot: Option<&mut bool>,
    mut remote_slot_precedes: Option<&mut bool>,
) -> PgResult<bool> {
    let s: ReplicationSlotHandle = slot::my_replication_slot::call();
    let mut updated_xmin_or_lsn = false;
    let mut updated_config = false;

    debug_assert!(slot::slot_data_invalidated::call(s) == RS_INVAL_NONE);

    if let Some(fc) = found_consistent_snapshot.as_deref_mut() {
        *fc = false;
    }
    if let Some(rp) = remote_slot_precedes.as_deref_mut() {
        *rp = false;
    }

    let slot_restart_lsn = slot::slot_data_restart_lsn::call(s);
    let slot_catalog_xmin = slot::slot_data_catalog_xmin::call(s);

    // Don't overwrite if we already have a newer catalog_xmin and restart_lsn.
    if remote_slot.restart_lsn < slot_restart_lsn
        || TransactionIdPrecedes(remote_slot.catalog_xmin, slot_catalog_xmin)
    {
        let level = if slot::slot_data_persistency::call(s) == RS_TEMPORARY {
            LOG.0
        } else {
            DEBUG1.0
        };
        emit_log(
            level,
            &format!(
                "could not synchronize replication slot \"{}\"",
                remote_slot.name
            ),
            Some(&format!(
                "Synchronization could lead to data loss, because the remote slot needs WAL at LSN {} and catalog xmin {}, but the standby has LSN {} and catalog xmin {}.",
                lsn_str(remote_slot.restart_lsn),
                remote_slot.catalog_xmin,
                lsn_str(slot_restart_lsn),
                slot_catalog_xmin
            )),
            None,
        );

        if let Some(p) = remote_slot_precedes.as_deref_mut() {
            *p = true;
        }
        return Ok(false);
    }

    let slot_confirmed_flush = slot::slot_data_confirmed_flush::call(s);

    if remote_slot.confirmed_lsn > slot_confirmed_flush
        || remote_slot.restart_lsn > slot_restart_lsn
        || TransactionIdFollows(remote_slot.catalog_xmin, slot_catalog_xmin)
    {
        if snapbuild::snap_build_snapshot_exists::call(remote_slot.restart_lsn) {
            slot::slot_spin_acquire::call(s)?;
            slot::set_slot_data_restart_lsn::call(s, remote_slot.restart_lsn);
            slot::set_slot_data_confirmed_flush::call(s, remote_slot.confirmed_lsn);
            slot::set_slot_data_catalog_xmin::call(s, remote_slot.catalog_xmin);
            slot::slot_spin_release::call(s)?;

            if let Some(fc) = found_consistent_snapshot.as_deref_mut() {
                *fc = true;
            }
        } else {
            logical::logical_slot_advance_and_check_snap_state::call(
                remote_slot.confirmed_lsn,
                found_consistent_snapshot.as_deref_mut(),
            )?;

            if slot::slot_data_confirmed_flush::call(s) != remote_slot.confirmed_lsn {
                return Err(PgError::error(format!(
                    "synchronized confirmed_flush for slot \"{}\" differs from remote slot",
                    remote_slot.name
                ))
                .with_detail(format!(
                    "Remote slot has LSN {} but local slot has LSN {}.",
                    lsn_str(remote_slot.confirmed_lsn),
                    lsn_str(slot::slot_data_confirmed_flush::call(s))
                )));
            }
        }

        updated_xmin_or_lsn = true;
    }

    if remote_dbid != slot::slot_data_database::call(s)
        || remote_slot.two_phase != slot::slot_data_two_phase::call(s)
        || remote_slot.failover != slot::slot_data_failover::call(s)
        || remote_slot.plugin != slot::slot_data_plugin::call(s)
        || remote_slot.two_phase_at != slot::slot_data_two_phase_at::call(s)
    {
        let plugin_name = remote_slot.plugin.clone();

        slot::slot_spin_acquire::call(s)?;
        slot::set_slot_data_plugin::call(s, &plugin_name);
        slot::set_slot_data_database::call(s, remote_dbid);
        slot::set_slot_data_two_phase::call(s, remote_slot.two_phase);
        slot::set_slot_data_two_phase_at::call(s, remote_slot.two_phase_at);
        slot::set_slot_data_failover::call(s, remote_slot.failover);
        slot::slot_spin_release::call(s)?;

        updated_config = true;

        debug_assert!(
            slot::slot_data_two_phase_at::call(s) <= slot::slot_data_confirmed_flush::call(s)
        );
    }

    if updated_config || updated_xmin_or_lsn {
        slot::replication_slot_mark_dirty::call()?;
        slot::replication_slot_save::call()?;
    }

    if updated_xmin_or_lsn {
        slot::slot_spin_acquire::call(s)?;
        slot::set_slot_effective_catalog_xmin::call(s, remote_slot.catalog_xmin);
        slot::slot_spin_release::call(s)?;

        slot::replication_slots_compute_required_xmin::call(false)?;
        slot::replication_slots_compute_required_lsn::call()?;
    }

    Ok(updated_config || updated_xmin_or_lsn)
}

// ===========================================================================
// get_local_synced_slots
// ===========================================================================

fn get_local_synced_slots() -> PgResult<Vec<ReplicationSlotHandle>> {
    let mut local_slots: Vec<ReplicationSlotHandle> = Vec::new();

    lwlock_acquire(REPLICATION_SLOT_CONTROL_LOCK, LW_SHARED)?;

    for i in 0..slot::max_replication_slots::call() {
        let s = slot::replication_slot::call(i);

        if slot::slot_in_use::call(s) && slot::slot_data_synced::call(s) {
            debug_assert!(slot::slot_is_logical::call(s));
            local_slots
                .try_reserve(1)
                .map_err(|_| PgError::error("out of memory"))?;
            local_slots.push(s);
        }
    }

    lwlock_release(REPLICATION_SLOT_CONTROL_LOCK)?;

    Ok(local_slots)
}

// ===========================================================================
// local_sync_slot_required
// ===========================================================================

fn local_sync_slot_required(
    local_slot: ReplicationSlotHandle,
    remote_slots: &[RemoteSlot],
) -> PgResult<bool> {
    let mut remote_exists = false;
    let mut locally_invalidated = false;

    let local_name = slot::slot_data_name::call(local_slot);

    for remote_slot in remote_slots {
        if remote_slot.name == local_name {
            remote_exists = true;

            slot::slot_spin_acquire::call(local_slot)?;
            locally_invalidated = (remote_slot.invalidated == RS_INVAL_NONE)
                && (slot::slot_data_invalidated::call(local_slot) != RS_INVAL_NONE);
            slot::slot_spin_release::call(local_slot)?;

            break;
        }
    }

    Ok(remote_exists && !locally_invalidated)
}

// ===========================================================================
// drop_local_obsolete_slots
// ===========================================================================

fn drop_local_obsolete_slots(remote_slot_list: &[RemoteSlot]) -> PgResult<()> {
    let local_slots = get_local_synced_slots()?;

    for local_slot in local_slots {
        if !local_sync_slot_required(local_slot, remote_slot_list)? {
            let dbid = slot::slot_data_database::call(local_slot);
            lmgr::lock_shared_object::call(DatabaseRelationId, dbid, 0, AccessShareLock)?;

            slot::slot_spin_acquire::call(local_slot)?;
            let synced_slot =
                slot::slot_in_use::call(local_slot) && slot::slot_data_synced::call(local_slot);
            slot::slot_spin_release::call(local_slot)?;

            if synced_slot {
                let name = slot::slot_data_name::call(local_slot);
                slot::replication_slot_acquire::call(&name, true, false)?;
                slot::replication_slot_drop_acquired::call()?;
            }

            let dbid = slot::slot_data_database::call(local_slot);
            lmgr::unlock_shared_object::call(DatabaseRelationId, dbid, 0, AccessShareLock)?;

            emit_log(
                LOG.0,
                &format!(
                    "dropped replication slot \"{}\" of database with OID {}",
                    slot::slot_data_name::call(local_slot),
                    slot::slot_data_database::call(local_slot)
                ),
                None,
                None,
            );
        }
    }

    Ok(())
}

// ===========================================================================
// reserve_wal_for_local_slot
// ===========================================================================

fn reserve_wal_for_local_slot(restart_lsn: XLogRecPtr) -> PgResult<()> {
    let s: ReplicationSlotHandle = slot::my_replication_slot::call();

    debug_assert!(!s.is_none());
    debug_assert!(!XLogRecPtrIsValid(slot::slot_data_restart_lsn::call(s)));

    lwlock_acquire(REPLICATION_SLOT_ALLOCATION_LOCK, LW_EXCLUSIVE)?;

    let mut min_safe_lsn = xlog::get_redo_rec_ptr::call();
    let slot_min_lsn = xlog::xlog_get_replication_slot_minimum_lsn::call();

    if XLogRecPtrIsValid(slot_min_lsn) && min_safe_lsn > slot_min_lsn {
        min_safe_lsn = slot_min_lsn;
    }

    slot::slot_spin_acquire::call(s)?;
    slot::set_slot_data_restart_lsn::call(s, MaxLsn(restart_lsn, min_safe_lsn));
    slot::slot_spin_release::call(s)?;

    slot::replication_slots_compute_required_lsn::call()?;

    let segno = XLByteToSeg(slot::slot_data_restart_lsn::call(s), xlog::wal_segment_size::call());
    if xlog::xlog_get_last_removed_segno::call() >= segno {
        let name = slot::slot_data_name::call(s);
        return Err(PgError::error(format!(
            "WAL required by replication slot {name} has been removed concurrently"
        )));
    }

    lwlock_release(REPLICATION_SLOT_ALLOCATION_LOCK)?;

    Ok(())
}

// ===========================================================================
// update_and_persist_local_synced_slot
// ===========================================================================

fn update_and_persist_local_synced_slot(
    remote_slot: &RemoteSlot,
    remote_dbid: Oid,
) -> PgResult<bool> {
    let s: ReplicationSlotHandle = slot::my_replication_slot::call();
    let mut found_consistent_snapshot = false;
    let mut remote_slot_precedes = false;

    let _ = update_local_synced_slot(
        remote_slot,
        remote_dbid,
        Some(&mut found_consistent_snapshot),
        Some(&mut remote_slot_precedes),
    )?;

    if remote_slot_precedes {
        return Ok(false);
    }

    if !found_consistent_snapshot {
        emit_log(
            LOG.0,
            &format!(
                "could not synchronize replication slot \"{}\"",
                remote_slot.name
            ),
            Some(&format!(
                "Synchronization could lead to data loss, because the standby could not build a consistent snapshot to decode WALs at LSN {}.",
                lsn_str(slot::slot_data_restart_lsn::call(s))
            )),
            None,
        );

        return Ok(false);
    }

    slot::replication_slot_persist::call()?;

    emit_log(
        LOG.0,
        &format!(
            "newly created replication slot \"{}\" is sync-ready now",
            remote_slot.name
        ),
        None,
        None,
    );

    Ok(true)
}

// ===========================================================================
// synchronize_one_slot
// ===========================================================================

fn synchronize_one_slot(remote_slot: &RemoteSlot, remote_dbid: Oid) -> PgResult<bool> {
    let mut slot_updated = false;

    let latest_flush_ptr = walsnd::get_standby_flush_rec_ptr::call();
    if remote_slot.confirmed_lsn > latest_flush_ptr {
        let msg = format!(
            "skipping slot synchronization because the received slot sync LSN {} for slot \"{}\" is ahead of the standby position {}",
            lsn_str(remote_slot.confirmed_lsn),
            remote_slot.name,
            lsn_str(latest_flush_ptr)
        );
        if miscinit::am_logical_slot_sync_worker_process::call() {
            emit_log(LOG.0, &msg, None, None);
        } else {
            return Err(PgError::error(msg)
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
        }

        return Ok(false);
    }

    let s = slot::search_named_replication_slot::call(&remote_slot.name, true);
    if !s.is_none() {
        slot::slot_spin_acquire::call(s)?;
        let synced = slot::slot_data_synced::call(s);
        slot::slot_spin_release::call(s)?;

        if !synced {
            return Err(PgError::error(format!(
                "exiting from slot synchronization because same name slot \"{}\" already exists on the standby",
                remote_slot.name
            ))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
        }

        slot::replication_slot_acquire::call(&remote_slot.name, true, false)?;

        debug_assert!(s == slot::my_replication_slot::call());

        if slot::slot_data_invalidated::call(s) == RS_INVAL_NONE
            && remote_slot.invalidated != RS_INVAL_NONE
        {
            slot::slot_spin_acquire::call(s)?;
            slot::set_slot_data_invalidated::call(s, remote_slot.invalidated);
            slot::slot_spin_release::call(s)?;

            slot::replication_slot_mark_dirty::call()?;
            slot::replication_slot_save::call()?;

            slot_updated = true;
        }

        if slot::slot_data_invalidated::call(s) != RS_INVAL_NONE {
            slot::replication_slot_release::call()?;
            return Ok(slot_updated);
        }

        if slot::slot_data_persistency::call(s) == RS_TEMPORARY {
            slot_updated = update_and_persist_local_synced_slot(remote_slot, remote_dbid)?;
        } else {
            if remote_slot.confirmed_lsn < slot::slot_data_confirmed_flush::call(s) {
                return Err(PgError::error(format!(
                    "cannot synchronize local slot \"{}\"",
                    remote_slot.name
                ))
                .with_detail(format!(
                    "Local slot's start streaming location LSN({}) is ahead of remote slot's LSN({}).",
                    lsn_str(slot::slot_data_confirmed_flush::call(s)),
                    lsn_str(remote_slot.confirmed_lsn)
                )));
            }

            slot_updated = update_local_synced_slot(remote_slot, remote_dbid, None, None)?;
        }
    } else {
        if remote_slot.invalidated != RS_INVAL_NONE {
            return Ok(false);
        }

        slot::replication_slot_create::call(
            &remote_slot.name,
            true,
            RS_TEMPORARY,
            remote_slot.two_phase,
            remote_slot.failover,
            true,
        )?;

        let s = slot::my_replication_slot::call();
        let plugin_name = remote_slot.plugin.clone();

        slot::slot_spin_acquire::call(s)?;
        slot::set_slot_data_database::call(s, remote_dbid);
        slot::set_slot_data_plugin::call(s, &plugin_name);
        slot::slot_spin_release::call(s)?;

        reserve_wal_for_local_slot(remote_slot.restart_lsn)?;

        lwlock_acquire(REPLICATION_SLOT_CONTROL_LOCK, LW_EXCLUSIVE)?;
        lwlock_acquire(PROC_ARRAY_LOCK, LW_EXCLUSIVE)?;
        let xmin_horizon = procarray::get_oldest_safe_decoding_transaction_id::call(true);
        slot::slot_spin_acquire::call(s)?;
        slot::set_slot_effective_catalog_xmin::call(s, xmin_horizon);
        slot::set_slot_data_catalog_xmin::call(s, xmin_horizon);
        slot::slot_spin_release::call(s)?;
        slot::replication_slots_compute_required_xmin::call(true)?;
        lwlock_release(PROC_ARRAY_LOCK)?;
        lwlock_release(REPLICATION_SLOT_CONTROL_LOCK)?;

        update_and_persist_local_synced_slot(remote_slot, remote_dbid)?;

        slot_updated = true;
    }

    slot::replication_slot_release::call()?;

    Ok(slot_updated)
}

// ===========================================================================
// synchronize_slots
// ===========================================================================

fn synchronize_slots(wrconn: WrConnHandle) -> PgResult<bool> {
    let slot_row: [Oid; SLOTSYNC_COLUMN_COUNT as usize] = [
        TEXTOID, TEXTOID, LSNOID, LSNOID, XIDOID, BOOLOID, LSNOID, BOOLOID, TEXTOID, TEXTOID,
    ];

    let mut remote_slot_list: Vec<RemoteSlot> = Vec::new();
    let mut some_slot_updated = false;
    let mut started_tx = false;
    let query = "SELECT slot_name, plugin, confirmed_flush_lsn, restart_lsn, catalog_xmin, two_phase, two_phase_at, failover, database, invalidation_reason FROM pg_catalog.pg_replication_slots WHERE failover and NOT temporary";

    if !xact::is_transaction_state::call() {
        xact::start_transaction_command::call()?;
        started_tx = true;
    }

    let res = walrcv::walrcv_exec::call(wrconn, query, SLOTSYNC_COLUMN_COUNT, &slot_row)?;
    if walrcv::res_status::call(res) != WALRCV_OK_TUPLES {
        let err = walrcv::res_err::call(res).unwrap_or_default();
        return Err(PgError::error(format!(
            "could not fetch failover logical slots info from the primary server: {err}"
        )));
    }

    let tupslot = walrcv::make_result_tupslot::call(res)?;
    while walrcv::result_gettupleslot::call(res, tupslot)? {
        let mut remote_slot = RemoteSlot::new();
        let mut col: i32 = 0;

        col += 1;
        let (name, isnull) = walrcv::getattr_text::call(tupslot, col)?;
        remote_slot.name = name.unwrap_or_default();
        debug_assert!(!isnull);

        col += 1;
        let (plugin, isnull) = walrcv::getattr_text::call(tupslot, col)?;
        remote_slot.plugin = plugin.unwrap_or_default();
        debug_assert!(!isnull);

        col += 1;
        let (lsn, isnull) = walrcv::getattr_lsn::call(tupslot, col)?;
        remote_slot.confirmed_lsn = if isnull { InvalidXLogRecPtr } else { lsn };

        col += 1;
        let (lsn, isnull) = walrcv::getattr_lsn::call(tupslot, col)?;
        remote_slot.restart_lsn = if isnull { InvalidXLogRecPtr } else { lsn };

        col += 1;
        let (xid, isnull) = walrcv::getattr_xid::call(tupslot, col)?;
        remote_slot.catalog_xmin = if isnull { InvalidTransactionId } else { xid };

        col += 1;
        let (two_phase, isnull) = walrcv::getattr_bool::call(tupslot, col)?;
        remote_slot.two_phase = two_phase;
        debug_assert!(!isnull);

        col += 1;
        let (lsn, isnull) = walrcv::getattr_lsn::call(tupslot, col)?;
        remote_slot.two_phase_at = if isnull { InvalidXLogRecPtr } else { lsn };

        col += 1;
        let (failover, isnull) = walrcv::getattr_bool::call(tupslot, col)?;
        remote_slot.failover = failover;
        debug_assert!(!isnull);

        col += 1;
        let (database, isnull) = walrcv::getattr_text::call(tupslot, col)?;
        remote_slot.database = database.unwrap_or_default();
        debug_assert!(!isnull);

        col += 1;
        let (reason, isnull) = walrcv::getattr_text::call(tupslot, col)?;
        remote_slot.invalidated = if isnull {
            RS_INVAL_NONE
        } else {
            slot::get_slot_invalidation_cause::call(&reason.unwrap_or_default())?
        };

        debug_assert!(col == SLOTSYNC_COLUMN_COUNT);

        if (XLogRecPtrIsInvalid(remote_slot.restart_lsn)
            || XLogRecPtrIsInvalid(remote_slot.confirmed_lsn)
            || !TransactionIdIsValid(remote_slot.catalog_xmin))
            && remote_slot.invalidated == RS_INVAL_NONE
        {
            drop(remote_slot);
        } else {
            remote_slot_list
                .try_reserve(1)
                .map_err(|_| PgError::error("out of memory"))?;
            remote_slot_list.push(remote_slot);
        }

        walrcv::exec_clear_tuple::call(tupslot)?;
    }

    drop_local_obsolete_slots(&remote_slot_list)?;

    for remote_slot in &remote_slot_list {
        let remote_dbid = dbcommands::get_database_oid::call(&remote_slot.database, false)?;

        lmgr::lock_shared_object::call(DatabaseRelationId, remote_dbid, 0, AccessShareLock)?;

        some_slot_updated |= synchronize_one_slot(remote_slot, remote_dbid)?;

        lmgr::unlock_shared_object::call(DatabaseRelationId, remote_dbid, 0, AccessShareLock)?;
    }

    drop(remote_slot_list);

    walrcv::walrcv_clear_result::call(res)?;

    if started_tx {
        xact::commit_transaction_command::call()?;
    }

    Ok(some_slot_updated)
}

// ===========================================================================
// validate_remote_info
// ===========================================================================

fn validate_remote_info(wrconn: WrConnHandle) -> PgResult<()> {
    let slot_row: [Oid; PRIMARY_INFO_OUTPUT_COL_COUNT as usize] = [BOOLOID, BOOLOID];
    let mut started_tx = false;

    let primary_slot_name = xlogrecovery::primary_slot_name::call().unwrap_or_default();
    let cmd = format!(
        "SELECT pg_is_in_recovery(), count(*) = 1 FROM pg_catalog.pg_replication_slots WHERE slot_type='physical' AND slot_name={}",
        quote::quote_literal_cstr::call(&primary_slot_name)
    );

    if !xact::is_transaction_state::call() {
        xact::start_transaction_command::call()?;
        started_tx = true;
    }

    let res = walrcv::walrcv_exec::call(wrconn, &cmd, PRIMARY_INFO_OUTPUT_COL_COUNT, &slot_row)?;

    if walrcv::res_status::call(res) != WALRCV_OK_TUPLES {
        let err = walrcv::res_err::call(res).unwrap_or_default();
        return Err(PgError::error(format!(
            "could not fetch primary slot name \"{primary_slot_name}\" info from the primary server: {err}"
        ))
        .with_hint("Check if \"primary_slot_name\" is configured correctly."));
    }

    let tupslot = walrcv::make_result_tupslot::call(res)?;
    if !walrcv::result_gettupleslot::call(res, tupslot)? {
        return Err(PgError::error(
            "failed to fetch tuple for the primary server slot specified by \"primary_slot_name\"",
        ));
    }

    let (remote_in_recovery, isnull) = walrcv::getattr_bool::call(tupslot, 1)?;
    debug_assert!(!isnull);

    if remote_in_recovery {
        return Err(
            PgError::error("cannot synchronize replication slots from a standby server")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        );
    }

    let (primary_slot_valid, isnull) = walrcv::getattr_bool::call(tupslot, 2)?;
    debug_assert!(!isnull);

    if !primary_slot_valid {
        return Err(PgError::error(format!(
            "replication slot \"{primary_slot_name}\" specified by \"{}\" does not exist on primary server",
            "primary_slot_name"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    walrcv::exec_clear_tuple::call(tupslot)?;
    walrcv::walrcv_clear_result::call(res)?;

    if started_tx {
        xact::commit_transaction_command::call()?;
    }

    Ok(())
}

// ===========================================================================
// CheckAndGetDbnameFromConninfo
// ===========================================================================

pub fn CheckAndGetDbnameFromConninfo() -> PgResult<String> {
    let primary_conninfo = xlogrecovery::primary_conn_info::call().unwrap_or_default();
    let dbname = walrcv::walrcv_get_dbname_from_conninfo::call(&primary_conninfo)?;
    match dbname {
        None => Err(PgError::error(format!(
            "replication slot synchronization requires \"{}\" to be specified in \"{}\"",
            "dbname", "primary_conninfo"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)),
        Some(name) => Ok(name),
    }
}

// ===========================================================================
// ValidateSlotSyncParams
// ===========================================================================

pub fn ValidateSlotSyncParams(elevel: i32) -> PgResult<bool> {
    if xlog::wal_level::call() < WAL_LEVEL_LOGICAL {
        report_at(
            elevel,
            "replication slot synchronization requires \"wal_level\" >= \"logical\"",
            ERRCODE_INVALID_PARAMETER_VALUE,
        )?;
        return Ok(false);
    }

    let primary_slot_name = xlogrecovery::primary_slot_name::call();
    if primary_slot_name
        .as_deref()
        .map(str::is_empty)
        .unwrap_or(true)
    {
        report_at(
            elevel,
            &format!(
                "replication slot synchronization requires \"{}\" to be set",
                "primary_slot_name"
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        )?;
        return Ok(false);
    }

    if !walrcv::hot_standby_feedback::call() {
        report_at(
            elevel,
            &format!(
                "replication slot synchronization requires \"{}\" to be enabled",
                "hot_standby_feedback"
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        )?;
        return Ok(false);
    }

    let primary_conninfo = xlogrecovery::primary_conn_info::call();
    if primary_conninfo
        .as_deref()
        .map(str::is_empty)
        .unwrap_or(true)
    {
        report_at(
            elevel,
            &format!(
                "replication slot synchronization requires \"{}\" to be set",
                "primary_conninfo"
            ),
            ERRCODE_INVALID_PARAMETER_VALUE,
        )?;
        return Ok(false);
    }

    Ok(true)
}

// ===========================================================================
// slotsync_reread_config
// ===========================================================================

fn slotsync_reread_config() -> PgResult<()> {
    let old_primary_conninfo = xlogrecovery::primary_conn_info::call().unwrap_or_default();
    let old_primary_slotname = xlogrecovery::primary_slot_name::call().unwrap_or_default();
    let old_sync_replication_slots = sync_replication_slots_guc();
    let old_hot_standby_feedback = walrcv::hot_standby_feedback::call();

    debug_assert!(old_sync_replication_slots);

    backend_postmaster_interrupt::SetConfigReloadPending(false);
    guc::process_config_file_sighup::call()?;

    let new_primary_conninfo = xlogrecovery::primary_conn_info::call().unwrap_or_default();
    let new_primary_slotname = xlogrecovery::primary_slot_name::call().unwrap_or_default();
    let new_sync_replication_slots = sync_replication_slots_guc();
    let new_hot_standby_feedback = walrcv::hot_standby_feedback::call();

    let conninfo_changed = old_primary_conninfo != new_primary_conninfo;
    let primary_slotname_changed = old_primary_slotname != new_primary_slotname;

    if old_sync_replication_slots != new_sync_replication_slots {
        emit_log(
            LOG.0,
            &format!(
                "replication slot synchronization worker will shut down because \"{}\" is disabled",
                "sync_replication_slots"
            ),
            None,
            None,
        );
        do_proc_exit(0);
    }

    if conninfo_changed
        || primary_slotname_changed
        || (old_hot_standby_feedback != new_hot_standby_feedback)
    {
        emit_log(
            LOG.0,
            "replication slot synchronization worker will restart because of a parameter change",
            None,
            None,
        );

        // Reset the last-start time so the postmaster can restart immediately.
        // C writes SlotSyncCtx->last_start_time = 0 directly, WITHOUT the
        // spinlock (treated as lock-free here and in SlotSyncWorkerCanRestart).
        ctx_mut().last_start_time = 0;

        do_proc_exit(0);
    }

    Ok(())
}

// ===========================================================================
// ProcessSlotSyncInterrupts
// ===========================================================================

fn ProcessSlotSyncInterrupts(_wrconn: WrConnHandle) -> PgResult<()> {
    postgres::check_for_interrupts::call()?;

    ctx_spin_acquire();
    let stop_signaled = ctx().stop_signaled;
    ctx_spin_release();

    if stop_signaled {
        emit_log(
            LOG.0,
            "replication slot synchronization worker is shutting down because promotion is triggered",
            None,
            None,
        );
        do_proc_exit(0);
    }

    if backend_postmaster_interrupt::ConfigReloadPending() {
        slotsync_reread_config()?;
    }

    Ok(())
}

// ===========================================================================
// slotsync_worker_disconnect / slotsync_worker_onexit (before_shmem_exit cbs)
// ===========================================================================

/// `slotsync_worker_disconnect(code, arg)` (slotsync.c). `arg` is the wrconn.
pub fn slotsync_worker_disconnect(_code: i32, arg: Datum) -> PgResult<()> {
    let wrconn = WrConnHandle(arg.as_u64());
    walrcv::walrcv_disconnect::call(wrconn)
}

/// `slotsync_worker_onexit(code, arg)` (slotsync.c).
pub fn slotsync_worker_onexit(_code: i32, _arg: Datum) -> PgResult<()> {
    if !slot::my_replication_slot::call().is_none() {
        slot::replication_slot_release::call()?;
    }

    slot::replication_slot_cleanup::call(false)?;

    ctx_spin_acquire();
    ctx_mut().pid = InvalidPid;

    // If syncing_slots is true, the process errored out without resetting the
    // flag, so clean up shared memory and reset the flag here.
    if syncing_slots() {
        ctx_mut().syncing = false;
        set_syncing_slots(false);
    }
    ctx_spin_release();

    Ok(())
}

// ===========================================================================
// wait_for_slot_activity
// ===========================================================================

fn wait_for_slot_activity(some_slot_updated: bool) -> PgResult<()> {
    if !some_slot_updated {
        set_sleep_ms(Min(sleep_ms() * 2, MAX_SLOTSYNC_WORKER_NAPTIME_MS));
    } else {
        set_sleep_ms(MIN_SLOTSYNC_WORKER_NAPTIME_MS);
    }

    let rc = latch::wait_latch_my_latch::call(
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        sleep_ms(),
        WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN,
    );

    if (rc as u32) & WL_LATCH_SET != 0 {
        latch::reset_latch_my_latch::call();
    }

    Ok(())
}

// ===========================================================================
// check_and_set_sync_info
// ===========================================================================

fn check_and_set_sync_info(worker_pid: i32) -> PgResult<()> {
    ctx_spin_acquire();

    debug_assert!(worker_pid == InvalidPid || ctx().pid == InvalidPid);

    if ctx().stop_signaled {
        ctx_spin_release();
        return Err(PgError::error(
            "cannot synchronize replication slots when standby promotion is ongoing",
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    if ctx().syncing {
        ctx_spin_release();
        return Err(
            PgError::error("cannot synchronize replication slots concurrently")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        );
    }

    ctx_mut().syncing = true;
    ctx_mut().pid = worker_pid;

    ctx_spin_release();

    set_syncing_slots(true);

    Ok(())
}

// ===========================================================================
// reset_syncing_flag
// ===========================================================================

fn reset_syncing_flag() -> PgResult<()> {
    ctx_spin_acquire();
    ctx_mut().syncing = false;
    ctx_spin_release();

    set_syncing_slots(false);

    Ok(())
}

// ===========================================================================
// ReplSlotSyncWorkerMain
// ===========================================================================

pub fn ReplSlotSyncWorkerMain(startup_data_len: usize) -> PgResult<()> {
    debug_assert!(startup_data_len == 0);

    miscinit::set_my_backend_type_slotsync::call()?;
    miscinit::init_ps_display::call()?;

    miscinit::init_process::call()?;
    miscinit::base_init::call()?;

    debug_assert!(ctx_is_initialized());

    miscinit::setup_signal_handlers::call()?;

    check_and_set_sync_info(miscinit::my_proc_pid::call())?;

    emit_log(LOG.0, "slot sync worker started", None, None);

    // Register the onexit callback as soon as SlotSyncCtx->pid is initialized.
    ipc::before_shmem_exit::call(slotsync_worker_onexit, Datum::from_i64(0))?;

    miscinit::initialize_timeouts::call()?;
    walrcv::load_libpqwalreceiver::call()?;
    miscinit::unblock_signals::call()?;
    guc::set_config_option_search_path_empty::call()?;

    let dbname = CheckAndGetDbnameFromConninfo()?;
    miscinit::init_postgres::call(&dbname)?;
    miscinit::set_processing_mode_normal::call()?;

    let cluster = guc::cluster_name::call();
    let app_name = if !cluster.is_empty() {
        format!("{cluster}_{}", "slotsync worker")
    } else {
        "slotsync worker".to_string()
    };

    let primary_conninfo = xlogrecovery::primary_conn_info::call().unwrap_or_default();
    let (wrconn, err) =
        walrcv::walrcv_connect::call(&primary_conninfo, false, false, false, &app_name)?;

    if wrconn.is_none() {
        return Err(PgError::error(format!(
            "synchronization worker \"{app_name}\" could not connect to the primary server: {}",
            err.unwrap_or_default()
        ))
        .with_sqlstate(ERRCODE_CONNECTION_FAILURE));
    }

    ipc::before_shmem_exit::call(slotsync_worker_disconnect, Datum::from_u64(wrconn.0))?;

    validate_remote_info(wrconn)?;

    loop {
        ProcessSlotSyncInterrupts(wrconn)?;

        let some_slot_updated = synchronize_slots(wrconn)?;

        wait_for_slot_activity(some_slot_updated)?;
    }
}

// ===========================================================================
// update_synced_slots_inactive_since
// ===========================================================================

fn update_synced_slots_inactive_since() -> PgResult<()> {
    let mut now: TimestampTz = 0;

    if !xlogrecovery::standby_mode::call() {
        return Ok(());
    }

    debug_assert!(ctx().pid == InvalidPid && !ctx().syncing);

    lwlock_acquire(REPLICATION_SLOT_CONTROL_LOCK, LW_SHARED)?;

    for i in 0..slot::max_replication_slots::call() {
        let s = slot::replication_slot::call(i);

        if slot::slot_in_use::call(s) && slot::slot_data_synced::call(s) {
            debug_assert!(slot::slot_is_logical::call(s));
            debug_assert!(slot::slot_active_pid::call(s) == 0);

            if now == 0 {
                now = timestamp::get_current_timestamp::call();
            }

            slot::replication_slot_set_inactive_since::call(s, now, true)?;
        }
    }

    lwlock_release(REPLICATION_SLOT_CONTROL_LOCK)?;

    Ok(())
}

// ===========================================================================
// ShutDownSlotSync
// ===========================================================================

pub fn ShutDownSlotSync() -> PgResult<()> {
    ctx_spin_acquire();

    ctx_mut().stop_signaled = true;

    if !ctx().syncing {
        ctx_spin_release();
        update_synced_slots_inactive_since()?;
        return Ok(());
    }

    let worker_pid = ctx().pid;

    ctx_spin_release();

    if worker_pid != InvalidPid {
        latch::kill_sigusr1::call(worker_pid)?;
    }

    loop {
        let rc = latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN,
        );

        if (rc as u32) & WL_LATCH_SET != 0 {
            latch::reset_latch_my_latch::call();
            postgres::check_for_interrupts::call()?;
        }

        ctx_spin_acquire();

        if !ctx().syncing {
            break;
        }

        ctx_spin_release();
    }

    ctx_spin_release();

    update_synced_slots_inactive_since()?;

    Ok(())
}

// ===========================================================================
// SlotSyncWorkerCanRestart
// ===========================================================================

pub fn SlotSyncWorkerCanRestart() -> PgResult<bool> {
    // C: time_t curtime = time(NULL); — whole-second wall clock, not the
    // microsecond TimestampTz. The throttle compares the delta against
    // SLOTSYNC_RESTART_INTERVAL_SEC (10 seconds), so the unit must be seconds.
    let curtime: i64 = time_seconds();
    let last = ctx().last_start_time;

    // C: if ((unsigned int) (curtime - last) < (unsigned int) INTERVAL)
    if (curtime.wrapping_sub(last) as u32) < SLOTSYNC_RESTART_INTERVAL_SEC {
        return Ok(false);
    }

    ctx_mut().last_start_time = curtime;

    Ok(true)
}

/// `time(NULL)` — current wall-clock time in whole seconds since the Unix
/// epoch (a `time_t`). Read directly off the OS clock, matching the repo's
/// existing direct OS-time usage (pg_rusage, error-report timestamping).
fn time_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => -(e.duration().as_secs() as i64),
    }
}

// ===========================================================================
// IsSyncingReplicationSlots
// ===========================================================================

pub fn IsSyncingReplicationSlots() -> bool {
    syncing_slots()
}

// ===========================================================================
// SlotSyncShmemSize / SlotSyncShmemInit
// ===========================================================================

pub fn SlotSyncShmemSize() -> Size {
    core::mem::size_of::<SlotSyncCtxStruct>()
}

pub fn SlotSyncShmemInit() -> PgResult<()> {
    let size = SlotSyncShmemSize();

    let (ptr, found) = backend_storage_ipc_shmem_seams::shmem_init_struct::call("Slot Sync Data", size)?;
    let ctx_ptr = ptr as *mut SlotSyncCtxStruct;

    if !found {
        // memset(SlotSyncCtx, 0, size); SlotSyncCtx->pid = InvalidPid;
        // SpinLockInit(&SlotSyncCtx->mutex);
        unsafe {
            core::ptr::write_bytes(ptr, 0, size);
            core::ptr::write(
                ctx_ptr,
                SlotSyncCtxStruct {
                    pid: InvalidPid,
                    stop_signaled: false,
                    syncing: false,
                    last_start_time: 0,
                    mutex: Spinlock::new(),
                },
            );
        }
    }

    SLOT_SYNC_CTX.with(|c| c.set(ctx_ptr));

    Ok(())
}

// ===========================================================================
// slotsync_failure_callback
// ===========================================================================

/// `slotsync_failure_callback(code, arg)` (slotsync.c). `arg` is the wrconn.
pub fn slotsync_failure_callback(_code: i32, wrconn: WrConnHandle) -> PgResult<()> {
    if !slot::my_replication_slot::call().is_none() {
        slot::replication_slot_release::call()?;
    }

    slot::replication_slot_cleanup::call(true)?;

    if syncing_slots() {
        reset_syncing_flag()?;
    }

    walrcv::walrcv_disconnect::call(wrconn)?;

    Ok(())
}

// ===========================================================================
// SyncReplicationSlots
// ===========================================================================

pub fn SyncReplicationSlots(wrconn: WrConnHandle) -> PgResult<()> {
    let body = (|| -> PgResult<()> {
        check_and_set_sync_info(InvalidPid)?;
        validate_remote_info(wrconn)?;
        synchronize_slots(wrconn)?;
        slot::replication_slot_cleanup::call(true)?;
        reset_syncing_flag()?;
        Ok(())
    })();

    match body {
        Ok(()) => Ok(()),
        Err(e) => {
            // PG_ENSURE_ERROR_CLEANUP runs the cleanup callback on error, then
            // re-raises.
            let _ = slotsync_failure_callback(0, wrconn);
            Err(e)
        }
    }
}

// ===========================================================================
// Local helpers.
// ===========================================================================

/// `sync_replication_slots` GUC (slotsync.c) — OWNED here as a per-backend
/// thread-local; assigned by the GUC machinery on SET/reload (modeled through
/// the assign hook when the GUC wiring lands). Until then the worker only ever
/// runs with it on; the SIGHUP reread compares old/new.
fn sync_replication_slots_guc() -> bool {
    SYNC_REPLICATION_SLOTS.with(|c| c.get())
}

thread_local! {
    /// `bool sync_replication_slots = false;` (slotsync.c GUC variable).
    static SYNC_REPLICATION_SLOTS: Cell<bool> = const { Cell::new(false) };
}

/// `proc_exit(code)` — the worker exits; never returns. `MyProcPid` is read off
/// the process-identity seam (the no-ambient-global rule: `proc_exit` takes the
/// caller's pid explicitly).
fn do_proc_exit(code: i32) -> ! {
    ipc::proc_exit::call(code, miscinit::my_proc_pid::call())
}

/// Emit a report at `elevel`; if the level is ERROR or worse, surface as an
/// `Err` (so C's non-returning `ereport(ERROR, ...)` is honored).
fn report_at(elevel: i32, msg: &str, sqlstate: SqlState) -> PgResult<()> {
    if elevel >= ERROR.0 {
        Err(PgError::error(msg.to_string()).with_sqlstate(sqlstate))
    } else {
        emit_log(elevel, msg, None, None);
        Ok(())
    }
}

// ===========================================================================
// Seam installation.
// ===========================================================================

pub fn init_seams() {
    use backend_replication_logical_slotsync_seams as s;
    s::shut_down_slot_sync::set(ShutDownSlotSync);
    s::validate_slot_sync_params::set(ValidateSlotSyncParams);
    s::slot_sync_worker_can_restart::set(SlotSyncWorkerCanRestart);
    s::is_syncing_replication_slots::set(IsSyncingReplicationSlots);
    s::sync_replication_slots::set(SyncReplicationSlots);
    s::check_and_get_dbname_from_conninfo::set(CheckAndGetDbnameFromConninfo);
    s::repl_slot_sync_worker_main::set(ReplSlotSyncWorkerMain);
    s::slot_sync_shmem_size::set(SlotSyncShmemSize);
    s::slot_sync_shmem_init::set(SlotSyncShmemInit);
}
