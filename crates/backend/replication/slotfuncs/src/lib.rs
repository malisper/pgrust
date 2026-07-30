#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod builtins;
#[cfg(test)]
mod tests;

use std::sync::atomic::Ordering::{Acquire, Relaxed};

use elog::{elog, ereport};
use init_small::globals as g;
use slot::{ReplicationSlot, RS_PERSISTENT, RS_TEMPORARY};
use types_core::{XLogRecPtr, XLogSegNo};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use xlogreader::LocalPageRead;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

pub const PG_GET_REPLICATION_SLOTS_COLS: usize = 20;

const InvalidXLogRecPtr: XLogRecPtr = 0;

pub(crate) fn create_physical_replication_slot(
    name: &str,
    immediately_reserve: bool,
    temporary: bool,
    restart_lsn: XLogRecPtr,
) -> PgResult<()> {
    assert!(slot::MyReplicationSlot().is_none());

    slot::ReplicationSlotCreate(
        name,
        false,
        if temporary { RS_TEMPORARY } else { RS_PERSISTENT },
        false,
        false,
        false,
    )?;

    if immediately_reserve {
        if restart_lsn == InvalidXLogRecPtr {
            slot::ReplicationSlotReserveWal()?;
        } else {
            let s = slot::MyReplicationSlot().unwrap();
            let mut d = s.data.get();
            d.restart_lsn = restart_lsn;
            s.data.set(d);
        }
        slot::ReplicationSlotMarkDirty();
        slot::ReplicationSlotSave()?;
    }
    Ok(())
}

// create_logical_replication_slot (slotfuncs.c); doesn't release the slot.
pub(crate) fn create_logical_replication_slot(
    name: &str,
    plugin: &str,
    temporary: bool,
    two_phase: bool,
    failover: bool,
    restart_lsn: XLogRecPtr,
    find_startpoint: bool,
) -> PgResult<()> {
    assert!(slot::MyReplicationSlot().is_none());

    slot::ReplicationSlotCreate(
        name,
        true,
        if temporary { RS_TEMPORARY } else { slot::RS_EPHEMERAL },
        two_phase,
        failover,
        false,
    )?;

    let mut ctx = logical::CreateInitDecodingContext(
        plugin,
        Vec::new(),
        false,
        restart_lsn,
        None,
        None,
        None,
    )?;

    if find_startpoint {
        logical_decode::DecodingContextFindStartpoint(&mut ctx)?;
    }

    ctx.free()
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WALAvailability {
    InvalidLsn,
    Reserved,
    Extended,
    Unreserved,
    Removed,
}

pub(crate) fn convert_to_xsegs(mb: i32, segsize: i32) -> u64 {
    (mb as u64) / ((segsize as u64) / (1024 * 1024))
}

pub(crate) fn get_xlog_write_rec_ptr() -> XLogRecPtr {
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.info_lck.with(|| ctl.logWriteResult.load(Acquire))
}

fn xlog_get_last_removed_segno() -> XLogSegNo {
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.info_lck.with(|| ctl.lastRemovedSegNo.load(Relaxed))
}

// KeepLogSeg (xlog.c), hosted here until WAL removal lands: the
// GetOldestUnsummarizedLSN arm is dropped (walsummarizer unported).
fn keep_log_seg(recptr: XLogRecPtr, log_seg_no: XLogSegNo) -> XLogSegNo {
    let segsize = transam_xlog::wal_segment_size();
    let curr_seg_no = transam_xlog::XLByteToSeg(recptr, segsize);
    let mut segno = curr_seg_no;

    let ctl = transam_xlog::ctl::XLogCtl();
    let keep = ctl.info_lck.with(|| ctl.replicationSlotMinLSN.load(Relaxed));
    if keep != InvalidXLogRecPtr && keep < recptr {
        segno = transam_xlog::XLByteToSeg(keep, segsize);
        let max_slot_wal_keep_size_mb = guc_tables::vars::max_slot_wal_keep_size_mb.read();
        if max_slot_wal_keep_size_mb >= 0 && !g::IsBinaryUpgrade() {
            let slot_keep_segs = convert_to_xsegs(max_slot_wal_keep_size_mb, segsize);
            if curr_seg_no - segno > slot_keep_segs {
                segno = curr_seg_no - slot_keep_segs;
            }
        }
    }

    let wal_keep_size_mb = guc_tables::vars::wal_keep_size_mb.read();
    if wal_keep_size_mb > 0 {
        let keep_segs = convert_to_xsegs(wal_keep_size_mb, segsize);
        if curr_seg_no - segno < keep_segs {
            segno = if curr_seg_no <= keep_segs { 1 } else { curr_seg_no - keep_segs };
        }
    }

    if segno < log_seg_no { segno } else { log_seg_no }
}

// GetWALAvailability (xlog.c), hosted here for the same reason.
pub(crate) fn get_wal_availability(target_lsn: XLogRecPtr) -> WALAvailability {
    if target_lsn == InvalidXLogRecPtr {
        return WALAvailability::InvalidLsn;
    }

    let segsize = transam_xlog::wal_segment_size();
    let currpos = get_xlog_write_rec_ptr();
    let oldest_slot_seg = keep_log_seg(currpos, transam_xlog::XLByteToSeg(currpos, segsize));

    let oldest_seg = xlog_get_last_removed_segno() + 1;

    let curr_seg = transam_xlog::XLByteToSeg(currpos, segsize);
    let keep_segs = convert_to_xsegs(guc_tables::vars::max_wal_size_mb.read(), segsize) + 1;
    let oldest_seg_max_wal_size = if curr_seg > keep_segs { curr_seg - keep_segs } else { 1 };

    let target_seg = transam_xlog::XLByteToSeg(target_lsn, segsize);

    if target_seg >= oldest_slot_seg {
        if target_seg >= oldest_seg_max_wal_size {
            return WALAvailability::Reserved;
        }
        return WALAvailability::Extended;
    }
    if target_seg >= oldest_seg {
        return WALAvailability::Unreserved;
    }
    WALAvailability::Removed
}

#[inline(always)]
fn cfi() -> PgResult<()> {
    if g::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

// PhysicalWakeupLogicalWalSnd (walsender.c): the actual wakeup is
// ConditionVariableBroadcast(&WalSndCtl->wal_confirm_rcv_cv), unreachable
// today because no walsender is ever waiting on it (unported); the early
// exits this port needs to be correct for (RecoveryInProgress, no configured
// sync standby slots) are live.
pub(crate) fn PhysicalWakeupLogicalWalSnd() -> PgResult<()> {
    let slot = slot::MyReplicationSlot().expect("PhysicalWakeupLogicalWalSnd: no slot acquired");
    debug_assert!(slot::SlotIsPhysical(slot));

    if transam_xlog::RecoveryInProgress() {
        return Ok(());
    }
    let name = String::from_utf8_lossy(slot.data.get().name.name_str()).into_owned();
    if slot::SlotExistsInSyncStandbySlots(&name) {
        panic!("PhysicalWakeupLogicalWalSnd: wal_confirm_rcv_cv broadcast unported (walsender unported)");
    }
    Ok(())
}

// pg_physical_replication_slot_advance (slotfuncs.c).
pub(crate) fn pg_physical_replication_slot_advance(moveto: XLogRecPtr) -> PgResult<XLogRecPtr> {
    let slot = slot::MyReplicationSlot().expect("pg_physical_replication_slot_advance: no slot");
    let startlsn = slot.data.get().restart_lsn;
    let mut retlsn = startlsn;
    debug_assert!(moveto != InvalidXLogRecPtr);

    if startlsn < moveto {
        slot.with_mutex(|| {
            let mut d = slot.data.get();
            d.restart_lsn = moveto;
            slot.data.set(d);
        });
        retlsn = moveto;

        slot::ReplicationSlotMarkDirty();
        PhysicalWakeupLogicalWalSnd()?;
    }

    Ok(retlsn)
}

// LogicalSlotAdvanceAndCheckSnapState (logical.c): decodes in fast_forward
// mode from the slot's confirmed_flush up to moveto, driving restart_lsn and
// confirmed_flush forward without emitting changes.
pub(crate) fn LogicalSlotAdvanceAndCheckSnapState(
    moveto: XLogRecPtr,
    mut found_consistent_snapshot: Option<&mut bool>,
) -> PgResult<XLogRecPtr> {
    debug_assert!(moveto != InvalidXLogRecPtr);
    if let Some(f) = found_consistent_snapshot.as_deref_mut() {
        *f = false;
    }

    let old_resowner = resowner::CurrentResourceOwner();

    let attempt: PgResult<Box<logical::LogicalDecodingContext>> = (|| {
        let mut ctx = logical::CreateDecodingContext(0, Vec::new(), true, None, None, None)?;

        slot::WaitForStandbyConfirmation(moveto)?;

        ctx.reader.XLogBeginRead(ctx.slot.data.get().restart_lsn);
        inval::local::InvalidateSystemCaches()?;

        let mut routine = LocalPageRead { wait_for_wal: true };
        while ctx.reader.v.EndRecPtr < moveto {
            let record = ctx.reader.XLogReadRecord(&mut routine)?;
            if record.is_none() {
                match ctx.reader.errormsg() {
                    Some(err) => elog(
                        ERROR,
                        format!("could not find record while advancing replication slot: {err}"),
                    ),
                    None => elog(
                        ERROR,
                        "could not find record while advancing replication slot",
                    ),
                }?;
                unreachable!("elog(ERROR) returns Err");
            }
            logical_decode::LogicalDecodingProcessRecord(&mut ctx)?;
            cfi()?;
        }

        if let Some(f) = found_consistent_snapshot {
            if logical::DecodingContextReady(&ctx) {
                *f = true;
            }
        }

        resowner::SetCurrentResourceOwner(old_resowner);

        if ctx.reader.v.EndRecPtr != InvalidXLogRecPtr {
            logical::LogicalConfirmReceivedLocation(moveto)?;
            slot::ReplicationSlotMarkDirty();
        }

        Ok(ctx)
    })();

    match attempt {
        Ok(ctx) => {
            let retlsn = slot::MyReplicationSlot()
                .expect("LogicalSlotAdvanceAndCheckSnapState: no slot")
                .data
                .get()
                .confirmed_flush;
            ctx.free()?;
            inval::local::InvalidateSystemCaches()?;
            Ok(retlsn)
        }
        Err(e) => {
            inval::local::InvalidateSystemCaches()?;
            Err(e)
        }
    }
}

// Result of copy_replication_slot: the tuple-building caller (builtins.rs)
// reads the destination's name/confirmed_flush off MyReplicationSlot() itself
// (still acquired on return), matching C's post-call values[] fill.
pub(crate) struct CopySlotOverrides<'a> {
    pub temporary: Option<bool>,
    pub plugin: Option<&'a str>,
}

// Shared core of copy_replication_slot (slotfuncs.c): locates the source
// slot, validates it, creates the destination slot at src's restart_lsn, then
// re-verifies and installs the source's current xmin/lsn state onto it.
// Callers (builtins.rs) extract fcinfo args and build the result tuple; the
// destination slot is left acquired (not released) on success, as in C.
pub(crate) fn copy_replication_slot(
    src_name: &str,
    dst_name: &str,
    logical_slot: bool,
    overrides: CopySlotOverrides<'_>,
) -> PgResult<()> {
    lwlock::LWLockAcquire(
        lwlock::main_lock(types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK),
        lwlock::LW_SHARED,
        g::MyProcNumber(),
    )?;
    let mut found: Option<(&'static ReplicationSlot, SlotSnapshot)> = None;
    for s in slot::ReplicationSlotCtl() {
        if s.in_use.get() && name_matches(s, src_name) {
            let snap = s.with_mutex(|| snapshot(s));
            found = Some((s, snap));
            break;
        }
    }
    lwlock::LWLockRelease(lwlock::main_lock(
        types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK,
    ))?;

    let Some((src, first)) = found else {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("replication slot \"{src_name}\" does not exist"))
            .finish(loc("copy_replication_slot"));
    };

    let src_islogical = first.database != types_core::InvalidOid;
    let src_restart_lsn = first.restart_lsn;

    if src_islogical != logical_slot {
        let msg = if src_islogical {
            format!("cannot copy physical replication slot \"{src_name}\" as a logical replication slot")
        } else {
            format!("cannot copy logical replication slot \"{src_name}\" as a physical replication slot")
        };
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(msg)
            .finish(loc("copy_replication_slot"));
    }
    if src_restart_lsn == InvalidXLogRecPtr {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot copy a replication slot that doesn't reserve WAL")
            .finish(loc("copy_replication_slot"));
    }
    if first.invalidated != slot::RS_INVAL_NONE {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot copy invalidated replication slot \"{src_name}\""))
            .finish(loc("copy_replication_slot"));
    }

    let temporary = overrides
        .temporary
        .unwrap_or(first.persistency == RS_TEMPORARY);

    if logical_slot {
        let plugin = overrides.plugin.unwrap_or(&first.plugin);
        create_logical_replication_slot(
            dst_name,
            plugin,
            temporary,
            false,
            false,
            src_restart_lsn,
            false,
        )?;
    } else {
        create_physical_replication_slot(dst_name, true, temporary, src_restart_lsn)?;
    }

    let second = src.with_mutex(|| snapshot(src));
    let copy_islogical = second.database != types_core::InvalidOid;
    if second.restart_lsn < src_restart_lsn
        || src_islogical != copy_islogical
        || second.name != src_name
    {
        return ereport(ERROR)
            .errmsg(format!("could not copy replication slot \"{src_name}\""))
            .errdetail(
                "The source replication slot was modified incompatibly during the copy operation.",
            )
            .finish(loc("copy_replication_slot"));
    }
    if src_islogical && second.confirmed_flush == InvalidXLogRecPtr {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot copy unfinished logical replication slot \"{src_name}\""))
            .errhint("Retry when the source replication slot's confirmed_flush_lsn is valid.")
            .finish(loc("copy_replication_slot"));
    }
    if second.invalidated != slot::RS_INVAL_NONE {
        return ereport(ERROR)
            .errmsg(format!("cannot copy replication slot \"{src_name}\""))
            .errdetail("The source replication slot was invalidated during the copy operation.")
            .finish(loc("copy_replication_slot"));
    }

    let dst = slot::MyReplicationSlot().expect("copy_replication_slot: destination not acquired");
    dst.with_mutex(|| {
        dst.effective_xmin.set(second.effective_xmin);
        dst.effective_catalog_xmin.set(second.effective_catalog_xmin);
        let mut d = dst.data.get();
        d.xmin = second.xmin;
        d.catalog_xmin = second.catalog_xmin;
        d.restart_lsn = second.restart_lsn;
        d.confirmed_flush = second.confirmed_flush;
        dst.data.set(d);
    });

    slot::ReplicationSlotMarkDirty();
    slot::ReplicationSlotsComputeRequiredXmin(false)?;
    slot::ReplicationSlotsComputeRequiredLSN()?;
    slot::ReplicationSlotSave()?;

    debug_assert!({
        let segsize = transam_xlog::wal_segment_size();
        let segno = transam_xlog::XLByteToSeg(second.restart_lsn, segsize);
        xlog_get_last_removed_segno() < segno
    });

    if logical_slot && !temporary {
        slot::ReplicationSlotPersist()?;
    }
    Ok(())
}

// Spinlock-guarded snapshot of the fields copy_replication_slot needs (C
// copies the whole ReplicationSlot struct under the mutex instead).
struct SlotSnapshot {
    name: String,
    plugin: String,
    database: types_core::Oid,
    persistency: slot::ReplicationSlotPersistency,
    restart_lsn: XLogRecPtr,
    confirmed_flush: XLogRecPtr,
    invalidated: slot::ReplicationSlotInvalidationCause,
    xmin: types_core::TransactionId,
    catalog_xmin: types_core::TransactionId,
    effective_xmin: types_core::TransactionId,
    effective_catalog_xmin: types_core::TransactionId,
}

fn snapshot(s: &ReplicationSlot) -> SlotSnapshot {
    let d = s.data.get();
    SlotSnapshot {
        name: String::from_utf8_lossy(d.name.name_str()).into_owned(),
        plugin: String::from_utf8_lossy(d.plugin.name_str()).into_owned(),
        database: d.database,
        persistency: d.persistency,
        restart_lsn: d.restart_lsn,
        confirmed_flush: d.confirmed_flush,
        invalidated: d.invalidated,
        xmin: d.xmin,
        catalog_xmin: d.catalog_xmin,
        effective_xmin: s.effective_xmin.get(),
        effective_catalog_xmin: s.effective_catalog_xmin.get(),
    }
}

fn name_matches(s: &ReplicationSlot, name: &str) -> bool {
    s.data.get().name.name_str() == name.as_bytes()
}

// Seam glue: slotsync's update_local_synced_slot advances slots through
// LogicalSlotAdvanceAndCheckSnapState (hosted here beside
// pg_replication_slot_advance); slotfuncs depends on slotsync, so the
// entry point is injected at init.
fn logical_slot_advance_for_slotsync(
    moveto: XLogRecPtr,
) -> types_error::PgResult<(XLogRecPtr, bool)> {
    let mut found_consistent_snapshot = false;
    let retlsn = LogicalSlotAdvanceAndCheckSnapState(moveto, Some(&mut found_consistent_snapshot))?;
    Ok((retlsn, found_consistent_snapshot))
}

pub fn init_seams() {
    slotsync::logical_slot_advance_and_check_snap_state::set(logical_slot_advance_for_slotsync);
}
