#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod builtins;
#[cfg(test)]
mod tests;

use std::sync::atomic::Ordering::{Acquire, Relaxed};

use init_small::globals as g;
use slot::{RS_PERSISTENT, RS_TEMPORARY};
use types_core::{XLogRecPtr, XLogSegNo};
use types_error::PgResult;

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
