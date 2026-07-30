use std::cell::Cell;
use std::sync::atomic::Ordering::Relaxed;

use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::PgResult;

use crate::ctl::XLogCtl;
use crate::*;

thread_local! {
    static CHECK_POINT_DISTANCE_ESTIMATE: Cell<f64> = const { Cell::new(0.0) };
    static PREV_CHECK_POINT_DISTANCE: Cell<f64> = const { Cell::new(0.0) };
}

// CheckXLogRemoved (xlog.c): error out if the segment we intend to read has
// already been recycled/removed under us.
pub fn CheckXLogRemoved(segno: XLogSegNo, tli: TimeLineID) -> PgResult<()> {
    let ctl = XLogCtl();
    let last_removed = ctl.info_lck.with(|| ctl.lastRemovedSegNo.load(Relaxed));
    if segno <= last_removed {
        let filename = crate::XLogFileName(tli, segno, wal_segment_size());
        return elog::ereport(types_error::ERROR)
            .errcode_for_file_access()
            .errmsg(format!("requested WAL segment {filename} has already been removed"))
            .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "CheckXLogRemoved"));
    }
    Ok(())
}

pub(crate) fn UpdateCheckPointDistanceEstimate(nbytes: u64) {
    let nbytes = nbytes as f64;
    PREV_CHECK_POINT_DISTANCE.set(nbytes);
    let est = CHECK_POINT_DISTANCE_ESTIMATE.get();
    CHECK_POINT_DISTANCE_ESTIMATE.set(if est < nbytes { nbytes } else { 0.90 * est + 0.10 * nbytes });
}

fn ConvertToXSegs(mb: i32, wal_segsz: i32) -> u64 {
    XLogMBVarToSegs(mb, wal_segsz) as u64
}

pub(crate) fn XLOGfileslop(lastredoptr: XLogRecPtr) -> XLogSegNo {
    let wal_segsz = wal_segment_size();
    let min_seg_no = lastredoptr / wal_segsz as u64
        + ConvertToXSegs(guc_tables::vars::min_wal_size_mb.read(), wal_segsz)
        - 1;
    let max_seg_no = lastredoptr / wal_segsz as u64
        + ConvertToXSegs(guc_tables::vars::max_wal_size_mb.read(), wal_segsz)
        - 1;

    let mut distance = (1.0 + guc_tables::vars::CheckPointCompletionTarget.read())
        * CHECK_POINT_DISTANCE_ESTIMATE.get();
    distance *= 1.10;

    // Sequential bounds: min_wal_size > max_wal_size is legal (max wins).
    let mut recycle_seg_no =
        ((lastredoptr as f64 + distance) / wal_segsz as f64).ceil() as XLogSegNo;
    if recycle_seg_no < min_seg_no {
        recycle_seg_no = min_seg_no;
    }
    if recycle_seg_no > max_seg_no {
        recycle_seg_no = max_seg_no;
    }
    recycle_seg_no
}

// Returns true when the max_slot_wal_keep_size cap pushed the slot horizon
// forward — C must then invalidate the affected slots.
pub(crate) fn KeepLogSeg(recptr: XLogRecPtr, log_seg_no: &mut XLogSegNo) -> PgResult<bool> {
    let ctl = XLogCtl();
    let keep = ctl.info_lck.with(|| ctl.replicationSlotMinLSN.load(Relaxed));
    let slot_horizon_capped = keep_log_seg_with(recptr, log_seg_no, keep);

    // C applies this clamp between the slot cap and wal_keep_size; all three
    // only lower segno, so applying it after keep_log_seg_with is equivalent.
    let unsummarized = walsummarizer_seams::get_oldest_unsummarized_lsn::call()?;
    if unsummarized != InvalidXLogRecPtr {
        let seg = XLByteToSeg(unsummarized, wal_segment_size());
        if seg < *log_seg_no {
            *log_seg_no = seg;
        }
    }
    Ok(slot_horizon_capped)
}

pub(crate) fn keep_log_seg_with(
    recptr: XLogRecPtr,
    log_seg_no: &mut XLogSegNo,
    keep: XLogRecPtr,
) -> bool {
    let wal_segsz = wal_segment_size();
    let curr_seg_no = XLByteToSeg(recptr, wal_segsz);
    let mut segno = curr_seg_no;
    let mut slot_horizon_capped = false;

    if keep != InvalidXLogRecPtr && keep < recptr {
        segno = XLByteToSeg(keep, wal_segsz);

        let max_slot_keep_mb = guc_tables::vars::max_slot_wal_keep_size_mb.read();
        if max_slot_keep_mb >= 0 && !init_small::globals::IsBinaryUpgrade() {
            let slot_keep_segs = ConvertToXSegs(max_slot_keep_mb, wal_segsz);
            if curr_seg_no - segno > slot_keep_segs {
                segno = curr_seg_no - slot_keep_segs;
                slot_horizon_capped = true;
            }
        }
    }

    let wal_keep_mb = guc_tables::vars::wal_keep_size_mb.read();
    if wal_keep_mb > 0 {
        let keep_segs = ConvertToXSegs(wal_keep_mb, wal_segsz);
        if curr_seg_no - segno < keep_segs {
            segno = if curr_seg_no <= keep_segs { 1 } else { curr_seg_no - keep_segs };
        }
    }

    if segno < *log_seg_no {
        *log_seg_no = segno;
    }
    slot_horizon_capped
}

pub(crate) fn IsXLogFileName(fname: &str) -> bool {
    fname.len() == 24 && fname.bytes().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase())
}

pub(crate) fn IsPartialXLogFileName(fname: &str) -> bool {
    fname.len() == 24 + ".partial".len()
        && IsXLogFileName(&fname[..24])
        && fname.ends_with(".partial")
}

pub(crate) fn XLogFromFileName(fname: &str, wal_segsz: i32) -> (TimeLineID, XLogSegNo) {
    let tli = u32::from_str_radix(&fname[0..8], 16).unwrap_or(0);
    let log = u64::from_str_radix(&fname[8..16], 16).unwrap_or(0);
    let seg = u64::from_str_radix(&fname[16..24], 16).unwrap_or(0);
    (tli, log * XLogSegmentsPerXLogId(wal_segsz) + seg)
}

pub fn XLogGetOldestSegno(tli: TimeLineID) -> PgResult<XLogSegNo> {
    let mut oldest_segno: XLogSegNo = 0;
    let xldir = fd::AllocateDir(XLOGDIR)?;
    while let Some(xlde) = fd::ReadDir(xldir, XLOGDIR)? {
        if !IsXLogFileName(&xlde.d_name) {
            continue;
        }
        let (file_tli, file_segno) = XLogFromFileName(&xlde.d_name, wal_segment_size());
        if tli != file_tli {
            continue;
        }
        if oldest_segno == 0 || file_segno < oldest_segno {
            oldest_segno = file_segno;
        }
    }
    fd::FreeDir(xldir)?;
    Ok(oldest_segno)
}

fn UpdateLastRemovedPtr(fname: &str) {
    let (_, segno) = XLogFromFileName(fname, wal_segment_size());
    let ctl = XLogCtl();
    ctl.info_lck.with(|| {
        if segno > ctl.lastRemovedSegNo.load(Relaxed) {
            ctl.lastRemovedSegNo.store(segno, Relaxed);
        }
    });
}

// archive_mode=off is C's XLogArchiveCheckDone fast path; the fast path also
// keeps seam-less unit tests off the uninstalled-seam panic.
fn xlog_archive_check_done(xlog: &str) -> PgResult<bool> {
    if !XLogArchivingActive() {
        return Ok(true);
    }
    xlogarchive_seams::xlog_archive_check_done::call(xlog)
}

pub(crate) fn xlog_archive_cleanup(xlog: &str) {
    if xlogarchive_seams::xlog_archive_cleanup::is_installed() {
        return xlogarchive_seams::xlog_archive_cleanup::call(xlog);
    }
    let _ = fd::pg_unlink(&format!("{XLOGDIR}/archive_status/{xlog}.done"));
    let _ = fd::pg_unlink(&format!("{XLOGDIR}/archive_status/{xlog}.ready"));
}

// The keep test ignores names' TLI so a parent timeline's segments survive.
pub(crate) fn RemoveOldXlogFiles(
    segno: XLogSegNo,
    lastredoptr: XLogRecPtr,
    endptr: XLogRecPtr,
    insert_tli: TimeLineID,
) -> PgResult<()> {
    let wal_segsz = wal_segment_size();
    let mut endlog_seg_no = XLByteToSeg(endptr, wal_segsz);
    let recycle_seg_no = XLOGfileslop(lastredoptr);

    let lastoff = XLogFileName(0, segno, wal_segsz);

    let xldir = fd::AllocateDir(XLOGDIR)?;
    if xldir.is_none() {
        return Ok(());
    }
    while let Some(de) = fd::ReadDirExtended(xldir, XLOGDIR, types_error::DEBUG1)? {
        let fname = de.d_name.as_str();
        if !IsXLogFileName(fname) && !IsPartialXLogFileName(fname) {
            continue;
        }
        // Full-tail compare keeps an equal-segno ".partial" (C strcmp parity).
        if fname[8..] <= lastoff[8..] && xlog_archive_check_done(fname)? {
            UpdateLastRemovedPtr(fname);
            RemoveXlogFile(fname, recycle_seg_no, &mut endlog_seg_no, insert_tli)?;
        }
    }
    fd::FreeDir(xldir)?;
    Ok(())
}

// After a timeline switch: drop old-timeline segments past the switch point.
pub fn RemoveNonParentXlogFiles(
    switchpoint: XLogRecPtr,
    new_tli: TimeLineID,
) -> PgResult<()> {
    let wal_segsz = wal_segment_size();
    let switch_seg_no = XLByteToPrevSeg(switchpoint, wal_segsz);
    let mut endlog_seg_no = XLByteToSeg(switchpoint, wal_segsz);
    let recycle_seg_no = endlog_seg_no + 10;

    let switchseg = XLogFileName(new_tli, switch_seg_no, wal_segsz);
    let _ = elog::elog(
        types_error::DEBUG2,
        format!("attempting to remove WAL segments newer than log file {switchseg}"),
    );

    let xldir = fd::AllocateDir(XLOGDIR)?;
    if xldir.is_none() {
        return Ok(());
    }
    while let Some(de) = fd::ReadDirExtended(xldir, XLOGDIR, types_error::DEBUG1)? {
        let fname = de.d_name.as_str();
        if !IsXLogFileName(fname) {
            continue;
        }
        if fname[..8] < switchseg[..8] && fname[8..] > switchseg[8..] {
            if !xlogarchive_seams::xlog_archive_is_ready::call(fname) {
                RemoveXlogFile(fname, recycle_seg_no, &mut endlog_seg_no, new_tli)?;
            }
        }
    }
    fd::FreeDir(xldir)?;
    Ok(())
}

// Only regular files recycle — never rename an archive symlink into pg_wal.
fn RemoveXlogFile(
    segname: &str,
    recycle_seg_no: XLogSegNo,
    endlog_seg_no: &mut XLogSegNo,
    insert_tli: TimeLineID,
) -> PgResult<()> {
    let path = format!("{XLOGDIR}/{segname}");

    // lstat: a symlink into pg_wal must never recycle (dirent d_type parity).
    let mut fi = fd::FileInfo::zeroed();
    let is_reg = fd::pg_lstat(&path, &mut fi) == 0 && fi.is_file();
    if guc_tables::vars::wal_recycle.read()
        && *endlog_seg_no <= recycle_seg_no
        && XLogCtl().InstallXLogFileSegmentActive.load(Relaxed)
        && is_reg
        && crate::write::InstallXLogFileSegment(
            endlog_seg_no,
            &path,
            true,
            recycle_seg_no,
            insert_tli,
        )?
    {
        crate::startup::checkpoint_stats_bump_segs_recycled();
        *endlog_seg_no += 1;
    } else {
        if fd::durable_unlink(&path, types_error::LOG).map(|rc| rc != 0).unwrap_or(true) {
            return Ok(());
        }
        crate::startup::checkpoint_stats_bump_segs_removed();
    }

    xlog_archive_cleanup(segname);
    Ok(())
}
