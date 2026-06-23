//! WAL retention-horizon arithmetic (xlog.c, PostgreSQL 18.3) — the pure cores
//! behind the WAL-retention paths: [`XLOGfileslop`], [`KeepLogSeg`],
//! [`GetWALAvailability`], the `XLogGetOldestSegno` directory-scan filter
//! ([`XLogGetOldestSegnoFromNames`]), the `RemoveOldXlogFiles` candidate filter
//! ([`IsOldXlogFileCandidate`]) + the `UpdateLastRemovedPtr` name parse
//! ([`SegnoFromRemovableXLogFileName`]), and the
//! `UpdateCheckPointDistanceEstimate` moving average
//! ([`UpdateCheckPointDistanceEstimateCore`]).
//!
//! Every function here is the 1:1 xlog.c LOGIC with its genuine externals lifted
//! to parameters: the GUC posture, the cross-subsystem reads (slot minimum LSN,
//! oldest-unsummarized LSN, `IsBinaryUpgrade`, the live write position, the
//! `XLogCtl->lastRemovedSegNo` field) and the `pg_wal` directory scan are
//! supplied by the (deferred) process-singleton driver.

#![allow(non_snake_case)]

use utils_error::PgResult;

use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use wal::xlog_consts::WALAvailability;

use crate::{
    ConvertToXSegs, IsPartialXLogFileName, IsXLogFileName, XLByteToSeg, XLogFromFileName,
    XLogRecPtrIsInvalid, InvalidXLogRecPtr, XLOG_FILE_SUFFIX_PARTIAL,
};

/// `ceil()` over a non-negative finite `f64`, truncated into `u64`. `core` has
/// no `f64::ceil`; for the non-negative LSN+distance quotient this integer
/// round-up is exact.
fn ceil_to_u64(x: f64) -> u64 {
    let t = x as u64;
    if (t as f64) < x {
        t + 1
    } else {
        t
    }
}

/// `XLOGfileslop(lastredoptr)` (xlog.c:2254) — at a checkpoint, the highest
/// segment that should be recycled as a preallocated future XLOG segment.
#[allow(clippy::too_many_arguments)]
pub fn XLOGfileslop(
    lastredoptr: XLogRecPtr,
    wal_segment_size: i32,
    min_wal_size_mb: i32,
    max_wal_size_mb: i32,
    checkpoint_completion_target: f64,
    checkpoint_distance_estimate: f64,
) -> XLogSegNo {
    let min_seg_no: XLogSegNo = lastredoptr / wal_segment_size as u64
        + ConvertToXSegs(min_wal_size_mb, wal_segment_size) as u64
        - 1;
    let max_seg_no: XLogSegNo = lastredoptr / wal_segment_size as u64
        + ConvertToXSegs(max_wal_size_mb, wal_segment_size) as u64
        - 1;

    let mut distance = (1.0 + checkpoint_completion_target) * checkpoint_distance_estimate;
    distance *= 1.10;

    let mut recycle_seg_no: XLogSegNo =
        ceil_to_u64((lastredoptr as f64 + distance) / wal_segment_size as f64);

    if recycle_seg_no < min_seg_no {
        recycle_seg_no = min_seg_no;
    }
    if recycle_seg_no > max_seg_no {
        recycle_seg_no = max_seg_no;
    }
    recycle_seg_no
}

/// `KeepLogSeg(recptr, *logSegNo)` (xlog.c:8020) — retreat `log_seg_no` to the
/// last segment that must be retained because of `wal_keep_size`, replication
/// slots (capped by `max_slot_wal_keep_size`), or pending WAL summarization.
#[allow(clippy::too_many_arguments)]
pub fn KeepLogSeg(
    recptr: XLogRecPtr,
    log_seg_no: XLogSegNo,
    wal_segment_size: i32,
    slot_minimum_lsn: XLogRecPtr,
    max_slot_wal_keep_size_mb: i32,
    is_binary_upgrade: bool,
    oldest_unsummarized_lsn: XLogRecPtr,
    wal_keep_size_mb: i32,
) -> XLogSegNo {
    let curr_seg_no = XLByteToSeg(recptr, wal_segment_size);
    let mut segno = curr_seg_no;

    let keep = slot_minimum_lsn;
    if keep != InvalidXLogRecPtr && keep < recptr {
        segno = XLByteToSeg(keep, wal_segment_size);

        if max_slot_wal_keep_size_mb >= 0 && !is_binary_upgrade {
            let slot_keep_segs: u64 =
                ConvertToXSegs(max_slot_wal_keep_size_mb, wal_segment_size) as u64;

            if curr_seg_no - segno > slot_keep_segs {
                segno = curr_seg_no - slot_keep_segs;
            }
        }
    }

    let keep = oldest_unsummarized_lsn;
    if keep != InvalidXLogRecPtr {
        let unsummarized_segno = XLByteToSeg(keep, wal_segment_size);
        if unsummarized_segno < segno {
            segno = unsummarized_segno;
        }
    }

    if wal_keep_size_mb > 0 {
        let keep_segs: u64 = ConvertToXSegs(wal_keep_size_mb, wal_segment_size) as u64;
        if curr_seg_no - segno < keep_segs {
            if curr_seg_no <= keep_segs {
                segno = 1;
            } else {
                segno = curr_seg_no - keep_segs;
            }
        }
    }

    if segno < log_seg_no {
        segno
    } else {
        log_seg_no
    }
}

/// `GetWALAvailability(targetLSN)` (xlog.c:7936) — classify a WAL position's
/// retention state for `pg_get_replication_slots`.
pub fn GetWALAvailability(
    target_lsn: XLogRecPtr,
    currpos: XLogRecPtr,
    oldest_slot_seg: XLogSegNo,
    last_removed_segno: XLogSegNo,
    wal_segment_size: i32,
    max_wal_size_mb: i32,
) -> WALAvailability {
    if XLogRecPtrIsInvalid(target_lsn) {
        return WALAvailability::InvalidLsn;
    }

    let oldest_seg: XLogSegNo = last_removed_segno + 1;

    let curr_seg = XLByteToSeg(currpos, wal_segment_size);
    let keep_segs: u64 = ConvertToXSegs(max_wal_size_mb, wal_segment_size) as u64 + 1;

    let oldest_seg_max_wal_size: XLogSegNo = if curr_seg > keep_segs {
        curr_seg - keep_segs
    } else {
        1
    };

    let target_seg = XLByteToSeg(target_lsn, wal_segment_size);

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

/// `XLogGetOldestSegno(tli)` (xlog.c:3794), with the `AllocateDir(XLOGDIR)`
/// entry stream lifted to an iterator of file names.
pub fn XLogGetOldestSegnoFromNames<'a>(
    names: impl Iterator<Item = &'a str>,
    tli: TimeLineID,
    wal_segment_size: i32,
) -> XLogSegNo {
    let mut oldest_segno: XLogSegNo = 0;

    for name in names {
        if !IsXLogFileName(name) {
            continue;
        }

        let Ok((file_tli, file_segno)) = XLogFromFileName(name, wal_segment_size) else {
            continue;
        };

        if tli != file_tli {
            continue;
        }

        if oldest_segno == 0 || file_segno < oldest_segno {
            oldest_segno = file_segno;
        }
    }

    oldest_segno
}

/// The per-entry name filter of `RemoveOldXlogFiles` (xlog.c:3885). The timeline
/// part (chars 0..8) is IGNORED in the comparison, exactly as in C; the
/// comparison is `strcmp(d_name + 8, lastoff + 8) <= 0`.
pub fn IsOldXlogFileCandidate(fname: &str, lastoff: &str) -> bool {
    if !IsXLogFileName(fname) && !IsPartialXLogFileName(fname) {
        return false;
    }
    fname.as_bytes()[8..] <= lastoff.as_bytes()[8..]
}

/// `UpdateLastRemovedPtr(filename)`'s name -> `(tli, segno)` parse
/// (xlog.c:3830). The C `XLogFromFileName` reads the first 24 hex chars and so
/// also accepts the `.partial` names this is called with; strip the suffix first.
pub fn SegnoFromRemovableXLogFileName(
    fname: &str,
    wal_segment_size: i32,
) -> PgResult<(TimeLineID, XLogSegNo)> {
    let base = fname.strip_suffix(XLOG_FILE_SUFFIX_PARTIAL).unwrap_or(fname);
    XLogFromFileName(base, wal_segment_size)
}

/// `UpdateCheckPointDistanceEstimate(nbytes)` (xlog.c:6848) — the bump-fast /
/// decay-slow moving average of the inter-checkpoint WAL distance.
pub fn UpdateCheckPointDistanceEstimateCore(
    check_point_distance_estimate: f64,
    nbytes: u64,
) -> f64 {
    if check_point_distance_estimate < nbytes as f64 {
        nbytes as f64
    } else {
        0.90 * check_point_distance_estimate + 0.10 * nbytes as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal::xlog_consts::DEFAULT_XLOG_SEG_SIZE;

    const SEG: i32 = DEFAULT_XLOG_SEG_SIZE;
    const SEG64: u64 = DEFAULT_XLOG_SEG_SIZE as u64;

    #[test]
    fn ceil_to_u64_is_ceil() {
        assert_eq!(ceil_to_u64(0.0), 0);
        assert_eq!(ceil_to_u64(1.0), 1);
        assert_eq!(ceil_to_u64(1.0001), 2);
        assert_eq!(ceil_to_u64(41.999), 42);
        assert_eq!(ceil_to_u64(42.0), 42);
    }

    #[test]
    fn xlogfileslop_clamps_between_min_and_max_wal_size() {
        let lastredoptr: u64 = 10 * SEG64;
        let slop = XLOGfileslop(lastredoptr, SEG, 80, 1024, 0.9, 0.0);
        assert_eq!(slop, 14);

        let slop = XLOGfileslop(lastredoptr, SEG, 80, 1024, 0.9, 1e15);
        assert_eq!(slop, 73);

        let slop = XLOGfileslop(lastredoptr, SEG, 80, 1024, 0.9, 20.0 * SEG64 as f64);
        assert_eq!(slop, 52);
    }

    #[test]
    fn keep_log_seg_no_retention_returns_input() {
        let recptr = 9 * SEG64 + 1234;
        let got = KeepLogSeg(recptr, 7, SEG, InvalidXLogRecPtr, -1, false, InvalidXLogRecPtr, 0);
        assert_eq!(got, 7);
    }

    #[test]
    fn keep_log_seg_slot_horizon_retreats() {
        let recptr = 9 * SEG64;
        let keep = 3 * SEG64 + 17;
        let got = KeepLogSeg(recptr, 9, SEG, keep, -1, false, InvalidXLogRecPtr, 0);
        assert_eq!(got, 3);
    }

    #[test]
    fn keep_log_seg_caps_slots_at_max_slot_wal_keep_size() {
        let recptr = 9 * SEG64;
        let keep = SEG64;
        let got = KeepLogSeg(recptr, 9, SEG, keep, 32, false, InvalidXLogRecPtr, 0);
        assert_eq!(got, 7);

        let got = KeepLogSeg(recptr, 9, SEG, keep, 32, true, InvalidXLogRecPtr, 0);
        assert_eq!(got, 1);
    }

    #[test]
    fn keep_log_seg_wal_keep_size_floor() {
        let recptr = 9 * SEG64;
        let got = KeepLogSeg(recptr, 9, SEG, InvalidXLogRecPtr, -1, false, InvalidXLogRecPtr, 64);
        assert_eq!(got, 5);

        let recptr = 2 * SEG64;
        let got = KeepLogSeg(recptr, 2, SEG, InvalidXLogRecPtr, -1, false, InvalidXLogRecPtr, 64);
        assert_eq!(got, 1);
    }

    #[test]
    fn keep_log_seg_summarizer_holds_wal() {
        let recptr = 9 * SEG64;
        let keep = 5 * SEG64;
        let got = KeepLogSeg(recptr, 9, SEG, keep, -1, false, 2 * SEG64 + 5, 0);
        assert_eq!(got, 2);
    }

    #[test]
    fn keep_log_seg_never_advances() {
        let recptr = 9 * SEG64;
        let got = KeepLogSeg(recptr, 4, SEG, InvalidXLogRecPtr, -1, false, InvalidXLogRecPtr, 0);
        assert_eq!(got, 4);
    }

    #[test]
    fn get_wal_availability_classifies_all_states() {
        let currpos = 100 * SEG64 + 7;
        let oldest_slot_seg = 30u64;
        let last_removed = 9u64;

        assert_eq!(
            GetWALAvailability(InvalidXLogRecPtr, currpos, oldest_slot_seg, last_removed, SEG, 1024),
            WALAvailability::InvalidLsn
        );
        assert_eq!(
            GetWALAvailability(40 * SEG64, currpos, oldest_slot_seg, last_removed, SEG, 1024),
            WALAvailability::Reserved
        );
        assert_eq!(
            GetWALAvailability(32 * SEG64, currpos, oldest_slot_seg, last_removed, SEG, 1024),
            WALAvailability::Extended
        );
        assert_eq!(
            GetWALAvailability(20 * SEG64, currpos, oldest_slot_seg, last_removed, SEG, 1024),
            WALAvailability::Unreserved
        );
        assert_eq!(
            GetWALAvailability(5 * SEG64, currpos, oldest_slot_seg, last_removed, SEG, 1024),
            WALAvailability::Removed
        );
        assert_eq!(
            GetWALAvailability(SEG64, 2 * SEG64, 1, 0, SEG, 1024),
            WALAvailability::Reserved
        );
    }

    #[test]
    fn oldest_segno_scan_filters_and_takes_min() {
        let names = [
            "000000010000000000000005",
            "000000010000000000000003",
            "000000020000000000000001",
            "000000010000000000000004.partial",
            "xlogtemp.1234",
            "00000001000000000000000A.backup",
        ];
        let got = XLogGetOldestSegnoFromNames(names.iter().copied(), 1, SEG);
        assert_eq!(got, 3);

        let got = XLogGetOldestSegnoFromNames(names.iter().copied(), 7, SEG);
        assert_eq!(got, 0);
    }

    #[test]
    fn old_xlog_candidate_ignores_timeline_and_keeps_partial_of_lastoff() {
        let lastoff = crate::XLogFileName(0, 4, SEG);
        assert_eq!(lastoff, "000000000000000000000004");

        assert!(IsOldXlogFileCandidate("000000010000000000000004", &lastoff));
        assert!(IsOldXlogFileCandidate("000000020000000000000003", &lastoff));
        assert!(!IsOldXlogFileCandidate("000000010000000000000005", &lastoff));
        assert!(!IsOldXlogFileCandidate("000000010000000000000004.partial", &lastoff));
        assert!(IsOldXlogFileCandidate("000000010000000000000003.partial", &lastoff));
        assert!(!IsOldXlogFileCandidate("xlogtemp.99", &lastoff));
    }

    #[test]
    fn segno_parse_accepts_partial_names() {
        let (tli, segno) =
            SegnoFromRemovableXLogFileName("000000010000000000000007", SEG).unwrap();
        assert_eq!((tli, segno), (1, 7));
        let (tli, segno) =
            SegnoFromRemovableXLogFileName("000000030000000000000002.partial", SEG).unwrap();
        assert_eq!((tli, segno), (3, 2));
        assert!(SegnoFromRemovableXLogFileName("xlogtemp.1", SEG).is_err());
    }

    #[test]
    fn distance_estimate_bumps_fast_decays_slow() {
        let est = UpdateCheckPointDistanceEstimateCore(0.0, 1000);
        assert_eq!(est, 1000.0);
        let est = UpdateCheckPointDistanceEstimateCore(1000.0, 500);
        assert_eq!(est, 0.90 * 1000.0 + 0.10 * 500.0);
    }
}
