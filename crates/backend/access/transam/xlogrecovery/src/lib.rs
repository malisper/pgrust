//! xlogrecovery.c (PostgreSQL 18.3): the startup process' WAL-recovery
//! driver. Ported here: InitWalRecovery's checkpoint read+validate through
//! the startup page reader, the PerformWalRecovery redo loop (rmgr dispatch
//! + redo-context bookkeeping), FinishWalRecovery (EndOfLog + last partial
//! page), ShutdownWalRecovery, and the shared replay-position getters.
//! Archive recovery, standby mode, recovery targets, backup_label, timeline
//! switches and the consistency/pause machinery are loud panics.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};

use elog::{elog, ereport};
use types_core::{TimeLineID, TransactionId, XLogRecPtr, XLogSegNo};
use types_error::{ErrorLevel, ErrorLocation, PgError, PgResult, DEBUG1, FATAL, LOG, PANIC};
use xlogreader::{
    XLogReaderRoutine, XLogReaderState, XLogSegmentRoutine, XLREAD_FAIL, XLREAD_SUCCESS,
};
use xlogreader_seams::{XLogReaderState as ReaderView, XLOG_BLCKSZ};
use xlogrecovery_seams::{EndOfWalRecoveryInfo, InitWalRecoveryResult};

#[cfg(test)]
mod tests;

const InvalidXLogRecPtr: XLogRecPtr = 0;
const RECOVERY_COMMAND_FILE: &str = "recovery.conf";
const RECOVERY_COMMAND_DONE: &str = "recovery.done";
const STANDBY_SIGNAL_FILE: &str = "standby.signal";
const RECOVERY_SIGNAL_FILE: &str = "recovery.signal";
const BACKUP_LABEL_FILE: &str = "backup_label";
const TABLESPACE_MAP: &str = "tablespace_map";
const TABLESPACE_MAP_OLD: &str = "tablespace_map.old";
const PROMOTE_SIGNAL_FILE: &str = "promote";
const XLOGDIR: &str = "pg_wal";

// SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint).
const CHECKPOINT_REC_TOT_LEN: u32 =
    (xlogreader::SIZE_OF_XLOG_RECORD + 2 + controldata_utils::SIZEOF_CHECKPOINT) as u32;

fn loc(func: &'static str) -> ErrorLocation {
    ErrorLocation::new("xlogrecovery.c", 0, func)
}

fn data_path(rel: &str) -> String {
    let dir = init_small::globals::DataDir().unwrap_or(".");
    format!("{dir}/{rel}")
}

fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

// The XLogRecoveryCtlData fields live consumers reach (single address space:
// plain atomics stand in for the spinlocked shmem struct).
static RECOVERY_TARGET_TLI: AtomicU32 = AtomicU32::new(0);
static ARCHIVE_RECOVERY_REQUESTED: AtomicBool = AtomicBool::new(false);
static IN_ARCHIVE_RECOVERY: AtomicBool = AtomicBool::new(false);
static REACHED_CONSISTENCY: AtomicBool = AtomicBool::new(false);
static PROMOTE_IS_TRIGGERED: AtomicBool = AtomicBool::new(false);
static LAST_REPLAYED_READ_REC_PTR: AtomicU64 = AtomicU64::new(0);
static LAST_REPLAYED_END_REC_PTR: AtomicU64 = AtomicU64::new(0);
static LAST_REPLAYED_TLI: AtomicU32 = AtomicU32::new(0);
static REPLAY_END_REC_PTR: AtomicU64 = AtomicU64::new(0);
static REPLAY_END_TLI: AtomicU32 = AtomicU32::new(0);

thread_local! {
    static DO_REQUEST_WALRCV_REPLY: Cell<bool> = const { Cell::new(false) };
    static RECOVERY: RefCell<Option<Recovery>> = const { RefCell::new(None) };
}

pub fn GetXLogReplayRecPtr() -> (XLogRecPtr, TimeLineID) {
    (
        LAST_REPLAYED_END_REC_PTR.load(Relaxed),
        LAST_REPLAYED_TLI.load(Relaxed),
    )
}

pub fn GetCurrentReplayRecPtr() -> (XLogRecPtr, TimeLineID) {
    (REPLAY_END_REC_PTR.load(Relaxed), REPLAY_END_TLI.load(Relaxed))
}

pub fn PromoteIsTriggered() -> bool {
    PROMOTE_IS_TRIGGERED.load(Relaxed)
}

pub fn RemovePromoteSignalFiles() {
    let _ = std::fs::remove_file(data_path(PROMOTE_SIGNAL_FILE));
}

pub fn XLogRequestWalReceiverReply() {
    DO_REQUEST_WALRCV_REPLY.set(true);
}

#[derive(Clone, Copy, PartialEq)]
enum XLogSource {
    Any,
    PgWal,
}

#[derive(Clone, Copy)]
struct Tle {
    tli: TimeLineID,
    begin: XLogRecPtr,
    end: XLogRecPtr,
}

// readTimeLineHistory (timeline.c), reduced: a real history file means a
// timeline switch happened — the timeline unit owns that; an absent file
// (the fresh-cluster case) yields C's dummy single-entry history.
fn read_timeline_history(target_tli: TimeLineID) -> Vec<Tle> {
    let path = data_path(&format!("{XLOGDIR}/{target_tli:08X}.history"));
    if target_tli != 1 && std::path::Path::new(&path).exists() {
        panic!("timeline history file parsing not ported (timeline.c): {path}");
    }
    vec![Tle {
        tli: target_tli,
        begin: InvalidXLogRecPtr,
        end: InvalidXLogRecPtr,
    }]
}

fn tli_in_history(tli: TimeLineID, tles: &[Tle]) -> bool {
    tles.is_empty() || tles.iter().any(|t| t.tli == tli)
}

fn tli_of_point_in_history(ptr: XLogRecPtr, tles: &[Tle]) -> PgResult<TimeLineID> {
    for t in tles {
        if t.begin <= ptr && (t.end == InvalidXLogRecPtr || ptr < t.end) {
            return Ok(t.tli);
        }
    }
    ereport(types_error::ERROR)
        .errmsg(format!("timeline of point {} not found in history", lsn_fmt(ptr)))
        .finish(loc("tliOfPointInHistory"))?;
    unreachable!()
}

// The file-static read state of xlogrecovery.c plus the XLogPageReadPrivate
// parameters; this is the startup reader's XLogReaderRoutine.
struct PageSource {
    read_file: i32,
    read_seg_no: XLogSegNo,
    read_off: u32,
    read_source: XLogSource,
    cur_file_tli: TimeLineID,
    last_source_failed: bool,
    expected_tles: Vec<Tle>,
    emode: ErrorLevel,
    fetching_ckpt: bool,
    rand_access: bool,
    replay_tli: TimeLineID,
    last_complaint: XLogRecPtr,
}

impl PageSource {
    fn new() -> Self {
        PageSource {
            read_file: -1,
            read_seg_no: 0,
            read_off: 0,
            read_source: XLogSource::Any,
            cur_file_tli: 0,
            last_source_failed: false,
            expected_tles: Vec::new(),
            emode: LOG,
            fetching_ckpt: false,
            rand_access: false,
            replay_tli: 0,
            last_complaint: InvalidXLogRecPtr,
        }
    }

    fn close_read_file(&mut self) {
        if self.read_file >= 0 {
            // SAFETY: read_file is an fd this module opened.
            unsafe { libc::close(self.read_file) };
            self.read_file = -1;
        }
    }

    fn emode_for_corrupt_record(&mut self, emode: ErrorLevel, rec_ptr: XLogRecPtr) -> ErrorLevel {
        if self.read_source == XLogSource::PgWal && emode == LOG {
            if rec_ptr == self.last_complaint {
                return DEBUG1;
            }
            self.last_complaint = rec_ptr;
        }
        emode
    }

    fn report(&mut self, emode: ErrorLevel, rec_ptr: XLogRecPtr, msg: String) -> PgResult<()> {
        let emode = self.emode_for_corrupt_record(emode, rec_ptr);
        if emode == PANIC || emode == FATAL {
            return Err(Box::new(PgError::new(emode, msg)));
        }
        let _ = elog(emode, msg);
        Ok(())
    }

    fn xlog_file_read(
        &mut self,
        segno: XLogSegNo,
        tli: TimeLineID,
        notfound_ok: bool,
    ) -> PgResult<i32> {
        let wal_segsz = transam_xlog::wal_segment_size();
        let fname = transam_xlog::XLogFileName(tli, segno, wal_segsz);
        let path = data_path(&format!("{XLOGDIR}/{fname}"));
        let cpath = std::ffi::CString::new(path.clone()).unwrap();
        // SAFETY: NUL-terminated path; O_RDONLY open.
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };
        if fd >= 0 {
            self.cur_file_tli = tli;
            if ps_status_seams::set_ps_display::is_installed() {
                ps_status_seams::set_ps_display::call(&format!("recovering {fname}"));
            }
            self.read_source = XLogSource::PgWal;
            return Ok(fd);
        }
        let errno = std::io::Error::last_os_error();
        if errno.raw_os_error() != Some(libc::ENOENT) || !notfound_ok {
            ereport(PANIC)
                .errmsg(format!("could not open file \"{path}\": {errno}"))
                .finish(loc("XLogFileRead"))?;
        }
        Ok(-1)
    }

    fn xlog_file_read_any_tli(&mut self, segno: XLogSegNo) -> PgResult<i32> {
        let wal_segsz = transam_xlog::wal_segment_size();
        let tles = if self.expected_tles.is_empty() {
            read_timeline_history(RECOVERY_TARGET_TLI.load(Relaxed))
        } else {
            std::mem::take(&mut self.expected_tles)
        };
        let mut found = -1;
        for hent in &tles {
            if hent.tli < self.cur_file_tli {
                break;
            }
            if hent.begin != InvalidXLogRecPtr {
                let beginseg = transam_xlog::XLByteToSeg(hent.begin, wal_segsz);
                if segno < beginseg {
                    continue;
                }
            }
            let fd = self.xlog_file_read(segno, hent.tli, true)?;
            if fd != -1 {
                found = fd;
                break;
            }
        }
        self.expected_tles = tles;
        Ok(found)
    }

    // WaitForWALToBecomeAvailable, reduced to the reachable arms: no archive
    // recovery, no standby (both panic in InitWalRecovery), so the state
    // machine collapses to a single pg_wal probe.
    fn wait_for_wal(&mut self) -> PgResult<i32> {
        debug_assert!(!IN_ARCHIVE_RECOVERY.load(Relaxed));
        if self.last_source_failed {
            return Ok(XLREAD_FAIL);
        }
        self.close_read_file();
        if self.rand_access {
            self.cur_file_tli = 0;
        }
        self.read_file = self.xlog_file_read_any_tli(self.read_seg_no)?;
        if self.read_file >= 0 {
            Ok(XLREAD_SUCCESS)
        } else {
            self.last_source_failed = true;
            Ok(XLREAD_FAIL)
        }
    }
}

impl XLogSegmentRoutine for PageSource {
    fn segment_open(
        &mut self,
        _v: &mut ReaderView,
        _segno: XLogSegNo,
        _tli: &mut TimeLineID,
    ) -> PgResult<()> {
        unreachable!("startup reader has no segment_open (files opened in page_read)");
    }
    fn segment_close(&mut self, v: &mut ReaderView) {
        if v.seg.ws_file >= 0 {
            // SAFETY: fd owned by the reader's segment slot.
            unsafe { libc::close(v.seg.ws_file) };
            v.seg.ws_file = -1;
        }
    }
}

impl XLogReaderRoutine for PageSource {
    // XLogPageRead (xlogrecovery.c), non-standby: one pass, no retry loop.
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        _target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        let wal_segsz = v.segcxt.ws_segsize;
        let target_page_off = transam_xlog::XLogSegmentOffset(target_page_ptr, wal_segsz);

        if self.read_file >= 0
            && transam_xlog::XLByteToSeg(target_page_ptr, wal_segsz) != self.read_seg_no
        {
            // The restartpoint request here needs archive recovery (unreachable).
            self.close_read_file();
            self.read_source = XLogSource::Any;
        }
        self.read_seg_no = transam_xlog::XLByteToSeg(target_page_ptr, wal_segsz);

        if self.read_file < 0 {
            if self.wait_for_wal()? != XLREAD_SUCCESS {
                self.close_read_file();
                self.read_source = XLogSource::Any;
                return Ok(XLREAD_FAIL);
            }
        }
        debug_assert!(self.read_file >= 0);

        self.read_off = target_page_off;
        // SAFETY: cur_page is the reader's XLOG_BLCKSZ read buffer.
        let r = unsafe {
            libc::pread(
                self.read_file,
                cur_page.as_mut_ptr() as *mut libc::c_void,
                XLOG_BLCKSZ,
                self.read_off as libc::off_t,
            )
        };
        if r != XLOG_BLCKSZ as isize {
            let errno = std::io::Error::last_os_error();
            let fname = transam_xlog::XLogFileName(self.cur_file_tli, self.read_seg_no, wal_segsz);
            let emode = self.emode;
            let msg = if r < 0 {
                format!(
                    "could not read from WAL segment {fname}, LSN {}, offset {}: {errno}",
                    lsn_fmt(target_page_ptr),
                    self.read_off
                )
            } else {
                format!(
                    "could not read from WAL segment {fname}, LSN {}, offset {}: read {r} of {XLOG_BLCKSZ}",
                    lsn_fmt(target_page_ptr),
                    self.read_off
                )
            };
            self.report(emode, target_page_ptr + req_len as u64, msg)?;
            self.last_source_failed = true;
            self.close_read_file();
            self.read_source = XLogSource::Any;
            return Ok(XLREAD_FAIL);
        }

        v.seg.ws_tli = self.cur_file_tli;
        Ok(XLOG_BLCKSZ as i32)
    }
}

struct Recovery {
    context: &'static mcx::MemoryContext,
    reader: XLogReaderState<'static>,
    src: PageSource,
    check_point_loc: XLogRecPtr,
    check_point_tli: TimeLineID,
    redo_start_lsn: XLogRecPtr,
    redo_start_tli: TimeLineID,
    aborted_rec_ptr: XLogRecPtr,
    missing_contrec_ptr: XLogRecPtr,
    oldest_active_xid: TransactionId,
}

// ReadRecord: Ok(true) = the reader's current record is the requested one.
fn read_record(
    rec: &mut Recovery,
    emode: ErrorLevel,
    fetching_ckpt: bool,
    replay_tli: TimeLineID,
) -> PgResult<bool> {
    rec.src.emode = emode;
    rec.src.fetching_ckpt = fetching_ckpt;
    rec.src.rand_access = rec.reader.v.ReadRecPtr == InvalidXLogRecPtr;
    rec.src.replay_tli = replay_tli;
    rec.src.last_source_failed = false;

    let got = rec.reader.XLogReadRecord(&mut rec.src)?;
    let mut have_record = got.is_some();
    match got {
        None => {
            if !ARCHIVE_RECOVERY_REQUESTED.load(Relaxed)
                && rec.reader.abortedRecPtr != InvalidXLogRecPtr
            {
                rec.aborted_rec_ptr = rec.reader.abortedRecPtr;
                rec.missing_contrec_ptr = rec.reader.missingContrecPtr;
            }
            rec.src.close_read_file();
            if let Some(msg) = rec.reader.errormsg() {
                let msg = msg.to_string();
                let end = rec.reader.v.EndRecPtr;
                rec.src.report(emode, end, msg)?;
            }
        }
        Some(_) => {
            let (latest_page_ptr, latest_page_tli) = rec.reader.latest_page();
            if !tli_in_history(latest_page_tli, &rec.src.expected_tles) {
                let wal_segsz = transam_xlog::wal_segment_size();
                let segno = transam_xlog::XLByteToSeg(latest_page_ptr, wal_segsz);
                let offset = transam_xlog::XLogSegmentOffset(latest_page_ptr, wal_segsz);
                let fname = transam_xlog::XLogFileName(rec.reader.v.seg.ws_tli, segno, wal_segsz);
                let end = rec.reader.v.EndRecPtr;
                rec.src.report(
                    emode,
                    end,
                    format!(
                        "unexpected timeline ID {latest_page_tli} in WAL segment {fname}, LSN {}, offset {offset}",
                        lsn_fmt(latest_page_ptr)
                    ),
                )?;
                have_record = false;
            }
        }
    }
    if have_record {
        return Ok(true);
    }
    rec.src.last_source_failed = true;
    // The crash-to-archive switch and the standby retry loop are unreachable
    // (ArchiveRecoveryRequested/StandbyMode panic in InitWalRecovery).
    Ok(false)
}

fn read_checkpoint_record(
    rec: &mut Recovery,
    rec_ptr: XLogRecPtr,
    replay_tli: TimeLineID,
) -> PgResult<bool> {
    if !transam_xlog::XRecOffIsValid(rec_ptr) {
        let _ = elog(LOG, "invalid checkpoint location".to_string());
        return Ok(false);
    }
    rec.reader.XLogBeginRead(rec_ptr);
    if !read_record(rec, LOG, true, replay_tli)? {
        let _ = elog(LOG, "invalid checkpoint record".to_string());
        return Ok(false);
    }
    if rec.reader.XLogRecGetRmid() != transam_xlog::RM_XLOG_ID {
        let _ = elog(LOG, "invalid resource manager ID in checkpoint record".to_string());
        return Ok(false);
    }
    let info = rec.reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK;
    if info != transam_xlog::XLOG_CHECKPOINT_SHUTDOWN
        && info != transam_xlog::XLOG_CHECKPOINT_ONLINE
    {
        let _ = elog(LOG, "invalid xl_info in checkpoint record".to_string());
        return Ok(false);
    }
    if rec.reader.XLogRecGetTotalLen() != CHECKPOINT_REC_TOT_LEN {
        let _ = elog(LOG, "invalid length of checkpoint record".to_string());
        return Ok(false);
    }
    Ok(true)
}

fn read_recovery_signal_file() -> PgResult<()> {
    if miscinit::IsBootstrapProcessingMode() {
        return Ok(());
    }
    if std::path::Path::new(&data_path(RECOVERY_COMMAND_FILE)).exists() {
        return ereport(FATAL)
            .errmsg(format!(
                "using recovery command file \"{RECOVERY_COMMAND_FILE}\" is not supported"
            ))
            .finish(loc("readRecoverySignalFile"));
    }
    let _ = std::fs::remove_file(data_path(RECOVERY_COMMAND_DONE));

    // Standby/archive recovery legs (EnableStandbyMode, target validation,
    // restore_command) are unported: a present signal file is a loud stop.
    for sig in [STANDBY_SIGNAL_FILE, RECOVERY_SIGNAL_FILE] {
        if std::path::Path::new(&data_path(sig)).exists() {
            panic!("standby/archive recovery not ported (xlogrecovery.c): \"{sig}\" present");
        }
    }
    Ok(())
}

pub fn InitWalRecovery() -> PgResult<InitWalRecoveryResult> {
    let cf = *transam_xlog::control_file::control_file();
    let mut in_recovery = false;

    let target_tli = if cf.minRecoveryPointTLI > cf.checkPointCopy.ThisTimeLineID {
        cf.minRecoveryPointTLI
    } else {
        cf.checkPointCopy.ThisTimeLineID
    };
    RECOVERY_TARGET_TLI.store(target_tli, Relaxed);

    read_recovery_signal_file()?;
    // validateRecoveryParameters: archive-recovery-only; unreachable here.

    let context: &'static mcx::MemoryContext =
        Box::leak(Box::new(mcx::MemoryContext::new("wal recovery")));
    let mut reader = XLogReaderState::allocate(context.mcx(), transam_xlog::wal_segment_size())?;
    reader.system_identifier = cf.system_identifier;
    reader.XLogReaderSetDecodeBuffer(guc_tables::vars::wal_decode_buffer_size.read() as usize);
    // No XLogPrefetcher: the prefetcher unit is unported; reads fall back to
    // XLogReadRecord exactly as C's prefetcher does on a cold queue.

    if std::path::Path::new(&data_path(BACKUP_LABEL_FILE)).exists() {
        panic!(
            "backup_label recovery not ported (xlogrecovery.c): \"{BACKUP_LABEL_FILE}\" present"
        );
    }
    if std::path::Path::new(&data_path(TABLESPACE_MAP)).exists() {
        let _ = std::fs::remove_file(data_path(TABLESPACE_MAP_OLD));
        let renamed = fd::durable_rename(
            &data_path(TABLESPACE_MAP),
            &data_path(TABLESPACE_MAP_OLD),
            DEBUG1,
        );
        let detail = match renamed {
            Ok(0) => format!("File \"{TABLESPACE_MAP}\" was renamed to \"{TABLESPACE_MAP_OLD}\"."),
            _ => format!("Could not rename file \"{TABLESPACE_MAP}\" to \"{TABLESPACE_MAP_OLD}\"."),
        };
        let _ = elog(
            LOG,
            format!(
                "ignoring file \"{TABLESPACE_MAP}\" because no file \"{BACKUP_LABEL_FILE}\" exists: {detail}"
            ),
        );
    }

    let mut rec = Recovery {
        context,
        reader,
        src: PageSource::new(),
        check_point_loc: cf.checkPoint,
        check_point_tli: cf.checkPointCopy.ThisTimeLineID,
        redo_start_lsn: cf.checkPointCopy.redo,
        redo_start_tli: cf.checkPointCopy.ThisTimeLineID,
        aborted_rec_ptr: InvalidXLogRecPtr,
        missing_contrec_ptr: InvalidXLogRecPtr,
        oldest_active_xid: types_core::InvalidTransactionId,
    };

    let (cp_loc, cp_tli) = (rec.check_point_loc, rec.check_point_tli);
    if !read_checkpoint_record(&mut rec, cp_loc, cp_tli)? {
        ereport(PANIC)
            .errmsg(format!(
                "could not locate a valid checkpoint record at {}",
                lsn_fmt(rec.check_point_loc)
            ))
            .finish(loc("InitWalRecovery"))?;
    }
    let check_point = controldata_utils::CheckPoint::from_bytes(rec.reader.XLogRecGetData());
    let was_shutdown = (rec.reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK)
        == transam_xlog::XLOG_CHECKPOINT_SHUTDOWN;
    rec.oldest_active_xid = check_point.oldestActiveXid;
    rec.redo_start_lsn = check_point.redo;
    rec.redo_start_tli = check_point.ThisTimeLineID;

    if check_point.redo < rec.check_point_loc {
        rec.reader.XLogBeginRead(check_point.redo);
        if !read_record(&mut rec, LOG, false, check_point.ThisTimeLineID)? {
            ereport(PANIC)
                .errmsg(format!(
                    "could not find redo location {} referenced by checkpoint record at {}",
                    lsn_fmt(check_point.redo),
                    lsn_fmt(rec.check_point_loc)
                ))
                .finish(loc("InitWalRecovery"))?;
        }
    }

    debug_assert!(!rec.src.expected_tles.is_empty());
    if tli_of_point_in_history(rec.check_point_loc, &rec.src.expected_tles)?
        != rec.check_point_tli
    {
        ereport(FATAL)
            .errmsg(format!(
                "requested timeline {target_tli} is not a child of this server's history"
            ))
            .finish(loc("InitWalRecovery"))?;
    }
    if cf.minRecoveryPoint != InvalidXLogRecPtr
        && tli_of_point_in_history(cf.minRecoveryPoint - 1, &rec.src.expected_tles)?
            != cf.minRecoveryPointTLI
    {
        ereport(FATAL)
            .errmsg(format!(
                "requested timeline {target_tli} does not contain minimum recovery point {} on timeline {}",
                lsn_fmt(cf.minRecoveryPoint),
                cf.minRecoveryPointTLI
            ))
            .finish(loc("InitWalRecovery"))?;
    }

    if (check_point.nextXid.value as u32) < types_core::FirstNormalTransactionId {
        ereport(PANIC)
            .errmsg("invalid next transaction ID")
            .finish(loc("InitWalRecovery"))?;
    }
    if check_point.redo > rec.check_point_loc {
        ereport(PANIC)
            .errmsg("invalid redo in checkpoint record")
            .finish(loc("InitWalRecovery"))?;
    }
    if check_point.redo < rec.check_point_loc {
        if was_shutdown {
            ereport(PANIC)
                .errmsg("invalid redo record in shutdown checkpoint")
                .finish(loc("InitWalRecovery"))?;
        }
        in_recovery = true;
    } else if cf.state != transam_xlog::DB_SHUTDOWNED {
        in_recovery = true;
    }

    if in_recovery {
        let _ = elog(
            LOG,
            "database system was not properly shut down; automatic recovery in progress"
                .to_string(),
        );
        if target_tli > cf.checkPointCopy.ThisTimeLineID {
            let _ = elog(
                LOG,
                format!(
                    "crash recovery starts in timeline {} and has target timeline {target_tli}",
                    cf.checkPointCopy.ThisTimeLineID
                ),
            );
        }
        let cp_loc = rec.check_point_loc;
        transam_xlog::control_file::control_file_update(|c| {
            c.state = transam_xlog::DB_IN_CRASH_RECOVERY;
            c.checkPoint = cp_loc;
            c.checkPointCopy = check_point;
        });
        xlogutils::set_in_recovery(true);
    }

    rec.aborted_rec_ptr = InvalidXLogRecPtr;
    rec.missing_contrec_ptr = InvalidXLogRecPtr;
    RECOVERY.with(|r| *r.borrow_mut() = Some(rec));

    Ok(InitWalRecoveryResult {
        was_shutdown,
        have_backup_label: false,
        have_tblspc_map: false,
    })
}

// CheckRecoveryConsistency: in crash recovery minRecoveryPoint stays invalid,
// so everything past the early return is archive-recovery-only.
fn check_recovery_consistency() -> PgResult<()> {
    if transam_xlog::control_file::control_file().minRecoveryPoint == InvalidXLogRecPtr {
        return Ok(());
    }
    panic!(
        "CheckRecoveryConsistency archive-recovery arms not ported \
         (minRecoveryPoint is set during crash recovery)"
    );
}

// The XLOG-rmgr record types handled by the recovery driver itself.
fn xlogrecovery_redo(rec: &Recovery) -> PgResult<()> {
    let record = &rec.reader.v;
    let info = rec.reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK;
    debug_assert_eq!(rec.reader.XLogRecGetRmid(), transam_xlog::RM_XLOG_ID);

    if info == transam_xlog::XLOG_OVERWRITE_CONTRECORD {
        panic!(
            "XLOG_OVERWRITE_CONTRECORD verification not ported \
             (torn-record overwrite; lands with CreateOverwriteContrecordRecord)"
        );
    } else if info == transam_xlog::XLOG_BACKUP_END {
        // backupStartPoint is nonzero only under backup_label recovery
        // (panics at init); C's mismatch arm is a DEBUG2, elided.
        let data = record.record.as_ref().expect("no decoded record");
        // SAFETY: main_data points into the reader's decode buffer.
        let startpoint =
            u64::from_ne_bytes(unsafe { data.main_data_bytes() }[..8].try_into().unwrap());
        if startpoint == transam_xlog::control_file::control_file().backupStartPoint
            && startpoint != InvalidXLogRecPtr
        {
            panic!("XLOG_BACKUP_END during an online backup: backup_label recovery not ported");
        }
    }
    Ok(())
}

const XLR_CHECK_CONSISTENCY: u8 = 0x02;

fn apply_wal_record(rec: &mut Recovery, replay_tli: &mut TimeLineID) -> PgResult<()> {
    let xid = rec.reader.XLogRecGetXid();
    let rmid = rec.reader.XLogRecGetRmid();
    let info = rec.reader.XLogRecGetInfo();

    varsup::AdvanceNextFullTransactionIdPastXid(xid)?;

    if rmid == transam_xlog::RM_XLOG_ID {
        let stripped = info & !transam_xlog::XLR_INFO_MASK;
        let mut new_replay_tli = *replay_tli;
        if stripped == transam_xlog::XLOG_CHECKPOINT_SHUTDOWN {
            let cp = controldata_utils::CheckPoint::from_bytes(rec.reader.XLogRecGetData());
            new_replay_tli = cp.ThisTimeLineID;
        } else if stripped == transam_xlog::XLOG_END_OF_RECOVERY {
            let data = rec.reader.XLogRecGetData();
            new_replay_tli = u32::from_ne_bytes(data[8..12].try_into().unwrap());
        }
        if new_replay_tli != *replay_tli {
            panic!(
                "timeline switch at {} not ported (checkTimeLineSwitch; timeline unit)",
                lsn_fmt(rec.reader.v.EndRecPtr)
            );
        }
    }

    REPLAY_END_REC_PTR.store(rec.reader.v.EndRecPtr, Relaxed);
    REPLAY_END_TLI.store(*replay_tli, Relaxed);

    debug_assert!(xlogutils::standby_state() == xlogutils::STANDBY_DISABLED);

    if rmid == transam_xlog::RM_XLOG_ID {
        xlogrecovery_redo(rec)?;
    }

    (rmgr::GetRmgr(rmid)?.rm_redo)(&mut rec.reader.v)?;

    if info & XLR_CHECK_CONSISTENCY != 0 {
        panic!("verifyBackupPageConsistency not ported (wal_consistency_checking record seen)");
    }

    LAST_REPLAYED_READ_REC_PTR.store(rec.reader.v.ReadRecPtr, Relaxed);
    LAST_REPLAYED_END_REC_PTR.store(rec.reader.v.EndRecPtr, Relaxed);
    LAST_REPLAYED_TLI.store(*replay_tli, Relaxed);

    // WalSndWakeup: cascade replication only (standby-mode, unreachable).
    if DO_REQUEST_WALRCV_REPLY.get() {
        panic!("WalRcvForceReply not ported (walreceiver): apply-feedback commit replayed");
    }

    check_recovery_consistency()
}

pub fn PerformWalRecovery() -> PgResult<()> {
    let mut rec = RECOVERY
        .with(|cell| cell.borrow_mut().take())
        .expect("PerformWalRecovery before InitWalRecovery");
    let result = perform_wal_recovery_guts(&mut rec);
    RECOVERY.with(|cell| *cell.borrow_mut() = Some(rec));
    result
}

fn perform_wal_recovery_guts(rec: &mut Recovery) -> PgResult<()> {
    if rec.redo_start_lsn < rec.check_point_loc {
        LAST_REPLAYED_READ_REC_PTR.store(InvalidXLogRecPtr, Relaxed);
        LAST_REPLAYED_END_REC_PTR.store(rec.redo_start_lsn, Relaxed);
        LAST_REPLAYED_TLI.store(rec.redo_start_tli, Relaxed);
    } else {
        LAST_REPLAYED_READ_REC_PTR.store(rec.reader.v.ReadRecPtr, Relaxed);
        LAST_REPLAYED_END_REC_PTR.store(rec.reader.v.EndRecPtr, Relaxed);
        LAST_REPLAYED_TLI.store(rec.check_point_tli, Relaxed);
    }
    REPLAY_END_REC_PTR.store(LAST_REPLAYED_END_REC_PTR.load(Relaxed), Relaxed);
    REPLAY_END_TLI.store(LAST_REPLAYED_TLI.load(Relaxed), Relaxed);

    if init_small::globals::IsUnderPostmaster() {
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_RECOVERY_STARTED);
    }

    check_recovery_consistency()?;

    let mut replay_tli;
    let mut have_record;
    if rec.redo_start_lsn < rec.check_point_loc {
        replay_tli = rec.redo_start_tli;
        let redo_start = rec.redo_start_lsn;
        rec.reader.XLogBeginRead(redo_start);
        have_record = read_record(rec, PANIC, false, replay_tli)?;
        debug_assert!(have_record);
        if rec.reader.XLogRecGetRmid() != transam_xlog::RM_XLOG_ID
            || rec.reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK
                != transam_xlog::XLOG_CHECKPOINT_REDO
        {
            ereport(FATAL)
                .errmsg(format!(
                    "unexpected record type found at redo point {}",
                    lsn_fmt(rec.reader.v.ReadRecPtr)
                ))
                .finish(loc("PerformWalRecovery"))?;
        }
    } else {
        debug_assert_eq!(rec.reader.v.ReadRecPtr, rec.check_point_loc);
        replay_tli = rec.check_point_tli;
        have_record = read_record(rec, LOG, false, replay_tli)?;
    }

    if have_record {
        rmgr::RmgrStartup(rec.context.mcx())?;
        let _ = elog(
            LOG,
            format!("redo starts at {}", lsn_fmt(rec.reader.v.ReadRecPtr)),
        );

        while have_record {
            if startup_seams::process_startup_proc_interrupts::is_installed() {
                startup_seams::process_startup_proc_interrupts::call()?;
            }
            // recoveryStopsBefore/After + pause/delay arms: recovery targets
            // and hot standby are archive-only (panics in InitWalRecovery).
            apply_wal_record(rec, &mut replay_tli)?;
            have_record = read_record(rec, LOG, false, replay_tli)?;
        }

        rmgr::RmgrCleanup();
        let _ = elog(
            LOG,
            format!("redo done at {}", lsn_fmt(rec.reader.v.ReadRecPtr)),
        );
    } else {
        let _ = elog(LOG, "redo is not required".to_string());
    }
    Ok(())
}

pub fn FinishWalRecovery() -> PgResult<EndOfWalRecoveryInfo> {
    RECOVERY.with(|cell| {
        let mut guard = cell.borrow_mut();
        let rec = guard.as_mut().expect("FinishWalRecovery before InitWalRecovery");

        // XLogShutdownWalRcv / ShutDownSlotSync: walreceiver and slot sync
        // never start here (standby mode panics at init).

        let (last_rec, last_rec_tli) = if !xlogutils::in_recovery() {
            (rec.check_point_loc, rec.check_point_tli)
        } else {
            (
                LAST_REPLAYED_READ_REC_PTR.load(Relaxed),
                LAST_REPLAYED_TLI.load(Relaxed),
            )
        };
        rec.reader.XLogBeginRead(last_rec);
        if !read_record(rec, PANIC, false, last_rec_tli)? {
            ereport(PANIC)
                .errmsg(format!("could not re-read record at {}", lsn_fmt(last_rec)))
                .finish(loc("FinishWalRecovery"))?;
        }
        let end_of_log = rec.reader.v.EndRecPtr;
        let end_of_log_tli = rec.reader.v.seg.ws_tli;

        let (last_page_begin_ptr, last_page): (XLogRecPtr, Box<[u8]>) =
            if end_of_log % XLOG_BLCKSZ as u64 != 0 {
                let page_begin_ptr = end_of_log - (end_of_log % XLOG_BLCKSZ as u64);
                debug_assert_eq!(
                    rec.src.read_off,
                    transam_xlog::XLogSegmentOffset(
                        page_begin_ptr,
                        transam_xlog::wal_segment_size()
                    )
                );
                let len = (end_of_log % XLOG_BLCKSZ as u64) as usize;
                (page_begin_ptr, rec.reader.read_buf()[..len].into())
            } else {
                (end_of_log, Box::default())
            };

        Ok(EndOfWalRecoveryInfo {
            lastRec: last_rec,
            lastRecTLI: last_rec_tli,
            endOfLog: end_of_log,
            endOfLogTLI: end_of_log_tli,
            lastPageBeginPtr: last_page_begin_ptr,
            lastPage: last_page,
            abortedRecPtr: rec.aborted_rec_ptr,
            missingContrecPtr: rec.missing_contrec_ptr,
            recovery_signal_file_found: false,
            standby_signal_file_found: false,
        })
    })
}

pub fn ShutdownWalRecovery() -> PgResult<()> {
    RECOVERY.with(|cell| {
        if let Some(mut rec) = cell.borrow_mut().take() {
            rec.src.close_read_file();
        }
    });
    // The reader's leaked "wal recovery" context stays allocated: a one-shot
    // boot-only arena (C frees it; a few KB once per process here).
    Ok(())
}

fn recovery_oldest_active_xid() -> TransactionId {
    RECOVERY.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|r| r.oldest_active_xid)
            .unwrap_or(types_core::InvalidTransactionId)
    })
}

pub fn init_seams() {
    use xlogrecovery_seams as s;
    s::reached_consistency::set(|| REACHED_CONSISTENCY.load(Relaxed));
    s::get_xlog_replay_rec_ptr::set(GetXLogReplayRecPtr);
    s::xlog_request_wal_receiver_reply::set(XLogRequestWalReceiverReply);
    s::init_wal_recovery::set(InitWalRecovery);
    s::perform_wal_recovery::set(PerformWalRecovery);
    s::finish_wal_recovery::set(FinishWalRecovery);
    s::shutdown_wal_recovery::set(ShutdownWalRecovery);
    s::archive_recovery_requested::set(|| ARCHIVE_RECOVERY_REQUESTED.load(Relaxed));
    s::in_archive_recovery::set(|| IN_ARCHIVE_RECOVERY.load(Relaxed));
    s::recovery_target_tli::set(|| RECOVERY_TARGET_TLI.load(Relaxed));
    s::promote_is_triggered::set(PromoteIsTriggered);
    s::get_current_replay_rec_ptr::set(GetCurrentReplayRecPtr);
    s::recovery_oldest_active_xid::set(recovery_oldest_active_xid);
    s::remove_promote_signal_files::set(RemovePromoteSignalFiles);
}
