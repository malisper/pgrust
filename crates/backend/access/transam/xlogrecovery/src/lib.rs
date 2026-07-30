//! xlogrecovery.c (PostgreSQL 18.3): the startup process' WAL-recovery
//! driver. Crash recovery, archive recovery (restore_command) and standby
//! mode are ported: signal files, recovery targets (xid/time/name/lsn/
//! immediate), pause/promote, timeline rescans and the standby WAL-source
//! state machine. Streaming (walreceiver) arms route through
//! walreceiverfuncs_seams and behave as "no walreceiver" until that unit
//! installs them.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};

use elog::{elog, ereport};
use types_core::{TimeLineID, TimestampTz, TransactionId, XLogRecPtr, XLogSegNo};
use types_error::{
    ErrorLevel, ErrorLocation, PgError, PgResult, DEBUG1, DEBUG2, ERROR, FATAL, LOG, PANIC,
    WARNING,
};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use xlogreader::{
    XLogReaderRoutine, XLogReaderState, XLogSegmentRoutine, XLREAD_FAIL, XLREAD_SUCCESS,
    XLREAD_WOULDBLOCK,
};
use xlogreader_seams::{XLogReaderState as ReaderView, XLOG_BLCKSZ};
use xlogrecovery_seams::{EndOfWalRecoveryInfo, InitWalRecoveryResult};

mod backup_label;
pub mod targets;

#[cfg(test)]
mod tests;

pub use targets::{
    CheckPromoteSignal, GetCurrentChunkReplayStartTime, GetLatestXTime, GetRecoveryPauseState,
    HotStandbyActive, PromoteIsTriggered, RecoveryRequiresIntParameter, SetRecoveryPause,
    WakeupRecovery,
};
use targets::{RecoveryTargetTimeLineGoal, RecoveryTargetType};

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
const PG_TBLSPC_DIR: &str = "pg_tblspc";

// SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint).
const CHECKPOINT_REC_TOT_LEN: u32 =
    (xlogreader::SIZE_OF_XLOG_RECORD + 2 + controldata_utils::SIZEOF_CHECKPOINT) as u32;

const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const PG_WAIT_TIMEOUT: u32 = 0x0900_0000;
const WAIT_EVENT_RECOVERY_WAL_STREAM: u32 = PG_WAIT_ACTIVITY + 10;
const WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL: u32 = PG_WAIT_TIMEOUT + 4;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
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
static STANDBY_MODE_REQUESTED: AtomicBool = AtomicBool::new(false);
static STANDBY_MODE: AtomicBool = AtomicBool::new(false);
static REACHED_CONSISTENCY: AtomicBool = AtomicBool::new(false);
pub(crate) static PROMOTE_IS_TRIGGERED: AtomicBool = AtomicBool::new(false);
static LAST_REPLAYED_READ_REC_PTR: AtomicU64 = AtomicU64::new(0);
static LAST_REPLAYED_END_REC_PTR: AtomicU64 = AtomicU64::new(0);
static LAST_REPLAYED_TLI: AtomicU32 = AtomicU32::new(0);
static REPLAY_END_REC_PTR: AtomicU64 = AtomicU64::new(0);
static REPLAY_END_TLI: AtomicU32 = AtomicU32::new(0);
static SIGNAL_FILE_STANDBY: AtomicBool = AtomicBool::new(false);
static SIGNAL_FILE_RECOVERY: AtomicBool = AtomicBool::new(false);

thread_local! {
    static DO_REQUEST_WALRCV_REPLY: Cell<bool> = const { Cell::new(false) };
    static RECOVERY: RefCell<Option<Recovery>> = const { RefCell::new(None) };
    // pendingWalRcvRestart + the walreceiver's started-with parameters
    // (xlogrecovery.c file statics). NOT fields of Recovery: PerformWalRecovery
    // takes the struct out of RECOVERY for the whole redo loop, so the SIGHUP
    // seam (ProcessStartupProcInterrupts -> StartupRereadWalRcvConfig ->
    // StartupRequestWalReceiverRestart) must reach them without it — all on
    // the startup thread.
    static PENDING_WALRCV_RESTART: Cell<bool> = const { Cell::new(false) };
    static WALRCV_STARTED_WITH: RefCell<Option<(String, String, bool)>> =
        const { RefCell::new(None) };
    // XLogReceiptTime/XLogReceiptSource (xlogrecovery.c file statics). NOT
    // PageSource fields: GetXLogReceiptTime is read by the startup ITSELF
    // mid-replay (ResolveRecoveryConflictWithBufferPin -> GetStandbyLimitTime)
    // while PerformWalRecovery has the Recovery struct taken out of RECOVERY —
    // a (0, false) fallback there put the standby-delay limit in the past and
    // every buffer-pin conflict cancelled instantly with the BUFFERPIN reason
    // (031's missing startup-deadlock detail).
    static RECEIPT_TIME: Cell<TimestampTz> = const { Cell::new(0) };
    static RECEIPT_SOURCE: Cell<XLogSource> = const { Cell::new(XLogSource::Any) };
}

pub fn ArchiveRecoveryRequested() -> bool {
    ARCHIVE_RECOVERY_REQUESTED.load(Relaxed)
}
pub fn InArchiveRecovery() -> bool {
    IN_ARCHIVE_RECOVERY.load(Relaxed)
}
pub fn StandbyModeRequested() -> bool {
    STANDBY_MODE_REQUESTED.load(Relaxed)
}
pub fn StandbyMode() -> bool {
    STANDBY_MODE.load(Relaxed)
}
pub(crate) fn reached_consistency() -> bool {
    REACHED_CONSISTENCY.load(Relaxed)
}

fn EnableStandbyMode() {
    STANDBY_MODE.store(true, Relaxed);
    if startup_seams::disable_startup_progress_timeout::is_installed() {
        startup_seams::disable_startup_progress_timeout::call();
    }
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

// stat(2)-succeeds existence probe over the fd-crate front (DST P1 inc-3).
fn recovery_file_exists(rel: &str) -> bool {
    let mut fi = fd::FileInfo::zeroed();
    fd::pg_stat(&data_path(rel), &mut fi) == 0
}

pub fn RemovePromoteSignalFiles() {
    let _ = fd::pg_unlink(&data_path(PROMOTE_SIGNAL_FILE));
}

pub fn XLogRequestWalReceiverReply() {
    DO_REQUEST_WALRCV_REPLY.set(true);
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum XLogSource {
    Any,
    Archive,
    PgWal,
    Stream,
}

fn xlog_source_name(s: XLogSource) -> &'static str {
    match s {
        XLogSource::Any => "any",
        XLogSource::Archive => "archive",
        XLogSource::PgWal => "pg_wal",
        XLogSource::Stream => "stream",
    }
}

use timeline_seams::TimeLineHistoryEntry as Tle;

// The scoped context is C's palloc'd history list (a few entries, boot-only);
// copied out because expectedTLEs outlives it.
fn read_timeline_history(target_tli: TimeLineID) -> PgResult<Vec<Tle>> {
    let history_cx = mcx::MemoryContext::new("timeline history");
    let tles = timeline_seams::read_timeline_history::call(history_cx.mcx(), target_tli)?;
    Ok(tles.iter().copied().collect())
}

fn tli_in_history(tli: TimeLineID, tles: &[Tle]) -> bool {
    tles.is_empty() || tles.iter().any(|t| t.tli == tli)
}

fn tli_of_point_in_history(ptr: XLogRecPtr, tles: &[Tle]) -> PgResult<TimeLineID> {
    timeline_seams::tli_of_point_in_history::call(ptr, tles)
}

fn wal_rcv_streaming() -> bool {
    walreceiverfuncs_seams::wal_rcv_streaming::is_installed()
        && walreceiverfuncs_seams::wal_rcv_streaming::call()
}

// XLogShutdownWalRcv (xlog.c): stop walreceiver + clear the install flag.
// The flag can only have been set in standby mode, so crash recovery (where
// tests run without XLOGShmemInit) skips the XLogCtl touch.
fn xlog_shutdown_wal_rcv() {
    if walreceiverfuncs_seams::shutdown_wal_rcv::is_installed() {
        walreceiverfuncs_seams::shutdown_wal_rcv::call();
    }
    if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed) {
        transam_xlog::ResetInstallXLogFileSegmentActive();
    }
}

// The file-static read state of xlogrecovery.c plus the XLogPageReadPrivate
// parameters; this is the startup reader's XLogReaderRoutine.
struct PageSource {
    read_file: i32,
    read_seg_no: XLogSegNo,
    read_off: u32,
    read_source: XLogSource,
    cur_source: XLogSource,
    cur_file_tli: TimeLineID,
    last_source_failed: bool,
    expected_tles: Vec<Tle>,
    emode: ErrorLevel,
    fetching_ckpt: bool,
    rand_access: bool,
    replay_tli: TimeLineID,
    last_complaint: XLogRecPtr,
    flushed_upto: XLogRecPtr,
    receive_tli: TimeLineID,
    last_fail_time: TimestampTz,
    // minRecoveryPoint & friends (file statics; consistency bookkeeping).
    min_recovery_point: XLogRecPtr,
    min_recovery_point_tli: TimeLineID,
    backup_start_point: XLogRecPtr,
    backup_end_point: XLogRecPtr,
    backup_end_required: bool,
    redo_start_lsn: XLogRecPtr,
    redo_start_tli: TimeLineID,
}

impl PageSource {
    fn new() -> Self {
        PageSource {
            read_file: -1,
            read_seg_no: 0,
            read_off: 0,
            read_source: XLogSource::Any,
            cur_source: XLogSource::Any,
            cur_file_tli: 0,
            last_source_failed: false,
            expected_tles: Vec::new(),
            emode: LOG,
            fetching_ckpt: false,
            rand_access: false,
            replay_tli: 0,
            last_complaint: InvalidXLogRecPtr,
            flushed_upto: InvalidXLogRecPtr,
            receive_tli: 0,
            last_fail_time: 0,
            min_recovery_point: InvalidXLogRecPtr,
            min_recovery_point_tli: 0,
            backup_start_point: InvalidXLogRecPtr,
            backup_end_point: InvalidXLogRecPtr,
            backup_end_required: false,
            redo_start_lsn: InvalidXLogRecPtr,
            redo_start_tli: 0,
        }
    }

    fn close_read_file(&mut self) {
        if self.read_file >= 0 {
            // read_file is an fd this module opened.
            fd::pg_close(self.read_file);
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
        source: XLogSource,
        notfound_ok: bool,
    ) -> PgResult<i32> {
        let wal_segsz = transam_xlog::wal_segment_size();
        let fname = transam_xlog::XLogFileName(tli, segno, wal_segsz);
        let path;
        match source {
            XLogSource::Archive => {
                if ps_status_seams::set_ps_display::is_installed() {
                    ps_status_seams::set_ps_display::call(&format!("waiting for {fname}"));
                }
                let Some(restored) = xlogarchive::RestoreArchivedFile(
                    &fname,
                    "RECOVERYXLOG",
                    wal_segsz as i64,
                    crate::in_redo(),
                )?
                else {
                    return Ok(-1);
                };
                xlogarchive::KeepFileRestoredFromArchive(&restored, &fname)?;
                path = data_path(&format!("{XLOGDIR}/{fname}"));
            }
            _ => {
                path = data_path(&format!("{XLOGDIR}/{fname}"));
            }
        }
        // BasicOpenFile: the fd-crate raw-open chokepoint (EMFILE-LRU aware),
        // as C's XLogFileRead does.
        let fd = fd::BasicOpenFile(&path, libc::O_RDONLY)?;
        if fd >= 0 {
            self.cur_file_tli = tli;
            if ps_status_seams::set_ps_display::is_installed() {
                ps_status_seams::set_ps_display::call(&format!("recovering {fname}"));
            }
            self.read_source = source;
            RECEIPT_SOURCE.with(|c| c.set(source));
            if source != XLogSource::Stream {
                RECEIPT_TIME.with(|c| c.set(timestamp_seams::get_current_timestamp::call()));
            }
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

    fn xlog_file_read_any_tli(&mut self, segno: XLogSegNo, source: XLogSource) -> PgResult<i32> {
        let wal_segsz = transam_xlog::wal_segment_size();
        // A freshly generated history is saved only if a segment is found:
        // a bootstrapping standby must later prefer the history streamed from
        // the primary over a locally fabricated single-entry list.
        let fresh = self.expected_tles.is_empty();
        let tles = if fresh {
            read_timeline_history(RECOVERY_TARGET_TLI.load(Relaxed))?
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
            if source == XLogSource::Any || source == XLogSource::Archive {
                let fd = self.xlog_file_read(segno, hent.tli, XLogSource::Archive, true)?;
                if fd != -1 {
                    let _ = elog(DEBUG1, "got WAL segment from archive".to_string());
                    found = fd;
                    break;
                }
            }
            if source == XLogSource::Any || source == XLogSource::PgWal {
                let fd = self.xlog_file_read(segno, hent.tli, XLogSource::PgWal, true)?;
                if fd != -1 {
                    found = fd;
                    break;
                }
            }
        }
        if !fresh || found >= 0 {
            self.expected_tles = tles;
        }
        Ok(found)
    }

    fn rescan_latest_timeline(
        &mut self,
        replay_tli: TimeLineID,
        replay_lsn: XLogRecPtr,
    ) -> PgResult<bool> {
        let old_target = RECOVERY_TARGET_TLI.load(Relaxed);
        let newtarget = timeline_seams::find_newest_timeline::call(old_target)?;
        if newtarget == old_target {
            return Ok(false);
        }
        let new_expected = read_timeline_history(newtarget)?;
        let Some(current_tle) = new_expected.iter().find(|t| t.tli == old_target) else {
            let _ = elog(
                LOG,
                format!(
                    "new timeline {newtarget} is not a child of database system timeline {replay_tli}"
                ),
            );
            return Ok(false);
        };
        if current_tle.end < replay_lsn {
            let _ = elog(
                LOG,
                format!(
                    "new timeline {newtarget} forked off current database system timeline {replay_tli} before current recovery point {}",
                    lsn_fmt(replay_lsn)
                ),
            );
            return Ok(false);
        }
        RECOVERY_TARGET_TLI.store(newtarget, Relaxed);
        self.expected_tles = new_expected;
        timeline_seams::restore_timeline_history_files::call(old_target + 1, newtarget)?;
        let _ = elog(LOG, format!("new target timeline is {newtarget}"));
        Ok(true)
    }

    // WaitForWALToBecomeAvailable (xlogrecovery.c:3575): the standby WAL
    // source state machine.
    fn wait_for_wal(
        &mut self,
        rec_ptr: XLogRecPtr,
        tli_rec_ptr: XLogRecPtr,
        replay_lsn: XLogRecPtr,
        nonblocking: bool,
    ) -> PgResult<i32> {
        let mut streaming_reply_sent = false;

        if !IN_ARCHIVE_RECOVERY.load(Relaxed) {
            self.cur_source = XLogSource::PgWal;
        } else if self.cur_source == XLogSource::Any
            || (!StandbyMode() && self.cur_source == XLogSource::Stream)
        {
            self.last_source_failed = false;
            self.cur_source = XLogSource::Archive;
        }

        loop {
            let old_source = self.cur_source;
            let mut start_walreceiver = false;

            if self.last_source_failed {
                // No retry loops during nonblocking readahead: yield already-
                // decoded records to replay first.
                if nonblocking {
                    return Ok(XLREAD_WOULDBLOCK);
                }
                match self.cur_source {
                    XLogSource::Archive | XLogSource::PgWal => {
                        if StandbyMode() && targets::CheckForStandbyTrigger() {
                            xlog_shutdown_wal_rcv();
                            return Ok(XLREAD_FAIL);
                        }
                        if !StandbyMode() {
                            return Ok(XLREAD_FAIL);
                        }
                        self.cur_source = XLogSource::Stream;
                        start_walreceiver = true;
                    }
                    XLogSource::Stream => {
                        debug_assert!(StandbyMode());
                        if wal_rcv_streaming() {
                            xlog_shutdown_wal_rcv();
                        } else {
                            transam_xlog::ResetInstallXLogFileSegmentActive();
                        }
                        if targets::timeline_goal() == RecoveryTargetTimeLineGoal::Latest
                            && self.rescan_latest_timeline(self.replay_tli, replay_lsn)?
                        {
                            self.cur_source = XLogSource::Archive;
                        } else {
                            let now = timestamp_seams::get_current_timestamp::call();
                            let retry_ms =
                                guc_tables::vars::wal_retrieve_retry_interval.read() as i64;
                            let elapsed_ms = (now - self.last_fail_time) / 1000;
                            if elapsed_ms < retry_ms {
                                let wait_time = retry_ms - elapsed_ms;
                                let _ = elog(
                                    LOG,
                                    format!(
                                        "waiting for WAL to become available at {}",
                                        lsn_fmt(rec_ptr)
                                    ),
                                );
                                if procarray_seams::known_assigned_transaction_ids_idle_maintenance::is_installed() {
                                    procarray_seams::known_assigned_transaction_ids_idle_maintenance::call();
                                }
                                let _ = latch::WaitLatch(
                                    Some(targets::recovery_wakeup_latch()),
                                    WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                                    wait_time,
                                    WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL,
                                )?;
                                latch::ResetLatch(targets::recovery_wakeup_latch());
                                startup_seams::process_startup_proc_interrupts::call()?;
                            }
                            self.last_fail_time =
                                timestamp_seams::get_current_timestamp::call();
                            self.cur_source = XLogSource::Archive;
                        }
                    }
                    XLogSource::Any => {
                        ereport(ERROR)
                            .errmsg("unexpected WAL source".to_string())
                            .finish(loc("WaitForWALToBecomeAvailable"))?;
        unreachable!()
                    }
                }
            } else if self.cur_source == XLogSource::PgWal && IN_ARCHIVE_RECOVERY.load(Relaxed) {
                // Prefer the archive over pg_wal for the next file.
                self.cur_source = XLogSource::Archive;
            }

            if self.cur_source != old_source {
                let _ = elog(
                    DEBUG2,
                    format!(
                        "switched WAL source from {} to {} after {}",
                        xlog_source_name(old_source),
                        xlog_source_name(self.cur_source),
                        if self.last_source_failed { "failure" } else { "success" }
                    ),
                );
            }

            self.last_source_failed = false;

            match self.cur_source {
                XLogSource::Archive | XLogSource::PgWal => {
                    debug_assert!(!wal_rcv_streaming());
                    self.close_read_file();
                    if self.rand_access {
                        self.cur_file_tli = 0;
                    }
                    let src = if self.cur_source == XLogSource::Archive {
                        XLogSource::Any
                    } else {
                        self.cur_source
                    };
                    self.read_file = self.xlog_file_read_any_tli(self.read_seg_no, src)?;
                    if self.read_file >= 0 {
                        return Ok(XLREAD_SUCCESS);
                    }
                    self.last_source_failed = true;
                }
                XLogSource::Stream => {
                    debug_assert!(StandbyMode());
                    if PENDING_WALRCV_RESTART.with(Cell::get) && !start_walreceiver {
                        xlog_shutdown_wal_rcv();
                        if targets::timeline_goal() == RecoveryTargetTimeLineGoal::Latest {
                            self.rescan_latest_timeline(self.replay_tli, replay_lsn)?;
                        }
                        start_walreceiver = true;
                    }
                    PENDING_WALRCV_RESTART.with(|c| c.set(false));

                    let conninfo = guc_tables::vars::PrimaryConnInfo.read().unwrap_or_default();
                    if start_walreceiver && !conninfo.is_empty() {
                        let (ptr, tli) = if self.fetching_ckpt {
                            (self.redo_start_lsn, self.redo_start_tli)
                        } else {
                            let tli = tli_of_point_in_history(tli_rec_ptr, &self.expected_tles)?;
                            if self.cur_file_tli > 0 && tli < self.cur_file_tli {
                                { ereport(ERROR)
                                    .errmsg(format!(
                                        "according to history file, WAL location {} belongs to timeline {tli}, but previous recovered WAL file came from timeline {}",
                                        lsn_fmt(tli_rec_ptr),
                                        self.cur_file_tli
                                    ))
                                    .finish(loc("WaitForWALToBecomeAvailable"))?; unreachable!() }
                            }
                            (rec_ptr, tli)
                        };
                        self.cur_file_tli = tli;
                        transam_xlog::SetInstallXLogFileSegmentActive()?;
                        let slotname =
                            guc_tables::vars::PrimarySlotName.read().unwrap_or_default();
                        let create_temp_slot =
                            guc_tables::vars::wal_receiver_create_temp_slot.read();
                        walreceiverfuncs_seams::request_xlog_streaming::call(
                            tli,
                            ptr,
                            &conninfo,
                            &slotname,
                            create_temp_slot,
                        )?;
                        WALRCV_STARTED_WITH.with(|c| {
                            *c.borrow_mut() =
                                Some((conninfo.clone(), slotname.clone(), create_temp_slot));
                        });
                        self.flushed_upto = 0;
                    }

                    if !wal_rcv_streaming() {
                        self.last_source_failed = true;
                        continue;
                    }

                    let mut havedata = false;
                    if rec_ptr < self.flushed_upto {
                        havedata = true;
                    } else {
                        let (flushed, latest_chunk_start, tli) =
                            walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::call();
                        self.flushed_upto = flushed;
                        self.receive_tli = tli;
                        if rec_ptr < self.flushed_upto && self.receive_tli == self.cur_file_tli {
                            havedata = true;
                            if latest_chunk_start <= rec_ptr {
                                let rt =
                                    timestamp_seams::get_current_timestamp::call();
                                RECEIPT_TIME.with(|c| c.set(rt));
                                targets::SetCurrentChunkStartTime(rt);
                            }
                        }
                    }
                    if havedata {
                        if self.read_file < 0 {
                            if self.expected_tles.is_empty() {
                                self.expected_tles =
                                    read_timeline_history(RECOVERY_TARGET_TLI.load(Relaxed))?;
                            }
                            let recv_tli = self.receive_tli;
                            self.read_file = self.xlog_file_read(
                                self.read_seg_no,
                                recv_tli,
                                XLogSource::Stream,
                                false,
                            )?;
                            debug_assert!(self.read_file >= 0);
                        } else {
                            self.read_source = XLogSource::Stream;
                            RECEIPT_SOURCE.with(|c| c.set(XLogSource::Stream));
                            return Ok(XLREAD_SUCCESS);
                        }
                    } else {
                        if nonblocking {
                            return Ok(XLREAD_WOULDBLOCK);
                        }
                        if targets::CheckForStandbyTrigger() {
                            self.last_source_failed = true;
                            continue;
                        }
                        if !streaming_reply_sent {
                            if walreceiverfuncs_seams::wal_rcv_force_reply::is_installed() {
                                walreceiverfuncs_seams::wal_rcv_force_reply::call();
                            }
                            streaming_reply_sent = true;
                        }
                        if procarray_seams::known_assigned_transaction_ids_idle_maintenance::is_installed() {
                            procarray_seams::known_assigned_transaction_ids_idle_maintenance::call();
                        }
                        let _ = latch::WaitLatch(
                            Some(targets::recovery_wakeup_latch()),
                            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                            -1,
                            WAIT_EVENT_RECOVERY_WAL_STREAM,
                        )?;
                        latch::ResetLatch(targets::recovery_wakeup_latch());
                    }
                }
                XLogSource::Any => {
                    ereport(ERROR)
                        .errmsg("unexpected WAL source".to_string())
                        .finish(loc("WaitForWALToBecomeAvailable"))?;
        unreachable!()
                }
            }

            if targets::GetRecoveryPauseState() != targets::RECOVERY_NOT_PAUSED {
                targets::recoveryPausesHere(false)?;
            }
            startup_seams::process_startup_proc_interrupts::call()?;
        }
    }
}

fn rec_end_for_report(target_page_ptr: XLogRecPtr, req_len: i32) -> XLogRecPtr {
    target_page_ptr + req_len as u64
}

// The recycled-segment signature: wrong magic or a page address belonging to
// the segment's previous life. Full validation stays in the reader.
fn page_header_plausible(page: &[u8], target_page_ptr: XLogRecPtr) -> bool {
    let magic = u16::from_ne_bytes(page[0..2].try_into().unwrap());
    let pageaddr = u64::from_ne_bytes(page[8..16].try_into().unwrap());
    magic == xlogreader::XLOG_PAGE_MAGIC && pageaddr == target_page_ptr
}

thread_local! {
    // InRedo (xlogrecovery.c file static; startup-thread only).
    static IN_REDO: Cell<bool> = const { Cell::new(false) };
}
pub(crate) fn in_redo() -> bool {
    IN_REDO.with(|c| c.get())
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
            // fd owned by the reader's segment slot.
            fd::pg_close(v.seg.ws_file);
            v.seg.ws_file = -1;
        }
    }
}

impl XLogReaderRoutine for PageSource {
    // XLogPageRead (xlogrecovery.c:3320).
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        let wal_segsz = v.segcxt.ws_segsize;
        let target_page_off = transam_xlog::XLogSegmentOffset(target_page_ptr, wal_segsz);

        if self.read_file >= 0
            && transam_xlog::XLByteToSeg(target_page_ptr, wal_segsz) != self.read_seg_no
        {
            // Request a restartpoint if we've replayed too much xlog since
            // the last one.
            if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed)
                && init_small::globals::IsUnderPostmaster()
                && transam_xlog::XLogCheckpointNeeded(self.read_seg_no)
            {
                transam_xlog::GetRedoRecPtr();
                if transam_xlog::XLogCheckpointNeeded(self.read_seg_no) {
                    checkpointer_seams::request_checkpoint::call(
                        transam_xlog::CHECKPOINT_CAUSE_XLOG,
                    )?;
                }
            }
            self.close_read_file();
            self.read_source = XLogSource::Any;
        }
        self.read_seg_no = transam_xlog::XLByteToSeg(target_page_ptr, wal_segsz);

        loop {
            if self.read_file < 0
                || (self.read_source == XLogSource::Stream
                    && self.flushed_upto < target_page_ptr + req_len as u64)
            {
                if self.read_file >= 0
                    && v.nonblocking
                    && self.read_source == XLogSource::Stream
                    && self.flushed_upto < target_page_ptr + req_len as u64
                {
                    return Ok(XLREAD_WOULDBLOCK);
                }
                let replay_lsn = REPLAY_END_REC_PTR.load(Relaxed);
                match self.wait_for_wal(
                    target_page_ptr + req_len as u64,
                    target_rec_ptr,
                    replay_lsn,
                    v.nonblocking,
                )? {
                    r if r == XLREAD_WOULDBLOCK => return Ok(XLREAD_WOULDBLOCK),
                    r if r == XLREAD_FAIL => {
                        self.close_read_file();
                        self.read_source = XLogSource::Any;
                        return Ok(XLREAD_FAIL);
                    }
                    _ => {}
                }
            }
            debug_assert!(self.read_file >= 0);

            self.read_off = target_page_off;
            let io_start =
                pgstat::io::pgstat_prepare_io_time(guc_tables::vars::track_wal_io_timing.read());
            // cur_page is the reader's XLOG_BLCKSZ read buffer.
            let r = fd::pg_pread(
                self.read_file,
                &mut cur_page[..XLOG_BLCKSZ],
                self.read_off as i64,
            );
            pgstat::io::pgstat_count_io_op_time(
                pgstat::io::IOObject::Wal,
                pgstat::io::IOContext::IOCONTEXT_NORMAL,
                pgstat::io::IOOp::Read,
                io_start,
                1,
                r.max(0) as u64,
            );
            if r != XLOG_BLCKSZ as isize {
                let errno = std::io::Error::last_os_error();
                let fname =
                    transam_xlog::XLogFileName(self.cur_file_tli, self.read_seg_no, wal_segsz);
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
            } else {
                v.seg.ws_tli = self.cur_file_tli;
                // In standby mode, sanity-check a segment-start page header
                // now: a contrecord's second half read from a recycled local
                // segment must retry another source here, or ReadRecord's
                // whole-record retry loops forever (xlogrecovery.c:3498).
                if StandbyMode()
                    && target_page_ptr % wal_segsz as u64 == 0
                    && !page_header_plausible(cur_page, target_page_ptr)
                {
                    let emode = self.emode;
                    let fname = transam_xlog::XLogFileName(
                        self.cur_file_tli,
                        self.read_seg_no,
                        wal_segsz,
                    );
                    self.report(
                        emode,
                        rec_end_for_report(target_page_ptr, req_len),
                        format!(
                            "invalid page header in WAL segment {fname}, LSN {}, offset {}",
                            lsn_fmt(target_page_ptr),
                            target_page_off
                        ),
                    )?;
                } else {
                let read_len = if self.read_source == XLogSource::Stream
                    && target_page_ptr / XLOG_BLCKSZ as u64 == self.flushed_upto / XLOG_BLCKSZ as u64
                {
                    (transam_xlog::XLogSegmentOffset(self.flushed_upto, wal_segsz)
                        - target_page_off) as i32
                } else {
                    XLOG_BLCKSZ as i32
                };
                return Ok(read_len);
                }
            }

            if v.nonblocking {
                return Ok(XLREAD_WOULDBLOCK);
            }
            self.last_source_failed = true;
            self.close_read_file();
            self.read_source = XLogSource::Any;
            if !StandbyMode() {
                return Ok(XLREAD_FAIL);
            }
        }
    }
}

struct Recovery {
    context: &'static mcx::MemoryContext,
    reader: XLogReaderState<'static>,
    prefetcher: xlogprefetcher::XLogPrefetcher<'static>,
    src: PageSource,
    check_point_loc: XLogRecPtr,
    check_point_tli: TimeLineID,
    aborted_rec_ptr: XLogRecPtr,
    missing_contrec_ptr: XLogRecPtr,
    oldest_active_xid: TransactionId,
}

// ReadRecord: Ok(true) = the reader's current record is the requested one.
// Carries the crash→archive switch and the standby retry loop.
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

    loop {
        let got = rec.prefetcher.XLogPrefetcherReadRecord(&mut rec.reader, &mut rec.src)?;
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
                    let fname =
                        transam_xlog::XLogFileName(rec.reader.v.seg.ws_tli, segno, wal_segsz);
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

        // Crash recovery ran out of WAL in pg_wal with archive recovery
        // requested: switch to the archive now (xlogrecovery.c:3253).
        if !IN_ARCHIVE_RECOVERY.load(Relaxed)
            && ARCHIVE_RECOVERY_REQUESTED.load(Relaxed)
            && !fetching_ckpt
        {
            let _ = elog(
                DEBUG1,
                "reached end of WAL in pg_wal, entering archive recovery".to_string(),
            );
            IN_ARCHIVE_RECOVERY.store(true, Relaxed);
            if STANDBY_MODE_REQUESTED.load(Relaxed) {
                EnableStandbyMode();
            }
            transam_xlog::SwitchIntoArchiveRecovery(rec.reader.v.EndRecPtr, replay_tli)?;
            rec.src.min_recovery_point = rec.reader.v.EndRecPtr;
            rec.src.min_recovery_point_tli = replay_tli;

            check_recovery_consistency(rec)?;

            rec.src.last_source_failed = false;
            rec.src.cur_source = XLogSource::Any;
            continue;
        }

        if StandbyMode() && !targets::CheckForStandbyTrigger() {
            continue;
        }
        return Ok(false);
    }
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
    rec.prefetcher.XLogPrefetcherBeginRead(&mut rec.reader, rec_ptr);
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
    if recovery_file_exists(RECOVERY_COMMAND_FILE) {
        { ereport(FATAL)
            .errmsg(format!(
                "using recovery command file \"{RECOVERY_COMMAND_FILE}\" is not supported"
            ))
            .finish(loc("readRecoverySignalFile"))?; unreachable!() }
    }
    let _ = fd::pg_unlink(&data_path(RECOVERY_COMMAND_DONE));

    let fsync_signal = |rel: &str| {
        if let Ok(f) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_path(rel))
        {
            use std::os::fd::AsRawFd;
            let _ = fd::pg_fsync(f.as_raw_fd());
        }
    };
    if std::path::Path::new(&data_path(STANDBY_SIGNAL_FILE)).exists() {
        fsync_signal(STANDBY_SIGNAL_FILE);
        SIGNAL_FILE_STANDBY.store(true, Relaxed);
    } else if std::path::Path::new(&data_path(RECOVERY_SIGNAL_FILE)).exists() {
        fsync_signal(RECOVERY_SIGNAL_FILE);
        SIGNAL_FILE_RECOVERY.store(true, Relaxed);
    }

    STANDBY_MODE_REQUESTED.store(false, Relaxed);
    ARCHIVE_RECOVERY_REQUESTED.store(false, Relaxed);
    if SIGNAL_FILE_STANDBY.load(Relaxed) {
        STANDBY_MODE_REQUESTED.store(true, Relaxed);
        ARCHIVE_RECOVERY_REQUESTED.store(true, Relaxed);
    } else if SIGNAL_FILE_RECOVERY.load(Relaxed) {
        ARCHIVE_RECOVERY_REQUESTED.store(true, Relaxed);
    } else {
        return Ok(());
    }

    if STANDBY_MODE_REQUESTED.load(Relaxed) && !init_small::globals::IsUnderPostmaster() {
        ereport(FATAL)
            .errmsg("standby mode is not supported by single-user servers")
            .finish(loc("readRecoverySignalFile"))?;
        unreachable!()
    }
    Ok(())
}

fn validate_recovery_parameters() -> PgResult<()> {
    if !ARCHIVE_RECOVERY_REQUESTED.load(Relaxed) {
        return Ok(());
    }

    let restore_command = guc_tables::vars::recoveryRestoreCommand
        .read()
        .unwrap_or_default();
    if STANDBY_MODE_REQUESTED.load(Relaxed) {
        let conninfo = guc_tables::vars::PrimaryConnInfo.read().unwrap_or_default();
        if conninfo.is_empty() && restore_command.is_empty() {
            let _ = ereport(WARNING)
                .errmsg("specified neither \"primary_conninfo\" nor \"restore_command\"")
                .errhint(
                    "The database server will regularly poll the pg_wal subdirectory to check for files placed there.",
                )
                .finish(loc("validateRecoveryParameters"));
        }
    } else if restore_command.is_empty() {
        ereport(FATAL)
            .errmsg("must specify \"restore_command\" when standby mode is not enabled")
            .finish(loc("validateRecoveryParameters"))?;
        unreachable!()
    }

    if targets::recovery_target_action() == targets::RECOVERY_TARGET_ACTION_PAUSE
        && !guc_tables::vars::EnableHotStandby.read()
    {
        targets::set_recovery_target_action_shutdown();
    }

    if targets::recovery_target() == RecoveryTargetType::Time {
        let s = targets::recovery_target_time_string();
        let t = adt_timestamp::timestamptz_in(&s, -1, None)?;
        targets::set_recovery_target_time(t);
    }

    match targets::timeline_goal() {
        RecoveryTargetTimeLineGoal::Numeric => {
            let rtli = targets::recovery_target_tli_requested();
            if rtli != 1 && !timeline_seams::exists_timeline_history::call(rtli)? {
                { ereport(FATAL)
                    .errmsg(format!("recovery target timeline {rtli} does not exist"))
                    .finish(loc("validateRecoveryParameters"))?; unreachable!() }
            }
            RECOVERY_TARGET_TLI.store(rtli, Relaxed);
        }
        RecoveryTargetTimeLineGoal::Latest => {
            let newest =
                timeline_seams::find_newest_timeline::call(RECOVERY_TARGET_TLI.load(Relaxed))?;
            RECOVERY_TARGET_TLI.store(newest, Relaxed);
        }
        RecoveryTargetTimeLineGoal::ControlFile => {}
    }
    Ok(())
}

pub fn InitWalRecovery() -> PgResult<InitWalRecoveryResult> {
    let cf = *transam_xlog::control_file::control_file();
    let dbstate_at_startup = cf.state;
    let mut in_recovery = false;

    let target_tli = if cf.minRecoveryPointTLI > cf.checkPointCopy.ThisTimeLineID {
        cf.minRecoveryPointTLI
    } else {
        cf.checkPointCopy.ThisTimeLineID
    };
    RECOVERY_TARGET_TLI.store(target_tli, Relaxed);

    read_recovery_signal_file()?;
    validate_recovery_parameters()?;

    if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed) {
        latch::OwnLatch(targets::recovery_wakeup_latch())?;
        // C's startup signal handlers all call WakeupRecovery() at delivery;
        // the thread rendering runs dispositions only at drain points, so a
        // delivered signal must also set this latch or an infinite sleep on
        // it (WaitForWALToBecomeAvailable's idle stream wait) never wakes to
        // notice SIGHUP/SIGTERM/SIGQUIT (048 reload; 030/032/033 shutdown).
        if procsignal_seams::set_thread_signal_extra_wake_latch::is_installed() {
            procsignal_seams::set_thread_signal_extra_wake_latch::call(Some(
                targets::recovery_wakeup_latch().as_usize(),
            ));
        }
    }

    let context: &'static mcx::MemoryContext =
        mcx::session_root("wal recovery");
    // LIFO: empty the droppy TLS slot before its context is freed.
    mcx::register_session_cleanup(Box::new(|| {
        RECOVERY.with(|r| drop(r.borrow_mut().take()));
    }));
    let mut reader = XLogReaderState::allocate(context.mcx(), transam_xlog::wal_segment_size())?;
    reader.system_identifier = cf.system_identifier;
    reader.XLogReaderSetDecodeBuffer(guc_tables::vars::wal_decode_buffer_size.read() as usize);
    let prefetcher = xlogprefetcher::XLogPrefetcher::XLogPrefetcherAllocate(context.mcx());

    let mut rec = Recovery {
        context,
        reader,
        prefetcher,
        src: PageSource::new(),
        check_point_loc: cf.checkPoint,
        check_point_tli: cf.checkPointCopy.ThisTimeLineID,
        aborted_rec_ptr: InvalidXLogRecPtr,
        missing_contrec_ptr: InvalidXLogRecPtr,
        oldest_active_xid: types_core::InvalidTransactionId,
    };
    rec.src.redo_start_lsn = cf.checkPointCopy.redo;
    rec.src.redo_start_tli = cf.checkPointCopy.ThisTimeLineID;

    let was_shutdown;
    let mut have_backup_label = false;
    let mut have_tblspc_map = false;
    let mut backup_from_standby = false;
    let mut backup_end_required = false;
    let check_point;

    if let Some(label) = backup_label::read_backup_label()? {
        // Archive recovery with a backup label: roll forward from the
        // label's checkpoint, not pg_control's.
        IN_ARCHIVE_RECOVERY.store(true, Relaxed);
        if STANDBY_MODE_REQUESTED.load(Relaxed) {
            EnableStandbyMode();
        }
        rec.src.redo_start_lsn = label.redo_start_lsn;
        rec.src.redo_start_tli = label.redo_start_tli;
        rec.check_point_loc = label.checkpoint_loc;
        rec.check_point_tli = label.backup_label_tli;
        backup_from_standby = label.backup_from_standby;
        backup_end_required = label.backup_end_required;

        let _ = elog(
            LOG,
            format!(
                "starting backup recovery with redo LSN {}, checkpoint LSN {}, on timeline ID {}",
                lsn_fmt(rec.src.redo_start_lsn),
                lsn_fmt(rec.check_point_loc),
                rec.check_point_tli
            ),
        );

        let (cp_loc, cp_tli) = (rec.check_point_loc, rec.check_point_tli);
        if !read_checkpoint_record(&mut rec, cp_loc, cp_tli)? {
            let dd = init_small::globals::DataDir().unwrap_or(".");
            { ereport(FATAL)
                .errmsg(format!(
                    "could not locate required checkpoint record at {}",
                    lsn_fmt(cp_loc)
                ))
                .errhint(format!(
                    "If you are restoring from a backup, touch \"{dd}/recovery.signal\" or \"{dd}/standby.signal\" and add required recovery options.\nIf you are not restoring from a backup, try removing the file \"{dd}/backup_label\".\nBe careful: removing \"{dd}/backup_label\" will result in a corrupt cluster if restoring from a backup."
                ))
                .finish(loc("InitWalRecovery"))?; unreachable!() }
        }
        check_point = controldata_utils::CheckPoint::from_bytes(rec.reader.XLogRecGetData());
        was_shutdown = (rec.reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK)
            == transam_xlog::XLOG_CHECKPOINT_SHUTDOWN;
        in_recovery = true; // force recovery even if SHUTDOWNED

        if check_point.redo < rec.check_point_loc {
            let redo = check_point.redo;
            rec.prefetcher.XLogPrefetcherBeginRead(&mut rec.reader, redo);
            if !read_record(&mut rec, LOG, false, check_point.ThisTimeLineID)? {
                let dd = init_small::globals::DataDir().unwrap_or(".");
                { ereport(FATAL)
                    .errmsg(format!(
                        "could not find redo location {} referenced by checkpoint record at {}",
                        lsn_fmt(check_point.redo),
                        lsn_fmt(rec.check_point_loc)
                    ))
                    .errhint(format!(
                        "If you are restoring from a backup, touch \"{dd}/recovery.signal\" or \"{dd}/standby.signal\" and add required recovery options.\nIf you are not restoring from a backup, try removing the file \"{dd}/backup_label\".\nBe careful: removing \"{dd}/backup_label\" will result in a corrupt cluster if restoring from a backup."
                    ))
                    .finish(loc("InitWalRecovery"))?; unreachable!() }
            }
        }

        if let Some(tablespaces) = backup_label::read_tablespace_map()? {
            for ti in &tablespaces {
                let linkloc = data_path(&format!("{PG_TBLSPC_DIR}/{}", ti.oid));
                let _ = std::fs::remove_file(&linkloc);
                // wasm32: std exposes no symlink creation on wasi (unix::fs
                // is absent; wasi::fs's is unstable) — refuse with the C
                // error shape (52 = WASI ENOSYS), the tablespace wasm arm's
                // convention. tablespace_map restores are unsupported there.
                #[cfg(target_family = "wasm")]
                let link_result: std::io::Result<()> =
                    Err(std::io::Error::from_raw_os_error(52));
                #[cfg(not(target_family = "wasm"))]
                let link_result = std::os::unix::fs::symlink(&ti.path, &linkloc);
                if let Err(e) = link_result {
                    { ereport(ERROR)
                        .errmsg(format!(
                            "could not create symbolic link \"{linkloc}\": {e}"
                        ))
                        .finish(loc("InitWalRecovery"))?; unreachable!() }
                }
            }
            have_tblspc_map = true;
        }
        have_backup_label = true;
    } else {
        if std::path::Path::new(&data_path(TABLESPACE_MAP)).exists() {
            let _ = std::fs::remove_file(data_path(TABLESPACE_MAP_OLD));
            let renamed = fd::durable_rename(
                &data_path(TABLESPACE_MAP),
                &data_path(TABLESPACE_MAP_OLD),
                DEBUG1,
            );
            let detail = match renamed {
                Ok(0) => {
                    format!("File \"{TABLESPACE_MAP}\" was renamed to \"{TABLESPACE_MAP_OLD}\".")
                }
                _ => format!(
                    "Could not rename file \"{TABLESPACE_MAP}\" to \"{TABLESPACE_MAP_OLD}\"."
                ),
            };
            let _ = elog(
                LOG,
                format!(
                    "ignoring file \"{TABLESPACE_MAP}\" because no file \"{BACKUP_LABEL_FILE}\" exists: {detail}"
                ),
            );
        }

        // No backup label: if we know how far to replay for consistency,
        // enter archive recovery directly; otherwise crash-recover pg_wal
        // first (the ReadRecord end-of-WAL switch does the transition).
        if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed)
            && (cf.minRecoveryPoint != InvalidXLogRecPtr
                || cf.backupEndRequired
                || cf.backupEndPoint != InvalidXLogRecPtr
                || cf.state == transam_xlog::DB_SHUTDOWNED)
        {
            IN_ARCHIVE_RECOVERY.store(true, Relaxed);
            if STANDBY_MODE_REQUESTED.load(Relaxed) {
                EnableStandbyMode();
            }
        }

        if cf.backupStartPoint != InvalidXLogRecPtr {
            let _ = elog(
                LOG,
                format!(
                    "restarting backup recovery with redo LSN {}",
                    lsn_fmt(cf.backupStartPoint)
                ),
            );
        }

        let (cp_loc, cp_tli) = (rec.check_point_loc, rec.check_point_tli);
        if !read_checkpoint_record(&mut rec, cp_loc, cp_tli)? {
            ereport(PANIC)
                .errmsg(format!(
                    "could not locate a valid checkpoint record at {}",
                    lsn_fmt(rec.check_point_loc)
                ))
                .finish(loc("InitWalRecovery"))?;
        }
        check_point = controldata_utils::CheckPoint::from_bytes(rec.reader.XLogRecGetData());
        was_shutdown = (rec.reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK)
            == transam_xlog::XLOG_CHECKPOINT_SHUTDOWN;

        if check_point.redo < rec.check_point_loc {
            let redo = check_point.redo;
            rec.prefetcher.XLogPrefetcherBeginRead(&mut rec.reader, redo);
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
    }

    if !have_backup_label {
        rec.src.redo_start_lsn = check_point.redo;
        rec.src.redo_start_tli = check_point.ThisTimeLineID;
    }
    rec.oldest_active_xid = check_point.oldestActiveXid;

    if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed) {
        let msg = if STANDBY_MODE_REQUESTED.load(Relaxed) {
            "entering standby mode".to_string()
        } else {
            match targets::recovery_target() {
                RecoveryTargetType::Xid => format!(
                    "starting point-in-time recovery to XID {}",
                    targets::recovery_target_xid()
                ),
                RecoveryTargetType::Time => format!(
                    "starting point-in-time recovery to {}",
                    timestamp_seams::timestamptz_to_str::call(targets::recovery_target_time())
                ),
                RecoveryTargetType::Name => format!(
                    "starting point-in-time recovery to \"{}\"",
                    targets::recovery_target_name()
                ),
                RecoveryTargetType::Lsn => format!(
                    "starting point-in-time recovery to WAL location (LSN) \"{}\"",
                    lsn_fmt(targets::recovery_target_lsn())
                ),
                RecoveryTargetType::Immediate => {
                    "starting point-in-time recovery to earliest consistent point".to_string()
                }
                RecoveryTargetType::Unset => "starting archive recovery".to_string(),
            }
        };
        let _ = elog(LOG, msg);
    }

    let target_tli = RECOVERY_TARGET_TLI.load(Relaxed);
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
    } else if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed) {
        in_recovery = true;
    }

    if in_recovery {
        let in_archive = IN_ARCHIVE_RECOVERY.load(Relaxed);
        if !in_archive {
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
        }
        let cp_loc = rec.check_point_loc;
        transam_xlog::control_file::control_file_update(|c| {
            c.state = if in_archive {
                transam_xlog::DB_IN_ARCHIVE_RECOVERY
            } else {
                transam_xlog::DB_IN_CRASH_RECOVERY
            };
            c.checkPoint = cp_loc;
            c.checkPointCopy = check_point;
            if in_archive && c.minRecoveryPoint < check_point.redo {
                c.minRecoveryPoint = check_point.redo;
                c.minRecoveryPointTLI = check_point.ThisTimeLineID;
            }
            if have_backup_label {
                c.backupStartPoint = check_point.redo;
                c.backupEndRequired = backup_end_required;
                if backup_from_standby {
                    if dbstate_at_startup != transam_xlog::DB_IN_ARCHIVE_RECOVERY
                        && dbstate_at_startup != transam_xlog::DB_SHUTDOWNED_IN_RECOVERY
                    {
                        // FATAL below, after the closure.
                    }
                    c.backupEndPoint = c.minRecoveryPoint;
                }
            }
        });
        if have_backup_label
            && backup_from_standby
            && dbstate_at_startup != transam_xlog::DB_IN_ARCHIVE_RECOVERY
            && dbstate_at_startup != transam_xlog::DB_SHUTDOWNED_IN_RECOVERY
        {
            ereport(FATAL)
                .errmsg("backup_label contains data inconsistent with control file")
                .errhint(
                    "This means that the backup is corrupted and you will have to use another backup for recovery.",
                )
                .finish(loc("InitWalRecovery"))?;
        unreachable!()
        }
        xlogutils::set_in_recovery(true);
    }

    {
        let cf_now = transam_xlog::control_file::control_file();
        rec.src.backup_start_point = cf_now.backupStartPoint;
        rec.src.backup_end_required = cf_now.backupEndRequired;
        rec.src.backup_end_point = cf_now.backupEndPoint;
        if IN_ARCHIVE_RECOVERY.load(Relaxed) {
            rec.src.min_recovery_point = cf_now.minRecoveryPoint;
            rec.src.min_recovery_point_tli = cf_now.minRecoveryPointTLI;
        } else {
            rec.src.min_recovery_point = InvalidXLogRecPtr;
            rec.src.min_recovery_point_tli = 0;
        }
    }

    rec.aborted_rec_ptr = InvalidXLogRecPtr;
    rec.missing_contrec_ptr = InvalidXLogRecPtr;
    RECOVERY.with(|r| *r.borrow_mut() = Some(rec));

    Ok(InitWalRecoveryResult {
        was_shutdown,
        have_backup_label,
        have_tblspc_map,
    })
}

// CheckTablespaceDirectory (xlogrecovery.c:2151).
fn check_tablespace_directory() -> PgResult<()> {
    let dir = data_path(PG_TBLSPC_DIR);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Ok(());
    };
    for de in entries.flatten() {
        let name = de.file_name();
        let name = name.to_string_lossy();
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let is_link = de
            .path()
            .symlink_metadata()
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false);
        if !is_link {
            let level = if guc_tables::vars::allow_in_place_tablespaces.read() {
                WARNING
            } else {
                PANIC
            };
            let r = ereport(level)
                .errmsg(format!(
                    "unexpected directory entry \"{name}\" found in {PG_TBLSPC_DIR}"
                ))
                .errdetail(format!(
                    "All directory entries in {PG_TBLSPC_DIR}/ should be symbolic links."
                ))
                .errhint(
                    "Remove those directories, or set \"allow_in_place_tablespaces\" to ON transiently to let recovery complete.",
                )
                .finish(loc("CheckTablespaceDirectory"));
            if level == PANIC {
                r?;
            }
        }
    }
    Ok(())
}

// CheckRecoveryConsistency (xlogrecovery.c:2196).
fn check_recovery_consistency(rec: &mut Recovery) -> PgResult<()> {
    if rec.src.min_recovery_point == InvalidXLogRecPtr {
        return Ok(());
    }
    debug_assert!(IN_ARCHIVE_RECOVERY.load(Relaxed));

    let last_replayed_end = LAST_REPLAYED_END_REC_PTR.load(Relaxed);
    let last_replayed_tli = LAST_REPLAYED_TLI.load(Relaxed);

    if rec.src.backup_end_point != InvalidXLogRecPtr
        && rec.src.backup_end_point <= last_replayed_end
    {
        let save_start = rec.src.backup_start_point;
        let save_end = rec.src.backup_end_point;
        let _ = elog(DEBUG1, "end of backup reached".to_string());
        transam_xlog::ReachedEndOfBackup(last_replayed_end, last_replayed_tli)?;
        rec.src.backup_start_point = InvalidXLogRecPtr;
        rec.src.backup_end_point = InvalidXLogRecPtr;
        rec.src.backup_end_required = false;
        let _ = elog(
            LOG,
            format!(
                "completed backup recovery with redo LSN {} and end LSN {}",
                lsn_fmt(save_start),
                lsn_fmt(save_end)
            ),
        );
    }

    if !reached_consistency()
        && !rec.src.backup_end_required
        && rec.src.min_recovery_point <= last_replayed_end
    {
        xlogutils::XLogCheckInvalidPages()?;
        check_tablespace_directory()?;
        REACHED_CONSISTENCY.store(true, Relaxed);
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_RECOVERY_CONSISTENT);
        let _ = elog(
            LOG,
            format!(
                "consistent recovery state reached at {}",
                lsn_fmt(last_replayed_end)
            ),
        );
    }

    if xlogutils::standby_state() == xlogutils::STANDBY_SNAPSHOT_READY
        && !targets::HotStandbyActiveInReplay()
        && reached_consistency()
        && init_small::globals::IsUnderPostmaster()
    {
        targets::set_shared_hot_standby_active();
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_BEGIN_HOT_STANDBY);
    }
    Ok(())
}

// checkTimeLineSwitch (xlogrecovery.c:2399).
fn check_timeline_switch(
    rec: &Recovery,
    lsn: XLogRecPtr,
    new_tli: TimeLineID,
    prev_tli: TimeLineID,
    replay_tli: TimeLineID,
) -> PgResult<()> {
    if prev_tli != replay_tli {
        { ereport(PANIC)
            .errmsg(format!(
                "unexpected previous timeline ID {prev_tli} (current timeline ID {replay_tli}) in checkpoint record"
            ))
            .finish(loc("checkTimeLineSwitch"))?; unreachable!() }
    }
    if new_tli < replay_tli || !tli_in_history(new_tli, &rec.src.expected_tles) {
        { ereport(PANIC)
            .errmsg(format!(
                "unexpected timeline ID {new_tli} (after {replay_tli}) in checkpoint record"
            ))
            .finish(loc("checkTimeLineSwitch"))?; unreachable!() }
    }
    if rec.src.min_recovery_point != InvalidXLogRecPtr
        && lsn < rec.src.min_recovery_point
        && new_tli > rec.src.min_recovery_point_tli
    {
        { ereport(PANIC)
            .errmsg(format!(
                "unexpected timeline ID {new_tli} in checkpoint record, before reaching minimum recovery point {} on timeline {}",
                lsn_fmt(rec.src.min_recovery_point),
                rec.src.min_recovery_point_tli
            ))
            .finish(loc("checkTimeLineSwitch"))?; unreachable!() }
    }
    Ok(())
}

// The XLOG-rmgr record types handled by the recovery driver itself.
fn xlogrecovery_redo(rec: &mut Recovery) -> PgResult<()> {
    let info = rec.reader.XLogRecGetInfo() & !transam_xlog::XLR_INFO_MASK;
    debug_assert_eq!(rec.reader.XLogRecGetRmid(), transam_xlog::RM_XLOG_ID);

    if info == transam_xlog::XLOG_OVERWRITE_CONTRECORD {
        // xl_overwrite_contrecord: overwritten_lsn 0..8, overwrite_time 8..16.
        let data = rec.reader.XLogRecGetData();
        let overwritten_lsn = u64::from_ne_bytes(data[..8].try_into().unwrap());
        let overwrite_time = i64::from_ne_bytes(data[8..16].try_into().unwrap());
        if overwritten_lsn != rec.reader.overwrittenRecPtr {
            { ereport(FATAL)
                .errmsg(format!(
                    "mismatching overwritten LSN {} -> {}",
                    lsn_fmt(overwritten_lsn),
                    lsn_fmt(rec.reader.overwrittenRecPtr)
                ))
                .finish(loc("xlogrecovery_redo"))?; unreachable!() }
        }

        rec.aborted_rec_ptr = InvalidXLogRecPtr;
        rec.missing_contrec_ptr = InvalidXLogRecPtr;

        let _ = elog(
            LOG,
            format!(
                "successfully skipped missing contrecord at {}, overwritten at {}",
                lsn_fmt(overwritten_lsn),
                timestamp_seams::timestamptz_to_str::call(overwrite_time)
            ),
        );

        // Verifying the record should only happen once.
        rec.reader.overwrittenRecPtr = InvalidXLogRecPtr;
    } else if info == transam_xlog::XLOG_BACKUP_END {
        let data = rec.reader.v.record.as_ref().expect("no decoded record");
        // SAFETY: main_data points into the reader's decode buffer.
        let startpoint =
            u64::from_ne_bytes(unsafe { data.main_data_bytes() }[..8].try_into().unwrap());
        if rec.src.backup_start_point == startpoint {
            let _ = elog(DEBUG1, "end of backup record reached".to_string());
            rec.src.backup_end_point = rec.reader.v.EndRecPtr;
        } else {
            let _ = elog(
                DEBUG1,
                format!(
                    "saw end-of-backup record for backup starting at {}, waiting for {}",
                    lsn_fmt(startpoint),
                    lsn_fmt(rec.src.backup_start_point)
                ),
            );
        }
    }
    Ok(())
}

const XLR_CHECK_CONSISTENCY: u8 = 0x02;

fn apply_wal_record(rec: &mut Recovery, replay_tli: &mut TimeLineID) -> PgResult<()> {
    let xid = rec.reader.XLogRecGetXid();
    let rmid = rec.reader.XLogRecGetRmid();
    let info = rec.reader.XLogRecGetInfo();
    let mut switched_tli = false;

    varsup::AdvanceNextFullTransactionIdPastXid(xid)?;

    if rmid == transam_xlog::RM_XLOG_ID {
        let stripped = info & !transam_xlog::XLR_INFO_MASK;
        let mut new_replay_tli = *replay_tli;
        let mut prev_replay_tli = *replay_tli;
        if stripped == transam_xlog::XLOG_CHECKPOINT_SHUTDOWN {
            let cp = controldata_utils::CheckPoint::from_bytes(rec.reader.XLogRecGetData());
            new_replay_tli = cp.ThisTimeLineID;
            prev_replay_tli = cp.PrevTimeLineID;
        } else if stripped == transam_xlog::XLOG_END_OF_RECOVERY {
            // xl_end_of_recovery: end_time 0..8, ThisTimeLineID 8..12,
            // PrevTimeLineID 12..16.
            let data = rec.reader.XLogRecGetData();
            new_replay_tli = u32::from_ne_bytes(data[8..12].try_into().unwrap());
            prev_replay_tli = u32::from_ne_bytes(data[12..16].try_into().unwrap());
        }
        if new_replay_tli != *replay_tli {
            check_timeline_switch(
                rec,
                rec.reader.v.EndRecPtr,
                new_replay_tli,
                prev_replay_tli,
                *replay_tli,
            )?;
            *replay_tli = new_replay_tli;
            switched_tli = true;
        }
    }

    REPLAY_END_REC_PTR.store(rec.reader.v.EndRecPtr, Relaxed);
    REPLAY_END_TLI.store(*replay_tli, Relaxed);

    if xlogutils::standby_state() != xlogutils::STANDBY_DISABLED
        && xid != types_core::InvalidTransactionId
    {
        procarray_seams::record_known_assigned_transaction_ids::call(xid)?;
    }

    if rmid == transam_xlog::RM_XLOG_ID {
        xlogrecovery_redo(rec)?;
    }

    (rmgr::GetRmgr(rmid)?.rm_redo)(&mut rec.reader.v)?;

    // PGRUST_REDO_PIN_CHECK: every redo arm must return with zero private
    // pins (C recovery holds no pins across records).
    if redo_pin_check() {
        bufmgr::debug_drain_prefetch_pins();
        let pins = bufmgr::debug_all_private_pins();
        if !pins.is_empty() {
            let desc: Vec<String> = pins
                .iter()
                .map(|&(b, rc)| format!("buf={b} rc={rc} tag={}", bufmgr::debug_buffer_tag_string(b)))
                .collect();
            panic!(
                "redo pin leak after rmid={} info={:#04x} end_lsn={:X}: [{}]",
                rmid,
                info,
                rec.reader.v.EndRecPtr,
                desc.join(", ")
            );
        }
    }

    if info & XLR_CHECK_CONSISTENCY != 0 {
        panic!("verifyBackupPageConsistency not ported (wal_consistency_checking record seen)");
    }

    LAST_REPLAYED_READ_REC_PTR.store(rec.reader.v.ReadRecPtr, Relaxed);
    LAST_REPLAYED_END_REC_PTR.store(rec.reader.v.EndRecPtr, Relaxed);
    LAST_REPLAYED_TLI.store(*replay_tli, Relaxed);

    // Wakeup walsenders (xlogrecovery.c:2056): on the standby the WAL is
    // flushed first (waking only physical walsenders, from the walreceiver)
    // and then applied, which wakes only logical walsenders — standby logical
    // decoding can only proceed once a record has been replayed. Physical
    // walsenders need a replay-side wakeup only on a timeline switch.
    // AllowCascadeReplication() = EnableHotStandby && max_wal_senders > 0.
    if guc_tables::vars::EnableHotStandby.read()
        && guc_tables::vars::max_wal_senders.read() > 0
        && walsender_seams::wal_snd_wakeup::is_installed()
    {
        walsender_seams::wal_snd_wakeup::call(switched_tli, true);
    }

    if DO_REQUEST_WALRCV_REPLY.get() {
        DO_REQUEST_WALRCV_REPLY.set(false);
        if walreceiverfuncs_seams::wal_rcv_force_reply::is_installed() {
            walreceiverfuncs_seams::wal_rcv_force_reply::call();
        }
    }

    check_recovery_consistency(rec)?;

    if switched_tli {
        transam_xlog::RemoveNonParentXlogFiles(rec.reader.v.EndRecPtr, *replay_tli)?;
        // XLogPrefetchReconfigure: prefetcher re-reads its GUC lazily here.
    }
    Ok(())
}


fn redo_pin_check() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("PGRUST_REDO_PIN_CHECK").is_some())
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
    let mut reached_recovery_target = false;

    if rec.src.redo_start_lsn < rec.check_point_loc {
        LAST_REPLAYED_READ_REC_PTR.store(InvalidXLogRecPtr, Relaxed);
        LAST_REPLAYED_END_REC_PTR.store(rec.src.redo_start_lsn, Relaxed);
        LAST_REPLAYED_TLI.store(rec.src.redo_start_tli, Relaxed);
    } else {
        LAST_REPLAYED_READ_REC_PTR.store(rec.reader.v.ReadRecPtr, Relaxed);
        LAST_REPLAYED_END_REC_PTR.store(rec.reader.v.EndRecPtr, Relaxed);
        LAST_REPLAYED_TLI.store(rec.check_point_tli, Relaxed);
    }
    REPLAY_END_REC_PTR.store(LAST_REPLAYED_END_REC_PTR.load(Relaxed), Relaxed);
    REPLAY_END_TLI.store(LAST_REPLAYED_TLI.load(Relaxed), Relaxed);
    targets::SetLatestXTime(0);
    targets::SetCurrentChunkStartTime(0);
    targets::SetRecoveryPause(false);
    RECEIPT_TIME.with(|c| c.set(timestamp_seams::get_current_timestamp::call()));

    if init_small::globals::IsUnderPostmaster() {
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_RECOVERY_STARTED);
    }

    check_recovery_consistency(rec)?;

    let mut replay_tli;
    let mut have_record;
    if rec.src.redo_start_lsn < rec.check_point_loc {
        replay_tli = rec.src.redo_start_tli;
        let redo_start = rec.src.redo_start_lsn;
        rec.prefetcher.XLogPrefetcherBeginRead(&mut rec.reader, redo_start);
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
        IN_REDO.with(|c| c.set(true));
        rmgr::RmgrStartup(rec.context.mcx())?;
        let _ = elog(
            LOG,
            format!("redo starts at {}", lsn_fmt(rec.reader.v.ReadRecPtr)),
        );

        while have_record {
            if startup_seams::process_startup_proc_interrupts::is_installed() {
                startup_seams::process_startup_proc_interrupts::call()?;
            }

            if targets::GetRecoveryPauseState() != targets::RECOVERY_NOT_PAUSED {
                targets::recoveryPausesHere(false)?;
            }

            if targets::recoveryStopsBefore(&rec.reader)? {
                reached_recovery_target = true;
                break;
            }

            if targets::recoveryApplyDelay(&rec.reader)?
                && targets::GetRecoveryPauseState() != targets::RECOVERY_NOT_PAUSED
            {
                targets::recoveryPausesHere(false)?;
            }

            apply_wal_record(rec, &mut replay_tli)?;

            if targets::recoveryStopsAfter(&rec.reader)? {
                reached_recovery_target = true;
                break;
            }

            have_record = read_record(rec, LOG, false, replay_tli)?;
        }

        if reached_recovery_target {
            if !reached_consistency() {
                ereport(FATAL)
                    .errmsg("requested recovery stop point is before consistent recovery point")
                    .finish(loc("PerformWalRecovery"))?;
            }
            match targets::recovery_target_action() {
                targets::RECOVERY_TARGET_ACTION_SHUTDOWN => {
                    ipc::proc_exit(3, init_small::globals::MyProcPid());
                }
                targets::RECOVERY_TARGET_ACTION_PAUSE => {
                    targets::SetRecoveryPause(true);
                    targets::recoveryPausesHere(true)?;
                    // drop into promote
                }
                _ => {}
            }
        }

        rmgr::RmgrCleanup();
        let _ = elog(
            LOG,
            format!("redo done at {}", lsn_fmt(rec.reader.v.ReadRecPtr)),
        );
        let xtime = targets::GetLatestXTime();
        if xtime != 0 {
            let _ = elog(
                LOG,
                format!(
                    "last completed transaction was at log time {}",
                    timestamp_seams::timestamptz_to_str::call(xtime)
                ),
            );
        }
        IN_REDO.with(|c| c.set(false));
    } else {
        let _ = elog(LOG, "redo is not required".to_string());
    }

    if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed)
        && targets::recovery_target() != RecoveryTargetType::Unset
        && !reached_recovery_target
    {
        ereport(FATAL)
            .errmsg("recovery ended before configured recovery target was reached")
            .finish(loc("PerformWalRecovery"))?;
        unreachable!()
    }
    Ok(())
}

pub fn FinishWalRecovery() -> PgResult<EndOfWalRecoveryInfo> {
    RECOVERY.with(|cell| {
        let mut guard = cell.borrow_mut();
        let rec = guard.as_mut().expect("FinishWalRecovery before InitWalRecovery");

        xlog_shutdown_wal_rcv();

        // Shutdown the slot sync machinery: drops its temporary slots and
        // stops it fetching failover slots ('synced' stays true, as in C).
        if xlogrecovery_seams::shut_down_slot_sync::is_installed() {
            xlogrecovery_seams::shut_down_slot_sync::call()?;
        }

        debug_assert!(!wal_rcv_streaming());
        STANDBY_MODE.store(false, Relaxed);

        let (last_rec, last_rec_tli) = if !xlogutils::in_recovery() {
            (rec.check_point_loc, rec.check_point_tli)
        } else {
            (
                LAST_REPLAYED_READ_REC_PTR.load(Relaxed),
                LAST_REPLAYED_TLI.load(Relaxed),
            )
        };
        rec.prefetcher.XLogPrefetcherBeginRead(&mut rec.reader, last_rec);
        if !read_record(rec, PANIC, false, last_rec_tli)? {
            ereport(PANIC)
                .errmsg(format!("could not re-read record at {}", lsn_fmt(last_rec)))
                .finish(loc("FinishWalRecovery"))?;
        }
        let end_of_log = rec.reader.v.EndRecPtr;
        let end_of_log_tli = rec.reader.v.seg.ws_tli;

        if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed) {
            debug_assert!(IN_ARCHIVE_RECOVERY.load(Relaxed));
            IN_ARCHIVE_RECOVERY.store(false, Relaxed);
            rec.src.close_read_file();
        }

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
            recoveryStopReason: targets::getRecoveryStopReason(),
            recovery_signal_file_found: SIGNAL_FILE_RECOVERY.load(Relaxed),
            standby_signal_file_found: SIGNAL_FILE_STANDBY.load(Relaxed),
        })
    })
}

pub fn ShutdownWalRecovery() -> PgResult<()> {
    RECOVERY.with(|cell| {
        if let Some(mut rec) = cell.borrow_mut().take() {
            rec.src.close_read_file();
        }
    });
    if ARCHIVE_RECOVERY_REQUESTED.load(Relaxed) {
        let _ = std::fs::remove_file(data_path(&format!("{XLOGDIR}/RECOVERYXLOG")));
        let _ = std::fs::remove_file(data_path(&format!("{XLOGDIR}/RECOVERYHISTORY")));
        if procsignal_seams::set_thread_signal_extra_wake_latch::is_installed() {
            procsignal_seams::set_thread_signal_extra_wake_latch::call(None);
        }
        latch::DisownLatch(targets::recovery_wakeup_latch());
    }
    // The reader's leaked "wal recovery" context stays allocated: a one-shot
    // boot-only arena (C frees it; a few KB once per process here).
    Ok(())
}

// StartupRereadConfig's walreceiver-parameter diff (startup.c:157), homed
// here because the per-process GUC copies C diffs do not exist in the
// thread model: compare the reloaded (process-shared) GUC values against
// what the running walreceiver was started with.
pub fn StartupRereadWalRcvConfig() {
    let read_str = |v: &guc_tables::GucStringVar| -> String {
        if v.installed() { v.read().unwrap_or_default() } else { String::new() }
    };
    let started = WALRCV_STARTED_WITH.with(|c| c.borrow().clone());
    let Some((conninfo, slotname, temp_slot)) = started else {
        return;
    };
    let conninfo_changed = conninfo != read_str(&guc_tables::vars::PrimaryConnInfo);
    let slotname_changed = slotname != read_str(&guc_tables::vars::PrimarySlotName);
    // wal_receiver_create_temp_slot only matters with no slot configured.
    let temp_slot_changed = !slotname_changed
        && slotname.is_empty()
        && guc_tables::vars::wal_receiver_create_temp_slot.installed()
        && temp_slot != guc_tables::vars::wal_receiver_create_temp_slot.read();

    if conninfo_changed || slotname_changed || temp_slot_changed {
        StartupRequestWalReceiverRestart();
    }
}

// StartupRequestWalReceiverRestart (xlogrecovery.c:4417). C also checks
// currentSource == XLOG_FROM_STREAM; the Recovery struct holding cur_source
// is moved out of RECOVERY during the redo loop, so the running-walreceiver
// check stands alone (recorded divergence: a transient non-stream source with
// a live walreceiver requests a restart C would skip — the pending flag is
// consumed and cleared by the next stream attempt either way).
pub fn StartupRequestWalReceiverRestart() {
    if walreceiverfuncs_seams::wal_rcv_running::is_installed()
        && walreceiverfuncs_seams::wal_rcv_running::call()
    {
        let _ = elog(LOG, "WAL receiver process shutdown requested".to_string());
        PENDING_WALRCV_RESTART.with(|c| c.set(true));
    }
}

pub fn GetXLogReceiptTime() -> (TimestampTz, bool) {
    (
        RECEIPT_TIME.with(Cell::get),
        RECEIPT_SOURCE.with(Cell::get) == XLogSource::Stream,
    )
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
    s::get_xlog_receipt_time::set(GetXLogReceiptTime);
    s::standby_mode::set(StandbyMode);
    s::standby_mode_requested::set(StandbyModeRequested);
    s::hot_standby_active::set(HotStandbyActive);
    s::get_recovery_pause_state::set(GetRecoveryPauseState);
    s::set_recovery_pause::set(SetRecoveryPause);
    s::wakeup_recovery::set(WakeupRecovery);
    s::check_promote_signal::set(CheckPromoteSignal);
    s::get_latest_x_time::set(GetLatestXTime);
    s::recovery_requires_int_parameter::set(RecoveryRequiresIntParameter);
    s::startup_request_wal_receiver_restart::set(StartupRequestWalReceiverRestart);
    s::startup_reread_walrcv_config::set(StartupRereadWalRcvConfig);
    targets::install_guc_hooks();
}
