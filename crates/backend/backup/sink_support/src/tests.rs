//! Tests for the base-backup progress sink.
//!
//! The progress callbacks call the real `pgstat_progress_*` functions in
//! `backend-utils-activity-small`, which reach this backend's status entry
//! (`MyBEEntry`) through the `backend-utils-activity-status-seams` slots. Those
//! slots are process globals (`OnceLock`s), so the harness installs them once
//! against a shared `PgBackendStatus` guarded by a mutex, and serializes the
//! tests on that mutex so the captured `st_progress_*` state stays consistent.

extern crate std;

use super::*;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;
use std::sync::{Mutex, MutexGuard, Once};

use sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_cleanup,
    bbsink_end_archive, bbsink_end_backup, bbsink_manifest_contents, bbsink_begin_manifest,
    bbsink_end_manifest, TablespaceInfo,
};
use mcx::MemoryContext;
use types_core::primitive::BLCKSZ;
use types_pgstat::backend_status::PgBackendStatus;

// --- Shared backend status entry the installed seams operate on. ---

static BEENTRY: Mutex<Option<PgBackendStatus>> = Mutex::new(None);
static SERIALIZE: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

/// Install the `backend_status` seams once. The progress functions consult
/// `my_be_entry_present`/`track_activities` and write through `with_my_beentry`.
fn install_seams() {
    INSTALL.call_once(|| {
        status_seams::my_be_entry_present::set(|| true);
        status_seams::track_activities::set(|| true);
        status_seams::with_my_beentry::set(|f| {
            let mut be = BEENTRY.lock().unwrap();
            f(be.as_mut().expect("backend entry initialized by begin_test"));
        });
    });
}

/// Acquire the serialization lock, install seams, and reset the shared entry.
fn begin_test() -> MutexGuard<'static, ()> {
    let guard = SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
    install_seams();
    *BEENTRY.lock().unwrap() = Some(PgBackendStatus::default());
    guard
}

fn param(index: i32) -> i64 {
    BEENTRY.lock().unwrap().as_ref().unwrap().st_progress_param[index as usize]
}

fn command() -> ProgressCommandType {
    BEENTRY.lock().unwrap().as_ref().unwrap().st_progress_command
}

// --- A leaf sink that records callbacks and owns the buffer. ---

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    BeginBackup,
    BeginArchive(String),
    ArchiveContents(Size),
    EndArchive,
    BeginManifest,
    ManifestContents(Size),
    EndManifest,
    EndBackup(XLogRecPtr, TimeLineID),
    Cleanup,
}

struct RecordingOps<'a, 'mcx> {
    log: &'a RefCell<Vec<Event>>,
    mcx: Mcx<'mcx>,
}

impl<'a, 'mcx> BbsinkOps<'mcx> for RecordingOps<'a, 'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginBackup);
        sink.set_buffer(self.mcx, BLCKSZ)
    }
    fn begin_archive(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        name: &str,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginArchive(name.to_string()));
        Ok(())
    }
    fn archive_contents(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::ArchiveContents(len));
        Ok(())
    }
    fn end_archive(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::EndArchive);
        Ok(())
    }
    fn begin_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginManifest);
        Ok(())
    }
    fn manifest_contents(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::ManifestContents(len));
        Ok(())
    }
    fn end_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::EndManifest);
        Ok(())
    }
    fn end_backup(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::EndBackup(endptr, endtli));
        Ok(())
    }
    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::Cleanup);
        sink.clear_buffer(self.mcx);
        Ok(())
    }
}

fn leaf<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    log: &'a RefCell<Vec<Event>>,
) -> Box<Bbsink<'mcx>>
where
    'a: 'mcx,
{
    Box::new(Bbsink::new(mcx, Box::new(RecordingOps { log, mcx }), None))
}

#[test]
fn new_starts_command_and_sets_total_unknown() {
    let _g = begin_test();
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let _sink = bbsink_progress_new(mcx, leaf(mcx, &log), true);

    // Command started, total advertised as -1 (unknown until estimated).
    assert_eq!(command(), ProgressCommandType::Basebackup);
    assert_eq!(param(PROGRESS_BASEBACKUP_BACKUP_TOTAL), -1);
}

#[test]
fn begin_backup_reports_phase_total_and_tablespaces() {
    let _g = begin_test();
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_progress_new(mcx, leaf(mcx, &log), true);

    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 2],
        bytes_total: 4096,
        bytes_total_is_valid: true,
        ..BbsinkState::default()
    };

    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    assert_eq!(param(PROGRESS_BASEBACKUP_PHASE), PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP);
    assert_eq!(param(PROGRESS_BASEBACKUP_BACKUP_TOTAL), 4096);
    assert_eq!(param(PROGRESS_BASEBACKUP_TBLSPC_TOTAL), 2);
    assert!(log.borrow().contains(&Event::BeginBackup));

    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn begin_backup_total_unknown_when_estimate_invalid() {
    let _g = begin_test();
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_progress_new(mcx, leaf(mcx, &log), false);

    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 1],
        bytes_total: 999,
        bytes_total_is_valid: false,
        ..BbsinkState::default()
    };
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    assert_eq!(param(PROGRESS_BASEBACKUP_BACKUP_TOTAL), -1);
    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn archive_contents_tracks_bytes_done() {
    let _g = begin_test();
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_progress_new(mcx, leaf(mcx, &log), true);

    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 1],
        bytes_total: 1_000_000,
        bytes_total_is_valid: true,
        ..BbsinkState::default()
    };
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_archive_contents(&mut sink, &mut st, 100).unwrap();
    bbsink_archive_contents(&mut sink, &mut st, 50).unwrap();

    assert_eq!(st.bytes_done, 150);
    assert_eq!(param(PROGRESS_BASEBACKUP_BACKUP_STREAMED), 150);
    // Estimate not exceeded, so total stays at the begin_backup value.
    assert_eq!(param(PROGRESS_BASEBACKUP_BACKUP_TOTAL), 1_000_000);

    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 1, 1).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn archive_contents_bumps_total_when_estimate_exceeded() {
    let _g = begin_test();
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_progress_new(mcx, leaf(mcx, &log), true);

    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 1],
        bytes_total: 100,
        bytes_total_is_valid: true,
        ..BbsinkState::default()
    };
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_archive_contents(&mut sink, &mut st, 250).unwrap();

    assert_eq!(st.bytes_done, 250);
    assert_eq!(param(PROGRESS_BASEBACKUP_BACKUP_STREAMED), 250);
    // Exceeded the estimate: total is bumped to bytes_done.
    assert_eq!(param(PROGRESS_BASEBACKUP_BACKUP_TOTAL), 250);

    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 1, 1).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn end_archive_advances_tablespace_with_guard() {
    let _g = begin_test();
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_progress_new(mcx, leaf(mcx, &log), false);

    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 2],
        ..BbsinkState::default()
    };
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    // First archive: tablespace_num 0 < 2, streamed -> 1, then num -> 1.
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    assert_eq!(param(PROGRESS_BASEBACKUP_TBLSPC_STREAMED), 1);
    assert_eq!(st.tablespace_num, 1);

    // Second archive: tablespace_num 1 < 2, streamed -> 2, then num -> 2.
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    assert_eq!(param(PROGRESS_BASEBACKUP_TBLSPC_STREAMED), 2);
    assert_eq!(st.tablespace_num, 2);

    // Third archive (e.g. WAL): guard prevents streamed exceeding total; the
    // streamed param stays at 2 but tablespace_num still advances.
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    assert_eq!(param(PROGRESS_BASEBACKUP_TBLSPC_STREAMED), 2);
    assert_eq!(st.tablespace_num, 3);
}

#[test]
fn wait_wal_archive_reports_all_tablespaces_done() {
    let _g = begin_test();
    let st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 3],
        ..BbsinkState::default()
    };
    basebackup_progress_wait_wal_archive(&st);
    assert_eq!(param(PROGRESS_BASEBACKUP_PHASE), PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE);
    assert_eq!(param(PROGRESS_BASEBACKUP_TBLSPC_STREAMED), 3);
}

#[test]
fn standalone_phase_helpers() {
    let _g = begin_test();
    basebackup_progress_wait_checkpoint();
    assert_eq!(param(PROGRESS_BASEBACKUP_PHASE), PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT);

    basebackup_progress_estimate_backup_size();
    assert_eq!(param(PROGRESS_BASEBACKUP_PHASE), PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE);

    basebackup_progress_transfer_wal();
    assert_eq!(param(PROGRESS_BASEBACKUP_PHASE), PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL);
}

#[test]
fn done_ends_command() {
    let _g = begin_test();
    // start a command first so end_command has something to clear.
    pgstat_progress_start_command(ProgressCommandType::Basebackup, InvalidOid);
    assert_eq!(command(), ProgressCommandType::Basebackup);
    basebackup_progress_done();
    assert_eq!(command(), ProgressCommandType::Invalid);
}

#[test]
fn full_lifecycle_forwards_to_leaf() {
    let _g = begin_test();
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_progress_new(mcx, leaf(mcx, &log), true);

    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 1],
        bytes_total: 4096,
        bytes_total_is_valid: true,
        ..BbsinkState::default()
    };
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_begin_archive(&mut sink, &mut st, "base.tar").unwrap();
    bbsink_archive_contents(&mut sink, &mut st, 128).unwrap();
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_begin_manifest(&mut sink, &mut st).unwrap();
    bbsink_manifest_contents(&mut sink, &mut st, 64).unwrap();
    bbsink_end_manifest(&mut sink, &mut st).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 99, 7).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();

    assert_eq!(
        log.borrow().as_slice(),
        &[
            Event::BeginBackup,
            Event::BeginArchive("base.tar".to_string()),
            Event::ArchiveContents(128),
            Event::EndArchive,
            Event::BeginManifest,
            Event::ManifestContents(64),
            Event::EndManifest,
            Event::EndBackup(99, 7),
            Event::Cleanup,
        ]
    );
}
