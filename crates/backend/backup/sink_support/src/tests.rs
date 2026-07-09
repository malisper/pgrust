//! `pgstat_progress_*` targets `MyBEEntry`, unset in a bare unit test, so those
//! calls are clean no-ops here; these tests assert the state mutations and
//! forwarding order this crate owns, via a recording leaf sink.

use super::*;
use ::mcx::{MemoryContext, Mcx};
use ::sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_begin_manifest,
    bbsink_cleanup, bbsink_end_archive, bbsink_end_backup, bbsink_end_manifest,
    bbsink_manifest_contents, TablespaceInfo,
};
use ::types_core::primitive::BLCKSZ;
use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;

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

fn leaf<'a, 'mcx>(mcx: Mcx<'mcx>, log: &'a RefCell<Vec<Event>>) -> Box<Bbsink<'mcx>>
where
    'a: 'mcx,
{
    Box::new(Bbsink::new(mcx, Box::new(RecordingOps { log, mcx }), None))
}

#[test]
fn archive_contents_tallies_bytes_done() {
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

    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 1, 1).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn end_archive_advances_tablespace_with_guard() {
    let ctx = MemoryContext::new("progress test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_progress_new(mcx, leaf(mcx, &log), false);

    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 2],
        ..BbsinkState::default()
    };
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    bbsink_end_archive(&mut sink, &mut st).unwrap();
    assert_eq!(st.tablespace_num, 1);
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    assert_eq!(st.tablespace_num, 2);
    // Third (e.g. WAL) archive: the streamed-count guard fires (num == total),
    // but tablespace_num still advances and nothing panics.
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    assert_eq!(st.tablespace_num, 3);
}

#[test]
fn full_lifecycle_forwards_to_leaf_in_order() {
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

#[test]
fn archive_contents_handles_estimate_exceeded() {
    // Drives the bytes_total < bytes_done branch (bumps the reported total).
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

    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 1, 1).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn standalone_phase_helpers_do_not_panic() {
    let st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 3],
        ..BbsinkState::default()
    };
    basebackup_progress_wait_checkpoint();
    basebackup_progress_estimate_backup_size();
    basebackup_progress_wait_wal_archive(&st);
    basebackup_progress_transfer_wal();
    basebackup_progress_done();
}
