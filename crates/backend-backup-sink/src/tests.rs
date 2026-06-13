use super::*;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;
use mcx::MemoryContext;

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

type Log<'a> = &'a RefCell<Vec<Event>>;

/// Leaf sink that records every callback and owns the backing buffer.
struct RecordingOps<'a, 'mcx> {
    log: Log<'a>,
    mcx: Mcx<'mcx>,
}

impl<'a, 'mcx> BbsinkOps<'mcx> for RecordingOps<'a, 'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginBackup);
        let len = sink.buffer_length;
        sink.set_buffer(self.mcx, len)
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

/// Forwarding sink that delegates every callback to the next sink.
struct ForwardingOps;

impl<'mcx> BbsinkOps<'mcx> for ForwardingOps {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_backup(sink, state)
    }
    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        name: &str,
    ) -> PgResult<()> {
        bbsink_forward_begin_archive(sink, state, name)
    }
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        bbsink_forward_archive_contents(sink, state, len)
    }
    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_end_archive(sink, state)
    }
    fn begin_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_begin_manifest(sink, state)
    }
    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        bbsink_forward_manifest_contents(sink, state, len)
    }
    fn end_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_end_manifest(sink, state)
    }
    fn end_backup(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        bbsink_forward_end_backup(sink, state, endptr, endtli)
    }
    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_cleanup(sink, state)
    }
}

fn state() -> BbsinkState {
    BbsinkState {
        startptr: 10,
        starttli: 2,
        ..BbsinkState::default()
    }
}

#[test]
fn begin_backup_sets_state_and_buffer_length() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut st = state();
    let mut sink = Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None);

    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    assert_eq!(sink.buffer_length(), BLCKSZ);
    assert!(sink.has_buffer());
    assert_eq!(log.borrow().as_slice(), &[Event::BeginBackup]);

    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn callbacks_dispatch_to_ops_table() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut st = state();
    let mut sink = Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None);

    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_begin_archive(&mut sink, &mut st, "base.tar").unwrap();
    bbsink_archive_contents(&mut sink, &mut st, 128).unwrap();
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_begin_manifest(&mut sink, &mut st).unwrap();
    bbsink_manifest_contents(&mut sink, &mut st, 256).unwrap();
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
            Event::ManifestContents(256),
            Event::EndManifest,
            Event::EndBackup(99, 7),
            Event::Cleanup,
        ]
    );
}

#[test]
fn forwarding_callbacks_delegate_to_next_sink() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut st = state();
    let leaf = Box::new(Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None));
    let mut forwarder = Bbsink::new(mcx, Box::new(ForwardingOps), Some(leaf));

    bbsink_begin_backup(&mut forwarder, &mut st, BLCKSZ).unwrap();
    assert!(forwarder.has_buffer());
    assert_eq!(
        forwarder.buffer_length(),
        forwarder.next().unwrap().buffer_length()
    );

    bbsink_begin_archive(&mut forwarder, &mut st, "base.tar").unwrap();
    bbsink_archive_contents(&mut forwarder, &mut st, 64).unwrap();
    bbsink_end_archive(&mut forwarder, &mut st).unwrap();
    bbsink_begin_manifest(&mut forwarder, &mut st).unwrap();
    bbsink_manifest_contents(&mut forwarder, &mut st, 32).unwrap();
    bbsink_end_manifest(&mut forwarder, &mut st).unwrap();
    bbsink_end_backup(&mut forwarder, &mut st, 100, 8).unwrap();
    bbsink_cleanup(&mut forwarder, &mut st).unwrap();

    assert_eq!(
        log.borrow().as_slice(),
        &[
            Event::BeginBackup,
            Event::BeginArchive("base.tar".to_string()),
            Event::ArchiveContents(64),
            Event::EndArchive,
            Event::BeginManifest,
            Event::ManifestContents(32),
            Event::EndManifest,
            Event::EndBackup(100, 8),
            Event::Cleanup,
        ]
    );
}

#[test]
#[should_panic(expected = "archive content length must fit sink buffer")]
fn archive_contents_rejects_empty_len() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut st = state();
    let mut sink = Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None);
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_archive_contents(&mut sink, &mut st, 0).unwrap();
}

#[test]
fn tablespaces_are_tracked() {
    let tablespaces = vec![
        TablespaceInfo {
            oid: 1234,
            path: Some("/data/ts1".to_string()),
            rpath: None,
            size: Some(4096),
        },
        TablespaceInfo {
            oid: 0,
            path: None,
            rpath: None,
            size: None,
        },
    ];
    let st = BbsinkState {
        tablespaces,
        starttli: 1,
        ..BbsinkState::default()
    };
    assert_eq!(st.tablespaces.len(), 2);
    assert_eq!(st.tablespaces[0].oid, 1234);
    assert_eq!(st.tablespaces[1].path, None);
}

#[test]
#[should_panic(expected = "all tablespaces must be processed")]
fn end_backup_requires_all_tablespaces_processed() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 2],
        starttli: 1,
        ..BbsinkState::default()
    };
    let mut sink = Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None);
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 1, 1).unwrap();
}

#[test]
fn end_backup_succeeds_when_all_tablespaces_processed() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut st = BbsinkState {
        tablespaces: vec![TablespaceInfo::default(); 2],
        starttli: 1,
        ..BbsinkState::default()
    };
    let mut sink = Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None);
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    st.tablespace_num = 2;
    bbsink_end_backup(&mut sink, &mut st, 5, 6).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();
    assert!(log.borrow().contains(&Event::EndBackup(5, 6)));
}

#[test]
fn set_buffer_rejects_oversized_allocation() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None);
    assert!(sink.set_buffer(mcx, mcx::MAX_ALLOC_SIZE + 1).is_err());
    assert!(!sink.has_buffer());
}

#[test]
fn full_backup_lifecycle_balances_context() {
    let ctx = MemoryContext::new("bbsink test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut st = state();
    let mut sink = Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None);

    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    assert!(ctx.used() > 0, "charged during the backup");
    bbsink_begin_archive(&mut sink, &mut st, "base.tar").unwrap();
    bbsink_archive_contents(&mut sink, &mut st, 128).unwrap();
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 99, 7).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();

    // cleanup released the buffer; the only remaining charge is the empty
    // PgVec installed by clear_buffer (zero capacity).
    assert_eq!(ctx.used(), 0, "buffer charge released after cleanup");
}
