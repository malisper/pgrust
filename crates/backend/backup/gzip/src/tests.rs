//! Tests for the gzip base-backup sink.
//!
//! The zlib codec primitives are seamed; here we install a fixture `deflate`
//! that models a trivial pass-through "compressor" (it copies input to output
//! verbatim and flushes nothing on `Z_FINISH`). That lets us drive the real
//! sink buffering / flush / forward logic and check that the bytes arrive at
//! the successor sink intact and that the chain callbacks are forwarded.

use super::*;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use std::sync::Once;

use ::gzip_seams::{DeflateOutcome, GzipStreamHandle};
use ::sink::{
    bbsink_begin_archive, bbsink_begin_backup, bbsink_end_archive, BbsinkState, TablespaceInfo,
};
use ::mcx::MemoryContext;
use ::types_core::primitive::BLCKSZ;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    BeginBackup(usize),
    BeginArchive(String),
    ArchiveContents(Vec<u8>),
    EndArchive,
}

type Log<'a> = &'a RefCell<Vec<Event>>;

/// Leaf sink that records every callback and captures the output bytes.
struct RecordingOps<'a, 'mcx> {
    log: Log<'a>,
    mcx: ::mcx::Mcx<'mcx>,
}

impl<'a, 'mcx> BbsinkOps<'mcx> for RecordingOps<'a, 'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        let len = sink.buffer_length();
        self.log.borrow_mut().push(Event::BeginBackup(len));
        sink.set_buffer(self.mcx, len)
    }
    fn begin_archive(
        &mut self,
        _: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        name: &str,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginArchive(name.to_string()));
        Ok(())
    }
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        let bytes = sink.buffer_slice(len).to_vec();
        self.log.borrow_mut().push(Event::ArchiveContents(bytes));
        Ok(())
    }
    fn end_archive(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::EndArchive);
        Ok(())
    }
    fn begin_manifest(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
    fn manifest_contents(
        &mut self,
        _: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        _: Size,
    ) -> PgResult<()> {
        Ok(())
    }
    fn end_manifest(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
    fn end_backup(
        &mut self,
        _: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        _: XLogRecPtr,
        _: TimeLineID,
    ) -> PgResult<()> {
        Ok(())
    }
    fn cleanup(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
}

/// Install a fixture `deflate` that copies input to output verbatim (a
/// pass-through "compressor"). `Z_FINISH` produces nothing extra.
fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        seam::deflate_init2::set(|_level, _wbits, _mem, _strategy| Ok(GzipStreamHandle(7)));
        seam::deflate::set(|_stream, input, output, _flush| {
            let n = input.len().min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            DeflateOutcome {
                res: 0,
                consumed: n,
                produced: n,
                msg: None,
            }
        });
    });
}

fn state() -> BbsinkState {
    BbsinkState {
        tablespaces: alloc::vec![TablespaceInfo::default()],
        tablespace_num: 0,
        ..Default::default()
    }
}

#[test]
fn gzip_pass_through_forwards_bytes_and_appends_suffix() {
    install_fixtures();
    let ctx = MemoryContext::new("gzip test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let spec = PgCompressSpecification {
        algorithm: compression::PgCompressAlgorithm::Gzip,
        options: 0,
        level: 6,
        workers: 0,
        long_distance: false,
        parse_error: None,
    };

    let leaf = Box::new(Bbsink::new(mcx, Box::new(RecordingOps { log: &log, mcx }), None));
    let mut sink = *bbsink_gzip_new(mcx, leaf, &spec);
    let mut st = state();

    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_begin_archive(&mut sink, &mut st, "base.tar").unwrap();

    // Feed some bytes through the sink's input buffer.
    let payload: Vec<u8> = (0u8..32).collect();
    sink.buffer_slice_mut(payload.len())[..payload.len()].copy_from_slice(&payload);
    bbsink_archive_contents(&mut sink, &mut st, payload.len()).unwrap();

    bbsink_end_archive(&mut sink, &mut st).unwrap();

    let events = log.borrow();
    assert_eq!(events[0], Event::BeginBackup(BLCKSZ));
    assert_eq!(events[1], Event::BeginArchive("base.tar.gz".to_string()));
    // With a fitting output buffer, no flush happens until end_archive; but the
    // pass-through compressor never produces on Z_FINISH, so the staged bytes
    // are flushed on the next-full check. Verify all payload bytes arrive and
    // the archive end is forwarded.
    let mut received: Vec<u8> = Vec::new();
    let mut saw_end = false;
    for e in events.iter().skip(2) {
        match e {
            Event::ArchiveContents(b) => received.extend_from_slice(b),
            Event::EndArchive => saw_end = true,
            _ => panic!("unexpected event {e:?}"),
        }
    }
    assert_eq!(received, payload);
    assert!(saw_end);
}
