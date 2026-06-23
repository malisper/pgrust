//! Tests for the lz4 base-backup sink.
//!
//! The LZ4 frame codec primitives are seamed; here we install fixtures that
//! model a trivial frame: `compressBegin` writes a 4-byte header,
//! `compressUpdate` copies the input verbatim, `compressEnd` writes a 2-byte
//! footer, and `compressBound(n)` returns `n + 64`. That lets us drive the real
//! sink buffering / flush / forward logic and verify the bytes arrive intact at
//! the successor and the chain callbacks are forwarded.

use super::*;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use std::sync::Once;

use ::lz4_seams::Lz4CtxHandle;
use ::sink::{
    bbsink_begin_archive, bbsink_begin_backup, bbsink_cleanup, bbsink_end_archive, BbsinkState,
    TablespaceInfo,
};
use ::mcx::MemoryContext;

const HEADER: &[u8] = b"LZ4H";
const FOOTER: &[u8] = b"FT";

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    BeginBackup(usize),
    BeginArchive(String),
    ArchiveContents(Vec<u8>),
    EndArchive,
}

type Log<'a> = &'a RefCell<Vec<Event>>;

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
        self.log
            .borrow_mut()
            .push(Event::BeginArchive(name.to_string()));
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

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        seam::lz4f_compress_bound::set(|input_size, _prefs| input_size + 64);
        seam::lz4f_create_compression_context::set(|| Ok(Lz4CtxHandle(11)));
        seam::lz4f_compress_begin::set(|_ctx, output, _prefs| {
            output[..HEADER.len()].copy_from_slice(HEADER);
            Ok(HEADER.len())
        });
        seam::lz4f_compress_update::set(|_ctx, output, input| {
            output[..input.len()].copy_from_slice(input);
            Ok(input.len())
        });
        seam::lz4f_compress_end::set(|_ctx, output| {
            output[..FOOTER.len()].copy_from_slice(FOOTER);
            Ok(FOOTER.len())
        });
        seam::lz4f_free_compression_context::set(|_ctx| {});
    });
}

fn state() -> BbsinkState {
    BbsinkState {
        tablespaces: alloc::vec![TablespaceInfo::default()],
        tablespace_num: 0,
        ..Default::default()
    }
}

fn spec() -> PgCompressSpecification {
    PgCompressSpecification {
        algorithm: compression::PgCompressAlgorithm::Lz4,
        options: 0,
        level: 1,
        workers: 0,
        long_distance: false,
        parse_error: None,
    }
}

#[test]
fn lz4_frame_forwards_header_payload_footer() {
    install_fixtures();
    let ctx = MemoryContext::new("lz4 test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());

    let leaf = Box::new(Bbsink::new(
        mcx,
        Box::new(RecordingOps { log: &log, mcx }),
        None,
    ));
    let mut sink = *bbsink_lz4_new(mcx, leaf, &spec());
    let mut st = state();

    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_begin_archive(&mut sink, &mut st, "base.tar").unwrap();

    let payload: Vec<u8> = (0u8..40).collect();
    sink.buffer_slice_mut(payload.len())[..payload.len()].copy_from_slice(&payload);
    bbsink_archive_contents(&mut sink, &mut st, payload.len()).unwrap();

    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_cleanup(&mut sink, &mut st).unwrap();

    let events = log.borrow();
    // begin_backup sized the successor's buffer to compressBound(BLCKSZ)=BLCKSZ+64
    // rounded up to a BLCKSZ multiple = 2*BLCKSZ.
    assert_eq!(events[0], Event::BeginBackup(2 * BLCKSZ));
    assert_eq!(events[1], Event::BeginArchive("base.tar.lz4".to_string()));

    let mut received: Vec<u8> = Vec::new();
    let mut saw_end = false;
    for e in events.iter().skip(2) {
        match e {
            Event::ArchiveContents(b) => received.extend_from_slice(b),
            Event::EndArchive => saw_end = true,
            _ => panic!("unexpected event {e:?}"),
        }
    }
    let mut expected: Vec<u8> = Vec::new();
    expected.extend_from_slice(HEADER);
    expected.extend_from_slice(&payload);
    expected.extend_from_slice(FOOTER);
    assert_eq!(received, expected);
    assert!(saw_end);
}
