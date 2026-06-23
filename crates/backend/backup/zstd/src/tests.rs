//! Tests for the zstd base-backup sink.
//!
//! The zstd codec primitives are seamed; here we install fixtures that model a
//! trivial stream: `ZSTD_e_continue` copies the remaining input into the output
//! verbatim, and `ZSTD_e_end` writes a 2-byte footer once and reports nothing
//! more to flush. `compressBound(n)` returns `n + 64`. That lets us drive the
//! real sink buffering / flush / forward logic and verify bytes arrive intact.

use super::*;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use std::sync::Once;

use ::sink::{
    bbsink_begin_archive, bbsink_begin_backup, bbsink_end_archive, BbsinkState, TablespaceInfo,
};
use ::zstd_seams::{ZstdCctxHandle, ZstdEndDirective, ZstdStreamOutcome};
use ::mcx::MemoryContext;

const FOOTER: &[u8] = b"ZE";

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
        seam::zstd_create_cctx::set(|| Some(ZstdCctxHandle(13)));
        seam::zstd_cctx_set_parameter::set(|_cctx, _p, _v| Ok(()));
        seam::zstd_cctx_reset::set(|_cctx, _d| {});
        seam::zstd_compress_bound::set(|n| n + 64);
        seam::zstd_compress_stream2::set(|_cctx, output, out_pos, input, in_pos, end_op| {
            match end_op {
                ZstdEndDirective::Continue => {
                    let n = (input.len() - in_pos).min(output.len() - out_pos);
                    output[out_pos..out_pos + n].copy_from_slice(&input[in_pos..in_pos + n]);
                    Ok(ZstdStreamOutcome {
                        in_pos: in_pos + n,
                        out_pos: out_pos + n,
                        yet_to_flush: 0,
                    })
                }
                ZstdEndDirective::End => {
                    output[out_pos..out_pos + FOOTER.len()].copy_from_slice(FOOTER);
                    Ok(ZstdStreamOutcome {
                        in_pos,
                        out_pos: out_pos + FOOTER.len(),
                        yet_to_flush: 0,
                    })
                }
            }
        });
        seam::zstd_free_cctx::set(|_cctx| {});
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
        algorithm: compression::PgCompressAlgorithm::Zstd,
        options: 0,
        level: 3,
        workers: 0,
        long_distance: false,
        parse_error: None,
    }
}

#[test]
fn zstd_stream_forwards_payload_and_footer() {
    install_fixtures();
    let ctx = MemoryContext::new("zstd test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());

    let leaf = Box::new(Bbsink::new(
        mcx,
        Box::new(RecordingOps { log: &log, mcx }),
        None,
    ));
    let mut sink = *bbsink_zstd_new(mcx, leaf, &spec());
    let mut st = state();

    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    bbsink_begin_archive(&mut sink, &mut st, "base.tar").unwrap();

    let payload: Vec<u8> = (0u8..48).collect();
    sink.buffer_slice_mut(payload.len())[..payload.len()].copy_from_slice(&payload);
    bbsink_archive_contents(&mut sink, &mut st, payload.len()).unwrap();

    bbsink_end_archive(&mut sink, &mut st).unwrap();

    let events = log.borrow();
    assert_eq!(events[0], Event::BeginBackup(2 * BLCKSZ));
    assert_eq!(events[1], Event::BeginArchive("base.tar.zst".to_string()));

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
    expected.extend_from_slice(&payload);
    expected.extend_from_slice(FOOTER);
    assert_eq!(received, expected);
    assert!(saw_end);
}
