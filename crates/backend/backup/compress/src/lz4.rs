//! Port of `basebackup_lz4.c`: bbsink implementing lz4 compression.
//!
//! C drives liblz4's frame API (LZ4F_*) with 256KB blocks. Here the frame
//! comes from lz4_flex's `FrameEncoder` configured the same way (256KB
//! blocks, linked block mode, no content checksum — C's zeroed
//! `LZ4F_preferences_t`). The encoder writes into a staging vec which is
//! drained into the successor sink's buffer in buffer-sized chunks.
//!
//! LEVEL NOTE: lz4_flex implements the fast compressor only. The full C
//! level range 0..12 is accepted (validate_compress_specification), but
//! levels >= 3 — LZ4HC territory in C — compress at fast mode here. That is
//! a compression-ratio divergence only; the emitted frames are valid for
//! any LZ4F decoder. Flagged in the PR rather than silently pulling in the
//! C liblz4 binding.

use std::boxed::Box;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use ::elog::ereport;
use ::mcx::Mcx;
use ::sink::{
    bbsink_archive_contents, bbsink_begin_backup, bbsink_forward_begin_manifest,
    bbsink_forward_cleanup, bbsink_forward_end_archive, bbsink_forward_end_backup,
    bbsink_forward_end_manifest, bbsink_manifest_contents, Bbsink, BbsinkOps, BbsinkState,
};
use ::types_core::{Size, TimeLineID, XLogRecPtr};
use ::types_error::{PgResult, ERROR};
use compression::PgCompressSpecification;
use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};

use crate::{loc, round_up_blcksz};

/// The staging writer the frame encoder emits into; drained into the
/// successor sink's buffer by [`BbsinkLz4::drain_staged`].
#[derive(Clone)]
struct StagedOut(Rc<RefCell<Vec<u8>>>);

impl Write for StagedOut {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// C `bbsink_lz4`; the chain and buffers live in the surrounding [`Bbsink`].
pub struct BbsinkLz4<'mcx> {
    mcx: Mcx<'mcx>,
    /// Compression level (kept for C parity; see the module LEVEL NOTE).
    #[allow(dead_code)]
    compresslevel: i32,
    /// Per-archive frame encoder (C's `LZ4F_compressionContext_t`).
    encoder: Option<FrameEncoder<StagedOut>>,
    staged: Rc<RefCell<Vec<u8>>>,
}

/// Create a new basebackup sink that performs lz4 compression
/// (C `bbsink_lz4_new`). lz4 is always built here (pure-Rust backend), so
/// C's `#ifndef USE_LZ4` refusal arm has no analog.
pub fn bbsink_lz4_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    compress: &PgCompressSpecification,
) -> PgResult<Box<Bbsink<'mcx>>> {
    let compresslevel = compress.level;
    debug_assert!(
        (0..=12).contains(&compresslevel),
        "lz4 level validated by validate_compress_specification"
    );
    Ok(Box::new(Bbsink::new(
        mcx,
        Box::new(BbsinkLz4 {
            mcx,
            compresslevel,
            encoder: None,
            staged: Rc::new(RefCell::new(Vec::new())),
        }),
        Some(next),
    )))
}

/// Worst-case LZ4 frame bytes for `n` input bytes with 256KB blocks (the
/// role C's `LZ4F_compressBound` plays in sizing the successor's buffer):
/// each block is stored uncompressed at worst (4-byte block header + data),
/// plus the frame header (<= 19 bytes) and the 8-byte end mark / checksum
/// slot.
fn lz4_output_bound(n: usize) -> usize {
    const BLOCK: usize = 256 * 1024;
    let blocks = n.div_ceil(BLOCK).max(1);
    n + 4 * blocks + 19 + 8
}

impl BbsinkLz4<'_> {
    fn frame_info() -> FrameInfo {
        // C zeroes LZ4F_preferences_t and sets blockSizeID = LZ4F_max256KB:
        // linked blocks, no content checksum, 256KB block size.
        FrameInfo::new()
            .block_size(BlockSize::Max256KB)
            .block_mode(BlockMode::Linked)
            .content_checksum(false)
            .block_checksums(false)
    }

    /// Forward everything the encoder has emitted so far, in chunks no
    /// larger than the successor's buffer.
    fn drain_staged(
        &mut self,
        next: &mut Bbsink<'_>,
        state: &mut BbsinkState,
    ) -> PgResult<()> {
        let staged = std::mem::take(&mut *self.staged.borrow_mut());
        let next_len = next.buffer_length();
        let mut off = 0usize;
        while off < staged.len() {
            let n = next_len.min(staged.len() - off);
            next.buffer_slice_mut(n).copy_from_slice(&staged[off..off + n]);
            bbsink_archive_contents(next, state, n)?;
            off += n;
        }
        Ok(())
    }
}

fn lz4_error<T>(action: &str, e: impl std::fmt::Display) -> PgResult<T> {
    ereport(ERROR)
        .errmsg(format!("could not {action}: {e}"))
        .finish(loc("bbsink_lz4"))?;
    unreachable!()
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkLz4<'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // We need our own buffer, because we're going to pass different data
        // to the next sink than what gets passed to us. Give the successor a
        // buffer that can accommodate the compressed input buffer, rounded
        // up to a multiple of BLCKSZ.
        let buffer_length = sink.buffer_length();
        sink.set_buffer(self.mcx, buffer_length)?;
        let output_buffer_bound = round_up_blcksz(lz4_output_bound(buffer_length));
        bbsink_begin_backup(
            sink.next_mut().expect("compression sink must have next sink"),
            state,
            output_buffer_bound,
        )
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        debug_assert!(self.staged.borrow().is_empty());
        self.encoder = Some(FrameEncoder::with_frame_info(
            Self::frame_info(),
            StagedOut(Rc::clone(&self.staged)),
        ));

        // Add ".lz4" to the archive name.
        let lz4_archive_name = format!("{archive_name}.lz4");
        let next = sink.next_mut().expect("compression sink must have next sink");
        ::sink::bbsink_begin_archive(next, state, &lz4_archive_name)
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        {
            let encoder = self
                .encoder
                .as_mut()
                .expect("lz4 encoder initialized in begin_archive");
            let (input, _next) = sink.own_buffer_and_next(len);
            if let Err(e) = encoder.write_all(input) {
                return lz4_error("compress data", e);
            }
        }
        let next = sink.next_mut().expect("compression sink must have next sink");
        self.drain_staged(next, state)
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // Flush lz4's internal buffers and finalize the frame, then forward
        // whatever compressed bytes remain.
        let encoder = self
            .encoder
            .take()
            .expect("lz4 encoder initialized in begin_archive");
        if let Err(e) = encoder.finish() {
            return lz4_error("end lz4 compression", e);
        }
        let next = sink.next_mut().expect("compression sink must have next sink");
        self.drain_staged(next, state)?;

        // Pass on the information that this archive has ended.
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
        // Manifest contents are not compressed, but we do need to copy them
        // into the successor sink's buffer, because we have our own.
        let (own, next) = sink.own_buffer_and_next(len);
        let next_len = next.buffer_length();
        next.buffer_slice_mut(next_len)[..len].copy_from_slice(own);
        bbsink_manifest_contents(next, state, len)
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
        // In case the backup fails, free the compression context (C calls
        // LZ4F_freeCompressionContext here).
        self.encoder = None;
        self.staged.borrow_mut().clear();
        bbsink_forward_cleanup(sink, state)
    }
}
