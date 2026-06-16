//! Port of PostgreSQL's `basebackup_lz4` (`src/backend/backup/
//! basebackup_lz4.c`): a base-backup [`Bbsink`] that compresses each archive's
//! contents through an LZ4 *frame* (`<lz4frame.h>`) and forwards the compressed
//! bytes to the next sink.
//!
//! The sink keeps its own working buffer (the input buffer the caller fills);
//! it writes the compressed frame into the *successor* sink's buffer. Because
//! `LZ4F_compressUpdate` needs an output buffer at least `LZ4F_compressBound`
//! bytes large, `begin_backup` sizes the successor's buffer to the bound of the
//! input-buffer length (rounded up to a `BLCKSZ` multiple). `begin_archive`
//! creates the compression context and writes the frame header.
//! `archive_contents` flushes the staged output to the successor whenever the
//! remaining output room falls below the compression bound for the new input,
//! then `LZ4F_compressUpdate`s. `end_archive` makes room for the footer,
//! `LZ4F_compressEnd`s, flushes the rest, frees the context, and forwards
//! `end_archive`. `cleanup` frees the context if the backup aborted. Manifest
//! bytes are copied (uncompressed) into the successor's buffer; the remaining
//! callbacks forward.
//!
//! All of that buffering / flush / forward logic is the real port over the
//! landed [`BbsinkOps`] trait; the LZ4 frame codec primitives are reached
//! through [`backend_backup_lz4_seams`], which panics until the lz4 FFI owner
//! lands — mirroring how other external-library boundaries are seamed.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::boxed::Box;
use alloc::string::String;

use backend_backup_lz4_seams::{self as seam, Lz4CtxHandle, Lz4Preferences, LZ4F_MAX_256KB};
use backend_backup_sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_end_archive, bbsink_forward_end_backup,
    bbsink_forward_end_manifest, bbsink_manifest_contents, Bbsink, BbsinkOps, BbsinkState,
};
use backend_utils_error::ereport;
use mcx::Mcx;
use types_compression::PgCompressSpecification;
use types_core::primitive::{Size, TimeLineID, XLogRecPtr, BLCKSZ};
use types_error::{ErrorLocation, PgResult, ERRCODE_INTERNAL_ERROR, ERROR};

/// The C source path used in `ereport`/`elog` error locations (`__FILE__`).
const SRCFILE: &str = "src/backend/backup/basebackup_lz4.c";

/// The lz4-compressing base-backup sink (C `struct bbsink_lz4`).
///
/// The C struct's `bbsink base` is provided by the surrounding [`Bbsink`]. The
/// remaining fields live here: the compression level, the live compression
/// context (the C `LZ4F_compressionContext_t ctx`, held as [`Lz4CtxHandle`]),
/// the frame preferences (the C `LZ4F_preferences_t prefs`), and
/// `bytes_written` (bytes staged in the successor's output buffer). `mcx` is
/// retained so `begin_backup` can `palloc` the sink's own working buffer.
pub struct BbsinkLz4<'mcx> {
    mcx: Mcx<'mcx>,
    compresslevel: i32,
    ctx: Option<Lz4CtxHandle>,
    prefs: Lz4Preferences,
    bytes_written: usize,
}

/// Create a new base-backup sink that performs lz4 compression
/// (C `bbsink_lz4_new`). Mirrors the C assertion that the level is in `0..=12`.
pub fn bbsink_lz4_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    compress: &PgCompressSpecification,
) -> Box<Bbsink<'mcx>> {
    let compresslevel = compress.level;
    assert!(
        compresslevel >= 0 && compresslevel <= 12,
        "lz4 compression level out of range"
    );

    let ops = BbsinkLz4 {
        mcx,
        compresslevel,
        ctx: None,
        // Zeroed prefs; the two fields the sink sets are filled in begin_backup.
        prefs: Lz4Preferences {
            block_size_id: 0,
            compression_level: 0,
        },
        bytes_written: 0,
    };
    Box::new(Bbsink::new(mcx, Box::new(ops), Some(next)))
}

impl<'mcx> BbsinkLz4<'mcx> {
    /// `elog(ERROR, "<context>: %s", LZ4F_getErrorName(...))`.
    fn lz4_error(prefix: &str, name: String, line: i32) -> types_error::PgError {
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!("{prefix}: {name}"))
            .into_error()
            .with_error_location(ErrorLocation::new(SRCFILE, line, "bbsink_lz4"))
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkLz4<'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // Initialize compressor object. (C memset(prefs, 0) + set the two
        // fields.)
        self.prefs = Lz4Preferences {
            block_size_id: LZ4F_MAX_256KB,
            compression_level: self.compresslevel,
        };

        // We need our own buffer, because we're going to pass different data to
        // the next sink than what gets passed to us.
        let buffer_length = sink.buffer_length();
        sink.set_buffer(self.mcx, buffer_length)?;

        // Since LZ4F_compressUpdate() requires the output buffer of size equal
        // or greater than that of LZ4F_compressBound(), make sure we have the
        // next sink's bbs_buffer of length that can accommodate the compressed
        // input buffer.
        let mut output_buffer_bound = seam::lz4f_compress_bound::call(buffer_length, self.prefs);

        // The buffer length is expected to be a multiple of BLCKSZ, so round
        // up.
        output_buffer_bound = output_buffer_bound + BLCKSZ - (output_buffer_bound % BLCKSZ);

        let next = sink.next_mut().expect("lz4 sink must have next sink");
        bbsink_begin_backup(next, state, output_buffer_bound)
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        let ctx = match seam::lz4f_create_compression_context::call() {
            Ok(c) => c,
            Err(name) => {
                return Err(Self::lz4_error(
                    "could not create lz4 compression context",
                    name,
                    141,
                ));
            }
        };
        self.ctx = Some(ctx);

        // First of all write the frame header to destination buffer.
        let prefs = self.prefs;
        let bytes_written = self.bytes_written;
        let next = sink.next_mut().expect("lz4 sink must have next sink");
        let out_len = next.buffer_length();
        let out = next.buffer_slice_mut(out_len);
        let header_size = match seam::lz4f_compress_begin::call(ctx, &mut out[bytes_written..], prefs)
        {
            Ok(n) => n,
            Err(name) => return Err(Self::lz4_error("could not write lz4 header", name, 145)),
        };

        // We need to write the compressed data after the header in the output
        // buffer. So, make sure to update the notion of bytes written to output
        // buffer.
        self.bytes_written += header_size;

        // Add ".lz4" to the archive name.
        let lz4_archive_name = alloc::format!("{archive_name}.lz4");
        let next = sink.next_mut().expect("lz4 sink must have next sink");
        bbsink_begin_archive(next, state, &lz4_archive_name)
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        avail_in: Size,
    ) -> PgResult<()> {
        let ctx = self.ctx.expect("lz4 context must be initialized");
        let avail_in_bound = seam::lz4f_compress_bound::call(avail_in, self.prefs);

        // If the number of available bytes has fallen below the value computed
        // by LZ4F_compressBound(), ask the next sink to process the data so that
        // we can empty the buffer.
        let out_len = sink
            .next()
            .expect("lz4 sink must have next sink")
            .buffer_length();
        if (out_len - self.bytes_written) < avail_in_bound {
            let bytes_written = self.bytes_written;
            let next = sink.next_mut().expect("lz4 sink must have next sink");
            bbsink_archive_contents(next, state, bytes_written)?;
            self.bytes_written = 0;
        }

        // Compress the input buffer and write it into the output buffer.
        let input = sink.buffer_slice(avail_in).to_vec();
        let bytes_written = self.bytes_written;
        let next = sink.next_mut().expect("lz4 sink must have next sink");
        let out_len = next.buffer_length();
        let out = next.buffer_slice_mut(out_len);
        let compressed_size =
            match seam::lz4f_compress_update::call(ctx, &mut out[bytes_written..], &input) {
                Ok(n) => n,
                Err(name) => return Err(Self::lz4_error("could not compress data", name, 203)),
            };

        // Update our notion of how many bytes we've written into output buffer.
        self.bytes_written += compressed_size;
        Ok(())
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        let ctx = self.ctx.expect("lz4 context must be initialized");
        let lz4_footer_bound = seam::lz4f_compress_bound::call(0, self.prefs);

        let out_len = sink
            .next()
            .expect("lz4 sink must have next sink")
            .buffer_length();
        assert!(
            out_len >= lz4_footer_bound,
            "output buffer too small for lz4 footer"
        );

        if (out_len - self.bytes_written) < lz4_footer_bound {
            let bytes_written = self.bytes_written;
            let next = sink.next_mut().expect("lz4 sink must have next sink");
            bbsink_archive_contents(next, state, bytes_written)?;
            self.bytes_written = 0;
        }

        let bytes_written = self.bytes_written;
        let next = sink.next_mut().expect("lz4 sink must have next sink");
        let out = next.buffer_slice_mut(out_len);
        let compressed_size = match seam::lz4f_compress_end::call(ctx, &mut out[bytes_written..]) {
            Ok(n) => n,
            Err(name) => return Err(Self::lz4_error("could not end lz4 compression", name, 245)),
        };

        // Update our notion of how many bytes we've written.
        self.bytes_written += compressed_size;

        // Send whatever accumulated output bytes we have.
        let bytes_written = self.bytes_written;
        let next = sink.next_mut().expect("lz4 sink must have next sink");
        bbsink_archive_contents(next, state, bytes_written)?;
        self.bytes_written = 0;

        // Release the resources.
        seam::lz4f_free_compression_context::call(ctx);
        self.ctx = None;

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
        let src = sink.buffer_slice(len).to_vec();
        let next = sink.next_mut().expect("lz4 sink must have next sink");
        let dst = next.buffer_slice_mut(len);
        dst.copy_from_slice(&src);
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

    fn cleanup(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        // In case the backup fails, make sure we free the compression context
        // if needed to avoid a memory leak. (C `bbsink_lz4_cleanup` only frees
        // the context; it does not forward `cleanup` to the successor.)
        if let Some(ctx) = self.ctx.take() {
            seam::lz4f_free_compression_context::call(ctx);
        }
        Ok(())
    }
}

/// Install this crate's seams. The lz4 sink is consumed directly by
/// `basebackup.c`, so it owns no inward seams; registered in
/// `seams-init::init_all` for uniformity.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
