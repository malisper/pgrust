//! Port of PostgreSQL's `basebackup_zstd` (`src/backend/backup/
//! basebackup_zstd.c`): a base-backup [`Bbsink`] that compresses each archive's
//! contents through a zstd stream (`<zstd.h>`) and forwards the compressed
//! bytes to the next sink.
//!
//! The sink keeps its own working buffer (the input buffer the caller fills);
//! it writes the compressed stream into the *successor* sink's buffer (the C
//! `ZSTD_outBuffer` whose `dst` is the successor's buffer). `begin_backup`
//! creates the context, applies the level / worker / long-distance parameters
//! from the compression specification, allocates the sink's own buffer, and
//! sizes the successor's buffer to `ZSTD_compressBound(buffer_length)` rounded
//! up to a `BLCKSZ` multiple. `begin_archive` does a session-only reset and
//! repoints the output buffer at the successor. `archive_contents` drives
//! `ZSTD_compressStream2(ZSTD_e_continue)`, flushing the output buffer to the
//! successor whenever the remaining room falls below the compression bound.
//! `end_archive` drains the frame with `ZSTD_e_end`, flushes the remainder, and
//! forwards `end_archive`. `end_backup` and `cleanup` free the context.
//! Manifest bytes are copied (uncompressed) into the successor's buffer.
//!
//! All of that buffering / flush / forward logic is the real port over the
//! landed [`BbsinkOps`] trait; the zstd codec primitives are reached through
//! [`zstd_seams`], which panics until the zstd FFI owner lands —
//! mirroring how other external-library boundaries are seamed.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::boxed::Box;
use alloc::string::String;

use sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_end_archive, bbsink_forward_end_backup,
    bbsink_forward_end_manifest, bbsink_manifest_contents, Bbsink, BbsinkOps, BbsinkState,
};
use zstd_seams::{
    self as seam, ZstdCParameter, ZstdCctxHandle, ZstdEndDirective, ZstdResetDirective,
    ZstdStreamOutcome,
};
use utils_error::ereport;
use mcx::Mcx;
use compression::{
    PgCompressSpecification, PG_COMPRESSION_OPTION_LONG_DISTANCE, PG_COMPRESSION_OPTION_WORKERS,
};
use types_core::primitive::{Size, TimeLineID, XLogRecPtr, BLCKSZ};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
};

/// The C source path used in `ereport`/`elog` error locations (`__FILE__`).
const SRCFILE: &str = "src/backend/backup/basebackup_zstd.c";

/// The zstd-compressing base-backup sink (C `struct bbsink_zstd`).
///
/// The C struct's `bbsink base` is provided by the surrounding [`Bbsink`]. The
/// remaining fields live here: the compression specification, the live context
/// (the C `ZSTD_CCtx *cctx`, held as [`ZstdCctxHandle`]), and the output-buffer
/// write cursor (`out_pos`, the C `zstd_outBuf.pos`; `zstd_outBuf.dst`/`.size`
/// are the successor sink's buffer / length). `mcx` is retained so
/// `begin_backup` can `palloc` the sink's own working buffer.
pub struct BbsinkZstd<'mcx> {
    mcx: Mcx<'mcx>,
    compress: PgCompressSpecification,
    cctx: Option<ZstdCctxHandle>,
    out_pos: usize,
}

/// Create a new base-backup sink that performs zstd compression
/// (C `bbsink_zstd_new`).
pub fn bbsink_zstd_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    compress: &PgCompressSpecification,
) -> Box<Bbsink<'mcx>> {
    let ops = BbsinkZstd {
        mcx,
        compress: compress.clone(),
        cctx: None,
        out_pos: 0,
    };
    Box::new(Bbsink::new(mcx, Box::new(ops), Some(next)))
}

impl<'mcx> BbsinkZstd<'mcx> {
    fn compress_error(name: String, line: i32) -> types_error::PgError {
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!("could not compress data: {name}"))
            .into_error()
            .with_error_location(ErrorLocation::new(SRCFILE, line, "bbsink_zstd"))
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkZstd<'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        let cctx = match seam::zstd_create_cctx::call() {
            Some(c) => c,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg("could not create zstd compression context")
                    .into_error()
                    .with_error_location(ErrorLocation::new(SRCFILE, 97, "bbsink_zstd_begin_backup")));
            }
        };
        self.cctx = Some(cctx);

        if let Err(name) = seam::zstd_cctx_set_parameter::call(
            cctx,
            ZstdCParameter::CompressionLevel,
            self.compress.level,
        ) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg(alloc::format!(
                    "could not set zstd compression level to {}: {name}",
                    self.compress.level
                ))
                .into_error()
                .with_error_location(ErrorLocation::new(SRCFILE, 102, "bbsink_zstd_begin_backup")));
        }

        if (self.compress.options & PG_COMPRESSION_OPTION_WORKERS) != 0 {
            // On older versions of libzstd, this option does not exist, and
            // trying to set it will fail. Similarly for newer versions if they
            // are compiled without threading support.
            if let Err(name) = seam::zstd_cctx_set_parameter::call(
                cctx,
                ZstdCParameter::NbWorkers,
                self.compress.workers,
            ) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(alloc::format!(
                        "could not set compression worker count to {}: {name}",
                        self.compress.workers
                    ))
                    .into_error()
                    .with_error_location(ErrorLocation::new(SRCFILE, 117, "bbsink_zstd_begin_backup")));
            }
        }

        if (self.compress.options & PG_COMPRESSION_OPTION_LONG_DISTANCE) != 0 {
            if let Err(name) = seam::zstd_cctx_set_parameter::call(
                cctx,
                ZstdCParameter::EnableLongDistanceMatching,
                self.compress.long_distance as i32,
            ) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(alloc::format!("could not enable long-distance mode: {name}"))
                    .into_error()
                    .with_error_location(ErrorLocation::new(SRCFILE, 129, "bbsink_zstd_begin_backup")));
            }
        }

        // We need our own buffer, because we're going to pass different data to
        // the next sink than what gets passed to us.
        let buffer_length = sink.buffer_length();
        sink.set_buffer(self.mcx, buffer_length)?;

        // Make sure that the next sink's bbs_buffer is big enough to accommodate
        // the compressed input buffer.
        let mut output_buffer_bound = seam::zstd_compress_bound::call(buffer_length);

        // The buffer length is expected to be a multiple of BLCKSZ, so round
        // up.
        output_buffer_bound = output_buffer_bound + BLCKSZ - (output_buffer_bound % BLCKSZ);

        let next = sink.next_mut().expect("zstd sink must have next sink");
        bbsink_begin_backup(next, state, output_buffer_bound)
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        let cctx = self.cctx.expect("zstd context must be initialized");

        // At the start of each archive we reset the state to start a new
        // compression operation. The parameters are sticky and they will stick
        // around as we are resetting with option ZSTD_reset_session_only.
        seam::zstd_cctx_reset::call(cctx, ZstdResetDirective::SessionOnly);

        // zstd_outBuf.dst = next->bbs_buffer; .size = next->bbs_buffer_length;
        // .pos = 0;
        self.out_pos = 0;

        // Add ".zst" to the archive name.
        let zstd_archive_name = alloc::format!("{archive_name}.zst");
        let next = sink.next_mut().expect("zstd sink must have next sink");
        bbsink_begin_archive(next, state, &zstd_archive_name)
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        let cctx = self.cctx.expect("zstd context must be initialized");

        // ZSTD_inBuffer inBuf = {bbs_buffer, len, 0};
        let input = sink.buffer_slice(len).to_vec();
        let in_size = input.len();
        let mut in_pos: usize = 0;

        while in_pos < in_size {
            let max_needed = seam::zstd_compress_bound::call(in_size - in_pos);

            let out_size = sink
                .next()
                .expect("zstd sink must have next sink")
                .buffer_length();

            // If the out buffer is not left with enough space, send the output
            // buffer to the next sink, and reset it.
            if out_size - self.out_pos < max_needed {
                let out_pos = self.out_pos;
                let next = sink.next_mut().expect("zstd sink must have next sink");
                bbsink_archive_contents(next, state, out_pos)?;
                // zstd_outBuf.dst/.size repointed at the (same) successor
                // buffer; .pos = 0.
                self.out_pos = 0;
            }

            let out_pos = self.out_pos;
            let next = sink.next_mut().expect("zstd sink must have next sink");
            let out_len = next.buffer_length();
            let out = next.buffer_slice_mut(out_len);
            let outcome = seam::zstd_compress_stream2::call(
                cctx,
                out,
                out_pos,
                &input,
                in_pos,
                ZstdEndDirective::Continue,
            );
            let ZstdStreamOutcome {
                in_pos: new_in,
                out_pos: new_out,
                yet_to_flush: _,
            } = match outcome {
                Ok(o) => o,
                Err(name) => return Err(Self::compress_error(name, 220)),
            };
            in_pos = new_in;
            self.out_pos = new_out;
        }
        Ok(())
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        let cctx = self.cctx.expect("zstd context must be initialized");

        loop {
            let max_needed = seam::zstd_compress_bound::call(0);

            let out_size = sink
                .next()
                .expect("zstd sink must have next sink")
                .buffer_length();

            // If the out buffer is not left with enough space, send the output
            // buffer to the next sink, and reset it.
            if out_size - self.out_pos < max_needed {
                let out_pos = self.out_pos;
                let next = sink.next_mut().expect("zstd sink must have next sink");
                bbsink_archive_contents(next, state, out_pos)?;
                self.out_pos = 0;
            }

            let out_pos = self.out_pos;
            let next = sink.next_mut().expect("zstd sink must have next sink");
            let out_len = next.buffer_length();
            let out = next.buffer_slice_mut(out_len);
            // ZSTD_inBuffer in = {NULL, 0, 0};
            let outcome = seam::zstd_compress_stream2::call(
                cctx,
                out,
                out_pos,
                &[],
                0,
                ZstdEndDirective::End,
            );
            let ZstdStreamOutcome {
                in_pos: _,
                out_pos: new_out,
                yet_to_flush,
            } = match outcome {
                Ok(o) => o,
                Err(name) => return Err(Self::compress_error(name, 264)),
            };
            self.out_pos = new_out;

            if yet_to_flush == 0 {
                break;
            }
        }

        // Make sure to pass any remaining bytes to the next sink.
        if self.out_pos > 0 {
            let out_pos = self.out_pos;
            let next = sink.next_mut().expect("zstd sink must have next sink");
            bbsink_archive_contents(next, state, out_pos)?;
            self.out_pos = 0;
        }

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
        let next = sink.next_mut().expect("zstd sink must have next sink");
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
        // Release the context.
        if let Some(cctx) = self.cctx.take() {
            seam::zstd_free_cctx::call(cctx);
        }
        bbsink_forward_end_backup(sink, state, endptr, endtli)
    }

    fn cleanup(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        // In case the backup fails, make sure we free any compression context
        // that got allocated, so that we don't leak memory. (C
        // `bbsink_zstd_cleanup` only frees the context; it does not forward.)
        if let Some(cctx) = self.cctx.take() {
            seam::zstd_free_cctx::call(cctx);
        }
        Ok(())
    }
}

/// Install this crate's seams. The zstd sink is consumed directly by
/// `basebackup.c`, so it owns no inward seams; registered in
/// `seams-init::init_all` for uniformity.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
