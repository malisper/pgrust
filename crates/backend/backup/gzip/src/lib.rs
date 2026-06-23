//! Port of PostgreSQL's `basebackup_gzip` (`src/backend/backup/
//! basebackup_gzip.c`): a base-backup [`Bbsink`] that gzip-compresses each
//! archive's contents through a zlib `deflate` stream and forwards the
//! compressed bytes to the next sink.
//!
//! The sink keeps its *own* working buffer (the C `bbs_buffer`, allocated in
//! `begin_backup`) because it passes different (compressed) data to the next
//! sink than what it receives. `archive_contents` feeds the freshly-filled
//! input buffer through `deflate(Z_NO_FLUSH)`, writing the output directly into
//! the *successor* sink's buffer; whenever that output buffer fills, it invokes
//! the successor's `archive_contents` and resets its write cursor.
//! `end_archive` drains zlib's internal buffers with `deflate(Z_FINISH)` and
//! then forwards `end_archive`. Manifest bytes are not compressed but must be
//! copied into the successor's buffer (we own ours). `begin_manifest`,
//! `end_manifest`, `end_backup`, and `cleanup` simply forward.
//!
//! All of that buffering / flushing / forwarding logic is the real port and
//! lives here over the landed [`BbsinkOps`] trait. The zlib codec primitives
//! (`deflateInit2` / `deflate`) are the external-library boundary and are
//! reached through [`gzip_seams`], which panics until the zlib
//! FFI owner lands — mirroring how other external-library boundaries (e.g. the
//! cryptographic primitives) are seamed.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use alloc::boxed::Box;
use alloc::string::String;

use gzip_seams::{
    self as seam, DeflateOutcome, GzipStreamHandle, Z_DEFAULT_COMPRESSION, Z_DEFAULT_STRATEGY,
    Z_FINISH, Z_NO_FLUSH, Z_STREAM_ERROR,
};
use sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_manifest_contents, Bbsink,
    BbsinkOps, BbsinkState,
};
use ::mcx::Mcx;
use ::compression::PgCompressSpecification;
use ::types_core::primitive::{Size, TimeLineID, XLogRecPtr};
use types_error::{ErrorLocation, PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use ::utils_error::ereport;

/// The C source path used in `ereport`/`elog` error locations (`__FILE__`).
const SRCFILE: &str = "src/backend/backup/basebackup_gzip.c";

/// The gzip-compressing base-backup sink (C `struct bbsink_gzip`).
///
/// The C struct's `bbsink base` (forwarding chain + working buffer) is provided
/// by the surrounding [`Bbsink`] this is installed into. The remaining fields
/// live here: the compression level, the live `deflate` stream (the C
/// `z_stream zstream`, held as the opaque [`GzipStreamHandle`] once
/// `begin_archive` initializes it), and `bytes_written` (the number of bytes
/// staged in the successor's output buffer). The surrounding memory-context
/// handle (`mcx`) is retained so `begin_backup` can `palloc` the sink's own
/// working buffer.
pub struct BbsinkGzip<'mcx> {
    /// Surrounding memory context (for allocating the working buffer).
    mcx: Mcx<'mcx>,
    /// Compression level (`compress->level`).
    compresslevel: i32,
    /// The live `deflate` stream, set in `begin_archive`.
    zstream: Option<GzipStreamHandle>,
    /// Number of bytes staged in the successor sink's output buffer.
    bytes_written: usize,
}

/// Create a new base-backup sink that performs gzip compression
/// (C `bbsink_gzip_new`).
///
/// `next` is the successor sink; `compress` carries the parsed compression
/// specification (its `level` is used here). Mirrors the C assertion that the
/// level is in `1..=9` or `Z_DEFAULT_COMPRESSION`.
pub fn bbsink_gzip_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    compress: &PgCompressSpecification,
) -> Box<Bbsink<'mcx>> {
    let compresslevel = compress.level;
    assert!(
        (compresslevel >= 1 && compresslevel <= 9) || compresslevel == Z_DEFAULT_COMPRESSION,
        "gzip compression level out of range"
    );

    let ops = BbsinkGzip {
        mcx,
        compresslevel,
        zstream: None,
        bytes_written: 0,
    };
    Box::new(Bbsink::new(mcx, Box::new(ops), Some(next)))
}

impl<'mcx> BbsinkGzip<'mcx> {
    /// `elog(ERROR, "could not compress data: %s", zs->msg)`.
    fn compress_error(msg: Option<String>, line: i32) -> ::types_error::PgError {
        let detail = msg.unwrap_or_default();
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!("could not compress data: {detail}"))
            .into_error()
            .with_error_location(ErrorLocation::new(SRCFILE, line, "bbsink_gzip"))
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkGzip<'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // We need our own buffer, because we're going to pass different data to
        // the next sink than what gets passed to us.
        //
        // `bbsink_begin_backup` has set `sink.buffer_length()` to the requested
        // length; allocate the sink's own working buffer of that size (the C
        // `sink->bbs_buffer = palloc(sink->bbs_buffer_length)`).
        let buffer_length = sink.buffer_length();
        sink.set_buffer(self.mcx, buffer_length)?;

        // Since deflate() doesn't require the output buffer to be of any
        // particular size, we can just make it the same size as the input
        // buffer.
        let next = sink
            .next_mut()
            .expect("gzip sink must have next sink");
        bbsink_begin_backup(next, state, buffer_length)
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        // Initialize compressor object.
        //
        // We need to use deflateInit2() rather than deflateInit() here so that
        // we can request a gzip header rather than a zlib header. Otherwise, we
        // want to supply the same values that would have been used by default
        // if we had just called deflateInit().
        //
        // Per the documentation for deflateInit2, the third argument must be
        // Z_DEFLATED; the fourth argument is the number of "window bits", by
        // default 15, but adding 16 gets you a gzip header rather than a zlib
        // header; the fifth argument controls memory usage, and 8 is the
        // default; and likewise Z_DEFAULT_STRATEGY is the default for the sixth
        // argument.
        let zstream = match seam::deflate_init2::call(
            self.compresslevel,
            15 + 16,
            8,
            Z_DEFAULT_STRATEGY,
        ) {
            Ok(zs) => zs,
            Err(_res) => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg("could not initialize compression library")
                    .into_error()
                    .with_error_location(ErrorLocation::new(SRCFILE, 139, "bbsink_gzip_begin_archive")));
            }
        };
        self.zstream = Some(zstream);

        // Add ".gz" to the archive name. Note that pg_basebackup -z produces
        // archives named ".tar.gz" rather than ".tgz", so we match that here.
        let gz_archive_name = alloc::format!("{archive_name}.gz");
        let next = sink
            .next_mut()
            .expect("gzip sink must have next sink");
        bbsink_begin_archive(next, state, &gz_archive_name)
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        let zstream = self.zstream.expect("gzip stream must be initialized");

        // Compress data from input buffer. The C code points `zs->next_in` at
        // `bbs_buffer` and `zs->avail_in` at `len`; we copy those bytes out so
        // the input borrow does not conflict with the mutable borrow of the
        // successor's output buffer below.
        let input = sink.buffer_slice(len).to_vec();
        let mut in_pos: usize = 0;

        while in_pos < input.len() {
            let next = sink
                .next_mut()
                .expect("gzip sink must have next sink");
            let out_len = next.buffer_length();

            // Write output data into unused portion of output buffer.
            assert!(
                self.bytes_written < out_len,
                "output buffer already full"
            );
            let out = next.buffer_slice_mut(out_len);

            // Try to compress. deflate() updates next_in/avail_in by `consumed`
            // and next_out/avail_out by `produced`.
            //
            // According to the zlib documentation, Z_STREAM_ERROR should only
            // occur if we've made a programming error, or if say there's been a
            // memory clobber; we use elog() rather than Assert() here out of an
            // abundance of caution.
            let DeflateOutcome {
                res,
                consumed,
                produced,
                msg,
            } = seam::deflate::call(
                zstream,
                &input[in_pos..],
                &mut out[self.bytes_written..],
                Z_NO_FLUSH,
            );
            if res == Z_STREAM_ERROR {
                return Err(Self::compress_error(msg, 200));
            }

            in_pos += consumed;
            // Update our notion of how many bytes we've written.
            self.bytes_written += produced;

            // If the output buffer is full, it's time for the next sink to
            // process the contents.
            if self.bytes_written >= out_len {
                let bytes_written = self.bytes_written;
                let next = sink
                    .next_mut()
                    .expect("gzip sink must have next sink");
                bbsink_archive_contents(next, state, bytes_written)?;
                self.bytes_written = 0;
            }
        }
        Ok(())
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // There might be some data inside zlib's internal buffers; we need to
        // get that flushed out and forwarded to the successor sink as archive
        // content. There is no more input available.
        let zstream = self.zstream.expect("gzip stream must be initialized");

        loop {
            let next = sink
                .next_mut()
                .expect("gzip sink must have next sink");
            let out_len = next.buffer_length();

            // Write output data into unused portion of output buffer.
            assert!(
                self.bytes_written < out_len,
                "output buffer already full"
            );
            let out = next.buffer_slice_mut(out_len);

            // As archive_contents, but pass Z_FINISH since there is no more
            // input.
            let DeflateOutcome {
                res,
                consumed: _,
                produced,
                msg,
            } = seam::deflate::call(zstream, &[], &mut out[self.bytes_written..], Z_FINISH);
            if res == Z_STREAM_ERROR {
                return Err(Self::compress_error(msg, 249));
            }

            // Update our notion of how many bytes we've written.
            self.bytes_written += produced;

            // Apparently we had no data in the output buffer and deflate() was
            // not able to add any. We must be done. (C: `bytes_written == 0`,
            // the absolute count of bytes staged in the output buffer.)
            if self.bytes_written == 0 {
                break;
            }

            // Send whatever accumulated output bytes we have.
            let bytes_written = self.bytes_written;
            let next = sink
                .next_mut()
                .expect("gzip sink must have next sink");
            bbsink_archive_contents(next, state, bytes_written)?;
            self.bytes_written = 0;
        }

        // Must also pass on the information that this archive has ended.
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
        let next = sink
            .next_mut()
            .expect("gzip sink must have next sink");
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

    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        bbsink_forward_cleanup(sink, state)
    }
}

/// Install this crate's seams. The gzip sink is a vtable node consumed directly
/// by `basebackup.c` (a direct dependency), so it owns no inward seams; it is
/// registered in `seams-init::init_all` for uniformity.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
