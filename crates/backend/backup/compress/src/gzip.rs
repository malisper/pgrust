//! Port of `basebackup_gzip.c`: bbsink implementing gzip compression.
//!
//! C drives zlib's `deflateInit2(15 + 16)` (gzip wrapper). Here the DEFLATE
//! body comes from miniz_oxide in raw mode and the gzip member framing (RFC
//! 1952 header + CRC32/ISIZE trailer) is emitted explicitly; the result is a
//! standard gzip member exactly as zlib would produce structurally.

use std::boxed::Box;

use ::elog::ereport;
use ::mcx::Mcx;
use ::sink::{
    bbsink_archive_contents, bbsink_begin_backup, bbsink_forward_begin_manifest,
    bbsink_forward_cleanup, bbsink_forward_end_archive, bbsink_forward_end_backup,
    bbsink_forward_end_manifest, bbsink_manifest_contents, Bbsink, BbsinkOps, BbsinkState,
};
use ::types_core::{Size, TimeLineID, XLogRecPtr};
use ::types_error::{PgResult, ERROR};
use compression::{PgCompressSpecification, Z_DEFAULT_COMPRESSION};
use miniz_oxide::deflate::core::{
    compress, create_comp_flags_from_zip_params, CompressorOxide, TDEFLFlush, TDEFLStatus,
};

use crate::loc;

/// C `bbsink_gzip`; the chain and buffers live in the surrounding [`Bbsink`].
pub struct BbsinkGzip<'mcx> {
    mcx: Mcx<'mcx>,
    /// Compression level.
    compresslevel: i32,
    /// Compressed data stream (per-archive; C's `z_stream`).
    compressor: Option<Box<CompressorOxide>>,
    /// Number of bytes staged in the successor's output buffer.
    bytes_written: usize,
    /// Running CRC32 / total input length of the current member (RFC 1952
    /// trailer; zlib maintains these inside the gzip-wrapped z_stream).
    crc: u32,
    isize_mod32: u32,
}

/// Create a new basebackup sink that performs gzip compression
/// (C `bbsink_gzip_new`). gzip is always built here (pure-Rust backend), so
/// C's `#ifndef HAVE_LIBZ` refusal arm has no analog.
pub fn bbsink_gzip_new<'mcx>(
    mcx: Mcx<'mcx>,
    next: Box<Bbsink<'mcx>>,
    compress: &PgCompressSpecification,
) -> PgResult<Box<Bbsink<'mcx>>> {
    let compresslevel = compress.level;
    debug_assert!(
        (1..=9).contains(&compresslevel) || compresslevel == Z_DEFAULT_COMPRESSION,
        "gzip level validated by validate_compress_specification"
    );
    Ok(Box::new(Bbsink::new(
        mcx,
        Box::new(BbsinkGzip {
            mcx,
            compresslevel,
            compressor: None,
            bytes_written: 0,
            crc: 0,
            isize_mod32: 0,
        }),
        Some(next),
    )))
}

impl BbsinkGzip<'_> {
    /// zlib's effective level: Z_DEFAULT_COMPRESSION maps to 6.
    fn effective_level(&self) -> i32 {
        if self.compresslevel == Z_DEFAULT_COMPRESSION {
            6
        } else {
            self.compresslevel
        }
    }

    /// The 10-byte RFC 1952 member header, as zlib emits for a default
    /// gz_header: MTIME=0, XFL per level, OS=3 (Unix).
    fn member_header(&self) -> [u8; 10] {
        let level = self.effective_level();
        let xfl: u8 = if level == 9 {
            2
        } else if level < 2 {
            4
        } else {
            0
        };
        [0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, xfl, 0x03]
    }

    fn compress_error<T>(&self) -> PgResult<T> {
        ereport(ERROR)
            .errmsg("could not compress data")
            .finish(loc("bbsink_gzip"))?;
        unreachable!()
    }
}

impl<'mcx> BbsinkOps<'mcx> for BbsinkGzip<'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // We need our own buffer, because we're going to pass different data
        // to the next sink than what gets passed to us. deflate() doesn't
        // require the output buffer to be of any particular size, so the
        // successor's buffer can just match ours.
        let buffer_length = sink.buffer_length();
        sink.set_buffer(self.mcx, buffer_length)?;
        bbsink_begin_backup(
            sink.next_mut().expect("compression sink must have next sink"),
            state,
            buffer_length,
        )
    }

    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()> {
        // Initialize the compressor object: raw DEFLATE (negative window
        // bits), default strategy — the body zlib's deflateInit2(15+16)
        // would produce inside the gzip wrapper.
        let flags = create_comp_flags_from_zip_params(self.effective_level(), -15, 0);
        self.compressor = Some(Box::new(CompressorOxide::new(flags)));
        self.crc = 0;
        self.isize_mod32 = 0;

        // Add ".gz" to the archive name. pg_basebackup -z produces archives
        // named ".tar.gz" rather than ".tgz"; match that here.
        let gz_archive_name = format!("{archive_name}.gz");
        let next = sink.next_mut().expect("compression sink must have next sink");
        ::sink::bbsink_begin_archive(next, state, &gz_archive_name)?;

        // Stage the gzip member header (zlib emits it on the first deflate
        // call; it is archive content either way).
        debug_assert_eq!(self.bytes_written, 0);
        let next_len = next.buffer_length();
        let header = self.member_header();
        next.buffer_slice_mut(next_len)[..header.len()].copy_from_slice(&header);
        self.bytes_written = header.len();
        Ok(())
    }

    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        let comp = self
            .compressor
            .as_mut()
            .expect("gzip compressor initialized in begin_archive");
        let (input, next) = sink.own_buffer_and_next(len);
        let next_len = next.buffer_length();

        self.crc = crc32c::zlib_crc32_extend(self.crc, input);
        self.isize_mod32 = self.isize_mod32.wrapping_add(len as u32);

        // Compress data from the input buffer until it is all consumed. Each
        // time the output buffer fills up, invoke archive_contents() on the
        // next sink. Since we're compressing, we may very commonly consume
        // all input without filling the output buffer; those bytes then ride
        // along until a later call or until end_archive.
        let mut in_pos = 0usize;
        while in_pos < len {
            debug_assert!(self.bytes_written < next_len);
            let (status, consumed, written) = {
                let out = next.buffer_slice_mut(next_len);
                compress(
                    comp,
                    &input[in_pos..],
                    &mut out[self.bytes_written..],
                    TDEFLFlush::None,
                )
            };
            if !matches!(status, TDEFLStatus::Okay | TDEFLStatus::Done) {
                return self.compress_error();
            }
            in_pos += consumed;
            self.bytes_written += written;

            // If the output buffer is full, it's time for the next sink to
            // process the contents.
            if self.bytes_written >= next_len {
                bbsink_archive_contents(next, state, self.bytes_written)?;
                self.bytes_written = 0;
            } else if consumed == 0 && written == 0 {
                // No progress with output space available: corrupt stream
                // state (C reaches its Z_STREAM_ERROR elog here).
                return self.compress_error();
            }
        }
        Ok(())
    }

    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
        // Flush what remains inside the compressor's internal buffers,
        // finish the DEFLATE stream, then append the gzip member trailer
        // (CRC32 + ISIZE — zlib writes these itself under Z_FINISH).
        let comp = self
            .compressor
            .as_mut()
            .expect("gzip compressor initialized in begin_archive");
        let next = sink.next_mut().expect("compression sink must have next sink");
        let next_len = next.buffer_length();

        loop {
            debug_assert!(self.bytes_written < next_len);
            let (status, _, written) = {
                let out = next.buffer_slice_mut(next_len);
                compress(comp, &[], &mut out[self.bytes_written..], TDEFLFlush::Finish)
            };
            self.bytes_written += written;
            match status {
                TDEFLStatus::Done => break,
                TDEFLStatus::Okay => {}
                _ => return self.compress_error(),
            }
            if self.bytes_written >= next_len {
                bbsink_archive_contents(next, state, self.bytes_written)?;
                self.bytes_written = 0;
            } else if written == 0 {
                return self.compress_error();
            }
        }

        let mut trailer = [0u8; 8];
        trailer[..4].copy_from_slice(&self.crc.to_le_bytes());
        trailer[4..].copy_from_slice(&self.isize_mod32.to_le_bytes());
        for &b in &trailer {
            if self.bytes_written == next_len {
                bbsink_archive_contents(next, state, self.bytes_written)?;
                self.bytes_written = 0;
            }
            next.buffer_slice_mut(next_len)[self.bytes_written] = b;
            self.bytes_written += 1;
        }

        // Send whatever accumulated output bytes we have.
        if self.bytes_written > 0 {
            bbsink_archive_contents(next, state, self.bytes_written)?;
            self.bytes_written = 0;
        }

        self.compressor = None;

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
        self.compressor = None;
        bbsink_forward_cleanup(sink, state)
    }
}
