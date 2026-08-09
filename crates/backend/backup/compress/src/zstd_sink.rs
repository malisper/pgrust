//! Port of `basebackup_zstd.c`: bbsink implementing zstd compression.
//!
//! Uses the same libzstd C library Postgres links (via zstd-sys), driven
//! through ZSTD_compressStream2 exactly as C does. Not available on wasm32
//! (zstd-sys links C); that arm keeps C's "not supported by this build"
//! refusal, which parse_compress_specification already raises anyway.
//!
//! WORKERS NOTE: the vendored libzstd is built without ZSTD_MULTITHREAD (no
//! `zstd/zstdmt` cargo feature), so COMPRESSION_DETAIL 'workers=N' fails at
//! ZSTD_CCtx_setParameter with C's exact "could not set compression worker
//! count" error — the same behavior as a C server linked against a
//! non-multithreaded libzstd. Enabling the `zstdmt` feature would make
//! workers functional; flagged in the PR.

#[cfg(not(target_family = "wasm"))]
pub use imp::bbsink_zstd_new;

/// C `bbsink_zstd_new` for a build without USE_ZSTD (here: wasm32, where
/// zstd-sys cannot be linked).
#[cfg(target_family = "wasm")]
pub fn bbsink_zstd_new<'mcx>(
    _mcx: ::mcx::Mcx<'mcx>,
    _next: std::boxed::Box<::sink::Bbsink<'mcx>>,
    _compress: &compression::PgCompressSpecification,
) -> ::types_error::PgResult<std::boxed::Box<::sink::Bbsink<'mcx>>> {
    ::elog::ereport(::types_error::ERROR)
        .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg("zstd compression is not supported by this build")
        .finish(crate::loc("bbsink_zstd_new"))?;
    unreachable!()
}

#[cfg(not(target_family = "wasm"))]
mod imp {
    use std::boxed::Box;

    use ::elog::ereport;
    use ::mcx::Mcx;
    use ::sink::{
        bbsink_archive_contents, bbsink_begin_backup, bbsink_forward_begin_manifest,
        bbsink_forward_cleanup, bbsink_forward_end_archive, bbsink_forward_end_backup,
        bbsink_forward_end_manifest, bbsink_manifest_contents, Bbsink, BbsinkOps, BbsinkState,
    };
    use ::types_core::{Size, TimeLineID, XLogRecPtr};
    use ::types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
    use compression::{
        PgCompressSpecification, PG_COMPRESSION_OPTION_LONG_DISTANCE,
        PG_COMPRESSION_OPTION_WORKERS,
    };
    use zstd::zstd_safe::{
        compress_bound, get_error_name, zstd_sys::ZSTD_EndDirective, CCtx, CParameter, InBuffer,
        OutBuffer, ResetDirective,
    };

    use crate::{loc, round_up_blcksz};

    /// C `bbsink_zstd`; the chain and buffers live in the surrounding
    /// [`Bbsink`]. C's `zstd_outBuf.pos` becomes `out_pos` (the dst pointer
    /// and size are re-derived from the successor's buffer each call).
    pub struct BbsinkZstd<'mcx> {
        mcx: Mcx<'mcx>,
        /// Compression options.
        compress: PgCompressSpecification,
        cctx: Option<CCtx<'static>>,
        out_pos: usize,
    }

    /// Create a new basebackup sink that performs zstd compression
    /// (C `bbsink_zstd_new`).
    pub fn bbsink_zstd_new<'mcx>(
        mcx: Mcx<'mcx>,
        next: Box<Bbsink<'mcx>>,
        compress: &PgCompressSpecification,
    ) -> PgResult<Box<Bbsink<'mcx>>> {
        Ok(Box::new(Bbsink::new(
            mcx,
            Box::new(BbsinkZstd {
                mcx,
                compress: compress.clone(),
                cctx: None,
                out_pos: 0,
            }),
            Some(next),
        )))
    }

    fn zstd_error<T>(msg: String) -> PgResult<T> {
        ereport(ERROR).errmsg(msg).finish(loc("bbsink_zstd"))?;
        unreachable!()
    }

    impl<'mcx> BbsinkOps<'mcx> for BbsinkZstd<'mcx> {
        fn begin_backup(
            &mut self,
            sink: &mut Bbsink<'mcx>,
            state: &mut BbsinkState,
        ) -> PgResult<()> {
            let compress = &self.compress;
            let Some(mut cctx) = CCtx::try_create() else {
                return zstd_error("could not create zstd compression context".into());
            };

            if let Err(e) = cctx.set_parameter(CParameter::CompressionLevel(compress.level)) {
                return zstd_error(format!(
                    "could not set zstd compression level to {}: {}",
                    compress.level,
                    get_error_name(e)
                ));
            }

            if (compress.options & PG_COMPRESSION_OPTION_WORKERS) != 0 {
                // On older versions of libzstd this option does not exist,
                // and trying to set it will fail; similarly for newer
                // versions compiled without threading support (which is what
                // the vendored build is — see the module WORKERS NOTE).
                if let Err(e) =
                    cctx.set_parameter(CParameter::NbWorkers(compress.workers as u32))
                {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!(
                            "could not set compression worker count to {}: {}",
                            compress.workers,
                            get_error_name(e)
                        ))
                        .finish(loc("bbsink_zstd_begin_backup"));
                }
            }

            if (compress.options & PG_COMPRESSION_OPTION_LONG_DISTANCE) != 0 {
                if let Err(e) = cctx.set_parameter(CParameter::EnableLongDistanceMatching(
                    compress.long_distance,
                )) {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!(
                            "could not enable long-distance mode: {}",
                            get_error_name(e)
                        ))
                        .finish(loc("bbsink_zstd_begin_backup"));
                }
            }

            self.cctx = Some(cctx);

            // We need our own buffer, because we're going to pass different
            // data to the next sink than what gets passed to us; make sure
            // the successor's buffer can accommodate the compressed input
            // buffer, rounded up to a multiple of BLCKSZ.
            let buffer_length = sink.buffer_length();
            sink.set_buffer(self.mcx, buffer_length)?;
            let output_buffer_bound = round_up_blcksz(compress_bound(buffer_length));
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
            // At the start of each archive, reset the state to start a new
            // compression operation. The parameters are sticky
            // (ZSTD_reset_session_only).
            let cctx = self.cctx.as_mut().expect("zstd cctx created in begin_backup");
            if let Err(e) = cctx.reset(ResetDirective::SessionOnly) {
                return zstd_error(format!(
                    "could not compress data: {}",
                    get_error_name(e)
                ));
            }
            self.out_pos = 0;

            // Add ".zst" to the archive name.
            let zstd_archive_name = format!("{archive_name}.zst");
            let next = sink.next_mut().expect("compression sink must have next sink");
            ::sink::bbsink_begin_archive(next, state, &zstd_archive_name)
        }

        fn archive_contents(
            &mut self,
            sink: &mut Bbsink<'mcx>,
            state: &mut BbsinkState,
            len: Size,
        ) -> PgResult<()> {
            let cctx = self.cctx.as_mut().expect("zstd cctx created in begin_backup");
            let (input, next) = sink.own_buffer_and_next(len);
            let next_len = next.buffer_length();
            let mut in_pos = 0usize;

            while in_pos < len {
                let max_needed = compress_bound(len - in_pos);

                // If the out buffer is not left with enough space, send the
                // output buffer to the next sink, and reset it.
                if next_len - self.out_pos < max_needed {
                    bbsink_archive_contents(next, state, self.out_pos)?;
                    self.out_pos = 0;
                }

                let res = {
                    let out = next.buffer_slice_mut(next_len);
                    let mut out_buf = OutBuffer::around_pos(out, self.out_pos);
                    let mut in_buf = InBuffer { src: input, pos: in_pos };
                    let res = cctx.compress_stream2(
                        &mut out_buf,
                        &mut in_buf,
                        ZSTD_EndDirective::ZSTD_e_continue,
                    );
                    self.out_pos = out_buf.pos();
                    in_pos = in_buf.pos;
                    res
                };
                if let Err(e) = res {
                    return zstd_error(format!(
                        "could not compress data: {}",
                        get_error_name(e)
                    ));
                }
            }
            Ok(())
        }

        fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
            // Flush anything inside zstd's internal buffers and end the
            // frame, then forward whatever remains to the successor.
            let cctx = self.cctx.as_mut().expect("zstd cctx created in begin_backup");
            let next = sink.next_mut().expect("compression sink must have next sink");
            let next_len = next.buffer_length();

            loop {
                let max_needed = compress_bound(0);
                if next_len - self.out_pos < max_needed {
                    bbsink_archive_contents(next, state, self.out_pos)?;
                    self.out_pos = 0;
                }

                let res = {
                    let out = next.buffer_slice_mut(next_len);
                    let mut out_buf = OutBuffer::around_pos(out, self.out_pos);
                    let mut in_buf = InBuffer { src: &[], pos: 0 };
                    let res = cctx.compress_stream2(
                        &mut out_buf,
                        &mut in_buf,
                        ZSTD_EndDirective::ZSTD_e_end,
                    );
                    self.out_pos = out_buf.pos();
                    res
                };
                match res {
                    Err(e) => {
                        return zstd_error(format!(
                            "could not compress data: {}",
                            get_error_name(e)
                        ));
                    }
                    Ok(0) => break,
                    Ok(_) => {}
                }
            }

            // Make sure to pass any remaining bytes to the next sink.
            if self.out_pos > 0 {
                bbsink_archive_contents(next, state, self.out_pos)?;
                self.out_pos = 0;
            }

            // Pass on the information that this archive has ended.
            bbsink_forward_end_archive(sink, state)
        }

        fn begin_manifest(
            &mut self,
            sink: &mut Bbsink<'mcx>,
            state: &mut BbsinkState,
        ) -> PgResult<()> {
            bbsink_forward_begin_manifest(sink, state)
        }

        fn manifest_contents(
            &mut self,
            sink: &mut Bbsink<'mcx>,
            state: &mut BbsinkState,
            len: Size,
        ) -> PgResult<()> {
            // Manifest contents are not compressed, but we do need to copy
            // them into the successor sink's buffer, because we have our own.
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
            // Release the context.
            self.cctx = None;
            bbsink_forward_end_backup(sink, state, endptr, endtli)
        }

        fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
            // In case the backup fails, free any compression context that
            // got allocated, so that we don't leak memory.
            self.cctx = None;
            bbsink_forward_cleanup(sink, state)
        }
    }
}
