//! Page decompression. Borrowed codecs (build-vs-borrow ruling): snap,
//! lz4_flex raw-block, zstd. Legacy/unsupported codecs error cleanly with
//! the codec name — never a wrong answer.

use types_error::{PgError, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT};

use crate::meta::{unsupported, CodecId};

/// Scan buffers keep 64 readable bytes past their logical end so decode
/// kernels never need tail special-casing.
pub(crate) const PAD: usize = 64;

#[track_caller]
#[cold]
#[inline(never)]
fn decompress_failed(codec: &str, detail: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!("corrupt {codec} page in parquet file"))
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT)
            .with_detail(detail),
    )
}

/// Decompress `src` into `dst` (cleared first), which ends up holding exactly
/// `uncompressed` bytes plus zeroed padding. The declared size is the
/// contract: a short or long result is a corruption error.
pub(crate) fn decompress_page(
    codec: CodecId,
    src: &[u8],
    dst: &mut Vec<u8>,
    uncompressed: usize,
) -> PgResult<()> {
    dst.clear();
    dst.try_reserve(uncompressed + PAD)
        .map_err(|_| Box::new(PgError::error("out of memory decompressing parquet page")))?;
    dst.resize(uncompressed, 0);
    let written = match codec {
        CodecId::Uncompressed => {
            // Callers slice the chunk buffer directly; reaching here means a
            // caller wanted an owned copy.
            if src.len() != uncompressed {
                return Err(decompress_failed(
                    "UNCOMPRESSED",
                    format!("page declares {uncompressed} bytes, holds {}", src.len()),
                ));
            }
            dst[..src.len()].copy_from_slice(src);
            src.len()
        }
        CodecId::Snappy => {
            let n = snap::raw::decompress_len(src)
                .map_err(|e| decompress_failed("SNAPPY", e.to_string()))?;
            if n != uncompressed {
                return Err(decompress_failed(
                    "SNAPPY",
                    format!("page declares {uncompressed} bytes, stream holds {n}"),
                ));
            }
            snap::raw::Decoder::new()
                .decompress(src, dst)
                .map_err(|e| decompress_failed("SNAPPY", e.to_string()))?
        }
        CodecId::Zstd => zstd_decompress(src, dst)?,
        CodecId::Lz4Raw => lz4_flex::block::decompress_into(src, dst)
            .map_err(|e| decompress_failed("LZ4_RAW", e.to_string()))?,
        other => {
            return Err(unsupported(format!("compression codec {}", other.name())))
        }
    };
    if written != uncompressed {
        return Err(decompress_failed(
            codec.name(),
            format!("page declares {uncompressed} bytes, decoded {written}"),
        ));
    }
    // Zeroed pad beyond the logical length (initialized capacity): kernels
    // that read ahead through raw pointers never touch uninitialized bytes.
    dst.resize(uncompressed + PAD, 0);
    // SAFETY: shrinking a Vec<u8> length over initialized bytes.
    unsafe { dst.set_len(uncompressed) };
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
fn zstd_decompress(src: &[u8], dst: &mut [u8]) -> PgResult<usize> {
    zstd::bulk::decompress_to_buffer(src, dst)
        .map_err(|e| decompress_failed("ZSTD", e.to_string()))
}

#[cfg(target_family = "wasm")]
fn zstd_decompress(_src: &[u8], _dst: &mut [u8]) -> PgResult<usize> {
    Err(unsupported("compression codec ZSTD (unavailable on this platform)".into()))
}
