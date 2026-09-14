
use ::miniz_oxide::deflate::core::{compress, create_comp_flags_from_zip_params, CompressorOxide};
use ::miniz_oxide::deflate::core::TDEFLStatus;
use ::miniz_oxide::inflate::core::{decompress, inflate_flags, DecompressorOxide};
use ::miniz_oxide::inflate::TINFLStatus;

fn miniz_level(level: i32) -> u8 {
    level.clamp(1, 9) as u8
}

fn deflate(data: &[u8], level: i32, zlib_header: bool) -> Vec<u8> {
    let flags = create_comp_flags_from_zip_params(miniz_level(level) as i32, zlib_header as i32, 0);
    let mut comp = CompressorOxide::new(flags);
    let mut out = vec![0u8; data.len() + data.len() / 2 + 128];
    let mut in_pos = 0usize;
    let mut out_pos = 0usize;
    loop {
        let (status, consumed, written) = compress(
            &mut comp,
            &data[in_pos..],
            &mut out[out_pos..],
            ::miniz_oxide::deflate::core::TDEFLFlush::Finish,
        );
        in_pos += consumed;
        out_pos += written;
        match status {
            TDEFLStatus::Done => break,
            TDEFLStatus::Okay => {
                if out_pos == out.len() {
                    let extra = out.len();
                    out.resize(out.len() + extra, 0);
                }
            }
            _ => break,
        }
    }
    out.truncate(out_pos);
    out
}

/// ZIP — raw DEFLATE (RFC 1951), no zlib header.
pub fn deflate_raw(data: &[u8], level: i32) -> Vec<u8> {
    deflate(data, level, false)
}

/// ZLIB — zlib-wrapped DEFLATE (RFC 1950).
pub fn deflate_zlib(data: &[u8], level: i32) -> Vec<u8> {
    deflate(data, level, true)
}

/// Upper bound on the total decompressed output of a single PGP compressed
/// packet. This mirrors upstream pgcrypto: `mbuf.c`'s `decompress_read` streams
/// inflate output into an `MBuf` whose `prepare_room`/`repalloc` growth is
/// capped at `MaxAllocSize`, so an over-large expansion fails cleanly with a
/// catchable "invalid memory alloc request size" ERROR rather than exhausting
/// memory. Our port grows a plain `Vec` on the global allocator, which would
/// otherwise `handle_alloc_error`/abort the whole (single-process) server, so
/// we enforce the same bound explicitly. A decompression bomb — a small DEFLATE
/// stream that inflates to gigabytes — hits this cap and is rejected via the
/// pgcrypto error path instead of allocating without limit.
const MAX_DECOMPRESSED: usize = ::mcx::MAX_ALLOC_SIZE;

/// zlib return codes as C's `inflate()` reports them for the same input:
/// truncated input at Z_FINISH is Z_BUF_ERROR, everything else Z_DATA_ERROR.
const Z_DATA_ERROR: i32 = -3;
const Z_BUF_ERROR: i32 = -5;

fn inflate_bounded(
    data: &[u8],
    zlib_header: bool,
    max_output: usize,
) -> Result<Vec<u8>, Option<i32>> {
    let mut inf = DecompressorOxide::new();
    let mut flags = inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF;
    if zlib_header {
        flags |= inflate_flags::TINFL_FLAG_PARSE_ZLIB_HEADER;
    }
    let mut out: Vec<u8> = Vec::with_capacity((data.len() * 4 + 256).min(max_output.max(1)));
    let mut in_pos = 0usize;
    loop {
        let out_len = out.len();
        // Never let the output buffer grow past the cap. If the decompressor
        // still wants to emit more once we are at the ceiling, the input is a
        // decompression bomb: bail out via the (catchable) error path.
        if out_len >= max_output {
            return Err(None);
        }
        let target = out.capacity().max(out_len + 256).min(max_output);
        out.resize(target, 0);
        let (status, consumed, written) =
            decompress(&mut inf, &data[in_pos..], &mut out, out_len, flags);
        in_pos += consumed;
        out.truncate(out_len + written);
        match status {
            TINFLStatus::Done => return Ok(out),
            TINFLStatus::HasMoreOutput => {
                if out.len() >= max_output {
                    return Err(None);
                }
                let cap = out.capacity();
                let want = cap.max(256).min(max_output - out.len());
                out.reserve(want);
            }
            TINFLStatus::NeedsMoreInput | TINFLStatus::FailedCannotMakeProgress => {
                return Err(Some(Z_BUF_ERROR))
            }
            _ => return Err(Some(Z_DATA_ERROR)),
        }
    }
}

fn inflate(data: &[u8], zlib_header: bool) -> Result<Vec<u8>, Option<i32>> {
    inflate_bounded(data, zlib_header, MAX_DECOMPRESSED)
}

pub fn inflate_raw(data: &[u8]) -> Result<Vec<u8>, Option<i32>> {
    inflate(data, false)
}

pub fn inflate_zlib(data: &[u8]) -> Result<Vec<u8>, Option<i32>> {
    inflate(data, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legitimate_message_round_trips() {
        // A moderately large, highly compressible payload must still inflate
        // fully — the cap only rejects abusive expansion, not valid data.
        let plain = vec![0x5au8; 4 * 1024 * 1024];
        let comp = deflate_raw(&plain, 6);
        assert!(comp.len() < plain.len());
        let back = inflate_raw(&comp).expect("valid stream must decompress");
        assert_eq!(back, plain);
    }

    #[test]
    fn decompression_bomb_is_rejected() {
        // 8 MiB of zeros deflate to a tiny stream but inflate far past a small
        // cap. With the bound in place the inflate must error rather than
        // allocate without limit.
        let plain = vec![0u8; 8 * 1024 * 1024];
        let comp = deflate_raw(&plain, 6);
        assert!(comp.len() < 64 * 1024);
        // Cap the output well below the true inflated size.
        assert_eq!(inflate_bounded(&comp, false, 64 * 1024).unwrap_err(), None);
    }

    #[test]
    fn inflate_failures_carry_zlib_codes() {
        assert_eq!(inflate_raw(&[0xff, 0xff, 0xff, 0xff]).unwrap_err(), Some(-3));
        assert_eq!(inflate_zlib(&[0x00, 0x00]).unwrap_err(), Some(-3));
        let comp = deflate_raw(b"truncated stream payload", 6);
        assert_eq!(inflate_raw(&comp[..comp.len() / 2]).unwrap_err(), Some(-5));
    }
}
