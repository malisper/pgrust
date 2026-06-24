//! PGP compression (pgp-compress.c): ZIP = raw DEFLATE (no zlib wrapper),
//! ZLIB = zlib-wrapped DEFLATE. Driven over `miniz_oxide`.

use ::miniz_oxide::deflate::core::{compress, create_comp_flags_from_zip_params, CompressorOxide};
use ::miniz_oxide::deflate::core::TDEFLStatus;
use ::miniz_oxide::inflate::core::{decompress, inflate_flags, DecompressorOxide};
use ::miniz_oxide::inflate::TINFLStatus;

/// Map a PGP compress-level (1..9) to a miniz level (0..10).
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

fn inflate(data: &[u8], zlib_header: bool) -> Result<Vec<u8>, ()> {
    let mut inf = DecompressorOxide::new();
    let mut flags = inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF;
    if zlib_header {
        flags |= inflate_flags::TINFL_FLAG_PARSE_ZLIB_HEADER;
    }
    let mut out: Vec<u8> = Vec::with_capacity(data.len() * 4 + 256);
    let mut in_pos = 0usize;
    loop {
        let out_len = out.len();
        let target = out.capacity().max(out_len + 256);
        out.resize(target, 0);
        let (status, consumed, written) =
            decompress(&mut inf, &data[in_pos..], &mut out, out_len, flags);
        in_pos += consumed;
        out.truncate(out_len + written);
        match status {
            TINFLStatus::Done => return Ok(out),
            TINFLStatus::HasMoreOutput => {
                let cap = out.capacity();
                out.reserve(cap.max(256));
            }
            TINFLStatus::NeedsMoreInput => return Err(()),
            _ => return Err(()),
        }
    }
}

pub fn inflate_raw(data: &[u8]) -> Result<Vec<u8>, ()> {
    inflate(data, false)
}

pub fn inflate_zlib(data: &[u8]) -> Result<Vec<u8>, ()> {
    inflate(data, true)
}
