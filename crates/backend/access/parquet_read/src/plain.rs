//! PLAIN-encoded value decoding: fixed-width little-endian copies (native on
//! this hardware class) and the BYTE_ARRAY length-walk.

use types_error::{PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE};

use crate::rle::corrupt_page;

/// Fixed-width PLAIN values: append `n` values read at `*pos`. The generic
/// width is monomorphized per caller; the copy is a straight little-endian
/// load (aarch64/x86 native).
macro_rules! plain_fixed {
    ($name:ident, $ty:ty) => {
        pub(crate) fn $name(
            buf: &[u8],
            end: usize,
            pos: &mut usize,
            n: usize,
            out: &mut Vec<$ty>,
        ) -> PgResult<()> {
            const W: usize = core::mem::size_of::<$ty>();
            let need = n.checked_mul(W).ok_or_else(|| corrupt_page("value count overflow"))?;
            if *pos + need > end || end > buf.len() {
                return Err(corrupt_page("values end before the declared count"));
            }
            out.try_reserve(n).map_err(|_| {
                Box::new(PgError::error("out of memory decoding parquet values"))
            })?;
            let src = &buf[*pos..*pos + need];
            for chunk in src.chunks_exact(W) {
                out.push(<$ty>::from_le_bytes(chunk.try_into().expect("exact chunk")));
            }
            *pos += need;
            Ok(())
        }
    };
}

plain_fixed!(plain_i32, i32);
plain_fixed!(plain_i64, i64);
plain_fixed!(plain_f32, f32);
plain_fixed!(plain_f64, f64);

#[cold]
#[inline(never)]
pub(crate) fn invalid_utf8(column: &str) -> Box<PgError> {
    Box::new(
        PgError::error("invalid byte sequence for encoding \"UTF8\" in parquet string column")
            .with_sqlstate(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
            .with_detail(format!("Column \"{column}\".")),
    )
}

/// Validate one string value: branch-lean ASCII pre-check (8-byte OR
/// accumulation of high bits), full validator only on non-ASCII payloads.
#[inline]
pub(crate) fn utf8_ok(v: &[u8]) -> bool {
    let mut acc: u8 = 0;
    let mut it = v.chunks_exact(8);
    for c in it.by_ref() {
        let w = u64::from_le_bytes(c.try_into().expect("exact chunk"));
        acc |= ((w & 0x8080_8080_8080_8080) != 0) as u8;
    }
    for &b in it.remainder() {
        acc |= b & 0x80;
    }
    if acc == 0 {
        return true;
    }
    simdutf8::basic::from_utf8(v).is_ok()
}

/// BYTE_ARRAY PLAIN length-walk: `n` values of [4-byte LE length][bytes],
/// appended to (`offsets`, `arena`). `offsets` carries one end-offset per
/// value (arena start = previous end). With `validate_utf8`, each value is
/// checked (ASCII fast path first).
#[allow(clippy::too_many_arguments)]
pub(crate) fn plain_byte_array(
    buf: &[u8],
    end: usize,
    pos: &mut usize,
    n: usize,
    offsets: &mut Vec<u32>,
    arena: &mut Vec<u8>,
    validate_utf8: bool,
    column: &str,
) -> PgResult<()> {
    offsets
        .try_reserve(n)
        .map_err(|_| Box::new(PgError::error("out of memory decoding parquet strings")))?;
    for _ in 0..n {
        let Some(lb) = buf.get(*pos..*pos + 4).filter(|_| *pos + 4 <= end) else {
            return Err(corrupt_page("string values end before the declared count"));
        };
        let len = u32::from_le_bytes(lb.try_into().expect("4-byte slice")) as usize;
        let start = *pos + 4;
        if start + len > end {
            return Err(corrupt_page("string value overruns page"));
        }
        let v = &buf[start..start + len];
        if validate_utf8 && !utf8_ok(v) {
            return Err(invalid_utf8(column));
        }
        if arena.len() + len > u32::MAX as usize {
            return Err(corrupt_page("string batch exceeds 4GB"));
        }
        arena
            .try_reserve(len)
            .map_err(|_| Box::new(PgError::error("out of memory decoding parquet strings")))?;
        arena.extend_from_slice(v);
        offsets.push(arena.len() as u32);
        *pos = start + len;
    }
    Ok(())
}
