//! RLE / bit-packed hybrid decoding (dictionary indices, definition levels,
//! RLE-encoded booleans) plus the PLAIN boolean bit reader.
//!
//! Kernel law for this hardware class: per-bit-width unrolled scalar kernels
//! decoding 8 values per iteration — the format's horizontal 8-value groups
//! cap SIMD gains, and the compiler vectorizes well-shaped unrolled scalar
//! code to intrinsics parity. Run headers are decoded branch-lean (a single
//! capped varint loop); short bit-packed runs re-enter the group loop rather
//! than a per-value path.

use types_error::{PgError, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT};

#[cold]
#[inline(never)]
pub(crate) fn corrupt_page(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("corrupt parquet page: {what}"))
            .with_sqlstate(ERRCODE_BAD_COPY_FILE_FORMAT),
    )
}

/// Unpack one 8-value group of `W`-bit values (LSB-first, little-endian bit
/// order per the format). `src` must hold at least `W` bytes.
#[inline(always)]
fn unpack8_narrow<const W: usize>(src: &[u8], out: &mut [u32; 8]) {
    // W <= 16: the whole group (8*W bits = W bytes) fits in a u128 window.
    debug_assert!(W >= 1 && W <= 16 && src.len() >= W);
    let mut acc: u128 = 0;
    for (i, &b) in src[..W].iter().enumerate() {
        acc |= (b as u128) << (8 * i);
    }
    let mask: u128 = (1u128 << W) - 1;
    for (i, o) in out.iter_mut().enumerate() {
        *o = ((acc >> (i * W)) & mask) as u32;
    }
}

/// Unpack one 8-value group of `w`-bit values for 17..=32-bit widths: each
/// value spans at most 5 bytes; extract through u64 windows over a padded
/// local copy (the group is exactly `w` bytes).
#[inline]
fn unpack8_wide(w: usize, src: &[u8], out: &mut [u32; 8]) {
    debug_assert!((17..=32).contains(&w) && src.len() >= w);
    let mut buf = [0u8; 40]; // 32-byte max group + u64 window slack
    buf[..w].copy_from_slice(&src[..w]);
    let mask: u64 = if w == 32 { u64::from(u32::MAX) } else { (1u64 << w) - 1 };
    for (i, o) in out.iter_mut().enumerate() {
        let bit = i * w;
        let byte = bit >> 3;
        let shift = bit & 7;
        let window = u64::from_le_bytes(buf[byte..byte + 8].try_into().expect("8-byte window"));
        *o = ((window >> shift) & mask) as u32;
    }
}

/// Dispatch to a monomorphized narrow kernel or the wide extractor.
#[inline]
fn unpack8(w: usize, src: &[u8], out: &mut [u32; 8]) {
    match w {
        1 => unpack8_narrow::<1>(src, out),
        2 => unpack8_narrow::<2>(src, out),
        3 => unpack8_narrow::<3>(src, out),
        4 => unpack8_narrow::<4>(src, out),
        5 => unpack8_narrow::<5>(src, out),
        6 => unpack8_narrow::<6>(src, out),
        7 => unpack8_narrow::<7>(src, out),
        8 => unpack8_narrow::<8>(src, out),
        9 => unpack8_narrow::<9>(src, out),
        10 => unpack8_narrow::<10>(src, out),
        11 => unpack8_narrow::<11>(src, out),
        12 => unpack8_narrow::<12>(src, out),
        13 => unpack8_narrow::<13>(src, out),
        14 => unpack8_narrow::<14>(src, out),
        15 => unpack8_narrow::<15>(src, out),
        16 => unpack8_narrow::<16>(src, out),
        _ => unpack8_wide(w, src, out),
    }
}

enum Run {
    /// Nothing buffered; the next header must be read.
    None,
    /// RLE run: `left` more copies of `value`.
    Rle { value: u32, left: usize },
    /// Bit-packed region: `groups_left` full 8-value groups still unread from
    /// the stream, plus up to 8 already-unpacked values in `buf`.
    Packed { groups_left: usize, buf: [u32; 8], buf_pos: usize, buf_len: usize },
}

/// Decoder state over an RLE/bit-packed hybrid stream. Holds offsets only —
/// the page buffer is passed into every call (self-borrow-free so the page
/// owner can hold both).
pub(crate) struct HybridState {
    pub pos: usize,
    end: usize,
    width: usize,
    run: Run,
}

impl HybridState {
    /// `[pos, end)` is the hybrid region inside the page buffer; `width` the
    /// value bit width (0 allowed: every value is implicitly zero).
    pub fn new(pos: usize, end: usize, width: usize) -> PgResult<HybridState> {
        if width > 32 {
            return Err(corrupt_page("bit width above 32"));
        }
        Ok(HybridState { pos, end, width, run: Run::None })
    }

    /// Fill `out` with exactly `out.len()` values or fail (a page never ends
    /// mid-batch: the caller sized the batch from the page's value count).
    pub fn fill(&mut self, buf: &[u8], out: &mut [u32]) -> PgResult<()> {
        if self.width == 0 {
            out.fill(0);
            return Ok(());
        }
        let mut n = 0usize;
        while n < out.len() {
            match &mut self.run {
                Run::Rle { value, left } => {
                    let take = (*left).min(out.len() - n);
                    out[n..n + take].fill(*value);
                    n += take;
                    *left -= take;
                    if *left == 0 {
                        self.run = Run::None;
                    }
                }
                Run::Packed { groups_left, buf: gbuf, buf_pos, buf_len } => {
                    if buf_pos < buf_len {
                        let take = (*buf_len - *buf_pos).min(out.len() - n);
                        out[n..n + take].copy_from_slice(&gbuf[*buf_pos..*buf_pos + take]);
                        *buf_pos += take;
                        n += take;
                        continue;
                    }
                    if *groups_left == 0 {
                        self.run = Run::None;
                        continue;
                    }
                    // Bulk lane: whole groups straight into the output.
                    let want_groups = (out.len() - n) / 8;
                    let mut groups = (*groups_left).min(want_groups);
                    *groups_left -= groups;
                    let w = self.width;
                    while groups > 0 {
                        let src = buf
                            .get(self.pos..self.pos + w)
                            .ok_or_else(|| corrupt_page("bit-packed run overruns page"))?;
                        let dst: &mut [u32] = &mut out[n..n + 8];
                        let mut tmp = [0u32; 8];
                        unpack8(w, src, &mut tmp);
                        dst.copy_from_slice(&tmp);
                        self.pos += w;
                        n += 8;
                        groups -= 1;
                    }
                    if n < out.len() && *groups_left > 0 {
                        // Tail: unpack one group into the staging buffer.
                        let src = buf
                            .get(self.pos..self.pos + w)
                            .ok_or_else(|| corrupt_page("bit-packed run overruns page"))?;
                        let mut tmp = [0u32; 8];
                        unpack8(w, src, &mut tmp);
                        self.pos += w;
                        *groups_left -= 1;
                        *gbuf = tmp;
                        *buf_pos = 0;
                        *buf_len = 8;
                    }
                }
                Run::None => {
                    self.read_header(buf)?;
                }
            }
        }
        Ok(())
    }

    fn read_header(&mut self, buf: &[u8]) -> PgResult<()> {
        if self.pos >= self.end {
            return Err(corrupt_page("levels or indices end before the declared count"));
        }
        // Capped ULEB128 (u32 range is ample: run lengths are per-page).
        let mut header: u64 = 0;
        let mut shift = 0u32;
        loop {
            let Some(&b) = buf.get(self.pos).filter(|_| self.pos < self.end) else {
                return Err(corrupt_page("run header overruns page"));
            };
            self.pos += 1;
            header |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 35 {
                return Err(corrupt_page("run header varint too long"));
            }
        }
        if header & 1 == 0 {
            // RLE run: fixed-width little-endian value, (width+7)/8 bytes.
            let len = usize::try_from(header >> 1)
                .map_err(|_| corrupt_page("run length out of range"))?;
            if len == 0 {
                // Zero-length runs are tolerated (some writers emit them as
                // padding); loop back for the next header.
                self.run = Run::None;
                return Ok(());
            }
            let nbytes = self.width.div_ceil(8);
            if self.pos + nbytes > self.end {
                return Err(corrupt_page("RLE run value overruns page"));
            }
            let mut v: u32 = 0;
            for i in 0..nbytes {
                v |= u32::from(buf[self.pos + i]) << (8 * i);
            }
            self.pos += nbytes;
            let mask: u32 =
                if self.width == 32 { u32::MAX } else { (1u32 << self.width) - 1 };
            if v > mask {
                return Err(corrupt_page("RLE run value exceeds the bit width"));
            }
            self.run = Run::Rle { value: v, left: len };
        } else {
            let groups = usize::try_from(header >> 1)
                .map_err(|_| corrupt_page("bit-packed group count out of range"))?;
            if groups == 0 {
                self.run = Run::None;
                return Ok(());
            }
            // Structural bound: the groups must physically fit in the region.
            if groups > (self.end - self.pos) / self.width + 1 {
                return Err(corrupt_page("bit-packed run larger than page"));
            }
            self.run =
                Run::Packed { groups_left: groups, buf: [0u32; 8], buf_pos: 0, buf_len: 0 };
        }
        Ok(())
    }
}

/// Definition levels for a flat OPTIONAL column (max_def = 1): fill a
/// present/absent mask. Returns the number of present (non-null) values.
/// The all-set single-RLE-run page never materializes per-value work beyond
/// the mask fill itself.
pub(crate) fn def_levels_max1(
    state: &mut HybridState,
    buf: &[u8],
    out: &mut [u32],
) -> PgResult<usize> {
    state.fill(buf, out)?;
    let mut present = 0usize;
    // Branch-free accumulate: levels are 0/1 by construction (width 1); any
    // other value is structurally impossible from a 1-bit stream.
    for &v in out.iter() {
        present += v as usize;
    }
    Ok(present)
}

/// PLAIN-encoded BOOLEAN values: bit-packed LSB-first, no run structure.
pub(crate) struct BoolBits {
    pub pos: usize,
    bit: u32,
}

impl BoolBits {
    pub fn new(pos: usize) -> BoolBits {
        BoolBits { pos, bit: 0 }
    }

    pub fn fill(&mut self, buf: &[u8], end: usize, out: &mut [bool]) -> PgResult<()> {
        for o in out.iter_mut() {
            let Some(&b) = buf.get(self.pos).filter(|_| self.pos < end) else {
                return Err(corrupt_page("boolean values end before the declared count"));
            };
            *o = (b >> self.bit) & 1 == 1;
            self.bit += 1;
            if self.bit == 8 {
                self.bit = 0;
                self.pos += 1;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packed_stream(width: usize, values: &[u32]) -> Vec<u8> {
        // Encoder used by tests only: one bit-packed run covering all values
        // (padded to a full group with zeros).
        let groups = values.len().div_ceil(8);
        let mut out = Vec::new();
        let header = ((groups as u64) << 1) | 1;
        let mut h = header;
        loop {
            let b = (h & 0x7f) as u8;
            h >>= 7;
            if h == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
        let mut bitbuf: u64 = 0;
        let mut bits = 0usize;
        for g in 0..groups {
            for i in 0..8 {
                let v = values.get(g * 8 + i).copied().unwrap_or(0) as u64;
                bitbuf |= (v & ((1u64 << width) - 1).max(1)) << bits;
                bits += width;
                while bits >= 8 {
                    out.push((bitbuf & 0xff) as u8);
                    bitbuf >>= 8;
                    bits -= 8;
                }
            }
        }
        if bits > 0 {
            out.push((bitbuf & 0xff) as u8);
        }
        out
    }

    #[test]
    fn bitpacked_roundtrip_all_widths() {
        for width in 1..=32usize {
            let mask: u64 = if width == 32 { u32::MAX as u64 } else { (1u64 << width) - 1 };
            let values: Vec<u32> =
                (0..37u64).map(|i| ((i.wrapping_mul(2654435761)) & mask) as u32).collect();
            let buf = packed_stream(width, &values);
            let mut st = HybridState::new(0, buf.len(), width).unwrap();
            let mut out = vec![0u32; values.len()];
            st.fill(&buf, &mut out).unwrap();
            assert_eq!(out, values, "width {width}");
        }
    }

    #[test]
    fn rle_runs_and_mixed() {
        // RLE run of 500 x 7 (width 3), then bit-packed 0..8.
        let mut buf = vec![];
        buf.extend_from_slice(&[0xe8, 0x07]); // varint(500 << 1 = 1000)
        buf.push(7); // value, 1 byte for width 3
        buf.extend_from_slice(&packed_stream(3, &[0, 1, 2, 3, 4, 5, 6, 7]));
        let mut st = HybridState::new(0, buf.len(), 3).unwrap();
        let mut out = vec![0u32; 508];
        st.fill(&buf, &mut out).unwrap();
        assert!(out[..500].iter().all(|&v| v == 7));
        assert_eq!(&out[500..], &[0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn truncated_streams_error_cleanly() {
        let buf = packed_stream(16, &(0..64u32).collect::<Vec<_>>());
        for cut in 0..buf.len() {
            let mut st = HybridState::new(0, cut, 16).unwrap();
            let mut out = vec![0u32; 64];
            assert!(st.fill(&buf[..cut], &mut out).is_err(), "cut {cut}");
        }
    }

    #[test]
    fn bool_bits() {
        let buf = [0b1010_0110u8, 0b0000_0001];
        let mut st = BoolBits::new(0);
        let mut out = [false; 9];
        st.fill(&buf, 2, &mut out).unwrap();
        assert_eq!(
            out,
            [false, true, true, false, false, true, false, true, true]
        );
    }
}
