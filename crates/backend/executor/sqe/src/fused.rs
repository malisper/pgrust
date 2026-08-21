//! FusedCodes — the verdict/consumer-fused code-unpack face (phase-2A
//! addition (e), from lane 2B's cross-lane finding: `decode_codes` ≈
//! 30ns/row is the shared floor of every dict-lane kernel; the fix is to
//! fuse the per-code consumer INTO the unpack loop instead of
//! materializing a u32 code array and re-walking it).
//!
//! Built ENTIRELY on public reader surface — `OpenPart::stream_directory`
//! + `extent_bytes`/`unwrapped_extent_bytes` — plus the frozen §6.11 block
//! layout (`{base: u32, width: u8, pad[3], packed bits LSB-first}`,
//! granule block found through the section frame table; transcribed from
//! `pgrc2_codec::dictcodes`, whose helpers are private). This is the
//! harness-shim prototype of the reader face; the product face would live
//! as a `StreamCursor::fold_codes` beside `decode_codes`.

use crate::bank::{binding, Bank};
use pgrc2_format::enc::{EncodingId, Wrapper};
use pgrc2_format::geom::FRAMES_PER_GRANULE;
use pgrc2_format::part::{StreamRole, StreamSectionHdr, STREAM_SECTION_HDR_LEN};
use pgrc2_read::openpart::SegBuf;
use pgrc2_read::ReadError;

const BLOCK_HDR_LEN: usize = 8;

struct FusedExtent {
    buf: SegBuf,
    payload_off: usize,
    payload_end: usize,
    ft_off: usize,
    frame_count: u32,
    granule_start: u32,
    granule_count: u32,
}

pub struct FusedCodes {
    extents: Vec<FusedExtent>,
}

/// Load one column's Values stream extents (unwrapping wrapped sections
/// through the same binding the cursor uses), refusing unless the elected
/// encoding matches. Shared by both hand-rolled faces.
fn open_extents(bank: &Bank, pi: usize, attno: u32, encoding: u16) -> Option<(Vec<FusedExtent>, u8)> {
    let part = &bank.parts[pi];
    let dir = part.stream_directory().ok()?;
    // [json-rung1] lane columns resolve to (parent attno, path_ord).
    let (sa, po) = bank.stream_key(pi, attno);
    let ps = dir.lookup(sa, po, StreamRole::Values)?;
    if ps.entry.encoding != encoding {
        return None;
    }
    let wrapper = Wrapper::from_u8(ps.entry.wrapper).ok()?;
    let mut extents = Vec::with_capacity(ps.extents.len());
    for (ei, rec) in ps.extents.iter().enumerate() {
        let buf = if wrapper == Wrapper::None {
            part.extent_bytes(&ps.entry, rec, ei as u32).ok()?
        } else {
            let uw = binding()
                .unwrappers
                .iter()
                .find(|u| u.wrapper() == wrapper)?;
            part.unwrapped_extent_bytes(&ps.entry, rec, ei as u32, &mut |raw| {
                let hdr = StreamSectionHdr::decode(raw).map_err(ReadError::Format)?;
                uw.unwrap_section(&hdr, raw).map_err(ReadError::Format)
            })
            .ok()?
        };
        let hdr = StreamSectionHdr::decode(buf.bytes()).ok()?;
        if hdr.gcount_table_off != 0 {
            return None; // root streams only in this prototype
        }
        let payload_end = if hdr.frame_table_off != 0 {
            hdr.frame_table_off as usize
        } else {
            return None; // both target encodings carry frame tables
        };
        extents.push(FusedExtent {
            buf,
            payload_off: STREAM_SECTION_HDR_LEN,
            payload_end,
            ft_off: hdr.frame_table_off as usize,
            frame_count: hdr.frame_count,
            granule_start: rec.granule_start,
            granule_count: rec.granule_count,
        });
    }
    Some((extents, ps.entry.width))
}

fn locate(extents: &[FusedExtent], g: u32) -> (&FusedExtent, u32) {
    let e = extents
        .iter()
        .find(|e| g >= e.granule_start && g < e.granule_start + e.granule_count)
        .expect("granule not covered by any extent");
    (e, g - e.granule_start)
}

/// Frame-table entry lookup (payload-relative start of frame `fidx`).
fn frame_at(e: &FusedExtent, fidx: u32) -> usize {
    assert!(fidx < e.frame_count, "frame index out of table");
    let fto = e.ft_off + fidx as usize * 4;
    u32::from_le_bytes(e.buf.bytes()[fto..fto + 4].try_into().unwrap()) as usize
}

impl FusedCodes {
    /// `None` when the column's Values stream is not DictCodes-encoded in
    /// this part (callers fall back to the cursor faces).
    pub fn open(bank: &Bank, pi: usize, attno: u32) -> Option<FusedCodes> {
        let (extents, _) = open_extents(bank, pi, attno, EncodingId::DictCodes.as_u16())?;
        Some(FusedCodes { extents })
    }

    fn block(&self, g: u32) -> (u32, u8, &[u8]) {
        let (e, gie) = locate(&self.extents, g);
        let first = frame_at(e, gie * FRAMES_PER_GRANULE);
        let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
        let bh = &payload[first - BLOCK_HDR_LEN..first];
        let base = u32::from_le_bytes(bh[..4].try_into().unwrap());
        let width = bh[4];
        assert!(width <= 32, "dict code width");
        (base, width, &payload[first..])
    }

    /// The fused face: unpack granule `g`'s codes and hand each (row, code)
    /// straight to `f` — no code array exists.
    #[inline]
    pub fn fold(&self, g: u32, values: usize, mut f: impl FnMut(usize, u32)) {
        let (base, width, bits) = self.block(g);
        match width {
            0 => {
                for r in 0..values {
                    f(r, base);
                }
            }
            8 => {
                for (r, &b) in bits[..values].iter().enumerate() {
                    f(r, base + b as u32);
                }
            }
            16 => {
                for (r, c) in bits[..values * 2].chunks_exact(2).enumerate() {
                    f(r, base + u16::from_le_bytes(c.try_into().unwrap()) as u32);
                }
            }
            // pgrc21-widths: the 24/32 byte-aligned arms (direct indexing,
            // no shift/mask) — reached only on byte-width-sealed banks.
            24 => {
                for (r, c) in bits[..values * 3].chunks_exact(3).enumerate() {
                    f(r, base + (c[0] as u32 | (c[1] as u32) << 8 | (c[2] as u32) << 16));
                }
            }
            32 => {
                for (r, c) in bits[..values * 4].chunks_exact(4).enumerate() {
                    f(r, base + u32::from_le_bytes(c.try_into().unwrap()));
                }
            }
            w => {
                let wu = w as usize;
                let mask = (1u64 << w) - 1;
                let need = (values * wu).div_ceil(8);
                let bits = &bits[..need.min(bits.len())];
                let fast = if bits.len() >= 8 {
                    (((bits.len() - 8) * 8 + 7) / wu + 1).min(values)
                } else {
                    0
                };
                let group: usize = if wu <= 14 { 4 } else if wu <= 28 { 2 } else { 1 };
                let mut i = 0usize;
                if group > 1 {
                    while i + group <= fast {
                        let bitoff = i * wu;
                        let byte = bitoff >> 3;
                        let win = u64::from_le_bytes(bits[byte..byte + 8].try_into().unwrap())
                            >> (bitoff & 7);
                        for k in 0..group {
                            f(i + k, base + ((win >> (k * wu)) & mask) as u32);
                        }
                        i += group;
                    }
                }
                while i < values {
                    let bitoff = i * wu;
                    let byte = bitoff >> 3;
                    let end = (byte + 8).min(bits.len());
                    let mut w8 = [0u8; 8];
                    w8[..end - byte].copy_from_slice(&bits[byte..end]);
                    let raw = u64::from_le_bytes(w8) >> (bitoff & 7);
                    f(i, base + (raw & mask) as u32);
                    i += 1;
                }
            }
        }
    }

    /// The counting specialization (the hot-shape consumer): counts[code]
    /// += 1 fused into the unpack loop.
    #[inline]
    pub fn count_into(&self, g: u32, values: usize, counts: &mut [u32]) {
        self.fold(g, values, |_, code| counts[code as usize] += 1);
    }

    /// ADDITIVE (lane hot-shape): same contract as `fold`, raw-pointer unpack.
    /// `fold`'s group loop pays a slice bounds check + `try_into` per
    /// 2-code window and a `[0u8; 8]` copy per tail code; RESULTS-optdecode
    /// finding #6 named the unpack loop itself as the hot-shape floor. Here the
    /// fast region does ONE unaligned 8-byte load per code (independent
    /// loads, no cross-iteration chain); the window always holds the whole
    /// code because width ≤ 32 and shift < 8, and the fast bound proves
    /// the load stays inside the payload slice.
    #[inline]
    pub fn fold_ptr(&self, g: u32, values: usize, mut f: impl FnMut(usize, u32)) {
        let (base, width, bits) = self.block(g);
        match width {
            0 | 8 | 16 => self.fold(g, values, f),
            w => {
                let wu = w as usize;
                let mask = (1u64 << w) - 1;
                let need = (values * wu).div_ceil(8);
                let bits = &bits[..need.min(bits.len())];
                let n = bits.len();
                // max i with (i*wu)>>3 + 8 <= n  (same bound as fold()).
                let fast = if n >= 8 {
                    (((n - 8) * 8 + 7) / wu + 1).min(values)
                } else {
                    0
                };
                let p = bits.as_ptr();
                let mut i = 0usize;
                while i < fast {
                    // SAFETY: i < fast ⇒ (i*wu)/8 + 8 ≤ n (proved above).
                    let bitoff = i * wu;
                    let win = unsafe {
                        std::ptr::read_unaligned(p.add(bitoff >> 3) as *const u64)
                    } >> (bitoff & 7);
                    f(i, base + (win & mask) as u32);
                    i += 1;
                }
                while i < values {
                    let bitoff = i * wu;
                    let byte = bitoff >> 3;
                    let end = (byte + 8).min(n);
                    let mut w8 = [0u8; 8];
                    w8[..end - byte].copy_from_slice(&bits[byte..end]);
                    let raw = u64::from_le_bytes(w8) >> (bitoff & 7);
                    f(i, base + (raw & mask) as u32);
                    i += 1;
                }
            }
        }
    }

    /// ADDITIVE (lane opt-decode): the granule's code-domain zone key —
    /// every code in granule `g` lies in [base, base + (1 << width))
    /// (width == 0 ⇒ the single code `base`). Lets a verdict consumer skip
    /// the whole unpack when the range holds no matching dict code (the
    /// zone-map idea replayed in CODE space).
    #[inline]
    pub fn code_zone(&self, g: u32) -> (u32, u8) {
        let (base, width, _) = self.block(g);
        (base, width)
    }
}

// ---------------------------------------------------------------------------
// FusedInts — hand-rolled BYTE_FOR decode (spec §6.10: frames of 1024,
// frame = [ref: i64 LE][deltas: n × W LE unsigned], value = ref +(wrap)
// delta). The Michael directive's item 2: the on-disk format is the
// contract; this face decodes int streams straight from the format bytes
// at NATIVE width (or fused into a consumer), bypassing the u64 datum ABI.
// VALIDITY LAW: this face ignores the validity stream — callers must hold
// the null-free proof (assert_null_free) before electing it; the library
// decode variant of every kernel remains the correctness oracle.
// ---------------------------------------------------------------------------

const FRAME_REF_LEN: usize = 8;
const FRAME_VALUES: usize = 1024; // == pgrc2_format::geom::FRAME_VALUES

pub struct FusedInts {
    extents: Vec<FusedExtent>,
    /// Elected delta width (bytes, 1..=8) from the stream entry.
    pub width: u8,
}

impl FusedInts {
    /// `None` unless the column's Values stream is ByteFor-encoded in this
    /// part (Const/FFOR/Verbatim parts fall back to the cursor faces).
    pub fn open(bank: &Bank, pi: usize, attno: u32) -> Option<FusedInts> {
        let (extents, width) = open_extents(bank, pi, attno, EncodingId::ByteFor.as_u16())?;
        Some(FusedInts { extents, width })
    }

    /// Fused per-value fold: hand each (row, value-as-u64-datum) to `f`
    /// straight out of the frame walk — no datum lane is materialized.
    #[inline]
    pub fn fold_u64(&self, g: u32, values: usize, mut f: impl FnMut(usize, u64)) {
        let (e, gie) = locate(&self.extents, g);
        let fbase = gie * FRAMES_PER_GRANULE;
        let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
        let w = self.width as usize;
        let mut r0 = 0usize;
        let mut fi = 0u32;
        while r0 < values {
            let n = (values - r0).min(FRAME_VALUES);
            let fs = frame_at(e, fbase + fi);
            let refw = u64::from_le_bytes(payload[fs..fs + FRAME_REF_LEN].try_into().unwrap());
            let deltas = &payload[fs + FRAME_REF_LEN..fs + FRAME_REF_LEN + n * w];
            macro_rules! walk {
                ($W:expr) => {{
                    for (j, c) in deltas.chunks_exact($W).enumerate() {
                        let mut b = [0u8; 8];
                        b[..$W].copy_from_slice(c);
                        f(r0 + j, refw.wrapping_add(u64::from_le_bytes(b)));
                    }
                }};
            }
            match w {
                1 => walk!(1),
                2 => walk!(2),
                3 => walk!(3),
                4 => walk!(4),
                5 => walk!(5),
                6 => walk!(6),
                7 => walk!(7),
                8 => walk!(8),
                _ => panic!("bytefor width {w}"),
            }
            r0 += n;
            fi += 1;
        }
    }

    /// ADDITIVE (lane hot-shape, round 3): fold `counts[value - lo] += 1` over the
    /// NONZERO int2 values of granule `g` — zero values are dropped, the
    /// caller's filter is on the key. Frames whose ref is 0 (any frame that
    /// holds a zero: ByteFor's ref is the frame min) walk their width-1
    /// delta bytes as u64 words: a zero word is eight zero rows retired in
    /// one compare, and inside a nonzero word the nonzero bytes are found by
    /// a SWAR high-bit mask + trailing_zeros, so the scatter (the
    /// store-forward chain of `counts[k] += 1`) is paid ONLY on the rows
    /// that survive the filter. Every other frame (all-nonzero, or a wider
    /// delta election) takes the general fold with the same drop-zero rule.
    /// `lo` is the column's exact stats minimum (domain base of `counts`).
    #[inline]
    pub fn count_nonzero_i16(&self, g: u32, values: usize, lo: i64, counts: &mut [u32]) {
        let (e, gie) = locate(&self.extents, g);
        let fbase = gie * FRAMES_PER_GRANULE;
        let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
        let w = self.width as usize;
        let mut r0 = 0usize;
        let mut fi = 0u32;
        while r0 < values {
            let n = (values - r0).min(FRAME_VALUES);
            let fs = frame_at(e, fbase + fi);
            let refw = u64::from_le_bytes(payload[fs..fs + FRAME_REF_LEN].try_into().unwrap());
            let deltas = &payload[fs + FRAME_REF_LEN..fs + FRAME_REF_LEN + n * w];
            if w == 1 && refw == 0 {
                // value == delta byte (0..=255, sign-free as int2).
                let mut words = deltas.chunks_exact(8);
                for c in &mut words {
                    let x = u64::from_le_bytes(c.try_into().unwrap());
                    if x == 0 {
                        continue;
                    }
                    // High bit of each byte set iff the byte is nonzero.
                    let t = (x & 0x7f7f_7f7f_7f7f_7f7f).wrapping_add(0x7f7f_7f7f_7f7f_7f7f);
                    let mut m = (t | x) & 0x8080_8080_8080_8080;
                    while m != 0 {
                        let k = (m.trailing_zeros() >> 3) as usize;
                        counts[(c[k] as i64 - lo) as usize] += 1;
                        m &= m - 1;
                    }
                }
                for &b in words.remainder() {
                    if b != 0 {
                        counts[(b as i64 - lo) as usize] += 1;
                    }
                }
            } else {
                macro_rules! walk {
                    ($W:expr) => {{
                        for c in deltas.chunks_exact($W) {
                            let mut b = [0u8; 8];
                            b[..$W].copy_from_slice(c);
                            let v = refw.wrapping_add(u64::from_le_bytes(b)) as i16 as i64;
                            if v != 0 {
                                counts[(v - lo) as usize] += 1;
                            }
                        }
                    }};
                }
                match w {
                    1 => walk!(1),
                    2 => walk!(2),
                    3 => walk!(3),
                    4 => walk!(4),
                    5 => walk!(5),
                    6 => walk!(6),
                    7 => walk!(7),
                    8 => walk!(8),
                    _ => panic!("bytefor width {w}"),
                }
            }
            r0 += n;
            fi += 1;
        }
    }

    /// Native-width decode into i16 lanes, straight from format bytes (the
    /// W2 face the u64 ABI blocks — charter 1b upgraded per directive).
    #[inline]
    pub fn decode_i16_into(&self, g: u32, values: usize, out: &mut Vec<i16>) {
        if out.len() < values {
            out.resize(values, 0);
        }
        let (e, gie) = locate(&self.extents, g);
        let fbase = gie * FRAMES_PER_GRANULE;
        let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
        let w = self.width as usize;
        let mut r0 = 0usize;
        let mut fi = 0u32;
        while r0 < values {
            let n = (values - r0).min(FRAME_VALUES);
            let fs = frame_at(e, fbase + fi);
            let refw = u64::from_le_bytes(payload[fs..fs + FRAME_REF_LEN].try_into().unwrap());
            let deltas = &payload[fs + FRAME_REF_LEN..fs + FRAME_REF_LEN + n * w];
            match w {
                1 => {
                    let r16 = refw as i16;
                    for (j, &d) in deltas.iter().enumerate() {
                        out[r0 + j] = r16.wrapping_add(d as i16);
                    }
                }
                2 => {
                    let r16 = refw as i16;
                    for (j, c) in deltas.chunks_exact(2).enumerate() {
                        out[r0 + j] =
                            r16.wrapping_add(u16::from_le_bytes(c.try_into().unwrap()) as i16);
                    }
                }
                _ => {
                    // wider elections still narrow exactly for int2 data
                    for (j, c) in deltas.chunks_exact(w).enumerate() {
                        let mut b = [0u8; 8];
                        b[..w].copy_from_slice(c);
                        out[r0 + j] = refw.wrapping_add(u64::from_le_bytes(b)) as i16;
                    }
                }
            }
            r0 += n;
            fi += 1;
        }
    }

    /// u64-datum decode into a resident buffer (for consumers needing
    /// random row access alongside another fused stream).
    #[inline]
    pub fn decode_u64_into(&self, g: u32, values: usize, out: &mut Vec<u64>) {
        if out.len() < values {
            out.resize(values, 0);
        }
        self.fold_u64(g, values, |r, v| out[r] = v);
    }
}

// ---------------------------------------------------------------------------
// WordCol — hand-rolled BYVAL-INT GATHER face (lane hot-shape, 2026-08-15): the
// survivor-only decode the reader lacks (pain point #7 — `decode_sel` is
// a per-point face). Reads §6.10 ByteFor frames, §6.7 Verbatim word
// slices and §6.9 Const records straight off the part-resident section
// images; `gather` completes ONLY the rows in a survivor list, `full`
// completes a whole granule. Root streams, CLASS_BYVAL only; any other
// election returns None and the caller falls back to the cursor face.
// VALIDITY LAW as FusedInts: callers hold the null-free proof; the
// library-path variants of every kernel remain the oracle.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum WordKind {
    ByteFor,
    Verbatim,
    Const,
}

pub struct WordCol {
    kind: WordKind,
    extents: Vec<FusedExtent>,
    /// ByteFor delta width / Verbatim word width, in bytes.
    width: usize,
    /// Verbatim: STREAMF_SIGNED ⇒ sign-extend the word to the datum.
    signed: bool,
    /// Const: the datum word per extent (index-aligned with `extents`).
    consts: Vec<u64>,
}

/// Word-class datum extension (spec §6.7) — twin of the format crate's
/// private `extend_word`.
#[inline(always)]
fn extend_word(raw: u64, width: usize, signed: bool) -> u64 {
    if !signed || width == 8 {
        return raw;
    }
    let shift = 64 - width as u32 * 8;
    (((raw << shift) as i64) >> shift) as u64
}

#[inline(always)]
fn read_le_n(bytes: &[u8]) -> u64 {
    let mut w = [0u8; 8];
    w[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(w)
}

impl WordCol {
    /// `None` unless the column's Values stream is a root ByteFor /
    /// Verbatim(byval) / Const stream in this part.
    pub fn open(bank: &Bank, pi: usize, attno: u32) -> Option<WordCol> {
        use pgrc2_format::class::CLASS_BYVAL;
        use pgrc2_format::part::STREAMF_SIGNED;
        let part = &bank.parts[pi];
        let dir = part.stream_directory().ok()?;
        // [json-rung1] lane columns resolve to (parent attno, path_ord).
        let (sa, po) = bank.stream_key(pi, attno);
        let ps = dir.lookup(sa, po, StreamRole::Values)?;
        let enc = ps.entry.encoding;
        let kind = if enc == EncodingId::ByteFor.as_u16() {
            WordKind::ByteFor
        } else if enc == EncodingId::Verbatim.as_u16() {
            WordKind::Verbatim
        } else if enc == EncodingId::Const.as_u16() {
            WordKind::Const
        } else {
            return None;
        };
        if ps.entry.class != CLASS_BYVAL {
            return None;
        }
        let width = ps.entry.width as usize;
        if kind != WordKind::Const && !(1..=8).contains(&width) {
            return None;
        }
        let (extents, _) = open_extents_any(bank, pi, attno, enc)?;
        if kind == WordKind::ByteFor && extents.iter().any(|e| e.ft_off == 0) {
            return None; // ByteFor frames are found through the frame table
        }
        let mut consts = Vec::new();
        if kind == WordKind::Const {
            for e in &extents {
                let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
                if payload.len() < 8 || payload[0] > 1 {
                    return None;
                }
                let len = u32::from_le_bytes(payload[4..8].try_into().unwrap()) as usize;
                if payload[0] == 1 {
                    consts.push(0);
                    continue;
                }
                if len != 8 || payload.len() < 16 {
                    return None;
                }
                consts.push(u64::from_le_bytes(payload[8..16].try_into().unwrap()));
            }
        }
        Some(WordCol {
            kind,
            extents,
            width,
            signed: ps.entry.flags & STREAMF_SIGNED != 0,
            consts,
        })
    }

    #[inline(always)]
    fn locate_idx(&self, g: u32) -> (usize, u32) {
        let i = self
            .extents
            .iter()
            .position(|e| g >= e.granule_start && g < e.granule_start + e.granule_count)
            .expect("granule not covered by any extent");
        (i, g - self.extents[i].granule_start)
    }

    /// Complete the whole granule (`rows` values) into `out[..rows]`.
    #[inline]
    pub fn full(&self, g: u32, rows: usize, out: &mut [u64]) {
        let (ei, gie) = self.locate_idx(g);
        let e = &self.extents[ei];
        let out = &mut out[..rows];
        match self.kind {
            WordKind::Const => {
                let v = self.consts[ei];
                for o in out.iter_mut() {
                    *o = v;
                }
            }
            WordKind::Verbatim => {
                let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
                let w = self.width;
                let base = gie as usize * GRANULE_ROWS_USIZE * w;
                let body = &payload[base..base + rows * w];
                let signed = self.signed;
                macro_rules! walk {
                    ($W:expr) => {{
                        for (o, c) in out.iter_mut().zip(body.chunks_exact($W)) {
                            let mut b = [0u8; 8];
                            b[..$W].copy_from_slice(c);
                            *o = extend_word(u64::from_le_bytes(b), $W, signed);
                        }
                    }};
                }
                match w {
                    1 => walk!(1),
                    2 => walk!(2),
                    4 => walk!(4),
                    8 => walk!(8),
                    _ => {
                        for (o, c) in out.iter_mut().zip(body.chunks_exact(w)) {
                            *o = extend_word(read_le_n(c), w, signed);
                        }
                    }
                }
            }
            WordKind::ByteFor => {
                let fbase = gie * FRAMES_PER_GRANULE;
                let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
                let w = self.width;
                let mut r0 = 0usize;
                let mut fi = 0u32;
                while r0 < rows {
                    let n = (rows - r0).min(FRAME_VALUES);
                    let fs = frame_at(e, fbase + fi);
                    let refw =
                        u64::from_le_bytes(payload[fs..fs + FRAME_REF_LEN].try_into().unwrap());
                    let deltas = &payload[fs + FRAME_REF_LEN..fs + FRAME_REF_LEN + n * w];
                    let o = &mut out[r0..r0 + n];
                    macro_rules! walk {
                        ($W:expr) => {{
                            for (o, c) in o.iter_mut().zip(deltas.chunks_exact($W)) {
                                let mut b = [0u8; 8];
                                b[..$W].copy_from_slice(c);
                                *o = refw.wrapping_add(u64::from_le_bytes(b));
                            }
                        }};
                    }
                    match w {
                        1 => {
                            for (o, &d) in o.iter_mut().zip(deltas.iter()) {
                                *o = refw.wrapping_add(d as u64);
                            }
                        }
                        2 => walk!(2),
                        3 => walk!(3),
                        4 => walk!(4),
                        5 => walk!(5),
                        6 => walk!(6),
                        7 => walk!(7),
                        8 => walk!(8),
                        _ => panic!("bytefor width {w}"),
                    }
                    r0 += n;
                    fi += 1;
                }
            }
        }
    }

    /// Complete ONLY the rows in `sel` (granule-local ordinals, ascending
    /// or not) into `out[..sel.len()]`, selection order.
    #[inline]
    pub fn gather(&self, g: u32, rows: usize, sel: &[u16], out: &mut [u64]) {
        let (ei, gie) = self.locate_idx(g);
        let e = &self.extents[ei];
        let out = &mut out[..sel.len()];
        match self.kind {
            WordKind::Const => {
                let v = self.consts[ei];
                for o in out.iter_mut() {
                    *o = v;
                }
            }
            WordKind::Verbatim => {
                let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
                let w = self.width;
                let base = gie as usize * GRANULE_ROWS_USIZE * w;
                let body = &payload[base..base + rows * w];
                let signed = self.signed;
                macro_rules! walk {
                    ($W:expr) => {{
                        for (o, &r) in out.iter_mut().zip(sel.iter()) {
                            let off = r as usize * $W;
                            let mut b = [0u8; 8];
                            b[..$W].copy_from_slice(&body[off..off + $W]);
                            *o = extend_word(u64::from_le_bytes(b), $W, signed);
                        }
                    }};
                }
                match w {
                    1 => walk!(1),
                    2 => walk!(2),
                    4 => walk!(4),
                    8 => walk!(8),
                    _ => {
                        for (o, &r) in out.iter_mut().zip(sel.iter()) {
                            let off = r as usize * w;
                            *o = extend_word(read_le_n(&body[off..off + w]), w, signed);
                        }
                    }
                }
            }
            WordKind::ByteFor => {
                let fbase = gie * FRAMES_PER_GRANULE;
                let payload = &e.buf.bytes()[e.payload_off..e.payload_end];
                let w = self.width;
                let nf = rows.div_ceil(FRAME_VALUES);
                // frame refs + delta-body starts, resolved once per granule
                let mut refs = [0u64; FRAMES_PER_GRANULE as usize];
                let mut starts = [0usize; FRAMES_PER_GRANULE as usize];
                for f in 0..nf {
                    let fs = frame_at(e, fbase + f as u32);
                    refs[f] = u64::from_le_bytes(payload[fs..fs + FRAME_REF_LEN].try_into().unwrap());
                    starts[f] = fs + FRAME_REF_LEN;
                }
                macro_rules! walk {
                    ($W:expr) => {{
                        for (o, &r) in out.iter_mut().zip(sel.iter()) {
                            let r = r as usize;
                            let f = r / FRAME_VALUES;
                            let off = starts[f] + (r % FRAME_VALUES) * $W;
                            let mut b = [0u8; 8];
                            b[..$W].copy_from_slice(&payload[off..off + $W]);
                            *o = refs[f].wrapping_add(u64::from_le_bytes(b));
                        }
                    }};
                }
                match w {
                    1 => {
                        for (o, &r) in out.iter_mut().zip(sel.iter()) {
                            let r = r as usize;
                            let f = r / FRAME_VALUES;
                            *o = refs[f].wrapping_add(payload[starts[f] + (r % FRAME_VALUES)] as u64);
                        }
                    }
                    2 => walk!(2),
                    3 => walk!(3),
                    4 => walk!(4),
                    5 => walk!(5),
                    6 => walk!(6),
                    7 => walk!(7),
                    8 => walk!(8),
                    _ => panic!("bytefor width {w}"),
                }
            }
        }
    }
}

const GRANULE_ROWS_USIZE: usize = pgrc2_format::geom::GRANULE_ROWS as usize;

/// `open_extents` without the frame-table requirement (Verbatim word and
/// Const streams carry none): payload_end falls back to the section end.
fn open_extents_any(
    bank: &Bank,
    pi: usize,
    attno: u32,
    encoding: u16,
) -> Option<(Vec<FusedExtent>, u8)> {
    let part = &bank.parts[pi];
    let dir = part.stream_directory().ok()?;
    // [json-rung1] lane columns resolve to (parent attno, path_ord).
    let (sa, po) = bank.stream_key(pi, attno);
    let ps = dir.lookup(sa, po, StreamRole::Values)?;
    if ps.entry.encoding != encoding {
        return None;
    }
    let wrapper = Wrapper::from_u8(ps.entry.wrapper).ok()?;
    let mut extents = Vec::with_capacity(ps.extents.len());
    for (ei, rec) in ps.extents.iter().enumerate() {
        let buf = if wrapper == Wrapper::None {
            part.extent_bytes(&ps.entry, rec, ei as u32).ok()?
        } else {
            let uw = binding()
                .unwrappers
                .iter()
                .find(|u| u.wrapper() == wrapper)?;
            part.unwrapped_extent_bytes(&ps.entry, rec, ei as u32, &mut |raw| {
                let hdr = StreamSectionHdr::decode(raw).map_err(ReadError::Format)?;
                uw.unwrap_section(&hdr, raw).map_err(ReadError::Format)
            })
            .ok()?
        };
        let hdr = StreamSectionHdr::decode(buf.bytes()).ok()?;
        if hdr.gcount_table_off != 0 {
            return None; // root streams only
        }
        let payload_end = if hdr.frame_table_off != 0 {
            hdr.frame_table_off as usize
        } else {
            buf.bytes().len()
        };
        extents.push(FusedExtent {
            buf,
            payload_off: STREAM_SECTION_HDR_LEN,
            payload_end,
            ft_off: hdr.frame_table_off as usize,
            frame_count: hdr.frame_count,
            granule_start: rec.granule_start,
            granule_count: rec.granule_count,
        });
    }
    Some((extents, ps.entry.width))
}
