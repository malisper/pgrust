//! Shared test scaffolding for the M3-C exit-slice suites (chunk table §5):
//! deterministic corpora, the built-stream harness, and decode helpers.
//!
//! Suites:
//! - [`roundtrip`] — per-encoding bit-exact round-trips (adversarial + fuzz
//!   corpora) + the ALP oracle differential + wrapped-section round-trips;
//! - [`selprop`] — `decode_sel ≡ decode_full ∘ select` under permuted
//!   selections, every codec;
//! - [`corrupt`] — bounds-validated decode on corrupt inputs (seeded
//!   corruption teeth; typed error, never UB) + unknown/reserved refusals;
//! - [`strview`] — the §7b varlena-shape + ≥8-alignment pins (born-RED:
//!   the checker proves it can fire);
//! - [`electiontests`] — gate math, typed demotions, input-decidability;
//! - [`jsonb_lanes`] — shred → lane-encode → decode → reconstruct
//!   byte-identity + the NULL trichotomy;
//! - [`dispatch_shape`] — fn-pointer distinctness per width (the S4
//!   pow2-switch pin), key normalization, registry coverage;
//! - [`layout`] — size pins on the structs that price engagement.

use crate::election::{encode_stream, StreamBuild};
use pgrc2_format::abi::{
    ByteArena, DecodeOut, EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection,
};
use pgrc2_format::class::StorageClass;
use pgrc2_format::part::{StreamSectionHdr, STREAMF_SIGNED};
use pgrc2_format::verbatim::encode_validity_bitmap;

mod corrupt;
mod dispatch_shape;
mod electiontests;
mod fsstmoc;
mod jsonb_lanes;
mod kernel_diff;
mod layout;
mod roundtrip;
mod sample_elect;
mod selprop;
mod strview;

/// Deterministic splitmix64 (failures reproduce).
pub fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// An 8-aligned byte buffer modeling the reader substrate (`SegBuf`: dict
/// sections are Arc'd u64 buffers, so entry alignment is absolute). Test
/// fixtures that hand `DictSections` to kernels directly use this — the
/// zero-copy gather emits pointers into the fixture, and the §7b alignment
/// pins are only meaningful over an 8-aligned base.
pub struct Aligned8 {
    words: Vec<u64>,
    len: usize,
}

impl Aligned8 {
    pub fn from_bytes(v: &[u8]) -> Aligned8 {
        let mut words = vec![0u64; v.len().div_ceil(8)];
        // SAFETY: u8 view of an exclusively owned u64 buffer; alignment
        // only loosens and `v.len() <= words.len() * 8`.
        unsafe {
            core::ptr::copy_nonoverlapping(v.as_ptr(), words.as_mut_ptr() as *mut u8, v.len());
        }
        Aligned8 { words, len: v.len() }
    }
}

impl core::ops::Deref for Aligned8 {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: immutable u8 view; `len <= words.len() * 8`.
        unsafe { core::slice::from_raw_parts(self.words.as_ptr() as *const u8, self.len) }
    }
}

/// One granule's owned input data.
pub struct GranuleData {
    pub rows: u32,
    pub datums: Vec<u64>,
    pub validity: Option<Vec<u64>>,
}

impl GranuleData {
    pub fn input(&self, class: StorageClass) -> EncodeInput<'_> {
        EncodeInput {
            class,
            rows: self.rows,
            datums: &self.datums,
            validity: self.validity.as_deref(),
        }
    }
}

/// Validity patterns the corpora cycle through.
pub fn validity_pattern(kind: u32, rows: u32, seed: &mut u64) -> Option<Vec<u64>> {
    match kind % 3 {
        0 => None,
        1 => {
            // Sparse nulls (~1/16).
            let mut words = vec![u64::MAX; rows.div_ceil(64) as usize];
            for r in 0..rows {
                if splitmix(seed) % 16 == 0 {
                    words[(r / 64) as usize] &= !(1u64 << (r % 64));
                }
            }
            Some(words)
        }
        _ => {
            // Dense nulls (~3/4).
            let mut words = vec![0u64; rows.div_ceil(64) as usize];
            for r in 0..rows {
                if splitmix(seed) % 4 == 0 {
                    words[(r / 64) as usize] |= 1u64 << (r % 64);
                }
            }
            Some(words)
        }
    }
}

/// A built stream extent + everything ctx construction needs.
pub struct Built {
    pub buf: Vec<u8>,
    pub ovf: Vec<u8>,
    pub build: StreamBuild,
    pub frame_table: Option<Vec<u32>>,
    pub flags: u16,
    pub fixed_len: u32,
    /// Per-granule (rows, validity bitmap bytes).
    pub granules: Vec<(u32, Option<Vec<u8>>)>,
}

/// Build one stream through the production driver (verify ON — every test
/// stream passes the election quadruple's fixed leg before any suite even
/// looks at it).
pub fn build_stream(
    enc: &mut dyn GranuleEncoder,
    granules: &[EncodeInput<'_>],
    fixed_len: u32,
    signed: bool,
) -> Built {
    build_stream_opts(enc, granules, fixed_len, signed, true)
}

pub fn build_stream_opts(
    enc: &mut dyn GranuleEncoder,
    granules: &[EncodeInput<'_>],
    fixed_len: u32,
    signed: bool,
    verify: bool,
) -> Built {
    let mut buf = Vec::new();
    let mut ovf = Vec::new();
    let build = encode_stream(
        &mut buf,
        &mut ovf,
        enc,
        granules,
        false,
        crate::registry(),
        verify,
        fixed_len,
        signed,
    )
    .expect("encode_stream");
    let hdr = StreamSectionHdr::decode(&buf[build.section_range.clone()]).expect("hdr");
    let frame_table = hdr
        .frame_table(&buf[build.section_range.clone()])
        .expect("frame table");
    let gmeta = granules
        .iter()
        .map(|g| {
            let bits = g.validity.map(|_| {
                let mut b = Vec::new();
                encode_validity_bitmap(g.validity, g.rows, &mut b);
                b
            });
            (g.rows, bits)
        })
        .collect();
    Built {
        buf,
        ovf,
        build,
        frame_table,
        flags: if signed { STREAMF_SIGNED } else { 0 },
        fixed_len,
        granules: gmeta,
    }
}

impl Built {
    pub fn section(&self) -> &[u8] {
        &self.buf[self.build.section_range.clone()]
    }

    pub fn key(&self) -> KernelKey {
        self.build.key
    }

    pub fn ctx<'a>(&'a self, g: u32) -> KernelCtx<'a> {
        self.ctx_with(g, self.key(), self.section(), self.frame_table.as_deref())
    }

    pub fn ctx_with<'a>(
        &'a self,
        g: u32,
        key: KernelKey,
        section: &'a [u8],
        frame_table: Option<&'a [u32]>,
    ) -> KernelCtx<'a> {
        let (rows, ref bits) = self.granules[g as usize];
        KernelCtx {
            key,
            flags: self.flags,
            fixed_len: self.fixed_len,
            bytes: section,
            frame_table,
            granule: g,
            granule_in_extent: g,
            rows,
            values: rows,
            validity_bytes: bits.as_deref(),
            overflow: if self.ovf.is_empty() {
                None
            } else {
                Some(&self.ovf)
            },
            dict: None,
        }
    }
}

/// Decode granule `g` full; returns (datums, arena buffer — keep alive
/// while reading pointer-class datums).
pub fn dec_full(b: &Built, g: u32) -> (Vec<u64>, Vec<u8>) {
    dec_full_ctx(&b.ctx(g))
}

pub fn dec_full_ctx(ctx: &KernelCtx<'_>) -> (Vec<u64>, Vec<u8>) {
    let vt = crate::registry().resolve(ctx.key).expect("kernel resolves");
    let mut datums = vec![0u64; ctx.rows as usize];
    let mut arena_buf = vec![0u8; 1 << 20];
    let n = {
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(&mut arena_buf),
        };
        (vt.decode_full)(ctx, &mut out).expect("decode_full")
    };
    assert_eq!(n, ctx.rows);
    (datums, arena_buf)
}

pub fn dec_sel_ctx(ctx: &KernelCtx<'_>, rows: &[u16]) -> (Vec<u64>, Vec<u8>) {
    let vt = crate::registry().resolve(ctx.key).expect("kernel resolves");
    let mut datums = vec![0u64; rows.len()];
    let mut arena_buf = vec![0u8; 1 << 20];
    let sel = Selection { rows };
    sel.validate(ctx.rows).expect("selection valid");
    let n = {
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(&mut arena_buf),
        };
        (vt.decode_sel)(ctx, &sel, &mut out).expect("decode_sel")
    };
    assert_eq!(n as usize, rows.len());
    (datums, arena_buf)
}

/// Canonical-bytes equality for one row (validity-aware callers skip null
/// rows themselves).
pub fn canon_eq(class: StorageClass, a: u64, b: u64) -> bool {
    let mut sa = [0u8; 8];
    let mut sb = [0u8; 8];
    // SAFETY: test datums obey the pointer-class contract by construction.
    let ca = unsafe { pgrc2_format::abi::datum_canonical_bytes(class, a, &mut sa) }.expect("canon");
    let cb = unsafe { pgrc2_format::abi::datum_canonical_bytes(class, b, &mut sb) }.expect("canon");
    ca == cb
}

/// A permuted-but-ascending selection over `rows` rows (deterministic).
pub fn random_selection(rows: u32, keep_mod: u64, seed: &mut u64) -> Vec<u16> {
    (0..rows)
        .filter(|_| splitmix(seed) % keep_mod == 0)
        .map(|r| r as u16)
        .collect()
}

/// Int-family corpus shapes (shared by the roundtrip + selection suites).
pub fn roundtrip_int_corpus(shape: u32, rows: u32, seed: &mut u64) -> GranuleData {
    let validity = validity_pattern(shape, rows, seed);
    let datums = (0..rows)
        .map(|r| match shape % 6 {
            0 => 1_000_000 + (splitmix(seed) % 200) as u64, // narrow
            1 => (splitmix(seed) % 70_000) as u64,          // 2-4 byte deltas
            2 => splitmix(seed),                            // full u64
            3 => (-(splitmix(seed) as i64 % 100_000)) as u64, // negative
            4 => (r as u64) * 3 + splitmix(seed) % 4,       // near-sorted
            _ => {
                if splitmix(seed) % 128 == 0 {
                    splitmix(seed) // outlier spikes
                } else {
                    42
                }
            }
        })
        .collect();
    GranuleData {
        rows,
        datums,
        validity,
    }
}

/// F32 corpus shapes (SB-5: decimal-scaled — the `f4_decimal2` corpus
/// shape — plus specials incl. NaN payloads / ±0 / subnormals and raw-bit
/// noise; datum words are bit images zero-extended per spec §6.7).
pub fn roundtrip_f32_corpus(shape: u32, rows: u32, seed: &mut u64) -> GranuleData {
    let validity = validity_pattern(shape, rows, seed);
    let datums = (0..rows)
        .map(|_| {
            let v: f32 = match shape % 5 {
                0 => (splitmix(seed) % 100_000) as f32 / 100.0, // decimal2
                1 => f32::from_bits(splitmix(seed) as u32),     // raw bits
                2 => match splitmix(seed) % 8 {
                    0 => f32::NAN,
                    1 => f32::from_bits(0x7FC0_00AB), // NaN payload
                    2 => f32::from_bits(0xFFC0_0042), // -NaN payload
                    3 => f32::INFINITY,
                    4 => f32::NEG_INFINITY,
                    5 => -0.0,
                    6 => f32::from_bits(1), // smallest subnormal
                    _ => 1.25,
                },
                3 => (splitmix(seed) % 1000) as f32 / 10.0,
                _ => 2.5,
            };
            v.to_bits() as u64
        })
        .collect();
    GranuleData {
        rows,
        datums,
        validity,
    }
}

/// Float corpus shapes (decimal-like, raw-bit, adversarial specials incl.
/// NaN payloads — the B1/B2 bit-exactness families).
pub fn roundtrip_float_corpus(shape: u32, rows: u32, seed: &mut u64) -> GranuleData {
    let validity = validity_pattern(shape, rows, seed);
    let datums = (0..rows)
        .map(|_| {
            let v: f64 = match shape % 6 {
                0 => (splitmix(seed) % 1_000_000) as f64 / 100.0, // decimal-ish
                1 => f64::from_bits(splitmix(seed)),              // raw bits (RD/raw)
                2 => (splitmix(seed) % 1000) as f64 / 10.0,
                3 => match splitmix(seed) % 8 {
                    0 => f64::NAN,
                    1 => f64::from_bits(0x7FF0_0000_0000_00AB), // sNaN payload
                    2 => f64::from_bits(0xFFF8_0000_0000_0042), // -qNaN payload
                    3 => f64::INFINITY,
                    4 => f64::NEG_INFINITY,
                    5 => -0.0,
                    6 => f64::MIN_POSITIVE / 8.0, // denormal
                    _ => 12345.678,
                },
                4 => 1.0,
                _ => (splitmix(seed) as i64) as f64 * 1e-3,
            };
            v.to_bits()
        })
        .collect();
    GranuleData {
        rows,
        datums,
        validity,
    }
}
