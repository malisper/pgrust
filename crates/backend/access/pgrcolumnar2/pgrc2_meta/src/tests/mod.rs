//! The M3-E exit-slice suite (`lanev3-m3-chunks.md` §5 M3-E, every clause):
//!
//! - coarse-key law born-RED (type-level pin + released-assert tooth) —
//!   `verdict_tests`;
//! - exact zone transforms vs decode oracle across every charter-§5 class —
//!   `key_tests` + `builder_tests`;
//! - constant-lowering fail-closed (non-inline varlena header, NaN float
//!   range, rescale abstention) — `lower_tests`;
//! - SMA/PSMA build+probe property tests (candidates always cover matches)
//!   — `aux_tests`;
//! - bloom arming-policy tests (unclustered ∧ NDV floor) — `aux_tests`;
//! - typed footer aggregates vs decode oracle + unit-grain
//!   verdict-vs-decode (born-RED both directions) — `builder_tests` +
//!   `verdict_tests`;
//! - NDV-register merge associativity/commutativity — `aux_tests`;
//! - byte- AND char-length text stats pinned — `builder_tests`;
//! - predicate cache HIT/BUILT witnesses + refuse-and-rebuild —
//!   `pcache_tests`;
//! - layout/golden pins — `pin_tests`.

use crate::format::abi::{
    ByteArena, CodecRegistry, DecodeOut, EncodeInput, GranuleEncoder, KernelCtx, KernelKey,
};
use crate::format::class::StorageClass;
use crate::format::enc::Wrapper;
use crate::format::part::{OverflowSink, StreamSectionHdr, StreamSectionWriter, STREAMF_SIGNED};
use crate::format::verbatim::{encode_validity_bitmap, reference_vtables, VerbatimEncoder};
use crate::format::wire::varlena_header_4b_u;

mod aux_tests;
mod builder_tests;
mod key_tests;
mod lower_tests;
mod pcache_tests;
mod pin_tests;
mod verdict_tests;

// ---------------------------------------------------------------------------
// deterministic RNG (splitmix64 walk — no external dependencies, no
// process entropy: every test corpus is reproducible from its seed)
// ---------------------------------------------------------------------------

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        crate::hash::mix64(self.0)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0);
        self.next() % n
    }
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
    /// Fisher–Yates.
    pub fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            v.swap(i, j);
        }
    }
}

// ---------------------------------------------------------------------------
// test column: owned images + datum words (the EncodeInput currency)
// ---------------------------------------------------------------------------

/// One test column's rows: `None` = NULL. Pointer-class images live in
/// individually-boxed buffers so datum pointers stay stable.
pub struct TestColumn {
    pub class: StorageClass,
    pub datums: Vec<u64>,
    pub validity: Option<Vec<u64>>,
    images: Vec<Box<[u8]>>,
}

impl TestColumn {
    /// Build from per-row values: byval rows carry datum words, pointer
    /// rows carry image bytes (Fixed: the N bytes; Varlena: the PAYLOAD —
    /// the 4B-U header is added here).
    pub fn new(class: StorageClass, rows: &[Option<TestValue>]) -> TestColumn {
        let mut datums = Vec::with_capacity(rows.len());
        let mut images: Vec<Box<[u8]>> = Vec::new();
        let mut any_null = false;
        let mut validity_words = vec![0u64; rows.len().div_ceil(64)];
        for (r, v) in rows.iter().enumerate() {
            match v {
                None => {
                    any_null = true;
                    datums.push(0);
                }
                Some(val) => {
                    validity_words[r / 64] |= 1 << (r % 64);
                    match (class, val) {
                        (
                            StorageClass::ByvalWord { .. }
                            | StorageClass::F32
                            | StorageClass::F64
                            | StorageClass::Bool,
                            TestValue::Word(w),
                        ) => datums.push(*w),
                        (StorageClass::Fixed { len }, TestValue::Bytes(b)) => {
                            assert_eq!(b.len(), len as usize, "fixed image length");
                            let img: Box<[u8]> = b.clone().into_boxed_slice();
                            datums.push(img.as_ptr() as u64);
                            images.push(img);
                        }
                        (StorageClass::VarlenaVerbatim, TestValue::Bytes(payload)) => {
                            let mut img = Vec::with_capacity(4 + payload.len());
                            img.extend_from_slice(
                                &varlena_header_4b_u(payload.len() as u32).to_le_bytes(),
                            );
                            img.extend_from_slice(payload);
                            let img: Box<[u8]> = img.into_boxed_slice();
                            datums.push(img.as_ptr() as u64);
                            images.push(img);
                        }
                        _ => panic!("value shape does not match class"),
                    }
                }
            }
        }
        TestColumn {
            class,
            datums,
            validity: if any_null { Some(validity_words) } else { None },
            images,
        }
    }

    pub fn rows(&self) -> u32 {
        self.datums.len() as u32
    }

    pub fn input(&self) -> EncodeInput<'_> {
        EncodeInput {
            class: self.class,
            rows: self.rows(),
            datums: &self.datums,
            validity: self.validity.as_deref(),
        }
    }

    pub fn valid(&self, r: u32) -> bool {
        self.input().valid(r)
    }

    /// The row's value view for oracles: datum word or image payload bytes
    /// (Fixed: the N bytes; Varlena: the payload).
    pub fn value(&self, r: u32) -> Option<OracleValue<'_>> {
        if !self.valid(r) {
            return None;
        }
        Some(match self.class {
            StorageClass::ByvalWord { .. }
            | StorageClass::F32
            | StorageClass::F64
            | StorageClass::Bool => OracleValue::Word(self.datums[r as usize]),
            StorageClass::Fixed { len } => {
                let p = self.datums[r as usize] as *const u8;
                // SAFETY: this column built the image and owns it.
                OracleValue::Bytes(unsafe { core::slice::from_raw_parts(p, len as usize) })
            }
            StorageClass::VarlenaVerbatim => {
                let p = self.datums[r as usize] as *const u8;
                // SAFETY: this column built the image and owns it.
                let header = u32::from_le_bytes(
                    unsafe { core::slice::from_raw_parts(p, 4) }
                        .try_into()
                        .unwrap(),
                );
                let payload_len = (header >> 2) as usize - 4;
                OracleValue::Bytes(unsafe { core::slice::from_raw_parts(p.add(4), payload_len) })
            }
        })
    }

    #[allow(dead_code)]
    fn keep_images(&self) -> usize {
        self.images.len()
    }
}

/// A row value for column construction.
#[derive(Debug, Clone, PartialEq)]
pub enum TestValue {
    Word(u64),
    Bytes(Vec<u8>),
}

/// A row value view for oracles.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OracleValue<'a> {
    Word(u64),
    Bytes(&'a [u8]),
}

// ---------------------------------------------------------------------------
// reference-codec decode loop (the decode oracle's transport: encode via
// the frozen Verbatim reference, decode via the six-face ABI, hand the
// DECODED datums back — closing every oracle over the ABI)
// ---------------------------------------------------------------------------

/// Encode one granule through the reference Verbatim codec and decode it
/// back; returns (decoded datums, arena backing) — the arena must outlive
/// any use of pointer-class decoded datums.
pub struct DecodedGranule {
    pub datums: Vec<u64>,
    #[allow(dead_code)]
    arena: Vec<u8>,
}

pub fn roundtrip_through_reference(col: &TestColumn) -> DecodedGranule {
    let input = col.input();
    let key = KernelKey {
        encoding: crate::format::enc::EncodingId::Verbatim.as_u16(),
        class: input.class.id(),
        width: input.class.width(),
    };
    let mut section = Vec::new();
    let mut overflow = Vec::new();
    {
        let mut w =
            StreamSectionWriter::begin(&mut section, key.encoding, key.width, Wrapper::None)
                .expect("writer begins");
        let mut ovf = OverflowSink::new(&mut overflow);
        let mut enc = VerbatimEncoder { class: input.class };
        enc.encode_granule(&input, &mut w, &mut ovf)
            .expect("encodes");
        enc.finish_stream(&mut w).expect("finishes");
        w.finish(false).expect("closes");
    }
    let hdr = StreamSectionHdr::decode(&section).expect("header");
    let frame_table = hdr.frame_table(&section).expect("frame table");
    let mut validity_bytes = Vec::new();
    encode_validity_bitmap(input.validity, input.rows, &mut validity_bytes);
    let leaked: &'static [&'static crate::format::abi::CodecVtable] =
        Box::leak(Box::new(reference_vtables()));
    let reg = CodecRegistry::new(leaked);
    let vt = reg.resolve(key).expect("kernel resolves");
    let ctx = KernelCtx {
        key,
        flags: if input.class.signed() {
            STREAMF_SIGNED
        } else {
            0
        },
        fixed_len: input.class.fixed_len(),
        bytes: &section,
        frame_table: frame_table.as_deref(),
        granule: 0,
        granule_in_extent: 0,
        rows: input.rows,
        values: input.rows,
        validity_bytes: if input.validity.is_some() {
            Some(&validity_bytes)
        } else {
            None
        },
        overflow: if overflow.is_empty() {
            None
        } else {
            Some(&overflow)
        },
        dict: None,
    };
    let mut datums = vec![0u64; input.rows as usize];
    let mut arena = vec![0u8; 1 << 20];
    {
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(&mut arena),
        };
        let n = (vt.decode_full)(&ctx, &mut out).expect("decode_full");
        assert_eq!(n, input.rows);
    }
    DecodedGranule { datums, arena }
}

// ---------------------------------------------------------------------------
// numeric wire-image generator (test-side oracle encoder for the parser)
// ---------------------------------------------------------------------------

/// Encode `value × 10^-scale` as a PG numeric VARLENA PAYLOAD (long form).
pub fn numeric_payload(value: i128, scale: u32) -> Vec<u8> {
    let neg = value < 0;
    let mag = value.unsigned_abs();
    // Pad the fraction to whole base-10000 groups.
    let scale4 = scale.div_ceil(4) * 4;
    let mut m = mag;
    for _ in 0..(scale4 - scale) {
        m *= 10;
    }
    let frac_groups = (scale4 / 4) as i32;
    // Base-10000 digits of m, most significant first.
    let mut digits: Vec<u16> = Vec::new();
    if m == 0 {
        // PG zero: no digits, weight 0.
        let mut out = Vec::new();
        out.extend_from_slice(&0u16.to_le_bytes()); // sign/dscale: positive, dscale 0
        out.extend_from_slice(&0i16.to_le_bytes()); // weight
                                                    // dscale actually carries `scale`; rewrite the header word:
        let header: u16 = (scale as u16) & 0x3FFF;
        out[0..2].copy_from_slice(&header.to_le_bytes());
        return out;
    }
    while m > 0 {
        digits.push((m % 10_000) as u16);
        m /= 10_000;
    }
    digits.reverse();
    let weight = digits.len() as i32 - 1 - frac_groups;
    let sign: u16 = if neg { 0x4000 } else { 0x0000 };
    let header: u16 = sign | ((scale as u16) & 0x3FFF);
    let mut out = Vec::new();
    out.extend_from_slice(&header.to_le_bytes());
    out.extend_from_slice(&(weight as i16).to_le_bytes());
    for d in digits {
        out.extend_from_slice(&d.to_le_bytes());
    }
    out
}

/// Encode the same value in the SHORT numeric header form when it fits
/// (sign bit 0x2000, 6-bit dscale at bit 7, 7-bit two's-complement
/// weight); panics if it does not fit — tests choose fitting inputs.
pub fn numeric_payload_short(value: i128, scale: u32) -> Vec<u8> {
    let long = numeric_payload(value, scale);
    let weight = i16::from_le_bytes(long[2..4].try_into().unwrap()) as i32;
    assert!(scale <= 0x3F, "short dscale is 6 bits");
    assert!((-64..=63).contains(&weight), "short weight is 7 bits");
    let neg = value < 0;
    let mut header: u16 = 0x8000;
    if neg {
        header |= 0x2000;
    }
    header |= ((scale as u16) & 0x3F) << 7;
    // 7-bit two's-complement weight (bit 6 = sign, bits 0..6 = value).
    header |= (weight as u16) & 0x7F;
    let mut out = Vec::new();
    out.extend_from_slice(&header.to_le_bytes());
    out.extend_from_slice(&long[4..]);
    out
}

/// PG numeric specials.
pub fn numeric_payload_nan() -> Vec<u8> {
    0xC000u16.to_le_bytes().to_vec()
}
pub fn numeric_payload_pinf() -> Vec<u8> {
    0xD000u16.to_le_bytes().to_vec()
}
pub fn numeric_payload_ninf() -> Vec<u8> {
    0xF000u16.to_le_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// oracle comparators (independent PG-semantics implementations)
// ---------------------------------------------------------------------------

use core::cmp::Ordering;

/// PG float8 comparison: NaN = NaN, NaN > everything, -0.0 = +0.0.
pub fn pg_f64_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).expect("non-NaN"),
    }
}

/// memcmp-with-length (bytea/uuid/C-collation text order).
pub fn memcmp_order(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}
