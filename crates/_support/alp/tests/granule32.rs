//! f32 granule-API proofs (SB-5): bit-exact round-trip through
//! self-describing frames (NaN payloads / -0.0 / infinities / denormals
//! placed across frame boundaries), exact byte accounting, per-granule
//! election, determinism, and frame-decode hardening on corrupt bytes.

use alp::granule32::{self, GranuleEncodedF32, GRANULE_VALUES};
use alp::Scheme;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// Encode + decode + every structural invariant the writer relies on.
/// The decode comparison is the independent oracle: bit equality against
/// the INPUT, never against anything the encoder computed.
#[track_caller]
fn roundtrip(values: &[f32]) -> GranuleEncodedF32 {
    let enc = granule32::encode(values);
    let r = &enc.report;
    assert_eq!(enc.frames.len(), values.len().div_ceil(GRANULE_VALUES));
    assert_eq!(r.ngranules, enc.frames.len());
    assert_eq!(r.total_values, values.len());
    assert_eq!(r.raw_bytes, values.len() * 4);
    // frame_bytes is EXACT: the emitted frames' total length, nothing else.
    assert_eq!(r.frame_bytes, enc.frames.iter().map(|f| f.len()).sum::<usize>());
    // Raw arm accounting is closed-form: header per granule + bit images.
    assert_eq!(r.raw_frame_bytes, 4 * r.ngranules + r.raw_bytes);
    // Per-granule raw fallback bounds every arm's emitted size.
    assert!(r.frame_bytes <= r.raw_frame_bytes, "{r:?}");
    assert!(r.frame_bytes <= r.alp_frame_bytes, "{r:?}");
    // ... and every individual frame is bounded by its raw image.
    for (g, frame) in enc.frames.iter().enumerate() {
        let glen = values[g * GRANULE_VALUES..].len().min(GRANULE_VALUES);
        assert!(frame.len() <= 4 + 4 * glen, "granule {g}: {} bytes", frame.len());
    }
    // analyze() is encode() minus the frames (election determinism).
    assert_eq!(granule32::analyze(values), *r);
    let decoded = enc.decode().expect("decode");
    assert_eq!(decoded.len(), values.len());
    for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
        assert_eq!(a.to_bits(), b.to_bits(), "bit mismatch at {i}: {a:?} vs {b:?}");
    }
    // The bit-image surface agrees with the f32 surface exactly.
    let mut bits: Vec<u32> = Vec::new();
    for frame in &enc.frames {
        granule32::decode_frame_bits32(frame, &mut bits).expect("bits decode");
    }
    for (i, (a, b)) in values.iter().zip(bits.iter()).enumerate() {
        assert_eq!(a.to_bits(), *b, "bit-surface mismatch at {i}");
    }
    enc
}

#[test]
fn decimal_origin_elects_alp() {
    let mut rng = Rng::new(0x51);
    // The f4_decimal2 shape: two-decimal values under 1000.
    let values: Vec<f32> = (0..65_536).map(|_| rng.below(100_000) as f32 / 100.0).collect();
    let enc = roundtrip(&values);
    let r = &enc.report;
    assert_eq!(r.granules_using(Scheme::Alp), 8, "{r:?}");
    // Decimal-scaled f32 round-trips (near-)exactly under the f64-domain
    // transform: the exception rate is the defect detector — a per-multiply
    // f32 evaluation regresses this to 13–25% (classic32.rs module doc).
    assert!(r.exceptions <= r.total_values / 1000, "{r:?}");
    // The writer-side election: must clear the >=10%-win gate against the
    // uncompressed 4-byte raw payload. Exact arithmetic bound: 100_000
    // distinct mantissas need 17 bits of the 32 — plus per-vector headers
    // that is ~53.5% of raw, so pin at 55% (a 24%-exception build priced
    // 89.4% and could never pass this).
    assert!(r.wins_by_ten_percent(r.raw_bytes), "{r:?}");
    assert!(r.frame_bytes * 20 <= r.raw_bytes * 11, "{r:?}");
}

#[test]
fn random_bits_fall_to_raw() {
    let mut rng = Rng::new(0x52);
    let values: Vec<f32> = (0..65_536).map(|_| f32::from_bits(rng.next_u64() as u32)).collect();
    let enc = roundtrip(&values);
    let r = &enc.report;
    assert_eq!(r.granules_using(Scheme::Raw), 8, "{r:?}");
    assert_eq!(r.frame_bytes, r.raw_frame_bytes, "{r:?}");
    assert_eq!(r.exceptions, 0, "{r:?}");
    // Headers alone can never clear the 10% gate.
    assert!(!r.wins_by_ten_percent(r.raw_bytes), "{r:?}");
}

#[test]
fn specials_are_bit_exact_across_frame_boundaries() {
    let mut rng = Rng::new(0x53);
    let specials: [f32; 8] = [
        f32::NAN,
        f32::from_bits(0x7FC0_00AB), // NaN payload
        f32::from_bits(0xFFC0_0042), // negative NaN payload
        f32::INFINITY,
        f32::NEG_INFINITY,
        -0.0,
        f32::from_bits(1),           // smallest subnormal
        f32::MIN_POSITIVE / 8.0,     // subnormal
    ];
    let mut values: Vec<f32> = (0..2 * GRANULE_VALUES + 100)
        .map(|_| rng.below(1_000_000) as f32 / 100.0)
        .collect();
    // Plant specials at frame/granule boundaries and interior slots.
    let n = values.len();
    for (k, &s) in specials.iter().enumerate() {
        values[k] = s;
        values[GRANULE_VALUES - 1 - k] = s;
        values[GRANULE_VALUES + k] = s;
        values[n - 1 - k] = s;
    }
    let enc = roundtrip(&values); // bit-exact assert lives in the harness
    assert!(enc.report.exceptions > 0, "specials must surface as exceptions");
}

#[test]
fn short_tail_and_single_value_roundtrip() {
    roundtrip(&[1.25f32]);
    roundtrip(&[f32::NAN]);
    let mut rng = Rng::new(0x54);
    let values: Vec<f32> = (0..GRANULE_VALUES + 7).map(|_| rng.below(10_000) as f32 / 10.0).collect();
    roundtrip(&values);
    // Fast-round domain boundary: values straddling 2^22.
    let boundary: Vec<f32> = (0..1500)
        .map(|i| (1 << 22) as f32 + i as f32 - 750.0)
        .collect();
    roundtrip(&boundary);
}

#[test]
fn mixed_regimes_split_per_granule() {
    let mut rng = Rng::new(0x55);
    let mut values = Vec::with_capacity(2 * GRANULE_VALUES);
    for _ in 0..GRANULE_VALUES {
        values.push(rng.below(1_000_000) as f32 / 100.0);
    }
    for _ in 0..GRANULE_VALUES {
        values.push(f32::from_bits(rng.next_u64() as u32));
    }
    let enc = roundtrip(&values);
    let r = &enc.report;
    assert_eq!(r.granule_schemes, vec![Scheme::Alp, Scheme::Raw], "{r:?}");
}

#[test]
fn corrupt_frames_refuse_typed_never_panic() {
    let mut rng = Rng::new(0x56);
    let values: Vec<f32> = (0..GRANULE_VALUES).map(|_| rng.below(100_000) as f32 / 100.0).collect();
    let enc = granule32::encode(&values);
    let frame = &enc.frames[0];
    // Every truncation refuses (typed, output restored).
    let mut refusals = 0usize;
    for cut in 0..frame.len().min(600) {
        let mut out = Vec::new();
        if granule32::decode_frame32(&frame[..cut], &mut out).is_err() {
            assert!(out.is_empty(), "failed decode must restore the sink");
            refusals += 1;
        }
    }
    assert!(refusals > 0, "truncation sweep never fired");
    // Seeded field defects refuse typed.
    for (off, val, what) in [
        (0usize, 9u8, "unknown scheme tag"),
        (0usize, 1u8, "RD tag is not an f32 scheme"),
        (1usize, 0u8, "zero nvectors"),
        (1usize, 200u8, "oversized nvectors"),
    ] {
        let mut bad = frame.clone();
        bad[off] = val;
        let mut out = Vec::new();
        assert!(
            granule32::decode_frame32(&bad, &mut out).is_err(),
            "{what} must refuse"
        );
    }
    // Random flips: never a panic; the sink is restored on every refusal.
    for _ in 0..800 {
        let mut bad = frame.clone();
        let i = (rng.next_u64() as usize) % bad.len();
        bad[i] ^= (rng.next_u64() as u8) | 1;
        let mut out = Vec::new();
        if granule32::decode_frame32(&bad, &mut out).is_err() {
            assert!(out.is_empty());
        }
    }
}
