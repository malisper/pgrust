//! Granule-API proofs: bit-exact round-trip through self-describing
//! 8192-value frames (NaN payloads / -0.0 / infinities placed across frame
//! boundaries), exact byte accounting, per-granule election, determinism,
//! and frame-decode hardening on corrupt bytes.

use alp::granule::{self, FrameError, GranuleEncoded, GRANULE_VALUES};
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

    fn i64_in(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as u64) as i64
    }
}

/// Encode + decode + every structural invariant the writer relies on.
#[track_caller]
fn roundtrip(values: &[f64]) -> GranuleEncoded {
    let enc = granule::encode(values);
    let r = &enc.report;
    assert_eq!(enc.frames.len(), values.len().div_ceil(GRANULE_VALUES));
    assert_eq!(r.ngranules, enc.frames.len());
    assert_eq!(r.total_values, values.len());
    assert_eq!(r.raw_bytes, values.len() * 8);
    // frame_bytes is EXACT: the emitted frames' total length, nothing else.
    assert_eq!(r.frame_bytes, enc.frames.iter().map(|f| f.len()).sum::<usize>());
    // Raw arm accounting is closed-form: header per granule + bit images.
    assert_eq!(r.raw_frame_bytes, 4 * r.ngranules + r.raw_bytes);
    // Per-granule raw fallback bounds every arm's emitted size.
    assert!(r.frame_bytes <= r.raw_frame_bytes, "{r:?}");
    assert!(r.frame_bytes <= r.alp_frame_bytes, "{r:?}");
    assert!(r.frame_bytes <= r.alp_rd_frame_bytes, "{r:?}");
    // ... and every individual frame is bounded by its raw image.
    for (g, frame) in enc.frames.iter().enumerate() {
        let glen = values[g * GRANULE_VALUES..].len().min(GRANULE_VALUES);
        assert!(frame.len() <= 4 + 8 * glen, "granule {g}: {} bytes", frame.len());
    }
    // analyze() is encode() minus the frames; the reports must be equal
    // (this also witnesses election determinism across two runs).
    assert_eq!(granule::analyze(values), *r);
    let decoded = enc.decode().expect("decode");
    assert_eq!(decoded.len(), values.len());
    for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
        assert_eq!(a.to_bits(), b.to_bits(), "bit mismatch at {i}: {a:?} vs {b:?}");
    }
    enc
}

#[test]
fn decimal_origin_elects_alp() {
    let mut rng = Rng::new(0x41);
    let values: Vec<f64> =
        (0..65_536).map(|_| rng.i64_in(-1_000_000, 1_000_000) as f64 / 100.0).collect();
    let enc = roundtrip(&values);
    let r = &enc.report;
    assert_eq!(r.granules_using(Scheme::Alp), 8, "{r:?}");
    // The writer-side election: uniform-scale decimals must clear the
    // >=10%-win gate against the uncompressed raw payload.
    assert!(r.wins_by_ten_percent(r.raw_bytes), "{r:?}");
    assert!(r.frame_bytes < r.raw_bytes / 2, "{r:?}");
}

#[test]
fn real_doubles_elect_rd() {
    let mut rng = Rng::new(0x42);
    // Uniform [0,1): 52 random mantissa bits — no (e,f) verifies, but the
    // left-bits dictionary captures the narrow exponent range.
    let values: Vec<f64> = (0..65_536)
        .map(|_| (rng.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64))
        .collect();
    let enc = roundtrip(&values);
    let r = &enc.report;
    assert_eq!(r.granules_using(Scheme::AlpRd), 8, "{r:?}");
    assert!(r.frame_bytes < r.raw_bytes, "{r:?}");
    assert!(r.exceptions < values.len() / 10, "{r:?}");
}

#[test]
fn random_bits_fall_to_raw() {
    let mut rng = Rng::new(0x43);
    let values: Vec<f64> = (0..65_536).map(|_| f64::from_bits(rng.next_u64())).collect();
    let enc = roundtrip(&values);
    let r = &enc.report;
    assert_eq!(r.granules_using(Scheme::Raw), 8, "{r:?}");
    assert_eq!(r.frame_bytes, r.raw_frame_bytes, "{r:?}");
    assert_eq!(r.exceptions, 0, "{r:?}");
    // Headers alone can never clear the 10% gate — the writer keeps RawF.
    assert!(!r.wins_by_ten_percent(r.raw_bytes), "{r:?}");
}

#[test]
fn mixed_regimes_split_per_granule() {
    let mut rng = Rng::new(0x44);
    let mut values = Vec::with_capacity(65_536);
    for _ in 0..4 * GRANULE_VALUES {
        values.push(rng.i64_in(-1_000_000, 1_000_000) as f64 / 100.0);
    }
    for _ in 0..4 * GRANULE_VALUES {
        values.push(f64::from_bits(rng.next_u64()));
    }
    let enc = roundtrip(&values);
    let r = &enc.report;
    // Election is per granule: the decimal half engages ALP, the random
    // half must not be forced through it.
    for g in 0..4 {
        assert_eq!(r.granule_schemes[g], Scheme::Alp, "{r:?}");
    }
    for g in 4..8 {
        assert_ne!(r.granule_schemes[g], Scheme::Alp, "{r:?}");
    }
}

#[test]
fn specials_across_frame_boundaries() {
    let mut rng = Rng::new(0x45);
    let n = 3 * GRANULE_VALUES + 511;
    let mut values: Vec<f64> =
        (0..n).map(|_| rng.i64_in(-100_000, 100_000) as f64 / 1000.0).collect();
    // Straddle every granule boundary (and the short tail's edges) with
    // the bit-exactness hazards: NaN payloads, -0.0, infinities, denormal.
    for b in [GRANULE_VALUES, 2 * GRANULE_VALUES, 3 * GRANULE_VALUES] {
        values[b - 1] = f64::from_bits(0xFFF8_DEAD_BEEF_0001);
        values[b] = -0.0;
        values[b + 1] = f64::NEG_INFINITY;
    }
    values[0] = f64::from_bits(0x7FF0_0000_0000_0001); // signaling NaN
    let last = values.len() - 1;
    values[last] = f64::from_bits(1); // smallest denormal
    let enc = roundtrip(&values);
    assert_eq!(enc.report.ngranules, 4);
    // Decode each frame independently: self-describing means no shared
    // state may leak between granules.
    let mut out = Vec::new();
    for frame in &enc.frames {
        granule::decode_frame(frame, &mut out).unwrap();
    }
    assert_eq!(out.len(), values.len());
    for (i, (a, b)) in values.iter().zip(out.iter()).enumerate() {
        assert_eq!(a.to_bits(), b.to_bits(), "per-frame decode mismatch at {i}");
    }
}

#[test]
fn tails_at_granule_and_vector_level() {
    let mut rng = Rng::new(0x46);
    for &n in &[
        1usize,
        2,
        511,
        1023,
        1024,
        1025,
        4096,
        8191,
        8192,
        8193,
        8192 + 513,
        2 * 8192 + 1,
        65_535,
        65_536,
        65_537,
        70_000,
    ] {
        let values: Vec<f64> =
            (0..n).map(|_| rng.i64_in(-50_000, 50_000) as f64 / 100.0).collect();
        roundtrip(&values);
        // And the same lengths on incompressible input (raw frames).
        let values: Vec<f64> = (0..n).map(|_| f64::from_bits(rng.next_u64())).collect();
        roundtrip(&values);
    }
}

#[test]
fn empty_input() {
    let enc = granule::encode(&[]);
    assert!(enc.frames.is_empty());
    assert_eq!(enc.report.ngranules, 0);
    assert_eq!(enc.report.frame_bytes, 0);
    assert_eq!(enc.decode().unwrap(), Vec::<f64>::new());
    assert_eq!(granule::analyze(&[]), enc.report);
}

#[test]
fn encoding_is_deterministic() {
    let mut rng = Rng::new(0x47);
    let mut values = Vec::with_capacity(40_000);
    for i in 0..40_000u64 {
        if (i / 1024) % 3 == 0 {
            values.push(f64::from_bits(rng.next_u64()));
        } else {
            values.push(rng.i64_in(-100_000, 100_000) as f64 / 100.0);
        }
    }
    // Byte-identical frames across independent encodes: the writer's
    // serial==parallel identity matrix depends on this.
    assert_eq!(granule::encode(&values), granule::encode(&values));
}

#[test]
fn corrupt_frames_error_not_panic() {
    let mut rng = Rng::new(0x48);
    let values: Vec<f64> =
        (0..10_000).map(|_| rng.i64_in(-1_000_000, 1_000_000) as f64 / 100.0).collect();
    let enc = granule::encode(&values);
    assert_eq!(enc.report.granule_schemes[0], Scheme::Alp);
    let frame = &enc.frames[0];

    let mut out = vec![7.0f64; 3];
    // Truncation at every prefix of the header and a mid-payload cut.
    for cut in [0usize, 1, 2, 3, 4, 10, frame.len() - 1] {
        let err = granule::decode_frame(&frame[..cut], &mut out).unwrap_err();
        assert_eq!(err, FrameError::Truncated, "cut {cut}");
        // Failed decodes must leave `out` exactly as it was.
        assert_eq!(out.len(), 3, "cut {cut}");
    }
    // Unknown scheme tag.
    let mut bad = frame.clone();
    bad[0] = 9;
    assert_eq!(granule::decode_frame(&bad, &mut out), Err(FrameError::BadScheme(9)));
    // Trailing bytes past the declared payload.
    let mut bad = frame.clone();
    bad.push(0);
    assert_eq!(granule::decode_frame(&bad, &mut out), Err(FrameError::Trailing));
    // Out-of-range field: first vector's exponent byte (header 4 + len 2).
    let mut bad = frame.clone();
    bad[6] = 200;
    assert_eq!(
        granule::decode_frame(&bad, &mut out),
        Err(FrameError::BadField("exponent/factor"))
    );
    // Zero vector count.
    let mut bad = frame.clone();
    bad[1] = 0;
    assert_eq!(granule::decode_frame(&bad, &mut out), Err(FrameError::BadField("nvectors")));
    assert_eq!(out, vec![7.0f64; 3]);

    // The pristine frame still decodes after all that.
    let mut ok = Vec::new();
    let n = granule::decode_frame(frame, &mut ok).unwrap();
    assert_eq!(n, GRANULE_VALUES);
}

#[test]
fn rd_frames_are_self_describing() {
    let mut rng = Rng::new(0x49);
    let values: Vec<f64> = (0..2 * GRANULE_VALUES)
        .map(|_| (rng.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64))
        .collect();
    let enc = granule::encode(&values);
    assert_eq!(enc.report.granules_using(Scheme::AlpRd), 2);
    // Decode the SECOND frame alone, first never touched: the dictionary
    // must ride inside the frame.
    let mut out = Vec::new();
    granule::decode_frame(&enc.frames[1], &mut out).unwrap();
    assert_eq!(out.len(), GRANULE_VALUES);
    for (i, (a, b)) in values[GRANULE_VALUES..].iter().zip(out.iter()).enumerate() {
        assert_eq!(a.to_bits(), b.to_bits(), "mismatch at {i}");
    }
}

#[test]
fn decode_frame_words_matches_decode_frame() {
    // The u64 bit-word surface (pgrcolumnar A4 datum words) must be exactly
    // decode_frame's to_bits images, across all three per-granule schemes
    // plus a short tail.
    let mut rng = Rng::new(0x4a4);
    let n = 2 * GRANULE_VALUES + 700;
    let values: Vec<f64> = (0..n)
        .map(|i| match i / GRANULE_VALUES {
            0 => rng.i64_in(-10_000_000, 10_000_000) as f64 / 100.0,
            1 => (rng.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64),
            _ => f64::from_bits(rng.next_u64()),
        })
        .collect();
    let enc = granule::encode(&values);
    let mut f64s: Vec<f64> = Vec::new();
    let mut words: Vec<u64> = Vec::new();
    for frame in &enc.frames {
        let a = granule::decode_frame(frame, &mut f64s).unwrap();
        let b = granule::decode_frame_words(frame, &mut words).unwrap();
        assert_eq!(a, b);
    }
    assert_eq!(words.len(), f64s.len());
    for (i, (w, v)) in words.iter().zip(f64s.iter()).enumerate() {
        assert_eq!(*w, v.to_bits(), "word mismatch at {i}");
    }
    // Error path restores the words sink exactly like the f64 sink.
    words.clear();
    words.push(7);
    let mut bad = enc.frames[0].clone();
    bad.push(0);
    assert!(granule::decode_frame_words(&bad, &mut words).is_err());
    assert_eq!(words, vec![7]);
}
