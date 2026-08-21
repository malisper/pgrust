//! Property tests for the ALP encoder. The merge bar: BIT-EXACT round-trip
//! (f64::to_bits equality) over every input class, and deterministic
//! encoding. No proptest in the workspace, so generators are hand-rolled
//! over a seeded splitmix64 — every failure reproduces from the seed.

use alp::{analyze, decode, encode, Scheme};

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

#[track_caller]
fn assert_roundtrip(values: &[f64]) {
    let encoded = encode(values);
    let decoded = decode(&encoded);
    assert_eq!(decoded.len(), values.len());
    for (i, (a, b)) in values.iter().zip(decoded.iter()).enumerate() {
        assert_eq!(
            a.to_bits(),
            b.to_bits(),
            "bit mismatch at {i}: {a:?} vs {b:?}"
        );
    }
}

#[test]
fn decimal_origin_values_at_many_scales() {
    let mut rng = Rng::new(0xA1);
    for f in 0..=18u32 {
        let fact = 10f64.powi(f as i32);
        let values: Vec<f64> = (0..6000)
            .map(|_| rng.i64_in(-(1 << 30), 1 << 30) as f64 / fact)
            .collect();
        assert_roundtrip(&values);
        if f <= 14 {
            // Uniform-scale decimals must engage ALP classic and beat raw.
            let a = analyze(&values);
            assert_eq!(a.alp_rowgroups, 1, "f={f}: {a:?}");
            assert!(a.encoded_bytes < a.raw_bytes, "f={f}: {a:?}");
        }
    }
}

#[test]
fn decimal_origin_large_mantissas() {
    let mut rng = Rng::new(0xA2);
    // Mantissas near the i64/fast-round budget: many fail verification and
    // must come back through the exception path.
    let values: Vec<f64> = (0..4096)
        .map(|_| rng.i64_in(-(1 << 52), 1 << 52) as f64 / 100.0)
        .collect();
    assert_roundtrip(&values);
}

#[test]
fn pure_random_bit_patterns() {
    let mut rng = Rng::new(0xB0);
    let values: Vec<f64> = (0..300_000).map(|_| f64::from_bits(rng.next_u64())).collect();
    assert_roundtrip(&values);
    let a = analyze(&values);
    // The raw fallback bounds every rowgroup at 8 bytes/value.
    assert!(a.encoded_bytes <= a.raw_bytes, "{a:?}");
}

#[test]
fn nan_payloads_survive() {
    let mut rng = Rng::new(0xC0);
    let mut values = Vec::with_capacity(8000);
    for i in 0..8000u64 {
        if i % 3 == 0 {
            // Quiet/signaling NaNs with random payloads and signs (payload
            // forced nonzero so the pattern stays NaN, not infinity).
            let payload = (rng.next_u64() & ((1u64 << 52) - 1)) | 1;
            let sign = rng.below(2) << 63;
            values.push(f64::from_bits(sign | 0x7FF0_0000_0000_0000 | payload));
        } else {
            values.push(rng.i64_in(-1_000_000, 1_000_000) as f64 / 1000.0);
        }
    }
    assert_roundtrip(&values);
}

#[test]
fn signed_zeros_infinities_denormals() {
    let mut rng = Rng::new(0xD0);
    let mut values = Vec::new();
    for i in 0..5000u64 {
        match i % 5 {
            0 => values.push(0.0),
            1 => values.push(-0.0),
            2 => values.push(if i % 2 == 0 { f64::INFINITY } else { f64::NEG_INFINITY }),
            // Denormals: biased exponent 0, nonzero mantissa.
            3 => values.push(f64::from_bits(rng.below((1 << 52) - 1) + 1)),
            _ => values.push(rng.i64_in(-999, 999) as f64 / 10.0),
        }
    }
    assert_roundtrip(&values);
    // -0.0 specifically must come back with its sign bit.
    let out = decode(&encode(&[-0.0, 0.0, -0.0]));
    assert_eq!(out[0].to_bits(), (-0.0f64).to_bits());
    assert_eq!(out[1].to_bits(), 0.0f64.to_bits());
    assert_eq!(out[2].to_bits(), (-0.0f64).to_bits());
}

#[test]
fn fast_round_domain_boundary() {
    let b51 = (1u64 << 51) as f64;
    let b52 = (1u64 << 52) as f64;
    let mut values = Vec::new();
    for k in 0..1024i64 {
        let k = k as f64;
        // Around 2^51 the ulp is 0.5: exercise exact integers, the last
        // representable halves below the boundary, and both sides of it.
        values.push(b51 - k);
        values.push(b51 - k - 0.5);
        values.push(b51 + k);
        values.push(-(b51 + k));
        values.push(b52 + k);
        values.push(-(b52 - k));
    }
    assert_roundtrip(&values);
}

#[test]
fn sampling_is_deterministic() {
    let mut rng = Rng::new(0xE0);
    let mut values = Vec::with_capacity(210_000);
    for i in 0..210_000u64 {
        if (i / 1024) % 3 == 0 {
            values.push(f64::from_bits(rng.next_u64()));
        } else {
            values.push(rng.i64_in(-100_000, 100_000) as f64 / 100.0);
        }
    }
    let e1 = encode(&values);
    let e2 = encode(&values);
    // Full structural equality: same schemes, same (e,f) per vector, same
    // dictionaries, same packed bytes.
    assert_eq!(e1, e2);
    assert_eq!(analyze(&values), analyze(&values));
}

#[test]
fn tail_vectors_and_boundary_lengths() {
    let mut rng = Rng::new(0xF0);
    for &n in &[1usize, 2, 31, 1023, 1024, 1025, 2055, 102_399, 102_400, 102_401, 103_423] {
        let values: Vec<f64> = (0..n).map(|_| rng.i64_in(-50_000, 50_000) as f64 / 100.0).collect();
        assert_roundtrip(&values);
    }
    assert_roundtrip(&[]);
    assert_eq!(analyze(&[]).encoded_bytes, 0);
}

#[test]
fn constant_and_all_exception_inputs() {
    // Constant vector: zero exceptions, near-zero payload.
    let values = vec![1.5f64; 5000];
    assert_roundtrip(&values);
    let a = analyze(&values);
    assert_eq!(a.exceptions, 0, "{a:?}");
    assert_eq!(a.alp_rowgroups, 1);
    assert!(a.encoded_bytes < a.raw_bytes / 8, "{a:?}");

    // All-NaN (single payload): nothing ALP-encodable, but RD compresses
    // the constant left parts losslessly.
    let nans = vec![f64::NAN; 3000];
    assert_roundtrip(&nans);

    // All-NaN with random payloads: nothing wins; raw fallback must cap it.
    let mut rng = Rng::new(0x11);
    let wild: Vec<f64> = (0..3000)
        .map(|_| f64::from_bits(0x7FF0_0000_0000_0000 | (rng.next_u64() & ((1 << 52) - 1)) | 1))
        .collect();
    assert_roundtrip(&wild);
    assert!(analyze(&wild).encoded_bytes <= wild.len() * 8);
}

#[test]
fn real_doubles_take_the_rd_path() {
    let mut rng = Rng::new(0x22);
    // Uniform [0,1): 52 random mantissa bits — decimal (e,f) scaling can
    // never verify, but the left-bits dictionary captures the narrow
    // exponent range.
    let values: Vec<f64> = (0..150_000)
        .map(|_| (rng.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64))
        .collect();
    assert_roundtrip(&values);
    let a = analyze(&values);
    assert_eq!(a.alp_rd_rowgroups, 2, "{a:?}");
    assert!(a.encoded_bytes < a.raw_bytes, "{a:?}");
    // The dictionary election targets a sub-10% exception rate on this
    // distribution.
    assert!(a.exceptions < values.len() / 10, "{a:?}");
}

#[test]
fn mixed_regimes_split_per_rowgroup() {
    let mut rng = Rng::new(0x33);
    let mut values = Vec::with_capacity(204_800);
    for _ in 0..102_400 {
        values.push(rng.i64_in(-1_000_000, 1_000_000) as f64 / 100.0);
    }
    for _ in 0..102_400 {
        values.push(f64::from_bits(rng.next_u64()));
    }
    assert_roundtrip(&values);
    let encoded = encode(&values);
    assert_eq!(encoded.rowgroups.len(), 2);
    assert_eq!(encoded.rowgroups[0].scheme(), Scheme::Alp);
    // Random bit patterns must not be forced through ALP classic.
    assert_ne!(encoded.rowgroups[1].scheme(), Scheme::Alp);
}

/// Throughput smoke, not a benchmark: run with
///   cargo test -p alp --release -- --ignored throughput
/// Reportable numbers come from the CI cluster, never from here.
#[test]
#[ignore]
fn throughput_smoke() {
    let mut rng = Rng::new(0x77);
    let n = 8 * 1024 * 1024;
    let values: Vec<f64> = (0..n).map(|_| rng.i64_in(-1_000_000, 1_000_000) as f64 / 100.0).collect();

    let t0 = std::time::Instant::now();
    let encoded = encode(&values);
    let t_enc = t0.elapsed();

    let t1 = std::time::Instant::now();
    let decoded = decode(&encoded);
    let t_dec = t1.elapsed();

    assert_eq!(decoded.len(), values.len());
    for (a, b) in values.iter().zip(decoded.iter()) {
        assert_eq!(a.to_bits(), b.to_bits());
    }
    let mb = (n * 8) as f64 / (1024.0 * 1024.0);
    eprintln!(
        "encode: {:.0} MB in {:?} ({:.0} MB/s); decode: {:?} ({:.0} MB/s); ratio {:.2} bits/value",
        mb,
        t_enc,
        mb / t_enc.as_secs_f64(),
        t_dec,
        mb / t_dec.as_secs_f64(),
        encoded.size_bytes() as f64 * 8.0 / n as f64,
    );
}
