//! NEON-class throughput characterization for the granule API.
//!
//! LOCAL SMOKE NUMBERS ONLY: these run on whatever host executes the test
//! (an arm64 mac is NEON-class but is not the CI cluster) and exist to catch
//! autovectorization cliffs in the hot loops, not to produce reportable
//! numbers — the CI cluster A/B on c8g owns every reportable figure.
//!
//! Run: cargo test -p alp --release --test throughput -- --ignored --nocapture

use alp::granule;
use alp::Scheme;

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn i64_in(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next_u64() % (hi - lo + 1) as u64) as i64
    }
}

const N: usize = 8 << 20; // 8Mi values = 64 MiB of f64 payload per arm

fn decimal_origin(rng: &mut Rng) -> Vec<f64> {
    (0..N).map(|_| rng.i64_in(-10_000_000, 10_000_000) as f64 / 100.0).collect()
}

fn real_double(rng: &mut Rng) -> Vec<f64> {
    (0..N).map(|_| (rng.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)).collect()
}

fn mixed(rng: &mut Rng) -> Vec<f64> {
    // Granule-grained interleave of the two regimes plus raw-bit noise:
    // exercises per-granule scheme switching on decode.
    (0..N)
        .map(|i| match (i / granule::GRANULE_VALUES) % 3 {
            0 => rng.i64_in(-10_000_000, 10_000_000) as f64 / 100.0,
            1 => (rng.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64),
            _ => f64::from_bits(rng.next_u64()),
        })
        .collect()
}

fn gbs(bytes: usize, secs: f64) -> f64 {
    bytes as f64 / 1e9 / secs
}

/// Encode chunk-by-chunk (pgrcolumnar RG geometry: 65536 values = 8
/// granules) and decode every frame; best-of-k wall time on each side.
fn characterize(name: &str, values: &[f64]) {
    const CHUNK: usize = 8 * granule::GRANULE_VALUES;
    let reps = 3;

    let mut enc_best = f64::INFINITY;
    let mut encoded: Vec<granule::GranuleEncoded> = Vec::new();
    for _ in 0..reps {
        encoded.clear();
        let t = std::time::Instant::now();
        for chunk in values.chunks(CHUNK) {
            encoded.push(granule::encode(chunk));
        }
        enc_best = enc_best.min(t.elapsed().as_secs_f64());
    }

    let mut dec_best = f64::INFINITY;
    let mut out: Vec<f64> = Vec::with_capacity(values.len());
    for _ in 0..reps + 2 {
        out.clear();
        let t = std::time::Instant::now();
        for enc in &encoded {
            for frame in &enc.frames {
                granule::decode_frame(frame, &mut out).unwrap();
            }
        }
        dec_best = dec_best.min(t.elapsed().as_secs_f64());
    }

    // Keep the measurement honest: the decoded stream must be bit-exact.
    assert_eq!(out.len(), values.len());
    for (a, b) in values.iter().zip(out.iter()) {
        assert_eq!(a.to_bits(), b.to_bits());
    }

    let bytes = values.len() * 8;
    let (mut alp, mut rd, mut raw, mut frame_bytes, mut exc) = (0, 0, 0, 0usize, 0usize);
    for e in &encoded {
        alp += e.report.granules_using(Scheme::Alp);
        rd += e.report.granules_using(Scheme::AlpRd);
        raw += e.report.granules_using(Scheme::Raw);
        frame_bytes += e.report.frame_bytes;
        exc += e.report.exceptions;
    }
    eprintln!(
        "{name:>14}: encode {:6.2} GB/s  decode {:6.2} GB/s  {:5.2} bits/value  \
         granules alp/rd/raw {alp}/{rd}/{raw}  exceptions {exc}",
        gbs(bytes, enc_best),
        gbs(bytes, dec_best),
        frame_bytes as f64 * 8.0 / values.len() as f64,
    );
}

#[test]
#[ignore]
fn throughput_characterization() {
    eprintln!(
        "== LOCAL SMOKE NUMBERS — this host only, never reportable; \
         CI cluster A/B on c8g owns real numbers =="
    );
    let mut rng = Rng(0x005E_EDA4);
    characterize("decimal-origin", &decimal_origin(&mut rng));
    characterize("real-double", &real_double(&mut rng));
    characterize("mixed", &mixed(&mut rng));
    eprintln!(
        "== encode includes the dual-arm exact election (both ALP and \
         ALP-RD encoded per granule) =="
    );
}
