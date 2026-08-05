//! Deterministic per-function differential sampler for the float-math
//! family: N cases per function through the same three-way comparator the
//! libFuzzer targets use (any divergence panics). Complements the
//! coverage-guided campaign with exact per-function case counts.
//!
//! Input distribution per case (xorshift64*, fixed seed => reproducible):
//!   50% uniform random f64 bit patterns (hits NaN payloads, denormals,
//!       huge exponents at their natural measure),
//!   25% "small range" values built from a random sign/exponent-window
//!       around [-1024, 1024] (the trig/exp/gamma live zone),
//!   25% boundary-corpus value perturbed by +-few ulps.
//!
//! Usage: float_math_sample [N_PER_FN]   (default 1_000_000)

fn main() {
    let n: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    // Boundary anchors (superset of the smoke-grid corpus).
    let anchors: Vec<f64> = vec![
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.5,
        -0.5,
        1.5,
        -2.0,
        -3.0,
        30.0,
        45.0,
        60.0,
        90.0,
        180.0,
        270.0,
        360.0,
        -360.0,
        1.5707963267948966,
        3.141592653589793,
        709.782712893384,
        -745.1332191019413,
        171.62437695630272,
        -171.5,
        5e-324,
        2.2250738585072014e-308,
        1e308,
        1e22,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
    ];

    let mut state: u64 = 0x9e3779b97f4a7c15;
    let mut next = move || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545f4914f6cdd1d)
    };
    let mut gen_val = |next: &mut dyn FnMut() -> u64| -> f64 {
        let r = next();
        match r % 4 {
            0 | 1 => f64::from_bits(next()),
            2 => {
                // sign * mantissa scaled into ~[-1024, 1024]
                let m = (next() as f64 / u64::MAX as f64) * 2048.0 - 1024.0;
                m
            }
            _ => {
                let a = anchors[(next() as usize) % anchors.len()];
                let ulps = (next() % 7) as i64 - 3;
                f64::from_bits((a.to_bits() as i64).wrapping_add(ulps) as u64)
            }
        }
    };

    let n1 = decoder_fuzz::diff::FLOAT_MATH1.len();
    for id in 0..n1 {
        for _ in 0..n {
            let x = gen_val(&mut next);
            let mut d = [0u8; 9];
            d[0] = id as u8;
            d[1..9].copy_from_slice(&x.to_le_bytes());
            decoder_fuzz::float_math_diff(&d);
        }
        println!("{}: {} cases, 0 diffs", decoder_fuzz::diff::FLOAT_MATH1[id].0, n);
    }
    for id in 0..decoder_fuzz::diff::FLOAT_MATH2.len() {
        for _ in 0..n {
            let a = gen_val(&mut next);
            let b = gen_val(&mut next);
            let mut d = [0u8; 17];
            d[0] = id as u8;
            d[1..9].copy_from_slice(&a.to_le_bytes());
            d[9..17].copy_from_slice(&b.to_le_bytes());
            decoder_fuzz::float_math2_diff(&d);
        }
        println!("{}: {} cases, 0 diffs", decoder_fuzz::diff::FLOAT_MATH2[id].0, n);
    }
    println!("TOTAL: {} functions x {} cases, all clean", n1 + decoder_fuzz::diff::FLOAT_MATH2.len(), n);
}
