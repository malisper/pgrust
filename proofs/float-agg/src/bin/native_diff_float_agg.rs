//! Native differential replay of the float-agg value grids — in particular
//! the three fleet FAILED harnesses (grid_stddev_pop, grid_stddev_samp,
//! grid_corr; suite 994f9977/1785310905), whose failed check is a sqrt-result
//! bit-compare = the dsqrt dual-mode artifact signature (CBMC's sqrt model
//! diverges from real silicon; native parity for dsqrt itself is proven in
//! proofs/float-arith/tests/dsqrt_grid_native.rs).
//!
//! SAME shipped Rust cores (adt_float::aggregates) vs the SAME vendored
//! REL_18_STABLE C (linked natively via build.rs). Coverage:
//!   1. the EXACT grid domains of every grid_* harness (t3: 6x12 = 72 cells,
//!      t6 up to 6x12^3 = 10368 cells) — a 0-diff here refutes the in-model
//!      FAILEDs as tool artifacts;
//!   2. a mass random open-region sweep (full random bit patterns incl NaN
//!      payloads) — census-grade cover of the regimes the grids exclude.
//!
//! Run: cargo run --release --bin native_diff_float_agg

use proof_float_agg as _; // bundle the vendored C archive (build.rs cc link)

use std::os::raw::c_int;

extern "C" {
    fn pg_float8_avg(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_var_pop(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_var_samp(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_stddev_pop(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_stddev_samp(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_sxx(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_syy(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_sxy(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_avgx(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_avgy(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_covar_pop(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_covar_samp(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_corr(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_r2(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_slope(trans: *const f64, out: *mut f64) -> c_int;
    fn pg_float8_regr_intercept(trans: *const f64, out: *mut f64) -> c_int;
}

// EXACT tables from src/lib.rs proofs module.
const N_GRID: [f64; 6] = [0.0, 1.0, 2.0, 3.0, 1e15, 1e300];
const S_GRID: [f64; 12] = [
    0.0,
    -0.0,
    1.0,
    -1.0,
    0.5,
    3.0,
    1e308,
    -1e308,
    5e-324,
    f64::INFINITY,
    f64::NEG_INFINITY,
    f64::NAN,
];

struct Stats {
    checked: u64,
    diffs: u64,
}

fn check(
    name: &str,
    stats: &mut Stats,
    t: &[f64],
    r: Option<f64>,
    cfn: unsafe extern "C" fn(*const f64, *mut f64) -> c_int,
) {
    let mut c_out = 0f64;
    let cflag = unsafe { cfn(t.as_ptr(), &mut c_out) };
    stats.checked += 1;
    let ok = match r {
        None => cflag == 1,
        Some(v) => cflag == 0 && v.to_bits() == c_out.to_bits(),
    };
    if !ok {
        stats.diffs += 1;
        eprintln!(
            "DIFF {name}: trans={:?} rust={:?} cflag={cflag} cval={c_out:?} (bits {:x?} vs {:x})",
            t,
            r,
            r.map(|v| v.to_bits()),
            c_out.to_bits()
        );
    }
}

// xorshift64* — deterministic mass sweep, full bit patterns (incl NaNs).
fn rng(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

fn main() {
    use adt_float::aggregates as agg;
    let mut stats = Stats {
        checked: 0,
        diffs: 0,
    };

    // ---- 1. exact grid domains of the Kani grid harnesses ----
    // t3 grids
    for &n in &N_GRID {
        for &s in &S_GRID {
            let avg = [n, s, 0.0];
            check("grid_avg", &mut stats, &avg, agg::float8_avg(avg), pg_float8_avg);
            let t = [n, 0.0, s];
            check("grid_var_pop", &mut stats, &t, agg::float8_var_pop(t), pg_float8_var_pop);
            check("grid_var_samp", &mut stats, &t, agg::float8_var_samp(t), pg_float8_var_samp);
            check("grid_stddev_pop", &mut stats, &t, agg::float8_stddev_pop(t), pg_float8_stddev_pop);
            check("grid_stddev_samp", &mut stats, &t, agg::float8_stddev_samp(t), pg_float8_stddev_samp);
        }
    }
    // t6 single-slot grids
    for &n in &N_GRID {
        for &s in &S_GRID {
            let t = [n, s, 0.0, 0.0, 0.0, 0.0];
            check("grid_regr_avgx", &mut stats, &t, agg::float8_regr_avgx(t), pg_float8_regr_avgx);
            let t = [n, 0.0, 0.0, s, 0.0, 0.0];
            check("grid_regr_avgy", &mut stats, &t, agg::float8_regr_avgy(t), pg_float8_regr_avgy);
            let t = [n, 0.0, 0.0, 0.0, 0.0, s];
            check("grid_covar_pop", &mut stats, &t, agg::float8_covar_pop(t), pg_float8_covar_pop);
            check("grid_covar_samp", &mut stats, &t, agg::float8_covar_samp(t), pg_float8_covar_samp);
        }
    }
    // t6 two-slot grid: slope [n, 0, sxx, 0, 0, sxy]
    for &n in &N_GRID {
        for &sxx in &S_GRID {
            for &sxy in &S_GRID {
                let t = [n, 0.0, sxx, 0.0, 0.0, sxy];
                check("grid_regr_slope", &mut stats, &t, agg::float8_regr_slope(t), pg_float8_regr_slope);
            }
        }
    }
    // t6 three-slot grids: r2/corr [n, 0, sxx, 0, syy, sxy]
    for &n in &N_GRID {
        for &sxx in &S_GRID {
            for &syy in &S_GRID {
                for &sxy in &S_GRID {
                    let t = [n, 0.0, sxx, 0.0, syy, sxy];
                    check("grid_regr_r2", &mut stats, &t, agg::float8_regr_r2(t), pg_float8_regr_r2);
                    check("grid_corr", &mut stats, &t, agg::float8_corr(t), pg_float8_corr);
                }
            }
        }
    }
    // intercept [n, sx, sxx, sy, 0, sxy]
    for &n in &N_GRID {
        for &sx in &S_GRID {
            for &sxx in &S_GRID {
                for &sxy in &S_GRID {
                    // keep 4D within budget: sy rides the sx table entry
                    let t = [n, sx, sxx, sx, 0.0, sxy];
                    check("grid_regr_intercept", &mut stats, &t, agg::float8_regr_intercept(t), pg_float8_regr_intercept);
                }
            }
        }
    }
    let grid_checked = stats.checked;
    let grid_diffs = stats.diffs;
    println!("grid domains: {grid_checked} checks, {grid_diffs} diffs");

    // ---- 2. mass random open-region sweep (full bit patterns) ----
    let mut seed = 0x9E3779B97F4A7C15u64;
    const SWEEP: usize = 500_000;
    for _ in 0..SWEEP {
        let t3 = [
            f64::from_bits(rng(&mut seed)),
            f64::from_bits(rng(&mut seed)),
            f64::from_bits(rng(&mut seed)),
        ];
        check("sweep_avg", &mut stats, &t3, agg::float8_avg(t3), pg_float8_avg);
        check("sweep_var_pop", &mut stats, &t3, agg::float8_var_pop(t3), pg_float8_var_pop);
        check("sweep_var_samp", &mut stats, &t3, agg::float8_var_samp(t3), pg_float8_var_samp);
        check("sweep_stddev_pop", &mut stats, &t3, agg::float8_stddev_pop(t3), pg_float8_stddev_pop);
        check("sweep_stddev_samp", &mut stats, &t3, agg::float8_stddev_samp(t3), pg_float8_stddev_samp);
        let t6 = [
            f64::from_bits(rng(&mut seed)),
            f64::from_bits(rng(&mut seed)),
            f64::from_bits(rng(&mut seed)),
            f64::from_bits(rng(&mut seed)),
            f64::from_bits(rng(&mut seed)),
            f64::from_bits(rng(&mut seed)),
        ];
        check("sweep_regr_sxx", &mut stats, &t6, agg::float8_regr_sxx(t6), pg_float8_regr_sxx);
        check("sweep_regr_syy", &mut stats, &t6, agg::float8_regr_syy(t6), pg_float8_regr_syy);
        check("sweep_regr_sxy", &mut stats, &t6, agg::float8_regr_sxy(t6), pg_float8_regr_sxy);
        check("sweep_regr_avgx", &mut stats, &t6, agg::float8_regr_avgx(t6), pg_float8_regr_avgx);
        check("sweep_regr_avgy", &mut stats, &t6, agg::float8_regr_avgy(t6), pg_float8_regr_avgy);
        check("sweep_covar_pop", &mut stats, &t6, agg::float8_covar_pop(t6), pg_float8_covar_pop);
        check("sweep_covar_samp", &mut stats, &t6, agg::float8_covar_samp(t6), pg_float8_covar_samp);
        check("sweep_corr", &mut stats, &t6, agg::float8_corr(t6), pg_float8_corr);
        check("sweep_regr_r2", &mut stats, &t6, agg::float8_regr_r2(t6), pg_float8_regr_r2);
        check("sweep_regr_slope", &mut stats, &t6, agg::float8_regr_slope(t6), pg_float8_regr_slope);
        check("sweep_regr_intercept", &mut stats, &t6, agg::float8_regr_intercept(t6), pg_float8_regr_intercept);
    }
    println!(
        "TOTAL: {} checks, {} diffs (grid portion: {grid_checked}/{grid_diffs})",
        stats.checked, stats.diffs
    );
    if stats.diffs > 0 {
        std::process::exit(1);
    }
}
