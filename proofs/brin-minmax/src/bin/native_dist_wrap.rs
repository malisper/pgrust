//! Native differential replay of the dist_time / dist_timetz OUT-OF-CONTRACT
//! wrap plane (fleet batchB FAILEDs at builtins.rs:124/137, suite tip
//! 994f9977: "attempt to subtract/add with overflow").
//!
//! Those FAILEDs are Kani debug-overflow-CHECK artifacts, not value
//! divergences: Kani compiles rustc's debug overflow panics into shipped
//! code, while the shipped RELEASE binary wraps two's-complement exactly as
//! the C (-fwrapv) does. This bin adjudicates by running the SAME shipped
//! wrappers (release wrap semantics) against the SAME vendored C compiled
//! natively with -fwrapv:
//!   1. dist_time: wrap-boundary spots + random full-i64 sweep — expect 0
//!      diffs (i64 wrap parity).
//!   2. dist_timetz: full-random times x in-bound zones (|zone| <= 86400)
//!      — expect 0 diffs.
//!   3. dist_timetz ZONE-WRAP plane (zone diff outside i32): expect
//!      DIVERGENCE — C subtracts zones in int32 (wraps) before widening,
//!      shipped Rust widens first. This is the staged
//!      probe_dist_timetz_zone_wrap candidate (ratified-unreachable:
//!      timetz_in bounds |zone| to 16h); the bin must REPRODUCE it or the
//!      probe's plane has closed.
//!
//! Run: cargo run --release --bin native_dist_wrap   (MUST be --release:
//! debug shipped code panics on the wrap plane by design.)

use proof_brin_minmax as _; // bundle the vendored C archive (build.rs cc link)

use datum::Datum;
use proof_support::fcinfo::call2;
use types_fmgr::PGFunction;

extern "C" {
    fn pg_dist_time(a1: i64, a2: i64) -> f64;
    fn pg_dist_timetz(ta_time: i64, ta_zone: i32, tb_time: i64, tb_zone: i32) -> f64;
}

fn builtin(oid: u32) -> PGFunction {
    let t = brin_minmax_multi::MINMAX_MULTI_BUILTINS;
    t.iter().find(|e| e.foid == oid).expect("oid registered").func
}

#[repr(C, align(8))]
struct TimeTzImg([u8; 12]);

fn timetz_img(time: i64, zone: i32) -> TimeTzImg {
    let mut img = TimeTzImg([0u8; 12]);
    img.0[0..8].copy_from_slice(&time.to_ne_bytes());
    img.0[8..12].copy_from_slice(&zone.to_ne_bytes());
    img
}

// xorshift64* — deterministic sweep, no deps.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn i64(&mut self) -> i64 {
        self.next() as i64
    }
    fn zone_inbound(&mut self) -> i32 {
        (self.next() % 172_801) as i32 - 86_400
    }
}

fn main() {
    let fc_time = builtin(4630);
    let fc_timetz = builtin(4632);
    let dist_time = |a: i64, b: i64| -> f64 {
        call2(fc_time, a, b).expect("dist_time infallible").as_f64()
    };
    let dist_timetz = |ta: i64, za: i32, tb: i64, zb: i32| -> f64 {
        let ia = timetz_img(ta, za);
        let ib = timetz_img(tb, zb);
        let r = call2(
            fc_timetz,
            Datum::from_usize(ia.0.as_ptr() as usize),
            Datum::from_usize(ib.0.as_ptr() as usize),
        )
        .expect("dist_timetz infallible")
        .as_f64();
        r
    };

    let mut checked = 0u64;
    let mut diffs = 0u64;
    let mut check_time = |a: i64, b: i64| {
        let r = dist_time(a, b);
        let c = unsafe { pg_dist_time(a, b) };
        checked += 1;
        if r.to_bits() != c.to_bits() {
            diffs += 1;
            eprintln!("DIFF dist_time a={a} b={b} rust={r:?} c={c:?}");
        }
    };

    // 1. dist_time wrap-boundary spots + sweep.
    let spots = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    for &a in &spots {
        for &b in &spots {
            check_time(a, b);
        }
    }
    let mut rng = Rng(0x9E3779B97F4A7C15);
    for _ in 0..2_000_000 {
        let a = rng.i64();
        let b = rng.i64();
        check_time(a, b);
    }
    println!("dist_time: {checked} checked, {diffs} diffs");
    let time_diffs = diffs;

    // 2. dist_timetz: full-random times x in-bound zones + wrap-boundary times.
    let checked2 = std::cell::Cell::new(0u64);
    let diffs2 = std::cell::Cell::new(0u64);
    let check_tz = |ta: i64, za: i32, tb: i64, zb: i32, expect_diff: bool| -> bool {
        let r = dist_timetz(ta, za, tb, zb);
        let c = unsafe { pg_dist_timetz(ta, za, tb, zb) };
        let diff = r.to_bits() != c.to_bits();
        if !expect_diff {
            checked2.set(checked2.get() + 1);
            if diff {
                diffs2.set(diffs2.get() + 1);
                eprintln!("DIFF dist_timetz ta={ta} za={za} tb={tb} zb={zb} rust={r:?} c={c:?}");
            }
        }
        diff
    };
    for &ta in &spots {
        for &tb in &spots {
            check_tz(ta, 86_400, tb, -86_400, false);
            check_tz(ta, 0, tb, 0, false);
        }
    }
    for _ in 0..2_000_000 {
        let (ta, tb) = (rng.i64(), rng.i64());
        let (za, zb) = (rng.zone_inbound(), rng.zone_inbound());
        check_tz(ta, za, tb, zb, false);
    }
    println!("dist_timetz (in-bound zones): {} checked, {} diffs", checked2.get(), diffs2.get());

    // 3. Zone-wrap plane: MUST reproduce the known C-int32-wrap divergence.
    let mut wrap_witnessed = 0u32;
    for &(za, zb) in &[
        (i32::MIN, i32::MAX),
        (i32::MAX, i32::MIN),
        (i32::MIN, 1),
        (-2, i32::MAX),
    ] {
        if check_tz(0, za, 0, zb, true) {
            wrap_witnessed += 1;
            let r = dist_timetz(0, za, 0, zb);
            let c = unsafe { pg_dist_timetz(0, za, 0, zb) };
            println!("zone-wrap witness za={za} zb={zb}: rust={r:?} c={c:?}");
        }
    }
    println!("zone-wrap plane: {wrap_witnessed}/4 witness points diverged (expected: all 4)");

    if time_diffs == 0 && diffs2.get() == 0 && wrap_witnessed == 4 {
        println!("VERDICT: wrap-plane parity holds (Kani FAILEDs are overflow-check artifacts); zone-wrap divergence reproduced (probe candidate value-visible only outside validated zones)");
    } else {
        println!("VERDICT: UNEXPECTED — investigate");
        std::process::exit(1);
    }
}
