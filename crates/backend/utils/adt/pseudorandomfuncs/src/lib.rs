pub mod builtins;
#[cfg(test)]
mod tests;

use std::cell::Cell;

use ::adt_numeric::var::NumericImage;
use ::adt_numeric::Num;
use ::pg_prng::PgPrng;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

std::thread_local! {
    // C file-scope statics prng_state + prng_seed_set (backend-private).
    static PRNG: Cell<PgPrng> = const { Cell::new(PgPrng::from_raw(0, 0)) };
    static SEED_SET: Cell<bool> = const { Cell::new(false) };
}

fn with_prng<R>(f: impl FnOnce(&mut PgPrng) -> R) -> R {
    initialize_prng();
    PRNG.with(|cell| {
        let mut state = cell.get();
        let r = f(&mut state);
        cell.set(state);
        r
    })
}

fn initialize_prng() {
    if !SEED_SET.get() {
        let mut state = strong_seed().unwrap_or_else(|| {
            let now = ::adt_timestamp::GetCurrentTimestamp();
            let iseed = now as u64 ^ ((::init_small::globals::MyProcPid() as u64) << 32);
            PgPrng::seeded(iseed)
        });
        state.ensure_seeded();
        PRNG.set(state);
        SEED_SET.set(true);
    }
}

fn strong_seed() -> Option<PgPrng> {
    let mut bytes = [0u8; 16];
    if !pg_strong_random::pg_strong_random(&mut bytes) {
        return None;
    }
    Some(PgPrng::from_raw(
        u64::from_ne_bytes(bytes[..8].try_into().unwrap()),
        u64::from_ne_bytes(bytes[8..].try_into().unwrap()),
    ))
}

#[cold]
#[inline(never)]
fn setseed_range_error(seed: f64) -> PgError {
    PgError::error(format!(
        "setseed parameter {} is out of allowed range [-1,1]",
        fmt_g(seed)
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// C printf %g, default precision 6 (guc::units::fmt_g's contract; that crate
// sits above this one in the dep order).
fn fmt_g(v: f64) -> String {
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v < 0.0 { "-inf" } else { "inf" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    let e_str = format!("{:.5e}", v);
    let x: i32 = e_str.rsplit('e').next().and_then(|s| s.parse().ok()).unwrap_or(0);
    if x < -4 || x >= 6 {
        let (mant, _) = e_str.rsplit_once('e').unwrap();
        let mant = mant.trim_end_matches('0').trim_end_matches('.');
        return format!("{mant}e{}{:02}", if x < 0 { '-' } else { '+' }, x.abs());
    }
    let s = format!("{:.*}", (6 - 1 - x).max(0) as usize, v);
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

pub fn setseed(seed: f64) -> PgResult<()> {
    if !(-1.0..=1.0).contains(&seed) || seed.is_nan() {
        return Err(setseed_range_error(seed).into());
    }
    PRNG.with(|cell| {
        let mut state = cell.get();
        state.fseed(seed);
        cell.set(state);
    });
    SEED_SET.set(true);
    Ok(())
}

pub fn drandom() -> f64 {
    with_prng(PgPrng::next_f64)
}

pub fn drandom_normal(mean: f64, stddev: f64) -> f64 {
    let z = with_prng(PgPrng::normal_f64);
    stddev * z + mean
}

#[cold]
#[inline(never)]
fn bound_order_error() -> PgError {
    PgError::error("lower bound must be less than or equal to upper bound")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

pub fn int4random(rmin: i32, rmax: i32) -> PgResult<i32> {
    if rmin > rmax {
        return Err(bound_order_error().into());
    }
    Ok(with_prng(|s| s.i64_range(rmin as i64, rmax as i64)) as i32)
}

pub fn int8random(rmin: i64, rmax: i64) -> PgResult<i64> {
    if rmin > rmax {
        return Err(bound_order_error().into());
    }
    Ok(with_prng(|s| s.i64_range(rmin, rmax)))
}

pub fn numeric_random(rmin: Num<'_>, rmax: Num<'_>) -> PgResult<NumericImage> {
    with_prng(|s| ::adt_numeric::random::random_numeric(s, rmin, rmax))
}
