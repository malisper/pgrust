//! Owned-value Rust port of PostgreSQL's `pseudorandomfuncs.c`
//! (`src/backend/utils/adt/pseudorandomfuncs.c`) -- SQL access to the
//! pseudorandom number generator: `random()` (`drandom`), `random_normal()`
//! (`drandom_normal`), `setseed()`, `random(int4, int4)` (`int4random`),
//! `random(int8, int8)` (`int8random`), and `random(numeric, numeric)`
//! (`numeric_random`).
//!
//! Every function defined in the C file is ported here with its original name
//! and 1:1 logic (control flow, bound checks, branch order, message text and
//! SQLSTATE preserved). The shared per-process PRNG state (`prng_state` plus the
//! `prng_seed_set` flag) is modelled faithfully as one process-global `Mutex`,
//! mirroring the two C file-scope statics
//! (`static pg_prng_state prng_state;` and `static bool prng_seed_set = false;`).
//!
//! ## Owned values, no `extern "C"`
//!
//! The PRNG itself is the in-process [`pg_prng`] crate: the C primitives map
//! onto its owned [`PgPrng`] methods --
//! `pg_prng_seed` -> [`PgPrng::seed`], `pg_prng_fseed` ->
//! [`PgPrng::seed_from_f64`], `pg_prng_double` -> [`PgPrng::next_f64`],
//! `pg_prng_double_normal` -> [`PgPrng::normal_f64`], `pg_prng_int64_range` ->
//! [`PgPrng::i64_range`] (inclusive, returning `rmin` on an empty range --
//! exactly as `pg_prng_int64_range` in `common/pg_prng.c`).
//!
//! `numeric_random` delegates to `random_numeric` in the sibling
//! [`backend_utils_adt_numeric`] crate, threading this crate's shared
//! [`PgPrng`] state into it -- exactly as the C body calls
//! `random_numeric(&prng_state, rmin, rmax)`. The C entry point unpacks two
//! on-disk `Numeric` Datums and returns one; that `PG_GETARG_NUMERIC` /
//! `PG_RETURN_NUMERIC` (toast/Datum boundary) conversion is the systemic
//! fmgr/Datum deferral, so this core takes and returns the on-disk `numeric`
//! byte images that `backend_utils_adt_numeric::random::random_numeric` works
//! on (its NaN/infinity bound rejection lives there, exactly as `random_var` in
//! `numeric.c`).
//!
//! Soft errors flow through [`types_error`].
//!
//! ## Seeding fallback (`initialize_prng`)
//!
//! C seeds the PRNG lazily: it first tries `pg_prng_strong_seed` (OS entropy),
//! and only on failure mixes `GetCurrentTimestamp()` with `MyProcPid`. The
//! in-repo [`pg_prng`] crate provides no strong-seed (OS-entropy) primitive, so
//! -- exactly as the faithful `src/` port models `pg_prng_strong_seed` as
//! conservatively returning `false` -- this port always takes the documented
//! timestamp/PID fallback. `GetCurrentTimestamp()` is genuinely external (the
//! timestamp subsystem is a separate owner) and is reached through the
//! `get_current_timestamp` seam; `MyProcPid` is a plain process global read
//! through the `my_proc_pid` seam.
//!
//! ## Systemic deferrals (project-wide, not crate gaps)
//!
//! * The fmgr `Datum NAME(PG_FUNCTION_ARGS)` shim layer -- argument unpacking
//!   (`PG_GETARG_*`), `PG_RETURN_*`, and the on-disk `Numeric` conversions in
//!   `numeric_random` -- is deferred pending the fmgr/Datum subsystem. The
//!   owned logic is implemented as plain-Rust typed entry points
//!   (`f64`/`i32`/`i64`/`&[u8]`).
//! * `_()` gettext translation wrapping of message strings.
//! * Exact `printf("%g", ...)` float formatting in the `setseed` error message;
//!   the compact default rendering is used and only fires on the
//!   out-of-range / NaN inputs `setseed` rejects.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::sync::{Mutex, MutexGuard};

use mcx::{Mcx, PgVec};
use pg_prng::PgPrng;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use backend_utils_adt_numeric::random::random_numeric;
use backend_utils_adt_timestamp_seams::get_current_timestamp;
use backend_utils_init_small_seams::my_proc_pid;

/// Shared PRNG state used by all the random functions, plus the
/// `prng_seed_set` flag, held together under one lock to mirror the two C
/// file-scope statics (`static pg_prng_state prng_state;` and
/// `static bool prng_seed_set = false;`).
struct PrngGlobal {
    state: PgPrng,
    seed_set: bool,
}

static PRNG: Mutex<PrngGlobal> = Mutex::new(PrngGlobal {
    state: PgPrng::from_raw(0, 0),
    seed_set: false,
});

/// `initialize_prng()` (pseudorandomfuncs.c:33) --
///
/// Initialize (seed) the PRNG, if not done yet in this process.
fn initialize_prng(g: &mut PrngGlobal) {
    if !g.seed_set {
        // If possible, seed the PRNG using high-quality random bits. Should
        // that fail for some reason, we fall back on a lower-quality seed
        // based on current time and PID.
        //
        // The in-repo `pg-prng` crate has no strong-seed (OS-entropy)
        // primitive, so -- as the faithful `src/` port models
        // `pg_prng_strong_seed` returning `false` -- we always take the
        // documented timestamp/PID fallback.
        let now = get_current_timestamp::call();

        // Mix the PID with the most predictable bits of the timestamp.
        let iseed: u64 = (now as u64) ^ ((my_proc_pid::call() as u64) << 32);
        g.state.seed(iseed);

        g.seed_set = true;
    }
}

/// `setseed()` (pseudorandomfuncs.c:61) --
///
/// Seed the PRNG from a specified value in the range [-1.0, 1.0].
pub fn setseed(seed: f64) -> PgResult<()> {
    if seed < -1.0 || seed > 1.0 || seed.is_nan() {
        return Err(PgError::error(format!(
            "setseed parameter {} is out of allowed range [-1,1]",
            fmt_g(seed)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let mut g = lock();
    g.state.seed_from_f64(seed);
    g.seed_set = true;

    Ok(())
}

/// `drandom()` (pseudorandomfuncs.c:83) --
///
/// Returns a random number chosen uniformly in the range [0.0, 1.0).
pub fn drandom() -> f64 {
    let mut g = lock();
    initialize_prng(&mut g);

    // pg_prng_double produces desired result range [0.0, 1.0)
    g.state.next_f64()
}

/// `drandom_normal()` (pseudorandomfuncs.c:101) --
///
/// Returns a random number from a normal distribution.
pub fn drandom_normal(mean: f64, stddev: f64) -> f64 {
    let mut g = lock();
    initialize_prng(&mut g);

    // Get random value from standard normal(mean = 0.0, stddev = 1.0)
    let z = g.state.normal_f64();
    // Transform the normal standard variable (z)
    // using the target normal distribution parameters
    (stddev * z) + mean
}

/// `int4random()` (pseudorandomfuncs.c:125) --
///
/// Returns a random 32-bit integer chosen uniformly in the specified range.
pub fn int4random(rmin: i32, rmax: i32) -> PgResult<i32> {
    if rmin > rmax {
        return Err(
            PgError::error("lower bound must be less than or equal to upper bound")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    let mut g = lock();
    initialize_prng(&mut g);

    let result = g.state.i64_range(rmin as i64, rmax as i64) as i32;

    Ok(result)
}

/// `int8random()` (pseudorandomfuncs.c:149) --
///
/// Returns a random 64-bit integer chosen uniformly in the specified range.
pub fn int8random(rmin: i64, rmax: i64) -> PgResult<i64> {
    if rmin > rmax {
        return Err(
            PgError::error("lower bound must be less than or equal to upper bound")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    let mut g = lock();
    initialize_prng(&mut g);

    let result = g.state.i64_range(rmin, rmax);

    Ok(result)
}

/// `numeric_random()` (pseudorandomfuncs.c:173) --
///
/// Returns a random numeric value chosen uniformly in the specified range.
///
/// The C entrypoint unpacks two on-disk `Numeric` Datums and returns one; that
/// `PG_GETARG_NUMERIC` / `PG_RETURN_NUMERIC` (disk image) conversion is the
/// systemic fmgr/Datum deferral, so this core takes and returns the on-disk
/// `numeric` byte images, delegating to `random_numeric` exactly as the C body
/// does -- threading this crate's shared PRNG state, so a single seed stream
/// backs every `random*()` variant (as the C shares one `static
/// pg_prng_state`). The `mcx` is the transient context the result image is
/// allocated in, mirroring the fmgr per-call context.
pub fn numeric_random<'mcx>(
    mcx: Mcx<'mcx>,
    rmin: &[u8],
    rmax: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut g = lock();
    initialize_prng(&mut g);

    random_numeric(mcx, &mut g.state, rmin, rmax)
}

fn lock() -> MutexGuard<'static, PrngGlobal> {
    PRNG.lock().expect("pseudorandom PRNG state lock poisoned")
}

/// Render a float for the `setseed` error message, modelling C's `%g`.
///
/// Exact `printf("%g", ...)` formatting is a project-wide concern; this path
/// only fires for the out-of-range / NaN inputs `setseed` rejects, where the
/// compact default rendering matches `%g` for the human-entered values
/// involved.
fn fmt_g(v: f64) -> String {
    format!("{}", v)
}

/// Install this owner's seams: the `setseed` seam declared by
/// `backend-commands-variable-seams` (`commands/variable.c`'s
/// `DirectFunctionCall1(setseed, ...)`), now backed by the real `setseed`
/// above.
pub fn init_seams() {
    backend_commands_variable_seams::setseed::set(setseed);
}

#[cfg(test)]
mod tests;
