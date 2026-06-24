//! Port of PostgreSQL's resource-usage measurement support routines
//! (`src/backend/utils/misc/pg_rusage.c`).
//!
//! `pg_rusage_init` captures a wall-clock + CPU-usage snapshot;
//! `pg_rusage_show` captures a fresh snapshot and formats the elapsed
//! user/system/wall deltas since an earlier snapshot. Used by VACUUM/ANALYZE/
//! CLUSTER/index-build progress messages.
//!
//! Differences from the C:
//! * the snapshot ([`PgRUsage`]) stores only the three `struct timeval`s the
//!   code ever reads (wall clock, `ru_utime`, `ru_stime`), as plain integers;
//! * [`pg_rusage_show`] returns an owned [`String`] instead of borrowing the
//!   C version's non-reentrant `static char[100]`.
//!
//! mcx decision (2026-06-12 migration pass): this crate deliberately does NOT
//! take `Mcx<'mcx>` / return `PgString<'mcx>`. C's `pg_rusage_show` never
//! pallocs — it formats into a `static char result[100]` and returns a
//! pointer into it, so there is no current-context allocation to translate.
//! The faithful alternatives were a per-backend `thread_local!` buffer
//! (reproducing the non-reentrancy the C comment itself calls "tacky") or an
//! owned `String`; the owned return is strictly safer and keeps the function
//! infallible like the C call sites expect (no palloc means OOM is not part
//! of its failure surface, so no `PgResult`).

#[cfg(not(target_family = "wasm"))]
use std::mem::MaybeUninit;
#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};

// `Timeval`/`PgRUsage` (`utils/pg_rusage.h`) are canonically defined in
// `types_rusage` (the shared types layer cluster/clean-seams also consume);
// re-exported here so existing paths keep working.
pub use types_rusage::{PgRUsage, Timeval};

/// Capture a fresh snapshot; constructor form of [`pg_rusage_init`]
/// (C `pg_rusage_init`). Free function rather than an inherent method because
/// the type now lives in `types_rusage`.
pub fn pg_rusage_new() -> PgRUsage {
    let mut ru0 = PgRUsage::default();
    pg_rusage_init(&mut ru0);
    ru0
}

/// `gettimeofday(&tv, NULL)` -> `(tv_sec, tv_usec)`.
#[cfg(not(target_family = "wasm"))]
fn os_gettimeofday() -> (i64, i64) {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (dur.as_secs() as i64, dur.subsec_micros() as i64)
}

/// wasm (single-process) stub: `std::time::SystemTime::now()` panics on
/// `wasm64-unknown-unknown` (no clock host import), so the wall-clock snapshot
/// reports zero. Only affects the cosmetic "elapsed" progress strings in
/// VACUUM/ANALYZE/CLUSTER and recovery logging.
#[cfg(target_family = "wasm")]
fn os_gettimeofday() -> (i64, i64) {
    (0, 0)
}

/// `getrusage(RUSAGE_SELF, &ru)` -> `(ru_utime.tv_sec, ru_utime.tv_usec,
/// ru_stime.tv_sec, ru_stime.tv_usec)`. On failure (which `getrusage`
/// essentially never returns for RUSAGE_SELF) reports zeros — the benign
/// degradation C would get from an all-zero `struct rusage`.
#[cfg(not(target_family = "wasm"))]
fn os_getrusage_self() -> (i64, i64, i64, i64) {
    let mut ru: MaybeUninit<libc::rusage> = MaybeUninit::uninit();
    // SAFETY: `getrusage` fills `ru` for RUSAGE_SELF; read only on success.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) };
    if rc != 0 {
        return (0, 0, 0, 0);
    }
    let ru = unsafe { ru.assume_init() };
    (
        ru.ru_utime.tv_sec as i64,
        ru.ru_utime.tv_usec as i64,
        ru.ru_stime.tv_sec as i64,
        ru.ru_stime.tv_usec as i64,
    )
}

/// wasm (single-process) stub: `getrusage` is unavailable under
/// wasm32/wasm64-wasip1, so CPU-time accounting reports zero. Only affects the
/// cosmetic "CPU: user/system" progress strings in VACUUM/ANALYZE/CLUSTER;
/// wall-clock elapsed (via `gettimeofday`/`SystemTime`) is still accurate.
#[cfg(target_family = "wasm")]
fn os_getrusage_self() -> (i64, i64, i64, i64) {
    (0, 0, 0, 0)
}

/// Initialize usage snapshot. As in C, OS-call failure status is ignored.
pub fn pg_rusage_init(ru0: &mut PgRUsage) {
    let (utime_sec, utime_usec, stime_sec, stime_usec) = os_getrusage_self();
    ru0.ru_utime = Timeval::new(utime_sec, utime_usec);
    ru0.ru_stime = Timeval::new(stime_sec, stime_usec);

    let (tv_sec, tv_usec) = os_gettimeofday();
    ru0.tv = Timeval::new(tv_sec, tv_usec);
}

/// Compute elapsed time since `ru0` usage snapshot, and format into a
/// displayable string.
pub fn pg_rusage_show(ru0: &PgRUsage) -> String {
    let ru1 = pg_rusage_new();
    pg_rusage_show_between(ru0, &ru1)
}

/// The pure core of C `pg_rusage_show` (everything after `pg_rusage_init(&ru1)`):
/// the borrow-a-second microsecond fixup, the `int`-narrowed subtractions, and
/// the `%d.%02d`-formatted output. Split out so the deterministic arithmetic is
/// testable without the OS clock.
pub fn pg_rusage_show_between(ru0: &PgRUsage, ru1: &PgRUsage) -> String {
    let delta = PgRUsageDelta::between(ru0, ru1);

    // _("CPU: user: %d.%02d s, system: %d.%02d s, elapsed: %d.%02d s")
    // The gettext `_()` wrapper is a project-wide systemic deferral; the
    // literal format string is ported verbatim. The centisecond fields are in
    // [0, 99], so `{:02}` matches `%02d` exactly.
    format!(
        "CPU: user: {}.{:02} s, system: {}.{:02} s, elapsed: {}.{:02} s",
        delta.user_sec,
        delta.user_centis,
        delta.system_sec,
        delta.system_centis,
        delta.elapsed_sec,
        delta.elapsed_centis,
    )
}

/// The six integers printed by [`pg_rusage_show_between`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PgRUsageDelta {
    user_sec: i32,
    user_centis: i32,
    system_sec: i32,
    system_centis: i32,
    elapsed_sec: i32,
    elapsed_centis: i32,
}

impl PgRUsageDelta {
    /// C casts each difference to `(int)` individually, and for the usec term
    /// the `/ 10000` applies to the already-`int`-cast difference (the cast
    /// binds tighter than `/`). [`elapsed_pair`] returns the two `i32`
    /// differences; `/ 10000` is applied here, matching the C exactly.
    fn between(start: &PgRUsage, end: &PgRUsage) -> Self {
        let (elapsed_sec, elapsed_usec) = elapsed_pair(start.tv, end.tv);
        let (system_sec, system_usec) = elapsed_pair(start.ru_stime, end.ru_stime);
        let (user_sec, user_usec) = elapsed_pair(start.ru_utime, end.ru_utime);

        Self {
            user_sec,
            user_centis: user_usec / 10_000,
            system_sec,
            system_centis: system_usec / 10_000,
            elapsed_sec,
            elapsed_centis: elapsed_usec / 10_000,
        }
    }
}

/// Apply C's borrow-a-second fixup to `end` and return the `(int)`-narrowed
/// `(sec_delta, usec_delta)` differences.
fn elapsed_pair(start: Timeval, mut end: Timeval) -> (i32, i32) {
    if end.tv_usec < start.tv_usec {
        end.tv_sec -= 1;
        end.tv_usec += 1_000_000;
    }

    (
        (end.tv_sec - start.tv_sec) as i32,
        (end.tv_usec - start.tv_usec) as i32,
    )
}

/// Install this crate's seams. The start snapshot is the caller's own value:
/// `pg_rusage_init` returns it and `pg_rusage_show` formats the delta.
pub fn init_seams() {
    pg_rusage_seams::pg_rusage_init::set(pg_rusage_new);
    pg_rusage_seams::pg_rusage_show::set(|ru0| pg_rusage_show(&ru0));

    // The `utils/misc/pg_rusage.c` unit is canonically named
    // `backend-utils-misc-clean`; CLUSTER / VACUUM FULL reach `pg_rusage_init`
    // / `pg_rusage_show` through that seam crate. Install them here (this is the
    // owner of the bodies). `pg_rusage_show` there returns an mcx `PgString`
    // (the verbose-message sink pallocs); marshal the owned `String` into the
    // current context.
    backend_utils_misc_clean_seams::pg_rusage_init::set(pg_rusage_new);
    backend_utils_misc_clean_seams::pg_rusage_show::set(|mcx, ru0| {
        mcx::PgString::from_str_in(&pg_rusage_show(&ru0), mcx)
    });
}

#[cfg(test)]
mod tests;
