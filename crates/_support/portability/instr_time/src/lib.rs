//! The OS-clock half of `portability/instr_time.h`. The `instr_time` type and
//! its pure-arithmetic `INSTR_TIME_*` operations live in
//! `::types_core::instrument` (dependency-free); this crate holds only the
//! pieces that need libc.

use ::types_core::instrument::{instr_time, NS_PER_S};

/// `pg_clock_gettime_ns()` — read `PG_INSTR_CLOCK` and convert to nanosecond
/// ticks (`tv_sec * NS_PER_S + tv_nsec`). PG picks `CLOCK_MONOTONIC_RAW` on
/// darwin (faster and higher resolution there) and `CLOCK_MONOTONIC`
/// elsewhere. Like the C inline, the (cannot-fail-for-these-args) return code
/// is ignored.
pub fn pg_clock_gettime_ns() -> instr_time {
    #[cfg(target_os = "macos")]
    const PG_INSTR_CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC_RAW;
    #[cfg(not(target_os = "macos"))]
    const PG_INSTR_CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;

    let mut tmp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime fills `tmp`; a valid clock id and out pointer.
    unsafe {
        libc::clock_gettime(PG_INSTR_CLOCK, &mut tmp);
    }
    instr_time {
        ticks: (tmp.tv_sec as i64) * NS_PER_S + tmp.tv_nsec as i64,
    }
}

/// `INSTR_TIME_SET_CURRENT(t)`.
pub fn instr_time_set_current(time: &mut instr_time) {
    *time = pg_clock_gettime_ns();
}

/// `INSTR_TIME_SET_CURRENT_LAZY(t)` — set `t` to the current time only if it
/// is zero; returns whether `t` was set.
pub fn instr_time_set_current_lazy(time: &mut instr_time) -> bool {
    if time.is_zero() {
        *time = pg_clock_gettime_ns();
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_is_monotonic_and_nonzero() {
        let a = pg_clock_gettime_ns();
        let b = pg_clock_gettime_ns();
        assert!(a.ticks > 0);
        assert!(b.ticks >= a.ticks);
    }

    #[test]
    fn set_current_lazy_only_sets_zero() {
        let mut t = instr_time::default();
        assert!(instr_time_set_current_lazy(&mut t));
        let set = t;
        assert!(!instr_time_set_current_lazy(&mut t));
        assert_eq!(t, set);
    }
}
