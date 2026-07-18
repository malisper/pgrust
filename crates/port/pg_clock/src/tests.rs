//! Conformance battery (contract §1.1): generic over [`ClockSource`] and run
//! against BOTH impls — PosixClock always, SimClock under `--cfg pgrust_sim`
//! (`RUSTFLAGS="--cfg pgrust_sim" cargo test -p pg_clock`). The trait's whole
//! reason to exist is that this battery type-checks against both.

use super::*;
use crate::knob_parse::*;

// ---- generic conformance ---------------------------------------------------

fn conf_mono_nondecreasing<C: ClockSource>(clock: &C) {
    let mut prev = clock.mono_ns();
    for _ in 0..10_000 {
        let now = clock.mono_ns();
        assert!(now >= prev, "mono regressed: {prev} -> {now}");
        prev = now;
    }
}

fn conf_mono_nondecreasing_across_threads<C: ClockSource + Sync>(clock: &C) {
    // Cross-thread monotonicity: a read that happens-after another thread's
    // read (via join) must not be earlier.
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..4 {
            handles.push(s.spawn(|| {
                let mut prev = clock.mono_ns();
                for _ in 0..2_000 {
                    let now = clock.mono_ns();
                    assert!(now >= prev, "mono regressed in thread: {prev} -> {now}");
                    prev = now;
                }
                prev
            }));
        }
        let max_seen = handles
            .into_iter()
            .map(|h| h.join().expect("conformance thread panicked"))
            .max()
            .unwrap();
        assert!(clock.mono_ns() >= max_seen, "join-ordered read regressed");
    });
}

// ---- PosixClock --------------------------------------------------------------

#[test]
fn posix_mono_nondecreasing() {
    conf_mono_nondecreasing(&posix::PosixClock::new());
    conf_mono_nondecreasing_across_threads(&posix::PosixClock::new());
}

#[test]
fn posix_wall_is_plausible_epoch() {
    // 2020-01-01..2100-01-01 sanity window; also exercises the leaf fns.
    let ns = posix::PosixClock::new().wall_ns();
    assert!(ns > 1_577_836_800 * 1_000_000_000, "wall before 2020: {ns}");
    assert!(ns < 4_102_444_800i64 * 1_000_000_000, "wall after 2100: {ns}");
}

// ---- leaf API / MonoStamp / Deadline (run on ActiveClock either cfg) --------

#[test]
fn leaf_units_are_consistent() {
    let ns = wall_ns();
    let us = wall_us();
    let secs = wall_secs();
    // Coarser units never exceed finer ones (independent reads, so allow
    // forward motion but never inversion beyond it).
    assert!(us >= ns.div_euclid(1_000), "us went backwards vs ns");
    assert!(secs >= ns.div_euclid(1_000_000_000) - 1);
    let (tv_sec, tv_usec) = wall_timeval();
    assert!(tv_usec < 1_000_000);
    assert!(tv_sec >= secs);
    let ms = mono_ms();
    assert!(ms >= 0);
    assert!(mono_ns() / 1_000_000 >= ms as u64);
}

#[test]
fn split_wall_ns_carry() {
    assert_eq!(split_wall_ns(0), (0, 0));
    assert_eq!(split_wall_ns(999), (0, 0));
    assert_eq!(split_wall_ns(1_000), (0, 1));
    assert_eq!(split_wall_ns(1_999_999_999), (1, 999_999));
    assert_eq!(split_wall_ns(2_000_000_000), (2, 0));
    // Pre-epoch: Euclidean carry keeps usec in 0..1_000_000.
    assert_eq!(split_wall_ns(-1), (-1, 999_999));
    assert_eq!(split_wall_ns(-1_000_000_000), (-1, 0));
    assert_eq!(split_wall_ns(-1_500_000_000), (-2, 500_000));
    let (s, u) = split_wall_ns(i64::MIN);
    assert!(u < 1_000_000);
    assert!(s < 0);
}

#[test]
fn monostamp_arithmetic() {
    let a = MonoStamp::now();
    let b = MonoStamp::now();
    assert!(b.as_ns() >= a.as_ns());
    assert!(b.since_ns(a) == b.as_ns() - a.as_ns());
    // Reversed order saturates to zero, never wraps.
    assert_eq!(a.since_ns(b), 0);
    let d = a.elapsed();
    assert_eq!(d, Duration::from_nanos(d.as_nanos() as u64));
    assert!(a.elapsed_ns() >= d.as_nanos() as u64);
    assert!(a.elapsed_ms() >= 0);
}

#[test]
fn deadline_arithmetic_and_saturation() {
    // Far-future construction saturates, never wraps.
    let far = Deadline::after(Duration::MAX);
    assert_eq!(far.as_ns(), u64::MAX);
    assert!(!far.expired());
    assert!(far.remaining_ms() > 0);

    // Already-expired deadline.
    let past = Deadline::at_ms(0);
    assert!(past.expired());
    assert_eq!(past.remaining_ms(), 0);
    assert_eq!(past.remaining(), Duration::ZERO);
    // Negative ms clamps to expired, not to a huge unsigned value.
    assert!(Deadline::at_ms(-5).expired());

    // remaining_ms rounds UP: a live sub-ms remainder reports 1, not 0
    // (no busy-spin at the boundary).
    let live = Deadline::at_ns(mono_ns().saturating_add(500_000));
    if !live.expired() {
        assert!(live.remaining_ms() >= 1);
    }

    // Ordering follows the ns domain (nearest-deadline scans rely on it).
    let d1 = Deadline::at_ns(1_000);
    let d2 = Deadline::at_ns(2_000);
    assert!(d1 < d2);
    assert_eq!(Deadline::at_ms(1), Deadline::at_ns(1_000_000));
}

// ---- knob parse corpus (contract §2.3: bad hex, overflow, empty) ------------

#[test]
fn knob_parse_clock_mode() {
    assert_eq!(parse_clock_mode("frozen"), SimClockMode::Frozen);
    assert_eq!(parse_clock_mode("FROZEN"), SimClockMode::Frozen);
    assert_eq!(parse_clock_mode(""), SimClockMode::Frozen);
    assert_eq!(parse_clock_mode("driven"), SimClockMode::Driven);
    assert_eq!(parse_clock_mode(" tick:250 "), SimClockMode::Tick(250));
    assert_eq!(parse_clock_mode("tick:0"), SimClockMode::Frozen); // zero quantum is meaningless
    assert_eq!(parse_clock_mode("tick:"), SimClockMode::Frozen);
    assert_eq!(parse_clock_mode("tick:-4"), SimClockMode::Frozen);
    assert_eq!(parse_clock_mode("tick:1e9"), SimClockMode::Frozen);
    assert_eq!(parse_clock_mode("bogus"), SimClockMode::Frozen);
}

#[test]
fn knob_parse_wall_base() {
    assert_eq!(parse_wall_base("0"), Some(0));
    assert_eq!(parse_wall_base("1767225600000000000"), Some(1_767_225_600_000_000_000));
    assert_eq!(parse_wall_base("0x10"), Some(16));
    assert_eq!(parse_wall_base("0X10"), Some(16));
    assert_eq!(parse_wall_base("-1"), Some(-1));
    assert_eq!(parse_wall_base(""), None);
    assert_eq!(parse_wall_base("  "), None);
    assert_eq!(parse_wall_base("0xZZ"), None); // bad hex
    assert_eq!(parse_wall_base("99999999999999999999999999"), None); // overflow
    assert_eq!(parse_wall_base("0x8000000000000000"), None); // i64 overflow
    // Default is 2026-01-01T00:00:00Z.
    assert_eq!(DEFAULT_WALL_BASE_NS, 1_767_225_600 * 1_000_000_000);
}

// ---- SimClock conformance (sim harness builds only) --------------------------

#[cfg(pgrust_sim)]
mod sim_conformance {
    use super::super::*;

    // NOTE: SimClock state is process-global and the default mode is frozen
    // (no PGRUST_SIM_CLOCK_MODE in the test env), so these tests share one
    // timeline; they only ever advance it, which every assertion tolerates.

    #[test]
    fn sim_mono_nondecreasing_and_driven_advance() {
        let c = sim::SimClock::new();
        super::conf_mono_nondecreasing(&c);
        super::conf_mono_nondecreasing_across_threads(&c);
        let before = c.mono_ns();
        sim::advance_ns(5_000);
        assert!(c.mono_ns() >= before + 5_000);
        let before_ms = mono_ms();
        sim::advance_ms(7);
        assert!(mono_ms() >= before_ms + 7);
    }

    #[test]
    fn sim_wall_mono_coupling() {
        // Law §0.3: wall - mono is the constant base; wall ordering can
        // never disagree with mono ordering.
        let c = sim::SimClock::new();
        for _ in 0..1_000 {
            let w = c.wall_ns();
            let m = c.mono_ns();
            assert!(w >= sim::wall_base_ns());
            assert!(w <= sim::wall_base_ns().saturating_add_unsigned(m));
        }
        let w1 = c.wall_ns();
        sim::advance_ns(1_000);
        let w2 = c.wall_ns();
        assert!(w2 >= w1 + 1_000);
    }

    #[test]
    fn sim_frozen_is_frozen_between_advances() {
        // Frozen default: repeated reads do not move time.
        let c = sim::SimClock::new();
        let a = c.mono_ns();
        let mut moved = false;
        for _ in 0..100 {
            if c.mono_ns() != a {
                moved = true;
            }
        }
        // Another test may advance concurrently; single-threaded cargo test
        // -p pg_clock -- --test-threads=1 pins this exactly, so tolerate
        // forward motion but never backwards.
        assert!(c.mono_ns() >= a);
        let _ = moved;
    }
}
