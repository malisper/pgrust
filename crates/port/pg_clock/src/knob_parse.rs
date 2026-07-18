//! Pure parse functions for the WS-CLK sim knobs (contract §2.3). These are
//! deliberately OUTSIDE `cfg(pgrust_sim)` so their unit corpus runs in every
//! test build; the env READS live in `sim.rs` and exist only under
//! `--cfg pgrust_sim` (no product reader exists).

/// `PGRUST_SIM_CLOCK_MODE`: `frozen` (default) | `tick:<ns>` | `driven`.
/// Unparsable input → `Frozen` (contract §2.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimClockMode {
    /// Mono never advances (the P2 smoke default).
    Frozen,
    /// Mono += quantum per mono read (fallback lever for corpora that
    /// busy-check elapsed time).
    Tick(u64),
    /// `advance()` only — the P3 scheduler mode.
    Driven,
}

pub fn parse_clock_mode(s: &str) -> SimClockMode {
    let s = s.trim();
    if s.eq_ignore_ascii_case("frozen") || s.is_empty() {
        return SimClockMode::Frozen;
    }
    if s.eq_ignore_ascii_case("driven") {
        return SimClockMode::Driven;
    }
    if let Some(q) = s.strip_prefix("tick:") {
        if let Ok(ns) = q.trim().parse::<u64>() {
            if ns > 0 {
                return SimClockMode::Tick(ns);
            }
        }
    }
    SimClockMode::Frozen
}

/// `PGRUST_SIM_WALL_BASE`: unix-epoch nanoseconds, hex (`0x…`) or decimal.
/// Returns `None` on bad hex / overflow / empty (caller falls back to
/// [`DEFAULT_WALL_BASE_NS`]).
pub fn parse_wall_base(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Some(h) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return i64::from_str_radix(h, 16).ok();
    }
    s.parse::<i64>().ok()
}

/// Default sim wall base: 2026-01-01T00:00:00Z as unix-epoch ns
/// (contract §2.3 fixed constant).
pub const DEFAULT_WALL_BASE_NS: i64 = 1_767_225_600 * 1_000_000_000;
