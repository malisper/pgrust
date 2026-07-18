//! Sim-scheduler knob parsing (R-KNOBS idiom, mirroring
//! `pg_strong_random/src/sim.rs`: one env reader per knob behind a OnceLock,
//! pure parse fns with a unit corpus, unparsable falls back to the default).
//!
//! Knobs (all `cfg(pgrust_sim)`-only — no product reader exists):
//!  - `PGRUST_SIM_SCHED`       — "1"/"true"/"on" enables the GLOBAL permit
//!                               scheduler (default OFF: registration at the
//!                               spawn door is a no-op, so existing sim
//!                               corpora — dst-determinism-smoke,
//!                               sim-net-e2e single-backend — are unaffected
//!                               until a harness opts in).
//!  - `PGRUST_SIM_PREEMPT_P`   — seeded-preemption probability at
//!                               non-blocking touches (contract §3.1: the
//!                               diversity dial; default on, small p = 0.05).
//!  - `PGRUST_SIM_WATCHDOG_S`  — watchdog no-handoff threshold in wall
//!                               seconds (contract §3.2; default 30).
//!  - `PGRUST_SIM_SCHEDLOG`    — "stream" additionally streams every SCHEDOP
//!                               line to stderr as emitted (WS-DEMO's
//!                               byte-compare capture); default ring-only.
//!
//! NOTE: `PGRUST_SIM_SEED` is deliberately NOT read here. The picker seed is
//! one 8-byte fill drawn through `pg_strong_random` — the sanctioned
//! SimEntropy funnel keyed by `(PGRUST_SIM_SEED, fill_no)` — so the
//! scheduler inherits the master seed without a second env reader.

use std::sync::OnceLock;

/// Default preemption probability (contract §3.1 "default on, small p").
pub const DEFAULT_PREEMPT_P: f64 = 0.05;
/// Default watchdog threshold, wall seconds (contract §3.2).
pub const DEFAULT_WATCHDOG_S: u64 = 30;

pub(crate) fn sched_enabled() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_SIM_SCHED")
            .ok()
            .map(|v| parse_bool_knob(&v))
            .unwrap_or(false)
    })
}

pub(crate) fn preempt_p() -> f64 {
    static V: OnceLock<f64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_SIM_PREEMPT_P")
            .ok()
            .and_then(|v| parse_probability(&v))
            .unwrap_or(DEFAULT_PREEMPT_P)
    })
}

pub(crate) fn watchdog_timeout_ms() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_SIM_WATCHDOG_S")
            .ok()
            .and_then(|v| parse_seconds(&v))
            .unwrap_or(DEFAULT_WATCHDOG_S)
            .saturating_mul(1000)
    })
}

pub(crate) fn stream_schedlog() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_SIM_SCHEDLOG")
            .ok()
            .map(|v| v.trim().eq_ignore_ascii_case("stream"))
            .unwrap_or(false)
    })
}

/// Pure: "1"/"true"/"on"/"yes" (any case, trimmed) = true; anything else
/// (incl. empty/malformed) = false — the sim-knob convention: unparsable
/// falls back to the default.
pub(crate) fn parse_bool_knob(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes"
    )
}

/// Pure: an f64 in [0, 1]. `None` on malformed / out-of-range / NaN — the
/// reader falls back to [`DEFAULT_PREEMPT_P`].
pub(crate) fn parse_probability(raw: &str) -> Option<f64> {
    let p = raw.trim().parse::<f64>().ok()?;
    if p.is_finite() && (0.0..=1.0).contains(&p) {
        Some(p)
    } else {
        None
    }
}

/// Pure: a decimal u64 of seconds. `None` on malformed/empty — the reader
/// falls back to [`DEFAULT_WATCHDOG_S`].
pub(crate) fn parse_seconds(raw: &str) -> Option<u64> {
    raw.trim().parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_knob_corpus() {
        for good in ["1", "true", "TRUE", "on", "On", "yes", " 1 "] {
            assert!(parse_bool_knob(good), "{good:?}");
        }
        for bad in ["", "0", "off", "no", "2", "enabled", "tru"] {
            assert!(!parse_bool_knob(bad), "{bad:?}");
        }
    }

    #[test]
    fn probability_corpus() {
        assert_eq!(parse_probability("0"), Some(0.0));
        assert_eq!(parse_probability("1"), Some(1.0));
        assert_eq!(parse_probability("0.25"), Some(0.25));
        assert_eq!(parse_probability(" 0.5 "), Some(0.5));
        assert_eq!(parse_probability(""), None);
        assert_eq!(parse_probability("-0.1"), None);
        assert_eq!(parse_probability("1.5"), None);
        assert_eq!(parse_probability("NaN"), None);
        assert_eq!(parse_probability("inf"), None);
        assert_eq!(parse_probability("p"), None);
    }

    #[test]
    fn seconds_corpus() {
        assert_eq!(parse_seconds("30"), Some(30));
        assert_eq!(parse_seconds(" 5 "), Some(5));
        assert_eq!(parse_seconds("0"), Some(0));
        assert_eq!(parse_seconds(""), None);
        assert_eq!(parse_seconds("-1"), None);
        assert_eq!(parse_seconds("5s"), None);
    }
}
