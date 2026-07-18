//! Outcome-class vocabulary — WS-ORACLE owned (contract §1.3).
//!
//! Class names and severities are PINNED to `scripts/sqlsmith/triage.py` as of
//! `ef070d066` (the commit that adds the `harness-fetch` P2 class; the t26
//! soak's fake rust-crash P1 is the cautionary case). Rust never invents a
//! divergence class that triage.py has under a different name — the G-O2
//! parity gate enforces this on real corpora. Harness-only additions are
//! namespaced `property-*` so grep vocabularies never collide.

use std::fmt;

/// Severity strings exactly as triage.py emits them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    P1,
    P2,
    P3,
    Coverage,
    Fine,
}

impl Severity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::P1 => "P1",
            Severity::P2 => "P2",
            Severity::P3 => "P3",
            Severity::Coverage => "coverage",
            Severity::Fine => "fine",
        }
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The pinned outcome-class vocabulary. Order groups: triage.py classes
/// first (verbatim names), then the `property-*` harness namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Class {
    // -- triage.py @ ef070d066, verbatim --
    Ok,
    ErrMatch,
    BigResultSkipped,
    Empty,
    RustCrash,
    CCrash,
    WrongResults,
    WrongResultsVolatile,
    WrongResultsNondet,
    WrongResultsUnderdetermined,
    RustErrCOk,
    RustErrCOkCoverage,
    RustErrCOkNondet,
    CErrRustOk,
    CErrRustOkNondet,
    ErrStateMismatch,
    ErrTextDrift,
    ErrTimeoutOneSide,
    HarnessFetch,
    // -- harness-only namespace (contract §1.3): property-* --
    PropertyViolation,
    PropertySkipped,
}

impl Class {
    pub fn as_str(&self) -> &'static str {
        match self {
            Class::Ok => "ok",
            Class::ErrMatch => "err-match",
            Class::BigResultSkipped => "big-result-skipped",
            Class::Empty => "empty",
            Class::RustCrash => "rust-crash",
            Class::CCrash => "c-crash",
            Class::WrongResults => "wrong-results",
            Class::WrongResultsVolatile => "wrong-results-volatile",
            Class::WrongResultsNondet => "wrong-results-nondet",
            Class::WrongResultsUnderdetermined => "wrong-results-underdetermined",
            Class::RustErrCOk => "rust-err-c-ok",
            Class::RustErrCOkCoverage => "rust-err-c-ok-coverage",
            Class::RustErrCOkNondet => "rust-err-c-ok-nondet",
            Class::CErrRustOk => "c-err-rust-ok",
            Class::CErrRustOkNondet => "c-err-rust-ok-nondet",
            Class::ErrStateMismatch => "err-state-mismatch",
            Class::ErrTextDrift => "err-text-drift",
            Class::ErrTimeoutOneSide => "err-timeout-one-side",
            Class::HarnessFetch => "harness-fetch",
            Class::PropertyViolation => "property-violation",
            Class::PropertySkipped => "property-skipped",
        }
    }

    /// Severity pinned per triage.py docstring + classify() and contract §1.3.
    pub fn severity(&self) -> Severity {
        match self {
            Class::RustCrash | Class::CCrash | Class::WrongResults | Class::PropertyViolation => {
                Severity::P1
            }
            Class::RustErrCOk
            | Class::CErrRustOk
            | Class::ErrStateMismatch
            | Class::HarnessFetch => Severity::P2,
            Class::ErrTextDrift | Class::ErrTimeoutOneSide => Severity::P3,
            Class::RustErrCOkCoverage => Severity::Coverage,
            Class::Ok
            | Class::ErrMatch
            | Class::BigResultSkipped
            | Class::Empty
            | Class::WrongResultsVolatile
            | Class::WrongResultsNondet
            | Class::WrongResultsUnderdetermined
            | Class::RustErrCOkNondet
            | Class::CErrRustOkNondet
            | Class::PropertySkipped => Severity::Fine,
        }
    }

    pub fn from_str(s: &str) -> Option<Class> {
        ALL_CLASSES.iter().copied().find(|c| c.as_str() == s)
    }
}

impl fmt::Display for Class {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every class, for validation (warts.toml class fields must name one).
pub const ALL_CLASSES: &[Class] = &[
    Class::Ok,
    Class::ErrMatch,
    Class::BigResultSkipped,
    Class::Empty,
    Class::RustCrash,
    Class::CCrash,
    Class::WrongResults,
    Class::WrongResultsVolatile,
    Class::WrongResultsNondet,
    Class::WrongResultsUnderdetermined,
    Class::RustErrCOk,
    Class::RustErrCOkCoverage,
    Class::RustErrCOkNondet,
    Class::CErrRustOk,
    Class::CErrRustOkNondet,
    Class::ErrStateMismatch,
    Class::ErrTextDrift,
    Class::ErrTimeoutOneSide,
    Class::HarnessFetch,
    Class::PropertyViolation,
    Class::PropertySkipped,
];

/// Counter label for the A5 hook-skip line: `SIMHARNESS|skipped-no-hook|n`.
/// This is a counter label, not an outcome class.
pub const SKIPPED_NO_HOOK: &str = "skipped-no-hook";

/// Counter label for declared-but-uninstrumented engagement floors (§0 A4):
/// `SIMHARNESS|floor-skipped-no-instrument|1`.
pub const FLOOR_SKIPPED_NO_INSTRUMENT: &str = "floor-skipped-no-instrument";

/// Counter label for a wart-allowlist hit: `SIMHARNESS|wart:<id>|n`.
/// Suppressed, never invisible (contract §3.1.5).
pub fn wart_counter(id: &str) -> String {
    format!("wart:{id}")
}

/// Runner-mechanics classes (harness self-checks), folded from WS-RUNNER at
/// integration so vocab.rs stays the single severity source. These are
/// namespaced away from the triage vocabulary; P1 entries fail the run:
///   - `dispatch-refusal`         mutation-split law would be violated
///   - `fault-reserved-refused`   reserved fault tag reached execution (H4)
///   - `fault-restart-noop`       restart_cmd exited 0 but the DUT survived
///   - `fault-armed`              a FaultDriver mapped+armed a reserved tag
///                                (H2 seam; only a non-NoOp driver mints it)
pub fn harness_class_severity(class: &str) -> Option<Severity> {
    if class.starts_with("wart:") {
        return Some(Severity::Fine);
    }
    Some(match class {
        "dispatch-refusal" | "fault-reserved-refused" | "fault-restart-noop" => Severity::P1,
        "err-uncompared"
        | "fault-skipped-no-restart-cmd"
        | "fault-restart-cmd-failed"
        | "fault-armed"
        | "floor-skipped-no-instrument"
        | "skipped-no-hook" => Severity::Fine,
        _ => return None,
    })
}

/// Severity for ANY census class string (pinned Class first, then the
/// harness-mechanics table, then Fine). `Coverage` maps to Fine for verdict
/// purposes (the coverage downgrade never fails a run).
pub fn severity_of(class: &str) -> Severity {
    if let Some(c) = Class::from_str(class) {
        return match c.severity() {
            Severity::Coverage => Severity::Fine,
            s => s,
        };
    }
    harness_class_severity(class).unwrap_or(Severity::Fine)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_str_roundtrip() {
        for c in ALL_CLASSES {
            assert_eq!(Class::from_str(c.as_str()), Some(*c));
        }
    }

    #[test]
    fn pinned_severities() {
        // Spot-pin the contract §1.3 lines.
        assert_eq!(Class::RustCrash.severity(), Severity::P1);
        assert_eq!(Class::WrongResults.severity(), Severity::P1);
        assert_eq!(Class::HarnessFetch.severity(), Severity::P2);
        assert_eq!(Class::RustErrCOkCoverage.severity(), Severity::Coverage);
        assert_eq!(Class::ErrTextDrift.severity(), Severity::P3);
        assert_eq!(Class::ErrTimeoutOneSide.severity(), Severity::P3);
        assert_eq!(Class::PropertyViolation.severity(), Severity::P1);
        assert_eq!(Class::PropertySkipped.severity(), Severity::Fine);
    }
}
