//! Executable coverage-exception macros (LIT-REVIEW-100PCT §5, the
//! fuzzuproof-crate skill's §d): a defensive arm recorded as a coverage
//! exception must stay auditable, never a comment-only carve.
//!
//! Unarmed (production default), `never_reached!` / `always_true!` are exact
//! pass-throughs — the defensive arm keeps its C behavior. A differential
//! fuzz harness (or test) calls [`arm_exception_audit`] once at startup;
//! from then on every execution audits the claim: an "impossible" arm that
//! fires panics with the site, becoming a crash artifact. The gate is a
//! relaxed atomic load on arms that are cold by definition, so no cfg
//! plumbing is needed and the audit is RELEASE-effective (debug-assert
//! masking law: debug-only bars have masked release defects).

use core::sync::atomic::{AtomicBool, Ordering};

static EXCEPTION_AUDIT: AtomicBool = AtomicBool::new(false);

/// Arm the exception audit for this process (fuzz harnesses, tests).
pub fn arm_exception_audit() {
    EXCEPTION_AUDIT.store(true, Ordering::Relaxed);
}

#[inline(always)]
#[doc(hidden)]
pub fn exception_audit_armed() -> bool {
    EXCEPTION_AUDIT.load(Ordering::Relaxed)
}

/// Wrap a defensive arm's expression that a recorded coverage-exception row
/// claims can never execute (e.g. OOM arms mirroring C's palloc ereport).
/// Pass-through in production; panics under an armed audit.
#[macro_export]
macro_rules! never_reached {
    ($e:expr) => {{
        if $crate::exceptions::exception_audit_armed() {
            panic!("never_reached arm fired: {}", stringify!($e));
        }
        $e
    }};
}

/// Assert-shaped sibling: a condition a recorded exception row claims is
/// always true. Evaluates and returns the condition in production; panics
/// under an armed audit if it is false.
#[macro_export]
macro_rules! always_true {
    ($cond:expr) => {{
        let cond = $cond;
        if !cond && $crate::exceptions::exception_audit_armed() {
            panic!("always_true violated: {}", stringify!($cond));
        }
        cond
    }};
}
