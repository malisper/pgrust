//! CLEAN-BUILD / BOOT guard: `init_all()` must install every seam EXACTLY once.
//!
//! ## The breakage class this catches
//!
//! Every owner crate installs its seams in `init_seams()` via `<seam>::set(..)`,
//! which is backed by a process-global `OnceLock` (see `seam-core`). The slot's
//! `set()` panics `"seam installed twice: <module_path>"` the instant a SECOND
//! crate tries to install the same slot:
//!
//! ```ignore
//! pub fn set(implementation: Signature) {
//!     if SLOT.set(implementation).is_err() {
//!         panic!(concat!("seam installed twice: ", module_path!()));
//!     }
//! }
//! ```
//!
//! A double-install therefore does NOT show up at compile time and does NOT
//! show up on a WARM run where `init_all()` was never re-driven from a clean
//! process — it only fires the first time the full `init_all()` sequence runs
//! in a fresh process (boot of `postgres`, the measure, the node flip, a fresh
//! lane). That made it a recurring "clean build breaks on origin/main" trap:
//! the same `expr_hash_eq_operator` slot was installed from BOTH `joininfo.c`
//! and `var.c` derived crates, and ~7 lanes independently rediscovered and
//! re-fixed it because nothing in the gate drove `init_all()` to completion.
//!
//! The static sibling guard
//! (`recurrence_guard::every_seam_installing_crate_is_wired_into_init_all`)
//! checks the OPPOSITE direction (every installer is *reachable* from
//! `init_all`). It is structurally blind to double-installs. This test closes
//! that gap by actually DRIVING the boot-time install sequence in-process and
//! asserting it runs to completion without the double-install (or any other)
//! panic — naming the offending seam in the failure when it does.

/// Drive the real boot-time seam-install sequence exactly as `postgres`'s
/// `main()` does, in-process, and assert it completes.
///
/// `init_all()` is idempotent-by-construction only in the sense that it is
/// meant to be called ONCE per process: the first `<seam>::set()` per slot must
/// succeed and there must be no second installer of any slot. If two crates own
/// the same slot, the second `set()` panics `"seam installed twice: <path>"`,
/// the panic unwinds out of `init_all()`, and this test fails with that exact
/// message — pinning the offending seam for whoever introduced the duplicate.
///
/// (We do NOT call `init_all()` twice here: a second call would re-run every
/// `set()` and trip the same `OnceLock` panic by design. The double-install
/// class is about TWO DISTINCT crates owning one slot within a SINGLE pass,
/// which a single pass already exposes.)
#[test]
fn init_all_installs_every_seam_exactly_once() {
    // A double-install anywhere in the sequence panics
    // "seam installed twice: <module_path>" out of here; the default test
    // harness reports that panic message verbatim, naming the duplicated seam.
    init::init_all();
}
