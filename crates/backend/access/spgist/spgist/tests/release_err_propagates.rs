//! Injection witness for the release_buffer error-propagation fixes: with a
//! seam fake whose ReleaseBuffer fails (C: elog(ERROR, "bad buffer ID")),
//! spgist's unlock/release helpers must return Err, not Ok. Before the fix
//! the Result was dropped as a bare statement and these paths reported
//! success over a failed release.
//!
//! Own test binary: seams are set-once per process, so the Err-returning
//! fake cannot share a binary with tests that need a working release path.

use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

static RELEASE_CALLS: AtomicU32 = AtomicU32::new(0);

fn install_seams() {
    bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
    bufmgr_seams::release_buffer::set(|buf| {
        RELEASE_CALLS.fetch_add(1, Relaxed);
        Err(Box::new(types_error::PgError::new(
            types_error::ERROR,
            format!("injected: bad buffer ID: {buf}"),
        )))
    });
}

#[test]
fn unlock_release_propagates_release_err() {
    install_seams();
    let before = RELEASE_CALLS.load(Relaxed);
    let r = spgist::utils::unlock_release(1);
    // The fake must actually have fired (guards against a vacuous pass if the
    // helper is ever restructured to skip the release), and its Err must
    // surface to the caller.
    assert!(RELEASE_CALLS.load(Relaxed) > before, "release seam never fired");
    assert!(r.is_err(), "unlock_release swallowed the release_buffer error");
    let e = r.unwrap_err();
    assert!(
        e.message().contains("injected"),
        "propagated a different error than the injected one: {}",
        e.message()
    );
}
