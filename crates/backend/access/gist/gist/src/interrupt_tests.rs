//! Unit tests for check_for_interrupts (lib.rs): a pending interrupt routes
//! through the ported ProcessInterrupts seam (the spgist/gin/hash pattern)
//! instead of the former unported-panic stub. The same helper sits on the
//! scan (gistgettuple/gistgetbitmap), vacuum, and buffered-build paths.
use ::types_error::{PgError, ERRCODE_QUERY_CANCELED};

#[test]
fn pending_interrupt_routes_through_process_interrupts_seam() {
    // No pending interrupt: Ok without consulting the seam (which is not
    // installed yet at this point — a stub call would panic).
    init_small::globals::SetInterruptPending(false);
    assert!(crate::check_for_interrupts().is_ok());

    // ProcessInterrupts mock: consumes the flag and raises the cancel error,
    // as C's query-cancel arm does.
    ::postgres_seams::check_for_interrupts::set(|| {
        init_small::globals::SetInterruptPending(false);
        Err(Box::new(
            PgError::error("canceling statement due to user request")
                .with_sqlstate(ERRCODE_QUERY_CANCELED),
        ))
    });

    init_small::globals::SetInterruptPending(true);
    let err = crate::check_for_interrupts().unwrap_err();
    assert_eq!(err.message(), "canceling statement due to user request");
    assert_eq!(err.sqlstate(), ERRCODE_QUERY_CANCELED);

    // The interrupt was consumed on the seam side; the next check is clean.
    assert!(!init_small::globals::InterruptPending());
    assert!(crate::check_for_interrupts().is_ok());
}
