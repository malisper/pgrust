//! Dispatcher parity tests for the `DISCARD` command.
//!
//! discard.c is a pure dispatcher. The single-target arms `DISCARD PLANS` and
//! `DISCARD TEMP` go to already-ported owners (plan cache / temp namespace)
//! whose real bodies need a live backend, so they are not unit-tested here;
//! the seam-backed `DISCARD SEQUENCES` arm and the `DISCARD ALL` short-circuit
//! at its first (seam-backed) step are.
//!
//! Seam slots are process-global `static mut` and the suite runs
//! single-threaded (`--test-threads=1`), so the shared recorder is race-free.

use super::*;
use std::cell::RefCell;

thread_local! {
    static CALLS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

fn record(tag: &'static str) {
    CALLS.with(|c| c.borrow_mut().push(tag));
}

fn take_calls() -> Vec<&'static str> {
    CALLS.with(|c| c.borrow_mut().drain(..).collect())
}

fn stmt(target: DiscardMode) -> DiscardStmt {
    DiscardStmt { target }
}

/// `DISCARD SEQUENCES` -> exactly one `ResetSequenceCaches` (seam-backed).
#[test]
fn discard_sequences_resets_only_sequence_caches() {
    reset_sequence_caches::set(|| {
        record("ResetSequenceCaches");
        Ok(())
    });
    take_calls();

    DiscardCommand(&stmt(DiscardMode::DISCARD_SEQUENCES), true).unwrap();

    assert_eq!(take_calls(), vec!["ResetSequenceCaches"]);
}

/// `DISCARD ALL` aborts the chain at the first failing step: if the
/// in-transaction-block guard errors, nothing else runs (the `?` short-circuit
/// mirrors C's `PreventInTransactionBlock` ereport-ing out).
#[test]
fn discard_all_short_circuits_on_transaction_block() {
    prevent_in_transaction_block::set(|_is_top_level, _stmt_type| {
        record("PreventInTransactionBlock");
        Err(types_error::PgError::error(
            "DISCARD ALL cannot run inside a transaction block",
        ))
    });
    take_calls();

    let err = DiscardCommand(&stmt(DiscardMode::DISCARD_ALL), true).unwrap_err();

    // Only the guard ran; every later reset was skipped.
    assert_eq!(take_calls(), vec!["PreventInTransactionBlock"]);
    assert!(err.message().contains("transaction block"));
}
