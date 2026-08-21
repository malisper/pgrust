//! Real-thread stress on the scan-state publication cell (default CI; the
//! loom model lives in `tests/loom.rs` — pgsync's L3 law makes the loom
//! arm std-backed for `OnceLock`, so THIS test is the practical race
//! coverage): N threads race `get_or_publish` over independently
//! materialized inputs; exactly one publication wins and every thread
//! observes the same `Arc` (or the same published error).

use std::sync::Arc;

use pgrc2_format::rowid::pack_rowid;

use crate::rowid::pack_delta_rowid;
use crate::scanstate::DeltaScanState;
use crate::DeltaError;

const THREADS: usize = 8;
const ROUNDS: usize = 200;

#[test]
fn racing_workers_share_one_published_arc() {
    for round in 0..ROUNDS {
        let state = Arc::new(DeltaScanState::new());
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let state = Arc::clone(&state);
            handles.push(std::thread::spawn(move || {
                // Each racer materialized its own (identical) scan result
                // outside the cell — the module-doc discipline.
                let rowids = vec![pack_rowid(0, 0, 1), pack_rowid(0, 0, 2)];
                let _ = (t, round);
                state.get_or_publish(&rowids).expect("publish succeeds")
            }));
        }
        let arcs: Vec<_> = handles.into_iter().map(|h| h.join().expect("join")).collect();
        for a in &arcs[1..] {
            assert!(Arc::ptr_eq(&arcs[0], a), "every worker must see the ONE index");
        }
        assert_eq!(arcs[0].total_deleted(), 2);
        assert!(arcs[0].is_deleted(pack_rowid(0, 0, 1)));
        // The publication is sticky: a later caller with DIFFERENT input
        // still receives the engagement's one outcome (set-once law).
        let later = state.get_or_publish(&[pack_rowid(0, 0, 99)]).expect("published");
        assert!(Arc::ptr_eq(&arcs[0], &later));
        assert!(!later.is_deleted(pack_rowid(0, 0, 99)));
    }
}

#[test]
fn racing_workers_share_one_published_error() {
    let state = Arc::new(DeltaScanState::new());
    let poisoned = vec![pack_rowid(0, 0, 1), pack_delta_rowid(3, 7)];
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let state = Arc::clone(&state);
        let poisoned = poisoned.clone();
        handles.push(std::thread::spawn(move || state.get_or_publish(&poisoned)));
    }
    for h in handles {
        match h.join().expect("join") {
            Err(DeltaError::DeltaTaggedTombstone { .. }) => {}
            other => panic!("every waiter must receive the ONE published error, got {other:?}"),
        }
    }
    // try_get serves the published outcome without folding.
    match state.try_get() {
        Some(Err(DeltaError::DeltaTaggedTombstone { .. })) => {}
        other => panic!("try_get must see the published error, got {other:?}"),
    }
    // Sticky even for a now-clean input (the engagement's one outcome).
    assert!(state.get_or_publish(&[pack_rowid(0, 0, 5)]).is_err());
}
