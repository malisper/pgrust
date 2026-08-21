//! Loom model for the delta scan-merge publication cell (§6 gate 8 names
//! "delta scan-merge publication"; chunk M5-B's only shared state).
//!
//! Scope honesty (pgsync L3 law): loom lacks `Once`/`OnceLock`, so
//! pgsync's loom world backs `OnceLock` with std — the model therefore
//! explores the SCHEDULES AROUND the cell (two racing publishers, a
//! publisher racing a reader) rather than the lock's internal
//! interleavings; the primitive itself is the sanctioned, std-backed
//! pattern, and its init closure is a PURE FOLD (the once-ledger proof
//! row in `lint-determinism.allow`). The default-CI real-thread stress
//! (`src/tests/scanstate_stress.rs`) carries the practical race
//! coverage.
//!
//! Build/run:
//!   RUSTFLAGS="--cfg loom" cargo test -p pgrc2_delta --test loom --release
//!
//! Models:
//!   1. `racing_publishers_agree_on_one_index` — two workers race
//!      `get_or_publish` in every explored schedule: one publication,
//!      one Arc, both observers agree on content.
//!   2. `reader_never_sees_torn_state` — a `try_get` racing the fold
//!      sees None or the COMPLETE published index, never a partial one.

#![cfg(loom)]

use std::sync::Arc;

use loom::thread;

use pgrc2_delta::scanstate::DeltaScanState;
use pgrc2_format::rowid::pack_rowid;

#[test]
fn racing_publishers_agree_on_one_index() {
    loom::model(|| {
        let state = Arc::new(DeltaScanState::new());

        let s1 = Arc::clone(&state);
        let t1 = thread::spawn(move || {
            let rowids = [pack_rowid(0, 0, 1), pack_rowid(0, 0, 2), pack_rowid(0, 0, 3)];
            s1.get_or_publish(&rowids).expect("publish")
        });

        let s2 = Arc::clone(&state);
        let t2 = thread::spawn(move || {
            let rowids = [pack_rowid(0, 0, 1), pack_rowid(0, 0, 2), pack_rowid(0, 0, 3)];
            s2.get_or_publish(&rowids).expect("publish")
        });

        let a1 = t1.join().expect("join");
        let a2 = t2.join().expect("join");
        assert!(Arc::ptr_eq(&a1, &a2), "one published Arc");
        assert_eq!(a1.total_deleted(), 3);
    });
}

#[test]
fn reader_never_sees_torn_state() {
    loom::model(|| {
        let state = Arc::new(DeltaScanState::new());

        let sw = Arc::clone(&state);
        let writer = thread::spawn(move || {
            let rowids = [pack_rowid(0, 0, 7), pack_rowid(0, 0, 8)];
            sw.get_or_publish(&rowids).expect("publish")
        });

        let sr = Arc::clone(&state);
        let reader = thread::spawn(move || sr.try_get());

        let published = writer.join().expect("join");
        match reader.join().expect("join") {
            None => {}
            Some(Ok(seen)) => {
                // Complete or absent — never partial.
                assert!(Arc::ptr_eq(&seen, &published));
                assert_eq!(seen.total_deleted(), 2);
                assert!(seen.is_deleted(pack_rowid(0, 0, 7)));
                assert!(seen.is_deleted(pack_rowid(0, 0, 8)));
            }
            Some(Err(e)) => panic!("unexpected published error: {e}"),
        }
    });
}
