//! The delta WAL crash battery (slice leg: "delta WAL crash smoke — acked
//! delta rows survive kill -9") and the #253 tooth ("kill -9 between ack
//! and sync must lose NOTHING — born-RED with a seeded sync-skip").
//!
//! Model: every mutation appends a WAL record; kill -9 at op boundary k
//! replays exactly the flushed prefix ([`SimHeap::revive_at`]); the
//! checker ([`SimHeap::check_acked_survive`]) demands every ACKED
//! transaction's effects survive and nothing resurrects. Delta WAL is
//! inherited from heap (crate law) — at product grain the sync guarantee
//! is the heap commit path's (`wrote_xlog = true` ⇒ XLogFlush before ack
//! under `synchronous_commit = on`); the model pins the same choreography
//! and the two teeth prove the checker can detect its absence:
//!
//! - tooth 1 (the #253 shape): a SYNC commit whose flush was seeded away
//!   (`skip_commit_flush`) acks, crashes, and the checker reports LOST;
//! - tooth 2 (the async arm): `commit(sync = false)` is the C-parity
//!   opt-out (`synchronous_commit = off` may lose acked commits on heap
//!   too); the checker detects that loss shape identically — the product
//!   trickle path only takes it when the user opted out.

use pgrc2_format::rowid::pack_rowid;

use crate::feed::DeltaCell;
use crate::lifecycle::DeltaLifecycle;
use crate::ops::{delete_sealed, trickle_insert, update_sealed};
use crate::testkit::{SimHeap, SimTableWriter};

const TABLE: u64 = 21;

fn row(k: u64) -> Vec<DeltaCell> {
    vec![DeltaCell::Word(k)]
}

/// A representative trickle schedule: sync-acked inserts, tombstones, an
/// update, an abort, an async flush, and a transaction left open (its
/// writes must never surface after any crash).
fn run_schedule(heap: &mut SimHeap) {
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap, binding };
        trickle_insert(&mut w, &row(1)).expect("insert");
        trickle_insert(&mut w, &row(2)).expect("insert");
    }
    heap.commit(true).expect("commit t1");

    heap.begin();
    {
        let mut w = SimTableWriter { heap, binding };
        delete_sealed(&mut w, pack_rowid(0, 0, 7)).expect("tombstone");
        update_sealed(&mut w, pack_rowid(0, 1, 8), &row(3)).expect("update");
    }
    heap.commit(true).expect("commit t2");

    // An aborted writer: nothing of it may ever surface.
    heap.begin();
    {
        let mut w = SimTableWriter { heap, binding };
        trickle_insert(&mut w, &row(99)).expect("insert");
    }
    heap.abort().expect("abort t3");

    // A stray walwriter-style flush between transactions.
    heap.flush_wal();

    heap.begin();
    {
        let mut w = SimTableWriter { heap, binding };
        trickle_insert(&mut w, &row(4)).expect("insert");
    }
    heap.commit(true).expect("commit t4");

    // A transaction still OPEN at crash time (never acked, never
    // committed — must resolve invisible after every revive).
    heap.begin();
    {
        let mut w = SimTableWriter { heap, binding };
        trickle_insert(&mut w, &row(1000)).expect("insert");
        delete_sealed(&mut w, pack_rowid(0, 2, 9)).expect("tombstone");
    }
    // (no commit)
}

#[test]
fn kill9_ladder_every_op_boundary_green() {
    let mut heap = SimHeap::new();
    run_schedule(&mut heap);
    let points = heap.op_points();
    assert!(points > 8, "the ladder needs real coverage, got {points} points");
    for k in 0..points {
        if let Err(e) = heap.check_acked_survive(k) {
            panic!("crash point {k}/{points}: {e}");
        }
    }
}

#[test]
fn revive_semantics_after_full_schedule() {
    // Deeper look at the final crash point: acked rows visible, aborted/
    // open writers invisible, tombstones of acked deleters indexed.
    let mut heap = SimHeap::new();
    run_schedule(&mut heap);
    let revived = heap.revive_at(heap.op_points() - 1);
    let binding = {
        let mut r = revived;
        let b = r.lookup(TABLE).expect("lookup").expect("pair survives replay");
        let snap = r.snapshot();
        let rows = r.visible_delta_rows(&snap, b.delta_rel);
        let keys: Vec<u64> = rows
            .iter()
            .map(|(_, cells)| match cells.as_slice() {
                [DeltaCell::Word(w)] => *w,
                other => panic!("row shape {other:?}"),
            })
            .collect();
        assert_eq!(keys, vec![1, 2, 3, 4], "exactly the acked rows, in tid order");
        let index = r.build_deletion_index(&snap, b.tombstone_rel).expect("build");
        assert!(index.is_deleted(pack_rowid(0, 0, 7)));
        assert!(index.is_deleted(pack_rowid(0, 1, 8)));
        assert!(!index.is_deleted(pack_rowid(0, 2, 9)), "open txn's tombstone leaked");
        b
    };
    let _ = binding;
}

#[test]
fn tooth_253_seeded_sync_skip_is_detected() {
    // The #253 defect shape: the commit record stays unflushed while the
    // client is acked. kill -9 right after the ack must LOSE the txn, and
    // the checker must SAY SO (the born-RED proof that the battery can
    // detect exactly this loss).
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        trickle_insert(&mut w, &row(1)).expect("insert");
    }
    heap.commit(true).expect("commit acked");
    // Sanity: the unseeded run survives its final crash point.
    let clean_final = heap.op_points() - 1;
    heap.check_acked_survive(clean_final).expect("clean run survives");

    let mut seeded = SimHeap::new();
    seeded.skip_commit_flush = true; // the seeded sync-skip
    seeded.begin();
    let binding = seeded.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap: &mut seeded, binding };
        trickle_insert(&mut w, &row(1)).expect("insert");
    }
    seeded.commit(true).expect("commit acked WITHOUT flush");
    let k = seeded.op_points() - 1;
    let err = seeded
        .check_acked_survive(k)
        .expect_err("the tooth must bite: an acked, unflushed commit is LOST at kill -9");
    assert!(
        err.contains("LOST"),
        "the checker must name the loss (got: {err})"
    );
}

#[test]
fn tooth_async_commit_loss_is_detected() {
    // The second tooth: the async-commit arm (C-parity opt-out) has a
    // real loss window; the same checker detects it. The product trickle
    // path only enters this window when `synchronous_commit = off`.
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        trickle_insert(&mut w, &row(1)).expect("insert");
    }
    heap.commit(false).expect("async commit acked");
    let k = heap.op_points() - 1;
    let err = heap
        .check_acked_survive(k)
        .expect_err("async-acked commit inside the loss window must be detected");
    assert!(err.contains("LOST"), "got: {err}");

    // And once the walwriter catches up (flush), the same txn survives.
    heap.flush_wal();
    let k2 = heap.op_points() - 1;
    heap.check_acked_survive(k2)
        .expect("flushed async commit survives kill -9");
}

#[test]
fn lifecycle_records_replay() {
    // Pair creation and TRUNCATE ride the WAL model: a revive
    // reconstructs the binding and the reset state.
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        trickle_insert(&mut w, &row(5)).expect("insert");
    }
    heap.commit(true).expect("commit");
    heap.reset(TABLE).expect("truncate");
    heap.flush_wal();

    let mut revived = heap.revive_at(heap.op_points() - 1);
    let b = revived.lookup(TABLE).expect("lookup").expect("binding replayed");
    assert_eq!(b, binding);
    let snap = revived.snapshot();
    assert!(
        revived.visible_delta_rows(&snap, b.delta_rel).is_empty(),
        "TRUNCATE replays"
    );
}
