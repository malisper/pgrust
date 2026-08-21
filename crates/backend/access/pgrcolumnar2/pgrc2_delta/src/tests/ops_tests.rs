//! Trickle-op composition laws ([`crate::ops`]) and the lifecycle
//! contract ([`crate::lifecycle`]) over the sim heap: UPDATE-of-sealed =
//! tombstone THEN delta-insert (pinned order, one transaction, shared
//! abort); delta rows die by heap delete (never tombstone — the refusal
//! is pinned in golden.rs); TRUNCATE resets; DROP removes; abort undoes.

use pgrc2_format::rowid::pack_rowid;

use crate::feed::DeltaCell;
use crate::lifecycle::DeltaLifecycle;
use crate::ops::{delete_delta, delete_sealed, trickle_insert, update_delta, update_sealed};
use crate::testkit::{SimHeap, SimTableWriter};

const TABLE: u64 = 11;

fn row(k: u64) -> Vec<DeltaCell> {
    vec![DeltaCell::Word(k), DeltaCell::Bytes(vec![b'v'; (k % 5) as usize + 1])]
}

#[test]
fn trickle_insert_lands_and_abort_undoes() {
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        trickle_insert(&mut w, &row(1)).expect("insert");
        trickle_insert(&mut w, &row(2)).expect("insert");
    }
    heap.commit(true).expect("commit");

    heap.begin();
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        trickle_insert(&mut w, &row(3)).expect("insert");
    }
    heap.abort().expect("abort");

    let snap = heap.snapshot();
    let rows = heap.visible_delta_rows(&snap, binding.delta_rel);
    assert_eq!(rows.len(), 2, "committed inserts visible, aborted one not");
    assert_eq!(rows[0].1, row(1));
    assert_eq!(rows[1].1, row(2));
}

#[test]
fn update_sealed_is_tombstone_then_insert_one_txn() {
    let mut heap = SimHeap::new();
    let target = pack_rowid(0, 4, 40);
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        update_sealed(&mut w, target, &row(9)).expect("update");
    }
    // Before commit: an outside observer sees NEITHER effect.
    let observer = heap.snapshot();
    assert!(
        !heap
            .build_deletion_index(&observer, binding.tombstone_rel)
            .expect("build")
            .is_deleted(target),
        "uncommitted tombstone leaked"
    );
    assert!(heap.visible_delta_rows(&observer, binding.delta_rel).is_empty());
    heap.commit(true).expect("commit");

    // After commit: BOTH effects, atomically.
    let snap = heap.snapshot();
    assert!(heap
        .build_deletion_index(&snap, binding.tombstone_rel)
        .expect("build")
        .is_deleted(target));
    let rows = heap.visible_delta_rows(&snap, binding.delta_rel);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, row(9));

    // And an ABORTED update leaves neither.
    let target2 = pack_rowid(0, 4, 41);
    heap.begin();
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        update_sealed(&mut w, target2, &row(10)).expect("update");
    }
    heap.abort().expect("abort");
    let snap = heap.snapshot();
    assert!(!heap
        .build_deletion_index(&snap, binding.tombstone_rel)
        .expect("build")
        .is_deleted(target2));
    assert_eq!(heap.visible_delta_rows(&snap, binding.delta_rel).len(), 1);
}

#[test]
fn delta_rows_die_by_heap_delete() {
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    let tid = {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        trickle_insert(&mut w, &row(1)).expect("insert")
    };
    heap.commit(true).expect("commit");

    heap.begin();
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_delta(&mut w, tid).expect("delete");
    }
    heap.commit(true).expect("commit");

    let snap = heap.snapshot();
    assert!(heap.visible_delta_rows(&snap, binding.delta_rel).is_empty());
    // No tombstone was written for the delta row (the tombstone relation
    // is untouched by delta-row deletion).
    let index = heap.build_deletion_index(&snap, binding.tombstone_rel).expect("build");
    assert!(index.is_empty());
}

#[test]
fn update_delta_replaces_version() {
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    let tid = {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        trickle_insert(&mut w, &row(1)).expect("insert")
    };
    heap.commit(true).expect("commit");

    heap.begin();
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        update_delta(&mut w, tid, &row(2)).expect("update");
    }
    heap.commit(true).expect("commit");

    let snap = heap.snapshot();
    let rows = heap.visible_delta_rows(&snap, binding.delta_rel);
    assert_eq!(rows.len(), 1, "exactly the new version");
    assert_eq!(rows[0].1, row(2));
    assert_ne!(rows[0].0, tid, "a new version lives at a new tid");
}

#[test]
fn delete_sealed_writes_exactly_one_tombstone() {
    let mut heap = SimHeap::new();
    let target = pack_rowid(6, 6, 6);
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_sealed(&mut w, target).expect("delete");
    }
    heap.commit(true).expect("commit");
    let snap = heap.snapshot();
    let index = heap.build_deletion_index(&snap, binding.tombstone_rel).expect("build");
    assert_eq!(index.total_deleted(), 1);
    assert!(index.is_deleted(target));
    assert!(heap.visible_delta_rows(&snap, binding.delta_rel).is_empty());
}

#[test]
fn lifecycle_laws() {
    let mut heap = SimHeap::new();
    // Absent pair = empty delta store.
    assert_eq!(heap.lookup(TABLE).expect("lookup"), None);

    // Idempotent lookup_or_create.
    let b1 = heap.lookup_or_create(TABLE).expect("create");
    let b2 = heap.lookup_or_create(TABLE).expect("lookup");
    assert_eq!(b1, b2);
    assert_eq!(heap.lookup(TABLE).expect("lookup"), Some(b1));
    // Distinct tables get distinct pairs.
    let other = heap.lookup_or_create(TABLE + 1).expect("create");
    assert_ne!(other.delta_rel, b1.delta_rel);
    assert_ne!(other.tombstone_rel, b1.tombstone_rel);

    // TRUNCATE resets both relations.
    heap.begin();
    {
        let mut w = SimTableWriter { heap: &mut heap, binding: b1 };
        trickle_insert(&mut w, &row(1)).expect("insert");
        delete_sealed(&mut w, pack_rowid(0, 0, 1)).expect("delete");
    }
    heap.commit(true).expect("commit");
    heap.reset(TABLE).expect("reset");
    let snap = heap.snapshot();
    assert!(heap.visible_delta_rows(&snap, b1.delta_rel).is_empty());
    assert!(heap
        .build_deletion_index(&snap, b1.tombstone_rel)
        .expect("build")
        .is_empty());
    // The binding survives a reset (TRUNCATE keeps the pair).
    assert_eq!(heap.lookup(TABLE).expect("lookup"), Some(b1));

    // DROP removes the pair.
    heap.drop_pair(TABLE).expect("drop");
    assert_eq!(heap.lookup(TABLE).expect("lookup"), None);
    // Dropping again is a no-op.
    heap.drop_pair(TABLE).expect("drop again");
}
