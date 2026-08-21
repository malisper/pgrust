//! The bitmap ≡ oracle differential (slice leg: "visible-tombstone bitmap
//! ≡ per-row heap MVCC oracle under concurrent txn schedules (subxact +
//! combo-cid cases)"), plus the bitmap structure laws (promotion ladder,
//! idempotent add, filter_window subtractive-only) and the two born-RED
//! teeth (seeded visibility skew; seeded tombstone drop).
//!
//! Differential shape: the PRODUCTION path (visible-tombstone scan →
//! `DeletionIndexBuilder` → `DeletionIndex::is_deleted`) against the
//! NAIVE per-row oracle (`SimHeap::oracle_is_deleted` — full tombstone
//! walk per probe, no index). The visibility LAW is shared (one
//! `tuple_visible`, the C-shaped rule; at product grain it is the real
//! heap's) — what the differential proves is the index machinery:
//! partitioning, granule/row split, promotion, dedup, lookup.

use pgrc2_format::geom::GRANULE_ROWS;
use pgrc2_format::rowid::pack_rowid;

use crate::bitmap::{DeletionIndexBuilder, GranuleDeletions, LIST_PROMOTE_AT};
use crate::lifecycle::DeltaLifecycle;
use crate::ops::delete_sealed;
use crate::testkit::{seed_tombstones, SimHeap, SimSnapshot, SimTableWriter, SkewTarget};

const TABLE: u64 = 7;

/// Probe set: every tombstoned rowid plus never-tombstoned neighbors
/// (same granule, adjacent granules, adjacent parts).
fn probes(tombstoned: &[u64]) -> Vec<u64> {
    let mut out = Vec::new();
    for &r in tombstoned {
        let (p, g, row) = (
            pgrc2_format::rowid::rowid_part(r),
            pgrc2_format::rowid::rowid_granule(r),
            pgrc2_format::rowid::rowid_row(r),
        );
        out.push(r);
        out.push(pack_rowid(p, g, (row + 1) % GRANULE_ROWS));
        out.push(pack_rowid(p, g + 1, row));
        out.push(pack_rowid(p + 1, g, row));
    }
    out.push(pack_rowid(0, 0, 0));
    out.push(pack_rowid(9999, 0, 0));
    out
}

fn assert_differential(heap: &SimHeap, snap: &SimSnapshot, tomb_rel: u64, probe_set: &[u64]) {
    let index = heap.build_deletion_index(snap, tomb_rel).expect("build");
    for &r in probe_set {
        assert_eq!(
            index.is_deleted(r),
            heap.oracle_is_deleted(snap, tomb_rel, r),
            "bitmap vs oracle disagree on rowid {r:#x}"
        );
    }
}

#[test]
fn committed_deletes_visible_everywhere() {
    let mut heap = SimHeap::new();
    let rowids: Vec<u64> = vec![
        pack_rowid(0, 0, 0),
        pack_rowid(0, 0, 8191),      // granule-edge row
        pack_rowid(0, 7, 100),       // band-edge granule
        pack_rowid(0, 8, 100),       // next band
        pack_rowid(3, 0, 1),         // another part
    ];
    let binding = seed_tombstones(&mut heap, TABLE, &rowids).expect("seed");
    let snap = heap.snapshot();
    let index = heap.build_deletion_index(&snap, binding.tombstone_rel).expect("build");
    assert_eq!(index.total_deleted(), rowids.len() as u64);
    assert_eq!(index.deleted_in_part(0), 4);
    assert_eq!(index.deleted_in_part(3), 1);
    assert_eq!(index.deleted_in_part(2), 0);
    assert_differential(&heap, &snap, binding.tombstone_rel, &probes(&rowids));
}

#[test]
fn in_progress_and_aborted_deletes_invisible() {
    let mut heap = SimHeap::new();
    let committed = pack_rowid(0, 0, 10);
    let binding = seed_tombstones(&mut heap, TABLE, &[committed]).expect("seed");

    // An ABORTED deleter: its tombstone must never surface.
    heap.begin();
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_sealed(&mut w, pack_rowid(0, 0, 11)).expect("write");
    }
    heap.abort().expect("abort");

    // An IN-PROGRESS deleter: invisible to a fresh observer.
    let t_open = heap.begin();
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_sealed(&mut w, pack_rowid(0, 0, 12)).expect("write");
    }
    let observer = heap.snapshot();
    let index = heap
        .build_deletion_index(&observer, binding.tombstone_rel)
        .expect("build");
    assert!(index.is_deleted(committed));
    assert!(!index.is_deleted(pack_rowid(0, 0, 11)), "aborted delete leaked");
    assert!(!index.is_deleted(pack_rowid(0, 0, 12)), "in-progress delete leaked");
    assert_differential(
        &heap,
        &observer,
        binding.tombstone_rel,
        &probes(&[committed, pack_rowid(0, 0, 11), pack_rowid(0, 0, 12)]),
    );

    // After that txn commits, a NEW snapshot sees it; the OLD snapshot
    // (taken while it ran — xip) still must not.
    heap.set_current(t_open);
    heap.commit(true).expect("commit");
    let index_old = heap
        .build_deletion_index(&observer, binding.tombstone_rel)
        .expect("build");
    assert!(!index_old.is_deleted(pack_rowid(0, 0, 12)), "xip snapshot leaked");
    let fresh = heap.snapshot();
    let index_new = heap
        .build_deletion_index(&fresh, binding.tombstone_rel)
        .expect("build");
    assert!(index_new.is_deleted(pack_rowid(0, 0, 12)));
    assert_differential(&heap, &observer, binding.tombstone_rel, &probes(&[pack_rowid(0, 0, 12)]));
    assert_differential(&heap, &fresh, binding.tombstone_rel, &probes(&[pack_rowid(0, 0, 12)]));
}

#[test]
fn subxact_schedules() {
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");

    // Committed subxact under a committed top: visible.
    let s1 = heap.begin_sub().expect("sub");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_sealed(&mut w, pack_rowid(0, 0, 1)).expect("write");
    }
    heap.commit_sub(s1).expect("subcommit");

    // Aborted subxact under the SAME committed top: dead regardless.
    let s2 = heap.begin_sub().expect("sub");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_sealed(&mut w, pack_rowid(0, 0, 2)).expect("write");
    }
    heap.abort_sub(s2).expect("subabort");

    // Nested: sub-sub committed, its parent sub committed.
    let s3 = heap.begin_sub().expect("sub");
    let s4 = heap.begin_sub().expect("subsub");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_sealed(&mut w, pack_rowid(0, 0, 3)).expect("write");
    }
    heap.commit_sub(s4).expect("subcommit");
    heap.commit_sub(s3).expect("subcommit");

    heap.commit(true).expect("commit");

    let snap = heap.snapshot();
    let index = heap.build_deletion_index(&snap, binding.tombstone_rel).expect("build");
    assert!(index.is_deleted(pack_rowid(0, 0, 1)), "committed subxact lost");
    assert!(!index.is_deleted(pack_rowid(0, 0, 2)), "aborted subxact leaked");
    assert!(index.is_deleted(pack_rowid(0, 0, 3)), "nested subxact lost");
    assert_differential(
        &heap,
        &snap,
        binding.tombstone_rel,
        &probes(&[pack_rowid(0, 0, 1), pack_rowid(0, 0, 2), pack_rowid(0, 0, 3)]),
    );
}

#[test]
fn combo_cid_self_visibility() {
    // The cid discipline a same-txn scan must honor (PG's combo-cid
    // semantics: cmin/cmax against the snapshot's curcid): a tombstone
    // written at command N is invisible to the txn's OWN snapshot taken
    // at command N, visible from command N+1.
    let mut heap = SimHeap::new();
    heap.begin();
    let binding = heap.lookup_or_create(TABLE).expect("pair");
    let target = pack_rowid(0, 0, 5);

    let before = heap.snapshot_of_current().expect("snap");
    {
        let mut w = SimTableWriter { heap: &mut heap, binding };
        delete_sealed(&mut w, target).expect("write");
    }
    let same_command = heap.snapshot_of_current().expect("snap");
    heap.cci().expect("cci");
    let after = heap.snapshot_of_current().expect("snap");

    for (snap, want, what) in [
        (&before, false, "before the deleting command"),
        (&same_command, false, "same command (cmin == curcid)"),
        (&after, true, "after CCI"),
    ] {
        let index = heap.build_deletion_index(snap, binding.tombstone_rel).expect("build");
        assert_eq!(index.is_deleted(target), want, "{what}");
        assert_differential(&heap, snap, binding.tombstone_rel, &probes(&[target]));
    }
    heap.commit(true).expect("commit");
}

#[test]
fn exhaustive_pattern_differential() {
    // A seeded rowid spread across parts/granules/edges, probed
    // exhaustively bitmap-vs-oracle under two snapshots (mid-schedule and
    // final).
    let mut heap = SimHeap::new();
    let mut rowids = Vec::new();
    // Deterministic spread: parts {0,1,5}, granule edges, dense run in
    // one granule (crosses the promotion ladder).
    for p in [0u32, 1, 5] {
        for g in [0u32, 7, 8, 19] {
            for row in [0u32, 1, 4095, 8190, 8191] {
                rowids.push(pack_rowid(p, g, row));
            }
        }
    }
    for row in 0..(LIST_PROMOTE_AT as u32 + 40) {
        rowids.push(pack_rowid(1, 3, row * 2)); // dense even rows: promotes
    }
    let binding = seed_tombstones(&mut heap, TABLE, &rowids).expect("seed");
    let mid = heap.snapshot();

    // A second, later deleter commits more (fresh snapshots see them,
    // `mid` must not).
    let late = vec![pack_rowid(0, 0, 77), pack_rowid(5, 19, 77)];
    seed_tombstones(&mut heap, TABLE, &late).expect("seed late");
    let fin = heap.snapshot();

    assert_differential(&heap, &mid, binding.tombstone_rel, &probes(&rowids));
    assert_differential(&heap, &mid, binding.tombstone_rel, &probes(&late));
    let mut all = rowids.clone();
    all.extend_from_slice(&late);
    assert_differential(&heap, &fin, binding.tombstone_rel, &probes(&all));
}

#[test]
fn duplicate_tombstones_collapse() {
    let mut heap = SimHeap::new();
    let r = pack_rowid(2, 2, 2);
    let binding = seed_tombstones(&mut heap, TABLE, &[r, r, r]).expect("seed");
    let snap = heap.snapshot();
    let index = heap.build_deletion_index(&snap, binding.tombstone_rel).expect("build");
    assert_eq!(index.total_deleted(), 1, "duplicate tombstones must collapse");
    assert!(index.is_deleted(r));
}

#[test]
fn promotion_ladder_preserves_content() {
    let mut b = DeletionIndexBuilder::new();
    // Descending insertion order (the builder must sort), crossing the
    // promotion threshold.
    let n = LIST_PROMOTE_AT as u32 + 17;
    for i in (0..n).rev() {
        b.add(pack_rowid(0, 0, i * 3)).expect("add");
    }
    let index = b.finish();
    let part = index.part(0).expect("part 0");
    let (g, dels) = part.granules().next().expect("granule 0");
    assert_eq!(g, 0);
    assert!(
        matches!(dels, GranuleDeletions::Bits { .. }),
        "past the threshold the ladder must have promoted"
    );
    assert_eq!(dels.count(), n);
    let rows = dels.rows();
    let want: Vec<u16> = (0..n).map(|i| (i * 3) as u16).collect();
    assert_eq!(rows, want, "promotion must preserve exact content, ordered");
    // Below the threshold a list stays a list.
    let mut b2 = DeletionIndexBuilder::new();
    for i in 0..(LIST_PROMOTE_AT as u32) {
        b2.add(pack_rowid(0, 0, i)).expect("add");
    }
    let i2 = b2.finish();
    let (_, d2) = i2.part(0).expect("part").granules().next().expect("granule");
    assert!(matches!(d2, GranuleDeletions::List(_)));
    // Memory account is nonzero and bounded (structural sanity for the
    // RSS witness).
    assert!(index.heap_bytes() >= 1024);
    assert!(index.heap_bytes() < 16 * 1024);
}

#[test]
fn filter_window_is_subtractive_only() {
    let mut b = DeletionIndexBuilder::new();
    for row in [3u32, 5, 900] {
        b.add(pack_rowid(0, 2, row)).expect("add");
    }
    let index = b.finish();
    let part = index.part(0).expect("part");

    // Window [0, 8) of granule 2: rows 3 and 5 die, order preserved.
    let mut positions: Vec<u32> = (0..8).collect();
    part.filter_window(2, 0, &mut positions);
    assert_eq!(positions, vec![0, 1, 2, 4, 6, 7]);

    // Window starting at 896: row 900 = position 4 dies.
    let mut positions: Vec<u32> = (0..8).collect();
    part.filter_window(2, 896, &mut positions);
    assert_eq!(positions, vec![0, 1, 2, 3, 5, 6, 7]);

    // A granule without deletions: untouched (including an already-
    // narrowed selection — subtractive-only composes with quals/PSMA in
    // any order).
    let mut positions = vec![1u32, 4, 7];
    part.filter_window(3, 0, &mut positions);
    assert_eq!(positions, vec![1, 4, 7]);
}

// -- the two born-RED teeth -------------------------------------------------

#[test]
fn tooth_seeded_visibility_skew_is_detected() {
    // The seeded skew hides one committed tombstone from the SCAN path
    // only; the differential MUST catch the disagreement (proving it can
    // fail — the born-RED law).
    let mut heap = SimHeap::new();
    let target = pack_rowid(0, 1, 42);
    let binding =
        seed_tombstones(&mut heap, TABLE, &[target, pack_rowid(0, 1, 43)]).expect("seed");
    heap.skew = Some(SkewTarget::HideTombstoneRowid(target));
    let snap = heap.snapshot();
    let index = heap.build_deletion_index(&snap, binding.tombstone_rel).expect("build");
    let disagreements: Vec<u64> = probes(&[target])
        .into_iter()
        .filter(|&r| index.is_deleted(r) != heap.oracle_is_deleted(&snap, binding.tombstone_rel, r))
        .collect();
    assert!(
        disagreements.contains(&target),
        "the differential failed to detect a seeded visibility skew — it has no teeth"
    );
}

#[test]
fn tooth_seeded_tombstone_drop_is_detected() {
    // A build path that loses one tombstone (the seeded drop) must be
    // caught by the differential.
    let mut heap = SimHeap::new();
    let rowids = [pack_rowid(0, 0, 1), pack_rowid(0, 0, 2), pack_rowid(0, 0, 3)];
    let binding = seed_tombstones(&mut heap, TABLE, &rowids).expect("seed");
    let snap = heap.snapshot();
    // Seed the drop: build from the visible list minus one entry.
    let mut visible = heap
        .visible_tombstones(&snap, binding.tombstone_rel)
        .expect("scan");
    let dropped = visible.pop().expect("nonempty");
    let mut b = DeletionIndexBuilder::new();
    b.add_all(visible).expect("add");
    let broken = b.finish();
    assert_ne!(
        broken.is_deleted(dropped),
        heap.oracle_is_deleted(&snap, binding.tombstone_rel, dropped),
        "the differential failed to detect a dropped tombstone — it has no teeth"
    );
}
