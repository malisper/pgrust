//! The visible-tombstone bitmap (charter §9: "scans build the visible-
//! tombstone bitmap per part and intersect it into the selection").
//!
//! Built ONCE per scan from the tombstone rows visible under the scan
//! snapshot (visibility = ordinary heap MVCC, decided by the heap scan
//! that feeds the builder — this crate never re-decides it), then consumed
//! immutably by every worker (`crate::scanstate` publishes the `Arc`).
//!
//! ## Structure
//!
//! part_no → granule → deletions, with the granule grain carrying the SAME
//! list/bitmap ladder as the frozen DV block vocabulary (spec §15:
//! `count` × u16 ordinals ascending, or an 8,192-bit bitmap) — deliberate
//! symmetry: M5-N's DV compiler folds a [`GranuleDeletions`] straight into
//! a `DvBlockKind` without reshaping. Sparse granules stay lists;
//! [`LIST_PROMOTE_AT`] promotes to the bitmap form (memory-bounded either
//! way: a granule costs at most 1 KiB + enum overhead, and only granules
//! with deletions cost anything — the peak-RSS account is
//! [`DeletionIndex::heap_bytes`]).
//!
//! ## Laws
//!
//! - **Idempotent add**: duplicate tombstones for one row collapse (crash
//!   replay and EPQ-adjacent schedules may legally present duplicates;
//!   the bitmap is a set).
//! - **Delta-tagged rowids refuse typed** (defense in depth behind the
//!   `tombstone` encode guard — product-grain bytes arrive from a heap
//!   scan this crate does not control).
//! - **Unknown parts are LEGAL at consumption**: a tombstone may name a
//!   part absent from the scan's effective manifest generation (published
//!   after the deleting txn, or folded away once M5-N compacts). The scan
//!   consumes per-part lookups ([`DeletionIndex::part`]) and never
//!   requires the index's part set ⊆ the scanned set; rows of an unseen
//!   part are simply never staged, so their deletions are moot. The
//!   builder therefore accepts any admissible part_no (≤ the pair bound —
//!   beyond it is corruption, refused typed).
//! - **Intersection is subtractive only**: a deletion can only REMOVE a
//!   staged position, never add one — composing with pruning/PSMA (which
//!   are advisory-skip) stays sound in any order.

use std::collections::BTreeMap;

use pgrc2_format::geom::GRANULE_ROWS;
use pgrc2_format::rowid::{rowid_granule, rowid_part, rowid_row};

use crate::rowid::{check_part_no_for_pair, is_delta_rowid};
use crate::{DeltaError, DeltaResult};

/// List→bitmap promotion threshold (entries per granule). At 128 entries a
/// list costs 256 B against the bitmap's fixed 1,024 B; promoting at 128
/// keeps worst-case memory within 4× of optimal while keeping sparse
/// granules cheap. Not a tuning surface — a structural constant (the DV
/// compiler re-decides its own wire kind per spec §15 independently).
pub const LIST_PROMOTE_AT: usize = 128;

const BITMAP_WORDS: usize = (GRANULE_ROWS as usize) / 64;

/// Deletions within one granule (the DV-block-shaped ladder).
#[derive(Clone, Debug)]
pub enum GranuleDeletions {
    /// Strictly-ascending row ordinals (kept sorted by insertion).
    List(Vec<u16>),
    /// 8,192-bit bitmap, LSB-first within words; `count` = set bits.
    Bits { words: Box<[u64; BITMAP_WORDS]>, count: u32 },
}

impl GranuleDeletions {
    #[inline]
    fn contains(&self, row: u16) -> bool {
        match self {
            GranuleDeletions::List(l) => l.binary_search(&row).is_ok(),
            GranuleDeletions::Bits { words, .. } => {
                (words[(row / 64) as usize] >> (row % 64)) & 1 != 0
            }
        }
    }

    /// Insert (idempotent); returns TRUE iff newly inserted.
    fn insert(&mut self, row: u16) -> bool {
        match self {
            GranuleDeletions::List(l) => match l.binary_search(&row) {
                Ok(_) => false,
                Err(pos) => {
                    l.insert(pos, row);
                    if l.len() > LIST_PROMOTE_AT {
                        let mut words = Box::new([0u64; BITMAP_WORDS]);
                        for &r in l.iter() {
                            words[(r / 64) as usize] |= 1u64 << (r % 64);
                        }
                        let count = l.len() as u32;
                        *self = GranuleDeletions::Bits { words, count };
                    }
                    true
                }
            },
            GranuleDeletions::Bits { words, count } => {
                let w = &mut words[(row / 64) as usize];
                let bit = 1u64 << (row % 64);
                if *w & bit != 0 {
                    false
                } else {
                    *w |= bit;
                    *count += 1;
                    true
                }
            }
        }
    }

    /// Deleted-row count in this granule.
    #[inline]
    pub fn count(&self) -> u32 {
        match self {
            GranuleDeletions::List(l) => l.len() as u32,
            GranuleDeletions::Bits { count, .. } => *count,
        }
    }

    /// Row ordinals in ascending order (the M5-N DV-compile currency).
    pub fn rows(&self) -> Vec<u16> {
        match self {
            GranuleDeletions::List(l) => l.clone(),
            GranuleDeletions::Bits { words, .. } => {
                let mut out = Vec::with_capacity(self.count() as usize);
                for (wi, &w) in words.iter().enumerate() {
                    let mut bits = w;
                    while bits != 0 {
                        let b = bits.trailing_zeros();
                        out.push((wi * 64) as u16 + b as u16);
                        bits &= bits - 1;
                    }
                }
                out
            }
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            GranuleDeletions::List(l) => l.capacity() * 2,
            GranuleDeletions::Bits { .. } => BITMAP_WORDS * 8,
        }
    }
}

/// One part's visible deletions.
#[derive(Clone, Debug, Default)]
pub struct PartDeletions {
    granules: BTreeMap<u32, GranuleDeletions>,
    deleted: u64,
}

impl PartDeletions {
    /// TRUE iff (granule, row) is deleted.
    ///
    /// The row-bound check is RELEASE-EFFECTIVE (the debug-assert-masking
    /// law; the silent-lossy-serialization class): `row as u16` under a
    /// debug-only guard would, in release, wrap rows ≥ 65,536 mod 65,536
    /// and could FALSELY match a deletion — silently dropping a live row.
    /// A caller outside the granule bound is a programming error adjacent
    /// to data loss: crash loudly, never corrupt.
    #[inline]
    pub fn is_deleted(&self, granule: u32, row: u32) -> bool {
        assert!(
            row < GRANULE_ROWS,
            "pgrc2 delta bitmap: row {row} outside the granule (lossy-cast guard)"
        );
        self.granules
            .get(&granule)
            .is_some_and(|g| g.contains(row as u16))
    }

    /// Deleted-row count in this part.
    #[inline]
    pub fn deleted(&self) -> u64 {
        self.deleted
    }

    /// This part's granules with deletions, ascending (the M5-N currency).
    pub fn granules(&self) -> impl Iterator<Item = (u32, &GranuleDeletions)> {
        self.granules.iter().map(|(g, d)| (*g, d))
    }

    /// Intersect the visible-tombstone bitmap into a staged window's
    /// selection: retain the positions of `positions` (window-relative)
    /// whose row `win_start + p` in `granule` is NOT deleted. Subtractive
    /// only; positions order is preserved. Row bounds are RELEASE-
    /// EFFECTIVE (see [`PartDeletions::is_deleted`] — the lossy-cast
    /// guard: a wrapped row could falsely match and drop a live row).
    pub fn filter_window(&self, granule: u32, win_start: u32, positions: &mut Vec<u32>) {
        let Some(g) = self.granules.get(&granule) else {
            return;
        };
        positions.retain(|&p| {
            let row = win_start + p;
            assert!(
                row < GRANULE_ROWS,
                "pgrc2 delta bitmap: window row {row} outside the granule \
                 (lossy-cast guard)"
            );
            !g.contains(row as u16)
        });
    }
}

/// The per-scan visible-tombstone index (immutable after build; shared as
/// `Arc` through `crate::scanstate`).
#[derive(Clone, Debug, Default)]
pub struct DeletionIndex {
    parts: BTreeMap<u32, PartDeletions>,
    total: u64,
}

impl DeletionIndex {
    /// TRUE iff no deletions at all (the fast common posture: scans skip
    /// every per-window lookup).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Total deleted rows across parts.
    #[inline]
    pub fn total_deleted(&self) -> u64 {
        self.total
    }

    /// One part's deletions, if any.
    #[inline]
    pub fn part(&self, part_no: u32) -> Option<&PartDeletions> {
        self.parts.get(&part_no)
    }

    /// Deleted-row count for one part (0 when unlisted).
    #[inline]
    pub fn deleted_in_part(&self, part_no: u32) -> u64 {
        self.parts.get(&part_no).map_or(0, |p| p.deleted)
    }

    /// TRUE iff `sealed_rowid` is deleted.
    #[inline]
    pub fn is_deleted(&self, sealed_rowid: u64) -> bool {
        self.parts
            .get(&rowid_part(sealed_rowid))
            .is_some_and(|p| p.is_deleted(rowid_granule(sealed_rowid), rowid_row(sealed_rowid)))
    }

    /// Parts with deletions, ascending (the M5-N currency).
    pub fn parts(&self) -> impl Iterator<Item = (u32, &PartDeletions)> {
        self.parts.iter().map(|(p, d)| (*p, d))
    }

    /// Structural memory account (the peak-RSS witness input): heap bytes
    /// held by the index beyond `size_of::<Self>`.
    pub fn heap_bytes(&self) -> usize {
        self.parts
            .values()
            .map(|p| {
                core::mem::size_of::<PartDeletions>()
                    + p.granules
                        .values()
                        .map(|g| core::mem::size_of::<GranuleDeletions>() + g.heap_bytes())
                        .sum::<usize>()
            })
            .sum()
    }
}

/// Builder: feed every VISIBLE tombstone rowid (heap MVCC already applied
/// by the scanning side), then [`DeletionIndexBuilder::finish`].
#[derive(Debug, Default)]
pub struct DeletionIndexBuilder {
    index: DeletionIndex,
}

impl DeletionIndexBuilder {
    pub fn new() -> DeletionIndexBuilder {
        DeletionIndexBuilder::default()
    }

    /// Add one visible tombstone's sealed rowid. Idempotent; typed
    /// refusal for delta-tagged rowids — which ALSO covers every
    /// pair-bound-violating part by construction: a part_no ≥ 2^31 packed
    /// per spec §10 sets rowid bit 63, i.e. IS the delta tag (the exact
    /// aliasing `rowid::MAX_PAIR_PART_NO` exists to forbid; the CI cluster
    /// units leg proved a separate part-bound check here unreachable).
    /// The part-bound check on REAL part numbers lives at the pair-scan
    /// builder, over the manifest's part_no field.
    pub fn add(&mut self, sealed_rowid: u64) -> DeltaResult<()> {
        if is_delta_rowid(sealed_rowid) {
            return Err(DeltaError::DeltaTaggedTombstone { rowid: sealed_rowid });
        }
        let part_no = rowid_part(sealed_rowid);
        debug_assert!(check_part_no_for_pair(part_no).is_ok(), "subsumed by the tag check");
        let granule = rowid_granule(sealed_rowid);
        let row = rowid_row(sealed_rowid);
        debug_assert!(row < GRANULE_ROWS);
        let part = self.index.parts.entry(part_no).or_default();
        let g = part
            .granules
            .entry(granule)
            .or_insert_with(|| GranuleDeletions::List(Vec::new()));
        if g.insert(row as u16) {
            part.deleted += 1;
            self.index.total += 1;
        }
        Ok(())
    }

    /// Convenience: add every rowid of an iterator (first error wins).
    pub fn add_all<I: IntoIterator<Item = u64>>(&mut self, rowids: I) -> DeltaResult<()> {
        for r in rowids {
            self.add(r)?;
        }
        Ok(())
    }

    pub fn finish(self) -> DeletionIndex {
        self.index
    }
}
