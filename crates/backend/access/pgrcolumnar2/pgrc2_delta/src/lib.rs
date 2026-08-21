//! # pgrc2_delta — the heap delta-store lifecycle for pgrcolumnar2 (chunk
//! M5-B, `docs/design/lanev3-m5-chunks.md` §2/§5)
//!
//! Design authority: `docs/design/pgrcolumnar-v2.md` §9 + rulings O-2/O-3
//! (tombstones-first with the DV ladder, ruled 08-07), consumed verbatim.
//! Byte authority: `docs/design/pgrc2-format.md` §15 (the FROZEN tombstone/
//! DV vocabulary, `pgrc2_format::dml`) — this crate writes MACHINERY over
//! that vocabulary and never moves a byte of format (`FORMAT_VERSION` is
//! untouchable here; amendments are A-lane PRs against the format crate).
//!
//! ## The delta store (what this crate is)
//!
//! A pgrcolumnar2 table's sealed parts are immutable (spec §1). Trickle DML
//! therefore lands ROW-SIDE, in a per-table **heap delta store**:
//!
//! - **Trickle INSERT** appends the row to the table's heap **delta
//!   relation** (the table's own column schema, verbatim).
//! - **DELETE of a sealed row** appends a **tombstone** to the table's heap
//!   **tombstone relation**: one int8 attribute carrying the deleted packed
//!   RowId (spec §15, `pgrc2_format::dml::TOMBSTONE_NATTS` = 1).
//! - **DELETE of a delta-resident row** is an ordinary heap delete on the
//!   delta relation — no tombstone is ever written for a delta row (LAW;
//!   [`tombstone`] and [`bitmap`] both refuse delta-tagged rowids typed).
//! - **UPDATE of a sealed row** = tombstone + delta-insert ([`ops`], one
//!   transaction, order pinned tombstone-first for determinism; atomicity
//!   is the transaction's, not the order's).
//! - **UPDATE of a delta-resident row** is an ordinary heap update.
//!
//! Deletion visibility is **ordinary heap MVCC on the tombstone rows** —
//! subtransactions, command ids (combo cids), and EPQ come free and
//! provably C-exact (the O-2 design's point). Scans build the per-part
//! **visible-tombstone bitmap** ([`bitmap::DeletionIndex`]: heap MVCC ∩
//! per-part selection) once per scan and intersect it into the staged
//! selection (the lx_source `pgrc.rs` consumption; the batch selection is
//! the row currency downstream, so an intersected selection is honored
//! end-to-end).
//!
//! ## WAL and durability (the #253 law, restated for the delta side)
//!
//! **Delta WAL is inherited from heap** (O-3): every delta/tombstone write
//! is an ordinary heap write and rides heap WAL — this crate builds no WAL
//! machinery. Consequently a trickle-DML transaction reaches commit with
//! `wrote_xlog = true` and takes the synchronous commit path structurally
//! under `synchronous_commit = on`: the #248/#253 hazard class (durable
//! state whose transaction wrote NO WAL of its own + the async-commit
//! shortcut ⇒ acked-then-lost commits and xid-recycling resurrection —
//! `xact_seams::force_sync_commit`, armed by `pgrc2_write::publish` at
//! every part publish) cannot arise for a pure trickle transaction. The
//! law this crate carries and its battery proves at crate grain
//! ([`testkit::SimHeap`] + `tests/crash.rs`): **an acked delta write
//! survives kill -9 at every op boundary** — the commit record is flushed
//! before the ack, and a seeded sync-skip (the born-RED tooth) makes the
//! checker fail, proving the battery can detect the loss shape.
//!
//! ## Scan-merge (the reserved `lx_source::hetero` windows)
//!
//! One logical scan = sealed parts + delta windows, composed by
//! `lx_source::hetero::PairSource` (built M1, reserved by the M3 table for
//! exactly this use). The staging laws this crate's faces uphold:
//!
//! - **Identical StageSchema** on both sides (PairSource's build gate).
//!   The delta side therefore stages DETOASTED, arena-resident images so
//!   its staged cells are byte-identical to what sealing + decode would
//!   produce ([`feed`]'s detoast law) — a raw heap source can never pass
//!   the gate (its varlena cells alias page images, `inline_proven:
//!   false`).
//! - **Whole-band claims** (spec §2, the +78% law): the sealed side claims
//!   whole boundary spans; the pair composite delegates claims side-at-a-
//!   time (A drained, then B), so no pair claim can split a band or a
//!   dict epoch by construction.
//! - **Ordering vouches only on evidence** (`with_vouched_ordering`
//!   discipline): the pair publishes an ordering only when both sides
//!   publish the same ordering AND the caller proves key separation
//!   (sealed max ≤ delta min); the delta side arms a release-effective
//!   floor witness so a false vouch fails loudly at stage time.
//!
//! ## RowId spaces ([`rowid`])
//!
//! Sealed rows are addressed by the frozen §10 packing (part, granule,
//! row). Delta-resident rows are addressed in a DISJOINT space: bit 63 set
//! + the heap TID packing (`(block << 16) | offnum`) — scan-plane
//! vocabulary owned HERE (not format; tombstone payloads never carry it).
//! Disjointness is enforced at pair build: a part_no ≥ 2^31 is refused
//! typed (`MAX_PAIR_PART_NO`; unreachable in practice — part numbers are
//! publish-monotone from zero).
//!
//! ## Ownership and handoffs (chunk table §3)
//!
//! This crate is M5-B's, handed ONCE at a merge boundary to M5-N
//! (compaction/DV: new files in this crate — the horizon rung, the DV
//! compile over `pgrc2_format::dml::encode_dv`, the fold via the frozen
//! M3-D seal face). M5-M binds [`ops`]/[`lifecycle`]/[`feed`] to the real
//! heap relations + catalog (the `catalog_toasting` companion-relation
//! precedent) and retires the `nodemodifytable` trickle gate. The faces in
//! this crate are their frozen donor surface — amendments after M5-B's
//! merge are single-owner PRs by the then-owner.
//!
//! ## Concurrency posture
//!
//! Scan-side state is worker-private (R2) except ONE surface: the
//! scan-state publication cell ([`scanstate::DeltaScanState`]) — a
//! set-once `pgsync::OnceLock` publishing the immutable
//! `Arc<DeletionIndex>` to an engagement's workers (built at most once;
//! losers wait). The bitmap is immutable after publication; there is no
//! other shared mutable state in this crate. `tests/loom.rs` models the
//! cell; a default-CI real-thread stress covers the practical schedules
//! (pgsync's L3 law: loom lacks OnceLock, so the loom arm is std-backed —
//! the model exercises the protocol, the stress exercises the race).

#![allow(clippy::result_large_err)]

pub mod bitmap;
pub mod feed;
pub mod lifecycle;
pub mod ops;
pub mod rowid;
pub mod scanstate;
pub mod testkit;
pub mod tombstone;

#[cfg(test)]
mod tests;

use std::fmt;

/// Typed error vocabulary (no panics on hostile input anywhere in this
/// crate; `Clone` so the scan-state cell can publish a build failure to
/// every waiter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaError {
    /// A tombstone (write or bitmap-build) named a delta-tagged rowid.
    /// Delta rows die by heap delete, never by tombstone (crate doc law).
    DeltaTaggedTombstone { rowid: u64 },
    /// A pair scan admitted a part whose part_no collides with the
    /// delta rowid tag space (`rowid::MAX_PAIR_PART_NO`).
    PartNoBeyondPairBound { part_no: u32 },
    /// A caller violated a documented face contract (programming error
    /// surfaced typed, never UB).
    Contract { detail: &'static str },
    /// The heap side (seam implementor) failed.
    Heap { at: &'static str, detail: String },
}

impl fmt::Display for DeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaError::DeltaTaggedTombstone { rowid } => write!(
                f,
                "pgrc2 delta: tombstone names a delta-tagged rowid {rowid:#x} \
                 (delta rows die by heap delete, never tombstone)"
            ),
            DeltaError::PartNoBeyondPairBound { part_no } => write!(
                f,
                "pgrc2 delta: part_no {part_no} beyond the pair-scan bound \
                 (delta rowid tag space; parts are publish-monotone from zero \
                 so this is corruption, not capacity)"
            ),
            DeltaError::Contract { detail } => {
                write!(f, "pgrc2 delta: contract violation: {detail}")
            }
            DeltaError::Heap { at, detail } => {
                write!(f, "pgrc2 delta: heap seam failure at {at}: {detail}")
            }
        }
    }
}

impl std::error::Error for DeltaError {}

pub type DeltaResult<T> = Result<T, DeltaError>;

impl DeltaError {
    /// Render for a foreign error surface (the lx_source binding maps
    /// through its own `PgError` vocabulary).
    pub fn to_message(&self) -> String {
        self.to_string()
    }
}
