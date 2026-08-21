//! The per-table delta-relation lifecycle: contract + laws (M5-B), real
//! catalog machinery at M5-M.
//!
//! A pgrcolumnar2 table owns AT MOST ONE **delta pair**: the delta
//! relation (the table's column schema verbatim) + the tombstone relation
//! (`TOMBSTONE_NATTS` = 1 int8 attribute, spec §15). Both are ordinary
//! heap relations — WAL, MVCC, vacuum, crash recovery all inherited from
//! heap; this crate adds NO storage semantics to them, only the binding
//! discipline:
//!
//! - **Lazy creation at first trickle write** (`lookup_or_create`),
//!   mirroring the table directory's own lazy-at-first-ingest posture
//!   (`pgrc2_am::ingest::open_writer`). Creation is TRANSACTIONAL catalog
//!   work (the `catalog_toasting` companion-relation precedent:
//!   `heap_create_with_catalog` + dependency link + CCI): the creating
//!   transaction's abort undoes the pair with no residue.
//! - **Idempotent lookup**: a second `lookup_or_create` returns the same
//!   binding; concurrent creators serialize on the implementor's lock
//!   discipline (product: the parent relation lock, exactly TOAST's).
//! - **Dropped with the table** (dependency-linked): DROP TABLE removes
//!   the pair through ordinary dependency traversal; no delta-specific
//!   DROP hook exists.
//! - **TRUNCATE resets both** relations of the pair (`reset`), riding the
//!   heap truncate machinery (transactional or not exactly as the
//!   table's own truncate arm decides — the pair follows the parent).
//! - **A table with no pair has an empty delta store**: every scan face
//!   treats `lookup == None` as zero delta rows and zero tombstones (the
//!   overwhelmingly common posture for bulk-only tables; scans pay
//!   nothing for the feature until first trickle DML).
//!
//! Identities are implementor-scoped opaque u64s (product: relation OIDs;
//! testkit: sim rel ids). This crate never interprets them.

use crate::DeltaResult;

/// A table's delta pair (opaque implementor identities).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaBinding {
    /// The delta relation (table columns verbatim).
    pub delta_rel: u64,
    /// The tombstone relation (one int8 attribute).
    pub tombstone_rel: u64,
}

/// The lifecycle seam (product implementor at M5-M over
/// `catalog_toasting`-shaped machinery; crate-grain implementor is
/// [`crate::testkit::SimHeap`]).
pub trait DeltaLifecycle {
    /// The table's pair, if it exists.
    fn lookup(&mut self, table: u64) -> DeltaResult<Option<DeltaBinding>>;

    /// The table's pair, creating it (transactionally, idempotently) on
    /// first need.
    fn lookup_or_create(&mut self, table: u64) -> DeltaResult<DeltaBinding>;

    /// TRUNCATE: reset both relations of the pair (no-op without one).
    fn reset(&mut self, table: u64) -> DeltaResult<()>;

    /// DROP TABLE path (dependency traversal at product grain): remove
    /// the pair (no-op without one).
    fn drop_pair(&mut self, table: u64) -> DeltaResult<()>;
}
