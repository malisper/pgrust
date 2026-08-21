//! The delta scan-merge publication cell (the ONE shared surface this
//! crate adds; §6 gate 8 names it).
//!
//! An engagement builds the visible-tombstone index at most once and every
//! worker consumes the same immutable `Arc`. The cell rides
//! `pgsync::OnceLock` — THE single lock library (determinism-lint law),
//! whose `get_or_init` is exactly-once with losers parking (native world =
//! std `OnceLock`; sim world models the park; loom world is std-backed per
//! pgsync's L3 law — loom lacks `OnceLock`, so the loom model in
//! `tests/loom.rs` exercises this wrapper's protocol while the default-CI
//! real-thread stress covers the practical schedules).
//!
//! **Once-ledger discipline (P3 row 19; the lint's once-fence):** the
//! init closure here is a PURE FOLD over a caller-materialized rowid
//! slice — no IO, no env, no choke crossing, so parked losers can never
//! deadlock against a scheduler the winner needs. The tombstone SCAN
//! (the IO) happens strictly OUTSIDE the cell: callers materialize the
//! visible rowids first (the engagement leader normally does this before
//! workers spawn; a racing worker at worst performs one redundant scan
//! whose result loses the publication — bounded waste, never a lock-held
//! wait). This is the structural alternative the ledger names for
//! choke-crossing closures, chosen over a review-row exemption.
//!
//! Build FAILURES publish too (`DeltaError` is `Clone`): a poisoned input
//! (e.g. a delta-tagged tombstone) is the engagement's one outcome for
//! every waiter — the loud-error posture, not a retry loop.

use std::sync::Arc;

use crate::bitmap::{DeletionIndex, DeletionIndexBuilder};
use crate::DeltaResult;

/// Set-once publication cell for an engagement's [`DeletionIndex`].
#[derive(Debug, Default)]
pub struct DeltaScanState {
    cell: pgsync::OnceLock<DeltaResult<Arc<DeletionIndex>>>,
}

impl DeltaScanState {
    pub const fn new() -> DeltaScanState {
        DeltaScanState { cell: pgsync::OnceLock::new() }
    }

    /// The engagement's index, publishing exactly once: the first caller
    /// folds `visible_rowids` (already scanned + materialized OUTSIDE
    /// this cell — module doc) through the production builder; every
    /// other caller parks on the pure fold and receives the published
    /// result (value or error — the engagement's one outcome).
    pub fn get_or_publish(&self, visible_rowids: &[u64]) -> DeltaResult<Arc<DeletionIndex>> {
        self.cell
            .get_or_init(|| {
                // PURE fold (once-ledger proof row): slice in, index out.
                let mut b = DeletionIndexBuilder::new();
                b.add_all(visible_rowids.iter().copied())?;
                Ok(Arc::new(b.finish()))
            })
            .clone()
    }

    /// The published outcome, if the fold already ran (never builds —
    /// the pre-scan fast path for racing workers).
    pub fn try_get(&self) -> Option<DeltaResult<Arc<DeletionIndex>>> {
        self.cell.get().cloned()
    }
}
