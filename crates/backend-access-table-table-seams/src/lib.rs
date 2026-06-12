//! Seam declarations for the `backend-access-table-table` unit
//! (`access/table/table.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The C `Relation` (`struct RelationData *`) crosses these seams as
//! [`types_rel::rel::RelationData`]; the `table_open`..`table_close`
//! ownership span is the [`OpenRelation`] guard, so the lock and the relcache
//! open-count can never leak across a `?` — `Drop` is the abort path, the
//! explicit [`OpenRelation::close`] the success path (C's `table_close`
//! call sites).

use types_error::PgResult;
use types_rel::rel::RelationData;
use types_tuple::access::LOCKMODE;

seam_core::seam!(
    /// `table_open(relationId, lockmode)` (access/table/table.c): open a table
    /// relation by OID — `relation_open` plus a check that the relation is not
    /// an index nor a composite type. The owner returns the opened
    /// [`RelationData`] wrapped in an [`OpenRelation`] guard
    /// (`OpenRelation::new(rel, lockmode)`). `Err` carries the C
    /// `ereport(ERROR)`s: a relation that cannot be opened (`could not open
    /// relation with OID %u`), a wrong relation kind, or a lock-acquisition
    /// error.
    pub fn table_open(
        relation_id: types_core::primitive::Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<OpenRelation>
);

seam_core::seam!(
    /// `table_close(relation, lockmode)` (access/table/table.c): close a table
    /// previously opened with `table_open`. If `lockmode` is not `NoLock`, the
    /// specified lock is then released; `LockRelease` can `elog(ERROR)`
    /// (`unrecognized lock mode`, `failed to re-find shared lock object`),
    /// carried on `Err`. Callers do not invoke this directly — it is reached
    /// through [`OpenRelation::close`] / [`OpenRelation`]'s `Drop`.
    pub fn table_close(relation: &RelationData, lockmode: LOCKMODE) -> PgResult<()>
);

/// The `table_open`..`table_close` ownership span: holds the open relation and
/// releases it exactly once. Dropping the guard (the error/unwind path —
/// any `?` while the relation is open) closes with the lock mode the open
/// took, mirroring transaction-abort lock release; the success path calls
/// [`OpenRelation::close`] with an explicit lock mode, mirroring the C
/// `table_close(rel, lockmode)` call sites (e.g. `NoLock` to keep the lock).
#[derive(Debug)]
pub struct OpenRelation {
    rel: RelationData,
    open_lockmode: LOCKMODE,
    closed: bool,
}

impl OpenRelation {
    /// Wrap a freshly opened relation. Constructed by the `table_open` owner;
    /// consumers only receive guards from [`table_open`].
    pub fn new(rel: RelationData, open_lockmode: LOCKMODE) -> Self {
        OpenRelation {
            rel,
            open_lockmode,
            closed: false,
        }
    }

    /// `table_close(relation, lockmode)` — the explicit success-path close.
    pub fn close(mut self, lockmode: LOCKMODE) -> PgResult<()> {
        self.closed = true;
        table_close::call(&self.rel, lockmode)
    }
}

impl core::ops::Deref for OpenRelation {
    type Target = RelationData;

    fn deref(&self) -> &RelationData {
        &self.rel
    }
}

impl Drop for OpenRelation {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        if let Err(e) = table_close::call(&self.rel, self.open_lockmode) {
            // An error while releasing on the error path is C's
            // error-during-abort: escalate, never swallow. (Skip when already
            // unwinding from a panic to avoid a double-panic abort hiding the
            // original failure.)
            if !std::thread::panicking() {
                panic!(
                    "table_close failed while unwinding (relation {}): {}",
                    self.rel.rd_id,
                    e.message()
                );
            }
        }
    }
}
