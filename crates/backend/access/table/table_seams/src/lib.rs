//! Seam declarations for the `backend-access-table-table` unit
//! (`access/table/table.c`). The owning unit installs these from its
//! `init_seams()` when the cross-crate-cycle paths land; until then a call
//! panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use rel::Relation;
use types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `table_open(relationId, lockmode)` (table.c): open+lock a table by OID.
    pub fn table_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    /// `try_table_open(relationId, lockmode)` (table.c): like [`table_open`], but
    /// returns `Ok(None)` (the C `NULL`) when the relation has disappeared rather
    /// than raising. Used by the REINDEX MISSING_OK path.
    pub fn try_table_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Option<Relation<'mcx>>>
);

seam_core::seam!(
    /// `relation_close(relid, lockmode)` (relation.c) for the cases where the
    /// owned-handle is no longer in scope (the C `goto out` early-exits close
    /// the same OID the caller still references). Refcount + optional lock.
    pub fn relation_close(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);
