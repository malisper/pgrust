//! Seam declarations for the `backend-access-index-indexam` unit
//! (`access/index/indexam.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `index_open(relationId, lockmode)` (indexam.c): open an index relation
    /// by OID — `relation_open` plus the not-an-index `ereport(ERROR)` check.
    /// The consumed slice of the relcache entry is copied into `mcx`. The
    /// owner installs the handle's closer, so `index_close(rel, lockmode)` is
    /// the returned handle's [`types_rel::Relation::close`] and drop is the
    /// abort-path `index_close(rel, NoLock)`.
    pub fn index_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<types_rel::Relation<'mcx>>
);
