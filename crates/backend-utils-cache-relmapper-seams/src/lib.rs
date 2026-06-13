//! Seam declarations for the `backend-utils-cache-relmapper` unit
//! (`utils/cache/relmapper.c`), the catalog-to-filenumber map for mapped
//! relations, plus the rmgr-table callbacks it owns (slots of `RmgrTable`,
//! populated from `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::{Oid, RelFileNumber};
use types_error::PgResult;

seam_core::seam!(
    /// `RelationMapFilenumberToOid(filenumber, shared)` (relmapper.c): the
    /// OID of the mapped relation with the given relfilenumber (`InvalidOid`
    /// if none). Infallible in C (pure in-memory map lookups).
    pub fn relation_map_filenumber_to_oid(filenumber: RelFileNumber, shared: bool) -> Oid
);

seam_core::seam!(
    /// `relmap_redo(record)` (relmapper.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn relmap_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtCCI_RelationMap()` — make pending relation-map changes visible to
    /// this transaction. C is `void`, but the merge can `elog(ERROR)` ("ran
    /// out of space in relation map") which longjmps; the owned port surfaces
    /// that as `Err`, so the seam returns `PgResult<()>`.
    pub fn at_cci_relation_map() -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOXact_RelationMap(isCommit, isParallelWorker)` — commit/discard
    /// relation-map updates; the commit path writes WAL and can
    /// `ereport(ERROR)`.
    pub fn at_eoxact_relation_map(is_commit: bool, is_parallel_worker: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtPrepare_RelationMap()` — errors out if the transaction changed the
    /// map (not supported under 2PC).
    pub fn at_prepare_relation_map() -> PgResult<()>
);

seam_core::seam!(
    /// `RelationMapInvalidate(shared)` (relmapper.c): reload the active
    /// relation map (the `shared` map when `shared`, else this database's
    /// local map) from the on-disk file — the `SHAREDINVALRELMAP_ID` arm of
    /// `LocalExecuteInvalidationMessage`. Reads the file, so can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn relation_map_invalidate(shared: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationMapFinishBootstrap()` (relmapper.c): write out the initial
    /// relation-map files at the end of bootstrap. `ereport(ERROR/FATAL)` on
    /// an I/O failure.
    pub fn relation_map_finish_bootstrap() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationMapOidToFilenumber(relationId, shared)` (relmapper.c).
    pub fn relation_map_oid_to_filenumber(relation_id: Oid, shared: bool) -> PgResult<RelFileNumber>
);
seam_core::seam!(
    /// `RelationMapUpdateMap(relationId, filenumber, shared, immediate)`.
    pub fn relation_map_update_map(
        relation_id: Oid,
        filenumber: RelFileNumber,
        shared: bool,
        immediate: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `RelationMapRemoveMapping(relationId)` (relmapper.c).
    pub fn relation_map_remove_mapping(relation_id: Oid) -> PgResult<()>
);
