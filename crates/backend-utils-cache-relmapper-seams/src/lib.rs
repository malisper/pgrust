//! Seam declarations for the `backend-utils-cache-relmapper` unit
//! (`utils/cache/relmapper.c`), the catalog-to-filenumber map for mapped
//! relations, plus the rmgr-table callbacks it owns (slots of `RmgrTable`,
//! populated from `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::{Oid, RelFileNumber};

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
