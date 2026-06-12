//! Seam declarations for the `backend-utils-cache-relmapper` unit
//! (`utils/cache/relmapper.c`), the catalog-to-filenumber map for mapped
//! relations.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::{Oid, RelFileNumber};

seam_core::seam!(
    /// `RelationMapFilenumberToOid(filenumber, shared)` (relmapper.c): the
    /// OID of the mapped relation with the given relfilenumber (`InvalidOid`
    /// if none). Infallible in C (pure in-memory map lookups).
    pub fn relation_map_filenumber_to_oid(filenumber: RelFileNumber, shared: bool) -> Oid
);
