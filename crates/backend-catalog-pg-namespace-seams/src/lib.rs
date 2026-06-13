//! Seam declarations for the `backend-catalog-pg-namespace` unit
//! (`catalog/pg_namespace.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `NamespaceCreate(nspName, ownerId, isTemp)` (pg_namespace.c): insert a
    /// `pg_namespace` row (plus dependencies / object-access hook for non-temp)
    /// and return its OID. Can `ereport(ERROR)` (duplicate name, OOM, ...),
    /// carried on `Err`.
    pub fn namespace_create(nsp_name: &str, owner_id: Oid, is_temp: bool) -> PgResult<Oid>
);
