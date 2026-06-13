//! Seam declarations for the `backend-catalog-pg-opclass` unit
//! (`src/backend/catalog/pg_opclass.c`).
//!
//! `GetDefaultOpClass` lives in `pg_opclass.c`, NOT in `lsyscache.c`: it opens
//! `pg_class` for `pg_opclass`, `systable_beginscan`s the `OpclassAmNameNspIndexId`
//! index for the access method, and walks the default opclasses applying
//! `getBaseType` / `TypeCategory` / `IsBinaryCoercible` / `IsPreferredType` to
//! resolve the unique exact/compatible/preferred-compatible match. All of that
//! machinery is in still-unported neighbors, so `lsyscache`'s `get_default_opclass`
//! convenience surface routes the whole computation through this owner's seam
//! (a loud panic until `pg_opclass.c` lands).
//!
//! The owning unit (`backend-catalog-pg-opclass`) installs this from its
//! `init_seams()` when it lands.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `GetDefaultOpClass(type_id, am_id)` (pg_opclass.c): the default operator
    /// class OID for `type_id` in access method `am_id`, or `InvalidOid` when
    /// there is no unambiguous default. `Err` carries the
    /// `ereport(ERROR, ...ambiguous...)` raised when more than one
    /// preferred-compatible / compatible default matches (and the catalog-scan
    /// failure surface).
    pub fn get_default_opclass(type_id: Oid, am_id: Oid) -> PgResult<Oid>
);
