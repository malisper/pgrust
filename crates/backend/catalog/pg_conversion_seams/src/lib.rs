//! Seam declarations for the `backend-catalog-pg-conversion` unit
//! (`catalog/pg_conversion.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `FindDefaultConversion(connamespace, for_encoding, to_encoding)`
    /// (pg_conversion.c): the pg_proc OID of the default conversion, or
    /// `InvalidOid` if not found. `Err` carries catcache-path
    /// `ereport(ERROR)`s.
    pub fn find_default_conversion(
        connamespace: Oid,
        for_encoding: i32,
        to_encoding: i32,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `ConversionCreate(conname, connamespace, conowner, conforencoding,
    /// contoencoding, conproc, def)` (pg_conversion.c): insert a new pg_conversion
    /// tuple (duplicate-name check, GetNewOidWithIndex, heap_form_tuple,
    /// CatalogTupleInsert), record the schema/owner/proc dependencies and the
    /// post-create hook, and return the new conversion's `ObjectAddress`. Opens
    /// and closes `pg_conversion` itself. `Err` carries the
    /// `ERRCODE_DUPLICATE_OBJECT` and catalog-path `ereport(ERROR)`s.
    pub fn conversion_create(
        conname: &str,
        connamespace: Oid,
        conowner: Oid,
        conforencoding: i32,
        contoencoding: i32,
        conproc: Oid,
        def: bool,
    ) -> PgResult<ObjectAddress>
);
