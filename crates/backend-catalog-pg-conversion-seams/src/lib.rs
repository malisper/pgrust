//! Seam declarations for the `backend-catalog-pg-conversion` unit
//! (`catalog/pg_conversion.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

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
