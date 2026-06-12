//! Seam declarations for the `backend-catalog-namespace` unit
//! (`catalog/namespace.c`), search-path-aware object name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_ts_config_oid(names, missing_ok)` (namespace.c): the OID of a
    /// text-search configuration given its possibly-qualified name list.
    /// With `missing_ok = false` a missing configuration raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `missing_ok = true` it is
    /// `Ok(InvalidOid)`.
    pub fn get_ts_config_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid>
);
