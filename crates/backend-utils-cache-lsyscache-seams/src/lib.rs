//! Seam declarations for the `backend-utils-cache-lsyscache` unit
//! (`utils/cache/lsyscache.c` convenience catalog lookups).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_opfamily_name(opfid, missing_ok)` (lsyscache.c): the opfamily's
    /// name. With `missing_ok = false` a missing opfamily raises (`Err`); with
    /// `missing_ok = true` it is `Ok(None)`.
    pub fn get_opfamily_name(opfid: Oid, missing_ok: bool) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `get_opclass_input_type(opclass)` (lsyscache.c): the opclass's
    /// `opcintype`. A missing opclass is the C `elog(ERROR, "cache lookup
    /// failed for opclass %u")`, carried on `Err`.
    pub fn get_opclass_input_type(opclass: Oid) -> PgResult<Oid>
);
