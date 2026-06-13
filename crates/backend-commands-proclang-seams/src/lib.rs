//! Seam declarations for the `backend-commands-proclang` unit
//! (`commands/proclang.c`): procedural-language lookups.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_language_oid(langname, missing_ok)` (proclang.c): the procedural
    /// language's OID. With `missing_ok = false` a missing language raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `missing_ok = true` it is
    /// `Ok(InvalidOid)`.
    pub fn get_language_oid(langname: &str, missing_ok: bool) -> PgResult<Oid>
);
