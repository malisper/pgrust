//! Seam declarations for the `backend-commands-amcmds` unit
//! (`commands/amcmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `get_index_am_oid(amname, missing_ok)` (amcmds.c): resolve an access
    /// method name to its OID, requiring `amtype == AMTYPE_INDEX`. With
    /// `missing_ok = false` a missing/wrong-type AM raises (`Err`); with
    /// `missing_ok = true` it returns `InvalidOid`.
    pub fn get_index_am_oid(
        amname: &str,
        missing_ok: bool,
    ) -> types_error::PgResult<types_core::Oid>
);
