//! Seam declarations for the `backend-commands-extension` unit
//! (`commands/extension.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// The `creating_extension` backend-global (extension.c): true while a
    /// CREATE EXTENSION script is executing. A plain global read — infallible.
    pub fn creating_extension() -> bool
);

seam_core::seam!(
    /// The `CurrentExtensionObject` backend-global (extension.c): the OID of
    /// the pg_extension row being created. A plain global read — infallible.
    pub fn current_extension_object() -> Oid
);

seam_core::seam!(
    /// `get_extension_name(ext_oid)` (extension.c): the extension's name,
    /// copied out of the syscache into `mcx` (C: `pstrdup` in the current
    /// context). `Ok(None)` when there is no such extension (the C NULL
    /// return). `Err` includes OOM from the copy.
    pub fn get_extension_name<'mcx>(
        mcx: Mcx<'mcx>,
        ext_oid: Oid,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_extension_oid(extname, missing_ok)` (extension.c): the OID of the
    /// named extension, or `InvalidOid` with `missing_ok = true`. With
    /// `missing_ok = false` a miss raises `ERRCODE_UNDEFINED_OBJECT` (`Err`).
    pub fn get_extension_oid(extname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RemoveExtensionById(extId)` (commands/extension.c): the per-class
    /// `OCLASS_EXTENSION` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_extension` object. Removes the extension's catalog row. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveExtensionById(extId: Oid) -> PgResult<()>
);
