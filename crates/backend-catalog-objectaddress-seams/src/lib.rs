//! Seam declarations for the `backend-catalog-objectaddress` unit
//! (`catalog/objectaddress.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::PgResult;

seam_core::seam!(
    /// `getObjectDescription(object, missing_ok)` (objectaddress.c): a
    /// human-readable description of the object, palloc'd in the caller's
    /// current context (here: `mcx`). With `missing_ok = false` a vanished
    /// object raises (`Err`); the description machinery's catalog lookups can
    /// `ereport(ERROR)` too. `Err` includes OOM from the copy.
    pub fn get_object_description<'mcx>(
        mcx: Mcx<'mcx>,
        object: &ObjectAddress,
        missing_ok: bool,
    ) -> PgResult<PgString<'mcx>>
);
