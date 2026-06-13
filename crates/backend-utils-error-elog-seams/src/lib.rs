//! Seam declarations for the `backend-utils-error-elog` unit
//! (`utils/error/elog.c`). The owning unit installs these from its
//! `init_seams()` when the cross-crate-cycle paths land; until then a call
//! panics loudly.

#![allow(non_snake_case)]

use types_error::{ErrorLevel, PgResult};

seam_core::seam!(
    /// `ereport(elevel, (errmsg("..."), [errdetail("...")]))` for the
    /// INFO/DEBUG2 progress logging in `copy_table_data`. Crosses as a
    /// pre-rendered message + optional detail; the owner emits at `elevel`.
    pub fn ereport_msg(elevel: ErrorLevel, msg: String, detail: Option<String>) -> PgResult<()>
);
