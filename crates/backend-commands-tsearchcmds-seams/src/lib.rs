//! Seam declarations for the `backend-commands-tsearchcmds` unit
//! (`commands/tsearchcmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_cache::DefElemString;
use types_error::PgResult;

seam_core::seam!(
    /// `deserialize_deflist(txt)` (tsearchcmds.c): build a `List` of
    /// `DefElem` from a stored `text` datum's string contents. Every produced
    /// `DefElem` has a `String`-node argument, so the list crosses as typed
    /// rows allocated in `mcx`. `Err` carries the C `ereport(ERROR, "invalid
    /// deserialize_deflist syntax")` and OOM.
    pub fn deserialize_deflist<'mcx>(
        mcx: Mcx<'mcx>,
        txt: &str,
    ) -> PgResult<PgVec<'mcx, DefElemString<'mcx>>>
);
