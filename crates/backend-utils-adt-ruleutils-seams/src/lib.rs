//! Seam declarations for the `backend-utils-adt-ruleutils` unit
//! (`utils/adt/ruleutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `quote_identifier(ident)` (ruleutils.c): double-quote the identifier
    /// if needed for re-parse safety (non-lowercase letters, keywords, ...).
    /// Allocates (C: palloc in the current context); `Err` carries OOM.
    pub fn quote_identifier(ident: &str) -> PgResult<String>
);
