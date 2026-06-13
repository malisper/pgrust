//! Seam declarations for the `backend-utils-adt-ruleutils` unit
//! (`utils/adt/ruleutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_error::PgResult;

seam_core::seam!(
    /// `quote_identifier(ident)` (ruleutils.c): double-quote the identifier
    /// if needed for re-parse safety (non-lowercase letters, keywords, ...).
    /// The result is copied into `mcx` (C pallocs the quoted form in the
    /// current context; the unquoted case returns the input pointer — the
    /// owned image copies either way). `Err` carries OOM.
    pub fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<PgString<'mcx>>
);
