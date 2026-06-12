//! Seam declarations for the `backend-utils-adt-ruleutils` unit
//! (`utils/adt/ruleutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_error::PgResult;

seam_core::seam!(
    /// `quote_qualified_identifier(qualifier, ident)` (ruleutils.c): each
    /// part quoted with `quote_identifier` if needed, joined with a dot,
    /// allocated in `mcx` (C: palloc in the current context). `Err` is OOM.
    pub fn quote_qualified_identifier<'mcx>(
        mcx: Mcx<'mcx>,
        qualifier: Option<&str>,
        ident: &str,
    ) -> PgResult<PgString<'mcx>>
);
