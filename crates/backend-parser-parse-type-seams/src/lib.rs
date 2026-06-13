//! Seam declarations for the `backend-parser-parse-type` unit
//! (`parser/parse_type.c`): name-list rendering.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_error::PgResult;

seam_core::seam!(
    /// `NameListToString(names)` (parse_type.c): render a possibly-qualified
    /// name (`List *` of `String`/`A_Star` nodes, here the name components)
    /// into a dotted string, allocated in `mcx` (C: `StringInfo` in the
    /// current context).
    pub fn name_list_to_string<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[PgString<'_>],
    ) -> PgResult<PgString<'mcx>>
);
