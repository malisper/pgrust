//! Seam declarations for the `backend-parser-analyze` unit
//! (`parser/analyze.c`): the `post_parse_analyze_hook` runner portalcmds
//! invokes after (re)jumbling the cursor query.
//!
//! The hook is a per-backend function pointer extensions install (NULL by
//! default). Rather than expose the raw pointer, the owner runs it: a no-op
//! when unset (`if (post_parse_analyze_hook) ...`).

use types_error::PgResult;
use types_nodes::portalcmds::{JumbleState, ParseState, Query};

seam_core::seam!(
    /// `if (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query,
    /// jstate);` (the call site analyze.c owns the hook for). Runs extension
    /// code; can `ereport(ERROR)`. `jstate` is `None` when query-id is off.
    pub fn run_post_parse_analyze_hook(
        pstate: &ParseState,
        query: &Query,
        jstate: Option<&JumbleState>,
    ) -> PgResult<()>
);
