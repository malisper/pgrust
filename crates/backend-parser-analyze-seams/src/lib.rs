//! Seam declarations for the `backend-parser-analyze` unit
//! (`parser/analyze.c` / `tcop/postgres.c`'s rewrite wrappers).
//!
//! Covers the COPY-(query)-TO driver's `pg_analyze_and_rewrite_fixedparams`
//! call and the `post_parse_analyze_hook` runner portalcmds invokes after
//! (re)jumbling the cursor query.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::copy_query::{Query as CopyQuery, RawStmt};
use types_nodes::portalcmds::{JumbleState, ParseState, Query};

seam_core::seam!(
    /// `pg_analyze_and_rewrite_fixedparams(parsetree, query_string, paramTypes,
    /// numParams, queryEnv)` (tcop/postgres.c): parse-analyze and rewrite a raw
    /// statement, returning the list of rewritten `Query`s (also acquiring the
    /// source tables' locks). COPY passes no parameters and a NULL query
    /// environment. The rewritten queries are allocated in `mcx`. `Err` carries
    /// any analysis/rewrite `ereport(ERROR)`.
    pub fn pg_analyze_and_rewrite_fixedparams<'mcx>(
        mcx: Mcx<'mcx>,
        parsetree: &RawStmt<'mcx>,
        query_string: &str,
    ) -> PgResult<PgVec<'mcx, CopyQuery<'mcx>>>
);

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
