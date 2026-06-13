//! Seam declarations for the parse-analysis entry points
//! (`parser/analyze.c` / `tcop/postgres.c`'s rewrite wrappers) the COPY-(query)
//! -TO driver calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::copy_query::{Query, RawStmt};

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
    ) -> PgResult<PgVec<'mcx, Query<'mcx>>>
);
