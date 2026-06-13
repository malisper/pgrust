//! plancache's slice of the rewriter (`rewrite/rewriteHandler.c`). The owning
//! unit installs these; until then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_plancache::{AnalyzedQueryHandle, PostRewriteHandle, QueryListHandle};

seam_core::seam!(
    /// `AcquireRewriteLocks(query, forExecute=true, forUpdatePushedDown=false)`.
    pub fn acquire_rewrite_locks(q: AnalyzedQueryHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `pg_rewrite_query(query)`.
    pub fn rewrite_query(q: AnalyzedQueryHandle) -> PgResult<QueryListHandle>
);

seam_core::seam!(
    /// Invoke the caller-supplied `postRewrite(querytree_list, postRewriteArg)`.
    pub fn invoke_post_rewrite(hook: PostRewriteHandle, querytree_list: QueryListHandle) -> PgResult<()>
);
