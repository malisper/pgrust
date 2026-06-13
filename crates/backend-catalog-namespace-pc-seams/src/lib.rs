//! plancache's slice of the search-path matcher (`catalog/namespace.c`).
//! `backend-catalog-namespace` is ported; it installs these. (The matcher is
//! held by an opaque `SearchPathMatcherHandle` because its storage lives in
//! the plancache source's long-lived context, reached via the mctx seam.)

use types_error::PgResult;
use types_plancache::{CtxId, SearchPathMatcherHandle};

seam_core::seam!(
    /// `GetSearchPathMatcher(context)`.
    pub fn get_search_path_matcher(context: CtxId) -> PgResult<SearchPathMatcherHandle>
);

seam_core::seam!(
    /// `SearchPathMatchesCurrentEnvironment(matcher)`.
    pub fn search_path_matches_current_environment(matcher: SearchPathMatcherHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `CopySearchPathMatcher(matcher)` in the current context.
    pub fn copy_search_path_matcher(matcher: SearchPathMatcherHandle) -> PgResult<SearchPathMatcherHandle>
);
