//! Seam declarations for `rewrite/rewriteManip.c`.
//!
//! These cross the cycle from the parser (`parse_agg.c`) into the rewriter,
//! which is not yet ported. The owning unit installs them from its
//! `init_seams()` when it lands; until then a call panics loudly.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::Node;

seam_core::seam!(
    /// `flatten_join_alias_vars(NULL, query, node)` (rewriteManip.c) — replace
    /// any join-alias Vars in `node` with the underlying base-relation Vars,
    /// using `query`'s range table for the join definitions. The PlannerInfo*
    /// first argument is always NULL at this call site, so it is omitted. The
    /// result is freshly allocated in `mcx`.
    pub fn flatten_join_alias_vars<'mcx>(
        mcx: Mcx<'mcx>,
        query: &Node<'mcx>,
        node: Node<'mcx>,
    ) -> PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `locate_agg_of_level(node, levelsup)` (rewriteManip.c) — find the parse
    /// location of the first aggregate of exactly the given query level in
    /// `node`'s tree, or -1 if none.
    pub fn locate_agg_of_level(node: &Node<'_>, levelsup: i32) -> i32
);

seam_core::seam!(
    /// `locate_windowfunc(node)` (rewriteManip.c) — find the parse location of
    /// the first window function in `node`'s tree, or -1 if none.
    pub fn locate_windowfunc(node: &Node<'_>) -> i32
);

seam_core::seam!(
    /// `contain_windowfuncs(node)` (rewriteManip.c) — true if `node`'s tree
    /// contains any window function call.
    pub fn contain_windowfuncs(node: &Node<'_>) -> bool
);
