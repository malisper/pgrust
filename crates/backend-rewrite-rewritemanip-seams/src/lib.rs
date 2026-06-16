//! Seam declarations for the `backend-rewrite-rewriteManip` unit
//! (`rewrite/rewriteManip.c`, part of the unported `backend-rewrite-core` unit).
//!
//! These cross the cycle from the parser (`parse_agg.c`) into the rewriter,
//! which is not yet ported. The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly
//! (mirror-PG-and-panic).

#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::Node;

seam_core::seam!(
    /// `contain_windowfuncs(node)` (rewriteManip.c): does the node contain any
    /// window function? Infallible (a pure expression-tree walk).
    pub fn contain_windowfuncs(node: &Node<'_>) -> bool
);

seam_core::seam!(
    /// `locate_windowfunc(node)` (rewriteManip.c): the parse location of any
    /// window function in the node, or `-1`. Infallible.
    pub fn locate_windowfunc(node: &Node<'_>) -> i32
);

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
    /// `map_variable_attnos((Node *) exprs, 1, 0, attmap, InvalidOid,
    /// &found_whole_row)` (rewriteManip.c) over a list of index expressions, as
    /// `catalog/index.c` `CompareIndexInfo` calls it on `info2->ii_Expressions`
    /// / `info2->ii_Predicate`. `target_varno`=1, `sublevels_up`=0,
    /// `to_rowtype`=`InvalidOid` are pinned to that single call site. Returns the
    /// freshly-mapped expression list (allocated in `mcx`) and the
    /// `found_whole_row` out-parameter. `Err` carries the rewrite
    /// `ereport(ERROR)` surface. Owned by the (unported) `backend-rewrite-core`
    /// unit (`map_variable_attnos` is concrete there); installed from its
    /// `init_seams()` when it lands — until then a call panics loudly.
    pub fn map_variable_attnos_expr_list<'mcx>(
        mcx: Mcx<'mcx>,
        exprs: mcx::PgVec<'mcx, types_nodes::primnodes::Expr>,
        attmap: &[types_core::primitive::AttrNumber],
    ) -> PgResult<(mcx::PgVec<'mcx, types_nodes::primnodes::Expr>, bool)>
);
