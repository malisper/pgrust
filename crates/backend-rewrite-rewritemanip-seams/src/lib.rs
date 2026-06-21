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
    /// `contain_aggs_of_level(node, levelsup)` (rewriteManip.c): does the node
    /// contain any aggregate of the specified query level? Infallible (a pure
    /// expression-tree walk).
    pub fn contain_aggs_of_level(node: &Node<'_>, levelsup: i32) -> bool
);

seam_core::seam!(
    /// `contain_windowfuncs(node)` (rewriteManip.c): does the node contain any
    /// window function? Infallible (a pure expression-tree walk).
    pub fn contain_windowfuncs(node: &Node<'_>) -> bool
);

seam_core::seam!(
    /// `checkExprHasSubLink(node)` (rewriteManip.c): does the node contain a
    /// SubLink? `RelationBuildRowSecurity` (commands/policy.c) uses it to cache
    /// `RowSecurityPolicy.hassublinks`. Infallible (a pure expression-tree walk).
    pub fn check_expr_has_sub_link(node: &Node<'_>) -> bool
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

seam_core::seam!(
    /// `map_variable_attnos((Node *) returningList, firstVarno, 0, attmap,
    /// RelationGetForm(partrel)->reltype, &found_whole_row)` (rewriteManip.c:1701)
    /// over a `List *` of `TargetEntry`, as `execPartition.c`
    /// `ExecInitPartitionInfo` calls it on the first plan's RETURNING list to
    /// translate the Vars to the leaf partition's attribute numbers. In C the
    /// `T_List` mutator arm recurses into each `TargetEntry`'s `expr`; over the
    /// owned model the input `Vec<TargetEntry>` is consumed and returned with each
    /// element's `expr` mapped in place, OR-ing `found_whole_row` across the list
    /// (the caller ignores it). `Err` carries the rewrite `ereport(ERROR)` surface.
    pub fn map_variable_attnos_targetentry_list<'mcx>(
        mcx: Mcx<'mcx>,
        tlist: mcx::PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
        target_varno: i32,
        attmap: &[types_core::primitive::AttrNumber],
        to_rowtype: types_core::primitive::Oid,
    ) -> PgResult<(mcx::PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>, bool)>
);

seam_core::seam!(
    /// `map_variable_attnos(node, target_varno, sublevels_up, attmap, to_rowtype,
    /// &found_whole_row)` (rewriteManip.c:1701) over a single `Node *`, as
    /// `commands/tablecmds.c` `MergeAttributes` calls it on inherited default and
    /// CHECK-constraint expressions. The owned model consumes the node, mutates it
    /// in place, and returns it together with the `found_whole_row` out-parameter.
    /// `Err` carries the rewrite `ereport(ERROR)` surface. Owned by the
    /// `backend-rewrite-core` unit; installed from its `init_seams()`.
    pub fn map_variable_attnos_node<'mcx>(
        mcx: Mcx<'mcx>,
        node: types_nodes::nodes::NodePtr<'mcx>,
        target_varno: i32,
        sublevels_up: i32,
        attmap: &[types_core::primitive::AttrNumber],
        to_rowtype: types_core::primitive::Oid,
    ) -> PgResult<(types_nodes::nodes::NodePtr<'mcx>, bool)>
);
