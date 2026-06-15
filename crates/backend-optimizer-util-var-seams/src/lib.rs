//! Seam declarations for the `backend-optimizer-util-var` unit
//! (`optimizer/util/var.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `pull_varattnos(node, varno, &varattnos)` (var.c): collect, into the
    /// returned bitmapset, the attribute numbers (offset by
    /// `FirstLowInvalidHeapAttributeNumber`) of all `Var`s in `node` that
    /// reference range-table entry `varno`. The C accumulates into a caller
    /// `Bitmapset *` initialized to `NULL`; the owned model returns the
    /// resulting set (allocated in `mcx`). `None` is the C empty/NULL set.
    /// Walking the tree can `ereport(ERROR)` on an unexpected node.
    pub fn pull_varattnos<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &types_nodes::primnodes::Expr,
        varno: u32,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `contain_var_clause(node)` (var.c): recursively scan the expression and
    /// return whether it contains any `Var` of the current query level
    /// (`varlevelsup == 0`). A pure structural predicate (the C body never
    /// `ereport`s), so infallible. Used by `clauses.c`'s `contain_leaked_vars`
    /// / `is_pseudo_constant_clause` to decide whether a clause references any
    /// relation column.
    pub fn contain_var_clause(node: &types_nodes::primnodes::Expr) -> bool
);

seam_core::seam!(
    /// `pull_varnos(root, node)` (var.c): collect the set of relids (range-table
    /// indexes) of all `Var`s / `PlaceHolderVar`s of the current query level
    /// referenced in `node`, returned as a relids bitmapset allocated in `mcx`
    /// (C's `Relids` = `Bitmapset *`; `None` = the empty set). The C call takes
    /// `PlannerInfo *root` only for the `varnullingrels` cross-check; the safe
    /// model threads no `PlannerInfo` (matching a `root == NULL` call, which
    /// `pull_varnos` accepts). `Err` carries the walk-time `ereport(ERROR)`.
    pub fn pull_varnos<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &types_nodes::primnodes::Expr,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `NumRelids(root, clause)` (clauses.c): the number of distinct base
    /// relations referenced by `clause`, i.e.
    /// `bms_num_members(bms_difference(pull_varnos(root, clause),
    /// root->outer_join_rels))`. Subtracting `root->outer_join_rels` requires a
    /// live `PlannerInfo`, which the lifetime-free `Expr` model does not thread,
    /// so the whole routine rides this var/planner-owned seam (the `pull_varnos`
    /// + `outer_join_rels` pair is inseparable from `root`). `Err` carries the
    /// walk-time `ereport(ERROR)`.
    pub fn num_relids(node: &types_nodes::primnodes::Expr) -> types_error::PgResult<i32>
);
