//! Seam declarations for `optimizer/util/placeholder.c` — the value-typed entry
//! points consumed by the parse-tree-aware planner driver (notably
//! `prepjointree.c`'s pull-up code, which calls `make_placeholder_expr`).
//!
//! The placeholder.c logic itself is ported in
//! `backend-optimizer-util-joininfo` (task #177); that crate is the OWNER and
//! installs these seams from its `init_seams()`. These two entries take/return
//! real owned node VALUES (`Expr` / `PlaceHolderVar` / `Relids`) over the
//! lifetime-free `PlannerInfo` — the `PhInfoId` arena already exists. This is
//! distinct from the `joinpath-seams::find_placeholder_info` declaration, which
//! is the `NodeId`-handle dispatch form consumed by joinpath/relnode; here
//! `prepjointree` works with the owned `PlaceHolderVar` it just constructed.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `make_placeholder_expr(root, expr, phrels)` (placeholder.c): wrap `expr`
    /// in a fresh `PlaceHolderVar` carrying a newly-assigned `phid`
    /// (`root->glob->lastPHId`). `phrels` is the syntactic location to attribute
    /// to the expression; the caller fixes up `phlevelsup` / `phnullingrels`.
    /// Touches only `root->glob`.
    pub fn make_placeholder_expr(
        root: &mut types_pathnodes::PlannerInfo,
        expr: types_nodes::primnodes::Expr<'static>,
        phrels: types_pathnodes::Relids,
    ) -> types_nodes::primnodes::PlaceHolderVar<'static>
);

seam_core::seam!(
    /// `find_placeholder_info(root, phv)` (placeholder.c): fetch — or, if
    /// missing, create — the `PlaceHolderInfo` for the given `PlaceHolderVar`,
    /// returning its `PhInfoId`. `Err` carries the `elog(ERROR, "too late to
    /// create a new PlaceHolderInfo")` surface (after the PHI set is frozen) and
    /// the `get_typavgwidth` catalog-lookup failure surface.
    pub fn find_placeholder_info(
        root: &mut types_pathnodes::PlannerInfo,
        phv: &types_nodes::primnodes::PlaceHolderVar<'static>,
    ) -> types_error::PgResult<types_pathnodes::PhInfoId>
);
