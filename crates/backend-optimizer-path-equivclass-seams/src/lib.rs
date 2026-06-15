//! Seam declarations for `optimizer/path/equivclass.c` — the EquivalenceClass
//! engine. `equivclass.c` is **not yet ported**; the handful of entry points
//! that `optimizer/path/pathkeys.c` (and other planner leaves) reach across into
//! it are declared here, arena-shaped over [`types_pathnodes::PlannerInfo`]
//! (`EcId` handles + `NodeId` expression handles into `PlannerInfo::node_arena`,
//! resolved with `root.node(id) -> &Expr`).
//!
//! Each seam defaults to a loud panic until the owning crate (the future
//! `backend-optimizer-path-equivclass`) installs a real implementation at
//! single-threaded startup. Pathkeys' `make_pathkey_from_sortinfo`,
//! `initialize_mergeclause_eclasses`, and `convert_subquery_pathkeys` call
//! `get_eclass_for_sort_expr`/`canonicalize_ec_expression`; its
//! `pathkeys_useful_for_merging` calls `eclass_useful_for_merging`.

use types_core::primitive::{Index, Oid};
use types_nodes::primnodes::Expr;
use types_pathnodes::{EcId, EquivalenceClass, PlannerInfo, RelOptInfo, Relids};

seam_core::seam!(
    /// `canonicalize_ec_expression(expr, req_type, req_collation)`
    /// (equivclass.c) — wrap the expression in a `RelabelType`/`CollateExpr` as
    /// needed so it exposes the requested type and collation. Returns the
    /// (possibly wrapped) expression value.
    pub fn canonicalize_ec_expression(
        expr: &Expr,
        req_type: Oid,
        req_collation: Oid,
    ) -> Expr
);

seam_core::seam!(
    /// `get_eclass_for_sort_expr(root, expr, opfamilies, opcintype, collation,
    /// sortref, rel, create_it)` (equivclass.c) — find or (optionally) create
    /// the canonical `EquivalenceClass` for a sort/group expression. `expr` is
    /// the sort key. Returns the canonical `EcId`, or `None` when no match and
    /// `create_it` is false.
    pub fn get_eclass_for_sort_expr(
        root: &mut PlannerInfo,
        expr: &Expr,
        opfamilies: &[Oid],
        opcintype: Oid,
        collation: Oid,
        sortref: Index,
        rel: &Relids,
        create_it: bool,
    ) -> Option<EcId>
);

seam_core::seam!(
    /// `eclass_useful_for_merging(root, eclass, rel)` (equivclass.c) — does the
    /// EquivalenceClass have members not yet joined to `rel` (so a fresh
    /// mergejoin clause could be generated)?
    pub fn eclass_useful_for_merging(
        root: &PlannerInfo,
        eclass: &EquivalenceClass,
        rel: &RelOptInfo,
    ) -> bool
);
