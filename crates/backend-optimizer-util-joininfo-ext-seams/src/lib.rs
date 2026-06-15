//! Outward seam declarations for the not-yet-ported (from this unit's view)
//! externals that `optimizer/util/{restrictinfo,placeholder,joininfo}.c` and
//! `optimizer/plan/orclauses.c` call: the node operators over the arena's
//! lifetime-free `&Expr`/`NodeId` (`pull_varnos`/`pull_var_clause`/
//! `contain_leaked_vars`/`exprType`/`exprTypmod`/`cost_qual_eval_node`), the
//! width clamp (`clamp_width_est`, costsize.c), the relnode lookup
//! `find_base_rel_ignore_join`, the initsplan always-true/false probes, and the
//! var.c targetlist/attr-needed adders.
//!
//! These belong to several owners (var.c, clauses.c, nodeFuncs.c, costsize.c,
//! relnode.c, initsplan.c) that are not ported yet in a form callable across
//! this unit's dependency cycle. They are homed here, in a single consumer-side
//! seam crate with NO owner directory, so each call panics loudly until the real
//! owner lands ("mirror PG and panic"); the owners install their own once
//! ported (the `every_declared_seam_is_installed_by_its_owner` guard skips this
//! crate because no `backend-optimizer-util-joininfo-ext` owner directory
//! exists).

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::{PlannerInfo, RelId, Relids};

seam_core::seam!(
    /// `pull_varnos(root, (Node *) expr)` (var.c) over an arena `&Expr`.
    pub fn pull_varnos_expr(root: &PlannerInfo, expr: &Expr) -> Relids
);
seam_core::seam!(
    /// `pull_var_clause((Node *) expr, flags)` (var.c) over an arena `&Expr`,
    /// returning the collected Var/PlaceHolderVar/Aggref/etc. nodes as owned
    /// `Expr` values.
    pub fn pull_var_clause_expr(expr: &Expr, flags: i32) -> Vec<Expr>
);
seam_core::seam!(
    /// `contain_leaked_vars((Node *) clause)` (clauses.c).
    pub fn contain_leaked_vars(clause: &Expr) -> PgResult<bool>
);
seam_core::seam!(
    /// `exprType((Node *) expr)` (nodeFuncs.c) over an arena `&Expr`.
    pub fn expr_type(expr: &Expr) -> Oid
);
seam_core::seam!(
    /// `exprTypmod((Node *) expr)` (nodeFuncs.c) over an arena `&Expr`.
    pub fn expr_typmod(expr: &Expr) -> i32
);
seam_core::seam!(
    /// `cost_qual_eval_node(&cost, (Node *) expr, root)` (costsize.c) for a
    /// single expression; returns `(startup, per_tuple)`.
    pub fn cost_qual_eval_node_expr(root: &PlannerInfo, expr: &Expr) -> (f64, f64)
);
seam_core::seam!(
    /// `clamp_width_est(tuple_width)` (costsize.c): clamp a 64-bit width estimate
    /// into the valid `int32` range.
    pub fn clamp_width_est(tuple_width: i64) -> i32
);
seam_core::seam!(
    /// `find_base_rel_ignore_join(root, relid)` (relnode.c): the base
    /// `RelOptInfo` for an RT index, or `None` if the slot is empty / the relid
    /// names a join rel rather than a base rel.
    pub fn find_base_rel_ignore_join(root: &PlannerInfo, relid: i32) -> Option<RelId>
);
seam_core::seam!(
    /// `restriction_is_always_true(root, restrictinfo)` (initsplan.c).
    pub fn restriction_is_always_true(root: &PlannerInfo, clause: &Expr) -> bool
);
seam_core::seam!(
    /// `restriction_is_always_false(root, restrictinfo)` (initsplan.c).
    pub fn restriction_is_always_false(root: &PlannerInfo, clause: &Expr) -> bool
);
seam_core::seam!(
    /// `add_vars_to_targetlist(root, vars, where_needed)` (var.c). `vars` is a
    /// list of Var/PlaceHolderVar nodes; `where_needed` the relids needing them.
    pub fn add_vars_to_targetlist(root: &mut PlannerInfo, vars: Vec<Expr>, where_needed: Relids) -> PgResult<()>
);
seam_core::seam!(
    /// `add_vars_to_attr_needed(root, vars, where_needed)` (var.c).
    pub fn add_vars_to_attr_needed(root: &mut PlannerInfo, vars: Vec<Expr>, where_needed: Relids) -> PgResult<()>
);
seam_core::seam!(
    /// `find_placeholders_in_jointree(root)` (placeholder.c) — the jointree walk
    /// over `root->parse->jointree`. The Query/jointree is not reachable in the
    /// arena model (`PlannerInfo::parse` is an opaque `QueryId`), so this is
    /// homed as a panic until the parse-tree-aware owner can drive it.
    pub fn find_placeholders_in_jointree_walk(root: &mut PlannerInfo) -> PgResult<()>
);
