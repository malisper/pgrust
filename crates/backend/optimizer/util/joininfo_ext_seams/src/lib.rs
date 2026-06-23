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
use nodes::primnodes::Expr;
use pathnodes::planner_run::PlannerRun;
use pathnodes::{PlannerInfo, RelId, Relids};

seam_core::seam!(
    /// `pull_varnos(root, (Node *) expr)` (var.c) over an arena `&Expr`. The
    /// input is only read, so its lifetime is independent (it may be a
    /// planner-run `'mcx` jointree node, not the `'static` arena).
    pub fn pull_varnos_expr<'a>(root: &PlannerInfo, expr: &Expr<'a>) -> Relids
);
seam_core::seam!(
    /// `pull_var_clause((Node *) expr, flags)` (var.c) over an arena `&Expr`,
    /// returning the collected Var/PlaceHolderVar/Aggref/etc. nodes as owned
    /// `Expr` values interned into the result context (`'static`). The input is
    /// only read, so its lifetime is independent of the returned values'.
    pub fn pull_var_clause_expr<'a>(expr: &Expr<'a>, flags: i32) -> Vec<Expr<'static>>
);
seam_core::seam!(
    /// `contain_leaked_vars((Node *) clause)` (clauses.c). Read-only: the input
    /// lifetime is independent.
    pub fn contain_leaked_vars<'a>(clause: &Expr<'a>) -> PgResult<bool>
);
seam_core::seam!(
    /// `exprType((Node *) expr)` (nodeFuncs.c) over an arena `&Expr`.
    pub fn expr_type(expr: &Expr<'static>) -> Oid
);
seam_core::seam!(
    /// `exprTypmod((Node *) expr)` (nodeFuncs.c) over an arena `&Expr`.
    pub fn expr_typmod(expr: &Expr<'static>) -> i32
);
seam_core::seam!(
    /// `cost_qual_eval_node(&cost, (Node *) expr, root)` (costsize.c) for a
    /// single expression; returns `(startup, per_tuple)`.
    pub fn cost_qual_eval_node_expr<'a>(root: &PlannerInfo, expr: &Expr<'a>) -> (f64, f64)
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
    ///
    /// Threads the planner-run resolver (`run`): the relnode.c body reads RTE
    /// fields through the re-signed `rte_*` seams that take `&PlannerRun<'mcx>`.
    pub fn find_base_rel_ignore_join<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &PlannerInfo,
        relid: i32,
    ) -> Option<RelId>
);
seam_core::seam!(
    /// `restriction_is_always_true(root, restrictinfo)` (initsplan.c). Takes the
    /// whole `RestrictInfo` (a `RinfoId`), not the bare clause, so the
    /// `has_clone`/`is_clone` guard and the `orclause` OR-recursion apply.
    pub fn restriction_is_always_true(root: &PlannerInfo, restrictinfo: pathnodes::RinfoId) -> bool
);
seam_core::seam!(
    /// `restriction_is_always_false(root, restrictinfo)` (initsplan.c). See
    /// [`restriction_is_always_true`].
    pub fn restriction_is_always_false(root: &PlannerInfo, restrictinfo: pathnodes::RinfoId) -> bool
);
seam_core::seam!(
    /// `add_vars_to_targetlist(root, vars, where_needed)` (var.c). `vars` is a
    /// list of Var/PlaceHolderVar nodes; `where_needed` the relids needing them.
    pub fn add_vars_to_targetlist(mcx: mcx::Mcx<'_>, root: &mut PlannerInfo, vars: Vec<Expr<'static>>, where_needed: Relids) -> PgResult<()>
);
seam_core::seam!(
    /// `add_vars_to_attr_needed(root, vars, where_needed)` (var.c).
    pub fn add_vars_to_attr_needed(mcx: mcx::Mcx<'_>, root: &mut PlannerInfo, vars: Vec<Expr<'static>>, where_needed: Relids) -> PgResult<()>
);
