//! Outward seam declarations for the not-yet-ported externals that
//! `optimizer/path/equivclass.c` calls — the node operators
//! (`equal`/`exprType`/`exprCollation`/`exprTypmod`/`applyRelabelType`/
//! `pull_var_clause`/`pull_varnos`/`contain_*`/`expression_returns_set`/
//! `is_parallel_safe`/`remove_nulling_relids`) and the
//! initsplan/restrictinfo/appendinfo/makefuncs clause machinery
//! (`make_restrictinfo`/`build_implied_join_equality`/`process_implied_equality`/
//! `distribute_restrictinfo_to_rels`/`add_vars_to_targetlist`/
//! `add_vars_to_attr_needed`/`add_outer_joins_to_relids`/`find_childrel_parents`/
//! `adjust_appendrel_attrs{,_multilevel}`/`makeBoolConst`).
//!
//! These belong to several distinct owners (nodeFuncs.c, equalfuncs.c, var.c,
//! clauses.c, initsplan.c, restrictinfo.c, appendinfo.c, relnode.c,
//! makefuncs.c) that are not ported yet. They are homed here, in a single
//! consumer-side seam crate with NO owner directory, so each call panics loudly
//! until the real owner lands ("mirror PG and panic"); the owning crates will
//! install their own once ported (the `every_declared_seam_is_installed_by_its_
//! owner` guard skips this crate because no `backend-optimizer-path-equivclass-
//! ext` owner directory exists).

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{Index, Oid};
use types_error::PgResult;
use types_nodes::primnodes::{CoercionForm, Expr};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo};


/* ---- nodeFuncs.c / equalfuncs.c node operators -------------------- */

seam_core::seam!(
    /// `equal(a, b)` (equalfuncs.c) — structural node equality.
    pub fn equal(a: &Expr, b: &Expr) -> bool
);
seam_core::seam!(
    /// `exprType((Node *) expr)` (nodeFuncs.c).
    pub fn expr_type(expr: &Expr) -> Oid
);
seam_core::seam!(
    /// `exprTypmod((Node *) expr)` (nodeFuncs.c).
    pub fn expr_typmod(expr: &Expr) -> i32
);
seam_core::seam!(
    /// `exprCollation((Node *) expr)` (nodeFuncs.c).
    pub fn expr_collation(expr: &Expr) -> Oid
);
seam_core::seam!(
    /// `applyRelabelType(arg, rtype, rtypmod, rcollid, rformat, rlocation,
    /// overwrite_ok)` (nodeFuncs.c) — wraps `arg` in a `RelabelType` (or
    /// rewrites an existing one), preserving const-flatness.
    pub fn apply_relabel_type(
        arg: Expr,
        rtype: Oid,
        rtypmod: i32,
        rcollid: Oid,
        rformat: CoercionForm,
        rlocation: i32,
        overwrite_ok: bool,
    ) -> PgResult<Expr>
);

/* ---- clauses.c / var.c expression analysis ------------------------ */

seam_core::seam!(
    /// `contain_volatile_functions((Node *) expr)` (clauses.c).
    pub fn contain_volatile_functions(expr: &Expr) -> bool
);
seam_core::seam!(
    /// `expression_returns_set((Node *) expr)` (nodeFuncs.c).
    pub fn expression_returns_set(expr: &Expr) -> bool
);
seam_core::seam!(
    /// `contain_agg_clause((Node *) expr)` (clauses.c).
    pub fn contain_agg_clause(expr: &Expr) -> bool
);
seam_core::seam!(
    /// `contain_window_function((Node *) expr)` (clauses.c).
    pub fn contain_window_function(expr: &Expr) -> bool
);
seam_core::seam!(
    /// `is_parallel_safe(root, (Node *) expr)` (clauses.c).
    pub fn is_parallel_safe(root: &PlannerInfo, expr: &Expr) -> bool
);
seam_core::seam!(
    /// `pull_var_clause((Node *) node, flags)` (var.c) — the Vars/quasi-Vars in
    /// `node`, per the `PVC_*` flags.
    pub fn pull_var_clause(node: &Expr, flags: i32) -> Vec<Expr>
);
seam_core::seam!(
    /// `pull_var_clause` over a list of expressions (the C
    /// `pull_var_clause((Node *) exprs, ...)` over a `List`).
    pub fn pull_var_clause_list(nodes: Vec<Expr>, flags: i32) -> Vec<Expr>
);
seam_core::seam!(
    /// `pull_varnos(root, (Node *) expr)` (var.c) — the relids referenced in
    /// `expr`.
    pub fn pull_varnos(root: &PlannerInfo, expr: &Expr) -> Relids
);
seam_core::seam!(
    /// `remove_nulling_relids((Node *) node, removable, except)` (var.c).
    pub fn remove_nulling_relids(node: Expr, removable: Relids, except: Relids) -> Expr
);

/* ---- appendrel attr translation (appendinfo.c) -------------------- */

seam_core::seam!(
    /// `adjust_appendrel_attrs(root, (Node *) node, nappinfos, appinfos)`
    /// (appendinfo.c) — single-level parent→child Var translation. `run` is
    /// threaded so the UNION-ALL whole-row→`RowExpr` branch can fetch the parent
    /// RTE's `eref->colnames` via `planner_rt_fetch(run, root, parent_relid)`.
    pub fn adjust_appendrel_attrs<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        node: Expr,
        appinfos: Vec<RelId>,
    ) -> PgResult<Expr>
);
seam_core::seam!(
    /// `adjust_appendrel_attrs_multilevel(root, (Node *) node, child_rel,
    /// top_parent)` (appendinfo.c) — multi-level Var translation.
    pub fn adjust_appendrel_attrs_multilevel(
        root: &mut PlannerInfo,
        node: Expr,
        child_rel: RelId,
        top_parent: Option<RelId>,
    ) -> PgResult<Expr>
);
seam_core::seam!(
    /// `find_childrel_parents(root, rel)` (relnode.c) — the topmost-parent
    /// relids of an "other member" rel.
    pub fn find_childrel_parents(root: &PlannerInfo, rel: RelId) -> Relids
);

/* ---- initsplan.c / restrictinfo.c clause machinery ---------------- */

seam_core::seam!(
    /// `make_restrictinfo(root, clause, is_pushed_down, has_clone, is_clone,
    /// pseudoconstant, security_level, required_relids, incompatible_relids,
    /// outer_relids)` (restrictinfo.c) — build a RestrictInfo, returning its
    /// arena handle.
    pub fn make_restrictinfo(
        mcx: mcx::Mcx<'_>,
        root: &mut PlannerInfo,
        clause: Expr,
        is_pushed_down: bool,
        has_clone: bool,
        is_clone: bool,
        pseudoconstant: bool,
        security_level: Index,
        required_relids: Relids,
        incompatible_relids: Relids,
        outer_relids: Relids,
    ) -> PgResult<RinfoId>
);
seam_core::seam!(
    /// `build_implied_join_equality(root, opno, collation, item1, item2,
    /// qualscope, security_level)` (initsplan.c:3455) — build a derived
    /// mergejoinable equality RestrictInfo, returning its arena handle.
    pub fn build_implied_join_equality<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        opno: Oid,
        collation: Oid,
        item1: Expr,
        item2: Expr,
        qualscope: Relids,
        security_level: Index,
    ) -> PgResult<RinfoId>
);
seam_core::seam!(
    /// `process_implied_equality(root, opno, collation, item1, item2,
    /// qualscope, security_level, both_const)` (initsplan.c) — build and
    /// distribute a derived equality; returns the new RestrictInfo handle, or
    /// `None` if the clause degenerated to a constant.
    pub fn process_implied_equality<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        opno: Oid,
        collation: Oid,
        item1: Expr,
        item2: Expr,
        qualscope: Relids,
        security_level: Index,
        both_const: bool,
    ) -> PgResult<Option<RinfoId>>
);
seam_core::seam!(
    /// `distribute_restrictinfo_to_rels(root, restrictinfo)` (initsplan.c).
    pub fn distribute_restrictinfo_to_rels<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        restrictinfo: RinfoId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `add_vars_to_targetlist(root, vars, where_needed)` (initsplan.c).
    pub fn add_vars_to_targetlist(
        root: &mut PlannerInfo,
        vars: Vec<Expr>,
        where_needed: Relids,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `add_vars_to_attr_needed(root, vars, where_needed)` (initsplan.c).
    pub fn add_vars_to_attr_needed(
        root: &mut PlannerInfo,
        vars: Vec<Expr>,
        where_needed: Relids,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `add_outer_joins_to_relids(root, input_relids, sjinfo, &nulledrels)`
    /// (initsplan.c) — add the OJ relids implied by joining over `input_relids`.
    pub fn add_outer_joins_to_relids(
        root: &PlannerInfo,
        input_relids: Relids,
        sjinfo: Option<SpecialJoinInfo>,
    ) -> Relids
);

/* ---- node makers (makefuncs.c) ----------------------------------- */

seam_core::seam!(
    /// `makeBoolConst(value, isnull)` (makefuncs.c) — a boolean `Const`.
    pub fn make_bool_const(value: bool, isnull: bool) -> Expr
);
seam_core::seam!(
    /// Build the `X IS NOT NULL` `NullTest` over `arg` (the conversion in
    /// `process_equivalence`; `argisrow=false`, `location=-1`).
    pub fn make_is_not_null(arg: Expr) -> Expr
);

/* ---- TargetEntry inspection (for add_setop_child_rel_equivalences) - */

seam_core::seam!(
    /// Read `tle->resjunk` for a TargetEntry node handle.
    pub fn target_entry_resjunk(root: &PlannerInfo, tle: NodeId) -> bool
);
seam_core::seam!(
    /// Read `tle->expr` for a TargetEntry node handle (as a value `Expr`).
    pub fn target_entry_expr(root: &PlannerInfo, tle: NodeId) -> Expr
);
