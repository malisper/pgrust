//! Seam declarations for the `backend-optimizer-path-small` unit
//! (`optimizer/path/clausesel.c` + `optimizer/path/tidpath.c`).
//!
//! This crate bundles BOTH:
//!
//!   * the unit's **inward** seam — [`clauselist_selectivity`], the selectivity
//!     entry point consumed by costsize.c and the rest of the planner. The
//!     owning crate (`backend-optimizer-path-small`) installs it from its
//!     `init_seams()`; until then a call panics loudly.
//!
//!   * the unit's **outward** seams — the cross-subsystem helpers clausesel.c /
//!     tidpath.c reach for whose owners are not yet ported (selfuncs.c
//!     per-clause estimators, plancat.c restriction/join/function selectivity,
//!     the extended-statistics estimator, the restrictinfo.c predicates,
//!     equivclass.c implied-equality generation, and the `&Expr`-shaped
//!     `pull_varnos`/`contain_volatile_functions` variants the existing
//!     joinpath-seams `NodeId` forms cannot serve). Each defaults to a loud
//!     panic until its owner lands and installs it (mirror-pg-and-panic).
//!
//! Outward helpers already declared by sibling `-seams` crates are referenced
//! there, not re-declared here:
//!   * `get_oprrest`                          -> backend-utils-cache-lsyscache-seams
//!   * `create_tidscan_path`/`create_tidrangescan_path`/`add_path`/`relids_*`
//!                                            -> backend-optimizer-util-pathnode-seams
//!   * `contain_volatile_functions_node`/`pull_varnos`(NodeId)
//!                                            -> backend-optimizer-path-joinpath-seams
//!   * `relids_is_member`/`relids_is_empty`/`relids_copy`
//!                                            -> backend-optimizer-util-relnode-seams

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use ::nodes::primnodes::Expr;
use pathnodes::planner_run::PlannerRun;
use pathnodes::{
    EcId, EmId, JoinType, NodeId, RelId, Relids, RinfoId, PlannerInfo, SpecialJoinInfo,
};

/* ======================================================================
 * INWARD seam — installed by backend-optimizer-path-small itself.
 * ==================================================================== */

seam_core::seam!(
    /// `clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo)`
    /// (clausesel.c): selectivity of an implicitly-ANDed `RestrictInfo`/clause
    /// list. The arena form takes the clause list as `RinfoId` handles.
    pub fn clauselist_selectivity<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clauses: &[RinfoId],
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);

/// An element of a `clauselist_selectivity` clause `List` that mixes
/// `RestrictInfo *` elements with bare `Expr *` elements (the C
/// `rinfo == NULL` elements). `selfuncs.c`'s `add_predicate_to_index_quals`
/// builds exactly such a mixed list (the index-bound RestrictInfos plus the
/// partial-index predicate, which is a bare `Node *`).
#[derive(Clone)]
pub enum ClauseListEntry {
    /// A `RestrictInfo` element (an index-qual RestrictInfo handle).
    Rinfo(RinfoId),
    /// A bare `Expr *` element (a partial-index predicate clause).
    Bare(Expr<'static>),
}

seam_core::seam!(
    /// `clauselist_selectivity_ext(root, clauses, varRelid, jointype, sjinfo,
    /// use_extended_stats)` (clausesel.c) — the explicit-`use_extended_stats`
    /// form. `dependencies_clauselist_selectivity`'s per-attribute simple
    /// selectivity (`clauselist_apply_dependencies`) calls this with
    /// `use_extended_stats=false` so it cannot recursively re-enter extended
    /// statistics while computing the base/simple selectivities it feeds back
    /// into the dependency combination.
    pub fn clauselist_selectivity_ext<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clauses: &[RinfoId],
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
        use_extended_stats: bool,
    ) -> PgResult<f64>
);

seam_core::seam!(
    /// `clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo)`
    /// (clausesel.c) — the **mixed-list** form, where `clauses` may contain
    /// both `RestrictInfo` handles and bare `Expr` predicate elements (the C
    /// `selectivityQuals` that `add_predicate_to_index_quals` returns).
    pub fn clauselist_selectivity_mixed<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clauses: &[ClauseListEntry],
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    /// `clause_selectivity(root, (Node *) clause, varRelid, jointype, sjinfo)`
    /// (clausesel.c): selectivity of a single `RestrictInfo`, identified by its
    /// [`RinfoId`] handle. (orclauses.c estimates its extracted OR clause and the
    /// original join OR clause through this.)
    pub fn clause_selectivity<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clause: RinfoId,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    /// `clause_selectivity(root, (Node *) clause, varRelid, jointype, sjinfo)`
    /// (clausesel.c) — the **bare expression** form (C `rinfo == NULL`), used by
    /// `selfuncs.c`'s `booltestsel` when no variable statistics are available
    /// (it asks `clause_selectivity` to estimate the raw boolean argument). The
    /// owning crate (`backend-optimizer-path-small`) installs it from its
    /// `init_seams()` (running `clause_selectivity_ext` with a `ClauseRef::Bare`).
    pub fn clause_selectivity_node<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clause: &Expr<'a>,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);

/* ======================================================================
 * statistics/extstats.c — extended-statistics estimator (unported owner).
 * ==================================================================== */

seam_core::seam!(
    /// `statext_clauselist_selectivity(root, clauses, varRelid, jointype,
    /// sjinfo, rel, &estimatedclauses, is_or)` (extended_stats.c): apply
    /// extended statistics across as many clauses as possible. Returns the
    /// partial selectivity and writes back, via the returned `Relids`, the
    /// 0-based clause-position set it consumed (the C `*estimatedclauses`
    /// in/out parameter is folded into the return tuple).
    ///
    /// `clauses` is the C `List *clauses` of `Node *` — each [`NodeId`] resolves
    /// (via `root.node`) to either an `Expr::RestrictInfo` or a bare BoolExpr-AND
    /// clause (the restrictinfo machinery doesn't wrap RestrictInfos on top of
    /// AND clauses), exactly as `statext_is_compatible_clause` dispatches.
    pub fn statext_clauselist_selectivity<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clauses: &[NodeId],
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
        rel: RelId,
        estimatedclauses: &Relids,
        is_or: bool,
    ) -> PgResult<(f64, Relids)>
);

/* ======================================================================
 * utils/adt/selfuncs.c — per-clause-node selectivity estimators (unported).
 * ==================================================================== */

seam_core::seam!(
    /// `restriction_selectivity(root, operatorid, args, inputcollid, varRelid)`
    /// (plancat.c): dispatch to the operator's restriction estimator.
    pub fn restriction_selectivity<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        operatorid: Oid,
        args: &[Expr<'a>],
        inputcollid: Oid,
        var_relid: i32,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `join_selectivity(root, operatorid, args, inputcollid, jointype, sjinfo)`
    /// (plancat.c): dispatch to the operator's join estimator.
    pub fn join_selectivity<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        operatorid: Oid,
        args: &[Expr<'a>],
        inputcollid: Oid,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `function_selectivity(root, funcid, args, inputcollid, is_join,
    /// varRelid, jointype, sjinfo)` (plancat.c): support-function estimate.
    pub fn function_selectivity<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        funcid: Oid,
        args: &[Expr<'a>],
        inputcollid: Oid,
        is_join: bool,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `scalararraysel(root, clause, is_join_clause, varRelid, jointype, sjinfo)`
    /// (selfuncs.c).
    pub fn scalararraysel<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clause: &Expr<'a>,
        is_join_clause: bool,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `rowcomparesel(root, clause, varRelid, jointype, sjinfo)` (selfuncs.c).
    pub fn rowcomparesel<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clause: &Expr<'a>,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `nulltestsel(root, nulltesttype, arg, varRelid, jointype, sjinfo)`
    /// (selfuncs.c). `arg` is the tested expression by value.
    pub fn nulltestsel<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        nulltesttype: i32,
        arg: &Expr<'a>,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `nulltestsel(root, IS_NULL, var, varRelid, jointype, sjinfo)` form used
    /// by `clauselist_selectivity` for the range-pair null adjustment, where the
    /// tested `var` is the common range variable (an `&Expr`).
    pub fn nulltestsel_var<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        nulltesttype: i32,
        var: &Expr<'a>,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `booltestsel(root, booltesttype, arg, varRelid, jointype, sjinfo)`
    /// (selfuncs.c).
    pub fn booltestsel<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        booltesttype: i32,
        arg: &Expr<'a>,
        var_relid: i32,
        jointype: JoinType,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `boolvarsel(root, arg, varRelid)` (selfuncs.c): selectivity of a boolean
    /// expression treated as a variable.
    pub fn boolvarsel<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        arg: &Expr<'a>,
        var_relid: i32,
    ) -> PgResult<f64>
);

/* ======================================================================
 * optimizer/util/clauses.c — analysis helpers (unported owner).
 * ==================================================================== */

seam_core::seam!(
    /// `NumRelids(root, clause)` (clauses.c): count of distinct rels in `clause`.
    pub fn num_relids<'a>(root: &mut PlannerInfo, clause: &Expr<'a>) -> PgResult<i32>
);
seam_core::seam!(
    /// `is_pseudo_constant_clause(clause)` (clauses.c).
    pub fn is_pseudo_constant_clause<'a>(clause: &Expr<'a>) -> PgResult<bool>
);
seam_core::seam!(
    /// `is_pseudo_constant_clause_relids(clause, relids)` (clauses.c).
    pub fn is_pseudo_constant_clause_relids<'a>(clause: &Expr<'a>, relids: &Relids) -> PgResult<bool>
);
seam_core::seam!(
    /// `estimate_expression_value(root, node)` (clauses.c): fold a node to a
    /// `Const` if possible. Returns the (possibly-rewritten) expression by value.
    pub fn estimate_expression_value<'mcx, 'a>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        node: &Expr<'a>,
    ) -> PgResult<Expr<'static>>
);

/* ======================================================================
 * nodes/equalfuncs.c — structural node equality (unported owner).
 * ==================================================================== */

/* ======================================================================
 * nodes/equalfuncs.c — `equal_expr` is the canonical
 * `backend-nodes-equalfuncs-seams` seam (referenced, not re-declared).
 * ====================================================================== */

/* ======================================================================
 * optimizer/util/restrictinfo.c — restrictinfo predicates (unported owner).
 * ==================================================================== */

seam_core::seam!(
    /// `restriction_is_securely_promotable(rinfo, rel)` (restrictinfo.c).
    pub fn restriction_is_securely_promotable(
        root: &PlannerInfo,
        rinfo: RinfoId,
        rel: RelId,
    ) -> bool
);
seam_core::seam!(
    /// `restriction_is_or_clause(rinfo)` (restrictinfo.c): rinfo wraps an OR.
    pub fn restriction_is_or_clause(root: &PlannerInfo, rinfo: RinfoId) -> bool
);
seam_core::seam!(
    /// `join_clause_is_movable_to(rinfo, rel)` (restrictinfo.c).
    pub fn join_clause_is_movable_to(root: &PlannerInfo, rinfo: RinfoId, rel: RelId) -> bool
);

/* ======================================================================
 * optimizer/path/equivclass.c — implied-equality generation (unported owner).
 *
 * The C `generate_implied_equalities_for_column` takes an opaque
 * `ec_matches_callback`. The matcher cannot ride a bare `fn` pointer that needs
 * the planner arena unless we thread `root` through it, so the seam takes a
 * matcher of the arena form `fn(&PlannerInfo, RelId, EcId, EmId) -> bool`; the
 * single caller (tidpath.c) supplies its ported `ec_member_matches_ctid`.
 * ==================================================================== */

seam_core::seam!(
    /// `generate_implied_equalities_for_column(root, rel, callback, callback_arg,
    /// prohibited_rels)` (equivclass.c). Returns the generated join-clause
    /// `RestrictInfo` list as `RinfoId` handles.
    pub fn generate_implied_equalities_for_column<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        rel: RelId,
        callback: fn(&PlannerInfo, RelId, EcId, EmId) -> bool,
        prohibited_rels: &Relids,
    ) -> PgResult<Vec<RinfoId>>
);

/* ======================================================================
 * optimizer/util/var.c + clauses.c — the `&Expr`-shaped variants tidpath.c
 * needs for operands that live inline in a node's `args` (no `NodeId` exists
 * for them). The `NodeId` forms live in joinpath-seams.
 * ==================================================================== */

seam_core::seam!(
    /// `pull_varnos(root, (Node *) expr)` (var.c) for an inline `&Expr` operand.
    pub fn pull_varnos_expr<'a>(root: &mut PlannerInfo, expr: &Expr<'a>) -> Relids
);
seam_core::seam!(
    /// `contain_volatile_functions((Node *) expr)` (clauses.c) for an inline
    /// `&Expr` operand.
    pub fn contain_volatile_functions_expr<'mcx>(expr: &Expr<'mcx>) -> bool
);
