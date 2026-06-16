//! Outward seam declarations for the not-yet-ported (or not-acyclically-
//! reachable) externals that `optimizer/plan/initsplan.c` calls and that have
//! no existing seam home elsewhere:
//!
//!   * `lookup_type_cache_hasheq` (lookup_type_cache.c / typcache.c) — the
//!     `TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR` lookup used by
//!     `check_memoizable`; returns `(hash_proc, eq_opr)`.
//!   * `phinfo_add_needed` (placeholder.c) — add `where_needed` to a
//!     PlaceHolderVar's `PlaceHolderInfo::ph_needed`. The owner placeholder.c
//!     does not expose this (it is a static helper there); homed here so
//!     `add_vars_to_targetlist`/`add_vars_to_attr_needed` can update PHV needs.
//!   * `expand_inherited_rtentry` (inherit.c) — UNPORTED owner; only reached for
//!     inheritance parents from `add_other_rels_to_query`.
//!   * `preprocess_phv_expression` (subselect.c) — UNPORTED owner; only reached
//!     for LATERAL PlaceHolderVars with `phlevelsup > 0` in
//!     `extract_lateral_references`.
//!   * `increment_var_sublevels_up` (rewriteManip.c) over an owned arena `Expr`
//!     — used by `extract_lateral_references` for upper-level PHVs.
//!
//! Each call panics loudly until the real owner installs it ("mirror PG and
//! panic"). This crate has NO owner directory, so the
//! `every_declared_seam_is_installed_by_its_owner` guard skips it.

#![allow(non_snake_case)]

extern crate alloc;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, PlaceHolderVar};
use types_pathnodes::{PlannerInfo, Relids};

seam_core::seam!(
    /// `lookup_type_cache(type, TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR)`
    /// (typcache.c) projected to `(hash_proc, eq_opr)` for `check_memoizable`.
    pub fn lookup_type_cache_hasheq(typid: Oid) -> (Oid, Oid)
);

seam_core::seam!(
    /// `find_placeholder_info(root, phv); phinfo->ph_needed = bms_add_members(
    /// phinfo->ph_needed, where_needed)` (placeholder.c / initsplan.c) — update
    /// a PlaceHolderVar's `ph_needed` set.
    pub fn phinfo_add_needed(root: &mut PlannerInfo, phv: &PlaceHolderVar, where_needed: &Relids) -> PgResult<()>
);

seam_core::seam!(
    /// `expand_inherited_rtentry(root, rel, rte, rti)` (inherit.c) — expand an
    /// inheritance/partition parent into its child "otherrels". UNPORTED owner.
    pub fn expand_inherited_rtentry(root: &mut PlannerInfo, rti: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `preprocess_phv_expression(root, expr)` (subselect.c) — run
    /// SS_process_sublinks / SS_replace_correlation_vars over an upper-level
    /// PlaceHolderVar's expression. UNPORTED owner; only reached for
    /// LATERAL PHVs with `phlevelsup > 0`.
    pub fn preprocess_phv_expression(root: &mut PlannerInfo, expr: Expr) -> PgResult<Expr>
);

seam_core::seam!(
    /// `IncrementVarSublevelsUp((Node *) expr, -((int) phlevelsup), 0)`
    /// (rewriteManip.c) over an owned arena `Expr`. Only reached for upper-level
    /// LATERAL PlaceHolderVars in `extract_lateral_references`.
    pub fn increment_var_sublevels_up_expr(expr: Expr, delta_sublevels_up: i32, min_sublevels_up: i32) -> PgResult<Expr>
);

seam_core::seam!(
    /// `pull_vars_of_level((Node *) node, levelsup)` (var.c) over a borrowed
    /// `Node` subtree. Used by `extract_lateral_references` to gather the
    /// level-`levelsup` Vars/PHVs referenced by a LATERAL RTE's
    /// `functions`/`tablefunc`/`values_lists`/`tablesample` parse subtrees
    /// (each a `Node *` in C). var.c is ported but its installed `&Expr`/`NodeId`
    /// var seams cannot name a whole parse `Node`; this per-`Node` seam is the
    /// home, loud-panic until var.c installs it.
    pub fn pull_vars_of_level_node<'mcx>(node: &Node<'mcx>, levelsup: i32) -> alloc::vec::Vec<Expr>
);

seam_core::seam!(
    /// `pull_vars_of_level((Node *) rte->subquery, levelsup)` (var.c) over a
    /// borrowed `Query`. The RTE_SUBQUERY arm of `extract_lateral_references`
    /// needs to walk the sub-`Query` (which enters one more query level); the
    /// owned `Query<'mcx>` is not a `Node` value and is not `Clone`, so it rides
    /// its own seam. Loud-panic until var.c installs it.
    pub fn pull_vars_of_level_query<'mcx>(query: &Query<'mcx>, levelsup: i32) -> alloc::vec::Vec<Expr>
);

seam_core::seam!(
    /// `add_nulling_relids((Node *) quals, target, added)` (rewriteManip.c) over
    /// an owned arena `Expr` (a single implicit-AND conjunct). The rewrite-core
    /// owner works over `&mut Node`, a model mismatch — homed here as a per-`Expr`
    /// seam. Only reached for outer-join clone quals in
    /// `deconstruct_distribute_oj_quals`.
    pub fn add_nulling_relids_expr(expr: Expr, target: Relids, added: Relids) -> Expr
);

seam_core::seam!(
    /// `eval_const_expressions(root, node)` (clauses.c) over an owned arena
    /// `Expr`. Used by `process_implied_equality` when both operands are
    /// pseudo-constant, to fold a derived `item1 op item2` clause to a boolean
    /// `Const` where possible. clauses.c is ported but works over `&Node`/`Mcx`;
    /// this per-`Expr` seam is the cycle break.
    pub fn eval_const_expressions_expr(root: &mut PlannerInfo, node: Expr) -> PgResult<Expr>
);

seam_core::seam!(
    /// `find_nonnullable_rels((Node *) expr)` (clauses.c) over an owned arena
    /// `Expr`. clauses.c is ported but works over `&Node`; this is the cycle
    /// break used by `distribute_qual_to_rels`/`expr_is_nonnullable` callers.
    pub fn find_nonnullable_rels_expr(expr: &Expr) -> Relids
);

seam_core::seam!(
    /// `find_forced_null_var((Node *) clause)` (clauses.c) over an owned arena
    /// `Expr`, returning the `Var` (as an owned `Expr::Var`) forced to NULL by an
    /// IS NULL test, or `None`. The cycle break for
    /// `check_redundant_nullability_qual`.
    pub fn find_forced_null_var_expr(clause: &Expr) -> Option<Expr>
);

seam_core::seam!(
    /// Project the per-RTE fields `add_base_clause_to_rel` reads from
    /// `root->simple_rte_array[rti]`: `(rtekind, rte->inh, rte->relkind)`.
    /// `PlannerInfo.simple_rte_array` is the opaque `RangeTblEntryId` handle, so
    /// these are resolved via this seam (see the .port-ref reachability note).
    /// `relkind` is the `char` pg_class relkind (`'p'` =
    /// `RELKIND_PARTITIONED_TABLE`).
    pub fn rte_kind_inh_relkind(root: &PlannerInfo, rti: i32) -> (i32, bool, i8)
);
