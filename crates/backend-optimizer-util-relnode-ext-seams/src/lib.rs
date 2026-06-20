//! Outward seam declarations for the not-yet-ported externals that
//! `optimizer/util/relnode.c` calls and that are not already declared by another
//! `-seams` crate.
//!
//! These belong to several distinct owners (plancat.c, parse_relation.c,
//! initsplan.c/inherit.c, allpaths.c, placeholder.c, costsize.c, equivclass.c,
//! pathkeys.c, makefuncs.c, lsyscache.c, nodeFuncs.c) that are not ported (or
//! whose relevant surface is not yet declared) at the point relnode.c lands.
//! They are homed here in a single consumer-side seam crate with NO owner
//! directory, so each call panics loudly until the real owner lands ("mirror PG
//! and panic"); the owning crates install their own once ported. The
//! `every_declared_seam_is_installed_by_its_owner` guard skips this crate because
//! no `backend-optimizer-util-relnode-ext` owner directory exists.

extern crate alloc;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{AppendRelInfo, PathId, PathTarget, PlannerInfo, RelId, RinfoId};

/* ---- plancat.c / parse_relation.c (base-rel construction) -------------- */

seam_core::seam!(
    /// `create_empty_pathtarget()` (tlist.c) — a fresh, empty `PathTarget`.
    pub fn create_empty_pathtarget() -> PathTarget
);
seam_core::seam!(
    /// `get_relation_info(root, relationObjectId, inhparent, rel)` (plancat.c) —
    /// fills the base `RelOptInfo` (handle into `rel_arena`) with catalog stats.
    ///
    /// Threads the planner-run resolver (`run`): the body reaches
    /// `get_relation_foreign_keys`, which reads RTE fields through the re-signed
    /// `rte_*` seams that now take `&PlannerRun<'mcx>`.
    pub fn get_relation_info<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        relation_object_id: Oid,
        inhparent: bool,
        rel: RelId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `getRTEPermissionInfo(root->parse->rteperminfos, rte)->checkAsUser`
    /// (parse_relation.c) — the userid to check access as for the RTE at the
    /// given RT index.
    pub fn get_rte_perminfo_checkasuser(root: &PlannerInfo, rti: u32) -> Oid
);
seam_core::seam!(
    /// `list_length(rte->eref->colnames)` — number of column aliases of the RTE
    /// at the given RT index (relnode.c uses this to size attr arrays). Threads
    /// the planner-run resolver so the RTE is reached through `planner_rt_fetch`
    /// like the rest of the `rte_*` projections (owner: rte-seams).
    pub fn rte_eref_colnames_len<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &PlannerInfo,
        rti: u32,
    ) -> i32
);

/* ---- initsplan.c / inherit.c / allpaths.c (child-rel quals) ------------ */

seam_core::seam!(
    /// `apply_child_basequals(root, parentrel, childrel, childRTE, appinfo)`
    /// (inherit.c) — translate the parent's base quals onto the child; returns
    /// false if the result is constant-FALSE/NULL.
    pub fn apply_child_basequals<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        parent: RelId,
        rel: RelId,
        rti: u32,
        appinfo: &AppendRelInfo,
    ) -> PgResult<bool>
);
seam_core::seam!(
    /// `mark_dummy_rel(rel)` (joinrels.c) — mark a relation as proven empty.
    /// Threads the planner-run resolver (`run`): the joinrels.c body adds a dummy
    /// path which reaches `add_path`/cost helpers keyed on `&PlannerRun<'mcx>`.
    pub fn mark_dummy_rel<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        rel: RelId,
    ) -> PgResult<()>
);

/* ---- placeholder.c ----------------------------------------------------- */

seam_core::seam!(
    /// `add_placeholders_to_joinrel(root, joinrel, outer_rel, inner_rel, sjinfo)`
    /// (placeholder.c) — add to the joinrel's tlist the PlaceHolderVars that need
    /// to be computed there.
    pub fn add_placeholders_to_joinrel(
        root: &mut PlannerInfo,
        joinrel: RelId,
        outer_rel: RelId,
        inner_rel: RelId,
        sjinfo: &types_pathnodes::SpecialJoinInfo,
    ) -> PgResult<()>
);

/* ---- restrictinfo.c (clause movability) -------------------------------- */

seam_core::seam!(
    /// `join_clause_is_movable_into(rinfo, currentrelids, current_and_required)`
    /// (restrictinfo.c) — the faithful relids-typed form (the C takes `Relids`,
    /// not rels). `current_relids` is the rel(s) the clause would attach to;
    /// `join_and_required` is `bms_union(current_relids, required_outer)`. The
    /// existing `RelId`-keyed `costsize` seam can't express a transient relid set
    /// (e.g. `bms_union(baserel->relids, required_outer)` is not a built rel), so
    /// relnode crosses here.
    pub fn join_clause_is_movable_into_relids(
        root: &PlannerInfo,
        rinfo: RinfoId,
        current_relids: &types_pathnodes::Relids,
        join_and_required: &types_pathnodes::Relids,
    ) -> bool
);

/* ---- appendinfo.c (single-node attr translation) ----------------------- */

seam_core::seam!(
    /// `adjust_appendrel_attrs(root, (Node *) node, nappinfos, appinfos)`
    /// (appendinfo.c) — single-level parent→child Var translation of one
    /// expression node, with the `AppendRelInfo` array passed faithfully (the C
    /// `AppendRelInfo **appinfos`). Used by `build_child_join_reltarget`.
    pub fn adjust_appendrel_attrs_node(
        root: &mut PlannerInfo,
        node: Expr,
        appinfos: &[AppendRelInfo],
    ) -> PgResult<Expr>
);

/* ---- costsize.c (size estimation) -------------------------------------- */

seam_core::seam!(
    /// `set_joinrel_size_estimates(root, joinrel, outer_rel, inner_rel, sjinfo,
    /// restrictlist)` (costsize.c) — set the joinrel's `rows`.
    pub fn set_joinrel_size_estimates<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        joinrel: RelId,
        outer_rel: RelId,
        inner_rel: RelId,
        sjinfo: &types_pathnodes::SpecialJoinInfo,
        restrictlist: &[RinfoId],
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `get_parameterized_baserel_size(root, rel, param_clauses)` (costsize.c).
    pub fn get_parameterized_baserel_size<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        baserel: RelId,
        param_clauses: &[RinfoId],
    ) -> f64
);
seam_core::seam!(
    /// `get_parameterized_joinrel_size(root, rel, outer_path, inner_path, sjinfo,
    /// restrict_clauses)` (costsize.c).
    pub fn get_parameterized_joinrel_size<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        joinrel: RelId,
        outer_path: PathId,
        inner_path: PathId,
        sjinfo: &types_pathnodes::SpecialJoinInfo,
        restrict_clauses: &[RinfoId],
    ) -> f64
);
seam_core::seam!(
    /// `clamp_width_est(tuple_width)` (costsize.c) — clamp a summed tuple width
    /// to `[0, MaxAllocSize]` and narrow to `int32`.
    pub fn clamp_width_est(tuple_width: i64) -> i32
);
seam_core::seam!(
    /// `enable_partitionwise_join` GUC (costsize.c) — whether partitionwise join
    /// is enabled.
    pub fn enable_partitionwise_join() -> bool
);

/* ---- equivclass.c / pathkeys.c (child-join eclasses) ------------------- */

seam_core::seam!(
    /// `add_child_join_rel_equivalences(root, nappinfos, appinfos,
    /// parent_joinrel, child_joinrel)` (equivclass.c).
    pub fn add_child_join_rel_equivalences<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        appinfos: &[AppendRelInfo],
        parent_joinrel: RelId,
        child_joinrel: RelId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `has_useful_pathkeys(root, rel)` (pathkeys.c).
    pub fn has_useful_pathkeys(root: &PlannerInfo, rel: RelId) -> bool
);

/* ---- lsyscache.c (partitionwise-join key matching) -------------------- */

seam_core::seam!(
    /// `linitial_oid(get_mergejoin_opfamilies(opno))` (lsyscache.c) — the first
    /// btree opfamily `opno` represents equality in, or `None` when the C
    /// `get_mergejoin_opfamilies` returns NIL. Relnode-local convenience over the
    /// `Mcx`-taking list form so relnode need not carry an `Mcx`.
    pub fn get_mergejoin_opfamilies_first(opno: Oid) -> Option<Oid>
);

/* ---- preptlist.c (row identity vars) ----------------------------------- */

seam_core::seam!(
    /// `((RowIdentityVarInfo *) list_nth(root->row_identity_vars, n))->rowidwidth`
    /// (preptlist.c) — the cached width of the n'th (0-based) `RowIdentityVarInfo`.
    /// `RowIdentityVarInfo` is not an `Expr`, so its payload is owned by the
    /// preptlist unit and reached through this seam rather than `node_arena`.
    pub fn row_identity_var_rowidwidth(root: &PlannerInfo, n: i32) -> i32
);

/* ---- nodeFuncs.c / makefuncs.c (partition-key expr building) ---------- */

seam_core::seam!(
    /// `makeNode(CoalesceExpr)` with `coalescetype = exprType(larg)`,
    /// `coalescecollid = exprCollation(larg)`, `args = list_make2(larg, rarg)`,
    /// `location = -1` (relnode.c `set_joinrel_partition_key_exprs`). `larg`/
    /// `rarg` are the partition-key expression handles; the owner builds the node
    /// and returns it for interning into `node_arena`.
    pub fn make_coalesce_expr(larg: &Expr, rarg: &Expr) -> Expr
);
