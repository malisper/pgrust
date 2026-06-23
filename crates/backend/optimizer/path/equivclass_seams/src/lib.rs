//! Inward seam declarations OWNED by `optimizer/path/equivclass.c` — the public
//! EquivalenceClass API that the rest of the optimizer (initsplan, indxpath,
//! planner, joinrels, createplan, …) reaches through. The owning crate
//! `backend-optimizer-path-equivclass` installs every one of these from its
//! `init_seams()` at single-threaded startup. Functions whose C path `palloc`s
//! (and so can `ereport(ERROR)`) return [`PgResult`]; pure predicates / scalar
//! reads return bare values.
//!
//! The outward seams equivclass.c *calls* into not-yet-ported externals
//! (node operators, initsplan/restrictinfo/appendinfo clause machinery) live in
//! the sibling consumer-side crate
//! `backend-optimizer-path-equivclass-ext-seams`, whose owners install them once
//! they land.
//!
//! Every cross-arena expression is an [`Expr`] value (the lifetime-free
//! `pathnodes::Expr`); relation/EC/EM/clause references are
//! `RelId`/`EcId`/`EmId`/`RinfoId` handles.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{Index, Oid};
use types_error::PgResult;
use ::nodes::primnodes::Expr;
use pathnodes::planner_run::PlannerRun;
use pathnodes::{EcId, PlannerInfo, RelId, Relids, RinfoId, SpecialJoinInfo};

/* ======================================================================
 * Family 1: inward seams OWNED by equivclass.c.
 * ==================================================================== */

seam_core::seam!(
    /// `process_equivalence(root, &restrictinfo, jdomain)` (equivclass.c:179) —
    /// the union-find merge core. May rewrite `*p_restrictinfo` (the X=X →
    /// X IS NOT NULL conversion), so the (possibly new) [`RinfoId`] is returned
    /// alongside the bool. Can `palloc`/`ereport`.
    pub fn process_equivalence<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        restrictinfo: RinfoId,
        jdomain: Relids,
    ) -> PgResult<(bool, RinfoId)>
);
seam_core::seam!(
    /// `get_eclass_for_sort_expr(...)` (equivclass.c:736) — find or build the EC
    /// for a sort/group expression. Returns the matched/created [`EcId`], or
    /// `None` when `create_it` is false and there is no match.
    pub fn get_eclass_for_sort_expr(
        root: &mut PlannerInfo,
        expr: Expr,
        opfamilies: Vec<Oid>,
        opcintype: Oid,
        collation: Oid,
        sortref: Index,
        rel: Relids,
        create_it: bool,
    ) -> PgResult<Option<EcId>>
);
seam_core::seam!(
    /// `generate_base_implied_equalities(root)` (equivclass.c:1188).
    pub fn generate_base_implied_equalities<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `generate_join_implied_equalities(...)` (equivclass.c:1550) — returns the
    /// derived join [`RinfoId`]s.
    pub fn generate_join_implied_equalities<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        join_relids: Relids,
        outer_relids: Relids,
        inner_rel: RelId,
        sjinfo: Option<SpecialJoinInfo>,
    ) -> PgResult<Vec<RinfoId>>
);
seam_core::seam!(
    /// `generate_join_implied_equalities_for_ecs(...)` (equivclass.c:1650).
    pub fn generate_join_implied_equalities_for_ecs<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        eclasses: Vec<EcId>,
        join_relids: Relids,
        outer_relids: Relids,
        inner_rel: RelId,
    ) -> PgResult<Vec<RinfoId>>
);
seam_core::seam!(
    /// `exprs_known_equal(root, item1, item2, opfamily)` (equivclass.c:2648).
    pub fn exprs_known_equal(
        root: &PlannerInfo,
        item1: Expr,
        item2: Expr,
        opfamily: Oid,
    ) -> bool
);
seam_core::seam!(
    /// `reconsider_outer_join_clauses(root)` (equivclass.c:2135).
    pub fn reconsider_outer_join_clauses<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `rebuild_eclass_attr_needed(root)` (equivclass.c:2574).
    pub fn rebuild_eclass_attr_needed(mcx: mcx::Mcx<'_>, root: &mut PlannerInfo) -> PgResult<()>
);
seam_core::seam!(
    /// `have_relevant_eclass_joinclause(root, rel1, rel2)` (equivclass.c:3370).
    pub fn have_relevant_eclass_joinclause(
        root: &PlannerInfo,
        rel1: RelId,
        rel2: RelId,
    ) -> bool
);
seam_core::seam!(
    /// `has_relevant_eclass_joinclause(root, rel1)` (equivclass.c:3446).
    pub fn has_relevant_eclass_joinclause(root: &PlannerInfo, rel1: RelId) -> bool
);
seam_core::seam!(
    /// `eclass_useful_for_merging(root, eclass, rel)` (equivclass.c:3490).
    pub fn eclass_useful_for_merging(
        root: &PlannerInfo,
        eclass: EcId,
        rel: RelId,
    ) -> bool
);
seam_core::seam!(
    /// `is_redundant_derived_clause(rinfo, clauselist)` (equivclass.c:3550).
    pub fn is_redundant_derived_clause(
        root: &PlannerInfo,
        rinfo: RinfoId,
        clauselist: Vec<RinfoId>,
    ) -> bool
);
seam_core::seam!(
    /// `add_child_rel_equivalences(...)` (equivclass.c:2833).
    pub fn add_child_rel_equivalences<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        appinfo: RelId,
        parent_rel: RelId,
        child_rel: RelId,
    ) -> PgResult<()>
);
