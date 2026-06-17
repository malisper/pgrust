//! Seam declarations for the unported `initsplan.c` callees of
//! `query_planner` (planmain.c), owned by `backend-optimizer-plan-initsplan`
//! when it lands (catalog unit `backend-optimizer-plan-small`,
//! `optimizer/plan/initsplan.c`).
//!
//! `query_planner` (in `backend-optimizer-plan-small`) drives a fixed
//! ~20-call sequence over the optimizer. The trivial single-`RTE_RESULT`
//! fast path is fully built from already-landed callees (relnode / pathnode /
//! clauses / equivclass); the general join path calls these initsplan.c steps,
//! which are a *later* port stage. Until that owner installs them, each call
//! panics loudly with its seam path — never a silent stub — exactly mirroring
//! the absent-subsystem boundary.
//!
//! Signatures mirror `optimizer/planmain.h` 1:1. None of these can
//! `ereport(ERROR)` on the paths `query_planner` reaches, so they return bare
//! values (the C functions are `void`/`List *`).

#![allow(non_snake_case)]

extern crate alloc;

use types_nodes::rawnodes::FromExpr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{JoinlistNode, PlannerInfo};

seam_core::seam!(
    /// `add_base_rels_to_query(root, jtnode)` (initsplan.c:158): construct
    /// `RelOptInfo` nodes for all base relations used in the query by
    /// recursively scanning the jointree. `query_planner` makes the top call
    /// with `(Node *) parse->jointree`, which at the query top is always the
    /// `FromExpr`; the owner's recursion handles the nested `JoinExpr` /
    /// `RangeTblRef` arms internally.
    pub fn add_base_rels_to_query<'mcx>(
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        jtnode: &FromExpr<'mcx>,
    )
);

seam_core::seam!(
    /// `remove_useless_groupby_columns(root)` (initsplan.c:412): remove any
    /// redundant GROUP BY columns made unique by a relation's primary key.
    pub fn remove_useless_groupby_columns<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);

seam_core::seam!(
    /// `build_base_rel_tlists(root, final_tlist)` (initsplan.c:235): add Vars
    /// referenced by `root->processed_tlist` (the final target list) to the
    /// per-baserel target lists, generating PlaceHolderInfos as needed. The C
    /// `final_tlist` argument is always `root->processed_tlist`, which the owner
    /// reads off `root`, so no list is carried across the seam.
    pub fn build_base_rel_tlists<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);

seam_core::seam!(
    /// `find_lateral_references(root)` (initsplan.c:658): mark Vars/PHVs needed
    /// by LATERAL references in the jointree.
    pub fn find_lateral_references<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);

seam_core::seam!(
    /// `deconstruct_jointree(root)` (initsplan.c:1084): build base-rel data
    /// structures by classifying the jointree's qual clauses (restriction vs
    /// join, EC building, SpecialJoinInfo formation) and return the target
    /// joinlist for `make_one_rel` to plan from.
    pub fn deconstruct_jointree<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>) -> types_error::PgResult<alloc::vec::Vec<JoinlistNode>>
);

seam_core::seam!(
    /// `create_lateral_join_info(root)` (initsplan.c:845): construct the lateral
    /// reference sets now that PlaceHolderVar eval levels are finalized.
    pub fn create_lateral_join_info<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);

seam_core::seam!(
    /// `match_foreign_keys_to_quals(root)` (initsplan.c:3631): match foreign
    /// keys to equivalence classes and join quals.
    pub fn match_foreign_keys_to_quals(root: &mut PlannerInfo)
);

seam_core::seam!(
    /// `add_other_rels_to_query(root)` (initsplan.c:196): expand appendrels by
    /// adding "otherrels" for their children, propagating lateral_relids etc.
    /// `run` resolves the per-RT-index RTE (`rtekind`/`inh`/`relkind`) the
    /// inheritance-expansion decision reads.
    pub fn add_other_rels_to_query<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);

seam_core::seam!(
    /// `rebuild_lateral_attr_needed(root)` (initsplan.c:807): re-add
    /// `attr_needed`/`ph_needed` bits for Vars/PHVs needed for lateral references,
    /// after a join removal cleared the per-rel attr_needed sets. Called by
    /// analyzejoins.c's `remove_leftjoinrel_from_query` /
    /// `remove_self_join_rel`. Owned by `backend-optimizer-plan-init-subselect`
    /// (ported).
    pub fn rebuild_lateral_attr_needed<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);

seam_core::seam!(
    /// `rebuild_joinclause_attr_needed(root)` (initsplan.c:3559): partially
    /// repeat the work of `deconstruct_jointree` to re-add the `attr_needed` bits
    /// contributed by join clauses, after a join removal cleared the per-rel
    /// attr_needed sets. Called by analyzejoins.c's
    /// `remove_leftjoinrel_from_query` / `remove_self_join_rel`. Owned by
    /// `backend-optimizer-plan-init-subselect` (NOT YET PORTED — panics until the
    /// owner lands it).
    pub fn rebuild_joinclause_attr_needed<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>)
);
