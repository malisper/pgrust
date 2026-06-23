//! Inward seam declarations for `optimizer/path/indxpath.c` — the index-path
//! generator. Arena-shaped over [`::pathnodes::PlannerInfo`].
//!
//! These are the externally-visible entry points of indxpath.c that other
//! (cycle-partner) crates call: `allpaths.c` (`create_index_paths`),
//! `plancat.c` (`check_index_predicates`), `analyzejoins.c` /
//! `joinpath.c` (`relation_has_unique_index_{for,ext}`), `pathkeys.c`
//! (`indexcol_is_bool_constant_for_query`), and `selfuncs.c`
//! (`match_index_to_operand`). The indxpath crate installs each from its
//! `init_seams()`; until then a call panics loudly.

extern crate alloc;

use alloc::vec::Vec;

use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{IndexOptInfo, NodeId, PlannerInfo, RelId, RinfoId};

seam_core::seam!(
    /// `create_index_paths(root, rel)` (indxpath.c:241) — generate all index
    /// paths (plain + bitmap) for the relation and submit them to the rel's
    /// pathlist. Allocates paths (can OOM), hence `PgResult`.
    ///
    /// Threads the planner-run resolver (`run`): the body reads RTE fields
    /// through the re-signed `rte_*`/path-builder seams that take
    /// `&PlannerRun<'mcx>`.
    pub fn create_index_paths<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `check_index_predicates(root, rel)` (indxpath.c:3943) — set each index's
    /// `predOK` / `indrestrictinfo` predicate-derived fields for the rel.
    ///
    /// Threads the planner-run resolver (`run`): the body reaches the re-signed
    /// `generate_join_implied_equalities` seam that takes `&PlannerRun<'mcx>`.
    pub fn check_index_predicates<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rel: RelId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `relation_has_unique_index_for(root, rel, restrictlist, exprlist,
    /// oprlist)` (indxpath.c:4149) — does the rel provably yield at most one row
    /// per combination of the given equality conditions (some unique index is
    /// fully constrained)? `restrictlist` is a list of `RinfoId`; `exprlist` is a
    /// list of clause `NodeId`s; `oprlist` is the parallel list of equality
    /// operator OIDs.
    pub fn relation_has_unique_index_for(
        root: &mut PlannerInfo,
        rel: RelId,
        restrictlist: &[RinfoId],
        exprlist: &[NodeId],
        oprlist: &[Oid]
    ) -> bool
);

seam_core::seam!(
    /// `relation_has_unique_index_ext(root, rel, restrictlist, exprlist,
    /// oprlist, &extra_clauses)` (indxpath.c:4163) — like
    /// `relation_has_unique_index_for`, but also returns (as the second tuple
    /// element) the baserestrictinfo clauses that contributed to uniqueness
    /// (empty when the result is false or no extra clauses were used).
    pub fn relation_has_unique_index_ext(
        root: &mut PlannerInfo,
        rel: RelId,
        restrictlist: &[RinfoId],
        exprlist: &[NodeId],
        oprlist: &[Oid]
    ) -> (bool, Vec<RinfoId>)
);

seam_core::seam!(
    /// `indexcol_is_bool_constant_for_query(root, index, indexcol)`
    /// (indxpath.c:4362) — is the index column constrained to a constant boolean
    /// value by the query's WHERE clauses (so it's irrelevant for sort order)?
    pub fn indexcol_is_bool_constant_for_query(
        root: &mut PlannerInfo,
        index: &IndexOptInfo,
        indexcol: i32
    ) -> bool
);

seam_core::seam!(
    /// `match_index_to_operand(operand, indexcol, index)` (indxpath.c:4413) —
    /// does the operand expression match index column `indexcol` of `index`?
    /// `operand` is an arena `NodeId`; `root` is threaded to resolve
    /// `index->rel->relid`. Exported for selfuncs.c.
    pub fn match_index_to_operand(
        root: &PlannerInfo,
        operand: NodeId,
        indexcol: i32,
        index: &IndexOptInfo
    ) -> bool
);
