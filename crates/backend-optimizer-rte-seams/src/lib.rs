//! Seam declarations for the RTE / Query field projections the optimizer reads
//! (`optimizer/path/allpaths.c`, `optimizer/util/relnode.c`,
//! `optimizer/path/costsize.c`, `optimizer/plan/setrefs.c`).
//!
//! # Why these are seams (the RTE/Query field-projection keystone)
//!
//! Architecturally [`types_pathnodes::PlannerInfo`] is `#![no_std]`,
//! lifetime-free, and `#[derive(Default)]`. It must stay that way: the planner
//! threads it everywhere and giving it an `'mcx` lifetime (or an inline RTE
//! arena) would force the lifetime through the entire optimizer. The real
//! range-table entries — [`types_nodes::RangeTblEntry`]`<'mcx>` and the owning
//! [`types_nodes::Query`]`<'mcx>` — are owned by the *Query* (the planner-entry
//! crate), not by `PlannerInfo`. `PlannerInfo` only holds opaque 1-based
//! handles: `simple_rte_array: Vec<RangeTblEntryId>` and `parse: QueryId`.
//!
//! So the optimizer cannot dereference `root->simple_rte_array[rti]->field`
//! directly the way C does — there is no value behind the handle inside this
//! lifetime-free struct. Instead each field read crosses a seam. The seam
//! signatures are deliberately lifetime-free: `(root, rti) -> T` where `T` is a
//! `Copy` scalar, an owned `String`, a length, or another opaque handle
//! ([`QueryId`]). Nothing returns a borrowed or `'mcx`-owned subtree, so the
//! seam-pointer slot stays a plain `fn` pointer.
//!
//! This mirrors the `node_arena` (expr-handle) keystone, but RTE/Query cannot
//! be *erased* into an arena (they are real parser-owned values), so the bridge
//! is a set of SEAMS that read fields, not an arena that owns nodes.
//!
//! # Failure surface (AGENTS.md)
//!
//! Every projection here is a pure field read of an already-built
//! range-table entry — no allocation, no `ereport`. They return bare values
//! (`Copy` scalars, lengths, handles, `Option`, `String`). The `String`-valued
//! `rte_ctename` returns an owned copy of the C `char *ctename`; the C field
//! read itself cannot fail, so it returns `String` directly, not `PgResult`.
//!
//! # Ownership / install
//!
//! The REAL implementation is installed by the planner-entry crate
//! (`subquery_planner` / `planner.c`) once it lands: it closes over the
//! `&'mcx Query` it is planning and resolves `RangeTblEntryId` -> the owned
//! `RangeTblEntry<'mcx>` it owns. That crate is unported. Until then the seams
//! are uninstalled and panic loudly on call (the honest
//! seam-and-panic-until-owner pattern).
//!
//! For consumer unit tests NOW (relnode/allpaths/costsize), this crate exposes
//! [`synthetic::install_synthetic_rte_table`]: a test fixture that installs
//! every seam against a hand-built, in-memory RT-index space. It lets the
//! optimizer crates be exercised against a synthetic range table before the
//! planner exists.

use types_core::primitive::Index;
use types_pathnodes::{JoinType, PlannerInfo, QueryId, RTEKind};

/* ======================================================================
 * RTE field projections — `root->simple_rte_array[rti]->field`.
 *
 * `rti` is the 1-based range-table index (the C `Index`). The accessors
 * mirror the exact set of `rte->...` reads in allpaths.c / relnode.c /
 * costsize.c (verified against PG 18.3 source).
 * ==================================================================== */

seam_core::seam!(
    /// `root->simple_rte_array[rti]->rtekind` (allpaths.c / relnode.c /
    /// costsize.c).
    pub fn rte_rtekind(root: &PlannerInfo, rti: Index) -> RTEKind
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->relkind` — the relation kind char
    /// (allpaths.c `set_rel_size` dispatch).
    pub fn rte_relkind(root: &PlannerInfo, rti: Index) -> i8
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->relid` — the relation OID
    /// (allpaths.c / relnode.c).
    pub fn rte_relid(root: &PlannerInfo, rti: Index) -> types_core::primitive::Oid
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->inh` — inheritance requested?
    /// (allpaths.c / relnode.c).
    pub fn rte_inh(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->subquery` (allpaths.c `set_subquery_pathlist`):
    /// the sub-Query as an opaque [`QueryId`] handle, or `None` when the RTE has
    /// no subquery. The owned `Query<'mcx>` value stays in the planner-entry
    /// crate; only the handle crosses (the planner itself runs
    /// `subquery_planner` on it).
    pub fn rte_subquery(root: &PlannerInfo, rti: Index) -> Option<QueryId>
);
seam_core::seam!(
    /// `list_length(root->simple_rte_array[rti]->functions)` — number of
    /// `RangeTblFunction`s in a function RTE (allpaths.c / costsize.c). A length,
    /// not the owned node list (the nodes stay parser-owned).
    pub fn rte_functions_len(root: &PlannerInfo, rti: Index) -> i32
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->funcordinality` — `WITH ORDINALITY`?
    /// (allpaths.c).
    pub fn rte_funcordinality(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->ctename` — the WITH-list item name
    /// (allpaths.c `set_cte_pathlist`), as an owned copy of the C `char *`.
    /// Empty string models a NULL `ctename`.
    pub fn rte_ctename(root: &PlannerInfo, rti: Index) -> alloc::string::String
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->ctelevelsup` — query levels up to the CTE
    /// (allpaths.c).
    pub fn rte_ctelevelsup(root: &PlannerInfo, rti: Index) -> Index
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->self_reference` — recursive CTE
    /// self-reference? (allpaths.c / costsize.c).
    pub fn rte_self_reference(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->tablesample != NULL` — does this relation
    /// RTE carry a TABLESAMPLE clause? (allpaths.c / costsize.c). Presence only;
    /// the `TableSampleClause` node stays parser-owned.
    pub fn rte_has_tablesample(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->tablefunc != NULL` — does this RTE carry a
    /// `TableFunc`? (costsize.c `cost_tablefuncscan`). Presence only.
    pub fn rte_has_tablefunc(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->security_barrier` — from a
    /// security_barrier view? (allpaths.c `set_subquery_pathlist`).
    pub fn rte_security_barrier(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `list_length(root->simple_rte_array[rti]->values_lists)` — number of
    /// VALUES rows (allpaths.c / costsize.c `cost_valuesscan`). A length; the
    /// expression lists stay parser-owned.
    pub fn rte_values_lists_len(root: &PlannerInfo, rti: Index) -> i32
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->lateral` — was LATERAL specified?
    /// (allpaths.c / relnode.c).
    pub fn rte_lateral(root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->jointype` — the join type of a JOIN RTE
    /// (relnode.c `build_join_rel` / `build_joinrel_partition_info`).
    pub fn rte_jointype(root: &PlannerInfo, rti: Index) -> JoinType
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->enrtuples` — caller-supplied tuple estimate
    /// for an ENR (costsize.c `set_namedtuplestore_size_estimates`).
    pub fn rte_enrtuples(root: &PlannerInfo, rti: Index) -> f64
);

/* ======================================================================
 * Query-level projections — `root->parse->...` (setrefs.c / relnode.c).
 *
 * `root->parse` is the opaque [`QueryId`]; setrefs.c walks the whole
 * range-table by index, so it needs the table length plus the per-index RTE
 * handle, and the parallel `rteperminfos` length.
 * ==================================================================== */

seam_core::seam!(
    /// `list_length(root->parse->rtable)` — number of range-table entries
    /// (setrefs.c `set_plan_references` / `add_rtes_to_flat_rtable`).
    pub fn parse_rtable_len(root: &PlannerInfo) -> i32
);
seam_core::seam!(
    /// `list_nth(root->parse->rtable, rti - 1)` — the `rti`-th (1-based) RTE as
    /// an opaque [`RangeTblEntryId`] handle. Equivalent to indexing
    /// `root->simple_rte_array[rti]` once the array is built; setrefs.c reads the
    /// flat `parse->rtable` list directly while flattening.
    pub fn parse_rte(root: &PlannerInfo, rti: Index) -> types_pathnodes::RangeTblEntryId
);
seam_core::seam!(
    /// `list_length(root->parse->rteperminfos)` — number of `RTEPermissionInfo`
    /// entries (setrefs.c `add_rte_to_flat_rtable` permission-info bookkeeping).
    pub fn parse_rteperminfos_len(root: &PlannerInfo) -> i32
);

extern crate alloc;

#[cfg(any(test, feature = "synthetic"))]
pub mod synthetic;

#[cfg(not(any(test, feature = "synthetic")))]
pub mod synthetic {
    //! Stub module so the public path exists in non-test builds; the real
    //! fixture lives behind `cfg(test)` / `feature = "synthetic"`.
}
