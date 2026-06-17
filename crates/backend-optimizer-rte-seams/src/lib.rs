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
//! lifetime-free struct. The real `'mcx`-bound RTE values live in the
//! planner-run resolver ([`types_pathnodes::planner_run::PlannerRun`], the #264
//! resolver), keyed by the `RangeTblEntryId` handles in `simple_rte_array`.
//! Each field read therefore crosses a seam that threads the resolver as its
//! first parameter:
//! `for<'mcx> fn(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> T`
//! where `T` is a `Copy` scalar, an owned `String`, a length, or another opaque
//! handle ([`QueryId`]). The seam resolves the handle through
//! [`planner_rt_fetch`](types_pathnodes::planner_run::planner_rt_fetch)`(run,
//! root, rti)` and projects the field.
//!
//! Nothing returns a borrowed or `'mcx`-owned subtree (only `Copy` scalars,
//! lengths, handles, owned `String`), so even with the `&PlannerRun<'mcx>`
//! parameter the seam slot stays a zero-capture, higher-ranked `for<'mcx> fn`
//! pointer — exactly the shape `seam!` stores for a lifetime-generic signature.
//! Because the resolver is a *parameter* (not captured state), the install is a
//! pure function of its arguments: it needs no `'mcx`-closing closure and lives
//! right here in the seam crate (see [`init_seams`]), no longer only a synthetic
//! `'static` test fixture.
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
//! The REAL implementation lives in this crate's [`init_seams`]: each seam
//! resolves its `rti` through `planner_rt_fetch(run, root, rti)` and projects
//! the field. This is the genuine install on the real planner path — the run
//! the consumer threads is the live `PlannerRun<'mcx>` that `query_planner`
//! owns, so the same install serves the planner and any test. `seams-init`
//! calls [`init_seams`] once at startup.
//!
//! The lone exception is [`rte_subquery`], whose `Option<QueryId>` return is a
//! handle into a query store that the current RTE model does not populate (the
//! `RangeTblEntry` carries its sub-`Query` inline as `Option<PgBox<Query>>`,
//! not as an interned [`QueryId`]); it has no consumer yet and is left
//! uninstalled — it panics loudly on call (the honest seam-and-panic pattern)
//! until the subquery-interning model lands. See [`init_seams`].
//!
//! For consumer unit tests that build a hand-shaped range table without a full
//! parser, [`synthetic`] still offers
//! [`synthetic::install_synthetic_rte_table`]; it installs the same seams
//! against an in-memory RT-index space (the `run` parameter is ignored there).

use types_core::primitive::Index;
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
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
    pub fn rte_rtekind<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> RTEKind
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->relkind` — the relation kind char
    /// (allpaths.c `set_rel_size` dispatch).
    pub fn rte_relkind<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> i8
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->relid` — the relation OID
    /// (allpaths.c / relnode.c).
    pub fn rte_relid<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> types_core::primitive::Oid
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->inh` — inheritance requested?
    /// (allpaths.c / relnode.c).
    pub fn rte_inh<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `getRTEPermissionInfo(root->parse->rteperminfos, rte)->checkAsUser`
    /// (parse_relation.c) for the RTE at 1-based `rti`: the userid to check
    /// access as, or `InvalidOid` when the RTE has no permission info
    /// (`perminfoindex == 0`). `build_simple_rel` (relnode.c) reads this for the
    /// base relation's `rel->userid`.
    pub fn rte_perminfo_checkasuser<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> types_core::primitive::Oid
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->subquery` (allpaths.c `set_subquery_pathlist`):
    /// the sub-Query as an opaque [`QueryId`] handle, or `None` when the RTE has
    /// no subquery. The owned `Query<'mcx>` value stays in the planner-entry
    /// crate; only the handle crosses (the planner itself runs
    /// `subquery_planner` on it).
    pub fn rte_subquery<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> Option<QueryId>
);
seam_core::seam!(
    /// `list_length(root->simple_rte_array[rti]->functions)` — number of
    /// `RangeTblFunction`s in a function RTE (allpaths.c / costsize.c). A length,
    /// not the owned node list (the nodes stay parser-owned).
    pub fn rte_functions_len<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> i32
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->funcordinality` — `WITH ORDINALITY`?
    /// (allpaths.c).
    pub fn rte_funcordinality<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->ctename` — the WITH-list item name
    /// (allpaths.c `set_cte_pathlist`), as an owned copy of the C `char *`.
    /// Empty string models a NULL `ctename`.
    pub fn rte_ctename<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> alloc::string::String
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->ctelevelsup` — query levels up to the CTE
    /// (allpaths.c).
    pub fn rte_ctelevelsup<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> Index
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->self_reference` — recursive CTE
    /// self-reference? (allpaths.c / costsize.c).
    pub fn rte_self_reference<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->tablesample != NULL` — does this relation
    /// RTE carry a TABLESAMPLE clause? (allpaths.c / costsize.c). Presence only;
    /// the `TableSampleClause` node stays parser-owned.
    pub fn rte_has_tablesample<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->tablefunc != NULL` — does this RTE carry a
    /// `TableFunc`? (costsize.c `cost_tablefuncscan`). Presence only.
    pub fn rte_has_tablefunc<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->security_barrier` — from a
    /// security_barrier view? (allpaths.c `set_subquery_pathlist`).
    pub fn rte_security_barrier<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `list_length(root->simple_rte_array[rti]->values_lists)` — number of
    /// VALUES rows (allpaths.c / costsize.c `cost_valuesscan`). A length; the
    /// expression lists stay parser-owned.
    pub fn rte_values_lists_len<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> i32
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->lateral` — was LATERAL specified?
    /// (allpaths.c / relnode.c).
    pub fn rte_lateral<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> bool
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->jointype` — the join type of a JOIN RTE
    /// (relnode.c `build_join_rel` / `build_joinrel_partition_info`).
    pub fn rte_jointype<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> JoinType
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->enrtuples` — caller-supplied tuple estimate
    /// for an ENR (costsize.c `set_namedtuplestore_size_estimates`).
    pub fn rte_enrtuples<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> f64
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
    pub fn parse_rtable_len<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo) -> i32
);
seam_core::seam!(
    /// `list_nth(root->parse->rtable, rti - 1)` — the `rti`-th (1-based) RTE as
    /// an opaque [`RangeTblEntryId`] handle. Equivalent to indexing
    /// `root->simple_rte_array[rti]` once the array is built; setrefs.c reads the
    /// flat `parse->rtable` list directly while flattening.
    pub fn parse_rte<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> types_pathnodes::RangeTblEntryId
);
seam_core::seam!(
    /// `list_length(root->parse->rteperminfos)` — number of `RTEPermissionInfo`
    /// entries (setrefs.c `add_rte_to_flat_rtable` permission-info bookkeeping).
    pub fn parse_rteperminfos_len<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo) -> i32
);

/* ======================================================================
 * Real install — the genuine planner-path implementation.
 *
 * Each seam resolves its 1-based RT index through `planner_rt_fetch(run, root,
 * rti)` (the #264 resolver behind `simple_rte_array` / `parse->rtable`) and
 * projects the field, exactly as C dereferences
 * `root->simple_rte_array[rti]->field`. The `run` parameter is the live
 * `PlannerRun<'mcx>` the consumer threads (`query_planner` owns it); because the
 * resolver is a parameter, every closure here is zero-capture and the install
 * is a pure function of its arguments — no `'mcx`-closing state.
 *
 * `seams-init` calls this once at startup.
 * ==================================================================== */

/// Install every RTE/Query-projection seam against the real planner-run
/// resolver. Called once from `seams-init`.
///
/// [`rte_subquery`] is intentionally NOT installed: its `Option<QueryId>`
/// return is a handle into a query store, but the current `RangeTblEntry` model
/// carries its sub-`Query` inline (`Option<PgBox<Query>>`), not as an interned
/// [`QueryId`]. There is no faithful way to manufacture a `QueryId` from the
/// inline value, and the seam has no consumer; it stays uninstalled and panics
/// loudly on call until the subquery-interning model lands.
pub fn init_seams() {
    // RTE field projections — `planner_rt_fetch(run, root, rti)->field`.
    rte_rtekind::set(|run, root, rti| planner_rt_fetch(run, root, rti).rtekind as RTEKind);

    // `add_base_clause_to_rel` / `add_other_rels_to_query` /
    // `remove_useless_groupby_columns` read `(rtekind, inh, relkind)` from one
    // RTE in a single projection. Declared in the init-subselect-ext consumer
    // crate; resolved here through the same `planner_rt_fetch` RTE store.
    backend_optimizer_plan_init_subselect_ext_seams::rte_kind_inh_relkind::set(|run, root, rti| {
        let rte = planner_rt_fetch(run, root, rti as Index);
        (rte.rtekind as i32, rte.inh, rte.relkind)
    });
    rte_relkind::set(|run, root, rti| planner_rt_fetch(run, root, rti).relkind);
    rte_relid::set(|run, root, rti| planner_rt_fetch(run, root, rti).relid);
    rte_inh::set(|run, root, rti| planner_rt_fetch(run, root, rti).inh);
    rte_perminfo_checkasuser::set(|run, root, rti| {
        // getRTEPermissionInfo(parse->rteperminfos, rte): list_nth(rteperminfos,
        // rte->perminfoindex - 1). perminfoindex == 0 ⇒ no permission info.
        let perminfoindex = planner_rt_fetch(run, root, rti).perminfoindex;
        if perminfoindex == 0 {
            return types_core::primitive::Oid::default(); // InvalidOid
        }
        run.resolve(root.parse).rteperminfos[(perminfoindex - 1) as usize].checkAsUser
    });
    rte_functions_len::set(|run, root, rti| {
        planner_rt_fetch(run, root, rti).functions.len() as i32
    });
    rte_funcordinality::set(|run, root, rti| planner_rt_fetch(run, root, rti).funcordinality);
    rte_ctename::set(|run, root, rti| match planner_rt_fetch(run, root, rti).ctename.as_ref() {
        Some(s) => alloc::string::String::from(s.as_str()),
        None => alloc::string::String::new(),
    });
    rte_ctelevelsup::set(|run, root, rti| planner_rt_fetch(run, root, rti).ctelevelsup);
    rte_self_reference::set(|run, root, rti| planner_rt_fetch(run, root, rti).self_reference);
    rte_has_tablesample::set(|run, root, rti| {
        planner_rt_fetch(run, root, rti).tablesample.is_some()
    });
    rte_has_tablefunc::set(|run, root, rti| planner_rt_fetch(run, root, rti).tablefunc.is_some());
    rte_security_barrier::set(|run, root, rti| {
        planner_rt_fetch(run, root, rti).security_barrier
    });
    rte_values_lists_len::set(|run, root, rti| {
        planner_rt_fetch(run, root, rti).values_lists.len() as i32
    });
    rte_lateral::set(|run, root, rti| planner_rt_fetch(run, root, rti).lateral);
    rte_jointype::set(|run, root, rti| planner_rt_fetch(run, root, rti).jointype as JoinType);
    rte_enrtuples::set(|run, root, rti| planner_rt_fetch(run, root, rti).enrtuples);

    // costsize.c `set_rel_width` — `rte->relid` from a RelOptInfo handle. The
    // RelOptInfo's `relid` field is the 1-based RT index; resolve the RTE through
    // the same `planner_rt_fetch` store and project its `relid` (table OID).
    backend_optimizer_path_costsize_seams::rte_relid::set(|run, root, rel| {
        let rti = root.rel(rel).relid;
        planner_rt_fetch(run, root, rti).relid
    });

    // Query-level projections — `root->parse->...`.
    parse_rtable_len::set(|run, root| run.rtable(root.parse).len() as i32);
    parse_rteperminfos_len::set(|run, root| run.resolve(root.parse).rteperminfos.len() as i32);
    parse_rte::set(|_run, root, rti| root.simple_rte_array[rti as usize]);
}

extern crate alloc;

#[cfg(any(test, feature = "synthetic"))]
pub mod synthetic;

#[cfg(not(any(test, feature = "synthetic")))]
pub mod synthetic {
    //! Stub module so the public path exists in non-test builds; the real
    //! fixture lives behind `cfg(test)` / `feature = "synthetic"`.
}
