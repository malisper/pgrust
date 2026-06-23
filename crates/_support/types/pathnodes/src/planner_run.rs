//! `QueryId` → `Query<'mcx>` resolver — the planner-run query store.
//!
//! # Why this exists (the C model it renders)
//!
//! In C, [`PlannerInfo`](crate::PlannerInfo)`->parse` is a bare `Query *`; the
//! planner dereferences it directly to reach `parse->jointree`,
//! `parse->targetList`, `parse->rtable`, and walks the sub-`Query`s reached
//! through subquery range-table entries, sublink subselects, CTEs, and set-op
//! arms. A whole statement is therefore a *tree of aliased `Query` nodes*.
//!
//! This repo models `PlannerInfo->parse` as the opaque [`QueryId`] handle
//! (`crate::QueryId`) — the deliberate "inherited opacity" rendering of the
//! aliased `Query *` (see `pathnodes.h:216`). The handle alone is not
//! walkable: the prep layer (`prepjointree`/`preptlist`/`prepunion`/`prepagg`)
//! and every downstream walk that touches `root->parse` need to *resolve* the
//! handle back to a real [`Query<'mcx>`](crate::Query) to follow the tree.
//!
//! [`PlannerRun`] is that resolver. It is the safe-Rust rendering of the C
//! `Query *` deref: a store that holds **every** `Query` node of a statement,
//! keyed by [`QueryId`], exactly as [`crate::PlannerInfo::rel_arena`] holds
//! every [`RelOptInfo`](crate::RelOptInfo) keyed by [`RelId`](crate::RelId).
//!
//! # Why it is NOT a field of [`PlannerInfo`]
//!
//! The store holds `Query<'mcx>` values, so it is `'mcx`-scoped. [`PlannerInfo`]
//! and [`crate::PlannerGlobal`] are deliberately **lifetime-free** (handle
//! arenas of lifetime-free payloads) — putting an `'mcx` store inside them
//! would force `<'mcx>` across the ~2126 `PlannerInfo` use sites and break the
//! `#[derive(Default)]` those structs rely on. So the query store lives in a
//! separate **planner-run context** that the top-level planner entry owns for
//! the duration of one planner invocation; [`PlannerInfo::parse`] stays a bare
//! [`QueryId`]. Consumers receive `&PlannerRun<'mcx>` as an additive parameter
//! alongside `&mut PlannerInfo` and call [`PlannerRun::resolve`] to walk.
//!
//! [`PlannerInfo`]: crate::PlannerInfo
//! [`PlannerInfo::parse`]: crate::PlannerInfo::parse
//! [`QueryId`]: crate::QueryId

use mcx::{Mcx, PgVec};
use types_core::primitive::Index;
use nodes::copy_query::Query;
use nodes::nodelockrows::PlanRowMark;
use nodes::nodes::Node;
use nodes::parsenodes::{RangeTblEntry, RTEPermissionInfo};
use nodes::primnodes::TargetEntry;
use nodes::rawnodes::FromExpr;

use crate::{
    PathId, PlanId, PlanRowMarkId, PlannerInfo, QueryId, RangeTblEntryId, RtePermInfoId,
};

/// The query store for one planner invocation — the resolver behind every
/// [`QueryId`].
///
/// Holds the top [`Query`] plus every nested sub-`Query` (subquery-RTE
/// subqueries, sublink subselects, CTE queries, set-op arms) interned during
/// parse-analysis / planner setup, each addressed by the [`QueryId`] returned
/// from [`intern`](Self::intern). This mirrors the arena pattern of
/// [`crate::PlannerInfo::rel_arena`] / `path_arena` / `node_arena`, but lives
/// outside the lifetime-free [`crate::PlannerInfo`] because its payload carries
/// `'mcx`.
#[derive(Debug)]
pub struct PlannerRun<'mcx> {
    /// Backing store for every [`Query`]; a [`QueryId`] indexes here. Allocated
    /// in the planner-run [`Mcx`] so the interned `Query<'mcx>` subtrees and the
    /// store share one context lifetime.
    queries: PgVec<'mcx, Query<'mcx>>,
    /// Backing store for every [`RangeTblEntry`]; a [`RangeTblEntryId`] indexes
    /// here. This is the value resolver behind `PlannerInfo::simple_rte_array`
    /// (the `RangeTblEntry **` handle array) — see [`planner_rt_fetch`]. The
    /// parser produces a [`Query`] whose `rtable` is `PgVec<RangeTblEntry>`;
    /// the planner setup interns each into this store, recording the returned
    /// [`RangeTblEntryId`] in `simple_rte_array` keyed by RT index, exactly as C
    /// fills `simple_rte_array[rti] = rt_fetch(rti, parse->rtable)`.
    rtes: PgVec<'mcx, RangeTblEntry<'mcx>>,
    /// Backing store for every SubPlan's owned `Plan` tree — the value resolver
    /// behind `PlannerGlobal::subplans` (the C `List *subplans` of `Plan *`). A
    /// [`PlanId`] indexes here; `build_subplan`/`SS_process_ctes`/
    /// `SS_make_initplan_from_plan` intern the freshly built plan tree (an owned
    /// [`Node`] embedding its `Plan` base) and append the returned [`PlanId`] to
    /// `glob->subplans`, setting `splan->plan_id = subplans.len()` (1-based).
    /// `finalize_plan` reads back through [`planner_subplan_get_plan`] to compute
    /// each init/regular SubPlan's `extParam`.
    subplans: PgVec<'mcx, Node<'mcx>>,
    /// Backing store for every SubPlan's owned `PlannerInfo` (`glob->subroots`,
    /// C `List *subroots` of `PlannerInfo *`). Parallel to [`subplans`]: the
    /// `PlanId` from interning a subplan also keys its subroot. [`PlannerInfo`]
    /// is lifetime-free, so this could live on `glob` directly, but it is kept
    /// beside the subplan store so the parallel three-list structure interns
    /// atomically (and `glob` stays `Clone`/`Default`, which a `Vec<PlannerInfo>`
    /// would break — `PlannerInfo` is not `Clone`).
    ///
    /// [`subplans`]: Self::intern_subplan
    subroots: PgVec<'mcx, PlannerInfo>,
    /// Per-subplan source-path handle (`glob->subpaths`, C `List *subpaths` of
    /// `Path *`). Parallel to [`subplans`]; element `i` is the [`PathId`] (in the
    /// matching `subroots[i]`'s `path_arena`) the subplan's `Plan` was created
    /// from. C only ever appends this list; the handle is carried for fidelity.
    subpaths: PgVec<'mcx, PathId>,
    /// Backing store for every [`PlanRowMark`]; a [`PlanRowMarkId`] indexes here.
    /// This is the value resolver behind both `PlannerInfo::rowMarks` (the C
    /// per-query `List *rowMarks` of owned `PlanRowMark *`, built by
    /// `preprocess_rowmarks`) and `PlannerGlobal::finalrowmarks` (the flat-copied
    /// list `set_plan_references` builds) — see [`planner_rowmark_fetch`].
    /// `PlanRowMark` is a scalar-only struct, so a flat copy in setrefs is a
    /// plain `Copy` of the resolved value re-interned here. Allocated in the
    /// planner-run [`Mcx`] so the store shares the run's context lifetime, the
    /// same shape as the RTE store.
    rowmarks: PgVec<'mcx, PlanRowMark>,
    /// Backing store for every flat-copied [`RTEPermissionInfo`]; a
    /// [`RtePermInfoId`] indexes here. This is the value resolver behind
    /// `PlannerGlobal::finalrteperminfos` (C `List *finalrteperminfos` of owned
    /// `RTEPermissionInfo *`). `set_plan_references`'s `add_rte_to_flat_rtable`
    /// clones each source-query perminfo (`copyObject` over `RTEPermissionInfo`)
    /// into this store and appends the returned [`RtePermInfoId`] to
    /// `glob->finalrteperminfos`; `standard_planner` resolves the handles back
    /// into the finished `PlannedStmt::permInfos`. Same shape as the RTE/rowmark
    /// stores — allocated in the planner-run [`Mcx`], so the owned
    /// `RTEPermissionInfo<'mcx>` (with its `Bitmapset` columns) shares the run's
    /// context lifetime.
    rteperminfos: PgVec<'mcx, RTEPermissionInfo<'mcx>>,
    /// Backing store for each MIN/MAX aggregate's cloned-and-planned `subroot`
    /// `PlannerInfo` (planagg.c `build_minmax_path`). C keeps the subroot alive on
    /// `mminfo->subroot` from preprocess time until `create_minmaxagg_plan` calls
    /// `create_plan(subroot, mminfo->path)`. [`PlannerInfo`] is lifetime-free but
    /// not `Clone`, so the subroot value lives here and
    /// [`crate::MinMaxAggInfo::subroot_idx`] carries the index.
    minmax_subroots: PgVec<'mcx, PlannerInfo>,
}

impl<'mcx> PlannerRun<'mcx> {
    /// The planner-run [`Mcx`] this store was created in. Recovered from a
    /// backing store's allocator handle (every `PgVec` field is allocated in the
    /// same context), so a consumer holding only `&PlannerRun<'mcx>` can reach
    /// the run's allocator without it being threaded as a separate parameter —
    /// the safe-Rust analogue of allocating in the planner's per-query
    /// `MemoryContext` (which C reaches via the global `CurrentMemoryContext`).
    #[inline]
    pub fn mcx(&self) -> Mcx<'mcx> {
        *self.queries.allocator()
    }

    /// Create an empty query store in the given planner-run context.
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PlannerRun {
            queries: PgVec::new_in(mcx),
            rtes: PgVec::new_in(mcx),
            subplans: PgVec::new_in(mcx),
            subroots: PgVec::new_in(mcx),
            subpaths: PgVec::new_in(mcx),
            rowmarks: PgVec::new_in(mcx),
            rteperminfos: PgVec::new_in(mcx),
            minmax_subroots: PgVec::new_in(mcx),
        }
    }

    /// Intern a MIN/MAX aggregate's planned `subroot`, returning the index
    /// [`crate::MinMaxAggInfo::subroot_idx`] carries. Producer: planagg's
    /// `build_minmax_path`.
    #[inline]
    pub fn intern_minmax_subroot(&mut self, subroot: PlannerInfo) -> usize {
        let idx = self.minmax_subroots.len();
        self.minmax_subroots.push(subroot);
        idx
    }

    /// Move a MIN/MAX subroot out of the store (replacing it with a default), so
    /// `create_minmaxagg_plan` can hold it `&mut` to run `create_plan(subroot, …)`
    /// while also passing `&PlannerRun`. Pair with [`Self::put_minmax_subroot`].
    #[inline]
    pub fn take_minmax_subroot(&mut self, idx: usize) -> PlannerInfo {
        core::mem::take(&mut self.minmax_subroots[idx])
    }

    /// Restore a MIN/MAX subroot taken via [`Self::take_minmax_subroot`].
    #[inline]
    pub fn put_minmax_subroot(&mut self, idx: usize, subroot: PlannerInfo) {
        self.minmax_subroots[idx] = subroot;
    }

    /// Intern a [`Query`] into the store, returning the [`QueryId`] handle that
    /// resolves to it. The producer path: parse-analysis hands its owned top
    /// `Query` here and stores the resulting [`QueryId`] in
    /// [`crate::PlannerInfo::parse`]; each subquery-RTE / sublink / CTE / set-op
    /// sub-`Query` is interned the same way and its [`QueryId`] stored in the
    /// referencing node.
    #[inline]
    pub fn intern(&mut self, query: Query<'mcx>) -> QueryId {
        let id = QueryId(self.queries.len() as u32);
        self.queries.push(query);
        id
    }

    /// Resolve a [`QueryId`] to its [`Query`] — the safe-Rust rendering of the
    /// C `Query *` deref. Panics on an out-of-range handle (a handle never
    /// produced by [`intern`](Self::intern) is a planner bug, like indexing
    /// `rel_arena` past its end).
    #[inline]
    pub fn resolve(&self, id: QueryId) -> &Query<'mcx> {
        &self.queries[id.0 as usize]
    }

    /// Resolve a [`QueryId`] for mutation (the prep layer rewrites
    /// `parse->jointree` / `parse->targetList` in place).
    #[inline]
    pub fn resolve_mut(&mut self, id: QueryId) -> &mut Query<'mcx> {
        &mut self.queries[id.0 as usize]
    }

    /// Total number of interned queries.
    #[inline]
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// Whether the store holds no queries yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }

    /* --------------------------------------------------------------------
     * Subtree accessors — the (QueryId) -> &subtree views the prep/walk
     * consumers use instead of dereferencing a raw `Query *` field. Each
     * resolves the handle and projects the field, so a consumer holding only
     * a `QueryId` + `&PlannerRun` walks the parse tree exactly as C walks
     * `root->parse->jointree` etc.
     * ------------------------------------------------------------------ */

    /// `query->jointree` (`FromExpr *`) for the interned [`Query`]. `None` when
    /// the query has no FROM/WHERE tree (matches the nullable C field).
    #[inline]
    pub fn jointree(&self, id: QueryId) -> Option<&FromExpr<'mcx>> {
        self.resolve(id).jointree.as_deref()
    }

    /// `query->jointree` for mutation (prepjointree pull-up rewrites it).
    #[inline]
    pub fn jointree_mut(&mut self, id: QueryId) -> Option<&mut FromExpr<'mcx>> {
        self.resolve_mut(id).jointree.as_deref_mut()
    }

    /// `query->targetList` (`List *` of `TargetEntry`) for the interned
    /// [`Query`].
    #[inline]
    pub fn target_list(&self, id: QueryId) -> &[TargetEntry<'mcx>] {
        &self.resolve(id).targetList
    }

    /// `query->targetList` for mutation (preptlist expands it).
    #[inline]
    pub fn target_list_mut(&mut self, id: QueryId) -> &mut PgVec<'mcx, TargetEntry<'mcx>> {
        &mut self.resolve_mut(id).targetList
    }

    /// `query->rtable` (`List *` of `RangeTblEntry`) for the interned
    /// [`Query`].
    #[inline]
    pub fn rtable(&self, id: QueryId) -> &[RangeTblEntry<'mcx>] {
        &self.resolve(id).rtable
    }

    /// `query->rtable` for mutation (prepjointree/prepunion add RTEs).
    #[inline]
    pub fn rtable_mut(&mut self, id: QueryId) -> &mut PgVec<'mcx, RangeTblEntry<'mcx>> {
        &mut self.resolve_mut(id).rtable
    }

    /* --------------------------------------------------------------------
     * RangeTblEntry store — the value resolver behind
     * `PlannerInfo::simple_rte_array`. C carries `simple_rte_array` as a
     * `RangeTblEntry **` (each slot is an aliased `RangeTblEntry *`); this
     * repo carries it as `Vec<RangeTblEntryId>` opaque handles. The store
     * here is the safe-Rust rendering of those aliased pointers — exactly the
     * same intern/resolve shape as the `Query` store above and as
     * `PlannerInfo::rel_arena` (RelId -> RelOptInfo).
     *
     * The planner's `setup_simple_rel_arrays` (relnode.c) walks the top
     * `Query`'s `rtable` and fills `simple_rte_array[rti]`; in this model it
     * interns each `RangeTblEntry` here and records the returned
     * `RangeTblEntryId` in `simple_rte_array`. A scan converter then calls
     * [`planner_rt_fetch`] to recover the `&RangeTblEntry` for its RT index.
     * ------------------------------------------------------------------ */

    /// Intern a [`RangeTblEntry`] into the store, returning the
    /// [`RangeTblEntryId`] handle that resolves to it. The producer path:
    /// `setup_simple_rel_arrays` hands each top-level `rtable` entry here and
    /// records the returned id in `PlannerInfo::simple_rte_array` at the RT
    /// index (1-based slot; index 0 is the unused C placeholder).
    #[inline]
    pub fn intern_rte(&mut self, rte: RangeTblEntry<'mcx>) -> RangeTblEntryId {
        let id = RangeTblEntryId(self.rtes.len() as u32);
        self.rtes.push(rte);
        id
    }

    /// Resolve a [`RangeTblEntryId`] to its [`RangeTblEntry`] — the safe-Rust
    /// rendering of a `RangeTblEntry *` deref. Panics on an out-of-range handle
    /// (a handle never produced by [`intern_rte`](Self::intern_rte) is a planner
    /// bug, like indexing past `rel_arena`'s end).
    #[inline]
    pub fn resolve_rte(&self, id: RangeTblEntryId) -> &RangeTblEntry<'mcx> {
        &self.rtes[id.0 as usize]
    }

    /// Resolve a [`RangeTblEntryId`] for mutation (planner passes mutate
    /// `inh`/`lateral`/`securityQuals` etc. on the in-place RTE).
    #[inline]
    pub fn resolve_rte_mut(&mut self, id: RangeTblEntryId) -> &mut RangeTblEntry<'mcx> {
        &mut self.rtes[id.0 as usize]
    }

    /// Number of interned range-table entries.
    #[inline]
    pub fn rte_len(&self) -> usize {
        self.rtes.len()
    }

    /* --------------------------------------------------------------------
     * PlanRowMark store — the value resolver behind `PlannerInfo::rowMarks`
     * and `PlannerGlobal::finalrowmarks`. C keeps both as `List *` of owned
     * `PlanRowMark *`: `preprocess_rowmarks` (planmain.c) builds the per-query
     * list via `makeNode(PlanRowMark)` + `lappend(root->rowMarks, ...)`, and
     * `set_plan_references` (setrefs.c:305-323) walks `root->rowMarks`,
     * flat-copies each into `glob->finalrowmarks`. Those values pin to the
     * arena world that `PlannerInfo`/`PlannerGlobal` deliberately keep
     * lifetime-free, so they live here and the lists carry `PlanRowMarkId`
     * handles — exactly the intern/resolve shape of the RTE store above.
     * ------------------------------------------------------------------ */

    /// Intern a [`PlanRowMark`] into the store, returning the [`PlanRowMarkId`]
    /// handle that resolves to it. The producer paths: `preprocess_rowmarks`
    /// hands each freshly built `PlanRowMark` here and appends the returned id
    /// to `PlannerInfo::rowMarks`; `set_plan_references` re-interns each
    /// flat-copied rowmark and appends to `PlannerGlobal::finalrowmarks`.
    #[inline]
    pub fn intern_rowmark(&mut self, rowmark: PlanRowMark) -> PlanRowMarkId {
        let id = PlanRowMarkId(self.rowmarks.len() as u32);
        self.rowmarks.push(rowmark);
        id
    }

    /// Resolve a [`PlanRowMarkId`] to its [`PlanRowMark`] — the safe-Rust
    /// rendering of a `PlanRowMark *` deref. Panics on an out-of-range handle
    /// (a handle never produced by [`intern_rowmark`](Self::intern_rowmark) is a
    /// planner bug, like indexing past `rel_arena`'s end).
    #[inline]
    pub fn resolve_rowmark(&self, id: PlanRowMarkId) -> &PlanRowMark {
        &self.rowmarks[id.0 as usize]
    }

    /// Resolve a [`PlanRowMarkId`] for mutation (`preprocess_rowmarks` ORs
    /// `allMarkTypes` and sets `isParent` on existing entries while building the
    /// list; setrefs renumbers `rti`/`prti` by `rtoffset`).
    #[inline]
    pub fn resolve_rowmark_mut(&mut self, id: PlanRowMarkId) -> &mut PlanRowMark {
        &mut self.rowmarks[id.0 as usize]
    }

    /// Number of interned [`PlanRowMark`]s.
    #[inline]
    pub fn rowmark_len(&self) -> usize {
        self.rowmarks.len()
    }

    /// Intern an [`RTEPermissionInfo`] into the store, returning the
    /// [`RtePermInfoId`] handle that resolves to it. The producer path:
    /// `set_plan_references`'s `add_rte_to_flat_rtable` clones each source-query
    /// perminfo (`addRTEPermissionInfo`'s `copyObject`) here and appends the
    /// returned id to `PlannerGlobal::finalrteperminfos`.
    #[inline]
    pub fn intern_rte_perminfo(&mut self, perminfo: RTEPermissionInfo<'mcx>) -> RtePermInfoId {
        let id = RtePermInfoId(self.rteperminfos.len() as u32);
        self.rteperminfos.push(perminfo);
        id
    }

    /// Resolve a [`RtePermInfoId`] to its [`RTEPermissionInfo`] — the safe-Rust
    /// rendering of an `RTEPermissionInfo *` deref. Panics on an out-of-range
    /// handle (a handle never produced by
    /// [`intern_rte_perminfo`](Self::intern_rte_perminfo) is a planner bug).
    #[inline]
    pub fn resolve_rte_perminfo(&self, id: RtePermInfoId) -> &RTEPermissionInfo<'mcx> {
        &self.rteperminfos[id.0 as usize]
    }

    /// Number of interned [`RTEPermissionInfo`]s.
    #[inline]
    pub fn rte_perminfo_len(&self) -> usize {
        self.rteperminfos.len()
    }

    /* --------------------------------------------------------------------
     * SubPlan stores — the value resolver behind `PlannerGlobal::subplans` /
     * `subroots` / `subpaths`. C keeps three parallel `glob` lists of owned
     * `Plan *` / `PlannerInfo *` / `Path *`; a `SubPlan`'s 1-based `plan_id`
     * indexes all three. Those owned values pin to `'mcx`, but `PlannerGlobal`
     * is lifetime-free — so the values live here and `glob` carries `PlanId`
     * handles, exactly the intern/resolve shape of the RTE store above.
     * ------------------------------------------------------------------ */

    /// Intern one SubPlan's three parallel values — the freshly built `Plan`
    /// tree (`plan`, an owned [`Node`]), its per-subplan `subroot`
    /// ([`PlannerInfo`]), and the `subpath` [`PathId`] it was made from —
    /// returning the [`PlanId`] handle that keys all three.
    ///
    /// The producer path (subselect.c `build_subplan`): after `create_plan`
    /// yields the subquery's plan, C does
    /// `glob->subplans = lappend(glob->subplans, plan); glob->subroots =
    /// lappend(glob->subroots, subroot); glob->subpaths = lappend(...);
    /// splan->plan_id = list_length(glob->subplans);`. Here the caller interns
    /// the three values, pushes the returned [`PlanId`] onto `glob.subplans`
    /// (and `subroots`/`subpaths`), and sets the `SubPlan`'s 1-based
    /// `plan_id = glob.subplans.len()` (== `PlanId + 1`).
    #[inline]
    pub fn intern_subplan(
        &mut self,
        plan: Node<'mcx>,
        subroot: PlannerInfo,
        subpath: PathId,
    ) -> PlanId {
        let id = PlanId(self.subplans.len() as u32);
        self.subplans.push(plan);
        self.subroots.push(subroot);
        self.subpaths.push(subpath);
        id
    }

    /// Resolve a [`PlanId`] to its owned `Plan` tree — the safe-Rust rendering
    /// of `list_nth(glob->subplans, plan_id - 1)`. Panics on an out-of-range
    /// handle (a handle never produced by [`intern_subplan`](Self::intern_subplan)
    /// is a planner bug).
    #[inline]
    pub fn resolve_subplan(&self, id: PlanId) -> &Node<'mcx> {
        &self.subplans[id.0 as usize]
    }

    /// Resolve a [`PlanId`] to its owned `Plan` tree for mutation (setrefs.c
    /// `set_plan_references` rewrites the subplan in place; `finalize_plan`
    /// stores back the computed `extParam`/`allParam`).
    #[inline]
    pub fn resolve_subplan_mut(&mut self, id: PlanId) -> &mut Node<'mcx> {
        &mut self.subplans[id.0 as usize]
    }

    /// Move a subplan's owned `Plan` tree out of the store (replacing it with a
    /// throwaway placeholder), so the caller can hold it owned while taking a
    /// shared borrow of the run (e.g. the subplan's `subroot` + the recursion's
    /// `&PlannerRun` in `SS_finalize_plan` / `set_plan_references`, which can't
    /// coexist with `&mut self.subplans[..]`). Pair with [`put_subplan`].
    #[inline]
    pub fn take_subplan(&mut self, id: PlanId) -> types_error::PgResult<Node<'mcx>> {
        let placeholder = Node::mk_range_tbl_ref(self.mcx(), nodes::rawnodes::RangeTblRef { rtindex: 0 })?;
        Ok(core::mem::replace(&mut self.subplans[id.0 as usize], placeholder))
    }

    /// Restore a subplan's owned `Plan` tree taken via [`take_subplan`].
    #[inline]
    pub fn put_subplan(&mut self, id: PlanId, plan: Node<'mcx>) {
        self.subplans[id.0 as usize] = plan;
    }

    /// Move a subplan's `subroot` [`PlannerInfo`] out of the store (replacing it
    /// with a default), so the caller can hold it `&mut` (e.g. to lend it the
    /// shared parent `glob`) while also passing `&mut PlannerRun` to
    /// `set_plan_references`. Pair with [`put_subroot`].
    #[inline]
    pub fn take_subroot(&mut self, id: PlanId) -> PlannerInfo {
        core::mem::take(&mut self.subroots[id.0 as usize])
    }

    /// Restore a subplan's `subroot` taken via [`take_subroot`].
    #[inline]
    pub fn put_subroot(&mut self, id: PlanId, subroot: PlannerInfo) {
        self.subroots[id.0 as usize] = subroot;
    }

    /// Resolve a [`PlanId`] to its per-subplan `subroot` [`PlannerInfo`]
    /// (`list_nth(glob->subroots, plan_id - 1)`). `SS_finalize_plan` reads each
    /// subplan's subroot while finalizing.
    #[inline]
    pub fn resolve_subroot(&self, id: PlanId) -> &PlannerInfo {
        &self.subroots[id.0 as usize]
    }

    /// Resolve a [`PlanId`] to its per-subplan `subroot` for mutation.
    #[inline]
    pub fn resolve_subroot_mut(&mut self, id: PlanId) -> &mut PlannerInfo {
        &mut self.subroots[id.0 as usize]
    }

    /// Resolve a [`PlanId`] to the `subpath` [`PathId`] the subplan was made
    /// from (`list_nth(glob->subpaths, plan_id - 1)`).
    #[inline]
    pub fn resolve_subpath(&self, id: PlanId) -> PathId {
        self.subpaths[id.0 as usize]
    }

    /// Number of interned subplans (== `list_length(glob->subplans)`; the next
    /// `SubPlan`'s 1-based `plan_id`).
    #[inline]
    pub fn subplan_len(&self) -> usize {
        self.subplans.len()
    }
}

/// `planner_rt_fetch(rti, root)` (pathnodes.h:594) — fetch the
/// [`RangeTblEntry`] at range-table index `rti`.
///
/// C macro:
/// ```c
/// #define planner_rt_fetch(rti, root) \
///     ((root)->simple_rte_array ? (root)->simple_rte_array[rti] : \
///      rt_fetch(rti, (root)->parse->rtable))
/// ```
///
/// The C macro has two legs because `simple_rte_array` may not be set up yet
/// (before `setup_simple_rel_arrays` / outside `query_planner`); in that case it
/// falls back to `rt_fetch(rti, parse->rtable)` — `list_nth(rtable, rti-1)`.
/// `createplan.c`'s scan converters run well after `query_planner`, where
/// `simple_rte_array` is always prepared, so they exercise only the first leg;
/// we render both faithfully: the array leg resolves the handle through the
/// run's RTE store, and the fallback leg resolves `root->parse` to its `Query`
/// and indexes `rtable[rti-1]` (1-based, matching `rt_fetch`).
///
/// Panics on an out-of-range `rti` (the C `rt_fetch` likewise "crashes and
/// burns if handed an out-of-range RT index").
#[inline]
pub fn planner_rt_fetch<'a, 'mcx>(
    run: &'a PlannerRun<'mcx>,
    root: &PlannerInfo,
    rti: Index,
) -> &'a RangeTblEntry<'mcx> {
    if !root.simple_rte_array.is_empty() {
        // simple_rte_array leg: simple_rte_array[rti] -> RangeTblEntryId -> RTE.
        let id = root.simple_rte_array[rti as usize];
        run.resolve_rte(id)
    } else {
        // rt_fetch fallback: list_nth(parse->rtable, rti-1).
        &run.rtable(root.parse)[(rti as usize) - 1]
    }
}

/// Fetch the [`PlanRowMark`] at position `idx` of `root->rowMarks` — the
/// safe-Rust rendering of `(PlanRowMark *) list_nth(root->rowMarks, idx)`.
///
/// `set_plan_references` (setrefs.c:305-323) walks `root->rowMarks` by list
/// position, reading each rowmark's `rti`/`prti`/`rowmarkId`/`markType` to
/// flat-copy it into `glob->finalrowmarks`; `preprocess_targetlist` (preptlist)
/// likewise iterates the list to build resjunk Vars. This projects
/// `root.rowMarks[idx]` to its [`PlanRowMarkId`] and resolves the owned
/// [`PlanRowMark`] through the run's rowmark store. Panics on an out-of-range
/// `idx` (a position past the list end is a planner bug, like the C `list_nth`
/// past the list end).
#[inline]
pub fn planner_rowmark_fetch<'a, 'mcx>(
    run: &'a PlannerRun<'mcx>,
    root: &PlannerInfo,
    idx: usize,
) -> &'a PlanRowMark {
    let id = root.rowMarks[idx];
    run.resolve_rowmark(id)
}

/// `planner_subplan_get_plan(root, subplan)` (subselect.c) — fetch the owned
/// `Plan` tree a [`SubPlan`](nodes::primnodes::SubPlan) refers to.
///
/// C inline:
/// ```c
/// static inline Plan *
/// planner_subplan_get_plan(PlannerInfo *root, SubPlan *subplan)
/// {
///     return (Plan *) list_nth(root->glob->subplans, subplan->plan_id - 1);
/// }
/// ```
///
/// `finalize_plan` / `SS_finalize_plan` call this to walk a child SubPlan's plan
/// tree and OR its `extParam` into the parent's. The `SubPlan`'s `plan_id` is
/// 1-based; `glob->subplans` carries [`PlanId`] handles into the run's owned
/// subplan store, so this projects `glob.subplans[plan_id - 1]` to its [`PlanId`]
/// and resolves the owned [`Node`] through the run. Panics on an out-of-range
/// `plan_id` (an unset/garbage `plan_id` is a planner bug, like the C
/// `list_nth` past the list end).
#[inline]
pub fn planner_subplan_get_plan<'a, 'mcx>(
    run: &'a PlannerRun<'mcx>,
    root: &PlannerInfo,
    plan_id: i32,
) -> &'a Node<'mcx> {
    // `glob->subplans` is a shared, append-only list parallel to the run's
    // subplan store: `intern_subplan` returns `PlanId(len)` and the same handle
    // is pushed onto `glob.subplans`, so `glob.subplans[plan_id - 1] ==
    // PlanId(plan_id - 1)`. C shares one `glob` across the parent root and every
    // subroot; this owned model moves the glob onto the top root only (a stashed
    // subquery subroot has `glob == None`). When `finalize_plan` recurses into a
    // SubqueryScan with its subroot (C: `finalize_plan(rel->subroot, ...)`), the
    // subroot has no glob — resolve directly against the run, which holds the
    // same shared subplan store. Equivalent for any root.
    match root.glob.as_ref() {
        Some(glob) => {
            let id = glob.subplans[(plan_id as usize) - 1];
            run.resolve_subplan(id)
        }
        None => run.resolve_subplan(PlanId((plan_id - 1) as u32)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use nodes::nodes::{ntag, CmdType};

    #[test]
    fn intern_resolve_round_trips_many_queries() {
        // A statement is a tree of nested Query nodes; the store holds many,
        // each keyed by the QueryId intern() hands back (the rel_arena/RelId
        // model). Distinct interns get distinct, dense, in-order ids.
        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());
        assert!(run.is_empty());

        let mut q0 = Query::new(cx.mcx());
        q0.commandType = CmdType::CMD_SELECT;
        let mut q1 = Query::new(cx.mcx());
        q1.commandType = CmdType::CMD_INSERT;

        let id0 = run.intern(q0);
        let id1 = run.intern(q1);
        assert_eq!(id0, QueryId(0));
        assert_eq!(id1, QueryId(1));
        assert_eq!(run.len(), 2);

        // resolve() is the safe-Rust `Query *` deref: handle -> &Query.
        assert_eq!(run.resolve(id0).commandType, CmdType::CMD_SELECT);
        assert_eq!(run.resolve(id1).commandType, CmdType::CMD_INSERT);
    }

    #[test]
    fn wires_into_planner_info_parse() {
        // The wiring point: parse-analysis hands its owned Query to intern(),
        // the resulting QueryId is stored in the lifetime-free PlannerInfo.parse
        // (which stays a bare handle — no `'mcx` forced onto PlannerInfo), and a
        // consumer resolves it back through the run context.
        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());

        let mut top = Query::new(cx.mcx());
        top.commandType = CmdType::CMD_UPDATE;

        let mut root = crate::PlannerInfo::default();
        root.parse = run.intern(top);

        // Downstream walk: holding only `root.parse` + `&run`, reach the Query.
        let parsed = run.resolve(root.parse);
        assert_eq!(parsed.commandType, CmdType::CMD_UPDATE);
    }

    #[test]
    fn subtree_accessors_project_and_mutate() {
        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());

        let q = Query::new(cx.mcx());
        let id = run.intern(q);

        // A fresh Query has an empty range table and target list (NIL) and a
        // NULL jointree, exactly as makeNode would leave them.
        assert!(run.jointree(id).is_none());
        assert!(run.target_list(id).is_empty());
        assert!(run.rtable(id).is_empty());

        // resolve_mut threads back to the same node (in-place prep rewrites).
        run.resolve_mut(id).commandType = CmdType::CMD_DELETE;
        assert_eq!(run.resolve(id).commandType, CmdType::CMD_DELETE);
    }

    #[test]
    fn rte_intern_resolve_round_trips() {
        // The RTE store is the value resolver behind simple_rte_array's
        // RangeTblEntryId handles; intern hands back dense, in-order ids.
        use nodes::parsenodes::{RangeTblEntry, RTEKind};

        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());
        assert_eq!(run.rte_len(), 0);

        let mut r0 = RangeTblEntry::new_in(cx.mcx());
        r0.rtekind = RTEKind::RTE_RELATION;
        let mut r1 = RangeTblEntry::new_in(cx.mcx());
        r1.rtekind = RTEKind::RTE_FUNCTION;

        let id0 = run.intern_rte(r0);
        let id1 = run.intern_rte(r1);
        assert_eq!(id0, RangeTblEntryId(0));
        assert_eq!(id1, RangeTblEntryId(1));
        assert_eq!(run.rte_len(), 2);

        // resolve_rte is the safe-Rust `RangeTblEntry *` deref.
        assert_eq!(run.resolve_rte(id0).rtekind, RTEKind::RTE_RELATION);
        assert_eq!(run.resolve_rte(id1).rtekind, RTEKind::RTE_FUNCTION);

        // resolve_rte_mut threads back to the same node.
        run.resolve_rte_mut(id0).rtekind = RTEKind::RTE_VALUES;
        assert_eq!(run.resolve_rte(id0).rtekind, RTEKind::RTE_VALUES);
    }

    #[test]
    fn rowmark_intern_resolve_and_fetch() {
        // The rowmark store is the value resolver behind PlannerInfo::rowMarks /
        // PlannerGlobal::finalrowmarks; intern hands back dense, in-order ids and
        // planner_rowmark_fetch walks root.rowMarks by list position.
        use nodes::nodelockrows::{PlanRowMark, ROW_MARK_COPY, ROW_MARK_EXCLUSIVE};

        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());
        assert_eq!(run.rowmark_len(), 0);

        let mut rm0 = PlanRowMark::default();
        rm0.rti = 1;
        rm0.markType = ROW_MARK_EXCLUSIVE;
        let mut rm1 = PlanRowMark::default();
        rm1.rti = 2;
        rm1.markType = ROW_MARK_COPY;

        let id0 = run.intern_rowmark(rm0);
        let id1 = run.intern_rowmark(rm1);
        assert_eq!(id0, PlanRowMarkId(0));
        assert_eq!(id1, PlanRowMarkId(1));
        assert_eq!(run.rowmark_len(), 2);

        // resolve_rowmark is the safe-Rust `PlanRowMark *` deref.
        assert_eq!(run.resolve_rowmark(id0).rti, 1);
        assert_eq!(run.resolve_rowmark(id1).markType, ROW_MARK_COPY);

        // resolve_rowmark_mut threads back (preprocess_rowmarks ORs allMarkTypes).
        run.resolve_rowmark_mut(id0).allMarkTypes = 7;
        assert_eq!(run.resolve_rowmark(id0).allMarkTypes, 7);

        // root.rowMarks carries the handles by list position; setrefs walks them
        // with planner_rowmark_fetch and flat-copies into glob.finalrowmarks.
        let mut root = crate::PlannerInfo::default();
        root.rowMarks = alloc::vec![id0, id1];
        assert_eq!(planner_rowmark_fetch(&run, &root, 0).rti, 1);
        assert_eq!(planner_rowmark_fetch(&run, &root, 1).rti, 2);

        // setrefs' flat-copy: read each rowmark, Copy it, re-intern into the
        // store and append the new handle to glob.finalrowmarks.
        let mut glob = crate::PlannerGlobal::default();
        for idx in 0..root.rowMarks.len() {
            let flat = *planner_rowmark_fetch(&run, &root, idx);
            let fid = run.intern_rowmark(flat);
            glob.finalrowmarks.push(fid);
        }
        assert_eq!(glob.finalrowmarks.len(), 2);
        assert_eq!(run.resolve_rowmark(glob.finalrowmarks[0]).rti, 1);
    }

    #[test]
    fn planner_rt_fetch_array_leg() {
        // The simple_rte_array leg: setup_simple_rel_arrays interned each RTE
        // and recorded its id in simple_rte_array keyed by RT index (slot 0 is
        // the unused C placeholder; rti 1 is the first real entry).
        use nodes::parsenodes::{RangeTblEntry, RTEKind};

        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());

        let mut rte = RangeTblEntry::new_in(cx.mcx());
        rte.rtekind = RTEKind::RTE_FUNCTION;
        let id = run.intern_rte(rte);

        let mut root = crate::PlannerInfo::default();
        // simple_rte_array[0] unused, [1] -> our function RTE.
        root.simple_rte_array = alloc::vec![RangeTblEntryId(0), id];

        // A scan converter fetches its RT index's RTE through the run store.
        let fetched = planner_rt_fetch(&run, &root, 1);
        assert_eq!(fetched.rtekind, RTEKind::RTE_FUNCTION);
    }

    #[test]
    fn subplan_intern_resolve_and_get_plan() {
        // glob->subplans is the owned-Plan list a SubPlan's 1-based plan_id
        // indexes; build_subplan interns the plan tree + subroot + subpath,
        // appends the PlanId to glob.subplans, and sets plan_id = len. finalize
        // reads it back through planner_subplan_get_plan.
        use nodes::noderesult::Result as ResultPlan;

        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());
        assert_eq!(run.subplan_len(), 0);

        // A built subquery plan tree (a Result node, plan_node_id tagged so we
        // can recognize it after resolution).
        let mut r = ResultPlan::default();
        r.plan.plan_node_id = 42;
        let plan = Node::mk_result(run.mcx(), r).unwrap();

        let subroot = crate::PlannerInfo::default();
        let id = run.intern_subplan(plan, subroot, crate::PathId(7));
        assert_eq!(id, crate::PlanId(0));
        assert_eq!(run.subplan_len(), 1);

        // The producer pushes the handle onto glob.subplans and sets the
        // SubPlan's 1-based plan_id.
        let mut glob = crate::PlannerGlobal::default();
        glob.subplans.push(id);
        glob.subroots.push(id);
        glob.subpaths.push(id);
        let plan_id = glob.subplans.len() as i32; // == 1

        let mut root = crate::PlannerInfo::default();
        root.glob = Some(alloc::boxed::Box::new(glob));

        // finalize_plan's deref: planner_subplan_get_plan(root, plan_id) ->
        // &Node, the owned plan tree.
        let fetched = planner_subplan_get_plan(&run, &root, plan_id);
        match fetched.node_tag() {
            ntag::T_Result => assert_eq!(fetched.expect_result().plan.plan_node_id, 42),
            _ => panic!("expected the interned Result plan"),
        }

        // Parallel stores resolve off the same PlanId.
        assert_eq!(run.resolve_subpath(id), crate::PathId(7));
        let _ = run.resolve_subroot(id);
        // resolve_subplan_mut threads back (setrefs rewrites in place).
        if let Some(res) = run.resolve_subplan_mut(id).as_result_mut() {
            res.plan.plan_node_id = 99;
        }
        if let Some(res) = run.resolve_subplan(id).as_result() {
            assert_eq!(res.plan.plan_node_id, 99);
        }
    }

    #[test]
    fn planner_rt_fetch_rtfetch_fallback() {
        // The rt_fetch fallback leg (simple_rte_array empty): resolve
        // root->parse to its Query and index rtable[rti-1] (1-based).
        use nodes::parsenodes::{RangeTblEntry, RTEKind};

        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());

        let mut top = Query::new(cx.mcx());
        let mut rte = RangeTblEntry::new_in(cx.mcx());
        rte.rtekind = RTEKind::RTE_VALUES;
        top.rtable.push(rte);
        let qid = run.intern(top);

        let mut root = crate::PlannerInfo::default();
        root.parse = qid;
        // simple_rte_array left empty -> macro's else branch.
        assert!(root.simple_rte_array.is_empty());

        let fetched = planner_rt_fetch(&run, &root, 1);
        assert_eq!(fetched.rtekind, RTEKind::RTE_VALUES);
    }
}
