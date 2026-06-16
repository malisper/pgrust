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
use types_nodes::copy_query::Query;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::RangeTblEntry;
use types_nodes::primnodes::TargetEntry;
use types_nodes::rawnodes::FromExpr;

use crate::{PathId, PlanId, PlannerInfo, QueryId, RangeTblEntryId};

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
}

impl<'mcx> PlannerRun<'mcx> {
    /// Create an empty query store in the given planner-run context.
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PlannerRun {
            queries: PgVec::new_in(mcx),
            rtes: PgVec::new_in(mcx),
            subplans: PgVec::new_in(mcx),
            subroots: PgVec::new_in(mcx),
            subpaths: PgVec::new_in(mcx),
        }
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

/// `planner_subplan_get_plan(root, subplan)` (subselect.c) — fetch the owned
/// `Plan` tree a [`SubPlan`](types_nodes::primnodes::SubPlan) refers to.
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
    let glob = root
        .glob
        .as_ref()
        .expect("planner_subplan_get_plan: root->glob is NULL");
    let id = glob.subplans[(plan_id as usize) - 1];
    run.resolve_subplan(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_nodes::nodes::CmdType;

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
        use types_nodes::parsenodes::{RangeTblEntry, RTEKind};

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
    fn planner_rt_fetch_array_leg() {
        // The simple_rte_array leg: setup_simple_rel_arrays interned each RTE
        // and recorded its id in simple_rte_array keyed by RT index (slot 0 is
        // the unused C placeholder; rti 1 is the first real entry).
        use types_nodes::parsenodes::{RangeTblEntry, RTEKind};

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
        use types_nodes::noderesult::Result as ResultPlan;

        let cx = MemoryContext::new("planner-run");
        let mut run = PlannerRun::new(cx.mcx());
        assert_eq!(run.subplan_len(), 0);

        // A built subquery plan tree (a Result node, plan_node_id tagged so we
        // can recognize it after resolution).
        let mut r = ResultPlan::default();
        r.plan.plan_node_id = 42;
        let plan = Node::Result(r);

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
        match fetched {
            Node::Result(res) => assert_eq!(res.plan.plan_node_id, 42),
            _ => panic!("expected the interned Result plan"),
        }

        // Parallel stores resolve off the same PlanId.
        assert_eq!(run.resolve_subpath(id), crate::PathId(7));
        let _ = run.resolve_subroot(id);
        // resolve_subplan_mut threads back (setrefs rewrites in place).
        if let Node::Result(res) = run.resolve_subplan_mut(id) {
            res.plan.plan_node_id = 99;
        }
        if let Node::Result(res) = run.resolve_subplan(id) {
            assert_eq!(res.plan.plan_node_id, 99);
        }
    }

    #[test]
    fn planner_rt_fetch_rtfetch_fallback() {
        // The rt_fetch fallback leg (simple_rte_array empty): resolve
        // root->parse to its Query and index rtable[rti-1] (1-based).
        use types_nodes::parsenodes::{RangeTblEntry, RTEKind};

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
