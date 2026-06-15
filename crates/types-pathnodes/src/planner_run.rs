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
use types_nodes::copy_query::Query;
use types_nodes::parsenodes::RangeTblEntry;
use types_nodes::primnodes::TargetEntry;
use types_nodes::rawnodes::FromExpr;

use crate::QueryId;

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
}

impl<'mcx> PlannerRun<'mcx> {
    /// Create an empty query store in the given planner-run context.
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PlannerRun {
            queries: PgVec::new_in(mcx),
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
}
