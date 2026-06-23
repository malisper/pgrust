//! `backend-optimizer-plan-small` — `src/backend/optimizer/plan/planmain.c`.
//!
//! "What's in a name, anyway?" — the top-level planner entry point lives in
//! `planner.c`; this file is the main code for planning one *basic* join
//! operation, shorn of subselects, inheritance, aggregates and grouping. Its
//! single public routine is [`query_planner`].
//!
//! # Owned-tree / arena model
//!
//! C `query_planner(root, qp_callback, qp_extra)` mutates `*root` in place and
//! returns a `RelOptInfo *`. Here:
//!
//! * `root->parse` is the opaque [`QueryId`]; the jointree it reaches
//!   (`parse->jointree`, a `FromExpr`) is resolved through the planner-run
//!   store [`PlannerRun`], passed alongside `&mut PlannerInfo` (the resolver
//!   model, #264). `query_planner` therefore takes `run: &mut PlannerRun<'mcx>`,
//!   because `setup_simple_rel_arrays` (relnode.c) now interns the top-level
//!   `rtable` entries into the run's RTE store (#300), recording each handle in
//!   `root.simple_rte_array`; all other uses of `run` are immutable reborrows.
//! * The returned rel is a [`RelId`] handle into `root.rel_arena` (the trivial
//!   path's rel is registered by `build_simple_rel`; the general path's by
//!   `make_one_rel`).
//! * `qp_callback`/`qp_extra` — the C function-pointer upcall that computes
//!   `query_pathkeys` once ECs are canonical — is a real Rust closure
//!   (`&mut dyn FnMut`); the caller (`grouping_planner`/`planner.c`) passes its
//!   own closure exactly as C passes the function pointer.
//!
//! # What is built vs seam-and-panic
//!
//! The trivial single-`RTE_RESULT` fast path (`SELECT expression`,
//! `INSERT ... VALUES()`) is built end to end over already-landed callees
//! (relnode / pathnode / clauses / equivclass). The general join path calls
//! eleven unported `initsplan.c` / `analyzejoins.c` / `appendinfo.c` steps
//! (`add_base_rels_to_query`, … `make_one_rel` aside); each is reached through
//! its owner's `-seams` crate and panics loudly until that owner lands — never
//! a silent stub, mirroring the absent-subsystem boundary.

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::nodes::nodes::Node;
use pathnodes::{NodeId, PlannerInfo, RelId, RTEKind};
use ::pathnodes::planner_run::PlannerRun;

use allpaths as allpaths;
use equivclass as equivclass;
use clauses as clauses;
use joininfo as joininfo;
use pathnode as pathnode;
use relnode as relnode;

use rte_seams as rte;

use init_subselect_seams as analyzejoins_seam;
use plan_small_seams as initsplan_seam;
use appendinfo_seams as appendinfo_seam;

/// `RTE_RESULT` (`parsenodes.h` `RTEKind`): an empty FROM clause, value 8.
const RTE_RESULT: RTEKind = 8;

/// `PROPARALLEL_SAFE` (`pg_proc.h`, `'s'`). Used by the trivial-path parallel
/// short-circuit, matching `clauses.c is_parallel_safe`.
const PROPARALLEL_SAFE: i8 = b's' as i8;

/// `query_planner`
///
/// Generate a path (that is, a simplified plan) for a basic query, which may
/// involve joins but not any fancier features. Returns the [`RelId`] for the
/// top level of joining; the caller (`grouping_planner`) chooses among the
/// surviving paths.
///
/// `run` resolves `root.parse` (the opaque [`QueryId`]) to its owned
/// `Query<'mcx>` so the jointree can be walked, and (via `&mut`)
/// `setup_simple_rel_arrays` interns its `rtable` into the run's RTE store.
/// `qp_callback` is the C
/// `query_pathkeys_callback` upcall (computes `query_pathkeys` once ECs are
/// canonical); the caller owns the closure.
pub fn query_planner<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    qp_callback: &mut dyn FnMut(&mut PlannerInfo) -> PgResult<()>,
) -> PgResult<RelId> {
    /*
     * Init planner lists to empty.
     *
     * NOTE: append_rel_list was set up by subquery_planner, so do not touch
     * here.
     */
    root.join_rel_list = Vec::new();
    root.join_rel_hash = None;
    root.join_rel_level = Vec::new();
    root.join_cur_level = 0;
    root.canon_pathkeys = Vec::new();
    root.left_join_clauses = Vec::new();
    root.right_join_clauses = Vec::new();
    root.full_join_clauses = Vec::new();
    root.join_info_list = Vec::new();
    root.placeholder_list = Vec::new();
    root.placeholder_array = Vec::new();
    root.placeholder_array_size = 0;
    root.fkey_list = Vec::new();
    root.initial_rels = Vec::new();

    /*
     * Set up arrays for accessing base relations and AppendRelInfos.
     */
    relnode::setup_simple_rel_arrays(run, root, mcx)?;

    /*
     * In the trivial case where the jointree is a single RTE_RESULT relation,
     * bypass all the rest of this function and just make a RelOptInfo and its
     * one access path.  This is worth optimizing because it applies for common
     * cases like "SELECT expression" and "INSERT ... VALUES()".
     */
    // Assert(parse->jointree->fromlist != NIL); plus the trivial-path test.
    let trivial_varno = trivial_path_varno(jointree_of(run, root));

    if let Some(varno) = trivial_varno {
        // int varno = ((RangeTblRef *) jtnode)->rtindex;
        // RangeTblEntry *rte = root->simple_rte_array[varno]; Assert(rte != NULL);
        let rtekind = rte::rte_rtekind::call(run, root, varno as u32);

        if rtekind == RTE_RESULT {
            // Make the RelOptInfo for it directly. build_simple_rel registers it
            // in root.simple_rel_array[varno]; that handle is our final_rel.
            let final_rel = relnode::build_simple_rel(run, root, varno, None)?;

            /*
             * If query allows parallelism in general, check whether the quals
             * are parallel-restricted.  (We need not check final_rel->reltarget
             * because it's empty at this point.  Anything parallel-restricted in
             * the query tlist will be dealt with later.)  We should always do
             * this in a subquery, since it might be useful to use the subquery
             * in parallel paths in the parent level.  At top level this is
             * normally not worth the cycles, because a Result-only plan would
             * never be interesting to parallelize.  However, if
             * debug_parallel_query is on, then we want to execute the Result in
             * a parallel worker if possible, so we must check.
             */
            let parallel_mode_ok = root
                .glob
                .as_ref()
                .map(|g| g.parallel_mode_ok)
                .unwrap_or(false);
            if parallel_mode_ok
                && (root.query_level > 1 || debug_parallel_query() != DEBUG_PARALLEL_OFF)
            {
                // is_parallel_safe(root, parse->jointree->quals): bridge the
                // jointree quals (a parse-tree Node) into the lifetime-free
                // arena (#280 deep clone), then run clauses.c's hazard walker
                // with root->glob state.
                let quals_id = clone_jointree_quals_into_arena(mcx, run, root)?;
                let safe = is_jointree_quals_parallel_safe(root, quals_id)?;
                root.rel_mut(final_rel).consider_parallel = safe;
            }

            /*
             * The only path for it is a trivial Result path.  We cheat a bit
             * here by using a GroupResultPath, because that way we can just jam
             * the quals into it without preprocessing them.  (But, if you hold
             * your head at the right angle, a FROM-less SELECT is a kind of
             * degenerate-grouping case, so it's not that much of a cheat.)
             *
             * add_path(final_rel, (Path *)
             *   create_group_result_path(root, final_rel, final_rel->reltarget,
             *                             (List *) parse->jointree->quals));
             */
            // create_group_result_path takes the target by value: clone the
            // rel's own (empty-at-this-point) reltarget, and bridge the quals
            // List into the arena (havingqual: Vec<NodeId>).
            let target = root
                .rel(final_rel)
                .reltarget
                .clone()
                .expect("build_simple_rel set final_rel->reltarget");
            let havingqual = clone_jointree_quals_list_into_arena(mcx, run, root)?;
            let new_path =
                pathnode::create::create_group_result_path(root, final_rel, target, havingqual)?;
            pathnode::add_path(root, final_rel, new_path)?;

            /* Select cheapest path (pretty easy in this case...) */
            pathnode::set_cheapest(root, final_rel)?;

            /*
             * We don't need to run generate_base_implied_equalities, but we do
             * need to pretend that EC merging is complete.
             */
            root.ec_merging_done = true;

            /*
             * We still are required to call qp_callback, in case it's something
             * like "SELECT 2+2 ORDER BY 1".
             */
            qp_callback(root)?;

            return Ok(final_rel);
        }

        // Not RTE_RESULT: fall through to the general path.
    }

    /*
     * Construct RelOptInfo nodes for all base relations used in the query.
     * Appendrel member relations ("other rels") will be added later.
     *
     * Note: the reason we find the baserels by searching the jointree, rather
     * than scanning the rangetable, is that the rangetable may contain RTEs for
     * rels not actively part of the query, for example views.  We don't want to
     * make RelOptInfos for them.
     *
     * add_base_rels_to_query(root, (Node *) parse->jointree);
     */
    initsplan_seam::add_base_rels_to_query::call(root, run, jointree_of(run, root))?;

    /* Remove any redundant GROUP BY columns */
    initsplan_seam::remove_useless_groupby_columns::call(root, run);

    /*
     * Examine the targetlist and join tree, adding entries to baserel
     * targetlists for all referenced Vars, and generating PlaceHolderInfo
     * entries for all referenced PlaceHolderVars.  Restrict and join clauses
     * are added to appropriate lists belonging to the mentioned relations.  We
     * also build EquivalenceClasses for provably equivalent expressions.  The
     * SpecialJoinInfo list is also built to hold information about join order
     * restrictions.  Finally, we form a target joinlist for make_one_rel() to
     * work from.
     *
     * build_base_rel_tlists(root, root->processed_tlist);
     */
    initsplan_seam::build_base_rel_tlists::call(root, run);

    joininfo::placeholder::find_placeholders_in_jointree(root, run)?;

    initsplan_seam::find_lateral_references::call(root, run)?;

    let mut joinlist = initsplan_seam::deconstruct_jointree::call(root, run)?;

    /*
     * Reconsider any postponed outer-join quals now that we have built up
     * equivalence classes.  (This could result in further additions or mergings
     * of classes.)
     */
    equivclass::reconsider_outer_join_clauses(root, run)?;

    /*
     * If we formed any equivalence classes, generate additional restriction
     * clauses as appropriate.  (Implied join clauses are formed on-the-fly
     * later.)
     */
    equivclass::generate_base_implied_equalities(root, run)?;

    /*
     * We have completed merging equivalence sets, so it's now possible to
     * generate pathkeys in canonical form; so compute query_pathkeys and other
     * pathkeys fields in PlannerInfo.
     */
    qp_callback(root)?;

    /*
     * Examine any "placeholder" expressions generated during subquery pullup.
     * Make sure that the Vars they need are marked as needed at the relevant
     * join level.  This must be done before join removal because it might cause
     * Vars or placeholders to be needed above a join when they weren't so marked
     * before.
     */
    joininfo::placeholder::fix_placeholder_input_needed_levels(run.mcx(), root)?;

    /*
     * Remove any useless outer joins.  Ideally this would be done during
     * jointree preprocessing, but the necessary information isn't available
     * until we've built baserel data structures and classified qual clauses.
     */
    joinlist = analyzejoins_seam::remove_useless_joins::call(root, run, joinlist);

    /*
     * Also, reduce any semijoins with unique inner rels to plain inner joins.
     * Likewise, this can't be done until now for lack of needed info.
     */
    analyzejoins_seam::reduce_unique_semijoins::call(root, run);

    /*
     * Remove self joins on a unique column.
     */
    joinlist = analyzejoins_seam::remove_useless_self_joins::call(root, run, joinlist)?;

    /*
     * Now distribute "placeholders" to base rels as needed.  This has to be
     * done after join removal because removal could change whether a
     * placeholder is evaluable at a base rel.
     */
    joininfo::placeholder::add_placeholders_to_base_rels(run.mcx(), root)?;

    /*
     * Construct the lateral reference sets now that we have finalized
     * PlaceHolderVar eval levels.
     */
    initsplan_seam::create_lateral_join_info::call(root, run);

    /*
     * Match foreign keys to equivalence classes and join quals.  This must be
     * done after finalizing equivalence classes, and it's useful to wait till
     * after join removal so that we can skip processing foreign keys involving
     * removed relations.
     */
    initsplan_seam::match_foreign_keys_to_quals::call(root);

    /*
     * Look for join OR clauses that we can extract single-relation restriction
     * OR clauses from.
     */
    joininfo::orclauses::extract_restriction_or_clauses(run, root)?;

    /*
     * Now expand appendrels by adding "otherrels" for their children.  We delay
     * this to the end so that we have as much information as possible available
     * for each baserel, including all restriction clauses.  That let us prune
     * away partitions that don't satisfy a restriction clause.  Also note that
     * some information such as lateral_relids is propagated from baserels to
     * otherrels here, so we must have computed it already.
     */
    initsplan_seam::add_other_rels_to_query::call(root, run);

    /*
     * Distribute any UPDATE/DELETE/MERGE row identity variables to the target
     * relations.  This can't be done till we've finished expansion of
     * appendrels.
     */
    appendinfo_seam::distribute_row_identity_vars::call(mcx, run, root)?;

    /*
     * Ready to do the primary planning.
     */
    let final_rel = allpaths::make_one_rel(mcx, run, root, &joinlist)?;

    /* Check that we got at least one usable path */
    // if (!final_rel || !final_rel->cheapest_total_path ||
    //     final_rel->cheapest_total_path->param_info != NULL)
    //     elog(ERROR, "failed to construct the join relation");
    let ok = match root.rel(final_rel).cheapest_total_path {
        Some(id) => root.path(id).base().param_info.is_none(),
        None => false,
    };
    if !ok {
        return Err(::types_error::PgError::error(
            "failed to construct the join relation",
        ));
    }

    Ok(final_rel)
}

/// `DEBUG_PARALLEL_OFF` — first value of the `optimizer.h` `DebugParallelMode`
/// enum (= 0); the backing GUC `debug_parallel_query` is read through
/// [`debug_parallel_query`].
const DEBUG_PARALLEL_OFF: i32 = 0;

/// `debug_parallel_query` (GUC, `int`). In C the backing `int
/// debug_parallel_query` lives in `optimizer/plan/planner.c`; in this repo it is
/// the `debug_parallel_query` GUC slot, read through the shared
/// `backend-access-transam-parallel-rt-seams::debug_parallel_query` accessor
/// (installed by `backend-tcop-postgres` from the GUC slot — the same value the
/// planner's force-parallel Gather leg reads). Consulted only on the trivial
/// `RTE_RESULT` path, after `parallel_mode_ok` and `query_level > 1` have been
/// ruled out, exactly as in C.
#[inline]
fn debug_parallel_query() -> i32 {
    parallel_rt_seams::debug_parallel_query::call()
}

/// `root->parse->jointree` — the `FromExpr` node at the top of the jointree,
/// resolved through the planner-run store. The C never reaches `query_planner`
/// with a null jointree.
#[inline]
fn jointree_of<'a, 'mcx>(
    run: &'a PlannerRun<'mcx>,
    root: &PlannerInfo,
) -> &'a ::nodes::rawnodes::FromExpr<'mcx> {
    run.jointree(root.parse)
        .expect("query_planner: root->parse->jointree != NULL")
}

/// `(List *) parse->jointree->quals` deep-cloned into `root.node_arena`, as the
/// `havingqual` list `create_group_result_path` consumes. The C `havingqual`
/// here is `parse->jointree->quals` cast to `List *` — an implicit-AND list of
/// clause nodes; `None`/absent quals → empty list.
///
/// In this owned model `parse->jointree->quals` is held in one of two equivalent
/// shapes (mirroring `quals_implicit_and` in init-subselect):
///   * `Node::Expr(e)` — a single clause (the common `SELECT expr WHERE a=5`
///     case): the havingqual is the one-element list `[e]`.
///   * `Node::List([...])` — an already-imploded implicit-AND list, produced by
///     `concat_quals` in `remove_useless_result_rtes` when it merges an
///     INNER/SEMI JoinExpr's (or elided single-child FromExpr's) quals up into
///     the parent FromExpr (e.g. `(select 1) JOIN (select 2) ON c1.f1=c2.f1`,
///     or `1 IN (select 2)`). Here the havingqual is the cloned list, one entry
///     per conjunct.
/// The earlier version only handled `Node::Expr` and dropped a `Node::List`
/// entirely, so a variable-free qual hoisted onto a single-`RTE_RESULT` jointree
/// silently vanished — `1 IN (select 2)` / `(select 1) JOIN (select 2) ON
/// c1.f1=c2.f1` wrongly returned the LHS row instead of 0 rows.
fn clone_jointree_quals_list_into_arena<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<Vec<NodeId>> {
    let quals = jointree_of(run, root).quals.as_deref();
    let mut out: Vec<NodeId> = Vec::new();
    match quals {
        None => {}
        Some(n) if n.as_list().is_some() => {
            // Already an implicit-AND conjunct list: clone each clause out.
            let items = n.as_list().unwrap();
            out.reserve(items.len());
            for it in items.iter() {
                let e = it.as_expr().unwrap_or_else(|| {
                    panic!(
                        "clone_jointree_quals_list_into_arena: jointree quals List element is not an Expr: {:?}",
                        it.node_tag()
                    )
                });
                out.push(root.alloc_node(e.clone_in(mcx)?));
            }
        }
        Some(n) => {
            // A single bare clause expression ⇒ one-element havingqual.
            if let Some(e) = n.as_expr() {
                out.push(root.alloc_node(e.clone_in(mcx)?));
            }
        }
    }
    Ok(out)
}

/// Deep-clone `parse->jointree->quals` (a parse-tree expression `Node`) into
/// `root.node_arena` as a lifetime-free `Expr` (#280 `clone_in`), returning its
/// [`NodeId`], or `None` when there are no quals.
fn clone_jointree_quals_into_arena<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<Option<NodeId>> {
    let quals = jointree_of(run, root).quals.as_deref();
    let expr_src = match quals.and_then(|n| n.as_expr()) {
        Some(e) => e,
        None => return Ok(None),
    };
    let expr_clone = expr_src.clone_in(mcx)?;
    Ok(Some(root.alloc_node(expr_clone)))
}

/// `is_parallel_safe(root, (Node *) parse->jointree->quals)` (clauses.c).
///
/// Builds the `safe_param_ids` set from this level's init-plan `SubPlan`
/// `setParam` lists and runs the `max_parallel_hazard` walker with
/// `root->glob` state.
///
/// Model note: the C walks `proot = root; proot->parent_root` to also collect
/// parent-level init-plan params. The lifetime-free `PlannerInfo` does not
/// carry `parent_root`, so only this level's init plans are collected. At the
/// outermost query (`query_level == 1`) `parent_root` is NULL and this is
/// exact; in a sub-query it is conservative (a missing safe param can only make
/// the result *less* parallel-safe, never wrongly safe).
fn is_jointree_quals_parallel_safe(
    root: &PlannerInfo,
    quals_id: Option<NodeId>,
) -> PgResult<bool> {
    let max_parallel_hazard_glob = root
        .glob
        .as_ref()
        .map(|g| g.max_parallel_hazard as u8)
        .unwrap_or(PROPARALLEL_SAFE as u8);
    let param_exec_types_is_empty = root
        .glob
        .as_ref()
        .map(|g| g.param_exec_types.is_empty())
        .unwrap_or(true);

    // safe_param_ids = concat of each init SubPlan's setParam at this level.
    let mut safe_param_ids: Vec<i32> = Vec::new();
    for &init_id in root.init_plans.iter() {
        if let Some(sp) = root.node(init_id).as_subplan() {
            for &p in sp.0.setParam.iter() {
                safe_param_ids.push(p);
            }
        }
    }

    // node: Option<&Expr> — the cloned quals expr in the arena.
    let node = quals_id.map(|id| root.node(id));
    clauses::is_parallel_safe(
        max_parallel_hazard_glob,
        param_exec_types_is_empty,
        safe_param_ids,
        node,
    )
}

/// The trivial-path varno test, as a pure function over the jointree
/// `FromExpr`:
///
/// ```c
/// Assert(parse->jointree->fromlist != NIL);
/// if (list_length(parse->jointree->fromlist) == 1) {
///     Node *jtnode = (Node *) linitial(parse->jointree->fromlist);
///     if (IsA(jtnode, RangeTblRef))
///         varno = ((RangeTblRef *) jtnode)->rtindex;   // candidate
/// }
/// ```
///
/// Returns `Some(rtindex)` when the jointree's single `fromlist` entry is a
/// `RangeTblRef` (the only case that can reach the `RTE_RESULT` fast path), and
/// `None` otherwise. Panics if `fromlist` is empty (the C `Assert`).
fn trivial_path_varno(jointree: &::nodes::rawnodes::FromExpr) -> Option<i32> {
    let fromlist = &jointree.fromlist;
    assert!(!fromlist.is_empty(), "parse->jointree->fromlist != NIL");

    if fromlist.len() == 1 {
        fromlist[0].as_rangetblref().map(|rtr| rtr.rtindex)
    } else {
        None
    }
}

/// Install the seams this unit *owns*. `query_planner` is a consumer of
/// already-landed sibling crates (called by direct dependency) and of the
/// unported `initsplan.c` / `analyzejoins.c` / `appendinfo.c` seams (owned by
/// those crates, not this one), so this unit installs nothing of its own.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
