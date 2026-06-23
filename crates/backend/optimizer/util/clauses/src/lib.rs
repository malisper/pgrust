#![allow(non_snake_case)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::too_many_arguments)]

//! Owned-tree port of `src/backend/optimizer/util/clauses.c` (PostgreSQL 18.3)
//! — the routines to inspect and manipulate qualification clauses.
//!
//! # Model
//!
//! The repo's executable-expression node is the lifetime-free
//! [`nodes::primnodes::Expr`] enum (NOT a `Node`). The generic recursion
//! engine is [`nodes_core::nodefuncs`]
//! (`expression_tree_walker(Option<&Expr>, &mut FnMut(&Expr)->bool)` and
//! `expression_tree_mutator(Expr, &mut FnMut(Expr)->Expr)`); C's
//! `bool (*)(Node *, void *)` walker becomes a named
//! `fn(Option<&Expr>, &mut Ctx) -> PgResult<bool>` whose `void *context` is the
//! explicit argument, and the default "recurse into children I don't care
//! about" is `expression_tree_walker(Some(node), &mut |n| walker(n, ctx))`.
//!
//! The `Expr` walker model does **not** carry `Query`/`List`/`FromExpr`/
//! `JoinExpr`/`RangeTblEntry` arms (the enum cannot construct those nodes), so
//! the C `query_tree_walker` recursion arms inside the contain-*-functions
//! walkers are unreachable for trees this model builds and are omitted; see
//! [`grounded`] for the per-function notes. `max_parallel_hazard(parse: Query)`
//! cannot be expressed (no walkable `Query`) and is not provided as a public
//! entry — no merged consumer calls it; `is_parallel_safe` / the
//! `max_parallel_hazard_walker` machinery over `Expr` IS ported.
//!
//! # Owned inward seam
//!
//! This crate owns and installs (`init_seams`) the one inward seam other crates
//! consume: `contain_subplans(&[Expr]) -> bool` (used by `nodeValuesscan`).
//!
//! # Cross-subsystem reads
//!
//! Catalog property reads (`func_volatile` / `func_strict` / `func_parallel` /
//! `get_func_leakproof` / `get_opcode` / `get_commutator` / `get_negator` /
//! `get_op_hash_functions` / `get_type_{in,out}put_info` / `get_typlenbyval[align]`)
//! call `backend-utils-cache-lsyscache-seams` directly (merged, real impls).
//! The fallible PgResult propagates: pure C predicates that now reach a fallible
//! seam become `PgResult`-returning, which is faithful (C ereports on cache
//! miss).
//!
//! The const-folding engine's executor-backed legs (fmgr invocation, the
//! pg_proc form read, the SQL inliner, planner support functions, type/domain
//! probes) ride the OUTWARD seams declared in
//! `backend-optimizer-util-clauses-seams`; `var.c`
//! (`contain_var_clause`/`pull_varnos`/`NumRelids`) rides
//! `backend-optimizer-util-var-seams`; `negate_clause` rides
//! `backend-optimizer-prep-prepqual-seams`; `ArrayGetNItems` rides
//! `backend-utils-adt-arrayfuncs-seams::array_const_nitems`. These are installed
//! by their real owners; until then a call panics loudly (the const-folding
//! legs C folds through the executor never silently skip).

extern crate alloc;

pub mod deferred;
pub mod fold;
pub mod grounded;
pub mod leaf;
pub mod srf_inline;
pub mod support_cost;
pub mod support_optimize_window;
pub mod support_rows;
pub mod support_simplify;

#[cfg(test)]
mod tests;

pub use deferred::{
    contain_mutable_functions_after_planning, contain_volatile_functions_after_planning,
};
pub use fold::{
    estimate_expression_value, eval_const_expressions, evaluate_expr, expand_function_arguments,
    make_SAOP_expr,
};
pub use grounded::{
    contain_agg_clause, contain_context_dependent_node, contain_exec_param, contain_leaked_vars,
    contain_mutable_functions, contain_nonstrict_functions, contain_subplans,
    contain_volatile_functions, contain_volatile_functions_not_nextval, contain_window_function,
    convert_saop_to_hashed_saop, expression_returns_set_rows, find_forced_null_var,
    find_forced_null_vars, find_nonnullable_rels, find_nonnullable_vars, find_window_functions,
    find_window_functions_in_exprs, is_parallel_safe, is_pseudo_constant_clause,
    is_pseudo_constant_clause_relids, max_parallel_hazard, num_relids,
    pull_paramids, CommuteOpExpr, WindowFuncLists,
};
pub use leaf::estimate_array_length;
pub use srf_inline::inline_set_returning_function;

/// Install the inward seam this unit owns (`contain_subplans`, consumed by
/// `nodeValuesscan`). The OUTWARD seams this crate declares (in
/// `clauses-seams` / `var-seams` / `prepqual-seams` / `arrayfuncs-seams`) are
/// installed by their real owners, not here.
pub fn init_seams() {
    clauses_seams::contain_subplans::set(grounded::contain_subplans_slice);

    // `expression_returns_set_rows(root, (Node *) clause)` (clauses.c:287) —
    // owned here. The costsize-seams declaration carries the clause as a
    // `NodeId` into the PlannerInfo arena; resolve it and delegate to the
    // owner body. The pathnode-seams adapter (installed by costsize) forwards
    // to this one, so the ProjectSet / SRF cost path reaches the real logic.
    costsize_seams::expression_returns_set_rows::set(|root, node| {
        grounded::expression_returns_set_rows(Some(root.node(node)))
            .unwrap_or_else(|e| {
                panic!("expression_returns_set_rows: {}", e.message())
            })
    });

    // The init-subselect cycle-break seam `find_forced_null_var_expr`
    // (`find_forced_null_var((Node *) clause)`, clauses.c) — owned here (the
    // impl is `grounded::find_forced_null_var`), installed for
    // `backend-optimizer-plan-init-subselect`'s `check_redundant_nullability_qual`
    // caller. The seam contract returns the forced-null `Var` by VALUE
    // (`Option<Expr>`); `find_forced_null_var` returns a borrow into the input
    // clause, so the adapter clones the matched `Expr::Var` (a leaf node).
    init_subselect_ext_seams::find_forced_null_var_expr::set(|clause| {
        grounded::find_forced_null_var(Some(clause)).cloned()
    });

    // `eval_const_expressions(root, node)` (clauses.c) over an owned arena `Expr`.
    // The port's `fold::eval_const_expressions` threads only an `Mcx` (the C
    // `root` is used solely for `boundParams`, not modeled here), so the seam
    // carries the planner-run `Mcx`. Used by `process_implied_equality` /
    // `simplify_EXISTS_query` / `convert_EXISTS_to_ANY` in init-subselect.
    init_subselect_ext_seams::eval_const_expressions_expr::set(|mcx, node| {
        fold::eval_const_expressions(mcx, node)
    });

    // `find_nonnullable_rels((Node *) expr)` (clauses.c) over a rootless `&Expr`,
    // the union of base relids non-nullable for the clause. The port allocates the
    // result `Bitmapset` in an `Mcx` and returns a `PgBox<Bitmapset<'mcx>>`; the
    // seam contract returns an owned, lifetime-free `Relids`
    // (`Option<Box<Bitmapset>>`), so the adapter copies the bit words out into an
    // owned `Bitmapset`. A `None`/empty result maps to the empty set (`None`).
    init_subselect_ext_seams::find_nonnullable_rels_expr::set(|expr| {
        // The port builds the result `Bitmapset` in an `Mcx`; the seam contract
        // returns an owned, lifetime-free `Relids`, so run the walker in a private
        // throwaway context and copy the bit words out before it drops.
        let scratch = mcx::MemoryContext::new("find_nonnullable_rels_expr");
        let bms = grounded::find_nonnullable_rels(scratch.mcx(), Some(expr))
            .expect("find_nonnullable_rels");
        match bms {
            Some(b) if !b.words.is_empty() => {
                Some(alloc::boxed::Box::new(pathnodes::Bitmapset {
                    words: b.words.iter().copied().collect(),
                }))
            }
            _ => None,
        }
    });

    // The equivclass-ext cycle-break leg owned by clauses.c:
    // `contain_volatile_functions((Node *) clause)` over a rootless `&Expr`
    // (initsplan.c `check_mergejoinable`/`check_hashjoinable` reject clauses with
    // volatile functions in their args). The impl is fallible only on a catalog
    // miss for a func OID in the tree; a propagated error is a loud panic
    // (mirrors C's elog/ereport).
    equivclass_ext_seams::contain_volatile_functions::set(|clause| {
        grounded::contain_volatile_functions(Some(clause))
            .expect("contain_volatile_functions")
    });

    // path-small.c / nodeWindowAgg.c reach `contain_volatile_functions((Node *)
    // expr)` over a rootless `&Expr`; clauses.c owns the predicate.
    path_small_seams::contain_volatile_functions_expr::set(|expr| {
        grounded::contain_volatile_functions(Some(expr)).expect("contain_volatile_functions")
    });

    // joinpath.c reaches `contain_volatile_functions` over planner-arena
    // handles: a single Expr node, a rel's `reltarget->exprs` list, or a
    // RestrictInfo's `clause`. clauses.c owns the predicate; resolve off `root`.
    joinpath_seams::contain_volatile_functions_node::set(|root, node| {
        grounded::contain_volatile_functions(Some(root.node(node)))
            .expect("contain_volatile_functions")
    });
    joinpath_seams::contain_volatile_functions_reltarget::set(
        |root, rel| {
            let reltarget = root
                .rel(rel)
                .reltarget
                .as_ref()
                .expect("contain_volatile_functions: RelOptInfo.reltarget is NULL");
            reltarget.exprs.iter().any(|&e| {
                grounded::contain_volatile_functions(Some(root.node(e)))
                    .expect("contain_volatile_functions")
            })
        },
    );
    joinpath_seams::contain_volatile_functions_rinfo::set(|root, rinfo| {
        let clause = root.rinfo(rinfo).clause;
        grounded::contain_volatile_functions(Some(root.node(clause)))
            .expect("contain_volatile_functions")
    });

    // relation_excluded_by_constraints (plancat.c) reaches
    // `contain_mutable_functions((Node *) clause)` over a planner-arena handle;
    // clauses.c owns the predicate, resolve off `root`.
    plancat_ext_seams::contain_mutable_functions::set(|root, node| {
        grounded::contain_mutable_functions(Some(root.node(node)))
    });

    // joinpath.c `paraminfo_get_equal_hashops` lateral-var leg: `IsA(node,
    // PlaceHolderVar)` and `lookup_type_cache(exprType(node), TYPECACHE_HASH_PROC
    // | TYPECACHE_EQ_OPR)` → `Some(eq_opr)` iff both the hash proc and eq operator
    // are valid (else Memoize declines). clauses.c owns the planner-arena node →
    // exprType bridge; the typcache lookup itself crosses to the typcache owner.
    joinpath_seams::node_is_placeholdervar::set(|root, node| {
        matches!(
            root.node(node),
            nodes::primnodes::Expr::PlaceHolderVar(_)
        )
    });
    joinpath_seams::expr_hash_eq_operator::set(|root, node| {
        let typid = nodes_core::nodefuncs::expr_type(Some(root.node(node)))
            .expect("expr_hash_eq_operator: exprType");
        typcache_seams::type_hash_eq_operator::call(typid)
            .expect("expr_hash_eq_operator: lookup_type_cache")
    });

    // get_eclass_for_sort_expr (equivclass.c) rejects an EC sort expression that
    // contains an aggregate or window function; clauses.c owns both predicates.
    // The grounded impls are fallible only on a catalog miss; a propagated error
    // is a loud panic (mirrors C's elog/ereport).
    equivclass_ext_seams::contain_agg_clause::set(|clause| {
        grounded::contain_agg_clause(Some(clause)).expect("contain_agg_clause")
    });
    equivclass_ext_seams::contain_window_function::set(|clause| {
        grounded::contain_window_function(Some(clause)).expect("contain_window_function")
    });

    // get_eclass_for_sort_expr / generate_join_implied_equalities (equivclass.c)
    // reach `is_parallel_safe(root, (Node *) em->em_expr)` when building partial
    // (parallel) paths — `require_parallel_safe` gates an EquivalenceMember's
    // expr. clauses.c owns the predicate; the ext-seams contract passes one
    // rootless `&Expr` plus `root` (for the planner globals the C reads off
    // `root->glob` / the init-plan chain). Compute the C inputs off `root` (same
    // derivation as `is_parallel_safe_nodes`) and run the grounded walk. A
    // propagated planner error is a loud panic (mirrors C's elog/ereport).
    equivclass_ext_seams::is_parallel_safe::set(|root, expr| {
        let (max_parallel_hazard_glob, param_exec_types_is_empty, safe_param_ids) =
            is_parallel_safe_inputs(root);
        grounded::is_parallel_safe(
            max_parallel_hazard_glob,
            param_exec_types_is_empty,
            safe_param_ids,
            Some(expr),
        )
        .expect("is_parallel_safe")
    });

    // joininfo.c / restrictinfo.c reach `contain_leaked_vars((Node *) clause)`
    // (clauses.c) over a rootless `&Expr` through the joininfo-ext consumer-side
    // seam crate (no owner directory). clauses.c owns it; the grounded impl
    // takes `Option<&Expr>` (the C `Node *clause`), so `Some(clause)`.
    joininfo_ext_seams::contain_leaked_vars::set(|clause| {
        grounded::contain_leaked_vars(Some(clause))
    });

    // clauses.c's clause-classification predicates declared in path-small-seams
    // (path-small.c's clauselist_selectivity / restriction analysis ride them).
    // The grounded impls take `Option<&Expr>` (the C `Node *clause`, possibly
    // NULL); the always-present seam `&Expr` maps to `Some(clause)`.
    path_small_seams::is_pseudo_constant_clause::set(|clause| {
        grounded::is_pseudo_constant_clause(Some(clause))
    });
    // C: `estimate_expression_value(root, node)` (clauses.c:2395). The port folds
    // over a memory context; `root` is part of the C signature but the
    // estimation-mode mutator does not read it (it const-folds stable functions
    // and strips PlaceHolderVars purely structurally). Run the fold in the
    // planner-run context the caller supplies.
    path_small_seams::estimate_expression_value::set(|run, _root, node| {
        // Deep-clone into the planner-run context via `clone_in` (NOT the
        // derived `.clone()`): the qual may carry context-allocated children
        // such as SubPlan/AlternativeSubPlan whose derived `Clone` panics.
        // The folded estimate is interned into the planner-run context; erase to
        // the planner arena's notional `'static` (the seam's arena-intern boundary).
        Ok(fold::estimate_expression_value(run.mcx(), node.clone_in(run.mcx())?)?.erase_lifetime())
    });
    path_small_seams::is_pseudo_constant_clause_relids::set(|clause, relids| {
        // C: `if (bms_is_empty(relids) && !contain_volatile_functions(clause))
        // return true;`. The seam threads `relids` as the planner-side
        // `pathnodes::Relids` (= Option<Box<Bitmapset>>, empty ⇔ None or
        // all-zero words), distinct from the grounded impl's `nodes`
        // Bitmapset; the only predicate over it is emptiness, computed inline.
        let relids_empty = match relids {
            None => true,
            Some(bms) => bms.words.iter().all(|w| *w == 0),
        };
        Ok(relids_empty && !grounded::contain_volatile_functions(Some(clause))?)
    });

    // clauses.c:751 `is_parallel_safe(root, (Node *) exprs/quals)` — the pathnode
    // create_*_path / gather-path parallel-safety guards. The pathnode-seams
    // contract passes the tlist/qual as `&[NodeId]` (handles into `root`'s node
    // arena); the planner globals C reads off `root->glob` / the `init_plans`
    // chain are threaded here from `PlannerInfo`. Both seam declarations (tlist
    // and qual) share the identical walk (C wraps the whole `List` as one Node).
    pathnode_seams::is_parallel_safe::set(is_parallel_safe_nodes);
    pathnode_seams::is_parallel_safe_quals::set(is_parallel_safe_nodes);

    // `simplify_function`'s `SupportRequestSimplify` dispatch (clauses.c:4108).
    // C calls `pg_proc.prosupport` through fmgr by OID; the owned model
    // dispatches through the `support_simplify` table, where support-bearing
    // crates register their decomposed simplify kernel. An OID with no
    // registered kernel declines (`Ok(None)`), the faithful counterpart of a
    // support function that does not handle `SupportRequestSimplify` (e.g.
    // `generate_series_int{4,8}_support`, which serve only `SupportRequestRows`)
    // returning NULL.
    clauses_seams::call_support_simplify::set(
        support_simplify::call_support_simplify,
    );

    // `inline_set_returning_function(root, rte)` (clauses.c:5067) — the SRF-
    // inline gate ladder, owned here. `preprocess_function_rtes`
    // (prepjointree.c) calls it for each FUNCTION RTE; it declines (`Ok(None)`)
    // every non-inlinable SRF (including every C-language SRF, which fails the
    // LANGUAGE-SQL gate) and only enters the SQL body parse/rewrite core (a
    // separate seam) for an inlinable SQL-language SRF.
    clauses_seams::inline_set_returning_function::set(
        srf_inline::inline_set_returning_function,
    );

    // `SupportRequestRows` dispatch (plancat.c:2200), used by `get_function_rows`
    // to estimate a set-returning function's rowcount through its `prosupport`
    // support function. Dispatched through the `support_rows` table; the
    // built-in `generate_series_int{4,8}_support` row kernels are registered
    // here. An OID with no kernel (or one that declines) returns `Ok(None)`, so
    // the caller falls back on `pg_proc.prorows`.
    support_rows::register_builtin_support_rows();
    clauses_seams::call_support_rows::set(
        support_rows::call_support_rows,
    );
    clauses_seams::call_support_rows_by_symbol::set(
        support_rows::call_support_rows_by_symbol,
    );

    // `SupportRequestOptimizeWindowClause` dispatch (planner.c:5848), used by
    // `optimize_window_clauses` to narrow a WindowClause's frame options through
    // each WindowFunc's `prosupport` support function. Dispatched through the
    // `support_optimize_window` table; the built-in ranking window functions'
    // kernels are registered here. An OID with no kernel returns `Ok(None)`, so
    // the caller leaves the frame options unchanged.
    support_optimize_window::register_builtin_support_optimize_window();

    // `SupportRequestCost` dispatch (plancat.c:2137), used by `add_function_cost`
    // to refine a function's cost through its `prosupport` support function.
    // Dispatched through the `support_cost` table; an OID with no kernel (or one
    // that declines, as `generate_series_int{4,8}_support` do — they serve only
    // SupportRequestRows) returns `Ok(None)`, so the caller falls back on
    // `pg_proc.procost`.
    clauses_seams::call_support_cost::set(
        support_cost::call_support_cost,
    );
    clauses_seams::call_support_cost_by_symbol::set(
        support_cost::call_support_cost_by_symbol,
    );
}

/// `is_parallel_safe(root, (Node *) nodes)` (clauses.c:751) over a list of
/// expression handles. Mirrors the C control flow: short-circuit when the global
/// `maxParallelHazard` is SAFE and no PARAM_EXEC params were generated, else walk
/// every element collecting the init-plan `setParam` ids (this query level and
/// all parents) as parallel-safe. Resolving the `is_parallel_safe` grounded impl
/// (which takes one `&Expr`) per list element is equivalent to walking the C
/// `List` node (the walker recurses element-wise). A propagated planner error is
/// a loud panic (mirrors C's elog/ereport).
/// Compute the C `is_parallel_safe` inputs that the `Expr` model does not
/// thread — `root->glob->maxParallelHazard`, whether `glob->paramExecTypes` is
/// empty, and the init-plan `setParam` ids of this query level and every parent
/// level (`for (proot = root; proot; proot = proot->parent_root) foreach
/// init_plans: concat initsubplan->setParam`).
fn is_parallel_safe_inputs(
    root: &pathnodes::PlannerInfo,
) -> (u8, bool, alloc::vec::Vec<i32>) {
    let glob = root
        .glob
        .as_ref()
        .expect("is_parallel_safe: PlannerInfo.glob is NULL");
    let max_parallel_hazard_glob = glob.max_parallel_hazard as u8;
    let param_exec_types_is_empty = glob.param_exec_types.is_empty();

    let mut safe_param_ids: alloc::vec::Vec<i32> = alloc::vec::Vec::new();
    let mut proot: Option<&pathnodes::PlannerInfo> = Some(root);
    while let Some(pr) = proot {
        for &ip in &pr.init_plans {
            if let Some(sp) = pr.node(ip).as_subplan() {
                safe_param_ids.extend(sp.0.setParam.iter().copied());
            }
        }
        proot = pr.parent_root.as_deref();
    }
    (
        max_parallel_hazard_glob,
        param_exec_types_is_empty,
        safe_param_ids,
    )
}

fn is_parallel_safe_nodes(
    root: &pathnodes::PlannerInfo,
    nodes: &[pathnodes::NodeId],
) -> bool {
    let (max_parallel_hazard_glob, param_exec_types_is_empty, safe_param_ids) =
        is_parallel_safe_inputs(root);

    for &nid in nodes {
        let expr = root.node(nid);
        let safe = grounded::is_parallel_safe(
            max_parallel_hazard_glob,
            param_exec_types_is_empty,
            safe_param_ids.clone(),
            Some(expr),
        )
        .expect("is_parallel_safe");
        if !safe {
            return false;
        }
    }
    true
}
