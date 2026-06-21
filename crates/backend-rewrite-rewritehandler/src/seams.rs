//! Seam installers owned by the rewriteHandler unit.
//!
//! This slice installs the rewriteHandler.c seams whose bodies are fully
//! ported here:
//!  * `build_column_default` (backend-rewrite-rewritehandler-seams)
//!  * `expand_generated_columns_in_expr` (backend-rewrite-rewritehandler-seams)
//!  * `view_query_is_auto_updatable` (backend-commands-view-seams) — the
//!    `check_cols = true` `DefineView` reduction.
//!  * `get_view_query` (backend-rewrite-rewritehandler-seams) — the view
//!    `_RETURN` rule reader, over the `relation_rules` carrier.
//!  * `relation_is_updatable` (backend-rewrite-rewritehandler-seams) — the
//!    auto-updatable-view event probe (consumers: misc.c
//!    `pg_relation_is_updatable` / `pg_column_is_updatable`).
//!  * `query_rewrite_canonical` (backend-rewrite-rewritehandler-seams) — the
//!    value-typed `QueryRewrite` entry. The legacy opaque `query_rewrite`
//!    contract is installed as a precise K1 panic boundary.

use mcx::{Mcx, PgBox, PgString};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_storage::lock::NoLock;

use crate::{
    build_column_default, expand_generated_columns_in_expr, get_view_query, relation_is_updatable,
    view_query_is_auto_updatable,
};

/// Install the rewriteHandler.c seams this slice owns.
pub fn init_seams() {
    backend_rewrite_rewritehandler_seams::build_column_default::set(seam_build_column_default);
    backend_rewrite_rewritehandler_seams::build_generation_expression::set(
        seam_build_generation_expression,
    );
    backend_rewrite_rewritehandler_seams::expand_generated_columns_in_expr::set(
        seam_expand_generated_columns_in_expr,
    );
    // plancat.c's get_relation_constraints expands virtual generated columns in
    // the constraint-expression list (arena handles); rewriteHandler.c owns the
    // body. Resolve each handle off `root`, expand, and store the owned result
    // back into the same arena slot.
    backend_optimizer_util_plancat_ext_seams::expand_generated_columns_in_expr::set(
        seam_expand_generated_columns_in_expr_arena,
    );
    backend_commands_view_seams::view_query_is_auto_updatable::set(
        seam_view_query_is_auto_updatable,
    );
    backend_rewrite_rewritehandler_seams::get_view_query::set(seam_get_view_query);
    backend_rewrite_rewritehandler_seams::relation_is_updatable::set(relation_is_updatable);
    backend_rewrite_rewritehandler_seams::query_rewrite::set(seam_query_rewrite_legacy);
    backend_rewrite_rewritehandler_seams::query_rewrite_canonical::set(
        seam_query_rewrite_canonical,
    );
    backend_rewrite_rewritehandler_seams::acquire_rewrite_locks::set(seam_acquire_rewrite_locks);
    // The ruleutils deparser declares its own `&mut Query` AcquireRewriteLocks
    // seam (get_query_def re-locks the deparsed tree). It is the same owner —
    // install it here over the in-place `AcquireRewriteLocks`.
    backend_utils_adt_ruleutils_seams::acquire_rewrite_locks::set(
        seam_acquire_rewrite_locks_inplace,
    );
    backend_rewrite_rewritehandler_seams::relation_has_security_invoker::set(
        seam_relation_has_security_invoker,
    );
    backend_rewrite_rewritehandler_seams::relation_is_security_view::set(
        seam_relation_is_security_view,
    );
    backend_rewrite_rewritehandler_seams::relation_has_check_option::set(
        seam_relation_has_check_option,
    );
    backend_rewrite_rewritehandler_seams::relation_has_cascaded_check_option::set(
        seam_relation_has_cascaded_check_option,
    );
}

// The view-option predicates (`utils/rel.h` `RelationHasSecurityInvoker`,
// `RelationIsSecurityView`, `RelationHasCheckOption`,
// `RelationHasCascadedCheckOption`) read the *view* `StdRdOptions`/`ViewOptions`
// out of `rd_options`. The trimmed `RelationData::rd_options` carries only the
// heap `StdRdOptions` (the relcache drops parsed View options — see
// `extract_rel_options_seam`), so a view's `security_barrier`/`security_invoker`/
// `check_option` flags are not recoverable from the relcache entry in this
// model. They therefore answer the no-view-options value (`false`), which is the
// correct result for a plain `CREATE VIEW` (no reloptions). A view defined WITH
// CHECK OPTION or as a security_barrier/security_invoker view is the documented
// banked blocker (its enforcement needs the ViewOptions carrier on rd_options).
/// The C view-option predicates read the parsed `ViewOptions` out of
/// `(relation)->rd_options`. The trimmed `RelationData::rd_options` carries only
/// the heap `StdRdOptions`, so the parsed view options are not recoverable from
/// the relcache entry. We recover the *same* information faithfully by fetching
/// the view's `pg_class.reloptions` text[] via syscache and re-running the
/// (already-complete) `view_reloptions` parser — exactly the parse the relcache
/// performs in C when it builds `rd_options`. A view with no reloptions parses to
/// `None`, yielding the C no-options default (all flags false / NOT_SET), which
/// matches `(relation)->rd_options ? ... : false`.
fn view_options_of(view: &types_rel::Relation<'_>) -> types_reloptions::relopts::ViewOptions {
    let relid = view.rd_id;
    let scratch = mcx::MemoryContext::new("RelationViewOptions");
    let smcx = scratch.mcx();
    let token =
        match backend_utils_cache_syscache_seams::fetch_class_reloptions::call(smcx, relid) {
            Ok(t) => t,
            // Missing tuple (concurrently dropped): treat as no options, matching
            // the C `rd_options == NULL` branch.
            Err(_) => return types_reloptions::relopts::ViewOptions::default(),
        };
    if token.is_null {
        return types_reloptions::relopts::ViewOptions::default();
    }
    // validate=false: never errors; just parse the stored, already-validated bytes.
    match backend_access_common_reloptions::view_reloptions(smcx, Some(&token.bytes), false) {
        Ok(Some(backend_access_common_reloptions::RelOptStruct::View(v))) => v,
        _ => types_reloptions::relopts::ViewOptions::default(),
    }
}

fn seam_relation_has_security_invoker(view: &types_rel::Relation<'_>) -> bool {
    view_options_of(view).security_invoker
}
fn seam_relation_is_security_view(view: &types_rel::Relation<'_>) -> bool {
    view_options_of(view).security_barrier
}
fn seam_relation_has_check_option(view: &types_rel::Relation<'_>) -> bool {
    view_options_of(view).check_option
        != types_reloptions::relopts::VIEW_OPTION_CHECK_OPTION_NOT_SET
}
fn seam_relation_has_cascaded_check_option(view: &types_rel::Relation<'_>) -> bool {
    view_options_of(view).check_option
        == types_reloptions::relopts::VIEW_OPTION_CHECK_OPTION_CASCADED
}

/// `AcquireRewriteLocks(parsetree, forExecute, forUpdatePushedDown)`
/// (rewriteHandler.c:148) over the value `Query` — the standalone re-lock entry
/// `plancache.c`'s `RevalidateCachedQuery` uses. The owned `AcquireRewriteLocks`
/// mutates `&mut Query` in place; the seam takes the `Query` by value, locks +
/// updates it, and returns it.
fn seam_acquire_rewrite_locks<'mcx>(
    mcx: Mcx<'mcx>,
    mut parsetree: types_nodes::copy_query::Query<'mcx>,
    for_execute: bool,
    for_update_pushed_down: bool,
) -> PgResult<types_nodes::copy_query::Query<'mcx>> {
    crate::AcquireRewriteLocks(mcx, &mut parsetree, for_execute, for_update_pushed_down)?;
    Ok(parsetree)
}

/// `AcquireRewriteLocks(query, forExecute, forUpdatePushedDown)` over the
/// in-place `&mut Query` — the ruleutils deparser's `get_query_def` re-lock
/// step (ruleutils.c 5654). Same owner body as `seam_acquire_rewrite_locks`,
/// matching the ruleutils seam's `&mut Query -> ()` shape.
fn seam_acquire_rewrite_locks_inplace<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &mut types_nodes::copy_query::Query<'mcx>,
    for_execute: bool,
    for_update_pushed_down: bool,
) -> PgResult<()> {
    crate::AcquireRewriteLocks(mcx, parsetree, for_execute, for_update_pushed_down)
}

/// Legacy opaque `portalcmds::Query` entry. Collapsing this contract into the
/// canonical value-typed `Query` belongs to K1 query unification, not this port.
fn seam_query_rewrite_legacy<'mcx>(
    _mcx: Mcx<'mcx>,
    _query: types_nodes::portalcmds::Query,
) -> PgResult<mcx::PgVec<'mcx, types_nodes::portalcmds::Query>> {
    panic!(
        "rewriteHandler legacy query_rewrite over portalcmds::Query reached: \
         blocked on K1 Query-unification debt; use query_rewrite_canonical once \
         parser/planner callers carry types_nodes::copy_query::Query"
    )
}

/// `QueryRewrite(parsetree)` (rewriteHandler.c:4566) — the canonical top-level
/// rule-rewriter entry over the value-typed `Query`.
fn seam_query_rewrite_canonical<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: types_nodes::copy_query::Query<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, types_nodes::copy_query::Query<'mcx>>> {
    let results = crate::QueryRewrite(mcx, parsetree)?;
    let mut out = mcx::PgVec::new_in(mcx);
    for q in results {
        out.push(q);
    }
    Ok(out)
}

fn seam_get_view_query<'mcx>(
    mcx: Mcx<'mcx>,
    view: &types_rel::Relation<'mcx>,
) -> PgResult<types_nodes::copy_query::Query<'mcx>> {
    get_view_query(mcx, view)
}

fn seam_build_column_default<'mcx>(
    mcx: Mcx<'mcx>,
    rel: types_rel::Relation<'mcx>,
    attrno: i32,
) -> PgResult<Option<PgBox<'mcx, Expr>>> {
    build_column_default(mcx, &rel, attrno)
}

fn seam_build_generation_expression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    attrno: i32,
) -> PgResult<PgBox<'mcx, Expr>> {
    let expr = crate::build_generation_expression(mcx, rel, attrno)?;
    mcx::alloc_in(mcx, expr)
}

fn seam_expand_generated_columns_in_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Expr>,
    rel_oid: Oid,
    rt_index: i32,
) -> PgResult<Option<Expr>> {
    // The C `expand_generated_columns_in_expr` takes an already-open Relation;
    // the consumers (publicationcmds/plancat) hold the relation open, so we
    // re-open with NoLock to consult the tuple descriptor.
    let rel = backend_access_table_table::table_open(mcx, rel_oid, NoLock)?;
    let result = expand_generated_columns_in_expr(mcx, node, &rel, rt_index);
    rel.close(NoLock)?;
    result
}

/// plancat-ext variant of `expand_generated_columns_in_expr` over a
/// planner-arena node list. C calls `expand_generated_columns_in_expr` once per
/// constraint expression after opening the relation; here the relation is opened
/// once (NoLock — the caller holds it), and each arena handle is resolved, run
/// through the owned-`Expr` body in a scratch context (the returned `Expr` is a
/// self-contained owned node), and stored back into its slot.
fn seam_expand_generated_columns_in_expr_arena(
    root: &mut types_pathnodes::PlannerInfo,
    nodes: &[types_pathnodes::NodeId],
    relid: Oid,
    varno: i32,
) -> PgResult<Vec<types_pathnodes::NodeId>> {
    let ctx = mcx::MemoryContext::new("expand_generated_columns_in_expr arena");
    let mcx = ctx.mcx();
    let rel = backend_access_table_table::table_open(mcx, relid, NoLock)?;
    for &id in nodes {
        let node = root.node(id).clone();
        let expanded = expand_generated_columns_in_expr(mcx, Some(node), &rel, varno)?;
        if let Some(e) = expanded {
            *root.node_mut(id) = e;
        }
    }
    rel.close(NoLock)?;
    Ok(nodes.to_vec())
}

fn seam_view_query_is_auto_updatable<'mcx>(
    mcx: Mcx<'mcx>,
    view_query: &types_nodes::copy_query::Query<'mcx>,
) -> PgResult<Option<PgString<'mcx>>> {
    // DefineView calls view_query_is_auto_updatable(viewParse, true).
    let detail = view_query_is_auto_updatable(view_query, true)?;
    match detail {
        Some(s) => Ok(Some(PgString::from_str_in(s, mcx)?)),
        None => Ok(None),
    }
}
