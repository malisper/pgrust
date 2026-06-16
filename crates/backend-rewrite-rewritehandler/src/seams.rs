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
    backend_rewrite_rewritehandler_seams::expand_generated_columns_in_expr::set(
        seam_expand_generated_columns_in_expr,
    );
    backend_commands_view_seams::view_query_is_auto_updatable::set(
        seam_view_query_is_auto_updatable,
    );
    backend_rewrite_rewritehandler_seams::get_view_query::set(seam_get_view_query);
    backend_rewrite_rewritehandler_seams::relation_is_updatable::set(relation_is_updatable);
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
