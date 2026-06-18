//! Seam declarations for the `backend-commands-view` unit (`commands/view.c`):
//! the `CREATE [OR REPLACE] VIEW` command driver.
//!
//! Two kinds of seam live here:
//!
//! * **Inward** — `define_view` is the entry point `tcop/utility.c`
//!   (`ProcessUtilitySlow`) calls across the command boundary to execute a
//!   `CREATE VIEW`. The owner (`backend-commands-view`) installs it from its
//!   `init_seams()`; until utility.c dispatches to it the declaration simply
//!   sits installed.
//!
//! * **Outward** — the view-updatability analysis
//!   (`view_query_is_auto_updatable` in `rewriteHandler.c`). view.c owns the
//!   declaration here; its owner (`backend-rewrite-rewritehandler`) installs it.
//!
//! The relation-creation / ALTER-TABLE machinery (`DefineRelation`,
//! `BuildDescForRelation`, `AlterTableInternal`) is reached through
//! `backend-commands-tablecmds` directly — view.c builds the `CreateStmt` and
//! `AlterTableCmd` node lists itself and calls the real entry points, so no
//! view-private adapter seams are needed for them.

use mcx::Mcx;
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::ddlnodes::ViewStmt;

seam_core::seam!(
    /// `DefineView(stmt, queryString, stmt_location, stmt_len)` (view.c) —
    /// execute a `CREATE [OR REPLACE] VIEW`. The dispatcher
    /// (`tcop/utility.c`) supplies the parsed `ViewStmt`, the source text, and
    /// the statement's location/length. Returns the created/replaced view's
    /// `ObjectAddress`. Can `ereport(ERROR)`.
    pub fn define_view<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: ViewStmt<'mcx>,
        query_string: &str,
        stmt_location: i32,
        stmt_len: i32,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `view_query_is_auto_updatable(viewquery, security_invoker, check_cols,
    /// securityQuals)` (rewriteHandler.c) reduced to the `DefineView` call shape
    /// `view_query_is_auto_updatable(viewParse, true)`: returns `None` if the
    /// view's defining query is automatically updatable, or `Some(message)` — the
    /// (untranslated) detail string explaining why it is not. Can
    /// `ereport(ERROR)`.
    pub fn view_query_is_auto_updatable<'mcx>(
        mcx: Mcx<'mcx>,
        view_query: &Query<'mcx>,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `StoreViewQuery(viewOid, viewParse, replace)` (view.c) — install the
    /// `ON SELECT` rule that backs a (materialized) view. Called by
    /// `create_ctas_internal` (createas.c, reached through `backend-commands-
    /// tablecmds`'s `create_ctas_relation`) for the matview leg. The owner
    /// (`backend-commands-view`) installs it; it depends on `tablecmds`, so the
    /// reverse call crosses this seam. Can `ereport(ERROR)`.
    pub fn store_view_query<'mcx>(
        mcx: Mcx<'mcx>,
        view_oid: types_core::primitive::Oid,
        view_parse: Query<'mcx>,
        replace: bool,
    ) -> PgResult<()>
);
