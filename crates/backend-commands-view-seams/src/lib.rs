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
//! * **Outward** — the relation-creation and ALTER-TABLE machinery
//!   (`DefineRelation`, `BuildDescForRelation`, `AlterTableInternal` for
//!   `AT_AddColumnToView` / `AT_ReplaceRelOptions`, all in `tablecmds.c`) and
//!   the view-updatability analysis (`view_query_is_auto_updatable` in
//!   `rewriteHandler.c`) have unported owners. view.c owns the declaration of
//!   those seams here; they panic loudly until their owners land and install
//!   them.

use mcx::{Mcx, PgVec};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::ddlnodes::ViewStmt;
use types_nodes::nodes::Node;
use types_nodes::rawnodes::{ColumnDef, RangeVar};
use types_tuple::heaptuple::TupleDescData;

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
    /// `DefineRelation(createStmt, RELKIND_VIEW, InvalidOid, NULL, NULL)`
    /// (tablecmds.c) for the view-creation leg of `DefineVirtualRelation`. The
    /// fake `CreateStmt` is built from the view's `RangeVar`, the `ColumnDef`
    /// list derived from the query target list, and the reloptions; inheritance,
    /// constraints, tablespace, `oncommit = ONCOMMIT_NOOP`, and `if_not_exists =
    /// false` are all the uninteresting view defaults. Returns the new
    /// relation's OID (`address.objectId`). tablecmds.c is not ported, so this
    /// panics until it lands and installs it. Can `ereport(ERROR)`.
    pub fn define_relation_view<'mcx>(
        mcx: Mcx<'mcx>,
        relation: RangeVar<'mcx>,
        attr_list: PgVec<'mcx, ColumnDef<'mcx>>,
        options: PgVec<'mcx, mcx::PgBox<'mcx, Node<'mcx>>>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `BuildDescForRelation(attrList)` (tablecmds.c) — build a tuple descriptor
    /// from a `ColumnDef` list, used by `DefineVirtualRelation`'s replace path to
    /// compare the proposed column list against the existing view. tablecmds.c is
    /// not ported, so this panics until it lands and installs it. Can
    /// `ereport(ERROR)`.
    pub fn build_desc_for_relation<'mcx>(
        mcx: Mcx<'mcx>,
        attr_list: &[ColumnDef<'mcx>],
    ) -> PgResult<TupleDescData<'mcx>>
);

seam_core::seam!(
    /// `AlterTableInternal(viewOid, atcmds, true)` with one `AT_AddColumnToView`
    /// command per new `ColumnDef` (tablecmds.c) — add `pg_attribute` entries for
    /// columns appended by `CREATE OR REPLACE VIEW`. Parse transformation is NOT
    /// run on these commands, so the supplied `ColumnDef`s must be execute-ready.
    /// tablecmds.c is not ported, so this panics until it lands and installs it.
    /// Can `ereport(ERROR)`.
    pub fn alter_table_add_columns_to_view<'mcx>(
        mcx: Mcx<'mcx>,
        view_oid: Oid,
        new_columns: PgVec<'mcx, ColumnDef<'mcx>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterTableInternal(viewOid, atcmds, true)` with a single
    /// `AT_ReplaceRelOptions` command (tablecmds.c) — replace the view's
    /// reloptions list with `options` (the new list replaces the existing one
    /// even when empty). tablecmds.c is not ported, so this panics until it lands
    /// and installs it. Can `ereport(ERROR)`.
    pub fn alter_table_replace_reloptions<'mcx>(
        mcx: Mcx<'mcx>,
        view_oid: Oid,
        options: PgVec<'mcx, mcx::PgBox<'mcx, Node<'mcx>>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `view_query_is_auto_updatable(viewquery, security_invoker, check_cols,
    /// securityQuals)` (rewriteHandler.c) reduced to the `DefineView` call shape
    /// `view_query_is_auto_updatable(viewParse, true)`: returns `None` if the
    /// view's defining query is automatically updatable, or `Some(message)` — the
    /// (untranslated) detail string explaining why it is not. rewriteHandler.c is
    /// not ported, so this panics until it lands and installs it. Can
    /// `ereport(ERROR)`.
    pub fn view_query_is_auto_updatable<'mcx>(
        mcx: Mcx<'mcx>,
        view_query: &Query<'mcx>,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);
