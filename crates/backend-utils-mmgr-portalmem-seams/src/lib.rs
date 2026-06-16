//! Seam declarations for the `backend-utils-mmgr-portalmem` unit
//! (`utils/mmgr/portalmem.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! C's `AtSubCommit_Portals` / `AtSubAbort_Portals` also receive the parent's
//! ResourceOwner; resource owners dissolve into RAII owner values here
//! (docs/query-lifecycle-raii.md), so those parameters drop out.

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `GetPortalByName(name)` (portalmem.c) lending the named cursor's live
    /// state to a callback. The portal owner looks up the `PortalData`, and —
    /// when it has a live `queryDesc`/`estate` — lends the scalar portal fields
    /// plus borrows of the running `EState` and `PlanState` tree as a
    /// [`RunningCursorState`]; `None` is the C `!PortalIsValid` (no such
    /// portal). The borrow is lent for the callback's duration only (no
    /// `&'static mut` escapes), per the seam rules. `execCurrentOf` runs all of
    /// its decision logic inside `f` and returns the resolved
    /// [`CurrentOfTid`]. Owner: `backend-utils-mmgr-portalmem`.
    pub fn with_running_cursor(
        name: &str,
        f: &mut dyn FnMut(
            Option<types_nodes::RunningCursorState>,
        ) -> PgResult<types_nodes::CurrentOfTid>,
    ) -> PgResult<types_nodes::CurrentOfTid>
);

seam_core::seam!(
    /// `PreCommit_Portals(isPrepare)` — close open portals before commit;
    /// returns true if it did anything (the caller loops). Runs user code:
    /// can `ereport(ERROR)`.
    pub fn pre_commit_portals(is_prepare: bool) -> PgResult<bool>
);

seam_core::seam!(
    /// `AtAbort_Portals()`.
    pub fn at_abort_portals() -> PgResult<()>
);

seam_core::seam!(
    /// `AtCleanup_Portals()` — now safe to release portal memory.
    pub fn at_cleanup_portals() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCommit_Portals(mySubid, parentSubid, parentLevel, parentXactOwner)`
    /// (owner parameter dissolved).
    pub fn at_subcommit_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
        parent_level: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubAbort_Portals(mySubid, parentSubid, myXactOwner, parentXactOwner)`
    /// (owner parameters dissolved).
    pub fn at_subabort_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCleanup_Portals(mySubid)`.
    pub fn at_subcleanup_portals(my_subid: SubTransactionId) -> PgResult<()>
);

// ---------------------------------------------------------------------------
// Portal operations the portalcmds (cursor command) unit calls. The portal
// itself crosses as the shared `types_portal::Portal` open handle.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `CreatePortal(name, allowDup, dupSilent)` (portalmem.c) — create a new
    /// portal (allocating its memory context). Can `ereport(ERROR)` on a
    /// duplicate name when `allow_dup` is false.
    pub fn create_portal(
        name: &str,
        allow_dup: bool,
        dup_silent: bool,
    ) -> types_error::PgResult<types_portal::Portal>
);

seam_core::seam!(
    /// `oldContext = MemoryContextSwitchTo(portal->portalContext);
    /// plan = copyObject(plan); queryString = pstrdup(sourceText);
    /// PortalDefineQuery(portal, NULL, queryString, CMDTAG_SELECT,
    /// list_make1(plan), NULL); MemoryContextSwitchTo(oldContext);`
    /// (portalmem.c), specialized to the cursor case (prepStmtName NULL,
    /// commandTag SELECT, single PlannedStmt, no CachedPlan). portalmem owns
    /// `portal->portalContext`, so it does the copyObject/pstrdup of the
    /// working-context `plan`/`source_text` into the portal context. Fallible:
    /// copying allocates.
    pub fn portal_define_query_select(
        portal: &types_portal::Portal,
        source_text: &str,
        plan: types_nodes::nodeindexscan::PlannedStmt<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `oldContext = MemoryContextSwitchTo(portal->portalContext);
    /// stmts = copyObject(stmts); queryString = pstrdup(sourceText);
    /// PortalDefineQuery(portal, prepStmtName, queryString, commandTag,
    /// stmts, cplan); MemoryContextSwitchTo(oldContext);` (portalmem.c) — the
    /// general `PortalDefineQuery` bridge for an arbitrary planned-statement
    /// list and command tag (`exec_simple_query` / `exec_bind_message`), not
    /// just the single-SELECT cursor case. portalmem owns
    /// `portal->portalContext`, so it does the `copyObject`/`pstrdup` of the
    /// working-context `stmts`/`source_text` into the portal context. `cplan`
    /// is `CachedPlanHandle::NULL` for the non-cached path (plans are then
    /// owned by the portal context); a non-NULL handle records that the plans
    /// belong to a cached plan instead. Fallible: copying allocates.
    pub fn portal_define_query_list(
        portal: &types_portal::Portal,
        prep_stmt_name: Option<&str>,
        source_text: &str,
        command_tag: types_portal::CommandTag,
        stmts: &[types_nodes::nodeindexscan::PlannedStmt<'_>],
        cplan: types_portal::CachedPlanHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `params = copyParamList(params)` after
    /// `MemoryContextSwitchTo(portal->portalContext)` (portalcmds.c) — copy the
    /// outer parameter list into the portal's own context (owned by portalmem).
    /// `None` in → `None` out (the C NULL). Fallible: copying allocates.
    pub fn copy_param_list_into_portal(
        portal: &types_portal::Portal,
        params: types_nodes::portalcmds::ParamListInfo,
    ) -> types_error::PgResult<types_nodes::portalcmds::ParamListInfo>
);

seam_core::seam!(
    /// `GetPortalByName(name)` (portalmem.c) — look up an open portal by name;
    /// `None` when absent (the C returns NULL / an invalid portal).
    pub fn get_portal_by_name(name: &str) -> types_error::PgResult<Option<types_portal::Portal>>
);

seam_core::seam!(
    /// `PortalHashTableDeleteAll()` (portalmem.c) — `CLOSE ALL`: drop every
    /// open portal. Runs portal cleanup hooks; can `ereport(ERROR)`.
    pub fn portal_hash_table_delete_all() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PortalDrop(portal, isTopCommit)` (portalmem.c) — drop a portal (runs
    /// its cleanup hook). Can `ereport(ERROR)`.
    pub fn portal_drop(portal: &types_portal::Portal, is_top_commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MarkPortalActive(portal)` (portalmem.c) — check for improper portal
    /// reentrancy and set status to PORTAL_ACTIVE. Can `ereport(ERROR)`.
    pub fn mark_portal_active(portal: &types_portal::Portal) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `oldcxt = MemoryContextSwitchTo(portal->holdContext);
    /// portal->tupDesc = CreateTupleDescCopy(portal->tupDesc);
    /// MemoryContextSwitchTo(oldcxt);` (portalcmds.c) — copy the portal's
    /// result tuple descriptor into its hold context (owned by portalmem) so it
    /// survives the executor shutdown, storing the copy back on the portal.
    /// Fallible: copying allocates.
    pub fn copy_tup_desc_into_hold_context(portal: &types_portal::Portal) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MarkPortalFailed(portal)` (portalmem.c) — set status to PORTAL_FAILED
    /// (error-abort path). Can `ereport(ERROR)`.
    pub fn mark_portal_failed(portal: &types_portal::Portal) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Run `f` with `portal` installed as `ActivePortal` and
    /// `portal->portalContext` as `PortalContext` (both portalmem-owned
    /// per-backend globals), restoring the previous values afterwards and on
    /// error. Models C's save/set/restore of `ActivePortal`/`PortalContext`
    /// around the `PersistHoldablePortal` PG_TRY block — a scoped capability,
    /// not an ambient setter pair.
    pub fn with_portal_globals(
        portal: &types_portal::Portal,
        f: &mut dyn FnMut() -> types_error::PgResult<()>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MemoryContextDeleteChildren(portal->portalContext)` (mcxt.c, reached
    /// via portalmem which owns the portal context) — release subsidiary
    /// memory of the portal's context.
    pub fn memory_context_delete_children(portal: &types_portal::Portal) -> types_error::PgResult<()>
);

// --- backend-utils-init-postinit consumer (portalmem.c) ---

seam_core::seam!(
    /// `EnablePortalManager()` (portalmem.c): set up the portal hashtable and
    /// memory context. `Err` carries its OOM surface.
    pub fn enable_portal_manager() -> types_error::PgResult<()>
);
