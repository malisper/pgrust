//! Seam declarations for the `backend-commands-portalcmds` unit
//! (`commands/portalcmds.c`). Cyclic callers (the utility dispatcher
//! `tcop/utility.c`, and `utils/mmgr/portalmem.c` which installs
//! `PortalCleanup` as the cursor cleanup hook and calls `PersistHoldablePortal`
//! at commit) reach these across the command↔portal cycle. The owning crate
//! installs all of them from its `init_seams()`.

use types_error::PgResult;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::portalcmds::{DeclareCursorStmt, FetchStmt, ParamListInfo, ParseState};
use types_portal::{Portal, QueryCompletion};

seam_core::seam!(
    /// `PerformCursorOpen(pstate, cstmt, params, isTopLevel)` — execute SQL
    /// `DECLARE CURSOR`. `mcx` is the caller's working (message) context for
    /// the planned statement. Runs user code (rewrite/plan/post-parse hook):
    /// can `ereport(ERROR)`.
    pub fn perform_cursor_open<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pstate: &ParseState,
        cstmt: DeclareCursorStmt,
        params: ParamListInfo,
        is_top_level: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PerformPortalFetch(stmt, dest, qc)` — execute SQL `FETCH`/`MOVE`.
    /// `dest` is the router-keyed [`DestReceiverHandle`] the dispatcher built
    /// (`tcop/dest.c`'s `CreateDestReceiver`) — QueryDesc de-handle F1b.
    pub fn perform_portal_fetch(
        stmt: &FetchStmt,
        dest: DestReceiverHandle,
        qc: Option<&mut QueryCompletion>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PerformPortalClose(name)` — close a cursor (`None` = `CLOSE ALL`).
    pub fn perform_portal_close(name: Option<&str>) -> PgResult<()>
);

seam_core::seam!(
    /// `PortalCleanup(portal)` — standard portal cleanup hook (installed by
    /// portalmem as `portal->cleanup`). Shuts the executor down; can
    /// `ereport(ERROR)`.
    pub fn portal_cleanup(portal: Portal) -> PgResult<()>
);

seam_core::seam!(
    /// `PersistHoldablePortal(portal)` — materialize a holdable cursor into its
    /// tuplestore so it survives transaction end. Runs the executor; can
    /// `ereport(ERROR)`.
    pub fn persist_holdable_portal(portal: Portal) -> PgResult<()>
);
