//! Seam declarations for `commands/portalcmds.c` (and the `pg_cursor` SRF
//! `Datum` body) that portalmem calls across the boundary. portalcmds owns the
//! `PortalCleanup` hook + held-cursor persistence; portalmem identifies a portal
//! by its (truncated) name string. Calls panic loudly until the owner lands.

use types_datum::Datum;
use types_portal::{ExternHandle, FcinfoHandle, PgCursorRow, PortalCleanupHook};

seam_core::seam!(
    /// The `PortalCleanup` function pointer `CreatePortal` installs as
    /// `portal->cleanup`. portalcmds owns the hook; portalmem only stores and
    /// later invokes it.
    pub fn portal_cleanup_hook() -> PortalCleanupHook
);

seam_core::seam!(
    /// Invoke `portal->cleanup(portal)` — runs portalcmds' `PortalCleanup`,
    /// which shuts the executor down. May run user code (can `ereport(ERROR)`).
    pub fn run_cleanup_hook(hook: PortalCleanupHook, portal: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PersistHoldablePortal(portal)` — drains the portal's query into its
    /// hold store so later transactions can read it. Runs the executor.
    pub fn persist_holdable_portal(portal: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PortalGetPrimaryStmt(portal)`'s `stmt->canSetTag` walk — the
    /// planned-statement list is owned by the cached plan, so the walk reads
    /// across the boundary; returns the first canSetTag `PlannedStmt` handle
    /// (`NONE` == none).
    pub fn first_can_set_tag_stmt(portal: &str) -> ExternHandle
);

seam_core::seam!(
    /// The `pg_cursor()` SRF body: `InitMaterializedSRF` + per-row `Datum`
    /// conversions + `tuplestore_putvalues` (the fmgr/`Datum` value layer).
    /// Given the already-collected visible rows, returns the SRF result Datum.
    pub fn pg_cursor_srf(fcinfo: FcinfoHandle, rows: &[PgCursorRow]) -> types_error::PgResult<Datum>
);
