//! Inward seam declarations for `backend-commands-matview`
//! (`commands/matview.c`): matview functions other (cyclic) crates call.
//!
//! The owning crate (`backend-commands-matview`) installs every one of these
//! from its `init_seams()`.
//!
//! - `RefreshMatViewByOid` / `SetMatViewPopulatedState` are called by
//!   `backend-commands-createas` (the CREATE MATERIALIZED VIEW populate path);
//! - `MatViewIncrementalMaintenanceIsEnabled` is called by the executor /
//!   rewriter to gate DML against materialized views (infallible — a plain
//!   `bool` read of the per-backend depth counter);
//! - the `transientrel_*` DestReceiver callbacks are invoked by the executor's
//!   destination dispatch on the `DR_transientrel` receiver matview installs.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use types_matview::{
    DestReceiverHandle, ObjectAddress, QueryCompletion, RefreshMatViewStmt, TupleDescHandle,
    TupleSlotHandle,
};

seam_core::seam!(
    /// `ExecRefreshMatView(stmt, queryString, qc)` (matview.c 120-140) — the
    /// REFRESH MATERIALIZED VIEW command entry point.
    pub fn ExecRefreshMatView(
        stmt: RefreshMatViewStmt,
        query_string: String,
        qc: Option<QueryCompletion>,
    ) -> PgResult<(ObjectAddress, Option<QueryCompletion>)>
);

seam_core::seam!(
    /// `RefreshMatViewByOid(matviewOid, is_create, skipData, concurrent,
    /// queryString, qc)` (matview.c 164-394) — refresh by OID (also the CREATE
    /// MATERIALIZED VIEW populate path).
    pub fn RefreshMatViewByOid(
        matview_oid: Oid,
        is_create: bool,
        skip_data: bool,
        concurrent: bool,
        query_string: String,
        qc: Option<QueryCompletion>,
    ) -> PgResult<(ObjectAddress, Option<QueryCompletion>)>
);

seam_core::seam!(
    /// `SetMatViewPopulatedState(relation, newstate)` (matview.c 78-110) — mark a
    /// materialized view populated or not. The relation is identified by OID
    /// (`RelationGetRelid`); the body re-opens pg_class via the catalog.
    pub fn SetMatViewPopulatedState(relation: Oid, newstate: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `MatViewIncrementalMaintenanceIsEnabled()` (matview.c 963-967) — whether
    /// the backend is in a context where DML may modify materialized views.
    /// Infallible (a plain read of `matview_maintenance_depth > 0`).
    pub fn MatViewIncrementalMaintenanceIsEnabled() -> bool
);

seam_core::seam!(
    /// `CreateTransientRelDestReceiver(transientoid)` (matview.c 464-477) —
    /// allocate and wire the `DR_transientrel` receiver.
    pub fn CreateTransientRelDestReceiver(transientoid: Oid) -> PgResult<DestReceiverHandle>
);

seam_core::seam!(
    /// `transientrel_startup(self, operation, typeinfo)` (matview.c 482-503).
    pub fn transientrel_startup(
        dest: DestReceiverHandle,
        operation: i32,
        typeinfo: TupleDescHandle,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `transientrel_receive(slot, self)` (matview.c 508-531). Returns the C
    /// `true`.
    pub fn transientrel_receive(slot: TupleSlotHandle, dest: DestReceiverHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `transientrel_shutdown(self)` (matview.c 536-548).
    pub fn transientrel_shutdown(dest: DestReceiverHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `transientrel_destroy(self)` (matview.c 553-557).
    pub fn transientrel_destroy(dest: DestReceiverHandle) -> PgResult<()>
);
