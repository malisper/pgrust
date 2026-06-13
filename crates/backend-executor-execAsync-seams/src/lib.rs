//! Seam declarations for the `backend-executor-execAsync` unit
//! (`executor/execAsync.c`): the asynchronous-execution dispatch the Append
//! node drives.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The C `AsyncRequest *areq` aliases its requestor
//! (the Append node) and requestee (the child subplan) by raw pointer; the
//! owned model passes the request record plus the `EState` the dispatch needs
//! to reach the requestee and its result slot. Each dispatch can
//! `ereport(ERROR)` (the FDW/child callbacks), so all return `PgResult`.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecAsyncRequest(areq)` (execAsync.c): request a tuple from the async
    /// requestee, calling its `ExecAsyncRequest` callback. On a synchronous
    /// completion the callback fills `areq->result` and marks
    /// `request_complete`; otherwise it sets `callback_pending`.
    pub fn exec_async_request<'mcx>(
        areq: &mut types_nodes::AsyncRequestData,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecAsyncConfigureWait(areq)` (execAsync.c): give the requestee a
    /// chance to register its file descriptor in the caller's wait-event set
    /// (via its `ExecAsyncConfigureWait` callback).
    pub fn exec_async_configure_wait<'mcx>(
        areq: &mut types_nodes::AsyncRequestData,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecAsyncNotify(areq)` (execAsync.c): notify the requestee that the
    /// file descriptor it is waiting on has become ready (its
    /// `ExecAsyncNotify` callback), which may complete the request.
    pub fn exec_async_notify<'mcx>(
        areq: &mut types_nodes::AsyncRequestData,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
