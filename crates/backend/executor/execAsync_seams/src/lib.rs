//! Seam declarations for the `backend-executor-execAsync` unit
//! (`executor/execAsync.c`): the asynchronous-execution dispatch the Append
//! node drives.
//!
//! Re-homed onto the Append node (its sole caller). The C dispatch switches on
//! `nodeTag(areq->requestee)` and reaches the requestee `PlanState` and the
//! requestor `AppendState` through raw back-pointers (`areq->requestee` /
//! `areq->requestor`). The owned `AsyncRequestData` carries neither pointer —
//! it carries only `request_index`, which is exactly the requestee subplan's
//! index in the requestor `AppendState`'s `appendplans`/`as_asyncrequests`
//! arrays (see `ExecAppendAsyncBegin`). So the seam carries the requestor
//! `AppendStateData` and that index: the dispatch reaches `appendplans[index]`
//! (requestee) and `as_asyncrequests[index]` (the `AsyncRequest`) from it, and
//! `ExecAsyncResponse` reaches the requestor directly. The installer
//! (`nodeAppend::init_seams`) owns both arrays, so the dispatch is faithful and
//! value-typed (no aliasing pointer is reconstructed).
//!
//! Each dispatch can `ereport(ERROR)` (the FDW/child callbacks), so all return
//! `PgResult`.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecAsyncRequest(areq)` (execAsync.c): request a tuple from the async
    /// requestee `node.appendplans[request_index]`, calling its
    /// `ExecAsyncRequest` callback (only `ForeignScanState` is async-capable).
    /// Wraps it with the C `ExecReScan`-if-`chgParam`, instrumentation, and the
    /// trailing `ExecAsyncResponse` (which delivers to the requestor `node`).
    /// On a synchronous completion the callback fills `areq->result` and marks
    /// `request_complete`; otherwise it sets `callback_pending`.
    pub fn exec_async_request<'mcx>(
        node: &mut nodes::AppendStateData<'mcx>,
        request_index: i32,
        estate: &mut nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecAsyncConfigureWait(areq)` (execAsync.c): give the requestee
    /// `node.appendplans[request_index]` a chance to register its file
    /// descriptor in the caller's wait-event set (via its
    /// `ExecAsyncConfigureWait` callback).
    pub fn exec_async_configure_wait<'mcx>(
        node: &mut nodes::AppendStateData<'mcx>,
        request_index: i32,
        estate: &mut nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecAsyncNotify(areq)` (execAsync.c): notify the requestee
    /// `node.appendplans[request_index]` that the file descriptor it is waiting
    /// on has become ready (its `ExecAsyncNotify` callback), which may complete
    /// the request. Followed by the trailing `ExecAsyncResponse`.
    pub fn exec_async_notify<'mcx>(
        node: &mut nodes::AppendStateData<'mcx>,
        request_index: i32,
        estate: &mut nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);
