//! Seam declarations for the `backend-access-common-printtup` unit
//! (`access/common/printtup.c`), for callers that would otherwise cycle.
//!
//! The `DestRemote` / `DestRemoteExecute` `DestReceiver`s are created by
//! printtup's `printtup_create_DR`, and the `DestDebug` receiver is the static
//! `debugtupDR` (`debugtup` / `debugStartup`); both live in `printtup.c`, but
//! `tcop/dest.c`'s `CreateDestReceiver` switch — which lives in a crate that
//! cannot depend on printtup directly without a cycle — needs to delegate to
//! those constructors. So dest reaches them through these seams, exactly as it
//! reaches copyto's `CreateCopyDestReceiver` through
//! `backend-commands-copyto-seams`.
//!
//! printtup installs these from its own `init_seams()`.

seam_core::seam!(
    /// `printtup_create_DR(CommandDest dest)` (printtup.c:81): build the printtup
    /// `DestReceiver` for one of the `DestRemote` / `DestRemoteExecute` kinds and
    /// register it into the tcop-dest router, returning its
    /// [`DestReceiverHandle`]. `tcop/dest.c`'s `CreateDestReceiver` switch calls
    /// this for those kinds (the owner cannot live in dest.c, so dest delegates
    /// here; printtup installs the seam from its own `init_seams()`).
    pub fn printtup_create_dr(
        dest: types_dest::CommandDest,
    ) -> nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `&debugtupDR` (printtup.c `debugtup` / `debugStartup`, dest.c:75): build the
    /// `DestDebug` `DestReceiver` and register it into the tcop-dest router,
    /// returning its [`DestReceiverHandle`]. `tcop/dest.c`'s `CreateDestReceiver`
    /// switch returns `unconstify(DestReceiver *, &debugtupDR)` for `DestDebug`;
    /// the standalone (`--single`) backend's `whereToSendOutput = DestDebug`
    /// routes `SELECT` output here, printing each tuple to stdout via
    /// `debugtup` / `debugStartup`. printtup installs the seam from its own
    /// `init_seams()`.
    pub fn create_debug_dest_receiver() -> nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `&printsimpleDR` (printsimple.c, dest.c): build the `DestRemoteSimple`
    /// `DestReceiver` and register it into the tcop-dest router, returning its
    /// [`DestReceiverHandle`]. `tcop/dest.c`'s `CreateDestReceiver` returns
    /// `unconstify(DestReceiver *, &printsimpleDR)` for `DestRemoteSimple`; the
    /// catalog-free single-row result path (IDENTIFY_SYSTEM / SHOW /
    /// READ_REPLICATION_SLOT / TIMELINE_HISTORY in a walsender) routes its rows
    /// here. printtup installs the seam from its own `init_seams()`.
    pub fn create_remote_simple_dest_receiver() -> nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `SendRowDescriptionMessage(&row_description_buf, portal->tupDesc,
    /// FetchPortalTargetList(portal), portal->formats)` else
    /// `pq_putemptymessage(PqMsg_NoData)` (postgres.c `exec_describe_portal_message`).
    /// Sends the Describe-portal reply: a `RowDescription` when the portal
    /// returns tuples (`portal->tupDesc != NULL`), otherwise a `NoData` message.
    /// The target-list projection (`resjunk`/`resorigtbl`/`resorigcol`) and the
    /// per-column result formats are read off the portal here, in printtup,
    /// where `SendRowDescriptionMessage` and the `TargetEntryInfo` projection
    /// live. `tcop/postgres.c` reaches it through this seam (it cannot depend on
    /// printtup directly without a cycle); printtup installs it from its own
    /// `init_seams()`.
    pub fn send_describe_portal<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        portal: &portal::Portal,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The Describe-statement reply (postgres.c `exec_describe_statement_message`):
    /// first a `ParameterDescription` listing the prepared statement's parameter
    /// type OIDs, then either a `RowDescription` (when the cached plan has a
    /// result descriptor) or a `NoData` message. The plancache reads
    /// (`num_params` / `param_types` / `resultDesc` / `CachedPlanGetTargetList`)
    /// are done by the caller and threaded in; printtup owns the wire encoding +
    /// the `TargetEntryInfo` projection. `result_desc` is `None` (C's
    /// `psrc->resultDesc == NULL`) → `NoData`. `tcop/postgres.c` reaches it
    /// through this seam; printtup installs it from its own `init_seams()`.
    pub fn send_describe_statement<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        param_types: &[types_core::Oid],
        result_desc: Option<&types_tuple::heaptuple::TupleDescData<'mcx>>,
        targetlist: &[nodes::nodes::Node<'mcx>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CreateExplainSerializeDestReceiver(es)` (explain_dr.c): build the
    /// EXPLAIN (SERIALIZE) `DestReceiver` and register it into the tcop-dest
    /// router, returning its [`DestReceiverHandle`]. The receiver serializes
    /// each result row (running the type out/send functions, deTOASTing as a
    /// side effect) and counts the bytes that would have been sent, but never
    /// flushes to the client. The flags it consults (`es->timing`,
    /// `es->buffers`) and the resolved wire `format` (0 = text, 1 = binary) are
    /// passed in lieu of the `ExplainState *`. `commands/explain.c` reaches it
    /// through this seam (it cannot depend on printtup directly without a
    /// cycle); printtup installs it from its own `init_seams()`.
    pub fn create_explain_serialize_dest_receiver(
        format: i16,
        timing: bool,
        buffers: bool,
    ) -> nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `GetSerializationMetrics(dest)` (explain_dr.c): collect the metrics a
    /// SERIALIZE `DestReceiver` accumulated. Returns all-zeroes when `dest` is
    /// not a SERIALIZE receiver (e.g. the IntoRel receiver used for EXPLAIN
    /// (SERIALIZE) CREATE TABLE AS). `commands/explain.c` reaches it through
    /// this seam; printtup installs it from its own `init_seams()`.
    pub fn get_serialization_metrics(
        dest: nodes::parsestmt::DestReceiverHandle,
    ) -> types_core::instrument::SerializeMetrics
);

seam_core::seam!(
    /// `printtup_destroy(DestReceiver *self)` (printtup.c) — release the
    /// per-receiver `DR_printtup` state owned by printtup's registry, named by
    /// the router's `state` token. `tcop/dest.c`'s `free_dest_receiver` reaches
    /// it through this seam when reclaiming a `DestRemote`/`DestRemoteExecute`/
    /// `DestDebug` receiver's router slot (the owner cannot live in dest.c, so
    /// dest delegates here; printtup installs the seam from its own
    /// `init_seams()`). Idempotent: freeing an already-released token is a no-op.
    pub fn printtup_free_dr(state: u64)
);
