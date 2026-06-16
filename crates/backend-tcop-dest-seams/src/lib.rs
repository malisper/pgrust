//! Seam declarations for the `DestReceiver` virtual dispatch (`tcop/dest.h`):
//! the `rStartup` / `receiveSlot` / `rShutdown` callbacks a `DestReceiver *`
//! carries. In C these are function pointers on the receiver struct; the owned
//! model keeps the receiver behind a [`DestReceiverHandle`] and dispatches
//! through these seams.
//!
//! The owning dest/printtup infrastructure installs these from its
//! `init_seams()` when it lands; until then a call panics loudly. Callers such
//! as `execTuples.c`'s `begin/do/end_tup_output` reach the receiver only
//! through this surface.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_dest::CommandDest;
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_portal::{CommandTag, QueryCompletion};
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `DestReceiver *CreateDestReceiver(CommandDest dest)` (tcop/dest.c) —
    /// return (a router-keyed handle to) the receiver function set for `dest`.
    /// `PerformPortalFetch` calls this with `DestNone` to discard `MOVE`
    /// output. Infallible in C; the seam returns a plain handle.
    pub fn create_dest_receiver(dest: CommandDest) -> DestReceiverHandle
);

seam_core::seam!(
    /// `dest->rStartup(dest, operation, tupdesc)` (tcop/dest.h): tell the
    /// receiver a result set of `tupdesc` rows is about to be sent under the
    /// given command type (`begin_tup_output_tupdesc` passes `CMD_SELECT`).
    /// `Err` carries whatever the receiver's startup raises.
    ///
    /// The leading `mcx: Mcx<'mcx>` is the per-query arena the receiver's
    /// startup works in (the DestReceiver mcx-vtable keystone): a receiver such
    /// as `intorel` opens its target relation and allocates its `BulkInsertState`
    /// here. Stateless/byte-stream receivers (`donothingDR`, COPY-TO) ignore it.
    /// The caller recovers `mcx` from `estate.es_query_cxt` (execMain) or its
    /// output-context (execTuples / pquery).
    pub fn dest_rstartup<'mcx>(
        mcx: Mcx<'mcx>,
        dest: DestReceiverHandle,
        operation: CmdType,
        tupdesc: &TupleDescData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `dest->receiveSlot(slot, dest)` (tcop/dest.h): send one tuple (held in
    /// `slot`) to the receiver, returning the receiver's bool result (C casts
    /// it to `(void)`). `Err` carries whatever the receiver raises.
    ///
    /// The leading `mcx: Mcx<'mcx>` (the keystone change) is what lets a
    /// `'mcx`-requiring receiver express its sink: `intorel_receive` calls
    /// `table_tuple_insert(mcx, &rel, slot, …)` — which needs both an
    /// `Mcx<'mcx>` and a `&mut SlotData<'mcx>` matching it — and COPY-FROM-style
    /// real inserts likewise. The slot lifetime is now `'mcx`-bound to that
    /// arena.
    pub fn dest_receive_slot<'mcx>(
        mcx: Mcx<'mcx>,
        slot: &mut SlotData<'mcx>,
        dest: DestReceiverHandle,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `dest->rShutdown(dest)` (tcop/dest.h): tell the receiver the result set
    /// is finished. `Err` carries whatever the receiver's shutdown raises.
    ///
    /// The leading `mcx: Mcx<'mcx>` lets a receiver finish `'mcx`-bound work at
    /// shutdown (`intorel_shutdown` frees its `BulkInsertState` /
    /// `table_finish_bulk_insert` and closes the relation in the query arena).
    pub fn dest_rshutdown<'mcx>(
        mcx: Mcx<'mcx>,
        dest: DestReceiverHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `dest->mydest` (tcop/dest.h): the [`CommandDest`] discriminant the
    /// receiver was created for. `PortalRunMulti` tests it against
    /// `DestRemoteExecute` (to swap in `None_Receiver`); `DoPortalRunFetch`
    /// tests it against `DestNone`. A plain field read; infallible.
    pub fn dest_get_mydest(dest: DestReceiverHandle) -> CommandDest
);

seam_core::seam!(
    /// `EndReplicationCommand(const char *commandTag)` (tcop/dest.c) — send the
    /// CommandComplete ('C') message ending a replication command. Can
    /// `ereport` on a send error.
    pub fn end_replication_command(command_tag: String) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BeginCommand(CommandTag commandTag, CommandDest dest)` (tcop/dest.c) —
    /// initialize the destination at the start of a command. "Nothing to do at
    /// present" in C, but the call is kept so the dispatch point is faithful.
    /// `PostgresMain`'s `exec_simple_query` / `exec_execute_message` call it.
    pub fn begin_command(command_tag: CommandTag, dest: CommandDest)
);

seam_core::seam!(
    /// `EndCommand(const QueryCompletion *qc, CommandDest dest, bool
    /// force_undecorated_output)` (tcop/dest.c) — clean up the destination at
    /// the end of a command, emitting the protocol `CommandComplete` ('C')
    /// message for the remote dests. Can `ereport` on a send error, carried on
    /// `Err`.
    ///
    /// The leading `mcx: Mcx<'mcx>` is the context the completion-tag
    /// `StringInfo` is built in (C uses a stack `char[]`); the caller supplies
    /// its per-command/message context.
    pub fn end_command<'mcx>(
        mcx: Mcx<'mcx>,
        qc: &QueryCompletion,
        dest: CommandDest,
        force_undecorated_output: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `NullCommand(CommandDest dest)` (tcop/dest.c) — tell the destination an
    /// empty query string was recognized, emitting the protocol
    /// `EmptyQueryResponse` ('I') message for the remote dests. Can `ereport` on
    /// a send error, carried on `Err`.
    pub fn null_command(dest: CommandDest) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ReadyForQuery(CommandDest dest)` (tcop/dest.c) — tell the destination we
    /// are ready for a new query, emitting the protocol `ReadyForQuery` ('Z')
    /// message (carrying the transaction-block status code) and flushing the
    /// output for the remote dests. Can `ereport` on a send error, carried on
    /// `Err`.
    ///
    /// The leading `mcx: Mcx<'mcx>` is the context the `'Z'` message
    /// `StringInfo` is built in (C uses a stack `StringInfoData`).
    pub fn ready_for_query<'mcx>(mcx: Mcx<'mcx>, dest: CommandDest) -> types_error::PgResult<()>
);
