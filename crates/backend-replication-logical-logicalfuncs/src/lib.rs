//! Port of PostgreSQL 18.3 `src/backend/replication/logical/logicalfuncs.c` —
//! the SQL-callable support functions for logical decoding and management of
//! logical replication slots via SQL.
//!
//! ## fmgr / `Datum` boundary
//!
//! Each `Datum f(PG_FUNCTION_ARGS)` entry point is exposed with its arguments
//! already unwrapped to native Rust types and its result returned as a typed
//! value, matching the repo's `slotfuncs.c` / `xlogfuncs.c` value-boundary
//! convention. The fmgr dispatch layer is responsible for the
//! `PG_GETARG_*`/`PG_RETURN_*` wrapping, so the `text`/`bytea` arguments arrive
//! as already-detoasted payload byte slices (the C `VARDATA_ANY` /
//! `VARSIZE_ANY_EXHDR` view) and the `pg_lsn` result is returned as a raw
//! [`XLogRecPtr`].
//!
//! ## Status of the two function families
//!
//! * **`pg_logical_emit_message_text` / `pg_logical_emit_message_bytea`** —
//!   fully ported. Both bottom out on
//!   [`backend_replication_logical_message::LogLogicalMessage`] (message.c),
//!   which is landed; `pg_logical_emit_message_text` forwards to the bytea
//!   variant exactly as the C does (`/* bytea and text are compatible */`).
//!
//! * **`pg_logical_slot_get_changes` / `pg_logical_slot_peek_changes` /
//!   `pg_logical_slot_get_binary_changes` /
//!   `pg_logical_slot_peek_binary_changes`** (all via the shared
//!   [`pg_logical_slot_get_changes_guts`]) — STOP, gated on the in-flight
//!   logical-decoding de-handle keystone (#351). See
//!   [`pg_logical_slot_get_changes_guts`] for the precise blocker: the C code
//!   passes its *own* per-call output-write callbacks
//!   ([`LogicalOutputPrepareWrite`] / [`LogicalOutputWrite`], which write each
//!   decoded change into this function's private `DecodingOutputState`
//!   tuplestore) plus an `output_writer_private` pointer to
//!   `CreateDecodingContext`. The repo's `LogicalDecodingContext` model carries
//!   the write callbacks as bare presence-`bool`s and dispatches every output
//!   write through a *single global* `walsender::call_write` seam installed only
//!   by `walsender.c`; there is no `output_writer_private` channel and no way to
//!   route output into a function-private tuplestore. Until #351 re-models the
//!   context with real per-context [`OutputPluginCallbackArgs`]-style write
//!   callbacks and a writer-private payload, this whole family cannot decode
//!   into a tuplestore and is faithfully seam-panicked, not stubbed.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use types_core::primitive::XLogRecPtr;
use types_error::PgResult;

use backend_replication_logical_message::LogLogicalMessage;

/// `DecodingOutputState` (logicalfuncs.c:40) — private data for writing the
/// decoded change stream out into the SRF tuplestore.
///
/// Held by reference through the C `ctx->output_writer_private` (`void *`).
/// Retained here to document the shape the de-handle keystone (#351) must be
/// able to thread through `CreateDecodingContext`'s write callbacks; the
/// `tupstore` / `tupdesc` fields are the executor-side carriers
/// ([`Tuplestorestate`] / [`TupleDesc`] in C) that the repo's
/// `LogicalDecodingContext` cannot currently reach from an output-write
/// callback.
#[allow(dead_code)]
struct DecodingOutputState {
    /// `bool binary_output`.
    binary_output: bool,
    /// `int64 returned_rows`.
    returned_rows: i64,
}

/// `LogicalOutputPrepareWrite` (logicalfuncs.c:51): prepare for an output
/// plugin write — `resetStringInfo(ctx->out)`.
///
/// STOP — gated on #351. In C this is installed as `ctx->prepare_write` by the
/// `CreateDecodingContext(...)` call in
/// [`pg_logical_slot_get_changes_guts`]; the repo's `LogicalDecodingContext`
/// has no per-context write-callback function pointer (only a presence-`bool`),
/// so this function cannot be installed and reached. See the crate-level doc.
pub fn LogicalOutputPrepareWrite() -> PgResult<()> {
    panic!(
        "logicalfuncs::LogicalOutputPrepareWrite: gated on the logical-decoding de-handle \
         keystone (#351) — CreateDecodingContext takes prepare_write as a presence-bool and \
         dispatches output writes through the single global walsender::call_write seam, with no \
         per-context write callback or output_writer_private channel to install this into."
    );
}

/// `LogicalOutputWrite` (logicalfuncs.c:61): perform an output plugin write into
/// the tuplestore — builds `values[3]` = (`lsn`, `xid`, `text(ctx->out)`) and
/// calls `tuplestore_putvalues(p->tupstore, p->tupdesc, values, nulls)`,
/// incrementing `p->returned_rows`.
///
/// STOP — gated on #351 (same blocker as [`LogicalOutputPrepareWrite`]): the
/// repo cannot install a per-call output-write callback that captures this
/// function's private `DecodingOutputState` tuplestore.
pub fn LogicalOutputWrite() -> PgResult<()> {
    panic!(
        "logicalfuncs::LogicalOutputWrite: gated on the logical-decoding de-handle keystone \
         (#351) — there is no per-context output-write callback nor output_writer_private channel \
         on LogicalDecodingContext to route a decoded change into this function's private \
         DecodingOutputState tuplestore (the write path is hard-wired to walsender::call_write)."
    );
}

/// `pg_logical_slot_get_changes_guts` (logicalfuncs.c:98): the shared helper for
/// all four SQL-callable change-stream functions. `confirm` advances the slot's
/// `confirmed_flush`; `binary` selects binary vs textual output.
///
/// STOP — gated on the logical-decoding de-handle keystone (#351).
///
/// The faithful body (mirrored from the C) is:
///   1. `CheckSlotPermissions()` + `CheckLogicalDecodingRequirements()`;
///   2. read args (slot name, `upto_lsn`, `upto_nchanges`, options array),
///      deconstruct the `text[]` options into a `List *` of `DefElem`s;
///   3. `InitMaterializedSRF(fcinfo, 0)` to obtain the result tuplestore;
///   4. compute `end_of_wal` (`GetFlushRecPtr` / `GetXLogReplayRecPtr`);
///   5. `ReplicationSlotAcquire(name, true, true)`;
///   6. `CreateDecodingContext(InvalidXLogRecPtr, options, false, XL_ROUTINE(...),
///      LogicalOutputPrepareWrite, LogicalOutputWrite, NULL)` and set
///      `ctx->output_writer_private = p`;
///   7. `WaitForStandbyConfirmation(...)`, `XLogBeginRead`,
///      `InvalidateSystemCaches`, then the decode loop
///      (`XLogReadRecord` + `LogicalDecodingProcessRecord`) bounded by
///      `upto_lsn` / `upto_nchanges`, with the `{begin,change,commit}` callbacks
///      writing rows into `p->tupstore` via [`LogicalOutputWrite`];
///   8. optionally `LogicalConfirmReceivedLocation` + `ReplicationSlotMarkDirty`
///      (when `confirm`), `FreeDecodingContext`, `ReplicationSlotRelease`.
///
/// Step 6 is the wall. The C passes this function's *own*
/// [`LogicalOutputPrepareWrite`] / [`LogicalOutputWrite`] plus an
/// `output_writer_private` pointer to the function-private
/// [`DecodingOutputState`] tuplestore. The repo's `LogicalDecodingContext`
/// carries the write callbacks as bare presence-`bool`s and dispatches *every*
/// output write through the single global `walsender::call_write` seam
/// (installed only by `walsender.c`); there is no per-context write callback nor
/// `output_writer_private` channel. So the decode loop's output cannot be
/// redirected into the SRF tuplestore. The decode substrate itself
/// (`CreateDecodingContext`, `LogicalDecodingProcessRecord`,
/// `FreeDecodingContext`, `WaitForStandbyConfirmation`, `ReplicationSlot*`,
/// `InitMaterializedSRF`) is all landed — the sole blocker is the
/// output-callback channel, which #351 owns.
pub fn pg_logical_slot_get_changes_guts(_confirm: bool, _binary: bool) -> PgResult<()> {
    panic!(
        "logicalfuncs::pg_logical_slot_get_changes_guts: gated on the logical-decoding de-handle \
         keystone (#351). The SQL change-stream decode loop must install this function's own \
         LogicalOutputPrepareWrite/LogicalOutputWrite callbacks (writing each decoded change into \
         a function-private DecodingOutputState tuplestore) and an output_writer_private pointer \
         via CreateDecodingContext, but the repo's LogicalDecodingContext carries write callbacks \
         as presence-bools and routes all output writes through the single global \
         walsender::call_write seam — there is no per-context write callback or \
         output_writer_private channel to redirect output into the SRF tuplestore."
    );
}

/// `pg_logical_slot_get_changes` (logicalfuncs.c:330): SQL function returning the
/// changestream as text, consuming the data.
///
/// STOP — gated on #351 via [`pg_logical_slot_get_changes_guts`].
pub fn pg_logical_slot_get_changes() -> PgResult<()> {
    pg_logical_slot_get_changes_guts(true, false)
}

/// `pg_logical_slot_peek_changes` (logicalfuncs.c:339): SQL function returning the
/// changestream as text, only peeking ahead.
///
/// STOP — gated on #351 via [`pg_logical_slot_get_changes_guts`].
pub fn pg_logical_slot_peek_changes() -> PgResult<()> {
    pg_logical_slot_get_changes_guts(false, false)
}

/// `pg_logical_slot_get_binary_changes` (logicalfuncs.c:348): SQL function
/// returning the changestream in binary, consuming the data.
///
/// STOP — gated on #351 via [`pg_logical_slot_get_changes_guts`].
pub fn pg_logical_slot_get_binary_changes() -> PgResult<()> {
    pg_logical_slot_get_changes_guts(true, true)
}

/// `pg_logical_slot_peek_binary_changes` (logicalfuncs.c:357): SQL function
/// returning the changestream in binary, only peeking ahead.
///
/// STOP — gated on #351 via [`pg_logical_slot_get_changes_guts`].
pub fn pg_logical_slot_peek_binary_changes() -> PgResult<()> {
    pg_logical_slot_get_changes_guts(false, true)
}

/// `pg_logical_emit_message_bytea` (logicalfuncs.c:367): SQL function for writing
/// a logical decoding message into WAL.
///
/// ```c
/// bool  transactional = PG_GETARG_BOOL(0);
/// char *prefix        = text_to_cstring(PG_GETARG_TEXT_PP(1));
/// bytea *data         = PG_GETARG_BYTEA_PP(2);
/// bool  flush         = PG_GETARG_BOOL(3);
/// lsn = LogLogicalMessage(prefix, VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data),
///                         transactional, flush);
/// PG_RETURN_LSN(lsn);
/// ```
///
/// At the value boundary the fmgr layer has already detoasted the arguments:
/// `prefix` is the prefix string's bytes (the C NUL-terminated `char *` view —
/// `LogLogicalMessage` appends the trailing NUL itself, matching
/// `XLogRegisterData(prefix, strlen(prefix) + 1)`), and `data` is the bytea
/// payload bytes (`VARDATA_ANY(data)` of length `VARSIZE_ANY_EXHDR(data)`).
pub fn pg_logical_emit_message_bytea(
    transactional: bool,
    prefix: &[u8],
    data: &[u8],
    flush: bool,
) -> PgResult<XLogRecPtr> {
    LogLogicalMessage(prefix, data, data.len(), transactional, flush)
}

/// `pg_logical_emit_message_text` (logicalfuncs.c:381): SQL function for writing
/// a logical decoding message into WAL, text payload.
///
/// `/* bytea and text are compatible */` — forwards directly to
/// [`pg_logical_emit_message_bytea`], exactly as the C does
/// (`return pg_logical_emit_message_bytea(fcinfo);`).
pub fn pg_logical_emit_message_text(
    transactional: bool,
    prefix: &[u8],
    data: &[u8],
    flush: bool,
) -> PgResult<XLogRecPtr> {
    pg_logical_emit_message_bytea(transactional, prefix, data, flush)
}
