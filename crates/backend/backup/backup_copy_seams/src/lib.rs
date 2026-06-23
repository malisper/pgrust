//! Seam declarations for `backend-backup-copy`
//! (`src/backend/backup/basebackup_copy.c`: the COPY-protocol base-backup
//! sink).
//!
//! Almost all of `basebackup_copy.c`'s work is portable in-crate: the sink
//! state machine, the in-band `CopyData` type-byte framing, the progress-report
//! timing policy, and the `"%X/%X"` LSN formatting. The libpq message
//! assembly/output (`pq_beginmessage` / `pq_send*` / `pq_endmessage` /
//! `pq_putmessage` / `pq_putemptymessage` / `pq_puttextmessage`) is reached
//! through the real, landed `backend-libpq-pqformat` + `backend-libpq-pqcomm`
//! crates, and `pq_flush_if_writable` / the wall clock are reached through their
//! own owners' seams (`backend-libpq-pqcomm-seams`,
//! `backend-utils-adt-timestamp-seams`). So none of that is declared here.
//!
//! What *is* declared here is the `DestReceiver` result-set path used by
//! `SendXlogRecPtrResult` and `SendTablespaceList`: `CreateDestReceiver(
//! DestRemoteSimple)` plus the `executor.h` tuple-output trio
//! (`begin_tup_output_tupdesc` / `do_tup_output` / `end_tup_output`). That
//! boundary crosses the (still-unbuilt) receiver-value router keystone — the
//! `DestRemoteSimple` receiver that turns projected rows into `RowDescription`
//! + `DataRow` + the `T`/`D` wire frames has no installer in this tree yet — so
//! these seams panic loudly with the C symbol name until that owner lands,
//! exactly as the `guc_funcs.c` `SHOW` projection seams do.
//!
//! The result-set rows are carried as already-rendered, typed values
//! ([`ResultValue`], matching `basebackup_copy.c`'s `Datum values[]` /
//! `bool nulls[]`): the crate selects the columns and renders the LSN text; the
//! owner converts to the canonical slot/`Datum` form and emits the wire frames.

use ::types_core::primitive::Oid;

/// A column type for one of the two `DestRemoteSimple` result sets
/// (`basebackup_copy.c`'s `TupleDescInitBuiltinEntry` calls). Mirrors the
/// builtin type OIDs from `catalog/pg_type_d.h`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultColumnType {
    /// `TEXTOID` (25).
    Text,
    /// `INT8OID` (20).
    Int8,
    /// `OIDOID` (26).
    Oid,
}

/// One result-set column descriptor: a name and its builtin type
/// (`CreateTemplateTupleDesc` + `TupleDescInitBuiltinEntry`, with typmod `-1`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultColumn {
    /// Column name (`attname`).
    pub name: String,
    /// Column builtin type.
    pub typ: ResultColumnType,
}

/// One projected value in a data row, or `None` for SQL `NULL` (the C
/// `Datum values[i]` / `nulls[i]` pair). The variant matches the column's
/// declared type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultValue {
    /// A `TEXTOID` value (already-rendered server text).
    Text(String),
    /// An `INT8OID` value.
    Int8(i64),
    /// An `OIDOID` value.
    Oid(Oid),
}

/// A `TupOutputState *` (`executor.h`) for a `DestRemoteSimple` result set.
///
/// The real C struct carries the destination receiver and a virtual
/// `TupleTableSlot`; over the (still-unbuilt) receiver-value router boundary the
/// owner threads its own slot, so this opaque carrier just holds the
/// destination handle that the rows are routed to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TupOutputState {
    /// `tstate->dest` — the destination receiver handle.
    pub dest: nodes::parsestmt::DestReceiverHandle,
}

seam_core::seam!(
    /// `CreateDestReceiver(DestRemoteSimple)` (tcop/dest.c) — the receiver that
    /// emits a simple result set (`RowDescription` + text/binary `DataRow`s) to
    /// the client. Returns a router-keyed handle.
    pub fn create_dest_remote_simple() -> nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual)` (execTuples.c):
    /// prepare a `TupOutputState` for projecting rows described by `columns`
    /// (the `CreateTemplateTupleDesc` + `TupleDescInitBuiltinEntry` layout) to
    /// `dest`, sending the `RowDescription`. The owner builds the real virtual
    /// slot from `columns`.
    pub fn begin_tup_output_tupdesc(
        dest: nodes::parsestmt::DestReceiverHandle,
        columns: Vec<ResultColumn>,
    ) -> TupOutputState
);

seam_core::seam!(
    /// `do_tup_output(tstate, values, isnull)` (execTuples.c): emit one data row
    /// whose columns are the supplied values (`None` is the C `isnull` flag).
    pub fn do_tup_output(tstate: TupOutputState, values: Vec<Option<ResultValue>>)
);

seam_core::seam!(
    /// `end_tup_output(tstate)` (execTuples.c): finish and tear down the
    /// `TupOutputState`.
    pub fn end_tup_output(tstate: TupOutputState)
);
