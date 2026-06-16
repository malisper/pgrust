//! Seam-signature types for the libpq *client* surface reached by
//! `replication/libpqwalreceiver/libpqwalreceiver.c`.
//!
//! libpqwalreceiver.c is the glue between the backend's `WalReceiverFunctions`
//! vtable and the libpq *client* library (`PQ*`) plus a small set of backend
//! leaves (tuplestore / tuple-descriptor / memory-context machinery,
//! `pg_lsn_in`, `quote_identifier`, encoding-name, …).  The libpq objects
//! `PGconn *` / `PGresult *` are modelled as opaque integer handles: the seam
//! *provider* (a future in-process libpq client) owns the real pointers in its
//! own registry; this module only passes handles around.
//!
//! These libpq enums (`ConnStatusType` / `ExecStatusType`) and `Pgsocket` are
//! namespaced here ON PURPOSE — the same names exist elsewhere with different
//! (frontend / port-layer) shapes, so keeping libpqwalreceiver's variants under
//! their own crate avoids any collision.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

// ===========================================================================
// Opaque libpq handles.
// ===========================================================================

/// Opaque handle for a libpq `PGconn *`. The real connection lives in the seam
/// provider's registry; `0` is the null/invalid handle (no connection).
pub type PgConnId = usize;

/// Opaque handle for a libpq `PGresult *`. The real result lives in the seam
/// provider's registry; `0` is the null/invalid handle (`PQgetResult` returning
/// NULL, i.e. "no more results").
pub type PgResultId = usize;

/// `pgsocket` — a socket file descriptor (`int`).
pub type Pgsocket = i32;

// ===========================================================================
// libpq client enums (libpq-fe.h) — only the values libpqwalreceiver.c inspects.
// ===========================================================================

/// `ConnStatusType` (libpq-fe.h) — only the two values this file inspects.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConnStatusType {
    CONNECTION_OK,
    CONNECTION_BAD,
    /// Any other libpq connection status (the file only distinguishes OK / BAD).
    Other,
}

/// `ExecStatusType` (libpq-fe.h), in declaration order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExecStatusType {
    PGRES_EMPTY_QUERY,
    PGRES_COMMAND_OK,
    PGRES_TUPLES_OK,
    PGRES_COPY_OUT,
    PGRES_COPY_IN,
    PGRES_BAD_RESPONSE,
    PGRES_NONFATAL_ERROR,
    PGRES_FATAL_ERROR,
    PGRES_COPY_BOTH,
    PGRES_SINGLE_TUPLE,
    PGRES_PIPELINE_SYNC,
    PGRES_PIPELINE_ABORTED,
    PGRES_TUPLES_CHUNK,
}

// ===========================================================================
// ConninfoOption — one parsed conninfo entry (PQconninfoOption).
// ===========================================================================

/// One parsed conninfo option (`keyword`, `val`, `dispchar`), as surfaced by the
/// seam from a `PQconninfoOption` entry. The provider returns the full option
/// list with the C terminator (`keyword == NULL`) already stripped, so the port
/// iterates a plain `Vec`. `None` `val` is an unset option.
#[derive(Clone, Debug, Default)]
pub struct ConninfoOption {
    /// `keyword` (always present; terminator entries are dropped by the provider).
    pub keyword: String,
    /// `val` — `None` when the option is not present.
    pub val: Option<String>,
    /// `dispchar` — display-character flags (`'D'` = debug, `'*'` = secret).
    pub dispchar: String,
}

// ===========================================================================
// Opaque backend-subsystem handles used by libpqrcv_processTuples.
// ===========================================================================

/// Opaque handle for a `Tuplestorestate *` (the result tuplestore). Owned by the
/// tuplestore subsystem behind the seam; `0` is the null/uninitialized handle.
pub type TuplestoreId = usize;

/// Opaque handle for a `TupleDesc`. Owned by the tuple-descriptor subsystem
/// behind the seam; `0` is the null/uninitialized handle.
pub type TupleDescId = usize;

/// Opaque handle for an `AttInMetadata *`.
pub type AttInMetadataId = usize;

/// Opaque handle for a `MemoryContext` (the per-row temporary context).
pub type MemoryContextId = usize;

/// Opaque handle for a `HeapTuple` built from C strings.
pub type HeapTupleId = usize;
