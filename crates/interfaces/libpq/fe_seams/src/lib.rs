//! Seam declarations for the libpq *client* (frontend) surface plus the handful
//! of backend leaves reached by `replication/libpqwalreceiver/libpqwalreceiver.c`
//! (and the libpqsrv be-fe helpers it wraps).
//!
//! There is no in-process libpq client in this tree yet, so every one of these
//! seams is **panic-until-bound**: a real call bottoms out in the macro's
//! uninstalled-stub panic until a libpq client (and the result/tuplestore
//! machinery) is ported and installs a provider.  This is the sanctioned route
//! for libpqwalreceiver — the whole module is glue over the libpq client API.
//!
//! The `PQ*` enum names (`ExecStatusType` / `ConnStatusType`) and `Pgsocket`
//! mirror the C libpq-fe.h types exactly; the parsed `ConninfoOption` mirrors
//! `PQconninfoOption`.  Connection / result objects cross as opaque handles
//! (`PgConnId` / `PgResultId`), owned by the future provider's registry.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use types_core::{Oid, TransactionId, XLogRecPtr};
use ::types_error::PgResult;
use types_libpqwalreceiver::{
    AttInMetadataId, ConnStatusType, ConninfoOption, ExecStatusType, HeapTupleId, MemoryContextId,
    PgConnId, PgResultId, Pgsocket, TupleDescId, TuplestoreId,
};

/// Opaque handle for a `TupleTableSlot *` made to iterate a `WalRcvExecResult`'s
/// tuplestore (slotsync.c `MakeTupleTableSlot(...)`).  Owned by the execTuples
/// subsystem behind the seam; `0` is the null/uninitialized handle.
pub type ResultTupslotId = usize;

// ===========================================================================
// libpq client transport (libpq-be-fe-helpers.h / libpq-fe.h).
// ===========================================================================

seam_core::seam!(
    /// `libpqsrv_connect_params(keys, vals, expand_dbname, wait_event_info)`
    /// (libpq/libpq-be-fe-helpers.h) — start a connection (with interrupt
    /// handling) and return the `PGconn *` handle.
    pub fn libpqsrv_connect_params(
        keys: Vec<String>,
        vals: Vec<Option<String>>,
        expand_dbname: bool,
        wait_event_info: u32
    ) -> PgConnId
);

seam_core::seam!(
    /// `libpqsrv_exec(conn, query, wait_event_info)` — send a query and wait
    /// for the (single) result.
    pub fn libpqsrv_exec(conn: PgConnId, query: String, wait_event_info: u32) -> PgResultId
);

seam_core::seam!(
    /// `libpqsrv_get_result(conn, wait_event_info)` — fetch the next result
    /// (`0` == NULL == no more results).
    pub fn libpqsrv_get_result(conn: PgConnId, wait_event_info: u32) -> PgResultId
);

seam_core::seam!(
    /// `libpqsrv_disconnect(conn)` — `PQfinish` with interrupt handling.
    pub fn libpqsrv_disconnect(conn: PgConnId)
);

seam_core::seam!(
    /// `PQstatus(conn)`.
    pub fn pq_status(conn: PgConnId) -> ConnStatusType
);

seam_core::seam!(
    /// `PQconnectionUsedPassword(conn)`.
    pub fn pq_connection_used_password(conn: PgConnId) -> bool
);

seam_core::seam!(
    /// `PQerrorMessage(conn)`.
    pub fn pq_error_message(conn: PgConnId) -> String
);

seam_core::seam!(
    /// `PQresultStatus(res)`.
    pub fn pq_result_status(res: PgResultId) -> ExecStatusType
);

seam_core::seam!(
    /// `PQresultErrorField(res, PG_DIAG_SQLSTATE)`.
    pub fn pq_result_error_field_sqlstate(res: PgResultId) -> Option<String>
);

seam_core::seam!(
    /// `PQclear(res)`.
    pub fn pq_clear(res: PgResultId)
);

seam_core::seam!(
    /// `PQnfields(res)`.
    pub fn pq_nfields(res: PgResultId) -> i32
);

seam_core::seam!(
    /// `PQntuples(res)`.
    pub fn pq_ntuples(res: PgResultId) -> i32
);

seam_core::seam!(
    /// `PQfname(res, field_num)`.
    pub fn pq_fname(res: PgResultId, field_num: i32) -> Option<String>
);

seam_core::seam!(
    /// `PQgetvalue(res, tup_num, field_num)` — returns the field bytes
    /// (binary-safe; for text fields these are NUL-free).
    pub fn pq_getvalue(res: PgResultId, tup_num: i32, field_num: i32) -> Vec<u8>
);

seam_core::seam!(
    /// `PQgetisnull(res, tup_num, field_num)`.
    pub fn pq_getisnull(res: PgResultId, tup_num: i32, field_num: i32) -> bool
);

seam_core::seam!(
    /// `PQgetlength(res, tup_num, field_num)`.
    pub fn pq_getlength(res: PgResultId, tup_num: i32, field_num: i32) -> i32
);

seam_core::seam!(
    /// `PQgetCopyData(conn, &buf, 1 /* async */)` — returns `(rawlen, buf)`.
    pub fn pq_get_copy_data(conn: PgConnId) -> (i32, Vec<u8>)
);

seam_core::seam!(
    /// `PQputCopyData(conn, buffer, nbytes)`.
    pub fn pq_put_copy_data(conn: PgConnId, buffer: Vec<u8>) -> i32
);

seam_core::seam!(
    /// `PQputCopyEnd(conn, NULL)`.
    pub fn pq_put_copy_end(conn: PgConnId) -> i32
);

seam_core::seam!(
    /// `PQflush(conn)`.
    pub fn pq_flush(conn: PgConnId) -> i32
);

seam_core::seam!(
    /// `PQconsumeInput(conn)`.
    pub fn pq_consume_input(conn: PgConnId) -> i32
);

seam_core::seam!(
    /// `PQsocket(conn)`.
    pub fn pq_socket(conn: PgConnId) -> Pgsocket
);

seam_core::seam!(
    /// `PQendcopy(conn)`.
    pub fn pq_endcopy(conn: PgConnId) -> i32
);

seam_core::seam!(
    /// `PQhost(conn)`.
    pub fn pq_host(conn: PgConnId) -> Option<String>
);

seam_core::seam!(
    /// `PQport(conn)`.
    pub fn pq_port(conn: PgConnId) -> Option<String>
);

seam_core::seam!(
    /// `PQserverVersion(conn)`.
    pub fn pq_server_version(conn: PgConnId) -> i32
);

seam_core::seam!(
    /// `PQbackendPID(conn)`.
    pub fn pq_backend_pid(conn: PgConnId) -> i32
);

seam_core::seam!(
    /// `PQconninfo(conn)` — the live connection's options (terminator stripped).
    pub fn pq_conninfo(conn: PgConnId) -> Option<Vec<ConninfoOption>>
);

seam_core::seam!(
    /// `PQconninfoParse(conninfo, &err)` — parse a conninfo string.  `Ok` is the
    /// option list (terminator stripped); `Err` carries the malloc'd error
    /// message (or `None` for out-of-memory).
    pub fn pq_conninfo_parse(conninfo: String) -> Result<Vec<ConninfoOption>, Option<String>>
);

seam_core::seam!(
    /// `PQescapeLiteral(conn, s, strlen(s))` (`None` on failure).
    pub fn pq_escape_literal(conn: PgConnId, s: String) -> Option<String>
);

seam_core::seam!(
    /// `PQescapeIdentifier(conn, s, strlen(s))` (`None` on failure).
    pub fn pq_escape_identifier(conn: PgConnId, s: String) -> Option<String>
);

// ===========================================================================
// Backend leaves reached by libpqrcv_processTuples / option assembly.
// ===========================================================================

seam_core::seam!(
    /// `GetDatabaseEncodingName()` (mb/pg_wchar.h).
    pub fn get_database_encoding_name() -> String
);

seam_core::seam!(
    /// `quote_identifier(ident)` (utils/adt/ruleutils.c).
    pub fn quote_identifier(ident: String) -> String
);

seam_core::seam!(
    /// `pg_strtoint32(s)` (utils/adt/numutils.c) — parse to int32, erroring on
    /// overflow / bad syntax.
    pub fn pg_strtoint32(s: String) -> PgResult<i32>
);

seam_core::seam!(
    /// `DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
    /// CStringGetDatum(s)))` — convert a C-string LSN to an `XLogRecPtr`.
    pub fn pg_lsn_in(value: Vec<u8>) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `MyDatabaseId` (miscadmin.h).
    pub fn my_database_id() -> Oid
);

seam_core::seam!(
    /// `work_mem` GUC (miscadmin.h).
    pub fn work_mem() -> i32
);

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` (miscadmin.h).
    pub fn check_for_interrupts() -> PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_begin_heap(randomAccess, interXact, maxKBytes)`
    /// (utils/sort/tuplestore.c).
    pub fn tuplestore_begin_heap(rand_access: bool, inter_xact: bool, max_kbytes: i32)
        -> TuplestoreId
);

seam_core::seam!(
    /// `CreateTemplateTupleDesc(natts)` (access/common/tupdesc.c).
    pub fn create_template_tuple_desc(natts: i32) -> TupleDescId
);

seam_core::seam!(
    /// `TupleDescInitEntry(desc, attno, name, oidtypeid, typmod, attdim)`.
    pub fn tuple_desc_init_entry(
        desc: TupleDescId,
        attribute_number: i16,
        name: Option<String>,
        oidtypeid: Oid,
        typmod: i32,
        attdim: i32
    )
);

seam_core::seam!(
    /// `TupleDescGetAttInMetadata(tupdesc)` (access/common/heaptuple.c).
    pub fn tuple_desc_get_att_in_metadata(tupdesc: TupleDescId) -> AttInMetadataId
);

seam_core::seam!(
    /// `BuildTupleFromCStrings(attinmeta, values)` (access/common/heaptuple.c).
    pub fn build_tuple_from_c_strings(
        attinmeta: AttInMetadataId,
        values: Vec<Option<Vec<u8>>>
    ) -> HeapTupleId
);

seam_core::seam!(
    /// `tuplestore_puttuple(state, tuple)` (utils/sort/tuplestore.c).
    pub fn tuplestore_puttuple(state: TuplestoreId, tuple: HeapTupleId)
);

seam_core::seam!(
    /// `AllocSetContextCreate(CurrentMemoryContext, name, ALLOCSET_DEFAULT_SIZES)`.
    pub fn alloc_set_context_create_default(name: String) -> MemoryContextId
);

seam_core::seam!(
    /// `MemoryContextSwitchTo(context)` — returns the previous context.
    pub fn memory_context_switch_to(context: MemoryContextId) -> MemoryContextId
);

seam_core::seam!(
    /// `MemoryContextReset(context)`.
    pub fn memory_context_reset(context: MemoryContextId)
);

seam_core::seam!(
    /// `MemoryContextDelete(context)`.
    pub fn memory_context_delete(context: MemoryContextId)
);

// ===========================================================================
// Result-tuplestore iteration (slotsync.c `tuplestore_gettupleslot` /
// `slot_getattr`); the slot machinery lives in the execTuples / tuplestore
// subsystems.  These back the `make_result_tupslot` / `result_gettupleslot` /
// `getattr_*` / `exec_clear_tuple` inward seams of libpqwalreceiver.
// ===========================================================================

seam_core::seam!(
    /// `MakeTupleTableSlot(tupledesc, &TTSOpsMinimalTuple)` (execTuples.c) — a
    /// slot suitable for iterating the result's tuplestore.
    pub fn make_tuple_table_slot(tupledesc: TupleDescId) -> ResultTupslotId
);

seam_core::seam!(
    /// `tuplestore_gettupleslot(state, true, false, tupslot)` — advance to the
    /// next tuple.  Returns false when the store is exhausted.
    pub fn tuplestore_gettupleslot(state: TuplestoreId, tupslot: ResultTupslotId) -> bool
);

seam_core::seam!(
    /// `TextDatumGetCString(slot_getattr(tupslot, col, &isnull))` — decode the
    /// column as `text`.  Returns `(value, isnull)`.
    pub fn slot_getattr_text(tupslot: ResultTupslotId, col: i32) -> PgResult<(Option<String>, bool)>
);

seam_core::seam!(
    /// `DatumGetLSN(slot_getattr(tupslot, col, &isnull))` — decode the column as
    /// `pg_lsn`.  Returns `(value, isnull)`.
    pub fn slot_getattr_lsn(tupslot: ResultTupslotId, col: i32) -> PgResult<(XLogRecPtr, bool)>
);

seam_core::seam!(
    /// `DatumGetTransactionId(slot_getattr(tupslot, col, &isnull))` — decode the
    /// column as `xid`.  Returns `(value, isnull)`.
    pub fn slot_getattr_xid(tupslot: ResultTupslotId, col: i32) -> PgResult<(TransactionId, bool)>
);

seam_core::seam!(
    /// `DatumGetBool(slot_getattr(tupslot, col, &isnull))` — decode the column as
    /// `bool`.  Returns `(value, isnull)`.
    pub fn slot_getattr_bool(tupslot: ResultTupslotId, col: i32) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `ExecClearTuple(tupslot)` (execTuples.c).
    pub fn exec_clear_tuple(tupslot: ResultTupslotId)
);

seam_core::seam!(
    /// `tuplestore_end(state)` (utils/sort/tuplestore.c) — free the tuplestore.
    pub fn tuplestore_end(state: TuplestoreId)
);

seam_core::seam!(
    /// `FreeTupleDesc(tupdesc)` (access/common/tupdesc.c).
    pub fn free_tuple_desc(tupdesc: TupleDescId)
);
