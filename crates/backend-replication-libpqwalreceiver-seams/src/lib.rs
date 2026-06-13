//! Seam declarations for the `backend-replication-libpqwalreceiver` unit
//! (`replication/libpqwalreceiver/libpqwalreceiver.c`) — the `WalReceiverConn`
//! hook implementations that are dynamically loaded as `WalReceiverFunctions`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. `load_libpqwalreceiver` corresponds to
//! `load_file("libpqwalreceiver", false)` plus the `WalReceiverFunctions !=
//! NULL` check (`elog(ERROR)` on failure).

use types_core::{pgsocket, Oid, TimeLineID, TransactionId, XLogRecPtr};
use types_walreceiver::{
    WalRcvExecResult, WalRcvExecStatus, WalRcvResultTupslot, WalRcvStreamOptions, WalReceiverConn,
};

seam_core::seam!(
    /// `load_file("libpqwalreceiver", false)` then verify
    /// `WalReceiverFunctions != NULL` (`elog(ERROR)` otherwise).
    pub fn load_libpqwalreceiver() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `walrcv_connect(conninfo, replication, logical, must_use_password,
    /// appname, &err)` — establish a connection to a cluster.  `replication`
    /// is true for a replication connection (then `logical` chooses logical vs
    /// physical), false for a regular connection (slotsync uses a regular
    /// connection so `walrcv_exec` can run catalog queries).  Returns the
    /// opaque connection, or the error string (C: NULL + `err`).
    pub fn walrcv_connect(
        conninfo: String,
        replication: bool,
        logical: bool,
        must_use_password: bool,
        appname: String
    ) -> Result<WalReceiverConn, String>
);

seam_core::seam!(
    /// `walrcv_exec(conn, query, nRetTypes, retTypes)` — run a SQL command on
    /// the connection (which must be a non-replication connection).  Returns
    /// the result; the caller inspects `res_status`/`res_err`.  `ereport(ERROR)`
    /// is possible on protocol failure.
    pub fn walrcv_exec(
        conn: WalReceiverConn,
        query: String,
        nret: i32,
        rettypes: Vec<Oid>
    ) -> types_error::PgResult<WalRcvExecResult>
);

seam_core::seam!(
    /// `res->status` — the [`WalRcvExecStatus`] of a `walrcv_exec` result.
    pub fn res_status(res: WalRcvExecResult) -> WalRcvExecStatus
);

seam_core::seam!(
    /// `res->err` — the error string of a failed `walrcv_exec` result, if any.
    pub fn res_err(res: WalRcvExecResult) -> Option<String>
);

seam_core::seam!(
    /// `MakeTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple)` (slotsync.c) —
    /// a slot suitable for iterating the result's tuplestore.
    pub fn make_result_tupslot(res: WalRcvExecResult)
        -> types_error::PgResult<WalRcvResultTupslot>
);

seam_core::seam!(
    /// `tuplestore_gettupleslot(res->tuplestore, true, false, tupslot)` —
    /// advance to the next tuple.  Returns false when the store is exhausted.
    pub fn result_gettupleslot(
        res: WalRcvExecResult,
        tupslot: WalRcvResultTupslot
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `slot_getattr(tupslot, col, &isnull)` decoded as `text` →
    /// `TextDatumGetCString`.  Returns `(value, isnull)`.
    pub fn getattr_text(
        tupslot: WalRcvResultTupslot,
        col: i32
    ) -> types_error::PgResult<(Option<String>, bool)>
);

seam_core::seam!(
    /// `slot_getattr(tupslot, col, &isnull)` decoded as `pg_lsn` →
    /// `DatumGetLSN`.  Returns `(value, isnull)`.
    pub fn getattr_lsn(
        tupslot: WalRcvResultTupslot,
        col: i32
    ) -> types_error::PgResult<(XLogRecPtr, bool)>
);

seam_core::seam!(
    /// `slot_getattr(tupslot, col, &isnull)` decoded as `xid` →
    /// `DatumGetTransactionId`.  Returns `(value, isnull)`.
    pub fn getattr_xid(
        tupslot: WalRcvResultTupslot,
        col: i32
    ) -> types_error::PgResult<(TransactionId, bool)>
);

seam_core::seam!(
    /// `slot_getattr(tupslot, col, &isnull)` decoded as `bool` →
    /// `DatumGetBool`.  Returns `(value, isnull)`.
    pub fn getattr_bool(
        tupslot: WalRcvResultTupslot,
        col: i32
    ) -> types_error::PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `ExecClearTuple(tupslot)` — clear the result iteration slot.
    pub fn exec_clear_tuple(tupslot: WalRcvResultTupslot) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `walrcv_clear_result(res)` — free a `walrcv_exec` result (err string,
    /// tuplestore, tupledesc, and the struct).
    pub fn walrcv_clear_result(res: WalRcvExecResult) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `walrcv_get_dbname_from_conninfo(conninfo)` — extract the `dbname`
    /// connection option, or `None` if not present.
    pub fn walrcv_get_dbname_from_conninfo(conninfo: String) -> Option<String>
);

seam_core::seam!(
    /// `walrcv_get_conninfo(conn)`.
    pub fn walrcv_get_conninfo(conn: WalReceiverConn) -> Option<String>
);

seam_core::seam!(
    /// `walrcv_get_senderinfo(conn, &host, &port)`.
    pub fn walrcv_get_senderinfo(conn: WalReceiverConn) -> (Option<String>, i32)
);

seam_core::seam!(
    /// `walrcv_identify_system(conn, &primary_tli)` — `ereport(ERROR)` on
    /// protocol failure.
    pub fn walrcv_identify_system(
        conn: WalReceiverConn
    ) -> types_error::PgResult<(String, TimeLineID)>
);

seam_core::seam!(
    /// `walrcv_get_backend_pid(conn)`.
    pub fn walrcv_get_backend_pid(conn: WalReceiverConn) -> i64
);

seam_core::seam!(
    /// `walrcv_create_slot(conn, slotname, true, false, false, 0, NULL)`.
    pub fn walrcv_create_slot(
        conn: WalReceiverConn,
        slotname: String
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `walrcv_startstreaming(conn, &options)` — true if streaming started.
    pub fn walrcv_startstreaming(
        conn: WalReceiverConn,
        options: WalRcvStreamOptions
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `walrcv_endstreaming(conn, &primary_tli)`.
    pub fn walrcv_endstreaming(conn: WalReceiverConn) -> types_error::PgResult<TimeLineID>
);

seam_core::seam!(
    /// `walrcv_receive(conn, &buf, &wait_fd)` — returns (len, buf, wait_fd).
    /// `len < 0` ⇒ end of COPY, `len == 0` ⇒ would block.
    pub fn walrcv_receive(
        conn: WalReceiverConn
    ) -> types_error::PgResult<(i32, Vec<u8>, pgsocket)>
);

seam_core::seam!(
    /// `walrcv_send(conn, buf, nbytes)`.
    pub fn walrcv_send(conn: WalReceiverConn, buf: Vec<u8>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `walrcv_readtimelinehistoryfile(conn, tli, &fname, &content, &len)`.
    pub fn walrcv_readtimelinehistoryfile(
        conn: WalReceiverConn,
        tli: TimeLineID
    ) -> types_error::PgResult<(String, Vec<u8>)>
);

seam_core::seam!(
    /// `walrcv_disconnect(conn)`.
    pub fn walrcv_disconnect(conn: WalReceiverConn)
);
