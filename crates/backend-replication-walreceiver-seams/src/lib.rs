//! Seam declarations for the `backend-replication-walreceiver` unit
//! (`replication/walreceiver.c` + the `libpqwalreceiver` connector and its
//! result-tuple iteration).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The receiver connection, its execution result,
//! and the per-row tuple are owner-resident objects named by their handles.

#![allow(non_snake_case)]

use types_core::primitive::{Oid, TransactionId, XLogRecPtr};
use types_error::PgResult;
use types_replication::{TupleTableSlotHandle, WalRcvExecResultHandle, WalRcvExecStatus, WrConnHandle};

seam_core::seam!(
    /// `LoadedWalrcv ... load_file("libpqwalreceiver", false)` — load the
    /// libpq-based walreceiver connector functions.
    pub fn load_libpqwalreceiver() -> PgResult<()>
);

seam_core::seam!(
    /// `hot_standby_feedback` GUC (walreceiver.c) — whether the standby sends
    /// xmin feedback to the primary.
    pub fn hot_standby_feedback() -> bool
);

seam_core::seam!(
    /// `walrcv_get_dbname_from_conninfo(conninfo)` — parse the `dbname` out of a
    /// connection string. `None` when not present.
    pub fn walrcv_get_dbname_from_conninfo(conninfo: &str) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `walrcv_connect(conninfo, replication, logical, must_use_password,
    /// appname, &err)`. Returns the connection handle (or
    /// [`WrConnHandle::NONE`] on failure) and the C `*err` string.
    pub fn walrcv_connect(
        conninfo: &str,
        replication: bool,
        logical: bool,
        must_use_password: bool,
        appname: &str,
    ) -> PgResult<(WrConnHandle, Option<String>)>
);

seam_core::seam!(
    /// `walrcv_disconnect(wrconn)`.
    pub fn walrcv_disconnect(wrconn: WrConnHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `walrcv_exec(wrconn, query, nRetTypes, retTypes)` — run a SQL query over
    /// the replication connection, returning a result handle.
    pub fn walrcv_exec(
        wrconn: WrConnHandle,
        query: &str,
        nret: i32,
        ret_types: &[Oid],
    ) -> PgResult<WalRcvExecResultHandle>
);

seam_core::seam!(
    /// `walrcv_clear_result(res)`.
    pub fn walrcv_clear_result(res: WalRcvExecResultHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `res->status`.
    pub fn res_status(res: WalRcvExecResultHandle) -> WalRcvExecStatus
);
seam_core::seam!(
    /// `res->err`.
    pub fn res_err(res: WalRcvExecResultHandle) -> Option<String>
);

seam_core::seam!(
    /// `MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple)` — create
    /// the slot used to iterate `res->tuplestore`.
    pub fn make_result_tupslot(res: WalRcvExecResultHandle) -> PgResult<TupleTableSlotHandle>
);
seam_core::seam!(
    /// `tuplestore_gettupleslot(res->tuplestore, true, false, tupslot)` — fetch
    /// the next row into `tupslot`; `false` when exhausted.
    pub fn result_gettupleslot(
        res: WalRcvExecResultHandle,
        tupslot: TupleTableSlotHandle,
    ) -> PgResult<bool>
);
seam_core::seam!(
    /// `ExecClearTuple(tupslot)`.
    pub fn exec_clear_tuple(tupslot: TupleTableSlotHandle) -> PgResult<()>
);

// --- per-column extraction (`slot_getattr` + the type's output codec) ---
// Each returns `(value, isnull)`; the value is meaningful only when `!isnull`.

seam_core::seam!(
    /// `TextDatumGetCString(slot_getattr(tupslot, col, &isnull))`.
    pub fn getattr_text(tupslot: TupleTableSlotHandle, col: i32) -> PgResult<(Option<String>, bool)>
);
seam_core::seam!(
    /// `DatumGetLSN(slot_getattr(tupslot, col, &isnull))`.
    pub fn getattr_lsn(tupslot: TupleTableSlotHandle, col: i32) -> PgResult<(XLogRecPtr, bool)>
);
seam_core::seam!(
    /// `DatumGetTransactionId(slot_getattr(tupslot, col, &isnull))`.
    pub fn getattr_xid(tupslot: TupleTableSlotHandle, col: i32) -> PgResult<(TransactionId, bool)>
);
seam_core::seam!(
    /// `DatumGetBool(slot_getattr(tupslot, col, &isnull))`.
    pub fn getattr_bool(tupslot: TupleTableSlotHandle, col: i32) -> PgResult<(bool, bool)>
);
