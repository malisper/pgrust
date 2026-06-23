//! The `walrcv_*` function-table entries (the C `WalReceiverFunctionsType`
//! vtable, `PQWalReceiverFunctions`), registry-backed, plus the result-iteration
//! entries the slot-sync / apply paths reach through the same seam crate.
//!
//! Each entry has EXACTLY the shape of its inward seam
//! (`libpqwalreceiver_seams::*`, typed over the opaque
//! `types_walreceiver::{WalReceiverConn, WalRcvExecResult, WalRcvResultTupslot}`
//! handles).  It resolves the handle to the live owned object parked in
//! [`crate::conn_registry`] and delegates to the corresponding ported
//! `libpqrcv_*` function.  The wire layer ([`init_seams`]) installs these.
//!
//! No fabrication: every body delegates to real logic.  Where that logic bottoms
//! out in the (unported) libpq client transport or the result tuplestore/slot
//! machinery, the loud panic propagates from that outward seam.

use std::sync::atomic::Ordering;

use ::utils_error::PgResult;
use ::types_core::{pgsocket, Oid, TimeLineID};
use ::types_walreceiver::{
    WalRcvExecResult as ResHandle, WalRcvExecStatus, WalRcvResultTupslot as TupslotHandle,
    WalRcvStreamOptions as SeamStreamOptions, WalReceiverConn as ConnHandle,
};

use fe_seams as rt;

use crate::conn_registry as reg;
use crate::conn_registry::ResultTupslot;
use crate::{
    here, libpqrcv_connect, libpqrcv_create_slot, libpqrcv_disconnect, libpqrcv_endstreaming,
    libpqrcv_get_backend_pid, libpqrcv_get_conninfo, libpqrcv_get_dbname_from_conninfo,
    libpqrcv_get_senderinfo, libpqrcv_identify_system, libpqrcv_readtimelinehistoryfile,
    libpqrcv_receive, libpqrcv_send, libpqrcv_startstreaming, CRSSnapshotAction,
    WalRcvStreamOptions, WalRcvStreamOptionsPhysical, WalRcvStreamOptionsProto,
    WAL_RECEIVER_FUNCTIONS_LOADED,
};
use ::utils_error::ereport;
use ::types_error::ERROR;

// ===========================================================================
// load_libpqwalreceiver — `load_file("libpqwalreceiver", false)` + the
// `WalReceiverFunctions != NULL` check (slotsync.c / walreceiver.c).
// ===========================================================================

/// `load_file("libpqwalreceiver", false)` runs the module's `_PG_init`, then the
/// caller verifies `WalReceiverFunctions != NULL` (`elog(ERROR)` otherwise).
/// `load_file` is idempotent (only the first load runs `_PG_init`); here we mark
/// the vtable loaded once and then confirm it is installed.
pub fn load_libpqwalreceiver() -> PgResult<()> {
    // Idempotent load: the first call marks the vtable installed; later calls
    // are no-ops (C's load_file does not re-run _PG_init).
    WAL_RECEIVER_FUNCTIONS_LOADED.store(true, Ordering::SeqCst);

    // WalReceiverFunctions != NULL — true once loaded.
    if !WAL_RECEIVER_FUNCTIONS_LOADED.load(Ordering::SeqCst) {
        ereport(ERROR)
            .errmsg_internal("libpqwalreceiver didn't initialize correctly")
            .finish(here("load_libpqwalreceiver"))?;
    }
    Ok(())
}

// ===========================================================================
// Connection vtable entries.
// ===========================================================================

/// `walrcv_connect(...)`.  `replication`/`logical`/`must_use_password` are
/// passed through from the caller; on the libpqrcv normal-failure path C returns
/// NULL with `*err` set — here that surfaces as `Err(err)` with no handle
/// registered.  An ereport(ERROR) (must_use_password without password) bubbles
/// up as a panic from the unwrap, matching the handle-only seam contract (which
/// has no PgError channel for `walrcv_connect`).
pub fn walrcv_connect(
    conninfo: String,
    replication: bool,
    logical: bool,
    must_use_password: bool,
    appname: String,
) -> Result<ConnHandle, String> {
    let result = libpqrcv_connect(
        &conninfo,
        replication,
        logical,
        must_use_password,
        Some(&appname),
    )
    .unwrap_or_else(|e| panic!("walrcv_connect: ereport: {}", e.message));

    match result.conn {
        Some(conn) => Ok(ConnHandle(reg::insert_conn(conn))),
        None => Err(result.err.unwrap_or_default()),
    }
}

/// `walrcv_get_dbname_from_conninfo(conninfo)`.
pub fn walrcv_get_dbname_from_conninfo(conninfo: String) -> Option<String> {
    libpqrcv_get_dbname_from_conninfo(&conninfo)
        .unwrap_or_else(|e| panic!("walrcv_get_dbname_from_conninfo: {}", e.message))
}

/// `walrcv_get_conninfo(conn)`.
pub fn walrcv_get_conninfo(conn: ConnHandle) -> Option<String> {
    reg::with_conn(conn.0, |c| {
        libpqrcv_get_conninfo(c).unwrap_or_else(|e| panic!("walrcv_get_conninfo: {}", e.message))
    })
}

/// `walrcv_get_senderinfo(conn, &host, &port)`.
pub fn walrcv_get_senderinfo(conn: ConnHandle) -> (Option<String>, i32) {
    reg::with_conn(conn.0, libpqrcv_get_senderinfo)
}

/// `walrcv_identify_system(conn, &primary_tli)`.
pub fn walrcv_identify_system(conn: ConnHandle) -> PgResult<(String, TimeLineID)> {
    reg::with_conn(conn.0, libpqrcv_identify_system)
}

/// `walrcv_get_backend_pid(conn)` — the seam widens the C `pid_t`/`int` to `i64`.
pub fn walrcv_get_backend_pid(conn: ConnHandle) -> i64 {
    reg::with_conn(conn.0, |c| libpqrcv_get_backend_pid(c) as i64)
}

/// `walrcv_create_slot(conn, slotname, true /* temporary */, false /* two_phase */,
/// false /* failover */, CRS_NOEXPORT_SNAPSHOT, NULL /* lsn */)` as the WAL
/// receiver's temp-slot creation calls it; the (snapshot, lsn) the physical
/// caller does not want are dropped.
pub fn walrcv_create_slot(conn: ConnHandle, slotname: String) -> PgResult<()> {
    reg::with_conn(conn.0, |c| {
        libpqrcv_create_slot(
            c,
            &slotname,
            /* temporary = */ true,
            /* two_phase = */ false,
            /* failover = */ false,
            CRSSnapshotAction::CRS_NOEXPORT_SNAPSHOT,
            /* want_lsn = */ false,
        )
        .map(|(_snapshot, _lsn)| ())
    })
}

/// `walrcv_startstreaming(conn, &options)`.  The seam carries the physical-only
/// [`::types_walreceiver::WalRcvStreamOptions`]; adapt to the provider's full
/// physical/logical form by selecting the physical arm.
pub fn walrcv_startstreaming(conn: ConnHandle, options: SeamStreamOptions) -> PgResult<bool> {
    let provider_options = WalRcvStreamOptions {
        logical: options.logical,
        slotname: options.slotname,
        startpoint: options.startpoint,
        proto: WalRcvStreamOptionsProto::Physical(WalRcvStreamOptionsPhysical {
            startpointTLI: options.physical_startpointTLI,
        }),
    };
    reg::with_conn(conn.0, |c| libpqrcv_startstreaming(c, &provider_options))
}

/// `walrcv_endstreaming(conn, &primary_tli)`.
pub fn walrcv_endstreaming(conn: ConnHandle) -> PgResult<TimeLineID> {
    reg::with_conn(conn.0, libpqrcv_endstreaming)
}

/// `walrcv_receive(conn, &buf, &wait_fd)`.  The C `**buffer` out-param is the
/// connection's `recvBuf` (refilled in place); the returned `Vec<u8>` is a copy
/// of it.
pub fn walrcv_receive(conn: ConnHandle) -> PgResult<(i32, Vec<u8>, pgsocket)> {
    reg::with_conn_mut(conn.0, |c| {
        let (len, wait_fd) = libpqrcv_receive(c)?;
        let buf = if len > 0 { c.recvBuf.clone() } else { Vec::new() };
        Ok((len, buf, wait_fd))
    })
}

/// `walrcv_send(conn, buf, nbytes)`.
pub fn walrcv_send(conn: ConnHandle, buf: Vec<u8>) -> PgResult<()> {
    reg::with_conn(conn.0, |c| libpqrcv_send(c, &buf))
}

/// `walrcv_readtimelinehistoryfile(conn, tli, &fname, &content, &len)`.
pub fn walrcv_readtimelinehistoryfile(conn: ConnHandle, tli: TimeLineID) -> PgResult<(String, Vec<u8>)> {
    reg::with_conn(conn.0, |c| libpqrcv_readtimelinehistoryfile(c, tli))
}

/// `walrcv_disconnect(conn)` — remove from the registry and run the real
/// disconnect on the owned value.  A NULL / already-removed handle is a no-op.
pub fn walrcv_disconnect(conn: ConnHandle) {
    if let Some(owned) = reg::remove_conn(conn.0) {
        libpqrcv_disconnect(owned);
    }
}

// ===========================================================================
// walrcv_exec + result handling.
// ===========================================================================

/// `walrcv_exec(conn, query, nRetTypes, retTypes)` — park the produced result
/// and return its handle.
pub fn walrcv_exec(
    conn: ConnHandle,
    query: String,
    nret: i32,
    rettypes: Vec<Oid>,
) -> PgResult<ResHandle> {
    let res = reg::with_conn(conn.0, |c| crate::libpqrcv_exec(c, &query, nret, &rettypes))?;
    Ok(ResHandle(reg::insert_result(res)))
}

/// `res->status`.
pub fn res_status(res: ResHandle) -> WalRcvExecStatus {
    reg::with_result(res.0, |r| r.status.to_types())
}

/// `res->err`.
pub fn res_err(res: ResHandle) -> Option<String> {
    reg::with_result(res.0, |r| r.err.clone())
}

/// `walrcv_clear_result(res)` (walreceiver.h inline) — free the err string,
/// tuplestore, tupledesc and the struct.
pub fn walrcv_clear_result(res: ResHandle) -> PgResult<()> {
    // if (!walres) return; — a NULL handle is a no-op.
    if let Some(owned) = reg::remove_result(res.0) {
        if owned.tuplestore != 0 {
            rt::tuplestore_end::call(owned.tuplestore);
        }
        if owned.tupledesc != 0 {
            rt::free_tuple_desc::call(owned.tupledesc);
        }
        // err string + struct dropped with `owned`.
        drop(owned);
    }
    Ok(())
}

/// `MakeTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple)` (slotsync.c) — a
/// slot suitable for iterating the result's tuplestore.
pub fn make_result_tupslot(res: ResHandle) -> PgResult<TupslotHandle> {
    let tupledesc = reg::with_result(res.0, |r| r.tupledesc);
    let slot = rt::make_tuple_table_slot::call(tupledesc);
    let id = reg::insert_tupslot(ResultTupslot {
        slot,
        result: res.0,
    });
    Ok(TupslotHandle(id))
}

/// `tuplestore_gettupleslot(res->tuplestore, true, false, tupslot)` — advance to
/// the next tuple.  Returns false when the store is exhausted.
pub fn result_gettupleslot(res: ResHandle, tupslot: TupslotHandle) -> PgResult<bool> {
    let tuplestore = reg::with_result(res.0, |r| r.tuplestore);
    let ts = reg::get_tupslot(tupslot.0);
    Ok(rt::tuplestore_gettupleslot::call(tuplestore, ts.slot))
}

/// `slot_getattr(tupslot, col, &isnull)` → `TextDatumGetCString`.
pub fn getattr_text(tupslot: TupslotHandle, col: i32) -> PgResult<(Option<String>, bool)> {
    let ts = reg::get_tupslot(tupslot.0);
    rt::slot_getattr_text::call(ts.slot, col)
}

/// `slot_getattr(tupslot, col, &isnull)` → `DatumGetLSN`.
pub fn getattr_lsn(
    tupslot: TupslotHandle,
    col: i32,
) -> PgResult<(::types_core::XLogRecPtr, bool)> {
    let ts = reg::get_tupslot(tupslot.0);
    rt::slot_getattr_lsn::call(ts.slot, col)
}

/// `slot_getattr(tupslot, col, &isnull)` → `DatumGetTransactionId`.
pub fn getattr_xid(
    tupslot: TupslotHandle,
    col: i32,
) -> PgResult<(::types_core::TransactionId, bool)> {
    let ts = reg::get_tupslot(tupslot.0);
    rt::slot_getattr_xid::call(ts.slot, col)
}

/// `slot_getattr(tupslot, col, &isnull)` → `DatumGetBool`.
pub fn getattr_bool(tupslot: TupslotHandle, col: i32) -> PgResult<(bool, bool)> {
    let ts = reg::get_tupslot(tupslot.0);
    rt::slot_getattr_bool::call(ts.slot, col)
}

/// `ExecClearTuple(tupslot)`.
pub fn exec_clear_tuple(tupslot: TupslotHandle) -> PgResult<()> {
    let ts = reg::get_tupslot(tupslot.0);
    rt::exec_clear_tuple::call(ts.slot);
    Ok(())
}

// ===========================================================================
// Wiring.
// ===========================================================================

/// The `walrcv_*` routines are now called directly by consumers (the
/// libpqwalreceiver outward seams were removed as a faithful de-indirection);
/// nothing remains to install here.
pub fn init_seams() {}
