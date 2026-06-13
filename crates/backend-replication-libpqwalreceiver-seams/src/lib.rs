//! Seam declarations for the `backend-replication-libpqwalreceiver` unit
//! (`replication/libpqwalreceiver/libpqwalreceiver.c`) — the `WalReceiverConn`
//! hook implementations that are dynamically loaded as `WalReceiverFunctions`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. `load_libpqwalreceiver` corresponds to
//! `load_file("libpqwalreceiver", false)` plus the `WalReceiverFunctions !=
//! NULL` check (`elog(ERROR)` on failure).

use types_core::{pgsocket, TimeLineID};
use types_walreceiver::{WalRcvStreamOptions, WalReceiverConn};

seam_core::seam!(
    /// `load_file("libpqwalreceiver", false)` then verify
    /// `WalReceiverFunctions != NULL` (`elog(ERROR)` otherwise).
    pub fn load_libpqwalreceiver() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `walrcv_connect(conninfo, true, false, false, appname, &err)` — returns
    /// the opaque connection, or the error string (C: NULL + `err`).
    pub fn walrcv_connect(
        conninfo: String,
        appname: String
    ) -> Result<WalReceiverConn, String>
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
