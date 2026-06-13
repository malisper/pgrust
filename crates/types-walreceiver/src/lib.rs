//! Signature types for `replication/walreceiver.c` / `walreceiverfuncs.c` /
//! `libpqwalreceiver`.
//!
//! These owned types are shared by the walreceiver port and the owner-seam
//! crates it calls into.  The shared-memory `WalRcvData` control block stays
//! resident in its (not-yet-ported) `walreceiverfuncs` owner; the owned port
//! only ever sees the spinlocked snapshots the seams hand back and pushes
//! spinlocked updates back through them.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::string::String;

use types_core::{TimeLineID, TimestampTz, XLogRecPtr};

/// Opaque libpq connection handle (`WalReceiverConn *`).  The concrete struct
/// lives in the (separately ported) libpqwalreceiver module; here it is the
/// opaque token the libpqwalreceiver seams hand back and forth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalReceiverConn(pub usize);

/// `replication/walreceiver.h`: maximum size of a connection string.
pub const MAXCONNINFO: usize = 1024;
/// `pg_config_manual.h` / `c.h`: `NAMEDATALEN`.
pub const NAMEDATALEN: usize = 64;
/// `<netdb.h>`: `NI_MAXHOST`.
pub const NI_MAXHOST: usize = 1025;
/// `access/xlog_internal.h`: `MAXFNAMELEN`.
pub const MAXFNAMELEN: usize = 64;

/// `datatype/timestamp.h`: `+infinity` sentinel (`DT_NOEND` == `PG_INT64_MAX`).
pub const TIMESTAMP_INFINITY: TimestampTz = i64::MAX;

/// `#define NUM_WALRCV_WAKEUPS (WALRCV_WAKEUP_HSFEEDBACK + 1)`.
pub const NUM_WALRCV_WAKEUPS: usize = WalRcvWakeupReason::WALRCV_WAKEUP_HSFEEDBACK as usize + 1;

/// `Values for WalRcv->walRcvState` (`replication/walreceiver.h`).  Order /
/// discriminants match the C enum exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum WalRcvState {
    WALRCV_STOPPED = 0,
    WALRCV_STARTING = 1,
    WALRCV_STREAMING = 2,
    WALRCV_WAITING = 3,
    WALRCV_RESTARTING = 4,
    WALRCV_STOPPING = 5,
}

/// Reasons to wake up and perform periodic tasks (file-local enum in
/// walreceiver.c).  Discriminants are the array indices into `wakeup[]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalRcvWakeupReason {
    WALRCV_WAKEUP_TERMINATE = 0,
    WALRCV_WAKEUP_PING = 1,
    WALRCV_WAKEUP_REPLY = 2,
    WALRCV_WAKEUP_HSFEEDBACK = 3,
}

/// Options for `walrcv_startstreaming` (`replication/walreceiver.h`).  Only the
/// physical-replication fields are used by walreceiver.c.
#[derive(Clone, Debug)]
pub struct WalRcvStreamOptions {
    /// True if this is a logical replication stream, false if physical.
    pub logical: bool,
    /// LSN of starting point.
    pub startpoint: XLogRecPtr,
    /// Name of the replication slot or `None`.
    pub slotname: Option<String>,
    /// Physical-stream proto: starting timeline.
    pub physical_startpointTLI: TimeLineID,
}

impl Default for WalRcvStreamOptions {
    fn default() -> Self {
        WalRcvStreamOptions {
            logical: false,
            startpoint: 0,
            slotname: None,
            physical_startpointTLI: 0,
        }
    }
}

/// Snapshot of the streaming parameters read out of `WalRcv` under the spinlock
/// in `WalReceiverMain`'s startup section.
#[derive(Clone, Debug)]
pub struct WalRcvStartupInfo {
    /// `walrcv->conninfo` (NUL-padded fixed buffer).
    pub conninfo: [u8; MAXCONNINFO],
    /// `walrcv->slotname` (NUL-padded fixed buffer).
    pub slotname: [u8; NAMEDATALEN],
    /// `walrcv->is_temp_slot`.
    pub is_temp_slot: bool,
    /// `walrcv->receiveStart`.
    pub receive_start: XLogRecPtr,
    /// `walrcv->receiveStartTLI`.
    pub receive_start_tli: TimeLineID,
}

/// Consistent snapshot of `WalRcv` read under the spinlock at the top of
/// `pg_stat_get_wal_receiver`.
#[derive(Clone, Debug)]
pub struct WalRcvStatSnapshot {
    pub pid: i32,
    pub ready_to_display: bool,
    pub state: WalRcvState,
    pub receive_start_lsn: XLogRecPtr,
    pub receive_start_tli: TimeLineID,
    pub flushed_lsn: XLogRecPtr,
    pub received_tli: TimeLineID,
    pub last_send_time: TimestampTz,
    pub last_receipt_time: TimestampTz,
    pub latest_end_lsn: XLogRecPtr,
    pub latest_end_time: TimestampTz,
    pub slotname: String,
    pub sender_host: String,
    pub sender_port: i32,
    pub conninfo: String,
}

/// Structured form of the `pg_stat_get_wal_receiver` result row.  `None` fields
/// correspond to SQL NULL in the returned tuple.
#[derive(Clone, Debug, Default)]
pub struct WalReceiverActivity {
    pub pid: i32,
    pub state: Option<String>,
    pub receive_start_lsn: Option<XLogRecPtr>,
    pub receive_start_tli: Option<TimeLineID>,
    pub written_lsn: Option<XLogRecPtr>,
    pub flushed_lsn: Option<XLogRecPtr>,
    pub received_tli: Option<TimeLineID>,
    pub last_send_time: Option<TimestampTz>,
    pub last_receipt_time: Option<TimestampTz>,
    pub latest_end_lsn: Option<XLogRecPtr>,
    pub latest_end_time: Option<TimestampTz>,
    pub slotname: Option<String>,
    pub sender_host: Option<String>,
    pub sender_port: Option<i32>,
    pub conninfo: Option<String>,
}
