//! Signature types for `replication/walreceiver.c` / `walreceiverfuncs.c` /
//! `libpqwalreceiver`.
//!
//! These owned types are shared by the walreceiver port and the owner-seam
//! crates it calls into.  The shared-memory `WalRcvData` control block
//! (`replication/walreceiver.h`) is defined here as a real synchronized type:
//! the spinlock-guarded fields live behind a host mutex and the lock-free
//! `writtenUpto`/`force_reply` words are atomics, so the walreceiver port runs
//! its own `switch(walRcvState)` / state-transition logic under the lock while
//! the (not-yet-ported) `walreceiverfuncs` owner supplies the actual block via
//! the `with_walrcv` accessor seam.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::string::String;
use std::sync::atomic::{AtomicI32, AtomicU64};

use ::condvar::ConditionVariable;
use ::types_core::{pg_time_t, ProcNumber, TimeLineID, TimestampTz, XLogRecPtr};
use ::types_storage::storage::Spinlock;

/// Opaque libpq connection handle (`WalReceiverConn *`).  The concrete struct
/// lives in the (separately ported) libpqwalreceiver module; here it is the
/// opaque token the libpqwalreceiver seams hand back and forth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalReceiverConn(pub usize);

/// `WalRcvExecResult *` (`replication/walreceiver.h`) — the result of a
/// `walrcv_exec` query.  The concrete struct (status, sqlstate, err string,
/// `Tuplestorestate *`, `TupleDesc`) is libpqwalreceiver-owned; here it is the
/// opaque token the libpqwalreceiver result seams hand back and forth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalRcvExecResult(pub usize);

/// `TupleTableSlot *` made for iterating a `WalRcvExecResult`'s tuplestore
/// (`MakeTupleTableSlot(...)` in slotsync.c).  Opaque, owner-resident token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalRcvResultTupslot(pub usize);

/// `WalRcvExecStatus` (`replication/walreceiver.h`) — `walrcv_exec` result
/// status.  Discriminants match the C enum exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum WalRcvExecStatus {
    /// `WALRCV_ERROR` — There was error while executing the query.
    WALRCV_ERROR = 0,
    /// `WALRCV_OK_COMMAND` — Query executed utility or replication command.
    WALRCV_OK_COMMAND = 1,
    /// `WALRCV_OK_TUPLES` — Query returned tuples.
    WALRCV_OK_TUPLES = 2,
    /// `WALRCV_OK_COPY_IN` — Query started COPY FROM.
    WALRCV_OK_COPY_IN = 3,
    /// `WALRCV_OK_COPY_OUT` — Query started COPY TO.
    WALRCV_OK_COPY_OUT = 4,
    /// `WALRCV_OK_COPY_BOTH` — Query started COPY BOTH replication command.
    WALRCV_OK_COPY_BOTH = 5,
}

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

/// `WalRcvData` (`replication/walreceiver.h`) — the spinlock-guarded fields of
/// the WAL-receiver shared-memory control block.
///
/// Trimmed to the fields walreceiver.c actually reads or writes (the C struct
/// also carries `startTime` and `walRcvStoppedCV`, both reached separately).
/// The C `slock_t mutex` that guards these fields is the embedded [`Spinlock`]
/// in [`WalRcvShared`]; the lock-free `writtenUpto` and `force_reply` words sit
/// outside it as atomics.
///
/// This block lives in the REAL shared-memory segment (allocated by
/// `WalRcvShmemInit` via `ShmemInitStruct`), so the startup process and the
/// forked walreceiver process share it.  That forces a `#[repr(C)]`,
/// heap-pointer-free layout: the three C `char[]` fields are fixed-size,
/// NUL-padded byte arrays here (matching the C struct exactly), not Rust
/// `String`s.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct WalRcvData {
    /// `ProcNumber procno` — the active receiver's proc number.
    pub procno: ProcNumber,
    /// `pid_t pid` — the active receiver's PID (0 when none).
    pub pid: i32,
    /// `WalRcvState walRcvState`.
    pub walRcvState: WalRcvState,
    /// `XLogRecPtr receiveStart`.
    pub receiveStart: XLogRecPtr,
    /// `TimeLineID receiveStartTLI`.
    pub receiveStartTLI: TimeLineID,
    /// `XLogRecPtr flushedUpto`.
    pub flushedUpto: XLogRecPtr,
    /// `TimeLineID receivedTLI`.
    pub receivedTLI: TimeLineID,
    /// `XLogRecPtr latestChunkStart`.
    pub latestChunkStart: XLogRecPtr,
    /// `TimestampTz lastMsgSendTime`.
    pub lastMsgSendTime: TimestampTz,
    /// `TimestampTz lastMsgReceiptTime`.
    pub lastMsgReceiptTime: TimestampTz,
    /// `XLogRecPtr latestWalEnd`.
    pub latestWalEnd: XLogRecPtr,
    /// `TimestampTz latestWalEndTime`.
    pub latestWalEndTime: TimestampTz,
    /// `char conninfo[MAXCONNINFO]` — user-visible (obfuscated) conn string,
    /// NUL-terminated.
    pub conninfo: [u8; MAXCONNINFO],
    /// `char sender_host[NI_MAXHOST]`, NUL-terminated.
    pub sender_host: [u8; NI_MAXHOST],
    /// `int sender_port`.
    pub sender_port: i32,
    /// `char slotname[NAMEDATALEN]`, NUL-terminated.
    pub slotname: [u8; NAMEDATALEN],
    /// `bool is_temp_slot`.
    pub is_temp_slot: bool,
    /// `bool ready_to_display`.
    pub ready_to_display: bool,
}

impl core::fmt::Debug for WalRcvData {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WalRcvData")
            .field("procno", &self.procno)
            .field("pid", &self.pid)
            .field("walRcvState", &self.walRcvState)
            .field("receiveStart", &self.receiveStart)
            .field("flushedUpto", &self.flushedUpto)
            .field("receivedTLI", &self.receivedTLI)
            .finish_non_exhaustive()
    }
}

impl Default for WalRcvData {
    fn default() -> Self {
        WalRcvData {
            procno: ::types_core::INVALID_PROC_NUMBER,
            pid: 0,
            walRcvState: WalRcvState::WALRCV_STOPPED,
            receiveStart: 0,
            receiveStartTLI: 0,
            flushedUpto: 0,
            receivedTLI: 0,
            latestChunkStart: 0,
            lastMsgSendTime: 0,
            lastMsgReceiptTime: 0,
            latestWalEnd: 0,
            latestWalEndTime: 0,
            conninfo: [0; MAXCONNINFO],
            sender_host: [0; NI_MAXHOST],
            sender_port: 0,
            slotname: [0; NAMEDATALEN],
            is_temp_slot: false,
            ready_to_display: false,
        }
    }
}

/// `strlcpy(dst, src, dst.len())` over a fixed C `char[]` field: copy `src` up
/// to the first NUL (or its end), leaving room for a trailing NUL, and
/// NUL-pad the rest. Mirrors the C `strlcpy(WalRcv->field, src, sizeof field)`.
pub fn walrcv_strlcpy<const N: usize>(dst: &mut [u8; N], src: &[u8]) {
    let src_len = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    let n = src_len.min(N - 1);
    *dst = [0; N];
    dst[..n].copy_from_slice(&src[..n]);
}

/// Render a fixed C `char[]` field (NUL-terminated) as an owned `String`,
/// stopping at the first NUL. The inverse of [`walrcv_strlcpy`].
pub fn walrcv_cstr_to_string(src: &[u8]) -> String {
    let end = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    String::from_utf8_lossy(&src[..end]).into_owned()
}

/// The whole shared-memory `WalRcvData` control block, laid out `#[repr(C)]`
/// for residence in the real shared-memory segment (`ShmemInitStruct`), so the
/// startup process and the forked walreceiver share it.
///
/// `mutex` is the genuine `slock_t` spinlock guarding `data`, `startTime`, and
/// the field-by-field updates of `walReceiverData`; `writtenUpto` /
/// `force_reply` are the two lock-free words.  The walreceiver port reaches the
/// guarded fields through the `with_walrcv` accessor seam, which takes
/// `mutex` around the caller's closure exactly like
/// `SpinLockAcquire`/`SpinLockRelease` bracket the C code.
#[repr(C)]
pub struct WalRcvShared {
    /// `slock_t mutex` — guards `data` (and `startTime`).
    pub mutex: Spinlock,
    /// The spinlock-guarded fields.
    pub data: WalRcvData,
    /// `pg_time_t startTime` — guarded by `mutex`.
    pub startTime: pg_time_t,
    /// `ConditionVariable walRcvStoppedCV`.
    pub walRcvStoppedCV: ConditionVariable,
    /// `pg_atomic_uint64 writtenUpto`.
    pub writtenUpto: AtomicU64,
    /// `sig_atomic_t force_reply` (used as a bool).
    pub force_reply: AtomicI32,
}

impl Default for WalRcvShared {
    fn default() -> Self {
        WalRcvShared {
            mutex: Spinlock::new(),
            data: WalRcvData::default(),
            startTime: 0,
            walRcvStoppedCV: ConditionVariable::new(),
            writtenUpto: AtomicU64::new(0),
            force_reply: AtomicI32::new(0),
        }
    }
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
