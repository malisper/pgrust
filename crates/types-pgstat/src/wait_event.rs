//! Wait-event classification vocabulary: the class bases from
//! `utils/wait_classes.h` plus the generated per-class event ids ports
//! consume so far (`wait_event_types.h`, generated from
//! `wait_event_names.txt` — first event of a class == the class base, then
//! +1 in file order).

pub const PG_WAIT_LWLOCK: u32 = 0x01000000;
pub const PG_WAIT_LOCK: u32 = 0x03000000;
pub const PG_WAIT_BUFFERPIN: u32 = 0x04000000;
pub const PG_WAIT_ACTIVITY: u32 = 0x05000000;
pub const PG_WAIT_CLIENT: u32 = 0x06000000;
pub const PG_WAIT_EXTENSION: u32 = 0x07000000;
pub const PG_WAIT_IPC: u32 = 0x08000000;
pub const PG_WAIT_TIMEOUT: u32 = 0x09000000;
pub const PG_WAIT_IO: u32 = 0x0A000000;
pub const PG_WAIT_INJECTIONPOINT: u32 = 0x0B000000;

/// `WAIT_EVENT_ARCHIVER_MAIN` — 1st entry (index 0) of the Activity section
/// of `wait_event_names.txt`, so `PG_WAIT_ACTIVITY + 0` (= 0x05000000,
/// matching c2rust's 83886080).
pub const WAIT_EVENT_ARCHIVER_MAIN: u32 = PG_WAIT_ACTIVITY + 0;

/// `WAIT_EVENT_SYSLOGGER_MAIN` — 14th entry (index 13) of the Activity
/// section of `wait_event_names.txt` (ARCHIVER_MAIN, AUTOVACUUM_MAIN,
/// BGWRITER_HIBERNATE, BGWRITER_MAIN, CHECKPOINTER_MAIN,
/// CHECKPOINTER_SHUTDOWN, IO_WORKER_MAIN, LOGICAL_APPLY_MAIN,
/// LOGICAL_LAUNCHER_MAIN, LOGICAL_PARALLEL_APPLY_MAIN, RECOVERY_WAL_STREAM,
/// REPLICATION_SLOTSYNC_MAIN, REPLICATION_SLOTSYNC_SHUTDOWN,
/// SYSLOGGER_MAIN, ...).
pub const WAIT_EVENT_SYSLOGGER_MAIN: u32 = PG_WAIT_ACTIVITY + 13;

/// `WAIT_EVENT_MESSAGE_QUEUE_INTERNAL` — "Waiting for another process to be
/// attached to a shared message queue." 34th entry (0-based 33) of the IPC
/// section, so `PG_WAIT_IPC | 33` (= 134217761, matching c2rust).
pub const WAIT_EVENT_MESSAGE_QUEUE_INTERNAL: u32 = PG_WAIT_IPC | 33;

/// `WAIT_EVENT_MESSAGE_QUEUE_RECEIVE` — "Waiting to receive bytes from a
/// shared message queue." `PG_WAIT_IPC | 35` (= 134217763, matching c2rust).
pub const WAIT_EVENT_MESSAGE_QUEUE_RECEIVE: u32 = PG_WAIT_IPC | 35;

/// `WAIT_EVENT_MESSAGE_QUEUE_SEND` — "Waiting to send bytes to a shared
/// message queue." `PG_WAIT_IPC | 36` (= 134217764, matching c2rust).
pub const WAIT_EVENT_MESSAGE_QUEUE_SEND: u32 = PG_WAIT_IPC | 36;

/// `WAIT_EVENT_SPIN_DELAY` — "Waiting while acquiring a contended spinlock."
/// 7th entry (0-based 6) of the `WaitEventTimeout` section, so the generated
/// enum value is `PG_WAIT_TIMEOUT | 6` (= 150994950, matching c2rust).
pub const WAIT_EVENT_SPIN_DELAY: u32 = PG_WAIT_TIMEOUT | 6;
