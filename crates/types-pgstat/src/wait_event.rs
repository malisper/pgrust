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

/// `WAIT_EVENT_SYSLOGGER_MAIN` — 14th entry (index 13) of the Activity
/// section of `wait_event_names.txt` (ARCHIVER_MAIN, AUTOVACUUM_MAIN,
/// BGWRITER_HIBERNATE, BGWRITER_MAIN, CHECKPOINTER_MAIN,
/// CHECKPOINTER_SHUTDOWN, IO_WORKER_MAIN, LOGICAL_APPLY_MAIN,
/// LOGICAL_LAUNCHER_MAIN, LOGICAL_PARALLEL_APPLY_MAIN, RECOVERY_WAL_STREAM,
/// REPLICATION_SLOTSYNC_MAIN, REPLICATION_SLOTSYNC_SHUTDOWN,
/// SYSLOGGER_MAIN, ...).
pub const WAIT_EVENT_SYSLOGGER_MAIN: u32 = PG_WAIT_ACTIVITY + 13;

/// `WAIT_EVENT_SPIN_DELAY` — "Waiting while acquiring a contended spinlock."
/// 7th entry (0-based 6) of the `WaitEventTimeout` section, so the generated
/// enum value is `PG_WAIT_TIMEOUT | 6` (= 150994950, matching c2rust).
pub const WAIT_EVENT_SPIN_DELAY: u32 = PG_WAIT_TIMEOUT | 6;

/// `WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN` — 10th entry (index 9) of the
/// `WaitEventActivity` section. The parallel-apply worker's idle wait in
/// `LogicalParallelApplyLoop`.
pub const WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN: u32 = PG_WAIT_ACTIVITY + 9;

/// `WAIT_EVENT_LOGICAL_APPLY_SEND_DATA` — index 29 of the `WaitEventIPC`
/// section. The leader's wait in `pa_send_data` while the queue is full.
pub const WAIT_EVENT_LOGICAL_APPLY_SEND_DATA: u32 = PG_WAIT_IPC + 29;

/// `WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE` — index 30 of the
/// `WaitEventIPC` section. The leader's wait in `pa_wait_for_xact_state`.
pub const WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE: u32 = PG_WAIT_IPC + 30;
