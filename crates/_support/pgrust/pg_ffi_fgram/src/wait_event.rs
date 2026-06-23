use crate::{pgsocket, uint32};
use core::ffi::{c_int, c_void};

pub const PG_WAIT_LWLOCK: uint32 = 0x01000000;
pub const PG_WAIT_LOCK: uint32 = 0x03000000;
pub const PG_WAIT_BUFFERPIN: uint32 = 0x04000000;
pub const PG_WAIT_ACTIVITY: uint32 = 0x05000000;
pub const PG_WAIT_CLIENT: uint32 = 0x06000000;
pub const PG_WAIT_EXTENSION: uint32 = 0x07000000;
pub const PG_WAIT_IPC: uint32 = 0x08000000;
pub const PG_WAIT_TIMEOUT: uint32 = 0x09000000;
pub const PG_WAIT_IO: uint32 = 0x0A000000;
pub const PG_WAIT_INJECTIONPOINT: uint32 = 0x0B000000;

pub const WAIT_EVENT_CLASS_MASK: uint32 = 0xFF000000;
pub const WAIT_EVENT_ID_MASK: uint32 = 0x0000FFFF;

pub const WAIT_EVENT_CUSTOM_INITIAL_ID: uint32 = 1;
pub const WAIT_EVENT_CUSTOM_HASH_INIT_SIZE: usize = 16;
pub const WAIT_EVENT_CUSTOM_HASH_MAX_SIZE: usize = 128;

pub const WAIT_EVENT_BUFFER_PIN: uint32 = PG_WAIT_BUFFERPIN;
/// `WAIT_EVENT_BUFFER_IO` — "Waiting for buffer I/O to complete." It is the 9th
/// entry (alphabetical) of the `WaitEventIPC` section of
/// `wait_event_names.txt`, so the generated enum value is `PG_WAIT_IPC | 9`.
pub const WAIT_EVENT_BUFFER_IO: uint32 = PG_WAIT_IPC | 9;
pub const WAIT_EVENT_MESSAGE_QUEUE_INTERNAL: uint32 = PG_WAIT_IPC | 1;
pub const WAIT_EVENT_MESSAGE_QUEUE_RECEIVE: uint32 = PG_WAIT_IPC | 3;
pub const WAIT_EVENT_MESSAGE_QUEUE_SEND: uint32 = PG_WAIT_IPC | 4;
/// `WAIT_EVENT_PARALLEL_BITMAP_SCAN` — "Waiting for parallel bitmap scan to
/// become initialized." Generated id `0x26` in the `IPC` class.
pub const WAIT_EVENT_PARALLEL_BITMAP_SCAN: uint32 = PG_WAIT_IPC | 0x26;
/// `WAIT_EVENT_REPLICATION_SLOT_DROP` — "Waiting for a replication slot to
/// become inactive so it can be dropped." Generated enum value `0x08000031`
/// (`PG_WAIT_IPC | 0x31`) in the `WaitEventIPC` section of `wait_event_names.txt`.
pub const WAIT_EVENT_REPLICATION_SLOT_DROP: uint32 = PG_WAIT_IPC | 0x31;
/// `WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION` — "Waiting for the WAL to be
/// received and flushed by the physical standby." Generated enum value
/// `0x06000006` (`PG_WAIT_CLIENT | 6`) in the `WaitEventClient` section of
/// `wait_event_names.txt`.
pub const WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION: uint32 = PG_WAIT_CLIENT | 6;
/// Replication-slot state-file I/O wait events (`WaitEventIO` section of
/// `wait_event_names.txt`). Generated ids: READ=0x2E, RESTORE_SYNC=0x2F,
/// SYNC=0x30, WRITE=0x31 (matching the c2rust reference values 167772206..209).
pub const WAIT_EVENT_REPLICATION_SLOT_READ: uint32 = PG_WAIT_IO | 0x2E;
pub const WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC: uint32 = PG_WAIT_IO | 0x2F;
pub const WAIT_EVENT_REPLICATION_SLOT_SYNC: uint32 = PG_WAIT_IO | 0x30;
pub const WAIT_EVENT_REPLICATION_SLOT_WRITE: uint32 = PG_WAIT_IO | 0x31;
pub const WAIT_EVENT_SPIN_DELAY: uint32 = PG_WAIT_TIMEOUT | 6;
pub const WAIT_EVENT_WAL_SUMMARY_READ: uint32 = PG_WAIT_IO | 236;
pub const WAIT_EVENT_WAL_SUMMARY_WRITE: uint32 = PG_WAIT_IO | 237;
/// `WAIT_EVENT_WAL_SUMMARIZER_WAL` — "Waiting in WAL summarizer for more WAL to
/// be generated." It is the 17th entry (0-based 16, alphabetical) of the
/// `WaitEventActivity` section of `wait_event_names.txt`, so the generated enum
/// value is `PG_WAIT_ACTIVITY | 16`.
pub const WAIT_EVENT_WAL_SUMMARIZER_WAL: uint32 = PG_WAIT_ACTIVITY | 16;
/// `WAIT_EVENT_WAL_SUMMARIZER_ERROR` — "Waiting after a WAL summarizer error."
/// It is the 10th entry (0-based 9, alphabetical) of the `WaitEventTimeout`
/// section, so the generated enum value is `PG_WAIT_TIMEOUT | 9`.
pub const WAIT_EVENT_WAL_SUMMARIZER_ERROR: uint32 = PG_WAIT_TIMEOUT | 9;
/// `WAIT_EVENT_WAL_SUMMARY_READY` — "Waiting for a new WAL summary to be
/// generated." It is the 56th entry (0-based 55, alphabetical) of the
/// `WaitEventIPC` section, so the generated enum value is `PG_WAIT_IPC | 55`.
pub const WAIT_EVENT_WAL_SUMMARY_READY: uint32 = PG_WAIT_IPC | 55;

// --- logical replication launcher wait events (launcher.c) -----------------
// Values transcribed from the generated 18.3 `wait_event_types.h`, whose enum
// entries are the case-insensitively sorted members of each class section in
// `wait_event_names.txt`, the first member taking the class base value.
//
// `WaitEventActivity`: ARCHIVER_MAIN(0), AUTOVACUUM_MAIN(1), BGWRITER_HIBERNATE
// (2), BGWRITER_MAIN(3), CHECKPOINTER_MAIN(4), CHECKPOINTER_SHUTDOWN(5),
// IO_WORKER_MAIN(6), LOGICAL_APPLY_MAIN(7), LOGICAL_LAUNCHER_MAIN(8), ...
pub const WAIT_EVENT_LOGICAL_LAUNCHER_MAIN: uint32 = PG_WAIT_ACTIVITY | 8;
// `WaitEventIPC`: APPEND_READY(0), ARCHIVE_CLEANUP_COMMAND(1), ARCHIVE_COMMAND
// (2), BACKEND_TERMINATION(3), BACKUP_WAIT_WAL_ARCHIVE(4), BGWORKER_SHUTDOWN(5),
// BGWORKER_STARTUP(6), ...
pub const WAIT_EVENT_BGWORKER_SHUTDOWN: uint32 = PG_WAIT_IPC | 5;
pub const WAIT_EVENT_BGWORKER_STARTUP: uint32 = PG_WAIT_IPC | 6;

// --- WaitEventActivity (aux-daemon main loops) ------------------------------
// The `WaitEventActivity` enum from the generated 18.3 `wait_event_types.h`,
// whose members are the case-insensitively-sorted entries of the `Activity`
// section of `wait_event_names.txt`, the first taking the class base value:
//   ARCHIVER_MAIN(0), AUTOVACUUM_MAIN(1), BGWRITER_HIBERNATE(2), BGWRITER_MAIN
//   (3), CHECKPOINTER_MAIN(4), CHECKPOINTER_SHUTDOWN(5), IO_WORKER_MAIN(6),
//   LOGICAL_APPLY_MAIN(7), LOGICAL_LAUNCHER_MAIN(8), LOGICAL_PARALLEL_APPLY_MAIN
//   (9), RECOVERY_WAL_STREAM(10), REPLICATION_SLOTSYNC_MAIN(11),
//   REPLICATION_SLOTSYNC_SHUTDOWN(12), SYSLOGGER_MAIN(13), WAL_RECEIVER_MAIN(14),
//   WAL_SENDER_MAIN(15), WAL_SUMMARIZER_WAL(16), WAL_WRITER_MAIN(17).
// (LOGICAL_LAUNCHER_MAIN(8) and WAL_SUMMARIZER_WAL(16) are defined above.)
pub const WAIT_EVENT_ARCHIVER_MAIN: uint32 = PG_WAIT_ACTIVITY | 0;
pub const WAIT_EVENT_AUTOVACUUM_MAIN: uint32 = PG_WAIT_ACTIVITY | 1;
pub const WAIT_EVENT_BGWRITER_HIBERNATE: uint32 = PG_WAIT_ACTIVITY | 2;
pub const WAIT_EVENT_BGWRITER_MAIN: uint32 = PG_WAIT_ACTIVITY | 3;
pub const WAIT_EVENT_CHECKPOINTER_MAIN: uint32 = PG_WAIT_ACTIVITY | 4;
pub const WAIT_EVENT_CHECKPOINTER_SHUTDOWN: uint32 = PG_WAIT_ACTIVITY | 5;
pub const WAIT_EVENT_IO_WORKER_MAIN: uint32 = PG_WAIT_ACTIVITY | 6;
pub const WAIT_EVENT_LOGICAL_APPLY_MAIN: uint32 = PG_WAIT_ACTIVITY | 7;
pub const WAIT_EVENT_LOGICAL_PARALLEL_APPLY_MAIN: uint32 = PG_WAIT_ACTIVITY | 9;
pub const WAIT_EVENT_RECOVERY_WAL_STREAM: uint32 = PG_WAIT_ACTIVITY | 10;
pub const WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN: uint32 = PG_WAIT_ACTIVITY | 11;
pub const WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN: uint32 = PG_WAIT_ACTIVITY | 12;
pub const WAIT_EVENT_SYSLOGGER_MAIN: uint32 = PG_WAIT_ACTIVITY | 13;
pub const WAIT_EVENT_WAL_RECEIVER_MAIN: uint32 = PG_WAIT_ACTIVITY | 14;
pub const WAIT_EVENT_WAL_SENDER_MAIN: uint32 = PG_WAIT_ACTIVITY | 15;
pub const WAIT_EVENT_WAL_WRITER_MAIN: uint32 = PG_WAIT_ACTIVITY | 17;

// Checkpoint wait events from the `WaitEventIPC` and `WaitEventTimeout` classes
// (positions per the same alphabetical scheme in `wait_event_types.h`).
pub const WAIT_EVENT_CHECKPOINT_DONE: uint32 = PG_WAIT_IPC | 11;
pub const WAIT_EVENT_CHECKPOINT_START: uint32 = PG_WAIT_IPC | 12;
// `WaitEventTimeout`: BASE_BACKUP_THROTTLE(0), CHECKPOINT_WRITE_DELAY(1), ...
pub const WAIT_EVENT_BASE_BACKUP_THROTTLE: uint32 = PG_WAIT_TIMEOUT | 0;
pub const WAIT_EVENT_CHECKPOINT_WRITE_DELAY: uint32 = PG_WAIT_TIMEOUT | 1;

pub const PGINVALID_SOCKET: pgsocket = -1;

pub const WL_LATCH_SET: c_int = 1 << 0;
pub const WL_SOCKET_READABLE: c_int = 1 << 1;
pub const WL_SOCKET_WRITEABLE: c_int = 1 << 2;
pub const WL_TIMEOUT: c_int = 1 << 3;
pub const WL_POSTMASTER_DEATH: c_int = 1 << 4;
pub const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;
pub const WL_SOCKET_CONNECTED: c_int = WL_SOCKET_WRITEABLE;
pub const WL_SOCKET_CLOSED: c_int = 1 << 7;
pub const WL_SOCKET_ACCEPT: c_int = WL_SOCKET_READABLE;
pub const WL_SOCKET_MASK: c_int = WL_SOCKET_READABLE
    | WL_SOCKET_WRITEABLE
    | WL_SOCKET_CONNECTED
    | WL_SOCKET_ACCEPT
    | WL_SOCKET_CLOSED;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WaitEvent {
    pub pos: c_int,
    pub events: uint32,
    pub fd: pgsocket,
    pub user_data: *mut c_void,
}
