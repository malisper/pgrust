//! Wait-event classification vocabulary: the class bases from
//! `utils/wait_classes.h` plus the generated per-class event ids ports
//! consume so far (`wait_event_types.h`, generated from
//! `wait_event_names.txt` — first event of a class == the class base, then
//! +1 in file order).

/// `WAIT_EVENT_CLASS_MASK` (`wait_event.c`): the high byte selecting the
/// wait-event class.
pub const WAIT_EVENT_CLASS_MASK: u32 = 0xFF000000;
/// `WAIT_EVENT_ID_MASK` (`wait_event.c`): the low 16 bits selecting the event
/// id within a class.
pub const WAIT_EVENT_ID_MASK: u32 = 0x0000FFFF;

/// `WAIT_EVENT_CUSTOM_HASH_INIT_SIZE` (`wait_event.c`): initial size of the
/// custom-wait-event shared hash tables.
pub const WAIT_EVENT_CUSTOM_HASH_INIT_SIZE: i64 = 16;
/// `WAIT_EVENT_CUSTOM_HASH_MAX_SIZE` (`wait_event.c`): maximum number of custom
/// wait events; also the ceiling the id counter checks against.
pub const WAIT_EVENT_CUSTOM_HASH_MAX_SIZE: i64 = 128;
/// `WAIT_EVENT_CUSTOM_INITIAL_ID` (`wait_event.c`): first id assigned to a
/// custom wait event.
pub const WAIT_EVENT_CUSTOM_INITIAL_ID: u32 = 1;

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

/// `WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN` — "Waiting in main loop of slot sync
/// worker." 12th entry (0-based 11) of the Activity section of
/// `wait_event_names.txt`, so `PG_WAIT_ACTIVITY + 11`.
pub const WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN: u32 = PG_WAIT_ACTIVITY + 11;

/// `WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN` — "Waiting for slot sync worker to
/// shut down." 13th entry (0-based 12) of the Activity section, so
/// `PG_WAIT_ACTIVITY + 12`.
pub const WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN: u32 = PG_WAIT_ACTIVITY + 12;

/// `WAIT_EVENT_APPEND_READY` — "Waiting for subplan nodes of an Append plan
/// node to be ready." 1st entry (0-based 0) of the IPC section, so
/// `PG_WAIT_IPC | 0` (= 134217728, matching c2rust).
pub const WAIT_EVENT_APPEND_READY: u32 = PG_WAIT_IPC;

/// `WAIT_EVENT_BACKEND_TERMINATION` — "Waiting for the termination of another
/// backend." 4th entry (0-based 3) of the IPC section of
/// `wait_event_names.txt` (APPEND_READY, ARCHIVE_CLEANUP_COMMAND,
/// ARCHIVE_COMMAND, BACKEND_TERMINATION, ...), so `PG_WAIT_IPC + 3`.
pub const WAIT_EVENT_BACKEND_TERMINATION: u32 = PG_WAIT_IPC + 3;

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

/// `WAIT_EVENT_RELATION_MAP_READ` — 41st entry (0-based 40) of the
/// `WaitEventIO` section of `wait_event_names.txt`. (= 167772200, matching
/// c2rust.)
pub const WAIT_EVENT_RELATION_MAP_READ: u32 = PG_WAIT_IO + 40;

/// `WAIT_EVENT_RELATION_MAP_REPLACE` — 42nd entry (0-based 41) of the
/// `WaitEventIO` section. (= 167772201, matching c2rust.)
pub const WAIT_EVENT_RELATION_MAP_REPLACE: u32 = PG_WAIT_IO + 41;

/// `WAIT_EVENT_RELATION_MAP_WRITE` — 43rd entry (0-based 42) of the
/// `WaitEventIO` section. (= 167772202, matching c2rust.)
pub const WAIT_EVENT_RELATION_MAP_WRITE: u32 = PG_WAIT_IO + 42;
/// `WAIT_EVENT_SLRU_FLUSH_SYNC` — "Waiting for SLRU data to reach durable
/// storage during a checkpoint or database shutdown." IO-class event
/// (value 167772210, matching c2rust).
pub const WAIT_EVENT_SLRU_FLUSH_SYNC: u32 = PG_WAIT_IO + 50;

/// `WAIT_EVENT_SLRU_READ` — "Waiting for a read of an SLRU page."
/// (value 167772211, matching c2rust).
pub const WAIT_EVENT_SLRU_READ: u32 = PG_WAIT_IO + 51;

/// `WAIT_EVENT_SLRU_SYNC` — "Waiting for SLRU data to reach durable storage
/// following a page write." (value 167772212, matching c2rust).
pub const WAIT_EVENT_SLRU_SYNC: u32 = PG_WAIT_IO + 52;

/// `WAIT_EVENT_SLRU_WRITE` — "Waiting for a write of an SLRU page."
/// (value 167772213, matching c2rust).
pub const WAIT_EVENT_SLRU_WRITE: u32 = PG_WAIT_IO + 53;

/// `WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC` — "Waiting for a timeline history
/// file received via streaming replication to reach durable storage."
/// (value 167772217, matching c2rust).
pub const WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC: u32 = PG_WAIT_IO + 57;

/// `WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE` — "Waiting for a write of a
/// timeline history file received via streaming replication."
/// (value 167772218, matching c2rust).
pub const WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE: u32 = PG_WAIT_IO + 58;

/// `WAIT_EVENT_TIMELINE_HISTORY_READ` — "Waiting for a read of a timeline
/// history file." (value 167772219, matching c2rust).
pub const WAIT_EVENT_TIMELINE_HISTORY_READ: u32 = PG_WAIT_IO + 59;

/// `WAIT_EVENT_TIMELINE_HISTORY_SYNC` — "Waiting for a newly created timeline
/// history file to reach durable storage." (value 167772220, matching c2rust).
pub const WAIT_EVENT_TIMELINE_HISTORY_SYNC: u32 = PG_WAIT_IO + 60;

/// `WAIT_EVENT_TIMELINE_HISTORY_WRITE` — "Waiting for a write of a newly
/// created timeline history file." (value 167772221, matching c2rust).
pub const WAIT_EVENT_TIMELINE_HISTORY_WRITE: u32 = PG_WAIT_IO + 61;

// Replication-slot wait events (`wait_event_names.txt`), values matching the
// generated `wait_event_types.h` (verified against the c2rust rendering).
/// IPC: waiting for a replication slot to become inactive (drop/acquire).
pub const WAIT_EVENT_REPLICATION_SLOT_DROP: u32 = PG_WAIT_IPC + 49;
/// CLIENT: waiting for physical standbys to confirm a logical-decoding LSN.
pub const WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION: u32 = PG_WAIT_CLIENT + 6;
/// IO: writing a replication slot's state file.
pub const WAIT_EVENT_REPLICATION_SLOT_WRITE: u32 = PG_WAIT_IO + 49;
/// IO: fsyncing a replication slot's state file at save time.
pub const WAIT_EVENT_REPLICATION_SLOT_SYNC: u32 = PG_WAIT_IO + 48;
/// IO: fsyncing a replication slot's state file at restore time.
pub const WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC: u32 = PG_WAIT_IO + 47;
/// IO: reading a replication slot's state file.
pub const WAIT_EVENT_REPLICATION_SLOT_READ: u32 = PG_WAIT_IO + 46;

/// `WAIT_EVENT_WAL_RECEIVER_MAIN` — index 14 of the Activity section of
/// `wait_event_names.txt` (after SYSLOGGER_MAIN at 13).
pub const WAIT_EVENT_WAL_RECEIVER_MAIN: u32 = PG_WAIT_ACTIVITY + 14;

/// `WAIT_EVENT_WAL_RECEIVER_EXIT` — "Waiting for the WAL receiver to exit."
/// Index 53 of the IPC section of `wait_event_names.txt`: it sorts immediately
/// before `WAL_RECEIVER_WAIT_START` (54).
pub const WAIT_EVENT_WAL_RECEIVER_EXIT: u32 = PG_WAIT_IPC + 53;

/// `WAIT_EVENT_WAL_RECEIVER_WAIT_START` — index 54 of the IPC section of
/// `wait_event_names.txt`.
pub const WAIT_EVENT_WAL_RECEIVER_WAIT_START: u32 = PG_WAIT_IPC + 54;

/// `WAIT_EVENT_WAL_WRITE` — index 80 of the IO section of
/// `wait_event_names.txt`.
pub const WAIT_EVENT_WAL_WRITE: u32 = PG_WAIT_IO + 80;

/// `WAIT_EVENT_LOGICAL_LAUNCHER_MAIN` — 9th entry (index 8) of the Activity
/// section of `wait_event_names.txt` (ARCHIVER_MAIN, AUTOVACUUM_MAIN,
/// BGWRITER_HIBERNATE, BGWRITER_MAIN, CHECKPOINTER_MAIN, CHECKPOINTER_SHUTDOWN,
/// IO_WORKER_MAIN, LOGICAL_APPLY_MAIN, LOGICAL_LAUNCHER_MAIN, ...).
pub const WAIT_EVENT_LOGICAL_LAUNCHER_MAIN: u32 = PG_WAIT_ACTIVITY + 8;

/// `WAIT_EVENT_BGWORKER_SHUTDOWN` — 6th entry (index 5) of the IPC section of
/// `wait_event_names.txt` (APPEND_READY, ARCHIVE_CLEANUP_COMMAND,
/// ARCHIVE_COMMAND, BACKEND_TERMINATION, BACKUP_WAIT_WAL_ARCHIVE,
/// BGWORKER_SHUTDOWN, BGWORKER_STARTUP, ...).
pub const WAIT_EVENT_BGWORKER_SHUTDOWN: u32 = PG_WAIT_IPC + 5;

/// `WAIT_EVENT_BGWORKER_STARTUP` — 7th entry (index 6) of the IPC section.
pub const WAIT_EVENT_BGWORKER_STARTUP: u32 = PG_WAIT_IPC + 6;

/// `WAIT_EVENT_WAL_SUMMARIZER_WAL` — "Waiting in WAL summarizer for more WAL
/// to be generated." 17th entry (0-based 16) of the Activity section, so
/// `PG_WAIT_ACTIVITY | 16`.
pub const WAIT_EVENT_WAL_SUMMARIZER_WAL: u32 = PG_WAIT_ACTIVITY | 16;

/// `WAIT_EVENT_WAL_SUMMARIZER_ERROR` — "Waiting after a WAL summarizer error."
/// 10th entry (0-based 9) of the `WaitEventTimeout` section, so
/// `PG_WAIT_TIMEOUT | 9`.
pub const WAIT_EVENT_WAL_SUMMARIZER_ERROR: u32 = PG_WAIT_TIMEOUT | 9;
