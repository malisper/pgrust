//! `replication/walreceiver.c` — the standby-side WAL receiver daemon.
//!
//! The WAL receiver is the standby-side daemon that connects to the primary's
//! walsender, receives XLOG records over a libpq replication connection, writes
//! them to `pg_wal`, and reports its write/flush/apply positions back to the
//! primary (plus optional hot-standby-feedback).
//!
//! C's `ereport(ERROR)`/`FATAL`/`PANIC` (which `longjmp` out of the daemon in
//! C) are modeled as `Err(PgError)` propagated with `?`. The C process-global
//! `static`s (`recvFile`, `recvFileTLI`, `recvSegNo`, `LogstreamResult`,
//! `wakeup[]`, the two function-local `static`s) plus the file-scope GUCs
//! defined in this translation unit (`wal_receiver_status_interval`,
//! `wal_receiver_timeout`, `hot_standby_feedback`) live in a per-thread
//! [`FileState`] — faithful, since the WAL receiver runs as one single-threaded
//! daemon per receiver.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use std::cell::RefCell;

use backend_utils_error::{elog, ereport, message_level_is_interesting};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, DEBUG2, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROTOCOL_VIOLATION, ERROR, FATAL, LOG, PANIC,
};

use types_core::{pgsocket, TimeLineID, TimestampTz, TransactionId, XLogRecPtr, XLogSegNo};
use types_pgstat::wait_event::{
    WAIT_EVENT_WAL_RECEIVER_MAIN, WAIT_EVENT_WAL_RECEIVER_WAIT_START, WAIT_EVENT_WAL_WRITE,
};
use types_startup::StartupData;
use types_wal::ArchiveMode;
use types_walreceiver::{
    WalRcvState, WalRcvStreamOptions, WalRcvWakeupReason, WalReceiverActivity, WalReceiverConn,
    NAMEDATALEN, NUM_WALRCV_WAKEUPS, TIMESTAMP_INFINITY,
};
use WalRcvWakeupReason::*;

use backend_access_transam_timeline_seams as timeline;
use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xlogarchive_seams as xlogarchive;
use backend_access_transam_xlogrecovery_seams as xlogrecovery;
use backend_access_transam_varsup_seams as varsup;
use backend_replication_libpqwalreceiver_seams as libpqwalrcv;
use backend_replication_walreceiverfuncs_seams as walrcvfuncs;
use backend_replication_walsender_seams as walsender;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_ipc_procarray_seams as procarray;
use backend_storage_ipc_seams as ipc;
use backend_tcop_postgres_seams as tcop;
use backend_utils_activity_pgstat_io_seams as pgstat_io;
use backend_utils_activity_pgstat_wal_seams as pgstat_wal;
use backend_utils_activity_waitevent_seams as waitevent;
use backend_utils_adt_acl_seams as acl;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_misc_guc_file_seams as guc_file;
use backend_utils_misc_guc_tables_seams as guc_tables;
use backend_utils_misc_ps_status_seams as ps_status;

// `access/xlogdefs.h`: `InvalidXLogRecPtr`.
const InvalidXLogRecPtr: XLogRecPtr = 0;
// `access/transam.h`: `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;

// `PGINVALID_SOCKET`.
const PGINVALID_SOCKET: pgsocket = -1;

// Latch wait flags (`storage/waiteventset.h`).
const WL_LATCH_SET: i32 = 1 << 0;
const WL_SOCKET_READABLE: i32 = 1 << 1;
const WL_TIMEOUT: i32 = 1 << 3;
const WL_EXIT_ON_PM_DEATH: i32 = 1 << 5;

// `catalog/pg_authid.d.h`: `ROLE_PG_READ_ALL_STATS`.
const ROLE_PG_READ_ALL_STATS: types_core::Oid = 3375;

/// `ArchiveMode == ARCHIVE_MODE_ALWAYS`.
fn xlog_archive_mode_is_always() -> bool {
    xlog::xlog_archive_mode::call() == ArchiveMode::ArchiveModeAlways
}

macro_rules! here {
    () => {
        ErrorLocation {
            filename: Some(file!().to_string()),
            lineno: line!() as i32,
            funcname: None,
        }
    };
}

// ---------------------------------------------------------------------------
// File-scope (static) state. In C these are process-global statics and
// file-scope GUCs; the walreceiver runs as a single-threaded daemon, so
// per-thread cells faithfully model "one set of state per receiver process".
// ---------------------------------------------------------------------------

/// `LogstreamResult` indicates the byte positions already written/fsynced.
#[derive(Clone, Copy, Default)]
struct LogstreamResultT {
    /// last byte + 1 written out in the standby.
    Write: XLogRecPtr,
    /// last byte + 1 flushed in the standby.
    Flush: XLogRecPtr,
}

struct FileState {
    /// libpqwalreceiver connection (`static WalReceiverConn *wrconn = NULL;`).
    wrconn: Option<WalReceiverConn>,
    /// `static int recvFile = -1;`
    recvFile: i32,
    /// `static TimeLineID recvFileTLI = 0;`
    recvFileTLI: TimeLineID,
    /// `static XLogSegNo recvSegNo = 0;`
    recvSegNo: XLogSegNo,
    /// `static struct { ... } LogstreamResult;`
    LogstreamResult: LogstreamResultT,
    /// `static TimestampTz wakeup[NUM_WALRCV_WAKEUPS];`
    wakeup: [TimestampTz; NUM_WALRCV_WAKEUPS],
    /// The current `startpointTLI` (C passes `&startpointTLI` to
    /// `on_shmem_exit(WalRcvDie, ...)`; we keep the latest value here so
    /// `WalRcvDie` reads the live timeline like the C pointer does).
    startpointTLI: TimeLineID,
    // -- function-local statics in XLogWalRcvSendReply --
    /// `static XLogRecPtr writePtr = 0;` (XLogWalRcvSendReply)
    reply_writePtr: XLogRecPtr,
    /// `static XLogRecPtr flushPtr = 0;` (XLogWalRcvSendReply)
    reply_flushPtr: XLogRecPtr,
    // -- function-local static in XLogWalRcvSendHSFeedback --
    /// `static bool primary_has_standby_xmin = true;`
    primary_has_standby_xmin: bool,
    // -- file-scope GUCs defined in walreceiver.c --
    /// `int wal_receiver_status_interval` (seconds).
    wal_receiver_status_interval: i32,
    /// `int wal_receiver_timeout` (milliseconds).
    wal_receiver_timeout: i32,
    /// `bool hot_standby_feedback`.
    hot_standby_feedback: bool,
}

impl Default for FileState {
    fn default() -> Self {
        FileState {
            wrconn: None,
            recvFile: -1,
            recvFileTLI: 0,
            recvSegNo: 0,
            LogstreamResult: LogstreamResultT::default(),
            wakeup: [0; NUM_WALRCV_WAKEUPS],
            startpointTLI: 0,
            reply_writePtr: 0,
            reply_flushPtr: 0,
            primary_has_standby_xmin: true,
            wal_receiver_status_interval: 10,
            wal_receiver_timeout: 60_000,
            hot_standby_feedback: false,
        }
    }
}

thread_local! {
    static STATE: RefCell<FileState> = RefCell::new(FileState::default());
}

fn with_state<R>(f: impl FnOnce(&mut FileState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

/// Reset the per-thread file state to its initial values. Exposed for tests
/// that re-run the daemon helpers in one thread.
#[doc(hidden)]
pub fn reset_state_for_tests() {
    STATE.with(|s| *s.borrow_mut() = FileState::default());
}

fn wal_receiver_status_interval() -> i32 {
    with_state(|s| s.wal_receiver_status_interval)
}
fn wal_receiver_timeout() -> i32 {
    with_state(|s| s.wal_receiver_timeout)
}
fn hot_standby_feedback() -> bool {
    with_state(|s| s.hot_standby_feedback)
}

// ---------------------------------------------------------------------------
// Small helpers ported from xlog_internal.h macros / inline functions.
// ---------------------------------------------------------------------------

/// `XLogRecPtrIsInvalid(r)` -> `(r) == InvalidXLogRecPtr`.
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// `XLogSegmentsPerXLogId(wal_segsz_bytes)` -> `0x100000000 / wal_segsz_bytes`.
#[inline]
fn XLogSegmentsPerXLogId(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000u64 / (wal_segsz_bytes as u64)
}

/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)` -> `xlogptr & (wal_segsz - 1)`.
#[inline]
fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: i32) -> i32 {
    (xlogptr & ((wal_segsz_bytes as u64) - 1)) as i32
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)` -> `logSegNo = xlrp / segsz`.
#[inline]
fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / (wal_segsz_bytes as u64)
}

/// `XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes)` -> `xlrp / segsz == logSegNo`.
#[inline]
fn XLByteInSeg(xlrp: XLogRecPtr, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> bool {
    (xlrp / (wal_segsz_bytes as u64)) == log_seg_no
}

/// `XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)` -> WAL segment file
/// name string.
fn XLogFileName(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let per = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        (log_seg_no / per) as u32,
        (log_seg_no % per) as u32
    )
}

/// `TLHistoryFileName(fname, tli)` -> timeline history file name string.
fn TLHistoryFileName(tli: TimeLineID) -> String {
    format!("{:08X}.history", tli)
}

/// `TimestampTzPlusMilliseconds(tz, ms)` (`utils/timestamp.h`).
#[inline]
fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: i64) -> TimestampTz {
    tz + (ms * 1000)
}

/// `TimestampTzPlusSeconds(tz, s)` (`utils/timestamp.h`).
#[inline]
fn TimestampTzPlusSeconds(tz: TimestampTz, s: i64) -> TimestampTz {
    tz + (s * 1_000_000)
}

/// Emit a non-ERROR-level `ereport(...)` (LOG/DEBUG): logs and returns.
fn emit(builder: backend_utils_error::ErrorBuilder) {
    let _ = builder.finish(here!());
}

/// `LSN_FORMAT_ARGS(lsn)` rendered as the canonical `%X/%X` string.
fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `TransactionIdIsValid(xid)` -> `xid != InvalidTransactionId`.
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `XidFromFullTransactionId(fxid)` -> low 32 bits.
#[inline]
fn XidFromFullTransactionId(fxid: u64) -> TransactionId {
    fxid as u32
}

/// `EpochFromFullTransactionId(fxid)` -> high 32 bits.
#[inline]
fn EpochFromFullTransactionId(fxid: u64) -> u32 {
    (fxid >> 32) as u32
}

// ---------------------------------------------------------------------------
// Main entry point for walreceiver process.
// ---------------------------------------------------------------------------

/// `void WalReceiverMain(const void *startup_data, size_t startup_data_len)`.
///
/// Never returns: it loops forever (the `for(;;)` at the top level), and the
/// only exits are through `proc_exit`/`exit` (which never return) or the
/// `ereport(ERROR|FATAL|PANIC)` paths which `longjmp` out of the daemon. We
/// surface those as a hard panic at the entry boundary (no caller to unwind
/// to), matching the `-> !` child-launch contract.
pub fn wal_receiver_main(_startup_data: &StartupData) -> ! {
    match wal_receiver_main_inner() {
        Ok(()) => unreachable!("WalReceiverMain loops forever or proc_exits"),
        Err(e) => {
            // The C daemon's top-level error handling longjmps to the process
            // top level and exits; there is no Rust caller to propagate to.
            panic!("walreceiver exiting on error: {e:?}");
        }
    }
}

fn wal_receiver_main_inner() -> PgResult<()> {
    // MyBackendType = B_WAL_RECEIVER; AuxiliaryProcessMainCommon();
    backend_postmaster_auxprocess_seams::auxiliary_process_main_common::call()?;

    /*
     * Mark walreceiver as running in shared memory, fail out if asked to stop,
     * advertise pid/procno, init message times, and read streaming params.
     * (lines 188-241 spinlock block.)
     */
    let now = timestamp::get_current_timestamp::call();
    let startup = match walrcvfuncs::walrcv_start_in_shmem::call(now)? {
        Some(info) => info,
        None => ipc::proc_exit::call(1, my_proc_pid()),
    };

    let conninfo = cstr_from_bytes(&startup.conninfo);
    let mut slotname = startup.slotname;
    let is_temp_slot = startup.is_temp_slot;
    let mut startpoint: XLogRecPtr = startup.receive_start;
    let mut startpointTLI: TimeLineID = startup.receive_start_tli;
    with_state(|s| s.startpointTLI = startpointTLI);

    /*
     * At most one of is_temp_slot and slotname can be set; otherwise,
     * RequestXLogStreaming messed up.
     */
    assert!(!is_temp_slot || (slotname[0] == 0));

    walrcvfuncs::set_written_upto::call(0); /* pg_atomic_write_u64(&WalRcv->writtenUpto, 0); */

    /* Arrange to clean up at walreceiver exit */
    ipc::on_shmem_exit::call(wal_rcv_die_callback, types_datum::Datum::null())?;

    /* Properly accept or ignore signals the postmaster might send us */
    setup_signal_handlers();

    /* Load the libpq-specific functions; verify it initialized */
    libpqwalrcv::load_libpqwalreceiver::call()?;

    /* Establish the connection to the primary for XLOG streaming */
    let cluster = guc_tables::cluster_name::call();
    let appname = if !cluster.is_empty() {
        cluster
    } else {
        "walreceiver".to_string()
    };
    let conn = match libpqwalrcv::walrcv_connect::call(conninfo, appname.clone()) {
        Ok(c) => c,
        Err(err) => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "streaming replication receiver \"{appname}\" could not connect to the primary server: {err}"
                ))
                .into_error());
        }
    };
    with_state(|s| s.wrconn = Some(conn));

    /*
     * Save user-visible connection string (clobbers original conninfo) plus
     * host/port of the sender server.
     */
    let tmp_conninfo = libpqwalrcv::walrcv_get_conninfo::call(conn);
    let (sender_host, sender_port) = libpqwalrcv::walrcv_get_senderinfo::call(conn);
    walrcvfuncs::walrcv_save_conninfo::call(tmp_conninfo, sender_host, sender_port);

    let mut first_stream = true;
    loop {
        /*
         * Check that we're connected to a valid server using the
         * IDENTIFY_SYSTEM replication command.
         */
        let (primary_sysid, primaryTLI) = libpqwalrcv::walrcv_identify_system::call(conn)?;

        let standby_sysid = format!("{}", xlog::get_system_identifier::call());
        if primary_sysid != standby_sysid {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("database system identifier differs between the primary and standby")
                .errdetail(format!(
                    "The primary's identifier is {primary_sysid}, the standby's identifier is {standby_sysid}."
                ))
                .into_error());
        }

        /*
         * Confirm that the current timeline of the primary is the same or ahead
         * of ours.
         */
        if primaryTLI < startpointTLI {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "highest timeline {primaryTLI} of the primary is behind recovery timeline {startpointTLI}"
                ))
                .into_error());
        }

        /*
         * Get any missing history files. We do this always, even when we're not
         * interested in that timeline.
         */
        WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI)?;

        /*
         * Create temporary replication slot if requested, and update slot name
         * in shared memory.
         */
        if is_temp_slot {
            let new_slot = format!(
                "pg_walreceiver_{}",
                libpqwalrcv::walrcv_get_backend_pid::call(conn)
            );
            slotname = name_from_str(&new_slot);
            libpqwalrcv::walrcv_create_slot::call(conn, new_slot.clone())?;
            walrcvfuncs::walrcv_set_slotname::call(new_slot);
        }

        /*
         * Start streaming.
         */
        let options = WalRcvStreamOptions {
            logical: false,
            startpoint,
            slotname: if slotname[0] != 0 {
                Some(cstr_from_bytes(&slotname))
            } else {
                None
            },
            physical_startpointTLI: startpointTLI,
        };

        if libpqwalrcv::walrcv_startstreaming::call(conn, options)? {
            if first_stream {
                emit(ereport(LOG).errmsg(format!(
                    "started streaming WAL from primary at {} on timeline {startpointTLI}",
                    lsn_fmt(startpoint)
                )));
            } else {
                emit(ereport(LOG).errmsg(format!(
                    "restarted WAL streaming at {} on timeline {startpointTLI}",
                    lsn_fmt(startpoint)
                )));
            }
            first_stream = false;

            /* Initialize LogstreamResult and buffers for processing messages */
            let replay = xlogrecovery::get_xlog_replay_recptr::call();
            with_state(|s| {
                s.LogstreamResult.Write = replay;
                s.LogstreamResult.Flush = replay;
            });

            /* Initialize nap wakeup times. */
            let now = timestamp::get_current_timestamp::call();
            for i in 0..NUM_WALRCV_WAKEUPS {
                WalRcvComputeNextWakeup(wakeup_reason_from_index(i), now);
            }

            /* Send initial reply/feedback messages. */
            XLogWalRcvSendReply(true, false)?;
            XLogWalRcvSendHSFeedback(true)?;

            /* Loop until end-of-streaming or error */
            let mut endofwal = false;
            loop {
                let mut wait_fd: pgsocket = PGINVALID_SOCKET;

                /*
                 * Exit walreceiver if we're not in recovery. This should not
                 * happen, but cross-check the status here.
                 */
                if !xlog::recovery_in_progress::call() {
                    return Err(ereport(FATAL)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg("cannot continue WAL streaming, recovery has already ended")
                        .into_error());
                }

                /* Process any requests or signals received recently */
                tcop::check_for_interrupts::call()?;

                if backend_postmaster_interrupt::ConfigReloadPending() {
                    backend_postmaster_interrupt::SetConfigReloadPending(false);
                    guc_file::process_config_file::call(types_guc::PGC_SIGHUP)?;
                    /* recompute wakeup times */
                    let now = timestamp::get_current_timestamp::call();
                    for i in 0..NUM_WALRCV_WAKEUPS {
                        WalRcvComputeNextWakeup(wakeup_reason_from_index(i), now);
                    }
                    XLogWalRcvSendHSFeedback(true)?;
                }

                /* See if we can read data immediately */
                let (mut len, mut buf, fd) = libpqwalrcv::walrcv_receive::call(conn)?;
                wait_fd = if fd != PGINVALID_SOCKET { fd } else { wait_fd };
                if len != 0 {
                    /*
                     * Process the received data, and any subsequent data we can
                     * read without blocking.
                     */
                    loop {
                        if len > 0 {
                            /*
                             * Something was received from primary, so adjust the
                             * ping and terminate wakeup times.
                             */
                            let now = timestamp::get_current_timestamp::call();
                            WalRcvComputeNextWakeup(WALRCV_WAKEUP_TERMINATE, now);
                            WalRcvComputeNextWakeup(WALRCV_WAKEUP_PING, now);
                            XLogWalRcvProcessMsg(buf[0], &buf[1..len as usize], startpointTLI)?;
                        } else if len == 0 {
                            break;
                        } else {
                            /* len < 0 */
                            let write = with_state(|s| s.LogstreamResult.Write);
                            emit(
                                ereport(LOG)
                                    .errmsg("replication terminated by primary server")
                                    .errdetail(format!(
                                        "End of WAL reached on timeline {startpointTLI} at {}.",
                                        lsn_fmt(write)
                                    )),
                            );
                            endofwal = true;
                            break;
                        }
                        let (l, b, f) = libpqwalrcv::walrcv_receive::call(conn)?;
                        len = l;
                        buf = b;
                        wait_fd = if f != PGINVALID_SOCKET { f } else { wait_fd };
                    }

                    /* Let the primary know that we received some data. */
                    XLogWalRcvSendReply(false, false)?;

                    /*
                     * If we've written some records, flush them to disk and let
                     * the startup process and primary server know about them.
                     */
                    XLogWalRcvFlush(false, startpointTLI)?;
                }

                /* Check if we need to exit the streaming loop. */
                if endofwal {
                    break;
                }

                /* Find the soonest wakeup time, to limit our nap. */
                let mut nextWakeup = TIMESTAMP_INFINITY;
                for i in 0..NUM_WALRCV_WAKEUPS {
                    let w = with_state(|s| s.wakeup[i]);
                    nextWakeup = core::cmp::min(w, nextWakeup);
                }

                /* Calculate the nap time, clamping as necessary. */
                let now = timestamp::get_current_timestamp::call();
                let nap = timestamp::timestamp_difference_milliseconds::call(now, nextWakeup);

                /*
                 * We can't reuse a WaitEventSet, since libpq's socket may have
                 * been closed/reopened.
                 */
                assert!(wait_fd != PGINVALID_SOCKET);
                let rc = latch::wait_latch_or_socket::call(
                    WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT | WL_LATCH_SET,
                    wait_fd,
                    nap,
                    WAIT_EVENT_WAL_RECEIVER_MAIN,
                );
                if rc & WL_LATCH_SET != 0 {
                    latch::reset_latch_my_latch::call();
                    tcop::check_for_interrupts::call()?;

                    if walrcvfuncs::take_force_reply::call() {
                        /*
                         * The recovery process has asked us to send apply
                         * feedback now. (take_force_reply clears the flag with
                         * the required memory barrier before we send.)
                         */
                        XLogWalRcvSendReply(true, false)?;
                    }
                }
                if rc & WL_TIMEOUT != 0 {
                    /*
                     * We didn't receive anything new. Ping / status-update
                     * housekeeping.
                     */
                    let mut requestReply = false;

                    /*
                     * Report pending statistics to the cumulative stats system.
                     */
                    pgstat_wal::pgstat_report_wal::call(false);

                    /*
                     * Check if time since last receive from primary has reached
                     * the configured limit.
                     */
                    let now = timestamp::get_current_timestamp::call();
                    if now >= with_state(|s| s.wakeup[WALRCV_WAKEUP_TERMINATE as usize]) {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_CONNECTION_FAILURE)
                            .errmsg("terminating walreceiver due to timeout")
                            .into_error());
                    }

                    /*
                     * If we didn't receive anything new for half of receiver
                     * replication timeout, then ping the server.
                     */
                    if now >= with_state(|s| s.wakeup[WALRCV_WAKEUP_PING as usize]) {
                        requestReply = true;
                        with_state(|s| s.wakeup[WALRCV_WAKEUP_PING as usize] = TIMESTAMP_INFINITY);
                    }

                    XLogWalRcvSendReply(requestReply, requestReply)?;
                    XLogWalRcvSendHSFeedback(false)?;
                }
            }

            /*
             * The backend finished streaming. Exit streaming COPY-mode from our
             * side, too.
             */
            let primaryTLI = libpqwalrcv::walrcv_endstreaming::call(conn)?;

            /*
             * If the server had switched to a new timeline that we didn't know
             * about when we began streaming, fetch its timeline history file
             * now.
             */
            WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI)?;
        } else {
            emit(ereport(LOG).errmsg(format!(
                "primary server contains no more WAL on requested timeline {startpointTLI}"
            )));
        }

        /*
         * End of WAL reached on the requested timeline. Close the last segment,
         * and await for new orders from the startup process.
         */
        let recv_file = with_state(|s| s.recvFile);
        if recv_file >= 0 {
            XLogWalRcvFlush(false, startpointTLI)?;
            let (recv_file_tli, recv_seg_no) = with_state(|s| (s.recvFileTLI, s.recvSegNo));
            let xlogfname = XLogFileName(recv_file_tli, recv_seg_no, xlog::wal_segment_size::call());
            if pg_close(recv_file) != 0 {
                return Err(ereport(PANIC)
                    .errcode_for_file_access()
                    .errmsg(format!("could not close WAL segment {xlogfname}: %m"))
                    .into_error());
            }

            /*
             * Create .done file forcibly to prevent the streamed segment from
             * being archived later.
             */
            if !xlog_archive_mode_is_always() {
                xlogarchive::xlog_archive_force_done::call(xlogfname)?;
            } else {
                xlogarchive::xlog_archive_notify::call(xlogfname)?;
            }
        }
        with_state(|s| s.recvFile = -1);

        elog(
            DEBUG1,
            "walreceiver ended streaming and awaits new instructions",
        )?;
        WalRcvWaitForStartPosition(&mut startpoint, &mut startpointTLI)?;
        with_state(|s| s.startpointTLI = startpointTLI);
    }
    /* not reached */
}

/// `static void WalRcvWaitForStartPosition(XLogRecPtr *startpoint, TimeLineID *startpointTLI)`.
fn WalRcvWaitForStartPosition(
    startpoint: &mut XLogRecPtr,
    startpointTLI: &mut TimeLineID,
) -> PgResult<()> {
    /*
     * SpinLockAcquire; if state != STREAMING, release and proc_exit(0) / FATAL;
     * else move to WAITING and clear receiveStart/TLI.
     */
    let state = walrcvfuncs::walrcv_begin_wait::call();
    if state != WalRcvState::WALRCV_STREAMING {
        if state == WalRcvState::WALRCV_STOPPING {
            ipc::proc_exit::call(0, my_proc_pid());
        } else {
            elog(FATAL, "unexpected walreceiver state")?;
        }
    }

    ps_status::set_ps_display::call("idle".to_string());

    /*
     * nudge startup process to notice that we've stopped streaming and are now
     * waiting for instructions.
     */
    xlogrecovery::wakeup_recovery::call();
    loop {
        latch::reset_latch_my_latch::call();

        tcop::check_for_interrupts::call()?;

        /*
         * Assert(walRcvState == RESTARTING || WAITING || STOPPING). Poll the
         * shmem state.
         */
        let (st, recv_start, recv_start_tli) = walrcvfuncs::walrcv_poll_wait::call();
        assert!(
            st == WalRcvState::WALRCV_RESTARTING
                || st == WalRcvState::WALRCV_WAITING
                || st == WalRcvState::WALRCV_STOPPING
        );
        if st == WalRcvState::WALRCV_RESTARTING {
            /*
             * No need to handle changes in primary_conninfo or
             * primary_slot_name here. (walrcv_poll_wait moved RESTARTING ->
             * STREAMING under the lock and returned the new start point.)
             */
            *startpoint = recv_start;
            *startpointTLI = recv_start_tli;
            break;
        }
        if st == WalRcvState::WALRCV_STOPPING {
            /*
             * We should've received SIGTERM if the startup process wants us to
             * die, but might as well check it here too.
             */
            ipc::proc_exit::call(1, my_proc_pid());
        }

        let _ = latch::wait_latch::call(
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_WAL_RECEIVER_WAIT_START,
        );
    }

    if ps_status::update_process_title::call() {
        let activitymsg = format!("restarting at {}", lsn_fmt(*startpoint));
        ps_status::set_ps_display::call(activitymsg);
    }

    Ok(())
}

/// `static void WalRcvFetchTimeLineHistoryFiles(TimeLineID first, TimeLineID last)`.
fn WalRcvFetchTimeLineHistoryFiles(first: TimeLineID, last: TimeLineID) -> PgResult<()> {
    let conn = with_state(|s| s.wrconn).ok_or_else(|| {
        PgError::error("WalRcvFetchTimeLineHistoryFiles: wrconn set before fetching history")
    })?;

    let mut tli = first;
    while tli <= last {
        /* there's no history file for timeline 1 */
        if tli != 1 && !timeline::exists_timeline_history::call(tli) {
            emit(ereport(LOG).errmsg(format!(
                "fetching timeline history file for timeline {tli} from primary server"
            )));

            let (fname, content) =
                libpqwalrcv::walrcv_readtimelinehistoryfile::call(conn, tli)?;

            /*
             * Check that the filename on the primary matches what we calculated
             * ourselves. This is just a sanity check, it should always match.
             */
            let expectedfname = TLHistoryFileName(tli);
            if fname != expectedfname {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg_internal(format!(
                        "primary reported unexpected file name for timeline history file of timeline {tli}"
                    ))
                    .into_error());
            }

            /* Write the file to pg_wal. */
            timeline::write_timeline_history_file::call(tli, content)?;

            /*
             * Mark the streamed history file as ready for archiving if
             * archive_mode is always.
             */
            if !xlog_archive_mode_is_always() {
                xlogarchive::xlog_archive_force_done::call(fname)?;
            } else {
                xlogarchive::xlog_archive_notify::call(fname)?;
            }
        }
        tli += 1;
    }
    Ok(())
}

/// `on_shmem_exit` callback wrapper for `WalRcvDie` (C registers
/// `PointerGetDatum(&startpointTLI)`; we read the live TLI from thread-local
/// state instead, matching the pointer's read-latest semantics).
fn wal_rcv_die_callback(code: i32, _arg: types_datum::Datum) -> PgResult<()> {
    let startpointTLI = with_state(|s| s.startpointTLI);
    WalRcvDie(code, startpointTLI)
}

/// `static void WalRcvDie(int code, Datum arg)`.
pub fn WalRcvDie(_code: i32, startpointTLI: TimeLineID) -> PgResult<()> {
    assert!(startpointTLI != 0); /* Assert(*startpointTLI_p != 0); */

    /* Ensure that all WAL records received are flushed to disk */
    XLogWalRcvFlush(true, startpointTLI)?;

    /*
     * Mark ourselves inactive in shared memory: assert running state, set
     * STOPPED, clear pid/procno/ready_to_display, broadcast the stopped CV.
     */
    walrcvfuncs::walrcv_die_shmem::call();

    /* Terminate the connection gracefully. */
    if let Some(conn) = with_state(|s| s.wrconn) {
        libpqwalrcv::walrcv_disconnect::call(conn);
    }

    /* Wake up the startup process to notice promptly that we're gone */
    xlogrecovery::wakeup_recovery::call();
    Ok(())
}

/// `static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len, TimeLineID tli)`.
fn XLogWalRcvProcessMsg(r#type: u8, buf: &[u8], tli: TimeLineID) -> PgResult<()> {
    let len = buf.len();
    match r#type {
        b'w' => {
            /* WAL records */
            let hdrlen = 8 + 8 + 8; /* sizeof(int64) * 3 */
            if len < hdrlen {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg_internal("invalid WAL message received from primary")
                    .into_error());
            }

            /* read the fields */
            let dataStart = pq_getmsgint64(&buf[0..8]) as XLogRecPtr;
            let walEnd = pq_getmsgint64(&buf[8..16]) as XLogRecPtr;
            let sendTime = pq_getmsgint64(&buf[16..24]) as TimestampTz;
            ProcessWalSndrMessage(walEnd, sendTime);

            XLogWalRcvWrite(&buf[hdrlen..], dataStart, tli)?;
            Ok(())
        }
        b'k' => {
            /* Keepalive */
            let hdrlen = 8 + 8 + 1; /* sizeof(int64)*2 + sizeof(char) */
            if len != hdrlen {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg_internal("invalid keepalive message received from primary")
                    .into_error());
            }

            /* read the fields */
            let walEnd = pq_getmsgint64(&buf[0..8]) as XLogRecPtr;
            let sendTime = pq_getmsgint64(&buf[8..16]) as TimestampTz;
            let replyRequested = buf[16]; /* pq_getmsgbyte */

            ProcessWalSndrMessage(walEnd, sendTime);

            /* If the primary requested a reply, send one immediately */
            if replyRequested != 0 {
                XLogWalRcvSendReply(true, false)?;
            }
            Ok(())
        }
        other => Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg_internal(format!("invalid replication message type {}", other as i32))
            .into_error()),
    }
}

/// `static void XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr, TimeLineID tli)`.
fn XLogWalRcvWrite(buf: &[u8], recptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    let wal_segment_size = xlog::wal_segment_size::call();

    assert!(tli != 0); /* Assert(tli != 0); */

    let mut recptr = recptr;
    let mut nbytes = buf.len() as i64;
    let mut buf_off = 0usize;

    while nbytes > 0 {
        /* Close the current segment if it's completed */
        let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
        if recv_file >= 0 && !XLByteInSeg(recptr, recv_seg_no, wal_segment_size) {
            XLogWalRcvClose(recptr, tli)?;
        }

        if with_state(|s| s.recvFile) < 0 {
            /* Create/use new log file */
            let seg = XLByteToSeg(recptr, wal_segment_size);
            with_state(|s| s.recvSegNo = seg);
            let fd = xlog::xlog_file_init::call(seg, tli)?;
            with_state(|s| {
                s.recvFile = fd;
                s.recvFileTLI = tli;
            });
        }

        /* Calculate the start offset of the received logs */
        let startoff = XLogSegmentOffset(recptr, wal_segment_size);

        let segbytes = if startoff as i64 + nbytes > wal_segment_size as i64 {
            (wal_segment_size - startoff) as i64
        } else {
            nbytes
        };

        /* OK to write the logs */

        /* Measure I/O timing to write WAL data, for pg_stat_io. */
        let start = pgstat_io::pgstat_prepare_io_time::call();

        waitevent::pgstat_report_wait_start::call(WAIT_EVENT_WAL_WRITE);
        let recv_file = with_state(|s| s.recvFile);
        let write_res = pg_pwrite(
            recv_file,
            &buf[buf_off..buf_off + segbytes as usize],
            startoff as i64,
        );
        waitevent::pgstat_report_wait_end::call();

        let byteswritten: i64 = match write_res {
            Ok(n) => n as i64,
            Err(_) => 0,
        };

        pgstat_io::pgstat_count_io_op_time::call(start, byteswritten.max(0) as u32);

        if byteswritten <= 0 {
            /*
             * if write didn't set errno, assume no disk space — the
             * filename/PANIC path.
             */
            let (recv_file_tli, recv_seg_no) = with_state(|s| (s.recvFileTLI, s.recvSegNo));
            let xlogfname = XLogFileName(recv_file_tli, recv_seg_no, wal_segment_size);
            return Err(ereport(PANIC)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not write to WAL segment {xlogfname} at offset {startoff}, length {segbytes}: %m"
                ))
                .into_error());
        }

        /* Update state for write */
        recptr += byteswritten as u64;

        nbytes -= byteswritten;
        buf_off += byteswritten as usize;

        with_state(|s| s.LogstreamResult.Write = recptr);
    }

    /* Update shared-memory status */
    let write = with_state(|s| s.LogstreamResult.Write);
    walrcvfuncs::set_written_upto::call(write);

    /*
     * Close the current segment if it's fully written up in the last cycle of
     * the loop, to create its archive notification file soon.
     */
    let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
    if recv_file >= 0 && !XLByteInSeg(recptr, recv_seg_no, wal_segment_size) {
        XLogWalRcvClose(recptr, tli)?;
    }

    Ok(())
}

/// `static void XLogWalRcvFlush(bool dying, TimeLineID tli)`.
fn XLogWalRcvFlush(dying: bool, tli: TimeLineID) -> PgResult<()> {
    assert!(tli != 0); /* Assert(tli != 0); */

    let (flush, write) = with_state(|s| (s.LogstreamResult.Flush, s.LogstreamResult.Write));
    if flush < write {
        let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
        xlog::issue_xlog_fsync::call(recv_file, recv_seg_no, tli)?;

        with_state(|s| s.LogstreamResult.Flush = s.LogstreamResult.Write);
        let new_flush = with_state(|s| s.LogstreamResult.Flush);

        /*
         * Update shared-memory status: advance flushedUpto/latestChunkStart/
         * receivedTLI if we moved forward (under the spinlock in the seam).
         */
        walrcvfuncs::flush_advance_shmem::call(new_flush, tli);

        /* Signal the startup process and walsender that new WAL has arrived */
        xlogrecovery::wakeup_recovery::call();
        walsender::walsnd_wakeup_if_cascading::call();

        /* Report XLOG streaming progress in PS display */
        if ps_status::update_process_title::call() {
            let activitymsg = format!("streaming {}", lsn_fmt(write));
            ps_status::set_ps_display::call(activitymsg);
        }

        /* Also let the primary know that we made some progress */
        if !dying {
            XLogWalRcvSendReply(false, false)?;
            XLogWalRcvSendHSFeedback(false)?;
        }
    }
    Ok(())
}

/// `static void XLogWalRcvClose(XLogRecPtr recptr, TimeLineID tli)`.
fn XLogWalRcvClose(recptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    let wal_segment_size = xlog::wal_segment_size::call();

    let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
    assert!(recv_file >= 0 && !XLByteInSeg(recptr, recv_seg_no, wal_segment_size));
    assert!(tli != 0);

    /*
     * fsync() and close current file before we switch to next one. We would
     * otherwise have to reopen this file to fsync it later.
     */
    XLogWalRcvFlush(false, tli)?;

    let (recv_file_tli, recv_seg_no) = with_state(|s| (s.recvFileTLI, s.recvSegNo));
    let xlogfname = XLogFileName(recv_file_tli, recv_seg_no, wal_segment_size);

    /*
     * XLOG segment files will be re-read by recovery in startup process soon,
     * so we don't advise the OS to release cache pages.
     */
    let recv_file = with_state(|s| s.recvFile);
    if pg_close(recv_file) != 0 {
        return Err(ereport(PANIC)
            .errcode_for_file_access()
            .errmsg(format!("could not close WAL segment {xlogfname}: %m"))
            .into_error());
    }

    /*
     * Create .done file forcibly to prevent the streamed segment from being
     * archived later.
     */
    if !xlog_archive_mode_is_always() {
        xlogarchive::xlog_archive_force_done::call(xlogfname)?;
    } else {
        xlogarchive::xlog_archive_notify::call(xlogfname)?;
    }

    with_state(|s| s.recvFile = -1);
    Ok(())
}

/// `static void XLogWalRcvSendReply(bool force, bool requestReply)`.
fn XLogWalRcvSendReply(force: bool, requestReply: bool) -> PgResult<()> {
    /*
     * If the user doesn't want status to be reported to the primary, be sure to
     * exit before doing anything at all.
     */
    if !force && wal_receiver_status_interval() <= 0 {
        return Ok(());
    }

    /* Get current timestamp. */
    let now = timestamp::get_current_timestamp::call();

    /*
     * We can compare the write and flush positions without a lock, but apply
     * requires a spin lock, so we don't check that unless something else has
     * changed or 10 seconds have passed.
     */
    let (write, flush, reply_write, reply_flush, reply_wakeup) = with_state(|s| {
        (
            s.LogstreamResult.Write,
            s.LogstreamResult.Flush,
            s.reply_writePtr,
            s.reply_flushPtr,
            s.wakeup[WALRCV_WAKEUP_REPLY as usize],
        )
    });
    if !force && reply_write == write && reply_flush == flush && now < reply_wakeup {
        return Ok(());
    }

    /* Make sure we wake up when it's time to send another reply. */
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_REPLY, now);

    /* Construct a new message */
    let writePtr = write;
    let flushPtr = flush;
    let applyPtr = xlogrecovery::get_xlog_replay_recptr::call();
    with_state(|s| {
        s.reply_writePtr = writePtr;
        s.reply_flushPtr = flushPtr;
    });

    let mut reply_message: Vec<u8> = Vec::new();
    pq_sendbyte(&mut reply_message, b'r');
    pq_sendint64(&mut reply_message, writePtr as i64);
    pq_sendint64(&mut reply_message, flushPtr as i64);
    pq_sendint64(&mut reply_message, applyPtr as i64);
    pq_sendint64(&mut reply_message, timestamp::get_current_timestamp::call());
    pq_sendbyte(&mut reply_message, if requestReply { 1 } else { 0 });

    /* Send it */
    elog(
        DEBUG2,
        format!(
            "sending write {} flush {} apply {}{}",
            lsn_fmt(writePtr),
            lsn_fmt(flushPtr),
            lsn_fmt(applyPtr),
            if requestReply {
                " (reply requested)"
            } else {
                ""
            }
        ),
    )?;

    let conn = with_state(|s| s.wrconn)
        .ok_or_else(|| PgError::error("XLogWalRcvSendReply: wrconn set during streaming"))?;
    libpqwalrcv::walrcv_send::call(conn, reply_message)?;
    Ok(())
}

/// `static void XLogWalRcvSendHSFeedback(bool immed)`.
fn XLogWalRcvSendHSFeedback(immed: bool) -> PgResult<()> {
    let primary_has_standby_xmin = with_state(|s| s.primary_has_standby_xmin);

    /*
     * If the user doesn't want status to be reported to the primary, be sure to
     * exit before doing anything at all.
     */
    if (wal_receiver_status_interval() <= 0 || !hot_standby_feedback()) && !primary_has_standby_xmin
    {
        return Ok(());
    }

    /* Get current timestamp. */
    let now = timestamp::get_current_timestamp::call();

    /* Send feedback at most once per wal_receiver_status_interval. */
    if !immed && now < with_state(|s| s.wakeup[WALRCV_WAKEUP_HSFEEDBACK as usize]) {
        return Ok(());
    }

    /* Make sure we wake up when it's time to send feedback again. */
    WalRcvComputeNextWakeup(WALRCV_WAKEUP_HSFEEDBACK, now);

    /*
     * If Hot Standby is not yet accepting connections there is nothing to send.
     */
    if !xlogrecovery::hot_standby_active::call() {
        return Ok(());
    }

    /*
     * Make the expensive call to get the oldest xmin once we are certain
     * everything else has been checked.
     */
    let (xmin, catalog_xmin): (TransactionId, TransactionId);
    if hot_standby_feedback() {
        let (x, c) = procarray::get_replication_horizons::call();
        xmin = x;
        catalog_xmin = c;
    } else {
        xmin = InvalidTransactionId;
        catalog_xmin = InvalidTransactionId;
    }

    /*
     * Get epoch and adjust if nextXid and oldestXmin are different sides of the
     * epoch boundary.
     */
    let nextFullXid = varsup::read_next_full_transaction_id::call().value;
    let nextXid = XidFromFullTransactionId(nextFullXid);
    let mut xmin_epoch = EpochFromFullTransactionId(nextFullXid);
    let mut catalog_xmin_epoch = xmin_epoch;
    if nextXid < xmin {
        xmin_epoch -= 1;
    }
    if nextXid < catalog_xmin {
        catalog_xmin_epoch -= 1;
    }

    elog(
        DEBUG2,
        format!(
            "sending hot standby feedback xmin {xmin} epoch {xmin_epoch} catalog_xmin {catalog_xmin} catalog_xmin_epoch {catalog_xmin_epoch}"
        ),
    )?;

    /* Construct the message and send it. */
    let mut reply_message: Vec<u8> = Vec::new();
    pq_sendbyte(&mut reply_message, b'h');
    pq_sendint64(&mut reply_message, timestamp::get_current_timestamp::call());
    pq_sendint32(&mut reply_message, xmin as i32);
    pq_sendint32(&mut reply_message, xmin_epoch as i32);
    pq_sendint32(&mut reply_message, catalog_xmin as i32);
    pq_sendint32(&mut reply_message, catalog_xmin_epoch as i32);
    let conn = with_state(|s| s.wrconn)
        .ok_or_else(|| PgError::error("XLogWalRcvSendHSFeedback: wrconn set during streaming"))?;
    libpqwalrcv::walrcv_send::call(conn, reply_message)?;
    if TransactionIdIsValid(xmin) || TransactionIdIsValid(catalog_xmin) {
        with_state(|s| s.primary_has_standby_xmin = true);
    } else {
        with_state(|s| s.primary_has_standby_xmin = false);
    }
    Ok(())
}

/// `static void ProcessWalSndrMessage(XLogRecPtr walEnd, TimestampTz sendTime)`.
fn ProcessWalSndrMessage(walEnd: XLogRecPtr, sendTime: TimestampTz) {
    let lastMsgReceiptTime = timestamp::get_current_timestamp::call();

    /* Update shared-memory status (latestWalEnd/Time, lastMsgSend/Receipt). */
    walrcvfuncs::process_walsndr_shmem::call(walEnd, sendTime, lastMsgReceiptTime);

    if message_level_is_interesting(DEBUG2) {
        /* Copy because timestamptz_to_str returns a static buffer */
        let sendtime = timestamp::timestamptz_to_str::call(sendTime);
        let receipttime = timestamp::timestamptz_to_str::call(lastMsgReceiptTime);
        let applyDelay = walrcvfuncs::get_replication_apply_delay::call();

        /* apply delay is not available */
        if applyDelay == -1 {
            let _ = elog(
                DEBUG2,
                format!(
                    "sendtime {sendtime} receipttime {receipttime} replication apply delay (N/A) transfer latency {} ms",
                    walrcvfuncs::get_replication_transfer_latency::call()
                ),
            );
        } else {
            let _ = elog(
                DEBUG2,
                format!(
                    "sendtime {sendtime} receipttime {receipttime} replication apply delay {applyDelay} ms transfer latency {} ms",
                    walrcvfuncs::get_replication_transfer_latency::call()
                ),
            );
        }
    }
}

/// `static void WalRcvComputeNextWakeup(WalRcvWakeupReason reason, TimestampTz now)`.
fn WalRcvComputeNextWakeup(reason: WalRcvWakeupReason, now: TimestampTz) {
    let idx = reason as usize;
    match reason {
        WALRCV_WAKEUP_TERMINATE => {
            let v = if wal_receiver_timeout() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                TimestampTzPlusMilliseconds(now, wal_receiver_timeout() as i64)
            };
            with_state(|s| s.wakeup[idx] = v);
        }
        WALRCV_WAKEUP_PING => {
            let v = if wal_receiver_timeout() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                TimestampTzPlusMilliseconds(now, (wal_receiver_timeout() / 2) as i64)
            };
            with_state(|s| s.wakeup[idx] = v);
        }
        WALRCV_WAKEUP_HSFEEDBACK => {
            let v = if !hot_standby_feedback() || wal_receiver_status_interval() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                TimestampTzPlusSeconds(now, wal_receiver_status_interval() as i64)
            };
            with_state(|s| s.wakeup[idx] = v);
        }
        WALRCV_WAKEUP_REPLY => {
            let v = if wal_receiver_status_interval() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                TimestampTzPlusSeconds(now, wal_receiver_status_interval() as i64)
            };
            with_state(|s| s.wakeup[idx] = v);
        } /* there's intentionally no default: here */
    }
}

/// `void WalRcvForceReply(void)`.
///
/// Called by the startup process whenever interesting xlog records are applied.
pub fn WalRcvForceReply() {
    /*
     * Set force_reply, read procno under the lock, and
     * SetLatch(&GetPGProcByNumber(procno)->procLatch) if valid. The whole
     * sequence lives behind the seam since WalRcv + PGPROC are shared memory.
     */
    walrcvfuncs::walrcv_force_reply::call();
}

/// `static const char *WalRcvGetStateString(WalRcvState state)`.
fn WalRcvGetStateString(state: WalRcvState) -> &'static str {
    match state {
        WalRcvState::WALRCV_STOPPED => "stopped",
        WalRcvState::WALRCV_STARTING => "starting",
        WalRcvState::WALRCV_STREAMING => "streaming",
        WalRcvState::WALRCV_WAITING => "waiting",
        WalRcvState::WALRCV_RESTARTING => "restarting",
        WalRcvState::WALRCV_STOPPING => "stopping",
    }
}

/// `Datum pg_stat_get_wal_receiver(PG_FUNCTION_ARGS)`.
///
/// The fmgr/`Datum`/tuple-construction layer is a project-wide systemic
/// deferral; the structured result is exposed here as a plain Rust value via
/// [`WalReceiverActivity`], with the field-by-field NULL/value selection logic
/// ported 1:1 from C. `Ok(None)` corresponds to C's `PG_RETURN_NULL()`.
pub fn pg_stat_get_wal_receiver() -> PgResult<Option<WalReceiverActivity>> {
    /* Take a lock to ensure value consistency (the seam snapshots WalRcv). */
    let snap = walrcvfuncs::pg_stat_get_wal_receiver_snapshot::call();

    /*
     * No WAL receiver (or not ready yet), just return a tuple with NULL values.
     */
    if snap.pid == 0 || !snap.ready_to_display {
        return Ok(None);
    }

    /*
     * Read "writtenUpto" without holding a spinlock.
     */
    let written_lsn = walrcvfuncs::get_written_upto::call();

    /* Fetch values */
    let mut act = WalReceiverActivity {
        pid: snap.pid,
        ..Default::default()
    };

    if !acl::has_privs_of_role::call(miscinit::get_user_id::call(), ROLE_PG_READ_ALL_STATS)? {
        /*
         * Only superusers and roles with pg_read_all_stats can see details.
         * Others get only pid (the rest stay NULL via Default).
         */
        return Ok(Some(act));
    }

    act.state = Some(WalRcvGetStateString(snap.state).to_string());

    if XLogRecPtrIsInvalid(snap.receive_start_lsn) {
        act.receive_start_lsn = None;
    } else {
        act.receive_start_lsn = Some(snap.receive_start_lsn);
    }
    act.receive_start_tli = Some(snap.receive_start_tli);
    if XLogRecPtrIsInvalid(written_lsn) {
        act.written_lsn = None;
    } else {
        act.written_lsn = Some(written_lsn);
    }
    if XLogRecPtrIsInvalid(snap.flushed_lsn) {
        act.flushed_lsn = None;
    } else {
        act.flushed_lsn = Some(snap.flushed_lsn);
    }
    act.received_tli = Some(snap.received_tli);
    if snap.last_send_time == 0 {
        act.last_send_time = None;
    } else {
        act.last_send_time = Some(snap.last_send_time);
    }
    if snap.last_receipt_time == 0 {
        act.last_receipt_time = None;
    } else {
        act.last_receipt_time = Some(snap.last_receipt_time);
    }
    if XLogRecPtrIsInvalid(snap.latest_end_lsn) {
        act.latest_end_lsn = None;
    } else {
        act.latest_end_lsn = Some(snap.latest_end_lsn);
    }
    if snap.latest_end_time == 0 {
        act.latest_end_time = None;
    } else {
        act.latest_end_time = Some(snap.latest_end_time);
    }
    if snap.slotname.is_empty() {
        act.slotname = None;
    } else {
        act.slotname = Some(snap.slotname);
    }
    if snap.sender_host.is_empty() {
        act.sender_host = None;
    } else {
        act.sender_host = Some(snap.sender_host);
    }
    if snap.sender_port == 0 {
        act.sender_port = None;
    } else {
        act.sender_port = Some(snap.sender_port);
    }
    if snap.conninfo.is_empty() {
        act.conninfo = None;
    } else {
        act.conninfo = Some(snap.conninfo);
    }

    Ok(Some(act))
}

// ---------------------------------------------------------------------------
// Signal-handler setup (the `pqsignal(...)` block from WalReceiverMain).
// ---------------------------------------------------------------------------

fn setup_signal_handlers() {
    use types_signal::SigHandler;
    let pqsignal = port_pqsignal_seams::pqsignal::call;

    pqsignal(
        libc::SIGHUP,
        SigHandler::Handler(signal_handler_for_config_reload),
    );
    pqsignal(libc::SIGINT, SigHandler::Ignore);
    pqsignal(libc::SIGTERM, SigHandler::Handler(tcop::die::call)); /* request shutdown */
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(libc::SIGALRM, SigHandler::Ignore);
    pqsignal(libc::SIGPIPE, SigHandler::Ignore);
    pqsignal(
        libc::SIGUSR1,
        SigHandler::Handler(backend_storage_ipc_procsignal_seams::procsignal_sigusr1_handler::call),
    );
    pqsignal(libc::SIGUSR2, SigHandler::Ignore);

    /* Reset some signals that are accepted by postmaster but not here */
    pqsignal(libc::SIGCHLD, SigHandler::Default);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    let masks = backend_libpq_pqsignal::signal_masks();
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), core::ptr::null_mut());
    }
}

/// `SignalHandlerForConfigReload(SIGNAL_ARGS)` — installed directly (the
/// interrupt unit is an acyclic direct dependency).
fn signal_handler_for_config_reload(_postgres_signal_arg: i32) {
    backend_postmaster_interrupt::SignalHandlerForConfigReload();
}

/// `MyProcPid` (globals.c) — passed to `proc_exit` per the no-ambient-global
/// rule.
fn my_proc_pid() -> i32 {
    backend_utils_init_small_seams::my_proc_pid::call()
}

// ---------------------------------------------------------------------------
// File I/O helpers (close / pg_pwrite) used by the segment write path.
// ---------------------------------------------------------------------------

/// `close(fd)` — direct libc call (file-descriptor close; the kernel call C
/// itself makes, no seam needed).
fn pg_close(fd: i32) -> i32 {
    unsafe { libc::close(fd) }
}

/// `pg_pwrite(fd, buf, count, offset)` — direct libc pwrite.
fn pg_pwrite(fd: i32, buf: &[u8], offset: i64) -> Result<usize, i32> {
    let n = unsafe {
        libc::pwrite(
            fd,
            buf.as_ptr() as *const libc::c_void,
            buf.len(),
            offset as libc::off_t,
        )
    };
    if n < 0 {
        Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
    } else {
        Ok(n as usize)
    }
}

// ---------------------------------------------------------------------------
// pqformat helpers (libpq/pqformat.h) used by message construction above.
// These are pure encoders/decoders, ported in-crate.
// ---------------------------------------------------------------------------

/// `pq_sendbyte(buf, byt)`.
fn pq_sendbyte(buf: &mut Vec<u8>, byt: u8) {
    buf.push(byt);
}

/// `pq_sendint32(buf, i)` — network (big-endian) byte order.
fn pq_sendint32(buf: &mut Vec<u8>, i: i32) {
    buf.extend_from_slice(&i.to_be_bytes());
}

/// `pq_sendint64(buf, i)` — network (big-endian) byte order.
fn pq_sendint64(buf: &mut Vec<u8>, i: i64) {
    buf.extend_from_slice(&i.to_be_bytes());
}

/// `pq_getmsgint64(msg)` — read a network-order int64.
fn pq_getmsgint64(b: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&b[0..8]);
    i64::from_be_bytes(arr)
}

// ---------------------------------------------------------------------------
// Misc helpers.
// ---------------------------------------------------------------------------

/// Decode a NUL-terminated fixed C char buffer into a Rust `String`.
fn cstr_from_bytes(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

/// `strlcpy(dst, src, NAMEDATALEN)` into a fresh fixed buffer.
fn name_from_str(s: &str) -> [u8; NAMEDATALEN] {
    let mut out = [0u8; NAMEDATALEN];
    let bytes = s.as_bytes();
    let n = bytes.len().min(NAMEDATALEN - 1);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

/// Map a `wakeup[]` index back to its `WalRcvWakeupReason`.
fn wakeup_reason_from_index(i: usize) -> WalRcvWakeupReason {
    match i {
        0 => WALRCV_WAKEUP_TERMINATE,
        1 => WALRCV_WAKEUP_PING,
        2 => WALRCV_WAKEUP_REPLY,
        3 => WALRCV_WAKEUP_HSFEEDBACK,
        _ => unreachable!("wakeup index out of range"),
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam declared in `backend-replication-walreceiver-seams`.
pub fn init_seams() {
    backend_replication_walreceiver_seams::wal_receiver_main::set(wal_receiver_main);
}

#[cfg(test)]
mod tests;
