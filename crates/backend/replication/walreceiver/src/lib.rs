// walreceiver.c: the standby-side WAL receiver daemon. The transport is the
// in-crate replication-protocol client (client.rs, mapping libpqwalreceiver.c).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::{Cell, RefCell};

use elog::{elog, ereport};
use init_small::globals as g;
use types_core::{
    pgsocket, InvalidXLogRecPtr, TimeLineID, TimestampTz, TransactionId, XLogRecPtr, XLogSegNo,
    INVALID_PROC_NUMBER, PGINVALID_SOCKET,
};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, DEBUG2, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROTOCOL_VIOLATION, ERROR, FATAL, LOG,
    PANIC,
};
use types_startup::StartupData;
use types_storage::waiteventset::{
    WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_SOCKET_READABLE, WL_TIMEOUT,
};
use walreceiverfuncs::{with_walrcv, WalRcvState, NAMEDATALEN};

pub mod client;
use client::PgConn;

const WAIT_EVENT_WAL_RECEIVER_MAIN: u32 = 0x0500_0000 | 14;
const WAIT_EVENT_WAL_RECEIVER_WAIT_START: u32 = 0x0800_0000 | 54;
const WAIT_EVENT_WAL_WRITE: u32 = 0x0A00_0000 | 80;

const NUM_WALRCV_WAKEUPS: usize = 4;
const WAKEUP_TERMINATE: usize = 0;
const WAKEUP_PING: usize = 1;
const WAKEUP_REPLY: usize = 2;
const WAKEUP_HSFEEDBACK: usize = 3;
const TIMESTAMP_INFINITY: TimestampTz = i64::MAX;
const InvalidTransactionId: TransactionId = 0;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/replication/walreceiver.c", line, func)
}

fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

fn get_ts() -> TimestampTz {
    timestamp_seams::get_current_timestamp::call()
}

struct FileState {
    recvFile: i32,
    recvFileTLI: TimeLineID,
    recvSegNo: XLogSegNo,
    write: XLogRecPtr,
    flush: XLogRecPtr,
    wakeup: [TimestampTz; NUM_WALRCV_WAKEUPS],
    reply_writePtr: XLogRecPtr,
    reply_flushPtr: XLogRecPtr,
    primary_has_standby_xmin: bool,
}

thread_local! {
    static STATE: RefCell<FileState> = RefCell::new(FileState {
        recvFile: -1,
        recvFileTLI: 0,
        recvSegNo: 0,
        write: InvalidXLogRecPtr,
        flush: InvalidXLogRecPtr,
        wakeup: [0; NUM_WALRCV_WAKEUPS],
        reply_writePtr: 0,
        reply_flushPtr: 0,
        primary_has_standby_xmin: true,
    });
    static CONN: RefCell<Option<PgConn>> = const { RefCell::new(None) };
    // on_shmem_exit(WalRcvDie, &startpointTLI): the pointer's read-latest
    // semantics, as a cell.
    static STARTPOINT_TLI: Cell<TimeLineID> = const { Cell::new(0) };
}

fn with_state<R>(f: impl FnOnce(&mut FileState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

fn with_conn<R>(f: impl FnOnce(&mut PgConn) -> R) -> R {
    CONN.with(|c| {
        let mut c = c.borrow_mut();
        f(c.as_mut().expect("walreceiver connection not established"))
    })
}

fn wal_receiver_status_interval() -> i32 {
    guc_tables::vars::wal_receiver_status_interval.read()
}
fn wal_receiver_timeout() -> i32 {
    guc_tables::vars::wal_receiver_timeout.read()
}
fn hot_standby_feedback() -> bool {
    guc_tables::vars::hot_standby_feedback.read()
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

fn sigterm_die() -> PgResult<()> {
    postgres_seams::die::call()
}

pub fn WalReceiverMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::WalReceiver);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    match wal_receiver_main_inner() {
        Ok(()) => unreachable!("WalReceiverMain loops forever"),
        Err(e) => fatal_exit(&e),
    }
}

fn wal_receiver_main_inner() -> PgResult<()> {
    let now = get_ts();

    // Mark walreceiver in shared memory as early as possible, so a failure
    // after this point flips the state to STOPPED via WalRcvDie.
    let mut should_exit = false;
    let mut conninfo = String::new();
    let mut slotname = String::new();
    let mut is_temp_slot = false;
    let mut startpoint = InvalidXLogRecPtr;
    let mut startpointTLI: TimeLineID = 0;
    let mut still_running = false;
    with_walrcv(|d| {
        assert!(d.pid == 0);
        match d.walRcvState {
            WalRcvState::Stopping => {
                d.walRcvState = WalRcvState::Stopped;
                should_exit = true;
            }
            WalRcvState::Stopped => should_exit = true,
            WalRcvState::Starting => {}
            WalRcvState::Waiting | WalRcvState::Streaming | WalRcvState::Restarting => {
                still_running = true;
            }
        }
        if should_exit || still_running {
            return;
        }
        d.pid = g::MyProcPid();
        d.walRcvState = WalRcvState::Streaming;
        d.ready_to_display = false;
        conninfo = d.conninfo.clone();
        slotname = d.slotname.clone();
        is_temp_slot = d.is_temp_slot;
        startpoint = d.receiveStart;
        startpointTLI = d.receiveStartTLI;
        d.lastMsgSendTime = now;
        d.lastMsgReceiptTime = now;
        d.latestWalEndTime = now;
        d.procno = g::MyProcNumber();
    });
    if still_running {
        return ereport(PANIC)
            .errmsg("walreceiver still running according to shared memory state")
            .finish(loc(213, "WalReceiverMain"));
    }
    if should_exit {
        walreceiverfuncs::wal_rcv_stopped_cv_broadcast();
        ipc::proc_exit(1, g::MyProcPid());
    }

    STARTPOINT_TLI.set(startpointTLI);
    assert!(!is_temp_slot || slotname.is_empty());

    walreceiverfuncs::set_written_upto(0);

    ipc::on_shmem_exit(wal_rcv_die, 0);

    {
        // procsignal::signums, not libc::SIG*: the wasi libc crate exposes
        // no SIG* names (thread-signal emulation numbering, signums law).
        use procsignal::signums::{SIGALRM, SIGHUP, SIGINT, SIGPIPE, SIGTERM, SIGUSR1, SIGUSR2};
        use procsignal::ThreadSignalHandler::{Fallible, Ignore, Simple};
        procsignal::pqsignal_thread(
            SIGHUP,
            Simple(interrupt::SignalHandlerForConfigReload),
        );
        procsignal::pqsignal_thread(SIGINT, Ignore);
        procsignal::pqsignal_thread(SIGTERM, Fallible(sigterm_die));
        procsignal::pqsignal_thread(SIGALRM, Ignore);
        procsignal::pqsignal_thread(SIGPIPE, Ignore);
        procsignal::pqsignal_thread(
            SIGUSR1,
            Simple(procsignal::procsignal_sigusr1_handler),
        );
        procsignal::pqsignal_thread(SIGUSR2, Ignore);
    }
    libpq_pqsignal::unblock_signals();

    // Establish the connection to the primary for XLOG streaming.
    let cluster_name = guc_tables::vars::cluster_name.read().unwrap_or_default();
    let appname = if cluster_name.is_empty() { "walreceiver" } else { cluster_name.as_str() };
    match client::connect(&conninfo, appname)? {
        Ok(conn) => CONN.with(|c| *c.borrow_mut() = Some(conn)),
        Err(err) => {
            return ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "streaming replication receiver \"{appname}\" could not connect to the primary server: {err}"
                ))
                .finish(loc(276, "WalReceiverMain"));
        }
    }

    let tmp_conninfo = with_conn(|c| c.display_conninfo());
    let (sender_host, sender_port) = with_conn(|c| c.senderinfo());
    with_walrcv(|d| {
        d.conninfo = tmp_conninfo;
        d.sender_host = sender_host;
        d.sender_port = sender_port;
        d.ready_to_display = true;
    });

    let mut first_stream = true;
    loop {
        let (primary_sysid, primaryTLI) = with_conn(client::identify_system)?;

        let standby_sysid = format!("{}", transam_xlog::GetSystemIdentifier());
        if primary_sysid != standby_sysid {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("database system identifier differs between the primary and standby")
                .errdetail(format!(
                    "The primary's identifier is {primary_sysid}, the standby's identifier is {standby_sysid}."
                ))
                .finish(loc(325, "WalReceiverMain"));
        }

        if primaryTLI < startpointTLI {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "highest timeline {primaryTLI} of the primary is behind recovery timeline {startpointTLI}"
                ))
                .finish(loc(336, "WalReceiverMain"));
        }

        WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI)?;

        if is_temp_slot {
            panic!(
                "walreceiver: wal_receiver_create_temp_slot needs the CREATE_REPLICATION_SLOT client command (unported)"
            );
        }

        let slot = if slotname.is_empty() { None } else { Some(slotname.clone()) };
        if with_conn(|c| client::start_streaming(c, slot.as_deref(), startpoint, startpointTLI))? {
            if first_stream {
                let _ = ereport(LOG)
                    .errmsg(format!(
                        "started streaming WAL from primary at {} on timeline {startpointTLI}",
                        lsn_fmt(startpoint)
                    ))
                    .finish(loc(389, "WalReceiverMain"));
            } else {
                let _ = ereport(LOG)
                    .errmsg(format!(
                        "restarted WAL streaming at {} on timeline {startpointTLI}",
                        lsn_fmt(startpoint)
                    ))
                    .finish(loc(393, "WalReceiverMain"));
            }
            first_stream = false;

            let (replay, _tli) = xlogrecovery_seams::get_xlog_replay_rec_ptr::call();
            with_state(|s| {
                s.write = replay;
                s.flush = replay;
            });

            let now = get_ts();
            for i in 0..NUM_WALRCV_WAKEUPS {
                WalRcvComputeNextWakeup(i, now);
            }

            XLogWalRcvSendReply(true, false)?;
            XLogWalRcvSendHSFeedback(true)?;

            let mut endofwal = false;
            while !endofwal {
                let mut wait_fd: pgsocket = PGINVALID_SOCKET;

                if !transam_xlog::RecoveryInProgress() {
                    return ereport(FATAL)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg("cannot continue WAL streaming, recovery has already ended")
                        .finish(loc(428, "WalReceiverMain"));
                }

                postgres_seams::check_for_interrupts::call()?;

                if interrupt::ConfigReloadPending() {
                    interrupt::SetConfigReloadPending(false);
                    guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
                    let now = get_ts();
                    for i in 0..NUM_WALRCV_WAKEUPS {
                        WalRcvComputeNextWakeup(i, now);
                    }
                    XLogWalRcvSendHSFeedback(true)?;
                }

                let (mut len, mut buf, fd) = with_conn(client::receive)?;
                if fd != PGINVALID_SOCKET {
                    wait_fd = fd;
                }
                if len != 0 {
                    loop {
                        if len > 0 {
                            let now = get_ts();
                            WalRcvComputeNextWakeup(WAKEUP_TERMINATE, now);
                            WalRcvComputeNextWakeup(WAKEUP_PING, now);
                            XLogWalRcvProcessMsg(buf[0], &buf[1..], startpointTLI)?;
                        } else if len == 0 {
                            break;
                        } else {
                            let write = with_state(|s| s.write);
                            let _ = ereport(LOG)
                                .errmsg("replication terminated by primary server")
                                .errdetail(format!(
                                    "End of WAL reached on timeline {startpointTLI} at {}.",
                                    lsn_fmt(write)
                                ))
                                .finish(loc(472, "WalReceiverMain"));
                            endofwal = true;
                            break;
                        }
                        let (l, b, f) = with_conn(client::receive)?;
                        len = l;
                        buf = b;
                        if f != PGINVALID_SOCKET {
                            wait_fd = f;
                        }
                    }

                    XLogWalRcvSendReply(false, false)?;
                    XLogWalRcvFlush(false, startpointTLI)?;
                }

                if endofwal {
                    break;
                }

                let mut next_wakeup = TIMESTAMP_INFINITY;
                for i in 0..NUM_WALRCV_WAKEUPS {
                    next_wakeup = next_wakeup.min(with_state(|s| s.wakeup[i]));
                }
                let now = get_ts();
                let nap = adt_timestamp::TimestampDifferenceMilliseconds(now, next_wakeup);

                assert!(wait_fd != PGINVALID_SOCKET);
                let rc = latch::WaitLatchOrSocket(
                    g::MyLatch(),
                    WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT | WL_LATCH_SET,
                    wait_fd,
                    nap,
                    WAIT_EVENT_WAL_RECEIVER_MAIN,
                )?;
                if rc & WL_LATCH_SET != 0 {
                    if let Some(l) = g::MyLatch() {
                        latch::ResetLatch(l);
                    }
                    postgres_seams::check_for_interrupts::call()?;

                    if walreceiverfuncs::take_force_reply() {
                        XLogWalRcvSendReply(true, false)?;
                    }
                }
                if rc & WL_TIMEOUT != 0 {
                    let mut request_reply = false;

                    pgstat::wal::pgstat_report_wal(false);

                    let now = get_ts();
                    if now >= with_state(|s| s.wakeup[WAKEUP_TERMINATE]) {
                        return ereport(ERROR)
                            .errcode(ERRCODE_CONNECTION_FAILURE)
                            .errmsg("terminating walreceiver due to timeout")
                            .finish(loc(573, "WalReceiverMain"));
                    }

                    if now >= with_state(|s| s.wakeup[WAKEUP_PING]) {
                        request_reply = true;
                        with_state(|s| s.wakeup[WAKEUP_PING] = TIMESTAMP_INFINITY);
                    }

                    XLogWalRcvSendReply(request_reply, request_reply)?;
                    XLogWalRcvSendHSFeedback(false)?;
                }
            }

            let primaryTLI = with_conn(client::end_streaming)?;
            WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI)?;
        } else {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "primary server contains no more WAL on requested timeline {startpointTLI}"
                ))
                .finish(loc(605, "WalReceiverMain"));
        }

        if with_state(|s| s.recvFile) >= 0 {
            XLogWalRcvFlush(false, startpointTLI)?;
            let (tli, segno) = with_state(|s| (s.recvFileTLI, s.recvSegNo));
            let xlogfname = transam_xlog::XLogFileName(tli, segno, transam_xlog::wal_segment_size());
            let recv_file = with_state(|s| s.recvFile);
            if unsafe { libc::close(recv_file) } != 0 {
                return ereport(PANIC)
                    .errcode_for_file_access()
                    .errmsg(format!("could not close WAL segment {xlogfname}"))
                    .finish(loc(621, "WalReceiverMain"));
            }

            if !transam_xlog::XLogArchivingAlways() {
                xlogarchive::XLogArchiveForceDone(&xlogfname)?;
            } else {
                xlogarchive::XLogArchiveNotify(&xlogfname)?;
            }
        }
        with_state(|s| s.recvFile = -1);

        let _ = elog(DEBUG1, "walreceiver ended streaming and awaits new instructions");
        WalRcvWaitForStartPosition(&mut startpoint, &mut startpointTLI)?;
        STARTPOINT_TLI.set(startpointTLI);
    }
}

fn WalRcvWaitForStartPosition(
    startpoint: &mut XLogRecPtr,
    startpointTLI: &mut TimeLineID,
) -> PgResult<()> {
    let mut state = WalRcvState::Streaming;
    with_walrcv(|d| {
        state = d.walRcvState;
        if state == WalRcvState::Streaming {
            d.walRcvState = WalRcvState::Waiting;
            d.receiveStart = InvalidXLogRecPtr;
            d.receiveStartTLI = 0;
        }
    });
    if state != WalRcvState::Streaming {
        if state == WalRcvState::Stopping {
            ipc::proc_exit(0, g::MyProcPid());
        }
        return Err(PgError::new(FATAL, "unexpected walreceiver state").into());
    }

    ps_status_seams::set_ps_display::call("idle");

    // Nudge startup to notice we stopped streaming and now await orders.
    xlogrecovery_seams::wakeup_recovery::call();
    loop {
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }
        postgres_seams::check_for_interrupts::call()?;

        let mut st = WalRcvState::Waiting;
        let mut recv_start = InvalidXLogRecPtr;
        let mut recv_start_tli: TimeLineID = 0;
        with_walrcv(|d| {
            st = d.walRcvState;
            debug_assert!(matches!(
                st,
                WalRcvState::Restarting | WalRcvState::Waiting | WalRcvState::Stopping
            ));
            if st == WalRcvState::Restarting {
                recv_start = d.receiveStart;
                recv_start_tli = d.receiveStartTLI;
                d.walRcvState = WalRcvState::Streaming;
            }
        });
        if st == WalRcvState::Restarting {
            *startpoint = recv_start;
            *startpointTLI = recv_start_tli;
            break;
        }
        if st == WalRcvState::Stopping {
            ipc::proc_exit(1, g::MyProcPid());
        }

        let _ = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            -1,
            WAIT_EVENT_WAL_RECEIVER_WAIT_START,
        )?;
    }

    if guc_tables::vars::update_process_title.read() {
        ps_status_seams::set_ps_display::call(&format!("restarting at {}", lsn_fmt(*startpoint)));
    }
    Ok(())
}

fn WalRcvFetchTimeLineHistoryFiles(first: TimeLineID, last: TimeLineID) -> PgResult<()> {
    for tli in first..=last {
        // There is no history file for timeline 1.
        if tli != 1 && !timeline::existsTimeLineHistory(tli, false)? {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "fetching timeline history file for timeline {tli} from primary server"
                ))
                .finish(loc(740, "WalRcvFetchTimeLineHistoryFiles"));

            let (fname, content) = with_conn(|c| client::read_timeline_history_file(c, tli))?;

            let expected = format!("{tli:08X}.history");
            if fname != expected {
                return ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg_internal(format!(
                        "primary reported unexpected file name for timeline history file of timeline {tli}"
                    ))
                    .finish(loc(754, "WalRcvFetchTimeLineHistoryFiles"));
            }

            timeline::writeTimeLineHistoryFile(tli, &content)?;

            if !transam_xlog::XLogArchivingAlways() {
                xlogarchive::XLogArchiveForceDone(&fname)?;
            } else {
                xlogarchive::XLogArchiveNotify(&fname)?;
            }
        }
    }
    Ok(())
}

fn wal_rcv_die(_code: i32, _arg: usize) {
    WalRcvDie().expect("WalRcvDie failed");
}

fn WalRcvDie() -> PgResult<()> {
    let startpointTLI = STARTPOINT_TLI.get();
    assert!(startpointTLI != 0);

    XLogWalRcvFlush(true, startpointTLI)?;

    with_walrcv(|d| {
        debug_assert!(matches!(
            d.walRcvState,
            WalRcvState::Streaming
                | WalRcvState::Restarting
                | WalRcvState::Starting
                | WalRcvState::Waiting
                | WalRcvState::Stopping
        ));
        debug_assert!(d.pid == g::MyProcPid());
        d.walRcvState = WalRcvState::Stopped;
        d.pid = 0;
        d.procno = INVALID_PROC_NUMBER;
        d.ready_to_display = false;
    });

    walreceiverfuncs::wal_rcv_stopped_cv_broadcast();

    if let Some(conn) = CONN.with(|c| c.borrow_mut().take()) {
        client::disconnect(conn);
    }

    xlogrecovery_seams::wakeup_recovery::call();
    Ok(())
}

fn XLogWalRcvProcessMsg(msg_type: u8, buf: &[u8], tli: TimeLineID) -> PgResult<()> {
    match msg_type {
        b'w' => {
            let hdrlen = 8 + 8 + 8;
            if buf.len() < hdrlen {
                return ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg_internal("invalid WAL message received from primary")
                    .finish(loc(837, "XLogWalRcvProcessMsg"));
            }
            let data_start = be_u64(&buf[0..8]);
            let wal_end = be_u64(&buf[8..16]);
            let send_time = be_u64(&buf[16..24]) as TimestampTz;
            ProcessWalSndrMessage(wal_end, send_time);
            XLogWalRcvWrite(&buf[hdrlen..], data_start, tli)
        }
        b'k' => {
            let hdrlen = 8 + 8 + 1;
            if buf.len() != hdrlen {
                return ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg_internal("invalid keepalive message received from primary")
                    .finish(loc(861, "XLogWalRcvProcessMsg"));
            }
            let wal_end = be_u64(&buf[0..8]);
            let send_time = be_u64(&buf[8..16]) as TimestampTz;
            let reply_requested = buf[16];
            ProcessWalSndrMessage(wal_end, send_time);
            if reply_requested != 0 {
                XLogWalRcvSendReply(true, false)?;
            }
            Ok(())
        }
        other => ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg_internal(format!("invalid replication message type {}", other as i32))
            .finish(loc(881, "XLogWalRcvProcessMsg")),
    }
}

fn be_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

fn XLogWalRcvWrite(buf: &[u8], recptr_in: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    let wal_segment_size = transam_xlog::wal_segment_size();
    assert!(tli != 0);

    let mut recptr = recptr_in;
    let mut off = 0usize;
    while off < buf.len() {
        let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
        if recv_file >= 0
            && transam_xlog::XLByteToSeg(recptr, wal_segment_size) != recv_seg_no
        {
            XLogWalRcvClose(recptr, tli)?;
        }

        if with_state(|s| s.recvFile) < 0 {
            let seg = transam_xlog::XLByteToSeg(recptr, wal_segment_size);
            with_state(|s| s.recvSegNo = seg);
            let fd = transam_xlog::write::XLogFileInit(seg, tli)?;
            with_state(|s| {
                s.recvFile = fd;
                s.recvFileTLI = tli;
            });
        }

        let startoff = transam_xlog::XLogSegmentOffset(recptr, wal_segment_size) as usize;
        let remaining = buf.len() - off;
        let segbytes = remaining.min(wal_segment_size as usize - startoff);

        let start_ns = pgstat::io::pgstat_prepare_io_time(
            guc_tables::vars::track_wal_io_timing.read(),
        );
        waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_WAL_WRITE);
        let recv_file = with_state(|s| s.recvFile);
        let byteswritten = unsafe {
            libc::pwrite(
                recv_file,
                buf[off..off + segbytes].as_ptr().cast(),
                segbytes,
                startoff as libc::off_t,
            )
        };
        waitevent_seams::pgstat_report_wait_end::call();

        if byteswritten <= 0 {
            let (tli_f, segno) = with_state(|s| (s.recvFileTLI, s.recvSegNo));
            let xlogfname = transam_xlog::XLogFileName(tli_f, segno, wal_segment_size);
            let e = std::io::Error::last_os_error();
            return ereport(PANIC)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not write to WAL segment {xlogfname} at offset {startoff}, length {segbytes}: {e}"
                ))
                .finish(loc(950, "XLogWalRcvWrite"));
        }
        pgstat::io::pgstat_count_io_op_time(
            pgstat::io::IOObject::Wal,
            pgstat::io::IOContext::IOCONTEXT_NORMAL,
            pgstat::io::IOOp::Write,
            start_ns,
            1,
            byteswritten as u64,
        );

        recptr += byteswritten as u64;
        off += byteswritten as usize;
        with_state(|s| s.write = recptr);
    }

    walreceiverfuncs::set_written_upto(with_state(|s| s.write));

    // Close a fully-written final segment now, so its archive notification
    // file is created promptly.
    let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
    if recv_file >= 0 && transam_xlog::XLByteToSeg(recptr, wal_segment_size) != recv_seg_no {
        XLogWalRcvClose(recptr, tli)?;
    }
    Ok(())
}

fn XLogWalRcvFlush(dying: bool, tli: TimeLineID) -> PgResult<()> {
    assert!(tli != 0);
    let (flush, write) = with_state(|s| (s.flush, s.write));
    if flush < write {
        let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
        transam_xlog::write::issue_xlog_fsync(recv_file, recv_seg_no, tli)?;

        with_state(|s| s.flush = s.write);
        let new_flush = write;

        with_walrcv(|d| {
            if d.flushedUpto < new_flush {
                d.latestChunkStart = d.flushedUpto;
                d.flushedUpto = new_flush;
                d.receivedTLI = tli;
            }
        });

        xlogrecovery_seams::wakeup_recovery::call();
        // AllowCascadeReplication().
        if guc_tables::vars::EnableHotStandby.read()
            && guc_tables::vars::max_wal_senders.read() > 0
            && walsender_seams::wal_snd_wakeup::is_installed()
        {
            walsender_seams::wal_snd_wakeup::call(true, false);
        }

        if guc_tables::vars::update_process_title.read() {
            ps_status_seams::set_ps_display::call(&format!("streaming {}", lsn_fmt(write)));
        }

        if !dying {
            XLogWalRcvSendReply(false, false)?;
            XLogWalRcvSendHSFeedback(false)?;
        }
    }
    Ok(())
}

fn XLogWalRcvClose(recptr: XLogRecPtr, tli: TimeLineID) -> PgResult<()> {
    let wal_segment_size = transam_xlog::wal_segment_size();
    let (recv_file, recv_seg_no) = with_state(|s| (s.recvFile, s.recvSegNo));
    assert!(
        recv_file >= 0 && transam_xlog::XLByteToSeg(recptr, wal_segment_size) != recv_seg_no
    );
    assert!(tli != 0);

    XLogWalRcvFlush(false, tli)?;

    let (recv_file_tli, recv_seg_no) = with_state(|s| (s.recvFileTLI, s.recvSegNo));
    let xlogfname = transam_xlog::XLogFileName(recv_file_tli, recv_seg_no, wal_segment_size);

    let recv_file = with_state(|s| s.recvFile);
    if unsafe { libc::close(recv_file) } != 0 {
        return ereport(PANIC)
            .errcode_for_file_access()
            .errmsg(format!("could not close WAL segment {xlogfname}"))
            .finish(loc(1063, "XLogWalRcvClose"));
    }

    if !transam_xlog::XLogArchivingAlways() {
        xlogarchive::XLogArchiveForceDone(&xlogfname)?;
    } else {
        xlogarchive::XLogArchiveNotify(&xlogfname)?;
    }

    with_state(|s| s.recvFile = -1);
    Ok(())
}

fn XLogWalRcvSendReply(force: bool, request_reply: bool) -> PgResult<()> {
    if !force && wal_receiver_status_interval() <= 0 {
        return Ok(());
    }

    let now = get_ts();
    let (write, flush, r_write, r_flush, r_wakeup) = with_state(|s| {
        (s.write, s.flush, s.reply_writePtr, s.reply_flushPtr, s.wakeup[WAKEUP_REPLY])
    });
    if !force && r_write == write && r_flush == flush && now < r_wakeup {
        return Ok(());
    }

    WalRcvComputeNextWakeup(WAKEUP_REPLY, now);

    let (apply, _tli) = xlogrecovery_seams::get_xlog_replay_rec_ptr::call();
    with_state(|s| {
        s.reply_writePtr = write;
        s.reply_flushPtr = flush;
    });

    let mut reply = Vec::with_capacity(34);
    reply.push(b'r');
    reply.extend_from_slice(&write.to_be_bytes());
    reply.extend_from_slice(&flush.to_be_bytes());
    reply.extend_from_slice(&apply.to_be_bytes());
    reply.extend_from_slice(&get_ts().to_be_bytes());
    reply.push(u8::from(request_reply));

    let _ = elog(
        DEBUG2,
        format!(
            "sending write {} flush {} apply {}{}",
            lsn_fmt(write),
            lsn_fmt(flush),
            lsn_fmt(apply),
            if request_reply { " (reply requested)" } else { "" }
        ),
    );

    with_conn(|c| client::send(c, &reply))
}

fn XLogWalRcvSendHSFeedback(immed: bool) -> PgResult<()> {
    let primary_has_standby_xmin = with_state(|s| s.primary_has_standby_xmin);

    if (wal_receiver_status_interval() <= 0 || !hot_standby_feedback())
        && !primary_has_standby_xmin
    {
        return Ok(());
    }

    let now = get_ts();
    if !immed && now < with_state(|s| s.wakeup[WAKEUP_HSFEEDBACK]) {
        return Ok(());
    }

    WalRcvComputeNextWakeup(WAKEUP_HSFEEDBACK, now);

    if !xlogrecovery_seams::hot_standby_active::call() {
        return Ok(());
    }

    let (xmin, catalog_xmin) = if hot_standby_feedback() {
        procarray::GetReplicationHorizons()?
    } else {
        (InvalidTransactionId, InvalidTransactionId)
    };

    let next_full_xid = varsup::ReadNextFullTransactionId()?.value;
    let next_xid = next_full_xid as TransactionId;
    let mut xmin_epoch = (next_full_xid >> 32) as u32;
    let mut catalog_xmin_epoch = xmin_epoch;
    if next_xid < xmin {
        xmin_epoch = xmin_epoch.wrapping_sub(1);
    }
    if next_xid < catalog_xmin {
        catalog_xmin_epoch = catalog_xmin_epoch.wrapping_sub(1);
    }

    let _ = elog(
        DEBUG2,
        format!(
            "sending hot standby feedback xmin {xmin} epoch {xmin_epoch} catalog_xmin {catalog_xmin} catalog_xmin_epoch {catalog_xmin_epoch}"
        ),
    );

    let mut msg = Vec::with_capacity(25);
    msg.push(b'h');
    msg.extend_from_slice(&get_ts().to_be_bytes());
    msg.extend_from_slice(&xmin.to_be_bytes());
    msg.extend_from_slice(&xmin_epoch.to_be_bytes());
    msg.extend_from_slice(&catalog_xmin.to_be_bytes());
    msg.extend_from_slice(&catalog_xmin_epoch.to_be_bytes());
    with_conn(|c| client::send(c, &msg))?;

    with_state(|s| {
        s.primary_has_standby_xmin =
            xmin != InvalidTransactionId || catalog_xmin != InvalidTransactionId
    });
    Ok(())
}

fn ProcessWalSndrMessage(wal_end: XLogRecPtr, send_time: TimestampTz) {
    let last_msg_receipt_time = get_ts();
    with_walrcv(|d| {
        if d.latestWalEnd < wal_end {
            d.latestWalEndTime = send_time;
        }
        d.latestWalEnd = wal_end;
        d.lastMsgSendTime = send_time;
        d.lastMsgReceiptTime = last_msg_receipt_time;
    });
}

fn WalRcvComputeNextWakeup(reason: usize, now: TimestampTz) {
    let v = match reason {
        WAKEUP_TERMINATE => {
            if wal_receiver_timeout() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                now + wal_receiver_timeout() as i64 * 1000
            }
        }
        WAKEUP_PING => {
            if wal_receiver_timeout() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                now + (wal_receiver_timeout() / 2) as i64 * 1000
            }
        }
        WAKEUP_HSFEEDBACK => {
            if !hot_standby_feedback() || wal_receiver_status_interval() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                now + wal_receiver_status_interval() as i64 * 1_000_000
            }
        }
        WAKEUP_REPLY => {
            if wal_receiver_status_interval() <= 0 {
                TIMESTAMP_INFINITY
            } else {
                now + wal_receiver_status_interval() as i64 * 1_000_000
            }
        }
        _ => unreachable!("bad wakeup reason"),
    };
    with_state(|s| s.wakeup[reason] = v);
}

/// WalRcvForceReply: called by the startup process when interesting records
/// were applied.
pub fn WalRcvForceReply() {
    walreceiverfuncs::set_force_reply();
    let procno = with_walrcv(|d| d.procno);
    if procno != INVALID_PROC_NUMBER {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(procno));
    }
}

pub fn wal_rcv_get_state_string(state: WalRcvState) -> &'static str {
    match state {
        WalRcvState::Stopped => "stopped",
        WalRcvState::Starting => "starting",
        WalRcvState::Streaming => "streaming",
        WalRcvState::Waiting => "waiting",
        WalRcvState::Restarting => "restarting",
        WalRcvState::Stopping => "stopping",
    }
}

pub fn init_seams() {
    walreceiverfuncs_seams::wal_rcv_force_reply::set(WalRcvForceReply);
}

const _: () = assert!(NAMEDATALEN == 64);
