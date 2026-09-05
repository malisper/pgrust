// walreceiverfuncs.c: the startup-process side of walreceiver control. Owns
// the WalRcv control block (C: ShmemInitStruct + spinlock; here a process
// static: Mutex for the spinlock-guarded fields, atomics for the lock-free
// words, a CV for walRcvStoppedCV).
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use pgsync::{Mutex, MutexGuard, OnceLock};

use condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariablePrepareToSleep, ConditionVariableSleep,
};
use types_core::{
    pg_time_t, InvalidXLogRecPtr, ProcNumber, TimeLineID, TimestampTz, XLogRecPtr,
    INVALID_PROC_NUMBER,
};
use types_error::PgResult;

pub const MAXCONNINFO: usize = 1024;
pub const NAMEDATALEN: usize = 64;
const WALRCV_STARTUP_TIMEOUT: pg_time_t = 10;
const WAIT_EVENT_WAL_RECEIVER_EXIT: u32 = 0x0800_0000 | 53;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WalRcvState {
    Stopped,
    Starting,
    Streaming,
    Waiting,
    Restarting,
    Stopping,
}

pub struct WalRcvData {
    pub pid: i32,
    pub walRcvState: WalRcvState,
    pub procno: ProcNumber,
    pub startTime: pg_time_t,
    pub receiveStart: XLogRecPtr,
    pub receiveStartTLI: TimeLineID,
    pub flushedUpto: XLogRecPtr,
    pub receivedTLI: TimeLineID,
    pub latestChunkStart: XLogRecPtr,
    pub latestWalEnd: XLogRecPtr,
    pub latestWalEndTime: TimestampTz,
    pub lastMsgSendTime: TimestampTz,
    pub lastMsgReceiptTime: TimestampTz,
    pub conninfo: String,
    pub slotname: String,
    pub sender_host: String,
    pub sender_port: i32,
    pub is_temp_slot: bool,
    pub ready_to_display: bool,
}

impl WalRcvData {
    fn new() -> Self {
        WalRcvData {
            pid: 0,
            walRcvState: WalRcvState::Stopped,
            procno: INVALID_PROC_NUMBER,
            startTime: 0,
            receiveStart: InvalidXLogRecPtr,
            receiveStartTLI: 0,
            flushedUpto: InvalidXLogRecPtr,
            receivedTLI: 0,
            latestChunkStart: InvalidXLogRecPtr,
            latestWalEnd: InvalidXLogRecPtr,
            latestWalEndTime: 0,
            lastMsgSendTime: 0,
            lastMsgReceiptTime: 0,
            conninfo: String::new(),
            slotname: String::new(),
            sender_host: String::new(),
            sender_port: 0,
            is_temp_slot: false,
            ready_to_display: false,
        }
    }
}

struct WalRcvShared {
    data: Mutex<WalRcvData>,
    writtenUpto: AtomicU64,
    force_reply: AtomicBool,
    walRcvStoppedCV: ConditionVariable,
}

static WAL_RCV: OnceLock<WalRcvShared> = OnceLock::new();

fn shmem() -> &'static WalRcvShared {
    WAL_RCV.get().expect("WalRcv accessed before WalRcvShmemInit")
}

// sizeof(WalRcvData) (replication/walreceiver.h) at 18.6, x86-64 and
// aarch64 Linux alike: procno/pid/walRcvState (12) + ConditionVariable (12)
// + startTime..latestWalEndTime (8 x 10 with two TimeLineID pads = 80)
// + conninfo[MAXCONNINFO=1024] + sender_host[NI_MAXHOST=1025] + pad +
// sender_port (4) + slotname[NAMEDATALEN=64] + is_temp_slot + ready_to_display
// + slock_t mutex + pad to the 8-aligned pg_atomic_uint64 writtenUpto +
// sig_atomic_t force_reply + tail pad = 2248.
const C_SIZE_OF_WAL_RCV_DATA: usize = 2248;

/// WalRcvShmemSize (walreceiverfuncs.c:44): add_size(0, sizeof(WalRcvData)).
pub fn WalRcvShmemSize() -> usize {
    C_SIZE_OF_WAL_RCV_DATA
}

/// WalRcvShmemInit (walreceiverfuncs.c:55): ShmemInitStruct("Wal Receiver
/// Ctl", WalRcvShmemSize()) registers the block in the ShmemIndex, so
/// pg_shmem_allocations lists it; a fresh segment (!found) boots the control
/// block (STOPPED, procno INVALID_PROC_NUMBER, writtenUpto 0), a re-entry
/// leaves the live state alone.
pub fn WalRcvShmemInit() -> PgResult<()> {
    let (_raw, found) = shmem::ShmemInitStruct("Wal Receiver Ctl", WalRcvShmemSize())?;
    if !found {
        WAL_RCV
            .set(WalRcvShared {
                data: Mutex::new(WalRcvData::new()),
                writtenUpto: AtomicU64::new(0),
                force_reply: AtomicBool::new(false),
                walRcvStoppedCV: ConditionVariable::new(),
            })
            .unwrap_or_else(|_| panic!("WalRcvShmemInit: fresh segment but WalRcv already booted"));
    }
    Ok(())
}

pub fn WalRcvShmemResetAfterCrash() {
    if let Some(s) = WAL_RCV.get() {
        *lock_of(s) = WalRcvData::new();
        s.writtenUpto.store(0, SeqCst);
        s.force_reply.store(false, SeqCst);
        condition_variable::cv_reset_after_crash(&s.walRcvStoppedCV);
    }
}

fn lock_of(s: &'static WalRcvShared) -> MutexGuard<'static, WalRcvData> {
    s.data.lock().unwrap_or_else(|e| e.into_inner())
}

/// SpinLockAcquire(&WalRcv->mutex) .. SpinLockRelease bracket.
pub fn with_walrcv<R>(f: impl FnOnce(&mut WalRcvData) -> R) -> R {
    f(&mut lock_of(shmem()))
}

pub fn set_written_upto(v: XLogRecPtr) {
    shmem().writtenUpto.store(v, SeqCst);
}

pub fn get_written_upto() -> XLogRecPtr {
    shmem().writtenUpto.load(SeqCst)
}

pub fn set_force_reply() {
    shmem().force_reply.store(true, SeqCst);
}

pub fn take_force_reply() -> bool {
    shmem().force_reply.swap(false, SeqCst)
}

pub fn wal_rcv_stopped_cv_broadcast() {
    ConditionVariableBroadcast(&shmem().walRcvStoppedCV);
}

fn now_seconds() -> pg_time_t {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs() as pg_time_t,
        Err(e) => -(e.duration().as_secs() as pg_time_t),
    }
}

fn stop_if_startup_timed_out(state: &mut WalRcvState, start_time: pg_time_t) {
    if *state != WalRcvState::Starting {
        return;
    }
    if now_seconds() - start_time > WALRCV_STARTUP_TIMEOUT {
        let stopped = with_walrcv(|d| {
            if d.walRcvState == WalRcvState::Starting {
                d.walRcvState = WalRcvState::Stopped;
                *state = WalRcvState::Stopped;
                true
            } else {
                false
            }
        });
        if stopped {
            wal_rcv_stopped_cv_broadcast();
        }
    }
}

pub fn WalRcvRunning() -> bool {
    let (mut state, start_time) = with_walrcv(|d| (d.walRcvState, d.startTime));
    stop_if_startup_timed_out(&mut state, start_time);
    state != WalRcvState::Stopped
}

pub fn WalRcvStreaming() -> bool {
    let (mut state, start_time) = with_walrcv(|d| (d.walRcvState, d.startTime));
    stop_if_startup_timed_out(&mut state, start_time);
    matches!(
        state,
        WalRcvState::Streaming | WalRcvState::Starting | WalRcvState::Restarting
    )
}

pub fn ShutdownWalRcv() -> PgResult<()> {
    let mut walrcvpid = 0;
    let mut stopped = false;
    with_walrcv(|d| match d.walRcvState {
        WalRcvState::Stopped => {}
        WalRcvState::Starting => {
            d.walRcvState = WalRcvState::Stopped;
            stopped = true;
        }
        WalRcvState::Streaming | WalRcvState::Waiting | WalRcvState::Restarting => {
            d.walRcvState = WalRcvState::Stopping;
            walrcvpid = d.pid;
        }
        WalRcvState::Stopping => walrcvpid = d.pid,
    });

    if stopped {
        wal_rcv_stopped_cv_broadcast();
    }
    if walrcvpid != 0 {
        // kill(walrcvpid, SIGTERM), thread rendering. procsignal::signums,
        // not libc::SIG*: the wasi libc crate exposes no SIG* names.
        let _ = procsignal::SendThreadSignal(walrcvpid, procsignal::signums::SIGTERM);
    }

    let cv = &shmem().walRcvStoppedCV;
    ConditionVariablePrepareToSleep(cv);
    while WalRcvRunning() {
        ConditionVariableSleep(cv, WAIT_EVENT_WAL_RECEIVER_EXIT)?;
    }
    ConditionVariableCancelSleep();
    Ok(())
}

fn strlcpy_trunc(s: &str, size: usize) -> String {
    let mut n = s.len().min(size - 1);
    while n > 0 && !s.is_char_boundary(n) {
        n -= 1;
    }
    s[..n].to_string()
}

pub fn RequestXLogStreaming(
    tli: TimeLineID,
    mut recptr: XLogRecPtr,
    conninfo: &str,
    slotname: &str,
    create_temp_slot: bool,
) -> PgResult<()> {
    let mut launch = false;
    let now = now_seconds();
    let wal_segment_size = transam_xlog::wal_segment_size();

    // Always start at a segment boundary: a segment with no records in its
    // first half must not be created by streaming (archiving hazard).
    let off = transam_xlog::XLogSegmentOffset(recptr, wal_segment_size) as XLogRecPtr;
    if off != 0 {
        recptr -= off;
    }

    let walrcv_proc = with_walrcv(|d| {
        debug_assert!(matches!(d.walRcvState, WalRcvState::Stopped | WalRcvState::Waiting));

        if !slotname.is_empty() {
            d.slotname = strlcpy_trunc(slotname, NAMEDATALEN);
            d.is_temp_slot = false;
        } else {
            d.slotname = String::new();
            d.is_temp_slot = create_temp_slot;
        }

        // A WAITING walreceiver keeps its connection: only a fresh launch may
        // clobber the user-visible conninfo WalReceiverMain stored.
        // upstream b903d17927ee (18.6): Avoid exposing WAL receiver raw conninfo during timeline jumps
        if d.walRcvState == WalRcvState::Stopped {
            launch = true;
            d.walRcvState = WalRcvState::Starting;
            d.conninfo = strlcpy_trunc(conninfo, MAXCONNINFO);
        } else {
            d.walRcvState = WalRcvState::Restarting;
        }
        d.startTime = now;

        if d.receiveStart == 0 || d.receivedTLI != tli {
            d.flushedUpto = recptr;
            d.receivedTLI = tli;
            d.latestChunkStart = recptr;
        }
        d.receiveStart = recptr;
        d.receiveStartTLI = tli;
        d.procno
    });

    if launch {
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_START_WALRECEIVER);
    } else if walrcv_proc != INVALID_PROC_NUMBER {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(walrcv_proc));
    }
    Ok(())
}

pub fn GetWalRcvFlushRecPtr() -> (XLogRecPtr, XLogRecPtr, TimeLineID) {
    with_walrcv(|d| (d.flushedUpto, d.latestChunkStart, d.receivedTLI))
}

pub fn GetWalRcvWriteRecPtr() -> XLogRecPtr {
    get_written_upto()
}

pub fn GetReplicationApplyDelay() -> i32 {
    let receive_ptr = with_walrcv(|d| d.flushedUpto);
    let (replay_ptr, _tli) = xlogrecovery::GetXLogReplayRecPtr();
    if receive_ptr == replay_ptr {
        return 0;
    }
    let chunk_replay_start_time = xlogrecovery::targets::GetCurrentChunkReplayStartTime();
    if chunk_replay_start_time == 0 {
        return -1;
    }
    adt_timestamp::TimestampDifferenceMilliseconds(
        chunk_replay_start_time,
        timestamp_seams::get_current_timestamp::call(),
    ) as i32
}

pub fn GetReplicationTransferLatency() -> i32 {
    let (send, receipt) = with_walrcv(|d| (d.lastMsgSendTime, d.lastMsgReceiptTime));
    adt_timestamp::TimestampDifferenceMilliseconds(send, receipt) as i32
}

fn wal_rcv_state_string(state: WalRcvState) -> &'static str {
    match state {
        WalRcvState::Stopped => "stopped",
        WalRcvState::Starting => "starting",
        WalRcvState::Streaming => "streaming",
        WalRcvState::Waiting => "waiting",
        WalRcvState::Restarting => "restarting",
        WalRcvState::Stopping => "stopping",
    }
}

fn pg_stat_wal_receiver_snapshot() -> Option<walreceiverfuncs_seams::WalRcvStatSnapshot> {
    let snap = with_walrcv(|d| {
        if d.pid == 0 || !d.ready_to_display {
            return None;
        }
        Some(walreceiverfuncs_seams::WalRcvStatSnapshot {
            pid: d.pid,
            state: wal_rcv_state_string(d.walRcvState),
            receive_start_lsn: d.receiveStart,
            receive_start_tli: d.receiveStartTLI,
            written_lsn: 0,
            flushed_lsn: d.flushedUpto,
            received_tli: d.receivedTLI,
            last_send_time: d.lastMsgSendTime,
            last_receipt_time: d.lastMsgReceiptTime,
            latest_end_lsn: d.latestWalEnd,
            latest_end_time: d.latestWalEndTime,
            slotname: d.slotname.clone(),
            sender_host: d.sender_host.clone(),
            sender_port: d.sender_port,
            conninfo: d.conninfo.clone(),
        })
    });
    snap.map(|mut s| {
        // C reads writtenUpto without the spinlock.
        s.written_lsn = get_written_upto();
        s
    })
}

pub fn init_seams() {
    walreceiverfuncs_seams::wal_rcv_streaming::set(WalRcvStreaming);
    walreceiverfuncs_seams::wal_rcv_running::set(WalRcvRunning);
    walreceiverfuncs_seams::shutdown_wal_rcv::set(ShutdownWalRcv);
    walreceiverfuncs_seams::request_xlog_streaming::set(RequestXLogStreaming);
    walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::set(GetWalRcvFlushRecPtr);
    walreceiverfuncs_seams::pg_stat_wal_receiver_snapshot::set(pg_stat_wal_receiver_snapshot);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_once() {
        static ONCE: pgsync::Once = pgsync::Once::new();
        ONCE.call_once(|| WalRcvShmemInit().unwrap());
    }

    // Serializes the tests that change walRcvState in the shared control block.
    static STATE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // audit-18.6 b190: walreceiverfuncs.c:55 WalRcvShmemInit registers the
    // control block as ShmemInitStruct("Wal Receiver Ctl", sizeof(WalRcvData))
    // — 2248 bytes at 18.6 (x86-64 and aarch64 Linux alike) — so a re-entry
    // finds it and pg_shmem_allocations lists it.
    #[test]
    fn shmem_index_lists_wal_receiver_ctl() {
        init_once();
        assert_eq!(WalRcvShmemSize(), 2248, "walreceiverfuncs.c:44 sizeof(WalRcvData)");
        let (_, found) = shmem::ShmemInitStruct("Wal Receiver Ctl", 2248).unwrap();
        assert!(
            found,
            "WalRcvShmemInit must register \"Wal Receiver Ctl\" (2248 bytes) in the ShmemIndex"
        );
    }

    // audit-18.6 b190: the ShutdownWalRcv seam. C's ConditionVariableSleep
    // runs CHECK_FOR_INTERRUPTS, so a SIGTERM/SIGINT arriving while the
    // startup process waits for the walreceiver to exit unwinds as a
    // FATAL/ERROR through XLogShutdownWalRcv; the seam must surface that
    // error, never panic. The harness is the condition_variable crate's own
    // test recipe: one owned latch, the real waiteventset stack, and a
    // CHECK_FOR_INTERRUPTS stand-in that raises once when armed.
    static INTERRUPT_ARMED: AtomicBool = AtomicBool::new(false);

    fn cv_harness() {
        use init_small::globals as g;
        use types_storage::latch::LatchHandle;
        use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;
        static SETUP: pgsync::Once = pgsync::Once::new();
        SETUP.call_once(|| {
            s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
            s_lock_seams::finish_spin_delay::set(|_| {});
            shmem_seams::mul_size::set(|a, b| Ok(a * b));
            shmem_seams::add_size::set(|a, b| Ok(a + b));
            ipc_seams::on_shmem_exit::set(|_, _| {});
            pg_sema_seams::pg_semaphore_create::set(|_| {});
            waitevent_seams::pgstat_report_wait_start::set(|_| {});
            waitevent_seams::pgstat_report_wait_end::set(|| {});
            postgres_seams::check_for_interrupts::set(|| {
                if INTERRUPT_ARMED.swap(false, SeqCst) {
                    // ProcessInterrupts' die arm (postgres.c).
                    Err(Box::new(
                        types_error::PgError::error(
                            "terminating connection due to administrator command",
                        )
                        .with_sqlstate(types_error::ERRCODE_ADMIN_SHUTDOWN),
                    ))
                } else {
                    Ok(())
                }
            });
            lmgr_proc_seams::proc_latch::set(|p| &lmgr_proc::GetPGProcByNumber(p).procLatch);
            g::SetIsUnderPostmaster(false);
            g::SetMaxConnections(4);
            g::set_max_worker_processes(2);
            g::SetMaxBackends(4 + 3 + 2 + 2 + NUM_SPECIAL_WORKER_PROCS);
            lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
                autovacuum_worker_slots: 3,
                max_wal_senders: 2,
                max_prepared_xacts: 2,
                fastpath_lock_groups_per_backend: 1,
            });
            waiteventset::init_seams();
            latch::init_seams();
            condition_variable::init_seams();
            init_seams();
            // Become backend 0 (the only thread that ever sleeps here).
            g::SetMyProcNumber(0);
            g::SetMyProcPid(7900);
            waiteventset::InitializeWaitEventSupport().unwrap();
            let h = LatchHandle::proc(0);
            lmgr_proc::GetPGProcByNumber(0).procLatch.owner_pid.store(0, SeqCst);
            latch::OwnLatch(h).unwrap();
            g::SetMyLatch(Some(h));
            latch::InitializeLatchWaitSet().unwrap();
        });
    }

    // The seam's return shape is what this witness adjudicates: C surfaces
    // the interrupt as an error (PgResult::Err); a unit return has no error
    // surface at all.
    trait SeamOutcome {
        fn surfaced_error(self) -> bool;
    }
    impl SeamOutcome for () {
        fn surfaced_error(self) -> bool {
            false
        }
    }
    impl SeamOutcome for PgResult<()> {
        fn surfaced_error(self) -> bool {
            self.is_err()
        }
    }

    #[test]
    fn shutdown_wal_rcv_seam_surfaces_the_sleep_interrupt() {
        init_once();
        cv_harness();
        let _g = STATE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // A streaming walreceiver with no process to signal (pid 0): C sets
        // STOPPING and sleeps on walRcvStoppedCV until WalRcvRunning() clears.
        with_walrcv(|d| {
            *d = WalRcvData::new();
            d.walRcvState = WalRcvState::Streaming;
            d.pid = 0;
        });
        INTERRUPT_ARMED.store(true, SeqCst);
        // The latch is already set, so the first WaitLatch returns at once and
        // the sleep reaches its CHECK_FOR_INTERRUPTS, which raises.
        latch::SetLatch(init_small::globals::MyLatch().unwrap());
        let outcome = std::panic::catch_unwind(|| {
            walreceiverfuncs_seams::shutdown_wal_rcv::call().surfaced_error()
        });
        ConditionVariableCancelSleep();
        INTERRUPT_ARMED.store(false, SeqCst);
        with_walrcv(|d| *d = WalRcvData::new());
        match outcome {
            Ok(true) => {}
            Ok(false) => panic!("shutdown_wal_rcv seam swallowed the interrupt error"),
            Err(_) => panic!(
                "shutdown_wal_rcv seam panicked instead of returning the interrupt error \
                 (C: ConditionVariableSleep's CHECK_FOR_INTERRUPTS unwinds through ShutdownWalRcv)"
            ),
        }
    }

    #[test]
    fn stopped_by_default_and_flush_ptr_tracks() {
        init_once();
        let _g = STATE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!WalRcvRunning());
        assert!(!WalRcvStreaming());
        with_walrcv(|d| {
            d.flushedUpto = 0x1_0000;
            d.latestChunkStart = 0x8000;
            d.receivedTLI = 3;
        });
        assert_eq!(GetWalRcvFlushRecPtr(), (0x1_0000, 0x8000, 3));
        set_written_upto(42);
        assert_eq!(GetWalRcvWriteRecPtr(), 42);
        with_walrcv(|d| *d = WalRcvData::new());
        set_written_upto(0);
    }

    #[test]
    fn force_reply_latches() {
        init_once();
        assert!(!take_force_reply());
        set_force_reply();
        assert!(take_force_reply());
        assert!(!take_force_reply());
    }

    #[test]
    fn stat_snapshot_none_until_ready() {
        init_once();
        assert!(pg_stat_wal_receiver_snapshot().is_none());
    }

    // upstream b903d17927ee (18.6): a WAITING walreceiver re-pointed at a new
    // timeline keeps the displayable conninfo (pg_stat_wal_receiver shows it
    // while ready_to_display is set); only a launch from STOPPED copies the raw one.
    #[test]
    fn request_streaming_keeps_display_conninfo_across_timeline_jump() {
        init_once();
        let _g = STATE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let raw = "host=primary user=rep password=dont_show_me";
        let shown = "host=primary user=rep";

        with_walrcv(|d| {
            *d = WalRcvData::new();
            d.walRcvState = WalRcvState::Waiting;
            d.conninfo = shown.to_string();
            d.ready_to_display = true;
        });
        RequestXLogStreaming(2, 0x0100_0000, raw, "", false).unwrap();
        with_walrcv(|d| {
            assert_eq!(d.walRcvState, WalRcvState::Restarting);
            assert_eq!(d.conninfo, shown, "timeline jump must not expose the raw conninfo");
            assert_eq!((d.receiveStart, d.receiveStartTLI), (0x0100_0000, 2));
            *d = WalRcvData::new();
        });

        RequestXLogStreaming(1, 0x0100_0000, raw, "", false).unwrap();
        with_walrcv(|d| {
            assert_eq!(d.walRcvState, WalRcvState::Starting);
            assert_eq!(d.conninfo, raw, "a fresh launch takes the configured conninfo");
            *d = WalRcvData::new();
        });
    }
}
