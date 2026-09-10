// walsender.c — WAL sender (PG 18.3). Increments 1-3 of the replication port:
// walsender identity flags, exec_replication_command dispatch, IDENTIFY_SYSTEM,
// SHOW, slot commands, TIMELINE_HISTORY, and physical START_REPLICATION live WAL
// streaming (CopyBoth + WalSndLoop + XLogSendPhysical), BASE_BACKUP (inc 5,
// via the walsender_seams::base_backup seam) and logical START_REPLICATION
// (inc 6), and UPLOAD_MANIFEST (incremental backup Stage 3): the CopyIn
// manifest receive loop feeding basebackup_incremental's parser, with the
// resulting IncrementalBackupInfo held in the SESSION-owned slot store
// (Q2 ruling — see UPLOADED_MANIFESTS).
#![allow(non_snake_case)]

pub mod replies;
pub mod logical_stream;
mod lag;
mod streaming;
pub mod wakeup;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

use condition_variable::ConditionVariable;
use datum::Datum;
use elog::errno::current_errno;
use elog::ereport;
use repl_gram::{
    AlterReplicationSlotCmd, CreateReplicationSlotCmd, DropReplicationSlotCmd, ReadReplicationSlotCmd,
    ReplCommand, ReplOptionArg, ReplicationKind, TimeLineHistoryCmd,
};
use types_core::{
    InvalidOid, InvalidXLogRecPtr, TimeLineID, TimestampTz, XLogRecPtr, INT8OID, TEXTOID,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_IN_FAILED_SQL_TRANSACTION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERROR, LOG,
};

const SRC: &str = "src/backend/replication/walsender.c";

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

// am_walsender/am_db_walsender live in walsender_seams (readable as `false`
// without this crate linked). The rest of walsender.c's globals live here;
// one backend = one thread (init_small globals pattern).
// log_replication_commands is session-settable: TLS, not a Relaxed atomic.
pub use walsender_seams::{am_db_walsender, am_walsender, set_walsender_flags};

thread_local! {
    static LOG_REPLICATION_COMMANDS: Cell<bool> = const { Cell::new(false) };
    // wal_sender_timeout GUC backing (session-settable, PGC_USERSET). Default is
    // the boot value (60s) so a read before GUC assignment is sane.
    static WAL_SENDER_TIMEOUT: Cell<i32> = const { Cell::new(60 * 1000) };
    pub(crate) static AM_CASCADING_WALSENDER: Cell<bool> = const { Cell::new(false) };
    pub(crate) static GOT_STOPPING: Cell<bool> = const { Cell::new(false) };
    pub(crate) static GOT_SIGUSR2: Cell<bool> = const { Cell::new(false) };
    pub(crate) static REPLICATION_ACTIVE: Cell<bool> = const { Cell::new(false) };
    // MyWalSnd as an index into WalSndCtl().walsnds; -1 = NULL.
    static MY_WAL_SND: Cell<i32> = const { Cell::new(-1) };

    // Per-backend streaming state (walsender.c file-static globals). One
    // backend = one thread (init_small globals pattern).
    pub(crate) static SENT_PTR: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    pub(crate) static SEND_TIME_LINE: Cell<TimeLineID> = const { Cell::new(0) };
    pub(crate) static SEND_TIME_LINE_IS_HISTORIC: Cell<bool> = const { Cell::new(false) };
    pub(crate) static SEND_TIME_LINE_VALID_UPTO: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    pub(crate) static SEND_TIME_LINE_NEXT_TLI: Cell<TimeLineID> = const { Cell::new(0) };
    pub(crate) static STREAMING_DONE_SENDING: Cell<bool> = const { Cell::new(false) };
    pub(crate) static STREAMING_DONE_RECEIVING: Cell<bool> = const { Cell::new(false) };
    pub(crate) static WAL_SND_CAUGHT_UP: Cell<bool> = const { Cell::new(false) };
    pub(crate) static WAITING_FOR_PING_RESPONSE: Cell<bool> = const { Cell::new(false) };
    pub(crate) static LAST_REPLY_TIMESTAMP: Cell<TimestampTz> = const { Cell::new(0) };
    pub(crate) static LAST_PROCESSING: Cell<TimestampTz> = const { Cell::new(0) };
    // upstream 98e96e579b91 (18.4): Fix premature NULL lag reporting in pg_stat_replication
    pub(crate) static PREV_WRITE_PTR: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    pub(crate) static PREV_FLUSH_PTR: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    pub(crate) static PREV_APPLY_PTR: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    // output_message StringInfo — reused across sends within a backend.
    pub(crate) static OUTPUT_MESSAGE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalSndState {
    Startup = 0,
    Backup,
    Catchup,
    Streaming,
    Stopping,
}

// WalSnd spinlock-guarded body; the per-slot Mutex is the C `mutex` field
// (thread-native decision 3, replication-port-plan.md).
pub struct WalSnd {
    pub pid: i32,
    pub state: WalSndState,
    pub sentPtr: XLogRecPtr,
    pub needreload: bool,
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,
    pub writeLag: i64,
    pub flushLag: i64,
    pub applyLag: i64,
    pub kind: ReplicationKind,
    pub sync_standby_priority: i32,
    pub replyTime: TimestampTz,
}

const fn walsnd_empty() -> WalSnd {
    WalSnd {
        pid: 0,
        state: WalSndState::Startup,
        sentPtr: InvalidXLogRecPtr,
        needreload: false,
        write: InvalidXLogRecPtr,
        flush: InvalidXLogRecPtr,
        apply: InvalidXLogRecPtr,
        writeLag: -1,
        flushLag: -1,
        applyLag: -1,
        kind: ReplicationKind::REPLICATION_KIND_PHYSICAL,
        sync_standby_priority: 0,
        replyTime: 0,
    }
}

// C sizes (walsender_private.h, LP64): offsetof(WalSndCtlData, walsnds) =
// 3 dlist_head (48) + 3 XLogRecPtr (24) + bits8 + 3 ConditionVariable (12
// each, int-aligned) = 112, MAXALIGN-padded to the WalSnd array; sizeof(WalSnd)
// = pid_t + WalSndState + sentPtr + needreload + write/flush/apply + three
// TimeOffsets + sync_standby_priority + slock_t + replyTime + ReplicationKind
// = 96.
const C_OFFSETOF_WAL_SND_CTL_DATA_WALSNDS: usize = 112;
const C_SIZEOF_WAL_SND: usize = 96;

/// WalSndShmemSize (walsender.c:3690-3699): the WalSndCtlData header plus
/// max_wal_senders WalSnd slots. The GUC value comes from the caller, as with
/// lmgr_proc's ProcGlobalConfig.
pub fn WalSndShmemSize(max_wal_senders: i32) -> PgResult<usize> {
    let mut size: usize = 0;
    size = mcx::add_size(size, C_OFFSETOF_WAL_SND_CTL_DATA_WALSNDS)?;
    size = mcx::add_size(size, mcx::mul_size(max_wal_senders as usize, C_SIZEOF_WAL_SND)?)?;
    Ok(size)
}

// WalSndCtlData (walsender_private.h). The header lives in the
// ShmemInitStruct("Wal Sender Ctl") arena (WalSndShmemInit); the per-slot
// WalSnd bodies hang off it as a boxed slice (the C flexible array member,
// with the slot spinlock as a per-slot Mutex). The per-kind wakeup CVs are
// broadcast by the WAL flush/replay paths via the wal_snd_wakeup seam.
pub struct WalSndCtlData {
    pub walsnds: Box<[Mutex<WalSnd>]>,
    pub wal_flush_cv: ConditionVariable,
    pub wal_replay_cv: ConditionVariable,
    pub wal_confirm_rcv_cv: ConditionVariable,
    // SyncRep state (walsender_private.h). All [SyncRepLock] except
    // sync_standbys_status, which is read lock-free (see SyncRepWaitForLSN)
    // and written under the lock.
    pub sync_rep_queue: [types_storage::storage::SyncCell<types_storage::storage::proclist_head>;
        NUM_SYNC_REP_WAIT_MODE],
    pub sync_rep_lsn: [std::sync::atomic::AtomicU64; NUM_SYNC_REP_WAIT_MODE],
    pub sync_standbys_status: std::sync::atomic::AtomicU32,
}

// syncrep.h wait-mode slots in WalSndCtl.lsn[] / SyncRepQueue[].
pub const NUM_SYNC_REP_WAIT_MODE: usize = 3;

// WalSndCtl->sync_standbys_status bits (walsender_private.h).
pub const SYNC_STANDBY_INIT: u32 = 1 << 0;
pub const SYNC_STANDBY_DEFINED: u32 = 1 << 1;

// The published control block: C's `WalSndCtlData *WalSndCtl` pointer — the
// ShmemInitStruct("Wal Sender Ctl") allocation (walsender.c:3708-3709),
// leaked for the cluster lifetime like C shmem — plus the pgrust-only
// UPLOAD_MANIFEST session store, which is absent from C's shared
// WalSndCtlData and so lives beside the block, not inside the C-sized arena
// (one cell per walsender slot).
//
// Q2 RULING (binding): C keeps the parsed manifest in a per-PROCESS
// static (walsender.c:152 `uploaded_manifest`, parented under
// CacheMemoryContext). pgrust is thread-per-backend under the
// "no state belongs to a thread" ruling: this state — potentially 100s
// of MB — must be SESSION-owned, never a per-thread static or
// thread_local, because sessions may migrate across pool threads and a
// second session hosted on the same thread must never see (or pay for)
// the prior session's manifest. The session identity we key on is the
// walsender slot tenure: InitWalSenderSlot acquires a slot at session
// start and WalSndKill (on_shmem_exit) releases it at session end, and
// both points clear this entry — so the manifest is dropped with the
// session, exactly when C's process death would have freed it.
struct WalSndShared {
    ctl: &'static WalSndCtlData,
    uploaded_manifests: Box<[Mutex<Option<Box<basebackup_incremental::IncrementalBackupInfo>>>]>,
}

static WAL_SND_CTL: OnceLock<WalSndShared> = OnceLock::new();

fn wal_snd_shared(ctl: &'static WalSndCtlData) -> WalSndShared {
    WalSndShared {
        ctl,
        uploaded_manifests: (0..ctl.walsnds.len()).map(|_| Mutex::new(None)).collect(),
    }
}

// The server publishes the arena from WalSndShmemInit before any backend
// runs; substrate test binaries without a shmem seam get a heap image sized
// by the walsender_config mirror of max_wal_senders (the ShmemIndex row is
// the only thing the seam adds).
fn wal_snd_ctl_shared() -> &'static WalSndShared {
    WAL_SND_CTL.get_or_init(|| {
        wal_snd_shared(Box::leak(Box::new(wal_snd_ctl_boot_image(
            walsender_config::max_wal_senders(),
        ))))
    })
}

pub(crate) fn uploaded_manifests(
) -> &'static [Mutex<Option<Box<basebackup_incremental::IncrementalBackupInfo>>>] {
    &wal_snd_ctl_shared().uploaded_manifests
}

// The "first time through" image of WalSndShmemInit (walsender.c:3711-3729):
// a zeroed block, the SyncRepQueue heads dlist_init'ed, every slot's spinlock
// initialized, the three CVs initialized.
fn wal_snd_ctl_boot_image(max_wal_senders: i32) -> WalSndCtlData {
    let n = max_wal_senders.max(0) as usize;
    WalSndCtlData {
        walsnds: (0..n).map(|_| Mutex::new(walsnd_empty())).collect(),
        wal_flush_cv: ConditionVariable::new(),
        wal_replay_cv: ConditionVariable::new(),
        wal_confirm_rcv_cv: ConditionVariable::new(),
        sync_rep_queue: std::array::from_fn(|_| {
            types_storage::storage::SyncCell::new(types_storage::storage::proclist_head::default())
        }),
        sync_rep_lsn: Default::default(),
        sync_standbys_status: std::sync::atomic::AtomicU32::new(0),
    }
}

/// WalSndShmemInit (walsender.c:3702-3730): ShmemInitStruct("Wal Sender
/// Ctl", WalSndShmemSize(), &found) registers the control block in the
/// ShmemIndex — so pg_shmem_allocations lists it with C's size,
/// offsetof(WalSndCtlData, walsnds) + max_wal_senders * sizeof(WalSnd) — and
/// hands back the arena the boot image is written into on the first pass
/// (!found: MemSet 0, dlist_init the SyncRepQueues, SpinLockInit every slot,
/// ConditionVariableInit the three CVs). A re-entry finds the block
/// (found = true) and leaves the live data alone. The Rust header (two
/// words for the boxed slot slice, three CVs, the SyncRep queues/LSNs and the
/// status byte) fits inside C's 112-byte header, so the arena is C-sized
/// even at max_wal_senders = 0. Called from CreateOrAttachShmemStructs
/// (ipci.c:331) with the GUC value, like WalSndShmemSize.
pub fn WalSndShmemInit(max_wal_senders: i32) -> PgResult<()> {
    const {
        assert!(core::mem::size_of::<WalSndCtlData>() <= C_OFFSETOF_WAL_SND_CTL_DATA_WALSNDS);
        assert!(core::mem::align_of::<WalSndCtlData>() <= 64, "PG_CACHE_LINE_SIZE alignment");
    }
    let (raw, found) =
        shmem_seams::shmem_init_struct::call("Wal Sender Ctl", WalSndShmemSize(max_wal_senders)?)?;
    let p = raw.cast::<WalSndCtlData>();
    if !found {
        // SAFETY: a fresh, zeroed, cache-line-aligned ShmemIndex allocation of
        // WalSndShmemSize(max_wal_senders) >= size_of::<WalSndCtlData>() bytes
        // (asserted above), written exactly once during single-threaded shmem
        // init and leaked for the cluster lifetime like C shmem.
        unsafe { p.write(wal_snd_ctl_boot_image(max_wal_senders)) };
    }
    // Same block on every call: the ShmemIndex hands back the first one.
    // SAFETY: the block is initialized (above, or by the first pass that
    // registered it) and never freed.
    let _ = WAL_SND_CTL.set(wal_snd_shared(unsafe { &*p }));
    Ok(())
}

// C: the `WalSndCtl` pointer.
pub fn WalSndCtl() -> &'static WalSndCtlData {
    wal_snd_ctl_shared().ctl
}

/// am_cascading_walsender (walsender.c global); syncrep's priority gate.
pub fn am_cascading_walsender_now() -> bool {
    AM_CASCADING_WALSENDER.get()
}

/// MyWalSnd's index into WalSndCtl().walsnds; -1 = NULL (syncrep's is_me test).
pub fn my_walsnd_index() -> i32 {
    MY_WAL_SND.get()
}

pub(crate) fn my_walsnd() -> &'static Mutex<WalSnd> {
    let i = MY_WAL_SND.get();
    assert!(i >= 0, "walsender: MyWalSnd is NULL");
    &WalSndCtl().walsnds[i as usize]
}

// InitWalSender (walsender.c:296).
pub fn InitWalSender() {
    AM_CASCADING_WALSENDER.set(transam_xlog::RecoveryInProgress());

    InitWalSenderSlot();

    resowner::CreateAuxProcessResourceOwner().expect("CreateAuxProcessResourceOwner");

    // No going back: we mustn't write any WAL after this.
    pmsignal::MarkPostmasterChildWalSender();
    pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_ADVANCE_STATE_MACHINE);

    if init_small::globals::MyDatabaseId() == InvalidOid {
        procarray::ProcSetStatusFlagAffectsAllHorizons()
            .expect("InitWalSender: PROC_AFFECTS_ALL_HORIZONS");
    }

    // Initialize empty timestamp buffer for lag tracking (walsender.c:333).
    lag::LagTrackerInit();
}

// InitWalSenderSlot (walsender.c:2937).
fn InitWalSenderSlot() {
    assert_eq!(MY_WAL_SND.get(), -1, "InitWalSenderSlot: MyWalSnd already set");

    let ctl = WalSndCtl();
    let my_pid = init_small::globals::MyProcPid();
    let kind = if init_small::globals::MyDatabaseId() == InvalidOid {
        ReplicationKind::REPLICATION_KIND_PHYSICAL
    } else {
        ReplicationKind::REPLICATION_KIND_LOGICAL
    };

    for (i, slot) in ctl.walsnds.iter().enumerate() {
        let mut walsnd = slot.lock().expect("walsnd mutex");
        if walsnd.pid != 0 {
            continue;
        }
        *walsnd = walsnd_empty();
        walsnd.pid = my_pid;
        walsnd.kind = kind;
        drop(walsnd);
        // Session-ownership (Q2): a fresh session tenure must never observe
        // a prior tenant's uploaded manifest. WalSndKill already clears it
        // at session end; this is the acquire-side belt to that suspender.
        *uploaded_manifests()[i].lock().expect("uploaded manifest mutex") = None;
        MY_WAL_SND.set(i as i32);
        break;
    }
    // C: must not fail, per the free-WAL-sender check in InitProcess.
    assert!(MY_WAL_SND.get() >= 0, "InitWalSenderSlot: no free walsender slot");

    ipc_seams::on_shmem_exit::call(WalSndKill, 0);
}

// WalSndKill (walsender.c:3012).
fn WalSndKill(_code: i32, _arg: usize) {
    let i = MY_WAL_SND.get();
    if i < 0 {
        return;
    }
    MY_WAL_SND.set(-1);
    // Session-ownership (Q2): the uploaded manifest dies with the session's
    // slot tenure (C frees it implicitly at process death; there is no
    // process death per session here). Drop it BEFORE releasing the slot so
    // no successor tenant can race into a stale entry.
    *uploaded_manifests()[i as usize]
        .lock()
        .expect("uploaded manifest mutex") = None;
    WalSndCtl().walsnds[i as usize].lock().expect("walsnd mutex").pid = 0;
}

// HandleWalSndInitStopping (walsender.c:3560): if replication has not yet
// started, die like SIGTERM; if active, flag the main loop — it sends any
// outstanding WAL, waits for the ack, and exits gracefully (the logical
// walsender self-arms got_SIGUSR2 when caught up; see XLogSendLogical).
pub fn HandleWalSndInitStopping() {
    debug_assert!(walsender_seams::am_walsender());
    if !REPLICATION_ACTIVE.get() {
        let _ = procsignal::SendThreadSignal(
            init_small::globals::MyProcPid(),
            procsignal::signums::SIGTERM,
        );
    } else {
        GOT_STOPPING.set(true);
    }
}

// WalSndRqstFileReload (walsender.c:3588): request walsenders to reload the
// currently-open WAL file — archive recovery may have replaced it
// (KeepFileRestoredFromArchive); XLogSendPhysical consumes the flag.
pub fn WalSndRqstFileReload() {
    for slot in WalSndCtl().walsnds.iter() {
        let mut w = slot.lock().expect("walsnd mutex");
        if w.pid == 0 {
            continue;
        }
        w.needreload = true;
    }
}

// WalSndInitStopping (walsender.c:3796): checkpointer tells every walsender
// to move to the stopping state before the shutdown checkpoint.
pub fn WalSndInitStopping() {
    for slot in WalSndCtl().walsnds.iter() {
        let pid = slot.lock().expect("walsnd mutex").pid;
        if pid == 0 {
            continue;
        }
        let _ = procsignal::SendProcSignal(
            pid,
            types_storage::storage::ProcSignalReason::PROCSIG_WALSND_INIT_STOPPING,
            types_core::INVALID_PROC_NUMBER,
        );
    }
}

// WalSndWaitStopping (walsender.c:3822): wait until every walsender has quit
// or reached the stopping state, so the shutdown checkpoint can proceed.
pub fn WalSndWaitStopping() {
    loop {
        let mut all_stopped = true;
        for slot in WalSndCtl().walsnds.iter() {
            let w = slot.lock().expect("walsnd mutex");
            if w.pid == 0 {
                continue;
            }
            if w.state != WalSndState::Stopping {
                all_stopped = false;
                break;
            }
        }
        if all_stopped {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

// WalSndGetStateString (walsender.c:3888).
pub fn WalSndGetStateString(state: WalSndState) -> &'static str {
    match state {
        WalSndState::Startup => "startup",
        WalSndState::Backup => "backup",
        WalSndState::Catchup => "catchup",
        WalSndState::Streaming => "streaming",
        WalSndState::Stopping => "stopping",
    }
}

// pg_stat_get_wal_senders' shared-memory scan half (walsender.c:3914); the SQL
// function body lives in pgstatfuncs. The sync-standby classification comes
// from syncrep's SyncRepGetCandidateStandbys via seam (empty when syncrep is
// not linked or synchronous_standby_names is unset — C-identical).
fn pg_stat_wal_senders_snapshot() -> Vec<walsender_seams::WalSndStatRow> {
    let ctl = WalSndCtl();
    let candidates: Vec<(i32, i32)> =
        if syncrep_seams::sync_rep_candidate_indexes::is_installed() {
            syncrep_seams::sync_rep_candidate_indexes::call().unwrap_or_default()
        } else {
            Vec::new()
        };
    let method_is_priority = if syncrep_seams::sync_rep_method_is_priority::is_installed() {
        syncrep_seams::sync_rep_method_is_priority::call()
    } else {
        true
    };
    let mut rows = Vec::new();
    for (i, slot) in ctl.walsnds.iter().enumerate() {
        let w = slot.lock().expect("walsnd mutex");
        if w.pid == 0 {
            continue;
        }
        // Stale-data protection: match both walsnd_index and pid (C comment).
        let is_sync_standby = candidates
            .iter()
            .any(|&(idx, pid)| idx == i as i32 && pid == w.pid);
        rows.push(walsender_seams::WalSndStatRow {
            pid: w.pid,
            state: WalSndGetStateString(w.state),
            sent_ptr: w.sentPtr,
            write: w.write,
            flush: w.flush,
            apply: w.apply,
            write_lag: w.writeLag,
            flush_lag: w.flushLag,
            apply_lag: w.applyLag,
            sync_priority: w.sync_standby_priority,
            is_sync_standby,
            syncrep_method_is_priority: method_is_priority,
            reply_time: w.replyTime,
        });
    }
    rows
}

// WalSndSetState (walsender.c:3858).
pub fn WalSndSetState(state: WalSndState) {
    debug_assert!(am_walsender());
    let mut walsnd = my_walsnd().lock().expect("walsnd mutex");
    walsnd.state = state;
}

pub(crate) fn my_walsnd_state() -> WalSndState {
    my_walsnd().lock().expect("walsnd mutex").state
}

// MyWalSnd shared-status writers (walsender.c: SpinLockAcquire(&MyWalSnd->mutex)
// … SpinLockRelease). Per-slot Mutex is the C spinlock (thread-native decision).
pub(crate) fn my_set_sentptr(lsn: XLogRecPtr) {
    my_walsnd().lock().expect("walsnd mutex").sentPtr = lsn;
}

pub(crate) fn my_flush() -> XLogRecPtr {
    my_walsnd().lock().expect("walsnd mutex").flush
}

pub(crate) fn my_write() -> XLogRecPtr {
    my_walsnd().lock().expect("walsnd mutex").write
}

pub(crate) fn my_kind() -> ReplicationKind {
    my_walsnd().lock().expect("walsnd mutex").kind
}

// ProcessStandbyReplyMessage's shared-status write.
#[allow(clippy::too_many_arguments)]
pub(crate) fn my_set_reply(
    write: XLogRecPtr,
    flush: XLogRecPtr,
    apply: XLogRecPtr,
    write_lag: i64,
    flush_lag: i64,
    apply_lag: i64,
    clear_lag_times: bool,
    reply_time: TimestampTz,
) {
    let mut w = my_walsnd().lock().expect("walsnd mutex");
    w.write = write;
    w.flush = flush;
    w.apply = apply;
    if write_lag != -1 || clear_lag_times {
        w.writeLag = write_lag;
    }
    if flush_lag != -1 || clear_lag_times {
        w.flushLag = flush_lag;
    }
    if apply_lag != -1 || clear_lag_times {
        w.applyLag = apply_lag;
    }
    w.replyTime = reply_time;
}

pub(crate) fn my_set_reply_time(reply_time: TimestampTz) {
    my_walsnd().lock().expect("walsnd mutex").replyTime = reply_time;
}

// WalSndErrorCleanup (walsender.c:344). WAL senders don't use transactions
// like regular backends: on the replication-command path no transaction is
// open, so tcop's AbortCurrentTransaction is a no-op (TBLOCK_DEFAULT) and
// THIS is the release point for LWLocks/CV sleeps/wait events held at the
// error (e.g. ReplicationSlotCreate's 42710 under
// ReplicationSlotAllocationLock, slot.c:412). The physical xlogreader is a
// StartReplication local here; its segment is closed there on both exits.
pub fn WalSndErrorCleanup() -> PgResult<()> {
    lwlock::LWLockReleaseAll()?;
    condition_variable::ConditionVariableCancelSleep();
    if waitevent_seams::pgstat_report_wait_end::is_installed() {
        waitevent_seams::pgstat_report_wait_end::call();
    }
    if aio_seams::pgaio_error_cleanup::is_installed() {
        aio_seams::pgaio_error_cleanup::call();
    }

    if slot::MyReplicationSlot().is_some() {
        slot::ReplicationSlotRelease()?;
    }
    slot::ReplicationSlotCleanup(false)?;

    REPLICATION_ACTIVE.set(false);

    // If there is a transaction in progress, it will clean up our
    // ResourceOwner, but if a replication command set up a resource owner
    // without a transaction, we've got to clean that up now.
    if !xact::IsTransactionOrTransactionBlock() {
        resowner::ReleaseAuxProcessResources(false)?;
    }

    if GOT_STOPPING.get() || GOT_SIGUSR2.get() {
        ipc_seams::proc_exit::call(0, init_small::globals::MyProcPid());
    }

    // Revert back to startup state
    WalSndSetState(WalSndState::Startup);
    Ok(())
}

// exec_replication_command (walsender.c:1983).
pub fn exec_replication_command(cmd_string: &str) -> PgResult<bool> {
    // C: `debug_query_string = cmd_string;` (walsender.c:2113); the scope's
    // drop is the tail reset, retiring the text for the error report.
    let _debug_query = elog::debug_query_string_scope(cmd_string);
    if GOT_STOPPING.get() {
        WalSndSetState(WalSndState::Stopping);
    }

    if my_walsnd_state() == WalSndState::Stopping {
        return ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot execute new commands while WAL sender is in stopping mode")
            .finish(loc(2003, "exec_replication_command"))
            .map(|()| false);
    }

    snapbuild::snap_build_clear_exported_snapshot()?;

    postgres_seams::check_for_interrupts::call()?;

    // C's retained cmd_context: transactions the command manages must not
    // outlive the context current at their start, so it lives per command
    // here, dropped only after the dispatch returns.
    let cmd_context = mcx::MemoryContext::new("Replication command context");
    let mcx = cmd_context.mcx();

    if !repl_gram::is_replication_command(cmd_string)? {
        if init_small::globals::MyDatabaseId() == InvalidOid {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot execute SQL commands in WAL sender for physical replication")
                .finish(loc(2063, "exec_replication_command"))
                .map(|()| false);
        }
        return Ok(false);
    }

    let cmd = repl_gram::replication_parse(cmd_string)?;

    backend_status_seams::pgstat_report_activity::call(
        backend_status_seams::BackendState::STATE_RUNNING,
        Some(cmd_string),
    );

    ereport(if LOG_REPLICATION_COMMANDS.get() {
        LOG
    } else {
        DEBUG1
    })
    .errmsg(format!("received replication command: {cmd_string}"))
    .finish(loc(2095, "exec_replication_command"))?;

    if xact::IsAbortedTransactionBlockState() {
        return ereport(ERROR)
            .errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION)
            .errmsg("current transaction is aborted, commands ignored until end of transaction block")
            .finish(loc(2103, "exec_replication_command"))
            .map(|()| false);
    }

    postgres_seams::check_for_interrupts::call()?;

    match cmd {
        ReplCommand::IdentifySystem => {
            let cmdtag = "IDENTIFY_SYSTEM";
            ps_status_seams::set_ps_display::call(cmdtag);
            IdentifySystem(mcx)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::VariableShow(n) => {
            let cmdtag = "SHOW";
            ps_status_seams::set_ps_display::call(cmdtag);

            let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::RemoteSimple);

            // syscache access needs a transaction environment
            xact::StartTransactionCommand()?;
            guc_funcs::GetPGVariable(mcx, &n.name, &mut dest)?;
            xact::CommitTransactionCommand()?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::ReadReplicationSlot(c) => {
            let cmdtag = "READ_REPLICATION_SLOT";
            ps_status_seams::set_ps_display::call(cmdtag);
            ReadReplicationSlot(mcx, c)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::CreateReplicationSlot(c) => {
            let cmdtag = "CREATE_REPLICATION_SLOT";
            ps_status_seams::set_ps_display::call(cmdtag);
            CreateReplicationSlot(mcx, c)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::DropReplicationSlot(c) => {
            let cmdtag = "DROP_REPLICATION_SLOT";
            ps_status_seams::set_ps_display::call(cmdtag);
            DropReplicationSlot(c)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::AlterReplicationSlot(c) => {
            let cmdtag = "ALTER_REPLICATION_SLOT";
            ps_status_seams::set_ps_display::call(cmdtag);
            AlterReplicationSlot(c)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::TimeLineHistory(c) => {
            let cmdtag = "TIMELINE_HISTORY";
            ps_status_seams::set_ps_display::call(cmdtag);
            xact::PreventInTransactionBlock(true, cmdtag)?;
            SendTimeLineHistory(mcx, c)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::StartReplication(c) => {
            // Physical closes with its own "START_STREAMING" completion
            // (walsender.c:1029), logical with "COPY 0"; the dispatch then
            // sends this "START_REPLICATION" completion — C's deliberate
            // dupe ("necessary per libpqrcv_endstreaming", walsender.c:2182).
            let cmdtag = "START_REPLICATION";
            ps_status_seams::set_ps_display::call(cmdtag);
            // walsender.c:2195
            xact::PreventInTransactionBlock(true, cmdtag)?;
            if c.kind == ReplicationKind::REPLICATION_KIND_PHYSICAL {
                streaming::StartReplication(mcx, &c)?;
            } else {
                logical_stream::StartLogicalReplication(&c)?;
            }
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::BaseBackup(c) => {
            let cmdtag = "BASE_BACKUP";
            ps_status_seams::set_ps_display::call(cmdtag);
            // walsender.c:2163
            xact::PreventInTransactionBlock(true, cmdtag)?;
            // SendBaseBackup lives in the basebackup crate (off the serial path);
            // installed as the walsender_seams::base_backup seam.
            walsender_seams::base_backup::call(c)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::UploadManifest => {
            let cmdtag = "UPLOAD_MANIFEST";
            ps_status_seams::set_ps_display::call(cmdtag);
            xact::PreventInTransactionBlock(true, cmdtag)?;
            UploadManifest(mcx)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
    }

    // ps display / pg_stat_activity reset to "idle" by PostgresMain.
    Ok(true)
}

// GetStandbyFlushRecPtr (xlog.c:6653): what a cascading standby may send —
// everything replayed, plus anything the walreceiver streamed on the replay
// timeline. Hosted here while transam_xlog is frozen (recovery-standby lanes);
// reads through the installed seams, exactly the two C callees.
pub(crate) fn GetStandbyFlushRecPtr() -> (types_core::XLogRecPtr, TimeLineID) {
    let (receive_ptr, _latest_chunk_start, receive_tli) =
        if walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::is_installed() {
            walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::call()
        } else {
            (0, 0, 0)
        };
    let (replay_ptr, replay_tli) = xlogrecovery_seams::get_xlog_replay_rec_ptr::call();
    let mut result = replay_ptr;
    if receive_tli == replay_tli && receive_ptr > replay_ptr {
        result = receive_ptr;
    }
    (result, replay_tli)
}

// StartReplication's historic-timeline epilogue (walsender.c:990): a
// single-row (next_tli int8, next_tli_startpos text) result set.
pub(crate) fn send_next_timeline_result_set(mcx: mcx::Mcx<'_>) -> PgResult<()> {
    let valid_upto = SEND_TIME_LINE_VALID_UPTO.with(|c| c.get());
    let next_tli = SEND_TIME_LINE_NEXT_TLI.with(|c| c.get());
    let startpos_str = format!("{:X}/{:X}", (valid_upto >> 32) as u32, valid_upto as u32);

    let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::RemoteSimple);

    // int8 for next_tli: int4 is not wide enough (TimeLineID is unsigned).
    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "next_tli", INT8OID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "next_tli_startpos", TEXTOID, -1, 0)?;

    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, &mut dest, Rc::new(tupdesc))?;

    let mut values = [Datum::null(); 2];
    let nulls = [false; 2];
    values[0] = Datum::from_i64(next_tli as i64);
    let pos_v = varlena::cstring_to_text(mcx, startpos_str.as_bytes())?;
    values[1] = Datum::from_usize(pos_v.as_bytes().as_ptr() as usize);

    exectuples_output::do_tup_output(&mut tstate, mcx, &values, &nulls)?;
    exectuples_output::end_tup_output(tstate)
}

// IdentifySystem (walsender.c:395).
fn IdentifySystem(mcx: mcx::Mcx<'_>) -> PgResult<()> {
    let sysid = format!("{}", transam_xlog::GetSystemIdentifier());

    let am_cascading = transam_xlog::RecoveryInProgress();
    AM_CASCADING_WALSENDER.set(am_cascading);

    let mut curr_tli: TimeLineID = 0;
    let logptr = if am_cascading {
        let (ptr, tli) = GetStandbyFlushRecPtr();
        curr_tli = tli;
        ptr
    } else {
        transam_xlog::GetFlushRecPtr(Some(&mut curr_tli))
    };

    let xloc = format!("{:X}/{:X}", (logptr >> 32) as u32, logptr as u32);

    let dbname = if init_small::globals::MyDatabaseId() != InvalidOid {
        // syscache access needs a transaction env.
        xact::StartTransactionCommand()?;
        let name = dbcommands_seams::get_database_name::call(init_small::globals::MyDatabaseId())?;
        xact::CommitTransactionCommand()?;
        name
    } else {
        None
    };

    let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::RemoteSimple);

    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 4)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "systemid", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "timeline", INT8OID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 3, "xlogpos", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 4, "dbname", TEXTOID, -1, 0)?;

    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, &mut dest, Rc::new(tupdesc))?;

    let mut values = [Datum::null(); 4];
    let mut nulls = [false; 4];

    let sysid_v = varlena::cstring_to_text(mcx, sysid.as_bytes())?;
    values[0] = Datum::from_usize(sysid_v.as_bytes().as_ptr() as usize);

    values[1] = Datum::from_i64(curr_tli as i64);

    let xloc_v = varlena::cstring_to_text(mcx, xloc.as_bytes())?;
    values[2] = Datum::from_usize(xloc_v.as_bytes().as_ptr() as usize);

    let dbname_v = match &dbname {
        Some(name) => Some(varlena::cstring_to_text(mcx, name.as_bytes())?),
        None => None,
    };
    match &dbname_v {
        Some(v) => values[3] = Datum::from_usize(v.as_bytes().as_ptr() as usize),
        None => nulls[3] = true,
    }

    exectuples_output::do_tup_output(&mut tstate, mcx, &values, &nulls)?;
    exectuples_output::end_tup_output(tstate)
}

// ReadReplicationSlot (walsender.c:478). One-row, three-column result set
// describing a *physical* slot: slot_type (text "physical"), restart_lsn
// (text), restart_tli (int8). A missing/unused slot yields all-NULL; a logical
// slot is rejected.
fn ReadReplicationSlot(mcx: mcx::Mcx<'_>, cmd: ReadReplicationSlotCmd) -> PgResult<()> {
    const COLS: usize = 3;

    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, COLS as i32)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "slot_type", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "restart_lsn", TEXTOID, -1, 0)?;
    // TimeLineID is unsigned, so int4 is not wide enough.
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 3, "restart_tli", INT8OID, -1, 0)?;

    let mut values = [Datum::null(); COLS];
    let mut nulls = [true; COLS];
    // Text buffers must outlive do_tup_output (Datums point into them).
    let mut slot_type_v = None;
    let mut restart_lsn_v = None;

    let control = lwlock::main_lock(types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK);
    lwlock::LWLockAcquire(control, lwlock::LW_SHARED, init_small::globals::MyProcNumber())?;
    let slotname = cmd.slotname.as_deref().unwrap_or("");
    let slot = slot::SearchNamedReplicationSlot(slotname, false)?;
    // SAFETY: in_use read under ReplicationSlotControlLock (held above)
    match slot.filter(|s| unsafe { s.in_use.get() }) {
        None => {
            lwlock::LWLockRelease(control)?;
        }
        Some(s) => {
            // Copy slot contents while holding spinlock, then release the
            // control lock (C copies the whole struct; we read the two fields).
            let (database, restart_lsn) = s.with_mutex(|| {
                // SAFETY: data read under the slot spinlock mutex (with_mutex)
                let d = unsafe { s.data.get() };
                (d.database, d.restart_lsn)
            });
            lwlock::LWLockRelease(control)?;

            if database != InvalidOid {
                return ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot use READ_REPLICATION_SLOT with a logical replication slot")
                    .finish(loc(519, "ReadReplicationSlot"));
            }

            let v = varlena::cstring_to_text(mcx, b"physical")?;
            values[0] = Datum::from_usize(v.as_bytes().as_ptr() as usize);
            slot_type_v = Some(v);
            nulls[0] = false;

            if restart_lsn != InvalidXLogRecPtr {
                let xloc = format!("{:X}/{:X}", (restart_lsn >> 32) as u32, restart_lsn as u32);
                let v = varlena::cstring_to_text(mcx, xloc.as_bytes())?;
                values[1] = Datum::from_usize(v.as_bytes().as_ptr() as usize);
                restart_lsn_v = Some(v);
                nulls[1] = false;

                // While in recovery, use the currently-replaying timeline to get
                // the LSN position's history.
                let current_timeline = if transam_xlog::RecoveryInProgress() {
                    xlogrecovery_seams::get_xlog_replay_rec_ptr::call().1
                } else {
                    transam_xlog::ctl::GetWALInsertionTimeLine()
                };
                let history = timeline_seams::read_timeline_history::call(mcx, current_timeline)?;
                let slots_position_timeline =
                    timeline_seams::tli_of_point_in_history::call(restart_lsn, &history)?;
                values[2] = Datum::from_i64(slots_position_timeline as i64);
                nulls[2] = false;
            }
        }
    }

    let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::RemoteSimple);
    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, &mut dest, Rc::new(tupdesc))?;
    exectuples_output::do_tup_output(&mut tstate, mcx, &values, &nulls)?;
    exectuples_output::end_tup_output(tstate)?;
    let _ = (slot_type_v, restart_lsn_v);
    Ok(())
}

// ereport for parseCreateReplSlotOptions / AlterReplicationSlot option clashes.
fn conflicting_or_redundant(func: &'static str, line: i32) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .finish(loc(line, func))
}

// parse_bool (bool.c): case-insensitive true/false/yes/no/on/off/1/0/t/f/y/n.
fn parse_bool(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" | "t" | "y" => Some(true),
        "false" | "no" | "off" | "0" | "f" | "n" => Some(false),
        _ => None,
    }
}

// defGetBoolean (define.c): NULL arg means true.
fn def_get_boolean(name: &str, arg: &Option<ReplOptionArg>, func: &'static str) -> PgResult<bool> {
    let value = match arg {
        None => Some(true),
        Some(ReplOptionArg::Bool(b)) => Some(*b),
        Some(ReplOptionArg::Int(0)) => Some(false),
        Some(ReplOptionArg::Int(1)) => Some(true),
        Some(ReplOptionArg::Int(_)) => None,
        Some(ReplOptionArg::Str(s)) => parse_bool(s),
    };
    match value {
        Some(v) => Ok(v),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("{name} requires a Boolean value"))
                .finish(loc(0, func))?;
            unreachable!()
        }
    }
}

// defGetString (define.c), the arms replication options use.
fn def_get_string(
    name: &str,
    arg: &Option<ReplOptionArg>,
    func: &'static str,
) -> PgResult<String> {
    match arg {
        Some(ReplOptionArg::Str(s)) => Ok(s.clone()),
        Some(ReplOptionArg::Int(i)) => Ok(i.to_string()),
        Some(ReplOptionArg::Bool(b)) => Ok(if *b { "true" } else { "false" }.to_string()),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("{name} requires a parameter"))
                .finish(loc(0, func))?;
            unreachable!()
        }
    }
}

// parseCreateReplSlotOptions (walsender.c:1114).
// CRSSnapshotAction (walsender.h).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CrsSnapshotAction {
    ExportSnapshot,
    NoExportSnapshot,
    UseSnapshot,
}

pub(crate) struct CreateReplSlotOptions {
    pub reserve_wal: bool,
    pub snapshot_action: CrsSnapshotAction,
    pub two_phase: bool,
    pub failover: bool,
}

fn parse_create_repl_slot_options(
    cmd: &CreateReplicationSlotCmd,
) -> PgResult<CreateReplSlotOptions> {
    let mut opts = CreateReplSlotOptions {
        reserve_wal: false,
        // Default when no SNAPSHOT option is given (walsender.c:1147).
        snapshot_action: if cmd.kind == ReplicationKind::REPLICATION_KIND_LOGICAL {
            CrsSnapshotAction::ExportSnapshot
        } else {
            CrsSnapshotAction::NoExportSnapshot
        },
        two_phase: false,
        failover: false,
    };
    let mut reserve_wal_given = false;
    let mut snapshot_action_given = false;
    let mut two_phase_given = false;
    let mut failover_given = false;

    for defel in &cmd.options {
        match defel.name.as_str() {
            "snapshot" => {
                if snapshot_action_given || cmd.kind != ReplicationKind::REPLICATION_KIND_LOGICAL {
                    conflicting_or_redundant("parseCreateReplSlotOptions", 1136)?;
                }
                let action = def_get_string("snapshot", &defel.arg, "parseCreateReplSlotOptions")?;
                snapshot_action_given = true;
                opts.snapshot_action = match action.as_str() {
                    "export" => CrsSnapshotAction::ExportSnapshot,
                    "nothing" => CrsSnapshotAction::NoExportSnapshot,
                    "use" => CrsSnapshotAction::UseSnapshot,
                    other => {
                        // walsender.c:1171
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!("unrecognized value for CREATE_REPLICATION_SLOT option \"snapshot\": \"{other}\""))
                            .finish(loc(1171, "parseCreateReplSlotOptions"))?;
                        unreachable!()
                    }
                };
            }
            "reserve_wal" => {
                if reserve_wal_given || cmd.kind != ReplicationKind::REPLICATION_KIND_PHYSICAL {
                    conflicting_or_redundant("parseCreateReplSlotOptions", 1158)?;
                }
                reserve_wal_given = true;
                opts.reserve_wal = def_get_boolean("reserve_wal", &defel.arg, "parseCreateReplSlotOptions")?;
            }
            "two_phase" => {
                if two_phase_given || cmd.kind != ReplicationKind::REPLICATION_KIND_LOGICAL {
                    conflicting_or_redundant("parseCreateReplSlotOptions", 1168)?;
                }
                two_phase_given = true;
                opts.two_phase = def_get_boolean("two_phase", &defel.arg, "parseCreateReplSlotOptions")?;
            }
            "failover" => {
                if failover_given || cmd.kind != ReplicationKind::REPLICATION_KIND_LOGICAL {
                    conflicting_or_redundant("parseCreateReplSlotOptions", 1177)?;
                }
                failover_given = true;
                opts.failover = def_get_boolean("failover", &defel.arg, "parseCreateReplSlotOptions")?;
            }
            other => {
                ereport(ERROR)
                    .errmsg_internal(format!("unrecognized option: {other}"))
                    .finish(loc(1183, "parseCreateReplSlotOptions"))?;
                unreachable!()
            }
        }
    }

    Ok(opts)
}

// CreateReplicationSlot (walsender.c:1191). Physical path fully ported (this is
// pg_basebackup / pg_receivewal --create-slot). Logical path: SNAPSHOT
// 'nothing' (pg_recvlogical) and USE_SNAPSHOT-less flows are ported; snapshot
// export/use remain contained refusals (snapbuild export unported).
fn CreateReplicationSlot(mcx: mcx::Mcx<'_>, cmd: CreateReplicationSlotCmd) -> PgResult<()> {
    let opts = parse_create_repl_slot_options(&cmd)?;
    let slotname = cmd.slotname.as_deref().unwrap_or("");
    let mut snapshot_name: Option<String> = None;

    if cmd.kind == ReplicationKind::REPLICATION_KIND_PHYSICAL {
        let persistency = if cmd.temporary { slot::RS_TEMPORARY } else { slot::RS_PERSISTENT };
        slot::ReplicationSlotCreate(slotname, false, persistency, false, false, false)?;

        if opts.reserve_wal {
            slot::ReplicationSlotReserveWal()?;
            slot::ReplicationSlotMarkDirty();
            // Write this slot to disk if it's a permanent one.
            if !cmd.temporary {
                slot::ReplicationSlotSave()?;
            }
        }
    } else {
        logical::CheckLogicalDecodingRequirements()?;

        match opts.snapshot_action {
            CrsSnapshotAction::UseSnapshot => {
                // walsender.c:1262: USE_SNAPSHOT preconditions.
                if !xact::IsTransactionBlock() {
                    return ereport(ERROR)
                        .errmsg("CREATE_REPLICATION_SLOT ... (SNAPSHOT 'use') must be called inside a transaction")
                        .finish(loc(1266, "CreateReplicationSlot"));
                }
                if xact::XactIsoLevel() != guc_tables::consts::XACT_REPEATABLE_READ {
                    return ereport(ERROR)
                        .errmsg("CREATE_REPLICATION_SLOT ... (SNAPSHOT 'use') must be called in REPEATABLE READ isolation mode transaction")
                        .finish(loc(1271, "CreateReplicationSlot"));
                }
                if !xact::XactReadOnly() {
                    return ereport(ERROR)
                        .errmsg("CREATE_REPLICATION_SLOT ... (SNAPSHOT 'use') must be called in a read-only transaction")
                        .finish(loc(1276, "CreateReplicationSlot"));
                }
                if snapmgr::FirstSnapshotSet() {
                    return ereport(ERROR)
                        .errmsg("CREATE_REPLICATION_SLOT ... (SNAPSHOT 'use') must be called before any query")
                        .finish(loc(1281, "CreateReplicationSlot"));
                }
                if xact::IsSubTransaction() {
                    return ereport(ERROR)
                        .errmsg("CREATE_REPLICATION_SLOT ... (SNAPSHOT 'use') must not be called in a subtransaction")
                        .finish(loc(1286, "CreateReplicationSlot"));
                }
            }
            // walsender.c:1273: options check done early so we bail before
            // the (possibly long) DecodingContextFindStartpoint; the export
            // itself happens after the start point is found, below.
            CrsSnapshotAction::ExportSnapshot => {
                if xact::IsTransactionBlock() {
                    return ereport(ERROR)
                        .errmsg("CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export') must not be called inside a transaction")
                        .finish(loc(1276, "CreateReplicationSlot"));
                }
            }
            CrsSnapshotAction::NoExportSnapshot => {}
        }

        slot::ReplicationSlotCreate(
            slotname,
            true,
            if cmd.temporary { slot::RS_TEMPORARY } else { slot::RS_EPHEMERAL },
            opts.two_phase,
            opts.failover,
            false,
        )?;

        // Build the initial decoding context and find the decoding start
        // point (the reported consistent_point). No output writer: nothing is
        // sent to the client during slot creation (walsender.c:1285 passes
        // WalSndPrepareWrite/WriteData, but FindStartpoint runs fast_forward
        // -- the SQL path (slotfuncs.c) passes NULLs the same way here).
        // USE_SNAPSHOT (and EXPORT) build a full snapshot (walsender.c:1310).
        let need_full_snapshot = matches!(
            opts.snapshot_action,
            CrsSnapshotAction::UseSnapshot | CrsSnapshotAction::ExportSnapshot
        );
        let mut ctx = logical::CreateInitDecodingContext(
            cmd.plugin.as_deref().unwrap_or(""),
            Vec::new(),
            need_full_snapshot,
            types_core::InvalidXLogRecPtr,
            None,
            None,
            None,
        )?;

        logical_decode::DecodingContextFindStartpoint(&mut ctx)?;

        if opts.snapshot_action == CrsSnapshotAction::ExportSnapshot {
            // Export the snapshot so it can be imported by SET TRANSACTION
            // SNAPSHOT in other sessions (walsender.c:1320); the name rides
            // the result row.
            snapshot_name = Some(ctx.snapshot_builder.export_snapshot()?);
        } else if opts.snapshot_action == CrsSnapshotAction::UseSnapshot {
            // Make the initial snapshot the surrounding transaction's
            // snapshot (walsender.c:1326: SnapBuildInitialSnapshot +
            // RestoreTransactionSnapshot against our own proc).
            let snap = ctx.snapshot_builder.initial_snapshot()?;
            let my_procno =
                lmgr_proc::MyProc().expect("walsender has a PGPROC");
            snapmgr::RestoreTransactionSnapshot(&snap, my_procno)?;
        }

        ctx.free()?;

        if !cmd.temporary {
            slot::ReplicationSlotPersist()?;
        }
    }

    let slot_ref = slot::MyReplicationSlot().expect("CreateReplicationSlot: no slot acquired");
    // SAFETY: MyReplicationSlot's data owned by this backend (slot spinlock)
    let d = unsafe { slot_ref.data.get() };
    let xloc = format!("{:X}/{:X}", (d.confirmed_flush >> 32) as u32, d.confirmed_flush as u32);
    let slot_name = String::from_utf8_lossy(d.name.name_str()).into_owned();

    let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::RemoteSimple);

    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 4)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "slot_name", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "consistent_point", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 3, "snapshot_name", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 4, "output_plugin", TEXTOID, -1, 0)?;

    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, &mut dest, Rc::new(tupdesc))?;

    let name_v = varlena::cstring_to_text(mcx, slot_name.as_bytes())?;
    let xloc_v = varlena::cstring_to_text(mcx, xloc.as_bytes())?;

    let mut values = [Datum::null(); 4];
    let mut nulls = [false; 4];
    values[0] = Datum::from_usize(name_v.as_bytes().as_ptr() as usize);
    values[1] = Datum::from_usize(xloc_v.as_bytes().as_ptr() as usize);
    let snap_v;
    if let Some(sn) = snapshot_name.as_deref() {
        snap_v = varlena::cstring_to_text(mcx, sn.as_bytes())?;
        values[2] = Datum::from_usize(snap_v.as_bytes().as_ptr() as usize);
    } else {
        nulls[2] = true;
    }
    let plugin_v;
    if let Some(pl) = cmd.plugin.as_deref() {
        plugin_v = varlena::cstring_to_text(mcx, pl.as_bytes())?;
        values[3] = Datum::from_usize(plugin_v.as_bytes().as_ptr() as usize);
    } else {
        nulls[3] = true;
    }

    exectuples_output::do_tup_output(&mut tstate, mcx, &values, &nulls)?;
    exectuples_output::end_tup_output(tstate)?;
    let _ = (name_v, xloc_v);

    slot::ReplicationSlotRelease()?;
    Ok(())
}

// DropReplicationSlot (walsender.c:1396).
fn DropReplicationSlot(cmd: DropReplicationSlotCmd) -> PgResult<()> {
    slot::ReplicationSlotDrop(cmd.slotname.as_deref().unwrap_or(""), !cmd.wait)
}

// ===========================================================================
// UPLOAD_MANIFEST (walsender.c:667 UploadManifest) — receive a backup
// manifest as a CopyIn stream and stash the parsed IncrementalBackupInfo in
// the SESSION-owned store (see UPLOADED_MANIFESTS for the Q2
// session-ownership ruling; C uses a process-static under
// CacheMemoryContext, which pgrust must not).
// ===========================================================================

// pqcomm.h message-size limits (as in commands/copy fromparse.rs).
const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;
const PQ_LARGE_MESSAGE_LIMIT: i32 = 0x3fffffff - 1;

/// The session's uploaded-manifest cell, keyed by the walsender slot this
/// session holds; None when MyWalSnd is unset. basebackup's SendBaseBackup
/// reads the manifest through this (C: the `uploaded_manifest` argument of
/// SendBaseBackup), holding the lock for the duration of its use — the only
/// other toucher is this same session's UploadManifest/WalSndKill.
pub fn uploaded_manifest_cell(
) -> Option<&'static Mutex<Option<Box<basebackup_incremental::IncrementalBackupInfo>>>> {
    let i = MY_WAL_SND.get();
    if i < 0 {
        return None;
    }
    Some(&uploaded_manifests()[i as usize])
}

/// Whether this walsender session has an uploaded manifest (basebackup's
/// `ib == NULL` gate for BASE_BACKUP (INCREMENTAL)).
pub fn uploaded_manifest_exists() -> bool {
    uploaded_manifest_cell()
        .is_some_and(|m| m.lock().expect("uploaded manifest mutex").is_some())
}

// Handle UPLOAD_MANIFEST command (walsender.c:667).
fn UploadManifest(mcx: mcx::Mcx<'_>) -> PgResult<()> {
    // C switches to AuxProcessResourceOwner because parsing uses the
    // cryptohash machinery; pgrust's SHA-256 (pg_sha2) is plain Rust with no
    // resource-owner footprint, so there is nothing to pin here.

    let mut ib = Box::new(basebackup_incremental::CreateIncrementalBackupInfo(
        transam_xlog::GetSystemIdentifier(),
    ));

    // Send a CopyInResponse message: overall format 0, 0 columns.
    pqcomm::pq_putmessage(b'G', &[0u8, 0, 0])?;
    pqcomm::pq_flush()?;

    // Receive packets from client until done.
    let mut buf = stringinfo::StringInfo::new_in(mcx)?;
    while HandleUploadManifestPacket(&mut buf, &mut ib)? {}

    // Finish up manifest processing.
    ib.FinalizeIncrementalManifest()?;

    // Discard any old manifest information and arrange to preserve the new
    // information we just got (C: MemoryContextDelete of the prior context +
    // reparent under CacheMemoryContext; here the session cell's old Box
    // drops on overwrite). Error paths above leave any prior manifest
    // intact, exactly like C.
    let cell = uploaded_manifest_cell().expect("walsender has a WalSnd slot");
    *cell.lock().expect("uploaded manifest mutex") = Some(ib);

    Ok(())
}

// Process one packet received during the handling of an UPLOAD_MANIFEST
// operation (walsender.c:733 HandleUploadManifestPacket). `buf` is scratch
// space. Returns true if the caller should continue processing additional
// packets, false if the UPLOAD_MANIFEST operation is complete.
fn HandleUploadManifestPacket(
    buf: &mut stringinfo::StringInfo<'_>,
    ib: &mut basebackup_incremental::IncrementalBackupInfo,
) -> PgResult<bool> {
    // HOLD_CANCEL_INTERRUPTS() .. RESUME_CANCEL_INTERRUPTS() around the
    // message read (drop guard: the resume must run on error paths too).
    struct CancelHoldoff;
    impl Drop for CancelHoldoff {
        fn drop(&mut self) {
            init_small::globals::ResumeCancelInterrupts();
        }
    }
    init_small::globals::HoldCancelInterrupts();
    let holdoff = CancelHoldoff;

    pqcomm::pq_startmsgread()?;
    let mtype = pqcomm::pq_getbyte()?;
    if mtype == pqcomm::EOF {
        return ereport(ERROR)
            .errcode(types_error::ERRCODE_CONNECTION_FAILURE)
            .errmsg("unexpected EOF on client connection with an open transaction")
            .finish(loc(744, "HandleUploadManifestPacket"))
            .map(|()| false);
    }

    let maxmsglen = match mtype as u8 {
        b'd' => PQ_LARGE_MESSAGE_LIMIT, /* CopyData */
        b'c' | b'f' | b'H' | b'S' => PQ_SMALL_MESSAGE_LIMIT, /* CopyDone, CopyFail, Flush, Sync */
        _ => {
            ereport(ERROR)
                .errcode(types_error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "unexpected message type 0x{:02X} during COPY from stdin",
                    mtype
                ))
                .finish(loc(760, "HandleUploadManifestPacket"))?;
            unreachable!();
        }
    };

    // Now collect the message body.
    if pqcomm::pq_getmessage(buf, maxmsglen)? != 0 {
        return ereport(ERROR)
            .errcode(types_error::ERRCODE_CONNECTION_FAILURE)
            .errmsg("unexpected EOF on client connection with an open transaction")
            .finish(loc(770, "HandleUploadManifestPacket"))
            .map(|()| false);
    }
    drop(holdoff); // RESUME_CANCEL_INTERRUPTS

    // Process the message.
    match mtype as u8 {
        b'd' => {
            /* CopyData */
            ib.AppendIncrementalManifestData(buf.as_bytes())?;
            Ok(true)
        }
        b'c' => Ok(false), /* CopyDone */
        b'H' | b'S' => Ok(true), /* Sync/Flush: ignore, as elsewhere in CopyOut */
        b'f' => {
            /* CopyFail */
            let body = buf.as_bytes();
            let nul = body.iter().position(|&b| b == 0).unwrap_or(body.len());
            let msg = String::from_utf8_lossy(&body[..nul]);
            ereport(ERROR)
                .errcode(types_error::ERRCODE_QUERY_CANCELED)
                .errmsg(format!("COPY from stdin failed: {msg}"))
                .finish(loc(791, "HandleUploadManifestPacket"))?;
            unreachable!();
        }
        _ => unreachable!("message type validated above"),
    }
}

// AlterReplicationSlot (walsender.c:1405).
fn AlterReplicationSlot(cmd: AlterReplicationSlotCmd) -> PgResult<()> {
    let mut failover_given = false;
    let mut two_phase_given = false;
    let mut failover = false;
    let mut two_phase = false;

    for defel in &cmd.options {
        match defel.name.as_str() {
            "failover" => {
                if failover_given {
                    conflicting_or_redundant("AlterReplicationSlot", 1419)?;
                }
                failover_given = true;
                failover = def_get_boolean("failover", &defel.arg, "AlterReplicationSlot")?;
            }
            "two_phase" => {
                if two_phase_given {
                    conflicting_or_redundant("AlterReplicationSlot", 1427)?;
                }
                two_phase_given = true;
                two_phase = def_get_boolean("two_phase", &defel.arg, "AlterReplicationSlot")?;
            }
            other => {
                return ereport(ERROR)
                    .errmsg_internal(format!("unrecognized option: {other}"))
                    .finish(loc(1434, "AlterReplicationSlot"));
            }
        }
    }

    slot::ReplicationSlotAlter(
        cmd.slotname.as_deref().unwrap_or(""),
        if failover_given { Some(failover) } else { None },
        if two_phase_given { Some(two_phase) } else { None },
    )
}

// SendTimeLineHistory (walsender.c:577). One-row, two-column result set: the
// timeline-history file name and its raw contents. RowDescription + DataRow go
// through DestRemoteSimple (byte-identical to C's manual framing for text).
fn SendTimeLineHistory(mcx: mcx::Mcx<'_>, cmd: TimeLineHistoryCmd) -> PgResult<()> {
    let mut dest = tcop_dest::CreateDestReceiver(types_dest::CommandDest::RemoteSimple);

    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "filename", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "content", TEXTOID, -1, 0)?;

    let histfname = timeline::TLHistoryFileName(cmd.timeline);
    let path = timeline::TLHistoryFilePath(cmd.timeline);

    // Send a RowDescription message (walsender.c:603 dest->rStartup) BEFORE
    // touching the file: an open error follows the 'T' on the wire, as in C.
    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, &mut dest, Rc::new(tupdesc))?;

    // O_RDONLY | PG_BINARY (PG_BINARY == 0 on non-Windows).
    let fd = fd::OpenTransientFile(&path, libc::O_RDONLY)?;
    if fd < 0 {
        let en = current_errno();
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .finish(loc(614, "SendTimeLineHistory"));
    }

    // Determine file length and send it to client (walsender.c:618).
    // SAFETY: fd is the open descriptor from OpenTransientFile above.
    let histfilelen = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
    if histfilelen < 0 {
        let en = current_errno();
        fd::CloseTransientFile(fd);
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not seek to end of file \"{path}\": %m"))
            .finish(loc(622, "SendTimeLineHistory"));
    }
    // SAFETY: as above.
    if unsafe { libc::lseek(fd, 0, libc::SEEK_SET) } != 0 {
        let en = current_errno();
        fd::CloseTransientFile(fd);
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not seek to beginning of file \"{path}\": %m"))
            .finish(loc(626, "SendTimeLineHistory"));
    }

    let mut content: Vec<u8> = Vec::with_capacity(histfilelen as usize);
    let mut rbuf = [0u8; 8192];
    let mut bytesleft = histfilelen as i64;
    while bytesleft > 0 {
        // SAFETY: rbuf is a live writable buffer.
        let nread = unsafe { libc::read(fd, rbuf.as_mut_ptr().cast(), rbuf.len()) };
        if nread < 0 {
            let en = current_errno();
            fd::CloseTransientFile(fd);
            return ereport(ERROR)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .finish(loc(643, "SendTimeLineHistory"));
        } else if nread == 0 {
            // The file shrank under us (walsender.c:646).
            fd::CloseTransientFile(fd);
            return ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!("could not read file \"{path}\": read {nread} of {bytesleft}"))
                .finish(loc(648, "SendTimeLineHistory"));
        }
        content.extend_from_slice(&rbuf[..nread as usize]);
        bytesleft -= nread as i64;
    }

    if fd::CloseTransientFile(fd) != 0 {
        let en = current_errno();
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\": %m"))
            .finish(loc(658, "SendTimeLineHistory"));
    }

    let fname_v = varlena::cstring_to_text(mcx, histfname.as_bytes())?;
    let content_v = varlena::cstring_to_text(mcx, &content)?;
    let values = [
        Datum::from_usize(fname_v.as_bytes().as_ptr() as usize),
        Datum::from_usize(content_v.as_bytes().as_ptr() as usize),
    ];
    let nulls = [false, false];
    exectuples_output::do_tup_output(&mut tstate, mcx, &values, &nulls)?;
    exectuples_output::end_tup_output(tstate)?;
    let _ = (fname_v, content_v);
    Ok(())
}

// WaitForStandbyConfirmation's wait loop (slot.c:2843, hosted here because
// WalSndCtl->wal_confirm_rcv_cv lives in this crate; slot.c's early exits run
// in the slot crate before the seam call).
// wait_event_names.txt Client section index 6 (see logical_stream.rs).
pub(crate) const WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION: u32 = 0x0600_0000 | 6;

fn wait_for_standby_confirmation_loop(wait_for_lsn: types_core::XLogRecPtr) -> PgResult<()> {

    condition_variable::ConditionVariablePrepareToSleep(&WalSndCtl().wal_confirm_rcv_cv);

    loop {
        postgres_seams::check_for_interrupts::call()?;

        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
        }

        // Exit if done waiting for every slot.
        if slot::StandbySlotsHaveCaughtup(wait_for_lsn, types_error::WARNING)? {
            break;
        }

        // 1s timeout so a changed synchronized_standby_slots is noticed.
        condition_variable::ConditionVariableTimedSleep(
            &WalSndCtl().wal_confirm_rcv_cv,
            1000,
            WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION,
        )?;
    }

    condition_variable::ConditionVariableCancelSleep();
    Ok(())
}

/// PhysicalWakeupLogicalWalSnd (walsender.c:1728): wake logical walsenders
/// with failover slots when the acquired physical slot is listed in
/// synchronized_standby_slots.
pub(crate) fn PhysicalWakeupLogicalWalSnd() {
    let s = slot::MyReplicationSlot().expect("PhysicalWakeupLogicalWalSnd: no slot");
    debug_assert!(slot::SlotIsPhysical(s));

    // On a standby there are no walsenders waiting for standbys (no syncing
    // to cascading standbys).
    if transam_xlog::RecoveryInProgress() {
        return;
    }

    // SAFETY: MyReplicationSlot's data owned by this backend (slot spinlock)
    let name = String::from_utf8_lossy(unsafe { s.data.get() }.name.name_str()).into_owned();
    if slot::SlotExistsInSyncStandbySlots(&name) {
        condition_variable::ConditionVariableBroadcast(&WalSndCtl().wal_confirm_rcv_cv);
    }
}

pub fn init_seams() {
    walsender_seams::wait_for_standby_confirmation::set(wait_for_standby_confirmation_loop);
    guc_tables::vars::log_replication_commands.install(guc_tables::GucVarAccessors {
        get: || LOG_REPLICATION_COMMANDS.get(),
        set: |v| LOG_REPLICATION_COMMANDS.set(v),
    });
    guc_tables::vars::wal_sender_timeout.install(guc_tables::GucVarAccessors {
        get: || WAL_SENDER_TIMEOUT.get(),
        set: |v| WAL_SENDER_TIMEOUT.set(v),
    });
    walsender_seams::exec_replication_command::set(exec_replication_command);
    walsender_seams::physical_wakeup_logical_walsnd::set(PhysicalWakeupLogicalWalSnd);
    walsender_seams::init_wal_sender::set(InitWalSender);
    walsender_seams::wal_snd_error_cleanup::set(WalSndErrorCleanup);
    walsender_seams::wal_snd_wakeup::set(wakeup::WalSndWakeup);
    walsender_seams::wal_snd_rqst_file_reload::set(WalSndRqstFileReload);
    // WalSndLastCycleHandler (walsender.c:3475).
    walsender_seams::handle_walsnd_init_stopping::set(HandleWalSndInitStopping);
    walsender_seams::wal_snd_init_stopping::set(WalSndInitStopping);
    walsender_seams::wal_snd_wait_stopping::set(WalSndWaitStopping);
    walsender_seams::wal_snd_last_cycle_handler::set(|| {
        GOT_SIGUSR2.set(true);
        latch_seams::set_latch_my_latch::call();
    });
    walsender_seams::pg_stat_wal_senders_snapshot::set(pg_stat_wal_senders_snapshot);
}

#[cfg(test)]
mod conformance_tests;

#[cfg(test)]
mod tests {
    use super::*;

    // WalSndSegmentOpen (walsender.c:3119): a missing segment is reported by
    // its bare file name (xlogutils' shared opener says "pg_wal/<name>").
    #[test]
    fn segment_open_reports_removed_segment_by_bare_name() {
        use xlogreader::XLogSegmentRoutine as _;
        file_seams::basic_open_file::set(|_name, _flags| {
            // A failed open(2) leaves ENOENT in this thread's errno slot.
            #[cfg(target_os = "macos")]
            // SAFETY: libc returns this thread's errno slot.
            unsafe { *libc::__error() = libc::ENOENT };
            #[cfg(not(target_os = "macos"))]
            // SAFETY: libc returns this thread's errno slot.
            unsafe { *libc::__errno_location() = libc::ENOENT };
            -1
        });
        let mut v = xlogreader_seams::XLogReaderState::default();
        v.segcxt.ws_segsize = 16 * 1024 * 1024;
        let mut tli = 1;
        let err = streaming::WalSndSegment
            .segment_open(&mut v, 0, &mut tli)
            .unwrap_err();
        assert_eq!(err.message, "requested WAL segment 000000010000000000000000 has already been removed");
        assert_eq!(err.sqlstate, types_error::ERRCODE_UNDEFINED_FILE);
        assert_eq!(err.location.as_ref().and_then(|l| l.funcname.as_deref()), Some("WalSndSegmentOpen"));
    }

    // WalSndRqstFileReload flags every ACTIVE slot's needreload and leaves
    // free slots (pid == 0) untouched, as C's per-slot pid gate does.
    #[test]
    fn rqst_file_reload_flags_only_active_slots() {
        let _g = slot_lock();
        let ctl = WalSndCtl();
        assert!(ctl.walsnds.len() >= 2, "boot max_wal_senders covers two slots");
        {
            let mut w = ctl.walsnds[0].lock().expect("walsnd mutex");
            w.pid = 4711;
            w.needreload = false;
        }

        WalSndRqstFileReload();

        assert!(
            ctl.walsnds[0].lock().expect("walsnd mutex").needreload,
            "active slot asked to reload"
        );
        assert!(
            !ctl.walsnds[1].lock().expect("walsnd mutex").needreload,
            "free slot left alone"
        );

        let mut w = ctl.walsnds[0].lock().expect("walsnd mutex");
        w.pid = 0;
        w.needreload = false;
    }

    // ------------------------------------------------------------------
    // UPLOAD_MANIFEST (walsender.c:667) — protocol flow + the Q2
    // session-ownership contract for the uploaded IncrementalBackupInfo.
    // ------------------------------------------------------------------

    use std::cell::RefCell as StdRefCell;
    use std::collections::VecDeque;
    use std::sync::Once;

    // Serialize every test that touches WalSndCtl slot state (they run on
    // parallel threads within one process; slots are shared).
    static SLOT_LOCK: Mutex<()> = Mutex::new(());

    pub(crate) fn slot_lock() -> std::sync::MutexGuard<'static, ()> {
        SLOT_LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    thread_local! {
        pub(crate) static WIRE: StdRefCell<Vec<u8>> = const { StdRefCell::new(Vec::new()) };
        pub(crate) static INPUT: StdRefCell<VecDeque<Vec<u8>>> = const { StdRefCell::new(VecDeque::new()) };
    }

    // Real C-generated manifest (the Stage-2 corpus fixture; provenance in
    // parse_manifest/src/tests.rs).
    const C_FIXTURE: &[u8] =
        include_bytes!("../../../../common/parse_manifest/testdata/backup_manifest_pg18");
    const C_FIXTURE_SYSID: u64 = 7671867332315642488;
    const C_FIXTURE_NFILES: usize = 968;

    pub(crate) fn upload_setup() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if !postgres_seams::check_for_interrupts::is_installed() {
                postgres_seams::check_for_interrupts::set(|| Ok(()));
            }
            if !backend_status_seams::pgstat_report_activity::is_installed() {
                backend_status_seams::pgstat_report_activity::set(|_, _| {});
            }
            if !ps_status_seams::set_ps_display::is_installed() {
                ps_status_seams::set_ps_display::set(|_| {});
            }
            ipc_seams::on_shmem_exit::set(|_, _| {});
            init_small::init_seams();
            pqcomm::init_seams();
            pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
            // Fake wire: writes append to WIRE, reads drain INPUT.
            be_secure_seams::secure_write::set(|buf| {
                WIRE.with(|w| w.borrow_mut().extend_from_slice(buf));
                Ok(Ok(buf.len()))
            });
            be_secure_seams::secure_read::set(|buf| {
                INPUT.with(|q| {
                    let mut q = q.borrow_mut();
                    match q.front_mut() {
                        None => Ok(Ok(0)),
                        Some(chunk) => {
                            let n = chunk.len().min(buf.len());
                            buf[..n].copy_from_slice(&chunk[..n]);
                            chunk.drain(..n);
                            if chunk.is_empty() {
                                q.pop_front();
                            }
                            Ok(Ok(n))
                        }
                    }
                })
            });
            be_secure_seams::set_port_noblock::set(|_| true);
            // The uploaded manifest is checked against this server's sysid;
            // pin the zeroed in-process control file to the fixture's.
            transam_xlog::control_file_mark_read_for_tests();
            transam_xlog::control_file::control_file_update(|cf| {
                cf.system_identifier = C_FIXTURE_SYSID;
            });
        });
        pqcomm::pq_init_buffers().expect("pq buffers");
        WIRE.with(|w| w.borrow_mut().clear());
        INPUT.with(|q| q.borrow_mut().clear());
    }

    /// Frame one frontend protocol message: type byte + i32 length
    /// (self-inclusive) + body.
    pub(crate) fn feed_msg(msgtype: u8, body: &[u8]) {
        let mut m = Vec::with_capacity(body.len() + 5);
        m.push(msgtype);
        m.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
        m.extend_from_slice(body);
        INPUT.with(|q| q.borrow_mut().push_back(m));
    }

    // The full UPLOAD_MANIFEST CopyIn flow over the (fake) wire: dispatch
    // sends CopyInResponse, drains CopyData/Flush packets to CopyDone,
    // parses the real C manifest, and stashes the IncrementalBackupInfo in
    // the SESSION store (file count + WAL ranges + sysid gate all live).
    #[test]
    fn upload_manifest_copyin_flow_builds_session_manifest() {
        let _g = slot_lock();
        upload_setup();

        // Acquire a walsender slot the way a session does.
        assert_eq!(MY_WAL_SND.get(), -1);
        InitWalSenderSlot();
        assert!(MY_WAL_SND.get() >= 0);

        // Split the manifest across several CopyData messages, with a Flush
        // ('H') interleaved (must be ignored), then CopyDone.
        let mid = C_FIXTURE.len() / 2;
        feed_msg(b'd', &C_FIXTURE[..mid]);
        feed_msg(b'H', &[]);
        feed_msg(b'd', &C_FIXTURE[mid..]);
        feed_msg(b'c', &[]);

        let ran = exec_replication_command("UPLOAD_MANIFEST").expect("UPLOAD_MANIFEST succeeds");
        assert!(ran);
        // CommandComplete is buffered until the main loop's ReadyForQuery
        // flush; flush explicitly to inspect the full wire.
        pqcomm::pq_flush().unwrap();

        // Wire starts with CopyInResponse: 'G', len 7, format 0, 0 columns.
        let wire = WIRE.with(|w| w.borrow().clone());
        assert_eq!(&wire[..8], &[b'G', 0, 0, 0, 7, 0, 0, 0]);
        // ... and ends with CommandComplete("UPLOAD_MANIFEST\0").
        let tail_needle = b"UPLOAD_MANIFEST\0";
        assert!(
            wire.windows(tail_needle.len()).any(|w| w == tail_needle),
            "CommandComplete missing from wire"
        );

        // The manifest is in the session store, fully parsed.
        assert!(uploaded_manifest_exists());
        {
            let cell = uploaded_manifest_cell().unwrap();
            let guard = cell.lock().unwrap();
            let ib = guard.as_ref().expect("manifest stored");
            assert_eq!(ib.manifest_file_count(), C_FIXTURE_NFILES);
            assert_eq!(ib.manifest_file_lookup(b"PG_VERSION"), Some(3));
            assert_eq!(ib.manifest_wal_ranges().len(), 1);
            assert_eq!(ib.manifest_wal_ranges()[0].tli, 1);
            assert_eq!(ib.manifest_wal_ranges()[0].start_lsn, 0x2000028);
            assert_eq!(ib.manifest_wal_ranges()[0].end_lsn, 0x2000120);
        }

        // Session end: WalSndKill (the on_shmem_exit hook) must drop the
        // manifest with the session.
        let slot = MY_WAL_SND.get();
        WalSndKill(0, 0);
        assert_eq!(MY_WAL_SND.get(), -1);
        assert!(
            uploaded_manifests()[slot as usize].lock().unwrap().is_none(),
            "manifest must die with the session"
        );
    }

    // Q2 correctness point: the uploaded manifest is SESSION-owned, not
    // thread-owned. A second session hosted on the SAME thread (pool-thread
    // reuse) must never observe the prior session's manifest — neither via
    // the accessor nor by re-acquiring the same slot.
    #[test]
    fn uploaded_manifest_is_dropped_with_session_not_thread() {
        let _g = slot_lock();
        upload_setup();

        // --- Session 1 on this thread ---
        assert_eq!(MY_WAL_SND.get(), -1);
        InitWalSenderSlot();
        let slot1 = MY_WAL_SND.get();
        let cell = uploaded_manifest_cell().expect("slot held");
        *cell.lock().unwrap() = Some(Box::new(
            basebackup_incremental::CreateIncrementalBackupInfo(42),
        ));
        assert!(uploaded_manifest_exists());

        // Session 1 ends (on_shmem_exit runs WalSndKill).
        WalSndKill(0, 0);

        // Between sessions this thread must hold nothing.
        assert!(uploaded_manifest_cell().is_none());
        assert!(!uploaded_manifest_exists());
        assert!(
            uploaded_manifests()[slot1 as usize].lock().unwrap().is_none(),
            "session 1's manifest leaked past WalSndKill"
        );

        // --- Session 2 on the same thread ---
        InitWalSenderSlot();
        let slot2 = MY_WAL_SND.get();
        // Whichever slot session 2 landed on (usually slot1 again), it must
        // start with no manifest.
        assert!(!uploaded_manifest_exists(), "second session saw prior session's manifest");
        assert!(
            uploaded_manifests()[slot2 as usize].lock().unwrap().is_none()
        );

        // Belt-and-suspenders: even a manifest left behind WITHOUT WalSndKill
        // (crash-shaped teardown) is cleared by the acquire side.
        *uploaded_manifests()[slot2 as usize].lock().unwrap() = Some(Box::new(
            basebackup_incremental::CreateIncrementalBackupInfo(43),
        ));
        WalSndCtl().walsnds[slot2 as usize].lock().unwrap().pid = 0; // slot freed, cell stale
        MY_WAL_SND.set(-1);
        InitWalSenderSlot();
        let slot3 = MY_WAL_SND.get();
        if slot3 == slot2 {
            assert!(
                !uploaded_manifest_exists(),
                "acquire-side clear failed: stale manifest visible to new session"
            );
        }
        WalSndKill(0, 0);
    }

    // CopyFail ('f') aborts UPLOAD_MANIFEST with C's query-canceled error and
    // leaves no manifest behind.
    #[test]
    fn upload_manifest_copyfail_cancels_c_exact() {
        let _g = slot_lock();
        upload_setup();

        assert_eq!(MY_WAL_SND.get(), -1);
        InitWalSenderSlot();

        feed_msg(b'd', &C_FIXTURE[..128]);
        feed_msg(b'f', b"client bailed\0");

        let err = exec_replication_command("UPLOAD_MANIFEST").err().unwrap();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_QUERY_CANCELED);
        assert_eq!(err.message(), "COPY from stdin failed: client bailed");
        assert!(!uploaded_manifest_exists(), "failed upload must not stash a manifest");

        WalSndKill(0, 0);
    }

    // ShmemIndex name -> (location, requested size) for the recording
    // shmem_init_struct seam below: pg_shmem_allocations' name and size.
    fn shmem_index() -> &'static Mutex<std::collections::HashMap<String, (usize, usize)>> {
        static R: OnceLock<Mutex<std::collections::HashMap<String, (usize, usize)>>> =
            OnceLock::new();
        R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
    }

    // walsender.c:3702-3730 WalSndShmemInit: ShmemInitStruct("Wal Sender
    // Ctl", WalSndShmemSize(), &found) registers the ShmemIndex row that
    // pg_shmem_allocations lists, sized offsetof(WalSndCtlData, walsnds) +
    // max_wal_senders * sizeof(WalSnd) (walsender.c:3690-3699: 112 + 96 n on
    // LP64); a second pass finds the block and leaves it alone. Audit
    // a186-candidate-fp-replication-walsender-p2-f22716dfceeffd54944d-1.
    #[test]
    fn shmem_init_registers_wal_sender_ctl_in_shmem_index() {
        let _g = slot_lock();
        if !shmem_seams::shmem_init_struct::is_installed() {
            shmem_seams::shmem_init_struct::set(|name, size| {
                let mut reg = shmem_index().lock().unwrap();
                if let Some(&(addr, _)) = reg.get(name) {
                    return Ok((std::ptr::with_exposed_provenance_mut(addr), true));
                }
                let layout = std::alloc::Layout::from_size_align(size.max(1), 128).unwrap();
                // SAFETY: non-zero size; the block is leaked like C shmem.
                let p = unsafe { std::alloc::alloc_zeroed(layout) };
                assert!(!p.is_null());
                reg.insert(name.to_string(), (p.expose_provenance(), size));
                Ok((p, false))
            });
        }
        let n = walsender_config::max_wal_senders();
        let want = C_OFFSETOF_WAL_SND_CTL_DATA_WALSNDS + C_SIZEOF_WAL_SND * n.max(0) as usize;

        WalSndShmemInit(n).expect("WalSndShmemInit");
        let first = {
            let reg = shmem_index().lock().unwrap();
            let (addr, size) = *reg
                .get("Wal Sender Ctl")
                .unwrap_or_else(|| panic!("Wal Sender Ctl is missing from the ShmemIndex: {:?}", reg.keys().collect::<Vec<_>>()));
            assert_eq!(size, want, "pg_shmem_allocations.size for Wal Sender Ctl");
            assert_eq!(size, WalSndShmemSize(n).unwrap());
            addr
        };
        // The control block is usable: every slot free, the SyncRep LSNs zero.
        let ctl = WalSndCtl();
        assert_eq!(ctl.walsnds.len(), n.max(0) as usize);
        assert!(ctl.walsnds.iter().all(|s| s.lock().unwrap().pid == 0));
        assert!(ctl.sync_rep_lsn.iter().all(|l| l.load(std::sync::atomic::Ordering::Relaxed) == 0));

        // Re-entry (found = true): one row, same location, live data kept.
        WalSndShmemInit(n).expect("WalSndShmemInit re-entry");
        let reg = shmem_index().lock().unwrap();
        assert_eq!(reg.get("Wal Sender Ctl").map(|&(a, _)| a), Some(first));
        assert_eq!(reg.keys().filter(|k| k.as_str() == "Wal Sender Ctl").count(), 1);
    }
}
