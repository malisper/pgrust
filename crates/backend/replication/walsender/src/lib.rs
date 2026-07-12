// walsender.c — WAL sender (PG 18.3). Increments 1-3 of the replication port:
// walsender identity flags, exec_replication_command dispatch, IDENTIFY_SYSTEM,
// SHOW, slot commands, TIMELINE_HISTORY, and physical START_REPLICATION live WAL
// streaming (CopyBoth + WalSndLoop + XLogSendPhysical). BASE_BACKUP (inc 5),
// UPLOAD_MANIFEST (inc 5) and logical START_REPLICATION (inc 6) are loud panics.
#![allow(non_snake_case)]

pub mod replies;
pub mod streaming;
pub mod wakeup;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

use condition_variable::ConditionVariable;
use datum::Datum;
use elog::ereport;
use repl_gram::{
    AlterReplicationSlotCmd, CreateReplicationSlotCmd, DropReplicationSlotCmd, ReadReplicationSlotCmd,
    ReplCommand, ReplOptionArg, ReplicationKind, TimeLineHistoryCmd,
};
use types_core::{
    InvalidOid, InvalidXLogRecPtr, TimeLineID, TimestampTz, XLogRecPtr, INT8OID, TEXTOID,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_IN_FAILED_SQL_TRANSACTION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR,
    ERROR, LOG,
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
    pub(crate) static FULLY_APPLIED_LAST_TIME: Cell<bool> = const { Cell::new(false) };
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

// WalSndCtlData minus the syncrep queues (SyncRep is out of P1 scope). The
// per-kind wakeup CVs land here in increment 3: the WAL flush/replay paths
// broadcast them via the wal_snd_wakeup seam.
pub struct WalSndCtlData {
    pub walsnds: Box<[Mutex<WalSnd>]>,
    pub wal_flush_cv: ConditionVariable,
    pub wal_replay_cv: ConditionVariable,
    pub wal_confirm_rcv_cv: ConditionVariable,
}

static WAL_SND_CTL: OnceLock<WalSndCtlData> = OnceLock::new();

// C: ShmemInitStruct("Wal Sender Ctl") sized by max_wal_senders; here the
// publish-before-threads static, first-touch initialized (OnceLock).
pub fn WalSndCtl() -> &'static WalSndCtlData {
    WAL_SND_CTL.get_or_init(|| {
        let n = walsender_config::max_wal_senders().max(0) as usize;
        WalSndCtlData {
            walsnds: (0..n).map(|_| Mutex::new(walsnd_empty())).collect(),
            wal_flush_cv: ConditionVariable::new(),
            wal_replay_cv: ConditionVariable::new(),
            wal_confirm_rcv_cv: ConditionVariable::new(),
        }
    })
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

    // lag_tracker allocation is streaming-path state (increment 3).
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
    WalSndCtl().walsnds[i as usize].lock().expect("walsnd mutex").pid = 0;
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

// WalSndErrorCleanup (walsender.c:341), minus the streaming-path residue:
// LWLockReleaseAll/ConditionVariableCancelSleep/pgstat_report_wait_end/
// pgaio_error_cleanup and the xlogreader close land with increment 3; the
// command-path errors of increment 1 hold none of that state.
pub fn WalSndErrorCleanup() -> PgResult<()> {
    if slot::MyReplicationSlot().is_some() {
        slot::ReplicationSlotRelease()?;
    }
    slot::ReplicationSlotCleanup(false)?;

    REPLICATION_ACTIVE.set(false);

    if !xact::IsTransactionOrTransactionBlock() {
        resowner::ReleaseAuxProcessResources(false)?;
    }

    if GOT_STOPPING.get() || GOT_SIGUSR2.get() {
        ipc_seams::proc_exit::call(0, init_small::globals::MyProcPid());
    }

    WalSndSetState(WalSndState::Startup);
    Ok(())
}

// exec_replication_command (walsender.c:1983).
pub fn exec_replication_command(cmd_string: &str) -> PgResult<bool> {
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
            // C dispatches physical/logical here, both closing with
            // EndReplicationCommand("START_STREAMING").
            if c.kind == ReplicationKind::REPLICATION_KIND_PHYSICAL {
                let cmdtag = "START_REPLICATION";
                ps_status_seams::set_ps_display::call(cmdtag);
                streaming::StartReplication(mcx, &c)?;
            } else {
                unported("START_REPLICATION ... LOGICAL", 6);
            }
            tcop_dest::EndReplicationCommand(b"START_STREAMING")?;
        }
        ReplCommand::BaseBackup(c) => {
            let cmdtag = "BASE_BACKUP";
            ps_status_seams::set_ps_display::call(cmdtag);
            // SendBaseBackup lives in the basebackup crate (off the serial path);
            // installed as the walsender_seams::base_backup seam.
            walsender_seams::base_backup::call(c)?;
            tcop_dest::EndReplicationCommand(cmdtag.as_bytes())?;
        }
        ReplCommand::UploadManifest => unported("UPLOAD_MANIFEST", 5),
    }

    // ps display / pg_stat_activity reset to "idle" by PostgresMain;
    // debug_query_string is not a raw pointer here, nothing to reset.
    Ok(true)
}

#[cold]
fn unported(cmdtag: &str, increment: u32) -> ! {
    panic!("walsender: {cmdtag} unported (replication-p1 increment {increment})");
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
    match slot.filter(|s| s.in_use.get()) {
        None => {
            lwlock::LWLockRelease(control)?;
        }
        Some(s) => {
            // Copy slot contents while holding spinlock, then release the
            // control lock (C copies the whole struct; we read the two fields).
            let (database, restart_lsn) = s.with_mutex(|| {
                let d = s.data.get();
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

// parseCreateReplSlotOptions (walsender.c:1114). Returns reserve_wal. The
// logical-only options (snapshot/two_phase/failover) are recognized so that
// supplying them on a PHYSICAL slot raises C's "conflicting or redundant
// options" error; their values are consumed by the (unported) logical path.
fn parse_create_repl_slot_options(cmd: &CreateReplicationSlotCmd) -> PgResult<bool> {
    let mut reserve_wal = false;
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
                snapshot_action_given = true;
            }
            "reserve_wal" => {
                if reserve_wal_given || cmd.kind != ReplicationKind::REPLICATION_KIND_PHYSICAL {
                    conflicting_or_redundant("parseCreateReplSlotOptions", 1158)?;
                }
                reserve_wal_given = true;
                reserve_wal = def_get_boolean("reserve_wal", &defel.arg, "parseCreateReplSlotOptions")?;
            }
            "two_phase" => {
                if two_phase_given || cmd.kind != ReplicationKind::REPLICATION_KIND_LOGICAL {
                    conflicting_or_redundant("parseCreateReplSlotOptions", 1168)?;
                }
                two_phase_given = true;
            }
            "failover" => {
                if failover_given || cmd.kind != ReplicationKind::REPLICATION_KIND_LOGICAL {
                    conflicting_or_redundant("parseCreateReplSlotOptions", 1177)?;
                }
                failover_given = true;
            }
            other => {
                ereport(ERROR)
                    .errmsg_internal(format!("unrecognized option: {other}"))
                    .finish(loc(1183, "parseCreateReplSlotOptions"))?;
                unreachable!()
            }
        }
    }

    Ok(reserve_wal)
}

// CreateReplicationSlot (walsender.c:1191). Physical path fully ported (this is
// pg_basebackup / pg_receivewal --create-slot). Logical path — decoding-context
// + snapshot builder — is a contained panic tagged increment 6.
fn CreateReplicationSlot(mcx: mcx::Mcx<'_>, cmd: CreateReplicationSlotCmd) -> PgResult<()> {
    let reserve_wal = parse_create_repl_slot_options(&cmd)?;
    let slotname = cmd.slotname.as_deref().unwrap_or("");

    if cmd.kind == ReplicationKind::REPLICATION_KIND_PHYSICAL {
        let persistency = if cmd.temporary { slot::RS_TEMPORARY } else { slot::RS_PERSISTENT };
        slot::ReplicationSlotCreate(slotname, false, persistency, false, false, false)?;

        if reserve_wal {
            slot::ReplicationSlotReserveWal()?;
            slot::ReplicationSlotMarkDirty();
            // Write this slot to disk if it's a permanent one.
            if !cmd.temporary {
                slot::ReplicationSlotSave()?;
            }
        }
    } else {
        panic!(
            "walsender: CREATE_REPLICATION_SLOT ... LOGICAL unported \
             (CreateInitDecodingContext; replication-p1 increment 6)"
        );
    }

    let slot_ref = slot::MyReplicationSlot().expect("CreateReplicationSlot: no slot acquired");
    let d = slot_ref.data.get();
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
    // snapshot_name — always NULL on the physical path (no exported snapshot).
    nulls[2] = true;
    // output_plugin — always NULL on the physical path (cmd.plugin is None).
    nulls[3] = true;

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

    // O_RDONLY | PG_BINARY (PG_BINARY == 0 on non-Windows).
    let fd = fd::OpenTransientFile(&path, libc::O_RDONLY)?;
    if fd < 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\""))
            .finish(loc(616, "SendTimeLineHistory"));
    }

    let mut content: Vec<u8> = Vec::new();
    let mut rbuf = [0u8; 8192];
    loop {
        // SAFETY: rbuf is a live writable buffer.
        let nread = unsafe { libc::read(fd, rbuf.as_mut_ptr().cast(), rbuf.len()) };
        if nread < 0 {
            fd::CloseTransientFile(fd);
            return ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\""))
                .finish(loc(643, "SendTimeLineHistory"));
        }
        if nread == 0 {
            break;
        }
        content.extend_from_slice(&rbuf[..nread as usize]);
    }

    if fd::CloseTransientFile(fd) != 0 {
        return ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\""))
            .finish(loc(658, "SendTimeLineHistory"));
    }

    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, &mut dest, Rc::new(tupdesc))?;
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

pub fn init_seams() {
    guc_tables::vars::log_replication_commands.install(guc_tables::GucVarAccessors {
        get: || LOG_REPLICATION_COMMANDS.get(),
        set: |v| LOG_REPLICATION_COMMANDS.set(v),
    });
    guc_tables::vars::wal_sender_timeout.install(guc_tables::GucVarAccessors {
        get: || WAL_SENDER_TIMEOUT.get(),
        set: |v| WAL_SENDER_TIMEOUT.set(v),
    });
    walsender_seams::exec_replication_command::set(exec_replication_command);
    walsender_seams::init_wal_sender::set(InitWalSender);
    walsender_seams::wal_snd_error_cleanup::set(WalSndErrorCleanup);
    walsender_seams::wal_snd_wakeup::set(wakeup::WalSndWakeup);
}
