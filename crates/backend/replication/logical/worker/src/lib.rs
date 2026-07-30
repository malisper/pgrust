// worker.c (replication/logical): the leader apply worker — connect to the
// publisher, START_REPLICATION ... LOGICAL with pgoutput, and the apply loop
// dispatching BEGIN/COMMIT/ORIGIN/INSERT/UPDATE/DELETE/TRUNCATE/RELATION/
// TYPE/MESSAGE. Non-streaming, non-two-phase, non-tablesync subset; the
// excluded paths refuse loudly (never silently skip).
//
// Renderings vs C (documented per lane convention):
// - Per-worker file-statics are thread-locals (thread-model).
// - SwitchToUntrustedUser (run_as_owner=false) is NOT ported: apply runs as
//   the worker's user (the subscription owner). Recorded divergence — the
//   privilege boundary matters for untrusted table owners, not for
//   correctness of apply itself.
// - ALTER SUBSCRIPTION ... SKIP (valid subskiplsn) refuses loudly.
// - Conflict reporting (ReportApplyConflict/conflict stats) is rendered as
//   LOG lines for the update/delete-missing cases; origin-differs detection
//   is not ported (no CommitTsData lookup).
// - The DirtySnapshot in the replica-identity lookups is rendered as
//   GetLatestSnapshot + the C lock-retry protocol (see apply.rs).
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};

use elog::ereport;
use mcx::{Mcx, MemoryContext};
use types_core::{InvalidXLogRecPtr, Oid, TimestampTz, XLogRecPtr};
use types_error::{
    ErrorLocation, PgResult, DEBUG2, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, LOG,
};

use walreceiver::client::{CopyData, PgConn};

mod apply;
mod stream_apply;
mod tablesync;
#[cfg(test)]
mod tests;

pub(crate) fn request_apply_worker_exit() {
    APPLY_WORKER_EXIT.set(true);
}

pub(crate) fn apply_worker_exit_requested() -> bool {
    APPLY_WORKER_EXIT.get()
}

#[track_caller]
pub(crate) fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn get_ts() -> TimestampTz {
    timestamp_seams::get_current_timestamp::call()
}

// The cached MySubscription (C keeps it in ApplyContext, invalidated by a
// syscache callback).
#[derive(Clone)]
pub(crate) struct MySub {
    pub oid: Oid,
    pub name: String,
    pub conninfo: String,
    pub slotname: Option<String>,
    pub publications: Vec<String>,
    pub binary: bool,
    pub stream: u8,
    // Read once tablesync/two-phase land (inc E); kept for the reread diff.
    #[allow(dead_code)]
    pub twophasestate: u8,
    pub enabled: bool,
    pub origin: String,
    pub skiplsn: XLogRecPtr,
    pub owner: Oid,
    pub ownersuperuser: bool,
    pub passwordrequired: bool,
    #[allow(dead_code)]
    pub dbid: Oid,
}

thread_local! {
    pub(crate) static MY_SUBSCRIPTION: RefCell<Option<MySub>> = const { RefCell::new(None) };
    pub(crate) static SUBSCRIPTION_CHANGED: Cell<bool> = const { Cell::new(false) };
    pub(crate) static IN_REMOTE_TRANSACTION: Cell<bool> = const { Cell::new(false) };
    pub(crate) static REMOTE_FINAL_LSN: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    // send_feedback statics.
    static LAST_RECVPOS: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    static LAST_WRITEPOS: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    static LAST_FLUSHPOS: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    static FEEDBACK_SEND_TIME: Cell<TimestampTz> = const { Cell::new(0) };
    // store_flush_position's lsn_mapping: (local commit end, remote end).
    static LSN_MAPPING: RefCell<Vec<(XLogRecPtr, XLogRecPtr)>> = const { RefCell::new(Vec::new()) };
    // Set when apply must exit for a subscription change (worker restarts).
    pub(crate) static APPLY_WORKER_EXIT: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn my_sub<R>(f: impl FnOnce(&MySub) -> R) -> R {
    MY_SUBSCRIPTION.with(|s| f(s.borrow().as_ref().expect("MySubscription loaded")))
}

fn subscription_change_cb(_arg: datum::Datum, _cacheid: i32, _hash: u32) {
    SUBSCRIPTION_CHANGED.set(true);
}

fn load_subscription(mcx: Mcx<'_>, subid: Oid) -> PgResult<Option<MySub>> {
    let Some(sub) = pg_subscription::GetSubscription(mcx, subid, true)? else {
        return Ok(None);
    };
    Ok(Some(MySub {
        oid: sub.oid,
        name: sub.name.as_str().to_string(),
        conninfo: sub.conninfo.as_str().to_string(),
        slotname: sub.slotname.as_ref().map(|s| s.as_str().to_string()),
        publications: sub.publications.iter().map(|p| p.to_string()).collect(),
        binary: sub.binary,
        stream: sub.stream,
        twophasestate: sub.twophasestate,
        enabled: sub.enabled,
        origin: sub.origin.as_str().to_string(),
        skiplsn: sub.skiplsn,
        owner: sub.owner,
        ownersuperuser: sub.ownersuperuser,
        passwordrequired: sub.passwordrequired,
        dbid: sub.dbid,
    }))
}

// maybe_reread_subscription (worker.c:3961): on a significant change, log and
// exit so the launcher restarts with fresh state; on disable/drop, exit.
pub(crate) fn maybe_reread_subscription(mcx: Mcx<'_>) -> PgResult<()> {
    if !SUBSCRIPTION_CHANGED.get() {
        return Ok(());
    }
    SUBSCRIPTION_CHANGED.set(false);

    // C: "This function might be called inside or outside of transaction."
    // The apply loop's idle arm calls it with no transaction (and hence no
    // resource owner) — catalog reads need one (worker.c:3902).
    let started_tx = if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
        true
    } else {
        false
    };
    let result = maybe_reread_subscription_guts(mcx);
    if started_tx {
        xact::CommitTransactionCommand()?;
    }
    result
}

fn maybe_reread_subscription_guts(mcx: Mcx<'_>) -> PgResult<()> {
    let subid = my_sub(|s| s.oid);
    let newsub = load_subscription(mcx, subid)?;

    let exit_msg = match &newsub {
        None => Some("subscription was removed"),
        Some(n) if !n.enabled => Some("subscription was disabled"),
        Some(n) => {
            let differs = my_sub(|old| {
                n.conninfo != old.conninfo
                    || n.name != old.name
                    || n.slotname != old.slotname
                    || n.binary != old.binary
                    || n.stream != old.stream
                    || n.publications != old.publications
                    || n.origin != old.origin
                    || n.owner != old.owner
            });
            if differs {
                Some("subscription was modified")
            } else {
                None
            }
        }
    };

    if let Some(why) = exit_msg {
        let name = my_sub(|s| s.name.clone());
        let _ = elog::elog(
            LOG,
            format!(
                "logical replication worker for subscription \"{name}\" will stop because {why}"
            ),
        );
        APPLY_WORKER_EXIT.set(true);
        return Ok(());
    }

    if let Some(n) = newsub {
        MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(n));
    }
    Ok(())
}

// store_flush_position (worker.c): remember (local commit end, remote end).
pub(crate) fn store_flush_position(remote_lsn: XLogRecPtr, local_lsn: XLogRecPtr) {
    LSN_MAPPING.with(|m| m.borrow_mut().push((local_lsn, remote_lsn)));
}

// get_flush_position (worker.c): remote positions whose local commits have
// been flushed; also reports whether unflushed commits remain.
fn get_flush_position() -> (XLogRecPtr, XLogRecPtr, bool) {
    let local_flush = transam_xlog::GetFlushRecPtr(None);
    let mut write = InvalidXLogRecPtr;
    let mut flush = InvalidXLogRecPtr;
    let have_pending = LSN_MAPPING.with(|m| {
        let mut m = m.borrow_mut();
        let mut i = 0;
        while i < m.len() {
            let (local, remote) = m[i];
            if local <= local_flush {
                write = remote;
                flush = remote;
                i += 1;
            } else {
                break;
            }
        }
        m.drain(..i);
        !m.is_empty()
    });
    (write, flush, have_pending)
}

// send_feedback (worker.c:3838): 'r' standby-status update on the copy stream.
pub(crate) fn send_feedback(
    conn: &mut PgConn,
    recvpos_in: XLogRecPtr,
    force: bool,
    request_reply: bool,
) -> PgResult<()> {
    let interval = guc_tables::vars::wal_receiver_status_interval.read();
    if !force && interval <= 0 {
        return Ok(());
    }

    let mut recvpos = recvpos_in;
    if recvpos < LAST_RECVPOS.get() {
        recvpos = LAST_RECVPOS.get();
    }

    let (mut writepos, mut flushpos, have_pending_txes) = get_flush_position();

    // Nothing outstanding: report the latest received position (matters for
    // synchronous replication).
    if !have_pending_txes {
        writepos = recvpos;
        flushpos = recvpos;
    }
    if writepos < LAST_WRITEPOS.get() {
        writepos = LAST_WRITEPOS.get();
    }
    if flushpos < LAST_FLUSHPOS.get() {
        flushpos = LAST_FLUSHPOS.get();
    }

    let now = get_ts();
    if !force
        && writepos == LAST_WRITEPOS.get()
        && flushpos == LAST_FLUSHPOS.get()
        && !adt_timestamp::TimestampDifferenceExceeds(FEEDBACK_SEND_TIME.get(), now, interval * 1000)
    {
        return Ok(());
    }
    FEEDBACK_SEND_TIME.set(now);

    let mut msg = Vec::with_capacity(1 + 8 * 4 + 1);
    msg.push(b'r');
    msg.extend_from_slice(&recvpos.to_be_bytes()); // write
    msg.extend_from_slice(&flushpos.to_be_bytes()); // flush
    msg.extend_from_slice(&writepos.to_be_bytes()); // apply
    msg.extend_from_slice(&(now as i64).to_be_bytes());
    msg.push(request_reply as u8);

    let _ = elog::elog(
        DEBUG2,
        format!(
            "sending feedback (force {force}) to recv {recvpos:X}, write {writepos:X}, flush {flushpos:X}"
        ),
    );

    if let Err(e) = conn.put_copy_data(&msg) {
        return elog::elog(ERROR, format!("could not send feedback message: {e}"));
    }

    if recvpos > LAST_RECVPOS.get() {
        LAST_RECVPOS.set(recvpos);
    }
    if writepos > LAST_WRITEPOS.get() {
        LAST_WRITEPOS.set(writepos);
    }
    if flushpos > LAST_FLUSHPOS.get() {
        LAST_FLUSHPOS.set(flushpos);
    }
    Ok(())
}

// NAPTIME_PER_CYCLE (worker.c:189): max sleep between apply-loop cycles, ms.
// The idle wait must time out on this cadence — the housekeeping arm
// (AcceptInvalidationMessages + maybe_reread_subscription) is how a worker
// against a quiet publisher ever notices subscription DDL.
const NAPTIME_PER_CYCLE: i64 = 1000;

// LogicalRepApplyLoop (worker.c:3574), non-streaming subset. The copy-both
// read is the client's get_copy_data; Block means "nothing available now" and
// maps to C's len==0 wait arm.
pub(crate) fn apply_loop(conn: &mut PgConn, mut last_received: XLogRecPtr) -> PgResult<()> {
    let top = MemoryContext::new("ApplyMessageContext");
    // SAFETY: `top` outlives the loop; per-message allocations are reset by
    // the arena when the context drops at function exit.
    let mcx: Mcx<'static> = unsafe { std::mem::transmute(top.mcx()) };

    // wal_receiver_timeout bookkeeping (LogicalRepApplyLoop locals).
    let mut last_recv_timestamp: TimestampTz = get_ts();
    let mut ping_sent = false;

    loop {
        postgres_seams::check_for_interrupts::call()?;

        let msg = match conn.get_copy_data() {
            Ok(m) => m,
            Err(e) => {
                return elog::elog(ERROR, format!("could not receive data from WAL stream: {e}"))
            }
        };

        match msg {
            CopyData::End => {
                let _ = elog::elog(LOG, "data stream from publisher has ended".to_string());
                return Ok(());
            }
            CopyData::Block => {
                // No data right now (C's walrcv_receive == 0 arm): confirm
                // writes; process invals when idle; wait for the socket.
                send_feedback(conn, last_received, false, false)?;

                // C gates this idle work on being outside BOTH a remote
                // transaction and a streamed chunk (worker.c:3720): a chunk's
                // spool transaction stays open until STREAM STOP.
                if !IN_REMOTE_TRANSACTION.get() && !stream_apply::in_streamed_transaction() {
                    inval::local::AcceptInvalidationMessages()?;
                    maybe_reread_subscription(mcx)?;
                    if APPLY_WORKER_EXIT.get() {
                        return Ok(());
                    }
                    // Launch/advance tablesync when idle (worker.c:3735).
                    tablesync::process_syncing_tables(mcx, conn, last_received)?;
                    if APPLY_WORKER_EXIT.get() {
                        return Ok(());
                    }
                }

                // Wait for more data or latch (worker.c:3736): bounded by
                // WalWriterDelay while local commits await flush (feedback
                // urgency), else NAPTIME_PER_CYCLE, so the idle housekeeping
                // above reruns on C's cadence.
                let wait_time = if LSN_MAPPING.with(|m| !m.borrow().is_empty()) {
                    guc_tables::vars::WalWriterDelay.read() as i64
                } else {
                    NAPTIME_PER_CYCLE
                };
                let readable = conn.wait_readable_timeout(wait_time)?;

                if interrupt::ConfigReloadPending() {
                    interrupt::SetConfigReloadPending(false);
                    guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
                }

                if !readable {
                    // WL_TIMEOUT arm (worker.c:3764): nothing new received.
                    // Error out once the publisher has been silent for
                    // wal_receiver_timeout; ping it at the halfway point.
                    let mut request_reply = false;
                    let wrt = guc_tables::vars::wal_receiver_timeout.read();
                    if wrt > 0 {
                        let now = get_ts();
                        // TimestampTzPlusMilliseconds: timestamps are µs.
                        if now >= last_recv_timestamp + wrt as i64 * 1000 {
                            ereport(ERROR)
                                .errcode(ERRCODE_CONNECTION_FAILURE)
                                .errmsg("terminating logical replication worker due to timeout")
                                .finish(loc("LogicalRepApplyLoop"))?;
                        }
                        if !ping_sent && now >= last_recv_timestamp + (wrt / 2) as i64 * 1000 {
                            request_reply = true;
                            ping_sent = true;
                        }
                    }
                    send_feedback(conn, last_received, request_reply, request_reply)?;
                }

                if !conn.consume_input() {
                    return elog::elog(
                        ERROR,
                        format!(
                            "could not receive data from WAL stream: {}",
                            conn.error_message()
                        ),
                    );
                }
            }
            CopyData::Msg(buf) => {
                // Reset the publisher-silence clock (worker.c:3655).
                last_recv_timestamp = get_ts();
                ping_sent = false;
                if buf.is_empty() {
                    continue;
                }
                match buf[0] {
                    b'w' => {
                        if buf.len() < 1 + 24 {
                            return elog::elog(
                                ERROR,
                                "invalid WAL message received from primary".to_string(),
                            );
                        }
                        let start_lsn = u64::from_be_bytes(buf[1..9].try_into().unwrap());
                        let end_lsn = u64::from_be_bytes(buf[9..17].try_into().unwrap());
                        if last_received < start_lsn {
                            last_received = start_lsn;
                        }
                        if last_received < end_lsn {
                            last_received = end_lsn;
                        }
                        apply::apply_dispatch(mcx, conn, &buf[25..])?;
                        if APPLY_WORKER_EXIT.get() {
                            return Ok(());
                        }
                    }
                    b'k' => {
                        if buf.len() < 1 + 17 {
                            continue;
                        }
                        let end_lsn = u64::from_be_bytes(buf[1..9].try_into().unwrap());
                        let reply_requested = buf[17] != 0;
                        if last_received < end_lsn {
                            last_received = end_lsn;
                        }
                        send_feedback(conn, last_received, reply_requested, false)?;
                    }
                    // Other message types are purposefully ignored.
                    _ => {}
                }
                // Confirm writes after processing (C sends per outer
                // iteration; the interval limiter makes this equivalent).
                send_feedback(conn, last_received, false, false)?;
            }
        }
    }
}

// libpqrcv_startstreaming's logical arm (libpqwalreceiver.c:554).
fn start_logical_streaming(
    conn: &mut PgConn,
    startpos: XLogRecPtr,
    two_phase: bool,
) -> PgResult<()> {
    let slot = my_sub(|s| s.slotname.clone().expect("slotname checked by caller"));
    start_logical_streaming_opts(conn, &slot, startpos, two_phase)
}

// Same, on an explicit slot (the tablesync catchup stream).
pub(crate) fn start_logical_streaming_on(
    conn: &mut PgConn,
    slotname: &str,
    startpos: XLogRecPtr,
) -> PgResult<()> {
    start_logical_streaming_opts(conn, slotname, startpos, false)
}

fn start_logical_streaming_opts(
    conn: &mut PgConn,
    slotname: &str,
    startpos: XLogRecPtr,
    two_phase: bool,
) -> PgResult<()> {
    let slot = slotname.to_string();
    let (publications, binary, origin_opt, stream_mode) = my_sub(|s| {
        (
            s.publications.clone(),
            s.binary,
            s.origin.clone(),
            s.stream,
        )
    });

    // set_stream_options (worker.c:4429): streaming!=off requests the serial
    // streamed-apply path. streaming=parallel — CREATE SUBSCRIPTION's DEFAULT
    // (subscriptioncmds.c:154) — applies serially: applyparallelworker.c is
    // not ported, and C's own leader serializes a parallel-mode transaction
    // whenever no parallel worker is available, so the serial path is inside
    // C's behavior space. Logged per worker start so the divergence is
    // visible. two_phase is requested only by run_apply_worker's
    // PENDING->ENABLED transition.
    if stream_mode == pg_subscription::LOGICALREP_STREAM_PARALLEL {
        let _ = elog::elog(
            LOG,
            "parallel streaming apply is not implemented; applying streamed transactions serially"
                .to_string(),
        );
    }
    let pubnames = publications
        .iter()
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(",");
    let pubnames_literal = format!("'{}'", pubnames.replace('\'', "''"));
    let mut cmd = format!(
        "START_REPLICATION SLOT \"{}\" LOGICAL {:X}/{:X} (proto_version '4'",
        slot.replace('"', "\"\""),
        (startpos >> 32) as u32,
        startpos as u32
    );
    if origin_opt != "any" {
        cmd.push_str(&format!(", origin '{}'", origin_opt.replace('\'', "''")));
    }
    cmd.push_str(&format!(", publication_names {pubnames_literal}"));
    if binary {
        cmd.push_str(", binary 'true'");
    }
    if stream_mode != pg_subscription::LOGICALREP_STREAM_OFF {
        cmd.push_str(", streaming 'on'");
    }
    if two_phase {
        cmd.push_str(", two_phase 'on'");
    }
    cmd.push(')');

    let res = conn.exec(&cmd)?;
    if res.status != walreceiver::client::ExecStatus::CopyBoth {
        return elog::elog(ERROR, format!("could not start WAL streaming: {}", res.err));
    }
    Ok(())
}

// replorigin_reset (worker.c): reset the session-origin advance state.
// Registered before_shmem_exit so that when a dying worker's exit drain
// reaches ShutdownPostgres -> AbortOutOfAnyTransaction, the abort record of
// a half-applied remote transaction does NOT advance the replication origin
// (RecordTransactionAbort's replorigin arm, xact engine): an advanced origin
// for an incomplete transaction loses it — the publisher never resends.
// LIFO order guarantees this runs before ShutdownPostgres, which InitPostgres
// registered earlier (postinit). The shared-memory session registration
// itself is released separately by ReplicationOriginExitCleanup (origin.c
// port), registered on_shmem_exit inside replorigin_session_setup.
fn replorigin_reset(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    origin::set_replorigin_session_origin(types_core::InvalidRepOriginId);
    origin::set_replorigin_session_origin_lsn(InvalidXLogRecPtr);
    origin::set_replorigin_session_origin_timestamp(0);
    Ok(())
}

// ApplyWorkerMain (worker.c:4818) + InitializeLogRepWorker + run_apply_worker,
// non-tablesync subset. main_arg = launcher worker-slot index.
pub fn ApplyWorkerMain(main_arg: u64) -> PgResult<()> {
    let slot = main_arg as usize;
    launcher::logicalrep_worker_attach(slot)?;

    // SetupApplyOrSyncWorker (worker.c:4784): SIGHUP reloads config; the
    // apply loop's idle arm consumes ConfigReloadPending. SIGTERM keeps the
    // bgworker default (bgworker_die; C installs die — same FATAL exit).
    procsignal::pqsignal_thread(
        procsignal::signums::SIGHUP,
        procsignal::ThreadSignalHandler::Simple(interrupt::SignalHandlerForConfigReload),
    );

    let result = apply_worker_body(slot);

    // Detach on every exit path; the launcher notices and relaunches.
    launcher::logicalrep_worker_detach();
    result
}

fn apply_worker_body(slot: usize) -> PgResult<()> {
    let w = launcher::worker_snapshot(slot).expect("attached worker slot");

    // InitializeLogRepWorker: database connection + subscription load.
    bgworker::BackgroundWorkerInitializeConnectionByOid(w.dbid, w.userid, 0)?;

    let top = MemoryContext::new("ApplyContext");
    // SAFETY: `top` outlives the worker body; see apply_loop.
    let mcx: Mcx<'static> = unsafe { std::mem::transmute(top.mcx()) };

    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::SUBSCRIPTIONOID,
        subscription_change_cb,
        datum::Datum::null(),
    )?;
    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::SUBSCRIPTIONRELMAP,
        tablesync::invalidate_table_states_cb,
        datum::Datum::null(),
    )?;

    xact::StartTransactionCommand()?;
    // InitializeLogRepWorker (worker.c:4688): lock the subscription to
    // prevent it from being concurrently dropped, then re-verify its
    // existence. Without this, a worker relaunched while a DROP/ALTER
    // SUBSCRIPTION transaction is still in flight reads the pre-commit
    // catalog image and can acquire the replication origin mid-DROP — the
    // origin drop then waits forever on a worker that never wakes.
    lmgr::LockSharedObject(
        pg_subscription::SubscriptionRelationId,
        w.subid,
        0,
        types_rel::AccessShareLock,
    )?;
    let Some(sub) = load_subscription(mcx, w.subid)? else {
        let _ = elog::elog(
            LOG,
            format!(
                "logical replication worker for subscription {} will not start because the subscription was removed during startup",
                w.subid
            ),
        );
        // Ensure we remove the no-longer-useful entry for the worker's start
        // time (worker.c:4700), so a successor for a recreated subscription
        // isn't throttled by this incarnation's timestamp.
        if !w.is_tablesync() {
            launcher::ApplyLauncherForgetWorkerStartTime(w.subid);
        }
        xact::CommitTransactionCommand()?;
        return Ok(());
    };
    if !sub.enabled {
        let _ = elog::elog(
            LOG,
            format!(
                "logical replication worker for subscription \"{}\" will not start because the subscription was disabled during startup",
                sub.name
            ),
        );
        xact::CommitTransactionCommand()?;
        return Ok(());
    }
    let subname = sub.name.clone();
    MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(sub));
    xact::CommitTransactionCommand()?;

    // InitializeLogRepWorker tail (worker.c:4761): register the origin-state
    // reset before any remote transaction can be applied — even a LOG line
    // after the origin state is set may process a shutdown signal before the
    // current apply operation commits. Registered here so both apply and
    // tablesync workers are protected (C's comment). The checkpointer-class
    // precedent for worker exit hooks: launcher's logicalrep_worker_onexit
    // (on_shmem_exit at attach) and checkpointer's
    // pgstat_before_server_shutdown_cb (before_shmem_exit).
    ipc::before_shmem_exit(replorigin_reset, datum::Datum::null())?;

    if w.is_tablesync() {
        let _ = elog::elog(
            LOG,
            format!(
                "logical replication table synchronization worker for subscription \"{subname}\", relation OID {} has started",
                w.relid
            ),
        );
        apply::logicalrep_relmap_prepare()?;
        let r = tablesync::run_tablesync_worker(mcx, w.relid);
        let _ = origin::replorigin_session_reset();
        return r;
    }

    let _ = elog::elog(
        LOG,
        format!("logical replication apply worker for subscription \"{subname}\" has started"),
    );

    // run_apply_worker (worker.c:4546).
    if my_sub(|s| s.slotname.is_none()) {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("subscription has no replication slot set")
            .finish(loc("run_apply_worker"))?;
        unreachable!();
    }

    if my_sub(|s| s.skiplsn) != InvalidXLogRecPtr {
        panic!("unported: ALTER SUBSCRIPTION ... SKIP (subskiplsn set)");
    }

    // Set up replication-origin tracking; the session origin rides each
    // commit record so restart positions survive crashes.
    let originname = format!("pg_{}", my_sub(|s| s.oid));
    xact::StartTransactionCommand()?;
    let mut originid = origin::replorigin_by_name(&originname, true)?;
    if originid == types_core::InvalidRepOriginId {
        originid = origin::replorigin_create(mcx, &originname)?;
    }
    origin::replorigin_session_setup(originid, 0)?;
    origin::set_replorigin_session_origin(originid);
    let origin_startpos = origin::replorigin_session_get_progress(false)?;
    xact::CommitTransactionCommand()?;

    let must_use_password = my_sub(|s| s.passwordrequired && !s.ownersuperuser);
    let (conninfo, name) = my_sub(|s| (s.conninfo.clone(), s.name.clone()));
    let mut conn = match walreceiver::client::connect_extended(
        &conninfo,
        true,
        true,
        must_use_password,
        &name,
    )? {
        Ok(c) => c,
        Err(e) => {
            ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "apply worker for subscription \"{name}\" could not connect to the publisher: {e}"
                ))
                .finish(loc("run_apply_worker"))?;
            unreachable!();
        }
    };

    // IDENTIFY_SYSTEM does some initialization upstream.
    let _ = walreceiver::client::identify_system(&mut conn)?;

    // run_apply_worker (worker.c:4546): decide whether to enable two_phase
    // now — the subscription is PENDING and every tablesync is READY.
    let enable_two_phase = my_sub(|s| s.twophasestate)
        == pg_subscription::LOGICALREP_TWOPHASE_STATE_PENDING
        && tablesync::all_tablesyncs_ready(mcx)?;

    apply::logicalrep_relmap_prepare()?;

    start_logical_streaming(&mut conn, origin_startpos, enable_two_phase)?;

    if enable_two_phase {
        // C: streaming started with two_phase; persist ENABLED so a restart
        // does not re-request it.
        xact::StartTransactionCommand()?;
        // C: pg_subscription update might involve TOAST access — snapshot.
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
        pg_subscription::UpdateTwoPhaseState(
            mcx,
            my_sub(|s| s.oid),
            pg_subscription::LOGICALREP_TWOPHASE_STATE_ENABLED,
        )?;
        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
        MY_SUBSCRIPTION.with(|s| {
            if let Some(s) = s.borrow_mut().as_mut() {
                s.twophasestate = pg_subscription::LOGICALREP_TWOPHASE_STATE_ENABLED;
            }
        });
        let name = my_sub(|s| s.name.clone());
        let _ = elog::elog(
            LOG,
            format!("logical replication apply worker for subscription \"{name}\" two_phase is ENABLED"),
        );
    }

    // start_apply (worker.c:4506).
    let r = apply_loop(&mut conn, origin_startpos);

    // Session origin teardown so a relaunched worker can re-acquire.
    let _ = origin::replorigin_session_reset();
    r
}

pub fn init_seams() {
    logical_worker_seams::apply_worker_main::set(ApplyWorkerMain);
}
