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

fn loc(func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/replication/logical/worker.c", 0, func)
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

// LogicalRepApplyLoop (worker.c:3574), non-streaming subset. The copy-both
// read is the client's get_copy_data; Block means "nothing available now" and
// maps to C's len==0 wait arm.
fn apply_loop(conn: &mut PgConn, mut last_received: XLogRecPtr) -> PgResult<()> {
    let top = MemoryContext::new("ApplyMessageContext");
    // SAFETY: `top` outlives the loop; per-message allocations are reset by
    // the arena when the context drops at function exit.
    let mcx: Mcx<'static> = unsafe { std::mem::transmute(top.mcx()) };

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

                if !IN_REMOTE_TRANSACTION.get() {
                    inval::local::AcceptInvalidationMessages()?;
                    maybe_reread_subscription(mcx)?;
                    if APPLY_WORKER_EXIT.get() {
                        return Ok(());
                    }
                }

                conn.wait_readable()?;
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
fn start_logical_streaming(conn: &mut PgConn, startpos: XLogRecPtr) -> PgResult<()> {
    let (slot, publications, binary, origin_opt) = my_sub(|s| {
        (
            s.slotname.clone().expect("slotname checked by caller"),
            s.publications.clone(),
            s.binary,
            s.origin.clone(),
        )
    });

    // Streaming and two-phase are never requested: the pgrust subscriber
    // handles neither yet.
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
    cmd.push(')');

    let res = conn.exec(&cmd)?;
    if res.status != walreceiver::client::ExecStatus::CopyBoth {
        return elog::elog(ERROR, format!("could not start WAL streaming: {}", res.err));
    }
    Ok(())
}

// ApplyWorkerMain (worker.c:4818) + InitializeLogRepWorker + run_apply_worker,
// non-tablesync subset. main_arg = launcher worker-slot index.
pub fn ApplyWorkerMain(main_arg: u64) -> PgResult<()> {
    let slot = main_arg as usize;
    launcher::logicalrep_worker_attach(slot)?;

    let result = apply_worker_body(slot);

    // Detach on every exit path; the launcher notices and relaunches.
    launcher::logicalrep_worker_detach();
    result
}

fn apply_worker_body(slot: usize) -> PgResult<()> {
    let w = launcher::worker_snapshot(slot).expect("attached worker slot");

    if w.is_tablesync() {
        panic!("unported: tablesync worker (round-4 inc E)");
    }

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

    xact::StartTransactionCommand()?;
    let Some(sub) = load_subscription(mcx, w.subid)? else {
        let _ = elog::elog(
            LOG,
            format!(
                "logical replication worker for subscription {} will not start because the subscription was removed during startup",
                w.subid
            ),
        );
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

    // Two-phase stays PENDING/DISABLED here: the pending->enabled transition
    // needs tablesync (AllTablesyncsReady), inc E.

    apply::logicalrep_relmap_prepare()?;

    start_logical_streaming(&mut conn, origin_startpos)?;

    // start_apply (worker.c:4506).
    let r = apply_loop(&mut conn, origin_startpos);

    // Session origin teardown so a relaunched worker can re-acquire.
    let _ = origin::replorigin_session_reset();
    r
}

pub fn init_seams() {
    logical_worker_seams::apply_worker_main::set(ApplyWorkerMain);
}
