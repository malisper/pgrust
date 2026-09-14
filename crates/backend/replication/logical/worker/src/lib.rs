// worker.c (replication/logical): the leader apply worker — connect to the
// publisher, START_REPLICATION ... LOGICAL with pgoutput, and the apply loop
// dispatching BEGIN/COMMIT/ORIGIN/INSERT/UPDATE/DELETE/TRUNCATE/RELATION/
// TYPE/MESSAGE. Non-streaming, non-two-phase, non-tablesync subset; the
// excluded paths refuse loudly (never silently skip).
//
// Renderings vs C (documented per lane convention):
// - Per-worker file-statics are thread-locals (thread-model).
// - Conflict reporting (ReportApplyConflict/conflict stats) is rendered as
//   single LOG lines (msg + C's DETAIL sentence) for the update/delete
//   missing and origin-differs cases; the tuple-content display half of C's
//   DETAIL is not rendered.
// - The DirtySnapshot in the replica-identity lookups is rendered as
//   GetLatestSnapshot + the C lock-retry protocol (see apply.rs).
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};

use elog::ereport;
use logicalproto::LogicalRepRelation;
use mcx::{Mcx, MemoryContext};
use types_core::{InvalidTransactionId, InvalidXLogRecPtr, Oid, TimestampTz, TransactionId, XLogRecPtr};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG2, ERRCODE_ADMIN_SHUTDOWN, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROTOCOL_VIOLATION, ERROR, FATAL, LOG, WARNING,
};

use walreceiver::client::{CopyData, PgConn};

mod apply;
mod conflict;
mod parallel;
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

pub(crate) fn get_ts() -> TimestampTz {
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
    // synchronous_commit the worker runs with (worker.c:4734, :4092).
    pub synccommit: String,
    pub skiplsn: XLogRecPtr,
    pub owner: Oid,
    pub ownersuperuser: bool,
    pub passwordrequired: bool,
    pub runasowner: bool,
    pub failover: bool,
    // disable_on_error: an apply/tablesync error disables the subscription
    // instead of crash-looping the worker (worker.c:4537).
    pub disableonerr: bool,
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
    // skip_xact_finish_lsn (worker.c:329): valid while the whole remote
    // transaction whose finish LSN it holds is being skipped
    // (ALTER SUBSCRIPTION ... SKIP).
    static SKIP_XACT_FINISH_LSN: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    // apply_error_callback_arg (worker.c:272): the apply error callback's
    // state, filled while applying a change.
    static APPLY_ERROR_CALLBACK_ARG: RefCell<ApplyErrorCallbackArg> =
        const { RefCell::new(ApplyErrorCallbackArg::INITIAL) };
}

// ApplyErrorCallbackArg (worker.c:213). `rel` is the remote relation of the
// LogicalRepRelMapEntry C points at (its nspname/relname/attnames are all the
// callback reads); `command` 0 means no message is being applied.
pub(crate) struct ApplyErrorCallbackArg {
    pub command: u8,
    pub rel: Option<LogicalRepRelation>,
    // Remote attribute number being processed, -1 if not applicable.
    pub remote_attnum: i32,
    pub remote_xid: TransactionId,
    // Remote transaction's finish WAL location (InvalidXLogRecPtr while
    // unknown, as in a streamed chunk).
    pub finish_lsn: XLogRecPtr,
    pub origin_name: Option<String>,
}

impl ApplyErrorCallbackArg {
    // worker.c:272-280: the static initializer.
    pub const INITIAL: ApplyErrorCallbackArg = ApplyErrorCallbackArg {
        command: 0,
        rel: None,
        remote_attnum: -1,
        remote_xid: InvalidTransactionId,
        finish_lsn: InvalidXLogRecPtr,
        origin_name: None,
    };
}

// apply_error_callback's six errcontext forms (worker.c:5053-5121); None when
// no command is being applied (command == 0), which is C's early return.
pub(crate) fn apply_error_context_line(errarg: &ApplyErrorCallbackArg) -> Option<String> {
    if errarg.command == 0 {
        return None;
    }
    // C asserts origin_name; a NULL %s prints "(null)" (snprintf.c).
    let origin = errarg.origin_name.as_deref().unwrap_or("(null)");
    let msgtype = logicalproto::logicalrep_message_type(errarg.command);
    let xid = errarg.remote_xid;
    let lsn = format!("{:X}/{:X}", (errarg.finish_lsn >> 32) as u32, errarg.finish_lsn as u32);
    let lsn_valid = errarg.finish_lsn != InvalidXLogRecPtr;
    Some(match &errarg.rel {
        None => {
            if xid == InvalidTransactionId {
                format!(
                    "processing remote data for replication origin \"{origin}\" during message type \"{msgtype}\""
                )
            } else if !lsn_valid {
                format!(
                    "processing remote data for replication origin \"{origin}\" during message type \"{msgtype}\" in transaction {xid}"
                )
            } else {
                format!(
                    "processing remote data for replication origin \"{origin}\" during message type \"{msgtype}\" in transaction {xid}, finished at {lsn}"
                )
            }
        }
        Some(rel) => {
            let (nsp, name) = (&rel.nspname, &rel.relname);
            if errarg.remote_attnum < 0 {
                if !lsn_valid {
                    format!(
                        "processing remote data for replication origin \"{origin}\" during message type \"{msgtype}\" for replication target relation \"{nsp}.{name}\" in transaction {xid}"
                    )
                } else {
                    format!(
                        "processing remote data for replication origin \"{origin}\" during message type \"{msgtype}\" for replication target relation \"{nsp}.{name}\" in transaction {xid}, finished at {lsn}"
                    )
                }
            } else {
                // C indexes attnames[remote_attnum] unchecked; the sites
                // that set it have bounds-checked it against the received
                // tuple (tuple_column_check), so a miss here is impossible —
                // render it empty rather than panic.
                let col = rel
                    .attnames
                    .get(errarg.remote_attnum as usize)
                    .map(String::as_str)
                    .unwrap_or("");
                if !lsn_valid {
                    format!(
                        "processing remote data for replication origin \"{origin}\" during message type \"{msgtype}\" for replication target relation \"{nsp}.{name}\" column \"{col}\" in transaction {xid}"
                    )
                } else {
                    format!(
                        "processing remote data for replication origin \"{origin}\" during message type \"{msgtype}\" for replication target relation \"{nsp}.{name}\" column \"{col}\" in transaction {xid}, finished at {lsn}"
                    )
                }
            }
        }
    })
}

// apply_error_callback (worker.c:5053): the errcontext line for the change
// being applied, read from the callback arg at report time.
pub(crate) fn apply_error_callback(e: &mut PgError) {
    let line = APPLY_ERROR_CALLBACK_ARG.with(|a| apply_error_context_line(&a.borrow()));
    if let Some(line) = line {
        e.add_context_line(line);
    }
}

// The pushed error-context frame (worker.c:3616-3622 / applyparallelworker.c
// :749-754): non-ERROR reports emitted inside the apply loop (LOG/WARNING/
// FATAL, decorated by errfinish at emit time) get the line through the
// emit-context callback; an ERROR propagating out as `Err` gets it through
// `attach` at the loop boundary (C's callback runs at the throw site with the
// same arg state — nothing between the throw and the loop exit touches it).
// Drop pops the frame (worker.c:3839 / applyparallelworker.c:839).
pub(crate) struct ApplyErrorContextFrame {
    callback: u64,
}

impl ApplyErrorContextFrame {
    pub(crate) fn push() -> Self {
        ApplyErrorContextFrame {
            callback: elog::push_emit_context_callback(Box::new(apply_error_callback)),
        }
    }

    pub(crate) fn attach<T>(&self, r: PgResult<T>) -> PgResult<T> {
        r.map_err(|mut e| {
            apply_error_callback(&mut e);
            e
        })
    }
}

impl Drop for ApplyErrorContextFrame {
    fn drop(&mut self) {
        elog::pop_emit_context_callback(self.callback);
    }
}

// set_apply_error_context_xact (worker.c:5125).
pub(crate) fn set_apply_error_context_xact(xid: TransactionId, lsn: XLogRecPtr) {
    APPLY_ERROR_CALLBACK_ARG.with(|a| {
        let mut a = a.borrow_mut();
        a.remote_xid = xid;
        a.finish_lsn = lsn;
    });
}

// reset_apply_error_context_info (worker.c:5133).
pub(crate) fn reset_apply_error_context_info() {
    APPLY_ERROR_CALLBACK_ARG.with(|a| {
        let mut a = a.borrow_mut();
        a.command = 0;
        a.rel = None;
        a.remote_attnum = -1;
        a.remote_xid = InvalidTransactionId;
        a.finish_lsn = InvalidXLogRecPtr;
    });
}

// set_apply_error_context_origin (worker.c:5195): C strdup's the name into
// ApplyContext (worker lifetime).
pub(crate) fn set_apply_error_context_origin(originname: &str) {
    APPLY_ERROR_CALLBACK_ARG.with(|a| a.borrow_mut().origin_name = Some(originname.to_string()));
}

// apply_error_callback_arg.rel = rel / NULL (worker.c:2442/2473, 2595/2661,
// 2804/2846).
pub(crate) fn set_apply_error_context_rel(rel: Option<&LogicalRepRelation>) {
    APPLY_ERROR_CALLBACK_ARG.with(|a| a.borrow_mut().rel = rel.cloned());
}

// apply_error_callback_arg.remote_attnum = n / -1 (worker.c:819/868,
// 938/983).
pub(crate) fn set_apply_error_context_attnum(remote_attnum: i32) {
    APPLY_ERROR_CALLBACK_ARG.with(|a| a.borrow_mut().remote_attnum = remote_attnum);
}

// apply_dispatch's command save (worker.c:3393-3394): returns the previous
// command (this is re-entered when applying spooled changes).
pub(crate) fn set_apply_error_context_command(command: u8) -> u8 {
    APPLY_ERROR_CALLBACK_ARG.with(|a| {
        let mut a = a.borrow_mut();
        let saved = a.command;
        a.command = command;
        saved
    })
}

// is_skipping_changes (worker.c:330).
pub(crate) fn is_skipping_changes() -> bool {
    SKIP_XACT_FINISH_LSN.get() != InvalidXLogRecPtr
}

// maybe_start_skipping_changes (worker.c:4904): start skipping if the
// transaction's finish LSN matches the subscription's skiplsn.
pub(crate) fn maybe_start_skipping_changes(finish_lsn: XLogRecPtr) {
    debug_assert!(!is_skipping_changes());
    debug_assert!(!IN_REMOTE_TRANSACTION.get());
    debug_assert!(!stream_apply::in_streamed_transaction());

    // Called for every remote transaction; skipping is rare.
    let skiplsn = my_sub(|s| s.skiplsn);
    if skiplsn == InvalidXLogRecPtr || skiplsn != finish_lsn {
        return;
    }

    SKIP_XACT_FINISH_LSN.set(finish_lsn);
    let _ = elog::elog(
        LOG,
        format!(
            "logical replication starts skipping transaction at LSN {:X}/{:X}",
            (finish_lsn >> 32) as u32,
            finish_lsn as u32
        ),
    );
}

// stop_skipping_changes (worker.c:4931).
pub(crate) fn stop_skipping_changes() {
    if !is_skipping_changes() {
        return;
    }
    let lsn = SKIP_XACT_FINISH_LSN.get();
    let _ = elog::elog(
        LOG,
        format!(
            "logical replication completed skipping transaction at LSN {:X}/{:X}",
            (lsn >> 32) as u32,
            lsn as u32
        ),
    );
    SKIP_XACT_FINISH_LSN.set(InvalidXLogRecPtr);
}

// clear_subscription_skip_lsn (worker.c:4955): the catalog half lives in
// pg_subscription::ClearSubscriptionSkipLsn; transaction/snapshot management
// and the mismatch WARNING live here. finish_lsn is the transaction's finish
// LSN: when it doesn't match the cleared skiplsn, warn (e.g. the user
// specified a wrong subskiplsn).
pub(crate) fn clear_subscription_skip_lsn(mcx: Mcx<'_>, finish_lsn: XLogRecPtr) -> PgResult<()> {
    let myskiplsn = my_sub(|s| s.skiplsn);
    if myskiplsn == InvalidXLogRecPtr {
        return Ok(());
    }

    let started_tx = if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
        true
    } else {
        false
    };

    // Updating pg_subscription might involve TOAST table access, so ensure a
    // valid snapshot.
    snapmgr::PushActiveSnapshot(&snapmgr::GetTransactionSnapshot()?)?;

    let (suboid, subname) = my_sub(|s| (s.oid, s.name.clone()));
    let cleared = pg_subscription::ClearSubscriptionSkipLsn(mcx, suboid, &subname, myskiplsn)?;
    if cleared && myskiplsn != finish_lsn {
        let _ = ereport(WARNING)
            .errmsg(format!("skip-LSN of subscription \"{subname}\" cleared"))
            .errdetail(format!(
                "Remote transaction's finish WAL location (LSN) {:X}/{:X} did not match skip-LSN {:X}/{:X}.",
                (finish_lsn >> 32) as u32,
                finish_lsn as u32,
                (myskiplsn >> 32) as u32,
                myskiplsn as u32,
            ))
            .finish(loc("clear_subscription_skip_lsn"));
    }

    snapmgr::PopActiveSnapshot()?;

    if started_tx {
        xact::CommitTransactionCommand()?;
    }
    Ok(())
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
        synccommit: sub.synccommit.as_str().to_string(),
        skiplsn: sub.skiplsn,
        owner: sub.owner,
        ownersuperuser: sub.ownersuperuser,
        passwordrequired: sub.passwordrequired,
        runasowner: sub.runasowner,
        failover: sub.failover,
        disableonerr: sub.disableonerr,
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
                    // password_required governs the publisher session's
                    // authentication: a change must reconnect and re-enforce.
                    || n.passwordrequired != old.passwordrequired
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
        if parallel::am_parallel_apply_worker() && newsub.is_some() {
            // apply_worker_exit's parallel arm (worker.c:3929): don't stop —
            // the leader detects the change and restarts logical replication;
            // stopping here could trip the leader's queue error paths.
            let _ = elog::elog(
                LOG,
                format!(
                    "logical replication parallel apply worker for subscription \"{name}\" will stop because {why}"
                ),
            );
        } else {
            let _ = elog::elog(
                LOG,
                format!(
                    "logical replication worker for subscription \"{name}\" will stop because {why}"
                ),
            );
            APPLY_WORKER_EXIT.set(true);
        }
        return Ok(());
    }

    // worker.c:4064: exit if the subscription owner's superuser privileges
    // have been revoked (an apply/tablesync worker owned by a now-non-superuser
    // must restart so it re-checks password_required and re-authenticates).
    if let Some(n) = &newsub {
        if owner_superuser_revoked(my_sub(|o| o.ownersuperuser), n.ownersuperuser) {
            let name = my_sub(|s| s.name.clone());
            if parallel::am_parallel_apply_worker() {
                let _ = elog::elog(
                    LOG,
                    format!(
                        "logical replication parallel apply worker for subscription \"{name}\" will stop because the subscription owner's superuser privileges have been revoked"
                    ),
                );
            } else {
                let _ = elog::elog(
                    LOG,
                    format!(
                        "logical replication worker for subscription \"{name}\" will restart because the subscription owner's superuser privileges have been revoked"
                    ),
                );
                APPLY_WORKER_EXIT.set(true);
            }
            return Ok(());
        }
    }

    if let Some(n) = newsub {
        let synccommit = n.synccommit.clone();
        MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(n));
        // Change synchronous commit according to the user's wishes
        // (worker.c:4092).
        set_synchronous_commit(&synccommit)?;
    }
    Ok(())
}

// worker.c:4064: a superuser-owned subscription whose owner is no longer a
// superuser must restart.
pub(crate) fn owner_superuser_revoked(old_superuser: bool, new_superuser: bool) -> bool {
    !new_superuser && old_superuser
}

// SetConfigOption("synchronous_commit", MySubscription->synccommit,
// PGC_BACKEND, PGC_S_OVERRIDE) (worker.c:4734, :4092).
fn set_synchronous_commit(synccommit: &str) -> PgResult<()> {
    guc::SetConfigOption(
        "synchronous_commit",
        Some(synccommit),
        types_guc::GucContext::PGC_BACKEND,
        types_guc::GucSource::PGC_S_OVERRIDE,
    )
}

// store_flush_position (worker.c:3548): remember (local commit end, remote
// end). Parallel apply workers skip it — the leader maintains the mapping.
pub(crate) fn store_flush_position(remote_lsn: XLogRecPtr, local_lsn: XLogRecPtr) {
    if parallel::am_parallel_apply_worker() {
        return;
    }
    LSN_MAPPING.with(|m| m.borrow_mut().push((local_lsn, remote_lsn)));
}

// get_flush_position (worker.c:3505): remote positions whose local commits have
// been flushed; also reports whether unflushed commits remain.
fn get_flush_position() -> (XLogRecPtr, XLogRecPtr, bool) {
    get_flush_position_at(transam_xlog::GetFlushRecPtr(None))
}

// The write position is the latest locally committed remote end (the tail
// entry once an unflushed commit is hit), not just the flushed one.
fn get_flush_position_at(local_flush: XLogRecPtr) -> (XLogRecPtr, XLogRecPtr, bool) {
    let mut write = InvalidXLogRecPtr;
    let mut flush = InvalidXLogRecPtr;
    let have_pending = LSN_MAPPING.with(|m| {
        let mut m = m.borrow_mut();
        let mut i = 0;
        while i < m.len() {
            let (local, remote) = m[i];
            write = remote;
            if local <= local_flush {
                flush = remote;
                i += 1;
            } else {
                write = m[m.len() - 1].1;
                break;
            }
        }
        m.drain(..i);
        !m.is_empty()
    });
    (write, flush, have_pending)
}

// LogicalRepApplyLoop's 'w' header (worker.c:3672-3686): pq_getmsgint64 x3
// over the body after the type byte; short data is 08P01. Returns
// (start_lsn, end_lsn, send_time, payload).
pub(crate) fn parse_wal_data_message(
    body: &[u8],
) -> PgResult<(XLogRecPtr, XLogRecPtr, TimestampTz, &[u8])> {
    let mut r = logicalproto::Reader::new(body);
    let start_lsn = r.get_int64()?;
    let end_lsn = r.get_int64()?;
    let send_time = r.get_int64()? as i64;
    Ok((start_lsn, end_lsn, send_time, &body[24..]))
}

// LogicalRepApplyLoop's 'k' body (worker.c:3700-3708): end_lsn, timestamp,
// reply_requested; short data is 08P01.
pub(crate) fn parse_keepalive_message(buf: &[u8]) -> PgResult<(XLogRecPtr, TimestampTz, bool)> {
    let mut r = logicalproto::Reader::new(buf);
    let end_lsn = r.get_int64()?;
    let timestamp = r.get_int64()? as i64;
    let reply_requested = r.get_byte()? != 0;
    Ok((end_lsn, timestamp, reply_requested))
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
        return Err(wal_stream_send_error(&e));
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

// LogicalRepApplyLoop (worker.c:3574). The copy-both read is the client's
// get_copy_data; Block means "nothing available now" and maps to C's len==0
// wait arm. The apply error context callback is pushed for the loop's
// duration (worker.c:3616-3622) and popped on every exit (worker.c:3839).
pub(crate) fn apply_loop(conn: &mut PgConn, last_received: XLogRecPtr) -> PgResult<()> {
    let frame = ApplyErrorContextFrame::push();
    let end_of_stream = frame.attach(apply_loop_guts(conn, last_received))?;
    drop(frame);
    // walrcv_endstreaming (worker.c:3843) once the publisher's CopyDone arrived.
    if end_of_stream {
        walreceiver::client::end_streaming(conn)?;
    }
    Ok(())
}

// libpqrcv_receive (libpqwalreceiver.c:850, 905): a failed PQconsumeInput is
// 08006, a stream that ends in anything but CopyDone/CopyIn is 08P01.
pub(crate) fn wal_stream_receive_error(err: &str, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not receive data from WAL stream: {}", pchomp(err)))
            .with_sqlstate(sqlstate),
    )
}

// libpqrcv_send (libpqwalreceiver.c:929).
pub(crate) fn wal_stream_send_error(err: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not send data to WAL stream: {}", pchomp(err)))
            .with_sqlstate(ERRCODE_CONNECTION_FAILURE),
    )
}

fn pchomp(s: &str) -> &str {
    s.trim_end_matches('\n')
}

fn apply_loop_guts(conn: &mut PgConn, mut last_received: XLogRecPtr) -> PgResult<bool> {
    // The ApplyMessageContext we clean up after each replication protocol
    // message (worker.c:3597-3602): a bump arena released wholesale at every
    // reset (the tcop MessageContext idiom — handlers palloc into it C-style
    // and never pfree).
    let mut top = MemoryContext::new_bump("ApplyMessageContext");

    // wal_receiver_timeout bookkeeping (LogicalRepApplyLoop locals).
    let mut last_recv_timestamp: TimestampTz = get_ts();
    let mut ping_sent = false;

    // Mark as idle, before starting to loop (worker.c:3613).
    apply::report_activity(apply::BackendState::STATE_IDLE);

    loop {
        postgres_seams::check_for_interrupts::call()?;

        // MemoryContextReset(ApplyMessageContext) after every message
        // (worker.c:3718) and at the end of every idle cycle (worker.c:3743):
        // one iteration here is one message or one idle cycle, so the reset
        // sits at its head. Nothing allocated from this context outlives the
        // iteration (the streamed-chunk spool file has its own context, see
        // stream_apply::StreamFd).
        top.reset();
        // SAFETY: `top` outlives the iteration; the handle is re-derived after
        // every reset and never stored past it.
        let mcx: Mcx<'static> = unsafe { std::mem::transmute(top.mcx()) };

        let msg = match conn.get_copy_data() {
            Ok(m) => m,
            Err(e) => {
                return Err(wal_stream_receive_error(&e, ERRCODE_PROTOCOL_VIOLATION));
            }
        };

        match msg {
            CopyData::End => {
                let _ = elog::elog(LOG, "data stream from publisher has ended".to_string());
                return Ok(true);
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
                        return Ok(false);
                    }
                    // Launch/advance tablesync when idle (worker.c:3735).
                    tablesync::process_syncing_tables(mcx, Some(conn), last_received)?;
                    if APPLY_WORKER_EXIT.get() {
                        return Ok(false);
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

                    // worker.c:3833: force reporting so long idle periods do
                    // not delay stats arbitrarily; only outside a transaction.
                    if !xact::IsTransactionState() {
                        pgstat::pending::pgstat_report_stat(true);
                    }
                }

                if !conn.consume_input() {
                    return Err(wal_stream_receive_error(
                        &conn.error_message(),
                        ERRCODE_CONNECTION_FAILURE,
                    ));
                }
            }
            CopyData::Msg(buf) => {
                // worker.c:3663: pending SIGHUP config changes apply before
                // each message, not only after an idle wait.
                if interrupt::ConfigReloadPending() {
                    interrupt::SetConfigReloadPending(false);
                    guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
                }
                // Reset the publisher-silence clock (worker.c:3655).
                last_recv_timestamp = get_ts();
                ping_sent = false;
                // pq_getmsgbyte (worker.c:3675) on an empty message.
                let mut hdr = logicalproto::Reader::new(&buf);
                match hdr.get_byte()? {
                    b'w' => {
                        let (start_lsn, end_lsn, send_time, payload) =
                            parse_wal_data_message(&buf[1..])?;
                        if last_received < start_lsn {
                            last_received = start_lsn;
                        }
                        if last_received < end_lsn {
                            last_received = end_lsn;
                        }
                        launcher::my_worker_update_stats(last_received, send_time, false);
                        apply::apply_dispatch(mcx, Some(conn), payload)?;
                        if APPLY_WORKER_EXIT.get() {
                            return Ok(false);
                        }
                    }
                    b'k' => {
                        let (end_lsn, timestamp, reply_requested) =
                            parse_keepalive_message(&buf[1..])?;
                        if last_received < end_lsn {
                            last_received = end_lsn;
                        }
                        send_feedback(conn, last_received, reply_requested, false)?;
                        launcher::my_worker_update_stats(last_received, timestamp, true);
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

// set_stream_options's proto_version negotiation (worker.c:4463): the newest
// protocol the publisher can speak.
pub(crate) fn logicalrep_proto_version(server_version: i32) -> u32 {
    if server_version >= 160000 {
        // LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM
        4
    } else if server_version >= 150000 {
        // LOGICALREP_PROTO_TWOPHASE_VERSION_NUM
        3
    } else if server_version >= 140000 {
        // LOGICALREP_PROTO_STREAM_VERSION_NUM
        2
    } else {
        // LOGICALREP_PROTO_VERSION_NUM
        1
    }
}

// set_stream_options's streaming-mode negotiation (worker.c:4476): "parallel"
// needs a >= 16 publisher and the parallel stream mode; "on" needs a >= 14
// publisher and any non-off stream mode; otherwise streaming is not requested.
pub(crate) fn logicalrep_streaming_str(
    server_version: i32,
    stream_mode: u8,
) -> Option<&'static str> {
    if server_version >= 160000 && stream_mode == pg_subscription::LOGICALREP_STREAM_PARALLEL {
        Some("parallel")
    } else if server_version >= 140000 && stream_mode != pg_subscription::LOGICALREP_STREAM_OFF {
        Some("on")
    } else {
        None
    }
}

// libpqrcv_startstreaming (libpqwalreceiver.c:630): the logical option list.
// binary, two_phase and origin are version-gated (>= 14, >= 15, >= 16) —
// an older publisher rejects options it does not know.
#[allow(clippy::too_many_arguments)]
fn start_replication_command(
    server_version: i32,
    slot: &str,
    startpos: XLogRecPtr,
    publications: &[String],
    binary: bool,
    origin_opt: &str,
    streaming_str: Option<&str>,
    two_phase: bool,
) -> String {
    let proto_version = logicalrep_proto_version(server_version);
    let pubnames = publications
        .iter()
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(",");
    let mut cmd = format!(
        "START_REPLICATION SLOT \"{}\" LOGICAL {:X}/{:X} (proto_version '{proto_version}'",
        slot.replace('"', "\"\""),
        (startpos >> 32) as u32,
        startpos as u32
    );
    if let Some(streaming) = streaming_str {
        cmd.push_str(&format!(", streaming '{streaming}'"));
    }
    if two_phase && server_version >= 150000 {
        cmd.push_str(", two_phase 'on'");
    }
    if origin_opt != "any" && server_version >= 160000 {
        cmd.push_str(&format!(", origin '{}'", origin_opt.replace('\'', "''")));
    }
    cmd.push_str(&format!(", publication_names '{}'", pubnames.replace('\'', "''")));
    if binary && server_version >= 140000 {
        cmd.push_str(", binary 'true'");
    }
    cmd.push(')');
    cmd
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

    // set_stream_options (worker.c:4452): negotiate proto_version and the
    // streaming mode against the publisher's server version. streaming=parallel
    // (CREATE SUBSCRIPTION's default) requests abort info on the wire and marks
    // this worker parallel-capable, but only against a >= 16 publisher; against
    // an older publisher we fall back to plain "on" (>= 14) or no streaming,
    // and proto_version drops accordingly, or the publisher rejects
    // START_REPLICATION. two_phase is requested only by run_apply_worker's
    // PENDING->ENABLED transition.
    let server_version = conn.server_version();
    let streaming_str = logicalrep_streaming_str(server_version, stream_mode);
    launcher::my_worker_set_parallel_apply(streaming_str == Some("parallel"));
    let cmd = start_replication_command(
        server_version,
        &slot,
        startpos,
        &publications,
        binary,
        &origin_opt,
        streaming_str,
        two_phase,
    );

    let res = conn.exec(&cmd)?;
    if res.status != walreceiver::client::ExecStatus::CopyBoth {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "could not start WAL streaming: {}",
                res.err.trim_end_matches('\n')
            ))
            .finish(loc("libpqrcv_startstreaming"))?;
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

// C ProcessInterrupts' IsLogicalWorker() die arm (postgres.c:3321-3324),
// carried by the SIGTERM disposition in the thread model (bgworker_die
// precedent).
pub(crate) fn logicalrep_worker_die() -> PgResult<()> {
    ereport(FATAL)
        .errcode(ERRCODE_ADMIN_SHUTDOWN)
        .errmsg("terminating logical replication worker due to administrator command")
        .finish(loc("ProcessInterrupts"))
}

// ApplyWorkerMain (worker.c:4818) + InitializeLogRepWorker + run_apply_worker,
// non-tablesync subset. main_arg = launcher worker-slot index.
pub fn ApplyWorkerMain(main_arg: u64) -> PgResult<()> {
    let slot = main_arg as usize;
    // InitializingApplyWorker = true (worker.c:4833) across attach + init;
    // TablesyncWorkerMain (tablesync.c:1780) never sets it.
    if launcher::worker_snapshot(slot).is_some_and(|w| !w.is_tablesync()) {
        launcher::set_initializing_apply_worker(true);
    }
    // logicalrep_worker_onexit's stream fileset removal (launcher.c:836-838)
    // is the worker crate's own before_shmem_exit callback; registered ahead
    // of the attach so it drains right after the launcher's onexit.
    ipc::before_shmem_exit(stream_apply::stream_fileset_delete_on_exit, datum::Datum::null())?;
    launcher::logicalrep_worker_attach(slot)?;
    // worker.c:4809: stats start at a sane value, not NULL until a message.
    launcher::my_worker_init_stats_times();

    // SetupApplyOrSyncWorker (worker.c:4784): SIGHUP reloads config; the
    // apply loop's idle arm consumes ConfigReloadPending. SIGTERM: C installs
    // die and ProcessInterrupts' IsLogicalWorker arm (postgres.c:3321-3324)
    // stamps the logical-worker message; here the disposition itself carries
    // that arm (same FATAL 57P01 exit as the bgworker default it replaces).
    procsignal::pqsignal_thread(
        procsignal::signums::SIGHUP,
        procsignal::ThreadSignalHandler::Simple(interrupt::SignalHandlerForConfigReload),
    );
    procsignal::pqsignal_thread(
        procsignal::signums::SIGTERM,
        procsignal::ThreadSignalHandler::Fallible(logicalrep_worker_die),
    );

    let result = apply_worker_body(slot);

    // Stop reading the parallel workers' error mailboxes before the detach
    // SIGTERMs them (logicalrep_worker_detach's leader arm), then detach on
    // every exit path; the launcher notices and relaunches.
    parallel::pa_detach_all_error_mq();
    launcher::logicalrep_worker_detach();
    parallel::pa_release_pool_dsm();
    result
}

// InitializeLogRepWorker (worker.c:4658), shared by the leader apply,
// tablesync, and parallel apply workers. Returns None (after its own LOG)
// when the subscription was removed/disabled during startup.
// InitializeLogRepWorker (worker.c:4751): the table synchronization worker's
// start line names the table via get_rel_name; C's %s of a NULL name (the
// relation vanished between launch and lookup) prints "(null)" (snprintf.c).
pub(crate) fn tablesync_worker_started_message(subname: &str, relname: Option<&str>) -> String {
    format!(
        "logical replication table synchronization worker for subscription \"{subname}\", table \"{}\" has started",
        relname.unwrap_or("(null)")
    )
}

// InitializeLogRepWorker (worker.c:4757).
pub(crate) fn apply_worker_started_message(subname: &str) -> String {
    format!("logical replication apply worker for subscription \"{subname}\" has started")
}

pub(crate) fn initialize_logrep_worker(
    mcx: Mcx<'static>,
    w: &launcher::LogicalRepWorker,
) -> PgResult<Option<String>> {
    // Run as the replica session replication role BEFORE connecting, so
    // every apply-path trigger and rewrite rule sees `replica` — ENABLE
    // REPLICA / ENABLE ALWAYS triggers fire, plain (ENABLE ORIGIN) ones stay
    // silent, exactly as in C.
    guc::SetConfigOption(
        "session_replication_role",
        Some("replica"),
        types_guc::GucContext::PGC_SUSET,
        types_guc::GucSource::PGC_S_OVERRIDE,
    )?;

    // Database connection + subscription load.
    bgworker::BackgroundWorkerInitializeConnectionByOid(w.dbid, w.userid, 0)?;

    // worker.c:4691: always-secure search path, so malicious users can't
    // redirect user code (e.g. pg_index.indexprs).
    guc::SetConfigOption(
        "search_path",
        Some(""),
        types_guc::GucContext::PGC_SUSET,
        types_guc::GucSource::PGC_S_OVERRIDE,
    )?;

    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::SUBSCRIPTIONOID,
        subscription_change_cb,
        datum::Datum::null(),
    )?;
    // worker.c:4745: also re-read the subscription when the owner role's
    // catalog row changes, so a revoked superuser attribute (or altered
    // password requirement) is noticed and acted on by maybe_reread_subscription.
    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::AUTHOID,
        subscription_change_cb,
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
        if !w.is_tablesync() && !w.is_parallel_apply() {
            launcher::ApplyLauncherForgetWorkerStartTime(w.subid);
        }
        xact::CommitTransactionCommand()?;
        return Ok(None);
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
        return Ok(None);
    }
    let subname = sub.name.clone();
    let synccommit = sub.synccommit.clone();
    MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(sub));

    // Setup synchronous commit according to the user's wishes
    // (worker.c:4734).
    set_synchronous_commit(&synccommit)?;

    // worker.c:4751: announce the start while the transaction is still open —
    // the tablesync line resolves the table name through the syscache
    // (get_rel_name); every worker kind that runs InitializeLogRepWorker logs
    // here, the parallel apply worker included.
    if w.is_tablesync() {
        let relname = lsyscache::get_rel_name(mcx, w.relid)?;
        let _ = elog::elog(
            LOG,
            tablesync_worker_started_message(&subname, relname.as_ref().map(|s| s.as_str())),
        );
    } else {
        let _ = elog::elog(LOG, apply_worker_started_message(&subname));
    }

    xact::CommitTransactionCommand()?;

    // InitializeLogRepWorker tail (worker.c:4761): register the origin-state
    // reset before any remote transaction can be applied — even a LOG line
    // after the origin state is set may process a shutdown signal before the
    // current apply operation commits. Registered here so both apply and
    // tablesync workers are protected (C's comment). The checkpointer-class
    // precedent for worker exit hooks: launcher's logicalrep_worker_onexit
    // (before_shmem_exit at attach, launcher.c:744) and checkpointer's
    // pgstat_before_server_shutdown_cb (before_shmem_exit).
    ipc::before_shmem_exit(replorigin_reset, datum::Datum::null())?;

    Ok(Some(subname))
}

fn apply_worker_body(slot: usize) -> PgResult<()> {
    let w = launcher::worker_snapshot(slot).expect("attached worker slot");

    let top = MemoryContext::new("ApplyContext");
    // SAFETY: `top` outlives the worker body; see apply_loop.
    let mcx: Mcx<'static> = unsafe { std::mem::transmute(top.mcx()) };

    if initialize_logrep_worker(mcx, &w)?.is_none() {
        return Ok(());
    }
    // worker.c:4837: initialized — session locks may be taken from here on,
    // and the exit callback releases them.
    launcher::set_initializing_apply_worker(false);

    inval::invalidate::CacheRegisterSyscacheCallback(
        cache_syscache::cacheinfo::SUBSCRIPTIONRELMAP,
        tablesync::invalidate_table_states_cb,
        datum::Datum::null(),
    )?;

    if w.is_tablesync() {
        apply::logicalrep_relmap_prepare()?;
        let r = tablesync::run_tablesync_worker(mcx, w.relid);
        let r = match r {
            Err(e) => finish_apply_error(mcx, e, true),
            ok => ok,
        };
        let _ = origin::replorigin_session_reset();
        return r;
    }

    // run_apply_worker (worker.c:4546).
    if my_sub(|s| s.slotname.is_none()) {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("subscription has no replication slot set")
            .finish(loc("run_apply_worker"))?;
        unreachable!();
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

    // run_apply_worker (worker.c:4616).
    set_apply_error_context_origin(&originname);

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
    let r = match r {
        Err(e) => finish_apply_error(mcx, e, false),
        ok => ok,
    };

    // Session origin teardown so a relaunched worker can re-acquire.
    let _ = origin::replorigin_session_reset();
    r
}

// start_apply / start_table_sync PG_CATCH arm (worker.c:4527, :4478): reset
// the origin advance state so a failed apply cannot advance origin progress
// (the publisher never resends that transaction), then either disable the
// subscription and exit cleanly (disable_on_error) or report the failure and
// re-throw.
fn finish_apply_error(
    mcx: Mcx<'static>,
    err: Box<types_error::PgError>,
    is_tablesync: bool,
) -> PgResult<()> {
    let _ = replorigin_reset(0, datum::Datum::null());
    let (subid, disableonerr) = my_sub(|s| (s.oid, s.disableonerr));
    if disableonerr {
        return disable_subscription_and_exit(mcx, err, subid, is_tablesync);
    }
    // Report the worker failed while applying changes. Abort the current
    // transaction so that the stats message is sent in an idle state
    // (worker.c:4541).
    xact::AbortOutOfAnyTransaction()?;
    pgstat::subscription::pgstat_report_subscription_error(subid, !is_tablesync);
    Err(err)
}

// DisableSubscriptionAndExit (worker.c:4849): emit the error, recover to an
// idle state, disable the subscription in a new transaction and exit cleanly
// (the launcher will not restart a disabled subscription's worker).
fn disable_subscription_and_exit(
    mcx: Mcx<'static>,
    err: Box<types_error::PgError>,
    subid: types_core::Oid,
    is_tablesync: bool,
) -> PgResult<()> {
    // Emit the error message, and recover from the error state to an idle
    // state.
    elog::emit_error_report_for(&err);
    xact::AbortOutOfAnyTransaction()?;

    // Report the worker failed during either table synchronization or apply.
    pgstat::subscription::pgstat_report_subscription_error(subid, !is_tablesync);

    // Disable the subscription. Updating pg_subscription might involve TOAST
    // table access, so ensure we have a valid snapshot.
    xact::StartTransactionCommand()?;
    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;
    pg_subscription::DisableSubscription(mcx, subid)?;
    snapmgr::PopActiveSnapshot()?;
    xact::CommitTransactionCommand()?;

    // Ensure we remove no-longer-useful entry for worker's start time.
    if !is_tablesync {
        launcher::ApplyLauncherForgetWorkerStartTime(subid);
    }

    // Notify the subscription has been disabled and exit.
    let name = my_sub(|s| s.name.clone());
    let _ = elog::elog(
        LOG,
        format!("subscription \"{name}\" has been disabled because of an error"),
    );
    Ok(())
}

pub fn init_seams() {
    logical_worker_seams::apply_worker_main::set(ApplyWorkerMain);
    parallel::init_seams();
}
