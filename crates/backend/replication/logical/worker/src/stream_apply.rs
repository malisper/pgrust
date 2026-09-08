// Streamed-transaction apply (worker.c stream handlers, all TransApplyAction
// arms): in-progress transactions arrive as STREAM START/STOP chunks. The
// leader either serializes the chunk's data messages — stripped of their
// per-change (sub)txn xid — into a per-(subscription, xid) fileset file
// (TRANS_LEADER_SERIALIZE, replayed at STREAM COMMIT), or forwards them to a
// parallel apply worker (TRANS_LEADER_SEND_TO_PARALLEL), falling back to
// spooling the remainder WITH xids for the parallel worker to replay
// (TRANS_LEADER_PARTIAL_SERIALIZE) when the queue backs up. A parallel apply
// worker itself runs the TRANS_PARALLEL_APPLY arms. See parallel.rs
// (applyparallelworker.c) for the worker pool and the locking protocol.

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use elog::ereport;
use fd::{
    BufFile, BufFileCreateFileSet, BufFileDeleteFileSet, BufFileOpenFileSet,
    BufFileOpenFileSetMaybe, FileSet,
};
use mcx::{Mcx, MemoryContext};
use types_core::{InvalidTransactionId, InvalidXLogRecPtr, Oid, TransactionId, XLogRecPtr};
use types_error::{
    PgResult, DEBUG1, ERRCODE_DATA_CORRUPTED, ERRCODE_PROTOCOL_VIOLATION, ERROR,
};
use types_rel::AccessExclusiveLock;
use walreceiver::client::PgConn;

use crate::apply::{apply_dispatch, begin_replication_step, end_replication_step, BackendState};
use crate::parallel::{
    self, pa_allocate_worker, pa_decr_and_wait_stream_block, pa_find_worker,
    pa_incr_pending_stream_count, pa_lock_stream, pa_lock_transaction, pa_send_data,
    pa_set_fileset_state, pa_set_last_commit_end, pa_set_stream_apply_worker,
    pa_set_xact_state, pa_start_subtrans, pa_stream_abort, pa_switch_to_partial_serialize,
    pa_unlock_stream, pa_unlock_transaction, pa_xact_finish, ParallelTransState,
    PartialFileSetState, Winfo, PARALLEL_STREAM_NCHANGES,
};
use crate::{
    loc, my_sub, reset_apply_error_context_info, set_apply_error_context_xact,
    IN_REMOTE_TRANSACTION, REMOTE_FINAL_LSN,
};

// SubXactInfo (worker.c:341): offset of the subxact's first change in the
// changes file.
#[derive(Clone, Copy)]
struct SubXactInfo {
    xid: TransactionId,
    fileno: i32,
    offset: i64,
}

// C MaxAllocSize (memutils.h): palloc rejects (with a catchable ERROR) any
// request larger than this, which is the only bound C places on counts/lengths
// read back from the streaming spool files.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

// On-disk width of one SubXactInfo record as written by subxact_info_write:
// xid (4) + fileno (4) + offset (8). Used to bound an untrusted subxact count
// against the actual remaining file size before allocating.
const DISK_SUBXACT_INFO_SIZE: i64 = 4 + 4 + 8;

// Per-segment size of a fileset BufFile (buffile.c MAX_PHYSICAL_FILESIZE): every
// segment but the last is exactly this many bytes, so a valid (fileno, offset)
// pair has offset in [0, this] and fileno*this + offset within the file. Used to
// bound the untrusted subxact fileno/offset before they reach segment indexing.
const BUFFILE_SEGMENT_SIZE: i64 = 0x4000_0000;

thread_local! {
    // C in_streamed_transaction / stream_xid.
    static IN_STREAMED_TRANSACTION: Cell<bool> = const { Cell::new(false) };
    static STREAM_XID: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
    // C MyLogicalRepWorker->stream_fileset: created on the first streamed
    // transaction, lives for the worker (= this thread). Arc so the leader
    // can hand it to a parallel apply worker at FS_SERIALIZE_DONE.
    // tls-dtor: try_with-safe — FileSet::drop skips its dir walk after fd-TLS teardown (startup reaper is the belt).
    static STREAM_FILESET: RefCell<Option<Arc<FileSet>>> = const { RefCell::new(None) };
    // C stream_fd: the open spool file between STREAM START and STREAM STOP,
    // and during spooled replay.
    // tls-dtor: plain-data TODAY — BufFile has no Drop (VFDs leak to the fd belts); adding one makes this an offender.
    static STREAM_FD: RefCell<Option<StreamFd>> = const { RefCell::new(None) };
    // C subxact_data (subxacts + subxact_last); nsubxacts_max is Vec growth.
    static SUBXACTS: RefCell<Vec<SubXactInfo>> = const { RefCell::new(Vec::new()) };
    static SUBXACT_LAST: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
}

// The open spool file together with the context its buffers live in. C keeps
// stream_fd's BufFile in LogicalStreamingContext (worker.c:4360, reset at
// stream stop, worker.c:1646) and the replay file in TopTransactionContext
// (worker.c:2043) — both outlive the per-message ApplyMessageContext reset.
// Here the file owns a context of its own with exactly that lifetime: created
// when the file is opened, freed when it is closed (the field order drops the
// file first).
struct StreamFd {
    file: BufFile<'static>,
    // Boxed: the file's Mcx handle references the context value itself, and
    // the heap address survives the move into this struct.
    _cx: Box<MemoryContext>,
}

impl StreamFd {
    fn open(
        name: &'static str,
        open: impl FnOnce(Mcx<'static>) -> PgResult<BufFile<'static>>,
    ) -> PgResult<StreamFd> {
        let cx = Box::new(MemoryContext::new_bump(name));
        // SAFETY: the file is allocated from the boxed `cx` (a stable address)
        // and stored beside it; it is closed and dropped before `cx`
        // (StreamFd's field order), and nothing else keeps the handle.
        let mcx: Mcx<'static> = unsafe { std::mem::transmute(cx.mcx()) };
        let file = open(mcx)?;
        Ok(StreamFd { file, _cx: cx })
    }
}

pub(crate) fn in_streamed_transaction() -> bool {
    IN_STREAMED_TRANSACTION.with(Cell::get)
}

fn subid() -> Oid {
    my_sub(|s| s.oid)
}

// TransApplyAction (worker.c:261).
#[derive(PartialEq, Eq, Clone, Copy)]
enum TransApplyAction {
    LeaderApply,
    LeaderSerialize,
    LeaderSendToParallel,
    LeaderPartialSerialize,
    ParallelApply,
}

// get_transaction_apply_action (worker.c:5198).
fn get_transaction_apply_action(xid: TransactionId) -> (TransApplyAction, Option<Winfo>) {
    use TransApplyAction::*;
    if parallel::am_parallel_apply_worker() {
        return (ParallelApply, None);
    }
    match pa_find_worker(xid) {
        Some(w) if w.borrow().serialize_changes => (LeaderPartialSerialize, Some(w)),
        Some(w) => (LeaderSendToParallel, Some(w)),
        None if in_streamed_transaction() => (LeaderSerialize, None),
        None => (LeaderApply, None),
    }
}

// changes_filename (worker.c:4290) / subxact_filename (worker.c:4283).
fn changes_filename(subid: Oid, xid: TransactionId) -> String {
    format!("{subid}-{xid}.changes")
}

fn subxact_filename(subid: Oid, xid: TransactionId) -> String {
    format!("{subid}-{xid}.subxacts")
}

fn protocol_violation(msg: &str, site: &'static str) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_PROTOCOL_VIOLATION)
        .errmsg(msg.to_string())
        .finish(loc(site))?;
    unreachable!();
}

// Run `f` with the worker's stream fileset, creating it on first use
// (stream_start_internal's lazy FileSetInit, worker.c:1452).
fn with_fileset<R>(f: impl FnOnce(&FileSet) -> PgResult<R>) -> PgResult<R> {
    STREAM_FILESET.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(Arc::new(FileSet::init()?));
        }
        f(slot.as_ref().expect("just initialized"))
    })
}

// The leader's stream fileset, shared with a parallel apply worker at
// FS_SERIALIZE_DONE (C copies the FileSet by value into the DSM).
pub(crate) fn stream_fileset_arc() -> Arc<FileSet> {
    STREAM_FILESET.with(|cell| {
        Arc::clone(cell.borrow().as_ref().expect("stream fileset initialized"))
    })
}

// logicalrep_worker_onexit's stream fileset arm (launcher.c:836-838):
// `if (MyLogicalRepWorker->stream_fileset != NULL) FileSetDeleteAll(..)`.
// A before_shmem_exit callback (C registers the whole onexit at attach,
// launcher.c:744); the worker mains register it just AHEAD of
// logicalrep_worker_attach so the LIFO drain runs it right after the
// launcher's onexit — C's detach-then-delete order, the parallel apply
// workers already stopped. It has to be a before_shmem_exit callback and
// nothing later: every spool segment the walk unlinks reports its size to
// pgstat (fd.c:1966 ReportTemporaryFileUsage -> pgstat_report_tempfile,
// pgstat_database.c:211), and pgstat_shutdown_hook — registered before any
// worker code runs, by BaseInit's pgstat_initialize (bgworker.c:825,
// pgstat.c:660) — drains after this one, flushing that report
// (pgstat.c:617) and shutting the backend's stats state down. Leaving the
// deletion to STREAM_FILESET's thread-exit drop ran it after the session's
// stats state was gone: the apply worker panicked on exit after a streamed
// transaction's apply error. The Arc is taken so that drop is a no-op; a
// parallel apply worker still holding a clone re-runs the deletion on an
// already-removed directory, which PathNameDeleteTemporaryDir skips.
pub(crate) fn stream_fileset_delete_on_exit(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    let fileset = STREAM_FILESET.with(|cell| cell.borrow_mut().take());
    if let Some(fileset) = fileset {
        fileset.delete_all()?;
    }
    Ok(())
}

// What apply_dispatch should do with a data message after
// handle_streamed_transaction looked at it.
pub(crate) enum StreamedHandling {
    // Not in streaming mode: parse from the action byte as usual.
    NotStreamed,
    // Spooled or forwarded; nothing left to do.
    Consumed,
    // The (sub)txn xid prefix was consumed; parse the payload at this offset
    // (parallel apply worker, and RELATION/TYPE which the leader also applies).
    ContinueAt(usize),
}

// handle_streamed_transaction (worker.c:551): while inside a streamed chunk
// every data message carries the (sub)txn xid first.
pub(crate) fn handle_streamed_transaction(
    mcx: Mcx<'static>,
    action: u8,
    buf: &[u8],
) -> PgResult<StreamedHandling> {
    use TransApplyAction::*;

    let (apply_action, winfo) = get_transaction_apply_action(STREAM_XID.get());
    if apply_action == LeaderApply {
        return Ok(StreamedHandling::NotStreamed);
    }
    debug_assert!(
        apply_action == ParallelApply || STREAM_XID.get() != InvalidTransactionId
    );

    if buf.len() < 5 {
        protocol_violation(
            "invalid transaction ID in streamed replication transaction",
            "handle_streamed_transaction",
        )?;
    }
    let current_xid = u32::from_be_bytes(buf[1..5].try_into().expect("4 bytes"));
    if current_xid == InvalidTransactionId {
        protocol_violation(
            "invalid transaction ID in streamed replication transaction",
            "handle_streamed_transaction",
        )?;
    }

    // RELATION/TYPE updates are also applied in the leader: the publisher
    // doesn't always resend them after the streamed transaction.
    let leader_also_applies = matches!(
        action,
        logicalproto::LOGICAL_REP_MSG_RELATION | logicalproto::LOGICAL_REP_MSG_TYPE
    );
    let forwarded = |()| {
        if leader_also_applies {
            StreamedHandling::ContinueAt(5)
        } else {
            StreamedHandling::Consumed
        }
    };

    match apply_action {
        LeaderSerialize => {
            subxact_info_add(current_xid)?;
            stream_write_change(action, &buf[5..])?;
            Ok(StreamedHandling::Consumed)
        }
        LeaderSendToParallel => {
            let winfo = winfo.expect("winfo");
            if pa_send_data(&winfo, buf)? {
                return Ok(forwarded(()));
            }
            // Queue full: switch to serialize mode.
            pa_switch_to_partial_serialize(mcx, &winfo, false)?;
            stream_write_change(action, &buf[1..])?;
            Ok(forwarded(()))
        }
        LeaderPartialSerialize => {
            // Spool the original message (xid included) for the parallel
            // apply worker's replay.
            stream_write_change(action, &buf[1..])?;
            Ok(forwarded(()))
        }
        ParallelApply => {
            PARALLEL_STREAM_NCHANGES.with(|c| c.set(c.get() + 1));
            pa_start_subtrans(current_xid, STREAM_XID.get())?;
            Ok(StreamedHandling::ContinueAt(5))
        }
        LeaderApply => unreachable!(),
    }
}

// stream_start_internal (worker.c:1445): open the spool file (create on the
// first segment) inside a transaction that lasts until stream stop, and load
// the subxact info for continued segments.
pub(crate) fn stream_start_internal(
    mcx: Mcx<'static>,
    xid: TransactionId,
    first_segment: bool,
) -> PgResult<()> {
    begin_replication_step(mcx)?;
    stream_open_file(mcx, subid(), xid, first_segment)?;
    if !first_segment {
        subxact_info_read(mcx, subid(), xid)?;
    }
    end_replication_step()
}

// apply_handle_stream_start (worker.c:1484). `buf` is the full message
// (action byte first) so it can be forwarded/spooled verbatim.
pub(crate) fn apply_handle_stream_start(mcx: Mcx<'static>, buf: &[u8]) -> PgResult<()> {
    use TransApplyAction::*;

    if in_streamed_transaction() {
        protocol_violation("duplicate STREAM START message", "apply_handle_stream_start")?;
    }
    debug_assert!(
        parallel::am_parallel_apply_worker() || STREAM_XID.get() == InvalidTransactionId
    );

    IN_STREAMED_TRANSACTION.set(true);

    let mut r = logicalproto::Reader::new(&buf[1..]);
    let (xid, first_segment) = logicalproto::logicalrep_read_stream_start(&mut r)?;
    if xid == InvalidTransactionId {
        protocol_violation(
            "invalid transaction ID in streamed replication transaction",
            "apply_handle_stream_start",
        )?;
    }
    STREAM_XID.set(xid);

    set_apply_error_context_xact(xid, InvalidXLogRecPtr);

    // Try to allocate a parallel apply worker for the streaming transaction.
    if first_segment {
        pa_allocate_worker(mcx, xid)?;
    }

    let (apply_action, winfo) = get_transaction_apply_action(xid);
    match apply_action {
        LeaderSerialize => stream_start_internal(mcx, xid, first_segment)?,

        LeaderSendToParallel => {
            let winfo = winfo.expect("winfo");
            if pa_send_data(&winfo, buf)? {
                // Unlock so the parallel apply worker can receive changes.
                if !first_segment {
                    let wxid = winfo.borrow().shared.lock_xid();
                    pa_unlock_stream(wxid, AccessExclusiveLock)?;
                }
                pa_incr_pending_stream_count(&winfo);
                pa_set_stream_apply_worker(Some(winfo));
            } else {
                pa_switch_to_partial_serialize(mcx, &winfo, !first_segment)?;
                // The switch opened the spool file already.
                stream_write_change(logicalproto::LOGICAL_REP_MSG_STREAM_START, &buf[1..])?;
                pa_set_stream_apply_worker(Some(winfo));
            }
        }

        LeaderPartialSerialize => {
            let winfo = winfo.expect("winfo");
            stream_start_internal(mcx, xid, first_segment)?;
            stream_write_change(logicalproto::LOGICAL_REP_MSG_STREAM_START, &buf[1..])?;
            pa_set_stream_apply_worker(Some(winfo));
        }

        ParallelApply => {
            if first_segment {
                // Hold the transaction lock until the end of the transaction.
                let myxid = parallel::my_parallel_shared_xid();
                pa_lock_transaction(myxid, AccessExclusiveLock)?;
                pa_set_xact_state(&parallel::my_shared(), ParallelTransState::Started);
                // The leader may be waiting for us in pa_wait_for_xact_state.
                launcher::logicalrep_worker_wakeup(subid(), types_core::InvalidOid);
            }
            PARALLEL_STREAM_NCHANGES.with(|c| c.set(0));
        }

        LeaderApply => {
            return elog::elog(ERROR, "unexpected apply action: TRANS_LEADER_APPLY".to_string())
        }
    }

    crate::apply::report_activity(BackendState::STATE_RUNNING);
    Ok(())
}

// stream_stop_internal (worker.c:1620): flush subxact info, close the spool
// file, and commit the per-stream transaction.
fn stream_stop_internal(mcx: Mcx<'static>, xid: TransactionId) -> PgResult<()> {
    subxact_info_write(mcx, subid(), xid)?;
    stream_close_file();
    debug_assert!(xact::IsTransactionState());
    xact::CommitTransactionCommand()
}

// apply_handle_stream_stop (worker.c:1643).
pub(crate) fn apply_handle_stream_stop(mcx: Mcx<'static>, buf: &[u8]) -> PgResult<()> {
    use TransApplyAction::*;

    if !in_streamed_transaction() {
        protocol_violation(
            "STREAM STOP message without STREAM START",
            "apply_handle_stream_stop",
        )?;
    }
    let xid = STREAM_XID.get();

    let (apply_action, winfo) = get_transaction_apply_action(xid);
    match apply_action {
        LeaderSerialize => stream_stop_internal(mcx, xid)?,

        LeaderSendToParallel => {
            let winfo = winfo.expect("winfo");
            // Lock before sending STREAM_STOP so the parallel apply worker
            // waits on the leader (deadlock detection; see parallel.rs).
            let wxid = winfo.borrow().shared.lock_xid();
            pa_lock_stream(wxid, AccessExclusiveLock)?;
            if pa_send_data(&winfo, buf)? {
                pa_set_stream_apply_worker(None);
            } else {
                pa_switch_to_partial_serialize(mcx, &winfo, true)?;
                stream_write_change(logicalproto::LOGICAL_REP_MSG_STREAM_STOP, &buf[1..])?;
                stream_stop_internal(mcx, xid)?;
                pa_set_stream_apply_worker(None);
            }
        }

        LeaderPartialSerialize => {
            stream_write_change(logicalproto::LOGICAL_REP_MSG_STREAM_STOP, &buf[1..])?;
            stream_stop_internal(mcx, xid)?;
            pa_set_stream_apply_worker(None);
        }

        ParallelApply => {
            let _ = elog::elog(
                DEBUG1,
                format!(
                    "applied {} changes in the streaming chunk",
                    PARALLEL_STREAM_NCHANGES.with(Cell::get)
                ),
            );
            pa_decr_and_wait_stream_block()?;
        }

        LeaderApply => {
            return elog::elog(ERROR, "unexpected apply action: TRANS_LEADER_APPLY".to_string())
        }
    }

    IN_STREAMED_TRANSACTION.set(false);
    STREAM_XID.set(InvalidTransactionId);

    // The parallel apply worker could be in a transaction, in which case the
    // state is STATE_IDLEINTRANSACTION (worker.c:1743).
    if xact::IsTransactionOrTransactionBlock() {
        crate::apply::report_activity(BackendState::STATE_IDLEINTRANSACTION);
    } else {
        crate::apply::report_activity(BackendState::STATE_IDLE);
    }

    reset_apply_error_context_info();
    Ok(())
}

// stream_abort_internal (worker.c:1746): toplevel abort deletes the spool;
// a subxact abort truncates the changes file at the subxact's first-change
// offset and drops it (and every later subxact) from the subxact info.
fn stream_abort_internal(
    mcx: Mcx<'static>,
    xid: TransactionId,
    subxid: TransactionId,
) -> PgResult<()> {
    if xid == subxid {
        stream_cleanup_files(subid(), xid)?;
        return Ok(());
    }

    begin_replication_step(mcx)?;
    subxact_info_read(mcx, subid(), xid)?;

    // Scan from the tail: we're likely aborting the most recent subxact.
    let subidx = SUBXACTS.with(|s| {
        s.borrow().iter().rposition(|info| info.xid == subxid)
    });

    let Some(subidx) = subidx else {
        // Empty subxact: just drop the loaded info.
        cleanup_subxact_info();
        end_replication_step()?;
        return xact::CommitTransactionCommand();
    };

    let target = SUBXACTS.with(|s| s.borrow()[subidx]);

    // Truncate the changes file at the subxact's start.
    let name = changes_filename(subid(), xid);
    let mut file = with_fileset(|fs| BufFileOpenFileSet(mcx, fs, &name, false))?;

    // target.fileno/target.offset were deserialized verbatim by
    // subxact_info_read from the untrusted .subxacts spool. truncate_fileset
    // indexes files[fileno] and truncates at offset: a negative fileno wraps
    // (fileno as usize) to a huge Vec index and panics the worker, and an
    // out-of-range offset drives an OOB FileTruncate. C leaves these unchecked
    // (relying on its own on-disk values); bound them to the changes file's
    // actual extent with checked arithmetic before use, raising a catchable
    // ERRCODE_DATA_CORRUPTED on violation.
    let file_size = file.size()?;
    let in_range = target.fileno >= 0
        && target.offset >= 0
        && target.offset <= BUFFILE_SEGMENT_SIZE
        && i64::from(target.fileno)
            .checked_mul(BUFFILE_SEGMENT_SIZE)
            .and_then(|base| base.checked_add(target.offset))
            .is_some_and(|pos| pos <= file_size);
    if !in_range {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "invalid subxact fileno/offset ({}, {}) in streaming transaction's changes file \"{name}\"",
                target.fileno, target.offset
            ))
            .finish(loc("stream_abort_internal"))?;
        unreachable!("ERROR finish returned");
    }

    file.truncate_fileset(target.fileno, target.offset)?;
    file.close()?;

    // Discard the subxacts added later.
    SUBXACTS.with(|s| s.borrow_mut().truncate(subidx));

    subxact_info_write(mcx, subid(), xid)?;
    end_replication_step()?;
    xact::CommitTransactionCommand()
}

// apply_handle_stream_abort (worker.c:1829).
pub(crate) fn apply_handle_stream_abort(mcx: Mcx<'static>, buf: &[u8]) -> PgResult<()> {
    use TransApplyAction::*;

    if in_streamed_transaction() {
        protocol_violation(
            "STREAM ABORT message without STREAM STOP",
            "apply_handle_stream_abort",
        )?;
    }

    // Abort info rides only when we requested parallel streaming.
    let read_abort_info = launcher::my_worker_slot()
        .and_then(launcher::worker_snapshot)
        .map(|w| w.parallel_apply)
        .unwrap_or(false);
    let mut r = logicalproto::Reader::new(&buf[1..]);
    let abort = logicalproto::logicalrep_read_stream_abort(&mut r, read_abort_info)?;
    let (xid, subxid) = (abort.xid, abort.subxid);
    let toplevel_xact = xid == subxid;

    set_apply_error_context_xact(subxid, abort.abort_lsn);

    let (apply_action, winfo) = get_transaction_apply_action(xid);
    match apply_action {
        LeaderApply => {
            // Serialized to file in the leader.
            stream_abort_internal(mcx, xid, subxid)?;
            let _ = elog::elog(
                DEBUG1,
                "finished processing the STREAM ABORT command".to_string(),
            );
        }

        LeaderSendToParallel => {
            let winfo = winfo.expect("winfo");
            // For a subxact abort, count a pending stream block and retake
            // the stream lock BEFORE sending, so the parallel apply worker
            // waits on the leader for the next set of changes afterwards.
            if !toplevel_xact {
                pa_unlock_stream(xid, AccessExclusiveLock)?;
                pa_incr_pending_stream_count(&winfo);
                pa_lock_stream(xid, AccessExclusiveLock)?;
            }
            if pa_send_data(&winfo, buf)? {
                // Wait for toplevel aborts to finish (xid-wraparound hazards
                // on the txn hash and the partial-serialize files).
                if toplevel_xact {
                    pa_xact_finish(&winfo, InvalidXLogRecPtr)?;
                }
            } else {
                pa_switch_to_partial_serialize(mcx, &winfo, true)?;
                stream_open_and_write_change(
                    mcx,
                    xid,
                    logicalproto::LOGICAL_REP_MSG_STREAM_ABORT,
                    &buf[1..],
                )?;
                if toplevel_xact {
                    pa_set_fileset_state(&winfo.borrow().shared, PartialFileSetState::SerializeDone);
                    pa_xact_finish(&winfo, InvalidXLogRecPtr)?;
                }
            }
        }

        LeaderPartialSerialize => {
            let winfo = winfo.expect("winfo");
            // The parallel apply worker might have applied some changes, so
            // spool the STREAM_ABORT so it can roll back if needed.
            stream_open_and_write_change(
                mcx,
                xid,
                logicalproto::LOGICAL_REP_MSG_STREAM_ABORT,
                &buf[1..],
            )?;
            if toplevel_xact {
                pa_set_fileset_state(&winfo.borrow().shared, PartialFileSetState::SerializeDone);
                pa_xact_finish(&winfo, InvalidXLogRecPtr)?;
            }
        }

        ParallelApply => {
            // Applying spooled messages: close the file before aborting.
            if toplevel_xact && STREAM_FD.with(|f| f.borrow().is_some()) {
                stream_close_file();
            }
            pa_stream_abort(&abort)?;
            // Wait for the next set of changes after a subxact rollback.
            if !toplevel_xact {
                pa_decr_and_wait_stream_block()?;
            }
            let _ = elog::elog(
                DEBUG1,
                "finished processing the STREAM ABORT command".to_string(),
            );
        }

        LeaderSerialize => {
            return elog::elog(
                ERROR,
                "unexpected apply action: TRANS_LEADER_SERIALIZE".to_string(),
            )
        }
    }

    reset_apply_error_context_info();
    Ok(())
}

// ensure_last_message (worker.c:1985).
fn ensure_last_message(
    mcx: Mcx<'static>,
    fileset: &FileSet,
    xid: TransactionId,
    fileno: i32,
    offset: i64,
) -> PgResult<()> {
    debug_assert!(!xact::IsTransactionState());
    begin_replication_step(mcx)?;
    let name = changes_filename(subid(), xid);
    let mut file = BufFileOpenFileSet(mcx, fileset, &name, true)?;
    file.seek(0, 0, fd::buffile::SEEK_END)?;
    let (last_fileno, last_offset) = file.tell();
    file.close()?;
    end_replication_step()?;
    if last_fileno != fileno || last_offset != offset {
        return elog::elog(
            ERROR,
            format!(
                "unexpected message left in streaming transaction's changes file \"{name}\""
            ),
        );
    }
    Ok(())
}

// apply_spooled_messages (worker.c:2017): replay every spooled message
// through apply_dispatch. The open spool file lives in STREAM_FD so the
// transaction-finish handlers (parallel apply replay) can close it mid-loop.
pub(crate) fn apply_spooled_messages(
    mcx: Mcx<'static>,
    mut conn: Option<&mut PgConn>,
    fileset: &FileSet,
    xid: TransactionId,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    if !parallel::am_parallel_apply_worker() {
        crate::maybe_start_skipping_changes(lsn);
    }

    begin_replication_step(mcx)?;

    // The file handle must survive every per-message reset below
    // (C: TopTransactionContext, worker.c:2043): it gets a context of its own.
    let name = changes_filename(subid(), xid);
    let file = StreamFd::open("LogicalStreamingContext", |fmcx| {
        BufFileOpenFileSet(fmcx, fileset, &name, true)
    })?;
    debug_assert!(STREAM_FD.with(|f| f.borrow().is_none()));
    STREAM_FD.with(|f| *f.borrow_mut() = Some(file));

    REMOTE_FINAL_LSN.set(lsn);
    // Make sure the apply_dispatch methods know we're in a remote txn.
    IN_REMOTE_TRANSACTION.set(true);
    crate::apply::report_activity(BackendState::STATE_RUNNING);

    end_replication_step()?;

    // Read the entries one by one and pass them through the same logic as
    // the live apply path. Each one is dispatched in the per-message
    // ApplyMessageContext and that context is reset after it
    // (worker.c:2119-2123): the caller's own message context holds nothing
    // this replay needs, so a fresh context stands in for C's switch.
    let mut msgcx = MemoryContext::new_bump("ApplyMessageContext");
    let mut buf: Vec<u8> = Vec::new();
    let mut nchanges = 0u32;
    loop {
        postgres_seams::check_for_interrupts::call()?;

        let read = STREAM_FD.with(|f| -> PgResult<Option<(i32, i64)>> {
            let mut slot = f.borrow_mut();
            let file = &mut slot.as_mut().expect("stream file open").file;
            let mut lenbuf = [0u8; 4];
            let nbytes = file.read_maybe_eof(&mut lenbuf, true)?;
            if nbytes == 0 {
                return Ok(None); // end of the file
            }
            let len = i32::from_ne_bytes(lenbuf);
            if len <= 0 {
                ereport(ERROR)
                    .errmsg(format!(
                        "incorrect length {len} in streaming transaction's changes file \"{name}\""
                    ))
                    .finish(loc("apply_spooled_messages"))?;
            }
            // `len` is an untrusted on-disk field (same fileset-spool threat as
            // subxact_info_read). C's repalloc(buffer, len) trips the
            // MaxAllocSize check and raises a catchable ERROR on a hostile
            // length; mirror that bound before growing the buffer, and reserve
            // fallibly so an infallible allocation can't abort the process.
            let len = len as usize;
            if len > MAX_ALLOC_SIZE {
                ereport(ERROR)
                    .errcode(ERRCODE_DATA_CORRUPTED)
                    .errmsg(format!(
                        "incorrect length {len} in streaming transaction's changes file \"{name}\""
                    ))
                    .finish(loc("apply_spooled_messages"))?;
                unreachable!("ERROR finish returned");
            }
            buf.clear();
            buf.try_reserve(len)
                .map_err(|_| mcx::oom_named("apply_spooled_messages", len))?;
            buf.resize(len, 0);
            file.read_exact(&mut buf)?;
            Ok(Some(file.tell()))
        })?;
        let Some((fileno, offset)) = read else { break };

        // The spooled record is action + payload, the live wire shape.
        {
            // SAFETY: `msgcx` outlives the dispatch; the handle is re-derived
            // after every reset and never stored past it.
            let mmcx: Mcx<'static> = unsafe { std::mem::transmute(msgcx.mcx()) };
            apply_dispatch(mmcx, conn.as_deref_mut(), &buf)?;
        }
        msgcx.reset();
        nchanges += 1;

        // The file may have been closed because we processed a transaction
        // end message (stream_commit in the parallel replay), in which case
        // that must be the last message.
        if STREAM_FD.with(|f| f.borrow().is_none()) {
            ensure_last_message(mcx, fileset, xid, fileno, offset)?;
            break;
        }
    }

    if STREAM_FD.with(|f| f.borrow().is_some()) {
        stream_close_file();
    }

    let _ = elog::elog(
        DEBUG1,
        format!("replayed {nchanges} (all) changes from file \"{name}\""),
    );
    Ok(())
}

// apply_handle_stream_commit (worker.c:2147).
pub(crate) fn apply_handle_stream_commit(
    mcx: Mcx<'static>,
    mut conn: Option<&mut PgConn>,
    buf: &[u8],
) -> PgResult<()> {
    use TransApplyAction::*;

    if in_streamed_transaction() {
        protocol_violation(
            "STREAM COMMIT message without STREAM STOP",
            "apply_handle_stream_commit",
        )?;
    }

    let mut r = logicalproto::Reader::new(&buf[1..]);
    let (xid, commit_data) = logicalproto::logicalrep_read_stream_commit(&mut r)?;
    set_apply_error_context_xact(xid, commit_data.commit_lsn);

    let (apply_action, winfo) = get_transaction_apply_action(xid);
    match apply_action {
        LeaderApply => {
            // Serialized to file: replay all the spooled operations.
            let fileset = stream_fileset_arc();
            apply_spooled_messages(
                mcx,
                conn.as_deref_mut(),
                &fileset,
                xid,
                commit_data.commit_lsn,
            )?;
            crate::apply::apply_handle_commit_internal(mcx, &commit_data)?;
            stream_cleanup_files(subid(), xid)?;
            let _ = elog::elog(
                DEBUG1,
                "finished processing the STREAM COMMIT command".to_string(),
            );
        }

        LeaderSendToParallel => {
            let winfo = winfo.expect("winfo");
            if pa_send_data(&winfo, buf)? {
                pa_xact_finish(&winfo, commit_data.end_lsn)?;
            } else {
                pa_switch_to_partial_serialize(mcx, &winfo, true)?;
                stream_open_and_write_change(
                    mcx,
                    xid,
                    logicalproto::LOGICAL_REP_MSG_STREAM_COMMIT,
                    &buf[1..],
                )?;
                pa_set_fileset_state(&winfo.borrow().shared, PartialFileSetState::SerializeDone);
                pa_xact_finish(&winfo, commit_data.end_lsn)?;
            }
        }

        LeaderPartialSerialize => {
            let winfo = winfo.expect("winfo");
            stream_open_and_write_change(
                mcx,
                xid,
                logicalproto::LOGICAL_REP_MSG_STREAM_COMMIT,
                &buf[1..],
            )?;
            pa_set_fileset_state(&winfo.borrow().shared, PartialFileSetState::SerializeDone);
            pa_xact_finish(&winfo, commit_data.end_lsn)?;
        }

        ParallelApply => {
            // Applying spooled messages: close the file before committing.
            if STREAM_FD.with(|f| f.borrow().is_some()) {
                stream_close_file();
            }
            crate::apply::apply_handle_commit_internal(mcx, &commit_data)?;
            pa_set_last_commit_end(transam_xlog_seams::xact_last_commit_end::call());
            // Set FINISHED before releasing the lock (pa_wait_for_xact_finish).
            pa_set_xact_state(&parallel::my_shared(), ParallelTransState::Finished);
            pa_unlock_transaction(xid, AccessExclusiveLock)?;
            parallel::pa_reset_subtrans();
            let _ = elog::elog(
                DEBUG1,
                "finished processing the STREAM COMMIT command".to_string(),
            );
        }

        LeaderSerialize => {
            return elog::elog(
                ERROR,
                "unexpected apply action: TRANS_LEADER_SERIALIZE".to_string(),
            )
        }
    }

    // Process any tables that are being synchronized in parallel.
    crate::tablesync::process_syncing_tables(mcx, conn, commit_data.end_lsn)?;

    crate::apply::report_activity(BackendState::STATE_IDLE);

    reset_apply_error_context_info();
    Ok(())
}

// apply_handle_stream_prepare (worker.c:1280).
pub(crate) fn apply_handle_stream_prepare(
    mcx: Mcx<'static>,
    mut conn: Option<&mut PgConn>,
    buf: &[u8],
) -> PgResult<()> {
    use TransApplyAction::*;

    if in_streamed_transaction() {
        protocol_violation(
            "STREAM PREPARE message without STREAM STOP",
            "apply_handle_stream_prepare",
        )?;
    }

    // Tablesync should never receive prepare.
    if crate::tablesync::AM_TABLESYNC_WORKER.with(std::cell::Cell::get) {
        protocol_violation(
            "tablesync worker received a STREAM PREPARE message",
            "apply_handle_stream_prepare",
        )?;
    }

    let mut r = logicalproto::Reader::new(&buf[1..]);
    let prepare_data = logicalproto::logicalrep_read_stream_prepare(&mut r)?;
    set_apply_error_context_xact(prepare_data.xid, prepare_data.prepare_lsn);

    let (apply_action, winfo) = get_transaction_apply_action(prepare_data.xid);
    match apply_action {
        LeaderApply => {
            // Replay the spool; the last change's transaction stays open for
            // the prepare.
            let fileset = stream_fileset_arc();
            apply_spooled_messages(
                mcx,
                conn.as_deref_mut(),
                &fileset,
                prepare_data.xid,
                prepare_data.prepare_lsn,
            )?;
            crate::apply::apply_handle_prepare_internal(&prepare_data)?;
            xact::CommitTransactionCommand()?;

            // The prepare record is always flushed; an invalid local LSN is ok.
            crate::store_flush_position(prepare_data.end_lsn, types_core::InvalidXLogRecPtr);
            IN_REMOTE_TRANSACTION.set(false);
            stream_cleanup_files(subid(), prepare_data.xid)?;
            let _ = elog::elog(
                DEBUG1,
                "finished processing the STREAM PREPARE command".to_string(),
            );
        }

        LeaderSendToParallel => {
            let winfo = winfo.expect("winfo");
            if pa_send_data(&winfo, buf)? {
                pa_xact_finish(&winfo, prepare_data.end_lsn)?;
            } else {
                pa_switch_to_partial_serialize(mcx, &winfo, true)?;
                stream_open_and_write_change(
                    mcx,
                    prepare_data.xid,
                    logicalproto::LOGICAL_REP_MSG_STREAM_PREPARE,
                    &buf[1..],
                )?;
                pa_set_fileset_state(&winfo.borrow().shared, PartialFileSetState::SerializeDone);
                pa_xact_finish(&winfo, prepare_data.end_lsn)?;
            }
        }

        LeaderPartialSerialize => {
            let winfo = winfo.expect("winfo");
            stream_open_and_write_change(
                mcx,
                prepare_data.xid,
                logicalproto::LOGICAL_REP_MSG_STREAM_PREPARE,
                &buf[1..],
            )?;
            pa_set_fileset_state(&winfo.borrow().shared, PartialFileSetState::SerializeDone);
            pa_xact_finish(&winfo, prepare_data.end_lsn)?;
        }

        ParallelApply => {
            // Applying spooled messages: close the file before preparing.
            if STREAM_FD.with(|f| f.borrow().is_some()) {
                stream_close_file();
            }
            begin_replication_step(mcx)?;
            crate::apply::apply_handle_prepare_internal(&prepare_data)?;
            end_replication_step()?;
            xact::CommitTransactionCommand()?;
            pa_set_last_commit_end(InvalidXLogRecPtr);
            pa_set_xact_state(&parallel::my_shared(), ParallelTransState::Finished);
            pa_unlock_transaction(parallel::my_parallel_shared_xid(), AccessExclusiveLock)?;
            parallel::pa_reset_subtrans();
            let _ = elog::elog(
                DEBUG1,
                "finished processing the STREAM PREPARE command".to_string(),
            );
        }

        LeaderSerialize => {
            return elog::elog(
                ERROR,
                "unexpected apply action: TRANS_LEADER_SERIALIZE".to_string(),
            )
        }
    }

    // worker.c:1412.
    pgstat::pending::pgstat_report_stat(false);

    // Process any tables that are being synchronized in parallel.
    crate::tablesync::process_syncing_tables(mcx, conn, prepare_data.end_lsn)?;

    // As in apply_handle_prepare: a crash before clearing the subskiplsn
    // leaves it set, cleared when finishing the next transaction.
    crate::stop_skipping_changes();
    crate::clear_subscription_skip_lsn(mcx, prepare_data.prepare_lsn)?;

    crate::apply::report_activity(BackendState::STATE_IDLE);

    reset_apply_error_context_info();
    Ok(())
}

// ---- spool file helpers -----------------------------------------------------

// stream_cleanup_files (worker.c:4304).
pub(crate) fn stream_cleanup_files(subid: Oid, xid: TransactionId) -> PgResult<()> {
    with_fileset(|fs| {
        BufFileDeleteFileSet(fs, &changes_filename(subid, xid), false)?;
        BufFileDeleteFileSet(fs, &subxact_filename(subid, xid), true)
    })
}

// stream_open_file (worker.c:4328).
fn stream_open_file(
    mcx: Mcx<'static>,
    subid: Oid,
    xid: TransactionId,
    first_segment: bool,
) -> PgResult<()> {
    debug_assert!(STREAM_FD.with(|f| f.borrow().is_none()));
    let _ = mcx;
    let name = changes_filename(subid, xid);
    // Create/open the buffile under the logical streaming context so that we
    // have it until stream stop (worker.c:4360).
    let file = StreamFd::open("LogicalStreamingContext", |smcx| {
        with_fileset(|fs| {
            if first_segment {
                BufFileCreateFileSet(smcx, fs, &name)
            } else {
                // Always append: seek to the end.
                let mut f = BufFileOpenFileSet(smcx, fs, &name, false)?;
                f.seek(0, 0, fd::buffile::SEEK_END)?;
                Ok(f)
            }
        })
    })?;
    STREAM_FD.with(|f| *f.borrow_mut() = Some(file));
    Ok(())
}

// stream_close_file (worker.c:4373).
fn stream_close_file() {
    let fd = STREAM_FD.with(|f| f.borrow_mut().take()).expect("stream file open");
    // The spool must be durable across chunks; close flushes the buffer.
    fd.file.close().expect("closing streamed-changes spool file");
}

// stream_write_change (worker.c:4391): [len][action][payload]. For the serial
// spool the payload has the (sub)txn xid stripped; for the partial-serialize
// spool it keeps it (the parallel apply worker needs it for savepoints).
fn stream_write_change(action: u8, payload: &[u8]) -> PgResult<()> {
    STREAM_FD.with(|f| {
        let mut slot = f.borrow_mut();
        let file = &mut slot.as_mut().expect("stream file open").file;
        let len = (payload.len() + 1) as i32;
        file.write(&len.to_ne_bytes())?;
        file.write(&[action])?;
        file.write(payload)
    })
}

// stream_open_and_write_change (worker.c:4421).
fn stream_open_and_write_change(
    mcx: Mcx<'static>,
    xid: TransactionId,
    action: u8,
    payload: &[u8],
) -> PgResult<()> {
    debug_assert!(!in_streamed_transaction());
    if STREAM_FD.with(|f| f.borrow().is_none()) {
        stream_start_internal(mcx, xid, false)?;
    }
    stream_write_change(action, payload)?;
    stream_stop_internal(mcx, xid)
}

// ---- subxact info -----------------------------------------------------------

// subxact_info_write (worker.c:4105): overwrite the whole subxact file; no
// subxacts deletes it.
fn subxact_info_write(mcx: Mcx<'static>, subid: Oid, xid: TransactionId) -> PgResult<()> {
    debug_assert!(xid != InvalidTransactionId);
    let name = subxact_filename(subid, xid);

    let subxacts: Vec<SubXactInfo> = SUBXACTS.with(|s| s.borrow().clone());
    if subxacts.is_empty() {
        cleanup_subxact_info();
        return with_fileset(|fs| BufFileDeleteFileSet(fs, &name, true));
    }

    let mut file = with_fileset(|fs| match BufFileOpenFileSetMaybe(mcx, fs, &name, false)? {
        Some(f) => Ok(f),
        None => BufFileCreateFileSet(mcx, fs, &name),
    })?;

    file.write(&(subxacts.len() as u32).to_ne_bytes())?;
    for info in &subxacts {
        file.write(&info.xid.to_ne_bytes())?;
        file.write(&info.fileno.to_ne_bytes())?;
        file.write(&info.offset.to_ne_bytes())?;
    }
    file.close()?;

    cleanup_subxact_info();
    Ok(())
}

// subxact_info_read (worker.c:4154).
fn subxact_info_read(mcx: Mcx<'static>, subid: Oid, xid: TransactionId) -> PgResult<()> {
    debug_assert!(SUBXACTS.with(|s| s.borrow().is_empty()));
    let name = subxact_filename(subid, xid);

    let Some(mut file) = with_fileset(|fs| BufFileOpenFileSetMaybe(mcx, fs, &name, true))? else {
        // No subxact file means no subxact info.
        return Ok(());
    };

    let mut nbuf = [0u8; 4];
    file.read_exact(&mut nbuf)?;
    let n = u32::from_ne_bytes(nbuf) as usize;

    // `n` is read verbatim from the .subxacts spool file, which lives in the
    // subscriber's pgsql_tmp fileset and carries no integrity protection: it is
    // whatever on-disk corruption or an attacker who can write the fileset put
    // there. C bounds this only implicitly — palloc(nsubxacts_max *
    // sizeof(SubXactInfo)) trips the MaxAllocSize check and raises a catchable
    // "invalid memory alloc request size" ERROR. An infallible
    // Vec::with_capacity(n) would instead abort the whole process
    // (thread-per-backend => cluster crash) on a hostile count, so validate the
    // count BEFORE allocating: it can describe no more records than remain in
    // the file, and it must fit the MaxAllocSize class. Then reserve fallibly.
    let file_size = file.size()?;
    let remaining = file_size - nbuf.len() as i64; // the count was already read
    let max_by_file = if remaining > 0 {
        (remaining / DISK_SUBXACT_INFO_SIZE) as usize
    } else {
        0
    };
    let max_by_mem = MAX_ALLOC_SIZE / core::mem::size_of::<SubXactInfo>();
    if n > max_by_file || n > max_by_mem {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "invalid subxact count {n} in streaming transaction's subxact file \"{name}\""
            ))
            .finish(loc("subxact_info_read"))?;
        unreachable!("ERROR finish returned");
    }

    let mut subxacts: Vec<SubXactInfo> = Vec::new();
    subxacts
        .try_reserve(n)
        .map_err(|_| mcx::oom_named("subxact_info_read", n * core::mem::size_of::<SubXactInfo>()))?;
    for _ in 0..n {
        let mut xidb = [0u8; 4];
        let mut fileb = [0u8; 4];
        let mut offb = [0u8; 8];
        file.read_exact(&mut xidb)?;
        file.read_exact(&mut fileb)?;
        file.read_exact(&mut offb)?;
        subxacts.push(SubXactInfo {
            xid: u32::from_ne_bytes(xidb),
            fileno: i32::from_ne_bytes(fileb),
            offset: i64::from_ne_bytes(offb),
        });
    }
    file.close()?;

    SUBXACTS.with(|s| *s.borrow_mut() = subxacts);
    Ok(())
}

// subxact_info_add (worker.c:4205): remember the offset of the subxact's
// first change in the changes file.
fn subxact_info_add(xid: TransactionId) -> PgResult<()> {
    debug_assert!(STREAM_XID.get() != InvalidTransactionId);

    // The toplevel transaction is not tracked.
    if STREAM_XID.get() == xid {
        return Ok(());
    }
    // Usually the same subxact as the previous change.
    if SUBXACT_LAST.get() == xid {
        return Ok(());
    }
    SUBXACT_LAST.set(xid);

    // Scan from the tail: we're likely adding a change for the most recent
    // subtransactions.
    if SUBXACTS.with(|s| s.borrow().iter().rev().any(|info| info.xid == xid)) {
        return Ok(());
    }

    let (fileno, offset) = STREAM_FD.with(|f| {
        f.borrow().as_ref().expect("stream file open").file.tell()
    });
    SUBXACTS.with(|s| s.borrow_mut().push(SubXactInfo { xid, fileno, offset }));
    Ok(())
}

// cleanup_subxact_info (worker.c:4487).
fn cleanup_subxact_info() {
    SUBXACTS.with(|s| s.borrow_mut().clear());
    SUBXACT_LAST.set(InvalidTransactionId);
}
