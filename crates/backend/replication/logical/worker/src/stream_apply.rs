// Serial streamed-transaction apply (worker.c, TRANS_LEADER_SERIALIZE arm):
// in-progress transactions arrive as STREAM START/STOP chunks whose data
// messages are spooled — stripped of their per-change (sub)txn xid — into a
// per-(subscription, xid) fileset file, plus a companion subxact file mapping
// each subxact to the offset of its first change (for STREAM ABORT of a
// subtransaction). STREAM COMMIT replays the spool through apply_dispatch.
// The parallel-apply arms (applyparallelworker.c) are NOT ported: a
// subscription with streaming=parallel is refused loudly at stream-options
// time (lib.rs).

use std::cell::{Cell, RefCell};

use elog::ereport;
use fd::{
    BufFile, BufFileCreateFileSet, BufFileDeleteFileSet, BufFileOpenFileSet,
    BufFileOpenFileSetMaybe, FileSet,
};
use mcx::Mcx;
use types_core::{InvalidTransactionId, Oid, TransactionId, XLogRecPtr};
use types_error::{PgResult, ERRCODE_PROTOCOL_VIOLATION, ERROR};
use walreceiver::client::PgConn;

use crate::apply::{apply_dispatch, begin_replication_step, end_replication_step};
use crate::{loc, my_sub, IN_REMOTE_TRANSACTION, REMOTE_FINAL_LSN};

// SubXactInfo (worker.c:341): offset of the subxact's first change in the
// changes file.
#[derive(Clone, Copy)]
struct SubXactInfo {
    xid: TransactionId,
    fileno: i32,
    offset: i64,
}

thread_local! {
    // C in_streamed_transaction / stream_xid.
    static IN_STREAMED_TRANSACTION: Cell<bool> = const { Cell::new(false) };
    static STREAM_XID: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
    // C MyLogicalRepWorker->stream_fileset: created on the first streamed
    // transaction, lives for the worker (= this thread). Serial apply only,
    // so nothing shares it.
    static STREAM_FILESET: RefCell<Option<FileSet>> = const { RefCell::new(None) };
    // C stream_fd: the open spool file between STREAM START and STREAM STOP,
    // and during spooled replay.
    static STREAM_FD: RefCell<Option<BufFile<'static>>> = const { RefCell::new(None) };
    // C subxact_data (subxacts + subxact_last); nsubxacts_max is Vec growth.
    static SUBXACTS: RefCell<Vec<SubXactInfo>> = const { RefCell::new(Vec::new()) };
    static SUBXACT_LAST: Cell<TransactionId> = const { Cell::new(InvalidTransactionId) };
}

pub(crate) fn in_streamed_transaction() -> bool {
    IN_STREAMED_TRANSACTION.with(Cell::get)
}

fn subid() -> Oid {
    my_sub(|s| s.oid)
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
            *slot = Some(FileSet::init()?);
        }
        f(slot.as_ref().expect("just initialized"))
    })
}

// handle_streamed_transaction (worker.c:539), serialize arm: while inside a
// streamed chunk every data message carries the (sub)txn xid first; remember
// the subxact and spool the message minus the xid. Returns true when the
// message was consumed here.
pub(crate) fn handle_streamed_transaction(action: u8, buf: &[u8]) -> PgResult<bool> {
    if !in_streamed_transaction() {
        return Ok(false);
    }
    debug_assert!(STREAM_XID.get() != InvalidTransactionId);

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

    subxact_info_add(current_xid)?;
    stream_write_change(action, &buf[5..])?;
    Ok(true)
}

// stream_start_internal (worker.c:1439): open the spool file (create on the
// first segment) inside a transaction that lasts until stream stop, and load
// the subxact info for continued segments.
fn stream_start_internal(mcx: Mcx<'static>, xid: TransactionId, first_segment: bool) -> PgResult<()> {
    begin_replication_step(mcx)?;
    stream_open_file(mcx, subid(), xid, first_segment)?;
    if !first_segment {
        subxact_info_read(mcx, subid(), xid)?;
    }
    end_replication_step()
}

// apply_handle_stream_start (worker.c:1484), serialize arm.
pub(crate) fn apply_handle_stream_start(
    mcx: Mcx<'static>,
    r: &mut logicalproto::Reader<'_>,
) -> PgResult<()> {
    if in_streamed_transaction() {
        protocol_violation("duplicate STREAM START message", "apply_handle_stream_start")?;
    }
    debug_assert!(STREAM_XID.get() == InvalidTransactionId);

    IN_STREAMED_TRANSACTION.set(true);

    let (xid, first_segment) = logicalproto::logicalrep_read_stream_start(r)?;
    if xid == InvalidTransactionId {
        protocol_violation(
            "invalid transaction ID in streamed replication transaction",
            "apply_handle_stream_start",
        )?;
    }
    STREAM_XID.set(xid);

    stream_start_internal(mcx, xid, first_segment)
}

// stream_stop_internal (worker.c:1617): flush subxact info, close the spool
// file, and commit the per-stream transaction.
fn stream_stop_internal(mcx: Mcx<'static>, xid: TransactionId) -> PgResult<()> {
    subxact_info_write(mcx, subid(), xid)?;
    stream_close_file();
    debug_assert!(xact::IsTransactionState());
    xact::CommitTransactionCommand()
}

// apply_handle_stream_stop (worker.c:1643), serialize arm.
pub(crate) fn apply_handle_stream_stop(mcx: Mcx<'static>) -> PgResult<()> {
    if !in_streamed_transaction() {
        protocol_violation(
            "STREAM STOP message without STREAM START",
            "apply_handle_stream_stop",
        )?;
    }

    stream_stop_internal(mcx, STREAM_XID.get())?;

    IN_STREAMED_TRANSACTION.set(false);
    STREAM_XID.set(InvalidTransactionId);
    Ok(())
}

// stream_abort_internal (worker.c:1744): toplevel abort deletes the spool;
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
    file.truncate_fileset(target.fileno, target.offset)?;
    file.close()?;

    // Discard the subxacts added later.
    SUBXACTS.with(|s| s.borrow_mut().truncate(subidx));

    subxact_info_write(mcx, subid(), xid)?;
    end_replication_step()?;
    xact::CommitTransactionCommand()
}

// apply_handle_stream_abort (worker.c:1829), serialize arm.
pub(crate) fn apply_handle_stream_abort(
    mcx: Mcx<'static>,
    r: &mut logicalproto::Reader<'_>,
) -> PgResult<()> {
    if in_streamed_transaction() {
        protocol_violation(
            "STREAM ABORT message without STREAM STOP",
            "apply_handle_stream_abort",
        )?;
    }

    // Abort info rides only with parallel apply, which this worker never
    // requests.
    let abort = logicalproto::logicalrep_read_stream_abort(r, false)?;
    stream_abort_internal(mcx, abort.xid, abort.subxid)
}

// apply_spooled_messages (worker.c:2018): replay every spooled message
// through apply_dispatch.
fn apply_spooled_messages(
    mcx: Mcx<'static>,
    conn: &mut PgConn,
    xid: TransactionId,
    lsn: XLogRecPtr,
) -> PgResult<()> {
    begin_replication_step(mcx)?;

    let name = changes_filename(subid(), xid);
    let mut file = with_fileset(|fs| BufFileOpenFileSet(mcx, fs, &name, true))?;

    REMOTE_FINAL_LSN.set(lsn);
    // Make sure the apply_dispatch methods know we're in a remote txn.
    IN_REMOTE_TRANSACTION.set(true);

    end_replication_step()?;

    // Read the entries one by one and pass them through the same logic as
    // the live apply path.
    let mut buf: Vec<u8> = Vec::new();
    loop {
        postgres_seams::check_for_interrupts::call()?;

        let mut lenbuf = [0u8; 4];
        let nbytes = file.read_maybe_eof(&mut lenbuf, true)?;
        if nbytes == 0 {
            break; // end of the file
        }
        let len = i32::from_ne_bytes(lenbuf);
        if len <= 0 {
            ereport(ERROR)
                .errmsg(format!(
                    "incorrect length {len} in streaming transaction's changes file \"{name}\""
                ))
                .finish(loc("apply_spooled_messages"))?;
        }

        buf.clear();
        buf.resize(len as usize, 0);
        file.read_exact(&mut buf)?;

        // The spooled record is action + payload, the live wire shape.
        apply_dispatch(mcx, conn, &buf)?;
    }

    file.close()?;
    Ok(())
}

// apply_handle_stream_commit (worker.c:2148), serialized-transaction arm +
// the shared commit tail.
pub(crate) fn apply_handle_stream_commit(
    mcx: Mcx<'static>,
    conn: &mut PgConn,
    r: &mut logicalproto::Reader<'_>,
) -> PgResult<()> {
    if in_streamed_transaction() {
        protocol_violation(
            "STREAM COMMIT message without STREAM STOP",
            "apply_handle_stream_commit",
        )?;
    }

    let (xid, commit_data) = logicalproto::logicalrep_read_stream_commit(r)?;

    // Replay all the spooled operations, then commit like a live COMMIT.
    apply_spooled_messages(mcx, conn, xid, commit_data.commit_lsn)?;
    crate::apply::apply_handle_commit_internal(mcx, &commit_data)?;

    // Unlink the files with serialized changes and subxact info.
    stream_cleanup_files(subid(), xid)?;

    crate::tablesync::process_syncing_tables(mcx, conn, commit_data.end_lsn)?;
    Ok(())
}

// ---- spool file helpers -----------------------------------------------------

// stream_cleanup_files (worker.c:4304).
fn stream_cleanup_files(subid: Oid, xid: TransactionId) -> PgResult<()> {
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
    let name = changes_filename(subid, xid);
    let file = with_fileset(|fs| {
        if first_segment {
            BufFileCreateFileSet(mcx, fs, &name)
        } else {
            // Always append: seek to the end.
            let mut f = BufFileOpenFileSet(mcx, fs, &name, false)?;
            f.seek(0, 0, fd::buffile::SEEK_END)?;
            Ok(f)
        }
    })?;
    STREAM_FD.with(|f| *f.borrow_mut() = Some(file));
    Ok(())
}

// stream_close_file (worker.c:4373).
fn stream_close_file() {
    let file = STREAM_FD.with(|f| f.borrow_mut().take()).expect("stream file open");
    // The spool must be durable across chunks; close flushes the buffer.
    file.close().expect("closing streamed-changes spool file");
}

// stream_write_change (worker.c:4391): [len][action][payload], the payload
// already stripped of the (sub)txn xid.
fn stream_write_change(action: u8, payload: &[u8]) -> PgResult<()> {
    STREAM_FD.with(|f| {
        let mut slot = f.borrow_mut();
        let file = slot.as_mut().expect("stream file open");
        let len = (payload.len() + 1) as i32;
        file.write(&len.to_ne_bytes())?;
        file.write(&[action])?;
        file.write(payload)
    })
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
    let mut subxacts = Vec::with_capacity(n);
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
        f.borrow().as_ref().expect("stream file open").tell()
    });
    SUBXACTS.with(|s| s.borrow_mut().push(SubXactInfo { xid, fileno, offset }));
    Ok(())
}

// cleanup_subxact_info (worker.c:4487).
fn cleanup_subxact_info() {
    SUBXACTS.with(|s| s.borrow_mut().clear());
    SUBXACT_LAST.set(InvalidTransactionId);
}
